package sidecar

import (
	"bytes"
	"context"
	cryptorand "crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"math/rand/v2"
	"net/http"
	"os"
	"path"
	"path/filepath"
	"strings"
	"syscall"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/smithy-go"

	"github.com/shipstream/bloodraven/internal/pitrarchive"
)

// Atomic create (D-6, §3.4). Writers establish an archive only on a store
// that refuses a second create of the same key; there is no Get-then-Put
// fallback, because two first writers would both "win".
var (
	// ErrObjectExists: the key already exists, so nothing was written.
	ErrObjectExists = errors.New("sidecar: object already exists")
	// ErrConditionalWriteUnsupported: the store cannot create atomically
	// (S3 501/NotImplemented or a 400 naming the header; PVC link(2)
	// EPERM/ENOTSUP/EOPNOTSUPP/EXDEV; a store type without PutIfAbsent).
	ErrConditionalWriteUnsupported = errors.New("sidecar: store does not support atomic create")
	// ErrConditionalConflict: S3 kept answering 409 ConditionalRequestConflict
	// after every retry. It never certifies support (G2-10).
	ErrConditionalConflict = errors.New("sidecar: conditional write kept conflicting")
)

// AtomicCreator is implemented by stores that can create an object only
// when its key is absent. Use PutIfAbsent rather than asserting it.
type AtomicCreator interface {
	// PutIfAbsent writes r under key only if key does not exist. It returns
	// ErrObjectExists, ErrConditionalWriteUnsupported or
	// ErrConditionalConflict (all matched with errors.Is), or another error
	// for an ordinary failure.
	PutIfAbsent(ctx context.Context, key string, r io.Reader, size int64) error
}

// PutIfAbsent creates key atomically on s. A store without atomic create
// returns ErrConditionalWriteUnsupported, so callers fail closed.
func PutIfAbsent(ctx context.Context, s ArchiveStore, key string, r io.Reader, size int64) error {
	c, ok := s.(AtomicCreator)
	if !ok {
		return fmt.Errorf("%w: %T", ErrConditionalWriteUnsupported, s)
	}
	return c.PutIfAbsent(ctx, key, r, size)
}

// maxAtomicCreateBytes bounds what PutIfAbsent buffers. Its callers write
// descriptors, probes and backup stamps, all well under a kilobyte.
const maxAtomicCreateBytes = 1 << 20

func readAtomicBody(r io.Reader) ([]byte, error) {
	body, err := io.ReadAll(io.LimitReader(r, maxAtomicCreateBytes+1))
	if err != nil {
		return nil, err
	}
	if len(body) > maxAtomicCreateBytes {
		return nil, fmt.Errorf("atomic create body exceeds %d bytes", maxAtomicCreateBytes)
	}
	return body, nil
}

// s3ConditionalRetries is how many times a 409 ConditionalRequestConflict
// is retried before PutIfAbsent gives up with ErrConditionalConflict.
const s3ConditionalRetries = 3

// s3ConditionalBackoff is the base delay between 409 retries; each retry
// waits a random delay in [base, 2*base). A variable for tests.
var s3ConditionalBackoff = 100 * time.Millisecond

func (s *s3Store) PutIfAbsent(ctx context.Context, key string, r io.Reader, _ int64) error {
	body, err := readAtomicBody(r)
	if err != nil {
		return fmt.Errorf("s3 create %s: %w", key, err)
	}
	for attempt := 0; ; attempt++ {
		_, err := s.client.PutObject(ctx, &s3.PutObjectInput{
			Bucket:        aws.String(s.bucket),
			Key:           aws.String(key),
			Body:          bytes.NewReader(body),
			ContentLength: aws.Int64(int64(len(body))),
			IfNoneMatch:   aws.String("*"),
		})
		switch classifyS3ConditionalError(err) {
		case s3ConditionalOK:
			return nil
		case s3ConditionalExists:
			return fmt.Errorf("s3 create %s: %w", key, ErrObjectExists)
		case s3ConditionalUnsupported:
			return fmt.Errorf("s3 create %s: %w: %v", key, ErrConditionalWriteUnsupported, err)
		case s3ConditionalRetry:
			if attempt >= s3ConditionalRetries {
				return fmt.Errorf("s3 create %s: %w: %v", key, ErrConditionalConflict, err)
			}
			delay := s3ConditionalBackoff + time.Duration(rand.Int64N(int64(s3ConditionalBackoff)+1))
			if !waitOrDone(ctx, delay) {
				return ctx.Err()
			}
		default:
			return fmt.Errorf("s3 create %s: %w", key, err)
		}
	}
}

type s3ConditionalOutcome int

const (
	s3ConditionalOK s3ConditionalOutcome = iota
	s3ConditionalExists
	s3ConditionalUnsupported
	s3ConditionalRetry
	s3ConditionalFailed
)

// classifyS3ConditionalError maps a conditional PutObject result. Only an
// actual 412 (or the PreconditionFailed code) means the key exists.
func classifyS3ConditionalError(err error) s3ConditionalOutcome {
	if err == nil {
		return s3ConditionalOK
	}
	status := 0
	var withStatus interface{ HTTPStatusCode() int }
	if errors.As(err, &withStatus) {
		status = withStatus.HTTPStatusCode()
	}
	code, message := "", ""
	var apiErr smithy.APIError
	if errors.As(err, &apiErr) {
		code, message = apiErr.ErrorCode(), apiErr.ErrorMessage()
	}
	switch {
	case status == http.StatusPreconditionFailed || code == "PreconditionFailed":
		return s3ConditionalExists
	case status == http.StatusConflict && (code == "ConditionalRequestConflict" || code == ""):
		return s3ConditionalRetry
	case status == http.StatusNotImplemented || code == "NotImplemented":
		return s3ConditionalUnsupported
	case status == http.StatusBadRequest && mentionsConditionalHeader(code+" "+message):
		return s3ConditionalUnsupported
	}
	return s3ConditionalFailed
}

func mentionsConditionalHeader(text string) bool {
	text = strings.ToLower(text)
	return strings.Contains(text, "if-none-match") || strings.Contains(text, "ifnonematch") || strings.Contains(text, "conditional")
}

// PutIfAbsent publishes key with link(2): the object is written to a
// temporary file in the destination directory, synced, then hard-linked
// to its final name, which fails with EEXIST when the name exists.
func (p *pvcStore) PutIfAbsent(_ context.Context, key string, r io.Reader, _ int64) error {
	dst, err := p.fullPath(key)
	if err != nil {
		return err
	}
	dir := filepath.Dir(dst)
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return fmt.Errorf("mkdir %s: %w", dir, err)
	}
	// The ".tmp" suffix keeps a crash leftover out of List and the legacy check.
	tmp, err := os.CreateTemp(dir, filepath.Base(dst)+".*.tmp")
	if err != nil {
		return fmt.Errorf("create temp for %s: %w", key, err)
	}
	tmpName := tmp.Name()
	defer os.Remove(tmpName)
	if _, err := io.Copy(tmp, r); err != nil {
		_ = tmp.Close()
		return fmt.Errorf("write %s: %w", tmpName, err)
	}
	if err := tmp.Sync(); err != nil {
		_ = tmp.Close()
		return fmt.Errorf("sync %s: %w", tmpName, err)
	}
	if err := tmp.Close(); err != nil {
		return fmt.Errorf("close %s: %w", tmpName, err)
	}
	if err := os.Link(tmpName, dst); err != nil {
		return fmt.Errorf("link %s: %w", key, mapLinkError(err))
	}
	if err := syncDir(dir); err != nil {
		return fmt.Errorf("sync %s: %w", dir, err)
	}
	return nil
}

// mapLinkError maps link(2) failures onto the atomic-create errors. Any
// errno that means "this filesystem cannot hard-link" is unsupported, never
// a reason to fall back to rename.
func mapLinkError(err error) error {
	var errno syscall.Errno
	if !errors.As(err, &errno) {
		return err
	}
	if errno == syscall.EEXIST {
		return fmt.Errorf("%w: %v", ErrObjectExists, err)
	}
	// ENOTSUP and EOPNOTSUPP share a value on Linux, so this is not a switch.
	for _, unsupported := range []syscall.Errno{syscall.EPERM, syscall.ENOTSUP, syscall.EOPNOTSUPP, syscall.EXDEV} {
		if errno == unsupported {
			return fmt.Errorf("%w: %v", ErrConditionalWriteUnsupported, err)
		}
	}
	return err
}

func syncDir(dir string) error {
	d, err := os.Open(dir)
	if err != nil {
		return err
	}
	defer d.Close()
	return d.Sync()
}

// ProbeAtomicCreate certifies that s enforces atomic create under prefix
// (§3.4). It creates a random probe key twice: the first create must
// succeed, and only ErrObjectExists on the second (an actual 412, or EEXIST)
// certifies support. It returns nil when certified; a *pitrarchive.VerifyError
// with TokenArchiveStoreUnsupported when the store accepted the second
// create or reported no support; an error wrapping ErrConditionalConflict
// when the result was inconclusive (retry later); or a transport error.
// The probe key is deleted best effort.
func ProbeAtomicCreate(ctx context.Context, s ArchiveStore, prefix string) error {
	var nonce [8]byte
	if _, err := cryptorand.Read(nonce[:]); err != nil {
		return fmt.Errorf("probe atomic create: %w", err)
	}
	key := path.Join(prefix, pitrarchive.CASProbeKeyPrefix+hex.EncodeToString(nonce[:]))
	body := []byte("bloodraven atomic-create probe\n")
	unsupported := func(detail string, err error) error {
		if err != nil {
			detail += ": " + err.Error()
		}
		return &pitrarchive.VerifyError{
			Token:     pitrarchive.TokenArchiveStoreUnsupported,
			Detail:    detail,
			Procedure: "use a store that enforces conditional writes (S3 If-None-Match, or a filesystem with hard links)",
		}
	}
	first := PutIfAbsent(ctx, s, key, bytes.NewReader(body), int64(len(body)))
	switch {
	case first == nil:
	case errors.Is(first, ErrConditionalWriteUnsupported):
		return unsupported("the store rejected a conditional create", first)
	case errors.Is(first, ErrObjectExists):
		// A random 64-bit key that already exists proves nothing either way.
		return fmt.Errorf("probe atomic create: %w: probe key %s already existed", ErrConditionalConflict, key)
	default:
		return fmt.Errorf("probe atomic create: %w", first)
	}
	defer func() { _ = s.Delete(context.WithoutCancel(ctx), key) }()
	second := PutIfAbsent(ctx, s, key, bytes.NewReader(body), int64(len(body)))
	switch {
	case errors.Is(second, ErrObjectExists):
		return nil
	case second == nil:
		return unsupported("the store accepted a second conditional create of the same key", nil)
	case errors.Is(second, ErrConditionalWriteUnsupported):
		return unsupported("the store rejected a conditional create", second)
	case errors.Is(second, ErrConditionalConflict):
		return fmt.Errorf("probe atomic create: inconclusive: %w", second)
	default:
		return fmt.Errorf("probe atomic create: %w", second)
	}
}

// archivePlaintextSource is implemented by store wrappers that transform
// object bytes. The descriptor and the probe are plaintext by definition,
// so the archive session writes them to the backend underneath.
type archivePlaintextSource interface {
	plaintextArchive() (ArchiveStore, []byte)
}

// splitArchiveStore returns the plaintext backend under s and the
// passphrase s encrypts with (nil for a plaintext store).
func splitArchiveStore(s ArchiveStore) (ArchiveStore, []byte) {
	if p, ok := s.(archivePlaintextSource); ok {
		return p.plaintextArchive()
	}
	return s, nil
}
