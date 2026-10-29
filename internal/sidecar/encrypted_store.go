package sidecar

import (
	"bufio"
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"os"

	"github.com/shipstream/bloodraven/internal/backupcrypto"
)

// ErrTamperedOrDowngrade is returned by a strict encrypted store when a
// fetched object does not begin with the BRV1 magic. Because the store
// was configured to require encryption for every object, missing magic
// is treated as a tampering / downgrade attempt rather than a benign
// "legacy plaintext object" — writing plaintext to a bucket that should
// only contain ciphertext is how an attacker with write access would
// try to land attacker-chosen SQL into a restore path.
var ErrTamperedOrDowngrade = errors.New("sidecar: object missing BRV1 magic while encryption is required")

// encryptedStore wraps an ArchiveStore with transparent AES-256-GCM
// envelope encryption for every Put / PutFile and transparent
// decryption for every Get / GetFile. List and Delete pass through
// unchanged — listing and deleting objects neither sees nor produces
// plaintext.
//
// We wrap at the store layer (rather than inside the archiver
// goroutine) so any current or future consumer — binlog archiver,
// retention sweeper, pitr-download init container — automatically
// gets the encryption semantics without having to remember to plug
// in their own encrypt/decrypt calls. A mis-wired caller that ignores
// the wrapper would bypass encryption, which is exactly the class of
// bug we'd rather make impossible.
type encryptedStore struct {
	inner      ArchiveStore
	passphrase []byte
	// allowPlaintext opts into the legacy mixed-encryption behavior:
	// Get / GetFile will pass through an object that lacks the BRV1
	// magic header. This is only safe during a time-bounded migration
	// window where an operator is knowingly moving an unencrypted prefix
	// to encryption. Default is false — missing magic becomes
	// ErrTamperedOrDowngrade so an attacker with write access to the
	// backend cannot overwrite an encrypted object with attacker-chosen
	// plaintext and have it silently load into MySQL.
	allowPlaintext bool
}

func (e *encryptedStore) BinlogContentID(digest string) string {
	h := hmac.New(sha256.New, e.passphrase)
	_, _ = h.Write([]byte("bloodraven-binlog-content-id-v1\x00"))
	_, _ = h.Write([]byte(digest))
	return hex.EncodeToString(h.Sum(nil))
}

// WrapWithEncryption returns a new ArchiveStore that transparently
// encrypts puts and decrypts gets using the supplied passphrase. A
// zero-length passphrase returns inner unchanged so callers can
// pass through the "encryption disabled" path without branching.
//
// Use WrapWithEncryptionAllowPlaintext to opt into the legacy
// mixed-encryption upgrade window.
func WrapWithEncryption(inner ArchiveStore, passphrase []byte) ArchiveStore {
	return WrapWithEncryptionOptions(inner, passphrase, false)
}

// WrapWithEncryptionOptions is the full form of WrapWithEncryption.
// Setting allowPlaintext=true restores the pre-hardening behavior
// where an object missing BRV1 magic is returned as plaintext. This
// should only be used for an explicit, operator-acknowledged migration
// window — it is an unauthenticated fallthrough from the ciphertext
// path and defeats the tamper-detection claim of AES-GCM.
func WrapWithEncryptionOptions(inner ArchiveStore, passphrase []byte, allowPlaintext bool) ArchiveStore {
	if len(passphrase) == 0 {
		return inner
	}
	return &encryptedStore{inner: inner, passphrase: passphrase, allowPlaintext: allowPlaintext}
}

// Put encrypts through a bounded pipe into the existing streaming uploader.
func (e *encryptedStore) Put(ctx context.Context, key string, source io.Reader, _ int64) error {
	reader, writer := io.Pipe()
	done := make(chan error, 1)
	go func() {
		_, err := backupcrypto.Encrypt(writer, source, e.passphrase)
		_ = writer.CloseWithError(err)
		done <- err
	}()
	err := e.inner.Put(ctx, key, reader, -1)
	_ = reader.CloseWithError(err)
	encryptErr := <-done
	if err != nil {
		return err
	}
	return encryptErr
}
func (e *encryptedStore) PutFile(ctx context.Context, key, path string) error {
	file, err := os.Open(path)
	if err != nil {
		return err
	}
	defer file.Close()
	return e.Put(ctx, key, file, -1)
}

type archiveReadCloser struct {
	io.Reader
	close func() error
}

func (r *archiveReadCloser) Close() error { return r.close() }

// Open decrypts incrementally. EOF, including final-chunk authentication, is
// mandatory before a caller may publish bytes or observations from the stream.
func (e *encryptedStore) Open(ctx context.Context, key string) (io.ReadCloser, error) {
	source, err := e.inner.Open(ctx, key)
	if err != nil {
		return nil, err
	}
	buffered := bufio.NewReader(source)
	prefix, _ := buffered.Peek(4)
	if !backupcrypto.LooksEncrypted(prefix) {
		if e.allowPlaintext {
			return &archiveReadCloser{Reader: buffered, close: source.Close}, nil
		}
		_ = source.Close()
		return nil, fmt.Errorf("%w: key=%s", ErrTamperedOrDowngrade, key)
	}
	reader, writer := io.Pipe()
	go func() {
		_, err := backupcrypto.Decrypt(writer, buffered, e.passphrase)
		_ = source.Close()
		_ = writer.CloseWithError(err)
	}()
	return &archiveReadCloser{Reader: reader, close: func() error { _ = reader.Close(); return source.Close() }}, nil
}
func (e *encryptedStore) Get(ctx context.Context, key string) ([]byte, bool, error) {
	reader, err := e.Open(ctx, key)
	if errors.Is(err, os.ErrNotExist) || isS3NotFound(err) {
		return nil, false, nil
	}
	if err != nil {
		return nil, false, err
	}
	defer reader.Close()
	data, err := io.ReadAll(reader)
	return data, err == nil, err
}
func (e *encryptedStore) GetFile(ctx context.Context, key, dst string) error {
	reader, err := e.Open(ctx, key)
	if err != nil {
		return err
	}
	defer reader.Close()
	return writeArchiveFile(reader, dst)
}

// Delete passes through; delete needs no plaintext.
func (e *encryptedStore) Delete(ctx context.Context, key string) error {
	return e.inner.Delete(ctx, key)
}

// List passes through; the object-name surface is not considered
// secret (see the backupcrypto package doc for threat-model
// boundaries).
func (e *encryptedStore) List(ctx context.Context, prefix string) ([]string, error) {
	return e.inner.List(ctx, prefix)
}
