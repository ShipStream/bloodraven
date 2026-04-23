package sidecar

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"

	"github.com/aws/aws-sdk-go-v2/aws"
	awsconfig "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/feature/s3/manager"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/smithy-go"

	"github.com/shipstream/bloodraven/internal/backupcrypto"
)

// ArchiveStore abstracts the two storage backends (S3 and local PVC)
// behind a tiny upload/download/delete/list interface. The archiver
// (sidecar goroutine), the retention sweeper, and the restore-side
// pitr-download subcommand all program against this — so adding a
// third backend (GCS, Azure) later is localized to this file.
type ArchiveStore interface {
	// Put writes data under key. Overwrites are OK: the archiver treats
	// the write as idempotent by key.
	Put(ctx context.Context, key string, r io.Reader, size int64) error

	// Get reads the object at key into a new byte slice. Used for
	// manifest read-modify-write and for the restore path's manifest
	// load; binlog files go through GetFile instead so they don't have
	// to fit in memory.
	Get(ctx context.Context, key string) ([]byte, bool, error)

	// GetFile streams the object at key into a local file at dst. Used
	// by the restore init container to download each needed binlog to
	// the shared emptyDir without buffering the whole thing in RAM.
	GetFile(ctx context.Context, key, dst string) error

	// Delete removes the object at key. Missing-key is a no-op (returns
	// nil). Used by retention cleanup.
	Delete(ctx context.Context, key string) error

	// PutFile is a file-path convenience wrapper that streams the file
	// at path to the object at key. For S3 it uses the managed uploader
	// (multipart if large); for PVC it is a sendfile-ish copy.
	PutFile(ctx context.Context, key, path string) error

	// List returns every key with the given prefix. S3 handles
	// pagination internally; PVC walks the on-disk tree. Keys are
	// returned relative to the storage root (matching the same form
	// used by Put/Get).
	List(ctx context.Context, prefix string) ([]string, error)
}

// archiveStore is the historical unexported alias; kept for the
// sidecar-internal call sites that already reference it to avoid a
// mass rename in this commit. New external call sites should use
// the exported ArchiveStore name directly.
type archiveStore = ArchiveStore

// NewArchiveStore is the exported constructor used by cmd/sidecar/main.go.
// Delegates to newArchiveStore; keeping the lowercase version around
// for internal tests that want to stub it.
func NewArchiveStore(ctx context.Context, cfg *PITRConfig) (archiveStore, error) {
	return newArchiveStore(ctx, cfg)
}

// newArchiveStore constructs the backend matching cfg. It is called
// once at sidecar startup; the returned store is safe for concurrent
// use from the archiver goroutine and any future retention worker.
//
// When cfg.PassphraseFile is set the concrete backend is wrapped with
// an encryptedStore so every Put/Get/PutFile/GetFile transparently
// runs through backupcrypto. List/Delete pass through unchanged.
func newArchiveStore(ctx context.Context, cfg *PITRConfig) (archiveStore, error) {
	var base archiveStore
	switch cfg.StorageType {
	case "S3":
		if cfg.S3 == nil {
			return nil, fmt.Errorf("archive store: S3 config is nil")
		}
		s, err := newS3Store(ctx, cfg.S3)
		if err != nil {
			return nil, err
		}
		base = s
	case "PVC":
		if cfg.PVC == nil {
			return nil, fmt.Errorf("archive store: PVC config is nil")
		}
		s, err := newPVCStore(cfg.PVC)
		if err != nil {
			return nil, err
		}
		base = s
	default:
		return nil, fmt.Errorf("archive store: unknown storage type %q", cfg.StorageType)
	}

	if cfg.PassphraseFile != "" {
		passphrase, err := backupcrypto.ReadPassphraseFile(cfg.PassphraseFile)
		if err != nil {
			return nil, fmt.Errorf("archive store: read passphrase: %w", err)
		}
		base = WrapWithEncryptionOptions(base, passphrase, cfg.AllowPlaintextFallback)
	}
	return base, nil
}

// ----------------------------------------------------------------------
// S3 implementation
// ----------------------------------------------------------------------

type s3Store struct {
	client   *s3.Client
	uploader *manager.Uploader
	bucket   string
}

func newS3Store(ctx context.Context, cfg *PITRS3Config) (*s3Store, error) {
	// Mirror the credential-file layout the backup/restore Python
	// scripts expect: read AWS_* from files under AWSCredsDir if
	// provided, otherwise fall through to the default SDK chain
	// (IRSA, env, instance profile).
	loadOpts := []func(*awsconfig.LoadOptions) error{}
	if cfg.Region != "" {
		loadOpts = append(loadOpts, awsconfig.WithRegion(cfg.Region))
	}
	if cfg.AWSCredsDir != "" {
		// The operator mounts a Secret here; a missing or unreadable
		// required file is an operator mistake, not a "fall back to
		// ambient creds" signal. Failing loud prevents the restore Job
		// from silently running under IRSA / instance-profile
		// credentials that may have a broader scope than the caller
		// intended (see AUDIT M2).
		ak, err := readTrimFile(filepath.Join(cfg.AWSCredsDir, "AWS_ACCESS_KEY_ID"))
		if err != nil {
			return nil, fmt.Errorf("aws creds dir: read AWS_ACCESS_KEY_ID: %w", err)
		}
		sk, err := readTrimFile(filepath.Join(cfg.AWSCredsDir, "AWS_SECRET_ACCESS_KEY"))
		if err != nil {
			return nil, fmt.Errorf("aws creds dir: read AWS_SECRET_ACCESS_KEY: %w", err)
		}
		// Session token is optional; missing file is OK, but a file we
		// can't read is not. ReadFile will only return os.IsNotExist
		// when the file is absent; anything else (permission denied,
		// truncated mount) is surfaced.
		st, err := readOptionalTrimFile(filepath.Join(cfg.AWSCredsDir, "AWS_SESSION_TOKEN"))
		if err != nil {
			return nil, fmt.Errorf("aws creds dir: read AWS_SESSION_TOKEN: %w", err)
		}
		rg, err := readOptionalTrimFile(filepath.Join(cfg.AWSCredsDir, "AWS_REGION"))
		if err != nil {
			return nil, fmt.Errorf("aws creds dir: read AWS_REGION: %w", err)
		}
		if cfg.Region == "" && rg != "" {
			loadOpts = append(loadOpts, awsconfig.WithRegion(rg))
		}
		if ak == "" || sk == "" {
			return nil, fmt.Errorf("aws creds dir: AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY must be non-empty")
		}
		loadOpts = append(loadOpts, awsconfig.WithCredentialsProvider(
			credentials.NewStaticCredentialsProvider(ak, sk, st),
		))
	}

	awsCfg, err := awsconfig.LoadDefaultConfig(ctx, loadOpts...)
	if err != nil {
		return nil, fmt.Errorf("load aws config: %w", err)
	}

	clientOpts := []func(*s3.Options){}
	if cfg.EndpointURL != "" {
		endpoint := cfg.EndpointURL
		clientOpts = append(clientOpts, func(o *s3.Options) {
			o.BaseEndpoint = aws.String(endpoint)
			// Path-style is needed for most S3-compatible stores
			// (MinIO, Ceph) when a custom endpoint is set.
			o.UsePathStyle = true
		})
	}

	client := s3.NewFromConfig(awsCfg, clientOpts...)
	uploader := manager.NewUploader(client)

	return &s3Store{client: client, uploader: uploader, bucket: cfg.Bucket}, nil
}

func (s *s3Store) Put(ctx context.Context, key string, r io.Reader, size int64) error {
	_, err := s.uploader.Upload(ctx, &s3.PutObjectInput{
		Bucket: aws.String(s.bucket),
		Key:    aws.String(key),
		Body:   r,
	})
	if err != nil {
		return fmt.Errorf("s3 upload %s: %w", key, err)
	}
	return nil
}

func (s *s3Store) PutFile(ctx context.Context, key, path string) error {
	f, err := os.Open(path)
	if err != nil {
		return fmt.Errorf("open %s: %w", path, err)
	}
	defer f.Close()
	fi, err := f.Stat()
	if err != nil {
		return fmt.Errorf("stat %s: %w", path, err)
	}
	return s.Put(ctx, key, f, fi.Size())
}

func (s *s3Store) Get(ctx context.Context, key string) ([]byte, bool, error) {
	resp, err := s.client.GetObject(ctx, &s3.GetObjectInput{
		Bucket: aws.String(s.bucket),
		Key:    aws.String(key),
	})
	if err != nil {
		if isS3NotFound(err) {
			return nil, false, nil
		}
		return nil, false, fmt.Errorf("s3 get %s: %w", key, err)
	}
	defer resp.Body.Close()
	var buf bytes.Buffer
	if _, err := io.Copy(&buf, resp.Body); err != nil {
		return nil, false, fmt.Errorf("s3 read %s: %w", key, err)
	}
	return buf.Bytes(), true, nil
}

func (s *s3Store) GetFile(ctx context.Context, key, dst string) error {
	if err := os.MkdirAll(filepath.Dir(dst), 0o755); err != nil {
		return fmt.Errorf("mkdir %s: %w", filepath.Dir(dst), err)
	}
	resp, err := s.client.GetObject(ctx, &s3.GetObjectInput{
		Bucket: aws.String(s.bucket),
		Key:    aws.String(key),
	})
	if err != nil {
		return fmt.Errorf("s3 get %s: %w", key, err)
	}
	defer resp.Body.Close()
	// Stream to a tmp file + rename for atomicity; a reader picking up
	// the file mid-download would otherwise get garbage.
	tmp := dst + ".part"
	f, err := os.OpenFile(tmp, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0o644)
	if err != nil {
		return fmt.Errorf("open %s: %w", tmp, err)
	}
	if _, err := io.Copy(f, resp.Body); err != nil {
		_ = f.Close()
		_ = os.Remove(tmp)
		return fmt.Errorf("stream %s: %w", key, err)
	}
	if err := f.Close(); err != nil {
		_ = os.Remove(tmp)
		return fmt.Errorf("close %s: %w", tmp, err)
	}
	if err := os.Rename(tmp, dst); err != nil {
		_ = os.Remove(tmp)
		return fmt.Errorf("rename %s: %w", dst, err)
	}
	return nil
}

// List returns every key with the given prefix. Uses the paginated
// ListObjectsV2 under the hood so large archives aren't truncated.
func (s *s3Store) List(ctx context.Context, prefix string) ([]string, error) {
	var out []string
	paginator := s3.NewListObjectsV2Paginator(s.client, &s3.ListObjectsV2Input{
		Bucket: aws.String(s.bucket),
		Prefix: aws.String(prefix),
	})
	for paginator.HasMorePages() {
		page, err := paginator.NextPage(ctx)
		if err != nil {
			return nil, fmt.Errorf("s3 list %s: %w", prefix, err)
		}
		for _, obj := range page.Contents {
			if obj.Key != nil {
				out = append(out, *obj.Key)
			}
		}
	}
	return out, nil
}

func (s *s3Store) Delete(ctx context.Context, key string) error {
	_, err := s.client.DeleteObject(ctx, &s3.DeleteObjectInput{
		Bucket: aws.String(s.bucket),
		Key:    aws.String(key),
	})
	if err != nil && !isS3NotFound(err) {
		return fmt.Errorf("s3 delete %s: %w", key, err)
	}
	return nil
}

// isS3NotFound returns true for the various flavours of "object/key
// does not exist" that the AWS SDK returns: NoSuchKey on GetObject,
// NotFound on HeadObject, 404 status on DeleteObject.
func isS3NotFound(err error) bool {
	if err == nil {
		return false
	}
	var apiErr smithy.APIError
	if !errors.As(err, &apiErr) {
		return false
	}
	code := apiErr.ErrorCode()
	return code == "NoSuchKey" || code == "NotFound" || code == "404"
}

// ----------------------------------------------------------------------
// PVC (local directory) implementation
// ----------------------------------------------------------------------

type pvcStore struct {
	root string
}

func newPVCStore(cfg *PITRPVCConfig) (*pvcStore, error) {
	if cfg.MountPath == "" {
		return nil, fmt.Errorf("pvc archive store: empty mount path")
	}
	// MountPath is the PVC mount root; binlog objects live under
	// <mount>/binlogs/. We lazily create the directory tree on each
	// Put, so no work needed here.
	return &pvcStore{root: cfg.MountPath}, nil
}

// fullPath resolves a storage-relative key into an absolute filesystem
// path under p.root. It rejects keys that try to escape the mounted
// backup volume via "..", which would otherwise write outside the PVC.
// Relies on filepath.Rel to detect escape rather than just string
// prefix matching so we catch cases like root="/mnt/pvc/" vs
// cleaned="/mnt/pvc-evil/...".
func (p *pvcStore) fullPath(key string) (string, error) {
	clean := filepath.Clean(strings.TrimLeft(key, "/\\"))
	if clean == "." || clean == ".." || strings.HasPrefix(clean, ".."+string(filepath.Separator)) {
		return "", fmt.Errorf("invalid archive key %q", key)
	}
	full := filepath.Join(p.root, clean)
	rel, err := filepath.Rel(p.root, full)
	if err != nil {
		return "", fmt.Errorf("resolve archive key %q: %w", key, err)
	}
	if rel == ".." || strings.HasPrefix(rel, ".."+string(filepath.Separator)) {
		return "", fmt.Errorf("archive key escapes pvc root: %q", key)
	}
	return full, nil
}

func (p *pvcStore) Put(_ context.Context, key string, r io.Reader, _ int64) error {
	dst, err := p.fullPath(key)
	if err != nil {
		return err
	}
	if err := os.MkdirAll(filepath.Dir(dst), 0o755); err != nil {
		return fmt.Errorf("mkdir %s: %w", filepath.Dir(dst), err)
	}
	// Write to a tmp file first then rename, so readers never see a
	// partial manifest under concurrent access.
	tmp := dst + ".tmp"
	f, err := os.OpenFile(tmp, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0o644)
	if err != nil {
		return fmt.Errorf("create %s: %w", tmp, err)
	}
	if _, err := io.Copy(f, r); err != nil {
		_ = f.Close()
		_ = os.Remove(tmp)
		return fmt.Errorf("write %s: %w", tmp, err)
	}
	if err := f.Sync(); err != nil {
		_ = f.Close()
		_ = os.Remove(tmp)
		return fmt.Errorf("sync %s: %w", tmp, err)
	}
	if err := f.Close(); err != nil {
		_ = os.Remove(tmp)
		return fmt.Errorf("close %s: %w", tmp, err)
	}
	if err := os.Rename(tmp, dst); err != nil {
		_ = os.Remove(tmp)
		return fmt.Errorf("rename %s -> %s: %w", tmp, dst, err)
	}
	return nil
}

func (p *pvcStore) PutFile(ctx context.Context, key, path string) error {
	f, err := os.Open(path)
	if err != nil {
		return fmt.Errorf("open %s: %w", path, err)
	}
	defer f.Close()
	fi, err := f.Stat()
	if err != nil {
		return fmt.Errorf("stat %s: %w", path, err)
	}
	return p.Put(ctx, key, f, fi.Size())
}

func (p *pvcStore) Get(_ context.Context, key string) ([]byte, bool, error) {
	src, err := p.fullPath(key)
	if err != nil {
		return nil, false, err
	}
	data, err := os.ReadFile(src)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, false, nil
		}
		return nil, false, fmt.Errorf("read %s: %w", key, err)
	}
	return data, true, nil
}

func (p *pvcStore) Delete(_ context.Context, key string) error {
	dst, err := p.fullPath(key)
	if err != nil {
		return err
	}
	if err := os.Remove(dst); err != nil && !os.IsNotExist(err) {
		return fmt.Errorf("delete %s: %w", key, err)
	}
	return nil
}

// GetFile copies the archived object at key into dst. Used by the
// restore-side pitr-download subcommand to pull binlog files onto a
// shared emptyDir so the mysqlsh container can feed them to
// mysqlbinlog. tmp+rename keeps the consumer from seeing a partial
// file if we're preempted mid-copy.
func (p *pvcStore) GetFile(_ context.Context, key, dst string) error {
	src, err := p.fullPath(key)
	if err != nil {
		return err
	}
	if err := os.MkdirAll(filepath.Dir(dst), 0o755); err != nil {
		return fmt.Errorf("mkdir %s: %w", filepath.Dir(dst), err)
	}
	in, err := os.Open(src)
	if err != nil {
		return fmt.Errorf("open %s: %w", src, err)
	}
	defer in.Close()
	tmp := dst + ".part"
	out, err := os.OpenFile(tmp, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0o644)
	if err != nil {
		return fmt.Errorf("create %s: %w", tmp, err)
	}
	if _, err := io.Copy(out, in); err != nil {
		_ = out.Close()
		_ = os.Remove(tmp)
		return fmt.Errorf("copy %s: %w", key, err)
	}
	if err := out.Close(); err != nil {
		_ = os.Remove(tmp)
		return fmt.Errorf("close %s: %w", tmp, err)
	}
	return os.Rename(tmp, dst)
}

// List walks the on-disk tree under prefix and returns every file key
// (relative to the PVC root), matching the S3 semantics.
func (p *pvcStore) List(_ context.Context, prefix string) ([]string, error) {
	root, err := p.fullPath(prefix)
	if err != nil {
		return nil, err
	}
	info, err := os.Stat(root)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, nil
		}
		return nil, fmt.Errorf("stat %s: %w", root, err)
	}
	var out []string
	if !info.IsDir() {
		// Caller passed a full key; mirror S3 behavior and return it
		// if it exists.
		out = append(out, prefix)
		return out, nil
	}
	err = filepath.Walk(root, func(path string, fi os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		if fi.IsDir() {
			return nil
		}
		rel, err := filepath.Rel(p.root, path)
		if err != nil {
			return err
		}
		// Normalize to forward slashes so callers building keys with
		// path.Join (which uses /) match regardless of GOOS.
		out = append(out, filepath.ToSlash(rel))
		return nil
	})
	if err != nil {
		return nil, fmt.Errorf("walk %s: %w", root, err)
	}
	return out, nil
}

// readTrimFile reads a small file and returns its trimmed contents.
// Used for the AWS_* credential files and similar single-line files.
func readTrimFile(path string) (string, error) {
	b, err := os.ReadFile(path)
	if err != nil {
		return "", err
	}
	return strings.TrimSpace(string(b)), nil
}

// readOptionalTrimFile is like readTrimFile but treats a missing file
// as empty string without error. Other errors (permission denied,
// truncated mount, EIO) are still surfaced so an operator mistake
// doesn't silently produce a degraded credential set.
func readOptionalTrimFile(path string) (string, error) {
	b, err := os.ReadFile(path)
	if err != nil {
		if os.IsNotExist(err) {
			return "", nil
		}
		return "", err
	}
	return strings.TrimSpace(string(b)), nil
}
