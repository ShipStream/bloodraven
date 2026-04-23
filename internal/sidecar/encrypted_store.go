package sidecar

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"

	"github.com/shipstream/bloodraven/internal/backupcrypto"
)

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
}

// WrapWithEncryption returns a new ArchiveStore that transparently
// encrypts puts and decrypts gets using the supplied passphrase. A
// zero-length passphrase returns inner unchanged so callers can
// pass through the "encryption disabled" path without branching.
func WrapWithEncryption(inner ArchiveStore, passphrase []byte) ArchiveStore {
	if len(passphrase) == 0 {
		return inner
	}
	return &encryptedStore{inner: inner, passphrase: passphrase}
}

// Put streams r through the backupcrypto encrypter into an in-memory
// buffer (bounded by the caller's input size) and forwards the
// ciphertext to the underlying store. We buffer rather than stream
// because the AWS SDK v2 uploader may need to seek the body for
// multipart retries; wrapping Encrypt directly around an io.Reader
// would break that assumption.
func (e *encryptedStore) Put(ctx context.Context, key string, r io.Reader, _ int64) error {
	var buf bytes.Buffer
	if _, err := backupcrypto.Encrypt(&buf, r, e.passphrase); err != nil {
		return fmt.Errorf("encrypt %s: %w", key, err)
	}
	return e.inner.Put(ctx, key, bytes.NewReader(buf.Bytes()), int64(buf.Len()))
}

// PutFile streams the file at path through backupcrypto and uploads
// the ciphertext. We write the ciphertext to a tmp file instead of
// buffering in memory because archived binlog files can be hundreds
// of MB and the sidecar container's memory limit is typically
// smaller than a single binlog. The tmp file is always removed on
// exit.
func (e *encryptedStore) PutFile(ctx context.Context, key, path string) error {
	tmp, err := os.CreateTemp("", "bloodraven-enc-*")
	if err != nil {
		return fmt.Errorf("encrypted store: create temp: %w", err)
	}
	tmpPath := tmp.Name()
	defer os.Remove(tmpPath)

	in, err := os.Open(path)
	if err != nil {
		_ = tmp.Close()
		return fmt.Errorf("encrypted store: open %s: %w", path, err)
	}
	defer in.Close()

	if _, err := backupcrypto.Encrypt(tmp, in, e.passphrase); err != nil {
		_ = tmp.Close()
		return fmt.Errorf("encrypted store: encrypt %s: %w", path, err)
	}
	if err := tmp.Sync(); err != nil {
		_ = tmp.Close()
		return fmt.Errorf("encrypted store: sync temp: %w", err)
	}
	if err := tmp.Close(); err != nil {
		return fmt.Errorf("encrypted store: close temp: %w", err)
	}
	return e.inner.PutFile(ctx, key, tmpPath)
}

// Get reads the underlying object and decrypts it into memory. Like
// Put, we buffer because the manifest load path consumes the whole
// slice. Callers that need streaming decryption for very large
// objects should use GetFile.
func (e *encryptedStore) Get(ctx context.Context, key string) ([]byte, bool, error) {
	data, ok, err := e.inner.Get(ctx, key)
	if err != nil || !ok {
		return data, ok, err
	}
	if !backupcrypto.LooksEncrypted(data) {
		// Legacy / unencrypted object: pass it through so upgrades
		// that turn on encryption mid-life of a deployment don't
		// lose access to already-archived files.
		return data, true, nil
	}
	var buf bytes.Buffer
	if _, err := backupcrypto.Decrypt(&buf, bytes.NewReader(data), e.passphrase); err != nil {
		if errors.Is(err, backupcrypto.ErrMagicMismatch) {
			// LooksEncrypted already returned true; this branch is
			// unreachable in practice, but defensive: fall back to
			// the raw data rather than failing.
			return data, true, nil
		}
		return nil, false, fmt.Errorf("decrypt %s: %w", key, err)
	}
	return buf.Bytes(), true, nil
}

// GetFile streams the ciphertext into a tmp file, decrypts it to
// dst, and removes the tmp on exit. We write the plaintext into a
// sibling ".decpart" file and rename so readers (mysqlbinlog,
// loadDump) never see a partial plaintext object.
func (e *encryptedStore) GetFile(ctx context.Context, key, dst string) error {
	if err := os.MkdirAll(filepath.Dir(dst), 0o755); err != nil {
		return fmt.Errorf("encrypted store: mkdir: %w", err)
	}
	tmp, err := os.CreateTemp("", "bloodraven-dec-*")
	if err != nil {
		return fmt.Errorf("encrypted store: create temp: %w", err)
	}
	tmpPath := tmp.Name()
	_ = tmp.Close()
	defer os.Remove(tmpPath)

	if err := e.inner.GetFile(ctx, key, tmpPath); err != nil {
		return err
	}
	in, err := os.Open(tmpPath)
	if err != nil {
		return fmt.Errorf("encrypted store: open ciphertext: %w", err)
	}
	defer in.Close()

	plainPart := dst + ".decpart"
	out, err := os.OpenFile(plainPart, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0o600)
	if err != nil {
		return fmt.Errorf("encrypted store: create plaintext: %w", err)
	}
	if _, err := backupcrypto.Decrypt(out, in, e.passphrase); err != nil {
		_ = out.Close()
		_ = os.Remove(plainPart)
		if errors.Is(err, backupcrypto.ErrMagicMismatch) {
			// The underlying object isn't actually encrypted; fall
			// through to a plain copy so mixed-encryption archives
			// (PITR migration in-flight) still restore correctly.
			if _, err := in.Seek(0, io.SeekStart); err != nil {
				return fmt.Errorf("encrypted store: rewind for plaintext passthrough: %w", err)
			}
			out, err := os.OpenFile(plainPart, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0o600)
			if err != nil {
				return fmt.Errorf("encrypted store: create plaintext passthrough: %w", err)
			}
			if _, err := io.Copy(out, in); err != nil {
				_ = out.Close()
				_ = os.Remove(plainPart)
				return fmt.Errorf("encrypted store: copy plaintext passthrough: %w", err)
			}
			if err := out.Close(); err != nil {
				_ = os.Remove(plainPart)
				return fmt.Errorf("encrypted store: close plaintext passthrough: %w", err)
			}
			return os.Rename(plainPart, dst)
		}
		return fmt.Errorf("encrypted store: decrypt %s: %w", key, err)
	}
	if err := out.Sync(); err != nil {
		_ = out.Close()
		_ = os.Remove(plainPart)
		return fmt.Errorf("encrypted store: sync: %w", err)
	}
	if err := out.Close(); err != nil {
		_ = os.Remove(plainPart)
		return fmt.Errorf("encrypted store: close: %w", err)
	}
	return os.Rename(plainPart, dst)
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
