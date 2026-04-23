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

// Put streams r through the backupcrypto encrypter, writes the
// ciphertext into a tmp file on disk, and forwards it to the
// underlying store via PutFile. We do not buffer the full plaintext in
// memory because callers are free to pass streams of arbitrary size;
// the old "bytes.Buffer everything" path was a latent OOM bug the day
// anything other than the small manifest JSON started using Put.
func (e *encryptedStore) Put(ctx context.Context, key string, r io.Reader, _ int64) error {
	tmp, err := os.CreateTemp("", "bloodraven-enc-put-*")
	if err != nil {
		return fmt.Errorf("encrypted store: create temp: %w", err)
	}
	tmpPath := tmp.Name()
	defer os.Remove(tmpPath)

	if _, err := backupcrypto.Encrypt(tmp, r, e.passphrase); err != nil {
		_ = tmp.Close()
		return fmt.Errorf("encrypt %s: %w", key, err)
	}
	if err := tmp.Sync(); err != nil {
		_ = tmp.Close()
		return fmt.Errorf("encrypted store: sync: %w", err)
	}
	if err := tmp.Close(); err != nil {
		return fmt.Errorf("encrypted store: close temp: %w", err)
	}
	return e.inner.PutFile(ctx, key, tmpPath)
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
		if e.allowPlaintext {
			return data, true, nil
		}
		return nil, false, fmt.Errorf("%w: key=%s", ErrTamperedOrDowngrade, key)
	}
	var buf bytes.Buffer
	if _, err := backupcrypto.Decrypt(&buf, bytes.NewReader(data), e.passphrase); err != nil {
		if errors.Is(err, backupcrypto.ErrMagicMismatch) {
			// LooksEncrypted returned true but Decrypt disagreed
			// (short object, truncated header). Under strict mode
			// treat this as tampering; in legacy mode, fall through
			// to the raw bytes as the old behaviour.
			if e.allowPlaintext {
				return data, true, nil
			}
			return nil, false, fmt.Errorf("%w: key=%s", ErrTamperedOrDowngrade, key)
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
			if !e.allowPlaintext {
				return fmt.Errorf("%w: key=%s", ErrTamperedOrDowngrade, key)
			}
			// Explicit mixed-encryption opt-in: copy bytes through
			// verbatim.
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
