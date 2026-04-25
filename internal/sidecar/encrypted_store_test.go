package sidecar

import (
	"bytes"
	"context"
	"crypto/rand"
	"errors"
	"io"
	"os"
	"path/filepath"
	"testing"
)

// TestEncryptedStore_RoundTrip exercises Put / PutFile / Get / GetFile
// through the wrapper with a real PVC-backed store underneath. The
// caller sees plaintext; the underlying files on disk are ciphertext.
func TestEncryptedStore_RoundTrip(t *testing.T) {
	tmp := t.TempDir()
	inner, err := newPVCStore(&PITRPVCConfig{MountPath: tmp})
	if err != nil {
		t.Fatal(err)
	}
	wrapped := WrapWithEncryption(inner, []byte("round-trip passphrase"))
	ctx := context.Background()

	// Put + Get round-trip.
	payload := []byte("hello encrypted store")
	if err := wrapped.Put(ctx, "dir/file.txt", bytes.NewReader(payload), int64(len(payload))); err != nil {
		t.Fatalf("Put: %v", err)
	}
	got, ok, err := wrapped.Get(ctx, "dir/file.txt")
	if err != nil || !ok {
		t.Fatalf("Get: ok=%v err=%v", ok, err)
	}
	if !bytes.Equal(got, payload) {
		t.Fatalf("Get returned %q want %q", got, payload)
	}

	// Underlying object must not be plaintext.
	raw, err := os.ReadFile(filepath.Join(tmp, "dir/file.txt"))
	if err != nil {
		t.Fatal(err)
	}
	if bytes.Equal(raw, payload) {
		t.Fatalf("expected ciphertext on disk, got plaintext")
	}
}

func TestEncryptedStore_PutFile_GetFile(t *testing.T) {
	tmp := t.TempDir()
	inner, err := newPVCStore(&PITRPVCConfig{MountPath: tmp})
	if err != nil {
		t.Fatal(err)
	}
	wrapped := WrapWithEncryption(inner, []byte("file round-trip passphrase"))
	ctx := context.Background()

	srcDir := t.TempDir()
	src := filepath.Join(srcDir, "plain.bin")
	// Make the payload big enough to cross a GCM chunk boundary.
	payload := make([]byte, 200*1024)
	if _, err := io.ReadFull(rand.Reader, payload); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(src, payload, 0o600); err != nil {
		t.Fatal(err)
	}

	if err := wrapped.PutFile(ctx, "bigfile", src); err != nil {
		t.Fatalf("PutFile: %v", err)
	}

	dstDir := t.TempDir()
	dst := filepath.Join(dstDir, "decrypted.bin")
	if err := wrapped.GetFile(ctx, "bigfile", dst); err != nil {
		t.Fatalf("GetFile: %v", err)
	}
	got, err := os.ReadFile(dst)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(got, payload) {
		t.Fatalf("GetFile decrypted content mismatch")
	}
}

// TestEncryptedStore_PlaintextRejectedByDefault proves the hardening
// for AUDIT B1: an object lacking the BRV1 magic must be rejected as
// a tamper/downgrade attempt when the store was configured for
// encryption, because letting attacker-written plaintext pass through
// defeats AES-GCM's authenticity guarantee.
func TestEncryptedStore_PlaintextRejectedByDefault(t *testing.T) {
	tmp := t.TempDir()
	inner, err := newPVCStore(&PITRPVCConfig{MountPath: tmp})
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()

	payload := []byte("attacker-chosen plaintext")
	if err := inner.Put(ctx, "victim.txt", bytes.NewReader(payload), int64(len(payload))); err != nil {
		t.Fatal(err)
	}

	wrapped := WrapWithEncryption(inner, []byte("some passphrase"))
	if _, _, err := wrapped.Get(ctx, "victim.txt"); err == nil {
		t.Fatalf("Get: expected ErrTamperedOrDowngrade, got nil")
	} else if !errors.Is(err, ErrTamperedOrDowngrade) {
		t.Fatalf("Get: want ErrTamperedOrDowngrade, got %v", err)
	}

	dstDir := t.TempDir()
	dst := filepath.Join(dstDir, "out")
	if err := wrapped.GetFile(ctx, "victim.txt", dst); err == nil {
		t.Fatalf("GetFile: expected ErrTamperedOrDowngrade, got nil")
	} else if !errors.Is(err, ErrTamperedOrDowngrade) {
		t.Fatalf("GetFile: want ErrTamperedOrDowngrade, got %v", err)
	}
}

// TestEncryptedStore_PlaintextPassthroughOptIn covers the legacy
// mixed-encryption upgrade window: when an operator explicitly opts in
// via WrapWithEncryptionOptions(allowPlaintext=true), objects missing
// BRV1 magic pass through verbatim so a deployment migrating to
// encryption can still read previously-written plaintext.
func TestEncryptedStore_PlaintextPassthroughOptIn(t *testing.T) {
	tmp := t.TempDir()
	inner, err := newPVCStore(&PITRPVCConfig{MountPath: tmp})
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()

	payload := []byte("legacy plaintext object")
	if err := inner.Put(ctx, "legacy/file.txt", bytes.NewReader(payload), int64(len(payload))); err != nil {
		t.Fatal(err)
	}

	wrapped := WrapWithEncryptionOptions(inner, []byte("some passphrase"), true)
	got, ok, err := wrapped.Get(ctx, "legacy/file.txt")
	if err != nil || !ok {
		t.Fatalf("Get: ok=%v err=%v", ok, err)
	}
	if !bytes.Equal(got, payload) {
		t.Fatalf("plaintext passthrough: got %q want %q", got, payload)
	}

	dstDir := t.TempDir()
	dst := filepath.Join(dstDir, "out")
	if err := wrapped.GetFile(ctx, "legacy/file.txt", dst); err != nil {
		t.Fatalf("GetFile: %v", err)
	}
	out, err := os.ReadFile(dst)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(out, payload) {
		t.Fatalf("GetFile plaintext passthrough mismatch")
	}
}

func TestWrapWithEncryption_EmptyPassphraseReturnsInner(t *testing.T) {
	tmp := t.TempDir()
	inner, err := newPVCStore(&PITRPVCConfig{MountPath: tmp})
	if err != nil {
		t.Fatal(err)
	}
	got := WrapWithEncryption(inner, nil)
	if got != archiveStore(inner) {
		t.Fatalf("empty passphrase should return the inner store unchanged")
	}
}
