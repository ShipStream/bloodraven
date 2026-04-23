package sidecar

import (
	"bytes"
	"context"
	"crypto/rand"
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

// TestEncryptedStore_PlaintextPassthrough covers the mixed-encryption
// upgrade window: a store that turned on encryption mid-life of a
// deployment still needs to decrypt legacy plaintext objects. The
// wrapper must detect the missing BRV1 magic and pass bytes through
// unchanged.
func TestEncryptedStore_PlaintextPassthrough(t *testing.T) {
	tmp := t.TempDir()
	inner, err := newPVCStore(&PITRPVCConfig{MountPath: tmp})
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()

	// Drop a plaintext object via the unwrapped store.
	payload := []byte("legacy plaintext object")
	if err := inner.Put(ctx, "legacy/file.txt", bytes.NewReader(payload), int64(len(payload))); err != nil {
		t.Fatal(err)
	}

	wrapped := WrapWithEncryption(inner, []byte("some passphrase"))
	got, ok, err := wrapped.Get(ctx, "legacy/file.txt")
	if err != nil || !ok {
		t.Fatalf("Get: ok=%v err=%v", ok, err)
	}
	if !bytes.Equal(got, payload) {
		t.Fatalf("plaintext passthrough: got %q want %q", got, payload)
	}

	// GetFile should also pass plaintext through unchanged.
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
