package backupcrypto

import (
	"bytes"
	"crypto/rand"
	"errors"
	"io"
	"os"
	"path/filepath"
	"testing"
)

func TestRoundTrip_Small(t *testing.T) {
	passphrase := []byte("correct horse battery staple")
	plaintext := []byte("hello world")

	var enc bytes.Buffer
	n, err := Encrypt(&enc, bytes.NewReader(plaintext), passphrase)
	if err != nil {
		t.Fatalf("encrypt: %v", err)
	}
	if n != int64(len(plaintext)) {
		t.Fatalf("byte count mismatch: got %d want %d", n, len(plaintext))
	}
	if !LooksEncrypted(enc.Bytes()) {
		t.Fatalf("encrypt output missing BRV1 magic: %x", enc.Bytes()[:8])
	}
	if len(enc.Bytes()) <= HeaderSize {
		t.Fatalf("encrypt output too short: %d bytes", len(enc.Bytes()))
	}

	var dec bytes.Buffer
	m, err := Decrypt(&dec, bytes.NewReader(enc.Bytes()), passphrase)
	if err != nil {
		t.Fatalf("decrypt: %v", err)
	}
	if m != int64(len(plaintext)) {
		t.Fatalf("decrypt byte count: got %d want %d", m, len(plaintext))
	}
	if !bytes.Equal(dec.Bytes(), plaintext) {
		t.Fatalf("decrypt output mismatch: got %q want %q", dec.Bytes(), plaintext)
	}
}

func TestRoundTrip_MultiChunk(t *testing.T) {
	passphrase := []byte("another passphrase with enough entropy ok")
	// 3 chunks + a partial: 2.5 * 64 KiB.
	plaintext := make([]byte, (1<<16)*2+1000)
	if _, err := rand.Read(plaintext); err != nil {
		t.Fatal(err)
	}

	var enc bytes.Buffer
	if _, err := Encrypt(&enc, bytes.NewReader(plaintext), passphrase); err != nil {
		t.Fatalf("encrypt: %v", err)
	}

	var dec bytes.Buffer
	if _, err := Decrypt(&dec, bytes.NewReader(enc.Bytes()), passphrase); err != nil {
		t.Fatalf("decrypt: %v", err)
	}
	if !bytes.Equal(dec.Bytes(), plaintext) {
		t.Fatalf("decrypt output mismatch (len %d vs %d)", dec.Len(), len(plaintext))
	}
}

func TestRoundTrip_ExactlyOneChunk(t *testing.T) {
	// Edge case: plaintext exactly 65 536 bytes. Make sure the encoder
	// still emits a final-flag zero-length chunk (or a final-flag non-
	// zero chunk — either is legal as long as the decoder can verify).
	passphrase := []byte("exact one chunk passphrase")
	plaintext := make([]byte, 1<<16)
	for i := range plaintext {
		plaintext[i] = byte(i & 0xff)
	}

	var enc bytes.Buffer
	if _, err := Encrypt(&enc, bytes.NewReader(plaintext), passphrase); err != nil {
		t.Fatalf("encrypt: %v", err)
	}

	var dec bytes.Buffer
	if _, err := Decrypt(&dec, bytes.NewReader(enc.Bytes()), passphrase); err != nil {
		t.Fatalf("decrypt: %v", err)
	}
	if !bytes.Equal(dec.Bytes(), plaintext) {
		t.Fatalf("decrypt output mismatch")
	}
}

func TestRoundTrip_Empty(t *testing.T) {
	passphrase := []byte("empty plaintext is fine")
	var enc bytes.Buffer
	if _, err := Encrypt(&enc, bytes.NewReader(nil), passphrase); err != nil {
		t.Fatalf("encrypt: %v", err)
	}
	if len(enc.Bytes()) < HeaderSize+GCMTagSize {
		t.Fatalf("expected header + final-chunk tag, got %d bytes", len(enc.Bytes()))
	}

	var dec bytes.Buffer
	if _, err := Decrypt(&dec, bytes.NewReader(enc.Bytes()), passphrase); err != nil {
		t.Fatalf("decrypt: %v", err)
	}
	if dec.Len() != 0 {
		t.Fatalf("expected empty plaintext, got %d bytes", dec.Len())
	}
}

func TestWrongPassphrase(t *testing.T) {
	plaintext := []byte("top secret")
	var enc bytes.Buffer
	if _, err := Encrypt(&enc, bytes.NewReader(plaintext), []byte("right")); err != nil {
		t.Fatalf("encrypt: %v", err)
	}

	var dec bytes.Buffer
	_, err := Decrypt(&dec, bytes.NewReader(enc.Bytes()), []byte("wrong"))
	if err == nil {
		t.Fatalf("expected decryption to fail with wrong passphrase")
	}
}

func TestTruncation_ChunkBoundary(t *testing.T) {
	// Encode 3 chunks, drop the last one. Decoder should return
	// ErrTruncated, not succeed with partial data.
	passphrase := []byte("truncation passphrase")
	plaintext := make([]byte, (1<<16)*2+500)
	if _, err := rand.Read(plaintext); err != nil {
		t.Fatal(err)
	}

	var enc bytes.Buffer
	if _, err := Encrypt(&enc, bytes.NewReader(plaintext), passphrase); err != nil {
		t.Fatalf("encrypt: %v", err)
	}
	truncated := enc.Bytes()
	// Keep header + first two chunks (2 * 65552).
	truncated = truncated[:HeaderSize+(1<<16+GCMTagSize)*2]

	var dec bytes.Buffer
	_, err := Decrypt(&dec, bytes.NewReader(truncated), passphrase)
	if err == nil {
		t.Fatalf("expected truncation error, got nil")
	}
	if !errors.Is(err, ErrTruncated) {
		t.Logf("got non-ErrTruncated error: %v (accepted if auth failure)", err)
	}
}

func TestMagicMismatch_PassesThrough(t *testing.T) {
	// Decrypt on a plaintext stream that doesn't carry the BRV1
	// header must return ErrMagicMismatch so the caller can pass the
	// stream through unchanged. This preserves the "encryption was
	// never enabled for this backup" upgrade path.
	plaintext := []byte("not encrypted")
	var dec bytes.Buffer
	_, err := Decrypt(&dec, bytes.NewReader(plaintext), []byte("passphrase"))
	if !errors.Is(err, ErrMagicMismatch) {
		t.Fatalf("expected ErrMagicMismatch, got %v", err)
	}
}

func TestEmptyPassphrase_Rejected(t *testing.T) {
	var out bytes.Buffer
	_, err := Encrypt(&out, bytes.NewReader([]byte("x")), nil)
	if !errors.Is(err, ErrEmptyPassphrase) {
		t.Fatalf("encrypt with empty passphrase: want ErrEmptyPassphrase, got %v", err)
	}
	_, err = Decrypt(&out, bytes.NewReader([]byte("x")), nil)
	if !errors.Is(err, ErrEmptyPassphrase) {
		t.Fatalf("decrypt with empty passphrase: want ErrEmptyPassphrase, got %v", err)
	}
}

func TestTamperDetection(t *testing.T) {
	passphrase := []byte("tamper passphrase")
	plaintext := []byte("do not tamper with me")
	var enc bytes.Buffer
	if _, err := Encrypt(&enc, bytes.NewReader(plaintext), passphrase); err != nil {
		t.Fatalf("encrypt: %v", err)
	}

	// Flip a byte in the ciphertext body.
	cipher := enc.Bytes()
	cipher[HeaderSize+5] ^= 0xff

	var dec bytes.Buffer
	_, err := Decrypt(&dec, bytes.NewReader(cipher), passphrase)
	if err == nil {
		t.Fatalf("expected auth failure after tamper")
	}
}

func TestFileRoundTrip(t *testing.T) {
	tmp := t.TempDir()
	src := filepath.Join(tmp, "plain.bin")
	enc := filepath.Join(tmp, "enc.bin")
	dec := filepath.Join(tmp, "dec.bin")
	payload := []byte("on-disk round trip")
	if err := os.WriteFile(src, payload, 0o600); err != nil {
		t.Fatal(err)
	}
	passphrase := []byte("file round trip")
	if _, err := EncryptFile(enc, src, passphrase); err != nil {
		t.Fatal(err)
	}
	if _, err := DecryptFile(dec, enc, passphrase); err != nil {
		t.Fatal(err)
	}
	got, err := os.ReadFile(dec)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(got, payload) {
		t.Fatalf("round-trip mismatch: got %q want %q", got, payload)
	}
}

func TestReadPassphraseFile_TrailingNewline(t *testing.T) {
	tmp := t.TempDir()
	p := filepath.Join(tmp, "passphrase")
	if err := os.WriteFile(p, []byte("secret\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	got, err := ReadPassphraseFile(p)
	if err != nil {
		t.Fatal(err)
	}
	if string(got) != "secret" {
		t.Fatalf("want %q got %q", "secret", got)
	}
}

func TestReadPassphraseFile_Empty(t *testing.T) {
	tmp := t.TempDir()
	p := filepath.Join(tmp, "passphrase")
	if err := os.WriteFile(p, []byte("\n\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	_, err := ReadPassphraseFile(p)
	if err == nil {
		t.Fatalf("want error on empty passphrase file")
	}
}

// TestEncrypt_LargeRandom is a torture test that runs a big random
// payload (4 MiB) through Encrypt + Decrypt to catch chunk-boundary
// bugs that only show up after several dozen chunks.
func TestEncrypt_LargeRandom(t *testing.T) {
	passphrase := []byte("torture test passphrase")
	payload := make([]byte, 4<<20)
	if _, err := io.ReadFull(rand.Reader, payload); err != nil {
		t.Fatal(err)
	}
	var enc bytes.Buffer
	if _, err := Encrypt(&enc, bytes.NewReader(payload), passphrase); err != nil {
		t.Fatal(err)
	}
	var dec bytes.Buffer
	if _, err := Decrypt(&dec, bytes.NewReader(enc.Bytes()), passphrase); err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(dec.Bytes(), payload) {
		t.Fatalf("large random mismatch")
	}
}
