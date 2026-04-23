package main

import (
	"bytes"
	"context"
	"os"
	"path"
	"path/filepath"
	"runtime"
	"strings"
	"testing"

	"github.com/shipstream/bloodraven/internal/sidecar"
)

// TestSafeJoinUnderRoot_Valid exercises the happy path for storage keys
// that resolve cleanly under the staging directory.
func TestSafeJoinUnderRoot_Valid(t *testing.T) {
	root := t.TempDir()
	cases := []string{
		"file.txt",
		"dir/file.txt",
		"deep/nested/path/file.bin",
	}
	for _, rel := range cases {
		got, err := safeJoinUnderRoot(root, rel)
		if err != nil {
			t.Errorf("safeJoinUnderRoot(%q, %q) unexpected err: %v", root, rel, err)
			continue
		}
		if !strings.HasPrefix(got, root) {
			t.Errorf("safeJoinUnderRoot(%q, %q) = %q, want prefix %q", root, rel, got, root)
		}
	}
}

// TestSafeJoinUnderRoot_Traversal covers the H1 defence: an S3 key can
// legally contain "../" segments, and the decrypt-download path must
// refuse to write outside its staging dir.
func TestSafeJoinUnderRoot_Traversal(t *testing.T) {
	root := t.TempDir()
	cases := []string{
		"../evil",
		"a/../../evil",
		"../../../etc/passwd",
	}
	for _, rel := range cases {
		if _, err := safeJoinUnderRoot(root, rel); err == nil {
			t.Errorf("safeJoinUnderRoot(%q, %q) should have failed", root, rel)
		}
	}
}

func TestSafeJoinUnderRoot_AbsolutePath(t *testing.T) {
	root := t.TempDir()
	abs := "/etc/passwd"
	if runtime.GOOS == "windows" {
		abs = `C:\Windows\System32\evil`
	}
	if _, err := safeJoinUnderRoot(root, abs); err == nil {
		t.Errorf("safeJoinUnderRoot(%q, %q) should have rejected absolute path", root, abs)
	}
}

func TestSafeJoinUnderRoot_NullByte(t *testing.T) {
	root := t.TempDir()
	if _, err := safeJoinUnderRoot(root, "a\x00b"); err == nil {
		t.Errorf("safeJoinUnderRoot should reject NUL bytes")
	}
}

// TestStorageConfigFromEnv_S3 covers the happy path for S3 config
// parsing; a missing bucket must fail fast so the Job doesn't
// silently reach for ambient AWS credentials.
func TestStorageConfigFromEnv_S3(t *testing.T) {
	t.Setenv("BLOODRAVEN_S3_BUCKET", "my-bucket")
	t.Setenv("AWS_REGION", "us-east-1")
	t.Setenv("BLOODRAVEN_AWS_CREDS_DIR", "/var/run/secrets/aws")
	cfg, prefix, display, err := storageConfigFromEnv("S3", "orders/backup-1/")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if cfg.S3 == nil || cfg.S3.Bucket != "my-bucket" {
		t.Errorf("S3 config missing or wrong bucket: %+v", cfg.S3)
	}
	if prefix != "orders/backup-1" {
		t.Errorf("prefix = %q, want %q", prefix, "orders/backup-1")
	}
	if !strings.HasPrefix(display, "s3://my-bucket/") {
		t.Errorf("display = %q, want s3:// prefix", display)
	}
}

func TestStorageConfigFromEnv_S3_MissingBucket(t *testing.T) {
	t.Setenv("BLOODRAVEN_S3_BUCKET", "")
	if _, _, _, err := storageConfigFromEnv("S3", "orders/"); err == nil {
		t.Errorf("expected error for missing BLOODRAVEN_S3_BUCKET")
	}
}

func TestStorageConfigFromEnv_PVC(t *testing.T) {
	mount := t.TempDir()
	t.Setenv("BLOODRAVEN_PVC_MOUNT_PATH", mount)
	outputURL := filepath.Join(mount, "orders", "backup-1")
	cfg, prefix, display, err := storageConfigFromEnv("PVC", outputURL)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if cfg.PVC == nil || cfg.PVC.MountPath != mount {
		t.Errorf("PVC config missing or wrong mount: %+v", cfg.PVC)
	}
	if prefix != "orders/backup-1" {
		t.Errorf("prefix = %q, want %q", prefix, "orders/backup-1")
	}
	if display != outputURL {
		t.Errorf("display = %q, want %q", display, outputURL)
	}
}

func TestStorageConfigFromEnv_PVC_OutsideMount(t *testing.T) {
	mount := t.TempDir()
	t.Setenv("BLOODRAVEN_PVC_MOUNT_PATH", mount)
	// A path outside the mount must be rejected.
	if _, _, _, err := storageConfigFromEnv("PVC", "/etc/passwd"); err == nil {
		t.Errorf("expected error for outputURL outside PVC mount")
	}
}

func TestStorageConfigFromEnv_UnknownType(t *testing.T) {
	if _, _, _, err := storageConfigFromEnv("gcs", "some/prefix"); err == nil {
		t.Errorf("expected error for unknown storage type")
	}
}

// TestEncryptAndUploadRoundTrip covers AUDIT M12: end-to-end the
// encrypt-upload → decrypt-download boundary on a PVC-backed store.
// Proves that ciphertext produced by one half of the data plane is
// fully readable by the other, with matching plaintext and verified
// per-file sha256 digests.
func TestEncryptAndUploadRoundTrip(t *testing.T) {
	ctx := context.Background()
	bucket := t.TempDir()
	passphrase := []byte("m12 round trip passphrase")
	cfg := &sidecar.PITRConfig{
		StorageType: "PVC",
		PVC:         &sidecar.PITRPVCConfig{MountPath: bucket},
	}
	rawStore, err := sidecar.NewArchiveStore(ctx, cfg)
	if err != nil {
		t.Fatal(err)
	}
	// Wrap in encryption explicitly — the CLI path would have threaded
	// the passphrase file, but for the test we drive the store layer.
	store := sidecar.WrapWithEncryption(rawStore, passphrase)

	// Stage a small plaintext dump tree.
	src := t.TempDir()
	files := map[string][]byte{
		"dump.json":             []byte(`{"k":"v"}`),
		"schemas/foo.sql":       []byte("CREATE TABLE foo (id INT);\n"),
		"schemas/bar.sql":       bytes.Repeat([]byte("abc"), 3000), // spans GCM chunk boundary
		"BLOODRAVEN_DUMP_META.json": []byte(`{"gtidExecuted":""}`),
	}
	for rel, body := range files {
		full := filepath.Join(src, rel)
		if err := os.MkdirAll(filepath.Dir(full), 0o755); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(full, body, 0o600); err != nil {
			t.Fatal(err)
		}
	}

	keyPrefix := "orders/backup-1"
	totalPT, totalCT, nfiles, digests, err := encryptAndUpload(ctx, store, keyPrefix, src, passphrase)
	if err != nil {
		t.Fatalf("encryptAndUpload: %v", err)
	}
	// dumpMeta is skipped by the uploader.
	if nfiles != len(files)-1 {
		t.Errorf("nfiles = %d, want %d", nfiles, len(files)-1)
	}
	if totalPT == 0 || totalCT == 0 {
		t.Errorf("totals should be non-zero, got pt=%d ct=%d", totalPT, totalCT)
	}
	if totalCT <= totalPT {
		t.Errorf("expected ciphertext > plaintext (AES-GCM header + tags), got pt=%d ct=%d", totalPT, totalCT)
	}
	if len(digests) != nfiles {
		t.Errorf("expected %d digests, got %d", nfiles, len(digests))
	}

	// Now round-trip via downloadAndDecrypt and verify plaintext
	// matches the original.
	dst := t.TempDir()
	for rel, want := range files {
		if rel == dumpMetaFileName {
			continue
		}
		key := path.Join(keyPrefix, rel)
		out := filepath.Join(dst, filepath.FromSlash(rel))
		if err := os.MkdirAll(filepath.Dir(out), 0o755); err != nil {
			t.Fatal(err)
		}
		if _, err := downloadAndDecrypt(ctx, store, key, out, passphrase, false); err != nil {
			t.Fatalf("downloadAndDecrypt %s: %v", key, err)
		}
		got, err := os.ReadFile(out)
		if err != nil {
			t.Fatal(err)
		}
		if !bytes.Equal(got, want) {
			t.Errorf("round-trip mismatch for %s", rel)
		}
		// sha256 digest is tracked in the manifest map.
		if digests[rel] == "" {
			t.Errorf("missing digest for %s", rel)
		}
		actual, err := sha256File(out)
		if err != nil {
			t.Fatal(err)
		}
		if actual != digests[rel] {
			t.Errorf("sha256 mismatch for %s: got %s want %s", rel, actual, digests[rel])
		}
	}
}

// TestEncryptAndUploadRejectsSymlink covers the walk-level symlink
// guard: a crafted symlink under the staging emptyDir would otherwise
// be followed by the downstream os.Open, letting an attacker who can
// plant a file in the dump directory exfiltrate arbitrary container-
// reachable files. The walk rejects any non-regular entry.
func TestEncryptAndUploadRejectsSymlink(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("symlink semantics differ on Windows")
	}
	ctx := context.Background()
	bucket := t.TempDir()
	passphrase := []byte("symlink rejection passphrase")
	cfg := &sidecar.PITRConfig{
		StorageType: "PVC",
		PVC:         &sidecar.PITRPVCConfig{MountPath: bucket},
	}
	rawStore, err := sidecar.NewArchiveStore(ctx, cfg)
	if err != nil {
		t.Fatal(err)
	}
	store := sidecar.WrapWithEncryption(rawStore, passphrase)

	src := t.TempDir()
	if err := os.WriteFile(filepath.Join(src, "dump.json"), []byte(`{}`), 0o600); err != nil {
		t.Fatal(err)
	}
	// Plant a symlink pointing outside the staging dir.
	outside := filepath.Join(t.TempDir(), "secret.txt")
	if err := os.WriteFile(outside, []byte("ssh-private-key"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink(outside, filepath.Join(src, "link")); err != nil {
		t.Fatal(err)
	}

	_, _, _, _, err = encryptAndUpload(ctx, store, "x/y", src, passphrase)
	if err == nil {
		t.Fatalf("expected encryptAndUpload to reject symlink, got nil")
	}
	if !strings.Contains(err.Error(), "non-regular") {
		t.Errorf("error %q should mention non-regular file rejection", err.Error())
	}
}

func TestIsSafeBasename(t *testing.T) {
	good := []string{"mysql-bin.000123", "a", "file_with_underscore.log"}
	for _, n := range good {
		if !isSafeBasename(n) {
			t.Errorf("isSafeBasename(%q) = false, want true", n)
		}
	}
	bad := []string{"", ".", "..", "a/b", "a\\b", "a\x00b", "\tleading-tab"}
	for _, n := range bad {
		if isSafeBasename(n) {
			t.Errorf("isSafeBasename(%q) = true, want false", n)
		}
	}
}
