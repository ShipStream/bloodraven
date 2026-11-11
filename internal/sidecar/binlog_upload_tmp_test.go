package sidecar

import (
	"context"
	"log/slog"
	"path/filepath"
	"testing"
)

// An upload must not depend on a writable $TMPDIR.
func TestUploadWorksWithoutWritableTmp(t *testing.T) {
	ctx := context.Background()
	pvc, err := newPVCStore(testPVCConfig(t.TempDir()), "")
	if err != nil {
		t.Fatal(err)
	}
	t.Setenv("TMPDIR", filepath.Join(t.TempDir(), "absent-read-only-root"))
	a := newTestArchiver(&PITRConfig{ManifestPrefix: "binlogs"}, "iad", &stubRoleChecker{}, pvc, slog.Default())
	file := "testdata/incarnation-a.bin"
	id, err := a.localBinlogID(file)
	if err != nil {
		t.Fatal(err)
	}
	if err := a.archiveOne(ctx, file, "mysql-bin.000001", id); err != nil {
		t.Fatalf("upload fails without writable TMPDIR: %v", err)
	}
}
