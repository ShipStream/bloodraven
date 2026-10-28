package sidecar

import (
	"context"
	"log/slog"
	"os"
	"path/filepath"
	"testing"
)

// R9 repair: the legacy row with a missing object stays beside the new
// <name>.<id> row, and both carry the same Previous_gtids.
func TestRereviewProbeMissingLegacyRowIsReplaced(t *testing.T) {
	ctx := context.Background()
	local := t.TempDir()
	cfg := &PITRConfig{BinlogDir: local, BinlogIndex: "mysql-bin.index", ManifestPrefix: "binlogs"}
	pvc, err := newPVCStore(&PITRPVCConfig{MountPath: t.TempDir()})
	if err != nil {
		t.Fatal(err)
	}
	payload, err := os.ReadFile("testdata/incarnation-a.bin")
	if err != nil {
		t.Fatal(err)
	}
	name := "mysql-bin.000001"
	if err := os.WriteFile(filepath.Join(local, name), payload, 0600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(local, cfg.BinlogIndex), []byte(name+"\nmysql-bin.000002\n"), 0600); err != nil {
		t.Fatal(err)
	}
	meta, err := parseBinlogMetadata(filepath.Join(local, name))
	if err != nil {
		t.Fatal(err)
	}
	digest, err := fileContentID(filepath.Join(local, name))
	if err != nil {
		t.Fatal(err)
	}
	legacy := ManifestEntry{ContentID: digest, Name: name, RemotePath: "binlogs/iad/" + name, PreviousGTIDs: meta.PreviousGTIDs, FirstEventTime: meta.FirstEventTime}
	if err := saveManifest(ctx, pvc, "binlogs", &Manifest{Site: "iad", Files: []ManifestEntry{legacy}}); err != nil {
		t.Fatal(err)
	}
	NewBinlogArchiver(cfg, "iad", &stubRoleChecker{}, pvc, slog.Default()).scanAndArchive(ctx)
	m, err := loadManifest(ctx, pvc, "binlogs", "iad")
	if err != nil {
		t.Fatal(err)
	}
	for _, f := range m.Files {
		t.Logf("row name=%s id=%.12s prev=%q", f.Name, f.ID, f.PreviousGTIDs)
	}
	if len(m.Files) != 1 || m.Files[0].ID != digest {
		t.Fatalf("missing legacy row kept beside its repair: %d rows with the same name and predecessors", len(m.Files))
	}
}

// Every upload now needs a writable $TMPDIR large enough for one binlog.
func TestRereviewProbeUploadNeedsWritableTmp(t *testing.T) {
	t.Setenv("TMPDIR", filepath.Join(t.TempDir(), "absent-read-only-root"))
	ctx := context.Background()
	pvc, err := newPVCStore(&PITRPVCConfig{MountPath: t.TempDir()})
	if err != nil {
		t.Fatal(err)
	}
	a := NewBinlogArchiver(&PITRConfig{ManifestPrefix: "binlogs"}, "iad", &stubRoleChecker{}, pvc, slog.Default())
	file := "testdata/incarnation-a.bin"
	id, err := a.localBinlogID(file)
	if err != nil {
		t.Fatal(err)
	}
	if err := a.archiveOne(ctx, file, "mysql-bin.000001", id); err != nil {
		t.Fatalf("upload fails without writable TMPDIR: %v", err)
	}
}
