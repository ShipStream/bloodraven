package sidecar

import (
	"context"
	"os"
	"testing"
	"time"
)

// TestManifestRoundTripPVC covers the load/append/prune cycle against
// the local filesystem backend. The PVC path is the easier of the two
// to test without a mock — it's just io to a temp dir.
func TestManifestRoundTripPVC(t *testing.T) {
	tmp := t.TempDir()
	store, err := newPVCStore(&PITRPVCConfig{MountPath: tmp})
	if err != nil {
		t.Fatalf("newPVCStore: %v", err)
	}

	ctx := context.Background()
	prefix := "binlogs"
	site := "us-east-1a"

	// Empty read returns an empty manifest, not an error.
	m, err := loadManifest(ctx, store, prefix, site)
	if err != nil {
		t.Fatalf("loadManifest empty: %v", err)
	}
	if len(m.Files) != 0 {
		t.Errorf("want 0 files, got %d", len(m.Files))
	}
	if m.Site != site {
		t.Errorf("want site %q, got %q", site, m.Site)
	}

	// Append two entries; the second overwrites the first if same name.
	now := time.Now().UTC().Truncate(time.Second)
	e1 := ManifestEntry{Name: "mysql-bin.000001", RemotePath: "binlogs/us-east-1a/mysql-bin.000001", Size: 100, FirstEventTime: now, LastEventTime: now, ArchivedAt: now}
	e2 := ManifestEntry{Name: "mysql-bin.000002", RemotePath: "binlogs/us-east-1a/mysql-bin.000002", Size: 200, FirstEventTime: now.Add(time.Minute), LastEventTime: now.Add(2 * time.Minute), ArchivedAt: now}

	if err := appendManifestEntry(ctx, store, prefix, site, e1); err != nil {
		t.Fatalf("append e1: %v", err)
	}
	if err := appendManifestEntry(ctx, store, prefix, site, e2); err != nil {
		t.Fatalf("append e2: %v", err)
	}

	// Re-appending e1 with updated size must replace, not duplicate.
	e1b := e1
	e1b.Size = 150
	if err := appendManifestEntry(ctx, store, prefix, site, e1b); err != nil {
		t.Fatalf("re-append e1: %v", err)
	}

	m, err = loadManifest(ctx, store, prefix, site)
	if err != nil {
		t.Fatalf("loadManifest after append: %v", err)
	}
	if got := len(m.Files); got != 2 {
		t.Fatalf("want 2 files, got %d", got)
	}
	// Files are sorted by name on write.
	if m.Files[0].Name != "mysql-bin.000001" || m.Files[0].Size != 150 {
		t.Errorf("want updated e1 size=150, got %+v", m.Files[0])
	}
	if m.Files[1].Name != "mysql-bin.000002" {
		t.Errorf("want mysql-bin.000002 second, got %s", m.Files[1].Name)
	}

	// Prune drops e1, returns its remote path for delete, and leaves
	// the on-disk manifest with just e2.
	removed, err := pruneManifest(ctx, store, prefix, site,
		map[string]struct{}{"mysql-bin.000001": {}})
	if err != nil {
		t.Fatalf("prune: %v", err)
	}
	if len(removed) != 1 || removed[0] != e1.RemotePath {
		t.Errorf("want removed=[%s], got %v", e1.RemotePath, removed)
	}
	m, err = loadManifest(ctx, store, prefix, site)
	if err != nil {
		t.Fatalf("loadManifest after prune: %v", err)
	}
	if len(m.Files) != 1 || m.Files[0].Name != "mysql-bin.000002" {
		t.Errorf("want only e2 after prune, got %+v", m.Files)
	}

	// Manifest file must be on disk where we expect it.
	if _, err := os.Stat(tmp + "/binlogs/manifest-us-east-1a.json"); err != nil {
		t.Errorf("manifest file missing: %v", err)
	}
}

// TestReadBinlogIndex covers the "drop last entry is active, sort the
// rest" behavior of readBinlogIndex against a synthetic index file.
func TestReadBinlogIndex(t *testing.T) {
	dir := t.TempDir()
	path := dir + "/mysql-bin.index"
	contents := "/var/lib/mysql/mysql-bin.000003\n" +
		"/var/lib/mysql/mysql-bin.000001\n" +
		"/var/lib/mysql/mysql-bin.000002\n"
	if err := os.WriteFile(path, []byte(contents), 0o644); err != nil {
		t.Fatalf("write index: %v", err)
	}
	names, err := readBinlogIndex(path)
	if err != nil {
		t.Fatalf("readBinlogIndex: %v", err)
	}
	// Sorted lexically (which matches numeric order for zero-padded names).
	want := []string{
		"/var/lib/mysql/mysql-bin.000001",
		"/var/lib/mysql/mysql-bin.000002",
		"/var/lib/mysql/mysql-bin.000003",
	}
	if len(names) != len(want) {
		t.Fatalf("want %d lines, got %d: %v", len(want), len(names), names)
	}
	for i := range names {
		if names[i] != want[i] {
			t.Errorf("want [%d]=%q, got %q", i, want[i], names[i])
		}
	}
}

// TestReadBinlogIndexMissing returns (nil, nil) when the index file
// doesn't exist — before MySQL first rotates, there is no index yet.
func TestReadBinlogIndexMissing(t *testing.T) {
	names, err := readBinlogIndex(t.TempDir() + "/nope.index")
	if err != nil {
		t.Fatalf("want nil err, got %v", err)
	}
	if names != nil {
		t.Errorf("want nil names, got %v", names)
	}
}
