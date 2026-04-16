package sidecar

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"
)

// stubRoleChecker implements roleChecker so the archiver can be driven
// without a live MySQL. isReadOnlyErr / readOnly are read each call so
// tests can flip them between scans.
type stubRoleChecker struct {
	mu             sync.Mutex
	readOnly       bool
	isReadOnlyErr  error
	connectableVal bool
}

func (s *stubRoleChecker) IsReadOnly(_ context.Context) (bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.readOnly, s.isReadOnlyErr
}

func (s *stubRoleChecker) isConnectable(_ context.Context) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.connectableVal
}

// memStore is an in-memory archiveStore used by the archiver tests. It
// is intentionally small — only Put / Get / PutFile / Delete / List are
// exercised by the archive path.
type memStore struct {
	mu      sync.Mutex
	objects map[string][]byte
	// putFileErr lets a test cause a specific key's PutFile to fail,
	// used for exercising the upload-failure counter.
	putFileErr map[string]error
}

func newMemStore() *memStore {
	return &memStore{objects: map[string][]byte{}, putFileErr: map[string]error{}}
}

func (m *memStore) Put(_ context.Context, key string, r io.Reader, _ int64) error {
	b, err := io.ReadAll(r)
	if err != nil {
		return err
	}
	m.mu.Lock()
	m.objects[key] = b
	m.mu.Unlock()
	return nil
}

func (m *memStore) Get(_ context.Context, key string) ([]byte, bool, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	b, ok := m.objects[key]
	if !ok {
		return nil, false, nil
	}
	return append([]byte(nil), b...), true, nil
}

func (m *memStore) GetFile(_ context.Context, _, _ string) error { return errors.New("not used") }

func (m *memStore) Delete(_ context.Context, key string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	delete(m.objects, key)
	return nil
}

func (m *memStore) PutFile(_ context.Context, key, path string) error {
	m.mu.Lock()
	if err, ok := m.putFileErr[key]; ok {
		m.mu.Unlock()
		return err
	}
	m.mu.Unlock()
	b, err := os.ReadFile(path)
	if err != nil {
		return err
	}
	m.mu.Lock()
	m.objects[key] = b
	m.mu.Unlock()
	return nil
}

func (m *memStore) List(_ context.Context, prefix string) ([]string, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	var out []string
	for k := range m.objects {
		if strings.HasPrefix(k, prefix) {
			out = append(out, k)
		}
	}
	return out, nil
}

// writeBinlogFile writes a minimal binlog header so parseBinlogMetadata
// doesn't choke. The archiver's metadata parse is permissive enough to
// accept a file with just the 4-byte magic + FDE frame; for the
// aggregate tests we only care that the file is uploadable and the
// manifest entry records a plausible Size.
func writeBinlogFile(t *testing.T, path string, size int) {
	t.Helper()
	buf := &bytes.Buffer{}
	// Binlog magic number \xfe bin + placeholder header bytes.
	buf.Write([]byte{0xfe, 0x62, 0x69, 0x6e})
	// Pad to requested size.
	if size > 4 {
		buf.Write(bytes.Repeat([]byte{0}, size-4))
	}
	if err := os.WriteFile(path, buf.Bytes(), 0o600); err != nil {
		t.Fatalf("write %s: %v", path, err)
	}
}

// seedArchiverWithManifest bypasses archiveOne (which requires a valid
// binlog format event) and pre-populates the manifest + backing storage
// so updateManifestAggregates can be tested deterministically.
func seedArchiverWithManifest(t *testing.T, a *BinlogArchiver, files []ManifestEntry) {
	t.Helper()
	m := &Manifest{Version: ManifestVersion, Site: a.site, Files: files}
	buf, err := json.Marshal(m)
	if err != nil {
		t.Fatalf("marshal manifest: %v", err)
	}
	if err := a.store.Put(context.Background(), manifestKey(a.cfg.ManifestPrefix, a.site), bytes.NewReader(buf), int64(len(buf))); err != nil {
		t.Fatalf("seed manifest: %v", err)
	}
}

func TestUpdateManifestAggregates(t *testing.T) {
	t0 := time.Date(2026, 4, 10, 10, 0, 0, 0, time.UTC)
	files := []ManifestEntry{
		{Name: "mysql-bin.000001", Size: 100, FirstEventTime: t0, LastEventTime: t0.Add(time.Minute)},
		{Name: "mysql-bin.000002", Size: 250, FirstEventTime: t0.Add(time.Minute), LastEventTime: t0.Add(3 * time.Minute)},
		{Name: "mysql-bin.000003", Size: 50, FirstEventTime: t0.Add(3 * time.Minute), LastEventTime: t0.Add(5 * time.Minute)},
	}
	a := &BinlogArchiver{}
	a.updateManifestAggregates(files)

	if got := a.manifestFileCount; got != 3 {
		t.Errorf("manifestFileCount = %d, want 3", got)
	}
	if got := a.manifestBytes; got != 400 {
		t.Errorf("manifestBytes = %d, want 400", got)
	}
	if !a.oldestArchivedTime.Equal(t0) {
		t.Errorf("oldestArchivedTime = %v, want %v", a.oldestArchivedTime, t0)
	}
	if want := t0.Add(5 * time.Minute); !a.newestArchivedTime.Equal(want) {
		t.Errorf("newestArchivedTime = %v, want %v", a.newestArchivedTime, want)
	}
}

func TestUpdateManifestAggregatesEmpty(t *testing.T) {
	a := &BinlogArchiver{}
	a.updateManifestAggregates(nil)
	if a.manifestFileCount != 0 || a.manifestBytes != 0 {
		t.Errorf("empty manifest should yield zero aggregates, got count=%d bytes=%d", a.manifestFileCount, a.manifestBytes)
	}
	if !a.oldestArchivedTime.IsZero() || !a.newestArchivedTime.IsZero() {
		t.Errorf("empty manifest should leave times zero, got oldest=%v newest=%v", a.oldestArchivedTime, a.newestArchivedTime)
	}
}

// TestScanRecordsFailuresOnRoleError exercises the error path:
// IsReadOnly fails → uploadFailures increments, lastError is populated.
func TestScanRecordsFailuresOnRoleError(t *testing.T) {
	dir := t.TempDir()
	store := newMemStore()
	checker := &stubRoleChecker{isReadOnlyErr: errors.New("mysql down")}
	cfg := &PITRConfig{
		StorageType:    "PVC",
		BinlogDir:      dir,
		BinlogIndex:    "mysql-bin.index",
		ManifestPrefix: "binlogs",
		PollInterval:   time.Hour,
	}
	a := NewBinlogArchiver(cfg, "dc1", checker, store, slog.Default())

	a.scanAndArchive(context.Background())

	snap := a.Snapshot(context.Background())
	if snap.UploadFailures != 1 {
		t.Errorf("UploadFailures = %d, want 1", snap.UploadFailures)
	}
	if !strings.Contains(snap.LastError, "role check") {
		t.Errorf("LastError = %q, want role check error", snap.LastError)
	}
}

// TestScanClearsBacklogOnStandby: a standby archiver should zero out
// backlog (nothing for it to archive, even if sealed files exist).
func TestScanClearsBacklogOnStandby(t *testing.T) {
	dir := t.TempDir()
	store := newMemStore()
	checker := &stubRoleChecker{readOnly: true}
	cfg := &PITRConfig{
		StorageType:    "PVC",
		BinlogDir:      dir,
		BinlogIndex:    "mysql-bin.index",
		ManifestPrefix: "binlogs",
		PollInterval:   time.Hour,
	}
	a := NewBinlogArchiver(cfg, "dc1", checker, store, slog.Default())
	// Force non-zero backlog before scan.
	a.setBacklog(5)

	a.scanAndArchive(context.Background())

	if got := a.Snapshot(context.Background()).BacklogFiles; got != 0 {
		t.Errorf("standby should clear backlog, got %d", got)
	}
}

// TestScanBacklogReflectsPendingFiles: primary with 3 sealed binlogs
// and none archived yet should report backlog=3 after the scan fails
// to upload them.
func TestScanBacklogReflectsPendingFiles(t *testing.T) {
	dir := t.TempDir()
	indexPath := filepath.Join(dir, "mysql-bin.index")
	// Index contains: 3 sealed + 1 active (last line is active).
	for i, name := range []string{"mysql-bin.000001", "mysql-bin.000002", "mysql-bin.000003"} {
		writeBinlogFile(t, filepath.Join(dir, name), 64)
		_ = i
	}
	writeBinlogFile(t, filepath.Join(dir, "mysql-bin.000004"), 64)
	idx := []byte("mysql-bin.000001\nmysql-bin.000002\nmysql-bin.000003\nmysql-bin.000004\n")
	if err := os.WriteFile(indexPath, idx, 0o600); err != nil {
		t.Fatal(err)
	}

	store := newMemStore()
	// Cause every upload to fail so we can observe the backlog reflecting
	// all sealed-but-not-archived files.
	store.putFileErr["binlogs/dc1/mysql-bin.000001"] = errors.New("upload failed")
	store.putFileErr["binlogs/dc1/mysql-bin.000002"] = errors.New("upload failed")
	store.putFileErr["binlogs/dc1/mysql-bin.000003"] = errors.New("upload failed")

	checker := &stubRoleChecker{readOnly: false}
	cfg := &PITRConfig{
		StorageType:    "PVC",
		BinlogDir:      dir,
		BinlogIndex:    "mysql-bin.index",
		ManifestPrefix: "binlogs",
		PollInterval:   time.Hour,
	}
	a := NewBinlogArchiver(cfg, "dc1", checker, store, slog.Default())

	a.scanAndArchive(context.Background())

	snap := a.Snapshot(context.Background())
	if snap.BacklogFiles != 3 {
		t.Errorf("BacklogFiles = %d, want 3 (all uploads failed, 3 sealed)", snap.BacklogFiles)
	}
	if snap.UploadFailures != 3 {
		t.Errorf("UploadFailures = %d, want 3", snap.UploadFailures)
	}
	// Aggregates reflect the manifest (still empty), not the index.
	if snap.ManifestFileCount != 0 {
		t.Errorf("ManifestFileCount = %d, want 0", snap.ManifestFileCount)
	}
}

// TestSnapshotExposesSeededManifestAggregates verifies the aggregates
// flow end-to-end through a scan cycle when the manifest is
// pre-populated and no new files need archiving.
func TestSnapshotExposesSeededManifestAggregates(t *testing.T) {
	dir := t.TempDir()
	indexPath := filepath.Join(dir, "mysql-bin.index")
	// Two sealed files, both already in the manifest, plus one active.
	for _, name := range []string{"mysql-bin.000001", "mysql-bin.000002", "mysql-bin.000003"} {
		writeBinlogFile(t, filepath.Join(dir, name), 64)
	}
	idx := []byte("mysql-bin.000001\nmysql-bin.000002\nmysql-bin.000003\n")
	if err := os.WriteFile(indexPath, idx, 0o600); err != nil {
		t.Fatal(err)
	}

	store := newMemStore()
	checker := &stubRoleChecker{readOnly: false}
	cfg := &PITRConfig{
		StorageType:    "PVC",
		BinlogDir:      dir,
		BinlogIndex:    "mysql-bin.index",
		ManifestPrefix: "binlogs",
		PollInterval:   time.Hour,
	}
	a := NewBinlogArchiver(cfg, "dc1", checker, store, slog.Default())

	t0 := time.Date(2026, 4, 10, 10, 0, 0, 0, time.UTC)
	seedArchiverWithManifest(t, a, []ManifestEntry{
		{Name: "mysql-bin.000001", Size: 100, FirstEventTime: t0, LastEventTime: t0.Add(time.Minute), ArchivedAt: t0},
		{Name: "mysql-bin.000002", Size: 200, FirstEventTime: t0.Add(time.Minute), LastEventTime: t0.Add(2 * time.Minute), ArchivedAt: t0},
	})

	a.scanAndArchive(context.Background())

	snap := a.Snapshot(context.Background())
	if snap.ManifestFileCount != 2 {
		t.Errorf("ManifestFileCount = %d, want 2", snap.ManifestFileCount)
	}
	if snap.ManifestBytes != 300 {
		t.Errorf("ManifestBytes = %d, want 300", snap.ManifestBytes)
	}
	if !snap.OldestArchivedTime.Equal(t0) {
		t.Errorf("OldestArchivedTime = %v, want %v", snap.OldestArchivedTime, t0)
	}
	if want := t0.Add(2 * time.Minute); !snap.NewestArchivedTime.Equal(want) {
		t.Errorf("NewestArchivedTime = %v, want %v", snap.NewestArchivedTime, want)
	}
	if snap.BacklogFiles != 0 {
		t.Errorf("BacklogFiles = %d, want 0 (all sealed files in manifest)", snap.BacklogFiles)
	}
}
