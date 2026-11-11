package sidecar

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// putObserverStore reports each object key just before it is written.
type putObserverStore struct {
	ArchiveStore
	onPut func(key string)
}

func (s *putObserverStore) Put(ctx context.Context, key string, r io.Reader, size int64) error {
	if s.onPut != nil {
		s.onPut(key)
	}
	return s.ArchiveStore.Put(ctx, key, r, size)
}

// archivedHistoryFixture is a site whose count sealed binlogs (payload
// each) are all archived, seen by a sidecar that just started: it holds no
// identity of any local file, so the history lane re-identifies them.
func archivedHistoryFixture(t *testing.T, count int, payload []byte) (*BinlogArchiver, *putObserverStore) {
	t.Helper()
	ctx := context.Background()
	dir := t.TempDir()
	header, err := os.CreateTemp(t.TempDir(), "header")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := header.Write(payload); err != nil {
		t.Fatal(err)
	}
	_ = header.Close()
	previous, err := localBinlogHeader(header.Name())
	if err != nil {
		t.Fatal(err)
	}
	base := newMemStore()
	m := &Manifest{Site: "iad"}
	var index []string
	for i := 1; i <= count; i++ {
		name := fmt.Sprintf("mysql-bin.%06d", i)
		if err := os.WriteFile(filepath.Join(dir, name), payload, 0o600); err != nil {
			t.Fatal(err)
		}
		row := unboundRow(name, payload)
		row.Size, row.PreviousGTIDs = int64(len(payload)), previous.previous
		key, err := ArchiveEntryKey("binlogs", "iad", row)
		if err != nil {
			t.Fatal(err)
		}
		if err := base.Put(ctx, key, bytes.NewReader(payload), int64(len(payload))); err != nil {
			t.Fatal(err)
		}
		m.Files = append(m.Files, row)
		index = append(index, name)
	}
	index = append(index, fmt.Sprintf("mysql-bin.%06d", count+1))
	if err := os.WriteFile(filepath.Join(dir, "mysql-bin.index"), []byte(strings.Join(index, "\n")+"\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := seedManifest(ctx, base, "binlogs", m); err != nil {
		t.Fatal(err)
	}
	store := &putObserverStore{ArchiveStore: base}
	cfg := &PITRConfig{BinlogDir: dir, BinlogIndex: "mysql-bin.index", ManifestPrefix: "binlogs"}
	return newTestArchiver(cfg, "iad", &stubRoleChecker{}, store, slog.New(slog.NewTextHandler(io.Discard, nil))), store
}

// seal writes payload as the sealed binlog number and opens number+1.
func sealHistoryFile(t *testing.T, a *BinlogArchiver, number int, payload []byte) string {
	t.Helper()
	name := fmt.Sprintf("mysql-bin.%06d", number)
	if err := os.WriteFile(filepath.Join(a.cfg.BinlogDir, name), payload, 0o600); err != nil {
		t.Fatal(err)
	}
	var index []string
	for i := 1; i <= number+1; i++ {
		index = append(index, fmt.Sprintf("mysql-bin.%06d", i))
	}
	if err := os.WriteFile(filepath.Join(a.cfg.BinlogDir, a.cfg.BinlogIndex), []byte(strings.Join(index, "\n")+"\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	return name
}

// Re-identifying archived local history is bounded per scan, and a newly
// sealed binlog is uploaded before the scan returns to it.
func TestRotationPreemptsColdHistory(t *testing.T) {
	ctx := context.Background()
	payload, err := os.ReadFile("testdata/incarnation-a.bin")
	if err != nil {
		t.Fatal(err)
	}
	a, store := archivedHistoryFixture(t, 16, payload)
	uploads := 0
	store.onPut = func(key string) {
		if strings.HasPrefix(key, "binlogs/iad/") {
			uploads++
		}
	}
	a.scanAndArchive(ctx)
	if st := a.Snapshot(ctx); st.LastError != "" || uploads != 0 {
		t.Fatalf("archived history: lastError %q, %d uploads", st.LastError, uploads)
	}
	if n := len(a.localIDs); n == 0 || n > 8 {
		t.Fatalf("one scan re-identified %d historical files, want a bounded batch of 1..8", n)
	}
	rotated, err := os.ReadFile("testdata/incarnation-b.bin")
	if err != nil {
		t.Fatal(err)
	}
	name := sealHistoryFile(t, a, 17, rotated)
	identifiedAtUpload := -1
	store.onPut = func(key string) {
		if strings.HasPrefix(key, "binlogs/iad/"+name+".") {
			identifiedAtUpload = len(a.localIDs)
		}
	}
	a.scanAndArchive(ctx)
	// The upload identified the rotated file itself.
	if identifiedAtUpload < 0 || identifiedAtUpload-1 >= 16 {
		t.Fatalf("the rotation waited for cold history: %d files identified at its upload", identifiedAtUpload)
	}
	// The history lane finishes over the following scans without uploading.
	for i := 0; i < 4; i++ {
		a.scanAndArchive(ctx)
	}
	if st := a.Snapshot(ctx); st.LastError != "" || st.BacklogFiles != 0 || len(a.localIDs) != 17 {
		t.Fatalf("history not settled: %+v, %d identified", st, len(a.localIDs))
	}
	if m, err := loadManifest(ctx, store, "binlogs", "iad"); err != nil || len(m.Files) != 17 {
		t.Fatalf("manifest: %v, %d rows, want 17", err, len(m.Files))
	}
}

// One historical file larger than a scan's byte budget is identified across
// scans, and a rotation in between is uploaded first.
func TestRotationPreemptsOneOversizedHistoricalRead(t *testing.T) {
	ctx := context.Background()
	header, err := os.ReadFile("testdata/incarnation-a.bin")
	if err != nil {
		t.Fatal(err)
	}
	a, store := archivedHistoryFixture(t, 1, header)
	defer func() {
		if a.historyTask != nil {
			a.historyTask.close()
		}
	}()
	// The archived file grows a sparse tail past the budget; its row keeps
	// the size and header the cheap check compares, so it stays history.
	big := filepath.Join(a.cfg.BinlogDir, "mysql-bin.000001")
	if err := os.Truncate(big, 65<<20); err != nil {
		t.Fatal(err)
	}
	m, err := loadManifest(ctx, store, "binlogs", "iad")
	if err != nil {
		t.Fatal(err)
	}
	m.Files[0].Size = 65 << 20
	if err := seedManifest(ctx, store, "binlogs", m); err != nil {
		t.Fatal(err)
	}
	a.scanAndArchive(ctx)
	if a.historyTask == nil || a.historyTask.reader == nil || len(a.localIDs) != 0 {
		t.Fatalf("the historical read did not yield mid-file: task=%+v identified=%d", a.historyTask, len(a.localIDs))
	}
	rotated, err := os.ReadFile("testdata/incarnation-b.bin")
	if err != nil {
		t.Fatal(err)
	}
	name := sealHistoryFile(t, a, 2, rotated)
	preempted := false
	store.onPut = func(key string) {
		if strings.HasPrefix(key, "binlogs/iad/"+name+".") {
			_, identified := a.localIDs["mysql-bin.000001"]
			preempted = !identified
		}
	}
	a.scanAndArchive(ctx)
	if !preempted {
		t.Fatal("the rotation waited for the oversized historical file")
	}
}
