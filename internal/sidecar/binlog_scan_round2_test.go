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
	"sync/atomic"
	"testing"
	"time"
)

type rotationHistoryStore struct {
	ArchiveStore
	reads    atomic.Int32
	rotate   func()
	uploaded func(int32)
	target   string
}

func (s *rotationHistoryStore) history(key string) {
	if strings.HasPrefix(key, "binlogs/iad/mysql-bin.") && !strings.Contains(filepath.Base(key)[len("mysql-bin."):], ".") {
		if s.reads.Add(1) == 1 {
			s.rotate()
		}
	}
}
func (s *rotationHistoryStore) Open(ctx context.Context, key string) (io.ReadCloser, error) {
	s.history(key)
	data, ok, err := s.ArchiveStore.Get(ctx, key)
	if err != nil {
		return nil, err
	}
	if !ok {
		return nil, os.ErrNotExist
	}
	return io.NopCloser(bytes.NewReader(data)), nil
}
func (s *rotationHistoryStore) GetFile(ctx context.Context, key, dst string) error {
	s.history(key)
	return s.ArchiveStore.GetFile(ctx, key, dst)
}
func (s *rotationHistoryStore) Put(ctx context.Context, key string, r io.Reader, size int64) error {
	err := s.ArchiveStore.Put(ctx, key, r, size)
	if err == nil && key == s.target {
		s.uploaded(s.reads.Load())
	}
	return err
}
func (s *rotationHistoryStore) PutFile(ctx context.Context, key, src string) error {
	err := s.ArchiveStore.PutFile(ctx, key, src)
	if err == nil && key == s.target {
		s.uploaded(s.reads.Load())
	}
	return err
}
func TestRound2RotationPreemptsColdHistory(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	dir := t.TempDir()
	cfg := &PITRConfig{BinlogDir: dir, BinlogIndex: "mysql-bin.index", ManifestPrefix: "binlogs", PollInterval: time.Millisecond}
	base, err := newPVCStore(&PITRPVCConfig{MountPath: t.TempDir()})
	if err != nil {
		t.Fatal(err)
	}
	original, err := os.ReadFile("testdata/incarnation-a.bin")
	if err != nil {
		t.Fatal(err)
	}
	old, err := os.ReadFile("testdata/incarnation-b.bin")
	if err != nil {
		t.Fatal(err)
	}
	meta, err := parseBinlogMetadata("testdata/incarnation-a.bin")
	if err != nil {
		t.Fatal(err)
	}
	manifest := &Manifest{Site: "iad"}
	var names []string
	for i := 1; i <= 16; i++ {
		name := fmt.Sprintf("mysql-bin.%06d", i)
		names = append(names, name)
		if err := os.WriteFile(filepath.Join(dir, name), original, 0600); err != nil {
			t.Fatal(err)
		}
		if err := base.Put(ctx, "binlogs/iad/"+name, bytes.NewReader(original), int64(len(original))); err != nil {
			t.Fatal(err)
		}
		manifest.Files = append(manifest.Files, ManifestEntry{Name: name, Size: meta.Size, PreviousGTIDs: meta.PreviousGTIDs})
	}
	oldID, err := fileContentID("testdata/incarnation-b.bin")
	if err != nil {
		t.Fatal(err)
	}
	manifest.Files = append(manifest.Files, ManifestEntry{Name: "mysql-bin.000017", ID: oldID, PreviousGTIDs: coverageTestSID + ":1-20", Size: int64(len(old))})
	if err := saveManifest(ctx, base, "binlogs", manifest); err != nil {
		t.Fatal(err)
	}
	writeIndex := func(extra string) error {
		return os.WriteFile(filepath.Join(dir, cfg.BinlogIndex), []byte(strings.Join(names, "\n")+"\n"+extra), 0600)
	}
	if err := writeIndex("mysql-bin.000017\n"); err != nil {
		t.Fatal(err)
	}
	id, err := fileContentID("testdata/incarnation-a.bin")
	if err != nil {
		t.Fatal(err)
	}
	result := make(chan int32, 1)
	store := &rotationHistoryStore{ArchiveStore: base, target: "binlogs/iad/mysql-bin.000017." + id}
	store.rotate = func() {
		if err := os.WriteFile(filepath.Join(dir, "mysql-bin.000017"), original, 0600); err != nil {
			panic(err)
		}
		if err := writeIndex("mysql-bin.000017\nmysql-bin.000018\n"); err != nil {
			panic(err)
		}
	}
	store.uploaded = func(reads int32) {
		select {
		case result <- reads:
		default:
		}
		cancel()
	}
	a := NewBinlogArchiver(cfg, "iad", &stubRoleChecker{}, store, slog.Default())
	done := make(chan struct{})
	go func() { defer close(done); a.Run(ctx) }()
	select {
	case reads := <-result:
		if reads >= 16 {
			t.Errorf("rotation waited for all %d historical objects", reads)
		}
	case <-time.After(10 * time.Second):
		t.Error("new known/reused-name rotation was not archived")
		cancel()
	}
	<-done
}

type historyZeroReader struct{}

func (historyZeroReader) Read(p []byte) (int, error) {
	clear(p)
	return len(p), nil
}

type historyBudgetStore struct {
	gate <-chan struct{}

	ArchiveStore
	largeKey       string
	readBytes      atomic.Int64
	manifestWrites int
	failSave       bool
	onPut          func(string)
}
type historyReadCounter struct {
	gate <-chan struct{}

	io.ReadCloser
	count *atomic.Int64
}

func (r *historyReadCounter) Read(p []byte) (int, error) {
	if r.gate != nil && r.count.Load() >= 64<<20 {
		<-r.gate
	}
	n, err := r.ReadCloser.Read(p)
	r.count.Add(int64(n))
	return n, err
}
func (s *historyBudgetStore) Open(ctx context.Context, key string) (io.ReadCloser, error) {
	if key == s.largeKey {
		// Content-ID discovery hashes opaque legacy bytes; replay validation is
		// independent. A virtual object avoids allocating a giant test fixture.
		return &historyReadCounter{ReadCloser: io.NopCloser(io.LimitReader(historyZeroReader{}, 65<<20)), count: &s.readBytes, gate: s.gate}, nil
	}
	return s.ArchiveStore.Open(ctx, key)
}
func (s *historyBudgetStore) Put(ctx context.Context, key string, r io.Reader, size int64) error {
	if key == ManifestKey("binlogs", "iad") {
		s.manifestWrites++
		if s.failSave {
			s.failSave = false
			return fmt.Errorf("injected manifest failure")
		}
	}
	if s.onPut != nil {
		s.onPut(key)
	}
	return s.ArchiveStore.Put(ctx, key, r, size)
}
func historyBudgetFixture(t *testing.T, count int) (*BinlogArchiver, *historyBudgetStore, []byte) {
	t.Helper()
	dir := t.TempDir()
	data, err := os.ReadFile("testdata/incarnation-a.bin")
	if err != nil {
		t.Fatal(err)
	}
	meta, err := parseBinlogMetadata("testdata/incarnation-a.bin")
	if err != nil {
		t.Fatal(err)
	}
	base := newMemStore()
	m := &Manifest{Site: "iad"}
	var index []string
	for i := 1; i <= count; i++ {
		name := fmt.Sprintf("mysql-bin.%06d", i)
		if err := os.WriteFile(filepath.Join(dir, name), data, 0600); err != nil {
			t.Fatal(err)
		}
		if err := base.Put(context.Background(), "binlogs/iad/"+name, bytes.NewReader(data), int64(len(data))); err != nil {
			t.Fatal(err)
		}
		m.Files = append(m.Files, ManifestEntry{Name: name, Size: meta.Size, PreviousGTIDs: meta.PreviousGTIDs})
		index = append(index, name)
	}
	index = append(index, fmt.Sprintf("mysql-bin.%06d", count+1))
	if err := os.WriteFile(filepath.Join(dir, "mysql-bin.index"), []byte(strings.Join(index, "\n")+"\n"), 0600); err != nil {
		t.Fatal(err)
	}
	if err := saveManifest(context.Background(), base, "binlogs", m); err != nil {
		t.Fatal(err)
	}
	store := &historyBudgetStore{ArchiveStore: base}
	return NewBinlogArchiver(&PITRConfig{BinlogDir: dir, BinlogIndex: "mysql-bin.index", ManifestPrefix: "binlogs"}, "iad", &stubRoleChecker{}, store, slog.Default()), store, data
}
func TestRound2RotationPreemptsOneOversizedHistoricalRead(t *testing.T) {
	a, store, data := historyBudgetFixture(t, 1)
	defer func() {
		if a.historyTask != nil {
			a.historyTask.close()
		}
	}()
	store.largeKey = "binlogs/iad/mysql-bin.000001"
	gate := make(chan struct{})
	store.gate = gate
	defer close(gate)
	a.scanAndArchive(context.Background())
	if store.readBytes.Load() <= 0 || store.readBytes.Load() > (64<<20)+(64<<10) || a.historyTask == nil {
		t.Fatalf("historical read did not yield mid-object: read=%d task=%v", store.readBytes.Load(), a.historyTask)
	}
	if err := os.WriteFile(filepath.Join(a.cfg.BinlogDir, "mysql-bin.000002"), data, 0600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(a.cfg.BinlogDir, a.cfg.BinlogIndex), []byte("mysql-bin.000001\nmysql-bin.000002\nmysql-bin.000003\n"), 0600); err != nil {
		t.Fatal(err)
	}
	preempted := false
	store.onPut = func(key string) {
		if strings.HasPrefix(key, "binlogs/iad/mysql-bin.000002.") {
			preempted = store.readBytes.Load() < 65<<20
		}
	}
	a.scanAndArchive(context.Background())
	if !preempted {
		t.Fatal("new rotation waited for the oversized historical object")
	}
}
func TestRound2LegacyIdentitiesBatchAndRetryManifestSave(t *testing.T) {
	for _, fail := range []bool{false, true} {
		t.Run(fmt.Sprint(fail), func(t *testing.T) {
			a, store, _ := historyBudgetFixture(t, 3)
			store.failSave = fail
			a.scanAndArchive(context.Background())
			if store.manifestWrites != 1 {
				t.Fatalf("identity backfill rewrote manifest %d times", store.manifestWrites)
			}
			if fail {
				a.scanAndArchive(context.Background())
			}
			m, err := loadManifest(context.Background(), store, "binlogs", "iad")
			if err != nil {
				t.Fatal(err)
			}
			for _, row := range m.Files {
				if row.ContentID == "" {
					t.Fatal("verified identity was not persisted after retry")
				}
			}
		})
	}
}
