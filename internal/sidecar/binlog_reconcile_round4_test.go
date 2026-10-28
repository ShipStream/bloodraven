package sidecar

import (
	"bytes"
	"context"
	"encoding/binary"
	"hash/crc32"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

type rr4GatedStore struct {
	ArchiveStore
	key              string
	reads            atomic.Int32
	started, release chan struct{}
}
type rr4GatedReader struct {
	io.ReadCloser
	ctx              context.Context
	started, release chan struct{}
	remaining        int
	once             sync.Once
}

func (r *rr4GatedReader) Read(p []byte) (int, error) {
	if r.remaining > 0 {
		n, err := r.ReadCloser.Read(p[:min(len(p), r.remaining)])
		r.remaining -= n
		return n, err
	}
	r.once.Do(func() { close(r.started) })
	select {
	case <-r.release:
		return r.ReadCloser.Read(p)
	case <-r.ctx.Done():
		return 0, r.ctx.Err()
	}
}
func (s *rr4GatedStore) Open(ctx context.Context, key string) (io.ReadCloser, error) {
	r, err := s.ArchiveStore.Open(ctx, key)
	if err == nil && key == s.key && s.reads.Add(1) == 1 {
		return &rr4GatedReader{ReadCloser: r, ctx: ctx, started: s.started, release: s.release, remaining: (64 << 10) + 1}, nil
	}
	return r, err
}
func rr4LargeBinlog(t *testing.T) []byte {
	t.Helper()
	data, err := os.ReadFile("testdata/incarnation-a.bin")
	if err != nil {
		t.Fatal(err)
	}
	out := bytes.Clone(data[:4])
	for off := 4; off < len(data); {
		size := int(binary.LittleEndian.Uint32(data[off+9 : off+13]))
		event := bytes.Clone(data[off : off+size])
		if event[4] == 2 {
			event = append(append(event[:size-4], bytes.Repeat([]byte(" "), 100<<10)...), event[size-4:]...)
		}
		binary.LittleEndian.PutUint32(event[9:13], uint32(len(event)))
		binary.LittleEndian.PutUint32(event[13:17], uint32(len(out)+len(event)))
		binary.LittleEndian.PutUint32(event[len(event)-4:], crc32.ChecksumIEEE(event[:len(event)-4]))
		out = append(out, event...)
		off += size
	}
	if _, err := InspectBinlog(bytes.NewReader(out), time.Time{}); err != nil {
		t.Fatal(err)
	}
	return out
}
func rr4AwaitWork(t *testing.T, a *BinlogArchiver) {
	t.Helper()
	if a.reuseWork != nil {
		select {
		case <-a.reuseWork.done:
		case <-time.After(5 * time.Second):
			t.Fatal("legacy verification failed to drain")
		}
	}
}
func TestRereview4LegacyVerificationPreemptsAndResumesWithoutRetention(t *testing.T) {
	ctx := context.Background()
	base := newMemStore()
	store := &rr4GatedStore{ArchiveStore: base, key: "binlogs/iad/mysql-bin.000001", started: make(chan struct{}), release: make(chan struct{})}
	large := rr4LargeBinlog(t)
	m := &Manifest{Site: "iad"}
	other, err := os.ReadFile("testdata/same-header.bin")
	if err != nil {
		t.Fatal(err)
	}
	for i, fixture := range [][]byte{large, other} {
		row := ManifestEntry{Name: []string{"mysql-bin.000001", "mysql-bin.000002"}[i]}
		key, err := ArchiveEntryKey("binlogs", "iad", row)
		if err != nil {
			t.Fatal(err)
		}
		if err := store.Put(ctx, key, bytes.NewReader(fixture), int64(len(fixture))); err != nil {
			t.Fatal(err)
		}
		m.Files = append(m.Files, row)
	}
	if err := saveManifest(ctx, store, "binlogs", m); err != nil {
		t.Fatal(err)
	}
	cfg := &PITRConfig{ManifestPrefix: "binlogs", BinlogDir: t.TempDir(), BinlogIndex: "mysql-bin.index"}
	a := NewBinlogArchiver(cfg, "iad", &stubRoleChecker{}, store, slog.Default())
	defer func() {
		if a.reuseWork != nil {
			a.reuseWork.close()
		}
	}()
	defer func() {
		select {
		case <-store.release:
		default:
			close(store.release)
		}
	}()
	if err := a.auditArchive(ctx, m); err != nil {
		t.Fatal(err)
	}
	select {
	case <-store.started:
	case <-time.After(5 * time.Second):
		t.Fatal("no background inspection scheduled for all-legacy archive")
	}
	// A newly sealed, disjoint transaction must publish while verification waits.
	data, err := os.ReadFile("testdata/incarnation-b.bin")
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(cfg.BinlogDir, "mysql-bin.000003"), data, 0600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(cfg.BinlogDir, cfg.BinlogIndex), []byte("mysql-bin.000003\nmysql-bin.000004\n"), 0600); err != nil {
		t.Fatal(err)
	}
	a.scanAndArchive(ctx)
	published, err := loadManifest(ctx, store, "binlogs", "iad")
	if err != nil {
		t.Fatal(err)
	}
	if len(published.Files) != 3 {
		t.Fatal("new rotation waited for legacy verification")
	}
	close(store.release)
	rr4AwaitWork(t, a)
	// Simulate a sidecar restart before the scan publishes the worker's result.
	a.reuseWork.close()
	a.reuseWork = nil
	a = NewBinlogArchiver(cfg, "iad", &stubRoleChecker{readOnly: true}, store, slog.Default())
	for attempts := 0; attempts < 12; attempts++ {
		m, err = loadManifest(ctx, store, "binlogs", "iad")
		if err != nil {
			t.Fatal(err)
		}
		_ = a.auditArchive(ctx, m)
		if m.Epoch != nil && m.Epoch.ResetReason != "" {
			return
		}
		rr4AwaitWork(t, a)
	}
	t.Fatal("legacy overlap was not revisited without retention after restart")
}
func TestRereview4AcknowledgementChecksUnrecordedReuse(t *testing.T) {
	ctx := context.Background()
	store := newMemStore()
	m := &Manifest{Site: "iad"}
	for i, name := range []string{"incarnation-a.bin", "same-header.bin"} {
		data, err := os.ReadFile("testdata/" + name)
		if err != nil {
			t.Fatal(err)
		}
		row := ManifestEntry{Name: []string{"mysql-bin.000001", "mysql-bin.000002"}[i]}
		key, err := ArchiveEntryKey("binlogs", "iad", row)
		if err != nil {
			t.Fatal(err)
		}
		if err := store.Put(ctx, key, bytes.NewReader(data), int64(len(data))); err != nil {
			t.Fatal(err)
		}
		m.Files = append(m.Files, row)
	}
	if err := saveManifest(ctx, store, "binlogs", m); err != nil {
		t.Fatal(err)
	}
	if err := AcknowledgeArchiveHistory(ctx, store, "binlogs", "iad", "validate legacy guard", coverageTestSID+":1-11"); err == nil || !strings.Contains(err.Error(), "reuse") {
		t.Fatalf("unrecorded conflicting issuance acknowledged: %v", err)
	}
}

func TestRereview4FailedIssuanceInspectionCoolsDown(t *testing.T) {
	ctx := context.Background()
	store := &retentionRound2Store{ArchiveStore: newMemStore(), reads: make(map[string]int)}
	data, err := os.ReadFile("testdata/incarnation-a.bin")
	if err != nil {
		t.Fatal(err)
	}
	row := ManifestEntry{Name: "mysql-bin.000001", ContentID: strings.Repeat("c", 64)}
	peer := ManifestEntry{Name: "mysql-bin.000002", ID: strings.Repeat("d", 64), GTIDCoverage: &BinlogGTIDCoverage{ContentID: strings.Repeat("d", 64)}}
	key, err := ArchiveEntryKey("binlogs", "iad", row)
	if err != nil {
		t.Fatal(err)
	}
	if err := store.Put(ctx, key, bytes.NewReader(data), int64(len(data))); err != nil {
		t.Fatal(err)
	}
	m := &Manifest{Site: "iad", Files: []ManifestEntry{row, peer}}
	a := NewBinlogArchiver(&PITRConfig{ManifestPrefix: "binlogs"}, "iad", &stubRoleChecker{}, store, slog.Default())
	if err := a.reconcileArchiveReuse(ctx, m); err == nil {
		t.Fatal("invalid legacy identity was accepted")
	}
	for i := 0; i < 5; i++ {
		_ = a.reconcileArchiveReuse(ctx, m)
	}
	if store.reads[key] != 1 || !a.archiveReusePending(m) {
		t.Fatalf("failed proof hot-looped or allowed retirement: reads=%d", store.reads[key])
	}
	for key := range a.reuseFailed {
		a.reuseFailed[key] = time.Now().Add(-time.Minute)
	}
	_ = a.reconcileArchiveReuse(ctx, m)
	if store.reads[key] != 2 || m.Epoch != nil {
		t.Fatal("failed proof did not retry safely after cooldown")
	}
}
