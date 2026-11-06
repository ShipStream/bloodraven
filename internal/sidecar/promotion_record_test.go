package sidecar

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/shipstream/bloodraven/internal/sidecar/binlogtest"
)

// orderedStore records the order of object writes.
type orderedStore struct {
	ArchiveStore
	mu     sync.Mutex
	writes []string
}

func (s *orderedStore) Put(ctx context.Context, key string, r io.Reader, size int64) error {
	s.mu.Lock()
	s.writes = append(s.writes, key)
	s.mu.Unlock()
	return s.ArchiveStore.Put(ctx, key, r, size)
}

func (s *orderedStore) PutIfAbsent(ctx context.Context, key string, r io.Reader, size int64) error {
	return PutIfAbsent(ctx, s.ArchiveStore, key, r, size)
}

// promotionFixture is a binlog directory with one sealed file.
func promotionFixture(t *testing.T) *PITRConfig {
	t.Helper()
	dir := t.TempDir()
	data := binlogtest.File{ServerID: 101, Txns: []binlogtest.Txn{{SID: hintSIDA, GNO: 1, Commit: time.Unix(1776243600, 0), ServerID: 101}}}.MustBytes()
	if err := os.WriteFile(filepath.Join(dir, "mysql-bin.000001"), data, 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, "mysql-bin.index"), []byte("mysql-bin.000001\nmysql-bin.000002\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	return &PITRConfig{BinlogDir: dir, BinlogIndex: "mysql-bin.index", ManifestPrefix: "binlogs", PollInterval: time.Hour}
}

func fixedClock(at time.Time) func() time.Time { return func() time.Time { return at } }

func TestPromotionRecordPrecedesUploads(t *testing.T) {
	ctx := context.Background()
	store := &orderedStore{ArchiveStore: newMemStore()}
	role := &stubRoleChecker{serverUUID: hintSIDA, serverID: 102, gtidExecuted: hintSIDA + ":1-7"}
	a := NewBinlogArchiver(withTestArchive(promotionFixture(t), store), "iad", role, store, slog.Default())
	at := time.Date(2026, 11, 4, 10, 0, 0, 123456789, time.FixedZone("x", 3600))
	a.now = fixedClock(at)
	a.scanAndArchive(ctx)
	st := a.Snapshot(ctx)
	if st.LastError != "" || st.FilesArchived != 1 {
		t.Fatalf("scan: %+v", st)
	}
	m, err := loadManifest(ctx, store, "binlogs", "iad")
	if err != nil {
		t.Fatal(err)
	}
	want := PromotionRecord{ServerUUID: hintSIDA, ServerID: 102, GTIDExecuted: hintSIDA + ":1-7", ObservedAt: at.UTC().Truncate(time.Microsecond)}
	if len(m.Promotions) != 1 || m.Promotions[0] != want {
		t.Fatalf("promotions = %+v, want %+v", m.Promotions, want)
	}
	if !st.PromotionRecordedAt.Equal(want.ObservedAt) {
		t.Fatalf("promotionRecordedAt = %v", st.PromotionRecordedAt)
	}
	firstManifest, firstBinlog := -1, -1
	for i, key := range store.writes {
		switch {
		case strings.HasSuffix(key, "/manifest-iad.json") && firstManifest < 0:
			firstManifest = i
		case strings.HasPrefix(key, "binlogs/iad/mysql-bin.") && firstBinlog < 0:
			firstBinlog = i
		}
	}
	if firstManifest < 0 || firstBinlog < 0 || firstManifest > firstBinlog {
		t.Fatalf("the record must be saved before any upload: writes %v", store.writes)
	}
	// Same episode: no second record.
	a.scanAndArchive(ctx)
	a.roleTick(ctx)
	if m, _ := loadManifest(ctx, store, "binlogs", "iad"); len(m.Promotions) != 1 {
		t.Fatalf("one episode wrote %d records", len(m.Promotions))
	}
}

func TestPromotionRecordFailureBlocksUploads(t *testing.T) {
	ctx := context.Background()
	store := &faultStore{ArchiveStore: newMemStore(), dropManifests: true}
	a := newTestArchiver(promotionFixture(t), "iad", &stubRoleChecker{}, store, slog.Default())
	a.scanAndArchive(ctx)
	st := a.Snapshot(ctx)
	if !strings.HasPrefix(st.LastError, "promotion record: ") || !strings.Contains(st.LastError, "read back") || st.FilesArchived != 0 {
		t.Fatalf("status = %+v", st)
	}
	if !st.PromotionRecordedAt.IsZero() {
		t.Fatal("unconfirmed record reported")
	}
	for key := range store.ArchiveStore.(*memStore).objects {
		if strings.HasPrefix(key, "binlogs/iad/") {
			t.Fatalf("uploaded %s before the record was confirmed", key)
		}
	}
	// The store recovers: the retry writes the original observation.
	store.dropManifests = false
	a.scanAndArchive(ctx)
	if st := a.Snapshot(ctx); st.LastError != "" || st.FilesArchived != 1 {
		t.Fatalf("after recovery: %+v", st)
	}
}

func TestPromotionRecordPerEpisode(t *testing.T) {
	ctx := context.Background()
	store := newMemStore()
	role := &stubRoleChecker{readOnly: true, gtidExecuted: hintSIDA + ":1-5"}
	cfg := promotionFixture(t)
	a := newTestArchiver(cfg, "iad", role, store, slog.Default())
	clock := time.Date(2026, 11, 4, 10, 0, 0, 0, time.UTC)
	a.now = func() time.Time { return clock }
	a.scanAndArchive(ctx) // Run's initial scan opens the session
	role.mu.Lock()
	role.readOnly = false
	role.mu.Unlock()
	a.roleTick(ctx) // the role probe records a promotion without waiting for a scan
	count := func() int {
		m, err := loadManifest(ctx, store, "binlogs", "iad")
		if err != nil {
			t.Fatal(err)
		}
		return len(m.Promotions)
	}
	if count() != 1 {
		t.Fatalf("role tick wrote %d records", count())
	}

	role.mu.Lock()
	role.readOnly = true
	role.mu.Unlock()
	a.roleTick(ctx)
	if !a.Snapshot(ctx).PromotionRecordedAt.IsZero() {
		t.Fatal("a read-only site reports a promotion record")
	}
	role.mu.Lock()
	role.readOnly, role.gtidExecuted = false, hintSIDA+":1-9"
	role.mu.Unlock()
	clock = clock.Add(time.Minute)
	a.scanAndArchive(ctx)
	if count() != 2 {
		t.Fatalf("ro -> rw wrote %d records in total, want 2", count())
	}

	// A failed probe ends the episode too.
	role.mu.Lock()
	role.isReadOnlyErr = errors.New("connection refused")
	role.mu.Unlock()
	a.roleTick(ctx)
	role.mu.Lock()
	role.isReadOnlyErr = nil
	role.mu.Unlock()
	clock = clock.Add(time.Minute)
	a.roleTick(ctx)
	if count() != 3 {
		t.Fatalf("after a failed probe: %d records, want 3", count())
	}

	// A sidecar restart while writable writes a record.
	restarted := newTestArchiver(cfg, "iad", role, store, slog.Default())
	restarted.now = fixedClock(clock.Add(time.Minute))
	restarted.scanAndArchive(ctx)
	if count() != 4 {
		t.Fatalf("after a restart: %d records, want 4", count())
	}
	m, _ := loadManifest(ctx, store, "binlogs", "iad")
	if last := m.Promotions[3]; last.GTIDExecuted != hintSIDA+":1-9" || !last.ObservedAt.Equal(clock.Add(time.Minute)) {
		t.Fatalf("last record = %+v", last)
	}
}

// bareRoleChecker reports read-only but not the server identity.
type bareRoleChecker struct{}

func (bareRoleChecker) IsReadOnly(context.Context) (bool, error) { return false, nil }
func (bareRoleChecker) isConnectable(context.Context) bool       { return true }

func TestPromotionRecordNeedsRoleProbe(t *testing.T) {
	ctx := context.Background()
	store := newMemStore()
	a := newTestArchiver(promotionFixture(t), "iad", bareRoleChecker{}, store, slog.Default())
	a.scanAndArchive(ctx)
	if st := a.Snapshot(ctx); st.FilesArchived != 0 || st.LastError == "" {
		t.Fatalf("status = %+v, want uploads blocked", st)
	}
}

// At the MaxPromotionRecords cap, a record observed after a backward clock
// step sorts before every kept record. It must still be in the saved
// manifest before the episode counts as recorded (A4 iv).
func TestPromotionRecordAtCapSurvivesBackwardClock(t *testing.T) {
	ctx := context.Background()
	store := newMemStore()
	cfg := promotionFixture(t)
	if err := seedTestDescriptor(ctx, store, cfg.ManifestPrefix); err != nil {
		t.Fatal(err)
	}
	base := time.Date(2026, 11, 4, 10, 0, 0, 0, time.UTC)
	full := &Manifest{Site: "iad", ArchiveID: testArchiveID}
	for i := 1; i <= MaxPromotionRecords; i++ {
		full.Promotions = append(full.Promotions, PromotionRecord{
			ServerUUID: hintSIDA, ServerID: 101, GTIDExecuted: hintSIDA + ":1-" + strconv.Itoa(i),
			ObservedAt: base.Add(time.Duration(i) * time.Second),
		})
	}
	if err := saveManifest(ctx, store, cfg.ManifestPrefix, full); err != nil {
		t.Fatal(err)
	}
	role := &stubRoleChecker{serverUUID: hintSIDA, serverID: 102, gtidExecuted: hintSIDA + ":1-300"}
	a := NewBinlogArchiver(withTestArchive(cfg, store), "iad", role, store, slog.Default())
	a.now = fixedClock(base) // the node's clock stepped back past every record
	a.scanAndArchive(ctx)

	want := PromotionRecord{ServerUUID: hintSIDA, ServerID: 102, GTIDExecuted: hintSIDA + ":1-300", ObservedAt: base}
	m, err := loadManifest(ctx, store, cfg.ManifestPrefix, "iad")
	if err != nil {
		t.Fatal(err)
	}
	if len(m.Promotions) != MaxPromotionRecords || !hasPromotionRecord(m.Promotions, want) {
		t.Fatalf("saved %d records; new record present = %t", len(m.Promotions), hasPromotionRecord(m.Promotions, want))
	}
	if st := a.Snapshot(ctx); !st.PromotionRecordedAt.Equal(base) || st.LastError != "" {
		t.Fatalf("status = %+v", st)
	}
}

func TestMergePromotionRecordsKeepsAppendedRecordAtCap(t *testing.T) {
	at := func(s int) time.Time { return time.Unix(1776243600+int64(s), 0).UTC() }
	rec := func(s int) PromotionRecord {
		return PromotionRecord{ServerUUID: hintSIDA, ServerID: 1, GTIDExecuted: hintSIDA + ":1-" + strconv.Itoa(s+1), ObservedAt: at(s)}
	}
	var guard []PromotionRecord
	for i := 1; i <= MaxPromotionRecords; i++ {
		guard = append(guard, rec(i))
	}
	m := &Manifest{Promotions: append([]PromotionRecord(nil), guard...)}
	m.AppendPromotionRecord(rec(0)) // older than every kept record
	got := mergePromotionRecords(m.Promotions, guard, nil)
	if len(got) != MaxPromotionRecords || !hasPromotionRecord(got, rec(0)) {
		t.Fatalf("merge kept %d records; appended record present = %t", len(got), hasPromotionRecord(got, rec(0)))
	}
	if hasPromotionRecord(got, rec(1)) {
		t.Fatal("the cap kept the oldest older record instead of dropping it")
	}
	for i := 1; i < len(got); i++ {
		if got[i].ObservedAt.Before(got[i-1].ObservedAt) {
			t.Fatalf("records out of order at %d", i)
		}
	}
}
