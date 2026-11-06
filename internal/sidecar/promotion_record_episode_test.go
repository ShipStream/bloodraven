package sidecar

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/shipstream/bloodraven/internal/pitrarchive"
)

// epStore is a memStore wrapper that can fail descriptor and manifest
// reads, for the episode tests.
type epStore struct {
	*memStore
	mu       sync.Mutex
	failGets bool
	failKey  string // when set, only this key fails
}

func (s *epStore) Get(ctx context.Context, key string) ([]byte, bool, error) {
	s.mu.Lock()
	fail := s.failGets && (s.failKey == "" || s.failKey == key)
	s.mu.Unlock()
	if fail {
		return nil, false, errors.New("503 SlowDown")
	}
	return s.memStore.Get(ctx, key)
}

func (s *epStore) PutIfAbsent(ctx context.Context, key string, r io.Reader, size int64) error {
	return s.memStore.PutIfAbsent(ctx, key, r, size)
}

func (s *epStore) setFailGets(v bool) {
	s.mu.Lock()
	s.failGets = v
	s.mu.Unlock()
}

func epPromotions(t *testing.T, store ArchiveStore) []PromotionRecord {
	t.Helper()
	m, err := loadManifest(context.Background(), store, "binlogs", "iad")
	if err != nil {
		t.Fatal(err)
	}
	return m.Promotions
}

const epSIDB = "bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb"

// A writable observation of another server incarnation starts a new
// episode with its own record even though the episode was recorded and no
// read-only observation came in between (N4, F011).
func TestPromotionRecordNewIncarnationWhileRecorded(t *testing.T) {
	recorded := hintSIDA + ":1," + epSIDB + ":1"
	tests := []struct {
		name       string
		uuid       string
		serverID   uint32
		gtids      string
		newEpisode bool
	}{
		{"history grew", hintSIDA, 102, hintSIDA + ":1-5," + epSIDB + ":1", false},
		{"same history", hintSIDA, 102, recorded, false},
		{"history shrank (reseed)", hintSIDA, 102, hintSIDA + ":1", true},
		{"server_uuid changed", epSIDB, 102, recorded, true},
		{"server_id changed", hintSIDA, 103, recorded, true},
		{"unparsable gtid_executed", hintSIDA, 102, "not-a-gtid-set", true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctx := context.Background()
			store := newMemStore()
			role := &stubRoleChecker{serverUUID: hintSIDA, serverID: 102, gtidExecuted: recorded}
			a := newTestArchiver(promotionFixture(t), "iad", role, store, slog.Default())
			clock := time.Date(2026, 11, 4, 10, 0, 0, 0, time.UTC)
			a.now = func() time.Time { return clock }
			a.scanAndArchive(ctx)
			if got := epPromotions(t, store); len(got) != 1 || got[0].GTIDExecuted != recorded {
				t.Fatalf("first episode records = %+v", got)
			}

			role.mu.Lock()
			role.serverUUID, role.serverID, role.gtidExecuted = tt.uuid, tt.serverID, tt.gtids
			role.mu.Unlock()
			clock = clock.Add(time.Minute)
			a.roleTick(ctx)
			a.scanAndArchive(ctx)
			got := epPromotions(t, store)
			if !tt.newEpisode {
				if len(got) != 1 {
					t.Fatalf("one incarnation wrote %d records", len(got))
				}
				return
			}
			if len(got) != 2 {
				t.Fatalf("new incarnation wrote %d records in total, want 2", len(got))
			}
			// The change happened after the previous probe a minute
			// earlier; the record is its fence, with no reset record.
			want := PromotionRecord{ServerUUID: tt.uuid, ServerID: tt.serverID, GTIDExecuted: tt.gtids, ObservedAt: clock,
				ProbeInterval: pitrarchive.Duration(time.Minute)}
			if got[1] != want {
				t.Fatalf("new record = %+v, want %+v", got[1], want)
			}
			if st := a.Snapshot(ctx); !st.PromotionRecordedAt.Equal(clock) {
				t.Fatalf("promotionRecordedAt = %v, want %v", st.PromotionRecordedAt, clock)
			}
		})
	}
}

// epSlowIntegrity advances the archiver clock inside the D-34 probe, as
// slow manifest and integrity I/O would.
type epSlowIntegrity struct {
	*stubRoleChecker
	clock *time.Time
	probe time.Duration // added inside ProbeRole
	slow  time.Duration // added inside HistoryIntegrityViolation
}

func (s *epSlowIntegrity) ProbeRole(ctx context.Context) (RoleObservation, error) {
	*s.clock = s.clock.Add(s.probe)
	return s.stubRoleChecker.ProbeRole(ctx)
}

func (s *epSlowIntegrity) HistoryIntegrityViolation(ctx context.Context) (pitrarchive.HistoryViolation, string, error) {
	*s.clock = s.clock.Add(s.slow)
	return s.stubRoleChecker.HistoryIntegrityViolation(ctx)
}

// The record's time is when the role probe that read its GTID set
// returned, not when the scan got round to writing it (F089).
func TestPromotionRecordTimeIsProbeTime(t *testing.T) {
	ctx := context.Background()
	store := newMemStore()
	clock := time.Date(2026, 11, 4, 10, 0, 0, 0, time.UTC)
	role := &epSlowIntegrity{stubRoleChecker: &stubRoleChecker{serverUUID: hintSIDA, serverID: 102, gtidExecuted: hintSIDA + ":1-3"}, clock: &clock, slow: time.Minute}
	a := newTestArchiver(promotionFixture(t), "iad", role, store, slog.Default())
	a.now = func() time.Time { return clock }
	probedAt := clock
	a.scanAndArchive(ctx)
	got := epPromotions(t, store)
	if len(got) != 1 || !got[0].ObservedAt.Equal(probedAt) {
		t.Fatalf("records = %+v, want one observed at %v (the probe), clock now %v", got, probedAt, clock)
	}
}

// A role probe slower than maxRoleProbeSpread cannot open a record: its
// GTID snapshot could be that much older than its time. Uploads wait for a
// prompt probe of the same episode.
func TestPromotionRecordWaitsForPromptProbe(t *testing.T) {
	ctx := context.Background()
	store := newMemStore()
	clock := time.Date(2026, 11, 4, 10, 0, 0, 0, time.UTC)
	role := &epSlowIntegrity{stubRoleChecker: &stubRoleChecker{serverUUID: hintSIDA, serverID: 102, gtidExecuted: hintSIDA + ":1-3"}, clock: &clock, probe: 2 * maxRoleProbeSpread}
	a := newTestArchiver(promotionFixture(t), "iad", role, store, slog.Default())
	a.now = func() time.Time { return clock }
	a.scanAndArchive(ctx)
	st := a.Snapshot(ctx)
	if !strings.Contains(st.LastError, "no role probe of this writable episode returned within") || st.FilesArchived != 0 {
		t.Fatalf("status = %+v", st)
	}
	if got := epPromotions(t, store); len(got) != 0 {
		t.Fatalf("a slow probe opened a record: %+v", got)
	}
	role.probe = 0
	a.scanAndArchive(ctx)
	if st := a.Snapshot(ctx); st.LastError != "" || st.FilesArchived != 1 {
		t.Fatalf("after a prompt probe: %+v", st)
	}
	if got := epPromotions(t, store); len(got) != 1 || !got[0].ObservedAt.Equal(clock) {
		t.Fatalf("records = %+v", got)
	}
}

// A read-only observation ends a recorded episode even when the scan then
// stops at session setup (F075): the next writable observation writes a
// new record before any upload.
func TestReadOnlyObservationEndsEpisodeBeforeSessionSetup(t *testing.T) {
	ctx := context.Background()
	inner := newMemStore()
	store := &epStore{memStore: inner}
	role := &stubRoleChecker{serverUUID: hintSIDA, serverID: 102, gtidExecuted: hintSIDA + ":1-3"}
	a := newTestArchiver(promotionFixture(t), "iad", role, store, slog.Default())
	clock := time.Date(2026, 11, 4, 10, 0, 0, 0, time.UTC)
	a.now = func() time.Time { return clock }
	// A recorded writable episode on an unverified session.
	a.roles.observe(RoleObservation{ServerUUID: hintSIDA, ServerID: 102, GTIDExecuted: hintSIDA + ":1-3", ObservedAt: clock}, true, nil)
	a.syncEpisode()
	a.episode.recorded = true

	role.mu.Lock()
	role.readOnly = true
	role.mu.Unlock()
	store.setFailGets(true) // opening the session fails
	a.scanAndArchive(ctx)
	if st := a.Snapshot(ctx); st.LastError == "" {
		t.Fatalf("session setup did not fail: %+v", st)
	}
	if a.episode.recorded || !a.Snapshot(ctx).PromotionRecordedAt.IsZero() {
		t.Fatal("a read-only observation left the episode recorded")
	}

	store.setFailGets(false)
	role.mu.Lock()
	role.readOnly = false
	role.mu.Unlock()
	clock = clock.Add(time.Minute)
	a.scanAndArchive(ctx)
	got := epPromotions(t, inner)
	if len(got) != 1 || !got[0].ObservedAt.Equal(clock) {
		t.Fatalf("records = %+v, want the new episode's record", got)
	}
	if st := a.Snapshot(ctx); st.FilesArchived != 1 {
		t.Fatalf("status = %+v", st)
	}
}

// The role sampler sees a demotion and re-promotion while a scan is busy
// (F074): the busy scan's manifest writes are refused, and the new
// episode's record carries the sampler's observation time.
func TestRoleSamplerEndsEpisodeDuringBusyScan(t *testing.T) {
	ctx := context.Background()
	store := newMemStore()
	role := &stubRoleChecker{serverUUID: hintSIDA, serverID: 102, gtidExecuted: hintSIDA + ":1-3"}
	a := newTestArchiver(promotionFixture(t), "iad", role, store, slog.Default())
	clock := time.Date(2026, 11, 4, 10, 0, 0, 0, time.UTC)
	a.now = func() time.Time { return clock }
	a.scanAndArchive(ctx)
	if len(epPromotions(t, store)) != 1 {
		t.Fatal("no first record")
	}
	m, err := a.loadManifest(ctx)
	if err != nil {
		t.Fatal(err)
	}

	// While the scan goroutine is busy, the sampler sees read-only, then
	// writable again.
	role.mu.Lock()
	role.readOnly = true
	role.mu.Unlock()
	clock = clock.Add(5 * time.Second)
	a.sampleRole(ctx)
	role.mu.Lock()
	role.readOnly, role.gtidExecuted = false, hintSIDA+":1-9"
	role.mu.Unlock()
	clock = clock.Add(5 * time.Second)
	promotedAt := clock
	a.sampleRole(ctx)

	if err := a.saveManifest(ctx, m); !errors.Is(err, errEpisodeEnded) {
		t.Fatalf("busy scan's save = %v, want errEpisodeEnded", err)
	}
	clock = clock.Add(10 * time.Minute) // the busy scan ends much later
	a.recordTick(ctx)
	got := epPromotions(t, store)
	if len(got) != 2 || !got[1].ObservedAt.Equal(promotedAt) || got[1].GTIDExecuted != hintSIDA+":1-9" {
		t.Fatalf("records = %+v, want a second observed at %v", got, promotedAt)
	}
	if err := a.saveManifest(ctx, m); err != nil {
		t.Fatalf("save in the new recorded episode: %v", err)
	}
}

// runRoleSampler probes on its own ticker, apart from the scan goroutine.
func TestRunRoleSamplerFeedsTimeline(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	role := &stubRoleChecker{serverUUID: hintSIDA, serverID: 102, gtidExecuted: hintSIDA + ":1-3"}
	cfg := promotionFixture(t)
	cfg.RoleProbeInterval = 5 * time.Millisecond
	a := newTestArchiver(cfg, "iad", role, newMemStore(), slog.Default())
	done := make(chan struct{})
	go func() { defer close(done); a.runRoleSampler(ctx) }()
	deadline := time.Now().Add(5 * time.Second)
	for a.roles.snapshot().state != roleWritable {
		if time.Now().After(deadline) {
			t.Fatal("sampler never observed the role")
		}
		time.Sleep(time.Millisecond)
	}
	epoch := a.roles.snapshot().epoch
	role.mu.Lock()
	role.readOnly = true
	role.mu.Unlock()
	for a.roles.snapshot().state != roleReadOnly {
		if time.Now().After(deadline) {
			t.Fatal("sampler never observed the demotion")
		}
		time.Sleep(time.Millisecond)
	}
	role.mu.Lock()
	role.readOnly = false
	role.mu.Unlock()
	for s := a.roles.snapshot(); s.state != roleWritable || s.epoch == epoch; s = a.roles.snapshot() {
		if time.Now().After(deadline) {
			t.Fatal("sampler never started a new epoch")
		}
		time.Sleep(time.Millisecond)
	}
	cancel()
	<-done
}

// An observation that finished before the latest one fed is stale.
func TestRoleTimelineDropsStaleObservation(t *testing.T) {
	var tl roleTimeline
	at := time.Date(2026, 11, 4, 10, 0, 0, 0, time.UTC)
	tl.observe(RoleObservation{ReadOnly: true, ObservedAt: at}, true, nil)
	tl.observe(RoleObservation{ServerUUID: hintSIDA, ServerID: 1, ObservedAt: at.Add(-time.Second)}, true, nil)
	if s := tl.snapshot(); s.state != roleReadOnly {
		t.Fatalf("stale writable observation applied: %+v", s)
	}
}

// The record holds the gtid_executed of the read-only observation of the
// same server directly before the episode (N5), and nothing otherwise.
func TestPromotionRecordReadOnlyGTIDExecuted(t *testing.T) {
	ctx := context.Background()
	store := newMemStore()
	role := &stubRoleChecker{readOnly: true, serverUUID: hintSIDA, serverID: 102, gtidExecuted: hintSIDA + ":1-5"}
	cfg := promotionFixture(t)
	a := newTestArchiver(cfg, "iad", role, store, slog.Default())
	clock := time.Date(2026, 11, 4, 10, 0, 0, 0, time.UTC)
	a.now = func() time.Time { return clock }
	a.scanAndArchive(ctx)
	role.mu.Lock()
	role.readOnly, role.gtidExecuted = false, hintSIDA+":1-6"
	role.mu.Unlock()
	clock = clock.Add(time.Second)
	a.scanAndArchive(ctx)
	got := epPromotions(t, store)
	if len(got) != 1 || got[0].ReadOnlyGTIDExecuted != hintSIDA+":1-5" {
		t.Fatalf("records = %+v, want readOnlyGtidExecuted %s:1-5", got, hintSIDA)
	}

	// A failed probe between the read-only and the writable observation
	// breaks adjacency.
	role.mu.Lock()
	role.readOnly = true
	role.mu.Unlock()
	clock = clock.Add(time.Second)
	a.roleTick(ctx)
	role.mu.Lock()
	role.isReadOnlyErr = errors.New("connection refused")
	role.mu.Unlock()
	clock = clock.Add(time.Second)
	a.roleTick(ctx)
	role.mu.Lock()
	role.isReadOnlyErr, role.readOnly = nil, false
	role.mu.Unlock()
	clock = clock.Add(time.Second)
	a.scanAndArchive(ctx)
	got = epPromotions(t, store)
	if len(got) != 2 || got[1].ReadOnlyGTIDExecuted != "" {
		t.Fatalf("records = %+v, want no readOnlyGtidExecuted after a failed probe", got)
	}

	// A sidecar start while writable has no read-only observation.
	restarted := newTestArchiver(cfg, "iad", role, store, slog.Default())
	restarted.now = fixedClock(clock.Add(time.Minute))
	restarted.scanAndArchive(ctx)
	got = epPromotions(t, store)
	if len(got) != 3 || got[2].ReadOnlyGTIDExecuted != "" {
		t.Fatalf("records = %+v", got)
	}
}

// The PEER_CHECK_INTERVAL tick records only on a verified session; on an
// unverified one it neither records nor touches the archive (F216).
func TestRecordTickNeedsVerifiedSession(t *testing.T) {
	ctx := context.Background()
	inner := newMemStore()
	store := &epStore{memStore: inner}
	role := &stubRoleChecker{serverUUID: hintSIDA, serverID: 102, gtidExecuted: hintSIDA + ":1-3"}
	a := newTestArchiver(promotionFixture(t), "iad", role, store, slog.Default())
	store.setFailGets(true) // any archive read would fail the test below
	a.roleTick(ctx)
	if a.episode.recorded || a.episode.pending != nil {
		t.Fatalf("tick on an unverified session started a record: %+v", a.episode)
	}
	if a.session.Verified() {
		t.Fatal("tick opened the session")
	}
	store.setFailGets(false)
	if err := a.openSession(ctx); err != nil {
		t.Fatal(err)
	}
	a.roleTick(ctx)
	if !a.episode.recorded || len(epPromotions(t, inner)) != 1 {
		t.Fatalf("tick on a verified session did not record: %+v", a.episode)
	}
}
