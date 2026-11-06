package sidecar

import (
	"context"
	"log/slog"
	"testing"
	"time"

	"github.com/shipstream/bloodraven/internal/pitrarchive"
)

// Incarnation-reset records (N3 revised): the sidecar writes one on every
// role observation, in any role, that no longer continues the incarnation
// of its previous observation or of the site's latest record. The planner
// uses every record of a site as a fence for that site's anchors.

const resetSID = hintSIDA

func resetRecords(records []PromotionRecord) []PromotionRecord {
	var out []PromotionRecord
	for _, r := range records {
		if r.Kind == pitrarchive.RecordReset {
			out = append(out, r)
		}
	}
	return out
}

// startWriter runs one scan of a writable archiver at clock, as the site's
// sidecar before a restart, and returns its promotion record.
func startWriter(t *testing.T, store ArchiveStore, gtids string, at time.Time) PromotionRecord {
	t.Helper()
	role := &stubRoleChecker{serverUUID: resetSID, serverID: 102, gtidExecuted: gtids}
	a := newTestArchiver(promotionFixture(t), "iad", role, store, slog.Default())
	a.now = fixedClock(at)
	a.scanAndArchive(context.Background())
	got := epPromotions(t, store)
	if len(got) != 1 || got[0].Kind != pitrarchive.RecordPromotion {
		t.Fatalf("writer records = %+v", got)
	}
	return got[0]
}

// A replica recloned while its sidecar runs: the read-only observation
// after the reclone no longer contains the previous one, so a reset
// record is written with the window since the previous probe's read.
func TestResetRecordReadOnlyReclone(t *testing.T) {
	ctx := context.Background()
	store := newMemStore()
	clock := time.Date(2026, 11, 4, 10, 0, 0, 0, time.UTC)
	role := &stubRoleChecker{readOnly: true, serverUUID: resetSID, serverID: 102, gtidExecuted: resetSID + ":1-10"}
	a := newTestArchiver(promotionFixture(t), "iad", role, store, slog.Default())
	a.now = func() time.Time { return clock }
	a.scanAndArchive(ctx)
	if got := epPromotions(t, store); len(got) != 0 {
		t.Fatalf("a replica with no change wrote %+v", got)
	}

	// The history grows: no record.
	role.mu.Lock()
	role.gtidExecuted = resetSID + ":1-12"
	role.mu.Unlock()
	clock = clock.Add(5 * time.Second)
	a.roleTick(ctx)
	if got := epPromotions(t, store); len(got) != 0 {
		t.Fatalf("a growing history wrote %+v", got)
	}

	// Recloned from a donor without :11-12.
	role.mu.Lock()
	role.gtidExecuted = resetSID + ":1-10"
	role.mu.Unlock()
	clock = clock.Add(12 * time.Second)
	a.roleTick(ctx)
	want := PromotionRecord{Kind: pitrarchive.RecordReset, ServerUUID: resetSID, ServerID: 102, GTIDExecuted: resetSID + ":1-10",
		ObservedAt: clock, ProbeInterval: pitrarchive.Duration(12 * time.Second)}
	if got := epPromotions(t, store); len(got) != 1 || got[0] != want {
		t.Fatalf("records = %+v, want %+v", got, want)
	}
	if st := a.Snapshot(ctx); st.LastError != "" || !st.PromotionRecordedAt.IsZero() {
		t.Fatalf("status %+v", st)
	}
}

// A sidecar start checks its first observation against the site's latest
// record: a reclone, reset or replaced server while the sidecar was down
// is recorded with a window that reaches back to that record. A restart
// in the middle of an era (same incarnation) writes only the new
// episode's promotion record, with the configured probe interval.
func TestResetRecordAtSidecarStart(t *testing.T) {
	t0 := time.Date(2026, 11, 4, 9, 0, 0, 0, time.UTC)
	restart := t0.Add(time.Hour)
	tests := []struct {
		name     string
		readOnly bool
		uuid     string
		gtids    string
		// wantReset is the reset record's ProbeInterval (0: none);
		// wantPromotion the new promotion record's (0: none).
		wantReset, wantPromotion time.Duration
	}{
		{"restart mid-era", false, resetSID, resetSID + ":1-20", 0, defaultPeerCheckInterval},
		{"restart as a replica", true, resetSID, resetSID + ":1-20", 0, 0},
		{"replica recloned while down", true, resetSID, resetSID + ":1-5", time.Hour, 0},
		{"server replaced while down", true, epSIDB, resetSID + ":1-7", time.Hour, 0},
		// The new episode's promotion record holds the same observation
		// and is the fence; no separate reset record.
		{"writer reseeded while down", false, resetSID, resetSID + ":1-5", 0, time.Hour},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctx := context.Background()
			store := newMemStore()
			before := startWriter(t, store, resetSID+":1-7", t0)

			role := &stubRoleChecker{readOnly: tt.readOnly, serverUUID: tt.uuid, serverID: 102, gtidExecuted: tt.gtids}
			a := NewBinlogArchiver(withTestArchive(promotionFixture(t), store), "iad", role, store, slog.Default())
			a.now = fixedClock(restart)
			a.scanAndArchive(ctx)
			got := epPromotions(t, store)
			if got[0] != before {
				t.Fatalf("the earlier record changed: %+v", got[0])
			}
			got = got[1:]
			var want []PromotionRecord
			if tt.wantReset > 0 {
				want = append(want, PromotionRecord{Kind: pitrarchive.RecordReset, ServerUUID: tt.uuid, ServerID: 102, GTIDExecuted: tt.gtids,
					ObservedAt: restart, ProbeInterval: pitrarchive.Duration(tt.wantReset)})
			}
			if tt.wantPromotion > 0 {
				want = append(want, PromotionRecord{ServerUUID: tt.uuid, ServerID: 102, GTIDExecuted: tt.gtids,
					ObservedAt: restart, ProbeInterval: pitrarchive.Duration(tt.wantPromotion)})
			}
			if len(got) != len(want) {
				t.Fatalf("records after the restart = %+v, want %+v", got, want)
			}
			for i := range want {
				if got[i] != want[i] {
					t.Fatalf("record %d = %+v, want %+v", i, got[i], want[i])
				}
			}
			if st := a.Snapshot(ctx); st.LastError != "" {
				t.Fatalf("status %+v", st)
			}
		})
	}
}

// The start-up check runs before the new episode's promotion record (and
// so before any upload): while the manifest cannot be read, a writable
// sidecar records nothing and reports the wait.
func TestResetRecordStartCheckGatesRecords(t *testing.T) {
	ctx := context.Background()
	store := &epStore{memStore: newMemStore()}
	clock := time.Date(2026, 11, 4, 10, 0, 0, 0, time.UTC)
	startWriter(t, store, resetSID+":1-7", clock)

	role := &stubRoleChecker{serverUUID: resetSID, serverID: 102, gtidExecuted: resetSID + ":1-5"}
	a := NewBinlogArchiver(withTestArchive(promotionFixture(t), store), "iad", role, store, slog.Default())
	clock = clock.Add(time.Minute)
	seen := clock
	a.now = func() time.Time { return clock }
	store.failKey = manifestKey("binlogs", "iad")
	store.setFailGets(true)
	a.scanAndArchive(ctx)
	a.recordTick(ctx)
	store.setFailGets(false)
	if st := a.Snapshot(ctx); st.LastError == "" || !st.PromotionRecordedAt.IsZero() {
		t.Fatalf("status %+v", st)
	}
	if got := epPromotions(t, store); len(got) != 1 {
		t.Fatalf("records written before the start-up check: %+v", got)
	}
	clock = clock.Add(time.Second)
	a.scanAndArchive(ctx)
	// The record holds the first observation (the reseed) and reaches
	// back to the latest record.
	got := epPromotions(t, store)
	if len(got) != 2 || got[1].Kind != pitrarchive.RecordPromotion || got[1].GTIDExecuted != resetSID+":1-5" ||
		!got[1].ObservedAt.Equal(seen) || time.Duration(got[1].ProbeInterval) != time.Minute {
		t.Fatalf("records = %+v", got)
	}
	if st := a.Snapshot(ctx); st.LastError != "" {
		t.Fatalf("status %+v", st)
	}
}

// A writable incarnation change seen first by a probe too slow to open
// the promotion record is not covered by it: a reset record holds that
// observation, and the record of the next fast probe follows.
func TestResetRecordSlowWritableObservation(t *testing.T) {
	ctx := context.Background()
	store := newMemStore()
	clock := time.Date(2026, 11, 4, 10, 0, 0, 0, time.UTC)
	role := &epSlowIntegrity{stubRoleChecker: &stubRoleChecker{serverUUID: resetSID, serverID: 102, gtidExecuted: resetSID + ":1-7"}, clock: &clock}
	a := newTestArchiver(promotionFixture(t), "iad", role, store, slog.Default())
	a.now = func() time.Time { return clock }
	a.scanAndArchive(ctx)
	first := clock

	role.mu.Lock()
	role.gtidExecuted = resetSID + ":1-3"
	role.mu.Unlock()
	clock = clock.Add(10 * time.Second)
	role.probe = 2 * time.Second
	a.roleTick(ctx)
	got := epPromotions(t, store)
	slowAt := clock
	want := PromotionRecord{Kind: pitrarchive.RecordReset, ServerUUID: resetSID, ServerID: 102, GTIDExecuted: resetSID + ":1-3",
		ObservedAt: slowAt, ProbeInterval: pitrarchive.Duration(slowAt.Sub(first))}
	if len(got) != 2 || got[1] != want {
		t.Fatalf("records = %+v, want a reset %+v", got, want)
	}

	role.probe = 0
	clock = clock.Add(5 * time.Second)
	a.roleTick(ctx)
	got = epPromotions(t, store)
	if len(got) != 3 || got[2].Kind != pitrarchive.RecordPromotion || !got[2].ObservedAt.Equal(clock) {
		t.Fatalf("records = %+v, want the new episode's promotion record last", got)
	}
}

// A covered change whose episode ends before its promotion record is
// written is recorded as a reset record.
func TestResetRecordEpisodeEndsUnrecorded(t *testing.T) {
	ctx := context.Background()
	store := &epStore{memStore: newMemStore()}
	clock := time.Date(2026, 11, 4, 10, 0, 0, 0, time.UTC)
	role := &stubRoleChecker{serverUUID: resetSID, serverID: 102, gtidExecuted: resetSID + ":1-7"}
	a := newTestArchiver(promotionFixture(t), "iad", role, store, slog.Default())
	a.now = func() time.Time { return clock }
	a.scanAndArchive(ctx)
	first := clock

	// Reseeded while writable; the record cannot be written.
	role.mu.Lock()
	role.gtidExecuted = resetSID + ":1-2"
	role.mu.Unlock()
	clock = clock.Add(8 * time.Second)
	seen := clock
	store.failKey = manifestKey("binlogs", "iad")
	store.setFailGets(true)
	a.roleTick(ctx)
	store.setFailGets(false)
	if got := epPromotions(t, store); len(got) != 1 {
		t.Fatalf("records = %+v", got)
	}

	// Demoted before the record was written.
	role.mu.Lock()
	role.readOnly = true
	role.mu.Unlock()
	clock = clock.Add(5 * time.Second)
	a.roleTick(ctx)
	want := PromotionRecord{Kind: pitrarchive.RecordReset, ServerUUID: resetSID, ServerID: 102, GTIDExecuted: resetSID + ":1-2",
		ObservedAt: seen, ProbeInterval: pitrarchive.Duration(seen.Sub(first))}
	if got := epPromotions(t, store); len(got) != 2 || got[1] != want {
		t.Fatalf("records = %+v, want %+v", got, want)
	}
}

// The pending candidates are bounded; merging the oldest widens the
// window to both (fail closed).
func TestResetCandidatesBounded(t *testing.T) {
	var tl roleTimeline
	at := time.Date(2026, 11, 4, 10, 0, 0, 0, time.UTC)
	for i := 0; i <= maxPendingResets+2; i++ {
		// Every observation after the first is another server.
		tl.observe(RoleObservation{ReadOnly: true, ServerUUID: resetSID, ServerID: uint32(102 + i%2), GTIDExecuted: resetSID + ":1-10",
			ObservedAt: at.Add(time.Duration(i) * time.Second)}, true, nil)
	}
	got := tl.pendingResets()
	if len(got) != maxPendingResets {
		t.Fatalf("%d candidates, want %d", len(got), maxPendingResets)
	}
	if !got[0].since.Equal(at) || !got[0].obs.ObservedAt.Equal(at.Add(3*time.Second)) {
		t.Fatalf("merged candidate %+v, want since %v at %v", got[0], at, at.Add(3*time.Second))
	}
}
