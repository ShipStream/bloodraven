package dst

import (
	"context"
	"strings"
	"testing"
	"time"

	apimeta "k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	v1alpha1 "github.com/shipstream/bloodraven/api/v1alpha1"
	"github.com/shipstream/bloodraven/internal/clock"
	"github.com/shipstream/bloodraven/internal/controller"
)

func newTestStore(t *testing.T) (*simPlannedStore, *clock.FakeClock) {
	t.Helper()
	clk := clock.NewFakeClock(time.Date(2030, 1, 1, 0, 0, 0, 0, time.UTC))
	obj := &v1alpha1.MysqlFailoverGroup{ObjectMeta: metav1.ObjectMeta{Namespace: "sim", Name: "sim"}}
	return newSimPlannedStore(obj, clk), clk
}

func mustLoad(t *testing.T, s *simPlannedStore) *v1alpha1.MysqlFailoverGroup {
	t.Helper()
	obj, err := s.Load(context.Background(), s.nn)
	if err != nil || obj == nil {
		t.Fatalf("load: %v %v", obj, err)
	}
	return obj
}

func block(run string, rev int64, phase v1alpha1.PlannedFailoverPhase) v1alpha1.PlannedFailoverStatus {
	return v1alpha1.PlannedFailoverStatus{RunID: run, Revision: rev, Phase: phase, TargetSite: "beta"}
}

func wantOutcome(t *testing.T, got, want controller.WriteOutcome, err error) {
	t.Helper()
	if got != want {
		t.Fatalf("outcome %s (err %v), want %s", got, err, want)
	}
	if (err == nil) != (want == controller.WriteCommitted) {
		t.Fatalf("outcome %s with err %v: err must be nil iff Committed", got, err)
	}
}

func TestSimStore_ConditionedWrite(t *testing.T) {
	s, _ := newTestStore(t)
	ctx := context.Background()
	base := mustLoad(t, s)

	out, err := s.Write(ctx, base, block("r1", 1, v1alpha1.PlannedFailoverPhaseAdmitting), nil)
	wantOutcome(t, out, controller.WriteCommitted, err)
	if s.ResourceVersion() != 2 {
		t.Fatalf("rv %d after a commit", s.ResourceVersion())
	}

	// The stale base conflicts and changes nothing.
	out, err = s.Write(ctx, base, block("r1", 2, v1alpha1.PlannedFailoverPhaseFencing), nil)
	wantOutcome(t, out, controller.WriteConflict, err)
	if got := mustLoad(t, s).Status.PlannedFailover; got.Revision != 1 {
		t.Fatalf("conflicting write landed: %+v", got)
	}

	// A no-op update does not move the resourceVersion (the no-op trap).
	cur := mustLoad(t, s)
	out, err = s.Write(ctx, cur, *cur.Status.PlannedFailover, nil)
	wantOutcome(t, out, controller.WriteCommitted, err)
	if s.ResourceVersion() != 2 {
		t.Fatalf("a no-op write moved rv to %d", s.ResourceVersion())
	}

	// mutate applies to the same write.
	out, err = s.Write(ctx, cur, block("r1", 2, v1alpha1.PlannedFailoverPhaseSucceeded), func(st *v1alpha1.MysqlFailoverGroupStatus) {
		st.SetLastFailover(st.LastFailoverTime(), "beta", st.PromotionGtidExecuted())
	})
	wantOutcome(t, out, controller.WriteCommitted, err)
	if got := mustLoad(t, s); got.Status.LastFailoverTarget() != "beta" || got.Status.PlannedFailover.Revision != 2 {
		t.Fatalf("mutate not applied: %+v", got.Status)
	}
}

func TestSimStore_InjectedOutcomes(t *testing.T) {
	ctx := context.Background()
	t.Run("conflictAndDefinite", func(t *testing.T) {
		s, _ := newTestStore(t)
		s.ConflictWrites(1)
		s.DefiniteWrites(1)
		out, err := s.Write(ctx, mustLoad(t, s), block("r1", 1, v1alpha1.PlannedFailoverPhaseAdmitting), nil)
		wantOutcome(t, out, controller.WriteConflict, err)
		out, err = s.Write(ctx, mustLoad(t, s), block("r1", 1, v1alpha1.PlannedFailoverPhaseAdmitting), nil)
		wantOutcome(t, out, controller.WriteNotCommitted, err)
		if mustLoad(t, s).Status.PlannedFailover != nil || s.ResourceVersion() != 1 {
			t.Fatal("an injected refusal committed")
		}
	})
	t.Run("lostIsFencedNotCommitted", func(t *testing.T) {
		s, _ := newTestStore(t)
		s.LoseWrites(1)
		out, err := s.Write(ctx, mustLoad(t, s), block("r1", 1, v1alpha1.PlannedFailoverPhaseAdmitting), nil)
		wantOutcome(t, out, controller.WriteNotCommitted, err)
		obj := mustLoad(t, s)
		if obj.Status.PlannedFailover != nil {
			t.Fatal("a lost write committed")
		}
		if obj.GetAnnotations()[v1alpha1.PlannedFailoverStatusFenceAnnotation] == "" || s.ResourceVersion() != 2 {
			t.Fatalf("settlement did not fence (rv %d, annotations %v)", s.ResourceVersion(), obj.GetAnnotations())
		}
	})
	t.Run("ambiguousCommittedSettlesCommitted", func(t *testing.T) {
		s, _ := newTestStore(t)
		s.AmbiguousWrites(1)
		out, err := s.Write(ctx, mustLoad(t, s), block("r1", 1, v1alpha1.PlannedFailoverPhaseAdmitting), nil)
		wantOutcome(t, out, controller.WriteCommitted, err)
		if mustLoad(t, s).GetAnnotations()[v1alpha1.PlannedFailoverStatusFenceAnnotation] != "" {
			t.Fatal("a committed write was fenced")
		}
	})
	t.Run("lateCommitKilledByTheFence", func(t *testing.T) {
		s, _ := newTestStore(t)
		s.LateWrites(1, 2)
		out, err := s.Write(ctx, mustLoad(t, s), block("r1", 1, v1alpha1.PlannedFailoverPhaseAdmitting), nil)
		wantOutcome(t, out, controller.WriteNotCommitted, err)
		s.BeginPoll(2)
		if mustLoad(t, s).Status.PlannedFailover != nil {
			t.Fatal("a fenced late write committed")
		}
	})
	t.Run("lateCommitLandsWhenSettlementCannotRead", func(t *testing.T) {
		s, _ := newTestStore(t)
		s.LateWrites(1, 2)
		base := mustLoad(t, s)
		s.ReadOutage(1)
		out, err := s.Write(ctx, base, block("r1", 1, v1alpha1.PlannedFailoverPhaseAdmitting), nil)
		wantOutcome(t, out, controller.WriteUnknown, err)
		s.BeginPoll(1) // read outage over; the late commit is not due yet
		if mustLoad(t, s).Status.PlannedFailover != nil {
			t.Fatal("late write committed early")
		}
		s.BeginPoll(2)
		if got := mustLoad(t, s).Status.PlannedFailover; got == nil || got.RunID != "r1" {
			t.Fatalf("unfenced late write did not commit: %+v", got)
		}
		// Settling again now sees it.
		out, err = s.Settle(ctx, s.nn, "1", "r1", 1)
		wantOutcome(t, out, controller.WriteCommitted, err)
	})
	t.Run("lateCommitRefusedAfterAnotherWriter", func(t *testing.T) {
		s, _ := newTestStore(t)
		s.LateWrites(1, 1)
		base := mustLoad(t, s)
		s.ReadOutage(1)
		_, _ = s.Write(ctx, base, block("r1", 1, v1alpha1.PlannedFailoverPhaseAdmitting), nil)
		s.ClearFaults()
		if err := s.MergeSnapshot(controller.TopologySnapshot{ActiveSite: "alpha"}); err != nil {
			t.Fatal(err)
		}
		s.BeginPoll(1)
		if mustLoad(t, s).Status.PlannedFailover != nil {
			t.Fatal("late write committed over a newer resourceVersion")
		}
	})
	t.Run("statusDeniedMetadataWorks", func(t *testing.T) {
		s, _ := newTestStore(t)
		s.DenyStatus(3)
		base := mustLoad(t, s)
		out, err := s.Write(ctx, base, block("r1", 1, v1alpha1.PlannedFailoverPhaseAdmitting), nil)
		wantOutcome(t, out, controller.WriteNotCommitted, err)
		if err := s.MergeSnapshot(controller.TopologySnapshot{ActiveSite: "alpha"}); err == nil {
			t.Fatal("the topology manager's status write was not denied")
		}
		m := controller.TerminalMarker{RunID: "r1", Outcome: v1alpha1.PlannedFailoverOutcomeNotStarted,
			Reason: v1alpha1.PlannedFailoverReasonStatusStalled}
		out, err = s.WriteTerminalMarker(ctx, base, m)
		wantOutcome(t, out, controller.WriteCommitted, err)
		s.BeginPoll(3)
		out, err = s.Write(ctx, mustLoad(t, s), block("r1", 1, v1alpha1.PlannedFailoverPhaseAdmitting), nil)
		wantOutcome(t, out, controller.WriteCommitted, err)
	})
	t.Run("readOutageIsUnknown", func(t *testing.T) {
		s, _ := newTestStore(t)
		s.ReadOutage(1)
		if _, err := s.Load(ctx, s.nn); err == nil {
			t.Fatal("load succeeded during a read outage")
		}
		out, err := s.Settle(ctx, s.nn, "1", "r1", 1)
		wantOutcome(t, out, controller.WriteUnknown, err)
	})
	t.Run("deadOperator", func(t *testing.T) {
		s, _ := newTestStore(t)
		base := mustLoad(t, s)
		s.dead = func() bool { return true }
		out, err := s.Write(ctx, base, block("r1", 1, v1alpha1.PlannedFailoverPhaseAdmitting), nil)
		wantOutcome(t, out, controller.WriteUnknown, err)
		if _, err := s.Load(ctx, s.nn); err == nil {
			t.Fatal("a dead operator loaded the object")
		}
	})
	t.Run("gone", func(t *testing.T) {
		s, _ := newTestStore(t)
		base := mustLoad(t, s)
		s.obj = nil
		out, err := s.Write(ctx, base, block("r1", 1, v1alpha1.PlannedFailoverPhaseAdmitting), nil)
		wantOutcome(t, out, controller.WriteGone, err)
		if obj, err := s.Load(ctx, s.nn); obj != nil || err != nil {
			t.Fatalf("load of a gone object: %v %v", obj, err)
		}
	})
}

func TestSimStore_MetadataWrites(t *testing.T) {
	s, clk := newTestStore(t)
	ctx := context.Background()
	s.SetRequest("beta")

	base := mustLoad(t, s)
	out, err := s.ConsumeRequest(ctx, base)
	wantOutcome(t, out, controller.WriteCommitted, err)
	if _, ok := mustLoad(t, s).GetAnnotations()[v1alpha1.PlannedFailoverAnnotation]; ok {
		t.Fatal("request not consumed")
	}
	// Conditioned: the old base now conflicts.
	out, err = s.ConsumeRequest(ctx, base)
	wantOutcome(t, out, controller.WriteConflict, err)

	// Terminal marker: written, cleared only by its own run.
	m := controller.TerminalMarker{RunID: "r1", Outcome: v1alpha1.PlannedFailoverOutcomeManualRecoveryRequired}
	out, err = s.WriteTerminalMarker(ctx, mustLoad(t, s), m)
	wantOutcome(t, out, controller.WriteCommitted, err)
	if !holdStands(mustLoad(t, s)) || plannedHoldOf(mustLoad(t, s)).RunID != "r1" {
		t.Fatal("a ManualRecoveryRequired marker must stand as a hold")
	}
	rv := s.ResourceVersion()
	out, err = s.ClearTerminalMarker(ctx, mustLoad(t, s), "r2")
	wantOutcome(t, out, controller.WriteCommitted, err)
	if s.ResourceVersion() != rv || markerRunID(mustLoad(t, s).GetAnnotations()[v1alpha1.PlannedFailoverTerminalAnnotation]) != "r1" {
		t.Fatal("another run cleared the marker")
	}
	out, err = s.ClearTerminalMarker(ctx, mustLoad(t, s), "r1")
	wantOutcome(t, out, controller.WriteCommitted, err)
	if holdStands(mustLoad(t, s)) {
		t.Fatal("marker not cleared")
	}

	// Deadline starts are written once per run and never moved forward.
	start, out, err := s.MarkDeadlineStart(ctx, mustLoad(t, s), controller.DeadlineStatusStall, "r1", clk.Now())
	wantOutcome(t, out, controller.WriteCommitted, err)
	clk.Advance(90 * time.Second)
	again, out, err := s.MarkDeadlineStart(ctx, mustLoad(t, s), controller.DeadlineStatusStall, "r1", clk.Now())
	wantOutcome(t, out, controller.WriteCommitted, err)
	if !again.Equal(start) {
		t.Fatalf("deadline start moved: %v → %v", start, again)
	}
	other, _, _ := s.MarkDeadlineStart(ctx, mustLoad(t, s), controller.DeadlineStatusStall, "r2", clk.Now())
	if !other.After(start) {
		t.Fatal("a new run must restart the deadline")
	}
	out, err = s.ClearDeadlineStart(ctx, mustLoad(t, s), controller.DeadlineStatusStall, "r1")
	wantOutcome(t, out, controller.WriteCommitted, err)
	if v := mustLoad(t, s).GetAnnotations()[v1alpha1.PlannedFailoverStallSinceAnnotation]; !strings.HasPrefix(v, "r2:since=") {
		t.Fatalf("r1 cleared r2's deadline: %q", v)
	}
	if _, out, _ := s.MarkDeadlineStart(ctx, mustLoad(t, s), controller.DeadlineKind("bogus"), "r1", clk.Now()); out == controller.WriteCommitted {
		t.Fatal("unknown deadline kind committed")
	}

	// Hold acknowledgement.
	s.mu.Lock()
	s.patchAnnotationsLocked("ack", map[string]string{v1alpha1.AcknowledgePlannedFailoverHoldAnnotation: "r1"})
	s.mu.Unlock()
	out, err = s.DeleteHoldAck(ctx, mustLoad(t, s), "r9")
	wantOutcome(t, out, controller.WriteCommitted, err)
	if mustLoad(t, s).GetAnnotations()[v1alpha1.AcknowledgePlannedFailoverHoldAnnotation] != "r1" {
		t.Fatal("a non-matching ack delete removed the ack")
	}
	out, err = s.DeleteHoldAck(ctx, mustLoad(t, s), "r1")
	wantOutcome(t, out, controller.WriteCommitted, err)
	if _, ok := mustLoad(t, s).GetAnnotations()[v1alpha1.AcknowledgePlannedFailoverHoldAnnotation]; ok {
		t.Fatal("ack not deleted")
	}
}

func TestSimStore_ClearHold(t *testing.T) {
	s, _ := newTestStore(t)
	ctx := context.Background()
	held := block("r1", 4, v1alpha1.PlannedFailoverPhaseFailed)
	out, err := s.Write(ctx, mustLoad(t, s), held, func(st *v1alpha1.MysqlFailoverGroupStatus) {
		setHoldRecord(st, "r1", "alpha")
	})
	wantOutcome(t, out, controller.WriteCommitted, err)
	if !holdStands(mustLoad(t, s)) || !holdNames(&mustLoad(t, s).Status, "r1") {
		t.Fatal("hold not set")
	}

	// Another run's ClearHold leaves the condition.
	out, err = s.ClearHold(ctx, mustLoad(t, s), "r2", nil)
	wantOutcome(t, out, controller.WriteCommitted, err)
	if !holdStands(mustLoad(t, s)) {
		t.Fatal("another run's ClearHold removed the condition")
	}

	// Ambiguous but committed: the settlement recognises the removal.
	s.AmbiguousWrites(1)
	final := block("r1", 5, v1alpha1.PlannedFailoverPhaseFailed)
	out, err = s.ClearHold(ctx, mustLoad(t, s), "r1", &final)
	wantOutcome(t, out, controller.WriteCommitted, err)
	obj := mustLoad(t, s)
	if holdStands(obj) || obj.Status.PlannedFailover.Revision != 5 {
		t.Fatalf("hold not cleared in one write: %+v", obj.Status)
	}
	if c := apimeta.FindStatusCondition(obj.Status.Conditions, v1alpha1.ConditionAutomaticFailoverHeld); c == nil ||
		c.Status != metav1.ConditionFalse || c.Reason != v1alpha1.ReasonPlannedFailoverHoldAcknowledged {
		t.Fatalf("hold condition after the acknowledgement = %+v, want False/%s", c, v1alpha1.ReasonPlannedFailoverHoldAcknowledged)
	}
}

func TestSimStore_SnapshotMergeContends(t *testing.T) {
	s, _ := newTestStore(t)
	ctx := context.Background()
	var kinds []string
	s.onCommit = func(kind string, _, _ *v1alpha1.MysqlFailoverGroup) { kinds = append(kinds, kind) }
	base := mustLoad(t, s)
	if err := s.MergeSnapshot(controller.TopologySnapshot{ActiveSite: "alpha", TopologyGeneration: 2}); err != nil {
		t.Fatal(err)
	}
	out, err := s.Write(ctx, base, block("r1", 1, v1alpha1.PlannedFailoverPhaseAdmitting), nil)
	wantOutcome(t, out, controller.WriteConflict, err)
	// An identical snapshot is a no-op; an older generation is refused.
	rv := s.ResourceVersion()
	_ = s.MergeSnapshot(controller.TopologySnapshot{ActiveSite: "alpha", TopologyGeneration: 2})
	_ = s.MergeSnapshot(controller.TopologySnapshot{ActiveSite: "beta", TopologyGeneration: 1})
	if s.ResourceVersion() != rv || mustLoad(t, s).Status.ActiveSite != "alpha" {
		t.Fatal("no-op or stale snapshot moved the object")
	}
	if err := s.PatchAnnotations("failoverRecord", map[string]string{"k": "v"}); err != nil || s.ResourceVersion() != rv+1 {
		t.Fatal("annotation patch did not move rv")
	}
	if strings.Join(kinds, ",") != "snapshot,failoverRecord" {
		t.Fatalf("commit observer saw %v", kinds)
	}
}

// TestOOBReplay_RegressionIsDetected: the OOB replay's oracle flags a
// stored record that moved backwards (K037); the production recorder and
// the late delivery of an older patch both leave it alone.
func TestOOBReplay_RegressionIsDetected(t *testing.T) {
	tr := twoSiteHandTrial(15, true, 20)
	tr.Actors = Actors{Enabled: true}
	r := &trialRunner{trial: tr, logger: discardLogger(), operatorDeadUntil: -1,
		kindsSeen: map[string]struct{}{}, reasonsSeen: map[string]struct{}{}}
	r.setup(false)
	pl := r.pl
	ctx := context.Background()
	t0 := r.clk.Now().UTC().Truncate(time.Second)
	older := controller.FailoverRecord{LastFailover: t0, LastFailoverTarget: "beta"}
	newer := controller.FailoverRecord{LastFailover: t0.Add(time.Minute), LastFailoverTarget: "alpha"}
	for _, rec := range []controller.FailoverRecord{older, newer} {
		if err := pl.recordOOB(ctx, rec); err != nil {
			t.Fatal(err)
		}
	}
	pl.replayOOB()
	if got, _ := pl.storedOOB(); got.LastFailoverTarget != "alpha" || len(pl.mon.violations) != 0 {
		t.Fatalf("replay regressed the record to %+v (violations %v)", got, pl.mon.violations)
	}
	if pl.counts["oobLateDelivered"] != 1 || pl.counts["oobLateRefused"] != 1 {
		t.Fatalf("late delivery of the older patch: counts %v, want delivered and refused", pl.counts)
	}

	// A writer that bypasses the recorder's rules is caught.
	before, _ := pl.storedOOB()
	a, err := controller.FailoverRecordAnnotations(older)
	if err != nil {
		t.Fatal(err)
	}
	if err := pl.store.PatchAnnotations("test", a); err != nil {
		t.Fatal(err)
	}
	pl.checkOOBNotRegressed(before, "a test write")
	if len(pl.mon.violations) != 1 || pl.mon.violations[0].Invariant != "OOBRecordRegressed" {
		t.Fatalf("violations %v, want OOBRecordRegressed", pl.mon.violations)
	}
}

// TestSettle_TwoFencesInOneTickBothMoveTheResourceVersion: two settlement
// fences at the same fake-clock instant each move the resourceVersion, so
// NotCommitted always rests on a fence that landed (K029).
func TestSettle_TwoFencesInOneTickBothMoveTheResourceVersion(t *testing.T) {
	s, _ := newTestStore(t)
	never := func(*v1alpha1.MysqlFailoverGroup) bool { return false }
	for i := range 2 {
		base := mustLoad(t, s)
		rv := s.ResourceVersion()
		out, _ := s.settle(context.Background(), base, never)
		if out != controller.WriteNotCommitted || s.ResourceVersion() == rv {
			t.Fatalf("fence %d: outcome %s, rv %d -> %d; want NotCommitted with the rv moved", i+1, out, rv, s.ResourceVersion())
		}
	}
}

// TestMetadataNoOpsAreDecidedFromBase: as production's patchAnnotations and
// ClearHold, a change that is empty against base succeeds without an API
// call, however stale base is, and cannot meet a scripted fault; the hold
// acknowledgement is compared trimmed (K106).
func TestMetadataNoOpsAreDecidedFromBase(t *testing.T) {
	ctx := context.Background()
	s, clk := newTestStore(t)
	base := mustLoad(t, s)
	start, out, _ := s.MarkDeadlineStart(ctx, base, controller.DeadlineStatusStall, "r1", clk.Now())
	if out != controller.WriteCommitted {
		t.Fatalf("mark: %s", out)
	}
	stale := mustLoad(t, s)
	s.SetRequest("beta") // the server moves on; stale already carries the deadline
	got, out, err := s.MarkDeadlineStart(ctx, stale, controller.DeadlineStatusStall, "r1", clk.Now())
	if out != controller.WriteCommitted || err != nil || !got.Equal(start) {
		t.Fatalf("re-mark on a stale base that has it: %s %v start %v, want Committed %v", out, err, got, start)
	}
	if out, _ := s.ClearTerminalMarker(ctx, stale, "r1"); out != controller.WriteCommitted {
		t.Fatalf("clearing an absent marker on a stale base: %s, want Committed", out)
	}

	s.ConflictWrites(1)
	if out, _ := s.ClearHold(ctx, mustLoad(t, s), "r1", nil); out != controller.WriteCommitted {
		t.Fatalf("no-op ClearHold: %s, want Committed", out)
	}
	if out, _ := s.Write(ctx, mustLoad(t, s), v1alpha1.PlannedFailoverStatus{RunID: "r1", Revision: 1,
		Phase: v1alpha1.PlannedFailoverPhaseAdmitting}, nil); out != controller.WriteConflict {
		t.Fatalf("the scripted conflict was consumed by a no-op ClearHold (next write: %s)", out)
	}

	if err := s.PatchAnnotations("ack", map[string]string{v1alpha1.AcknowledgePlannedFailoverHoldAnnotation: " r1 \n"}); err != nil {
		t.Fatal(err)
	}
	if out, _ := s.DeleteHoldAck(ctx, mustLoad(t, s), "r1"); out != controller.WriteCommitted {
		t.Fatalf("delete ack: %s", out)
	}
	if _, ok := s.Object().GetAnnotations()[v1alpha1.AcknowledgePlannedFailoverHoldAnnotation]; ok {
		t.Fatal("a whitespace-padded acknowledgement survived its deletion")
	}
}

// TestMergeSnapshot_TwoStepWrite: the topology manager's status write reads,
// computes, and commits after other writers moved on (K042), as the
// runner's updateCRStatus: what it computed replaces the status, except
// the subtrees the runner re-reads (the planned block and hold, the
// AutomaticFailoverHeld condition), which keep the concurrent writer's.
func TestMergeSnapshot_TwoStepWrite(t *testing.T) {
	ctx := context.Background()
	s, clk := newTestStore(t)
	pf := v1alpha1.PlannedFailoverStatus{RunID: "r1", Revision: 1, Phase: v1alpha1.PlannedFailoverPhaseDraining, TargetSite: "beta"}
	yielded := false
	s.yield = func(point string) {
		if point != YieldSnapshotCommit || yielded {
			return
		}
		yielded = true
		// A planned write lands between the snapshot's read and its commit.
		if out, err := s.Write(ctx, mustLoad(t, s), pf, func(st *v1alpha1.MysqlFailoverGroupStatus) {
			setHoldRecord(st, "r1", "alpha")
		}); out != controller.WriteCommitted {
			t.Fatalf("concurrent write: %s %v", out, err)
		}
	}
	if err := s.MergeSnapshot(controller.TopologySnapshot{ActiveSite: "alpha", LastFailover: clk.Now(), LastFailoverTarget: "alpha"}); err != nil {
		t.Fatal(err)
	}
	if !yielded {
		t.Fatal("the snapshot write did not yield between its read and its commit")
	}
	st := s.Object().Status
	if st.ActiveSite != "alpha" || st.LastFailoverTarget() != "alpha" {
		t.Fatalf("computed fields not written: %+v", st)
	}
	if st.PlannedFailover == nil || st.PlannedFailover.RunID != "r1" || st.PlannedFailoverHold == nil ||
		apimeta.FindStatusCondition(st.Conditions, v1alpha1.ConditionAutomaticFailoverHeld) == nil {
		t.Fatalf("the concurrent planned block, hold or condition was overwritten: %+v", st)
	}
}

// TestMergeSnapshot_ReMergesTheFailoverRecord: the snapshot write's second
// step merges the failover record again over the live one, as
// updateCRStatus does (K058). A planned run's terminal write that applies a
// newer record between the two steps keeps it: the snapshot, taken before
// the manager's own next poll, still carries the older record and must not
// move status.lastFailover back. A snapshot with a newer record than the
// live one still moves it forward.
func TestMergeSnapshot_ReMergesTheFailoverRecord(t *testing.T) {
	ctx := context.Background()
	s, clk := newTestStore(t)
	old := clk.Now().Add(-2 * time.Hour)
	stale := controller.TopologySnapshot{ActiveSite: "alpha", LastFailover: old, LastFailoverTarget: "alpha", ObservedAt: clk.Now()}
	if err := s.MergeSnapshot(stale); err != nil {
		t.Fatal(err)
	}
	confirmed := metav1.NewTime(clk.Now())
	const watermark = "00000000-0000-0000-0000-000000000001:1-9"
	pf := v1alpha1.PlannedFailoverStatus{RunID: "r1", Revision: 1, Phase: v1alpha1.PlannedFailoverPhaseSucceeded, TargetSite: "beta"}
	yielded := false
	s.yield = func(point string) {
		if point != YieldSnapshotCommit || yielded {
			return
		}
		yielded = true
		if out, err := s.Write(ctx, mustLoad(t, s), pf, func(st *v1alpha1.MysqlFailoverGroupStatus) {
			st.SetLastFailover(&confirmed, "beta", watermark)
		}); out != controller.WriteCommitted {
			t.Fatalf("concurrent terminal write: %s %v", out, err)
		}
	}
	if err := s.MergeSnapshot(stale); err != nil {
		t.Fatal(err)
	}
	if !yielded {
		t.Fatal("the snapshot write did not yield between its read and its commit")
	}
	st := s.Object().Status
	if lf := st.LastFailoverTime(); lf == nil || !lf.Time.Equal(confirmed.Time) || st.LastFailoverTarget() != "beta" ||
		st.PromotionGtidExecuted() != watermark {
		t.Fatalf("the snapshot write replaced the newer failover record: %+v", st.LastFailover)
	}
	s.yield = nil
	later := clk.Now().Add(time.Minute)
	if err := s.MergeSnapshot(controller.TopologySnapshot{ActiveSite: "alpha", LastFailover: later, LastFailoverTarget: "alpha",
		ObservedAt: later}); err != nil {
		t.Fatal(err)
	}
	st = s.Object().Status
	if lf := st.LastFailoverTime(); lf == nil || !lf.Time.Equal(later) || st.LastFailoverTarget() != "alpha" {
		t.Fatalf("a newer snapshot record did not move status.lastFailover forward: %+v", st.LastFailover)
	}
}
