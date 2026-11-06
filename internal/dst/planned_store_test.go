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
		st.LastFailoverTarget = "beta"
	})
	wantOutcome(t, out, controller.WriteCommitted, err)
	if got := mustLoad(t, s); got.Status.LastFailoverTarget != "beta" || got.Status.PlannedFailover.Revision != 2 {
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
		if obj.GetAnnotations()[controller.PlannedFailoverStatusFenceAnnotation] == "" || s.ResourceVersion() != 2 {
			t.Fatalf("settlement did not fence (rv %d, annotations %v)", s.ResourceVersion(), obj.GetAnnotations())
		}
	})
	t.Run("ambiguousCommittedSettlesCommitted", func(t *testing.T) {
		s, _ := newTestStore(t)
		s.AmbiguousWrites(1)
		out, err := s.Write(ctx, mustLoad(t, s), block("r1", 1, v1alpha1.PlannedFailoverPhaseAdmitting), nil)
		wantOutcome(t, out, controller.WriteCommitted, err)
		if mustLoad(t, s).GetAnnotations()[controller.PlannedFailoverStatusFenceAnnotation] != "" {
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
	if _, ok := mustLoad(t, s).GetAnnotations()[controller.PlannedFailoverAnnotation]; ok {
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
	if s.ResourceVersion() != rv || markerRunID(mustLoad(t, s).GetAnnotations()[controller.PlannedFailoverTerminalAnnotation]) != "r1" {
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
	if v := mustLoad(t, s).GetAnnotations()[controller.PlannedFailoverStallSinceAnnotation]; !strings.HasPrefix(v, "r2:since=") {
		t.Fatalf("r1 cleared r2's deadline: %q", v)
	}
	if _, out, _ := s.MarkDeadlineStart(ctx, mustLoad(t, s), controller.DeadlineKind("bogus"), "r1", clk.Now()); out == controller.WriteCommitted {
		t.Fatal("unknown deadline kind committed")
	}

	// Hold acknowledgement.
	s.mu.Lock()
	s.patchAnnotationsLocked("ack", map[string]string{controller.AcknowledgePlannedFailoverHoldAnnotation: "r1"})
	s.mu.Unlock()
	out, err = s.DeleteHoldAck(ctx, mustLoad(t, s), "r9")
	wantOutcome(t, out, controller.WriteCommitted, err)
	if mustLoad(t, s).GetAnnotations()[controller.AcknowledgePlannedFailoverHoldAnnotation] != "r1" {
		t.Fatal("a non-matching ack delete removed the ack")
	}
	out, err = s.DeleteHoldAck(ctx, mustLoad(t, s), "r1")
	wantOutcome(t, out, controller.WriteCommitted, err)
	if _, ok := mustLoad(t, s).GetAnnotations()[controller.AcknowledgePlannedFailoverHoldAnnotation]; ok {
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
