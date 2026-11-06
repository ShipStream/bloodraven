package controller

import (
	"context"
	"testing"
	"time"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	apimeta "k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"

	v1alpha1 "github.com/shipstream/bloodraven/api/v1alpha1"
	"github.com/shipstream/bloodraven/internal/clock"
)

// pfStoreFixture is the production store over a fake client with a fault
// script, seeded with run-1 in WaitingForLag.
func pfStoreFixture(t *testing.T) (*apiPlannedFailoverStore, *pfFaults, types.NamespacedName, func() *v1alpha1.MysqlFailoverGroup) {
	t.Helper()
	fg := plannedFailoverFG("")
	fg.Status.PlannedFailover = pfRunAt(v1alpha1.PlannedFailoverPhaseWaitingForLag, time.Now())
	faults := &pfFaults{}
	c := newPFFakeClient(faults, fg)
	nn := fgNN(fg)
	store := newAPIPlannedFailoverStore(c, c, clock.NewFakeClock(time.Now()))
	load := func() *v1alpha1.MysqlFailoverGroup {
		t.Helper()
		got, err := store.Load(context.Background(), nn)
		if err != nil || got == nil {
			t.Fatalf("load: %v %v", got, err)
		}
		return got
	}
	return store, faults, nn, load
}

func pfNextRevision(base *v1alpha1.MysqlFailoverGroup) v1alpha1.PlannedFailoverStatus {
	next := base.Status.PlannedFailover.DeepCopy()
	next.Revision++
	next.Message = "next"
	return *next
}

// A write that commits and then loses its response is found committed by
// its (runId, revision).
func TestStore_Settle_CommittedThenTimeout(t *testing.T) {
	store, faults, _, load := pfStoreFixture(t)
	base := load()
	faults.statusFaults = []pfWriteFault{pfWriteCommitTimeout}
	out, err := store.Write(context.Background(), base, pfNextRevision(base), nil)
	if out != WriteCommitted || err != nil {
		t.Fatalf("Write = %s, %v; want Committed", out, err)
	}
	if got := load(); got.Status.PlannedFailover.Revision != base.Status.PlannedFailover.Revision+1 {
		t.Errorf("revision = %d", got.Status.PlannedFailover.Revision)
	}
}

// A write dropped without committing is fenced: the base version moves,
// and a late replay of the same write conditioned on it is refused.
func TestStore_Settle_DroppedIsFencedNotCommitted(t *testing.T) {
	store, faults, _, load := pfStoreFixture(t)
	base := load()
	faults.statusFaults = []pfWriteFault{pfWriteDropTimeout}
	out, _ := store.Write(context.Background(), base, pfNextRevision(base), nil)
	if out != WriteNotCommitted {
		t.Fatalf("Write = %s, want NotCommitted", out)
	}
	after := load()
	if after.Annotations[PlannedFailoverStatusFenceAnnotation] == "" || after.ResourceVersion == base.ResourceVersion {
		t.Fatalf("settlement did not fence: rv %s → %s, annotations %v", base.ResourceVersion, after.ResourceVersion, after.Annotations)
	}
	// The late commit of the lost write (conditioned on the old version).
	late := base.DeepCopy()
	late.Status.PlannedFailover = pfPtr(pfNextRevision(base))
	if err := store.client.Status().Update(context.Background(), late); !apierrors.IsConflict(err) {
		t.Fatalf("late commit after the fence = %v, want a conflict", err)
	}
}

// A base version that moved already proves the write can never land; no
// fence is written.
func TestStore_Settle_MovedVersionIsNotCommitted(t *testing.T) {
	store, _, nn, load := pfStoreFixture(t)
	base := load()
	moved := base.DeepCopy()
	moved.Status.UpdatePhase = "someone else"
	if err := store.client.Status().Update(context.Background(), moved); err != nil {
		t.Fatal(err)
	}
	out, _ := store.Settle(context.Background(), nn, base.ResourceVersion, "run-1", base.Status.PlannedFailover.Revision+1)
	if out != WriteNotCommitted {
		t.Fatalf("Settle = %s, want NotCommitted", out)
	}
	if load().Annotations[PlannedFailoverStatusFenceAnnotation] != "" {
		t.Error("a fence was written although the version had moved")
	}
}

// A conflicting fence sends settlement back to its read; the retry decides.
func TestStore_Settle_FenceConflictRetries(t *testing.T) {
	store, faults, nn, load := pfStoreFixture(t)
	base := load()
	faults.metaFaults = []pfWriteFault{pfWriteConflict}
	out, _ := store.Settle(context.Background(), nn, base.ResourceVersion, "run-1", base.Status.PlannedFailover.Revision+1)
	if out != WriteNotCommitted {
		t.Fatalf("Settle = %s, want NotCommitted after the retried fence", out)
	}
}

// A fence that fails for another reason leaves the write Unknown.
func TestStore_Settle_UnfenceableIsUnknown(t *testing.T) {
	store, faults, nn, load := pfStoreFixture(t)
	base := load()
	faults.metaFaults = []pfWriteFault{pfWriteForbidden}
	if out, _ := store.Settle(context.Background(), nn, base.ResourceVersion, "run-1", base.Status.PlannedFailover.Revision+1); out != WriteUnknown {
		t.Fatalf("Settle = %s, want Unknown", out)
	}
	faults.metaFaults = []pfWriteFault{pfWriteConflict, pfWriteConflict, pfWriteConflict}
	if out, _ := store.Settle(context.Background(), nn, base.ResourceVersion, "run-1", base.Status.PlannedFailover.Revision+1); out != WriteUnknown {
		t.Fatalf("Settle after three conflicts = %s, want Unknown", out)
	}
}

// Definite responses are classified without settlement.
func TestStore_Write_DefiniteOutcomes(t *testing.T) {
	for _, tc := range []struct {
		fault pfWriteFault
		want  WriteOutcome
	}{
		{pfWriteOK, WriteCommitted},
		{pfWriteConflict, WriteConflict},
		{pfWriteForbidden, WriteNotCommitted},
	} {
		store, faults, _, load := pfStoreFixture(t)
		base := load()
		faults.statusFaults = []pfWriteFault{tc.fault}
		if out, _ := store.Write(context.Background(), base, pfNextRevision(base), nil); out != tc.want {
			t.Errorf("fault %d: Write = %s, want %s", tc.fault, out, tc.want)
		}
		if tc.fault != pfWriteOK && load().Annotations[PlannedFailoverStatusFenceAnnotation] != "" {
			t.Errorf("fault %d: a definite outcome was settled", tc.fault)
		}
	}
	store, _, _, load := pfStoreFixture(t)
	base := load()
	stale := base.DeepCopy()
	if err := store.client.Delete(context.Background(), base); err != nil {
		t.Fatal(err)
	}
	if out, _ := store.Write(context.Background(), stale, pfNextRevision(stale), nil); out != WriteGone {
		t.Errorf("Write on a deleted group = %s, want Gone", out)
	}
}

// §6.4: the terminal marker moves the resourceVersion, so a pending or late
// status write conditioned on the version it read can no longer commit.
func TestStore_TerminalMarkerFencesLateCommit(t *testing.T) {
	store, _, _, load := pfStoreFixture(t)
	base := load()
	m := TerminalMarker{RunID: "run-1", Outcome: v1alpha1.PlannedFailoverOutcomeSourceLeftFenced, Reason: v1alpha1.PlannedFailoverReasonStatusStalled}
	if out, err := store.WriteTerminalMarker(context.Background(), base, m); out != WriteCommitted {
		t.Fatalf("marker = %s, %v", out, err)
	}
	if out, _ := store.Write(context.Background(), base, pfNextRevision(base), nil); out != WriteConflict {
		t.Fatalf("late status write = %s, want Conflict", out)
	}
	after := load()
	if _, marked := plannedFailoverRun(after); !marked {
		t.Error("marker not read back")
	}
	// A stale marker write conflicts too (conditioned metadata write).
	if out, _ := store.WriteTerminalMarker(context.Background(), base, TerminalMarker{RunID: "run-1",
		Outcome: v1alpha1.PlannedFailoverOutcomeNotStarted}); out != WriteConflict {
		t.Errorf("stale marker write = %s, want Conflict", out)
	}
	if out, _ := store.ClearTerminalMarker(context.Background(), after, "run-2"); out != WriteCommitted || load().Annotations[PlannedFailoverTerminalAnnotation] == "" {
		t.Error("a marker of run-1 was cleared for run-2")
	}
	if out, _ := store.ClearTerminalMarker(context.Background(), load(), "run-1"); out != WriteCommitted || load().Annotations[PlannedFailoverTerminalAnnotation] != "" {
		t.Error("run-1's marker not cleared")
	}
}

// The durable deadline start is never moved forward for the same run.
func TestStore_MarkDeadlineStartNeverMovesForward(t *testing.T) {
	store, _, _, load := pfStoreFixture(t)
	t0 := time.Date(2026, 11, 4, 12, 0, 0, 500, time.UTC)
	start, out, err := store.MarkDeadlineStart(context.Background(), load(), DeadlineStatusStall, "run-1", t0)
	if out != WriteCommitted || err != nil || !start.Equal(t0.Truncate(time.Second)) {
		t.Fatalf("first mark = %s %s %v", start, out, err)
	}
	again, out, _ := store.MarkDeadlineStart(context.Background(), load(), DeadlineStatusStall, "run-1", t0.Add(time.Minute))
	if out != WriteCommitted || !again.Equal(start) {
		t.Fatalf("second mark = %s %s, want %s", again, out, start)
	}
	other, _, _ := store.MarkDeadlineStart(context.Background(), load(), DeadlineStatusStall, "run-2", t0.Add(time.Hour))
	if !other.Equal(t0.Add(time.Hour).Truncate(time.Second)) {
		t.Errorf("another run's start = %s", other)
	}
	if out, _ := store.ClearDeadlineStart(context.Background(), load(), DeadlineStatusStall, "run-1"); out != WriteCommitted ||
		load().Annotations[PlannedFailoverStallSinceAnnotation] == "" {
		t.Error("run-1's clear removed run-2's start")
	}
	if out, _ := store.ClearDeadlineStart(context.Background(), load(), DeadlineStatusStall, "run-2"); out != WriteCommitted ||
		load().Annotations[PlannedFailoverStallSinceAnnotation] != "" {
		t.Error("run-2's start not cleared")
	}
}

// ClearHold removes only the hold naming the run (its condition set
// False/PlannedFailoverHoldAcknowledged), with the terminal record when
// given, in one conditioned status write.
func TestStore_ClearHold(t *testing.T) {
	store, _, _, load := pfStoreFixture(t)
	base := load()
	setPlannedHold(&base.Status, v1alpha1.PlannedFailoverHold{RunID: "run-9", Reason: v1alpha1.PlannedFailoverReasonForeignWritable,
		AmbiguousSites: []string{"pdx"}, HoldTime: metav1.Now()}, base.Generation)
	if err := store.client.Status().Update(context.Background(), base); err != nil {
		t.Fatal(err)
	}
	if out, _ := store.ClearHold(context.Background(), load(), "run-1", nil); out != WriteCommitted {
		t.Fatal(out)
	}
	if c := apimeta.FindStatusCondition(load().Status.Conditions, v1alpha1.ConditionAutomaticFailoverHeld); c == nil ||
		c.Status != metav1.ConditionTrue || load().Status.PlannedFailoverHold == nil {
		t.Fatal("the hold of another run was cleared")
	}
	cur := load()
	final := cur.Status.PlannedFailover.DeepCopy()
	final.RunID, final.Revision, final.Phase = "run-9", 1, v1alpha1.PlannedFailoverPhaseFailed
	final.Result = &v1alpha1.PlannedFailoverResult{Outcome: v1alpha1.PlannedFailoverOutcomeManualRecoveryRequired, CompletionTime: metav1.Now()}
	if out, _ := store.ClearHold(context.Background(), cur, "run-9", final); out != WriteCommitted {
		t.Fatal(out)
	}
	got := load()
	c := apimeta.FindStatusCondition(got.Status.Conditions, v1alpha1.ConditionAutomaticFailoverHeld)
	if got.Status.PlannedFailoverHold != nil || c == nil || c.Status != metav1.ConditionFalse ||
		c.Reason != v1alpha1.ReasonPlannedFailoverHoldAcknowledged || got.Status.PlannedFailover.RunID != "run-9" {
		t.Fatalf("after ClearHold: hold %+v conditions %v block %+v", got.Status.PlannedFailoverHold, got.Status.Conditions, got.Status.PlannedFailover)
	}
}

func pfPtr[T any](v T) *T { return &v }
