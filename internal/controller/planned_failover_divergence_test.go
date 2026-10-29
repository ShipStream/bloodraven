package controller

import (
	"context"
	"strings"
	"testing"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	v1alpha1 "github.com/shipstream/bloodraven/api/v1alpha1"
)

// A target with recorded divergence is refused before anything is fenced,
// like a mid-rotation target, with a message naming the set.
func TestValidatePlannedFailover_RefusesDivergentTarget(t *testing.T) {
	fg := plannedFailoverFG("pdx")
	count := int64(221)
	fg.Status.Sites[1].RecoveryState = recoveryStateBlocked
	fg.Status.Sites[1].DivergentGtid = issue202ErrantGTID
	fg.Status.Sites[1].DivergentTransactionCount = &count

	result, reason, err := validatePlannedFailoverRequest(fg, PlannedFailoverRequest{Site: "pdx"}, time.Now(), false)

	if result != PlannedFailoverReject || reason != "DivergentTarget" {
		t.Fatalf("validate = %v/%q (%v), want Reject/DivergentTarget", result, reason, err)
	}
	if err == nil || !strings.Contains(err.Error(), "221") || !strings.Contains(err.Error(), issue202ErrantGTID) {
		t.Errorf("error = %v, want it to name 221 transactions and the set", err)
	}
}

// The fenced source cannot gain transactions, so a target holding any GTID
// outside SourceGtidAtFence has history the source lacks. The lag wait
// rolls back (source unfenced, Failed/DivergentTarget) instead of
// promoting it.
func TestPlannedFailoverWaitingForLag_RollsBackOnTargetExtras(t *testing.T) {
	fg := plannedFailoverFG("")
	start := metav1.NewTime(time.Now().Add(-2 * time.Second))
	fg.Status.PlannedFailover = &v1alpha1.PlannedFailoverStatus{
		Phase:             v1alpha1.PlannedFailoverPhaseWaitingForLag,
		Target:            "pdx",
		SourcePrimary:     "iad",
		StartTime:         &start,
		DrainStartTime:    &start,
		LagWaitStartTime:  &start,
		SourceGtidAtFence: issue202PrimaryGTID,
		MaxLagWait:        &metav1.Duration{Duration: 30 * time.Second},
	}
	nn := fgNN(fg)
	iad := &mockMySQL{readOnly: true, superReadOnly: true, gtidExecuted: issue202PrimaryGTID}
	pdx := &mockMySQL{readOnly: true, superReadOnly: true, gtidExecuted: issue202ReplicaGTID}

	r, _ := newReconciler(fg)
	r.Runner = plannedDrainRunner(nn, iad, pdx)

	if _, err := r.reconcilePlannedFailover(context.Background(), fetchFG(t, r, nn)); err != nil {
		t.Fatalf("reconcile: %v", err)
	}
	pf := fetchFG(t, r, nn).Status.PlannedFailover
	if pf.Phase != v1alpha1.PlannedFailoverPhaseFailed || pf.Reason != "DivergentTarget" {
		t.Fatalf("phase=%q reason=%q (%s), want Failed/DivergentTarget", pf.Phase, pf.Reason, pf.Message)
	}
	if !strings.Contains(pf.Message, "221") {
		t.Errorf("message = %q, want the extra count", pf.Message)
	}
	if iad.superReadOnly {
		t.Error("source left fenced after the rollback")
	}
	if !pdx.readOnly {
		t.Error("divergent target was promoted")
	}
}

// The last-line check in PlannedPromote refuses a target the manager knows
// is divergent even when the CR status the validation read did not show
// it yet; the source is unfenced and nothing is promoted.
func TestPlannedFailoverPromoting_RollsBackDivergentTarget(t *testing.T) {
	fg := plannedFailoverFG("")
	start := metav1.NewTime(time.Now().Add(-2 * time.Second))
	fg.Status.PlannedFailover = &v1alpha1.PlannedFailoverStatus{
		Phase:             v1alpha1.PlannedFailoverPhasePromoting,
		Target:            "pdx",
		SourcePrimary:     "iad",
		StartTime:         &start,
		SourceGtidAtFence: issue202PrimaryGTID,
	}
	nn := fgNN(fg)
	iad := &mockMySQL{readOnly: true, superReadOnly: true, gtidExecuted: issue202PrimaryGTID}
	pdx := &mockMySQL{readOnly: true, superReadOnly: true, gtidExecuted: issue202PrimaryGTID}
	r, _ := newReconciler(fg)
	r.Runner = plannedDrainRunner(nn, iad, pdx)
	tm, err := r.Runner.plannedFailoverManager(nn)
	if err != nil {
		t.Fatal(err)
	}
	tm.SetRecoveryBlockedFrom("pdx", issue202ErrantGTID, 221, divergenceDetectedByAudit, "iad")

	if _, err := r.reconcilePlannedFailover(context.Background(), fetchFG(t, r, nn)); err != nil {
		t.Fatalf("reconcile: %v", err)
	}
	pf := fetchFG(t, r, nn).Status.PlannedFailover
	if pf.Phase != v1alpha1.PlannedFailoverPhaseFailed || pf.Reason != "DivergentTarget" {
		t.Fatalf("phase=%q reason=%q (%s), want Failed/DivergentTarget", pf.Phase, pf.Reason, pf.Message)
	}
	if iad.superReadOnly || !pdx.readOnly {
		t.Errorf("after rollback iad super_read_only=%v, pdx read_only=%v; want iad unfenced and pdx not promoted", iad.superReadOnly, pdx.readOnly)
	}
}
