package controller

import (
	"slices"
	"testing"

	apimeta "k8s.io/apimachinery/pkg/api/meta"

	v1alpha1 "github.com/shipstream/bloodraven/api/v1alpha1"
)

// TestHoldAckLifecycle: a ManualRecoveryRequired run sets the hold (status
// condition + hold-mode slot); new requests are refused; an
// acknowledgement for another run is ignored; the matching one clears
// status.plannedFailoverHold (the condition turns False), then releases the
// slot, then deletes the acknowledgement.
func TestHoldAckLifecycle(t *testing.T) {
	h := pfHarnessAt(t, v1alpha1.PlannedFailoverPhasePromoting)
	h.topo.promote = func(PlannedRunRef) (PromoteOutcome, error) {
		return PromoteOutcome{Kind: PromoteInconsistent, Reason: v1alpha1.PlannedFailoverReasonForeignWritable, MayHaveWritten: true}, nil
	}
	h.step()
	if hold := PlannedHoldFrom(h.fg()); hold == nil || hold.RunID != "run-1" {
		t.Fatalf("hold = %+v", hold)
	}
	if held, mode := h.topo.held(); held != "run-1" || !mode {
		t.Fatalf("slot = %q hold=%v", held, mode)
	}

	// A new request is refused while the hold stands; the hold run's record
	// stays.
	h.setAnnotation(v1alpha1.PlannedFailoverAnnotation, "pdx")
	h.step()
	fg := h.fg()
	if _, ok := fg.Annotations[v1alpha1.PlannedFailoverAnnotation]; ok {
		t.Error("request kept while a hold stands")
	}
	if pf := fg.Status.PlannedFailover; pf.RunID != "run-1" || pf.Result.Outcome != v1alpha1.PlannedFailoverOutcomeManualRecoveryRequired {
		t.Errorf("hold run's record replaced: %+v", pf)
	}
	if h.topo.count("claim") != 0 {
		t.Error("a request asked for the claim while a hold stands")
	}

	// An acknowledgement naming another run is ignored.
	h.setAnnotation(v1alpha1.AcknowledgePlannedFailoverHoldAnnotation, "run-0")
	h.step()
	if PlannedHoldFrom(h.fg()) == nil || h.sink.eventCount("PlannedFailoverHoldAcknowledgmentIgnored") != 1 {
		t.Fatalf("a foreign acknowledgement cleared the hold (events %v)", h.sink.events)
	}

	// The matching acknowledgement: condition, then slot, then the ack.
	h.setAnnotation(v1alpha1.AcknowledgePlannedFailoverHoldAnnotation, "run-1")
	h.step()
	fg = h.fg()
	if c := apimeta.FindStatusCondition(fg.Status.Conditions, v1alpha1.ConditionAutomaticFailoverHeld); fg.Status.PlannedFailoverHold != nil ||
		c == nil || c.Status != "False" || c.Reason != v1alpha1.ReasonPlannedFailoverHoldAcknowledged {
		t.Fatalf("the first acknowledged pass left hold %+v, condition %+v; want no hold and False/%s",
			fg.Status.PlannedFailoverHold, c, v1alpha1.ReasonPlannedFailoverHoldAcknowledged)
	}
	if held, _ := h.topo.held(); held != "run-1" {
		t.Fatal("slot released before the acknowledgement order reached it")
	}
	h.step()
	fg = h.fg()
	if held, _ := h.topo.held(); held != "" {
		t.Errorf("slot still held by %q", held)
	}
	if _, ok := fg.Annotations[v1alpha1.AcknowledgePlannedFailoverHoldAnnotation]; ok {
		t.Error("acknowledgement not deleted")
	}
	if got := h.topo.releasesCopy(); !slices.Equal(got, []string{"run-1:toHold", "run-1:ack"}) {
		t.Errorf("releases = %v", got)
	}
	if h.sink.eventCount("PlannedFailoverHoldAcknowledged") != 1 {
		t.Errorf("events = %v", h.sink.events)
	}
}

// TestHoldAck_FailedMarkerDeleteThenRestart: a hold carried by the terminal
// marker alone (the stalled run's status never landed). The acknowledged
// pass commits the terminal record first; the marker delete then fails, and
// the operator restarts: the slot stays held (hydrated from the marker)
// until the marker is gone, and only then is the acknowledgement deleted.
func TestHoldAck_FailedMarkerDeleteThenRestart(t *testing.T) {
	h := pfHarnessAt(t, v1alpha1.PlannedFailoverPhasePromoting)
	h.setAnnotation(v1alpha1.PlannedFailoverTerminalAnnotation, `{"schemaVersion":1,"runID":"run-1","outcome":"ManualRecoveryRequired","reason":"ForeignWritable"}`)
	h.topo.setHeld("run-1", true)
	h.setAnnotation(v1alpha1.AcknowledgePlannedFailoverHoldAnnotation, "run-1")
	h.faults.denyMetaKey = v1alpha1.PlannedFailoverTerminalAnnotation

	h.step() // step 2: the terminal record lands (no condition to remove)
	fg := h.fg()
	if pf := fg.Status.PlannedFailover; pf.Result == nil || pf.Result.Outcome != v1alpha1.PlannedFailoverOutcomeManualRecoveryRequired {
		t.Fatalf("terminal record = %+v", pf)
	}
	if apimeta.FindStatusCondition(fg.Status.Conditions, v1alpha1.ConditionAutomaticFailoverHeld) != nil {
		t.Fatal("the acknowledged pass re-set the hold condition")
	}
	h.step() // step 3: the marker delete fails
	if _, ok := h.fg().Annotations[v1alpha1.PlannedFailoverTerminalAnnotation]; !ok {
		t.Fatal("setup: marker delete should have failed")
	}

	// Restart: a new engine, and startManager hydrates the hold from the
	// marker into the (new process's) slot.
	h.restart()
	h.topo.setHeld("", false)
	h.topo.setHeld("run-1", true)
	h.step()
	if held, mode := h.topo.held(); held != "run-1" || !mode {
		t.Fatalf("slot = %q hold=%v; the hold must stand while the marker does", held, mode)
	}
	if _, ok := h.fg().Annotations[v1alpha1.AcknowledgePlannedFailoverHoldAnnotation]; !ok {
		t.Fatal("acknowledgement deleted before the marker")
	}
	if plannedFailoverOwnsSites(h.fg()) || plannedFailoverActive(h.fg()) {
		t.Fatal("the terminal record left a live run behind")
	}

	h.faults.denyMetaKey = ""
	h.step() // the marker goes
	h.step() // both records gone: release, then delete the acknowledgement
	fg = h.fg()
	if _, ok := fg.Annotations[v1alpha1.PlannedFailoverTerminalAnnotation]; ok {
		t.Error("marker kept")
	}
	if _, ok := fg.Annotations[v1alpha1.AcknowledgePlannedFailoverHoldAnnotation]; ok {
		t.Error("acknowledgement kept")
	}
	if held, _ := h.topo.held(); held != "" {
		t.Errorf("slot still held by %q", held)
	}
	if h.topo.count("ensure") != 0 || h.topo.count("promote") != 0 {
		t.Errorf("a held run was driven as a live run: %v", h.topo.calls)
	}
}
