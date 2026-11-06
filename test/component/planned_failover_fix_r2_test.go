package component

import (
	"errors"
	"testing"
	"time"

	v1alpha1 "github.com/shipstream/bloodraven/api/v1alpha1"
	"github.com/shipstream/bloodraven/internal/controller"
)

// releasedWithStalledPublishing runs a planned dc1 → dc2 failover until the
// confirmed tail released the claim with the Publishing write denied.
func releasedWithStalledPublishing(t *testing.T, h *pfHarness) {
	t.Helper()
	h.request("dc2")
	h.runUntil(60, inPhase(v1alpha1.PlannedFailoverPhasePromoting))
	h.store.DenyStatus(1 << 20) // the Publishing write cannot land
	for range 4 {
		h.pass()
	}
	if h.tgt.isReadOnly() {
		t.Fatal("setup: the target was not promoted")
	}
	pf := h.pf()
	if pf.Phase != v1alpha1.PlannedFailoverPhasePromoting {
		t.Fatalf("setup: the Publishing write landed: %+v", pf)
	}
	if run, _, ok := h.held(); ok {
		t.Fatalf("setup: the claim of %s is still held", run)
	}
	if !controller.PlannedOwnRecordDurableForSim(h.store.Object(), pf, h.clk.Now()) {
		t.Fatal("setup: the run's own failover record is not durable")
	}
}

// O-m1(b): a restart after the confirmed tail released the claim, with only
// the Publishing write left, does not hydrate the claim back from the
// Promoting block: automation (a split-brain fence) is not deferred, and the
// run publishes without a claim.
func TestFixR2_RecordedPromotionIsNotHydrated(t *testing.T) {
	h := newPFHarness(t, pfOpts{})
	releasedWithStalledPublishing(t, h)
	h.newProcess()
	if run, _, ok := h.held(); ok {
		t.Fatalf("the restart hydrated the claim of %s from a Promoting block whose promotion is recorded", run)
	}
	h.pass()
	if run, _, ok := h.held(); ok {
		t.Fatalf("the engine re-claimed %s for a recorded promotion", run)
	}
	h.src.setRO(false, false) // an external writer: two writable sites
	fenced := false
	for range 10 {
		h.tick()
		h.pass()
		if h.src.isSuperReadOnly() {
			fenced = true
			break
		}
	}
	if !fenced {
		t.Fatal("dc1 stayed writable beside the promoted dc2 after the restart")
	}
	if h.tgt.isReadOnly() {
		t.Fatal("the split-brain fence fenced the promoted target")
	}
	h.store.ClearFaults()
	pf := h.runUntil(40, terminal)
	h.wantOutcome(pf, v1alpha1.PlannedFailoverOutcomeSucceeded, "")
	if h.holdCondition() {
		t.Fatal("a recorded promotion ended with an automation hold")
	}
}

// O-m1(c): a released run whose Publishing write stalls is ended by the
// terminal marker within plannedReleasedStallDeadline, not the 2-minute
// status stall. A later emergency failover then replaces the run's record,
// and a restart finds a marked run instead of hydrating a claim whose
// re-proof would hold the group (AuthorityChanged).
func TestFixR2_RestartAfterLaterEmergencyFailoverDoesNotHold(t *testing.T) {
	h := newPFHarness(t, pfOpts{cooldown: 15 * time.Second})
	releasedWithStalledPublishing(t, h)
	marked := false
	for range 20 {
		h.tick()
		h.pass()
		if m, ok := controller.ParseTerminalMarker(h.store.Object().GetAnnotations()[controller.PlannedFailoverTerminalAnnotation]); ok {
			if m.Outcome != v1alpha1.PlannedFailoverOutcomeSucceeded {
				t.Fatalf("marker %+v, want Succeeded", m)
			}
			marked = true
			break
		}
	}
	if !marked {
		t.Fatal("a released run with a durable record was not ended by the marker within 20s")
	}
	// dc2 dies after the cooldown; emergency failover promotes dc1.
	h.tgt.setError(errors.New("dc2 down"))
	promoted := false
	for range 40 {
		h.tick()
		h.pass()
		if !h.src.isReadOnly() {
			promoted = true
			break
		}
	}
	if !promoted {
		t.Fatal("setup: emergency failover did not promote dc1")
	}
	h.newProcess()
	for range 10 {
		h.tick()
		h.pass()
	}
	if _, hold, ok := h.held(); ok && hold {
		t.Fatal("a restart after a later emergency failover holds the group")
	}
	if h.holdCondition() {
		t.Fatal("a restart after a later emergency failover set AutomaticFailoverHeld")
	}
	h.store.ClearFaults()
	pf := h.runUntil(40, terminal)
	h.wantOutcome(pf, v1alpha1.PlannedFailoverOutcomeSucceeded, "")
}
