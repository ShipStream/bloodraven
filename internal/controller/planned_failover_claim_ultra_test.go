package controller

import (
	"context"
	"slices"
	"strings"
	"testing"
	"time"

	v1alpha1 "github.com/shipstream/bloodraven/api/v1alpha1"
)

// K022: a run that ended SourceLeftFenced with FenceNotAttributable, and
// whose terminal status record has not landed (only the marker has), is
// not named for re-assert after a restart, and the marker-based terminal
// record keeps the warning.
func TestUltraK022_MarkerKeepsFenceNotAttributable(t *testing.T) {
	pf := opsRun(v1alpha1.PlannedFailoverPhaseRollingBack)
	marker := TerminalMarker{RunID: pf.RunID, Outcome: v1alpha1.PlannedFailoverOutcomeSourceLeftFenced,
		Reason: v1alpha1.PlannedFailoverReasonLagTimeout,
		Warnings: []v1alpha1.PlannedFailoverWarning{v1alpha1.PlannedFailoverWarningFenceNotAttributable,
			v1alpha1.PlannedFailoverWarningStatusStalled}}
	fg := opsGroup(pf)
	fg.Annotations = map[string]string{PlannedFailoverTerminalAnnotation: marker.String()}

	if _, ok := plannedFenceAttributableEnd(fg); ok {
		t.Fatal("a marker carrying FenceNotAttributable authorises a named re-assert of the source")
	}
	f := newOpsFixture(t, "")
	f.tm.RestorePlannedFailoverStateForSim(fg)
	f.tm.mu.RLock()
	named := f.tm.lastActiveSiteNamed && f.tm.lastActiveSite == pf.SourceSite
	f.tm.mu.RUnlock()
	if named {
		t.Fatal("the restart named a source whose fence the run could not attribute")
	}

	// Control: the same marker without the warning names the source.
	ok := TerminalMarker{RunID: pf.RunID, Outcome: v1alpha1.PlannedFailoverOutcomeSourceLeftFenced,
		Reason: v1alpha1.PlannedFailoverReasonLagTimeout}
	fg.Annotations[PlannedFailoverTerminalAnnotation] = ok.String()
	if _, attributable := plannedFenceAttributableEnd(fg); !attributable {
		t.Fatal("an attributable marker no longer names the source")
	}

	// The marker-based terminal record keeps the warning.
	fg.Annotations[PlannedFailoverTerminalAnnotation] = marker.String()
	h := newPFEngineHarness(t, fg)
	h.stepUntil(5, func(cur *v1alpha1.MysqlFailoverGroup) bool {
		return cur.Status.PlannedFailover != nil && cur.Status.PlannedFailover.Result != nil
	})
	res := h.fg().Status.PlannedFailover.Result
	if res.Outcome != v1alpha1.PlannedFailoverOutcomeSourceLeftFenced ||
		!slices.Contains(res.Warnings, v1alpha1.PlannedFailoverWarningFenceNotAttributable) ||
		!slices.Contains(res.Warnings, v1alpha1.PlannedFailoverWarningStatusStalled) {
		t.Fatalf("marker-based terminal record = %+v, want SourceLeftFenced with FenceNotAttributable and StatusStalled", res)
	}
}

// K024: when a replacement manager's uncached read fails, the
// predecessor's naming of a source a run left fenced carries over.
func TestUltraK024_ReplacementKeepsPredecessorNamingWithoutView(t *testing.T) {
	old := newOpsFixture(t, "")
	old.tm.mu.Lock()
	old.tm.lastActiveSite, old.tm.lastActiveSiteNamed = "dc1", true
	old.tm.mu.Unlock()

	repl := newOpsFixture(t, "")
	repl.tm.mu.Lock()
	repl.tm.lastActiveSite, repl.tm.lastActiveSiteNamed = "dc1", false // observed from status only
	repl.tm.mu.Unlock()
	if !repl.tm.carryNamedActiveSite(old.tm) {
		t.Fatal("the predecessor's naming was not carried")
	}
	repl.tm.mu.RLock()
	got, named := repl.tm.lastActiveSite, repl.tm.lastActiveSiteNamed
	repl.tm.mu.RUnlock()
	if got != "dc1" || !named {
		t.Fatalf("replacement lastActiveSite = %q named=%v, want dc1 named", got, named)
	}

	// A replacement that named something itself keeps its own naming.
	own := newOpsFixture(t, "")
	own.tm.mu.Lock()
	own.tm.lastActiveSite, own.tm.lastActiveSiteNamed = "dc2", true
	own.tm.mu.Unlock()
	if own.tm.carryNamedActiveSite(old.tm) {
		t.Fatal("the predecessor's naming overrode the replacement's own")
	}
	// A predecessor naming a site the replacement no longer has carries nothing.
	gone := newOpsFixture(t, "")
	old.tm.mu.Lock()
	old.tm.lastActiveSite = "dc9"
	old.tm.mu.Unlock()
	if gone.tm.carryNamedActiveSite(old.tm) {
		t.Fatal("a removed site was carried")
	}
}

// K076: a retired manager's record whose target the replacement's
// configuration no longer has is not carried.
func TestUltraK076_CarryRejectsRemovedSite(t *testing.T) {
	f := newOpsFixture(t, "")
	at := f.clk.Now().Add(-time.Minute)
	if f.tm.carryFailoverRecord(FailoverRecord{LastFailover: at, LastFailoverTarget: "dc3", PromotionGtidExecuted: opsF}) {
		t.Fatal("a record naming a site the manager does not have was carried")
	}
	f.tm.mu.RLock()
	target, auth := f.tm.lastFailoverTarget, f.tm.deploymentActiveSite
	f.tm.mu.RUnlock()
	if target == "dc3" || auth == "dc3" {
		t.Fatalf("carried a removed site: lastFailoverTarget %q, deploymentActiveSite %q", target, auth)
	}
	if !f.tm.carryFailoverRecord(FailoverRecord{LastFailover: at, LastFailoverTarget: "dc2", PromotionGtidExecuted: opsF}) {
		t.Fatal("a record naming a configured site was not carried")
	}
}

// K087: hydration on a live manager is published through the
// follower-mutation gate and waits for manager-local work a grant waits
// for, instead of installing the claim beside an admitted recovery or an
// in-place restore.
func TestUltraK087_LiveHydrationWaitsForAdmittedWork(t *testing.T) {
	pf := opsRun(v1alpha1.PlannedFailoverPhaseWaitingForLag)

	f := newOpsFixture(t, "")
	if !f.tm.followerMutationGate.enter() {
		t.Fatal("could not admit a follower mutation")
	}
	d, err := f.topo.EnsureClaim(context.Background(), f.ref, opsGroup(pf))
	if err != nil || d.Kind != ClaimWait || d.WaitReason != v1alpha1.PlannedFailoverWaitRecoveryInFlight {
		t.Fatalf("EnsureClaim beside an admitted follower mutation = %+v, %v; want Wait{RecoveryInFlight}", d, err)
	}
	if f.reg.slot.Held() {
		t.Fatal("the claim was hydrated while a follower mutation ran on the sites")
	}
	f.tm.followerMutationGate.exit()
	if d, err := f.topo.EnsureClaim(context.Background(), f.ref, opsGroup(pf)); err != nil || d.Kind != ClaimGranted {
		t.Fatalf("EnsureClaim once the mutation exited = %+v, %v; want Granted", d, err)
	}

	g := newOpsFixture(t, "")
	g.tm.mu.Lock()
	g.tm.topologyFrozen = true
	g.tm.mu.Unlock()
	d, err = g.topo.EnsureClaim(context.Background(), g.ref, opsGroup(pf))
	if err != nil || d.Kind != ClaimWait || d.WaitReason != v1alpha1.PlannedFailoverWaitRestoreInFlight || g.reg.slot.Held() {
		t.Fatalf("EnsureClaim beside an in-place restore = %+v, %v (held %v); want Wait{RestoreInFlight}", d, err, g.reg.slot.Held())
	}
}

// K104: a standing hold is reported as a hold that needs the
// acknowledgement, not as a planned failover in progress.
func TestUltraK104_HoldRefusalNamesTheAcknowledgement(t *testing.T) {
	f := newOpsFixture(t, "")
	f.reg.slot.hydrate(plannedClaim{mode: claimModeRun, runID: "run-1"})
	f.tm.mu.Lock()
	run := f.tm.bootstrapClaimRefusedLocked()
	f.tm.mu.Unlock()
	if run != "a planned failover is in progress" {
		t.Fatalf("refusal under a run claim = %q", run)
	}
	f.reg.slot.toHold("run-1")
	f.tm.mu.Lock()
	held := f.tm.bootstrapClaimRefusedLocked()
	f.tm.mu.Unlock()
	if !strings.Contains(held, AcknowledgePlannedFailoverHoldAnnotation+"=run-1") || strings.Contains(held, "in progress") {
		t.Fatalf("refusal under a hold = %q; want it to name the acknowledgement", held)
	}
	ok, why := f.tm.ReserveKeyringRotation("dc2")
	if ok || !strings.Contains(why, AcknowledgePlannedFailoverHoldAnnotation+"=run-1") {
		t.Fatalf("rotation under a hold = %v %q", ok, why)
	}
}
