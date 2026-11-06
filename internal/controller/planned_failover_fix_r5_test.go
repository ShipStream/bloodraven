package controller

import (
	"context"
	"slices"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	v1alpha1 "github.com/shipstream/bloodraven/api/v1alpha1"
	"github.com/shipstream/bloodraven/internal/dragonfly"
	"github.com/shipstream/bloodraven/internal/mysql"
	"github.com/shipstream/bloodraven/internal/platform"
	"github.com/shipstream/bloodraven/internal/state"
)

// Fix round 5, item 1: the run finishes a Dragonfly takeover only on a
// REPLTAKEOVER success reply this process received for the run. Every
// other case is an unconfirmed takeover: no Dragonfly role changes (the
// source is not demoted, evicted or deleted, the target is not relabelled;
// only the source's traffic label is put back), the takeover is recorded
// Failed with DragonflyPromotionFailed, and onSyncTimeout applies.
func TestFixR5_DragonflyTakeoverFinishesOnlyOnInProcessSuccessReply(t *testing.T) {
	t.Run("success reply finishes", func(t *testing.T) {
		// From WaitingForDragonflySync: this process captures the offset,
		// writes the intent itself, and gets REPLTAKEOVER's success reply.
		fg := plannedFailoverFGWithDragonfly("Fail")
		sourcePod := makeDragonflyPod(fg.Name, "iad", "master", true)
		targetPod := makeDragonflyPod(fg.Name, "pdx", "replica", true)
		r, c, _ := pfDFReconciler(t, fg, sourcePod, targetPod)
		source := &fakeDragonflyConn{info: dragonfly.ReplicationInfo{Role: "master", MasterReplOffset: 100}}
		target := &pfTakeoverConn{fakeDragonflyConn: &fakeDragonflyConn{info: dragonfly.ReplicationInfo{
			Role: "slave", MasterLinkStatus: "up", SlaveReplOffset: 100}}}
		installFakeDragonflyConn(t, r, map[string]DragonflyConnection{pfDFPdx: target, pfDFIad: source})
		for range 4 {
			pfDFPass(t, r, fgNN(fg))
		}
		pf := fetchFG(t, r, fgNN(fg)).Status.PlannedFailover
		if pf.Phase != v1alpha1.PlannedFailoverPhasePromoting || pf.Dragonfly.Takeover.State != v1alpha1.DragonflyTakeoverSucceeded {
			t.Fatalf("block = %s %+v, want Promoting with the takeover Succeeded", pf.Phase, pf.Dragonfly.Takeover)
		}
		if target.takeovers != 1 || len(source.clientKillTypes) != 1 || !pfDFPodGone(t, c, sourcePod) {
			t.Errorf("REPLTAKEOVER %d, CLIENT KILL %v, source deleted %v; want the takeover finished once",
				target.takeovers, source.clientKillTypes, pfDFPodGone(t, c, sourcePod))
		}
		if l := pfDFPodLabels(t, c, targetPod); l[labelDragonflyRole] != "master" || l[labelDragonflyTraffic] != dragonflyTrafficEnabled {
			t.Errorf("target labels = %v, want the serving master", l)
		}
	})
	t.Run("lost reply in-process", func(t *testing.T) {
		// The server promoted the target, but the reply never arrived: this
		// process cannot confirm it.
		fg := plannedFailoverFGWithDragonfly("Proceed")
		sourcePod := makeDragonflyPod(fg.Name, "iad", "master", true)
		targetPod := makeDragonflyPod(fg.Name, "pdx", "replica", true)
		r, c, _ := pfDFReconciler(t, fg, sourcePod, targetPod)
		var n atomic.Int32
		source := &fakeDragonflyConn{info: dragonfly.ReplicationInfo{Role: "master", MasterReplOffset: 100}}
		target := &pfLostReplyConn{fakeDragonflyConn: &fakeDragonflyConn{info: dragonfly.ReplicationInfo{
			Role: "slave", MasterLinkStatus: "up", SlaveReplOffset: 100}}, promote: true, takeovers: &n}
		installFakeDragonflyConn(t, r, map[string]DragonflyConnection{pfDFPdx: target, pfDFIad: source})
		for range 4 {
			pfDFPass(t, r, fgNN(fg))
		}
		got := fetchFG(t, r, fgNN(fg))
		pf := got.Status.PlannedFailover
		if pf.Phase != v1alpha1.PlannedFailoverPhasePromoting || pf.Dragonfly.Takeover.State != v1alpha1.DragonflyTakeoverFailed ||
			pf.Dragonfly.Reason != v1alpha1.DragonflyPlannedReasonPromotionFailed || pf.Dragonfly.Sessions != v1alpha1.DragonflySessionsLost ||
			!strings.Contains(pf.Dragonfly.Message, "unconfirmed") {
			t.Fatalf("block = %s %+v, want Promoting (Proceed) with the takeover Failed as unconfirmed", pf.Phase, pf.Dragonfly)
		}
		if got := n.Load(); got != 1 {
			t.Errorf("REPLTAKEOVER sent %d times, want 1", got)
		}
		pfDFAssertSourceKept(t, c, source, sourcePod, targetPod)
		if m := effectiveDragonflyMasterSite(got); m != "iad" {
			t.Errorf("Dragonfly master = %q, want the source left as it was", m)
		}
	})
	for _, tc := range []struct {
		name, policy, role string
		want               v1alpha1.PlannedFailoverPhase
	}{
		{"restart with an Intended record, target a replica, Fail", "Fail", "slave", v1alpha1.PlannedFailoverPhaseRollingBack},
		{"restart with an Intended record, target master, Proceed", "Proceed", "master", v1alpha1.PlannedFailoverPhasePromoting},
	} {
		t.Run(tc.name, func(t *testing.T) {
			// An earlier process wrote the intent (and stripped the source's
			// traffic label) before it restarted: this process never sent
			// REPLTAKEOVER for the run and sends none now, even to a target
			// that would take over.
			fg := pfDFIntended(tc.policy, time.Now().Add(-5*time.Second))
			sourcePod := makeDragonflyPod(fg.Name, "iad", "master", false)
			targetPod := makeDragonflyPod(fg.Name, "pdx", "replica", true)
			r, c, _ := pfDFReconciler(t, fg, sourcePod, targetPod)
			pfDFForgetIntent(r, "run-1")
			source := &fakeDragonflyConn{info: dragonfly.ReplicationInfo{Role: "master", MasterReplOffset: 100}}
			target := &pfTakeoverConn{fakeDragonflyConn: &fakeDragonflyConn{info: dragonfly.ReplicationInfo{
				Role: tc.role, MasterLinkStatus: "up", SlaveReplOffset: 100}}}
			installFakeDragonflyConn(t, r, map[string]DragonflyConnection{pfDFPdx: target, pfDFIad: source})
			pfDFTakeoverPasses(t, r, fgNN(fg))
			pf := fetchFG(t, r, fgNN(fg)).Status.PlannedFailover
			if pf.Phase != tc.want || pf.Dragonfly.Takeover.State != v1alpha1.DragonflyTakeoverFailed ||
				pf.Dragonfly.Reason != v1alpha1.DragonflyPlannedReasonPromotionFailed || !strings.Contains(pf.Dragonfly.Message, "unconfirmed") {
				t.Fatalf("block = %s %+v, want %s with the takeover Failed as unconfirmed", pf.Phase, pf.Dragonfly, tc.want)
			}
			if tc.want == v1alpha1.PlannedFailoverPhaseRollingBack &&
				(pf.Rollback == nil || pf.Rollback.Reason != v1alpha1.PlannedFailoverReasonDragonflyPromotionFailed) {
				t.Errorf("rollback = %+v, want DragonflyPromotionFailed", pf.Rollback)
			}
			if target.takeovers != 0 {
				t.Errorf("REPLTAKEOVER sent %d times on a cross-process re-run", target.takeovers)
			}
			// The source's traffic label is put back; nothing else moves.
			pfDFAssertSourceKept(t, c, source, sourcePod, targetPod)
			if l := pfDFPodLabels(t, c, targetPod); l[labelDragonflyRole] != "replica" {
				t.Errorf("target labels = %v, want them untouched", l)
			}
		})
	}
}

// Fix round 5, item 2 (K072): an unreadable target is not a verified
// re-fence. With the source already writable, the rollback retries while
// the target cannot be read and, at the rollback deadline, ends RolledBack
// with TargetSuperReadOnlyNotRestored (no hold).
func TestFixR5_K072_UnreadableTargetIsNotAVerifiedRefence(t *testing.T) {
	f := newOpsFixture(t, "")
	f.hold(false)
	f.src.mu.Lock()
	f.src.ro, f.src.sro = false, false // an earlier unfence whose readback was lost
	f.src.mu.Unlock()
	f.tgt.mu.Lock()
	f.tgt.repl, f.tgt.down = nil, true
	f.tgt.mu.Unlock()
	pf := opsUltraRollbackRun(f, v1alpha1.PlannedFailoverReasonTargetLost, false)
	res, err := f.topo.Rollback(context.Background(), f.ref, pf)
	if err != nil || res.Outcome != "" {
		t.Fatalf("Rollback = %+v, %v; want a retry while the target cannot be read", res, err)
	}
	f.clk.Advance(plannedFailoverRollbackDeadline - plannedRequeuePoll)
	res, err = f.topo.Rollback(context.Background(), f.ref, pf)
	if err != nil || res.Outcome != v1alpha1.PlannedFailoverOutcomeRolledBack ||
		!slices.Contains(res.Warnings, v1alpha1.PlannedFailoverWarningTargetSuperReadOnlyNotRestored) ||
		!slices.Contains(res.Warnings, v1alpha1.PlannedFailoverWarningTargetReplicationNotRestored) {
		t.Fatalf("at the deadline: Rollback = %+v, %v; want RolledBack with TargetSuperReadOnlyNotRestored", res, err)
	}
	// The target comes back readable and re-fences: a clean RolledBack.
	f2 := newOpsFixture(t, "")
	f2.hold(false)
	f2.src.mu.Lock()
	f2.src.ro, f2.src.sro = false, false
	f2.src.mu.Unlock()
	f2.tgt.mu.Lock()
	f2.tgt.repl, f2.tgt.ro, f2.tgt.sro = nil, true, false
	f2.tgt.mu.Unlock()
	if res, err := f2.topo.Rollback(context.Background(), f2.ref, opsUltraRollbackRun(f2, v1alpha1.PlannedFailoverReasonTargetLost, false)); err != nil ||
		res.Outcome != v1alpha1.PlannedFailoverOutcomeRolledBack || slices.Contains(res.Warnings, v1alpha1.PlannedFailoverWarningTargetSuperReadOnlyNotRestored) {
		t.Fatalf("readable target: Rollback = %+v, %v; want a clean RolledBack", res, err)
	}
}

// Fix round 5, item 3 (I001): the per-statement guard refuses when the
// manager that classified the stage was replaced. A spec change that makes
// the target read-only replaces the manager between the gate's probes and
// the SRO clear (the claim carries over); the retired manager sends
// nothing more, and the next pass, under the replacement, refuses the
// target: Aborted{TargetNotPromotable}. The target is never made writable.
func TestFixR5_I001_ManagerReplacedBetweenGateProbesAndSROClear(t *testing.T) {
	f := newOpsFixture(t, "")
	f.hold(false)
	f.fenced()
	var replaced bool
	SetSimHooksForSim(f.topo, SimHooks{Yield: func(point string) {
		if point != YieldGateProbeForSim || replaced {
			return
		}
		replaced = true
		next := opsReplacementManager(t, f, state.SiteRoleReadOnly)
		f.tm.retire()
		f.reg.mu.Lock()
		f.reg.current = next
		f.reg.mu.Unlock()
	}})
	pf := opsRun(v1alpha1.PlannedFailoverPhasePromoting)
	out, err := f.topo.Promote(context.Background(), f.ref, pf)
	if err != nil || out.Kind != PromoteUnconfirmed || !replaced {
		t.Fatalf("Promote across the replacement = %+v, %v (replaced %v); want nothing more sent (Unconfirmed, retried)", out, err, replaced)
	}
	if got := f.tgt.statements(); slices.Contains(got, "sro=OFF") || slices.Contains(got, "ro=OFF") {
		t.Fatalf("target statements = %v; the retired manager cleared the target's fences", got)
	}
	out, err = f.topo.Promote(context.Background(), f.ref, pf)
	if err != nil || out.Kind != PromoteAborted || out.Reason != v1alpha1.PlannedFailoverReasonTargetNotPromotable {
		t.Fatalf("next pass = %+v, %v; want Aborted{TargetNotPromotable}", out, err)
	}
	if got := f.tgt.statements(); slices.Contains(got, "sro=OFF") || slices.Contains(got, "ro=OFF") {
		t.Fatalf("target statements = %v; the target was made writable", got)
	}
}

// opsReplacementManager is the manager a spec change starts for f's group:
// the same sites and checkers with the target's role set to targetRole,
// wired to the same claim slot.
func opsReplacementManager(t *testing.T, f *opsFixture, targetRole state.SiteRole) *TopologyManager {
	t.Helper()
	sites := []SiteTopologyConfig{
		{Name: "dc1", Role: state.SiteRolePrimaryCandidate, Host: "mysql-dc1", LBIP: "1.1.1.1"},
		{Name: "dc2", Role: targetRole, Host: "mysql-dc2", LBIP: "2.2.2.2"},
	}
	cfg := TopologyConfig{Name: "lion", Namespace: "ns", Sites: sites, PollInterval: int64(50 * time.Millisecond),
		FailureThreshold: 3, RecoveryThreshold: 2}
	tm := NewTopologyManagerWithClock(cfg, []mysql.Checker{f.src, f.tgt}, NewFailoverController(testLogger()), nil, nil,
		BootstrapConfig{ReplUser: "repl", ReplPassword: "secret"}, newMockTainter(), platform.NewHub(testLogger()), &mockDNS{}, testLogger(), f.clk)
	tm.failoverCooldown = 0
	tm.mu.Lock()
	tm.sites[0].state = state.StateWritable
	tm.sites[1].state = state.StateReadOnly
	tm.mu.Unlock()
	tm.SetReadyForSim()
	WireRegisteredManager(tm, ManagerWiring{Slot: f.reg.slot})
	return tm
}
