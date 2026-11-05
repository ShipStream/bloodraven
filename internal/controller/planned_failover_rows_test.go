package controller

import (
	"context"
	"log/slog"
	"testing"
	"time"

	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"

	v1alpha1 "github.com/shipstream/bloodraven/api/v1alpha1"
	"github.com/shipstream/bloodraven/internal/state"
)

// The §14 interleaving rows whose mechanism is the Dragonfly split (§8):
// Dragonfly is not modeled in the DST nor in the component harness, so they
// are pinned here, on the controller's Dragonfly fixtures. The other rows
// are in test/component/planned_failover_claim_test.go.

// Row 16: an upgrade drops the shared flag. The pause sources are split:
// ending a Dragonfly snapshot upgrade neither unpauses the manager nor
// releases a planned claim held beside it.
func TestPF_I16_UpgradeDoesNotDropClaim(t *testing.T) {
	mgr := NewDragonflyManager(nil, nil, slog.Default(), client.ObjectKey{Namespace: "ns", Name: "fg"}, time.Second)
	slot := &PlannedClaimSlot{}
	mgr.SetPlannedOwner(slot.Held)
	mgr.SetUpgradePaused(true)
	if !slot.hydrate(plannedClaim{mode: claimModeRun, runID: "run-1", source: "dc1", target: "dc2"}) {
		t.Fatal("setup: claim not installed")
	}
	mgr.SetUpgradePaused(false) // the upgrade finishes while the run holds the claim
	if !slot.Held() {
		t.Fatal("ending the upgrade released the planned claim")
	}
	if !mgr.paused() {
		t.Fatal("ending the upgrade unpaused Dragonfly while the run owns it")
	}
	if !slot.release("run-1", ReleaseTerminal) || mgr.paused() {
		t.Fatal("after the run's release the manager must run again")
	}
}

// Row 27: a waiting request is not the Dragonfly owner. Neither pending
// phase owns the group's Dragonfly or pauses its manager; a run that owns
// the sites does both.
func TestPF_I27_WaitingRequestNotDragonflyOwner(t *testing.T) {
	mgr := NewDragonflyManager(nil, nil, slog.Default(), client.ObjectKey{Namespace: "ns", Name: "fg"}, time.Second)
	slot := &PlannedClaimSlot{}
	mgr.SetPlannedOwner(slot.Held)
	fg := fgWithDragonflyEnabledAndActive()
	for _, phase := range []v1alpha1.PlannedFailoverPhase{v1alpha1.PlannedFailoverPhaseAdmitting, v1alpha1.PlannedFailoverPhaseDeferred} {
		fg.Status.PlannedFailover = &v1alpha1.PlannedFailoverStatus{RunID: "run-1", Phase: phase, Target: "dc2"}
		if plannedFailoverOwnsDragonfly(fg, slot.Held()) {
			t.Errorf("a %s request owns the group's Dragonfly", phase)
		}
		if mgr.paused() {
			t.Errorf("a %s request paused the Dragonfly manager", phase)
		}
	}
	fg.Status.PlannedFailover.Phase = v1alpha1.PlannedFailoverPhaseFencing
	slot.hydrate(plannedClaim{mode: claimModeRun, runID: "run-1", source: "dc1", target: "dc2"})
	if !plannedFailoverOwnsDragonfly(fg, slot.Held()) || !mgr.paused() {
		t.Error("a run that holds the claim does not own the group's Dragonfly")
	}
}

// Row 28: the upgrade supersedes itself on MySQL authority re-derived on
// each reconcile: from the registered manager's active site, or, with no
// manager, from the durable failover record. A failover from before the
// upgrade started does not supersede it.
func TestPF_I28_DragonflyUpgradeSupersededByMySQLAuthority(t *testing.T) {
	start := time.Now().Add(-time.Minute).Truncate(time.Second)
	r := &MysqlFailoverGroupReconciler{}

	during := upgradeDuringEmergency(v1alpha1.DragonflyUpgradePhaseSavingSnapshot, start, start.Add(10*time.Second))
	if why, active := r.dragonflyUpgradeSupersededByMySQL(during, during.Status.Dragonfly.Upgrade); why == "" || active != "dc2" {
		t.Fatalf("a failover during the upgrade did not supersede it (why %q, active %q)", why, active)
	}
	before := upgradeDuringEmergency(v1alpha1.DragonflyUpgradePhaseSavingSnapshot, start, start.Add(-10*time.Second))
	if why, _ := r.dragonflyUpgradeSupersededByMySQL(before, before.Status.Dragonfly.Upgrade); why != "" {
		t.Fatalf("a failover from before the upgrade superseded it: %q", why)
	}

	// With a registered manager its live active site decides, even when the
	// durable record has not caught up: the manager sees dc2 writable.
	idle := upgradeDuringEmergency(v1alpha1.DragonflyUpgradePhaseSavingSnapshot, start, time.Time{})
	nn := types.NamespacedName{Namespace: idle.Namespace, Name: idle.Name}
	f := newOpsFixture(t, "")
	f.tm.mu.Lock()
	f.tm.sites[0].state, f.tm.sites[1].state = state.StateReadOnly, state.StateWritable
	f.tm.mu.Unlock()
	r.Runner = &TopologyManagerRunner{logger: slog.Default(), managers: map[types.NamespacedName]*managedTopology{nn: {tm: f.tm}}}
	if site, ok := r.Runner.ActiveSite(nn); !ok || site != "dc2" {
		t.Fatalf("setup: the manager's active site is %q (%v), want dc2", site, ok)
	}
	if why, active := r.dragonflyUpgradeSupersededByMySQL(idle, idle.Status.Dragonfly.Upgrade); why == "" || active != "dc2" {
		t.Fatalf("the live manager authority did not supersede the upgrade (why %q, active %q)", why, active)
	}
	f.tm.mu.Lock()
	f.tm.sites[0].state, f.tm.sites[1].state = state.StateWritable, state.StateReadOnly
	f.tm.mu.Unlock()
	if why, _ := r.dragonflyUpgradeSupersededByMySQL(idle, idle.Status.Dragonfly.Upgrade); why != "" {
		t.Fatalf("the upgrade's own active site superseded it: %q", why)
	}
}

// Row 6, Dragonfly part: a run that fails the Dragonfly sync under the
// `fail` policy rolls back with nothing written to the MySQL target.
func TestPF_I06_DragonflyFailRollsBack(t *testing.T) {
	fg := plannedFailoverFGWithDragonfly("fail")
	fg.Status.PlannedFailover.Dragonfly.SourceOffsetState = v1alpha1.DragonflyOffsetUnavailable
	r, _, _ := pfDFReconciler(t, fg)
	pfDFPass(t, r, fgNN(fg))
	pf := fetchFG(t, r, fgNN(fg)).Status.PlannedFailover
	if pf.Phase != v1alpha1.PlannedFailoverPhaseRollingBack || pf.Rollback == nil ||
		pf.Rollback.Reason != v1alpha1.PlannedFailoverReasonDragonflySyncTimeout {
		t.Fatalf("block %s rollback %+v, want RollingBack{DragonflySyncTimeout}", pf.Phase, pf.Rollback)
	}
	if pf.Promotion != nil {
		t.Fatal("a Promoting intent was written before the Dragonfly sync passed")
	}
}

// Further row: the pass wall time is bounded (§2.4 step 8). Every facade
// call of a pass runs under the pass's deadline: 15s for an ordinary pass,
// 30s for the Promoting statements pass, so a hung statement ends the pass
// rather than the reconcile worker.
func TestPF_PassWallTimeBounded(t *testing.T) {
	for _, tc := range []struct {
		phase v1alpha1.PlannedFailoverPhase
		op    string
		bound time.Duration
	}{
		{v1alpha1.PlannedFailoverPhaseWaitingForLag, "probe", plannedPassOrdinary},
		{v1alpha1.PlannedFailoverPhasePromoting, "promote", plannedPassPromoting},
	} {
		t.Run(string(tc.phase), func(t *testing.T) {
			h := pfHarnessAt(t, tc.phase)
			var left time.Duration
			seen := false
			h.topo.onCtx = func(op string, ctx context.Context) {
				if op != tc.op || seen {
					return
				}
				seen = true
				dl, ok := ctx.Deadline()
				if !ok {
					t.Errorf("%s ran with no deadline", op)
					return
				}
				left = time.Until(dl)
			}
			start := time.Now()
			h.step()
			if !seen {
				t.Fatalf("the pass never called %s", tc.op)
			}
			if spent := time.Since(start); left > tc.bound || left < tc.bound-spent-time.Second {
				t.Fatalf("%s saw %s left of the pass, want the %s bound", tc.op, left, tc.bound)
			}
		})
	}
}
