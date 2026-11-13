package controller

import (
	"context"
	"time"

	v1alpha1 "github.com/shipstream/bloodraven/api/v1alpha1"
)

// Hooks for the deterministic simulation of the paced rollout
// (internal/dst, rollout_*.go) and its component tests (test/component).
// Both run the production pacer, gate, drift check and ordered updater from
// outside this package; these are the entry points and the real-time bounds
// they cannot reach otherwise. They are not used in production.

// CheckSpecDriftForSim runs the runner's drift check of fg against tm, as
// syncManagers does on every pass for a registered manager: the sites whose
// Deployment differs from the running render become tm's drift, except
// those the paced rollout holds (rolloutHoldsSite).
func (r *TopologyManagerRunner) CheckSpecDriftForSim(ctx context.Context, fg *v1alpha1.MysqlFailoverGroup, tm *TopologyManager) {
	r.checkSpecDrift(ctx, fg, tm)
}

// SetRolloutWaitForSim bounds ReconcileSiteDeployment's wait for a site's
// rollout (production: 5 minutes, probed every 2 seconds). The wait runs on
// the wall clock, so a simulation whose pods never become Ready shortens
// it instead of sitting it out.
func (r *MysqlFailoverGroupReconciler) SetRolloutWaitForSim(timeout, interval time.Duration) {
	r.rolloutWaitTimeout, r.rolloutPollInterval = timeout, interval
}

// SetRolledSiteTimeoutForTest bounds the ordered update's wait for a site
// it rolled to replicate and to have a Ready pod (production: 5 minutes, on
// the wall clock).
func (u *UpdateController) SetRolledSiteTimeoutForTest(d time.Duration) { u.rolledSiteTimeout = d }

// WaitOrderedUpdateIdleForSim returns once tm runs no ordered update
// attempt and no stall recheck: both run on goroutines of their own that a
// Poll starts. A simulation calls it after each Poll, so that the attempt
// has finished before the next actor moves and a trial replays exactly. The
// caller must be the only one polling tm.
func (tm *TopologyManager) WaitOrderedUpdateIdleForSim() {
	tm.updateWorkers.Wait()
	for {
		tm.mu.RLock()
		checking := tm.updateStallChecking
		tm.mu.RUnlock()
		if !checking {
			return
		}
		time.Sleep(20 * time.Microsecond)
	}
}

// CheckUpdateForSim runs Poll's ordered-update step by itself: it starts an
// update attempt when drift is recorded and nothing forbids one, or the
// stall recheck otherwise, and reports whether an attempt started. A
// simulation polls with ApplyUpdate unset (Poll then starts no attempt),
// sets it, calls this and waits with WaitOrderedUpdateIdleForSim, so the
// attempt's goroutine never runs beside the rest of the Poll that started
// it.
func (tm *TopologyManager) CheckUpdateForSim(ctx context.Context) bool {
	return tm.checkUpdate(ctx)
}
