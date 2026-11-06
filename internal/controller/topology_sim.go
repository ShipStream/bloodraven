package controller

import (
	"context"
	"errors"
	"time"

	"k8s.io/apimachinery/pkg/types"

	v1alpha1 "github.com/shipstream/bloodraven/api/v1alpha1"
)

// Hooks for the deterministic simulation's step model of an ordered update
// (internal/dst update actor). They drive the real claim API: the same
// preconditions and the same claim as startUpdate and its goroutine, with
// the MySQL statements of the handoff issued by the simulation one at a
// time. They are not used in production.

// errOrderedUpdateRefused is BeginOrderedUpdateForSim's refusal: startUpdate
// would not have started an update now.
var errOrderedUpdateRefused = errors.New("ordered update refused: another actor holds the sites or an update is pending")

// SimOrderedUpdate is one simulated ordered update between
// BeginOrderedUpdateForSim and FinishOrderedUpdateForSim.
type SimOrderedUpdate struct {
	tm       *TopologyManager
	epoch    uint64
	active   string
	claimed  bool
	finished bool
}

// BeginOrderedUpdateForSim starts an ordered update the way startUpdate
// does once it has decided to: refused while one is pending, the manager
// is retired, a predecessor's update runs, or a planned-failover claim or
// hold is held; otherwise it marks the update pending and registers it on
// updateWorkers. It captures the authority epoch and active site the update
// is planned against.
func (tm *TopologyManager) BeginOrderedUpdateForSim() (*SimOrderedUpdate, error) {
	tm.mu.Lock()
	defer tm.mu.Unlock()
	if tm.deploymentUpdatePending || tm.retired || tm.predecessorUpdateInFlight || tm.plannedClaimHeldLocked() {
		return nil, errOrderedUpdateRefused
	}
	active := tm.activeSiteLocked()
	if active == "" {
		return nil, errOrderedUpdateRefused
	}
	tm.deploymentUpdatePending = true
	tm.updateWorkers.Add(1)
	return &SimOrderedUpdate{tm: tm, epoch: tm.authorityEpoch, active: active}, nil
}

// Active is the site the update was planned against.
func (u *SimOrderedUpdate) Active() string { return u.active }

// ClaimOrderedUpdateForSim takes the update's MySQL mutation window with
// claimOrderedUpdate. It reports whether the claim was granted; a refused
// update must still be finished.
func (u *SimOrderedUpdate) ClaimOrderedUpdateForSim(ctx context.Context) (bool, error) {
	claimed, err := u.tm.claimOrderedUpdate(ctx, u.epoch, u.active)
	u.claimed = claimed
	return claimed, err
}

// RecordHandoffForSim records a completed handoff to target as the update's
// goroutine does: recordFailover, then DNS.
func (u *SimOrderedUpdate) RecordHandoffForSim(ctx context.Context, target, promotionGTID string) {
	tm := u.tm
	tm.recordFailover(ctx, tm.clock.Now(), target, promotionGTID)
	if site := tm.getSite(target); site != nil {
		if err := tm.applyDNS(ctx, target, site.lbIP); err != nil {
			tm.logger.Error("ordered update: DNS flip failed after handoff", "site", target, "error", err)
		}
	}
}

// FinishOrderedUpdateForSim ends the update as its goroutine's deferred
// calls do: the claimed window is finished, the pending flag cleared and
// the worker unregistered. Idempotent.
func (u *SimOrderedUpdate) FinishOrderedUpdateForSim() {
	if u.finished {
		return
	}
	u.finished = true
	tm := u.tm
	if u.claimed {
		tm.finishDeploymentTopologyChange()
		tm.mu.Lock()
		tm.deploymentUpdateRunning = false
		tm.mu.Unlock()
	}
	tm.mu.Lock()
	tm.deploymentUpdatePending = false
	tm.mu.Unlock()
	tm.updateWorkers.Done()
}

// SetSimHooksForSim installs the simulation's yield hooks on a facade built
// by NewPlannedFailoverTopology. Call before first use.
func SetSimHooksForSim(topo PlannedFailoverTopology, hooks SimHooks) {
	if p, ok := topo.(*runnerPlannedTopology); ok {
		p.hooks = hooks
	}
}

// PlannedClaimViewForSim returns tm's claim slot view (ok=false: empty or
// not wired).
func (tm *TopologyManager) PlannedClaimViewForSim() (PlannedClaimView, bool) {
	return tm.plannedClaimSlot().View()
}

// SetReadyForSim marks tm as having completed a poll, as the first Poll
// does. Simulation and component harnesses only.
func (tm *TopologyManager) SetReadyForSim() {
	tm.mu.Lock()
	tm.ready = true
	tm.lastPollTime = tm.clock.Now()
	tm.mu.Unlock()
}

// Yield points the planned-failover engine and its topology facade pass to
// SimHooks.Yield (§15.1). A statement point carries ":<site>:<statement>".
const (
	YieldEngineLoadForSim    = "engine.load"          // after the uncached Load
	YieldEngineWriteForSim   = "engine.write"         // after each store Write
	YieldEngineSettleForSim  = "engine.settle"        // after each Settle
	YieldPeerProbeForSim     = "facade.peerProbe"     // after a live peer probe (grant, fence, rollback)
	YieldBeforePublishForSim = "facade.beforePublish" // before the grant's publish under tm.mu
	YieldAfterPublishForSim  = "facade.afterPublish"  // after the grant's publish
	YieldBeforeStmtForSim    = "facade.beforeStmt"    // before a facade statement
	YieldAfterStmtForSim     = "facade.afterStmt"     // after a facade statement
	YieldGateProbeForSim     = "facade.gateProbe"     // after the pre-write gate's probe
	YieldConfirmProbeForSim  = "facade.confirmProbe"  // after the confirm path's peer probe
)

// RetireForSim retires tm as the runner does before unregistering it.
func (tm *TopologyManager) RetireForSim() { tm.retire() }

// ChangingSitesForSim reports whether tm is changing the sites now (a
// claimed promotion or update, or an admitted follower mutation); the
// simulator's registry uses it for ManagerWiring.RetiringBusy.
func (tm *TopologyManager) ChangingSitesForSim() bool { return tm.changingSites() }

// OrderedUpdateClaimedForSim reports whether tm's ordered update has
// claimed its mutation window (ManagerWiring.RetiringUpdateInFlight).
func (tm *TopologyManager) OrderedUpdateClaimedForSim() bool { return tm.orderedUpdateClaimed() }

// SetPredecessorUpdateInFlightForSim sets or clears the predecessor guard,
// as the runner does when a retired manager's update goroutine exits.
func (tm *TopologyManager) SetPredecessorUpdateInFlightForSim(v bool) {
	tm.setPredecessorUpdateInFlight(v)
}

// The TryLock probes report whether a mutex is free, leaving it unlocked.
// The simulator calls them at every yield: a yield must hold no mutex.
func (tm *TopologyManager) TryLockManagerForSim() bool { return tryUnlock(&tm.mu) }

func (tm *TopologyManager) TryLockFailoverStateWriteForSim() bool {
	return tryUnlock(&tm.failoverStateWriteMu)
}

func (tm *TopologyManager) TryLockRestoreProtectionForSim() bool {
	return tryUnlock(&tm.restoreProtectionMu)
}

func (tm *TopologyManager) TryLockRestoreFenceForSim() bool { return tryUnlock(&tm.restoreFenceMu) }

type tryLocker interface {
	TryLock() bool
	Unlock()
}

func tryUnlock(m tryLocker) bool {
	if !m.TryLock() {
		return false
	}
	m.Unlock()
	return true
}

// HeldForSim returns the held claim's run and whether it is a hold;
// ok=false when the slot is empty.
func (s *PlannedClaimSlot) HeldForSim() (runID string, hold, ok bool) {
	c, held := s.snapshot()
	if !held {
		return "", false, false
	}
	return c.runID, c.mode == claimModeHold, true
}

// ReleasesForSim returns the releases recorded since the last call and
// forgets them.
func (s *PlannedClaimSlot) ReleasesForSim() []ClaimReleaseNote {
	if s == nil {
		return nil
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	out := s.releases
	s.releases = nil
	return out
}

// SetArmCrossSiteRetryHookForSim installs fn, run with no lock held each
// time tm arms a deferred cross-site decision for retry. Call before Run.
func (tm *TopologyManager) SetArmCrossSiteRetryHookForSim(fn func()) {
	tm.mu.Lock()
	tm.armCrossSiteRetryHook = fn
	tm.mu.Unlock()
}

// SetBeforePromotionClaimForTest installs fn, run between an emergency
// failover's (or primary re-assert's) eligibility checks and its claim.
// Call before Run.
func (tm *TopologyManager) SetBeforePromotionClaimForTest(fn func(target string)) {
	tm.mu.Lock()
	tm.beforePromotionClaim = fn
	tm.mu.Unlock()
}

// SetTickIntervalForTest sets how often the ordered update polls replica
// and pod readiness. Call before the update starts.
func (u *UpdateController) SetTickIntervalForTest(d time.Duration) { u.tickInterval = d }

// UpdaterForTest returns tm's ordered-update controller (nil when none), for
// harnesses that shrink its tick.
func (tm *TopologyManager) UpdaterForTest() *UpdateController { return tm.updater }

// SetRunIDSourceForSim makes e mint run IDs with fn instead of random UUIDs,
// so a simulated trial replays byte for byte. Call before the first Step.
func (e *PlannedFailoverEngine) SetRunIDSourceForSim(fn func() string) { e.runIDSource = fn }

// RestoreFailoverStateForSim is startManager's restore for a simulated
// process: rec becomes the out-of-band recorder, then the observed
// authority, the watermark and the newer failover record are restored from
// fg exactly as restoreFailoverState does. Call before the first Poll.
func (tm *TopologyManager) RestoreFailoverStateForSim(fg *v1alpha1.MysqlFailoverGroup, rec FailoverStateRecorder) {
	tm.SetFailoverStateRecorder(rec)
	tm.restoreFailoverRecordFrom(fg, types.NamespacedName{Namespace: fg.Namespace, Name: fg.Name}, tm.logger)
}

// RestorePlannedFailoverStateForSim is startManager's planned-failover
// restore from an uncached view (§3.7, §6.5): a confirmed run's record and
// the naming of a source the run left fenced. Call after
// RestoreFailoverStateForSim.
func (tm *TopologyManager) RestorePlannedFailoverStateForSim(view *v1alpha1.MysqlFailoverGroup) {
	if view == nil {
		return
	}
	var r *TopologyManagerRunner // the restore reads nothing from the runner
	r.restorePlannedFailoverState(tm, view, types.NamespacedName{Namespace: view.Namespace, Name: view.Name})
}

// PlannedOwnRecordDurableForSim reports whether fg's durable failover
// record is the run pf's own, by §4.4's by-content rule (the DST-3 ruling's
// durable-record half), evaluated at now.
func PlannedOwnRecordDurableForSim(fg *v1alpha1.MysqlFailoverGroup, pf *v1alpha1.PlannedFailoverStatus, now time.Time) bool {
	return plannedOwnRecordDurable(fg, pf, now)
}
