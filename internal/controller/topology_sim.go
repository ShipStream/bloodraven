package controller

import (
	"context"
	"errors"
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
