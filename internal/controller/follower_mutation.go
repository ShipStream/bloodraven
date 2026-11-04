package controller

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"
)

// recoveryAttemptTimeout bounds each admitted old-primary recovery section:
// the decision (pre-probe, defensive fence, one drain pass, GTID comparison)
// and, separately, the replication rewrite. The status write between them
// runs outside any admitted section, so a slow API server never holds a
// handoff at the barrier.
const recoveryAttemptTimeout = 30 * time.Second

// errHandoffNotAdmitted is returned by PlannedPromote when its context ended
// while it waited for an admitted follower mutation to finish. Nothing has
// been sent to the target, so the planned failover can roll back.
var errHandoffNotAdmitted = errors.New("planned promote: a recovery or source-convergence step on the sites was still running")

// mutationGate is the admission barrier between follower mutations
// (recovery drain, old-primary recovery, source convergence) and topology
// handoffs (planned promotion, ordered-update claim). Mutations enter and
// exit; a handoff blocks new entries, waits, cancellably, until every
// admitted mutation has exited, then publishes the flag that keeps later
// mutations out. The zero value is ready to use.
type mutationGate struct {
	mu      sync.Mutex
	active  int           // admitted mutation sections
	pending int           // handoffs waiting to publish
	idle    chan struct{} // closed when active drops to zero; nil while zero
}

func (g *mutationGate) enter() bool {
	g.mu.Lock()
	defer g.mu.Unlock()
	if g.pending > 0 {
		return false
	}
	if g.active == 0 {
		g.idle = make(chan struct{})
	}
	g.active++
	return true
}

// busy reports whether an admitted section has not exited yet.
func (g *mutationGate) busy() bool {
	g.mu.Lock()
	defer g.mu.Unlock()
	return g.active > 0
}

func (g *mutationGate) exit() {
	g.mu.Lock()
	defer g.mu.Unlock()
	g.active--
	if g.active == 0 {
		close(g.idle)
		g.idle = nil
	}
}

// admit waits for every admitted section to exit, refusing new ones
// meanwhile, then runs publish. It gives up when ctx ends.
func (g *mutationGate) admit(ctx context.Context, publish func()) error {
	g.mu.Lock()
	g.pending++
	idle := g.idle
	g.mu.Unlock()
	defer func() {
		g.mu.Lock()
		g.pending--
		g.mu.Unlock()
	}()
	if idle != nil {
		select {
		case <-idle:
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	publish()
	return nil
}

// followerMutationSuppressedLocked reports whether another actor owns the
// sites or authority is still moving: a planned failover (the Dragonfly
// snapshot upgrade writes the same flag, but the runner's planned-failover
// sync clears it, so an upgrade defers mutations only intermittently), an
// ordered update, a promotion in flight or not yet observed writable, an
// in-place restore, or an auto-bootstrap. Old-primary recovery, the recovery
// connection drain and replication-source convergence all stand down while
// it holds: they kill sessions and rewrite replication on the very sites a
// handoff is using.
//
// predecessorUpdateInFlight covers a replaced manager's ordered update,
// which outlives that manager's Run; beginFollowerMutation also checks
// predecessorMutating, anything else a replaced manager is still doing.
//
// promotedSite is the end-of-handoff barrier: recordFailover sets it, and it
// clears only once a poll observes the target writable (or its TTL lapses),
// so a poll still carrying pre-promotion observations cannot act on them.
// Must hold tm.mu.
func (tm *TopologyManager) followerMutationSuppressedLocked() bool {
	return tm.retired || tm.promotedSite != "" || tm.topologyFrozen || tm.plannedFailoverActive ||
		!bootstrapIdlePhase(tm.bootstrapPhase) ||
		tm.deploymentUpdateRunning || tm.deploymentChanging > 0 || tm.deploymentAwaitingObservation ||
		tm.predecessorUpdateInFlight
}

func (tm *TopologyManager) followerMutationSuppressed() bool {
	tm.mu.RLock()
	defer tm.mu.RUnlock()
	return tm.followerMutationSuppressedLocked()
}

// beginFollowerMutation admits one follower-mutation section (a recovery
// drain pass, an old-primary recovery step, a source repoint). It returns
// ok=false, holding nothing, when a handoff is waiting at the gate or
// mutation is suppressed. Otherwise the caller must call release when the
// section's MySQL statements are done. Every section must be bounded by a
// deadline and must not write CR status: its duration is how long a handoff
// waits.
func (tm *TopologyManager) beginFollowerMutation() (release func(), ok bool) {
	if tm.predecessorMutating() {
		return nil, false
	}
	if !tm.followerMutationGate.enter() {
		return nil, false
	}
	if tm.followerMutationSuppressed() {
		tm.followerMutationGate.exit()
		return nil, false
	}
	return tm.followerMutationGate.exit, true
}

// admitTopologyHandoff runs publish, which must set a flag that
// followerMutationSuppressedLocked observes, once every admitted follower
// mutation has finished. It returns ctx's error, without publishing, if ctx
// ends first. Callers must not hold tm.mu.
func (tm *TopologyManager) admitTopologyHandoff(ctx context.Context, publish func()) error {
	return tm.followerMutationGate.admit(ctx, publish)
}

// pauseRecoveryDrains restarts the budget of every unfinished recovery
// connection drain. A drain deferred behind a handoff must not age out
// meanwhile: the first pass afterwards would otherwise report a timeout
// without evicting anything. Clearing drainStartedAt also makes
// initiateRecovery re-fence (idempotently) before it resumes.
func (tm *TopologyManager) pauseRecoveryDrains() {
	tm.mu.Lock()
	defer tm.mu.Unlock()
	for _, rec := range tm.recovery {
		if rec != nil && !rec.drainComplete {
			rec.drainStartedAt = time.Time{}
		}
	}
}

// noteRecoveryDeferred logs, once per transition and at debug level, that
// old-primary recovery is standing down for a handoff.
func (tm *TopologyManager) noteRecoveryDeferred(deferred bool) {
	tm.mu.Lock()
	changed := tm.recoveryDeferred != deferred
	tm.recoveryDeferred = deferred
	tm.mu.Unlock()
	if !changed {
		return
	}
	if deferred {
		tm.logger.Debug("recovery deferred: topology handoff in flight", "fg", tm.cfg.Name)
	} else {
		tm.logger.Debug("recovery resumed after topology handoff", "fg", tm.cfg.Name)
	}
}

// rewriteOldPrimary runs the replication rewrite that initiateRecovery
// decided on, after RecoveryInProgress was persisted. It is a separate
// admitted section: a handoff claimed while the status write ran (the
// authority epoch moved) or still in flight defers it, and the next poll
// re-decides from fresh observations without waiting out the retry delay.
func (tm *TopologyManager) rewriteOldPrimary(ctx context.Context, oldPrimaryIdx, newPrimaryIdx int, decidedEpoch uint64) {
	name := tm.sites[oldPrimaryIdx].name
	release, admitted := tm.beginFollowerMutation()
	if admitted {
		tm.mu.RLock()
		moved := tm.authorityEpoch != decidedEpoch
		tm.mu.RUnlock()
		if !moved {
			tm.executeRecovery(ctx, oldPrimaryIdx, newPrimaryIdx)
			release()
			return
		}
		release()
	}
	tm.mu.Lock()
	if rec := tm.recovery[name]; rec != nil {
		rec.retryAfter = time.Time{}
	}
	tm.mu.Unlock()
	tm.pauseRecoveryDrains()
	tm.noteRecoveryDeferred(true)
}

// retire stops this manager from starting further ordered updates. The
// runner calls it before unregistering the manager, so an update can only
// be in flight on a retired manager if it began earlier, and rotation
// admission can see it there (see orderedUpdateInFlight).
func (tm *TopologyManager) retire() {
	tm.mu.Lock()
	tm.retired = true
	tm.mu.Unlock()
}

// orderedUpdateClaimed reports whether this manager has an ordered update
// that has claimed its MySQL mutation window. A pending goroutine that has
// not claimed it has touched nothing, and on a retired manager (or during a
// planned failover) it never will.
func (tm *TopologyManager) orderedUpdateClaimed() bool {
	tm.mu.RLock()
	defer tm.mu.RUnlock()
	return tm.deploymentUpdateRunning
}

// mutationClaimed reports whether this manager is already changing the
// sites: an ordered update has claimed its window, or a promotion
// (emergency failover, re-assert) is in flight. Unlike mutationInFlight it
// ignores a pending update that has not claimed, which has touched nothing.
func (tm *TopologyManager) mutationClaimed() bool {
	tm.mu.RLock()
	defer tm.mu.RUnlock()
	return tm.deploymentUpdateRunning || tm.deploymentChanging > 0
}

// Refusals of a planned failover's claim on the sites.
const (
	plannedClaimRefusedUpdateRunning      = "an ordered update is running"
	plannedClaimRefusedPromotion          = "a promotion is in flight"
	plannedClaimRefusedUnobserved         = "a promotion has not been observed yet"
	plannedClaimRefusedPredecessorUpdate  = "a replaced topology manager's ordered update is still finishing"
	plannedClaimRefusedPredecessorPromote = "a replaced topology manager is still inside a promotion"
	plannedClaimRefusedNoManager          = "no topology manager is running for the group yet"
	plannedClaimRefusedRestore            = "an in-place restore is in progress"
	plannedClaimRefusedBootstrap          = "a bootstrap clone is running"
	plannedClaimRefusedRotation           = "the target is mid-keyring-rotation"
	plannedClaimRefusedAuthorityUnsettled = "the topology manager has not established a unique writable primary"
)

// plannedClaimRefusedSourceMoved is the refusal when the manager observes
// a different primary than the planned failover's source.
func plannedClaimRefusedSourceMoved(active, source string) string {
	return fmt.Sprintf("the active primary is now %q, not the source %q", active, source)
}

// ClaimPlannedFailover sets the planned-failover guard for a planned
// failover about to fence source and promote target. Decided under mu
// against every other claim (see the matrix in topology_claims.go), so
// either another actor or the planned failover owns the sites, never
// both. retry marks a refusal that clears on its own within seconds (a
// promotion finishing or being observed); the rest fail the request. A
// manager that observes a primary other than source refuses for good: a
// promotion moved authority since the request was validated. A manager
// that cannot name a unique writable primary (not yet classified after a
// start, or two sites writable) cannot rule out that some other site is the
// real primary, so it refuses for now (retry). Idempotent.
func (tm *TopologyManager) ClaimPlannedFailover(source, target string) (ok bool, why string, retry bool) {
	return tm.claimPlannedFailover(source, target, false)
}

// claimPlannedFailover is ClaimPlannedFailover. sourceFencedAlone records
// that a live probe just found the source read-only and every other site
// read-only: the state this planned failover's own fence leaves behind when
// its status write was lost, in which no site is writable and the manager
// therefore names no primary.
func (tm *TopologyManager) claimPlannedFailover(source, target string, sourceFencedAlone bool) (ok bool, why string, retry bool) {
	tm.mu.Lock()
	defer tm.mu.Unlock()
	switch {
	case tm.retired:
		return false, plannedClaimRefusedNoManager, true
	case tm.deploymentUpdateRunning:
		return false, plannedClaimRefusedUpdateRunning, false
	case tm.predecessorUpdateInFlight:
		return false, plannedClaimRefusedPredecessorUpdate, false
	case tm.topologyFrozen:
		return false, plannedClaimRefusedRestore, false
	case !bootstrapIdlePhase(tm.bootstrapPhase):
		return false, plannedClaimRefusedBootstrap, false
	case tm.keyringRotationBlockedLocked(target):
		return false, plannedClaimRefusedRotation, false
	case tm.deploymentChanging > 0:
		return false, plannedClaimRefusedPromotion, true
	case tm.promotionUnobservedLocked():
		return false, plannedClaimRefusedUnobserved, true
	}
	switch active := tm.activeSiteLocked(); {
	case active == "" && !sourceFencedAlone:
		return false, plannedClaimRefusedAuthorityUnsettled, true
	case active != "" && active != source:
		return false, plannedClaimRefusedSourceMoved(active, source), false
	}
	tm.plannedFailoverActive = true
	return true, "", false
}

// sourceFencedAlone probes source and every other site live and reports
// whether source is read-only and every other site confirmed read-only,
// with what it found otherwise.
func (tm *TopologyManager) sourceFencedAlone(ctx context.Context, source string) (bool, string) {
	site := tm.getSite(source)
	if site == nil {
		return false, fmt.Sprintf("source %q is not a site of this group", source)
	}
	var ro bool
	if err := withMySQLSafetyTimeout(ctx, func(probeCtx context.Context) error {
		var err error
		ro, err = site.mysql.CheckReadOnly(probeCtx)
		return err
	}); err != nil {
		return false, fmt.Sprintf("source %q could not be probed: %v", source, err)
	}
	if !ro {
		return false, fmt.Sprintf("source %q is still writable", source)
	}
	writable, unknown := tm.WritablePeers(ctx, source)
	if len(writable)+len(unknown) > 0 {
		return false, fmt.Sprintf("writable: %v, unreadable: %v", writable, unknown)
	}
	return true, ""
}

// mutationInFlight reports whether this manager has an ordered update
// pending or running, or a promotion claimed. A retiring manager's Run can
// still be inside a promotion until it notices its cancelled context.
func (tm *TopologyManager) mutationInFlight() bool {
	tm.mu.RLock()
	defer tm.mu.RUnlock()
	return tm.deploymentUpdatePending || tm.deploymentUpdateRunning || tm.deploymentChanging > 0
}

// changingSites reports whether this manager is changing the sites now: a
// promotion or ordered update claimed, or a follower mutation admitted.
// On a retired manager it only ever goes from true to false.
func (tm *TopologyManager) changingSites() bool {
	return tm.mutationClaimed() || tm.followerMutationGate.busy()
}

// waitUpdateWorkers blocks until every ordered-update goroutine has exited.
// Only meaningful on a retired manager, which adds no further worker.
func (tm *TopologyManager) waitUpdateWorkers() {
	tm.updateWorkers.Wait()
}

// errPredecessorUpdateInFlight is returned by PlannedPromote while a
// replaced manager's ordered update is still unwinding on the sites.
// Nothing has been sent to the target.
var errPredecessorUpdateInFlight = errors.New("planned promote: a replaced topology manager's ordered update is still finishing on the sites")

// setPredecessorUpdateInFlight sets or clears the predecessor guard and
// logs each transition, since while it holds this manager takes no
// failover, re-assert, recovery or update and status shows no update.
func (tm *TopologyManager) setPredecessorUpdateInFlight(v bool) {
	tm.mu.Lock()
	was := tm.predecessorUpdateInFlight
	tm.predecessorUpdateInFlight = v
	var heldFor time.Duration
	if v && !was {
		tm.predecessorUpdateSince = tm.clock.Now()
	} else if !v && was {
		heldFor = tm.clock.Since(tm.predecessorUpdateSince)
	}
	tm.mu.Unlock()
	switch {
	case v && !was:
		tm.logger.Info("topology manager deferring to its predecessor's ordered update; no failover, recovery or update until it exits",
			"fg", tm.cfg.Name)
	case !v && was:
		tm.logger.Info("predecessor's ordered update exited; topology manager resuming",
			"fg", tm.cfg.Name, "heldFor", heldFor.String())
	}
}

func (tm *TopologyManager) predecessorUpdateUnwinding() bool {
	tm.mu.RLock()
	defer tm.mu.RUnlock()
	return tm.predecessorUpdateInFlight
}
