package controller

import (
	"context"
	"errors"
	"sync"
	"time"

	"github.com/shipstream/bloodraven/internal/clock"
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
// handoffs (planned promotion, ordered-update claim, the planned-failover
// claim). Mutations enter and exit; a blocking handoff (admit) blocks new
// entries, waits, cancellably, until every admitted mutation has exited,
// then publishes the flag that keeps later mutations out. A non-blocking
// handoff (tryAdmit) registers a pending entry that expires on its own and
// publishes only when no mutation is admitted. The zero value is ready to
// use; clock defaults to the wall clock.
type mutationGate struct {
	mu      sync.Mutex
	active  int           // admitted mutation sections
	pending int           // blocking handoffs waiting to publish
	idle    chan struct{} // closed when active drops to zero; nil while zero
	// tryPending maps a non-blocking handoff's key (a planned-failover
	// runId) to when its pending entry expires. An unexpired entry
	// refuses new mutations exactly like a blocking handoff.
	tryPending map[string]time.Time
	clock      clock.Clock
}

func (g *mutationGate) now() time.Time {
	if g.clock == nil {
		return time.Now()
	}
	return g.clock.Now()
}

// tryPendingLocked reports whether an unexpired non-blocking handoff is
// registered, dropping expired entries. Must hold g.mu.
func (g *mutationGate) tryPendingLocked() bool {
	now := g.now()
	for key, expires := range g.tryPending {
		if !now.Before(expires) {
			delete(g.tryPending, key)
		}
	}
	return len(g.tryPending) > 0
}

func (g *mutationGate) enter() bool {
	g.mu.Lock()
	defer g.mu.Unlock()
	if g.pending > 0 || g.tryPendingLocked() {
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

// tryAdmit is the planned-failover claim's non-blocking admission. It
// registers (or renews) a pending entry for key that refuses new follower
// mutations until it expires ttl from now on the gate's clock. If an
// admitted mutation is still running it returns errGateBusy at once; the
// caller retries later and renews the entry. Otherwise it runs publish
// with g.mu released (publish takes tm.mu, which is never held together
// with g.mu) and clears the entry. A caller that stops retrying stops
// costing follower mutations anything once its entry lapses.
func (g *mutationGate) tryAdmit(key string, ttl time.Duration, publish func()) error {
	g.mu.Lock()
	if g.tryPending == nil {
		g.tryPending = make(map[string]time.Time)
	}
	g.tryPending[key] = g.now().Add(ttl)
	busy := g.active > 0
	g.mu.Unlock()
	if busy {
		return errGateBusy
	}
	// Registered and idle: enter() refuses from here on, so no mutation
	// can be admitted between the check and publish.
	publish()
	g.mu.Lock()
	delete(g.tryPending, key)
	g.mu.Unlock()
	return nil
}

// followerMutationSuppressedLocked reports whether another actor owns the
// sites or authority is still moving: a planned-failover claim or hold
// (the group's claim slot), an ordered update, a promotion in flight or not yet observed writable, an
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
	return tm.retired || tm.promotedSite != "" || tm.topologyFrozen || tm.plannedClaimHeldLocked() ||
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
