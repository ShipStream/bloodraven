package controller

import (
	"context"
	"errors"
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
// which outlives that manager's Run.
//
// promotedSite is the end-of-handoff barrier: recordFailover sets it, and it
// clears only once a poll observes the target writable (or its TTL lapses),
// so a poll still carrying pre-promotion observations cannot act on them.
// Must hold tm.mu.
func (tm *TopologyManager) followerMutationSuppressedLocked() bool {
	return tm.promotedSite != "" || tm.topologyFrozen || tm.plannedFailoverActive ||
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

// orderedUpdateInFlight reports whether an ordered-update goroutine of this
// manager is pending or running.
func (tm *TopologyManager) orderedUpdateInFlight() bool {
	tm.mu.RLock()
	defer tm.mu.RUnlock()
	return tm.deploymentUpdatePending || tm.deploymentUpdateRunning
}

// waitUpdateWorkers blocks until every ordered-update goroutine has exited.
// Only meaningful on a retired manager, which adds no further worker.
func (tm *TopologyManager) waitUpdateWorkers() {
	tm.updateWorkers.Wait()
}

func (tm *TopologyManager) setPredecessorUpdateInFlight(v bool) {
	tm.mu.Lock()
	tm.predecessorUpdateInFlight = v
	tm.mu.Unlock()
}
