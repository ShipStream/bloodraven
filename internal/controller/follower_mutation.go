package controller

import "time"

// recoveryAttemptTimeout bounds one old-primary recovery attempt (pre-probe,
// defensive fence, one drain pass, GTID comparison and the replication
// rewrite). The attempt runs as an admitted follower mutation, so this is
// also the longest a planned promotion or an ordered-update claim can wait
// at the admission barrier behind a hung old primary.
const recoveryAttemptTimeout = 30 * time.Second

// followerMutationSuppressedLocked reports whether another actor owns the
// sites or authority is still moving: a planned failover (or Dragonfly
// snapshot upgrade, which drives the same flag), an ordered update, a
// promotion in flight or not yet observed writable, an in-place restore, or
// an auto-bootstrap. Old-primary recovery, the recovery connection drain and
// replication-source convergence all stand down while it holds: they kill
// sessions and rewrite replication on the very sites a handoff is using.
//
// promotedSite is the end-of-handoff barrier: recordFailover sets it, and it
// clears only once a poll observes the target writable (or its TTL lapses),
// so a poll still carrying pre-promotion observations cannot act on them.
// Must hold tm.mu.
func (tm *TopologyManager) followerMutationSuppressedLocked() bool {
	return tm.promotedSite != "" || tm.topologyFrozen || tm.plannedFailoverActive ||
		!bootstrapIdlePhase(tm.bootstrapPhase) ||
		tm.deploymentUpdateRunning || tm.deploymentChanging > 0 || tm.deploymentAwaitingObservation
}

func (tm *TopologyManager) followerMutationSuppressed() bool {
	tm.mu.RLock()
	defer tm.mu.RUnlock()
	return tm.followerMutationSuppressedLocked()
}

// beginFollowerMutation admits one follower-mutation section (a recovery
// drain pass, an old-primary recovery attempt, a source repoint). It returns
// ok=false, holding nothing, when mutation is suppressed. Otherwise the
// caller must call release when the section's MySQL statements are done.
//
// The shared hold pairs with admitTopologyHandoff: a handoff publishes its
// suppression flag while holding followerMutationMu exclusively, so it
// first waits out every admitted section, and every section admitted later
// sees the flag. Sections must not nest; a nested RLock deadlocks against a
// waiting writer.
func (tm *TopologyManager) beginFollowerMutation() (release func(), ok bool) {
	tm.followerMutationMu.RLock()
	if tm.followerMutationSuppressed() {
		tm.followerMutationMu.RUnlock()
		return nil, false
	}
	return tm.followerMutationMu.RUnlock, true
}

// admitTopologyHandoff runs publish, which must set a flag that
// followerMutationSuppressedLocked observes, after every admitted follower
// mutation has finished. Callers must not hold tm.mu.
func (tm *TopologyManager) admitTopologyHandoff(publish func()) {
	tm.followerMutationMu.Lock()
	defer tm.followerMutationMu.Unlock()
	publish()
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
