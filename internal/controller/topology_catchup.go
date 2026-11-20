package controller

import (
	"context"
	"time"

	"github.com/shipstream/bloodraven/internal/metrics"
	"github.com/shipstream/bloodraven/internal/mysql"
)

// catchUpRefusalBackoff is the delay after the n-th consecutive catch-up
// refusal (n >= 1): failoverRetryBackoff doubled per refusal, capped at
// catchUpRefusalMaxBackoff.
func catchUpRefusalBackoff(n int) time.Duration {
	d := failoverRetryBackoff
	for i := 1; i < n && d < catchUpRefusalMaxBackoff; i++ {
		d *= 2
	}
	return min(d, catchUpRefusalMaxBackoff)
}

// recordCatchUpRefusal handles an emergency failover the catch-up gate
// refused: it logs and counts it, records it for the runner's Warning
// Event, and sets the refusal backoff, which a refusal of the same
// candidate and old primary within catchUpRefusalResetAfter of the previous
// one doubles. The decision is re-run on the next poll:
// deferAfterCatchUpRefusal holds the next fence back until the backoff
// ends, and lets the promotion through as soon as the old primary stops
// answering.
func (tm *TopologyManager) recordCatchUpRefusal(e *CandidateNotCaughtUpError) {
	now := tm.clock.Now()
	tm.mu.Lock()
	if tm.catchUpRefusedCandidate != e.Candidate || tm.catchUpRefusedOldPrimary != e.OldPrimarySite ||
		tm.lastCatchUpRefusal.IsZero() || now.Sub(tm.lastCatchUpRefusal) >= catchUpRefusalResetAfter {
		tm.catchUpRefusals = 0
	}
	tm.catchUpRefusals++
	n := tm.catchUpRefusals
	retryAt := now.Add(catchUpRefusalBackoff(n))
	tm.catchUpRefusedCandidate, tm.catchUpRefusedOldPrimary = e.Candidate, e.OldPrimarySite
	tm.lastCatchUpRefusal, tm.catchUpRetryAt = now, retryAt
	tm.catchUpRefusalSeq++
	tm.catchUpRefusal = &PromotionCatchUpRefusal{
		Seq:                 tm.catchUpRefusalSeq,
		Candidate:           e.Candidate,
		OldPrimary:          e.OldPrimarySite,
		MissingTransactions: e.MissingTransactions,
		Waited:              e.Waited,
		Reason:              e.Reason,
		OldPrimaryOutcome:   e.OldPrimary,
		RetryAt:             retryAt,
	}
	tm.mu.Unlock()

	metrics.FailoverCatchUpRefusalsTotal.WithLabelValues(e.Candidate).Inc()
	tm.logger.Warn("failover refused: candidate has not caught up with the old primary, which answered the fence",
		"candidate", e.Candidate, "oldPrimary", e.OldPrimarySite,
		"missingTransactions", e.MissingTransactions, "missingGtid", e.MissingGtid,
		"waitedMs", e.Waited.Milliseconds(), "oldPrimaryFence", e.OldPrimary,
		"refusals", n, "retryAt", retryAt, "error", e.Err)
	tm.armCrossSiteRetry()
}

// resetCatchUpRefusals ends the refusal backoff after a promotion. The
// last refusal record stays, so its Event is not emitted again.
func (tm *TopologyManager) resetCatchUpRefusals() {
	tm.mu.Lock()
	tm.catchUpRefusals = 0
	tm.catchUpRefusedCandidate, tm.catchUpRefusedOldPrimary = "", ""
	tm.lastCatchUpRefusal, tm.catchUpRetryAt = time.Time{}, time.Time{}
	tm.mu.Unlock()
}

// deferAfterCatchUpRefusal reports whether an emergency failover of
// candidate over oldPrimary must wait for the catch-up refusal backoff: the
// gate refused this pair less than its backoff ago, and the old primary
// still answers. Each attempt fences that old primary and pauses its writes
// for up to the gate's budget, so state transitions (a flapping primary
// produces one every few polls) must not repeat it faster than the
// backoff. An old primary that no longer answers ends the backoff at once:
// its fence would fail, and the promotion would go ahead without a wait.
// A split-brain winner is never gated, so never deferred.
func (tm *TopologyManager) deferAfterCatchUpRefusal(ctx context.Context, candidate, oldPrimary string, oldChecker mysql.Checker, splitBrainWinner bool) bool {
	if splitBrainWinner || oldChecker == nil {
		return false
	}
	tm.mu.RLock()
	retryAt := tm.catchUpRetryAt
	same := tm.catchUpRefusedCandidate == candidate && tm.catchUpRefusedOldPrimary == oldPrimary
	seq := tm.catchUpRefusalSeq
	tm.mu.RUnlock()
	if !same || !tm.clock.Now().Before(retryAt) {
		return false
	}
	if err := withMySQLSafetyTimeout(ctx, func(probeCtx context.Context) error {
		_, err := oldChecker.CheckReadOnly(probeCtx)
		return err
	}); err != nil {
		tm.logger.Info("failover backoff ended: the old primary stopped answering",
			"candidate", candidate, "oldPrimary", oldPrimary, "error", err)
		return false
	}
	// Decided again on the next poll, so an old primary that dies during
	// the backoff is failed over from without waiting for its end. Logged
	// once per refusal.
	tm.mu.Lock()
	first := tm.catchUpDeferLoggedSeq != seq
	tm.catchUpDeferLoggedSeq = seq
	tm.mu.Unlock()
	log := tm.logger.Debug
	if first {
		log = tm.logger.Info
	}
	log("failover deferred: backing off after a catch-up refusal; the old primary still answers",
		"candidate", candidate, "oldPrimary", oldPrimary, "retryAt", retryAt)
	tm.armCrossSiteRetry()
	return true
}
