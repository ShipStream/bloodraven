package controller

import (
	"fmt"

	"github.com/shipstream/bloodraven/internal/state"
)

// A planned write pause is a topology with no writable site that the
// operator made itself and will end itself: the maintenance handoff of an
// ordered update fences the active site before it promotes the standby,
// and a planned failover fences its source from Fencing until its target
// is promoted or the run is rolled back. Both are bounded (the handoff's
// catch-up budget, the run's maxLagWait and phase timeouts).
//
// Reporting that window as NoPrimary pages people for routine maintenance
// (#291), so the poll records whether one of the two operations explains
// what it read, and status, the ALERT log line and the NoPrimaryDetected
// Event follow that record (plannedWritePause). Nothing else changes: the
// site states and status.activeSite are what was read, no cross-site action
// is taken on the verdict either way (applyCrossSiteAction defers while an
// update or a planned claim holds), and the per-site metrics still show no
// writable site, so an alert on them covers a pause that outlasts its
// bound.
//
// The record is in memory only, like the operations it reads. An operator
// that restarts inside the window reports Initializing and then, for a
// handoff, NoPrimary until the re-assert lifts the fence: the handoff died
// with the process. A planned run's claim is hydrated from status, so its
// pause is reported again.

// ownWritePauseSample is one reading of the two operations.
type ownWritePauseSample struct {
	// handoff: the ordered update is in its Failover phase.
	handoff bool
	// handoffs counts the handoffs entered so far.
	handoffs uint64
	// run is the planned failover that holds the claim; runHeld is false
	// for an empty slot and for a hold.
	run     PlannedClaimView
	runHeld bool
	// runs counts the Run-mode claims taken so far.
	runs uint64
}

// sampleOwnWritePause reads the two operations. It takes tm.mu.
func (tm *TopologyManager) sampleOwnWritePause() ownWritePauseSample {
	tm.mu.RLock()
	defer tm.mu.RUnlock()
	return tm.sampleOwnWritePauseLocked()
}

// sampleOwnWritePauseLocked is sampleOwnWritePause for callers that hold
// tm.mu (read or write). The updater and the slot have leaf mutexes.
func (tm *TopologyManager) sampleOwnWritePauseLocked() ownWritePauseSample {
	var s ownWritePauseSample
	if tm.updater != nil {
		s.handoff, s.handoffs = tm.updater.HandoffPhase()
	}
	s.run, s.runHeld, s.runs = tm.plannedSlot.RunClaim()
	return s
}

// ownWritePauseLocked decides, for the poll that is being committed,
// whether one of the operator's own operations explains a topology with no
// writable site, and returns the message for status ("" when none does).
// before was read when the poll started and after when it commits: an
// operation that ran at either reading, or began between them (the
// counters), overlapped the probes. A poll that spans a promotion is
// discarded at its authority-epoch check before this is reached.
//
// The explanation also covers the polls after a handoff's or a planned
// failover's confirmed promotion that could not read the promoted target
// yet (promotionAwaitingPoll): the pause ended with that promotion, and
// the topology manager is waiting for the target's first answer. An
// emergency failover's promotion fenced nothing and is not covered. Must
// hold tm.mu.
func (tm *TopologyManager) ownWritePauseLocked(before, after ownWritePauseSample) string {
	switch {
	case after.runHeld:
		return plannedRunPauseMessage(after.run)
	case before.runHeld:
		return plannedRunPauseMessage(before.run)
	case after.handoff || before.handoff || after.handoffs != before.handoffs:
		return "Writes are paused by the maintenance handoff of an ordered update: the active site is fenced until the standby is promoted, or unfenced if the handoff is abandoned"
	case after.runs != before.runs:
		return "Writes are paused by a planned failover: its source is fenced until the target is promoted or the run is rolled back"
	case tm.promotionAwaitingPoll != "" && tm.promotionEndedWritePause:
		return fmt.Sprintf("Writes were paused for the promotion of %s, which is confirmed; the site has not answered a probe since", tm.promotionAwaitingPoll)
	}
	return ""
}

func plannedRunPauseMessage(run PlannedClaimView) string {
	if run.Source == "" || run.Target == "" {
		return fmt.Sprintf("Writes are paused by planned failover %s: its source is fenced until the target is promoted or the run is rolled back", run.RunID)
	}
	return fmt.Sprintf("Writes are paused by planned failover %s from %s to %s: %s is fenced until %s is promoted or the run is rolled back",
		run.RunID, run.Source, run.Target, run.Source, run.Target)
}

// plannedWritePause returns the write pause that explains action, or ""
// when action is not a NoPrimary verdict or the last poll found none. It
// takes tm.mu.
func (tm *TopologyManager) plannedWritePause(action state.CrossSiteAction) string {
	if action.Reason != state.ReasonNoPrimary {
		return ""
	}
	tm.mu.RLock()
	defer tm.mu.RUnlock()
	return tm.writePause
}
