// Package controller — the planned-failover terminal marker, the
// automation hold and its acknowledgement.
//
// A run that may have written to the target and cannot explain the result
// ends ManualRecoveryRequired. Its terminal status write also records
// status.plannedFailoverHold and the True AutomaticFailoverHeld condition
// that summarises it, and the claim slot switches to hold
// mode: automatic cross-site actions stay held for the group until a human
// resolves the split brain and annotates
// bloodraven.shipstream.io/acknowledge-planned-failover-hold=<runID>. A terminal
// marker with outcome ManualRecoveryRequired (a stalled run) holds the same
// way. The acknowledgement clears the records in an order that a restart
// at any step resumes (§10.5): the terminal record and the removal of
// status.plannedFailoverHold (the condition turns False), then the marker,
// then the slot, then the acknowledgement.
package controller

import (
	"context"
	"fmt"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/log"

	v1alpha1 "github.com/shipstream/bloodraven/api/v1alpha1"
)

// holdPass keeps a standing hold in force: the slot in hold mode, new
// requests refused, and the PlannedFailoverHoldPersisted Warning every 10
// minutes. done=false: the block is a pending run of another run, which
// the caller still drives.
func (e *PlannedFailoverEngine) holdPass(ctx context.Context, fg *v1alpha1.MysqlFailoverGroup, pf *v1alpha1.PlannedFailoverStatus,
	hold *PlannedHold) (time.Duration, error) {
	nn := types.NamespacedName{Namespace: fg.Namespace, Name: fg.Name}
	if hold.RunID != "" {
		if !e.topo.PlannedClaimHeld(nn) {
			ref := PlannedRunRef{Group: nn, RunID: hold.RunID}
			if pf != nil && pf.RunID == hold.RunID {
				ref = plannedRunRef(nn, pf)
			}
			// Hydrates the hold from the uncached records when the slot is
			// empty; a wait or refusal changes nothing here.
			_, _ = e.topo.EnsureClaim(ctx, ref, fg)
		}
		e.topo.ConvertToHold(nn, hold.RunID, hold.Sites...)
	} else {
		// A True AutomaticFailoverHeld condition naming no run holds
		// automation too (fail closed), as startManager's hydration of it
		// does; removing the condition by hand releases it (releaseZombie).
		e.topo.ConvertToHold(nn, "")
	}

	msg := fmt.Sprintf("planned failover refused: automation is held by planned failover %s (%s); resolve it, then annotate %s=%s",
		hold.RunID, hold.Reason, AcknowledgePlannedFailoverHoldAnnotation, hold.RunID)
	if pf != nil && pf.RunID != hold.RunID && plannedFailoverPendingPhase(pf.Phase) && !plannedFailoverTerminal(pf) {
		return e.endNotStarted(ctx, fg, pf, v1alpha1.PlannedFailoverReasonManualRecoveryPending, msg, ReleaseTerminal,
			pf.Phase == v1alpha1.PlannedFailoverPhaseDeferred)
	}
	if raw, ok := fg.GetAnnotations()[PlannedFailoverAnnotation]; ok {
		if pf == nil || (plannedFailoverTerminal(pf) && pf.RunID != hold.RunID) {
			req, perr := parsePlannedFailoverAnnotation(raw)
			if perr == nil && req.Site != "" {
				next := e.newRunBlock(fg, req, v1alpha1.PlannedFailoverPhaseFailed, msg)
				next.Result = &v1alpha1.PlannedFailoverResult{Outcome: v1alpha1.PlannedFailoverOutcomeNotStarted,
					Reason: v1alpha1.PlannedFailoverReasonManualRecoveryPending, CompletionTime: metav1.NewTime(e.clk.Now())}
				out, _, _ := e.commit(ctx, fg, false, freshRunBuild(next))
				if out != WriteCommitted {
					return requeueFor(out), nil
				}
				e.reportTerminal(nn, fg, next, v1alpha1.EventPlannedFailoverRejected)
				e.consumeRequest(ctx, e.reload(ctx, fg), "held")
				return plannedRequeueNext, nil
			}
		}
		// The hold run's own record stays visible; the request is refused
		// with an Event only.
		e.sink.Event(fg, corev1.EventTypeWarning, v1alpha1.EventPlannedFailoverRejected, msg)
		e.sink.RunTerminal(nn, &v1alpha1.PlannedFailoverStatus{Result: &v1alpha1.PlannedFailoverResult{
			Outcome: v1alpha1.PlannedFailoverOutcomeNotStarted, Reason: v1alpha1.PlannedFailoverReasonManualRecoveryPending}})
		e.consumeRequest(ctx, fg, "held")
	}
	if pf != nil && pf.RunID != hold.RunID && !plannedFailoverTerminal(pf) {
		// A run that owns sites beside a hold of another run (not produced by
		// this engine): drive it; the facade refuses its operations while the
		// slot is in hold mode.
		return e.claimPass(ctx, fg, pf)
	}

	g := e.groupMemo(nn)
	now := e.clk.Now()
	if g.holdPendingAt.IsZero() {
		g.holdPendingAt = now
	} else if now.Sub(g.holdPendingAt) >= plannedFailoverHoldPendingInterval {
		g.holdPendingAt = now
		e.sink.Event(fg, corev1.EventTypeWarning, v1alpha1.EventPlannedFailoverHoldPersisted,
			fmt.Sprintf("automatic failover is still held by planned failover %s (%s); resolve the split brain by hand, then annotate %s=%s",
				hold.RunID, hold.Reason, AcknowledgePlannedFailoverHoldAnnotation, hold.RunID))
	}
	return max(time.Second, plannedFailoverHoldPendingInterval-now.Sub(g.holdPendingAt)), nil
}

// plannedFailoverPendingPhase: Admitting or Deferred.
func plannedFailoverPendingPhase(p v1alpha1.PlannedFailoverPhase) bool {
	return p == v1alpha1.PlannedFailoverPhaseAdmitting || p == v1alpha1.PlannedFailoverPhaseDeferred
}

// ackPass runs one step of the acknowledgement order (§10.5). done=false:
// the acknowledgement names another run and is ignored.
func (e *PlannedFailoverEngine) ackPass(ctx context.Context, fg *v1alpha1.MysqlFailoverGroup, hold *PlannedHold, ack string) (time.Duration, bool) {
	nn := types.NamespacedName{Namespace: fg.Namespace, Name: fg.Name}
	if hold == nil {
		// Steps 4-6: both durable hold records are gone. Release the slot,
		// then delete the acknowledgement.
		e.release(ctx, nn, ack, ReleaseAck, "")
		if out, err := e.store.DeleteHoldAck(ctx, fg, ack); out != WriteCommitted {
			log.FromContext(ctx).Info("planned failover: could not delete the hold acknowledgement; retrying",
				"fg", nn.String(), "runId", ack, "error", errString(err))
			return plannedRequeueBackoff, true
		}
		e.groupMemo(nn).holdPendingAt = time.Time{}
		e.sink.Event(fg, corev1.EventTypeNormal, v1alpha1.EventPlannedFailoverHoldAcknowledged,
			fmt.Sprintf("the automation hold of planned failover %s was acknowledged and cleared; automatic failover resumes", ack))
		return plannedRequeueNext, true
	}
	if ack != hold.RunID {
		if g := e.groupMemo(nn); g.ackWarned != ack {
			g.ackWarned = ack
			e.sink.Event(fg, corev1.EventTypeWarning, v1alpha1.EventPlannedFailoverHoldAcknowledgementIgnored,
				fmt.Sprintf("%s=%s ignored: the automation hold is for planned failover %q", AcknowledgePlannedFailoverHoldAnnotation, ack, hold.RunID))
		}
		return 0, false
	}
	m := e.runMemo(hold.RunID)
	if !m.holdAcked {
		m.holdAcked = true
		log.FromContext(ctx).Info("planned failover: hold acknowledged", "fg", nn.String(), "runId", hold.RunID)
	}

	// Step 2: the terminal record (when only the marker carries the hold)
	// and the condition removal, in one conditioned status write.
	pf := fg.Status.PlannedFailover
	var final *v1alpha1.PlannedFailoverStatus
	if pf != nil && pf.RunID == hold.RunID && !plannedFailoverTerminal(pf) {
		final = m.pendingTerminal
		if final == nil || final.RunID != pf.RunID {
			final = e.terminalRecord(pf, plannedEnd{outcome: v1alpha1.PlannedFailoverOutcomeManualRecoveryRequired, reason: hold.Reason,
				warnings: []v1alpha1.PlannedFailoverWarning{v1alpha1.PlannedFailoverWarningStatusStalled},
				msg:      fmt.Sprintf("ended ManualRecoveryRequired (%s) by the terminal marker; the hold was acknowledged", hold.Reason)})
		} else {
			final = final.DeepCopy()
		}
		final.Revision = pf.Revision + 1
	}
	if final != nil || hold.FromStatus {
		out, err := e.store.ClearHold(ctx, fg, hold.RunID, final)
		e.yield(plannedYieldAfterWrite)
		if out != WriteCommitted {
			log.FromContext(ctx).Info("planned failover: could not clear the hold condition; retrying",
				"fg", nn.String(), "runId", hold.RunID, "outcome", out.String(), "error", errString(err))
			return requeueFor(out), true
		}
		if final != nil {
			// The run was reported when the marker ended it.
			m.pendingTerminal = nil
		}
		return plannedRequeueNext, true
	}
	// Step 3: the marker. A failed delete is retried; the slot stays held.
	if hold.FromMarker {
		if out, err := e.store.ClearTerminalMarker(ctx, fg, hold.RunID); out != WriteCommitted {
			log.FromContext(ctx).Info("planned failover: could not delete the hold's terminal marker; retrying",
				"fg", nn.String(), "runId", hold.RunID, "outcome", out.String(), "error", errString(err))
			return requeueFor(out), true
		}
	}
	return plannedRequeueNext, true
}
