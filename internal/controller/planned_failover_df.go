package controller

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/log"

	v1alpha1 "github.com/shipstream/bloodraven/api/v1alpha1"
	"github.com/shipstream/bloodraven/internal/clock"
	"github.com/shipstream/bloodraven/internal/dragonfly"
	"github.com/shipstream/bloodraven/internal/metrics"
)

// defaultDragonflySyncWait is the fallback used when neither the spec
// nor the recorded planned-failover settings carry a Dragonfly sync
// budget. Matches the API default in DragonflyPlannedFailoverSpec.
const defaultDragonflySyncWait = 30 * time.Second

// plannedFailoverDragonflyStripActive reports whether the planned-failover
// run has stripped (or is about to strip) the traffic label from the source
// Dragonfly pod and not yet finished the takeover: PromotingDragonfly with
// the takeover still Intended (§8.4).
//
// While true, syncDragonflyPodLabels must NOT re-stamp the traffic label
// on the source pod, otherwise it would re-attach the soon-to-be-demoted
// master to the active Service mid-REPLTAKEOVER and re-introduce the
// dual-master selector window the strip exists to close. A run the
// terminal marker ended is not stripping anything.
func plannedFailoverDragonflyStripActive(fg *v1alpha1.MysqlFailoverGroup) bool {
	pf, marked := plannedFailoverRun(fg)
	if pf == nil || marked || pf.Phase != v1alpha1.PlannedFailoverPhasePromotingDragonfly {
		return false
	}
	return pf.Dragonfly == nil || pf.Dragonfly.Takeover == nil || pf.Dragonfly.Takeover.State == v1alpha1.DragonflyTakeoverIntended
}

// plannedFailoverSourceSite returns the run's source site while the run
// owns sites, or "".
func plannedFailoverSourceSite(fg *v1alpha1.MysqlFailoverGroup) string {
	return plannedFailoverFencedSite(fg)
}

// plannedDragonflyTakeoverSucceeded reports whether the run's Dragonfly
// takeover is the group's current Dragonfly authority; see
// plannedTakeoverMasterSite.
func plannedDragonflyTakeoverSucceeded(fg *v1alpha1.MysqlFailoverGroup) bool {
	return plannedTakeoverMasterSite(fg) != ""
}

// plannedTakeoverMasterSite is the run's target while the run's Dragonfly
// takeover is the latest Dragonfly authority, or "" (§8.4). It is the one
// takeover-success predicate: effectiveDragonflyMasterSite (labels,
// rollouts) and activeDragonflySiteFromObservation (status, replica wiring)
// both use it. The takeover counts when:
//
//   - the run has an ID and a target, and its takeover is recorded
//     Succeeded;
//   - the run is past the takeover and not terminal (PromotingDragonfly,
//     Promoting, Publishing or RollingBack). A terminal marker does not end
//     it: until the run's terminal record lands, the block is the only
//     record that the target took the Dragonfly master (its promotion stamp
//     is best-effort), and nothing has moved Dragonfly since;
//   - no successful Dragonfly promotion to another site was stamped at or
//     after the takeover completed. Once the run releases its claim (its
//     promotion is recorded) an emergency follow-up may legitimately move
//     the master while status still reads Promoting; its stamp supersedes
//     the run's takeover.
func plannedTakeoverMasterSite(fg *v1alpha1.MysqlFailoverGroup) string {
	pf := fg.Status.PlannedFailover
	if pf == nil || pf.RunID == "" || pf.TargetSite == "" || pf.Dragonfly == nil || pf.Dragonfly.Takeover == nil ||
		pf.Dragonfly.Takeover.State != v1alpha1.DragonflyTakeoverSucceeded {
		return ""
	}
	switch pf.Phase {
	case v1alpha1.PlannedFailoverPhasePromotingDragonfly,
		v1alpha1.PlannedFailoverPhasePromoting,
		v1alpha1.PlannedFailoverPhasePublishing,
		v1alpha1.PlannedFailoverPhaseRollingBack:
	default:
		return ""
	}
	tk := pf.Dragonfly.Takeover
	completed := tk.IntentTime.Time
	if tk.CompletionTime != nil {
		completed = tk.CompletionTime.Time
	}
	if df := fg.Status.Dragonfly; df != nil && df.LastPromotionTime != nil && df.LastPromotionTarget != "" &&
		df.LastPromotionTarget != pf.TargetSite && df.ActiveSite == df.LastPromotionTarget &&
		!df.LastPromotionTime.Time.Before(completed.Truncate(time.Second)) {
		return ""
	}
	return pf.TargetSite
}

// effectiveDragonflyOnSyncTimeout returns the configured policy, or the
// default (Proceed) when none is set. Only Fail rolls back.
func effectiveDragonflyOnSyncTimeout(fg *v1alpha1.MysqlFailoverGroup) v1alpha1.DragonflySyncTimeoutPolicy {
	if d := fg.Spec.Dragonfly; d != nil && d.PlannedFailover != nil {
		return normalizeDragonflySyncTimeout(d.PlannedFailover.OnSyncTimeout)
	}
	return v1alpha1.DragonflySyncTimeoutProceed
}

// normalizeDragonflySyncTimeout maps a stored policy to its canonical
// value. The comparison ignores case: releases before v1beta1 wrote the
// lower-case `fail`/`proceed`, and CRD ratcheting keeps such a value on
// an object that is updated without touching the field, so a stored
// `fail` must keep rolling back. Anything else is Proceed.
func normalizeDragonflySyncTimeout(p v1alpha1.DragonflySyncTimeoutPolicy) v1alpha1.DragonflySyncTimeoutPolicy {
	if strings.EqualFold(string(p), string(v1alpha1.DragonflySyncTimeoutFail)) {
		return v1alpha1.DragonflySyncTimeoutFail
	}
	return v1alpha1.DragonflySyncTimeoutProceed
}

// effectiveDragonflyMaxSyncWait returns the configured budget or the
// default. The run snapshots it into settings.dragonfly at acceptance.
func effectiveDragonflyMaxSyncWait(fg *v1alpha1.MysqlFailoverGroup) time.Duration {
	if d := fg.Spec.Dragonfly; d != nil && d.PlannedFailover != nil && d.PlannedFailover.MaxSyncWait != nil && d.PlannedFailover.MaxSyncWait.Duration > 0 {
		return d.PlannedFailover.MaxSyncWait.Duration
	}
	return defaultDragonflySyncWait
}

// errDragonflyGateBusy: the group's Dragonfly gate stayed busy; the
// takeover was not attempted.
var errDragonflyGateBusy = errors.New("planned failover: dragonfly gate busy")

// plannedTakeoverResult is the result of one takeover attempt.
type plannedTakeoverResult struct {
	Succeeded    bool
	TargetOffset *int64
	Why          string // failure detail
	// Sent is what this attempt sent: plannedTakeoverMaybe when a
	// REPLTAKEOVER went out whose outcome could have been a promotion,
	// plannedTakeoverDone when it was answered OK.
	Sent plannedTakeoverAttempt
	// Unresolved: REPLTAKEOVER was sent, its reply was lost, and the
	// target's role could not be observed afterwards. Nothing is concluded;
	// the takeover stays Intended and the next pass observes again.
	Unresolved bool
}

// plannedTakeoverAttempt is what this process knows it sent for a run's
// Dragonfly takeover. It is evidence that the run itself made a target that
// now reports role:master the master. A restart loses it; the durable
// evidence takeoverEvidence reads remains.
type plannedTakeoverAttempt int

const (
	// plannedTakeoverNone: no REPLTAKEOVER whose outcome could have been a
	// promotion (none sent, or the server answered with an error).
	plannedTakeoverNone plannedTakeoverAttempt = iota
	// plannedTakeoverMaybe: a REPLTAKEOVER was sent and its reply lost (a
	// transport error or deadline): the target may have taken over.
	plannedTakeoverMaybe
	// plannedTakeoverDone: a REPLTAKEOVER was answered OK.
	plannedTakeoverDone
)

// pfDragonflyMemo is this process's memory of a run's Dragonfly takeover.
type pfDragonflyMemo struct {
	attempt plannedTakeoverAttempt
	// outcome is the takeover result this process established and has not
	// seen commit. A pass records it before anything else (the loss windows
	// included), and a terminal record built while it is pending carries
	// it, so a completed takeover is never left behind an Intended record.
	outcome *pfTakeoverOutcome
}

// pfTakeoverOutcome is an established takeover result.
type pfTakeoverOutcome struct {
	succeeded    bool
	targetOffset *int64
	why          string
	at           metav1.Time
}

// applyTo records the outcome on next's takeover.
func (o pfTakeoverOutcome) applyTo(next *v1alpha1.PlannedFailoverStatus) {
	d := plannedDragonflyBlock(next)
	t := &v1alpha1.DragonflyTakeoverRecord{State: v1alpha1.DragonflyTakeoverIntended, IntentTime: o.at}
	if d.Takeover != nil {
		t = d.Takeover.DeepCopy()
	}
	at := o.at
	t.CompletionTime = &at
	if o.succeeded {
		t.State = v1alpha1.DragonflyTakeoverSucceeded
		t.Method = v1alpha1.DragonflyTakeoverMethodReplTakeover
		t.TargetOffset = o.targetOffset
		d.Takeover = t
		if d.Sessions == "" {
			d.Sessions = v1alpha1.DragonflySessionsPreserved
		}
		d.Message = fmt.Sprintf("REPLTAKEOVER succeeded on %q (sessions %s)", next.TargetSite, d.Sessions)
		return
	}
	t.State = v1alpha1.DragonflyTakeoverFailed
	d.Takeover = t
	d.Sessions = v1alpha1.DragonflySessionsLost
	d.Reason = v1alpha1.DragonflyPlannedReasonPromotionFailed
	d.Message = "REPLTAKEOVER failed: " + o.why
}

// applyPendingTakeoverOutcome records, on a record built from a block whose
// takeover is still Intended (a terminal record, a rollback intent), the
// takeover outcome this process established but never saw commit, so the
// record and its warnings say what Dragonfly did.
func (e *PlannedFailoverEngine) applyPendingTakeoverOutcome(next *v1alpha1.PlannedFailoverStatus) {
	if next.Dragonfly == nil || next.Dragonfly.Takeover == nil || next.Dragonfly.Takeover.State != v1alpha1.DragonflyTakeoverIntended {
		return
	}
	if o := e.runMemo(next.RunID).dragonfly.outcome; o != nil {
		o.applyTo(next)
	}
}

// plannedDragonflyOps runs the Dragonfly side of a planned failover for
// the engine. Production: reconcilerDragonflyOps. Dragonfly is not modeled
// in DST, where the engine runs without it.
type plannedDragonflyOps interface {
	// CaptureSourceOffset reads the source's master_repl_offset and its
	// process run_id ("" when unreadable).
	CaptureSourceOffset(ctx context.Context, fg *v1alpha1.MysqlFailoverGroup, source string) (offset int64, runID string, err error)
	// TargetSyncReady reports whether the target replica has reached
	// sourceOffset; detail explains a not-ready answer, and runID is the
	// target process's run_id read on the same connection ("" when
	// unreadable).
	TargetSyncReady(ctx context.Context, fg *v1alpha1.MysqlFailoverGroup, target string, sourceOffset int64) (ready bool, detail, runID string, err error)
	// Takeover promotes the target under the group's Dragonfly gate: strip
	// the source traffic label, REPLTAKEOVER, relabel. A target already
	// holding role:master is reported Succeeded without a second
	// REPLTAKEOVER. errDragonflyGateBusy: nothing was attempted. guard is
	// the run's claim check (§3.3); Takeover calls it immediately before
	// the label strip and again before REPLTAKEOVER, and returns its error
	// (after restoring a stripped label) when it fails. In
	// plannedTakeoverFinishOnly mode it strips nothing and sends no
	// REPLTAKEOVER: it only finishes a takeover an earlier attempt already
	// made (the target holds role:master), and otherwise reports failure.
	//
	// A target that reports role:master counts as taken over only with
	// evidence that the run's takeover made it so (takeoverEvidence; prior
	// is what this process already sent for the run): a target that
	// restarted mid-run comes back as an empty standalone master.
	Takeover(ctx context.Context, fg *v1alpha1.MysqlFailoverGroup, pf *v1alpha1.PlannedFailoverStatus, mode plannedTakeoverMode,
		prior plannedTakeoverAttempt, guard func() error) (plannedTakeoverResult, error)
	// ObserveRole reads a site's Dragonfly role (INFO replication) without
	// the gate and without changing anything.
	ObserveRole(ctx context.Context, fg *v1alpha1.MysqlFailoverGroup, site string) (string, error)
}

// plannedTakeoverMode selects what a Takeover call may send.
type plannedTakeoverMode int

const (
	// plannedTakeoverFull strips the source traffic label and sends
	// REPLTAKEOVER unless the target already took over.
	plannedTakeoverFull plannedTakeoverMode = iota
	// plannedTakeoverFinishOnly is past the takeover deadline (§6.2): no new
	// role change is started; a takeover an earlier attempt made (whose
	// result write was lost) is still recognised and finished, so the
	// onSyncTimeout policy never rolls MySQL back beside a Dragonfly target
	// that already holds the master role.
	plannedTakeoverFinishOnly
)

// plannedDragonflyTakeoverSlack is the REPLTAKEOVER allowance beyond
// maxSyncWait (§6.2: maxSyncWait + 12s).
const plannedDragonflyTakeoverSlack = 12 * time.Second

// plannedDragonflyGateAllowance is how long PromotingDragonfly may wait for
// the group's Dragonfly gate across passes (§6.2: about 40s).
const plannedDragonflyGateAllowance = 40 * time.Second

// plannedDragonflyMaxSyncWait is the run's snapshotted Dragonfly sync
// budget.
func plannedDragonflyMaxSyncWait(pf *v1alpha1.PlannedFailoverStatus) time.Duration {
	if d := plannedSettings(pf).Dragonfly; d != nil && d.MaxSyncWait.Duration > 0 {
		return d.MaxSyncWait.Duration
	}
	return defaultDragonflySyncWait
}

// plannedDragonflyTakeoverDeadline bounds PromotingDragonfly from
// takeover.intentTime (§6.2): the gate allowance plus REPLTAKEOVER's
// maxSyncWait + 12s. Past it the takeover is treated as failed and the
// run's onSyncTimeout policy applies.
func plannedDragonflyTakeoverDeadline(pf *v1alpha1.PlannedFailoverStatus) time.Duration {
	return plannedDragonflyGateAllowance + plannedDragonflyMaxSyncWait(pf) + plannedDragonflyTakeoverSlack
}

// plannedDragonflyPassBound is a PromotingDragonfly pass's wall-time bound:
// the gate wait, REPLTAKEOVER (maxSyncWait + 12s) and one ordinary write
// budget for the result write.
func plannedDragonflyPassBound(pf *v1alpha1.PlannedFailoverStatus) time.Duration {
	return defaultDragonflyGateWait + plannedDragonflyMaxSyncWait(pf) + plannedDragonflyTakeoverSlack + plannedPassOrdinary
}

// plannedDragonflyCleanupBudget bounds the detached label restore after a
// failed takeover.
const plannedDragonflyCleanupBudget = 10 * time.Second

// plannedDragonflyGateWaitMessage is status.plannedFailover.dragonfly.message
// while the Dragonfly phase waits for the group's Dragonfly gate.
const plannedDragonflyGateWaitMessage = "waiting for an in-progress Dragonfly promotion to finish before promoting; " +
	"the MySQL source stays fenced meanwhile (at most about 40s)"

// ---------- engine steps ----------

// dragonflyLossCheck applies the MySQL source- and target-loss windows in
// the Dragonfly phases (§6.2). done=true: the run moved on.
func (e *PlannedFailoverEngine) dragonflyLossCheck(ctx context.Context, fg *v1alpha1.MysqlFailoverGroup, pf *v1alpha1.PlannedFailoverStatus,
	ref PlannedRunRef) (time.Duration, error, bool) {
	m := e.runMemo(pf.RunID)
	probe, err := e.topo.ProbeLag(ctx, ref, plannedFenceSet(pf))
	if d, ferr, handled := e.facadeErr(ctx, fg, pf, err); handled {
		return d, ferr, true
	}
	if err != nil {
		probe = LagProbe{SourceReachable: true}
	}
	if e.lossWindow(&m.sourceLossSince, probe.SourceReachable) {
		d, err := e.toRollingBack(ctx, fg, pf, v1alpha1.PlannedFailoverReasonSourceLost)
		return d, err, true
	}
	if e.lossWindow(&m.targetLossSince, probe.TargetReachable) {
		d, err := e.toRollingBack(ctx, fg, pf, v1alpha1.PlannedFailoverReasonTargetLost)
		return d, err, true
	}
	if probe.Divergent {
		d, err := e.toRollingBack(ctx, fg, pf, v1alpha1.PlannedFailoverReasonDivergentTarget)
		return d, err, true
	}
	return 0, nil, false
}

func plannedDragonflyBlock(next *v1alpha1.PlannedFailoverStatus) *v1alpha1.PlannedFailoverDragonflyStatus {
	if next.Dragonfly == nil {
		next.Dragonfly = &v1alpha1.PlannedFailoverDragonflyStatus{}
	} else {
		next.Dragonfly = next.Dragonfly.DeepCopy()
	}
	return next.Dragonfly
}

// skipDragonfly moves past the Dragonfly phases when the engine has no
// Dragonfly operations or the run was accepted without Dragonfly.
func (e *PlannedFailoverEngine) skipDragonfly(ctx context.Context, fg *v1alpha1.MysqlFailoverGroup, pf *v1alpha1.PlannedFailoverStatus,
	ref PlannedRunRef) (time.Duration, error) {
	return e.enterPromoting(ctx, fg, pf, ref, func(next *v1alpha1.PlannedFailoverStatus) {
		d := plannedDragonflyBlock(next)
		d.Message = "skipped: dragonfly not enabled"
		next.Message = fmt.Sprintf("promoting %q", pf.TargetSite)
	})
}

// dragonflySyncStep waits for the target Dragonfly replica to reach the
// source's offset, bounded by settings.dragonfly.maxSyncWait.
func (e *PlannedFailoverEngine) dragonflySyncStep(ctx context.Context, fg *v1alpha1.MysqlFailoverGroup, pf *v1alpha1.PlannedFailoverStatus,
	ref PlannedRunRef) (time.Duration, error) {
	if d, err, done := e.dragonflyLossCheck(ctx, fg, pf, ref); done {
		return d, err
	}
	settings := plannedSettings(pf)
	if e.dragonfly == nil || settings.Dragonfly == nil || !dragonflyEnabled(fg) {
		return e.skipDragonfly(ctx, fg, pf, ref)
	}
	if pf.Dragonfly == nil || pf.Dragonfly.SourceOffsetState == "" {
		offset, runID, err := e.dragonfly.CaptureSourceOffset(ctx, fg, pf.SourceSite)
		out, werr := e.advance(ctx, fg, pf, func(next *v1alpha1.PlannedFailoverStatus) {
			d := plannedDragonflyBlock(next)
			if d.SyncWaitStartTime == nil {
				now := metav1.NewTime(e.clk.Now())
				d.SyncWaitStartTime = &now
			}
			if err != nil {
				log.FromContext(ctx).Info("dragonfly: source offset unavailable; cannot prove sync", "error", err)
				d.SourceOffsetState = v1alpha1.DragonflyOffsetUnavailable
				d.Message = fmt.Sprintf("source dragonfly offset unavailable: %v", err)
				return
			}
			o := offset
			d.SourceOffsetState = v1alpha1.DragonflyOffsetCaptured
			d.SourceOffset = &o
			d.SourceRunID = runID
			d.Message = fmt.Sprintf("source offset %d captured; polling target", offset)
		})
		return e.afterAdvance(ctx, fg, pf, out, werr)
	}
	start := e.clk.Now()
	if pf.Dragonfly.SyncWaitStartTime != nil {
		start = pf.Dragonfly.SyncWaitStartTime.Time
	}
	elapsed := e.clk.Now().Sub(start)
	if pf.Dragonfly.SourceOffsetState == v1alpha1.DragonflyOffsetUnavailable {
		return e.dragonflySyncTimeout(ctx, fg, pf, elapsed, "source offset could not be captured; cannot prove target sync")
	}
	var offset int64
	if pf.Dragonfly.SourceOffset != nil {
		offset = *pf.Dragonfly.SourceOffset
	}
	ready, detail, targetRunID, err := e.dragonfly.TargetSyncReady(ctx, fg, pf.TargetSite, offset)
	if err == nil && ready {
		secs := int64(elapsed.Seconds())
		now := metav1.NewTime(e.clk.Now())
		out, werr := e.advance(ctx, fg, pf, func(next *v1alpha1.PlannedFailoverStatus) {
			next.Phase = v1alpha1.PlannedFailoverPhasePromotingDragonfly
			d := plannedDragonflyBlock(next)
			d.SyncWaitSeconds = &secs
			d.Takeover = &v1alpha1.DragonflyTakeoverRecord{State: v1alpha1.DragonflyTakeoverIntended, IntentTime: now,
				TargetRunID: targetRunID}
			d.Message = fmt.Sprintf("target replica caught up in %s", truncateDur(elapsed))
		})
		return e.afterAdvance(ctx, fg, pf, out, werr)
	}
	if elapsed >= settings.Dragonfly.MaxSyncWait.Duration {
		why := detail
		if err != nil {
			why = err.Error()
		}
		return e.dragonflySyncTimeout(ctx, fg, pf, elapsed, why)
	}
	return dragonflySyncPollInterval, nil
}

// dragonflySyncPollInterval is how often WaitingForDragonflySync polls the
// target replica.
const dragonflySyncPollInterval = 1 * time.Second

// dragonflySyncTimeout applies onSyncTimeout: Proceed goes on to the
// takeover with sessions lost; Fail rolls back.
func (e *PlannedFailoverEngine) dragonflySyncTimeout(ctx context.Context, fg *v1alpha1.MysqlFailoverGroup, pf *v1alpha1.PlannedFailoverStatus,
	elapsed time.Duration, why string) (time.Duration, error) {
	e.sink.Event(fg, corev1.EventTypeWarning, ReasonDragonflySyncTimeout,
		fmt.Sprintf("dragonfly sync timed out (%s): %s", truncateDur(elapsed), why))
	if plannedSettings(pf).Dragonfly.OnSyncTimeout == v1alpha1.DragonflySyncTimeoutFail {
		return e.toRollingBack(ctx, fg, pf, v1alpha1.PlannedFailoverReasonDragonflySyncTimeout)
	}
	secs := int64(elapsed.Seconds())
	now := metav1.NewTime(e.clk.Now())
	out, werr := e.advance(ctx, fg, pf, func(next *v1alpha1.PlannedFailoverStatus) {
		next.Phase = v1alpha1.PlannedFailoverPhasePromotingDragonfly
		d := plannedDragonflyBlock(next)
		d.SyncWaitSeconds = &secs
		d.Sessions = v1alpha1.DragonflySessionsLost
		d.Reason = v1alpha1.DragonflyPlannedReasonSyncTimeout
		d.Takeover = &v1alpha1.DragonflyTakeoverRecord{State: v1alpha1.DragonflyTakeoverIntended, IntentTime: now}
		d.Message = fmt.Sprintf("sync wait timed out (%s): %s; proceeding best-effort", truncateDur(elapsed), why)
	})
	return e.afterAdvance(ctx, fg, pf, out, werr)
}

// dragonflyTakeoverStep runs the recorded takeover intent and records its
// result in its own write; the next pass then writes the MySQL Promoting
// intent. Recording the result before enterPromoting keeps a completed
// takeover from being re-run (and the source re-demoted and deleted) while
// the target's MySQL is unreadable.
func (e *PlannedFailoverEngine) dragonflyTakeoverStep(ctx context.Context, fg *v1alpha1.MysqlFailoverGroup, pf *v1alpha1.PlannedFailoverStatus,
	ref PlannedRunRef) (time.Duration, error) {
	m := e.runMemo(pf.RunID)
	tk := (*v1alpha1.DragonflyTakeoverRecord)(nil)
	if pf.Dragonfly != nil {
		tk = pf.Dragonfly.Takeover
	}
	if tk != nil && tk.State != v1alpha1.DragonflyTakeoverIntended {
		m.dragonfly.outcome = nil
	}
	if tk != nil && tk.State == v1alpha1.DragonflyTakeoverIntended && m.dragonfly.outcome != nil {
		// This process established the takeover's result but its write did
		// not commit. Record it before anything else: a loss-window rollback
		// or a stall marker taken now would leave a completed takeover
		// Intended (and the run would drop its sessions-lost warning and
		// the target's Dragonfly authority).
		return e.recordTakeoverOutcome(ctx, fg, pf, *m.dragonfly.outcome)
	}
	if d, err, done := e.dragonflyLossCheck(ctx, fg, pf, ref); done {
		return d, err
	}
	if e.dragonfly == nil || plannedSettings(pf).Dragonfly == nil || !dragonflyEnabled(fg) {
		return e.skipDragonfly(ctx, fg, pf, ref)
	}
	if tk == nil {
		now := metav1.NewTime(e.clk.Now())
		out, werr := e.advance(ctx, fg, pf, func(next *v1alpha1.PlannedFailoverStatus) {
			plannedDragonflyBlock(next).Takeover = &v1alpha1.DragonflyTakeoverRecord{State: v1alpha1.DragonflyTakeoverIntended, IntentTime: now}
		})
		return e.afterAdvance(ctx, fg, pf, out, werr)
	}
	if tk.State != v1alpha1.DragonflyTakeoverIntended {
		// The takeover's result is durable; only the Promoting intent is missing.
		return e.enterPromoting(ctx, fg, pf, ref, func(next *v1alpha1.PlannedFailoverStatus) {})
	}
	// The takeover changes roles: it runs only under a re-proven claim, with
	// the leader context alive (§3.3). A claim hydrated after a restart
	// re-proves authority first (errReproveRequired), so Dragonfly never
	// moves to the target after MySQL authority moved elsewhere.
	if err := e.topo.CheckRunClaim(ref); err != nil {
		if d, ferr, handled := e.facadeErr(ctx, fg, pf, err); handled {
			return d, ferr
		}
		return plannedRequeueBackoff, nil
	}
	mode := plannedTakeoverFull
	deadline := tk.IntentTime.Add(plannedDragonflyTakeoverDeadline(pf))
	pastDeadline := !e.clk.Now().Before(deadline)
	if pastDeadline {
		// §6.2: the takeover (gate waits included) did not finish within its
		// budget from the intent. No new role change starts with the source
		// fenced; a takeover an earlier attempt already made is finished,
		// anything else fails and the run's onSyncTimeout policy applies.
		mode = plannedTakeoverFinishOnly
	}
	guard := func() error { return e.topo.CheckRunClaim(ref) }
	res, err := e.dragonfly.Takeover(ctx, fg, pf, mode, m.dragonfly.attempt, guard)
	if res.Sent > m.dragonfly.attempt {
		m.dragonfly.attempt = res.Sent
	}
	switch {
	case errors.Is(err, errDragonflyGateBusy) && !pastDeadline:
		return e.noteDragonflyGateWait(ctx, fg, pf)
	case errors.Is(err, errDragonflyGateBusy):
		// Past the deadline the gate is still busy, so nothing can be
		// finished or restored now. A takeover an earlier attempt may have
		// made (this process sent one, or the target reports role:master) is
		// not declared failed while the gate keeps it from being checked: a
		// failed takeover under onSyncTimeout=Fail would roll MySQL back
		// beside a Dragonfly target that already holds the master. That
		// wait is bounded by one more gate allowance.
		if e.clk.Now().Before(deadline.Add(plannedDragonflyGateAllowance)) {
			maybe := m.dragonfly.attempt != plannedTakeoverNone
			if !maybe {
				role, rerr := e.dragonfly.ObserveRole(ctx, fg, pf.TargetSite)
				maybe = rerr == nil && role == "master"
			}
			if maybe {
				return dragonflyGateRetry, nil
			}
		}
		res, err = plannedTakeoverResult{}, nil
	case errors.Is(err, errPlannedBlockMoved):
		return plannedRequeuePoll, nil
	}
	if d, ferr, handled := e.facadeErr(ctx, fg, pf, err); handled {
		return d, ferr
	}
	if err == nil && res.Unresolved {
		log.FromContext(ctx).Info("planned failover: dragonfly takeover outcome unknown; observing the target again",
			"fg", ref.Group.String(), "runId", pf.RunID, "site", pf.TargetSite, "detail", res.Why)
		return plannedRequeuePoll, nil
	}
	if pastDeadline && err == nil && !res.Succeeded && res.Why == "" {
		res.Why = fmt.Sprintf("the takeover did not finish within %s of its intent", plannedDragonflyTakeoverDeadline(pf))
	}
	outcome := pfTakeoverOutcome{succeeded: err == nil && res.Succeeded, targetOffset: res.TargetOffset, why: res.Why,
		at: metav1.NewTime(e.clk.Now())}
	if err != nil {
		outcome.why = err.Error()
	}
	m.dragonfly.outcome = &outcome
	return e.recordTakeoverOutcome(ctx, fg, pf, outcome)
}

// recordTakeoverOutcome writes an established takeover result, staying in
// PromotingDragonfly (the next pass writes the Promoting intent), or, for a
// failed takeover under onSyncTimeout=Fail, the rollback intent.
func (e *PlannedFailoverEngine) recordTakeoverOutcome(ctx context.Context, fg *v1alpha1.MysqlFailoverGroup, pf *v1alpha1.PlannedFailoverStatus,
	o pfTakeoverOutcome) (time.Duration, error) {
	m := e.runMemo(pf.RunID)
	rollBack := !o.succeeded && plannedSettings(pf).Dragonfly != nil &&
		plannedSettings(pf).Dragonfly.OnSyncTimeout == v1alpha1.DragonflySyncTimeoutFail
	out, werr := e.advance(ctx, fg, pf, func(next *v1alpha1.PlannedFailoverStatus) {
		o.applyTo(next)
		switch {
		case o.succeeded:
			next.Message = fmt.Sprintf("Dragonfly promoted; promoting %q", pf.TargetSite)
		case rollBack:
			next.Phase = v1alpha1.PlannedFailoverPhaseRollingBack
			next.Rollback = &v1alpha1.PlannedFailoverRollbackRecord{Reason: v1alpha1.PlannedFailoverReasonDragonflyPromotionFailed, IntentTime: o.at}
			next.Message = fmt.Sprintf("rolling back (%s)", v1alpha1.PlannedFailoverReasonDragonflyPromotionFailed)
		default:
			next.Dragonfly.Message = "REPLTAKEOVER failed; continuing best-effort: " + o.why
			next.Message = fmt.Sprintf("Dragonfly takeover failed; promoting %q best-effort", pf.TargetSite)
		}
	})
	if out == WriteCommitted {
		m.dragonfly.outcome = nil
		if rollBack {
			e.sink.Event(fg, corev1.EventTypeWarning, v1alpha1.EventPlannedFailoverRollbackStarted,
				fmt.Sprintf("planned failover to %q rolling back (%s)", pf.TargetSite, v1alpha1.PlannedFailoverReasonDragonflyPromotionFailed))
		}
	}
	return e.afterAdvance(ctx, fg, pf, out, werr)
}

// noteDragonflyGateWait makes a gate wait visible: writes stay paused on
// the fenced source while the takeover waits, so the reason goes into
// status.plannedFailover.dragonfly.message and a Warning Event, once per
// wait.
func (e *PlannedFailoverEngine) noteDragonflyGateWait(ctx context.Context, fg *v1alpha1.MysqlFailoverGroup,
	pf *v1alpha1.PlannedFailoverStatus) (time.Duration, error) {
	if pf.Dragonfly != nil && pf.Dragonfly.Message == plannedDragonflyGateWaitMessage {
		return dragonflyGateRetry, nil
	}
	out, _ := e.advance(ctx, fg, pf, func(next *v1alpha1.PlannedFailoverStatus) {
		plannedDragonflyBlock(next).Message = plannedDragonflyGateWaitMessage
	})
	if out == WriteCommitted {
		e.sink.Event(fg, corev1.EventTypeWarning, ReasonDragonflyPromotionWaiting,
			fmt.Sprintf("planned failover to %q is waiting for an in-progress Dragonfly promotion to finish; MySQL writes stay paused on %q until it does (at most about 40s)",
				pf.TargetSite, pf.SourceSite))
	}
	return dragonflyGateRetry, nil
}

// ---------- production operations ----------

// reconcilerDragonflyOps implements plannedDragonflyOps with the
// reconciler's Dragonfly dialer, labels and gate.
type reconcilerDragonflyOps struct {
	r *MysqlFailoverGroupReconciler
	// clk is the engine's clock, which stamps the takeover intent; the
	// takeover evidence compares process uptimes against it. nil: real.
	clk clock.Clock
}

func (o reconcilerDragonflyOps) now() time.Time {
	if o.clk == nil {
		return time.Now()
	}
	return o.clk.Now()
}

// dragonflyServerInfoReader is a DragonflyConnection that can read `INFO
// server` (the production client can). Without it a process's incarnation
// is unknown, and a takeover cannot be proven from the target's role.
type dragonflyServerInfoReader interface {
	InfoServer(ctx context.Context) (dragonfly.ServerInfo, error)
}

// dragonflySiteObservation is one read of a site's Dragonfly: its role and
// offsets, and its process identity: run_id ("" unknown) and how long it
// has been running (-1: unknown).
type dragonflySiteObservation struct {
	info   dragonfly.ReplicationInfo
	runID  string
	uptime int64
}

// observeDragonflySite reads site's replication info and process uptime on
// one fresh connection. A missing uptime is not an error: it is -1.
func (o reconcilerDragonflyOps) observeDragonflySite(ctx context.Context, fg *v1alpha1.MysqlFailoverGroup, site string) (dragonflySiteObservation, error) {
	obs := dragonflySiteObservation{uptime: -1}
	conn, err := o.r.dragonflyDial(ctx, fg, site)
	if err != nil {
		return obs, err
	}
	defer func() { _ = conn.Close() }()
	if obs.info, err = conn.InfoReplication(ctx); err != nil {
		return obs, err
	}
	if sr, ok := conn.(dragonflyServerInfoReader); ok {
		if si, serr := sr.InfoServer(ctx); serr == nil {
			obs.runID, obs.uptime = si.RunID, si.UptimeSeconds
		}
	}
	return obs, nil
}

// processStart classifies, from a process's uptime (whole seconds, rounded
// down) seen at now, whether it started after t.
type processStart int

const (
	processStartUnknown processStart = iota // uptime unreadable, or too close to t to tell
	processStartBefore                      // the process was already running at t
	processStartAfter                       // the process started after t (a restart)
)

func processStartedRelativeTo(uptime int64, t, now time.Time) processStart {
	if uptime < 0 {
		return processStartUnknown
	}
	elapsed := now.Sub(t)
	switch {
	case time.Duration(uptime)*time.Second >= elapsed:
		return processStartBefore
	case time.Duration(uptime+1)*time.Second <= elapsed:
		return processStartAfter
	}
	return processStartUnknown
}

var _ plannedDragonflyOps = reconcilerDragonflyOps{}

func (o reconcilerDragonflyOps) CaptureSourceOffset(ctx context.Context, fg *v1alpha1.MysqlFailoverGroup, source string) (int64, string, error) {
	conn, err := o.r.dragonflyDial(ctx, fg, source)
	if err != nil {
		return 0, "", fmt.Errorf("dial source: %w", err)
	}
	defer func() { _ = conn.Close() }()
	info, err := conn.InfoReplication(ctx)
	if err != nil {
		return 0, "", fmt.Errorf("INFO replication: %w", err)
	}
	return info.MasterReplOffset, dragonflyRunID(ctx, conn), nil
}

// dragonflyRunID reads the process run_id on conn, last on it: a failed
// read leaves the connection unusable. "" when unreadable.
func dragonflyRunID(ctx context.Context, conn DragonflyConnection) string {
	sr, ok := conn.(dragonflyServerInfoReader)
	if !ok {
		return ""
	}
	si, err := sr.InfoServer(ctx)
	if err != nil {
		return ""
	}
	return si.RunID
}

func (o reconcilerDragonflyOps) TargetSyncReady(ctx context.Context, fg *v1alpha1.MysqlFailoverGroup, target string, sourceOffset int64) (bool, string, string, error) {
	conn, err := o.r.dragonflyDial(ctx, fg, target)
	if err != nil {
		return false, "", "", fmt.Errorf("target unreachable: %w", err)
	}
	info, err := conn.InfoReplication(ctx)
	if err != nil {
		// The RESP buffer is in an unknown state; do not issue another
		// command on this connection.
		_ = conn.Close()
		return false, "", "", fmt.Errorf("INFO replication failed: %w", err)
	}
	persist, perr := conn.InfoPersistence(ctx)
	runID := ""
	if perr == nil {
		runID = dragonflyRunID(ctx, conn)
	}
	_ = conn.Close()
	if dragonfly.CandidateSyncReady(info, persist, sourceOffset) {
		return true, "", runID, nil
	}
	return false, fmt.Sprintf("target offset %d behind source %d", info.SlaveReplOffset, sourceOffset), runID, nil
}

// Takeover runs the four-step planned Dragonfly promotion under the group's
// Dragonfly gate:
//
//  1. Strip the dragonfly-traffic label from the source pod. The active
//     Service selector requires (role=master AND traffic=enabled), so this
//     sheds the source endpoint regardless of the pending role-label flip.
//  2. Issue REPLTAKEOVER against the target Dragonfly (bounded by the run's
//     maxSyncWait).
//  3. On success, stamp role+traffic labels on the target and demote the
//     source to role=replica with its traffic label restored.
//  4. Best-effort CLIENT KILL TYPE NORMAL on the demoted source, and
//     recreate its pod.
//
// On failure the source's traffic label is restored before returning. A
// REPLTAKEOVER whose reply is lost (not a server error) is resolved by
// observing the target's role afterwards: master is a completed takeover,
// replica a failed one; when the target cannot be read the result is
// Unresolved and the source stays shed (if the target did take over, the
// source process has exited, and putting it back in the active Service
// would route clients to it once it restarts).
func (o reconcilerDragonflyOps) Takeover(ctx context.Context, fg *v1alpha1.MysqlFailoverGroup, pf *v1alpha1.PlannedFailoverStatus,
	mode plannedTakeoverMode, prior plannedTakeoverAttempt, guard func() error) (plannedTakeoverResult, error) {
	if guard == nil {
		guard = func() error { return nil }
	}
	r := o.r
	nn := types.NamespacedName{Namespace: fg.Namespace, Name: fg.Name}
	// Own the group's Dragonfly gate. The Dragonfly manager yields at its
	// next safe point; a promotion that already sent a role-changing
	// command finishes recording it first, so the state read below
	// includes it.
	release, ok := r.acquireDragonflyGate(ctx, nn)
	if !ok {
		return plannedTakeoverResult{}, errDragonflyGateBusy
	}
	defer release()
	fresh, err := r.freshFailoverGroup(ctx, nn)
	if err != nil {
		return plannedTakeoverResult{}, err
	}
	cur, marked := plannedFailoverRun(fresh)
	if cur == nil || marked || cur.RunID != pf.RunID || cur.Phase != v1alpha1.PlannedFailoverPhasePromotingDragonfly {
		return plannedTakeoverResult{}, errPlannedBlockMoved
	}
	fg = fresh
	timeout := plannedDragonflyMaxSyncWait(pf)

	// The takeover moves the Dragonfly master from the run's source to its
	// target. When Dragonfly is served from a third site (a Dragonfly-only
	// promotion moved it independently of MySQL), stripping and demoting
	// the MySQL source would leave the real master in the active Service
	// beside the promoted target: refuse, and leave Dragonfly as it is.
	if master := effectiveDragonflyMasterSite(fg); master != "" && master != pf.SourceSite && master != pf.TargetSite {
		why := fmt.Sprintf("the Dragonfly master is %q, not the run's source %q; not taking over", master, pf.SourceSite)
		metrics.DragonflyPromotionsTotal.WithLabelValues(fg.Name, pf.TargetSite, "failed").Inc()
		if r.Recorder != nil {
			r.Recorder.Eventf(fg, corev1.EventTypeWarning, ReasonDragonflyPromotionFailed, "%s", why)
		}
		return plannedTakeoverResult{Why: why}, nil
	}

	if mode == plannedTakeoverFinishOnly {
		return o.finishEarlierTakeover(ctx, fg, pf, guard)
	}
	if r.Recorder != nil {
		r.Recorder.Eventf(fg, corev1.EventTypeNormal, ReasonDragonflyPromotionStarted,
			"promoting Dragonfly target %q via REPLTAKEOVER", pf.TargetSite)
	}
	// The gate wait and the fresh read above may have taken seconds: the
	// claim is checked again immediately before the first role-changing
	// step.
	if err := guard(); err != nil {
		return plannedTakeoverResult{}, err
	}
	if pf.SourceSite != "" {
		if err := r.setDragonflyTrafficOnSite(ctx, fg, pf.SourceSite, false); err != nil {
			// A failed strip means the source might still be in the active
			// Service when the target is promoted: the dual-master window
			// the strip closes. Treat it as a promotion failure.
			why := fmt.Sprintf("strip source traffic label: %v", err)
			o.fail(ctx, fg, pf, why)
			return plannedTakeoverResult{Why: why}, nil
		}
	}
	conn, err := r.dragonflyDial(ctx, fg, pf.TargetSite)
	if err != nil {
		why := fmt.Sprintf("dial target: %v", err)
		o.fail(ctx, fg, pf, why)
		return plannedTakeoverResult{Why: why}, nil
	}
	// A re-run after a lost record: a target that already holds the master
	// role took over, if the evidence says this run made it master; never
	// send a second REPLTAKEOVER.
	info, infoErr := conn.InfoReplication(ctx)
	if infoErr == nil && info.Role == "master" {
		_ = conn.Close()
		return o.settleMasterTarget(ctx, fg, pf, prior, ""), nil
	}
	if infoErr != nil {
		// The RESP buffer is in an unknown state: take a fresh connection.
		_ = conn.Close()
		if conn, err = r.dragonflyDial(ctx, fg, pf.TargetSite); err != nil {
			why := fmt.Sprintf("dial target: %v", err)
			o.fail(ctx, fg, pf, why)
			return plannedTakeoverResult{Why: why}, nil
		}
	}
	if err := guard(); err != nil {
		_ = conn.Close()
		o.restoreDetached(ctx, fg, pf)
		return plannedTakeoverResult{}, err
	}
	if err := conn.ReplTakeover(ctx, timeout); err != nil {
		_ = conn.Close()
		if dragonfly.IsServerError(err, "") {
			// The target answered: it refused or could not complete the
			// takeover and is still a replica.
			why := fmt.Sprintf("REPLTAKEOVER: %v", err)
			o.fail(ctx, fg, pf, why)
			return plannedTakeoverResult{Why: why}, nil
		}
		return o.resolveLostTakeoverReply(ctx, fg, pf, prior, err), nil
	}
	postInfo, _ := conn.InfoReplication(ctx)
	_ = conn.Close()
	o.finishPromotion(ctx, fg, pf)
	res := plannedTakeoverResult{Succeeded: true, Sent: plannedTakeoverDone}
	offset := postInfo.MasterReplOffset
	if offset == 0 {
		offset = postInfo.SlaveReplOffset
	}
	if offset != 0 {
		res.TargetOffset = &offset
	}
	return res, nil
}

// resolveLostTakeoverReply settles a REPLTAKEOVER whose reply was lost (a
// transport error or a deadline, not a server error): the server may have
// promoted the target. The client read deadline covers the server's whole
// takeover budget, so a fresh observation (on a detached, bounded context:
// the pass's may be spent) settles it. A target still a replica failed; a
// target reporting master is a takeover only with the same evidence any
// role:master target needs (settleMasterTarget): it may instead have
// restarted as an empty standalone master after the reply was lost.
func (o reconcilerDragonflyOps) resolveLostTakeoverReply(ctx context.Context, fg *v1alpha1.MysqlFailoverGroup,
	pf *v1alpha1.PlannedFailoverStatus, prior plannedTakeoverAttempt, sendErr error) plannedTakeoverResult {
	cctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), plannedDragonflyCleanupBudget)
	defer cancel()
	role, err := o.ObserveRole(cctx, fg, pf.TargetSite)
	switch {
	case err != nil:
		return plannedTakeoverResult{Unresolved: true, Sent: plannedTakeoverMaybe,
			Why: fmt.Sprintf("REPLTAKEOVER reply lost (%v) and the target's role is unreadable (%v)", sendErr, err)}
	case role == "master":
		res := o.settleMasterTarget(cctx, fg, pf, max(prior, plannedTakeoverMaybe),
			fmt.Sprintf("REPLTAKEOVER reply lost (%v)", sendErr))
		res.Sent = plannedTakeoverMaybe
		return res
	}
	why := fmt.Sprintf("REPLTAKEOVER: %v; the target is still a replica", sendErr)
	o.fail(ctx, fg, pf, why)
	return plannedTakeoverResult{Why: why}
}

// plannedTakeoverVerdict is what the evidence says about a target that
// reports role:master.
type plannedTakeoverVerdict int

const (
	takeoverUnproven plannedTakeoverVerdict = iota // cannot tell yet
	takeoverProven                                 // the run's takeover made it master
	takeoverRefuted                                // it is master for another reason (an empty restart)
)

// settleMasterTarget acts on a target that reports role:master: finish
// the promotion when the evidence proves the run's takeover made it so;
// fail (restoring the source's traffic, never demoting or deleting the
// source) when the evidence refutes it; and leave it Unresolved, the
// source still shed, when the evidence cannot be read yet. prefix, when
// set, prefixes the reason.
func (o reconcilerDragonflyOps) settleMasterTarget(ctx context.Context, fg *v1alpha1.MysqlFailoverGroup,
	pf *v1alpha1.PlannedFailoverStatus, prior plannedTakeoverAttempt, prefix string) plannedTakeoverResult {
	verdict, why := o.takeoverEvidence(ctx, fg, pf)
	if prefix != "" && why != "" {
		why = prefix + ": " + why
	}
	switch verdict {
	case takeoverProven:
		o.finishPromotion(ctx, fg, pf)
		return plannedTakeoverResult{Succeeded: true}
	case takeoverRefuted:
		o.fail(ctx, fg, pf, why)
		return plannedTakeoverResult{Why: why}
	}
	return plannedTakeoverResult{Unresolved: true, Sent: prior, Why: why}
}

// takeoverEvidence decides whether a target reporting role:master is the
// Dragonfly master because of this run's takeover. A Dragonfly pod restarts
// with no --replicaof and empty data, so a target that restarted mid-run
// (before or after a REPLTAKEOVER, whose reply may also have been lost)
// reports role:master too; promoting it would discard the whole cache the
// source still serves. That this process sent a REPLTAKEOVER is therefore
// never enough on its own: the evidence must come from the target's
// current process.
//
//   - A promotion of the target stamped at or after the takeover intent
//     proves it (finishPromotion stamps it after relabelling and demoting
//     the source: the takeover was already finished once).
//   - The target's run_id, recorded with the intent when it was last seen
//     as a caught-up replica, decides it: the same process now reporting
//     master was promoted during the run (proven); another process
//     restarted (refuted).
//   - Without both run_ids: a target process started after the intent
//     (its uptime) refutes it; otherwise it needs corroboration: the
//     target's master_repl_offset reached the captured source offset, or
//     the source is no longer the master process it was (role:slave, a
//     run_id other than the one recorded with the offset, or a process
//     started after the intent: REPLTAKEOVER exits the source). The same
//     source process still master refutes it.
//
// Anything that cannot be read leaves it unproven.
func (o reconcilerDragonflyOps) takeoverEvidence(ctx context.Context, fg *v1alpha1.MysqlFailoverGroup,
	pf *v1alpha1.PlannedFailoverStatus) (plannedTakeoverVerdict, string) {
	var tk *v1alpha1.DragonflyTakeoverRecord
	if pf.Dragonfly != nil {
		tk = pf.Dragonfly.Takeover
	}
	if tk == nil {
		return takeoverRefuted, fmt.Sprintf("target %q reports role master with no takeover intent recorded", pf.TargetSite)
	}
	intent := tk.IntentTime.Time
	if df := fg.Status.Dragonfly; df != nil && df.LastPromotionTarget == pf.TargetSite && df.ActiveSite == pf.TargetSite &&
		df.LastPromotionTime != nil && !df.LastPromotionTime.Time.Before(intent.Truncate(time.Second)) {
		return takeoverProven, ""
	}
	restarted := fmt.Sprintf("target %q reports role master, but nothing shows this run's REPLTAKEOVER made it master "+
		"(it may have restarted as an empty standalone master); keeping %q as the Dragonfly master", pf.TargetSite, pf.SourceSite)
	now := o.now()
	target, err := o.observeDragonflySite(ctx, fg, pf.TargetSite)
	switch {
	case err != nil:
		return takeoverUnproven, fmt.Sprintf("target %q unreadable while checking the takeover: %v", pf.TargetSite, err)
	case target.info.Role != "master":
		return takeoverRefuted, fmt.Sprintf("target %q is no longer a master (role %q)", pf.TargetSite, target.info.Role)
	case tk.TargetRunID != "" && target.runID != "":
		if target.runID == tk.TargetRunID {
			return takeoverProven, ""
		}
		return takeoverRefuted, restarted
	case processStartedRelativeTo(target.uptime, intent, now) == processStartAfter:
		return takeoverRefuted, restarted
	}
	d := pf.Dragonfly
	if d.SourceOffsetState == v1alpha1.DragonflyOffsetCaptured && d.SourceOffset != nil &&
		*d.SourceOffset > 0 && target.info.MasterReplOffset >= *d.SourceOffset {
		return takeoverProven, ""
	}
	if pf.SourceSite == "" {
		return takeoverRefuted, restarted
	}
	source, err := o.observeDragonflySite(ctx, fg, pf.SourceSite)
	if err != nil {
		return takeoverUnproven, fmt.Sprintf("source %q unreadable while checking the takeover: %v", pf.SourceSite, err)
	}
	if source.info.Role == "slave" {
		return takeoverProven, ""
	}
	if d.SourceRunID != "" && source.runID != "" {
		if source.runID != d.SourceRunID {
			return takeoverProven, ""
		}
		return takeoverRefuted, restarted
	}
	switch processStartedRelativeTo(source.uptime, intent, now) {
	case processStartAfter:
		return takeoverProven, ""
	case processStartBefore:
		return takeoverRefuted, restarted
	}
	return takeoverUnproven, fmt.Sprintf("target %q reports role master; cannot yet tell whether this run's takeover made it so", pf.TargetSite)
}

// ObserveRole reads site's Dragonfly role on its own connection.
func (o reconcilerDragonflyOps) ObserveRole(ctx context.Context, fg *v1alpha1.MysqlFailoverGroup, site string) (string, error) {
	conn, err := o.r.dragonflyDial(ctx, fg, site)
	if err != nil {
		return "", err
	}
	defer func() { _ = conn.Close() }()
	info, err := conn.InfoReplication(ctx)
	if err != nil {
		return "", err
	}
	return info.Role, nil
}

// finishEarlierTakeover is Takeover past the deadline (under the gate): a
// target that already holds the master role took over on an earlier
// attempt, when the evidence says so, and is finished (relabel, demote the
// source); otherwise nothing is sent and the takeover is reported failed,
// with the source's traffic label put back in case an earlier attempt left
// it stripped. The source is never demoted or deleted without evidence.
func (o reconcilerDragonflyOps) finishEarlierTakeover(ctx context.Context, fg *v1alpha1.MysqlFailoverGroup,
	pf *v1alpha1.PlannedFailoverStatus, guard func() error) (plannedTakeoverResult, error) {
	why := fmt.Sprintf("the takeover did not finish within %s of its intent", plannedDragonflyTakeoverDeadline(pf))
	if role, err := o.ObserveRole(ctx, fg, pf.TargetSite); err == nil && role == "master" {
		// Past the deadline an unproven takeover is not waited for again:
		// the run's onSyncTimeout policy applies.
		verdict, ewhy := o.takeoverEvidence(ctx, fg, pf)
		if verdict == takeoverProven {
			if err := guard(); err != nil {
				return plannedTakeoverResult{}, err
			}
			o.finishPromotion(ctx, fg, pf)
			return plannedTakeoverResult{Succeeded: true}, nil
		}
		why = ewhy
	}
	o.fail(ctx, fg, pf, why)
	return plannedTakeoverResult{Why: why}, nil
}

// fail restores the source's traffic label after a failed takeover so the
// active Service keeps an endpoint, and reports the failure. The restore
// runs on a detached, bounded context: a takeover that used up the pass
// (REPLTAKEOVER clamps to it) must still put the label back.
func (o reconcilerDragonflyOps) fail(ctx context.Context, fg *v1alpha1.MysqlFailoverGroup, pf *v1alpha1.PlannedFailoverStatus, why string) {
	o.restoreDetached(ctx, fg, pf)
	metrics.DragonflyPromotionsTotal.WithLabelValues(fg.Name, pf.TargetSite, "failed").Inc()
	if o.r.Recorder != nil {
		o.r.Recorder.Eventf(fg, corev1.EventTypeWarning, ReasonDragonflyPromotionFailed, "%s", why)
	}
}

// restoreDetached restores the source's traffic label on a detached
// context bounded by plannedDragonflyCleanupBudget.
func (o reconcilerDragonflyOps) restoreDetached(ctx context.Context, fg *v1alpha1.MysqlFailoverGroup, pf *v1alpha1.PlannedFailoverStatus) {
	cctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), plannedDragonflyCleanupBudget)
	defer cancel()
	o.r.bestEffortRestoreSourceTraffic(cctx, fg, pf.SourceSite)
}

// finishPromotion relabels the target as master and demotes the source
// (steps 3 and 4).
func (o reconcilerDragonflyOps) finishPromotion(ctx context.Context, fg *v1alpha1.MysqlFailoverGroup, pf *v1alpha1.PlannedFailoverStatus) {
	r := o.r
	if err := r.setDragonflyRoleOnSite(ctx, fg, pf.TargetSite, "master"); err != nil {
		log.FromContext(ctx).Error(err, "post-promotion: stamp target role=master", "site", pf.TargetSite)
	}
	if err := r.setDragonflyTrafficOnSite(ctx, fg, pf.TargetSite, true); err != nil {
		log.FromContext(ctx).Error(err, "post-promotion: stamp target traffic=enabled", "site", pf.TargetSite)
	}
	sourceDemoted := pf.SourceSite == ""
	if pf.SourceSite != "" {
		// The demote MUST succeed before the source's traffic label is
		// restored: a pod still labelled master would rejoin the active
		// Service beside the new master (split brain at the routing layer).
		if err := r.setDragonflyRoleOnSite(ctx, fg, pf.SourceSite, "replica"); err != nil {
			log.FromContext(ctx).Error(err, "post-promotion: stamp source role=replica; leaving source out of active Service to avoid split-brain", "site", pf.SourceSite)
			if r.Recorder != nil {
				r.Recorder.Eventf(fg, corev1.EventTypeWarning, ReasonDragonflyPromotionFailed,
					"post-promotion demote of source %q failed (%v); source kept out of active Service to avoid split-brain. Manual intervention required.",
					pf.SourceSite, err)
			}
		} else {
			sourceDemoted = true
			if err := r.setDragonflyTrafficOnSite(ctx, fg, pf.SourceSite, true); err != nil {
				log.FromContext(ctx).Error(err, "post-promotion: restore source traffic=enabled", "site", pf.SourceSite)
			}
		}
	}
	r.bestEffortClientKillSource(ctx, fg, pf.SourceSite)
	if sourceDemoted {
		r.bestEffortRecreateTakeoverSource(ctx, fg, pf.SourceSite)
	}
	r.stampDragonflyLastPromotion(ctx, fg, pf.TargetSite, metav1.Now())
	metrics.DragonflyPromotionsTotal.WithLabelValues(fg.Name, pf.TargetSite, "success").Inc()
	if r.Recorder != nil {
		r.Recorder.Eventf(fg, corev1.EventTypeNormal, ReasonDragonflyPromotionCompleted,
			"Dragonfly target %q promoted via REPLTAKEOVER", pf.TargetSite)
	}
}

// bestEffortRestoreSourceTraffic restores the source pod's traffic
// label after a promotion failure so the active Service still has an
// endpoint (proceed-mode) or the rollback path lands in a clean state
// (fail-mode). Logs but never propagates errors — this is recovery
// from a partial failure, not a fresh failure path.
func (r *MysqlFailoverGroupReconciler) bestEffortRestoreSourceTraffic(ctx context.Context, fg *v1alpha1.MysqlFailoverGroup, sourceSite string) {
	if sourceSite == "" {
		return
	}
	if err := r.setDragonflyTrafficOnSite(ctx, fg, sourceSite, true); err != nil {
		log.FromContext(ctx).Error(err, "restore source traffic label after promotion failure",
			"site", sourceSite)
	}
}

// bestEffortClientKillSource issues `CLIENT KILL TYPE NORMAL` against
// the old master so application clients reconnect through the active
// Service. Bounded context, all errors logged and dropped.
//
// Log lines include `fg` per the site/content/docs/8.observability/7.log-schema.md Event
// reference contract — downstream pipelines filter on `fg`.
func (r *MysqlFailoverGroupReconciler) bestEffortClientKillSource(ctx context.Context, fg *v1alpha1.MysqlFailoverGroup, sourceSite string) {
	if sourceSite == "" {
		return
	}
	fgKey := fg.Namespace + "/" + fg.Name
	killCtx, cancel := context.WithTimeout(ctx, 3*time.Second)
	defer cancel()
	conn, err := r.dragonflyDial(killCtx, fg, sourceSite)
	if err != nil {
		log.FromContext(ctx).Info("client-kill: source unreachable; skipping", "fg", fgKey, "site", sourceSite, "error", err)
		return
	}
	defer func() { _ = conn.Close() }()
	if err := conn.ClientKillType(killCtx, "NORMAL"); err != nil {
		log.FromContext(ctx).Info("client-kill: source rejected CLIENT KILL", "fg", fgKey, "site", sourceSite, "error", err)
		return
	}
	log.FromContext(ctx).Info("client-kill: evicted clients from old master", "fg", fgKey, "site", sourceSite)
}

// bestEffortRecreateTakeoverSource deletes the demoted source pod after a
// successful REPLTAKEOVER. Dragonfly exits that process by design, and a
// fresh pod avoids waiting through kubelet CrashLoopBackOff before replica
// rejoin.
func (r *MysqlFailoverGroupReconciler) bestEffortRecreateTakeoverSource(ctx context.Context, fg *v1alpha1.MysqlFailoverGroup, sourceSite string) {
	if sourceSite == "" {
		return
	}
	if err := r.deleteDragonflyPodsForSite(ctx, fg, sourceSite); err != nil {
		log.FromContext(ctx).Info("takeover source recreate: delete pod failed", "site", sourceSite, "error", err)
		return
	}
	log.FromContext(ctx).Info("takeover source recreate: deleted demoted source pod", "site", sourceSite)
}

// stampDragonflyLastPromotion is a best-effort patch on
// status.dragonfly.{activeSite,lastPromotionTime,lastPromotionTarget}.
// Wrapped in RetryOnConflict so a concurrent DragonflyManager status patch
// doesn't silently drop the planned-failover promotion stamp.
func (r *MysqlFailoverGroupReconciler) stampDragonflyLastPromotion(ctx context.Context, fg *v1alpha1.MysqlFailoverGroup, target string, when metav1.Time) {
	nn := types.NamespacedName{Namespace: fg.Namespace, Name: fg.Name}
	if err := stampDragonflyPromotionStatus(ctx, r.authorityReader(), r.Client, nn, target, when, true); err != nil {
		log.FromContext(ctx).Error(err, "stampDragonflyLastPromotion: status patch")
	}
}
