package controller

import (
	"context"
	"errors"
	"fmt"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/log"

	v1alpha1 "github.com/shipstream/bloodraven/api/v1alpha1"
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
		return dragonflySyncTimeoutPolicy(d.PlannedFailover.OnSyncTimeout)
	}
	return v1alpha1.DragonflySyncTimeoutProceed
}

// dragonflySyncTimeoutPolicy returns p when it is Fail, else Proceed (the
// default; an empty or malformed value never enables the rollback).
func dragonflySyncTimeoutPolicy(p v1alpha1.DragonflySyncTimeoutPolicy) v1alpha1.DragonflySyncTimeoutPolicy {
	if p == v1alpha1.DragonflySyncTimeoutFail {
		return v1alpha1.DragonflySyncTimeoutFail
	}
	return v1alpha1.DragonflySyncTimeoutProceed
}

// effectiveDragonflyMaxSyncWait returns the configured budget or the
// default. The run snapshots it into settings.dragonfly at acceptance.
func effectiveDragonflyMaxSyncWait(fg *v1alpha1.MysqlFailoverGroup) time.Duration {
	if d := fg.Spec.Dragonfly; d != nil && d.PlannedFailover != nil && d.PlannedFailover.MaxSyncWait != nil {
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
}

// pfDragonflyMemo is this process's memory of a run's Dragonfly takeover.
// The run finishes a takeover only on a REPLTAKEOVER success reply this
// process received for the run (§8.4); nothing durable stands in for it.
type pfDragonflyMemo struct {
	// intent is the takeover intent time this process wrote for the run.
	// A takeover recorded Intended whose intent this process did not write
	// is a cross-process re-run (an operator restart or a leader change):
	// an earlier process may have sent REPLTAKEOVER, and nothing here can
	// confirm its outcome.
	intent time.Time
	// outcome is the takeover result this process established and has not
	// seen commit. A pass records it before anything else (the loss windows
	// included), and a terminal record built while it is pending carries
	// it, so a completed takeover is never left behind an Intended record.
	outcome *pfTakeoverOutcome
	// source is the source's replication position this process read for
	// the sync wait, per flow. The run's record holds only its sum
	// (dragonfly.sourceOffset), so a process that takes the run over reads
	// the source again: its wait then ends at a later position, never an
	// earlier one.
	source *dragonfly.Position
	// sourceReread is set once this process has read the source again
	// because the target follows another replication id than source
	// carries (the source's process was replaced during the wait). It is
	// done once per run, so a target that keeps following something else
	// cannot keep the wait re-reading.
	sourceReread bool
}

// errDragonflyTargetFollowsOther: the target is linked and in stable sync,
// but follows another replication id than the source position carries. The
// position was read from an earlier instance of the source.
var errDragonflyTargetFollowsOther = errors.New("target follows another replication id than the source position")

// errDragonflyPositionsIncomparable: the source and the target report their
// positions in different forms (one per flow, the other a single offset
// because it refused the per-flow command). Waiting does not change that.
var errDragonflyPositionsIncomparable = errors.New("source and target report their replication positions in different forms")

// dragonflySyncUnprovable reports whether err, from reading or comparing a
// replication position, means that the sync can never be proven for this
// pair, as opposed to not yet: a site refused the position command
// (dragonfly.IsCommandRefused: no such command, wrong arguments, an ACL
// that does not allow it), or the two positions are in different forms. A
// transport error, a site that is unreachable and a target that is not yet
// in stable sync or still behind are not that.
func dragonflySyncUnprovable(err error) bool {
	return err != nil && (dragonfly.IsCommandRefused(err) || errors.Is(err, errDragonflyPositionsIncomparable))
}

// wroteIntent reports whether this process wrote the takeover intent tk
// (compared at the second, the precision of the stored record).
func (m *pfDragonflyMemo) wroteIntent(tk *v1alpha1.DragonflyTakeoverRecord) bool {
	return !m.intent.IsZero() && m.intent.Truncate(time.Second).Equal(tk.IntentTime.Time.Truncate(time.Second))
}

// newTakeoverIntent is a takeover intent stamped now, remembered as this
// process's own.
func (e *PlannedFailoverEngine) newTakeoverIntent(runID string) *v1alpha1.DragonflyTakeoverRecord {
	now := e.clk.Now()
	e.runMemo(runID).dragonfly.intent = now
	return &v1alpha1.DragonflyTakeoverRecord{State: v1alpha1.DragonflyTakeoverIntended, IntentTime: metav1.NewTime(now)}
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
	// CaptureSourceOffset reads the source's replication position: the
	// journal position of each of its shards. An error means the source
	// gave no position; the caller must not read that as zero.
	CaptureSourceOffset(ctx context.Context, fg *v1alpha1.MysqlFailoverGroup, source string) (dragonfly.Position, error)
	// TargetSyncReady reports whether the target replica is linked to the
	// source and every one of its flows has reached the position source;
	// detail explains a not-ready answer.
	TargetSyncReady(ctx context.Context, fg *v1alpha1.MysqlFailoverGroup, target string, source dragonfly.Position) (ready bool, detail string, err error)
	// Takeover promotes the target under the group's Dragonfly gate: strip
	// the source traffic label, REPLTAKEOVER, relabel. It finishes the
	// promotion (relabel, demote and recreate the source) only on a
	// REPLTAKEOVER success reply to this call. Anything else (a target
	// already reporting role:master, a lost reply, a server error) changes
	// no Dragonfly role: the source's traffic label is put back and the
	// takeover is reported failed. errDragonflyGateBusy: nothing was
	// attempted. guard is the run's claim check (§3.3); Takeover calls it
	// immediately before the label strip and again before REPLTAKEOVER, and
	// returns its error (after restoring a stripped label) when it fails.
	Takeover(ctx context.Context, fg *v1alpha1.MysqlFailoverGroup, pf *v1alpha1.PlannedFailoverStatus, guard func() error) (plannedTakeoverResult, error)
	// Abandon gives up the takeover without sending anything: under the
	// group's Dragonfly gate it puts the source's traffic label back (an
	// earlier attempt may have stripped it) and reports the takeover failed
	// with why. errDragonflyGateBusy: nothing was done.
	Abandon(ctx context.Context, fg *v1alpha1.MysqlFailoverGroup, pf *v1alpha1.PlannedFailoverStatus, why string) (plannedTakeoverResult, error)
}

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
// position the source had when the wait began, bounded by
// settings.dragonfly.maxSyncWait.
//
// The source keeps taking cache writes during the wait (only MySQL is
// fenced), so "caught up" is always relative to a moment: the wait proves
// the target is linked to the source and applied everything the source had
// written when its position was read. What the source writes after that is
// REPLTAKEOVER's to carry over: the source stops taking commands, waits
// for the target to acknowledge every shard's journal, and fails the
// takeover when that does not happen within the budget.
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
		pos, err := e.dragonfly.CaptureSourceOffset(ctx, fg, pf.SourceSite)
		if err == nil {
			e.runMemo(pf.RunID).dragonfly.source = &pos
		}
		offset := pos.Total()
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
	// The record says a position was captured; its per-flow form is this
	// process's. A process that did not capture it (an operator restart or
	// a leader change during the wait) reads the source again.
	m := e.runMemo(pf.RunID)
	ready, detail := false, ""
	var err error
	if m.dragonfly.source == nil {
		var pos dragonfly.Position
		if pos, err = e.dragonfly.CaptureSourceOffset(ctx, fg, pf.SourceSite); err == nil {
			m.dragonfly.source = &pos
		} else {
			err = fmt.Errorf("source position unavailable: %w", err)
		}
	}
	if m.dragonfly.source != nil {
		ready, detail, err = e.dragonfly.TargetSyncReady(ctx, fg, pf.TargetSite, *m.dragonfly.source)
	}
	if errors.Is(err, errDragonflyTargetFollowsOther) && !m.dragonfly.sourceReread {
		// The source's process was replaced after its position was read
		// and the target has synced from the new one: the goal belongs to
		// an instance that is gone. Read the source again, once; the
		// wait's deadline does not move.
		m.dragonfly.sourceReread = true
		pos, perr := e.dragonfly.CaptureSourceOffset(ctx, fg, pf.SourceSite)
		if perr == nil {
			log.FromContext(ctx).Info("dragonfly: source restarted during the sync wait; position read again",
				"site", pf.SourceSite, "target", pf.TargetSite, "offset", pos.Total())
			m.dragonfly.source = &pos
			ready, detail, err = e.dragonfly.TargetSyncReady(ctx, fg, pf.TargetSite, pos)
		} else {
			err = fmt.Errorf("source position unavailable after the source restarted: %w", perr)
		}
	}
	if dragonflySyncUnprovable(err) {
		// A site that will not report a comparable position does not
		// start to by being asked again: holding the MySQL source fenced
		// for the rest of maxSyncWait buys nothing. onSyncTimeout
		// decides now.
		log.FromContext(ctx).Info("dragonfly: replication position cannot be compared; not waiting for maxSyncWait",
			"source", pf.SourceSite, "target", pf.TargetSite, "error", err)
		return e.dragonflySyncTimeout(ctx, fg, pf, elapsed, fmt.Sprintf("the sync cannot be proven, so the wait was ended: %v", err))
	}
	if err == nil && ready {
		secs := int64(elapsed.Seconds())
		intent := e.newTakeoverIntent(pf.RunID)
		out, werr := e.advance(ctx, fg, pf, func(next *v1alpha1.PlannedFailoverStatus) {
			next.Phase = v1alpha1.PlannedFailoverPhasePromotingDragonfly
			d := plannedDragonflyBlock(next)
			d.SyncWaitSeconds = &secs
			d.Takeover = intent.DeepCopy()
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
	e.sink.Event(fg, corev1.EventTypeWarning, v1alpha1.EventDragonflySyncTimedOut,
		fmt.Sprintf("dragonfly sync timed out (%s): %s", truncateDur(elapsed), why))
	if plannedSettings(pf).Dragonfly.OnSyncTimeout == v1alpha1.DragonflySyncTimeoutFail {
		return e.toRollingBack(ctx, fg, pf, v1alpha1.PlannedFailoverReasonDragonflySyncTimeout)
	}
	secs := int64(elapsed.Seconds())
	intent := e.newTakeoverIntent(pf.RunID)
	out, werr := e.advance(ctx, fg, pf, func(next *v1alpha1.PlannedFailoverStatus) {
		next.Phase = v1alpha1.PlannedFailoverPhasePromotingDragonfly
		d := plannedDragonflyBlock(next)
		d.SyncWaitSeconds = &secs
		d.Sessions = v1alpha1.DragonflySessionsLost
		d.Reason = v1alpha1.DragonflyPlannedReasonSyncTimeout
		d.Takeover = intent.DeepCopy()
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
		intent := e.newTakeoverIntent(pf.RunID)
		out, werr := e.advance(ctx, fg, pf, func(next *v1alpha1.PlannedFailoverStatus) {
			plannedDragonflyBlock(next).Takeover = intent.DeepCopy()
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
	deadline := tk.IntentTime.Add(plannedDragonflyTakeoverDeadline(pf))
	pastDeadline := !e.clk.Now().Before(deadline)
	// §8.4: the run finishes a takeover only on a REPLTAKEOVER success reply
	// this process receives. A takeover it cannot confirm changes no
	// Dragonfly role: the run puts the source's traffic label back, records
	// the takeover Failed and applies onSyncTimeout; the Dragonfly manager
	// reconciles Dragonfly once the run releases it.
	abandon := ""
	switch {
	case !m.dragonfly.wroteIntent(tk):
		// An earlier operator process (before a restart or a leader change)
		// wrote the intent and may have sent REPLTAKEOVER; its reply is gone.
		abandon = "unconfirmed: the takeover intent was written by an earlier operator process, whose REPLTAKEOVER outcome " +
			"cannot be confirmed; no Dragonfly role was changed"
	case pastDeadline:
		// §6.2: the takeover (gate waits included) did not finish within its
		// budget from the intent. No role change starts with the source
		// fenced.
		abandon = fmt.Sprintf("the takeover did not finish within %s of its intent", plannedDragonflyTakeoverDeadline(pf))
	}
	var res plannedTakeoverResult
	var err error
	if abandon != "" {
		res, err = e.dragonfly.Abandon(ctx, fg, pf, abandon)
	} else {
		res, err = e.dragonfly.Takeover(ctx, fg, pf, func() error { return e.topo.CheckRunClaim(ref) })
	}
	switch {
	case errors.Is(err, errDragonflyGateBusy) && !pastDeadline:
		return e.noteDragonflyGateWait(ctx, fg, pf)
	case errors.Is(err, errDragonflyGateBusy):
		// Past the deadline the gate is still busy: nothing can be restored
		// now. The takeover is recorded failed; with it no longer Intended,
		// the label sweep puts the source's traffic label back.
		res, err = plannedTakeoverResult{Why: abandon}, nil
	case errors.Is(err, errPlannedBlockMoved):
		return plannedRequeuePoll, nil
	}
	if d, ferr, handled := e.facadeErr(ctx, fg, pf, err); handled {
		return d, ferr
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
		e.sink.Event(fg, corev1.EventTypeWarning, v1alpha1.EventDragonflyPromotionDeferred,
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
}

var _ plannedDragonflyOps = reconcilerDragonflyOps{}

func (o reconcilerDragonflyOps) CaptureSourceOffset(ctx context.Context, fg *v1alpha1.MysqlFailoverGroup, source string) (dragonfly.Position, error) {
	return o.r.dragonflySourcePosition(ctx, fg, source)
}

func (o reconcilerDragonflyOps) TargetSyncReady(ctx context.Context, fg *v1alpha1.MysqlFailoverGroup, target string, source dragonfly.Position) (bool, string, error) {
	return o.r.dragonflyTargetReached(ctx, fg, target, source)
}

// dragonflySourcePosition reads the replication position of source, the
// Dragonfly master a promotion is about to take over from. The site must
// answer as a master: a position read from anything else proves nothing
// about what a replica of the master has to reach.
func (r *MysqlFailoverGroupReconciler) dragonflySourcePosition(ctx context.Context, fg *v1alpha1.MysqlFailoverGroup, source string) (dragonfly.Position, error) {
	conn, err := r.dragonflyDial(ctx, fg, source)
	if err != nil {
		return dragonfly.Position{}, fmt.Errorf("dial source: %w", err)
	}
	defer func() { _ = conn.Close() }()
	pos, err := conn.ReplicationPosition(ctx)
	if err != nil {
		return dragonfly.Position{}, fmt.Errorf("source replication position: %w", err)
	}
	if pos.Role != "master" {
		return dragonfly.Position{}, fmt.Errorf("source reports role %q, not master", pos.Role)
	}
	return pos, nil
}

// dragonflyTargetReached reports whether target, a replica of the source
// whose position is source, can be promoted without losing what the source
// had written at that position: its own INFO answers say it is linked and
// not syncing or loading (dragonfly.CandidateSyncReady), and its
// replication position has reached source on every flow
// (dragonfly.Position.ReachedBy). detail explains a not-ready answer.
//
// The position is the part that can tell a replica that fell behind: one
// cut off from its master without a reset keeps reporting a healthy link.
// A target whose position cannot be read is not ready.
//
// Two errors say more than "not ready": errDragonflyTargetFollowsOther (the
// source position is from an earlier instance of the source) and one for
// which dragonflySyncUnprovable holds (the comparison can never be made).
func (r *MysqlFailoverGroupReconciler) dragonflyTargetReached(ctx context.Context, fg *v1alpha1.MysqlFailoverGroup, target string, source dragonfly.Position) (bool, string, error) {
	look, err := r.dragonflyTargetSync(ctx, fg, target, source)
	return look.Reached, look.Detail, err
}

// dragonflyTargetLook is what one look at a promotion target says.
type dragonflyTargetLook struct {
	// Reached: the target is in stable sync and has reached the source
	// position on every flow.
	Reached bool
	// Linked: the target's own INFO answers say it is in stable sync
	// (dragonfly.CandidateSyncReady). When it is linked and has not
	// reached the position, it is behind, and looking again shortly can
	// help; when it is not linked (link down, full sync, loading), it
	// cannot within seconds.
	Linked bool
	// Detail explains a look that did not reach the position.
	Detail string
}

// dragonflyTargetSync looks at target once; see dragonflyTargetReached.
func (r *MysqlFailoverGroupReconciler) dragonflyTargetSync(ctx context.Context, fg *v1alpha1.MysqlFailoverGroup, target string, source dragonfly.Position) (dragonflyTargetLook, error) {
	conn, err := r.dragonflyDial(ctx, fg, target)
	if err != nil {
		return dragonflyTargetLook{}, fmt.Errorf("target unreachable: %w", err)
	}
	info, err := conn.InfoReplication(ctx)
	if err != nil {
		// The RESP buffer is in an unknown state; do not issue another
		// command on this connection.
		_ = conn.Close()
		return dragonflyTargetLook{}, fmt.Errorf("INFO replication failed: %w", err)
	}
	persist, _ := conn.InfoPersistence(ctx)
	if !dragonfly.CandidateSyncReady(info, persist) {
		_ = conn.Close()
		return dragonflyTargetLook{Detail: fmt.Sprintf("target not in stable sync (role=%s link=%s fullSync=%v loading=%v)",
			info.Role, info.MasterLinkStatus, info.MasterSyncInProgress, persist.Loading)}, nil
	}
	pos, err := conn.ReplicationPosition(ctx)
	_ = conn.Close()
	if err != nil {
		return dragonflyTargetLook{Linked: true}, fmt.Errorf("target replication position: %w", err)
	}
	reached, detail := source.ReachedBy(pos)
	look := dragonflyTargetLook{Reached: reached, Linked: true, Detail: detail}
	switch {
	case reached:
	case pos.Role == "slave" && len(pos.Flows) > 0 && len(source.Flows) > 0 && pos.Scalar != source.Scalar:
		return look, fmt.Errorf("%w: %s", errDragonflyPositionsIncomparable, detail)
	case pos.Role == "slave" && pos.ReplID != "" && source.ReplID != "" && pos.ReplID != source.ReplID:
		return look, fmt.Errorf("%w: %s", errDragonflyTargetFollowsOther, detail)
	}
	return look, nil
}

// underGate runs fn under the group's Dragonfly gate on a fresh, uncached
// read of the group, while the run is still in PromotingDragonfly
// (errPlannedBlockMoved otherwise). The takeover moves the Dragonfly
// master from the run's source to its target: when Dragonfly is served
// from a third site (a Dragonfly-only promotion moved it independently of
// MySQL), stripping or restoring the MySQL source would leave the real
// master in the active Service beside the target, so fn is not run and the
// takeover fails with Dragonfly left as it is.
func (o reconcilerDragonflyOps) underGate(ctx context.Context, fg *v1alpha1.MysqlFailoverGroup, pf *v1alpha1.PlannedFailoverStatus,
	fn func(fg *v1alpha1.MysqlFailoverGroup) (plannedTakeoverResult, error)) (plannedTakeoverResult, error) {
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
	if master := effectiveDragonflyMasterSite(fresh); master != "" && master != pf.SourceSite && master != pf.TargetSite {
		why := fmt.Sprintf("the Dragonfly master is %q, not the run's source %q; not taking over", master, pf.SourceSite)
		metrics.DragonflyPromotionsTotal.WithLabelValues(fresh.Name, pf.TargetSite, "failed").Inc()
		if r.Recorder != nil {
			r.Recorder.Eventf(fresh, corev1.EventTypeWarning, v1alpha1.EventDragonflyPromotionFailed, "%s", why)
		}
		return plannedTakeoverResult{Why: why}, nil
	}
	return fn(fresh)
}

// Abandon: see plannedDragonflyOps.
func (o reconcilerDragonflyOps) Abandon(ctx context.Context, fg *v1alpha1.MysqlFailoverGroup, pf *v1alpha1.PlannedFailoverStatus,
	why string) (plannedTakeoverResult, error) {
	return o.underGate(ctx, fg, pf, func(fg *v1alpha1.MysqlFailoverGroup) (plannedTakeoverResult, error) {
		o.fail(ctx, fg, pf, why)
		return plannedTakeoverResult{Why: why}, nil
	})
}

// Takeover runs the four-step planned Dragonfly promotion under the group's
// Dragonfly gate:
//
//  1. Strip the dragonfly-traffic label from the source pod. The active
//     Service selector requires (role=master AND traffic=true), so this
//     sheds the source endpoint regardless of the pending role-label flip.
//  2. Issue REPLTAKEOVER against the target Dragonfly (bounded by the run's
//     maxSyncWait).
//  3. On a success reply, stamp role+traffic labels on the target and
//     demote the source to role=replica with its traffic label restored.
//  4. Best-effort CLIENT KILL TYPE NORMAL on the demoted source, and
//     recreate its pod.
//
// Steps 3 and 4 run only on REPLTAKEOVER's success reply. On anything else
// the source's traffic label is restored before returning and no other
// label, role or pod is touched (§8.4): a target that already reports
// role:master before REPLTAKEOVER may have restarted as an empty standalone
// master (a Dragonfly pod restarts with no --replicaof and no data), and a
// lost reply (a transport error or a deadline) says nothing this process
// can confirm, so either is an unconfirmed takeover.
func (o reconcilerDragonflyOps) Takeover(ctx context.Context, fg *v1alpha1.MysqlFailoverGroup, pf *v1alpha1.PlannedFailoverStatus,
	guard func() error) (plannedTakeoverResult, error) {
	if guard == nil {
		guard = func() error { return nil }
	}
	return o.underGate(ctx, fg, pf, func(fg *v1alpha1.MysqlFailoverGroup) (plannedTakeoverResult, error) {
		return o.takeover(ctx, fg, pf, guard)
	})
}

func (o reconcilerDragonflyOps) takeover(ctx context.Context, fg *v1alpha1.MysqlFailoverGroup, pf *v1alpha1.PlannedFailoverStatus,
	guard func() error) (plannedTakeoverResult, error) {
	r := o.r
	timeout := plannedDragonflyMaxSyncWait(pf)
	if r.Recorder != nil {
		r.Recorder.Eventf(fg, corev1.EventTypeNormal, v1alpha1.EventDragonflyPromotionStarted,
			"promoting Dragonfly target %q via REPLTAKEOVER", pf.TargetSite)
	}
	// The gate wait and the fresh read may have taken seconds: the claim is
	// checked again immediately before the first role-changing step.
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
	// This process has sent no REPLTAKEOVER for the run (a result it got
	// would have been recorded first), so a target already holding the
	// master role was not promoted by it: never send a second REPLTAKEOVER,
	// and never promote it.
	info, infoErr := conn.InfoReplication(ctx)
	if infoErr == nil && info.Role == "master" {
		_ = conn.Close()
		why := fmt.Sprintf("unconfirmed: target %q already reports role master, but this run did not promote it "+
			"(it may have restarted as an empty standalone master); no Dragonfly role was changed", pf.TargetSite)
		o.fail(ctx, fg, pf, why)
		return plannedTakeoverResult{Why: why}, nil
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
		why := fmt.Sprintf("REPLTAKEOVER: %v", err)
		if dragonfly.TakeoverOutcomeUnknown(err) {
			// The reply was lost (a transport error or a deadline), or
			// the target gave up waiting for the source (`Couldn't
			// execute takeover: Operation canceled`): the takeover may
			// have happened, or the source may still run it and exit
			// (seen on v1.38). The run does not finish it and does not
			// claim that nothing changed: it says what the two sites
			// report now, and the Dragonfly manager reconciles from what
			// they report once the run releases it.
			why = fmt.Sprintf("unconfirmed: REPLTAKEOVER did not report its outcome (%v); the source may still complete it. "+
				"The run changed no label or pod. Observed afterwards: %s", err, o.observedRoles(ctx, fg, pf))
		}
		o.fail(ctx, fg, pf, why)
		return plannedTakeoverResult{Why: why}, nil
	}
	// Best-effort record of where the promoted target's journal stands
	// (takeover.targetOffset): absent when the target gives no position.
	postPos, posErr := conn.ReplicationPosition(ctx)
	_ = conn.Close()
	o.finishPromotion(ctx, fg, pf)
	res := plannedTakeoverResult{Succeeded: true}
	if posErr == nil && postPos.Role == "master" {
		offset := postPos.Total()
		res.TargetOffset = &offset
	}
	return res, nil
}

// observedRoles reads the replication role of the run's target and source
// after a takeover whose outcome is unknown, for the run's record:
// "target=slave, source=master", with "unreachable" for a site that does
// not answer. It decides nothing.
func (o reconcilerDragonflyOps) observedRoles(ctx context.Context, fg *v1alpha1.MysqlFailoverGroup, pf *v1alpha1.PlannedFailoverStatus) string {
	cctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), plannedDragonflyCleanupBudget)
	defer cancel()
	role := func(site string) string {
		if site == "" {
			return "none"
		}
		conn, err := o.r.dragonflyDial(cctx, fg, site)
		if err != nil {
			return "unreachable"
		}
		defer func() { _ = conn.Close() }()
		info, err := conn.InfoReplication(cctx)
		if err != nil {
			return "unreachable"
		}
		if info.Role == "" {
			return "unknown"
		}
		return info.Role
	}
	return fmt.Sprintf("target=%s, source=%s", role(pf.TargetSite), role(pf.SourceSite))
}

// fail restores the source's traffic label after a failed takeover so the
// active Service keeps an endpoint, and reports the failure. The restore
// runs on a detached, bounded context: a takeover that used up the pass
// (REPLTAKEOVER clamps to it) must still put the label back.
func (o reconcilerDragonflyOps) fail(ctx context.Context, fg *v1alpha1.MysqlFailoverGroup, pf *v1alpha1.PlannedFailoverStatus, why string) {
	o.restoreDetached(ctx, fg, pf)
	metrics.DragonflyPromotionsTotal.WithLabelValues(fg.Name, pf.TargetSite, "failed").Inc()
	if o.r.Recorder != nil {
		o.r.Recorder.Eventf(fg, corev1.EventTypeWarning, v1alpha1.EventDragonflyPromotionFailed, "%s", why)
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
		log.FromContext(ctx).Error(err, "post-promotion: stamp target traffic=true", "site", pf.TargetSite)
	}
	sourceDemoted := pf.SourceSite == ""
	if pf.SourceSite != "" {
		// The demote MUST succeed before the source's traffic label is
		// restored: a pod still labelled master would rejoin the active
		// Service beside the new master (split brain at the routing layer).
		if err := r.setDragonflyRoleOnSite(ctx, fg, pf.SourceSite, "replica"); err != nil {
			log.FromContext(ctx).Error(err, "post-promotion: stamp source role=replica; leaving source out of active Service to avoid split-brain", "site", pf.SourceSite)
			if r.Recorder != nil {
				r.Recorder.Eventf(fg, corev1.EventTypeWarning, v1alpha1.EventDragonflyPromotionFailed,
					"post-promotion demote of source %q failed (%v); source kept out of active Service to avoid split-brain. Manual intervention required.",
					pf.SourceSite, err)
			}
		} else {
			sourceDemoted = true
			if err := r.setDragonflyTrafficOnSite(ctx, fg, pf.SourceSite, true); err != nil {
				log.FromContext(ctx).Error(err, "post-promotion: restore source traffic=true", "site", pf.SourceSite)
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
		r.Recorder.Eventf(fg, corev1.EventTypeNormal, v1alpha1.EventDragonflyPromotionSucceeded,
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
