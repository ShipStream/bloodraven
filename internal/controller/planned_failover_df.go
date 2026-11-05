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
// takeover has succeeded and the run is past it (PromotingDragonfly,
// Promoting, Publishing or RollingBack): the target holds the Dragonfly
// master (§8.4).
func plannedDragonflyTakeoverSucceeded(fg *v1alpha1.MysqlFailoverGroup) bool {
	pf, marked := plannedFailoverRun(fg)
	if pf == nil || marked || pf.Target == "" || pf.Dragonfly == nil || pf.Dragonfly.Takeover == nil ||
		pf.Dragonfly.Takeover.State != v1alpha1.DragonflyTakeoverSucceeded {
		return false
	}
	switch pf.Phase {
	case v1alpha1.PlannedFailoverPhasePromotingDragonfly,
		v1alpha1.PlannedFailoverPhasePromoting,
		v1alpha1.PlannedFailoverPhasePublishing,
		v1alpha1.PlannedFailoverPhaseRollingBack:
		return true
	}
	return false
}

// effectiveDragonflyOnSyncTimeout returns the configured policy or the
// default ("proceed") when none is set.
func effectiveDragonflyOnSyncTimeout(fg *v1alpha1.MysqlFailoverGroup) string {
	if d := fg.Spec.Dragonfly; d != nil && d.PlannedFailover != nil && d.PlannedFailover.OnSyncTimeout != "" {
		return d.PlannedFailover.OnSyncTimeout
	}
	return "proceed"
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
}

// plannedDragonflyOps runs the Dragonfly side of a planned failover for
// the engine. Production: reconcilerDragonflyOps. Dragonfly is not modeled
// in DST, where the engine runs without it.
type plannedDragonflyOps interface {
	// CaptureSourceOffset reads the source's master_repl_offset.
	CaptureSourceOffset(ctx context.Context, fg *v1alpha1.MysqlFailoverGroup, source string) (int64, error)
	// TargetSyncReady reports whether the target replica has reached
	// sourceOffset; detail explains a not-ready answer.
	TargetSyncReady(ctx context.Context, fg *v1alpha1.MysqlFailoverGroup, target string, sourceOffset int64) (ready bool, detail string, err error)
	// Takeover promotes the target under the group's Dragonfly gate: strip
	// the source traffic label, REPLTAKEOVER, relabel. A target already
	// holding role:master is reported Succeeded without a second
	// REPLTAKEOVER. errDragonflyGateBusy: nothing was attempted.
	Takeover(ctx context.Context, fg *v1alpha1.MysqlFailoverGroup, pf *v1alpha1.PlannedFailoverStatus) (plannedTakeoverResult, error)
}

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
		next.Message = fmt.Sprintf("promoting %q", pf.Target)
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
		offset, err := e.dragonfly.CaptureSourceOffset(ctx, fg, pf.Source)
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
	var offset int64
	if pf.Dragonfly.SourceOffset != nil {
		offset = *pf.Dragonfly.SourceOffset
	}
	ready, detail, err := e.dragonfly.TargetSyncReady(ctx, fg, pf.Target, offset)
	if err == nil && ready {
		secs := int64(elapsed.Seconds())
		now := metav1.NewTime(e.clk.Now())
		out, werr := e.advance(ctx, fg, pf, func(next *v1alpha1.PlannedFailoverStatus) {
			next.Phase = v1alpha1.PlannedFailoverPhasePromotingDragonfly
			d := plannedDragonflyBlock(next)
			d.SyncWaitSeconds = &secs
			d.Takeover = &v1alpha1.DragonflyTakeoverRecord{State: v1alpha1.DragonflyTakeoverIntended, IntendedAt: now}
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

// dragonflySyncTimeout applies onSyncTimeout: "proceed" goes on to the
// takeover with sessions lost; "fail" rolls back.
func (e *PlannedFailoverEngine) dragonflySyncTimeout(ctx context.Context, fg *v1alpha1.MysqlFailoverGroup, pf *v1alpha1.PlannedFailoverStatus,
	elapsed time.Duration, why string) (time.Duration, error) {
	e.sink.Event(fg, corev1.EventTypeWarning, ReasonDragonflySyncTimeout,
		fmt.Sprintf("dragonfly sync timed out (%s): %s", truncateDur(elapsed), why))
	if plannedSettings(pf).Dragonfly.OnSyncTimeout == "fail" {
		return e.toRollingBack(ctx, fg, pf, v1alpha1.PlannedFailoverReasonDragonflySyncTimeout)
	}
	secs := int64(elapsed.Seconds())
	now := metav1.NewTime(e.clk.Now())
	out, werr := e.advance(ctx, fg, pf, func(next *v1alpha1.PlannedFailoverStatus) {
		next.Phase = v1alpha1.PlannedFailoverPhasePromotingDragonfly
		d := plannedDragonflyBlock(next)
		d.SyncWaitSeconds = &secs
		d.Sessions = v1alpha1.DragonflySessionsLost
		d.Reason = string(v1alpha1.DragonflyPlannedReasonSyncTimeout)
		d.Takeover = &v1alpha1.DragonflyTakeoverRecord{State: v1alpha1.DragonflyTakeoverIntended, IntendedAt: now}
		d.Message = fmt.Sprintf("sync wait timed out (%s): %s; proceeding best-effort", truncateDur(elapsed), why)
	})
	return e.afterAdvance(ctx, fg, pf, out, werr)
}

// dragonflyTakeoverStep runs the recorded takeover intent, then writes the
// MySQL Promoting intent with the takeover's result.
func (e *PlannedFailoverEngine) dragonflyTakeoverStep(ctx context.Context, fg *v1alpha1.MysqlFailoverGroup, pf *v1alpha1.PlannedFailoverStatus,
	ref PlannedRunRef) (time.Duration, error) {
	if d, err, done := e.dragonflyLossCheck(ctx, fg, pf, ref); done {
		return d, err
	}
	if e.dragonfly == nil || plannedSettings(pf).Dragonfly == nil || !dragonflyEnabled(fg) {
		return e.skipDragonfly(ctx, fg, pf, ref)
	}
	tk := (*v1alpha1.DragonflyTakeoverRecord)(nil)
	if pf.Dragonfly != nil {
		tk = pf.Dragonfly.Takeover
	}
	if tk == nil {
		now := metav1.NewTime(e.clk.Now())
		out, werr := e.advance(ctx, fg, pf, func(next *v1alpha1.PlannedFailoverStatus) {
			plannedDragonflyBlock(next).Takeover = &v1alpha1.DragonflyTakeoverRecord{State: v1alpha1.DragonflyTakeoverIntended, IntendedAt: now}
		})
		return e.afterAdvance(ctx, fg, pf, out, werr)
	}
	if tk.State != v1alpha1.DragonflyTakeoverIntended {
		// The takeover's result is durable; only the Promoting intent is missing.
		return e.enterPromoting(ctx, fg, pf, ref, func(next *v1alpha1.PlannedFailoverStatus) {})
	}
	res, err := e.dragonfly.Takeover(ctx, fg, pf)
	if errors.Is(err, errDragonflyGateBusy) {
		return e.noteDragonflyGateWait(ctx, fg, pf)
	}
	if errors.Is(err, errPlannedBlockMoved) {
		return plannedRequeuePoll, nil
	}
	now := metav1.NewTime(e.clk.Now())
	if err == nil && res.Succeeded {
		return e.enterPromoting(ctx, fg, pf, ref, func(next *v1alpha1.PlannedFailoverStatus) {
			d := plannedDragonflyBlock(next)
			t := d.Takeover.DeepCopy()
			t.State = v1alpha1.DragonflyTakeoverSucceeded
			t.CompletedAt = &now
			t.Method = v1alpha1.DragonflyTakeoverMethodReplTakeover
			t.TargetOffset = res.TargetOffset
			d.Takeover = t
			if d.Sessions == "" {
				d.Sessions = v1alpha1.DragonflySessionsPreserved
			}
			d.Message = fmt.Sprintf("REPLTAKEOVER succeeded on %q (sessions %s)", pf.Target, d.Sessions)
			next.Message = fmt.Sprintf("Dragonfly promoted; promoting %q", pf.Target)
		})
	}
	why := res.Why
	if err != nil {
		why = err.Error()
	}
	failed := func(next *v1alpha1.PlannedFailoverStatus) {
		d := plannedDragonflyBlock(next)
		t := d.Takeover.DeepCopy()
		t.State = v1alpha1.DragonflyTakeoverFailed
		t.CompletedAt = &now
		d.Takeover = t
		d.Sessions = v1alpha1.DragonflySessionsLost
		d.Reason = string(v1alpha1.DragonflyPlannedReasonPromotionFailed)
		d.Message = "REPLTAKEOVER failed: " + why
	}
	if plannedSettings(pf).Dragonfly.OnSyncTimeout == "fail" {
		out, werr := e.advance(ctx, fg, pf, func(next *v1alpha1.PlannedFailoverStatus) {
			failed(next)
			next.Phase = v1alpha1.PlannedFailoverPhaseRollingBack
			next.Rollback = &v1alpha1.PlannedFailoverRollbackRecord{Reason: v1alpha1.PlannedFailoverReasonDragonflyPromotionFailed, IntendedAt: now}
			next.Message = fmt.Sprintf("rolling back (%s)", v1alpha1.PlannedFailoverReasonDragonflyPromotionFailed)
		})
		if out == WriteCommitted {
			e.sink.Event(fg, corev1.EventTypeWarning, "PlannedFailoverRollingBack",
				fmt.Sprintf("planned failover to %q rolling back (%s)", pf.Target, v1alpha1.PlannedFailoverReasonDragonflyPromotionFailed))
		}
		return e.afterAdvance(ctx, fg, pf, out, werr)
	}
	return e.enterPromoting(ctx, fg, pf, ref, func(next *v1alpha1.PlannedFailoverStatus) {
		failed(next)
		next.Dragonfly.Message = "REPLTAKEOVER failed; continuing best-effort: " + why
	})
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
				pf.Target, pf.Source))
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

func (o reconcilerDragonflyOps) CaptureSourceOffset(ctx context.Context, fg *v1alpha1.MysqlFailoverGroup, source string) (int64, error) {
	conn, err := o.r.dragonflyDial(ctx, fg, source)
	if err != nil {
		return 0, fmt.Errorf("dial source: %w", err)
	}
	defer func() { _ = conn.Close() }()
	info, err := conn.InfoReplication(ctx)
	if err != nil {
		return 0, fmt.Errorf("INFO replication: %w", err)
	}
	return info.MasterReplOffset, nil
}

func (o reconcilerDragonflyOps) TargetSyncReady(ctx context.Context, fg *v1alpha1.MysqlFailoverGroup, target string, sourceOffset int64) (bool, string, error) {
	conn, err := o.r.dragonflyDial(ctx, fg, target)
	if err != nil {
		return false, "", fmt.Errorf("target unreachable: %w", err)
	}
	info, err := conn.InfoReplication(ctx)
	if err != nil {
		// The RESP buffer is in an unknown state; do not issue another
		// command on this connection.
		_ = conn.Close()
		return false, "", fmt.Errorf("INFO replication failed: %w", err)
	}
	persist, _ := conn.InfoPersistence(ctx)
	_ = conn.Close()
	if dragonfly.CandidateSyncReady(info, persist, sourceOffset) {
		return true, "", nil
	}
	return false, fmt.Sprintf("target offset %d behind source %d", info.SlaveReplOffset, sourceOffset), nil
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
// On failure the source's traffic label is restored before returning.
func (o reconcilerDragonflyOps) Takeover(ctx context.Context, fg *v1alpha1.MysqlFailoverGroup, pf *v1alpha1.PlannedFailoverStatus) (plannedTakeoverResult, error) {
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
	timeout := effectiveDragonflyMaxSyncWait(fg)
	if d := plannedSettings(pf).Dragonfly; d != nil {
		timeout = d.MaxSyncWait.Duration
	}

	if r.Recorder != nil {
		r.Recorder.Eventf(fg, corev1.EventTypeNormal, ReasonDragonflyPromotionStarted,
			"promoting Dragonfly target %q via REPLTAKEOVER", pf.Target)
	}
	if pf.Source != "" {
		if err := r.setDragonflyTrafficOnSite(ctx, fg, pf.Source, false); err != nil {
			// A failed strip means the source might still be in the active
			// Service when the target is promoted: the dual-master window
			// the strip closes. Treat it as a promotion failure.
			why := fmt.Sprintf("strip source traffic label: %v", err)
			o.fail(ctx, fg, pf, why)
			return plannedTakeoverResult{Why: why}, nil
		}
	}
	conn, err := r.dragonflyDial(ctx, fg, pf.Target)
	if err != nil {
		why := fmt.Sprintf("dial target: %v", err)
		o.fail(ctx, fg, pf, why)
		return plannedTakeoverResult{Why: why}, nil
	}
	// A re-run after a lost record: a target that already holds the master
	// role took over; never send a second REPLTAKEOVER.
	info, infoErr := conn.InfoReplication(ctx)
	if infoErr == nil && info.Role == "master" {
		_ = conn.Close()
		o.finishPromotion(ctx, fg, pf)
		return plannedTakeoverResult{Succeeded: true}, nil
	}
	if infoErr != nil {
		// The RESP buffer is in an unknown state: take a fresh connection.
		_ = conn.Close()
		if conn, err = r.dragonflyDial(ctx, fg, pf.Target); err != nil {
			why := fmt.Sprintf("dial target: %v", err)
			o.fail(ctx, fg, pf, why)
			return plannedTakeoverResult{Why: why}, nil
		}
	}
	if err := conn.ReplTakeover(ctx, timeout); err != nil {
		_ = conn.Close()
		why := fmt.Sprintf("REPLTAKEOVER: %v", err)
		o.fail(ctx, fg, pf, why)
		return plannedTakeoverResult{Why: why}, nil
	}
	postInfo, _ := conn.InfoReplication(ctx)
	_ = conn.Close()
	o.finishPromotion(ctx, fg, pf)
	res := plannedTakeoverResult{Succeeded: true}
	offset := postInfo.MasterReplOffset
	if offset == 0 {
		offset = postInfo.SlaveReplOffset
	}
	if offset != 0 {
		res.TargetOffset = &offset
	}
	return res, nil
}

// fail restores the source's traffic label after a failed takeover so the
// active Service keeps an endpoint, and reports the failure.
func (o reconcilerDragonflyOps) fail(ctx context.Context, fg *v1alpha1.MysqlFailoverGroup, pf *v1alpha1.PlannedFailoverStatus, why string) {
	o.r.bestEffortRestoreSourceTraffic(ctx, fg, pf.Source)
	metrics.DragonflyPromotionsTotal.WithLabelValues(fg.Name, pf.Target, "failed").Inc()
	if o.r.Recorder != nil {
		o.r.Recorder.Eventf(fg, corev1.EventTypeWarning, ReasonDragonflyPromotionFailed, "%s", why)
	}
}

// finishPromotion relabels the target as master and demotes the source
// (steps 3 and 4).
func (o reconcilerDragonflyOps) finishPromotion(ctx context.Context, fg *v1alpha1.MysqlFailoverGroup, pf *v1alpha1.PlannedFailoverStatus) {
	r := o.r
	if err := r.setDragonflyRoleOnSite(ctx, fg, pf.Target, "master"); err != nil {
		log.FromContext(ctx).Error(err, "post-promotion: stamp target role=master", "site", pf.Target)
	}
	if err := r.setDragonflyTrafficOnSite(ctx, fg, pf.Target, true); err != nil {
		log.FromContext(ctx).Error(err, "post-promotion: stamp target traffic=enabled", "site", pf.Target)
	}
	sourceDemoted := pf.Source == ""
	if pf.Source != "" {
		// The demote MUST succeed before the source's traffic label is
		// restored: a pod still labelled master would rejoin the active
		// Service beside the new master (split brain at the routing layer).
		if err := r.setDragonflyRoleOnSite(ctx, fg, pf.Source, "replica"); err != nil {
			log.FromContext(ctx).Error(err, "post-promotion: stamp source role=replica; leaving source out of active Service to avoid split-brain", "site", pf.Source)
			if r.Recorder != nil {
				r.Recorder.Eventf(fg, corev1.EventTypeWarning, ReasonDragonflyPromotionFailed,
					"post-promotion demote of source %q failed (%v); source kept out of active Service to avoid split-brain. Manual intervention required.",
					pf.Source, err)
			}
		} else {
			sourceDemoted = true
			if err := r.setDragonflyTrafficOnSite(ctx, fg, pf.Source, true); err != nil {
				log.FromContext(ctx).Error(err, "post-promotion: restore source traffic=enabled", "site", pf.Source)
			}
		}
	}
	r.bestEffortClientKillSource(ctx, fg, pf.Source)
	if sourceDemoted {
		r.bestEffortRecreateTakeoverSource(ctx, fg, pf.Source)
	}
	r.stampDragonflyLastPromotion(ctx, fg, pf.Target, metav1.Now())
	metrics.DragonflyPromotionsTotal.WithLabelValues(fg.Name, pf.Target, "success").Inc()
	if r.Recorder != nil {
		r.Recorder.Eventf(fg, corev1.EventTypeNormal, ReasonDragonflyPromotionCompleted,
			"Dragonfly target %q promoted via REPLTAKEOVER", pf.Target)
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
