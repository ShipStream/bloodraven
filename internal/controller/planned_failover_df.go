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
	"github.com/shipstream/bloodraven/internal/dragonfly"
	"github.com/shipstream/bloodraven/internal/metrics"
)

// defaultDragonflySyncWait is the fallback used when neither the spec
// nor the recorded planned-failover status carries a Dragonfly sync
// budget. Matches the API default in DragonflyPlannedFailoverSpec.
const defaultDragonflySyncWait = 30 * time.Second

// dragonflySyncPollInterval is how often WaitingForDragonflySync polls
// the target replica's INFO replication. Aligned with the MySQL lag
// poll cadence so phase transitions feel symmetrical to operators.
const dragonflySyncPollInterval = 1 * time.Second

// effectiveDragonflyOnSyncTimeout returns the configured policy or the
// default ("proceed") when none is set. Centralized so the handlers
// stay terse.
func effectiveDragonflyOnSyncTimeout(fg *v1alpha1.MysqlFailoverGroup) string {
	if d := fg.Spec.Dragonfly; d != nil && d.PlannedFailover != nil && d.PlannedFailover.OnSyncTimeout != "" {
		return d.PlannedFailover.OnSyncTimeout
	}
	return "proceed"
}

// effectiveDragonflyMaxSyncWait returns the configured budget or the
// default. Reads from spec; the planned-failover status doesn't
// currently snapshot this on accept (kept simple for first slice).
func effectiveDragonflyMaxSyncWait(fg *v1alpha1.MysqlFailoverGroup) time.Duration {
	if d := fg.Spec.Dragonfly; d != nil && d.PlannedFailover != nil && d.PlannedFailover.MaxSyncWait != nil && d.PlannedFailover.MaxSyncWait.Duration > 0 {
		return d.PlannedFailover.MaxSyncWait.Duration
	}
	return defaultDragonflySyncWait
}

// plannedFailoverWaitingForDragonflySync polls the target Dragonfly
// replica's offset until it reaches the source's master_repl_offset
// captured at phase entry, bounded by spec.dragonfly.plannedFailover.maxSyncWait.
//
// On entry this handler also captures the source offset on the first
// reconcile (when status.plannedFailover.dragonfly.sourceOffsetAtDrain is nil).
//
// On catch-up it advances to PromotingDragonfly. On timeout it follows
// the configured onSyncTimeout policy: "proceed" stamps sessionsPreserved=false
// and advances to PromotingDragonfly anyway; "fail" rolls back the MySQL fence
// and stamps Failed{DragonflySyncTimeout}.
func (r *MysqlFailoverGroupReconciler) plannedFailoverWaitingForDragonflySync(ctx context.Context, fg *v1alpha1.MysqlFailoverGroup, nn types.NamespacedName) (time.Duration, error) {
	cur := fg.Status.PlannedFailover

	// Defense-in-depth: if Dragonfly is not actually enabled, skip
	// straight to MySQL promotion. The dispatcher should have routed
	// us elsewhere, but operator restarts may land here mid-phase.
	if !dragonflyEnabled(fg) {
		return r.advancePastDragonflyPhases(ctx, fg, cur, "", "skipped: dragonfly not enabled")
	}

	// Initialise the dragonfly status block on first entry.
	if cur.Dragonfly == nil {
		next := cur.DeepCopy()
		next.Dragonfly = &v1alpha1.PlannedFailoverDragonflyStatus{
			Enabled: true,
			Message: fmt.Sprintf("waiting for target %q replica to catch up", cur.Target),
		}
		if err := r.setPlannedFailoverStatus(ctx, fg, next); err != nil {
			return 0, err
		}
		return dragonflySyncPollInterval, nil
	}

	// Capture the source offset on first entry by querying the source.
	// We use the master_repl_offset from `INFO replication`. If we
	// cannot reach the source we proceed (the source is fenced and may
	// be on its way to becoming a stale master); session preservation
	// is best-effort.
	if cur.Dragonfly.SourceOffsetAtDrain == nil {
		conn, err := r.dragonflyDial(ctx, fg, cur.SourcePrimary)
		if err != nil {
			log.FromContext(ctx).Info("dragonfly: source unreachable at drain capture; proceeding without offset", "error", err)
			zero := int64(0)
			next := cur.DeepCopy()
			next.Dragonfly.SourceOffsetAtDrain = &zero
			next.Dragonfly.Message = "source dragonfly unreachable at drain capture"
			if err := r.setPlannedFailoverStatus(ctx, fg, next); err != nil {
				return 0, err
			}
			return dragonflySyncPollInterval, nil
		}
		info, infoErr := conn.InfoReplication(ctx)
		_ = conn.Close()
		offset := int64(0)
		if infoErr == nil {
			offset = info.MasterReplOffset
		}
		next := cur.DeepCopy()
		next.Dragonfly.SourceOffsetAtDrain = &offset
		next.Dragonfly.Message = fmt.Sprintf("source offset %d captured; polling target", offset)
		if err := r.setPlannedFailoverStatus(ctx, fg, next); err != nil {
			return 0, err
		}
		return dragonflySyncPollInterval, nil
	}

	// Determine elapsed against the lag-wait stamp; the same stamp is
	// reused so we don't add yet another time field.
	var start time.Time
	switch {
	case cur.LagWaitStartTime != nil:
		start = cur.LagWaitStartTime.Time
	case cur.StartTime != nil:
		start = cur.StartTime.Time
	default:
		start = time.Now()
	}
	maxWait := effectiveDragonflyMaxSyncWait(fg)
	elapsed := time.Since(start)

	// Poll the target.
	conn, err := r.dragonflyDial(ctx, fg, cur.Target)
	if err != nil {
		if elapsed >= maxWait {
			return r.dragonflySyncTimeoutHandler(ctx, fg, nn, cur, elapsed, fmt.Sprintf("target unreachable: %v", err))
		}
		return dragonflySyncPollInterval, nil
	}
	info, infoErr := conn.InfoReplication(ctx)
	persist, _ := conn.InfoPersistence(ctx)
	_ = conn.Close()
	if infoErr != nil {
		if elapsed >= maxWait {
			return r.dragonflySyncTimeoutHandler(ctx, fg, nn, cur, elapsed, fmt.Sprintf("INFO replication failed: %v", infoErr))
		}
		return dragonflySyncPollInterval, nil
	}
	sourceOffset := int64(0)
	if cur.Dragonfly.SourceOffsetAtDrain != nil {
		sourceOffset = *cur.Dragonfly.SourceOffsetAtDrain
	}
	if dragonfly.CandidateSyncReady(info, persist, sourceOffset) {
		// Caught up: advance to PromotingDragonfly.
		syncSecs := int64(elapsed.Seconds())
		next := cur.DeepCopy()
		next.Phase = v1alpha1.PlannedFailoverPhasePromotingDragonfly
		next.Dragonfly.SyncWaitSeconds = &syncSecs
		next.Dragonfly.Message = fmt.Sprintf("target replica caught up in %s", truncateDur(elapsed))
		if err := r.setPlannedFailoverStatus(ctx, fg, next); err != nil {
			return 0, err
		}
		return 1 * time.Second, nil
	}
	if elapsed >= maxWait {
		return r.dragonflySyncTimeoutHandler(ctx, fg, nn, cur, elapsed,
			fmt.Sprintf("target offset %d behind source %d", info.SlaveReplOffset, sourceOffset))
	}
	return dragonflySyncPollInterval, nil
}

// dragonflySyncTimeoutHandler implements the onSyncTimeout policy
// branching: "proceed" advances to PromotingDragonfly with sessions
// flagged unpreserved; "fail" rolls back the MySQL fence and stamps
// terminal Failed.
func (r *MysqlFailoverGroupReconciler) dragonflySyncTimeoutHandler(ctx context.Context, fg *v1alpha1.MysqlFailoverGroup, nn types.NamespacedName, cur *v1alpha1.PlannedFailoverStatus, elapsed time.Duration, why string) (time.Duration, error) {
	r.Recorder.Eventf(fg, corev1.EventTypeWarning, ReasonDragonflySyncTimeout,
		"dragonfly sync timed out (%s): %s", truncateDur(elapsed), why)

	policy := effectiveDragonflyOnSyncTimeout(fg)
	if policy == "fail" {
		return r.plannedFailoverRollback(ctx, fg, nn, ReasonDragonflySyncTimeout,
			fmt.Sprintf("dragonfly sync timeout (%s): %s; mysql fence released, primary %q still active",
				truncateDur(elapsed), why, cur.SourcePrimary), "failed_timeout")
	}

	// Proceed best-effort — go through PromotingDragonfly so the operator
	// still attempts REPLTAKEOVER (a flaky link doesn't necessarily mean
	// a useless promote).
	syncSecs := int64(elapsed.Seconds())
	preserved := false
	next := cur.DeepCopy()
	next.Phase = v1alpha1.PlannedFailoverPhasePromotingDragonfly
	if next.Dragonfly == nil {
		next.Dragonfly = &v1alpha1.PlannedFailoverDragonflyStatus{Enabled: true}
	}
	next.Dragonfly.SyncWaitSeconds = &syncSecs
	next.Dragonfly.SessionsPreserved = &preserved
	next.Dragonfly.Reason = ReasonDragonflySyncTimeout
	next.Dragonfly.Message = fmt.Sprintf("sync wait timed out (%s): %s; proceeding best-effort", truncateDur(elapsed), why)
	if err := r.setPlannedFailoverStatus(ctx, fg, next); err != nil {
		return 0, err
	}
	return 1 * time.Second, nil
}

// plannedFailoverPromotingDragonfly issues REPLTAKEOVER against the
// target Dragonfly. On success it stamps sessionsPreserved=true (unless
// already false from sync timeout proceed-path), records the target
// offset, and advances to Promoting (MySQL). On failure it follows the
// onSyncTimeout policy (consistent with the sync phase).
func (r *MysqlFailoverGroupReconciler) plannedFailoverPromotingDragonfly(ctx context.Context, fg *v1alpha1.MysqlFailoverGroup, nn types.NamespacedName) (time.Duration, error) {
	cur := fg.Status.PlannedFailover

	if !dragonflyEnabled(fg) {
		return r.advancePastDragonflyPhases(ctx, fg, cur, "", "skipped: dragonfly not enabled")
	}

	r.Recorder.Eventf(fg, corev1.EventTypeNormal, ReasonDragonflyPromotionStarted,
		"promoting Dragonfly target %q via REPLTAKEOVER", cur.Target)

	timeout := effectiveDragonflyMaxSyncWait(fg)

	conn, err := r.dragonflyDial(ctx, fg, cur.Target)
	if err != nil {
		return r.dragonflyPromoteFailHandler(ctx, fg, nn, cur, fmt.Sprintf("dial target: %v", err))
	}
	if takeoverErr := conn.ReplTakeover(ctx, timeout); takeoverErr != nil {
		_ = conn.Close()
		return r.dragonflyPromoteFailHandler(ctx, fg, nn, cur, fmt.Sprintf("REPLTAKEOVER: %v", takeoverErr))
	}
	// Capture the post-promotion offset best-effort.
	postInfo, _ := conn.InfoReplication(ctx)
	_ = conn.Close()

	// Stamp success on the dragonfly sub-status.
	preservedAny := true
	preservedFinal := preservedAny
	if cur.Dragonfly != nil && cur.Dragonfly.SessionsPreserved != nil && !*cur.Dragonfly.SessionsPreserved {
		// A previous sync timeout already marked sessions lost; keep that.
		preservedFinal = false
	}
	now := metav1.Now()
	next := cur.DeepCopy()
	if next.Dragonfly == nil {
		next.Dragonfly = &v1alpha1.PlannedFailoverDragonflyStatus{Enabled: true}
	}
	offset := postInfo.MasterReplOffset
	if offset == 0 {
		offset = postInfo.SlaveReplOffset
	}
	if offset != 0 {
		next.Dragonfly.TargetOffsetAtPromotion = &offset
	}
	next.Dragonfly.SessionsPreserved = &preservedFinal
	next.Dragonfly.PromotionMethod = "REPLTAKEOVER"
	next.Dragonfly.Message = fmt.Sprintf("REPLTAKEOVER succeeded on %q (sessions_preserved=%t)", cur.Target, preservedFinal)
	next.Phase = v1alpha1.PlannedFailoverPhasePromoting
	if err := r.setPlannedFailoverStatus(ctx, fg, next); err != nil {
		return 0, err
	}

	// Update the parent DragonflyStatus.lastPromotion fields (best-effort).
	r.stampDragonflyLastPromotion(ctx, fg, cur.Target, now)

	metrics.DragonflyPromotionsTotal.WithLabelValues(fg.Name, cur.Target, "success").Inc()
	r.Recorder.Eventf(fg, corev1.EventTypeNormal, ReasonDragonflyPromotionCompleted,
		"Dragonfly target %q promoted via REPLTAKEOVER (sessions_preserved=%t)", cur.Target, preservedFinal)
	return 1 * time.Second, nil
}

// dragonflyPromoteFailHandler reacts to REPLTAKEOVER failure. Same
// policy as the sync timeout: "proceed" continues to MySQL promotion
// flagged unpreserved; "fail" rolls back.
func (r *MysqlFailoverGroupReconciler) dragonflyPromoteFailHandler(ctx context.Context, fg *v1alpha1.MysqlFailoverGroup, nn types.NamespacedName, cur *v1alpha1.PlannedFailoverStatus, why string) (time.Duration, error) {
	metrics.DragonflyPromotionsTotal.WithLabelValues(fg.Name, cur.Target, "failed").Inc()
	r.Recorder.Eventf(fg, corev1.EventTypeWarning, ReasonDragonflyPromotionFailed, "%s", why)

	policy := effectiveDragonflyOnSyncTimeout(fg)
	if policy == "fail" {
		return r.plannedFailoverRollback(ctx, fg, nn, ReasonDragonflyPromotionFailed,
			fmt.Sprintf("dragonfly promotion failed (%s); mysql fence released, primary %q still active",
				why, cur.SourcePrimary), "failed_other")
	}

	preserved := false
	next := cur.DeepCopy()
	if next.Dragonfly == nil {
		next.Dragonfly = &v1alpha1.PlannedFailoverDragonflyStatus{Enabled: true}
	}
	next.Dragonfly.SessionsPreserved = &preserved
	next.Dragonfly.Reason = ReasonDragonflyPromotionFailed
	next.Dragonfly.Message = "REPLTAKEOVER failed; continuing best-effort: " + why
	next.Phase = v1alpha1.PlannedFailoverPhasePromoting
	if err := r.setPlannedFailoverStatus(ctx, fg, next); err != nil {
		return 0, err
	}
	return 1 * time.Second, nil
}

// advancePastDragonflyPhases is used when Dragonfly is disabled or
// otherwise inactive: from either of the new phases, jump straight to
// MySQL Promoting.
func (r *MysqlFailoverGroupReconciler) advancePastDragonflyPhases(ctx context.Context, fg *v1alpha1.MysqlFailoverGroup, cur *v1alpha1.PlannedFailoverStatus, _ string, msg string) (time.Duration, error) {
	next := cur.DeepCopy()
	next.Phase = v1alpha1.PlannedFailoverPhasePromoting
	if next.Dragonfly == nil {
		next.Dragonfly = &v1alpha1.PlannedFailoverDragonflyStatus{
			Enabled:         false,
			PromotionMethod: "skipped",
			Message:         msg,
		}
	}
	if err := r.setPlannedFailoverStatus(ctx, fg, next); err != nil {
		return 0, err
	}
	metrics.DragonflyPromotionsTotal.WithLabelValues(fg.Name, cur.Target, "skipped").Inc()
	return 1 * time.Second, nil
}

// stampDragonflyLastPromotion is a best-effort patch on
// status.dragonfly.{lastPromotionTime,lastPromotionTarget}. Failures
// are logged; the planned-failover state machine continues regardless.
func (r *MysqlFailoverGroupReconciler) stampDragonflyLastPromotion(ctx context.Context, fg *v1alpha1.MysqlFailoverGroup, target string, when metav1.Time) {
	var fresh v1alpha1.MysqlFailoverGroup
	if err := r.Get(ctx, types.NamespacedName{Namespace: fg.Namespace, Name: fg.Name}, &fresh); err != nil {
		log.FromContext(ctx).Error(err, "stampDragonflyLastPromotion: get fg")
		return
	}
	if fresh.Status.Dragonfly == nil {
		fresh.Status.Dragonfly = &v1alpha1.DragonflyStatus{Enabled: true}
	}
	fresh.Status.Dragonfly.LastPromotionTime = &when
	fresh.Status.Dragonfly.LastPromotionTarget = target
	if err := r.Status().Update(ctx, &fresh); err != nil {
		log.FromContext(ctx).Error(err, "stampDragonflyLastPromotion: update status")
	}
}
