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

// plannedFailoverDragonflyStripActive reports whether the planned-
// failover state machine has transiently stripped the traffic label
// from the source Dragonfly pod and not yet completed the takeover.
//
// While true, syncDragonflyPodLabels must NOT re-stamp the traffic
// label on the source pod, otherwise it would re-attach the soon-to-be-
// demoted master to the active Service mid-REPLTAKEOVER and re-introduce
// the dual-master selector window the strip exists to close.
//
// The predicate is "phase is PromotingDragonfly AND PromotionMethod is
// not yet stamped." Both success and failure exits stamp PromotionMethod
// (or advance phase entirely), so the gate releases atomically with
// the transition.
func plannedFailoverDragonflyStripActive(fg *v1alpha1.MysqlFailoverGroup) bool {
	if fg == nil || fg.Status.PlannedFailover == nil {
		return false
	}
	pf := fg.Status.PlannedFailover
	if pf.Phase != v1alpha1.PlannedFailoverPhasePromotingDragonfly {
		return false
	}
	if pf.Dragonfly == nil {
		return true
	}
	return pf.Dragonfly.PromotionMethod == ""
}

// plannedFailoverSourceSite returns the source-primary site name from
// the in-flight planned-failover status, or "" if no planned failover
// is active.
func plannedFailoverSourceSite(fg *v1alpha1.MysqlFailoverGroup) string {
	if fg == nil || fg.Status.PlannedFailover == nil {
		return ""
	}
	return fg.Status.PlannedFailover.SourcePrimary
}

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

// plannedFailoverPromotingDragonfly executes the four-step planned
// promotion sequence atomically within one reconcile pass:
//
//  1. Strip the dragonfly-traffic label from the source pod. The active
//     Service selector requires (role=master AND traffic=enabled), so
//     this atomically sheds the source endpoint regardless of the
//     pending role-label flip.
//
//  2. Issue REPLTAKEOVER against the target Dragonfly. This is a
//     blocking call that can take up to MaxSyncWait (~30s default).
//
//  3. On success, stamp role+traffic labels on the target (master,
//     enabled) and demote the source to role=replica with its traffic
//     label restored. The DragonflyManager will then keep the source
//     wired as a replica via REPLICAOF on its next tick.
//
//  4. Best-effort CLIENT KILL TYPE NORMAL on the now-demoted source so
//     application clients reconnect through the active Service and
//     land on the new master. Failure here is non-fatal.
//
// On REPLTAKEOVER failure the source's traffic label is restored before
// the failure handler runs, so the active Service can keep routing
// (proceed mode) or the rollback path returns to a known good state.
//
// PromotionMethod is the gate that releases syncDragonflyPodLabels'
// restraint on the source's traffic label (see plannedFailoverDragonflyStripActive).
// Setting PromotionMethod after step 3 means a syncDragonflyPodLabels
// run later in the same reconcile is harmless: target+source labels
// already reflect the desired steady state.
func (r *MysqlFailoverGroupReconciler) plannedFailoverPromotingDragonfly(ctx context.Context, fg *v1alpha1.MysqlFailoverGroup, nn types.NamespacedName) (time.Duration, error) {
	cur := fg.Status.PlannedFailover

	if !dragonflyEnabled(fg) {
		return r.advancePastDragonflyPhases(ctx, fg, cur, "", "skipped: dragonfly not enabled")
	}

	r.Recorder.Eventf(fg, corev1.EventTypeNormal, ReasonDragonflyPromotionStarted,
		"promoting Dragonfly target %q via REPLTAKEOVER", cur.Target)

	// Step 1: strip the source's traffic label. Idempotent on retries
	// (operator restart between strip and takeover lands here again).
	if cur.SourcePrimary != "" {
		if err := r.setDragonflyTrafficOnSite(ctx, fg, cur.SourcePrimary, false); err != nil {
			// Label patch is the only K8s-side step before REPLTAKEOVER.
			// A patch failure means the source might still be in the
			// active Service when we promote the target — the dual-master
			// window we're trying to close. Treat as a promotion failure.
			return r.dragonflyPromoteFailHandler(ctx, fg, nn, cur, fmt.Sprintf("strip source traffic label: %v", err))
		}
	}

	timeout := effectiveDragonflyMaxSyncWait(fg)

	// Step 2: REPLTAKEOVER on target.
	conn, err := r.dragonflyDial(ctx, fg, cur.Target)
	if err != nil {
		// Restore source traffic before invoking the failure handler so
		// the active Service still routes to the source on proceed-mode
		// best-effort, and the rollback path returns to a clean state.
		r.bestEffortRestoreSourceTraffic(ctx, fg, cur.SourcePrimary)
		return r.dragonflyPromoteFailHandler(ctx, fg, nn, cur, fmt.Sprintf("dial target: %v", err))
	}
	if takeoverErr := conn.ReplTakeover(ctx, timeout); takeoverErr != nil {
		_ = conn.Close()
		r.bestEffortRestoreSourceTraffic(ctx, fg, cur.SourcePrimary)
		return r.dragonflyPromoteFailHandler(ctx, fg, nn, cur, fmt.Sprintf("REPLTAKEOVER: %v", takeoverErr))
	}
	// Capture the post-promotion offset best-effort.
	postInfo, _ := conn.InfoReplication(ctx)
	_ = conn.Close()

	// Step 3: re-label target as master+traffic, source as replica with
	// traffic restored. Both are best-effort writes against the
	// kubeapi server; partial success is fine because syncDragonflyPodLabels
	// will reconcile the steady state on the next reconcile pass once
	// PromotionMethod is set.
	if err := r.setDragonflyRoleOnSite(ctx, fg, cur.Target, "master"); err != nil {
		log.FromContext(ctx).Error(err, "post-promotion: stamp target role=master", "site", cur.Target)
	}
	if err := r.setDragonflyTrafficOnSite(ctx, fg, cur.Target, true); err != nil {
		log.FromContext(ctx).Error(err, "post-promotion: stamp target traffic=enabled", "site", cur.Target)
	}
	if cur.SourcePrimary != "" {
		if err := r.setDragonflyRoleOnSite(ctx, fg, cur.SourcePrimary, "replica"); err != nil {
			log.FromContext(ctx).Error(err, "post-promotion: stamp source role=replica", "site", cur.SourcePrimary)
		}
		// Restore the source's traffic label so it rejoins the cluster
		// as a healthy replica (the active Service still ignores it
		// because role=replica).
		if err := r.setDragonflyTrafficOnSite(ctx, fg, cur.SourcePrimary, true); err != nil {
			log.FromContext(ctx).Error(err, "post-promotion: restore source traffic=enabled", "site", cur.SourcePrimary)
		}
	}

	// Step 4: best-effort CLIENT KILL on the old master. Forces app
	// clients to reconnect through the active Service and land on the
	// new master rather than keep talking to the demoted pod via cached
	// pod IPs. Wrapped in its own context so a slow/unreachable old
	// master never holds the reconcile.
	r.bestEffortClientKillSource(ctx, fg, cur.SourcePrimary)

	// Stamp success on the dragonfly sub-status.
	preservedFinal := true
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
func (r *MysqlFailoverGroupReconciler) bestEffortClientKillSource(ctx context.Context, fg *v1alpha1.MysqlFailoverGroup, sourceSite string) {
	if sourceSite == "" {
		return
	}
	killCtx, cancel := context.WithTimeout(ctx, 3*time.Second)
	defer cancel()
	conn, err := r.dragonflyDial(killCtx, fg, sourceSite)
	if err != nil {
		log.FromContext(ctx).Info("client-kill: source unreachable; skipping", "site", sourceSite, "error", err)
		return
	}
	defer func() { _ = conn.Close() }()
	if err := conn.ClientKillType(killCtx, "NORMAL"); err != nil {
		log.FromContext(ctx).Info("client-kill: source rejected CLIENT KILL", "site", sourceSite, "error", err)
		return
	}
	log.FromContext(ctx).Info("client-kill: evicted clients from old master", "site", sourceSite)
}

// dragonflyPromoteFailHandler reacts to REPLTAKEOVER failure. Same
// policy as the sync timeout: "proceed" continues to MySQL promotion
// flagged unpreserved; "fail" rolls back. Callers MUST have already
// restored the source's traffic label (via bestEffortRestoreSourceTraffic)
// before invoking this so the steady-state Service has an endpoint.
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
	// Leave PromotionMethod empty: effectiveDragonflyMasterSite branches
	// on it to decide whether the target has actually been promoted.
	// Stamping anything non-empty here would re-route the active Service
	// selector to the target, which never took over.
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
