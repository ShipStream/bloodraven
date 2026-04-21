// Package controller — planned-failover state machine driver.
//
// Entry point `reconcilePlannedFailover` is invoked from
// MysqlFailoverGroupReconciler.Reconcile alongside reconcileInPlaceRestore.
// The state machine advances one phase per reconcile so operator
// restarts always land on a well-defined observable state.
//
// Lifecycle (happy path):
//
//	Pending → Validating → Draining → WaitingForLag → Promoting → Resuming → Succeeded
//
// Failure paths always either succeed in unfencing the old primary or
// leave a clear terminal status explaining what an operator must do.
package controller

import (
	"context"
	"fmt"
	"time"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	k8sretry "k8s.io/client-go/util/retry"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/log"

	v1alpha1 "github.com/shipstream/bloodraven/api/v1alpha1"
	"github.com/shipstream/bloodraven/internal/metrics"
	internalmysql "github.com/shipstream/bloodraven/internal/mysql"
)

// plannedFailoverLagPollInterval is how often WaitingForLag re-queries
// the target's GTID_EXECUTED while waiting for it to cover the source's
// fenced GTID. Kept deliberately shorter than the topology manager's
// 2s poll so the state machine reacts within one MySQL roundtrip.
const plannedFailoverLagPollInterval = 1 * time.Second

// plannedFailoverTerminal reports whether the given status is in a
// terminal phase (Succeeded or Failed).
func plannedFailoverTerminal(s *v1alpha1.PlannedFailoverStatus) bool {
	if s == nil {
		return false
	}
	return s.Phase == v1alpha1.PlannedFailoverPhaseSucceeded ||
		s.Phase == v1alpha1.PlannedFailoverPhaseFailed
}

// plannedFailoverInFlight reports whether the given status is in a
// non-terminal, non-empty phase. Used to drive the topology guard.
func plannedFailoverInFlight(s *v1alpha1.PlannedFailoverStatus) bool {
	if s == nil {
		return false
	}
	switch s.Phase {
	case v1alpha1.PlannedFailoverPhasePending,
		v1alpha1.PlannedFailoverPhaseValidating,
		v1alpha1.PlannedFailoverPhaseDraining,
		v1alpha1.PlannedFailoverPhaseWaitingForLag,
		v1alpha1.PlannedFailoverPhasePromoting,
		v1alpha1.PlannedFailoverPhaseResuming:
		return true
	}
	return false
}

// reconcilePlannedFailover drives the planned-failover state machine
// for the given CR. Returns a non-zero requeue duration when the
// reconciler should wake up before the default resync; a nil error
// means any caller error was already stamped into status and no retry
// is needed.
func (r *MysqlFailoverGroupReconciler) reconcilePlannedFailover(ctx context.Context, fg *v1alpha1.MysqlFailoverGroup) (time.Duration, error) {
	logger := log.FromContext(ctx)
	nn := types.NamespacedName{Namespace: fg.Namespace, Name: fg.Name}

	// Keep the topology-manager guard in sync with the observed status.
	// This is idempotent and cheap; setting it unconditionally guards
	// against operator restarts landing between phase transitions.
	if r.Runner != nil {
		r.Runner.SetPlannedFailoverActive(nn, plannedFailoverInFlight(fg.Status.PlannedFailover))
	}

	// Did the admin just arm a new run? Look for the annotation.
	raw, hasAnnotation := fg.GetAnnotations()[PlannedFailoverAnnotation]

	// No annotation, no active status → nothing to do.
	if !hasAnnotation && fg.Status.PlannedFailover == nil {
		return 0, nil
	}

	// No annotation but terminal status present → nothing to drive. Leave
	// the terminal block in place for `kubectl describe` to show.
	if !hasAnnotation && plannedFailoverTerminal(fg.Status.PlannedFailover) {
		return 0, nil
	}

	// An annotation is present. Two sub-cases:
	//   a) no prior in-flight run → consume + start a new run
	//   b) an in-flight run already exists → ignore the annotation
	//      until terminal (prevents re-arm during a live run)
	if hasAnnotation && plannedFailoverInFlight(fg.Status.PlannedFailover) {
		// Stale / duplicate annotation during an active run. Clear it
		// so the admin is not surprised by delayed re-arm behaviour.
		if err := r.removePlannedFailoverAnnotation(ctx, nn); err != nil {
			logger.Error(err, "remove stale planned-failover annotation", "fg", nn)
		}
		r.Recorder.Eventf(fg, corev1.EventTypeWarning, "PlannedFailoverRejected",
			"planned-failover annotation ignored: a previous planned failover is still running (phase=%q)",
			fg.Status.PlannedFailover.Phase)
	}

	// Fresh annotation + no in-flight run: parse, validate, and stamp
	// Pending. On rejection the annotation is cleared + event emitted.
	if hasAnnotation && !plannedFailoverInFlight(fg.Status.PlannedFailover) {
		if d, err := r.acceptPlannedFailoverAnnotation(ctx, fg, nn, raw); err != nil {
			return 0, err
		} else if d > 0 {
			return d, nil
		}
	}

	// Dispatch on the current phase.
	cur := fg.Status.PlannedFailover
	if cur == nil || plannedFailoverTerminal(cur) {
		return 0, nil
	}
	switch cur.Phase {
	case v1alpha1.PlannedFailoverPhasePending, v1alpha1.PlannedFailoverPhaseValidating:
		return r.plannedFailoverValidating(ctx, fg, nn)
	case v1alpha1.PlannedFailoverPhaseDraining:
		return r.plannedFailoverDraining(ctx, fg, nn)
	case v1alpha1.PlannedFailoverPhaseWaitingForLag:
		return r.plannedFailoverWaitingForLag(ctx, fg, nn)
	case v1alpha1.PlannedFailoverPhasePromoting:
		return r.plannedFailoverPromoting(ctx, fg, nn)
	case v1alpha1.PlannedFailoverPhaseResuming:
		return r.plannedFailoverResuming(ctx, fg, nn)
	}
	// Unknown phase: wipe so the next reconcile can restart from scratch.
	r.setPlannedFailoverStatus(ctx, fg, nil)
	return 2 * time.Second, nil
}

// acceptPlannedFailoverAnnotation parses + validates the annotation
// value. On accept it stamps status.plannedFailover.phase=Pending,
// records the target + maxLagWait + startTime, clears the annotation,
// emits PlannedFailoverStarted, and returns a short requeue. On skip
// (already-active) it emits PlannedFailoverSkipped and clears. On
// reject it stamps a terminal Failed status with the Reason tag and
// clears.
func (r *MysqlFailoverGroupReconciler) acceptPlannedFailoverAnnotation(ctx context.Context, fg *v1alpha1.MysqlFailoverGroup, nn types.NamespacedName, raw string) (time.Duration, error) {
	req, parseErr := parsePlannedFailoverAnnotation(raw)
	if parseErr != nil {
		r.Recorder.Eventf(fg, corev1.EventTypeWarning, "PlannedFailoverRejected", "%s", parseErr.Error())
		metrics.PlannedFailoversTotal.WithLabelValues("", "rejected").Inc()
		if err := r.removePlannedFailoverAnnotation(ctx, nn); err != nil {
			log.FromContext(ctx).Error(err, "remove invalid planned-failover annotation", "fg", nn)
		}
		return 0, nil
	}

	result, reason, err := validatePlannedFailoverRequest(fg, req, time.Now())
	switch result {
	case PlannedFailoverSkip:
		r.Recorder.Eventf(fg, corev1.EventTypeNormal, "PlannedFailoverSkipped",
			"planned-failover: site %q is already the active primary; nothing to do", req.Site)
		if rmErr := r.removePlannedFailoverAnnotation(ctx, nn); rmErr != nil {
			log.FromContext(ctx).Error(rmErr, "remove idempotent planned-failover annotation", "fg", nn)
		}
		return 0, nil
	case PlannedFailoverReject:
		// Stamp a terminal Failed block so kubectl describe shows the
		// rejection reason. Don't increment lost/promotion counters —
		// nothing mutated.
		now := metav1.Now()
		r.setPlannedFailoverStatus(ctx, fg, &v1alpha1.PlannedFailoverStatus{
			Phase:          v1alpha1.PlannedFailoverPhaseFailed,
			Target:         req.Site,
			SourcePrimary:  fg.Status.ActiveSite,
			StartTime:      &now,
			CompletionTime: &now,
			Reason:         reason,
			Message:        err.Error(),
		})
		r.Recorder.Eventf(fg, corev1.EventTypeWarning, "PlannedFailoverRejected", "%s", err.Error())
		metrics.PlannedFailoversTotal.WithLabelValues(req.Site, "rejected").Inc()
		if rmErr := r.removePlannedFailoverAnnotation(ctx, nn); rmErr != nil {
			log.FromContext(ctx).Error(rmErr, "remove rejected planned-failover annotation", "fg", nn)
		}
		return 0, nil
	}

	// Accept: stamp Pending, clear annotation, requeue fast.
	now := metav1.Now()
	maxLagWait := effectiveMaxLagWait(fg, req)
	durWrap := metav1.Duration{Duration: maxLagWait}
	r.setPlannedFailoverStatus(ctx, fg, &v1alpha1.PlannedFailoverStatus{
		Phase:         v1alpha1.PlannedFailoverPhasePending,
		Target:        req.Site,
		SourcePrimary: fg.Status.ActiveSite,
		StartTime:     &now,
		Message:       fmt.Sprintf("admin requested graceful switchover to %q", req.Site),
		MaxLagWait:    &durWrap,
	})
	if rmErr := r.removePlannedFailoverAnnotation(ctx, nn); rmErr != nil {
		log.FromContext(ctx).Error(rmErr, "remove accepted planned-failover annotation", "fg", nn)
	}
	r.Recorder.Eventf(fg, corev1.EventTypeNormal, "PlannedFailoverStarted",
		"planned failover from %q to %q accepted (maxLagWait=%s)",
		fg.Status.ActiveSite, req.Site, maxLagWait)
	return 1 * time.Second, nil
}

// plannedFailoverValidating re-checks pre-conditions one more time
// (cluster state may have shifted since the annotation was accepted),
// then transitions to Draining. The cooldown check here is redundant
// with the one in acceptPlannedFailoverAnnotation but defends against
// a concurrent emergency failover bumping status.lastFailover between
// the two reconciles.
func (r *MysqlFailoverGroupReconciler) plannedFailoverValidating(ctx context.Context, fg *v1alpha1.MysqlFailoverGroup, nn types.NamespacedName) (time.Duration, error) {
	cur := fg.Status.PlannedFailover
	req := PlannedFailoverRequest{Site: cur.Target}
	if cur.MaxLagWait != nil && cur.MaxLagWait.Duration > 0 {
		req.MaxLagWait = cur.MaxLagWait.Duration
	}

	result, reason, err := validatePlannedFailoverRequest(fg, req, time.Now())
	if result != PlannedFailoverAccept {
		return r.plannedFailoverFail(ctx, fg, reason, err.Error(), "failed_other")
	}

	// Advance to Draining on the next reconcile so status transitions
	// are observable one at a time.
	next := cur.DeepCopy()
	next.Phase = v1alpha1.PlannedFailoverPhaseDraining
	next.Message = fmt.Sprintf("fencing source primary %q", next.SourcePrimary)
	r.setPlannedFailoverStatus(ctx, fg, next)
	// Set the topology guard now so the draining reconcile cannot
	// race an emergency failover.
	if r.Runner != nil {
		r.Runner.SetPlannedFailoverActive(nn, true)
	}
	return 1 * time.Second, nil
}

// plannedFailoverDraining applies super_read_only=ON on the source and
// records its GTID_EXECUTED. The primary role label will be stripped
// by syncPodLabels on this reconcile pass (the reconciler already
// calls syncPodLabels at the end of Reconcile).
func (r *MysqlFailoverGroupReconciler) plannedFailoverDraining(ctx context.Context, fg *v1alpha1.MysqlFailoverGroup, nn types.NamespacedName) (time.Duration, error) {
	cur := fg.Status.PlannedFailover
	if r.Runner == nil {
		return r.plannedFailoverFail(ctx, fg, "InternalError",
			"planned-failover: runner not wired; cannot fence source primary",
			"failed_other")
	}

	// If the source disappears between annotation and Draining, there
	// is nothing to fence — we hand off to the emergency path and
	// stamp Failed with SourceCrashed so the operator sees why the
	// planned attempt was abandoned.
	if cur.SourcePrimary == "" {
		return r.plannedFailoverFail(ctx, fg, "SourceCrashed",
			"planned-failover: no active source primary at draining phase; emergency failover will handle promotion",
			"failed_other")
	}

	fenceCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	gtid, err := r.Runner.PlannedFailoverFence(fenceCtx, nn, cur.SourcePrimary)
	cancel()
	if err != nil {
		// The source may have crashed mid-drain. We do not unfence —
		// there is nothing to unfence, and the emergency path will
		// handle the (now-empty) primary. Stamp Failed so the operator
		// sees the reason, and let the topology manager take over by
		// clearing the planned-failover flag.
		if r.Runner != nil {
			r.Runner.SetPlannedFailoverActive(nn, false)
		}
		return r.plannedFailoverFail(ctx, fg, "SourceCrashed",
			fmt.Sprintf("planned-failover: failed to fence source primary %q: %v; emergency failover path will take over", cur.SourcePrimary, err),
			"failed_other")
	}

	// Fence took. Advance to WaitingForLag with the recorded GTID.
	next := cur.DeepCopy()
	next.Phase = v1alpha1.PlannedFailoverPhaseWaitingForLag
	next.SourceGtidAtFence = gtid
	next.Message = fmt.Sprintf("source fenced at gtid %s; waiting for target %q to catch up", truncateGtidHint(gtid), cur.Target)
	r.setPlannedFailoverStatus(ctx, fg, next)
	r.Recorder.Eventf(fg, corev1.EventTypeNormal, "PlannedFailoverDraining",
		"fenced source primary %q; waiting for target %q to catch up", cur.SourcePrimary, cur.Target)
	return 1 * time.Second, nil
}

// plannedFailoverWaitingForLag polls the target's GTID_EXECUTED until
// it contains the source's fenced GTID. On catch-up it advances to
// Promoting; on timeout it rolls back (unfences the source, stamps
// Failed{LagTimeout}).
func (r *MysqlFailoverGroupReconciler) plannedFailoverWaitingForLag(ctx context.Context, fg *v1alpha1.MysqlFailoverGroup, nn types.NamespacedName) (time.Duration, error) {
	cur := fg.Status.PlannedFailover
	if r.Runner == nil {
		return r.plannedFailoverFail(ctx, fg, "InternalError",
			"planned-failover: runner not wired; cannot poll target",
			"failed_other")
	}

	// Compute the lag-wait deadline from the StartTime of *this* phase
	// (we stamped into Draining at most ~1s ago, so using the overall
	// StartTime overstates the wait budget by one reconcile). We
	// conservatively use StartTime; the inaccuracy is bounded by how
	// long Draining took to stamp.
	var start time.Time
	if cur.StartTime != nil {
		start = cur.StartTime.Time
	} else {
		start = time.Now()
	}
	maxLagWait := defaultPlannedFailoverMaxLagWait
	if cur.MaxLagWait != nil && cur.MaxLagWait.Duration > 0 {
		maxLagWait = cur.MaxLagWait.Duration
	}
	elapsed := time.Since(start)

	queryCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	targetGtid, err := r.Runner.PlannedFailoverGtidExecuted(queryCtx, nn, cur.Target)
	cancel()
	if err != nil {
		// Transient query failure: log and retry. If we exceed the
		// lag-wait budget we fall into the timeout path below.
		log.FromContext(ctx).Error(err, "planned-failover: query target gtid", "site", cur.Target)
		if elapsed >= maxLagWait {
			return r.plannedFailoverRollback(ctx, fg, nn, "LagTimeout",
				fmt.Sprintf("target %q unresponsive during lag wait (last error: %v); source %q unfenced",
					cur.Target, err, cur.SourcePrimary), elapsed)
		}
		return plannedFailoverLagPollInterval, nil
	}

	caughtUp, cmpErr := gtidContains(targetGtid, cur.SourceGtidAtFence)
	if cmpErr != nil {
		// Malformed GTID: treat as unrecoverable, roll back.
		return r.plannedFailoverRollback(ctx, fg, nn, "LagTimeout",
			fmt.Sprintf("planned-failover: cannot parse GTID sets to compare catch-up (%v); source %q unfenced",
				cmpErr, cur.SourcePrimary), elapsed)
	}

	if caughtUp {
		next := cur.DeepCopy()
		next.Phase = v1alpha1.PlannedFailoverPhasePromoting
		next.TargetGtidAtPromotion = targetGtid
		next.Message = fmt.Sprintf("target %q caught up in %s; promoting", cur.Target, truncateDur(elapsed))
		r.setPlannedFailoverStatus(ctx, fg, next)
		metrics.PlannedFailoverLagWaitSeconds.WithLabelValues(cur.Target).Observe(elapsed.Seconds())
		r.Recorder.Eventf(fg, corev1.EventTypeNormal, "PlannedFailoverLagOK",
			"target %q caught up after %s; proceeding to promote", cur.Target, truncateDur(elapsed))
		return 1 * time.Second, nil
	}

	if elapsed >= maxLagWait {
		// Record the lag-wait observation even on timeout so dashboards
		// show the worst case.
		metrics.PlannedFailoverLagWaitSeconds.WithLabelValues(cur.Target).Observe(elapsed.Seconds())
		return r.plannedFailoverRollback(ctx, fg, nn, "LagTimeout",
			fmt.Sprintf("target %q did not reach source GTID within %s; fence released, primary %q still active",
				cur.Target, maxLagWait, cur.SourcePrimary), elapsed)
	}
	return plannedFailoverLagPollInterval, nil
}

// plannedFailoverPromoting runs FailoverController.Execute against the
// target and flips DNS. On success it advances to Resuming.
func (r *MysqlFailoverGroupReconciler) plannedFailoverPromoting(ctx context.Context, fg *v1alpha1.MysqlFailoverGroup, nn types.NamespacedName) (time.Duration, error) {
	cur := fg.Status.PlannedFailover
	if r.Runner == nil {
		return r.plannedFailoverFail(ctx, fg, "InternalError",
			"planned-failover: runner not wired; cannot promote target",
			"failed_other")
	}

	promoteCtx, cancel := context.WithTimeout(ctx, 2*time.Minute)
	promotionGtid, err := r.Runner.PlannedFailoverPromote(promoteCtx, nn, cur.Target, cur.SourcePrimary)
	cancel()
	if err != nil {
		return r.plannedFailoverFail(ctx, fg, "ExecuteFailed",
			fmt.Sprintf("planned-failover: promotion of %q failed: %v; manual recovery required",
				cur.Target, err),
			"failed_other")
	}

	next := cur.DeepCopy()
	next.Phase = v1alpha1.PlannedFailoverPhaseResuming
	if promotionGtid != "" {
		next.TargetGtidAtPromotion = promotionGtid
	}
	next.Message = fmt.Sprintf("promoted %q; finalising status", cur.Target)
	r.setPlannedFailoverStatus(ctx, fg, next)
	return 1 * time.Second, nil
}

// plannedFailoverResuming writes the shared lastFailover/
// lastFailoverTarget/promotionGtidExecuted fields so the anti-flap
// cooldown applies and dashboards reflect the switchover, then stamps
// Succeeded.
func (r *MysqlFailoverGroupReconciler) plannedFailoverResuming(ctx context.Context, fg *v1alpha1.MysqlFailoverGroup, nn types.NamespacedName) (time.Duration, error) {
	cur := fg.Status.PlannedFailover
	now := metav1.Now()

	// Stamp the shared failover-tracking fields. The topology manager
	// already set its in-memory equivalents in PlannedPromote; here we
	// persist to the CR so the next operator restart reads them back.
	if err := r.patchFailoverTrackingFields(ctx, nn, cur.Target, cur.TargetGtidAtPromotion, now); err != nil {
		log.FromContext(ctx).Error(err, "planned-failover: persist lastFailover fields", "fg", nn)
	}

	// Compute transactionsLost. On a clean planned switchover this is
	// zero by construction: the lag gate only advances once the target
	// GTID ⊇ source GTID at fence. A non-zero value would indicate a
	// bug; surface it for future debugging.
	lost := plannedFailoverTransactionsLost(cur.SourceGtidAtFence, cur.TargetGtidAtPromotion)

	duration := int64(0)
	if cur.StartTime != nil {
		duration = int64(now.Sub(cur.StartTime.Time).Seconds())
		if duration < 0 {
			duration = 0
		}
	}

	next := cur.DeepCopy()
	next.Phase = v1alpha1.PlannedFailoverPhaseSucceeded
	next.CompletionTime = &now
	next.DurationSeconds = &duration
	next.TransactionsLost = &lost
	next.Message = fmt.Sprintf("promoted %q, %d transactions lost", cur.Target, lost)
	r.setPlannedFailoverStatus(ctx, fg, next)

	metrics.PlannedFailoversTotal.WithLabelValues(cur.Target, "success").Inc()
	metrics.PlannedFailoverDurationSeconds.WithLabelValues(cur.Target).Observe(float64(duration))
	// Promotion itself already incremented bloodraven_dns_flips_total
	// inside PlannedPromote. Don't double-count here.

	r.Recorder.Eventf(fg, corev1.EventTypeNormal, "PlannedFailoverCompleted",
		"planned failover to %q complete (duration=%ds, transactionsLost=%d)",
		cur.Target, duration, lost)

	// Clear the topology guard: emergency failover is allowed again
	// (cooldown-gated by the shared lastFailover we just stamped).
	if r.Runner != nil {
		r.Runner.SetPlannedFailoverActive(nn, false)
	}
	return 0, nil
}

// plannedFailoverRollback unfences the source, stamps Failed with the
// given reason, and clears the topology guard. Used from WaitingForLag.
func (r *MysqlFailoverGroupReconciler) plannedFailoverRollback(ctx context.Context, fg *v1alpha1.MysqlFailoverGroup, nn types.NamespacedName, reason, msg string, lagElapsed time.Duration) (time.Duration, error) {
	cur := fg.Status.PlannedFailover

	// Best-effort unfence of the source primary. If this fails the
	// cluster is still consistent — the source has super_read_only=ON
	// but the topology manager will notice on its next poll and clear
	// the flag via its returning-primary logic once the planned-
	// failover guard is released.
	if cur != nil && cur.SourcePrimary != "" && r.Runner != nil {
		unfenceCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
		if err := r.Runner.PlannedFailoverUnfence(unfenceCtx, nn, cur.SourcePrimary); err != nil {
			log.FromContext(ctx).Error(err, "planned-failover: unfence after rollback",
				"fg", nn, "site", cur.SourcePrimary)
		}
		cancel()
	}

	return r.plannedFailoverFail(ctx, fg, reason, msg, "failed_timeout")
}

// plannedFailoverFail stamps a terminal Failed status, emits an event,
// increments the result-labelled counter, and clears the topology guard.
func (r *MysqlFailoverGroupReconciler) plannedFailoverFail(ctx context.Context, fg *v1alpha1.MysqlFailoverGroup, reason, msg, metricResult string) (time.Duration, error) {
	now := metav1.Now()
	cur := fg.Status.PlannedFailover
	var start *metav1.Time
	target := ""
	source := ""
	sourceGtid := ""
	maxLagWait := (*metav1.Duration)(nil)
	if cur != nil {
		start = cur.StartTime
		target = cur.Target
		source = cur.SourcePrimary
		sourceGtid = cur.SourceGtidAtFence
		maxLagWait = cur.MaxLagWait
	}
	if start == nil {
		start = &now
	}

	duration := int64(now.Sub(start.Time).Seconds())
	if duration < 0 {
		duration = 0
	}
	nn := types.NamespacedName{Namespace: fg.Namespace, Name: fg.Name}

	r.setPlannedFailoverStatus(ctx, fg, &v1alpha1.PlannedFailoverStatus{
		Phase:             v1alpha1.PlannedFailoverPhaseFailed,
		Target:            target,
		SourcePrimary:     source,
		SourceGtidAtFence: sourceGtid,
		StartTime:         start,
		CompletionTime:    &now,
		DurationSeconds:   &duration,
		Reason:            reason,
		Message:           msg,
		MaxLagWait:        maxLagWait,
	})

	r.Recorder.Eventf(fg, corev1.EventTypeWarning, "PlannedFailoverFailed",
		"planned failover failed (%s): %s", reason, msg)
	metrics.PlannedFailoversTotal.WithLabelValues(target, metricResult).Inc()
	if duration > 0 {
		metrics.PlannedFailoverDurationSeconds.WithLabelValues(target).Observe(float64(duration))
	}

	if r.Runner != nil {
		r.Runner.SetPlannedFailoverActive(nn, false)
	}
	return 0, nil
}

// setPlannedFailoverStatus patches only fg.Status.PlannedFailover.
// Mirrors setInPlaceRestoreStatus's merge-patch pattern. Passing nil
// clears the block.
func (r *MysqlFailoverGroupReconciler) setPlannedFailoverStatus(ctx context.Context, fg *v1alpha1.MysqlFailoverGroup, s *v1alpha1.PlannedFailoverStatus) {
	patch := client.MergeFrom(fg.DeepCopy())
	fg.Status.PlannedFailover = s
	if err := r.Status().Patch(ctx, fg, patch); err != nil && !apierrors.IsNotFound(err) {
		log.FromContext(ctx).Error(err, "update planned-failover status", "fg", fg.Name)
	}
}

// removePlannedFailoverAnnotation deletes the one-shot annotation from
// the CR. Mirrors removeRecloneAnnotation.
func (r *MysqlFailoverGroupReconciler) removePlannedFailoverAnnotation(ctx context.Context, nn types.NamespacedName) error {
	return k8sretry.RetryOnConflict(k8sretry.DefaultRetry, func() error {
		var fresh v1alpha1.MysqlFailoverGroup
		if err := r.Get(ctx, nn, &fresh); err != nil {
			return err
		}
		annotations := fresh.GetAnnotations()
		if annotations == nil {
			return nil
		}
		if _, ok := annotations[PlannedFailoverAnnotation]; !ok {
			return nil
		}
		delete(annotations, PlannedFailoverAnnotation)
		fresh.SetAnnotations(annotations)
		return r.Update(ctx, &fresh)
	})
}

// patchFailoverTrackingFields stamps the shared lastFailover,
// lastFailoverTarget, and promotionGtidExecuted fields on the CR
// status. These are the same fields that the automatic-failover path
// writes, so the cooldown check applies uniformly.
func (r *MysqlFailoverGroupReconciler) patchFailoverTrackingFields(ctx context.Context, nn types.NamespacedName, target, promotionGtid string, now metav1.Time) error {
	return k8sretry.RetryOnConflict(k8sretry.DefaultRetry, func() error {
		var fresh v1alpha1.MysqlFailoverGroup
		if err := r.Get(ctx, nn, &fresh); err != nil {
			return err
		}
		patch := client.MergeFrom(fresh.DeepCopy())
		t := now
		fresh.Status.LastFailover = &t
		fresh.Status.LastFailoverTarget = target
		fresh.Status.ActiveSite = target
		if promotionGtid != "" {
			fresh.Status.PromotionGtidExecuted = promotionGtid
		}
		return r.Status().Patch(ctx, &fresh, patch)
	})
}

// plannedFailoverTransactionsLost returns |sourceGtid \ targetGtid|.
// Zero on a clean planned switchover. On parse errors returns zero so
// we do not stamp a nonsense count; the log line flags the issue.
func plannedFailoverTransactionsLost(sourceGtid, targetGtid string) int64 {
	src, err := internalmysql.ParseGTIDSet(sourceGtid)
	if err != nil {
		return 0
	}
	dst, err := internalmysql.ParseGTIDSet(targetGtid)
	if err != nil {
		return 0
	}
	// Per-UUID intervals on source that are not covered by the target.
	var lost int64
	for uuid, srcIntervals := range src {
		dstIntervals := dst[uuid]
		for _, si := range srcIntervals {
			// Count transactions in si that are NOT inside any dst
			// interval of the same uuid.
			covered := int64(0)
			for _, di := range dstIntervals {
				lo := maxInt64(si.Start, di.Start)
				hi := minInt64(si.End, di.End)
				if hi >= lo {
					covered += hi - lo + 1
				}
			}
			span := si.End - si.Start + 1
			if span > covered {
				lost += span - covered
			}
		}
	}
	return lost
}

// gtidContains reports whether superGtid ⊇ subGtid (i.e. every GTID in
// subGtid is also in superGtid). An error is returned when either
// string fails to parse.
func gtidContains(superGtid, subGtid string) (bool, error) {
	super, err := internalmysql.ParseGTIDSet(superGtid)
	if err != nil {
		return false, fmt.Errorf("parse super gtid: %w", err)
	}
	sub, err := internalmysql.ParseGTIDSet(subGtid)
	if err != nil {
		return false, fmt.Errorf("parse sub gtid: %w", err)
	}
	return super.Contains(sub), nil
}

// truncateGtidHint returns a short prefix of a GTID set suitable for
// status.message. Keeping it short avoids flooding the event / status
// with ~thousands of interval boundaries.
func truncateGtidHint(s string) string {
	const hintLen = 48
	if len(s) <= hintLen {
		return s
	}
	return s[:hintLen] + "…"
}

// truncateDur rounds a duration to the nearest 100ms for concise
// human-readable display in messages and events.
func truncateDur(d time.Duration) time.Duration {
	return d.Round(100 * time.Millisecond)
}

func maxInt64(a, b int64) int64 {
	if a > b {
		return a
	}
	return b
}

func minInt64(a, b int64) int64 {
	if a < b {
		return a
	}
	return b
}


