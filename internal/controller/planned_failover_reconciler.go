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

// plannedFailoverInFlight reports whether the given status represents
// an *active* state machine run — one that is holding the topology
// manager guard and making forward progress toward promotion. Deferred
// is intentionally excluded: we have not fenced the source yet, so the
// topology manager must remain free to handle an emergency failover
// (the same cooldown check applies to both paths).
func plannedFailoverInFlight(s *v1alpha1.PlannedFailoverStatus) bool {
	if s == nil {
		return false
	}
	switch s.Phase {
	case v1alpha1.PlannedFailoverPhasePending,
		v1alpha1.PlannedFailoverPhaseValidating,
		v1alpha1.PlannedFailoverPhaseDraining,
		v1alpha1.PlannedFailoverPhaseWaitingForLag,
		v1alpha1.PlannedFailoverPhaseWaitingForDragonflySync,
		v1alpha1.PlannedFailoverPhasePromotingDragonfly,
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

	raw, hasAnnotation := fg.GetAnnotations()[PlannedFailoverAnnotation]
	cur := fg.Status.PlannedFailover

	// Nothing to do: no annotation and no non-terminal status.
	if !hasAnnotation && (cur == nil || plannedFailoverTerminal(cur)) {
		return 0, nil
	}

	// Admin removed the annotation while a deferred request was
	// waiting for cooldown → treat as an explicit cancel.
	if !hasAnnotation && cur != nil && cur.Phase == v1alpha1.PlannedFailoverPhaseDeferred {
		return r.plannedFailoverFail(ctx, fg, "Cancelled",
			"planned-failover annotation removed while deferred (cooldown still pending); run cancelled",
			"rejected")
	}

	// Annotation present: decide between accepting a fresh arm,
	// retrying a deferred run, or clearing a stale duplicate.
	if hasAnnotation {
		switch {
		case cur == nil || plannedFailoverTerminal(cur):
			// Fresh arm.
			if d, err := r.acceptPlannedFailoverAnnotation(ctx, fg, nn, raw); err != nil {
				return 0, err
			} else if d > 0 {
				return d, nil
			}
			cur = fg.Status.PlannedFailover
		case cur.Phase == v1alpha1.PlannedFailoverPhaseDeferred:
			// Retry path handled in the phase dispatch below. Fall
			// through without consuming the annotation.
		default:
			// Stale duplicate during an active run. Clear and warn so
			// the admin does not see delayed re-arm behaviour.
			if err := r.removePlannedFailoverAnnotation(ctx, nn); err != nil {
				logger.Error(err, "remove stale planned-failover annotation", "fg", nn)
			}
			r.Recorder.Eventf(fg, corev1.EventTypeWarning, "PlannedFailoverRejected",
				"planned-failover annotation ignored: a previous planned failover is still running (phase=%q)",
				cur.Phase)
		}
	}

	// Phase dispatch.
	if cur == nil || plannedFailoverTerminal(cur) {
		return 0, nil
	}
	switch cur.Phase {
	case v1alpha1.PlannedFailoverPhasePending, v1alpha1.PlannedFailoverPhaseValidating:
		return r.plannedFailoverValidating(ctx, fg, nn)
	case v1alpha1.PlannedFailoverPhaseDeferred:
		return r.plannedFailoverDeferredReconcile(ctx, fg, nn, raw)
	case v1alpha1.PlannedFailoverPhaseDraining:
		return r.plannedFailoverDraining(ctx, fg, nn)
	case v1alpha1.PlannedFailoverPhaseWaitingForLag:
		return r.plannedFailoverWaitingForLag(ctx, fg, nn)
	case v1alpha1.PlannedFailoverPhaseWaitingForDragonflySync:
		return r.plannedFailoverWaitingForDragonflySync(ctx, fg, nn)
	case v1alpha1.PlannedFailoverPhasePromotingDragonfly:
		return r.plannedFailoverPromotingDragonfly(ctx, fg, nn)
	case v1alpha1.PlannedFailoverPhasePromoting:
		return r.plannedFailoverPromoting(ctx, fg, nn)
	case v1alpha1.PlannedFailoverPhaseResuming:
		return r.plannedFailoverResuming(ctx, fg, nn)
	}
	// Unknown phase: wipe so the next reconcile can restart from scratch.
	if err := r.setPlannedFailoverStatus(ctx, fg, nil); err != nil {
		return 0, err
	}
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

	now := time.Now()
	result, reason, err := validatePlannedFailoverRequest(fg, req, now, false)
	switch result {
	case PlannedFailoverSkip:
		r.Recorder.Eventf(fg, corev1.EventTypeNormal, "PlannedFailoverSkipped",
			"planned-failover: site %q is already the active primary; nothing to do", req.Site)
		if rmErr := r.removePlannedFailoverAnnotation(ctx, nn); rmErr != nil {
			log.FromContext(ctx).Error(rmErr, "remove idempotent planned-failover annotation", "fg", nn)
		}
		return 0, nil
	case PlannedFailoverReject:
		// Cooldown-active AND spec opts into defer → stamp Deferred,
		// keep the annotation, requeue at cooldown expiry.
		if reason == "CooldownActive" && effectiveOnCooldown(fg) == PlannedFailoverOnCooldownDefer {
			return r.stampDeferred(ctx, fg, req, now, err.Error(), true)
		}

		// Terminal Failed.
		metaNow := metav1.NewTime(now)
		if err := r.setPlannedFailoverStatus(ctx, fg, &v1alpha1.PlannedFailoverStatus{
			Phase:          v1alpha1.PlannedFailoverPhaseFailed,
			Target:         req.Site,
			SourcePrimary:  fg.Status.ActiveSite,
			StartTime:      &metaNow,
			CompletionTime: &metaNow,
			Reason:         reason,
			Message:        err.Error(),
		}); err != nil {
			return 0, err
		}
		r.Recorder.Eventf(fg, corev1.EventTypeWarning, "PlannedFailoverRejected", "%s", err.Error())
		metrics.PlannedFailoversTotal.WithLabelValues(req.Site, "rejected").Inc()
		if rmErr := r.removePlannedFailoverAnnotation(ctx, nn); rmErr != nil {
			log.FromContext(ctx).Error(rmErr, "remove rejected planned-failover annotation", "fg", nn)
		}
		return 0, nil
	}

	// Accept: stamp Pending, clear annotation, requeue fast.
	metaNow := metav1.NewTime(now)
	maxLagWait := effectiveMaxLagWait(fg, req)
	drainTimeout := effectiveDrainTimeout(fg)
	lagWrap := metav1.Duration{Duration: maxLagWait}
	drainWrap := metav1.Duration{Duration: drainTimeout}
	if err := r.setPlannedFailoverStatus(ctx, fg, &v1alpha1.PlannedFailoverStatus{
		Phase:         v1alpha1.PlannedFailoverPhasePending,
		Target:        req.Site,
		SourcePrimary: fg.Status.ActiveSite,
		StartTime:     &metaNow,
		Message:       fmt.Sprintf("admin requested graceful switchover to %q", req.Site),
		MaxLagWait:    &lagWrap,
		DrainTimeout:  &drainWrap,
	}); err != nil {
		return 0, err
	}
	if rmErr := r.removePlannedFailoverAnnotation(ctx, nn); rmErr != nil {
		log.FromContext(ctx).Error(rmErr, "remove accepted planned-failover annotation", "fg", nn)
	}
	r.Recorder.Eventf(fg, corev1.EventTypeNormal, "PlannedFailoverStarted",
		"planned failover from %q to %q accepted (maxLagWait=%s, drainTimeout=%s)",
		fg.Status.ActiveSite, req.Site, maxLagWait, drainTimeout)
	return 1 * time.Second, nil
}

// stampDeferred writes the Deferred phase and returns a requeue bound
// by the cooldown expiry so we wake up exactly when retry is due.
// When initial is true the first PlannedFailoverDeferred event is
// emitted; subsequent updates (same phase already stamped) stay silent
// to avoid flooding the event recorder.
func (r *MysqlFailoverGroupReconciler) stampDeferred(ctx context.Context, fg *v1alpha1.MysqlFailoverGroup, req PlannedFailoverRequest, now time.Time, rejectMsg string, initial bool) (time.Duration, error) {
	metaNow := metav1.NewTime(now)
	maxLagWait := effectiveMaxLagWait(fg, req)
	drainTimeout := effectiveDrainTimeout(fg)
	lagWrap := metav1.Duration{Duration: maxLagWait}
	drainWrap := metav1.Duration{Duration: drainTimeout}

	retry := cooldownRetryAfter(fg)
	var retryMeta *metav1.Time
	if !retry.IsZero() {
		m := metav1.NewTime(retry)
		retryMeta = &m
	}

	// Preserve the original StartTime across re-stamps of the Deferred
	// phase so status.durationSeconds reflects wall-clock from first
	// acceptance, not from the most recent retry.
	start := &metaNow
	if cur := fg.Status.PlannedFailover; cur != nil && cur.StartTime != nil && cur.Phase == v1alpha1.PlannedFailoverPhaseDeferred {
		start = cur.StartTime
	}

	msg := fmt.Sprintf("planned failover to %q deferred until %s (cooldown active); annotation retained for automatic retry",
		req.Site, retry.UTC().Format(time.RFC3339))
	if rejectMsg != "" {
		msg = rejectMsg + " — annotation retained for automatic retry"
	}

	if err := r.setPlannedFailoverStatus(ctx, fg, &v1alpha1.PlannedFailoverStatus{
		Phase:         v1alpha1.PlannedFailoverPhaseDeferred,
		Target:        req.Site,
		SourcePrimary: fg.Status.ActiveSite,
		StartTime:     start,
		Message:       msg,
		Reason:        "CooldownActive",
		MaxLagWait:    &lagWrap,
		DrainTimeout:  &drainWrap,
		RetryAfter:    retryMeta,
	}); err != nil {
		return 0, err
	}

	if initial {
		r.Recorder.Eventf(fg, corev1.EventTypeNormal, "PlannedFailoverDeferred",
			"planned failover to %q deferred: cooldown active, retrying at %s",
			req.Site, retry.UTC().Format(time.RFC3339))
	}

	// Compute a requeue that wakes exactly at retry time (with a 1s
	// cushion so we observe the expired cooldown on the next tick).
	if retry.IsZero() {
		return 30 * time.Second, nil
	}
	delay := time.Until(retry) + time.Second
	if delay < time.Second {
		delay = time.Second
	}
	if delay > 10*time.Minute {
		delay = 10 * time.Minute
	}
	return delay, nil
}

// plannedFailoverDeferredReconcile re-parses the annotation (it may
// have been edited while deferred), re-validates, and either advances
// to Validating when the cooldown has cleared or re-stamps Deferred
// with an updated RetryAfter.
func (r *MysqlFailoverGroupReconciler) plannedFailoverDeferredReconcile(ctx context.Context, fg *v1alpha1.MysqlFailoverGroup, nn types.NamespacedName, raw string) (time.Duration, error) {
	req, parseErr := parsePlannedFailoverAnnotation(raw)
	if parseErr != nil {
		// Annotation edited into something invalid — treat as a reject.
		if rmErr := r.removePlannedFailoverAnnotation(ctx, nn); rmErr != nil {
			log.FromContext(ctx).Error(rmErr, "remove invalid planned-failover annotation", "fg", nn)
		}
		return r.plannedFailoverFail(ctx, fg, "InvalidAnnotation", parseErr.Error(), "rejected")
	}

	now := time.Now()
	result, reason, err := validatePlannedFailoverRequest(fg, req, now, false)
	switch result {
	case PlannedFailoverAccept:
		// Cooldown cleared. Promote the request to Pending and let the
		// normal state machine take over.
		cur := fg.Status.PlannedFailover
		metaNow := metav1.NewTime(now)
		maxLagWait := effectiveMaxLagWait(fg, req)
		drainTimeout := effectiveDrainTimeout(fg)
		lagWrap := metav1.Duration{Duration: maxLagWait}
		drainWrap := metav1.Duration{Duration: drainTimeout}
		start := &metaNow
		if cur != nil && cur.StartTime != nil {
			start = cur.StartTime
		}
		if err := r.setPlannedFailoverStatus(ctx, fg, &v1alpha1.PlannedFailoverStatus{
			Phase:         v1alpha1.PlannedFailoverPhasePending,
			Target:        req.Site,
			SourcePrimary: fg.Status.ActiveSite,
			StartTime:     start,
			Message:       fmt.Sprintf("cooldown cleared; proceeding with planned failover to %q", req.Site),
			MaxLagWait:    &lagWrap,
			DrainTimeout:  &drainWrap,
		}); err != nil {
			return 0, err
		}
		if rmErr := r.removePlannedFailoverAnnotation(ctx, nn); rmErr != nil {
			log.FromContext(ctx).Error(rmErr, "remove promoted-deferred planned-failover annotation", "fg", nn)
		}
		r.Recorder.Eventf(fg, corev1.EventTypeNormal, "PlannedFailoverStarted",
			"deferred planned failover to %q resumed (maxLagWait=%s, drainTimeout=%s)",
			req.Site, maxLagWait, drainTimeout)
		return 1 * time.Second, nil
	case PlannedFailoverSkip:
		r.Recorder.Eventf(fg, corev1.EventTypeNormal, "PlannedFailoverSkipped",
			"deferred planned failover: site %q is already the active primary; clearing annotation", req.Site)
		if rmErr := r.removePlannedFailoverAnnotation(ctx, nn); rmErr != nil {
			log.FromContext(ctx).Error(rmErr, "remove idempotent deferred annotation", "fg", nn)
		}
		// Move to a terminal Failed (describing the skip) so operators
		// can see the outcome on the CR.
		return r.plannedFailoverFail(ctx, fg, "AlreadyActive",
			fmt.Sprintf("deferred planned failover to %q resolved as no-op: target is already active", req.Site),
			"rejected")
	}

	// Still rejected. If cooldown is still active, re-stamp Deferred
	// with an updated retryAfter. Any other reject reason becomes
	// terminal (the cluster changed state in a way that makes the
	// request invalid for more than cooldown).
	if reason == "CooldownActive" {
		return r.stampDeferred(ctx, fg, req, now, err.Error(), false)
	}
	if rmErr := r.removePlannedFailoverAnnotation(ctx, nn); rmErr != nil {
		log.FromContext(ctx).Error(rmErr, "remove newly-rejected deferred annotation", "fg", nn)
	}
	return r.plannedFailoverFail(ctx, fg, reason, err.Error(), "rejected")
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

	result, reason, err := validatePlannedFailoverRequest(fg, req, time.Now(), true)
	if result == PlannedFailoverSkip {
		return r.plannedFailoverFail(ctx, fg, "AlreadyActive",
			fmt.Sprintf("planned-failover: target %q is already the active primary; nothing to do", cur.Target),
			"failed_other")
	}
	if result != PlannedFailoverAccept {
		return r.plannedFailoverFail(ctx, fg, reason, err.Error(), "failed_other")
	}

	// Advance to Draining on the next reconcile so status transitions
	// are observable one at a time.
	next := cur.DeepCopy()
	next.Phase = v1alpha1.PlannedFailoverPhaseDraining
	next.Message = fmt.Sprintf("fencing source primary %q", next.SourcePrimary)
	if err := r.setPlannedFailoverStatus(ctx, fg, next); err != nil {
		return 0, err
	}
	// Set the topology guard now so the draining reconcile cannot
	// race an emergency failover.
	if r.Runner != nil {
		r.Runner.SetPlannedFailoverActive(nn, true)
	}
	return 1 * time.Second, nil
}

// plannedFailoverDraining runs a two-step flow inside the Draining
// phase:
//
//  1. First entry (SourceGtidAtFence == ""): fence the source with
//     super_read_only=ON and record its GTID_EXECUTED. The primary
//     role label will be stripped by syncPodLabels at the end of the
//     reconcile. Stamp DrainStartTime and requeue fast for the drain
//     loop.
//
//  2. Subsequent entries: call KillAppConnections on the source.
//     Advance to WaitingForLag when the last call returned zero
//     killed (clean drain) or DrainTimeout has elapsed (budget
//     exceeded; proceed anyway rather than block indefinitely on a
//     stuck client).
//
// The source primary is already read-only at this point, so no new
// writes can slip in while we drain.
func (r *MysqlFailoverGroupReconciler) plannedFailoverDraining(ctx context.Context, fg *v1alpha1.MysqlFailoverGroup, nn types.NamespacedName) (time.Duration, error) {
	cur := fg.Status.PlannedFailover
	// If the source disappears between annotation and Draining, there
	// is nothing to fence — we hand off to the emergency path and
	// stamp Failed with SourceCrashed so the operator sees why the
	// planned attempt was abandoned. This check runs before the Runner
	// guard so the hand-off path is taken regardless of whether the
	// topology manager is wired yet.
	if cur.SourcePrimary == "" {
		return r.plannedFailoverFail(ctx, fg, "SourceCrashed",
			"planned-failover: no active source primary at draining phase; emergency failover will handle promotion",
			"failed_other")
	}
	if r.Runner == nil {
		return r.plannedFailoverFail(ctx, fg, "InternalError",
			"planned-failover: runner not wired; cannot fence source primary",
			"failed_other")
	}

	// Step 1: first entry into Draining — fence and record GTID.
	if cur.SourceGtidAtFence == "" {
		fenceCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
		gtid, err := r.Runner.PlannedFailoverFence(fenceCtx, nn, cur.SourcePrimary)
		cancel()
		if err != nil {
			// Either the source has crashed (nothing to unfence) or a
			// transient GTID read failed after the fence took effect.
			// FenceSite is transactional — on GTID read failure it
			// best-effort unfences before returning — so we do not need
			// to retry the unfence here. The emergency failover path
			// will handle the now-unavailable primary.
			r.Runner.SetPlannedFailoverActive(nn, false)
			return r.plannedFailoverFail(ctx, fg, "SourceCrashed",
				fmt.Sprintf("planned-failover: failed to fence source primary %q: %v; emergency failover path will take over", cur.SourcePrimary, err),
				"failed_other")
		}

		now := metav1.Now()
		next := cur.DeepCopy()
		next.SourceGtidAtFence = gtid
		next.DrainStartTime = &now
		next.Message = fmt.Sprintf("source fenced at gtid %s; draining connections on %q",
			truncateGtidHint(gtid), cur.SourcePrimary)
		if err := r.setPlannedFailoverStatus(ctx, fg, next); err != nil {
			return 0, err
		}
		r.Recorder.Eventf(fg, corev1.EventTypeNormal, "PlannedFailoverDraining",
			"fenced source primary %q; draining application connections (timeout %s)",
			cur.SourcePrimary, effectiveDrainTimeoutFromStatus(cur))
		return 1 * time.Second, nil
	}

	// Step 2: drain loop. Kill stragglers; advance when idle or budget
	// exhausted.
	drainTimeout := effectiveDrainTimeoutFromStatus(cur)
	var drainStart time.Time
	if cur.DrainStartTime != nil {
		drainStart = cur.DrainStartTime.Time
	}
	elapsed := time.Since(drainStart)
	budgetExceeded := !drainStart.IsZero() && elapsed >= drainTimeout

	killCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	killed, killErr := r.Runner.PlannedFailoverDrainConnections(killCtx, nn, cur.SourcePrimary)
	cancel()

	if killErr != nil {
		// Transient failure to talk to MySQL. If the drain budget has
		// elapsed, proceed anyway — the source is fenced and we do not
		// want a stuck client to block the switchover indefinitely.
		if budgetExceeded {
			return r.advanceToWaitingForLag(ctx, fg, cur,
				fmt.Sprintf("drain budget exhausted after %s (last kill: %v); proceeding to promotion", truncateDur(elapsed), killErr))
		}
		log.FromContext(ctx).Info("planned-failover drain: transient kill error, will retry",
			"fg", nn, "site", cur.SourcePrimary, "error", killErr.Error())
		return 1 * time.Second, nil
	}

	if killed == 0 {
		return r.advanceToWaitingForLag(ctx, fg, cur,
			fmt.Sprintf("source %q drained cleanly in %s", cur.SourcePrimary, truncateDur(elapsed)))
	}
	if budgetExceeded {
		return r.advanceToWaitingForLag(ctx, fg, cur,
			fmt.Sprintf("drain budget exhausted after %s with %d connection(s) remaining on %q; proceeding",
				truncateDur(elapsed), killed, cur.SourcePrimary))
	}
	return 1 * time.Second, nil
}

// advanceToWaitingForLag stamps the WaitingForLag phase with the given
// message and emits no event (the phase-entry event is
// PlannedFailoverLagOK, which fires only after catch-up).
func (r *MysqlFailoverGroupReconciler) advanceToWaitingForLag(ctx context.Context, fg *v1alpha1.MysqlFailoverGroup, cur *v1alpha1.PlannedFailoverStatus, msg string) (time.Duration, error) {
	now := metav1.Now()
	next := cur.DeepCopy()
	next.Phase = v1alpha1.PlannedFailoverPhaseWaitingForLag
	next.LagWaitStartTime = &now
	next.Message = fmt.Sprintf("%s; waiting for target %q to catch up", msg, cur.Target)
	if err := r.setPlannedFailoverStatus(ctx, fg, next); err != nil {
		return 0, err
	}
	return 1 * time.Second, nil
}

// effectiveDrainTimeoutFromStatus returns the drainTimeout captured on
// status at acceptance time; falling back to the package default when
// status does not have it set (e.g. an operator upgraded mid-flight).
func effectiveDrainTimeoutFromStatus(cur *v1alpha1.PlannedFailoverStatus) time.Duration {
	if cur != nil && cur.DrainTimeout != nil && cur.DrainTimeout.Duration > 0 {
		return cur.DrainTimeout.Duration
	}
	return defaultPlannedFailoverDrainTimeout
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

	// Measure the lag-wait budget from LagWaitStartTime (stamped when
	// we entered this phase). Using StartTime would include
	// Pending/Validating/Draining/Deferred time and could cause
	// premature timeouts while also skewing the lag-wait histogram.
	// Fall back to StartTime if the phase-entry stamp is missing (an
	// operator upgraded mid-flight against a status object from the
	// previous schema).
	var start time.Time
	switch {
	case cur.LagWaitStartTime != nil:
		start = cur.LagWaitStartTime.Time
	case cur.StartTime != nil:
		start = cur.StartTime.Time
	default:
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
					cur.Target, err, cur.SourcePrimary), "failed_timeout")
		}
		return plannedFailoverLagPollInterval, nil
	}

	caughtUp, cmpErr := gtidContains(targetGtid, cur.SourceGtidAtFence)
	if cmpErr != nil {
		// Malformed GTID is not a timeout — record it as such so
		// dashboards can distinguish a lag-starved target from a
		// parse/format bug that needs operator attention.
		return r.plannedFailoverRollback(ctx, fg, nn, "InvalidGTID",
			fmt.Sprintf("planned-failover: cannot parse GTID sets to compare catch-up (%v); source %q unfenced",
				cmpErr, cur.SourcePrimary), "failed_other")
	}

	if caughtUp {
		next := cur.DeepCopy()
		// Insert the Dragonfly sync/promote phases between MySQL lag-OK
		// and MySQL promotion when the subsystem is enabled. Disabled
		// failover groups keep the original two-step (lag-OK → promote).
		if dragonflyEnabled(fg) {
			next.Phase = v1alpha1.PlannedFailoverPhaseWaitingForDragonflySync
			next.Message = fmt.Sprintf("target %q caught up in %s; waiting for Dragonfly replica", cur.Target, truncateDur(elapsed))
		} else {
			next.Phase = v1alpha1.PlannedFailoverPhasePromoting
			next.Message = fmt.Sprintf("target %q caught up in %s; promoting", cur.Target, truncateDur(elapsed))
		}
		next.TargetGtidAtPromotion = targetGtid
		if err := r.setPlannedFailoverStatus(ctx, fg, next); err != nil {
			return 0, err
		}
		metrics.PlannedFailoverLagWaitSeconds.WithLabelValues(cur.Target).Observe(elapsed.Seconds())
		r.Recorder.Eventf(fg, corev1.EventTypeNormal, "PlannedFailoverLagOK",
			"target %q caught up after %s; proceeding", cur.Target, truncateDur(elapsed))
		return 1 * time.Second, nil
	}

	if elapsed >= maxLagWait {
		// Record the lag-wait observation even on timeout so dashboards
		// show the worst case.
		metrics.PlannedFailoverLagWaitSeconds.WithLabelValues(cur.Target).Observe(elapsed.Seconds())
		return r.plannedFailoverRollback(ctx, fg, nn, "LagTimeout",
			fmt.Sprintf("target %q did not reach source GTID within %s; fence released, primary %q still active",
				cur.Target, maxLagWait, cur.SourcePrimary), "failed_timeout")
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
	if err := r.setPlannedFailoverStatus(ctx, fg, next); err != nil {
		return 0, err
	}
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
		return 2 * time.Second, nil
	}
	fg.Status.ActiveSite = cur.Target
	fg.Status.LastFailover = &now
	fg.Status.LastFailoverTarget = cur.Target
	if cur.TargetGtidAtPromotion != "" {
		fg.Status.PromotionGtidExecuted = cur.TargetGtidAtPromotion
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
	if err := r.setPlannedFailoverStatus(ctx, fg, next); err != nil {
		return 0, err
	}

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
// metricResult labels the planned-failover counter — typically
// "failed_timeout" for LagTimeout and "failed_other" for parse or
// internal errors.
func (r *MysqlFailoverGroupReconciler) plannedFailoverRollback(ctx context.Context, fg *v1alpha1.MysqlFailoverGroup, nn types.NamespacedName, reason, msg, metricResult string) (time.Duration, error) {
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
			msg = fmt.Sprintf("%s (warning: source primary unfence failed: %v)", msg, err)
		}
		cancel()
	}

	return r.plannedFailoverFail(ctx, fg, reason, msg, metricResult)
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

	if err := r.setPlannedFailoverStatus(ctx, fg, &v1alpha1.PlannedFailoverStatus{
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
	}); err != nil {
		return 0, err
	}

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
func (r *MysqlFailoverGroupReconciler) setPlannedFailoverStatus(ctx context.Context, fg *v1alpha1.MysqlFailoverGroup, s *v1alpha1.PlannedFailoverStatus) error {
	patch := client.MergeFrom(fg.DeepCopy())
	fg.Status.PlannedFailover = s
	if err := r.Status().Patch(ctx, fg, patch); err != nil && !apierrors.IsNotFound(err) {
		log.FromContext(ctx).Error(err, "update planned-failover status", "fg", fg.Name)
		return err
	}
	return nil
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
		log.Log.Error(err, "planned-failover: parse sourceGtidAtFence failed; reporting transactionsLost=0", "gtid", sourceGtid)
		return 0
	}
	dst, err := internalmysql.ParseGTIDSet(targetGtid)
	if err != nil {
		log.Log.Error(err, "planned-failover: parse targetGtidAtPromotion failed; reporting transactionsLost=0", "gtid", targetGtid)
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
				lo := max(si.Start, di.Start)
				hi := min(si.End, di.End)
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
