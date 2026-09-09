package controller

import (
	"context"
	"fmt"
	"time"

	v1alpha1 "github.com/shipstream/bloodraven/api/v1alpha1"
	"github.com/shipstream/bloodraven/internal/metrics"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	k8sretry "k8s.io/client-go/util/retry"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

func (r *MysqlFailoverGroupReconciler) stampDeploymentHold(ctx context.Context, fg *v1alpha1.MysqlFailoverGroup, req PlannedFailoverRequest, hold *DeploymentLeaseView) (time.Duration, error) {
	now := time.Now()
	if m := r.deploymentLeaseManager(); m != nil {
		now = m.now()
	}
	_, reason, validationErr := validatePlannedFailoverRequest(fg, req, now, false, hold)
	if validationErr == nil {
		return time.Second, nil
	}
	initial := fg.Status.PlannedFailover == nil || fg.Status.PlannedFailover.Reason != reason || fg.Status.PlannedFailover.Phase != v1alpha1.PlannedFailoverPhaseDeferred
	start := metav1.NewTime(now)
	retry := metav1.NewTime(hold.ExpiresAt)
	next := &v1alpha1.PlannedFailoverStatus{Phase: v1alpha1.PlannedFailoverPhaseDeferred, Reason: reason, Message: validationErr.Error(), Target: req.Site, SourcePrimary: fg.Status.ActiveSite, StartTime: &start, RetryAfter: &retry,
		MaxLagWait: &metav1.Duration{Duration: effectiveMaxLagWait(fg, req)}, DrainTimeout: &metav1.Duration{Duration: effectiveDrainTimeout(fg)}}
	if cur := fg.Status.PlannedFailover; cur != nil && cur.StartTime != nil {
		next.StartTime = cur.StartTime
	}
	// Normally the annotation is still present. Restore it when validation of
	// an already-accepted request discovers a durable hold after restart.
	if fg.Annotations[PlannedFailoverAnnotation] == "" {
		if err := k8sretry.RetryOnConflict(k8sretry.DefaultRetry, func() error {
			fresh := &v1alpha1.MysqlFailoverGroup{}
			if err := r.Get(ctx, client.ObjectKeyFromObject(fg), fresh); err != nil {
				return err
			}
			patch := client.MergeFrom(fresh.DeepCopy())
			if fresh.Annotations == nil {
				fresh.Annotations = make(map[string]string)
			}
			if fresh.Annotations[PlannedFailoverAnnotation] == "" {
				fresh.Annotations[PlannedFailoverAnnotation] = fmt.Sprintf("%s:maxLagWait=%s", req.Site, effectiveMaxLagWait(fg, req))
			}
			return r.Patch(ctx, fresh, patch)
		}); err != nil {
			return 0, err
		}
	}
	if err := r.setPlannedFailoverStatus(ctx, fg, next); err != nil {
		return 0, err
	}
	if r.Runner != nil {
		r.Runner.SetPlannedFailoverActive(client.ObjectKeyFromObject(fg), false)
	}
	if initial {
		metrics.PlannedFailoversDeferredTotal.WithLabelValues(fg.Name, "DeploymentHold").Inc()
		if r.Recorder != nil {
			r.Recorder.Eventf(fg, corev1.EventTypeNormal, "PlannedFailoverDeferred", "%s", next.Message)
		}
	}
	return max(time.Second, hold.ExpiresAt.Sub(now)+time.Nanosecond), nil
}
