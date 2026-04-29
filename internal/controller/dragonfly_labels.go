package controller

import (
	"context"
	"fmt"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/types"
	k8sretry "k8s.io/client-go/util/retry"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/log"

	v1alpha1 "github.com/shipstream/bloodraven/api/v1alpha1"
)

// syncDragonflyPodLabels enforces the steady-state Dragonfly pod
// labels for a failover group:
//
//   - master pod: dragonfly-role=master, dragonfly-traffic=enabled
//   - replica pod: dragonfly-role=replica, dragonfly-traffic=enabled
//
// The traffic label is the canonical "this pod serves writes" gate:
// the active Service selects on (role=master AND traffic=enabled), so
// removing the traffic label sheds the endpoint atomically without
// depending on the order of role-label flips. The planned-failover
// state machine drives the strip→takeover→restore sequence directly
// (see plannedFailoverPromotingDragonfly); this function only enforces
// steady state and is a no-op while the sequence is mid-flight on the
// source pod.
//
// The pod that should be master is determined by
// effectiveDragonflyMasterSite, which respects the planned-failover
// state machine's in-flight promotion target. Replicas are everything
// else.
func (r *MysqlFailoverGroupReconciler) syncDragonflyPodLabels(ctx context.Context, fg *v1alpha1.MysqlFailoverGroup) error {
	if !dragonflyEnabled(fg) {
		return nil
	}
	master := effectiveDragonflyMasterSite(fg)
	if master == "" {
		// Nothing to align until MySQL has chosen an active site.
		return nil
	}
	logger := log.FromContext(ctx)

	for _, site := range fg.Spec.Sites {
		desiredRole := "replica"
		if site.Name == master {
			desiredRole = "master"
		}
		// During PromotingDragonfly we have transiently stripped the
		// traffic label from the source pod between strip and the
		// final restore. Re-stamping it here would re-attach the old
		// master to the active Service mid-takeover, which is exactly
		// the bug the strip is preventing. Skip the source pod's
		// traffic label while we're in that window.
		stripActiveOnSource := plannedFailoverDragonflyStripActive(fg) && site.Name == plannedFailoverSourceSite(fg)

		pods := &corev1.PodList{}
		if err := r.List(ctx, pods,
			client.InNamespace(fg.Namespace),
			client.MatchingLabels{
				labelAppName:  dragonflyAppName,
				labelInstance: fg.Name,
				labelSite:     site.Name,
			},
		); err != nil {
			return fmt.Errorf("list dragonfly pods for site %s: %w", site.Name, err)
		}
		for i := range pods.Items {
			pod := &pods.Items[i]
			needRole := pod.Labels[labelDragonflyRole] != desiredRole
			needTraffic := !stripActiveOnSource && pod.Labels[labelDragonflyTraffic] != dragonflyTrafficEnabled
			if !needRole && !needTraffic {
				continue
			}
			podName := pod.Name
			podNamespace := pod.Namespace
			if err := k8sretry.RetryOnConflict(k8sretry.DefaultRetry, func() error {
				var fresh corev1.Pod
				if err := r.Get(ctx, types.NamespacedName{Name: podName, Namespace: podNamespace}, &fresh); err != nil {
					return err
				}
				if fresh.Labels == nil {
					fresh.Labels = map[string]string{}
				}
				if fresh.Labels[labelDragonflyRole] != desiredRole {
					fresh.Labels[labelDragonflyRole] = desiredRole
				}
				if !stripActiveOnSource && fresh.Labels[labelDragonflyTraffic] != dragonflyTrafficEnabled {
					fresh.Labels[labelDragonflyTraffic] = dragonflyTrafficEnabled
				}
				return r.Update(ctx, &fresh)
			}); err != nil {
				return fmt.Errorf("update dragonfly pod %s labels: %w", podName, err)
			}
			logger.Info("synced dragonfly pod labels", "pod", podName, "role", desiredRole)
		}
	}
	return nil
}

// setDragonflyTrafficOnSite either stamps or removes the traffic label
// on every Dragonfly pod for the named site. Used by the planned-
// failover state machine to atomically shed (or restore) the active-
// Service endpoint independent of the role label. Removing the label
// (set=false) is preferred over stamping a "disabled" value because
// the active-Service selector is an exists-and-equals check on
// "enabled" — a missing key sheds the endpoint without ambiguity.
func (r *MysqlFailoverGroupReconciler) setDragonflyTrafficOnSite(ctx context.Context, fg *v1alpha1.MysqlFailoverGroup, siteName string, set bool) error {
	return setDragonflyTrafficOnSite(ctx, r.Client, fg, siteName, set)
}

// setDragonflyRoleOnSite forces the role label on every Dragonfly pod
// for the named site to the given value. Used by the planned-failover
// PromotingDragonfly handler so role transitions land deterministically
// in-phase rather than waiting for the next reconcile sweep.
func (r *MysqlFailoverGroupReconciler) setDragonflyRoleOnSite(ctx context.Context, fg *v1alpha1.MysqlFailoverGroup, siteName, role string) error {
	return setDragonflyRoleOnSite(ctx, r.Client, fg, siteName, role)
}

// setDragonflyTrafficOnSite is the package-level helper. Both the
// reconciler and the DragonflyManager call this directly; previously
// each had its own copy that drifted. Callers pass a client.Client.
func setDragonflyTrafficOnSite(ctx context.Context, c client.Client, fg *v1alpha1.MysqlFailoverGroup, siteName string, set bool) error {
	pods, err := listDragonflyPodsForSite(ctx, c, fg, siteName)
	if err != nil {
		return err
	}
	logger := log.FromContext(ctx)
	for i := range pods {
		pod := &pods[i]
		current, has := pod.Labels[labelDragonflyTraffic]
		if set {
			if has && current == dragonflyTrafficEnabled {
				continue
			}
		} else if !has {
			continue
		}
		podName := pod.Name
		podNamespace := pod.Namespace
		if err := k8sretry.RetryOnConflict(k8sretry.DefaultRetry, func() error {
			var fresh corev1.Pod
			if err := c.Get(ctx, types.NamespacedName{Name: podName, Namespace: podNamespace}, &fresh); err != nil {
				return err
			}
			if fresh.Labels == nil {
				fresh.Labels = map[string]string{}
			}
			if set {
				fresh.Labels[labelDragonflyTraffic] = dragonflyTrafficEnabled
			} else {
				delete(fresh.Labels, labelDragonflyTraffic)
			}
			return c.Update(ctx, &fresh)
		}); err != nil {
			return fmt.Errorf("update dragonfly traffic label on pod %s: %w", podName, err)
		}
		logger.Info("dragonfly traffic label updated", "pod", podName, "set", set)
	}
	return nil
}

// setDragonflyRoleOnSite is the package-level helper.
func setDragonflyRoleOnSite(ctx context.Context, c client.Client, fg *v1alpha1.MysqlFailoverGroup, siteName, role string) error {
	pods, err := listDragonflyPodsForSite(ctx, c, fg, siteName)
	if err != nil {
		return err
	}
	logger := log.FromContext(ctx)
	for i := range pods {
		pod := &pods[i]
		if pod.Labels[labelDragonflyRole] == role {
			continue
		}
		podName := pod.Name
		podNamespace := pod.Namespace
		if err := k8sretry.RetryOnConflict(k8sretry.DefaultRetry, func() error {
			var fresh corev1.Pod
			if err := c.Get(ctx, types.NamespacedName{Name: podName, Namespace: podNamespace}, &fresh); err != nil {
				return err
			}
			if fresh.Labels == nil {
				fresh.Labels = map[string]string{}
			}
			fresh.Labels[labelDragonflyRole] = role
			return c.Update(ctx, &fresh)
		}); err != nil {
			return fmt.Errorf("update dragonfly role label on pod %s: %w", podName, err)
		}
		logger.Info("dragonfly role label updated", "pod", podName, "role", role)
	}
	return nil
}

// listDragonflyPodsForSite returns the Dragonfly pods belonging to the
// given site for a failover group. Centralized so the label helpers
// share the same selector and don't drift.
func listDragonflyPodsForSite(ctx context.Context, c client.Client, fg *v1alpha1.MysqlFailoverGroup, siteName string) ([]corev1.Pod, error) {
	pods := &corev1.PodList{}
	if err := c.List(ctx, pods,
		client.InNamespace(fg.Namespace),
		client.MatchingLabels{
			labelAppName:  dragonflyAppName,
			labelInstance: fg.Name,
			labelSite:     siteName,
		},
	); err != nil {
		return nil, fmt.Errorf("list dragonfly pods for site %s: %w", siteName, err)
	}
	return pods.Items, nil
}
