package controller

import (
	"context"
	"fmt"
	"time"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/types"
	k8sretry "k8s.io/client-go/util/retry"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/log"

	v1alpha1 "github.com/shipstream/bloodraven/api/v1alpha1"
)

// syncDragonflyPodLabels enforces the steady-state Dragonfly pod
// labels for a failover group:
//
//   - master pod: dragonfly-role=master, dragonfly-traffic=true
//   - replica pod: dragonfly-role=replica, dragonfly-traffic=true
//
// The traffic label is the canonical "this pod serves writes" gate:
// the active Service selects on (role=master AND traffic=true), so
// removing the traffic label sheds the endpoint atomically without
// depending on the order of role-label flips. The planned-failover
// state machine drives the strip→takeover→restore sequence directly
// (reconcilerDragonflyOps.Takeover in planned_failover_df.go, run by the
// engine's PromotingDragonfly step); this function only enforces
// steady state and is a no-op while the sequence is mid-flight on the
// source pod.
//
// The pod that should be master is determined by
// effectiveDragonflyMasterSite, which respects the planned-failover
// state machine's in-flight promotion target. Replicas are everything
// else.
//
// The sweep holds the group's Dragonfly gate and reads the group after
// acquiring it, so it never relabels from status that a promotion has just
// superseded. If another writer holds the gate it skips this pass.
//
// The returned duration is non-zero while the sweep holds a site's traffic
// for a snapshot load (dragonflySweepHoldsTraffic): the load ending
// produces no event, so the reconcile must come back to probe it.
func (r *MysqlFailoverGroupReconciler) syncDragonflyPodLabels(ctx context.Context, fg *v1alpha1.MysqlFailoverGroup) (time.Duration, error) {
	if !dragonflyEnabled(fg) {
		return 0, nil
	}
	nn := types.NamespacedName{Namespace: fg.Namespace, Name: fg.Name}
	release, ok := r.tryDragonflyGate(nn)
	if !ok {
		log.FromContext(ctx).V(1).Info("dragonfly label sweep skipped: gate busy", "fg", nn)
		return 0, nil
	}
	defer release()
	fresh, err := r.freshFailoverGroup(ctx, nn)
	if err != nil {
		if apierrors.IsNotFound(err) {
			return 0, nil
		}
		return 0, fmt.Errorf("re-read failover group for dragonfly label sweep: %w", err)
	}
	fg = fresh
	if !dragonflyEnabled(fg) {
		return 0, nil
	}
	master := effectiveDragonflyMasterSite(fg)
	if master == "" {
		// Nothing to align until MySQL has chosen an active site.
		return 0, nil
	}
	logger := log.FromContext(ctx)
	var requeue time.Duration

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
				labelAppName:       dragonflyAppName,
				labelInstance:      fg.Name,
				v1alpha1.SiteLabel: site.Name,
			},
		); err != nil {
			return 0, fmt.Errorf("list dragonfly pods for site %s: %w", site.Name, err)
		}
		holdTraffic, err := r.dragonflySweepHoldsTraffic(ctx, fg, site.Name, desiredRole, stripActiveOnSource, pods.Items)
		if err != nil {
			return 0, err
		}
		if holdTraffic {
			// A pod re-created from the template, or one another writer
			// gave traffic back, carries it; take it off before the role
			// below could make it an endpoint.
			if err := holdDragonflyTrafficForLoad(ctx, r.Client, fg, site.Name, r.now()); err != nil {
				return 0, err
			}
			requeue = dragonflyUpgradePollInterval
		}
		for i := range pods.Items {
			pod := &pods.Items[i]
			needRole := pod.Labels[v1alpha1.DragonflyRoleLabel] != desiredRole
			needTraffic := !stripActiveOnSource && !holdTraffic && pod.Labels[v1alpha1.DragonflyTrafficLabel] != dragonflyTrafficEnabled
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
				if fresh.Labels[v1alpha1.DragonflyRoleLabel] != desiredRole {
					fresh.Labels[v1alpha1.DragonflyRoleLabel] = desiredRole
				}
				if !stripActiveOnSource && !holdTraffic && fresh.Labels[v1alpha1.DragonflyTrafficLabel] != dragonflyTrafficEnabled {
					fresh.Labels[v1alpha1.DragonflyTrafficLabel] = dragonflyTrafficEnabled
				}
				return r.Update(ctx, &fresh)
			}); err != nil {
				return 0, fmt.Errorf("update dragonfly pod %s labels: %w", podName, err)
			}
			logger.Info("synced dragonfly pod labels", "pod", podName, "role", desiredRole)
		}
	}
	return requeue, nil
}

// dragonflySweepHoldsTraffic reports whether the sweep keeps site's
// Dragonfly pods out of traffic this pass: site is the active site of a
// Failed snapshot-restore run (dragonflyUpgradeTrafficGateSite), and the
// site does not answer as loaded while either a pod carries
// DragonflyLoadHoldAnnotation (the run ended, or an earlier sweep held it,
// while it loaded) or the sweep would make a pod serve (give it the
// traffic label, or the master role while it carries traffic). A run that
// ends while the pod it restarted on its snapshot is still loading must
// not have that pod put behind the Services before the load completes,
// by this sweep or by another writer (a promotion that restores its
// source's traffic): the pod's TCP readiness probe passes while it loads.
// Once the site answers as loaded the marks are removed; a mark on a site
// that is no longer gated is removed without a probe.
func (r *MysqlFailoverGroupReconciler) dragonflySweepHoldsTraffic(ctx context.Context, fg *v1alpha1.MysqlFailoverGroup, site, desiredRole string, stripActiveOnSource bool, pods []corev1.Pod) (bool, error) {
	marked := false
	serves := false
	for i := range pods {
		if _, ok := pods[i].Annotations[v1alpha1.DragonflyLoadHoldAnnotation]; ok {
			marked = true
		}
		traffic := pods[i].Labels[v1alpha1.DragonflyTrafficLabel] == dragonflyTrafficEnabled
		if !traffic || (desiredRole == "master" && pods[i].Labels[v1alpha1.DragonflyRoleLabel] != desiredRole) {
			serves = true
		}
	}
	if site != dragonflyUpgradeTrafficGateSite(fg) {
		if marked {
			return false, releaseDragonflyLoadHold(ctx, r.Client, fg, site)
		}
		return false, nil
	}
	if stripActiveOnSource || (!serves && !marked) {
		return false, nil
	}
	loaded, state, err := r.dragonflySiteLoaded(ctx, fg, site)
	if err != nil {
		return false, err
	}
	if !loaded {
		log.FromContext(ctx).Info("dragonfly traffic held until the site loads after a failed snapshot-restore upgrade",
			"fg", fg.Namespace+"/"+fg.Name, "site", site, "state", state)
		return true, nil
	}
	if marked {
		return false, releaseDragonflyLoadHold(ctx, r.Client, fg, site)
	}
	return false, nil
}

// holdDragonflyTrafficForLoad removes the traffic label from site's
// Dragonfly pods and marks them with DragonflyLoadHoldAnnotation, so the
// label sweep keeps re-checking them until the site has loaded.
func holdDragonflyTrafficForLoad(ctx context.Context, c client.Client, fg *v1alpha1.MysqlFailoverGroup, site string, now time.Time) error {
	return updateDragonflySitePods(ctx, c, fg, site, func(p *corev1.Pod) bool {
		_, traffic := p.Labels[v1alpha1.DragonflyTrafficLabel]
		_, marked := p.Annotations[v1alpha1.DragonflyLoadHoldAnnotation]
		if !traffic && marked {
			return false
		}
		delete(p.Labels, v1alpha1.DragonflyTrafficLabel)
		if !marked {
			if p.Annotations == nil {
				p.Annotations = map[string]string{}
			}
			p.Annotations[v1alpha1.DragonflyLoadHoldAnnotation] = now.UTC().Format(time.RFC3339)
		}
		return true
	})
}

// releaseDragonflyLoadHold removes DragonflyLoadHoldAnnotation from site's
// Dragonfly pods. It does not restore traffic; the sweep does.
func releaseDragonflyLoadHold(ctx context.Context, c client.Client, fg *v1alpha1.MysqlFailoverGroup, site string) error {
	return updateDragonflySitePods(ctx, c, fg, site, func(p *corev1.Pod) bool {
		if _, ok := p.Annotations[v1alpha1.DragonflyLoadHoldAnnotation]; !ok {
			return false
		}
		delete(p.Annotations, v1alpha1.DragonflyLoadHoldAnnotation)
		return true
	})
}

// updateDragonflySitePods applies mutate to each of site's Dragonfly pods
// and writes the ones it changed, retrying on a conflict.
func updateDragonflySitePods(ctx context.Context, c client.Client, fg *v1alpha1.MysqlFailoverGroup, site string, mutate func(*corev1.Pod) bool) error {
	pods, err := listDragonflyPodsForSite(ctx, c, fg, site)
	if err != nil {
		return err
	}
	for i := range pods {
		key := types.NamespacedName{Name: pods[i].Name, Namespace: pods[i].Namespace}
		if err := k8sretry.RetryOnConflict(k8sretry.DefaultRetry, func() error {
			var fresh corev1.Pod
			if err := c.Get(ctx, key, &fresh); err != nil {
				return err
			}
			if fresh.Labels == nil {
				fresh.Labels = map[string]string{}
			}
			if !mutate(&fresh) {
				return nil
			}
			return c.Update(ctx, &fresh)
		}); err != nil && !apierrors.IsNotFound(err) {
			return fmt.Errorf("update dragonfly pod %s: %w", key.Name, err)
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
// "true" — a missing key sheds the endpoint without ambiguity.
func (r *MysqlFailoverGroupReconciler) setDragonflyTrafficOnSite(ctx context.Context, fg *v1alpha1.MysqlFailoverGroup, siteName string, set bool) error {
	return setDragonflyTrafficOnSite(ctx, r.Client, fg, siteName, set)
}

// setDragonflyRoleOnSite forces the role label on every Dragonfly pod
// for the named site to the given value. Used by the planned-failover
// takeover (reconcilerDragonflyOps in planned_failover_df.go) so role
// transitions land deterministically in-phase rather than waiting for the
// next reconcile sweep.
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
		current, has := pod.Labels[v1alpha1.DragonflyTrafficLabel]
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
				fresh.Labels[v1alpha1.DragonflyTrafficLabel] = dragonflyTrafficEnabled
			} else {
				delete(fresh.Labels, v1alpha1.DragonflyTrafficLabel)
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
		if pod.Labels[v1alpha1.DragonflyRoleLabel] == role {
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
			fresh.Labels[v1alpha1.DragonflyRoleLabel] = role
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
			labelAppName:       dragonflyAppName,
			labelInstance:      fg.Name,
			v1alpha1.SiteLabel: siteName,
		},
	); err != nil {
		return nil, fmt.Errorf("list dragonfly pods for site %s: %w", siteName, err)
	}
	return pods.Items, nil
}
