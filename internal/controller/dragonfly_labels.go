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

// syncDragonflyPodLabels patches dragonfly-role=master|replica onto the
// per-site Dragonfly pods so the active app-facing Service follows the
// promotion. No-op when spec.dragonfly is disabled.
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
		desired := "replica"
		if site.Name == master {
			desired = "master"
		}
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
			if pod.Labels[labelDragonflyRole] == desired {
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
				fresh.Labels[labelDragonflyRole] = desired
				return r.Update(ctx, &fresh)
			}); err != nil {
				return fmt.Errorf("update dragonfly pod %s label: %w", podName, err)
			}
			logger.Info("updated dragonfly pod label", "pod", podName, "role", desired)
		}
	}
	return nil
}
