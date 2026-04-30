package kube

import (
	"context"
	"fmt"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
)

// DragonflyStatefulSetName mirrors internal/controller's
// dragonflyStatefulSetName: <fg>-dragonfly-<site>. The chaos runner
// scales / inspects this StatefulSet by name.
func DragonflyStatefulSetName(fg, site string) string {
	return fmt.Sprintf("%s-dragonfly-%s", fg, site)
}

// DragonflySiteServiceName is the per-site Service used for replication
// wiring and direct site-local dials. Same string as the StatefulSet.
func DragonflySiteServiceName(fg, site string) string {
	return fmt.Sprintf("%s-dragonfly-%s", fg, site)
}

// DragonflyActiveServiceName is the cross-site write Service whose
// selector tracks (role=master AND traffic=enabled). Application
// clients connect through this.
func DragonflyActiveServiceName(fg string) string {
	return fmt.Sprintf("%s-dragonfly", fg)
}

// DragonflyPodSelector builds a label selector that targets a single
// site's Dragonfly pod. Mirrors the StatefulSet pod template labels.
func DragonflyPodSelector(fg, site string) string {
	return fmt.Sprintf("app.kubernetes.io/name=dragonfly,app.kubernetes.io/instance=%s,shipstream.io/site=%s", fg, site)
}

// ListDragonflyPods returns Dragonfly pods for one site.
func (c *Client) ListDragonflyPods(ctx context.Context, namespace, fg, site string) (*corev1.PodList, error) {
	if namespace == "" {
		namespace = PlaygroundNamespace
	}
	return c.Kubernetes.CoreV1().Pods(namespace).List(ctx, metav1.ListOptions{
		LabelSelector: DragonflyPodSelector(fg, site),
	})
}

// GetSiteDragonflyPod returns the single Dragonfly pod for a site.
// Errors when zero or more than one match — the operator runs one
// replica per site by default and the chaos runner relies on that
// invariant for stable port-forward targets.
func (c *Client) GetSiteDragonflyPod(ctx context.Context, namespace, fg, site string) (*corev1.Pod, error) {
	pods, err := c.ListDragonflyPods(ctx, namespace, fg, site)
	if err != nil {
		return nil, err
	}
	if len(pods.Items) == 0 {
		return nil, fmt.Errorf("no Dragonfly pod found for site %q (fg=%s)", site, fg)
	}
	if len(pods.Items) > 1 {
		return nil, fmt.Errorf("expected 1 Dragonfly pod for site %q, found %d", site, len(pods.Items))
	}
	return &pods.Items[0], nil
}

// DeleteSiteDragonflyPod force-deletes the single Dragonfly pod for a
// site. The StatefulSet respawns it. Used by the dragonfly-master-kill
// chaos scenario.
func (c *Client) DeleteSiteDragonflyPod(ctx context.Context, namespace, fg, site string, gracePeriod *int64) error {
	if namespace == "" {
		namespace = PlaygroundNamespace
	}
	pod, err := c.GetSiteDragonflyPod(ctx, namespace, fg, site)
	if err != nil {
		return err
	}
	opts := metav1.DeleteOptions{}
	if gracePeriod != nil {
		opts.GracePeriodSeconds = gracePeriod
	}
	return c.Kubernetes.CoreV1().Pods(namespace).Delete(ctx, pod.Name, opts)
}

// ScaleDragonflyStatefulSet patches a Dragonfly StatefulSet's replica
// count. Used to hold a Dragonfly site offline past the brief
// StatefulSet-respawn window for emergency-failover scenarios.
func (c *Client) ScaleDragonflyStatefulSet(ctx context.Context, namespace, fg, site string, replicas int32) error {
	if namespace == "" {
		namespace = PlaygroundNamespace
	}
	name := DragonflyStatefulSetName(fg, site)
	body := fmt.Sprintf(`{"spec":{"replicas":%d}}`, replicas)
	_, err := c.Kubernetes.AppsV1().StatefulSets(namespace).Patch(
		ctx, name, types.StrategicMergePatchType, []byte(body), metav1.PatchOptions{},
	)
	return err
}

// DragonflyActiveServiceEndpointPods returns the names of Pods
// currently selected by the active Dragonfly Service. A planned
// failover should observably converge this set from {source} to
// {target} once the new master's role+traffic labels land.
//
// Reads via the Endpoints API, which kube-proxy populates from the
// Service selector + Pod labels. An empty result means kube-proxy has
// no backends — either between transitions or with an actual outage.
func (c *Client) DragonflyActiveServiceEndpointPods(ctx context.Context, namespace, fg string) ([]string, error) {
	if namespace == "" {
		namespace = PlaygroundNamespace
	}
	name := DragonflyActiveServiceName(fg)
	ep, err := c.Kubernetes.CoreV1().Endpoints(namespace).Get(ctx, name, metav1.GetOptions{})
	if err != nil {
		if apierrors.IsNotFound(err) {
			return nil, nil
		}
		return nil, fmt.Errorf("get endpoints %s/%s: %w", namespace, name, err)
	}
	var out []string
	for _, sub := range ep.Subsets {
		for _, addr := range sub.Addresses {
			if addr.TargetRef != nil && addr.TargetRef.Kind == "Pod" {
				out = append(out, addr.TargetRef.Name)
			}
		}
	}
	return out, nil
}
