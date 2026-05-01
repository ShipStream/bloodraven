package kube

import (
	"context"
	"fmt"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	networkingv1 "k8s.io/api/networking/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
)

// MysqlDeploymentName returns the name of the MySQL deployment for a
// given failover group + site (mirrors internal/controller naming).
func MysqlDeploymentName(fg, site string) string {
	return fmt.Sprintf("mysql-%s-%s", fg, site)
}

// MysqlPodSelector builds a label selector that targets a single
// site's MySQL pod (deployment-managed; one replica per site).
func MysqlPodSelector(fg, site string) string {
	return fmt.Sprintf("app.kubernetes.io/name=mysql,app.kubernetes.io/instance=%s,shipstream.io/site=%s", fg, site)
}

// ListMysqlPods returns the MySQL pods for a single site.
func (c *Client) ListMysqlPods(ctx context.Context, namespace, fg, site string) (*corev1.PodList, error) {
	if namespace == "" {
		namespace = PlaygroundNamespace
	}
	return c.Kubernetes.CoreV1().Pods(namespace).List(ctx, metav1.ListOptions{
		LabelSelector: MysqlPodSelector(fg, site),
	})
}

// GetSiteMysqlPod returns the single MySQL pod for a site, or an
// error if zero or more than one pod matches.
func (c *Client) GetSiteMysqlPod(ctx context.Context, namespace, fg, site string) (*corev1.Pod, error) {
	pods, err := c.ListMysqlPods(ctx, namespace, fg, site)
	if err != nil {
		return nil, err
	}
	if len(pods.Items) == 0 {
		return nil, fmt.Errorf("no MySQL pod found for site %q (fg=%s)", site, fg)
	}
	if len(pods.Items) > 1 {
		return nil, fmt.Errorf("expected 1 MySQL pod for site %q, found %d", site, len(pods.Items))
	}
	return &pods.Items[0], nil
}

// DeleteSitePod deletes the MySQL pod for a site. The Deployment
// controller respawns the pod within a few seconds.
func (c *Client) DeleteSitePod(ctx context.Context, namespace, fg, site string, gracePeriod *int64) error {
	if namespace == "" {
		namespace = PlaygroundNamespace
	}
	pod, err := c.GetSiteMysqlPod(ctx, namespace, fg, site)
	if err != nil {
		return err
	}
	opts := metav1.DeleteOptions{}
	if gracePeriod != nil {
		opts.GracePeriodSeconds = gracePeriod
	}
	return c.Kubernetes.CoreV1().Pods(namespace).Delete(ctx, pod.Name, opts)
}

// ScaleDeployment patches the deployment's replica count. Used to
// hold a site down past the brief Deployment-respawn window: scale to
// 0 simulates a sustained outage that pod-delete cannot.
func (c *Client) ScaleDeployment(ctx context.Context, namespace, name string, replicas int32) error {
	if namespace == "" {
		namespace = PlaygroundNamespace
	}
	body := []byte(fmt.Sprintf(`{"spec":{"replicas":%d}}`, replicas))
	_, err := c.Kubernetes.AppsV1().Deployments(namespace).Patch(
		ctx, name, types.StrategicMergePatchType, body, metav1.PatchOptions{},
	)
	return err
}

// GetDeployment fetches a Deployment by name.
func (c *Client) GetDeployment(ctx context.Context, namespace, name string) (*appsv1.Deployment, error) {
	if namespace == "" {
		namespace = PlaygroundNamespace
	}
	return c.Kubernetes.AppsV1().Deployments(namespace).Get(ctx, name, metav1.GetOptions{})
}

// ApplyDenyAllNetworkPolicy creates a NetworkPolicy that blocks all
// ingress and egress for the MySQL pod at a site. This is the
// pod-network-level partition that kube-proxy DNAT cannot bypass —
// the host-netns iptables technique in early chaos.sh did not work.
//
// The NP is labeled app=chaos-partition so RemoveAll can sweep them.
func (c *Client) ApplyDenyAllNetworkPolicy(ctx context.Context, namespace, fg, site string) error {
	if namespace == "" {
		namespace = PlaygroundNamespace
	}
	name := fmt.Sprintf("chaos-partition-%s", site)
	np := &networkingv1.NetworkPolicy{
		ObjectMeta: metav1.ObjectMeta{
			Name:      name,
			Namespace: namespace,
			Labels:    map[string]string{"app": "chaos-partition"},
		},
		Spec: networkingv1.NetworkPolicySpec{
			PodSelector: metav1.LabelSelector{
				MatchLabels: map[string]string{
					"app.kubernetes.io/name":     "mysql",
					"app.kubernetes.io/instance": fg,
					"shipstream.io/site":         site,
				},
			},
			PolicyTypes: []networkingv1.PolicyType{
				networkingv1.PolicyTypeIngress,
				networkingv1.PolicyTypeEgress,
			},
			Ingress: []networkingv1.NetworkPolicyIngressRule{},
			Egress:  []networkingv1.NetworkPolicyEgressRule{},
		},
	}
	_, err := c.Kubernetes.NetworkingV1().NetworkPolicies(namespace).Create(ctx, np, metav1.CreateOptions{})
	if apierrors.IsAlreadyExists(err) {
		// Already partitioned — treat as idempotent.
		return nil
	}
	return err
}

// RemoveDenyAllNetworkPolicy deletes the NetworkPolicy created by
// ApplyDenyAllNetworkPolicy for a site. Idempotent.
func (c *Client) RemoveDenyAllNetworkPolicy(ctx context.Context, namespace, site string) error {
	if namespace == "" {
		namespace = PlaygroundNamespace
	}
	name := fmt.Sprintf("chaos-partition-%s", site)
	err := c.Kubernetes.NetworkingV1().NetworkPolicies(namespace).Delete(ctx, name, metav1.DeleteOptions{})
	if apierrors.IsNotFound(err) {
		return nil
	}
	return err
}

// RemoveAllChaosNetworkPolicies sweeps every NetworkPolicy with the
// app=chaos-partition label. Used by GlobalRecover to ensure no
// partition leaks across scenarios.
func (c *Client) RemoveAllChaosNetworkPolicies(ctx context.Context, namespace string) error {
	if namespace == "" {
		namespace = PlaygroundNamespace
	}
	return c.Kubernetes.NetworkingV1().NetworkPolicies(namespace).DeleteCollection(
		ctx,
		metav1.DeleteOptions{},
		metav1.ListOptions{LabelSelector: "app=chaos-partition"},
	)
}

// AddEphemeralContainer appends an ephemeral container to a pod via
// the ephemeralcontainers subresource. Ephemeral containers cannot be
// removed once added; the caller is responsible for picking a unique
// Name per invocation if the same pod may be targeted multiple times.
//
// Mirrors the wire-level path that `kubectl debug` uses: GET the pod,
// append to spec.ephemeralContainers, PUT to the subresource.
func (c *Client) AddEphemeralContainer(ctx context.Context, namespace, podName string, ec corev1.EphemeralContainer) error {
	if namespace == "" {
		namespace = PlaygroundNamespace
	}
	pod, err := c.Kubernetes.CoreV1().Pods(namespace).Get(ctx, podName, metav1.GetOptions{})
	if err != nil {
		return fmt.Errorf("get pod %s/%s: %w", namespace, podName, err)
	}
	pod.Spec.EphemeralContainers = append(pod.Spec.EphemeralContainers, ec)
	if _, err := c.Kubernetes.CoreV1().Pods(namespace).UpdateEphemeralContainers(ctx, podName, pod, metav1.UpdateOptions{}); err != nil {
		return fmt.Errorf("update ephemeralcontainers on %s/%s: %w", namespace, podName, err)
	}
	return nil
}

// MysqlPVCName returns the PVC name the operator's reconcilePVC creates
// for a site's data directory. Mirrors `resourceName(fg, site) + "-data"`
// in internal/controller/reconciler.go.
func MysqlPVCName(fg, site string) string {
	return MysqlDeploymentName(fg, site) + "-data"
}

// DeletePVC deletes a PVC by name. Returns nil if the PVC is already
// gone (idempotent). The operator-owned MFG controller recreates the
// PVC on its next reconcile.
func (c *Client) DeletePVC(ctx context.Context, namespace, name string) error {
	if namespace == "" {
		namespace = PlaygroundNamespace
	}
	err := c.Kubernetes.CoreV1().PersistentVolumeClaims(namespace).Delete(ctx, name, metav1.DeleteOptions{})
	if apierrors.IsNotFound(err) {
		return nil
	}
	return err
}

// PVCExists reports whether the named PVC is present in the namespace.
// Used by chaos cleanup to wait for a PVC to actually disappear after
// the deployment has been scaled to 0 (terminating pods can keep a
// volume bound for a few seconds).
func (c *Client) PVCExists(ctx context.Context, namespace, name string) (bool, error) {
	if namespace == "" {
		namespace = PlaygroundNamespace
	}
	_, err := c.Kubernetes.CoreV1().PersistentVolumeClaims(namespace).Get(ctx, name, metav1.GetOptions{})
	if apierrors.IsNotFound(err) {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	return true, nil
}

// PVCUID returns the metadata.uid of the named PVC, or "" if it does
// not exist. Used by chaos primitives that need to detect "the PVC has
// been replaced with a fresh one" — the operator's reconcile re-creates
// the PVC almost immediately after a delete, so a race-free check needs
// to compare UIDs rather than wait for absence.
func (c *Client) PVCUID(ctx context.Context, namespace, name string) (string, error) {
	if namespace == "" {
		namespace = PlaygroundNamespace
	}
	pvc, err := c.Kubernetes.CoreV1().PersistentVolumeClaims(namespace).Get(ctx, name, metav1.GetOptions{})
	if apierrors.IsNotFound(err) {
		return "", nil
	}
	if err != nil {
		return "", err
	}
	return string(pvc.UID), nil
}

// PodCount returns the number of pods matching a label selector in a
// namespace. Used by chaos primitives that need to wait for a pod to
// actually disappear after a scale-to-0 (the pod terminates async).
func (c *Client) PodCount(ctx context.Context, namespace, labelSelector string) (int, error) {
	if namespace == "" {
		namespace = PlaygroundNamespace
	}
	pods, err := c.Kubernetes.CoreV1().Pods(namespace).List(ctx, metav1.ListOptions{
		LabelSelector: labelSelector,
	})
	if err != nil {
		return 0, err
	}
	return len(pods.Items), nil
}

// SidecarRestartCount returns the restart count for the sidecar
// container in the named pod, or an error if the pod or container is
// missing.
func (c *Client) SidecarRestartCount(ctx context.Context, namespace, podName string) (int32, error) {
	if namespace == "" {
		namespace = PlaygroundNamespace
	}
	pod, err := c.Kubernetes.CoreV1().Pods(namespace).Get(ctx, podName, metav1.GetOptions{})
	if err != nil {
		return 0, fmt.Errorf("get pod %s/%s: %w", namespace, podName, err)
	}
	for _, cs := range pod.Status.ContainerStatuses {
		if cs.Name == "sidecar" {
			return cs.RestartCount, nil
		}
	}
	return 0, fmt.Errorf("no sidecar container status on pod %s/%s", namespace, podName)
}

// MysqlContainerRestartCount returns the restartCount for the mysql
// container in the named pod, or an error if the pod or container is
// missing. Counterpart to SidecarRestartCount used by scenarios that
// kill mysqld and want to confirm the container actually restarted.
func (c *Client) MysqlContainerRestartCount(ctx context.Context, namespace, podName string) (int32, error) {
	if namespace == "" {
		namespace = PlaygroundNamespace
	}
	pod, err := c.Kubernetes.CoreV1().Pods(namespace).Get(ctx, podName, metav1.GetOptions{})
	if err != nil {
		return 0, fmt.Errorf("get pod %s/%s: %w", namespace, podName, err)
	}
	for _, cs := range pod.Status.ContainerStatuses {
		if cs.Name == "mysql" {
			return cs.RestartCount, nil
		}
	}
	return 0, fmt.Errorf("no mysql container status on pod %s/%s", namespace, podName)
}

// PodLogTailLines reads the most recent N lines from a pod's
// container. Used by forensic capture; live tailing lives in the
// playground/logs package.
func (c *Client) PodLogTailLines(ctx context.Context, namespace, pod, container string, tail int64) ([]byte, error) {
	if namespace == "" {
		namespace = PlaygroundNamespace
	}
	opts := &corev1.PodLogOptions{
		Container: container,
		TailLines: &tail,
	}
	req := c.Kubernetes.CoreV1().Pods(namespace).GetLogs(pod, opts)
	stream, err := req.Stream(ctx)
	if err != nil {
		return nil, fmt.Errorf("open log stream: %w", err)
	}
	defer stream.Close()
	buf := make([]byte, 0, 64*1024)
	chunk := make([]byte, 8*1024)
	for {
		n, rerr := stream.Read(chunk)
		if n > 0 {
			buf = append(buf, chunk[:n]...)
		}
		if rerr != nil {
			break
		}
	}
	return buf, nil
}
