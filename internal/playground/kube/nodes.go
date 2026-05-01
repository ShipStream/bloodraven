package kube

import (
	"context"
	"encoding/json"
	"fmt"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
)

// GetNode fetches a Node by name.
func (c *Client) GetNode(ctx context.Context, name string) (*corev1.Node, error) {
	return c.Kubernetes.CoreV1().Nodes().Get(ctx, name, metav1.GetOptions{})
}

// AddNodeLabels merges labels onto a node via JSON merge patch. Pre-
// existing keys are overwritten; keys not in the map are untouched.
func (c *Client) AddNodeLabels(ctx context.Context, name string, labels map[string]string) error {
	if len(labels) == 0 {
		return nil
	}
	patch := map[string]any{
		"metadata": map[string]any{
			"labels": labels,
		},
	}
	body, err := json.Marshal(patch)
	if err != nil {
		return fmt.Errorf("marshal label patch: %w", err)
	}
	_, err = c.Kubernetes.CoreV1().Nodes().Patch(ctx, name, types.MergePatchType, body, metav1.PatchOptions{})
	return err
}

// RemoveNodeLabels removes the named label keys from a node via JSON
// merge patch (RFC 7396 — null values delete keys).
func (c *Client) RemoveNodeLabels(ctx context.Context, name string, keys []string) error {
	if len(keys) == 0 {
		return nil
	}
	labels := make(map[string]any, len(keys))
	for _, k := range keys {
		labels[k] = nil
	}
	patch := map[string]any{
		"metadata": map[string]any{
			"labels": labels,
		},
	}
	body, err := json.Marshal(patch)
	if err != nil {
		return fmt.Errorf("marshal label-remove patch: %w", err)
	}
	_, err = c.Kubernetes.CoreV1().Nodes().Patch(ctx, name, types.MergePatchType, body, metav1.PatchOptions{})
	return err
}

// NodeHasTaint returns true iff the node has a taint with the given key.
func NodeHasTaint(node *corev1.Node, key string) bool {
	for _, t := range node.Spec.Taints {
		if t.Key == key {
			return true
		}
	}
	return false
}

// ListNodes returns every node in the cluster.
func (c *Client) ListNodes(ctx context.Context) (*corev1.NodeList, error) {
	return c.Kubernetes.CoreV1().Nodes().List(ctx, metav1.ListOptions{})
}

// RemoveNodeTaint removes every taint with the given key from a node.
// Idempotent: returns nil if the node has no taint with that key. Used
// by scenarios that need to scrub the operator-applied
// `db-readonly-<fg>` taint so a fresh PVC can be provisioned on a node
// the local-path-provisioner helper pod doesn't tolerate.
func (c *Client) RemoveNodeTaint(ctx context.Context, name, key string) error {
	node, err := c.GetNode(ctx, name)
	if err != nil {
		return fmt.Errorf("get node %s: %w", name, err)
	}
	kept := node.Spec.Taints[:0]
	removed := false
	for _, t := range node.Spec.Taints {
		if t.Key == key {
			removed = true
			continue
		}
		kept = append(kept, t)
	}
	if !removed {
		return nil
	}
	patchTaints := make([]any, 0, len(kept))
	for _, t := range kept {
		patchTaints = append(patchTaints, map[string]any{
			"key":    t.Key,
			"value":  t.Value,
			"effect": string(t.Effect),
		})
	}
	patch := map[string]any{
		"spec": map[string]any{
			"taints": patchTaints,
		},
	}
	body, err := json.Marshal(patch)
	if err != nil {
		return fmt.Errorf("marshal taint-remove patch: %w", err)
	}
	_, err = c.Kubernetes.CoreV1().Nodes().Patch(ctx, name, types.MergePatchType, body, metav1.PatchOptions{})
	return err
}

// CreatePod applies a Pod manifest to the namespace.
func (c *Client) CreatePod(ctx context.Context, namespace string, pod *corev1.Pod) error {
	if namespace == "" {
		namespace = PlaygroundNamespace
	}
	_, err := c.Kubernetes.CoreV1().Pods(namespace).Create(ctx, pod, metav1.CreateOptions{})
	return err
}

// DeletePodByName deletes a pod by name. Returns nil if the pod is
// already gone (idempotent).
func (c *Client) DeletePodByName(ctx context.Context, namespace, name string) error {
	if namespace == "" {
		namespace = PlaygroundNamespace
	}
	err := c.Kubernetes.CoreV1().Pods(namespace).Delete(ctx, name, metav1.DeleteOptions{})
	if apierrors.IsNotFound(err) {
		return nil
	}
	return err
}

// GetPod fetches a Pod by name.
func (c *Client) GetPod(ctx context.Context, namespace, name string) (*corev1.Pod, error) {
	if namespace == "" {
		namespace = PlaygroundNamespace
	}
	return c.Kubernetes.CoreV1().Pods(namespace).Get(ctx, name, metav1.GetOptions{})
}
