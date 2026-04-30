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
