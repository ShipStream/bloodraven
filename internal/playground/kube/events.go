package kube

import (
	"context"
	"sort"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// RecentEvents returns the most recent events in the given namespace,
// sorted by lastTimestamp ascending (oldest first), capped at limit.
func (c *Client) RecentEvents(ctx context.Context, namespace string, limit int) ([]corev1.Event, error) {
	if namespace == "" {
		namespace = PlaygroundNamespace
	}
	list, err := c.Kubernetes.CoreV1().Events(namespace).List(ctx, metav1.ListOptions{})
	if err != nil {
		return nil, err
	}
	items := list.Items
	sort.SliceStable(items, func(i, j int) bool {
		return items[i].LastTimestamp.Before(&items[j].LastTimestamp)
	})
	if limit > 0 && len(items) > limit {
		items = items[len(items)-limit:]
	}
	return items, nil
}
