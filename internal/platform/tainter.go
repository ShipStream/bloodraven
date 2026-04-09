package platform

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/kubernetes"
	k8sretry "k8s.io/client-go/util/retry"
)

const (
	TaintKey   = "shipstream.io/db-readonly"
	TaintValue = "true"
)

// NodeTainter manages node taints for failover group site failover.
// The selector parameter is a Kubernetes label selector string, e.g.
// "shipstream.io/failover-group=orders,shipstream.io/site=iad".
type NodeTainter interface {
	SetTaint(ctx context.Context, selector string, taint bool) error
}

type nodeTainter struct {
	client kubernetes.Interface
	logger *slog.Logger
}

func NewNodeTainter(client kubernetes.Interface, logger *slog.Logger) NodeTainter {
	return &nodeTainter{client: client, logger: logger}
}

func (t *nodeTainter) SetTaint(ctx context.Context, selector string, taint bool) error {
	nodes, err := t.client.CoreV1().Nodes().List(ctx, metav1.ListOptions{
		LabelSelector: selector,
	})
	if err != nil {
		return fmt.Errorf("list nodes for selector %s: %w", selector, err)
	}

	var errs []error
	for _, node := range nodes.Items {
		node := node
		if err := k8sretry.RetryOnConflict(k8sretry.DefaultRetry, func() error {
			// Re-fetch the node to get latest resource version.
			fresh, err := t.client.CoreV1().Nodes().Get(ctx, node.Name, metav1.GetOptions{})
			if err != nil {
				return fmt.Errorf("get node %s: %w", node.Name, err)
			}
			return t.patchNodeTaint(ctx, *fresh, taint)
		}); err != nil {
			errs = append(errs, err)
		}
	}
	return errors.Join(errs...)
}

func (t *nodeTainter) patchNodeTaint(ctx context.Context, node corev1.Node, apply bool) error {
	hasTaint, _ := findTaint(node.Spec.Taints)

	if apply && hasTaint {
		return nil
	}
	if !apply && !hasTaint {
		return nil
	}

	newTaints := make([]corev1.Taint, 0, len(node.Spec.Taints)+1)
	for _, taint := range node.Spec.Taints {
		if taint.Key != TaintKey {
			newTaints = append(newTaints, taint)
		}
	}
	if apply {
		newTaints = append(newTaints, corev1.Taint{
			Key:    TaintKey,
			Value:  TaintValue,
			Effect: corev1.TaintEffectNoExecute,
		})
		t.logger.Info("applying taint", "node", node.Name)
	} else {
		t.logger.Info("removing taint", "node", node.Name)
	}

	patch := map[string]interface{}{
		"spec": map[string]interface{}{
			"taints": newTaints,
		},
	}
	patchData, err := json.Marshal(patch)
	if err != nil {
		return fmt.Errorf("marshal patch for %s: %w", node.Name, err)
	}

	_, err = t.client.CoreV1().Nodes().Patch(ctx, node.Name, types.StrategicMergePatchType, patchData, metav1.PatchOptions{})
	if err != nil {
		return fmt.Errorf("patch node %s: %w", node.Name, err)
	}
	return nil
}

func findTaint(taints []corev1.Taint) (bool, int) {
	for i, t := range taints {
		if t.Key == TaintKey {
			return true, i
		}
	}
	return false, -1
}
