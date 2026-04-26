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
	TaintKeyPrefix = "shipstream.io/db-readonly-"
	TaintValue     = "true"
)

func TaintKeyForGroup(group string) string {
	return TaintKeyPrefix + group
}

// NodeTainter manages node taints for failover group site failover.
// The selector parameter is a Kubernetes label selector string, usually
// produced from spec.sites[].taintNodeSelector.
// The group parameter is the failover group name, used to derive the
// per-group taint key (e.g. "shipstream.io/db-readonly-orders").
type NodeTainter interface {
	SetTaint(ctx context.Context, selector string, group string, taint bool) error
}

type nodeTainter struct {
	client kubernetes.Interface
	logger *slog.Logger
}

func NewNodeTainter(client kubernetes.Interface, logger *slog.Logger) NodeTainter {
	return &nodeTainter{client: client, logger: logger}
}

func (t *nodeTainter) SetTaint(ctx context.Context, selector string, group string, taint bool) error {
	taintKey := TaintKeyForGroup(group)
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
			fresh, err := t.client.CoreV1().Nodes().Get(ctx, node.Name, metav1.GetOptions{})
			if err != nil {
				return fmt.Errorf("get node %s: %w", node.Name, err)
			}
			return t.patchNodeTaint(ctx, *fresh, taintKey, taint)
		}); err != nil {
			errs = append(errs, err)
		}
	}
	return errors.Join(errs...)
}

func (t *nodeTainter) patchNodeTaint(ctx context.Context, node corev1.Node, taintKey string, apply bool) error {
	var hasTaint bool
	for _, taint := range node.Spec.Taints {
		if taint.Key == taintKey {
			hasTaint = true
		}
	}

	if apply && hasTaint {
		return nil
	}
	if !apply && !hasTaint {
		return nil
	}

	newTaints := make([]corev1.Taint, 0, len(node.Spec.Taints)+1)
	for _, taint := range node.Spec.Taints {
		if taint.Key != taintKey {
			newTaints = append(newTaints, taint)
		}
	}
	if apply {
		newTaints = append(newTaints, corev1.Taint{
			Key:    taintKey,
			Value:  TaintValue,
			Effect: corev1.TaintEffectNoExecute,
		})
		t.logger.Info("applying taint", "node", node.Name, "key", taintKey)
	} else {
		t.logger.Info("removing taint", "node", node.Name, "key", taintKey)
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
