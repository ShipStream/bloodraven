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
)

const (
	TaintKey   = "shipstream.io/db-readonly"
	TaintValue = "true"
)

// NodeTainter manages node taints for DC failover.
type NodeTainter interface {
	SetTaint(ctx context.Context, zone string, taint bool) error
}

type nodeTainter struct {
	client kubernetes.Interface
	logger *slog.Logger
}

func NewNodeTainter(client kubernetes.Interface, logger *slog.Logger) NodeTainter {
	return &nodeTainter{client: client, logger: logger}
}

func (t *nodeTainter) SetTaint(ctx context.Context, zone string, taint bool) error {
	nodes, err := t.client.CoreV1().Nodes().List(ctx, metav1.ListOptions{
		LabelSelector: fmt.Sprintf("topology.kubernetes.io/zone=%s", zone),
	})
	if err != nil {
		return fmt.Errorf("list nodes for zone %s: %w", zone, err)
	}

	var errs []error
	for _, node := range nodes.Items {
		if err := t.patchNodeTaint(ctx, node, taint); err != nil {
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
