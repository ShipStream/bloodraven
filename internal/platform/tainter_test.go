package platform

import (
	"context"
	"encoding/json"
	"log/slog"
	"os"
	"testing"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/kubernetes/fake"
	k8stesting "k8s.io/client-go/testing"
)

func TestNodeTainter_ApplyAndRemoveTaint(t *testing.T) {
	node1 := &corev1.Node{
		ObjectMeta: metav1.ObjectMeta{
			Name: "node1",
			Labels: map[string]string{
				"shipstream.io/failover-group": "orders",
				"shipstream.io/site":           "iad",
			},
		},
	}
	node2 := &corev1.Node{
		ObjectMeta: metav1.ObjectMeta{
			Name: "node2",
			Labels: map[string]string{
				"shipstream.io/failover-group": "orders",
				"shipstream.io/site":           "iad",
			},
		},
	}
	node3 := &corev1.Node{
		ObjectMeta: metav1.ObjectMeta{
			Name: "node3",
			Labels: map[string]string{
				"shipstream.io/failover-group": "orders",
				"shipstream.io/site":           "pdx",
			},
		},
	}

	client := fake.NewSimpleClientset(node1, node2, node3)

	// The fake client doesn't apply strategic merge patches, so we intercept
	// Patch calls and apply the taint changes manually.
	client.PrependReactor("patch", "nodes", func(action k8stesting.Action) (bool, runtime.Object, error) {
		patchAction := action.(k8stesting.PatchAction)
		name := patchAction.GetName()

		node, err := client.Tracker().Get(
			corev1.SchemeGroupVersion.WithResource("nodes"),
			"", name,
		)
		if err != nil {
			return true, nil, err
		}

		n := node.(*corev1.Node)
		var patch struct {
			Spec struct {
				Taints []corev1.Taint `json:"taints"`
			} `json:"spec"`
		}
		if err := json.Unmarshal(patchAction.GetPatch(), &patch); err != nil {
			return true, nil, err
		}
		n.Spec.Taints = patch.Spec.Taints

		if err := client.Tracker().Update(
			corev1.SchemeGroupVersion.WithResource("nodes"),
			n, "",
		); err != nil {
			return true, nil, err
		}
		return true, n, nil
	})

	logger := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelWarn}))
	tainter := NewNodeTainter(client, logger)
	ctx := context.Background()

	iadSelector := "shipstream.io/failover-group=orders,shipstream.io/site=iad"
	pdxSelector := "shipstream.io/failover-group=orders,shipstream.io/site=pdx"

	// Apply taint to iad nodes
	if err := tainter.SetTaint(ctx, iadSelector, true); err != nil {
		t.Fatalf("apply taint: %v", err)
	}

	// Verify iad nodes are tainted
	for _, name := range []string{"node1", "node2"} {
		node, _ := client.CoreV1().Nodes().Get(ctx, name, metav1.GetOptions{})
		if !hasTaint(node.Spec.Taints) {
			t.Errorf("%s should be tainted", name)
		}
	}

	// Verify pdx node is NOT tainted
	n3, _ := client.CoreV1().Nodes().Get(ctx, "node3", metav1.GetOptions{})
	if hasTaint(n3.Spec.Taints) {
		t.Error("node3 (pdx) should not be tainted")
	}

	// Apply taint again (idempotent)
	if err := tainter.SetTaint(ctx, iadSelector, true); err != nil {
		t.Fatalf("re-apply taint: %v", err)
	}

	node1Out, _ := client.CoreV1().Nodes().Get(ctx, "node1", metav1.GetOptions{})
	count := 0
	for _, taint := range node1Out.Spec.Taints {
		if taint.Key == TaintKey {
			count++
		}
	}
	if count != 1 {
		t.Errorf("expected exactly 1 taint, got %d", count)
	}

	// Remove taint
	if err := tainter.SetTaint(ctx, iadSelector, false); err != nil {
		t.Fatalf("remove taint: %v", err)
	}

	node1Out, _ = client.CoreV1().Nodes().Get(ctx, "node1", metav1.GetOptions{})
	if hasTaint(node1Out.Spec.Taints) {
		t.Error("node1 should not be tainted after removal")
	}

	// Remove again (idempotent)
	if err := tainter.SetTaint(ctx, iadSelector, false); err != nil {
		t.Fatalf("re-remove taint: %v", err)
	}

	// Verify pdx selector works independently
	if err := tainter.SetTaint(ctx, pdxSelector, true); err != nil {
		t.Fatalf("apply taint to pdx: %v", err)
	}
	n3, _ = client.CoreV1().Nodes().Get(ctx, "node3", metav1.GetOptions{})
	if !hasTaint(n3.Spec.Taints) {
		t.Error("node3 (pdx) should be tainted after pdx selector taint")
	}
	// iad nodes should still be untainted
	node1Out, _ = client.CoreV1().Nodes().Get(ctx, "node1", metav1.GetOptions{})
	if hasTaint(node1Out.Spec.Taints) {
		t.Error("node1 (iad) should not be tainted after pdx-only taint")
	}
}

func hasTaint(taints []corev1.Taint) bool {
	for _, t := range taints {
		if t.Key == TaintKey {
			return true
		}
	}
	return false
}
