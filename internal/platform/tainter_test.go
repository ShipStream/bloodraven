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
	installPatchReactor(client)

	logger := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelWarn}))
	tainter := NewNodeTainter(client, logger)
	ctx := context.Background()

	iadSelector := "shipstream.io/failover-group=orders,shipstream.io/site=iad"
	pdxSelector := "shipstream.io/failover-group=orders,shipstream.io/site=pdx"

	// Apply taint to iad nodes
	if err := tainter.SetTaint(ctx, iadSelector, "orders", true); err != nil {
		t.Fatalf("apply taint: %v", err)
	}

	// Verify iad nodes are tainted
	for _, name := range []string{"node1", "node2"} {
		node, _ := client.CoreV1().Nodes().Get(ctx, name, metav1.GetOptions{})
		if !hasTaintForGroup(node.Spec.Taints, "orders") {
			t.Errorf("%s should be tainted", name)
		}
	}

	// Verify pdx node is NOT tainted
	n3, _ := client.CoreV1().Nodes().Get(ctx, "node3", metav1.GetOptions{})
	if hasTaintForGroup(n3.Spec.Taints, "orders") {
		t.Error("node3 (pdx) should not be tainted")
	}

	// Apply taint again (idempotent)
	if err := tainter.SetTaint(ctx, iadSelector, "orders", true); err != nil {
		t.Fatalf("re-apply taint: %v", err)
	}

	node1Out, _ := client.CoreV1().Nodes().Get(ctx, "node1", metav1.GetOptions{})
	count := 0
	for _, taint := range node1Out.Spec.Taints {
		if taint.Key == TaintKeyForGroup("orders") {
			count++
		}
	}
	if count != 1 {
		t.Errorf("expected exactly 1 taint, got %d", count)
	}

	// Remove taint
	if err := tainter.SetTaint(ctx, iadSelector, "orders", false); err != nil {
		t.Fatalf("remove taint: %v", err)
	}

	node1Out, _ = client.CoreV1().Nodes().Get(ctx, "node1", metav1.GetOptions{})
	if hasTaintForGroup(node1Out.Spec.Taints, "orders") {
		t.Error("node1 should not be tainted after removal")
	}

	// Remove again (idempotent)
	if err := tainter.SetTaint(ctx, iadSelector, "orders", false); err != nil {
		t.Fatalf("re-remove taint: %v", err)
	}

	// Verify pdx selector works independently
	if err := tainter.SetTaint(ctx, pdxSelector, "orders", true); err != nil {
		t.Fatalf("apply taint to pdx: %v", err)
	}
	n3, _ = client.CoreV1().Nodes().Get(ctx, "node3", metav1.GetOptions{})
	if !hasTaintForGroup(n3.Spec.Taints, "orders") {
		t.Error("node3 (pdx) should be tainted after pdx selector taint")
	}
	// iad nodes should still be untainted
	node1Out, _ = client.CoreV1().Nodes().Get(ctx, "node1", metav1.GetOptions{})
	if hasTaintForGroup(node1Out.Spec.Taints, "orders") {
		t.Error("node1 (iad) should not be tainted after pdx-only taint")
	}
}

func TestNodeTainter_LegacyTaintCleanup(t *testing.T) {
	node := &corev1.Node{
		ObjectMeta: metav1.ObjectMeta{
			Name: "node1",
			Labels: map[string]string{
				"shipstream.io/failover-group": "orders",
				"shipstream.io/site":           "iad",
			},
		},
		Spec: corev1.NodeSpec{
			Taints: []corev1.Taint{
				{Key: LegacyTaintKey, Value: TaintValue, Effect: corev1.TaintEffectNoExecute},
			},
		},
	}

	client := fake.NewSimpleClientset(node)
	installPatchReactor(client)

	logger := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelWarn}))
	tainter := NewNodeTainter(client, logger)
	ctx := context.Background()
	selector := "shipstream.io/failover-group=orders,shipstream.io/site=iad"

	// Applying the new per-group taint should also strip the legacy taint.
	if err := tainter.SetTaint(ctx, selector, "orders", true); err != nil {
		t.Fatalf("apply taint: %v", err)
	}

	n, _ := client.CoreV1().Nodes().Get(ctx, "node1", metav1.GetOptions{})
	if hasLegacyTaint(n.Spec.Taints) {
		t.Error("legacy taint should be removed after per-group taint apply")
	}
	if !hasTaintForGroup(n.Spec.Taints, "orders") {
		t.Error("per-group taint should be present")
	}

	// Reset: put legacy taint back, remove per-group taint.
	n.Spec.Taints = []corev1.Taint{
		{Key: LegacyTaintKey, Value: TaintValue, Effect: corev1.TaintEffectNoExecute},
	}
	client.Tracker().Update(corev1.SchemeGroupVersion.WithResource("nodes"), n, "")

	// Removing the per-group taint should also strip the legacy taint.
	if err := tainter.SetTaint(ctx, selector, "orders", false); err != nil {
		t.Fatalf("remove taint: %v", err)
	}

	n, _ = client.CoreV1().Nodes().Get(ctx, "node1", metav1.GetOptions{})
	if hasLegacyTaint(n.Spec.Taints) {
		t.Error("legacy taint should be removed after per-group taint removal")
	}
	if hasTaintForGroup(n.Spec.Taints, "orders") {
		t.Error("per-group taint should not be present after removal")
	}
}

func TestNodeTainter_PerGroupIsolation(t *testing.T) {
	node := &corev1.Node{
		ObjectMeta: metav1.ObjectMeta{
			Name: "shared-node",
			Labels: map[string]string{
				"shipstream.io/failover-group": "orders",
				"shipstream.io/site":           "iad",
			},
		},
	}

	client := fake.NewSimpleClientset(node)
	installPatchReactor(client)

	logger := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelWarn}))
	tainter := NewNodeTainter(client, logger)
	ctx := context.Background()
	selector := "shipstream.io/failover-group=orders,shipstream.io/site=iad"

	// Apply orders taint
	if err := tainter.SetTaint(ctx, selector, "orders", true); err != nil {
		t.Fatalf("apply orders taint: %v", err)
	}

	n, _ := client.CoreV1().Nodes().Get(ctx, "shared-node", metav1.GetOptions{})
	if !hasTaintForGroup(n.Spec.Taints, "orders") {
		t.Error("orders taint should be present")
	}
	if hasTaintForGroup(n.Spec.Taints, "inventory") {
		t.Error("inventory taint should not be present")
	}

	// Remove orders taint — only orders key affected
	if err := tainter.SetTaint(ctx, selector, "orders", false); err != nil {
		t.Fatalf("remove orders taint: %v", err)
	}

	n, _ = client.CoreV1().Nodes().Get(ctx, "shared-node", metav1.GetOptions{})
	if hasTaintForGroup(n.Spec.Taints, "orders") {
		t.Error("orders taint should be removed")
	}
}

// installPatchReactor adds a reactor that applies strategic merge patches to
// the fake client's object tracker, since the fake client doesn't natively
// support strategic merge patches.
func installPatchReactor(client *fake.Clientset) {
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
}

func hasTaintForGroup(taints []corev1.Taint, group string) bool {
	key := TaintKeyForGroup(group)
	for _, t := range taints {
		if t.Key == key {
			return true
		}
	}
	return false
}

func hasLegacyTaint(taints []corev1.Taint) bool {
	for _, t := range taints {
		if t.Key == LegacyTaintKey {
			return true
		}
	}
	return false
}
