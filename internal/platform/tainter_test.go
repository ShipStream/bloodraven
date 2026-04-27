package platform

import (
	"context"
	"encoding/json"
	"log/slog"
	"os"
	"strings"
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
				"shipstream.io/failover-group.orders": "true",
				"shipstream.io/site.orders":           "iad",
			},
		},
	}
	node2 := &corev1.Node{
		ObjectMeta: metav1.ObjectMeta{
			Name: "node2",
			Labels: map[string]string{
				"shipstream.io/failover-group.orders": "true",
				"shipstream.io/site.orders":           "iad",
			},
		},
	}
	node3 := &corev1.Node{
		ObjectMeta: metav1.ObjectMeta{
			Name: "node3",
			Labels: map[string]string{
				"shipstream.io/failover-group.orders": "true",
				"shipstream.io/site.orders":           "pdx",
			},
		},
	}

	client := fake.NewSimpleClientset(node1, node2, node3)
	installPatchReactor(client)

	logger := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelWarn}))
	tainter := NewNodeTainter(client, logger)
	ctx := context.Background()

	iadSelector := "shipstream.io/failover-group.orders=true,shipstream.io/site.orders=iad"
	pdxSelector := "shipstream.io/failover-group.orders=true,shipstream.io/site.orders=pdx"

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

func TestNodeTainter_PerGroupIsolation(t *testing.T) {
	node := &corev1.Node{
		ObjectMeta: metav1.ObjectMeta{
			Name: "shared-node",
			Labels: map[string]string{
				"shipstream.io/failover-group.orders":    "true",
				"shipstream.io/failover-group.inventory": "true",
				"shipstream.io/site.orders":              "iad",
				"shipstream.io/site.inventory":           "iad",
			},
		},
	}

	client := fake.NewSimpleClientset(node)
	installPatchReactor(client)

	logger := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelWarn}))
	tainter := NewNodeTainter(client, logger)
	ctx := context.Background()
	selector := "shipstream.io/failover-group.orders=true,shipstream.io/site.orders=iad"

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

func TestNodeTainter_RejectsEmptySelector(t *testing.T) {
	client := fake.NewSimpleClientset(&corev1.Node{ObjectMeta: metav1.ObjectMeta{Name: "node1"}})
	logger := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelWarn}))
	tainter := NewNodeTainter(client, logger)

	err := tainter.SetTaint(context.Background(), " ", "orders", true)
	if err == nil || !strings.Contains(err.Error(), "empty selector") {
		t.Fatalf("expected empty selector error, got %v", err)
	}
}

func TestNodeTainter_ConvertsExistingGroupTaintToNoExecute(t *testing.T) {
	node := &corev1.Node{
		ObjectMeta: metav1.ObjectMeta{
			Name: "node1",
			Labels: map[string]string{
				"shipstream.io/failover-group.orders": "true",
				"shipstream.io/site.orders":           "iad",
			},
		},
		Spec: corev1.NodeSpec{
			Taints: []corev1.Taint{
				{Key: TaintKeyForGroup("orders"), Value: "old", Effect: corev1.TaintEffectNoSchedule},
			},
		},
	}
	client := fake.NewSimpleClientset(node)
	installPatchReactor(client)
	logger := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelWarn}))
	tainter := NewNodeTainter(client, logger)

	selector := "shipstream.io/failover-group.orders=true,shipstream.io/site.orders=iad"
	if err := tainter.SetTaint(context.Background(), selector, "orders", true); err != nil {
		t.Fatalf("apply taint: %v", err)
	}

	out, _ := client.CoreV1().Nodes().Get(context.Background(), "node1", metav1.GetOptions{})
	if len(out.Spec.Taints) != 1 {
		t.Fatalf("expected exactly 1 taint, got %v", out.Spec.Taints)
	}
	if taint := out.Spec.Taints[0]; taint.Key != TaintKeyForGroup("orders") || taint.Value != TaintValue || taint.Effect != corev1.TaintEffectNoExecute {
		t.Fatalf("expected canonical NoExecute taint, got %v", taint)
	}
}

func TestNodeTainter_RemovesLegacyTaint(t *testing.T) {
	node := &corev1.Node{
		ObjectMeta: metav1.ObjectMeta{
			Name: "node1",
			Labels: map[string]string{
				"shipstream.io/failover-group.orders": "true",
				"shipstream.io/site.orders":           "iad",
			},
		},
		Spec: corev1.NodeSpec{
			Taints: []corev1.Taint{
				{Key: LegacyTaintKey, Value: TaintValue, Effect: corev1.TaintEffectNoExecute},
				{Key: TaintKeyForGroup("orders"), Value: TaintValue, Effect: corev1.TaintEffectNoExecute},
			},
		},
	}
	client := fake.NewSimpleClientset(node)
	installPatchReactor(client)
	logger := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelWarn}))
	tainter := NewNodeTainter(client, logger)

	selector := "shipstream.io/failover-group.orders=true,shipstream.io/site.orders=iad"
	if err := tainter.SetTaint(context.Background(), selector, "orders", false); err != nil {
		t.Fatalf("remove taint: %v", err)
	}

	out, _ := client.CoreV1().Nodes().Get(context.Background(), "node1", metav1.GetOptions{})
	for _, taint := range out.Spec.Taints {
		if taint.Key == LegacyTaintKey || taint.Key == TaintKeyForGroup("orders") {
			t.Fatalf("expected readonly taints removed, got %v", out.Spec.Taints)
		}
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
		if t.Key == key && t.Effect == corev1.TaintEffectNoExecute {
			return true
		}
	}
	return false
}
