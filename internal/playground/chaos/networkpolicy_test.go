package chaos

import (
	"context"
	"reflect"
	"testing"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes/fake"

	pgkube "github.com/shipstream/bloodraven/internal/playground/kube"
)

// TestDenyDNSEgressAppliesAndReverts proves the fix for the live scenario 33
// failure: the applied NetworkPolicy excepts every deny IP passed in (the
// kube-dns ClusterIP plus its backend pod IPs), and the reverter removes it
// by the same name DenyDNSEgress used to create it.
func TestDenyDNSEgressAppliesAndReverts(t *testing.T) {
	cs := fake.NewSimpleClientset()
	k := &pgkube.Client{Kubernetes: cs}
	a := New(k, pgkube.PlaygroundNamespace, pgkube.FailoverGroupName)

	ctx := context.Background()
	denyIPs := []string{"10.43.0.10", "10.42.1.5", "10.42.2.9"}
	if err := a.DenyDNSEgress(ctx, "iad", denyIPs); err != nil {
		t.Fatalf("DenyDNSEgress: %v", err)
	}

	name := pgkube.DNSEgressDenyPolicyName("iad")
	np, err := cs.NetworkingV1().NetworkPolicies(pgkube.PlaygroundNamespace).Get(ctx, name, metav1.GetOptions{})
	if err != nil {
		t.Fatalf("get applied NetworkPolicy %s: %v", name, err)
	}
	want := []string{"10.43.0.10/32", "10.42.1.5/32", "10.42.2.9/32"}
	got := np.Spec.Egress[0].To[0].IPBlock.Except
	if !reflect.DeepEqual(got, want) {
		t.Errorf("applied except = %v, want %v", got, want)
	}

	if pend := a.PendingReverts(); len(pend) != 1 {
		t.Fatalf("expected 1 pending reverter after DenyDNSEgress, got %v", pend)
	}
	if err := a.Revert(ctx); err != nil {
		t.Fatalf("Revert: %v", err)
	}
	if _, err := cs.NetworkingV1().NetworkPolicies(pgkube.PlaygroundNamespace).Get(ctx, name, metav1.GetOptions{}); err == nil {
		t.Errorf("NetworkPolicy %s still present after revert", name)
	}
}

func TestPartitionDragonflyPodAppliesAndReverts(t *testing.T) {
	cs := fake.NewSimpleClientset()
	a := New(&pgkube.Client{Kubernetes: cs}, pgkube.PlaygroundNamespace, pgkube.FailoverGroupName)
	ctx := context.Background()
	if err := a.PartitionDragonflyPod(ctx, "iad"); err != nil {
		t.Fatalf("PartitionDragonflyPod: %v", err)
	}
	name := pgkube.DragonflyPartitionPolicyName("iad")
	np, err := cs.NetworkingV1().NetworkPolicies(pgkube.PlaygroundNamespace).Get(ctx, name, metav1.GetOptions{})
	if err != nil {
		t.Fatalf("get applied NetworkPolicy: %v", err)
	}
	if len(np.Spec.PolicyTypes) != 2 || len(np.Spec.Ingress) != 0 || len(np.Spec.Egress) != 0 {
		t.Fatalf("partition policy is not deny-all: %+v", np.Spec)
	}
	if len(a.PendingReverts()) != 1 {
		t.Fatalf("pending reverts = %v, want one", a.PendingReverts())
	}
	if err := a.Revert(ctx); err != nil {
		t.Fatalf("Revert: %v", err)
	}
	if _, err := cs.NetworkingV1().NetworkPolicies(pgkube.PlaygroundNamespace).Get(ctx, name, metav1.GetOptions{}); err == nil {
		t.Fatalf("NetworkPolicy %s remains after revert", name)
	}
}
