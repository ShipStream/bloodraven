package kube

import (
	"context"
	"reflect"
	"testing"

	corev1 "k8s.io/api/core/v1"
	networkingv1 "k8s.io/api/networking/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes/fake"
)

func TestBuildDNSEgressDenyPolicy(t *testing.T) {
	// Both the ClusterIP and backend pod IPs must be excepted: some CNIs
	// enforce NetworkPolicy before Service DNAT (destination is still the
	// ClusterIP) and others after (destination is already a pod IP).
	np := BuildDNSEgressDenyPolicy("playground", "iad", []string{"10.43.0.10", "10.42.1.5", "10.42.2.9"})

	if np.Name != "chaos-dns-deny-iad" {
		t.Errorf("name = %q, want chaos-dns-deny-iad", np.Name)
	}
	if np.Labels["app"] != ChaosNetworkPolicyLabelValue {
		t.Errorf("missing chaos-partition label so GlobalRecover can sweep it: %v", np.Labels)
	}
	// Selects only the MySQL pod for the site.
	sel := np.Spec.PodSelector.MatchLabels
	if sel["app.kubernetes.io/name"] != "mysql" || sel["app.kubernetes.io/instance"] != "playground" || sel["shipstream.io/site"] != "iad" {
		t.Errorf("pod selector = %v, want mysql/playground/iad", sel)
	}
	// Egress-only, allow 0.0.0.0/0 except the kube-dns ClusterIP and pod IPs.
	if len(np.Spec.PolicyTypes) != 1 || np.Spec.PolicyTypes[0] != networkingv1.PolicyTypeEgress {
		t.Errorf("policyTypes = %v, want [Egress]", np.Spec.PolicyTypes)
	}
	if len(np.Spec.Egress) != 1 || len(np.Spec.Egress[0].To) != 1 {
		t.Fatalf("egress rule shape unexpected: %+v", np.Spec.Egress)
	}
	ipb := np.Spec.Egress[0].To[0].IPBlock
	if ipb == nil || ipb.CIDR != "0.0.0.0/0" {
		t.Fatalf("egress CIDR = %+v, want 0.0.0.0/0", ipb)
	}
	wantExcept := []string{"10.43.0.10/32", "10.42.1.5/32", "10.42.2.9/32"}
	if !reflect.DeepEqual(ipb.Except, wantExcept) {
		t.Errorf("egress except = %v, want %v", ipb.Except, wantExcept)
	}
	// No ingress rules (this is not a full partition).
	if np.Spec.Ingress != nil {
		t.Errorf("DNS-deny policy must not carry ingress rules: %v", np.Spec.Ingress)
	}
}

func TestBuildDNSEgressDenyPolicyForSelector_DedupesAndSkipsEmpty(t *testing.T) {
	np := BuildDNSEgressDenyPolicyForSelector("chaos-s33-dns-canary-deny", map[string]string{"app": "chaos-dns-canary"},
		[]string{"10.43.0.10", "", "10.42.1.5", "10.43.0.10", "10.42.1.5"})

	ipb := np.Spec.Egress[0].To[0].IPBlock
	want := []string{"10.43.0.10/32", "10.42.1.5/32"}
	if !reflect.DeepEqual(ipb.Except, want) {
		t.Errorf("egress except = %v, want %v (deduped, empty entries dropped)", ipb.Except, want)
	}
}

func TestDiscoverKubeDNSEndpointIPs(t *testing.T) {
	ep := &corev1.Endpoints{
		ObjectMeta: metav1.ObjectMeta{Name: "kube-dns", Namespace: "kube-system"},
		Subsets: []corev1.EndpointSubset{
			{Addresses: []corev1.EndpointAddress{{IP: "10.42.1.5"}, {IP: "10.42.2.9"}}},
		},
	}
	c := &Client{Kubernetes: fake.NewSimpleClientset(ep)}

	got, err := c.DiscoverKubeDNSEndpointIPs(context.Background())
	if err != nil {
		t.Fatalf("DiscoverKubeDNSEndpointIPs: %v", err)
	}
	want := []string{"10.42.1.5", "10.42.2.9"}
	if !reflect.DeepEqual(got, want) {
		t.Errorf("pod IPs = %v, want %v", got, want)
	}
}

func TestDiscoverKubeDNSEndpointIPs_NoBackends(t *testing.T) {
	ep := &corev1.Endpoints{
		ObjectMeta: metav1.ObjectMeta{Name: "kube-dns", Namespace: "kube-system"},
	}
	c := &Client{Kubernetes: fake.NewSimpleClientset(ep)}

	if _, err := c.DiscoverKubeDNSEndpointIPs(context.Background()); err == nil {
		t.Error("expected an error when kube-dns Endpoints has no backing addresses, got nil")
	}
}

func TestBuildStandbyIngressHoldPolicy(t *testing.T) {
	np := BuildStandbyIngressHoldPolicy("playground", "pdx")

	if np.Name != "chaos-standby-hold-pdx" {
		t.Errorf("name = %q, want chaos-standby-hold-pdx", np.Name)
	}
	if np.Labels["app"] != ChaosNetworkPolicyLabelValue {
		t.Errorf("missing chaos-partition label: %v", np.Labels)
	}
	// Selector must target ONLY the standby site's MySQL pod, never the
	// active pod — a mis-scoped hold would freeze the wrong site.
	sel := np.Spec.PodSelector.MatchLabels
	if sel["app.kubernetes.io/name"] != "mysql" || sel["app.kubernetes.io/instance"] != "playground" || sel["shipstream.io/site"] != "pdx" {
		t.Errorf("pod selector = %v, want mysql/playground/pdx only", sel)
	}
	// Ingress-only deny: egress (outbound replication) must stay open.
	if len(np.Spec.PolicyTypes) != 1 || np.Spec.PolicyTypes[0] != networkingv1.PolicyTypeIngress {
		t.Errorf("policyTypes = %v, want [Ingress] only (egress replication must stay open)", np.Spec.PolicyTypes)
	}
	if len(np.Spec.Ingress) != 0 {
		t.Errorf("deny-all-ingress policy must have empty ingress rules, got %v", np.Spec.Ingress)
	}
}

func TestBuildDragonflyPartitionPolicy(t *testing.T) {
	np := BuildDragonflyPartitionPolicy("playground", "iad")

	if np.Name != "chaos-df-partition-iad" {
		t.Errorf("name = %q, want chaos-df-partition-iad", np.Name)
	}
	if np.Labels["app"] != ChaosNetworkPolicyLabelValue {
		t.Errorf("missing chaos-partition label: %v", np.Labels)
	}
	sel := np.Spec.PodSelector.MatchLabels
	if sel["app.kubernetes.io/name"] != "dragonfly" || sel["app.kubernetes.io/instance"] != "playground" || sel["shipstream.io/site"] != "iad" {
		t.Errorf("pod selector = %v, want dragonfly/playground/iad", sel)
	}
	// Deny-all: both ingress and egress policy types, empty rule slices.
	types := map[networkingv1.PolicyType]bool{}
	for _, p := range np.Spec.PolicyTypes {
		types[p] = true
	}
	if !types[networkingv1.PolicyTypeIngress] || !types[networkingv1.PolicyTypeEgress] {
		t.Errorf("policyTypes = %v, want both Ingress and Egress", np.Spec.PolicyTypes)
	}
	if len(np.Spec.Ingress) != 0 || len(np.Spec.Egress) != 0 {
		t.Errorf("deny-all policy must have empty ingress/egress rules, got ingress=%v egress=%v", np.Spec.Ingress, np.Spec.Egress)
	}
}
