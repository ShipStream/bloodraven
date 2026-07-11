package platform

import (
	"context"
	"testing"

	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
)

func TestDNSEndpointUpdater_CreateAndUpdate(t *testing.T) {
	scheme := runtime.NewScheme()
	gvk := schema.GroupVersionKind{Group: "externaldns.k8s.io", Version: "v1alpha1", Kind: "DNSEndpoint"}
	listGVK := schema.GroupVersionKind{Group: "externaldns.k8s.io", Version: "v1alpha1", Kind: "DNSEndpointList"}
	scheme.AddKnownTypeWithName(gvk, &unstructured.Unstructured{})
	scheme.AddKnownTypeWithName(listGVK, &unstructured.UnstructuredList{})

	c := fake.NewClientBuilder().WithScheme(scheme).Build()

	dns := NewDNSEndpointUpdater(c, "main", "uid-123", "default", "main", "lion.az.example.com", 60)

	ctx := context.Background()

	// First call: creates the DNSEndpoint.
	if err := dns.UpdateDNSRecord(ctx, "10.0.1.100"); err != nil {
		t.Fatalf("first UpdateDNSRecord: %v", err)
	}

	// Verify the object was created.
	obj := &unstructured.Unstructured{}
	obj.SetGroupVersionKind(gvk)
	if err := c.Get(ctx, client.ObjectKey{Namespace: "default", Name: "bloodraven-main"}, obj); err != nil {
		t.Fatalf("get DNSEndpoint: %v", err)
	}

	// Check spec.endpoints.
	spec, ok := obj.Object["spec"].(map[string]interface{})
	if !ok {
		t.Fatal("missing spec")
	}
	endpoints, ok := spec["endpoints"].([]interface{})
	if !ok || len(endpoints) != 1 {
		t.Fatalf("expected 1 endpoint, got %v", endpoints)
	}
	ep := endpoints[0].(map[string]interface{})
	if ep["dnsName"] != "lion.az.example.com" {
		t.Errorf("dnsName: got %v, want lion.az.example.com", ep["dnsName"])
	}
	targets := ep["targets"].([]interface{})
	if len(targets) != 1 || targets[0] != "10.0.1.100" {
		t.Errorf("targets: got %v, want [10.0.1.100]", targets)
	}

	// Check owner reference.
	owners := obj.GetOwnerReferences()
	if len(owners) != 1 {
		t.Fatalf("expected 1 owner ref, got %d", len(owners))
	}
	if owners[0].Name != "main" || owners[0].Kind != "MysqlFailoverGroup" {
		t.Errorf("owner ref: got %+v", owners[0])
	}

	// Second call: updates the target IP.
	if err := dns.UpdateDNSRecord(ctx, "10.0.2.200"); err != nil {
		t.Fatalf("second UpdateDNSRecord: %v", err)
	}

	if err := c.Get(ctx, client.ObjectKey{Namespace: "default", Name: "bloodraven-main"}, obj); err != nil {
		t.Fatalf("get after update: %v", err)
	}
	spec = obj.Object["spec"].(map[string]interface{})
	endpoints = spec["endpoints"].([]interface{})
	ep = endpoints[0].(map[string]interface{})
	targets = ep["targets"].([]interface{})
	if len(targets) != 1 || targets[0] != "10.0.2.200" {
		t.Errorf("updated targets: got %v, want [10.0.2.200]", targets)
	}
}

// TestDNSEndpointUpdater_CurrentDNSRecord covers the read side the topology
// manager reconciles against: an absent record reads as not-found (a
// divergence to repair), and an existing one reads back the target it steers.
// This is what makes a denied DNS flip heal after an operator restart, when
// nothing about the failed write survives in memory.
func TestDNSEndpointUpdater_CurrentDNSRecord(t *testing.T) {
	scheme := runtime.NewScheme()
	gvk := schema.GroupVersionKind{Group: "externaldns.k8s.io", Version: "v1alpha1", Kind: "DNSEndpoint"}
	listGVK := schema.GroupVersionKind{Group: "externaldns.k8s.io", Version: "v1alpha1", Kind: "DNSEndpointList"}
	scheme.AddKnownTypeWithName(gvk, &unstructured.Unstructured{})
	scheme.AddKnownTypeWithName(listGVK, &unstructured.UnstructuredList{})

	c := fake.NewClientBuilder().WithScheme(scheme).Build()
	dns := NewDNSEndpointUpdater(c, "main", "uid-123", "default", "main", "lion.az.example.com", 60)
	reader, ok := dns.(DNSRecordReader)
	if !ok {
		t.Fatal("the DNSEndpoint updater must implement DNSRecordReader so DNS can be reconciled against the live record")
	}
	ctx := context.Background()

	// No DNSEndpoint yet: not found, no error — the caller treats it as a
	// divergence and creates the record.
	target, found, err := reader.CurrentDNSRecord(ctx)
	if err != nil {
		t.Fatalf("CurrentDNSRecord on a missing DNSEndpoint must not error: %v", err)
	}
	if found || target != "" {
		t.Errorf("missing DNSEndpoint: got (%q, %v), want (\"\", false)", target, found)
	}

	if err := dns.UpdateDNSRecord(ctx, "10.0.1.100"); err != nil {
		t.Fatalf("UpdateDNSRecord: %v", err)
	}
	target, found, err = reader.CurrentDNSRecord(ctx)
	if err != nil {
		t.Fatalf("CurrentDNSRecord: %v", err)
	}
	if !found || target != "10.0.1.100" {
		t.Errorf("after apply: got (%q, %v), want (\"10.0.1.100\", true)", target, found)
	}

	// A record for some other hostname is not ours to report.
	other := &unstructured.Unstructured{}
	other.SetGroupVersionKind(gvk)
	if err := c.Get(ctx, client.ObjectKey{Namespace: "default", Name: "bloodraven-main"}, other); err != nil {
		t.Fatalf("get DNSEndpoint to rewrite: %v", err)
	}
	if err := unstructured.SetNestedSlice(other.Object, []interface{}{
		map[string]interface{}{
			"dnsName":    "someone-else.example.com",
			"recordType": "A",
			"targets":    []interface{}{"192.0.2.1"},
		},
	}, "spec", "endpoints"); err != nil {
		t.Fatalf("rewrite endpoints: %v", err)
	}
	if err := c.Update(ctx, other); err != nil {
		t.Fatalf("seed foreign endpoint: %v", err)
	}
	target, found, err = reader.CurrentDNSRecord(ctx)
	if err != nil {
		t.Fatalf("CurrentDNSRecord with a foreign endpoint: %v", err)
	}
	if found || target != "" {
		t.Errorf("foreign hostname: got (%q, %v), want (\"\", false)", target, found)
	}
}

// Verify the interfaces are satisfied at compile time.
var (
	_ DNSUpdater      = (*dnsEndpointUpdater)(nil)
	_ DNSRecordReader = (*dnsEndpointUpdater)(nil)
)
