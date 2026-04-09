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

// Verify the interface is satisfied at compile time.
var _ DNSUpdater = (*dnsEndpointUpdater)(nil)
