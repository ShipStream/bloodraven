package platform

import (
	"context"
	"strings"
	"sync"
	"testing"

	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"
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

func TestDNSEndpointUpdater_SetRecordSpecRewritesHostnameAndTTL(t *testing.T) {
	c, gvk := newDNSFakeClient(t)
	dns := NewDNSEndpointUpdater(c, "main", "uid-123", "default", "main", "old.example.com", 60)
	ctx := context.Background()
	if err := dns.UpdateDNSRecord(ctx, "10.0.1.100"); err != nil {
		t.Fatalf("seed UpdateDNSRecord: %v", err)
	}

	spec, ok := dns.(DNSSpecController)
	if !ok {
		t.Fatal("DNSEndpoint updater must implement DNSSpecController so hostname/TTL can change without a manager restart")
	}
	spec.SetRecordSpec("new.example.com", 30, 4)
	if !spec.SpecNeedsApply() {
		t.Fatal("SetRecordSpec that changes hostname/TTL must set SpecNeedsApply")
	}

	host, ttl := spec.RecordSpec()
	if host != "new.example.com" || ttl != 30 {
		t.Fatalf("RecordSpec() = (%q, %d), want (new.example.com, 30)", host, ttl)
	}

	// Live object still has the old name: CurrentDNSRecord (hostname-keyed)
	// reports not-found, CurrentDNSEndpoint reports the live old name.
	reader := dns.(DNSRecordReader)
	if target, found, err := reader.CurrentDNSRecord(ctx); err != nil || found || target != "" {
		t.Fatalf("CurrentDNSRecord after rename: got (%q, %v, %v), want (\"\", false, nil)", target, found, err)
	}
	liveHost, liveTarget, liveTTL, found, err := spec.CurrentDNSEndpoint(ctx)
	if err != nil || !found {
		t.Fatalf("CurrentDNSEndpoint: found=%v err=%v", found, err)
	}
	if liveHost != "old.example.com" || liveTarget != "10.0.1.100" || liveTTL != 60 {
		t.Fatalf("live endpoint = (%q, %q, %d), want (old.example.com, 10.0.1.100, 60)", liveHost, liveTarget, liveTTL)
	}

	if err := dns.UpdateDNSRecord(ctx, "10.0.1.100"); err != nil {
		t.Fatalf("rewrite UpdateDNSRecord: %v", err)
	}
	if spec.SpecNeedsApply() {
		t.Fatal("successful UpdateDNSRecord should clear SpecNeedsApply")
	}
	obj := getDNSEndpoint(t, c, gvk)
	ep := firstEndpoint(t, obj)
	if ep["dnsName"] != "new.example.com" {
		t.Errorf("dnsName after rewrite: got %v, want new.example.com", ep["dnsName"])
	}
	if got := recordTTLOf(t, ep); got != 30 {
		t.Errorf("recordTTL after rewrite: got %d, want 30", got)
	}
}

func TestDNSEndpointUpdater_SetRecordSpecIgnoresOlderGeneration(t *testing.T) {
	c, _ := newDNSFakeClient(t)
	dns := NewDNSEndpointUpdater(c, "main", "uid", "default", "main", "first.example.com", 60).(DNSSpecController)
	dns.SetRecordSpec("second.example.com", 15, 5)
	dns.SetRecordSpec("stale.example.com", 99, 4)
	host, ttl := dns.RecordSpec()
	if host != "second.example.com" || ttl != 15 {
		t.Fatalf("older generation overwrote spec: got (%q, %d)", host, ttl)
	}
	dns.SetRecordSpec("third.example.com", 20, 5)
	host, ttl = dns.RecordSpec()
	if host != "third.example.com" || ttl != 20 {
		t.Fatalf("equal generation should apply: got (%q, %d)", host, ttl)
	}
}

func TestDNSEndpointUpdater_UpdateRetriesIfSpecChangesDuringApply(t *testing.T) {
	scheme := runtime.NewScheme()
	gvk := schema.GroupVersionKind{Group: "externaldns.k8s.io", Version: "v1alpha1", Kind: "DNSEndpoint"}
	listGVK := schema.GroupVersionKind{Group: "externaldns.k8s.io", Version: "v1alpha1", Kind: "DNSEndpointList"}
	scheme.AddKnownTypeWithName(gvk, &unstructured.Unstructured{})
	scheme.AddKnownTypeWithName(listGVK, &unstructured.UnstructuredList{})

	var spec DNSSpecController
	var once sync.Once
	c := fake.NewClientBuilder().WithScheme(scheme).
		WithInterceptorFuncs(interceptor.Funcs{
			Patch: func(ctx context.Context, underlying client.WithWatch, obj client.Object, patch client.Patch, opts ...client.PatchOption) error {
				once.Do(func() {
					if spec != nil {
						spec.SetRecordSpec("new.example.com", 15, 2)
					}
				})
				return underlying.Patch(ctx, obj, patch, opts...)
			},
		}).
		Build()

	dns := NewDNSEndpointUpdater(c, "main", "uid", "default", "main", "old.example.com", 60)
	spec = dns.(DNSSpecController)
	spec.SetRecordSpec("old.example.com", 60, 1)
	if err := dns.UpdateDNSRecord(context.Background(), "10.0.1.100"); err != nil {
		t.Fatalf("UpdateDNSRecord: %v", err)
	}
	obj := getDNSEndpoint(t, c, gvk)
	ep := firstEndpoint(t, obj)
	if ep["dnsName"] != "new.example.com" {
		t.Fatalf("stale in-flight apply won: dnsName=%v, want new.example.com", ep["dnsName"])
	}
	if got := recordTTLOf(t, ep); got != 15 {
		t.Fatalf("stale in-flight apply won: ttl=%d, want 15", got)
	}
	if spec.SpecNeedsApply() {
		t.Fatal("successful catch-up apply should clear SpecNeedsApply")
	}
}

func TestDNSEndpointUpdater_CurrentDNSEndpointReadsFloat64TTL(t *testing.T) {
	scheme := runtime.NewScheme()
	gvk := schema.GroupVersionKind{Group: "externaldns.k8s.io", Version: "v1alpha1", Kind: "DNSEndpoint"}
	scheme.AddKnownTypeWithName(gvk, &unstructured.Unstructured{})
	obj := &unstructured.Unstructured{Object: map[string]interface{}{
		"apiVersion": "externaldns.k8s.io/v1alpha1", "kind": "DNSEndpoint",
		"metadata": map[string]interface{}{"name": "bloodraven-main", "namespace": "default"},
		"spec": map[string]interface{}{"endpoints": []interface{}{
			map[string]interface{}{
				"dnsName":    "lion.az.example.com",
				"recordType": "A",
				"targets":    []interface{}{"10.0.0.1"},
				"recordTTL":  float64(60), // API-server JSON decode
			},
		}},
	}}
	obj.SetGroupVersionKind(gvk)
	c := fake.NewClientBuilder().WithScheme(scheme).WithObjects(obj).Build()
	dns := NewDNSEndpointUpdater(c, "main", "uid", "default", "main", "lion.az.example.com", 60).(DNSSpecController)
	host, target, ttl, found, err := dns.CurrentDNSEndpoint(context.Background())
	if err != nil || !found {
		t.Fatalf("CurrentDNSEndpoint: found=%v err=%v", found, err)
	}
	if host != "lion.az.example.com" || target != "10.0.0.1" || ttl != 60 {
		t.Fatalf("got (%q, %q, %d), want (lion.az.example.com, 10.0.0.1, 60)", host, target, ttl)
	}
}

func TestDNSEndpointUpdater_CurrentDNSRecordRejectsMalformedFields(t *testing.T) {
	for _, tc := range []struct {
		name     string
		endpoint map[string]interface{}
		want     string
	}{
		{name: "dnsName", endpoint: map[string]interface{}{"dnsName": int64(1), "targets": []interface{}{"10.0.0.1"}}, want: "dnsName"},
		{name: "targets", endpoint: map[string]interface{}{"dnsName": "lion.az.example.com", "targets": "10.0.0.1"}, want: "targets"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			scheme := runtime.NewScheme()
			gvk := schema.GroupVersionKind{Group: "externaldns.k8s.io", Version: "v1alpha1", Kind: "DNSEndpoint"}
			scheme.AddKnownTypeWithName(gvk, &unstructured.Unstructured{})
			obj := &unstructured.Unstructured{Object: map[string]interface{}{
				"apiVersion": "externaldns.k8s.io/v1alpha1", "kind": "DNSEndpoint",
				"metadata": map[string]interface{}{"name": "bloodraven-main", "namespace": "default"},
				"spec":     map[string]interface{}{"endpoints": []interface{}{tc.endpoint}},
			}}
			obj.SetGroupVersionKind(gvk)
			c := fake.NewClientBuilder().WithScheme(scheme).WithObjects(obj).Build()
			dns := NewDNSEndpointUpdater(c, "main", "uid", "default", "main", "lion.az.example.com", 60).(DNSRecordReader)
			if _, _, err := dns.CurrentDNSRecord(context.Background()); err == nil || !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("CurrentDNSRecord error = %v, want malformed %s error", err, tc.want)
			}
		})
	}
}

func newDNSFakeClient(t *testing.T) (client.Client, schema.GroupVersionKind) {
	t.Helper()
	scheme := runtime.NewScheme()
	gvk := schema.GroupVersionKind{Group: "externaldns.k8s.io", Version: "v1alpha1", Kind: "DNSEndpoint"}
	listGVK := schema.GroupVersionKind{Group: "externaldns.k8s.io", Version: "v1alpha1", Kind: "DNSEndpointList"}
	scheme.AddKnownTypeWithName(gvk, &unstructured.Unstructured{})
	scheme.AddKnownTypeWithName(listGVK, &unstructured.UnstructuredList{})
	return fake.NewClientBuilder().WithScheme(scheme).Build(), gvk
}

func getDNSEndpoint(t *testing.T, c client.Client, gvk schema.GroupVersionKind) *unstructured.Unstructured {
	t.Helper()
	obj := &unstructured.Unstructured{}
	obj.SetGroupVersionKind(gvk)
	if err := c.Get(context.Background(), client.ObjectKey{Namespace: "default", Name: "bloodraven-main"}, obj); err != nil {
		t.Fatalf("get DNSEndpoint: %v", err)
	}
	return obj
}

func firstEndpoint(t *testing.T, obj *unstructured.Unstructured) map[string]interface{} {
	t.Helper()
	spec, ok := obj.Object["spec"].(map[string]interface{})
	if !ok {
		t.Fatal("missing spec")
	}
	endpoints, ok := spec["endpoints"].([]interface{})
	if !ok || len(endpoints) != 1 {
		t.Fatalf("expected 1 endpoint, got %v", endpoints)
	}
	ep, ok := endpoints[0].(map[string]interface{})
	if !ok {
		t.Fatalf("endpoint is %T", endpoints[0])
	}
	return ep
}

func recordTTLOf(t *testing.T, ep map[string]interface{}) int64 {
	t.Helper()
	ttl, err := nestedRecordTTL(ep)
	if err != nil {
		t.Fatalf("recordTTL: %v", err)
	}
	return ttl
}
