package kube

import (
	"context"
	"fmt"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

// dnsEndpointGVK is the external-dns DNSEndpoint GroupVersionKind the operator
// writes via internal/platform/dns.go. Read as unstructured so the playground
// runner does not need the external-dns typed API vendored.
var dnsEndpointGVK = schema.GroupVersionKind{
	Group:   "externaldns.k8s.io",
	Version: "v1alpha1",
	Kind:    "DNSEndpoint",
}

// DNSEndpointName returns the external-dns DNSEndpoint object name the
// operator manages for a failover group: "bloodraven-<fg>" (see
// internal/platform/dns.go).
func DNSEndpointName(fg string) string {
	return "bloodraven-" + fg
}

// GetDNSEndpointTargets reads the external-dns DNSEndpoint object
// bloodraven-<fg> and returns spec.endpoints[0].targets — the A-record IPs the
// operator last successfully applied. Returns found=false if the object is
// absent. Scenario 38 uses this to prove the DNS target stays stale while the
// operator's dnsendpoints write is RBAC-denied, then flips to the promoted
// site's LBIP after the denial is lifted.
func (c *Client) GetDNSEndpointTargets(ctx context.Context, namespace, fg string) (targets []string, found bool, err error) {
	if namespace == "" {
		namespace = PlaygroundNamespace
	}
	obj := &unstructured.Unstructured{}
	obj.SetGroupVersionKind(dnsEndpointGVK)
	key := client.ObjectKey{Namespace: namespace, Name: DNSEndpointName(fg)}
	if getErr := c.Controller.Get(ctx, key, obj); getErr != nil {
		if apierrors.IsNotFound(getErr) {
			return nil, false, nil
		}
		return nil, false, fmt.Errorf("get DNSEndpoint %s/%s: %w", namespace, DNSEndpointName(fg), getErr)
	}
	endpoints, ok, nestedErr := unstructured.NestedSlice(obj.Object, "spec", "endpoints")
	if nestedErr != nil {
		return nil, true, fmt.Errorf("read spec.endpoints: %w", nestedErr)
	}
	if !ok || len(endpoints) == 0 {
		return nil, true, nil
	}
	ep0, ok := endpoints[0].(map[string]interface{})
	if !ok {
		return nil, true, fmt.Errorf("spec.endpoints[0] is not an object (got %T)", endpoints[0])
	}
	rawTargets, ok, nestedErr := unstructured.NestedStringSlice(ep0, "targets")
	if nestedErr != nil {
		return nil, true, fmt.Errorf("read spec.endpoints[0].targets: %w", nestedErr)
	}
	if !ok {
		return nil, true, nil
	}
	return rawTargets, true, nil
}
