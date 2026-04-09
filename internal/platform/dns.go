package platform

import (
	"context"
	"fmt"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

// DNSUpdater updates DNS records for failover.
type DNSUpdater interface {
	UpdateDNSRecord(ctx context.Context, ip string) error
}

type dnsEndpointUpdater struct {
	client    client.Client
	ownerRef  metav1.OwnerReference
	namespace string
	name      string // DNSEndpoint resource name
	hostname  string
	ttl       int64
}

// NewDNSEndpointUpdater creates a DNSUpdater that manages a DNSEndpoint CR
// for external-dns to sync. The owner parameter is the MysqlFailoverGroup CR;
// an owner reference is set so the DNSEndpoint is garbage-collected on CR deletion.
func NewDNSEndpointUpdater(c client.Client, ownerName, ownerUID, namespace, fgName, hostname string, ttl int64) DNSUpdater {
	blockOwnerDeletion := true
	return &dnsEndpointUpdater{
		client:    c,
		namespace: namespace,
		name:      "bloodraven-" + fgName,
		hostname:  hostname,
		ttl:       ttl,
		ownerRef: metav1.OwnerReference{
			APIVersion:         "shipstream.io/v1alpha1",
			Kind:               "MysqlFailoverGroup",
			Name:               ownerName,
			UID:                types.UID(ownerUID),
			BlockOwnerDeletion: &blockOwnerDeletion,
		},
	}
}

func (d *dnsEndpointUpdater) UpdateDNSRecord(ctx context.Context, ip string) error {
	obj := &unstructured.Unstructured{
		Object: map[string]interface{}{
			"apiVersion": "externaldns.k8s.io/v1alpha1",
			"kind":       "DNSEndpoint",
			"metadata": map[string]interface{}{
				"name":      d.name,
				"namespace": d.namespace,
			},
			"spec": map[string]interface{}{
				"endpoints": []interface{}{
					map[string]interface{}{
						"dnsName":    d.hostname,
						"recordType": "A",
						"targets":    []interface{}{ip},
						"recordTTL":  d.ttl,
					},
				},
			},
		},
	}
	obj.SetOwnerReferences([]metav1.OwnerReference{d.ownerRef})
	obj.SetGroupVersionKind(schema.GroupVersionKind{
		Group:   "externaldns.k8s.io",
		Version: "v1alpha1",
		Kind:    "DNSEndpoint",
	})

	if err := d.client.Patch(ctx, obj, client.Apply, client.FieldOwner("bloodraven"), client.ForceOwnership); err != nil {
		return fmt.Errorf("apply DNSEndpoint %s/%s: %w", d.namespace, d.name, err)
	}
	return nil
}
