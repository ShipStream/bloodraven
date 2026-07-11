package platform

import (
	"context"
	"fmt"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
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

// DNSRecordReader is the optional read side of a DNSUpdater. When an
// updater implements it, the topology manager reconciles DNS against the
// LIVE record instead of relying on what this process happens to remember:
// on every poll it compares the record with the current active site and
// re-applies only on a real divergence. That is what makes DNS steering
// restart-safe — a promotion-time apply that was rejected (say, RBAC-denied)
// still heals after an operator restart, because the desired target is
// re-derived from the live topology rather than from an in-memory retry.
//
// Implementations return found=false (and an empty target) when the record
// does not exist yet, which the caller treats as a divergence to repair.
type DNSRecordReader interface {
	CurrentDNSRecord(ctx context.Context) (target string, found bool, err error)
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

// CurrentDNSRecord reads back the target this updater steers: the first
// target of the endpoint whose dnsName matches the managed hostname. A
// missing DNSEndpoint (or an endpoint list with no entry for the hostname)
// returns found=false so the caller repairs it. Errors other than NotFound
// are returned so the caller can fall back to its in-process state instead
// of mistaking an unreadable record for an absent one.
func (d *dnsEndpointUpdater) CurrentDNSRecord(ctx context.Context) (string, bool, error) {
	obj := &unstructured.Unstructured{}
	obj.SetGroupVersionKind(schema.GroupVersionKind{
		Group:   "externaldns.k8s.io",
		Version: "v1alpha1",
		Kind:    "DNSEndpoint",
	})
	key := client.ObjectKey{Namespace: d.namespace, Name: d.name}
	if err := d.client.Get(ctx, key, obj); err != nil {
		if apierrors.IsNotFound(err) {
			return "", false, nil
		}
		return "", false, fmt.Errorf("get DNSEndpoint %s/%s: %w", d.namespace, d.name, err)
	}

	endpoints, found, err := unstructured.NestedSlice(obj.Object, "spec", "endpoints")
	if err != nil {
		return "", false, fmt.Errorf("read DNSEndpoint %s/%s spec.endpoints: %w", d.namespace, d.name, err)
	}
	if !found {
		return "", false, nil
	}
	for _, e := range endpoints {
		ep, ok := e.(map[string]interface{})
		if !ok {
			continue
		}
		name, _, err := unstructured.NestedString(ep, "dnsName")
		if err != nil {
			return "", false, fmt.Errorf("read DNSEndpoint %s/%s endpoint dnsName: %w", d.namespace, d.name, err)
		}
		if name != d.hostname {
			continue
		}
		targets, _, err := unstructured.NestedStringSlice(ep, "targets")
		if err != nil {
			return "", false, fmt.Errorf("read DNSEndpoint %s/%s endpoint targets: %w", d.namespace, d.name, err)
		}
		if len(targets) == 0 {
			return "", false, nil
		}
		return targets[0], true, nil
	}
	return "", false, nil
}
