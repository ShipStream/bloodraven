package platform

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"

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

// DNSSpecController is the optional live-update and inspect side of a
// DNSUpdater. The topology manager uses it so spec.dns.hostname and
// spec.dns.ttl can change on a running group without restarting the
// manager, and so reconcileDNS can compare the full owned record
// (dnsName, recordTTL, target) instead of the target IP alone.
type DNSSpecController interface {
	SetRecordSpec(hostname string, ttl int64, generation int64)
	RecordSpec() (hostname string, ttl int64)
	// SpecNeedsApply reports that hostname or TTL changed since the last
	// successful write. reconcileDNS uses this when the live object cannot
	// be read, so a rename still applies instead of waiting on the IP path.
	SpecNeedsApply() bool
	// CurrentDNSEndpoint returns the first endpoint on the managed
	// DNSEndpoint, regardless of whether dnsName matches the desired
	// hostname. found=false means the object or its endpoint list is
	// absent.
	CurrentDNSEndpoint(ctx context.Context) (hostname, target string, ttl int64, found bool, err error)
}

type dnsEndpointUpdater struct {
	client    client.Client
	ownerRef  metav1.OwnerReference
	namespace string
	name      string // DNSEndpoint resource name

	mu         sync.Mutex
	hostname   string
	ttl        int64
	generation int64
	needsApply bool
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

// SetRecordSpec updates the hostname and TTL this updater will write.
// A call whose generation is older than the last applied spec is ignored
// so a stale List snapshot cannot overwrite a newer Reconcile.
func (d *dnsEndpointUpdater) SetRecordSpec(hostname string, ttl int64, generation int64) {
	d.mu.Lock()
	defer d.mu.Unlock()
	if generation < d.generation {
		return
	}
	if d.hostname != hostname || d.ttl != ttl {
		d.needsApply = true
	}
	d.generation = generation
	d.hostname = hostname
	d.ttl = ttl
}

func (d *dnsEndpointUpdater) RecordSpec() (string, int64) {
	d.mu.Lock()
	defer d.mu.Unlock()
	return d.hostname, d.ttl
}

func (d *dnsEndpointUpdater) SpecNeedsApply() bool {
	d.mu.Lock()
	defer d.mu.Unlock()
	return d.needsApply
}

func (d *dnsEndpointUpdater) snapshotSpec() (hostname string, ttl int64, generation int64) {
	d.mu.Lock()
	defer d.mu.Unlock()
	return d.hostname, d.ttl, d.generation
}

func (d *dnsEndpointUpdater) UpdateDNSRecord(ctx context.Context, ip string) error {
	// Re-apply if SetRecordSpec races in a newer generation while this
	// write is in flight, so a delayed SSA cannot restore a stale name.
	const maxAttempts = 3
	for attempt := 0; attempt < maxAttempts; attempt++ {
		hostname, ttl, gen := d.snapshotSpec()
		if err := d.applyEndpoint(ctx, hostname, ttl, ip); err != nil {
			return err
		}
		d.mu.Lock()
		caughtUp := d.generation == gen
		if caughtUp {
			d.needsApply = false
		}
		d.mu.Unlock()
		if caughtUp {
			return nil
		}
	}
	return nil
}

func (d *dnsEndpointUpdater) applyEndpoint(ctx context.Context, hostname string, ttl int64, ip string) error {
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
						"dnsName":    hostname,
						"recordType": "A",
						"targets":    []interface{}{ip},
						"recordTTL":  ttl,
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
	want, _, _ := d.snapshotSpec()
	endpoints, found, err := d.readEndpoints(ctx)
	if err != nil || !found {
		return "", found, err
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
		if name != want {
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

// CurrentDNSEndpoint returns the first endpoint on the managed object
// without filtering by the desired hostname. That is what lets
// reconcileDNS see a rename: the live dnsName is the old name, the
// desired spec is the new one, and the record is rewritten.
func (d *dnsEndpointUpdater) CurrentDNSEndpoint(ctx context.Context) (string, string, int64, bool, error) {
	endpoints, found, err := d.readEndpoints(ctx)
	if err != nil || !found {
		return "", "", 0, found, err
	}
	for _, e := range endpoints {
		ep, ok := e.(map[string]interface{})
		if !ok {
			continue
		}
		name, target, ttl, err := parseEndpoint(ep)
		if err != nil {
			return "", "", 0, false, fmt.Errorf("read DNSEndpoint %s/%s endpoint %s: %w", d.namespace, d.name, fieldHint(err), err)
		}
		if target == "" {
			return name, "", ttl, false, nil
		}
		return name, target, ttl, true, nil
	}
	return "", "", 0, false, nil
}

func (d *dnsEndpointUpdater) readEndpoints(ctx context.Context) ([]interface{}, bool, error) {
	obj := &unstructured.Unstructured{}
	obj.SetGroupVersionKind(schema.GroupVersionKind{
		Group:   "externaldns.k8s.io",
		Version: "v1alpha1",
		Kind:    "DNSEndpoint",
	})
	key := client.ObjectKey{Namespace: d.namespace, Name: d.name}
	if err := d.client.Get(ctx, key, obj); err != nil {
		if apierrors.IsNotFound(err) {
			return nil, false, nil
		}
		return nil, false, fmt.Errorf("get DNSEndpoint %s/%s: %w", d.namespace, d.name, err)
	}

	endpoints, found, err := unstructured.NestedSlice(obj.Object, "spec", "endpoints")
	if err != nil {
		return nil, false, fmt.Errorf("read DNSEndpoint %s/%s spec.endpoints: %w", d.namespace, d.name, err)
	}
	if !found {
		return nil, false, nil
	}
	return endpoints, true, nil
}

type endpointFieldError struct {
	field string
	err   error
}

func (e *endpointFieldError) Error() string { return e.err.Error() }
func (e *endpointFieldError) Unwrap() error { return e.err }

func fieldHint(err error) string {
	if e, ok := err.(*endpointFieldError); ok {
		return e.field
	}
	return "fields"
}

func parseEndpoint(ep map[string]interface{}) (name, target string, ttl int64, err error) {
	name, _, err = unstructured.NestedString(ep, "dnsName")
	if err != nil {
		return "", "", 0, &endpointFieldError{field: "dnsName", err: err}
	}
	targets, _, err := unstructured.NestedStringSlice(ep, "targets")
	if err != nil {
		return "", "", 0, &endpointFieldError{field: "targets", err: err}
	}
	if len(targets) > 0 {
		target = targets[0]
	}
	ttl, err = nestedRecordTTL(ep)
	if err != nil {
		return "", "", 0, &endpointFieldError{field: "recordTTL", err: err}
	}
	return name, target, ttl, nil
}

// nestedRecordTTL reads recordTTL from an unstructured endpoint. After a
// real API round-trip JSON numbers decode as float64; the fake client
// preserves the int64 we wrote. Accept both, plus json.Number.
func nestedRecordTTL(ep map[string]interface{}) (int64, error) {
	v, found, err := unstructured.NestedFieldNoCopy(ep, "recordTTL")
	if err != nil {
		return 0, err
	}
	if !found || v == nil {
		return 0, nil
	}
	switch n := v.(type) {
	case int64:
		return n, nil
	case int32:
		return int64(n), nil
	case int:
		return int64(n), nil
	case float64:
		return int64(n), nil
	case float32:
		return int64(n), nil
	case json.Number:
		i, err := n.Int64()
		if err != nil {
			return 0, fmt.Errorf("recordTTL: %w", err)
		}
		return i, nil
	default:
		return 0, fmt.Errorf("recordTTL is %T, not a number", v)
	}
}

// Verify the interfaces are satisfied at compile time.
var (
	_ DNSUpdater        = (*dnsEndpointUpdater)(nil)
	_ DNSRecordReader   = (*dnsEndpointUpdater)(nil)
	_ DNSSpecController = (*dnsEndpointUpdater)(nil)
)
