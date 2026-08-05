package controller

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/shipstream/bloodraven/api/v1alpha1"
)

const (
	// LastFailoverAnnotation and LastFailoverTargetAnnotation carry the
	// anti-flap record on the failover group's OBJECT metadata, alongside
	// the copy in status.lastFailover / status.lastFailoverTarget.
	//
	// The duplication is the point. status is a subresource: writes to it
	// travel a separate API path with its own RBAC rule
	// (mysqlfailovergroups/status) and its own admission plugins, so a
	// status-write outage can persist while ordinary object writes succeed.
	// When that outage overlaps an operator restart, the restarted process
	// rehydrates a stale cooldown and can promote inside the window the
	// previous process intended to enforce — the CooldownViolated(restart)
	// class the deterministic simulator surfaces. These annotations are the
	// second, independently-failing durable path that closes it.
	//
	// Format: RFC3339 UTC at SECOND precision, and the target site name
	// verbatim. Both are written together on every promotion.
	//
	// The precision is deliberately matched to metav1.Time, which is what
	// status.lastFailover serializes as. Storing nanoseconds here would make
	// the annotation strictly newer than the status copy of the very same
	// promotion, so every restart would "fall back" to the out-of-band path
	// and log the warning that is supposed to mean status writes are
	// failing. Matching precision makes the common case an exact tie.
	LastFailoverAnnotation       = "bloodraven.shipstream.io/last-failover"
	LastFailoverTargetAnnotation = "bloodraven.shipstream.io/last-failover-target"
)

// failoverStateWriteTimeout bounds one out-of-band write. It runs inline on
// the poll goroutine immediately after a promotion, so it must not be able
// to stall the loop indefinitely; a write that misses this budget is retried
// on the next poll by the pending-record path in Poll.
//
// The promotion path waits for an in-flight write before starting its own,
// so its worst case is TWO of these budgets back to back (~20s): one
// draining the write it queued behind, one for its own API call. That only
// happens when two promotions race, and a bounded post-promotion delay is
// preferred over a promotion whose record silently loses the race.
const failoverStateWriteTimeout = 10 * time.Second

// FailoverRecord is the anti-flap state that must survive an operator
// restart: when the last promotion happened, and which site it landed on.
type FailoverRecord struct {
	LastFailover       time.Time
	LastFailoverTarget string
}

// IsZero reports whether the record carries no failover history at all.
func (r FailoverRecord) IsZero() bool {
	return r.LastFailover.IsZero() && r.LastFailoverTarget == ""
}

// FailoverStateRecorder persists a FailoverRecord out of band from the CR
// status subresource. Implemented in production by the annotation store
// below and in the deterministic simulator by a store whose availability is
// modeled independently of simulated status writes — which is the whole
// reason the seam exists.
type FailoverStateRecorder interface {
	RecordFailoverState(ctx context.Context, rec FailoverRecord) error
}

// annotationFailoverStateRecorder writes the record to the failover group's
// annotations.
type annotationFailoverStateRecorder struct {
	client client.Client
	nn     types.NamespacedName
}

// NewAnnotationFailoverStateRecorder returns the production out-of-band
// anti-flap store: the failover group's own object annotations.
func NewAnnotationFailoverStateRecorder(c client.Client, nn types.NamespacedName) FailoverStateRecorder {
	return &annotationFailoverStateRecorder{client: c, nn: nn}
}

// RecordFailoverState stamps both annotations in one JSON merge patch.
//
// A merge patch rather than get-modify-update: it carries no
// resourceVersion, so it cannot conflict with a concurrent writer and needs
// no retry loop, and it touches only the two keys it names — every other
// annotation on the object is left alone. That matters because the reclone,
// planned-failover, and keyring-rotation flows all write annotations on this
// same object.
func (s *annotationFailoverStateRecorder) RecordFailoverState(ctx context.Context, rec FailoverRecord) error {
	annotations := map[string]string{
		LastFailoverTargetAnnotation: rec.LastFailoverTarget,
	}
	if rec.LastFailover.IsZero() {
		annotations[LastFailoverAnnotation] = ""
	} else {
		annotations[LastFailoverAnnotation] = failoverStampFormat(rec.LastFailover)
	}

	patch, err := json.Marshal(map[string]any{
		"metadata": map[string]any{"annotations": annotations},
	})
	if err != nil {
		return fmt.Errorf("marshal failover-state annotation patch: %w", err)
	}

	obj := &v1alpha1.MysqlFailoverGroup{}
	obj.Namespace = s.nn.Namespace
	obj.Name = s.nn.Name
	return s.client.Patch(ctx, obj, client.RawPatch(types.MergePatchType, patch))
}

// failoverStampFormat renders a promotion instant for the annotation, at the
// same UTC second precision metav1.Time gives status.lastFailover.
func failoverStampFormat(t time.Time) string {
	return t.UTC().Truncate(time.Second).Format(time.RFC3339)
}

// FailoverRecordFromAnnotations reads the out-of-band anti-flap record off a
// failover group's annotations. A missing timestamp yields the zero time
// (no history); an unparseable one is an error, because silently treating
// corrupt state as "no history" is what resets the cooldown.
//
// Parsing accepts RFC3339Nano so a record written by an older build (which
// stamped nanoseconds) still reads back correctly on upgrade.
func FailoverRecordFromAnnotations(annotations map[string]string) (FailoverRecord, error) {
	var rec FailoverRecord
	if len(annotations) == 0 {
		return rec, nil
	}
	rec.LastFailoverTarget = annotations[LastFailoverTargetAnnotation]
	raw := annotations[LastFailoverAnnotation]
	if raw == "" {
		return rec, nil
	}
	t, err := time.Parse(time.RFC3339Nano, raw)
	if err != nil {
		return FailoverRecord{}, fmt.Errorf("parse %s annotation %q: %w", LastFailoverAnnotation, raw, err)
	}
	rec.LastFailover = t.UTC()
	return rec, nil
}

// NewerFailoverRecord returns whichever record was stamped later.
//
// The two durable paths fail independently, so either can be the fresher
// one after a restart: status is ahead when the annotation write was
// rejected, the annotation is ahead when the status write was. Ties go to
// the first argument, which callers pass as the CR status copy — when both
// carry the same instant they describe the same promotion.
func NewerFailoverRecord(a, b FailoverRecord) FailoverRecord {
	if b.LastFailover.After(a.LastFailover) {
		return b
	}
	return a
}
