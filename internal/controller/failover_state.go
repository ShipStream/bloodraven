package controller

import (
	"context"
	"encoding/json"
	"errors"
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

	// FailoverClockSkewGrace bounds how far either durable timestamp may be
	// ahead of the reader's clock. A larger jump is unsafe to install because
	// the cooldown gate treats negative elapsed time as still active.
	FailoverClockSkewGrace = 5 * time.Minute
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
	annotations := map[string]any{}
	if rec.LastFailover.IsZero() {
		if rec.LastFailoverTarget != "" {
			return fmt.Errorf("failover-state record has target %q without a timestamp", rec.LastFailoverTarget)
		}
		// JSON merge patch deletes map keys only when their values are null.
		// Empty strings leave the annotations present and can let a later
		// status-only clear be resurrected by the out-of-band copy.
		annotations[LastFailoverAnnotation] = nil
		annotations[LastFailoverTargetAnnotation] = nil
	} else {
		encoded, err := FailoverRecordAnnotations(rec)
		if err != nil {
			return err
		}
		for key, value := range encoded {
			annotations[key] = value
		}
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

// FailoverRecordAnnotations encodes a non-zero record exactly as the
// production annotation writer does. The simulator uses the same helper so
// its durable-store seam exercises the real precision and parsing contract.
func FailoverRecordAnnotations(rec FailoverRecord) (map[string]string, error) {
	if rec.LastFailover.IsZero() {
		return nil, fmt.Errorf("cannot encode a zero failover record; use ClearFailoverState to delete it")
	}
	if rec.LastFailoverTarget == "" {
		return nil, fmt.Errorf("failover-state record has timestamp %s without a target", rec.LastFailover.UTC().Format(time.RFC3339Nano))
	}
	return map[string]string{
		LastFailoverAnnotation:       failoverStampFormat(rec.LastFailover),
		LastFailoverTargetAnnotation: rec.LastFailoverTarget,
	}, nil
}

// ClearFailoverState deletes both out-of-band anti-flap annotations. Callers
// that intentionally clear status.lastFailover and status.lastFailoverTarget
// must call this while the operator is stopped, otherwise the running manager
// can immediately repopulate either durable copy.
func ClearFailoverState(ctx context.Context, c client.Client, nn types.NamespacedName) error {
	return NewAnnotationFailoverStateRecorder(c, nn).RecordFailoverState(ctx, FailoverRecord{})
}

// failoverStampFormat renders a promotion instant for the annotation, at the
// same UTC second precision metav1.Time gives status.lastFailover.
func failoverStampFormat(t time.Time) string {
	return t.UTC().Format(time.RFC3339)
}

// FailoverRecordFromAnnotations reads the out-of-band anti-flap record off a
// failover group's annotations. A missing timestamp yields the zero time
// (no history); an unparseable one is an error, because silently treating
// corrupt state as "no history" is what resets the cooldown.
//
// Parsing accepts RFC3339Nano defensively, but normalizes it to the same
// second precision as the write path and metav1.Time status copy.
func FailoverRecordFromAnnotations(annotations map[string]string) (FailoverRecord, error) {
	var rec FailoverRecord
	if len(annotations) == 0 {
		return rec, nil
	}
	rec.LastFailoverTarget = annotations[LastFailoverTargetAnnotation]
	raw := annotations[LastFailoverAnnotation]
	if raw == "" {
		if rec.LastFailoverTarget != "" {
			return FailoverRecord{}, fmt.Errorf("%s annotation is set without %s", LastFailoverTargetAnnotation, LastFailoverAnnotation)
		}
		return rec, nil
	}
	if rec.LastFailoverTarget == "" {
		return FailoverRecord{}, fmt.Errorf("%s annotation is set without %s", LastFailoverAnnotation, LastFailoverTargetAnnotation)
	}
	t, err := time.Parse(time.RFC3339Nano, raw)
	if err != nil {
		return FailoverRecord{}, fmt.Errorf("parse %s annotation %q: %w", LastFailoverAnnotation, raw, err)
	}
	rec.LastFailover = t.UTC().Truncate(time.Second)
	return rec, nil
}

// FailoverRecordFromStatus reads the status-subresource copy of the durable
// anti-flap record.
func FailoverRecordFromStatus(fg *v1alpha1.MysqlFailoverGroup) FailoverRecord {
	rec := FailoverRecord{LastFailoverTarget: fg.Status.LastFailoverTarget}
	if fg.Status.LastFailover != nil && !fg.Status.LastFailover.IsZero() {
		rec.LastFailover = fg.Status.LastFailover.Time.UTC().Truncate(time.Second)
	}
	return rec
}

// FailoverRecordReadError reports invalid durable copies while allowing the
// caller to continue with any valid copy that remains.
type FailoverRecordReadError struct {
	Status      error
	Annotations error
}

func (e *FailoverRecordReadError) Error() string {
	if joined := errors.Join(e.Status, e.Annotations); joined != nil {
		return joined.Error()
	}
	return ""
}

// EffectiveFailoverRecord returns the newest safe durable copy. Malformed,
// unpaired, or implausibly future-dated copies are ignored independently so
// one bad path cannot poison the cooldown or discard the other good path.
func EffectiveFailoverRecord(fg *v1alpha1.MysqlFailoverGroup, now time.Time) (FailoverRecord, bool, error) {
	statusRecord := FailoverRecordFromStatus(fg)
	oobRecord, oobErr := FailoverRecordFromAnnotations(fg.GetAnnotations())
	readErr := &FailoverRecordReadError{Annotations: oobErr}

	limit := now.Add(FailoverClockSkewGrace)
	if !statusRecord.LastFailover.IsZero() && statusRecord.LastFailover.After(limit) {
		readErr.Status = fmt.Errorf("status.lastFailover %s is more than %s ahead of local time %s",
			statusRecord.LastFailover.Format(time.RFC3339), FailoverClockSkewGrace, now.UTC().Format(time.RFC3339))
		statusRecord = FailoverRecord{}
	}
	if oobErr == nil && !oobRecord.LastFailover.IsZero() && oobRecord.LastFailover.After(limit) {
		readErr.Annotations = fmt.Errorf("%s annotation %s is more than %s ahead of local time %s",
			LastFailoverAnnotation, oobRecord.LastFailover.Format(time.RFC3339), FailoverClockSkewGrace, now.UTC().Format(time.RFC3339))
		oobRecord = FailoverRecord{}
	}
	if statusRecord.LastFailoverTarget != "" && !failoverTargetInSpec(fg, statusRecord.LastFailoverTarget) {
		readErr.Status = errors.Join(readErr.Status,
			fmt.Errorf("status.lastFailoverTarget %q is not present in spec.sites", statusRecord.LastFailoverTarget))
		statusRecord = FailoverRecord{}
	}
	if oobErr == nil && oobRecord.LastFailoverTarget != "" && !failoverTargetInSpec(fg, oobRecord.LastFailoverTarget) {
		readErr.Annotations = errors.Join(readErr.Annotations,
			fmt.Errorf("%s annotation target %q is not present in spec.sites", LastFailoverTargetAnnotation, oobRecord.LastFailoverTarget))
		oobRecord = FailoverRecord{}
	}

	winner := NewerFailoverRecord(statusRecord, oobRecord)
	fromAnnotations := winner != statusRecord
	if readErr.Status == nil && readErr.Annotations == nil {
		return winner, fromAnnotations, nil
	}
	return winner, fromAnnotations, readErr
}

func failoverTargetInSpec(fg *v1alpha1.MysqlFailoverGroup, target string) bool {
	for i := range fg.Spec.Sites {
		if fg.Spec.Sites[i].Name == target {
			return true
		}
	}
	return false
}

// NewerFailoverRecord returns whichever record was stamped later.
//
// The two durable paths fail independently, so either can be the fresher
// one after a restart: status is ahead when the annotation write was
// rejected, the annotation is ahead when the status write was. Ties go to
// the first argument, which callers pass as the CR status copy — when both
// carry the same instant they describe the same promotion.
//
// That tie rule rests on a precondition: no two promotions share a truncated
// second. Both paths store second precision (see the annotation doc above),
// so two promotions inside one second would be indistinguishable here, and a
// tie between them would be broken arbitrarily rather than correctly — the
// rehydrated LastFailoverTarget could name the earlier one. The precondition
// holds because a promotion is several MySQL round-trips (fence, GTID wait,
// writable grant) and the paths that can promote back to back — planned
// switchover and the ordered-update handoff — each complete one before
// starting the next. The exposure if it were ever violated is bounded to a
// stale fencing target for one cooldown window: the timestamps are equal, so
// the cooldown deadline itself is correct either way.
func NewerFailoverRecord(a, b FailoverRecord) FailoverRecord {
	if b.LastFailover.After(a.LastFailover) {
		return b
	}
	return a
}
