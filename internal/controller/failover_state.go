package controller

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/shipstream/bloodraven/api/v1alpha1"
	"github.com/shipstream/bloodraven/internal/clock"
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

// maxPromotionGtidAnnotationBytes bounds the out-of-band watermark. A GTID
// set this large means thousands of intervals; such a watermark is left
// out of the annotation and the status copy is the only durable one.
const maxPromotionGtidAnnotationBytes = 64 << 10

// FailoverRecord is the anti-flap state that must survive an operator
// restart: when the last promotion happened, which site it landed on, and
// the promotion watermark (the GTID set the promoted site held when it
// took writes). The watermark rides with the record so a status outage
// after a promotion cannot leave an older watermark in force.
type FailoverRecord struct {
	LastFailover          time.Time
	LastFailoverTarget    string
	PromotionGtidExecuted string
}

// IsZero reports whether the record carries no failover history at all.
func (r FailoverRecord) IsZero() bool {
	return r.LastFailover.IsZero() && r.LastFailoverTarget == ""
}

// sameEvent reports whether r and o describe the same promotion: equal
// time (second precision) and target, whatever their watermarks.
func (r FailoverRecord) sameEvent(o FailoverRecord) bool {
	return r.LastFailover.UTC().Truncate(time.Second).Equal(o.LastFailover.UTC().Truncate(time.Second)) &&
		r.LastFailoverTarget == o.LastFailoverTarget
}

// encodableWatermark is the watermark the annotation can carry: "" when it
// exceeds maxPromotionGtidAnnotationBytes.
func (r FailoverRecord) encodableWatermark() string {
	if len(r.PromotionGtidExecuted) > maxPromotionGtidAnnotationBytes {
		return ""
	}
	return r.PromotionGtidExecuted
}

// failoverStampFutureDated reports whether a stored lastFailover is more
// than FailoverClockSkewGrace ahead of the writer's clock now (zero: no
// clock, never). EffectiveFailoverRecord ignores such a copy on restore, so
// a promotion it no longer blocks must not then be refused by the writers:
// they treat it as absent and write over it (K023).
func failoverStampFutureDated(stored, now time.Time) bool {
	return !now.IsZero() && !stored.IsZero() && stored.After(now.Add(FailoverClockSkewGrace))
}

// supersedes reports whether writing r over stored moves the out-of-band
// record forward: stored is older (second precision), or as old with a
// different target, or as old and the same but without a watermark r can
// supply, or implausibly future-dated against now. An older or equal
// record is never written over a newer one.
func (r FailoverRecord) supersedes(stored FailoverRecord, now time.Time) bool {
	at := r.LastFailover.UTC().Truncate(time.Second)
	st := stored.LastFailover.UTC().Truncate(time.Second)
	switch {
	case stored.IsZero():
		return true
	case failoverStampFutureDated(st, now):
		return true
	case st.After(at):
		return false
	case st.Before(at):
		return true
	case stored.LastFailoverTarget != r.LastFailoverTarget:
		return true
	}
	return stored.PromotionGtidExecuted == "" && r.encodableWatermark() != ""
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
	reader client.Reader
	nn     types.NamespacedName
	clock  clock.Clock // the writer's clock for the future-dated check
}

// failoverStateConflictRetries bounds the conditioned write's retries on a
// conflict; a write still conflicting is left dirty for the per-poll retry.
const failoverStateConflictRetries = 3

// NewAnnotationFailoverStateRecorder returns the production out-of-band
// anti-flap store: the failover group's own object annotations. reader must
// be uncached (the runner passes its APIReader): the write is conditioned
// on what it reads.
func NewAnnotationFailoverStateRecorder(c client.Client, reader client.Reader, nn types.NamespacedName) FailoverStateRecorder {
	return NewAnnotationFailoverStateRecorderWithClock(c, reader, nn, clock.RealClock{})
}

// NewAnnotationFailoverStateRecorderWithClock is NewAnnotationFailoverStateRecorder
// with the writer's clock injected. The future-dated check (supersedes) must
// use the same clock as the manager that reads the record back through
// EffectiveFailoverRecord: against another clock a stored record stamped by
// the manager looks future-dated and is written over by an older one.
func NewAnnotationFailoverStateRecorderWithClock(c client.Client, reader client.Reader, nn types.NamespacedName,
	clk clock.Clock) FailoverStateRecorder {
	if reader == nil {
		reader = c
	}
	return &annotationFailoverStateRecorder{client: c, reader: reader, nn: nn, clock: clk}
}

// RecordFailoverState writes the record's annotations as a read-compare-
// patch: an uncached read, then a merge patch conditioned on the read's
// resourceVersion (MergeFromWithOptimisticLock). It writes only when rec
// supersedes the stored record (FailoverRecord.supersedes) and is otherwise
// a no-op success, so a late write from an older process can no longer
// overwrite a newer record. A conflict re-reads and retries, at most
// failoverStateConflictRetries times. Only the owned keys change; every
// other annotation on the object is left alone.
//
// The zero record deletes the owned annotations unconditionally (an
// intentional clear, see ClearFailoverState).
func (s *annotationFailoverStateRecorder) RecordFailoverState(ctx context.Context, rec FailoverRecord) error {
	var encoded map[string]string
	if rec.LastFailover.IsZero() {
		if rec.LastFailoverTarget != "" {
			return fmt.Errorf("failover-state record has target %q without a timestamp", rec.LastFailoverTarget)
		}
	} else {
		var err error
		if encoded, err = FailoverRecordAnnotations(rec); err != nil {
			return err
		}
	}
	var lastErr error
	for attempt := 0; attempt < failoverStateConflictRetries; attempt++ {
		var cur v1alpha1.MysqlFailoverGroup
		if err := s.reader.Get(ctx, s.nn, &cur); err != nil {
			return err
		}
		if encoded != nil {
			// A corrupt stored record is overwritten: it is no record.
			if stored, err := FailoverRecordFromAnnotations(cur.GetAnnotations()); err == nil && !rec.supersedes(stored, s.clock.Now()) {
				return nil
			}
		}
		base := cur.DeepCopy()
		ann := make(map[string]string, len(cur.GetAnnotations())+3)
		for k, v := range cur.GetAnnotations() {
			ann[k] = v
		}
		delete(ann, LastFailoverAnnotation)
		delete(ann, LastFailoverTargetAnnotation)
		delete(ann, PromotionGtidExecutedAnnotation)
		delete(ann, PromotionGtidExecutedForAnnotation)
		for k, v := range encoded {
			ann[k] = v
		}
		cur.SetAnnotations(ann)
		err := s.client.Patch(ctx, &cur, client.MergeFromWithOptions(base, client.MergeFromWithOptimisticLock{}))
		if !apierrors.IsConflict(err) {
			return err
		}
		lastErr = err
	}
	return fmt.Errorf("out-of-band failover record kept conflicting: %w", lastErr)
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
	out := map[string]string{
		LastFailoverAnnotation:       failoverStampFormat(rec.LastFailover),
		LastFailoverTargetAnnotation: rec.LastFailoverTarget,
	}
	if w := rec.encodableWatermark(); w != "" {
		out[PromotionGtidExecutedAnnotation] = w
		out[PromotionGtidExecutedForAnnotation] = failoverWatermarkBinding(rec.LastFailover, rec.LastFailoverTarget)
	}
	return out, nil
}

// ClearFailoverState deletes the out-of-band anti-flap annotations. Callers
// that intentionally clear status.lastFailover and status.lastFailoverTarget
// must call this while the operator is stopped, otherwise the running manager
// can immediately repopulate either durable copy.
func ClearFailoverState(ctx context.Context, c client.Client, nn types.NamespacedName) error {
	return NewAnnotationFailoverStateRecorder(c, c, nn).RecordFailoverState(ctx, FailoverRecord{})
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
	// The watermark counts only when its binding names this event (K095):
	// a writer that knows only the two failover keys can advance them
	// beside an older event's watermark.
	if annotations[PromotionGtidExecutedForAnnotation] == failoverWatermarkBinding(rec.LastFailover, rec.LastFailoverTarget) {
		rec.PromotionGtidExecuted = annotations[PromotionGtidExecutedAnnotation]
	}
	return rec, nil
}

// failoverWatermarkBinding is the PromotionGtidExecutedForAnnotation value
// for the event {at, target}.
func failoverWatermarkBinding(at time.Time, target string) string {
	return failoverStampFormat(at) + " " + target
}

// FailoverRecordFromStatus reads the status-subresource copy of the durable
// anti-flap record.
func FailoverRecordFromStatus(fg *v1alpha1.MysqlFailoverGroup) FailoverRecord {
	rec := FailoverRecord{LastFailoverTarget: fg.Status.LastFailoverTarget, PromotionGtidExecuted: fg.Status.PromotionGtidExecuted}
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

// plannedFailoverInternalAnnotations are the planned failover's
// operator-internal and acknowledgement annotations.
var plannedFailoverInternalAnnotations = []string{
	PlannedFailoverStatusFenceAnnotation,
	PlannedFailoverTerminalAnnotation,
	AcknowledgePlannedFailoverHoldAnnotation,
	PlannedFailoverStallSinceAnnotation,
	PlannedFailoverManagerAbsentSinceAnnotation,
}

// ClearPlannedFailoverAnnotations deletes the planned failover's internal
// and acknowledgement annotations. A reset that clears status.plannedFailover
// and the conditions must call it too: a terminal marker left behind with
// outcome ManualRecoveryRequired rebuilds an automation hold on the next
// operator start. Like ClearFailoverState, call it with the operator
// stopped.
func ClearPlannedFailoverAnnotations(ctx context.Context, c client.Client, nn types.NamespacedName) error {
	annotations := make(map[string]any, len(plannedFailoverInternalAnnotations))
	for _, k := range plannedFailoverInternalAnnotations {
		annotations[k] = nil
	}
	patch, err := json.Marshal(map[string]any{"metadata": map[string]any{"annotations": annotations}})
	if err != nil {
		return fmt.Errorf("marshal planned-failover annotation patch: %w", err)
	}
	obj := &v1alpha1.MysqlFailoverGroup{}
	obj.Namespace, obj.Name = nn.Namespace, nn.Name
	return c.Patch(ctx, obj, client.RawPatch(types.MergePatchType, patch))
}
