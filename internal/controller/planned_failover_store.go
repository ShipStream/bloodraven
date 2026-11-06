// Package controller — the planned-failover status store.
//
// apiPlannedFailoverStore is the planned-failover engine's only path to
// the API server. Every read is uncached (the manager's APIReader), and
// every write is conditioned on the resourceVersion of such a read:
// status writes are Status().Update of the loaded object, metadata writes
// are merge patches with MergeFromWithOptimisticLock. An ambiguous status
// write (timeout, reset, 5xx) is settled before Write returns: either the
// block shows the write's (runId, revision), or the object is moved past
// the write's base version by a metadata fence, after which the write can
// never commit.
package controller

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/log"

	v1alpha1 "github.com/shipstream/bloodraven/api/v1alpha1"
	"github.com/shipstream/bloodraven/internal/clock"
)

const (
	// plannedStoreSettleAttemptTimeout bounds one settlement attempt.
	plannedStoreSettleAttemptTimeout = 10 * time.Second
	// plannedStoreSettleAttempts bounds the settlement retries after a
	// conflicting fence (and the reads that precede it).
	plannedStoreSettleAttempts = 3
)

// apiPlannedFailoverStore implements PlannedFailoverStore over a
// controller-runtime client (writes) and an uncached reader (reads).
type apiPlannedFailoverStore struct {
	client client.Client
	reader client.Reader
	clk    clock.Clock
}

var _ PlannedFailoverStore = (*apiPlannedFailoverStore)(nil)

// newAPIPlannedFailoverStore returns a store. reader must bypass the
// informer cache in production; nil falls back to c (unit tests).
func newAPIPlannedFailoverStore(c client.Client, reader client.Reader, clk clock.Clock) *apiPlannedFailoverStore {
	if reader == nil {
		reader = c
	}
	if clk == nil {
		clk = clock.RealClock{}
	}
	return &apiPlannedFailoverStore{client: c, reader: reader, clk: clk}
}

// Load returns the uncached object, or (nil, nil) when it is gone.
func (s *apiPlannedFailoverStore) Load(ctx context.Context, nn types.NamespacedName) (*v1alpha1.MysqlFailoverGroup, error) {
	var fg v1alpha1.MysqlFailoverGroup
	if err := s.reader.Get(ctx, nn, &fg); err != nil {
		if apierrors.IsNotFound(err) {
			return nil, nil
		}
		return nil, err
	}
	return &fg, nil
}

// Write commits next (and mutate) with Status().Update conditioned on
// base.ResourceVersion. base is not modified.
func (s *apiPlannedFailoverStore) Write(ctx context.Context, base *v1alpha1.MysqlFailoverGroup, next v1alpha1.PlannedFailoverStatus,
	mutate func(*v1alpha1.MysqlFailoverGroupStatus)) (WriteOutcome, error) {
	obj := base.DeepCopy()
	obj.Status.PlannedFailover = next.DeepCopy()
	if mutate != nil {
		mutate(&obj.Status)
	}
	err := s.client.Status().Update(ctx, obj)
	outcome, definite := classifyPlannedStatusWrite(err)
	if definite {
		return outcome, err
	}
	nn := client.ObjectKeyFromObject(base)
	log.FromContext(ctx).Info("planned failover: status write ambiguous; settling",
		"fg", nn.String(), "runId", next.RunID, "revision", next.Revision, "error", err.Error())
	settled, serr := s.settle(ctx, nn, base.ResourceVersion, func(cur *v1alpha1.MysqlFailoverGroup) bool {
		pf := cur.Status.PlannedFailover
		return pf != nil && pf.RunID == next.RunID && pf.Revision >= next.Revision
	})
	if settled == WriteCommitted {
		return WriteCommitted, nil
	}
	if serr == nil {
		serr = err
	}
	return settled, serr
}

// Settle decides an ambiguous write of (runID, revision) made against baseRV.
func (s *apiPlannedFailoverStore) Settle(ctx context.Context, nn types.NamespacedName, baseRV, runID string, revision int64) (WriteOutcome, error) {
	return s.settle(ctx, nn, baseRV, func(cur *v1alpha1.MysqlFailoverGroup) bool {
		pf := cur.Status.PlannedFailover
		return pf != nil && pf.RunID == runID && pf.Revision >= revision
	})
}

// settle is §2.4 step 5 with the commit test supplied by the caller: read
// uncached; committed means the write landed; a moved resourceVersion
// means the write (conditioned on baseRV) can never land; an unmoved one
// is moved by a conditioned metadata fence. Each attempt is detached from
// ctx's cancellation and bounded at 10s.
func (s *apiPlannedFailoverStore) settle(ctx context.Context, nn types.NamespacedName, baseRV string,
	committed func(*v1alpha1.MysqlFailoverGroup) bool) (WriteOutcome, error) {
	detached := context.WithoutCancel(ctx)
	var lastErr error
	for attempt := 0; attempt < plannedStoreSettleAttempts; attempt++ {
		outcome, err, retry := s.settleOnce(detached, nn, baseRV, committed)
		if !retry {
			return outcome, err
		}
		lastErr = err
	}
	if lastErr == nil {
		lastErr = errors.New("planned failover: settlement exhausted its attempts")
	}
	return WriteUnknown, lastErr
}

func (s *apiPlannedFailoverStore) settleOnce(ctx context.Context, nn types.NamespacedName, baseRV string,
	committed func(*v1alpha1.MysqlFailoverGroup) bool) (WriteOutcome, error, bool) {
	actx, cancel := context.WithTimeout(ctx, plannedStoreSettleAttemptTimeout)
	defer cancel()
	cur, err := s.Load(actx, nn)
	if err != nil {
		return WriteUnknown, fmt.Errorf("settle: read: %w", err), true
	}
	if cur == nil {
		return WriteGone, errors.New("settle: the MysqlFailoverGroup is gone"), false
	}
	if committed(cur) {
		return WriteCommitted, nil, false
	}
	if cur.ResourceVersion != baseRV {
		return WriteNotCommitted, errors.New("settle: the object moved past the write's base version; the write can never commit"), false
	}
	fenced := cur.DeepCopy()
	if fenced.Annotations == nil {
		fenced.Annotations = map[string]string{}
	}
	fenced.Annotations[PlannedFailoverStatusFenceAnnotation] = FormatStatusFenceValue(s.clk.Now())
	patch := client.MergeFromWithOptions(cur.DeepCopy(), client.MergeFromWithOptimisticLock{})
	switch err := s.client.Patch(actx, fenced, patch); {
	case err == nil:
		return WriteNotCommitted, errors.New("settle: fenced the write's base version; the write can never commit"), false
	case apierrors.IsConflict(err):
		return WriteUnknown, fmt.Errorf("settle: fence conflicted: %w", err), true
	case apierrors.IsNotFound(err):
		return WriteGone, err, false
	default:
		return WriteUnknown, fmt.Errorf("settle: fence: %w", err), false
	}
}

// classifyPlannedStatusWrite maps a write error to its outcome. definite is
// false for an ambiguous error, which the caller must settle.
func classifyPlannedStatusWrite(err error) (WriteOutcome, bool) {
	switch {
	case err == nil:
		return WriteCommitted, true
	case apierrors.IsConflict(err):
		return WriteConflict, true
	case apierrors.IsNotFound(err):
		return WriteGone, true
	case apierrors.IsForbidden(err), apierrors.IsInvalid(err), apierrors.IsBadRequest(err):
		return WriteNotCommitted, true
	}
	return WriteUnknown, false
}

// classifyPlannedMetaWrite maps a metadata patch error to its outcome. A
// metadata write is idempotent and re-derived on the next pass, so an
// ambiguous one is reported Unknown without settlement.
func classifyPlannedMetaWrite(err error) WriteOutcome {
	if o, definite := classifyPlannedStatusWrite(err); definite {
		return o
	}
	return WriteUnknown
}

// patchAnnotations applies edit to a copy of base's annotations and, if it
// changed anything, patches them conditioned on base.ResourceVersion.
func (s *apiPlannedFailoverStore) patchAnnotations(ctx context.Context, base *v1alpha1.MysqlFailoverGroup,
	edit func(map[string]string) bool) (WriteOutcome, error) {
	obj := base.DeepCopy()
	if obj.Annotations == nil {
		obj.Annotations = map[string]string{}
	}
	if !edit(obj.Annotations) {
		return WriteCommitted, nil
	}
	patch := client.MergeFromWithOptions(base.DeepCopy(), client.MergeFromWithOptimisticLock{})
	err := s.client.Patch(ctx, obj, patch)
	return classifyPlannedMetaWrite(err), err
}

// WriteTerminalMarker records m in the planned-failover-terminal annotation.
func (s *apiPlannedFailoverStore) WriteTerminalMarker(ctx context.Context, base *v1alpha1.MysqlFailoverGroup, m TerminalMarker) (WriteOutcome, error) {
	return s.patchAnnotations(ctx, base, func(a map[string]string) bool {
		if a[PlannedFailoverTerminalAnnotation] == m.String() {
			return false
		}
		a[PlannedFailoverTerminalAnnotation] = m.String()
		return true
	})
}

// ClearTerminalMarker removes the marker only if it names runID.
func (s *apiPlannedFailoverStore) ClearTerminalMarker(ctx context.Context, base *v1alpha1.MysqlFailoverGroup, runID string) (WriteOutcome, error) {
	return s.patchAnnotations(ctx, base, func(a map[string]string) bool {
		v, ok := a[PlannedFailoverTerminalAnnotation]
		if !ok {
			return false
		}
		if !TerminalMarkerNames(v, runID) {
			return false
		}
		delete(a, PlannedFailoverTerminalAnnotation)
		return true
	})
}

// ConsumeRequest removes the planned-failover request annotation.
func (s *apiPlannedFailoverStore) ConsumeRequest(ctx context.Context, base *v1alpha1.MysqlFailoverGroup) (WriteOutcome, error) {
	return s.patchAnnotations(ctx, base, func(a map[string]string) bool {
		if _, ok := a[PlannedFailoverAnnotation]; !ok {
			return false
		}
		delete(a, PlannedFailoverAnnotation)
		return true
	})
}

// MarkDeadlineStart records now for (kind, runID) unless the annotation
// already names runID, and returns the durable start.
func (s *apiPlannedFailoverStore) MarkDeadlineStart(ctx context.Context, base *v1alpha1.MysqlFailoverGroup, kind DeadlineKind, runID string,
	now time.Time) (time.Time, WriteOutcome, error) {
	key := kind.Annotation()
	if key == "" {
		return time.Time{}, WriteNotCommitted, fmt.Errorf("planned failover: unknown deadline kind %q", kind)
	}
	if start, ok := parseDeadlineStart(base.GetAnnotations()[key], runID); ok {
		return start, WriteCommitted, nil
	}
	start := now.UTC().Truncate(time.Second)
	outcome, err := s.patchAnnotations(ctx, base, func(a map[string]string) bool {
		a[key] = FormatDeadlineStart(runID, start)
		return true
	})
	if outcome != WriteCommitted {
		return time.Time{}, outcome, err
	}
	return start, WriteCommitted, nil
}

// ClearDeadlineStart removes the deadline annotation if it names runID.
func (s *apiPlannedFailoverStore) ClearDeadlineStart(ctx context.Context, base *v1alpha1.MysqlFailoverGroup, kind DeadlineKind, runID string) (WriteOutcome, error) {
	key := kind.Annotation()
	return s.patchAnnotations(ctx, base, func(a map[string]string) bool {
		if _, ok := parseDeadlineStart(a[key], runID); !ok {
			return false
		}
		delete(a, key)
		return true
	})
}

// ClearHold commits final (if non-nil) and the removal of
// status.plannedFailoverHold naming runID, with the AutomaticFailoverHeld
// condition set False/PlannedFailoverHoldAcknowledged, in one conditioned
// status write. Nothing to change is a no-op success.
func (s *apiPlannedFailoverStore) ClearHold(ctx context.Context, base *v1alpha1.MysqlFailoverGroup, runID string,
	final *v1alpha1.PlannedFailoverStatus) (WriteOutcome, error) {
	obj := base.DeepCopy()
	changed := false
	if final != nil {
		obj.Status.PlannedFailover = final.DeepCopy()
		changed = true
	}
	if ClearPlannedHold(&obj.Status, runID, base.Generation, s.clk.Now()) {
		changed = true
	}
	if !changed {
		return WriteCommitted, nil
	}
	err := s.client.Status().Update(ctx, obj)
	outcome, definite := classifyPlannedStatusWrite(err)
	if definite {
		return outcome, err
	}
	settled, serr := s.settle(ctx, client.ObjectKeyFromObject(base), base.ResourceVersion, func(cur *v1alpha1.MysqlFailoverGroup) bool {
		if statusHoldNames(&cur.Status, runID) {
			return false
		}
		if final == nil {
			return true
		}
		pf := cur.Status.PlannedFailover
		return pf != nil && pf.RunID == final.RunID && pf.Revision >= final.Revision
	})
	if settled == WriteCommitted {
		return WriteCommitted, nil
	}
	if serr == nil {
		serr = err
	}
	return settled, serr
}

// DeleteHoldAck removes the acknowledgement annotation if it names runID.
func (s *apiPlannedFailoverStore) DeleteHoldAck(ctx context.Context, base *v1alpha1.MysqlFailoverGroup, runID string) (WriteOutcome, error) {
	return s.patchAnnotations(ctx, base, func(a map[string]string) bool {
		v, ok := a[AcknowledgePlannedFailoverHoldAnnotation]
		if !ok || strings.TrimSpace(v) != runID {
			return false
		}
		delete(a, AcknowledgePlannedFailoverHoldAnnotation)
		return true
	})
}

// parseDeadlineStart returns the start recorded for runID; false when the
// value is absent, malformed, or names another run.
func parseDeadlineStart(v, runID string) (time.Time, bool) {
	id, t, ok := ParseDeadlineStart(v)
	if !ok || id != runID {
		return time.Time{}, false
	}
	return t, true
}
