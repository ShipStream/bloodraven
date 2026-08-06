package controller

import (
	"context"
	"errors"
	"log/slog"
	"sync"
	"testing"
	"time"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"

	"github.com/shipstream/bloodraven/api/v1alpha1"
	"github.com/shipstream/bloodraven/internal/clock"
)

func failoverStateScheme(t *testing.T) *runtime.Scheme {
	t.Helper()
	scheme := runtime.NewScheme()
	if err := v1alpha1.AddToScheme(scheme); err != nil {
		t.Fatalf("add v1alpha1 to scheme: %v", err)
	}
	return scheme
}

func failoverStateFG() *v1alpha1.MysqlFailoverGroup {
	return &v1alpha1.MysqlFailoverGroup{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "sim",
			Namespace: "bloodraven",
			Annotations: map[string]string{
				"unrelated.example.com/keep-me": "yes",
			},
		},
	}
}

// TestAnnotationFailoverStateRecorder_RoundTrip: what the recorder writes is
// what FailoverRecordFromAnnotations reads back, at metav1.Time precision,
// and unrelated annotations on the same object survive the merge patch.
func TestAnnotationFailoverStateRecorder_RoundTrip(t *testing.T) {
	scheme := failoverStateScheme(t)
	fg := failoverStateFG()
	c := fake.NewClientBuilder().WithScheme(scheme).WithObjects(fg).Build()
	nn := types.NamespacedName{Namespace: fg.Namespace, Name: fg.Name}

	// Sub-second component on purpose: what comes back must match what the
	// status subresource would carry for the same promotion, not what the
	// in-memory clock read.
	stamped := time.Date(2030, 3, 4, 5, 6, 7, 891011121, time.UTC)
	rec := NewAnnotationFailoverStateRecorder(c, nn)
	if err := rec.RecordFailoverState(context.Background(), FailoverRecord{
		LastFailover: stamped, LastFailoverTarget: "beta",
	}); err != nil {
		t.Fatalf("RecordFailoverState: %v", err)
	}

	var got v1alpha1.MysqlFailoverGroup
	if err := c.Get(context.Background(), nn, &got); err != nil {
		t.Fatalf("get fg: %v", err)
	}
	if got.Annotations["unrelated.example.com/keep-me"] != "yes" {
		t.Errorf("merge patch clobbered an unrelated annotation: %v", got.Annotations)
	}
	back, err := FailoverRecordFromAnnotations(got.Annotations)
	if err != nil {
		t.Fatalf("FailoverRecordFromAnnotations: %v", err)
	}
	if back.LastFailoverTarget != "beta" {
		t.Errorf("target round trip = %q, want beta", back.LastFailoverTarget)
	}
	if want := stamped.Truncate(time.Second); !back.LastFailover.Equal(want) {
		t.Errorf("timestamp round trip = %v, want %v (metav1.Time precision)", back.LastFailover, want)
	}
}

// TestAnnotationRecordTiesWithStatusCopy: both durable copies of one
// promotion must compare equal, so a healthy restart rehydrates from status
// and does NOT log the out-of-band fallback warning. Storing more precision
// in the annotation than metav1.Time can hold would make every restart look
// like a status-write outage.
func TestAnnotationRecordTiesWithStatusCopy(t *testing.T) {
	scheme := failoverStateScheme(t)
	fg := failoverStateFG()
	c := fake.NewClientBuilder().WithScheme(scheme).WithObjects(fg).Build()
	nn := types.NamespacedName{Namespace: fg.Namespace, Name: fg.Name}

	promotedAt := time.Date(2030, 3, 4, 5, 6, 7, 891011121, time.UTC)
	if err := NewAnnotationFailoverStateRecorder(c, nn).RecordFailoverState(
		context.Background(), FailoverRecord{LastFailover: promotedAt, LastFailoverTarget: "beta"},
	); err != nil {
		t.Fatalf("RecordFailoverState: %v", err)
	}

	var got v1alpha1.MysqlFailoverGroup
	if err := c.Get(context.Background(), nn, &got); err != nil {
		t.Fatalf("get fg: %v", err)
	}
	oob, err := FailoverRecordFromAnnotations(got.Annotations)
	if err != nil {
		t.Fatalf("FailoverRecordFromAnnotations: %v", err)
	}

	// What status.lastFailover would hold for the same promotion.
	statusRecord := FailoverRecord{
		LastFailover:       metav1.NewTime(promotedAt).Rfc3339Copy().Time.UTC(),
		LastFailoverTarget: "beta",
	}
	if winner := NewerFailoverRecord(statusRecord, oob); winner != statusRecord {
		t.Errorf("annotation copy beat the status copy of the same promotion (status=%v oob=%v); every restart would warn about a status outage",
			statusRecord.LastFailover, oob.LastFailover)
	}
}

// TestFailoverRecordFromAnnotations covers the read side, including the case
// that matters most: a corrupt timestamp must be an error, not a silent
// "no history" that would reset the cooldown.
func TestFailoverRecordFromAnnotations(t *testing.T) {
	stamp := time.Date(2030, 1, 2, 3, 4, 5, 0, time.UTC)
	tests := []struct {
		name        string
		annotations map[string]string
		want        FailoverRecord
		wantErr     bool
	}{
		{name: "nil map", annotations: nil},
		{name: "unrelated keys only", annotations: map[string]string{"a": "b"}},
		{
			name:        "target only",
			annotations: map[string]string{LastFailoverTargetAnnotation: "beta"},
			want:        FailoverRecord{LastFailoverTarget: "beta"},
		},
		{
			name: "full record",
			annotations: map[string]string{
				LastFailoverAnnotation:       stamp.Format(time.RFC3339Nano),
				LastFailoverTargetAnnotation: "gamma",
			},
			want: FailoverRecord{LastFailover: stamp, LastFailoverTarget: "gamma"},
		},
		{
			name: "non-UTC timestamp normalizes to UTC",
			annotations: map[string]string{
				LastFailoverAnnotation: stamp.In(time.FixedZone("x", 3600)).Format(time.RFC3339Nano),
			},
			want: FailoverRecord{LastFailover: stamp},
		},
		{
			name:        "corrupt timestamp is an error",
			annotations: map[string]string{LastFailoverAnnotation: "not-a-time"},
			wantErr:     true,
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got, err := FailoverRecordFromAnnotations(tc.annotations)
			if tc.wantErr {
				if err == nil {
					t.Fatalf("want error, got record %+v", got)
				}
				if !got.IsZero() {
					t.Errorf("error path must return the zero record, got %+v", got)
				}
				return
			}
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if !got.LastFailover.Equal(tc.want.LastFailover) || got.LastFailoverTarget != tc.want.LastFailoverTarget {
				t.Errorf("got %+v, want %+v", got, tc.want)
			}
		})
	}
}

// TestNewerFailoverRecord: either durable path can be the fresher one, and a
// tie resolves to the status copy passed first.
func TestNewerFailoverRecord(t *testing.T) {
	early := FailoverRecord{LastFailover: time.Unix(1000, 0).UTC(), LastFailoverTarget: "alpha"}
	late := FailoverRecord{LastFailover: time.Unix(2000, 0).UTC(), LastFailoverTarget: "beta"}
	tie := FailoverRecord{LastFailover: early.LastFailover, LastFailoverTarget: "gamma"}

	if got := NewerFailoverRecord(early, late); got != late {
		t.Errorf("annotations ahead: got %+v, want %+v", got, late)
	}
	if got := NewerFailoverRecord(late, early); got != late {
		t.Errorf("status ahead: got %+v, want %+v", got, late)
	}
	if got := NewerFailoverRecord(early, tie); got != early {
		t.Errorf("tie must favor the first (status) record: got %+v, want %+v", got, early)
	}
	if got := NewerFailoverRecord(FailoverRecord{}, late); got != late {
		t.Errorf("empty status: got %+v, want %+v", got, late)
	}
}

// stubFailoverRecorder records every write and can be made to fail, so the
// retry path is observable without an API server. before, when set, runs
// inside the write — the hook the concurrency test uses to slip a newer
// promotion in while a write is in flight.
type stubFailoverRecorder struct {
	mu     sync.Mutex
	writes []FailoverRecord
	err    error
	before func()
}

func (s *stubFailoverRecorder) RecordFailoverState(_ context.Context, rec FailoverRecord) error {
	if s.before != nil {
		hook := s.before
		s.before = nil
		hook()
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	s.writes = append(s.writes, rec)
	return s.err
}

func (s *stubFailoverRecorder) lastWrite() (FailoverRecord, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if len(s.writes) == 0 {
		return FailoverRecord{}, false
	}
	return s.writes[len(s.writes)-1], true
}

func failoverStateTestManager(rec FailoverStateRecorder) *TopologyManager {
	tm := &TopologyManager{
		logger: slog.New(slog.DiscardHandler),
		clock:  clock.NewFakeClock(time.Date(2030, 1, 1, 0, 0, 0, 0, time.UTC)),
	}
	tm.SetFailoverStateRecorder(rec)
	return tm
}

// TestPersistFailoverState_RetriesUntilAccepted is the core of the fix: a
// rejected out-of-band write must not be dropped, or the record it carries is
// exactly as lost as the status copy it exists to back up.
func TestPersistFailoverState_RetriesUntilAccepted(t *testing.T) {
	stub := &stubFailoverRecorder{err: errors.New("mysqlfailovergroups patch forbidden")}
	tm := failoverStateTestManager(stub)
	ctx := context.Background()

	now := tm.clock.Now()
	tm.recordFailover(ctx, now, "beta", "uuid:1-5")

	if tm.lastFailoverTarget != "beta" || !tm.lastFailover.Equal(now) {
		t.Fatalf("in-memory state not stamped: target=%q lastFailover=%v", tm.lastFailoverTarget, tm.lastFailover)
	}
	if !tm.failoverStateDirty {
		t.Fatal("rejected write left nothing to retry")
	}

	// Two more polls while the write is still denied: still dirty, still
	// retrying, and never re-stamping a different record.
	tm.flushFailoverState(ctx, false)
	tm.flushFailoverState(ctx, false)
	if len(stub.writes) != 3 {
		t.Fatalf("want 3 write attempts (1 promotion + 2 retries), got %d", len(stub.writes))
	}
	for i, w := range stub.writes {
		if w.LastFailoverTarget != "beta" || !w.LastFailover.Equal(now) {
			t.Errorf("attempt %d wrote %+v, want the promotion record", i, w)
		}
	}

	// Write is permitted again: the retry lands and disarms.
	stub.err = nil
	tm.flushFailoverState(ctx, false)
	if tm.failoverStateDirty {
		t.Error("dirty flag survived a successful write")
	}
	tm.flushFailoverState(ctx, false)
	if len(stub.writes) != 4 {
		t.Errorf("want no further attempts after success, got %d total", len(stub.writes))
	}
}

// TestPersistFailoverState_SupersededByNewerPromotion: a second promotion
// replaces the pending record rather than queueing behind it, so the retry
// can never re-stamp an older instant over a newer one.
func TestPersistFailoverState_SupersededByNewerPromotion(t *testing.T) {
	stub := &stubFailoverRecorder{err: errors.New("denied")}
	tm := failoverStateTestManager(stub)
	ctx := context.Background()
	fake := tm.clock.(*clock.FakeClock)

	first := tm.clock.Now()
	tm.recordFailover(ctx, first, "beta", "")
	fake.Advance(time.Minute)
	second := tm.clock.Now()
	tm.recordFailover(ctx, second, "gamma", "")

	stub.err = nil
	tm.flushFailoverState(ctx, false)

	last, _ := stub.lastWrite()
	if last.LastFailoverTarget != "gamma" || !last.LastFailover.Equal(second) {
		t.Errorf("retry wrote %+v, want the newer promotion (gamma @ %v)", last, second)
	}
}

// TestPersistFailoverState_NewerPromotionDuringWrite is the lost-update
// case. The ordered-update handoff records promotions from its own
// goroutine while Poll keeps running, so a newer promotion can land while a
// flush is mid-write. The write in flight must not be able to mark the
// store clean on the older record's behalf, or the durable copy stays
// behind the real last promotion with nothing scheduled to fix it.
func TestPersistFailoverState_NewerPromotionDuringWrite(t *testing.T) {
	stub := &stubFailoverRecorder{}
	tm := failoverStateTestManager(stub)
	ctx := context.Background()
	fake := tm.clock.(*clock.FakeClock)

	first := tm.clock.Now()
	second := first.Add(time.Minute)

	// While the first record is being written, a second promotion arrives.
	stub.before = func() {
		fake.Advance(time.Minute)
		tm.mu.Lock()
		cp := FailoverRecord{LastFailover: second, LastFailoverTarget: "gamma"}
		tm.desiredFailoverState = &cp
		tm.failoverStateDirty = true
		tm.mu.Unlock()
	}
	tm.recordFailover(ctx, first, "beta", "")

	if !tm.failoverStateDirty {
		t.Fatal("a newer promotion arrived mid-write, but the store was marked clean on the older record")
	}

	// The next poll writes the newer record and only then goes clean.
	tm.flushFailoverState(ctx, false)
	last, ok := stub.lastWrite()
	if !ok || last.LastFailoverTarget != "gamma" || !last.LastFailover.Equal(second) {
		t.Errorf("follow-up write = %+v, want gamma @ %v", last, second)
	}
	if tm.failoverStateDirty {
		t.Error("still dirty after the newest record landed")
	}
}

// TestRecordFailover_IgnoresOlderPromotion: an out-of-order record (a slow
// ordered-update goroutine reporting a promotion older than one already
// seen) must not roll the desired record backwards — nor the in-memory
// anti-flap pair, which feeds the cooldown check and split-brain fencing
// directly.
func TestRecordFailover_IgnoresOlderPromotion(t *testing.T) {
	stub := &stubFailoverRecorder{}
	tm := failoverStateTestManager(stub)
	ctx := context.Background()

	newer := tm.clock.Now()
	older := newer.Add(-time.Minute)

	tm.recordFailover(ctx, newer, "gamma", "")
	tm.recordFailover(ctx, older, "beta", "")

	if tm.desiredFailoverState == nil || tm.desiredFailoverState.LastFailoverTarget != "gamma" {
		t.Errorf("desired record rolled back to %+v; want gamma @ %v", tm.desiredFailoverState, newer)
	}
	last, _ := stub.lastWrite()
	if last.LastFailoverTarget != "gamma" {
		t.Errorf("store last saw %+v, want the newer record", last)
	}
	if !tm.lastFailover.Equal(newer) {
		t.Errorf("in-memory cooldown anchor rolled back to %v; want %v", tm.lastFailover, newer)
	}
	if tm.lastFailoverTarget != "gamma" {
		t.Errorf("in-memory fencing target renamed to %q; want gamma", tm.lastFailoverTarget)
	}
}

// TestPersistFailoverState_EqualTimestampSupersede: under a coarse clock two
// promotions can share an instant. A supersede that changes only the target
// must keep the store dirty when it lands mid-write, or the newer target is
// dropped with nothing scheduled to retry.
//
// The superseding promotion goes through recordFailover rather than being
// stamped onto the manager by hand, so both halves of the equal-timestamp
// rule are under test: recordFailover must ADVANCE on an equal instant with
// a different target (a strict Before comparison there would drop gamma
// silently), and the flush must then refuse to mark the store clean on
// beta's behalf.
func TestPersistFailoverState_EqualTimestampSupersede(t *testing.T) {
	stub := &stubFailoverRecorder{}
	tm := failoverStateTestManager(stub)
	ctx := context.Background()

	now := tm.clock.Now()
	gammaDone := make(chan struct{})
	releaseGamma := make(chan struct{})

	// While (now, beta) is being written, a same-instant promotion of gamma
	// arrives from another goroutine — the ordered-update handoff racing
	// Poll, which is the only way two promotions can share an instant.
	stub.before = func() {
		// gamma's own flush must be pinned inside the stub once it wins the
		// write mutex, so the assertions below observe the state beta's
		// write left behind rather than racing gamma's write to clear it.
		// Set before beta's flush releases the mutex, so gamma cannot miss it.
		stub.before = func() { <-releaseGamma }

		go func() {
			defer close(gammaDone)
			tm.recordFailover(ctx, now, "gamma", "")
		}()
		// Wait only for the record gamma publishes, never for its call to
		// return: its flush blocks on the write mutex this one holds.
		waitForDesiredTarget(t, tm, "gamma")
	}
	tm.recordFailover(ctx, now, "beta", "")

	if !failoverStateIsDirty(tm) {
		t.Fatal("equal-timestamp supersede was marked clean; the gamma record would never be written")
	}

	// Let gamma's retry through. It is the real per-promotion flush, so this
	// also proves the pending record survives to a successful write.
	close(releaseGamma)
	<-gammaDone

	last, ok := stub.lastWrite()
	if !ok || last.LastFailoverTarget != "gamma" || !last.LastFailover.Equal(now) {
		t.Errorf("follow-up write = %+v, want gamma @ %v", last, now)
	}
	if failoverStateIsDirty(tm) {
		t.Error("still dirty after the newest record landed")
	}
	// The in-memory anti-flap pair feeds the cooldown check and split-brain
	// fencing, and it is subject to the same equal-instant rule.
	tm.mu.RLock()
	target := tm.lastFailoverTarget
	tm.mu.RUnlock()
	if target != "gamma" {
		t.Errorf("in-memory fencing target = %q, want the same-instant supersede gamma", target)
	}
}

// waitForDesiredTarget blocks until the pending out-of-band record names
// target. It is how a test observes a promotion recorded from another
// goroutine without waiting for that goroutine's flush, which may be queued
// behind the caller's own write.
func waitForDesiredTarget(t *testing.T, tm *TopologyManager, target string) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		tm.mu.RLock()
		rec, dirty := tm.desiredFailoverState, tm.failoverStateDirty
		tm.mu.RUnlock()
		if dirty && rec != nil && rec.LastFailoverTarget == target {
			return
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatalf("timed out waiting for the pending record to name %q; recordFailover dropped the equal-instant supersede", target)
}

func failoverStateIsDirty(tm *TopologyManager) bool {
	tm.mu.RLock()
	defer tm.mu.RUnlock()
	return tm.failoverStateDirty
}

// TestPersistFailoverState_NoRecorder: the store is optional, and its absence
// must not panic or arm a retry that can never clear.
func TestPersistFailoverState_NoRecorder(t *testing.T) {
	tm := failoverStateTestManager(nil)
	tm.recordFailover(context.Background(), tm.clock.Now(), "beta", "")
	tm.flushFailoverState(context.Background(), false)
}

// TestAnnotationFailoverStateRecorder_PatchRejected: a denied patch surfaces
// as an error so the caller can arm its retry. This is the failure the whole
// out-of-band path is defending against, on its own leg.
func TestAnnotationFailoverStateRecorder_PatchRejected(t *testing.T) {
	scheme := failoverStateScheme(t)
	fg := failoverStateFG()
	denied := errors.New("mysqlfailovergroups.shipstream.io is forbidden")
	c := fake.NewClientBuilder().WithScheme(scheme).WithObjects(fg).
		WithInterceptorFuncs(interceptor.Funcs{
			Patch: func(_ context.Context, _ client.WithWatch, _ client.Object, _ client.Patch, _ ...client.PatchOption) error {
				return denied
			},
		}).Build()

	rec := NewAnnotationFailoverStateRecorder(c, types.NamespacedName{Namespace: fg.Namespace, Name: fg.Name})
	err := rec.RecordFailoverState(context.Background(), FailoverRecord{
		LastFailover: time.Now(), LastFailoverTarget: "beta",
	})
	if !errors.Is(err, denied) {
		t.Fatalf("want the patch error to propagate, got %v", err)
	}
}

// TestAnnotationFailoverStateRecorder_MissingObject: patching a group that no
// longer exists must report NotFound rather than silently succeeding.
func TestAnnotationFailoverStateRecorder_MissingObject(t *testing.T) {
	scheme := failoverStateScheme(t)
	c := fake.NewClientBuilder().WithScheme(scheme).Build()
	rec := NewAnnotationFailoverStateRecorder(c, types.NamespacedName{Namespace: "bloodraven", Name: "gone"})
	err := rec.RecordFailoverState(context.Background(), FailoverRecord{LastFailoverTarget: "beta"})
	if !apierrors.IsNotFound(err) {
		t.Fatalf("want NotFound, got %v", err)
	}
}
