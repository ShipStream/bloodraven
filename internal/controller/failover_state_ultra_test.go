package controller

import (
	"context"
	"testing"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"

	v1alpha1 "github.com/shipstream/bloodraven/api/v1alpha1"
	"github.com/shipstream/bloodraven/internal/clock"
	"github.com/shipstream/bloodraven/internal/state"
)

// K023: a stored failover record more than FailoverClockSkewGrace ahead of
// the writer's clock no longer blocks a later real promotion from reaching
// either durable copy; a plausible newer one still does.
func TestUltraK023_FutureDatedRecordDoesNotBlockWriters(t *testing.T) {
	now := time.Date(2026, 11, 4, 12, 0, 0, 0, time.UTC)
	for _, tc := range []struct {
		name      string
		storedAt  time.Time
		overwrite bool
	}{
		{name: "an hour ahead (skewed or hand-edited)", storedAt: now.Add(time.Hour), overwrite: true},
		{name: "a minute ahead (within the grace)", storedAt: now.Add(time.Minute), overwrite: false},
	} {
		t.Run(tc.name+"/status merge", func(t *testing.T) {
			stored := metav1.NewTime(tc.storedAt)
			st := v1alpha1.MysqlFailoverGroupStatus{LastFailover: &v1alpha1.FailoverRecord{FailoverRecordRef: v1alpha1.FailoverRecordRef{Time: &stored, Target: "dc1"}, PromotionGtidExecuted: "old"}}
			snap := TopologySnapshot{LastFailover: now, LastFailoverTarget: "dc2", PromotionGtidExecuted: "new", ObservedAt: now}
			MergeTopologySnapshotStatus(&st, snap)
			if got := st.LastFailoverTarget() == "dc2" && st.PromotionGtidExecuted() == "new"; got != tc.overwrite {
				t.Fatalf("merged triple = %v %q %q; overwrite=%v", st.LastFailover, st.LastFailoverTarget(), st.PromotionGtidExecuted(), tc.overwrite)
			}
		})
		t.Run(tc.name+"/planned record", func(t *testing.T) {
			stored := metav1.NewTime(tc.storedAt)
			st := v1alpha1.MysqlFailoverGroupStatus{LastFailover: &v1alpha1.FailoverRecord{FailoverRecordRef: v1alpha1.FailoverRecordRef{Time: &stored, Target: "dc1"}, PromotionGtidExecuted: "old"}}
			applyPlannedFailoverRecord(&st, now, "dc2", "new", now)
			if got := st.LastFailoverTarget() == "dc2" && st.PromotionGtidExecuted() == "new"; got != tc.overwrite {
				t.Fatalf("applied triple = %v %q %q; overwrite=%v", st.LastFailover, st.LastFailoverTarget(), st.PromotionGtidExecuted(), tc.overwrite)
			}
		})
		t.Run(tc.name+"/annotation recorder", func(t *testing.T) {
			fg := failoverStateFG()
			ann, err := FailoverRecordAnnotations(FailoverRecord{LastFailover: tc.storedAt, LastFailoverTarget: "alpha", PromotionGtidExecuted: opsUUIDA + ":1-5"})
			if err != nil {
				t.Fatal(err)
			}
			for k, v := range ann {
				fg.Annotations[k] = v
			}
			c := fake.NewClientBuilder().WithScheme(failoverStateScheme(t)).WithObjects(fg).Build()
			nn := types.NamespacedName{Namespace: fg.Namespace, Name: fg.Name}
			rec := NewAnnotationFailoverStateRecorderWithClock(c, c, nn, clock.NewFakeClock(now))
			want := FailoverRecord{LastFailover: now, LastFailoverTarget: "beta", PromotionGtidExecuted: opsUUIDA + ":1-9"}
			if err := rec.RecordFailoverState(context.Background(), want); err != nil {
				t.Fatal(err)
			}
			var got v1alpha1.MysqlFailoverGroup
			if err := c.Get(context.Background(), nn, &got); err != nil {
				t.Fatal(err)
			}
			back, err := FailoverRecordFromAnnotations(got.Annotations)
			if err != nil {
				t.Fatal(err)
			}
			if (back == want) != tc.overwrite {
				t.Fatalf("stored record = %+v; overwrite=%v", back, tc.overwrite)
			}
		})
	}
}

// K095: the out-of-band watermark counts only for the event it was written
// with. A writer that advances only the two failover keys (an older
// operator, a hand edit) leaves an older event's watermark that is ignored.
func TestUltraK095_WatermarkBoundToItsEvent(t *testing.T) {
	t1 := time.Date(2026, 11, 4, 12, 0, 0, 0, time.UTC)
	ann, err := FailoverRecordAnnotations(FailoverRecord{LastFailover: t1, LastFailoverTarget: "dc1", PromotionGtidExecuted: opsUUIDA + ":1-5"})
	if err != nil {
		t.Fatal(err)
	}
	rec, err := FailoverRecordFromAnnotations(ann)
	if err != nil || rec.PromotionGtidExecuted != opsUUIDA+":1-5" {
		t.Fatalf("own watermark not read back: %+v, %v", rec, err)
	}
	// A two-key writer advances the event and leaves the watermark.
	ann[v1alpha1.LastFailoverAnnotation] = failoverStampFormat(t1.Add(time.Hour))
	ann[v1alpha1.LastFailoverTargetAnnotation] = "dc2"
	rec, err = FailoverRecordFromAnnotations(ann)
	if err != nil {
		t.Fatal(err)
	}
	if rec.PromotionGtidExecuted != "" || rec.LastFailoverTarget != "dc2" {
		t.Fatalf("record = %+v; want the newer event with no watermark", rec)
	}
	// A watermark with no binding at all is no watermark either.
	delete(ann, v1alpha1.PromotionGtidExecutedForAnnotation)
	if rec, _ := FailoverRecordFromAnnotations(ann); rec.PromotionGtidExecuted != "" {
		t.Fatalf("an unbound watermark was read: %+v", rec)
	}
}

// K058: a status write whose first Update conflicts with the planned
// engine's terminal write (a newer failover record at the same topology
// generation) re-merges the triple over the live one instead of writing
// its pre-merged copy back.
func TestUltraK058_StatusRetryKeepsNewerFailoverTriple(t *testing.T) {
	old := time.Date(2026, 11, 4, 11, 0, 0, 0, time.UTC)
	newer := old.Add(time.Hour)
	fg := newTestFG()
	oldT := metav1.NewTime(old)
	fg.Status.SetLastFailover(&oldT, "dc1", "old")
	fg.Status.TopologyGeneration = 4
	injected := false
	c := fake.NewClientBuilder().WithScheme(testScheme()).
		WithStatusSubresource(&v1alpha1.MysqlFailoverGroup{}).
		WithObjects(fg).
		WithInterceptorFuncs(interceptor.Funcs{
			SubResourceUpdate: func(ctx context.Context, underlying client.Client, sub string, obj client.Object, opts ...client.SubResourceUpdateOption) error {
				if !injected {
					injected = true
					// The engine's Succeeded write lands first.
					var cur v1alpha1.MysqlFailoverGroup
					if err := underlying.Get(ctx, client.ObjectKeyFromObject(obj), &cur); err != nil {
						return err
					}
					nt := metav1.NewTime(newer)
					cur.Status.SetLastFailover(&nt, "dc2", "planned")
					if err := underlying.Status().Update(ctx, &cur); err != nil {
						return err
					}
				}
				return underlying.Status().Update(ctx, obj, opts...)
			},
		}).
		Build()
	r := &TopologyManagerRunner{client: c, logger: testLogger(), managers: map[types.NamespacedName]*managedTopology{}}
	nn := types.NamespacedName{Namespace: fg.Namespace, Name: fg.Name}
	snap := TopologySnapshot{TopologyGeneration: 4, ActiveSite: "dc1", LastFailover: old, LastFailoverTarget: "dc1",
		PromotionGtidExecuted: "old", ObservedAt: newer,
		Sites: []SiteSnapshot{{Name: "dc1", State: state.StateWritable}, {Name: "dc2", State: state.StateReadOnly}}}
	if err := r.updateCRStatus(context.Background(), nn, snap); err != nil {
		t.Fatalf("updateCRStatus: %v", err)
	}
	if !injected {
		t.Fatal("the conflict was not injected")
	}
	var got v1alpha1.MysqlFailoverGroup
	if err := c.Get(context.Background(), nn, &got); err != nil {
		t.Fatal(err)
	}
	if got.Status.LastFailoverTarget() != "dc2" || got.Status.PromotionGtidExecuted() != "planned" || !got.Status.LastFailoverTime().Time.Equal(newer) {
		t.Fatalf("triple after the retried write = %v %q %q; want the planned engine's newer record",
			got.Status.LastFailoverTime(), got.Status.LastFailoverTarget(), got.Status.PromotionGtidExecuted())
	}
}

// K121 (runner half): the runner's sync never re-pauses the Dragonfly
// manager from a lagging cache; it re-reads uncached when the cache
// disagrees, and still repairs a pause the live state no longer backs.
func TestUltraK121_SyncPauseFollowsUncachedState(t *testing.T) {
	fg := newTestFG()
	fg.Status.Dragonfly = &v1alpha1.DragonflyStatus{Upgrade: &v1alpha1.DragonflyUpgradeStatus{Phase: v1alpha1.DragonflyUpgradePhaseFailed}}
	c := fake.NewClientBuilder().WithScheme(testScheme()).WithObjects(fg).Build()
	r := &TopologyManagerRunner{client: c, apiReader: c, logger: testLogger(), managers: map[types.NamespacedName]*managedTopology{}}
	nn := types.NamespacedName{Namespace: fg.Namespace, Name: fg.Name}

	df := &DragonflyManager{}
	// The reconcile cleared the pause under the gate (upgrade Failed); the
	// cache still shows it in flight.
	r.syncDragonflyUpgradePause(context.Background(), nn, df, true)
	if df.upgradePaused.Load() {
		t.Fatal("a lagging cache paused the Dragonfly manager again after the upgrade ended")
	}
	// A stale pause the live state no longer backs is repaired.
	df.SetUpgradePaused(true)
	r.syncDragonflyUpgradePause(context.Background(), nn, df, false)
	if df.upgradePaused.Load() {
		t.Fatal("a pause with no in-flight upgrade behind it was kept")
	}
}
