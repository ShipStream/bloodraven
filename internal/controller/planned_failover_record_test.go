package controller

import (
	"context"
	"log/slog"
	"testing"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/tools/record"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	v1alpha1 "github.com/shipstream/bloodraven/api/v1alpha1"
	"github.com/shipstream/bloodraven/internal/clock"
	"github.com/shipstream/bloodraven/internal/state"
)

// TestFailoverStateRecorder_ConditionedNoOverwrite: the out-of-band record
// carries the watermark, and an older record written late (by an older
// process, or a slow flush) never overwrites a newer one.
func TestFailoverStateRecorder_ConditionedNoOverwrite(t *testing.T) {
	fg := failoverStateFG()
	c := fake.NewClientBuilder().WithScheme(failoverStateScheme(t)).WithObjects(fg).Build()
	nn := types.NamespacedName{Namespace: fg.Namespace, Name: fg.Name}
	t1 := time.Date(2030, 1, 1, 0, 0, 10, 0, time.UTC)
	t0 := t1.Add(-time.Minute)
	// The writer's clock is at t1: neither record is future-dated (K023).
	rec := NewAnnotationFailoverStateRecorderWithClock(c, c, nn, clock.NewFakeClock(t1))
	read := func() FailoverRecord {
		t.Helper()
		var got v1alpha1.MysqlFailoverGroup
		if err := c.Get(context.Background(), nn, &got); err != nil {
			t.Fatal(err)
		}
		r, err := FailoverRecordFromAnnotations(got.Annotations)
		if err != nil {
			t.Fatal(err)
		}
		return r
	}
	newer := FailoverRecord{LastFailover: t1, LastFailoverTarget: "beta", PromotionGtidExecuted: opsUUIDA + ":1-20"}
	if err := rec.RecordFailoverState(context.Background(), newer); err != nil {
		t.Fatal(err)
	}
	if got := read(); got != newer {
		t.Fatalf("stored = %+v, want %+v", got, newer)
	}
	late := FailoverRecord{LastFailover: t0, LastFailoverTarget: "alpha", PromotionGtidExecuted: opsUUIDA + ":1-10"}
	if err := rec.RecordFailoverState(context.Background(), late); err != nil {
		t.Fatalf("a late older record must be a no-op success: %v", err)
	}
	if got := read(); got != newer {
		t.Fatalf("a late older record overwrote the newer one: %+v", got)
	}
	// Same second, other target (a coarse clock): written.
	sameSecond := FailoverRecord{LastFailover: t1.Add(300 * time.Millisecond), LastFailoverTarget: "gamma", PromotionGtidExecuted: opsUUIDA + ":1-21"}
	if err := rec.RecordFailoverState(context.Background(), sameSecond); err != nil {
		t.Fatal(err)
	}
	if got := read(); got.LastFailoverTarget != "gamma" {
		t.Fatalf("same-second record with a new target not written: %+v", got)
	}
	// An oversized watermark is left out of the annotation.
	huge := FailoverRecord{LastFailover: t1.Add(time.Minute), LastFailoverTarget: "beta",
		PromotionGtidExecuted: string(make([]byte, maxPromotionGtidAnnotationBytes+1))}
	if err := rec.RecordFailoverState(context.Background(), huge); err != nil {
		t.Fatal(err)
	}
	if got := read(); got.PromotionGtidExecuted != "" || !got.LastFailover.Equal(huge.LastFailover) {
		t.Fatalf("oversized watermark record = %+v; want the record without the watermark", got)
	}
	// An intentional clear deletes all three annotations.
	if err := ClearFailoverState(context.Background(), c, nn); err != nil {
		t.Fatal(err)
	}
	if got := read(); !got.IsZero() || got.PromotionGtidExecuted != "" {
		t.Fatalf("clear left %+v", got)
	}
}

// TestRestore_WatermarkFromNewerOOBRecord: a status outage after a
// promotion cannot leave an older watermark in force across a restart.
func TestRestore_WatermarkFromNewerOOBRecord(t *testing.T) {
	older := metav1.NewTime(time.Now().Add(-time.Hour).UTC().Truncate(time.Second))
	newer := older.Add(time.Minute)
	cases := []struct {
		name      string
		oobWater  string
		wantWater string
	}{
		{"newer out-of-band record with its watermark", opsUUIDA + ":1-20", opsUUIDA + ":1-20"},
		{"newer out-of-band record without one falls back to status", "", opsUUIDA + ":1-10"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			fg := newTestFG()
			fg.Status.LastFailover, fg.Status.LastFailoverTarget, fg.Status.PromotionGtidExecuted = &older, "dc1", opsUUIDA+":1-10"
			fg.Annotations = map[string]string{v1alpha1.LastFailoverAnnotation: failoverStampFormat(newer), v1alpha1.LastFailoverTargetAnnotation: "dc2"}
			if tc.oobWater != "" {
				fg.Annotations[v1alpha1.PromotionGtidExecutedAnnotation] = tc.oobWater
				fg.Annotations[v1alpha1.PromotionGtidExecutedForAnnotation] = failoverWatermarkBinding(newer, "dc2")
			}
			tm, _, _ := newTestTopologyManager(&mockMySQL{}, &mockMySQL{readOnly: true})
			c := fake.NewClientBuilder().WithScheme(testScheme()).WithObjects(fg).Build()
			r := &TopologyManagerRunner{client: c, logger: testLogger()}
			r.restoreFailoverState(tm, fg, types.NamespacedName{Namespace: fg.Namespace, Name: fg.Name})
			if tm.promotionGtidExecuted != tc.wantWater || tm.lastFailoverTarget != "dc2" {
				t.Fatalf("restored watermark %q target %q; want %q dc2", tm.promotionGtidExecuted, tm.lastFailoverTarget, tc.wantWater)
			}
		})
	}
}

// TestUpdateCRStatus_TrackingTripleMonotonic: a snapshot never moves the
// failover triple to a strictly older lastFailover, and an equal one moves
// only the target (OPEN-2).
func TestUpdateCRStatus_TrackingTripleMonotonic(t *testing.T) {
	stored := time.Date(2030, 1, 1, 0, 0, 30, 0, time.UTC)
	sites := []SiteSnapshot{
		{Name: "dc1", Role: state.SiteRolePrimaryCandidate, State: state.StateWritable},
		{Name: "dc2", Role: state.SiteRolePrimaryCandidate, State: state.StateReadOnly},
	}
	cases := []struct {
		name              string
		snapAt            time.Time
		snapTarget, snapW string
		wantAt            time.Time
		wantTarget, wantW string
	}{
		{"older snapshot", stored.Add(-time.Minute), "dc1", "old", stored, "dc2", "stored"},
		{"no history in the snapshot", time.Time{}, "", "", stored, "dc2", "stored"},
		{"same second names a target", stored.Add(400 * time.Millisecond), "dc1", "other", stored, "dc1", "stored"},
		{"newer snapshot", stored.Add(time.Minute), "dc1", "new", stored.Add(time.Minute), "dc1", "new"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			fg := newTestFG()
			at := metav1.NewTime(stored)
			fg.Status.LastFailover, fg.Status.LastFailoverTarget, fg.Status.PromotionGtidExecuted = &at, "dc2", "stored"
			c := fake.NewClientBuilder().WithScheme(testScheme()).WithStatusSubresource(&v1alpha1.MysqlFailoverGroup{}).WithObjects(fg).Build()
			r := &TopologyManagerRunner{client: c, logger: testLogger(), recorder: record.NewFakeRecorder(32),
				managers: make(map[types.NamespacedName]*managedTopology)}
			nn := types.NamespacedName{Namespace: fg.Namespace, Name: fg.Name}
			snap := TopologySnapshot{ActiveSite: "dc1", Sites: sites, LastFailover: tc.snapAt, LastFailoverTarget: tc.snapTarget, PromotionGtidExecuted: tc.snapW}
			if err := r.updateCRStatus(context.Background(), nn, snap); err != nil {
				t.Fatal(err)
			}
			var got v1alpha1.MysqlFailoverGroup
			if err := c.Get(context.Background(), nn, &got); err != nil {
				t.Fatal(err)
			}
			if got.Status.LastFailover == nil || !got.Status.LastFailover.UTC().Truncate(time.Second).Equal(tc.wantAt) ||
				got.Status.LastFailoverTarget != tc.wantTarget || got.Status.PromotionGtidExecuted != tc.wantW {
				t.Fatalf("triple = %v/%q/%q, want %s/%q/%q", got.Status.LastFailover, got.Status.LastFailoverTarget,
					got.Status.PromotionGtidExecuted, tc.wantAt, tc.wantTarget, tc.wantW)
			}
		})
	}
}

// upgradeDuringEmergency is a snapshot upgrade in phase retaining dc1,
// started at start, with MySQL's durable record saying it failed over to
// dc2 at failedAt.
func upgradeDuringEmergency(phase v1alpha1.DragonflyUpgradePhase, start, failedAt time.Time) *v1alpha1.MysqlFailoverGroup {
	fg := fgWithDragonflySnapshotUpgrade()
	fg.Annotations = nil
	st := metav1.NewTime(start)
	fg.Status.Dragonfly.Upgrade = &v1alpha1.DragonflyUpgradeStatus{Phase: phase, ActiveSite: "dc1",
		TargetImage: "docker.dragonflydb.io/dragonflydb/dragonfly:v1.39.0", SnapshotDir: fg.Spec.Dragonfly.Snapshot.Dir, StartTime: &st}
	if !failedAt.IsZero() {
		at := metav1.NewTime(failedAt)
		fg.Status.LastFailover, fg.Status.LastFailoverTarget = &at, "dc2"
	}
	return fg
}

// TestDragonflyUpgrade_EmergencyDuringSavingSnapshot and
// …DuringReattachingReplicas: an emergency MySQL failover during the
// upgrade supersedes it on the next upgrade reconcile, re-derived from the
// durable failover record when the manager was restarted in between (its
// in-memory authority is gone), and nothing is sent to Dragonfly.
func TestDragonflyUpgrade_EmergencyDuringSavingSnapshot(t *testing.T) {
	testDragonflyUpgradeEmergency(t, v1alpha1.DragonflyUpgradePhaseSavingSnapshot)
}

func TestDragonflyUpgrade_EmergencyDuringReattachingReplicas(t *testing.T) {
	testDragonflyUpgradeEmergency(t, v1alpha1.DragonflyUpgradePhaseReattachingReplicas)
}

func testDragonflyUpgradeEmergency(t *testing.T, phase v1alpha1.DragonflyUpgradePhase) {
	start := time.Now().Add(-time.Minute).Truncate(time.Second)
	t.Run("manager restarted: from the durable record", func(t *testing.T) {
		fg := upgradeDuringEmergency(phase, start, start.Add(10*time.Second))
		r, c := newReconciler(fg)
		r.Runner = &TopologyManagerRunner{logger: slog.Default(), managers: make(map[types.NamespacedName]*managedTopology)}
		dials := 0
		r.dragonflyConnector = func(context.Context, string, string) (DragonflyConnection, error) {
			dials++
			return &fakeDragonflyConn{}, nil
		}
		if _, err := r.reconcileDragonflySnapshotUpgrade(context.Background(), fg); err != nil {
			t.Fatal(err)
		}
		got := getFGByKey(t, c, types.NamespacedName{Namespace: fg.Namespace, Name: fg.Name})
		if up := got.Status.Dragonfly.Upgrade; up.Phase != v1alpha1.DragonflyUpgradePhaseFailed || up.Reason != "ActiveSiteChanged" {
			t.Fatalf("upgrade = %s/%s, want Failed/ActiveSiteChanged", up.Phase, up.Reason)
		}
		if dials != 0 {
			t.Fatalf("a superseded upgrade dialled Dragonfly %d times", dials)
		}
	})
	t.Run("a failover before the upgrade does not supersede it", func(t *testing.T) {
		fg := upgradeDuringEmergency(phase, start, start.Add(-time.Hour))
		r, c := newReconciler(fg)
		r.Runner = &TopologyManagerRunner{logger: slog.Default(), managers: make(map[types.NamespacedName]*managedTopology)}
		r.dragonflyConnector = func(context.Context, string, string) (DragonflyConnection, error) { return &fakeDragonflyConn{}, nil }
		_, _ = r.reconcileDragonflySnapshotUpgrade(context.Background(), fg)
		if up := getFGByKey(t, c, types.NamespacedName{Namespace: fg.Namespace, Name: fg.Name}).Status.Dragonfly.Upgrade; up.Reason == "ActiveSiteChanged" {
			t.Fatal("an older failover superseded the upgrade")
		}
	})
	t.Run("in-memory authority hands the follow-up to the Dragonfly manager", func(t *testing.T) {
		fg := upgradeDuringEmergency(phase, start, time.Time{})
		r, c := newReconciler(fg)
		nn := types.NamespacedName{Namespace: fg.Namespace, Name: fg.Name}
		runner := &TopologyManagerRunner{logger: slog.Default(), managers: make(map[types.NamespacedName]*managedTopology)}
		tm, _, _ := newTestTopologyManager(&mockMySQL{readOnly: true}, &mockMySQL{})
		tm.mu.Lock()
		tm.sites[0].state, tm.sites[1].state = state.StateReadOnly, state.StateWritable // promoted, status not yet written
		tm.mu.Unlock()
		df := NewDragonflyManager(c, record.NewFakeRecorder(8), slog.Default(), nn, time.Second)
		runner.registerManager(nn, &managedTopology{tm: tm, dragonfly: df})
		r.Runner = runner
		r.dragonflyConnector = func(context.Context, string, string) (DragonflyConnection, error) { return &fakeDragonflyConn{}, nil }
		if _, err := r.reconcileDragonflySnapshotUpgrade(context.Background(), fg); err != nil {
			t.Fatal(err)
		}
		if up := getFGByKey(t, c, nn).Status.Dragonfly.Upgrade; up.Phase != v1alpha1.DragonflyUpgradePhaseFailed {
			t.Fatalf("upgrade phase = %s, want Failed", up.Phase)
		}
		req := df.peekPending()
		if req == nil || req.target != "dc2" || req.oldSource != "dc1" {
			t.Fatalf("follow-up = %+v, want dc2 taking over from dc1", req)
		}
		if df.upgradePaused.Load() {
			t.Fatal("the abandoned upgrade left the Dragonfly manager paused")
		}
	})
}
