package controller

import (
	"context"
	"testing"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/tools/record"
	"sigs.k8s.io/controller-runtime/pkg/client"

	v1alpha1 "github.com/shipstream/bloodraven/api/v1alpha1"
	"github.com/shipstream/bloodraven/internal/dragonfly"
)

// inFlightUpgradeFixture is a group with a run recorded at phase, both
// sites' StatefulSets seeded (the active on the target image once the run
// has moved it) and a Ready pod per site.
func inFlightUpgradeFixture(t *testing.T, phase v1alpha1.DragonflyUpgradePhase) (*MysqlFailoverGroupReconciler, client.Client, *v1alpha1.MysqlFailoverGroup, *fakeDragonflyConn) {
	t.Helper()
	ctx := context.Background()
	fg := fgWithDragonflySnapshotUpgrade()
	now := metav1.Now()
	fg.Status.Dragonfly.Upgrade = &v1alpha1.DragonflyUpgradeStatus{
		Phase: phase, SourceImage: dfSourceImage, TargetImage: dfTargetImage, ActiveSite: "dc1",
		SnapshotDir: fg.Spec.Dragonfly.Snapshot.Dir, SpecDigest: dragonflySpecDigest(fg.Spec.Dragonfly),
		StartTime: &now, SnapshotTime: &now,
	}
	r, c := newReconciler(fg)
	r.Runner = NewTopologyManagerRunner(c, nil, nil, record.NewFakeRecorder(64), testLogger())
	activeImage := dfSourceImage
	if phase == v1alpha1.DragonflyUpgradePhaseWaitingForActiveRestore || phase == v1alpha1.DragonflyUpgradePhaseReattachingReplicas {
		activeImage = dfTargetImage
	}
	seedDragonflyStatefulSets(t, r, c, fg, map[string]string{"dc1": activeImage, "dc2": dfSourceImage})
	for site, img := range map[string]string{"dc1": activeImage, "dc2": dfSourceImage} {
		if err := c.Create(ctx, dragonflyPodForTest(fg, site, img, true)); err != nil {
			t.Fatal(err)
		}
	}
	conn := &fakeDragonflyConn{info: dragonfly.ReplicationInfo{Role: "master"}}
	r.dragonflyConnector = func(context.Context, string, string) (DragonflyConnection, error) { return conn, nil }
	return r, c, fg, conn
}

// deleteDragonflySite deletes site's StatefulSet and its pod, as a
// cascading delete does.
func deleteDragonflySite(t *testing.T, c client.Client, fg *v1alpha1.MysqlFailoverGroup, site string) {
	t.Helper()
	ctx := context.Background()
	name := dragonflyStatefulSetName(fg.Name, site)
	if err := c.Delete(ctx, &appsv1.StatefulSet{ObjectMeta: metav1.ObjectMeta{Namespace: fg.Namespace, Name: name}}); err != nil {
		t.Fatal(err)
	}
	if err := c.Delete(ctx, &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Namespace: fg.Namespace, Name: name + "-0"}}); err != nil && !apierrors.IsNotFound(err) {
		t.Fatal(err)
	}
}

// R2-03: a StatefulSet deleted while a run is in flight must not wedge it.
// The run holds the reconcile before the render, so the render can never
// re-create it: the run either re-creates it on the target image (where
// its own step would leave the site there anyway) or ends, so the render
// re-creates it. No pass returns an error.
func TestDragonflySnapshotUpgrade_DeletedStatefulSetDoesNotWedge(t *testing.T) {
	type outcome int
	const (
		recreated outcome = iota
		ended
	)
	phases := []v1alpha1.DragonflyUpgradePhase{
		v1alpha1.DragonflyUpgradePhasePending,
		v1alpha1.DragonflyUpgradePhaseSavingSnapshot,
		v1alpha1.DragonflyUpgradePhaseUpdatingActive,
		v1alpha1.DragonflyUpgradePhaseWaitingForActiveRestore,
		v1alpha1.DragonflyUpgradePhaseReattachingReplicas,
	}
	want := func(phase v1alpha1.DragonflyUpgradePhase, site string) outcome {
		if site == "dc2" {
			return recreated
		}
		switch phase {
		case v1alpha1.DragonflyUpgradePhaseUpdatingActive, v1alpha1.DragonflyUpgradePhaseWaitingForActiveRestore:
			return recreated
		}
		return ended
	}
	for _, phase := range phases {
		for _, site := range []string{"dc1", "dc2"} {
			t.Run(string(phase)+"/"+site, func(t *testing.T) {
				ctx := context.Background()
				r, c, fg, _ := inFlightUpgradeFixture(t, phase)
				deleteDragonflySite(t, c, fg, site)

				step, err := r.reconcileDragonflySnapshotUpgrade(ctx, getFGByKey(t, c, fgNN(fg)))
				if err != nil {
					t.Fatalf("first pass: %v", err)
				}
				up := upgradeRecord(t, c, fg)
				switch want(phase, site) {
				case ended:
					if up == nil || up.Phase != v1alpha1.DragonflyUpgradePhaseFailed || up.Reason != v1alpha1.DragonflyUpgradeReasonActiveWorkloadDeleted {
						t.Fatalf("upgrade = %+v, want Failed/ActiveWorkloadDeleted", up)
					}
					if step.hold != 0 {
						t.Fatalf("an ended run still holds the reconcile (%+v), so the render cannot re-create the StatefulSet", step)
					}
				case recreated:
					if up == nil || up.Phase != phase {
						t.Fatalf("upgrade = %+v, want it still at %s", up, phase)
					}
					if img := liveDragonflyImage(t, c, fg, site); img != dfTargetImage {
						t.Fatalf("re-created StatefulSet on %q, want the target", img)
					}
					// The run keeps going: further passes never return the
					// NotFound that used to repeat forever.
					for pass := range 3 {
						if _, err := r.reconcileDragonflySnapshotUpgrade(ctx, getFGByKey(t, c, fgNN(fg))); err != nil {
							t.Fatalf("pass %d after re-creation: %v", pass, err)
						}
					}
				}
			})
		}
	}
}

// N-05: once the active's image has moved, a selector refusal on another
// site does not end the run while the active may still be loading its
// snapshot (ending it would give traffic back to a loading Dragonfly). The
// refusal is recorded at once and ends the run at ReattachingReplicas,
// after the active restored. The active's own refusal still ends the run.
func TestDragonflySnapshotUpgrade_ReplicaRefusalWaitsForActiveRestore(t *testing.T) {
	ctx := context.Background()
	r, c, fg, conn := inFlightUpgradeFixture(t, v1alpha1.DragonflyUpgradePhaseWaitingForActiveRestore)
	conn.persist = dragonfly.PersistenceInfo{Loading: true}
	// The run shed the active's traffic before restarting it.
	if err := setDragonflyTrafficOnSite(ctx, c, fg, "dc1", false); err != nil {
		t.Fatal(err)
	}
	_, rv := foreignSelectorDragonflySite(t, c, fg, "dc2")

	for pass := range 2 {
		step, err := r.reconcileDragonflySnapshotUpgrade(ctx, getFGByKey(t, c, fgNN(fg)))
		if err != nil || step.hold == 0 {
			t.Fatalf("pass %d while loading: step %+v err %v, want a hold", pass, step, err)
		}
	}
	if up := upgradeRecord(t, c, fg); up == nil || up.Phase != v1alpha1.DragonflyUpgradePhaseWaitingForActiveRestore {
		t.Fatalf("upgrade = %+v, want it still waiting for the active", up)
	}
	var active corev1.Pod
	if err := c.Get(ctx, types.NamespacedName{Namespace: fg.Namespace, Name: dragonflyStatefulSetName(fg.Name, "dc1") + "-0"}, &active); err != nil {
		t.Fatal(err)
	}
	if _, has := active.Labels[v1alpha1.DragonflyTrafficLabel]; has {
		t.Fatal("traffic was given back to the active while it loads its snapshot")
	}
	if reason, _ := r.Runner.refusalDegraded(fgNN(fg)); reason != v1alpha1.ReasonWorkloadSelectorMismatch {
		t.Fatalf("refusal = %q, want WorkloadSelectorMismatch recorded at once", reason)
	}

	// The active restores; the run reaches the step that touches the
	// refused replica and ends there, without writing it.
	conn.persist = dragonfly.PersistenceInfo{}
	for pass := range 2 {
		if _, err := r.reconcileDragonflySnapshotUpgrade(ctx, getFGByKey(t, c, fgNN(fg))); err != nil {
			t.Fatalf("pass %d after restore: %v", pass, err)
		}
	}
	up := upgradeRecord(t, c, fg)
	if up == nil || up.Phase != v1alpha1.DragonflyUpgradePhaseFailed || up.Reason != v1alpha1.DragonflyUpgradeReasonWorkloadSelectorMismatch {
		t.Fatalf("upgrade = %+v, want Failed/WorkloadSelectorMismatch at ReattachingReplicas", up)
	}
	if got := dragonflyStatefulSetResourceVersion(t, c, fg, "dc2"); got != rv {
		t.Fatalf("the refused StatefulSet was written (resourceVersion %s -> %s)", rv, got)
	}

	t.Run("active refused", func(t *testing.T) {
		r, c, fg, conn := inFlightUpgradeFixture(t, v1alpha1.DragonflyUpgradePhaseWaitingForActiveRestore)
		conn.persist = dragonfly.PersistenceInfo{Loading: true}
		foreignSelectorDragonflySite(t, c, fg, "dc1")
		if _, err := r.reconcileDragonflySnapshotUpgrade(ctx, getFGByKey(t, c, fgNN(fg))); err != nil {
			t.Fatal(err)
		}
		if up := upgradeRecord(t, c, fg); up == nil || up.Phase != v1alpha1.DragonflyUpgradePhaseFailed || up.Reason != v1alpha1.DragonflyUpgradeReasonWorkloadSelectorMismatch {
			t.Fatalf("upgrade = %+v, want Failed/WorkloadSelectorMismatch", up)
		}
	})
}
