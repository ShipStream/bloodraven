package controller

import (
	"context"
	"errors"
	"strings"
	"testing"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/tools/record"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"

	v1alpha1 "github.com/shipstream/bloodraven/api/v1alpha1"
	"github.com/shipstream/bloodraven/internal/dragonfly"
)

const (
	dfSourceImage = "docker.dragonflydb.io/dragonflydb/dragonfly:v1.38.0"
	dfTargetImage = "docker.dragonflydb.io/dragonflydb/dragonfly:v1.39.0"
)

// fgWithDragonflySnapshotUpgrade is a group on upgradeStrategy
// SnapshotRestore whose spec.dragonfly.image is dfTargetImage. Seed its
// StatefulSets on another image (seedDragonflyStatefulSets) to give it
// drift.
func fgWithDragonflySnapshotUpgrade() *v1alpha1.MysqlFailoverGroup {
	fg := fgWithDragonfly(func(fg *v1alpha1.MysqlFailoverGroup) {
		fg.Spec.Dragonfly.Image = dfTargetImage
		fg.Spec.Dragonfly.UpgradeStrategy = v1alpha1.DragonflyUpgradeStrategySnapshotRestore
		fg.Spec.Dragonfly.Snapshot = &v1alpha1.DragonflySnapshotSpec{
			Dir:                "s3://tenant-dragonfly/orders/prod",
			ServiceAccountName: "dragonfly-backup",
		}
	})
	fg.Status.ActiveSite = "dc1"
	fg.Status.Dragonfly = &v1alpha1.DragonflyStatus{ActiveSite: "dc1", Phase: v1alpha1.DragonflyPhaseReady}
	return fg
}

// seedDragonflyStatefulSets creates each named site's StatefulSet as the
// render would, running the given image.
func seedDragonflyStatefulSets(t *testing.T, r *MysqlFailoverGroupReconciler, c client.Client, fg *v1alpha1.MysqlFailoverGroup, images map[string]string) {
	t.Helper()
	for _, site := range fg.Spec.Sites {
		img, ok := images[site.Name]
		if !ok {
			continue
		}
		sts, err := r.desiredDragonflyStatefulSet(fg, site, nil)
		if err != nil {
			t.Fatal(err)
		}
		sts.Spec.Template.Spec.Containers[0].Image = img
		if err := c.Create(context.Background(), sts); err != nil {
			t.Fatal(err)
		}
	}
}

func liveDragonflyImage(t *testing.T, c client.Client, fg *v1alpha1.MysqlFailoverGroup, site string) string {
	t.Helper()
	var sts appsv1.StatefulSet
	if err := c.Get(context.Background(), types.NamespacedName{Namespace: fg.Namespace, Name: dragonflyStatefulSetName(fg.Name, site)}, &sts); err != nil {
		t.Fatal(err)
	}
	return dragonflyTemplateImage(&sts)
}

func upgradeRecord(t *testing.T, c client.Client, fg *v1alpha1.MysqlFailoverGroup) *v1alpha1.DragonflyUpgradeStatus {
	t.Helper()
	return dragonflyUpgradeStatus(getFGByKey(t, c, fgNN(fg)))
}

func bothSites(image string) map[string]string { return map[string]string{"dc1": image, "dc2": image} }

// A spec image that differs from the live StatefulSets starts a run, with
// the source taken from the live active site and the spec digest recorded;
// spec is not written.
func TestDragonflySnapshotUpgrade_SpecDriftStartsRun(t *testing.T) {
	ctx := context.Background()
	fg := fgWithDragonflySnapshotUpgrade()
	r, c := newReconciler(fg)
	// The active site runs another image than the replica: the source is
	// the active site's live image, never spec.
	seedDragonflyStatefulSets(t, r, c, fg, map[string]string{"dc1": "dragonfly:active-live", "dc2": dfSourceImage})

	step, err := r.reconcileDragonflySnapshotUpgrade(ctx, fg)
	if err != nil {
		t.Fatalf("reconcileDragonflySnapshotUpgrade: %v", err)
	}
	if step.hold == 0 {
		t.Fatal("a started run must hold the reconcile")
	}
	got := getFGByKey(t, c, fgNN(fg))
	if got.Spec.Dragonfly.Image != dfTargetImage {
		t.Fatalf("spec.dragonfly.image changed: %q", got.Spec.Dragonfly.Image)
	}
	up := dragonflyUpgradeStatus(got)
	if up == nil || up.Phase != v1alpha1.DragonflyUpgradePhasePending {
		t.Fatalf("upgrade = %+v, want Pending", up)
	}
	if up.SourceImage != "dragonfly:active-live" || up.TargetImage != dfTargetImage {
		t.Fatalf("images source=%q target=%q", up.SourceImage, up.TargetImage)
	}
	if up.SpecDigest != dragonflySpecDigest(fg.Spec.Dragonfly) || !strings.HasPrefix(up.SpecDigest, "sha256:") {
		t.Fatalf("specDigest = %q", up.SpecDigest)
	}
	if up.SnapshotDir != "s3://tenant-dragonfly/orders/prod" || up.ActiveSite != "dc1" {
		t.Fatalf("snapshotDir=%q activeSite=%q", up.SnapshotDir, up.ActiveSite)
	}
}

// OPUS-18: an unknown live image (no StatefulSet yet, so a new group or one
// whose status was lost) starts nothing; neither does a Rolling group, or
// a group already on the spec image.
func TestDragonflySnapshotUpgrade_NoRunWithoutKnownDrift(t *testing.T) {
	for _, tc := range []struct {
		name   string
		mutate func(*v1alpha1.MysqlFailoverGroup)
		live   map[string]string
	}{
		{name: "no StatefulSets", live: nil},
		{name: "active site's StatefulSet missing", live: map[string]string{"dc2": dfSourceImage}},
		{name: "no drift", live: bothSites(dfTargetImage)},
		{name: "Rolling strategy", live: bothSites(dfSourceImage), mutate: func(fg *v1alpha1.MysqlFailoverGroup) {
			fg.Spec.Dragonfly.UpgradeStrategy = v1alpha1.DragonflyUpgradeStrategyRolling
		}},
		{name: "Dragonfly disabled", live: bothSites(dfSourceImage), mutate: func(fg *v1alpha1.MysqlFailoverGroup) {
			fg.Spec.Dragonfly.Enabled = false
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			fg := fgWithDragonflySnapshotUpgrade()
			if tc.mutate != nil {
				tc.mutate(fg)
			}
			r, c := newReconciler(fg)
			seedDragonflyStatefulSets(t, r, c, fg, tc.live)
			step, err := r.reconcileDragonflySnapshotUpgrade(context.Background(), fg)
			if err != nil || step != (dragonflyUpgradeStep{}) {
				t.Fatalf("= (%+v, %v), want nothing", step, err)
			}
			if up := upgradeRecord(t, c, fg); up != nil {
				t.Fatalf("a run started: %+v", up)
			}
		})
	}
}

// failingSaveFixture is a group with drift on both sites whose active
// Dragonfly answers SAVE with an error.
func failingSaveFixture(t *testing.T) (*MysqlFailoverGroupReconciler, client.Client, *v1alpha1.MysqlFailoverGroup, *fakeDragonflyConn, *record.FakeRecorder) {
	t.Helper()
	fg := fgWithDragonflySnapshotUpgrade()
	r, c := newReconciler(fg)
	rec := record.NewFakeRecorder(64)
	r.Recorder = rec
	seedDragonflyStatefulSets(t, r, c, fg, bothSites(dfSourceImage))
	for _, site := range []string{"dc1", "dc2"} {
		if err := c.Create(context.Background(), dragonflyPodForTest(fg, site, dfSourceImage, true)); err != nil {
			t.Fatal(err)
		}
	}
	conn := &fakeDragonflyConn{info: dragonfly.ReplicationInfo{Role: "master"}, saveErr: errors.New("disk full")}
	r.dragonflyConnector = func(context.Context, string, string) (DragonflyConnection, error) { return conn, nil }
	return r, c, fg, conn, rec
}

// driveToFailure runs the upgrade until its record is Failed.
func driveToFailure(t *testing.T, r *MysqlFailoverGroupReconciler, c client.Client, fg *v1alpha1.MysqlFailoverGroup) *v1alpha1.DragonflyUpgradeStatus {
	t.Helper()
	for i := 0; i < 5; i++ {
		if _, err := r.reconcileDragonflySnapshotUpgrade(context.Background(), getFGByKey(t, c, fgNN(fg))); err != nil {
			t.Fatal(err)
		}
		if up := upgradeRecord(t, c, fg); up != nil && up.Phase == v1alpha1.DragonflyUpgradePhaseFailed {
			return up
		}
	}
	t.Fatalf("the run did not fail: %+v", upgradeRecord(t, c, fg))
	return nil
}

func countMatching(events []string, needle string) int {
	n := 0
	for _, e := range events {
		if strings.Contains(e, needle) {
			n++
		}
	}
	return n
}

// F-03: a failed SAVE ends in one terminal run. While spec.dragonfly is
// unchanged no new run starts, nothing restarts, the Warning is not
// repeated, and the ordinary render keeps every site on its live image.
func TestDragonflySnapshotUpgrade_FailedSaveIsTerminal(t *testing.T) {
	ctx := context.Background()
	r, c, fg, conn, rec := failingSaveFixture(t)
	failed := driveToFailure(t, r, c, fg)
	if failed.Reason != v1alpha1.DragonflyUpgradeReasonSnapshotSaveFailed {
		t.Fatalf("reason = %q, want %s", failed.Reason, v1alpha1.DragonflyUpgradeReasonSnapshotSaveFailed)
	}
	if n := countMatching(drainEvents(rec), "Warning DragonflyUpgradeFailed"); n != 1 {
		t.Fatalf("%d DragonflyUpgradeFailed events, want 1", n)
	}

	for i := 0; i < 3; i++ {
		live := getFGByKey(t, c, fgNN(fg))
		step, err := r.reconcileDragonflySnapshotUpgrade(ctx, live)
		if err != nil || step != (dragonflyUpgradeStep{}) {
			t.Fatalf("pass %d after the failure = (%+v, %v), want nothing", i, step, err)
		}
		if _, err := r.reconcileDragonflyResources(ctx, live); err != nil {
			t.Fatal(err)
		}
	}
	up := upgradeRecord(t, c, fg)
	if up.Phase != v1alpha1.DragonflyUpgradePhaseFailed || !up.StartTime.Equal(failed.StartTime) {
		t.Fatalf("record = %+v, want the same Failed run", up)
	}
	if conn.saveCalls != 1 {
		t.Fatalf("SAVE ran %d times, want once", conn.saveCalls)
	}
	if events := drainEvents(rec); countMatching(events, "DragonflyUpgrade") != 0 {
		t.Fatalf("upgrade events after the terminal run: %v", events)
	}
	for _, site := range []string{"dc1", "dc2"} {
		if img := liveDragonflyImage(t, c, fg, site); img != dfSourceImage {
			t.Fatalf("site %s moved to %q after the failure", site, img)
		}
		var pod corev1.Pod
		if err := c.Get(ctx, types.NamespacedName{Namespace: fg.Namespace, Name: dragonflyStatefulSetName(fg.Name, site) + "-0"}, &pod); err != nil {
			t.Fatalf("site %s pod was restarted: %v", site, err)
		}
	}
}

// F-03 re-arm: any change to spec.dragonfly starts a new run for the same
// target.
func TestDragonflySnapshotUpgrade_SpecChangeRearms(t *testing.T) {
	ctx := context.Background()
	r, c, fg, conn, _ := failingSaveFixture(t)
	failed := driveToFailure(t, r, c, fg)

	live := getFGByKey(t, c, fgNN(fg))
	live.Spec.Dragonfly.Snapshot.Dir = "s3://tenant-dragonfly/orders/fixed"
	if err := c.Update(ctx, live); err != nil {
		t.Fatal(err)
	}
	conn.saveErr = nil
	step, err := r.reconcileDragonflySnapshotUpgrade(ctx, getFGByKey(t, c, fgNN(fg)))
	if err != nil || step.hold == 0 {
		t.Fatalf("= (%+v, %v), want a new run", step, err)
	}
	up := upgradeRecord(t, c, fg)
	if up.Phase != v1alpha1.DragonflyUpgradePhasePending || up.SpecDigest == failed.SpecDigest ||
		up.SnapshotDir != "s3://tenant-dragonfly/orders/fixed" {
		t.Fatalf("record = %+v, want a new Pending run on the changed spec", up)
	}
}

// The two-step retry: setting the image back to the running one clears the
// drift and the Failed record; setting it forward again starts a new run.
func TestDragonflySnapshotUpgrade_TwoStepRetry(t *testing.T) {
	ctx := context.Background()
	r, c, fg, _, _ := failingSaveFixture(t)
	driveToFailure(t, r, c, fg)

	setImage := func(image string) {
		live := getFGByKey(t, c, fgNN(fg))
		live.Spec.Dragonfly.Image = image
		if err := c.Update(ctx, live); err != nil {
			t.Fatal(err)
		}
	}
	setImage(dfSourceImage)
	if _, err := r.reconcileDragonflySnapshotUpgrade(ctx, getFGByKey(t, c, fgNN(fg))); err != nil {
		t.Fatal(err)
	}
	if up := upgradeRecord(t, c, fg); up != nil {
		t.Fatalf("the Failed record outlived the drift: %+v", up)
	}
	setImage(dfTargetImage)
	step, err := r.reconcileDragonflySnapshotUpgrade(ctx, getFGByKey(t, c, fgNN(fg)))
	if err != nil || step.hold == 0 {
		t.Fatalf("= (%+v, %v), want a new run", step, err)
	}
	if up := upgradeRecord(t, c, fg); up == nil || up.Phase != v1alpha1.DragonflyUpgradePhasePending {
		t.Fatalf("record = %+v, want Pending", up)
	}
}

// F-03: a run blocked by a missing activeSite or a planned failover waits
// (it is not dropped and does not hold the reconcile), the render keeps
// every site's live image meanwhile, the deferral Event is not repeated
// for the same cause, and the run starts once the blocker clears.
func TestDragonflySnapshotUpgrade_BlockedRunWaitsWithoutMovingSites(t *testing.T) {
	for _, tc := range []struct {
		name          string
		block, unlock func(*v1alpha1.MysqlFailoverGroup)
	}{
		{name: "no activeSite",
			block:  func(fg *v1alpha1.MysqlFailoverGroup) { fg.Status.ActiveSite = "" },
			unlock: func(fg *v1alpha1.MysqlFailoverGroup) { fg.Status.ActiveSite = "dc1" }},
		{name: "planned failover hold",
			block: func(fg *v1alpha1.MysqlFailoverGroup) {
				setPlannedHold(&fg.Status, v1alpha1.PlannedFailoverHold{RunID: "run-1", Reason: v1alpha1.PlannedFailoverReasonAuthorityChanged,
					SourceSite: "dc1", TargetSite: "dc2", HoldTime: metav1.Now()}, fg.Generation)
			},
			unlock: func(fg *v1alpha1.MysqlFailoverGroup) {
				fg.Status.PlannedFailoverHold = nil
				fg.Status.Conditions = nil
			}},
		{name: "Dragonfly master elsewhere",
			block:  func(fg *v1alpha1.MysqlFailoverGroup) { fg.Status.Dragonfly.ActiveSite = "dc2" },
			unlock: func(fg *v1alpha1.MysqlFailoverGroup) { fg.Status.Dragonfly.ActiveSite = "dc1" }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctx := context.Background()
			fg := fgWithDragonflySnapshotUpgrade()
			tc.block(fg)
			r, c := newReconciler(fg)
			rec := record.NewFakeRecorder(64)
			r.Recorder = rec
			seedDragonflyStatefulSets(t, r, c, fg, bothSites(dfSourceImage))

			for i := 0; i < 2; i++ {
				live := getFGByKey(t, c, fgNN(fg))
				step, err := r.reconcileDragonflySnapshotUpgrade(ctx, live)
				if err != nil || step.hold != 0 || step.wait != dragonflyUpgradeDeferRetry {
					t.Fatalf("pass %d = (%+v, %v), want a %s wait", i, step, err, dragonflyUpgradeDeferRetry)
				}
				if _, err := r.reconcileDragonflyResources(ctx, live); err != nil {
					t.Fatal(err)
				}
			}
			if up := upgradeRecord(t, c, fg); up != nil {
				t.Fatalf("a blocked run was recorded: %+v", up)
			}
			for _, site := range []string{"dc1", "dc2"} {
				if img := liveDragonflyImage(t, c, fg, site); img != dfSourceImage {
					t.Fatalf("the render moved site %s to %q while the run waited", site, img)
				}
			}
			if n := countMatching(drainEvents(rec), "Normal DragonflyUpgradeDeferred"); n != 1 {
				t.Fatalf("%d DragonflyUpgradeDeferred events over two passes, want 1", n)
			}

			live := getFGByKey(t, c, fgNN(fg))
			tc.unlock(live)
			if err := c.Status().Update(ctx, live); err != nil {
				t.Fatal(err)
			}
			step, err := r.reconcileDragonflySnapshotUpgrade(ctx, getFGByKey(t, c, fgNN(fg)))
			if err != nil || step.hold == 0 {
				t.Fatalf("after the blocker cleared = (%+v, %v), want a started run", step, err)
			}
			if up := upgradeRecord(t, c, fg); up == nil || up.Phase != v1alpha1.DragonflyUpgradePhasePending {
				t.Fatalf("record = %+v, want Pending", up)
			}
		})
	}
}

// Under SnapshotRestore the ordinary render keeps each site's live image;
// under Rolling it renders spec.dragonfly.image; a new StatefulSet starts on
// spec.dragonfly.image either way.
func TestDragonflyRender_SnapshotRestoreKeepsLiveImage(t *testing.T) {
	ctx := context.Background()
	fg := fgWithDragonflySnapshotUpgrade()
	r, c := newReconciler(fg)
	seedDragonflyStatefulSets(t, r, c, fg, map[string]string{"dc1": dfSourceImage})
	if _, err := r.reconcileDragonflyResources(ctx, fg); err != nil {
		t.Fatal(err)
	}
	if img := liveDragonflyImage(t, c, fg, "dc1"); img != dfSourceImage {
		t.Fatalf("SnapshotRestore render moved the existing site to %q", img)
	}
	if img := liveDragonflyImage(t, c, fg, "dc2"); img != dfTargetImage {
		t.Fatalf("a new site started on %q, want spec.dragonfly.image", img)
	}
	if got := getFGByKey(t, c, fgNN(fg)); got.Status.Dragonfly == nil || got.Status.Dragonfly.Image != dfSourceImage {
		t.Fatalf("status.dragonfly.image = %+v, want the active site's live image", got.Status.Dragonfly)
	}

	live := getFGByKey(t, c, fgNN(fg))
	live.Spec.Dragonfly.UpgradeStrategy = v1alpha1.DragonflyUpgradeStrategyRolling
	desired, err := r.desiredDragonflyStatefulSet(live, live.Spec.Sites[0], nil)
	if err != nil {
		t.Fatal(err)
	}
	if img := dragonflyTemplateImage(desired); img != dfTargetImage {
		t.Fatalf("Rolling renders %q, want spec.dragonfly.image", img)
	}
}

// F-03: a run interrupted after the active site moved resumes after an
// operator restart from its record, and, with the record lost, from the
// per-site drift: no second SAVE and no restart of the active site.
func TestDragonflySnapshotUpgrade_ResumesAfterRestart(t *testing.T) {
	for _, tc := range []struct {
		name   string
		record bool
	}{
		{name: "record kept", record: true},
		{name: "record lost", record: false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctx := context.Background()
			fg := fgWithDragonflySnapshotUpgrade()
			if tc.record {
				start := metav1.Now()
				fg.Status.Dragonfly.Upgrade = &v1alpha1.DragonflyUpgradeStatus{
					Phase: v1alpha1.DragonflyUpgradePhaseReattachingReplicas, SourceImage: dfSourceImage, TargetImage: dfTargetImage,
					SpecDigest: dragonflySpecDigest(fg.Spec.Dragonfly), ActiveSite: "dc1", SnapshotDir: fg.Spec.Dragonfly.Snapshot.Dir, StartTime: &start,
				}
			}
			_, c := newReconciler(fg)
			// A fresh reconciler on the same API state: the operator restarted.
			r := &MysqlFailoverGroupReconciler{Client: c, Scheme: testScheme(), Recorder: record.NewFakeRecorder(64)}
			seedDragonflyStatefulSets(t, r, c, fg, map[string]string{"dc1": dfTargetImage, "dc2": dfSourceImage})
			active := dragonflyPodForTest(fg, "dc1", dfTargetImage, true)
			for _, pod := range []*corev1.Pod{active, dragonflyPodForTest(fg, "dc2", dfSourceImage, true)} {
				if err := c.Create(ctx, pod); err != nil {
					t.Fatal(err)
				}
			}
			conn := &fakeDragonflyConn{info: dragonfly.ReplicationInfo{Role: "master"}}
			r.dragonflyConnector = func(context.Context, string, string) (DragonflyConnection, error) { return conn, nil }

			for i := 0; i < 3; i++ {
				if _, err := r.reconcileDragonflySnapshotUpgrade(ctx, getFGByKey(t, c, fgNN(fg))); err != nil {
					t.Fatal(err)
				}
			}
			up := upgradeRecord(t, c, fg)
			if up == nil || up.Phase != v1alpha1.DragonflyUpgradePhaseReattachingReplicas {
				t.Fatalf("record = %+v, want a run reattaching the replicas", up)
			}
			if !tc.record && (up.SourceImage != dfSourceImage || up.TargetImage != dfTargetImage) {
				t.Fatalf("resumed run images source=%q target=%q", up.SourceImage, up.TargetImage)
			}
			if conn.saveCalls != 0 {
				t.Fatalf("SAVE ran %d times on a resumed run", conn.saveCalls)
			}
			if err := c.Get(ctx, client.ObjectKeyFromObject(active), &corev1.Pod{}); err != nil {
				t.Fatalf("the already-moved active site was restarted: %v", err)
			}
			if img := liveDragonflyImage(t, c, fg, "dc2"); img != dfTargetImage {
				t.Fatalf("replica StatefulSet on %q, want the target", img)
			}
		})
	}
}

// The operator never writes spec: a whole run, from drift to Succeeded,
// goes through without one Update or Patch of the group's main resource,
// and spec.dragonfly.image keeps the user's value.
func TestDragonflySnapshotUpgrade_NeverWritesSpec(t *testing.T) {
	ctx := context.Background()
	fg := fgWithDragonflySnapshotUpgrade()
	specWrites := 0
	countSpecWrite := func(obj client.Object) {
		if _, ok := obj.(*v1alpha1.MysqlFailoverGroup); ok {
			specWrites++
		}
	}
	c := fake.NewClientBuilder().WithScheme(testScheme()).WithStatusSubresource(&v1alpha1.MysqlFailoverGroup{}).
		WithObjects(fg).WithInterceptorFuncs(interceptor.Funcs{
		Update: func(ctx context.Context, cl client.WithWatch, obj client.Object, opts ...client.UpdateOption) error {
			countSpecWrite(obj)
			return cl.Update(ctx, obj, opts...)
		},
		Patch: func(ctx context.Context, cl client.WithWatch, obj client.Object, patch client.Patch, opts ...client.PatchOption) error {
			countSpecWrite(obj)
			return cl.Patch(ctx, obj, patch, opts...)
		},
	}).Build()
	rec := record.NewFakeRecorder(128)
	r := &MysqlFailoverGroupReconciler{Client: c, Scheme: testScheme(), Recorder: rec}
	seedDragonflyStatefulSets(t, r, c, fg, bothSites(dfSourceImage))
	for _, site := range []string{"dc1", "dc2"} {
		if err := c.Create(ctx, dragonflyPodForTest(fg, site, dfSourceImage, true)); err != nil {
			t.Fatal(err)
		}
	}
	conns := map[string]*fakeDragonflyConn{
		"dc1": {info: dragonfly.ReplicationInfo{Role: "master"}},
		"dc2": {info: dragonfly.ReplicationInfo{Role: "slave", MasterLinkStatus: "up"}},
	}
	r.dragonflyConnector = func(_ context.Context, addr, _ string) (DragonflyConnection, error) {
		for site, conn := range conns {
			if strings.HasPrefix(addr, dragonflySiteServiceName(fg.Name, site)+".") {
				return conn, nil
			}
		}
		return nil, errors.New("unknown address " + addr)
	}
	// kubelet: a site whose pod was deleted gets one on its StatefulSet's
	// current image.
	recreatePods := func() {
		for _, site := range []string{"dc1", "dc2"} {
			key := types.NamespacedName{Namespace: fg.Namespace, Name: dragonflyStatefulSetName(fg.Name, site) + "-0"}
			if err := c.Get(ctx, key, &corev1.Pod{}); apierrors.IsNotFound(err) {
				if err := c.Create(ctx, dragonflyPodForTest(fg, site, liveDragonflyImage(t, c, fg, site), true)); err != nil {
					t.Fatal(err)
				}
			}
		}
	}

	for i := 0; i < 20; i++ {
		if _, err := r.reconcileDragonflySnapshotUpgrade(ctx, getFGByKey(t, c, fgNN(fg))); err != nil {
			t.Fatal(err)
		}
		recreatePods()
		if up := upgradeRecord(t, c, fg); up != nil && up.Phase == v1alpha1.DragonflyUpgradePhaseSucceeded {
			break
		}
	}
	up := upgradeRecord(t, c, fg)
	if up == nil || up.Phase != v1alpha1.DragonflyUpgradePhaseSucceeded {
		t.Fatalf("record = %+v, want Succeeded", up)
	}
	if specWrites != 0 {
		t.Fatalf("the upgrade wrote the group's main resource %d times", specWrites)
	}
	if got := getFGByKey(t, c, fgNN(fg)); got.Spec.Dragonfly.Image != dfTargetImage {
		t.Fatalf("spec.dragonfly.image = %q", got.Spec.Dragonfly.Image)
	}
	for _, site := range []string{"dc1", "dc2"} {
		if img := liveDragonflyImage(t, c, fg, site); img != dfTargetImage {
			t.Fatalf("site %s on %q after the run, want the target", site, img)
		}
	}
	if n := countMatching(drainEvents(rec), "Normal DragonflyUpgradeSucceeded"); n != 1 {
		t.Fatalf("%d DragonflyUpgradeSucceeded events, want 1", n)
	}
	// Converged: the next pass starts nothing.
	if step, err := r.reconcileDragonflySnapshotUpgrade(ctx, getFGByKey(t, c, fgNN(fg))); err != nil || step != (dragonflyUpgradeStep{}) {
		t.Fatalf("after Succeeded = (%+v, %v), want nothing", step, err)
	}
}

func TestDragonflySnapshotUpgrade_SaveThenUpdateActive(t *testing.T) {
	ctx := context.Background()
	fg := fgWithDragonflySnapshotUpgrade()
	now := metav1.Now()
	fg.Status.Dragonfly.Upgrade = &v1alpha1.DragonflyUpgradeStatus{
		Phase:       v1alpha1.DragonflyUpgradePhasePending,
		SourceImage: dfSourceImage,
		TargetImage: dfTargetImage,
		ActiveSite:  "dc1",
		SnapshotDir: fg.Spec.Dragonfly.Snapshot.Dir,
		StartTime:   &now,
	}
	r, c := newReconciler(fg)
	seedDragonflyStatefulSets(t, r, c, fg, bothSites(dfSourceImage))
	oldMasterPod := dragonflyPodForTest(fg, "dc1", dfSourceImage, true)
	if err := c.Create(ctx, oldMasterPod); err != nil {
		t.Fatalf("create pod: %v", err)
	}
	conn := &fakeDragonflyConn{info: dragonfly.ReplicationInfo{Role: "master"}}
	r.dragonflyConnector = func(context.Context, string, string) (DragonflyConnection, error) { return conn, nil }

	step, err := r.reconcileDragonflySnapshotUpgrade(ctx, fg)
	if err != nil || step.hold == 0 {
		t.Fatalf("pending reconcile step=%+v err=%v", step, err)
	}
	if err := c.Get(ctx, types.NamespacedName{Name: fg.Name, Namespace: fg.Namespace}, fg); err != nil {
		t.Fatal(err)
	}
	if fg.Status.Dragonfly.Upgrade.Phase != v1alpha1.DragonflyUpgradePhaseSavingSnapshot {
		t.Fatalf("phase=%q want SavingSnapshot", fg.Status.Dragonfly.Upgrade.Phase)
	}

	step, err = r.reconcileDragonflySnapshotUpgrade(ctx, fg)
	if err != nil || step.hold == 0 {
		t.Fatalf("save reconcile step=%+v err=%v", step, err)
	}
	if conn.saveCalls != 1 {
		t.Fatalf("saveCalls=%d want 1", conn.saveCalls)
	}
	if err := c.Get(ctx, types.NamespacedName{Name: fg.Name, Namespace: fg.Namespace}, fg); err != nil {
		t.Fatal(err)
	}
	if fg.Status.Dragonfly.Upgrade.Phase != v1alpha1.DragonflyUpgradePhaseUpdatingActive {
		t.Fatalf("phase=%q want UpdatingActive", fg.Status.Dragonfly.Upgrade.Phase)
	}

	step, err = r.reconcileDragonflySnapshotUpgrade(ctx, fg)
	if err != nil || step.hold == 0 {
		t.Fatalf("update-active reconcile step=%+v err=%v", step, err)
	}
	var deleted corev1.Pod
	if err := c.Get(ctx, types.NamespacedName{Name: oldMasterPod.Name, Namespace: oldMasterPod.Namespace}, &deleted); err == nil && deleted.DeletionTimestamp == nil {
		t.Fatal("active pod was not deleted for restart")
	}
	if img := liveDragonflyImage(t, c, fg, "dc1"); img != dfTargetImage {
		t.Fatalf("active StatefulSet on %q, want the target", img)
	}
}

func dragonflyPodForTest(fg *v1alpha1.MysqlFailoverGroup, site, image string, ready bool) *corev1.Pod {
	labels := dragonflyCommonLabels(fg.Name, site)
	labels[v1alpha1.DragonflyRoleLabel] = "replica"
	labels[v1alpha1.DragonflyTrafficLabel] = dragonflyTrafficEnabled
	if site == fg.Status.ActiveSite {
		labels[v1alpha1.DragonflyRoleLabel] = "master"
	}
	status := corev1.PodStatus{}
	if ready {
		status.Conditions = []corev1.PodCondition{{Type: corev1.PodReady, Status: corev1.ConditionTrue}}
	}
	return &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      dragonflyStatefulSetName(fg.Name, site) + "-0",
			Namespace: fg.Namespace,
			Labels:    labels,
		},
		Spec:   corev1.PodSpec{Containers: []corev1.Container{{Name: dragonflyContainerName, Image: image}}},
		Status: status,
	}
}

func TestDragonflyUpgradeInFlight(t *testing.T) {
	if dragonflyUpgradeInFlight(nil) {
		t.Fatal("nil status is not in-flight")
	}
	if !dragonflyUpgradeInFlight(&v1alpha1.DragonflyUpgradeStatus{Phase: v1alpha1.DragonflyUpgradePhaseSavingSnapshot}) {
		t.Fatal("SavingSnapshot should be in-flight")
	}
	if dragonflyUpgradeInFlight(&v1alpha1.DragonflyUpgradeStatus{Phase: v1alpha1.DragonflyUpgradePhaseSucceeded}) {
		t.Fatal("Succeeded should be terminal")
	}
}

// foreignSelectorDragonflySite gives site's seeded StatefulSet an extra
// immutable selector label, as a StatefulSet created by hand or by
// another release would carry, and returns its resourceVersion.
func foreignSelectorDragonflySite(t *testing.T, c client.Client, fg *v1alpha1.MysqlFailoverGroup, site string) (string, string) {
	t.Helper()
	ctx := context.Background()
	key := types.NamespacedName{Namespace: fg.Namespace, Name: dragonflyStatefulSetName(fg.Name, site)}
	var sts appsv1.StatefulSet
	if err := c.Get(ctx, key, &sts); err != nil {
		t.Fatal(err)
	}
	sts.Spec.Selector.MatchLabels["tier"] = "cache"
	if err := c.Update(ctx, &sts); err != nil {
		t.Fatal(err)
	}
	if err := c.Get(ctx, key, &sts); err != nil {
		t.Fatal(err)
	}
	return key.Name, sts.ResourceVersion
}

func dragonflyStatefulSetResourceVersion(t *testing.T, c client.Client, fg *v1alpha1.MysqlFailoverGroup, site string) string {
	t.Helper()
	var sts appsv1.StatefulSet
	if err := c.Get(context.Background(), types.NamespacedName{Namespace: fg.Namespace, Name: dragonflyStatefulSetName(fg.Name, site)}, &sts); err != nil {
		t.Fatal(err)
	}
	return sts.ResourceVersion
}

// R1-04 (A-77): a snapshot-restore run is not admitted while any site's
// StatefulSet is refused for its selector, and a run in flight when one
// becomes refused stops before saving, restarting or updating any site.
// The refusal is recorded as the render records it.
func TestDragonflySnapshotUpgrade_HonoursSelectorRefusal(t *testing.T) {
	for _, refusedSite := range []string{"dc1", "dc2"} {
		t.Run("admission/"+refusedSite, func(t *testing.T) {
			ctx := context.Background()
			fg := fgWithDragonflySnapshotUpgrade()
			r, c := newReconciler(fg)
			rec := record.NewFakeRecorder(64)
			r.Recorder = rec
			r.Runner = NewTopologyManagerRunner(c, nil, nil, rec, testLogger())
			seedDragonflyStatefulSets(t, r, c, fg, bothSites(dfSourceImage))
			name, rv := foreignSelectorDragonflySite(t, c, fg, refusedSite)
			conn := &fakeDragonflyConn{info: dragonfly.ReplicationInfo{Role: "master"}}
			r.dragonflyConnector = func(context.Context, string, string) (DragonflyConnection, error) { return conn, nil }

			for pass := range 3 {
				step, err := r.reconcileDragonflySnapshotUpgrade(ctx, getFGByKey(t, c, fgNN(fg)))
				if err != nil {
					t.Fatalf("pass %d: %v", pass, err)
				}
				if step.hold != 0 || step.wait == 0 {
					t.Fatalf("pass %d: step %+v, want a wait without a hold", pass, step)
				}
			}
			if up := upgradeRecord(t, c, fg); up != nil {
				t.Fatalf("a run was admitted with %s refused: %+v", name, up)
			}
			if got := dragonflyStatefulSetResourceVersion(t, c, fg, refusedSite); got != rv {
				t.Fatalf("the refused StatefulSet was written (resourceVersion %s -> %s)", rv, got)
			}
			if conn.saveCalls != 0 {
				t.Fatalf("SAVE ran %d times", conn.saveCalls)
			}
			if reason, msg := r.Runner.refusalDegraded(fgNN(fg)); reason != v1alpha1.ReasonWorkloadSelectorMismatch || !strings.Contains(msg, name) {
				t.Fatalf("refusal = %s %q, want WorkloadSelectorMismatch naming %s", reason, msg, name)
			}
			events := drainEventReasons(rec)
			if n := countMatching(events, v1alpha1.EventWorkloadSelectorMismatch); n != 1 {
				t.Fatalf("%d WorkloadSelectorMismatch Events (%v), want 1", n, events)
			}
			if n := countMatching(events, v1alpha1.EventDragonflyUpgradeDeferred); n != 1 {
				t.Fatalf("%d DragonflyUpgradeDeferred Events (%v), want 1", n, events)
			}
		})
	}

	for _, phase := range []v1alpha1.DragonflyUpgradePhase{
		v1alpha1.DragonflyUpgradePhasePending,
		v1alpha1.DragonflyUpgradePhaseSavingSnapshot,
		v1alpha1.DragonflyUpgradePhaseUpdatingActive,
		v1alpha1.DragonflyUpgradePhaseReattachingReplicas,
	} {
		for _, refusedSite := range []string{"dc1", "dc2"} {
			t.Run("inFlight/"+string(phase)+"/"+refusedSite, func(t *testing.T) {
				ctx := context.Background()
				fg := fgWithDragonflySnapshotUpgrade()
				now := metav1.Now()
				fg.Status.Dragonfly.Upgrade = &v1alpha1.DragonflyUpgradeStatus{
					Phase: phase, SourceImage: dfSourceImage, TargetImage: dfTargetImage, ActiveSite: "dc1",
					SnapshotDir: fg.Spec.Dragonfly.Snapshot.Dir, SpecDigest: dragonflySpecDigest(fg.Spec.Dragonfly), StartTime: &now,
				}
				r, c := newReconciler(fg)
				r.Runner = NewTopologyManagerRunner(c, nil, nil, record.NewFakeRecorder(64), testLogger())
				seedDragonflyStatefulSets(t, r, c, fg, bothSites(dfSourceImage))
				_, rv := foreignSelectorDragonflySite(t, c, fg, refusedSite)
				var pods []*corev1.Pod
				for _, site := range []string{"dc1", "dc2"} {
					p := dragonflyPodForTest(fg, site, dfSourceImage, true)
					if err := c.Create(ctx, p); err != nil {
						t.Fatal(err)
					}
					pods = append(pods, p)
				}
				conn := &fakeDragonflyConn{info: dragonfly.ReplicationInfo{Role: "master"}}
				r.dragonflyConnector = func(context.Context, string, string) (DragonflyConnection, error) { return conn, nil }

				if _, err := r.reconcileDragonflySnapshotUpgrade(ctx, getFGByKey(t, c, fgNN(fg))); err != nil {
					t.Fatal(err)
				}
				up := upgradeRecord(t, c, fg)
				if up == nil || up.Phase != v1alpha1.DragonflyUpgradePhaseFailed || up.Reason != v1alpha1.DragonflyUpgradeReasonWorkloadSelectorMismatch {
					t.Fatalf("upgrade = %+v, want Failed/WorkloadSelectorMismatch", up)
				}
				if got := dragonflyStatefulSetResourceVersion(t, c, fg, refusedSite); got != rv {
					t.Fatalf("the refused StatefulSet was written (resourceVersion %s -> %s)", rv, got)
				}
				for _, site := range []string{"dc1", "dc2"} {
					if img := liveDragonflyImage(t, c, fg, site); img != dfSourceImage {
						t.Fatalf("site %s StatefulSet moved to %q", site, img)
					}
				}
				for _, p := range pods {
					var got corev1.Pod
					if err := c.Get(ctx, client.ObjectKeyFromObject(p), &got); err != nil || got.DeletionTimestamp != nil {
						t.Fatalf("pod %s was deleted (%v)", p.Name, err)
					}
				}
				if conn.saveCalls != 0 {
					t.Fatalf("SAVE ran %d times", conn.saveCalls)
				}
				if reason, _ := r.Runner.refusalDegraded(fgNN(fg)); reason != v1alpha1.ReasonWorkloadSelectorMismatch {
					t.Fatalf("refusal = %q, want WorkloadSelectorMismatch", reason)
				}
			})
		}
	}
}
