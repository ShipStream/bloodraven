package controller

import (
	"context"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"

	v1alpha1 "github.com/shipstream/bloodraven/api/v1alpha1"
	"github.com/shipstream/bloodraven/internal/dragonfly"
)

func fgWithDragonflySnapshotUpgrade() *v1alpha1.MysqlFailoverGroup {
	fg := fgWithDragonfly(func(fg *v1alpha1.MysqlFailoverGroup) {
		fg.Spec.Dragonfly.Snapshot = &v1alpha1.DragonflySnapshotSpec{
			Dir:                "s3://tenant-dragonfly/orders/prod",
			ServiceAccountName: "dragonfly-backup",
		}
	})
	fg.Status.ActiveSite = "dc1"
	fg.Status.Dragonfly = &v1alpha1.DragonflyStatus{Enabled: true, ActiveSite: "dc1", Phase: v1alpha1.DragonflyPhaseReady}
	fg.Annotations = map[string]string{DragonflySnapshotUpgradeAnnotation: "docker.dragonflydb.io/dragonflydb/dragonfly:v1.39.0"}
	return fg
}

func TestDragonflySnapshotUpgrade_AcceptClearsAnnotationAndStampsStatus(t *testing.T) {
	ctx := context.Background()
	fg := fgWithDragonflySnapshotUpgrade()
	r, c := newReconciler(fg)

	d, err := r.reconcileDragonflySnapshotUpgrade(ctx, fg)
	if err != nil {
		t.Fatalf("reconcileDragonflySnapshotUpgrade: %v", err)
	}
	if d == 0 {
		t.Fatal("expected requeue after accepting upgrade")
	}

	var got v1alpha1.MysqlFailoverGroup
	if err := c.Get(ctx, types.NamespacedName{Name: fg.Name, Namespace: fg.Namespace}, &got); err != nil {
		t.Fatal(err)
	}
	if got.Spec.Dragonfly.Image != "docker.dragonflydb.io/dragonflydb/dragonfly:v1.38.0" {
		t.Fatalf("spec.dragonfly.image changed before upgrade success: %q", got.Spec.Dragonfly.Image)
	}
	if _, ok := got.Annotations[DragonflySnapshotUpgradeAnnotation]; ok {
		t.Fatal("upgrade annotation was not removed")
	}
	if got.Status.Dragonfly == nil || got.Status.Dragonfly.Upgrade == nil {
		t.Fatalf("status.dragonfly.upgrade missing: %#v", got.Status.Dragonfly)
	}
	up := got.Status.Dragonfly.Upgrade
	if up.Phase != v1alpha1.DragonflyUpgradePhasePending {
		t.Fatalf("phase=%q want Pending", up.Phase)
	}
	if up.SourceImage != "docker.dragonflydb.io/dragonflydb/dragonfly:v1.38.0" || up.TargetImage != "docker.dragonflydb.io/dragonflydb/dragonfly:v1.39.0" {
		t.Fatalf("images source=%q target=%q", up.SourceImage, up.TargetImage)
	}
	if up.SnapshotDir != "s3://tenant-dragonfly/orders/prod" {
		t.Fatalf("snapshotDir=%q", up.SnapshotDir)
	}
}

func TestDragonflySnapshotUpgrade_SaveThenUpdateActive(t *testing.T) {
	ctx := context.Background()
	fg := fgWithDragonflySnapshotUpgrade()
	fg.Annotations = nil
	now := metav1.Now()
	fg.Status.Dragonfly.Upgrade = &v1alpha1.DragonflyUpgradeStatus{
		Phase:       v1alpha1.DragonflyUpgradePhasePending,
		SourceImage: fg.Spec.Dragonfly.Image,
		TargetImage: "docker.dragonflydb.io/dragonflydb/dragonfly:v1.39.0",
		ActiveSite:  "dc1",
		SnapshotDir: fg.Spec.Dragonfly.Snapshot.Dir,
		StartTime:   &now,
	}
	r, c := newReconciler(fg)
	if err := r.reconcileDragonflyResources(ctx, fg); err != nil {
		t.Fatalf("reconcile resources: %v", err)
	}
	oldMasterPod := dragonflyPodForTest(fg, "dc1", fg.Spec.Dragonfly.Image, true)
	if err := c.Create(ctx, oldMasterPod); err != nil {
		t.Fatalf("create pod: %v", err)
	}
	conn := &fakeDragonflyConn{info: dragonfly.ReplicationInfo{Role: "master"}}
	r.dragonflyConnector = func(context.Context, string, string) (DragonflyConnection, error) { return conn, nil }

	d, err := r.reconcileDragonflySnapshotUpgrade(ctx, fg)
	if err != nil || d == 0 {
		t.Fatalf("pending reconcile d=%s err=%v", d, err)
	}
	if err := c.Get(ctx, types.NamespacedName{Name: fg.Name, Namespace: fg.Namespace}, fg); err != nil {
		t.Fatal(err)
	}
	if fg.Status.Dragonfly.Upgrade.Phase != v1alpha1.DragonflyUpgradePhaseSavingSnapshot {
		t.Fatalf("phase=%q want SavingSnapshot", fg.Status.Dragonfly.Upgrade.Phase)
	}

	d, err = r.reconcileDragonflySnapshotUpgrade(ctx, fg)
	if err != nil || d == 0 {
		t.Fatalf("save reconcile d=%s err=%v", d, err)
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

	d, err = r.reconcileDragonflySnapshotUpgrade(ctx, fg)
	if err != nil || d == 0 {
		t.Fatalf("update-active reconcile d=%s err=%v", d, err)
	}
	var deleted corev1.Pod
	if err := c.Get(ctx, types.NamespacedName{Name: oldMasterPod.Name, Namespace: oldMasterPod.Namespace}, &deleted); err == nil && deleted.DeletionTimestamp == nil {
		t.Fatal("active pod was not deleted for restart")
	}
}

func dragonflyPodForTest(fg *v1alpha1.MysqlFailoverGroup, site, image string, ready bool) *corev1.Pod {
	labels := dragonflyCommonLabels(fg.Name, site)
	labels[labelDragonflyRole] = "replica"
	labels[labelDragonflyTraffic] = dragonflyTrafficEnabled
	if site == fg.Status.ActiveSite {
		labels[labelDragonflyRole] = "master"
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
	_ = time.Second // keep time imported when assertions evolve.
}
