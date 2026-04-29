package controller

import (
	"context"
	"testing"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"

	v1alpha1 "github.com/shipstream/bloodraven/api/v1alpha1"
)

// makeDragonflyPod returns a pod fixture matching the labels the
// label-sync helpers select on, for the given site/role/traffic.
func makeDragonflyPod(fgName, siteName, role string, trafficEnabled bool) *corev1.Pod {
	labels := map[string]string{
		labelAppName:       dragonflyAppName,
		labelInstance:      fgName,
		labelFailoverGroup: fgName,
		labelSite:          siteName,
		labelDragonflyRole: role,
	}
	if trafficEnabled {
		labels[labelDragonflyTraffic] = dragonflyTrafficEnabled
	}
	return &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      fgName + "-dragonfly-" + siteName + "-0",
			Namespace: "shared-lion",
			Labels:    labels,
		},
	}
}

func TestSyncDragonflyPodLabels_StampsBothLabels(t *testing.T) {
	fg := fgWithDragonfly()
	fg.Status.ActiveSite = "dc1"
	// dc1 pod has no labels yet; dc2 has stale role=master from a
	// previous topology.
	dc1Pod := makeDragonflyPod(fg.Name, "dc1", "", false)
	dc2Pod := makeDragonflyPod(fg.Name, "dc2", "master", true)
	r, c := newReconciler(fg, dc1Pod, dc2Pod)

	if err := r.syncDragonflyPodLabels(context.Background(), fg); err != nil {
		t.Fatalf("syncDragonflyPodLabels: %v", err)
	}
	var got1, got2 corev1.Pod
	if err := c.Get(context.Background(), types.NamespacedName{Name: dc1Pod.Name, Namespace: dc1Pod.Namespace}, &got1); err != nil {
		t.Fatalf("get dc1 pod: %v", err)
	}
	if err := c.Get(context.Background(), types.NamespacedName{Name: dc2Pod.Name, Namespace: dc2Pod.Namespace}, &got2); err != nil {
		t.Fatalf("get dc2 pod: %v", err)
	}
	if got1.Labels[labelDragonflyRole] != "master" {
		t.Errorf("dc1 role = %q, want master", got1.Labels[labelDragonflyRole])
	}
	if got1.Labels[labelDragonflyTraffic] != dragonflyTrafficEnabled {
		t.Errorf("dc1 traffic = %q, want %q", got1.Labels[labelDragonflyTraffic], dragonflyTrafficEnabled)
	}
	if got2.Labels[labelDragonflyRole] != "replica" {
		t.Errorf("dc2 role = %q, want replica", got2.Labels[labelDragonflyRole])
	}
	if got2.Labels[labelDragonflyTraffic] != dragonflyTrafficEnabled {
		t.Errorf("dc2 traffic = %q, want %q (replicas keep traffic enabled — selector is gated by role too)", got2.Labels[labelDragonflyTraffic], dragonflyTrafficEnabled)
	}
}

func TestSyncDragonflyPodLabels_SkipsRestoreDuringStripWindow(t *testing.T) {
	// Mid-flight planned-failover PromotingDragonfly with the source's
	// traffic label transiently stripped (PromotionMethod still empty).
	// syncDragonflyPodLabels must NOT re-stamp traffic on the source
	// pod or it would re-attach the soon-to-be-demoted master to the
	// active Service mid-REPLTAKEOVER.
	fg := fgWithDragonfly()
	fg.Status.ActiveSite = "dc1"
	fg.Status.PlannedFailover = &v1alpha1.PlannedFailoverStatus{
		Phase:         v1alpha1.PlannedFailoverPhasePromotingDragonfly,
		Target:        "dc2",
		SourcePrimary: "dc1",
		Dragonfly: &v1alpha1.PlannedFailoverDragonflyStatus{
			Enabled: true,
		},
	}
	// dc1 (source) has role=master but traffic stripped (no traffic label).
	dc1Pod := makeDragonflyPod(fg.Name, "dc1", "master", false)
	dc2Pod := makeDragonflyPod(fg.Name, "dc2", "replica", true)
	r, c := newReconciler(fg, dc1Pod, dc2Pod)

	if err := r.syncDragonflyPodLabels(context.Background(), fg); err != nil {
		t.Fatalf("syncDragonflyPodLabels: %v", err)
	}
	var got1 corev1.Pod
	if err := c.Get(context.Background(), types.NamespacedName{Name: dc1Pod.Name, Namespace: dc1Pod.Namespace}, &got1); err != nil {
		t.Fatalf("get dc1 pod: %v", err)
	}
	if _, ok := got1.Labels[labelDragonflyTraffic]; ok {
		t.Errorf("dc1 traffic label = %q, want absent (strip-active gate)", got1.Labels[labelDragonflyTraffic])
	}
}

func TestSyncDragonflyPodLabels_RestoresAfterPromotionMethodSet(t *testing.T) {
	// PromotingDragonfly phase BUT PromotionMethod is now set, meaning
	// the handler has finished the strip→takeover→restore sequence.
	// effectiveDragonflyMasterSite returns target. syncDragonflyPodLabels
	// should enforce target=master,traffic=enabled and source=replica,traffic=enabled.
	fg := fgWithDragonfly()
	fg.Status.ActiveSite = "dc1"
	fg.Status.PlannedFailover = &v1alpha1.PlannedFailoverStatus{
		Phase:         v1alpha1.PlannedFailoverPhasePromotingDragonfly,
		Target:        "dc2",
		SourcePrimary: "dc1",
		Dragonfly: &v1alpha1.PlannedFailoverDragonflyStatus{
			Enabled:         true,
			PromotionMethod: "REPLTAKEOVER",
		},
	}
	// Both pods have been rewritten by the phase handler to their new
	// roles; this sweep should be a no-op.
	dc1Pod := makeDragonflyPod(fg.Name, "dc1", "replica", true)
	dc2Pod := makeDragonflyPod(fg.Name, "dc2", "master", true)
	r, c := newReconciler(fg, dc1Pod, dc2Pod)

	if err := r.syncDragonflyPodLabels(context.Background(), fg); err != nil {
		t.Fatalf("syncDragonflyPodLabels: %v", err)
	}
	var got1, got2 corev1.Pod
	if err := c.Get(context.Background(), types.NamespacedName{Name: dc1Pod.Name, Namespace: dc1Pod.Namespace}, &got1); err != nil {
		t.Fatalf("get dc1: %v", err)
	}
	if err := c.Get(context.Background(), types.NamespacedName{Name: dc2Pod.Name, Namespace: dc2Pod.Namespace}, &got2); err != nil {
		t.Fatalf("get dc2: %v", err)
	}
	if got1.Labels[labelDragonflyRole] != "replica" {
		t.Errorf("dc1 role = %q, want replica", got1.Labels[labelDragonflyRole])
	}
	if got2.Labels[labelDragonflyRole] != "master" {
		t.Errorf("dc2 role = %q, want master", got2.Labels[labelDragonflyRole])
	}
	if got2.Labels[labelDragonflyTraffic] != dragonflyTrafficEnabled {
		t.Errorf("dc2 traffic = %q, want enabled", got2.Labels[labelDragonflyTraffic])
	}
}

func TestSetDragonflyTrafficOnSite_RemovesAndRestores(t *testing.T) {
	fg := fgWithDragonfly()
	pod := makeDragonflyPod(fg.Name, "dc1", "master", true)
	r, c := newReconciler(fg, pod)

	// Strip
	if err := r.setDragonflyTrafficOnSite(context.Background(), fg, "dc1", false); err != nil {
		t.Fatalf("strip: %v", err)
	}
	var got corev1.Pod
	if err := c.Get(context.Background(), types.NamespacedName{Name: pod.Name, Namespace: pod.Namespace}, &got); err != nil {
		t.Fatalf("get pod: %v", err)
	}
	if _, ok := got.Labels[labelDragonflyTraffic]; ok {
		t.Errorf("after strip, traffic label still present: %q", got.Labels[labelDragonflyTraffic])
	}

	// Restore
	if err := r.setDragonflyTrafficOnSite(context.Background(), fg, "dc1", true); err != nil {
		t.Fatalf("restore: %v", err)
	}
	if err := c.Get(context.Background(), types.NamespacedName{Name: pod.Name, Namespace: pod.Namespace}, &got); err != nil {
		t.Fatalf("get pod after restore: %v", err)
	}
	if got.Labels[labelDragonflyTraffic] != dragonflyTrafficEnabled {
		t.Errorf("after restore, traffic label = %q, want %q", got.Labels[labelDragonflyTraffic], dragonflyTrafficEnabled)
	}
}
