package controller

import (
	"context"
	"fmt"
	"time"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	k8sretry "k8s.io/client-go/util/retry"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/log"

	v1alpha1 "github.com/shipstream/bloodraven/api/v1alpha1"
	"github.com/shipstream/bloodraven/internal/dragonfly"
)

const (
	DragonflySnapshotUpgradeAnnotation = "bloodraven.shipstream.io/dragonfly-snapshot-upgrade"
	dragonflyUpgradePollInterval       = 2 * time.Second
	dragonflySaveTimeout               = 5 * time.Minute
)

func dragonflyUpgradeInFlight(s *v1alpha1.DragonflyUpgradeStatus) bool {
	if s == nil {
		return false
	}
	return s.Phase != v1alpha1.DragonflyUpgradePhaseSucceeded && s.Phase != v1alpha1.DragonflyUpgradePhaseFailed
}

func (r *MysqlFailoverGroupReconciler) reconcileDragonflySnapshotUpgrade(ctx context.Context, fg *v1alpha1.MysqlFailoverGroup) (time.Duration, error) {
	nn := types.NamespacedName{Namespace: fg.Namespace, Name: fg.Name}
	if r.Runner != nil {
		r.Runner.SetPlannedFailoverActive(nn, dragonflyUpgradeInFlight(dragonflyUpgradeStatus(fg)))
	}

	raw, hasAnnotation := fg.GetAnnotations()[DragonflySnapshotUpgradeAnnotation]
	cur := dragonflyUpgradeStatus(fg)
	if hasAnnotation && !dragonflyUpgradeInFlight(cur) {
		return r.acceptDragonflySnapshotUpgrade(ctx, fg, nn, raw)
	}
	if hasAnnotation && dragonflyUpgradeInFlight(cur) {
		if err := r.removeDragonflySnapshotUpgradeAnnotation(ctx, nn); err != nil {
			log.FromContext(ctx).Error(err, "remove duplicate dragonfly snapshot-upgrade annotation", "fg", nn)
		}
		r.Recorder.Eventf(fg, corev1.EventTypeWarning, ReasonDragonflyUpgradeRejected,
			"dragonfly snapshot-upgrade ignored: previous upgrade still running (phase=%q)", cur.Phase)
	}
	if !dragonflyUpgradeInFlight(cur) {
		return 0, nil
	}

	switch cur.Phase {
	case v1alpha1.DragonflyUpgradePhasePending:
		return r.dragonflyUpgradeToSaving(ctx, fg, cur)
	case v1alpha1.DragonflyUpgradePhaseSavingSnapshot:
		return r.dragonflyUpgradeSave(ctx, fg, cur)
	case v1alpha1.DragonflyUpgradePhaseUpdatingActive:
		return r.dragonflyUpgradeUpdateActive(ctx, fg, cur)
	case v1alpha1.DragonflyUpgradePhaseWaitingForActiveRestore:
		return r.dragonflyUpgradeWaitActive(ctx, fg, cur)
	case v1alpha1.DragonflyUpgradePhaseReattachingReplicas:
		return r.dragonflyUpgradeReattachReplicas(ctx, fg, cur)
	default:
		return r.failDragonflyUpgrade(ctx, fg, cur, "UnknownPhase", fmt.Sprintf("unknown phase %q", cur.Phase))
	}
}

func dragonflyUpgradeStatus(fg *v1alpha1.MysqlFailoverGroup) *v1alpha1.DragonflyUpgradeStatus {
	if fg == nil || fg.Status.Dragonfly == nil {
		return nil
	}
	return fg.Status.Dragonfly.Upgrade
}

func copyDragonflyUpgradeStatus(in *v1alpha1.DragonflyUpgradeStatus) *v1alpha1.DragonflyUpgradeStatus {
	if in == nil {
		return nil
	}
	out := *in
	if in.StartTime != nil {
		t := *in.StartTime
		out.StartTime = &t
	}
	if in.SnapshotTime != nil {
		t := *in.SnapshotTime
		out.SnapshotTime = &t
	}
	if in.CompletionTime != nil {
		t := *in.CompletionTime
		out.CompletionTime = &t
	}
	return &out
}

func (r *MysqlFailoverGroupReconciler) acceptDragonflySnapshotUpgrade(ctx context.Context, fg *v1alpha1.MysqlFailoverGroup, nn types.NamespacedName, targetImage string) (time.Duration, error) {
	if targetImage == "" {
		r.Recorder.Eventf(fg, corev1.EventTypeWarning, ReasonDragonflyUpgradeRejected, "dragonfly snapshot-upgrade target image is empty")
		_ = r.removeDragonflySnapshotUpgradeAnnotation(ctx, nn)
		return 0, nil
	}
	if !dragonflyEnabled(fg) || fg.Spec.Dragonfly.Snapshot == nil || fg.Spec.Dragonfly.Snapshot.Dir == "" {
		r.Recorder.Eventf(fg, corev1.EventTypeWarning, ReasonDragonflyUpgradeRejected,
			"dragonfly snapshot-upgrade requires spec.dragonfly.enabled=true and spec.dragonfly.snapshot.dir")
		_ = r.removeDragonflySnapshotUpgradeAnnotation(ctx, nn)
		return 0, nil
	}
	if fg.Status.ActiveSite == "" {
		r.Recorder.Eventf(fg, corev1.EventTypeWarning, ReasonDragonflyUpgradeRejected, "dragonfly snapshot-upgrade requires status.activeSite")
		_ = r.removeDragonflySnapshotUpgradeAnnotation(ctx, nn)
		return 0, nil
	}
	if fg.Status.PlannedFailover != nil && plannedFailoverInFlight(fg.Status.PlannedFailover) {
		r.Recorder.Eventf(fg, corev1.EventTypeWarning, ReasonDragonflyUpgradeRejected, "dragonfly snapshot-upgrade rejected: planned failover is running")
		_ = r.removeDragonflySnapshotUpgradeAnnotation(ctx, nn)
		return 0, nil
	}

	if err := r.removeDragonflySnapshotUpgradeAnnotation(ctx, nn); err != nil {
		return 0, err
	}

	now := metav1.Now()
	st := &v1alpha1.DragonflyUpgradeStatus{
		Phase:       v1alpha1.DragonflyUpgradePhasePending,
		SourceImage: fg.Spec.Dragonfly.Image,
		TargetImage: targetImage,
		ActiveSite:  fg.Status.ActiveSite,
		SnapshotDir: fg.Spec.Dragonfly.Snapshot.Dir,
		StartTime:   &now,
		Message:     fmt.Sprintf("accepted snapshot-restore upgrade to %q", targetImage),
	}
	if err := r.setDragonflyUpgradeStatus(ctx, fg, st); err != nil {
		return 0, err
	}
	r.Recorder.Eventf(fg, corev1.EventTypeNormal, ReasonDragonflyUpgradeStarted,
		"dragonfly snapshot-restore upgrade accepted: %s -> %s", st.SourceImage, st.TargetImage)
	return 1 * time.Second, nil
}

func (r *MysqlFailoverGroupReconciler) dragonflyUpgradeToSaving(ctx context.Context, fg *v1alpha1.MysqlFailoverGroup, cur *v1alpha1.DragonflyUpgradeStatus) (time.Duration, error) {
	if err := r.setDragonflyTrafficOnSite(ctx, fg, cur.ActiveSite, false); err != nil {
		return 0, err
	}
	next := copyDragonflyUpgradeStatus(cur)
	next.Phase = v1alpha1.DragonflyUpgradePhaseSavingSnapshot
	next.Message = "active Dragonfly traffic shed; saving snapshot"
	if err := r.setDragonflyUpgradeStatus(ctx, fg, next); err != nil {
		return 0, err
	}
	r.Recorder.Eventf(fg, corev1.EventTypeNormal, ReasonDragonflyUpgradeSnapshotStarted,
		"saving Dragonfly snapshot on active site %s", cur.ActiveSite)
	return 1 * time.Second, nil
}

func (r *MysqlFailoverGroupReconciler) dragonflyUpgradeSave(ctx context.Context, fg *v1alpha1.MysqlFailoverGroup, cur *v1alpha1.DragonflyUpgradeStatus) (time.Duration, error) {
	conn, err := r.dragonflyDial(ctx, fg, cur.ActiveSite)
	if err != nil {
		return r.failDragonflyUpgrade(ctx, fg, cur, "DragonflySnapshotDialFailed", err.Error())
	}
	saveCtx, cancel := context.WithTimeout(ctx, dragonflySaveTimeout)
	err = conn.Save(saveCtx)
	cancel()
	_ = conn.Close()
	if err != nil {
		return r.failDragonflyUpgrade(ctx, fg, cur, "DragonflySnapshotSaveFailed", err.Error())
	}
	now := metav1.Now()
	next := copyDragonflyUpgradeStatus(cur)
	next.Phase = v1alpha1.DragonflyUpgradePhaseUpdatingActive
	next.SnapshotTime = &now
	next.Message = "snapshot saved; updating active Dragonfly pod"
	if err := r.setDragonflyUpgradeStatus(ctx, fg, next); err != nil {
		return 0, err
	}
	r.Recorder.Eventf(fg, corev1.EventTypeNormal, ReasonDragonflyUpgradeSnapshotCompleted,
		"Dragonfly snapshot saved for upgrade using %s", cur.SnapshotDir)
	return 1 * time.Second, nil
}

func (r *MysqlFailoverGroupReconciler) dragonflyUpgradeUpdateActive(ctx context.Context, fg *v1alpha1.MysqlFailoverGroup, cur *v1alpha1.DragonflyUpgradeStatus) (time.Duration, error) {
	if err := r.updateDragonflyStatefulSetImage(ctx, fg, cur.ActiveSite, cur.TargetImage); err != nil {
		return 0, err
	}
	if err := r.deleteDragonflyPodsForSite(ctx, fg, cur.ActiveSite); err != nil {
		return 0, err
	}
	next := copyDragonflyUpgradeStatus(cur)
	next.Phase = v1alpha1.DragonflyUpgradePhaseWaitingForActiveRestore
	next.Message = "active Dragonfly pod restarted; waiting for snapshot restore"
	if err := r.setDragonflyUpgradeStatus(ctx, fg, next); err != nil {
		return 0, err
	}
	return dragonflyUpgradePollInterval, nil
}

func (r *MysqlFailoverGroupReconciler) dragonflyUpgradeWaitActive(ctx context.Context, fg *v1alpha1.MysqlFailoverGroup, cur *v1alpha1.DragonflyUpgradeStatus) (time.Duration, error) {
	ready, msg, err := r.dragonflySiteReadyOnImage(ctx, fg, cur.ActiveSite, cur.TargetImage, true)
	if err != nil {
		return 0, err
	}
	if !ready {
		return dragonflyUpgradePollInterval, r.updateDragonflyUpgradeMessage(ctx, fg, cur, msg)
	}
	if err := r.setDragonflyTrafficOnSite(ctx, fg, cur.ActiveSite, false); err != nil {
		return 0, err
	}
	if err := r.setDragonflyRoleOnSite(ctx, fg, cur.ActiveSite, "master"); err != nil {
		return 0, err
	}
	next := copyDragonflyUpgradeStatus(cur)
	next.Phase = v1alpha1.DragonflyUpgradePhaseReattachingReplicas
	next.Message = "active Dragonfly restored; updating replicas"
	if err := r.setDragonflyUpgradeStatus(ctx, fg, next); err != nil {
		return 0, err
	}
	return 1 * time.Second, nil
}

func (r *MysqlFailoverGroupReconciler) dragonflyUpgradeReattachReplicas(ctx context.Context, fg *v1alpha1.MysqlFailoverGroup, cur *v1alpha1.DragonflyUpgradeStatus) (time.Duration, error) {
	allReady := true
	for _, site := range fg.Spec.Sites {
		if site.Name == cur.ActiveSite {
			continue
		}
		if err := r.updateDragonflyStatefulSetImage(ctx, fg, site.Name, cur.TargetImage); err != nil {
			return 0, err
		}
		ready, msg, err := r.dragonflyReplicaReadyOrAttach(ctx, fg, site.Name, cur.ActiveSite, cur.TargetImage)
		if err != nil {
			return 0, err
		}
		if !ready {
			allReady = false
			if sameImage, imgErr := r.dragonflySitePodImageMatches(ctx, fg, site.Name, cur.TargetImage); imgErr != nil {
				return 0, imgErr
			} else if !sameImage {
				_ = r.deleteDragonflyPodsForSite(ctx, fg, site.Name)
			}
			_ = r.updateDragonflyUpgradeMessage(ctx, fg, cur, msg)
		}
	}
	if !allReady {
		return dragonflyUpgradePollInterval, nil
	}
	if err := r.setDragonflyTrafficOnSite(ctx, fg, cur.ActiveSite, true); err != nil {
		return 0, err
	}
	if err := r.setDragonflyRoleOnSite(ctx, fg, cur.ActiveSite, "master"); err != nil {
		return 0, err
	}
	if err := r.patchDragonflySpecImage(ctx, types.NamespacedName{Namespace: fg.Namespace, Name: fg.Name}, cur.TargetImage); err != nil {
		return 0, err
	}
	now := metav1.Now()
	next := copyDragonflyUpgradeStatus(cur)
	next.Phase = v1alpha1.DragonflyUpgradePhaseSucceeded
	next.CompletionTime = &now
	next.Reason = "Completed"
	next.Message = "snapshot-restore upgrade completed"
	if err := r.setDragonflyUpgradeStatus(ctx, fg, next); err != nil {
		return 0, err
	}
	if r.Runner != nil {
		r.Runner.SetPlannedFailoverActive(types.NamespacedName{Namespace: fg.Namespace, Name: fg.Name}, false)
	}
	r.Recorder.Eventf(fg, corev1.EventTypeNormal, ReasonDragonflyUpgradeCompleted,
		"Dragonfly snapshot-restore upgrade completed on image %s", cur.TargetImage)
	return 0, nil
}

func (r *MysqlFailoverGroupReconciler) failDragonflyUpgrade(ctx context.Context, fg *v1alpha1.MysqlFailoverGroup, cur *v1alpha1.DragonflyUpgradeStatus, reason, msg string) (time.Duration, error) {
	_ = r.setDragonflyTrafficOnSite(ctx, fg, cur.ActiveSite, true)
	now := metav1.Now()
	next := copyDragonflyUpgradeStatus(cur)
	next.Phase = v1alpha1.DragonflyUpgradePhaseFailed
	next.CompletionTime = &now
	next.Reason = reason
	next.Message = msg
	if err := r.setDragonflyUpgradeStatus(ctx, fg, next); err != nil {
		return 0, err
	}
	if r.Runner != nil {
		r.Runner.SetPlannedFailoverActive(types.NamespacedName{Namespace: fg.Namespace, Name: fg.Name}, false)
	}
	r.Recorder.Eventf(fg, corev1.EventTypeWarning, ReasonDragonflyUpgradeFailed, "%s: %s", reason, msg)
	return 0, nil
}

func (r *MysqlFailoverGroupReconciler) setDragonflyUpgradeStatus(ctx context.Context, fg *v1alpha1.MysqlFailoverGroup, s *v1alpha1.DragonflyUpgradeStatus) error {
	patch := client.MergeFrom(fg.DeepCopy())
	if fg.Status.Dragonfly == nil {
		fg.Status.Dragonfly = &v1alpha1.DragonflyStatus{Enabled: dragonflyEnabled(fg)}
	}
	fg.Status.Dragonfly.Upgrade = s
	if err := r.Status().Patch(ctx, fg, patch); err != nil && !apierrors.IsNotFound(err) {
		return err
	}
	return nil
}

func (r *MysqlFailoverGroupReconciler) updateDragonflyUpgradeMessage(ctx context.Context, fg *v1alpha1.MysqlFailoverGroup, cur *v1alpha1.DragonflyUpgradeStatus, msg string) error {
	next := copyDragonflyUpgradeStatus(cur)
	next.Message = msg
	return r.setDragonflyUpgradeStatus(ctx, fg, next)
}

func (r *MysqlFailoverGroupReconciler) removeDragonflySnapshotUpgradeAnnotation(ctx context.Context, nn types.NamespacedName) error {
	return k8sretry.RetryOnConflict(k8sretry.DefaultRetry, func() error {
		var fresh v1alpha1.MysqlFailoverGroup
		if err := r.Get(ctx, nn, &fresh); err != nil {
			return err
		}
		ann := fresh.GetAnnotations()
		if ann == nil {
			return nil
		}
		delete(ann, DragonflySnapshotUpgradeAnnotation)
		fresh.SetAnnotations(ann)
		return r.Update(ctx, &fresh)
	})
}

func (r *MysqlFailoverGroupReconciler) patchDragonflySpecImage(ctx context.Context, nn types.NamespacedName, image string) error {
	return k8sretry.RetryOnConflict(k8sretry.DefaultRetry, func() error {
		var fresh v1alpha1.MysqlFailoverGroup
		if err := r.Get(ctx, nn, &fresh); err != nil {
			return err
		}
		if fresh.Spec.Dragonfly == nil {
			return fmt.Errorf("dragonfly disabled")
		}
		fresh.Spec.Dragonfly.Image = image
		return r.Update(ctx, &fresh)
	})
}

func (r *MysqlFailoverGroupReconciler) updateDragonflyStatefulSetImage(ctx context.Context, fg *v1alpha1.MysqlFailoverGroup, siteName, image string) error {
	return k8sretry.RetryOnConflict(k8sretry.DefaultRetry, func() error {
		var sts appsv1.StatefulSet
		key := types.NamespacedName{Namespace: fg.Namespace, Name: dragonflyStatefulSetName(fg.Name, siteName)}
		if err := r.Get(ctx, key, &sts); err != nil {
			return err
		}
		if len(sts.Spec.Template.Spec.Containers) == 0 {
			return fmt.Errorf("dragonfly statefulset %s has no containers", key.Name)
		}
		sts.Spec.Template.Spec.Containers[0].Image = image
		return r.Update(ctx, &sts)
	})
}

func (r *MysqlFailoverGroupReconciler) deleteDragonflyPodsForSite(ctx context.Context, fg *v1alpha1.MysqlFailoverGroup, siteName string) error {
	return deleteDragonflyPodsForSite(ctx, r.Client, fg, siteName)
}

func deleteDragonflyPodsForSite(ctx context.Context, c client.Client, fg *v1alpha1.MysqlFailoverGroup, siteName string) error {
	pods, err := listDragonflyPodsForSite(ctx, c, fg, siteName)
	if err != nil {
		return err
	}
	for i := range pods {
		if err := c.Delete(ctx, &pods[i]); err != nil && !apierrors.IsNotFound(err) {
			return err
		}
	}
	return nil
}

func (r *MysqlFailoverGroupReconciler) dragonflySiteReadyOnImage(ctx context.Context, fg *v1alpha1.MysqlFailoverGroup, siteName, image string, wantMaster bool) (bool, string, error) {
	pods, err := listDragonflyPodsForSite(ctx, r.Client, fg, siteName)
	if err != nil {
		return false, "", err
	}
	if len(pods) != 1 {
		return false, fmt.Sprintf("site %s has %d pods", siteName, len(pods)), nil
	}
	pod := pods[0]
	if pod.DeletionTimestamp != nil {
		return false, fmt.Sprintf("site %s pod terminating", siteName), nil
	}
	if len(pod.Spec.Containers) == 0 || pod.Spec.Containers[0].Image != image {
		return false, fmt.Sprintf("site %s pod image not updated", siteName), nil
	}
	if !podReady(&pod) {
		return false, fmt.Sprintf("site %s pod not Ready", siteName), nil
	}
	conn, err := r.dragonflyDial(ctx, fg, siteName)
	if err != nil {
		return false, fmt.Sprintf("site %s not reachable: %v", siteName, err), nil
	}
	info, infoErr := conn.InfoReplication(ctx)
	persist, _ := conn.InfoPersistence(ctx)
	_ = conn.Close()
	if infoErr != nil {
		return false, fmt.Sprintf("site %s INFO failed: %v", siteName, infoErr), nil
	}
	if persist.Loading || persist.LoadState != "" {
		return false, fmt.Sprintf("site %s still loading snapshot", siteName), nil
	}
	if wantMaster {
		return info.Role == "master", fmt.Sprintf("site %s role=%s", siteName, info.Role), nil
	}
	ready := dragonfly.CandidateSyncReady(info, persist, 0)
	return ready, fmt.Sprintf("site %s role=%s link=%s sync=%v", siteName, info.Role, info.MasterLinkStatus, info.MasterSyncInProgress), nil
}

func (r *MysqlFailoverGroupReconciler) dragonflyReplicaReadyOrAttach(ctx context.Context, fg *v1alpha1.MysqlFailoverGroup, siteName, activeSite, image string) (bool, string, error) {
	ready, msg, err := r.dragonflySiteReadyOnImage(ctx, fg, siteName, image, false)
	if err != nil || ready {
		if ready {
			_ = r.setDragonflyRoleOnSite(ctx, fg, siteName, "replica")
		}
		return ready, msg, err
	}
	if sameImage, err := r.dragonflySitePodImageMatches(ctx, fg, siteName, image); err != nil {
		return false, "", err
	} else if !sameImage {
		return false, msg, nil
	}
	conn, err := r.dragonflyDial(ctx, fg, siteName)
	if err != nil {
		return false, msg, nil
	}
	host, port := splitHostPort(dragonflyAddr(fg, activeSite), dragonflyPort(fg.Spec.Dragonfly))
	err = conn.ReplicaOf(ctx, host, port)
	_ = conn.Close()
	if err != nil {
		return false, fmt.Sprintf("site %s REPLICAOF failed: %v", siteName, err), nil
	}
	_ = r.setDragonflyRoleOnSite(ctx, fg, siteName, "replica")
	return false, fmt.Sprintf("site %s attached as replica; waiting for sync", siteName), nil
}

func (r *MysqlFailoverGroupReconciler) dragonflySitePodImageMatches(ctx context.Context, fg *v1alpha1.MysqlFailoverGroup, siteName, image string) (bool, error) {
	pods, err := listDragonflyPodsForSite(ctx, r.Client, fg, siteName)
	if err != nil {
		return false, err
	}
	if len(pods) != 1 || len(pods[0].Spec.Containers) == 0 {
		return false, nil
	}
	return pods[0].Spec.Containers[0].Image == image, nil
}

func podReady(p *corev1.Pod) bool {
	for _, c := range p.Status.Conditions {
		if c.Type == corev1.PodReady && c.Status == corev1.ConditionTrue {
			return true
		}
	}
	return false
}
