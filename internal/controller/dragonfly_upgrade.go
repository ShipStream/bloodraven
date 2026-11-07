package controller

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
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
	dragonflyUpgradePollInterval = 2 * time.Second
	dragonflySaveTimeout         = 5 * time.Minute
	// dragonflyUpgradeDeferRetry is how often a deferred snapshot-upgrade
	// request is re-checked.
	dragonflyUpgradeDeferRetry = 10 * time.Second
)

func dragonflyUpgradeInFlight(s *v1alpha1.DragonflyUpgradeStatus) bool {
	if s == nil {
		return false
	}
	return s.Phase != v1alpha1.DragonflyUpgradePhaseSucceeded && s.Phase != v1alpha1.DragonflyUpgradePhaseFailed
}

// dragonflyUpgradeStep is what one snapshot-upgrade pass asks of the rest
// of the reconcile. hold > 0 while a run is in flight (or the gate is busy
// on the way to one): the reconcile requeues and stops there, as the run
// owns the Dragonfly sites. wait > 0 while a needed run waits for
// admission (no MySQL active site, a planned failover owns the sites, the
// Dragonfly master is elsewhere): the rest of the reconcile still runs,
// since the planned failover it waits for is driven later in the same pass,
// and the render keeps every site's live image meanwhile.
type dragonflyUpgradeStep struct {
	hold time.Duration
	wait time.Duration
}

// reconcileDragonflySnapshotUpgrade drives the snapshot-restore upgrade
// (spec.dragonfly.upgradeStrategy SnapshotRestore). The trigger is level,
// not a request: a run is needed while some site's live StatefulSet runs an
// image other than spec.dragonfly.image. The operator never writes spec.
//
//   - A site whose StatefulSet does not exist yet has no live image, so a
//     new group, or one whose status was lost, never starts a run nobody
//     asked for.
//   - A Failed run (failed or abandoned) is not restarted while
//     spec.dragonfly still matches the digest it recorded; its Warning was
//     emitted once, when it ended. Any change to spec.dragonfly re-arms
//     it. Once no site runs another image the Failed record is removed, so
//     setting the image back to the running one and then forward again
//     retries a run whose cause was outside spec.
//   - A run that cannot be admitted waits (dragonflyUpgradeStep.wait); it is
//     not dropped.
func (r *MysqlFailoverGroupReconciler) reconcileDragonflySnapshotUpgrade(ctx context.Context, fg *v1alpha1.MysqlFailoverGroup) (dragonflyUpgradeStep, error) {
	nn := types.NamespacedName{Namespace: fg.Namespace, Name: fg.Name}
	if !dragonflyUpgradeInFlight(dragonflyUpgradeStatus(fg)) {
		// The cheap decision on the cached read: only a run to start or a
		// record to remove needs the gate.
		d, err := r.dragonflyUpgradeDecide(ctx, r.Client, fg)
		if err != nil {
			return dragonflyUpgradeStep{}, err
		}
		switch d.action {
		case dragonflyUpgradeNone:
			r.clearDragonflyUpgradeDeferral(nn)
			return dragonflyUpgradeStep{}, nil
		case dragonflyUpgradeWait:
			r.noteDragonflyUpgradeDeferral(ctx, fg, d.why)
			return dragonflyUpgradeStep{wait: dragonflyUpgradeDeferRetry}, nil
		}
	}
	// Every step below changes Dragonfly routing (traffic and role labels,
	// REPLICAOF, pod deletion, StatefulSet images) or
	// status.dragonfly.upgrade, so it runs under the group's Dragonfly gate
	// and on a read made after acquiring it. A promotion already on the
	// wire finishes and records first; the upgrade then revalidates
	// against it.
	release, ok := r.acquireDragonflyGate(ctx, nn)
	if !ok {
		return dragonflyUpgradeStep{hold: dragonflyGateRetry}, nil
	}
	defer release()
	fresh, err := r.freshFailoverGroup(ctx, nn)
	if err != nil {
		if apierrors.IsNotFound(err) {
			return dragonflyUpgradeStep{}, nil
		}
		return dragonflyUpgradeStep{}, err
	}
	fg = fresh

	cur := dragonflyUpgradeStatus(fg)
	// The upgrade pauses only the Dragonfly manager, through its own pause
	// source; it never touches the planned-failover claim or MySQL topology
	// decisions. The pause follows the upgrade state read under the gate,
	// never the informer cache: a cached in-flight upgrade that has since
	// ended (abandoned, failed, succeeded) must not pause the manager again,
	// and every return below, the terminal ones included, leaves the pause
	// matching the uncached state.
	r.setDragonflyUpgradePause(nn, dragonflyUpgradeInFlight(cur))
	if !dragonflyUpgradeInFlight(cur) {
		reader := client.Reader(r.Client)
		if r.APIReader != nil {
			reader = r.APIReader
		}
		d, err := r.dragonflyUpgradeDecide(ctx, reader, fg)
		if err != nil {
			return dragonflyUpgradeStep{}, err
		}
		switch d.action {
		case dragonflyUpgradeStart:
			r.clearDragonflyUpgradeDeferral(nn)
			return r.startDragonflySnapshotUpgrade(ctx, fg, nn, d)
		case dragonflyUpgradeClearRecord:
			r.clearDragonflyUpgradeDeferral(nn)
			return dragonflyUpgradeStep{}, r.setDragonflyUpgradeStatus(ctx, fg, nil)
		case dragonflyUpgradeWait:
			r.noteDragonflyUpgradeDeferral(ctx, fg, d.why)
			return dragonflyUpgradeStep{wait: dragonflyUpgradeDeferRetry}, nil
		default:
			r.clearDragonflyUpgradeDeferral(nn)
			return dragonflyUpgradeStep{}, nil
		}
	}
	why, mysqlActive := r.dragonflyUpgradeSupersededByMySQL(fg, cur)
	if why == "" {
		why = dragonflyUpgradeSuperseded(fg, cur)
	}
	if why != "" {
		return r.abandonDragonflyUpgrade(ctx, fg, cur, why, mysqlActive)
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
		return r.failDragonflyUpgrade(ctx, fg, cur, v1alpha1.DragonflyUpgradeReasonUnknownPhase, fmt.Sprintf("unknown phase %q", cur.Phase))
	}
}

// dragonflyUpgradeAction is what dragonflyUpgradeDecide asks for when no
// run is in flight.
type dragonflyUpgradeAction int

const (
	dragonflyUpgradeNone dragonflyUpgradeAction = iota
	dragonflyUpgradeStart
	dragonflyUpgradeWait
	dragonflyUpgradeClearRecord
)

type dragonflyUpgradeDecision struct {
	action dragonflyUpgradeAction
	// why is the wait's cause (dragonflyUpgradeWait).
	why string
	// source, target, digest and activeMoved describe the run to start
	// (dragonflyUpgradeStart): activeMoved means the active site already
	// runs the target, so the run only moves the other sites.
	source, target, digest string
	activeMoved            bool
}

// dragonflyUpgradeStrategy returns spec.dragonfly.upgradeStrategy; empty
// (a Go-built object the API server never defaulted) is Rolling.
func dragonflyUpgradeStrategy(spec *v1alpha1.DragonflySpec) v1alpha1.DragonflyUpgradeStrategy {
	if spec == nil || spec.UpgradeStrategy == "" {
		return v1alpha1.DragonflyUpgradeStrategyRolling
	}
	return spec.UpgradeStrategy
}

// dragonflySpecDigest is "sha256:<hex>" over the JSON of spec.dragonfly.
func dragonflySpecDigest(spec *v1alpha1.DragonflySpec) string {
	raw, err := json.Marshal(spec)
	if err != nil {
		// A struct of strings, numbers and API types always marshals.
		panic(fmt.Sprintf("marshal spec.dragonfly: %v", err))
	}
	sum := sha256.Sum256(raw)
	return "sha256:" + hex.EncodeToString(sum[:])
}

// dragonflyLiveImages returns each spec site's live Dragonfly image, read
// from its StatefulSet's pod template. A site whose StatefulSet does not
// exist, or has no Dragonfly container, is absent: its image is unknown.
func dragonflyLiveImages(ctx context.Context, reader client.Reader, fg *v1alpha1.MysqlFailoverGroup) (map[string]string, error) {
	out := make(map[string]string, len(fg.Spec.Sites))
	for _, site := range fg.Spec.Sites {
		var sts appsv1.StatefulSet
		key := types.NamespacedName{Namespace: fg.Namespace, Name: dragonflyStatefulSetName(fg.Name, site.Name)}
		if err := reader.Get(ctx, key, &sts); err != nil {
			if apierrors.IsNotFound(err) {
				continue
			}
			return nil, fmt.Errorf("get dragonfly statefulset %s: %w", site.Name, err)
		}
		if img := dragonflyTemplateImage(&sts); img != "" {
			out[site.Name] = img
		}
	}
	return out, nil
}

// dragonflyTemplateImage is the Dragonfly container image of a
// StatefulSet's pod template, or "".
func dragonflyTemplateImage(sts *appsv1.StatefulSet) string {
	if sts == nil {
		return ""
	}
	if c, ok := dragonflyContainerFromTemplate(sts.Spec.Template.Spec); ok {
		return c.Image
	}
	return ""
}

// dragonflyUpgradeDecide decides, with no run in flight, whether a
// snapshot-restore run is needed and can start (see
// reconcileDragonflySnapshotUpgrade for the rules). reader reads the live
// StatefulSets: the cache for the cheap first look, the API server under
// the gate, where a just-moved image must not look like drift.
func (r *MysqlFailoverGroupReconciler) dragonflyUpgradeDecide(ctx context.Context, reader client.Reader, fg *v1alpha1.MysqlFailoverGroup) (dragonflyUpgradeDecision, error) {
	if !dragonflyEnabled(fg) {
		return dragonflyUpgradeDecision{}, nil
	}
	spec := fg.Spec.Dragonfly
	cur := dragonflyUpgradeStatus(fg)
	snapshotRestore := dragonflyUpgradeStrategy(spec) == v1alpha1.DragonflyUpgradeStrategySnapshotRestore
	if !snapshotRestore && (cur == nil || cur.Phase != v1alpha1.DragonflyUpgradePhaseFailed) {
		// Rolling: the render moves the image. Only a Failed record left
		// from a SnapshotRestore run needs a look (its removal below).
		return dragonflyUpgradeDecision{}, nil
	}
	live, err := dragonflyLiveImages(ctx, reader, fg)
	if err != nil {
		return dragonflyUpgradeDecision{}, err
	}
	target := spec.Image
	drifted := ""
	for _, site := range fg.Spec.Sites {
		if img, ok := live[site.Name]; ok && img != target {
			drifted = site.Name
			break
		}
	}
	if drifted == "" {
		if cur != nil && cur.Phase == v1alpha1.DragonflyUpgradePhaseFailed {
			return dragonflyUpgradeDecision{action: dragonflyUpgradeClearRecord}, nil
		}
		return dragonflyUpgradeDecision{}, nil
	}
	if !snapshotRestore || spec.Snapshot == nil || spec.Snapshot.Dir == "" {
		// Admission requires snapshot.dir under SnapshotRestore.
		return dragonflyUpgradeDecision{}, nil
	}
	digest := dragonflySpecDigest(spec)
	if cur != nil && cur.Phase == v1alpha1.DragonflyUpgradePhaseFailed && cur.TargetImage == target && cur.SpecDigest == digest {
		return dragonflyUpgradeDecision{}, nil
	}
	active := fg.Status.ActiveSite
	if active == "" {
		return dragonflyUpgradeDecision{action: dragonflyUpgradeWait,
			why: "status.activeSite is empty; the run starts once MySQL has an active site"}, nil
	}
	activeImage, ok := live[active]
	if !ok {
		// The active site's live image is unknown (its StatefulSet is not
		// there yet): nothing to snapshot from.
		return dragonflyUpgradeDecision{}, nil
	}
	if r.plannedOwnsDragonfly(fg) {
		return dragonflyUpgradeDecision{action: dragonflyUpgradeWait,
			why: "a planned failover run or automation hold owns the sites; the run starts once it releases them"}, nil
	}
	if master := effectiveDragonflyMasterSite(fg); master != "" && master != active {
		// The upgrade saves and restarts status.activeSite as the Dragonfly
		// master and re-attaches every other site to it. While Dragonfly
		// serves from another site (a planned takeover that failed after
		// MySQL moved, before drift repair re-aligns Dragonfly, or a
		// Dragonfly-only promotion), that would restart a replica and
		// re-point the real master at it.
		return dragonflyUpgradeDecision{action: dragonflyUpgradeWait,
			why: fmt.Sprintf("Dragonfly master %q differs from the MySQL active site %q; the run starts once they match", master, active)}, nil
	}
	d := dragonflyUpgradeDecision{action: dragonflyUpgradeStart, source: activeImage, target: target, digest: digest}
	if activeImage == target {
		// A run that ended after the active site moved (or whose record
		// was lost): only the other sites still move.
		d.activeMoved = true
		d.source = live[drifted]
	}
	return d, nil
}

// noteDragonflyUpgradeDeferral logs a waiting run and emits
// DragonflyUpgradeDeferred when the cause changes, not on every retry.
func (r *MysqlFailoverGroupReconciler) noteDragonflyUpgradeDeferral(ctx context.Context, fg *v1alpha1.MysqlFailoverGroup, why string) {
	nn := types.NamespacedName{Namespace: fg.Namespace, Name: fg.Name}
	r.dragonflyUpgradeDeferMu.Lock()
	prev, seen := r.dragonflyUpgradeDeferNote[nn]
	if r.dragonflyUpgradeDeferNote == nil {
		r.dragonflyUpgradeDeferNote = make(map[types.NamespacedName]string)
	}
	r.dragonflyUpgradeDeferNote[nn] = why
	r.dragonflyUpgradeDeferMu.Unlock()
	if seen && prev == why {
		return
	}
	log.FromContext(ctx).Info("dragonfly snapshot-upgrade deferred", "fg", nn.String(), "reason", why)
	r.Recorder.Eventf(fg, corev1.EventTypeNormal, v1alpha1.EventDragonflyUpgradeDeferred,
		"dragonfly snapshot-restore upgrade to %q deferred: %s", fg.Spec.Dragonfly.Image, why)
}

func (r *MysqlFailoverGroupReconciler) clearDragonflyUpgradeDeferral(nn types.NamespacedName) {
	r.dragonflyUpgradeDeferMu.Lock()
	delete(r.dragonflyUpgradeDeferNote, nn)
	r.dragonflyUpgradeDeferMu.Unlock()
}

// setDragonflyUpgradePause sets the Dragonfly manager's upgrade pause
// source, when a runner is wired.
func (r *MysqlFailoverGroupReconciler) setDragonflyUpgradePause(nn types.NamespacedName, paused bool) {
	if r.Runner != nil {
		r.Runner.SetDragonflyUpgradePaused(nn, paused)
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

// startDragonflySnapshotUpgrade records a new run decided under the gate.
// A run whose active site already runs the target starts at
// ReattachingReplicas: the snapshot and the active restart are behind it.
func (r *MysqlFailoverGroupReconciler) startDragonflySnapshotUpgrade(ctx context.Context, fg *v1alpha1.MysqlFailoverGroup, nn types.NamespacedName, d dragonflyUpgradeDecision) (dragonflyUpgradeStep, error) {
	now := metav1.Now()
	st := &v1alpha1.DragonflyUpgradeStatus{
		Phase:       v1alpha1.DragonflyUpgradePhasePending,
		SourceImage: d.source,
		TargetImage: d.target,
		SpecDigest:  d.digest,
		ActiveSite:  fg.Status.ActiveSite,
		SnapshotDir: fg.Spec.Dragonfly.Snapshot.Dir,
		StartTime:   &now,
		Message:     fmt.Sprintf("started snapshot-restore upgrade to %q", d.target),
	}
	if d.activeMoved {
		st.Phase = v1alpha1.DragonflyUpgradePhaseReattachingReplicas
		st.Message = fmt.Sprintf("active site already runs %q; updating replicas", d.target)
	}
	if err := r.setDragonflyUpgradeStatus(ctx, fg, st); err != nil {
		return dragonflyUpgradeStep{}, err
	}
	r.setDragonflyUpgradePause(nn, true)
	r.Recorder.Eventf(fg, corev1.EventTypeNormal, v1alpha1.EventDragonflyUpgradeStarted,
		"dragonfly snapshot-restore upgrade started: %s -> %s", st.SourceImage, st.TargetImage)
	return dragonflyUpgradeStep{hold: time.Second}, nil
}

func (r *MysqlFailoverGroupReconciler) dragonflyUpgradeToSaving(ctx context.Context, fg *v1alpha1.MysqlFailoverGroup, cur *v1alpha1.DragonflyUpgradeStatus) (dragonflyUpgradeStep, error) {
	if err := r.setDragonflyTrafficOnSite(ctx, fg, cur.ActiveSite, false); err != nil {
		return dragonflyUpgradeStep{}, err
	}
	next := copyDragonflyUpgradeStatus(cur)
	next.Phase = v1alpha1.DragonflyUpgradePhaseSavingSnapshot
	next.Message = "active Dragonfly traffic shed; saving snapshot"
	if err := r.setDragonflyUpgradeStatus(ctx, fg, next); err != nil {
		return dragonflyUpgradeStep{}, err
	}
	r.Recorder.Eventf(fg, corev1.EventTypeNormal, v1alpha1.EventDragonflyUpgradeSnapshotStarted,
		"saving Dragonfly snapshot on active site %s", cur.ActiveSite)
	return dragonflyUpgradeStep{hold: 1 * time.Second}, nil
}

func (r *MysqlFailoverGroupReconciler) dragonflyUpgradeSave(ctx context.Context, fg *v1alpha1.MysqlFailoverGroup, cur *v1alpha1.DragonflyUpgradeStatus) (dragonflyUpgradeStep, error) {
	conn, err := r.dragonflyDial(ctx, fg, cur.ActiveSite)
	if err != nil {
		return r.failDragonflyUpgrade(ctx, fg, cur, v1alpha1.DragonflyUpgradeReasonSnapshotDialFailed, err.Error())
	}
	saveCtx, cancel := context.WithTimeout(ctx, dragonflySaveTimeout)
	err = conn.Save(saveCtx)
	cancel()
	_ = conn.Close()
	if err != nil {
		return r.failDragonflyUpgrade(ctx, fg, cur, v1alpha1.DragonflyUpgradeReasonSnapshotSaveFailed, err.Error())
	}
	now := metav1.Now()
	next := copyDragonflyUpgradeStatus(cur)
	next.Phase = v1alpha1.DragonflyUpgradePhaseUpdatingActive
	next.SnapshotTime = &now
	next.Message = "snapshot saved; updating active Dragonfly pod"
	if err := r.setDragonflyUpgradeStatus(ctx, fg, next); err != nil {
		return dragonflyUpgradeStep{}, err
	}
	r.Recorder.Eventf(fg, corev1.EventTypeNormal, v1alpha1.EventDragonflyUpgradeSnapshotCompleted,
		"Dragonfly snapshot saved for upgrade using %s", cur.SnapshotDir)
	return dragonflyUpgradeStep{hold: 1 * time.Second}, nil
}

func (r *MysqlFailoverGroupReconciler) dragonflyUpgradeUpdateActive(ctx context.Context, fg *v1alpha1.MysqlFailoverGroup, cur *v1alpha1.DragonflyUpgradeStatus) (dragonflyUpgradeStep, error) {
	if err := r.updateDragonflyStatefulSetImage(ctx, fg, cur.ActiveSite, cur.TargetImage); err != nil {
		return dragonflyUpgradeStep{}, err
	}
	if err := r.deleteDragonflyPodsForSite(ctx, fg, cur.ActiveSite); err != nil {
		return dragonflyUpgradeStep{}, err
	}
	next := copyDragonflyUpgradeStatus(cur)
	next.Phase = v1alpha1.DragonflyUpgradePhaseWaitingForActiveRestore
	next.Message = "active Dragonfly pod restarted; waiting for snapshot restore"
	if err := r.setDragonflyUpgradeStatus(ctx, fg, next); err != nil {
		return dragonflyUpgradeStep{}, err
	}
	return dragonflyUpgradeStep{hold: dragonflyUpgradePollInterval}, nil
}

func (r *MysqlFailoverGroupReconciler) dragonflyUpgradeWaitActive(ctx context.Context, fg *v1alpha1.MysqlFailoverGroup, cur *v1alpha1.DragonflyUpgradeStatus) (dragonflyUpgradeStep, error) {
	ready, msg, err := r.dragonflySiteReadyOnImage(ctx, fg, cur.ActiveSite, cur.TargetImage, true)
	if err != nil {
		return dragonflyUpgradeStep{}, err
	}
	if !ready {
		return dragonflyUpgradeStep{hold: dragonflyUpgradePollInterval}, r.updateDragonflyUpgradeMessage(ctx, fg, cur, msg)
	}
	if err := r.setDragonflyTrafficOnSite(ctx, fg, cur.ActiveSite, false); err != nil {
		return dragonflyUpgradeStep{}, err
	}
	if err := r.setDragonflyRoleOnSite(ctx, fg, cur.ActiveSite, "master"); err != nil {
		return dragonflyUpgradeStep{}, err
	}
	next := copyDragonflyUpgradeStatus(cur)
	next.Phase = v1alpha1.DragonflyUpgradePhaseReattachingReplicas
	next.Message = "active Dragonfly restored; updating replicas"
	if err := r.setDragonflyUpgradeStatus(ctx, fg, next); err != nil {
		return dragonflyUpgradeStep{}, err
	}
	return dragonflyUpgradeStep{hold: 1 * time.Second}, nil
}

func (r *MysqlFailoverGroupReconciler) dragonflyUpgradeReattachReplicas(ctx context.Context, fg *v1alpha1.MysqlFailoverGroup, cur *v1alpha1.DragonflyUpgradeStatus) (dragonflyUpgradeStep, error) {
	allReady := true
	for _, site := range fg.Spec.Sites {
		if site.Name == cur.ActiveSite {
			continue
		}
		if err := r.updateDragonflyStatefulSetImage(ctx, fg, site.Name, cur.TargetImage); err != nil {
			return dragonflyUpgradeStep{}, err
		}
		ready, msg, err := r.dragonflyReplicaReadyOrAttach(ctx, fg, site.Name, cur.ActiveSite, cur.TargetImage)
		if err != nil {
			return dragonflyUpgradeStep{}, err
		}
		if !ready {
			allReady = false
			if sameImage, imgErr := r.dragonflySitePodImageMatches(ctx, fg, site.Name, cur.TargetImage); imgErr != nil {
				return dragonflyUpgradeStep{}, imgErr
			} else if !sameImage {
				_ = r.deleteDragonflyPodsForSite(ctx, fg, site.Name)
			}
			_ = r.updateDragonflyUpgradeMessage(ctx, fg, cur, msg)
		}
	}
	if !allReady {
		return dragonflyUpgradeStep{hold: dragonflyUpgradePollInterval}, nil
	}
	if err := r.setDragonflyTrafficOnSite(ctx, fg, cur.ActiveSite, true); err != nil {
		return dragonflyUpgradeStep{}, err
	}
	if err := r.setDragonflyRoleOnSite(ctx, fg, cur.ActiveSite, "master"); err != nil {
		return dragonflyUpgradeStep{}, err
	}
	now := metav1.Now()
	next := copyDragonflyUpgradeStatus(cur)
	next.Phase = v1alpha1.DragonflyUpgradePhaseSucceeded
	next.CompletionTime = &now
	next.Reason = v1alpha1.DragonflyUpgradeReasonCompleted
	next.Message = "snapshot-restore upgrade completed"
	if err := r.setDragonflyUpgradeStatus(ctx, fg, next); err != nil {
		return dragonflyUpgradeStep{}, err
	}
	if r.Runner != nil {
		r.Runner.SetDragonflyUpgradePaused(types.NamespacedName{Namespace: fg.Namespace, Name: fg.Name}, false)
	}
	r.Recorder.Eventf(fg, corev1.EventTypeNormal, v1alpha1.EventDragonflyUpgradeSucceeded,
		"Dragonfly snapshot-restore upgrade completed on image %s", cur.TargetImage)
	return dragonflyUpgradeStep{}, nil
}

// dragonflyUpgradeSuperseded returns why an in-flight upgrade no longer
// matches the group's Dragonfly authority, or "". The upgrade restarts and
// restores its retained active site and re-attaches every other site to
// it; once a promotion stamped at or after the upgrade started has moved
// the Dragonfly master elsewhere (an emergency promotion that was already
// running when the upgrade started), continuing would demote the new
// master.
func dragonflyUpgradeSuperseded(fg *v1alpha1.MysqlFailoverGroup, cur *v1alpha1.DragonflyUpgradeStatus) string {
	df := fg.Status.Dragonfly
	if df == nil || df.ActiveSite == "" || df.ActiveSite == cur.ActiveSite || df.LastPromotionTime == nil {
		return ""
	}
	if cur.StartTime != nil && df.LastPromotionTime.Time.Before(cur.StartTime.Time) {
		// Dragonfly already disagreed with MySQL when the upgrade started;
		// no promotion has moved it since.
		return ""
	}
	return fmt.Sprintf("active Dragonfly site moved to %q while the upgrade retained %q", df.ActiveSite, cur.ActiveSite)
}

// dragonflyUpgradeSupersededByMySQL re-derives, on every upgrade
// reconcile, whether MySQL failed over during the upgrade (§8.3). The
// effective MySQL authority is the registered manager's active site, which
// reflects a promotion at once; with no manager registered it is the newer
// of the status and out-of-band failover records. The upgrade is superseded
// when that site is set and differs from the upgrade's retained active
// site, and either the manager names it or the record's lastFailover is not
// before the upgrade's start. It returns why, and the MySQL active site.
func (r *MysqlFailoverGroupReconciler) dragonflyUpgradeSupersededByMySQL(fg *v1alpha1.MysqlFailoverGroup, cur *v1alpha1.DragonflyUpgradeStatus) (why, active string) {
	nn := types.NamespacedName{Namespace: fg.Namespace, Name: fg.Name}
	if r.Runner != nil {
		// A registered manager that names a site decides. One that names
		// none yet (it has not polled since a restart) falls through to the
		// durable record, which carries an emergency failover recorded
		// before the restart (§8.3).
		if site, ok := r.Runner.ActiveSite(nn); ok && site != "" {
			if site != cur.ActiveSite {
				return fmt.Sprintf("MySQL failed over to %q during the upgrade", site), site
			}
			return "", ""
		}
	}
	rec, _, _ := EffectiveFailoverRecord(fg, time.Now())
	if rec.LastFailoverTarget == "" || rec.LastFailoverTarget == cur.ActiveSite || rec.LastFailover.IsZero() {
		return "", ""
	}
	if cur.StartTime != nil && rec.LastFailover.Before(cur.StartTime.Time.UTC().Truncate(time.Second)) {
		return "", ""
	}
	return fmt.Sprintf("MySQL failed over to %q during the upgrade", rec.LastFailoverTarget), rec.LastFailoverTarget
}

// abandonDragonflyUpgrade fails a superseded upgrade without touching the
// routing labels: they belong to the promotion that superseded it. It runs
// under the group's Dragonfly gate: it stamps Failed{ActiveSiteChanged},
// clears the upgrade pause and, when MySQL moved (mysqlActive), hands the
// follow-up promotion to the Dragonfly manager, which revalidates it under
// the gate against the uncached state once this reconcile releases it. A
// lost hand-off is harmless: the upgrade is already Failed, and the
// manager's drift tick promotes the MySQL active site.
func (r *MysqlFailoverGroupReconciler) abandonDragonflyUpgrade(ctx context.Context, fg *v1alpha1.MysqlFailoverGroup, cur *v1alpha1.DragonflyUpgradeStatus, why, mysqlActive string) (dragonflyUpgradeStep, error) {
	now := metav1.Now()
	next := copyDragonflyUpgradeStatus(cur)
	next.Phase = v1alpha1.DragonflyUpgradePhaseFailed
	next.CompletionTime = &now
	next.Reason = v1alpha1.DragonflyUpgradeReasonActiveSiteChanged
	next.Message = why + "; upgrade abandoned. It is not restarted for this spec.dragonfly: change it, or set spec.dragonfly.image to the running image and then forward again, to retry against the new active site"
	if err := r.setDragonflyUpgradeStatus(ctx, fg, next); err != nil {
		return dragonflyUpgradeStep{}, err
	}
	if r.Runner != nil {
		nn := types.NamespacedName{Namespace: fg.Namespace, Name: fg.Name}
		r.Runner.SetDragonflyUpgradePaused(nn, false)
		if mysqlActive != "" {
			r.Runner.EnqueueDragonflyFollowUp(nn, mysqlActive, cur.ActiveSite)
		}
	}
	r.Recorder.Eventf(fg, corev1.EventTypeWarning, v1alpha1.EventDragonflyUpgradeFailed, "ActiveSiteChanged: %s", next.Message)
	return dragonflyUpgradeStep{}, nil
}

func (r *MysqlFailoverGroupReconciler) failDragonflyUpgrade(ctx context.Context, fg *v1alpha1.MysqlFailoverGroup, cur *v1alpha1.DragonflyUpgradeStatus, reason v1alpha1.DragonflyUpgradeReason, msg string) (dragonflyUpgradeStep, error) {
	_ = r.setDragonflyTrafficOnSite(ctx, fg, cur.ActiveSite, true)
	now := metav1.Now()
	next := copyDragonflyUpgradeStatus(cur)
	next.Phase = v1alpha1.DragonflyUpgradePhaseFailed
	next.CompletionTime = &now
	next.Reason = reason
	next.Message = msg
	if err := r.setDragonflyUpgradeStatus(ctx, fg, next); err != nil {
		return dragonflyUpgradeStep{}, err
	}
	if r.Runner != nil {
		r.Runner.SetDragonflyUpgradePaused(types.NamespacedName{Namespace: fg.Namespace, Name: fg.Name}, false)
	}
	r.Recorder.Eventf(fg, corev1.EventTypeWarning, v1alpha1.EventDragonflyUpgradeFailed, "%s: %s", reason, msg)
	return dragonflyUpgradeStep{}, nil
}

func (r *MysqlFailoverGroupReconciler) setDragonflyUpgradeStatus(ctx context.Context, fg *v1alpha1.MysqlFailoverGroup, s *v1alpha1.DragonflyUpgradeStatus) error {
	patch := client.MergeFrom(fg.DeepCopy())
	if fg.Status.Dragonfly == nil {
		fg.Status.Dragonfly = &v1alpha1.DragonflyStatus{}
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
