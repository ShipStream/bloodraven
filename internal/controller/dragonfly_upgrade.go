package controller

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"slices"
	"strings"
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
	// dragonflyUpgradeRunTimeout bounds a run from its start time: a run
	// still in flight after it ends Failed{TimedOut}, so an active site
	// that never restores, or a replica that never syncs, does not hold
	// the group's reconcile for ever.
	dragonflyUpgradeRunTimeout = time.Hour
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
//     while every site runs one image, setting the image back to it and
//     then forward again retries a run whose cause was outside spec.
//   - A run that cannot be admitted waits (dragonflyUpgradeStep.wait); it is
//     not dropped. That includes a run whose sites do not run the other
//     spec.dragonfly settings yet (dragonflyUpgradeConfigPending).
//   - A run in flight ends when spec.dragonfly changes or after
//     dragonflyUpgradeRunTimeout (checkDragonflyUpgradeRequest).
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
		d, err := r.dragonflyUpgradeDecide(ctx, r.uncachedReader(), fg)
		if err != nil {
			return dragonflyUpgradeStep{}, err
		}
		switch d.action {
		case dragonflyUpgradeStart:
			r.clearDragonflyUpgradeDeferral(nn)
			return r.startDragonflySnapshotUpgrade(ctx, fg, nn, d)
		case dragonflyUpgradeClearRecord:
			r.clearDragonflyUpgradeDeferral(nn)
			// The Failed record gates the label sweep while its active's
			// traffic is held for a snapshot load; it goes once the sweep
			// has given that traffic back.
			if held, err := r.dragonflyUpgradeTrafficHeld(ctx, fg, cur); err != nil || held {
				return dragonflyUpgradeStep{wait: dragonflyUpgradePollInterval}, err
			}
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
	if step, done, err := r.checkDragonflyUpgradeRequest(ctx, fg, cur); done || err != nil {
		return step, err
	}
	if step, done, err := r.checkDragonflyUpgradeWorkloads(ctx, fg, cur); done || err != nil {
		return step, err
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

// dragonflyLiveWorkloads is what dragonflyLiveImages read.
type dragonflyLiveWorkloads struct {
	// images is each site's live Dragonfly image. A site whose StatefulSet
	// does not exist, or has no Dragonfly container, is absent: its image
	// is unknown.
	images map[string]string
	// refused are the StatefulSets whose immutable selector differs from
	// the rendered one (workload_selector.go), which an upgrade must not
	// touch.
	refused []*workloadSelectorMismatchError
	// missing are the spec sites whose StatefulSet does not exist.
	missing []string
	// sets are the StatefulSets read, by site.
	sets map[string]*appsv1.StatefulSet
}

// dragonflyLiveImages reads each spec site's Dragonfly StatefulSet: its
// live image (from the pod template), whether it is refused for its
// selector, and whether it exists.
func dragonflyLiveImages(ctx context.Context, reader client.Reader, fg *v1alpha1.MysqlFailoverGroup) (dragonflyLiveWorkloads, error) {
	out := dragonflyLiveWorkloads{
		images: make(map[string]string, len(fg.Spec.Sites)),
		sets:   make(map[string]*appsv1.StatefulSet, len(fg.Spec.Sites)),
	}
	for _, site := range fg.Spec.Sites {
		sts := &appsv1.StatefulSet{}
		key := types.NamespacedName{Namespace: fg.Namespace, Name: dragonflyStatefulSetName(fg.Name, site.Name)}
		if err := reader.Get(ctx, key, sts); err != nil {
			if apierrors.IsNotFound(err) {
				out.missing = append(out.missing, site.Name)
				continue
			}
			return dragonflyLiveWorkloads{}, fmt.Errorf("get dragonfly statefulset %s: %w", site.Name, err)
		}
		out.sets[site.Name] = sts
		if mismatch := statefulSetSelectorMismatch(sts, fg, site.Name); mismatch != nil {
			out.refused = append(out.refused, mismatch)
		}
		if img := dragonflyTemplateImage(sts); img != "" {
			out.images[site.Name] = img
		}
	}
	return out, nil
}

// checkDragonflyUpgradeRequest ends an in-flight run that spec.dragonfly
// no longer asks for, or that has outlived dragonflyUpgradeRunTimeout.
// done reports that it ended the run or used the pass, and step is then
// the pass's result.
//
// A run moves the sites for the spec.dragonfly it started on (its
// specDigest), and the render that would apply anything newer is held
// while it runs:
//   - Dragonfly disabled or removed ends the run at once
//     (DragonflyDisabled); the render then removes the sites.
//   - Any other change of spec.dragonfly (the image reverted or moved on,
//     upgradeStrategy set to Rolling, the snapshot block or any other
//     field changed) ends the run (SpecChanged), and the next pass decides
//     afresh for the new spec, as it does for a Failed record whose digest
//     no longer matches. Continuing would restart, restore or re-create a
//     site with settings the user has replaced.
//   - A run older than dragonflyUpgradeRunTimeout ends (TimedOut).
//
// Ending a run gives the active site's traffic back. Once the active
// site's image has moved it may still be loading its snapshot, so a
// SpecChanged run first holds with that traffic off until the active
// answers as loaded, as for a refused active (checkDragonflyUpgradeWorkloads);
// the run's deadline bounds that hold too.
func (r *MysqlFailoverGroupReconciler) checkDragonflyUpgradeRequest(ctx context.Context, fg *v1alpha1.MysqlFailoverGroup, cur *v1alpha1.DragonflyUpgradeStatus) (dragonflyUpgradeStep, bool, error) {
	if !dragonflyEnabled(fg) {
		step, err := r.failDragonflyUpgrade(ctx, fg, cur, v1alpha1.DragonflyUpgradeReasonDragonflyDisabled,
			"spec.dragonfly was disabled or removed during the run; the run ends and the operator removes the Dragonfly workloads")
		return step, true, err
	}
	if cur.StartTime != nil && time.Since(cur.StartTime.Time) > dragonflyUpgradeRunTimeout {
		step, err := r.failDragonflyUpgrade(ctx, fg, cur, v1alpha1.DragonflyUpgradeReasonTimedOut,
			fmt.Sprintf("the run did not finish within %s of its start; it ended in phase %s (%s)", dragonflyUpgradeRunTimeout, cur.Phase, cur.Message))
		return step, true, err
	}
	why := dragonflyUpgradeSpecChange(fg.Spec.Dragonfly, cur)
	if why == "" {
		return dragonflyUpgradeStep{}, false, nil
	}
	live, err := dragonflyLiveImages(ctx, r.uncachedReader(), fg)
	if err != nil {
		return dragonflyUpgradeStep{}, true, err
	}
	// An active whose StatefulSet is gone has no pod left to load: the run
	// ends at once so the render re-creates it from the new spec rather
	// than the run re-creating it from settings the user replaced.
	if dragonflyUpgradeActiveMoved(cur, live.images) && live.sets[cur.ActiveSite] != nil {
		loaded, state, err := r.dragonflySiteLoaded(ctx, fg, cur.ActiveSite)
		if err != nil {
			return dragonflyUpgradeStep{}, true, err
		}
		if !loaded {
			if err := r.setDragonflyTrafficOnSite(ctx, fg, cur.ActiveSite, false); err != nil {
				return dragonflyUpgradeStep{}, true, err
			}
			msg := why + "; the run ends once the active site has loaded its snapshot (" + state + "), and its traffic stays off until then"
			return dragonflyUpgradeStep{hold: dragonflyUpgradePollInterval}, true, r.updateDragonflyUpgradeMessage(ctx, fg, cur, msg)
		}
	}
	step, err := r.failDragonflyUpgrade(ctx, fg, cur, v1alpha1.DragonflyUpgradeReasonSpecChanged,
		why+"; the run ends and the operator decides again for the new spec.dragonfly")
	return step, true, err
}

// dragonflyUpgradeSpecChange returns why spec no longer asks for the
// in-flight run cur, or "" while it still does.
func dragonflyUpgradeSpecChange(spec *v1alpha1.DragonflySpec, cur *v1alpha1.DragonflyUpgradeStatus) string {
	switch {
	case dragonflyUpgradeStrategy(spec) != v1alpha1.DragonflyUpgradeStrategySnapshotRestore:
		return fmt.Sprintf("spec.dragonfly.upgradeStrategy changed to %s during the run", dragonflyUpgradeStrategy(spec))
	case spec.Image != cur.TargetImage:
		return fmt.Sprintf("spec.dragonfly.image changed from %q to %q during the run", cur.TargetImage, spec.Image)
	case dragonflySpecDigest(spec) != cur.SpecDigest:
		return "spec.dragonfly changed during the run"
	}
	return ""
}

// dragonflyUpgradeActiveMoved reports whether the in-flight run cur has
// moved its active site's image (live is each site's live image): the
// active may then be loading its snapshot.
func dragonflyUpgradeActiveMoved(cur *v1alpha1.DragonflyUpgradeStatus, live map[string]string) bool {
	switch cur.Phase {
	case v1alpha1.DragonflyUpgradePhaseWaitingForActiveRestore:
		return true
	case v1alpha1.DragonflyUpgradePhaseUpdatingActive:
		return live[cur.ActiveSite] == cur.TargetImage
	}
	return false
}

// checkDragonflyUpgradeWorkloads checks, uncached, the StatefulSets of an
// admitted run before its next step. done reports that it ended the run
// or used the pass, and step is then the pass's result.
//
// A refused StatefulSet (replaced by hand, or by another release) ends the
// run before a step saves, restarts or updates it. Holding the run instead
// would keep the reconcile from re-creating the StatefulSet once it is
// deleted, which is the refusal's only remedy. Once the active site's
// image has moved (UpdatingActive after the image update, and
// WaitingForActiveRestore), the active may still be loading its snapshot,
// and ending the run gives its traffic back. A refused replica is recorded
// and ends the run at ReattachingReplicas, the step that touches it, once
// the active has restored. The active's own refusal is recorded at once and
// ends the run once the active answers as loaded; until then the run keeps
// the active's traffic off and holds.
//
// A deleted StatefulSet must not wedge the run either: the run holds the
// reconcile before the render that would re-create it. Where the run's own
// step would leave the site on the target image anyway, the run re-creates
// it on the target image and continues: a replica in any phase, which
// ReattachingReplicas moves and re-attaches, and the active once the
// snapshot is saved and before it is restored (UpdatingActive,
// WaitingForActiveRestore), where the new pod restores from the snapshot
// as the restarted one would. An active deleted before its snapshot was
// saved (Pending, SavingSnapshot) or after it restored (ReattachingReplicas)
// lost the data the run moves, so the run ends with ActiveWorkloadDeleted
// and the ordinary render and the Dragonfly manager take the site over, as
// they would without a run.
func (r *MysqlFailoverGroupReconciler) checkDragonflyUpgradeWorkloads(ctx context.Context, fg *v1alpha1.MysqlFailoverGroup, cur *v1alpha1.DragonflyUpgradeStatus) (dragonflyUpgradeStep, bool, error) {
	live, err := dragonflyLiveImages(ctx, r.uncachedReader(), fg)
	if err != nil {
		return dragonflyUpgradeStep{}, true, err
	}
	activeMoved := dragonflyUpgradeActiveMoved(cur, live.images)
	// A site found absent or with the rendered selector is no longer
	// refused, as the render would record; a StatefulSet re-created below
	// gets the rendered selector.
	for _, site := range fg.Spec.Sites {
		name := dragonflyStatefulSetName(fg.Name, site.Name)
		if !slices.ContainsFunc(live.refused, func(m *workloadSelectorMismatchError) bool { return m.name == name }) {
			r.noteWorkloadSelector(fg, "StatefulSet", name, nil)
		}
	}
	blocking := live.refused
	if activeMoved {
		blocking = nil
		for _, m := range live.refused {
			if m.site == cur.ActiveSite {
				blocking = append(blocking, m)
			} else {
				r.noteWorkloadSelector(fg, m.kind, m.name, m)
			}
		}
	}
	if why := r.noteDragonflySelectorRefusals(fg, blocking); why != "" {
		if activeMoved {
			// Only the active's own refusal blocks here, and ending the run
			// gives the active's traffic back, as does the label sweep of
			// the reconcile the run stops holding. While the active may
			// still be loading its snapshot, keep its traffic off and hold;
			// deleting the refused StatefulSet re-creates it above and the
			// run continues.
			loaded, state, err := r.dragonflySiteLoaded(ctx, fg, cur.ActiveSite)
			if err != nil {
				return dragonflyUpgradeStep{}, true, err
			}
			if !loaded {
				if err := r.setDragonflyTrafficOnSite(ctx, fg, cur.ActiveSite, false); err != nil {
					return dragonflyUpgradeStep{}, true, err
				}
				msg := why + "; the run ends once the active site has loaded its snapshot (" + state + "), and its traffic stays off until then"
				return dragonflyUpgradeStep{hold: dragonflyUpgradePollInterval}, true, r.updateDragonflyUpgradeMessage(ctx, fg, cur, msg)
			}
		}
		step, err := r.failDragonflyUpgrade(ctx, fg, cur, v1alpha1.DragonflyUpgradeReasonWorkloadSelectorMismatch, why)
		return step, true, err
	}
	if len(live.missing) == 0 {
		return dragonflyUpgradeStep{}, false, nil
	}
	for _, site := range live.missing {
		if site != cur.ActiveSite {
			continue
		}
		switch cur.Phase {
		case v1alpha1.DragonflyUpgradePhaseUpdatingActive, v1alpha1.DragonflyUpgradePhaseWaitingForActiveRestore:
		default:
			step, err := r.failDragonflyUpgrade(ctx, fg, cur, v1alpha1.DragonflyUpgradeReasonActiveWorkloadDeleted,
				fmt.Sprintf("StatefulSet %s of the active site %s was deleted during %s; the run ends and the operator re-creates it",
					dragonflyStatefulSetName(fg.Name, site), site, cur.Phase))
			return step, true, err
		}
	}
	for _, site := range fg.Spec.Sites {
		if !slices.Contains(live.missing, site.Name) {
			continue
		}
		if err := r.recreateDragonflyStatefulSetForUpgrade(ctx, fg, site, cur.TargetImage); err != nil {
			return dragonflyUpgradeStep{}, true, err
		}
		log.FromContext(ctx).Info("dragonfly snapshot-upgrade re-created a deleted StatefulSet",
			"fg", fg.Namespace+"/"+fg.Name, "site", site.Name, "phase", string(cur.Phase), "image", cur.TargetImage)
	}
	msg := fmt.Sprintf("re-created the deleted Dragonfly StatefulSet of site %s on %q", strings.Join(live.missing, ", "), cur.TargetImage)
	return dragonflyUpgradeStep{hold: dragonflyUpgradePollInterval}, true, r.updateDragonflyUpgradeMessage(ctx, fg, cur, msg)
}

// recreateDragonflyStatefulSetForUpgrade creates site's StatefulSet as the
// render would, on image.
func (r *MysqlFailoverGroupReconciler) recreateDragonflyStatefulSetForUpgrade(ctx context.Context, fg *v1alpha1.MysqlFailoverGroup, site v1alpha1.SiteSpec, image string) error {
	sts, err := r.desiredDragonflyStatefulSet(fg, site, nil)
	if err != nil {
		return err
	}
	for i := range sts.Spec.Template.Spec.Containers {
		if sts.Spec.Template.Spec.Containers[i].Name == dragonflyContainerName {
			sts.Spec.Template.Spec.Containers[i].Image = image
		}
	}
	if err := r.Create(ctx, sts); err != nil && !apierrors.IsAlreadyExists(err) {
		return fmt.Errorf("re-create dragonfly statefulset %s: %w", sts.Name, err)
	}
	return nil
}

// noteDragonflySelectorRefusals records refused StatefulSets as group
// refusals, as the render does, and returns why an upgrade cannot touch
// the sites, or "" when none is refused. Every snapshot-restore run saves,
// restarts or re-attaches every site, so one refused site keeps the whole
// run from being admitted or continued.
func (r *MysqlFailoverGroupReconciler) noteDragonflySelectorRefusals(fg *v1alpha1.MysqlFailoverGroup, refused []*workloadSelectorMismatchError) string {
	if len(refused) == 0 {
		return ""
	}
	names := make([]string, 0, len(refused))
	for _, m := range refused {
		r.noteWorkloadSelector(fg, m.kind, m.name, m)
		names = append(names, m.name)
	}
	return fmt.Sprintf("StatefulSet %s has a selector the operator does not render and is refused (WorkloadSelectorMismatch); "+
		"delete it and the operator re-creates it", strings.Join(names, ", "))
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
	workloads, err := dragonflyLiveImages(ctx, reader, fg)
	if err != nil {
		return dragonflyUpgradeDecision{}, err
	}
	live, refused := workloads.images, workloads.refused
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
	if why := r.noteDragonflySelectorRefusals(fg, refused); why != "" {
		// Admitting the run would save, restart or update a site whose
		// workload the operator refuses.
		return dragonflyUpgradeDecision{action: dragonflyUpgradeWait, why: why + "; the run starts once no site is refused"}, nil
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
	if why, err := r.dragonflyUpgradeConfigPending(fg, workloads); err != nil || why != "" {
		return dragonflyUpgradeDecision{action: dragonflyUpgradeWait, why: why}, err
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

// dragonflyUpgradeConfigPending returns why a run cannot be admitted yet
// because the sites do not run spec.dragonfly's settings other than the
// image, or "" once they do.
//
// A run saves the snapshot on the running active pod and restarts the
// sites by changing only their StatefulSet image, and it holds the render
// while it runs. Everything else the snapshot needs comes from the live
// pod template: --dir and the S3 flags, the credential env and the service
// account. A run admitted before the render installed them (one edit that
// sets the image, SnapshotRestore and the snapshot block together, or a
// fix of the snapshot settings that re-arms a Failed run) would save to
// the old place and restart the active empty. So the run waits while the
// render, which keeps each live image under SnapshotRestore, still has a
// change to make to any site's StatefulSet, and until the active site's
// pod runs its StatefulSet's current template and is Ready.
func (r *MysqlFailoverGroupReconciler) dragonflyUpgradeConfigPending(fg *v1alpha1.MysqlFailoverGroup, live dragonflyLiveWorkloads) (string, error) {
	var pending []string
	for _, site := range fg.Spec.Sites {
		sts := live.sets[site.Name]
		if sts == nil {
			continue
		}
		desired, err := r.desiredDragonflyStatefulSet(fg, site, sts)
		if err != nil {
			return "", fmt.Errorf("render dragonfly statefulset %s: %w", site.Name, err)
		}
		if !dragonflyStatefulSetTemplateEqual(sts, desired) {
			pending = append(pending, site.Name)
		}
	}
	if len(pending) > 0 {
		return fmt.Sprintf("the Dragonfly StatefulSet of site %s does not carry the current spec.dragonfly settings yet; "+
			"the run starts once they are rolled out with each site's running image", strings.Join(pending, ", ")), nil
	}
	active := fg.Status.ActiveSite
	if sts := live.sets[active]; sts == nil || !dragonflyStatefulSetRolledOut(sts) {
		return fmt.Sprintf("the Dragonfly pod of the active site %s does not run its StatefulSet's current template, or is not Ready, yet; "+
			"the run starts once it does", active), nil
	}
	return "", nil
}

// dragonflyStatefulSetRolledOut reports whether sts's status shows its
// current template rolled out: observed, every pod on the update
// revision and Ready.
func dragonflyStatefulSetRolledOut(sts *appsv1.StatefulSet) bool {
	if !dragonflyStatefulSetRolloutComplete(sts) {
		return false
	}
	return sts.Status.UpdateRevision == "" || sts.Status.CurrentRevision == sts.Status.UpdateRevision
}

// uncachedReader is the API server reader when one is wired, else the
// cached client.
func (r *MysqlFailoverGroupReconciler) uncachedReader() client.Reader {
	if r.APIReader != nil {
		return r.APIReader
	}
	return r.Client
}

// noteDragonflyUpgradeDeferral logs a waiting run and emits
// DragonflyUpgradeDeferred when the cause changes, not on every retry. The
// memo is per group incarnation (UID), so a group deleted and re-created
// under its name between two reconciles still gets its first Event; the
// reconcile drops it once the group is gone.
func (r *MysqlFailoverGroupReconciler) noteDragonflyUpgradeDeferral(ctx context.Context, fg *v1alpha1.MysqlFailoverGroup, why string) {
	nn := types.NamespacedName{Namespace: fg.Namespace, Name: fg.Name}
	note := string(fg.UID) + "\x00" + why
	r.dragonflyUpgradeDeferMu.Lock()
	prev, seen := r.dragonflyUpgradeDeferNote[nn]
	if r.dragonflyUpgradeDeferNote == nil {
		r.dragonflyUpgradeDeferNote = make(map[types.NamespacedName]string)
	}
	r.dragonflyUpgradeDeferNote[nn] = note
	r.dragonflyUpgradeDeferMu.Unlock()
	if seen && prev == note {
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
// A run whose active site already runs the target (one that ended after
// the active moved, or whose record was lost) starts at
// WaitingForActiveRestore: the snapshot and the active restart are behind
// it, but the template image is no proof that the active restored, so no
// replica is touched until the active answers as a loaded master on the
// target.
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
		st.Phase = v1alpha1.DragonflyUpgradePhaseWaitingForActiveRestore
		st.Message = fmt.Sprintf("active site already runs %q; waiting for it to answer as restored before updating replicas", d.target)
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
	next.Message = why + "; upgrade abandoned. It is not restarted for this spec.dragonfly: change spec.dragonfly to retry against the new active site " +
		"(setting spec.dragonfly.image back and forward again works only while every site runs one image)"
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

// failDragonflyUpgrade ends the run cur as Failed{reason}. The active
// site's traffic is given back, unless the run has restarted the active
// on its snapshot (dragonflyUpgradeActiveMoved) and the active does not
// answer as loaded yet: a run that ends then (its deadline, a spec change,
// a refusal) must not put a Dragonfly that is still loading behind the
// active Service. Its traffic is removed instead, and the label sweep
// gives it back once the site answers as loaded
// (dragonflyUpgradeTrafficGateSite).
func (r *MysqlFailoverGroupReconciler) failDragonflyUpgrade(ctx context.Context, fg *v1alpha1.MysqlFailoverGroup, cur *v1alpha1.DragonflyUpgradeStatus, reason v1alpha1.DragonflyUpgradeReason, msg string) (dragonflyUpgradeStep, error) {
	loading, state, err := r.dragonflyUpgradeActiveLoading(ctx, fg, cur, reason)
	if err != nil {
		return dragonflyUpgradeStep{}, err
	}
	if loading {
		if err := holdDragonflyTrafficForLoad(ctx, r.Client, fg, cur.ActiveSite, r.now()); err != nil {
			return dragonflyUpgradeStep{}, err
		}
		msg += "; the active site's Dragonfly has not loaded its snapshot yet (" + state + "), so its traffic stays off until it does"
	} else {
		_ = r.setDragonflyTrafficOnSite(ctx, fg, cur.ActiveSite, true)
	}
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

// dragonflyUpgradeActiveLoading reports whether the run cur, ending for
// reason, leaves its active site restarted on the snapshot and not yet
// answering as loaded, with the probe's state. An active the run has not
// restarted is the pod that served before the run and gets its traffic
// back as it was; so does one whose StatefulSet is gone, and every site
// when Dragonfly is disabled, as the render then removes the workloads.
func (r *MysqlFailoverGroupReconciler) dragonflyUpgradeActiveLoading(ctx context.Context, fg *v1alpha1.MysqlFailoverGroup, cur *v1alpha1.DragonflyUpgradeStatus, reason v1alpha1.DragonflyUpgradeReason) (bool, string, error) {
	if reason == v1alpha1.DragonflyUpgradeReasonDragonflyDisabled || !dragonflyEnabled(fg) {
		return false, "", nil
	}
	live, err := dragonflyLiveImages(ctx, r.uncachedReader(), fg)
	if err != nil {
		return false, "", err
	}
	if !dragonflyUpgradeActiveMoved(cur, live.images) || live.sets[cur.ActiveSite] == nil {
		return false, "", nil
	}
	loaded, state, err := r.dragonflySiteLoaded(ctx, fg, cur.ActiveSite)
	if err != nil {
		return false, "", err
	}
	return !loaded, state, nil
}

// dragonflyUpgradeTrafficGateSite returns the site whose Dragonfly pods
// the label sweep gives traffic (or the master role) only once the site
// answers as loaded: the active site of a Failed snapshot-restore run,
// which can have ended (its deadline, a spec change, a refusal) while the
// pod it restarted on the snapshot was still loading. "" when there is
// none. The sweep probes only when it would change a pod's labels, so a
// site already serving is never probed.
func dragonflyUpgradeTrafficGateSite(fg *v1alpha1.MysqlFailoverGroup) string {
	cur := dragonflyUpgradeStatus(fg)
	if cur == nil || cur.Phase != v1alpha1.DragonflyUpgradePhaseFailed {
		return ""
	}
	return cur.ActiveSite
}

// dragonflyUpgradeTrafficHeld reports whether a pod of the Failed run
// cur's active site carries DragonflyLoadHoldAnnotation or has no traffic
// label: the run, or the label sweep's gate after it, holds it off until
// the site loads. The record is the gate's input, so it is not removed
// while that holds.
func (r *MysqlFailoverGroupReconciler) dragonflyUpgradeTrafficHeld(ctx context.Context, fg *v1alpha1.MysqlFailoverGroup, cur *v1alpha1.DragonflyUpgradeStatus) (bool, error) {
	if cur == nil || cur.Phase != v1alpha1.DragonflyUpgradePhaseFailed || cur.ActiveSite == "" {
		return false, nil
	}
	pods, err := listDragonflyPodsForSite(ctx, r.Client, fg, cur.ActiveSite)
	if err != nil {
		return false, err
	}
	for i := range pods {
		if _, ok := pods[i].Annotations[v1alpha1.DragonflyLoadHoldAnnotation]; ok {
			return true, nil
		}
		if pods[i].Labels[v1alpha1.DragonflyTrafficLabel] != dragonflyTrafficEnabled {
			return true, nil
		}
	}
	return false, nil
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
		// The run checked every selector before this step; this keeps a
		// refused StatefulSet from being updated whatever the caller.
		if mismatch := statefulSetSelectorMismatch(&sts, fg, siteName); mismatch != nil {
			r.noteWorkloadSelector(fg, mismatch.kind, mismatch.name, mismatch)
			return mismatch
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

// dragonflySiteReadyOnImage reports whether siteName's Dragonfly runs image
// and has restored: as master (wantMaster), or as a replica in sync.
func (r *MysqlFailoverGroupReconciler) dragonflySiteReadyOnImage(ctx context.Context, fg *v1alpha1.MysqlFailoverGroup, siteName, image string, wantMaster bool) (bool, string, error) {
	probe, state, ok, err := r.probeDragonflySite(ctx, fg, siteName, image, true)
	if err != nil || !ok {
		return false, state, err
	}
	info := probe.info
	if wantMaster {
		return info.Role == "master", fmt.Sprintf("site %s role=%s", siteName, info.Role), nil
	}
	ready := dragonfly.CandidateSyncReady(info, probe.persist, 0)
	return ready, fmt.Sprintf("site %s role=%s link=%s sync=%v", siteName, info.Role, info.MasterLinkStatus, info.MasterSyncInProgress), nil
}

// dragonflySiteLoaded reports whether siteName's Dragonfly can take
// traffic again whatever workload or image runs it.
func (r *MysqlFailoverGroupReconciler) dragonflySiteLoaded(ctx context.Context, fg *v1alpha1.MysqlFailoverGroup, siteName string) (bool, string, error) {
	_, state, ok, err := r.probeDragonflySite(ctx, fg, siteName, "", false)
	if err != nil || !ok {
		return false, state, err
	}
	return true, fmt.Sprintf("site %s loaded", siteName), nil
}

// dragonflySiteProbe is what probeDragonflySite read.
type dragonflySiteProbe struct {
	info    dragonfly.ReplicationInfo
	persist dragonfly.PersistenceInfo
}

// probeDragonflySite checks that siteName has exactly one Dragonfly pod,
// not terminating and Ready (and on image, unless image is ""), that
// answers INFO persistence (and INFO replication, withReplication) and is
// not loading a snapshot. Anything it cannot confirm is not ok, and state
// says why; an unanswered INFO persistence counts as still loading.
func (r *MysqlFailoverGroupReconciler) probeDragonflySite(ctx context.Context, fg *v1alpha1.MysqlFailoverGroup, siteName, image string, withReplication bool) (dragonflySiteProbe, string, bool, error) {
	pods, err := listDragonflyPodsForSite(ctx, r.Client, fg, siteName)
	if err != nil {
		return dragonflySiteProbe{}, "", false, err
	}
	if len(pods) != 1 {
		return dragonflySiteProbe{}, fmt.Sprintf("site %s has %d pods", siteName, len(pods)), false, nil
	}
	pod := pods[0]
	if pod.DeletionTimestamp != nil {
		return dragonflySiteProbe{}, fmt.Sprintf("site %s pod terminating", siteName), false, nil
	}
	if image != "" && (len(pod.Spec.Containers) == 0 || pod.Spec.Containers[0].Image != image) {
		return dragonflySiteProbe{}, fmt.Sprintf("site %s pod image not updated", siteName), false, nil
	}
	if !podReady(&pod) {
		return dragonflySiteProbe{}, fmt.Sprintf("site %s pod not Ready", siteName), false, nil
	}
	// The pod is dialed with the settings it runs, not spec.dragonfly's,
	// which a spec change during (or just after) a run may have replaced.
	conn, err := r.dragonflyDialPod(ctx, fg, siteName, &pod)
	if err != nil {
		return dragonflySiteProbe{}, fmt.Sprintf("site %s not reachable: %v", siteName, err), false, nil
	}
	defer func() { _ = conn.Close() }()
	var probe dragonflySiteProbe
	if withReplication {
		if probe.info, err = conn.InfoReplication(ctx); err != nil {
			return dragonflySiteProbe{}, fmt.Sprintf("site %s INFO failed: %v", siteName, err), false, nil
		}
	}
	if probe.persist, err = conn.InfoPersistence(ctx); err != nil {
		return dragonflySiteProbe{}, fmt.Sprintf("site %s INFO persistence failed: %v", siteName, err), false, nil
	}
	if probe.persist.Loading || probe.persist.LoadState != "" {
		return dragonflySiteProbe{}, fmt.Sprintf("site %s still loading snapshot", siteName), false, nil
	}
	return probe, "", true, nil
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
