package controller

import (
	"context"
	stderrors "errors"
	"fmt"
	"time"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
	"sigs.k8s.io/controller-runtime/pkg/log"

	v1alpha1 "github.com/shipstream/bloodraven/api/v1alpha1"
)

// The Dragonfly template rollout inside one group (issue #217).
//
// A Dragonfly pod keeps its data in an emptyDir: a restarted pod comes back
// empty. The cache survives a template change only if the pods are replaced
// one at a time and the master's pod is replaced only after another site
// took the master over. The rollout is therefore a strict protocol, and
// every step that cannot be proven safe is a wait, never a restart:
//
//  1. Nothing is written until the operator knows which site is the master:
//     status names a site, that site answers as a master, and no other site
//     does (dragonflyRolloutAuthority).
//  2. One site at a time. The template write marks the site's StatefulSet
//     (DragonflyRolloutInFlightAnnotation) in the same update, and the mark
//     stays until the site's pod is Ready on the new template and the site
//     is back in sync with the master. While a site is marked, no other
//     site's template is written.
//  3. Replicas first, in spec order. The master's site is last: a replica
//     proven in sync (dragonflyProveSync) takes the master over with
//     REPLTAKEOVER, and only then is the former master's template written.
//     A takeover that fails is retried with a backoff for as long as it
//     takes. No REPLICAOF NO ONE is sent and the master's pod is not
//     restarted where it is.
//
// The one way past a wait is an administrator's:
// DragonflyRestartInPlaceAnnotation names a site whose template is applied
// without these proofs.
//
// Every template write, the administrator's included, is made under the
// group's Dragonfly gate. The cross-group pacing of a release-driven change
// (rollout.go) is decided before any of this: a StatefulSet the group is
// not admitted to roll is not part of the rollout.

const (
	// dragonflyRolloutRequeue is how soon a rollout that is not finished is
	// looked at again.
	dragonflyRolloutRequeue = 2 * time.Second
	// dragonflyRolloutObserveTimeout bounds the read of one site's role when
	// the rollout establishes which site is the master.
	dragonflyRolloutObserveTimeout = 2 * time.Second
)

// The states of a rollout that waits for something other than its
// promotion target's sync. With the dragonflyRollout* states of a sync check
// they are the `state` of the rollout's log lines and of the
// DragonflyRolloutStalled Event.
const (
	// The operator cannot tell which site is the Dragonfly master.
	dragonflyRolloutMasterUnknown = "MasterUnknown"
	// A site the rollout restarted is not back in sync.
	dragonflyRolloutSiteInFlight = "SiteInFlight"
	// REPLTAKEOVER from the active site failed; it is retried.
	dragonflyRolloutTakeoverFailed = "TakeoverFailed"
)

// dragonflyRolloutSite is one site's StatefulSet as the rollout pass read it.
type dragonflyRolloutSite struct {
	site    v1alpha1.SiteSpec
	current *appsv1.StatefulSet
	// drift: the template differs from the running render and the paced
	// rollout does not hold it. The rollout has to write it.
	drift bool
	// inFlight: the StatefulSet carries DragonflyRolloutInFlightAnnotation.
	inFlight bool
}

// dragonflyRolloutWait is what a rollout pass waits for; the zero value is a
// pass that waits for nothing the wait record covers.
type dragonflyRolloutWait struct {
	State  string
	Detail string
}

// dragonflyRolloutWaitState is the record of one group's wait: since when,
// and whether the stall has been reported.
type dragonflyRolloutWaitState struct {
	state    string
	since    time.Time
	reported bool
}

// reconcileDragonflyStatefulSetsSerial creates missing Dragonfly
// StatefulSets and moves the others to the running render one site at a
// time (see the protocol at the top of this file). A positive duration
// means the rollout is not finished and the group is to be reconciled again.
func (r *MysqlFailoverGroupReconciler) reconcileDragonflyStatefulSetsSerial(ctx context.Context, fg *v1alpha1.MysqlFailoverGroup) (time.Duration, error) {
	reader := r.authorityReader()
	sites := make([]dragonflyRolloutSite, 0, len(fg.Spec.Sites))
	anyDrift, anyInFlight, anyHeld := false, false, false

	for i := range fg.Spec.Sites {
		site := fg.Spec.Sites[i]
		current := &appsv1.StatefulSet{}
		key := types.NamespacedName{Namespace: fg.Namespace, Name: dragonflyStatefulSetName(fg.Name, site.Name)}
		if err := reader.Get(ctx, key, current); err != nil {
			if apierrors.IsNotFound(err) {
				r.noteWorkloadSelector(fg, "StatefulSet", key.Name, nil)
				if err := r.reconcileDragonflyStatefulSet(ctx, fg, site); err != nil {
					return 0, fmt.Errorf("reconcile dragonfly statefulset %s: %w", site.Name, err)
				}
				continue
			}
			return 0, fmt.Errorf("get dragonfly statefulset %s: %w", site.Name, err)
		}
		// A StatefulSet whose immutable selector differs is refused: the
		// site is skipped, not updated, and not retried in a loop.
		mismatch := statefulSetSelectorMismatch(current, fg, site.Name)
		r.noteWorkloadSelector(fg, "StatefulSet", key.Name, mismatch)
		if mismatch != nil {
			continue
		}

		desired, err := r.desiredDragonflyStatefulSet(fg, site, current)
		if err != nil {
			return 0, fmt.Errorf("build desired dragonfly statefulset %s: %w", site.Name, err)
		}
		st := dragonflyRolloutSite{site: site, current: current}
		if !dragonflyStatefulSetTemplateEqual(current, desired) {
			// A template another operator release rendered moves only once
			// the group is admitted to the paced rollout (rollout.go).
			held, err := r.gateDragonflyRollout(ctx, fg, current)
			if err != nil {
				return 0, fmt.Errorf("paced rollout of dragonfly statefulset %s: %w", site.Name, err)
			}
			st.drift = !held
			anyHeld = anyHeld || held
		} else if r.RolloutPacing && renderedByOther(current) {
			if err := r.restampWorkload(ctx, current); err != nil {
				return 0, fmt.Errorf("restamp dragonfly statefulset %s: %w", site.Name, err)
			}
		}
		st.inFlight = current.Annotations[v1alpha1.DragonflyRolloutInFlightAnnotation] != ""
		anyDrift = anyDrift || st.drift
		anyInFlight = anyInFlight || st.inFlight
		sites = append(sites, st)
	}

	if !anyDrift && !anyInFlight {
		return 0, r.endDragonflyRollout(ctx, fg, anyHeld)
	}
	requeue, wait, err := r.stepDragonflyRollout(ctx, fg, sites)
	if err != nil {
		return 0, err
	}
	r.noteDragonflyRolloutWait(ctx, fg, wait)
	return requeue, nil
}

// endDragonflyRollout runs when no template is to be written and no site is
// in flight: it ends the group's wait records, and removes a
// DragonflyRestartInPlaceAnnotation that has nothing left to apply to, so
// that it does not stay armed for a later rollout. While the paced rollout
// holds a template the annotation is kept: it may be meant for that roll.
func (r *MysqlFailoverGroupReconciler) endDragonflyRollout(ctx context.Context, fg *v1alpha1.MysqlFailoverGroup, held bool) error {
	nn := types.NamespacedName{Namespace: fg.Namespace, Name: fg.Name}
	r.noteDragonflyRolloutWait(ctx, fg, dragonflyRolloutWait{})
	r.dragonflyRolloutMu.Lock()
	for key := range r.dragonflyRolloutBackoff {
		if key.fg == nn {
			delete(r.dragonflyRolloutBackoff, key)
		}
	}
	r.dragonflyRolloutMu.Unlock()
	if held || fg.Annotations[v1alpha1.DragonflyRestartInPlaceAnnotation] == "" {
		return nil
	}
	removed, err := r.removeDragonflyRestartInPlace(ctx, nn, "")
	if err != nil {
		return fmt.Errorf("remove the %s annotation: %w", v1alpha1.DragonflyRestartInPlaceAnnotation, err)
	}
	if removed {
		log.FromContext(ctx).Info("dragonfly rollout: removed the restart-in-place annotation",
			"reason", "no Dragonfly pod template is pending", "fg", nn)
	}
	return nil
}

// removeDragonflyRestartInPlace removes DragonflyRestartInPlaceAnnotation
// from the group, read uncached. With site set it is removed only while it
// still names that site.
func (r *MysqlFailoverGroupReconciler) removeDragonflyRestartInPlace(ctx context.Context, nn types.NamespacedName, site string) (bool, error) {
	fresh, err := r.freshFailoverGroup(ctx, nn)
	if err != nil {
		return false, client.IgnoreNotFound(err)
	}
	value, ok := fresh.Annotations[v1alpha1.DragonflyRestartInPlaceAnnotation]
	if !ok || (site != "" && value != site) {
		return false, nil
	}
	base := fresh.DeepCopy()
	delete(fresh.Annotations, v1alpha1.DragonflyRestartInPlaceAnnotation)
	if err := r.Patch(ctx, fresh, metadataPatch(base)); err != nil {
		return false, client.IgnoreNotFound(err)
	}
	return true, nil
}

// stepDragonflyRollout takes the rollout's next step, at most one template
// write, from the sites one pass read. wait says what it waits for.
func (r *MysqlFailoverGroupReconciler) stepDragonflyRollout(ctx context.Context, fg *v1alpha1.MysqlFailoverGroup, sites []dragonflyRolloutSite) (time.Duration, dragonflyRolloutWait, error) {
	nn := types.NamespacedName{Namespace: fg.Namespace, Name: fg.Name}
	// The status the rollout decides by is read uncached: the reconcile's
	// own read can be behind a promotion that has just been stamped.
	fresh, err := r.freshFailoverGroup(ctx, nn)
	if err != nil {
		if apierrors.IsNotFound(err) {
			return 0, dragonflyRolloutWait{}, nil
		}
		return 0, dragonflyRolloutWait{}, fmt.Errorf("re-read failover group for the dragonfly rollout: %w", err)
	}

	// The administrator's way past a wait comes before every proof.
	inPlace := fresh.Annotations[v1alpha1.DragonflyRestartInPlaceAnnotation]
	inPlaceNote := ""
	if inPlace != "" {
		known := false
		for i := range sites {
			if sites[i].site.Name != inPlace {
				continue
			}
			known = true
			if sites[i].drift {
				requeue, err := r.restartDragonflySiteInPlace(ctx, fg, sites[i].site)
				return requeue, dragonflyRolloutWait{}, err
			}
		}
		if !known {
			inPlaceNote = fmt.Sprintf("; the %s annotation names %q, which is not a site with a Dragonfly StatefulSet",
				v1alpha1.DragonflyRestartInPlaceAnnotation, inPlace)
		}
	}

	auth := r.dragonflyRolloutAuthority(ctx, fresh)
	if auth.Master == "" {
		return dragonflyRolloutRequeue, dragonflyRolloutWait{State: dragonflyRolloutMasterUnknown, Detail: auth.Detail + inPlaceNote}, nil
	}

	// A site the rollout restarted is done once it is back in sync.
	unsettled := make(map[string]string)
	for i := range sites {
		s := &sites[i]
		if !s.inFlight {
			continue
		}
		settled, detail := r.dragonflyRolloutSiteSettled(ctx, fg, *s, auth.Master)
		if !settled {
			unsettled[s.site.Name] = detail
			continue
		}
		if err := r.clearDragonflyRolloutInFlight(ctx, s.current); err != nil {
			return 0, dragonflyRolloutWait{}, fmt.Errorf("clear the in-flight mark of dragonfly statefulset %s: %w", s.site.Name, err)
		}
		s.inFlight = false
		log.FromContext(ctx).Info("dragonfly rollout: site back in sync", "site", s.site.Name, "master", auth.Master, "fg", nn)
	}

	// The next site to write: a replica that is in flight and whose
	// template changed again (it is the one site already disturbed), then
	// the replicas in spec order, then the master's site.
	var next *dragonflyRolloutSite
	for _, want := range []func(s dragonflyRolloutSite) bool{
		func(s dragonflyRolloutSite) bool { return s.inFlight && s.site.Name != auth.Master },
		func(s dragonflyRolloutSite) bool { return s.site.Name != auth.Master },
		func(dragonflyRolloutSite) bool { return true },
	} {
		for i := range sites {
			if next == nil && sites[i].drift && want(sites[i]) {
				next = &sites[i]
			}
		}
	}
	// One site at a time: a site in flight holds every other site's write.
	for _, s := range sites {
		if !s.inFlight || (next != nil && s.site.Name == next.site.Name) {
			continue
		}
		detail := fmt.Sprintf("site %q was restarted by the rollout and is not back in sync with master %q (%s)",
			s.site.Name, auth.Master, unsettled[s.site.Name])
		if next != nil {
			detail += fmt.Sprintf("; site %q waits for it", next.site.Name)
		}
		return dragonflyRolloutRequeue, dragonflyRolloutWait{State: dragonflyRolloutSiteInFlight, Detail: detail + inPlaceNote}, nil
	}
	if next == nil {
		return dragonflyRolloutRequeue, dragonflyRolloutWait{}, nil
	}

	if next.site.Name != auth.Master {
		requeue, err := r.rollDragonflyReplicaStatefulSet(ctx, fg, next.site, auth.Master)
		if err != nil || requeue > 0 {
			return requeue, dragonflyRolloutWait{}, err
		}
		return dragonflyRolloutRequeue, dragonflyRolloutWait{}, nil
	}
	return r.rollDragonflyMasterSite(ctx, fg, sites, *next)
}

// rollDragonflyMasterSite is the rollout's last step: the master's own site.
// Its template is written only by a takeover that succeeded
// (promoteDragonflyForRollout). Until then the site keeps its pod.
func (r *MysqlFailoverGroupReconciler) rollDragonflyMasterSite(ctx context.Context, fg *v1alpha1.MysqlFailoverGroup, sites []dragonflyRolloutSite, master dragonflyRolloutSite) (time.Duration, dragonflyRolloutWait, error) {
	// A planned failover that owns the group's Dragonfly owns its active
	// site too (plannedOwnsDragonfly): with the topology manager
	// registered, the claim slot decides (a run, its provisional grant,
	// or a hold; a run that released its claim after recording its
	// promotion owns nothing, whatever status says); before that, the
	// status form does (a run that still needs the claim, or a durable
	// hold). A request that is only waiting owns nothing.
	if r.plannedOwnsDragonfly(fg) {
		return dragonflyRolloutRequeue, dragonflyRolloutWait{}, nil
	}
	source := master.site.Name
	target := ""
	for _, s := range sites {
		if s.site.Name == source {
			continue
		}
		check := dragonflyRolloutCheck{State: dragonflyRolloutNotLinked,
			Detail: "its StatefulSet rollout is not complete (no Ready pod on its current template)"}
		if dragonflyStatefulSetRolloutComplete(s.current) {
			check = r.dragonflyRolloutCandidateSyncReady(ctx, fg, s.site.Name, source)
		}
		r.noteDragonflyRolloutReadiness(ctx, fg, s.site.Name, source, check)
		if check.Ready {
			target = s.site.Name
			break
		}
	}
	if target == "" {
		// noteDragonflyRolloutReadiness reports the stall.
		return dragonflyRolloutRequeue, dragonflyRolloutWait{}, nil
	}
	failed := func(attempts int) dragonflyRolloutWait {
		return dragonflyRolloutWait{State: dragonflyRolloutTakeoverFailed, Detail: fmt.Sprintf(
			"REPLTAKEOVER of the Dragonfly master from %q to %q has failed %d time(s) and is retried; %q keeps its pod until a takeover succeeds",
			source, target, attempts, source)}
	}
	if wait, attempts := r.dragonflyRolloutPromotionBackoff(fg, target, source); wait > 0 {
		return wait, failed(attempts), nil
	}
	// The takeover is the roll's first action on this site: latch first.
	if err := r.latchDragonflyRollout(ctx, fg, master.current); err != nil {
		if stderrors.Is(err, errRolloutHeld) {
			return dragonflyRolloutRequeue, dragonflyRolloutWait{}, nil
		}
		return 0, dragonflyRolloutWait{}, fmt.Errorf("paced rollout of dragonfly statefulset %s: %w", source, err)
	}
	promoted, deferred := r.promoteDragonflyForRollout(ctx, fg, target, source, master.site)
	switch {
	case deferred:
		return dragonflyGateRetry, dragonflyRolloutWait{}, nil
	case promoted:
		r.clearDragonflyRolloutPromotionBackoff(fg, target, source)
		return dragonflyRolloutRequeue, dragonflyRolloutWait{}, nil
	}
	return dragonflyRolloutRequeue, failed(r.recordDragonflyRolloutPromotionFailure(fg, target, source)), nil
}

// dragonflyRolloutAuthority is the rollout's answer to "which site is the
// Dragonfly master". Master is empty when it cannot tell; Detail says why.
type dragonflyRolloutAuthority struct {
	Master string
	Detail string
}

// dragonflyRolloutAuthority establishes the Dragonfly master for a rollout
// from fg (an uncached read) and from the sites themselves. Status alone is
// not enough: it is empty before the manager's first publication and can be
// behind a role change. The master is known when status names a site of the
// group, that site answers INFO as a master, and no other site that answers
// does. A site that does not answer is not a master that serves, so it does
// not make the answer unknown, unless it is the one status names.
func (r *MysqlFailoverGroupReconciler) dragonflyRolloutAuthority(ctx context.Context, fg *v1alpha1.MysqlFailoverGroup) dragonflyRolloutAuthority {
	claimed := effectiveDragonflyMasterSite(fg)
	if claimed == "" {
		return dragonflyRolloutAuthority{Detail: "status names no Dragonfly master (status.dragonfly.activeSite and status.activeSite are empty)"}
	}
	known := false
	for _, site := range fg.Spec.Sites {
		known = known || site.Name == claimed
	}
	if !known {
		return dragonflyRolloutAuthority{Detail: fmt.Sprintf("status names %q as the Dragonfly master, which is not a site of the group", claimed)}
	}
	dial := r.dragonflySiteDialer(fg)
	role, err := dragonflyObservedRole(ctx, dial, claimed)
	switch {
	case err != nil:
		return dragonflyRolloutAuthority{Detail: fmt.Sprintf("status names %q as the Dragonfly master, and it could not be read: %v", claimed, err)}
	case role != "master":
		return dragonflyRolloutAuthority{Detail: fmt.Sprintf("status names %q as the Dragonfly master, but it answers with role %q", claimed, role)}
	}
	for _, site := range fg.Spec.Sites {
		if site.Name == claimed {
			continue
		}
		if role, err := dragonflyObservedRole(ctx, dial, site.Name); err == nil && role == "master" {
			return dragonflyRolloutAuthority{Detail: fmt.Sprintf("status names %q as the Dragonfly master, but %q answers as a master too", claimed, site.Name)}
		}
	}
	return dragonflyRolloutAuthority{Master: claimed}
}

// dragonflyObservedRole reads the role site's Dragonfly reports in INFO
// replication, within dragonflyRolloutObserveTimeout.
func dragonflyObservedRole(ctx context.Context, dial dragonflySiteDialer, site string) (string, error) {
	ctx, cancel := context.WithTimeout(ctx, dragonflyRolloutObserveTimeout)
	defer cancel()
	conn, err := dial(ctx, site)
	if err != nil {
		return "", err
	}
	defer func() { _ = conn.Close() }()
	info, err := conn.InfoReplication(ctx)
	if err != nil {
		return "", err
	}
	return info.Role, nil
}

// dragonflyRolloutSiteSettled reports whether a site the rollout restarted
// is done: its pod is Ready on the StatefulSet's current template, and the
// site is the master itself or a replica proven in sync with it
// (dragonflyProveSync, which a replica in a full sync, one that is behind
// and one that follows another instance do not pass). detail says what is
// missing.
func (r *MysqlFailoverGroupReconciler) dragonflyRolloutSiteSettled(ctx context.Context, fg *v1alpha1.MysqlFailoverGroup, s dragonflyRolloutSite, master string) (bool, string) {
	if !dragonflyStatefulSetRolloutComplete(s.current) {
		return false, "no Ready pod on the new template yet"
	}
	if s.site.Name == master {
		return true, ""
	}
	check := dragonflyProveSync(ctx, r.dragonflySiteDialer(fg), s.site.Name, master, 0, dragonflyRolloutSyncPoll)
	if check.Ready {
		return true, ""
	}
	return false, fmt.Sprintf("%s: %s", check.State, check.Detail)
}

// rollDragonflyStatefulSetTemplate writes site's StatefulSet with the
// running render and marks it in flight in the same update. The caller
// holds the group's Dragonfly gate.
func (r *MysqlFailoverGroupReconciler) rollDragonflyStatefulSetTemplate(ctx context.Context, fg *v1alpha1.MysqlFailoverGroup, site v1alpha1.SiteSpec) error {
	sts := &appsv1.StatefulSet{
		ObjectMeta: metav1.ObjectMeta{
			Name:      dragonflyStatefulSetName(fg.Name, site.Name),
			Namespace: fg.Namespace,
		},
	}
	// The paced rollout's start is latched before the write (rollout.go).
	current := &appsv1.StatefulSet{}
	switch err := r.authorityReader().Get(ctx, client.ObjectKeyFromObject(sts), current); {
	case apierrors.IsNotFound(err):
	case err != nil:
		return err
	default:
		if err := r.latchDragonflyRollout(ctx, fg, current); err != nil {
			return err
		}
	}
	_, err := controllerutil.CreateOrUpdate(ctx, r.Client, sts, func() error {
		if err := r.applyDragonflyStatefulSetSpec(fg, site, sts); err != nil {
			return err
		}
		if sts.Annotations == nil {
			sts.Annotations = make(map[string]string)
		}
		sts.Annotations[v1alpha1.DragonflyRolloutInFlightAnnotation] = r.now().UTC().Format(time.RFC3339)
		return nil
	})
	return err
}

// clearDragonflyRolloutInFlight removes the in-flight mark with a patch of
// that one annotation.
func (r *MysqlFailoverGroupReconciler) clearDragonflyRolloutInFlight(ctx context.Context, sts *appsv1.StatefulSet) error {
	base := sts.DeepCopy()
	delete(sts.Annotations, v1alpha1.DragonflyRolloutInFlightAnnotation)
	return client.IgnoreNotFound(r.Patch(ctx, sts, client.MergeFrom(base)))
}

// restartDragonflySiteInPlace applies site's pending template because an
// administrator asked for it (DragonflyRestartInPlaceAnnotation), without
// knowing which site is the master and without a takeover. It still waits
// for a planned failover that owns the group's Dragonfly, and it takes the
// group's Dragonfly gate. The annotation is removed once the template is
// written: it covers one restart.
func (r *MysqlFailoverGroupReconciler) restartDragonflySiteInPlace(ctx context.Context, fg *v1alpha1.MysqlFailoverGroup, site v1alpha1.SiteSpec) (time.Duration, error) {
	nn := types.NamespacedName{Namespace: fg.Namespace, Name: fg.Name}
	logger := log.FromContext(ctx)
	release, ok := r.acquireDragonflyGate(ctx, nn)
	if !ok {
		return dragonflyGateRetry, nil
	}
	defer release()
	fresh, err := r.freshFailoverGroup(ctx, nn)
	if err != nil {
		return 0, fmt.Errorf("re-read failover group before the in-place dragonfly restart of %s: %w", site.Name, err)
	}
	if fresh.Annotations[v1alpha1.DragonflyRestartInPlaceAnnotation] != site.Name {
		return dragonflyGateRetry, nil
	}
	if r.plannedOwnsDragonfly(fresh) || r.plannedClaimHeld(nn) || PlannedHoldFrom(fresh) != nil {
		logger.Info("dragonfly rollout: a planned failover owns Dragonfly; holding the in-place restart", "site", site.Name, "fg", nn)
		return dragonflyRolloutRequeue, nil
	}
	master := effectiveDragonflyMasterSite(fresh)
	logger.Info("dragonfly rollout: restarting site in place at the administrator's request",
		"site", site.Name, "statusMaster", master, "fg", nn)
	if err := r.rollDragonflyStatefulSetTemplate(ctx, fg, site); err != nil {
		return 0, fmt.Errorf("reconcile dragonfly statefulset %s in place: %w", site.Name, err)
	}
	if r.Recorder != nil {
		r.Recorder.Eventf(fg, corev1.EventTypeWarning, v1alpha1.EventDragonflyRestartedInPlace,
			"the pod template of Dragonfly site %q was applied in place because the %s annotation names it (status names %q as the Dragonfly master); if the site held the master, its cache is lost",
			site.Name, v1alpha1.DragonflyRestartInPlaceAnnotation, master)
	}
	if _, err := r.removeDragonflyRestartInPlace(ctx, nn, site.Name); err != nil {
		return 0, fmt.Errorf("remove the %s annotation: %w", v1alpha1.DragonflyRestartInPlaceAnnotation, err)
	}
	return dragonflyRolloutRequeue, nil
}

// noteDragonflyRolloutWait makes a rollout that waits visible without a
// line every pass: the state is logged when it changes, with the detail,
// and a wait that has lasted dragonflyRolloutStallAfter is reported once in
// a Warning Event (DragonflyRolloutStalled). The zero wait ends the record.
func (r *MysqlFailoverGroupReconciler) noteDragonflyRolloutWait(ctx context.Context, fg *v1alpha1.MysqlFailoverGroup, wait dragonflyRolloutWait) {
	nn := types.NamespacedName{Namespace: fg.Namespace, Name: fg.Name}
	now := r.now()
	r.dragonflyRolloutMu.Lock()
	st := r.dragonflyRolloutWaits[nn]
	was := st.state
	if wait.State == "" {
		delete(r.dragonflyRolloutWaits, nn)
		r.dragonflyRolloutMu.Unlock()
		if was != "" {
			log.FromContext(ctx).Info("dragonfly rollout: no longer waiting", "state", was, "fg", nn)
		}
		return
	}
	if was != wait.State {
		st = dragonflyRolloutWaitState{state: wait.State, since: now}
	}
	waited := now.Sub(st.since)
	report := !st.reported && waited >= dragonflyRolloutStallAfter
	if report {
		st.reported = true
	}
	if r.dragonflyRolloutWaits == nil {
		r.dragonflyRolloutWaits = make(map[types.NamespacedName]dragonflyRolloutWaitState)
	}
	r.dragonflyRolloutWaits[nn] = st
	r.dragonflyRolloutMu.Unlock()

	logger := log.FromContext(ctx)
	if was != wait.State {
		logger.Info("dragonfly rollout: waiting", "state", wait.State, "detail", wait.Detail, "fg", nn)
	}
	if report {
		logger.Info("dragonfly rollout: stalled", "state", wait.State, "detail", wait.Detail,
			"waited", waited.Truncate(time.Second).String(), "fg", nn)
		if r.Recorder != nil {
			r.Recorder.Eventf(fg, corev1.EventTypeWarning, v1alpha1.EventDragonflyRolloutStalled,
				"the Dragonfly template rollout has waited %s and changes no StatefulSet until this clears (%s): %s",
				waited.Truncate(time.Second), wait.State, wait.Detail)
		}
	}
}
