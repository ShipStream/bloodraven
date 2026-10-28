package controller

import (
	"context"
	"fmt"
	"slices"
	"strings"
	"time"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	k8sretry "k8s.io/client-go/util/retry"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/log"

	v1alpha1 "github.com/shipstream/bloodraven/api/v1alpha1"
)

// The restore-owned reclone queue (status.restoreReclones).
//
// A full-instance in-place restore replaces the primary's data, so every
// peer must be recloned from it afterwards. The restore fixes that set of
// peers when it is accepted (inPlacePreflight, before Fencing) and
// releases it when the load succeeds (inPlaceResuming). The reconciler
// then requests one reclone at a time through the reclone annotation,
// which the topology manager consumes (it runs one clone at a time). The
// runner removes a site from the queue only when its reclone succeeded and
// was started for this queue (RecloneOutcome.Owner); a failed attempt is
// retried after restoreRecloneRetryBackoff. Queued sites are excluded from
// automatic promotion and removed from the -replica and read-only Services
// until they leave the queue. A Held queue (settleArmedRestoreReclones)
// keeps its sites excluded but clones nothing.

const (
	// restoreRecloneRetryBackoff spaces out attempts after a failed or
	// lost reclone so a persistent failure does not clone in a loop.
	restoreRecloneRetryBackoff = time.Minute
	// restoreRecloneWaitRequeue is how often the drain re-checks while a
	// reclone runs or an administrator's reclone request is pending.
	restoreRecloneWaitRequeue = 30 * time.Second
)

// restoreRecloneRecipients returns every site except target in the order
// the queue reclones them: primary-candidates first (sitePriorities
// order, then declared order), then dr-only followers, then read-only
// readers. Promotable copies are restored first because they are the
// ones a failover could otherwise land on.
func restoreRecloneRecipients(fg *v1alpha1.MysqlFailoverGroup, target string) []string {
	var priorities []string
	if fg.Spec.SplitBrainPolicy != nil {
		priorities = fg.Spec.SplitBrainPolicy.SitePriorities
	}
	rank := func(s v1alpha1.SiteSpec) int {
		switch {
		case s.IsPromotable():
			return 0
		case s.IsReadOnlyReader():
			return 2
		default:
			return 1
		}
	}
	var out []string
	for _, tier := range []int{0, 1, 2} {
		if tier == 0 {
			for _, name := range priorities {
				for _, s := range fg.Spec.Sites {
					if s.Name == name && s.Name != target && rank(s) == 0 && !slices.Contains(out, name) {
						out = append(out, name)
					}
				}
			}
		}
		for _, s := range fg.Spec.Sites {
			if s.Name != target && rank(s) == tier && !slices.Contains(out, s.Name) {
				out = append(out, s.Name)
			}
		}
	}
	return out
}

// restoreReclonePendingSites returns the sites the restore queue has not
// recloned yet. They are excluded from automatic promotion and removed
// from the -replica and read-only Services.
func restoreReclonePendingSites(fg *v1alpha1.MysqlFailoverGroup) []string {
	if fg == nil || fg.Status.RestoreReclones == nil {
		return nil
	}
	return append([]string(nil), fg.Status.RestoreReclones.Pending...)
}

// restoreRecloneBlockedSites returns the pending sites no reclone may
// start for: the queue is Armed (its restore is running and will reclone
// them afterwards) or Held (the primary is no trusted donor).
func restoreRecloneBlockedSites(fg *v1alpha1.MysqlFailoverGroup) []string {
	if fg == nil || fg.Status.RestoreReclones == nil || fg.Status.RestoreReclones.Phase == v1alpha1.RestoreReclonesDraining {
		return nil
	}
	return append([]string(nil), fg.Status.RestoreReclones.Pending...)
}

// specDeclaresSite reports whether spec.sites names site.
func specDeclaresSite(fg *v1alpha1.MysqlFailoverGroup, site string) bool {
	for _, s := range fg.Spec.Sites {
		if s.Name == site {
			return true
		}
	}
	return false
}

// restoreReclonePending reports whether site is still queued for a
// post-restore reclone.
func restoreReclonePending(fg *v1alpha1.MysqlFailoverGroup, site string) bool {
	return fg != nil && fg.Status.RestoreReclones != nil && slices.Contains(fg.Status.RestoreReclones.Pending, site)
}

// mutateRestoreReclones applies mutate to a fresh copy of
// status.restoreReclones and writes it when mutate reports a change. The
// conflict retry re-runs mutate against the latest object, so decisions
// made inside it never act on a stale cache view.
func (r *MysqlFailoverGroupReconciler) mutateRestoreReclones(ctx context.Context, fg *v1alpha1.MysqlFailoverGroup, mutate func(fresh *v1alpha1.MysqlFailoverGroup) bool) (bool, error) {
	changed := false
	err := k8sretry.RetryOnConflict(k8sretry.DefaultRetry, func() error {
		fresh := &v1alpha1.MysqlFailoverGroup{}
		if err := r.Get(ctx, client.ObjectKeyFromObject(fg), fresh); err != nil {
			return err
		}
		changed = mutate(fresh)
		if !changed {
			*fg = *fresh
			return nil
		}
		if err := r.Status().Update(ctx, fresh); err != nil {
			return err
		}
		*fg = *fresh
		return nil
	})
	if err != nil {
		return false, err
	}
	if changed && r.Runner != nil {
		r.Runner.SetRestoreRecloneBlocked(client.ObjectKeyFromObject(fg), restoreRecloneBlockedSites(fg))
		r.Runner.SetRestoreReclonePending(client.ObjectKeyFromObject(fg), restoreReclonePendingSites(fg))
	}
	return changed, nil
}

// restoreReclonesReadyForNewRestore prepares the reclone queue for a newly
// accepted full-instance restore, before armRestoreReclones replaces it.
// It returns a wait message while that restore must not proceed yet.
//
//   - A clone running now (any site, any source) must finish first: the
//     new restore would drop and reload its donor under it.
//   - The queue's own outstanding request that has not started is
//     withdrawn (its annotation removed, the topology manager's pending
//     request cancelled) while the queue still owns it. Otherwise it
//     would survive the new restore and later clone from its primary,
//     even if that restore failed and the queue was put back Held.
func (r *MysqlFailoverGroupReconciler) restoreReclonesReadyForNewRestore(ctx context.Context, fg *v1alpha1.MysqlFailoverGroup) (string, error) {
	nn := client.ObjectKeyFromObject(fg)
	if r.Runner != nil {
		if site, known := r.Runner.CloneInProgress(nn); known && site != "" {
			return fmt.Sprintf("waiting for the running clone of %s to finish before restoring", site), nil
		}
	}
	q := fg.Status.RestoreReclones
	if q == nil || q.Requested == "" {
		return "", nil
	}
	requested := q.Requested
	if err := r.removeOwnRecloneAnnotation(ctx, fg, requested); err != nil {
		return "", err
	}
	if r.Runner != nil {
		r.Runner.CancelPendingReclone(nn, requested)
		// The clone may have started between the check above and the
		// cancel; the next pass waits for it.
		if site, known := r.Runner.CloneInProgress(nn); known && site != "" {
			return fmt.Sprintf("waiting for the running clone of %s to finish before restoring", site), nil
		}
	}
	if _, err := r.mutateRestoreReclones(ctx, fg, func(fresh *v1alpha1.MysqlFailoverGroup) bool {
		fq := fresh.Status.RestoreReclones
		if fq == nil || fq.Requested != requested {
			return false
		}
		fq.Requested, fq.RequestedAt = "", nil
		return true
	}); err != nil {
		return "", err
	}
	r.Recorder.Eventf(fg, corev1.EventTypeNormal, "RestoreRecloneWithdrawn",
		"withdrew the not yet started reclone of %s (in-place restore confirm=%s) before a new full-instance restore", requested, q.ConfirmToken)
	return "", nil
}

// armRestoreReclones records the peers an accepted full-instance restore
// of target must reclone. It replaces any older queue: the new restore
// overwrites the primary again, so every peer needs the new data. An
// older queue that was still draining (or held) is kept in Superseded
// until the new restore succeeds, so a new restore that is cancelled or
// fails cannot release peers the older one never recloned.
func (r *MysqlFailoverGroupReconciler) armRestoreReclones(ctx context.Context, fg *v1alpha1.MysqlFailoverGroup, target, confirm string) error {
	recipients := restoreRecloneRecipients(fg, target)
	_, err := r.mutateRestoreReclones(ctx, fg, func(fresh *v1alpha1.MysqlFailoverGroup) bool {
		old := fresh.Status.RestoreReclones
		if len(recipients) == 0 {
			if old == nil {
				return false
			}
			fresh.Status.RestoreReclones = nil
			return true
		}
		next := &v1alpha1.RestoreReclonesStatus{
			ConfirmToken: confirm,
			Phase:        v1alpha1.RestoreReclonesArmed,
			Pending:      recipients,
		}
		switch {
		case old != nil && old.ConfirmToken == confirm:
			next.Superseded = old.Superseded
		case old != nil && (old.Phase == v1alpha1.RestoreReclonesDraining || old.Phase == v1alpha1.RestoreReclonesHeld) && len(old.Pending) > 0:
			next.Superseded = &v1alpha1.RestoreReclonesCarryover{
				ConfirmToken: old.ConfirmToken,
				Pending:      append([]string(nil), old.Pending...),
				Held:         old.Phase == v1alpha1.RestoreReclonesHeld,
			}
		case old != nil:
			next.Superseded = old.Superseded
		}
		fresh.Status.RestoreReclones = next
		return true
	})
	return err
}

// releaseRestoreReclones moves the queue of a successful full-instance
// restore to Draining. A restore accepted by an operator that predates the
// queue has none yet; it gets one here so its peers are still recloned.
// The superseded queue is dropped: this restore's queue covers every peer.
func (r *MysqlFailoverGroupReconciler) releaseRestoreReclones(ctx context.Context, fg *v1alpha1.MysqlFailoverGroup, target, confirm string) error {
	recipients := restoreRecloneRecipients(fg, target)
	_, err := r.mutateRestoreReclones(ctx, fg, func(fresh *v1alpha1.MysqlFailoverGroup) bool {
		q := fresh.Status.RestoreReclones
		switch {
		case q != nil && q.Phase == v1alpha1.RestoreReclonesDraining:
			return false
		case q != nil:
			q.Phase = v1alpha1.RestoreReclonesDraining
			q.Superseded = nil
			return true
		case len(recipients) == 0:
			return false
		default:
			fresh.Status.RestoreReclones = &v1alpha1.RestoreReclonesStatus{
				ConfirmToken: confirm,
				Phase:        v1alpha1.RestoreReclonesDraining,
				Pending:      recipients,
			}
			return true
		}
	})
	return err
}

// setRecloneFailure records site's latest failed attempt.
func setRecloneFailure(q *v1alpha1.RestoreReclonesStatus, site, msg string, now metav1.Time) {
	for i := range q.Failures {
		if q.Failures[i].Site == site {
			q.Failures[i].Message, q.Failures[i].Time = msg, now
			return
		}
	}
	q.Failures = append(q.Failures, v1alpha1.RestoreRecloneFailure{Site: site, Message: msg, Time: now})
}

// clearRecloneFailure forgets site's failure (it was recloned or left the
// queue).
func clearRecloneFailure(q *v1alpha1.RestoreReclonesStatus, site string) {
	q.Failures = slices.DeleteFunc(q.Failures, func(f v1alpha1.RestoreRecloneFailure) bool { return f.Site == site })
}

// recloneBackoff returns how long site must still wait after its last
// failed attempt (0 when it may be requested now).
func recloneBackoff(q *v1alpha1.RestoreReclonesStatus, site string, now time.Time) time.Duration {
	for _, f := range q.Failures {
		if f.Site == site {
			if wait := restoreRecloneRetryBackoff - now.Sub(f.Time.Time); wait > 0 {
				return wait
			}
		}
	}
	return 0
}

// nextRestoreReclone picks the first pending site, in queue order, that is
// not backing off after a failure, so one site that cannot be cloned does
// not hold up the others. With every site backing off it returns "" and
// the shortest remaining wait.
func nextRestoreReclone(q *v1alpha1.RestoreReclonesStatus, pending []string, now time.Time) (string, time.Duration) {
	var wait time.Duration
	for _, site := range pending {
		w := recloneBackoff(q, site, now)
		if w == 0 {
			return site, 0
		}
		if wait == 0 || w < wait {
			wait = w
		}
	}
	return "", wait
}

// reconcileRestoreReclones advances status.restoreReclones by at most one
// step. It returns the requeue interval the drain needs (0 when idle).
func (r *MysqlFailoverGroupReconciler) reconcileRestoreReclones(ctx context.Context, fg *v1alpha1.MysqlFailoverGroup) (time.Duration, error) {
	q := fg.Status.RestoreReclones
	if q == nil {
		return 0, nil
	}
	if q.Phase != v1alpha1.RestoreReclonesDraining && q.Phase != v1alpha1.RestoreReclonesHeld {
		return 0, r.settleArmedRestoreReclones(ctx, fg)
	}

	known := make([]string, 0, len(q.Pending))
	for _, site := range q.Pending {
		if specDeclaresSite(fg, site) {
			known = append(known, site)
		}
	}
	if len(known) > 0 && len(known) != len(q.Pending) {
		// A queued site was removed from spec.sites; drop it so it cannot
		// hold up the rest of the queue.
		if _, err := r.mutateRestoreReclones(ctx, fg, func(fresh *v1alpha1.MysqlFailoverGroup) bool {
			fq := fresh.Status.RestoreReclones
			if fq == nil {
				return false
			}
			before := len(fq.Pending)
			fq.Pending = slices.DeleteFunc(fq.Pending, func(site string) bool { return !specDeclaresSite(fresh, site) })
			fq.Failures = slices.DeleteFunc(fq.Failures, func(f v1alpha1.RestoreRecloneFailure) bool { return !slices.Contains(fq.Pending, f.Site) })
			if fq.Requested != "" && !slices.Contains(fq.Pending, fq.Requested) {
				fq.Requested, fq.RequestedAt = "", nil
			}
			return len(fq.Pending) != before
		}); err != nil {
			return 0, err
		}
		if fg.Status.RestoreReclones == nil {
			return 0, nil
		}
		q = fg.Status.RestoreReclones
	}
	if len(known) == 0 {
		if q.Phase == v1alpha1.RestoreReclonesHeld {
			r.clearRestoreReclones(ctx, fg, corev1.EventTypeNormal, "RestoreReclonesComplete",
				"no site of the held reclone queue (confirm=%s) is left in spec.sites", q.ConfirmToken)
			return 0, nil
		}
		r.clearRestoreReclones(ctx, fg, corev1.EventTypeNormal, "RestoreReclonesComplete",
			"every peer of the in-place restore (confirm=%s) has been recloned", q.ConfirmToken)
		return 0, nil
	}
	active := fg.Status.ActiveSite
	if active == "" {
		return restoreRecloneWaitRequeue, nil
	}
	if slices.Contains(known, active) {
		// Something other than this queue (a planned failover, a manual
		// promotion) made a site with pre-restore data the primary.
		// Cloning the rest from it would spread that data, so stop.
		r.clearRestoreReclones(ctx, fg, corev1.EventTypeWarning, "RestoreReclonesAbandoned",
			"site %s became the primary before the in-place restore recloned it; it keeps its pre-restore data and the remaining reclone queue (%s) is abandoned",
			active, strings.Join(known, ", "))
		return 0, nil
	}
	if q.Phase == v1alpha1.RestoreReclonesHeld {
		// The primary is no trusted donor (settleArmedRestoreReclones);
		// the sites wait, excluded, for an administrator's decision.
		return 0, nil
	}
	raw := fg.GetAnnotations()[RecloneAnnotation]
	nn := client.ObjectKeyFromObject(fg)

	if q.Requested != "" {
		requested := q.Requested
		if raw != "" && parseRecloneAnnotation(raw).Site == requested {
			if !r.restoreRecloneStalled(fg, q) {
				return restoreRecloneWaitRequeue, nil
			}
			// The clone never started (unreachable recipient, keyring
			// unseal never ready, no confirmed donor, ...). Give the slot
			// to the next site; this one retries after the backoff.
			msg := fmt.Sprintf("the reclone of %s did not start within %s", requested, restoreRecloneStartTimeout)
			if err := r.failRestoreRecloneRequest(ctx, fg, requested, msg); err != nil {
				return 0, err
			}
			if err := r.removeOwnRecloneAnnotation(ctx, fg, requested); err != nil {
				return 0, err
			}
			if r.Runner != nil {
				r.Runner.CancelPendingReclone(nn, requested)
			}
			r.Recorder.Eventf(fg, corev1.EventTypeWarning, "RestoreRecloneFailed",
				"%s; trying the other queued sites, and %s again in %s", msg, requested, restoreRecloneRetryBackoff)
			return time.Second, nil
		}
		// Our request disappeared without the runner recording an
		// outcome (the annotation was removed by hand, or the site left
		// the spec). Re-request after the backoff.
		if err := r.failRestoreRecloneRequest(ctx, fg, requested,
			fmt.Sprintf("the reclone request for %s was removed before the clone finished", requested)); err != nil {
			return 0, err
		}
		return time.Second, nil
	}

	if raw != "" {
		// Never overwrite another reclone request (an administrator's,
		// or one for a site not in this queue); wait for it to resolve.
		log.FromContext(ctx).Info("restore reclone queue waiting for another reclone request",
			"fg", fg.Name, "pending", strings.Join(known, ","), "annotation", raw)
		return restoreRecloneWaitRequeue, nil
	}
	if plannedFailoverInFlight(fg.Status.PlannedFailover) {
		// A planned failover may be about to make a queued site the
		// primary; a clone requested now could copy that site's stale data
		// before Resuming rewrites status.activeSite.
		return restoreRecloneWaitRequeue, nil
	}
	head, wait := nextRestoreReclone(q, known, time.Now())
	if head == "" {
		return wait, nil
	}

	value := recloneAnnotationValue(fg, head)
	if err := validateRecloneRequest(fg, parseRecloneAnnotation(value)); err != nil {
		return 0, fmt.Errorf("restore reclone request %q for %s does not pass the reclone interlock: %w", value, head, err)
	}
	claimed, err := r.mutateRestoreReclones(ctx, fg, func(fresh *v1alpha1.MysqlFailoverGroup) bool {
		fq := fresh.Status.RestoreReclones
		if fq == nil || fq.Phase != v1alpha1.RestoreReclonesDraining || fq.Requested != "" ||
			!slices.Contains(fq.Pending, head) || fresh.GetAnnotations()[RecloneAnnotation] != "" {
			return false
		}
		now := metav1.Now()
		fq.Requested, fq.RequestedAt = head, &now
		return true
	})
	if err != nil || !claimed {
		return restoreRecloneWaitRequeue, err
	}
	// Requested is recorded before the annotation exists, so any object
	// version carrying the annotation also names this queue as its owner.
	written, werr := r.writeRecloneAnnotation(ctx, fg, value)
	if werr != nil || !written {
		if werr != nil {
			if err := r.failRestoreRecloneRequest(ctx, fg, head,
				fmt.Sprintf("could not write the reclone request for %s: %v", head, werr)); err != nil {
				return 0, err
			}
			return restoreRecloneWaitRequeue, werr
		}
		// Another reclone request appeared first: withdraw ours.
		if _, err := r.mutateRestoreReclones(ctx, fg, func(fresh *v1alpha1.MysqlFailoverGroup) bool {
			fq := fresh.Status.RestoreReclones
			if fq == nil || fq.Requested != head {
				return false
			}
			fq.Requested, fq.RequestedAt = "", nil
			return true
		}); err != nil {
			return 0, err
		}
		return restoreRecloneWaitRequeue, nil
	}
	r.Recorder.Eventf(fg, corev1.EventTypeNormal, "RestoreRecloneRequested",
		"requested reclone of %s from restored primary %s (in-place restore confirm=%s; %d site(s) queued)",
		head, active, q.ConfirmToken, len(known))
	if r.Runner != nil {
		r.Runner.ApplyRecloneRequest(ctx, fg)
	}
	return restoreRecloneWaitRequeue, nil
}

// restoreRecloneStartTimeout bounds how long a requested reclone may go
// without its clone starting before the queue moves on.
const restoreRecloneStartTimeout = 10 * time.Minute

// restoreRecloneStalled reports whether q's requested reclone has been
// waiting longer than restoreRecloneStartTimeout without the topology
// manager starting it. With no manager to ask it is never stalled.
//
// The wait is measured from when this process first saw the request not
// running, not from the durable requestedAt: after an operator restart a
// clone that ran for an hour looks unstarted until the new topology
// manager picks the annotation up again, and must get the full timeout.
// Seeing the clone running resets the wait.
func (r *MysqlFailoverGroupReconciler) restoreRecloneStalled(fg *v1alpha1.MysqlFailoverGroup, q *v1alpha1.RestoreReclonesStatus) bool {
	if q.RequestedAt == nil || time.Since(q.RequestedAt.Time) < restoreRecloneStartTimeout || r.Runner == nil {
		return false
	}
	nn := client.ObjectKeyFromObject(fg)
	inProgress, known := r.Runner.RecloneInProgress(nn, q.Requested)
	if !known {
		return false
	}
	key := restoreRecloneWait{site: q.Requested, requestedAt: q.RequestedAt.Time}
	r.restoreRecloneMu.Lock()
	defer r.restoreRecloneMu.Unlock()
	if inProgress {
		delete(r.restoreRecloneIdle, nn)
		return false
	}
	if r.restoreRecloneIdle == nil {
		r.restoreRecloneIdle = map[types.NamespacedName]restoreRecloneWait{}
	}
	idle, ok := r.restoreRecloneIdle[nn]
	if !ok || idle.site != key.site || !idle.requestedAt.Equal(key.requestedAt) {
		key.since = time.Now()
		r.restoreRecloneIdle[nn] = key
		return false
	}
	return time.Since(idle.since) >= restoreRecloneStartTimeout
}

// restoreRecloneWait is when this process first saw a queue request
// (site, requestedAt) not running.
type restoreRecloneWait struct {
	site        string
	requestedAt time.Time
	since       time.Time
}

// failRestoreRecloneRequest ends the queue's outstanding request for site
// as a failed attempt.
func (r *MysqlFailoverGroupReconciler) failRestoreRecloneRequest(ctx context.Context, fg *v1alpha1.MysqlFailoverGroup, site, msg string) error {
	_, err := r.mutateRestoreReclones(ctx, fg, func(fresh *v1alpha1.MysqlFailoverGroup) bool {
		fq := fresh.Status.RestoreReclones
		if fq == nil || fq.Requested != site {
			return false
		}
		fq.Requested, fq.RequestedAt = "", nil
		setRecloneFailure(fq, site, msg, metav1.Now())
		return true
	})
	return err
}

// removeOwnRecloneAnnotation removes the reclone annotation if it still
// carries the queue's own request for site.
func (r *MysqlFailoverGroupReconciler) removeOwnRecloneAnnotation(ctx context.Context, fg *v1alpha1.MysqlFailoverGroup, site string) error {
	return k8sretry.RetryOnConflict(k8sretry.DefaultRetry, func() error {
		var fresh v1alpha1.MysqlFailoverGroup
		if err := r.Get(ctx, client.ObjectKeyFromObject(fg), &fresh); err != nil {
			return err
		}
		annotations := fresh.GetAnnotations()
		if annotations[RecloneAnnotation] != recloneAnnotationValue(&fresh, site) {
			return nil
		}
		delete(annotations, RecloneAnnotation)
		fresh.SetAnnotations(annotations)
		if err := r.Update(ctx, &fresh); err != nil {
			return err
		}
		*fg = fresh
		return nil
	})
}

// settleArmedRestoreReclones keeps an Armed queue while its restore runs
// and settles it once that restore can no longer succeed. Nothing is ever
// cloned from a primary whose restore failed.
//
// If the restore replaced an earlier restore's queue, that queue is put
// back: its sites still hold data from before the earlier restore. It
// drains again only when the failed restore provably changed nothing on
// the primary (no Job was created: cancelled, rejected, or the Job could
// not be built). Once its Job was created the restore may have dropped
// or half-loaded the primary, so the earlier queue is put back Held: its
// sites stay excluded, and may be the only intact copies, until an
// administrator decides.
func (r *MysqlFailoverGroupReconciler) settleArmedRestoreReclones(ctx context.Context, fg *v1alpha1.MysqlFailoverGroup) error {
	rip := fg.Status.RestoreInPlace
	if rip != nil {
		switch rip.Phase {
		case v1alpha1.RestoreInPlacePreflight, v1alpha1.RestoreInPlaceFencing,
			v1alpha1.RestoreInPlaceRestoring, v1alpha1.RestoreInPlaceResuming:
			return nil
		}
	}
	q := fg.Status.RestoreReclones
	if rip != nil && rip.Phase == v1alpha1.RestoreInPlaceSucceeded && rip.ConfirmTokenUsed == q.ConfirmToken {
		// The queue's own restore succeeded (an Armed copy written back
		// after the release, or a release lost to a crash): release it
		// rather than discard it, so its peers are still recloned.
		return r.releaseRestoreReclones(ctx, fg, rip.TargetSite, q.ConfirmToken)
	}
	if sup := q.Superseded; sup != nil && len(sup.Pending) > 0 {
		// Without a status the run's Job cannot be ruled out.
		executed := rip == nil || rip.JobName != ""
		phase := v1alpha1.RestoreReclonesDraining
		if executed || sup.Held {
			phase = v1alpha1.RestoreReclonesHeld
		}
		restored, err := r.mutateRestoreReclones(ctx, fg, func(fresh *v1alpha1.MysqlFailoverGroup) bool {
			fq := fresh.Status.RestoreReclones
			if fq == nil || fq.Phase != v1alpha1.RestoreReclonesArmed || fq.ConfirmToken != q.ConfirmToken {
				return false
			}
			fresh.Status.RestoreReclones = &v1alpha1.RestoreReclonesStatus{
				ConfirmToken: sup.ConfirmToken,
				Phase:        phase,
				Pending:      append([]string(nil), sup.Pending...),
			}
			return true
		})
		if err != nil {
			return err
		}
		switch {
		case !restored:
		case phase == v1alpha1.RestoreReclonesDraining:
			r.Recorder.Eventf(fg, corev1.EventTypeNormal, "RestoreReclonesRestored",
				"the in-place restore (confirm=%s) ended before its Job ran, so the primary is unchanged; "+
					"the earlier restore's reclone queue (confirm=%s: %s) resumes",
				q.ConfirmToken, sup.ConfirmToken, strings.Join(sup.Pending, ", "))
		default:
			r.Recorder.Eventf(fg, corev1.EventTypeWarning, "RestoreReclonesHeld",
				"the in-place restore (confirm=%s) ran on the primary and did not succeed, so the primary no longer holds the earlier restore's data. "+
					"%s still hold data from before that restore (confirm=%s) and are not recloned from this primary; they stay excluded from promotion "+
					"and the reader Services. Restore the primary and set status.restoreReclones.phase to Draining, run a new full-instance restore, "+
					"or accept their data by removing status.restoreReclones",
				q.ConfirmToken, strings.Join(sup.Pending, ", "), sup.ConfirmToken)
		}
		return nil
	}
	r.clearRestoreReclones(ctx, fg, corev1.EventTypeWarning, "RestoreReclonesDiscarded",
		"the in-place restore (confirm=%s) did not succeed; %s are not recloned from its primary and are eligible for promotion again. "+
			"They may have replicated the restore's DROP statements; check their data before relying on them",
		q.ConfirmToken, strings.Join(q.Pending, ", "))
	return nil
}

// clearRestoreReclones removes the queue and records why.
func (r *MysqlFailoverGroupReconciler) clearRestoreReclones(ctx context.Context, fg *v1alpha1.MysqlFailoverGroup, eventType, reason, format string, args ...any) {
	cleared, err := r.mutateRestoreReclones(ctx, fg, func(fresh *v1alpha1.MysqlFailoverGroup) bool {
		if fresh.Status.RestoreReclones == nil {
			return false
		}
		fresh.Status.RestoreReclones = nil
		return true
	})
	if err != nil {
		log.FromContext(ctx).Error(err, "clear restore reclone queue", "fg", fg.Name)
		return
	}
	if cleared {
		r.Recorder.Eventf(fg, eventType, reason, format, args...)
	}
}

// writeRecloneAnnotation sets the reclone annotation to value unless some
// request is already there. It reports whether it wrote.
func (r *MysqlFailoverGroupReconciler) writeRecloneAnnotation(ctx context.Context, fg *v1alpha1.MysqlFailoverGroup, value string) (bool, error) {
	written := false
	err := k8sretry.RetryOnConflict(k8sretry.DefaultRetry, func() error {
		var fresh v1alpha1.MysqlFailoverGroup
		if err := r.Get(ctx, client.ObjectKeyFromObject(fg), &fresh); err != nil {
			return err
		}
		annotations := fresh.GetAnnotations()
		if annotations[RecloneAnnotation] != "" {
			written = false
			return nil
		}
		if annotations == nil {
			annotations = map[string]string{}
		}
		annotations[RecloneAnnotation] = value
		fresh.SetAnnotations(annotations)
		if err := r.Update(ctx, &fresh); err != nil {
			return err
		}
		written = true
		*fg = fresh
		return nil
	})
	return written, err
}

// recordRestoreRecloneOutcome applies a finished reclone to the restore
// queue before the reclone annotation is removed. Only a clone started
// for this queue (outcome.Owner) can complete a site; a success leaves the
// queue, a failure stays queued and is retried after the backoff while the
// other queued sites go first.
func (r *TopologyManagerRunner) recordRestoreRecloneOutcome(ctx context.Context, nn types.NamespacedName, site string, outcome RecloneOutcome) error {
	var (
		event     func(fg *v1alpha1.MysqlFailoverGroup)
		remaining []string
		updated   bool
	)
	err := k8sretry.RetryOnConflict(k8sretry.DefaultRetry, func() error {
		event, updated = nil, false
		var fresh v1alpha1.MysqlFailoverGroup
		if err := r.client.Get(ctx, nn, &fresh); err != nil {
			return err
		}
		q := fresh.Status.RestoreReclones
		if q == nil || q.Requested != site {
			return nil
		}
		q.Requested, q.RequestedAt = "", nil
		switch {
		case outcome.Owner != q.ConfirmToken:
			// A clone this queue did not start used up its request (an
			// administrator's reclone of the same site was already
			// running). Request again; that clone may predate the restore.
		case outcome.Err == nil:
			q.Pending = slices.DeleteFunc(q.Pending, func(s string) bool { return s == site })
			clearRecloneFailure(q, site)
			left := len(q.Pending)
			confirm := q.ConfirmToken
			if left == 0 {
				fresh.Status.RestoreReclones = nil
			}
			event = func(fg *v1alpha1.MysqlFailoverGroup) {
				r.recorder.Eventf(fg, corev1.EventTypeNormal, "RestoreRecloneSucceeded",
					"recloned %s from the restored primary (%d site(s) still queued)", site, left)
				if left == 0 {
					r.recorder.Eventf(fg, corev1.EventTypeNormal, "RestoreReclonesComplete",
						"every peer of the in-place restore (confirm=%s) has been recloned", confirm)
				}
			}
		default:
			msg := fmt.Sprintf("reclone of %s failed: %v", site, outcome.Err)
			setRecloneFailure(q, site, msg, metav1.Now())
			event = func(fg *v1alpha1.MysqlFailoverGroup) {
				r.recorder.Eventf(fg, corev1.EventTypeWarning, "RestoreRecloneFailed",
					"%s; trying the other queued sites, and %s again in %s", msg, site, restoreRecloneRetryBackoff)
			}
		}
		if err := r.client.Status().Update(ctx, &fresh); err != nil {
			return err
		}
		updated = true
		remaining = restoreReclonePendingSites(&fresh)
		if event != nil && r.recorder != nil {
			event(&fresh)
		}
		return nil
	})
	if err != nil {
		if apierrors.IsNotFound(err) {
			return nil
		}
		return err
	}
	if updated {
		r.SetRestoreReclonePending(nn, remaining)
	}
	return nil
}
