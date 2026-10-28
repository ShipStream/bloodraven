package controller

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/types"
	k8sretry "k8s.io/client-go/util/retry"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/log"

	v1alpha1 "github.com/shipstream/bloodraven/api/v1alpha1"
	"github.com/shipstream/bloodraven/internal/mysql"
)

// The database fence of a full-instance in-place restore.
//
// Stripping the primary role label only stops new connections through the
// -primary Service. Pooled connections survive it, and clients of the
// per-site Service or spec.dns.hostname never pass through it. So the
// restore also sets @@GLOBAL.offline_mode=ON on the target: MySQL then
// disconnects and refuses every session without CONNECTION_ADMIN or SUPER.
// The operator, the sidecar and the restore Job all use the operator
// account, which preflight requires to hold CONNECTION_ADMIN.
//
// Lifecycle (status.restoreOfflineFence, written before offline_mode is
// touched):
//   - Preflight checks the privileges; a missing one rejects the restore
//     without consuming its confirm.
//   - Fencing records the fence, then the prior offline_mode, then enables
//     the fence, evicts the sessions that predate it, and only then lets the
//     restore create its Job.
//   - The topology manager re-asserts offline_mode=ON on every poll while
//     the restore runs (a restarted mysqld comes back with it OFF).
//   - Once the restore is Succeeded or Failed, the reconciler restores the
//     prior value and clears the record.

// errInPlaceRunEnded means enforceRestoreOfflineFence ended the run (it
// stamped a terminal status); the caller stops.
var errInPlaceRunEnded = errors.New("in-place restore run ended")

// errRestoreFenceUnsupported means the site's checker cannot apply the
// database fence (test doubles that do not implement mysql.RestoreFencer).
var errRestoreFenceUnsupported = errors.New("the site's MySQL checker cannot apply the restore offline_mode fence")

// errRestoreFenceNoPrivilege means the operator account lacks one of
// mysql.RestoreFencePrivileges. Without CONNECTION_ADMIN offline_mode would
// lock it out; without the others the fence or the restore Job stalls or
// fails part-way.
var errRestoreFenceNoPrivilege = errors.New("the operator MySQL account lacks")

func (tm *TopologyManager) restoreFencer(site string) (mysql.RestoreFencer, error) {
	s := tm.getSite(site)
	if s == nil {
		return nil, fmt.Errorf("unknown site %q", site)
	}
	f, ok := s.mysql.(mysql.RestoreFencer)
	if !ok {
		return nil, errRestoreFenceUnsupported
	}
	return f, nil
}

// RestoreFencePrivileges returns nil when the operator account holds every
// privilege the full-instance fence needs on site, an error wrapping
// errRestoreFenceNoPrivilege that names the missing ones when it does not,
// and another error when that could not be determined.
func (tm *TopologyManager) RestoreFencePrivileges(ctx context.Context, site string) error {
	f, err := tm.restoreFencer(site)
	if err != nil {
		return err
	}
	var missing []string
	err = withMySQLSafetyTimeout(ctx, func(c context.Context) error {
		var e error
		missing, e = f.MissingRestoreFencePrivileges(c)
		return e
	})
	if err != nil {
		return err
	}
	if len(missing) > 0 {
		return fmt.Errorf("%w %s", errRestoreFenceNoPrivilege, strings.Join(missing, ", "))
	}
	return nil
}

// RestoreOfflineMode reads @@GLOBAL.offline_mode on site.
func (tm *TopologyManager) RestoreOfflineMode(ctx context.Context, site string) (bool, error) {
	f, err := tm.restoreFencer(site)
	if err != nil {
		return false, err
	}
	var on bool
	err = withMySQLSafetyTimeout(ctx, func(c context.Context) error {
		var e error
		on, e = f.GetOfflineMode(c)
		return e
	})
	return on, err
}

// EnforceRestoreOfflineFence enables offline_mode on site for the restore
// run owner (fenceOwner), keeps re-asserting it on every poll, evicts the
// client sessions that predate the fence, and returns how many of them
// are still connected, and the users of the SYSTEM_USER sessions the
// operator account may not kill.
func (tm *TopologyManager) EnforceRestoreOfflineFence(ctx context.Context, site, owner string) (int, []string, error) {
	f, err := tm.restoreFencer(site)
	if err != nil {
		return 0, nil, err
	}
	tm.restoreFenceMu.Lock()
	defer tm.restoreFenceMu.Unlock()
	if owner == tm.restoreFenceReleased {
		return 0, nil, fmt.Errorf("the restore offline_mode fence of run %s was already released", owner)
	}
	tm.restoreFenceSite, tm.restoreFenceOwner = site, owner
	if err := withMySQLSafetyTimeout(ctx, func(c context.Context) error { return f.SetOfflineMode(c, true) }); err != nil {
		return 0, nil, err
	}
	keep := []string{}
	if tm.bootstrapCfg.ReplUser != "" {
		keep = append(keep, tm.bootstrapCfg.ReplUser)
	}
	var remaining int
	var protected []string
	err = withMySQLSafetyTimeout(ctx, func(c context.Context) error {
		var e error
		remaining, protected, e = f.EvictSessions(c, keep)
		return e
	})
	return remaining, protected, err
}

// SetRestoreOfflineFence tells the manager which site's fence to re-assert
// on every poll ("" for none). The runner calls it from status on each
// sync, so a restarted operator resumes re-asserting. A fence the manager
// already released for owner is not re-armed by a stale sync.
func (tm *TopologyManager) SetRestoreOfflineFence(site, owner string) {
	if tm == nil {
		return
	}
	tm.restoreFenceMu.Lock()
	defer tm.restoreFenceMu.Unlock()
	if site != "" && owner == tm.restoreFenceReleased {
		return
	}
	tm.restoreFenceSite, tm.restoreFenceOwner = site, owner
}

// ReleaseRestoreOfflineFence puts offline_mode on site back to prior (and
// verifies it took) when prior is known, then stops re-asserting the
// owner's fence. A prior of ON is set too: a mysqld that restarted during
// the restore comes back with offline_mode OFF. With prior nil the fence
// was never applied and MySQL is not touched. On error nothing is
// released, so the caller retries.
func (tm *TopologyManager) ReleaseRestoreOfflineFence(ctx context.Context, site, owner string, prior *bool) error {
	tm.restoreFenceMu.Lock()
	defer tm.restoreFenceMu.Unlock()
	if prior != nil {
		f, err := tm.restoreFencer(site)
		if err != nil {
			return err
		}
		if err := withMySQLSafetyTimeout(ctx, func(c context.Context) error { return f.SetOfflineMode(c, *prior) }); err != nil {
			return err
		}
	}
	if tm.restoreFenceOwner == owner || tm.restoreFenceSite == site {
		tm.restoreFenceSite, tm.restoreFenceOwner = "", ""
	}
	tm.restoreFenceReleased = owner
	return nil
}

// reassertRestoreOfflineFence keeps offline_mode=ON on the fenced site.
// A mysqld that restarts mid-restore comes back with it OFF (it is never
// persisted), which would readmit application sessions.
func (tm *TopologyManager) reassertRestoreOfflineFence(ctx context.Context) {
	tm.restoreFenceMu.Lock()
	defer tm.restoreFenceMu.Unlock()
	site := tm.restoreFenceSite
	if site == "" {
		return
	}
	f, err := tm.restoreFencer(site)
	if err != nil {
		return
	}
	var on bool
	if err := withMySQLSafetyTimeout(ctx, func(c context.Context) error {
		var e error
		on, e = f.GetOfflineMode(c)
		return e
	}); err != nil || on {
		return
	}
	tm.logger.Warn("re-asserting restore offline_mode fence", "site", site)
	if err := withMySQLSafetyTimeout(ctx, func(c context.Context) error { return f.SetOfflineMode(c, true) }); err != nil {
		tm.logger.Error("failed to re-assert restore offline_mode fence", "site", site, "error", err)
	}
}

func (r *TopologyManagerRunner) restoreFenceManager(nn types.NamespacedName) (*TopologyManager, error) {
	if r == nil {
		return nil, fmt.Errorf("no topology runner")
	}
	r.mu.RLock()
	mt, ok := r.managers[nn]
	r.mu.RUnlock()
	if !ok {
		return nil, fmt.Errorf("no topology manager running for %s", nn)
	}
	return mt.tm, nil
}

// RestoreFencePrivileges checks the operator account on site; see the
// TopologyManager method.
func (r *TopologyManagerRunner) RestoreFencePrivileges(ctx context.Context, nn types.NamespacedName, site string) error {
	tm, err := r.restoreFenceManager(nn)
	if err != nil {
		return err
	}
	return tm.RestoreFencePrivileges(ctx, site)
}

// RestoreOfflineMode reads @@GLOBAL.offline_mode on site.
func (r *TopologyManagerRunner) RestoreOfflineMode(ctx context.Context, nn types.NamespacedName, site string) (bool, error) {
	tm, err := r.restoreFenceManager(nn)
	if err != nil {
		return false, err
	}
	return tm.RestoreOfflineMode(ctx, site)
}

// EnforceRestoreOfflineFence enables the fence and evicts sessions.
func (r *TopologyManagerRunner) EnforceRestoreOfflineFence(ctx context.Context, nn types.NamespacedName, site, owner string) (int, []string, error) {
	tm, err := r.restoreFenceManager(nn)
	if err != nil {
		return 0, nil, err
	}
	return tm.EnforceRestoreOfflineFence(ctx, site, owner)
}

// ReleaseRestoreOfflineFence releases the fence.
func (r *TopologyManagerRunner) ReleaseRestoreOfflineFence(ctx context.Context, nn types.NamespacedName, site, owner string, prior *bool) error {
	tm, err := r.restoreFenceManager(nn)
	if err != nil {
		return err
	}
	return tm.ReleaseRestoreOfflineFence(ctx, site, owner, prior)
}

// restoreRunOwner identifies fg's in-place restore run as a fence owner:
// its runID. A run accepted by an operator that predates runID falls back
// to its confirm.
func restoreRunOwner(fg *v1alpha1.MysqlFailoverGroup) string {
	rip := fg.Status.RestoreInPlace
	switch {
	case rip != nil && rip.RunID != "":
		return rip.RunID
	case fg.Spec.RestoreInPlace != nil:
		return fg.Spec.RestoreInPlace.Confirm
	case rip != nil && rip.RunConfirm != "":
		return rip.RunConfirm
	case rip != nil:
		return rip.ConfirmTokenUsed
	}
	return ""
}

// fenceOwner is the run that owns the fence record q.
func fenceOwner(q *v1alpha1.RestoreOfflineFenceStatus) string {
	if q.RunID != "" {
		return q.RunID
	}
	return q.ConfirmToken
}

// restoreOfflineFenceWanted reports whether the restore that owns
// status.restoreOfflineFence still needs it: it is running (Fencing,
// Restoring or Resuming) as a full-instance restore of the fenced site.
func restoreOfflineFenceWanted(fg *v1alpha1.MysqlFailoverGroup) bool {
	q, rip := fg.Status.RestoreOfflineFence, fg.Status.RestoreInPlace
	if q == nil || rip == nil || rip.TargetSite != q.Site {
		return false
	}
	switch rip.Phase {
	case v1alpha1.RestoreInPlaceFencing, v1alpha1.RestoreInPlaceRestoring, v1alpha1.RestoreInPlaceResuming:
	default:
		return false
	}
	_, schema := inPlaceRunScope(fg)
	return schema == ""
}

// restoreOfflineFenceSite is the site the topology manager must keep
// fenced, and the owner, derived from status for the runner sync. It is
// empty until the prior offline_mode is on record: the fence is never
// applied before the value to restore is durable.
func restoreOfflineFenceSite(fg *v1alpha1.MysqlFailoverGroup) (site, owner string) {
	q := fg.Status.RestoreOfflineFence
	if q == nil || q.PriorOfflineMode == nil || !restoreOfflineFenceWanted(fg) {
		return "", ""
	}
	return q.Site, fenceOwner(q)
}

// mutateRestoreOfflineFence applies mutate to a fresh copy of
// status.restoreOfflineFence and writes it when it reports a change.
func (r *MysqlFailoverGroupReconciler) mutateRestoreOfflineFence(ctx context.Context, fg *v1alpha1.MysqlFailoverGroup, mutate func(fresh *v1alpha1.MysqlFailoverGroup) bool) error {
	return k8sretry.RetryOnConflict(k8sretry.DefaultRetry, func() error {
		fresh := &v1alpha1.MysqlFailoverGroup{}
		if err := r.Get(ctx, client.ObjectKeyFromObject(fg), fresh); err != nil {
			return err
		}
		if !mutate(fresh) {
			*fg = *fresh
			return nil
		}
		if err := r.Status().Update(ctx, fresh); err != nil {
			return err
		}
		*fg = *fresh
		return nil
	})
}

// recordRestoreOfflineFence creates the fence record for the accepted
// full-instance restore run (runID, confirm) of target, before anything
// touches offline_mode. Only the same run's record is kept as is: a run
// re-applied with the same confirm after a cancellation gets its own.
func (r *MysqlFailoverGroupReconciler) recordRestoreOfflineFence(ctx context.Context, fg *v1alpha1.MysqlFailoverGroup, target, confirm, runID string) error {
	owner := runID
	if owner == "" {
		owner = confirm
	}
	return r.mutateRestoreOfflineFence(ctx, fg, func(fresh *v1alpha1.MysqlFailoverGroup) bool {
		if q := fresh.Status.RestoreOfflineFence; q != nil && q.Site == target && fenceOwner(q) == owner {
			return false
		}
		if q := fresh.Status.RestoreOfflineFence; q != nil && q.PriorOfflineMode != nil {
			// An unreleased fence of an earlier restore on the same site:
			// keep its prior value, which is the one to restore at the end.
			if q.Site == target {
				fresh.Status.RestoreOfflineFence = &v1alpha1.RestoreOfflineFenceStatus{
					Site: target, ConfirmToken: confirm, RunID: runID, PriorOfflineMode: q.PriorOfflineMode,
				}
				return true
			}
		}
		fresh.Status.RestoreOfflineFence = &v1alpha1.RestoreOfflineFenceStatus{Site: target, ConfirmToken: confirm, RunID: runID}
		return true
	})
}

// enforceRestoreOfflineFence drives the fence during Fencing. It returns
// "" once the fence is on and no session from before it remains, or a
// message saying what it is waiting for.
func (r *MysqlFailoverGroupReconciler) enforceRestoreOfflineFence(ctx context.Context, fg *v1alpha1.MysqlFailoverGroup) (string, error) {
	cur := fg.Status.RestoreInPlace
	q := fg.Status.RestoreOfflineFence
	if q == nil || q.Site != cur.TargetSite {
		// A restore accepted by an operator that predates the fence: it
		// never passed the privilege check, so run it before anything
		// touches offline_mode, which would otherwise lock the operator
		// out of the primary.
		var msg string
		var waitErr error
		if fg.Spec.RestoreInPlace != nil {
			msg, waitErr = r.inPlaceFencePrivilegeRejection(ctx, fg, fg.Spec.RestoreInPlace, cur.TargetSite)
		}
		if msg != "" {
			r.noteRestorePrivilegeCheck(fg, fg.Spec.RestoreInPlace, true)
			r.stampTerminalFailure(ctx, fg, cur.TargetSite, cur.Scope, cur.ConfirmTokenUsed, msg, "RestoreInPlaceRejected")
			return "", errInPlaceRunEnded
		}
		if waitErr != nil {
			return fmt.Sprintf("waiting to verify the operator account's privileges on %s: %v", cur.TargetSite, waitErr), nil
		}
		confirm := cur.ConfirmTokenUsed
		if fg.Spec.RestoreInPlace != nil {
			confirm = fg.Spec.RestoreInPlace.Confirm
		}
		if err := r.recordRestoreOfflineFence(ctx, fg, cur.TargetSite, confirm, cur.RunID); err != nil {
			return "", err
		}
		return "recording the offline_mode fence", nil
	}
	if q.SessionsEvicted {
		return "", nil
	}
	nn := client.ObjectKeyFromObject(fg)
	if q.PriorOfflineMode == nil {
		prior, err := r.Runner.RestoreOfflineMode(ctx, nn, q.Site)
		if err != nil {
			return fmt.Sprintf("waiting to read offline_mode on %s: %v", q.Site, err), nil
		}
		if err := r.mutateRestoreOfflineFence(ctx, fg, func(fresh *v1alpha1.MysqlFailoverGroup) bool {
			fq := fresh.Status.RestoreOfflineFence
			if fq == nil || fq.Site != q.Site || fenceOwner(fq) != fenceOwner(q) || fq.PriorOfflineMode != nil {
				return false
			}
			fq.PriorOfflineMode = &prior
			return true
		}); err != nil {
			return "", err
		}
		q = fg.Status.RestoreOfflineFence
		if q == nil || q.PriorOfflineMode == nil {
			return "recording the prior offline_mode", nil
		}
	}
	remaining, protected, err := r.Runner.EnforceRestoreOfflineFence(ctx, nn, q.Site, fenceOwner(q))
	if err != nil {
		return fmt.Sprintf("waiting to apply the offline_mode fence on %s: %v", q.Site, err), nil
	}
	if len(protected) > 0 {
		// offline_mode already refuses these accounts unless they hold
		// CONNECTION_ADMIN, and an account that does could reconnect
		// anyway; waiting for them would only hold the primary fenced.
		r.Recorder.Eventf(fg, corev1.EventTypeWarning, "RestoreOfflineFenceSessionsKept",
			"could not kill the sessions of SYSTEM_USER account(s) %s on %s (the operator account lacks SYSTEM_USER); "+
				"they stay connected during the restore", strings.Join(protected, ", "), q.Site)
	}
	if remaining > 0 {
		return fmt.Sprintf("offline_mode is ON on %s; waiting for %d application session(s) to disconnect", q.Site, remaining), nil
	}
	if err := r.mutateRestoreOfflineFence(ctx, fg, func(fresh *v1alpha1.MysqlFailoverGroup) bool {
		fq := fresh.Status.RestoreOfflineFence
		if fq == nil || fq.Site != q.Site || fenceOwner(fq) != fenceOwner(q) || fq.SessionsEvicted {
			return false
		}
		fq.SessionsEvicted = true
		return true
	}); err != nil {
		return "", err
	}
	r.Recorder.Eventf(fg, corev1.EventTypeNormal, "RestoreOfflineFenceApplied",
		"offline_mode is ON on %s and no application session from before the fence remains; starting the restore", q.Site)
	return "", nil
}

// reconcileRestoreOfflineFence releases the fence once its restore has
// finished (Succeeded or Failed, or cancelled), restoring the prior
// offline_mode. It retries until the release succeeds.
func (r *MysqlFailoverGroupReconciler) reconcileRestoreOfflineFence(ctx context.Context, fg *v1alpha1.MysqlFailoverGroup) (time.Duration, error) {
	q := fg.Status.RestoreOfflineFence
	if q == nil || restoreOfflineFenceWanted(fg) {
		return 0, nil
	}
	restoreOff := q.PriorOfflineMode != nil && !*q.PriorOfflineMode
	if q.PriorOfflineMode == nil {
		// offline_mode was never touched; just make sure the topology
		// manager is not re-asserting it.
		_ = r.Runner.ReleaseRestoreOfflineFence(ctx, client.ObjectKeyFromObject(fg), q.Site, fenceOwner(q), nil)
	} else {
		if err := r.Runner.ReleaseRestoreOfflineFence(ctx, client.ObjectKeyFromObject(fg), q.Site, fenceOwner(q), q.PriorOfflineMode); err != nil {
			log.FromContext(ctx).Error(err, "release restore offline_mode fence", "fg", fg.Name, "site", q.Site)
			r.Recorder.Eventf(fg, corev1.EventTypeWarning, "RestoreOfflineFenceReleaseFailed",
				"could not restore offline_mode on %s after the in-place restore: %v; retrying", q.Site, err)
			return 15 * time.Second, nil
		}
	}
	if err := r.mutateRestoreOfflineFence(ctx, fg, func(fresh *v1alpha1.MysqlFailoverGroup) bool {
		fq := fresh.Status.RestoreOfflineFence
		if fq == nil || fq.Site != q.Site || fenceOwner(fq) != fenceOwner(q) || restoreOfflineFenceWanted(fresh) {
			return false
		}
		fresh.Status.RestoreOfflineFence = nil
		return true
	}); err != nil {
		return 0, err
	}
	if q.PriorOfflineMode != nil {
		state := "ON (it was already ON before the restore)"
		if restoreOff {
			state = "OFF"
		}
		r.Recorder.Eventf(fg, corev1.EventTypeNormal, "RestoreOfflineFenceReleased",
			"in-place restore finished; offline_mode on %s is %s", q.Site, state)
	}
	return 0, nil
}
