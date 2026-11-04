package controller

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"sort"
	"strings"
	"time"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"

	v1alpha1 "github.com/shipstream/bloodraven/api/v1alpha1"
	"github.com/shipstream/bloodraven/internal/metrics"
	"github.com/shipstream/bloodraven/internal/mysql"
)

// Peer replication hold of a full-instance in-place restore (#212).
//
// The restore drops every user schema on the primary with binlogged DROP
// DATABASE statements. A peer whose replication receiver is running pulls
// them: in credentials mode the peers replicate as the operator account,
// which offline_mode does not disconnect, and a legacy replicator that
// offline_mode did disconnect reconnects once the fence is released. So:
//
//   - Fencing stops every queued peer's receiver (STOP REPLICA IO_THREAD;
//     the applier keeps applying what it already received) and verifies
//     it before the restore Job can exist (stopRestorePeerReplication).
//   - While a site is in status.restoreReclones.pending, nothing in the
//     operator starts its replication (the hold, enforced by
//     withReplicaStartGate), except bootstrap, which refuses Armed and Held
//     recipients and is the reclone that takes a Draining site over.
//   - While the queue is Armed past Fencing, or Held, Poll re-stops a
//     receiver someone started (reassertRestorePeerReplicationStop).
//   - A failed restore that changed the primary leaves the queue Held, so
//     the peers keep their pre-restore data (settleArmedRestoreReclones).

// restorePeerCatchUpTimeout bounds how long Fencing waits for a peer whose
// receiver is running to receive the primary's fenced GTID set before it
// stops the receiver anyway.
const restorePeerCatchUpTimeout = 30 * time.Second

// restorePeerStopTimeout bounds the whole peer-stop step. offline_mode is
// already ON, so the application is down while it waits; a peer that
// cannot be stopped in time ends the run Failed without a Job.
const restorePeerStopTimeout = 2 * time.Minute

// errRestorePeerHeld is returned for a replication start refused because
// the site's replication is held by an in-place restore.
var errRestorePeerHeld = errors.New("replication of the site is held stopped by an in-place restore (status.restoreReclones)")

// errReplicaIOStopUnsupported means the site's checker cannot stop its
// receiver (a test double that does not implement mysql.ReplicaIOStopper).
var errReplicaIOStopUnsupported = errors.New("the site's MySQL checker cannot stop its replication receiver")

// errRestorePeerStartBusy means a replication start is in flight on some
// site; the caller retries the hold shortly.
var errRestorePeerStartBusy = errors.New("a replication start is in flight")

// restorePeerHoldFromStatus derives the topology manager's hold from
// status: held is every site the queue has not recloned yet; restop is the
// subset whose receiver Poll keeps stopped: the queue is Held, or Armed
// for a run past Fencing. An Armed queue in Preflight or Fencing is left
// to stopRestorePeerReplication (it lets a running receiver catch up
// first), and a Draining site is restarted by its own reclone.
func restorePeerHoldFromStatus(fg *v1alpha1.MysqlFailoverGroup) (held, restop []string) {
	if fg == nil || fg.Status.RestoreReclones == nil {
		return nil, nil
	}
	q := fg.Status.RestoreReclones
	held = append([]string(nil), q.Pending...)
	switch q.Phase {
	case v1alpha1.RestoreReclonesHeld:
		restop = append([]string(nil), q.Pending...)
	case v1alpha1.RestoreReclonesArmed:
		if rip := fg.Status.RestoreInPlace; rip == nil ||
			(rip.Phase != v1alpha1.RestoreInPlacePreflight && rip.Phase != v1alpha1.RestoreInPlaceFencing) {
			restop = append([]string(nil), q.Pending...)
		}
	}
	return held, restop
}

func sortedUnion(a, b []string) []string {
	out := append(append([]string(nil), a...), b...)
	sort.Strings(out)
	return slices.Compact(out)
}

func sortedSet(a []string) []string {
	out := append([]string(nil), a...)
	sort.Strings(out)
	return slices.Compact(out)
}

// restoreProtection is every restore safety guard the topology manager
// enforces locally, derived from status as one snapshot:
//   - held: no operator-issued replication start (restorePeerHeld);
//   - restop: Poll re-stops the receiver (restorePeerRestop);
//   - blocked: no clone into the site (restoreRecloneBlocked);
//   - pending: no automatic promotion (restoreReclonePending);
//   - frozen: no cross-site action (topologyFrozen);
//   - fenceSite/fenceOwner: offline_mode re-asserted on the primary.
type restoreProtection struct {
	held, restop, blocked, pending []string
	frozen                         bool
	fenceSite, fenceOwner          string
}

// restoreProtectionFromStatus derives the protection from status.
func restoreProtectionFromStatus(fg *v1alpha1.MysqlFailoverGroup) restoreProtection {
	held, restop := restorePeerHoldFromStatus(fg)
	fenceSite, fenceOwner := restoreOfflineFenceGuard(fg)
	return restoreProtection{
		held: sortedSet(held), restop: sortedSet(restop), blocked: sortedSet(restoreRecloneBlockedSites(fg)),
		pending: sortedSet(restoreReclonePendingSites(fg)), frozen: inPlaceRestoreFreezesTopology(fg),
		fenceSite: fenceSite, fenceOwner: fenceOwner,
	}
}

// restoreOfflineFenceGuard is the offline_mode fence the topology manager
// must keep re-asserting. That is the recorded fence while its restore runs
// (restoreOfflineFenceSite), and also after the restore ended for as long
// as its reclone queue is still Armed: settlement can wait, with the fence
// up, for the failed restore's Job pods to terminate, and a mysqld restart
// in that window must not readmit applications while such a pod may still
// drop schemas. It is separate from restoreOfflineFenceWanted, which
// decides whether the fence may be released (settlement first, then
// ReleaseRestoreOfflineFence); once the queue is settled the fence is
// released in the same pass, and no longer re-asserted, so an
// administrator who turns offline_mode OFF by hand after a failed release
// is not overridden.
func restoreOfflineFenceGuard(fg *v1alpha1.MysqlFailoverGroup) (site, owner string) {
	if site, owner = restoreOfflineFenceSite(fg); site != "" {
		return site, owner
	}
	q := fg.Status.RestoreOfflineFence
	if q == nil || q.PriorOfflineMode == nil {
		return "", ""
	}
	if rq := fg.Status.RestoreReclones; rq != nil && rq.Phase == v1alpha1.RestoreReclonesArmed {
		return q.Site, fenceOwner(q)
	}
	return "", ""
}

// equal reports whether p and q are the same guards.
func (p restoreProtection) equal(q restoreProtection) bool {
	return slices.Equal(p.held, q.held) && slices.Equal(p.restop, q.restop) && slices.Equal(p.blocked, q.blocked) &&
		slices.Equal(p.pending, q.pending) && p.frozen == q.frozen && p.fenceSite == q.fenceSite && p.fenceOwner == q.fenceOwner
}

// onlyTightens reports whether moving from cur to p only adds guards: more
// held, blocked or pending sites, a freeze, or an offline_mode fence where
// none was set. That keeps replication stopped, clones and promotion out
// and applications fenced, so it is safe to apply from a possibly stale
// view. Any release, and any change to restop (which actively stops a
// receiver, and may stop one a legitimate reclone started), is not.
func (p restoreProtection) onlyTightens(cur restoreProtection) bool {
	fenceOK := (p.fenceSite == cur.fenceSite && p.fenceOwner == cur.fenceOwner) || (cur.fenceSite == "" && p.fenceSite != "")
	return slices.Equal(p.restop, cur.restop) && isSubset(cur.held, p.held) && isSubset(cur.blocked, p.blocked) &&
		isSubset(cur.pending, p.pending) && (p.frozen || !cur.frozen) && fenceOK
}

func isSubset(sub, set []string) bool {
	for _, s := range sub {
		if !slices.Contains(set, s) {
			return false
		}
	}
	return true
}

// restoreProtectionSnapshot returns a copy of the manager's guards.
func (tm *TopologyManager) restoreProtectionSnapshot() restoreProtection {
	tm.restoreFenceMu.Lock()
	fenceSite, fenceOwner := tm.restoreFenceSite, tm.restoreFenceOwner
	tm.restoreFenceMu.Unlock()
	tm.mu.RLock()
	defer tm.mu.RUnlock()
	return restoreProtection{
		held:      append([]string(nil), tm.restorePeerHeld...),
		restop:    append([]string(nil), tm.restorePeerRestop...),
		blocked:   append([]string(nil), tm.restoreRecloneBlocked...),
		pending:   append([]string(nil), tm.restoreReclonePending...),
		frozen:    tm.topologyFrozen,
		fenceSite: fenceSite, fenceOwner: fenceOwner,
	}
}

// setRestoreProtection replaces the manager's guards.
func (tm *TopologyManager) setRestoreProtection(p restoreProtection) {
	tm.mu.Lock()
	held := sortedSet(p.held)
	if !slices.Equal(held, tm.restorePeerHeld) {
		// Held sites are excluded from promotion: re-run the decision.
		tm.keyringRotationBlockedDirty = true
	}
	tm.restorePeerHeld, tm.restorePeerRestop, tm.restoreRecloneBlocked = held, sortedSet(p.restop), sortedSet(p.blocked)
	tm.mu.Unlock()
	tm.SetRestoreReclonePending(p.pending)
	tm.SetTopologyFrozen(p.frozen)
	tm.SetRestoreOfflineFence(p.fenceSite, p.fenceOwner)
}

// tightenRestoreProtection adds p's guards (held, blocked and pending
// sites, a freeze, a fence where none is set) and releases nothing;
// restop is left alone.
func (tm *TopologyManager) tightenRestoreProtection(p restoreProtection) {
	cur := tm.restoreProtectionSnapshot()
	tm.mu.Lock()
	held := sortedUnion(tm.restorePeerHeld, p.held)
	if !slices.Equal(held, tm.restorePeerHeld) {
		tm.keyringRotationBlockedDirty = true
	}
	tm.restorePeerHeld = held
	tm.restoreRecloneBlocked = sortedUnion(tm.restoreRecloneBlocked, p.blocked)
	tm.mu.Unlock()
	tm.SetRestoreReclonePending(sortedUnion(cur.pending, p.pending))
	if p.frozen {
		tm.SetTopologyFrozen(true)
	}
	if cur.fenceSite == "" && p.fenceSite != "" {
		tm.SetRestoreOfflineFence(p.fenceSite, p.fenceOwner)
	}
}

// SetRestorePeerHold sets the held and re-stop sets directly (blocked is
// left alone). authoritative replaces them; otherwise held sites are only
// added. The runner uses applyRestoreProtection; this is for tests.
func (tm *TopologyManager) SetRestorePeerHold(held, restop []string, authoritative bool) {
	if tm == nil {
		return
	}
	if authoritative {
		tm.mu.Lock()
		tm.restorePeerHeld, tm.restorePeerRestop = sortedSet(held), sortedSet(restop)
		tm.mu.Unlock()
		return
	}
	tm.mu.Lock()
	tm.restorePeerHeld = sortedUnion(tm.restorePeerHeld, held)
	tm.mu.Unlock()
}

// restorePeerHoldSets returns copies of the held and re-stop sets.
func (tm *TopologyManager) restorePeerHoldSets() (held, restop []string) {
	p := tm.restoreProtectionSnapshot()
	return p.held, p.restop
}

// restorePeerHeldSite reports whether site's replication is held.
func (tm *TopologyManager) restorePeerHeldSite(site string) bool {
	tm.mu.RLock()
	defer tm.mu.RUnlock()
	return slices.Contains(tm.restorePeerHeld, site)
}

// withReplicaStartGate runs op, an operation that may start replication on
// site, unless the site's replication is held (errRestorePeerHeld). The
// hold is checked under the read side of peerStartMu, which is held until
// op returns, so StopRestorePeerReplication (write side) waits for every
// start already admitted, and no start is admitted once the hold is set.
// Calls must not nest.
func (tm *TopologyManager) withReplicaStartGate(site string, op func() error) error {
	tm.peerStartMu.RLock()
	defer tm.peerStartMu.RUnlock()
	if tm.restorePeerHeldSite(site) {
		return errRestorePeerHeld
	}
	return op()
}

// startReplicaGate is withReplicaStartGate for UpdateController.
func (tm *TopologyManager) startReplicaGate(site string, start func() error) error {
	return tm.withReplicaStartGate(site, start)
}

// RestorePeerStopOutcome is the result of one StopRestorePeerReplication
// call for a site.
type RestorePeerStopOutcome struct {
	// Stopped is true once the receiver is verified stopped (neither
	// running nor connecting), or the site has no replication channel.
	Stopped bool
	// NoChannel is true when the site has no replication channel.
	NoChannel bool
	// CatchingUp is true when the receiver is still running and has not
	// received the snapshot yet; it was left running.
	CatchingUp bool
	// Missing is the part of the snapshot the site had neither received
	// nor applied (empty when it had everything, or it could not be
	// computed).
	Missing string
}

// StopRestorePeerReplication holds site's replication and stops its
// receiver. While waitForCatchUp is true, a running receiver that has not
// yet received snapshot (the primary's fenced gtid_executed) is left
// running and reported CatchingUp; a receiver that is connecting or
// stopped never catches up and is stopped at once. The hold is added
// under the write side of peerStartMu (errRestorePeerStartBusy when a
// start is in flight), so no start the operator admitted can restart the
// receiver after this returns Stopped.
func (tm *TopologyManager) StopRestorePeerReplication(ctx context.Context, site, snapshot string, waitForCatchUp bool) (RestorePeerStopOutcome, error) {
	s := tm.getSite(site)
	if s == nil {
		return RestorePeerStopOutcome{}, fmt.Errorf("unknown site %q", site)
	}
	if !tm.peerStartMu.TryLock() {
		return RestorePeerStopOutcome{}, errRestorePeerStartBusy
	}
	// Serialized with applyRestoreProtection, which replaces the held set.
	tm.restoreProtectionMu.Lock()
	tm.mu.Lock()
	if !slices.Contains(tm.restorePeerHeld, site) {
		tm.restorePeerHeld = sortedUnion(tm.restorePeerHeld, []string{site})
	}
	tm.mu.Unlock()
	tm.restoreProtectionMu.Unlock()
	tm.peerStartMu.Unlock()

	stopper, ok := s.mysql.(mysql.ReplicaIOStopper)
	if !ok {
		return RestorePeerStopOutcome{}, errReplicaIOStopUnsupported
	}
	var rs *mysql.ReplicaStatus
	if err := withMySQLSafetyTimeout(ctx, func(c context.Context) error {
		var e error
		rs, e = s.mysql.ShowReplicaStatus(c)
		return e
	}); err != nil {
		return RestorePeerStopOutcome{}, err
	}
	if rs == nil || strings.TrimSpace(rs.SourceHost) == "" {
		return RestorePeerStopOutcome{Stopped: true, NoChannel: true}, nil
	}
	missing := restorePeerMissing(snapshot, rs)
	if waitForCatchUp && rs.IORunning && missing != "" {
		return RestorePeerStopOutcome{CatchingUp: true, Missing: missing}, nil
	}
	if rs.IORunning || rs.IOConnecting {
		if err := withMySQLSafetyTimeout(ctx, func(c context.Context) error { return stopper.StopReplicaIOThread(c) }); err != nil {
			return RestorePeerStopOutcome{}, err
		}
	}
	if err := withMySQLSafetyTimeout(ctx, func(c context.Context) error {
		var e error
		rs, e = s.mysql.ShowReplicaStatus(c)
		return e
	}); err != nil {
		return RestorePeerStopOutcome{}, err
	}
	if rs == nil {
		return RestorePeerStopOutcome{Stopped: true, NoChannel: true}, nil
	}
	if rs.IORunning || rs.IOConnecting {
		return RestorePeerStopOutcome{}, fmt.Errorf("replication receiver on %s still running after STOP REPLICA IO_THREAD", site)
	}
	return RestorePeerStopOutcome{Stopped: true, Missing: restorePeerMissing(snapshot, rs)}, nil
}

// restorePeerMissing returns the part of snapshot rs has neither retrieved
// nor executed, or "" when it has all of it or the sets cannot be parsed.
func restorePeerMissing(snapshot string, rs *mysql.ReplicaStatus) string {
	if strings.TrimSpace(snapshot) == "" || rs == nil {
		return ""
	}
	want, err := mysql.ParseGTIDSet(snapshot)
	if err != nil {
		return ""
	}
	retrieved, rerr := mysql.ParseGTIDSet(rs.RetrievedGtidSet)
	executed, eerr := mysql.ParseGTIDSet(rs.ExecutedGtidSet)
	if rerr != nil || eerr != nil {
		return ""
	}
	missing := want.Subtract(retrieved).Subtract(executed)
	if missing.IsEmpty() {
		return ""
	}
	return missing.String()
}

// reassertRestorePeerReplicationStop keeps the receivers of the re-stop
// sites stopped: a restore that is past Fencing, or failed and left the
// queue Held. A manual START REPLICA, or an operator that was upgraded
// mid-restore, would otherwise stream the restore's DROP statements.
func (tm *TopologyManager) reassertRestorePeerReplicationStop(ctx context.Context) {
	_, restop := tm.restorePeerHoldSets()
	for _, name := range restop {
		s := tm.getSite(name)
		if s == nil {
			continue
		}
		tm.mu.RLock()
		skip := s.probeFailed
		tm.mu.RUnlock()
		if skip {
			continue
		}
		stopper, ok := s.mysql.(mysql.ReplicaIOStopper)
		if !ok {
			continue
		}
		var rs *mysql.ReplicaStatus
		if err := withMySQLSafetyTimeout(ctx, func(c context.Context) error {
			var e error
			rs, e = s.mysql.ShowReplicaStatus(c)
			return e
		}); err != nil || rs == nil || (!rs.IORunning && !rs.IOConnecting) {
			continue
		}
		tm.logger.Warn("re-stopping restore peer replication IO thread", "site", name)
		if err := withMySQLSafetyTimeout(ctx, func(c context.Context) error { return stopper.StopReplicaIOThread(c) }); err != nil {
			tm.logger.Error("failed to re-stop restore peer replication IO thread", "site", name, "error", err)
		}
	}
}

// restoreMutationInFlight returns why an operation that may start
// replication or reshape the topology is still running ("" when none):
// an ordered update or handoff, a planned failover, or a bootstrap/clone.
// With requireFrozen it also waits for the restore freeze to be observed:
// a frozen manager admits no new update, clone or cross-site action.
func (tm *TopologyManager) restoreMutationInFlight(requireFrozen bool) string {
	tm.mu.RLock()
	frozen := tm.topologyFrozen
	planned := tm.plannedFailoverActive
	phase := tm.bootstrapPhase
	recipient := tm.bootstrapRecipient
	admitting := tm.bootstrapAdmitting
	tm.mu.RUnlock()
	switch {
	case requireFrozen && !frozen:
		return "waiting for the topology manager to observe the restore freeze"
	case admitting != "":
		return fmt.Sprintf("waiting for the clone admission of %s to finish", admitting)
	case tm.isUpdating():
		return "waiting for the running ordered update to finish"
	case planned:
		return "waiting for the planned failover to finish"
	case !bootstrapIdlePhase(phase):
		return fmt.Sprintf("waiting for the running clone of %s to finish", recipient)
	}
	return ""
}

// RestorePeerStopPrivileges checks that the operator account can stop
// site's receiver: nil when it can, an error wrapping
// errRestoreFenceNoPrivilege naming the missing privileges, or another
// error (unreachable, checker without the capability) when unknown.
func (tm *TopologyManager) RestorePeerStopPrivileges(ctx context.Context, site string) error {
	s := tm.getSite(site)
	if s == nil {
		return fmt.Errorf("unknown site %q", site)
	}
	stopper, ok := s.mysql.(mysql.ReplicaIOStopper)
	if !ok {
		return errReplicaIOStopUnsupported
	}
	var missing []string
	if err := withMySQLSafetyTimeout(ctx, func(c context.Context) error {
		var e error
		missing, e = stopper.MissingReplicaIOStopPrivileges(c)
		return e
	}); err != nil {
		return err
	}
	if len(missing) > 0 {
		return fmt.Errorf("%w %s", errRestoreFenceNoPrivilege, strings.Join(missing, ", "))
	}
	return nil
}

// RestoreGtidExecuted reads @@GLOBAL.gtid_executed on site.
func (tm *TopologyManager) RestoreGtidExecuted(ctx context.Context, site string) (string, error) {
	s := tm.getSite(site)
	if s == nil {
		return "", fmt.Errorf("unknown site %q", site)
	}
	var gtid string
	err := withMySQLSafetyTimeout(ctx, func(c context.Context) error {
		var e error
		gtid, e = s.mysql.GetGtidExecuted(c)
		return e
	})
	return gtid, err
}

// --- runner wrappers ---

// RestorePeerStopPrivileges checks site; see the TopologyManager method.
func (r *TopologyManagerRunner) RestorePeerStopPrivileges(ctx context.Context, nn types.NamespacedName, site string) error {
	tm, err := r.restoreFenceManager(nn)
	if err != nil {
		return err
	}
	return tm.RestorePeerStopPrivileges(ctx, site)
}

// RestoreGtidExecuted reads gtid_executed on site.
func (r *TopologyManagerRunner) RestoreGtidExecuted(ctx context.Context, nn types.NamespacedName, site string) (string, error) {
	tm, err := r.restoreFenceManager(nn)
	if err != nil {
		return "", err
	}
	return tm.RestoreGtidExecuted(ctx, site)
}

// StopRestorePeerReplication holds and stops site's receiver; see the
// TopologyManager method.
func (r *TopologyManagerRunner) StopRestorePeerReplication(ctx context.Context, nn types.NamespacedName, site, snapshot string, waitForCatchUp bool) (RestorePeerStopOutcome, error) {
	tm, err := r.restoreFenceManager(nn)
	if err != nil {
		return RestorePeerStopOutcome{}, err
	}
	return tm.StopRestorePeerReplication(ctx, site, snapshot, waitForCatchUp)
}

// RestoreMutationInFlight returns why a topology mutation is still
// running, or "" when none is (see restoreMutationInFlight).
func (r *TopologyManagerRunner) RestoreMutationInFlight(nn types.NamespacedName, requireFrozen bool) (string, error) {
	tm, err := r.restoreFenceManager(nn)
	if err != nil {
		return "", err
	}
	return tm.restoreMutationInFlight(requireFrozen), nil
}

// applyRestoreProtection brings tm's restore guards (restoreProtection:
// held, restop, blocked, pending, the freeze and the offline_mode fence)
// and the bloodraven_restore_reclone_pending gauge up to date with fg. Every application is serialized per manager. Only a change
// that tightens the protection is taken from fg as given (the runner's
// cached list may be stale, and a stale view that releases a hold, or
// re-arms a re-stop after the queue moved on, would let a peer replicate a
// restore's DROP statements or stop a legitimate reclone's replication);
// any other change is applied from a read through the uncached API reader
// taken under the lock, so applications never go back in time. written
// marks an object this operator just wrote; it is still confirmed, because
// two writers' pushes may arrive out of order. When the read fails the
// change is only tightened and the error returned; the next sync retries.
// Without an uncached reader (tests) fg is the only view there is.
func (r *TopologyManagerRunner) applyRestoreProtection(ctx context.Context, tm *TopologyManager, fg *v1alpha1.MysqlFailoverGroup, written bool) error {
	if r == nil || tm == nil || fg == nil {
		return nil
	}
	tm.restoreProtectionMu.Lock()
	defer tm.restoreProtectionMu.Unlock()
	want := restoreProtectionFromStatus(fg)
	if r.apiReader == nil {
		tm.setRestoreProtection(want)
		emitRestoreReclonePendingMetric(fg)
		return nil
	}
	if !written {
		cur := tm.restoreProtectionSnapshot()
		if want.equal(cur) {
			emitRestoreReclonePendingMetric(fg)
			return nil
		}
		if want.onlyTightens(cur) {
			// Additions only: nothing a concurrent writer set is cleared.
			tm.tightenRestoreProtection(want)
			emitRestoreReclonePendingMetric(fg)
			return nil
		}
	}
	var fresh v1alpha1.MysqlFailoverGroup
	if err := r.apiReader.Get(ctx, client.ObjectKeyFromObject(fg), &fresh); err != nil {
		tm.tightenRestoreProtection(want)
		r.logger.Warn("could not confirm a restore protection change; applying additions only", "fg", client.ObjectKeyFromObject(fg).String(), "error", err)
		return err
	}
	tm.setRestoreProtection(restoreProtectionFromStatus(&fresh))
	emitRestoreReclonePendingMetric(&fresh)
	return nil
}

// initRestoreProtection sets every restore guard of a new topology manager
// (restoreProtection) before it starts, from a read through the uncached
// API reader: the runner's cached list may predate the restore queue, and
// the new manager has no guard of its own to keep. If the read fails, a
// replacement inherits every guard of the manager it replaces (tightened
// with the cached view); a brand-new manager is not started (the next sync
// retries). After start the guards change only through
// applyRestoreProtection and the restore's own serialized writes (see
// TopologyManager.restoreProtectionMu).
func (r *TopologyManagerRunner) initRestoreProtection(ctx context.Context, tm *TopologyManager, fg *v1alpha1.MysqlFailoverGroup, prev *managedTopology) error {
	tm.restoreProtectionMu.Lock()
	defer tm.restoreProtectionMu.Unlock()
	view := fg
	if r.apiReader != nil {
		var fresh v1alpha1.MysqlFailoverGroup
		if err := r.apiReader.Get(ctx, client.ObjectKeyFromObject(fg), &fresh); err != nil {
			if prev == nil || prev.tm == nil {
				return fmt.Errorf("read the group uncached to initialise its restore protection: %w", err)
			}
			tm.setRestoreProtection(prev.tm.restoreProtectionSnapshot())
			tm.tightenRestoreProtection(restoreProtectionFromStatus(fg))
			r.logger.Warn("could not read the group uncached; the new topology manager keeps its predecessor's restore protection",
				"fg", client.ObjectKeyFromObject(fg).String(), "error", err)
			return nil
		}
		view = &fresh
	}
	tm.setRestoreProtection(restoreProtectionFromStatus(view))
	emitRestoreReclonePendingMetric(view)
	return nil
}

// SetRestorePeerHold brings fg's running topology manager's restore
// protection up to date after this operator wrote fg.
func (r *TopologyManagerRunner) SetRestorePeerHold(ctx context.Context, fg *v1alpha1.MysqlFailoverGroup) {
	if r == nil || fg == nil {
		return
	}
	r.mu.RLock()
	mt, ok := r.managers[client.ObjectKeyFromObject(fg)]
	r.mu.RUnlock()
	if !ok {
		emitRestoreReclonePendingMetric(fg)
		return
	}
	_ = r.applyRestoreProtection(ctx, mt.tm, fg, true)
}

// emitRestoreReclonePendingMetric mirrors status.restoreReclones.pending
// into bloodraven_restore_reclone_pending.
func emitRestoreReclonePendingMetric(fg *v1alpha1.MysqlFailoverGroup) {
	if fg == nil {
		return
	}
	q := fg.Status.RestoreReclones
	if q == nil {
		metrics.SetRestoreReclonePending(fg.Namespace, fg.Name, "", nil)
		return
	}
	metrics.SetRestoreReclonePending(fg.Namespace, fg.Name, string(q.Phase), q.Pending)
}

// --- reconciler: the Fencing step ---

// restorePeerStopWait is when this process first tried to stop the peers'
// receivers for a run.
type restorePeerStopWait struct {
	runID string
	since time.Time
}

// restorePeerStopElapsed returns how long this process has been trying to
// stop the peers' receivers for fg's run.
func (r *MysqlFailoverGroupReconciler) restorePeerStopElapsed(fg *v1alpha1.MysqlFailoverGroup) time.Duration {
	nn := client.ObjectKeyFromObject(fg)
	runID := restoreRunOwner(fg)
	r.restorePeerStopMu.Lock()
	defer r.restorePeerStopMu.Unlock()
	if r.restorePeerStopStart == nil {
		r.restorePeerStopStart = map[types.NamespacedName]restorePeerStopWait{}
	}
	w, ok := r.restorePeerStopStart[nn]
	if !ok || w.runID != runID {
		w = restorePeerStopWait{runID: runID, since: time.Now()}
		r.restorePeerStopStart[nn] = w
	}
	return time.Since(w.since)
}

// restorePeerStopSitesMissingFromQueue lists the non-target sites in
// spec.sites that the run's reclone queue does not cover: sites added
// after the restore was accepted.
func restorePeerStopSitesMissingFromQueue(fg *v1alpha1.MysqlFailoverGroup, target string) []string {
	var missing []string
	q := fg.Status.RestoreReclones
	for _, s := range fg.Spec.Sites {
		if s.Name == target {
			continue
		}
		if q == nil || !slices.Contains(q.Pending, s.Name) {
			missing = append(missing, s.Name)
		}
	}
	return missing
}

// stopRestorePeerReplication is Fencing's last step for a full-instance
// restore: it stops the replication receiver of every queued peer, after
// letting a running receiver catch up to the fenced primary, and returns
// "" once every receiver is verified stopped, or a message saying what it
// waits for. It ends the run (errInPlaceRunEnded) when the peer set
// changed since the restore was accepted, or when the peers cannot be
// stopped within restorePeerStopTimeout.
func (r *MysqlFailoverGroupReconciler) stopRestorePeerReplication(ctx context.Context, fg *v1alpha1.MysqlFailoverGroup) (string, error) {
	cur := fg.Status.RestoreInPlace
	spec := fg.Spec.RestoreInPlace
	target := cur.TargetSite
	nn := client.ObjectKeyFromObject(fg)

	if len(restoreRecloneRecipients(fg, target)) > 0 {
		if q := fg.Status.RestoreReclones; q == nil || q.Phase != v1alpha1.RestoreReclonesArmed {
			// A run accepted by an operator that predates the queue being
			// armed at Preflight (or a queue lost to a status write): arm it
			// now, with the confirm the Job will run under.
			if spec == nil {
				return "", fmt.Errorf("in-place restore in Fencing without spec.restoreInPlace")
			}
			if err := r.armRestoreReclones(ctx, fg, target, inPlaceRunConfirm(cur, spec)); err != nil {
				return "", fmt.Errorf("record in-place restore reclone queue: %w", err)
			}
			if q := fg.Status.RestoreReclones; q == nil || q.Phase != v1alpha1.RestoreReclonesArmed {
				return "recording the peer reclone queue", nil
			}
		}
	}
	if added := restorePeerStopSitesMissingFromQueue(fg, target); len(added) > 0 {
		r.stampTerminalFailure(ctx, fg, target, cur.Scope, cur.ConfirmTokenUsed,
			fmt.Sprintf("cancelled: the peer set changed after the restore was accepted (%s not in status.restoreReclones.pending); nothing was restored. "+
				"The restore re-runs with the current peers", strings.Join(added, ", ")),
			"RestoreInPlaceCancelled")
		return "", errInPlaceRunEnded
	}
	q := fg.Status.RestoreReclones
	if q == nil || len(q.Pending) == 0 {
		return "", nil
	}

	elapsed := r.restorePeerStopElapsed(fg)
	fail := func(why string) (string, error) {
		msg := fmt.Sprintf("could not stop the replication receivers of the peers within %s (%s); nothing was restored. "+
			"Bring the peer back or remove it from spec.sites, then bump spec.restoreInPlace.confirm to retry", restorePeerStopTimeout, why)
		// No Job exists, but offline_mode was applied: consume the confirm
		// (as a Job that could not be built does) so the fence is not
		// re-applied in a loop.
		confirm := cur.ConfirmTokenUsed
		if spec != nil {
			confirm = inPlaceRunConfirm(cur, spec)
		} else if cur.RunConfirm != "" {
			confirm = cur.RunConfirm
		}
		r.stampTerminalFailure(ctx, fg, target, cur.Scope, confirm, msg, "RestoreInPlacePeerStopFailed")
		return "", errInPlaceRunEnded
	}

	// The freeze normally reached the topology manager when Fencing was
	// entered; confirm it again in case that manager was (re)started since.
	r.Runner.SetRestorePeerHold(ctx, fg)
	if wait, err := r.Runner.RestoreMutationInFlight(nn, true); err != nil || wait != "" {
		if err != nil {
			wait = fmt.Sprintf("waiting for the topology manager: %v", err)
		}
		if elapsed >= restorePeerStopTimeout {
			return fail(wait)
		}
		return wait, nil
	}

	if q.FencedGtidExecuted == "" {
		gtid, err := r.Runner.RestoreGtidExecuted(ctx, nn, target)
		if err != nil {
			wait := fmt.Sprintf("waiting to read gtid_executed on %s: %v", target, err)
			if elapsed >= restorePeerStopTimeout {
				return fail(wait)
			}
			return wait, nil
		}
		if _, err := r.mutateRestoreReclones(ctx, fg, func(fresh *v1alpha1.MysqlFailoverGroup) bool {
			fq := fresh.Status.RestoreReclones
			if fq == nil || fq.Phase != v1alpha1.RestoreReclonesArmed || fq.FencedGtidExecuted != "" {
				return false
			}
			fq.FencedGtidExecuted = gtid
			return true
		}); err != nil {
			return "", err
		}
		q = fg.Status.RestoreReclones
		if q == nil || q.FencedGtidExecuted != gtid {
			return "recording the primary's fenced gtid_executed", nil
		}
	}

	catchUp := elapsed < restorePeerCatchUpTimeout
	var waits, incomplete, stopped []string
	for _, site := range q.Pending {
		if !specDeclaresSite(fg, site) {
			// Removed from spec.sites after acceptance: the operator no
			// longer manages it.
			continue
		}
		stopped = append(stopped, site)
		out, err := r.Runner.StopRestorePeerReplication(ctx, nn, site, q.FencedGtidExecuted, catchUp)
		switch {
		case err != nil:
			waits = append(waits, fmt.Sprintf("%s: %v", site, err))
		case out.CatchingUp:
			waits = append(waits, fmt.Sprintf("%s: receiving the primary's last transactions (%s missing)", site, out.Missing))
		case out.Missing != "":
			incomplete = append(incomplete, fmt.Sprintf("%s (missing %s)", site, out.Missing))
		}
	}
	if len(waits) > 0 {
		wait := "waiting to stop replication on peer " + strings.Join(waits, "; ")
		if elapsed >= restorePeerStopTimeout {
			return fail(wait)
		}
		return wait, nil
	}
	if len(incomplete) > 0 {
		r.Recorder.Eventf(fg, corev1.EventTypeWarning, "RestorePeerReplicationIncomplete",
			"stopped the replication receiver of %s before it received every transaction the primary had committed before the fence; "+
				"the peer keeps its pre-restore data without those transactions", strings.Join(incomplete, ", "))
	}
	r.Recorder.Eventf(fg, corev1.EventTypeNormal, "RestorePeerReplicationStopped",
		"stopped the replication receiver of %s; they keep their data until they are recloned from the restored primary",
		strings.Join(stopped, ", "))
	r.restorePeerStopMu.Lock()
	delete(r.restorePeerStopStart, nn)
	r.restorePeerStopMu.Unlock()
	return "", nil
}

// inPlacePeerStopRejection checks, before anything is fenced, that the
// operator can stop every peer's receiver: the rejection message for a
// missing privilege, or the error that kept the check from completing
// (an unreachable peer, a checker without the capability), which makes
// Preflight wait.
func (r *MysqlFailoverGroupReconciler) inPlacePeerStopRejection(ctx context.Context, fg *v1alpha1.MysqlFailoverGroup, target string) (string, error) {
	nn := client.ObjectKeyFromObject(fg)
	for _, site := range restoreRecloneRecipients(fg, target) {
		err := r.Runner.RestorePeerStopPrivileges(ctx, nn, site)
		if errors.Is(err, errRestoreFenceNoPrivilege) {
			return fmt.Sprintf("%s%v on %s. A full-instance restore stops the replication receiver of every peer so the restore's DROP statements "+
				"do not reach them; grant it ON *.* to the operator account (%s). The restore is retried with the same confirm about once a minute",
				restorePrivilegeRejectionPrefix, strings.TrimPrefix(err.Error(), errRestoreFenceNoPrivilege.Error()+" "), site,
				fg.Spec.EffectiveOperatorSecretName()), nil
		}
		if errors.Is(err, errReplicaIOStopUnsupported) {
			return fmt.Sprintf("restoreInPlace rejected: the operator cannot stop the replication receiver of peer %s (its MySQL checker does not "+
				"support STOP REPLICA IO_THREAD); a full-instance restore stops every peer's receiver so the restore's DROP statements do not reach them", site), nil
		}
		if err != nil {
			return "", &restorePeerWaitError{site: site, err: err}
		}
	}
	return "", nil
}

// restorePeerWaitError is a peer that could not be checked before a
// full-instance restore (unreachable): Preflight waits for it.
type restorePeerWaitError struct {
	site string
	err  error
}

func (e *restorePeerWaitError) Error() string { return fmt.Sprintf("peer %s: %v", e.site, e.err) }
func (e *restorePeerWaitError) Unwrap() error { return e.err }
