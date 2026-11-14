package controller

import (
	"context"
	"errors"
	"time"

	"k8s.io/apimachinery/pkg/types"

	v1alpha1 "github.com/shipstream/bloodraven/api/v1alpha1"
	"github.com/shipstream/bloodraven/internal/mysql"
)

// Hooks for the deterministic simulation's step model of an ordered update
// (internal/dst update actor). They drive the real claim API: the same
// preconditions and the same claim as startUpdate and its goroutine, with
// the MySQL statements of the handoff issued by the simulation one at a
// time. They are not used in production.

// errOrderedUpdateRefused is BeginOrderedUpdateForSim's refusal: startUpdate
// would not have started an update now.
var errOrderedUpdateRefused = errors.New("ordered update refused: another actor holds the sites or an update is pending")

// SimOrderedUpdate is one simulated ordered update between
// BeginOrderedUpdateForSim and FinishOrderedUpdateForSim.
type SimOrderedUpdate struct {
	tm       *TopologyManager
	epoch    uint64
	active   string
	claimed  bool
	finished bool
}

// BeginOrderedUpdateForSim starts an ordered update the way startUpdate
// does once it has decided to: refused while one is pending, the manager
// is retired, a predecessor's update runs, or a planned-failover claim or
// hold is held; otherwise it marks the update pending and registers it on
// updateWorkers. It captures the authority epoch and active site the update
// is planned against.
func (tm *TopologyManager) BeginOrderedUpdateForSim() (*SimOrderedUpdate, error) {
	tm.mu.Lock()
	defer tm.mu.Unlock()
	if tm.deploymentUpdatePending || tm.retired || tm.predecessorUpdateInFlight || tm.plannedClaimHeldLocked() {
		return nil, errOrderedUpdateRefused
	}
	active := tm.activeSiteLocked()
	if active == "" {
		return nil, errOrderedUpdateRefused
	}
	tm.deploymentUpdatePending = true
	tm.updateWorkers.Add(1)
	return &SimOrderedUpdate{tm: tm, epoch: tm.authorityEpoch, active: active}, nil
}

// Active is the site the update was planned against.
func (u *SimOrderedUpdate) Active() string { return u.active }

// ClaimOrderedUpdateForSim takes the update's MySQL mutation window with
// claimOrderedUpdate. It reports whether the claim was granted; a refused
// update must still be finished.
func (u *SimOrderedUpdate) ClaimOrderedUpdateForSim(ctx context.Context) (bool, error) {
	claimed, err := u.tm.claimOrderedUpdate(ctx, u.epoch, u.active)
	u.claimed = claimed
	return claimed, err
}

// HandoffRefusalForSim is the standby's handoff eligibility as
// ExecuteTargets reads it when it chooses the handoff target
// (UpdateTarget.HandoffBlocked, liveHandoffRefusal): nil, or why site must
// not receive the handoff (mid-keyring-rotation, awaiting its post-restore
// reclone, lacking transactions it issued itself, divergent).
func (u *SimOrderedUpdate) HandoffRefusalForSim(site string) error {
	return u.tm.liveHandoffRefusal(site)()
}

// RecordHandoffForSim records a completed handoff from the update's active
// site to target as the update's goroutine does (recordHandoffPromotion):
// recordFailover, DNS, and the fenced source for the next poll.
func (u *SimOrderedUpdate) RecordHandoffForSim(ctx context.Context, target, promotionGTID string) {
	u.tm.recordHandoffPromotion(ctx, u.active, target, promotionGTID)
}

// RotateSourceBinlogForSim is ExecuteHandoff's best-effort rotation of the
// fenced source's binary log, through the production rotateBinlog (the
// manager's failover controller and its clock).
func (u *SimOrderedUpdate) RotateSourceBinlogForSim(ctx context.Context, source mysql.Checker, site string) {
	u.tm.failover.rotateBinlog(ctx, source, site, outgoingPrimaryRotation)
}

// RotateCandidateBinlogForSim is promoteVerifiedFrom's best-effort rotation
// of the candidate's binary log before its writes are enabled.
func (u *SimOrderedUpdate) RotateCandidateBinlogForSim(ctx context.Context, candidate mysql.Checker, site string) {
	u.tm.failover.rotateBinlog(ctx, candidate, site, promotionRotation)
}

// FinishOrderedUpdateForSim ends the update as its goroutine's deferred
// calls do: the claimed window is finished, the pending flag cleared and
// the worker unregistered. Idempotent.
func (u *SimOrderedUpdate) FinishOrderedUpdateForSim() {
	if u.finished {
		return
	}
	u.finished = true
	tm := u.tm
	if u.claimed {
		tm.finishDeploymentTopologyChange()
		tm.mu.Lock()
		tm.deploymentUpdateRunning = false
		tm.mu.Unlock()
	}
	tm.mu.Lock()
	tm.deploymentUpdatePending = false
	tm.mu.Unlock()
	tm.updateWorkers.Done()
}

// CancelPendingRecloneForSim withdraws a pending reclone request that has
// not started its clone, as the runner does when the annotation is removed.
func (tm *TopologyManager) CancelPendingRecloneForSim(site string) { tm.cancelPendingReclone(site) }

// SetRecloneTailStepRunnerForSim replaces the goroutine that runs each step
// of the pre-reclone archive-tail wait (recloneArchiveTailSettled). A
// simulation passes func(step func()) { step() } to run steps inline, so a
// step's statements land inside the Poll that started it and the model
// sees one actor at a time. Call before the first Poll.
func (tm *TopologyManager) SetRecloneTailStepRunnerForSim(run func(step func())) {
	tm.recloneTailRun = run
}

// WaitRecloneTailStepsForSim blocks until every step goroutine of the
// pre-reclone wait has returned (tests that assert none is left behind).
func (tm *TopologyManager) WaitRecloneTailStepsForSim() { tm.waitRecloneTailWorkers() }

// SetSimHooksForSim installs the simulation's yield hooks on a facade built
// by NewPlannedFailoverTopology. Call before first use.
func SetSimHooksForSim(topo PlannedFailoverTopology, hooks SimHooks) {
	if p, ok := topo.(*runnerPlannedTopology); ok {
		p.hooks = hooks
	}
}

// SetReadyForSim marks tm as having completed a poll, as the first Poll
// does. Simulation and component harnesses only.
func (tm *TopologyManager) SetReadyForSim() {
	tm.mu.Lock()
	tm.ready = true
	tm.lastPollTime = tm.clock.Now()
	tm.mu.Unlock()
}

// Yield points the planned-failover engine and its topology facade pass to
// SimHooks.Yield (§15.1). A statement point carries ":<site>:<statement>".
const (
	YieldEngineLoadForSim    = "engine.load"          // after the uncached Load
	YieldEngineWriteForSim   = "engine.write"         // after each store Write
	YieldEngineSettleForSim  = "engine.settle"        // after each Settle
	YieldPeerProbeForSim     = "facade.peerProbe"     // after a live peer probe (grant, fence, rollback)
	YieldBeforePublishForSim = "facade.beforePublish" // before the grant's publish, which then takes tm.mu
	YieldAfterPublishForSim  = "facade.afterPublish"  // after the grant's publish
	YieldBeforeStmtForSim    = "facade.beforeStmt"    // before a facade statement
	YieldAfterStmtForSim     = "facade.afterStmt"     // after a facade statement
	YieldGateProbeForSim     = "facade.gateProbe"     // after the pre-write gate's probe
	YieldConfirmProbeForSim  = "facade.confirmProbe"  // after the confirm path's peer probe
)

// RetireForSim retires tm as the runner does before unregistering it.
func (tm *TopologyManager) RetireForSim() { tm.retire() }

// ChangingSitesForSim reports whether tm is changing the sites now (a
// claimed promotion or update, or an admitted follower mutation); the
// simulator's registry uses it for ManagerWiring.RetiringBusy.
func (tm *TopologyManager) ChangingSitesForSim() bool { return tm.changingSites() }

// OrderedUpdateClaimedForSim reports whether tm's ordered update has
// claimed its mutation window (ManagerWiring.RetiringUpdateInFlight).
func (tm *TopologyManager) OrderedUpdateClaimedForSim() bool { return tm.orderedUpdateClaimed() }

// SetPredecessorUpdateInFlightForSim sets or clears the predecessor guard,
// as the runner does when a retired manager's update goroutine exits.
func (tm *TopologyManager) SetPredecessorUpdateInFlightForSim(v bool) {
	tm.setPredecessorUpdateInFlight(v)
}

// The TryLock probes report whether a mutex is free, leaving it unlocked.
// The simulator calls them at every yield: a yield must hold no mutex.
func (tm *TopologyManager) TryLockManagerForSim() bool { return tryUnlock(&tm.mu) }

func (tm *TopologyManager) TryLockFailoverStateWriteForSim() bool {
	return tryUnlock(&tm.failoverStateWriteMu)
}

func (tm *TopologyManager) TryLockRestoreProtectionForSim() bool {
	return tryUnlock(&tm.restoreProtectionMu)
}

func (tm *TopologyManager) TryLockRestoreFenceForSim() bool { return tryUnlock(&tm.restoreFenceMu) }

type tryLocker interface {
	TryLock() bool
	Unlock()
}

func tryUnlock(m tryLocker) bool {
	if !m.TryLock() {
		return false
	}
	m.Unlock()
	return true
}

// HeldForSim returns the held claim's run and whether it is a hold;
// ok=false when the slot is empty.
func (s *PlannedClaimSlot) HeldForSim() (runID string, hold, ok bool) {
	c, held := s.snapshot()
	if !held {
		return "", false, false
	}
	return c.runID, c.mode == claimModeHold, true
}

// ReleasesForSim returns the releases recorded since the last call and
// forgets them.
func (s *PlannedClaimSlot) ReleasesForSim() []ClaimReleaseNote {
	if s == nil {
		return nil
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	out := s.releases
	s.releases = nil
	return out
}

// SetTickIntervalForTest sets how often the ordered update polls replica
// and pod readiness. Call before the update starts.
func (u *UpdateController) SetTickIntervalForTest(d time.Duration) { u.tickInterval = d }

// SetRunIDSourceForSim makes e mint run IDs with fn instead of random UUIDs,
// so a simulated trial replays byte for byte. Call before the first Step.
func (e *PlannedFailoverEngine) SetRunIDSourceForSim(fn func() string) { e.runIDSource = fn }

// RestoreFailoverStateForSim is startManager's restore for a simulated
// process: rec becomes the out-of-band recorder, then the observed
// authority, the watermark and the newer failover record are restored from
// fg exactly as restoreFailoverState does. Call before the first Poll.
func (tm *TopologyManager) RestoreFailoverStateForSim(fg *v1alpha1.MysqlFailoverGroup, rec FailoverStateRecorder) {
	tm.SetFailoverStateRecorder(rec)
	tm.restoreFailoverRecordFrom(fg, types.NamespacedName{Namespace: fg.Namespace, Name: fg.Name}, tm.logger)
}

// RestorePlannedFailoverStateForSim is startManager's planned-failover
// restore from an uncached view (§3.7, §6.5): a confirmed run's record and
// the naming of a source the run left fenced. Call after
// RestoreFailoverStateForSim.
func (tm *TopologyManager) RestorePlannedFailoverStateForSim(view *v1alpha1.MysqlFailoverGroup) {
	if view == nil {
		return
	}
	var r *TopologyManagerRunner // the restore reads nothing from the runner
	r.restorePlannedFailoverState(tm, view, types.NamespacedName{Namespace: view.Namespace, Name: view.Name})
}

// PlannedOwnRecordDurableForSim reports whether fg's durable failover
// record is the run pf's own, by §4.4's by-content rule (the DST-3 ruling's
// durable-record half), evaluated at now.
func PlannedOwnRecordDurableForSim(fg *v1alpha1.MysqlFailoverGroup, pf *v1alpha1.PlannedFailoverStatus, now time.Time) bool {
	return plannedOwnRecordDurable(fg, pf, now)
}

// FailoverRecordForSim is tm's in-memory failover record (ok=false: none),
// as the runner's retired-record stash reads it (stashRetiredFailoverLocked).
func (tm *TopologyManager) FailoverRecordForSim() (FailoverRecord, bool) {
	return tm.failoverRecordSnapshot()
}

// PlannedFailoverOwnsSitesForSim is plannedFailoverOwnsSites: fg's unmarked
// run is in a phase that owns the sites.
func PlannedFailoverOwnsSitesForSim(fg *v1alpha1.MysqlFailoverGroup) bool {
	return plannedFailoverOwnsSites(fg)
}

// SetRecloneRunnerForSim replaces the clone of a reclone (the bootstrap
// goroutine: CLONE INSTANCE, the wait for the restart, the replication
// setup) with run, which applies a clone's effect to the simulation's
// model and reports whether it could (the donor and the recipient are up
// and can reach each other). checkReclone's own decisions stay the
// production ones: the request, the donor confirmation, the claims and
// the pre-reclone archive-tail wait. After a clone the recipient has no
// replication channel; old-primary recovery configures it on a later poll,
// as it would after an operator restart between a clone and its
// replication setup. Call before the first Poll.
func (tm *TopologyManager) SetRecloneRunnerForSim(run func(donor, recipient string) bool) {
	tm.recloneForSim = run
}

// finishRecloneForSim runs the simulated clone of recipient from donor and
// settles the request as the bootstrap goroutine does after a clone that
// completed.
func (tm *TopologyManager) finishRecloneForSim(donor string, recipient *siteTracker) bool {
	if !tm.recloneForSim(donor, recipient.name) {
		return false
	}
	tm.mu.Lock()
	if tm.reclonePendingSite == recipient.name {
		tm.reclonePendingSite, tm.reclonePendingOwner = "", ""
	}
	recipient.sourceHost = ""
	recipient.sourceConvergenceState = ""
	recipient.sourceConvergenceReason = ""
	recipient.servingHealthy = false
	tm.mu.Unlock()
	tm.clearRecloneArchiveTail()
	// The bootstrap marks an installed clone on the site's server_uuid
	// timeline (cloneCompletedInProcess).
	tm.markSiteCloned(context.Background(), recipient.name)
	tm.clearOwnLossAfterClone(recipient.name)
	return true
}

// CloneIssuedAndFailedForSim records what the bootstrap records when it
// issues a clone into site, for a clone that then fails or is killed
// before it installs: the production recordCloneIdentity, with the
// server_uuid the site runs under, and nothing after it. A simulation
// uses it to put the trace of a failed clone on the site's server_uuid
// timeline. It reports whether the site's identity could be read.
func (tm *TopologyManager) CloneIssuedAndFailedForSim(site string) bool {
	s := tm.getSite(site)
	if s == nil {
		return false
	}
	ctx := context.Background()
	uuid, err := readServerUUID(ctx, s.mysql)
	if err != nil {
		return false
	}
	tm.knownSiteServerUUIDs(site, uuid)
	return tm.recordCloneIdentity(ctx, site, uuid, nil, nil) == nil
}

// CarryOwnLossReportsForSim is startManager's hand-over of the own-loss
// reports of the manager tm replaces in the same process
// (carryOwnLossReports).
func (tm *TopologyManager) CarryOwnLossReportsForSim(prev *TopologyManager) {
	tm.carryOwnLossReports(prev)
}

// OwnTransactionsLostForSim reports the set a site is recorded to lack of
// the transactions it issued itself ("" when it has no report).
func (tm *TopologyManager) OwnTransactionsLostForSim(site string) string {
	tm.mu.RLock()
	defer tm.mu.RUnlock()
	if rec := tm.ownLoss[site]; rec != nil {
		return rec.lostGtid
	}
	return ""
}

// AppendServerUUIDEntryForSim appends entry to a site's server_uuid
// timeline by the rule the group's store uses (appendServerUUIDEntry), for
// a simulation's SiteIdentityStore.
func AppendServerUUIDEntryForSim(timeline []string, entry string) []string {
	timeline, _ = appendServerUUIDEntry(timeline, entry)
	return timeline
}
