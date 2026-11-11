package controller

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"strings"
	"sync"
	"time"

	"k8s.io/apimachinery/pkg/types"

	v1alpha1 "github.com/shipstream/bloodraven/api/v1alpha1"
	"github.com/shipstream/bloodraven/internal/mysql"
	"github.com/shipstream/bloodraven/internal/state"
)

// The planned-failover topology facade: the engine's only path to the
// topology managers and MySQL. Every mutating call checks the run's claim
// immediately before its first statement (requireRunClaim) and sends
// nothing when that check fails. Reads need no claim.
//
// The facade is process-scoped and owns the run's in-process memos: which
// fence it sent (fence attribution) and how far its promotion statements got
// (stage classification). They are never persisted; a cross-process re-run
// has none and falls back to the durable records plus @@server_uuid.

const (
	// plannedDNSBudget bounds the run's DNS apply (3 attempts).
	plannedDNSBudget   = 10 * time.Second
	plannedDNSAttempts = 3
	// plannedDNSRetryBackoff is the pause before the second attempt; it
	// doubles before the third (500ms, 1s).
	plannedDNSRetryBackoff = 500 * time.Millisecond
)

// promoteMemoStage is how far this process's promotion statements for a
// run got. Each stage is recorded before its statement is sent, so "sent"
// includes a statement whose reply was lost.
type promoteMemoStage int

const (
	promoteMemoNone promoteMemoStage = iota
	promoteMemoStopSent
	promoteMemoResetSent
	promoteMemoSROClearSent
	promoteMemoROClearSent
)

// fenceMemoEntry records that this process sent the run's fence to the
// server with @@server_uuid uuid; an entry exists only once it was sent.
type fenceMemoEntry struct {
	uuid string
}

// runnerPlannedTopology implements PlannedFailoverTopology over a
// ManagerRegistry.
type runnerPlannedTopology struct {
	reg   ManagerRegistry
	hooks SimHooks

	mu          sync.Mutex
	fenceMemo   map[string]fenceMemoEntry
	promoteMemo map[string]promoteMemoStage
	// promoteFlushed holds the runs whose promotion this process already
	// sent FLUSH BINARY LOGS for (acknowledged or not), so a promotion
	// retried over several passes rotates the target's binary log once.
	promoteFlushed map[string]bool
	// sourceFlushed is the same for the fenced source's rotation.
	sourceFlushed map[string]bool
	// sourceUnsealedNoted holds the runs for which this process logged
	// that it proved the fence without rotating the source's binary log
	// (noteSourceNotSealed), so the line appears once per run.
	sourceUnsealedNoted map[string]bool
	// sourceLoss and rollbackTargetLoss are the first failed read of a
	// run's loss window inside the facade. sourceLoss is fed by every
	// source observation the facade makes for the run (the drain's kill
	// pass, the lag probe, the pre-write gate), so the gate continues the
	// window the earlier phases opened, as the engine's own window does
	// (§6.2); rollbackTargetLoss is the target at a rollback's
	// precondition. One failed read is no evidence of a lost server; the
	// window decides.
	sourceLoss         map[string]time.Time
	rollbackTargetLoss map[string]time.Time
}

// NewPlannedFailoverTopology returns the production facade over reg. The
// runner builds one per process; the simulator builds one per simulated
// process.
func NewPlannedFailoverTopology(reg ManagerRegistry) PlannedFailoverTopology {
	return newRunnerPlannedTopology(reg)
}

func newRunnerPlannedTopology(reg ManagerRegistry) *runnerPlannedTopology {
	return &runnerPlannedTopology{
		reg:                 reg,
		fenceMemo:           make(map[string]fenceMemoEntry),
		promoteMemo:         make(map[string]promoteMemoStage),
		promoteFlushed:      make(map[string]bool),
		sourceFlushed:       make(map[string]bool),
		sourceUnsealedNoted: make(map[string]bool),
		sourceLoss:          make(map[string]time.Time),
		rollbackTargetLoss:  make(map[string]time.Time),
	}
}

var _ PlannedFailoverTopology = (*runnerPlannedTopology)(nil)

func (p *runnerPlannedTopology) leader() context.Context { return p.reg.LeaderContext() }

// manager returns nn's current manager, or errManagerAbsent.
func (p *runnerPlannedTopology) manager(nn types.NamespacedName) (*TopologyManager, error) {
	if tm := p.reg.Snapshot(nn).Current; tm != nil {
		return tm, nil
	}
	return nil, errManagerAbsent
}

func (p *runnerPlannedTopology) fenceMemoFor(runID string) (fenceMemoEntry, bool) {
	p.mu.Lock()
	defer p.mu.Unlock()
	e, ok := p.fenceMemo[runID]
	return e, ok
}

func (p *runnerPlannedTopology) noteFenceSent(runID, uuid string) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.fenceMemo[runID] = fenceMemoEntry{uuid: uuid}
}

// forgetRun drops every in-process memo of runID. It is called when the
// run's claim is released or becomes a hold: from then on the run sends
// nothing, so nothing reads them, and a long-lived leader must not keep
// one entry per run it ever drove.
func (p *runnerPlannedTopology) forgetRun(runID string) {
	p.mu.Lock()
	defer p.mu.Unlock()
	delete(p.fenceMemo, runID)
	delete(p.promoteMemo, runID)
	delete(p.promoteFlushed, runID)
	delete(p.sourceFlushed, runID)
	delete(p.sourceUnsealedNoted, runID)
	delete(p.sourceLoss, runID)
	delete(p.rollbackTargetLoss, runID)
}

// lossExpired records a failed read of runID's server in window (the
// first failure starts it) and reports whether plannedFailoverLossWindow
// has passed since, or the hard deadline (zero: none) is due before the
// engine's next poll.
func (p *runnerPlannedTopology) lossExpired(window map[string]time.Time, runID string, now, deadline time.Time) bool {
	p.mu.Lock()
	defer p.mu.Unlock()
	since, ok := window[runID]
	if !ok {
		since = now
		window[runID] = now
	}
	if !deadline.IsZero() && !now.Add(plannedRequeuePoll).Before(deadline) {
		return true
	}
	return now.Sub(since) >= plannedFailoverLossWindow
}

// noteSourceObserved feeds one source observation of runID into its
// source-loss window.
func (p *runnerPlannedTopology) noteSourceObserved(runID string, reachable bool, now time.Time) {
	if reachable {
		p.lossRecovered(p.sourceLoss, runID)
		return
	}
	p.lossExpired(p.sourceLoss, runID, now, time.Time{})
}

// lossRecovered closes runID's loss window after a successful read.
func (p *runnerPlannedTopology) lossRecovered(window map[string]time.Time, runID string) {
	p.mu.Lock()
	defer p.mu.Unlock()
	delete(window, runID)
}

// firstFlush reports whether this process has not yet sent the rotation
// that memo tracks for runID, and records that it now has.
func (p *runnerPlannedTopology) firstFlush(memo map[string]bool, runID string) bool {
	p.mu.Lock()
	defer p.mu.Unlock()
	if memo[runID] {
		return false
	}
	memo[runID] = true
	return true
}

// promoteMemoFor returns how far this process's promotion of runID got,
// and whether this process has any promotion memo for it at all.
func (p *runnerPlannedTopology) promoteMemoFor(runID string) (promoteMemoStage, bool) {
	p.mu.Lock()
	defer p.mu.Unlock()
	s, ok := p.promoteMemo[runID]
	return s, ok
}

func (p *runnerPlannedTopology) startPromoteMemo(runID string) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if _, ok := p.promoteMemo[runID]; !ok {
		p.promoteMemo[runID] = promoteMemoNone
	}
}

// notePromoteSent advances runID's memo. A process with no memo for the
// run (a cross-process re-run) keeps none: its memo could not prove what an
// earlier process sent.
func (p *runnerPlannedTopology) notePromoteSent(runID string, s promoteMemoStage) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if cur, ok := p.promoteMemo[runID]; ok && cur < s {
		p.promoteMemo[runID] = s
	}
}

// memoProvesNoWrite reports whether this process's memo proves the run
// never sent super_read_only=OFF to the target.
func (p *runnerPlannedTopology) memoProvesNoWrite(runID string) bool {
	s, ok := p.promoteMemoFor(runID)
	return ok && s < promoteMemoSROClearSent
}

// ---------- claim ----------

// ClaimPlannedFailover is §3.2: step 0 (the retiring managers, with no
// lock held), then the current manager's claimPlannedFailover.
func (p *runnerPlannedTopology) ClaimPlannedFailover(ctx context.Context, nn types.NamespacedName, req PlannedClaimRequest) (PlannedClaimDecision, error) {
	if l := p.leader(); l != nil && l.Err() != nil {
		return PlannedClaimDecision{}, errLeaderLost
	}
	snap := p.reg.Snapshot(nn)
	if snap.Current == nil {
		return claimDecisionWait(v1alpha1.PlannedFailoverWaitManagerAbsent, "no topology manager is registered"), nil
	}
	if d, busy := retiringManagersBusy(snap); busy {
		return d, nil
	}
	return snap.Current.claimPlannedFailover(ctx, req, p.hooks)
}

// retiringManagersBusy is §3.2 step 0, shared by the grant and the
// hydration: a retiring manager still changing the sites, or still
// holding an ordered update, keeps any claim from being published.
func retiringManagersBusy(snap RegistrySnapshot) (PlannedClaimDecision, bool) {
	for _, old := range snap.Retiring {
		if old.changingSites() || old.orderedUpdateClaimed() {
			return claimDecisionWait(v1alpha1.PlannedFailoverWaitPredecessorBusy, "a replaced topology manager is still changing the sites"), true
		}
	}
	return PlannedClaimDecision{}, false
}

// EnsureClaim is the engine's hydration (§3.4) for an owns-sites pass: it
// returns Granted when the slot holds the run, waits while another run or
// conflicting work holds the sites, and otherwise hydrates the run's
// claim with needsReprove from fg, the pass's uncached read. A run that is
// not live in fg (marked, another run's, owning no sites), or whose claim
// was released after it recorded its promotion, gets errClaimLost.
//
// For a live run with no current manager it returns Wait{ManagerAbsent},
// after hydrating an empty slot so the next manager is blocked from its
// first poll: the run can send nothing, and the manager-absence deadline
// (§6.3) must keep running rather than restart on a Granted pass.
//
// Hydration on a live manager is published the way a grant is (§3.2 step
// 4): through the follower-mutation gate, so no recovery, drain or source
// convergence admitted earlier is still running on the sites, and under
// tm.mu against the manager-local work a grant also waits for. The
// grant's refusals (unknown or unpromotable target, divergence, the
// active site) are not repeated: the run is past its grant, and its
// re-proof (§4.4) decides those before its next side effect.
func (p *runnerPlannedTopology) EnsureClaim(ctx context.Context, ref PlannedRunRef, fg *v1alpha1.MysqlFailoverGroup) (PlannedClaimDecision, error) {
	if l := p.leader(); l != nil && l.Err() != nil {
		return PlannedClaimDecision{}, errLeaderLost
	}
	pf, marked := plannedFailoverRun(fg)
	if pf == nil || marked || pf.RunID != ref.RunID || !plannedFailoverOwnsSites(fg) {
		return PlannedClaimDecision{}, errClaimLost
	}
	snap := p.reg.Snapshot(ref.Group)
	slot, tm := snap.Slot, snap.Current
	now := time.Now()
	if tm != nil {
		now = tm.clock.Now()
	}
	held, ok := slot.snapshot()
	if (!ok || held.runID != ref.RunID) && !PlannedRunNeedsClaim(fg, now) {
		// A recorded promotion released its claim; it is never hydrated
		// back (O-m1). The engine publishes it without a claim.
		return PlannedClaimDecision{}, errClaimLost
	}
	c, hydratable := hydratedRunClaim(pf)
	if tm == nil {
		// No other topology actor exists without a manager. An empty slot
		// is filled now.
		if !ok && hydratable && slot != nil {
			slot.hydrate(c)
		}
		return claimDecisionWait(v1alpha1.PlannedFailoverWaitManagerAbsent, "no topology manager is registered"), nil
	}
	if ok {
		switch {
		case held.mode == claimModeHold:
			return claimDecisionReject(v1alpha1.PlannedFailoverReasonManualRecoveryPending,
				"automatic failover is held after planned failover run %s", held.runID), nil
		case held.runID != ref.RunID:
			return claimDecisionWait(v1alpha1.PlannedFailoverWaitClaimHeld, "planned failover run %s holds the claim", held.runID), nil
		}
		return claimDecisionGranted(&held), nil
	}
	if !hydratable {
		return PlannedClaimDecision{}, errClaimLost
	}
	if d, busy := retiringManagersBusy(snap); busy {
		return d, nil
	}
	if tm.predecessorMutating() {
		return claimDecisionWait(v1alpha1.PlannedFailoverWaitPredecessorBusy, "a replaced topology manager is still changing the sites"), nil
	}
	var (
		d              PlannedClaimDecision
		decided, raced bool
	)
	if err := tm.followerMutationGate.tryAdmit(ref.RunID, plannedClaimGateTTL, func() {
		d, decided, raced = tm.hydratePlannedClaim(slot, c, ref.Target)
	}); err != nil {
		return claimDecisionWait(v1alpha1.PlannedFailoverWaitRecoveryInFlight, "a recovery or source-convergence step is still running on the sites"), nil
	}
	if raced {
		// Another claim landed between the check above and the lock;
		// decide again against it.
		return p.EnsureClaim(ctx, ref, fg)
	}
	if decided {
		return d, nil
	}
	return claimDecisionGranted(&c), nil
}

// hydratePlannedClaim installs c in slot under tm.mu unless manager-local
// work a grant would also wait for holds the sites (decided, with the
// Wait), or the slot is no longer empty (raced). The caller holds the
// follower-mutation gate.
func (tm *TopologyManager) hydratePlannedClaim(slot *PlannedClaimSlot, c plannedClaim, target string) (d PlannedClaimDecision, decided, raced bool) {
	tm.mu.Lock()
	defer tm.mu.Unlock()
	switch {
	case tm.plannedSlot != slot:
		return claimDecisionWait(v1alpha1.PlannedFailoverWaitManagerNotReady, "the topology manager is not wired to the group's claim slot"), true, false
	case tm.retired:
		return claimDecisionWait(v1alpha1.PlannedFailoverWaitManagerRetiring, "the topology manager is being replaced"), true, false
	case tm.deploymentChanging > 0 || tm.promotionUnobservedLocked():
		return claimDecisionWait(v1alpha1.PlannedFailoverWaitPromotionInFlight, "a promotion is in flight or not yet observed"), true, false
	case tm.deploymentUpdateRunning || tm.predecessorUpdateInFlight:
		return claimDecisionWait(v1alpha1.PlannedFailoverWaitUpdateInFlight, "an ordered update holds the sites"), true, false
	case tm.topologyFrozen:
		return claimDecisionWait(v1alpha1.PlannedFailoverWaitRestoreInFlight, "an in-place restore holds the sites"), true, false
	case tm.bootstrapBlocksCrossSiteLocked():
		return claimDecisionWait(v1alpha1.PlannedFailoverWaitBootstrapInFlight, "a bootstrap of a promotable site is running"), true, false
	case slices.Contains(tm.keyringRotationReserved, target):
		return claimDecisionWait(v1alpha1.PlannedFailoverWaitKeyringRotation, "a keyring rotation of %q is being admitted", target), true, false
	}
	if !slot.hydrate(c) {
		return PlannedClaimDecision{}, false, true
	}
	tm.crossSiteRetry = true
	return PlannedClaimDecision{}, false, false
}

// ReleasePlannedFailoverClaim releases the claim only when runID holds it,
// and records why (§3.6). It takes slot.mu only, then tm.mu to arm the
// manager's cross-site retry, so the deferred decisions run again.
func (p *runnerPlannedTopology) ReleasePlannedFailoverClaim(nn types.NamespacedName, runID string, why ClaimReleaseReason) bool {
	if why == ReleaseToHold {
		return p.ConvertToHold(nn, runID)
	}
	snap := p.reg.Snapshot(nn)
	if !snap.Slot.release(runID, why) {
		return false
	}
	p.forgetRun(runID)
	if tm := snap.Current; tm != nil {
		tm.armCrossSiteRetry()
		tm.logger.Info("planned failover: claim released", "runId", runID, "why", string(why))
	}
	return true
}

// ConvertToHold turns runID's claim into an automation hold (why=toHold).
// sites are the hold's ambiguous sites when the caller knows them (logged
// comma-separated, as the engine logs them).
func (p *runnerPlannedTopology) ConvertToHold(nn types.NamespacedName, runID string, sites ...string) bool {
	snap := p.reg.Snapshot(nn)
	if !snap.Slot.toHold(runID) {
		return false
	}
	p.forgetRun(runID)
	if tm := snap.Current; tm != nil {
		tm.logger.Warn("planned failover: automation held pending acknowledgement", "runId", runID, "sites", strings.Join(sites, ","))
	}
	return true
}

// PlannedClaimHeld reports whether nn's slot holds a claim in either mode.
func (p *runnerPlannedTopology) PlannedClaimHeld(nn types.NamespacedName) bool {
	snap := p.reg.Snapshot(nn)
	if snap.Slot.Held() {
		return true
	}
	return snap.Current != nil && snap.Current.plannedClaimHeld()
}

// HeldHoldClaim returns the run of nn's Hold-mode claim ("" for a hold
// rebuilt from a condition that names no run).
func (p *runnerPlannedTopology) HeldHoldClaim(nn types.NamespacedName) (string, bool) {
	held, ok := p.reg.Snapshot(nn).Slot.snapshot()
	if !ok || held.mode != claimModeHold {
		return "", false
	}
	return held.runID, true
}

// HeldRunClaim returns the run of nn's Run-mode claim.
func (p *runnerPlannedTopology) HeldRunClaim(nn types.NamespacedName) (string, bool) {
	held, ok := p.reg.Snapshot(nn).Slot.snapshot()
	if !ok || held.mode != claimModeRun {
		return "", false
	}
	return held.runID, true
}

// ManagerAuthority is the current manager's active site and topology
// generation, as its next snapshot would carry them.
func (p *runnerPlannedTopology) ManagerAuthority(nn types.NamespacedName) (PlannedAuthority, bool) {
	tm := p.reg.Snapshot(nn).Current
	if tm == nil {
		return PlannedAuthority{}, false
	}
	tm.mu.RLock()
	defer tm.mu.RUnlock()
	return PlannedAuthority{ActiveSite: tm.effectiveActiveSiteLocked(), DeploymentActiveSite: tm.deploymentActiveSite,
		TopologyGeneration: tm.topologyGeneration}, true
}

// ActiveSite is the current manager's effective active site.
func (p *runnerPlannedTopology) ActiveSite(nn types.NamespacedName) (string, bool) {
	tm := p.reg.Snapshot(nn).Current
	if tm == nil {
		return "", false
	}
	tm.mu.RLock()
	defer tm.mu.RUnlock()
	return tm.effectiveActiveSiteLocked(), true
}

// ---------- probes ----------

// plannedProbe is one site's live read_only answer.
type plannedProbe struct {
	name     string
	role     state.SiteRole
	readOnly bool
	err      error
}

// plannedProbeSites reads read_only on every site but skip, concurrently,
// each bounded by withMySQLSafetyTimeout. Site config is immutable after
// construction, so no lock is held.
func (tm *TopologyManager) plannedProbeSites(ctx context.Context, skip ...string) []plannedProbe {
	out := make([]plannedProbe, 0, len(tm.sites))
	var mu sync.Mutex
	var wg sync.WaitGroup
	for i := range tm.sites {
		site := &tm.sites[i]
		if slices.Contains(skip, site.name) {
			continue
		}
		wg.Add(1)
		go func(site *siteTracker) {
			defer wg.Done()
			var ro bool
			err := withMySQLSafetyTimeout(ctx, func(probeCtx context.Context) error {
				var err error
				ro, err = site.mysql.CheckReadOnly(probeCtx)
				return err
			})
			mu.Lock()
			out = append(out, plannedProbe{name: site.name, role: site.role, readOnly: ro, err: err})
			mu.Unlock()
		}(site)
	}
	wg.Wait()
	slices.SortFunc(out, func(a, b plannedProbe) int { return strings.Compare(a.name, b.name) })
	return out
}

// plannedPeerVerdict classifies a probe set by §4.1's site classes.
type plannedPeerVerdict struct {
	writable          []string // any class, positively writable
	unreadable        []string // primary-candidate or dr-only, no answer
	unreadableReaders []string // read-only-role sites, no answer (allowed)
}

func classifyPlannedPeers(probes []plannedProbe) plannedPeerVerdict {
	var v plannedPeerVerdict
	for _, p := range probes {
		switch {
		case p.err == nil && !p.readOnly:
			v.writable = append(v.writable, p.name)
		case p.err != nil && p.role == state.SiteRoleReadOnly:
			v.unreadableReaders = append(v.unreadableReaders, p.name)
		case p.err != nil:
			v.unreadable = append(v.unreadable, p.name)
		}
	}
	return v
}

func (v plannedPeerVerdict) allUnreadable() []string {
	return append(slices.Clone(v.unreadable), v.unreadableReaders...)
}

// plannedEvidence is plannedGrantEvidence's result: a Wait decision, or
// the source's @@server_uuid when the evidence holds.
type plannedEvidence struct {
	wait       *PlannedClaimDecision
	sourceUUID string
}

// plannedGrantEvidence is §4.1 at the grant: the source writable (and its
// @@server_uuid from a pooled snapshot), the target a healthy read-only
// replica of the source, every other primary-candidate and dr-only site
// positively read-only, and no reader writable. It owns nothing.
func (tm *TopologyManager) plannedGrantEvidence(ctx context.Context, source, target string) plannedEvidence {
	wait := func(reason v1alpha1.PlannedFailoverWaitReason, format string, args ...any) plannedEvidence {
		d := claimDecisionWait(reason, format, args...)
		return plannedEvidence{wait: &d}
	}
	src, tgt := tm.getSite(source), tm.getSite(target)
	if src == nil || tgt == nil {
		return wait(v1alpha1.PlannedFailoverWaitAuthorityUnsettled, "source %q or target %q is not in the topology", source, target)
	}
	var (
		wg      sync.WaitGroup
		srcUUID string
		srcErr  error
		repl    *mysql.ReplicaStatus
		replErr error
	)
	wg.Add(2)
	go func() {
		defer wg.Done()
		srcErr = withMySQLSafetyTimeout(ctx, func(probeCtx context.Context) error {
			r, ok := src.mysql.(gtidSnapshotReader)
			if !ok {
				return mysql.ErrSessionUnsupported
			}
			snap, err := r.ReadGTIDSnapshot(probeCtx)
			srcUUID = snap.ServerUUID
			return err
		})
	}()
	go func() {
		defer wg.Done()
		replErr = withMySQLSafetyTimeout(ctx, func(probeCtx context.Context) error {
			var err error
			repl, err = tgt.mysql.ShowReplicaStatus(probeCtx)
			return err
		})
	}()
	probes := tm.plannedProbeSites(ctx)
	wg.Wait()

	for _, pr := range probes {
		switch pr.name {
		case source:
			switch {
			case pr.err != nil:
				return wait(v1alpha1.PlannedFailoverWaitPeerUnreadable, "the source %q did not answer: %v", source, pr.err)
			case pr.readOnly:
				return wait(v1alpha1.PlannedFailoverWaitAuthorityUnsettled, "the source %q reads read-only", source)
			}
		case target:
			switch {
			case pr.err != nil:
				return wait(v1alpha1.PlannedFailoverWaitTargetNotReady, "the target %q did not answer: %v", target, pr.err)
			case !pr.readOnly:
				return wait(v1alpha1.PlannedFailoverWaitAuthorityUnsettled, "the target %q reads writable", target)
			}
		default:
			switch {
			case pr.err == nil && !pr.readOnly:
				return wait(v1alpha1.PlannedFailoverWaitAuthorityUnsettled, "site %q reads writable beside the source", pr.name)
			case pr.err != nil && pr.role != state.SiteRoleReadOnly:
				return wait(v1alpha1.PlannedFailoverWaitPeerUnreadable, "site %q did not answer: %v", pr.name, pr.err)
			}
		}
	}
	switch {
	case srcErr != nil || srcUUID == "":
		return wait(v1alpha1.PlannedFailoverWaitPeerUnreadable, "could not read the source's server_uuid: %v", srcErr)
	case replErr != nil:
		return wait(v1alpha1.PlannedFailoverWaitTargetNotReady, "could not read the target's replication status: %v", replErr)
	case repl == nil:
		return wait(v1alpha1.PlannedFailoverWaitTargetNotReady, "the target %q has no replication channel", target)
	case !repl.IORunning || !repl.SQLRunning:
		return wait(v1alpha1.PlannedFailoverWaitTargetNotReady, "the target %q's replication threads are not both running", target)
	case repl.LastSQLError != "":
		return wait(v1alpha1.PlannedFailoverWaitTargetNotReady, "the target %q's applier reports an error: %s", target, repl.LastSQLError)
	case canonicalSourceHost(repl.SourceHost) != canonicalSourceHost(src.host):
		return wait(v1alpha1.PlannedFailoverWaitTargetNotReady, "the target %q replicates from %q, not the source %q", target, repl.SourceHost, source)
	}
	return plannedEvidence{sourceUUID: srcUUID}
}

// sourceIdentity is the source's live read_only, super_read_only and
// @@server_uuid.
type sourceIdentity struct {
	readOnly, superReadOnly bool
	uuid                    string
	gtid                    string
}

func readSourceIdentity(ctx context.Context, s interface {
	CheckReadOnly(context.Context) (bool, error)
	CheckSuperReadOnly(context.Context) (bool, error)
	ReadGTIDSnapshot(context.Context) (mysql.GTIDSnapshot, error)
}) (sourceIdentity, error) {
	var id sourceIdentity
	err := withMySQLSafetyTimeout(ctx, func(probeCtx context.Context) error {
		var err error
		if id.readOnly, err = s.CheckReadOnly(probeCtx); err != nil {
			return err
		}
		if id.superReadOnly, err = s.CheckSuperReadOnly(probeCtx); err != nil {
			return err
		}
		snap, err := s.ReadGTIDSnapshot(probeCtx)
		id.uuid, id.gtid = snap.ServerUUID, snap.GtidExecuted
		return err
	})
	return id, err
}

// pooledIdentity adapts a Checker for readSourceIdentity; the optional
// methods fail with ErrSessionUnsupported when the checker lacks them.
type pooledIdentity struct{ c mysql.Checker }

func (p pooledIdentity) CheckReadOnly(ctx context.Context) (bool, error) {
	return p.c.CheckReadOnly(ctx)
}
func (p pooledIdentity) CheckSuperReadOnly(ctx context.Context) (bool, error) {
	if c, ok := p.c.(superReadOnlyChecker); ok {
		return c.CheckSuperReadOnly(ctx)
	}
	return false, mysql.ErrSessionUnsupported
}

// ReadGTIDSnapshot falls back to GetGtidExecuted with an unknown identity
// (ServerUUID "") for a checker without the combined read; an unknown
// identity never matches a recorded @@server_uuid, so attribution then
// fails safe.
func (p pooledIdentity) ReadGTIDSnapshot(ctx context.Context) (mysql.GTIDSnapshot, error) {
	if c, ok := p.c.(gtidSnapshotReader); ok {
		return c.ReadGTIDSnapshot(ctx)
	}
	gtid, err := p.c.GetGtidExecuted(ctx)
	return mysql.GTIDSnapshot{GtidExecuted: gtid}, err
}

// ---------- fence ----------

// Fence is §4.2 plus §4.3 on one pinned source session: re-prove
// authority, check the source is still the intent's server and writable,
// SET super_read_only=ON, read it back, snapshot the fenced set, then run
// the post-fence proof. The fence is not transactional: nothing is undone
// on error; the run's records and the rollback decide.
//
// A source that already reads read-only is never reported "nothing was
// sent" before its super_read_only and @@server_uuid were read and §4.3's
// attribution (this process's memo, or the intent's uuid) applied: the run's
// own fence, set by an earlier attempt whose readback or Draining write was
// lost, goes on to the post-fence proof. A failed identity read is a source
// that did not answer (the loss window), never "nothing was sent".
func (p *runnerPlannedTopology) Fence(ctx context.Context, ref PlannedRunRef, sourceServerUUID string) (FenceProof, error) {
	proof, err := p.fence(ctx, ref, sourceServerUUID)
	if _, ok := p.fenceMemoFor(ref.RunID); ok {
		proof.Sent = true
	}
	return proof, err
}

func (p *runnerPlannedTopology) fence(ctx context.Context, ref PlannedRunRef, sourceServerUUID string) (FenceProof, error) {
	tm, err := p.manager(ref.Group)
	if err != nil {
		return FenceProof{}, err
	}
	if err := tm.requireRunClaim(p.leader(), ref, false); err != nil {
		return FenceProof{}, err
	}
	src := tm.getSite(ref.Source)
	if src == nil {
		return FenceProof{}, errSiteNotFound
	}
	intent := &v1alpha1.PlannedFailoverFenceRecord{State: v1alpha1.PlannedFailoverFenceIntended, SourceServerUUID: sourceServerUUID}
	// attributed decides a source found already fenced by §4.3's rule.
	attributed := func(id sourceIdentity) (FenceProof, bool) {
		if !id.readOnly || !id.superReadOnly {
			return FenceProof{}, false
		}
		by := p.fenceAttribution(ref.RunID, intent, id.uuid)
		if by == FenceUnattributed {
			return FenceProof{Kind: FenceNotAttributable, Reason: v1alpha1.PlannedFailoverReasonAuthorityChanged, SourceServerUUID: id.uuid}, true
		}
		p.noteSourceNotSealed(tm, ref, by)
		return p.postFenceProof(ctx, tm, ref, id, by), true
	}
	// A re-run whose fence may already be in place (a lost Draining write,
	// or a new process). The durable intent named sourceServerUUID before
	// this call.
	id, err := readSourceIdentity(ctx, pooledIdentity{src.mysql})
	switch {
	case err != nil && !errors.Is(err, mysql.ErrSessionUnsupported):
		return FenceProof{Kind: FenceSourceUnreachable, Reason: v1alpha1.PlannedFailoverReasonSourceLost}, nil
	case err == nil:
		if proof, ok := attributed(id); ok {
			return proof, nil
		}
	}
	// §4.2: the source must still be the unique writable site.
	probes := tm.plannedProbeSites(ctx)
	p.hooks.yield(YieldPeerProbeForSim)
	var srcProbe plannedProbe
	var others []plannedProbe
	for _, pr := range probes {
		if pr.name == ref.Source {
			srcProbe = pr
		} else {
			others = append(others, pr)
		}
	}
	if srcProbe.err != nil {
		return FenceProof{Kind: FenceSourceUnreachable, Reason: v1alpha1.PlannedFailoverReasonSourceLost}, nil
	}
	v := classifyPlannedPeers(others)
	switch {
	case srcProbe.readOnly:
		// Read-only since the identity read: decide by attribution before
		// reporting that nothing was sent.
		id, err := readSourceIdentity(ctx, pooledIdentity{src.mysql})
		if err != nil && !errors.Is(err, mysql.ErrSessionUnsupported) {
			return FenceProof{Kind: FenceSourceUnreachable, Reason: v1alpha1.PlannedFailoverReasonSourceLost}, nil
		}
		if err == nil {
			if proof, ok := attributed(id); ok {
				return proof, nil
			}
		}
		return FenceProof{Kind: FenceNotSent, Reason: v1alpha1.PlannedFailoverReasonAuthorityChanged, SourceServerUUID: id.uuid}, nil
	case len(v.writable) > 0:
		return FenceProof{Kind: FenceNotSent, Reason: v1alpha1.PlannedFailoverReasonAuthorityChanged, SourceWritable: true}, nil
	case len(v.unreadable) > 0:
		// Strict evidence: an unreadable primary-candidate or dr-only
		// peer may be a writable site the operator cannot see. Nothing
		// is sent; the engine retries until fenceDeadline.
		return FenceProof{Kind: FenceRetry, Reason: v1alpha1.PlannedFailoverReasonPeerUnreadable, UnreadablePeers: v.unreadable}, nil
	}

	sess, err := src.mysql.PinSession(ctx)
	if err != nil {
		return FenceProof{Kind: FenceSourceUnreachable, Reason: v1alpha1.PlannedFailoverReasonSourceLost}, nil
	}
	defer func() { _ = sess.Close() }()
	id, err = readSourceIdentity(ctx, sess)
	if err != nil {
		return FenceProof{Kind: FenceSourceUnreachable, Reason: v1alpha1.PlannedFailoverReasonSourceLost}, nil
	}
	switch {
	case id.uuid != sourceServerUUID:
		// The source was replaced since the grant; it is untouched.
		return FenceProof{Kind: FenceNotSent, Reason: v1alpha1.PlannedFailoverReasonAuthorityChanged,
			SourceWritable: !id.readOnly, SourceServerUUID: id.uuid}, nil
	case id.readOnly:
		if proof, ok := attributed(id); ok {
			return proof, nil
		}
		return FenceProof{Kind: FenceNotSent, Reason: v1alpha1.PlannedFailoverReasonAuthorityChanged, SourceServerUUID: id.uuid}, nil
	}
	if err := tm.requireRunClaim(p.leader(), ref, false); err != nil {
		return FenceProof{}, err
	}
	p.hooks.stmt(true, ref.Source, "super_read_only=ON")
	// Recorded before the statement: a SET whose reply is lost may still
	// have fenced the source.
	p.noteFenceSent(ref.RunID, id.uuid)
	setErr := withMySQLSafetyTimeout(ctx, func(c context.Context) error { return sess.SetSuperReadOnly(c, true) })
	p.hooks.stmt(false, ref.Source, "super_read_only=ON")
	if setErr != nil {
		return FenceProof{Kind: FenceRetry, Reason: v1alpha1.PlannedFailoverReasonFenceFailed, SourceServerUUID: id.uuid}, nil
	}
	after, err := readSourceIdentity(ctx, sess)
	if err != nil || !after.superReadOnly || after.uuid != id.uuid {
		return FenceProof{Kind: FenceRetry, Reason: v1alpha1.PlannedFailoverReasonFenceFailed, SourceServerUUID: id.uuid}, nil
	}
	p.sealFencedSource(ctx, tm, ref, src)
	return p.postFenceProof(ctx, tm, ref, after, FenceByMemo), nil
}

// sealFencedSource rotates the fenced source's binary log, best effort
// (FailoverController.rotateBinlog): the source is read-only and its
// executed set is final, so the file the rotation closes holds the last
// transactions it took as the primary, and its sidecar can archive that
// file although the site no longer is one. Without it the tail stays in
// the active file until some later rotation, which an early reclone of the
// site would never see.
//
// It runs at most once per run and process, on a connection of its own,
// and only while the run still holds the claim. Its outcome changes
// nothing: the run continues the same whether the rotation was
// acknowledged, failed or was abandoned at its bound.
//
// Only the pass that sent the fence and read it back gets here, so some
// runs never rotate their source:
//
//   - the fence statement applied but its reply or its read-back was lost
//     (FenceRetry): the next pass proves the fence by this process's memo
//     and sends nothing;
//   - the operator restarted between the fence and its Draining record: the
//     new process proves the fence by the durable intent and the server's
//     @@server_uuid. Whether the dead process rotated the source first is
//     not known to it;
//   - the run lost its claim while the fence was in flight (the check
//     below): the run sends nothing more, and its fence is the topology
//     manager's to heal.
//
// The first two log noteSourceNotSealed's line. In every case the tail
// stays in the source's active file until its next rotation: a natural one,
// the pre-reclone rotation (recloneArchiveTailSettled), or the promotion
// rotation if the site becomes the primary again.
//
// The claim check and the statement are not atomic: leadership can be
// lost, or the claim released by another goroutine of this process, between
// the two. The rotation then reaches a read-only source the run no longer
// owns, which is one more binary log file there and nothing else.
//
// The once-per-run memo matters when the fence is sent twice in one
// process: an external writer cleared the run's fence between two passes
// (ProveFence reports the source writable and the engine fences it again
// through Fence). The second fence is not followed by a second rotation,
// so a run whose fence is fought over cannot add a file, and up to
// BinlogRotationTimeout of write pause, per pass. The transactions the
// source took between the two fences then stay in its active file, as in
// the cases above.
func (p *runnerPlannedTopology) sealFencedSource(ctx context.Context, tm *TopologyManager, ref PlannedRunRef, src *siteTracker) {
	if tm.requireRunClaim(p.leader(), ref, false) != nil {
		return
	}
	if !p.firstFlush(p.sourceFlushed, ref.RunID) {
		return
	}
	p.hooks.stmt(true, ref.Source, "FLUSH BINARY LOGS")
	tm.failover.rotateBinlog(ctx, src.mysql, ref.Source, outgoingPrimaryRotation, "runId", ref.RunID)
	p.hooks.stmt(false, ref.Source, "FLUSH BINARY LOGS")
}

// The reasons of noteSourceNotSealed's log line (in the log schema).
const (
	// sourceNotSealedFenceReplyLost: this process sent the fence, did not
	// read it back in that pass, and proved it on a later one.
	sourceNotSealedFenceReplyLost = "fenceReplyLost"
	// sourceNotSealedEarlierProcess: an earlier operator process sent the
	// fence; this one proved it from the durable intent.
	sourceNotSealedEarlierProcess = "fenceSentByEarlierProcess"
)

// noteSourceNotSealed logs, once per run and process, that this process
// proved the run's fence by attribution (by) without having rotated the
// source's binary log: sealFencedSource runs only in the pass that sends
// the fence and reads it back. Nothing is sent here. A rotation costs
// write-pause time, the pass that proves a fence by attribution did not
// see the source become read-only, and the tail is sealed by the source's
// next rotation anyway; the line is what tells an operator that the
// outgoing primary's last binary log is not sealed yet.
func (p *runnerPlannedTopology) noteSourceNotSealed(tm *TopologyManager, ref PlannedRunRef, by FenceAttribution) {
	p.mu.Lock()
	if p.sourceFlushed[ref.RunID] || p.sourceUnsealedNoted[ref.RunID] {
		p.mu.Unlock()
		return
	}
	p.sourceUnsealedNoted[ref.RunID] = true
	p.mu.Unlock()
	reason := sourceNotSealedEarlierProcess
	if by == FenceByMemo {
		reason = sourceNotSealedFenceReplyLost
	}
	tm.logger.Info("planned failover: fenced source binlog not rotated", "runId", ref.RunID, "site", ref.Source, "reason", reason)
}

// postFenceProof re-runs the probe set after the fence (§4.3): a writable
// peer of any class is FenceForeignWritable; an unreadable promotable peer
// is recorded and enforced at the pre-write gate.
func (p *runnerPlannedTopology) postFenceProof(ctx context.Context, tm *TopologyManager, ref PlannedRunRef, id sourceIdentity, by FenceAttribution) FenceProof {
	v := classifyPlannedPeers(tm.plannedProbeSites(ctx, ref.Source))
	proof := FenceProof{Kind: FenceApplied, SourceServerUUID: id.uuid, GtidSet: id.gtid, Attribution: by, UnreadablePeers: v.allUnreadable()}
	if len(v.writable) > 0 {
		proof.Kind = FenceForeignWritable
		proof.Reason = v1alpha1.PlannedFailoverReasonForeignWritable
	}
	return proof
}

// ProveFence re-proves a fence whose intent is durable but whose result
// is not (§4.3 attribution on a re-run), or re-checks one after a gap.
// super_read_only=ON is the run's only if this process's memo says it sent
// the fence to this server, or (cross process) the intent is durable and
// the server's @@server_uuid is the intent's. A writable source returns
// FenceRetry with SourceWritable: the engine fences it again through Fence,
// which re-proves authority first.
func (p *runnerPlannedTopology) ProveFence(ctx context.Context, ref PlannedRunRef, fence v1alpha1.PlannedFailoverFenceRecord) (FenceProof, error) {
	proof, err := p.proveFence(ctx, ref, fence)
	if _, ok := p.fenceMemoFor(ref.RunID); ok {
		proof.Sent = true
	}
	return proof, err
}

func (p *runnerPlannedTopology) proveFence(ctx context.Context, ref PlannedRunRef, fence v1alpha1.PlannedFailoverFenceRecord) (FenceProof, error) {
	if l := p.leader(); l != nil && l.Err() != nil {
		return FenceProof{}, errLeaderLost
	}
	tm, err := p.manager(ref.Group)
	if err != nil {
		return FenceProof{}, err
	}
	src := tm.getSite(ref.Source)
	if src == nil {
		return FenceProof{}, errSiteNotFound
	}
	id, err := readSourceIdentity(ctx, pooledIdentity{src.mysql})
	if err != nil {
		return FenceProof{Kind: FenceSourceUnreachable, Reason: v1alpha1.PlannedFailoverReasonSourceLost}, nil
	}
	switch {
	case !id.readOnly:
		return FenceProof{Kind: FenceRetry, SourceWritable: true, SourceServerUUID: id.uuid}, nil
	case !id.superReadOnly:
		// mysqld's boot state: neither writable nor fenced by the run.
		return FenceProof{Kind: FenceNotSent, Reason: v1alpha1.PlannedFailoverReasonSourceUnfenced, SourceServerUUID: id.uuid}, nil
	}
	by := p.fenceAttribution(ref.RunID, &fence, id.uuid)
	if by == FenceUnattributed {
		return FenceProof{Kind: FenceNotAttributable, Reason: v1alpha1.PlannedFailoverReasonAuthorityChanged, SourceServerUUID: id.uuid}, nil
	}
	if fence.State == v1alpha1.PlannedFailoverFenceIntended {
		// The fence's result is not recorded yet: this proof is the one
		// that establishes it, in place of the pass that would have
		// rotated the source. A later re-check of a recorded fence is not.
		p.noteSourceNotSealed(tm, ref, by)
	}
	return p.postFenceProof(ctx, tm, ref, id, by), nil
}

// fenceAttribution is §4.3's rule: the memo for this server, or a durable
// intent naming this server's @@server_uuid.
func (p *runnerPlannedTopology) fenceAttribution(runID string, fence *v1alpha1.PlannedFailoverFenceRecord, liveUUID string) FenceAttribution {
	if m, ok := p.fenceMemoFor(runID); ok && m.uuid == liveUUID {
		return FenceByMemo
	}
	if fence != nil && fence.SourceServerUUID != "" && fence.SourceServerUUID == liveUUID {
		return FenceByIntentAndUUID
	}
	return FenceUnattributed
}

// appConnectionCounter is the optional count of the sessions a kill pass
// targets that are still connected (mysql.checker implements it).
type appConnectionCounter interface {
	CountAppConnections(ctx context.Context) (int, error)
}

// KillAppConnections drains the fenced source's application sessions and
// returns how many are still connected after the kill pass (the drain
// record's remainingConnections), not how many it killed: clients that
// reconnect at once still count, sessions that went away do not. A server
// without the count reports the killed count, which is zero only when the
// pass found no session.
func (p *runnerPlannedTopology) KillAppConnections(ctx context.Context, ref PlannedRunRef) (int32, error) {
	tm, err := p.manager(ref.Group)
	if err != nil {
		return 0, err
	}
	if err := tm.requireRunClaim(p.leader(), ref, false); err != nil {
		return 0, err
	}
	p.hooks.stmt(true, ref.Source, "KILL")
	killed, err := tm.KillSiteAppConnections(ctx, ref.Source)
	p.hooks.stmt(false, ref.Source, "KILL")
	p.noteSourceObserved(ref.RunID, err == nil, tm.clock.Now())
	if err != nil {
		return int32(killed), err
	}
	src := tm.getSite(ref.Source)
	if src == nil {
		return int32(killed), nil
	}
	c, ok := src.mysql.(appConnectionCounter)
	if !ok {
		return int32(killed), nil
	}
	remaining := killed
	if cerr := withMySQLSafetyTimeout(ctx, func(probeCtx context.Context) error {
		n, err := c.CountAppConnections(probeCtx)
		if err == nil {
			remaining = n
		}
		return err
	}); cerr != nil {
		// The kill pass answered; only the count did not. The killed count
		// stands in (zero only for a pass that found nobody).
		remaining = killed
	}
	return int32(remaining), nil
}

// ProbeLag observes the target catching up with the fenced set F, and
// whether the source and target answer (for the loss windows).
func (p *runnerPlannedTopology) ProbeLag(ctx context.Context, ref PlannedRunRef, fenced string) (LagProbe, error) {
	tm, err := p.manager(ref.Group)
	if err != nil {
		return LagProbe{}, err
	}
	want, err := mysql.ParseGTIDSet(fenced)
	if err != nil {
		return LagProbe{}, fmt.Errorf("parse fenced GTID set: %w", err)
	}
	src, tgt := tm.getSite(ref.Source), tm.getSite(ref.Target)
	if src == nil || tgt == nil {
		return LagProbe{}, errSiteNotFound
	}
	var out LagProbe
	out.SourceReachable = withMySQLSafetyTimeout(ctx, func(c context.Context) error {
		_, err := src.mysql.CheckReadOnly(c)
		return err
	}) == nil
	p.noteSourceObserved(ref.RunID, out.SourceReachable, tm.clock.Now())
	// An unreadable or unparseable target set both count toward the
	// target-loss window: neither can prove the target holds F.
	_, have, err := readGTIDSetRaw(ctx, tgt.mysql)
	if err != nil {
		return out, nil
	}
	out.TargetReachable = true
	out.CaughtUp = have.Contains(want)
	out.Divergent = !have.Subtract(want).IsEmpty()
	return out, nil
}

// ReadTargetServerUUID reads the target's @@server_uuid on a pinned
// session, for the Promoting intent. Calling it marks the start of this
// process's promotion memo for the run: from here on the memo can prove
// which promotion statements this process did not send.
func (p *runnerPlannedTopology) ReadTargetServerUUID(ctx context.Context, ref PlannedRunRef) (string, error) {
	tm, err := p.manager(ref.Group)
	if err != nil {
		return "", err
	}
	if err := tm.requireRunClaim(p.leader(), ref, false); err != nil {
		return "", err
	}
	tgt := tm.getSite(ref.Target)
	if tgt == nil {
		return "", errSiteNotFound
	}
	sess, err := tgt.mysql.PinSession(ctx)
	if err != nil {
		return "", err
	}
	defer func() { _ = sess.Close() }()
	var snap mysql.GTIDSnapshot
	if err := withMySQLSafetyTimeout(ctx, func(c context.Context) error {
		var err error
		snap, err = sess.ReadGTIDSnapshot(c)
		return err
	}); err != nil {
		return "", err
	}
	if snap.ServerUUID == "" {
		return "", fmt.Errorf("target %q reported an empty server_uuid", ref.Target)
	}
	p.startPromoteMemo(ref.RunID)
	return snap.ServerUUID, nil
}

// RecordPlannedPromotion records the confirmed promotion into the
// topology manager (recordFailover) while the claim is held, once per
// run, and leaves it on the claim for a replacement manager of this
// process.
func (p *runnerPlannedTopology) RecordPlannedPromotion(ctx context.Context, ref PlannedRunRef, confirmedAt time.Time, watermark string) error {
	tm, err := p.manager(ref.Group)
	if err != nil {
		return err
	}
	if err := tm.requireRunClaim(p.leader(), ref, false); err != nil {
		return err
	}
	tm.recordPlannedPromotion(ctx, ref, confirmedAt, watermark)
	return nil
}

func (tm *TopologyManager) recordPlannedPromotion(ctx context.Context, ref PlannedRunRef, confirmedAt time.Time, watermark string) {
	tm.mu.Lock()
	done := tm.lastPlannedRunRecorded == ref.RunID
	tm.lastPlannedRunRecorded = ref.RunID
	slot := tm.plannedSlot
	tm.mu.Unlock()
	slot.update(ref.RunID, func(c *plannedClaim) { c.promotion = &recordedPromotion{confirmedAt: confirmedAt} })
	if done {
		return
	}
	tm.recordFailover(ctx, confirmedAt, ref.Target, watermark)
}

// ApplyDNS points DNS at the confirmed target while the claim is held.
func (p *runnerPlannedTopology) ApplyDNS(ctx context.Context, ref PlannedRunRef) (v1alpha1.PlannedFailoverDNSState, error) {
	tm, err := p.manager(ref.Group)
	if err != nil {
		return "", err
	}
	if err := tm.requireRunClaim(p.leader(), ref, false); err != nil {
		return "", err
	}
	tgt := tm.getSite(ref.Target)
	if tgt == nil {
		return "", errSiteNotFound
	}
	if tm.dns == nil || tgt.lbIP == "" {
		return v1alpha1.PlannedFailoverDNSSkipped, nil
	}
	dnsCtx, cancel := context.WithTimeout(ctx, plannedDNSBudget)
	defer cancel()
	var last error
	backoff := plannedDNSRetryBackoff
	if p.hooks.Yield != nil {
		// A simulation drives a fake clock and never waits on the wall
		// clock.
		backoff = 0
	}
	for i := 0; i < plannedDNSAttempts; i++ {
		if i > 0 {
			// Back off between attempts so a fast-failing provider outage
			// does not use all three at once, and re-check the claim: DNS
			// is never written for a run that no longer holds it.
			t := time.NewTimer(backoff << (i - 1))
			select {
			case <-dnsCtx.Done():
				t.Stop()
			case <-t.C:
			}
			if err := tm.requireRunClaim(p.leader(), ref, false); err != nil {
				return v1alpha1.PlannedFailoverDNSFailed, err
			}
		}
		if dnsCtx.Err() != nil {
			break
		}
		if last = tm.applyDNS(dnsCtx, ref.Target, tgt.lbIP); last == nil {
			return v1alpha1.PlannedFailoverDNSApplied, nil
		}
	}
	if last == nil {
		// The budget (or the pass) ran out before an attempt finished.
		last = fmt.Errorf("planned failover: DNS update for %q not attempted: %w", ref.Target, context.Cause(dnsCtx))
	}
	return v1alpha1.PlannedFailoverDNSFailed, last
}

// NameUnconfirmedTarget names a target that may have taken writes but
// could not be confirmed as lastFailoverTarget in the manager's memory only
// (OPEN-1): no lastFailover and no promotedSite, so emergency failover to
// the fenced source is not cooldown-blocked. The name reaches
// status.lastFailover.target through the next snapshot write.
func (p *runnerPlannedTopology) NameUnconfirmedTarget(ref PlannedRunRef) error {
	tm, err := p.manager(ref.Group)
	if err != nil {
		return err
	}
	if err := tm.requireRunClaim(p.leader(), ref, true); err != nil {
		return err
	}
	tm.namePlannedTargetUnconfirmed(ref.Target)
	return nil
}

func (tm *TopologyManager) namePlannedTargetUnconfirmed(target string) {
	tm.mu.Lock()
	defer tm.mu.Unlock()
	tm.lastFailoverTarget = target
	tm.statusWriteFailed = true
	// The retry replays statusRetrySnapshot, which refreshes the failover
	// record only after a topology-generation bump; a name-only target (no
	// generation bump when read_only=OFF was never sent) must reach
	// status.lastFailover.target all the same.
	if tm.statusRetrySnapshot != nil {
		snap := *tm.statusRetrySnapshot
		snap.LastFailoverTarget = target
		tm.statusRetrySnapshot = &snap
	}
}

// NameFencedSource names the run's own fenced source for the topology
// manager's re-assert (§6.5) before the claim is released.
func (p *runnerPlannedTopology) NameFencedSource(ref PlannedRunRef) error {
	tm, err := p.manager(ref.Group)
	if err != nil {
		return err
	}
	if err := tm.requireRunClaim(p.leader(), ref, true); err != nil {
		return err
	}
	tm.nameFencedSource(ref.RunID, ref.Source)
	return nil
}

func (tm *TopologyManager) nameFencedSource(runID, source string) {
	tm.mu.Lock()
	tm.lastActiveSite, tm.lastActiveSiteNamed = source, true
	tm.mu.Unlock()
	tm.logger.Info("planned failover: named fenced source for re-assert", "runId", runID, "site", source)
}

// CheckRunClaim is requireRunClaim for a side effect outside the facade.
func (p *runnerPlannedTopology) CheckRunClaim(ref PlannedRunRef) error {
	tm, err := p.manager(ref.Group)
	if err != nil {
		return err
	}
	return tm.requireRunClaim(p.leader(), ref, false)
}

// ---------- re-proof (§4.4) ----------

// ownFailoverRecord reports whether the durable record rec is the run's
// own (recordFailover flushed it before the Publishing write landed):
// the run's target, not before promotion.intendedAt, carrying the fenced
// set F as its watermark. The watermark is the one of the record
// EffectiveFailoverRecord selected (§4.4 check 1), so a stale out-of-band
// copy of an earlier failover never decides for a newer status record;
// only when the selected copy carries none is the other copy used, and
// only if it describes the same event.
func ownFailoverRecord(fg *v1alpha1.MysqlFailoverGroup, rec FailoverRecord, pf *v1alpha1.PlannedFailoverStatus) bool {
	if pf.Promotion == nil || pf.Fence == nil || pf.Fence.GtidSet == nil || rec.LastFailoverTarget != pf.TargetSite {
		return false
	}
	if rec.LastFailover.Before(pf.Promotion.IntentTime.UTC().Truncate(time.Second)) {
		return false
	}
	watermark := rec.PromotionGtidExecuted
	if watermark == "" {
		// The selected copy carries none (the §5.3 size guard, or a status
		// write that tied with the annotation but predates its watermark):
		// the other copy of the same event, if it is one, carries it.
		if st := FailoverRecordFromStatus(fg); st.sameEvent(rec) && st.PromotionGtidExecuted != "" {
			watermark = st.PromotionGtidExecuted
		} else if oob, err := FailoverRecordFromAnnotations(fg.GetAnnotations()); err == nil && oob.sameEvent(rec) {
			watermark = oob.PromotionGtidExecuted
		}
	}
	return sameGTIDText(watermark, *pf.Fence.GtidSet)
}

// runRecordDurable reports whether the run's own failover record
// {confirmedAt, target, F} is the durable record of fg (read uncached):
// the selected record is the same event and carries F.
func runRecordDurable(fg *v1alpha1.MysqlFailoverGroup, pf *v1alpha1.PlannedFailoverStatus, confirmedAt time.Time, now time.Time) bool {
	if fg == nil || pf == nil || confirmedAt.IsZero() {
		return false
	}
	rec, _, err := EffectiveFailoverRecord(fg, now)
	if err != nil && rec.IsZero() {
		return false
	}
	want := FailoverRecord{LastFailover: confirmedAt.UTC().Truncate(time.Second), LastFailoverTarget: pf.TargetSite}
	return rec.sameEvent(want) && ownFailoverRecord(fg, rec, pf)
}

// sameGTIDText compares two GTID sets by value, falling back to the text.
func sameGTIDText(a, b string) bool {
	x, errA := mysql.ParseGTIDSet(a)
	y, errB := mysql.ParseGTIDSet(b)
	if errA != nil || errB != nil {
		return strings.TrimSpace(a) == strings.TrimSpace(b)
	}
	return x.Contains(y) && y.Contains(x)
}

// phaseAfterFencing reports whether the run's fence must be in place.
func phaseAfterFencing(phase v1alpha1.PlannedFailoverPhase) bool {
	switch phase {
	case v1alpha1.PlannedFailoverPhaseDraining, v1alpha1.PlannedFailoverPhaseWaitingForLag,
		v1alpha1.PlannedFailoverPhaseWaitingForDragonflySync, v1alpha1.PlannedFailoverPhasePromotingDragonfly,
		v1alpha1.PlannedFailoverPhasePromoting:
		return true
	}
	return false
}

// Reprove is the first mutating call after hydration (§4.4). It clears
// needsReprove when the record is unchanged (or is the run's own), the
// run's fence is intact and the probe set passes, and maps a failure by
// phase. A HandOff result also clears needsReprove: the engine then calls
// Promote, whose pre-write gate and confirm apply the §7.2/§7.3 mappings,
// so a lack of evidence never holds.
func (p *runnerPlannedTopology) Reprove(ctx context.Context, ref PlannedRunRef, fg *v1alpha1.MysqlFailoverGroup) (ReproveResult, error) {
	tm, err := p.manager(ref.Group)
	if err != nil {
		return ReproveResult{}, err
	}
	if err := tm.requireRunClaim(p.leader(), ref, true); err != nil {
		return ReproveResult{}, err
	}
	pf, marked := plannedFailoverRun(fg)
	if pf == nil || marked || pf.RunID != ref.RunID {
		return ReproveResult{}, errClaimLost
	}
	src, tgt := tm.getSite(ref.Source), tm.getSite(ref.Target)
	if src == nil || tgt == nil {
		// An endpoint was removed from the spec during the run: nothing
		// can be re-proven, and the engine ends the run (G-M5).
		return ReproveResult{}, errSiteNotFound
	}
	clear := func() { tm.plannedSlot.update(ref.RunID, func(c *plannedClaim) { c.needsReprove = false }) }

	if pf.Phase == v1alpha1.PlannedFailoverPhaseRollingBack {
		// Each rollback step is decided live under §9's stricter
		// preconditions (no writable peer of any class, an attributable
		// fence).
		clear()
		return ReproveResult{OK: true}, nil
	}
	F, ferr := plannedFenceGTIDSet(pf)

	// Check 1: the durable record is the baseline, or the run's own.
	var failed *v1alpha1.PlannedFailoverReason
	recordChanged := false
	if !plannedRecordUnchanged(fg, pf, tm.clock.Now()) {
		failed, recordChanged = reasonPtr(v1alpha1.PlannedFailoverReasonAuthorityChanged), true
	}
	// Check 2: the run's fence is intact, and the source's history has not
	// moved past F under it (someone lifted the fence, wrote, and fenced
	// again: same server, same super_read_only, more transactions).
	lackOfEvidence, sourceWrote := false, false
	var warnings []v1alpha1.PlannedFailoverWarning
	if failed == nil && phaseAfterFencing(pf.Phase) {
		id, err := readSourceIdentity(ctx, pooledIdentity{src.mysql})
		switch {
		case err != nil && pf.Phase != v1alpha1.PlannedFailoverPhasePromoting:
			// One failed read is no evidence of a lost source: the engine
			// applies the source-loss window (§6.2) before rolling back.
			return ReproveResult{SourceUnreachable: true}, nil
		case err != nil:
			failed, lackOfEvidence = reasonPtr(v1alpha1.PlannedFailoverReasonSourceLost), true
		case !id.superReadOnly || pf.Fence == nil || id.uuid != pf.Fence.SourceServerUUID:
			failed, lackOfEvidence = reasonPtr(v1alpha1.PlannedFailoverReasonSourceUnfenced), true
		case ferr == nil && sourceAdvancedPast(id, F, plannedPromotionUUID(pf)):
			failed, sourceWrote = reasonPtr(v1alpha1.PlannedFailoverReasonAuthorityChanged), true
		}
	}
	// Check 3: the probe set, with the source read-only.
	v := classifyPlannedPeers(tm.plannedProbeSites(ctx, ref.Source, ref.Target))
	foreignWritable := len(v.writable) > 0
	if failed == nil && foreignWritable {
		failed = reasonPtr(v1alpha1.PlannedFailoverReasonForeignWritable)
	}
	if len(v.allUnreadable()) > 0 {
		warnings = append(warnings, v1alpha1.PlannedFailoverWarningUnreachablePeers)
	}
	if len(v.unreadable) > 0 && pf.Phase == v1alpha1.PlannedFailoverPhasePromoting && failed == nil {
		// Only a promotable (primary-candidate or dr-only) peer is missing
		// evidence; an unreadable reader is allowed by §4.1 and the gate.
		lackOfEvidence = true
	}

	if pf.Phase != v1alpha1.PlannedFailoverPhasePromoting {
		if failed != nil {
			return ReproveResult{RollBack: failed}, nil
		}
		clear()
		return ReproveResult{OK: true}, nil
	}

	if failed == nil && !lackOfEvidence {
		clear()
		return ReproveResult{OK: true}, nil
	}
	// Promoting: classify the target first.
	if ferr == nil {
		if live, err := readPromotionLive(ctx, tgt.mysql); err == nil && live.readOnly {
			if extra := live.gtid.Subtract(F); extra.IsEmpty() {
				// No transaction of the run committed: put the target back
				// behind super_read_only and roll back. A failed SET is
				// logged here and enforced by the rollback, which never
				// restores the target's channel or unfences the source
				// while the target reads super_read_only=OFF.
				if !live.superReadOnly {
					if err := tm.requireRunClaim(p.leader(), ref, true); err != nil {
						return ReproveResult{}, err
					}
					p.hooks.stmt(true, ref.Target, "super_read_only=ON")
					err := withMySQLSafetyTimeout(ctx, func(c context.Context) error { return tgt.mysql.SetSuperReadOnly(c, true) })
					p.hooks.stmt(false, ref.Target, "super_read_only=ON")
					if err != nil {
						tm.logger.Warn("planned failover: could not restore the target's super_read_only",
							"runId", ref.RunID, "site", ref.Target, "error", err.Error())
					}
				}
				reason := v1alpha1.PlannedFailoverReasonPeerUnreadable
				if failed != nil {
					reason = *failed
				}
				return ReproveResult{RollBack: reasonPtr(reason)}, nil
			}
		}
	}
	switch {
	case recordChanged:
		return ReproveResult{Hold: reasonPtr(v1alpha1.PlannedFailoverReasonAuthorityChanged)}, nil
	case foreignWritable, sourceWrote:
		return ReproveResult{Hold: reasonPtr(v1alpha1.PlannedFailoverReasonForeignWritable)}, nil
	}
	// Only a lack of evidence: hand off to the §7.2/§7.3 mappings for the
	// target's classified stage, run now rather than synthesised, so a
	// re-proof past the promotion deadline still confirms a target that
	// is writable with F (Confirmed plus a warning) instead of ending
	// PromotedUnconfirmed without looking.
	clear()
	out, err := tm.promotePlannedRun(ctx, p, ref, pf)
	if err != nil {
		return ReproveResult{}, err
	}
	out.Warnings = mergeWarnings(out.Warnings, warnings)
	return ReproveResult{HandOff: &out}, nil
}

// sourceAdvancedPast reports whether the source's GTID set, read with id,
// holds transactions outside the fenced set F that the target (@@server_uuid
// U, "" before Promoting) did not write: its history moved on after the
// fence, so F no longer names everything the source acknowledged. The
// target's own transactions replicated back are not lost by the
// promotion. An unparseable set proves nothing either way.
func sourceAdvancedPast(id sourceIdentity, F mysql.GTIDSet, U string) bool {
	have, err := mysql.ParseGTIDSet(id.gtid)
	if err != nil {
		return false
	}
	for sid := range have.Subtract(F) {
		if uuid, _, _ := strings.Cut(sid, ":"); U == "" || !strings.EqualFold(uuid, U) {
			return true
		}
	}
	return false
}

// plannedPromotionUUID is the Promoting intent's target @@server_uuid, or "".
func plannedPromotionUUID(pf *v1alpha1.PlannedFailoverStatus) string {
	if pf.Promotion == nil {
		return ""
	}
	return pf.Promotion.TargetServerUUID
}

// plannedFenceGTIDSet parses the run's fenced set F.
func plannedFenceGTIDSet(pf *v1alpha1.PlannedFailoverStatus) (mysql.GTIDSet, error) {
	if pf.Fence == nil || pf.Fence.GtidSet == nil {
		return nil, errors.New("the run has no fenced GTID set")
	}
	return mysql.ParseGTIDSet(*pf.Fence.GtidSet)
}

// ---------- rollback (§9) ----------

// Rollback runs one idempotent pass of §9, every step decided live: the
// hard preconditions, the target's channel, then the source. Outcome ""
// means not finished: the engine retries until rollbackDeadline.
func (p *runnerPlannedTopology) Rollback(ctx context.Context, ref PlannedRunRef, pf *v1alpha1.PlannedFailoverStatus) (RollbackResult, error) {
	tm, err := p.manager(ref.Group)
	if err != nil {
		return RollbackResult{}, err
	}
	if err := tm.requireRunClaim(p.leader(), ref, false); err != nil {
		return RollbackResult{}, err
	}
	return tm.rollbackRun(ctx, p, ref, pf)
}

func (tm *TopologyManager) rollbackRun(ctx context.Context, p *runnerPlannedTopology, ref PlannedRunRef, pf *v1alpha1.PlannedFailoverStatus) (RollbackResult, error) {
	reason := v1alpha1.PlannedFailoverReasonInternalError
	var deadline time.Time
	if pf.Rollback != nil {
		reason = pf.Rollback.Reason
		deadline = pf.Rollback.IntentTime.Add(plannedFailoverRollbackDeadline)
	}
	src, tgt := tm.getSite(ref.Source), tm.getSite(ref.Target)
	if src == nil || tgt == nil {
		return RollbackResult{}, errSiteNotFound
	}
	res := RollbackResult{Reason: reason}
	F, fErr := plannedFenceGTIDSet(pf)

	// Precondition: once a write may have been sent, the target must be
	// reachable and hold nothing outside F. One failed read is no evidence
	// of a lost target: the source stays fenced while the read is retried
	// within the loss window (ending before the rollback deadline), and
	// only then is the run PromotedUnconfirmed.
	live, liveErr := readPromotionLive(ctx, tgt.mysql)
	if liveErr == nil {
		p.lossRecovered(p.rollbackTargetLoss, ref.RunID)
	}
	if pf.Promotion != nil && !p.memoProvesNoWrite(ref.RunID) {
		switch {
		case liveErr != nil:
			if p.lossExpired(p.rollbackTargetLoss, ref.RunID, tm.clock.Now(), deadline) {
				res.Outcome = v1alpha1.PlannedFailoverOutcomePromotedUnconfirmed
			}
			return res, nil
		case fErr != nil || !live.gtid.Subtract(F).IsEmpty():
			res.Outcome, res.Reason = v1alpha1.PlannedFailoverOutcomeManualRecoveryRequired, v1alpha1.PlannedFailoverReasonExecuteFailed
			return res, nil
		}
	}

	// The source as it is now.
	id, idErr := readSourceIdentity(ctx, pooledIdentity{src.mysql})
	if idErr == nil && !id.readOnly {
		// Already writable (an earlier unfence whose readback was lost, or
		// someone else's). Another writable site beside it is two writers,
		// not a clean rollback: when it is the target and the run may have
		// written to it, the run's own promotion is one of them, so the
		// run holds (§10.5). A writer the run never touched is reported and
		// left to the topology manager's split-brain handling.
		peers := classifyPlannedPeers(tm.plannedProbeSites(ctx, ref.Source))
		p.hooks.yield(YieldPeerProbeForSim)
		if len(peers.writable) > 0 {
			tm.logger.Warn("planned failover: rollback found another writable site beside the source",
				"runId", ref.RunID, "sites", strings.Join(peers.writable, ","))
			if slices.Contains(peers.writable, ref.Target) && pf.Promotion != nil && !p.memoProvesNoWrite(ref.RunID) {
				res.Outcome, res.Reason = v1alpha1.PlannedFailoverOutcomeManualRecoveryRequired, v1alpha1.PlannedFailoverReasonForeignWritable
				return res, nil
			}
		}
		// The target goes back behind super_read_only before the rollback
		// completes: at read_only=ON, super_read_only=OFF it would accept
		// SUPER writes beside the writable source. A re-fence that is not
		// verified (it failed, or the target could not be read at all) is
		// retried until just before the rollback deadline; then the run
		// ends RolledBack (the source does serve writes) saying so, and
		// the target's channel is left alone. No hold: read_only=ON,
		// super_read_only=OFF is also mysqld's boot state for a replica.
		err := tm.refencePlannedTarget(ctx, p, ref, tgt, &live, liveErr)
		if err == nil && liveErr != nil {
			err = fmt.Errorf("the target is unreadable: %w", liveErr)
		}
		if err != nil {
			if plannedClaimError(err) {
				return RollbackResult{}, err
			}
			if deadline.IsZero() || tm.clock.Now().Add(plannedRequeuePoll).Before(deadline) {
				return res, nil
			}
			res.Warnings = append(res.Warnings, v1alpha1.PlannedFailoverWarningTargetSuperReadOnlyNotRestored,
				v1alpha1.PlannedFailoverWarningTargetReplicationNotRestored)
			res.Outcome = v1alpha1.PlannedFailoverOutcomeRolledBack
			return res, nil
		}
		verified := p.fenceAttribution(ref.RunID, pf.Fence, id.uuid) != FenceUnattributed
		if !verified && pf.Fence != nil && pf.Fence.SourceServerUUID != "" {
			res.Warnings = append(res.Warnings, v1alpha1.PlannedFailoverWarningSourceReplaced)
		}
		tm.restorePlannedTargetChannel(ctx, p, ref, src, tgt, live, liveErr, F, fErr, verified, &res)
		res.Outcome = v1alpha1.PlannedFailoverOutcomeRolledBack
		return res, nil
	}
	// The target must read super_read_only=ON before its channel is
	// restored or the source unfenced: a target left at read_only=ON,
	// super_read_only=OFF (an SRO clear whose reply was lost, a restart
	// between the clears, a failed re-fence in the re-proof) would accept
	// SUPER writes beside the writable source.
	refenceErr := tm.refencePlannedTarget(ctx, p, ref, tgt, &live, liveErr)
	peers := classifyPlannedPeers(tm.plannedProbeSites(ctx, ref.Source))
	p.hooks.yield(YieldPeerProbeForSim)
	// Attribution is decided whenever the source answered, so every
	// SourceLeftFenced end below carries FenceNotAttributable for a fence
	// that is not the run's (a replaced pod fenced by someone else, say),
	// and the topology manager is never told to lift it (§6.5).
	unattributed := idErr == nil && p.fenceAttribution(ref.RunID, pf.Fence, id.uuid) == FenceUnattributed
	leftFenced := func() (RollbackResult, error) {
		res.Outcome = v1alpha1.PlannedFailoverOutcomeSourceLeftFenced
		if unattributed {
			res.Warnings = append(res.Warnings, v1alpha1.PlannedFailoverWarningFenceNotAttributable)
		}
		// The target's channel is not re-pointed at a source whose
		// identity cannot be verified; one this run removed is reported.
		tm.restorePlannedTargetChannel(ctx, p, ref, src, tgt, live, liveErr, F, fErr, false, &res)
		return res, nil
	}
	if len(peers.writable) > 0 {
		return leftFenced()
	}
	if reason == v1alpha1.PlannedFailoverReasonSourceLost {
		// §9 step 3: no unfence retries for a lost source.
		return leftFenced()
	}
	if idErr != nil {
		return res, nil // unreachable: retry until rollbackDeadline
	}
	if unattributed {
		return leftFenced()
	}
	if refenceErr != nil {
		return res, nil // retry the re-fence until rollbackDeadline
	}

	// Verify the attributed server on a pinned session before anything
	// is pointed at it or unfenced.
	sess, err := src.mysql.PinSession(ctx)
	if err != nil {
		return res, nil
	}
	defer func() { _ = sess.Close() }()
	pinned, err := readSourceIdentity(ctx, sess)
	switch {
	case err != nil:
		return res, nil
	case pinned.uuid != id.uuid:
		res.Outcome = v1alpha1.PlannedFailoverOutcomeSourceLeftFenced
		res.Warnings = append(res.Warnings, v1alpha1.PlannedFailoverWarningFenceNotAttributable)
		tm.restorePlannedTargetChannel(ctx, p, ref, src, tgt, live, liveErr, F, fErr, false, &res)
		return res, nil
	case (fErr == nil && !gtidSetHolds(pinned.gtid, F)) || (liveErr == nil && tm.lacksOwnTransactions(ref.Source, pinned.uuid, pinned.gtid, live.gtid)):
		// The same server_uuid on an older data directory: the source
		// restarted without part of the set it was fenced with, or
		// (restarted before the fence was read) without transactions it
		// issued itself that the target already holds. Unfencing it would
		// have it issue those GTIDs again, and the target, which holds
		// them, must not be pointed at it. It stays fenced; the topology
		// manager reports what it lacks (own_transactions_lost.go). A
		// target that holds transactions of another server is a
		// divergent target, which does not keep the source fenced.
		tm.logger.Warn("planned failover: rollback leaves the source fenced, it lacks transactions it held",
			"runId", ref.RunID, "site", ref.Source, "fenceGtid", truncateGtidSet(F.String()),
			"sourceGtid", truncateGtidSet(pinned.gtid), "targetGtid", truncateGtidSet(live.gtidRaw))
		res.Outcome = v1alpha1.PlannedFailoverOutcomeSourceLeftFenced
		tm.restorePlannedTargetChannel(ctx, p, ref, src, tgt, live, liveErr, F, fErr, false, &res)
		return res, nil
	}
	tm.restorePlannedTargetChannel(ctx, p, ref, src, tgt, live, liveErr, F, fErr, true, &res)
	if !pinned.readOnly {
		res.Outcome = v1alpha1.PlannedFailoverOutcomeRolledBack
		return res, nil
	}
	if err := tm.requireRunClaim(p.leader(), ref, false); err != nil {
		return RollbackResult{}, err
	}
	p.hooks.stmt(true, ref.Source, "unfence")
	unfenceErr := withMySQLSafetyTimeout(ctx, func(c context.Context) error {
		return errors.Join(sess.SetSuperReadOnly(c, false), sess.SetReadOnly(c, false))
	})
	p.hooks.stmt(false, ref.Source, "unfence")
	if unfenceErr != nil {
		return res, nil
	}
	ro := true
	if err := withMySQLSafetyTimeout(ctx, func(c context.Context) error {
		var err error
		ro, err = sess.CheckReadOnly(c)
		return err
	}); err != nil || ro {
		return res, nil
	}
	res.Outcome = v1alpha1.PlannedFailoverOutcomeRolledBack
	return res, nil
}

// plannedClaimError reports whether err is one of the facade's "nothing
// was sent: the claim or leadership is gone" sentinels.
func plannedClaimError(err error) bool {
	return errors.Is(err, errClaimLost) || errors.Is(err, errLeaderLost) || errors.Is(err, errReproveRequired)
}

// refencePlannedTarget puts a read-only target back behind
// super_read_only, reads it back and records it in live. A target that
// reads super_read_only=ON already, is writable (decided by the caller's
// peer checks) or did not answer is left alone; an error means the target
// still reads OFF.
func (tm *TopologyManager) refencePlannedTarget(ctx context.Context, p *runnerPlannedTopology, ref PlannedRunRef, tgt *siteTracker,
	live *promotionLive, liveErr error) error {
	if liveErr != nil || !live.readOnly || live.superReadOnly {
		return nil
	}
	if err := tm.requireRunClaim(p.leader(), ref, false); err != nil {
		return err
	}
	p.hooks.stmt(true, ref.Target, "super_read_only=ON")
	err := withMySQLSafetyTimeout(ctx, func(c context.Context) error { return tgt.mysql.SetSuperReadOnly(c, true) })
	p.hooks.stmt(false, ref.Target, "super_read_only=ON")
	if err == nil {
		err = withMySQLSafetyTimeout(ctx, func(c context.Context) error {
			on, err := pooledIdentity{tgt.mysql}.CheckSuperReadOnly(c)
			if err == nil && !on {
				err = errors.New("super_read_only still reads OFF")
			}
			return err
		})
	}
	if err != nil {
		tm.logger.Warn("planned failover: could not restore the target's super_read_only",
			"runId", ref.RunID, "site", ref.Target, "error", err.Error())
		return err
	}
	live.superReadOnly = true
	return nil
}

// restorePlannedTargetChannel is §9 step 2: give the target its channel
// back from the source when it is read-only, has no channel and holds a
// non-empty subset of F; restart a stopped channel; otherwise leave it for
// clone or reclone with the warning TargetReplicationNotRestored. A new
// channel is pointed at the source host only when sourceVerified (the
// source's identity was proven the run's on a pinned session, or it is
// the run's writable source): never at a replaced or unverified server.
func (tm *TopologyManager) restorePlannedTargetChannel(ctx context.Context, p *runnerPlannedTopology, ref PlannedRunRef, src, tgt *siteTracker,
	live promotionLive, liveErr error, F mysql.GTIDSet, fErr error, sourceVerified bool, res *RollbackResult) {
	hooks := p.hooks
	warn := func() {
		if !slices.Contains(res.Warnings, v1alpha1.PlannedFailoverWarningTargetReplicationNotRestored) {
			res.Warnings = append(res.Warnings, v1alpha1.PlannedFailoverWarningTargetReplicationNotRestored)
		}
	}
	if liveErr != nil || fErr != nil || !live.readOnly {
		warn()
		return
	}
	if live.repl != nil && live.repl.IORunning && live.repl.SQLRunning {
		return
	}
	if !live.superReadOnly {
		// Its re-fence failed: it is not handed a channel while it could
		// still take SUPER writes.
		warn()
		return
	}
	if live.repl != nil {
		if tm.requireRunClaim(p.leader(), ref, false) != nil {
			warn()
			return
		}
		hooks.stmt(true, ref.Target, "START REPLICA")
		err := tm.withReplicaStartGate(ref.Target, func() error {
			return withMySQLSafetyTimeout(ctx, func(c context.Context) error { return tgt.mysql.StartReplica(c) })
		})
		hooks.stmt(false, ref.Target, "START REPLICA")
		if err != nil {
			warn()
		}
		return
	}
	if !sourceVerified || live.gtid.IsEmpty() || !live.gtid.Subtract(F).IsEmpty() || tm.bootstrapCfg.ReplUser == "" {
		warn()
		return
	}
	if tm.requireRunClaim(p.leader(), ref, false) != nil {
		warn()
		return
	}
	opts := mysql.ReplicationSourceOpts{Host: src.host, User: tm.bootstrapCfg.ReplUser, Password: tm.bootstrapCfg.ReplPassword, UseSSL: tm.bootstrapCfg.UseSSL}
	hooks.stmt(true, ref.Target, "CHANGE REPLICATION SOURCE")
	err := tm.withReplicaStartGate(ref.Target, func() error {
		return withMySQLSafetyTimeout(ctx, func(c context.Context) error {
			return errors.Join(tgt.mysql.ChangeReplicationSource(c, opts), tgt.mysql.StartReplica(c))
		})
	})
	hooks.stmt(false, ref.Target, "CHANGE REPLICATION SOURCE")
	if err != nil {
		warn()
	}
}

// ---------- promotion (§7) ----------

// promotionLive is the target's state read for stage classification.
type promotionLive struct {
	readOnly, superReadOnly bool
	repl                    *mysql.ReplicaStatus
	uuid                    string
	gtid                    mysql.GTIDSet
	gtidRaw                 string
}

type promotionReader interface {
	CheckReadOnly(context.Context) (bool, error)
	ShowReplicaStatus(context.Context) (*mysql.ReplicaStatus, error)
}

// readPromotionLive reads the stage inputs. On a pinned Session every read
// is on that one connection; on a Checker the optional reads go through
// pooledIdentity.
func readPromotionLive(ctx context.Context, r promotionReader) (promotionLive, error) {
	var id interface {
		CheckSuperReadOnly(context.Context) (bool, error)
		ReadGTIDSnapshot(context.Context) (mysql.GTIDSnapshot, error)
	}
	switch c := r.(type) {
	case mysql.Session:
		id = c
	case mysql.Checker:
		id = pooledIdentity{c}
	default:
		return promotionLive{}, errors.New("unsupported promotion reader")
	}
	var live promotionLive
	err := withMySQLSafetyTimeout(ctx, func(c context.Context) error {
		var err error
		if live.readOnly, err = r.CheckReadOnly(c); err != nil {
			return err
		}
		if live.superReadOnly, err = id.CheckSuperReadOnly(c); err != nil {
			return err
		}
		if live.repl, err = r.ShowReplicaStatus(c); err != nil {
			return err
		}
		snap, err := id.ReadGTIDSnapshot(c)
		if err != nil {
			return err
		}
		live.uuid, live.gtidRaw = snap.ServerUUID, snap.GtidExecuted
		live.gtid, err = mysql.ParseGTIDSet(snap.GtidExecuted)
		return err
	})
	return live, err
}

// ownExtras reports whether every GTID in extra was written by the server
// with @@server_uuid u.
func ownExtras(extra mysql.GTIDSet, u string) bool {
	for sid := range extra {
		if uuid, _, _ := strings.Cut(sid, ":"); !strings.EqualFold(uuid, u) {
			return false
		}
	}
	return true
}

// classifyPromotionStage is §7.1's table: from the target's live state on
// the pinned session, the fenced set F, the intent's @@server_uuid U and
// whether this process's memo proves super_read_only=OFF was never sent,
// it returns the stage to resume from and a nil outcome, or, for the
// Aborted and Inconsistent rows, the final outcome the promotion returns
// without sending anything.
func classifyPromotionStage(live promotionLive, F mysql.GTIDSet, U string, memoNoWrite bool) (PromotionStage, *PromoteOutcome) {
	abort := func(reason v1alpha1.PlannedFailoverReason, terminal v1alpha1.PlannedFailoverOutcome) (PromotionStage, *PromoteOutcome) {
		return PromotionStageTargetReplaced, &PromoteOutcome{Kind: PromoteAborted, Stage: PromotionStageTargetReplaced, Reason: reason,
			Terminal: terminal, TargetServerUUID: live.uuid, TargetReachable: true}
	}
	inconsistent := func(reason v1alpha1.PlannedFailoverReason) (PromotionStage, *PromoteOutcome) {
		return PromotionStageInconsistent, &PromoteOutcome{Kind: PromoteInconsistent, Stage: PromotionStageInconsistent, Reason: reason,
			TargetServerUUID: live.uuid, TargetReachable: true, MayHaveWritten: true}
	}
	extra := live.gtid.Subtract(F)
	missing := !live.gtid.Contains(F)
	own := ownExtras(extra, U)
	switch {
	case !strings.EqualFold(live.uuid, U):
		if memoNoWrite {
			return abort(v1alpha1.PlannedFailoverReasonTargetChanged, "")
		}
		return inconsistent(v1alpha1.PlannedFailoverReasonExecuteFailed)
	case !live.readOnly && live.repl != nil:
		if memoNoWrite {
			return abort(v1alpha1.PlannedFailoverReasonForeignWritable, v1alpha1.PlannedFailoverOutcomeSourceLeftFenced)
		}
		return inconsistent(v1alpha1.PlannedFailoverReasonForeignWritable)
	case !live.readOnly && memoNoWrite:
		// Writable although this process never cleared super_read_only:
		// someone else made it writable.
		return abort(v1alpha1.PlannedFailoverReasonForeignWritable, v1alpha1.PlannedFailoverOutcomeSourceLeftFenced)
	case !live.readOnly && !missing && own:
		return PromotionStageWritable, nil
	case !live.readOnly && missing:
		return inconsistent(v1alpha1.PlannedFailoverReasonExecuteFailed)
	case !live.readOnly:
		return inconsistent(v1alpha1.PlannedFailoverReasonForeignWritable)
	case live.repl != nil && (live.repl.IORunning || live.repl.IOConnecting || live.repl.SQLRunning):
		return PromotionStageNotStarted, nil
	case live.repl != nil:
		return PromotionStageReplicaStopped, nil
	case !missing && extra.IsEmpty():
		return PromotionStageReplicaReset, nil
	case !missing && own:
		if memoNoWrite {
			return abort(v1alpha1.PlannedFailoverReasonAuthorityChanged, v1alpha1.PlannedFailoverOutcomeSourceLeftFenced)
		}
		return PromotionStageWasWritable, nil
	case missing && own:
		if memoNoWrite {
			return abort(v1alpha1.PlannedFailoverReasonTargetChanged, "")
		}
		return inconsistent(v1alpha1.PlannedFailoverReasonExecuteFailed)
	}
	return inconsistent(v1alpha1.PlannedFailoverReasonForeignWritable)
}

// plannedGateRefusal carries the outcome of a refused pre-write gate
// through promoteVerifiedFrom.
type plannedGateRefusal struct{ out PromoteOutcome }

func (e *plannedGateRefusal) Error() string {
	return fmt.Sprintf("planned failover: pre-write gate refused (%s)", e.out.Reason)
}

// Promote is §7: classify the target's stage on a pinned session, resume
// the staged promotion from it with the pre-write gate immediately before
// super_read_only=OFF, then confirm on the same session.
func (p *runnerPlannedTopology) Promote(ctx context.Context, ref PlannedRunRef, pf *v1alpha1.PlannedFailoverStatus) (PromoteOutcome, error) {
	tm, err := p.manager(ref.Group)
	if err != nil {
		return PromoteOutcome{}, err
	}
	if err := tm.requireRunClaim(p.leader(), ref, false); err != nil {
		return PromoteOutcome{}, err
	}
	return tm.promotePlannedRun(ctx, p, ref, pf)
}

// beginPlannedPromotion claims the topology-change window for the run's
// promotion; the caller defers finishDeploymentTopologyChange.
func (tm *TopologyManager) beginPlannedPromotion() {
	tm.mu.Lock()
	defer tm.mu.Unlock()
	tm.authorityEpoch++
	tm.deploymentChanging++
	tm.deploymentTopologyPendingLocked()
}

func (tm *TopologyManager) promotePlannedRun(ctx context.Context, p *runnerPlannedTopology, ref PlannedRunRef, pf *v1alpha1.PlannedFailoverStatus) (PromoteOutcome, error) {
	tgt := tm.getSite(ref.Target)
	if tgt == nil || tm.getSite(ref.Source) == nil {
		return PromoteOutcome{}, errSiteNotFound
	}
	if pf.Promotion == nil || pf.Promotion.TargetServerUUID == "" {
		return PromoteOutcome{}, errors.New("planned failover: the Promoting record names no target server_uuid")
	}
	U := pf.Promotion.TargetServerUUID
	F, err := plannedFenceGTIDSet(pf)
	if err != nil {
		return PromoteOutcome{Kind: PromoteAborted, Reason: v1alpha1.PlannedFailoverReasonInvalidGTID}, nil
	}
	if !tgt.isPromotable() {
		// The manager's configuration is the current spec: a claim that
		// survived a manager replacement (a role change restarts the
		// manager) is not re-proven, so the target's role is checked here
		// before any statement, and again at the pre-write gate.
		tm.logger.Warn("planned failover: the target is no longer a primary candidate; not promoting it",
			"runId", ref.RunID, "site", ref.Target, "role", string(APISiteRole(tgt.role)))
		return PromoteOutcome{Kind: PromoteAborted, Reason: v1alpha1.PlannedFailoverReasonTargetNotPromotable,
			TargetServerUUID: U, TargetReachable: true, MayHaveWritten: !p.memoProvesNoWrite(ref.RunID)}, nil
	}
	tm.beginPlannedPromotion()
	defer tm.finishDeploymentTopologyChange()

	// The pinned session is closed before the unconfirmed path's pooled
	// reachability probe: the target's pool is small (2 connections), and
	// a probe queued behind the promotion's own session and a concurrent
	// poll could time out and report a reachable target lost.
	var sess mysql.Session
	closeSess := func() {
		if sess != nil {
			_ = sess.Close()
			sess = nil
		}
	}
	defer closeSess()
	unconfirmed := func(stage PromotionStage, warnings ...v1alpha1.PlannedFailoverWarning) PromoteOutcome {
		closeSess()
		reachable := withMySQLSafetyTimeout(ctx, func(c context.Context) error {
			_, err := tgt.mysql.CheckReadOnly(c)
			return err
		}) == nil
		return PromoteOutcome{Kind: PromoteUnconfirmed, Stage: stage, Reason: v1alpha1.PlannedFailoverReasonPromotionUnconfirmed,
			Warnings: warnings, TargetServerUUID: U, TargetReachable: reachable, MayHaveWritten: !p.memoProvesNoWrite(ref.RunID)}
	}

	pinned, err := tgt.mysql.PinSession(ctx)
	if err != nil {
		return unconfirmed(""), nil
	}
	sess = pinned
	live, err := readPromotionLive(ctx, sess)
	if err != nil {
		return unconfirmed(""), nil
	}
	_, haveMemo := p.promoteMemoFor(ref.RunID)
	stage, verdict := classifyPromotionStage(live, F, U, p.memoProvesNoWrite(ref.RunID))
	if verdict != nil {
		if verdict.Kind == PromoteInconsistent && !haveMemo && live.readOnly && !strings.EqualFold(live.uuid, U) {
			verdict.RollbackIfUnrecorded = tm.replacedTargetRollbackSafe(ctx, p, ref, pf, F)
		}
		return *verdict, nil
	}
	if stage == PromotionStageNotStarted || stage == PromotionStageReplicaStopped {
		// A configured channel proves no process ever cleared the target's
		// super_read_only for this run (RESET REPLICA ALL precedes it), so
		// a re-run with no memo starts one here: from now on it proves no
		// write until this process sends the clear, and a failed STOP or
		// RESET is not reported as a possible write.
		p.startPromoteMemo(ref.RunID)
	}
	if stage == PromotionStageWritable {
		// Writable with F and only own extras: the clears already ran
		// (in an earlier pass or process). Authority moves as on the path
		// that sends them, before the confirm.
		tm.deploymentPromotionExecuted(ref.Target)
		out := tm.confirmPlannedPromotion(ctx, p.hooks, ref, pf, sess, F, U)
		out.Stage = PromotionStageWritable
		if out.Kind == PromoteConfirmed {
			// The pre-clear snapshot of the pass that cleared the fences is
			// not observable any more; the run's watermark F is logged.
			tm.failover.logger.Info("failover complete", "promotedSite", ref.Target, "promotionGtid", F.String())
		}
		return out, nil
	}
	var gateWarnings []v1alpha1.PlannedFailoverWarning
	gateGtid := ""
	lastStmt := ""
	hooks := promoteHooks{
		before: func(s promoteMemoStage) {
			p.notePromoteSent(ref.RunID, s)
			lastStmt = s.statement()
			p.hooks.stmt(true, ref.Target, lastStmt)
		},
		after: func() { p.hooks.stmt(false, ref.Target, lastStmt) },
		gate: func(gateCtx context.Context) error {
			w, gtid, err := tm.plannedPreWriteGate(gateCtx, p, ref, pf, sess, F, U, stage == PromotionStageWasWritable)
			gateWarnings, gateGtid = w, gtid
			return err
		},
		// The claim and the leader are re-checked immediately before
		// every statement, including the SRO clear right after the
		// gate's probes (§3.3): a pass that lost either while it waited
		// on a probe sends nothing more. So are this manager and the
		// target's role (I001): the claim is shared with a replacement
		// manager, so a spec change during the probes must stop a
		// retired manager from clearing the target's fences.
		guard: func() error {
			if err := tm.requireRunClaim(p.leader(), ref, false); err != nil {
				return err
			}
			err := tm.checkPlannedPromotionManager(p, ref, U, !p.memoProvesNoWrite(ref.RunID))
			var refusal *plannedGateRefusal
			if errors.As(err, &refusal) {
				tm.logger.Warn("planned failover: the target is no longer a primary candidate; not promoting it",
					"runId", ref.RunID, "site", ref.Target, "role", string(APISiteRole(tgt.role)))
			}
			return err
		},
		statementTimeout: mysqlSafetyProbeTimeout,
		planned:          true,
		// One rotation per run and process: a promotion whose gate is
		// retried over several passes would otherwise add an empty binary
		// log file, and up to BinlogRotationTimeout, on every pass.
		flush: func(send func()) {
			if !p.firstFlush(p.promoteFlushed, ref.RunID) {
				return
			}
			p.hooks.stmt(true, ref.Target, "FLUSH BINARY LOGS")
			send()
			p.hooks.stmt(false, ref.Target, "FLUSH BINARY LOGS")
		},
	}
	promotionGtid, _, err := tm.failover.promoteVerifiedFrom(ctx, sess, tgt.mysql, ref.Target, F, stage, hooks)
	var refusal *plannedGateRefusal
	var divergent *DivergentCandidateError
	switch {
	case errors.As(err, &refusal):
		refusal.out.Stage = stage
		return refusal.out, nil
	case errors.Is(err, errLeaderLost) || errors.Is(err, errClaimLost) || errors.Is(err, errSiteNotFound):
		return PromoteOutcome{}, err
	case errors.Is(err, errCandidateGTIDMismatch) || errors.As(err, &divergent):
		// The GTID proofs run only on the configured-channel stages
		// (NotStarted, ReplicaStopped), before the gate and the SRO clear:
		// the channel proved no SRO clear ever ran on this server (RESET
		// REPLICA ALL precedes it), cross process too.
		return PromoteOutcome{Kind: PromoteAborted, Stage: stage, Reason: v1alpha1.PlannedFailoverReasonTargetChanged,
			TargetServerUUID: U, TargetReachable: true}, nil
	case err != nil:
		return unconfirmed(stage, gateWarnings...), nil
	}
	if stage == PromotionStageWasWritable && gateGtid != "" {
		// The set the gate read on sess immediately before the clears: F
		// plus the target's own earlier writes, not F alone.
		promotionGtid = gateGtid
	}
	tm.deploymentPromotionExecuted(ref.Target)
	out := tm.confirmPlannedPromotion(ctx, p.hooks, ref, pf, sess, F, U)
	out.Stage = stage
	out.Warnings = mergeWarnings(gateWarnings, out.Warnings)
	if out.Kind == PromoteUnconfirmed {
		out.TargetReachable = unconfirmed(stage).TargetReachable
	}
	if out.Kind == PromoteConfirmed {
		// The documented completion line, only once the promotion is
		// confirmed live (§7.3); a pass whose confirm finds a foreign
		// writable peer or cannot read the target never emits it.
		tm.failover.logger.Info("failover complete", "promotedSite", ref.Target, "promotionGtid", promotionGtid)
	}
	return out, nil
}

// errPlannedManagerReplaced: the topology manager that classified the
// promotion's stage was retired or replaced (a spec change restarts it)
// while the promotion ran. Nothing more is sent on this pass; the next pass
// classifies again under the current manager, which checks the target's
// role before any statement.
var errPlannedManagerReplaced = errors.New("planned failover: the topology manager was replaced during the promotion")

// checkPlannedPromotionManager is I001's check immediately before a
// promotion statement: tm must still be the group's current, unretired
// manager (errPlannedManagerReplaced otherwise), and the target must still
// be a primary candidate in its configuration, which is the current spec
// (a *plannedGateRefusal with Aborted{TargetNotPromotable} otherwise,
// carrying mayHaveWritten).
func (tm *TopologyManager) checkPlannedPromotionManager(p *runnerPlannedTopology, ref PlannedRunRef, U string, mayHaveWritten bool) error {
	cur, err := p.manager(ref.Group)
	tm.mu.RLock()
	retired := tm.retired
	tm.mu.RUnlock()
	if err != nil || cur != tm || retired {
		return errPlannedManagerReplaced
	}
	if tgt := tm.getSite(ref.Target); tgt == nil || !tgt.isPromotable() {
		return &plannedGateRefusal{out: PromoteOutcome{Kind: PromoteAborted, Reason: v1alpha1.PlannedFailoverReasonTargetNotPromotable,
			TargetServerUUID: U, TargetReachable: true, MayHaveWritten: mayHaveWritten, Sites: []string{ref.Target}}}
	}
	return nil
}

// replacedTargetRollbackSafe is the facade's half of the DST-3 ruling for
// a cross-process re-run that finds the target replaced: the source still
// reads read-only under the run's attributable fence (the intent's
// @@server_uuid with super_read_only=ON) and holds all of F, and no other
// site reads writable. The engine adds the durable-record half.
func (tm *TopologyManager) replacedTargetRollbackSafe(ctx context.Context, p *runnerPlannedTopology, ref PlannedRunRef,
	pf *v1alpha1.PlannedFailoverStatus, F mysql.GTIDSet) bool {
	src := tm.getSite(ref.Source)
	if src == nil {
		return false
	}
	id, err := readSourceIdentity(ctx, pooledIdentity{src.mysql})
	if err != nil || !id.readOnly || !id.superReadOnly || p.fenceAttribution(ref.RunID, pf.Fence, id.uuid) == FenceUnattributed {
		return false
	}
	have, err := mysql.ParseGTIDSet(id.gtid)
	if err != nil || !have.Contains(F) {
		return false
	}
	// The target was just read read-only on its pinned session. Every other
	// promotable peer must positively read read-only: an unreadable
	// primary-candidate or dr-only site may be the writable one (strict
	// evidence, as §4.1), so the conservative hold stands.
	peers := classifyPlannedPeers(tm.plannedProbeSites(ctx, ref.Source, ref.Target))
	return len(peers.writable) == 0 && len(peers.unreadable) == 0
}

func mergeWarnings(a, b []v1alpha1.PlannedFailoverWarning) []v1alpha1.PlannedFailoverWarning {
	out := slices.Clone(a)
	for _, w := range b {
		if !slices.Contains(out, w) {
			out = append(out, w)
		}
	}
	return out
}

// plannedPreWriteGate is §7.2, run on sess immediately before
// SET super_read_only=OFF. A refusal is a *plannedGateRefusal with the
// mapped outcome; nothing has been written to the target by this attempt.
// Any other error is retried by the next pass (Unconfirmed, inside the
// promotion deadline). On success it also returns the GTID set it read on
// sess, the set the clears are about to make writable.
func (tm *TopologyManager) plannedPreWriteGate(ctx context.Context, p *runnerPlannedTopology, ref PlannedRunRef,
	pf *v1alpha1.PlannedFailoverStatus, sess mysql.Session, F mysql.GTIDSet, U string, wasWritable bool) ([]v1alpha1.PlannedFailoverWarning, string, error) {
	// 1. Leader and claim.
	if err := tm.requireRunClaim(p.leader(), ref, false); err != nil {
		return nil, "", err
	}
	refuse := func(kind PromoteOutcomeKind, reason v1alpha1.PlannedFailoverReason, terminal v1alpha1.PlannedFailoverOutcome, sites []string) error {
		tm.logger.Warn("planned failover: pre-write gate refused", "runId", ref.RunID, "reason", string(reason), "sites", sites)
		return &plannedGateRefusal{out: PromoteOutcome{Kind: kind, Reason: reason, Terminal: terminal, TargetServerUUID: U,
			TargetReachable: true, MayHaveWritten: wasWritable, Sites: slices.Clone(sites)}}
	}
	// The target's role, per the current manager's configuration, right
	// before the write (I001): nothing has been written by this attempt.
	if err := tm.checkPlannedPromotionManager(p, ref, U, wasWritable); err != nil {
		var refusal *plannedGateRefusal
		if errors.As(err, &refusal) {
			return nil, "", refuse(PromoteAborted, v1alpha1.PlannedFailoverReasonTargetNotPromotable, "", []string{ref.Target})
		}
		return nil, "", err
	}
	// 2. The target, on the session that will be written.
	live, err := readPromotionLive(ctx, sess)
	if err != nil {
		return nil, "", err
	}
	extra := live.gtid.Subtract(F)
	gtidOK := live.gtid.Contains(F) && extra.IsEmpty()
	if wasWritable {
		gtidOK = live.gtid.Contains(F) && ownExtras(extra, U)
	}
	if !live.readOnly || live.repl != nil || !strings.EqualFold(live.uuid, U) || !gtidOK {
		if wasWritable {
			return nil, "", refuse(PromoteInconsistent, v1alpha1.PlannedFailoverReasonExecuteFailed, "", []string{ref.Target})
		}
		return nil, "", refuse(PromoteAborted, v1alpha1.PlannedFailoverReasonTargetChanged, "", []string{ref.Target})
	}
	// 3. The source (pooled: it is not the server being written).
	var warnings []v1alpha1.PlannedFailoverWarning
	src := tm.getSite(ref.Source)
	if src == nil {
		return nil, "", errSiteNotFound
	}
	id, err := readSourceIdentity(ctx, pooledIdentity{src.mysql})
	switch {
	case err != nil && wasWritable:
		// The target already holds the run's own committed writes, so a
		// lost source is no reason to name the source: like an unreadable
		// peer, the run ends PromotedUnconfirmed and names the target.
		return nil, "", refuse(PromoteAborted, v1alpha1.PlannedFailoverReasonSourceLost, v1alpha1.PlannedFailoverOutcomePromotedUnconfirmed, []string{ref.Source})
	case err != nil:
		// One failed read is no evidence of a lost source (§6.2): retry
		// within the loss window, then roll back with SourceLost, so §9
		// handles the target channel this promotion already removed.
		if !p.lossExpired(p.sourceLoss, ref.RunID, tm.clock.Now(), time.Time{}) {
			return nil, "", fmt.Errorf("planned failover: the source %q did not answer at the pre-write gate: %w", ref.Source, err)
		}
		return nil, "", refuse(PromoteAborted, v1alpha1.PlannedFailoverReasonSourceLost, "", []string{ref.Source})
	}
	p.lossRecovered(p.sourceLoss, ref.RunID)
	switch {
	case wasWritable && !id.readOnly:
		return nil, "", refuse(PromoteInconsistent, v1alpha1.PlannedFailoverReasonForeignWritable, "", []string{ref.Source})
	case wasWritable && sourceAdvancedPast(id, F, U):
		// Both the target (its own writes) and the source (transactions
		// committed after the fence was lifted and set again) hold history
		// the other lacks.
		return nil, "", refuse(PromoteInconsistent, v1alpha1.PlannedFailoverReasonForeignWritable, "", []string{ref.Source})
	case wasWritable && (pf.Fence == nil || id.uuid != pf.Fence.SourceServerUUID):
		warnings = append(warnings, v1alpha1.PlannedFailoverWarningSourceReplaced)
	case !wasWritable && (!id.superReadOnly || pf.Fence == nil || id.uuid != pf.Fence.SourceServerUUID):
		return nil, "", refuse(PromoteAborted, v1alpha1.PlannedFailoverReasonSourceUnfenced, "", []string{ref.Source})
	case !wasWritable && sourceAdvancedPast(id, F, U):
		// The source's fence was lifted and set again after F: promoting a
		// target that holds only F would lose those transactions. Nothing
		// was written to the target; the rollback catches it up.
		return nil, "", refuse(PromoteAborted, v1alpha1.PlannedFailoverReasonAuthorityChanged, "", []string{ref.Source})
	}
	// 4. Every other site.
	v := classifyPlannedPeers(tm.plannedProbeSites(ctx, ref.Source, ref.Target))
	p.hooks.yield(YieldGateProbeForSim)
	switch {
	case len(v.writable) > 0 && wasWritable:
		return nil, "", refuse(PromoteInconsistent, v1alpha1.PlannedFailoverReasonForeignWritable, "", v.writable)
	case len(v.writable) > 0:
		return nil, "", refuse(PromoteAborted, v1alpha1.PlannedFailoverReasonForeignWritable, "", v.writable)
	case len(v.unreadable) > 0 && wasWritable:
		return nil, "", refuse(PromoteAborted, v1alpha1.PlannedFailoverReasonPeerUnreadable, v1alpha1.PlannedFailoverOutcomePromotedUnconfirmed, v.unreadable)
	case len(v.unreadable) > 0:
		return nil, "", refuse(PromoteAborted, v1alpha1.PlannedFailoverReasonPeerUnreadable, "", v.unreadable)
	}
	if len(v.unreadableReaders) > 0 {
		warnings = append(warnings, v1alpha1.PlannedFailoverWarningUnreachablePeers)
	}
	return warnings, live.gtidRaw, nil
}

// confirmPlannedPromotion is §7.3: every check live, on the session that
// was written and on the other sites.
func (tm *TopologyManager) confirmPlannedPromotion(ctx context.Context, hooks SimHooks, ref PlannedRunRef, pf *v1alpha1.PlannedFailoverStatus,
	sess mysql.Session, F mysql.GTIDSet, U string) PromoteOutcome {
	out := PromoteOutcome{TargetServerUUID: U, TargetReachable: true, MayHaveWritten: true}
	live, err := readPromotionLive(ctx, sess)
	if err != nil {
		out.Kind, out.Reason, out.TargetReachable = PromoteUnconfirmed, v1alpha1.PlannedFailoverReasonPromotionUnconfirmed, false
		return out
	}
	extra := live.gtid.Subtract(F)
	switch {
	case live.readOnly:
		// Re-fenced or restarted after the clear: re-classify next pass.
		out.Kind, out.Reason = PromoteUnconfirmed, v1alpha1.PlannedFailoverReasonPromotionUnconfirmed
		return out
	case live.repl != nil || !live.gtid.Contains(F) || !ownExtras(extra, U):
		out.Kind, out.Reason = PromoteInconsistent, v1alpha1.PlannedFailoverReasonForeignWritable
		return out
	case !strings.EqualFold(live.uuid, U):
		out.Kind, out.Reason = PromoteInconsistent, v1alpha1.PlannedFailoverReasonExecuteFailed
		return out
	}
	if src := tm.getSite(ref.Source); src != nil {
		id, err := readSourceIdentity(ctx, pooledIdentity{src.mysql})
		switch {
		case err != nil:
			out.Warnings = append(out.Warnings, v1alpha1.PlannedFailoverWarningUnreachablePeers)
		case !id.readOnly, sourceAdvancedPast(id, F, U):
			// A writable source, or one whose history moved past F after
			// the fence: two sites hold history the other lacks.
			out.Kind, out.Reason = PromoteInconsistent, v1alpha1.PlannedFailoverReasonForeignWritable
			return out
		case pf.Fence == nil || id.uuid != pf.Fence.SourceServerUUID:
			out.Warnings = append(out.Warnings, v1alpha1.PlannedFailoverWarningSourceReplaced)
		}
	}
	v := classifyPlannedPeers(tm.plannedProbeSites(ctx, ref.Source, ref.Target))
	hooks.yield(YieldConfirmProbeForSim)
	if len(v.writable) > 0 {
		out.Kind, out.Reason = PromoteInconsistent, v1alpha1.PlannedFailoverReasonForeignWritable
		return out
	}
	if len(v.allUnreadable()) > 0 && !slices.Contains(out.Warnings, v1alpha1.PlannedFailoverWarningUnreachablePeers) {
		out.Warnings = append(out.Warnings, v1alpha1.PlannedFailoverWarningUnreachablePeers)
	}
	out.Kind, out.Reason = PromoteConfirmed, ""
	return out
}
