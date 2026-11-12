package controller

import (
	"context"
	"fmt"
	"slices"
	"strings"
	"sync"
	"time"

	"k8s.io/apimachinery/pkg/types"

	v1alpha1 "github.com/shipstream/bloodraven/api/v1alpha1"
	"github.com/shipstream/bloodraven/internal/mysql"
)

// The planned-failover claim.
//
// A planned failover owns the group's sites through one claim slot per
// group, kept on the runner and shared by every topology-manager
// generation of the group (WireRegisteredManager hands the same pointer to
// each before its Run). The claim is granted only under tm.mu, against
// every other claimant in topology_claims.go, and only after live peer
// evidence; it is hydrated only from uncached reads, and only the owning
// run releases it. Lock order: reservationMu → r.mu → tm.mu → slot.mu.
// slot.mu is a leaf: nothing is called while it is held.

const (
	// plannedClaimGateTTL is how long a waiting claim's entry at the
	// follower-mutation gate refuses new mutations without being renewed.
	plannedClaimGateTTL = 15 * time.Second
	// plannedClaimGateInterval spaces a run's gate attempts.
	plannedClaimGateInterval = time.Second
)

// PlannedClaimSlot is a group's planned-failover claim. The runner keeps one
// per group (like dragonflyGates) and hands the same pointer to every
// topology manager generation before its Run. The zero value is an empty
// slot.
type PlannedClaimSlot struct {
	mu   sync.Mutex // leaf: tm.mu → slot.mu, never the reverse
	held *plannedClaim
	// releaseGen counts releases, and recent keeps the newest few with
	// the generation each made. startManager stamps the generation before
	// its uncached read, so a registration whose read predates a run's
	// release does not hydrate that run's claim back (K004).
	releaseGen uint64
	recent     [8]releasedRun
	// releases is instrumentation (tests and DST): runId, why. It keeps
	// the newest maxClaimReleaseNotes; the simulator drains it each step.
	releases []ClaimReleaseNote
}

// releasedRun is one release: the run and the generation it made.
type releasedRun struct {
	gen   uint64
	runID string
}

// releasedSince reports whether runID's claim (either mode) was released
// after generation gen. A release too old to be in recent is not
// reported: hydrating then fails closed, and the engine's zombie and
// orphan-hold releases clear a stale claim on its next pass.
func (s *PlannedClaimSlot) releasedSince(gen uint64, runID string) bool {
	if s == nil {
		return false
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, r := range s.recent {
		if r.gen > gen && r.runID == runID {
			return true
		}
	}
	return false
}

// maxClaimReleaseNotes bounds PlannedClaimSlot.releases on a long-lived
// leader.
const maxClaimReleaseNotes = 32

// noteLocked records one release note, keeping the newest
// maxClaimReleaseNotes. Must hold s.mu.
func (s *PlannedClaimSlot) noteLocked(n ClaimReleaseNote) {
	if len(s.releases) >= maxClaimReleaseNotes {
		s.releases = slices.Delete(s.releases, 0, len(s.releases)-maxClaimReleaseNotes+1)
	}
	s.releases = append(s.releases, n)
}

// generation returns how many releases the slot has seen.
func (s *PlannedClaimSlot) generation() uint64 {
	if s == nil {
		return 0
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.releaseGen
}

// ClaimReleaseNote records one release (or conversion to a hold).
type ClaimReleaseNote struct {
	RunID string
	Why   ClaimReleaseReason
}

type plannedClaimMode int

const (
	claimModeRun  plannedClaimMode = iota // the run owns the sites
	claimModeHold                         // ManualRecoveryRequired: automation held until ack
)

func (m plannedClaimMode) String() string {
	if m == claimModeHold {
		return "Hold"
	}
	return "Run"
}

type plannedClaim struct {
	mode                  plannedClaimMode
	runID, source, target string
	sourceUUID            string             // the grant probe's source @@server_uuid
	baseline              FailoverRecordRef  // second precision, compared with sameFailoverRecord
	needsReprove          bool               // hydrated: re-prove before the next side effect
	provisional           bool               // Fencing write not yet Committed
	promotion             *recordedPromotion // {confirmedAt} only; the watermark always comes from the run's fence.gtidSet
}

// recordedPromotion is what RecordPlannedPromotion leaves on the claim, so
// a replacement manager of the same process applies it before Run.
type recordedPromotion struct {
	confirmedAt time.Time
}

// PlannedClaimView is a copy of a held claim, for tests and the
// simulator.
type PlannedClaimView struct {
	Mode         string // "Run" or "Hold"
	RunID        string
	Source       string
	Target       string
	Provisional  bool
	NeedsReprove bool
	Promoted     bool // RecordPlannedPromotion ran for the run
}

// View returns the held claim, or ok=false when the slot is empty.
func (s *PlannedClaimSlot) View() (v PlannedClaimView, ok bool) {
	if s == nil {
		return PlannedClaimView{}, false
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.held == nil {
		return PlannedClaimView{}, false
	}
	h := s.held
	return PlannedClaimView{
		Mode: h.mode.String(), RunID: h.runID, Source: h.source, Target: h.target,
		Provisional: h.provisional, NeedsReprove: h.needsReprove, Promoted: h.promotion != nil,
	}, true
}

// ReleaseNotes returns every release recorded on the slot, oldest first.
func (s *PlannedClaimSlot) ReleaseNotes() []ClaimReleaseNote {
	if s == nil {
		return nil
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	return slices.Clone(s.releases)
}

// Held reports whether the slot holds a claim in either mode.
func (s *PlannedClaimSlot) Held() bool {
	if s == nil {
		return false
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.held != nil
}

// snapshot returns a copy of the held claim.
func (s *PlannedClaimSlot) snapshot() (plannedClaim, bool) {
	if s == nil {
		return plannedClaim{}, false
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.held == nil {
		return plannedClaim{}, false
	}
	c := *s.held
	if c.promotion != nil {
		p := *c.promotion
		c.promotion = &p
	}
	return c, true
}

// hydrate sets c only if the slot is empty. Hydration never clears or
// replaces a held claim.
func (s *PlannedClaimSlot) hydrate(c plannedClaim) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.held != nil {
		return false
	}
	s.held = &c
	return true
}

// update applies fn to the held claim if it is runID's Run-mode claim.
func (s *PlannedClaimSlot) update(runID string, fn func(*plannedClaim)) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.held == nil || s.held.mode != claimModeRun || s.held.runID != runID {
		return false
	}
	fn(s.held)
	return true
}

// release empties the slot if it holds runID, and records why. A Hold
// claim is released only by an acknowledgement or as an orphan (no durable
// hold record is left), and a Run claim by neither; a zombie release
// applies to a Run claim only.
func (s *PlannedClaimSlot) release(runID string, why ClaimReleaseReason) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.held == nil || s.held.runID != runID {
		return false
	}
	if (s.held.mode == claimModeHold) != (why == ReleaseAck || why == ReleaseOrphanHold) {
		return false
	}
	s.held = nil
	s.releaseGen++
	s.recent[s.releaseGen%uint64(len(s.recent))] = releasedRun{gen: s.releaseGen, runID: runID}
	s.noteLocked(ClaimReleaseNote{RunID: runID, Why: why})
	return true
}

// toHold switches runID's claim, in either mode, to a hold. It also
// installs a hold for runID in an empty slot (a marker that landed after
// the claim was gone): the hold must be in force either way. It returns
// false, recording nothing, when the slot already holds runID's hold: a
// standing hold is re-asserted on every pass and must not grow the release
// notes or repeat the log line.
func (s *PlannedClaimSlot) toHold(runID string) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	switch {
	case s.held != nil && s.held.runID == runID && s.held.mode == claimModeHold:
		return false
	case s.held == nil:
		s.held = &plannedClaim{mode: claimModeHold, runID: runID}
	case s.held.runID != runID:
		return false
	default:
		s.held.mode = claimModeHold
		s.held.provisional, s.held.needsReprove = false, false
	}
	s.noteLocked(ClaimReleaseNote{RunID: runID, Why: ReleaseToHold})
	return true
}

// plannedClaimHeldLocked reports whether the group's planned-failover
// claim slot is held, in either mode (a run, its provisional grant, or a
// hold). It is the only planned-failover guard: nothing derives it from
// status. Must hold tm.mu (read or write).
func (tm *TopologyManager) plannedClaimHeldLocked() bool {
	return tm.plannedSlot.Held()
}

// plannedClaimHeld is plannedClaimHeldLocked under tm.mu.
func (tm *TopologyManager) plannedClaimHeld() bool {
	tm.mu.RLock()
	defer tm.mu.RUnlock()
	return tm.plannedClaimHeldLocked()
}

// plannedClaimSlot returns the slot WireRegisteredManager installed, or
// nil.
func (tm *TopologyManager) plannedClaimSlot() *PlannedClaimSlot {
	tm.mu.RLock()
	defer tm.mu.RUnlock()
	return tm.plannedSlot
}

// claimDecisionWait and claimDecisionReject build decisions.
func claimDecisionWait(reason v1alpha1.PlannedFailoverWaitReason, format string, args ...any) PlannedClaimDecision {
	return PlannedClaimDecision{Kind: ClaimWait, WaitReason: reason, Message: fmt.Sprintf(format, args...)}
}

func claimDecisionReject(reason v1alpha1.PlannedFailoverReason, format string, args ...any) PlannedClaimDecision {
	return PlannedClaimDecision{Kind: ClaimReject, RejectReason: reason, Message: fmt.Sprintf(format, args...)}
}

func claimDecisionGranted(c *plannedClaim) PlannedClaimDecision {
	return PlannedClaimDecision{Kind: ClaimGranted, Source: c.source, SourceServerUUID: c.sourceUUID, Baseline: c.baseline}
}

// plannedClaimRefusalLocked decides every tm-local condition of the grant
// (§3.2 step 1) for req. It returns decided=true with the decision when
// the claim cannot be granted now, or when the same run already holds it
// (an idempotent Granted). Otherwise it returns the poll-named source.
// Must hold tm.mu.
func (tm *TopologyManager) plannedClaimRefusalLocked(req PlannedClaimRequest) (d PlannedClaimDecision, polled string, decided bool) {
	if held, ok := tm.plannedSlot.snapshot(); ok {
		switch {
		case held.mode == claimModeHold:
			return claimDecisionReject(v1alpha1.PlannedFailoverReasonManualRecoveryPending,
				"automatic failover is held after planned failover run %s; acknowledge the hold first", held.runID), "", true
		case held.runID != req.RunID:
			return claimDecisionWait(v1alpha1.PlannedFailoverWaitClaimHeld,
				"planned failover run %s holds the claim", held.runID), "", true
		case held.target != req.Target:
			return claimDecisionReject(v1alpha1.PlannedFailoverReasonInternalError,
				"run %s holds the claim for target %q, not %q", held.runID, held.target, req.Target), "", true
		default:
			return claimDecisionGranted(&held), held.source, true
		}
	}
	target := tm.getSite(req.Target)
	switch {
	case target == nil:
		return claimDecisionReject(v1alpha1.PlannedFailoverReasonUnknownSite, "site %q is not in the topology", req.Target), "", true
	case !target.isPromotable():
		return claimDecisionReject(v1alpha1.PlannedFailoverReasonTargetNotPromotable,
			"site %q has role %s; only PrimaryCandidate sites may be promoted", req.Target, APISiteRole(target.role)), "", true
	case !tm.ready:
		return claimDecisionWait(v1alpha1.PlannedFailoverWaitManagerNotReady, "the topology manager has not completed a poll"), "", true
	case tm.retired:
		return claimDecisionWait(v1alpha1.PlannedFailoverWaitManagerRetiring, "the topology manager is being replaced"), "", true
	case tm.predecessorUpdateInFlight:
		return claimDecisionWait(v1alpha1.PlannedFailoverWaitPredecessorBusy, "a replaced topology manager's ordered update is still running"), "", true
	case tm.deploymentChanging > 0 || tm.promotionUnobservedLocked():
		return claimDecisionWait(v1alpha1.PlannedFailoverWaitPromotionInFlight, "a promotion is in flight or not yet observed"), "", true
	case tm.deploymentUpdateRunning:
		return claimDecisionWait(v1alpha1.PlannedFailoverWaitUpdateInFlight, "an ordered update holds the sites"), "", true
	case tm.topologyFrozen:
		return claimDecisionWait(v1alpha1.PlannedFailoverWaitRestoreInFlight, "an in-place restore holds the sites"), "", true
	case tm.bootstrapBlocksCrossSiteLocked():
		return claimDecisionWait(v1alpha1.PlannedFailoverWaitBootstrapInFlight, "a bootstrap of a promotable site is running"), "", true
	case slices.Contains(tm.keyringRotationBlocked, req.Target):
		return claimDecisionReject(v1alpha1.PlannedFailoverReasonKeyringRotation,
			"site %q is mid-keyring-rotation; wait for it to be Sealed and re-annotate", req.Target), "", true
	case slices.Contains(tm.keyringRotationReserved, req.Target):
		return claimDecisionWait(v1alpha1.PlannedFailoverWaitKeyringRotation, "a keyring rotation of %q is being admitted", req.Target), "", true
	}
	if rec := tm.recovery[req.Target]; rec != nil && rec.state == v1alpha1.RecoveryStateBlocked {
		return claimDecisionReject(v1alpha1.PlannedFailoverReasonDivergentTarget,
			"site %q has %d transactions the primary lacks (%s)", req.Target, rec.divergentCount, truncateGtidSet(rec.divergentGtid)), "", true
	}
	if tm.restoreReclonePendingLocked(req.Target) {
		return claimDecisionReject(v1alpha1.PlannedFailoverReasonTargetUnhealthy,
			"site %q still awaits its post-restore reclone", req.Target), "", true
	}
	if rec := tm.ownLoss[req.Target]; rec != nil {
		return claimDecisionReject(v1alpha1.PlannedFailoverReasonTargetUnhealthy,
			"site %q lacks %d transactions it issued itself (%s): %s", req.Target, rec.lostCount, truncateGtidSet(rec.lostGtid),
			tm.ownLossPromotionRemedyLocked(req.Target)), "", true
	}
	polled = tm.activeSiteLocked()
	switch {
	case polled == req.Target:
		return claimDecisionReject(v1alpha1.PlannedFailoverReasonAlreadyActive, "site %q is already the active primary", req.Target), "", true
	case polled == "":
		return claimDecisionWait(v1alpha1.PlannedFailoverWaitAuthorityUnsettled, "the topology manager names no unique writable primary"), "", true
	}
	return PlannedClaimDecision{}, polled, false
}

// claimPlannedFailover is §3.2 steps 1 to 5 on this manager: the fast path,
// live peer evidence with no lock held, the durable-baseline check, the
// non-blocking follower-mutation gate, and the publish that re-decides
// every tm-local condition under tm.mu. The caller has done step 0 (the
// retiring managers) with no lock held. It never calls a callback that
// takes r.mu while holding tm.mu, and holds no lock across a probe.
func (tm *TopologyManager) claimPlannedFailover(ctx context.Context, req PlannedClaimRequest, hooks SimHooks) (PlannedClaimDecision, error) {
	if tm.plannedClaimSlot() == nil {
		return claimDecisionWait(v1alpha1.PlannedFailoverWaitManagerNotReady, "the topology manager is not wired to a claim slot"), nil
	}
	if tm.predecessorMutating() {
		return claimDecisionWait(v1alpha1.PlannedFailoverWaitPredecessorBusy, "a replaced topology manager is still changing the sites"), nil
	}
	tm.mu.RLock()
	d, polled, decided := tm.plannedClaimRefusalLocked(req)
	epoch := tm.authorityEpoch
	inMemory := newFailoverRecordRef(tm.lastFailover, tm.lastFailoverTarget)
	tm.mu.RUnlock()
	if decided {
		return d, nil
	}

	ev := tm.plannedGrantEvidence(ctx, polled, req.Target)
	hooks.yield(YieldPeerProbeForSim)
	if ev.wait != nil {
		return *ev.wait, nil
	}
	if !sameFailoverRecord(inMemory, req.DurableBaseline) {
		return claimDecisionWait(v1alpha1.PlannedFailoverWaitAuthorityUnsettled,
			"the durable failover record (%s) has not caught up with the topology manager's (%s)",
			describeFailoverRecordRef(req.DurableBaseline), describeFailoverRecordRef(inMemory)), nil
	}

	// A run whose last gate attempt found a follower mutation running
	// tries again at most once per plannedClaimGateInterval.
	tm.mu.RLock()
	throttled := tm.plannedGateRunID == req.RunID && tm.clock.Since(tm.plannedGateAttemptAt) < plannedClaimGateInterval
	tm.mu.RUnlock()
	if throttled {
		return claimDecisionWait(v1alpha1.PlannedFailoverWaitRecoveryInFlight, "a follower mutation was still running at the last gate attempt"), nil
	}

	hooks.yield(YieldBeforePublishForSim)
	var out PlannedClaimDecision
	err := tm.followerMutationGate.tryAdmit(req.RunID, plannedClaimGateTTL, func() {
		out = tm.publishPlannedClaim(req, epoch, polled, inMemory, ev.sourceUUID)
	})
	if err != nil {
		tm.mu.Lock()
		tm.plannedGateRunID, tm.plannedGateAttemptAt = req.RunID, tm.clock.Now()
		tm.mu.Unlock()
		return claimDecisionWait(v1alpha1.PlannedFailoverWaitRecoveryInFlight, "a recovery or source-convergence step is still running on the sites"), nil
	}
	hooks.yield(YieldAfterPublishForSim)
	if out.Kind == ClaimGranted {
		tm.logger.Info("planned failover: claim granted", "runId", req.RunID, "source", out.Source, "target", req.Target)
	}
	return out, nil
}

// publishPlannedClaim is §3.2 step 4: under tm.mu then slot.mu it
// re-decides every tm-local condition, the authority epoch and the poll's
// active site, and on success installs the provisional claim. The
// predecessor checks are not repeated: they would need r.mu under tm.mu,
// and a retired manager starts nothing new, so "predecessor busy" only
// goes from true to false.
func (tm *TopologyManager) publishPlannedClaim(req PlannedClaimRequest, epoch uint64, polled string, inMemory FailoverRecordRef, sourceUUID string) PlannedClaimDecision {
	tm.mu.Lock()
	defer tm.mu.Unlock()
	d, now, decided := tm.plannedClaimRefusalLocked(req)
	switch {
	case decided:
		return d
	case tm.authorityEpoch != epoch || now != polled:
		return claimDecisionWait(v1alpha1.PlannedFailoverWaitAuthorityUnsettled, "authority moved while the claim was being decided")
	case !sameFailoverRecord(newFailoverRecordRef(tm.lastFailover, tm.lastFailoverTarget), inMemory):
		return claimDecisionWait(v1alpha1.PlannedFailoverWaitAuthorityUnsettled, "a failover was recorded while the claim was being decided")
	}
	c := plannedClaim{
		mode: claimModeRun, runID: req.RunID, source: polled, target: req.Target,
		sourceUUID: sourceUUID, baseline: inMemory, provisional: true,
	}
	if !tm.plannedSlot.hydrate(c) {
		return claimDecisionWait(v1alpha1.PlannedFailoverWaitClaimHeld, "another run took the claim")
	}
	tm.authorityEpoch++
	tm.crossSiteRetry = true
	return claimDecisionGranted(&c)
}

func describeFailoverRecordRef(r FailoverRecordRef) string {
	if r.LastFailover.IsZero() && r.LastFailoverTarget == "" {
		return "no failover"
	}
	return fmt.Sprintf("%s at %s", r.LastFailoverTarget, r.LastFailover.UTC().Format(time.RFC3339))
}

// requireRunClaim is §3.3: immediately before a mutating facade call's
// first statement, the leader context is alive, the slot holds ref in Run
// mode with the same source and target, and (unless the call is the
// re-proof) the claim needs no re-proof.
func (tm *TopologyManager) requireRunClaim(leader context.Context, ref PlannedRunRef, reprove bool) error {
	if leader != nil && leader.Err() != nil {
		return errLeaderLost
	}
	tm.mu.RLock()
	held, ok := tm.plannedSlot.snapshot()
	tm.mu.RUnlock()
	switch {
	case !ok || held.mode != claimModeRun || held.runID != ref.RunID || held.source != ref.Source || held.target != ref.Target:
		return errClaimLost
	case held.needsReprove && !reprove:
		return errReproveRequired
	}
	if held.provisional {
		// Every mutating call runs only after the record that announces it
		// committed, and the first one follows the committed Fencing write:
		// the grant is no longer provisional.
		tm.plannedSlot.update(ref.RunID, func(c *plannedClaim) { c.provisional = false })
	}
	return nil
}

// hydratedRunClaim builds the Run claim an owns-sites block hydrates to,
// or ok=false when pf cannot be hydrated (no runId, no source, or a
// phase that owns nothing).
func hydratedRunClaim(pf *v1alpha1.PlannedFailoverStatus) (plannedClaim, bool) {
	if pf == nil || pf.RunID == "" || pf.SourceSite == "" || pf.TargetSite == "" {
		return plannedClaim{}, false
	}
	if !plannedFailoverOwnsSites(&v1alpha1.MysqlFailoverGroup{Status: v1alpha1.MysqlFailoverGroupStatus{PlannedFailover: pf}}) {
		return plannedClaim{}, false
	}
	return plannedClaim{
		mode: claimModeRun, runID: pf.RunID, source: pf.SourceSite, target: pf.TargetSite,
		baseline: failoverRecordRefFromAPI(pf.ClaimBaseline), needsReprove: true,
	}, true
}

// fillWireRegisteredManager is WireRegisteredManager's body.
func fillWireRegisteredManager(tm *TopologyManager, w ManagerWiring) {
	tm.restoreKeyringRotationReservations(w.PendingRotations)
	if w.RetiringUpdateInFlight {
		tm.setPredecessorUpdateInFlight(true)
	}
	// DST-1: a record a predecessor of this process made (and may not have
	// made durable) reaches the new manager before its first poll. No lock
	// of tm is held here: each carry takes the predecessor's lock, then
	// tm's, never both.
	if w.RetiredFailover != nil {
		tm.carryFailoverRecord(*w.RetiredFailover)
	}
	for _, old := range w.Predecessors {
		CarryRetiredFailoverRecord(tm, old)
	}
	tm.mu.Lock()
	defer tm.mu.Unlock()
	if w.RetiringBusy != nil {
		tm.predecessorBusy = w.RetiringBusy
	}
	if w.Slot == nil {
		return
	}
	tm.plannedSlot = w.Slot
	switch {
	case w.HoldFrom != nil:
		if w.Slot.hydrate(plannedClaim{mode: claimModeHold, runID: w.HoldFrom.RunID}) {
			tm.logger.Warn("planned failover: automation held pending acknowledgement",
				"runId", w.HoldFrom.RunID, "sites", strings.Join(w.HoldFrom.Sites, ","))
		}
	case w.HydrateFrom != nil:
		if c, ok := hydratedRunClaim(w.HydrateFrom); ok {
			w.Slot.hydrate(c)
		}
	}
	held, ok := w.Slot.snapshot()
	if !ok || held.mode != claimModeRun || held.promotion == nil {
		return
	}
	// The run recorded its promotion into an earlier generation of this
	// process; bring it into this one. The watermark always comes from
	// the run's uncached fence.gtidSet. Without it (the uncached read
	// failed) nothing is applied: moving lastFailover forward beside the
	// old watermark would let a later equal-second write keep the stale
	// one (OPEN-2). The predecessor's own record was carried above, and
	// the engine records the run into this manager again.
	if pf := w.HydrateFrom; pf != nil && pf.RunID == held.runID && pf.Fence != nil && pf.Fence.GtidSet != nil {
		tm.applyPlannedPromotionLocked(held.promotion.confirmedAt, held.target, *pf.Fence.GtidSet)
	}
}

// CarryRetiredFailoverRecord brings old's in-memory failover record into
// tm under the timestamp-monotonic rule (DST-1): a retiring manager's
// ordered-update handoff, recorded only in its memory and dropped from
// status (a replaced manager's status writes are discarded) and from the
// out-of-band store (its flush retries stop with its Run), would otherwise
// be lost to the replacement, which could then promote inside the cooldown
// or name the wrong last failover target. It takes old's lock, then tm's,
// never both; the runner calls it at registration and again after the
// retired manager's update workers exit.
func CarryRetiredFailoverRecord(tm, old *TopologyManager) bool {
	if tm == nil || old == nil || tm == old {
		return false
	}
	rec, ok := old.failoverRecordSnapshot()
	if !ok {
		return false
	}
	return tm.carryFailoverRecord(rec)
}

// failoverRecordSnapshot is tm's in-memory failover triple.
func (tm *TopologyManager) failoverRecordSnapshot() (FailoverRecord, bool) {
	tm.mu.RLock()
	defer tm.mu.RUnlock()
	if tm.lastFailover.IsZero() || tm.lastFailoverTarget == "" {
		return FailoverRecord{}, false
	}
	return FailoverRecord{LastFailover: tm.lastFailover, LastFailoverTarget: tm.lastFailoverTarget,
		PromotionGtidExecuted: tm.promotionGtidExecuted}, true
}

// carryFailoverRecord applies rec iff it is strictly newer than tm's record
// (second precision; an equal one is the same event or loses the tie, as
// in applyPlannedPromotionLocked), and marks it for tm's next status write
// and out-of-band flush, which persist it.
func (tm *TopologyManager) carryFailoverRecord(rec FailoverRecord) bool {
	if rec.LastFailover.IsZero() || rec.LastFailoverTarget == "" {
		return false
	}
	if tm.getSite(rec.LastFailoverTarget) == nil {
		// A site the replacement's configuration no longer has (removed
		// or renamed): startup rejects such a durable record, and a
		// carried one would name a site nothing can reach (K076).
		tm.logger.Info("not carrying a retired topology manager's failover record: its target is no longer a site",
			"target", rec.LastFailoverTarget, "lastFailover", rec.LastFailover)
		return false
	}
	tm.mu.Lock()
	defer tm.mu.Unlock()
	at := rec.LastFailover.UTC().Truncate(time.Second)
	if !tm.lastFailover.IsZero() && !at.After(tm.lastFailover.UTC().Truncate(time.Second)) {
		return false
	}
	tm.lastFailover, tm.lastFailoverTarget = rec.LastFailover, rec.LastFailoverTarget
	tm.lastFailoverLocal = true
	tm.nameActiveSiteLocked(rec.LastFailoverTarget, true)
	if rec.PromotionGtidExecuted != "" {
		tm.promotionGtidExecuted = rec.PromotionGtidExecuted
		tm.groupHasData = true
	}
	tm.deploymentAuthorityLocked(rec.LastFailoverTarget)
	if tm.desiredFailoverState == nil || tm.desiredFailoverState.LastFailover.Before(rec.LastFailover) {
		cp := rec
		tm.desiredFailoverState = &cp
		tm.failoverStateDirty = true
	}
	// The next poll writes status even with nothing else changed, and a
	// replayed retry snapshot carries the record.
	tm.statusWriteFailed = true
	if tm.statusRetrySnapshot != nil {
		snap := *tm.statusRetrySnapshot
		snap.LastFailover, snap.LastFailoverTarget, snap.PromotionGtidExecuted =
			tm.lastFailover, tm.lastFailoverTarget, tm.promotionGtidExecuted
		tm.statusRetrySnapshot = &snap
	}
	tm.logger.Info("carried a retired topology manager's failover record",
		"target", rec.LastFailoverTarget, "lastFailover", rec.LastFailover)
	return true
}

// applyPlannedPromotionLocked brings a planned promotion recorded
// elsewhere (the claim, or the run's durable Publishing or Succeeded
// record) into this manager's failover record under the timestamp-monotonic
// rule: it applies iff confirmedAt is not before the current lastFailover
// (second precision) and, on equal times, only when the target matches (an
// idempotent re-apply). Must hold tm.mu.
func (tm *TopologyManager) applyPlannedPromotionLocked(confirmedAt time.Time, target, watermark string) bool {
	if confirmedAt.IsZero() || target == "" {
		return false
	}
	at := confirmedAt.UTC().Truncate(time.Second)
	cur := tm.lastFailover.UTC().Truncate(time.Second)
	switch {
	case at.Before(cur):
		return false
	case at.Equal(cur) && !tm.lastFailover.IsZero() && tm.lastFailoverTarget != target:
		return false
	}
	tm.lastFailover, tm.lastFailoverTarget = at, target
	tm.nameActiveSiteLocked(target, true)
	if watermark != "" {
		tm.promotionGtidExecuted = watermark
		tm.groupHasData = true
	}
	return true
}

func (h SimHooks) yield(point string) {
	if h.Yield != nil {
		h.Yield(point)
	}
}

// stmt yields immediately before (or after) a facade statement on site.
func (h SimHooks) stmt(before bool, site, statement string) {
	if h.Yield == nil {
		return
	}
	point := YieldAfterStmtForSim
	if before {
		point = YieldBeforeStmtForSim
	}
	h.Yield(point + ":" + site + ":" + statement)
}

// statement names the statement a promote memo stage sends.
func (s promoteMemoStage) statement() string {
	switch s {
	case promoteMemoStopSent:
		return "STOP REPLICA"
	case promoteMemoResetSent:
		return "RESET REPLICA ALL"
	case promoteMemoSROClearSent:
		return "super_read_only=OFF"
	case promoteMemoROClearSent:
		return "read_only=OFF"
	}
	return ""
}

// plannedRunStartTime is when the run was accepted (zero if unknown).
func plannedRunStartTime(pf *v1alpha1.PlannedFailoverStatus) time.Time {
	if !pf.StartTime.IsZero() {
		return pf.StartTime.Time
	}
	if pf.ClaimTime != nil {
		return pf.ClaimTime.Time
	}
	return time.Time{}
}

// plannedFenceAttributableEnd reports whether the latest run ended
// SourceLeftFenced (by its terminal record, or by a terminal marker naming
// it) with a fence the topology manager may lift by a named re-assert
// (§6.5): the run's source is known, its fence was attributable, and the
// fence was not refused (a legacy run) or never sent because authority had
// already moved.
func plannedFenceAttributableEnd(fg *v1alpha1.MysqlFailoverGroup) (pf *v1alpha1.PlannedFailoverStatus, ok bool) {
	pf, marked := plannedFailoverRun(fg)
	if pf == nil || pf.RunID == "" || pf.SourceSite == "" || pf.Fence == nil {
		return nil, false
	}
	var reason v1alpha1.PlannedFailoverReason
	switch {
	case pf.Result != nil && pf.Result.Outcome == v1alpha1.PlannedFailoverOutcomeSourceLeftFenced:
		if slices.Contains(pf.Result.Warnings, v1alpha1.PlannedFailoverWarningFenceNotAttributable) {
			return nil, false
		}
		reason = pf.Result.Reason
	case marked:
		m, _ := ParseTerminalMarker(fg.GetAnnotations()[v1alpha1.PlannedFailoverTerminalAnnotation])
		if m.Outcome != v1alpha1.PlannedFailoverOutcomeSourceLeftFenced ||
			slices.Contains(m.Warnings, v1alpha1.PlannedFailoverWarningFenceNotAttributable) {
			return nil, false
		}
		reason = m.Reason
	default:
		return nil, false
	}
	switch {
	case reason == v1alpha1.PlannedFailoverReasonLegacyRunRefused:
		return nil, false
	case reason == v1alpha1.PlannedFailoverReasonAuthorityChanged && pf.Fence.State != v1alpha1.PlannedFailoverFenceApplied:
		return nil, false
	}
	return pf, true
}

// restorePlannedFailoverState brings the latest planned-failover run into
// a new manager from view, the start's uncached read (nil: nothing):
//   - a run that ended Succeeded (or is Publishing) applies its promotion
//     iff its confirmedAt is not before the restored lastFailover and, on
//     equal times, only when the target matches (an idempotent re-apply);
//   - a run that ended SourceLeftFenced with its own, attributable fence
//     names its source for re-assert (§6.5), unless a failover recorded
//     after the run started has moved authority since.
func (r *TopologyManagerRunner) restorePlannedFailoverState(tm *TopologyManager, view *v1alpha1.MysqlFailoverGroup, nn types.NamespacedName) {
	if view == nil {
		return
	}
	pf, _ := plannedFailoverRun(view)
	if pf == nil {
		return
	}
	tm.mu.Lock()
	defer tm.mu.Unlock()
	confirmed := pf.Phase == v1alpha1.PlannedFailoverPhasePublishing ||
		(pf.Result != nil && pf.Result.Outcome == v1alpha1.PlannedFailoverOutcomeSucceeded)
	if confirmed && pf.Promotion != nil && pf.Promotion.ConfirmationTime != nil && pf.Fence != nil && pf.Fence.GtidSet != nil {
		tm.applyPlannedPromotionLocked(pf.Promotion.ConfirmationTime.Time, pf.TargetSite, *pf.Fence.GtidSet)
	} else if pf.Phase == v1alpha1.PlannedFailoverPhasePromoting && pf.Promotion != nil && pf.Fence != nil && pf.Fence.GtidSet != nil {
		// A confirmed tail that released its claim before Publishing
		// committed left its confirmation in the planned-failover-confirmed
		// annotation (K014): the run's record may not be durable yet.
		if at, ok := parseDeadlineStart(view.GetAnnotations()[v1alpha1.PlannedFailoverConfirmedAnnotation], pf.RunID); ok {
			tm.applyPlannedPromotionLocked(at, pf.TargetSite, *pf.Fence.GtidSet)
		}
	}
	src, ok := plannedFenceAttributableEnd(view)
	if !ok {
		return
	}
	started := plannedRunStartTime(src).UTC().Truncate(time.Second)
	if tm.lastFailover.UTC().Truncate(time.Second).After(started) {
		return
	}
	tm.nameFencedSourceLocked(src.RunID, src.SourceSite, src.Fence.SourceServerUUID, plannedFenceGtidString(src))
}

// plannedFencedSource is the source a planned run left fenced and named for
// the re-assert (§6.5), with the server its fence was meant for: the
// @@server_uuid the durable fence intent names. The zero value binds
// nothing.
//
// The run's fence is its own only on that server (§4.3: this process's
// memo for that server, or the durable intent naming its @@server_uuid).
// A run can end without reading the source (SourceLost, a status stall),
// and its fence may be proven only by an intent an earlier operator process
// wrote, so the binding is checked when the re-assert acts, not when the
// source is named: a source that answers under another @@server_uuid then
// is a replaced server, whose read-only state is not the run's fence, and it
// is treated as a site the operator only observed (lifted from mysqld's boot
// read_only, never from super_read_only).
type plannedFencedSource struct {
	runID, site, serverUUID string
	// fenceGtid is the executed set the source reported under the run's
	// fence ("" when the run never read it). The re-assert reports what
	// the source no longer holds of it (reassertNamedSourceCheck).
	fenceGtid string
	// warnedUUID is the @@server_uuid the mismatch was last logged for.
	warnedUUID string
	// lossReported is the lost set the re-assert last reported for this
	// binding, so a re-assert that is retried reports it once.
	lossReported string
}

// plannedFenceGtidString is the run's fenced GTID set as recorded ("" when
// the run has none).
func plannedFenceGtidString(pf *v1alpha1.PlannedFailoverStatus) string {
	if pf == nil || pf.Fence == nil || pf.Fence.GtidSet == nil {
		return ""
	}
	return *pf.Fence.GtidSet
}

// reassertNamedSourceCheck is the re-assert's second look at a source a
// planned run named, made on the snapshot that read the target's
// server_uuid together with the set the re-assert decides on (target,
// under serverUUID with the executed set have; identityKnown false for a
// checker that reads no identity).
//
// It returns false when the snapshot's server is not the one the run's
// fence names. plannedNamedSourceIsFenceServer has checked that already,
// on an earlier read: a pod replaced between the two reads would
// otherwise be lifted from a fence that is not the run's, on the first
// read's word. The next poll decides again.
//
// When the server is the run's and every peer has been compared (the
// caller is about to lift the fence), it reports what the source no
// longer holds of the set it was fenced with. No site holds those
// transactions any more: the re-assert has just found that the source
// contains every peer. They were acknowledged writes, and they are lost.
// The fence is lifted all the same: no site is a better primary, and
// nothing remains that the source could diverge from. The loss is logged
// at ERROR and raised as PlannedSourceTransactionsLost
// (ReassertDataLossCallback); with PITR the promotion's own check reports
// whether the archive still holds them (PITRArchiveReissueRiskDetected).
func (tm *TopologyManager) reassertNamedSourceCheck(target, serverUUID string, identityKnown bool, have mysql.GTIDSet, report bool) bool {
	tm.mu.Lock()
	bound := tm.plannedFencedSource
	if bound.site != target || bound.serverUUID == "" {
		tm.mu.Unlock()
		return true
	}
	if !identityKnown || !strings.EqualFold(serverUUID, bound.serverUUID) {
		tm.mu.Unlock()
		tm.logger.Warn("primary re-assert deferred: the fenced source answered as another server than the one its fence was checked on",
			"runId", bound.runID, "site", target, "fenceServerUUID", bound.serverUUID, "serverUUID", serverUUID)
		return false
	}
	var lost mysql.GTIDSet
	if report && bound.fenceGtid != "" {
		if fenced, err := mysql.ParseGTIDSet(bound.fenceGtid); err == nil {
			lost = fenced.Subtract(have)
		}
	}
	announce := !lost.IsEmpty() && tm.plannedFencedSource.lossReported != lost.String()
	if announce {
		tm.plannedFencedSource.lossReported = lost.String()
	}
	tm.mu.Unlock()
	if announce {
		tm.logger.Error("primary re-assert: the fenced source no longer holds transactions it held under the planned failover's fence, and no site holds them; they are lost",
			"runId", bound.runID, "site", target, "lostTransactions", lost.TransactionCount(), "lostGtid", truncateGtidSet(lost.String()))
		if tm.ReassertDataLossCallback != nil {
			tm.ReassertDataLossCallback(target, bound.runID, lost.String(), lost.TransactionCount())
		}
	}
	return true
}

// nameActiveSiteLocked records site as the group's last active site, named
// (a promotion, a failover record, a restore seed) or only observed
// writable. It ends a planned run's binding (plannedFencedSource): that
// binding ties the name to the server the run fenced, and a name given by
// anything else is not about that fence. Left standing, it would make a
// later re-assert of the same site, named by a promotion, treat its server
// as "not the run's" and refuse to lift a fence the promotion's authority
// covers. nameFencedSourceLocked sets the binding again when a run names
// its source. Must hold tm.mu.
func (tm *TopologyManager) nameActiveSiteLocked(site string, named bool) {
	tm.lastActiveSite, tm.lastActiveSiteNamed = site, named
	tm.plannedFencedSource = plannedFencedSource{}
}

// nameFencedSourceLocked names source for the re-assert on behalf of run
// runID and binds the name to the server serverUUID. A run whose fence
// intent names no server binds nothing and names nothing: there is no
// server its fence could be attributed to. Must hold tm.mu.
func (tm *TopologyManager) nameFencedSourceLocked(runID, source, serverUUID, fenceGtid string) bool {
	if serverUUID == "" {
		return false
	}
	tm.lastActiveSite, tm.lastActiveSiteNamed = source, true
	tm.plannedFencedSource = plannedFencedSource{runID: runID, site: source, serverUUID: serverUUID, fenceGtid: fenceGtid}
	tm.logger.Info("planned failover: named fenced source for re-assert", "runId", runID, "site", source)
	return true
}

// plannedNamedSourceIsFenceServer reports whether site, the re-assert's
// lastActiveSite target, may be lifted from a fence as a named site: it was
// not named by a planned run (no binding for it), or it answers under the
// @@server_uuid that run's fence intent names. A failed identity read is
// no proof.
func (tm *TopologyManager) plannedNamedSourceIsFenceServer(ctx context.Context, site *siteTracker) bool {
	tm.mu.RLock()
	bound := tm.plannedFencedSource
	tm.mu.RUnlock()
	if bound.site != site.name || bound.serverUUID == "" {
		return true
	}
	var snap mysql.GTIDSnapshot
	err := withMySQLSafetyTimeout(ctx, func(probeCtx context.Context) error {
		var err error
		snap, err = pooledIdentity{site.mysql}.ReadGTIDSnapshot(probeCtx)
		return err
	})
	if err == nil && strings.EqualFold(snap.ServerUUID, bound.serverUUID) {
		return true
	}
	seen := snap.ServerUUID
	if err != nil {
		seen = "unreadable"
	}
	tm.mu.Lock()
	logged := tm.plannedFencedSource.site == bound.site && tm.plannedFencedSource.runID == bound.runID &&
		tm.plannedFencedSource.warnedUUID == seen
	if !logged && tm.plannedFencedSource.runID == bound.runID {
		tm.plannedFencedSource.warnedUUID = seen
	}
	tm.mu.Unlock()
	if !logged {
		tm.logger.Warn("planned failover: the fenced source is not the server the run's fence names; its fence is not lifted",
			"runId", bound.runID, "site", site.name, "fenceServerUUID", bound.serverUUID, "serverUUID", seen)
	}
	return false
}

// carryNamedActiveSite brings old's named lastActiveSite into tm when
// tm's own restore named nothing and the site is still one of tm's: the
// replacement is the same process, and old's naming (a source a run left
// fenced, a promotion, a seed) is the newest this process holds. It takes
// old's lock, then tm's, never both.
func (tm *TopologyManager) carryNamedActiveSite(old *TopologyManager) bool {
	if tm == nil || old == nil || tm == old {
		return false
	}
	old.mu.RLock()
	site, named, bound := old.lastActiveSite, old.lastActiveSiteNamed, old.plannedFencedSource
	old.mu.RUnlock()
	if !named || site == "" {
		return false
	}
	tm.mu.Lock()
	defer tm.mu.Unlock()
	if tm.lastActiveSiteNamed || tm.getSite(site) == nil {
		return false
	}
	tm.lastActiveSite, tm.lastActiveSiteNamed = site, true
	if bound.site == site {
		// A source a planned run named stays bound to the run's server.
		bound.warnedUUID = ""
		tm.plannedFencedSource = bound
	}
	tm.logger.Info("carried the replaced topology manager's named active site", "site", site)
	return true
}

// plannedFailoverView is the start's uncached read of the group, for the
// planned-failover hydration. Without an uncached reader (tests) it is fg.
// A read error fails the start unless the group's slot already holds a
// claim or a predecessor of this process is being replaced: hydration
// only ever fills an empty slot, and this process's runs hold theirs.
func (r *TopologyManagerRunner) plannedFailoverView(ctx context.Context, fg *v1alpha1.MysqlFailoverGroup, prev *managedTopology) (*v1alpha1.MysqlFailoverGroup, error) {
	if r.apiReader == nil {
		return fg, nil
	}
	nn := FailoverGroupNamespacedName(fg)
	var fresh v1alpha1.MysqlFailoverGroup
	if err := r.apiReader.Get(ctx, nn, &fresh); err != nil {
		if prev != nil || r.Slot(nn).Held() {
			r.logger.Warn("could not read the group uncached; the new topology manager shares its predecessor's planned-failover claim",
				"fg", nn.String(), "error", err)
			return nil, nil
		}
		return nil, fmt.Errorf("read the group uncached to hydrate its planned-failover claim: %w", err)
	}
	return &fresh, nil
}
