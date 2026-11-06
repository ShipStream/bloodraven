package controller

import (
	"context"
	"fmt"

	v1alpha1 "github.com/shipstream/bloodraven/api/v1alpha1"
	"github.com/shipstream/bloodraven/internal/state"
)

// Topology authority claims.
//
// Every actor that changes which site is writable, or changes the sites a
// handoff is using, first claims topology authority. Each claim is decided
// under tm.mu against every claim another actor may hold, so of two
// conflicting claims exactly one is granted, in whichever order they race.
// Claims made on the poll goroutine (Auto, Fol, Boot) cannot race each
// other, but they still check, so the matrix does not rest on scheduling.
// TestTopologyClaimMatrix asserts every cell.
//
// Claimants (columns):
//
//	Auto   emergency failover, primary re-assert   beginAutoPromotionOf
//	Upd    ordered update                          claimOrderedUpdate
//	Rot    keyring rotation                        ReserveKeyringRotation
//	Fol    old-primary recovery, recovery drain,   beginFollowerMutation
//	       source convergence
//	Boot   auto-clone, reclone, fresh deploy       claimBootstrap
//	PF     planned failover                        claimPlannedFailover
//
// The planned-failover claim lives in the group's claim slot, shared by
// every manager generation (planned_failover_claim.go). A Hold is what a
// run that ended ManualRecoveryRequired leaves in the slot until a human
// acknowledges it; it holds exactly what a running claim holds. Every
// other claimant's helper refuses while the slot holds either.
//
// Held claim (rows) against claimant: R refused, W refused for now and
// retried by the claimant later (Upd: it waits at the follower mutation
// gate; PF: the request stays Admitting until its admission deadline, with
// gate attempts at most once per second), A allowed.
//
//	held \ claimant              Auto Upd Rot Fol Boot  PF
//	Auto promotion               R    R   R   R   R     W
//	planned-failover claim       R    R   R   R   R     W7
//	(incl. provisional grant)
//	planned-failover hold        R    R   R   R   R     R12
//	(ManualRecoveryRequired)
//	promotion not yet observed   R    R   R   R   A1    W
//	ordered update claimed       R    R   R   R   R     W
//	ordered update pending       A2   A2  R   A2  A2    A2
//	rotation of the target       R    A3  A   A4  A4    W8 / R8
//	follower mutation            A5   W   A4  A   A5    W9
//	bootstrap (promotable site)  R    R   A4  R   R     W
//	bootstrap (reader)           A10  R   A4  R   R     A10
//	in-place restore             R    R   A4  R   R     W
//	predecessor's ordered update R    R   R   R   R     W
//	predecessor still promoting  R    R   R   R   R     W
//	predecessor in a follower    R    R   A4  R   R     W
//	mutation
//	this manager retired         R    R   R6  R   R     W11
//	planned request waiting      A    A   A   A   A     -
//	(Admitting/Deferred, no grant: owns nothing)
//
//	A1 Boot re-confirms the donor's authority before cloning, and the
//	   fresh-deploy bootstrap starts right behind its own seed promotion.
//	A2 A pending update has touched nothing; its own claim (Upd, the cell
//	   in its column) refuses once anything else holds.
//	A3 The update's handoff re-checks the rotation block live
//	   (liveHandoffRefusal) before promoting.
//	A4 Not a topology change. The encryption reconciler decides from status
//	   (a rotating site is not Sealed; a clone needs its recipient unsealed).
//	A5 Both run on the poll goroutine, one after the other.
//	R6 A retired manager is unregistered; the runner sees no manager.
//	W7 Another run holds the claim: Wait{ClaimHeld}. The same run gets an
//	   idempotent Granted.
//	W8 / R8 A rotation reservation of the target waits; a durable
//	   UnsealReason=Rotation stamp rejects (Failed{NotStarted,
//	   KeyringRotation}). Rotation of any other site is allowed.
//	W9 Non-blocking: tryAdmit registers a 15s entry that refuses new
//	   follower mutations and returns at once while one is running.
//	A10 A clone into a non-promotable reader holds off neither Auto nor PF.
//	W11 The runner routes the claim to the current manager; the next pass
//	   reaches the replacement.
//	R12 A new request ends Failed{NotStarted, ManualRecoveryPending}.
//
// A bootstrap into a read-only reader does not hold off Auto (see
// bootstrapBlocksCrossSite); its row is otherwise the promotable one's.
//
// "Predecessor" rows are a replaced manager whose Run or update outlived
// its stop. Its ordered update sets predecessorUpdateInFlight on the
// replacement; anything else it is still doing (a promotion, a follower
// mutation) is seen through predecessorMutating, and by rotation admission
// through the retiring list. A retired manager claims nothing new, so both
// only ever clear.

// promotionUnobservedLocked reports whether a promotion has finished but
// no poll has yet observed its outcome. Must hold tm.mu.
func (tm *TopologyManager) promotionUnobservedLocked() bool {
	return tm.promotedSite != "" || tm.deploymentAwaitingObservation
}

// bootstrapBlocksCrossSiteLocked is bootstrapBlocksCrossSite under tm.mu.
func (tm *TopologyManager) bootstrapBlocksCrossSiteLocked() bool {
	if bootstrapIdlePhase(tm.bootstrapPhase) {
		return false
	}
	for i := range tm.sites {
		if tm.bootstrapRecipient != "" && tm.sites[i].name == tm.bootstrapRecipient {
			return tm.sites[i].role != state.SiteRoleReadOnly
		}
	}
	return true
}

// predecessorMutating reports whether a replaced manager of this group is
// still inside a promotion, an ordered update or a follower mutation. A
// retired manager starts none of these, so once false it stays false and
// may be checked before a claim's locked section.
func (tm *TopologyManager) predecessorMutating() bool {
	tm.mu.RLock()
	busy := tm.predecessorBusy
	tm.mu.RUnlock()
	return busy != nil && busy()
}

// beginAutoPromotionOf claims the topology-change window for an automatic
// promotion (emergency failover, primary re-assert) of target. It refuses
// (rotationBlocked) when target is mid-keyring-rotation, durably or by
// reservation; ReserveKeyringRotation decides under the same lock and
// refuses while a change is claimed, so the two never both proceed, and the
// dirty bit is re-armed so the next poll re-decides without target. It also
// refuses anything another actor holds: a
// planned failover, an in-place restore, a topology-relevant bootstrap, an
// unobserved promotion, a predecessor still changing the sites, or this
// manager being retired. The caller's earlier checks of the same flags are
// only a fast path; this decides.
func (tm *TopologyManager) beginAutoPromotionOf(target string) (began, rotationBlocked bool) {
	if tm.predecessorMutating() {
		return false, false
	}
	tm.mu.Lock()
	defer tm.mu.Unlock()
	if tm.keyringRotationBlockedLocked(target) {
		tm.keyringRotationBlockedDirty = true
		return false, true
	}
	if tm.retired || tm.plannedClaimHeldLocked() || tm.topologyFrozen || tm.promotionUnobservedLocked() || tm.bootstrapBlocksCrossSiteLocked() {
		return false, false
	}
	return tm.beginDeploymentTopologyChangeLocked(), false
}

// orderedUpdateClaimRefusedLocked reports whether an ordered update planned
// at authority epoch against active may not claim now. Must hold tm.mu.
func (tm *TopologyManager) orderedUpdateClaimRefusedLocked(epoch uint64, active string) bool {
	return tm.retired || tm.authorityEpoch != epoch || tm.activeSiteLocked() != active ||
		tm.deploymentChanging > 0 || tm.promotionUnobservedLocked() || tm.topologyFrozen ||
		tm.plannedClaimHeldLocked() || !bootstrapIdlePhase(tm.bootstrapPhase) || tm.predecessorUpdateInFlight
}

// claimOrderedUpdate claims the MySQL mutation window for an ordered update
// planned at authority epoch against active, publishing it at the
// follower-mutation barrier. It returns ctx's error if an admitted
// follower mutation outlasts ctx.
func (tm *TopologyManager) claimOrderedUpdate(ctx context.Context, epoch uint64, active string) (bool, error) {
	if tm.predecessorMutating() {
		return false, nil
	}
	claimed := false
	err := tm.admitTopologyHandoff(ctx, func() {
		tm.mu.Lock()
		defer tm.mu.Unlock()
		if ctx.Err() != nil || tm.orderedUpdateClaimRefusedLocked(epoch, active) {
			return
		}
		tm.deploymentUpdateRunning = true
		tm.deploymentChanging++
		tm.authorityEpoch++
		tm.deploymentTopologyPendingLocked()
		claimed = true
	})
	return claimed, err
}

// bootstrapClaimRefusedLocked returns why a bootstrap may not start now, or
// "". Must hold tm.mu.
func (tm *TopologyManager) bootstrapClaimRefusedLocked() string {
	switch {
	case tm.retired:
		return "the topology manager is stopping"
	case !bootstrapIdlePhase(tm.bootstrapPhase):
		return "a bootstrap is already running"
	case tm.topologyFrozen:
		return "an in-place restore is in progress"
	case tm.plannedClaimHeldLocked():
		return tm.plannedClaimBlockerLocked()
	case tm.deploymentUpdateRunning || tm.predecessorUpdateInFlight:
		return "an ordered update is in progress"
	case tm.deploymentChanging > 0:
		return "a promotion is in flight"
	}
	return ""
}

// plannedClaimBlockerLocked says why the held planned-failover claim
// refuses an action: a run in progress, or a standing automation hold that
// waiting does not clear (it needs the acknowledgement). Must hold tm.mu.
func (tm *TopologyManager) plannedClaimBlockerLocked() string {
	held, ok := tm.plannedSlot.snapshot()
	if ok && held.mode == claimModeHold {
		if held.runID == "" {
			return "automatic failover is held by a True AutomaticFailoverHeld condition that names no run; resolve the split brain, then remove the condition"
		}
		return fmt.Sprintf("automatic failover is held after planned failover %s; resolve the split brain, then annotate %s=%s",
			held.runID, v1alpha1.AcknowledgePlannedFailoverHoldAnnotation, held.runID)
	}
	return "a planned failover is in progress"
}

// bootstrapRefusedRestoreHold is claimBootstrap's reason when an in-place
// restore has frozen the topology or queued the recipient; the caller
// reports it with noteBootstrapRefused.
const bootstrapRefusedRestoreHold = "the recipient is held by an in-place restore"

// claimBootstrap starts a bootstrap of recipient unless another actor
// holds the sites; it returns why not, or "", and the owner of a pending
// reclone of recipient.
func (tm *TopologyManager) claimBootstrap(recipient, source string) (recloneOwner, why string) {
	if tm.predecessorMutating() {
		return "", "a replaced topology manager is still changing the sites"
	}
	tm.mu.Lock()
	defer tm.mu.Unlock()
	// Re-checked under the same lock as the commit, so a clone never
	// starts after an in-place restore has frozen the topology or queued
	// the recipient (see startBootstrapByName's admission reservation).
	if tm.bootstrapAdmissionRefusedLocked(recipient) {
		return "", bootstrapRefusedRestoreHold
	}
	if why := tm.bootstrapClaimRefusedLocked(); why != "" {
		return "", why
	}
	if source == "reclone" && tm.reclonePendingSite == recipient {
		recloneOwner = tm.reclonePendingOwner
	}
	tm.bootstrapAdmitting = ""
	tm.bootstrapPhase = BootstrapPhaseCloning
	tm.bootstrapErr = nil
	tm.bootstrapSource = source
	tm.bootstrapRecipient = recipient
	tm.bootstrapRefusedSite = ""
	// A leftover release from a previous attempt must not fire while
	// this clone is running — that would reseal the pod mid-CLONE.
	if tm.cloneHoldReleaseSite == recipient {
		tm.cloneHoldReleaseSite = ""
	}
	return recloneOwner, ""
}
