package controller

import (
	"context"
	"fmt"
	"time"

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
//	PProm  PlannedPromote (planned failover holds) claimPlannedPromotion
//	PF     planned failover before its fence       ClaimPlannedFailover
//	Upd    ordered update                          claimOrderedUpdate
//	Rot    keyring rotation                        ReserveKeyringRotation
//	Fol    old-primary recovery, recovery drain,   beginFollowerMutation
//	       source convergence
//	Boot   auto-clone, reclone, fresh deploy       claimBootstrap
//
// Held claim (rows) against claimant: R refused, W refused for now and
// retried by the claimant (PF with a deadline, PProm until its context
// ends, the rest on a later poll), A allowed.
//
//	held \ claimant              Auto PProm PF  Upd Rot Fol Boot
//	Auto promotion               R    R     W   R   R   R   R
//	PlannedPromote               R    R     W   R   R   R   R
//	promotion not yet observed   R    R     W   R   R   R   A1
//	planned failover             R    A     A   R   R   R   R
//	ordered update claimed       R    R     R   R   R   R   R
//	ordered update pending       A2   A2    A2  A2  R   A2  A2
//	rotation of the target       R    R     R   A3  A   A4  A4
//	follower mutation            A5   W     A6  W   A4  A   A5
//	bootstrap (promotable site)  R    R     R   R   A4  R   R
//	in-place restore             R    R     R   R   A4  R   R
//	predecessor's ordered update R    R     R   R   R   R   R
//	predecessor still promoting  R    W     W   R   R   R   R
//	predecessor in a follower    R    W     A6  R   A4  R   R
//	mutation
//	this manager retired         R    R     W7  R   R   R   R
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
//	A6 The fence touches only the source; PlannedPromote then waits at the
//	   follower-mutation gate.
//	W7 A retired manager is unregistered; the runner sees no manager (Rot
//	   is refused for the same reason).
//
// A bootstrap into a read-only reader does not hold off Auto (see
// bootstrapBlocksCrossSite); its row is otherwise the promotable one's.
//
// "Predecessor" rows are a replaced manager whose Run or update outlived
// its stop. Its ordered update sets predecessorUpdateInFlight on the
// replacement; anything else it is still doing (a promotion, a follower
// mutation) is seen through predecessorMutating, and by the runner-level
// claims (PF, Rot) through the retiring list. A retired manager claims
// nothing new, so both only ever clear.

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
	if tm.retired || tm.plannedFailoverActive || tm.topologyFrozen || tm.promotionUnobservedLocked() || tm.bootstrapBlocksCrossSiteLocked() {
		return false, false
	}
	return tm.beginDeploymentTopologyChangeLocked(), false
}

// beginPlannedPromotionOf claims the topology-change window for a planned
// failover's promotion of target. The planned failover must hold its own
// claim (plannedFailoverActive), which kept every other actor out since its
// fence; this refuses what that claim does not cover.
func (tm *TopologyManager) beginPlannedPromotionOf(target string) (began, rotationBlocked bool) {
	tm.mu.Lock()
	defer tm.mu.Unlock()
	if tm.keyringRotationBlockedLocked(target) {
		tm.keyringRotationBlockedDirty = true
		return false, true
	}
	if tm.retired || !tm.plannedFailoverActive || tm.topologyFrozen || tm.promotionUnobservedLocked() || !bootstrapIdlePhase(tm.bootstrapPhase) {
		return false, false
	}
	return tm.beginDeploymentTopologyChangeLocked(), false
}

// claimPlannedPromotion takes PlannedPromote's claim. It waits, until ctx
// ends, for a predecessor still changing the sites, then publishes the claim
// at the follower-mutation barrier, so a recovery step admitted before the
// planned failover's flag was set has finished before Execute starts.
func (tm *TopologyManager) claimPlannedPromotion(ctx context.Context, target string) error {
	for tm.predecessorMutating() {
		select {
		case <-ctx.Done():
			return fmt.Errorf("%w: %v", errHandoffNotAdmitted, ctx.Err())
		case <-time.After(20 * time.Millisecond):
		}
	}
	var began, rotationBlocked bool
	if err := tm.admitTopologyHandoff(ctx, func() { began, rotationBlocked = tm.beginPlannedPromotionOf(target) }); err != nil {
		return fmt.Errorf("%w: %v", errHandoffNotAdmitted, err)
	}
	switch {
	case rotationBlocked:
		return fmt.Errorf("%w: site %q", errKeyringRotationBlocked, target)
	case !began && tm.predecessorUpdateUnwinding():
		return errPredecessorUpdateInFlight
	case !began:
		return errPlannedPromotionRefused
	}
	return nil
}

// errPlannedPromotionRefused is returned by PlannedPromote when another
// actor holds the sites. Nothing has been sent to the target.
var errPlannedPromotionRefused = fmt.Errorf("planned promote: another topology change holds the sites")

// orderedUpdateClaimRefusedLocked reports whether an ordered update planned
// at authority epoch against active may not claim now. Must hold tm.mu.
func (tm *TopologyManager) orderedUpdateClaimRefusedLocked(epoch uint64, active string) bool {
	return tm.retired || tm.authorityEpoch != epoch || tm.activeSiteLocked() != active ||
		tm.deploymentChanging > 0 || tm.promotionUnobservedLocked() || tm.topologyFrozen ||
		tm.plannedFailoverActive || !bootstrapIdlePhase(tm.bootstrapPhase) || tm.predecessorUpdateInFlight
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
	case tm.plannedFailoverActive:
		return "a planned failover is in progress"
	case tm.deploymentUpdateRunning || tm.predecessorUpdateInFlight:
		return "an ordered update is in progress"
	case tm.deploymentChanging > 0:
		return "a promotion is in flight"
	}
	return ""
}

// claimBootstrap starts a bootstrap of recipient unless another actor
// holds the sites; it returns why not, or "", and the owner of a pending
// reclone of recipient.
func (tm *TopologyManager) claimBootstrap(recipient, source string) (recloneOwner, why string) {
	if tm.predecessorMutating() {
		return "", "a replaced topology manager is still changing the sites"
	}
	tm.mu.Lock()
	defer tm.mu.Unlock()
	if why := tm.bootstrapClaimRefusedLocked(); why != "" {
		return "", why
	}
	if source == "reclone" && tm.reclonePendingSite == recipient {
		recloneOwner = tm.reclonePendingOwner
	}
	tm.bootstrapPhase = BootstrapPhaseCloning
	tm.bootstrapErr = nil
	tm.bootstrapSource = source
	tm.bootstrapRecipient = recipient
	// A leftover release from a previous attempt must not fire while
	// this clone is running — that would reseal the pod mid-CLONE.
	if tm.cloneHoldReleaseSite == recipient {
		tm.cloneHoldReleaseSite = ""
	}
	return recloneOwner, ""
}
