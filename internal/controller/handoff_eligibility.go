package controller

import (
	"errors"
	"fmt"

	v1alpha1 "github.com/shipstream/bloodraven/api/v1alpha1"
)

// handoffBlock says why a standby must not receive an ordered-update
// handoff. The update's snapshot, its active-site precheck and the live
// re-check at handoff selection all read it, so they cannot disagree.
type handoffBlock struct {
	// rotation: mid-keyring-rotation, durably stamped or reserved.
	rotation bool
	// reclonePending: an in-place restore has not recloned the site yet.
	reclonePending bool
	// divergence is the recorded RecoveryBlocked report, if any.
	divergence siteRecovery
	// ownLost: the site carries a report of transactions it issued itself
	// and lacks. ownLostRemedy says what ends the report: its reclone when
	// the primary holds them, the repair of the site that does when the
	// primary does not (ownLossPromotionRemedyLocked).
	ownLost       bool
	ownLostRemedy string
}

// handoffBlockLocked reads site's handoff eligibility. Must hold tm.mu.
func (tm *TopologyManager) handoffBlockLocked(site string) handoffBlock {
	b := handoffBlock{
		rotation:       tm.keyringRotationBlockedLocked(site),
		reclonePending: tm.restoreReclonePendingLocked(site),
		ownLost:        tm.ownLossLocked(site),
		ownLostRemedy:  tm.ownLossPromotionRemedyLocked(site),
	}
	if rec := tm.recovery[site]; rec != nil && rec.state == v1alpha1.RecoveryStateBlocked {
		b.divergence = *rec
	}
	return b
}

func (b handoffBlock) err(site string) error {
	switch {
	case b.rotation:
		return fmt.Errorf("%s mid-keyring-rotation (UnsealReason=Rotation); finish the rotation before this site can be promoted", site)
	case b.reclonePending:
		return fmt.Errorf("%s pending post-restore reclone; wait for status.restoreReclones to drain", site)
	case b.ownLost:
		return fmt.Errorf("%s lacks transactions it issued itself (status.sites[].lostGtid) and would issue their GTIDs again; %s", site, b.ownLostRemedy)
	case b.divergence.state == v1alpha1.RecoveryStateBlocked:
		return errors.New(divergentStandbyMessage(UpdateTarget{Name: site, DivergentTransactions: b.divergence.divergentCount}))
	}
	return nil
}

// liveHandoffRefusal returns the UpdateTarget.HandoffBlocked callback for
// site: its current eligibility, read when the handoff target is chosen
// rather than when the update was planned.
func (tm *TopologyManager) liveHandoffRefusal(site string) func() error {
	return func() error {
		tm.mu.RLock()
		b := tm.handoffBlockLocked(site)
		tm.mu.RUnlock()
		return b.err(site)
	}
}
