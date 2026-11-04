package controller

import (
	"errors"
	"fmt"
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
}

// handoffBlockLocked reads site's handoff eligibility. Must hold tm.mu.
func (tm *TopologyManager) handoffBlockLocked(site string) handoffBlock {
	b := handoffBlock{
		rotation:       tm.keyringRotationBlockedLocked(site),
		reclonePending: tm.restoreReclonePendingLocked(site),
	}
	if rec := tm.recovery[site]; rec != nil && rec.state == recoveryStateBlocked {
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
	case b.divergence.state == recoveryStateBlocked:
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
