package controller

import (
	"context"
	"slices"
	"sort"

	"k8s.io/apimachinery/pkg/types"

	v1alpha1 "github.com/shipstream/bloodraven/api/v1alpha1"
	"github.com/shipstream/bloodraven/internal/state"
)

// RecloneOutcome describes a finished reclone attempt for
// RecloneCompleteCallback.
type RecloneOutcome struct {
	// Owner is the restore reclone queue (its confirm token) that
	// requested the clone, or "" for an administrator's request.
	Owner string
	// Err is nil when the clone and the replication setup succeeded.
	Err error
}

// SetRestoreReclonePending replaces the set of sites an in-place restore
// has not recloned yet (status.restoreReclones.pending). Those sites hold
// pre-restore data, so automatic promotion skips them. A change re-runs
// the cross-site decision on the next poll, like a keyring-rotation
// block change.
func (tm *TopologyManager) SetRestoreReclonePending(sites []string) {
	if tm == nil {
		return
	}
	cp := append([]string(nil), sites...)
	sort.Strings(cp)
	tm.mu.Lock()
	defer tm.mu.Unlock()
	if stringSlicesEqual(tm.restoreReclonePending, cp) {
		return
	}
	tm.restoreReclonePending = cp
	tm.keyringRotationBlockedDirty = true
	if len(cp) == 0 {
		tm.restoreReclonePromotionRefused = nil
	}
}

// SetRestoreRecloneBlocked replaces the set of pending sites whose queue
// is Armed or Held (restoreRecloneBlockedSites): no reclone of them may
// start, whoever requested it.
func (tm *TopologyManager) SetRestoreRecloneBlocked(sites []string) {
	if tm == nil {
		return
	}
	cp := append([]string(nil), sites...)
	sort.Strings(cp)
	tm.mu.Lock()
	tm.restoreRecloneBlocked = cp
	tm.mu.Unlock()
}

func (tm *TopologyManager) restoreRecloneBlockedSite(site string) bool {
	tm.mu.RLock()
	defer tm.mu.RUnlock()
	return slices.Contains(tm.restoreRecloneBlocked, site)
}

// restoreReclonePendingLocked reports whether site is waiting for its
// post-restore reclone. Caller holds tm.mu.
func (tm *TopologyManager) restoreReclonePendingLocked(site string) bool {
	return slices.Contains(tm.restoreReclonePending, site)
}

// restoreReclonePendingCandidates returns the read-only primary-candidates
// that automatic promotion skipped because they await their post-restore
// reclone. Caller must not hold tm.mu.
func (tm *TopologyManager) restoreReclonePendingCandidates() []string {
	tm.mu.RLock()
	defer tm.mu.RUnlock()
	var out []string
	for i := range tm.sites {
		site := &tm.sites[i]
		if site.state == state.StateReadOnly && site.isPromotable() && tm.restoreReclonePendingLocked(site.name) {
			out = append(out, site.name)
		}
	}
	return out
}

// recordRestoreReclonePromotionRefused records the candidates the last
// promotion decision refused for a pending reclone (nil when it refused
// none), for the runner's Warning event.
func (tm *TopologyManager) recordRestoreReclonePromotionRefused(sites []string) {
	tm.mu.Lock()
	tm.restoreReclonePromotionRefused = append([]string(nil), sites...)
	tm.mu.Unlock()
}

// SetRestoreReclonePending pushes the pending set onto the managed
// TopologyManager. Returns false when no manager is running; the next
// sync or startManager call applies it.
func (r *TopologyManagerRunner) SetRestoreReclonePending(nn types.NamespacedName, sites []string) bool {
	if r == nil {
		return false
	}
	r.mu.RLock()
	mt, ok := r.managers[nn]
	r.mu.RUnlock()
	if !ok {
		return false
	}
	mt.tm.SetRestoreReclonePending(sites)
	return true
}

// SetRestoreRecloneBlocked pushes the blocked set onto the managed
// TopologyManager. Returns false when no manager is running; the next
// sync or startManager call applies it.
func (r *TopologyManagerRunner) SetRestoreRecloneBlocked(nn types.NamespacedName, sites []string) bool {
	if r == nil {
		return false
	}
	r.mu.RLock()
	mt, ok := r.managers[nn]
	r.mu.RUnlock()
	if !ok {
		return false
	}
	mt.tm.SetRestoreRecloneBlocked(sites)
	return true
}

// CloneInProgress reports the recipient of a clone the topology manager is
// running now (any source), and whether a manager was there to ask.
func (r *TopologyManagerRunner) CloneInProgress(nn types.NamespacedName) (site string, known bool) {
	if r == nil {
		return "", false
	}
	r.mu.RLock()
	mt, ok := r.managers[nn]
	r.mu.RUnlock()
	if !ok {
		return "", false
	}
	return mt.tm.cloneInProgress(), true
}

func (tm *TopologyManager) cloneInProgress() string {
	tm.mu.RLock()
	defer tm.mu.RUnlock()
	switch tm.bootstrapPhase {
	case BootstrapPhaseCloning, BootstrapPhaseRestarting, BootstrapPhaseSetupRepl:
		return tm.bootstrapRecipient
	}
	return ""
}

// ApplyRecloneRequest hands fg's reclone annotation to its running
// topology manager now, instead of on the next 30s runner sync.
func (r *TopologyManagerRunner) ApplyRecloneRequest(ctx context.Context, fg *v1alpha1.MysqlFailoverGroup) {
	if r == nil {
		return
	}
	nn := FailoverGroupNamespacedName(fg)
	r.mu.RLock()
	mt, ok := r.managers[nn]
	r.mu.RUnlock()
	if !ok {
		return
	}
	r.handleRecloneAnnotation(ctx, fg, nn, mt.tm)
}

// RecloneInProgress reports whether site's reclone is running, and whether
// a topology manager was there to ask.
func (r *TopologyManagerRunner) RecloneInProgress(nn types.NamespacedName, site string) (inProgress, known bool) {
	if r == nil {
		return false, false
	}
	r.mu.RLock()
	mt, ok := r.managers[nn]
	r.mu.RUnlock()
	if !ok {
		return false, false
	}
	return mt.tm.recloneInProgress(site), true
}

// CancelPendingReclone drops a reclone request for site that has not
// started yet, so a stalled request does not start later.
func (r *TopologyManagerRunner) CancelPendingReclone(nn types.NamespacedName, site string) {
	if r == nil {
		return
	}
	r.mu.RLock()
	mt, ok := r.managers[nn]
	r.mu.RUnlock()
	if ok {
		mt.tm.cancelPendingReclone(site)
	}
}

// recloneInProgress also counts a clone that succeeded but whose
// completion callback is still being retried (recloneCompletedSite): its
// outcome must reach the queue rather than being failed as unstarted.
func (tm *TopologyManager) recloneInProgress(site string) bool {
	tm.mu.RLock()
	defer tm.mu.RUnlock()
	if tm.recloneCompletedSite == site {
		return true
	}
	return tm.bootstrapSource == "reclone" && tm.bootstrapRecipient == site &&
		(tm.bootstrapPhase == BootstrapPhaseCloning || tm.bootstrapPhase == BootstrapPhaseRestarting || tm.bootstrapPhase == BootstrapPhaseSetupRepl)
}

func (tm *TopologyManager) cancelPendingReclone(site string) {
	tm.mu.Lock()
	defer tm.mu.Unlock()
	running := tm.bootstrapRecipient == site &&
		(tm.bootstrapPhase == BootstrapPhaseCloning || tm.bootstrapPhase == BootstrapPhaseRestarting || tm.bootstrapPhase == BootstrapPhaseSetupRepl)
	if tm.reclonePendingSite == site && !running {
		tm.reclonePendingSite, tm.reclonePendingOwner = "", ""
	}
}
