package controller

import (
	"fmt"
	"sort"

	"k8s.io/apimachinery/pkg/types"

	v1alpha1 "github.com/shipstream/bloodraven/api/v1alpha1"
	"github.com/shipstream/bloodraven/internal/metrics"
	"github.com/shipstream/bloodraven/internal/state"
)

// errKeyringRotationBlocked is returned by PlannedPromote when the
// target is mid-keyring-rotation. plannedFailoverPromoting maps it to
// a rollback (unfence the source) with reason KeyringRotation rather
// than the generic ExecuteFailed / manual-recovery path.
var errKeyringRotationBlocked = fmt.Errorf("planned promote: target is mid-keyring-rotation")

// rotationBlockedSites returns the names of sites whose UnsealReason is
// Rotation. Order is spec-stable (sorted) so slice compares are not
// order-sensitive across status rewrites.
func rotationBlockedSites(fg *v1alpha1.MysqlFailoverGroup) []string {
	if fg == nil || !fg.Spec.EncryptionEnabled() || fg.Status.EncryptionAtRest == nil {
		return nil
	}
	var out []string
	for _, s := range fg.Status.EncryptionAtRest.Sites {
		if s.UnsealReason == v1alpha1.UnsealReasonRotation {
			out = append(out, s.Name)
		}
	}
	sort.Strings(out)
	return out
}

// SetKeyringRotationBlocked replaces the set of sites that must not be
// promoted because they are mid-keyring-rotation. A change arms a dirty
// bit so the next Poll re-runs applyCrossSiteAction even when no MySQL
// state transition occurred — otherwise a 2-site refuse never heals
// after the replica reaches Sealed.
func (tm *TopologyManager) SetKeyringRotationBlocked(sites []string) {
	if tm == nil {
		return
	}
	cp := append([]string(nil), sites...)
	sort.Strings(cp)
	tm.mu.Lock()
	defer tm.mu.Unlock()
	if stringSlicesEqual(tm.keyringRotationBlocked, cp) {
		return
	}
	tm.keyringRotationBlocked = cp
	tm.keyringRotationBlockedDirty = true
}

func (tm *TopologyManager) peekKeyringRotationBlockedDirty() bool {
	tm.mu.RLock()
	defer tm.mu.RUnlock()
	return tm.keyringRotationBlockedDirty
}

func (tm *TopologyManager) clearKeyringRotationBlockedDirty() {
	tm.mu.Lock()
	tm.keyringRotationBlockedDirty = false
	tm.mu.Unlock()
}

func (tm *TopologyManager) keyringRotationBlockedLocked(site string) bool {
	for _, name := range tm.keyringRotationBlocked {
		if name == site {
			return true
		}
	}
	return false
}

func (tm *TopologyManager) recordKeyringPromotionDecision(skipped, refused []string) {
	tm.mu.Lock()
	tm.keyringPromotionSkipped = append([]string(nil), skipped...)
	tm.keyringPromotionRefused = append([]string(nil), refused...)
	tm.mu.Unlock()
}

// rotationBlockedCandidates returns rotation-blocked sites that are
// currently read-only primary-candidates — the ones the matrix would
// have considered for promotion. Caller must not hold tm.mu.
func (tm *TopologyManager) rotationBlockedCandidates() []string {
	tm.mu.RLock()
	defer tm.mu.RUnlock()
	var out []string
	for i := range tm.sites {
		site := &tm.sites[i]
		if site.state != state.StateReadOnly || !site.isPromotable() {
			continue
		}
		if tm.keyringRotationBlockedLocked(site.name) {
			out = append(out, site.name)
		}
	}
	return out
}

func (tm *TopologyManager) incrementKeyringPromotionBlocked(sites []string, outcome string) {
	ns, group := tm.cfg.Namespace, tm.cfg.Name
	for _, site := range sites {
		metrics.KeyringPromotionsBlockedTotal.WithLabelValues(ns, group, site, outcome).Inc()
	}
}

// SetKeyringRotationBlocked pushes the current rotation-blocked set onto
// the managed TopologyManager. Returns false when no manager is running;
// the next sync / startManager call heals that.
func (r *TopologyManagerRunner) SetKeyringRotationBlocked(nn types.NamespacedName, sites []string) bool {
	if r == nil {
		return false
	}
	r.mu.RLock()
	mt, ok := r.managers[nn]
	r.mu.RUnlock()
	if !ok {
		return false
	}
	mt.tm.SetKeyringRotationBlocked(sites)
	return true
}

func stringSlicesEqual(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}
