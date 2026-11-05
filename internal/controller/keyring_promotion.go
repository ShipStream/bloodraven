package controller

import (
	"slices"
	"sort"

	"k8s.io/apimachinery/pkg/types"

	v1alpha1 "github.com/shipstream/bloodraven/api/v1alpha1"
	"github.com/shipstream/bloodraven/internal/metrics"
	"github.com/shipstream/bloodraven/internal/state"
)

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
// promoted because they are mid-keyring-rotation, as read from a possibly
// lagging informer cache (runner sync, manager start). A change arms a
// dirty bit so the next Poll re-runs applyCrossSiteAction even when no
// MySQL state transition occurred — otherwise a 2-site refuse never heals
// after the replica reaches Sealed. It never drops a reservation: a cache
// that has not caught up with a fresh stamp must not unblock the site.
func (tm *TopologyManager) SetKeyringRotationBlocked(sites []string) {
	if tm == nil {
		return
	}
	cp := append([]string(nil), sites...)
	sort.Strings(cp)
	tm.mu.Lock()
	defer tm.mu.Unlock()
	tm.setKeyringRotationBlockedLocked(cp)
}

// PublishKeyringRotationBlocked replaces the durable set from an
// authoritative view: the encryption reconciler's own status write, or an
// uncached read after an ambiguous one. Reservations the view does not
// carry are over (their rotation finished or was never persisted) and are
// dropped; the rest stay until such a view lacks them.
func (tm *TopologyManager) PublishKeyringRotationBlocked(sites []string) {
	if tm == nil {
		return
	}
	cp := append([]string(nil), sites...)
	sort.Strings(cp)
	tm.mu.Lock()
	defer tm.mu.Unlock()
	kept := tm.keyringRotationReserved[:0]
	for _, name := range tm.keyringRotationReserved {
		if slices.Contains(cp, name) {
			kept = append(kept, name)
		}
	}
	if len(kept) != len(tm.keyringRotationReserved) {
		tm.keyringRotationBlockedDirty = true
	}
	tm.keyringRotationReserved = kept
	tm.setKeyringRotationBlockedLocked(cp)
}

func (tm *TopologyManager) setKeyringRotationBlockedLocked(sorted []string) {
	if stringSlicesEqual(tm.keyringRotationBlocked, sorted) {
		return
	}
	tm.keyringRotationBlocked = sorted
	tm.keyringRotationBlockedDirty = true
}

// ReserveKeyringRotation admits a keyring rotation on site before the
// encryption reconciler persists UnsealReason=Rotation. Decided under mu,
// it refuses while an ordered update is pending or running (the update's
// handoff re-checks eligibility live, so a rotation and a handoff onto the
// same standby can never both be admitted), while a promotion is claimed,
// in flight or not yet observed (the promotion claims decide the other side
// under the same lock), and unless this manager currently observes a
// unique writable primary other than site with site read-only, and while a
// planned failover is in flight:
// status.activeSite, which rotationAllowed reads, lags a promotion and an
// operator restart. Once granted, the site is promotion-blocked at once,
// exactly like a durable stamp, until an authoritative view lacks it or it
// is released.
func (tm *TopologyManager) ReserveKeyringRotation(site string) (bool, string) {
	tm.mu.Lock()
	defer tm.mu.Unlock()
	active := tm.activeSiteLocked()
	switch {
	case tm.retired || tm.predecessorUpdateInFlight:
		return false, rotationRefusedManagerRestarting
	case tm.deploymentUpdatePending || tm.deploymentUpdateRunning:
		return false, "an ordered update is in progress"
	case tm.plannedClaimHeldLocked():
		// A planned failover holds its claim (or a hold stands):
		// admitting a rotation of the target now could roll back a
		// failover that already fenced the source.
		return false, rotationRefusedPlannedFailover
	case tm.deploymentChanging > 0 || tm.promotedSite != "" || tm.deploymentAwaitingObservation:
		return false, rotationRefusedPromotionInFlight
	case active == site:
		return false, rotationRefusedActivePrimary
	case active == "" || !tm.siteObservedReadOnlyLocked(site):
		// A manager that has not yet observed one writable primary and
		// the target read-only (just started, or authority unsettled)
		// cannot rule out that the target is the real primary: status
		// may still name the old one.
		return false, rotationRefusedAuthorityUnsettled
	}
	tm.addKeyringRotationReservationsLocked([]string{site})
	return true, ""
}

func (tm *TopologyManager) siteObservedReadOnlyLocked(name string) bool {
	for i := range tm.sites {
		if tm.sites[i].name == name {
			return tm.sites[i].state == state.StateReadOnly
		}
	}
	return false
}

// restoreKeyringRotationReservations carries the runner's pending
// reservations onto a manager before its Run starts.
func (tm *TopologyManager) restoreKeyringRotationReservations(sites []string) {
	tm.mu.Lock()
	defer tm.mu.Unlock()
	tm.addKeyringRotationReservationsLocked(sites)
}

func (tm *TopologyManager) addKeyringRotationReservationsLocked(sites []string) {
	for _, site := range sites {
		if !slices.Contains(tm.keyringRotationReserved, site) {
			tm.keyringRotationReserved = append(tm.keyringRotationReserved, site)
			tm.keyringRotationBlockedDirty = true
		}
	}
	sort.Strings(tm.keyringRotationReserved)
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

// keyringRotationBlockedLocked reports whether site is mid-keyring-rotation:
// durably stamped, or reserved by a rotation admitted but not yet persisted.
func (tm *TopologyManager) keyringRotationBlockedLocked(site string) bool {
	return slices.Contains(tm.keyringRotationBlocked, site) || slices.Contains(tm.keyringRotationReserved, site)
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

// Rotation reservations are group-scoped on the runner as well as held by
// the group's current manager: a manager started or replaced between a
// reservation and the status write that persists it receives the pending
// reservations before its Run begins (registerManager), so no manager can
// admit an ordered update onto a site whose rotation was already admitted.
// reservationMu orders reservation changes against manager registration;
// it is taken before r.mu and before any manager's mu.
//
// Lifecycle of one site's reservation. Invariant: whenever a rotation stamp
// may be durable, the running manager treats the site as blocked (durable
// set ∪ reservations), so promotion cannot proceed (no fail-open). There
// is a single "held" state: whether a hold is new, inherited from an
// earlier pass or manager, or covers a write of unknown outcome changes
// nothing, because only an authoritative view of the status ends it.
//
//	state  event                                        result
//	none   reserve admitted (stable authority; no       held; blocked at once
//	       update, planned failover or promotion; no
//	       busy retiring manager)
//	none   reserve refused                              none; request kept and retried,
//	                                                    except for the active primary
//	held   status write ok, view carries the stamp      held (also durable)
//	held   status write ok, view lacks it               settled (a no-op write does not
//	                                                    move the resourceVersion, so an
//	                                                    earlier lost write may still land)
//	held   pass ends without a successful write         settled
//	held   settle: uncached read carries the stamp      held (also durable)
//	held   settle: read lacks it, conditional           none (never stamped, or done;
//	       metadata fence ok                            no lost write can land now)
//	held   settle: read or fence fails                  held (fail closed)
//	held   stale cache push (runner sync, start)        held; only the durable set moves
//	held   manager replaced or retired                  copied to the replacement before
//	                                                    its Run; retired managers start
//	                                                    no update
//	held   promotion claim (beginAutoPromotionOf)       claim refused; dirty bit re-armed
//	held   planned-failover claim (claimPlanned-        Wait{KeyringRotation} while
//	       Failover)                                    reserved
//	any    claim taken / promotion unobserved           reserve refused until observed
//	held   encryption disabled / group deleted          none
//
// The only way a hold outlives its need (fail-stuck) is every settlement
// failing its uncached read or its fence, i.e. the API server being
// unreachable or the object changing under every fence; the first
// settlement that gets through ends the hold.

// ReserveKeyringRotation admits a rotation for nn's site. The group's
// running manager decides (see TopologyManager.ReserveKeyringRotation).
// It is refused, for the caller to retry, while no manager runs (one about
// to start could not be asked) and while a retiring manager still has an
// ordered update in flight: that update's handoff re-checked eligibility on
// the old manager, which a reservation recorded now would never reach.
func (r *TopologyManagerRunner) ReserveKeyringRotation(nn types.NamespacedName, site string) (bool, string) {
	if r == nil {
		return true, ""
	}
	r.reservationMu.Lock()
	defer r.reservationMu.Unlock()
	r.mu.RLock()
	mt := r.managers[nn]
	retiring := append([]*managedTopology(nil), r.retiring[nn]...)
	r.mu.RUnlock()
	for _, old := range retiring {
		if old.tm.mutationInFlight() {
			return false, rotationRefusedManagerRestarting
		}
	}
	if mt == nil {
		return false, rotationRefusedNoManager
	}
	if ok, why := mt.tm.ReserveKeyringRotation(site); !ok {
		return false, why
	}
	if pending := r.pendingKeyringRotationReservations(nn); !slices.Contains(pending, site) {
		r.setPendingKeyringRotationReservations(nn, append(pending, site))
	}
	return true, ""
}

// rotationRefusedActivePrimary is the one live refusal that does not clear
// on its own: the request is consumed, as rotationAllowed's status-based
// refusal of the active primary is.
const rotationRefusedActivePrimary = "site is the active primary; rotate replicas first, then run a planned failover and rotate this site once it is a replica"

// Live rotation refusals that clear on their own; the encryption reconciler
// keeps the request and requeues on them.
const (
	rotationRefusedPlannedFailover    = "a planned failover is in progress; retrying"
	rotationRefusedPromotionInFlight  = "a promotion is in flight; retrying once the new primary is observed"
	rotationRefusedAuthorityUnsettled = "the operator has not yet observed a unique writable primary with this site read-only; retrying"
	rotationRefusedManagerRestarting  = "the topology manager is restarting with an ordered update or promotion still in flight; retrying"
	rotationRefusedNoManager          = "no topology manager is running for the group yet; retrying"
)

// PendingKeyringRotationReservations returns nn's reservations.
func (r *TopologyManagerRunner) PendingKeyringRotationReservations(nn types.NamespacedName) []string {
	if r == nil {
		return nil
	}
	r.reservationMu.Lock()
	defer r.reservationMu.Unlock()
	return r.pendingKeyringRotationReservations(nn)
}

// PublishKeyringRotationBlocked pushes an authoritative rotation-blocked
// set (see TopologyManager.PublishKeyringRotationBlocked) and drops nn's
// reservations it does not carry.
func (r *TopologyManagerRunner) PublishKeyringRotationBlocked(nn types.NamespacedName, sites []string) {
	if r == nil {
		return
	}
	r.reservationMu.Lock()
	defer r.reservationMu.Unlock()
	r.setPendingKeyringRotationReservations(nn, slices.DeleteFunc(r.pendingKeyringRotationReservations(nn), func(s string) bool {
		return !slices.Contains(sites, s)
	}))
	if mt := r.registered(nn); mt != nil {
		mt.tm.PublishKeyringRotationBlocked(sites)
	}
}

// pendingKeyringRotationReservations returns nn's reservations. Caller
// holds reservationMu.
func (r *TopologyManagerRunner) pendingKeyringRotationReservations(nn types.NamespacedName) []string {
	return append([]string(nil), r.rotationReservations[nn]...)
}

// setPendingKeyringRotationReservations stores nn's reservations. Caller
// holds reservationMu.
func (r *TopologyManagerRunner) setPendingKeyringRotationReservations(nn types.NamespacedName, sites []string) {
	if len(sites) == 0 {
		delete(r.rotationReservations, nn)
		return
	}
	if r.rotationReservations == nil {
		r.rotationReservations = make(map[types.NamespacedName][]string)
	}
	r.rotationReservations[nn] = sites
}

func (r *TopologyManagerRunner) registered(nn types.NamespacedName) *managedTopology {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.managers[nn]
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
