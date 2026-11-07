package controller

import (
	"context"
	"errors"
	"strings"
	"testing"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	v1alpha1 "github.com/shipstream/bloodraven/api/v1alpha1"
	"github.com/shipstream/bloodraven/internal/state"
)

func TestRotationBlockedSites(t *testing.T) {
	if got := rotationBlockedSites(nil); got != nil {
		t.Fatalf("nil fg = %v, want nil", got)
	}
	fg := &v1alpha1.MysqlFailoverGroup{
		ObjectMeta: metav1.ObjectMeta{Name: "orders", Namespace: "default"},
		Spec: v1alpha1.MysqlFailoverGroupSpec{
			EncryptionAtRest: &v1alpha1.EncryptionAtRestSpec{Enabled: true},
		},
		Status: v1alpha1.MysqlFailoverGroupStatus{
			EncryptionAtRest: &v1alpha1.EncryptionAtRestStatus{
				Sites: []v1alpha1.SiteEncryptionStatus{
					{Name: "pdx", UnsealReason: v1alpha1.UnsealReasonRotation},
					{Name: "iad", UnsealReason: v1alpha1.UnsealReasonClone},
					{Name: "fra", UnsealReason: v1alpha1.UnsealReasonRotation},
				},
			},
		},
	}
	got := rotationBlockedSites(fg)
	if len(got) != 2 || got[0] != "fra" || got[1] != "pdx" {
		t.Fatalf("got %v, want sorted [fra pdx]", got)
	}
	fg.Spec.EncryptionAtRest.Enabled = false
	if got := rotationBlockedSites(fg); got != nil {
		t.Fatalf("encryption off = %v, want nil", got)
	}
}

func TestReserveKeyringRotation_RefusedWhileOrderedUpdateInFlight(t *testing.T) {
	for _, tc := range []struct {
		name string
		set  func(tm *TopologyManager)
	}{
		{"pending", func(tm *TopologyManager) { tm.deploymentUpdatePending = true }},
		{"running", func(tm *TopologyManager) { tm.deploymentUpdateRunning = true }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			tm, _, _ := newTestTopologyManager(&mockMySQL{}, &mockMySQL{readOnly: true})
			tm.mu.Lock()
			tc.set(tm)
			tm.mu.Unlock()
			ok, why := tm.ReserveKeyringRotation("dc2")
			if ok || why != "an ordered update is in progress" {
				t.Fatalf("ReserveKeyringRotation = %v, %q; want refused for the ordered update", ok, why)
			}
			tm.mu.RLock()
			blocked := tm.keyringRotationBlockedLocked("dc2")
			tm.mu.RUnlock()
			if blocked {
				t.Fatal("a refused reservation must not block promotion")
			}
		})
	}
}

func TestReserveKeyringRotation_BlocksPromotionUntilStampedOrReleased(t *testing.T) {
	tm, _, _ := newTestTopologyManager(&mockMySQL{}, &mockMySQL{readOnly: true})
	settleAuthority(tm)
	if ok, why := tm.ReserveKeyringRotation("dc2"); !ok {
		t.Fatalf("reservation refused with no update in flight: %s", why)
	}
	tm.mu.RLock()
	block := tm.handoffBlockLocked("dc2")
	dirty := tm.keyringRotationBlockedDirty
	tm.mu.RUnlock()
	if !block.rotation || !dirty {
		t.Fatalf("reserved site: rotation=%v dirty=%v, want both true", block.rotation, dirty)
	}
	if err := tm.liveHandoffRefusal("dc2")(); err == nil {
		t.Fatal("live handoff check accepted a reserved site")
	}

	// A cache-derived push never drops a reservation, even one that lacks
	// the site: the cache may not have caught up with the stamp yet.
	tm.SetKeyringRotationBlocked(nil)
	if err := tm.liveHandoffRefusal("dc2")(); err == nil {
		t.Fatal("a cache-derived push dropped the reservation")
	}
	// An authoritative view carrying the stamp keeps it; one lacking it
	// (rotation finished) ends it.
	tm.PublishKeyringRotationBlocked([]string{"dc2"})
	tm.PublishKeyringRotationBlocked(nil)
	tm.mu.RLock()
	blocked := tm.keyringRotationBlockedLocked("dc2")
	tm.mu.RUnlock()
	if blocked {
		t.Fatal("reservation outlived the authoritative view that ended the rotation")
	}

	// A reservation whose stamp never persisted is released explicitly.
	tm.ReserveKeyringRotation("dc2")
	tm.PublishKeyringRotationBlocked(nil)
	if err := tm.liveHandoffRefusal("dc2")(); err != nil {
		t.Fatalf("released reservation still blocks the handoff: %v", err)
	}
}

func TestHandoffBlockErr(t *testing.T) {
	for _, tc := range []struct {
		name  string
		block handoffBlock
		want  string
	}{
		{"eligible", handoffBlock{}, ""},
		{"rotation", handoffBlock{rotation: true}, "UnsealReason=Rotation"},
		{"reclone", handoffBlock{reclonePending: true}, "pending post-restore reclone"},
		{"divergent", handoffBlock{divergence: siteRecovery{state: v1alpha1.RecoveryStateBlocked, divergentCount: 3}}, "3 transactions the primary lacks"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			err := tc.block.err("iad")
			if tc.want == "" {
				if err != nil {
					t.Fatalf("err = %v, want nil", err)
				}
				return
			}
			if err == nil || !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("err = %v, want %q", err, tc.want)
			}
		})
	}
}

// status.activeSite lags a promotion, so the live admission refuses the
// site this manager observes as primary, and any rotation while a
// promotion is in flight or not yet observed.
func TestReserveKeyringRotation_RefusesLivePrimaryAndPromotionInFlight(t *testing.T) {
	for _, tc := range []struct {
		name string
		set  func(tm *TopologyManager)
		site string
	}{
		{"observed primary", func(tm *TopologyManager) {
			tm.sites[0].state, tm.sites[1].state = state.StateWritable, state.StateReadOnly
		}, "dc1"},
		{"promotion not yet observed", func(tm *TopologyManager) { tm.promotedSite = "dc2" }, "dc2"},
		{"promotion in flight", func(tm *TopologyManager) { tm.deploymentChanging = 1 }, "dc2"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			tm, _, _ := newTestTopologyManager(&mockMySQL{}, &mockMySQL{readOnly: true})
			tm.mu.Lock()
			tc.set(tm)
			tm.mu.Unlock()
			if ok, why := tm.ReserveKeyringRotation(tc.site); ok {
				t.Fatalf("rotation of %s admitted", tc.site)
			} else if why == "" {
				t.Fatal("refusal carries no reason")
			}
		})
	}
	tm, _, _ := newTestTopologyManager(&mockMySQL{}, &mockMySQL{readOnly: true})
	tm.mu.Lock()
	tm.sites[0].state, tm.sites[1].state = state.StateWritable, state.StateReadOnly
	tm.mu.Unlock()
	if ok, why := tm.ReserveKeyringRotation("dc2"); !ok {
		t.Fatalf("rotation of a settled replica refused: %s", why)
	}
}

// settleAuthority makes dc1 the observed writable primary and dc2 a
// read-only replica, the state rotation admission requires.
func settleAuthority(tm *TopologyManager) {
	tm.mu.Lock()
	defer tm.mu.Unlock()
	tm.sites[0].state, tm.sites[1].state = state.StateWritable, state.StateReadOnly
}

// Promotion claim and rotation admission are decided under one lock: a
// rotation admitted before the claim stops the promotion, and one requested
// after the claim is refused.
func TestBeginPromotionOf_ExcludesRotationAdmission(t *testing.T) {
	tm, _, _ := newTestTopologyManager(&mockMySQL{}, &mockMySQL{readOnly: true})
	settleAuthority(tm)
	if ok, why := tm.ReserveKeyringRotation("dc2"); !ok {
		t.Fatalf("setup: %s", why)
	}
	if began, blocked := tm.beginAutoPromotionOf("dc2"); began || !blocked {
		t.Fatalf("beginAutoPromotionOf after a reservation = began %v, blocked %v; want refused", began, blocked)
	}
	tm.PublishKeyringRotationBlocked(nil)

	began, blocked := tm.beginAutoPromotionOf("dc2")
	if !began || blocked {
		t.Fatalf("beginAutoPromotionOf = began %v, blocked %v; want claimed", began, blocked)
	}
	if ok, _ := tm.ReserveKeyringRotation("dc2"); ok {
		t.Fatal("rotation admitted while a promotion of the site is claimed")
	}
	tm.finishDeploymentTopologyChange()
	if ok, why := tm.ReserveKeyringRotation("dc2"); ok || why != rotationRefusedPromotionInFlight {
		t.Fatalf("rotation before the promotion is observed = %v, %q; want refused", ok, why)
	}
}

// A manager that has not yet observed a unique writable primary with the
// target read-only cannot tell whether the target is the real primary:
// status.activeSite may lag a promotion or an operator restart.
func TestReserveKeyringRotation_RequiresObservedAuthority(t *testing.T) {
	tm, _, _ := newTestTopologyManager(&mockMySQL{}, &mockMySQL{readOnly: true})
	if ok, why := tm.ReserveKeyringRotation("dc2"); ok || why != rotationRefusedAuthorityUnsettled {
		t.Fatalf("fresh manager = %v, %q; want refused until authority is observed", ok, why)
	}
	tm.mu.Lock()
	tm.sites[0].state = state.StateWritable
	tm.mu.Unlock()
	if ok, why := tm.ReserveKeyringRotation("dc2"); ok || why != rotationRefusedAuthorityUnsettled {
		t.Fatalf("target not yet observed read-only = %v, %q; want refused", ok, why)
	}
	settleAuthority(tm)
	if ok, why := tm.ReserveKeyringRotation("dc2"); !ok {
		t.Fatalf("settled replica refused: %s", why)
	}
}

// The replacement-manager case GPT describes: status still names dc1, but
// dc2 was promoted before the restart. A new manager must not admit a
// rotation of dc2 before it has polled, nor after it has seen dc2 writable.
func TestReserveKeyringRotation_StaleStatusAfterRestartRefusesRealPrimary(t *testing.T) {
	oldPrimary := &mockMySQL{readOnly: true}
	realPrimary := &mockMySQL{readOnly: false}
	tm, _, _ := newTestTopologyManager(oldPrimary, realPrimary)
	if ok, _ := tm.ReserveKeyringRotation("dc2"); ok {
		t.Fatal("unpolled replacement admitted a rotation of the real primary")
	}
	pollN(tm, 4)
	if ok, why := tm.ReserveKeyringRotation("dc2"); ok {
		t.Fatalf("rotation of the observed primary admitted (%s)", why)
	}
	if ok, why := tm.ReserveKeyringRotation("dc1"); !ok {
		t.Fatalf("rotation of the actual replica refused: %s", why)
	}
}

// A reservation that slips in between the emergency failover's eligibility
// check and its claim stops the promotion: no Execute, and the dirty bit is
// re-armed so the next poll re-decides without the candidate.
func TestEmergencyFailover_ReservationBeforeClaimStopsPromotion(t *testing.T) {
	site0 := &mockMySQL{readOnly: false}
	site1 := &mockMySQL{readOnly: true}
	tm, _, _ := newTestTopologyManager(site0, site1)
	pollN(tm, 2)
	tm.beforePromotionClaim = func(target string) {
		tm.restoreKeyringRotationReservations([]string{target})
	}
	site0.setError(errors.New("connection refused"))
	pollN(tm, 3)

	site1.mu.Lock()
	promoted, readOnly := site1.promoted, site1.readOnly
	site1.mu.Unlock()
	if promoted || !readOnly {
		t.Fatal("emergency failover promoted a site whose rotation was admitted before the claim")
	}
	tm.mu.RLock()
	changing := tm.deploymentChanging
	tm.mu.RUnlock()
	if changing != 0 {
		t.Fatalf("refused claim left deploymentChanging=%d", changing)
	}
}

// The same at the primary re-assert: no unfence, no cooldown charged, and
// the dirty bit re-armed.
func TestPrimaryReassert_ReservationBeforeClaimStopsUnfence(t *testing.T) {
	site0 := &mockMySQL{readOnly: true, gtidExecuted: "aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa:1-10"}
	site1 := &mockMySQL{readOnly: true, gtidExecuted: "aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa:1-8"}
	tm, _, _ := newTestTopologyManager(site0, site1)
	setWedgedTopology(tm, "dc1")
	tm.beforePromotionClaim = func(target string) {
		tm.restoreKeyringRotationReservations([]string{target})
	}
	tm.clearKeyringRotationBlockedDirty()

	if tm.checkPrimaryReassert(context.Background()) {
		t.Fatal("re-assert ran against a site whose rotation was admitted before the claim")
	}
	site0.mu.Lock()
	readOnly := site0.readOnly
	site0.mu.Unlock()
	tm.mu.RLock()
	stamped, dirty := tm.lastReassert, tm.keyringRotationBlockedDirty
	tm.mu.RUnlock()
	if !readOnly {
		t.Fatal("re-assert unfenced a site mid-keyring-rotation")
	}
	if !stamped.IsZero() {
		t.Fatal("a refused claim charged the re-assert cooldown")
	}
	if !dirty {
		t.Fatal("refused claim did not re-arm the cross-site dirty bit")
	}
}

// While a replaced manager's update is still unwinding, the replacement
// takes no cross-site action: no failover, no re-assert.
func TestPredecessorUpdateHoldsCrossSiteActions(t *testing.T) {
	site0 := &mockMySQL{readOnly: false}
	site1 := &mockMySQL{readOnly: true}
	tm, _, _ := newTestTopologyManager(site0, site1)
	pollN(tm, 2)
	tm.setPredecessorUpdateInFlight(true)
	site0.setError(errors.New("connection refused"))
	pollN(tm, 4)
	site1.mu.Lock()
	readOnly := site1.readOnly
	site1.mu.Unlock()
	if !readOnly {
		t.Fatal("replacement failed over while its predecessor's update was in flight")
	}

	wedged, _, _ := newTestTopologyManager(&mockMySQL{readOnly: true, gtidExecuted: "aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa:1-10"},
		&mockMySQL{readOnly: true, gtidExecuted: "aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa:1-8"})
	setWedgedTopology(wedged, "dc1")
	wedged.setPredecessorUpdateInFlight(true)
	if wedged.checkPrimaryReassert(context.Background()) {
		t.Fatal("replacement re-asserted while its predecessor's update was in flight")
	}

	tm.setPredecessorUpdateInFlight(false)
	pollN(tm, 3)
	site1.mu.Lock()
	readOnly = site1.readOnly
	site1.mu.Unlock()
	if readOnly {
		t.Fatal("failover did not resume once the predecessor's update ended")
	}
}
