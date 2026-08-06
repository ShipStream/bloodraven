package controller

import (
	"context"
	"errors"
	"testing"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/shipstream/bloodraven/api/v1alpha1"
	"github.com/shipstream/bloodraven/internal/clock"
	internalmysql "github.com/shipstream/bloodraven/internal/mysql"
	"github.com/shipstream/bloodraven/internal/platform"
	"github.com/shipstream/bloodraven/internal/state"
)

// ---------------------------------------------------------------------------
// Safety Invariant Tests (Testing 2.0)
//
// Each test validates one of the non-negotiable safety invariants listed in
// TESTING_2.0.md. These are release-blocking requirements.
// ---------------------------------------------------------------------------

// newSafetyTestTM creates a TopologyManager with a FakeClock for safety tests.
func newSafetyTestTM(site0, site1 *mockMySQL) (*TopologyManager, *mockTainter, *mockDNS, *clock.FakeClock) {
	cfg := testTopologyConfig()
	tainter := newMockTainter()
	hub := platform.NewHub(testLogger())
	dns := &mockDNS{}
	fc := NewFailoverController(testLogger())
	start := time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC)
	clk := clock.NewFakeClock(start)
	tm := NewTopologyManagerWithClock(cfg, []internalmysql.Checker{site0, site1}, fc, nil, nil, BootstrapConfig{}, tainter, hub, dns, testLogger(), clk)
	tm.failoverCooldown = 0 // disabled by default for most tests
	return tm, tainter, dns, clk
}

// INVARIANT 1: Never expose two primaries through the primary service at the same time.
//
// If both sites report writable (split brain), the system must NOT promote
// either one or flip DNS. The primary service selector uses the "role=primary"
// label which is set by the reconciler based on status.ActiveSite. The topology
// manager must not set promotedSite or trigger DNS flips in split brain.
func TestInvariant_NeverExposeTwoPrimaries(t *testing.T) {
	site0 := &mockMySQL{readOnly: false} // writable
	site1 := &mockMySQL{readOnly: false} // writable (split brain)
	tm, _, dns, _ := newSafetyTestTM(site0, site1)

	// Poll enough times for recovery threshold
	pollN(tm, 5)

	// DNS must never have been flipped
	if dns.getLastIP() != "" {
		t.Error("SAFETY VIOLATION: DNS flipped during split brain — could expose two primaries")
	}

	// No promotion should have been initiated
	if tm.promotedSite != "" {
		t.Errorf("SAFETY VIOLATION: promotedSite=%q during split brain", tm.promotedSite)
	}
}

// INVARIANT 2: DNS flips at the time of failover trigger (before promotion).
//
// DNS is flipped immediately when failover is triggered so that DNS
// propagation overlaps with the relay-log drain and MySQL promotion.
func TestInvariant_DNSFlipsAtFailoverTrigger(t *testing.T) {
	site0 := &mockMySQL{readOnly: false}
	site1 := &mockMySQL{readOnly: true}
	tm, _, dns, _ := newSafetyTestTM(site0, site1)

	// Establish normal state (site0 primary)
	pollN(tm, 2)

	// site0 goes down
	site0.setError(errors.New("connection refused"))

	// Hit failure threshold — triggers failover + immediate DNS flip
	pollN(tm, 3)

	// DNS should have flipped immediately at failover trigger
	if dns.getLastIP() != "2.2.2.2" {
		t.Errorf("SAFETY VIOLATION: DNS should flip at failover trigger, got %q", dns.getLastIP())
	}

	// DNS should flip exactly once
	dns.mu.Lock()
	calls := dns.calls
	dns.mu.Unlock()
	if calls != 1 {
		t.Errorf("SAFETY VIOLATION: DNS should flip exactly once, got %d", calls)
	}
}

// INVARIANT 3: Never auto-unfence a self-fenced primary.
//
// Once the fencing monitor sets fenced=true, it must remain true. Only
// Bloodraven (via manual intervention) can restore it.
func TestInvariant_NeverAutoUnfenceSelfFencedPrimary(t *testing.T) {
	site0 := &mockMySQL{readOnly: false}
	site1 := &mockMySQL{readOnly: true}
	tm, _, _, _ := newSafetyTestTM(site0, site1)

	// Establish normal state
	pollN(tm, 2)

	// Simulate site0 being fenced by the sidecar (super_read_only=ON)
	site0.mu.Lock()
	site0.readOnly = true
	site0.mu.Unlock()

	// Poll many times — the topology manager should never promote site0 back
	// just because it's now read-only. It would need to become writable again
	// first, which requires manual intervention.
	pollN(tm, 10)

	// site0 should still be read-only (the topology manager doesn't unfence)
	site0.mu.Lock()
	ro := site0.readOnly
	site0.mu.Unlock()
	if !ro {
		t.Error("SAFETY VIOLATION: topology manager auto-unfenced site0")
	}
}

// INVARIANT 4: Never promote during anti-flap cooldown.
func TestInvariant_NeverPromoteDuringCooldown(t *testing.T) {
	site0 := &mockMySQL{readOnly: false}
	site1 := &mockMySQL{readOnly: true}

	cfg := testTopologyConfig()
	cfg.FailoverCooldown = int64(1 * time.Hour)
	tainter := newMockTainter()
	hub := platform.NewHub(testLogger())
	dns := &mockDNS{}
	fc := NewFailoverController(testLogger())
	start := time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC)
	clk := clock.NewFakeClock(start)
	tm := NewTopologyManagerWithClock(cfg, []internalmysql.Checker{site0, site1}, fc, nil, nil, BootstrapConfig{}, tainter, hub, dns, testLogger(), clk)

	// Establish normal state
	pollN(tm, 2)

	// First failover: site0 goes down
	site0.setError(errors.New("down"))
	pollN(tm, 3) // hits threshold, triggers failover

	// Verify failover happened
	site1.mu.Lock()
	site1RO := site1.readOnly
	site1.mu.Unlock()
	if site1RO {
		t.Fatal("site1 should have been promoted")
	}

	// Advance only 30 minutes (cooldown is 1 hour)
	clk.Advance(30 * time.Minute)

	// Now site1 goes down too, site0 comes back
	site0.setReadOnly(true) // comes back as replica
	site1.setError(errors.New("down"))

	// Poll to threshold
	pollN(tm, 5)

	// site0 should NOT have been promoted (cooldown active)
	site0.mu.Lock()
	site0RO := site0.readOnly
	site0.mu.Unlock()
	if !site0RO {
		t.Error("SAFETY VIOLATION: site0 promoted during anti-flap cooldown")
	}
}

// INVARIANT 4b: Anti-flap cooldown survives operator restart.
//
// After a process restart, a fresh TopologyManager starts with
// tm.lastFailover == time.Time{} (zero), which makes the cooldown branch in
// topology.go a no-op. The runner hydration path MUST install the newer of
// CR status and the out-of-band annotations so the cooldown still applies
// when either API path missed the promotion write.
//
// This test simulates the restart by constructing a fresh
// TopologyManager (the invariant under test is "SetLastFailover is
// honoured by the cooldown branch") and verifies no promotion fires
// while the rehydrated cooldown is active.
func TestInvariant_CooldownSurvivesOperatorRestart(t *testing.T) {
	// Simulate the cluster state seen by a freshly-started operator:
	// site0 (the previously-killed primary) is unreachable, site1 was
	// promoted and is writable. The new primary is now also gone (the
	// scenario s09b-anti-flap-cooldown shape: peer fails inside the
	// cooldown), so site0 has been brought back as a read-only candidate.
	site0 := &mockMySQL{readOnly: true}                           // candidate
	site1 := &mockMySQL{readOnly: false, err: errors.New("down")} // unreachable

	cfg := testTopologyConfig()
	cfg.FailoverCooldown = int64(1 * time.Hour)
	tainter := newMockTainter()
	hub := platform.NewHub(testLogger())
	dns := &mockDNS{}
	fc := NewFailoverController(testLogger())
	start := time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC)
	clk := clock.NewFakeClock(start)
	tm := NewTopologyManagerWithClock(cfg, []internalmysql.Checker{site0, site1}, fc, nil, nil, BootstrapConfig{}, tainter, hub, dns, testLogger(), clk)

	// Status is stale, while the annotation copy carries the promotion from
	// 30 minutes ago. Rehydrate the effective record exactly as the runner
	// does and verify that the out-of-band winner enforces the cooldown.
	statusStamp := metav1.NewTime(start.Add(-2 * time.Hour))
	fg := &v1alpha1.MysqlFailoverGroup{
		Spec: v1alpha1.MysqlFailoverGroupSpec{Sites: []v1alpha1.SiteSpec{{Name: "dc1"}, {Name: "dc2"}}},
		ObjectMeta: metav1.ObjectMeta{Annotations: map[string]string{
			LastFailoverAnnotation:       start.Add(-30 * time.Minute).Format(time.RFC3339),
			LastFailoverTargetAnnotation: "dc2",
		}},
		Status: v1alpha1.MysqlFailoverGroupStatus{LastFailover: &statusStamp, LastFailoverTarget: "dc1"},
	}
	record, fromAnnotations, err := EffectiveFailoverRecord(fg, start)
	if err != nil {
		t.Fatalf("effective failover record: %v", err)
	}
	if !fromAnnotations {
		t.Fatal("expected annotation copy to win over stale status")
	}
	tm.SetLastFailoverTarget(record.LastFailoverTarget)
	tm.SetLastFailover(record.LastFailover)

	// Drive the topology manager. With site0=read-only and
	// site1=unreachable + writable=0, EvalCrossSite returns
	// PromotionCandidates=[dc1]. The cooldown branch must reject the
	// promotion despite this being a fresh in-memory state.
	pollN(tm, 5)

	site0.mu.Lock()
	site0RO := site0.readOnly
	site0.mu.Unlock()
	if !site0RO {
		t.Error("SAFETY VIOLATION: site0 was promoted by a restarted operator inside the rehydrated cooldown window")
	}
	if dns.getLastIP() != "" {
		t.Errorf("SAFETY VIOLATION: DNS flipped during rehydrated cooldown (lastIP=%q)", dns.getLastIP())
	}
}

// INVARIANT 4c: After the rehydrated cooldown has elapsed, the
// restarted operator may promote normally. This is the partner of
// INVARIANT 4b: it confirms the rehydration is not a permanent
// "block all promotions forever" — the timestamp + cooldown duration
// together must permit promotions once the wall clock advances past
// the window.
func TestInvariant_CooldownHonouredButExpiresAfterRestart(t *testing.T) {
	site0 := &mockMySQL{readOnly: true}                           // candidate
	site1 := &mockMySQL{readOnly: false, err: errors.New("down")} // unreachable

	cfg := testTopologyConfig()
	cfg.FailoverCooldown = int64(5 * time.Minute)
	tainter := newMockTainter()
	hub := platform.NewHub(testLogger())
	dns := &mockDNS{}
	fc := NewFailoverController(testLogger())
	start := time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC)
	clk := clock.NewFakeClock(start)
	tm := NewTopologyManagerWithClock(cfg, []internalmysql.Checker{site0, site1}, fc, nil, nil, BootstrapConfig{}, tainter, hub, dns, testLogger(), clk)

	// Rehydrate: previous failover happened 6 minutes ago, cooldown is
	// 5 minutes, so it has already expired.
	tm.SetLastFailoverTarget("dc2")
	tm.SetLastFailover(start.Add(-6 * time.Minute))

	pollN(tm, 5)

	site0.mu.Lock()
	site0RO := site0.readOnly
	site0.mu.Unlock()
	if site0RO {
		t.Error("SAFETY VIOLATION: cooldown remained active despite the rehydrated timestamp being older than the cooldown duration")
	}
	if dns.getLastIP() == "" {
		t.Error("expected DNS flip after rehydrated cooldown expired")
	}
}

// INVARIANT 5: Never self-fence a replica.
//
// The fencing monitor must check IsReadOnly first. If the instance is
// already read-only (replica), it must skip fencing.
func TestInvariant_NeverSelfFenceReplica(t *testing.T) {
	// This is tested in sidecar/fencing_test.go TestEvaluateDoesNotFenceWhenAlreadyReadOnly
	// but we also verify it at the state-machine level.

	// A replica (read-only) site should never trigger taint removal or
	// promotion actions by the topology manager.
	site0 := &mockMySQL{readOnly: true}
	site1 := &mockMySQL{readOnly: true}
	tm, _, dns, _ := newSafetyTestTM(site0, site1)

	pollN(tm, 5)

	// Neither site should be promoted
	if dns.getLastIP() != "" {
		t.Error("SAFETY VIOLATION: promoted a read-only site")
	}

	// Both should remain in read-only state in the topology manager
	if tm.sites[0].state != state.StateReadOnly {
		t.Errorf("site0 state: got %s, want read-only", tm.sites[0].state)
	}
	if tm.sites[1].state != state.StateReadOnly {
		t.Errorf("site1 state: got %s, want read-only", tm.sites[1].state)
	}
}

// INVARIANT 6: Never treat transient poll failures as confirmed outage before threshold.
func TestInvariant_NeverTreatTransientFailureAsOutage(t *testing.T) {
	site0 := &mockMySQL{readOnly: false}
	site1 := &mockMySQL{readOnly: true}
	tm, _, dns, _ := newSafetyTestTM(site0, site1)

	// Establish normal state
	pollN(tm, 2)

	// Single failure — below threshold of 3
	site0.setError(errors.New("timeout"))
	pollN(tm, 1)

	if tm.sites[0].state == state.StateUnreachable {
		t.Error("SAFETY VIOLATION: single failure marked site as unreachable before threshold")
	}
	if dns.getLastIP() != "" {
		t.Error("SAFETY VIOLATION: DNS flipped on transient failure")
	}

	// Two failures — still below threshold of 3
	pollN(tm, 1)

	if tm.sites[0].state == state.StateUnreachable {
		t.Error("SAFETY VIOLATION: two failures marked site as unreachable before threshold of 3")
	}

	// Recovery before threshold
	site0.setReadOnly(false)
	pollN(tm, 2)

	if tm.sites[0].state != state.StateWritable {
		t.Errorf("site0 should recover to writable, got %s", tm.sites[0].state)
	}
}

// INVARIANT 7: Never treat a single writable observation as recovered primary before threshold.
func TestInvariant_NeverTreatSingleWritableAsRecovered(t *testing.T) {
	site0 := &mockMySQL{readOnly: true}
	site1 := &mockMySQL{readOnly: false}
	tm, _, _, _ := newSafetyTestTM(site0, site1)

	// Establish site1 as primary
	pollN(tm, 2)

	// site0 becomes writable (recovery threshold = 2)
	site0.setReadOnly(false)

	// Single poll — should NOT yet be writable
	pollN(tm, 1)

	if tm.sites[0].state == state.StateWritable {
		t.Error("SAFETY VIOLATION: single writable observation treated as confirmed recovery")
	}

	// Second poll — NOW should be writable
	pollN(tm, 1)

	if tm.sites[0].state != state.StateWritable {
		t.Errorf("site0 should be writable after recovery threshold, got %s", tm.sites[0].state)
	}
}

// INVARIANT 8: Never silently ignore status drift.
//
// Verifies that every state transition triggers the StatusCallback with
// correct snapshot data including ActiveSite, site states, and failover info.
// The reconciler uses these snapshots to update pod labels (role, healthy)
// and sidecar inputs. If the callback is missed or contains wrong data,
// pod labels and sidecar env vars will drift from actual state.
func TestInvariant_NeverSilentlyIgnoreStatusDrift(t *testing.T) {
	site0 := &mockMySQL{readOnly: false}
	site1 := &mockMySQL{readOnly: true}
	tm, _, _, _ := newSafetyTestTM(site0, site1)

	var callbacks []TopologySnapshot
	tm.StatusCallback = func(snap TopologySnapshot) {
		callbacks = append(callbacks, snap)
	}

	// Initial polls: Unknown -> ReadOnly for site1 on first poll,
	// then site0 needs RecoveryThreshold (2) polls to confirm Writable.
	pollN(tm, 1)

	if len(callbacks) == 0 {
		t.Fatal("SAFETY VIOLATION: status callback not invoked on initial state transition")
	}

	// First poll: site1 becomes ReadOnly (immediate), site0 not yet confirmed writable.
	// Verify callback was invoked with the transition data.
	initial := callbacks[len(callbacks)-1]
	if initial.Sites[1].State != state.StateReadOnly {
		t.Errorf("initial snapshot Sites[1].State: got %v, want ReadOnly", initial.Sites[1].State)
	}

	// Second poll: site0 recovery threshold met -> Writable
	pollN(tm, 1)
	last := callbacks[len(callbacks)-1]
	if last.Sites[0].State != state.StateWritable {
		t.Errorf("snapshot after recovery threshold Sites[0].State: got %v, want Writable", last.Sites[0].State)
	}

	// State change: site0 goes down -> should trigger callback with
	// state that would cause pod label updates
	site0.setError(errors.New("down"))
	callbacks = nil

	// Poll to failure threshold
	pollN(tm, 3)

	gotTransition := false
	for _, snap := range callbacks {
		if snap.Sites[0].State == state.StateUnreachable {
			gotTransition = true
			// The reconciler uses ActiveSite to set role=primary/replica labels.
			// When site0 goes unreachable and site1 is promoted, ActiveSite must change.
			// Verify snapshot contains failover target info.
			if snap.LastFailoverTarget == "" && snap.ActiveSite == "" {
				t.Error("SAFETY VIOLATION: snapshot after failover has no active site or failover target — pod labels would not update")
			}
		}
	}
	if !gotTransition {
		t.Error("SAFETY VIOLATION: status callback not invoked when site0 became unreachable")
	}

	// Verify every state change produces a callback
	site0.setReadOnly(true) // site0 comes back as read-only
	callbacks = nil
	pollN(tm, 1)

	if len(callbacks) == 0 {
		t.Error("SAFETY VIOLATION: status callback not invoked when site0 recovered from unreachable to read-only")
	}
}

// INVARIANT: DNS flips at most once per failover event.
func TestInvariant_DNSFlipsOnlyOnce(t *testing.T) {
	site0 := &mockMySQL{readOnly: false}
	site1 := &mockMySQL{readOnly: true}
	tm, _, dns, _ := newSafetyTestTM(site0, site1)

	// Establish normal state
	pollN(tm, 2)

	// site0 goes down
	site0.setError(errors.New("down"))

	// Hit threshold, trigger failover
	pollN(tm, 3)

	// Poll many more times while site0 stays down and site1 is writable
	pollN(tm, 10)

	dns.mu.Lock()
	calls := dns.calls
	dns.mu.Unlock()

	if calls > 1 {
		t.Errorf("SAFETY VIOLATION: DNS flipped %d times, expected at most 1", calls)
	}
}

// INVARIANT: Failover uses correct step sequence.
// Fence -> Kill connections -> Drain -> Stop -> Reset -> GetGtid -> Clear super_read_only -> Set read_only=0
func TestInvariant_FailoverSequence(t *testing.T) {
	candidate := &trackingMock{readOnly: true}
	oldPrimary := &trackingMock{}
	fc := NewFailoverController(testLogger())

	_, err := fc.Execute(context.Background(), candidate, oldPrimary, "site1")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	// Old primary must be fenced and have connections killed
	opCalls := oldPrimary.getCalls()
	opExpected := []string{"SetSuperReadOnly(ON)", "KillAppConnections"}
	if len(opCalls) != len(opExpected) {
		t.Fatalf("SAFETY VIOLATION: old primary calls: %v, want %v", opCalls, opExpected)
	}
	for i, want := range opExpected {
		if opCalls[i] != want {
			t.Errorf("SAFETY VIOLATION: old primary call[%d]: got %q, want %q", i, opCalls[i], want)
		}
	}

	// Candidate must follow exact sequence
	candCalls := candidate.getCalls()
	expected := []string{"WaitForRelayLogDrain", "StopReplica", "ResetReplicaAll", "GetGtidExecuted", "SetSuperReadOnly(OFF)", "SetReadOnly(OFF)"}
	if len(candCalls) != len(expected) {
		t.Fatalf("SAFETY VIOLATION: wrong number of candidate calls: %v, want %v", candCalls, expected)
	}
	for i, want := range expected {
		if candCalls[i] != want {
			t.Errorf("SAFETY VIOLATION: candidate call[%d]: got %q, want %q", i, candCalls[i], want)
		}
	}
}
