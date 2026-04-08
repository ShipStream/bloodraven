package controller

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/shipstream/bloodraven/internal/clock"
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
func newSafetyTestTM(dc1, dc2 *mockMySQL) (*TopologyManager, *mockTainter, *mockDNS, *clock.FakeClock) {
	cfg := testTopologyConfig()
	tainter := newMockTainter()
	hub := platform.NewHub(testLogger())
	dns := &mockDNS{}
	fc := NewFailoverController(testLogger())
	start := time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC)
	clk := clock.NewFakeClock(start)
	tm := NewTopologyManagerWithClock(cfg, dc1, dc2, fc, tainter, hub, dns, testLogger(), clk)
	tm.failoverCooldown = 0 // disabled by default for most tests
	return tm, tainter, dns, clk
}

// INVARIANT 1: Never expose two primaries through the primary service at the same time.
//
// If both DCs report writable (split brain), the system must NOT promote
// either one or flip DNS. The primary service selector uses the "role=primary"
// label which is set by the reconciler based on status.PrimaryDC. The topology
// manager must not set promotedDC or trigger DNS flips in split brain.
func TestInvariant_NeverExposeTwoPrimaries(t *testing.T) {
	dc1 := &mockMySQL{readOnly: false} // writable
	dc2 := &mockMySQL{readOnly: false} // writable (split brain)
	tm, _, dns, _ := newSafetyTestTM(dc1, dc2)

	// Poll enough times for recovery threshold
	pollN(tm, 5)

	// DNS must never have been flipped
	if dns.getLastIP() != "" {
		t.Error("SAFETY VIOLATION: DNS flipped during split brain — could expose two primaries")
	}

	// No promotion should have been initiated
	if tm.promotedDC != "" {
		t.Errorf("SAFETY VIOLATION: promotedDC=%q during split brain", tm.promotedDC)
	}
}

// INVARIANT 2: Never flip DNS before promoted MySQL is confirmed writable.
//
// After failover, the DNS flip must be deferred until the promoted DC's
// read_only=0 is confirmed through the recovery threshold.
func TestInvariant_NeverFlipDNSBeforeConfirmation(t *testing.T) {
	dc1 := &mockMySQL{readOnly: false}
	dc2 := &mockMySQL{readOnly: true}
	tm, _, dns, _ := newSafetyTestTM(dc1, dc2)

	// Establish normal state (dc1 primary)
	pollN(tm, 2)

	// dc1 goes down
	dc1.setError(errors.New("connection refused"))

	// Hit failure threshold — triggers failover
	pollN(tm, 3)

	// At this point, FailoverController.Execute has set dc2's readOnly=false,
	// but the topology manager has NOT yet confirmed it through polls.
	// DNS must not have flipped yet.
	if dns.getLastIP() != "" {
		t.Error("SAFETY VIOLATION: DNS flipped before promotion was confirmed via polling")
	}

	// Now poll to confirm (recovery threshold = 2)
	pollN(tm, 2)

	// NOW DNS should have flipped
	if dns.getLastIP() != "2.2.2.2" {
		t.Errorf("DNS should point to dc2 after confirmation, got %q", dns.getLastIP())
	}
}

// INVARIANT 3: Never auto-unfence a self-fenced primary.
//
// Once the fencing monitor sets fenced=true, it must remain true. Only
// Bloodraven (via manual intervention) can restore it.
func TestInvariant_NeverAutoUnfenceSelfFencedPrimary(t *testing.T) {
	dc1 := &mockMySQL{readOnly: false}
	dc2 := &mockMySQL{readOnly: true}
	tm, _, _, _ := newSafetyTestTM(dc1, dc2)

	// Establish normal state
	pollN(tm, 2)

	// Simulate dc1 being fenced by the sidecar (super_read_only=ON)
	dc1.mu.Lock()
	dc1.readOnly = true
	dc1.mu.Unlock()

	// Poll many times — the topology manager should never promote dc1 back
	// just because it's now read-only. It would need to become writable again
	// first, which requires manual intervention.
	pollN(tm, 10)

	// dc1 should still be read-only (the topology manager doesn't unfence)
	dc1.mu.Lock()
	ro := dc1.readOnly
	dc1.mu.Unlock()
	if !ro {
		t.Error("SAFETY VIOLATION: topology manager auto-unfenced dc1")
	}
}

// INVARIANT 4: Never promote during anti-flap cooldown.
func TestInvariant_NeverPromoteDuringCooldown(t *testing.T) {
	dc1 := &mockMySQL{readOnly: false}
	dc2 := &mockMySQL{readOnly: true}

	cfg := testTopologyConfig()
	cfg.FailoverCooldown = int64(1 * time.Hour)
	tainter := newMockTainter()
	hub := platform.NewHub(testLogger())
	dns := &mockDNS{}
	fc := NewFailoverController(testLogger())
	start := time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC)
	clk := clock.NewFakeClock(start)
	tm := NewTopologyManagerWithClock(cfg, dc1, dc2, fc, tainter, hub, dns, testLogger(), clk)

	// Establish normal state
	pollN(tm, 2)

	// First failover: dc1 goes down
	dc1.setError(errors.New("down"))
	pollN(tm, 3) // hits threshold, triggers failover

	// Verify failover happened
	dc2.mu.Lock()
	dc2RO := dc2.readOnly
	dc2.mu.Unlock()
	if dc2RO {
		t.Fatal("dc2 should have been promoted")
	}

	// Advance only 30 minutes (cooldown is 1 hour)
	clk.Advance(30 * time.Minute)

	// Now dc2 goes down too, dc1 comes back
	dc1.setReadOnly(true) // comes back as replica
	dc2.setError(errors.New("down"))

	// Poll to threshold
	pollN(tm, 5)

	// dc1 should NOT have been promoted (cooldown active)
	dc1.mu.Lock()
	dc1RO := dc1.readOnly
	dc1.mu.Unlock()
	if !dc1RO {
		t.Error("SAFETY VIOLATION: dc1 promoted during anti-flap cooldown")
	}
}

// INVARIANT 5: Never self-fence a replica.
//
// The fencing monitor must check IsReadOnly first. If the instance is
// already read-only (replica), it must skip fencing.
func TestInvariant_NeverSelfFenceReplica(t *testing.T) {
	// This is tested in sidecar/fencing_test.go TestEvaluateDoesNotFenceWhenAlreadyReadOnly
	// but we also verify it at the state-machine level.

	// A replica (read-only) DC should never trigger taint removal or
	// promotion actions by the topology manager.
	dc1 := &mockMySQL{readOnly: true}
	dc2 := &mockMySQL{readOnly: true}
	tm, _, dns, _ := newSafetyTestTM(dc1, dc2)

	pollN(tm, 5)

	// Neither DC should be promoted
	if dns.getLastIP() != "" {
		t.Error("SAFETY VIOLATION: promoted a read-only DC")
	}

	// Both should remain in read-only state in the topology manager
	if tm.dc1.state != state.StateReadOnly {
		t.Errorf("dc1 state: got %s, want read-only", tm.dc1.state)
	}
	if tm.dc2.state != state.StateReadOnly {
		t.Errorf("dc2 state: got %s, want read-only", tm.dc2.state)
	}
}

// INVARIANT 6: Never treat transient poll failures as confirmed outage before threshold.
func TestInvariant_NeverTreatTransientFailureAsOutage(t *testing.T) {
	dc1 := &mockMySQL{readOnly: false}
	dc2 := &mockMySQL{readOnly: true}
	tm, _, dns, _ := newSafetyTestTM(dc1, dc2)

	// Establish normal state
	pollN(tm, 2)

	// Single failure — below threshold of 3
	dc1.setError(errors.New("timeout"))
	pollN(tm, 1)

	if tm.dc1.state == state.StateUnreachable {
		t.Error("SAFETY VIOLATION: single failure marked DC as unreachable before threshold")
	}
	if dns.getLastIP() != "" {
		t.Error("SAFETY VIOLATION: DNS flipped on transient failure")
	}

	// Two failures — still below threshold of 3
	pollN(tm, 1)

	if tm.dc1.state == state.StateUnreachable {
		t.Error("SAFETY VIOLATION: two failures marked DC as unreachable before threshold of 3")
	}

	// Recovery before threshold
	dc1.setReadOnly(false)
	pollN(tm, 2)

	if tm.dc1.state != state.StateWritable {
		t.Errorf("dc1 should recover to writable, got %s", tm.dc1.state)
	}
}

// INVARIANT 7: Never treat a single writable observation as recovered primary before threshold.
func TestInvariant_NeverTreatSingleWritableAsRecovered(t *testing.T) {
	dc1 := &mockMySQL{readOnly: true}
	dc2 := &mockMySQL{readOnly: false}
	tm, _, _, _ := newSafetyTestTM(dc1, dc2)

	// Establish dc2 as primary
	pollN(tm, 2)

	// dc1 becomes writable (recovery threshold = 2)
	dc1.setReadOnly(false)

	// Single poll — should NOT yet be writable
	pollN(tm, 1)

	if tm.dc1.state == state.StateWritable {
		t.Error("SAFETY VIOLATION: single writable observation treated as confirmed recovery")
	}

	// Second poll — NOW should be writable
	pollN(tm, 1)

	if tm.dc1.state != state.StateWritable {
		t.Errorf("dc1 should be writable after recovery threshold, got %s", tm.dc1.state)
	}
}

// INVARIANT 8: Never silently ignore status drift.
//
// Verifies that every state transition triggers the StatusCallback with
// correct snapshot data including PrimaryDC, DC states, and failover info.
// The reconciler uses these snapshots to update pod labels (role, healthy)
// and sidecar inputs. If the callback is missed or contains wrong data,
// pod labels and sidecar env vars will drift from actual state.
func TestInvariant_NeverSilentlyIgnoreStatusDrift(t *testing.T) {
	dc1 := &mockMySQL{readOnly: false}
	dc2 := &mockMySQL{readOnly: true}
	tm, _, _, _ := newSafetyTestTM(dc1, dc2)

	var callbacks []TopologySnapshot
	tm.StatusCallback = func(snap TopologySnapshot) {
		callbacks = append(callbacks, snap)
	}

	// Initial polls: Unknown -> ReadOnly for dc2 on first poll,
	// then dc1 needs RecoveryThreshold (2) polls to confirm Writable.
	pollN(tm, 1)

	if len(callbacks) == 0 {
		t.Fatal("SAFETY VIOLATION: status callback not invoked on initial state transition")
	}

	// First poll: dc2 becomes ReadOnly (immediate), dc1 not yet confirmed writable.
	// Verify callback was invoked with the transition data.
	initial := callbacks[len(callbacks)-1]
	if initial.DC2State != state.StateReadOnly {
		t.Errorf("initial snapshot DC2State: got %v, want ReadOnly", initial.DC2State)
	}

	// Second poll: dc1 recovery threshold met -> Writable
	pollN(tm, 1)
	last := callbacks[len(callbacks)-1]
	if last.DC1State != state.StateWritable {
		t.Errorf("snapshot after recovery threshold DC1State: got %v, want Writable", last.DC1State)
	}

	// State change: dc1 goes down -> should trigger callback with
	// state that would cause pod label updates
	dc1.setError(errors.New("down"))
	callbacks = nil

	// Poll to failure threshold
	pollN(tm, 3)

	gotTransition := false
	for _, snap := range callbacks {
		if snap.DC1State == state.StateUnreachable {
			gotTransition = true
			// The reconciler uses PrimaryDC to set role=primary/replica labels.
			// When DC1 goes unreachable and DC2 is promoted, PrimaryDC must change.
			// Verify snapshot contains failover target info.
			if snap.LastFailoverTarget == "" && snap.PrimaryDC == "" {
				t.Error("SAFETY VIOLATION: snapshot after failover has no primary DC or failover target — pod labels would not update")
			}
		}
	}
	if !gotTransition {
		t.Error("SAFETY VIOLATION: status callback not invoked when DC1 became unreachable")
	}

	// Verify every state change produces a callback
	dc1.setReadOnly(true) // dc1 comes back as read-only
	callbacks = nil
	pollN(tm, 1)

	if len(callbacks) == 0 {
		t.Error("SAFETY VIOLATION: status callback not invoked when DC1 recovered from unreachable to read-only")
	}
}

// INVARIANT: DNS flips at most once per failover event.
func TestInvariant_DNSFlipsOnlyOnce(t *testing.T) {
	dc1 := &mockMySQL{readOnly: false}
	dc2 := &mockMySQL{readOnly: true}
	tm, _, dns, _ := newSafetyTestTM(dc1, dc2)

	// Establish normal state
	pollN(tm, 2)

	// dc1 goes down
	dc1.setError(errors.New("down"))

	// Hit threshold, trigger failover
	pollN(tm, 3)

	// Poll many more times while dc1 stays down and dc2 is writable
	pollN(tm, 10)

	dns.mu.Lock()
	calls := dns.calls
	dns.mu.Unlock()

	if calls > 1 {
		t.Errorf("SAFETY VIOLATION: DNS flipped %d times, expected at most 1", calls)
	}
}

// INVARIANT: Failover uses correct step sequence.
// Fence -> Drain -> Stop -> Reset -> Clear super_read_only -> Set read_only=0
func TestInvariant_FailoverSequence(t *testing.T) {
	candidate := &trackingMock{readOnly: true}
	oldPrimary := &trackingMock{}
	fc := NewFailoverController(testLogger())

	err := fc.Execute(context.Background(), candidate, oldPrimary, "dc2")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	// Old primary must be fenced first
	opCalls := oldPrimary.getCalls()
	if len(opCalls) != 1 || opCalls[0] != "SetSuperReadOnly(ON)" {
		t.Errorf("SAFETY VIOLATION: old primary not fenced first, calls: %v", opCalls)
	}

	// Candidate must follow exact sequence
	candCalls := candidate.getCalls()
	expected := []string{"WaitForRelayLogDrain", "StopReplica", "ResetReplicaAll", "SetSuperReadOnly(OFF)", "SetReadOnly(OFF)"}
	if len(candCalls) != len(expected) {
		t.Fatalf("SAFETY VIOLATION: wrong number of candidate calls: %v, want %v", candCalls, expected)
	}
	for i, want := range expected {
		if candCalls[i] != want {
			t.Errorf("SAFETY VIOLATION: candidate call[%d]: got %q, want %q", i, candCalls[i], want)
		}
	}
}
