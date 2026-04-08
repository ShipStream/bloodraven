package component

import (
	"testing"
	"time"
)

func TestFailover_DC1Down_PromoteDC2(t *testing.T) {
	h := newTestHarness(t) // DC1 writable, DC2 read-only

	// 1. Establish DC1 as primary, DC2 as replica (recovery threshold = 2 polls).
	h.pollN(2)

	s := h.tm.Status()
	if s.DC1State != "writable" {
		t.Fatalf("setup: dc1 state: got %s, want writable", s.DC1State)
	}
	if s.DC2State != "read-only" {
		t.Fatalf("setup: dc2 state: got %s, want read-only", s.DC2State)
	}

	// 2. DC1 goes down.
	h.dc1MySQL.setError(errDown)

	// 3. Poll 3x (failure threshold) -- DC1 becomes unreachable.
	h.pollN(3)

	s = h.tm.Status()
	if s.DC1State != "unreachable" {
		t.Errorf("after failure: dc1 state: got %s, want unreachable", s.DC1State)
	}

	// 4. Verify: DC1 is tainted.
	if !h.tainter.isTainted("lion-dc1") {
		t.Error("dc1 should be tainted after going unreachable")
	}

	// FailoverController.Execute calls SetReadOnly(false) on the candidate,
	// so DC2 should now report readOnly=false.
	if h.dc2MySQL.isReadOnly() {
		t.Error("dc2 should have been promoted (readOnly should be false)")
	}

	// DNS flip is deferred until promotion is confirmed via recovery threshold.
	if h.dns.getLastIP() != "" {
		t.Error("DNS should not flip before promotion confirmation")
	}

	// 5. Poll 2x more -- DC2 confirmed writable (recovery threshold = 2).
	h.pollN(2)

	// 6. Verify: DNS flipped to DC2 IP.
	if h.dns.getLastIP() != "2.2.2.2" {
		t.Errorf("DNS should point to dc2 (2.2.2.2), got %s", h.dns.getLastIP())
	}

	s = h.tm.Status()
	if s.DC2State != "writable" {
		t.Errorf("dc2 state after confirmation: got %s, want writable", s.DC2State)
	}
}

func TestFailover_DC2Down_PromoteDC1(t *testing.T) {
	// Mirror: DC2 is the primary, DC1 is the replica.
	h := newTestHarnessWithMySQL(t,
		&mockMySQL{readOnly: true},
		&mockMySQL{readOnly: false},
	)

	// Establish DC2 primary.
	h.pollN(2)

	s := h.tm.Status()
	if s.DC2State != "writable" {
		t.Fatalf("setup: dc2 state: got %s, want writable", s.DC2State)
	}

	// DC2 goes down.
	h.dc2MySQL.setError(errDown)

	// Failure threshold.
	h.pollN(3)

	if !h.tainter.isTainted("lion-dc2") {
		t.Error("dc2 should be tainted")
	}

	// DC1 should have been promoted.
	if h.dc1MySQL.isReadOnly() {
		t.Error("dc1 should have been promoted")
	}

	// Confirm promotion.
	h.pollN(2)

	if h.dns.getLastIP() != "1.1.1.1" {
		t.Errorf("DNS should point to dc1 (1.1.1.1), got %s", h.dns.getLastIP())
	}
}

func TestFailover_DNSDeferredUntilConfirmed(t *testing.T) {
	h := newTestHarness(t) // DC1 writable, DC2 read-only

	// Establish normal.
	h.pollN(2)

	// DC1 goes down.
	h.dc1MySQL.setError(errDown)

	// Reach failure threshold: failover triggers.
	h.pollN(3)

	// At this point, promotion happened (SetReadOnly(false) on DC2), but
	// DNS should NOT have flipped yet -- it waits for recovery threshold
	// confirmation that DC2 is actually writable.
	if h.dns.getLastIP() != "" {
		t.Error("DNS should be deferred until promotion confirmed")
	}

	// One more poll (1 of 2 recovery polls).
	h.pollN(1)
	if h.dns.getLastIP() != "" {
		t.Error("DNS should still be deferred after 1 recovery poll (need 2)")
	}

	// Second recovery poll -- now confirmed.
	h.pollN(1)
	if h.dns.getLastIP() != "2.2.2.2" {
		t.Errorf("DNS should flip after 2 recovery polls, got %s", h.dns.getLastIP())
	}
}

func TestFailover_AntiFlap(t *testing.T) {
	// Use a meaningful cooldown so we can test the anti-flap guard.
	h := newTestHarnessWithCooldown(t, 200*time.Millisecond)

	// Establish DC1 primary.
	h.pollN(2)

	// First failover: DC1 down.
	h.dc1MySQL.setError(errDown)
	h.pollN(3) // failure threshold

	// DC2 should have been promoted.
	if h.dc2MySQL.isReadOnly() {
		t.Fatal("first failover: dc2 should have been promoted")
	}

	// Confirm promotion so DNS flips.
	h.pollN(2)
	if h.dns.getLastIP() != "2.2.2.2" {
		t.Fatalf("first failover: DNS should point to dc2, got %s", h.dns.getLastIP())
	}

	// Now DC2 goes down too (while cooldown is still active).
	h.dc2MySQL.setError(errDown)
	h.dc1MySQL.setError(nil)
	h.dc1MySQL.setReadOnly(true) // DC1 comes back as read-only replica

	h.pollN(3) // reach failure threshold for DC2

	// The second failover should be BLOCKED by anti-flap cooldown.
	// DC1 should still be read-only (not promoted).
	if !h.dc1MySQL.isReadOnly() {
		t.Error("second failover should be blocked by anti-flap cooldown")
	}

	// DNS should still point to DC2 from the first failover.
	if h.dns.getLastIP() != "2.2.2.2" {
		t.Errorf("DNS should not change during cooldown, got %s", h.dns.getLastIP())
	}

	// Expire the cooldown by advancing the fake clock past the cooldown period.
	h.clock.Advance(2 * time.Hour)

	// After cooldown, cross-DC eval only fires on state transitions.
	// DC2 is already unreachable, DC1 is already read-only -- no transition.
	// We need to cause a state transition to trigger re-evaluation.
	// Temporarily make DC2 come back, then go down again.
	h.dc2MySQL.setReadOnly(true) // DC2 comes back as read-only
	h.pollN(1)                   // DC2 transitions from unreachable -> read-only

	h.dc2MySQL.setError(errDown) // DC2 goes down again
	h.pollN(3)                   // DC2 transitions from read-only -> unreachable

	// Now cooldown has expired and a state transition occurred.
	// DC1 should have been promoted.
	if h.dc1MySQL.isReadOnly() {
		t.Error("failover should be allowed after cooldown expires")
	}
}

func TestFailover_DNSFlipsOnlyOnce(t *testing.T) {
	h := newTestHarness(t)

	// Establish normal.
	h.pollN(2)

	// DC1 down, trigger failover.
	h.dc1MySQL.setError(errDown)
	h.pollN(3)

	// Confirm and let it settle.
	h.pollN(5)

	calls := h.dns.getCalls()
	if calls != 1 {
		t.Errorf("DNS should flip exactly once, got %d calls", calls)
	}
}
