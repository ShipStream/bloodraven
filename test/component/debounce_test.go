package component

import "testing"

func TestDebounce_SingleBlipNoFailover(t *testing.T) {
	h := newTestHarness(t) // DC1 writable, DC2 read-only

	// Establish DC1 as primary.
	h.pollN(2)

	// DC1 has 1 error, then recovers immediately.
	h.dc1MySQL.setError(errDown)
	h.pollN(1)

	// Recovery: DC1 comes back.
	h.dc1MySQL.setReadOnly(false)
	h.pollN(2) // recovery threshold

	// Should NOT have triggered failover.
	if h.dns.getLastIP() != "" {
		t.Error("single blip should not trigger DNS flip")
	}

	s := h.tm.Status()
	if s.DC1State != "writable" {
		t.Errorf("dc1 should recover to writable, got %s", s.DC1State)
	}
}

func TestDebounce_RecoveryRequiresMultiplePolls(t *testing.T) {
	// DC1 starts read-only, then becomes writable.
	h := newTestHarnessWithMySQL(t,
		&mockMySQL{readOnly: true},
		&mockMySQL{readOnly: false},
	)

	// Establish initial state.
	h.pollN(2)

	s := h.tm.Status()
	if s.DC1State != "read-only" {
		t.Fatalf("setup: dc1 should be read-only, got %s", s.DC1State)
	}

	// DC1 becomes writable.
	h.dc1MySQL.setReadOnly(false)

	// After 1 poll, DC1 should NOT yet be writable (recovery threshold = 2).
	h.pollN(1)
	s = h.tm.Status()
	if s.DC1State == "writable" {
		t.Error("dc1 should not be writable after only 1 recovery poll")
	}

	// After 2nd poll, DC1 should be writable.
	h.pollN(1)
	s = h.tm.Status()
	if s.DC1State != "writable" {
		t.Errorf("dc1 should be writable after 2 recovery polls, got %s", s.DC1State)
	}
}

func TestDebounce_FailureRollback(t *testing.T) {
	h := newTestHarness(t) // DC1 writable, DC2 read-only

	// Establish DC1 primary.
	h.pollN(2)

	// DC1 has 2 failures (under threshold of 3), then recovers.
	h.dc1MySQL.setError(errDown)
	h.pollN(2)

	// Verify no failover yet.
	s := h.tm.Status()
	if s.DC1State == "unreachable" {
		t.Error("dc1 should not be unreachable after only 2 failures (threshold is 3)")
	}

	// DC1 recovers.
	h.dc1MySQL.setReadOnly(false)
	h.pollN(2) // recovery threshold

	// Fail count should have reset; DC1 should be writable.
	s = h.tm.Status()
	if s.DC1State != "writable" {
		t.Errorf("dc1 should recover to writable, got %s", s.DC1State)
	}

	// No DNS flip should have occurred.
	if h.dns.getLastIP() != "" {
		t.Error("no DNS flip expected after sub-threshold failures")
	}
}

func TestDebounce_ThresholdExactlyMet(t *testing.T) {
	h := newTestHarness(t) // DC1 writable, DC2 read-only

	// Establish DC1 primary.
	h.pollN(2)

	// Exactly 3 failures (= threshold).
	h.dc1MySQL.setError(errDown)
	h.pollN(3)

	// DC1 should now be unreachable.
	s := h.tm.Status()
	if s.DC1State != "unreachable" {
		t.Errorf("dc1 should be unreachable after exactly 3 failures, got %s", s.DC1State)
	}

	// Failover should have been initiated.
	if h.dc2MySQL.isReadOnly() {
		t.Error("dc2 should have been promoted after threshold met")
	}
}

func TestDebounce_IntermittentErrors(t *testing.T) {
	h := newTestHarness(t) // DC1 writable, DC2 read-only

	// Establish DC1 primary.
	h.pollN(2)

	// Pattern: fail, recover, fail, recover -- never reaches threshold.
	for i := 0; i < 3; i++ {
		h.dc1MySQL.setError(errDown)
		h.pollN(1)
		h.dc1MySQL.setReadOnly(false) // clears error, sets writable
		h.pollN(1)
	}

	// No failover should have occurred.
	if h.dns.getLastIP() != "" {
		t.Error("intermittent errors should not trigger DNS flip")
	}

	// DC2 should still be read-only (not promoted).
	if !h.dc2MySQL.isReadOnly() {
		t.Error("dc2 should not have been promoted from intermittent errors")
	}
}
