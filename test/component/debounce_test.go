package component

import (
	"testing"

	v1alpha1 "github.com/shipstream/bloodraven/api/v1alpha1"
	"github.com/shipstream/bloodraven/internal/controller"
)

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
	if s.Sites[0].State != v1alpha1.SiteStateWritable {
		t.Errorf("dc1 should recover to writable, got %s", s.Sites[0].State)
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
	if s.Sites[0].State != v1alpha1.SiteStateReadOnly {
		t.Fatalf("setup: dc1 should be read-only, got %s", s.Sites[0].State)
	}

	// DC1 becomes writable.
	h.dc1MySQL.setReadOnly(false)

	// After 1 poll, DC1 should NOT yet be writable (recovery threshold = 2).
	h.pollN(1)
	s = h.tm.Status()
	if s.Sites[0].State == v1alpha1.SiteStateWritable {
		t.Error("dc1 should not be writable after only 1 recovery poll")
	}

	// After 2nd poll, DC1 should be writable.
	h.pollN(1)
	s = h.tm.Status()
	if s.Sites[0].State != v1alpha1.SiteStateWritable {
		t.Errorf("dc1 should be writable after 2 recovery polls, got %s", s.Sites[0].State)
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
	if s.Sites[0].State == v1alpha1.SiteStateUnreachable {
		t.Error("dc1 should not be unreachable after only 2 failures (threshold is 3)")
	}

	// DC1 recovers.
	h.dc1MySQL.setReadOnly(false)
	h.pollN(2) // recovery threshold

	// Fail count should have reset; DC1 should be writable.
	s = h.tm.Status()
	if s.Sites[0].State != v1alpha1.SiteStateWritable {
		t.Errorf("dc1 should recover to writable, got %s", s.Sites[0].State)
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
	if s.Sites[0].State != v1alpha1.SiteStateUnreachable {
		t.Errorf("dc1 should be unreachable after exactly 3 failures, got %s", s.Sites[0].State)
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

// Issue #216: the first poll after an operator start sees the primary's
// writable answer before the recovery threshold confirms it. That is an
// unclassified topology, not a missing primary, and it must not be acted on.
func TestDebounce_FirstPollAfterStartIsInitializing(t *testing.T) {
	h := newTestHarness(t) // DC1 writable, DC2 read-only
	var snap controller.TopologySnapshot
	h.tm.StatusCallback = func(s controller.TopologySnapshot) { snap = s }

	h.pollN(1)
	if snap.DegradedReason != "Initializing" || snap.Alert != "" || snap.ActiveSite != "" {
		t.Fatalf("first poll: reason=%q alert=%q active=%q, want Initializing with no alert or active site",
			snap.DegradedReason, snap.Alert, snap.ActiveSite)
	}
	if h.dns.getCalls() != 0 || !h.dc2MySQL.isReadOnly() {
		t.Fatal("first poll acted on an unconfirmed writable answer")
	}

	h.pollN(1)
	if snap.DegradedReason != "Healthy" || snap.ActiveSite != "dc1" {
		t.Fatalf("second poll: reason=%q active=%q, want Healthy with dc1 active", snap.DegradedReason, snap.ActiveSite)
	}
}
