package component

import (
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/shipstream/bloodraven/internal/metrics"
)

func TestFailover_DC1Down_PromoteDC2(t *testing.T) {
	h := newTestHarness(t) // dc1 writable, dc2 read-only

	// 1. Establish dc1 as primary, dc2 as replica (recovery threshold = 2 polls).
	h.pollN(2)

	s := h.tm.Status()
	if s.Sites[0].State != "writable" {
		t.Fatalf("setup: dc1 state: got %s, want writable", s.Sites[0].State)
	}
	if s.Sites[1].State != "read-only" {
		t.Fatalf("setup: dc2 state: got %s, want read-only", s.Sites[1].State)
	}

	// 2. dc1 goes down.
	h.dc1MySQL.setError(errDown)

	// 3. Poll 3x (failure threshold) -- dc1 becomes unreachable.
	h.pollN(3)

	s = h.tm.Status()
	if s.Sites[0].State != "unreachable" {
		t.Errorf("after failure: dc1 state: got %s, want unreachable", s.Sites[0].State)
	}

	// 4. Verify: dc1 is tainted.
	if !h.tainter.isTainted("shipstream.io/failover-group=lion,shipstream.io/site=dc1") {
		t.Error("dc1 should be tainted after going unreachable")
	}

	// FailoverController.Execute calls SetReadOnly(false) on the candidate,
	// so dc2 should now report readOnly=false.
	if h.dc2MySQL.isReadOnly() {
		t.Error("dc2 should have been promoted (readOnly should be false)")
	}

	// DNS should have flipped immediately at failover trigger (before promotion).
	if h.dns.getLastIP() != "2.2.2.2" {
		t.Errorf("DNS should flip at failover trigger, got %s", h.dns.getLastIP())
	}

	// 5. Poll 2x more -- dc2 confirmed writable (recovery threshold = 2).
	h.pollN(2)

	s = h.tm.Status()
	if s.Sites[1].State != "writable" {
		t.Errorf("dc2 state after confirmation: got %s, want writable", s.Sites[1].State)
	}
}

func TestFailover_DC2Down_PromoteDC1(t *testing.T) {
	// Mirror: dc2 is the primary, dc1 is the replica.
	h := newTestHarnessWithMySQL(t,
		&mockMySQL{readOnly: true},
		&mockMySQL{readOnly: false},
	)

	// Establish dc2 primary.
	h.pollN(2)

	s := h.tm.Status()
	if s.Sites[1].State != "writable" {
		t.Fatalf("setup: dc2 state: got %s, want writable", s.Sites[1].State)
	}

	// dc2 goes down.
	h.dc2MySQL.setError(errDown)

	// Failure threshold.
	h.pollN(3)

	if !h.tainter.isTainted("shipstream.io/failover-group=lion,shipstream.io/site=dc2") {
		t.Error("dc2 should be tainted")
	}

	// dc1 should have been promoted.
	if h.dc1MySQL.isReadOnly() {
		t.Error("dc1 should have been promoted")
	}

	// DNS should have flipped immediately at failover trigger.
	if h.dns.getLastIP() != "1.1.1.1" {
		t.Errorf("DNS should flip at failover trigger, got %s", h.dns.getLastIP())
	}
}

func TestFailover_DNSFlipsAtTrigger(t *testing.T) {
	h := newTestHarness(t) // DC1 writable, DC2 read-only

	// Establish normal.
	h.pollN(2)

	// DC1 goes down.
	h.dc1MySQL.setError(errDown)

	// Reach failure threshold: failover triggers — DNS flips immediately.
	h.pollN(3)

	if h.dns.getLastIP() != "2.2.2.2" {
		t.Errorf("DNS should flip at failover trigger, got %s", h.dns.getLastIP())
	}

	// Additional polls should NOT cause another DNS flip.
	h.pollN(5)
	if h.dns.getCalls() != 1 {
		t.Errorf("DNS should flip exactly once, got %d", h.dns.getCalls())
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

	// DNS should have flipped immediately at trigger time.
	if h.dns.getLastIP() != "2.2.2.2" {
		t.Fatalf("first failover: DNS should point to dc2, got %s", h.dns.getLastIP())
	}

	// Poll to confirm promotion.
	h.pollN(2)

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

func TestFailover_FailoversTotalMetric(t *testing.T) {
	h := newTestHarness(t) // dc1 writable, dc2 read-only

	before := testutil.ToFloat64(metrics.FailoversTotal.WithLabelValues("dc2"))

	// Establish normal.
	h.pollN(2)

	// DC1 down, trigger failover.
	h.dc1MySQL.setError(errDown)
	h.pollN(3)

	after := testutil.ToFloat64(metrics.FailoversTotal.WithLabelValues("dc2"))
	if after-before != 1 {
		t.Errorf("expected FailoversTotal for dc2 to increment by 1, got delta %v", after-before)
	}
}

func TestFailover_PromotionGtidInStatus(t *testing.T) {
	dc1 := &mockMySQL{readOnly: false, gtidExecuted: "uuid1:1-10"}
	dc2 := &mockMySQL{readOnly: true, gtidExecuted: "uuid1:1-10"}
	h := newTestHarnessWithMySQL(t, dc1, dc2)

	// Establish normal.
	h.pollN(2)

	// DC1 down, trigger failover.
	dc1.setError(errDown)
	h.pollN(3) // failover

	s := h.tm.Status()
	if s.PromotionGtidExecuted == "" {
		t.Error("expected PromotionGtidExecuted to be populated after failover")
	}
}
