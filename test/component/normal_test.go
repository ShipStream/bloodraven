package component

import "testing"

func TestNormalOperation_DC1Primary(t *testing.T) {
	// Site 0 (dc1) writable (read_only=0), Site 1 (dc2) read-only (read_only=1).
	h := newTestHarnessWithMySQL(t,
		&mockMySQL{readOnly: false},
		&mockMySQL{readOnly: true},
	)

	// Poll enough times for recovery threshold to confirm dc1 writable.
	h.pollN(2)

	s := h.tm.Status()
	if s.Sites[0].State != "writable" {
		t.Errorf("dc1 state: got %s, want writable", s.Sites[0].State)
	}
	if s.Sites[1].State != "read-only" {
		t.Errorf("dc2 state: got %s, want read-only", s.Sites[1].State)
	}

	// dc1 should NOT be tainted (it is the primary).
	if h.tainter.isTainted("shipstream.io/failover-group=lion,shipstream.io/site=dc1") {
		t.Error("dc1 should not be tainted when writable")
	}
	// dc2 should be tainted (read-only replica).
	if !h.tainter.isTainted("shipstream.io/failover-group=lion,shipstream.io/site=dc2") {
		t.Error("dc2 should be tainted when read-only")
	}

	// No DNS flip should have occurred.
	if h.dns.getLastIP() != "" {
		t.Errorf("no DNS flip expected, got ip=%s", h.dns.getLastIP())
	}
	// No promotion should have occurred.
	if h.dc1MySQL.isPromoted() || h.dc2MySQL.isPromoted() {
		t.Error("no promotion expected in normal steady state")
	}
}

func TestNormalOperation_DC2Primary(t *testing.T) {
	// Mirror: dc2 writable, dc1 read-only.
	h := newTestHarnessWithMySQL(t,
		&mockMySQL{readOnly: true},
		&mockMySQL{readOnly: false},
	)

	h.pollN(2)

	s := h.tm.Status()
	if s.Sites[0].State != "read-only" {
		t.Errorf("dc1 state: got %s, want read-only", s.Sites[0].State)
	}
	if s.Sites[1].State != "writable" {
		t.Errorf("dc2 state: got %s, want writable", s.Sites[1].State)
	}

	// dc1 tainted, dc2 not tainted.
	if !h.tainter.isTainted("shipstream.io/failover-group=lion,shipstream.io/site=dc1") {
		t.Error("dc1 should be tainted when read-only")
	}
	if h.tainter.isTainted("shipstream.io/failover-group=lion,shipstream.io/site=dc2") {
		t.Error("dc2 should not be tainted when writable")
	}

	// No DNS flip, no promotion.
	if h.dns.getLastIP() != "" {
		t.Error("no DNS flip expected")
	}
}

func TestNormalOperation_ReadyAfterFirstPoll(t *testing.T) {
	h := newTestHarness(t)

	if h.tm.Ready() {
		t.Error("should not be ready before first poll")
	}

	h.pollN(1)

	if !h.tm.Ready() {
		t.Error("should be ready after first poll")
	}
}
