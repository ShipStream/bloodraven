package component

import "testing"

func TestSplitBrain_BothWritable_NoAction(t *testing.T) {
	// Both DCs are writable -- split brain scenario.
	h := newTestHarnessWithMySQL(t,
		&mockMySQL{readOnly: false},
		&mockMySQL{readOnly: false},
	)

	h.pollN(2) // recovery threshold

	// Should NOT promote either DC.
	// The topology manager should recognize split brain and not flip DNS.
	if h.dns.getLastIP() != "" {
		t.Error("split brain should not trigger DNS flip")
	}

	// Both should be writable (split brain is detected but no automated fix).
	s := h.tm.Status()
	if s.Sites[0].State != "writable" {
		t.Errorf("dc1 state: got %s, want writable", s.Sites[0].State)
	}
	if s.Sites[1].State != "writable" {
		t.Errorf("dc2 state: got %s, want writable", s.Sites[1].State)
	}

	// Neither should be tainted (both are writable).
	if h.tainter.isTainted("shipstream.io/failover-group=lion,shipstream.io/site=dc1") {
		t.Error("dc1 should not be tainted (writable)")
	}
	if h.tainter.isTainted("shipstream.io/failover-group=lion,shipstream.io/site=dc2") {
		t.Error("dc2 should not be tainted (writable)")
	}
}

func TestDoubleReadOnly_NoPromotion(t *testing.T) {
	// Both DCs are read-only -- no primary exists.
	h := newTestHarnessWithMySQL(t,
		&mockMySQL{readOnly: true},
		&mockMySQL{readOnly: true},
	)

	h.pollN(2)

	// Both should be tainted.
	if !h.tainter.isTainted("shipstream.io/failover-group=lion,shipstream.io/site=dc1") {
		t.Error("dc1 should be tainted (read-only)")
	}
	if !h.tainter.isTainted("shipstream.io/failover-group=lion,shipstream.io/site=dc2") {
		t.Error("dc2 should be tainted (read-only)")
	}

	// No promotion, no DNS flip.
	if h.dns.getLastIP() != "" {
		t.Error("double read-only should not trigger DNS flip")
	}

	s := h.tm.Status()
	if s.Sites[0].State != "read-only" {
		t.Errorf("dc1 state: got %s, want read-only", s.Sites[0].State)
	}
	if s.Sites[1].State != "read-only" {
		t.Errorf("dc2 state: got %s, want read-only", s.Sites[1].State)
	}
}

func TestTotalLoss_BothUnreachable(t *testing.T) {
	// Both DCs are unreachable from the start.
	h := newTestHarnessWithMySQL(t,
		&mockMySQL{err: errDown},
		&mockMySQL{err: errDown},
	)

	// Reach failure threshold.
	h.pollN(3)

	// Both should be tainted.
	if !h.tainter.isTainted("shipstream.io/failover-group=lion,shipstream.io/site=dc1") {
		t.Error("dc1 should be tainted")
	}
	if !h.tainter.isTainted("shipstream.io/failover-group=lion,shipstream.io/site=dc2") {
		t.Error("dc2 should be tainted")
	}

	// No promotion -- conservative approach.
	if h.dns.getLastIP() != "" {
		t.Error("total loss should not trigger DNS flip")
	}

	s := h.tm.Status()
	if s.Sites[0].State != "unreachable" {
		t.Errorf("dc1 state: got %s, want unreachable", s.Sites[0].State)
	}
	if s.Sites[1].State != "unreachable" {
		t.Errorf("dc2 state: got %s, want unreachable", s.Sites[1].State)
	}
}

func TestSplitBrain_OneGoesDown(t *testing.T) {
	// Start with split brain, then one goes down. Should taint the down DC
	// but not promote (the remaining one is already writable).
	h := newTestHarnessWithMySQL(t,
		&mockMySQL{readOnly: false},
		&mockMySQL{readOnly: false},
	)

	h.pollN(2) // both writable

	// DC2 goes down.
	h.dc2MySQL.setError(errDown)
	h.pollN(3) // failure threshold

	// DC2 should be tainted.
	if !h.tainter.isTainted("shipstream.io/failover-group=lion,shipstream.io/site=dc2") {
		t.Error("dc2 should be tainted after going down")
	}

	s := h.tm.Status()
	if s.Sites[0].State != "writable" {
		t.Errorf("dc1 state: got %s, want writable", s.Sites[0].State)
	}
	if s.Sites[1].State != "unreachable" {
		t.Errorf("dc2 state: got %s, want unreachable", s.Sites[1].State)
	}
}
