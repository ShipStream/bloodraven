package component

import (
	"testing"

	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/shipstream/bloodraven/internal/metrics"
)

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

func TestSplitBrain_PreferSite_FencesLoserAndPromotesWinner(t *testing.T) {
	// Both sites writable, preferSite=dc1 → dc2 should be fenced and dc1
	// re-promoted via the standard failover path.
	before := testutil.ToFloat64(metrics.SplitBrainAutoResolveTotal.WithLabelValues("dc1"))
	h := newTestHarnessWithPriorities(t, []string{"dc1"})

	h.pollN(2) // recovery threshold — transition both sites to writable

	if !h.dc2MySQL.superReadOnly {
		t.Error("dc2 should have been fenced (super_read_only=ON) by preferSite policy")
	}
	if h.dc1MySQL.isReadOnly() {
		t.Error("dc1 should have been promoted (readOnly=false)")
	}
	if h.dns.getLastIP() != "1.1.1.1" {
		t.Errorf("DNS should point to preferred site, got %q", h.dns.getLastIP())
	}
	if got := testutil.ToFloat64(metrics.SplitBrainAutoResolveTotal.WithLabelValues("dc1")); got-before != 1 {
		t.Errorf("bloodraven_split_brain_auto_resolve_total{prefer_site=dc1}: got delta %v, want 1", got-before)
	}
}

func TestSplitBrain_PreferSite_DC2Wins(t *testing.T) {
	// Mirror: preferSite=dc2 should fence dc1 and promote dc2.
	h := newTestHarnessWithPriorities(t, []string{"dc2"})

	h.pollN(2)

	if !h.dc1MySQL.superReadOnly {
		t.Error("dc1 should have been fenced (super_read_only=ON) by priority policy")
	}
	if h.dc2MySQL.isReadOnly() {
		t.Error("dc2 should have been promoted (readOnly=false)")
	}
	if h.dns.getLastIP() != "2.2.2.2" {
		t.Errorf("DNS should point to preferred site, got %q", h.dns.getLastIP())
	}
}

func TestSplitBrain_NoPreferSite_NoAction(t *testing.T) {
	// Empty preferSite (manual mode) should retain the existing alert-only
	// behavior: no fencing, no DNS flip.
	h := newTestHarnessWithPriorities(t, nil)

	h.pollN(2)

	if h.dc1MySQL.superReadOnly || h.dc2MySQL.superReadOnly {
		t.Error("manual policy (empty preferSite) must not fence either site")
	}
	if h.dns.getLastIP() != "" {
		t.Errorf("manual policy must not flip DNS, got %q", h.dns.getLastIP())
	}
}

func TestSplitBrain_PreferSite_UnknownName_NoAction(t *testing.T) {
	// If preferSite doesn't match a real site (should be caught by CRD
	// validation, but defend at runtime too), fall back to manual behavior.
	h := newTestHarnessWithPriorities(t, []string{"dc-ghost"})

	h.pollN(2)

	if h.dc1MySQL.superReadOnly || h.dc2MySQL.superReadOnly {
		t.Error("unknown preferSite must not fence either site")
	}
	if h.dns.getLastIP() != "" {
		t.Errorf("unknown preferSite must not flip DNS, got %q", h.dns.getLastIP())
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
