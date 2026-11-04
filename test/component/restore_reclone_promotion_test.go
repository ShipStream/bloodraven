package component

import (
	"context"
	"strings"
	"testing"

	"github.com/shipstream/bloodraven/internal/controller"
)

// A peer an in-place restore has not recloned yet holds pre-restore data.
// When it is the only promotion candidate left, the group must not fail
// over to it automatically, and it must say why.
func TestRestoreReclonePending_EmergencyRefusesOnlyCandidate(t *testing.T) {
	h := newTestHarness(t)
	var last controller.TopologySnapshot
	h.tm.StatusCallback = func(snap controller.TopologySnapshot) { last = snap }
	h.tm.SetRestoreReclonePending([]string{"dc2"})

	h.pollN(2)
	h.dc1MySQL.setError(errDown)
	h.pollN(3)

	if !h.dc2MySQL.isReadOnly() || h.dc2MySQL.isPromoted() {
		t.Fatal("dc2 must not be promoted while it awaits its post-restore reclone")
	}
	if h.dns.getLastIP() == "2.2.2.2" {
		t.Fatal("DNS must not flip to the not-yet-recloned replica")
	}
	if last.DegradedReason != "NoPrimary" || !strings.Contains(last.Alert, "pending post-restore reclone") || !strings.Contains(last.Alert, "dc2") {
		t.Fatalf("alert = %q (%s), want a NoPrimary pending-reclone refusal naming dc2", last.Alert, last.DegradedReason)
	}
	if len(last.RestoreReclonePromotionRefused) != 1 || last.RestoreReclonePromotionRefused[0] != "dc2" {
		t.Fatalf("RestoreReclonePromotionRefused = %v, want [dc2]", last.RestoreReclonePromotionRefused)
	}
}

// Once the reclone finishes the site leaves the pending set and the
// stalled failover proceeds without needing a new MySQL transition.
func TestRestoreReclonePending_EmergencyHealsWhenRecloned(t *testing.T) {
	h := newTestHarness(t)
	var last controller.TopologySnapshot
	h.tm.StatusCallback = func(snap controller.TopologySnapshot) { last = snap }
	h.tm.SetRestoreReclonePending([]string{"dc2"})
	h.pollN(2)
	h.dc1MySQL.setError(errDown)
	h.pollN(3)
	if !h.dc2MySQL.isReadOnly() {
		t.Fatal("setup: dc2 should still be read-only")
	}

	h.tm.SetRestoreReclonePending(nil)
	h.pollN(2)
	if h.dc2MySQL.isReadOnly() {
		t.Fatal("dc2 must be promoted once it is no longer pending")
	}
	if len(last.RestoreReclonePromotionRefused) != 0 {
		t.Fatalf("refusal must clear after the promotion, got %v", last.RestoreReclonePromotionRefused)
	}
}

// With a recloned candidate available, automatic failover picks it even
// when the pending one is further ahead on GTIDs.
func TestRestoreReclonePending_EmergencySkipsPendingSiteWhenAnotherRemains(t *testing.T) {
	dc1 := &mockMySQL{readOnly: false, gtidExecuted: "uuid:1-10"}
	dc2 := &mockMySQL{readOnly: true, gtidExecuted: "uuid:1-20"}
	dc3 := &mockMySQL{readOnly: true, gtidExecuted: "uuid:1-5"}
	h := newThreeSiteHarness(t, dc1, dc2, dc3)
	h.tm.SetRestoreReclonePending([]string{"dc2"})

	h.pollN(2)
	dc1.setError(errDown)
	h.pollN(3)

	if !dc2.isReadOnly() {
		t.Fatal("pending dc2 must not be promoted even though it is GTID-ahead")
	}
	if dc3.isReadOnly() {
		t.Fatal("recloned dc3 must be promoted")
	}
	if h.dns.getLastIP() != "3.3.3.3" {
		t.Fatalf("DNS = %q, want 3.3.3.3", h.dns.getLastIP())
	}
}

// A split-brain resolution must not hand authority to a site that still
// holds pre-restore data.
func TestRestoreReclonePending_SplitBrainWinnerNotPromoted(t *testing.T) {
	h := newTestHarnessWithPriorities(t, []string{"dc2", "dc1"})
	h.tm.SetRestoreReclonePending([]string{"dc2"})
	h.pollN(5)
	if h.dc1MySQL.isReadOnly() {
		t.Fatal("priority loser must not be fenced when the winner awaits its reclone")
	}
}

// kubectl bloodraven promote (PlannedPromote) is the explicit override
// and is not blocked by a pending reclone.
func TestRestoreReclonePending_PlannedPromoteOverrides(t *testing.T) {
	h := newTestHarness(t)
	h.pollN(2)
	h.tm.SetRestoreReclonePending([]string{"dc2"})
	if _, err := h.tm.PlannedPromote(context.Background(), "dc2", "dc1"); err != nil {
		t.Fatalf("PlannedPromote of a pending site: %v", err)
	}
	if h.dc2MySQL.isReadOnly() {
		t.Fatal("explicitly promoted site should be writable")
	}
}
