package component

import (
	"context"
	"log/slog"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/shipstream/bloodraven/internal/clock"
	"github.com/shipstream/bloodraven/internal/controller"
	"github.com/shipstream/bloodraven/internal/metrics"
	"github.com/shipstream/bloodraven/internal/mysql"
	"github.com/shipstream/bloodraven/internal/platform"
	"github.com/shipstream/bloodraven/internal/state"
)

func TestKeyringPromotion_EmergencyRefuseWhenOnlyCandidateRotating(t *testing.T) {
	h := newTestHarness(t)
	var last controller.TopologySnapshot
	h.tm.StatusCallback = func(snap controller.TopologySnapshot) { last = snap }
	h.tm.SetKeyringRotationBlocked([]string{"dc2"})

	h.pollN(2)
	h.dc1MySQL.setError(errDown)
	beforeFailovers := testutil.ToFloat64(metrics.FailoversTotal.WithLabelValues("dc2"))
	beforeRefused := testutil.ToFloat64(metrics.KeyringPromotionsBlockedTotal.WithLabelValues("", "lion", "dc2", "refused"))

	h.pollN(3)

	if !h.dc2MySQL.isReadOnly() {
		t.Fatal("dc2 must not be promoted while UnsealReason=Rotation")
	}
	if n := h.dc2MySQL.writableGrants(); n != 0 {
		t.Fatalf("dc2 writable grants = %d, want 0", n)
	}
	if h.dns.getLastIP() == "2.2.2.2" {
		t.Fatal("DNS must not flip to the rotating replica")
	}
	if delta := testutil.ToFloat64(metrics.FailoversTotal.WithLabelValues("dc2")) - beforeFailovers; delta != 0 {
		t.Fatalf("FailoversTotal delta = %v, want 0", delta)
	}
	if delta := testutil.ToFloat64(metrics.KeyringPromotionsBlockedTotal.WithLabelValues("", "lion", "dc2", "refused")) - beforeRefused; delta != 1 {
		t.Fatalf("refused metric delta = %v, want 1", delta)
	}
	if !strings.Contains(last.Alert, "UnsealReason=Rotation") || !strings.Contains(last.Alert, "dc2") {
		t.Fatalf("alert = %q, want rotation refusal naming dc2", last.Alert)
	}
	if last.DegradedReason != "NoPrimary" {
		t.Fatalf("DegradedReason = %q, want NoPrimary", last.DegradedReason)
	}
}

func TestKeyringPromotion_EmergencyHealsWhenRotationFinishes(t *testing.T) {
	h := newTestHarness(t)
	h.tm.SetKeyringRotationBlocked([]string{"dc2"})
	h.pollN(2)
	h.dc1MySQL.setError(errDown)
	h.pollN(3)
	if !h.dc2MySQL.isReadOnly() {
		t.Fatal("setup: dc2 should still be read-only")
	}

	h.tm.SetKeyringRotationBlocked(nil)
	h.pollN(2)

	if h.dc2MySQL.isReadOnly() {
		t.Fatal("dc2 must be promoted once the rotation block clears, without a MySQL state flap")
	}
	if h.dns.getLastIP() != "2.2.2.2" {
		t.Fatalf("DNS = %q, want 2.2.2.2", h.dns.getLastIP())
	}
}

func TestKeyringPromotion_EmergencySkipsRotatingSiteWhenAnotherRemains(t *testing.T) {
	dc1 := &mockMySQL{readOnly: false, gtidExecuted: "uuid:1-10"}
	dc2 := &mockMySQL{readOnly: true, gtidExecuted: "uuid:1-20"}
	dc3 := &mockMySQL{readOnly: true, gtidExecuted: "uuid:1-5"}
	h := newThreeSiteHarness(t, dc1, dc2, dc3)
	h.tm.SetKeyringRotationBlocked([]string{"dc2"})

	h.pollN(2)
	dc1.setError(errDown)
	beforeSkipped := testutil.ToFloat64(metrics.KeyringPromotionsBlockedTotal.WithLabelValues("", "lion", "dc2", "skipped"))

	h.pollN(3)

	if !dc2.isReadOnly() {
		t.Fatal("rotating dc2 must not be promoted even though it is GTID-ahead")
	}
	if n := dc2.writableGrants(); n != 0 {
		t.Fatalf("dc2 writable grants = %d, want 0", n)
	}
	if dc3.isReadOnly() {
		t.Fatal("sealed dc3 must be promoted")
	}
	if h.dns.getLastIP() != "3.3.3.3" {
		t.Fatalf("DNS = %q, want 3.3.3.3", h.dns.getLastIP())
	}
	if delta := testutil.ToFloat64(metrics.KeyringPromotionsBlockedTotal.WithLabelValues("", "lion", "dc2", "skipped")) - beforeSkipped; delta != 1 {
		t.Fatalf("skipped metric delta = %v, want 1", delta)
	}
}

func TestKeyringPromotion_EmergencyHealsAfterCooldown(t *testing.T) {
	h := newTestHarness(t)
	h.tm.SetKeyringRotationBlocked([]string{"dc2"})
	h.pollN(2)
	h.dc1MySQL.setError(errDown)
	h.pollN(3)
	if !h.dc2MySQL.isReadOnly() {
		t.Fatal("setup: dc2 should still be read-only")
	}

	h.tm.SetLastFailoverForTest(h.clock.Now())
	h.tm.SetKeyringRotationBlocked(nil)
	h.pollN(2)
	if !h.dc2MySQL.isReadOnly() {
		t.Fatal("dc2 must stay read-only while anti-flap cooldown is active")
	}

	h.clock.Advance(5 * time.Minute)
	h.pollN(2)
	if h.dc2MySQL.isReadOnly() {
		t.Fatal("dc2 must be promoted after cooldown without a MySQL state flap")
	}
}

func TestKeyringPromotion_SplitBrainWinnerBlockedDoesNotFenceLosers(t *testing.T) {
	h := newTestHarnessWithPriorities(t, []string{"dc2", "dc1"})
	h.tm.SetKeyringRotationBlocked([]string{"dc2"})
	h.pollN(5)
	if h.dc1MySQL.isReadOnly() {
		t.Fatal("priority loser must not be fenced when the winner is mid-rotation")
	}
	if n := h.dc1MySQL.writableGrants(); n != 0 {
		t.Fatalf("dc1 must not have been re-granted writability; grants=%d", n)
	}
	if h.dc2MySQL.isReadOnly() {
		t.Fatal("rotating winner must remain writable; fencing it would leave no primary")
	}
}

func TestKeyringPromotion_SealedReplicaStillPromotes(t *testing.T) {
	h := newTestHarness(t)
	h.tm.SetKeyringRotationBlocked(nil)
	h.pollN(2)
	h.dc1MySQL.setError(errDown)
	h.pollN(3)
	if h.dc2MySQL.isReadOnly() {
		t.Fatal("sealed replica must still promote")
	}
}

func TestKeyringPromotion_PlannedPromoteRefusesRotatingTarget(t *testing.T) {
	h := newTestHarness(t)
	h.pollN(2)
	h.tm.SetKeyringRotationBlocked([]string{"dc2"})
	_, err := h.tm.PlannedPromote(context.Background(), "dc2", "dc1")
	if err == nil || !strings.Contains(err.Error(), "mid-keyring-rotation") {
		t.Fatalf("PlannedPromote err = %v, want mid-keyring-rotation", err)
	}
	if !h.dc2MySQL.isReadOnly() {
		t.Fatal("planned promote must not make the rotating target writable")
	}
}

func TestKeyringPromotion_PlannedPromoteAllowsSealedTarget(t *testing.T) {
	h := newTestHarness(t)
	h.pollN(2)
	if _, err := h.tm.PlannedPromote(context.Background(), "dc2", "dc1"); err != nil {
		t.Fatalf("PlannedPromote sealed target: %v", err)
	}
	if h.dc2MySQL.isReadOnly() {
		t.Fatal("sealed planned-promote target should be writable")
	}
}

type threeSiteHarness struct {
	tm       *controller.TopologyManager
	dc1MySQL *mockMySQL
	dc2MySQL *mockMySQL
	dc3MySQL *mockMySQL
	dns      *mockDNS
}

func (h *threeSiteHarness) pollN(n int) {
	for i := 0; i < n; i++ {
		h.tm.Poll(context.Background())
	}
}

func newThreeSiteHarness(t *testing.T, dc1, dc2, dc3 *mockMySQL) *threeSiteHarness {
	t.Helper()
	logger := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelWarn}))
	tainter := newMockTainter()
	hub := platform.NewHub(logger)
	dns := &mockDNS{}
	fc := controller.NewFailoverController(logger)
	clk := clock.NewFakeClock(time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC))
	sites := defaultTwoSiteConfig()
	sites = append(sites, controller.SiteTopologyConfig{
		Name: "dc3", Zone: "lion-dc3", LBIP: "3.3.3.3", Role: state.SiteRolePrimaryCandidate,
		TaintSelector: taintSelector("dc3"), Host: "mysql-lion-dc3.default.svc.cluster.local",
	})
	cfg := controller.TopologyConfig{
		Name: "lion", Sites: sites, PollInterval: int64(50 * time.Millisecond),
		FailureThreshold: 3, RecoveryThreshold: 2, FailoverCooldown: 0,
	}
	tm := controller.NewTopologyManagerWithClock(cfg, []mysql.Checker{dc1, dc2, dc3}, fc, nil, nil, controller.BootstrapConfig{}, tainter, hub, dns, logger, clk)
	return &threeSiteHarness{tm: tm, dc1MySQL: dc1, dc2MySQL: dc2, dc3MySQL: dc3, dns: dns}
}
