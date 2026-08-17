package component

import (
	"context"
	"log/slog"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/shipstream/bloodraven/internal/clock"
	"github.com/shipstream/bloodraven/internal/controller"
	"github.com/shipstream/bloodraven/internal/mysql"
	"github.com/shipstream/bloodraven/internal/platform"
	"github.com/shipstream/bloodraven/internal/state"
)

// TestEncryptionSealRoll_UpdatesReaderWhenActiveHasNoStandby pins the
// v1.1.0 encryption E2E gate stall. After escrow, every site wants the
// sealed rendering. The new primary cannot hand off (only promotable
// standby is divergent) but the read-only reader is a healthy direct
// replica and must still be rolled. Before the fix, checkUpdate returned
// false and nobody sealed.
func TestEncryptionSealRoll_UpdatesReaderWhenActiveHasNoStandby(t *testing.T) {
	logger := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelWarn}))
	pdx := &mockMySQL{readOnly: false, gtidExecuted: "uuid:1-12"}
	iad := &mockMySQL{readOnly: true, gtidExecuted: "uuid:1-13", replicaStatus: &mysql.ReplicaStatus{}}
	reader := &mockMySQL{readOnly: true, gtidExecuted: "uuid:1-12", replicaStatus: &mysql.ReplicaStatus{
		IORunning: true, SQLRunning: true, SourceHost: "mysql-lion-pdx.default.svc.cluster.local",
	}}
	sites := []controller.SiteTopologyConfig{
		{Name: "pdx", Zone: "zone-pdx", LBIP: "10.0.0.2", Role: state.SiteRolePrimaryCandidate,
			Host: "mysql-lion-pdx.default.svc.cluster.local"},
		{Name: "iad", Zone: "zone-iad", LBIP: "10.0.0.1", Role: state.SiteRolePrimaryCandidate,
			Host: "mysql-lion-iad.default.svc.cluster.local"},
		{Name: "reader", Zone: "zone-reader", Role: state.SiteRoleReadOnly,
			Host: "mysql-lion-reader.default.svc.cluster.local"},
	}
	fc := controller.NewFailoverController(logger)
	updater := controller.NewUpdateController(fc, logger)
	clk := clock.NewFakeClock(time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC))
	tm := controller.NewTopologyManagerWithClock(
		controller.TopologyConfig{
			Name:              "playground",
			Sites:             sites,
			FailureThreshold:  3,
			RecoveryThreshold: 2,
		},
		[]mysql.Checker{pdx, iad, reader},
		fc, updater, nil,
		controller.BootstrapConfig{ReplUser: "repl", ReplPassword: "secret"},
		newMockTainter(), platform.NewHub(logger), &mockDNS{}, logger, clk,
	)

	var mu sync.Mutex
	var applied []string
	tm.ApplyUpdate = func(_ context.Context, site string) error {
		mu.Lock()
		applied = append(applied, site)
		mu.Unlock()
		return nil
	}

	for i := 0; i < 4; i++ {
		tm.Poll(context.Background())
	}

	tm.SetSpecDriftSites([]string{"pdx", "iad", "reader"})
	tm.Poll(context.Background())

	deadline := time.Now().Add(2 * time.Second)
	var got string
	for time.Now().Before(deadline) {
		mu.Lock()
		got = strings.Join(applied, ",")
		mu.Unlock()
		if got == "reader" {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("applied = %q, want reader only (active has no promotable standby)", got)
}
