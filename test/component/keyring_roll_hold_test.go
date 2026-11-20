package component

import (
	"context"
	"log/slog"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/shipstream/bloodraven/internal/clock"
	"github.com/shipstream/bloodraven/internal/controller"
	"github.com/shipstream/bloodraven/internal/mysql"
	"github.com/shipstream/bloodraven/internal/platform"
	"github.com/shipstream/bloodraven/internal/state"
)

// TestKeyringRollHold_CloneRecipientWaitsForItsEscrow pins the v1.8.0
// clone-recipient incident at the topology manager: the follower has spec
// drift and replicates healthily, which is every condition the ordered
// update had for rolling it, but its memory-backed keyring holds keys its
// new pod would not get. The update must neither roll it nor fence or hand
// off the active site while the check refuses, must not retry at poll
// frequency, and must roll the follower once the check passes.
func TestKeyringRollHold_CloneRecipientWaitsForItsEscrow(t *testing.T) {
	logger := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelWarn}))
	primary := &mockMySQL{readOnly: false, gtidExecuted: "uuid:1-10"}
	follower := &mockMySQL{readOnly: true, gtidExecuted: "uuid:1-10", replicaStatus: &mysql.ReplicaStatus{
		IORunning: true, SQLRunning: true, SourceHost: "mysql-lion-dc1.default.svc.cluster.local",
	}}
	sites := []controller.SiteTopologyConfig{
		{Name: "dc1", Zone: "zone-dc1", LoadBalancerIP: "10.0.0.1", Role: state.SiteRolePrimaryCandidate,
			Host: "mysql-lion-dc1.default.svc.cluster.local"},
		{Name: "dc2", Zone: "zone-dc2", LoadBalancerIP: "10.0.0.2", Role: state.SiteRolePrimaryCandidate,
			Host: "mysql-lion-dc2.default.svc.cluster.local"},
	}
	fc := controller.NewFailoverController(logger)
	clk := clock.NewFakeClock(time.Date(2026, 11, 20, 11, 4, 15, 0, time.UTC))
	tm := controller.NewTopologyManagerWithClock(
		controller.TopologyConfig{Name: "main", Sites: sites, FailureThreshold: 3, RecoveryThreshold: 2},
		[]mysql.Checker{primary, follower}, fc, controller.NewUpdateController(fc, logger), nil,
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
	var escrowed atomic.Bool
	var checks atomic.Int32
	tm.KeyringRollRefusal = func(_ context.Context, site string) error {
		checks.Add(1)
		if site == "dc2" && !escrowed.Load() {
			return &controller.KeyringRollRefusedError{Site: site, Reason: "its running pod holds keyring keys that are not in escrow v2"}
		}
		return nil
	}
	appliedSites := func() string {
		mu.Lock()
		defer mu.Unlock()
		return strings.Join(applied, ",")
	}
	settle := tm.WaitOrderedUpdateIdleForSim

	for i := 0; i < 4; i++ {
		tm.Poll(context.Background())
	}
	tm.SetSpecDriftSites([]string{"dc2"})
	for i := 0; i < 5; i++ {
		tm.Poll(context.Background())
		settle()
		clk.Advance(2 * time.Second)
	}
	if got := appliedSites(); got != "" {
		t.Fatalf("applied %q while dc2's keyring holds keys its new pod would lack", got)
	}
	if n := checks.Load(); n == 0 || n > 2 {
		t.Fatalf("keyring roll checked %d times over 10s of polls, want the hold to back off", n)
	}
	if primary.superReadOnly || primary.readOnly || follower.promoted {
		t.Fatal("MySQL authority was touched while the update was held")
	}

	escrowed.Store(true)
	clk.Advance(time.Minute)
	deadline := time.Now().Add(2 * time.Second)
	for appliedSites() != "dc2" {
		if time.Now().After(deadline) {
			t.Fatalf("applied %q after the escrow was recorded, want dc2", appliedSites())
		}
		tm.Poll(context.Background())
		settle()
		clk.Advance(2 * time.Second)
	}
}
