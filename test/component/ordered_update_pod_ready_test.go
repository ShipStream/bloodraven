package component

import (
	"context"
	"errors"
	"log/slog"
	"os"
	"slices"
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

// Issue #209: an OrderedUpdate rolls iad onto a template whose sidecar
// cannot start. iad's MySQL keeps answering through the -internal Service
// (it publishes not-ready addresses) and keeps replicating, but its pod is
// never Ready, so it is out of every app Service and has no self-fencing.
// The rollout wait gives up (ReconcileSiteDeployment's 5 minutes), and the
// runner's drift check then finds iad's Deployment on the new hash, so only
// pdx is left drifted. The next attempt used to hand pdx's role to iad and
// roll pdx into the same state, leaving no Ready site at all. It must stop
// on iad's pod instead, with pdx serving and untouched.
func TestOrderedUpdate_DoesNotWalkTheGroupWhenRolledPodIsNotReady(t *testing.T) {
	logger := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelError}))
	pdxHost := "mysql-lion-pdx.default.svc.cluster.local"
	iadHost := "mysql-lion-iad.default.svc.cluster.local"
	pdx := &mockMySQL{readOnly: false, gtidExecuted: handoffFenced}
	iad := &mockMySQL{readOnly: true, superReadOnly: true, gtidExecuted: handoffFenced,
		replicaStatus: &mysql.ReplicaStatus{IORunning: true, SQLRunning: true, SourceHost: pdxHost}}
	sites := []controller.SiteTopologyConfig{
		{Name: "pdx", Zone: "zone-pdx", LoadBalancerIP: "10.0.0.2", Role: state.SiteRolePrimaryCandidate, Host: pdxHost},
		{Name: "iad", Zone: "zone-iad", LoadBalancerIP: "10.0.0.1", Role: state.SiteRolePrimaryCandidate, Host: iadHost},
	}
	clk := clock.NewFakeClock(time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC))
	fc := controller.NewFailoverController(logger)
	uc := controller.NewUpdateController(fc, logger)
	tm := controller.NewTopologyManagerWithClock(
		controller.TopologyConfig{Name: "lion", Sites: sites, FailureThreshold: 3, RecoveryThreshold: 2},
		[]mysql.Checker{pdx, iad}, fc, uc, nil,
		controller.BootstrapConfig{ReplUser: "repl", ReplPassword: "secret"},
		newMockTainter(), platform.NewHub(logger), &mockDNS{}, logger, clk,
	)

	var mu sync.Mutex
	var applied []string
	rolled := map[string]bool{}
	probes := map[string]int{}
	var stalls []string
	tm.ApplyUpdate = func(_ context.Context, site string) error {
		mu.Lock()
		defer mu.Unlock()
		applied = append(applied, site)
		rolled[site] = true
		return &controller.RolloutIncompleteError{Site: site,
			Err: errors.New("timeout waiting for deployment default/mysql-lion-" + site + " rollout: context deadline exceeded")}
	}
	tm.PodReady = func(_ context.Context, site string) (controller.PodReadiness, error) {
		mu.Lock()
		defer mu.Unlock()
		probes[site]++
		pod := "mysql-lion-" + site + "-1"
		if rolled[site] {
			return controller.PodReadiness{Pod: pod, Reason: "sidecar not ready: crash looping, last exit code 1 (Error)"}, nil
		}
		return controller.PodReadiness{Ready: true, Pod: pod}, nil
	}
	tm.StatusCallback = func(s controller.TopologySnapshot) {
		mu.Lock()
		defer mu.Unlock()
		if s.UpdateStalled != "" && (len(stalls) == 0 || stalls[len(stalls)-1] != s.UpdateStalled) {
			stalls = append(stalls, s.UpdateStalled)
		}
	}
	iadProbes := func() int {
		mu.Lock()
		defer mu.Unlock()
		return probes["iad"]
	}
	pdxRolled := func() bool {
		mu.Lock()
		defer mu.Unlock()
		return rolled["pdx"]
	}

	for i := 0; i < 4; i++ {
		tm.Poll(context.Background())
	}
	if s := tm.Status(); s.ActiveSite != "pdx" {
		t.Fatalf("setup: active site = %q, want pdx", s.ActiveSite)
	}

	// Attempt 1: iad is rolled and its rollout never completes.
	tm.SetSpecDriftSites([]string{"pdx", "iad"})
	tm.Poll(context.Background())
	deadline := time.Now().Add(10 * time.Second)
	for uc.IsUpdating() || !slices.Equal(tm.SpecDriftSites(), []string{"pdx"}) {
		if time.Now().After(deadline) {
			t.Fatalf("drift after attempt 1 = %v, want pdx only (iad's Deployment has the new template)", tm.SpecDriftSites())
		}
		time.Sleep(time.Millisecond)
	}

	// Later attempts, after the stall back-off, see iad as an updated,
	// healthy MySQL replica: exactly the standby the old code handed off
	// to. A poll that lands while the previous attempt is still finishing
	// starts nothing, so count the attempts that probed iad.
	attempts, last := 0, iadProbes()
	for attempts < 3 && !pdxRolled() {
		if time.Now().After(deadline) {
			t.Fatalf("only %d retries ran", attempts)
		}
		clk.Advance(31 * time.Second)
		tm.Poll(context.Background())
		time.Sleep(5 * time.Millisecond)
		if n := iadProbes(); n > last {
			attempts, last = attempts+1, n
		}
	}
	for uc.IsUpdating() {
		time.Sleep(time.Millisecond)
	}

	mu.Lock()
	defer mu.Unlock()
	if slices.Contains(applied, "pdx") {
		t.Fatalf("applied = %v: pdx was rolled while iad's pod is not Ready; no site is Ready", applied)
	}
	if !iad.isReadOnly() || iad.isPromoted() {
		t.Fatal("the active site was handed to iad, whose pod is not Ready")
	}
	if pdx.isReadOnly() || pdx.isSuperReadOnly() {
		t.Fatal("pdx was fenced; it is the only Ready site and must keep serving")
	}
	if s := tm.Status(); s.ActiveSite != "pdx" {
		t.Fatalf("active site = %q, want pdx", s.ActiveSite)
	}
	want := []string{
		"site iad was updated but its pod mysql-lion-iad-1 is not Ready (sidecar not ready: crash looping, last exit code 1 (Error))",
		"site iad already runs the new pod template but its pod mysql-lion-iad-1 is not Ready (sidecar not ready: crash looping, last exit code 1 (Error)); not rolling another site",
	}
	if strings.Join(stalls, "\n") != strings.Join(want, "\n") {
		t.Fatalf("reported stalls:\n%s\nwant:\n%s", strings.Join(stalls, "\n"), strings.Join(want, "\n"))
	}
}
