package component

import (
	"context"
	"log/slog"
	"os"
	"slices"
	"sync"
	"testing"
	"time"

	"github.com/shipstream/bloodraven/internal/clock"
	"github.com/shipstream/bloodraven/internal/controller"
	"github.com/shipstream/bloodraven/internal/mysql"
	"github.com/shipstream/bloodraven/internal/platform"
	"github.com/shipstream/bloodraven/internal/state"
)

const (
	handoffPreFence = "3e11fa47-71ca-11e1-9e33-c80aa9429562:1-90"
	handoffFenced   = "3e11fa47-71ca-11e1-9e33-c80aa9429562:1-100"
)

// commitsUntilFenced is a healthy active primary that commits 91-100 after
// the operator's first look: its gtid_executed reads 1-90 until it is fenced
// and 1-100 from then on.
type commitsUntilFenced struct{ *mockMySQL }

func (s commitsUntilFenced) GetGtidExecuted(context.Context) (string, error) {
	if s.isSuperReadOnly() {
		return handoffFenced, nil
	}
	return handoffPreFence, nil
}

// slowReceiver is a direct replica that has applied everything it received
// (1-90). Its receiver fetches 91-100 only after the source is fenced, and
// the first look after the fence still shows 1-90.
type slowReceiver struct {
	*mockMySQL
	source *mockMySQL

	mu          sync.Mutex
	fencedReads int
}

func (r *slowReceiver) GetGtidExecuted(ctx context.Context) (string, error) {
	if r.source.isSuperReadOnly() {
		r.mu.Lock()
		r.fencedReads++
		caughtUp := r.fencedReads >= 2
		r.mu.Unlock()
		if caughtUp {
			r.setGtidExecuted(handoffFenced)
		}
	}
	return r.mockMySQL.GetGtidExecuted(ctx)
}

// An OrderedUpdate of the active site hands it to its standby while the
// standby's receiver still lacks 91-100. The emergency path's relay-log
// drain sees nothing pending and promotes at 1-90; the maintenance handoff
// must wait for the fenced source's full set before promoting.
func TestOrderedUpdate_HandoffPromotesOnlyAfterStandbyHasFencedSourceGTIDs(t *testing.T) {
	logger := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelWarn}))
	pdxHost := "mysql-lion-pdx.default.svc.cluster.local"
	iadHost := "mysql-lion-iad.default.svc.cluster.local"
	pdx := &mockMySQL{readOnly: false, gtidExecuted: handoffPreFence}
	iad := &mockMySQL{readOnly: true, superReadOnly: true, gtidExecuted: handoffPreFence,
		replicaStatus: &mysql.ReplicaStatus{IORunning: true, SQLRunning: true, SourceHost: pdxHost}}
	sites := []controller.SiteTopologyConfig{
		{Name: "pdx", Zone: "zone-pdx", LBIP: "10.0.0.2", Role: state.SiteRolePrimaryCandidate, Host: pdxHost},
		{Name: "iad", Zone: "zone-iad", LBIP: "10.0.0.1", Role: state.SiteRolePrimaryCandidate, Host: iadHost},
	}
	fc := controller.NewFailoverController(logger)
	tm := controller.NewTopologyManagerWithClock(
		controller.TopologyConfig{Name: "lion", Sites: sites, FailureThreshold: 3, RecoveryThreshold: 2},
		[]mysql.Checker{commitsUntilFenced{pdx}, &slowReceiver{mockMySQL: iad, source: pdx}},
		fc, controller.NewUpdateController(fc, logger), nil,
		controller.BootstrapConfig{ReplUser: "repl", ReplPassword: "secret"},
		newMockTainter(), platform.NewHub(logger), &mockDNS{}, logger,
		clock.NewFakeClock(time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC)),
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
	if s := tm.Status(); s.ActiveSite != "pdx" {
		t.Fatalf("setup: active site = %q, want pdx", s.ActiveSite)
	}

	tm.SetSpecDriftSites([]string{"pdx"})
	tm.Poll(context.Background())

	// The update runs on its own goroutine; wait for the old active's
	// Deployment update, which follows the handoff.
	deadline := time.Now().Add(10 * time.Second)
	for {
		mu.Lock()
		done := slices.Contains(applied, "pdx")
		mu.Unlock()
		if done {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("old active was never updated; applied=%v", applied)
		}
		time.Sleep(10 * time.Millisecond)
	}

	if got := tm.Status().PromotionGtidExecuted; got != handoffFenced {
		t.Fatalf("PromotionGtidExecuted = %q, want %q: the standby was promoted without the source's 91-100", got, handoffFenced)
	}
	if iad.isReadOnly() {
		t.Error("iad was not promoted")
	}
}

// The handoff hands pdx's role to iad, and pdx comes back from its
// Deployment update holding transactions iad never received (an
// out-of-band write; the handoff itself cannot lose any). The rollout
// refuses to repoint pdx and leaves it fenced with no replication channel.
// The refusal is only safe because old-primary recovery then takes the
// site: it must report the set as divergentGtid and block, not rejoin it.
func TestOrderedUpdate_OldActiveWithTransactionsNewPrimaryLacksIsReportedByRecovery(t *testing.T) {
	const errant = "d1240b6c-71ca-11f0-9e33-c80aa9429563:1-221"
	logger := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelWarn}))
	pdxHost := "mysql-lion-pdx.default.svc.cluster.local"
	iadHost := "mysql-lion-iad.default.svc.cluster.local"
	pdx := &mockMySQL{readOnly: false, gtidExecuted: handoffFenced}
	iad := &mockMySQL{readOnly: true, superReadOnly: true, gtidExecuted: handoffFenced,
		replicaStatus: &mysql.ReplicaStatus{IORunning: true, SQLRunning: true, SourceHost: pdxHost}}
	sites := []controller.SiteTopologyConfig{
		{Name: "pdx", Zone: "zone-pdx", LBIP: "10.0.0.2", Role: state.SiteRolePrimaryCandidate, Host: pdxHost},
		{Name: "iad", Zone: "zone-iad", LBIP: "10.0.0.1", Role: state.SiteRolePrimaryCandidate, Host: iadHost},
	}
	clk := clock.NewFakeClock(time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC))
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
	tm.ApplyUpdate = func(_ context.Context, site string) error {
		mu.Lock()
		applied = append(applied, site)
		mu.Unlock()
		if site == "pdx" {
			// The pod is replaced: mysqld is back with read_only only,
			// holding writes iad never received.
			pdx.respawn(true)
			pdx.setGtidExecuted(handoffFenced + "," + errant)
		}
		return nil
	}

	for i := 0; i < 4; i++ {
		tm.Poll(context.Background())
	}
	if s := tm.Status(); s.ActiveSite != "pdx" {
		t.Fatalf("setup: active site = %q, want pdx", s.ActiveSite)
	}

	tm.SetSpecDriftSites([]string{"pdx"})
	tm.Poll(context.Background())
	deadline := time.Now().Add(10 * time.Second)
	for {
		mu.Lock()
		updated := slices.Contains(applied, "pdx")
		mu.Unlock()
		if updated && !uc.IsUpdating() {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("ordered update did not reach and finish the old active; applied=%v", applied)
		}
		time.Sleep(10 * time.Millisecond)
	}
	if iad.isReadOnly() {
		t.Fatal("setup: the handoff did not promote iad")
	}
	// The runner rebuilds drift from live Deployment hashes, and pdx's
	// Deployment was updated, so it has none left.
	tm.SetSpecDriftSites(nil)

	var pdxStatus controller.SiteStatusEntry
	for i := 0; i < 10 && pdxStatus.RecoveryState != "RecoveryBlocked"; i++ {
		clk.Advance(2 * time.Second)
		tm.Poll(context.Background())
		pdxStatus = tm.Status().Sites[0]
	}

	if pdxStatus.RecoveryState != "RecoveryBlocked" {
		t.Fatalf("pdx recoveryState = %q, want RecoveryBlocked: old-primary recovery did not report the refused old active", pdxStatus.RecoveryState)
	}
	if pdxStatus.DivergentGtid != errant || pdxStatus.DivergentTransactionCount == nil || *pdxStatus.DivergentTransactionCount != 221 {
		t.Errorf("pdx divergentGtid = %q (count %v), want %q (221)", pdxStatus.DivergentGtid, pdxStatus.DivergentTransactionCount, errant)
	}
	if pdxStatus.DivergenceDetectedBy != "OldPrimaryRecovery" {
		t.Errorf("pdx divergenceDetectedBy = %q, want OldPrimaryRecovery", pdxStatus.DivergenceDetectedBy)
	}
	pdx.mu.Lock()
	repointed := pdx.replicationSourceSet || pdx.replicaStarted
	pdx.mu.Unlock()
	if repointed {
		t.Error("pdx was repointed at iad although it holds transactions iad lacks")
	}
	if !pdx.isSuperReadOnly() {
		t.Error("pdx is not fenced with super_read_only")
	}
}
