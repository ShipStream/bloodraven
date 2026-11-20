package component

import (
	"context"
	"log/slog"
	"os"
	"sync"
	"testing"
	"time"

	promtestutil "github.com/prometheus/client_golang/prometheus/testutil"
	v1beta1 "github.com/shipstream/bloodraven/api/v1beta1"
	"github.com/shipstream/bloodraven/internal/clock"
	"github.com/shipstream/bloodraven/internal/controller"
	"github.com/shipstream/bloodraven/internal/metrics"
	"github.com/shipstream/bloodraven/internal/mysql"
	"github.com/shipstream/bloodraven/internal/platform"
)

// The emergency failover's catch-up gate (issue #202): when the old primary
// answers the fence, the candidate is promoted only once it executes the
// old primary's fenced GTID set. These tests drive the real TopologyManager
// and FailoverController. The gate waits on the harness's fake clock, as
// in the DST: its sleep advances the clock, so a 30s budget costs no wall
// time.

const (
	catchUpUUID  = "aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa"
	catchUpFull  = catchUpUUID + ":1-100"
	catchUpBehin = catchUpUUID + ":1-90"
)

// newCatchUpHarness is newTestHarnessWithMySQL with the gate on the fake
// clock and an anti-flap cooldown of zero. priorities, when set, are
// spec.splitBrainPolicy.sitePriorities.
func newCatchUpHarness(t *testing.T, dc1, dc2 mysql.Checker, priorities []string) *testHarness {
	t.Helper()
	logger := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelWarn}))
	clk := clock.NewFakeClock(time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC))
	fc := controller.NewFailoverControllerWithClock(logger, clk)
	fc.SetCatchUpSleepForSim(func(_ context.Context, d time.Duration) error {
		clk.Advance(d)
		return nil
	})
	cfg := controller.TopologyConfig{
		Name:              "lion",
		Sites:             defaultTwoSiteConfig(),
		PollInterval:      int64(50 * time.Millisecond),
		FailureThreshold:  3,
		RecoveryThreshold: 2,
		SitePriorities:    priorities,
	}
	h := &testHarness{
		tainter: newMockTainter(),
		hub:     platform.NewHub(logger),
		dns:     &mockDNS{},
		logger:  logger,
		clock:   clk,
	}
	h.tm = controller.NewTopologyManagerWithClock(cfg, []mysql.Checker{dc1, dc2}, fc, nil, nil, controller.BootstrapConfig{},
		h.tainter, h.hub, h.dns, logger, clk)
	return h
}

// replicatingFrom is a healthy replica status of dc1.
func replicatingFrom() *mysql.ReplicaStatus {
	lag := int64(0)
	return &mysql.ReplicaStatus{IORunning: true, SQLRunning: true, SourceHost: "mysql-lion-dc1.default.svc.cluster.local", SecondsBehindSource: &lag}
}

// catchUpPair is dc1, the writable primary holding catchUpFull, and dc2, its
// read-only replica, which has executed only catchUpBehin and keeps
// replicating.
func catchUpPair() (*mockMySQL, *mockMySQL) {
	dc1 := &mockMySQL{readOnly: false, gtidExecuted: catchUpFull}
	dc2 := &mockMySQL{readOnly: true, gtidExecuted: catchUpBehin, replicaStatus: replicatingFrom()}
	return dc1, dc2
}

// flapOut makes dc1 miss the next three topology probes and answer again
// from then on: the operator declares it unreachable, and it answers the
// fence that follows.
func flapOut(dc1 *mockMySQL) { dc1.failNextCalls("CheckReadOnly", 3) }

func catchUpRefusals(site string) float64 {
	return promtestutil.ToFloat64(metrics.FailoverCatchUpRefusalsTotal.WithLabelValues(site))
}

// The #202 hole on the emergency path: the old primary answered the fence,
// but the candidate had executed only 90 of its 100 transactions. The
// failover is refused, the candidate is not touched, and the old primary,
// which was writable before the fence, keeps serving.
func TestEmergencyFailover_CandidateBehindReachableOldPrimaryIsNotPromoted(t *testing.T) {
	dc1, dc2 := catchUpPair()
	h := newCatchUpHarness(t, dc1, dc2, nil)
	h.pollN(2)
	before := catchUpRefusals("dc2")

	flapOut(dc1)
	h.pollN(3)

	if got := catchUpRefusals("dc2") - before; got != 1 {
		t.Fatalf("catch-up refusals = %v, want 1", got)
	}
	if !dc2.isReadOnly() || dc2.callCount("StopReplica") != 0 || dc2.callCount("ResetReplicaAll") != 0 {
		t.Fatalf("candidate touched: read_only=%v STOP REPLICA=%d RESET REPLICA ALL=%d",
			dc2.isReadOnly(), dc2.callCount("StopReplica"), dc2.callCount("ResetReplicaAll"))
	}
	if dc1.isReadOnly() || dc1.isSuperReadOnly() {
		t.Fatalf("old primary left fenced: read_only=%v super_read_only=%v; it was writable before the fence",
			dc1.isReadOnly(), dc1.isSuperReadOnly())
	}
	if dc1.callCount("SetSuperReadOnly") == 0 {
		t.Fatal("old primary was never fenced: the gate did not run")
	}
	if ip := h.dns.getLastIP(); ip == "2.2.2.2" {
		t.Fatal("DNS flipped to the refused candidate")
	}

	// The old primary is confirmed writable again and nothing else happens.
	h.pollN(3)
	s := h.tm.Status()
	if s.Sites[0].State != v1beta1.SiteStateWritable || s.Sites[1].State != v1beta1.SiteStateReadOnly {
		t.Fatalf("after the refusal: dc1=%s dc2=%s, want writable/read-only", s.Sites[0].State, s.Sites[1].State)
	}
	if !dc2.isReadOnly() {
		t.Fatal("candidate promoted after the refusal")
	}
}

// gtidHook makes a mockMySQL's GetGtidExecuted answer through fn once armed.
type gtidHook struct {
	*mockMySQL
	mu    sync.Mutex
	fn    func(reads int) string
	reads int
}

func (g *gtidHook) GetGtidExecuted(ctx context.Context) (string, error) {
	g.mu.Lock()
	fn := g.fn
	if fn != nil {
		g.reads++
	}
	reads := g.reads
	g.mu.Unlock()
	if fn != nil {
		g.setGtidExecuted(fn(reads))
	}
	return g.mockMySQL.GetGtidExecuted(ctx)
}

func (g *gtidHook) arm(fn func(reads int) string) {
	g.mu.Lock()
	defer g.mu.Unlock()
	g.fn = fn
}

// A candidate that receives the rest of the old primary's transactions
// during the wait is promoted, with all of them.
func TestEmergencyFailover_CandidateCatchesUpWithinBudgetIsPromoted(t *testing.T) {
	dc1, dc2mock := catchUpPair()
	dc2 := &gtidHook{mockMySQL: dc2mock}
	h := newCatchUpHarness(t, dc1, dc2, nil)
	h.pollN(2)
	before := catchUpRefusals("dc2")
	start := h.clock.Now()

	// The picker's read and the first few of the gate's see 90; then the
	// receiver delivers the rest.
	dc2.arm(func(reads int) string {
		if reads < 5 {
			return catchUpBehin
		}
		return catchUpFull
	})
	flapOut(dc1)
	h.pollN(3)

	if dc2.isReadOnly() {
		t.Fatal("candidate that caught up was not promoted")
	}
	if got := catchUpRefusals("dc2") - before; got != 0 {
		t.Fatalf("catch-up refusals = %v, want 0", got)
	}
	if !dc1.isSuperReadOnly() {
		t.Fatal("old primary not fenced at promotion")
	}
	if waited := h.clock.Since(start); waited >= 30*time.Second {
		t.Fatalf("gate waited %s, the whole budget; it should have ended at catch-up", waited)
	}
	h.pollN(2)
	if s := h.tm.Status(); s.LastFailover == nil || s.LastFailover.Target != "dc2" || s.LastFailover.PromotionGtidExecuted != catchUpFull {
		t.Fatalf("failover record = %+v, want target dc2 with promotion GTID %s", s.LastFailover, catchUpFull)
	}
}

// An old primary that was already read-only when the failover fenced it
// (its sidecar's fence, or the read_only a restarted mysqld boots with) is
// left fenced when the failover is refused: lifting a fence the failover
// did not set is the primary re-assert's decision, not the abort's.
func TestEmergencyFailover_SelfFencedOldPrimaryStaysFencedOnRefusal(t *testing.T) {
	dc1, dc2 := catchUpPair()
	h := newCatchUpHarness(t, dc1, dc2, nil)
	h.pollN(2)
	before := catchUpRefusals("dc2")

	flapOut(dc1)
	h.pollN(2)
	// Self-fenced while the operator could not reach it.
	dc1.mu.Lock()
	dc1.readOnly, dc1.superReadOnly = true, true
	dc1.mu.Unlock()
	h.pollN(1)

	if got := catchUpRefusals("dc2") - before; got != 1 {
		t.Fatalf("catch-up refusals = %v, want 1", got)
	}
	if !dc1.isSuperReadOnly() || !dc1.isReadOnly() {
		t.Fatalf("self-fenced old primary unfenced by the refusal: read_only=%v super_read_only=%v", dc1.isReadOnly(), dc1.isSuperReadOnly())
	}
	if n := dc1.writableGrants(); n != 0 {
		t.Fatalf("old primary received %d writable grants", n)
	}
	if !dc2.isReadOnly() {
		t.Fatal("candidate promoted")
	}
}

// The usual emergency, a dead old primary: nothing is waited for, and the
// candidate is promoted with what it has, as before.
func TestEmergencyFailover_UnreachableOldPrimaryIsNotWaitedFor(t *testing.T) {
	dc1, dc2 := catchUpPair()
	h := newCatchUpHarness(t, dc1, dc2, nil)
	h.pollN(2)
	start := h.clock.Now()

	dc1.setError(errDown)
	h.pollN(3)

	if dc2.isReadOnly() {
		t.Fatal("candidate not promoted over an unreachable old primary")
	}
	if waited := h.clock.Since(start); waited != 0 {
		t.Fatalf("emergency failover over an unreachable old primary waited %s", waited)
	}
	if n := dc1.callCount("KillAppConnections"); n != 0 {
		t.Fatalf("KillAppConnections sent %d times to an old primary that was never fenced", n)
	}
}

// A split brain resolved by spec.splitBrainPolicy.sitePriorities: both sides
// took writes, so the winner can never contain the loser's set. The gate
// must not wait for it, or the policy could never resolve anything.
func TestEmergencyFailover_SplitBrainWinnerIsNotGated(t *testing.T) {
	const otherUUID = "bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb"
	dc1 := &mockMySQL{readOnly: false, gtidExecuted: catchUpFull}
	dc2 := &mockMySQL{readOnly: false, gtidExecuted: catchUpBehin + "," + otherUUID + ":1-3"}
	h := newCatchUpHarness(t, dc1, dc2, []string{"dc2", "dc1"})
	before := catchUpRefusals("dc2")
	start := h.clock.Now()

	h.pollN(3)

	if got := catchUpRefusals("dc2") - before; got != 0 {
		t.Fatalf("catch-up refusals = %v, want 0: the split-brain winner was gated", got)
	}
	if waited := h.clock.Since(start); waited != 0 {
		t.Fatalf("split-brain resolution waited %s for a set the winner can never contain", waited)
	}
	if !dc1.isSuperReadOnly() {
		t.Fatal("split-brain loser not fenced")
	}
	if ip := h.dns.getLastIP(); ip != "2.2.2.2" {
		t.Fatalf("DNS = %q, want the winner dc2 (2.2.2.2): the promotion was not recorded", ip)
	}
}

// After a refusal, a transition back to Unreachable while the old primary
// still answers does not fence it again before the backoff: each attempt
// pauses its writes. Once it stops answering, the failover goes ahead at
// once, without waiting.
func TestEmergencyFailover_RefusalBackoffUntilOldPrimaryStopsAnswering(t *testing.T) {
	dc1, dc2 := catchUpPair()
	h := newCatchUpHarness(t, dc1, dc2, nil)
	h.pollN(2)
	before := catchUpRefusals("dc2")

	flapOut(dc1)
	h.pollN(3)
	if got := catchUpRefusals("dc2") - before; got != 1 {
		t.Fatalf("catch-up refusals = %v, want 1", got)
	}
	h.pollN(2) // dc1 confirmed writable again
	fences := dc1.callCount("SetSuperReadOnly")

	// It flaps again within the backoff.
	flapOut(dc1)
	h.pollN(3)
	if n := dc1.callCount("SetSuperReadOnly"); n != fences {
		t.Fatalf("old primary fenced again within the refusal backoff (%d fences, want %d)", n, fences)
	}
	if !dc2.isReadOnly() {
		t.Fatal("candidate promoted during the backoff")
	}

	// It dies: the next decision probes it, finds it gone and promotes.
	dc1.setError(errDown)
	h.pollN(1)
	if dc2.isReadOnly() {
		t.Fatal("candidate not promoted once the old primary stopped answering")
	}
	if got := catchUpRefusals("dc2") - before; got != 1 {
		t.Fatalf("catch-up refusals = %v, want still 1", got)
	}
}
