package controller

import (
	"context"
	"errors"
	"log/slog"
	"os"
	"testing"
	"time"

	"k8s.io/apimachinery/pkg/types"

	"github.com/shipstream/bloodraven/internal/mysql"
	"github.com/shipstream/bloodraven/internal/platform"
	"github.com/shipstream/bloodraven/internal/state"
)

// claimFixture is a two-site group, dc1 the settled primary and dc2 a
// read-only replica, with its manager registered on a runner.
type claimFixture struct {
	r  *TopologyManagerRunner
	nn types.NamespacedName
	tm *TopologyManager
}

func newClaimFixture(t *testing.T) *claimFixture {
	t.Helper()
	f := &claimFixture{
		r: &TopologyManagerRunner{
			logger:   slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelError})),
			managers: make(map[types.NamespacedName]*managedTopology),
		},
		nn: types.NamespacedName{Namespace: "shared-lion", Name: "lion"},
	}
	f.tm = f.newManager()
	f.r.registerManager(f.nn, &managedTopology{tm: f.tm})
	return f
}

// claimMySQL is a mockMySQL that also reports its server_uuid, as the
// production checker does, for the planned-failover grant's source probe.
type claimMySQL struct {
	*mockMySQL
	uuid string
}

func (m *claimMySQL) ReadGTIDSnapshot(ctx context.Context) (mysql.GTIDSnapshot, error) {
	gtid, err := m.GetGtidExecuted(ctx)
	return mysql.GTIDSnapshot{ServerUUID: m.uuid, GtidExecuted: gtid}, err
}

func (f *claimFixture) newManager() *TopologyManager {
	dc1 := &claimMySQL{mockMySQL: &mockMySQL{}, uuid: "aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa"}
	dc2 := &claimMySQL{mockMySQL: &mockMySQL{readOnly: true, superReadOnly: true,
		replicaStatusVal: &mysql.ReplicaStatus{IORunning: true, SQLRunning: true, SourceHost: "mysql-dc1"}},
		uuid: "bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb"}
	tm := NewTopologyManager(testTopologyConfig(), []mysql.Checker{dc1, dc2}, NewFailoverController(testLogger()), nil, nil,
		BootstrapConfig{}, newMockTainter(), platform.NewHub(testLogger()), &mockDNS{}, testLogger())
	tm.failoverCooldown = 0
	settleAuthority(tm)
	tm.SetReadyForSim()
	return tm
}

// retire retires the registered manager as a runner sync does, keeping it
// on the retiring list (its Run has not returned) until the test ends.
func (f *claimFixture) retire(t *testing.T) {
	t.Helper()
	f.r.mu.Lock()
	mt := f.r.managers[f.nn]
	done := make(chan struct{})
	mt.done = done
	f.r.retireManagerLocked(f.nn, mt)
	f.r.mu.Unlock()
	t.Cleanup(func() { close(done) })
}

type claimOutcome byte

const (
	claimAllowed claimOutcome = 'A'
	claimRefused claimOutcome = 'R'
	claimWaits   claimOutcome = 'W'
)

func shortCtx(t *testing.T) context.Context {
	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	t.Cleanup(cancel)
	return ctx
}

// claimants are the columns of the matrix in topology_claims.go, each
// calling the real claim entry point.
var claimants = []struct {
	name  string
	claim func(t *testing.T, f *claimFixture) claimOutcome
}{
	{"Auto", func(t *testing.T, f *claimFixture) claimOutcome {
		if began, _ := f.tm.beginAutoPromotionOf("dc2"); began {
			return claimAllowed
		}
		return claimRefused
	}},
	{"Upd", func(t *testing.T, f *claimFixture) claimOutcome {
		// Planned now, so the epoch and active site match: only an
		// explicit check can refuse.
		f.tm.mu.RLock()
		epoch, active := f.tm.authorityEpoch, f.tm.activeSiteLocked()
		f.tm.mu.RUnlock()
		claimed, err := f.tm.claimOrderedUpdate(shortCtx(t), epoch, active)
		switch {
		case err != nil:
			return claimWaits
		case claimed:
			return claimAllowed
		}
		return claimRefused
	}},
	{"Rot", func(t *testing.T, f *claimFixture) claimOutcome {
		if ok, _ := f.r.ReserveKeyringRotation(f.nn, "dc2"); ok {
			return claimAllowed
		}
		return claimRefused
	}},
	{"Fol", func(t *testing.T, f *claimFixture) claimOutcome {
		release, ok := f.tm.beginFollowerMutation()
		if !ok {
			return claimRefused
		}
		release()
		return claimAllowed
	}},
	{"Boot", func(t *testing.T, f *claimFixture) claimOutcome {
		if _, why := f.tm.claimBootstrap("dc2", "auto-clone"); why == "" {
			return claimAllowed
		}
		return claimRefused
	}},
	{"PF", func(t *testing.T, f *claimFixture) claimOutcome {
		d, err := f.r.ClaimPlannedFailover(context.Background(), f.nn, claimRequest(f.tm, "pf-column"))
		switch {
		case err != nil:
			t.Fatalf("PF claim: %v", err)
		case d.Kind == ClaimGranted:
			return claimAllowed
		case d.Kind == ClaimWait:
			return claimWaits
		}
		return claimRefused
	}},
}

// holdPlannedClaim installs another run's claim in the group's slot.
func holdPlannedClaim(f *claimFixture, mode plannedClaimMode, provisional bool) {
	f.r.Slot(f.nn).hydrate(plannedClaim{mode: mode, runID: "run-held", source: "dc1", target: "dc2", provisional: provisional})
}

// TestTopologyClaimMatrix: for every held claim (row) and every claimant
// (column), the claimant's real entry point is refused, told to wait, or
// allowed exactly as the matrix in topology_claims.go documents. Rows are
// taken through the holders' own claim entry points where one exists.
func TestTopologyClaimMatrix(t *testing.T) {
	ctx := context.Background()
	rows := []struct {
		name string
		want string // one outcome per claimant, in column order
		hold func(t *testing.T, f *claimFixture)
	}{
		{"Auto promotion", "RRRRRW", func(t *testing.T, f *claimFixture) {
			if began, _ := f.tm.beginAutoPromotionOf("dc2"); !began {
				t.Fatal("setup: auto promotion not claimed")
			}
		}},
		{"promotion not yet observed", "RRRRAW", func(t *testing.T, f *claimFixture) {
			if began, _ := f.tm.beginAutoPromotionOf("dc2"); !began {
				t.Fatal("setup: auto promotion not claimed")
			}
			f.tm.mu.Lock()
			f.tm.promotedSite = "dc2" // recordFailover
			f.tm.mu.Unlock()
			f.tm.finishDeploymentTopologyChange()
		}},
		{"lost-status wait", "RRRAAW", func(t *testing.T, f *claimFixture) {
			f.tm.mu.Lock()
			f.tm.awaitingObservedPrimary = true // restoreFailoverRecordFrom
			f.tm.mu.Unlock()
		}},
		{"planned-failover claim", "RRRRRW", func(t *testing.T, f *claimFixture) {
			holdPlannedClaim(f, claimModeRun, false)
		}},
		{"planned-failover claim (provisional grant)", "RRRRRW", func(t *testing.T, f *claimFixture) {
			holdPlannedClaim(f, claimModeRun, true)
		}},
		{"planned-failover hold", "RRRRRR", func(t *testing.T, f *claimFixture) {
			holdPlannedClaim(f, claimModeHold, false)
		}},
		{"planned request waiting", "AAAAAA", func(t *testing.T, f *claimFixture) {}},
		{"ordered update claimed", "RRRRRW", func(t *testing.T, f *claimFixture) {
			f.tm.mu.RLock()
			epoch, active := f.tm.authorityEpoch, f.tm.activeSiteLocked()
			f.tm.mu.RUnlock()
			if claimed, err := f.tm.claimOrderedUpdate(ctx, epoch, active); !claimed || err != nil {
				t.Fatalf("setup: claimed=%v err=%v", claimed, err)
			}
		}},
		{"ordered update pending", "AARAAA", func(t *testing.T, f *claimFixture) {
			f.tm.mu.Lock()
			f.tm.deploymentUpdatePending = true // startUpdate
			f.tm.mu.Unlock()
		}},
		{"rotation of the target", "RAAAAW", func(t *testing.T, f *claimFixture) {
			if ok, why := f.r.ReserveKeyringRotation(f.nn, "dc2"); !ok {
				t.Fatalf("setup: %s", why)
			}
		}},
		{"follower mutation", "AWAAAW", func(t *testing.T, f *claimFixture) {
			release, ok := f.tm.beginFollowerMutation()
			if !ok {
				t.Fatal("setup: follower mutation not admitted")
			}
			t.Cleanup(release)
		}},
		{"bootstrap into a promotable site", "RRARRW", func(t *testing.T, f *claimFixture) {
			if _, why := f.tm.claimBootstrap("dc2", "reclone"); why != "" {
				t.Fatalf("setup: %s", why)
			}
		}},
		{"in-place restore", "RRARRW", func(t *testing.T, f *claimFixture) {
			f.tm.SetTopologyFrozen(true) // runner, from status.restoreInPlace
		}},
		{"predecessor's ordered update", "RRRRRW", func(t *testing.T, f *claimFixture) {
			f.tm.setPredecessorUpdateInFlight(true) // registerManager
		}},
		{"predecessor still promoting", "RRRRRW", func(t *testing.T, f *claimFixture) {
			if began, _ := f.tm.beginAutoPromotionOf("dc2"); !began {
				t.Fatal("setup: auto promotion not claimed")
			}
			f.retire(t)
			f.tm = f.newManager()
			f.r.registerManager(f.nn, &managedTopology{tm: f.tm})
		}},
		{"predecessor still in a follower mutation", "RRARRW", func(t *testing.T, f *claimFixture) {
			release, ok := f.tm.beginFollowerMutation()
			if !ok {
				t.Fatal("setup: follower mutation not admitted")
			}
			t.Cleanup(release)
			f.retire(t)
			f.tm = f.newManager()
			f.r.registerManager(f.nn, &managedTopology{tm: f.tm})
		}},
		{"this manager retired", "RRRRRW", func(t *testing.T, f *claimFixture) {
			f.retire(t)
		}},
	}
	for _, row := range rows {
		for i, col := range claimants {
			t.Run(row.name+"/"+col.name, func(t *testing.T) {
				f := newClaimFixture(t)
				row.hold(t, f)
				if got, want := col.claim(t, f), claimOutcome(row.want[i]); got != want {
					t.Fatalf("%s while %s holds: got %c, want %c", col.name, row.name, got, want)
				}
			})
		}
	}
}

// GPT round 11: automatic promotion's claim did not check the planned
// failover's guard, so a planned failover claimed between an emergency
// failover's (or re-assert's) eligibility checks and its claim was raced:
// the promotion or unfence went ahead on sites the planned failover had
// fenced. Every holder that refuses Auto in the matrix is placed in that
// window here, through the real decision path.
func TestAutoPromotion_ClaimBetweenEligibilityAndClaimStopsIt(t *testing.T) {
	holders := []struct {
		name string
		hold func(t *testing.T, r *TopologyManagerRunner, nn types.NamespacedName, tm *TopologyManager)
	}{
		{"planned-failover claim", func(t *testing.T, r *TopologyManagerRunner, nn types.NamespacedName, _ *TopologyManager) {
			r.Slot(nn).hydrate(plannedClaim{mode: claimModeRun, runID: "run-1", source: "dc1", target: "dc2", provisional: true})
		}},
		{"planned-failover hold", func(t *testing.T, r *TopologyManagerRunner, nn types.NamespacedName, _ *TopologyManager) {
			r.Slot(nn).toHold("run-1")
		}},
		{"in-place restore", func(t *testing.T, _ *TopologyManagerRunner, _ types.NamespacedName, tm *TopologyManager) {
			tm.SetTopologyFrozen(true)
		}},
		{"bootstrap", func(t *testing.T, _ *TopologyManagerRunner, _ types.NamespacedName, tm *TopologyManager) {
			if _, why := tm.claimBootstrap("dc2", "reclone"); why != "" {
				t.Fatalf("bootstrap claim refused in the window: %s", why)
			}
		}},
	}
	newRunner := func(tm *TopologyManager) (*TopologyManagerRunner, types.NamespacedName) {
		r := &TopologyManagerRunner{
			logger:   slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelError})),
			managers: make(map[types.NamespacedName]*managedTopology),
		}
		nn := types.NamespacedName{Namespace: "shared-lion", Name: "lion"}
		r.registerManager(nn, &managedTopology{tm: tm})
		return r, nn
	}
	for _, h := range holders {
		t.Run("emergency failover/"+h.name, func(t *testing.T) {
			site0 := &mockMySQL{readOnly: false}
			site1 := &mockMySQL{readOnly: true}
			tm, _, _ := newTestTopologyManager(site0, site1)
			r, nn := newRunner(tm)
			pollN(tm, 2)
			fired := false
			tm.beforePromotionClaim = func(string) {
				if !fired {
					fired = true
					h.hold(t, r, nn, tm)
				}
			}
			site0.setError(errors.New("connection refused"))
			pollN(tm, 3)
			if !fired {
				t.Fatal("setup: the emergency failover never reached its claim")
			}
			site1.mu.Lock()
			promoted, readOnly := site1.promoted, site1.readOnly
			site1.mu.Unlock()
			if promoted || !readOnly {
				t.Fatalf("emergency failover promoted dc2 after a %s claimed the sites", h.name)
			}
			tm.mu.RLock()
			changing, retry := tm.deploymentChanging, tm.crossSiteRetry
			tm.mu.RUnlock()
			if changing != 0 || !retry {
				t.Fatalf("refused claim: deploymentChanging=%d crossSiteRetry=%v; want 0 and re-armed", changing, retry)
			}
		})
		t.Run("primary re-assert/"+h.name, func(t *testing.T) {
			site0 := &mockMySQL{readOnly: true, gtidExecuted: "aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa:1-10"}
			site1 := &mockMySQL{readOnly: true, gtidExecuted: "aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa:1-8"}
			tm, _, _ := newTestTopologyManager(site0, site1)
			r, nn := newRunner(tm)
			setWedgedTopology(tm, "dc1")
			fired := false
			tm.beforePromotionClaim = func(string) {
				fired = true
				h.hold(t, r, nn, tm)
			}
			if tm.checkPrimaryReassert(context.Background()) {
				t.Fatalf("re-assert ran after a %s claimed the sites", h.name)
			}
			if !fired {
				t.Fatal("setup: the re-assert never reached its claim")
			}
			site0.mu.Lock()
			readOnly := site0.readOnly
			site0.mu.Unlock()
			tm.mu.RLock()
			stamped := tm.lastReassert
			tm.mu.RUnlock()
			if !readOnly || !stamped.IsZero() {
				t.Fatalf("re-assert unfenced=%v cooldown charged=%v after a %s claimed the sites", !readOnly, !stamped.IsZero(), h.name)
			}
		})
	}
}

// holdPlannedClaimLockedForTest makes tm's group hold a planned-failover
// claim (another run's), wiring a slot if tm has none. The caller holds
// tm.mu or no other goroutine uses tm yet.
func holdPlannedClaimLockedForTest(tm *TopologyManager, held bool) {
	if tm.plannedSlot == nil {
		tm.plannedSlot = &PlannedClaimSlot{}
	}
	slot := tm.plannedSlot
	slot.mu.Lock()
	defer slot.mu.Unlock()
	if held {
		slot.held = &plannedClaim{mode: claimModeRun, runID: "run-test", source: "dc1", target: "dc2"}
	} else {
		slot.held = nil
	}
}

// setPlannedClaimForTest is holdPlannedClaimLockedForTest under tm.mu.
func setPlannedClaimForTest(tm *TopologyManager, held bool) {
	tm.mu.Lock()
	defer tm.mu.Unlock()
	holdPlannedClaimLockedForTest(tm, held)
}

// TestTopologyClaimMatrix_ReaderBootstrap is the matrix's "bootstrap
// (reader)" row on a group with a read-only reader dc3: a clone into the
// reader holds off neither Auto nor PF (A10), the rest as for a promotable
// recipient.
func TestTopologyClaimMatrix_ReaderBootstrap(t *testing.T) {
	const want = "ARARRA"
	for i, col := range claimants {
		t.Run(col.name, func(t *testing.T) {
			f := newClaimFixture(t)
			cfg := testTopologyConfig()
			cfg.Sites = append(cfg.Sites, SiteTopologyConfig{Name: "dc3", Zone: "lion-dc3", LBIP: "3.3.3.3", Role: state.SiteRoleReadOnly, Host: "mysql-dc3"})
			dc1 := &claimMySQL{mockMySQL: &mockMySQL{}, uuid: "aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa"}
			dc2 := &claimMySQL{mockMySQL: &mockMySQL{readOnly: true, superReadOnly: true,
				replicaStatusVal: &mysql.ReplicaStatus{IORunning: true, SQLRunning: true, SourceHost: "mysql-dc1"}},
				uuid: "bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb"}
			dc3 := &claimMySQL{mockMySQL: &mockMySQL{readOnly: true}, uuid: "cccccccc-cccc-cccc-cccc-cccccccccccc"}
			tm := NewTopologyManager(cfg, []mysql.Checker{dc1, dc2, dc3}, NewFailoverController(testLogger()), nil, nil,
				BootstrapConfig{}, newMockTainter(), platform.NewHub(testLogger()), &mockDNS{}, testLogger())
			tm.failoverCooldown = 0
			tm.mu.Lock()
			tm.sites[0].state, tm.sites[1].state, tm.sites[2].state = state.StateWritable, state.StateReadOnly, state.StateReadOnly
			tm.mu.Unlock()
			tm.SetReadyForSim()
			f.tm = tm
			f.r.registerManager(f.nn, &managedTopology{tm: tm})
			if _, why := tm.claimBootstrap("dc3", "auto-clone"); why != "" {
				t.Fatalf("setup: %s", why)
			}
			if got := col.claim(t, f); got != claimOutcome(want[i]) {
				t.Fatalf("%s while a reader clone runs: got %c, want %c", col.name, got, want[i])
			}
		})
	}
}
