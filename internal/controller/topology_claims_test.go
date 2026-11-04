package controller

import (
	"context"
	"errors"
	"log/slog"
	"os"
	"testing"
	"time"

	"k8s.io/apimachinery/pkg/types"

	v1alpha1 "github.com/shipstream/bloodraven/api/v1alpha1"
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

func (f *claimFixture) newManager() *TopologyManager {
	tm, _, _ := newTestTopologyManager(&mockMySQL{}, &mockMySQL{readOnly: true})
	settleAuthority(tm)
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
	{"PProm", func(t *testing.T, f *claimFixture) claimOutcome {
		// PlannedPromote runs only under its planned failover's claim.
		f.tm.SetPlannedFailoverActive(true)
		_, err := f.tm.claimPlannedPromotion(shortCtx(t), "dc2")
		switch {
		case err == nil:
			return claimAllowed
		case errors.Is(err, errHandoffNotAdmitted):
			return claimWaits
		}
		return claimRefused
	}},
	{"PF", func(t *testing.T, f *claimFixture) claimOutcome {
		ok, _, retry := f.r.ClaimPlannedFailover(f.nn, "dc1", "dc2")
		switch {
		case ok:
			return claimAllowed
		case retry:
			return claimWaits
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
		{"Auto promotion", "RRWRRRR", func(t *testing.T, f *claimFixture) {
			if began, _ := f.tm.beginAutoPromotionOf("dc2"); !began {
				t.Fatal("setup: auto promotion not claimed")
			}
		}},
		{"PlannedPromote", "RRWRRRR", func(t *testing.T, f *claimFixture) {
			if ok, why, _ := f.r.ClaimPlannedFailover(f.nn, "dc1", "dc2"); !ok {
				t.Fatalf("setup: %s", why)
			}
			if _, err := f.tm.claimPlannedPromotion(ctx, "dc2"); err != nil {
				t.Fatalf("setup: %v", err)
			}
		}},
		// The same planned failover re-runs Promoting after its Resuming
		// write was lost: its own completed promotion is recognised, not
		// refused as an unobserved one.
		{"own planned promotion completed", "RAWRRRR", func(t *testing.T, f *claimFixture) {
			if ok, why, _ := f.r.ClaimPlannedFailover(f.nn, "dc1", "dc2"); !ok {
				t.Fatalf("setup: %s", why)
			}
			if _, err := f.tm.PlannedPromote(ctx, "dc2", "dc1"); err != nil {
				t.Fatalf("setup: %v", err)
			}
		}},
		{"promotion not yet observed", "RRWRRRA", func(t *testing.T, f *claimFixture) {
			if began, _ := f.tm.beginAutoPromotionOf("dc2"); !began {
				t.Fatal("setup: auto promotion not claimed")
			}
			f.tm.mu.Lock()
			f.tm.promotedSite = "dc2" // recordFailover
			f.tm.mu.Unlock()
			f.tm.finishDeploymentTopologyChange()
		}},
		// A request waiting before its fence holds nothing: the guard is
		// re-derived from its (pre-fence) status as unset.
		{"planned failover waiting to claim", "AAAAAAA", func(t *testing.T, f *claimFixture) {
			if ok, _, _ := f.r.ClaimPlannedFailover(f.nn, "dc2", "dc1"); ok {
				t.Fatal("setup: a claim against the wrong source was granted")
			}
			f.tm.SetPlannedFailoverActive(plannedFailoverHoldsSites(&v1alpha1.PlannedFailoverStatus{Phase: v1alpha1.PlannedFailoverPhaseDraining}))
		}},
		{"planned failover", "RAARRRR", func(t *testing.T, f *claimFixture) {
			if ok, why, _ := f.r.ClaimPlannedFailover(f.nn, "dc1", "dc2"); !ok {
				t.Fatalf("setup: %s", why)
			}
		}},
		{"ordered update claimed", "RRRRRRR", func(t *testing.T, f *claimFixture) {
			f.tm.mu.RLock()
			epoch, active := f.tm.authorityEpoch, f.tm.activeSiteLocked()
			f.tm.mu.RUnlock()
			if claimed, err := f.tm.claimOrderedUpdate(ctx, epoch, active); !claimed || err != nil {
				t.Fatalf("setup: claimed=%v err=%v", claimed, err)
			}
		}},
		{"ordered update pending", "AAAARAA", func(t *testing.T, f *claimFixture) {
			f.tm.mu.Lock()
			f.tm.deploymentUpdatePending = true // startUpdate
			f.tm.mu.Unlock()
		}},
		{"rotation of the target", "RRRAAAA", func(t *testing.T, f *claimFixture) {
			if ok, why := f.r.ReserveKeyringRotation(f.nn, "dc2"); !ok {
				t.Fatalf("setup: %s", why)
			}
		}},
		{"follower mutation", "AWAWAAA", func(t *testing.T, f *claimFixture) {
			release, ok := f.tm.beginFollowerMutation()
			if !ok {
				t.Fatal("setup: follower mutation not admitted")
			}
			t.Cleanup(release)
		}},
		{"bootstrap into a promotable site", "RRRRARR", func(t *testing.T, f *claimFixture) {
			if _, why := f.tm.claimBootstrap("dc2", "reclone"); why != "" {
				t.Fatalf("setup: %s", why)
			}
		}},
		{"in-place restore", "RRRRARR", func(t *testing.T, f *claimFixture) {
			f.tm.SetTopologyFrozen(true) // runner, from status.restoreInPlace
		}},
		{"predecessor's ordered update", "RRRRRRR", func(t *testing.T, f *claimFixture) {
			f.tm.setPredecessorUpdateInFlight(true) // registerManager
		}},
		{"predecessor still promoting", "RWWRRRR", func(t *testing.T, f *claimFixture) {
			if began, _ := f.tm.beginAutoPromotionOf("dc2"); !began {
				t.Fatal("setup: auto promotion not claimed")
			}
			f.retire(t)
			f.tm = f.newManager()
			f.r.registerManager(f.nn, &managedTopology{tm: f.tm})
		}},
		{"predecessor still in a follower mutation", "RWARARR", func(t *testing.T, f *claimFixture) {
			release, ok := f.tm.beginFollowerMutation()
			if !ok {
				t.Fatal("setup: follower mutation not admitted")
			}
			t.Cleanup(release)
			f.retire(t)
			f.tm = f.newManager()
			f.r.registerManager(f.nn, &managedTopology{tm: f.tm})
		}},
		{"this manager retired", "RRWRRRR", func(t *testing.T, f *claimFixture) {
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

// A planned failover's claim refuses for good when the manager observes a
// primary other than its source: authority moved since validation.
func TestClaimPlannedFailover_RefusesWhenSourceIsNoLongerPrimary(t *testing.T) {
	f := newClaimFixture(t)
	if ok, why, retry := f.r.ClaimPlannedFailover(f.nn, "dc2", "dc1"); ok || retry || why != plannedClaimRefusedSourceMoved("dc1", "dc2") {
		t.Fatalf("claim with source dc2 while dc1 is primary = %v, %q, retry=%v; want refused for good", ok, why, retry)
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
		{"planned failover", func(t *testing.T, r *TopologyManagerRunner, nn types.NamespacedName, tm *TopologyManager) {
			// No site is writable here, so the claim itself waits (GPT
			// round 13); the guard still arrives from status, as the
			// runner's sync derives it for an in-flight planned failover.
			if ok, why, retry := r.ClaimPlannedFailover(nn, "dc1", "dc2"); ok || !retry || why != plannedClaimRefusedAuthorityUnsettled {
				t.Fatalf("planned-failover claim with no primary named = %v, %q, retry=%v; want a wait", ok, why, retry)
			}
			tm.SetPlannedFailoverActive(true)
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
