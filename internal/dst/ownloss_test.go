package dst

import (
	"context"
	"crypto/sha256"
	"fmt"
	"strings"
	"testing"

	"github.com/shipstream/bloodraven/internal/mysql"
)

// scheduleGoldenBeforeOwnLossFaults is scheduleGolden at the commit before
// the regress faults existed: with OpRegressSite and OpRegressBoot
// removed, every seed's schedule must still hash to it.
const scheduleGoldenBeforeOwnLossFaults = "b184a6370e18cf9382b844893e97f7d31d3b86ec9eafea28250131e7411f7c92"

// The regress faults come from a PRNG stream of their own: they add ops to
// a seed's schedule and change none of the ops it had.
func TestGenerateTrial_OwnLossFaultsOnlyAdd(t *testing.T) {
	h := sha256.New()
	regress, boots, primary := 0, 0, 0
	for seed := uint64(1); seed <= 2000; seed++ {
		tr := GenerateTrial(seed)
		kept := tr.Ops[:0:0]
		var regressAt, bootAt = -1, -1
		for _, op := range tr.Ops {
			switch op.Kind {
			case OpRegressSite:
				regress++
				regressAt = op.At
				if op.Site == tr.SiteNames[0] {
					primary++
				}
				if op.At < tr.WarmupPolls || op.At >= tr.HealAt || op.K < 1 || op.K > 6 {
					t.Fatalf("seed %d: regress outside its shape: %s", seed, op)
				}
				continue
			case OpRegressBoot:
				boots++
				bootAt = op.At
				if op.At >= tr.HealAt || op.BootWritable {
					t.Fatalf("seed %d: regress boot outside its shape: %s", seed, op)
				}
				continue
			}
			kept = append(kept, op)
		}
		if bootAt >= 0 && (regressAt < 0 || bootAt <= regressAt || bootAt > regressAt+12) {
			t.Fatalf("seed %d: regress at p%d, boot at p%d", seed, regressAt, bootAt)
		}
		tr.Ops = kept
		fmt.Fprint(h, tr.String())
	}
	if got := fmt.Sprintf("%x", h.Sum(nil)); got != scheduleGoldenBeforeOwnLossFaults {
		t.Fatalf("the regress faults changed the rest of the schedule: hash %s, want %s", got, scheduleGoldenBeforeOwnLossFaults)
	}
	if regress < 400 || regress > 600 || boots < regress/2 || primary < regress/2 {
		t.Fatalf("regress faults over 2000 seeds: %d (boots %d, on the initial primary %d); want about one trial in four, most with a boot, most on the initial primary", regress, boots, primary)
	}
}

func ownLossCluster(t *testing.T) (*Cluster, string, string) {
	t.Helper()
	const alphaUUID, betaUUID = "00000000-0000-0000-0000-000000000001", "00000000-0000-0000-0000-000000000002"
	c := NewCluster([]SiteSpec{
		{Name: "alpha", Host: "alpha.mysql.sim", LoadBalancerIP: "10.0.0.1", UUID: alphaUUID},
		{Name: "beta", Host: "beta.mysql.sim", LoadBalancerIP: "10.0.0.2", UUID: betaUUID},
	}, 100)
	for i := 0; i < 6; i++ {
		c.Tick()
	}
	return c, alphaUUID, betaUUID
}

func truthOf(c *Cluster, site string) SiteTruth {
	for _, s := range c.Truth() {
		if s.Name == site {
			return s
		}
	}
	return SiteTruth{}
}

// lossyRejoin puts the cluster in the state the defect produced: alpha (the
// old primary) is back on a data directory three ticks old, beta was
// promoted, and alpha replicates from beta.
func lossyRejoin(t *testing.T) (c *Cluster, alphaUUID, betaUUID string) {
	t.Helper()
	c, alphaUUID, betaUUID = ownLossCluster(t)
	if !c.RegressSite("alpha", 3) {
		t.Fatal("the regress was not applied")
	}
	ctx := context.Background()
	beta := c.NewChecker("beta")
	if err := beta.Promote(ctx); err != nil {
		t.Fatal(err)
	}
	c.Recover("alpha", true, false)
	alpha := c.NewChecker("alpha")
	if err := alpha.ChangeReplicationSource(ctx, mysql.ReplicationSourceOpts{Host: "beta.mysql.sim"}); err != nil {
		t.Fatal(err)
	}
	if err := alpha.StartReplica(ctx); err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 5; i++ {
		c.Tick()
	}
	return c, alphaUUID, betaUUID
}

// The measured MySQL behaviour the model must have: a replica never
// receives the events it issued itself. It catches up on everything else,
// and both threads keep running without an error.
func TestModel_ReceiverDropsTheReplicasOwnTransactions(t *testing.T) {
	c, alphaUUID, betaUUID := lossyRejoin(t)
	alpha, beta := truthOf(c, "alpha"), truthOf(c, "beta")
	if got, want := alpha.Executed[alphaUUID], beta.Executed[alphaUUID]-3; got != want {
		t.Fatalf("alpha holds %d of its own stream, want %d: its receiver must drop its own events (beta holds %d)",
			got, want, beta.Executed[alphaUUID])
	}
	if alpha.Executed[betaUUID] != beta.Executed[betaUUID] || beta.Executed[betaUUID] == 0 {
		t.Fatalf("alpha holds %d of beta's stream, beta %d: everything else must replicate", alpha.Executed[betaUUID], beta.Executed[betaUUID])
	}
	rs, err := c.NewChecker("alpha").ShowReplicaStatus(context.Background())
	if err != nil || rs == nil || !rs.IORunning || !rs.SQLRunning || rs.LastIOError != "" || rs.LastSQLError != "" {
		t.Fatalf("replica status = %+v, %v; want both threads running and no error (the divergence is silent)", rs, err)
	}
}

// PromotedWithoutOwnTransactions: fires when the operator makes such a
// site writable while a site it can read holds what the site lacks, and
// stays quiet for the three cases it must not judge.
func TestModel_PromotedWithoutOwnTransactions(t *testing.T) {
	ctx := context.Background()
	fired := func(c *Cluster) bool {
		for _, v := range c.Violations() {
			if v.Invariant == "PromotedWithoutOwnTransactions" {
				return true
			}
		}
		return false
	}
	t.Run("fires: the holder answers", func(t *testing.T) {
		c, _, _ := lossyRejoin(t)
		c.RogueFence("beta")
		if err := c.NewChecker("alpha").Promote(ctx); err != nil {
			t.Fatal(err)
		}
		if !fired(c) {
			t.Fatal("no violation for a promotion of a site that lacks its own transactions beside a readable holder")
		}
	})
	t.Run("fires for SET GLOBAL read_only=OFF as well", func(t *testing.T) {
		c, _, _ := lossyRejoin(t)
		c.RogueFence("beta")
		if err := c.NewChecker("alpha").SetReadOnly(ctx, false); err != nil {
			t.Fatal(err)
		}
		if !fired(c) {
			t.Fatal("no violation")
		}
	})
	t.Run("quiet: no holder the operator can read", func(t *testing.T) {
		c, _, _ := lossyRejoin(t)
		c.Crash("beta")
		if err := c.NewChecker("alpha").Promote(ctx); err != nil {
			t.Fatal(err)
		}
		if fired(c) {
			t.Fatal("a promotion without evidence was judged")
		}
	})
	t.Run("quiet: the site was writable already", func(t *testing.T) {
		c, _, _ := lossyRejoin(t)
		c.RogueFence("beta")
		c.RogueWritable("alpha")
		if err := c.NewChecker("alpha").SetReadOnly(ctx, false); err != nil {
			t.Fatal(err)
		}
		if fired(c) {
			t.Fatal("a statement on a site an external writer had opened was judged as a grant")
		}
	})
	t.Run("quiet: after a reclone", func(t *testing.T) {
		c, _, _ := lossyRejoin(t)
		if !c.Reclone("beta", "alpha") {
			t.Fatal("reclone refused")
		}
		c.RogueFence("beta")
		if err := c.NewChecker("alpha").Promote(ctx); err != nil {
			t.Fatal(err)
		}
		if fired(c) {
			t.Fatal("a recloned site was judged")
		}
	})
}

func TestModel_Reclone(t *testing.T) {
	c, alphaUUID, _ := lossyRejoin(t)
	before := truthOf(c, "alpha")
	if !c.Reclone("beta", "alpha") {
		t.Fatal("reclone refused")
	}
	alpha, beta := truthOf(c, "alpha"), truthOf(c, "beta")
	if alpha.Executed.deficit(beta.Executed) != 0 || beta.Executed.deficit(alpha.Executed) != 0 {
		t.Fatalf("recipient %s, donor %s: want equal sets", alpha.Executed, beta.Executed)
	}
	if alpha.ServerUUID == before.ServerUUID || alpha.ServerUUID == alphaUUID {
		t.Fatalf("server_uuid after the clone = %s, want a new one", alpha.ServerUUID)
	}
	if alpha.ReplConfig || !alpha.ReadOnly || alpha.SuperReadOnly {
		t.Fatalf("recipient after the clone = %+v, want no channel and the read_only boot state", alpha)
	}
	// The site still owns its earlier stream and owns the new one: its
	// receiver drops both.
	c.mu.Lock()
	s := c.byName["alpha"]
	ownsOld, ownsNew, ownsBeta := ownsStream(s, alphaUUID), ownsStream(s, s.serverUUID), ownsStream(s, c.byName["beta"].uuid)
	c.mu.Unlock()
	if !ownsOld || !ownsNew || ownsBeta {
		t.Fatalf("ownsStream old=%v new=%v beta=%v, want true, true, false", ownsOld, ownsNew, ownsBeta)
	}

	// Refused when the clone could not have run.
	c2, _, _ := lossyRejoin(t)
	c2.Crash("beta")
	if c2.Reclone("beta", "alpha") {
		t.Fatal("a clone from a crashed donor was applied")
	}
	c3, _, _ := lossyRejoin(t)
	c3.SetDirLink("alpha", "beta", true)
	if c3.Reclone("beta", "alpha") {
		t.Fatal("a clone was applied although the recipient cannot reach the donor")
	}
	c4, _, _ := lossyRejoin(t)
	c4.SetOperatorLink("alpha", true)
	if c4.Reclone("beta", "alpha") {
		t.Fatal("a clone was applied although the operator cannot reach the recipient")
	}
}

// A regress never destroys the last copy of a transaction, and keeps the
// site's identity.
func TestModel_RegressSite(t *testing.T) {
	c, alphaUUID, _ := ownLossCluster(t)
	before := truthOf(c, "alpha")
	// beta stops fetching, alpha writes on: the newest transactions are
	// on alpha only.
	c.SetFetchStalled("beta", true)
	for i := 0; i < 4; i++ {
		c.Tick()
	}
	if c.RegressSite("alpha", 2) {
		t.Fatal("a regress rolled back transactions only alpha held")
	}
	if got := truthOf(c, "alpha"); !got.Crashed || got.Executed[alphaUUID] != before.Executed[alphaUUID]+4 {
		t.Fatalf("after a skipped regress alpha = %+v, want crashed with its data intact", got)
	}

	c, alphaUUID, _ = ownLossCluster(t)
	before = truthOf(c, "alpha")
	if !c.RegressSite("alpha", 2) {
		t.Fatal("the regress was not applied")
	}
	got := truthOf(c, "alpha")
	if got.Executed[alphaUUID] != before.Executed[alphaUUID]-2 || got.ServerUUID != before.ServerUUID || !got.Crashed {
		t.Fatalf("after the regress alpha = %+v, want two transactions fewer under the same server_uuid, crashed", got)
	}
}

// Over the quick range the regress fault reaches the operator's whole
// answer: sites reported as lacking their own transactions, reclones the
// operator requested and the model applied, and no trial in which such a
// site was made writable beside a readable holder (TestDST_Quick fails on
// that too; here it is counted).
func TestSweep_RegressedSitesAreRecloned(t *testing.T) {
	applied, reclones, reported := 0, 0, 0
	for seed := uint64(1); seed <= 400; seed++ {
		tr := GenerateTrial(seed)
		hasRegress := false
		for _, op := range tr.Ops {
			if op.Kind == OpRegressSite {
				hasRegress = true
			}
		}
		if !hasRegress {
			continue
		}
		r := RunTrial(tr, true, nil)
		for _, v := range r.Violations {
			if v.Invariant == "PromotedWithoutOwnTransactions" {
				t.Errorf("seed %d: %s", seed, v)
			}
		}
		lostOwn := false
		for _, e := range r.Events {
			switch {
			case e.Kind == EvRegress && strings.Contains(e.Detail, "regress=applied") && !strings.Contains(e.Detail, "ownLost=0"):
				lostOwn = true
			case e.Kind == EvReclone && e.Outcome == "applied":
				reclones++
			}
		}
		if lostOwn {
			applied++
		}
		for _, s := range r.FinalStatus.Sites {
			if s.LostGtid != "" {
				reported++
			}
		}
	}
	if applied < 20 {
		t.Fatalf("regresses that lost own transactions: %d over 400 seeds, want at least 20", applied)
	}
	if reclones < 10 {
		t.Fatalf("reclones applied: %d (of %d regresses that lost own transactions), want at least 10", reclones, applied)
	}
	t.Logf("regresses that lost own transactions %d, reclones applied %d, sites still reported at the end %d", applied, reclones, reported)
}

// Seeds on which the regress fault found something while the change was
// written, each kept as a quick regression. They are a convenience: a
// change to the schedule generator moves them, and the durable pins are the
// unit and component tests named here.
//
//   - 66: an emergency failover promoted the regressed candidate although a
//     disaster-recovery site that answered held its lost transactions
//     (test/component: ...WitnessKeepsARegressedCandidateFromPromotion).
//   - 926, 5942: the failed primary answered again in the poll that
//     replaced it and held the candidate's lost transactions; the tracker
//     still called it unreachable, so it was not read (controller:
//     TestOwnLoss_PickerRefusesACandidateOnAWitness reads every site).
//   - 5364: a scripted reclone request for another site displaced the
//     operator's own, which then waited out a retry delay meant for failed
//     clones (controller: TestOwnLoss_FailedRecloneIsRetriedAfterTheDelayOnly).
//   - planned 8, 855, 2548: a planned failover's rollback unfenced a source
//     that had restarted on an older data directory under the same
//     server_uuid (controller: TestRollbackPreconditions).
func TestSweep_OwnLossSeedsStayClean(t *testing.T) {
	for _, seed := range []uint64{33, 66, 926, 5364, 5942} {
		if v := RunTrial(GenerateTrial(seed), false, nil).Violations; len(v) > 0 {
			t.Errorf("seed %d: %v", seed, v)
		}
	}
	for _, seed := range []uint64{8, 855, 2548, 2844} {
		r := RunTrial(GeneratePlannedTrial(seed), false, nil)
		for _, v := range r.Violations {
			if v.Invariant == "PromotedWithoutOwnTransactions" || v.Invariant == "RepointDivergent" || v.Invariant == "NonConvergence" {
				t.Errorf("planned seed %d: %s", seed, v)
			}
		}
	}
}
