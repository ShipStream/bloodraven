package dst

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/shipstream/bloodraven/internal/clock"
	"github.com/shipstream/bloodraven/internal/controller"
	"github.com/shipstream/bloodraven/internal/mysql"
)

func flushTestCluster() (*Cluster, *clock.FakeClock) {
	c := NewCluster([]SiteSpec{
		{Name: "alpha", Host: "mysql-alpha", LoadBalancerIP: "10.0.0.1", UUID: "aaaaaaaa-0000-0000-0000-000000000001"},
		{Name: "beta", Host: "mysql-beta", LoadBalancerIP: "10.0.0.2", UUID: "bbbbbbbb-0000-0000-0000-000000000002"},
	}, 10)
	clk := clock.NewFakeClock(time.Date(2030, 1, 1, 0, 0, 0, 0, time.UTC))
	c.SetClockAdvancer(clk.Advance)
	return c, clk
}

func siteTruth(c *Cluster, name string) SiteTruth {
	for _, t := range c.Truth() {
		if t.Name == name {
			return t
		}
	}
	panic("unknown site " + name)
}

// TestSimFlush_RotatesWithoutChangingTheServer: the modelled FLUSH BINARY
// LOGS opens a file whose Previous_gtids is the executed set, on a fenced
// replica too, changes nothing else, and spends none of the mutation
// faults: a fault armed for the next promotion statement still hits it.
func TestSimFlush_RotatesWithoutChangingTheServer(t *testing.T) {
	c, _ := flushTestCluster()
	ctx := context.Background()
	beta := c.NewChecker("beta")
	before := siteTruth(c, "beta")
	if !before.SuperReadOnly {
		t.Fatal("fixture: beta is not a fenced replica")
	}
	c.AddFailMutations("beta", 1)
	if err := beta.FlushBinaryLogs(ctx); err != nil {
		t.Fatalf("FlushBinaryLogs on a fenced replica: %v", err)
	}
	after := siteTruth(c, "beta")
	if after.BinlogSeq != before.BinlogSeq+1 || after.BinlogPrev.String() != after.Executed.String() {
		t.Fatalf("after the flush: file %d prev %s, want file %d prev %s", after.BinlogSeq, after.BinlogPrev, before.BinlogSeq+1, after.Executed)
	}
	if after.Executed.String() != before.Executed.String() || !after.ReadOnly || !after.SuperReadOnly || !after.ReplConfig {
		t.Fatalf("the flush changed the server: %+v", after)
	}
	if err := beta.StopReplica(ctx); !errors.Is(err, errNotApplied) {
		t.Fatalf("StopReplica after the flush = %v; the flush used up the fault armed for a promotion statement", err)
	}
}

// TestSimFlush_Faults: a failing flush rotates nothing; a slow one takes
// the operator's whole bound of fake time and answers only when abandoned;
// a late one is completed by the server at the next tick, on whatever the
// site has become.
func TestSimFlush_Faults(t *testing.T) {
	ctx := context.Background()
	t.Run("failing", func(t *testing.T) {
		c, _ := flushTestCluster()
		c.AddFlushFailures("beta", 1)
		beta := c.NewChecker("beta")
		if err := beta.FlushBinaryLogs(ctx); !errors.Is(err, errFlushFailed) {
			t.Fatalf("FlushBinaryLogs = %v, want the injected failure", err)
		}
		if got := siteTruth(c, "beta").BinlogSeq; got != 0 {
			t.Fatalf("a failed flush rotated the binary log (file %d)", got)
		}
		if err := beta.FlushBinaryLogs(ctx); err != nil {
			t.Fatalf("the fault was good for one flush; the next = %v", err)
		}
	})
	t.Run("unreachable", func(t *testing.T) {
		c, _ := flushTestCluster()
		c.Crash("beta")
		if err := c.NewChecker("beta").FlushBinaryLogs(ctx); err == nil {
			t.Fatal("FlushBinaryLogs on a crashed site returned nil")
		}
	})
	for _, late := range []bool{false, true} {
		name := "slow"
		if late {
			name = "slowThenLate"
		}
		t.Run(name, func(t *testing.T) {
			c, clk := flushTestCluster()
			c.AddSlowFlushes("beta", 1, late)
			beta := c.NewChecker("beta")
			start := clk.Now()
			flushCtx, cancel := context.WithCancel(ctx)
			timer := clk.NewTimer(controller.BinlogRotationTimeout)
			done := make(chan error, 1)
			go func() { done <- beta.FlushBinaryLogs(flushCtx) }()
			select {
			case <-timer.C():
			case err := <-done:
				t.Fatalf("a slow flush answered (%v) before the operator's bound", err)
			case <-time.After(5 * time.Second):
				t.Fatal("a slow flush never advanced the fake clock to the operator's bound")
			}
			if got := clk.Since(start); got != controller.BinlogRotationTimeout {
				t.Fatalf("the slow flush took %s of fake time, want %s", got, controller.BinlogRotationTimeout)
			}
			cancel()
			if err := <-done; !errors.Is(err, context.Canceled) {
				t.Fatalf("the abandoned flush = %v, want its context's cancellation", err)
			}
			if got := siteTruth(c, "beta").BinlogSeq; got != 0 {
				t.Fatalf("an abandoned flush rotated at once (file %d)", got)
			}
			// The operator moves on and enables writes; then the data
			// plane ticks.
			if err := beta.StopReplica(ctx); err != nil {
				t.Fatal(err)
			}
			if err := beta.ResetReplicaAll(ctx); err != nil {
				t.Fatal(err)
			}
			if err := beta.SetReadOnly(ctx, false); err != nil {
				t.Fatal(err)
			}
			c.Tick()
			st := siteTruth(c, "beta")
			switch {
			case late && st.BinlogSeq != 1:
				t.Fatalf("the late rotation did not land at the next tick (file %d)", st.BinlogSeq)
			case !late && st.BinlogSeq != 0:
				t.Fatalf("a rotation landed although the server never completed it (file %d)", st.BinlogSeq)
			}
			if !st.Writable() {
				t.Fatal("the site is not writable after the promotion statements")
			}
		})
	}
}

// crashPrimaryTrial is a two-site trial whose only scheduled fault is the
// primary's crash, plus the given rotation fault on the candidate.
func crashPrimaryTrial(extra ...Op) Trial {
	tr := GenerateTrial(1)
	tr.SiteNames = []string{"alpha", "beta"}
	tr.Roles = tr.Roles[:2]
	for i := range tr.Roles {
		tr.Roles[i] = "primary-candidate"
	}
	tr.Priorities = nil
	tr.SeedHistory = false
	tr.Ops = append(append([]Op{}, extra...), Op{At: tr.WarmupPolls + 2, Kind: OpCrash, Site: "alpha", Fenced: true})
	tr.Skip = nil
	return tr
}

// promotionEvents returns the first rotation and the first read_only clear
// the operator sent to site.
func promotionEvents(events []Event, site string) (flush *Event, grant *Event) {
	for i := range events {
		e := &events[i]
		if e.Site != site {
			continue
		}
		switch {
		case e.Kind == EvFlushBinlog && e.Outcome != "late" && flush == nil:
			flush = e
		case e.Kind == EvSetRO && e.Detail == "on=false" && effectiveOutcome(e.Outcome) && grant == nil:
			grant = e
		}
	}
	return flush, grant
}

// TestTrial_PromotionRotatesAtThePromotionGTID: an emergency promotion in a
// trial rotates the candidate's binary log before its writes are enabled,
// and the file that rotation opens starts at exactly the set the operator
// records as the promotion GTID (#269 relies on it). With the rotation
// failing or slow, the promotion completes the same and records the same
// set; only the file boundary is missing or late.
func TestTrial_PromotionRotatesAtThePromotionGTID(t *testing.T) {
	cases := []struct {
		name    string
		fault   []Op
		outcome string // the candidate's flush event outcome
		rotated int    // beta's binlog files at the end
	}{
		{name: "acknowledged", outcome: "", rotated: 1},
		{name: "failing", fault: []Op{{At: 8, Kind: OpFailFlush, Site: "beta", N: 1}}, outcome: "failed", rotated: 0},
		{name: "slow", fault: []Op{{At: 8, Kind: OpSlowFlush, Site: "beta", N: 1}}, outcome: "timeout", rotated: 0},
		{name: "slowThenLate", fault: []Op{{At: 8, Kind: OpSlowFlush, Site: "beta", N: 1, K: 1}}, outcome: "timeout", rotated: 1},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			tr := crashPrimaryTrial(tc.fault...)
			r := RunTrial(tr, true, nil)
			if len(r.Violations) > 0 {
				t.Fatalf("violations: %v\n%s", r.Violations, tr)
			}
			if r.FinalStatus.ActiveSite != "beta" {
				t.Fatalf("active site %q, want beta promoted", r.FinalStatus.ActiveSite)
			}
			flush, grant := promotionEvents(r.Events, "beta")
			if flush == nil || grant == nil {
				t.Fatalf("no rotation or no write grant on beta: flush=%v grant=%v", flush, grant)
			}
			if flush.Outcome != tc.outcome {
				t.Fatalf("rotation outcome %q, want %q", flush.Outcome, tc.outcome)
			}
			if flush.Seq > grant.Seq || flush.Poll != grant.Poll {
				t.Fatalf("the rotation (poll %d seq %d) did not precede the write grant (poll %d seq %d) in its promotion",
					flush.Poll, flush.Seq, grant.Poll, grant.Seq)
			}
			if r.FinalFailoverRecord == nil || r.FinalFailoverRecord.LastFailoverTarget != "beta" {
				t.Fatalf("failover record = %+v, want beta", r.FinalFailoverRecord)
			}
			recorded, err := mysql.ParseGTIDSet(r.FinalFailoverRecord.PromotionGtidExecuted)
			if err != nil || recorded.IsEmpty() {
				t.Fatalf("recorded promotion GTID %q: %v", r.FinalFailoverRecord.PromotionGtidExecuted, err)
			}
			var beta SiteTruth
			for _, s := range r.FinalTruth {
				if s.Name == "beta" {
					beta = s
				}
			}
			if beta.BinlogSeq != tc.rotated {
				t.Fatalf("beta ended with %d rotations, want %d", beta.BinlogSeq, tc.rotated)
			}
			if tc.outcome != "" {
				return
			}
			_, prevText, ok := strings.Cut(flush.Detail, "prev=")
			if !ok {
				t.Fatalf("rotation event %q carries no Previous_gtids", flush.Detail)
			}
			prev, err := mysql.ParseGTIDSet(prevText)
			if err != nil {
				t.Fatal(err)
			}
			if !prev.Contains(recorded) || !recorded.Contains(prev) {
				t.Fatalf("the rotated file starts at %s, the recorded promotion GTID is %s", prevText, r.FinalFailoverRecord.PromotionGtidExecuted)
			}
		})
	}
}

// TestSweep_RotationFaultsReachPromotions: over the shared plain sweep the
// rotation is seen acknowledged, failing, abandoned and completed late, so
// the quick sweep's invariants are judged with every outcome in play.
func TestSweep_RotationFaultsReachPromotions(t *testing.T) {
	seen := map[string]int{}
	for seed := uint64(1); seed <= actorSweepSeeds; seed++ {
		for _, key := range sweepTrial(seed).eventKeys {
			if strings.HasPrefix(key, string(EvFlushBinlog)) {
				seen[key]++
			}
		}
	}
	// "unreachable" is not in the list: without the actors nothing runs
	// between a promotion's reset and its rotation, so a site that took the
	// reset still answers the rotation.
	for _, key := range []string{"flushBinaryLogs", "flushBinaryLogs:failed", "flushBinaryLogs:timeout", "flushBinaryLogs:late"} {
		if seen[key] == 0 {
			t.Errorf("no trial in seeds 1..%d has a %s event; the rotation outcome is not exercised (seen: %v)", actorSweepSeeds, key, seen)
		}
	}
}

// TestPlannedSweep_RotationFaultsReachRunsAndHandoffs: with the actors on,
// the planned failover's two rotations (the fenced source, the target) and
// the ordered update's are sent, and each meets a failing and an abandoned
// flush somewhere in the range, with no violation outside the allowed
// classes.
func TestPlannedSweep_RotationFaultsReachRunsAndHandoffs(t *testing.T) {
	const seeds = 300
	seen := map[string]int{}
	for seed := uint64(1); seed <= seeds; seed++ {
		r := RunTrial(GeneratePlannedTrial(seed), true, nil)
		if fp := fingerprint(r.Violations); fp != "" && !allowedFingerprint(fp) {
			t.Fatalf("seed %d: %s: %v", seed, fp, r.Violations)
		}
		for _, e := range r.Events {
			if e.Kind != EvFlushBinlog || e.Outcome == "late" {
				continue
			}
			outcome := "rotated"
			if e.Outcome != "" {
				outcome = "notRotated"
			}
			seen[string(effectiveActor(e.Actor))+":"+outcome]++
		}
	}
	for _, key := range []string{"pf:rotated", "pf:notRotated", "upd:rotated", "upd:notRotated", "tm:rotated", "tm:notRotated"} {
		if seen[key] == 0 {
			t.Errorf("no %s rotation in planned seeds 1..%d (seen: %v)", key, seeds, seen)
		}
	}
}

// TestSweep_PreRecloneWaitRotatesTheRecipient: the scripted reclone
// requests reach the manager's pre-reclone step. With a sidecar that
// answers, the manager rotates the recipient's binary log outside any
// promotion and then waits; no wait reaches its bound (the harness
// withdraws the request first, CLONE not being modelled), and no trial
// reports a violation for it.
func TestSweep_PreRecloneWaitRotatesTheRecipient(t *testing.T) {
	rotations, waits := 0, 0
	for seed := uint64(1); seed <= 300; seed++ {
		tr := GenerateTrial(seed)
		var op *Op
		for i := range tr.Ops {
			if tr.Ops[i].Kind == OpRecloneWait && tr.Ops[i].K != 0 {
				op = &tr.Ops[i]
			}
		}
		if op == nil {
			continue
		}
		waits++
		r := RunTrial(tr, true, nil)
		for _, v := range r.Violations {
			if v.Invariant == "HarnessRecloneWaitExpired" || v.Invariant == "FlushAfterWriteGrant" {
				t.Fatalf("seed %d: %v\n%s", seed, v, tr)
			}
		}
		promoting := map[int]bool{} // polls with a promotion statement on the site
		for _, e := range r.Events {
			if e.Site == op.Site && (e.Kind == EvStopReplica || e.Kind == EvResetReplica) {
				promoting[e.Poll] = true
			}
		}
		for _, e := range r.Events {
			if e.Site == op.Site && e.Kind == EvFlushBinlog && e.Outcome != "late" && effectiveActor(e.Actor) == ActorTM &&
				e.Poll >= op.At && e.Poll < op.At+op.N && !promoting[e.Poll] {
				rotations++
			}
		}
	}
	if waits < 30 || rotations < 20 {
		t.Fatalf("%d scripted reclone waits produced %d pre-reclone rotations; the step is not being reached", waits, rotations)
	}
}

// TestCheckEnd_CooldownIsJudgedOnTheClock: a slow rotation takes fake time
// inside a poll, so two promotions fewer polls apart than the cooldown can
// still be a full cooldown apart on the operator's clock. CooldownViolated
// judges the clock windows of the two polls: a violation only when the
// later poll ended less than a cooldown after the earlier one began.
func TestCheckEnd_CooldownIsJudgedOnTheClock(t *testing.T) {
	t0 := time.Date(2030, 1, 1, 0, 0, 0, 0, time.UTC)
	at := func(sec int) time.Time { return t0.Add(time.Duration(sec) * time.Second) }
	for _, tc := range []struct {
		name  string
		spans []clockSpan
		want  bool
	}{
		{"nine polls and nine seconds apart", []clockSpan{{at(25), at(25)}, {at(34), at(34)}}, true},
		// Two slow rotations between the promotions and one inside the
		// second promotion's poll: nine polls, twelve seconds.
		{"nine polls but twelve seconds apart", []clockSpan{{at(25), at(26)}, {at(36), at(37)}}, false},
	} {
		tr := twoSiteHandTrial(15, true, 10)
		r := &trialRunner{trial: tr, logger: discardLogger(), operatorDeadUntil: -1,
			kindsSeen: map[string]struct{}{}, reasonsSeen: map[string]struct{}{}}
		r.setup(false)
		r.promotionPolls = []int{25, 34}
		r.promotionSpans = tc.spans
		got := false
		for _, v := range r.checkEnd() {
			if v.Invariant == "CooldownViolated" {
				got = true
			}
		}
		if got != tc.want {
			t.Errorf("%s (cooldown 10s): CooldownViolated=%v, want %v", tc.name, got, tc.want)
		}
	}
}
