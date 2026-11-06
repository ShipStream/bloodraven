package dst

import (
	"context"
	"errors"
	"log/slog"
	"runtime"
	"sort"
	"strings"
	"sync"
	"testing"

	"k8s.io/apimachinery/pkg/types"

	v1alpha1 "github.com/shipstream/bloodraven/api/v1alpha1"
	"github.com/shipstream/bloodraven/internal/controller"
	"github.com/shipstream/bloodraven/internal/mysql"
)

// The fidelity tests (§15.1) pin the model properties the planned-failover
// invariants rest on. A model that silently stops pinning sessions, or a
// scheduler that never interleaves, would make every planned trial green
// for the wrong reason.

func pfCtx() context.Context { return WithActor(context.Background(), ActorPF) }

func discardLogger() *slog.Logger { return slog.New(slog.DiscardHandler) }

func mustPin(t *testing.T, c *Cluster, site string) mysql.Session {
	t.Helper()
	sess, err := c.NewChecker(site).PinSession(pfCtx())
	if err != nil {
		t.Fatalf("pin %s: %v", site, err)
	}
	return sess
}

// TestSimSession_FailsAfterReplacementBetweenProofAndWrite: pin, prove,
// replace the pod, then expect the write to fail without reaching either
// server; a pooled call reaches the new pod.
func TestSimSession_FailsAfterReplacementBetweenProofAndWrite(t *testing.T) {
	c := twoSiteCluster()
	ctx := pfCtx()
	sess := mustPin(t, c, "beta")

	// The proof: read-only, the identity and the executed set, all on the
	// pinned connection.
	ro, err := sess.CheckReadOnly(ctx)
	if err != nil || !ro {
		t.Fatalf("proof: read_only=%v err=%v", ro, err)
	}
	snap, err := sess.ReadGTIDSnapshot(ctx)
	if err != nil || snap.ServerUUID != "00000000-0000-0000-0000-000000000002" {
		t.Fatalf("proof: snapshot %+v err=%v", snap, err)
	}

	c.ReplacePod("beta", 0)

	if err := sess.SetSuperReadOnly(ctx, false); !errors.Is(err, errSessionLost) {
		t.Fatalf("SET super_read_only after replacement: err=%v, want errSessionLost", err)
	}
	if err := sess.SetReadOnly(ctx, false); !errors.Is(err, errSessionLost) {
		t.Fatalf("SET read_only after replacement: err=%v, want errSessionLost", err)
	}
	if _, err := sess.GetGtidExecuted(ctx); !errors.Is(err, errSessionLost) {
		t.Fatalf("read after replacement: err=%v, want errSessionLost", err)
	}
	beta := truthNamed(c.Truth(), "beta")
	if !beta.ReadOnly || beta.SuperReadOnly {
		t.Fatalf("replaced pod must boot read_only=ON super_read_only=OFF and stay untouched: %+v", beta)
	}
	for _, e := range c.PollEvents() {
		if e.Site == "beta" && (e.Kind == EvSetRO || e.Kind == EvSetSuperRO) && effectiveOutcome(e.Outcome) {
			t.Fatalf("a pinned write reached the new pod: %s", e)
		}
	}

	// The pool redials: the pooled checker reaches the new incarnation.
	if ro, err := c.NewChecker("beta").CheckReadOnly(ctx); err != nil || !ro {
		t.Fatalf("pooled read after replacement: ro=%v err=%v", ro, err)
	}
	// And a fresh pin is on the new incarnation.
	fresh := mustPin(t, c, "beta")
	if _, err := fresh.GetGtidExecuted(ctx); err != nil {
		t.Fatalf("fresh pin: %v", err)
	}
}

// TestSimSession_FailsAfterRestart: a crash and recovery, or an in-place
// restart, also break the pinned connection.
func TestSimSession_FailsAfterRestart(t *testing.T) {
	for name, restart := range map[string]func(c *Cluster){
		"crashRecover": func(c *Cluster) { c.Crash("beta"); c.Recover("beta", false, false) },
		"restartSite":  func(c *Cluster) { c.RestartSite("beta") },
	} {
		t.Run(name, func(t *testing.T) {
			c := twoSiteCluster()
			sess := mustPin(t, c, "beta")
			restart(c)
			if err := sess.StopReplica(pfCtx()); !errors.Is(err, errSessionLost) {
				t.Fatalf("err=%v, want errSessionLost", err)
			}
		})
	}
}

// TestSimSession_ImplementsTheSessionContract: every Session method runs on
// the pinned server, RESET REPLICA ALL leaves no channel, super_read_only is
// distinct from read_only, and Close ends the session.
func TestSimSession_ImplementsTheSessionContract(t *testing.T) {
	c := twoSiteCluster()
	ctx := pfCtx()
	sess := mustPin(t, c, "beta")

	if sro, err := sess.CheckSuperReadOnly(ctx); err != nil || !sro {
		t.Fatalf("replica super_read_only=%v err=%v", sro, err)
	}
	if rs, err := sess.ShowReplicaStatus(ctx); err != nil || rs == nil || rs.SourceHost != "alpha.mysql.sim" {
		t.Fatalf("replica status %+v err=%v", rs, err)
	}
	for _, step := range []func() error{
		func() error { return sess.StopReplica(ctx) },
		func() error { return sess.ResetReplicaAll(ctx) },
	} {
		if err := step(); err != nil {
			t.Fatal(err)
		}
	}
	if rs, err := sess.ShowReplicaStatus(ctx); err != nil || rs != nil {
		t.Fatalf("after RESET REPLICA ALL: status %+v err=%v, want nil, nil", rs, err)
	}
	if err := sess.SetSuperReadOnly(ctx, false); err != nil {
		t.Fatal(err)
	}
	ro, _ := sess.CheckReadOnly(ctx)
	sro, _ := sess.CheckSuperReadOnly(ctx)
	if !ro || sro {
		t.Fatalf("after SRO=OFF: read_only=%v super_read_only=%v, want true/false", ro, sro)
	}
	if err := sess.ChangeReplicationSource(ctx, mysql.ReplicationSourceOpts{Host: "alpha.mysql.sim"}); err != nil {
		t.Fatal(err)
	}
	if err := sess.StartReplica(ctx); err != nil {
		t.Fatal(err)
	}
	if err := sess.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := sess.CheckReadOnly(ctx); !errors.Is(err, errSessionClosed) {
		t.Fatalf("after Close: err=%v, want errSessionClosed", err)
	}
	// Pins by the planned actor are evented, attributed and stamped.
	pins := eventsOfKind(c.PollEvents(), EvPinSession)
	if len(pins) != 1 || pins[0].Actor != ActorPF || pins[0].Incarnation != 0 {
		t.Fatalf("pin events %v", pins)
	}
	for _, e := range c.PollEvents() {
		if e.Kind == EvStopReplica && e.Actor != ActorPF {
			t.Fatalf("statement not attributed to the pinning actor: %s", e)
		}
	}
}

// TestReplacePod_RegressAndBootSemantics: a regressing replacement rolls a
// read-only site back under a fresh server_uuid that its own later writes
// use; a writable site, or the sole holder of a transaction, is not
// regressed.
func TestReplacePod_RegressAndBootSemantics(t *testing.T) {
	c := twoSiteCluster()
	for range 5 {
		c.Tick()
	}
	before := truthNamed(c.Truth(), "beta")
	c.ReplacePod("beta", 3)
	after := truthNamed(c.Truth(), "beta")
	if after.Incarnation != before.Incarnation+1 {
		t.Fatalf("incarnation %d → %d", before.Incarnation, after.Incarnation)
	}
	if after.ServerUUID == before.ServerUUID {
		t.Fatal("a regressed data directory must boot with a fresh server_uuid")
	}
	if _, err := mysql.ParseGTIDSet(after.ServerUUID + ":1"); err != nil {
		t.Fatalf("replacement uuid %q is not a valid GTID source: %v", after.ServerUUID, err)
	}
	if after.Executed.contains(before.Executed) {
		t.Fatalf("regress did not roll back: before %s after %s", before.Executed, after.Executed)
	}
	if !after.ReadOnly || after.SuperReadOnly || !after.ReplConfig {
		t.Fatalf("boot semantics: %+v", after)
	}

	// The writable primary is never regressed.
	c.ReplacePod("alpha", 3)
	alpha := truthNamed(c.Truth(), "alpha")
	if alpha.ServerUUID != "00000000-0000-0000-0000-000000000001" || !alpha.ReadOnly {
		t.Fatalf("writable primary regressed or not booted read-only: %+v", alpha)
	}
	if ev := eventsOfKind(c.PollEvents(), EvReplacePod); !strings.Contains(ev[len(ev)-1].Detail, "skipped(writable)") {
		t.Fatalf("writable regress not reported skipped: %v", ev)
	}

	// Writes after a regressed site is promoted land in its new stream.
	c.RogueWritable("beta")
	c.Tick()
	beta := truthNamed(c.Truth(), "beta")
	if beta.Executed[beta.ServerUUID] != 1 {
		t.Fatalf("own writes not in the new stream: %s", beta.Executed)
	}

	// The sole holder of a transaction is not regressed.
	c2 := twoSiteCluster()
	c2.SetFetchStalled("beta", true)
	for range 3 {
		c2.Tick()
	}
	c2.RogueFence("alpha")
	c2.ReplacePod("alpha", 2)
	if ev := eventsOfKind(c2.PollEvents(), EvReplacePod); !strings.Contains(ev[0].Detail, "skipped(sole holder)") {
		t.Fatalf("sole-holder regress not skipped: %v", ev)
	}
}

// slowCluster is a two-site cluster whose pf SET super_read_only on beta
// is slow, with yield running at each of its two yields.
func slowCluster(yield func(c *Cluster, point string)) *Cluster {
	c := twoSiteCluster()
	c.setPlannedHooks(plannedHooks{
		yield: func(point string) { yield(c, point) },
		slow: func(site string, kind EventKind, actor Actor) bool {
			return site == "beta" && kind == EvSetSuperRO && actor == ActorPF
		},
	})
	return c
}

// TestSlowStatement_YieldsInsideTheStatement: a slow statement yields with
// Cluster.mu released between issue and apply and between apply and ack;
// a replacement in the first window keeps it from applying, in the second
// it applied and the reply is lost.
func TestSlowStatement_YieldsInsideTheStatement(t *testing.T) {
	t.Run("clean", func(t *testing.T) {
		var points []string
		c := slowCluster(func(c *Cluster, point string) {
			if !c.tryLockCluster() {
				t.Fatalf("yield at %s with Cluster.mu held", point)
			}
			points = append(points, point)
		})
		sess := mustPin(t, c, "beta")
		if err := sess.SetSuperReadOnly(pfCtx(), false); err != nil {
			t.Fatal(err)
		}
		want := []string{YieldStmtIssued + ":beta:setSuperReadOnly", YieldStmtApplied + ":beta:setSuperReadOnly"}
		if strings.Join(points, ",") != strings.Join(want, ",") {
			t.Fatalf("yields %v, want %v", points, want)
		}
		// A pooled tm statement is never slow.
		points = nil
		if err := c.NewChecker("beta").SetSuperReadOnly(context.Background(), true); err != nil || len(points) != 0 {
			t.Fatalf("tm statement yielded %v (err %v)", points, err)
		}
	})
	t.Run("replacedBeforeApply", func(t *testing.T) {
		c := slowCluster(func(c *Cluster, point string) {
			if yieldClass(point) == YieldStmtIssued {
				c.ReplacePod("beta", 0)
			}
		})
		sess := mustPin(t, c, "beta")
		c.RogueFence("beta") // make the SRO=OFF observable
		err := sess.SetSuperReadOnly(pfCtx(), false)
		if !errors.Is(err, errSessionLost) {
			t.Fatalf("err=%v, want errSessionLost", err)
		}
		if ev := eventsOfKind(c.PollEvents(), EvSetSuperRO); ev[len(ev)-1].Outcome != "lostBeforeApply" {
			t.Fatalf("events %v", ev)
		}
	})
	t.Run("replacedBeforeAck", func(t *testing.T) {
		applied := false
		c := slowCluster(func(c *Cluster, point string) {
			if yieldClass(point) == YieldStmtApplied {
				applied = !truthNamed(c.Truth(), "beta").SuperReadOnly
				c.ReplacePod("beta", 0)
			}
		})
		sess := mustPin(t, c, "beta")
		err := sess.SetSuperReadOnly(pfCtx(), false)
		if !errors.Is(err, errAmbiguous) || !applied {
			t.Fatalf("err=%v applied=%v, want errAmbiguous and applied", err, applied)
		}
		var outcomes []string
		for _, e := range eventsOfKind(c.PollEvents(), EvSetSuperRO) {
			outcomes = append(outcomes, e.Outcome)
		}
		if strings.Join(outcomes, ",") != ",ack:responseLost" {
			t.Fatalf("outcomes %q: the apply must be one effective event, the lost ack a second", outcomes)
		}
	})
}

// TestScheduler_YieldDiscipline: a yield with a tracked mutex held panics,
// naming it.
func TestScheduler_YieldDiscipline(t *testing.T) {
	c := twoSiteCluster()
	s := newYieldScheduler(1)
	s.trackLock("Cluster.mu", c.tryLockCluster)
	s.Yield("engine.load") // free: fine
	defer func() {
		p := recover()
		if p == nil || !strings.Contains(p.(string), "while holding Cluster.mu") {
			t.Fatalf("panic %v, want a yield-while-holding panic", p)
		}
	}()
	c.mu.Lock()
	defer c.mu.Unlock()
	s.Yield("engine.write")
}

// TestScheduler_InterleavesReentrancyAndDepth: at a yield other actors step,
// an actor on the stack never re-enters, nesting stops at schedMaxDepth, and
// the choices replay from the seed.
func TestScheduler_InterleavesReentrancyAndDepth(t *testing.T) {
	run := func(seed uint64) (trace []string, maxDepth int) {
		s := newYieldScheduler(seed)
		for _, name := range []string{"a", "b", "c"} {
			s.addActor(&schedActor{name: name, pct: 100, step: func(context.Context) {
				trace = append(trace, name)
				maxDepth = max(maxDepth, s.depth)
				s.Yield("engine.write:" + name)
			}})
		}
		s.run("a", func() { s.Yield(YieldEngineLoad) })
		if s.stepsBy["a"] != 0 {
			t.Fatalf("actor a re-entered from its own yield")
		}
		return trace, maxDepth
	}
	trace, depth := run(7)
	if len(trace) == 0 {
		t.Fatal("no actor stepped at a yield")
	}
	if depth > schedMaxDepth {
		t.Fatalf("nesting reached depth %d, bound %d", depth, schedMaxDepth)
	}
	again, _ := run(7)
	if strings.Join(trace, ",") != strings.Join(again, ",") {
		t.Fatalf("scheduler not deterministic: %v vs %v", trace, again)
	}
	// With pct 0 nothing steps.
	s := newYieldScheduler(1)
	s.addActor(&schedActor{name: "never", pct: 0, step: func(context.Context) { t.Fatal("stepped at pct 0") }})
	s.Yield(YieldEngineLoad)
}

// TestRegistry_SlotSharedByGenerationsAndDroppedByRestart: a replacement in
// the same process shares the slot; a restart (a new registry) does not; a
// retiring manager polls for its window only when the retire seams are
// wired; absence hides the current manager.
func TestRegistry_SlotSharedByGenerationsAndDroppedByRestart(t *testing.T) {
	r := &trialRunner{trial: twoSiteHandTrial(15, true, 20), logger: discardLogger(), operatorDeadUntil: -1,
		kindsSeen: map[string]struct{}{}, reasonsSeen: map[string]struct{}{}}
	r.setup(false)
	nn := types.NamespacedName{Namespace: "sim", Name: "sim"}
	g := newSimRegistry(nn)
	first, second := r.tm, r.newManager(nil)
	g.register(first, controller.ManagerWiring{})
	slot := g.Slot(nn)
	if slot == nil || g.Snapshot(nn).Slot != slot {
		t.Fatal("slot missing from the snapshot")
	}

	if g.replace(second, controller.ManagerWiring{}, 3, nil) {
		t.Fatal("replace honoured the in-flight window without the retire seams")
	}
	if got := g.retiringToPoll(); len(got) != 0 {
		t.Fatalf("an unretirable manager kept polling: %d", len(got))
	}
	if g.Slot(nn) != slot || g.Current() != second {
		t.Fatal("replacement must share the process's slot")
	}

	retired := 0
	g.retire = func(*controller.TopologyManager) { retired++ }
	g.changingSites = func(*controller.TopologyManager) bool { return false }
	third := r.newManager(nil)
	if !g.replace(third, controller.ManagerWiring{}, 2, nil) || retired != 1 {
		t.Fatalf("seamed replace: retired=%d", retired)
	}
	if snap := g.Snapshot(nn); len(snap.Retiring) != 1 || snap.Retiring[0] != second || snap.Current != third {
		t.Fatalf("snapshot after replace: %+v", snap)
	}
	for i := range 2 {
		if got := g.retiringToPoll(); len(got) != 1 || got[0] != second {
			t.Fatalf("poll %d: retiring polls %v", i, got)
		}
	}
	if got := g.retiringToPoll(); len(got) != 0 || g.retiringCount() != 0 {
		t.Fatalf("retiring manager outlived its window")
	}

	// An absence retires the current manager (its replacement's starts
	// fail) and stashes its record; the window's end is reported, and the
	// harness starts a fresh manager then.
	third.SetLastFailover(r.clk.Now())
	third.SetLastFailoverTarget("beta")
	g.setAbsent(2, nil)
	if g.Current() != nil || g.Snapshot(nn).Current != nil || !g.absent() {
		t.Fatal("absent manager still visible")
	}
	if snap := g.Snapshot(nn); len(snap.Retiring) != 1 || snap.Retiring[0] != third {
		t.Fatalf("absence did not retire the current manager: %+v", snap)
	}
	if g.endPoll() || !g.endPoll() || g.absent() {
		t.Fatal("absence window did not end after 2 polls")
	}
	g.retiringToPoll() // drops the retired manager: its record stays stashed
	fourth := r.newManager(nil)
	g.register(fourth, controller.ManagerWiring{})
	if g.Current() != fourth {
		t.Fatal("a start after the absence did not register")
	}
	if rec, ok := fourth.FailoverRecordForSim(); !ok || rec.LastFailoverTarget != "beta" {
		t.Fatalf("the retired manager's stashed record did not reach the fresh start: %+v ok=%v (K185)", rec, ok)
	}

	restarted := newSimRegistry(nn)
	if restarted.Slot(nn) == slot {
		t.Fatal("a restarted process must not inherit the slot")
	}
	if g.Slot(types.NamespacedName{Name: "other"}) == slot {
		t.Fatal("another group shares the slot")
	}
	g.shutdown()
	if g.LeaderContext().Err() == nil {
		t.Fatal("shutdown did not cancel the leader context")
	}
}

// TestPlannedTrial_ExercisesTheActorOps: over a fixed range the actors
// reach every part of the machinery: runs of every outcome class through the
// production engine, ordered updates, manager replacements with an in-flight
// window, pod replacements (some regressed), late out-of-band replays and
// restarted promoted targets. A generator or harness change that quietly
// drops one fails here.
func TestPlannedTrial_ExercisesTheActorOps(t *testing.T) {
	counts := map[string]int{}
	outcomes := map[string]int{}
	replacedPods, regressed := 0, 0
	for seed := uint64(1); seed <= 150; seed++ {
		r := RunTrial(GeneratePlannedTrial(seed), true, nil)
		for k, v := range r.Planned.Counts {
			counts[k] += v
		}
		for _, o := range r.Planned.Outcomes {
			outcomes[o]++
		}
		if n := r.Planned.Unsupported[OpReplaceManager]; n > 0 {
			t.Fatalf("seed %d: %d manager replacements without the in-flight window", seed, n)
		}
		for _, e := range r.Events {
			if e.Kind == EvReplacePod {
				replacedPods++
				if strings.Contains(e.Detail, "server_uuid=") {
					regressed++
				}
			}
		}
	}
	for _, k := range []string{"enginePasses", "updatesStarted", "managersReplaced", "oobReplayed", "promotedTargetRestarted"} {
		if counts[k] == 0 {
			t.Errorf("counter %s never moved (counts %v)", k, counts)
		}
	}
	if replacedPods == 0 || regressed == 0 {
		t.Errorf("pod replacements %d, regressed %d", replacedPods, regressed)
	}
	for _, o := range []v1alpha1.PlannedFailoverOutcome{
		v1alpha1.PlannedFailoverOutcomeSucceeded, v1alpha1.PlannedFailoverOutcomeNotStarted,
		v1alpha1.PlannedFailoverOutcomeRolledBack,
	} {
		if outcomes[string(o)] == 0 {
			t.Errorf("no run ended %s (outcomes %v)", o, outcomes)
		}
	}
	t.Logf("outcomes %v counts %v", outcomes, counts)
}

// TestPlannedTrial_SnapshotWritesContendOnTheObject: the topology manager's
// status writes land on the simulated object through the production merge
// and move its resourceVersion, so they contend with the run's writes.
func TestPlannedTrial_SnapshotWritesContendOnTheObject(t *testing.T) {
	tr := twoSiteHandTrial(15, true, 20)
	tr.Seed = 0
	tr.Actors = Actors{Enabled: true}
	r := RunTrial(tr, false, nil)
	obj := r.Planned.Object
	if obj == nil || obj.Status.ActiveSite != "alpha" {
		t.Fatalf("snapshot not merged into the object: %+v", obj)
	}
	if rv := parseRV(obj.ResourceVersion); rv < 2 {
		t.Fatalf("resourceVersion %d never moved", rv)
	}
	if len(r.Violations) != 0 {
		t.Fatalf("fault-free actors-on trial: %v", r.Violations)
	}
}

// plannedSweepSeeds is the range the planned fidelity tests share: wide
// enough that every terminal outcome, the long-outage deadlines and a
// WasWritable re-promotion all occur in it.
const plannedSweepSeeds = 400

var plannedSweepOnce = sync.OnceValues(func() (map[string]int, map[string]int) {
	y, s, c := plannedSweepOf(plannedSweepSeeds)
	for k, v := range c {
		s["count:"+k] = v
	}
	return y, s
})

// plannedSweep aggregates the planned reports of seeds 1..plannedSweepSeeds
// once per package run.
func plannedSweep() (yields, steps, counts map[string]int) {
	y, s := plannedSweepOnce()
	counts = map[string]int{}
	steps = map[string]int{}
	for k, v := range s {
		if strings.HasPrefix(k, "count:") {
			counts[strings.TrimPrefix(k, "count:")] = v
		} else {
			steps[k] = v
		}
	}
	return y, steps, counts
}

// plannedSweepOf aggregates the planned reports of seeds 1..n.
func plannedSweepOf(n uint64) (yields, steps, counts map[string]int) {
	return plannedSweepRange(1, n)
}

// plannedSweepRange aggregates the planned reports of seeds from..to. The
// trials run in parallel (runBatch, as the campaign runs them); the sums do
// not depend on the order they finish in.
func plannedSweepRange(from, to uint64) (yields, steps, counts map[string]int) {
	yields, steps, counts = map[string]int{}, map[string]int{}, map[string]int{}
	if to < from {
		return yields, steps, counts
	}
	for _, r := range runBatch(from, int(to-from+1), runtime.GOMAXPROCS(0), GeneratePlannedTrial) {
		for k, v := range r.Planned.Yields {
			yields[k] += v
		}
		for k, v := range r.Planned.Steps {
			steps[k] += v
		}
		for k, v := range r.Planned.Counts {
			counts[k] += v
		}
		for _, o := range r.Planned.Outcomes {
			counts["outcome:"+o]++
		}
		for _, pf := range r.Planned.Terminals {
			if pf.Result != nil {
				counts["reason:"+string(pf.Result.Reason)]++
			}
		}
	}
	return yields, steps, counts
}

// plannedOutcomeSeedCap bounds the extended search for the rare outcomes.
// PromotedUnconfirmed and ManualRecoveryRequired need a fault to land in
// the few polls between a target's read_only=OFF and its confirmation, so
// they occur roughly once per few hundred seeds, and every engine change
// that moves a run's timing by a poll moves them to other seeds. (After the
// ultra-review fixes the first of each moved from seeds 142/190 to
// 1122/1633: the confirmed-promotion manager-absent end became Succeeded,
// K152, and the shifted timing missed the rogue writes.)
const plannedOutcomeSeedCap = 3000

// TestPlannedSweep_ReachesEveryOutcome: the generated trials end runs with
// every terminal outcome (K172), so a generator or harness change that
// stops producing one (the rare PromotedUnconfirmed and
// ManualRecoveryRequired above all) fails here instead of leaving its
// invariants unexercised. An outcome the shared sweep misses is searched
// for in further seeds, in parallel batches, up to plannedOutcomeSeedCap.
func TestPlannedSweep_ReachesEveryOutcome(t *testing.T) {
	_, _, counts := plannedSweep()
	// K041: a live engine holds an Unknown write whose late commit lands.
	if counts["lateCommitAfterUnknown"] == 0 {
		t.Errorf("no late commit landed after a settlement ended Unknown (counts %v)", counts)
	}
	outcomes := []v1alpha1.PlannedFailoverOutcome{
		v1alpha1.PlannedFailoverOutcomeSucceeded, v1alpha1.PlannedFailoverOutcomeNotStarted,
		v1alpha1.PlannedFailoverOutcomeRolledBack, v1alpha1.PlannedFailoverOutcomeSourceLeftFenced,
		v1alpha1.PlannedFailoverOutcomePromotedUnconfirmed, v1alpha1.PlannedFailoverOutcomeManualRecoveryRequired,
	}
	missing := func() []v1alpha1.PlannedFailoverOutcome {
		var out []v1alpha1.PlannedFailoverOutcome
		for _, o := range outcomes {
			if counts["outcome:"+string(o)] == 0 {
				out = append(out, o)
			}
		}
		return out
	}
	last := uint64(plannedSweepSeeds)
	batch := uint64(max(64, 4*runtime.GOMAXPROCS(0)))
	for len(missing()) > 0 && last < plannedOutcomeSeedCap {
		to := min(last+batch, plannedOutcomeSeedCap)
		_, _, more := plannedSweepRange(last+1, to)
		for k, v := range more {
			counts[k] += v
		}
		last = to
	}
	for _, o := range missing() {
		t.Errorf("no run ended %s over %d seeds", o, last)
	}
	t.Logf("searched seeds 1..%d", last)
}

// TestScheduler_ReachesEveryYieldClass: over a fixed range the production
// engine and facade reach every yield point the scheduler knows, and
// another actor steps at each. engine.settle (a settlement that ends
// Unknown) needs an ambiguous write and a read outage together, so it gets
// a targeted trial.
func TestScheduler_ReachesEveryYieldClass(t *testing.T) {
	yields, steps, _ := plannedSweep()
	for _, class := range []string{YieldEngineLoad, YieldEngineWrite, YieldPeerProbe, YieldBeforePublish, YieldAfterPublish,
		YieldBeforeStmt, YieldAfterStmt, YieldGateProbe, YieldConfirmProbe, YieldStmtIssued, YieldStmtApplied} {
		if yields[class] == 0 {
			t.Errorf("yield class %s never reached (yields %v)", class, yields)
		} else if steps[class] == 0 {
			t.Errorf("no actor ever stepped at %s", class)
		}
	}
	reached := false
	for seed := uint64(1); seed <= 50 && !reached; seed++ {
		tr := GeneratePlannedTrial(seed)
		for _, op := range tr.Actors.Ops {
			if op.Kind == OpPlannedFailover && op.Site != tr.SiteNames[0] && op.Site != unknownPlannedTarget {
				tr.Actors.Ops = append(tr.Actors.Ops,
					Op{At: op.At + 1, Kind: OpPFWriteLost, N: 1}, Op{At: op.At + 1, Kind: opPFSettleOutage, N: settleAttempts})
				sort.SliceStable(tr.Actors.Ops, func(i, j int) bool { return tr.Actors.Ops[i].At < tr.Actors.Ops[j].At })
				break
			}
		}
		reached = RunTrial(tr, false, nil).Planned.Yields[YieldEngineSettle] > 0
	}
	if !reached {
		t.Error("an unsettled write never yielded engine.settle")
	}
}

// TestSlowStatementInterleavesWithOtherActors: a slow statement yields
// inside the statement and other actors (a grant, a Poll, an update step)
// run while it is blocked.
func TestSlowStatementInterleavesWithOtherActors(t *testing.T) {
	_, steps, _ := plannedSweep()
	if steps[YieldStmtIssued] == 0 || steps[YieldStmtApplied] == 0 {
		t.Fatalf("no actor stepped inside a slow statement (steps %v)", steps)
	}
}

// TestRetiringPollAfterReplacement: after a manager replacement the
// retired manager keeps polling inside its window (a Poll in flight).
func TestRetiringPollAfterReplacement(t *testing.T) {
	_, _, counts := plannedSweep()
	if counts["managersReplaced"] == 0 || counts["retiringPolls"] == 0 {
		t.Fatalf("replacements %d, retiring polls %d", counts["managersReplaced"], counts["retiringPolls"])
	}
}

// TestRestartPromotedTargetReachesWasWritable: a target restarted after the
// run's read_only=OFF and an application write on it boots read_only=ON
// with its own post-promotion transactions; with its replica channel reset
// that classifies as §7.1 WasWritable, and the run itself (not merely some
// run of the trial) finishes Succeeded, never rolling back (K028).
func TestRestartPromotedTargetReachesWasWritable(t *testing.T) {
	_, _, counts := plannedSweep()
	if n := counts["restartedRun"+string(v1alpha1.PlannedFailoverOutcomeRolledBack)]; n > 0 {
		t.Fatalf("%d runs whose target restarted holding its own writes rolled back", n)
	}
	if counts["restartedRunRewrittenSucceeded"] == 0 {
		t.Fatal("no run made its target writable again and finished Succeeded after the target restarted holding its own post-promotion writes")
	}
}

// TestScheduler_YieldStepsRunTheManagerPolls: a manager actor the scheduler
// steps at a yield actually polls (K019). The scheduler puts the actor on
// its stack before the step; a step that re-entered sched.run would find
// it there and silently skip the Poll while consuming the tick's poll.
func TestScheduler_YieldStepsRunTheManagerPolls(t *testing.T) {
	stepped, polled := 0, 0
	retiringStepped, retiringPolled := 0, 0
	for seed := uint64(1); seed <= 60; seed++ {
		r := RunTrial(GeneratePlannedTrial(seed), false, nil)
		stepped += r.Planned.StepsBy["tm"]
		polled += r.Planned.Counts["tmPollsAtYield"]
		retiringStepped += r.Planned.StepsBy["retiring"]
		retiringPolled += r.Planned.Counts["retiringPollsAtYield"]
		if r.Planned.StepsBy["tm"] != r.Planned.Counts["tmPollsAtYield"] {
			t.Errorf("seed %d: the tm actor stepped %d times at yields but polled %d times there",
				seed, r.Planned.StepsBy["tm"], r.Planned.Counts["tmPollsAtYield"])
		}
	}
	if stepped == 0 || polled == 0 {
		t.Fatalf("no manager Poll ran at a yield (steps %d, polls %d)", stepped, polled)
	}
	t.Logf("tm: %d yield steps, %d polls; retiring: %d yield steps, %d polls", stepped, polled, retiringStepped, retiringPolled)
}

// TestPlannedTrial_LongOutagesReachTheDeadlines: the generated long
// outages (K036) outlive the 2m status-stall deadline and the 5m
// manager-absent deadline, so the sweep ends runs by the stall marker and
// with outcome ManagerUnavailable reasons, not only the 10s released-run
// marker.
func TestPlannedTrial_LongOutagesReachTheDeadlines(t *testing.T) {
	_, _, counts := plannedSweep()
	if counts["longStatusDenials"] == 0 || counts["longManagerAbsences"] == 0 {
		t.Fatalf("no long outage generated (counts %v)", counts)
	}
	if counts["stallMarker2m"] == 0 {
		t.Errorf("no run ended by the 2m status-stall marker (counts %v)", counts)
	}
	if counts["reason:"+string(v1alpha1.PlannedFailoverReasonManagerUnavailable)] == 0 {
		t.Errorf("no run ended ManagerUnavailable (counts %v)", counts)
	}
}

// TestSimSession_TransportFailureBreaksTheSession: a statement that timed
// out breaks the pinned connection, applied or not, so the next statement
// on the session fails instead of landing (K079); a fresh pin works.
func TestSimSession_TransportFailureBreaksTheSession(t *testing.T) {
	for name, inject := range map[string]func(c *Cluster){
		"notApplied": func(c *Cluster) { c.AddFailMutations("alpha", 1) },
		"ambiguous":  func(c *Cluster) { c.AddAmbiguousMutations("alpha", 1) },
	} {
		t.Run(name, func(t *testing.T) {
			c := twoSiteCluster()
			ctx := pfCtx()
			sess := mustPin(t, c, "alpha")
			if err := sess.SetSuperReadOnly(ctx, true); err != nil {
				t.Fatal(err)
			}
			inject(c)
			if err := sess.SetSuperReadOnly(ctx, false); err == nil {
				t.Fatal("injected timeout did not fail the statement")
			}
			roBefore := truthNamed(c.Truth(), "alpha").ReadOnly
			if err := sess.SetReadOnly(ctx, false); !errors.Is(err, errSessionLost) {
				t.Fatalf("statement after the timeout: err=%v, want errSessionLost", err)
			}
			if got := truthNamed(c.Truth(), "alpha").ReadOnly; got != roBefore {
				t.Fatalf("a statement on the broken session changed read_only %v -> %v", roBefore, got)
			}
			if _, err := mustPin(t, c, "alpha").GetGtidExecuted(ctx); err != nil {
				t.Fatalf("fresh pin after the broken session: %v", err)
			}
		})
	}
	// A MySQL rejection (RESET REPLICA ALL on a running replica) does not
	// break the connection.
	c := twoSiteCluster()
	sess := mustPin(t, c, "beta")
	if err := sess.ResetReplicaAll(pfCtx()); err == nil || !strings.Contains(err.Error(), "Error 3081") {
		t.Fatalf("RESET REPLICA ALL on a running replica: err=%v, want Error 3081", err)
	}
	if _, err := sess.GetGtidExecuted(pfCtx()); err != nil {
		t.Fatalf("a MySQL rejection broke the session: %v", err)
	}
}

// TestScheduler_SwitchAttributesToTheRunningActor: what happens between two
// scheduler switches is attributed to the actor that ran (K177): an outer
// actor's work before and after a yield to it, a nested step's to the
// nested actor.
func TestScheduler_SwitchAttributesToTheRunningActor(t *testing.T) {
	s := newYieldScheduler(1)
	var work []string // what the actors did since the last switch
	var got []string
	s.onSwitch = func(owner string) {
		for _, w := range work {
			got = append(got, w+"@"+owner)
		}
		work = nil
	}
	s.addActor(&schedActor{name: "inner", pct: 100, step: func(context.Context) { work = append(work, "i") }})
	s.addActor(&schedActor{name: "outer", pct: 0, step: func(context.Context) {}})
	s.run("outer", func() {
		work = append(work, "o1")
		s.Yield(YieldEngineWrite)
		work = append(work, "o2")
	})
	if want := "o1@outer,i@inner,o2@outer"; strings.Join(got, ",") != want {
		t.Fatalf("attribution %v, want %s", got, want)
	}
}
