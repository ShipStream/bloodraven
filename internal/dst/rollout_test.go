package dst

import (
	"fmt"
	"log/slog"
	"os"
	"reflect"
	"runtime"
	"sort"
	"strconv"
	"strings"
	"sync"
	"testing"
)

// rolloutSweepSeeds is the quick sweep's seed range, shared by the tests
// that judge what the range reaches.
const rolloutSweepSeeds = 64

var (
	rolloutSweepOnce    sync.Once
	rolloutSweepResults []TrialResult
)

// rolloutSweep runs rollout seeds 1..rolloutSweepSeeds once per test
// binary, in parallel.
// rolloutDeferSeed is a trial in which an admission is withdrawn because the
// group did not start (TestRolloutSweep_ReachesTheMidRollStates).
const rolloutDeferSeed = 101

func rolloutSweep() []TrialResult {
	rolloutSweepOnce.Do(func() {
		rolloutSweepResults = runBatch(1, rolloutSweepSeeds, runtime.GOMAXPROCS(0), GenerateRolloutTrial)
	})
	return rolloutSweepResults
}

// rolloutCoverage sums what a batch of rollout trials did.
type rolloutCoverage struct {
	trials, failing                       int
	transitions                           map[string]int
	crashKinds                            map[string]int
	restarts, midRoll, writeCrashes       int
	releases, templateWrites, handoffs    int
	completedGroups, groups, needsHuman   int
	trialsWithRetargetStarted, signatures int
}

func coverRollout(results []TrialResult) rolloutCoverage {
	c := rolloutCoverage{transitions: map[string]int{}, crashKinds: map[string]int{}}
	sigs := map[string]struct{}{}
	for _, r := range results {
		c.trials++
		if r.Failed() {
			c.failing++
		}
		sigs[r.Signature] = struct{}{}
		ro := r.Rollout
		for k, n := range ro.Transitions {
			c.transitions[k] += n
		}
		for _, k := range ro.CrashKinds {
			c.crashKinds[k]++
		}
		c.restarts += ro.Restarts
		c.midRoll += ro.MidRollRestarts
		c.writeCrashes += ro.WriteCrashes
		c.releases += ro.Releases
		c.templateWrites += ro.TemplateWrites
		c.handoffs += ro.Handoffs
		c.completedGroups += ro.Completed
		c.groups += len(r.Trial.Rollout.Groups)
		c.needsHuman += len(ro.NeedsHuman)
		if strings.Contains(r.Signature, "retargetStarted") {
			c.trialsWithRetargetStarted++
		}
	}
	c.signatures = len(sigs)
	return c
}

func (c rolloutCoverage) String() string {
	keys := func(m map[string]int) string {
		names := make([]string, 0, len(m))
		for k := range m {
			names = append(names, k)
		}
		sort.Strings(names)
		parts := make([]string, len(names))
		for i, k := range names {
			parts[i] = fmt.Sprintf("%s=%d", k, m[k])
		}
		return strings.Join(parts, " ")
	}
	return fmt.Sprintf("%d trials (%d failing), %d signatures; %d groups, %d completed, %d left for a human; %d releases, %d template writes, %d handoffs; "+
		"%d operator restarts (%d mid-roll, %d after a death at an API write; %d trials retargeted a started roll)\n  record transitions: %s\n  died at: %s",
		c.trials, c.failing, c.signatures, c.groups, c.completedGroups, c.needsHuman, c.releases, c.templateWrites, c.handoffs,
		c.restarts, c.midRoll, c.writeCrashes, c.trialsWithRetargetStarted, keys(c.transitions), keys(c.crashKinds))
}

// TestDST_RolloutQuick is the CI-speed sweep of the paced rollout: any
// invariant violation across the fixed seed range fails the build.
// DST_ROLLOUT_TRIALS and DST_ROLLOUT_START run another range instead (one
// range per process), for a wider sweep.
func TestDST_RolloutQuick(t *testing.T) {
	var results []TrialResult
	if os.Getenv("DST_ROLLOUT_TRIALS") != "" || os.Getenv("DST_ROLLOUT_START") != "" {
		results = runBatch(uint64(envInt("DST_ROLLOUT_START", 1)), envInt("DST_ROLLOUT_TRIALS", rolloutSweepSeeds),
			runtime.GOMAXPROCS(0), GenerateRolloutTrial)
	} else {
		results = rolloutSweep()
	}
	failures := map[string]uint64{}
	counts := map[string]int{}
	for _, r := range results {
		for _, v := range r.Violations {
			t.Errorf("seed %d: %s", r.Trial.Seed, v)
		}
		if r.Failed() {
			fp := fingerprint(r.Violations)
			if _, ok := failures[fp]; !ok {
				failures[fp] = r.Trial.Seed
			}
			counts[fp]++
		}
	}
	for fp, seed := range failures {
		t.Logf("finding: %s (%d/%d trials, first seed %d)", fp, counts[fp], len(results), seed)
	}
	var needsHuman []uint64
	for _, r := range results {
		if len(r.Rollout.NeedsHuman) > 0 {
			needsHuman = append(needsHuman, r.Trial.Seed)
		}
	}
	t.Logf("coverage: %s", coverRollout(results))
	t.Logf("seeds that left a group for a human (README, Inherent): %v", needsHuman)
}

// TestRolloutSweep_ReachesTheMidRollStates: the quick range must keep
// reaching what the rollout trials exist for. A generator change that
// stops producing one of these leaves the sweep green and blind.
func TestRolloutSweep_ReachesTheMidRollStates(t *testing.T) {
	// A deferral needs an admitted group kept from starting for the whole
	// grace, which a few trials in a thousand produce; rolloutDeferSeed is
	// one of them, run next to the quick range.
	results := append([]TrialResult{}, rolloutSweep()...)
	results = append(results, runBatch(rolloutDeferSeed, 1, 1, GenerateRolloutTrial)...)
	for _, r := range results[len(results)-1:] {
		if r.Failed() {
			t.Fatalf("seed %d: %v\ntrial:\n%s", r.Trial.Seed, r.Violations, r.Trial)
		}
	}
	c := coverRollout(results)
	for _, want := range []string{
		"admit", "start", "complete", // a roll from admission to completion
		"stall:OrderedUpdateStalled", // the ordered updater stopped on a pod that is not Ready
		"stall:StallTimeout",         // a roll (Recreate, or one with no updater report) ran out of time
		"retarget",                   // a second release found an unfinished admission
		"defer",                      // an admission that could not start was withdrawn
	} {
		if c.transitions[want] == 0 {
			t.Errorf("no trial in seeds 1..%d and %d made the record transition %q", rolloutSweepSeeds, rolloutDeferSeed, want)
		}
	}
	if c.midRoll == 0 {
		t.Errorf("no operator restart landed while a roll had started and not completed")
	}
	if c.trialsWithRetargetStarted == 0 {
		t.Errorf("no second release landed on a started roll")
	}
	for _, want := range []string{"the start latch", "update Deployment", "status MysqlFailoverGroup"} {
		if c.crashKinds[want] == 0 {
			t.Errorf("no operator process died at %q (deaths: %v)", want, c.crashKinds)
		}
	}
	if c.handoffs == 0 {
		t.Errorf("no ordered update handed the active site over")
	}
	if c.completedGroups == 0 {
		t.Errorf("no group completed its roll")
	}
	t.Logf("coverage: %s", c)
}

// TestRolloutDeterminism: the same seed is the same trial, twice, also
// under a shrink mask and with the step log captured.
func TestRolloutDeterminism(t *testing.T) {
	summary := func(r TrialResult) string {
		ro := *r.Rollout
		ro.Log = nil
		return fmt.Sprintf("%s | %+v | %v", r.Signature, ro, r.Violations)
	}
	type variant struct {
		seed   uint64
		masked bool
	}
	var variants []variant
	for seed := uint64(1); seed <= 5; seed++ {
		variants = append(variants, variant{seed, false}, variant{seed, true})
	}
	gen := func(v variant) Trial {
		trial := GenerateRolloutTrial(v.seed)
		if v.masked {
			trial.Rollout.Skip = make([]bool, len(trial.Rollout.Ops))
			for i := range trial.Rollout.Skip {
				trial.Rollout.Skip[i] = i%3 == 1
			}
		}
		return trial
	}
	// Three runs of each variant: uncaptured, and captured twice.
	runs := make([]TrialResult, 3*len(variants))
	var wg sync.WaitGroup
	for i := range runs {
		wg.Add(1)
		go func() {
			defer wg.Done()
			runs[i] = RunTrial(gen(variants[i/3]), i%3 != 0, nil)
		}()
	}
	wg.Wait()
	for i, v := range variants {
		a, b, c := runs[3*i], runs[3*i+1], runs[3*i+2]
		if summary(a) != summary(b) {
			t.Fatalf("seed %d (masked=%v): capture changed the run:\n  a=%s\n  b=%s", v.seed, v.masked, summary(a), summary(b))
		}
		if summary(b) != summary(c) || !reflect.DeepEqual(b.Rollout.Log, c.Rollout.Log) {
			t.Fatalf("seed %d (masked=%v): two runs differ:\n%s\n%s\n---\n%s\n%s", v.seed, v.masked,
				summary(b), strings.Join(b.Rollout.Log, "\n"), summary(c), strings.Join(c.Rollout.Log, "\n"))
		}
	}
}

// TestRolloutFaultFreeBaseline: a release with nothing else scheduled rolls
// every group, one after another, and the trial stops early.
func TestRolloutFaultFreeBaseline(t *testing.T) {
	onlyTheRelease := func(seed uint64) Trial {
		trial := GenerateRolloutTrial(seed)
		trial.Rollout.Skip = make([]bool, len(trial.Rollout.Ops))
		released := false
		for i, op := range trial.Rollout.Ops {
			trial.Rollout.Skip[i] = op.Kind != OpRelease || released
			released = released || op.Kind == OpRelease
		}
		return trial
	}
	for _, r := range runBatch(1, 4, runtime.GOMAXPROCS(0), onlyTheRelease) {
		seed, trial := r.Trial.Seed, r.Trial
		if r.Failed() {
			t.Fatalf("seed %d (one release, no faults): %v\ntrial:\n%s", seed, r.Violations, trial)
		}
		groups := len(trial.Rollout.Groups)
		if r.Rollout.Completed != groups || r.Rollout.Transitions["admit"] != groups || r.Rollout.Transitions["complete"] != groups {
			t.Fatalf("seed %d (one release, no faults): %d groups, result %+v", seed, groups, *r.Rollout)
		}
	}
}

// TestGenerateTrial_UnchangedByRollout: the rollout trials are a family of
// their own, and the failover trials' generator does not know them.
func TestGenerateTrial_UnchangedByRollout(t *testing.T) {
	for seed := uint64(1); seed <= 50; seed++ {
		if tr := GenerateTrial(seed); tr.Rollout.Enabled || len(tr.Rollout.Ops) > 0 {
			t.Fatalf("seed %d: GenerateTrial produced a rollout plan", seed)
		}
		if tr := GeneratePlannedTrial(seed); tr.Rollout.Enabled {
			t.Fatalf("seed %d: GeneratePlannedTrial produced a rollout plan", seed)
		}
	}
}

// TestDST_RolloutRepro replays one rollout seed with the step log.
// Usage: DST_SEED=7 [DST_ROLLOUT_SKIP=0,3] [DST_LOG=1] go test ./internal/dst -run RolloutRepro -v
func TestDST_RolloutRepro(t *testing.T) {
	seedStr := os.Getenv("DST_SEED")
	if seedStr == "" {
		t.Skip("set DST_SEED to replay a rollout trial")
	}
	seed, err := strconv.ParseUint(seedStr, 10, 64)
	if err != nil {
		t.Fatalf("bad DST_SEED: %v", err)
	}
	trial := GenerateRolloutTrial(seed)
	if mask := os.Getenv("DST_ROLLOUT_SKIP"); mask != "" {
		trial.Rollout.Skip = make([]bool, len(trial.Rollout.Ops))
		for _, part := range splitCommas(mask) {
			i, err := strconv.Atoi(part)
			if err != nil || i < 0 || i >= len(trial.Rollout.Ops) {
				t.Fatalf("bad DST_ROLLOUT_SKIP entry %q", part)
			}
			trial.Rollout.Skip[i] = true
		}
	}
	t.Logf("trial:\n%s", trial)
	var handler slog.Handler
	if os.Getenv("DST_LOG") == "1" {
		handler = slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelDebug})
	}
	r := RunTrial(trial, true, handler)
	for _, line := range r.Rollout.Log {
		t.Log(line)
	}
	summary := *r.Rollout
	summary.Log = nil
	t.Logf("result: %+v", summary)
	t.Logf("signature: %s", r.Signature)
	for _, v := range r.Violations {
		t.Logf("VIOLATION: %s", v)
	}
}

// rolloutHandBuilt is a rollout trial written out by hand: the fleet and the
// ops, with the warm-up and a settle window around them.
func rolloutHandBuilt(groups []RolloutGroup, healAt int, ops ...Op) Trial {
	return Trial{
		WarmupPolls: rolloutWarmup,
		HealAt:      healAt,
		Polls:       healAt + 60,
		Rollout: RolloutPlan{
			Enabled: true, Groups: groups, StartGraceSteps: 8, StallAfterSteps: 6, Ops: ops,
		},
	}
}

// TestRollout_OperatorRestartDuringRecreateRollNeedsAHuman is the inherent
// class of the rollout trials (README "Paced rollouts"), written out so
// that it does not move with the generator: a Recreate roll restarts the
// primary, the operator restarts before the group's status names an active
// site again, and the next process finds two read-only sites and no
// authority on record. The model's pods are away for no time at all, so it
// takes a second release one step after the first to restart the primary
// under the new process; in a cluster the restart alone does it.
//
// The group is left for a human and the fleet waits behind it; neither is
// a violation. Without the second restart the same schedule converges.
func TestRollout_OperatorRestartDuringRecreateRollNeedsAHuman(t *testing.T) {
	groups := []RolloutGroup{
		{Name: "ant", Sites: rolloutSiteNames[:2], Recreate: true},
		{Name: "bee", Sites: rolloutSiteNames[:2]},
	}
	wedged := RunTrial(rolloutHandBuilt(groups, 20,
		Op{At: 4, Kind: OpRelease},
		Op{At: 5, Kind: OpRelease, N: 1},
	), true, nil)
	if wedged.Failed() {
		t.Fatalf("violations: %v\n%s", wedged.Violations, strings.Join(wedged.Rollout.Log, "\n"))
	}
	if !reflect.DeepEqual(wedged.Rollout.NeedsHuman, []string{"ant"}) || wedged.Rollout.Completed != 0 {
		t.Fatalf("NeedsHuman = %v, completed = %d; want ant left for a human and bee waiting behind it\n%s",
			wedged.Rollout.NeedsHuman, wedged.Rollout.Completed, strings.Join(wedged.Rollout.Log, "\n"))
	}

	healed := RunTrial(rolloutHandBuilt(groups, 20, Op{At: 4, Kind: OpRelease}), true, nil)
	if healed.Failed() || len(healed.Rollout.NeedsHuman) > 0 || healed.Rollout.Completed != 2 {
		t.Fatalf("without the second restart: violations %v, NeedsHuman %v, completed %d\n%s",
			healed.Violations, healed.Rollout.NeedsHuman, healed.Rollout.Completed, strings.Join(healed.Rollout.Log, "\n"))
	}
}
