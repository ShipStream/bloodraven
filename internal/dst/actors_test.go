package dst

import (
	"fmt"
	"strings"
	"sync"
	"testing"

	"github.com/shipstream/bloodraven/internal/state"
)

// A simulated actor that never acts is worse than no actor: it costs
// runtime, reads as coverage, and finds nothing. The tests here assert the
// new machinery actually fires over a fixed seed range, so a generator
// rebalance, a reachability-rule change, or a production refactor that
// quietly stops the sidecar from ever fencing shows up as a failure rather
// than as a silently shrinking search space.

// actorSweepSeeds is the fixed range the reachability assertions share.
const actorSweepSeeds = 600

// actorEventCounts replays actorSweepSeeds trials with full event capture
// and counts, per event key, how many produced at least one.
//
// Computed once for the whole package: replaying the same range in each
// assertion below would triple the cost of `make test` for identical data.
var actorEventCounts = sync.OnceValue(func() map[string]int {
	counts := map[string]int{}
	for seed := uint64(1); seed <= actorSweepSeeds; seed++ {
		r := RunTrial(GenerateTrial(seed), true, nil)
		seen := map[string]bool{}
		for _, e := range r.Events {
			key := string(e.Kind)
			if e.Outcome != "" {
				key += ":" + e.Outcome
			}
			if !seen[key] {
				seen[key] = true
				counts[key]++
			}
		}
		if len(r.SelfFenced) > 0 {
			counts["<endSelfFenced>"]++
		}
	}
	return counts
})

// TestSidecarActorIsReachable: the real FencingMonitor must actually decide
// to fence, and must reach its ambiguous-write path, across the seed range.
func TestSidecarActorIsReachable(t *testing.T) {
	const seeds = actorSweepSeeds
	counts := actorEventCounts()

	for _, want := range []struct {
		key string
		min int
		why string
	}{
		{string(EvSidecarFence), 20, "the monitor never self-fenced — check reachability rules and lease arithmetic"},
		{string(EvSidecarKill), 20, "a fence that lands always evicts; no evictions means doFence is not completing"},
		{string(EvSidecarFence) + ":ambiguous", 1, "the ambiguous-fence path (doFence → fenceLanded) is unreachable"},
		{"<endSelfFenced>", 20, "no trial ended with a monitor holding its self-fenced flag"},
	} {
		if counts[want.key] < want.min {
			t.Errorf("%s fired in %d/%d trials, want >= %d: %s", want.key, counts[want.key], seeds, want.min, want.why)
		}
	}
}

// TestMidExecuteCrashIsReachable: operator deaths must land inside mutation
// sequences, in both the applied-then-died and died-before-apply shapes, and
// on more than one statement of the promotion sequence. A crash that only
// ever landed on one statement would be a restart with extra steps.
func TestMidExecuteCrashIsReachable(t *testing.T) {
	const seeds = actorSweepSeeds
	counts := actorEventCounts()

	if counts[string(EvOperatorDie)] < 5 {
		t.Fatalf("operator died mid-execute in only %d/%d trials; the crash countdown is not landing", counts[string(EvOperatorDie)], seeds)
	}

	applied, preApply := 0, 0
	statements := map[string]bool{}
	for key, n := range counts {
		kind, outcome, ok := strings.Cut(key, ":")
		if !ok {
			continue
		}
		switch outcome {
		case "appliedThenOperatorDied":
			applied += n
			statements[kind] = true
		case "operatorDied":
			preApply += n
			statements[kind] = true
		}
	}
	if applied == 0 {
		t.Error("no trial killed the operator AFTER its statement applied — the nastiest shape is unreachable")
	}
	if preApply == 0 {
		t.Error("no trial killed the operator BEFORE its statement applied")
	}
	if len(statements) < 3 {
		t.Errorf("deaths landed on only %d distinct statements (%v); the countdown is not spreading across the sequence",
			len(statements), statements)
	}
}

// TestOutOfBandStoreIsExercised: the anti-flap store must be written on
// promotions and must sometimes be denied, or the fingerprint split between
// the inherent and regression cooldown classes is untested by the campaign.
func TestOutOfBandStoreIsExercised(t *testing.T) {
	const seeds = actorSweepSeeds
	counts := actorEventCounts()

	if counts[string(EvStateWrite)] < 20 {
		t.Errorf("the out-of-band store was written in only %d/%d trials", counts[string(EvStateWrite)], seeds)
	}
	if counts[string(EvStateOutage)] < 5 {
		t.Errorf("the out-of-band store outage fired in only %d/%d trials", counts[string(EvStateOutage)], seeds)
	}
	if counts[string(EvStateWrite)+":denied"] < 1 {
		t.Error("no trial ever had an out-of-band write rejected; the outage never overlaps a promotion")
	}
}

// TestFaultWeightsCoverEveryKind: every kind in the distribution must be
// drawable, and the distribution must be exhaustive. pickFaultKind falls
// through to the last entry, so a table whose weights summed low would
// silently over-sample it — the init() guard catches that, and this catches
// a kind added with weight 0.
func TestFaultWeightsCoverEveryKind(t *testing.T) {
	drawn := map[OpKind]int{}
	for roll := 0; roll < 100; roll++ {
		drawn[pickFaultKind(roll)]++
	}
	for _, w := range faultWeights {
		if drawn[w.kind] != w.weight {
			t.Errorf("kind %s drawn on %d of 100 rolls, want %d", w.kind, drawn[w.kind], w.weight)
		}
	}
	if len(drawn) != len(faultWeights) {
		t.Errorf("drew %d distinct kinds from a table of %d", len(drawn), len(faultWeights))
	}
}

// TestSidecarFencesStalePrimary is the behavior the actor exists to model:
// a primary that returns from a partition after the operator failed over
// must fence itself on the topology rule, even though its peer is reachable
// (peer reachability alone keeps the lease rule quiet).
func TestSidecarFencesStalePrimary(t *testing.T) {
	trial := Trial{
		SiteNames:   []string{"alpha", "beta"},
		Roles:       []state.SiteRole{state.SiteRolePrimaryCandidate, state.SiteRolePrimaryCandidate},
		CooldownSec: 10,
		WarmupPolls: 8,
		HealAt:      45,
		Polls:       120,
		// Long enough that the lease rule cannot be what fires.
		SidecarLeasePolls: 60,
		SidecarTopology:   true,
		Ops: []Op{
			// alpha is cut off from the operator and comes back writable —
			// the documented "old primary respawned writable" shape.
			{At: 10, Kind: OpPartOpSite, Site: "alpha"},
			{At: 12, Kind: OpCrash, Site: "alpha", Fenced: false},
			{At: 20, Kind: OpRecover, Site: "alpha", Fenced: false},
			{At: 30, Kind: OpHealOpSite, Site: "alpha"},
		},
	}
	r := RunTrial(trial, true, nil)

	var fences []Event
	for _, e := range r.Events {
		if e.Kind == EvSidecarFence && e.Site == "alpha" {
			fences = append(fences, e)
		}
	}
	if len(fences) == 0 {
		t.Fatalf("alpha's sidecar never self-fenced after returning writable into a failed-over group\nevents:\n%s", formatEvents(r.Events))
	}
	if fences[0].Poll >= 30 {
		t.Errorf("alpha self-fenced only at poll %d, after its operator link healed at 30 — the topology rule should have fired while still partitioned (via the peer relay)", fences[0].Poll)
	}
}

func formatEvents(events []Event) string {
	var b strings.Builder
	for _, e := range events {
		fmt.Fprintf(&b, "  %s\n", e)
	}
	return b.String()
}
