package dst

import (
	"crypto/sha256"
	"fmt"
	"log/slog"
	"os"
	"reflect"
	"strconv"
	"testing"
)

// scheduleGolden is sha256 over GenerateTrial(seed).String() for seeds
// 1..2000. The actors draw from a stream of their own (§15.1, D18); this
// pins that adding them, or any later actor-stream change, leaves every
// existing seed's fault schedule byte-identical. A deliberate change to the
// fault generator updates it knowingly: it last changed when the binary log
// rotation faults (failFlush, slowFlush) and the pre-reclone wait
// (recloneWait) were added.
const scheduleGolden = "b184a6370e18cf9382b844893e97f7d31d3b86ec9eafea28250131e7411f7c92"

// scheduleGoldenBeforeFlushFaults is the same hash at the commit before the
// planned-failover actors and the rotation faults existed. The rotation
// faults draw from a stream of their own too (flushFaults), so a schedule
// with them removed must still hash to it.
const scheduleGoldenBeforeFlushFaults = "811becbb1b33126dca8a4eb681871d1b0d7e83215020d0110a690f93514a585a"

// TestGenerateTrial_FlushFaultsOnlyAdd: the rotation faults only add ops.
// With them removed, every seed's schedule is byte-identical to what it was
// before they existed, so a seed's other faults land where they always did.
func TestGenerateTrial_FlushFaultsOnlyAdd(t *testing.T) {
	h := sha256.New()
	kinds := map[OpKind]int{}
	late := 0
	for seed := uint64(1); seed <= 2000; seed++ {
		tr := GenerateTrial(seed)
		kept := tr.Ops[:0:0]
		for _, op := range tr.Ops {
			if op.Kind == OpRecloneWait {
				kinds[op.Kind]++
				if op.At < tr.WarmupPolls || op.At >= tr.HealAt || op.N < 5 || op.N > 34 || op.K < 0 || op.K > 3 {
					t.Fatalf("seed %d: reclone wait outside its shape: %s", seed, op)
				}
				continue
			}
			if op.Kind == OpFailFlush || op.Kind == OpSlowFlush {
				kinds[op.Kind]++
				late += op.K
				if op.At < tr.WarmupPolls || op.At >= tr.HealAt || op.N < 1 || op.N > 3 {
					t.Fatalf("seed %d: rotation fault outside its shape: %s", seed, op)
				}
				continue
			}
			kept = append(kept, op)
		}
		tr.Ops = kept
		fmt.Fprint(h, tr.String())
	}
	if got := fmt.Sprintf("%x", h.Sum(nil)); got != scheduleGoldenBeforeFlushFaults {
		t.Fatalf("the rotation faults changed the rest of the schedule: hash %s, want %s", got, scheduleGoldenBeforeFlushFaults)
	}
	if kinds[OpFailFlush] < 200 || kinds[OpSlowFlush] < 200 || late < 50 || late == kinds[OpSlowFlush] || kinds[OpRecloneWait] < 400 {
		t.Fatalf("rotation faults over 2000 seeds: %v (late %d); want both kinds, with and without a late rotation, and reclone waits", kinds, late)
	}
}

func TestGenerateTrial_ScheduleUnchangedByActors(t *testing.T) {
	h := sha256.New()
	for seed := uint64(1); seed <= 2000; seed++ {
		tr := GenerateTrial(seed)
		if tr.Actors.Enabled {
			t.Fatalf("seed %d: GenerateTrial enabled the actors", seed)
		}
		fmt.Fprint(h, tr.String())
	}
	if got := fmt.Sprintf("%x", h.Sum(nil)); got != scheduleGolden {
		t.Fatalf("fault schedules changed: hash %s, want %s", got, scheduleGolden)
	}
}

func TestGeneratePlannedTrial_KeepsFaultSchedule(t *testing.T) {
	for seed := uint64(1); seed <= 500; seed++ {
		plain, planned := GenerateTrial(seed), GeneratePlannedTrial(seed)
		if !reflect.DeepEqual(plain.Ops, planned.Ops) {
			t.Fatalf("seed %d: actors changed the fault ops", seed)
		}
		if !planned.Actors.Enabled {
			t.Fatalf("seed %d: actors not enabled", seed)
		}
		if !reflect.DeepEqual(planned.Actors, GenerateActors(plain)) {
			t.Fatalf("seed %d: actor schedule not deterministic", seed)
		}
	}
}

// TestGenerateActors_ShapeAndCoverage: the stream generates 1-3 planned
// requests, 0-2 updates and 0-2 replacements per trial, and over a fixed
// range every actor fault kind and every target class appears.
func TestGenerateActors_ShapeAndCoverage(t *testing.T) {
	kinds := map[OpKind]int{}
	targets := map[string]int{}
	slow := 0
	for seed := uint64(1); seed <= 600; seed++ {
		tr := GeneratePlannedTrial(seed)
		per := map[OpKind]int{}
		for i, op := range tr.Actors.Ops {
			per[op.Kind]++
			kinds[op.Kind]++
			if i > 0 && tr.Actors.Ops[i-1].At > op.At {
				t.Fatalf("seed %d: actor ops not sorted", seed)
			}
			if op.At < tr.WarmupPolls {
				t.Fatalf("seed %d: actor op %s inside warmup", seed, op)
			}
			// The settle window is fault-free (K105): every actor op lands
			// before it, companion restarts of a status denial included.
			if op.At >= tr.settleStart() {
				t.Fatalf("seed %d: actor op %s inside the settle window (HealAt %d, settle from %d)",
					seed, op, tr.HealAt, tr.settleStart())
			}
			if op.Kind == OpPlannedFailover {
				switch op.Site {
				case tr.SiteNames[0]:
					targets["active"]++
				case unknownPlannedTarget:
					targets["unknown"]++
				default:
					targets["valid"]++
				}
			}
		}
		if n := per[OpPlannedFailover]; n < 1 || n > 3 {
			t.Fatalf("seed %d: %d planned requests", seed, n)
		}
		if per[OpOrderedUpdate] > 2 || per[OpReplaceManager] > 2 {
			t.Fatalf("seed %d: %d updates, %d replacements", seed, per[OpOrderedUpdate], per[OpReplaceManager])
		}
		slow += len(tr.Actors.Slow)
	}
	for _, w := range actorFaultWeights {
		if kinds[w.kind] == 0 {
			t.Errorf("actor fault kind %s never generated", w.kind)
		}
	}
	for _, k := range []OpKind{OpOrderedUpdate, OpReplaceManager, OpRestartOperator, OpPFStatusDeniedLong, OpManagerAbsentLong} {
		if kinds[k] == 0 {
			t.Errorf("actor op %s never generated", k)
		}
	}
	for _, c := range []string{"valid", "active", "unknown"} {
		if targets[c] == 0 {
			t.Errorf("no planned request with a %s target", c)
		}
	}
	if slow == 0 {
		t.Error("no slow statements generated")
	}
}

func TestActorFaultWeightsCoverEveryKind(t *testing.T) {
	drawn := map[OpKind]int{}
	for roll := 0; roll < 100; roll++ {
		drawn[pickActorFaultKind(roll)]++
	}
	for _, w := range actorFaultWeights {
		if drawn[w.kind] != w.weight {
			t.Errorf("kind %s drawn on %d of 100 rolls, want %d", w.kind, drawn[w.kind], w.weight)
		}
	}
	if len(drawn) != len(actorFaultWeights) {
		t.Errorf("drew %d distinct kinds from a table of %d", len(drawn), len(actorFaultWeights))
	}
}

// TestDeterminism_Planned: a trial with actors replays exactly, store log
// and yield counts included.
func TestDeterminism_Planned(t *testing.T) {
	for seed := uint64(1); seed <= 40; seed++ {
		a := RunTrial(GeneratePlannedTrial(seed), false, nil)
		b := RunTrial(GeneratePlannedTrial(seed), false, nil)
		if a.Signature != b.Signature || fmt.Sprint(a.Violations) != fmt.Sprint(b.Violations) {
			t.Fatalf("seed %d: runs differ:\n  sig %s\n  vs  %s\n  viol %v\n  vs   %v", seed, a.Signature, b.Signature, a.Violations, b.Violations)
		}
		if !reflect.DeepEqual(a.Planned.StoreLog, b.Planned.StoreLog) || !reflect.DeepEqual(a.Planned.Yields, b.Planned.Yields) {
			t.Fatalf("seed %d: planned reports differ", seed)
		}
		if !reflect.DeepEqual(a.Planned.Object, b.Planned.Object) {
			t.Fatalf("seed %d: final objects differ", seed)
		}
	}
}

// plannedAllowedClasses are the only finding classes a trial with actors
// may produce (README, Known finding classes):
//   - CooldownViolated(restart+stateLost): inherent. OpPFStatusDenied
//     combines a status outage with operator restarts, and when the fault
//     stream denies the out-of-band store over the same restarts, nothing
//     could carry the cooldown.
//
// DST-1 (a retiring manager's record not carried into its replacement:
// CooldownViolated/AntiFlapStateLost/WedgedNoPrimary(replacement)) and
// DST-4 (DualWritableUnresolved(confirmedClaim): a confirmed run's claim
// held through a stalled Publishing write) are fixed and no longer allowed;
// both are pinned in test/component.
var plannedAllowedClasses = map[string]bool{
	"CooldownViolated(restart+stateLost)": true,
}

// allowedFingerprint reports whether every class of fp is allowed. Classes
// are joined with '+', which also appears inside a class's parentheses.
func allowedFingerprint(fp string) bool {
	depth, start := 0, 0
	for i := 0; i <= len(fp); i++ {
		switch {
		case i < len(fp) && fp[i] == '(':
			depth++
		case i < len(fp) && fp[i] == ')':
			depth--
		case i == len(fp) || (fp[i] == '+' && depth == 0):
			if !plannedAllowedClasses[fp[start:i]] {
				return false
			}
			start = i + 1
		}
	}
	return true
}

// TestDST_PlannedQuick is the actors-on sweep (§15.1 "Runs"): 300 seeds with
// every actor enabled, driving the production engine, facade, claim and
// out-of-band recorder. Any violation outside plannedAllowedClasses fails
// the build. Seeds shift whenever the actor generator changes; fixed
// findings are pinned in test/component/dst_regression_test.go.
func TestDST_PlannedQuick(t *testing.T) {
	trials := 300
	if s := os.Getenv("DST_PLANNED_TRIALS"); s != "" {
		if n, err := strconv.Atoi(s); err == nil {
			trials = n
		}
	}
	start := uint64(1)
	if s := os.Getenv("DST_PLANNED_START"); s != "" {
		if n, err := strconv.ParseUint(s, 10, 64); err == nil && n > 0 {
			start = n
		}
	}
	first := map[string]uint64{}
	counts := map[string]int{}
	for seed := start; seed < start+uint64(trials); seed++ {
		r := RunTrial(GeneratePlannedTrial(seed), false, nil)
		if len(r.Violations) == 0 {
			continue
		}
		fp := fingerprint(r.Violations)
		if _, ok := first[fp]; !ok {
			first[fp] = seed
		}
		counts[fp]++
		if allowedFingerprint(fp) {
			continue
		}
		for _, v := range r.Violations {
			t.Errorf("seed %d: %s", seed, v)
		}
	}
	for fp, seed := range first {
		t.Logf("class %s: %d/%d trials, first seed %d", fp, counts[fp], trials, seed)
	}
}

// TestDST_PlannedRepro replays one actors-on seed with full capture.
// Usage: DST_SEED=12345 [DST_SKIP=0,3] [DST_ACTOR_SKIP=1,4] go test ./internal/dst -run PlannedRepro -v
// DST_SKIP masks fault ops and DST_ACTOR_SKIP actor ops, by index in the
// printed schedules (K090).
func TestDST_PlannedRepro(t *testing.T) {
	seedStr := os.Getenv("DST_SEED")
	if seedStr == "" {
		t.Skip("set DST_SEED to replay a trial with actors")
	}
	seed, err := strconv.ParseUint(seedStr, 10, 64)
	if err != nil {
		t.Fatalf("bad DST_SEED: %v", err)
	}
	trial := GeneratePlannedTrial(seed)
	trial.Skip = parseSkipMask(t, "DST_SKIP", len(trial.Ops))
	trial.Actors.Skip = parseSkipMask(t, "DST_ACTOR_SKIP", len(trial.Actors.Ops))
	t.Logf("trial:\n%s", trial)
	var handler slog.Handler
	if os.Getenv("DST_LOG") != "" {
		handler = slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelDebug})
	}
	r := RunTrial(trial, true, handler)
	for _, e := range r.Events {
		t.Log(e)
	}
	for _, op := range r.Planned.StoreLog {
		t.Logf("store %s", op)
	}
	t.Logf("yields %v; unsupported actor ops %v; stats %+v", r.Planned.Yields, r.Planned.Unsupported, r.Planned.Stats)
	for _, v := range r.Violations {
		t.Logf("VIOLATION: %s", v)
	}
}

// parseSkipMask reads a comma-separated op-index mask from env (nil when
// unset).
func parseSkipMask(t *testing.T, env string, n int) []bool {
	t.Helper()
	v := os.Getenv(env)
	if v == "" {
		return nil
	}
	mask := make([]bool, n)
	for _, part := range splitCommas(v) {
		i, err := strconv.Atoi(part)
		if err != nil || i < 0 || i >= n {
			t.Fatalf("bad %s entry %q", env, part)
		}
		mask[i] = true
	}
	return mask
}

// TestShrinkTrial_MasksActorOps: the shrinker minimizes a planned failure
// over the actor schedule too (K090): ops the failure does not need are
// masked, the ones it needs stay, and the shrunk trial reproduces it.
func TestShrinkTrial_MasksActorOps(t *testing.T) {
	tr := twoSiteHandTrial(15, true, 40)
	tr.Actors = Actors{Enabled: true, Ops: []Op{
		{At: 10, Kind: OpPlannedFailover, Site: "beta"},
		{At: 11, Kind: OpOOBLateWrite},
		{At: 12, Kind: OpPFStatusDeniedLong, N: 1000}, // the run outlives the trial
		{At: 13, Kind: OpReplacePod, Site: "alpha"},
	}}
	r := RunTrial(tr, false, nil)
	if !r.Failed() {
		t.Fatal("the hand trial does not fail")
	}
	fp := fingerprint(r.Violations)
	shrunk, v := ShrinkTrial(tr, fp)
	if fingerprint(v) != fp {
		t.Fatalf("shrunk trial fingerprint %s, want %s", fingerprint(v), fp)
	}
	if shrunk.Actors.Skip == nil || shrunk.Actors.Skip[0] || shrunk.Actors.Skip[2] || !shrunk.Actors.Skip[1] || !shrunk.Actors.Skip[3] {
		t.Fatalf("actor mask %v, want the request and the denial kept and the rest masked (fingerprint %s)", shrunk.Actors.Skip, fp)
	}
}
