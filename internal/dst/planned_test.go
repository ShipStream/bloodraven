package dst

import (
	"crypto/sha256"
	"fmt"
	"os"
	"reflect"
	"strconv"
	"testing"
)

// scheduleGolden is sha256 over GenerateTrial(seed).String() for seeds
// 1..2000, taken at the commit before the planned-failover actors existed.
// The actors draw from a stream of their own (§15.1, D18); this pins that
// adding them, or any later actor-stream change, leaves every existing
// seed's fault schedule byte-identical. A deliberate change to the fault
// generator updates it knowingly.
const scheduleGolden = "811becbb1b33126dca8a4eb681871d1b0d7e83215020d0110a690f93514a585a"

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
	for _, k := range []OpKind{OpOrderedUpdate, OpReplaceManager, OpRestartOperator} {
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

// plannedInherentFingerprint is the one finding class a trial with actors may
// produce (README, Known finding classes): OpPFStatusDenied combines a
// status outage with operator restarts, and when the fault stream denies the
// out-of-band store over the same restarts, nothing could carry the cooldown.
const plannedInherentFingerprint = "CooldownViolated(restart+stateLost)"

// TestDST_PlannedQuick is the actors-on sweep (§15.1 "Runs"): 300 seeds with
// every actor enabled. Any violation outside the documented inherent class
// fails the build. Seeds shift whenever the actor generator changes; fixed
// findings are pinned in test/component/dst_regression_test.go.
func TestDST_PlannedQuick(t *testing.T) {
	trials := 300
	if s := os.Getenv("DST_PLANNED_TRIALS"); s != "" {
		if n, err := strconv.Atoi(s); err == nil {
			trials = n
		}
	}
	inherent := 0
	for seed := uint64(1); seed <= uint64(trials); seed++ {
		r := RunTrial(GeneratePlannedTrial(seed), false, nil)
		if len(r.Violations) == 0 {
			continue
		}
		if fingerprint(r.Violations) == plannedInherentFingerprint {
			inherent++
			continue
		}
		for _, v := range r.Violations {
			t.Errorf("seed %d: %s", seed, v)
		}
	}
	t.Logf("%d/%d trials in the inherent class %s", inherent, trials, plannedInherentFingerprint)
}

// TestDST_PlannedRepro replays one actors-on seed with full capture.
// Usage: DST_SEED=12345 go test ./internal/dst -run PlannedRepro -v
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
	t.Logf("trial:\n%s", trial)
	r := RunTrial(trial, true, nil)
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
