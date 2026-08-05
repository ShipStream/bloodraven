package dst

import (
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"strconv"
	"testing"
	"time"
)

// TestDeterminism is the property everything else rests on: the same seed
// must produce byte-identical behavior, twice, including under shrink masks.
func TestDeterminism(t *testing.T) {
	// maskEveryOther is the deterministic stand-in for a shrink mask: the
	// shrinker masks arbitrary subsets, so replay must be stable with Skip
	// set, not only with the full schedule.
	maskEveryOther := func(trial Trial) Trial {
		trial.Skip = make([]bool, len(trial.Ops))
		for i := range trial.Skip {
			trial.Skip[i] = i%2 == 0
		}
		return trial
	}
	for seed := uint64(1); seed <= 40; seed++ {
		for _, tc := range []struct {
			name string
			gen  func(uint64) Trial
		}{
			{"unmasked", GenerateTrial},
			{"masked", func(s uint64) Trial { return maskEveryOther(GenerateTrial(s)) }},
		} {
			a := RunTrial(tc.gen(seed), false, nil)
			b := RunTrial(tc.gen(seed), false, nil)
			if a.Signature != b.Signature {
				t.Fatalf("seed %d (%s): signatures differ:\n  a=%s\n  b=%s", seed, tc.name, a.Signature, b.Signature)
			}
			if fmt.Sprint(a.Violations) != fmt.Sprint(b.Violations) {
				t.Fatalf("seed %d (%s): violations differ:\n  a=%v\n  b=%v", seed, tc.name, a.Violations, b.Violations)
			}
			if fmt.Sprint(a.PromotionPolls) != fmt.Sprint(b.PromotionPolls) {
				t.Fatalf("seed %d (%s): promotion polls differ: %v vs %v", seed, tc.name, a.PromotionPolls, b.PromotionPolls)
			}
		}
	}
}

// TestFaultFreeBaseline: a trial whose schedule is empty must stay healthy
// and violation-free — otherwise the harness itself is broken.
func TestFaultFreeBaseline(t *testing.T) {
	for seed := uint64(1); seed <= 20; seed++ {
		trial := GenerateTrial(seed)
		trial.Skip = make([]bool, len(trial.Ops))
		for i := range trial.Skip {
			trial.Skip[i] = true
		}
		r := RunTrial(trial, false, nil)
		if len(r.Violations) > 0 {
			t.Fatalf("seed %d (no faults): unexpected violations: %v\ntrial:\n%s", seed, r.Violations, trial)
		}
		if r.FinalStatus.ActiveSite != trial.SiteNames[0] {
			t.Fatalf("seed %d (no faults): active site %q, want %q", seed, r.FinalStatus.ActiveSite, trial.SiteNames[0])
		}
	}
}

// TestDST_Quick is the CI-speed regression sweep: ANY invariant violation
// across the fixed seed range fails the build. The range is deterministic, so
// this is a stable gate, not a flake source — a new failure here means a real
// behavior change. If a documented known-limitation class (see
// README "Known finding classes") ever enters the range after an intentional
// change, regenerate the expectations knowingly rather than widening this
// filter silently.
//
// Note that seeds shift whenever the schedule generator changes — adding a
// fault kind or rebalancing faultWeights redraws every trial. That is why
// fixed findings are pinned as component tests (test/component/
// dst_regression_test.go) and as the hand-built trials in antiflap_test.go,
// rather than as seeds.
func TestDST_Quick(t *testing.T) {
	trials := 400
	if s := os.Getenv("DST_QUICK_TRIALS"); s != "" {
		if n, err := strconv.Atoi(s); err == nil {
			trials = n
		}
	}
	failures := map[string]uint64{}
	counts := map[string]int{}
	for seed := uint64(1); seed <= uint64(trials); seed++ {
		r := RunTrial(GenerateTrial(seed), false, nil)
		for _, v := range r.Violations {
			t.Errorf("seed %d: %s", seed, v)
		}
		if r.Failed() {
			fp := fingerprint(r.Violations)
			if _, ok := failures[fp]; !ok {
				failures[fp] = seed
			}
			counts[fp]++
		}
	}
	for fp, seed := range failures {
		t.Logf("finding: %s (%d/%d trials, first seed %d)", fp, counts[fp], trials, seed)
	}
}

// TestDST_Campaign runs the full saturation campaign. Gated behind DST_FULL=1
// (invoked via `make dst`); prints the report and never fails the build — it
// is the exploration tool, not the regression gate.
func TestDST_Campaign(t *testing.T) {
	if os.Getenv("DST_FULL") != "1" {
		t.Skip("set DST_FULL=1 (or run `make dst`) for the full saturation campaign")
	}
	cfg := CampaignConfig{
		StartSeed:      1,
		BatchSize:      envInt("DST_BATCH", 1000),
		DryBatches:     envInt("DST_DRY_BATCHES", 3),
		MaxTrials:      envInt("DST_MAX_TRIALS", 500_000),
		MaxWall:        time.Duration(envInt("DST_WALL_SECONDS", 1800)) * time.Second,
		ShrinkFailures: true,
	}
	res := RunCampaign(cfg, func(msg string) { t.Log(msg) })
	t.Log("\n" + res.Report())
	if path := os.Getenv("DST_REPORT"); path != "" {
		if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
			t.Errorf("create report dir: %v", err)
		} else if err := os.WriteFile(path, []byte(res.Report()), 0o644); err != nil {
			t.Errorf("write report: %v", err)
		}
	}
}

// TestDST_Repro replays one seed with full event capture and operator logs.
// Usage: DST_SEED=12345 [DST_SKIP=0,3,7] go test ./internal/dst -run Repro -v
func TestDST_Repro(t *testing.T) {
	seedStr := os.Getenv("DST_SEED")
	if seedStr == "" {
		t.Skip("set DST_SEED to replay a trial")
	}
	seed, err := strconv.ParseUint(seedStr, 10, 64)
	if err != nil {
		t.Fatalf("bad DST_SEED: %v", err)
	}
	trial := GenerateTrial(seed)
	if mask := os.Getenv("DST_SKIP"); mask != "" {
		trial.Skip = make([]bool, len(trial.Ops))
		for _, part := range splitCommas(mask) {
			i, err := strconv.Atoi(part)
			if err != nil || i < 0 || i >= len(trial.Ops) {
				t.Fatalf("bad DST_SKIP entry %q", part)
			}
			trial.Skip[i] = true
		}
	}
	t.Logf("trial:\n%s", trial)
	handler := slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelDebug})
	r := RunTrial(trial, true, handler)
	for _, e := range r.Events {
		t.Log(e)
	}
	t.Logf("promotions at polls %v; restarts at %v; restarts that lost the anti-flap record at %v",
		r.PromotionPolls, r.RestartPolls, r.LostStatePolls)
	t.Logf("sidecars believing they self-fenced at end: %v", r.SelfFenced)
	t.Logf("out-of-band anti-flap record: %+v", r.FinalFailoverRecord)
	t.Logf("final status: %+v", r.FinalStatus)
	for _, tr := range r.FinalTruth {
		t.Logf("truth %s: crashed=%v ro=%v sro=%v repl=%v src=%q io=%v sql=%v exec=%s",
			tr.Name, tr.Crashed, tr.ReadOnly, tr.SuperReadOnly, tr.ReplConfig, tr.SourceHost, tr.IORunning, tr.SQLRunning, tr.Executed)
	}
	for _, v := range r.Violations {
		t.Logf("VIOLATION: %s", v)
	}
}

func envInt(name string, def int) int {
	if s := os.Getenv(name); s != "" {
		if n, err := strconv.Atoi(s); err == nil {
			return n
		}
	}
	return def
}

func splitCommas(s string) []string {
	var out []string
	cur := ""
	for _, c := range s {
		if c == ',' {
			if cur != "" {
				out = append(out, cur)
			}
			cur = ""
		} else {
			cur += string(c)
		}
	}
	if cur != "" {
		out = append(out, cur)
	}
	return out
}
