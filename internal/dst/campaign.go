package dst

import (
	"fmt"
	"regexp"
	"runtime"
	"sort"
	"strings"
	"sync"
	"time"
)

// restartMarker matches a CooldownViolated detail that explicitly records at
// least one operator restart between the offending promotions.
var restartMarker = regexp.MustCompile(`operator restarts between: [1-9]`)

// lostStateMarker matches a CooldownViolated detail in which at least one of
// those restarts came up without the durable anti-flap record — which only
// happens when the out-of-band store was also rejecting writes, since the
// operator writes the record at promotion time and retries every poll.
var lostStateMarker = regexp.MustCompile(`lost the durable anti-flap record: [1-9]`)

// CampaignConfig controls a batch campaign.
type CampaignConfig struct {
	StartSeed uint64
	BatchSize int
	// DryBatches is the saturation rule: stop after this many consecutive
	// "dry" batches — a batch is dry when it produces no new failure
	// fingerprint and its new-signature rate falls below 1% of the batch
	// size ("diminishing returns"). A long tail of rare signature combos is
	// expected; requiring literally zero new signatures would never fire.
	DryBatches int
	MaxTrials  int           // hard cap; 0 = unlimited
	MaxWall    time.Duration // wall-clock cap; 0 = unlimited
	Workers    int           // 0 = GOMAXPROCS
	// ShrinkFailures minimizes the first failing trial of each distinct
	// fingerprint down to a minimal fault schedule.
	ShrinkFailures bool
	// Planned runs the actors-on trials (GeneratePlannedTrial), and shrinks
	// over both the fault ops and the actor ops (K090).
	Planned bool
}

// Failure is one distinct failure fingerprint with a shrunk reproduction.
type Failure struct {
	Fingerprint string
	Count       int
	FirstSeed   uint64
	Violations  []Violation
	// Repro is the minimized trial (Skip mask populated by the shrinker).
	Repro Trial
	// ReproViolations are the violations the minimized trial still produces.
	ReproViolations []Violation
}

// CampaignResult aggregates a full campaign run.
type CampaignResult struct {
	Trials       int
	Batches      int
	Signatures   int
	Failures     []Failure
	FailureCount int // total failing trials (pre-dedup)
	Wall         time.Duration
	// NewSigPerBatch shows the coverage curve: new signatures per batch.
	NewSigPerBatch []int
	StopReason     string
}

// fingerprint dedups failures by the set of violated invariants.
//
// CooldownViolated splits three ways, because only one of the three is
// inherent and the other two must not dedup into it and vanish:
//
//   - CooldownViolated — no restart between the two promotions. Always a
//     regression: one process ignored its own cooldown.
//   - CooldownViolated(restart) — a restart, but the durable anti-flap
//     record survived it. Also a regression: the restarted process had the
//     record and promoted anyway. Before the out-of-band store existed this
//     was the documented/expected class; it is a regression now.
//   - CooldownViolated(restart+stateLost) — a restart that came up without
//     the record, which requires BOTH durable paths to have been rejecting
//     writes. Inherent: nothing could have carried the cooldown across.
func fingerprint(violations []Violation) string {
	set := map[string]struct{}{}
	for _, v := range violations {
		key := v.Invariant
		// Fail safe: only EXPLICIT nonzero markers narrow the class;
		// anything unrecognized stays in the broadest regression class
		// rather than being silently excused.
		if v.Invariant == "CooldownViolated" && restartMarker.MatchString(v.Detail) {
			key = "CooldownViolated(restart)"
			if lostStateMarker.MatchString(v.Detail) {
				key = "CooldownViolated(restart+stateLost)"
			}
		}
		// The inherent restart+stateLost class keeps precedence over the
		// replacement tag (K171): both durable paths rejected the record
		// across the restart, whichever manager made it.
		if strings.Contains(v.Detail, replacedBeforeRecordMarker) && key != "CooldownViolated(restart+stateLost)" &&
			(v.Invariant == "AntiFlapStateLost" || v.Invariant == "CooldownViolated" || v.Invariant == "WedgedNoPrimary") {
			key = v.Invariant + "(replacement)"
		}
		if v.Invariant == "DualWritableUnresolved" && strings.Contains(v.Detail, confirmedClaimMarker) {
			key = "DualWritableUnresolved(confirmedClaim)"
		}
		// The planned-failover findings (README: the fixed ones, now
		// regressions, and the open ones) are classes of their own, so a
		// sweep names the finding a trial hit.
		switch {
		case v.Invariant == "HoldWithoutDataAtRisk" && strings.Contains(v.Detail, writableTargetWithChannelMarker):
			key = "HoldWithoutDataAtRisk(writableTargetWithChannel)"
		case v.Invariant == "HoldWithoutDataAtRisk" && strings.Contains(v.Detail, replacedTargetSameProcessMarker):
			key = "HoldWithoutDataAtRisk(replacedTargetSameProcess)"
		case v.Invariant == "HoldWithoutDataAtRisk" && strings.Contains(v.Detail, regressedTargetMarker):
			key = "HoldWithoutDataAtRisk(regressedTarget)"
		case (v.Invariant == "OutcomeDishonest" || v.Invariant == "SucceededBesideWritable") && strings.Contains(v.Detail, unreadablePeerMarker):
			key = v.Invariant + "(unreadablePeer)"
		case v.Invariant == "OutcomeDishonest" && strings.Contains(v.Detail, targetLacksFencedSetMarker):
			key = "OutcomeDishonest(targetLacksFencedSet)"
		case v.Invariant == "OutcomeDishonest" && strings.Contains(v.Detail, rolledBackAfterClearMarker):
			key = "OutcomeDishonest(rolledBackAfterClear)"
		case v.Invariant == "OutcomeDishonest" && strings.Contains(v.Detail, rolledBackFencedSourceMarker):
			key = "OutcomeDishonest(rolledBackFencedSource)"
		}
		set[key] = struct{}{}
	}
	keys := make([]string, 0, len(set))
	for k := range set {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	return strings.Join(keys, "+")
}

// RunCampaign runs seeded trials in parallel batches until the saturation
// rule fires or a cap is hit.
func RunCampaign(cfg CampaignConfig, progress func(string)) CampaignResult {
	if cfg.BatchSize == 0 {
		cfg.BatchSize = 500
	}
	if cfg.DryBatches == 0 {
		cfg.DryBatches = 3
	}
	if cfg.Workers <= 0 {
		cfg.Workers = runtime.GOMAXPROCS(0)
	}
	if progress == nil {
		progress = func(string) {}
	}

	start := time.Now()
	signatures := map[string]struct{}{}
	failures := map[string]*Failure{}
	var failureOrder []string
	res := CampaignResult{}
	seed := cfg.StartSeed
	dry := 0

	for {
		if cfg.MaxTrials > 0 && res.Trials >= cfg.MaxTrials {
			res.StopReason = "trial cap"
			break
		}
		if cfg.MaxWall > 0 && time.Since(start) > cfg.MaxWall {
			res.StopReason = "wall-clock cap"
			break
		}

		batchSize := cfg.BatchSize
		if cfg.MaxTrials > 0 && res.Trials+batchSize > cfg.MaxTrials {
			batchSize = cfg.MaxTrials - res.Trials // honor the cap exactly
		}
		batch := runBatch(seed, batchSize, cfg.Workers, cfg.generate())
		seed += uint64(batchSize)
		res.Trials += batchSize
		res.Batches++

		newSigs, newFails := 0, 0
		for _, tr := range batch {
			if _, ok := signatures[tr.Signature]; !ok {
				signatures[tr.Signature] = struct{}{}
				newSigs++
			}
			if tr.Failed() {
				res.FailureCount++
				fp := fingerprint(tr.Violations)
				if f, ok := failures[fp]; ok {
					f.Count++
				} else {
					failures[fp] = &Failure{
						Fingerprint: fp,
						Count:       1,
						FirstSeed:   tr.Trial.Seed,
						Violations:  tr.Violations,
					}
					failureOrder = append(failureOrder, fp)
					newFails++
				}
			}
		}
		res.NewSigPerBatch = append(res.NewSigPerBatch, newSigs)

		dryThreshold := cfg.BatchSize / 100
		if dryThreshold < 1 {
			dryThreshold = 1
		}
		if newSigs < dryThreshold && newFails == 0 {
			dry++
		} else {
			dry = 0
		}
		progress(fmt.Sprintf("batch %d: %d trials total, %d signatures (+%d), %d failing trials, %d distinct failures, dry=%d/%d",
			res.Batches, res.Trials, len(signatures), newSigs, res.FailureCount, len(failures), dry, cfg.DryBatches))

		if dry >= cfg.DryBatches {
			res.StopReason = fmt.Sprintf("saturated: %d consecutive batches with no new coverage", cfg.DryBatches)
			break
		}
	}

	res.Signatures = len(signatures)
	for _, fp := range failureOrder {
		f := failures[fp]
		if cfg.ShrinkFailures {
			progress("shrinking " + fp + " (seed " + fmt.Sprint(f.FirstSeed) + ")")
			f.Repro, f.ReproViolations = ShrinkTrial(cfg.generate()(f.FirstSeed), fp)
		} else {
			f.Repro = cfg.generate()(f.FirstSeed)
			f.ReproViolations = f.Violations
		}
		res.Failures = append(res.Failures, *f)
	}
	res.Wall = time.Since(start)
	return res
}

// generate is the campaign's trial generator.
func (cfg CampaignConfig) generate() func(uint64) Trial {
	if cfg.Planned {
		return GeneratePlannedTrial
	}
	return GenerateTrial
}

func runBatch(startSeed uint64, n, workers int, gen func(uint64) Trial) []TrialResult {
	results := make([]TrialResult, n)
	var wg sync.WaitGroup
	sem := make(chan struct{}, workers)
	for i := 0; i < n; i++ {
		wg.Add(1)
		sem <- struct{}{}
		go func(i int) {
			defer wg.Done()
			defer func() { <-sem }()
			results[i] = RunTrial(gen(startSeed+uint64(i)), false, nil)
		}(i)
	}
	wg.Wait()
	return results
}

// Shrink greedily removes fault-schedule ops from a failing trial while the
// failure fingerprint is preserved, returning the minimal trial and its
// violations. Deterministic replay makes this exact: same seed + same mask =
// same run.
func Shrink(seed uint64, wantFingerprint string) (Trial, []Violation) {
	return ShrinkTrial(GenerateTrial(seed), wantFingerprint)
}

// ShrinkTrial is Shrink for any generated trial: with actors on it masks
// the actor ops (Actors.Skip) as well as the fault ops (K090).
func ShrinkTrial(trial Trial, wantFingerprint string) (Trial, []Violation) {
	trial.Skip = make([]bool, len(trial.Ops))
	if trial.Actors.Enabled {
		trial.Actors.Skip = make([]bool, len(trial.Actors.Ops))
	}

	still := func(t Trial) ([]Violation, bool) {
		r := RunTrial(t, false, nil)
		return r.Violations, r.Failed() && fingerprint(r.Violations) == wantFingerprint
	}

	best, ok := still(trial)
	if !ok {
		// Non-reproducible under re-run would mean nondeterminism — surface
		// loudly by returning the unshrunk trial.
		return trial, best
	}

	// Pass 1..k: try dropping each active op of either schedule until a full
	// pass removes none.
	for changed := true; changed; {
		changed = false
		for _, mask := range [][]bool{trial.Skip, trial.Actors.Skip} {
			for i := range mask {
				if mask[i] {
					continue
				}
				mask[i] = true
				if v, ok := still(trial); ok {
					best = v
					changed = true
				} else {
					mask[i] = false
				}
			}
		}
	}
	return trial, best
}

// Report renders a human-readable campaign summary.
func (r CampaignResult) Report() string {
	var b strings.Builder
	fmt.Fprintf(&b, "DST campaign: %d trials in %d batches (%s), stopped: %s\n",
		r.Trials, r.Batches, r.Wall.Round(time.Millisecond), r.StopReason)
	fmt.Fprintf(&b, "behavior signatures: %d; coverage curve (new/batch): %v\n", r.Signatures, r.NewSigPerBatch)
	fmt.Fprintf(&b, "failing trials: %d; distinct failure fingerprints: %d\n", r.FailureCount, len(r.Failures))
	for _, f := range r.Failures {
		fmt.Fprintf(&b, "\n=== %s (%d trials, first seed %d) ===\n", f.Fingerprint, f.Count, f.FirstSeed)
		fmt.Fprintf(&b, "minimal repro:\n%s", indent(f.Repro.String(), "  "))
		fmt.Fprintf(&b, "violations:\n")
		for _, v := range f.ReproViolations {
			fmt.Fprintf(&b, "  %s\n", v)
		}
	}
	return b.String()
}

func indent(s, pad string) string {
	lines := strings.Split(strings.TrimRight(s, "\n"), "\n")
	for i := range lines {
		lines[i] = pad + lines[i]
	}
	return strings.Join(lines, "\n") + "\n"
}
