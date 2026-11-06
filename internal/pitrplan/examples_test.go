package pitrplan_test

import (
	"testing"
	"time"

	"github.com/shipstream/bloodraven/internal/pitrarchive"
	"github.com/shipstream/bloodraven/internal/pitrplan"
)

// The worked examples of design §4.15 are the planner's oracle. Notation:
// A: is site a's UUID (server_id 101), Bu: site b's (102). Each site writes
// one file across role changes; a file is archived only when it seals while
// its site is writable. Default backup β1 is taken on b with B = A:1-40
// before t(A:45).

const ms500 = 500 * time.Millisecond

// era1Files archives a0 = A:1-30 and a1 = A:31-70 and records a's bootstrap
// promotion R_a0; withB0 also archives b0 = base A:1-10, A:11-60 (replica).
func era1Files(w *world, withB0 bool) {
	w.promote("a", "A", "", epoch.Add(-time.Hour))
	w.file("a", "a0", 101, "", "", "A:1-30")
	w.file("a", "a1", 101, "A:1-30", "", "A:31-70")
	if withB0 {
		w.file("b", "b0", 102, "", "A:1-10", "A:11-60")
	}
}

func beta1(w *world) { w.backup("A:1-40", "b", 102, w.at("A:42").Add(200*time.Millisecond)) }

var ts = pitrplan.ModeTimestamp

func TestExample1SingleSite(t *testing.T) {
	w := newWorld(t)
	w.commit("A:1-110")
	era1Files(w, false)
	w.file("a", "a2", 101, "A:1-70", "", "A:71-100")
	beta1(w)
	p := w.check("ex1", query{mode: ts, stop: w.in("A:80")}, want{outcome: pitrplan.OutcomeTargetCovered, witness: "A:80", truth: pitrplan.GroundTruthOrigin,
		lambda: "A:1-79", replay: "A:41-79", post: ptr("A:80-100"),
		segs: []seg{{"a/a1", "A:41-70", "A:1-40"}, {"a/a2", "A:71-79", "A:1-70"}}})
	// a0 is left unread on its hint (hmax t(A:30) < t(A:40)), so t(A:40)
	// is a lower bound (review F005) and the target passed D-52 instead.
	if p.BackupFrontierProven || p.BackupNewestCommit != w.at("A:40").UnixMicro() {
		t.Errorf("frontier = %v %d, want unproven t(A:40)", p.BackupFrontierProven, p.BackupNewestCommit)
	}
	// Example 8(a): latest mode on the same archive.
	w.check("ex8a", query{mode: pitrplan.ModeLatest}, want{outcome: pitrplan.OutcomeArchiveHead, lambda: "A:1-100",
		segs: []seg{{"a/a1", "A:41-70", ""}, {"a/a2", "A:71-100", ""}}})
}

// example2 is the planned failover A→B at A:100.
func example2(t *testing.T, withB1 bool) *world {
	w := newWorld(t)
	w.commit("A:1-100")
	tauB := w.pass(2 * time.Second)
	w.commit("Bu:1-50")
	era1Files(w, true)
	w.promote("b", "Bu", "A:1-100", tauB)
	if withB1 {
		w.file("b", "b1", 102, "A:1-60", "", "A:61-100", "Bu:1-50")
	}
	beta1(w)
	return w
}

func TestExample2PlannedFailover(t *testing.T) {
	w := example2(t, false)
	w.check("2a", query{stop: w.in("A:80")}, want{token: pitrarchive.TokenTargetNotCovered})
	w.check("2a partial", query{stop: w.in("A:80"), partial: true}, want{outcome: pitrplan.OutcomePartial, lambda: "A:1-70",
		segs: []seg{{"a/a1", "A:41-70", ""}}})

	w = example2(t, true)
	p := w.check("2b", query{stop: w.in("A:80")}, want{outcome: pitrplan.OutcomeTargetCovered, witness: "A:80", truth: pitrplan.GroundTruthAnchored,
		lambda: "A:1-79", segs: []seg{{"a/a1", "A:41-70", ""}, {"b/b1", "A:71-79", "A:1-70"}}})
	if a := p.Witness; len(a.AnchorRecords) != 1 || a.AnchorRecords[0] != "a#0" || a.AnchorOriginRow == "" || a.AnchorOriginRow[:5] != "a/a1/" {
		t.Errorf("2b anchor = %+v, want R_a0 and a1", a)
	}
	w.check("2c", query{stop: w.in("Bu:21")}, want{outcome: pitrplan.OutcomeTargetCovered, witness: "Bu:21", truth: pitrplan.GroundTruthOrigin,
		lambda: "A:1-100,Bu:1-20", segs: []seg{{"a/a1", "A:41-70", ""}, {"b/b1", "A:71-100,Bu:1-20", ""}}})
}

// example3 is the headline failback without reclone: a short b era with no
// b rotation, b's file never archived; a's mixed file a2 holds everything.
// b's bootstrap clone bridge b0 is retained: a clone bridge row is no fence
// (N3 revised); only b's records date its incarnations.
func example3(t *testing.T, lag bool) *world {
	w := newWorld(t)
	w.commit("A:1-100")
	tauB := w.pass(2 * time.Second)
	if lag {
		w.commit("Bu:1-3")
		tauB = w.pass(500 * time.Millisecond)
		w.commit("Bu:4-50")
	} else {
		w.commit("Bu:1-50")
	}
	tauA3 := w.pass(2 * time.Second)
	w.commit("A:101-135")
	era1Files(w, true)
	if lag {
		w.promote("b", "Bu", "A:1-100,Bu:1-3", tauB)
	} else {
		w.promote("b", "Bu", "A:1-100", tauB)
	}
	w.promote("a", "A", "A:1-100,Bu:1-50", tauA3)
	w.file("a", "a2", 101, "A:1-70", "", "A:71-100", "Bu:1-50", "A:101-110")
	beta1(w)
	return w
}

func TestExample3FailbackWithoutReclone(t *testing.T) {
	w := example3(t, false)
	w.check("3i", query{stop: w.in("A:80")}, want{outcome: pitrplan.OutcomeTargetCovered, witness: "A:80", truth: pitrplan.GroundTruthOrigin,
		lambda: "A:1-79", segs: []seg{{"a/a1", "A:41-70", ""}, {"a/a2", "A:71-79", ""}}})
	p := w.check("3ii", query{stop: w.in("Bu:36")}, want{outcome: pitrplan.OutcomeTargetCovered, witness: "Bu:36", truth: pitrplan.GroundTruthAnchored,
		lambda: "A:1-100,Bu:1-35", segs: []seg{{"a/a1", "A:41-70", ""}, {"a/a2", "A:71-100,Bu:1-35", ""}}})
	if a := p.Witness; len(a.AnchorRecords) != 1 || a.AnchorRecords[0] != "b#0" || a.AnchorOriginRow != "" {
		t.Errorf("3ii anchor = %+v, want R_b only", a)
	}
	w.check("3iii", query{stop: w.in("A:106")}, want{outcome: pitrplan.OutcomeTargetCovered, witness: "A:106", truth: pitrplan.GroundTruthOrigin,
		lambda: "A:1-100,Bu:1-50,A:101-105", segs: []seg{{"a/a1", "A:41-70", ""}, {"a/a2", "A:71-100,Bu:1-50,A:101-105", ""}}})
	// Example 8(b): latest mode; A:101-110 are not in R_b, but they were
	// committed long after τ_b + δ.
	w.check("8b", query{mode: pitrplan.ModeLatest}, want{outcome: pitrplan.OutcomeArchiveHead, lambda: "A:1-110,Bu:1-50",
		segs: []seg{{"a/a1", "A:41-70", ""}, {"a/a2", "A:71-100,Bu:1-50,A:101-110", ""}}})
	// Example 8(e): a dump newer than the archive.
	w.backup("A:1-135,Bu:1-50", "a", 101, w.at("A:135").Add(time.Second))
	w.check("8e", query{mode: pitrplan.ModeLatest}, want{token: pitrarchive.TokenNoArchivedReplay})

	// Lag variant: b commits Bu:1-3 before its sidecar records the
	// promotion, so R_b holds Bu:2 and cannot anchor it (D-53).
	w = example3(t, true)
	tauB := w.manifest("b").Promotions[0].ObservedAt
	anchorFrom := tauB.Add(5 * time.Second).Format(microLayout)
	w.check("3ii lag", query{stop: w.in("Bu:2")}, want{token: pitrarchive.TokenStopAmbiguous,
		msg: []string{"archived only as a replica copy on a", "after " + anchorFrom + " (b's promotion record at " + tauB.Format(microLayout),
			"safe targets are at or before " + w.at("A:100").Format(microLayout) + ", or after " + anchorFrom}})
	// G1: R_b anchors only witnesses committed more than δ after it.
	// Bu:5 (2 s after) is within the margin.
	w.check("3ii lag, inside the margin", query{stop: w.in("Bu:5")}, want{token: pitrarchive.TokenStopAmbiguous,
		msg: []string{"after " + anchorFrom + " (b's promotion record at " + tauB.Format(microLayout) + ", which is not provably before"}})
	w.check("3ii lag, after the record", query{stop: w.in("Bu:10")}, want{outcome: pitrplan.OutcomeTargetCovered, witness: "Bu:10",
		truth: pitrplan.GroundTruthAnchored, lambda: "A:1-100,Bu:1-9", segs: []seg{{"a/a1", "A:41-70", ""}, {"a/a2", "A:71-100,Bu:1-9", ""}}})

	// N3 revised: a reset record of b (b recloned, a sidecar restart that
	// found b's history replaced) observed after R_b and not provably
	// after Bu:36's episode began fences R_b off; one observed long enough
	// after Bu:36 does not.
	for _, tc := range []struct {
		name   string
		at     time.Time
		pi     time.Duration
		fenced bool
	}{
		{"reset inside b's era", w.at("Bu:40"), 5 * time.Second, true},
		{"reset after Bu:36 within its probe interval", w.at("Bu:36").Add(9 * time.Second), 5 * time.Second, true},
		{"reset after Bu:36 beyond its probe interval and δ", w.at("Bu:36").Add(11 * time.Second), 5 * time.Second, false},
		{"start-up reset reaching back to b's record", w.at("Bu:36").Add(time.Hour), time.Hour + time.Minute, true},
		{"reset with an unknown probe interval", w.at("Bu:36").Add(24 * time.Hour), 0, true},
	} {
		w := example3(t, false)
		w.record("b", pitrarchive.RecordReset, "Bu", "A:1-100,Bu:1-50", tc.at, tc.pi)
		if !tc.fenced {
			w.check("3ii, "+tc.name, query{stop: w.in("Bu:36")}, want{outcome: pitrplan.OutcomeTargetCovered, witness: "Bu:36", truth: pitrplan.GroundTruthAnchored,
				lambda: "A:1-100,Bu:1-35", segs: []seg{{"a/a1", "A:41-70", ""}, {"a/a2", "A:71-100,Bu:1-35", ""}}})
			continue
		}
		w.check("3ii, "+tc.name, query{stop: w.in("Bu:36")}, want{token: pitrarchive.TokenStopAmbiguous,
			msg: []string{"archived only as a replica copy on a", "the promotion records b#0 are not anchors, because a later record of site b, b#1 (an incarnation-reset record observed at " + tc.at.UTC().Format(microLayout)}})
	}
}

const microLayout = "2006-01-02T15:04:05.000000Z"

// TestExampleG1RecloneRecordIsNoAnchor is review round 1's G1: A:85
// survives only on b, whose copy has b's errant Bu:1 before A:61. a dies
// with A:61-85 unarchived, is recloned from c (A:1-60,Bu:1, no A:85) and
// re-promoted; its record R_a' = A:1-60,Bu:1 does not hold A:85, but it was
// observed after A:85 committed, so it says nothing about a's history at
// A:85. Anchoring on it would put Bu:1 into Λ.
func TestExampleG1RecloneRecordIsNoAnchor(t *testing.T) {
	build := func(withCloneFile bool) *world {
		w := newWorld(t)
		w.commit("A:1-60")
		w.commit("Bu:1")
		w.commit("A:61-90")
		tauA := w.pass(time.Minute)
		w.promote("a", "A", "", epoch.Add(-time.Hour))
		w.file("a", "a0", 101, "", "", "A:1-30")
		w.file("a", "a1", 101, "A:1-30", "", "A:31-60")
		w.file("b", "b0", 102, "", "A:1-10", "A:11-60")
		w.file("b", "b1", 102, "A:1-60", "", "Bu:1", "A:61-90")
		w.promote("a", "A", "A:1-60,Bu:1", tauA)
		if withCloneFile {
			// a rotated after its promotion: the clone's first file.
			w.file("a", "a0", 101, "", "A:1-60,Bu:1")
		}
		w.backup("A:1-40", "b", 102, w.at("A:42").Add(200*time.Millisecond))
		return w
	}
	for _, withCloneFile := range []bool{false, true} {
		w := build(withCloneFile)
		w.check("G1", query{stop: w.in("A:85")}, want{token: pitrarchive.TokenStopAmbiguous,
			msg: []string{"archived only as a replica copy on b", "predecessors include {Bu}:1", "Exclude {Bu}:1"}})
		w.check("G1 X", query{stop: w.in("A:85"), exclude: "Bu:1"}, want{outcome: pitrplan.OutcomeTargetCovered, witness: "A:85",
			truth: pitrplan.GroundTruthAnchored, lambda: "A:1-84", div: "Bu:1", segs: []seg{{"a/a1", "A:41-60", ""}, {"b/b1", "A:61-84", ""}}})
	}
}

// example4 is the failback with reclone (s68): a is recloned from b at
// A:1-100,Bu:1-50 and writes A:101-130 into a new first file (name reused,
// new id, empty Previous_gtids, base = the clone point).
func example4(t *testing.T, flushB bool) *world {
	w := newWorld(t)
	w.commit("A:1-100")
	tauB := w.pass(2 * time.Second)
	w.commit("Bu:1-50")
	tauA := w.pass(time.Minute)
	w.commit("A:101-130")
	era1Files(w, true)
	w.promote("b", "Bu", "A:1-100", tauB)
	if flushB {
		w.file("b", "b1", 102, "A:1-60", "", "A:61-100", "Bu:1-50")
	}
	w.promote("a", "A", "A:1-100,Bu:1-50", tauA)
	w.file("a", "a0", 101, "", "A:1-100,Bu:1-50", "A:101-110")
	beta1(w)
	return w
}

func TestExample4FailbackWithReclone(t *testing.T) {
	w := example4(t, true)
	p := w.check("4i", query{stop: w.in("A:80")}, want{outcome: pitrplan.OutcomeTargetCovered, witness: "A:80", truth: pitrplan.GroundTruthAnchored,
		lambda: "A:1-79", segs: []seg{{"a/a1", "A:41-70", ""}, {"b/b1", "A:71-79", ""}}})
	if a := p.Witness; len(a.AnchorRecords) != 1 || a.AnchorRecords[0] != "a#0" {
		t.Errorf("4i anchor records = %v, want only R_a0 (R_a' holds A:80)", a.AnchorRecords)
	}
	w.check("4ii", query{stop: w.in("Bu:36")}, want{outcome: pitrplan.OutcomeTargetCovered, witness: "Bu:36", truth: pitrplan.GroundTruthOrigin,
		lambda: "A:1-100,Bu:1-35", segs: []seg{{"a/a1", "A:41-70", ""}, {"b/b1", "A:71-100,Bu:1-35", ""}}})
	w.check("4iii", query{stop: w.in("A:106")}, want{outcome: pitrplan.OutcomeTargetCovered, witness: "A:106", truth: pitrplan.GroundTruthOrigin,
		lambda: "A:1-105,Bu:1-50", segs: []seg{{"a/a1", "A:41-70", ""}, {"b/b1", "A:71-100,Bu:1-50", ""}, {"a/a0", "A:101-105", "A:1-100,Bu:1-50"}}})

	// 4g: b's era is never archived before the reclone.
	w = example4(t, false)
	gap := []string{"{A}:71-100", "never archived", "unarchived tail", "in the base of a/a0",
		"choose a target at or before " + w.at("A:70").Format(microLayout), "backup taken after " + w.at("A:101").Format(microLayout)}
	w.check("4g", query{stop: w.in("A:106")}, want{token: pitrarchive.TokenArchiveGap, msg: gap})
	w.check("4g partial", query{stop: w.in("A:106"), partial: true}, want{token: pitrarchive.TokenArchiveGap, msg: gap})
	w.check("4g (ii)", query{stop: w.in("Bu:36")}, want{token: pitrarchive.TokenArchiveGap, msg: []string{"never archived"}})
}

// example5 is a lossy failover: b had applied only A:1-95 when a died.
func example5(t *testing.T, withB1 bool) *world {
	w := newWorld(t)
	w.commit("A:1-110")
	tauB := w.pass(10 * time.Second)
	w.commit("Bu:1-50")
	era1Files(w, true)
	w.file("a", "a2", 101, "A:1-70", "", "A:71-100")
	w.promote("b", "Bu", "A:1-95", tauB)
	if withB1 {
		w.file("b", "b1", 102, "A:1-60", "", "A:61-95", "Bu:1-50")
	}
	beta1(w)
	return w
}

func TestExample5LossyFailover(t *testing.T) {
	w := example5(t, true)
	w.check("5i", query{stop: w.in("A:91")}, want{outcome: pitrplan.OutcomeTargetCovered, witness: "A:91", truth: pitrplan.GroundTruthOrigin,
		lambda: "A:1-90", segs: []seg{{"a/a1", "A:41-70", ""}, {"a/a2", "A:71-90", ""}}})
	w.check("5ii", query{stop: w.in("A:98")}, want{outcome: pitrplan.OutcomeTargetCovered, witness: "A:98", lambda: "A:1-97", truth: pitrplan.GroundTruthOrigin,
		segs: []seg{{"a/a1", "A:41-70", ""}, {"a/a2", "A:71-97", ""}}})
	w.check("5iii", query{stop: w.in("Bu:1")}, want{token: pitrarchive.TokenArchiveDivergentHistory,
		msg: []string{"5 archived transactions", "{A}:96-100", "choose a target at or before " + w.at("A:96").Format(microLayout)}})
	w.check("5iii X", query{stop: w.in("Bu:1"), exclude: "A:96-100"}, want{outcome: pitrplan.OutcomeTargetCovered, witness: "Bu:1", truth: pitrplan.GroundTruthOrigin,
		lambda: "A:1-95", div: "A:96-100", segs: []seg{{"a/a1", "A:41-70", ""}, {"a/a2", "A:71-95", ""}}})
	w.check("5iv", query{stop: w.in("Bu:21")}, want{token: pitrarchive.TokenArchiveDivergentHistory, msg: []string{"{A}:96-100"}})
	w.check("5iv X", query{stop: w.in("Bu:21"), exclude: "A:96-100"}, want{outcome: pitrplan.OutcomeTargetCovered, witness: "Bu:21", truth: pitrplan.GroundTruthOrigin,
		lambda: "A:1-95,Bu:1-20", div: "A:96-100", segs: []seg{{"a/a1", "A:41-70", ""}, {"b/b1", "A:71-95,Bu:1-20", ""}}})

	// β2: taken on b in era 2 at Bu:10; the D window starts after A:96-100.
	w.backup("A:1-95,Bu:1-10", "b", 102, w.at("Bu:12").Add(200*time.Millisecond))
	p := w.check("5v", query{stop: w.in("Bu:21")}, want{outcome: pitrplan.OutcomeTargetCovered, witness: "Bu:21", truth: pitrplan.GroundTruthOrigin,
		lambda: "A:1-95,Bu:1-20", segs: []seg{{"b/b1", "Bu:11-20", "A:1-60,A:61-95,Bu:1-10"}}})
	if p.BackupNewestCommit != w.at("Bu:10").UnixMicro() {
		t.Errorf("5v maxOct(B) = %d, want t(Bu:10)", p.BackupNewestCommit)
	}
	w.check("5vi", query{stop: w.in("A:80")}, want{token: pitrarchive.TokenBackupAfterTarget})

	// Example 8(c), (d): latest mode before and after b1 is archived.
	w = example5(t, false)
	tauB := w.manifest("b").Promotions[0].ObservedAt
	w.check("8c", query{mode: pitrplan.ModeLatest}, want{token: pitrarchive.TokenArchiveDivergentHistory,
		msg: []string{"{A}:96-100", "b was promoted at " + tauB.Format(microLayout)}})
	w.check("8c X", query{mode: pitrplan.ModeLatest, exclude: "A:96-100"}, want{outcome: pitrplan.OutcomeArchiveHead, lambda: "A:1-100", lost: "A:96-100",
		segs: []seg{{"a/a1", "A:41-70", ""}, {"a/a2", "A:71-95", ""}}})
	w = example5(t, true)
	w.check("8d", query{mode: pitrplan.ModeLatest}, want{token: pitrarchive.TokenArchiveSplitBrain})
	w.check("8d X", query{mode: pitrplan.ModeLatest, exclude: "A:96-100"}, want{outcome: pitrplan.OutcomeArchiveHead, lambda: "A:1-95,Bu:1-50",
		segs: []seg{{"a/a1", "A:41-70", ""}, {"b/b1", "A:71-95,Bu:1-50", ""}}})
}

// example6 has an errant transaction Bu:1 ("Bu:e") written on replica b
// between A:50 and A:51; b's own era 2 is then Bu:2-21.
func example6(t *testing.T) *world {
	w := newWorld(t)
	w.commit("A:1-50")
	w.commit("Bu:1")
	w.commit("A:51-100")
	tauB := w.pass(2 * time.Second)
	w.commit("Bu:2-21")
	w.promote("a", "A", "", epoch.Add(-time.Hour))
	w.file("a", "a0", 101, "", "", "A:1-30")
	w.file("a", "a1", 101, "A:1-30", "", "A:31-70")
	w.file("b", "b0", 102, "", "A:1-10", "A:11-50", "Bu:1", "A:51-60")
	w.promote("b", "Bu", "A:1-100,Bu:1", tauB)
	w.file("b", "b1", 102, "A:1-60,Bu:1", "", "A:61-100", "Bu:2-21")
	beta1(w)
	return w
}

func TestExample6Errant(t *testing.T) {
	w := example6(t)
	w.check("6i", query{stop: w.in("A:80")}, want{token: pitrarchive.TokenStopAmbiguous, msg: []string{"predecessors include {Bu}:1", "Exclude {Bu}:1"}})
	w.check("6i X", query{stop: w.in("A:80"), exclude: "Bu:1"}, want{outcome: pitrplan.OutcomeTargetCovered, witness: "A:80", truth: pitrplan.GroundTruthAnchored,
		lambda: "A:1-79", div: "Bu:1", segs: []seg{{"a/a1", "A:41-70", ""}, {"b/b1", "A:71-79", "A:1-70,Bu:1"}}})
	w.check("6ii", query{stop: w.in("Bu:12")}, want{outcome: pitrplan.OutcomeTargetCovered, witness: "Bu:12", truth: pitrplan.GroundTruthOrigin,
		lambda: "A:1-100,Bu:1-11", segs: []seg{{"b/b0", "A:41-50,Bu:1,A:51-60", ""}, {"b/b1", "A:61-100,Bu:2-11", ""}}})
	w.check("6iii", query{stop: w.in("Bu:1")}, want{outcome: pitrplan.OutcomeTargetCovered, witness: "Bu:1", truth: pitrplan.GroundTruthOrigin,
		lambda: "A:1-50", segs: []seg{{"a/a1", "A:41-50", ""}}})

	// (iv) A backup taken on b after the errant.
	w.backup("A:1-55,Bu:1", "b", 102, w.at("A:56").Add(200*time.Millisecond))
	w.check("6iv", query{stop: w.in("A:80")}, want{token: pitrarchive.TokenStopAmbiguous})
	w.check("6iv X", query{stop: w.in("A:80"), exclude: "Bu:1"}, want{token: pitrarchive.TokenExcludeIntersectsBackup})
	w.file("a", "a2", 101, "A:1-70", "", "A:71-100")
	w.check("6iv origin", query{stop: w.in("A:80")}, want{token: pitrarchive.TokenBackupNotOnLineage, msg: []string{"the backup holds {Bu}:1"}})
}

func TestExample6InjectedEmpty(t *testing.T) {
	// (v) b is never promoted. The DBA injects an empty Bu:1 on a between
	// A:52 and A:53 (so it lands in a1); b's own Bu:1 has effects.
	w := newWorld(t)
	w.commit("A:1-50")
	w.commit("Bu:1")
	w.commit("A:51-52")
	injected := w.pass(500 * time.Millisecond)
	w.commit("A:53-100")
	w.promote("a", "A", "", epoch.Add(-time.Hour))
	w.file("a", "a0", 101, "", "", "A:1-30")
	w.clock["Bu:1"] = injected
	w.file("a", "a1", 101, "A:1-30", "", "A:31-52", "Bu:1~@101", "A:53-70")
	w.file("a", "a2", 101, "A:1-70,Bu:1", "", "A:71-100")
	w.backup("A:1-60,Bu:1", "b", 102, w.at("A:61").Add(200*time.Millisecond))
	w.check("6v", query{stop: w.in("A:80")}, want{token: pitrarchive.TokenBackupNotOnLineage,
		msg: []string{"injected empty transaction under {Bu}:1", "take the backup on the site that injected it, a (server_id 101)"}})
	w.backup("A:1-60,Bu:1", "a", 101, w.at("A:61").Add(200*time.Millisecond))
	w.check("6v on a", query{stop: w.in("A:80")}, want{outcome: pitrplan.OutcomeTargetCovered, witness: "A:80", truth: pitrplan.GroundTruthOrigin,
		lambda: "A:1-79,Bu:1", segs: []seg{{"a/a1", "A:61-70", ""}, {"a/a2", "A:71-79", ""}}})
}

func TestExample7SplitBrain(t *testing.T) {
	w := newWorld(t)
	w.commit("A:1-100")
	tauB := w.pass(2 * time.Second)
	w.commit("A:101-110")
	w.commit("Bu:1-15")
	w.pass(2 * time.Second)
	w.commit("Bu:16")
	w.commit("A:111-120")
	w.commit("Bu:17-30")
	era1Files(w, true)
	w.file("a", "a2", 101, "A:1-70", "", "A:71-120")
	w.promote("b", "Bu", "A:1-100", tauB)
	w.file("b", "b1", 102, "A:1-60", "", "A:61-100", "Bu:1-30")
	beta1(w)
	T := w.in("Bu:16")
	w.check("7", query{stop: T}, want{token: pitrarchive.TokenArchiveDivergentHistory, msg: []string{"{A}:101-110", "excludeGtids", "{Bu}:1-30"}})
	w.check("7 keep a", query{stop: T, exclude: "Bu:1-30"}, want{outcome: pitrplan.OutcomeTargetCovered, witness: "A:111", truth: pitrplan.GroundTruthOrigin,
		lambda: "A:1-110", div: "Bu:1-15", segs: []seg{{"a/a1", "A:41-70", ""}, {"a/a2", "A:71-110", ""}}})
	w.check("7 latest", query{mode: pitrplan.ModeLatest}, want{token: pitrarchive.TokenArchiveSplitBrain})
	w.check("7 latest keep a", query{mode: pitrplan.ModeLatest, exclude: "Bu:1-30"}, want{outcome: pitrplan.OutcomeArchiveHead, lambda: "A:1-120",
		segs: []seg{{"a/a1", "A:41-70", ""}, {"a/a2", "A:71-120", ""}}})
	w.check("7 latest keep b", query{mode: pitrplan.ModeLatest, exclude: "A:101-120"}, want{outcome: pitrplan.OutcomeArchiveHead, lambda: "A:1-100,Bu:1-30",
		segs: []seg{{"a/a1", "A:41-70", ""}, {"b/b1", "A:71-100,Bu:1-30", ""}}})
}

func TestExample8EmptyHead(t *testing.T) {
	// (f) The head file is empty with prev A:1-20; A:16-20 were never
	// archived.
	w := newWorld(t)
	w.commit("A:1-20")
	w.promote("a", "A", "", epoch.Add(-time.Hour))
	w.file("a", "f1", 101, "", "", "A:1-15")
	w.file("a", "f2", 101, "A:1-20", "")
	w.backup("A:1-10", "a", 101, w.at("A:11"))
	w.check("8f", query{mode: pitrplan.ModeLatest}, want{token: pitrarchive.TokenArchiveGap, msg: []string{"{A}:16-20"}})
}

func TestExample9RoundTwo(t *testing.T) {
	t.Run("G2-1 dump bridges a gap", func(t *testing.T) {
		w := newWorld(t)
		w.commit("A:1-30")
		w.promote("a", "A", "", epoch.Add(-time.Hour))
		w.file("a", "r1", 101, "", "", "A:1-10")
		w.file("a", "r2", 101, "A:1-20", "", "A:21-30")
		w.backup("A:1-20", "a", 101, w.at("A:20").Add(ms500))
		w.check("before the dump end", query{stop: w.in("A:16")}, want{token: pitrarchive.TokenBackupAfterTarget})
		p := w.check("after", query{stop: w.in("A:26")}, want{outcome: pitrplan.OutcomeTargetCovered, witness: "A:26", truth: pitrplan.GroundTruthOrigin,
			lambda: "A:1-25", segs: []seg{{"a/r2", "A:21-25", ""}}})
		if p.BackupFrontierProven {
			t.Error("A:11-20 are not archived, so maxOct(B) is not proven")
		}
	})
	t.Run("G2-5 ties", func(t *testing.T) {
		w := newWorld(t)
		w.commit("A:1-51")
		w.commitAt("A:52", w.at("A:51"))
		w.commit("A:53-70")
		era1Files(w, false)
		beta1(w)
		w.check("causal successor", query{stop: w.at("A:51")}, want{outcome: pitrplan.OutcomeTargetCovered, witness: "A:51", truth: pitrplan.GroundTruthOrigin,
			lambda: "A:1-50", segs: []seg{{"a/a1", "A:41-50", ""}}})
		// An unrelated tie on another writer with the same predecessors
		// proceeds; with different predecessors it refuses.
		w.commitAt("Cu:1", w.at("A:51"))
		w.file("c", "c1", 103, "A:1-50", "", "Cu:1")
		w.check("equal pred", query{stop: w.at("A:51")}, want{outcome: pitrplan.OutcomeTargetCovered, witness: "A:51", truth: pitrplan.GroundTruthOrigin,
			lambda: "A:1-50", segs: []seg{{"a/a1", "A:41-50", ""}}})
		w.manifest("c").Files = nil
		w.file("c", "c1", 103, "A:1-49", "", "Cu:1")
		w.check("unequal pred", query{stop: w.at("A:51")}, want{token: pitrarchive.TokenStopAmbiguous, msg: []string{"on different histories"}})
	})
	t.Run("G2-6 record anchors without an origin file", func(t *testing.T) {
		w := example2(t, true)
		w.manifest("a").Files = nil
		p := w.check("2b without a's files", query{stop: w.in("A:80")}, want{outcome: pitrplan.OutcomeTargetCovered, witness: "A:80",
			truth: pitrplan.GroundTruthAnchored, lambda: "A:1-79", segs: []seg{{"b/b0", "A:41-60", ""}, {"b/b1", "A:61-79", ""}}})
		if p.Witness.AnchorOriginRow != "" || len(p.Witness.AnchorRecords) != 1 {
			t.Errorf("anchor = %+v, want R_a0 only", p.Witness)
		}
	})
	t.Run("G2-7 on-history exclusion", func(t *testing.T) {
		w := newWorld(t)
		w.commit("A:1-70")
		era1Files(w, false)
		beta1(w)
		w.check("x then y", query{stop: w.in("A:45"), exclude: "A:41"}, want{outcome: pitrplan.OutcomeTargetCovered, witness: "A:45", truth: pitrplan.GroundTruthOrigin,
			lambda: "A:1-44", onHist: "A:41", segs: []seg{{"a/a1", "A:42-44", "A:1-40"}}})
	})
	t.Run("O2-4 quarantine waiver", func(t *testing.T) {
		w := newWorld(t)
		w.commit("A:1-100")
		era1Files(w, false)
		w.file("a", "a2", 101, "A:1-70", "", "A:71-100")
		w.lose("a", "a1")
		qAt := w.at("A:85").Add(100 * time.Millisecond)
		w.quarantine("a", "a1", qAt)
		// Stamped δ + 1 s after the quarantine.
		w.backup("A:1-85", "a", 101, qAt.Add(7*time.Second))
		w.check("waived", query{stop: w.in("A:95")}, want{outcome: pitrplan.OutcomeTargetCovered, witness: "A:95", truth: pitrplan.GroundTruthOrigin,
			lambda: "A:1-94", segs: []seg{{"a/a2", "A:86-94", "A:1-85"}}})
		beta1(w)
		w.check("not waived", query{stop: w.in("A:95")}, want{token: pitrarchive.TokenArchiveNamespaceReuse, msg: []string{"cannot verify a/a1"}})
	})
	t.Run("O2-6 reissue after the dump", func(t *testing.T) {
		w := newWorld(t)
		w.commit("A:1-100")
		tauB := w.pass(2 * time.Second)
		tauA := w.pass(time.Hour)
		w.commit("A:91-130") // reissued
		era1Files(w, true)
		w.promote("b", "Bu", "A:1-90", tauB)
		w.promote("a", "A", "A:1-90", tauA)
		w.file("a", "a0", 101, "", "A:1-90", "A:91-130")
		w.backup("A:1-95", "a", 101, tauB.Add(-time.Second))
		w.check("reissued", query{stop: w.in("A:121")}, want{token: pitrarchive.TokenArchiveNamespaceReuse, msg: []string{"after this dump finished", "{A}:95"}})
	})
}

func TestExample10CarriedOver(t *testing.T) {
	t.Run("clock skew", func(t *testing.T) {
		// 10:05:SS.mmm, after era 1's commits.
		at := func(s string) time.Time {
			v, err := time.Parse("04:05.000", s)
			if err != nil {
				t.Fatal(err)
			}
			return time.Date(2026, 11, 1, 10, 5+v.Minute(), v.Second(), v.Nanosecond(), time.UTC)
		}
		for _, variant := range []string{"skew", "late"} {
			w := newWorld(t)
			w.commit("A:1-99")
			w.commitAt("A:100", at("00:05.000"))
			w.commitAt("Bu:1", at("00:03.000"))
			if variant == "skew" {
				w.commitAt("Bu:2", at("00:06.000"))
			} else {
				w.commitAt("Bu:2", at("00:04.500"))
			}
			era1Files(w, true)
			w.file("a", "a2", 101, "A:1-70", "", "A:71-100")
			w.promote("b", "Bu", "A:1-100", at("00:02.000"))
			w.file("b", "b1", 102, "A:1-60", "", "A:61-100", "Bu:1-2")
			beta1(w)
			if variant == "skew" {
				w.check("skew", query{stop: at("00:04.000")}, want{token: pitrarchive.TokenStopAmbiguous,
					msg: []string{"at or before 2026-11-01T10:05:03.000000Z or after 2026-11-01T10:05:05.000000Z"}})
			} else {
				w.check("late", query{stop: at("00:04.000")}, want{token: pitrarchive.TokenStopAmbiguous, msg: []string{"{A}:100 (committed"}})
			}
		}
	})
	t.Run("marked site", func(t *testing.T) {
		w := newWorld(t)
		w.commit("A:1-100")
		era1Files(w, true)
		w.file("a", "a2", 101, "A:1-70", "", "A:71-100")
		w.mark("b", pitrarchive.ViolationCommitOrder)
		w.backup("A:1-41,A:43", "b", 102, w.at("A:44"))
		w.check("no start in unmarked rows", query{stop: w.in("A:80")}, want{token: pitrarchive.TokenArchiveUnsafeHistory, msg: []string{"site b", "CommitOrder"}})
	})
	t.Run("G6 inconsistent cross-server order", func(t *testing.T) {
		w := newWorld(t)
		w.commit("A:1-10")
		w.commit("Cu:1")
		w.commit("A:11-13")
		w.commit("Cu:2")
		w.promote("f", "A", "", epoch)
		w.promote("g", "Cu", "", epoch)
		w.file("f", "f1", 101, "A:1-10", "", "A:11-13")
		w.file("g", "g1", 103, "A:1-10", "", "Cu:1", "A:11")
		w.file("g", "g2", 103, "A:1-11,Cu:1", "", "A:12-13", "Cu:2")
		w.backup("A:1-10", "f", 101, w.at("A:10").Add(ms500))
		w.check("G6", query{stop: w.in("Cu:2")}, want{outcome: pitrplan.OutcomeTargetCovered, witness: "Cu:2", truth: pitrplan.GroundTruthOrigin,
			lambda: "A:1-13,Cu:1", segs: []seg{{"g/g1", "Cu:1,A:11", ""}, {"g/g2", "A:12-13", ""}}})
	})
	t.Run("G7 skew across files", func(t *testing.T) {
		w := newWorld(t)
		w.commit("A:1-10")
		base := w.at("A:10")
		w.commitAt("A:11", base.Add(1*time.Second))
		w.commitAt("A:12", base.Add(5*time.Second))
		w.commitAt("Bu:1", base.Add(3*time.Second))
		w.file("a", "F", 101, "A:1-10", "", "A:11-12")
		w.file("b", "G", 102, "A:1-12", "", "Bu:1")
		w.backup("A:1-10", "a", 101, base.Add(ms500))
		w.check("G7", query{stop: base.Add(4 * time.Second)}, want{token: pitrarchive.TokenStopAmbiguous})
	})
	t.Run("O3 frontier on a file boundary", func(t *testing.T) {
		w := newWorld(t)
		w.commit("A:1-70")
		w.file("a", "a0", 101, "", "", "A:1-40")
		w.file("a", "a1", 101, "A:1-40", "", "A:41-70")
		w.backup("A:1-40", "a", 101, w.at("A:41"))
		w.check("O3", query{stop: w.in("A:36")}, want{token: pitrarchive.TokenBackupAfterTarget})
	})
	t.Run("O13 exclude the first post-target transaction", func(t *testing.T) {
		w := newWorld(t)
		w.commit("A:1-70")
		era1Files(w, false)
		beta1(w)
		w.check("O13", query{stop: w.in("A:50"), exclude: "A:50"}, want{outcome: pitrplan.OutcomeTargetCovered, witness: "A:51", truth: pitrplan.GroundTruthOrigin,
			lambda: "A:1-50", segs: []seg{{"a/a1", "A:41-49", ""}}})
	})
	t.Run("G8 entirely pre-target files", func(t *testing.T) {
		w := newWorld(t)
		w.commit("A:1-80")
		era1Files(w, false)
		beta1(w)
		w.check("G8", query{stop: w.in("A:75")}, want{token: pitrarchive.TokenTargetNotCovered})
		w.check("G8 partial", query{stop: w.in("A:75"), partial: true}, want{outcome: pitrplan.OutcomePartial, lambda: "A:1-70",
			segs: []seg{{"a/a1", "A:41-70", ""}}})
	})
}
