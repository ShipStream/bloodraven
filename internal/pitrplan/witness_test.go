package pitrplan

import (
	"testing"
	"time"

	"github.com/shipstream/bloodraven/internal/pitrarchive"
)

func TestWitnessAscendingSearch(t *testing.T) {
	s := newSynth(t)
	s.file("a", "a0", 101, "", "", run("A", 1, 30, 1, 101))
	s.file("a", "a1", 101, "A:1-30", "", run("A", 31, 70, 31, 101))
	s.file("a", "a2", 101, "A:1-70", "", run("A", 71, 100, 71, 101))
	s.file("a", "a3", 101, "A:1-100", "", run("A", 101, 130, 101, 101))
	s.backup("A:1-40", "a", 101, 42)
	p, err := s.run(ModeTimestamp, 79.5, "", false)
	if err != nil {
		t.Fatal(err)
	}
	if p.Witness.GTID.String() != "A:80" || s.fetches["a/a3"] != 0 {
		t.Fatalf("witness %v, a3 fetched %d times", p.Witness.GTID, s.fetches["a/a3"])
	}
}

// §4.4: a promotion record that already contains w is not an anchor.
//
// w = A:85 is archived only on b, whose copy has pred = A:1-84, B:e (an
// errant on b). b is promoted briefly and writes nothing; a is re-promoted
// with R2 = A:1-100, B:e. R2 ∖ {A} ⊆ pred_b(A:85), so a lag allowance
// would accept and put B:e into Λ, which a never had at A:85.
func TestWitnessRecordContainingWIsNotAnAnchor(t *testing.T) {
	s := newSynth(t)
	s.promote("a", "A", 101, "", -100)
	s.file("a", "a1", 101, "", "", run("A", 1, 60, 1, 101))
	errant := stx{g: "B:1", oct: sec(60.5), srv: 102}
	s.file("b", "b1", 102, "A:1-60", "", []stx{errant}, run("A", 61, 100, 61, 101))
	s.promote("b", "B", 102, "A:1-100,B:1", 101)
	s.promote("a", "A", 101, "A:1-100,B:1", 102)
	s.backup("A:1-40", "a", 101, 42)
	_, err := s.run(ModeTimestamp, 84.5, "", false)
	wantToken(t, err, pitrarchive.TokenStopAmbiguous, "replica copy on b", "predecessors include B:1")
	// Excluding the errant certifies a's history.
	p, err := s.run(ModeTimestamp, 84.5, "B:1", false)
	if err != nil {
		t.Fatal(err)
	}
	if p.Witness.GroundTruth != GroundTruthAnchored || !p.Lineage.Equal(MustParseSet("A:1-84")) || p.Witness.AnchorOriginRow == "" {
		t.Fatalf("plan %+v Λ=%s", p.Witness, p.Lineage)
	}
}

func TestWitnessAnchors(t *testing.T) {
	era := func(withRecord, withOrigin bool) *synth {
		s := newSynth(t)
		if withRecord {
			s.promote("a", "A", 101, "A:1-50", 50.5)
		} else {
			s.promote("a", "A", 101, "A:1-100", 1000) // a later record that holds w: not an anchor
		}
		if withOrigin {
			s.file("a", "a1", 101, "", "", run("A", 1, 60, 1, 101))
		}
		s.file("b", "b1", 102, "", "", run("A", 1, 100, 1, 101))
		s.backup("", "b", 102, 0)
		return s
	}
	// A record before w anchors it.
	p, err := era(true, false).run(ModeTimestamp, 79.5, "", false)
	if err != nil || p.Witness.GroundTruth != GroundTruthAnchored || p.Witness.AnchorRecords[0] != "a#0" {
		t.Fatalf("record anchor: %+v %v", p.Witness, err)
	}
	// q alone anchors it.
	p, err = era(false, true).run(ModeTimestamp, 79.5, "", false)
	if err != nil || p.Witness.AnchorOriginRow == "" || len(p.Witness.AnchorRecords) != 0 {
		t.Fatalf("origin anchor: %+v %v", p.Witness, err)
	}
	// A replica copy's own-UUID predecessors need no anchor at all.
	if _, err := era(false, false).run(ModeTimestamp, 79.5, "", false); err != nil {
		t.Fatalf("no anchor needed: %v", err)
	}
}

func TestWitnessMarkedOccurrenceRefuses(t *testing.T) {
	s := newSynth(t)
	s.file("a", "a1", 101, "", "", run("A", 1, 60, 1, 101))
	s.file("b", "b1", 102, "", "", run("A", 1, 100, 1, 101))
	s.file("c", "c1", 103, "", "", run("A", 1, 100, 1, 101))
	s.site("b").Marked = &pitrarchive.HistoryIntegrity{Violation: pitrarchive.ViolationReplicationFilter, Detail: "replicate_do_db"}
	s.backup("", "a", 101, 0)
	_, err := s.run(ModeTimestamp, 79.5, "", false)
	wantToken(t, err, pitrarchive.TokenArchiveUnsafeHistory, "site b", "ReplicationFilter")
}

func TestWitnessTies(t *testing.T) {
	// Unrelated ties with unequal pred on two writers refuse; the causal
	// successor of a same-µs pair loses (G2-5, see examples_test).
	s := newSynth(t)
	s.file("a", "a1", 101, "", "", run("A", 1, 10, 1, 101))
	s.file("c", "c1", 103, "A:1-8", "", []stx{{g: "C:1", oct: sec(10), srv: 103}})
	s.backup("", "a", 101, 0)
	_, err := s.run(ModeTimestamp, 10, "", false)
	wantToken(t, err, pitrarchive.TokenStopAmbiguous, "same µs", "choose a target ≠")
}

// G1, N3 revised: a record anchors w only when no later record of its
// site fences it off. a records R_a = A:1-20,C:1 at 20 s; c (a replica
// with C:1 of its own) archives a's A:51-60 as replica copies. R_a must not
// explain C:1 in c's copy of A:55 when a's server may have started a later
// episode or incarnation (a reclone without C:1, F062; a reseed, F011)
// before A:55.
func TestWitnessAnchorFence(t *testing.T) {
	reset := pitrarchive.RecordReset
	tests := []struct {
		name  string
		add   func(s *synth)
		fence bool
	}{
		{"no later record", func(*synth) {}, false},
		// F062: a recloned from a donor without C:1; its sidecar recorded
		// the reset, the new era's promotion record never persisted.
		{"reset record before w", func(s *synth) { s.record("a", reset, "A", 101, "A:1-50", 40, 5*time.Second) }, true},
		// The reset is observed after w, but the change it reports may
		// lie up to its probe interval (and δ) before it.
		{"reset record within its probe interval + δ after w", func(s *synth) { s.record("a", reset, "A", 101, "A:1-50", 64.9, 5*time.Second) }, true},
		{"reset record beyond its probe interval + δ after w", func(s *synth) { s.record("a", reset, "A", 101, "A:1-50", 65.1, 5*time.Second) }, false},
		// A sidecar start that found a's history replaced reaches back to
		// a's latest record.
		{"start-up reset reaching back to R_a", func(s *synth) { s.record("a", reset, "A", 101, "A:1-50", 300, 280*time.Second) }, true},
		{"record with an unknown probe interval", func(s *synth) { s.record("a", reset, "A", 101, "A:1-50", 10000, 0) }, true},
		// F011: the reseeded incarnation's own promotion record (lacking
		// C:1), observed within δ of A:55, so it anchors nothing itself.
		{"new incarnation's promotion record", func(s *synth) { s.promote("a", "A", 101, "A:1-50", 52) }, true},
		// A later episode's record that still holds R_a: w may belong to
		// that episode, whose start the archive does not date.
		{"later episode's record holding R_a", func(s *synth) { s.promote("a", "A", 101, "A:1-50,C:1", 52) }, true},
		{"another kind of record", func(s *synth) { s.record("a", "Future", "A", 101, "A:1-50,C:1", 45, time.Second) }, true},
		// F009: clone bridges and rows of earlier states are no fences,
		// whatever their first event times; records date incarnations.
		{"clone bridge row lacking C:1", func(s *synth) {
			s.file("a", "a0", 101, "", "A:1-50")
			s.sites["a"].Rows[1].Created = synthEpoch.Add(45 * time.Second)
		}, false},
		{"clone bridge row with an unknown open time", func(s *synth) {
			s.file("a", "a0", 101, "", "A:1-50")
			s.sites["a"].Rows[1].Created = time.Time{}
		}, false},
		{"earlier-state row opened between R_a and w", func(s *synth) {
			s.file("a", "a0", 101, "A:1-10", "")
			s.sites["a"].Rows[1].Created = synthEpoch.Add(40 * time.Second)
		}, false},
		{"clone bridge row and its reset record", func(s *synth) {
			s.file("a", "a0", 101, "", "A:1-50")
			s.record("a", reset, "A", 101, "A:1-50", 45, 5*time.Second)
		}, true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := incarnationArchive(t)
			tt.add(s)
			if !tt.fence {
				wantAnchoredByRa(t, tt.name, s)
				return
			}
			wantRaNoAnchor(t, tt.name, s)
			p, err := s.run(ModeTimestamp, 54.5, "C:1", false)
			if err != nil || !p.Lineage.Equal(MustParseSet("A:1-54")) {
				t.Fatalf("C:1 excluded: %+v %v", p, err)
			}
		})
	}
	// The refusal names the fence.
	s := incarnationArchive(t)
	s.record("a", reset, "A", 101, "A:1-50", 40, 5*time.Second)
	_, err := s.run(ModeTimestamp, 54.5, "", false)
	wantToken(t, err, pitrarchive.TokenStopAmbiguous, "the promotion records a#0 are not anchors, because a later record of site a, a#1 (an incarnation-reset record observed at 2026-11-01T12:00:40.000000Z, reporting a change that happened up to 5s before it), is not provably after A:55")
}

// A sidecar restart in the middle of a's era writes a second promotion
// record of the same incarnation. Witnesses well before it keep R_a#0,
// witnesses more than δ after it use R_a#1, and the window between
// (where the restart record may mark a later episode) refuses with a
// safe target.
func TestWitnessAnchorSidecarRestartMidEra(t *testing.T) {
	s := newSynth(t)
	s.promote("a", "A", 101, "A:1-50,D:1", 50.5)
	s.promote("a", "A", 101, "A:1-70,D:1", 70.5) // the restart's record
	s.file("b", "b1", 102, "A:1-50,D:1", "", run("A", 51, 100, 51, 101))
	s.backup("A:1-50,D:1", "b", 102, 50.8)
	p, err := s.run(ModeTimestamp, 55.7, "", false)
	if err != nil || p.Witness.GTID.String() != "A:56" || len(p.Witness.AnchorRecords) != 1 || p.Witness.AnchorRecords[0] != "a#0" {
		t.Fatalf("before the restart: %+v %v", p, err)
	}
	_, err = s.run(ModeTimestamp, 61.7, "", false)
	wantToken(t, err, pitrarchive.TokenStopAmbiguous, "predecessors include D:1",
		"or choose a target after 2026-11-01T12:01:15.500000Z (a's promotion record at 2026-11-01T12:01:10.500000Z plus the 5s clock-skew margin)",
		"the promotion records a#0 are not anchors, because a later record of site a, a#1 (a promotion record")
	p, err = s.run(ModeTimestamp, 76.7, "", false)
	if err != nil || p.Witness.GTID.String() != "A:77" || len(p.Witness.AnchorRecords) != 1 || p.Witness.AnchorRecords[0] != "a#1" {
		t.Fatalf("after the restart: %+v %v", p, err)
	}
}

// G1: a record observed less than δ before w (or after it) does not
// provably precede w, so it is no anchor; the refusal names the first
// target it would anchor.
func TestWitnessAnchorClockSkewMargin(t *testing.T) {
	build := func(at float64) *synth {
		s := newSynth(t)
		// D:1 came to a from another source before its promotion; only
		// the record proves a had it.
		s.promote("a", "A", 101, "A:1-50,D:1", at)
		s.file("b", "b1", 102, "A:1-50,D:1", "", run("A", 51, 100, 51, 101))
		s.backup("A:1-50,D:1", "b", 102, 50.8)
		return s
	}
	// A:56 commits 5.5 s after the record: anchored.
	p, err := build(50.5).run(ModeTimestamp, 55.7, "", false)
	if err != nil || p.Witness.GTID.String() != "A:56" || p.Witness.AnchorRecords[0] != "a#0" {
		t.Fatalf("anchored: %+v %v", p, err)
	}
	// A:54 commits 3.5 s after it: within δ.
	_, err = build(50.5).run(ModeTimestamp, 53.7, "", false)
	wantToken(t, err, pitrarchive.TokenStopAmbiguous, "predecessors include D:1",
		"or choose a target after 2026-11-01T12:00:55.500000Z (a's promotion record at 2026-11-01T12:00:50.500000Z, which is not provably before A:54, plus the 5s clock-skew margin)")
}

// F010: every occurrence row of the witness enters the closure. w = A:3
// has an origin copy in a1 (which the plan uses) and a replica copy in b1,
// whose A:1 is a member of B; c1 holds a conflicting copy of A:1. Only
// b1's overlap reaches c1, so b1 must be in U.
func TestWitnessOccurrencesEnterClosure(t *testing.T) {
	s := newSynth(t)
	s.file("a", "a1", 101, "A:1,C:1", "", run("A", 2, 3, 10, 101))
	s.file("b", "b1", 102, "C:1", "", run("A", 1, 1, 1, 101), run("A", 2, 3, 10, 101))
	s.file("c", "c1", 103, "", "", []stx{{g: "A:1", oct: sec(1), srv: 109}})
	s.file("d", "d1", 104, "", "", []stx{{g: "C:1", oct: sec(5), srv: 104}})
	s.backup("A:1,C:1", "a", 101, 6)
	_, err := s.run(ModeTimestamp, 10.5, "", false)
	wantToken(t, err, pitrarchive.TokenArchiveNamespaceReuse, "A:1 differs between", "origin server 101 vs 109")
}

// incarnationArchive is TestWitnessAnchorIncarnation's archive without
// evidence: R_a = A:1-20,C:1 at 20 s anchors c's replica copy of A:55.
func incarnationArchive(t *testing.T) *synth {
	s := newSynth(t)
	s.file("a", "a1", 101, "", "", run("A", 1, 50, 1, 101))
	s.promote("a", "A", 101, "A:1-20,C:1", 20)
	s.file("c", "c1", 103, "A:1-50", "", []stx{{g: "C:1", oct: sec(0.5), srv: 103}}, run("A", 51, 60, 51, 101))
	s.backup("A:1-40", "a", 101, 42)
	return s
}

func wantAnchoredByRa(t *testing.T, name string, s *synth) {
	t.Helper()
	p, err := s.run(ModeTimestamp, 54.5, "", false)
	if err != nil || p.Witness.GroundTruth != GroundTruthAnchored || !p.Lineage.Equal(MustParseSet("A:1-54,C:1")) || len(p.Witness.AnchorRecords) != 1 {
		t.Fatalf("%s: R_a anchors: %+v %v", name, p, err)
	}
}

func wantRaNoAnchor(t *testing.T, name string, s *synth) {
	t.Helper()
	_, err := s.run(ModeTimestamp, 54.5, "", false)
	r, ok := RefusalOf(err)
	if !ok || r.Token != pitrarchive.TokenStopAmbiguous {
		t.Fatalf("%s: got %v, want StopAmbiguous", name, err)
	}
	wantToken(t, err, pitrarchive.TokenStopAmbiguous, "predecessors include C:1", "a#0 are not anchors")
}

// F062: another server's promotion between R_a and w means a lost its
// role and regained it with no record of the new era in the archive; a
// reclone may lie in between, so R_a anchors nothing.
func TestWitnessAnchorAcrossRoleChange(t *testing.T) {
	wantAnchoredByRa(t, "no evidence", incarnationArchive(t))
	s := incarnationArchive(t)
	s.promote("b", "B", 102, "A:1-30", 35)
	wantRaNoAnchor(t, "b promoted at 35 s", s)
	// Boundary controls: observed δ or more before R_a, or after w + δ.
	for _, at := range []float64{15, 60} {
		s := incarnationArchive(t)
		s.promote("b", "B", 102, "A:1-30", at)
		wantAnchoredByRa(t, "b promoted outside (R_a, w)", s)
	}
	// A record that holds w was taken after w committed, however close its
	// observedAt: the failover after a's last commits is no role change
	// before them.
	s = incarnationArchive(t)
	s.promote("b", "B", 102, "A:1-55,C:1", 54)
	wantAnchoredByRa(t, "b promoted holding w", s)
}

// F065: a record of another server on a's own site fences R_a off like
// any record of the site; one observed beyond w + its probe interval + δ
// does not. Downloaded bytes of a clone bridge prove nothing either way
// (F009): even with an origin transaction committed before R_a in them,
// the bridge is no fence.
func TestWitnessAnchorEvidenceSources(t *testing.T) {
	s := incarnationArchive(t)
	s.promote("a", "Z", 109, "A:1-30", 40)
	wantRaNoAnchor(t, "another server on site a", s)
	s = incarnationArchive(t)
	s.promote("a", "Z", 109, "A:1-30", 66)
	wantAnchoredByRa(t, "another server on site a after w + PI + δ", s)

	// E:1, a member of B committed after a1's newest member of B, makes
	// the frontier download a0 before the witness is grounded.
	s = incarnationArchive(t)
	s.file("a", "a0", 101, "", "A:1-5", run("A", 6, 6, 6, 101), []stx{{g: "E:1", oct: sec(41), srv: 105}})
	s.sites["a"].Rows[1].Created = time.Time{}
	s.backup("A:1-40,E:1", "a", 101, 42)
	// R_a anchors A:55 (Λ = A:1-54,C:1); the plan then refuses only
	// because this backup's E:1 is off that history.
	p, err := s.run(ModeTimestamp, 54.5, "", false)
	wantToken(t, err, pitrarchive.TokenBackupNotOnLineage, "the backup holds E:1, which is not in the history of A:55")
	if p == nil || p.Witness == nil || p.Witness.GroundTruth != GroundTruthAnchored || !p.Lineage.Equal(MustParseSet("A:1-54,C:1")) {
		t.Fatalf("bridge with early origin bytes: %+v", p)
	}
	if s.fetches["a/a0"] == 0 {
		t.Fatal("a0 was not downloaded; the case needs its bytes")
	}
}

// F064: the origin anchor q is fenced too. a1 (a's own file, which
// carries D:1 from before) is the only anchor for c's replica copy of
// A:55. A record of a observed after q (A:50, at 50 s; δ before it on the
// server's clock) and not provably after A:55 makes q no anchor; a clone
// bridge row does not.
func TestWitnessOriginAnchorIncarnation(t *testing.T) {
	build := func(add func(s *synth)) *synth {
		s := newSynth(t)
		s.promote("a", "A", 101, "", -100)
		s.file("a", "a1", 101, "D:1", "", run("A", 1, 50, 1, 101))
		s.file("c", "c1", 103, "D:1,A:1-50", "", run("A", 51, 60, 51, 101))
		add(s)
		s.backup("D:1,A:1-40", "a", 101, 42)
		return s
	}
	anchored := map[string]func(*synth){
		"no record":         func(*synth) {},
		"bridge from exec":  func(s *synth) { s.file("a", "a9", 101, "", "D:1,A:1-50") },
		"bridge without D1": func(s *synth) { s.file("a", "a9", 101, "", "A:1-50") },
		"reset before q":    func(s *synth) { s.record("a", pitrarchive.RecordReset, "A", 101, "D:1,A:1-40", 44.9, 5*time.Second) },
		"reset after w":     func(s *synth) { s.record("a", pitrarchive.RecordReset, "A", 101, "A:1-60", 65.1, 5*time.Second) },
	}
	for name, add := range anchored {
		p, err := build(add).run(ModeTimestamp, 54.5, "", false)
		if err != nil || p.Witness.GroundTruth != GroundTruthAnchored || p.Witness.AnchorOriginRow != "a/a1/id-a1" || !p.Lineage.Equal(MustParseSet("D:1,A:1-54")) {
			t.Fatalf("%s: q anchors: %+v %v", name, p, err)
		}
	}
	fenced := map[string]func(*synth){
		"reset between q and w":     func(s *synth) { s.record("a", pitrarchive.RecordReset, "A", 101, "A:1-50", 52, 5*time.Second) },
		"reset within δ before q":   func(s *synth) { s.record("a", pitrarchive.RecordReset, "A", 101, "A:1-50", 45.1, 5*time.Second) },
		"promotion record after q":  func(s *synth) { s.promote("a", "A", 101, "D:1,A:1-50", 51) },
		"reset within PI + δ after": func(s *synth) { s.record("a", pitrarchive.RecordReset, "A", 101, "A:1-60", 64.9, 5*time.Second) },
	}
	for name, add := range fenced {
		_, err := build(add).run(ModeTimestamp, 54.5, "", false)
		t.Log(name)
		wantToken(t, err, pitrarchive.TokenStopAmbiguous, "archived only as a replica copy on c", "predecessors include D:1",
			"the origin row a/a1 (through A:50) is not an anchor, because a later record of site a, a#1")
	}
}

// F066: an anchor row from a marked site refuses, though the witness's
// own copy is on an unmarked site.
func TestWitnessMarkedAnchorRowRefuses(t *testing.T) {
	s := newSynth(t)
	s.promote("a", "A", 101, "", -100)
	s.file("a", "a1", 101, "D:1", "", run("A", 1, 50, 1, 101))
	s.file("c", "c1", 103, "D:1,A:1-50", "", run("A", 51, 60, 51, 101))
	s.site("a").Marked = &pitrarchive.HistoryIntegrity{Violation: pitrarchive.ViolationReplicationFilter, Detail: "replicate_do_db"}
	s.backup("D:1,A:1-40", "a", 101, 42)
	_, err := s.run(ModeTimestamp, 54.5, "", false)
	wantToken(t, err, pitrarchive.TokenArchiveUnsafeHistory, "the anchor row a/a1 for the replica-copy witness A:55", "site a's history-integrity marker")
}

// F179: D-39's copy-agreement refusals.
func TestWitnessCopiesDisagree(t *testing.T) {
	// Two origin copies (written by the witness's own server) with
	// different predecessors.
	s := newSynth(t)
	s.file("a", "a1", 101, "", "", run("A", 1, 10, 1, 101))
	s.file("z", "z1", 101, "Q:1", "", run("A", 5, 10, 5, 101))
	s.backup("", "a", 101, 0)
	_, err := s.run(ModeTimestamp, 7.5, "", false)
	wantToken(t, err, pitrarchive.TokenStopAmbiguous, "A:8 has origin copies with different predecessor sets (a/a1 and z/z1)")

	// Two accepted replica copies that disagree modulo X: R_a explains
	// b's D:1, and c's copy lacks it.
	s = newSynth(t)
	s.promote("a", "A", 101, "A:1-50,D:1", 50.5)
	s.file("b", "b1", 102, "A:1-50,D:1", "", run("A", 51, 60, 51, 101))
	s.file("c", "c1", 103, "A:1-50", "", run("A", 51, 60, 51, 101))
	s.backup("A:1-40", "b", 102, 50.8)
	_, err = s.run(ModeTimestamp, 55.7, "", false)
	wantToken(t, err, pitrarchive.TokenStopAmbiguous, "A:56 has replica copies with different predecessor sets (b/b1 and c/c1)")
	// Excluding D:1 makes them agree (the plan then fails later: no
	// archived position equals this backup).
	_, err = s.run(ModeTimestamp, 55.7, "D:1", false)
	if r, ok := RefusalOf(err); !ok || r.Token == pitrarchive.TokenStopAmbiguous {
		t.Fatalf("D:1 excluded: %v", err)
	}
}

// F171: a row whose commit-time hints are unknown is searched for the
// witness wherever T falls.
func TestWitnessUnknownTimeHints(t *testing.T) {
	s := newSynth(t)
	s.file("a", "a1", 101, "", "", run("A", 1, 30, 1, 101))
	s.file("a", "a2", 101, "A:1-30", "", run("A", 31, 60, 31, 101))
	r := &s.sites["a"].Rows[1]
	r.Hints.Min, r.Hints.Max = 0, 0
	s.backup("A:1-20", "a", 101, 20.5)
	p, err := s.run(ModeTimestamp, 40.5, "", false)
	if err != nil || p.Witness.GTID.String() != "A:41" {
		t.Fatalf("witness %+v: %v", p, err)
	}
}
