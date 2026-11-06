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

// G1: a record anchors w only when it provably precedes w on s's current
// history. a records R_a = A:1-20,C:1 at 20 s; a is later recloned from a
// donor without C:1 and re-promoted, then writes A:51-60, which only c (a
// replica that has C:1 of its own) archives. R_a must not explain C:1 in
// c's copy of A:55: a's new incarnation never had it.
func TestWitnessAnchorIncarnation(t *testing.T) {
	build := func(evidence string) *synth {
		s := newSynth(t)
		s.file("a", "a1", 101, "", "", run("A", 1, 50, 1, 101))
		s.promote("a", "A", 101, "A:1-20,C:1", 20)
		cOwn := stx{g: "C:1", oct: sec(0.5), srv: 103}
		s.file("c", "c1", 103, "A:1-50", "", []stx{cOwn}, run("A", 51, 60, 51, 101))
		switch evidence {
		case "later record":
			// a's re-promotion record lacks C:1 (observed within δ of A:55,
			// so it cannot anchor A:55 itself).
			s.promote("a", "A", 101, "A:1-50", 52)
		case "later record keeps R":
			s.promote("a", "A", 101, "A:1-50,C:1", 52)
		case "reclone row":
			// The post-clone first file: base A:1-50, opened at 45 s.
			s.file("a", "a0", 101, "", "A:1-50")
			s.sites["a"].Rows[1].Created = synthEpoch.Add(45 * time.Second)
		case "row opened before R":
			s.file("a", "a0", 101, "", "A:1-50")
			s.sites["a"].Rows[1].Created = synthEpoch.Add(5 * time.Second)
		case "row opened after w":
			s.file("a", "a0", 101, "", "A:1-50")
			s.sites["a"].Rows[1].Created = synthEpoch.Add(70 * time.Second)
		case "row opened at an unknown time":
			s.file("a", "a0", 101, "", "A:1-50")
			s.sites["a"].Rows[1].Created = time.Time{}
		}
		s.backup("A:1-40", "a", 101, 42)
		return s
	}
	// N3 (review F009): no recorded time proves when a file was opened,
	// so a clone bridge lacking part of R_a is evidence whatever its
	// firstEventTime says.
	for _, ev := range []string{"later record", "reclone row", "row opened at an unknown time", "row opened before R", "row opened after w"} {
		_, err := build(ev).run(ModeTimestamp, 54.5, "", false)
		wantToken(t, err, pitrarchive.TokenStopAmbiguous, "replica copy on c", "predecessors include C:1", "a#0 are not anchors")
		p, err := build(ev).run(ModeTimestamp, 54.5, "C:1", false)
		if err != nil || !p.Lineage.Equal(MustParseSet("A:1-54")) {
			t.Fatalf("%s, C:1 excluded: %+v %v", ev, p, err)
		}
	}
	for _, ev := range []string{"later record keeps R"} {
		p, err := build(ev).run(ModeTimestamp, 54.5, "", false)
		if err != nil || p.Witness.GroundTruth != GroundTruthAnchored || !p.Lineage.Equal(MustParseSet("A:1-54,C:1")) ||
			len(p.Witness.AnchorRecords) != 1 || p.Witness.AnchorRecords[0] != "a#0" {
			t.Fatalf("%s: R_a anchors: %+v %v", ev, p, err)
		}
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

// F065: G1's other evidence sources, with boundary controls.
func TestWitnessAnchorEvidenceSources(t *testing.T) {
	// Another server's record on a's own site, before w (+δ).
	s := incarnationArchive(t)
	s.promote("a", "Z", 109, "A:1-30", 40)
	wantRaNoAnchor(t, "another server on site a", s)
	s = incarnationArchive(t)
	s.promote("a", "Z", 109, "A:1-30", 61)
	wantAnchoredByRa(t, "another server on site a after w + δ", s)

	// An ordinary row starting from an earlier state of R_a, whose first
	// event time lies between R_a + δ and w + δ: the history went back.
	opened := func(at float64) *synth {
		s := incarnationArchive(t)
		s.file("a", "a0", 101, "A:1-10", "")
		s.sites["a"].Rows[1].Created = synthEpoch.Add(time.Duration(at * float64(time.Second)))
		return s
	}
	wantRaNoAnchor(t, "earlier-state row opened at 40 s", opened(40))
	wantAnchoredByRa(t, "earlier-state row opened at R_a + δ", opened(25))

	// A clone bridge with no first event time whose downloaded bytes hold
	// an origin transaction committed before R_a: commit times prove
	// nothing about when a file was opened (N3), so it is evidence. E:1,
	// a member of B committed after a1's newest member of B, makes the
	// frontier download a0 before the witness is grounded.
	s = incarnationArchive(t)
	s.file("a", "a0", 101, "", "A:1-5", run("A", 6, 6, 6, 101), []stx{{g: "E:1", oct: sec(41), srv: 105}})
	s.sites["a"].Rows[1].Created = time.Time{}
	s.backup("A:1-40,E:1", "a", 101, 42)
	wantRaNoAnchor(t, "bridge with early origin bytes", s)
	if s.fetches["a/a0"] == 0 {
		t.Fatal("a0 was not downloaded; the case needs its bytes")
	}
}

// F064: the origin anchor q is guarded too. a1 (a's own file, which
// carries D:1 from before) is the only anchor for c's replica copy of
// A:55; a clone bridge on a lacking part of exec(q) makes q no anchor,
// while a bridge that starts from all of exec(q) does not.
func TestWitnessOriginAnchorIncarnation(t *testing.T) {
	build := func(bridgeBase string) *synth {
		s := newSynth(t)
		s.promote("a", "A", 101, "", -100)
		s.file("a", "a1", 101, "D:1", "", run("A", 1, 50, 1, 101))
		s.file("c", "c1", 103, "D:1,A:1-50", "", run("A", 51, 60, 51, 101))
		if bridgeBase != "" {
			s.file("a", "a9", 101, "", bridgeBase)
		}
		s.backup("D:1,A:1-40", "a", 101, 42)
		return s
	}
	for _, base := range []string{"", "D:1,A:1-50"} {
		p, err := build(base).run(ModeTimestamp, 54.5, "", false)
		if err != nil || p.Witness.GroundTruth != GroundTruthAnchored || p.Witness.AnchorOriginRow != "a/a1/id-a1" || !p.Lineage.Equal(MustParseSet("D:1,A:1-54")) {
			t.Fatalf("bridge %q: q anchors: %+v %v", base, p, err)
		}
	}
	_, err := build("A:1-50").run(ModeTimestamp, 54.5, "", false)
	wantToken(t, err, pitrarchive.TokenStopAmbiguous, "archived only as a replica copy on c", "predecessors include D:1")
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
