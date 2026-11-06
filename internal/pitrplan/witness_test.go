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
	for _, ev := range []string{"later record", "reclone row", "row opened at an unknown time"} {
		_, err := build(ev).run(ModeTimestamp, 54.5, "", false)
		wantToken(t, err, pitrarchive.TokenStopAmbiguous, "replica copy on c", "predecessors include C:1", "a#0 are not anchors")
		p, err := build(ev).run(ModeTimestamp, 54.5, "C:1", false)
		if err != nil || !p.Lineage.Equal(MustParseSet("A:1-54")) {
			t.Fatalf("%s, C:1 excluded: %+v %v", ev, p, err)
		}
	}
	for _, ev := range []string{"later record keeps R", "row opened before R", "row opened after w"} {
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
