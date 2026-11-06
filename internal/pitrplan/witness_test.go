package pitrplan

import (
	"testing"

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
