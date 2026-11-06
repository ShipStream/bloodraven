package pitrplan

import (
	"testing"

	"github.com/shipstream/bloodraven/internal/pitrarchive"
)

// D-51: two downloaded sequences that order common GTIDs oppositely.
func TestOrderInversion(t *testing.T) {
	a11 := stx{g: "A:11", oct: sec(11), srv: 101}
	c1 := stx{g: "C:1", oct: sec(11.5), srv: 103}
	build := func(cFirst bool, b string) *synth {
		s := newSynth(t)
		s.file("a", "a1", 101, "A:1-10", "", []stx{a11, c1}, run("A", 12, 20, 12, 101))
		if cFirst {
			s.file("c", "c1", 103, "A:1-10", "", []stx{c1, a11})
		} else {
			s.file("c", "c1", 103, "A:1-10", "", []stx{a11, c1})
		}
		s.backup(b, "a", 101, 10.5)
		return s
	}
	_, err := build(true, "A:1-10").run(ModeTimestamp, 15.5, "", false)
	wantToken(t, err, pitrarchive.TokenArchiveOrderInversion, "A:11", "C:1", "a/a1", "c/c1")
	// An inversion against a file that holds only baseline occurrences.
	s := build(true, "A:1-11,C:1")
	s.backup("A:1-11,C:1", "a", 101, 11.7)
	_, err = s.run(ModeTimestamp, 15.5, "", false)
	wantToken(t, err, pitrarchive.TokenArchiveOrderInversion)
	// Consistent interleavings pass.
	if _, err := build(false, "A:1-10").run(ModeTimestamp, 15.5, "", false); err != nil {
		t.Fatal(err)
	}
}

func TestOrderInversionThreeCycle(t *testing.T) {
	x := stx{g: "X:1", oct: sec(1), srv: 1}
	y := stx{g: "Y:1", oct: sec(2), srv: 2}
	z := stx{g: "Z:1", oct: sec(3), srv: 3}
	s := newSynth(t)
	s.file("f", "f1", 1, "", "", []stx{x, y})
	s.file("g", "g1", 2, "", "", []stx{y, z})
	s.file("h", "h1", 3, "", "", []stx{z, x})
	s.file("w", "w1", 4, "X:1,Y:1,Z:1", "", []stx{{g: "W:1", oct: sec(10), srv: 4}})
	s.backup("", "w", 4, 0)
	_, err := s.run(ModeTimestamp, 9, "", false)
	wantToken(t, err, pitrarchive.TokenArchiveOrderInversion, "in a cycle", "X:1 before Y:1 (f/f1)", "Y:1 before Z:1 (g/g1)", "Z:1 before X:1 (h/h1)")
}

// P1: the second half of an inversion is visible only through a later
// file's Previous_gtids. a writes A:1-20 (a1, a2); b applied A:1-4, then its
// errant B:1, then A:5-20 (b1). After a planned failover to b (b2 =
// B:2-10), a replicates B:1 and B:2-10 into a3, whose prev A:1-20 proves a
// applied A:5-20 before B:1. b applied B:1 first. No single file holds both
// legs, and a1 (A:5-10 as a's own transactions) is never downloaded.
func TestOrderInversionAcrossRotation(t *testing.T) {
	build := func() *synth {
		s := newSynth(t)
		s.promote("a", "A", 101, "", -100)
		s.file("a", "a1", 101, "", "", run("A", 1, 10, 1, 101))
		s.file("a", "a2", 101, "A:1-10", "", run("A", 11, 20, 11, 101))
		errant := stx{g: "B:1", oct: sec(4.9), srv: 102}
		s.file("b", "b1", 102, "A:1-4", "", []stx{errant}, run("A", 5, 20, 5, 101))
		s.promote("b", "B", 102, "A:1-20,B:1", 21)
		s.file("b", "b2", 102, "A:1-20,B:1", "", run("B", 2, 10, 22, 102))
		s.file("a", "a3", 101, "A:1-20", "", []stx{errant}, run("B", 2, 10, 22, 102))
		s.backup("A:1-15", "a", 101, 15.5)
		return s
	}
	s := build()
	_, err := s.run(ModeTimestamp, 25.5, "", false)
	wantToken(t, err, pitrarchive.TokenArchiveOrderInversion, "in a cycle", "before B:1 (a/a3, by its previous GTIDs)", "B:1 before A:")
	if s.fetches["a/a1"] != 0 {
		t.Errorf("a1 was downloaded; the inversion must be seen without it")
	}
	// Excluding the errant, as the refusal advises, certifies a's order.
	p, err := build().run(ModeTimestamp, 25.5, "B:1", false)
	if err != nil || !p.Lineage.Equal(MustParseSet("A:1-20,B:1-5")) {
		t.Fatalf("B:1 excluded: %+v %v", p, err)
	}
}

// Consistent histories across rotations and failovers produce no false
// inversion: Example-shaped planned failover with both sites' files.
func TestOrderConsistentAcrossRotation(t *testing.T) {
	s := newSynth(t)
	s.promote("a", "A", 101, "", -100)
	s.file("a", "a1", 101, "", "", run("A", 1, 10, 1, 101))
	s.file("a", "a2", 101, "A:1-10", "", run("A", 11, 20, 11, 101))
	s.file("b", "b1", 102, "A:1-4", "", run("A", 5, 20, 5, 101))
	s.promote("b", "B", 102, "A:1-20", 21)
	s.file("b", "b2", 102, "A:1-20", "", run("B", 1, 10, 22, 102))
	s.file("a", "a3", 101, "A:1-20", "", run("B", 1, 10, 22, 102))
	s.backup("A:1-15", "a", 101, 15.5)
	p, err := s.run(ModeTimestamp, 25.5, "", false)
	if err != nil || !p.Lineage.Equal(MustParseSet("A:1-20,B:1-4")) {
		t.Fatalf("plan %+v %v", p, err)
	}
}

// Only downloaded bytes are compared: an inversion in rows no download set
// reaches is not observed (a documented limitation).
func TestOrderInversionUnobserved(t *testing.T) {
	s := newSynth(t)
	s.file("a", "a1", 101, "", "", run("A", 1, 20, 1, 101))
	q1 := stx{g: "Q:1", oct: sec(100), srv: 9}
	q2 := stx{g: "Q:2", oct: sec(101), srv: 9}
	s.file("d", "d1", 9, "A:1-20", "", []stx{q1, q2})
	s.file("e", "e1", 8, "A:1-20", "", []stx{q2, q1})
	s.backup("A:1-5", "a", 101, 5.5)
	if _, err := s.run(ModeTimestamp, 10.5, "", false); err != nil {
		t.Fatal(err)
	}
	if s.fetches["d/d1"]+s.fetches["e/e1"] != 0 {
		t.Fatalf("rows after the witness were downloaded: %v", s.fetches)
	}
}
