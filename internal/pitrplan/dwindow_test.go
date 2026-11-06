package pitrplan

import (
	"strings"
	"testing"

	"github.com/shipstream/bloodraven/internal/pitrarchive"
)

// F048: the time remedies of a DivergentHistory refusal must clear the
// unreadable contributors too. A lost quarantined row z1 spans 10–70 s,
// the backup's newest commit is 20 s, T = 60 s, and the readable
// divergence starts at 50 s: a target at or before 50 s would still meet
// z1, so only the backup remedy (after z1's hinted hmax) is offered; with
// unknown hinted times, neither is.
func TestDWindowRemediesCoverUnreadableRows(t *testing.T) {
	build := func(timesKnown bool) *synth {
		s := newSynth(t)
		s.file("a", "a1", 101, "", "", run("A", 1, 100, 1, 101))
		s.file("y", "y1", 105, "A:1-49", "", []stx{{g: "Y:1", oct: sec(50), srv: 105}})
		z := s.file("z", "z1", 106, "A:1-9", "", run("Z", 1, 61, 10, 106))
		s.lost[s.quarantine(z, synthEpoch)] = true
		if !timesKnown {
			r := &s.sites["z"].Rows[0]
			r.Hints.Min, r.Hints.Max = 0, 0
		}
		s.backup("A:1-20", "a", 101, 20.5)
		return s
	}
	_, err := build(true).run(ModeTimestamp, 60, "", false)
	wantToken(t, err, pitrarchive.TokenArchiveDivergentHistory, "Y:1", "Z:1-61", "z (quarantined z1, unreadable)", "or use a backup taken after 2026-11-01T12:01:10.000000Z")
	if r, _ := RefusalOf(err); strings.Contains(r.Message, "choose a target at or before") {
		t.Fatalf("offers a target that still meets the unreadable row: %s", r.Message)
	}
	_, err = build(false).run(ModeTimestamp, 60, "", false)
	wantToken(t, err, pitrarchive.TokenArchiveDivergentHistory, "Z:1-61")
	if r, _ := RefusalOf(err); strings.Contains(r.Message, "choose a target") || strings.Contains(r.Message, "backup taken after") {
		t.Fatalf("offers time remedies without the unreadable row's times: %s", r.Message)
	}
	// Excluding the unreadable row's transactions leaves only Y:1, whose
	// remedies are exact.
	_, err = build(true).run(ModeTimestamp, 60, "Z:1-61", false)
	wantToken(t, err, pitrarchive.TokenArchiveDivergentHistory, "or choose a target at or before 2026-11-01T12:00:50.000000Z, or use a backup taken after 2026-11-01T12:00:50.000000Z")
}

// F049: the unreadable-row refusals of the D window.
func TestDWindowUnreadableRows(t *testing.T) {
	// An active row that cannot be read may hold an off-lineage
	// transaction before T.
	s := newSynth(t)
	s.file("a", "a1", 101, "", "", run("A", 1, 30, 1, 101))
	z := s.file("z", "z1", 105, "A:1-12", "", []stx{{g: "Z:1", oct: sec(12.5), srv: 105}})
	s.lost[z] = true
	s.backup("A:1-10", "a", 101, 10.5)
	_, err := s.run(ModeTimestamp, 20.5, "", false)
	wantToken(t, err, pitrarchive.TokenArchiveObjectInvalid, "cannot read z/z1", "committed before the target")

	// A lost quarantined row without complete hints cannot be bounded.
	s = newSynth(t)
	s.file("a", "a1", 101, "", "", run("A", 1, 30, 1, 101))
	z = s.file("z", "z1", 105, "A:1-12", "", []stx{{g: "Z:1", oct: sec(12.5), srv: 105}})
	s.lost[s.quarantine(z, synthEpoch)] = true
	r := &s.sites["z"].Rows[0]
	r.Hints = Hints{Previous: r.Hints.Previous}
	s.backup("", "a", 101, 0)
	_, err = s.run(ModeTimestamp, 20.5, "", false)
	wantToken(t, err, pitrarchive.TokenArchiveNamespaceReuse, "cannot verify z/z1", "no complete hints")
}
