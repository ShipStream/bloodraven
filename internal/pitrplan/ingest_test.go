package pitrplan

import (
	"math"
	"testing"
	"time"

	"github.com/shipstream/bloodraven/internal/pitrarchive"
)

// F059: ingest's guards each refuse on their own. Every case corrupts one
// field of a1's manifest row; a1 is downloaded for the plan.
func TestIngestGuards(t *testing.T) {
	for _, tc := range []struct {
		name    string
		corrupt func(r *Row)
		token   pitrarchive.Token
		msg     string
	}{
		{"base overlaps the row's transactions", func(r *Row) { r.Base = MustParseSet("A:35") }, pitrarchive.TokenArchiveNamespaceReuse, "are in the row's baseGtids and among its transactions"},
		{"previous GTIDs hint", func(r *Row) { r.Hints.Previous = MustParseSet("A:1-29") }, pitrarchive.TokenArchiveObjectInvalid, `previous GTIDs "A:1-29"`},
		{"GTIDs hint", func(r *Row) { r.Hints.GTIDs = MustParseSet("A:31-69") }, pitrarchive.TokenArchiveObjectInvalid, `GTIDs "A:31-69"`},
		{"empty GTIDs hint", func(r *Row) { r.Hints.Empty = MustParseSet("A:40") }, pitrarchive.TokenArchiveObjectInvalid, `empty GTIDs "A:40"`},
		{"commit-time hint", func(r *Row) { r.Hints.Min++ }, pitrarchive.TokenArchiveObjectInvalid, "commit times"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			s := newSynth(t)
			s.file("a", "a0", 101, "", "", run("A", 1, 30, 1, 101))
			s.file("a", "a1", 101, "A:1-30", "", run("A", 31, 70, 31, 101))
			s.backup("A:1-30", "a", 101, 30.5)
			tc.corrupt(&s.sites["a"].Rows[1])
			_, err := s.run(ModeTimestamp, 50.5, "", false)
			wantToken(t, err, tc.token, "a/a1", tc.msg)
		})
	}
}

// F052: a row whose bytes fail their content integrity (digest, framing,
// CRC) is evidence like a lost object: a corrupt quarantined row takes the
// waiver under the same three conditions, and a corrupt active row is
// refused with the lost-object remedy.
func TestCorruptRowIsUnreadable(t *testing.T) {
	build := func(createdAfter time.Duration) *synth {
		s := newSynth(t)
		s.file("a", "a0", 101, "", "", run("A", 1, 30, 1, 101))
		q := s.file("a", "a1", 101, "A:1-30", "", run("A", 31, 70, 31, 101))
		s.file("a", "a2", 101, "A:1-70", "", run("A", 71, 100, 71, 101))
		s.corrupt[s.quarantine(q, synthEpoch.Add(86*time.Second))] = true
		s.backup("A:1-85", "a", 101, 86+createdAfter.Seconds())
		return s
	}
	if _, err := build(7*time.Second).run(ModeTimestamp, 94.5, "", false); err != nil {
		t.Fatalf("waived: %v", err)
	}
	_, err := build(3*time.Second).run(ModeTimestamp, 94.5, "", false)
	wantToken(t, err, pitrarchive.TokenArchiveNamespaceReuse, "cannot verify a/a1", "digest does not match", "quarantined row")

	s := newSynth(t)
	s.file("a", "a1", 101, "", "", run("A", 1, 30, 1, 101))
	ref := s.file("a", "a2", 101, "A:1-30", "", run("A", 31, 60, 31, 101))
	s.corrupt[ref] = true
	s.backup("A:1-20", "a", 101, 20.5)
	_, err = s.run(ModeTimestamp, 40.5, "", false)
	wantToken(t, err, pitrarchive.TokenArchiveObjectInvalid, "cannot read a/a2", "digest does not match", "pitr-quarantine")
}

// F050: D-49 covers every downloaded copy of a member of B, quarantined
// ones included. a1 holds A:6-10 reissued at 20–24 s, after the dump
// finished at 10 s, and is quarantined; the closure downloads it.
func TestReissueInDownloadedQuarantinedRow(t *testing.T) {
	build := func() (*synth, RowRef) {
		s := newSynth(t)
		s.file("a", "a0", 101, "", "", run("A", 1, 5, 1, 101))
		q := s.file("a", "a1", 101, "A:1-5", "", run("A", 6, 10, 20, 101))
		s.file("a", "a2", 101, "A:1-10", "", run("A", 11, 12, 25, 101))
		s.backup("A:1-10", "a", 101, 10)
		return s, q
	}
	s, _ := build()
	_, err := s.run(ModeTimestamp, 25.5, "", false)
	wantToken(t, err, pitrarchive.TokenArchiveNamespaceReuse, "A:10", "after this dump finished")
	s, q := build()
	s.quarantine(q, synthEpoch.Add(30*time.Second))
	_, err = s.run(ModeTimestamp, 25.5, "", false)
	wantToken(t, err, pitrarchive.TokenArchiveNamespaceReuse, "A:6 (site a, a1) committed at 2026-11-01T12:00:20.000000Z, after this dump finished")
}

// F175: a hinted transaction count too large to multiply refuses
// PITRPlanTooLarge before any download, and a charge near the int64 limit
// cannot overflow the budget's comparison.
func TestBudgetOverflow(t *testing.T) {
	s := newSynth(t)
	s.file("a", "a1", 101, "", "", run("A", 1, 30, 1, 101))
	s.sites["a"].Rows[0].Hints.GTIDs = MustParseSet("A:1-100000000000000000")
	s.backup("A:1-10", "a", 101, 10.5)
	_, err := s.run(ModeTimestamp, 20.5, "", false)
	wantToken(t, err, pitrarchive.TokenPlanTooLarge, "estimated index of a/a1")
	if s.fetches["a/a1"] != 0 {
		t.Fatal("a1 was downloaded")
	}
	b := NewBudget(100)
	if err := b.Charge(50, "x"); err != nil {
		t.Fatal(err)
	}
	if _, ok := RefusalOf(b.Charge(math.MaxInt64, "y")); !ok || b.Used() != 50 {
		t.Fatalf("near-max charge: used %d", b.Used())
	}
	if MustParseSet("A:1-9223372036854775806,B:1-9223372036854775806").Count() != math.MaxInt64 {
		t.Fatal("Count does not saturate")
	}
}
