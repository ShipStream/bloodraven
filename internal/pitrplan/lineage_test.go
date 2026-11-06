package pitrplan

import (
	"testing"
	"time"

	"github.com/shipstream/bloodraven/internal/pitrarchive"
)

// F008: L2 reads quarantined rows as evidence. a1 holds A:1-10, an empty
// B:1 written by a (server 101), then A:11-20; the backup, taken on b
// (server 102), holds B:1 with whatever effects b gave it. Quarantining a1
// must not hide the injected empty copy; a lost quarantined a1 refuses
// instead of taking the identity waiver.
func TestLineageL2QuarantinedEvidence(t *testing.T) {
	build := func() (*synth, RowRef) {
		s := newSynth(t)
		txns := append(run("A", 1, 10, 1, 101), stx{g: "B:1", oct: sec(10.5), srv: 101, empty: true})
		ref := s.file("a", "a1", 101, "", "", txns, run("A", 11, 20, 11, 101))
		s.file("a", "a2", 101, "A:1-20,B:1", "", run("A", 21, 40, 21, 101))
		s.backup("A:1-20,B:1", "b", 102, 21)
		return s, ref
	}
	s, _ := build()
	_, err := s.run(ModeTimestamp, 30.5, "", false)
	wantToken(t, err, pitrarchive.TokenBackupNotOnLineage, "injected empty transaction under B:1", "server_id 101")

	s, ref := build()
	s.quarantine(ref, synthEpoch.Add(25*time.Second))
	_, err = s.run(ModeTimestamp, 30.5, "", false)
	wantToken(t, err, pitrarchive.TokenBackupNotOnLineage, "injected empty transaction under B:1")

	// Lost, and the dump stamped long after the quarantine: the identity
	// waiver would apply, but L2 runs first and fails closed.
	s, ref = build()
	s.lost[s.quarantine(ref, synthEpoch.Add(-time.Minute))] = true
	_, err = s.run(ModeTimestamp, 30.5, "", false)
	wantToken(t, err, pitrarchive.TokenArchiveNamespaceReuse, "cannot verify a/a1", "quarantined row", "empty copies of B:1")

	// A lost quarantined row with no complete hints may hold one too.
	s, ref = build()
	s.lost[s.quarantine(ref, synthEpoch.Add(-time.Minute))] = true
	r := &s.sites["a"].Rows[0]
	r.Hints = Hints{Previous: r.Hints.Previous}
	_, err = s.run(ModeTimestamp, 30.5, "", false)
	wantToken(t, err, pitrarchive.TokenArchiveNamespaceReuse, "cannot verify a/a1", "no complete hints")
}

// F171: an unreadable active row that may hold an injected empty copy of a
// member of B refuses ObjectInvalid.
func TestLineageL2UnreadableActiveRow(t *testing.T) {
	s := newSynth(t)
	txns := append(run("A", 1, 10, 1, 101), stx{g: "B:1", oct: sec(10.5), srv: 101, empty: true})
	ref := s.file("a", "a1", 101, "", "", txns, run("A", 11, 20, 11, 101))
	s.file("a", "a2", 101, "A:1-20,B:1", "", run("A", 21, 40, 21, 101))
	s.backup("A:1-20,B:1", "b", 102, 21)
	s.lost[ref] = true
	_, err := s.run(ModeTimestamp, 30.5, "", false)
	wantToken(t, err, pitrarchive.TokenArchiveObjectInvalid, "cannot read a/a1", "may hold an empty copy of a backup transaction", "in an archive copy, copy it again from the source archive", "quarantine the row there with bloodraven pitr-quarantine")
}
