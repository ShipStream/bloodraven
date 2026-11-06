package pitrplan

import (
	"testing"

	"github.com/shipstream/bloodraven/internal/pitrarchive"
)

// loss builds: a writes A:1-100 (archived), b is promoted at τ with
// R_b = A:1-95 and writes nothing archived.
func lossArchive(t *testing.T, tau float64) *synth {
	s := newSynth(t)
	s.promote("a", "A", 101, "", -100)
	s.file("a", "a1", 101, "", "", run("A", 1, 100, 1, 101))
	s.promote("b", "B", 102, "A:1-95", tau)
	s.backup("A:1-10", "a", 101, 10.5)
	return s
}

func TestLatestLossEvidence(t *testing.T) {
	// A:96-100 committed at 96..100 s; b observed writable at 200 s without
	// them: lost.
	_, err := lossArchive(t, 200).run(ModeLatest, 0, "", false)
	wantToken(t, err, pitrarchive.TokenArchiveDivergentHistory, "A:96-100", "b was promoted")
	// δ: committed within δ before the record still counts...
	_, err = lossArchive(t, 96).run(ModeLatest, 0, "", false)
	wantToken(t, err, pitrarchive.TokenArchiveDivergentHistory, "A:96-100")
	// ...but a transaction committed δ or more after the record does not.
	_, err = lossArchive(t, 93).run(ModeLatest, 0, "", false) // bound 98 s
	wantToken(t, err, pitrarchive.TokenArchiveDivergentHistory, "includes A:96-97,")
	if _, err := lossArchive(t, 80).run(ModeLatest, 0, "", false); err != nil {
		t.Fatalf("τ=80: A:96 committed 16 s later than the record + δ: %v", err)
	}
	// Excluding the lost set restores what the cluster kept, and records it.
	p, err := lossArchive(t, 200).run(ModeLatest, 0, "A:96-100", false)
	if err != nil || !p.Lost.Equal(MustParseSet("A:96-100")) || p.Outcome != OutcomeArchiveHead {
		t.Fatalf("plan %+v %v", p, err)
	}
	// A record stops being in force once X holds its owner's GTIDs outside
	// it: the user excluded that promotion's branch.
	s := lossArchive(t, 200)
	s.file("b", "b1", 102, "A:1-95", "", run("B", 1, 3, 300, 102))
	p, err = s.run(ModeLatest, 0, "B:1-3", false)
	if err != nil || !p.Lineage.Equal(MustParseSet("A:1-100")) {
		t.Fatalf("owner excluded: %+v %v", p, err)
	}
}

func TestLatestOwnerExclusion(t *testing.T) {
	// A record never proves its own server's transactions lost: a's later
	// writes after its own record are not evidence.
	s := newSynth(t)
	s.promote("a", "A", 101, "A:1-50", 50.5)
	s.file("a", "a1", 101, "", "", run("A", 1, 100, 1, 101))
	s.backup("A:1-10", "a", 101, 10.5)
	if _, err := s.run(ModeLatest, 0, "", false); err != nil {
		t.Fatal(err)
	}
}

func TestLatestAuditNegativeEvidence(t *testing.T) {
	s := newSynth(t)
	s.file("a", "a1", 101, "", "", run("A", 1, 20, 1, 101))
	s.sites["a"].Rows[0].Divergent = MustParseSet("A:15-16")
	s.backup("A:1-10", "a", 101, 10.5)
	_, err := s.run(ModeLatest, 0, "", false)
	wantToken(t, err, pitrarchive.TokenArchiveDivergentHistory, "divergence audit", "A:15-16")
}

func TestLatestHeads(t *testing.T) {
	s := newSynth(t)
	s.file("a", "a1", 101, "", "", run("A", 1, 20, 1, 101))
	s.file("b", "b1", 102, "A:1-10", "", run("B", 1, 5, 30, 102))
	s.backup("A:1-5", "a", 101, 5.5)
	_, err := s.run(ModeLatest, 0, "", false)
	wantToken(t, err, pitrarchive.TokenArchiveSplitBrain, "a/a1 holds A:11-20", "b/b1 holds B:1-5")
	p, err := s.run(ModeLatest, 0, "B:1-5", false)
	if err != nil || p.Head.Site != "a" {
		t.Fatalf("X keeps a: %+v %v", p, err)
	}
	// Nothing beyond the backup.
	s.backup("A:1-20", "a", 101, 20.5)
	_, err = s.run(ModeLatest, 0, "B:1-5", false)
	wantToken(t, err, pitrarchive.TokenNoArchivedReplay)
	// The backup is not on the head's history.
	s.backup("A:1-10,C:1", "a", 101, 10.5)
	_, err = s.run(ModeLatest, 0, "B:1-5", false)
	wantToken(t, err, pitrarchive.TokenBackupNotOnLineage)
}
