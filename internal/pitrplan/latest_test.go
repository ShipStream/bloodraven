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
	// δ (G2): only a transaction committed at least δ before the record
	// is provably lost by it; inside the margin the order is unknown...
	_, err = lossArchive(t, 103).run(ModeLatest, 0, "", false) // bound 98 s
	wantToken(t, err, pitrarchive.TokenArchiveDivergentHistory, "includes A:96-97,")
	for _, tau := range []float64{100, 96, 93, 80} {
		// ...and a transaction committed after the record is never lost
		// by it.
		if _, err := lossArchive(t, tau).run(ModeLatest, 0, "", false); err != nil {
			t.Fatalf("τ=%v: A:96-100 are not provably before the record − δ: %v", tau, err)
		}
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

// G2 / opus A1: a record never proves later transactions lost. b is
// promoted at 100 s (R_b = A:1-50), then a fails back and commits A:51-60
// from 103 s; a mid-era record of a and a sidecar restart on b add records
// that also precede a's commits. Latest mode certifies the healthy history.
func TestLatestRecordsNeverProveLaterCommitsLost(t *testing.T) {
	s := newSynth(t)
	s.promote("a", "A", 101, "", -100)
	s.file("a", "a1", 101, "", "", run("A", 1, 50, 1, 101))
	s.promote("b", "B", 102, "A:1-50", 100)
	s.promote("a", "A", 101, "A:1-50", 102)
	s.file("a", "a2", 101, "A:1-50", "", run("A", 51, 60, 103, 101))
	s.promote("a", "A", 101, "A:1-55", 108) // a mid-era record (failed probe)
	s.promote("b", "B", 102, "A:1-52", 106) // b's sidecar restarted mid-era
	s.backup("A:1-10", "a", 101, 10.5)
	p, err := s.run(ModeLatest, 0, "", false)
	if err != nil || !p.Lineage.Equal(MustParseSet("A:1-60")) || !p.Lost.IsEmpty() {
		t.Fatalf("plan %+v %v", p, err)
	}
	// A transaction of a source the record never followed is not evidence
	// either: R_b knows no C.
	s.file("c", "c1", 103, "A:1-60", "", run("C", 1, 2, 1, 103))
	s.file("a", "a3", 101, "A:1-60,C:1-2", "", run("A", 61, 62, 200, 101))
	if _, err := s.run(ModeLatest, 0, "", false); err != nil {
		t.Fatalf("C:1-2: %v", err)
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
