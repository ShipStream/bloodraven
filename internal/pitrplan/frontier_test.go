package pitrplan

import (
	"testing"

	"github.com/shipstream/bloodraven/internal/pitrarchive"
)

func TestFrontierDescendingSearchStopsEarly(t *testing.T) {
	s := newSynth(t)
	s.file("a", "a0", 101, "", "", run("A", 1, 30, 1, 101))
	s.file("a", "a1", 101, "A:1-30", "", run("A", 31, 70, 31, 101))
	s.file("a", "a2", 101, "A:1-70", "", run("A", 71, 100, 71, 101))
	s.backup("A:1-40", "a", 101, 42)
	p, err := s.run(ModeTimestamp, 80, "", false)
	if err != nil {
		t.Fatal(err)
	}
	if p.BackupNewestCommit != sec(40) || !p.BackupFrontierProven {
		t.Fatalf("maxOct(B) = %d proven=%v", p.BackupNewestCommit, p.BackupFrontierProven)
	}
	if s.fetches["a/a0"] != 0 {
		t.Errorf("a0 (hmax t(A:30) ≤ t(A:40)) was downloaded")
	}
}

func TestFrontierHints(t *testing.T) {
	// An understated hmax only stops the search early: maxOct(B) is lower
	// and D larger, which fails safe.
	s := newSynth(t)
	s.file("a", "a0", 101, "", "", run("A", 1, 30, 1, 101))
	ref := s.file("a", "a1", 101, "A:1-30", "", run("A", 31, 70, 31, 101))
	s.file("a", "a2", 101, "A:1-70", "", run("A", 71, 100, 71, 101))
	s.backup("A:1-40", "a", 101, 42)
	s.sites["a"].Rows[1].Hints.Max = sec(20)
	_, err := s.run(ModeTimestamp, 80, "", false)
	// a1's hints no longer match its bytes once it is downloaded (the
	// witness search reads it for the plan), so the overstatement or
	// understatement surfaces as ObjectInvalid.
	wantToken(t, err, pitrarchive.TokenArchiveObjectInvalid, "hints disagree")
	_ = ref
	// An overstated hint is downloaded and refused.
	s.sites["a"].Rows[1].Hints.Max = sec(500)
	_, err = s.run(ModeTimestamp, 80, "", false)
	wantToken(t, err, pitrarchive.TokenArchiveObjectInvalid)
}

func TestFrontierReuseAndAfterTarget(t *testing.T) {
	base := func() *synth {
		s := newSynth(t)
		s.file("a", "a0", 101, "", "", run("A", 1, 30, 1, 101))
		s.file("a", "a1", 101, "A:1-30", "", run("A", 31, 70, 31, 101))
		return s
	}
	// D-49: an archived member of B committed after the dump finished.
	s := base()
	s.backup("A:1-40", "a", 101, 20)
	_, err := s.run(ModeTimestamp, 80, "", false)
	wantToken(t, err, pitrarchive.TokenArchiveNamespaceReuse, "A:40", "after this dump finished")
	// The reuse check also runs in latest mode.
	_, err = s.run(ModeLatest, 0, "", false)
	wantToken(t, err, pitrarchive.TokenArchiveNamespaceReuse)
	// T before ceil_s(dumpEnd).
	s = base()
	s.backup("A:1-40", "a", 101, 50.2)
	_, err = s.run(ModeTimestamp, 50.5, "", false)
	wantToken(t, err, pitrarchive.TokenBackupAfterTarget, "the dump finished")
	// maxOct(B) ≥ T.
	s = base()
	s.backup("A:1-40", "a", 101, 41)
	_, err = s.run(ModeTimestamp, 39.5, "", false)
	wantToken(t, err, pitrarchive.TokenBackupAfterTarget)
	// The frontier runs before the witness: a backup that is too new never
	// reports TargetNotCovered (O2-11).
	s = base()
	s.backup("A:1-70", "a", 101, 75)
	_, err = s.run(ModeTimestamp, 72, "", false)
	wantToken(t, err, pitrarchive.TokenBackupAfterTarget)
}

// G3-2: with B-frontier bytes missing, the dumped server's clock bounds the
// target (D-52), because the job clock can be behind the server's commits.
func TestFrontierServerClockBound(t *testing.T) {
	// r1 = A:1-10; the next file's prev is A:1-20, so A:11-20 are missing.
	s := newSynth(t)
	s.file("a", "r1", 101, "", "", run("A", 1, 10, -9, 101))
	s.file("a", "r2", 101, "A:1-20", "", run("A", 21, 30, 10, 101))
	s.backup("A:1-20", "a", 101, 1)                               // @.done.json end by the job's clock: 12:00:01
	s.dump.ServerClock.End.Time = s.dump.ServerClock.End.Add(3e9) // the server read 12:00:04
	_, err := s.run(ModeTimestamp, 2, "", false)                  // T = 12:00:02
	wantToken(t, err, pitrarchive.TokenBackupAfterTarget, "no longer holds every transaction", "A:11-20", "use a target at or after 2026-11-01T12:00:05.000000Z")
	// At or after serverClock.end + 1 s the bound passes.
	if _, err := s.run(ModeTimestamp, 15, "", false); err != nil {
		t.Fatal(err)
	}

	// With the bytes present the proven check decides: oct(A:20) = 12:00:03
	// ≥ T refuses, and a target after it passes.
	s = newSynth(t)
	s.file("a", "r1", 101, "", "", run("A", 1, 10, -9, 101))
	s.file("a", "r1b", 101, "A:1-10", "", run("A", 11, 20, -6, 101)) // A:20 at 12:00:03
	s.file("a", "r2", 101, "A:1-20", "", run("A", 21, 30, 10, 101))
	s.backup("A:1-20", "a", 101, 1)
	_, err = s.run(ModeTimestamp, 2, "", false)
	wantToken(t, err, pitrarchive.TokenBackupAfterTarget, "already contains A:20")
	p, err := s.run(ModeTimestamp, 3.5, "", false)
	if err != nil || !p.BackupFrontierProven {
		t.Fatalf("proven frontier: %v %v", p.BackupFrontierProven, err)
	}
}

func TestFrontierNoArchivedMemberOfB(t *testing.T) {
	s := newSynth(t)
	s.file("a", "a1", 101, "C:1-5", "", run("A", 1, 10, 1, 101))
	s.backup("C:1-5", "a", 101, 0)
	p, err := s.run(ModeTimestamp, 5.5, "", false)
	if err != nil {
		t.Fatal(err)
	}
	if p.BackupNewestCommit != 0 {
		t.Fatalf("maxOct(B) = %d, want −∞", p.BackupNewestCommit)
	}
}
