package pitrplan

import (
	"testing"
	"time"

	"github.com/shipstream/bloodraven/internal/backupmeta"
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
	// a0 was left unread on its hint, so maxOct(B) is only a lower bound
	// (review F005): the D-52 server-clock bound applies instead.
	if p.BackupNewestCommit != sec(40) || p.BackupFrontierProven {
		t.Fatalf("maxOct(B) = %d proven=%v, want t(A:40), unproven", p.BackupNewestCommit, p.BackupFrontierProven)
	}
	if s.fetches["a/a0"] != 0 {
		t.Errorf("a0 (hmax t(A:30) ≤ t(A:40)) was downloaded")
	}
	_, err = s.run(ModeTimestamp, 42.5, "", false)
	wantToken(t, err, pitrarchive.TokenBackupAfterTarget, "proven only up to 2026-11-01T12:00:40.000000Z", "1 archived rows", "use a target at or after 2026-11-01T12:00:43.000000Z")
}

// F005: a hint that understates its row's newest member of B must not
// prove the frontier. a1 holds A:31-40 committed at 31–40 s but hints
// [20, 20]; the search stops before it, so a target between the job's
// dump end and the dumped server's clock + 1 s refuses (D-52) instead of
// certifying an empty replay that keeps A:36-40, committed after T.
func TestFrontierUnverifiedHintIsNotProof(t *testing.T) {
	s := newSynth(t)
	s.file("a", "a0", 101, "", "", run("A", 1, 30, 1, 101))
	s.file("a", "a1", 101, "A:1-30", "", run("A", 31, 40, 31, 101))
	s.file("a", "a2", 101, "A:1-40", "", run("A", 41, 50, 41, 101))
	s.sites["a"].Rows[1].Hints.Min, s.sites["a"].Rows[1].Hints.Max = sec(20), sec(20)
	s.backup("A:1-40", "a", 101, 34)
	s.dump.ServerClock.End = backupmeta.NewMicroTime(time.UnixMicro(sec(41)))
	_, err := s.run(ModeTimestamp, 35, "", false)
	wantToken(t, err, pitrarchive.TokenBackupAfterTarget, "proven only up to 2026-11-01T12:00:30.000000Z", "use a target at or after 2026-11-01T12:00:42.000000Z")
	if s.fetches["a/a1"] != 0 {
		t.Fatalf("a1 was downloaded; the case needs the search to stop on its hint")
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
	// P5: the bound is the dumped server's clock, not the dump Job's. A Job
	// whose clock lags the server by more than δ is not a reissue...
	s = base()
	s.backup("A:1-40", "a", 101, 50)
	s.dump.End = backupmeta.NewMicroTime(time.UnixMicro(sec(20)))
	if _, err := s.run(ModeTimestamp, 60.5, "", false); err != nil {
		t.Fatalf("Job clock 30 s behind the server: %v", err)
	}
	// ...while a server clock before the archived commit (+δ) is.
	s = base()
	s.backup("A:1-40", "a", 101, 50)
	s.dump.ServerClock.End = backupmeta.NewMicroTime(time.UnixMicro(sec(34)))
	_, err = s.run(ModeTimestamp, 80, "", false)
	wantToken(t, err, pitrarchive.TokenArchiveNamespaceReuse, "A:40", "the dumped server's clock read 2026-11-01T12:00:34.000000Z")
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
	if err != nil || p.BackupNewestCommit != sec(3) {
		t.Fatalf("frontier %d: %v", p.BackupNewestCommit, err)
	}
	// r1 (hmax 12:00:00) was left unread on its hint: unproven, and the
	// target passed the server-clock bound (12:00:02).
	if p.BackupFrontierProven || s.fetches["a/r1"] != 0 {
		t.Fatalf("proven=%v, r1 fetched %d times", p.BackupFrontierProven, s.fetches["a/r1"])
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
	// C:1-5 are only in a1's previous GTIDs: their commit times are
	// unknown, so the frontier is unproven (F168).
	if p.BackupNewestCommit != 0 || p.BackupFrontierProven {
		t.Fatalf("maxOct(B) = %d proven=%v, want −∞, unproven", p.BackupNewestCommit, p.BackupFrontierProven)
	}
	// A target between the job's dump end (12:00:00) and the dumped
	// server's clock + 1 s refuses (D-52).
	s.dump.ServerClock.End = backupmeta.NewMicroTime(time.UnixMicro(sec(2)))
	_, err = s.run(ModeTimestamp, 2.5, "", false)
	wantToken(t, err, pitrarchive.TokenBackupAfterTarget, "C:1-5", "use a target at or after 2026-11-01T12:00:03.000000Z")
	if _, err := s.run(ModeTimestamp, 5.5, "", false); err != nil {
		t.Fatalf("after the bound: %v", err)
	}
}

// F051: a lost object holding members of B leaves maxOct(B) a lower bound
// (D-52), with complete hints or none.
func TestFrontierLostObject(t *testing.T) {
	for _, hinted := range []bool{true, false} {
		build := func() *synth {
			s := newSynth(t)
			lost := s.file("a", "a0", 101, "", "", run("A", 1, 30, 1, 101))
			s.file("a", "a1", 101, "A:1-30", "", run("A", 31, 70, 31, 101))
			s.lost[lost] = true
			if !hinted {
				r := &s.sites["a"].Rows[0]
				r.Hints = Hints{Previous: r.Hints.Previous}
			}
			s.backup("A:1-30", "a", 101, 30.5)
			s.dump.ServerClock.End = backupmeta.NewMicroTime(time.UnixMicro(sec(32)))
			return s
		}
		s := build()
		p, err := s.run(ModeTimestamp, 32.5, "", false)
		// a1's previous GTIDs name A:1-30, which no readable row holds.
		wantToken(t, err, pitrarchive.TokenBackupAfterTarget, "no longer holds every transaction of this backup (A:1-30)", "use a target at or after 2026-11-01T12:00:33.000000Z")
		if p.BackupFrontierProven {
			t.Fatalf("hinted=%v: frontier proven with a lost row", hinted)
		}
		p, err = build().run(ModeTimestamp, 40.5, "", false)
		if !hinted {
			// A row with no complete hints may hold the witness too.
			wantToken(t, err, pitrarchive.TokenArchiveObjectInvalid, "cannot read a/a0", "may hold the first transaction at or after")
			continue
		}
		if err != nil || p.BackupFrontierProven || p.BackupNewestCommit != 0 || !p.Replay.Equal(MustParseSet("A:31-40")) {
			t.Fatalf("after the bound: %+v %v", p, err)
		}
	}
}
