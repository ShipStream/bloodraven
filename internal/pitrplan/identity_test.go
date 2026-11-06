package pitrplan

import (
	"testing"
	"time"

	"github.com/shipstream/bloodraven/internal/pitrarchive"
)

// O2-9: the closure is one step. Rows overlapping the used files are
// compared; a row fetched only for comparison pulls nothing further in.
func TestClosureIsOneStep(t *testing.T) {
	s := newSynth(t)
	s.file("a", "a1", 101, "", "", run("A", 1, 30, 1, 101))
	s.file("a", "a2", 101, "A:1-30", "", run("A", 31, 60, 31, 101))
	// b1 overlaps the used file a2 (A:31-40); c1 overlaps only b1 (A:25-28)
	// and a1, neither of which the plan uses.
	s.file("b", "b1", 102, "A:1-24", "", run("A", 25, 40, 25, 101))
	s.file("c", "c1", 103, "A:1-20", "", run("A", 21, 28, 21, 101))
	s.backup("A:1-50", "a", 101, 50.5)
	p, err := s.run(ModeTimestamp, 55.5, "", false)
	if err != nil {
		t.Fatal(err)
	}
	if segString(p) != "[a/a2: A:51-55]" {
		t.Fatalf("plan %s", segString(p))
	}
	want := map[string]int{"a/a2": 1, "b/b1": 1}
	for row, n := range s.fetches {
		if want[row] != n {
			t.Errorf("%s downloaded %d times, want %d", row, n, want[row])
		}
	}
	if p.Downloaded != 2 {
		t.Errorf("downloaded %d rows, want 2 (a2 for the plan, b1 to compare)", p.Downloaded)
	}
}

func TestFingerprintMismatch(t *testing.T) {
	for name, other := range map[string]stx{
		"commit time":   {g: "A:20", oct: sec(19.5), srv: 101},
		"origin server": {g: "A:20", oct: sec(20), srv: 109},
		"one is empty":  {g: "A:20", oct: sec(20), srv: 101, empty: true},
	} {
		t.Run(name, func(t *testing.T) {
			s := newSynth(t)
			s.file("a", "a1", 101, "", "", run("A", 1, 30, 1, 101))
			s.file("b", "b1", 102, "A:1-19", "", []stx{other}, run("A", 21, 25, 21, 101))
			s.backup("A:1-10", "a", 101, 10.5)
			_, err := s.run(ModeTimestamp, 27.5, "", false)
			wantToken(t, err, pitrarchive.TokenArchiveNamespaceReuse, "A:20 differs between")
		})
	}
}

// O2-4: the waiver needs all three conditions.
func TestQuarantineWaiver(t *testing.T) {
	build := func(b string, createdAfter time.Duration) *synth {
		s := newSynth(t)
		s.file("a", "a0", 101, "", "", run("A", 1, 30, 1, 101))
		q := s.file("a", "a1", 101, "A:1-30", "", run("A", 31, 70, 31, 101))
		s.file("a", "a2", 101, "A:1-70", "", run("A", 71, 100, 71, 101))
		at := synthEpoch.Add(86 * time.Second)
		s.lost[s.quarantine(q, at)] = true
		s.backup(b, "a", 101, 86+createdAfter.Seconds())
		return s
	}
	// All three hold.
	s := build("A:1-85", 7*time.Second)
	if _, err := s.run(ModeTimestamp, 94.5, "", false); err != nil {
		t.Fatalf("waived: %v", err)
	}
	// Stamped too soon after the quarantine (≤ At + δ).
	s = build("A:1-85", 3*time.Second)
	_, err := s.run(ModeTimestamp, 94.5, "", false)
	wantToken(t, err, pitrarchive.TokenArchiveNamespaceReuse, "cannot verify a/a1")
	// The row's GTIDs are not all in B (and some are replayed).
	s = build("A:1-50", 4*time.Second)
	s.dump.Stamp.CreatedAt.Time = s.dump.Stamp.CreatedAt.Add(time.Minute)
	_, err = s.run(ModeTimestamp, 94.5, "", false)
	wantToken(t, err, pitrarchive.TokenArchiveNamespaceReuse, "cannot verify a/a1")
}

func TestDWindow(t *testing.T) {
	// D is scoped to (maxOct(B), T): a dead transaction before the backup's
	// newest transaction is outside it.
	s := newSynth(t)
	s.file("a", "a1", 101, "", "", run("A", 1, 30, 1, 101))
	s.file("z", "z1", 105, "A:1-5", "", []stx{{g: "Z:1", oct: sec(5.5), srv: 105}})
	s.backup("A:1-10", "a", 101, 10.5)
	p, err := s.run(ModeTimestamp, 20.5, "", false)
	if err != nil || !p.Divergent.IsEmpty() {
		t.Fatalf("D = %s, %v", p.Divergent, err)
	}
	// Inside the window it refuses, and excluding it certifies.
	s.sites["z"].Rows = nil
	s.file("z", "z1", 105, "A:1-12", "", []stx{{g: "Z:1", oct: sec(12.5), srv: 105}})
	_, err = s.run(ModeTimestamp, 20.5, "", false)
	wantToken(t, err, pitrarchive.TokenArchiveDivergentHistory, "Z:1")
	p, err = s.run(ModeTimestamp, 20.5, "Z:1", false)
	if err != nil || !p.Divergent.Equal(MustParseSet("Z:1")) {
		t.Fatalf("D = %v, %v", p, err)
	}
	// maxOct(B) = −∞ (B empty): the window starts at the beginning.
	s.backup("", "a", 101, 0)
	_, err = s.run(ModeTimestamp, 20.5, "", false)
	wantToken(t, err, pitrarchive.TokenArchiveDivergentHistory, "Z:1")
	// An unreadable quarantined row counts its hinted GTIDs.
	s = newSynth(t)
	s.file("a", "a1", 101, "", "", run("A", 1, 30, 1, 101))
	q := s.file("z", "z1", 105, "A:1-12", "", []stx{{g: "Z:1", oct: sec(12.5), srv: 105}})
	s.lost[s.quarantine(q, synthEpoch)] = true
	s.backup("A:1-10", "a", 101, 10.5)
	_, err = s.run(ModeTimestamp, 20.5, "", false)
	wantToken(t, err, pitrarchive.TokenArchiveDivergentHistory, "Z:1")
}

func TestLineageChecks(t *testing.T) {
	// L5: an XA transaction in P.
	s := newSynth(t)
	txns := run("A", 1, 30, 1, 101)
	txns[14].xa = true
	s.file("a", "a1", 101, "", "", txns)
	s.backup("A:1-10", "a", 101, 10.5)
	_, err := s.run(ModeTimestamp, 20.5, "", false)
	wantToken(t, err, pitrarchive.TokenReplayUnsupported, "A:15", "XA")
	// Excluding it lets the replay proceed (on-history, reported).
	p, err := s.run(ModeTimestamp, 20.5, "A:15", false)
	if err != nil || !p.OnHistoryExclusions.Equal(MustParseSet("A:15")) {
		t.Fatalf("%+v %v", p, err)
	}
	// L2's third-site false positive fails closed: the backup was taken on
	// c, which replicated b's injected empty copy.
	s = newSynth(t)
	inj := run("A", 1, 20, 1, 101)
	inj = append(inj[:5], append([]stx{{g: "B:1", oct: sec(5.5), srv: 102, empty: true}}, inj[5:]...)...)
	s.file("a", "a1", 101, "", "", inj)
	s.backup("A:1-10,B:1", "c", 103, 10.5)
	_, err = s.run(ModeTimestamp, 15.5, "", false)
	wantToken(t, err, pitrarchive.TokenBackupNotOnLineage, "injected empty", "server_id 102")
}

func TestBudget(t *testing.T) {
	s := newSynth(t)
	s.file("a", "a1", 101, "", "", run("A", 1, 3000, 1, 101))
	s.backup("A:1-10", "a", 101, 10.5)
	s.budget = 64 << 10
	_, err := s.run(ModeTimestamp, 2000.5, "", false)
	wantToken(t, err, pitrarchive.TokenPlanTooLarge, "choose a backup closer to the target")
	b := NewBudget(64 << 20)
	if b.MaxEventBytes() != 8<<20 {
		t.Errorf("MaxEventBytes = %d, want budget/8", b.MaxEventBytes())
	}
	if NewBudget(4<<30).MaxEventBytes() != MaxEventBytesCap {
		t.Errorf("MaxEventBytes is capped at 64 MiB")
	}
}
