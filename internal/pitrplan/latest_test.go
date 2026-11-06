package pitrplan

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"strings"
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
	// N1 (review F006): excluding the record's own branch never sets the
	// record aside. b wrote B:1-3 after its promotion; excluding them
	// still leaves a's A:96-100 flagged, and only excluding exactly those
	// resolves it.
	s := lossArchive(t, 200)
	s.file("b", "b1", 102, "A:1-95", "", run("B", 1, 3, 300, 102))
	p, err = s.run(ModeLatest, 0, "B:1-3", false)
	wantToken(t, err, pitrarchive.TokenArchiveDivergentHistory, "includes A:96-100,", "b was promoted")
	if !p.Divergent.Equal(MustParseSet("A:96-100")) || !p.Lost.Equal(MustParseSet("A:96-100")) {
		t.Fatalf("refusal coverage: divergent %s lost %s", p.Divergent, p.Lost)
	}
	// b's branch is then the head; A:96-100 are not on it.
	p, err = s.run(ModeLatest, 0, "A:96-100", false)
	if err != nil || !p.Lineage.Equal(MustParseSet("A:1-95,B:1-3")) || !p.Lost.IsEmpty() {
		t.Fatalf("lost tail excluded: %+v %v", p, err)
	}
}

// F006: a server whose earlier tail was lost and excluded is later
// promoted again; its new record still proves the other writer's tail
// lost. a loses A:96-100 when b takes over; a is recloned and promoted
// again with A:1-95,B:1-45, so b's B:46-50 are lost. Excluding a's old
// tail must not disable a's later record.
func TestLatestExcludedTailKeepsLaterRecord(t *testing.T) {
	s := newSynth(t)
	s.promote("a", "A", 101, "", -100)
	s.file("a", "a1", 101, "", "", run("A", 1, 100, 1, 101))
	s.promote("b", "B", 102, "A:1-95", 200)
	s.file("b", "b1", 102, "A:1-95", "", run("B", 1, 50, 201, 102))
	s.promote("a", "A", 101, "A:1-95,B:1-45", 300)
	s.backup("A:1-10", "a", 101, 10.5)
	_, err := s.run(ModeLatest, 0, "A:96-100", false)
	wantToken(t, err, pitrarchive.TokenArchiveDivergentHistory, "includes B:46-50,", "a was promoted")
	p, err := s.run(ModeLatest, 0, "A:96-100,B:46-50", false)
	if err != nil || !p.Lineage.Equal(MustParseSet("A:1-95,B:1-50")) || !p.Lost.Equal(MustParseSet("B:46-50")) {
		t.Fatalf("both tails excluded: %+v %v", p, err)
	}
}

// F007 (N2): a record proves transactions lost whatever their source. c
// was promoted with A:1-60 and archived C:1-5; b was promoted much later
// with only A:1-60, so c's whole first era is lost, although R_b names no
// C source.
func TestLatestLostFirstEraOfAWriter(t *testing.T) {
	s := newSynth(t)
	s.promote("a", "A", 101, "", -100)
	s.file("a", "a1", 101, "", "", run("A", 1, 60, 1, 101))
	s.promote("c", "C", 103, "A:1-60", 100)
	s.file("c", "c1", 103, "A:1-60", "", run("C", 1, 5, 101, 103))
	s.promote("b", "B", 102, "A:1-60", 500)
	s.backup("A:1-10", "a", 101, 10.5)
	_, err := s.run(ModeLatest, 0, "", false)
	wantToken(t, err, pitrarchive.TokenArchiveDivergentHistory, "includes C:1-5,", "b was promoted")
	// c's own record never proves its own later writes lost.
	s.sites["b"].Promotions = nil
	if _, err := s.run(ModeLatest, 0, "", false); err != nil {
		t.Fatalf("without R_b: %v", err)
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
	// A transaction of another source committed after the records is not
	// evidence either...
	s.file("c", "c1", 103, "A:1-60", "", run("C", 1, 2, 150, 103))
	s.file("a", "a3", 101, "A:1-60,C:1-2", "", run("A", 61, 62, 200, 101))
	if _, err := s.run(ModeLatest, 0, "", false); err != nil {
		t.Fatalf("C:1-2: %v", err)
	}
	// ...but one committed before them is, though no record names C (N2).
	s.sites["c"].Rows = nil
	s.file("c", "c1", 103, "", "", run("C", 1, 2, 1, 103))
	_, err = s.run(ModeLatest, 0, "", false)
	wantToken(t, err, pitrarchive.TokenArchiveDivergentHistory, "includes C:1-2,")
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

// F057: when only marked sites archived the answer, the refusal is
// PITRArchiveUnsafeHistory, not a remedy that waits for archiving.
func TestLatestOnlyMarkedSites(t *testing.T) {
	s := newSynth(t)
	s.file("a", "a1", 101, "", "", run("A", 1, 30, 1, 101))
	s.site("a").Marked = &pitrarchive.HistoryIntegrity{Violation: pitrarchive.ViolationBinlogFormat, Detail: "STATEMENT"}
	s.backup("A:1-10", "a", 101, 10.5)
	_, err := s.run(ModeLatest, 0, "", false)
	wantToken(t, err, pitrarchive.TokenArchiveUnsafeHistory, "no unmarked site archived a binlog", "site a's history-integrity marker")
	for _, partial := range []bool{false, true} {
		_, err = s.run(ModeTimestamp, 20.5, "", partial)
		wantToken(t, err, pitrarchive.TokenArchiveUnsafeHistory, "no unmarked site archived a transaction at or after", "a/a1 may hold it")
	}
	// A marked site that holds nothing at or after T leaves the ordinary
	// refusal.
	_, err = s.run(ModeTimestamp, 40.5, "", false)
	wantToken(t, err, pitrarchive.TokenTargetNotCovered)
	// An empty archive is NoArchivedReplay.
	s = newSynth(t)
	s.backup("", "a", 101, 0)
	_, err = s.run(ModeLatest, 0, "", false)
	wantToken(t, err, pitrarchive.TokenNoArchivedReplay, "no binlog of an unmarked site")
}

// F171: head candidates without complete hints are downloaded, and an
// unreadable head refuses.
func TestLatestHeadFallbacks(t *testing.T) {
	s := newSynth(t)
	s.file("a", "a1", 101, "", "", run("A", 1, 20, 1, 101))
	ref := s.file("a", "a2", 101, "A:1-20", "", run("A", 21, 30, 21, 101))
	r := &s.sites["a"].Rows[1]
	r.Hints = Hints{Previous: r.Hints.Previous}
	s.backup("A:1-10", "a", 101, 10.5)
	p, err := s.run(ModeLatest, 0, "", false)
	if err != nil || p.Head.Name != "a2" || !p.Lineage.Equal(MustParseSet("A:1-30")) {
		t.Fatalf("unhinted head: %+v %v", p, err)
	}
	s.lost[ref] = true
	_, err = s.run(ModeLatest, 0, "", false)
	wantToken(t, err, pitrarchive.TokenArchiveObjectInvalid, "cannot read a/a2", "may be the archive head")
	r.Hints = Hints{Known: true, Previous: MustParseSet("A:1-20"), GTIDs: MustParseSet("A:21-30"), Empty: Set{}, Min: sec(21), Max: sec(30)}
	_, err = s.run(ModeLatest, 0, "", false)
	wantToken(t, err, pitrarchive.TokenArchiveObjectInvalid, "cannot read a/a2", "an archive head")
}

// F056: the maximal heads are the rows no other row strictly covers, in
// file order: incomparable branches and equal copies are kept.
func TestMaximalHeads(t *testing.T) {
	s := newSynth(t)
	for i := 0; i < 50; i++ {
		prev := ""
		if i > 0 {
			prev = "A:1-" + strconv.Itoa(i)
		}
		s.file("a", fmt.Sprintf("a%03d", i), 101, prev, "", run("A", int64(i+1), int64(i+1), float64(i+1), 101))
	}
	s.file("b", "b1", 102, "A:1-40", "", run("B", 1, 2, 100, 102))
	s.file("c", "c1", 103, "A:1-49", "", run("A", 50, 50, 50, 101))
	p := searchPlanner(t, s, "", "", "")
	heads, err := p.maximalHeads(p.rows)
	if err != nil {
		t.Fatal(err)
	}
	var got []string
	for _, h := range heads {
		got = append(got, h.row.Label())
	}
	if strings.Join(got, " ") != "a/a049 b/b1 c/c1" {
		t.Fatalf("heads %v", got)
	}
}

// F054: a loss-evidence refusal keeps the complete set in the plan (for
// coverage.json and pitr-download's divergent-set line), however long the
// message's rendering is.
func TestLatestLossRefusalKeepsFullSet(t *testing.T) {
	s := newSynth(t)
	s.promote("a", "A", 101, "", -100)
	s.file("a", "a1", 101, "", "", run("A", 1, 1000, 1, 101))
	var odd []string
	for n := 1; n <= 1000; n += 2 {
		odd = append(odd, "A:"+strconv.Itoa(n))
	}
	recorded := MustParseSet(strings.Join(odd, ",")).Union(MustParseSet("A:1-10"))
	s.promote("b", "B", 102, recorded.String(), 2000)
	s.backup("A:1-10", "a", 101, 10.5)
	p, err := s.run(ModeLatest, 0, "", false)
	wantToken(t, err, pitrarchive.TokenArchiveDivergentHistory, "transactions)")
	want := MustParseSet("A:1-1000").Subtract(recorded)
	if !p.Divergent.Equal(want) || !p.Lost.Equal(want) || len(p.Divergent.String()) <= 1024 {
		t.Fatalf("divergent %d bytes, lost %d bytes, want %s", len(p.Divergent.String()), len(p.Lost.String()), want.bounded(64))
	}
	c := p.Coverage(CoverageArchive{}, CoverageDump{}, err.Error())
	if c.Divergent != want.String() || c.Lost != want.String() {
		t.Fatal("coverage.json lacks the full set")
	}
}

// F053: the loss-evidence pass checks for cancellation (it may scan many
// records over a large download). The fetcher cancels planning while the
// head is downloaded, the last download of the run.
func TestLossEvidenceHonorsCancellation(t *testing.T) {
	s := lossArchive(t, 200)
	var a Archive
	for _, name := range s.order {
		a.Sites = append(a.Sites, *s.sites[name])
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	fetch := FetchFunc(func(_ context.Context, r RowRef) (*FileIndex, error) {
		cancel()
		return s.idx[r], nil
	})
	_, err := Run(ctx, Request{Mode: ModeLatest, Dump: s.dump, Budget: NewBudget(64 << 20)}, a, fetch)
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("err = %v, want context.Canceled", err)
	}
}
