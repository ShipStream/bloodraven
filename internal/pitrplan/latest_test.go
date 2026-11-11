package pitrplan

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"testing"
	"time"

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

// The loss report is the whole set, whichever files the plan downloads. A
// file that holds only excluded transactions is not needed for the replay,
// so the report must not depend on a downloaded copy.
//
// a archives A:1-95, then A:96-98 in a file of its own, then A:99-100; b
// is promoted at 200 s with A:1-95. With A:96-100 excluded the middle file
// replays nothing.
func TestLatestLostListsTransactionsOfFilesNotDownloaded(t *testing.T) {
	s := newSynth(t)
	s.promote("a", "A", 101, "", -100)
	s.file("a", "a1", 101, "", "", run("A", 1, 95, 1, 101))
	s.file("a", "a2", 101, "A:1-95", "", run("A", 96, 98, 96, 101))
	s.file("a", "a3", 101, "A:1-98", "", run("A", 99, 100, 99, 101))
	s.promote("b", "B", 102, "A:1-95", 200)
	s.backup("A:1-10", "a", 101, 10.5)
	p, err := s.run(ModeLatest, 0, "A:96-100", false)
	if err != nil || p.Outcome != OutcomeArchiveHead {
		t.Fatalf("plan %+v %v", p, err)
	}
	if !p.Lost.Equal(MustParseSet("A:96-100")) {
		t.Fatalf("lost %s, want A:96-100", p.Lost)
	}
	// The manifest row's commit times date a2 (every transaction of it was
	// committed before the record − δ): it is not downloaded for the report.
	if s.fetches["a/a2"] != 0 || p.Downloaded != 2 {
		t.Fatalf("fetches %v, downloaded %d: a2 is dated by its hints", s.fetches, p.Downloaded)
	}
	if c := p.Coverage(CoverageArchive{}, CoverageDump{}, ""); c.Lost != "A:96-100" {
		t.Fatalf("coverage.json lost %q", c.Lost)
	}
	// A row whose commit times all lie after the record − δ holds nothing
	// the record lost, and is not read either: b's record at 97 s proves
	// A:96 lost at most (bound 92 s: nothing).
	s.sites["b"].Promotions[0].ObservedAt = synthEpoch.Add(97 * time.Second)
	p, err = s.run(ModeLatest, 0, "A:96-100", false)
	if err != nil || !p.Lost.IsEmpty() || s.fetches["a/a2"] != 0 {
		t.Fatalf("record at 97 s: %+v %v, fetches %v", p, err, s.fetches)
	}
}

// lostStraddle builds a file the manifest cannot date: a2 holds A:96-98,
// committed before b's promotion at 200 s, and A:99, committed after it.
func lostStraddle(t *testing.T) (*synth, RowRef) {
	s := newSynth(t)
	s.promote("a", "A", 101, "", -100)
	s.file("a", "a1", 101, "", "", run("A", 1, 95, 1, 101))
	ref := s.file("a", "a2", 101, "A:1-95", "", run("A", 96, 98, 96, 101), run("A", 99, 99, 300, 101))
	s.file("a", "a3", 101, "A:1-99", "", run("A", 100, 101, 301, 101))
	s.promote("b", "B", 102, "A:1-95", 200)
	s.backup("A:1-10", "a", 101, 10.5)
	return s, ref
}

// A file whose commit times lie on both sides of the record is read for
// the report: only its transactions committed before the record − δ are
// lost. A:99 is excluded too, but was committed after b's promotion.
func TestLatestLostReadsAFileItsHintsCannotDate(t *testing.T) {
	s, ref := lostStraddle(t)
	p, err := s.run(ModeLatest, 0, "A:96-99", false)
	if err != nil || !p.Lost.Equal(MustParseSet("A:96-98")) {
		t.Fatalf("plan %+v %v, want lost A:96-98", p, err)
	}
	if s.fetches["a/a2"] != 1 || p.Downloaded != 3 {
		t.Fatalf("fetches %v, downloaded %d", s.fetches, p.Downloaded)
	}
	// The read is for the report only. The file is in no segment and its
	// transactions are not indexed into the plan.
	for _, f := range p.Files {
		if f.Row.Name == "a2" {
			t.Fatalf("a2 is a plan file: %+v", p.Files)
		}
	}
	if p.IndexedTransactions != 95+2 {
		t.Fatalf("indexed %d transactions, want those of a1 and a3", p.IndexedTransactions)
	}
	// A report read never changes the answer: a file that cannot be read,
	// or whose bytes fail their integrity check, is left out of the report
	// and the plan is the same.
	for _, damage := range []string{"lost", "corrupt"} {
		s, _ := lostStraddle(t)
		if damage == "lost" {
			s.lost[ref] = true
		} else {
			s.corrupt[ref] = true
		}
		q, err := s.run(ModeLatest, 0, "A:96-99", false)
		if err != nil || !q.Lost.IsEmpty() || segString(q) != segString(p) || !q.Lineage.Equal(p.Lineage) {
			t.Fatalf("%s a2: plan %+v %v", damage, q, err)
		}
	}
	// Without a record there is nothing to date against, and nothing is
	// read.
	s, _ = lostStraddle(t)
	s.sites["b"].Promotions = nil
	if p, err := s.run(ModeLatest, 0, "A:96-99", false); err != nil || !p.Lost.IsEmpty() || s.fetches["a/a2"] != 0 {
		t.Fatalf("no record: %+v %v, fetches %v", p, err, s.fetches)
	}
	// Nor is a file read whose excluded transactions the record holds.
	s, _ = lostStraddle(t)
	s.sites["b"].Promotions[0].GTIDExecuted = "A:1-98"
	if p, err := s.run(ModeLatest, 0, "A:96-99", false); err != nil || !p.Lost.IsEmpty() || s.fetches["a/a2"] != 1 {
		// A:99 is not in the record, so the file is still a candidate.
		t.Fatalf("record holds A:96-98: %+v %v, fetches %v", p, err, s.fetches)
	}
	s, _ = lostStraddle(t)
	s.sites["b"].Promotions[0].GTIDExecuted = "A:1-99"
	if p, err := s.run(ModeLatest, 0, "A:96-99", false); err != nil || !p.Lost.IsEmpty() || s.fetches["a/a2"] != 0 {
		t.Fatalf("record holds A:96-99: %+v %v, fetches %v", p, err, s.fetches)
	}
	// A record's own site's rows are its later history, here as in the
	// downloaded rows: a record of a itself flags nothing in a's file.
	s, _ = lostStraddle(t)
	s.sites["b"].Promotions = nil
	s.promote("a", "A", 101, "A:1-95", 200)
	if p, err := s.run(ModeLatest, 0, "A:96-99", false); err != nil || !p.Lost.IsEmpty() || s.fetches["a/a2"] != 0 {
		t.Fatalf("the record is a's own: %+v %v, fetches %v", p, err, s.fetches)
	}
}

// A copy on a marked site is loss evidence like any other, read or not (a
// marked site's file that a plan happens to download counts too). c alone
// archived C:1, which b's record lacks; with C:1 excluded the plan stands
// and reports it, with c marked as without, and c's row is dated from its
// manifest in both.
func TestLatestLostCountsUnreadRowsOfAMarkedSite(t *testing.T) {
	s := newSynth(t)
	s.promote("a", "A", 101, "", -100)
	s.file("a", "a1", 101, "", "", run("A", 1, 95, 1, 101))
	s.file("c", "c1", 103, "A:1-40", "", run("C", 1, 1, 50, 103))
	s.file("a", "a2", 101, "A:1-95,C:1", "", run("A", 96, 97, 300, 101))
	s.promote("b", "B", 102, "A:1-95", 200)
	s.backup("A:1-10", "a", 101, 10.5)
	p, err := s.run(ModeLatest, 0, "C:1", false)
	if err != nil || !p.Lost.Equal(MustParseSet("C:1")) {
		t.Fatalf("c unmarked: %+v %v, want lost C:1", p, err)
	}
	s.site("c").Marked = &pitrarchive.HistoryIntegrity{Violation: pitrarchive.ViolationBinlogFormat, Detail: "STATEMENT"}
	p, err = s.run(ModeLatest, 0, "C:1", false)
	if err != nil || !p.Lost.Equal(MustParseSet("C:1")) || s.fetches["c/c1"] != 0 {
		t.Fatalf("c marked: %+v %v, fetches %v", p, err, s.fetches)
	}
}

// A marked site's bytes cannot certify history, so its file is no loss
// evidence either. The backup holds A:9, which makes the B-frontier pass
// download a's file a2 although a is marked.
func TestLatestOwnWritesOfAMarkedSiteAreNotEvidence(t *testing.T) {
	s := unrecorded(t, 40, 42)
	s.backup("A:1-9", "a", 101, 40.5)
	_, err := s.run(ModeLatest, 0, "", false)
	wantToken(t, err, pitrarchive.TokenArchiveDivergentHistory, "includes B:3,", "a committed A:11")
	s.site("a").Marked = &pitrarchive.HistoryIntegrity{Violation: pitrarchive.ViolationBinlogFormat, Detail: "STATEMENT"}
	// The run ends on the marker (b alone archived no copy of A:6-8), not
	// on a loss read from the marked site's file.
	p, err := s.run(ModeLatest, 0, "", false)
	wantToken(t, err, pitrarchive.TokenArchiveUnsafeHistory, "site a's history-integrity marker is set")
	if s.fetches["a/a2"] != 1 || !p.Lost.IsEmpty() || !p.Divergent.IsEmpty() {
		t.Fatalf("a marked: lost %s divergent %s, fetches %v", p.Lost, p.Divergent, s.fetches)
	}
}

// A report read that fails for a reason that says nothing about the row (a
// store error after the fetcher's retries) ends planning as any other
// download does: it is not reported as "nothing lost".
func TestLatestLostReportReadTransientError(t *testing.T) {
	s, ref := lostStraddle(t)
	var a Archive
	for _, name := range s.order {
		a.Sites = append(a.Sites, *s.sites[name])
	}
	fetch := FetchFunc(func(_ context.Context, r RowRef) (*FileIndex, error) {
		if r == ref {
			return nil, &TransientError{Err: errors.New("503")}
		}
		return s.idx[r], nil
	})
	_, err := Run(context.Background(), Request{Mode: ModeLatest, Exclude: MustParseSet("A:96-99"), Dump: s.dump, Budget: NewBudget(64 << 20)}, a, fetch)
	if !IsTransient(err) {
		t.Fatalf("err = %v, want the transient error", err)
	}
}

// A report read keeps the index it paid for: when the planner needs the
// row later, it is not downloaded twice.
func TestLatestLostReportReadIsReused(t *testing.T) {
	s, ref := lostStraddle(t)
	var a Archive
	for _, name := range s.order {
		a.Sites = append(a.Sites, *s.sites[name])
	}
	fetches := 0
	fetch := FetchFunc(func(_ context.Context, r RowRef) (*FileIndex, error) {
		if r == ref {
			fetches++
		}
		return s.idx[r], nil
	})
	p, err := newPlanner(context.Background(), Request{Mode: ModeLatest, Exclude: MustParseSet("A:96-99"), Dump: s.dump, Budget: NewBudget(64 << 20)}, a, fetch)
	if err != nil {
		t.Fatal(err)
	}
	if err := p.run(); err != nil {
		t.Fatal(err)
	}
	var row *Row
	for _, r := range p.rows {
		if r.ref() == ref {
			row = r
		}
	}
	if _, err := p.fetch(row); err != nil || fetches != 1 || p.plan.Downloaded != 3 || p.files[row] == nil {
		t.Fatalf("fetch after a report read: %v, %d fetches of a2, %d downloads", err, fetches, p.plan.Downloaded)
	}
}

// Known limitation, as before the report was completed: loss evidence
// about a transaction the backup holds is seen only when its file is
// downloaded for another reason, so the answer depends on where the files
// were cut. a commits A:96-100 before b is promoted without them, keeps
// writing (a split brain), and the backup is taken on a afterwards. A
// backup cannot unapply a transaction, so excluding A:96-100 is refused
// (PITRExcludeIntersectsBackup) and the refusal's advice cannot be
// followed; reading every row of B to make the two layouts agree would add
// downloads to every latest restore and a refusal with no remedy but an
// older backup. The decision is the maintainer's; this test fails when the
// two layouts start to agree.
func TestKnownLimitationLossEvidenceInsideTheBackup(t *testing.T) {
	build := func(cut bool) *synth {
		s := newSynth(t)
		s.promote("a", "A", 101, "", -100)
		if cut {
			// The lost transactions are in a file older than the newest
			// file of B: the B-frontier pass stops before it.
			s.file("a", "a1", 101, "", "", run("A", 1, 100, 1, 101))
			s.file("a", "a2", 101, "A:1-100", "", run("A", 101, 110, 301, 101))
		} else {
			s.file("a", "a1", 101, "", "", run("A", 1, 100, 1, 101), run("A", 101, 110, 301, 101))
		}
		s.file("a", "a3", 101, "A:1-110", "", run("A", 111, 120, 400, 101))
		s.promote("b", "B", 102, "A:1-95", 200)
		s.backup("A:1-110", "a", 101, 311.5)
		return s
	}
	_, err := build(false).run(ModeLatest, 0, "", false)
	wantToken(t, err, pitrarchive.TokenArchiveDivergentHistory, "includes A:96-100,", "b was promoted")
	s := build(true)
	p, err := s.run(ModeLatest, 0, "", false)
	if err != nil || p.Outcome != OutcomeArchiveHead || !p.Lost.IsEmpty() || s.fetches["a/a1"] != 0 {
		t.Fatalf("the layout with the lost transactions in an unread file of B: %+v %v, fetches %v", p, err, s.fetches)
	}
}

// unrecorded is a writable episode with no promotion record: a writes
// A:1-8; b is promoted and commits B:1-3 (20-22 s), of which a receives
// B:1-2; a is promoted again, unobserved, and commits A:9-11 (at own, own+1
// and last); b, which still holds B:3, applies them, is promoted again and
// commits B:4-6. The archived head holds B:3, which a never had while it
// was writable.
func unrecorded(t *testing.T, own, last float64) *synth {
	s := newSynth(t)
	s.promote("a", "A", 101, "", -100)
	s.file("a", "a1", 101, "", "", run("A", 1, 8, 1, 101))
	s.promote("b", "B", 102, "A:1-8", 15)
	s.file("b", "b1", 102, "A:1-8", "", run("B", 1, 3, 20, 102))
	a3 := append(run("A", 9, 10, own, 101), run("A", 11, 11, last, 101)...)
	s.file("a", "a2", 101, "A:1-8,B:1-2", "", a3)
	s.file("b", "b2", 102, "A:1-8,B:1-3", "", a3)
	s.promote("b", "B", 102, "A:1-11,B:1-3", 55)
	s.file("b", "b3", 102, "A:1-11,B:1-3", "", run("B", 4, 6, 60, 102))
	s.backup("A:1-5", "a", 101, 5.5)
	return s
}

// A binlog is loss evidence without a record: a's file a2 holds
// transactions a's own server committed after its Previous_gtids, which
// lack B:3. A promotion record of that episode would say the same.
func TestLatestLossEvidenceFromOwnWrites(t *testing.T) {
	p, err := unrecorded(t, 40, 42).run(ModeLatest, 0, "", false)
	wantToken(t, err, pitrarchive.TokenArchiveDivergentHistory, "includes B:3,", "before a committed A:11 at", "in a/a2, whose server had not executed them", "left no promotion record")
	if !p.Divergent.Equal(MustParseSet("B:3")) || !p.Lost.Equal(MustParseSet("B:3")) {
		t.Fatalf("refusal coverage: divergent %s lost %s", p.Divergent, p.Lost)
	}
	// The remedy is the record's: exclude exactly the lost transaction.
	p, err = unrecorded(t, 40, 42).run(ModeLatest, 0, "B:3", false)
	if err != nil || p.Outcome != OutcomeArchiveHead || !p.Lost.Equal(MustParseSet("B:3")) || !p.Replay.Equal(MustParseSet("A:6-11,B:1-2:4-6")) {
		t.Fatalf("B:3 excluded: %+v %v", p, err)
	}
	// With the record the answer is the same, and the record is named.
	s := unrecorded(t, 40, 42)
	s.promote("a", "A", 101, "A:1-8,B:1-2", 35)
	_, err = s.run(ModeLatest, 0, "", false)
	wantToken(t, err, pitrarchive.TokenArchiveDivergentHistory, "includes B:3,", "a was promoted")

	// δ: B:3 was committed at 22 s. An own transaction proves it lost only
	// when it was committed more than δ later, and the newest own
	// transaction of the file is the one that counts.
	if p, err := unrecorded(t, 23, 26).run(ModeLatest, 0, "", false); err != nil || !p.Lost.IsEmpty() {
		t.Fatalf("A:11 committed 4 s after B:3: %+v %v", p, err)
	}
	_, err = unrecorded(t, 23, 28).run(ModeLatest, 0, "", false)
	wantToken(t, err, pitrarchive.TokenArchiveDivergentHistory, "includes B:3,", "a committed A:11")

	// Excluding one of the episode's transactions does not set the file
	// aside; its other own transactions still prove the loss.
	_, err = unrecorded(t, 40, 42).run(ModeLatest, 0, "A:11", false)
	wantToken(t, err, pitrarchive.TokenArchiveDivergentHistory, "includes B:3,", "a committed A:10")
}

// Only a transaction the file's own server originated shows that server
// writable. A replica's file that lacks older transactions (c replicates
// from a alone and never had B:1-3) is no evidence that they were lost.
func TestLatestReplicaCopiesAreNotLossEvidence(t *testing.T) {
	s := newSynth(t)
	s.promote("a", "A", 101, "", -100)
	s.file("a", "a1", 101, "", "", run("A", 1, 8, 1, 101))
	s.promote("b", "B", 102, "A:1-8", 15)
	s.file("b", "b1", 102, "A:1-8", "", run("B", 1, 3, 20, 102))
	s.promote("a", "A", 101, "A:1-8,B:1-3", 35)
	a3 := run("A", 9, 11, 40, 101)
	s.file("a", "a2", 101, "A:1-8,B:1-3", "", a3)
	s.file("c", "c1", 103, "A:1-8", "", a3)
	s.backup("A:1-5", "a", 101, 5.5)
	p, err := s.run(ModeLatest, 0, "", false)
	if err != nil || !p.Lineage.Equal(MustParseSet("A:1-11,B:1-3")) || !p.Lost.IsEmpty() {
		t.Fatalf("plan %+v %v", p, err)
	}
	// The same file written by the server that originated A:9-11 is.
	s.sites["c"].Rows = nil
	s.file("c", "c1", 101, "A:1-8", "", a3)
	_, err = s.run(ModeLatest, 0, "", false)
	wantToken(t, err, pitrarchive.TokenArchiveDivergentHistory, "includes B:1-3,", "c committed A:11")
}

// The report of own-write evidence does not depend on the downloads
// either: B:3 sits alone in a file the replay does not need once it is
// excluded.
func TestLatestLostFromOwnWritesInAFileNotDownloaded(t *testing.T) {
	s := newSynth(t)
	s.promote("a", "A", 101, "", -100)
	s.file("a", "a1", 101, "", "", run("A", 1, 8, 1, 101))
	s.promote("b", "B", 102, "A:1-8", 15)
	s.file("b", "b1", 102, "A:1-8", "", run("B", 1, 2, 20, 102))
	s.file("b", "b1b", 102, "A:1-8,B:1-2", "", run("B", 3, 3, 22, 102))
	a3 := run("A", 9, 11, 40, 101)
	s.file("a", "a2", 101, "A:1-8,B:1-2", "", a3)
	s.file("b", "b2", 102, "A:1-8,B:1-3", "", a3)
	s.promote("b", "B", 102, "A:1-11,B:1-3", 55)
	s.file("b", "b3", 102, "A:1-11,B:1-3", "", run("B", 4, 6, 60, 102))
	s.backup("A:1-5", "a", 101, 5.5)
	p, err := s.run(ModeLatest, 0, "B:3", false)
	if err != nil || !p.Lost.Equal(MustParseSet("B:3")) || s.fetches["b/b1b"] != 0 {
		t.Fatalf("plan %+v %v, fetches %v", p, err, s.fetches)
	}
}

// Known limitation: what is left of the difference between a recorded and
// an unrecorded episode. A file is evidence through its replayed own
// transactions, because only then is it certain to be downloaded. With
// every transaction of the unobserved episode excluded, its file is not
// read and nothing flags B:3; a promotion record is never set aside (N1)
// and still does. The plan without the record restores b's history without
// a's episode, which is a state b held.
func TestKnownLimitationUnrecordedEpisodeWhollyExcluded(t *testing.T) {
	p, err := unrecorded(t, 40, 42).run(ModeLatest, 0, "A:9-11", false)
	if err != nil || p.Outcome != OutcomeArchiveHead || !p.Lost.IsEmpty() || !p.Replay.Equal(MustParseSet("A:6-8,B:1-6")) {
		t.Fatalf("unrecorded, episode excluded: %+v %v", p, err)
	}
	s := unrecorded(t, 40, 42)
	s.promote("a", "A", 101, "A:1-8,B:1-2", 35)
	_, err = s.run(ModeLatest, 0, "A:9-11", false)
	wantToken(t, err, pitrarchive.TokenArchiveDivergentHistory, "includes B:3,", "a was promoted")
}
