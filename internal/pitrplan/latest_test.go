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
		// The plan names the row its report lacks.
		if len(q.LostUnread) != 1 || q.LostUnread[0].Row.Name != "a2" || q.LostUnread[0].Reason == "" {
			t.Fatalf("%s a2: unread %+v", damage, q.LostUnread)
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

// archive is the synthetic archive as Run takes it.
func (s *synth) archive() Archive {
	var a Archive
	for _, name := range s.order {
		a.Sites = append(a.Sites, *s.sites[name])
	}
	return a
}

// lostStraddleTwo is lostStraddle with two files the manifest cannot date:
// a2 holds A:96 (before b's promotion at 200 s) and A:97 (after it), a3
// holds A:98 (before) and A:99 (after).
func lostStraddleTwo(t *testing.T) (s *synth, a2, a3 RowRef) {
	s = newSynth(t)
	s.promote("a", "A", 101, "", -100)
	s.file("a", "a1", 101, "", "", run("A", 1, 95, 1, 101))
	a2 = s.file("a", "a2", 101, "A:1-95", "", run("A", 96, 96, 96, 101), run("A", 97, 97, 300, 101))
	a3 = s.file("a", "a3", 101, "A:1-97", "", run("A", 98, 98, 98, 101), run("A", 99, 99, 301, 101))
	s.file("a", "a4", 101, "A:1-99", "", run("A", 100, 101, 302, 101))
	s.promote("b", "B", 102, "A:1-95", 200)
	s.backup("A:1-10", "a", 101, 10.5)
	return s, a2, a3
}

// A report read never ends a plan. A store error that outlasts the
// fetcher's retries, and a row that retention retired since the manifests
// were read (the fetcher reports both as transient), leave the row out of
// the report and name it; the plan is the one planned without the report.
// After a store error no other row is read for the report: each read would
// wait out the fetcher's retries again. A row whose object is damaged does
// not stop the reads of the others.
func TestLatestLostReportReadErrorsNeverEndAPlan(t *testing.T) {
	s, _, _ := lostStraddleTwo(t)
	want, err := s.run(ModeLatest, 0, "A:96-99", false)
	if err != nil || !want.Lost.Equal(MustParseSet("A:96,A:98")) || len(want.LostUnread) != 0 || want.Downloaded != 4 {
		t.Fatalf("both rows readable: %+v %v", want, err)
	}
	for _, tc := range []struct {
		name          string
		fail          error
		lost          string
		unread        []string
		a3Read, total int
	}{
		{"a store error after the retries", &TransientError{Err: errors.New("503")}, "", []string{"a/a2: transient archive read error: 503", "a/a3: not read after the store error of the row before"}, 0, 3},
		{"a row retired meanwhile", &TransientError{Err: errors.New("a/a2 is gone and its site's live manifest no longer lists it")}, "", []string{"a/a2: transient archive read error: a/a2 is gone and its site's live manifest no longer lists it", "a/a3: not read after the store error of the row before"}, 0, 3},
		{"a missing object", errSynthLost, "A:98", []string{"a/a2: object missing"}, 1, 4},
		{"an index that does not fit the budget", refuse(pitrarchive.TokenPlanTooLarge, "planning needs more than 1 bytes"), "A:98", []string{"a/a2: PITRPlanTooLarge: planning needs more than 1 bytes"}, 1, 4},
	} {
		t.Run(tc.name, func(t *testing.T) {
			s, a2, a3 := lostStraddleTwo(t)
			fetches := map[RowRef]int{}
			fetch := FetchFunc(func(_ context.Context, r RowRef) (*FileIndex, error) {
				fetches[r]++
				if r == a2 {
					return nil, tc.fail
				}
				return s.idx[r], nil
			})
			p, err := Run(context.Background(), Request{Mode: ModeLatest, Exclude: MustParseSet("A:96-99"), Dump: s.dump, Budget: NewBudget(64 << 20)}, s.archive(), fetch)
			if err != nil {
				t.Fatalf("a report read ended the plan: %v", err)
			}
			var unread []string
			for _, u := range p.LostUnread {
				unread = append(unread, u.Row.Site+"/"+u.Row.Name+": "+u.Reason)
			}
			if !p.Lost.Equal(MustParseSet(tc.lost)) || strings.Join(unread, "|") != strings.Join(tc.unread, "|") || fetches[a2] != 1 || fetches[a3] != tc.a3Read || p.Downloaded != tc.total {
				t.Fatalf("lost %s, unread %q, fetches %v, downloaded %d", p.Lost, unread, fetches, p.Downloaded)
			}
			if p.Outcome != want.Outcome || !p.Lineage.Equal(want.Lineage) || !p.Replay.Equal(want.Replay) || segString(p) != segString(want) || p.IndexedTransactions != want.IndexedTransactions {
				t.Fatalf("the plan differs from the one planned with the row readable: %+v", p)
			}
		})
	}
	// Cancellation is not about the row: it ends planning, as everywhere.
	s, a2, _ := lostStraddleTwo(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	fetch := FetchFunc(func(_ context.Context, r RowRef) (*FileIndex, error) {
		if r == a2 {
			cancel()
			return nil, &TransientError{Err: context.Canceled}
		}
		return s.idx[r], nil
	})
	if _, err := Run(ctx, Request{Mode: ModeLatest, Exclude: MustParseSet("A:96-99"), Dump: s.dump, Budget: NewBudget(64 << 20)}, s.archive(), fetch); !errors.Is(err, context.Canceled) {
		t.Fatalf("err = %v, want context.Canceled", err)
	}
}

// chargingFetch indexes a row the way pitr-download's fetcher does: through
// the planner's budget, inside a checkpoint of its own that it keeps.
func chargingFetch(t *testing.T, s *synth, b *Budget, fetched map[RowRef]int) Fetcher {
	return FetchFunc(func(_ context.Context, r RowRef) (*FileIndex, error) {
		fetched[r]++
		src := s.idx[r]
		if src == nil {
			return nil, errSynthLost
		}
		attempt := b.Checkpoint()
		ib := NewIndexBuilder(r.Site+"/"+r.Name, b)
		ib.SetWriter(src.WriterServerID)
		err := ib.SetPrevious(src.Previous.Clone())
		for i := 0; err == nil && i < len(src.Txns); i++ {
			if err = ib.BeginTxn(src.GTID(i), src.Txns[i].OCT, src.Txns[i].Srv, src.Txns[i].Pos); err == nil {
				ib.MarkData()
			}
		}
		var idx *FileIndex
		if err == nil {
			idx, err = ib.Finish(src.Digest, src.Size)
		}
		if err != nil {
			attempt.Undo()
			return nil, err
		}
		attempt.Keep()
		return idx, nil
	})
}

// The index of a row read for the report is charged to the budget only
// while its commit times are read: afterwards the budget, and the spill
// arena, hold what they held before. A report read therefore cannot make a
// later allocation of the plan fail.
func TestLatestLostReportReadReturnsItsBudget(t *testing.T) {
	for _, spill := range []bool{false, true} {
		t.Run(fmt.Sprintf("spill=%v", spill), func(t *testing.T) {
			s, ref := lostStraddle(t)
			b := NewBudget(64 << 20)
			arena := newMemArena()
			if spill {
				b.SetSpill(tinySpill(arena))
			}
			fetched := map[RowRef]int{}
			p, err := newPlanner(context.Background(), Request{Mode: ModeLatest, Exclude: MustParseSet("A:96-99"), Dump: s.dump, Budget: b}, s.archive(), chargingFetch(t, s, b, fetched))
			if err != nil {
				t.Fatal(err)
			}
			var row *Row
			for _, r := range p.rows {
				if r.ref() == ref {
					row = r
				}
			}
			used, spilled, blocks := b.Used(), b.Spilled(), len(arena.blocks)
			var during, duringSpilled int64
			seen := 0
			if _, err := p.reportRead(row, func(idx *FileIndex) {
				during, duringSpilled, seen = b.Used(), b.Spilled(), len(idx.Txns)
			}); err != nil {
				t.Fatal(err)
			}
			if seen != 4 || during <= used || (spill && duringSpilled <= spilled) {
				t.Fatalf("the read charged nothing: %d transactions, memory %d (before %d), spilled %d (before %d)", seen, during, used, duringSpilled, spilled)
			}
			if b.Used() != used || b.Spilled() != spilled || len(arena.blocks) != blocks || b.attempt != nil {
				t.Fatalf("after the read: memory %d (want %d), spilled %d (want %d), %d arena blocks (want %d)", b.Used(), used, b.Spilled(), spilled, len(arena.blocks), blocks)
			}
			// The whole plan: the report read leaves the budget where the
			// plan without it does.
			fetched = map[RowRef]int{}
			b = NewBudget(64 << 20)
			plan, err := Run(context.Background(), Request{Mode: ModeLatest, Exclude: MustParseSet("A:96-99"), Dump: s.dump, Budget: b}, s.archive(), chargingFetch(t, s, b, fetched))
			if err != nil || !plan.Lost.Equal(MustParseSet("A:96-98")) || fetched[ref] != 1 {
				t.Fatalf("plan %+v %v, fetches %v", plan, err, fetched)
			}
			s.sites["b"].Promotions[0].ObservedAt = synthEpoch.Add(50 * time.Second) // nothing to date: no read
			fetched = map[RowRef]int{}
			b2 := NewBudget(64 << 20)
			plan, err = Run(context.Background(), Request{Mode: ModeLatest, Exclude: MustParseSet("A:96-99"), Dump: s.dump, Budget: b2}, s.archive(), chargingFetch(t, s, b2, fetched))
			if err != nil || !plan.Lost.IsEmpty() || fetched[ref] != 0 {
				t.Fatalf("no report read: %+v %v, fetches %v", plan, err, fetched)
			}
			if b.Used() != b2.Used() {
				t.Fatalf("the plan with a report read ends with %d bytes charged, the one without with %d", b.Used(), b2.Used())
			}
		})
	}
}

// A report read whose index does not fit what is left of the budget leaves
// the row out of the report. The plan, which fits, stands: a2 holds 20,000
// excluded transactions and the budget has room for a tenth of their index.
func TestLatestLostReportReadOverBudgetKeepsThePlan(t *testing.T) {
	s := newSynth(t)
	s.promote("a", "A", 101, "", -100)
	s.file("a", "a1", 101, "", "", run("A", 1, 95, 1, 101))
	ref := s.file("a", "a2", 101, "A:1-95", "", run("A", 96, 20095, 96, 101), run("A", 20096, 20096, 70000, 101))
	s.file("a", "a3", 101, "A:1-20096", "", run("A", 20097, 20098, 70001, 101))
	s.promote("b", "B", 102, "A:1-95", 60000)
	s.backup("A:1-10", "a", 101, 10.5)
	limit := 2000 * TxnBytes
	plan := func() (*Plan, map[RowRef]int, error) {
		fetched := map[RowRef]int{}
		b := NewBudget(limit)
		p, err := Run(context.Background(), Request{Mode: ModeLatest, Exclude: MustParseSet("A:96-20096"), Dump: s.dump, Budget: b}, s.archive(), chargingFetch(t, s, b, fetched))
		return p, fetched, err
	}
	p, fetched, err := plan()
	if err != nil || p.Outcome != OutcomeArchiveHead || fetched[ref] != 1 {
		t.Fatalf("plan %+v %v, fetches %v", p, err, fetched)
	}
	if !p.Lost.IsEmpty() || len(p.LostUnread) != 1 || p.LostUnread[0].Row != ref || !strings.Contains(p.LostUnread[0].Reason, string(pitrarchive.TokenPlanTooLarge)) {
		t.Fatalf("lost %s, unread %+v", p.Lost, p.LostUnread)
	}
	// With room for the index the row is read and its transactions reported.
	limit = 64 << 20
	if p, _, err := plan(); err != nil || !p.Lost.Equal(MustParseSet("A:96-20095")) || len(p.LostUnread) != 0 {
		t.Fatalf("with room: lost %s unread %+v %v", p.Lost.bounded(64), p.LostUnread, err)
	}
}

// A row is read for the report only when no record dates it from its
// manifest commit times. b's record (200 s) cannot date a2, whose
// transactions were committed at 96-98 s and at 300 s. c's record (400 s)
// lacks them too and dates them all: a2 is not read, whichever record
// comes first.
func TestLatestLostReportReadsNoRowAnotherRecordDates(t *testing.T) {
	for _, first := range []string{"b", "c"} {
		s := newSynth(t)
		s.promote("a", "A", 101, "", -100)
		s.file("a", "a1", 101, "", "", run("A", 1, 95, 1, 101))
		s.file("a", "a2", 101, "A:1-95", "", run("A", 96, 98, 96, 101), run("A", 99, 99, 300, 101))
		s.file("a", "a3", 101, "A:1-99", "", run("A", 100, 101, 500, 101))
		if first == "b" {
			s.promote("b", "B", 102, "A:1-95", 200)
			s.promote("c", "C", 103, "A:1-95", 400)
		} else {
			s.promote("c", "C", 103, "A:1-95", 400)
			s.promote("b", "B", 102, "A:1-95", 200)
		}
		s.backup("A:1-10", "a", 101, 10.5)
		p, err := s.run(ModeLatest, 0, "A:96-99", false)
		if err != nil || !p.Lost.Equal(MustParseSet("A:96-99")) || s.fetches["a/a2"] != 0 || p.Downloaded != 2 {
			t.Fatalf("%s first: %+v %v, fetches %v", first, p, err, s.fetches)
		}
	}
}

// A copy in a quarantined row dates its transaction like any other copy: a
// quarantined row is never data, and the report is not data. The identity
// check downloads a quarantined row that meets the history, and the report
// reads it from there.
func TestLatestLostCountsQuarantinedRows(t *testing.T) {
	s := newSynth(t)
	s.promote("a", "A", 101, "", -100)
	s.file("a", "a1", 101, "", "", run("A", 1, 95, 1, 101))
	a2 := s.file("a", "a2", 101, "A:1-95", "", run("A", 96, 98, 96, 101))
	s.file("a", "a3", 101, "A:1-98", "", run("A", 99, 100, 99, 101))
	s.promote("b", "B", 102, "A:1-95", 200)
	s.backup("A:1-10", "a", 101, 10.5)
	s.quarantine(a2, synthEpoch.Add(150*time.Second))
	p, err := s.run(ModeLatest, 0, "A:96-100", false)
	if err != nil || !p.Lost.Equal(MustParseSet("A:96-100")) || s.fetches["a/a2"] != 1 {
		t.Fatalf("plan %+v %v, fetches %v", p, err, s.fetches)
	}
}

// The report is completed when the plan stands, so a row it needs that the
// plan's own checks download is downloaded once. c archived copies of
// A:96-99, which the manifest cannot date; the identity check reads c1
// because a2, a plan file, holds the same transactions.
func TestLatestLostReportReadsNoRowThePlanDownloads(t *testing.T) {
	s := newSynth(t)
	s.promote("a", "A", 101, "", -100)
	s.file("a", "a1", 101, "", "", run("A", 1, 95, 1, 101))
	lostRun := append(run("A", 96, 98, 96, 101), run("A", 99, 99, 300, 101)...)
	s.file("a", "a2", 101, "A:1-95", "", lostRun, run("A", 100, 101, 301, 101))
	s.file("c", "c1", 103, "A:1-95", "", lostRun)
	s.promote("b", "B", 102, "A:1-95", 200)
	s.backup("A:1-10", "a", 101, 10.5)
	p, err := s.run(ModeLatest, 0, "A:96-99", false)
	if err != nil || !p.Lost.Equal(MustParseSet("A:96-98")) {
		t.Fatalf("plan %+v %v", p, err)
	}
	if s.fetches["c/c1"] != 1 || p.Downloaded != 3 || p.IndexedTransactions != 95+6+4 {
		t.Fatalf("fetches %v, downloaded %d, indexed %d", s.fetches, p.Downloaded, p.IndexedTransactions)
	}
}

// On a refusal the report is completed too (coverage.json's lost lists
// every flagged transaction): A:96-98, excluded, sit in a file the refused
// plan did not download, and A:99-100 are what the refusal asks for.
func TestLatestLostIsCompleteOnARefusal(t *testing.T) {
	s := newSynth(t)
	s.promote("a", "A", 101, "", -100)
	s.file("a", "a1", 101, "", "", run("A", 1, 95, 1, 101))
	s.file("a", "a2", 101, "A:1-95", "", run("A", 96, 98, 96, 101))
	s.file("a", "a3", 101, "A:1-98", "", run("A", 99, 100, 99, 101))
	s.promote("b", "B", 102, "A:1-95", 200)
	s.backup("A:1-10", "a", 101, 10.5)
	p, err := s.run(ModeLatest, 0, "A:96-98", false)
	wantToken(t, err, pitrarchive.TokenArchiveDivergentHistory, "includes A:99-100,", "b was promoted")
	if !p.Lost.Equal(MustParseSet("A:96-100")) || !p.Divergent.Equal(MustParseSet("A:99-100")) || s.fetches["a/a2"] != 0 {
		t.Fatalf("refusal coverage: divergent %s lost %s, fetches %v", p.Divergent, p.Lost, s.fetches)
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

// planLine is a plan, or a refusal, in one line.
func planLine(p *Plan, err error) string {
	if err != nil {
		tok := err.Error()
		if r, ok := RefusalOf(err); ok {
			tok = string(r.Token)
		}
		return fmt.Sprintf("refused %s divergent=%s lost=%s", tok, p.Divergent, p.Lost)
	}
	return fmt.Sprintf("%s lineage=%s lost=%s %s", p.Outcome, p.Lineage, p.Lost, segString(p))
}

// planCase is one query of a fixture and the answer the planner gave before
// latest mode read loss evidence from a site's own binlog (commit 56f7b2fb).
type planCase struct {
	name    string
	mode    Mode
	stop    float64
	exclude string
	partial bool
	want    string
}

func checkPlanCases(t *testing.T, build func() *synth, cases []planCase) {
	t.Helper()
	for _, c := range cases {
		p, err := build().run(c.mode, c.stop, c.exclude, c.partial)
		if got := planLine(p, err); got != c.want {
			t.Errorf("%s:\n got %s\nwant %s (%v)", c.name, got, c.want, err)
		}
	}
}

// errantThenPromoted is a supported history: replica b originates B:1 (an
// errant transaction: FLUSH PRIVILEGES and its like consume a GTID under
// super_read_only, and any write by an account that may write does), goes
// on applying a's transactions and is promoted later with everything a
// committed. Nothing is lost. lag: b has applied only A:1-5 when it writes
// B:1 at 40 s, its binlog rotates, and it applies A:6-10 (committed on a at
// 20-24 s) in its next file.
func errantThenPromoted(t *testing.T, lag bool) *synth {
	s := newSynth(t)
	s.promote("a", "A", 101, "", -100)
	s.file("a", "a1", 101, "", "", run("A", 1, 5, 1, 101), run("A", 6, 10, 20, 101))
	if lag {
		s.file("b", "b1", 102, "", "", run("A", 1, 5, 1, 101), run("B", 1, 1, 40, 102))
		s.file("b", "b2", 102, "A:1-5,B:1", "", run("A", 6, 10, 20, 101))
		s.promote("b", "B", 102, "A:1-10,B:1", 60)
		s.file("b", "b3", 102, "A:1-10,B:1", "", run("B", 2, 4, 70, 102))
	} else {
		s.file("b", "b1", 102, "", "", run("A", 1, 5, 1, 101), run("B", 1, 1, 8, 102), run("A", 6, 10, 20, 101))
		s.promote("b", "B", 102, "A:1-10,B:1", 30)
		s.file("b", "b2", 102, "A:1-10,B:1", "", run("B", 2, 4, 40, 102))
	}
	s.backup("A:1-3", "a", 101, 3.5)
	return s
}

// A replica's own transaction is no evidence that the transactions it had
// yet to apply were lost. Latest mode once read a file with a transaction
// its own server originated as a sample of a writable server's state and
// refused this history, naming A:6-10, which every server holds, as the
// transactions to exclude; following that advice restored a server without
// them. The archive plans, in every mode, as it did before that rule.
func TestLatestErrantTransactionOnALaggingReplicaLosesNothing(t *testing.T) {
	checkPlanCases(t, func() *synth { return errantThenPromoted(t, true) }, []planCase{
		{"latest", ModeLatest, 0, "", false, "ArchiveHead lineage=A:1-10,B:1-4 lost= [b/b1: A:4-5,B:1] [b/b2: A:6-10] [b/b3: B:2-4]"},
		{"before B:4", ModeTimestamp, 71.5, "", false, "TargetCovered lineage=A:1-10,B:1-3 lost= [b/b1: A:4-5,B:1] [b/b2: A:6-10] [b/b3: B:2-3]"},
		{"past the archive, allowPartial", ModeTimestamp, 500, "", true, "Partial lineage=A:1-10,B:1-4 lost= [b/b1: A:4-5,B:1] [b/b2: A:6-10] [b/b3: B:2-4]"},
	})
	// The replica is never promoted: its errant transaction is on the
	// newest archived state of b, which covers a's, and excluding it
	// restores a's history.
	unpromoted := func() *synth {
		s := errantThenPromoted(t, true)
		s.sites["b"].Promotions = nil
		s.sites["b"].Rows = s.sites["b"].Rows[:2]
		return s
	}
	checkPlanCases(t, unpromoted, []planCase{
		{"never promoted, latest", ModeLatest, 0, "", false, "ArchiveHead lineage=A:1-10,B:1 lost= [b/b1: A:4-5,B:1] [b/b2: A:6-10]"},
		{"never promoted, latest excluding B:1", ModeLatest, 0, "B:1", false, "ArchiveHead lineage=A:1-10,B:1 lost= [a/a1: A:4-10]"},
	})
}

// The documented history (backup-restore.md, "Divergent history"): once b
// was promoted with its errant transaction, later targets include it as
// history and need no exclusion. A backup taken on b after the errant
// transaction holds it, and such a backup stays usable: excluding B:1 is
// impossible then (PITRExcludeIntersectsBackup), so a refusal that named
// B:1 had no remedy.
func TestLatestErrantTransactionThenPromotionIsHistory(t *testing.T) {
	checkPlanCases(t, func() *synth { return errantThenPromoted(t, false) }, []planCase{
		{"latest", ModeLatest, 0, "", false, "ArchiveHead lineage=A:1-10,B:1-4 lost= [b/b1: A:4-10,B:1] [b/b2: B:2-4]"},
		{"before B:4", ModeTimestamp, 41.5, "", false, "TargetCovered lineage=A:1-10,B:1-3 lost= [b/b1: A:4-10,B:1] [b/b2: B:2-3]"},
		{"before the promotion", ModeTimestamp, 25, "", false, "TargetCovered lineage=A:1-10,B:1 lost= [b/b1: A:4-10,B:1]"},
		{"before the promotion, excluding B:1", ModeTimestamp, 25, "B:1", false, "TargetCovered lineage=A:1-10,B:1 lost= [a/a1: A:4-10]"},
		{"past the archive, allowPartial", ModeTimestamp, 500, "", true, "Partial lineage=A:1-10,B:1-4 lost= [b/b1: A:4-10,B:1] [b/b2: B:2-4]"},
	})
	inBackup := func() *synth {
		s := errantThenPromoted(t, false)
		s.backup("A:1-5,B:1", "b", 102, 10)
		return s
	}
	checkPlanCases(t, inBackup, []planCase{
		{"backup on b, latest", ModeLatest, 0, "", false, "ArchiveHead lineage=A:1-10,B:1-4 lost= [b/b1: A:6-10] [b/b2: B:2-4]"},
		{"backup on b, latest excluding B:1", ModeLatest, 0, "B:1", false, "refused PITRExcludeIntersectsBackup divergent= lost="},
		{"backup on b, past the archive, allowPartial", ModeTimestamp, 500, "", true, "Partial lineage=A:1-10,B:1-4 lost= [b/b1: A:6-10] [b/b2: B:2-4]"},
	})
}

// A planned handoff to a server whose clock runs behind: b's clock is 9 s
// behind a's (δ is 5 s), so b's record (2 s) and its first transactions
// (3-5 s) carry times before a's last ones (up to 10 s). A record never
// proves lost a transaction committed after it, so the history plans; it
// tolerates a later primary's clock running behind by the previous era's
// length plus δ.
func TestLatestPlannedHandoffToAServerWhoseClockRunsBehind(t *testing.T) {
	build := func() *synth {
		s := newSynth(t)
		s.promote("a", "A", 101, "", -100)
		s.file("a", "a1", 101, "", "", run("A", 1, 10, 1, 101))
		s.promote("b", "B", 102, "A:1-10", 2)
		s.file("b", "b1", 102, "A:1-10", "", run("B", 1, 3, 3, 102))
		s.backup("A:1-3", "a", 101, 3.5)
		return s
	}
	checkPlanCases(t, build, []planCase{
		{"latest", ModeLatest, 0, "", false, "ArchiveHead lineage=A:1-10,B:1-3 lost= [a/a1: A:4-10] [b/b1: B:1-3]"},
	})
}

// unrecordedEpisode is a history in which transactions are lost twice: a
// writes A:1-8; b is promoted and commits B:1-3 (20-22 s), of which a
// receives B:1-2; a is promoted again and commits A:9-11 (40-42 s) without
// B:3; b, which still holds B:3, applies them, is promoted again and
// commits B:4-6. The archived head holds B:3, which a never had while it
// was writable. recorded says whether a's second episode left a promotion
// record (its sidecar observed it).
func unrecordedEpisode(t *testing.T, recorded bool) *synth {
	s := newSynth(t)
	s.promote("a", "A", 101, "", -100)
	s.file("a", "a1", 101, "", "", run("A", 1, 8, 1, 101))
	s.promote("b", "B", 102, "A:1-8", 15)
	s.file("b", "b1", 102, "A:1-8", "", run("B", 1, 3, 20, 102))
	if recorded {
		s.promote("a", "A", 101, "A:1-8,B:1-2", 35)
	}
	episode := run("A", 9, 11, 40, 101)
	s.file("a", "a2", 101, "A:1-8,B:1-2", "", episode)
	s.file("b", "b2", 102, "A:1-8,B:1-3", "", episode)
	s.promote("b", "B", 102, "A:1-11,B:1-3", 55)
	s.file("b", "b3", 102, "A:1-11,B:1-3", "", run("B", 4, 6, 60, 102))
	s.backup("A:1-5", "a", 101, 5.5)
	return s
}

// Known limitation: a writable episode that no sidecar recorded is no loss
// evidence in latest mode. With a's record of its second episode the run
// refuses and names B:3; without it the planner restores the head, B:3
// included.
//
// The difference cannot be decided from the archive. What a's file a2
// shows, "a's server committed A:9-11 after a state that lacks B:3", is
// also what a read-only replica's file shows when the replica originates a
// transaction while it has yet to apply B:3 and nothing is lost
// (TestLatestErrantTransactionOnALaggingReplicaLosesNothing): the bytes
// say that a server wrote, not that it was the group's primary. A rule on
// them refused healthy archives and told the administrator to exclude
// transactions every server holds. The promotion record is the evidence
// that a server was promoted, and the sidecar writes one for every
// writable episode it observes; an episode stays unrecorded only when the
// site's sidecar was down for all of it.
//
// This test fails when the two histories start to agree.
func TestKnownLimitationUnrecordedEpisodeIsNoLossEvidence(t *testing.T) {
	checkPlanCases(t, func() *synth { return unrecordedEpisode(t, true) }, []planCase{
		{"recorded, latest", ModeLatest, 0, "", false, "refused PITRArchiveDivergentHistory divergent=B:3 lost=B:3"},
		{"recorded, latest excluding B:3", ModeLatest, 0, "B:3", false, "ArchiveHead lineage=A:1-11,B:1-6 lost=B:3 [a/a1: A:6-8] [b/b1: B:1-2] [a/a2: A:9-11] [b/b3: B:4-6]"},
		// Excluding the episode's own transactions does not set its record
		// aside (N1).
		{"recorded, latest excluding A:9-11", ModeLatest, 0, "A:9-11", false, "refused PITRArchiveDivergentHistory divergent=B:3 lost=B:3"},
	})
	checkPlanCases(t, func() *synth { return unrecordedEpisode(t, false) }, []planCase{
		{"unrecorded, latest", ModeLatest, 0, "", false, "ArchiveHead lineage=A:1-11,B:1-6 lost= [a/a1: A:6-8] [b/b1: B:1-3] [b/b2: A:9-11] [b/b3: B:4-6]"},
		{"unrecorded, latest excluding B:3", ModeLatest, 0, "B:3", false, "ArchiveHead lineage=A:1-11,B:1-6 lost= [a/a1: A:6-8] [b/b1: B:1-2] [a/a2: A:9-11] [b/b3: B:4-6]"},
		{"unrecorded, latest excluding A:9-11", ModeLatest, 0, "A:9-11", false, "ArchiveHead lineage=A:1-11,B:1-6 lost= [a/a1: A:6-8] [b/b1: B:1-3] [b/b3: B:4-6]"},
		{"unrecorded, past the archive, allowPartial", ModeTimestamp, 500, "", true, "Partial lineage=A:1-11,B:1-6 lost= [a/a1: A:6-8] [b/b1: B:1-3] [b/b2: A:9-11] [b/b3: B:4-6]"},
	})
}

// Only a promotion record is loss evidence: a replica's file that lacks
// older transactions (c replicates from a alone and never had B:1-3) is no
// evidence that they were lost, whichever server wrote the file.
func TestLatestReplicaCopiesAreNotLossEvidence(t *testing.T) {
	for _, writer := range []uint32{103, 101} {
		s := newSynth(t)
		s.promote("a", "A", 101, "", -100)
		s.file("a", "a1", 101, "", "", run("A", 1, 8, 1, 101))
		s.promote("b", "B", 102, "A:1-8", 15)
		s.file("b", "b1", 102, "A:1-8", "", run("B", 1, 3, 20, 102))
		s.promote("a", "A", 101, "A:1-8,B:1-3", 35)
		a3 := run("A", 9, 11, 40, 101)
		s.file("a", "a2", 101, "A:1-8,B:1-3", "", a3)
		s.file("c", "c1", writer, "A:1-8", "", a3)
		s.backup("A:1-5", "a", 101, 5.5)
		p, err := s.run(ModeLatest, 0, "", false)
		if err != nil || !p.Lineage.Equal(MustParseSet("A:1-11,B:1-3")) || !p.Lost.IsEmpty() {
			t.Fatalf("c1 written by server %d: plan %+v %v", writer, p, err)
		}
	}
}
