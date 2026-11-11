package pitrplan

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"strings"
	"testing"
	"time"
	"unsafe"

	"github.com/shipstream/bloodraven/internal/pitrarchive"
)

// fx returns txns with the effect digest fx on every one.
func fx(txns []stx, digest byte) []stx {
	for i := range txns {
		txns[i].fx = digest
	}
	return txns
}

// Issue #268: two downloaded copies of a GTID that agree on commit time,
// origin server and emptiness are compared on their row changes. Other
// tables refuse; the same tables with another layout say nothing; the same
// tables and layout with other row bytes refuse. The refusal names the
// GTID, both rows, what differs in plain words, and both digests. b1
// overlaps the used file a1, so the closure downloads it.
func TestEffectMismatch(t *testing.T) {
	build := func(other stx) *synth {
		s := newSynth(t)
		s.file("a", "a1", 101, "", "", fx(run("A", 1, 30, 1, 101), 1))
		s.file("b", "b1", 102, "A:1-19", "", []stx{other}, fx(run("A", 21, 25, 21, 101), 1))
		s.backup("A:1-10", "a", 101, 10.5)
		return s
	}
	const advice = "restore from a full backup taken later, once its newest transactions are archived in binlog files after these two"
	// The same changes: the plan is certified.
	p, err := build(stx{g: "A:20", oct: sec(20), srv: 101, fx: 1}).run(ModeTimestamp, 27.5, "", false)
	if err != nil || segString(p) != "[a/a1: A:11-27]" {
		t.Fatalf("faithful copy: %s, %v", segString(p), err)
	}
	// The same tables with another layout (binlog_row_image, a column added
	// or widened on one site): not comparable, whatever the row bytes are.
	for _, bag := range []byte{1, 2} {
		p, err = build(stx{g: "A:20", oct: sec(20), srv: 101, fx: 1, lay: 2, bag: bag}).run(ModeTimestamp, 27.5, "", false)
		if err != nil || segString(p) != "[a/a1: A:11-27]" {
			t.Fatalf("a copy with another layout (rows part %d): %s, %v", bag, segString(p), err)
		}
	}
	// Other tables under the same GTID, commit time and origin server. The
	// layout and the rows say nothing more.
	for _, other := range []stx{{fx: 2}, {fx: 2, lay: 1, bag: 1}} {
		other.g, other.oct, other.srv = "A:20", sec(20), 101
		_, err = build(other).run(ModeTimestamp, 27.5, "", false)
		wantToken(t, err, pitrarchive.TokenArchiveNamespaceReuse,
			"A:20 differs between a/a1 and b/b1 (row changes differ: they change different tables, effect digest 01010101-0101010101010101-01010101 vs 02020202-",
			"a transaction with other changes was injected under this GTID (a manual repair with SET GTID_NEXT), or one server applied only part of it", advice)
		if strings.Contains(err.Error(), "data had diverged") {
			t.Fatalf("other tables are not reported as diverged data: %v", err)
		}
	}
	// The same tables and layout with other row bytes.
	_, err = build(stx{g: "A:20", oct: sec(20), srv: 101, fx: 1, bag: 2}).run(ModeTimestamp, 27.5, "", false)
	wantToken(t, err, pitrarchive.TokenArchiveNamespaceReuse,
		"A:20 differs between a/a1 and b/b1 (row changes differ: the same tables with different row data, effect digest 01010101-0101010101010101-01010101 vs 01010101-0101010101010101-02020202)",
		"a transaction with other values was injected under this GTID (a manual repair with SET GTID_NEXT), or the two servers' data had diverged when they applied it", advice)
	// With another attribute differing too, every reason is listed and the
	// wording stays the general one.
	_, err = build(stx{g: "A:20", oct: sec(19.5), srv: 109, fx: 2}).run(ModeTimestamp, 27.5, "", false)
	wantToken(t, err, pitrarchive.TokenArchiveNamespaceReuse, "A:20 differs between a/a1 and b/b1 (commit time ", "origin server 101 vs 109", "row changes differ: they change different tables, effect digest 01", "the GTID was reissued, or one site's history was rewritten")
	if strings.Contains(err.Error(), "injected under this GTID") {
		t.Fatalf("a copy that differs in its GTID event is not reported as an injected repair: %v", err)
	}
	// Another attribute and another layout: the row changes are not a reason.
	_, err = build(stx{g: "A:20", oct: sec(19.5), srv: 101, fx: 1, lay: 2, bag: 2}).run(ModeTimestamp, 27.5, "", false)
	wantToken(t, err, pitrarchive.TokenArchiveNamespaceReuse, "A:20 differs between a/a1 and b/b1 (commit time ", "the GTID was reissued")
	if strings.Contains(err.Error(), "row changes") {
		t.Fatalf("copies with different layouts are not compared on their rows: %v", err)
	}
}

// EffectDigest.Compare is the relation the planner refuses on.
func TestEffectDigestCompare(t *testing.T) {
	base := effectParts(1, 1, 1)
	for _, tc := range []struct {
		name  string
		other EffectDigest
		want  EffectRelation
	}{
		{"equal", effectParts(1, 1, 1), EffectEqual},
		{"no digest", EffectDigest{}, EffectNotComparable},
		{"other tables", effectParts(2, 1, 1), EffectTablesDiffer},
		{"other tables, layout and rows", effectParts(2, 2, 2), EffectTablesDiffer},
		{"other layout", effectParts(1, 2, 1), EffectNotComparable},
		{"other layout and rows", effectParts(1, 2, 2), EffectNotComparable},
		{"other rows", effectParts(1, 1, 2), EffectRowsDiffer},
	} {
		if got := base.Compare(tc.other); got != tc.want {
			t.Errorf("%s: %d, want %d", tc.name, got, tc.want)
		}
		if got := tc.other.Compare(base); got != tc.want {
			t.Errorf("%s, reversed: %d, want %d", tc.name, got, tc.want)
		}
	}
	if (EffectDigest{}).Compare(EffectDigest{}) != EffectNotComparable {
		t.Error("two copies without digests are comparable")
	}
	// The parts are where the type says: 32, 64 and 32 bits.
	d := NewEffectDigest(0x01020304, 0x1112131415161718, 0x21222324)
	if d.String() != "01020304-1112131415161718-21222324" || d.IsZero() || !NewEffectDigest(0, 0, 0).IsZero() {
		t.Fatalf("digest %s", d)
	}
	for bit := 0; bit < 128; bit++ {
		o := d
		o[bit/8] ^= 1 << (bit % 8)
		want := EffectTablesDiffer
		switch {
		case bit >= 96:
			want = EffectRowsDiffer
		case bit >= 32:
			want = EffectNotComparable
		}
		if got := d.Compare(o); got != want {
			t.Errorf("bit %d flipped: %d, want %d", bit, got, want)
		}
	}
}

// A zero digest is "not comparable": it never refuses, on either side or
// both, and it does not hide a later comparable pair. The registry keeps
// the first copy that has a digest (a1 is downloaded first in every case).
func TestEffectZeroDigestIsNotCompared(t *testing.T) {
	for _, tc := range []struct {
		name    string
		a, b, c byte // A:20's digest in a1, b1 and c1 (c1 is absent when c is 0xff)
		refused string
	}{
		{name: "both zero", a: 0, b: 0, c: 0xff},
		{name: "first zero", a: 0, b: 2, c: 0xff},
		{name: "second zero", a: 1, b: 0, c: 0xff},
		{name: "first zero, the others agree", a: 0, b: 2, c: 2},
		{name: "first zero, the others differ", a: 0, b: 2, c: 3, refused: "A:20 differs between b/b1 and c/c1 (row changes differ"},
		{name: "second zero, the others differ", a: 1, b: 0, c: 3, refused: "A:20 differs between a/a1 and c/c1 (row changes differ"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			s := newSynth(t)
			txns := run("A", 1, 30, 1, 101)
			txns[19].fx = tc.a
			s.file("a", "a1", 101, "", "", txns)
			s.file("b", "b1", 102, "A:1-19", "", []stx{{g: "A:20", oct: sec(20), srv: 101, fx: tc.b}})
			if tc.c != 0xff {
				s.file("c", "c1", 103, "A:1-19", "", []stx{{g: "A:20", oct: sec(20), srv: 101, fx: tc.c}})
			}
			s.backup("A:1-10", "a", 101, 10.5)
			_, err := s.run(ModeTimestamp, 27.5, "", false)
			if tc.refused == "" {
				if err != nil {
					t.Fatalf("refused: %v", err)
				}
				return
			}
			wantToken(t, err, pitrarchive.TokenArchiveNamespaceReuse, tc.refused)
		})
	}
}

// "Not comparable" is not transitive: a copy with another layout, or
// without a digest, says nothing about the copies on either side of it,
// and two of those may still differ. The registry keeps one copy per
// layout, so every comparable pair of downloaded copies is compared: a
// restore is refused exactly when some pair of its copies differs,
// whatever the order of the downloads, in memory and spilled. Every
// assignment of six kinds of copy to four sites is planned, which covers
// every order of every multiset of them.
func TestEffectKeptCopiesAnyOrder(t *testing.T) {
	kinds := []stx{
		{},                      // no digest
		{fx: 1, lay: 1, bag: 1}, // tables 1, layout 1
		{fx: 1, lay: 1, bag: 2}, // the same with other rows
		{fx: 1, lay: 2, bag: 1}, // tables 1, layout 2
		{fx: 1, lay: 2, bag: 2}, // the same with other rows
		{fx: 2, lay: 3, bag: 1}, // other tables
		{fx: 1, lay: 3, bag: 1}, // tables 1, a third layout
	}
	digest := func(k int) EffectDigest { return effectParts(kinds[k].fx, kinds[k].lay, kinds[k].bag) }
	sites := []string{"a", "b", "c", "d"}
	plan := func(pick [4]int) error {
		s := newSynth(t)
		for k, site := range sites {
			tx := kinds[pick[k]]
			tx.g, tx.oct, tx.srv = "A:20", sec(20), 101
			if k == 0 {
				txns := run("A", 1, 30, 1, 101)
				txns[19] = tx
				s.file(site, site+"1", 101, "", "", txns)
				continue
			}
			s.file(site, site+"1", uint32(101+k), "A:1-19", "", []stx{tx})
		}
		s.backup("A:1-10", "a", 101, 10.5)
		_, err := s.run(ModeTimestamp, 27.5, "", false)
		return err
	}
	check := func(t *testing.T) (refusals, plans, chains int) {
		var pick [4]int
		for n := 0; n < len(kinds)*len(kinds)*len(kinds)*len(kinds); n++ {
			for k, v := 0, n; k < 4; k, v = k+1, v/len(kinds) {
				pick[k] = v % len(kinds)
			}
			// The oracle: every pair of copies.
			differ := map[[2]string]EffectRelation{}
			layouts := map[byte]bool{}
			for i := range sites {
				if kinds[pick[i]].fx == 1 {
					layouts[kinds[pick[i]].lay] = true
				}
				for j := range sites {
					if rel := digest(pick[i]).Compare(digest(pick[j])); rel == EffectTablesDiffer || rel == EffectRowsDiffer {
						differ[[2]string{sites[i], sites[j]}] = rel
					}
				}
			}
			if len(layouts) > 1 {
				chains++
			}
			err := plan(pick)
			if len(differ) == 0 {
				if err != nil {
					t.Fatalf("copies %v: no pair differs, refused: %v", pick, err)
				}
				plans++
				continue
			}
			refusals++
			r, ok := RefusalOf(err)
			if !ok || r.Token != pitrarchive.TokenArchiveNamespaceReuse {
				t.Fatalf("copies %v: pairs %v differ, got %v", pick, differ, err)
			}
			// The refusal names a pair that differs, and how.
			var x, y string
			if _, serr := fmt.Sscanf(r.Message, "A:20 differs between %1s/", &x); serr != nil {
				t.Fatalf("copies %v: %v", pick, err)
			}
			rest := r.Message[strings.Index(r.Message, " and ")+5:]
			y = rest[:1]
			rel, ok := differ[[2]string{x, y}]
			want := "the same tables with different row data"
			if rel == EffectTablesDiffer {
				want = "they change different tables"
			}
			if !ok || !strings.Contains(r.Message, "(row changes differ: "+want+", effect digest "+digest(pick[x[0]-'a']).String()+" vs "+digest(pick[y[0]-'a']).String()+")") {
				t.Fatalf("copies %v: pairs %v differ, the refusal is %v", pick, differ, err)
			}
		}
		return refusals, plans, chains
	}
	t.Run("in memory", func(t *testing.T) {
		refusals, plans, chains := check(t)
		if refusals < 1500 || plans < 300 || chains < 500 {
			t.Fatalf("%d refusals, %d plans, %d cases with more than one layout", refusals, plans, chains)
		}
	})
	t.Run("spilled", func(t *testing.T) {
		forceSpill(t)
		check(t)
	})
}

// A GTID's kept copies are a list in the registry: in the order they were
// kept, however many, and beside other GTIDs' lists.
func TestOccKeptCopies(t *testing.T) {
	for _, spill := range []bool{false, true} {
		b := NewBudget(1 << 20)
		if spill {
			b.SetSpill(tinySpill(newFileArena(t)))
		}
		tab := newOccTable(b)
		recs := make([]occRec, 3)
		want := make([][]occCopy, 3)
		for g := range recs {
			recs[g] = occRec{file: uint32(g) + 1, i: uint32(10 * g), n: 1}
			want[g] = []occCopy{{uint32(g) + 1, uint32(10 * g)}}
		}
		// Interleaved, so the lists' nodes are not adjacent.
		for round := 1; round <= 40; round++ {
			for g := range recs {
				if g == 2 && round > 1 {
					continue // one GTID keeps two copies
				}
				c := occCopy{file: uint32(100*round + g), i: uint32(round)}
				if err := tab.keep(&recs[g], c); err != nil {
					t.Fatal(err)
				}
				want[g] = append(want[g], c)
			}
		}
		for g := range recs {
			if got := tab.kept(recs[g], nil); !reflect.DeepEqual(got, want[g]) || recs[g].file != uint32(g)+1 || recs[g].list != 1 {
				t.Fatalf("spill %v, GTID %d: kept %v (record %+v), want %v", spill, g, got, recs[g], want[g])
			}
		}
		if got := tab.kept(occRec{file: 7, i: 3, n: 1}, nil); !reflect.DeepEqual(got, []occCopy{{7, 3}}) {
			t.Fatalf("a record without a list: %v", got)
		}
		if spill != (b.Spilled() > 0) {
			t.Fatalf("spill %v: %d bytes spilled", spill, b.Spilled())
		}
	}
	if unsafe.Sizeof(occRec{}) != occurrenceBytes {
		t.Fatalf("occRec is %d bytes; the up-front estimate charges %d", unsafe.Sizeof(occRec{}), occurrenceBytes)
	}
}

// failingArena is a memArena whose Alloc fails after left more calls.
type failingArena struct {
	*memArena
	left int
	err  error
}

func (f *failingArena) Alloc(n int64) ([]byte, error) {
	if f.left == 0 {
		return nil, f.err
	}
	f.left--
	return f.memArena.Alloc(n)
}

// Review R3 finding 4: an ingest that fails after it registered some of a
// file's transactions leaves records that name a file which never took its
// place in the planner's order. The next file would take that place, and
// its lookups would read the abandoned file's records as its own. Planning
// ends at the first failed ingest instead: every later fetch returns its
// error, and a record that names no file is an internal error.
func TestFailedIngestEndsPlanning(t *testing.T) {
	s := newSynth(t)
	// a1's transactions need two registry extents; b1 holds other
	// transaction indexes for the same GTIDs.
	s.file("a", "a1", 101, "", "", fx(run("A", 1, 30, 1, 101), 1))
	s.file("b", "b1", 102, "A:1-4", "", fx(run("A", 5, 30, 5, 101), 1))
	s.backup("A:1-2", "a", 101, 2.5)
	var a Archive
	for _, name := range s.order {
		a.Sites = append(a.Sites, *s.sites[name])
	}
	fetch := FetchFunc(func(_ context.Context, r RowRef) (*FileIndex, error) { return s.idx[r], nil })
	// The registry's extents are the only arrays ingest allocates; the
	// staging volume fails when the second one is allocated, after a1's
	// first eight transactions were registered.
	failed := errors.New("the staging volume failed")
	arena := &failingArena{memArena: newMemArena(), left: 1, err: failed}
	budget := NewBudget(64 << 20)
	budget.SetSpill(tinySpill(arena))
	p, err := newPlanner(context.Background(), Request{Mode: ModeLatest, Dump: s.dump, Budget: budget}, a, fetch)
	if err != nil {
		t.Fatal(err)
	}
	row := func(site string) *Row {
		for _, r := range p.rows {
			if r.Site == site {
				return r
			}
		}
		t.Fatalf("no row of site %s", site)
		return nil
	}
	_, err = p.fetch(row("a"))
	if !errors.Is(err, failed) {
		t.Fatalf("ingest of a1: %v, want the arena's error", err)
	}
	if rec, ok := p.occ.get(0, 1); !ok || rec.file != 1 || len(p.order) != 0 {
		t.Fatalf("the failed ingest registered nothing (record %+v, %v) or took a place in the order (%d files)", rec, ok, len(p.order))
	}
	// With the volume working again, b1 must not be ingested over a1's
	// records: it would compare its A:5 (transaction 0) with "file 1,
	// transaction 4", which would be its own A:9.
	arena.left = 1 << 20
	_, again := p.fetch(row("b"))
	if again != err {
		t.Fatalf("the fetch after a failed ingest returned %v, want the first error %v", again, err)
	}
	if _, again = p.fetch(row("a")); again != err {
		t.Fatalf("fetching the failed row again returned %v", again)
	}
	if p.mismatch != nil || len(p.order) != 0 || p.files[row("b")] != nil {
		t.Fatalf("planning went on after a failed ingest: mismatch %v, %d files", p.mismatch, len(p.order))
	}
	if err := p.orderCheck(); err != p.broken {
		t.Fatalf("the order check after a failed ingest: %v", err)
	}
	// A record that names a file outside the order is never resolved.
	if _, _, cerr := p.copyOf(occCopy{file: 1, i: 4}); cerr == nil || !strings.Contains(cerr.Error(), "internal error") {
		t.Fatalf("copyOf of a file that is not in the order: %v", cerr)
	}
}

// The closure compares row changes wherever it compares the other
// attributes: in an active row that overlaps a used file, in a divergent
// row and in a readable quarantined row. A copy whose other copy is in no
// downloaded file is not compared (the documented limit of the check): a0
// is never read, because the frontier stops at a1 and the plan uses a2.
func TestEffectClosureCoversDivergentAndQuarantinedRows(t *testing.T) {
	build := func(other string, at float64) (*synth, RowRef) {
		s := newSynth(t)
		s.file("a", "a0", 101, "", "", fx(run("A", 1, 10, 1, 101), 1))
		s.file("a", "a1", 101, "A:1-10", "", fx(run("A", 11, 30, 11, 101), 1))
		s.file("a", "a2", 101, "A:1-30", "", fx(run("A", 31, 60, 31, 101), 1))
		ref := s.file("b", "b1", 102, "", "", []stx{{g: other, oct: sec(at), srv: 101, fx: 9}})
		s.backup("A:1-30", "a", 101, 30.5)
		return s, ref
	}
	const refused = "A:25 differs between a/a1 and b/b1 (row changes differ"
	s, _ := build("A:25", 25)
	_, err := s.run(ModeTimestamp, 50.5, "", false)
	wantToken(t, err, pitrarchive.TokenArchiveNamespaceReuse, refused)
	s, _ = build("A:25", 25)
	s.sites["b"].Rows[0].Divergent = MustParseSet("B:1")
	_, err = s.run(ModeTimestamp, 50.5, "", false)
	wantToken(t, err, pitrarchive.TokenArchiveNamespaceReuse, refused)
	s, ref := build("A:25", 25)
	s.quarantine(ref, synthEpoch.Add(40*time.Second))
	_, err = s.run(ModeTimestamp, 50.5, "", false)
	wantToken(t, err, pitrarchive.TokenArchiveNamespaceReuse, refused)

	s, _ = build("A:5", 5)
	p, err := s.run(ModeTimestamp, 50.5, "", false)
	if err != nil || s.fetches["a/a0"] != 0 || s.fetches["b/b1"] != 0 {
		t.Fatalf("a copy outside the closure: %v; a0 fetched %d times, b1 %d (%s)", err, s.fetches["a/a0"], s.fetches["b/b1"], segString(p))
	}
}

// Issue #271: every segment carries the file offset of the first
// transaction it replays, which is not its first position when that one is
// excluded, and coverage.json prints it.
func TestSegmentStartPosition(t *testing.T) {
	s := newSynth(t)
	s.file("a", "a1", 101, "", "", run("A", 1, 20, 1, 101))
	s.file("b", "b1", 102, "A:1-20", "", run("B", 1, 10, 21, 102))
	s.file("a", "a2", 101, "A:1-20,B:1-10", "", run("A", 21, 30, 31, 101))
	s.promote("b", "B", 102, "A:1-20", 20.2)
	s.backup("A:1-10", "a", 101, 10.5)
	// A:11-12 open the first segment and are excluded: it starts at A:13.
	p, err := s.run(ModeTimestamp, 35.5, "A:11-12", false)
	if err != nil {
		t.Fatal(err)
	}
	if segString(p) != "[a/a1: A:13-20] [b/b1: B:1-10] [a/a2: A:21-25]" {
		t.Fatalf("plan %s", segString(p))
	}
	for k, want := range []struct {
		start int
		pos   int64
	}{{10, synthPos(12)}, {0, synthPos(0)}, {0, synthPos(0)}} {
		if seg := p.Segments[k]; seg.Start != want.start || seg.StartPosition != want.pos {
			t.Errorf("segment %d: position %d, offset %d; want %d, %d", k, seg.Start, seg.StartPosition, want.start, want.pos)
		}
	}
	data, err := p.Coverage(CoverageArchive{}, CoverageDump{}, "").Marshal()
	if err != nil {
		t.Fatal(err)
	}
	var cov struct {
		Segments []map[string]any `json:"segments"`
	}
	if err := json.Unmarshal(data, &cov); err != nil {
		t.Fatal(err)
	}
	for k, seg := range cov.Segments {
		if got, ok := seg["startPosition"].(float64); !ok || int64(got) != p.Segments[k].StartPosition {
			t.Errorf("coverage segment %d: startPosition %v, want %d", k, seg["startPosition"], p.Segments[k].StartPosition)
		}
	}
}

// The index builder refuses offsets that do not advance: every
// transaction's GTID event follows the previous one's.
func TestIndexBuilderOffsets(t *testing.T) {
	b := NewIndexBuilder("a/a1", NewBudget(1<<20))
	if err := b.SetPrevious(Set{}); err != nil {
		t.Fatal(err)
	}
	wantToken(t, b.BeginTxn(GTID{"A", 1}, 1, 101, 3), pitrarchive.TokenArchiveObjectInvalid, "offset 3")
	if err := b.BeginTxn(GTID{"A", 1}, 1, 101, 120); err != nil {
		t.Fatal(err)
	}
	b.SetEffect(effect(7))
	wantToken(t, b.BeginTxn(GTID{"A", 2}, 2, 101, 120), pitrarchive.TokenArchiveObjectInvalid, "A:2 at offset 120 does not follow")
	idx, err := b.Finish("d", 200)
	if err != nil {
		t.Fatal(err)
	}
	if tx := idx.Txns[0]; tx.Pos != 120 || tx.Effect != effect(7) || tx.Effect.IsZero() || !(EffectDigest{}).IsZero() {
		t.Fatalf("transaction %+v", tx)
	}
}

// Txn stays a fixed-size record without pointers (the index is spilled to
// disk as is, issue #266), and its size is what the budget charges.
func TestTxnIsFixedSizeAndPointerFree(t *testing.T) {
	typ := reflect.TypeOf(Txn{})
	if typ.Size() != 56 || TxnBytes != 56 {
		t.Fatalf("Txn is %d bytes, charged %d; update the budget comments and the spill format", typ.Size(), TxnBytes)
	}
	for i := 0; i < typ.NumField(); i++ {
		switch k := typ.Field(i).Type.Kind(); k {
		case reflect.Int64, reflect.Uint32, reflect.Uint8:
		case reflect.Array:
			if typ.Field(i).Type.Elem().Kind() != reflect.Uint8 {
				t.Errorf("field %s is an array of %s", typ.Field(i).Name, typ.Field(i).Type.Elem())
			}
		default:
			t.Errorf("field %s is a %s; Txn must hold no pointers", typ.Field(i).Name, k)
		}
	}
}
