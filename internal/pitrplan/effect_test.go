package pitrplan

import (
	"encoding/json"
	"reflect"
	"strings"
	"testing"
	"time"

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
// origin server and emptiness but change different rows are refused, naming
// the GTID, both rows and both digests. b1 overlaps the used file a1, so
// the closure downloads it.
func TestEffectMismatch(t *testing.T) {
	build := func(other stx) *synth {
		s := newSynth(t)
		s.file("a", "a1", 101, "", "", fx(run("A", 1, 30, 1, 101), 1))
		s.file("b", "b1", 102, "A:1-19", "", []stx{other}, fx(run("A", 21, 25, 21, 101), 1))
		s.backup("A:1-10", "a", 101, 10.5)
		return s
	}
	// The same changes: the plan is certified.
	p, err := build(stx{g: "A:20", oct: sec(20), srv: 101, fx: 1}).run(ModeTimestamp, 27.5, "", false)
	if err != nil || segString(p) != "[a/a1: A:11-27]" {
		t.Fatalf("faithful copy: %s, %v", segString(p), err)
	}
	// Other changes under the same GTID, commit time and origin server.
	_, err = build(stx{g: "A:20", oct: sec(20), srv: 101, fx: 2}).run(ModeTimestamp, 27.5, "", false)
	wantToken(t, err, pitrarchive.TokenArchiveNamespaceReuse,
		"A:20 differs between a/a1 and b/b1 (row changes differ: effect digest 01010101010101010101010101010101 vs 02020202020202020202020202020202)",
		"injected under this GTID", "SET GTID_NEXT", "restore from a full backup taken later, once its newest transactions are archived in binlog files after these two")
	// With another attribute differing too, every reason is listed and the
	// wording stays the general one.
	_, err = build(stx{g: "A:20", oct: sec(19.5), srv: 109, fx: 2}).run(ModeTimestamp, 27.5, "", false)
	wantToken(t, err, pitrarchive.TokenArchiveNamespaceReuse, "A:20 differs between a/a1 and b/b1 (commit time ", "origin server 101 vs 109", "row changes differ: effect digest 01", "the GTID was reissued, or one site's history was rewritten")
	if strings.Contains(err.Error(), "injected under this GTID") {
		t.Fatalf("a copy that differs in its GTID event is not reported as an injected repair: %v", err)
	}
}

// A zero digest is "not comparable": it never refuses, on either side or
// both, and it does not hide a later comparable pair. The registry keeps
// the first copy that has a digest, so the outcome does not depend on the
// download order (a1 is downloaded first in every case).
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
