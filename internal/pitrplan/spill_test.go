package pitrplan

import (
	"context"
	"errors"
	"fmt"
	"math/rand"
	"reflect"
	"runtime"
	"slices"
	"strconv"
	"strings"
	"testing"
	"time"
	"unsafe"

	"github.com/shipstream/bloodraven/internal/pitrarchive"
)

// TestSpilledRecordsArePointerFree: every record type the planner puts in
// an array is a fixed-size type without pointers, so the garbage collector
// has nothing to find in arena memory, and a type with a pointer cannot be
// spilled by mistake.
func TestSpilledRecordsArePointerFree(t *testing.T) {
	for _, typ := range []reflect.Type{
		reflect.TypeFor[Txn](), reflect.TypeFor[occRec](), reflect.TypeFor[posRec](), reflect.TypeFor[classRec](),
		reflect.TypeFor[classState](), reflect.TypeFor[orderNode](), reflect.TypeFor[orderEdge](), reflect.TypeFor[orderFrame](),
		reflect.TypeFor[bool](), reflect.TypeFor[byte](), reflect.TypeFor[int32](), reflect.TypeFor[int64](),
	} {
		if !pointerFree(typ) {
			t.Errorf("%s holds pointers", typ)
		}
	}
	for _, typ := range []reflect.Type{reflect.TypeFor[GTID](), reflect.TypeFor[*int](), reflect.TypeFor[[]byte](), reflect.TypeFor[struct{ f *file }]()} {
		if pointerFree(typ) {
			t.Errorf("%s passes as pointer-free", typ)
		}
	}
	if unsafe.Sizeof(occRec{}) != occurrenceBytes || unsafe.Sizeof(posRec{}) != 24 || unsafe.Sizeof(classRec{}) != 24 {
		t.Errorf("record sizes changed: occRec %d, posRec %d, classRec %d", unsafe.Sizeof(occRec{}), unsafe.Sizeof(posRec{}), unsafe.Sizeof(classRec{}))
	}
	defer func() {
		if recover() == nil {
			t.Error("an array of a type with pointers was spilled")
		}
	}()
	b := NewBudget(1 << 20)
	b.SetSpill(tinySpill(newMemArena()))
	_, _ = alloc[GTID](b, 4, "x")
}

// TestSpillPolicy: an array stays on the heap, charged, while the memory
// charge is within HeapMax; after that it goes to the arena and costs the
// memory budget only its bookkeeping; an array below MinBytes never
// spills; without an arena the budget refuses as it always did.
func TestSpillPolicy(t *testing.T) {
	m := newMemArena()
	b := NewBudget(16 << 20)
	b.SetSpill(SpillOptions{Arena: m, HeapMax: 64 << 10, MinBytes: 4096, DirtyMax: 64 << 10})
	window := int64(b.dirtyMax) << spillPageShift
	if !b.Spilling() {
		t.Fatal("Spilling() is false with an arena")
	}
	a1, err := alloc[int64](b, 4096, "x") // 32 KiB: within HeapMax
	if err != nil || a1.d != nil || b.Used() != 32<<10 || m.allocs != 0 {
		t.Fatalf("first array: %v, spilled %t, used %d", err, a1.d != nil, b.Used())
	}
	a2, err := alloc[int64](b, 8192, "x") // 64 KiB: would pass HeapMax
	if err != nil || a2.d == nil || b.Spilled() != 64<<10 || m.used != 64<<10 {
		t.Fatalf("second array: %v, spilled %t, %d bytes in the arena", err, a2.d != nil, m.used)
	}
	// Memory: the first array, the dirty window, the block's bookkeeping.
	if want := int64(32<<10) + window + spillOverhead(64<<10); b.Used() != want {
		t.Fatalf("memory charge %d, want %d", b.Used(), want)
	}
	small, err := alloc[int64](b, 100, "x") // 800 bytes: below MinBytes
	if err != nil || small.d != nil {
		t.Fatalf("an array below MinBytes: %v, spilled %t", err, small.d != nil)
	}
	// Reads and writes work alike.
	for i := range a2.s {
		a2.set(i, int64(i))
	}
	for i, v := range a2.s {
		if v != int64(i) {
			t.Fatalf("a spilled array reads %d at %d", v, i)
		}
	}
	// Growing a heap array past HeapMax moves it to the arena with its
	// records; shrinking a spilled one keeps them.
	for i := range a1.s {
		a1.set(i, int64(-i))
	}
	if err := a1.resize(16384, "x"); err != nil || a1.d == nil {
		t.Fatalf("growing: %v, spilled %t", err, a1.d != nil)
	}
	if err := a1.resize(1000, "x"); err != nil || len(a1.s) != 1000 {
		t.Fatal(err)
	}
	for i, v := range a1.s {
		if v != int64(-i) {
			t.Fatalf("a resized array reads %d at %d", v, i)
		}
	}
	a1.free()
	a2.free()
	small.free()
	a1.free() // freeing twice is harmless
	if b.Used() != 0 || b.Spilled() != 0 || m.used != 0 || len(m.blocks) != 0 {
		t.Fatalf("after freeing everything: memory %d, spilled %d, arena %d bytes in %d blocks", b.Used(), b.Spilled(), m.used, len(m.blocks))
	}
	if b.SpillPeak() < 128<<10 {
		t.Fatalf("spill peak %d", b.SpillPeak())
	}
	// No arena: the old behaviour.
	b = NewBudget(1 << 20)
	b.SetSpill(SpillOptions{})
	if _, err := alloc[int64](b, 1<<20, "the index"); err == nil {
		t.Fatal("8 MiB fit a 1 MiB budget")
	} else {
		wantToken(t, err, pitrarchive.TokenPlanTooLarge, "planning needs more than 1048576 bytes (the index;", "raise the Job's memory limit")
	}
}

// TestSpillRefusalsNameTheLimit: when the arena has no room an array
// still uses memory that is left; when neither has room the refusal names
// both limits; what cannot spill refuses on memory and says so; an arena
// error that is not a refusal ends planning as an error.
func TestSpillRefusalsNameTheLimit(t *testing.T) {
	m := newMemArena()
	m.limit = 100 << 10
	b := NewBudget(1 << 20)
	b.SetSpill(SpillOptions{Arena: m, HeapMax: 0, MinBytes: 1, DirtyMax: 64 << 10})
	a1, err := alloc[byte](b, 80<<10, "x")
	if err != nil || a1.d == nil {
		t.Fatalf("%v", err)
	}
	// The arena is full; memory has room.
	a2, err := alloc[byte](b, 80<<10, "x")
	if err != nil || a2.d != nil {
		t.Fatalf("an array the arena refused did not fall back to memory: %v", err)
	}
	// Growth of a spilled array the arena cannot take moves to memory.
	a1.fill(0, len(a1.s), 7)
	if err := a1.resize(200<<10, "x"); err != nil || a1.d != nil || a1.s[80<<10-1] != 7 || a1.s[80<<10] != 0 {
		t.Fatalf("growth past the arena's room: %v, spilled %t", err, a1.d != nil)
	}
	// Neither has room.
	_, err = alloc[byte](b, 2<<20, "the plan search positions")
	wantToken(t, err, pitrarchive.TokenPlanTooLarge, "planning needs 2097152 more bytes (the plan search positions;", "fit neither limit", "the memory budget of 1048576 bytes", "raise the staging volume's size")
	// What cannot spill.
	err = b.Charge(2<<20, "file sets")
	wantToken(t, err, pitrarchive.TokenPlanTooLarge, "of memory for data it cannot spill to the staging volume (file sets;", "raise the Job's memory limit")
	// A failing arena.
	m.failAlloc = errors.New("input/output error")
	_, err = alloc[byte](b, 8<<10, "x")
	if err == nil || err.Error() != "input/output error" {
		t.Fatalf("an arena I/O error became %v", err)
	}
	if _, ok := RefusalOf(err); ok {
		t.Fatal("an arena I/O error became a refusal")
	}
}

// TestSpillWriteBackIsBounded: the pages of spilled arrays written and
// not yet written back never exceed DirtyMax, whatever the pattern of the
// writes. The test keeps its own set of written pages, removing the ones
// each Sync covers: a page the budget failed to count would stay in it.
func TestSpillWriteBackIsBounded(t *testing.T) {
	const dirtyPages = 32
	m := newMemArena()
	written := map[uintptr]bool{}
	m.onSync = func(block []byte, off, n int64) {
		base := uintptr(unsafe.Pointer(&block[0]))
		for pg := (base + uintptr(off)) >> spillPageShift; pg <= (base+uintptr(off+n)-1)>>spillPageShift; pg++ {
			delete(written, pg)
		}
	}
	b := NewBudget(64 << 20)
	b.SetSpill(SpillOptions{Arena: m, MinBytes: 1, DirtyMax: dirtyPages << spillPageShift})
	wrote := func(what string, a *arr[int64], lo, hi int) {
		t.Helper()
		base := uintptr(unsafe.Pointer(&a.s[0]))
		for pg := (base + uintptr(lo)*8) >> spillPageShift; pg <= (base+uintptr(hi)*8-1)>>spillPageShift; pg++ {
			written[pg] = true
		}
		if len(written) > dirtyPages {
			t.Fatalf("%s: %d pages are written and not written back, more than the bound of %d", what, len(written), dirtyPages)
		}
		if b.dirtyPages > dirtyPages {
			t.Fatalf("%s: the budget counts %d dirty pages, more than its bound", what, b.dirtyPages)
		}
	}
	const n = 1 << 18 // 2 MiB each
	var arrays []*arr[int64]
	for range 3 {
		a, err := alloc[int64](b, n, "x")
		if err != nil {
			t.Fatal(err)
		}
		arrays = append(arrays, &a)
	}
	rng := rand.New(rand.NewSource(266))
	// In order.
	for i := range n {
		arrays[0].set(i, int64(i))
		wrote("in order", arrays[0], i, i+1)
	}
	// At random, across arrays, rewriting pages.
	for range 200000 {
		a := arrays[rng.Intn(len(arrays))]
		i := rng.Intn(n)
		a.set(i, int64(i))
		wrote("at random", a, i, i+1)
	}
	// In bulk: fill and copyIn write more than the bound in one call, in
	// steps the budget writes back between; the last step is what remains.
	arrays[1].fill(0, n, 5)
	arrays[2].copyIn(0, arrays[0].s)
	if b.dirtyPages > dirtyPages {
		t.Fatalf("after bulk writes the budget counts %d dirty pages", b.dirtyPages)
	}
	for i := range n {
		if arrays[0].s[i] != int64(i) || arrays[1].s[i] != 5 || arrays[2].s[i] != int64(i) {
			t.Fatalf("record %d: %d %d %d", i, arrays[0].s[i], arrays[1].s[i], arrays[2].s[i])
		}
	}
	if m.syncs == 0 || b.flushes == 0 {
		t.Fatal("nothing was written back")
	}
	// A freed array's pages are written back before the arena takes it,
	// and no longer count.
	arrays[2].set(0, 1)
	before := m.syncs
	arrays[2].free()
	if m.syncs != before+1 || b.dirtyPages > dirtyPages {
		t.Fatalf("freeing a written array: %d syncs, %d dirty pages", m.syncs-before, b.dirtyPages)
	}
	// A write-back that fails is kept, and stops every later allocation.
	m.failSync = errors.New("input/output error")
	arrays[0].fill(0, n, 1)
	if b.SpillErr() == nil || !strings.Contains(b.SpillErr().Error(), "input/output error") {
		t.Fatalf("SpillErr = %v", b.SpillErr())
	}
	if _, err := alloc[int64](b, n, "x"); err == nil || err != b.SpillErr() {
		t.Fatalf("an allocation after a failed write-back: %v", err)
	}
}

// TestSortFuncMatchesStdlib: a spilled array sorts to exactly what the
// standard library gives a heap copy, for inputs that are random, sorted,
// reversed, and made of few distinct runs, at sizes around the leaf size.
func TestSortFuncMatchesStdlib(t *testing.T) {
	rng := rand.New(rand.NewSource(266))
	cmp := func(x, y posRec) int { return cmpBucket(x, y) }
	for _, n := range []int{0, 1, 2, 3, 100, 2729, 2730, 2731, 2732, 5461, 20000, 100003} {
		for _, shape := range []string{"random", "sorted", "reversed", "few hashes", "sawtooth"} {
			b := NewBudget(64 << 20)
			m := newMemArena()
			b.SetSpill(tinySpill(m))
			a, err := alloc[posRec](b, n, "x")
			if err != nil {
				t.Fatal(err)
			}
			for i := range n {
				rec := posRec{v: int32(i)}
				switch shape {
				case "random":
					rec.hash = rng.Uint64()
				case "sorted":
					rec.hash = uint64(i)
				case "reversed":
					rec.hash = uint64(n - i)
				case "few hashes":
					rec.hash, rec.count = uint64(rng.Intn(3)), int64(rng.Intn(3))
				case "sawtooth":
					rec.hash = uint64(i % 17)
				}
				a.set(i, rec)
			}
			want := slices.Clone(a.s)
			slices.SortFunc(want, cmp)
			a.sortFunc(cmp)
			if !slices.Equal(a.s, want) {
				t.Fatalf("n=%d %s: the spilled sort differs from the standard library's", n, shape)
			}
			if n > 0 && a.d == nil {
				t.Fatal("the array was not spilled")
			}
			if b.dirtyPages > b.dirtyMax {
				t.Fatalf("n=%d %s: %d dirty pages after the sort", n, shape, b.dirtyPages)
			}
			a.sortFunc(cmpNode)
			for i, rec := range a.s {
				if rec.v != int32(i) {
					t.Fatalf("n=%d %s: node order not restored at %d", n, shape, i)
				}
			}
			a.free()
		}
	}
}

// TestOccTableAgainstMap: the occurrence registry returns what a map
// would, for GNOs that arrive in order, in reverse, at random, far apart,
// and from many sources; and each visits every GTID once, in order.
func TestOccTableAgainstMap(t *testing.T) {
	type key struct {
		sid uint32
		gno int64
	}
	for _, spill := range []bool{false, true} {
		rng := rand.New(rand.NewSource(266))
		b := NewBudget(256 << 20)
		if spill {
			b.SetSpill(tinySpill(newFileArena(t)))
		}
		tbl := occTable{b: b}
		ref := map[key]occRec{}
		put := func(sid uint32, gno int64) {
			t.Helper()
			k := key{sid, gno}
			got, ok := tbl.get(sid, gno)
			want, had := ref[k]
			if ok != had || got != want {
				t.Fatalf("get(%d, %d) = %+v, %t; want %+v, %t", sid, gno, got, ok, want, had)
			}
			rec := occRec{file: uint32(rng.Intn(1000)) + 1, i: rng.Uint32(), n: uint16(rng.Intn(5)) + 1, node: rng.Int31()}
			if err := tbl.put(sid, gno, rec); err != nil {
				t.Fatal(err)
			}
			ref[k] = rec
		}
		for g := int64(1); g <= 20000; g++ { // a dense run, in order
			put(0, g)
		}
		for g := int64(50000); g > 30000; g-- { // in reverse
			put(1, g)
		}
		for range 20000 { // at random, with rewrites
			put(2, 1+rng.Int63n(40000))
		}
		for range 2000 { // far apart
			put(3, 1+rng.Int63n(MaxGNO))
		}
		put(3, MaxGNO)
		put(3, 1)
		for sid := uint32(4); sid < 3000; sid++ { // a tag per transaction
			put(sid, 1)
		}
		for g := int64(20001); g <= 30000; g++ { // slightly out of order
			put(4, g^1)
		}
		for k, want := range ref {
			if got, ok := tbl.get(k.sid, k.gno); !ok || got != want {
				t.Fatalf("spill=%t: get(%d, %d) = %+v, %t; want %+v", spill, k.sid, k.gno, got, ok, want)
			}
		}
		for _, k := range []key{{0, 20001}, {1, 30000}, {3, 2}, {5000, 1}, {7, 2}} {
			if _, had := ref[k]; had {
				continue
			}
			if got, ok := tbl.get(k.sid, k.gno); ok {
				t.Fatalf("get(%d, %d) of a GTID never put = %+v", k.sid, k.gno, got)
			}
		}
		seen := 0
		last := key{}
		err := tbl.each(func(sid uint32, gno int64, rec occRec) error {
			k := key{sid, gno}
			if rec != ref[k] {
				t.Fatalf("each(%d, %d) = %+v, want %+v", sid, gno, rec, ref[k])
			}
			if seen > 0 && (sid < last.sid || (sid == last.sid && gno <= last.gno)) {
				t.Fatalf("each is out of order: (%d, %d) after (%d, %d)", sid, gno, last.sid, last.gno)
			}
			seen, last = seen+1, k
			return nil
		})
		if err != nil || seen != len(ref) {
			t.Fatalf("each visited %d of %d GTIDs: %v", seen, len(ref), err)
		}
		// A source used once costs its first extent, not a page.
		var extents int
		for sid := 4; sid < 3000; sid++ {
			extents += len(tbl.src[sid].exts)
			if n := len(tbl.src[sid].exts[0].recs.s); n > occMinExtent {
				t.Fatalf("source %d's first extent has %d records", sid, n)
			}
		}
		if spill && b.Spilled() == 0 {
			t.Fatal("nothing spilled")
		}
		t.Logf("spill=%t: %d GTIDs, memory %d B, spilled %d B", spill, len(ref), b.Used(), b.Spilled())
	}
}

// TestCheckpointUndo: an abandoned attempt returns its memory charges and
// its arena blocks; a kept one keeps both.
func TestCheckpointUndo(t *testing.T) {
	m := newMemArena()
	b := NewBudget(1 << 20)
	b.SetSpill(tinySpill(m))
	kept, err := alloc[int64](b, 100, "x")
	if err != nil {
		t.Fatal(err)
	}
	used, spilled := b.Used(), b.Spilled()
	cp := b.Checkpoint()
	ib := NewIndexBuilder("a/a1", b)
	if err := ib.SetPrevious(Set{}); err != nil {
		t.Fatal(err)
	}
	for i := int64(1); i <= 1000; i++ {
		if err := ib.BeginTxn(GTID{"A", i}, i, 1, 4+i); err != nil {
			t.Fatal(err)
		}
	}
	if b.Spilled() == spilled || b.Used() == used {
		t.Fatal("the attempt allocated nothing")
	}
	cp.Undo()
	if b.Used() != used || b.Spilled() != spilled || len(m.blocks) != 1 {
		t.Fatalf("after Undo: memory %d (want %d), spilled %d (want %d), %d arena blocks", b.Used(), used, b.Spilled(), spilled, len(m.blocks))
	}
	cp = b.Checkpoint()
	extra, err := alloc[int64](b, 100, "x")
	if err != nil {
		t.Fatal(err)
	}
	cp.Keep()
	if len(m.blocks) != 2 || b.attempt != nil {
		t.Fatalf("after Keep: %d arena blocks", len(m.blocks))
	}
	extra.free()
	kept.free()
	if b.Used() != 0 || len(m.blocks) != 0 {
		t.Fatalf("after freeing: memory %d, %d arena blocks", b.Used(), len(m.blocks))
	}
}

// TestSpilledWritesAreAllCounted: the bound on unwritten spill holds only
// if every write to a spilled array goes through the budget. The arena
// here keeps what was last written back for every block; at the planner's
// peaks and at its end, a page that differs from that copy must be one
// the budget counts as written. A write that bypassed arr.set (a slice
// assignment, a copy, a sort) fails here. The plans cover the index
// build, the occurrence registry, the search with its sorts, classes,
// member lists and queue, a search that finds no plan, the order check
// with and without a cycle, and the segment fill.
func TestSpilledWritesAreAllCounted(t *testing.T) {
	m := newMemArena()
	m.shadow = map[*byte][]byte{}
	var budget *Budget
	old := testSpill
	testSpill = func() SpillOptions { return tinySpill(m) }
	defer func() { testSpill = old }()
	probes := 0
	check := func(where string) {
		t.Helper()
		probes++
		if bad := m.untracked(budget); len(bad) > 0 {
			t.Fatalf("%s: %d pages were written without being counted: %v", where, len(bad), bad[:min(len(bad), 5)])
		}
	}
	memProbe = func() { check("at a planner peak") }
	defer func() { memProbe = nil }()
	plan := func(s *synth, mode Mode, stop float64, exclude string) error {
		t.Helper()
		var a Archive
		for _, name := range s.order {
			a.Sites = append(a.Sites, *s.sites[name])
		}
		budget = NewBudget(256 << 20)
		// Each row is indexed under the plan's budget, into the arena.
		fetch := FetchFunc(func(_ context.Context, r RowRef) (*FileIndex, error) {
			idx, err := reindex(s.idx[r], r, budget)
			check("after indexing " + r.Name)
			return idx, err
		})
		var at time.Time
		if mode == ModeTimestamp {
			at = time.UnixMicro(sec(stop)).UTC()
		}
		_, err := Run(context.Background(), Request{Mode: mode, Stop: at, Exclude: MustParseSet(exclude), Dump: s.dump, Budget: budget}, a, fetch)
		check("at the end of the plan")
		return err
	}
	// Two sites, the same window: every structure is populated.
	const n = 30000
	s := newSynth(t)
	s.file("a", "a1", 101, "", "", run("A", 1, n, 1, 101))
	s.file("b", "b1", 102, "", "", run("A", 1, n, 1, 101))
	s.file("a", "a2", 101, "A:1-"+strconv.Itoa(n), "", run("A", n+1, n+10, n+1, 101))
	s.backup("A:1", "a", 101, 1.5)
	if err := plan(s, ModeTimestamp, n+0.5, ""); err != nil {
		t.Fatal(err)
	}
	if err := plan(s, ModeLatest, 0, "A:17"); err != nil {
		t.Fatal(err)
	}
	if budget.flushes == 0 || budget.SpillPeak() == 0 {
		t.Fatalf("nothing was spilled and written back: %d flushes, %d bytes", budget.flushes, budget.SpillPeak())
	}
	// No plan: the backup's set is no archived position (the diagnosis
	// walk), and a gap.
	s.backup("A:1-3,A:5", "a", 101, 5.5)
	wantToken(t, plan(s, ModeTimestamp, n+0.5, ""), pitrarchive.TokenArchiveGap)
	// An order inversion: b applied two transactions the other way round.
	s = newSynth(t)
	txns := run("A", 1, n, 1, 101)
	s.file("a", "a1", 101, "", "", txns)
	swapped := append([]stx(nil), txns...)
	swapped[n/2], swapped[n/2+1] = swapped[n/2+1], swapped[n/2]
	s.file("b", "b1", 102, "", "", swapped)
	s.backup("A:1", "a", 101, 1.5)
	wantToken(t, plan(s, ModeLatest, 0, ""), pitrarchive.TokenArchiveOrderInversion)
	if probes < 20 {
		t.Fatalf("only %d checks ran", probes)
	}
}

// coverageOf is a plan's coverage.json and its refusal, the two things
// pitr-download publishes.
func coverageOf(t testing.TB, p *Plan, err error) string {
	t.Helper()
	reason := ""
	if err != nil {
		reason = err.Error()
	}
	if p == nil {
		return "no plan: " + reason
	}
	data, merr := p.Coverage(CoverageArchive{}, CoverageDump{}, reason).Marshal()
	if merr != nil {
		t.Fatal(merr)
	}
	return string(data) + "\n" + reason
}

// diffHistory is randomHistory with what the search alone does not need:
// promotion records at the writer changes, effect digests (some copies
// not comparable, a few different), and sometimes two adjacent
// transactions swapped on one site (an order inversion). It returns the
// archive, B, X and the number of transactions in B.
func diffHistory(t *testing.T, r *rand.Rand) (*synth, string, string, int) {
	s := newSynth(t)
	// A transaction keeps its commit time on every site.
	type ev struct {
		g   string
		srv uint32
		fx  byte
		at  float64
	}
	var hist []ev
	gnos := map[string]int64{}
	writer := 0
	sids, sites := []string{"A", "B"}, []string{"a", "b"}
	executed := Set{}
	for len(hist) < 4+r.Intn(10) {
		if r.Intn(4) == 0 {
			writer = 1 - writer
			s.promote(sites[writer], sids[writer], uint32(101+writer), executed.String(), float64(len(hist))-0.5)
		}
		sid := sids[writer]
		gnos[sid]++
		e := ev{fmt.Sprintf("%s:%d", sid, gnos[sid]), uint32(101 + writer), byte(5 * r.Intn(2)), float64(len(hist))}
		executed.add(MustParseSet(e.g).GTIDs()[0])
		hist = append(hist, e)
	}
	bLen := r.Intn(len(hist))
	B, X := Set{}, Set{}
	for i, e := range hist {
		g := MustParseSet(e.g).GTIDs()[0]
		if i < bLen {
			B.add(g)
		} else if r.Intn(6) == 0 {
			X.add(g)
		}
	}
	for k, site := range sites {
		seq := append([]ev(nil), hist...)
		if r.Intn(3) == 0 {
			at := bLen + r.Intn(len(hist)-bLen+1)
			e := ev{fmt.Sprintf("E%d:%d", k, 1), uint32(101 + k), 0, float64(at) - 0.5}
			seq = append(seq[:at], append([]ev{e}, seq[at:]...)...)
			if r.Intn(2) == 0 {
				X.add(MustParseSet(e.g).GTIDs()[0])
			}
		}
		if k == 1 && r.Intn(6) == 0 && len(seq) > 2 {
			at := r.Intn(len(seq) - 1)
			seq[at], seq[at+1] = seq[at+1], seq[at]
		}
		n := len(seq)
		for cut := 0; cut < n; {
			end := cut + 1 + r.Intn(n-cut)
			prev := Set{}
			for _, e := range seq[:cut] {
				prev.add(MustParseSet(e.g).GTIDs()[0])
			}
			var txns []stx
			for _, e := range seq[cut:end] {
				tx := stx{g: e.g, oct: sec(e.at), srv: e.srv, fx: e.fx}
				switch r.Intn(40) {
				case 0:
					tx.fx = 6 // other row changes under the same GTID
				case 1, 2, 3, 4, 5, 6:
					tx.fx = 0 // this copy is not comparable
				}
				txns = append(txns, tx)
			}
			if r.Intn(4) != 0 {
				s.file(site, fmt.Sprintf("%s%d", site, cut), uint32(101+k), prev.String(), "", txns)
			}
			cut = end
		}
	}
	return s, B.String(), X.String(), bLen
}

// TestPlansIdenticalSpilled is the differential test of the spill: the
// same random two-site histories (alternating writers with promotion
// records, errant transactions, order inversions, effect digests, files
// cut at random and some missing) are indexed and planned twice, in
// memory and with every array in a file arena, in latest and timestamp
// mode, with and without forced hash collisions. The coverage.json and
// the refusal text must be the same byte for byte.
func TestPlansIdenticalSpilled(t *testing.T) {
	planned, refused := 0, 0
	tokens := map[pitrarchive.Token]int{}
	arena := newFileArena(t)
	runAll := func(seed int64, spill bool) []string {
		old := testSpill
		defer func() { testSpill = old }()
		testSpill = nil
		if spill {
			testSpill = func() SpillOptions { return tinySpill(arena) }
		}
		s, b, x, bLen := diffHistory(t, rand.New(rand.NewSource(seed)))
		// The dump ended after the backup's newest transaction and before
		// the next one.
		at := float64(bLen)
		s.backup(b, "a", 101, at-0.5)
		var out []string
		for _, collide := range []bool{false, true} {
			for _, q := range []struct {
				mode    Mode
				stop    float64
				exclude string
				partial bool
			}{
				{ModeLatest, 0, x, false}, {ModeLatest, 0, "", false},
				{ModeTimestamp, at + 1.5, x, false}, {ModeTimestamp, at + 3.5, x, false}, {ModeTimestamp, at + 3.5, "", false},
				{ModeTimestamp, at + 6.5, x, false}, {ModeTimestamp, 40, x, true},
			} {
				if collide {
					searchHashOverride = func(string, int64) uint64 { return 7 }
				}
				p, err := s.run(q.mode, q.stop, q.exclude, q.partial)
				searchHashOverride = nil
				if !spill && !collide {
					if err == nil {
						planned++
					} else {
						refused++
						r, ok := RefusalOf(err)
						if !ok {
							t.Fatalf("seed %d: %v", seed, err)
						}
						tokens[r.Token]++
					}
				}
				out = append(out, coverageOf(t, p, err))
			}
		}
		return out
	}
	for seed := int64(1); seed <= 400; seed++ {
		heap, spilled := runAll(seed, false), runAll(seed, true)
		for i := range heap {
			if heap[i] != spilled[i] {
				t.Fatalf("seed %d, query %d: the spilled plan differs from the in-memory plan\nin memory:\n%s\nspilled:\n%s", seed, i, heap[i], spilled[i])
			}
		}
	}
	t.Logf("%d plans and %d refusals compared: %v", planned, refused, tokens)
	if planned < 200 || refused < 200 || tokens[pitrarchive.TokenArchiveOrderInversion] < 5 || tokens[pitrarchive.TokenArchiveNamespaceReuse] < 5 || tokens[pitrarchive.TokenArchiveDivergentHistory] < 5 {
		t.Errorf("the histories do not exercise the planner: %d plans, %d refusals, %v", planned, refused, tokens)
	}
}

// oracleSuites are the planner's oracle tests, run again by
// TestOraclesSpilled with every array in a file arena. The tests that
// assert a memory refusal or a heap bound are not in it: spilling is what
// they exclude.
var oracleSuites = map[string]func(*testing.T){
	"TestSearchMatchesBruteForce":                          TestSearchMatchesBruteForce,
	"TestSearchCanonicalPlanAndPlateaus":                   TestSearchCanonicalPlanAndPlateaus,
	"TestSearchNoPlanCases":                                TestSearchNoPlanCases,
	"TestSearchReleasesCharges":                            TestSearchReleasesCharges,
	"TestSegmentSplitBoundsGTIDArgument":                   TestSegmentSplitBoundsGTIDArgument,
	"TestGapNamesUnreadableRow":                            TestGapNamesUnreadableRow,
	"TestOrderConsistentAcrossRotation":                    TestOrderConsistentAcrossRotation,
	"TestOrderInversion":                                   TestOrderInversion,
	"TestOrderInversionAcrossRotation":                     TestOrderInversionAcrossRotation,
	"TestOrderInversionThreeCycle":                         TestOrderInversionThreeCycle,
	"TestOrderInversionUnobserved":                         TestOrderInversionUnobserved,
	"TestClosureIsOneStep":                                 TestClosureIsOneStep,
	"TestDWindow":                                          TestDWindow,
	"TestFingerprintMismatch":                              TestFingerprintMismatch,
	"TestLineageChecks":                                    TestLineageChecks,
	"TestQuarantineWaiver":                                 TestQuarantineWaiver,
	"TestEffectMismatch":                                   TestEffectMismatch,
	"TestEffectZeroDigestIsNotCompared":                    TestEffectZeroDigestIsNotCompared,
	"TestEffectClosureCoversDivergentAndQuarantinedRows":   TestEffectClosureCoversDivergentAndQuarantinedRows,
	"TestSegmentStartPosition":                             TestSegmentStartPosition,
	"TestIndexBuilderOffsets":                              TestIndexBuilderOffsets,
	"TestDWindowRemediesCoverUnreadableRows":               TestDWindowRemediesCoverUnreadableRows,
	"TestDWindowUnreadableRows":                            TestDWindowUnreadableRows,
	"TestLineageL2QuarantinedEvidence":                     TestLineageL2QuarantinedEvidence,
	"TestLineageL2UnreadableActiveRow":                     TestLineageL2UnreadableActiveRow,
	"TestFetchForPlanMayStage":                             TestFetchForPlanMayStage,
	"TestFetchTransientErrorIsNotUnreadable":               TestFetchTransientErrorIsNotUnreadable,
	"TestCorruptRowIsUnreadable":                           TestCorruptRowIsUnreadable,
	"TestIngestGuards":                                     TestIngestGuards,
	"TestReissueInDownloadedQuarantinedRow":                TestReissueInDownloadedQuarantinedRow,
	"TestFrontierDescendingSearchStopsEarly":               TestFrontierDescendingSearchStopsEarly,
	"TestFrontierHints":                                    TestFrontierHints,
	"TestFrontierLostObject":                               TestFrontierLostObject,
	"TestFrontierNoArchivedMemberOfB":                      TestFrontierNoArchivedMemberOfB,
	"TestFrontierReuseAndAfterTarget":                      TestFrontierReuseAndAfterTarget,
	"TestFrontierServerClockBound":                         TestFrontierServerClockBound,
	"TestFrontierUnverifiedHintIsNotProof":                 TestFrontierUnverifiedHintIsNotProof,
	"TestLatestAuditNegativeEvidence":                      TestLatestAuditNegativeEvidence,
	"TestLatestExcludedTailKeepsLaterRecord":               TestLatestExcludedTailKeepsLaterRecord,
	"TestLatestHeadFallbacks":                              TestLatestHeadFallbacks,
	"TestLatestHeads":                                      TestLatestHeads,
	"TestLatestLossEvidence":                               TestLatestLossEvidence,
	"TestLatestLossRefusalKeepsFullSet":                    TestLatestLossRefusalKeepsFullSet,
	"TestLatestLostFirstEraOfAWriter":                      TestLatestLostFirstEraOfAWriter,
	"TestLatestOnlyMarkedSites":                            TestLatestOnlyMarkedSites,
	"TestLatestOwnerExclusion":                             TestLatestOwnerExclusion,
	"TestLatestRecordsNeverProveLaterCommitsLost":          TestLatestRecordsNeverProveLaterCommitsLost,
	"TestLossEvidenceHonorsCancellation":                   TestLossEvidenceHonorsCancellation,
	"TestMaximalHeads":                                     TestMaximalHeads,
	"TestWitnessAnchorAcrossRoleChange":                    TestWitnessAnchorAcrossRoleChange,
	"TestWitnessAnchorClockSkewMargin":                     TestWitnessAnchorClockSkewMargin,
	"TestWitnessAnchorEvidenceSources":                     TestWitnessAnchorEvidenceSources,
	"TestWitnessAnchorFence":                               TestWitnessAnchorFence,
	"TestWitnessAnchors":                                   TestWitnessAnchors,
	"TestWitnessAnchorSidecarRestartMidEra":                TestWitnessAnchorSidecarRestartMidEra,
	"TestWitnessAscendingSearch":                           TestWitnessAscendingSearch,
	"TestWitnessCopiesDisagree":                            TestWitnessCopiesDisagree,
	"TestWitnessMarkedAnchorRowRefuses":                    TestWitnessMarkedAnchorRowRefuses,
	"TestWitnessMarkedOccurrenceRefuses":                   TestWitnessMarkedOccurrenceRefuses,
	"TestWitnessOccurrencesEnterClosure":                   TestWitnessOccurrencesEnterClosure,
	"TestWitnessOriginAnchorIncarnation":                   TestWitnessOriginAnchorIncarnation,
	"TestWitnessRecordContainingWIsNotAnAnchor":            TestWitnessRecordContainingWIsNotAnAnchor,
	"TestWitnessTies":                                      TestWitnessTies,
	"TestWitnessUnknownTimeHints":                          TestWitnessUnknownTimeHints,
	"TestBudgetBoundsLiveHeapManyTags (segments, spilled)": TestBudgetBoundsLiveHeapManyTags,
}

// TestOraclesSpilled runs the planner's oracle suites with every array
// spilled, however small, and written back every 16 pages: the search
// against its brute-force reference with forced hash collisions, the
// witness, identity, order, lineage, frontier and latest-mode tests. A
// plan that depended on where an array lives would fail its oracle here.
func TestOraclesSpilled(t *testing.T) {
	forceSpill(t)
	names := make([]string, 0, len(oracleSuites))
	for name := range oracleSuites {
		names = append(names, name)
	}
	slices.Sort(names)
	for _, name := range names {
		t.Run(name, oracleSuites[name])
	}
}

// twoSiteWindow is the shape of TestBudgetBoundsLiveHeap: two sites hold
// the same n transactions, so every position is in a two-file bucket and
// has an exact-key class, and every transaction is shared.
func twoSiteWindow(t *testing.T, n int) (*synth, Archive, time.Time) {
	s := newSynth(t)
	s.file("a", "a1", 101, "", "", run("A", 1, int64(n), 1, 101))
	s.file("b", "b1", 102, "", "", run("A", 1, int64(n), 1, 101))
	s.file("a", "a2", 101, "A:1-"+strconv.Itoa(n), "", run("A", int64(n)+1, int64(n)+10, float64(n)+1, 101))
	s.backup("A:1", "a", 101, 1.5)
	var a Archive
	for _, name := range s.order {
		a.Sites = append(a.Sites, *s.sites[name])
	}
	return s, a, time.UnixMicro(sec(float64(n) + 0.5)).UTC()
}

// reindex builds a copy of src under budget, as an index pass would.
func reindex(src *FileIndex, r RowRef, budget *Budget) (*FileIndex, error) {
	ib := NewIndexBuilder(r.Site+"/"+r.Name, budget)
	ib.SetWriter(src.WriterServerID)
	if err := ib.SetPrevious(src.Previous); err != nil {
		return nil, err
	}
	for i, tx := range src.Txns {
		if err := ib.BeginTxn(src.GTID(i), tx.OCT, tx.Srv, tx.Pos); err != nil {
			return nil, err
		}
		if !tx.Empty() {
			ib.MarkData()
		}
		ib.SetEffect(tx.Effect)
	}
	return ib.Finish(src.Digest, src.Size)
}

// TestLargeWindowSpills: a window whose index and search do not fit the
// memory budget refuses PITRPlanTooLarge without an arena, as it did
// before issue #266, and plans with one. The plan is the one a budget
// large enough to hold everything in memory produces.
func TestLargeWindowSpills(t *testing.T) {
	const n = 40000
	const small = 1 << 20
	s, a, stop := twoSiteWindow(t, n)
	plan := func(limit int64, opts *SpillOptions) (string, *Budget, error) {
		budget := NewBudget(limit)
		if opts != nil {
			budget.SetSpill(*opts)
		}
		// Each row is indexed under the plan's budget, as pitr-download
		// does.
		fetch := FetchFunc(func(_ context.Context, r RowRef) (*FileIndex, error) { return reindex(s.idx[r], r, budget) })
		p, err := Run(context.Background(), Request{Mode: ModeTimestamp, Stop: stop, Dump: s.dump, Budget: budget}, a, fetch)
		return coverageOf(t, p, err), budget, err
	}
	want, big, err := plan(1<<30, nil)
	if err != nil {
		t.Fatal(err)
	}
	if big.Peak() < 4*small {
		t.Fatalf("the window needs only %d bytes: not a test of a %d byte budget", big.Peak(), small)
	}
	_, _, err = plan(small, nil)
	wantToken(t, err, pitrarchive.TokenPlanTooLarge, "raise the Job's memory limit")
	arena := newFileArena(t)
	got, budget, err := plan(small, &SpillOptions{Arena: arena, HeapMax: small / 4, DirtyMax: 64 << 10})
	if err != nil {
		t.Fatalf("with a spill arena: %v", err)
	}
	if got != want {
		t.Fatalf("the spilled plan differs from the in-memory plan\nin memory:\n%s\nspilled:\n%s", want, got)
	}
	if budget.SpillPeak() < 2*small || budget.Peak() > small {
		t.Fatalf("spill peak %d, memory peak %d", budget.SpillPeak(), budget.Peak())
	}
	t.Logf("%d transactions on two sites: in memory %d B; with a %d B budget %d B of memory and %d B spilled", n, big.Peak(), small, budget.Peak(), budget.SpillPeak())
	// An arena without room: the refusal says which limit, and what to
	// raise.
	m := newMemArena()
	m.limit = 256 << 10
	_, _, err = plan(small, &SpillOptions{Arena: m, HeapMax: small / 4, DirtyMax: 64 << 10})
	wantToken(t, err, pitrarchive.TokenPlanTooLarge, "fit neither limit", "the memory budget of 1048576 bytes", "raise the staging volume's size")
	// A failed write-back is an error, never a plan and never a refusal.
	m = newMemArena()
	m.failSync = errors.New("input/output error")
	_, _, err = plan(small, &SpillOptions{Arena: m, HeapMax: small / 4, DirtyMax: 64 << 10})
	if _, refused := RefusalOf(err); err == nil || refused || !strings.Contains(err.Error(), "input/output error") {
		t.Fatalf("a failed write-back gave %v", err)
	}
}

// TestBudgetBoundsLiveHeapSpilled is TestBudgetBoundsLiveHeap with a
// spill arena and a memory budget the window does not fit: at the
// planner's peaks the live heap stays within liveHeapFactor of the memory
// charge, which no longer includes the spilled arrays. So the arrays have
// really left the heap, and the charge still bounds what is on it.
func TestBudgetBoundsLiveHeapSpilled(t *testing.T) {
	if testing.Short() {
		t.Skip("plans 120000 positions")
	}
	const n = 60000
	arena := newFileArena(t)
	if _, heap := arena.(*memArena); heap {
		t.Skip("no file arena on this platform: a heap arena's blocks are live heap")
	}
	const limit = 2 << 20
	src, a, stop := twoSiteWindow(t, n)
	budget := NewBudget(limit)
	budget.SetSpill(SpillOptions{Arena: arena, HeapMax: limit / 4, DirtyMax: 64 << 10})
	// Each row is indexed once under the plan's budget, as pitr-download
	// does.
	indexes := map[RowRef]*FileIndex{}
	fetch := FetchFunc(func(_ context.Context, r RowRef) (*FileIndex, error) {
		if idx := indexes[r]; idx != nil {
			return idx, nil
		}
		idx, err := reindex(src.idx[r], r, budget)
		indexes[r] = idx
		return idx, err
	})
	// The source indexes (built by the synth under budgets of their own)
	// are part of the baseline, not of what this budget charged.
	var ms runtime.MemStats
	runtime.GC()
	runtime.ReadMemStats(&ms)
	base := ms.HeapAlloc
	var ratio float64
	var live, charged int64
	memProbe = func() {
		runtime.GC()
		runtime.ReadMemStats(&ms)
		l := int64(ms.HeapAlloc) - int64(base)
		if r := float64(l) / float64(budget.Used()); r > ratio {
			ratio, live, charged = r, l, budget.Used()
		}
	}
	defer func() { memProbe = nil }()
	p, err := Run(context.Background(), Request{Mode: ModeTimestamp, Stop: stop, Dump: src.dump, Budget: budget}, a, fetch)
	if err != nil {
		t.Fatal(err)
	}
	if p.Witness == nil || p.Witness.GTID.String() != "A:"+strconv.Itoa(n+1) {
		t.Fatalf("plan %+v", p)
	}
	t.Logf("live heap %d B, charged %d B: %.2f× (memory peak %d B of %d, spill peak %d B)", live, charged, ratio, budget.Peak(), limit, budget.SpillPeak())
	if ratio > liveHeapFactor {
		t.Fatalf("the planner holds %.2f× what it charged (live %d B, charged %d B), more than the documented %.1f×", ratio, live, charged, liveHeapFactor)
	}
	if budget.SpillPeak() < 4*limit {
		t.Fatalf("only %d bytes spilled", budget.SpillPeak())
	}
	runtime.KeepAlive(src)
}
