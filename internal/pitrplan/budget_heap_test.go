package pitrplan

import (
	"context"
	"runtime"
	"strconv"
	"testing"
	"time"
)

// liveHeapFactor is the documented bound (D-42, review round 1 P3): the
// live heap the planner's own structures hold never exceeds this multiple
// of what it charged. pitr-download budgets 60% of the container's memory
// and sets the Go soft limit at 90%, so 60% × 1.4 = 84% leaves the GC room
// below the soft limit instead of the kernel's OOM kill.
const liveHeapFactor = 1.4

// TestBudgetBoundsLiveHeap: two sites hold the same window, so every
// position sits in a two-file bucket and the search materializes one exact
// key class per position (Opus r1 #3's shape). The live heap at the
// planner's peaks stays within liveHeapFactor of the charges.
func TestBudgetBoundsLiveHeap(t *testing.T) {
	if testing.Short() {
		t.Skip("allocates a few hundred MB")
	}
	const n = 60000
	s := newSynth(t)
	s.file("a", "a1", 101, "", "", run("A", 1, n, 1, 101))
	s.file("b", "b1", 102, "", "", run("A", 1, n, 1, 101))
	s.file("a", "a2", 101, "A:1-"+strconv.Itoa(n), "", run("A", n+1, n+10, n+1, 101))
	s.backup("A:1", "a", 101, 1.5)
	var a Archive
	for _, name := range s.order {
		a.Sites = append(a.Sites, *s.sites[name])
	}
	fetch := FetchFunc(func(_ context.Context, r RowRef) (*FileIndex, error) { return s.idx[r], nil })
	budget := NewBudget(4 << 30)
	var ms runtime.MemStats
	runtime.GC()
	runtime.ReadMemStats(&ms)
	base := ms.HeapAlloc
	// At each probe: live heap growth against what is charged right then.
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
	stop := time.UnixMicro(sec(float64(n) + 0.5)).UTC()
	p, err := Run(context.Background(), Request{Mode: ModeTimestamp, Stop: stop, Dump: s.dump, Budget: budget}, a, fetch)
	if err != nil {
		t.Fatal(err)
	}
	if p.Witness == nil || p.Witness.GTID.String() != "A:"+strconv.Itoa(n+1) {
		t.Fatalf("plan %+v", p)
	}
	t.Logf("live heap %d B, charged %d B: %.2f× (budget peak %d B)", live, charged, ratio, budget.Peak())
	if ratio > liveHeapFactor {
		t.Fatalf("the planner holds %.2f× what it charged (live %d B, charged %d B), more than the documented %.1f×", ratio, live, charged, liveHeapFactor)
	}
}

// TestIndexBudgetBoundsLiveHeap: the index pass's transaction array grows
// by appending, so it may hold up to twice its length (and both arrays
// during a copy); the charges cover that. With a distinct tag per
// transaction every one also adds a source identifier to the file's sets
// and identifier tables (review F047); with every other GNO it adds an
// interval.
func TestIndexBudgetBoundsLiveHeap(t *testing.T) {
	if testing.Short() {
		t.Skip("allocates tens of MB")
	}
	for _, shape := range []string{"one source", "a tag per transaction", "gaps"} {
		t.Run(shape, func(t *testing.T) { indexHeapRatio(t, shape) })
	}
}

func indexHeapRatio(t *testing.T, shape string) {
	const n = 300000
	budget := NewBudget(4 << 30)
	var ms runtime.MemStats
	runtime.GC()
	runtime.ReadMemStats(&ms)
	base := ms.HeapAlloc
	b := NewIndexBuilder("a/a1", budget)
	if err := b.SetPrevious(Set{}); err != nil {
		t.Fatal(err)
	}
	ratio := 0.0
	measure := func() {
		runtime.GC()
		runtime.ReadMemStats(&ms)
		ratio = max(ratio, float64(int64(ms.HeapAlloc)-int64(base))/float64(budget.Used()))
	}
	for i := int64(1); i <= n; i++ {
		g := GTID{"A", i}
		switch shape {
		case "a tag per transaction":
			g = GTID{"aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa:t" + strconv.FormatInt(i, 10), 1}
		case "gaps":
			g = GTID{"A", 2 * i}
		}
		if err := b.BeginTxn(g, i, 1); err != nil {
			t.Fatal(err)
		}
		if i%2 == 0 {
			b.MarkData() // the others are empty: EmptyGTIDs grows too
		}
		if i%(n/16) == 0 {
			measure()
		}
	}
	idx, err := b.Finish("d", 1)
	if err != nil {
		t.Fatal(err)
	}
	measure()
	runtime.KeepAlive(idx)
	t.Logf("index (%s): live/charged %.2f×", shape, ratio)
	if ratio > liveHeapFactor {
		t.Fatalf("the index holds %.2f× what it charged, more than %.1f×", ratio, liveHeapFactor)
	}
}

// TestBudgetBoundsLiveHeapManyTags: a replay file whose transactions each
// carry a distinct tag. Every predecessor set then has a source identifier
// per transaction, and the segments split on the mysqlbinlog argument
// bound, each keeping its own StartPredecessors (review F047, F055). The
// probes at the end of the search build, the BFS, the order check and the
// segment fill stay within liveHeapFactor of the charges.
func TestBudgetBoundsLiveHeapManyTags(t *testing.T) {
	if testing.Short() {
		t.Skip("allocates tens of MB")
	}
	const n = 6000
	s := newSynth(t)
	var txns []stx
	for i := 1; i <= n; i++ {
		txns = append(txns, stx{g: "A:t" + strconv.Itoa(i) + ":1", oct: sec(float64(i + 1)), srv: 101})
	}
	s.file("a", "a1", 101, "A:1", "", txns, run("A", 2, 2, float64(n+2), 101))
	s.backup("A:1", "a", 101, 1.5)
	var a Archive
	for _, name := range s.order {
		a.Sites = append(a.Sites, *s.sites[name])
	}
	fetch := FetchFunc(func(_ context.Context, r RowRef) (*FileIndex, error) { return s.idx[r], nil })
	budget := NewBudget(4 << 30)
	var ms runtime.MemStats
	runtime.GC()
	runtime.ReadMemStats(&ms)
	base := ms.HeapAlloc
	var ratio float64
	memProbe = func() {
		runtime.GC()
		runtime.ReadMemStats(&ms)
		ratio = max(ratio, float64(int64(ms.HeapAlloc)-int64(base))/float64(budget.Used()))
	}
	defer func() { memProbe = nil }()
	old := maxSegmentGTIDBytes
	maxSegmentGTIDBytes = 16 << 10
	defer func() { maxSegmentGTIDBytes = old }()
	stop := time.UnixMicro(sec(float64(n) + 1.5)).UTC()
	p, err := Run(context.Background(), Request{Mode: ModeTimestamp, Stop: stop, Dump: s.dump, Budget: budget}, a, fetch)
	if err != nil {
		t.Fatal(err)
	}
	if len(p.Segments) < 10 {
		t.Fatalf("%d segments, want the split", len(p.Segments))
	}
	t.Logf("many tags: %d segments, live/charged %.2f× (budget peak %d B)", len(p.Segments), ratio, budget.Peak())
	if ratio > liveHeapFactor {
		t.Fatalf("the planner holds %.2f× what it charged, more than %.1f×", ratio, liveHeapFactor)
	}
}
