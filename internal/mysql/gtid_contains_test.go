package mysql

import (
	"math/rand"
	"sort"
	"testing"
	"time"
)

// referenceIntervalsContain is the previous per-interval coverage walk,
// kept as the oracle for Contains.
func referenceIntervalsContain(intervals []Interval, target Interval) bool {
	sorted := append([]Interval(nil), intervals...)
	sort.Slice(sorted, func(i, j int) bool { return sorted[i].Start < sorted[j].Start })
	pos := target.Start
	for _, iv := range sorted {
		if iv.Start > pos {
			return false
		}
		if iv.End >= pos {
			pos = iv.End + 1
		}
		if pos > target.End {
			return true
		}
	}
	return pos > target.End
}

func referenceContains(g, other GTIDSet) bool {
	for uuid, ois := range other {
		mine, ok := g[uuid]
		if !ok {
			return false
		}
		for _, oi := range ois {
			if !referenceIntervalsContain(mine, oi) {
				return false
			}
		}
	}
	return true
}

func randomIntervals(r *rand.Rand, n int) []Interval {
	out := make([]Interval, n)
	for i := range out {
		s := int64(r.Intn(60) + 1)
		out[i] = Interval{Start: s, End: s + int64(r.Intn(6))}
	}
	return out
}

// K063: Contains agrees with the previous coverage walk on random,
// unmerged, overlapping and adjacent interval lists.
func TestGTIDSetContains_MatchesReference(t *testing.T) {
	r := rand.New(rand.NewSource(63))
	const u1, u2 = "3e11fa47-71ca-11e1-9e33-c80aa9429562", "4e11fa47-71ca-11e1-9e33-c80aa9429562"
	for i := 0; i < 5000; i++ {
		g := GTIDSet{u1: randomIntervals(r, r.Intn(8)+1)}
		if r.Intn(2) == 0 {
			g[u2] = randomIntervals(r, r.Intn(4)+1)
		}
		o := GTIDSet{u1: randomIntervals(r, r.Intn(3)+1)}
		if r.Intn(3) == 0 {
			o[u2] = randomIntervals(r, 1)
		}
		if got, want := g.Contains(o), referenceContains(g, o); got != want {
			t.Fatalf("Contains(%v, %v) = %v, reference %v", g, o, got, want)
		}
	}
	// Adjacent intervals cover their union.
	if !(GTIDSet{u1: {{1, 5}, {6, 10}}}).Contains(GTIDSet{u1: {{3, 8}}}) {
		t.Fatal("adjacent intervals did not cover their union")
	}
}

// K063: a fragmented set of thousands of intervals is checked without a
// sort per interval (the previous walk took seconds here).
func TestGTIDSetContains_FragmentedSetIsFast(t *testing.T) {
	const u = "3e11fa47-71ca-11e1-9e33-c80aa9429562"
	ivs := make([]Interval, 5000)
	for i := range ivs {
		ivs[len(ivs)-1-i] = Interval{Start: int64(2*i + 1), End: int64(2*i + 1)}
	}
	g := GTIDSet{u: ivs}
	start := time.Now()
	for i := 0; i < 2; i++ {
		if !g.Contains(g) {
			t.Fatal("a set does not contain itself")
		}
	}
	if d := time.Since(start); d > 500*time.Millisecond {
		t.Fatalf("two Contains of a 5000-interval set took %s", d)
	}
}
