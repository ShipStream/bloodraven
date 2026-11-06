package pitrplan

import (
	"context"
	"fmt"
	"math/rand"
	"strconv"
	"testing"

	"github.com/shipstream/bloodraven/internal/pitrarchive"
)

// searchPlanner builds a planner over every row of s with B, X and Λ set
// directly, for exercising search() alone.
func searchPlanner(t *testing.T, s *synth, b, x, lambda string) *planner {
	t.Helper()
	var a Archive
	for _, name := range s.order {
		a.Sites = append(a.Sites, *s.sites[name])
	}
	s.backup(b, "a", 101, 0)
	fetch := FetchFunc(func(_ context.Context, r RowRef) (*FileIndex, error) { return s.idx[r], nil })
	p, err := newPlanner(context.Background(), Request{Mode: ModeLatest, Exclude: MustParseSet(x), Dump: s.dump, Budget: NewBudget(1 << 30)}, a, fetch)
	if err != nil {
		t.Fatal(err)
	}
	for _, r := range p.rows {
		if _, err := p.fetch(r); err != nil {
			t.Fatal(err)
		}
	}
	p.lambda = MustParseSet(lambda)
	p.plan.Replay = p.lambda.Subtract(p.B).Subtract(p.X)
	return p
}

// bruteMinJumps is an independent reference: explicit keys, explicit
// pairwise equality, plain BFS over (jumps) layers.
func bruteMinJumps(p *planner, goal Set) int {
	var fs []*file
	for _, r := range p.rows {
		if f := p.files[r]; f != nil && !f.marked && (f.idx.GTIDs.Overlaps(p.plan.Replay) || f.idx.GTIDs.Overlaps(p.B)) {
			fs = append(fs, f)
		}
	}
	type node struct{ f, i int }
	key := map[node]string{}
	var nodes []node
	for k, f := range fs {
		cur := f.prev.Subtract(p.X)
		for i := 0; i <= f.n(); i++ {
			n := node{k, i}
			nodes = append(nodes, n)
			key[n] = cur.String()
			if i < f.n() && !p.X.Has(f.gtid(i)) {
				cur.add(f.gtid(i))
			}
		}
	}
	allowed := p.lambda.Union(p.X)
	// closure under 0-cost steps
	stepClose := func(set map[node]bool) {
		for changed := true; changed; {
			changed = false
			for n := range set {
				if n.i < fs[n.f].n() && allowed.Has(fs[n.f].gtid(n.i)) {
					next := node{n.f, n.i + 1}
					if !set[next] {
						set[next] = true
						changed = true
					}
				}
			}
		}
	}
	reached := map[node]bool{}
	for _, n := range nodes {
		if key[n] == p.B.String() {
			reached[n] = true
		}
	}
	gs := goal.String()
	for jumps := 0; jumps <= len(nodes); jumps++ {
		stepClose(reached)
		for n := range reached {
			if key[n] == gs {
				return jumps
			}
		}
		next := map[node]bool{}
		for n := range reached {
			next[n] = true
			for _, m := range nodes {
				if m.f != n.f && key[m] == key[n] {
					next[m] = true
				}
			}
		}
		if len(next) == len(reached) {
			return -1
		}
		reached = next
	}
	return -1
}

func checkPlanWalk(t *testing.T, p *planner, res *searchResult, goal Set) {
	t.Helper()
	state := p.B.Clone()
	for _, s := range res.segments {
		f := res.files[s.file]
		pred := f.prev.Clone()
		for j := 0; j < s.start; j++ {
			pred.add(f.gtid(j))
		}
		if !pred.Subtract(p.X).Equal(state) {
			t.Fatalf("segment %s@%d starts at %s ∖ X ≠ %s", f.row.Name, s.start, pred, state)
		}
		for j := s.start; j < s.end; j++ {
			if g := f.gtid(j); !p.X.Has(g) {
				if state.Has(g) {
					t.Fatalf("%s applied twice", g)
				}
				state.add(g)
			}
		}
	}
	if !state.Equal(goal) {
		t.Fatalf("final state %s, want %s", state, goal)
	}
}

// randomHistory: two writers alternate eras; replicas copy the source in
// order; each site's sequence is cut into files at random; some files are
// dropped (not archived).
func randomHistory(t *testing.T, r *rand.Rand) (*synth, string, string, string) {
	s := newSynth(t)
	type ev struct {
		g   string
		srv uint32
	}
	var hist []ev
	gnos := map[string]int64{}
	writer := 0
	sids := []string{"A", "B"}
	for len(hist) < 4+r.Intn(8) {
		if r.Intn(4) == 0 {
			writer = 1 - writer
		}
		sid := sids[writer]
		gnos[sid]++
		hist = append(hist, ev{fmt.Sprintf("%s:%d", sid, gnos[sid]), uint32(101 + writer)})
	}
	n := len(hist)
	for k, site := range []string{"a", "b"} {
		cut := 0
		for cut < n {
			end := cut + 1 + r.Intn(n-cut)
			prev := Set{}
			for _, e := range hist[:cut] {
				prev.add(MustParseSet(e.g).GTIDs()[0])
			}
			var txns []stx
			for i, e := range hist[cut:end] {
				txns = append(txns, stx{g: e.g, oct: sec(float64(cut + i)), srv: e.srv})
			}
			if r.Intn(3) != 0 {
				s.file(site, fmt.Sprintf("%s%d", site, cut), uint32(101+k), prev.String(), "", txns)
			}
			cut = end
		}
	}
	bLen := r.Intn(n)
	B, lambda, X := Set{}, Set{}, Set{}
	for i, e := range hist {
		g := MustParseSet(e.g).GTIDs()[0]
		lambda.add(g)
		if i < bLen {
			B.add(g)
		} else if r.Intn(5) == 0 {
			X.add(g)
		}
	}
	return s, B.String(), X.String(), lambda.String()
}

func TestSearchMatchesBruteForce(t *testing.T) {
	r := rand.New(rand.NewSource(206))
	planned, refused, multi := 0, 0, 0
	defer func() {
		t.Logf("%d plans (%d multi-segment), %d refusals", planned, multi, refused)
		if planned < 50 || refused < 20 || multi < 20 {
			t.Errorf("the generator is not exercising the search: %d plans, %d multi-segment, %d refusals", planned, multi, refused)
		}
	}()
	for trial := 0; trial < 400; trial++ {
		s, b, x, lambda := randomHistory(t, r)
		for _, collide := range []bool{false, true} {
			if collide {
				searchHashOverride = func(string, int64) uint64 { return 7 }
			}
			p := searchPlanner(t, s, b, x, lambda)
			goal := p.lambda.Subtract(p.X)
			want := bruteMinJumps(p, goal)
			res, err := p.search(goal)
			searchHashOverride = nil
			if goal.Equal(p.B) {
				if err != nil || len(res.segments) != 0 {
					t.Fatalf("trial %d: G = B must give an empty plan, got %v %v", trial, res, err)
				}
				continue
			}
			if want < 0 {
				refused++
				if err == nil {
					t.Fatalf("trial %d (collide=%v): planner found a plan the reference says does not exist", trial, collide)
				}
				continue
			}
			if err != nil {
				t.Fatalf("trial %d (collide=%v): reference finds %d jumps, planner refuses: %v", trial, collide, want, err)
			}
			// Dropping X-only segments can only shorten the plan below
			// the reference's jump count.
			if len(res.segments) > want+1 {
				t.Fatalf("trial %d: %d segments, reference needs %d jumps", trial, len(res.segments), want)
			}
			checkPlanWalk(t, p, res, goal)
			planned++
			if len(res.segments) > 1 {
				multi++
			}
		}
	}
}

func TestSearchCanonicalPlanAndPlateaus(t *testing.T) {
	// Two copies of the same history: longest-first, smallest-file.
	s := newSynth(t)
	s.file("a", "a1", 101, "", "", run("A", 1, 10, 1, 101))
	s.file("b", "b1", 102, "", "", run("A", 1, 10, 1, 101))
	p := searchPlanner(t, s, "A:1-2", "A:5", "A:1-10")
	res, err := p.search(p.lambda.Subtract(p.X))
	if err != nil || len(res.segments) != 1 || res.files[res.segments[0].file].row.Name != "a1" {
		t.Fatalf("plan %+v %v", res, err)
	}
	// An X-only segment is dropped: b1 holds only an excluded transaction
	// between a1 and a2.
	s = newSynth(t)
	s.file("a", "a1", 101, "", "", run("A", 1, 5, 1, 101))
	s.file("b", "b1", 102, "A:1-5", "", run("A", 6, 6, 6, 101))
	s.file("a", "a2", 101, "A:1-6", "", run("A", 7, 10, 7, 101))
	p = searchPlanner(t, s, "A:1-2", "A:6", "A:1-10")
	res, err = p.search(p.lambda.Subtract(p.X))
	if err != nil {
		t.Fatal(err)
	}
	for _, sg := range res.segments {
		if res.files[sg.file].row.Name == "b1" {
			t.Fatalf("X-only segment kept: %+v", res.segments)
		}
	}
	checkPlanWalk(t, p, res, p.lambda.Subtract(p.X))
}

func TestSearchNoPlanCases(t *testing.T) {
	// No start: the backup's set is no archived position.
	s := newSynth(t)
	s.file("a", "a1", 101, "", "", run("A", 1, 10, 1, 101))
	p := searchPlanner(t, s, "A:1-3,A:5", "", "A:1-10")
	_, err := p.search(p.lambda)
	wantToken(t, err, pitrarchive.TokenArchiveGap, "no archived position")
	// Missing transactions.
	s = newSynth(t)
	s.file("a", "a1", 101, "", "", run("A", 1, 5, 1, 101))
	s.file("a", "a2", 101, "A:1-8", "", run("A", 9, 10, 9, 101))
	p = searchPlanner(t, s, "A:1-2", "", "A:1-10")
	_, err = p.search(p.lambda)
	wantToken(t, err, pitrarchive.TokenArchiveGap, "A:6-8")
	// Everything archived, but no continuation: OrderConflict.
	s = newSynth(t)
	s.file("a", "a1", 101, "", "", run("A", 1, 3, 1, 101))
	s.file("b", "b1", 102, "A:1-2", "", []stx{{g: "B:1", oct: sec(3), srv: 102}})
	s.file("c", "c1", 103, "A:1-2,B:1", "", run("A", 3, 3, 4, 101))
	p = searchPlanner(t, s, "A:1-2", "", "A:1-3,B:1")
	// a1's A:3 lacks B:1, b1 reaches A:1-2,B:1 and c1 adds A:3 — so a plan
	// exists; remove c1 to make the history unreachable.
	if _, err := p.search(p.lambda); err != nil {
		t.Fatal(err)
	}
	s.sites["c"].Rows = nil
	p = searchPlanner(t, s, "A:1-2", "", "A:1-3,B:1")
	_, err = p.search(p.lambda)
	wantToken(t, err, pitrarchive.TokenArchiveOrderConflict)
	// A marked site comes first.
	s.site("z").Marked = &pitrarchive.HistoryIntegrity{Violation: pitrarchive.ViolationBinlogFormat}
	p = searchPlanner(t, s, "A:1-2", "", "A:1-3,B:1")
	_, err = p.search(p.lambda)
	wantToken(t, err, pitrarchive.TokenArchiveUnsafeHistory)
}

func TestKeyHashClosedForm(t *testing.T) {
	h := &keyHasher{coeff: map[string][3]uint64{}}
	s := MustParseSet("A:1-1000:2000-2003,B:7")
	var sum uint64
	for _, g := range s.GTIDs() {
		sum += h.term(g.SID, g.GNO)
	}
	if h.set(s) != sum {
		t.Fatal("closed-form interval hash differs from the per-GTID sum")
	}
	big := MustParseSet("A:1-9223372036854775806")
	_ = h.set(big) // no overflow panic
}

// P7: a segment whose GTID set would exceed one mysqlbinlog argument is
// split into consecutive ranges of the same file; the pieces chain (R4)
// and replay exactly what the unsplit segment would.
func TestSegmentSplitBoundsGTIDArgument(t *testing.T) {
	build := func() *synth {
		s := newSynth(t)
		s.file("a", "a1", 101, "", "", run("A", 1, 200, 1, 101))
		s.backup("A:1-20", "a", 101, 20.5)
		return s
	}
	var excl []string
	for n := 41; n <= 160; n += 2 {
		excl = append(excl, "A:"+strconv.Itoa(n))
	}
	x := MustParseSet(joinComma(excl)).String()
	whole, err := build().run(ModeTimestamp, 180.5, x, false)
	if err != nil || len(whole.Segments) != 1 {
		t.Fatalf("unsplit: %+v %v", whole, err)
	}
	old := maxSegmentGTIDBytes
	maxSegmentGTIDBytes = 300
	defer func() { maxSegmentGTIDBytes = old }()
	p, err := build().run(ModeTimestamp, 180.5, x, false)
	if err != nil {
		t.Fatal(err)
	}
	if len(p.Segments) < 3 {
		t.Fatalf("%d segments, want a split", len(p.Segments))
	}
	applied := p.Baseline
	var count int64
	for k, s := range p.Segments {
		if s.File != 0 || len(s.GTIDs.String()) > 300 || s.Transactions != s.GTIDs.Count() {
			t.Fatalf("segment %d: %+v", k, s)
		}
		if k > 0 && s.Start != p.Segments[k-1].End {
			t.Fatalf("segment %d starts at %d, previous ends at %d", k, s.Start, p.Segments[k-1].End)
		}
		if !s.StartPredecessors.Subtract(p.Excluded).Equal(applied) {
			t.Fatalf("R4: segment %d starts after %s, the server holds %s", k, s.StartPredecessors.Subtract(p.Excluded), applied)
		}
		applied = applied.Union(s.GTIDs)
		count += s.Transactions
	}
	if !applied.Equal(p.Baseline.Union(whole.Replay)) || count != whole.ReplayedTransactions() || !p.OnHistoryExclusions.Equal(whole.OnHistoryExclusions) ||
		p.NewestReplayedCommit() != whole.NewestReplayedCommit() {
		t.Fatalf("split plan replays %s (%d), unsplit %s (%d); on-history %s vs %s", applied, count, whole.Replay, whole.ReplayedTransactions(), p.OnHistoryExclusions, whole.OnHistoryExclusions)
	}
}
