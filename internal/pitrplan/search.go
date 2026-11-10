package pitrplan

import (
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"fmt"
	"math"
	"runtime"
	"sort"
	"strings"

	"github.com/shipstream/bloodraven/internal/pitrarchive"
)

// The plan search (§4.6, D-15): a 0-1 BFS over positions (f, i) of the
// search set. A step (f,i)→(f,i+1) exists iff gtid(t_i) ∈ Λ ∪ X and costs
// 0; a jump between positions of different files with equal
// Key = pred ∖ X costs 1. Starts have Key = B, goals Key = G = Λ ∖ X.
// Whenever a plan is at v, the server's gtid_executed is Key(v), so every
// path is a replay plan and a shortest path has the fewest segments.

type searchResult struct {
	files    []*file
	segments []rawSegment
}

type rawSegment struct{ file, start, end int }

// keyHasher is the additive position hash: Σ over GTIDs (u,n) of
// a_u + b_u·n + c_u·n² (mod 2⁶⁴), with a_u, b_u, c_u from SHA-256(u).
// Intervals sum in closed form. Equal sets hash equal; a collision only
// costs one exact compare.
type keyHasher struct {
	coeff map[string][3]uint64
	// force, when set, replaces the hash (tests force collisions).
	force func(sid string, gno int64) uint64
}

func (h *keyHasher) coeffs(sid string) [3]uint64 {
	if c, ok := h.coeff[sid]; ok {
		return c
	}
	sum := sha256.Sum256([]byte(sid))
	c := [3]uint64{binary.LittleEndian.Uint64(sum[0:]), binary.LittleEndian.Uint64(sum[8:]), binary.LittleEndian.Uint64(sum[16:])}
	h.coeff[sid] = c
	return c
}

func (h *keyHasher) term(sid string, n int64) uint64 {
	if h.force != nil {
		return h.force(sid, n)
	}
	c := h.coeffs(sid)
	u := uint64(n)
	return c[0] + c[1]*u + c[2]*u*u
}

// sum1 is Σ_{k=1..m} k and sum2 is Σ_{k=1..m} k², mod 2⁶⁴.
func sum1(m uint64) uint64 {
	x, y := m, m+1
	if x%2 == 0 {
		x /= 2
	} else {
		y /= 2
	}
	return x * y
}

func sum2(m uint64) uint64 {
	x, y, z := m, m+1, 2*m+1
	if x%2 == 0 {
		x /= 2
	} else {
		y /= 2
	}
	switch {
	case x%3 == 0:
		x /= 3
	case y%3 == 0:
		y /= 3
	default:
		z /= 3
	}
	return x * y * z
}

func (h *keyHasher) set(s Set) uint64 {
	var total uint64
	for sid, ivs := range s {
		for _, iv := range ivs {
			if h.force != nil {
				for n := iv.Start; n <= iv.End; n++ {
					total += h.force(sid, n)
				}
				continue
			}
			c := h.coeffs(sid)
			a, b := uint64(iv.Start), uint64(iv.End)
			length := b - a + 1
			s1 := sum1(b) - sum1(a-1)
			s2 := sum2(b) - sum2(a-1)
			total += c[0]*length + c[1]*s1 + c[2]*s2
		}
	}
	return total
}

// searchHashOverride lets tests force hash collisions.
var searchHashOverride func(sid string, gno int64) uint64

type searchGraph struct {
	files  []*file
	off    []int // node offset of each file; off[len(files)] = N
	stepOK [][]bool
	inX    [][]bool
	hash   []uint64
	count  []int64
	class  []int32
	start  []bool
	goal   []bool
	// members[c] lists class c's nodes in node order; only classes with
	// nodes in at least two files are kept.
	members [][]int32
	dist    []int32
}

func (g *searchGraph) fileOf(v int) int {
	return sort.Search(len(g.files), func(k int) bool { return g.off[k+1] > v }) //nolint:gosec
}

// search runs §4.6 for goal G. On failure the result still lists the
// search set, which identity() treats as the used files. The graph dies
// when search returns, and so do its charges (review F060).
func (p *planner) search(goal Set) (*searchResult, error) {
	held := charges{b: p.budget}
	defer held.release()
	P := p.plan.Replay
	var fs []*file
	for _, r := range p.rows {
		f := p.files[r]
		if f == nil || r.State != RowActive || f.marked {
			continue
		}
		if f.idx.GTIDs.Overlaps(P) || f.idx.GTIDs.Overlaps(p.B) {
			fs = append(fs, f)
		}
	}
	res := &searchResult{files: fs}
	if goal.Equal(p.B) {
		res.files = nil
		return res, nil
	}
	g, err := p.buildGraph(fs, goal, &held)
	if err != nil {
		return res, err
	}
	p.bfs(g)
	best := -1
	for v, s := range g.start {
		if s && g.dist[v] >= 0 && (best < 0 || g.dist[v] < g.dist[best]) {
			best = v
		}
	}
	if best < 0 {
		return res, p.noPlan(g, goal)
	}
	// Walk forward: step while the distance holds, else jump to the first
	// (file order, position) node of the class one jump closer.
	v := best
	k := g.fileOf(v)
	segStart := v - g.off[k]
	var segs []rawSegment
	for !g.goal[v] {
		i := v - g.off[k]
		if i < g.files[k].n() && g.stepOK[k][i] && g.dist[v+1] == g.dist[v] {
			v++
			continue
		}
		next := -1
		if c := g.class[v]; c >= 0 && g.members[c] != nil {
			for _, u := range g.members[c] {
				if int(u) != v && g.fileOf(int(u)) != k && g.dist[u] == g.dist[v]-1 {
					next = int(u)
					break
				}
			}
		}
		if next < 0 {
			return res, fmt.Errorf("pitrplan: internal error: no successor at %s position %d (distance %d)", g.files[k].row.Label(), i, g.dist[v])
		}
		segs = append(segs, rawSegment{k, segStart, i})
		v = next
		k = g.fileOf(v)
		segStart = v - g.off[k]
	}
	segs = append(segs, rawSegment{k, segStart, v - g.off[k]})
	// Drop segments that include nothing (X only, or zero length).
	for _, s := range segs {
		include := false
		for i := s.start; i < s.end; i++ {
			if !g.inX[s.file][i] {
				include = true
				break
			}
		}
		if include {
			res.segments = append(res.segments, s)
		}
	}
	res.files = g.files
	return res, nil
}

func (p *planner) buildGraph(fs []*file, goal Set, held *charges) (*searchGraph, error) {
	g := &searchGraph{files: fs, off: make([]int, len(fs)+1)}
	// Besides the per-position arrays: one file's running key at a time
	// (at most exec(f)), the hasher's coefficients per source identifier,
	// and the spellings of B and G (review F047).
	var keyBytes, sources int64
	for k, f := range fs {
		g.off[k+1] = g.off[k] + f.n() + 1
		keyBytes = max(keyBytes, setBytes(f.exec))
		sources += int64(len(f.prev) + len(f.idx.SIDs))
	}
	n := g.off[len(fs)]
	if err := held.charge(int64(n)*nodeBytes+keyBytes+sources*sourceBytes, "plan search positions"); err != nil {
		return nil, err
	}
	h := &keyHasher{coeff: map[string][3]uint64{}, force: searchHashOverride}
	g.hash, g.count = make([]uint64, n), make([]int64, n)
	g.class, g.start, g.goal, g.dist = make([]int32, n), make([]bool, n), make([]bool, n), make([]int32, n)
	allowed := p.lambda.Union(p.X)
	for k, f := range fs {
		key := f.prev.Subtract(p.X)
		hv, cv := h.set(key), key.Count()
		ok, inX := make([]bool, f.n()), make([]bool, f.n())
		for i := 0; i <= f.n(); i++ {
			v := g.off[k] + i
			g.hash[v], g.count[v], g.class[v] = hv, cv, -1
			if i == f.n() {
				break
			}
			gt := f.gtid(i)
			ok[i] = allowed.Has(gt)
			inX[i] = p.X.Has(gt)
			if !inX[i] {
				hv += h.term(gt.SID, gt.GNO)
				cv++
			}
		}
		g.stepOK, g.inX = append(g.stepOK, ok), append(g.inX, inX)
	}
	// Bucket positions by (hash, |Key|); a bucket spanning two files holds
	// candidate jumps, which the exact compare below confirms.
	hB, cB, hG, cG := h.set(p.B), p.B.Count(), h.set(goal), goal.Count()
	needed := make([]bool, n)
	idx := make([]int32, n)
	for v := range idx {
		idx[v] = int32(v) //nolint:gosec // n is budget-bounded
	}
	sort.Slice(idx, func(a, b int) bool {
		x, y := idx[a], idx[b]
		if g.hash[x] != g.hash[y] {
			return g.hash[x] < g.hash[y]
		}
		if g.count[x] != g.count[y] {
			return g.count[x] < g.count[y]
		}
		return x < y
	})
	for a := 0; a < n; {
		b := a + 1
		for b < n && g.hash[idx[b]] == g.hash[idx[a]] && g.count[idx[b]] == g.count[idx[a]] {
			b++
		}
		if b-a > 1 && g.fileOf(int(idx[a])) != g.fileOf(int(idx[b-1])) {
			for _, v := range idx[a:b] {
				needed[v] = true
			}
		}
		a = b
	}
	for v := 0; v < n; v++ {
		if (g.hash[v] == hB && g.count[v] == cB) || (g.hash[v] == hG && g.count[v] == cG) {
			needed[v] = true
		}
	}
	// Exact keys: sweep each file once, materializing Key only at the
	// needed positions; equal keys share one class. A class records the
	// file of its first member and whether a member lies in another file;
	// only such spanning classes keep a member list (a jump needs two
	// files).
	bStr, gStr := p.B.String(), goal.String()
	if err := held.charge(int64(len(bStr)+len(gStr)), "plan search keys"); err != nil {
		return nil, err
	}
	classes := map[string]int32{}
	var classFirst []int32
	var classMulti []bool
	for k, f := range fs {
		cur := f.prev.Subtract(p.X)
		for i := 0; i <= f.n(); i++ {
			v := g.off[k] + i
			if needed[v] {
				s := cur.String()
				switch s {
				case bStr:
					g.start[v] = true
				}
				if s == gStr {
					g.goal[v] = true
				}
				c, ok := classes[s]
				if !ok {
					if err := held.charge(int64(len(s))+classBytes, "plan search keys"); err != nil {
						return nil, err
					}
					c = int32(len(classFirst)) //nolint:gosec // bounded by n
					classes[s] = c
					classFirst = append(classFirst, int32(k)) //nolint:gosec // bounded by the file count
					classMulti = append(classMulti, false)
				} else if classFirst[c] != int32(k) { //nolint:gosec
					classMulti[c] = true
				}
				g.class[v] = c
			}
			if i < f.n() && !g.inX[k][i] {
				cur.add(f.gtid(i))
			}
		}
	}
	g.members = make([][]int32, len(classFirst))
	for v := 0; v < n; v++ {
		if c := g.class[v]; c >= 0 && classMulti[c] {
			g.members[c] = append(g.members[c], int32(v)) //nolint:gosec
		}
	}
	probeMemory()
	// The probe measures the build's peak: keep its locals live.
	runtime.KeepAlive(classes)
	runtime.KeepAlive(classFirst)
	runtime.KeepAlive(classMulti)
	runtime.KeepAlive(needed)
	runtime.KeepAlive(idx)
	return g, nil
}

// bfs computes d(v), the fewest jumps from v to a goal, by a 0-1 BFS over
// the reversed edges. Each class is relaxed at most twice: from its first
// finalized node (for the other files' members), and from the first
// finalized node of another file (for the first node's file).
func (p *planner) bfs(g *searchGraph) {
	n := len(g.dist)
	for v := range g.dist {
		g.dist[v] = -1
	}
	done := make([]bool, n)
	type classState struct {
		pass      uint8
		firstFile int32
	}
	state := make([]classState, len(g.members))
	dq := newDeque(n)
	for v := 0; v < n; v++ {
		if g.goal[v] {
			g.dist[v] = 0
			dq.pushBack(int32(v)) //nolint:gosec
		}
	}
	relax := func(u int, d int32, front bool) {
		if g.dist[u] < 0 || g.dist[u] > d {
			g.dist[u] = d
			if front {
				dq.pushFront(int32(u)) //nolint:gosec
			} else {
				dq.pushBack(int32(u)) //nolint:gosec
			}
		}
	}
	for !dq.empty() {
		v := int(dq.popFront())
		if done[v] {
			continue
		}
		done[v] = true
		k := g.fileOf(v)
		if i := v - g.off[k]; i > 0 && g.stepOK[k][i-1] {
			relax(v-1, g.dist[v], true)
		}
		c := g.class[v]
		if c < 0 || g.members[c] == nil {
			continue
		}
		st := &state[c]
		switch {
		case st.pass == 0:
			st.pass, st.firstFile = 1, int32(k) //nolint:gosec // bounded by the file count
			for _, u := range g.members[c] {
				if g.fileOf(int(u)) != k {
					relax(int(u), g.dist[v]+1, false)
				}
			}
		case st.pass == 1 && int32(k) != st.firstFile: //nolint:gosec
			st.pass = 2
			for _, u := range g.members[c] {
				if g.fileOf(int(u)) == int(st.firstFile) {
					relax(int(u), g.dist[v]+1, false)
				}
			}
		}
	}
	probeMemory()
	runtime.KeepAlive(done)
	runtime.KeepAlive(state)
	runtime.KeepAlive(dq)
}

type deque struct {
	buf        []int32
	head, size int
}

func newDeque(capHint int) *deque { return &deque{buf: make([]int32, max(16, capHint))} }

func (d *deque) empty() bool { return d.size == 0 }

func (d *deque) grow() {
	if d.size < len(d.buf) {
		return
	}
	nb := make([]int32, 2*len(d.buf))
	for i := 0; i < d.size; i++ {
		nb[i] = d.buf[(d.head+i)%len(d.buf)]
	}
	d.buf, d.head = nb, 0
}

func (d *deque) pushBack(v int32) {
	d.grow()
	d.buf[(d.head+d.size)%len(d.buf)] = v
	d.size++
}

func (d *deque) pushFront(v int32) {
	d.grow()
	d.head = (d.head - 1 + len(d.buf)) % len(d.buf)
	d.buf[d.head] = v
	d.size++
}

func (d *deque) popFront() int32 {
	v := d.buf[d.head]
	d.head = (d.head + 1) % len(d.buf)
	d.size--
	return v
}

// noPlan explains why no plan exists (§4.6 "No plan"); the first matching
// case wins.
func (p *planner) noPlan(g *searchGraph, goal Set) error {
	P := p.plan.Replay
	have := Set{}
	for _, f := range g.files {
		have = have.Union(f.idx.GTIDs)
	}
	missing := P.Subtract(have)
	for _, s := range p.sites {
		if p.isMarked(s) {
			return p.unsafe(s, "no plan exists from unmarked sites' binlogs, and a plan may need "+s+"'s binlogs")
		}
	}
	anyStart := false
	for _, s := range g.start {
		anyStart = anyStart || s
	}
	if !anyStart {
		return refuse(pitrarchive.TokenArchiveGap, "no archived position has exactly the backup's GTID set %s; the backup is newer than every archived position of its history, or its server committed in an order no archived binlog has",
			p.B.bounded(1024))
	}
	reach, lastOCT, furthest := p.forwardReach(g)
	_ = reach
	if !missing.IsEmpty() {
		return p.gapRefusal(missing, lastOCT)
	}
	key := Set{}
	if furthest >= 0 {
		k := g.fileOf(furthest)
		f := g.files[k]
		key = f.prev.Subtract(p.X)
		for i := 0; i < furthest-g.off[k]; i++ {
			if !g.inX[k][i] {
				key.add(f.gtid(i))
			}
		}
	}
	return refuse(pitrarchive.TokenArchiveOrderConflict, "every transaction of the target history is archived, but no archived binlog continues from %s toward %s; this needs commit orders no server produced",
		key.bounded(1024), goal.Subtract(key).bounded(512))
}

// forwardReach marks the positions reachable from a start, the newest
// commit time applied on the way, and the reachable node with the largest
// key.
func (p *planner) forwardReach(g *searchGraph) ([]bool, int64, int) {
	n := len(g.dist)
	reach := make([]bool, n)
	var queue []int32
	for v, s := range g.start {
		if s {
			reach[v] = true
			queue = append(queue, int32(v)) //nolint:gosec
		}
	}
	seenClass := make([]bool, len(g.members))
	var lastOCT int64 = math.MinInt64
	furthest := -1
	for len(queue) > 0 {
		v := int(queue[0])
		queue = queue[1:]
		if furthest < 0 || g.count[v] > g.count[furthest] {
			furthest = v
		}
		k := g.fileOf(v)
		if i := v - g.off[k]; i < g.files[k].n() && g.stepOK[k][i] {
			if !g.inX[k][i] {
				lastOCT = max(lastOCT, g.files[k].txn(i).OCT)
			}
			if !reach[v+1] {
				reach[v+1] = true
				queue = append(queue, int32(v+1)) //nolint:gosec
			}
		}
		if c := g.class[v]; c >= 0 && g.members[c] != nil && !seenClass[c] {
			seenClass[c] = true
			for _, u := range g.members[c] {
				if !reach[u] {
					reach[u] = true
					queue = append(queue, u)
				}
			}
		}
	}
	return reach, lastOCT, furthest
}

// gapRefusal is PITRArchiveGap with the §4.11 hints.
func (p *planner) gapRefusal(missing Set, lastOCT int64) error {
	var b strings.Builder
	fmt.Fprintf(&b, "the target history needs %s, which no archived file holds", missing.bounded(1024))
	for _, r := range p.rows {
		if r.State == RowQuarantined && r.Hints.Known && r.Hints.GTIDs.Overlaps(missing) {
			fmt.Fprintf(&b, "; it is in quarantined row %s (id %s); un-quarantine it, or use a backup taken after it", r.Label(), r.ID)
			return refuse(pitrarchive.TokenArchiveGap, "%s", b.String())
		}
	}
	// An archived row whose object cannot be read is not a never-archived
	// tail (review F061).
	for _, r := range p.rows {
		if err := p.failed[r]; err != nil && r.State == RowActive && r.Hints.mayMeet(missing) {
			fmt.Fprintf(&b, "; it may be in %s (id %s), whose object cannot be read (%v); %s, or use a backup taken after it", r.Label(), r.ID, errors.Unwrap(err), lostObjectAdvice)
			return refuse(pitrarchive.TokenArchiveGap, "%s", b.String())
		}
	}
	var holder *file
	var firstNeed int64 = math.MaxInt64
	for _, f := range p.order {
		if f.prev.Overlaps(missing) && f.n() > 0 {
			firstNeed = min(firstNeed, f.txn(0).OCT)
			if holder == nil && f.prev.Contains(missing) {
				holder = f
			}
		}
	}
	if holder != nil {
		var sites []string
		seen := map[string]bool{}
		for uuid := range missing.UUIDs() {
			s := p.siteOfUUID(uuid)
			if !seen[s] {
				seen[s] = true
				sites = append(sites, s)
			}
		}
		sort.Strings(sites)
		fmt.Fprintf(&b, "; it was committed on %s and never archived (an unarchived tail: its writer died, or was recloned before its sidecar archived it); it is in the base of %s", strings.Join(sites, ", "), holder.row.Label())
		if lastOCT != math.MinInt64 {
			fmt.Fprintf(&b, "; choose a target at or before %s", fmtMicros(lastOCT))
		}
		if firstNeed != math.MaxInt64 {
			fmt.Fprintf(&b, ", or use a backup taken after %s", fmtMicros(firstNeed))
		}
	}
	return refuse(pitrarchive.TokenArchiveGap, "%s", b.String())
}
