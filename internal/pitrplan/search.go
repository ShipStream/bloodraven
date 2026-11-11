package pitrplan

import (
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"fmt"
	"math"
	"sort"
	"strings"
	"unsafe"

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

// posRec is one search position: the additive hash and the size of its
// key, and the exact-key bucket it belongs to.
type posRec struct {
	hash  uint64
	count int64
	// v is the position's node number.
	v int32
	// lead is the first position (in node order) of v's (hash, count)
	// bucket when the bucket needs exact keys, else -1. Every position of
	// such a bucket has the same lead, and equal keys share a bucket.
	lead int32
}

// classRec is one exact-key class.
type classRec struct {
	// The key's spelling in the key store.
	chunk, off, n uint32
	// next is the next class of the same bucket (another key with the
	// same hash and size: a collision), or -1.
	next int32
	// first is the file of the class's first member; multi says a member
	// lies in another file.
	first int32
	multi bool
}

// classState is a class's BFS state.
type classState struct {
	pass      uint8
	firstFile int32
}

// keyStore holds the spellings of the exact keys, appended to chunks that
// never move.
type keyStore struct {
	b      *Budget
	chunks []arr[byte]
	used   int
}

const (
	keyChunkMin = 4 << 10
	keyChunkMax = 1 << 20
	// keyChunkBytes is charged per chunk for its entry in the store.
	keyChunkBytes = 64
)

// put appends s and returns its place.
func (k *keyStore) put(s string) (chunk, off uint32, err error) {
	if int64(len(s)) > math.MaxUint32 {
		return 0, 0, refuse(pitrarchive.TokenPlanTooLarge, "a plan search key is %d bytes long; the planner's limit is %d", len(s), uint32(math.MaxUint32))
	}
	if last := len(k.chunks) - 1; last < 0 || k.used+len(s) > len(k.chunks[last].s) {
		size := keyChunkMin
		if last >= 0 {
			size = min(2*len(k.chunks[last].s), keyChunkMax)
		}
		if err := k.b.Charge(keyChunkBytes, "plan search keys"); err != nil {
			return 0, 0, err
		}
		c, err := alloc[byte](k.b, max(size, len(s)), "plan search keys")
		if err != nil {
			k.b.Release(keyChunkBytes)
			return 0, 0, err
		}
		k.chunks = append(k.chunks, c)
		k.used = 0
	}
	last := len(k.chunks) - 1
	at := k.used
	k.chunks[last].copyIn(at, unsafe.Slice(unsafe.StringData(s), len(s)))
	k.used += len(s)
	return uint32(last), uint32(at), nil //nolint:gosec // chunks and offsets are bounded by the store's size
}

// equal reports whether the key stored at (chunk, off) with length n is s.
func (k *keyStore) equal(chunk, off, n uint32, s string) bool {
	return int(n) == len(s) && string(k.chunks[chunk].s[off:off+n]) == s
}

func (k *keyStore) free() {
	for i := range k.chunks {
		k.chunks[i].free()
		k.b.Release(keyChunkBytes)
	}
	k.chunks = nil
}

// searchGraph is the search's state. Every array that has an entry per
// position or per class is allocated through the budget, so it can spill
// (issue #266); free releases them all.
type searchGraph struct {
	b     *Budget
	files []*file
	off   []int // node offset of each file; off[len(files)] = N
	// Per position v = off[k] + i (position i of file k, 0 ≤ i ≤ n_k).
	// stepOK and inX describe transaction i, and are unused at i = n_k.
	pos    arr[posRec]
	stepOK arr[bool]
	inX    arr[bool]
	class  arr[int32]
	start  arr[bool]
	goal   arr[bool]
	dist   arr[int32]
	// Per class. members(c) lists class c's nodes in node order; only
	// classes with nodes in at least two files have any.
	classes vec[classRec]
	keys    keyStore
	memOff  arr[int32]
	mem     arr[int32]
}

func (g *searchGraph) free() {
	g.pos.free()
	g.stepOK.free()
	g.inX.free()
	g.class.free()
	g.start.free()
	g.goal.free()
	g.dist.free()
	g.classes.free()
	g.keys.free()
	g.memOff.free()
	g.mem.free()
}

func (g *searchGraph) fileOf(v int) int {
	return sort.Search(len(g.files), func(k int) bool { return g.off[k+1] > v }) //nolint:gosec
}

// members are the nodes of class c, when it spans two files.
func (g *searchGraph) members(c int32) []int32 {
	return g.mem.s[g.memOff.s[c]:g.memOff.s[c+1]]
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
	g := &searchGraph{b: p.budget, files: fs, off: make([]int, len(fs)+1), classes: newVec[classRec](p.budget), keys: keyStore{b: p.budget}}
	defer g.free()
	if err := p.buildGraph(g, goal, &held); err != nil {
		return res, err
	}
	if err := p.bfs(g); err != nil {
		return res, err
	}
	best := -1
	for v, s := range g.start.s {
		if s && g.dist.s[v] >= 0 && (best < 0 || g.dist.s[v] < g.dist.s[best]) {
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
	for !g.goal.s[v] {
		i := v - g.off[k]
		if i < g.files[k].n() && g.stepOK.s[v] && g.dist.s[v+1] == g.dist.s[v] {
			v++
			continue
		}
		next := -1
		if c := g.class.s[v]; c >= 0 {
			for _, u := range g.members(c) {
				if int(u) != v && g.fileOf(int(u)) != k && g.dist.s[u] == g.dist.s[v]-1 {
					next = int(u)
					break
				}
			}
		}
		if next < 0 {
			return res, fmt.Errorf("pitrplan: internal error: no successor at %s position %d (distance %d)", g.files[k].row.Label(), i, g.dist.s[v])
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
			if !g.inX.s[g.off[s.file]+i] {
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

// cmpBucket orders positions by (hash, |Key|, node): the positions of a
// bucket are adjacent, in node order. cmpNode restores node order. Both
// are strict total orders: node numbers are distinct.
func cmpBucket(x, y posRec) int {
	switch {
	case x.hash != y.hash:
		if x.hash < y.hash {
			return -1
		}
		return 1
	case x.count != y.count:
		if x.count < y.count {
			return -1
		}
		return 1
	}
	return int(x.v) - int(y.v)
}

func cmpNode(x, y posRec) int { return int(x.v) - int(y.v) }

func (p *planner) buildGraph(g *searchGraph, goal Set, held *charges) error {
	fs := g.files
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
	// Nodes are numbered in int32.
	if n > math.MaxInt32 {
		return refuse(pitrarchive.TokenPlanTooLarge, "the plan search covers %d positions (one per transaction of every binlog file that holds a transaction of the backup or of the replay), and the planner's limit is %d; choose a backup closer to the target", n, math.MaxInt32)
	}
	if err := held.charge(keyBytes+sources*sourceBytes, "plan search positions"); err != nil {
		return err
	}
	var err error
	if g.pos, err = alloc[posRec](p.budget, n, "plan search positions"); err != nil {
		return err
	}
	if g.stepOK, err = alloc[bool](p.budget, n, "plan search positions"); err != nil {
		return err
	}
	if g.inX, err = alloc[bool](p.budget, n, "plan search positions"); err != nil {
		return err
	}
	if g.class, err = alloc[int32](p.budget, n, "plan search positions"); err != nil {
		return err
	}
	if g.start, err = alloc[bool](p.budget, n, "plan search positions"); err != nil {
		return err
	}
	if g.goal, err = alloc[bool](p.budget, n, "plan search positions"); err != nil {
		return err
	}
	if g.dist, err = alloc[int32](p.budget, n, "plan search positions"); err != nil {
		return err
	}
	g.class.fill(0, n, -1)
	h := &keyHasher{coeff: map[string][3]uint64{}, force: searchHashOverride}
	allowed := p.lambda.Union(p.X)
	for k, f := range fs {
		key := f.prev.Subtract(p.X)
		hv, cv := h.set(key), key.Count()
		for i := 0; i <= f.n(); i++ {
			v := g.off[k] + i
			g.pos.set(v, posRec{hash: hv, count: cv, v: int32(v), lead: -1}) //nolint:gosec // n fits int32
			if i == f.n() {
				break
			}
			gt := f.gtid(i)
			if allowed.Has(gt) {
				g.stepOK.set(v, true)
			}
			if p.X.Has(gt) {
				g.inX.set(v, true)
			} else {
				hv += h.term(gt.SID, gt.GNO)
				cv++
			}
		}
	}
	// Bucket positions by (hash, |Key|); a bucket spanning two files holds
	// candidate jumps, and the buckets of B and of G hold the candidate
	// starts and goals. The exact compare below confirms them. Positions
	// are sorted into buckets and back into node order, so that both the
	// bucketing and the sweep read the array in order: a spilled array is
	// then read from disk in order too.
	hB, cB, hG, cG := h.set(p.B), p.B.Count(), h.set(goal), goal.Count()
	g.pos.sortFunc(cmpBucket)
	leaders := 0
	for a := 0; a < n; {
		first := g.pos.s[a]
		b := a + 1
		for b < n && g.pos.s[b].hash == first.hash && g.pos.s[b].count == first.count {
			b++
		}
		spans := b-a > 1 && g.fileOf(int(first.v)) != g.fileOf(int(g.pos.s[b-1].v))
		if spans || (first.hash == hB && first.count == cB) || (first.hash == hG && first.count == cG) {
			leaders++
			for c := a; c < b; c++ {
				rec := g.pos.s[c]
				rec.lead = first.v
				g.pos.set(c, rec)
			}
		}
		a = b
	}
	g.pos.sortFunc(cmpNode)
	// Exact keys: sweep each file once, materializing Key only at the
	// positions of those buckets; equal keys share one class. A class
	// records the file of its first member and whether a member lies in
	// another file; only such spanning classes get a member list (a jump
	// needs two files). The classes of a bucket are chained from the class
	// of its first position, so a key is compared only with the keys that
	// share its hash and size: one, unless hashes collide.
	bStr, gStr := p.B.String(), goal.String()
	if err := held.charge(int64(len(bStr)+len(gStr)), "plan search keys"); err != nil {
		return err
	}
	if err := g.classes.reserve(leaders, "plan search keys"); err != nil {
		return err
	}
	for k, f := range fs {
		cur := f.prev.Subtract(p.X)
		for i := 0; i <= f.n(); i++ {
			v := g.off[k] + i
			if lead := g.pos.s[v].lead; lead >= 0 {
				s := cur.String()
				if s == bStr {
					g.start.set(v, true)
				}
				if s == gStr {
					g.goal.set(v, true)
				}
				c, err := g.classOf(s, int32(v), lead, int32(k)) //nolint:gosec // bounded by n and the file count
				if err != nil {
					return err
				}
				g.class.set(v, c)
			}
			if i < f.n() && !g.inX.s[v] {
				cur.add(f.gtid(i))
			}
		}
	}
	// Member lists, by counting: memOff[c] is where class c's list starts.
	nc := g.classes.n
	classes := g.classes.items()
	if g.memOff, err = alloc[int32](p.budget, nc+1, "plan search keys"); err != nil {
		return err
	}
	for v := 0; v < n; v++ {
		if c := g.class.s[v]; c >= 0 && classes[c].multi {
			g.memOff.set(int(c)+1, g.memOff.s[c+1]+1)
		}
	}
	for c := 0; c < nc; c++ {
		g.memOff.set(c+1, g.memOff.s[c+1]+g.memOff.s[c])
	}
	if g.mem, err = alloc[int32](p.budget, int(g.memOff.s[nc]), "plan search keys"); err != nil {
		return err
	}
	// Filling advances memOff[c] to the end of c's list, which is where
	// c+1's starts: shifting the offsets up by one class restores them.
	for v := 0; v < n; v++ {
		if c := g.class.s[v]; c >= 0 && classes[c].multi {
			at := g.memOff.s[c]
			g.mem.set(int(at), int32(v)) //nolint:gosec
			g.memOff.set(int(c), at+1)
		}
	}
	for c := nc; c > 0; c-- {
		g.memOff.set(c, g.memOff.s[c-1])
	}
	if nc > 0 {
		g.memOff.set(0, 0)
	}
	probeMemory()
	return nil
}

// classOf returns the class of the key s at position v of file k, whose
// bucket starts at position lead, creating the class when s is new.
func (g *searchGraph) classOf(s string, v, lead, k int32) (int32, error) {
	last := int32(-1)
	if lead != v {
		c := g.class.s[lead]
		if c < 0 {
			return 0, fmt.Errorf("pitrplan: internal error: position %d follows its bucket's first position %d, which has no class", v, lead)
		}
		for ; c >= 0; c = g.classes.items()[c].next {
			rec := g.classes.items()[c]
			if g.keys.equal(rec.chunk, rec.off, rec.n, s) {
				if rec.first != k && !rec.multi {
					rec.multi = true
					g.classes.set(int(c), rec)
				}
				return c, nil
			}
			last = c
		}
	}
	chunk, off, err := g.keys.put(s)
	if err != nil {
		return 0, err
	}
	rec := classRec{chunk: chunk, off: off, n: uint32(len(s)), next: -1, first: k} //nolint:gosec // put bounds the length
	c := int32(g.classes.n)                                                        //nolint:gosec // at most one class per position
	if err := g.classes.push(rec, "plan search keys"); err != nil {
		return 0, err
	}
	if last >= 0 {
		rec := g.classes.items()[last]
		rec.next = c
		g.classes.set(int(last), rec)
	}
	return c, nil
}

// bfs computes d(v), the fewest jumps from v to a goal, by a 0-1 BFS over
// the reversed edges. Each class is relaxed at most twice: from its first
// finalized node (for the other files' members), and from the first
// finalized node of another file (for the first node's file).
func (p *planner) bfs(g *searchGraph) error {
	n := len(g.dist.s)
	g.dist.fill(0, n, -1)
	done, err := alloc[bool](p.budget, n, "plan search positions")
	if err != nil {
		return err
	}
	defer done.free()
	state, err := alloc[classState](p.budget, g.classes.n, "plan search keys")
	if err != nil {
		return err
	}
	defer state.free()
	dq, err := newDeque(p.budget, n)
	if err != nil {
		return err
	}
	defer dq.free()
	for v := 0; v < n; v++ {
		if g.goal.s[v] {
			g.dist.set(v, 0)
			if err := dq.pushBack(int32(v)); err != nil { //nolint:gosec
				return err
			}
		}
	}
	relax := func(u int, d int32, front bool) error {
		if g.dist.s[u] < 0 || g.dist.s[u] > d {
			g.dist.set(u, d)
			if front {
				return dq.pushFront(int32(u)) //nolint:gosec
			}
			return dq.pushBack(int32(u)) //nolint:gosec
		}
		return nil
	}
	for !dq.empty() {
		v := int(dq.popFront())
		if done.s[v] {
			continue
		}
		done.set(v, true)
		k := g.fileOf(v)
		if i := v - g.off[k]; i > 0 && g.stepOK.s[v-1] {
			if err := relax(v-1, g.dist.s[v], true); err != nil {
				return err
			}
		}
		c := g.class.s[v]
		if c < 0 || len(g.members(c)) == 0 {
			continue
		}
		st := state.s[c]
		switch {
		case st.pass == 0:
			state.set(int(c), classState{pass: 1, firstFile: int32(k)}) //nolint:gosec // bounded by the file count
			for _, u := range g.members(c) {
				if g.fileOf(int(u)) != k {
					if err := relax(int(u), g.dist.s[v]+1, false); err != nil {
						return err
					}
				}
			}
		case st.pass == 1 && int32(k) != st.firstFile: //nolint:gosec
			state.set(int(c), classState{pass: 2, firstFile: st.firstFile})
			for _, u := range g.members(c) {
				if g.fileOf(int(u)) == int(st.firstFile) {
					if err := relax(int(u), g.dist.s[v]+1, false); err != nil {
						return err
					}
				}
			}
		}
	}
	probeMemory()
	return nil
}

// deque is a ring buffer of nodes that doubles when it is full.
type deque struct {
	buf        arr[int32]
	head, size int
}

func newDeque(b *Budget, capHint int) (*deque, error) {
	buf, err := alloc[int32](b, max(16, capHint), "plan search queue")
	if err != nil {
		return nil, err
	}
	return &deque{buf: buf}, nil
}

func (d *deque) free() { d.buf.free() }

func (d *deque) empty() bool { return d.size == 0 }

func (d *deque) grow() error {
	if d.size < len(d.buf.s) {
		return nil
	}
	nb, err := alloc[int32](d.buf.b, 2*len(d.buf.s), "plan search queue")
	if err != nil {
		return err
	}
	// Unrolled from the head: the two runs of the ring, in order.
	tail := d.buf.s[d.head:]
	nb.copyIn(0, tail)
	nb.copyIn(len(tail), d.buf.s[:d.head])
	d.buf.free()
	d.buf, d.head = nb, 0
	return nil
}

func (d *deque) pushBack(v int32) error {
	if err := d.grow(); err != nil {
		return err
	}
	d.buf.set((d.head+d.size)%len(d.buf.s), v)
	d.size++
	return nil
}

func (d *deque) pushFront(v int32) error {
	if err := d.grow(); err != nil {
		return err
	}
	d.head = (d.head - 1 + len(d.buf.s)) % len(d.buf.s)
	d.buf.set(d.head, v)
	d.size++
	return nil
}

func (d *deque) popFront() int32 {
	v := d.buf.s[d.head]
	d.head = (d.head + 1) % len(d.buf.s)
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
	for _, s := range g.start.s {
		anyStart = anyStart || s
	}
	if !anyStart {
		return refuse(pitrarchive.TokenArchiveGap, "no archived position has exactly the backup's GTID set %s; the backup is newer than every archived position of its history, or its server committed in an order no archived binlog has",
			p.B.bounded(1024))
	}
	lastOCT, furthest, err := p.forwardReach(g)
	if err != nil {
		return err
	}
	if !missing.IsEmpty() {
		return p.gapRefusal(missing, lastOCT)
	}
	key := Set{}
	if furthest >= 0 {
		k := g.fileOf(furthest)
		f := g.files[k]
		key = f.prev.Subtract(p.X)
		for i := 0; i < furthest-g.off[k]; i++ {
			if !g.inX.s[g.off[k]+i] {
				key.add(f.gtid(i))
			}
		}
	}
	return refuse(pitrarchive.TokenArchiveOrderConflict, "every transaction of the target history is archived, but no archived binlog continues from %s toward %s; this needs commit orders no server produced",
		key.bounded(1024), goal.Subtract(key).bounded(512))
}

// forwardReach walks the positions reachable from a start and returns the
// newest commit time applied on the way and the reachable node with the
// largest key.
func (p *planner) forwardReach(g *searchGraph) (int64, int, error) {
	n := len(g.dist.s)
	reach, err := alloc[bool](p.budget, n, "plan search diagnosis")
	if err != nil {
		return 0, 0, err
	}
	defer reach.free()
	// A node enters the queue once, when it is first reached.
	queue, err := alloc[int32](p.budget, n, "plan search diagnosis")
	if err != nil {
		return 0, 0, err
	}
	defer queue.free()
	seenClass, err := alloc[bool](p.budget, g.classes.n, "plan search diagnosis")
	if err != nil {
		return 0, 0, err
	}
	defer seenClass.free()
	head, tail := 0, 0
	for v, s := range g.start.s {
		if s {
			reach.set(v, true)
			queue.set(tail, int32(v)) //nolint:gosec
			tail++
		}
	}
	var lastOCT int64 = math.MinInt64
	furthest := -1
	for head < tail {
		v := int(queue.s[head])
		head++
		if furthest < 0 || g.pos.s[v].count > g.pos.s[furthest].count {
			furthest = v
		}
		k := g.fileOf(v)
		if i := v - g.off[k]; i < g.files[k].n() && g.stepOK.s[v] {
			if !g.inX.s[v] {
				lastOCT = max(lastOCT, g.files[k].txn(i).OCT)
			}
			if !reach.s[v+1] {
				reach.set(v+1, true)
				queue.set(tail, int32(v+1)) //nolint:gosec
				tail++
			}
		}
		if c := g.class.s[v]; c >= 0 && len(g.members(c)) > 0 && !seenClass.s[c] {
			seenClass.set(int(c), true)
			for _, u := range g.members(c) {
				if !reach.s[u] {
					reach.set(int(u), true)
					queue.set(tail, u)
					tail++
				}
			}
		}
	}
	return lastOCT, furthest, nil
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
