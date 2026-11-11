package pitrplan

import (
	"fmt"
	"math"
	"sort"
	"strings"

	"github.com/shipstream/bloodraven/internal/pitrarchive"
)

// identity is the one-step closure and the divergent/quarantined compare
// (§4.9, D-16, O2-9). It downloads what must be compared; ingest compares
// fingerprints as each file arrives and the caller reports a mismatch.
func (p *planner) identity() error {
	hist := p.B.Union(p.lambda)
	// Divergent and quarantined rows meeting B ∪ Λ ∪ P are always compared.
	for _, r := range p.rows {
		if r.State == RowActive && r.Divergent == nil {
			continue
		}
		if !r.Hints.mayMeet(hist) {
			continue
		}
		if err := p.compareRow(r); err != nil {
			return err
		}
	}
	// One step: every row overlapping, within B ∪ Λ, the GTIDs of the used
	// files. A row fetched only to compare contributes nothing further.
	S := Set{}
	for _, f := range p.order {
		if p.used[f.row] {
			S = S.Union(f.idx.GTIDs.Intersect(hist))
		}
	}
	if S.IsEmpty() {
		return nil
	}
	for _, r := range p.rows {
		if p.files[r] != nil || !r.Hints.mayMeet(S) {
			continue
		}
		if err := p.compareRow(r); err != nil {
			return err
		}
	}
	return nil
}

func (p *planner) compareRow(r *Row) error {
	_, err := p.fetch(r)
	if err == nil || !isUnreadable(err) {
		return err
	}
	if r.State == RowQuarantined && p.waived(r) {
		return nil
	}
	if r.State == RowQuarantined {
		return refuse(pitrarchive.TokenArchiveNamespaceReuse, "cannot verify %v, a quarantined row that overlaps the target history; restore its object (in an archive copy, copy it again from the source archive), or use a full backup taken after the quarantine that holds its transactions", err)
	}
	return refuse(pitrarchive.TokenArchiveNamespaceReuse, "cannot verify %v, which overlaps the target history; %s, then take a full backup after the quarantine", err, lostObjectAdvice)
}

// waived is the quarantine waiver (O2-4): an unreadable quarantined row
// needs no identity download when its GTIDs are all in B, the dump was
// stamped after the quarantine (+δ), and none of them is replayed.
func (p *planner) waived(r *Row) bool {
	if !r.Hints.Known || r.QuarantinedAt.IsZero() {
		return false
	}
	created := p.req.Dump.Stamp.CreatedAt.UnixMicro()
	return p.B.Contains(r.Hints.GTIDs) &&
		created > r.QuarantinedAt.UnixMicro()+p.delta &&
		!r.Hints.GTIDs.Overlaps(p.plan.Replay)
}

// orderNode is a node of the order check's graph: a shared transaction
// (file < 0: its source and GNO), or the start or end node of the file
// planner.order[file].
type orderNode struct {
	gno  int64
	sid  uint32
	file int32
}

// orderEdge is "from precedes to", proven by the bytes of
// planner.order[file].
type orderEdge struct {
	from, to, file int32
}

// orderFrame is one level of the depth-first search: a node and the next
// of its edges to follow.
type orderFrame struct {
	next int64
	v    int32
}

// orderGraph is the order check's graph and its search state. Everything
// with an entry per node or per edge is allocated through the budget, so
// it can spill (issue #266).
type orderGraph struct {
	p     *planner
	nodes vec[orderNode]
	edges vec[orderEdge]
	// The edges of node v, in the order they were added, are
	// to[off[v]:off[v+1]], each proven by via[...].
	off     arr[int64]
	to, via arr[int32]
	// start and end are each file's virtual nodes, by file.ord.
	start, end []int32
}

func (g *orderGraph) free() {
	g.nodes.free()
	g.edges.free()
	g.off.free()
	g.to.free()
	g.via.free()
}

func (g *orderGraph) node(n orderNode) (int32, error) {
	if g.nodes.n >= math.MaxInt32 {
		return 0, refuse(pitrarchive.TokenPlanTooLarge, "the order check covers more than %d shared transactions, the planner's limit; choose a backup closer to the target", math.MaxInt32)
	}
	id := int32(g.nodes.n) //nolint:gosec // checked above
	return id, g.nodes.push(n, "order check")
}

func (g *orderGraph) link(from, to int32, f *file) error {
	if from == to {
		return nil
	}
	return g.edges.push(orderEdge{from: from, to: to, file: int32(f.ord)}, "order check") //nolint:gosec // bounded by the file count
}

// real returns the node of the shared transaction (sid, gno), creating it.
// The node's number is kept in the transaction's registry record.
func (g *orderGraph) real(sid uint32, gno int64) (int32, error) {
	rec, ok := g.p.occ.get(sid, gno)
	if !ok {
		return 0, fmt.Errorf("pitrplan: internal error: shared transaction %s:%d is in no downloaded file", g.p.sidNames[sid], gno)
	}
	if rec.node != 0 {
		return rec.node - 1, nil
	}
	id, err := g.node(orderNode{gno: gno, sid: sid, file: -1})
	if err != nil {
		return 0, err
	}
	rec.node = id + 1
	return id, g.p.occ.put(sid, gno, rec)
}

func (g *orderGraph) isReal(v int32) bool { return g.nodes.items()[v].file < 0 }

// name is the GTID of a real node.
func (g *orderGraph) name(v int32) GTID {
	n := g.nodes.items()[v]
	return GTID{g.p.sidNames[n.sid], n.gno}
}

// fileOf is the file of a virtual node.
func (g *orderGraph) fileOf(v int32) *file { return g.p.order[g.nodes.items()[v].file] }

// adjacency turns the edge list into per-node lists, each in the order
// its edges were added, and releases the list.
func (g *orderGraph) adjacency() error {
	n, edges := g.nodes.n, g.edges.items()
	var err error
	if g.off, err = alloc[int64](g.p.budget, n+1, "order check"); err != nil {
		return err
	}
	if g.to, err = alloc[int32](g.p.budget, len(edges), "order check"); err != nil {
		return err
	}
	if g.via, err = alloc[int32](g.p.budget, len(edges), "order check"); err != nil {
		return err
	}
	for _, e := range edges {
		g.off.set(int(e.from)+1, g.off.s[e.from+1]+1)
	}
	for v := 0; v < n; v++ {
		g.off.set(v+1, g.off.s[v+1]+g.off.s[v])
	}
	// Filling advances off[v] to the end of v's list, which is where
	// v+1's starts: shifting the offsets up by one node restores them.
	for _, e := range edges {
		at := g.off.s[e.from]
		g.to.set(int(at), e.to)
		g.via.set(int(at), e.file)
		g.off.set(int(e.from), at+1)
	}
	for v := n; v > 0; v-- {
		g.off.set(v, g.off.s[v-1])
	}
	g.off.set(0, 0)
	g.edges.free()
	return nil
}

// orderCheck is D-51: the downloaded binlogs must admit one order of their
// shared transactions. Each file contributes what its bytes prove about
// its server's order:
//
//   - its transactions in file order (edges between adjacent shared ones);
//   - prev(f) before every transaction of f (P1): a GTID of prev(f)
//     precedes f's first transaction. This carries order across a binlog
//     rotation, where the second half of an inversion is visible only in a
//     later file's Previous_gtids.
//
// A GTID outside X is shared when two downloaded files hold it as a
// transaction, or one holds it as a transaction and a file of another site
// holds it in prev. Each file has a start node V_f (prev(f) → V_f → its first shared
// transaction) and an end node E_f (its last shared transaction → E_f);
// the files of one site are chained E_f → V_f' when exec(f) ⊆ prev(f'),
// so only prev(f') ∖ exec(f) needs its own edges and the graph stays
// linear in the occurrences. A cycle is an inversion.
func (p *planner) orderCheck() error {
	// The registry of a failed ingest is not read (fetch).
	if p.broken != nil {
		return p.broken
	}
	// The shared set.
	txnsBySite := map[string]Set{}
	for _, f := range p.order {
		txnsBySite[f.row.Site] = txnsBySite[f.row.Site].Union(f.idx.GTIDs)
	}
	shared := Set{}
	for _, f := range p.order {
		for site, txns := range txnsBySite {
			if site != f.row.Site {
				shared = shared.Union(f.prev.Intersect(txns))
			}
		}
	}
	// The registry is read in GNO order, so each source's GTIDs with two
	// copies extend its last interval. A node number left by an earlier
	// check is cleared.
	err := p.occ.each(func(sid uint32, gno int64, rec occRec) error {
		if rec.n >= 2 {
			shared.add(GTID{p.sidNames[sid], gno})
		}
		if rec.node != 0 {
			rec.node = 0
			return p.occ.put(sid, gno, rec)
		}
		return nil
	})
	if err != nil {
		return err
	}
	// An excluded transaction is never replayed, so how the sites ordered
	// it does not matter (the refusal's own remedy is to exclude it).
	shared = shared.Subtract(p.X)
	if shared.IsEmpty() {
		return nil
	}
	g := &orderGraph{p: p, nodes: newVec[orderNode](p.budget), edges: newVec[orderEdge](p.budget),
		start: make([]int32, len(p.order)), end: make([]int32, len(p.order))}
	defer g.free()
	if err := p.budget.Charge(8*int64(len(p.order)), "order check"); err != nil {
		return err
	}
	defer p.budget.Release(8 * int64(len(p.order)))
	for _, f := range p.order {
		v, err := g.node(orderNode{file: int32(f.ord)}) //nolint:gosec // bounded by the file count
		if err != nil {
			return err
		}
		e, err := g.node(orderNode{file: int32(f.ord)}) //nolint:gosec
		if err != nil {
			return err
		}
		g.start[f.ord], g.end[f.ord] = v, e
		last := v
		for i, t := range f.idx.Txns {
			if !shared.Has(f.gtid(i)) {
				continue
			}
			id, err := g.real(f.sids[t.SID], t.GNO)
			if err != nil {
				return err
			}
			if err := g.link(last, id, f); err != nil {
				return err
			}
			last = id
		}
		if err := g.link(last, e, f); err != nil {
			return err
		}
	}
	// prev edges, chained per site in file order.
	bySite := map[string][]*file{}
	for _, f := range p.order {
		bySite[f.row.Site] = append(bySite[f.row.Site], f)
	}
	for _, site := range p.sites {
		fs := bySite[site]
		sort.SliceStable(fs, func(i, j int) bool { return rowLess(fs[i].row, fs[j].row) })
		for k, f := range fs {
			need := f.prev.Intersect(shared)
			if k > 0 && f.prev.Contains(fs[k-1].exec) {
				if err := g.link(g.end[fs[k-1].ord], g.start[f.ord], f); err != nil {
					return err
				}
				need = need.Subtract(fs[k-1].exec)
			}
			// By source identifier, so that the graph, and with it the
			// inversion a refusal names, is the same on every run.
			names := make([]string, 0, len(need))
			for name := range need {
				names = append(names, name)
			}
			sort.Strings(names)
			for _, name := range names {
				sid, ok := p.sidIndex[name]
				if !ok {
					return fmt.Errorf("pitrplan: internal error: shared transactions of %s are in no downloaded file", name)
				}
				for _, iv := range need[name] {
					for n := iv.Start; n <= iv.End; n++ {
						id, err := g.real(sid, n)
						if err != nil {
							return err
						}
						if err := g.link(id, g.start[f.ord], f); err != nil {
							return err
						}
					}
				}
			}
		}
	}
	if err := g.adjacency(); err != nil {
		return err
	}
	n := g.nodes.n
	// Iterative Tarjan. The stack and the call stack hold a node at most
	// once each.
	index, err := alloc[int32](p.budget, n, "order check")
	if err != nil {
		return err
	}
	defer index.free()
	low, err := alloc[int32](p.budget, n, "order check")
	if err != nil {
		return err
	}
	defer low.free()
	onStack, err := alloc[bool](p.budget, n, "order check")
	if err != nil {
		return err
	}
	defer onStack.free()
	comp, err := alloc[int32](p.budget, n, "order check")
	if err != nil {
		return err
	}
	defer comp.free()
	stack, err := alloc[int32](p.budget, n, "order check")
	if err != nil {
		return err
	}
	defer stack.free()
	call, err := alloc[orderFrame](p.budget, n, "order check")
	if err != nil {
		return err
	}
	defer call.free()
	defer probeMemory()
	index.fill(0, n, -1)
	var counter int32
	var cycleComp int32 = -1
	ncomp := int32(0)
	sp := 0
	for root := 0; root < n && cycleComp < 0; root++ {
		if index.s[root] >= 0 {
			continue
		}
		depth := 1
		call.set(0, orderFrame{next: g.off.s[root], v: int32(root)}) //nolint:gosec
		index.set(root, counter)
		low.set(root, counter)
		counter++
		stack.set(sp, int32(root)) //nolint:gosec
		sp++
		onStack.set(root, true)
		for depth > 0 {
			top := call.s[depth-1]
			v := top.v
			if top.next < g.off.s[v+1] {
				w := g.to.s[top.next]
				top.next++
				call.set(depth-1, top)
				if index.s[w] < 0 {
					index.set(int(w), counter)
					low.set(int(w), counter)
					counter++
					stack.set(sp, w)
					sp++
					onStack.set(int(w), true)
					call.set(depth, orderFrame{next: g.off.s[w], v: w})
					depth++
				} else if onStack.s[w] {
					low.set(int(v), min(low.s[v], index.s[w]))
				}
				continue
			}
			depth--
			if depth > 0 {
				u := call.s[depth-1].v
				low.set(int(u), min(low.s[u], low.s[v]))
			}
			if low.s[v] == index.s[v] {
				size := 0
				for {
					sp--
					w := stack.s[sp]
					onStack.set(int(w), false)
					comp.set(int(w), ncomp)
					size++
					if w == v {
						break
					}
				}
				if size > 1 && cycleComp < 0 {
					cycleComp = ncomp
				}
				ncomp++
			}
		}
	}
	if cycleComp < 0 {
		return nil
	}
	// Name the inversion: a 2-cycle between transactions when one exists,
	// else a cycle path.
	for v := int32(0); int(v) < n; v++ {
		if comp.s[v] != cycleComp || !g.isReal(v) {
			continue
		}
		for e := g.off.s[v]; e < g.off.s[v+1]; e++ {
			w := g.to.s[e]
			if !g.isReal(w) {
				continue
			}
			for back := g.off.s[w]; back < g.off.s[w+1]; back++ {
				if g.to.s[back] == v {
					return p.inversion(g.name(v), g.name(w), p.order[g.via.s[e]], p.order[g.via.s[back]])
				}
			}
		}
	}
	// seenAt[v] is 1 + v's place on the path, or 0.
	seenAt, err := alloc[int32](p.budget, n, "order check")
	if err != nil {
		return err
	}
	defer seenAt.free()
	path, files := newVec[int32](p.budget), newVec[int32](p.budget)
	defer path.free()
	defer files.free()
	for v := int32(0); int(v) < n; v++ {
		if comp.s[v] != cycleComp || !g.isReal(v) {
			continue
		}
		// Follow in-component edges until a node repeats.
		for _, u := range path.items() {
			seenAt.set(int(u), 0)
		}
		path.n, files.n = 0, 0
		if err := path.push(v, "order check"); err != nil {
			return err
		}
		cur := v
		for {
			seenAt.set(int(cur), int32(path.n)) //nolint:gosec // a path holds a node once
			next := int64(-1)
			for e := g.off.s[cur]; e < g.off.s[cur+1]; e++ {
				if comp.s[g.to.s[e]] == cycleComp {
					next = e
					break
				}
			}
			if next < 0 {
				break
			}
			if err := files.push(g.via.s[next], "order check"); err != nil {
				return err
			}
			to := g.to.s[next]
			if at := seenAt.s[to]; at != 0 {
				// The cycle is path[at-1:], closed by the edge back to
				// `to`; cyc(i) and cf(i) are its nodes and its edges'
				// files once it is rotated to start at a transaction.
				cycle, cfiles := path.items()[at-1:], files.items()[at-1:]
				length := len(cycle)
				r := 0
				for r < length && !g.isReal(cycle[r]) {
					r++
				}
				if r == length {
					break
				}
				cyc := func(i int) int32 { return cycle[(r+i)%length] }
				cf := func(i int) *file { return p.order[cfiles[(r+i)%length]] }
				// Hops between consecutive transactions, each named by the
				// file whose bytes prove it; consecutive hops of one file
				// merge. Eight are printed, so nine are enough to know
				// whether more follow.
				type hop struct {
					from, to GTID
					file     *file
					viaPrev  bool
				}
				var hops []hop
				from, viaPrev := g.name(cyc(0)), false
				for i := 1; i <= length && len(hops) <= 8; i++ {
					if u := cyc(i); !g.isReal(u) {
						if g.start[g.fileOf(u).ord] == u {
							viaPrev = true
						}
						continue
					}
					h := hop{from, g.name(cyc(i)), cf(i - 1), viaPrev}
					if k := len(hops) - 1; k >= 0 && hops[k].file == h.file && hops[k].viaPrev == h.viaPrev {
						hops[k].to = h.to
					} else {
						hops = append(hops, h)
					}
					from, viaPrev = h.to, false
				}
				var parts []string
				for i, h := range hops {
					if i == 8 {
						parts = append(parts, "…")
						break
					}
					how := h.file.row.Label()
					if h.viaPrev {
						how += ", by its previous GTIDs"
					}
					parts = append(parts, fmt.Sprintf("%s before %s (%s)", h.from, h.to, how))
				}
				return refuse(pitrarchive.TokenArchiveOrderInversion, "the downloaded binlogs order shared transactions in a cycle: %s; the sites applied the same transactions in different orders, so equal GTID sets may hold different data (an errant transaction?); exclude the errant transaction, or use a backup and archive prefix that start after it",
					strings.Join(parts, ", "))
			}
			if err := path.push(to, "order check"); err != nil {
				return err
			}
			cur = to
		}
	}
	return refuse(pitrarchive.TokenArchiveOrderInversion, "downloaded binlogs order shared transactions inconsistently")
}

func (p *planner) inversion(g1, g2 GTID, f1, f2 *file) error {
	return refuse(pitrarchive.TokenArchiveOrderInversion, "%s and %s are ordered %s,%s in %s and %s,%s in %s; the sites applied the same transactions in different orders, so equal GTID sets may hold different data (an errant transaction?); exclude the errant transaction, or use a backup and archive prefix that start after it",
		g1, g2, g1, g2, f1.row.Label(), g2, g1, f2.row.Label())
}
