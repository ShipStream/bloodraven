package pitrplan

import (
	"fmt"
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
	return refuse(pitrarchive.TokenArchiveNamespaceReuse, "cannot verify %v, which overlaps the target history; restore its object, or (for a lost object) quarantine it and take a full backup after the quarantine", err)
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

// orderNodeBytes and orderEdgeBytes are the order check's charges: per
// node, its map entry, name, adjacency header, Tarjan arrays and stack
// frames; per edge, the edge record.
const (
	orderNodeBytes = 48 + 24 + 24 + 13 + 4 + 16
	orderEdgeBytes = 16
)

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
	charged := int64(0)
	defer func() { p.budget.Release(charged) }()
	charge := func(n int64) error {
		if err := p.budget.Charge(n, "order check"); err != nil {
			return err
		}
		charged += n
		return nil
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
	for g, o := range p.occ {
		if o.n >= 2 {
			shared.add(g)
		}
	}
	// An excluded transaction is never replayed, so how the sites ordered
	// it does not matter (the refusal's own remedy is to exclude it).
	shared = shared.Subtract(p.X)
	if shared.IsEmpty() {
		return nil
	}
	ids := map[GTID]int32{}
	var names []GTID   // real nodes; virtual nodes have a zero GTID
	var labels []*file // the file of a virtual node
	type edge struct {
		to   int32
		file *file
	}
	var adj [][]edge
	node := func(g GTID, f *file) (int32, error) {
		if err := charge(orderNodeBytes); err != nil {
			return 0, err
		}
		id := int32(len(adj)) //nolint:gosec // budget-bounded
		names, labels, adj = append(names, g), append(labels, f), append(adj, nil)
		return id, nil
	}
	link := func(from, to int32, f *file) error {
		if from == to {
			return nil
		}
		if err := charge(orderEdgeBytes); err != nil {
			return err
		}
		adj[from] = append(adj[from], edge{to, f})
		return nil
	}
	real := func(g GTID) (int32, error) {
		if id, ok := ids[g]; ok {
			return id, nil
		}
		id, err := node(g, nil)
		if err != nil {
			return 0, err
		}
		ids[g] = id
		return id, nil
	}
	start := map[*file]int32{}
	end := map[*file]int32{}
	for _, f := range p.order {
		v, err := node(GTID{}, f)
		if err != nil {
			return err
		}
		e, err := node(GTID{}, f)
		if err != nil {
			return err
		}
		start[f], end[f] = v, e
		last := v
		for i := range f.idx.Txns {
			g := f.gtid(i)
			if !shared.Has(g) {
				continue
			}
			id, err := real(g)
			if err != nil {
				return err
			}
			if err := link(last, id, f); err != nil {
				return err
			}
			last = id
		}
		if err := link(last, e, f); err != nil {
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
				if err := link(end[fs[k-1]], start[f], f); err != nil {
					return err
				}
				need = need.Subtract(fs[k-1].exec)
			}
			for sid, ivs := range need {
				for _, iv := range ivs {
					for n := iv.Start; n <= iv.End; n++ {
						id, err := real(GTID{sid, n})
						if err != nil {
							return err
						}
						if err := link(id, start[f], f); err != nil {
							return err
						}
					}
				}
			}
		}
	}
	n := len(adj)
	// Iterative Tarjan.
	index := make([]int32, n)
	low := make([]int32, n)
	onStack := make([]bool, n)
	defer probeMemory()
	for i := range index {
		index[i] = -1
	}
	var stack []int32
	var counter int32
	comp := make([]int32, n)
	type frame struct {
		v  int32
		ei int
	}
	var cycleComp int32 = -1
	ncomp := int32(0)
	for root := 0; root < n && cycleComp < 0; root++ {
		if index[root] >= 0 {
			continue
		}
		call := []frame{{int32(root), 0}} //nolint:gosec
		index[root], low[root] = counter, counter
		counter++
		stack = append(stack, int32(root)) //nolint:gosec
		onStack[root] = true
		for len(call) > 0 {
			top := &call[len(call)-1]
			v := top.v
			if top.ei < len(adj[v]) {
				w := adj[v][top.ei].to
				top.ei++
				if index[w] < 0 {
					index[w], low[w] = counter, counter
					counter++
					stack = append(stack, w)
					onStack[w] = true
					call = append(call, frame{w, 0})
				} else if onStack[w] {
					low[v] = min(low[v], index[w])
				}
				continue
			}
			call = call[:len(call)-1]
			if len(call) > 0 {
				u := call[len(call)-1].v
				low[u] = min(low[u], low[v])
			}
			if low[v] == index[v] {
				size := 0
				for {
					w := stack[len(stack)-1]
					stack = stack[:len(stack)-1]
					onStack[w] = false
					comp[w] = ncomp
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
	isReal := func(v int32) bool { return labels[v] == nil }
	// Name the inversion: a 2-cycle between transactions when one exists,
	// else a cycle path.
	for v := 0; v < n; v++ {
		if comp[v] != cycleComp || !isReal(int32(v)) { //nolint:gosec
			continue
		}
		for _, e := range adj[v] {
			if !isReal(e.to) {
				continue
			}
			for _, back := range adj[e.to] {
				if int(back.to) == v {
					return p.inversion(names[v], names[e.to], e.file, back.file, "")
				}
			}
		}
	}
	for v := 0; v < n; v++ {
		if comp[v] != cycleComp || !isReal(int32(v)) { //nolint:gosec
			continue
		}
		// Follow in-component edges until a node repeats.
		seen := map[int32]int{}
		path := []int32{int32(v)} //nolint:gosec
		var files []*file
		cur := int32(v) //nolint:gosec
		for {
			seen[cur] = len(path) - 1
			var next *edge
			for i := range adj[cur] {
				if comp[adj[cur][i].to] == cycleComp {
					next = &adj[cur][i]
					break
				}
			}
			if next == nil {
				break
			}
			files = append(files, next.file)
			if at, ok := seen[next.to]; ok {
				cyc := append(append([]int32(nil), path[at:]...), next.to)
				cf := files[at:]
				// Rotate to start at a transaction.
				r := 0
				for r < len(cyc)-1 && !isReal(cyc[r]) {
					r++
				}
				if r == len(cyc)-1 {
					break
				}
				cyc = append(append([]int32(nil), cyc[r:len(cyc)-1]...), cyc[:r+1]...)
				cf = append(append([]*file(nil), cf[r:]...), cf[:r]...)
				// Hops between consecutive transactions, each named by the
				// file whose bytes prove it; consecutive hops of one file
				// merge.
				type hop struct {
					from, to GTID
					file     *file
					viaPrev  bool
				}
				var hops []hop
				from, viaPrev := names[cyc[0]], false
				for i := 1; i < len(cyc); i++ {
					if !isReal(cyc[i]) {
						if v := cyc[i]; start[labels[v]] == v {
							viaPrev = true
						}
						continue
					}
					h := hop{from, names[cyc[i]], cf[i-1], viaPrev}
					if k := len(hops) - 1; k >= 0 && hops[k].file == h.file && hops[k].viaPrev == h.viaPrev {
						hops[k].to = h.to
					} else {
						hops = append(hops, h)
					}
					from, viaPrev = names[cyc[i]], false
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
			path = append(path, next.to)
			cur = next.to
		}
	}
	return refuse(pitrarchive.TokenArchiveOrderInversion, "downloaded binlogs order shared transactions inconsistently")
}

func (p *planner) inversion(g1, g2 GTID, f1, f2 *file, via string) error {
	return refuse(pitrarchive.TokenArchiveOrderInversion, "%s and %s are ordered %s,%s in %s and %s,%s in %s%s; the sites applied the same transactions in different orders, so equal GTID sets may hold different data (an errant transaction?); exclude the errant transaction, or use a backup and archive prefix that start after it",
		g1, g2, g1, g2, f1.row.Label(), g2, g1, f2.row.Label(), via)
}
