package pitrplan

import (
	"fmt"
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

// orderCheck is D-51: every pair of downloaded occurrence sequences orders
// their common GTIDs identically. Edges join adjacent shared GTIDs of each
// sequence; a cycle is an inversion.
func (p *planner) orderCheck() error {
	ids := map[GTID]int32{}
	var names []GTID
	type edge struct {
		to   int32
		file *file
	}
	var adj [][]edge
	for _, f := range p.order {
		last := int32(-1)
		for i := range f.idx.Txns {
			g := f.gtid(i)
			if o := p.occ[g]; o == nil || o.n < 2 {
				continue
			}
			id, ok := ids[g]
			if !ok {
				id = int32(len(names)) //nolint:gosec // budget-bounded
				ids[g] = id
				names = append(names, g)
				adj = append(adj, nil)
			}
			if last >= 0 && last != id {
				adj[last] = append(adj[last], edge{id, f})
			}
			last = id
		}
	}
	n := len(names)
	if n == 0 {
		return nil
	}
	// Iterative Tarjan.
	index := make([]int32, n)
	low := make([]int32, n)
	onStack := make([]bool, n)
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
	// Name the inversion: a 2-cycle when one exists, else a cycle path.
	for v := 0; v < n; v++ {
		if comp[v] != cycleComp {
			continue
		}
		for _, e := range adj[v] {
			for _, back := range adj[e.to] {
				if int(back.to) == v {
					return p.inversion(names[v], names[e.to], e.file, back.file, "")
				}
			}
		}
	}
	for v := 0; v < n; v++ {
		if comp[v] != cycleComp {
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
				var through []string
				for _, id := range cyc[1:] {
					through = append(through, names[id].String())
				}
				var fl []string
				for _, f := range cf[1:] {
					fl = append(fl, f.row.Label())
				}
				return p.inversion(names[cyc[0]], names[cyc[1]], cf[0], cf[len(cf)-1],
					fmt.Sprintf(" (through %s in %s)", strings.Join(through, ","), strings.Join(fl, ", ")))
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
