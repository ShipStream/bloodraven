package pitrplan

import (
	"fmt"
	"sort"
	"strings"

	"github.com/shipstream/bloodraven/internal/pitrarchive"
)

// latest is latest mode, and timestamp mode with no witness under
// allowPartial (§4.8): Λ = exec(h*), the unique head modulo X.
func (p *planner) latest(outcome Outcome) error {
	pl := p.plan
	var cands []*Row
	for _, r := range p.rows {
		if r.State == RowActive && !p.isMarked(r.Site) {
			cands = append(cands, r)
		}
	}
	// Rows without complete hints are downloaded: any could be a head.
	for _, r := range cands {
		if !r.Hints.Known {
			if _, err := p.fetch(r); err != nil {
				if isUnreadable(err) {
					return refuse(pitrarchive.TokenArchiveObjectInvalid, "cannot read %v, which may be the archive head; quarantine the row with bloodraven pitr-quarantine if its object is lost", err)
				}
				return err
			}
		}
	}
	execOf := func(r *Row) Set {
		if f := p.files[r]; f != nil {
			return f.exec
		}
		return r.Hints.Previous.Union(r.Hints.GTIDs)
	}
	type head struct {
		row  *Row
		exec Set
	}
	var heads []head
	execs := make([]Set, len(cands))
	for i, r := range cands {
		execs[i] = execOf(r)
	}
	for i, r := range cands {
		maximal := true
		for j := range cands {
			if i != j && execs[j].Contains(execs[i]) && !execs[i].Contains(execs[j]) {
				maximal = false
				break
			}
		}
		if maximal {
			heads = append(heads, head{r, execs[i]})
		}
	}
	if len(heads) == 0 {
		return refuse(pitrarchive.TokenNoArchivedReplay, "the archive holds no binlog of an unmarked site")
	}
	for _, h := range heads {
		if _, err := p.fetch(h.row); err != nil {
			if isUnreadable(err) {
				return refuse(pitrarchive.TokenArchiveObjectInvalid, "cannot read %v, an archive head; quarantine the row with bloodraven pitr-quarantine if its object is lost", err)
			}
			return err
		}
	}
	// h* covers every other head modulo X; several qualifying heads must
	// agree modulo X.
	var star *head
	agree := true
	for i := range heads {
		ok := true
		for j := range heads {
			if i != j && !p.X.Contains(heads[j].exec.Subtract(heads[i].exec)) {
				ok = false
				break
			}
		}
		if !ok {
			continue
		}
		if star == nil {
			star = &heads[i]
		} else if !star.exec.Subtract(p.X).Equal(heads[i].exec.Subtract(p.X)) {
			agree = false
		}
	}
	if !agree {
		star = nil
	}
	if star == nil {
		var parts []string
		for i, h := range heads {
			only := h.exec
			for j, o := range heads {
				if i != j {
					only = only.Subtract(o.exec)
				}
			}
			parts = append(parts, fmt.Sprintf("%s holds %s", h.row.Label(), only.Subtract(p.X).bounded(256)))
			if len(parts) == 8 {
				break
			}
		}
		return refuse(pitrarchive.TokenArchiveSplitBrain, "no archived head covers the others modulo excludeGtids: %s; exclude one branch's transactions, or restore to a timestamp", strings.Join(parts, "; "))
	}
	ref := star.row.ref()
	pl.Head = &ref
	p.used[star.row] = true
	p.lambda = star.exec
	pl.Lineage = p.lambda
	pl.Replay = p.lambda.Subtract(p.B).Subtract(p.X)
	p.replayKnown = true
	if err := p.fetchReplayRows(); err != nil {
		return err
	}
	if err := p.lossEvidence(); err != nil {
		return err
	}
	if !p.lambda.Contains(p.B) {
		extra := p.B.Subtract(p.lambda)
		if p.B.Contains(p.lambda) && !p.anyRowMeets(extra) {
			return refuse(pitrarchive.TokenNoArchivedReplay, "nothing is archived beyond the backup: the backup holds %s beyond the newest archived state %s; wait for archiving (FLUSH BINARY LOGS on the primary rotates the active binlog), or use an older backup",
				extra.bounded(512), star.row.Label())
		}
		return refuse(pitrarchive.TokenBackupNotOnLineage, "the backup holds %s, which is not in the history of the archive head %s", extra.bounded(1024), star.row.Label())
	}
	if err := p.lineageL2(); err != nil {
		return err
	}
	if pl.Replay.IsEmpty() {
		return refuse(pitrarchive.TokenNoArchivedReplay, "nothing is archived beyond the backup (the archive head %s holds no transaction the backup lacks); wait for archiving (FLUSH BINARY LOGS on the primary rotates the active binlog), or use an older backup", star.row.Label())
	}
	return p.searchAndFinish(outcome)
}

func (p *planner) anyRowMeets(s Set) bool {
	for _, r := range p.rows {
		if !r.Hints.Known || r.Hints.GTIDs.Overlaps(s) || r.Hints.Previous.Overlaps(s) {
			return true
		}
	}
	return false
}

// lossEvidence is D-37 with the round-1 bound (G2): a promotion record R
// in force proves g lost only when g was provably committed before R was
// observed and R's server already followed g's source, yet lacked g:
//
//	oct(g) < R.observedAt − δ,  sid(g) ∈ uuids(R),  sid(g) ≠ R.serverUUID,  g ∉ R
//
// A transaction committed after the record (a later writer episode, a
// healthy failback, a mid-era record) is never evidence: the record says
// nothing about what came after it. Inside ±δ of the record the order is
// unknown, so the window is not evidence either.
func (p *planner) lossEvidence() error {
	lost := Set{}
	var lostAt *promoRef
	for i := range p.promos {
		pr := &p.promos[i]
		if !p.X.OfUUID(pr.rec.ServerUUID).Subtract(pr.set).IsEmpty() {
			continue // the user excluded this promotion's branch
		}
		bound := pr.rec.ObservedAt.UnixMicro() - p.delta
		known := pr.set.UUIDs()
		for _, f := range p.order {
			if f.idx.CommitMin >= bound || !f.idx.GTIDs.Overlaps(p.lambda) {
				continue
			}
			for j, t := range f.idx.Txns {
				g := f.gtid(j)
				u := g.UUID()
				if t.OCT >= bound || u == pr.rec.ServerUUID || !known[u] || pr.set.Has(g) || !p.lambda.Has(g) {
					continue
				}
				if lost.add(g) && lostAt == nil && !p.X.Has(g) {
					lostAt = pr
				}
			}
		}
	}
	p.plan.Lost = lost.Intersect(p.X)
	if bad := lost.Subtract(p.X); !bad.IsEmpty() {
		return refuse(pitrarchive.TokenArchiveDivergentHistory, "the newest archived state includes %s, committed before %s was promoted at %s without them; they were lost at that failover. Add them to excludeGtids to restore what the cluster kept, or restore to a timestamp",
			bad.bounded(1024), lostAt.site, lostAt.rec.ObservedAt.UTC().Format(microLayout))
	}
	// The divergence audit is negative evidence too.
	audit := Set{}
	var auditRows []string
	for _, r := range p.rows {
		if r.Divergent != nil && r.Divergent.Overlaps(p.lambda.Subtract(p.X)) {
			audit = audit.Union(r.Divergent.Intersect(p.lambda.Subtract(p.X)))
			auditRows = append(auditRows, r.Label())
		}
	}
	if !audit.IsEmpty() {
		sort.Strings(auditRows)
		return refuse(pitrarchive.TokenArchiveDivergentHistory, "the newest archived state includes %s, which the divergence audit found missing from the history (rows %s); add them to excludeGtids to restore what the cluster kept, or restore to a timestamp",
			audit.bounded(1024), strings.Join(auditRows, ", "))
	}
	return nil
}

// maxSegmentGTIDBytes bounds a segment's canonical GTID set, which
// restore.py hands mysqlbinlog as one --include-gtids argument; Linux caps
// one argument at 128 KiB (MAX_ARG_STRLEN), and a fragmented set (many
// exclusions inside one file) can exceed it. A longer set is split into
// consecutive position ranges of the same file (review round 1 P7).
var maxSegmentGTIDBytes = 64 << 10

// segmentArgBytes bounds len((s ∪ {g}).String()): per source identifier
// its spelling and separator, per interval at most 41 bytes.
func segmentArgBytes(s Set, g GTID) int {
	n := 0
	if _, ok := s[g.SID]; !ok {
		n += len(g.SID) + 1 + 41
	}
	for sid, ivs := range s {
		n += len(sid) + 1 + 41*(len(ivs)+1)
	}
	return n
}

// fillSegments records the plan's files, segments, Q and the D-48 set.
func (p *planner) fillSegments(res *searchResult) {
	pl := p.plan
	if res == nil {
		return
	}
	fileIndex := map[*file]int{}
	onHistory := Set{}
	staged := Set{}
	var newest int64
	var count int64
	sites := map[string]bool{}
	for _, s := range res.segments {
		f := res.files[s.file]
		fi, ok := fileIndex[f]
		if !ok {
			fi = len(pl.Files)
			fileIndex[f] = fi
			pl.Files = append(pl.Files, PlanFile{Row: f.row.ref(), Digest: f.idx.Digest, Size: f.idx.Size, Previous: f.prev, GTIDs: f.idx.GTIDs})
			staged = staged.Union(f.idx.GTIDs)
			sites[f.row.Site] = true
		}
		// One raw segment becomes one or more consecutive position ranges
		// of its file, each with a GTID argument of at most
		// maxSegmentGTIDBytes.
		start := f.prev.Clone()
		for j := 0; j < s.start; j++ {
			start.add(f.gtid(j))
		}
		seg := Segment{File: fi, Start: s.start, GTIDs: Set{}, StartPredecessors: start.Clone()}
		pred := start // pred(f, j) as j advances
		last := -1    // the piece's last replayed transaction
		finish := func(end int) {
			seg.End = end
			count += seg.Transactions
			if last >= 0 {
				// pred(f, last): the predecessors of the piece's last
				// replayed transaction (D-48).
				lp := seg.StartPredecessors.Clone()
				for j := seg.Start; j < last; j++ {
					lp.add(f.gtid(j))
				}
				onHistory = onHistory.Union(p.X.Intersect(p.lambda).Intersect(lp))
			}
			pl.Segments = append(pl.Segments, seg)
		}
		for j := s.start; j < s.end; j++ {
			g := f.gtid(j)
			if !p.X.Has(g) {
				if seg.Transactions > 0 && segmentArgBytes(seg.GTIDs, g) > maxSegmentGTIDBytes {
					finish(j)
					seg = Segment{File: fi, Start: j, GTIDs: Set{}, StartPredecessors: pred.Clone()}
					last = -1
				}
				t := f.txn(j)
				seg.GTIDs.add(g)
				seg.Transactions++
				if seg.FirstCommit == 0 {
					seg.FirstCommit = t.OCT
				}
				seg.LastCommit = t.OCT
				newest = max(newest, t.OCT)
				last = j
			}
			pred.add(g)
		}
		finish(s.end)
	}
	pl.OnHistoryExclusions = onHistory
	pl.PostStop = staged.Subtract(p.lambda).Subtract(p.X)
	for s := range sites {
		pl.Sites = append(pl.Sites, s)
	}
	sort.Strings(pl.Sites)
	pl.newestReplayed, pl.replayedCount = newest, count
}
