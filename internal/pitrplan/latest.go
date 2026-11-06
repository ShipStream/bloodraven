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

// lossEvidence is D-37: a promotion record R in force proves g lost when
// sid(g) ≠ R.serverUUID, g ∉ R and oct(g) < R.observedAt + δ.
func (p *planner) lossEvidence() error {
	lost := Set{}
	var lostAt *promoRef
	for i := range p.promos {
		pr := &p.promos[i]
		if !p.X.OfUUID(pr.rec.ServerUUID).Subtract(pr.set).IsEmpty() {
			continue // the user excluded this promotion's branch
		}
		bound := pr.rec.ObservedAt.UnixMicro() + p.delta
		for _, f := range p.order {
			if f.idx.CommitMin >= bound || !f.idx.GTIDs.Overlaps(p.lambda) {
				continue
			}
			for j, t := range f.idx.Txns {
				g := f.gtid(j)
				if t.OCT >= bound || g.UUID() == pr.rec.ServerUUID || pr.set.Has(g) || !p.lambda.Has(g) {
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
		seg := Segment{File: fi, Start: s.start, End: s.end, GTIDs: Set{}}
		start := f.prev.Clone()
		for j := 0; j < s.start; j++ {
			start.add(f.gtid(j))
		}
		seg.StartPredecessors = start
		lastIncluded := -1
		for j := s.start; j < s.end; j++ {
			g := f.gtid(j)
			if p.X.Has(g) {
				continue
			}
			t := f.txn(j)
			seg.GTIDs.add(g)
			seg.Transactions++
			if seg.FirstCommit == 0 {
				seg.FirstCommit = t.OCT
			}
			seg.LastCommit = t.OCT
			newest = max(newest, t.OCT)
			lastIncluded = j
		}
		count += seg.Transactions
		if lastIncluded >= 0 {
			// pred(f_s, last_s): the predecessors of the segment's last
			// replayed transaction.
			pred := start.Clone()
			for j := s.start; j < lastIncluded; j++ {
				pred.add(f.gtid(j))
			}
			onHistory = onHistory.Union(p.X.Intersect(p.lambda).Intersect(pred))
		}
		pl.Segments = append(pl.Segments, seg)
	}
	pl.OnHistoryExclusions = onHistory
	pl.PostStop = staged.Subtract(p.lambda).Subtract(p.X)
	for s := range sites {
		pl.Sites = append(pl.Sites, s)
	}
	sort.Strings(pl.Sites)
	pl.newestReplayed, pl.replayedCount = newest, count
}
