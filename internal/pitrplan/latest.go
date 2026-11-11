package pitrplan

import (
	"fmt"
	"runtime"
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
					return refuse(pitrarchive.TokenArchiveObjectInvalid, "cannot read %v, which may be the archive head; %s", err, lostObjectAdvice)
				}
				return err
			}
		}
	}
	heads, err := p.maximalHeads(cands)
	if err != nil {
		return err
	}
	if len(heads) == 0 {
		// Only marked sites archived anything: their bytes cannot
		// certify history, and waiting for archiving cannot help (review
		// F057).
		if err := p.markedMayHold(func(*Row) bool { return true }, "no unmarked site archived a binlog"); err != nil {
			return err
		}
		return refuse(pitrarchive.TokenNoArchivedReplay, "the archive holds no binlog of an unmarked site")
	}
	for _, h := range heads {
		if _, err := p.fetch(h.row); err != nil {
			if isUnreadable(err) {
				return refuse(pitrarchive.TokenArchiveObjectInvalid, "cannot read %v, an archive head; %s", err, lostObjectAdvice)
			}
			return err
		}
		h.exec = p.files[h.row].exec
	}
	// h* covers every other head modulo X. Two qualifying heads agree
	// modulo X by construction (each one's difference from the other is
	// in X), so the first in file order is h* (review F172).
	var star *head
	for i := range heads {
		ok := true
		for j := range heads {
			if i != j && !p.X.Contains(heads[j].exec.Subtract(heads[i].exec)) {
				ok = false
				break
			}
		}
		if ok {
			star = heads[i]
			break
		}
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

// head is a maximal row of latest mode and its exec.
type head struct {
	row  *Row
	exec Set
}

// maximalHeads returns the rows whose exec no other candidate's strictly
// contains, in file order. Candidates are visited largest first, so a
// strict superset of a candidate is always visited before it, and is
// either a head or inside one: comparing a candidate with the heads so far
// suffices (review F056: linear in the rows for a single chain, where the
// all-pairs scan was quadratic). The execs of rows not downloaded are
// built from their hints and charged while they live.
func (p *planner) maximalHeads(cands []*Row) ([]*head, error) {
	tmp := charges{b: p.budget}
	defer tmp.release()
	type cand struct {
		k     int
		row   *Row
		exec  Set
		count int64
	}
	cs := make([]cand, len(cands))
	for k, r := range cands {
		e := r.Hints.Previous.Union(r.Hints.GTIDs)
		if f := p.files[r]; f != nil {
			e = f.exec
		} else if err := tmp.chargeSet(e, "archive head candidates"); err != nil {
			return nil, err
		}
		cs[k] = cand{k, r, e, e.Count()}
		if k%1024 == 0 {
			if err := p.ctx.Err(); err != nil {
				return nil, err
			}
		}
	}
	sort.SliceStable(cs, func(i, j int) bool { return cs[i].count > cs[j].count })
	var kept []cand
	for _, c := range cs {
		maximal := true
		for _, h := range kept {
			if h.count > c.count && h.exec.Contains(c.exec) {
				maximal = false
				break
			}
		}
		if maximal {
			kept = append(kept, c)
		}
	}
	sort.Slice(kept, func(i, j int) bool { return kept[i].k < kept[j].k })
	heads := make([]*head, len(kept))
	for i, c := range kept {
		heads[i] = &head{row: c.row, exec: c.exec}
	}
	return heads, nil
}

func (p *planner) anyRowMeets(s Set) bool {
	for _, r := range p.rows {
		if !r.Hints.Known || r.Hints.GTIDs.Overlaps(s) || r.Hints.Previous.Overlaps(s) {
			return true
		}
	}
	return false
}

// lossEvidence is D-37 as narrowed by review rulings N1 and N2: a
// promotion record R proves g lost when g, a transaction of the head with
// a downloaded occurrence in a row of a site other than R's, was provably
// committed before R was observed, yet R lacks it:
//
//	oct(g) < R.observedAt − δ,  g ∉ R
//
// and refuses unless every such g is excluded (coverage.json records the
// excluded ones as lost; lossEvidenceUnread completes that report with the
// excluded transactions of rows the plan did not download).
//
// R's server became writable without g, so a history holding g is a branch
// R's site abandoned (a lost tail) or a concurrent writer's. Whether R
// knows g's source does not matter: a writer whose whole first era was
// lost leaves no source in a later record (N2). A record is never set
// aside: only excluding exactly the flagged transactions resolves the
// refusal (N1). A transaction committed after the record (a later writer
// episode, a healthy failback, a mid-era record) is never evidence, and
// inside ±δ of the record the order is unknown, so the window is not
// evidence either. The record's own site's rows are its own later history.
//
// A binlog proves the same without a record (ownWrites): a file of site s
// that holds a replayed transaction its own server originated shows that
// server writable at that transaction's commit time, having executed at
// most exec(f). A record is one sample of a writable server's state and
// such a file is another, read from verified bytes, so a writable episode
// that no sidecar observed leaves the same loss evidence as a recorded one
// once its file is archived.
func (p *planner) lossEvidence() error {
	pl := p.plan
	goal := p.lambda.Subtract(p.X)
	points, err := p.lossPoints()
	if err != nil {
		return err
	}
	evidence := Set{}
	var lostAt *lossPoint
	for i := range points {
		if err := p.ctx.Err(); err != nil {
			return err
		}
		pt := &points[i]
		// Only the head's transactions the server lacked can be lost by it
		// (review F053: points without candidates cost nothing).
		cand := p.lambda.Subtract(pt.have)
		if cand.IsEmpty() {
			continue
		}
		for _, f := range p.order {
			if f.row.Site == pt.site || f.idx.CommitMin >= pt.bound || !f.idx.GTIDs.Overlaps(cand) {
				continue
			}
			for j, t := range f.idx.Txns {
				if t.OCT >= pt.bound {
					continue
				}
				if g := f.gtid(j); cand.Has(g) && evidence.add(g) && lostAt == nil && !p.X.Has(g) {
					lostAt = pt
				}
			}
		}
	}
	// The report is complete: excluded transactions in rows this plan did
	// not download are flagged too.
	evidence, err = p.lossEvidenceUnread(points, evidence)
	if err != nil {
		return err
	}
	// The divergence audit is negative evidence too.
	audit := Set{}
	var auditRows []string
	for _, r := range p.rows {
		if r.Divergent != nil && r.Divergent.Overlaps(goal) {
			audit = audit.Union(r.Divergent.Intersect(goal))
			auditRows = append(auditRows, r.Label())
		}
	}
	pl.Lost = evidence.Intersect(p.X)
	lost := evidence.Subtract(p.X)
	if lost.IsEmpty() && audit.IsEmpty() {
		return nil
	}
	// The complete set to exclude goes to coverage.json and to
	// pitr-download's BLOODRAVEN_PITR_DIVERGENT_SET line (review F054);
	// the message is bounded.
	pl.Lost = evidence
	pl.Divergent = lost.Union(audit)
	sort.Strings(auditRows)
	if !lost.IsEmpty() {
		msg := fmt.Sprintf("the newest archived state includes %s, %s", lost.bounded(1024), lostAt.describe())
		if !audit.IsEmpty() {
			msg += fmt.Sprintf("; the divergence audit also found %s missing from the history (rows %s)", audit.bounded(512), strings.Join(auditRows, ", "))
		}
		return refuse(pitrarchive.TokenArchiveDivergentHistory, "%s. Add them to excludeGtids to restore what the cluster kept, or restore to a timestamp", msg)
	}
	return refuse(pitrarchive.TokenArchiveDivergentHistory, "the newest archived state includes %s, which the divergence audit found missing from the history (rows %s); add them to excludeGtids to restore what the cluster kept, or restore to a timestamp",
		audit.bounded(1024), strings.Join(auditRows, ", "))
}

// lossPoint is one sample of a writable server's state: at bound + δ the
// server of site had executed at most have. It is a promotion record, or a
// binlog holding a transaction the server originated (ownWrites).
type lossPoint struct {
	site  string
	have  Set
	bound int64 // µs: a transaction committed before it is provably older
	rec   *promoRef
	// file and own are set for a binlog: own is the newest replayed
	// transaction the file's server originated, committed at ownOCT.
	file   *file
	own    GTID
	ownOCT int64
}

// describe is the half of the refusal that names the evidence.
func (pt *lossPoint) describe() string {
	if pt.rec != nil {
		return fmt.Sprintf("committed before %s was promoted at %s without them; they were lost at that failover",
			pt.site, pt.rec.rec.ObservedAt.UTC().Format(microLayout))
	}
	return fmt.Sprintf("committed before %s committed %s at %s in %s, whose server had not executed them; they were lost at a failover to %s that left no promotion record",
		pt.site, pt.own, fmtMicros(pt.ownOCT), pt.file.row.Label(), pt.site)
}

// lossPoints are the promotion records, then the binlogs with own writes.
func (p *planner) lossPoints() ([]lossPoint, error) {
	points := make([]lossPoint, 0, len(p.promos))
	for i := range p.promos {
		pr := &p.promos[i]
		points = append(points, lossPoint{site: pr.site, have: pr.set, bound: pr.rec.ObservedAt.UnixMicro() - p.delta, rec: pr})
	}
	for k, f := range p.order {
		if k%1024 == 0 {
			if err := p.ctx.Err(); err != nil {
				return nil, err
			}
		}
		if g, oct, ok := p.ownWrites(f); ok {
			points = append(points, lossPoint{site: f.row.Site, have: f.exec, bound: oct - p.delta, file: f, own: g, ownOCT: oct})
		}
	}
	return points, nil
}

// ownWrites returns the newest replayed transaction of f that f's own
// server originated (its origin server_id is the file's writer), and its
// commit time. Such a transaction was written on that server by a client,
// so the server was writable then, and it had executed at most exec(f):
// what the bytes of f prove about a writable episode, with or without a
// promotion record of it.
//
// Only an active row of an unmarked site counts (a marked site's bytes
// cannot certify history, and a quarantined row is waived evidence), and
// only a transaction of P = Λ ∖ (B ∪ X): every row that may hold one is
// downloaded before the evidence pass, so this evidence does not depend on
// which other rows a plan happened to read.
func (p *planner) ownWrites(f *file) (GTID, int64, bool) {
	if f.marked || f.row.State != RowActive || !f.idx.GTIDs.Overlaps(p.plan.Replay) {
		return GTID{}, 0, false
	}
	var own GTID
	var newest int64
	found := false
	for j, t := range f.idx.Txns {
		if !f.origin(j) || (found && t.OCT <= newest) {
			continue
		}
		if g := f.gtid(j); p.plan.Replay.Has(g) {
			own, newest, found = g, t.OCT, true
		}
	}
	return own, newest, found
}

// lossEvidenceUnread adds to evidence the excluded transactions that
// lossEvidence's rule flags and whose copies are in rows the plan did not
// download. Every active row of an unmarked site that may hold a member of
// P = Λ ∖ (B ∪ X) is downloaded before the evidence pass, so a row left
// unread holds, of Λ ∖ B, only excluded transactions (a marked site's rows
// are not downloaded for P at all, and count here like any other copy, as
// they do in the pass over the downloaded files): without this pass
// coverage.json's lost list depended on where the files were cut (lost
// A:2-5 in three files was reported as A:2,A:5).
//
// These transactions are in X, so they change the report and never the
// answer, and the pass is built so that it cannot change the answer either:
//
//   - A row whose manifest commit times lie on one side of the point's
//     bound is decided from them (hmin/hmax are bound to the row's content
//     and are what every unread row is planned on).
//   - A row whose times straddle the bound, or are unknown, is read for its
//     commit times only (reportIndex). It is not ingested: its copies are
//     not compared with others, it is in no order check and not in the
//     closure, so a row read for the report cannot cause a refusal.
//   - A row that cannot be read is left out of the report. A store error
//     that says nothing about the row ends planning, as in every download.
//
// Members of B are outside this pass (X ∩ B = ∅): see
// TestKnownLimitationLossEvidenceInsideTheBackup.
func (p *planner) lossEvidenceUnread(points []lossPoint, evidence Set) (Set, error) {
	excluded := p.lambda.Intersect(p.X)
	if excluded.IsEmpty() {
		return evidence, nil
	}
	for i := range points {
		if err := p.ctx.Err(); err != nil {
			return nil, err
		}
		pt := &points[i]
		want := excluded.Subtract(pt.have)
		if want.IsEmpty() {
			continue
		}
		bound := pt.bound
		for _, r := range p.rows {
			if r.Site == pt.site || r.State != RowActive || p.files[r] != nil || p.failed[r] != nil ||
				!r.Hints.Known || r.Hints.lo() >= bound || !r.Hints.GTIDs.Overlaps(want) {
				continue
			}
			in := r.Hints.GTIDs.Intersect(want).Subtract(evidence)
			if in.IsEmpty() {
				continue
			}
			if r.Hints.hi() < bound {
				evidence = evidence.Union(in)
				continue
			}
			idx, err := p.reportIndex(r)
			if err != nil {
				return nil, err
			}
			if idx == nil {
				continue
			}
			found := Set{}
			for j, t := range idx.Txns {
				if t.OCT < bound {
					if g := idx.GTID(j); in.Has(g) {
						found.add(g)
					}
				}
			}
			evidence = evidence.Union(found)
		}
	}
	return evidence, nil
}

// reportIndex downloads and indexes r for a report, without making it one
// of the planner's files. It returns nil when the row cannot be read. The
// index stays charged to the budget and is kept: a later fetch of the row
// ingests it instead of downloading the row again.
func (p *planner) reportIndex(r *Row) (*FileIndex, error) {
	if idx, ok := p.reportRead[r]; ok {
		return idx, nil
	}
	if err := p.ctx.Err(); err != nil {
		return nil, err
	}
	var idx *FileIndex
	var err error
	if sf, ok := p.fetcher.(StagingFetcher); ok {
		idx, err = sf.FetchForPlan(p.ctx, r.ref(), false)
	} else {
		idx, err = p.fetcher.Fetch(p.ctx, r.ref())
	}
	p.plan.Downloaded++
	if err != nil {
		if p.ctx.Err() != nil {
			return nil, p.ctx.Err()
		}
		if IsTransient(err) {
			return nil, err
		}
		idx = nil
	}
	p.reportRead[r] = idx
	return idx, nil
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
// Each segment's StartPredecessors is retained in the plan (coverage.json
// prints it), so each is charged; the running predecessor set is charged
// at its largest while it lives (review F055).
func (p *planner) fillSegments(res *searchResult) error {
	pl := p.plan
	if res == nil {
		return nil
	}
	tmp := charges{b: p.budget}
	defer tmp.release()
	onHistXL := p.X.Intersect(p.lambda)
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
			if err := tmp.chargeSet(f.idx.GTIDs, "staged GTIDs"); err != nil {
				return err
			}
			staged = staged.Union(f.idx.GTIDs)
			sites[f.row.Site] = true
		}
		// pred(f, j) as j advances: at most exec(f).
		if err := tmp.chargeSet(f.exec, "segment predecessors"); err != nil {
			return err
		}
		pred := f.prev.Clone()
		for j := 0; j < s.start; j++ {
			pred.add(f.gtid(j))
		}
		// One raw segment becomes one or more consecutive position ranges
		// of its file, each with a GTID argument of at most
		// maxSegmentGTIDBytes.
		var seg Segment
		open := func(j int) error {
			if err := p.budget.chargeSet(pred, "segment start predecessors"); err != nil {
				return err
			}
			seg = Segment{File: fi, Start: j, GTIDs: Set{}, StartPredecessors: pred.Clone()}
			return nil
		}
		if err := open(s.start); err != nil {
			return err
		}
		last := -1 // the raw segment's last replayed transaction
		for j := s.start; j < s.end; j++ {
			g := f.gtid(j)
			if !p.X.Has(g) {
				if seg.Transactions > 0 && segmentArgBytes(seg.GTIDs, g) > maxSegmentGTIDBytes {
					seg.End = j
					count += seg.Transactions
					pl.Segments = append(pl.Segments, seg)
					if err := open(j); err != nil {
						return err
					}
				}
				t := f.txn(j)
				if seg.Transactions == 0 {
					seg.StartPosition = t.Pos
				}
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
		seg.End = s.end
		count += seg.Transactions
		pl.Segments = append(pl.Segments, seg)
		// D-48: the excluded members of Λ among pred(f, last), the
		// predecessors of the last replayed transaction. The pieces of
		// one raw segment chain, so their union is the last piece's;
		// it is collected without materializing pred(f, last).
		if last >= 0 {
			onHistory = onHistory.Union(onHistXL.Intersect(f.prev))
			for j := 0; j < last; j++ {
				if g := f.gtid(j); onHistXL.Has(g) {
					onHistory.add(g)
				}
			}
		}
	}
	pl.OnHistoryExclusions = onHistory
	pl.PostStop = staged.Subtract(p.lambda).Subtract(p.X)
	for s := range sites {
		pl.Sites = append(pl.Sites, s)
	}
	sort.Strings(pl.Sites)
	pl.newestReplayed, pl.replayedCount = newest, count
	probeMemory()
	runtime.KeepAlive(staged)
	return nil
}
