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
	p.reportLoss = true
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
// excluded ones as lost; lossReport completes that list once the answer is
// decided).
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
// Only a promotion record is such evidence. A binlog in which a server
// committed transactions of its own without g is not: a read-only replica
// originates transactions too (FLUSH PRIVILEGES and its like consume a
// GTID under super_read_only, and so does any errant write), it may do so
// while it has yet to apply g, and it may be promoted later with nothing
// lost. The archived bytes of that healthy history and those of a writable
// episode that no sidecar recorded say the same thing, "this server wrote
// x without g", so a rule on them refuses archives in which nothing was
// lost and names transactions every server holds as the ones to exclude.
// An unrecorded episode therefore plans where a recorded one refuses
// (TestKnownLimitationUnrecordedEpisodeIsNoLossEvidence).
func (p *planner) lossEvidence() error {
	pl := p.plan
	goal := p.lambda.Subtract(p.X)
	evidence := Set{}
	var lostAt *promoRef
	for i := range p.promos {
		if err := p.ctx.Err(); err != nil {
			return err
		}
		pr := &p.promos[i]
		// Only the head's transactions the record lacks can be lost by it
		// (review F053: records without candidates cost nothing).
		cand := p.lambda.Subtract(pr.set)
		if cand.IsEmpty() {
			continue
		}
		bound := pr.rec.ObservedAt.UnixMicro() - p.delta
		for _, f := range p.order {
			if f.row.Site == pr.site || f.idx.CommitMin >= bound || !f.idx.GTIDs.Overlaps(cand) {
				continue
			}
			for j, t := range f.idx.Txns {
				if t.OCT >= bound {
					continue
				}
				if g := f.gtid(j); cand.Has(g) && evidence.add(g) && lostAt == nil && !p.X.Has(g) {
					lostAt = pr
				}
			}
		}
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
	// the message is bounded. The run is refused, so the report is
	// completed here: nothing it reads can change the answer any more.
	pl.Lost = evidence
	pl.Divergent = lost.Union(audit)
	if err := p.lossReport(); err != nil {
		return err
	}
	sort.Strings(auditRows)
	if !lost.IsEmpty() {
		msg := fmt.Sprintf("the newest archived state includes %s, committed before %s was promoted at %s without them; they were lost at that failover",
			lost.bounded(1024), lostAt.site, lostAt.rec.ObservedAt.UTC().Format(microLayout))
		if !audit.IsEmpty() {
			msg += fmt.Sprintf("; the divergence audit also found %s missing from the history (rows %s)", audit.bounded(512), strings.Join(auditRows, ", "))
		}
		return refuse(pitrarchive.TokenArchiveDivergentHistory, "%s. Add them to excludeGtids to restore what the cluster kept, or restore to a timestamp", msg)
	}
	return refuse(pitrarchive.TokenArchiveDivergentHistory, "the newest archived state includes %s, which the divergence audit found missing from the history (rows %s); add them to excludeGtids to restore what the cluster kept, or restore to a timestamp",
		audit.bounded(1024), strings.Join(auditRows, ", "))
}

// LostUnread is a row the loss report could not take into account.
type LostUnread struct {
	Row RowRef
	// Reason says why the row was not read.
	Reason string
}

// lossReport completes plan.Lost, coverage.json's lost list: it adds the
// excluded transactions of the head that lossEvidence's rule flags through
// a copy in any listed row, whether or not the plan downloaded that row.
// Without it the list depended on where the files were cut (a file that
// holds only excluded transactions is not needed for the replay: lost
// A:2-5 in three files was reported as A:2,A:5).
//
// Every listed row counts, as every downloaded file does in lossEvidence:
// a quarantined row and a row of a marked site are never data, and a copy
// in one still dates its transaction.
//
// The report never changes the answer and cannot fail it:
//
//   - It runs when the answer is decided: after the last check of a plan
//     (searchAndFinish), or when lossEvidence has refused. Every download
//     the plan needs has happened by then, so no row is read for the
//     report and downloaded again for the plan.
//   - It adds members of X ∩ Λ only.
//   - A downloaded row is read from its index. A row that was not
//     downloaded is decided from its manifest commit times when every
//     record that lacks one of its transactions has them all on one side
//     of its bound (hmin/hmax are bound to the row's content and are what
//     every unread row is planned on). Only a row that some record cannot
//     decide that way, and that may hold a transaction no other row has
//     flagged, is read (reportRead).
//   - A row read for the report is not ingested (its copies are not
//     compared with others and it is in no order check), and what its
//     index charged to the budget, or spilled, is returned as soon as its
//     commit times are read.
//   - A row that cannot be read is left out of the report and named in
//     plan.LostUnread, whatever the reason: a missing or damaged object, a
//     store error after the fetcher's retries, a row that retention
//     retired since the manifests were read, an index that does not fit
//     the budget. After a store error no further row is read (each read
//     would wait out the fetcher's retries). Only cancellation ends
//     planning.
//
// Members of B are outside the report (X ∩ B = ∅): see
// TestKnownLimitationLossEvidenceInsideTheBackup.
func (p *planner) lossReport() error {
	excluded := p.lambda.Intersect(p.X)
	if excluded.IsEmpty() {
		return nil
	}
	lost := p.plan.Lost.Clone()
	type point struct {
		bound int64
		want  Set
	}
	var points []point
	// undated are the rows that some point cannot decide from their hints;
	// needs are the points that need a row's commit times.
	var undated []*Row
	needs := map[*Row][]int{}
	for i := range p.promos {
		if err := p.ctx.Err(); err != nil {
			return err
		}
		pr := &p.promos[i]
		want := excluded.Subtract(pr.set)
		if want.IsEmpty() {
			continue
		}
		pt := point{bound: pr.rec.ObservedAt.UnixMicro() - p.delta, want: want}
		points = append(points, pt)
		for _, r := range p.rows {
			if r.Site == pr.site {
				continue
			}
			if f := p.files[r]; f != nil {
				if f.idx.CommitMin >= pt.bound || !f.idx.GTIDs.Overlaps(want) {
					continue
				}
				for j, t := range f.idx.Txns {
					if t.OCT < pt.bound {
						if g := f.gtid(j); want.Has(g) {
							lost.add(g)
						}
					}
				}
				continue
			}
			if r.Hints.lo() >= pt.bound || !r.Hints.mayMeet(want) {
				continue
			}
			if r.Hints.Known && r.Hints.hi() < pt.bound {
				lost = lost.Union(r.Hints.GTIDs.Intersect(want))
				continue
			}
			if needs[r] == nil {
				undated = append(undated, r)
			}
			needs[r] = append(needs[r], len(points)-1)
		}
	}
	p.sortRows(undated)
	storeFailed := false
	for _, r := range undated {
		// Nothing is read for transactions another row has flagged.
		needed := false
		for _, k := range needs[r] {
			in := points[k].want
			if r.Hints.Known {
				in = r.Hints.GTIDs.Intersect(in)
			}
			if !lost.Contains(in) {
				needed = true
				break
			}
		}
		if !needed {
			continue
		}
		if err := p.failed[r]; err != nil {
			p.plan.LostUnread = append(p.plan.LostUnread, LostUnread{Row: r.ref(), Reason: err.Error()})
			continue
		}
		if storeFailed {
			p.plan.LostUnread = append(p.plan.LostUnread, LostUnread{Row: r.ref(), Reason: "not read after the store error of the row before"})
			continue
		}
		transient, err := p.reportRead(r, func(idx *FileIndex) {
			for _, k := range needs[r] {
				pt := &points[k]
				for j, t := range idx.Txns {
					if t.OCT < pt.bound {
						if g := idx.GTID(j); pt.want.Has(g) {
							lost.add(g)
						}
					}
				}
			}
		})
		if err != nil {
			if cerr := p.ctx.Err(); cerr != nil {
				return cerr
			}
			storeFailed = transient
			p.plan.LostUnread = append(p.plan.LostUnread, LostUnread{Row: r.ref(), Reason: err.Error()})
		}
	}
	p.plan.Lost = lost
	return nil
}

// reportRead downloads and indexes r for a report and hands the index to
// use, without making the row one of the planner's files. The index lives
// for the call only: what the read charged to the budget is returned, and
// what it spilled is freed, before reportRead returns, so use must not
// keep any part of the index. A nil error means use ran; otherwise
// transient says whether the store failed rather than the row.
func (p *planner) reportRead(r *Row, use func(*FileIndex)) (transient bool, err error) {
	scope := p.budget.Checkpoint()
	defer scope.Undo()
	var idx *FileIndex
	if sf, ok := p.fetcher.(StagingFetcher); ok {
		idx, err = sf.FetchForPlan(p.ctx, r.ref(), false)
	} else {
		idx, err = p.fetcher.Fetch(p.ctx, r.ref())
	}
	p.plan.Downloaded++
	if err != nil {
		return IsTransient(err), err
	}
	use(idx)
	return false, nil
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
