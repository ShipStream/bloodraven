package pitrplan

import (
	"fmt"
	"math"
	"sort"

	"github.com/shipstream/bloodraven/internal/pitrarchive"
)

// groundTruth is a candidate witness's established predecessor set.
type groundTruth struct {
	g       GTID
	lambda  Set
	kind    GroundTruth
	occ     occurrence
	records []string
	origin  string
	anchor  *file
}

// witness finds w and Λ (§4.4). It reports false when W is empty.
func (p *planner) witness() (bool, error) {
	var rows []*Row
	for _, r := range p.rows {
		if r.State == RowActive && !p.isMarked(r.Site) && r.Hints.hi() >= p.T {
			rows = append(rows, r)
		}
	}
	sort.SliceStable(rows, func(i, j int) bool { return rows[i].Hints.lo() < rows[j].Hints.lo() })
	best := int64(math.MaxInt64)
	cands := Set{}
	for _, r := range rows {
		if r.Hints.lo() > best {
			break
		}
		f, err := p.fetch(r)
		if err != nil {
			if isUnreadable(err) {
				return false, refuse(pitrarchive.TokenArchiveObjectInvalid, "cannot read %v, which may hold the first transaction at or after %s; quarantine the row with bloodraven pitr-quarantine if its object is lost", err, fmtMicros(p.T))
			}
			return false, err
		}
		for i, t := range f.idx.Txns {
			if t.OCT < p.T || t.OCT > best {
				continue
			}
			g := f.gtid(i)
			if p.B.Has(g) || p.X.Has(g) {
				continue
			}
			if t.OCT < best {
				best, cands = t.OCT, Set{}
			}
			cands.add(g)
		}
	}
	if cands.IsEmpty() {
		return false, nil
	}
	tied := cands.GTIDs()
	gts := make(map[GTID]*groundTruth, len(tied))
	for _, c := range tied {
		gt, err := p.groundTruth(c)
		if err != nil {
			return false, err
		}
		gts[c] = gt
	}
	// C*: the causally minimal tied candidates (D-12).
	var minimal []*groundTruth
	for _, c := range tied {
		if !gts[c].lambda.Overlaps(cands.Subtract(SetOf(c))) {
			minimal = append(minimal, gts[c])
		}
	}
	if len(minimal) == 0 {
		return false, refuse(pitrarchive.TokenStopAmbiguous, "the transactions %s committed at the same µs %s each precede another; choose a target ≠ %s", cands.bounded(512), fmtMicros(best), fmtMicros(best))
	}
	w := minimal[0]
	for _, o := range minimal[1:] {
		if !o.lambda.Subtract(p.X).Equal(w.lambda.Subtract(p.X)) {
			return false, refuse(pitrarchive.TokenStopAmbiguous, "%s and %s committed at the same µs %s on different histories; choose a target ≠ %s", w.g, o.g, fmtMicros(best), fmtMicros(best))
		}
	}
	p.lambda = w.lambda
	p.wFile, p.wPos = w.occ.f, w.occ.i
	p.w = &Witness{GTID: w.g, Row: w.occ.f.row.ref(), CommitTime: best, GroundTruth: w.kind, AnchorRecords: w.records, AnchorOriginRow: w.origin}
	p.plan.Witness = p.w
	p.used[w.occ.f.row] = true
	if w.anchor != nil {
		p.used[w.anchor.row] = true
	}
	return true, nil
}

// groundTruth establishes pred(c) from c's occurrences (§4.4, D-39).
func (p *planner) groundTruth(c GTID) (*groundTruth, error) {
	occs, err := p.occurrences(c, "a witness candidate")
	if err != nil {
		return nil, err
	}
	if len(occs) == 0 {
		return nil, fmt.Errorf("pitrplan: candidate %s has no occurrence", c)
	}
	for _, o := range occs {
		if o.f.marked {
			return nil, p.unsafe(o.f.row.Site, fmt.Sprintf("the first transaction at or after the target, %s, is archived in %s", c, o.f.row.Label()))
		}
	}
	var origins []occurrence
	for _, o := range occs {
		if o.f.origin(o.i) {
			origins = append(origins, o)
		}
	}
	if len(origins) > 0 {
		lambda, err := p.pred(origins[0].f, origins[0].i)
		if err != nil {
			return nil, err
		}
		for _, o := range origins[1:] {
			other, err := p.pred(o.f, o.i)
			if err != nil {
				return nil, err
			}
			if !other.Equal(lambda) {
				return nil, refuse(pitrarchive.TokenStopAmbiguous, "%s has origin copies with different predecessor sets (%s and %s)", c, origins[0].f.row.Label(), o.f.row.Label())
			}
		}
		return &groundTruth{g: c, lambda: lambda, kind: GroundTruthOrigin, occ: origins[0]}, nil
	}
	return p.anchoredTruth(c, occs)
}

// anchoredTruth accepts a replica occurrence through an anchor (D-39):
//
//	Anchor(w) = ∪ {R.gtidExecuted : R a promotion record of s with w ∉ R}
//	          ∪ exec(q), q the latest archived origin occurrence of s in pred(o_r)
//
// and accepts o_r iff every g ∈ pred(o_r) ∖ X ∖ Anchor(w) has sid(g) = sid(w).
func (p *planner) anchoredTruth(c GTID, occs []occurrence) (*groundTruth, error) {
	uuid := c.UUID()
	srv := occs[0].f.txn(occs[0].i).Srv
	records := Set{}
	var recordNames []string
	var lagRecord *promoRef
	originSites := map[string]bool{}
	for i := range p.promos {
		pr := &p.promos[i]
		if pr.rec.ServerUUID != uuid || pr.rec.ServerID != srv {
			continue
		}
		originSites[pr.site] = true
		if pr.set.Has(c) {
			if lagRecord == nil || pr.rec.ObservedAt.Before(lagRecord.rec.ObservedAt) {
				lagRecord = pr
			}
			continue
		}
		records = records.Union(pr.set)
		recordNames = append(recordNames, fmt.Sprintf("%s#%d", pr.site, pr.index))
	}
	var accepted *groundTruth
	var acceptedKey Set
	type rejection struct {
		o occurrence
		g GTID
	}
	var rejected []rejection
	for _, o := range occs {
		pred, err := p.pred(o.f, o.i)
		if err != nil {
			return nil, err
		}
		anchor := records
		q, qExec, err := p.originAnchor(uuid, srv, pred, originSites)
		if err != nil {
			return nil, err
		}
		if q != nil {
			anchor = anchor.Union(qExec)
		}
		extra := pred.Subtract(p.X).Subtract(anchor)
		var bad *GTID
		for sid := range extra {
			if sidUUID(sid) != uuid {
				g, _ := extra.Intersect(Set{sid: extra[sid]}).First()
				if bad == nil || g.Less(*bad) {
					bad = &g
				}
			}
		}
		if bad != nil {
			rejected = append(rejected, rejection{o, *bad})
			continue
		}
		key := pred.Subtract(p.X)
		if accepted != nil {
			if !key.Equal(acceptedKey) {
				return nil, refuse(pitrarchive.TokenStopAmbiguous, "%s has replica copies with different predecessor sets (%s and %s)", c, accepted.occ.f.row.Label(), o.f.row.Label())
			}
			continue
		}
		lambda := key.Union(p.X.Intersect(pred).Intersect(anchor.Union(pred.OfUUID(uuid))))
		gt := &groundTruth{g: c, lambda: lambda, kind: GroundTruthAnchored, occ: o, records: recordNames}
		if q != nil {
			if q.marked {
				return nil, p.unsafe(q.row.Site, fmt.Sprintf("the anchor row %s for the replica-copy witness %s", q.row.Label(), c))
			}
			gt.anchor = q
			gt.origin = q.row.Site + "/" + q.row.Name + "/" + q.row.ID
		}
		accepted, acceptedKey = gt, key
	}
	if accepted != nil {
		return accepted, nil
	}
	r := rejected[0]
	originSite := ""
	for s := range originSites {
		if originSite == "" || s < originSite {
			originSite = s
		}
	}
	if originSite == "" {
		originSite = fmt.Sprintf("the server with uuid %s (server_id %d)", uuid, srv)
	}
	msg := fmt.Sprintf("the first transaction at or after %s, %s, is archived only as a replica copy on %s. Its predecessors include %s (sid %s), which %s's origin (%s) is not proven to have executed. Exclude %s",
		fmtMicros(p.T), c, r.o.f.row.Site, r.g, r.g.UUID(), c, originSite, r.g)
	if lagRecord != nil {
		// D-53: w was committed in its era's first probe interval, before
		// the promotion record that would anchor it.
		before := int64(0)
		eraStart := r.o.f.txn(r.o.i).OCT
		for j := 0; j < r.o.i; j++ {
			t := r.o.f.txn(j)
			if r.o.f.gtid(j).UUID() != uuid {
				before = max(before, t.OCT)
			} else if !records.Has(r.o.f.gtid(j)) && t.OCT < eraStart {
				eraStart = t.OCT
			}
		}
		msg += fmt.Sprintf(", or choose a target after %s's promotion record at %s. %s was committed at %s, in the window (%s, %s] between its era's first commit and that record, which records it as already executed",
			lagRecord.site, lagRecord.rec.ObservedAt.UTC().Format(microLayout), c, fmtMicros(r.o.f.txn(r.o.i).OCT), fmtMicros(eraStart), lagRecord.rec.ObservedAt.UTC().Format(microLayout))
		if before > 0 {
			msg += fmt.Sprintf("; safe targets are at or before %s, or after %s", fmtMicros(before), lagRecord.rec.ObservedAt.UTC().Format(microLayout))
		}
	} else if newest := p.newestRecord(originSites); newest != nil {
		msg += fmt.Sprintf(", or choose a target after %s's promotion record at %s", newest.site, newest.rec.ObservedAt.UTC().Format(microLayout))
	}
	return nil, refuse(pitrarchive.TokenStopAmbiguous, "%s", msg)
}

const microLayout = "2006-01-02T15:04:05.000000Z"

func (p *planner) newestRecord(sites map[string]bool) *promoRef {
	var out *promoRef
	for i := range p.promos {
		pr := &p.promos[i]
		if sites[pr.site] && (out == nil || pr.rec.ObservedAt.After(out.rec.ObservedAt)) {
			out = pr
		}
	}
	return out
}

// originAnchor finds q: the latest archived origin occurrence of s =
// (uuid, srv) whose GTID is in pred, among the rows of the sites whose
// promotion records name s. It returns q's file and exec(q).
func (p *planner) originAnchor(uuid string, srv uint32, pred Set, sites map[string]bool) (*file, Set, error) {
	ofS := pred.OfUUID(uuid)
	if ofS.IsEmpty() {
		return nil, nil, nil
	}
	var rows []*Row
	for _, r := range p.rows {
		if r.State == RowActive && sites[r.Site] && r.Hints.mayMeet(ofS) {
			rows = append(rows, r)
		}
	}
	sort.SliceStable(rows, func(i, j int) bool { return rows[i].Hints.hi() > rows[j].Hints.hi() })
	for _, r := range rows {
		f, err := p.fetch(r)
		if err != nil {
			if isUnreadable(err) {
				continue // an anchor is optional; a lost row only narrows it
			}
			return nil, nil, err
		}
		if f.idx.WriterServerID != srv {
			continue
		}
		for i := f.n() - 1; i >= 0; i-- {
			g := f.gtid(i)
			if f.origin(i) && g.UUID() == uuid && pred.Has(g) {
				exec, err := p.pred(f, i+1)
				if err != nil {
					return nil, nil, err
				}
				return f, exec, nil
			}
		}
	}
	return nil, nil, nil
}

// unsafe is the D-34 refusal for a marked site's bytes.
func (p *planner) unsafe(site, what string) *Refusal {
	m := p.marked[site]
	detail := ""
	if m != nil {
		detail = fmt.Sprintf(" (%s: %s, at %s)", m.Violation, m.Detail, m.At.UTC().Format(microLayout))
	}
	return refuse(pitrarchive.TokenArchiveUnsafeHistory, "%s, but site %s's history-integrity marker is set%s, so its binlogs cannot certify history; start a new archive prefix and take a full backup", what, site, detail)
}
