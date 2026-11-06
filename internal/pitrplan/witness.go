package pitrplan

import (
	"fmt"
	"math"
	"slices"
	"sort"
	"strings"
	"time"

	"github.com/shipstream/bloodraven/internal/pitrarchive"
)

// groundTruth is a candidate witness's established predecessor set.
type groundTruth struct {
	g      GTID
	lambda Set
	kind   GroundTruth
	occ    occurrence
	// occs are every downloaded occurrence of g; all of their rows enter
	// the closure U (review F010).
	occs    []occurrence
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
				return false, refuse(pitrarchive.TokenArchiveObjectInvalid, "cannot read %v, which may hold the first transaction at or after %s; %s", err, fmtMicros(p.T), lostObjectAdvice)
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
	p.w = &Witness{GTID: w.g, Row: w.occ.f.row.ref(), CommitTime: best, GroundTruth: w.kind, AnchorRecords: w.records, AnchorOriginRow: w.origin}
	p.plan.Witness = p.w
	for _, o := range w.occs {
		p.used[o.f.row] = true
	}
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
		return &groundTruth{g: c, lambda: lambda, kind: GroundTruthOrigin, occ: origins[0], occs: occs}, nil
	}
	gt, err := p.anchoredTruth(c, occs)
	if err != nil {
		return nil, err
	}
	gt.occs = occs
	return gt, nil
}

// anchoredTruth accepts a replica occurrence through an anchor (D-39):
//
//	Anchor(w) = ∪ {R.gtidExecuted : R a promotion record of s that provably precedes w}
//	          ∪ exec(q), q the latest archived origin occurrence of s in pred(o_r)
//
// and accepts o_r iff every g ∈ pred(o_r) ∖ X ∖ Anchor(w) has sid(g) = sid(w).
// anchorRecords says which promotion records provably precede w and are
// not fenced off by a later record of their site (round-1 G1, N3
// revised); q is fenced the same way (originAnchor).
func (p *planner) anchoredTruth(c GTID, occs []occurrence) (*groundTruth, error) {
	uuid := c.UUID()
	first := occs[0].f.txn(occs[0].i)
	srv := first.Srv
	choice := p.anchorRecords(c, srv, first.OCT)
	records, recordNames, lagRecord, originSites := choice.set, choice.names, choice.lag, choice.sites
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
		q, qExec, qFence, err := p.originAnchor(c, srv, pred, originSites, o.f.txn(o.i).OCT)
		if err != nil {
			return nil, err
		}
		if qFence != "" && !slices.Contains(choice.shrunk, qFence) {
			choice.shrunk = append(choice.shrunk, qFence)
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
	if lagRecord != nil && len(choice.shrunk) == 0 {
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
		after := p.anchorFrom(lagRecord)
		msg += fmt.Sprintf(", or choose a target after %s (%s's promotion record at %s plus the %s clock-skew margin). %s was committed at %s, in the window (%s, %s] between its era's first commit and that record, which records it as already executed",
			fmtMicros(after), lagRecord.site, lagRecord.rec.ObservedAt.UTC().Format(microLayout), p.deltaString(), c, fmtMicros(r.o.f.txn(r.o.i).OCT), fmtMicros(eraStart), lagRecord.rec.ObservedAt.UTC().Format(microLayout))
		if before > 0 {
			msg += fmt.Sprintf("; safe targets are at or before %s, or after %s", fmtMicros(before), fmtMicros(after))
		}
	} else if pending := choice.pending; pending != nil {
		// A record observed after w, or within δ before it, is not
		// provably before w.
		msg += fmt.Sprintf(", or choose a target after %s (%s's promotion record at %s, which is not provably before %s, plus the %s clock-skew margin)",
			fmtMicros(p.anchorFrom(pending)), pending.site, pending.rec.ObservedAt.UTC().Format(microLayout), c, p.deltaString())
	} else if newest := p.newestRecord(originSites); newest != nil {
		msg += fmt.Sprintf(", or choose a target after %s (%s's promotion record at %s plus the %s clock-skew margin)",
			fmtMicros(p.anchorFrom(newest)), newest.site, newest.rec.ObservedAt.UTC().Format(microLayout), p.deltaString())
	}
	if len(choice.shrunk) > 0 {
		msg += "; " + strings.Join(choice.shrunk, "; ")
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
// promotion records name s. It returns q's file and exec(q). A q that a
// record of its site fences off before w (until = oct(w)) is no anchor:
// exec(q) is then not proven to precede w on s, and the third result
// says why.
func (p *planner) originAnchor(c GTID, srv uint32, pred Set, sites map[string]bool, until int64) (*file, Set, string, error) {
	uuid := c.UUID()
	ofS := pred.OfUUID(uuid)
	if ofS.IsEmpty() {
		return nil, nil, "", nil
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
			return nil, nil, "", err
		}
		if f.idx.WriterServerID != srv {
			continue
		}
		for i := f.n() - 1; i >= 0; i-- {
			g := f.gtid(i)
			if f.origin(i) && g.UUID() == uuid && pred.Has(g) {
				exec, err := p.pred(f, i+1)
				if err != nil {
					return nil, nil, "", err
				}
				// q's commit time is on the server's clock and its site's
				// records on the sidecar's: widen by δ.
				if fc := p.fenceOf(r.Site, nil, c, srv, f.txn(i).OCT, until, p.delta); fc != nil {
					return nil, nil, fc.explain(fmt.Sprintf("the origin row %s (through %s) is not an anchor", r.Label(), g), c), nil
				}
				return f, exec, "", nil
			}
		}
	}
	return nil, nil, "", nil
}

// anchorChoice is the record part of Anchor(w).
type anchorChoice struct {
	set   Set
	names []string
	// lag is the earliest record of s that already holds w (D-53).
	lag *promoRef
	// pending is the earliest record of s without w observed within δ of
	// w, or after it: its order against w is unknown.
	pending *promoRef
	// shrunk says which anchors a fence refused, and why.
	shrunk []string
	// sites are the sites whose records name s.
	sites map[string]bool
}

// anchorRecords selects the promotion records of s = (uuid(w), srv) that
// provably precede w (§4.4, round-1 G1, N3 revised). Per site, it takes
// the latest promotion record R with
//
//	w ∉ R  and  R.observedAt + δ < oct(w)
//
// (observed before w even with the clock-skew margin), and uses it only if
// no record fences it off (fenceOf): R ⊆ pred_s(w) holds because
// gtid_executed only grows within one server incarnation, and the
// sidecar records every episode and every incarnation change it observes.
// Earlier records of the site add nothing: within one incarnation they
// are subsets of R.
func (p *planner) anchorRecords(c GTID, srv uint32, oct int64) anchorChoice {
	uuid := c.UUID()
	out := anchorChoice{set: Set{}, sites: map[string]bool{}}
	for i := range p.promos {
		pr := &p.promos[i]
		if pr.rec.ServerUUID != uuid || pr.rec.ServerID != srv {
			continue
		}
		out.sites[pr.site] = true
		if pr.set.Has(c) {
			if out.lag == nil || pr.rec.ObservedAt.Before(out.lag.rec.ObservedAt) {
				out.lag = pr
			}
		}
	}
	sites := make([]string, 0, len(out.sites))
	for s := range out.sites {
		sites = append(sites, s)
	}
	sort.Strings(sites)
	for _, site := range sites {
		var best *promoRef
		for i := range p.promos {
			pr := &p.promos[i]
			if pr.site != site || pr.rec.ServerUUID != uuid || pr.rec.ServerID != srv || pr.set.Has(c) {
				continue
			}
			if pr.rec.ObservedAt.UnixMicro()+p.delta >= oct {
				if out.pending == nil || pr.rec.ObservedAt.Before(out.pending.rec.ObservedAt) {
					out.pending = pr
				}
				continue
			}
			if best == nil || pr.rec.ObservedAt.After(best.rec.ObservedAt) {
				best = pr
			}
		}
		if best == nil {
			continue
		}
		name := best.name()
		// R and the site's later records share the sidecar's clock.
		if fc := p.fenceOf(site, best, c, srv, best.rec.ObservedAt.UnixMicro(), oct, 0); fc != nil {
			out.shrunk = append(out.shrunk, fc.explain("the promotion records "+name+" are not anchors", c))
			continue
		}
		out.set = out.set.Union(best.set)
		out.names = append(out.names, name)
	}
	return out
}

func (pr *promoRef) name() string { return fmt.Sprintf("%s#%d", pr.site, pr.index) }

// fence is a record that keeps an anchor from proving s's history at w.
type fence struct {
	rec *promoRef
	// role: another server's promotion record (F062), not a record of
	// the anchor's own site.
	role bool
}

// explain completes "<anchor> is not an anchor" with the fence's reason.
func (f *fence) explain(what string, w GTID) string {
	at := f.rec.rec.ObservedAt.UTC().Format(microLayout)
	if f.role {
		return fmt.Sprintf("%s, because %s, another server's promotion record observed at %s, between it and %s, does not hold %s: the server lost its role in between and its next era's record is missing or not provably before %s",
			what, f.rec.name(), at, w, w, w)
	}
	kind := "a promotion record"
	if f.rec.rec.Kind == pitrarchive.RecordReset {
		kind = "an incarnation-reset record"
	} else if f.rec.rec.Kind != pitrarchive.RecordPromotion {
		kind = fmt.Sprintf("a %q record", f.rec.rec.Kind)
	}
	window := "an unknown time"
	if pi := time.Duration(f.rec.rec.ProbeInterval); pi > 0 {
		window = "up to " + pi.String()
	}
	return fmt.Sprintf("%s, because a later record of site %s, %s (%s observed at %s, reporting a change that happened %s before it), is not provably after %s: %s may belong to a later episode or server incarnation, whose history need not hold the anchor",
		what, f.rec.site, f.rec.name(), kind, at, window, w, w)
}

// fenceOf returns the first record that keeps an anchor of site, taken at
// since (µs: a record's observedAt, or q's commit time), from proving
// that s = (uuid(w), srv) still held its set when w committed at until
// (µs), or nil (§4.4, N3 revised). margin widens since for an anchor on
// another clock than the site's records (q's commit time). self is the
// anchor record itself (nil for q). A fence is
//
//   - a record of the site, of any kind and any server, observed after
//     the anchor (at ≥ since − margin) and no later than until + PI + δ,
//     PI the record's ProbeInterval (unknown: unbounded). The change the
//     record reports (a new episode or server incarnation) happened
//     within PI before it, so it may lie before w, and w may belong to a
//     later episode or incarnation whose history need not hold the
//     anchor. Neither a file's first event time nor a clone bridge row
//     dates an incarnation (review F009): the sidecar's records do;
//   - (review F062) another server's promotion record, on any site,
//     observed within (since − δ, until + δ) that does not hold w: s lost
//     its role and regained it in between, and its new era's record is
//     missing or not provably before w. A record that holds w was taken
//     after w committed, so it is no role change before w.
//
// Residual risk: an incarnation change the sidecar never observes (one
// that happens and is reverted between two probes, or while the sidecar
// is down when the server's new history still holds the site's latest
// record) leaves no fence.
func (p *planner) fenceOf(site string, self *promoRef, w GTID, srv uint32, since, until, margin int64) *fence {
	uuid := w.UUID()
	for i := range p.fences {
		f := &p.fences[i]
		if f.site != site || (self != nil && f.index == self.index) {
			continue
		}
		at := f.rec.ObservedAt.UnixMicro()
		if at+margin < since {
			continue
		}
		pi := time.Duration(f.rec.ProbeInterval).Microseconds()
		if pi <= 0 || at <= until+pi+p.delta {
			return &fence{rec: f}
		}
	}
	for i := range p.promos {
		pr := &p.promos[i]
		if pr.rec.ServerUUID == uuid && pr.rec.ServerID == srv {
			continue
		}
		at := pr.rec.ObservedAt.UnixMicro()
		if at+p.delta > since && at-p.delta < until && !pr.set.Has(w) {
			return &fence{rec: pr, role: true}
		}
	}
	return nil
}

// anchorFrom is the earliest commit time a witness can have for record pr
// to provably precede it: R.observedAt + δ (exclusive bound).
func (p *planner) anchorFrom(pr *promoRef) int64 {
	return pr.rec.ObservedAt.UnixMicro() + p.delta
}

func (p *planner) deltaString() string {
	return (time.Duration(p.delta) * time.Microsecond).String()
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
