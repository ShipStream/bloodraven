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
//	          ∪ P(R*), R* the promotion record of s whose episode committed w
//	          ∪ exec(q), q the latest archived origin occurrence of s in pred(o_r)
//
// and accepts o_r iff every g ∈ pred(o_r) ∖ X ∖ Anchor(w) has sid(g) = sid(w).
// anchorRecords says which promotion records provably precede w and are
// not fenced off by a later record of their site (round-1 G1, N3
// revised), and containmentAnchor which record's promotion set P does
// (issue #269); q is fenced the same way (originAnchor). The planner ships
// without the P(R*) term: containmentAnchors is off, and containmentAnchor
// then selects no record.
//
// The anchor proves Anchor(w) ⊆ pred_s(w). The plan certifies the whole of
// pred(o_r), so a copy that passes is then held to what the anchor says of
// s's history (copyAgainstAnchor); a copy that fails refuses the plan, it
// is not passed over for another copy.
func (p *planner) anchoredTruth(c GTID, occs []occurrence) (*groundTruth, error) {
	uuid := c.UUID()
	first := occs[0].f.txn(occs[0].i)
	srv := first.Srv
	choice := p.anchorRecords(c, srv, first.OCT)
	if r := choice.contradicted; r != nil {
		// Refused outright, not only set aside: without the record another
		// copy or another anchor could certify a target that refuses with
		// it (two copies that the record shows to disagree, for one), and a
		// record must never be evidence whose loss admits a plan.
		msg := fmt.Sprintf("the first transaction at or after %s, %s, is archived only as a replica copy. The confirmation of the promotion record %s, observed at %s, holds %s, yet the archived %s was committed at %s, after it: the archived transaction is not the one %s's origin had executed then (the GTID was issued twice, or the server's clock was set back in between), so no record of the origin dates the history it belongs to",
			fmtMicros(p.T), c, r.name(), r.rec.ConfirmedAt.UTC().Format(microLayout), c, c, fmtMicros(first.OCT), c)
		if before := occs[0].lastOtherBefore(uuid); before > 0 {
			msg += fmt.Sprintf(". Choose a target at or before %s, the last transaction of another server before %s in %s", fmtMicros(before), c, occs[0].f.row.Label())
		}
		msg += ". If the GTID was issued twice, start a new archive prefix and take a full backup"
		return nil, refuse(pitrarchive.TokenStopAmbiguous, "%s", msg)
	}
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
		if err := p.copyAgainstAnchor(c, o, pred, &choice, anchor, q, qExec); err != nil {
			return nil, err
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
	if cr := choice.contained; cr != nil {
		// P anchored the copy and still does not explain r.g.
		msg += fmt.Sprintf("; the promotion record %s proves by containment only its promotion set (promotionGtidExecuted), which does not hold %s: %s reached %s's server after its sidecar's last read-only observation and the promotion-time binlog rotation was not acknowledged, or %s is not part of %s's history",
			cr.name(), r.g, r.g, c, r.g, c)
	} else if choice.uncontained != "" {
		msg += "; " + choice.uncontained
	}
	return nil, refuse(pitrarchive.TokenStopAmbiguous, "%s", msg)
}

// copyAgainstAnchor holds o, a replica copy of w that the anchor accepts,
// to what the anchor says of the history of w's origin s (review r5). The
// anchor proves Anchor(w) ⊆ pred_s(w); the plan certifies the whole of
// pred(o) as pred_s(w). The two differ when the copy's server did not
// follow s. A server never applies through replication a transaction that
// carries its own server_id, so an old primary that lost its last
// transactions and rejoined never gets them back; and a server that lost
// transactions issues their GTIDs again, to other transactions, which a
// replica that holds the lost ones skips. Every check here refuses
// (PITRStopAmbiguous); none accepts anything:
//
//   - Anchor(w) ∖ X ⊆ pred(o): s had executed all of the anchor before w,
//     so a faithful copy follows all of it;
//   - with a containment anchor R (none unless containmentAnchors is on),
//     S being the sample of R that holds w (its gtidExecuted, or its
//     confirmation): pred(o) ∖ X ⊆ S, because
//     pred_s(w) ⊆ S (S was read after w committed, and gtid_executed only
//     grows within one incarnation); and every g ∈ S ∖ Anchor(w) ∖ X has
//     the server_uuid of w: s executed any other after its promotion set
//     and before S was read, nothing orders it against w, and a copy
//     without it may be missing a predecessor;
//   - for the containment anchor R, and for every promotion record R of
//     s that carries a promotion set P and was observed more than δ before
//     w (a time anchor, or an earlier episode's record): every archived
//     copy of a g ∈ pred(o) ∖ X ∖ P under w's server_uuid was committed in
//     R's episode or later (episodeDates). By the proof of
//     containmentAnchor s committed such a g after P, so an archived copy
//     committed before the episode can have begun is another transaction:
//     the GTID was issued twice. The earlier records matter because the
//     anchor's own promotion set holds what s issued again in an earlier
//     episode. A record needs no fence check for this: it is not an
//     anchor, and a history that was replaced after it only makes the
//     check refuse.
//
// X waives a transaction here as it does for the anchor: an excluded
// transaction is not replayed, whichever server had it. That is no
// exception to N1: no record is set aside and none becomes an anchor.
func (p *planner) copyAgainstAnchor(c GTID, o occurrence, pred Set, choice *anchorChoice, anchor Set, q *file, qExec Set) error {
	uuid := c.UUID()
	observedAt := func(r *promoRef) string { return r.rec.ObservedAt.UTC().Format(microLayout) }
	// The parts of the anchor, so that the refusal names its evidence.
	type part struct {
		set  Set
		what string
	}
	var parts []part
	for _, r := range choice.time {
		parts = append(parts, part{r.set, fmt.Sprintf("the promotion record %s, observed at %s, before %s, holds them", r.name(), observedAt(r), c)})
	}
	cr := choice.contained
	if cr != nil {
		parts = append(parts, part{cr.proven, fmt.Sprintf("they are in the promotion set (promotionGtidExecuted) of the promotion record %s, observed at %s, which the server executed before the episode that committed %s", cr.name(), observedAt(cr), c)})
	}
	if q != nil {
		parts = append(parts, part{qExec, fmt.Sprintf("the origin row %s holds them before a transaction that precedes %s", q.row.Label(), c)})
	}
	for _, part := range parts {
		if missing := part.set.Subtract(p.X).Subtract(pred); !missing.IsEmpty() {
			return p.copyRefusal(c, o, missing, "Its copy in %s does not follow %s, which %s's origin had executed before it: %s. The copy's server applied them later or never (a server does not apply through replication a transaction that carries its own server_id, so an old primary that lost its last transactions and rejoined does not get them back), so the copy's predecessors are not %s's history",
				o.f.row.Label(), missing.bounded(256), c, part.what, c)
		}
	}
	if cr != nil {
		sample, sampled, which := cr.set, cr.rec.ObservedAt.UnixMicro(), "gtidExecuted"
		if !cr.set.Has(c) {
			sample, sampled, which = cr.confirmed, cr.rec.ConfirmedAt.UnixMicro(), "confirmation"
		}
		if over := pred.Subtract(p.X).Subtract(sample); !over.IsEmpty() {
			return p.copyRefusal(c, o, over, "Its copy in %s follows %s, which the %s of the promotion record %s lacks. That is the sample of the origin's gtid_executed that holds %s, read after it committed, so the origin had not executed them before %s: the copy's predecessors are not %s's history, or the archived %s is not the transaction that sample saw (the GTID was issued twice)",
				o.f.row.Label(), over.bounded(256), which, cr.name(), c, c, c, c)
		}
		loose := Set{}
		for sid, ivs := range sample.Subtract(anchor).Subtract(p.X) {
			if sidUUID(sid) != uuid {
				loose[sid] = ivs
			}
		}
		if !loose.IsEmpty() {
			return p.copyRefusal(c, o, loose, "The promotion record %s (observed at %s) would anchor it by containment, but its %s holds %s outside its promotion set (promotionGtidExecuted): the origin executed them after the promotion set and before that sample was read, and nothing orders them against %s. Its copy in %s does not follow them, so it may be missing predecessors of %s (the promotion-time binlog rotation was not acknowledged and they arrived after the sidecar's last read-only observation, or the server was still replicating when it committed %s)",
				cr.name(), observedAt(cr), which, loose.bounded(256), c, o.f.row.Label(), c, c)
		}
		if err := p.episodeDates(c, o, cr, pred.OfUUID(uuid).Subtract(cr.proven).Subtract(p.X), sampled); err != nil {
			return err
		}
	}
	for _, r := range choice.dating {
		if err := p.episodeDates(c, o, r, pred.OfUUID(uuid).Subtract(r.proven).Subtract(p.X), math.MaxInt64); err != nil {
			return err
		}
	}
	return nil
}

// copyRefusal is the PITRStopAmbiguous refusal for o, a replica copy of w
// that an anchor shows not to be w's history. exclude are the transactions
// whose exclusion lifts the refusal. A target at or before the last
// transaction of another server_uuid before o in its file has another
// witness.
func (p *planner) copyRefusal(c GTID, o occurrence, exclude Set, format string, args ...any) *Refusal {
	msg := fmt.Sprintf("the first transaction at or after %s, %s, is archived only as a replica copy. ", fmtMicros(p.T), c) +
		fmt.Sprintf(format, args...) +
		fmt.Sprintf(". Exclude %s (the restore then holds none of them)", exclude.bounded(512))
	if before := o.lastOtherBefore(c.UUID()); before > 0 {
		msg += fmt.Sprintf(", or choose a target at or before %s, the last transaction of another server before %s in %s", fmtMicros(before), c, o.f.row.Label())
	}
	return refuse(pitrarchive.TokenStopAmbiguous, "%s", msg)
}

// lastOtherBefore is the commit time (µs) of the newest transaction before
// o in its file that is not under uuid, or 0: a target at or before it has
// another witness than o's transaction.
func (o occurrence) lastOtherBefore(uuid string) int64 {
	before := int64(0)
	for j := 0; j < o.i; j++ {
		if o.f.gtid(j).UUID() != uuid {
			before = max(before, o.f.txn(j).OCT)
		}
	}
	return before
}

// episodeDates refuses when an archived copy of a member of own was
// committed before rec's episode can have begun (episodeStart), or after
// until (µs; math.MaxInt64 for no bound). own are predecessors of o, a
// replica copy of w, that rec's promotion set shows w's origin committed
// in rec's episode or later (copyAgainstAnchor).
//
// The cost is bounded by the hints. A row that may hold a member of own is
// not downloaded when its hinted commit times all lie inside the bounds;
// and once one copy outside them has been read from its bytes, a row whose
// hinted times all lie outside is counted from its hints. What is
// downloaded is a row that may hold a member of own and straddles a bound,
// or has no time hints: the files that were being written when the episode
// began, one per site as a rule. They are fetched and charged like every
// other row (in memory, or spilled). A row that cannot be read may hold
// the copy that decides, so it refuses.
func (p *planner) episodeDates(c GTID, o occurrence, rec *promoRef, own Set, until int64) error {
	if own.IsEmpty() {
		return nil
	}
	since := rec.episodeStart()
	var rows []*Row
	for _, r := range p.rows {
		if r.State == RowActive && r.Hints.mayMeet(own) {
			rows = append(rows, r)
		}
	}
	// Oldest first: a copy from before the episode is found before the
	// rows of the episode itself are read.
	sort.SliceStable(rows, func(i, j int) bool { return rows[i].Hints.lo() < rows[j].Hints.lo() })
	outside := Set{}
	var first *occurrence
	for _, r := range rows {
		f := p.files[r]
		if f == nil && r.Hints.timesKnown() {
			if r.Hints.lo() >= since && r.Hints.hi() <= until {
				continue
			}
			if first != nil && (r.Hints.hi() < since || r.Hints.lo() > until) {
				outside = outside.Union(r.Hints.GTIDs.Intersect(own))
				continue
			}
		}
		if f == nil {
			var err error
			if f, err = p.fetch(r); err != nil {
				if isUnreadable(err) {
					return refuse(pitrarchive.TokenArchiveObjectInvalid, "cannot read %v, which may hold a copy of %s (a predecessor of the witness candidate %s whose commit time must lie in the episode of the promotion record %s); %s",
						err, own.Intersect(r.Hints.GTIDs).bounded(256), c, rec.name(), lostObjectAdvice)
				}
				return err
			}
		}
		if !f.idx.GTIDs.Overlaps(own) {
			continue
		}
		for i, t := range f.idx.Txns {
			if t.OCT >= since && t.OCT <= until {
				continue
			}
			if g := f.gtid(i); own.Has(g) {
				outside.add(g)
				if first == nil {
					first = &occurrence{f, i}
				}
			}
		}
	}
	if first == nil {
		return nil
	}
	g, oct := first.f.gtid(first.i), first.f.txn(first.i).OCT
	when := fmt.Sprintf("was committed at %s, before that episode can have begun (%s, the record's probe interval of %s before it was observed)",
		fmtMicros(oct), fmtMicros(since), time.Duration(rec.rec.ProbeInterval))
	switch {
	case oct == 0:
		when = "carries no commit time, so nothing dates it inside that episode"
	case oct > until:
		when = fmt.Sprintf("was committed at %s, after the sample that holds %s (%s)", fmtMicros(oct), c, fmtMicros(until))
	}
	return p.copyRefusal(c, o, outside, "Its copy in %s follows %s, which is outside the promotion set (promotionGtidExecuted) of the promotion record %s (observed at %s), so the origin committed it in that record's episode or later. Yet the copy of %s in %s %s: the GTID was issued twice (a server that loses its last transactions issues their GTIDs again), and the archive holds a transaction that %s's origin did not have when it committed %s",
		o.f.row.Label(), g, rec.name(), rec.rec.ObservedAt.UTC().Format(microLayout), g, first.f.row.Label(), when, c, c)
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
	// time are the records whose gtidExecuted is in set: the anchors by
	// time.
	time []*promoRef
	// dating are the promotion records of s that carry a promotion set and
	// were observed more than δ before w, anchors or not: each dates the
	// transactions s committed from its episode on (copyAgainstAnchor).
	dating []*promoRef
	// lag is the earliest record of s that already holds w (D-53).
	lag *promoRef
	// pending is the earliest record of s without w observed within δ of
	// w, or after it: its order against w is unknown.
	pending *promoRef
	// shrunk says which anchors a fence refused, and why.
	shrunk []string
	// contradicted is a record that the time rule would take as an anchor
	// and whose confirmation holds w although the archived w was committed
	// after that confirmation: the witness refuses (anchoredTruth).
	contradicted *promoRef
	// sites are the sites whose records name s.
	sites map[string]bool
	// contained is the record whose promotion set anchors w by
	// containment (issue #269). uncontained says, when no record does and
	// w lies in a record's lag window, why that record could not. Both are
	// unset while containmentAnchors is off.
	contained   *promoRef
	uncontained string
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
//
// A record whose confirmation holds w, although the archived w was
// committed after that confirmation was read, is no anchor either: the
// archived transaction is not the one its episode committed. Such a w
// refuses (anchorChoice.contradicted): a record that is only set aside
// leaves the plan to the other copies and anchors, which may certify what
// they could not with the record.
//
// A record that this rule cannot order against w (it holds w, or it was
// observed within δ of w or after it) is no anchor by time: w is in its
// lag window (D-53). containmentAnchor may anchor w with that record's
// promotion set, when containmentAnchors is on; the planner ships with it
// off, and w then refuses.
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
		if pr.proven != nil && pr.rec.ObservedAt.UnixMicro()+p.delta < oct {
			out.dating = append(out.dating, pr)
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
		// R's confirmation is a later sample of R's episode. One that holds
		// w was read after w committed, by the sidecar of the pod that
		// committed it: on one clock. An archived w committed after it is
		// another transaction than the one the confirmation saw.
		if best.confirmed != nil && best.confirmed.Has(c) && oct > best.rec.ConfirmedAt.UnixMicro() {
			if out.contradicted == nil {
				out.contradicted = best
			}
			continue
		}
		// R and the site's later records share the sidecar's clock.
		if fc := p.fenceOf(site, best, c, srv, best.rec.ObservedAt.UnixMicro(), oct, 0); fc != nil {
			out.shrunk = append(out.shrunk, fc.explain("the promotion records "+name+" are not anchors", c))
			continue
		}
		out.set = out.set.Union(best.set)
		out.names = append(out.names, name)
		out.time = append(out.time, best)
	}
	out.contained, out.uncontained = p.containmentAnchor(c, srv, oct, out.pending)
	if out.contained != nil {
		out.set = out.set.Union(out.contained.proven)
		out.names = append(out.names, out.contained.name())
	}
	return out
}

// containmentAnchors says whether the promotion set of a promotion record
// may anchor a replica-copy witness by containment (containmentAnchor,
// issue #269). It is off, and only tests turn it on: the planner anchors
// with promotion records by time alone, as it did before that issue, and a
// witness in a record's lag window refuses.
//
// Why it is off. The containment proof orders a promotion set and the
// transaction w that the sidecar sampled. A plan needs more: the archived
// transaction named w, and the archived transactions its copy follows under
// w's server_uuid, must be the ones that episode's server had. They are not
// when a server_uuid issues a GTID twice. A server that loses transactions
// its replica already holds (a crash with an unsynced binlog, a volume that
// comes back older) and rejoins by replication never gets them back (a
// server does not apply a transaction that carries its own server_id), and
// once it is promoted again it issues their numbers to other transactions:
// the first commits of its next episode, which is the lag window. The
// checks of containmentAnchor and copyAgainstAnchor date both issues
// against the record, and two classes of wrong certification through a
// containment anchor are still known, each pinned by a test: both issues
// committed inside one episode window
// (TestWitnessContainmentResidualIssuedTwice), and a GTID issued again one
// episode back whose record carries no promotion set
// (TestWitnessLostTailReissuedInAnEarlierEpisode). The time rule has the
// same root cause from δ after a record on (a record without a promotion
// set dates nothing); this anchor would extend it into the window where a
// reissue always falls.
//
// Before it is turned on, both must hold:
//
//   - the operator no longer rejoins by replication an old primary that
//     lacks transactions of its own server_uuid that the new primary holds
//     (it is recloned instead, and a clone gives it a new server_uuid,
//     issue #265), so that a server_uuid cannot issue a GTID twice without
//     a clone;
//   - the rule, with that guarantee, has passed a fresh adversarial review.
//
// While it is off, containmentAnchor selects nothing and says nothing, so
// no plan is certified through a promotion set or a confirmation. What
// else reads that evidence only refuses, and stays active: the dating of a
// copy's predecessors against every promotion set observed more than δ
// before the witness (copyAgainstAnchor), and the witness that the
// confirmation of its time anchor contradicts (anchorRecords). The other
// checks of containmentAnchor (the episode window, the earliest holder,
// the record order, the same server on two sites, the record cap, the
// wider fences) only ever withdraw a containment anchor, so they have
// nothing to withdraw.
var containmentAnchors = false

// EnableContainmentAnchorsForTest turns the containment anchor on until
// the test t ends (see containmentAnchors for why nothing else may). It is
// here, and not in a _test.go file, only so that the property sweep in
// test/component can cover the rule; TestContainmentAnchorsAreOff fails
// when a file that is not a test names it. The switch is one variable:
// call it from a test that plans on no other goroutine while t runs.
func EnableContainmentAnchorsForTest(t interface {
	Helper()
	Cleanup(func())
}) {
	t.Helper()
	old := containmentAnchors
	containmentAnchors = true
	t.Cleanup(func() { containmentAnchors = old })
}

// containmentAnchor finds the promotion record of s = (uuid(w), srv) whose
// promotion set P anchors w by containment (issue #269), or says why the
// record in whose lag window w lies does not. pending is anchorRecords'
// pending record. With containmentAnchors off, which is how the planner
// ships, it finds none and gives no reason.
//
// A promotion record R carries P ⊆ R.gtidExecuted (recordEvidence): every
// member of P was executed by R's server incarnation before any
// transaction under the server's own server_uuid outside the read-only
// observation G_ro that directly preceded R's episode, and G_ro ⊆ P. The
// record may also carry a confirmation: a later sample C ⊇ R.gtidExecuted
// of the same incarnation and the same continuously writable episode. So
//
//	w ∉ P  and  (w ∈ R.gtidExecuted  or  w ∈ C)   ⇒   P ⊆ pred_s(w):
//
// w is in a sample of the incarnation the sidecar observed without a gap
// from G_ro on, so that incarnation executed it, and it did so by
// committing it: a server never applies an event with its own server_id
// through replication (replicate_same_server_id is off, as the origin
// test Srv == WriterServerID already assumes). w is outside G_ro, so it
// was committed after G_ro was read, and outside the Previous_gtids of
// the binlog file the rest of P comes from, so after that file was
// opened (internal/sidecar promotionSet has the derivation). Every member
// of P was executed before one of the two. No clock orders R and w here.
// It covers the two cases the time rule refuses: w committed before R
// was observed ("lag": R holds w) and w committed within δ after it
// ("pending": the confirmation holds w).
//
// The proof is about the transaction the sidecar sampled and about P. It
// says nothing of the rest of the copy's predecessors (copyAgainstAnchor
// holds the copy to the record for those), and the sets cannot show that
// the archived transaction named w is the one that episode committed: a
// server that loses a transaction (a crash with an unsynced binlog, a
// reset, a restored data directory) issues its GTID again. That is what
// records and clocks are for, as for every other anchor, and each check
// below only ever withdraws the anchor:
//
//   - R is the earliest record of s, of any kind and on any site, that
//     holds w (in its sample or its confirmation). An earlier one means
//     the GTID was issued before R's episode;
//   - every record of s is on R's site. One sidecar observes a server
//     without a gap; the records of two sites' sidecars for one
//     (server_uuid, server_id) are not one such observation, whichever
//     holds w and whenever they were taken. server_id is assigned per
//     site, so no manifest the operator's pods write has this;
//   - w's commit time lies inside the episode as the record dates it. The
//     record's times and w's commit time are readings of one clock: the
//     sidecar probes the mysqld of its own pod (127.0.0.1), a pod runs on
//     one node, and the sidecar process that took G_ro is the one that
//     took the sample that holds w (a restarted sidecar has no G_ro and
//     writes no P). So no clock-skew margin applies, and the bounds are
//     the episode's own: for a w in R's sample, R.observedAt −
//     R.probeInterval ≤ oct(w) ≤ R.observedAt (the episode began after
//     the read of the sidecar's previous observation, and the sample was
//     read after w committed); for a w only the confirmation holds,
//     R.observedAt − PromotionProbeSpread ≤ oct(w) ≤ R.confirmedAt (R's
//     sample, read at most that long before R.observedAt, lacks w). A
//     reader does not rely on the writer's confirmation delay for the
//     upper bound;
//   - no fence (fenceOf) from oct(w) − δ on, which is at or before
//     R.observedAt: another record of R's site that is not provably
//     before w (observed at or after oct(w) − δ) and not provably after it
//     (observed up to oct(w) + its probe interval + δ), or another
//     server's promotion record around w that lacks w. Later records
//     fence as for a time anchor: the archived w may belong to a later
//     incarnation whose records are in the window, or missing (review
//     F062). Earlier records fence here too: the archived w may have been
//     committed before the change such a record reports, by an earlier
//     incarnation than the one P describes;
//   - R is not the oldest record of a manifest that is at its cap
//     (MaxPromotionRecords): the cap drops the oldest records, and for
//     this rule a dropped record could be such an earlier fence or holder;
//   - the records of R's site are in the manifest in the order they were
//     observed, as far as R is concerned: a sidecar appends them as it
//     observes them, so a record written after R and observed before it
//     (or the reverse) means its clock was set back in between, and then
//     neither the window nor "earliest" nor "oldest" above means what it
//     says.
//
// Residual risk: a GTID that is issued twice with both commit times inside
// the window above is not told apart. The server must lose the transaction
// and commit another under its GTID between two consecutive role probes
// that both pass the sidecar's incarnation check (for a w in R's sample:
// between the last read-only probe and R's), which is the unobserved
// incarnation change fenceOf names. The earlier issue's commit time is a
// reading of the clock of the node its server ran on then, so across a pod
// move the window is that wide to within the two nodes' skew.
//
// X plays no part (N1): excluding a transaction neither sets a record
// aside nor makes one an anchor.
func (p *planner) containmentAnchor(w GTID, srv uint32, oct int64, pending *promoRef) (*promoRef, string) {
	if !containmentAnchors {
		return nil, ""
	}
	uuid := w.UUID()
	var holder *promoRef
	for i := range p.fences {
		f := &p.fences[i]
		if f.rec.ServerUUID != uuid || f.rec.ServerID != srv || !f.holds(w) {
			continue
		}
		if holder == nil || f.rec.ObservedAt.Before(holder.rec.ObservedAt) {
			holder = f
		}
	}
	// The record in whose lag window w lies: the earliest holder when it
	// is the kind of record the time rule cannot order against w, else the
	// pending record, which lacks w and has no confirmation that holds it.
	r := pending
	if holder != nil && holder.rec.Kind == pitrarchive.RecordPromotion &&
		(holder.set.Has(w) || holder.rec.ObservedAt.UnixMicro()+p.delta >= oct) {
		r = holder
	}
	if r == nil || (r != holder && holder != nil && !holder.rec.ObservedAt.After(r.rec.ObservedAt)) {
		// No record of s is in question, or w was already held before the
		// pending record: w is no transaction of that record's episode.
		return nil, ""
	}
	at := r.rec.ObservedAt.UTC().Format(microLayout)
	no := func(format string, args ...any) (*promoRef, string) {
		return nil, fmt.Sprintf("the promotion record %s (observed at %s) does not anchor %s by containment: ", r.name(), at, w) + fmt.Sprintf(format, args...)
	}
	observed := r.rec.ObservedAt.UnixMicro()
	pi := time.Duration(r.rec.ProbeInterval).Microseconds()
	if r != holder || r.proven == nil {
		// r does not hold w outside a promotion set. Say why only when w
		// lies near r's episode, with the margin of the refusal that
		// precedes this clause: a record observed long after w is not the
		// record of w's episode, with or without this evidence.
		near := pi > 0 && oct > observed-pi-p.delta
		switch {
		case r.proven != nil && r.proven.Has(w):
			return nil, ""
		case r.unusable != "" && (near || pi <= 0):
			return no("%s", r.unusable)
		case !near:
			return nil, ""
		case r.proven == nil:
			return no("it carries no promotion set (promotionGtidExecuted): the sidecar did not observe the server read-only directly before the episode, could not read the local binlog headers in time, or predates this evidence")
		case r.confirmed == nil:
			return no("%s is not in it and it has no confirmation (confirmedAt): the sidecar or the server stopped, or the episode ended, within about %s of the record", w, pitrarchive.PromotionConfirmDelay)
		}
		return no("neither it nor its confirmation at %s holds %s", r.rec.ConfirmedAt.UTC().Format(microLayout), w)
	}
	if r.proven.Has(w) {
		// Executed before r's episode began: not a transaction of it.
		return nil, ""
	}
	records, oldest := 0, true
	for i := range p.fences {
		f := &p.fences[i]
		if f.site == r.site {
			records++
			oldest = oldest && !f.rec.ObservedAt.Before(r.rec.ObservedAt)
			if (f.index > r.index && f.rec.ObservedAt.Before(r.rec.ObservedAt)) || (f.index < r.index && f.rec.ObservedAt.After(r.rec.ObservedAt)) {
				return no("site %s's records are not in the order they were observed (%s was observed at %s): the sidecar's clock was set back between them, so the record's time does not date its episode", r.site, f.name(), f.rec.ObservedAt.UTC().Format(microLayout))
			}
			continue
		}
		if f.rec.ServerUUID == uuid && f.rec.ServerID == srv {
			return no("%s, observed at %s, is a record of the same server (server_uuid and server_id) on another site: the two sites' sidecars did not observe that server as one, so the promotion set does not date the episode that committed %s", f.name(), f.rec.ObservedAt.UTC().Format(microLayout), w)
		}
	}
	if oldest && records >= pitrarchive.MaxPromotionRecords {
		return no("it is the oldest of site %s's %d records, the most a manifest keeps, so an earlier record that would set it aside may have been dropped", r.site, records)
	}
	// The episode window: no margin, the record and w are on one clock.
	if r.set.Has(w) {
		if oct < observed-pi {
			return no("%s was committed at %s, more than the record's probe interval (%s) before the record was observed, so before its episode can have begun: the archived transaction is not the one that episode committed (the GTID was issued twice)",
				w, fmtMicros(oct), time.Duration(r.rec.ProbeInterval))
		}
		if oct > observed {
			return no("%s was committed at %s, after the record that holds it was observed: the archived transaction is not the one the record saw (the GTID was issued twice, or the server's clock was set back in between)",
				w, fmtMicros(oct))
		}
	} else {
		if oct < observed-pitrarchive.PromotionProbeSpread.Microseconds() {
			return no("%s was committed at %s, more than %s before the record was observed, yet the record does not hold it and its confirmation does: the archived transaction is not the one the confirmation saw (the GTID was issued twice)",
				w, fmtMicros(oct), pitrarchive.PromotionProbeSpread)
		}
		if confirmed := r.rec.ConfirmedAt.UnixMicro(); oct > confirmed {
			return no("%s was committed at %s, after the confirmation that holds it was observed (%s): the archived transaction is not the one the confirmation saw (the GTID was issued twice, or the server's clock was set back in between)",
				w, fmtMicros(oct), fmtMicros(confirmed))
		}
	}
	if fc := p.fenceOf(r.site, r, w, srv, min(observed, oct-p.delta), oct, 0); fc != nil {
		fc.earlier = !fc.role && fc.rec.rec.ObservedAt.Before(r.rec.ObservedAt)
		return nil, fc.explain(fmt.Sprintf("the promotion record %s (observed at %s) does not anchor %s by containment", r.name(), at, w), w)
	}
	return r, ""
}

// episodeStart is the earliest time (µs) the episode of a promotion record
// that carries a promotion set can have begun: its probe interval before
// it was observed, on the clock of the record (recordEvidence: such a
// record has a probe interval).
func (pr *promoRef) episodeStart() int64 {
	return pr.rec.ObservedAt.UnixMicro() - time.Duration(pr.rec.ProbeInterval).Microseconds()
}

func (pr *promoRef) name() string { return fmt.Sprintf("%s#%d", pr.site, pr.index) }

// fence is a record that keeps an anchor from proving s's history at w.
type fence struct {
	rec *promoRef
	// role: another server's promotion record (F062), not a record of
	// the anchor's own site.
	role bool
	// earlier: a record of the anchor's site observed before the anchor
	// record (containmentAnchor only).
	earlier bool
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
	if f.earlier {
		return fmt.Sprintf("%s, because an earlier record of site %s, %s (%s observed at %s, reporting a change that happened %s before it), is not provably before %s: %s may have been committed before that change, by an earlier episode or server incarnation than the one the promotion set describes",
			what, f.rec.site, f.rec.name(), kind, at, window, w, w)
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
