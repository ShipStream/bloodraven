package pitrplan

import (
	"fmt"
	"math"

	"github.com/shipstream/bloodraven/internal/pitrarchive"
)

// lineageL1: the backup is on the target history (§4.5).
func (p *planner) lineageL1() error {
	if out := p.B.Subtract(p.lambda); !out.IsEmpty() {
		// Timestamp mode only (latest mode has its own baseline check), so
		// the witness is set.
		return refuse(pitrarchive.TokenBackupNotOnLineage, "the backup holds %s, which is not in the history of %s; use a backup taken on that history, or a target on the backup's history",
			out.bounded(1024), p.w.GTID)
	}
	return nil
}

// lineageL2 is the injected-empty check (O2-7): a GTID of B whose archived
// copy is empty and was written by another server than the dumped one.
// Quarantined rows are evidence here too (never data): a quarantined copy
// that shows a member of B empty is a history in which it had no effects
// (review F008). An unreadable quarantined row that may hold such a copy
// refuses, rather than leaning on the identity waiver.
func (p *planner) lineageL2() error {
	if p.B.IsEmpty() {
		return nil
	}
	for _, r := range p.rows {
		if r.Hints.Known && !r.Hints.Empty.Overlaps(p.B) {
			continue
		}
		if _, err := p.fetch(r); err != nil {
			if !isUnreadable(err) {
				return err
			}
			if r.State == RowQuarantined {
				what := "it has no complete hints"
				if r.Hints.Known {
					what = "its hints hold empty copies of " + r.Hints.Empty.Intersect(p.B).bounded(256)
				}
				return refuse(pitrarchive.TokenArchiveNamespaceReuse, "cannot verify %v, a quarantined row that may hold an empty copy of a backup transaction (%s), whose effects the backup may hold; restore its object (in an archive copy, copy it again from the source archive), or use a backup that does not hold those transactions", err, what)
			}
			return refuse(pitrarchive.TokenArchiveObjectInvalid, "cannot read %v, which may hold an empty copy of a backup transaction; %s", err, lostObjectAdvice)
		}
	}
	dumped := p.req.Dump.Stamp.ServerID
	for _, f := range p.order {
		if !f.idx.EmptyGTIDs.Overlaps(p.B) {
			continue
		}
		for i, t := range f.idx.Txns {
			if !t.Empty() || t.Srv == dumped {
				continue
			}
			g := f.gtid(i)
			if !p.B.Has(g) {
				continue
			}
			injector := p.siteOfServer(t.Srv)
			if f.idx.WriterServerID == t.Srv {
				injector = f.row.Site
			}
			return refuse(pitrarchive.TokenBackupNotOnLineage, "the history holds an injected empty transaction under %s (in %s, server_id %d), but the backup was taken on %s (server_id %d), which may hold %s's effects; take the backup on the site that injected it, %s (server_id %d)",
				g, f.row.Label(), t.Srv, p.req.Dump.Stamp.Site, dumped, g, injector, t.Srv)
		}
	}
	return nil
}

// siteOfServer names the site whose promotion records carry server_id.
func (p *planner) siteOfServer(srv uint32) string {
	for _, pr := range p.promos {
		if pr.rec.ServerID == srv {
			return pr.site
		}
	}
	return fmt.Sprintf("server_id %d", srv)
}

// siteOfUUID names the site whose promotion records carry uuid.
func (p *planner) siteOfUUID(uuid string) string {
	for _, pr := range p.promos {
		if pr.rec.ServerUUID == uuid {
			return pr.site
		}
	}
	return "uuid " + uuid
}

// fetchReplayRows downloads R_P: the active, unmarked rows that may hold P.
// An unreadable one is left out of the search set (its transactions then
// count as missing).
func (p *planner) fetchReplayRows() error {
	P := p.plan.Replay
	if P.IsEmpty() {
		return nil
	}
	for _, r := range p.rows {
		if r.State != RowActive || p.isMarked(r.Site) || !r.Hints.mayMeet(P) {
			continue
		}
		if _, err := p.fetch(r); err != nil && !isUnreadable(err) {
			return err
		}
	}
	return nil
}

// lineageL3: no member of P committed at or after T (D-13 (1)).
func (p *planner) lineageL3() error {
	P := p.plan.Replay
	late := Set{}
	lo, hi := int64(math.MaxInt64), int64(math.MinInt64)
	var first GTID
	var firstOCT int64
	for _, f := range p.order {
		if !f.idx.GTIDs.Overlaps(P) {
			continue
		}
		for i, t := range f.idx.Txns {
			if t.OCT < p.T {
				continue
			}
			g := f.gtid(i)
			if !P.Has(g) {
				continue
			}
			if late.IsEmpty() || t.OCT < firstOCT {
				first, firstOCT = g, t.OCT
			}
			late.add(g)
			lo, hi = min(lo, t.OCT), max(hi, t.OCT)
		}
	}
	if late.IsEmpty() {
		return nil
	}
	lo = min(lo, p.w.CommitTime)
	hi = max(hi, p.w.CommitTime)
	return refuse(pitrarchive.TokenStopAmbiguous, "%s (committed %s) precedes %s (committed %s) on the history, although it committed at or after the target %s (%d such transactions: %s); the servers' clocks disagree; choose a target ≤ %s or > %s",
		first, fmtMicros(firstOCT), p.w.GTID, fmtMicros(p.w.CommitTime), fmtMicros(p.T), late.Count(), late.bounded(512), fmtMicros(lo), fmtMicros(hi))
}

// checkL5: no replayed transaction is XA or otherwise unsupported.
func (p *planner) checkL5(res *searchResult) error {
	if res == nil {
		return nil
	}
	for _, s := range res.segments {
		f := res.files[s.file]
		for i := s.start; i < s.end; i++ {
			t := f.txn(i)
			if t.Flags&(TxnXA|TxnUnsupported) == 0 || p.X.Has(f.gtid(i)) {
				continue
			}
			kind := "an XA transaction"
			if t.Flags&TxnUnsupported != 0 {
				kind = "a transaction with an event the planner cannot certify (LOAD DATA, an incident, or an unknown event type)"
			}
			return refuse(pitrarchive.TokenReplayUnsupported, "the replay needs %s, %s in %s; choose a target before %s, or use a backup taken after it",
				f.gtid(i), kind, f.row.Label(), fmtMicros(t.OCT))
		}
	}
	return nil
}
