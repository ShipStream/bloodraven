package pitrplan

import (
	"math"
	"sort"
	"time"

	"github.com/shipstream/bloodraven/internal/pitrarchive"
)

// frontier is the B-frontier pass (§4.3, D-44, D-49, D-52): it proves
// maxOct(B) from bytes, then checks the dump against reissue and against
// the target.
//
// The descending search stops on the next row's hinted hmax, and a hint is
// unverified until its row is downloaded: a row whose hint understates its
// newest member of B would hide a commit after T (review F005). So
// maxOct(B) is proven only when every active row meeting B was downloaded;
// a search that stopped on a hint leaves it a lower bound, and the D-52
// server-clock bound applies.
func (p *planner) frontier() error {
	var rows []*Row
	for _, r := range p.rows {
		if r.State == RowActive && !p.B.IsEmpty() && r.Hints.mayMeet(p.B) {
			rows = append(rows, r)
		}
	}
	// Descending hmax; rows with unknown hints first.
	sort.SliceStable(rows, func(i, j int) bool { return rows[i].Hints.hi() > rows[j].Hints.hi() })
	missing := Set{}
	unreadable := false
	hinted := 0 // rows meeting B left unread on their hint
	for k, r := range rows {
		if p.maxOctB != math.MinInt64 && r.Hints.hi() <= p.maxOctB {
			hinted = len(rows) - k
			break
		}
		f, err := p.fetch(r)
		if err != nil {
			if isUnreadable(err) {
				// A row whose object is lost: its members of B are
				// unseen, so maxOct(B) is only a lower bound (D-52).
				unreadable = true
				missing = missing.Union(r.Hints.GTIDs.Intersect(p.B))
				continue
			}
			return err
		}
		for i, t := range f.idx.Txns {
			if t.OCT > p.maxOctB && p.B.Has(f.gtid(i)) {
				p.maxOctB, p.maxOctBGTID, p.maxOctBFile = t.OCT, f.gtid(i), f
			}
		}
	}
	if p.maxOctBFile != nil {
		p.used[p.maxOctBFile.row] = true
	}

	// Proven versus missing: for each source identifier, a member of B
	// that a retained row's hprev holds, no retained row's hgtids holds,
	// and whose GNO is above the largest archived member, may have been
	// committed later than any visible one.
	archived, mentioned := Set{}, Set{}
	for _, r := range p.rows {
		if r.State != RowActive {
			continue
		}
		gtids, prev := r.Hints.GTIDs, r.Hints.Previous
		if f := p.files[r]; f != nil {
			gtids, prev = f.idx.GTIDs, f.prev
		} else if !r.Hints.Known {
			continue
		}
		archived = archived.Union(gtids.Intersect(p.B))
		mentioned = mentioned.Union(prev.Intersect(p.B))
	}
	for sid, ivs := range mentioned.Subtract(archived) {
		top := int64(0)
		if a := archived[sid]; len(a) > 0 {
			top = a[len(a)-1].End
		}
		for _, iv := range ivs {
			if iv.End > top {
				missing[sid] = append(missing[sid], Interval{max(iv.Start, top+1), iv.End})
			}
		}
	}
	missing = missing.normalized()
	pl := p.plan
	pl.BackupFrontierProven = missing.IsEmpty() && !unreadable && hinted == 0
	if p.maxOctB != math.MinInt64 {
		pl.BackupNewestCommit = p.maxOctB
	}

	// D-49 against the dumped server's own clock (P5): commit times are
	// server clocks, so the Job's clock (@.done.json end) is no bound for
	// them; δ covers the skew between the dumped server and B's other
	// origins. The newest one is named; ingest also records any other
	// downloaded copy (p.reissue), which the closure reports.
	if p.maxOctB != math.MinInt64 && p.maxOctB > p.serverEnd+p.delta {
		return p.reissueRefusal(p.maxOctBGTID, p.maxOctBFile, p.maxOctB)
	}
	if p.reissue != nil {
		return p.reissue
	}
	if p.req.Mode != ModeTimestamp {
		return nil
	}
	end := p.req.Dump.End.Time
	ceil := end.Truncate(time.Second)
	if !ceil.Equal(end) {
		ceil = ceil.Add(time.Second)
	}
	if p.T < ceil.UnixMicro() {
		return refuse(pitrarchive.TokenBackupAfterTarget, "the dump finished at %s, after the target %s; use a backup that completed before %s",
			p.req.Dump.End, fmtMicros(p.T), fmtMicros(p.T))
	}
	if p.maxOctB != math.MinInt64 && p.maxOctB >= p.T {
		return refuse(pitrarchive.TokenBackupAfterTarget, "the backup already contains %s, committed %s ≥ the target %s; use a backup that completed before the target",
			p.maxOctBGTID, fmtMicros(p.maxOctB), fmtMicros(p.T))
	}
	if !pl.BackupFrontierProven {
		bound := p.req.Dump.ServerClock.End.Add(time.Second)
		if p.T < bound.UnixMicro() {
			if missing.IsEmpty() && !unreadable {
				return refuse(pitrarchive.TokenBackupAfterTarget, "this backup's newest commit time is proven only up to %s; %d archived rows holding its transactions were not read, and their manifest hints are unverified; the dumped server's clock read %s when the dump finished; use a target at or after %s",
					fmtMicros(p.maxOctB), hinted, p.req.Dump.ServerClock.End, fmtMicros(bound.UnixMicro()))
			}
			what := missing.bounded(512)
			if what == "" {
				what = "an unreadable row's transactions"
			}
			return refuse(pitrarchive.TokenBackupAfterTarget, "the archive no longer holds every transaction of this backup (%s), so its newest commit time is unknown; the dumped server's clock read %s when the dump finished; use a target at or after %s, or a backup whose transactions are archived",
				what, p.req.Dump.ServerClock.End, fmtMicros(bound.UnixMicro()))
		}
	}
	return nil
}

// reissueRefusal is D-49: g ∈ B has a copy committed after the dump
// finished (by the dumped server's clock, plus δ).
func (p *planner) reissueRefusal(g GTID, f *file, oct int64) *Refusal {
	return refuse(pitrarchive.TokenArchiveNamespaceReuse, "the archive says %s (site %s, %s) committed at %s, after this dump finished (the dumped server's clock read %s at the dump's end, and the clock-skew margin is %s); the GTID was reissued (a reclone kept the server_uuid); start a new archive prefix and take a full backup",
		g, f.row.Site, f.row.Name, fmtMicros(oct), p.req.Dump.ServerClock.End, time.Duration(p.delta)*time.Microsecond)
}
