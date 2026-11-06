package pitrplan

import (
	"fmt"
	"math"
	"sort"
	"strings"

	"github.com/shipstream/bloodraven/internal/pitrarchive"
)

// dWindow is L4 (D-11, D-13 (2)): every archived transaction committed in
// (maxOct(B), T) is in Λ ∪ B, or excluded.
//
// D here is {g : maxOct(B) < oct(g) < T, g ∉ Λ ∪ B}, and the check is
// D ⊆ X; coverage.json records D (deviation: §4.1 writes D with "∉ X" and
// then requires D ⊆ X, which only holds for D = ∅; restore.py's R3 checks
// divergent ⊆ excluded, so D is reported with its excluded members).
func (p *planner) dWindow() error {
	// Rows whose hinted GTIDs are all in Λ ∪ B hold no member of D. Rows
	// holding excluded transactions are read too, so coverage.json reports
	// D in full (deviation: §4.5 skips rows ⊆ Λ ∪ B ∪ X, which would leave
	// excluded members of D unreported).
	known := p.lambda.Union(p.B)
	floor := p.maxOctB
	unread := Set{}
	for _, r := range p.rows {
		if r.Hints.hi() <= floor || r.Hints.lo() >= p.T {
			continue
		}
		if r.Hints.Known && known.Contains(r.Hints.GTIDs) {
			continue
		}
		if _, err := p.fetch(r); err != nil {
			if !isUnreadable(err) {
				return err
			}
			if r.State == RowQuarantined && r.Hints.Known {
				// Fail closed: count its hinted transactions (§4.5 L4).
				unread = unread.Union(r.Hints.GTIDs.Subtract(p.lambda).Subtract(p.B))
				continue
			}
			if r.State == RowQuarantined {
				return refuse(pitrarchive.TokenArchiveNamespaceReuse, "cannot verify %v, a quarantined row with no complete hints that may hold transactions committed before the target", err)
			}
			return refuse(pitrarchive.TokenArchiveObjectInvalid, "cannot read %v, which may hold transactions committed before the target; quarantine the row with bloodraven pitr-quarantine if its object is lost", err)
		}
	}
	D := unread.Clone()
	var lo, hi int64 = math.MaxInt64, math.MinInt64
	sites := map[string]bool{}
	for _, f := range p.order {
		for i, t := range f.idx.Txns {
			if t.OCT <= floor || t.OCT >= p.T {
				continue
			}
			g := f.gtid(i)
			if p.lambda.Has(g) || p.B.Has(g) {
				continue
			}
			D.add(g)
			if !p.X.Has(g) {
				lo, hi = min(lo, t.OCT), max(hi, t.OCT)
				sites[f.row.Site] = true
			}
		}
	}
	p.plan.Divergent = D
	bad := D.Subtract(p.X)
	if bad.IsEmpty() {
		return nil
	}
	// Skew: a member of D whose own history includes w (D-13 (2)).
	for _, f := range p.order {
		pw := p.witnessPos(f)
		for i, t := range f.idx.Txns {
			g := f.gtid(i)
			if !bad.Has(g) {
				continue
			}
			if pw == -1 || (pw >= 0 && pw < i) {
				return refuse(pitrarchive.TokenStopAmbiguous, "%s (committed %s, before the target) is outside the history of %s, yet its own history includes %s (committed %s): the servers' clocks disagree; choose a target at or before %s or after %s",
					g, fmtMicros(t.OCT), p.w.GTID, p.w.GTID, fmtMicros(p.w.CommitTime), fmtMicros(t.OCT), fmtMicros(p.w.CommitTime))
			}
		}
	}
	var siteList []string
	for s := range sites {
		siteList = append(siteList, s)
	}
	sort.Strings(siteList)
	if len(siteList) == 0 {
		siteList = []string{"unreadable quarantined rows"}
	}
	window := ""
	if lo != math.MaxInt64 {
		window = ", " + fmtMicros(lo) + "…" + fmtMicros(hi)
	}
	msg := strings.Builder{}
	msg.WriteString(fmt.Sprintf("%d archived transactions committed between the backup's newest transaction (%s) and %s are not in the history of %s (site %s, committed %s): %s (sites %s%s). They were lost at a failover, written on a replica (errant), or belong to another branch. To restore %s's history, add them to excludeGtids",
		bad.Count(), fmtMicros(p.plan.BackupNewestCommit), fmtMicros(p.T), p.w.GTID, p.w.Row.Site, fmtMicros(p.w.CommitTime), bad.bounded(1024), strings.Join(siteList, ","), window, p.w.GTID))
	if lo != math.MaxInt64 {
		msg.WriteString(fmt.Sprintf(", or choose a target at or before %s, or use a backup taken after %s", fmtMicros(lo), fmtMicros(hi)))
	}
	if alt := p.witnessBranch(bad); !alt.IsEmpty() {
		msg.WriteString(fmt.Sprintf(". To keep the other history instead, exclude %s's branch: %s", p.w.GTID, alt.bounded(512)))
	}
	return refuse(pitrarchive.TokenArchiveDivergentHistory, "%s", msg.String())
}

// witnessPos is the position of w in f: -1 when w ∈ prev(f), its index when
// f holds it, and -2 when f's history does not include w.
func (p *planner) witnessPos(f *file) int {
	if !f.posWDone {
		f.posWDone = true
		switch {
		case f.prev.Has(p.w.GTID):
			f.posW = -1
		default:
			f.posW = position(f, p.w.GTID)
			if f.posW < 0 {
				f.posW = -2
			}
		}
	}
	return f.posW
}

// witnessBranch is the set to exclude to keep D's history instead of w's:
// Λ's transactions outside the history of D's earliest member, plus the
// archived later transactions of those source UUIDs.
func (p *planner) witnessBranch(bad Set) Set {
	var df *file
	di := -1
	var best int64 = math.MaxInt64
	for _, f := range p.order {
		for i, t := range f.idx.Txns {
			if t.OCT < best && bad.Has(f.gtid(i)) {
				best, df, di = t.OCT, f, i
			}
		}
	}
	if df == nil {
		return Set{}
	}
	other := df.prev.Clone()
	for j := 0; j <= di; j++ {
		other.add(df.gtid(j))
	}
	only := p.lambda.Subtract(other).Subtract(p.X)
	out := only.Clone()
	uuids := only.UUIDs()
	for _, f := range p.order {
		for i, t := range f.idx.Txns {
			g := f.gtid(i)
			if t.OCT >= p.T && uuids[g.UUID()] && !other.Has(g) {
				out.add(g)
			}
		}
	}
	return out
}
