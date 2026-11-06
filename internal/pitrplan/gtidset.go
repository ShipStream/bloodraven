package pitrplan

import (
	"fmt"
	"sort"
	"strconv"
	"strings"

	internalmysql "github.com/shipstream/bloodraven/internal/mysql"
)

// GTID is one transaction identifier. SID is the source identifier: the
// server UUID, or "uuid:tag" for a tagged GTID (tags are lower-case, as
// internal/mysql normalizes them). Tagged and untagged GTIDs of one UUID
// have separate GNO sequences.
type GTID struct {
	SID string
	GNO int64
}

// String is MySQL's spelling, "uuid:n" or "uuid:tag:n".
func (g GTID) String() string { return g.SID + ":" + strconv.FormatInt(g.GNO, 10) }

// UUID is sid(g) in the design: the UUID part, without any tag.
func (g GTID) UUID() string { return sidUUID(g.SID) }

// Less orders GTIDs by source identifier, then numerically by GNO.
func (g GTID) Less(o GTID) bool {
	if g.SID != o.SID {
		return g.SID < o.SID
	}
	return g.GNO < o.GNO
}

func sidUUID(sid string) string {
	u, _, _ := strings.Cut(sid, ":")
	return u
}

// Interval is an inclusive GNO range.
type Interval struct{ Start, End int64 }

// Set is a canonical GTID set: per source identifier, sorted, merged,
// non-empty interval lists. The zero value (nil) is the empty set. Methods
// that return a Set never alias their receiver's slices, so Sets are values;
// only add mutates, and only on a Set built locally.
type Set map[string][]Interval

// ParseSet parses a MySQL GTID set (tags included) into canonical form.
func ParseSet(s string) (Set, error) {
	raw, err := internalmysql.ParseGTIDSet(s)
	if err != nil {
		return nil, err
	}
	return FromMySQL(raw), nil
}

// MustParseSet is ParseSet for constants in tests and fixtures.
func MustParseSet(s string) Set {
	set, err := ParseSet(s)
	if err != nil {
		panic(err)
	}
	return set
}

// FromMySQL normalizes an internal/mysql set.
func FromMySQL(raw internalmysql.GTIDSet) Set {
	out := make(Set, len(raw))
	for sid, ivs := range raw {
		if n := normalizeIntervals(ivs); len(n) > 0 {
			out[sid] = n
		}
	}
	return out
}

// ParseUserSet parses a GTID set a user wrote (excludeGtids) the way MySQL
// reads it: UUIDs are case-insensitive, so they are lower-cased, as MySQL
// prints them, the binlogs carry them and internal/mysql already does for
// tags. Without it an upper-case excludeGtids would never match the
// archive's GTIDs in the planner, while restore.py's server-side checks
// would. Sets read from servers, binlogs and manifests use ParseSet.
func ParseUserSet(s string) (Set, error) {
	raw, err := internalmysql.ParseGTIDSet(s)
	if err != nil {
		return nil, err
	}
	folded := make(internalmysql.GTIDSet, len(raw))
	for sid, ivs := range raw {
		key := strings.ToLower(sid)
		folded[key] = append(folded[key], ivs...)
	}
	return FromMySQL(folded), nil
}

func normalizeIntervals(in []internalmysql.Interval) []Interval {
	ivs := make([]Interval, 0, len(in))
	for _, iv := range in {
		ivs = append(ivs, Interval{iv.Start, iv.End})
	}
	sort.Slice(ivs, func(i, j int) bool { return ivs[i].Start < ivs[j].Start })
	merged := ivs[:0]
	for _, iv := range ivs {
		if n := len(merged); n > 0 && iv.Start <= merged[n-1].End+1 {
			if iv.End > merged[n-1].End {
				merged[n-1].End = iv.End
			}
			continue
		}
		merged = append(merged, iv)
	}
	return merged
}

// SetOf builds a set from single GTIDs.
func SetOf(gs ...GTID) Set {
	s := Set{}
	for _, g := range gs {
		s.add(g)
	}
	return s
}

// add inserts g and reports whether it was absent.
func (s Set) add(g GTID) bool {
	ivs := s[g.SID]
	i := sort.Search(len(ivs), func(i int) bool { return ivs[i].End >= g.GNO })
	if i < len(ivs) && ivs[i].Start <= g.GNO {
		return false
	}
	switch {
	case i > 0 && ivs[i-1].End+1 == g.GNO && i < len(ivs) && ivs[i].Start-1 == g.GNO:
		ivs[i-1].End = ivs[i].End
		ivs = append(ivs[:i], ivs[i+1:]...)
	case i > 0 && ivs[i-1].End+1 == g.GNO:
		ivs[i-1].End = g.GNO
	case i < len(ivs) && ivs[i].Start-1 == g.GNO:
		ivs[i].Start = g.GNO
	default:
		ivs = append(ivs, Interval{})
		copy(ivs[i+1:], ivs[i:])
		ivs[i] = Interval{g.GNO, g.GNO}
	}
	s[g.SID] = ivs
	return true
}

// Has reports g ∈ s.
func (s Set) Has(g GTID) bool {
	ivs := s[g.SID]
	i := sort.Search(len(ivs), func(i int) bool { return ivs[i].End >= g.GNO })
	return i < len(ivs) && ivs[i].Start <= g.GNO
}

// IsEmpty reports s = ∅.
func (s Set) IsEmpty() bool { return len(s) == 0 }

// Count is |s|.
func (s Set) Count() int64 {
	var n int64
	for _, ivs := range s {
		for _, iv := range ivs {
			n += iv.End - iv.Start + 1
		}
	}
	return n
}

// Intervals is the total interval count, a size measure for budgeting.
func (s Set) Intervals() int {
	n := 0
	for _, ivs := range s {
		n += len(ivs)
	}
	return n
}

// Clone copies s.
func (s Set) Clone() Set {
	out := make(Set, len(s))
	for sid, ivs := range s {
		out[sid] = append([]Interval(nil), ivs...)
	}
	return out
}

// Union is s ∪ o.
func (s Set) Union(o Set) Set {
	out := make(Set, len(s)+len(o))
	for sid, ivs := range s {
		out[sid] = unionIntervals(ivs, o[sid])
	}
	for sid, ivs := range o {
		if _, ok := s[sid]; !ok {
			out[sid] = append([]Interval(nil), ivs...)
		}
	}
	return out
}

// Subtract is s ∖ o.
func (s Set) Subtract(o Set) Set {
	out := make(Set, len(s))
	for sid, ivs := range s {
		if r := subtractIntervals(ivs, o[sid]); len(r) > 0 {
			out[sid] = r
		}
	}
	return out
}

// Intersect is s ∩ o.
func (s Set) Intersect(o Set) Set {
	out := make(Set)
	for sid, ivs := range s {
		if r := intersectIntervals(ivs, o[sid]); len(r) > 0 {
			out[sid] = r
		}
	}
	return out
}

// Overlaps reports s ∩ o ≠ ∅.
func (s Set) Overlaps(o Set) bool {
	for sid, ivs := range s {
		b := o[sid]
		i, j := 0, 0
		for i < len(ivs) && j < len(b) {
			if ivs[i].End < b[j].Start {
				i++
			} else if b[j].End < ivs[i].Start {
				j++
			} else {
				return true
			}
		}
	}
	return false
}

// Contains reports o ⊆ s.
func (s Set) Contains(o Set) bool {
	for sid, ivs := range o {
		mine := s[sid]
		j := 0
		for _, iv := range ivs {
			for j < len(mine) && mine[j].End < iv.Start {
				j++
			}
			if j == len(mine) || mine[j].Start > iv.Start || mine[j].End < iv.End {
				return false
			}
		}
	}
	return true
}

// Equal reports s = o.
func (s Set) Equal(o Set) bool {
	if len(s) != len(o) {
		return false
	}
	for sid, ivs := range s {
		b, ok := o[sid]
		if !ok || len(b) != len(ivs) {
			return false
		}
		for i := range ivs {
			if ivs[i] != b[i] {
				return false
			}
		}
	}
	return true
}

// UUIDs returns the distinct UUID parts of s's source identifiers.
func (s Set) UUIDs() map[string]bool {
	out := make(map[string]bool, len(s))
	for sid := range s {
		out[sidUUID(sid)] = true
	}
	return out
}

// OfUUID returns the members of s whose UUID part is uuid (all tags).
func (s Set) OfUUID(uuid string) Set {
	out := make(Set)
	for sid, ivs := range s {
		if sidUUID(sid) == uuid {
			out[sid] = append([]Interval(nil), ivs...)
		}
	}
	return out
}

// GTIDs lists the members in order; callers bound the size.
func (s Set) GTIDs() []GTID {
	var out []GTID
	for _, sid := range s.sids() {
		for _, iv := range s[sid] {
			for n := iv.Start; n <= iv.End; n++ {
				out = append(out, GTID{sid, n})
			}
		}
	}
	return out
}

// First is the smallest member in GTID order; ok is false for ∅.
func (s Set) First() (GTID, bool) {
	sids := s.sids()
	if len(sids) == 0 {
		return GTID{}, false
	}
	return GTID{sids[0], s[sids[0]][0].Start}, true
}

func (s Set) sids() []string {
	sids := make([]string, 0, len(s))
	for sid := range s {
		sids = append(sids, sid)
	}
	sort.Strings(sids)
	return sids
}

// String is the canonical spelling: source identifiers sorted, intervals
// merged, "uuid:1-5:7,uuid:tag:1-2". MySQL accepts it everywhere a GTID
// set is expected (GTID_SUBSET, mysqlbinlog --include-gtids).
func (s Set) String() string {
	if len(s) == 0 {
		return ""
	}
	var b strings.Builder
	for i, sid := range s.sids() {
		if i > 0 {
			b.WriteByte(',')
		}
		b.WriteString(sid)
		for _, iv := range s[sid] {
			b.WriteByte(':')
			b.WriteString(strconv.FormatInt(iv.Start, 10))
			if iv.End != iv.Start {
				b.WriteByte('-')
				b.WriteString(strconv.FormatInt(iv.End, 10))
			}
		}
	}
	return b.String()
}

// bounded renders s for a message, cut at limit bytes.
func (s Set) bounded(limit int) string {
	str := s.String()
	if len(str) <= limit {
		return str
	}
	return str[:limit] + fmt.Sprintf("… (%d transactions)", s.Count())
}

func unionIntervals(a, b []Interval) []Interval {
	out := make([]Interval, 0, len(a)+len(b))
	i, j := 0, 0
	push := func(iv Interval) {
		if n := len(out); n > 0 && iv.Start <= out[n-1].End+1 {
			if iv.End > out[n-1].End {
				out[n-1].End = iv.End
			}
			return
		}
		out = append(out, iv)
	}
	for i < len(a) || j < len(b) {
		if j == len(b) || (i < len(a) && a[i].Start <= b[j].Start) {
			push(a[i])
			i++
		} else {
			push(b[j])
			j++
		}
	}
	return out
}

func subtractIntervals(a, b []Interval) []Interval {
	var out []Interval
	j := 0
	for _, iv := range a {
		cur := iv
		for j < len(b) && b[j].End < cur.Start {
			j++
		}
		k := j
		for k < len(b) && b[k].Start <= cur.End {
			if b[k].Start > cur.Start {
				out = append(out, Interval{cur.Start, b[k].Start - 1})
			}
			if b[k].End >= cur.End {
				cur.Start = cur.End + 1
				break
			}
			cur.Start = b[k].End + 1
			k++
		}
		if cur.Start <= cur.End {
			out = append(out, cur)
		}
	}
	return out
}

func intersectIntervals(a, b []Interval) []Interval {
	var out []Interval
	i, j := 0, 0
	for i < len(a) && j < len(b) {
		lo, hi := max(a[i].Start, b[j].Start), min(a[i].End, b[j].End)
		if lo <= hi {
			out = append(out, Interval{lo, hi})
		}
		if a[i].End < b[j].End {
			i++
		} else {
			j++
		}
	}
	return out
}

// normalized sorts and merges intervals appended out of order.
func (s Set) normalized() Set {
	out := make(Set, len(s))
	for sid, ivs := range s {
		raw := make([]internalmysql.Interval, len(ivs))
		for i, iv := range ivs {
			raw[i] = internalmysql.Interval{Start: iv.Start, End: iv.End}
		}
		if n := normalizeIntervals(raw); len(n) > 0 {
			out[sid] = n
		}
	}
	return out
}
