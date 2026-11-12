package mysql

import (
	"fmt"
	"sort"
	"strconv"
	"strings"
)

// GTIDSet represents a set of GTID intervals per source identifier. A source
// identifier is either a server UUID or a tagged server UUID ("uuid:tag").
type GTIDSet map[string][]Interval

// Interval represents a range of transaction sequence numbers.
type Interval struct {
	Start int64
	End   int64
}

// ParseGTIDSet parses a MySQL GTID set string like "uuid1:1-5,uuid2:1-3:7-9".
// Entries for different UUIDs are separated by commas (or newlines). Within an
// entry, colons separate the UUID, its intervals, and any tags. The grammar
// follows MySQL 8.4's GTID set syntax:
//
//	entry    = uuid ":" group { ":" group }
//	group    = interval | tag ":" interval
//	interval = n | n "-" m
//	tag      = [a-zA-Z_][a-zA-Z0-9_]{0,31}
//
// A tag applies to the intervals that follow it until the next tag. MySQL
// prints each UUID once, with its untagged intervals first and then one
// ":tag:intervals" group per tag, for example
// "uuid:1-100:app:1-5:ops:1-2". The untagged UUID and each uuid:tag are
// distinct source identifiers and are stored under separate keys ("uuid",
// "uuid:app", "uuid:ops"). Tags are case-insensitive and stored lower-case,
// as MySQL normalizes them. Every tag must be followed by at least one
// interval.
func ParseGTIDSet(s string) (GTIDSet, error) {
	s = strings.TrimSpace(s)
	if s == "" {
		return GTIDSet{}, nil
	}

	result := make(GTIDSet)

	// Newlines can separate entries in multi-line GTID sets, like commas.
	s = strings.ReplaceAll(s, "\n", ",")
	parts := strings.Split(s, ",")

	for _, part := range parts {
		part = strings.TrimSpace(part)
		if part == "" {
			continue
		}

		segments := strings.Split(part, ":")
		if len(segments) < 2 {
			return nil, fmt.Errorf("invalid GTID entry %q: expected uuid[:tag]:interval", part)
		}

		uuid := segments[0]
		if uuid == "" {
			return nil, fmt.Errorf("empty UUID in GTID entry %q", part)
		}

		// Walk the segments after the UUID. Intervals belong to the current
		// source identifier, which starts as the bare UUID and switches to
		// uuid:tag at each tag. The syntactic interval check comes first so
		// a malformed interval (e.g. "5-3" or "1-2-3") is reported as such
		// rather than mistaken for a tag.
		sid := uuid
		pendingTag := ""
		hasInterval := false
		for i, seg := range segments[1:] {
			// When more segments follow, the one right after the UUID must
			// be well formed; otherwise space-padded and empty segments are
			// tolerated, as this parser always has.
			if i > 0 || len(segments) == 2 {
				seg = strings.TrimSpace(seg)
				if seg == "" {
					continue
				}
			}
			switch {
			case isIntervalSegment(seg):
				iv, err := parseInterval(seg)
				if err != nil {
					return nil, fmt.Errorf("invalid interval %q in GTID entry %q: %w", seg, part, err)
				}
				result[sid] = append(result[sid], iv)
				hasInterval = true
				pendingTag = ""
			case isTagSegment(seg):
				if pendingTag != "" {
					return nil, fmt.Errorf("tag %q has no intervals in GTID entry %q", pendingTag, part)
				}
				tag := strings.ToLower(seg)
				// MySQL treats uuid and uuid:tag as distinct source
				// identifiers. Retain the tag in the map key so set
				// arithmetic cannot merge transactions from different
				// tagged sources.
				sid = uuid + ":" + tag
				pendingTag = seg
			default:
				return nil, fmt.Errorf("invalid segment %q in GTID entry %q: not an interval or valid tag", seg, part)
			}
		}
		if pendingTag != "" {
			return nil, fmt.Errorf("tag %q has no intervals in GTID entry %q", pendingTag, part)
		}
		if !hasInterval {
			return nil, fmt.Errorf("no intervals found in GTID entry %q", part)
		}
	}

	return result, nil
}

// isIntervalSegment reports whether s has the syntax of a GTID interval ("N" or "N-M"),
// consisting only of ASCII digits with an optional single hyphen separator.
// It does not validate semantics (e.g. start <= end); use parseInterval for that.
func isIntervalSegment(s string) bool {
	if s == "" {
		return false
	}
	parts := strings.SplitN(s, "-", 2)
	for _, p := range parts {
		if p == "" {
			return false
		}
		for _, c := range p {
			if c < '0' || c > '9' {
				return false
			}
		}
	}
	return true
}

// maxGTIDTagLength is MySQL's limit on the length of a GTID tag.
const maxGTIDTagLength = 32

// isTagSegment reports whether s is a valid GTID tag under MySQL's grammar:
// 1 to 32 ASCII letters, digits, and underscores, not starting with a digit.
func isTagSegment(s string) bool {
	if s == "" || len(s) > maxGTIDTagLength {
		return false
	}
	for i, c := range s {
		if (c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z') || c == '_' {
			continue
		}
		if i > 0 && c >= '0' && c <= '9' {
			continue
		}
		return false
	}
	return true
}

// parseInterval parses "start-end" or "start" (which means start-start).
func parseInterval(s string) (Interval, error) {
	parts := strings.SplitN(s, "-", 2)
	start, err := strconv.ParseInt(parts[0], 10, 64)
	if err != nil {
		return Interval{}, fmt.Errorf("parse start: %w", err)
	}

	end := start
	if len(parts) == 2 {
		end, err = strconv.ParseInt(parts[1], 10, 64)
		if err != nil {
			return Interval{}, fmt.Errorf("parse end: %w", err)
		}
	}

	if start > end {
		return Interval{}, fmt.Errorf("start (%d) > end (%d)", start, end)
	}

	return Interval{Start: start, End: end}, nil
}

// IsEmpty returns true if the set contains no transactions.
func (g GTIDSet) IsEmpty() bool {
	return len(g) == 0
}

// Contains returns true if this set contains all transactions in other.
// Each UUID's intervals are sorted and coalesced once, then every interval
// of other is looked up by binary search: O((n+m) log n) per UUID rather
// than a sort per interval of other (a fragmented set of thousands of
// intervals is checked under the runner's registration locks).
func (g GTIDSet) Contains(other GTIDSet) bool {
	for uuid, otherIntervals := range other {
		myIntervals, ok := g[uuid]
		if !ok {
			return false
		}
		merged := coalesceIntervals(myIntervals)
		for _, oi := range otherIntervals {
			if !coalescedContain(merged, oi) {
				return false
			}
		}
	}
	return true
}

// coalesceIntervals returns intervals sorted by start with overlapping and
// adjacent ones merged. The input is not modified.
func coalesceIntervals(intervals []Interval) []Interval {
	sorted := make([]Interval, len(intervals))
	copy(sorted, intervals)
	sort.Slice(sorted, func(i, j int) bool {
		return sorted[i].Start < sorted[j].Start
	})
	out := sorted[:0]
	for _, iv := range sorted {
		if n := len(out); n > 0 && iv.Start <= out[n-1].End+1 {
			if iv.End > out[n-1].End {
				out[n-1].End = iv.End
			}
			continue
		}
		out = append(out, iv)
	}
	return out
}

// coalescedContain reports whether merged (coalesceIntervals' output)
// covers target: one merged interval must span it.
func coalescedContain(merged []Interval, target Interval) bool {
	// The last interval starting at or before target.Start.
	i := sort.Search(len(merged), func(i int) bool { return merged[i].Start > target.Start }) - 1
	return i >= 0 && merged[i].End >= target.End
}

// Subtract returns the transactions in g that are not in other.
// This is the Go equivalent of MySQL's GTID_SUBTRACT(g, other).
func (g GTIDSet) Subtract(other GTIDSet) GTIDSet {
	result := make(GTIDSet)
	for uuid, intervals := range g {
		otherIntervals, ok := other[uuid]
		if !ok {
			cp := make([]Interval, len(intervals))
			copy(cp, intervals)
			result[uuid] = cp
			continue
		}
		remaining := subtractAllIntervals(intervals, otherIntervals)
		if len(remaining) > 0 {
			result[uuid] = remaining
		}
	}
	return result
}

// Union returns the transactions that are in g or in other, with each
// source's intervals sorted and merged. Neither set is modified.
func (g GTIDSet) Union(other GTIDSet) GTIDSet {
	result := make(GTIDSet, len(g)+len(other))
	for _, set := range []GTIDSet{g, other} {
		for sid, intervals := range set {
			result[sid] = append(result[sid], intervals...)
		}
	}
	for sid, intervals := range result {
		result[sid] = coalesceIntervals(intervals)
	}
	return result
}

// subtractAllIntervals removes all ranges covered by sub from the source intervals.
func subtractAllIntervals(source, sub []Interval) []Interval {
	var result []Interval
	for _, s := range source {
		pieces := []Interval{s}
		for _, r := range sub {
			var next []Interval
			for _, p := range pieces {
				next = append(next, subtractInterval(p, r)...)
			}
			pieces = next
		}
		result = append(result, pieces...)
	}
	return result
}

// subtractInterval removes the range [b.Start, b.End] from interval a,
// returning zero, one, or two remaining intervals.
func subtractInterval(a, b Interval) []Interval {
	if b.End < a.Start || b.Start > a.End {
		return []Interval{a}
	}
	var result []Interval
	if a.Start < b.Start {
		result = append(result, Interval{Start: a.Start, End: b.Start - 1})
	}
	if a.End > b.End {
		result = append(result, Interval{Start: b.End + 1, End: a.End})
	}
	return result
}

// HasCommonUUIDs returns true if the two sets share at least one server
// UUID. Tags are ignored: "uuid", "uuid:a" and "uuid:b" all name history
// written by the same server, and a tag does not change which server's
// history a transaction belongs to. Contains and Subtract stay tag-exact.
func (g GTIDSet) HasCommonUUIDs(other GTIDSet) bool {
	uuids := make(map[string]struct{}, len(other))
	for sid := range other {
		uuids[sourceUUID(sid)] = struct{}{}
	}
	for sid := range g {
		if _, ok := uuids[sourceUUID(sid)]; ok {
			return true
		}
	}
	return false
}

// OriginatedBy returns the transactions of g that were issued under one of
// the given server UUIDs, tagged ones included. UUIDs are compared without
// regard to case; an empty UUID matches nothing.
func (g GTIDSet) OriginatedBy(uuids ...string) GTIDSet {
	want := make(map[string]struct{}, len(uuids))
	for _, u := range uuids {
		if u = strings.ToLower(strings.TrimSpace(u)); u != "" {
			want[u] = struct{}{}
		}
	}
	result := make(GTIDSet)
	if len(want) == 0 {
		return result
	}
	for sid, intervals := range g {
		if _, ok := want[strings.ToLower(sourceUUID(sid))]; !ok || len(intervals) == 0 {
			continue
		}
		cp := make([]Interval, len(intervals))
		copy(cp, intervals)
		result[sid] = cp
	}
	return result
}

// sourceUUID returns the server UUID of a source identifier, dropping any
// ":tag" suffix.
func sourceUUID(sid string) string {
	uuid, _, _ := strings.Cut(sid, ":")
	return uuid
}

// TransactionCount returns the total number of transactions in the set.
func (g GTIDSet) TransactionCount() int64 {
	var total int64
	for _, intervals := range g {
		for _, iv := range intervals {
			total += iv.End - iv.Start + 1
		}
	}
	return total
}

// String returns the canonical string representation of the GTID set.
func (g GTIDSet) String() string {
	if len(g) == 0 {
		return ""
	}

	// Sort UUIDs for deterministic output.
	uuids := make([]string, 0, len(g))
	for uuid := range g {
		uuids = append(uuids, uuid)
	}
	sort.Strings(uuids)

	var parts []string
	for _, uuid := range uuids {
		intervals := g[uuid]
		var ivStrs []string
		for _, iv := range intervals {
			if iv.Start == iv.End {
				ivStrs = append(ivStrs, strconv.FormatInt(iv.Start, 10))
			} else {
				ivStrs = append(ivStrs, fmt.Sprintf("%d-%d", iv.Start, iv.End))
			}
		}
		parts = append(parts, uuid+":"+strings.Join(ivStrs, ":"))
	}

	return strings.Join(parts, ",")
}
