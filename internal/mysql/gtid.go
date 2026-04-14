package mysql

import (
	"fmt"
	"sort"
	"strconv"
	"strings"
)

// GTIDSet represents a set of GTID intervals per server UUID.
type GTIDSet map[string][]Interval

// Interval represents a range of transaction sequence numbers.
type Interval struct {
	Start int64
	End   int64
}

// ParseGTIDSet parses a MySQL GTID set string like "uuid1:1-5,uuid2:1-3:7-9".
// Multiple UUIDs are separated by commas. Multiple intervals for the same UUID
// are separated by colons after the initial uuid[:tag]:interval.
// Supported formats:
//   - Traditional: uuid:interval[:interval][,uuid:interval[:interval]]...
//   - Tagged:      uuid:tag:interval[:interval][,uuid:tag:interval[:interval]]...
func ParseGTIDSet(s string) (GTIDSet, error) {
	s = strings.TrimSpace(s)
	if s == "" {
		return GTIDSet{}, nil
	}

	result := make(GTIDSet)

	// Split by comma to get per-UUID entries. However, a single UUID entry
	// can look like "uuid:1-5:7-9" (colons separate intervals within one UUID).
	// With tags, it can look like "uuid:tag:1-5:7-9".
	// The tricky part: commas separate different UUIDs, and within a UUID entry
	// colons separate the UUID from its (optional) tag, and the tag/intervals from each other.
	//
	// MySQL GTID format: uuid[:tag]:interval[:interval][,uuid[:tag]:interval[:interval]]...
	// where interval is either "n" or "n-m".

	// Split by comma first to separate UUID groups. But note that newlines
	// can also separate them in multi-line GTID sets.
	s = strings.ReplaceAll(s, "\n", ",")
	parts := strings.Split(s, ",")

	for _, part := range parts {
		part = strings.TrimSpace(part)
		if part == "" {
			continue
		}

		// Split by colon: first element is UUID, second might be tag or interval, rest are intervals.
		segments := strings.Split(part, ":")
		if len(segments) < 2 {
			return nil, fmt.Errorf("invalid GTID entry %q: expected uuid[:tag]:interval", part)
		}

		uuid := segments[0]
		if uuid == "" {
			return nil, fmt.Errorf("empty UUID in GTID entry %q", part)
		}

		// Determine if we have a tag (format: uuid:tag:interval) or not (format: uuid:interval).
		// Use a strict syntactic check: a segment is an interval if it contains only digits with
		// an optional single hyphen separator. This avoids conflating a malformed interval (e.g.
		// "5-3" with start>end, or "1-2-3") with a tag, which would silently skip the bad segment.
		tagIndex := 1
		if len(segments) >= 3 {
			secondSeg := segments[1]
			if !isIntervalSegment(secondSeg) {
				if !isTagSegment(secondSeg) {
					return nil, fmt.Errorf("invalid segment %q in GTID entry %q: not an interval or valid tag", secondSeg, part)
				}
				tagIndex = 2
			}
		}

		hasInterval := false
		for i := tagIndex; i < len(segments); i++ {
			seg := strings.TrimSpace(segments[i])
			if seg == "" {
				continue
			}
			iv, err := parseInterval(seg)
			if err != nil {
				return nil, fmt.Errorf("invalid interval %q in GTID entry %q: %w", seg, part, err)
			}
			result[uuid] = append(result[uuid], iv)
			hasInterval = true
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

// isTagSegment reports whether s could be a valid GTID tag: non-empty and
// containing only ASCII letters, digits, and underscores.
func isTagSegment(s string) bool {
	if s == "" {
		return false
	}
	for _, c := range s {
		if (c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z') || (c >= '0' && c <= '9') || c == '_' {
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
func (g GTIDSet) Contains(other GTIDSet) bool {
	for uuid, otherIntervals := range other {
		myIntervals, ok := g[uuid]
		if !ok {
			return false
		}
		for _, oi := range otherIntervals {
			if !intervalsContain(myIntervals, oi) {
				return false
			}
		}
	}
	return true
}

// intervalsContain returns true if the intervals collectively cover the target interval.
func intervalsContain(intervals []Interval, target Interval) bool {
	// For each position in target, check if it's covered by some interval.
	// Since intervals may not be merged, we need to check coverage.
	// Sort intervals by start for efficient checking.
	sorted := make([]Interval, len(intervals))
	copy(sorted, intervals)
	sort.Slice(sorted, func(i, j int) bool {
		return sorted[i].Start < sorted[j].Start
	})

	pos := target.Start
	for _, iv := range sorted {
		if iv.Start > pos {
			// Gap: pos is not covered.
			return false
		}
		if iv.End >= pos {
			pos = iv.End + 1
		}
		if pos > target.End {
			return true
		}
	}
	return pos > target.End
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
