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
// are separated by colons after the initial uuid:interval.
func ParseGTIDSet(s string) (GTIDSet, error) {
	s = strings.TrimSpace(s)
	if s == "" {
		return GTIDSet{}, nil
	}

	result := make(GTIDSet)

	// Split by comma to get per-UUID entries. However, a single UUID entry
	// can look like "uuid:1-5:7-9" (colons separate intervals within one UUID).
	// The tricky part: commas separate different UUIDs, and within a UUID entry
	// colons separate the UUID from its intervals, and intervals from each other.
	//
	// MySQL GTID format: uuid:interval[:interval][,uuid:interval[:interval]]...
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

		// Split by colon: first element is UUID, rest are intervals.
		segments := strings.Split(part, ":")
		if len(segments) < 2 {
			return nil, fmt.Errorf("invalid GTID entry %q: expected uuid:interval", part)
		}

		uuid := segments[0]
		if uuid == "" {
			return nil, fmt.Errorf("empty UUID in GTID entry %q", part)
		}

		hasInterval := false
		for _, seg := range segments[1:] {
			seg = strings.TrimSpace(seg)
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
