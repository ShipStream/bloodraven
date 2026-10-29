package sidecar

import (
	"sort"

	internalmysql "github.com/shipstream/bloodraven/internal/mysql"
)

// UnionBinlogGTIDs merges interval overlaps before comparing replay frontiers.
func UnionBinlogGTIDs(sets ...internalmysql.GTIDSet) internalmysql.GTIDSet {
	result := make(internalmysql.GTIDSet)
	for _, set := range sets {
		for sid, intervals := range set {
			result[sid] = append(result[sid], intervals...)
		}
	}
	for sid, intervals := range result {
		sort.Slice(intervals, func(i, j int) bool { return intervals[i].Start < intervals[j].Start })
		merged := intervals[:0]
		for _, interval := range intervals {
			if len(merged) > 0 && interval.Start-1 <= merged[len(merged)-1].End {
				if interval.End > merged[len(merged)-1].End {
					merged[len(merged)-1].End = interval.End
				}
			} else {
				merged = append(merged, interval)
			}
		}
		result[sid] = merged
	}
	return result
}
