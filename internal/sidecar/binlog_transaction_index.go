package sidecar

import (
	"sort"

	internalmysql "github.com/shipstream/bloodraven/internal/mysql"
)

type binlogTransactionSpan struct {
	start, end, maxEnd int64
	entry              int
}

// BinlogTransactionIndex locates files with overlapping source UUID/tag GTIDs.
// Chronological inserts append; unusual out-of-order evidence rebuilds only
// that source's suffix. Queries compare only overlapping source intervals.
type BinlogTransactionIndex struct {
	sources map[string][]binlogTransactionSpan
}

func (x *BinlogTransactionIndex) Add(entry int, gtids internalmysql.GTIDSet) {
	if x.sources == nil {
		x.sources = make(map[string][]binlogTransactionSpan)
	}
	for sid, intervals := range gtids {
		spans := x.sources[sid]
		for _, iv := range intervals {
			i := sort.Search(len(spans), func(i int) bool { return spans[i].start > iv.Start })
			spans = append(spans, binlogTransactionSpan{})
			copy(spans[i+1:], spans[i:])
			spans[i] = binlogTransactionSpan{start: iv.Start, end: iv.End, entry: entry}
			for j := i; j < len(spans); j++ {
				spans[j].maxEnd = spans[j].end
				if j > 0 {
					spans[j].maxEnd = max(spans[j].maxEnd, spans[j-1].maxEnd)
				}
			}
		}
		x.sources[sid] = spans
	}
}
func (x *BinlogTransactionIndex) Overlaps(gtids internalmysql.GTIDSet) []int {
	seen := make(map[int]bool)
	for sid, intervals := range gtids {
		spans := x.sources[sid]
		for _, iv := range intervals {
			start := sort.Search(len(spans), func(i int) bool { return spans[i].maxEnd >= iv.Start })
			for i := start; i < len(spans) && spans[i].start <= iv.End; i++ {
				if spans[i].end >= iv.Start {
					seen[spans[i].entry] = true
				}
			}
		}
	}
	result := make([]int, 0, len(seen))
	for id := range seen {
		result = append(result, id)
	}
	sort.Ints(result)
	return result
}
