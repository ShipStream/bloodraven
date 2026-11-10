package pitrplan

import (
	"math"
	"slices"
	"sort"
)

// The occurrence registry records, for every GTID among the downloaded
// transactions, the copy later copies are compared with and how many
// copies there are (ingest, plan.go), and the GTID's node in the order
// check (identity.go).
//
// It is addressed by (source, GNO) instead of hashed, so that it can spill
// (issue #266): a source's GNOs are dense runs, files are read in GNO
// order, and so are the registry's records. A hash table of the same size
// would touch a random page for every transaction once it no longer fits
// in memory. Each source has a sorted list of extents; an extent is an
// array of records for consecutive GNOs.

// occRec is the registry's record of one GTID.
type occRec struct {
	// file is 1 + the index in planner.order of the file holding the copy
	// that later copies are compared with: the first copy, or the first
	// with an effect digest. 0 means no downloaded file holds the GTID.
	file uint32
	// i is that copy's transaction index in its file.
	i uint32
	// n counts the copies, saturating.
	n uint16
	_ uint16
	// node is 1 + the GTID's node in the order check, or 0.
	node int32
}

const (
	// occMinExtent and occMaxExtent bound an extent, in records. An
	// extent that continues the one before it is twice its size; any
	// other starts small. So a lone transaction (a GTID tag used once, a
	// GNO far from the others) costs 8 records, and a long run costs one
	// extent per 4096 transactions.
	occMinExtent = 8
	occMaxExtent = 4096
	// occExtentBytes is charged per extent for its entry in the source's
	// sorted list: the start, the array's header and the list's growth.
	occExtentBytes = 64
	// occSourceBytes is charged per source for its list header.
	occSourceBytes = 48
)

// occurrenceBytes is the registry's share of the up-front estimate of a
// row's index (planner.fetch).
const occurrenceBytes = 16

type occExtent struct {
	start int64 // the GNO of recs[0]
	recs  arr[occRec]
}

func (e *occExtent) end() int64 { return e.start + int64(len(e.recs.s)) }

type occSource struct {
	exts []occExtent // sorted by start, disjoint
	// last is the extent of the previous lookup: a file's transactions
	// mostly continue where the last one was.
	last int
}

type occTable struct {
	b   *Budget
	src []occSource
}

// grow makes room for source sid.
func (t *occTable) grow(sid uint32) error {
	for int(sid) >= len(t.src) {
		if err := t.b.Charge(occSourceBytes, "occurrence registry"); err != nil {
			return err
		}
		t.src = append(t.src, occSource{})
	}
	return nil
}

// find returns the index of the extent holding gno, or -1, and the index
// of the last extent that starts at or before gno (-1 when none does).
func (s *occSource) find(gno int64) (at, before int) {
	if s.last < len(s.exts) {
		if e := &s.exts[s.last]; gno >= e.start && gno < e.end() {
			return s.last, s.last
		}
	}
	before = sort.Search(len(s.exts), func(k int) bool { return s.exts[k].start > gno }) - 1
	if before >= 0 && gno < s.exts[before].end() {
		s.last = before
		return before, before
	}
	return -1, before
}

// get returns the record of (sid, gno); ok is false for a GTID never put.
func (t *occTable) get(sid uint32, gno int64) (rec occRec, ok bool) {
	if int(sid) >= len(t.src) {
		return occRec{}, false
	}
	s := &t.src[sid]
	at, _ := s.find(gno)
	if at < 0 {
		return occRec{}, false
	}
	e := &s.exts[at]
	rec = e.recs.s[gno-e.start]
	return rec, rec.file != 0
}

// put stores the record of (sid, gno), allocating its extent.
func (t *occTable) put(sid uint32, gno int64, rec occRec) error {
	if err := t.grow(sid); err != nil {
		return err
	}
	s := &t.src[sid]
	at, before := s.find(gno)
	if at < 0 {
		// A new extent. One that continues the extent before it (gno is
		// within that extent's length of its end) starts at its end and is
		// twice its size. Any other is small and starts on a multiple of
		// its size, so that transactions that arrive slightly out of order
		// share it. Either ends before the extent after it.
		size := int64(occMinExtent)
		start := (gno-1)/size*size + 1
		if before >= 0 {
			prev := &s.exts[before]
			if n := int64(len(prev.recs.s)); gno-prev.end() < n {
				size = min(2*n, occMaxExtent)
				start = prev.end()
			}
			start = max(start, prev.end())
		}
		end := int64(math.MaxInt64)
		if start <= math.MaxInt64-size {
			end = start + size
		}
		if before+1 < len(s.exts) {
			end = min(end, s.exts[before+1].start)
		}
		if err := t.b.Charge(occExtentBytes, "occurrence registry"); err != nil {
			return err
		}
		recs, err := alloc[occRec](t.b, int(end-start), "occurrence registry")
		if err != nil {
			t.b.Release(occExtentBytes)
			return err
		}
		at = before + 1
		s.exts = slices.Insert(s.exts, at, occExtent{start: start, recs: recs})
		s.last = at
	}
	e := &s.exts[at]
	e.recs.set(int(gno-e.start), rec)
	return nil
}

// each calls fn for every GTID put, by source and ascending GNO. fn may
// put the GTID it is called with.
func (t *occTable) each(fn func(sid uint32, gno int64, rec occRec) error) error {
	for sid := range t.src {
		for k := range t.src[sid].exts {
			e := &t.src[sid].exts[k]
			for i, rec := range e.recs.s {
				if rec.file == 0 {
					continue
				}
				if err := fn(uint32(sid), e.start+int64(i), rec); err != nil { //nolint:gosec // sources are counted in uint32
					return err
				}
			}
		}
	}
	return nil
}
