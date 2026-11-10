package main

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"syscall"

	"github.com/shipstream/bloodraven/internal/pitrarchive"
	"github.com/shipstream/bloodraven/internal/pitrplan"
	"github.com/shipstream/bloodraven/internal/pitrplan/spillfile"
	"github.com/shipstream/bloodraven/internal/util"
)

// pitrSpillDirName is the planner's spill directory under the staging
// directory. Its files are unlinked as they are created, so it is always
// empty, and it is removed before the plan's files are staged: restore.py
// never sees it, and the staged layout is unchanged.
const pitrSpillDirName = ".pitr-spill"

// stagingSpill is the planner's spill arena on the staging volume (issue
// #266): a spillfile.Arena whose disk use is budgeted against the same
// limits as the staged files and the tee cache, the emptyDir's sizeLimit
// and the node filesystem's floor. A nil *stagingSpill means planning
// stays in memory, as before: the platform has no file mapping, or the
// staging volume is itself memory.
type stagingSpill struct {
	arena *spillfile.Arena
	rf    *rowFetcher
}

// pitrStagingInMemory reports whether dir's filesystem keeps its files in
// memory (tmpfs, ramfs); tests replace it. Spilling there would move the
// planner's arrays from memory it budgets to memory it does not.
var pitrStagingInMemory = stagingInMemory

// spillDirtyBytes is the bound on spilled pages written and not yet
// written back, for a planner budget of memory bytes: an eighth of it,
// between 1 MiB and 64 MiB. It is charged to the budget while anything is
// spilled, and it is the only part of the spill the container's memory
// has to hold: every other spilled page is clean page cache.
func spillDirtyBytes(memory int64) int64 {
	return min(max(memory/8, 1<<20), 64<<20)
}

// spillHeapMax is how much of the planner budget the arrays that can spill
// may use before they do: half of what the dirty window leaves. The other
// half stays for what cannot spill (interval sets, manifest rows, the
// event being decoded), so a plan that spills is not refused for memory
// its own arrays hold.
func spillHeapMax(memory int64) int64 {
	return (memory - spillDirtyBytes(memory)) / 2
}

// openStagingSpill gives budget a spill arena under req.outDir, or returns
// nil when planning must stay in memory.
func openStagingSpill(req pitrDownloadRequest, rf *rowFetcher, budget *pitrplan.Budget) *stagingSpill {
	if pitrStagingInMemory(req.outDir) {
		return nil
	}
	s := &stagingSpill{rf: rf}
	arena, err := spillfile.New(filepath.Join(req.outDir, pitrSpillDirName), spillfile.Options{Reserve: s.reserve})
	if err != nil {
		// No file mapping on this platform (spillfile.ErrUnsupported).
		return nil
	}
	s.arena = arena
	budget.SetSpill(pitrplan.SpillOptions{Arena: s, HeapMax: spillHeapMax(req.memory), DirtyMax: spillDirtyBytes(req.memory)})
	return s
}

// diskBytes is the staging disk the spill holds now.
func (s *stagingSpill) diskBytes() int64 {
	if s == nil {
		return 0
	}
	return s.arena.Stats().DiskBytes
}

func (s *stagingSpill) stats() spillfile.Stats {
	if s == nil {
		return spillfile.Stats{}
	}
	return s.arena.Stats()
}

// guard runs the planner so that a fault in spilled memory (an I/O error
// of the staging volume) is an error, not a crash.
func (s *stagingSpill) guard(fn func() error) error {
	if s == nil {
		return fn()
	}
	return s.arena.Guard(fn)
}

// finish ends the spill once planning is done: it reports a write-back
// error of any spill file, and releases them all.
func (s *stagingSpill) finish() error {
	if s == nil {
		return nil
	}
	err := s.arena.Check()
	if cerr := s.arena.Close(); err == nil {
		err = cerr
	}
	return err
}

// close releases the spill; it is safe after finish.
func (s *stagingSpill) close() {
	if s != nil {
		_ = s.arena.Close()
	}
}

// reserve is the staging-disk budget of the spill: the arena calls it
// before it takes n more bytes. The spill, the tee cache and the reserve
// must fit the emptyDir's sizeLimit, and the node filesystem must keep its
// floor. Cached rows are only a saved download, so they make room first.
func (s *stagingSpill) reserve(n int64) error {
	rf := s.rf
	for {
		var refusal error
		held := s.arena.Stats().DiskBytes
		if rf.limit > 0 && held+rf.cachedBytes+n+stagingReserve > rf.limit {
			refusal = &pitrplan.Refusal{Token: pitrarchive.TokenPlanTooLarge, Message: fmt.Sprintf(
				"planning must spill %s more to the staging volume, which already holds %s of spilled planning data and %s of cached binlog files; that is more than the volume's sizeLimit of %s; raise spec.backup.stagingVolumeSizeLimit, raise the Job's memory limit so that less is spilled, or choose a backup closer to the target",
				util.HumanBytes(n), util.HumanBytes(held), util.HumanBytes(rf.cachedBytes), util.HumanBytes(rf.limit))}
		} else if free, threshold := rf.freeAfter(rf.stagingDir(), n); free < threshold {
			refusal = &pitrplan.Refusal{Token: pitrarchive.TokenPlanTooLarge, Message: fmt.Sprintf(
				"planning must spill %s more to the staging volume (it holds %s of spilled planning data), which would leave less than the node filesystem's floor of %s free (max(10%%, 256 MiB), kept clear of the kubelet's eviction threshold); give the Job's node more ephemeral storage, raise the Job's memory limit so that less is spilled, or choose a backup closer to the target",
				util.HumanBytes(n), util.HumanBytes(held), util.HumanBytes(threshold))}
		}
		if refusal == nil {
			return nil
		}
		if !rf.dropCachedRow() {
			return refusal
		}
	}
}

// dropCachedRow deletes the largest cached row (the first in row order
// among equals) and reports whether there was one. The row is downloaded
// again if the plan stages it.
func (rf *rowFetcher) dropCachedRow() bool {
	var victim pitrplan.RowRef
	found := false
	for ref, c := range rf.cached {
		v := rf.cached[victim]
		if !found || c.size > v.size || (c.size == v.size && rowRefLess(ref, victim)) {
			victim, found = ref, true
		}
	}
	if !found {
		return false
	}
	c := rf.cached[victim]
	_ = os.Remove(c.path)
	rf.cachedBytes -= blocks(c.size)
	delete(rf.cached, victim)
	return true
}

func rowRefLess(a, b pitrplan.RowRef) bool {
	if a.Site != b.Site {
		return a.Site < b.Site
	}
	if a.Name != b.Name {
		return a.Name < b.Name
	}
	if a.ID != b.ID {
		return a.ID < b.ID
	}
	return a.State < b.State
}

// spillError turns the volume running out of space under an allocation
// (another writer took it after reserve looked) into the refusal reserve
// would have given. Any other error is the volume failing.
func (s *stagingSpill) spillError(err error) error {
	if errors.Is(err, syscall.ENOSPC) || errors.Is(err, syscall.EDQUOT) {
		return &pitrplan.Refusal{Token: pitrarchive.TokenPlanTooLarge, Message: fmt.Sprintf(
			"the staging volume ran out of space while planning spilled to it (%v; it holds %s of spilled planning data); give the Job's node more ephemeral storage, raise the Job's memory limit so that less is spilled, or choose a backup closer to the target",
			err, util.HumanBytes(s.arena.Stats().DiskBytes))}
	}
	return err
}

// Alloc implements pitrplan.Arena.
func (s *stagingSpill) Alloc(n int64) ([]byte, error) {
	mem, err := s.arena.Alloc(n)
	if err != nil {
		return nil, s.spillError(err)
	}
	return mem, nil
}

// Resize implements pitrplan.Arena.
func (s *stagingSpill) Resize(mem []byte, n int64) ([]byte, error) {
	out, err := s.arena.Resize(mem, n)
	if err != nil {
		return nil, s.spillError(err)
	}
	return out, nil
}

// Free implements pitrplan.Arena.
func (s *stagingSpill) Free(mem []byte) { s.arena.Free(mem) }

// WriteBack implements pitrplan.Arena.
func (s *stagingSpill) WriteBack(block []byte, off, n int64) error {
	return s.arena.WriteBack(block, off, n)
}

// Settle implements pitrplan.Arena.
func (s *stagingSpill) Settle() error { return s.arena.Settle() }
