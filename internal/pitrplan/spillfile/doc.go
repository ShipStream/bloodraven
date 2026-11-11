// Package spillfile is the file-backed arena behind the PITR planner's spill
// (issue #266): pitr-download hands it to pitrplan as a pitrplan.Arena, and
// the planner places the arrays that grow with the archive (the
// per-transaction index, the occurrence registry, the plan search and the
// order check) in it once they no longer fit the memory budget.
//
// A block is a range of a file in the arena's directory, mapped shared and
// read-write. The pages of such a mapping are page cache, not anonymous
// memory: a clean one is dropped under memory pressure and read back from
// the file when it is touched again. A dirty one is not reclaimable until
// it is written back, and the kernel does not hold a mapping's writer back
// reliably. Measured on Linux 7.0 (cgroup v2, ext4, a Docker container)
// with TestArenaUnderMemoryLimit: a process that filled 2 GiB of mapped
// blocks at memory speed inside a 128 MiB cgroup was OOM-killed in 3 runs
// of 6, and one that filled 4 GiB inside 256 MiB in its one run; the
// kernel's reports show 77% to 95% of the cgroup's limit dirty or under
// write-back at the kill. The same process calling WriteBack and Settle
// after every 8 MiB it wrote (4 MiB inside 64 MiB, 16 MiB inside 256 MiB)
// survived all 11 runs, random rewrites of every page included. So the
// arena's user bounds what it has written and not yet settled
// (pitrplan.Budget does, for the planner), and the arena does the same
// for its own bulk writes.
//
// This package has no dependency on pitrplan; its methods take and return
// builtin types only, so the planner's own tests can use it.
//
// What the arena guarantees, and how:
//
//   - Nothing is left behind. A file is unlinked as soon as it is created,
//     so its space returns when the process ends, however it ends (exit,
//     signal, OOM kill), and nothing can open it by name. Close unmaps
//     everything and removes the directory. No file is ever opened to be
//     read back: a rerun plans from scratch.
//
//   - No fault for lack of space. A file is fully preallocated (fallocate,
//     or zeros written where the filesystem has no fallocate) before it is
//     mapped, and a mapping never extends past its file, so touching a page
//     cannot fail with ENOSPC or reach past the end of the file, the two
//     ordinary causes of SIGBUS on a mapping. ENOSPC surfaces from Alloc or
//     Resize as an error.
//
//   - A fault that happens anyway is an error. Guard runs the planner with
//     runtime/debug.SetPanicOnFault and turns a fault inside the arena (a
//     page the device could not read, EIO) into a *FaultError instead of a
//     crash.
//
//   - Lost writes are noticed. The kernel reports a failed write-back of a
//     dirty page on the next fsync of the file, not to the code that
//     wrote the page. The arena syncs a file before it releases it and
//     Check syncs the rest, so a plan computed over pages that were lost
//     is reported as an I/O error instead of being trusted.
//
// The mapping is available on Linux. Elsewhere New returns ErrUnsupported
// and the planner keeps everything in memory, as it did before.
//
// An Arena is not safe for concurrent use; the planner is single-threaded.
package spillfile

import (
	"errors"
	"fmt"
)

// ErrUnsupported is New's error where file mappings are not implemented.
var ErrUnsupported = errors.New("spillfile: file-backed spill is not supported on this platform")

// DefaultSlabBytes is the size of the files small blocks share.
const DefaultSlabBytes = 64 << 20

// Options configure an Arena.
type Options struct {
	// Reserve, when set, is called before the arena takes n more bytes of
	// disk (a new file, or a file's growth). Its error stops the
	// allocation and is returned unchanged: this is where the caller
	// enforces its disk budget.
	Reserve func(n int64) error
	// SlabBytes is the size of the files that small blocks share
	// (DefaultSlabBytes when 0). The first slabs are smaller, doubling up
	// to it, so a plan that spills little preallocates little. A block
	// larger than a quarter of it gets a file of its own, which is
	// released as soon as the block is freed.
	SlabBytes int64
}

// FaultError is a fault inside a mapped block: the kernel could not
// produce the page (an I/O error on the device holding the arena).
type FaultError struct {
	Addr uintptr
}

func (e *FaultError) Error() string {
	return fmt.Sprintf("spillfile: fault at %#x reading spilled data: the staging volume returned an I/O error", e.Addr)
}

// Stats describe an arena's use of its directory's filesystem.
type Stats struct {
	// DiskBytes is the preallocated size of the files held now; PeakBytes
	// its largest value.
	DiskBytes, PeakBytes int64
	// Files is the number of files held now; Created counts every file
	// the arena made.
	Files, Created int
}
