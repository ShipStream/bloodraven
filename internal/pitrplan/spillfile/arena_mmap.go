//go:build linux

package spillfile

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"runtime/debug"
	"syscall"
	"unsafe"
)

const (
	// blockAlign is the alignment of every block: enough for any
	// fixed-size record the planner stores.
	blockAlign = 16
	// firstSlabBytes is the size of an arena's first shared file.
	firstSlabBytes = 1 << 20
	// The flags of sync_file_range(2).
	syncFileRangeWaitBefore = 1
	syncFileRangeWrite      = 2
	syncFileRangeWaitAfter  = 4
	// bulkStep bounds what the arena itself writes to a mapping (a moved
	// block's copy, the zeroing of a shrunk one) before it has the kernel
	// write it back.
	bulkStep = 1 << 20
	// The mode of fallocate(2) that returns a range's blocks.
	fallocKeepSize  = 0x1
	fallocPunchHole = 0x2
)

// Arena allocates blocks of file-backed memory in one directory. See the
// package comment for what it guarantees.
type Arena struct {
	dir      string
	opts     Options
	made     bool
	nextSlab int64
	// cur is the shared file small blocks are cut from.
	cur     *region
	regions map[*region]struct{}
	blocks  map[*byte]blockRef
	// pending are the regions with a range queued by WriteBack.
	pending []*region
	stats   Stats
	// err is the first write-back error a file reported.
	err    error
	closed bool
}

// region is one file and its mapping: a slab shared by small blocks, or
// the file of one large block.
type region struct {
	f   *os.File
	mem []byte
	// size is the file's preallocated size, which is the mapping's.
	size int64
	slab bool
	// used is a slab's bump offset. Every byte at or after it is zero.
	used int64
	live int
	// punched counts the bytes of freed blocks whose disk space was
	// returned: size - punched is what the file still holds.
	punched int64
	// [pendLo, pendHi) is queued for write-back; pendHi 0 means nothing.
	pendLo, pendHi int64
}

// blockRef locates a live block: its region, its offset and its size
// rounded up to blockAlign.
type blockRef struct {
	r      *region
	off, n int64
}

// New returns an arena that keeps its files in dir, which it creates on the
// first allocation and removes on Close. dir must be on a filesystem whose
// files are not memory (not tmpfs): the caller checks.
func New(dir string, opts Options) (*Arena, error) {
	if dir == "" {
		return nil, errors.New("spillfile: no directory")
	}
	if opts.SlabBytes <= 0 {
		opts.SlabBytes = DefaultSlabBytes
	}
	opts.SlabBytes = roundUp(opts.SlabBytes, pageSize())
	return &Arena{dir: dir, opts: opts, nextSlab: min(firstSlabBytes, opts.SlabBytes),
		regions: map[*region]struct{}{}, blocks: map[*byte]blockRef{}}, nil
}

func pageSize() int64 { return int64(os.Getpagesize()) }

func roundUp(n, to int64) int64 { return (n + to - 1) / to * to }

// Alloc returns a zeroed block of n bytes, aligned to 16 bytes. It stays
// valid until it is freed, resized or the arena is closed.
func (a *Arena) Alloc(n int64) ([]byte, error) {
	if a.closed {
		return nil, errors.New("spillfile: the arena is closed")
	}
	if n < 0 {
		return nil, fmt.Errorf("spillfile: negative block size %d", n)
	}
	if n == 0 {
		return nil, nil
	}
	size := roundUp(n, blockAlign)
	if size > a.opts.SlabBytes/4 {
		r, err := a.newRegion(roundUp(size, pageSize()), false)
		if err != nil {
			return nil, err
		}
		return a.place(r, 0, n, size), nil
	}
	if a.cur == nil || a.cur.used+size > int64(len(a.cur.mem)) {
		r, err := a.newRegion(max(a.nextSlab, roundUp(size, pageSize())), true)
		if err != nil {
			return nil, err
		}
		a.cur = r
		a.nextSlab = min(2*a.nextSlab, a.opts.SlabBytes)
	}
	r := a.cur
	off := r.used
	r.used += size
	return a.place(r, off, n, size), nil
}

func (a *Arena) place(r *region, off, n, size int64) []byte {
	r.live++
	mem := r.mem[off : off+n : off+n]
	a.blocks[unsafe.SliceData(mem)] = blockRef{r: r, off: off, n: size}
	return mem
}

// newRegion creates, unlinks, preallocates and maps a file of size bytes.
func (a *Arena) newRegion(size int64, slab bool) (*region, error) {
	if a.opts.Reserve != nil {
		if err := a.opts.Reserve(size); err != nil {
			return nil, err
		}
	}
	if !a.made {
		if err := os.MkdirAll(a.dir, 0o700); err != nil {
			return nil, fmt.Errorf("spillfile: %w", err)
		}
		a.made = true
	}
	a.stats.Created++
	name := filepath.Join(a.dir, fmt.Sprintf("%06d.spill", a.stats.Created))
	f, err := os.OpenFile(name, os.O_RDWR|os.O_CREATE|os.O_EXCL, 0o600)
	if err != nil {
		return nil, fmt.Errorf("spillfile: %w", err)
	}
	// Unlinked at once: the file lives as long as this descriptor and its
	// mapping, and its space returns when the process ends, however it
	// ends.
	if err := os.Remove(name); err != nil {
		_ = f.Close()
		return nil, fmt.Errorf("spillfile: %w", err)
	}
	if err := preallocate(f, 0, size); err != nil {
		_ = f.Close()
		return nil, fmt.Errorf("spillfile: preallocate %d bytes in %s: %w", size, a.dir, err)
	}
	mem, err := mapFile(f, size)
	if err != nil {
		_ = f.Close()
		return nil, err
	}
	r := &region{f: f, mem: mem, size: size, slab: slab}
	a.regions[r] = struct{}{}
	a.stats.Files++
	a.grew(size)
	return r, nil
}

func (a *Arena) grew(n int64) {
	a.stats.DiskBytes += n
	a.stats.PeakBytes = max(a.stats.PeakBytes, a.stats.DiskBytes)
}

func mapFile(f *os.File, size int64) ([]byte, error) {
	if int64(int(size)) != size {
		return nil, fmt.Errorf("spillfile: a mapping of %d bytes does not fit the address space", size)
	}
	mem, err := syscall.Mmap(int(f.Fd()), 0, int(size), syscall.PROT_READ|syscall.PROT_WRITE, syscall.MAP_SHARED)
	if err != nil {
		return nil, fmt.Errorf("spillfile: map %d bytes: %w", size, err)
	}
	return mem, nil
}

// preallocate reserves [off, off+n) of f on disk, extending the file, so
// that no later write to a mapped page of it can fail for lack of space.
// Where the filesystem has no fallocate it writes zeros.
func preallocate(f *os.File, off, n int64) error {
	var err error
	for range 1000 {
		// A signal can interrupt a long allocation, which then starts
		// over.
		if err = syscall.Fallocate(int(f.Fd()), 0, off, n); err != syscall.EINTR {
			break
		}
	}
	if err == nil {
		return nil
	}
	if !errors.Is(err, syscall.EOPNOTSUPP) && !errors.Is(err, syscall.ENOSYS) && err != syscall.EINTR {
		return err
	}
	zeros := make([]byte, 1<<20)
	for done := int64(0); done < n; {
		chunk := min(int64(len(zeros)), n-done)
		if _, err := f.WriteAt(zeros[:chunk], off+done); err != nil {
			return err
		}
		done += chunk
	}
	return nil
}

// Resize returns a block of n bytes that starts with the first
// min(len(mem), n) bytes of mem; any further bytes are zero. mem is invalid
// afterwards: the block may have moved. A block grows in place when it has
// a file of its own or is the last one cut from its slab.
func (a *Arena) Resize(mem []byte, n int64) ([]byte, error) {
	if a.closed {
		return nil, errors.New("spillfile: the arena is closed")
	}
	if len(mem) == 0 {
		return a.Alloc(n)
	}
	if n <= 0 {
		a.Free(mem)
		return nil, nil
	}
	key := unsafe.SliceData(mem)
	ref, ok := a.blocks[key]
	if !ok {
		return nil, errors.New("spillfile: Resize of a block this arena does not hold")
	}
	old := int64(len(mem))
	size := roundUp(n, blockAlign)
	r := ref.r
	if !r.slab {
		return a.resizeOwn(r, key, old, n, size)
	}
	if n < old {
		// What the block gives up reads as zero if it grows again.
		if err := a.zero(r, ref.off+n, old-n); err != nil {
			return nil, err
		}
	}
	switch {
	case ref.off+ref.n == r.used && ref.off+size <= int64(len(r.mem)):
		r.used = ref.off + size
		ref.n = size
	case size <= ref.n:
		// Not the last block of its slab: it keeps its room.
	default:
		moved, err := a.Alloc(n)
		if err != nil {
			return nil, err
		}
		to := a.blocks[unsafe.SliceData(moved)]
		for done := int64(0); done < old; done += bulkStep {
			step := min(bulkStep, old-done)
			copy(moved[done:done+step], mem[done:done+step])
			if err := a.syncRange(to.r, to.off+done, step); err != nil {
				a.Free(moved)
				return nil, err
			}
		}
		a.Free(mem)
		return moved, nil
	}
	a.blocks[key] = ref
	return r.mem[ref.off : ref.off+n : ref.off+n], nil
}

// resizeOwn resizes the block that has the file r to itself.
func (a *Arena) resizeOwn(r *region, key *byte, old, n, size int64) ([]byte, error) {
	mapped := int64(len(r.mem))
	want := roundUp(size, pageSize())
	if want > mapped {
		if a.opts.Reserve != nil {
			if err := a.opts.Reserve(want - mapped); err != nil {
				return nil, err
			}
		}
		if err := preallocate(r.f, mapped, want-mapped); err != nil {
			return nil, fmt.Errorf("spillfile: preallocate %d bytes in %s: %w", want-mapped, a.dir, err)
		}
	}
	if n < old {
		// Only what stays mapped needs zeroing: the rest is cut off.
		if err := a.zero(r, n, min(old, want)-n); err != nil {
			return nil, err
		}
	}
	if want != mapped {
		// The file keeps the data while the mapping is replaced. A
		// mapping never extends past its file: the file shrinks only
		// after the old mapping is gone.
		if err := syscall.Munmap(r.mem); err != nil {
			return nil, fmt.Errorf("spillfile: unmap: %w", err)
		}
		r.mem = nil
		delete(a.blocks, key)
		if want < mapped {
			if err := r.f.Truncate(want); err != nil {
				a.drop(r)
				return nil, fmt.Errorf("spillfile: truncate: %w", err)
			}
		}
		mem, err := mapFile(r.f, want)
		if err != nil {
			a.drop(r)
			return nil, err
		}
		r.mem, r.size = mem, want
		a.grew(want - mapped)
	}
	block := r.mem[:n:n]
	a.blocks[unsafe.SliceData(block)] = blockRef{r: r, n: size}
	return block, nil
}

// Free releases a block. A block with a file of its own returns its disk
// space at once; a slab returns its space when its last block is freed.
func (a *Arena) Free(mem []byte) {
	if a.closed || len(mem) == 0 {
		return
	}
	key := unsafe.SliceData(mem)
	ref, ok := a.blocks[key]
	if !ok {
		return
	}
	delete(a.blocks, key)
	r := ref.r
	r.live--
	if r.live == 0 {
		a.sample(r)
		a.drop(r)
		return
	}
	// The slab lives on for its other blocks. The freed block's whole
	// pages go back to the filesystem; nothing touches them again (a slab
	// only ever hands out the room past its last block).
	page := pageSize()
	lo, hi := roundUp(ref.off, page), (ref.off+ref.n)/page*page
	if hi > lo && syscall.Fallocate(int(r.f.Fd()), fallocPunchHole|fallocKeepSize, lo, hi-lo) == nil {
		r.punched += hi - lo
		a.stats.DiskBytes -= hi - lo
	}
}

// zero clears [off, off+n) of r's mapping, having the kernel write each
// step back before the next, so the arena's own writes never pile up as
// dirty pages.
func (a *Arena) zero(r *region, off, n int64) error {
	for done := int64(0); done < n; done += bulkStep {
		step := min(bulkStep, n-done)
		clear(r.mem[off+done : off+done+step])
		if err := a.syncRange(r, off+done, step); err != nil {
			return err
		}
	}
	return nil
}

// WriteBack queues bytes [off, off+n) of a block for write-back; Settle
// does it. A page of a shared mapping that was written to stays dirty
// until the kernel's flusher gets to it, and a cgroup whose pages are
// mostly dirty has nothing to reclaim, so a writer that is faster than the
// flusher is OOM-killed at its memory limit (measured: package comment).
// The planner therefore queues what it wrote and settles after a bounded
// amount of writing.
func (a *Arena) WriteBack(block []byte, off, n int64) error {
	if a.closed {
		return errors.New("spillfile: the arena is closed")
	}
	if n <= 0 || len(block) == 0 {
		return nil
	}
	ref, ok := a.blocks[unsafe.SliceData(block)]
	if !ok {
		return errors.New("spillfile: WriteBack of a block this arena does not hold")
	}
	if off < 0 || off+n > int64(len(block)) {
		return fmt.Errorf("spillfile: WriteBack of [%d, %d) in a block of %d bytes", off, off+n, len(block))
	}
	r := ref.r
	lo, hi := ref.off+off, ref.off+off+n
	if r.pendHi == 0 {
		r.pendLo, r.pendHi = lo, hi
		a.pending = append(a.pending, r)
	} else {
		r.pendLo, r.pendHi = min(r.pendLo, lo), max(r.pendHi, hi)
	}
	return nil
}

// Settle writes back the dirty pages of every queued range and waits for
// them: they are clean page cache afterwards, which the kernel can drop.
// Each file is written in one request spanning its queued ranges (the
// pages between them that are clean cost nothing). A failed write-back is
// returned, and remembered for Check.
func (a *Arena) Settle() error {
	if a.closed {
		return errors.New("spillfile: the arena is closed")
	}
	var first error
	for _, r := range a.pending {
		if r.pendHi == 0 {
			continue // released since
		}
		if err := a.syncRange(r, r.pendLo, r.pendHi-r.pendLo); err != nil && first == nil {
			first = err
		}
		r.pendHi = 0
	}
	a.pending = a.pending[:0]
	return first
}

// syncRange starts write-back of the dirty pages of r's file in
// [off, off+n) and waits for it. It does not make them durable (no journal
// commit, no device flush): the files are scratch.
func (a *Arena) syncRange(r *region, off, n int64) error {
	var err error
	for range 1000 {
		err = syscall.SyncFileRange(int(r.f.Fd()), off, n, syncFileRangeWaitBefore|syncFileRangeWrite|syncFileRangeWaitAfter)
		if err != syscall.EINTR {
			break
		}
	}
	if errors.Is(err, syscall.ENOSYS) || errors.Is(err, syscall.EINVAL) || errors.Is(err, syscall.ESPIPE) {
		err = r.f.Sync()
	}
	if err != nil {
		err = fmt.Errorf("spillfile: the staging volume lost spilled data (write-back failed): %w", err)
		if a.err == nil {
			a.err = err
		}
	}
	return err
}

// drop unmaps and closes a region; its unlinked file's space returns.
func (a *Arena) drop(r *region) {
	a.stats.DiskBytes -= r.size - r.punched
	r.pendHi = 0
	if r.mem != nil {
		_ = syscall.Munmap(r.mem)
		r.mem = nil
	}
	_ = r.f.Close()
	delete(a.regions, r)
	a.stats.Files--
	if a.cur == r {
		a.cur = nil
	}
}

// sample records a write-back error the kernel has for r's file. A dirty
// page of a mapping is written back by the kernel on its own schedule, and
// a failure is reported to nobody until the file is synced; a page that
// failed and was then evicted would be read back stale. Waiting for the
// write-back in flight (without starting any) is enough to collect the
// error, and costs nothing when none is.
func (a *Arena) sample(r *region) {
	var err error
	for range 1000 {
		if err = syscall.SyncFileRange(int(r.f.Fd()), 0, 0, syncFileRangeWaitBefore); err != syscall.EINTR {
			break
		}
	}
	if errors.Is(err, syscall.ENOSYS) || errors.Is(err, syscall.EINVAL) || errors.Is(err, syscall.ESPIPE) {
		// No sync_file_range here: a full sync reports the same error.
		err = r.f.Sync()
	}
	if err != nil && a.err == nil {
		a.err = fmt.Errorf("spillfile: the staging volume lost spilled data (write-back failed): %w", err)
	}
}

// Check reports whether any file of the arena, held now or released
// earlier, had a write-back error. The caller runs it once planning is
// done and before it trusts the result.
func (a *Arena) Check() error {
	if a == nil {
		return nil
	}
	for r := range a.regions {
		a.sample(r)
	}
	return a.err
}

// Close unmaps every block and removes the directory. Every block is
// invalid afterwards. It is safe to call twice.
func (a *Arena) Close() error {
	if a == nil || a.closed {
		return nil
	}
	a.closed = true
	for r := range a.regions {
		a.drop(r)
	}
	a.blocks = nil
	if a.made {
		// Its files were unlinked when they were created.
		if err := os.Remove(a.dir); err != nil && !os.IsNotExist(err) {
			return fmt.Errorf("spillfile: %w", err)
		}
	}
	return nil
}

// Contains reports whether addr lies in a mapped file of the arena.
func (a *Arena) Contains(addr uintptr) bool {
	if a == nil {
		return false
	}
	for r := range a.regions {
		if len(r.mem) == 0 {
			continue
		}
		base := uintptr(unsafe.Pointer(unsafe.SliceData(r.mem)))
		if addr >= base && addr-base < uintptr(len(r.mem)) {
			return true
		}
	}
	return false
}

// Stats reports the arena's disk use.
func (a *Arena) Stats() Stats {
	if a == nil {
		return Stats{}
	}
	return a.stats
}

// Guard runs fn, which reads and writes the arena's blocks on the calling
// goroutine, and returns a *FaultError when fn faults inside the arena: a
// page the kernel could not read back (an I/O error on the staging volume)
// raises SIGBUS, which would otherwise crash the process. Any other panic
// continues.
func (a *Arena) Guard(fn func() error) (err error) {
	if a == nil {
		return fn()
	}
	old := debug.SetPanicOnFault(true)
	defer debug.SetPanicOnFault(old)
	defer func() {
		r := recover()
		if r == nil {
			return
		}
		if f, ok := r.(interface{ Addr() uintptr }); ok && a.Contains(f.Addr()) {
			err = &FaultError{Addr: f.Addr()}
			return
		}
		panic(r)
	}()
	return fn()
}
