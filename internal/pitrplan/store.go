package pitrplan

import (
	"fmt"
	"math"
	"math/bits"
	"os"
	"reflect"
	"slices"
	"sync"
	"unsafe"

	"github.com/shipstream/bloodraven/internal/pitrarchive"
)

// Spill (issue #266). The structures whose size grows with the number of
// transactions or search positions are arrays of fixed-size records without
// pointers: the per-file transaction index (Txn), the occurrence registry
// (occRec), the plan search's positions, classes, keys and queues, and the
// order check's nodes and edges. Each is allocated through the budget
// (alloc, vec), which keeps it on the Go heap, charged to the memory
// budget, while the charge stays within SpillOptions.HeapMax, and places it
// in the Arena after that. An array in the arena is not charged to the
// memory budget: it is file-backed and costs staging disk, which the
// arena's provider accounts for.
//
// The planner runs the same code over a heap array and a spilled one (both
// are a Go slice), so where an array lives cannot change a plan. It reads
// a spilled array like any slice, and writes it only through arr.set and
// its relatives, which count the pages written: a written page of a file
// mapping stays dirty, and unreclaimable, until it is written back, and
// the kernel does not hold the writer back reliably (spillfile's package
// comment has the measurement). After SpillOptions.DirtyMax bytes of pages
// the budget has the arena write them back and waits, so the dirty spill
// is bounded whatever the kernel does; DirtyMax is charged to the memory
// budget for as long as anything is spilled.
//
// What does not spill, and still refuses PITRPlanTooLarge on memory: GTID
// interval sets (per file, per promotion record, per segment), a file's
// manifest row and source identifiers, arrays smaller than
// SpillOptions.MinBytes, and the binlog event being decoded.

// Arena is storage outside the Go heap for the planner's arrays.
// pitr-download provides one backed by files on the staging volume
// (internal/pitrplan/spillfile); the planner itself does no I/O. A block is
// zeroed when allocated, aligned for any fixed-size record, and valid until
// it is freed, resized or the provider closes the arena, which it does only
// after planning: a Plan holds no arena memory.
//
// A *Refusal with PITRPlanTooLarge means the arena has no room (the staging
// volume's limit); the budget then tries memory. Any other error ends
// planning as an error.
type Arena interface {
	Alloc(n int64) ([]byte, error)
	// Resize returns a block of n bytes that starts with the first
	// min(len(mem), n) bytes of mem, the rest zeroed. mem is invalid
	// afterwards. On a refusal mem is unchanged.
	Resize(mem []byte, n int64) ([]byte, error)
	Free(mem []byte)
	// WriteBack queues the pages of bytes [off, off+n) of a block to be
	// written back to the arena's storage; Settle writes back everything
	// queued and waits, so that those pages are reclaimable again.
	WriteBack(block []byte, off, n int64) error
	Settle() error
}

// SpillOptions configure where the planner's arrays live.
type SpillOptions struct {
	// Arena receives the arrays that leave the heap. Nil: none do.
	Arena Arena
	// HeapMax: an array stays on the heap while the memory charge
	// including it is at most HeapMax. Charges for what cannot spill use
	// the whole budget. 0 spills every array of MinBytes or more.
	HeapMax int64
	// MinBytes: a smaller array always stays on the heap; its bookkeeping
	// would cost more than it. Default 4096.
	MinBytes int64
	// DirtyMax bounds the spilled pages written and not yet written back,
	// in bytes. It is charged to the memory budget from the first spilled
	// array on. Default DefaultSpillDirtyBytes; at least 16 pages.
	DirtyMax int64
}

// DefaultSpillDirtyBytes is SpillOptions.DirtyMax when unset.
const DefaultSpillDirtyBytes = 16 << 20

// defaultSpillMinBytes is SpillOptions.MinBytes when unset.
const defaultSpillMinBytes = 4096

// spillBlockBytes is charged to the memory budget per spilled array for
// its bookkeeping: the dirty-page state and its entries in the budget's
// tables. The page bitmap (one bit per page) is charged on top.
const spillBlockBytes = 160

var (
	spillPageSize  = uintptr(os.Getpagesize())
	spillPageShift = uint(bits.TrailingZeros(uint(os.Getpagesize())))
)

// SetSpill configures the spill. A zero SpillOptions restores the default:
// everything in memory.
func (b *Budget) SetSpill(o SpillOptions) {
	b.arena = o.Arena
	b.spillAt = min(max(o.HeapMax, 0), b.limit)
	b.spillMin = o.MinBytes
	if b.spillMin <= 0 {
		b.spillMin = defaultSpillMinBytes
	}
	dirty := o.DirtyMax
	if dirty <= 0 {
		dirty = DefaultSpillDirtyBytes
	}
	b.dirtyMax = max(int(dirty>>spillPageShift), 16)
}

// Spilling reports whether the budget has an arena.
func (b *Budget) Spilling() bool { return b.arena != nil }

// Spilled is the size of the arrays in the arena now.
func (b *Budget) Spilled() int64 { return b.spilled }

// SpillPeak is the largest value Spilled had.
func (b *Budget) SpillPeak() int64 { return b.spillPeak }

// SpillErr is the first error of a write-back. Writes to spilled arrays
// cannot fail where they happen, so the budget keeps the error: every
// later allocation returns it, and Run checks it before it returns a plan.
func (b *Budget) SpillErr() error { return b.spillErr }

// dirtyState tracks the pages of one spilled array written since they
// were last written back.
type dirtyState struct {
	mem []byte
	// page0 is the page number of the block's first byte.
	page0 uintptr
	bits  []uint64
	count int
	// first and last bound the marked pages (block-relative page numbers).
	first, last int
	listed      bool
}

// reserve decides where an array of n bytes lives. It returns the array's
// arena block and its dirty-page state, or nil after charging n to the
// memory budget.
func (b *Budget) reserve(n int64, what string) (*dirtyState, error) {
	if b.spillErr != nil {
		return nil, b.spillErr
	}
	if b.arena == nil || n < b.spillMin || n <= b.spillAt-b.used {
		return nil, b.Charge(n, what)
	}
	if err := b.chargeSpilling(n); err != nil {
		return nil, b.spillRefused(err, n, what)
	}
	mem, err := b.arena.Alloc(n)
	if err != nil {
		b.releaseSpilling(n)
		return nil, b.spillRefused(err, n, what)
	}
	if int64(len(mem)) != n {
		b.releaseSpilling(n)
		return nil, fmt.Errorf("pitrplan: the spill arena returned %d bytes for %d (%s)", len(mem), n, what)
	}
	return b.track(mem), nil
}

// spillOverhead is the memory charge for spilling an array of n bytes.
func spillOverhead(n int64) int64 {
	pages := (n + int64(spillPageSize) - 1) >> spillPageShift
	return spillBlockBytes + (pages+1+63)/64*8
}

// chargeSpilling charges the memory a spilled array of n bytes costs: its
// bookkeeping and, for the first one, the dirty window.
func (b *Budget) chargeSpilling(n int64) error {
	need := spillOverhead(n)
	if b.spilledBlocks == 0 {
		need += int64(b.dirtyMax) << spillPageShift
	}
	if need > b.limit-b.used {
		return refuse(pitrarchive.TokenPlanTooLarge, "spilling needs %d bytes of memory for its own bookkeeping, which the budget does not have", need)
	}
	b.used += need
	b.peak = max(b.peak, b.used)
	b.spilledBlocks++
	return nil
}

func (b *Budget) releaseSpilling(n int64) {
	b.spilledBlocks--
	free := spillOverhead(n)
	if b.spilledBlocks == 0 {
		free += int64(b.dirtyMax) << spillPageShift
	}
	b.Release(free)
}

// spillRefused handles a failed spill of an array of n bytes. When the
// arena only has no room, the array may still fit the memory budget (nil:
// it is charged); when neither has room the refusal names both limits.
func (b *Budget) spillRefused(err error, n int64, what string) error {
	r, ok := RefusalOf(err)
	if !ok || r.Token != pitrarchive.TokenPlanTooLarge {
		return err
	}
	if n <= b.limit-b.used {
		return b.Charge(n, what)
	}
	return refuse(pitrarchive.TokenPlanTooLarge, "planning needs %d more bytes (%s; %d transactions indexed, %d bytes spilled to the staging volume), which fit neither limit: the memory budget of %d bytes has %d left, and %s",
		n, what, b.indexed, b.spilled, b.limit, b.limit-b.used, r.Message)
}

// track starts accounting for a block the arena returned.
func (b *Budget) track(mem []byte) *dirtyState {
	base := uintptr(unsafe.Pointer(unsafe.SliceData(mem)))
	pages := int((base+uintptr(len(mem))-1)>>spillPageShift-base>>spillPageShift) + 1
	d := &dirtyState{mem: mem, page0: base >> spillPageShift, bits: make([]uint64, (pages+63)/64)}
	b.spilled += int64(len(mem))
	b.spillPeak = max(b.spillPeak, b.spilled)
	if b.attempt != nil {
		b.attempt.blocks[d] = struct{}{}
	}
	return d
}

// untrack stops accounting for a block: the caller frees it, or the arena
// took it back. Its marked pages no longer count: they are the arena's.
func (b *Budget) untrack(d *dirtyState) {
	b.spilled -= int64(len(d.mem))
	b.dirtyPages -= d.count
	d.count = 0
	for c := b.attempt; c != nil; c = c.outer {
		delete(c.blocks, d)
	}
	b.releaseSpilling(int64(len(d.mem)))
	d.mem = nil
}

// freeBlock returns a block to the arena. Its written pages are written
// back first: a block cut from a shared file leaves them behind, dirty,
// where nothing would count them.
func (b *Budget) freeBlock(d *dirtyState) {
	b.syncBlock(d)
	mem := d.mem
	b.untrack(d)
	b.arena.Free(mem)
}

// mark records that pages [first, last] of d's block (block-relative) are
// about to be written. When that would pass the bound, everything written
// so far is written back first, so the pages marked here are still marked
// when the caller writes them.
func (b *Budget) mark(d *dirtyState, first, last int) {
	fresh := 0
	for pg := first; pg <= last; pg++ {
		if d.bits[pg>>6]&(1<<(pg&63)) == 0 {
			fresh++
		}
	}
	if fresh == 0 {
		return
	}
	if b.dirtyPages+fresh > b.dirtyMax {
		b.flush()
		fresh = last - first + 1
	}
	if d.count == 0 {
		d.first, d.last = first, last
		if !d.listed {
			d.listed = true
			b.dirty = append(b.dirty, d)
		}
	} else {
		d.first, d.last = min(d.first, first), max(d.last, last)
	}
	for pg := first; pg <= last; pg++ {
		d.bits[pg>>6] |= 1 << (pg & 63)
	}
	d.count += fresh
	b.dirtyPages += fresh
}

// flush writes every marked page back and waits.
func (b *Budget) flush() {
	for _, d := range b.dirty {
		d.listed = false
		b.queueBlock(d)
	}
	b.dirty = b.dirty[:0]
	b.dirtyPages = 0
	b.flushes++
	b.settle()
}

// syncBlock writes d's marked pages back, waits, and unmarks them.
func (b *Budget) syncBlock(d *dirtyState) {
	if d.count == 0 || d.mem == nil {
		return
	}
	b.queueBlock(d)
	b.settle()
}

// queueBlock queues d's marked pages for write-back and unmarks them; the
// caller settles.
func (b *Budget) queueBlock(d *dirtyState) {
	if d.count == 0 || d.mem == nil {
		return
	}
	base := uintptr(unsafe.Pointer(unsafe.SliceData(d.mem)))
	lo := max(int64((d.page0+uintptr(d.first))<<spillPageShift)-int64(base), 0)
	hi := min(int64((d.page0+uintptr(d.last)+1)<<spillPageShift)-int64(base), int64(len(d.mem)))
	b.spillFailed(b.arena.WriteBack(d.mem, lo, hi-lo))
	clear(d.bits[d.first>>6 : d.last>>6+1])
	b.dirtyPages -= d.count
	d.count = 0
}

func (b *Budget) settle() { b.spillFailed(b.arena.Settle()) }

// spillFailed keeps the first write-back error.
func (b *Budget) spillFailed(err error) {
	if err != nil && b.spillErr == nil {
		b.spillErr = fmt.Errorf("pitrplan: writing spilled data back: %w", err)
	}
}

// Checkpoint brackets work that may be abandoned: one download attempt,
// whose partial index is garbage when the attempt fails, or a read whose
// index is needed only until its commit times are read (the planner's
// reportRead).
type Checkpoint struct {
	b      *Budget
	used   int64
	blocks map[*dirtyState]struct{}
	// outer is the checkpoint this one was started inside, or nil.
	outer *Checkpoint
}

// Checkpoint starts recording. Exactly one of Keep and Undo ends it.
// Checkpoints nest (a report read brackets the fetcher's download
// attempts): an inner one ends before the one it was started inside, and
// what it keeps is then the outer one's to keep or undo.
func (b *Budget) Checkpoint() *Checkpoint {
	c := &Checkpoint{b: b, used: b.used, blocks: map[*dirtyState]struct{}{}, outer: b.attempt}
	b.attempt = c
	return c
}

// Keep ends the checkpoint and keeps what was allocated since.
func (c *Checkpoint) Keep() {
	if c.b.attempt == c {
		c.b.attempt = c.outer
	}
	if c.outer != nil {
		for d := range c.blocks {
			c.outer.blocks[d] = struct{}{}
		}
	}
	c.blocks = nil
}

// Undo returns every memory charge made since the checkpoint and frees
// every arena block allocated since that is still held.
func (c *Checkpoint) Undo() {
	b := c.b
	if b.attempt == c {
		b.attempt = c.outer
	}
	for d := range c.blocks {
		b.freeBlock(d)
	}
	c.blocks = nil
	// Less may be charged now than at the checkpoint (something older was
	// released since): then there is nothing to return.
	if n := b.used - c.used; n > 0 {
		b.Release(n)
	}
}

// arr is an array of n records of T, on the heap or in the arena. T is a
// fixed-size type without pointers: the garbage collector never scans
// arena memory, and a record must mean the same wherever it is stored.
// Read s freely. Write it only through set, fill, copyIn, sortFunc and
// vec.push: a spilled array's written pages are counted (see the comment
// at the top of this file).
type arr[T any] struct {
	s []T
	b *Budget
	// d is the dirty-page state of the arena block holding s, or nil when
	// s is on the heap and charged to the memory budget.
	d *dirtyState
}

func arrayBytes[T any](n int) (int64, error) {
	var zero T
	size := int64(unsafe.Sizeof(zero))
	if n < 0 || (size > 0 && int64(n) > math.MaxInt64/size) {
		return 0, fmt.Errorf("pitrplan: an array of %d records of %d bytes cannot be allocated", n, size)
	}
	return int64(n) * size, nil
}

// alloc returns a zeroed array of n records.
func alloc[T any](b *Budget, n int, what string) (arr[T], error) {
	a := arr[T]{b: b}
	if n == 0 {
		return a, nil
	}
	bytes, err := arrayBytes[T](n)
	if err != nil {
		return a, err
	}
	d, err := b.reserve(bytes, what)
	if err != nil {
		return a, err
	}
	if d == nil {
		a.s = make([]T, n)
		return a, nil
	}
	a.d, a.s = d, view[T](d.mem, n)
	return a, nil
}

// view is the arena block mem as n records of T.
func view[T any](mem []byte, n int) []T {
	requirePointerFree[T]()
	return unsafe.Slice((*T)(unsafe.Pointer(unsafe.SliceData(mem))), n)
}

var pointerFreeTypes sync.Map // reflect.Type → bool

// requirePointerFree panics for a record type that must not live in the
// arena. It is a programming error, which every spilled test would hit.
func requirePointerFree[T any]() {
	t := reflect.TypeFor[T]()
	ok, seen := pointerFreeTypes.Load(t)
	if !seen {
		ok = pointerFree(t)
		pointerFreeTypes.Store(t, ok)
	}
	if !ok.(bool) {
		panic(fmt.Sprintf("pitrplan: internal error: %s holds pointers and cannot be spilled", t))
	}
}

func pointerFree(t reflect.Type) bool {
	switch t.Kind() {
	case reflect.Bool, reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64,
		reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64, reflect.Uintptr,
		reflect.Float32, reflect.Float64, reflect.Complex64, reflect.Complex128:
		return true
	case reflect.Array:
		return pointerFree(t.Elem())
	case reflect.Struct:
		for i := range t.NumField() {
			if !pointerFree(t.Field(i).Type) {
				return false
			}
		}
		return true
	default:
		return false
	}
}

// touch marks the pages of records [lo, hi) before they are written.
func (a *arr[T]) touch(lo, hi int) {
	var zero T
	size := unsafe.Sizeof(zero)
	if hi <= lo || size == 0 {
		return
	}
	d := a.d
	base := uintptr(unsafe.Pointer(unsafe.SliceData(d.mem)))
	first := int((base+uintptr(lo)*size)>>spillPageShift - d.page0)
	last := int((base+uintptr(hi)*size-1)>>spillPageShift - d.page0)
	if first == last && d.bits[first>>6]&(1<<(first&63)) != 0 {
		return
	}
	// A range larger than the bound is marked, and so written back, in
	// steps; the caller writes it in the same steps (see fill, copyIn).
	a.b.mark(d, first, last)
}

// writeStep is how many records a bulk write marks at a time: half the
// dirty bound, so the pages it marks are never flushed before it writes
// them.
func (a *arr[T]) writeStep() int {
	var zero T
	size := max(int(unsafe.Sizeof(zero)), 1)
	return max((a.b.dirtyMax/2)<<spillPageShift/size, 1)
}

// set writes record i.
func (a *arr[T]) set(i int, v T) {
	if a.d != nil {
		a.touch(i, i+1)
	}
	a.s[i] = v
}

// fill sets records [lo, hi) to v.
func (a *arr[T]) fill(lo, hi int, v T) {
	if a.d == nil {
		for i := lo; i < hi; i++ {
			a.s[i] = v
		}
		return
	}
	for step := a.writeStep(); lo < hi; lo += step {
		end := min(lo+step, hi)
		a.touch(lo, end)
		for i := lo; i < end; i++ {
			a.s[i] = v
		}
	}
}

// copyIn copies src to records [at, at+len(src)).
func (a *arr[T]) copyIn(at int, src []T) {
	if a.d == nil {
		copy(a.s[at:], src)
		return
	}
	for step := a.writeStep(); len(src) > 0; at += step {
		n := min(step, len(src))
		a.touch(at, at+n)
		copy(a.s[at:at+n], src[:n])
		src = src[n:]
	}
}

// sortFunc sorts the array by cmp, which must be a strict total order
// (no two records compare equal), so the result does not depend on how
// it is sorted. A heap array is sorted by the standard library. A
// spilled one is partitioned with counted writes until a range fits one
// write step, which the standard library then sorts in place.
func (a *arr[T]) sortFunc(cmp func(x, y T) int) {
	if a.d == nil {
		slices.SortFunc(a.s, cmp)
		return
	}
	a.quicksort(0, len(a.s), cmp, a.writeStep())
}

func (a *arr[T]) quicksort(lo, hi int, cmp func(x, y T) int, leaf int) {
	for hi-lo > leaf {
		// The median of three keeps sorted and reversed inputs balanced;
		// the positions are spread so that a pattern in the input does
		// not meet them.
		n := hi - lo
		x, y, z := a.s[lo+n/4], a.s[lo+n/2], a.s[lo+n-1-n/4]
		if cmp(x, y) > 0 {
			x, y = y, x
		}
		if cmp(y, z) > 0 {
			y = z
			if cmp(x, y) > 0 {
				y = x
			}
		}
		pivot := y
		// Hoare partition: [lo, j] ≤ pivot ≤ [i, hi).
		i, j := lo, hi-1
		for i <= j {
			for cmp(a.s[i], pivot) < 0 {
				i++
			}
			for cmp(a.s[j], pivot) > 0 {
				j--
			}
			if i <= j {
				if i != j {
					vi, vj := a.s[i], a.s[j]
					a.set(i, vj)
					a.set(j, vi)
				}
				i++
				j--
			}
		}
		// Recurse into the smaller side: the stack stays logarithmic.
		if j+1-lo < hi-i {
			a.quicksort(lo, j+1, cmp, leaf)
			lo = i
		} else {
			a.quicksort(i, hi, cmp, leaf)
			hi = j + 1
		}
	}
	if hi-lo > 1 {
		a.touch(lo, hi)
		slices.SortFunc(a.s[lo:hi], cmp)
	}
}

// free releases the array; it is empty afterwards and may be freed again.
func (a *arr[T]) free() {
	if a.b == nil {
		return
	}
	if a.d != nil {
		a.b.freeBlock(a.d)
	} else if n := len(a.s); n > 0 {
		bytes, _ := arrayBytes[T](n)
		a.b.Release(bytes)
	}
	a.s, a.d = nil, nil
}

// resize makes the array n records long, keeping the first
// min(len, n). The array may move, and may move between the heap and the
// arena: a growing array is placed like a new one. Records past the old
// length are zero.
func (a *arr[T]) resize(n int, what string) error {
	if n == len(a.s) {
		return nil
	}
	if n == 0 {
		a.free()
		return nil
	}
	bytes, err := arrayBytes[T](n)
	if err != nil {
		return err
	}
	if a.d != nil {
		return a.resizeSpilled(n, bytes, what)
	}
	old, _ := arrayBytes[T](len(a.s))
	d, err := a.b.reserve(bytes, what)
	if err != nil {
		return err
	}
	moved := arr[T]{b: a.b, d: d}
	if d == nil {
		moved.s = make([]T, n)
	} else {
		moved.s = view[T](d.mem, n)
	}
	moved.copyIn(0, a.s[:min(n, len(a.s))])
	a.b.Release(old)
	*a = moved
	return nil
}

func (a *arr[T]) resizeSpilled(n int, bytes int64, what string) error {
	b, d := a.b, a.d
	if b.spillErr != nil {
		return b.spillErr
	}
	// The arena may move the block: its pages are written back first, so
	// none of them is counted against a block that no longer exists.
	b.syncBlock(d)
	oldMem := d.mem
	// The bookkeeping of the new size is charged before the old is
	// returned.
	if err := b.chargeSpilling(bytes); err != nil {
		return err
	}
	mem, err := b.arena.Resize(oldMem, bytes)
	if err == nil && int64(len(mem)) != bytes {
		err = fmt.Errorf("pitrplan: the spill arena returned %d bytes for %d (%s)", len(mem), bytes, what)
	}
	if err != nil {
		b.releaseSpilling(bytes)
		// The arena has no room for the growth; memory may.
		if err := b.spillRefused(err, bytes, what); err != nil {
			return err
		}
		s := make([]T, n)
		copy(s, a.s)
		b.freeBlock(d)
		a.d, a.s = nil, s
		return nil
	}
	b.untrack(d)
	a.d = b.track(mem)
	a.s = view[T](mem, n)
	return nil
}

// vecMinCap is a growing array's first capacity, in records. One: a file
// with a single transaction is charged a single record, also while it is
// being indexed.
const vecMinCap = 1

// vec is an array that grows by appending. Its capacity doubles, and it is
// charged (or spilled) at its capacity, not its length.
type vec[T any] struct {
	a arr[T]
	n int
}

func newVec[T any](b *Budget) vec[T] { return vec[T]{a: arr[T]{b: b}} }

// push appends x.
func (v *vec[T]) push(x T, what string) error {
	if v.n == len(v.a.s) {
		if v.n > math.MaxInt/2 {
			return fmt.Errorf("pitrplan: %s cannot grow past %d records", what, v.n)
		}
		if err := v.a.resize(max(vecMinCap, 2*v.n), what); err != nil {
			return err
		}
	}
	v.a.set(v.n, x)
	v.n++
	return nil
}

// reserve grows the capacity to at least n records.
func (v *vec[T]) reserve(n int, what string) error {
	if n <= len(v.a.s) {
		return nil
	}
	return v.a.resize(n, what)
}

// items are the records pushed so far, to read. The slice is invalid after
// the next push, reserve, trim or free.
func (v *vec[T]) items() []T { return v.a.s[:v.n] }

// set overwrites record i < len(items()).
func (v *vec[T]) set(i int, x T) { v.a.set(i, x) }

// trim gives back the unused capacity.
func (v *vec[T]) trim(what string) error { return v.a.resize(v.n, what) }

func (v *vec[T]) free() {
	v.a.free()
	v.n = 0
}
