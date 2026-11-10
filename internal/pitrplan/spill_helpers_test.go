package pitrplan

import (
	"bytes"
	"errors"
	"fmt"
	"path/filepath"
	"testing"
	"unsafe"

	"github.com/shipstream/bloodraven/internal/pitrarchive"
	"github.com/shipstream/bloodraven/internal/pitrplan/spillfile"
)

// memArena is an Arena on the Go heap: the planner's arrays go through
// the spill paths without a file. Tests use it where files are not
// available, and to make the arena refuse or fail.
type memArena struct {
	blocks map[*byte][]uint64
	// limit, when positive, is the arena's room: past it Alloc and Resize
	// refuse PITRPlanTooLarge, as pitr-download's staging budget does.
	limit      int64
	used, peak int64
	allocs     int
	// syncs counts the ranges written back; queued are those waiting for
	// Settle.
	syncs  int
	queued []memRange
	// failAlloc and failSync, when set, are returned by Alloc/Resize and
	// by Settle.
	failAlloc, failSync error
	// onSync observes every range a Settle writes back.
	onSync func(block []byte, off, n int64)
	// shadow, when non-nil, holds for every block what "the disk" has:
	// zeros at first, then whatever each Settle wrote back. A page that
	// differs from its shadow and is not counted as written by the budget
	// was written behind the budget's back (untracked).
	shadow map[*byte][]byte
}

type memRange struct {
	block  []byte
	off, n int64
}

func newMemArena() *memArena { return &memArena{blocks: map[*byte][]uint64{}} }

func (m *memArena) room(n int64) error {
	if m.failAlloc != nil {
		return m.failAlloc
	}
	if m.limit > 0 && m.used+n > m.limit {
		return refuse(pitrarchive.TokenPlanTooLarge, "the test arena has %d bytes, %d in use, and %d more are needed; raise the staging volume's size", m.limit, m.used, n)
	}
	return nil
}

func (m *memArena) Alloc(n int64) ([]byte, error) {
	if n <= 0 {
		return nil, nil
	}
	if err := m.room(n); err != nil {
		return nil, err
	}
	// Words, so the block is aligned for any record.
	words := make([]uint64, (n+7)/8)
	mem := unsafe.Slice((*byte)(unsafe.Pointer(&words[0])), n)
	m.blocks[&mem[0]] = words
	if m.shadow != nil {
		m.shadow[&mem[0]] = make([]byte, n)
	}
	m.used += n
	m.peak = max(m.peak, m.used)
	m.allocs++
	return mem, nil
}

func (m *memArena) Resize(mem []byte, n int64) ([]byte, error) {
	if len(mem) == 0 {
		return m.Alloc(n)
	}
	if _, ok := m.blocks[&mem[0]]; !ok {
		return nil, errors.New("memArena: Resize of an unknown block")
	}
	if n > int64(len(mem)) {
		if err := m.room(n - int64(len(mem))); err != nil {
			return nil, err
		}
	}
	moved, err := m.Alloc(n)
	if err != nil {
		return nil, err
	}
	copy(moved, mem)
	if m.shadow != nil {
		// The arena wrote the moved bytes itself.
		copy(m.shadow[&moved[0]], moved)
	}
	m.Free(mem)
	return moved, nil
}

func (m *memArena) Free(mem []byte) {
	if len(mem) == 0 {
		return
	}
	if _, ok := m.blocks[&mem[0]]; ok {
		delete(m.blocks, &mem[0])
		delete(m.shadow, &mem[0])
		m.used -= int64(len(mem))
	}
}

func (m *memArena) WriteBack(block []byte, off, n int64) error {
	if _, ok := m.blocks[&block[0]]; !ok {
		return errors.New("memArena: WriteBack of an unknown block")
	}
	if off < 0 || n < 0 || off+n > int64(len(block)) {
		return errors.New("memArena: WriteBack outside its block")
	}
	m.queued = append(m.queued, memRange{block, off, n})
	return nil
}

func (m *memArena) Settle() error {
	for _, r := range m.queued {
		m.syncs++
		if m.onSync != nil {
			m.onSync(r.block, r.off, r.n)
		}
		if sh := m.shadow[&r.block[0]]; sh != nil {
			copy(sh[r.off:r.off+r.n], r.block[r.off:r.off+r.n])
		}
	}
	m.queued = m.queued[:0]
	return m.failSync
}

// untracked lists the pages of live blocks whose bytes differ from what
// was last written back although b does not count them as written.
func (m *memArena) untracked(b *Budget) []string {
	counted := map[uintptr]bool{}
	for _, d := range b.dirty {
		if d.count == 0 || d.mem == nil {
			continue
		}
		for pg := d.first; pg <= d.last; pg++ {
			if d.bits[pg>>6]&(1<<(pg&63)) != 0 {
				counted[d.page0+uintptr(pg)] = true
			}
		}
	}
	var out []string
	for key, sh := range m.shadow {
		block := unsafe.Slice(key, len(sh))
		base := uintptr(unsafe.Pointer(key))
		for off := 0; off < len(block); {
			page := (base + uintptr(off)) >> spillPageShift
			end := min(int((page+1)<<spillPageShift-base), len(block))
			if !counted[page] && !bytes.Equal(block[off:end], sh[off:end]) {
				out = append(out, fmt.Sprintf("block of %d bytes, offset %d", len(block), off))
			}
			off = end
		}
	}
	return out
}

// newFileArena returns the production arena (files in a temporary
// directory), or a memArena where the platform has no file mapping.
func newFileArena(t testing.TB) Arena {
	t.Helper()
	a, err := spillfile.New(filepath.Join(t.TempDir(), "spill"), spillfile.Options{SlabBytes: 1 << 20})
	if errors.Is(err, spillfile.ErrUnsupported) {
		return newMemArena()
	}
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := a.Check(); err != nil {
			t.Errorf("spill arena: %v", err)
		}
		_ = a.Close()
	})
	return a
}

// tinySpill spills every array, however small, and writes back after 16
// pages, so that small tests go through every spill path.
func tinySpill(a Arena) SpillOptions {
	return SpillOptions{Arena: a, HeapMax: 0, MinBytes: 1, DirtyMax: 1}
}

// forceSpill makes every budget created until the test ends spill
// everything into one file arena.
func forceSpill(t testing.TB) {
	t.Helper()
	a := newFileArena(t)
	old := testSpill
	testSpill = func() SpillOptions { return tinySpill(a) }
	t.Cleanup(func() { testSpill = old })
}
