//go:build linux

package spillfile

import (
	"bytes"
	"errors"
	"fmt"
	"math/rand"
	"os"
	"path/filepath"
	"testing"
	"unsafe"
)

func newArena(t *testing.T, opts Options) (*Arena, string) {
	t.Helper()
	dir := filepath.Join(t.TempDir(), "spill")
	a, err := New(dir, opts)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = a.Close() })
	return a, dir
}

func visibleFiles(t *testing.T, dir string) []string {
	t.Helper()
	entries, err := os.ReadDir(dir)
	if os.IsNotExist(err) {
		return nil
	}
	if err != nil {
		t.Fatal(err)
	}
	var names []string
	for _, e := range entries {
		names = append(names, e.Name())
	}
	return names
}

// TestArenaBlocks: blocks are zeroed, aligned and independent; small ones
// share a slab; a large one has a file of its own, whose space returns
// when it is freed; no file is ever visible in the directory, and Close
// removes the directory.
func TestArenaBlocks(t *testing.T) {
	a, dir := newArena(t, Options{SlabBytes: 1 << 20})
	if _, err := os.Stat(dir); !os.IsNotExist(err) {
		t.Fatalf("the directory exists before the first allocation: %v", err)
	}
	var small [][]byte
	for i := range 100 {
		mem, err := a.Alloc(1000)
		if err != nil {
			t.Fatal(err)
		}
		if len(mem) != 1000 || cap(mem) != 1000 || uintptr(unsafe.Pointer(&mem[0]))%blockAlign != 0 {
			t.Fatalf("block %d: len %d cap %d at %p", i, len(mem), cap(mem), &mem[0])
		}
		if !bytes.Equal(mem, make([]byte, 1000)) {
			t.Fatalf("block %d is not zeroed", i)
		}
		for j := range mem {
			mem[j] = byte(i)
		}
		small = append(small, mem)
	}
	for i, mem := range small {
		if !bytes.Equal(mem, bytes.Repeat([]byte{byte(i)}, 1000)) {
			t.Fatalf("block %d was overwritten", i)
		}
	}
	if st := a.Stats(); st.Files != 1 || st.DiskBytes != 1<<20 {
		t.Fatalf("100 small blocks: %+v, want one slab of 1 MiB", st)
	}
	big, err := a.Alloc(3 << 20)
	if err != nil {
		t.Fatal(err)
	}
	big[0], big[len(big)-1] = 1, 2
	if st := a.Stats(); st.Files != 2 || st.DiskBytes != 4<<20 {
		t.Fatalf("with a large block: %+v, want a second file of 3 MiB", st)
	}
	if names := visibleFiles(t, dir); len(names) != 0 {
		t.Fatalf("the directory lists %v; spill files must be unlinked", names)
	}
	a.Free(big)
	if st := a.Stats(); st.Files != 1 || st.DiskBytes != 1<<20 || st.PeakBytes != 4<<20 {
		t.Fatalf("after freeing the large block: %+v", st)
	}
	for _, mem := range small {
		a.Free(mem)
	}
	if st := a.Stats(); st.Files != 0 || st.DiskBytes != 0 {
		t.Fatalf("after freeing everything: %+v", st)
	}
	if err := a.Check(); err != nil {
		t.Fatal(err)
	}
	if err := a.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(dir); !os.IsNotExist(err) {
		t.Fatalf("Close left the directory: %v", err)
	}
	if _, err := a.Alloc(1); err == nil {
		t.Fatal("Alloc after Close succeeded")
	}
	if err := a.Close(); err != nil {
		t.Fatal(err)
	}
}

// TestArenaResize: a block keeps its bytes through growth and shrinking,
// in place or moved, and every byte it gains reads as zero, also bytes it
// gave up earlier.
func TestArenaResize(t *testing.T) {
	a, _ := newArena(t, Options{SlabBytes: 1 << 20})
	fill := func(mem []byte, from int) {
		for i := from; i < len(mem); i++ {
			mem[i] = byte(i%251) + 1
		}
	}
	check := func(what string, mem []byte, kept int) {
		t.Helper()
		for i, b := range mem {
			want := byte(0)
			if i < kept {
				want = byte(i%251) + 1
			}
			if b != want {
				t.Fatalf("%s: byte %d is %d, want %d", what, i, b, want)
			}
		}
	}
	// The last block of its slab grows and shrinks in place.
	mem, err := a.Alloc(100)
	if err != nil {
		t.Fatal(err)
	}
	fill(mem, 0)
	base := &mem[0]
	if mem, err = a.Resize(mem, 5000); err != nil || &mem[0] != base {
		t.Fatalf("growing the last block moved it: %v", err)
	}
	check("grown in place", mem, 100)
	fill(mem, 100)
	if mem, err = a.Resize(mem, 50); err != nil || &mem[0] != base {
		t.Fatalf("shrinking moved the block: %v", err)
	}
	if mem, err = a.Resize(mem, 5000); err != nil {
		t.Fatal(err)
	}
	check("shrunk then grown", mem, 50)
	// Another block behind it: growth now moves it within the slab.
	other, err := a.Alloc(64)
	if err != nil {
		t.Fatal(err)
	}
	other[0] = 0xff
	fill(mem, 50)
	if mem, err = a.Resize(mem, 9000); err != nil || &mem[0] == base {
		t.Fatalf("growing a block that is not last did not move it: %v", err)
	}
	check("moved", mem, 5000)
	// A block that outgrows its slab gets a file of its own, which grows
	// and shrinks by remapping.
	files, slabs := a.Stats().Files, a.Stats().DiskBytes
	fill(mem, 5000)
	if mem, err = a.Resize(mem, 2<<20); err != nil {
		t.Fatal(err)
	}
	check("own file", mem, 9000)
	// The slab gave the whole pages of the block that left it back to the
	// filesystem.
	if st := a.Stats(); st.Files != files+1 || st.DiskBytes >= slabs+2<<20 || st.DiskBytes < slabs+2<<20-3*pageSize() {
		t.Fatalf("%+v, want a second file of 2 MiB and the slab's freed pages returned (the slab held %d)", st, slabs)
	}
	slabs = a.Stats().DiskBytes - 2<<20
	fill(mem, 9000)
	if mem, err = a.Resize(mem, 4<<20); err != nil {
		t.Fatal(err)
	}
	check("own file grown", mem, 2<<20)
	if got := a.Stats().DiskBytes; got != slabs+4<<20 {
		t.Fatalf("disk %d after growing, want %d", got, slabs+4<<20)
	}
	fill(mem, 2<<20)
	if mem, err = a.Resize(mem, 5000); err != nil {
		t.Fatal(err)
	}
	if got := a.Stats().DiskBytes; got != slabs+2*pageSize() {
		t.Fatalf("disk %d after shrinking, want %d", got, slabs+2*pageSize())
	}
	if mem, err = a.Resize(mem, 3<<20); err != nil {
		t.Fatal(err)
	}
	check("own file shrunk then grown", mem, 5000)
	if other[0] != 0xff {
		t.Fatal("a neighbour was overwritten")
	}
	if mem, err = a.Resize(mem, 0); err != nil || mem != nil {
		t.Fatalf("Resize to 0: %v", err)
	}
	if got := a.Stats().Files; got != files {
		t.Fatalf("%d files after freeing the large block, want %d", got, files)
	}
	if _, err := a.Resize(make([]byte, 8), 16); err == nil {
		t.Fatal("Resize of a foreign block succeeded")
	}
}

// TestArenaAgainstModel: random allocations, resizes and frees keep every
// live block equal to a heap copy.
func TestArenaAgainstModel(t *testing.T) {
	a, dir := newArena(t, Options{SlabBytes: 256 << 10})
	rng := rand.New(rand.NewSource(266))
	type blk struct{ mem, want []byte }
	var live []*blk
	sizeOf := func() int64 {
		switch rng.Intn(10) {
		case 0:
			return int64(rng.Intn(300 << 10))
		case 1:
			return int64(rng.Intn(64 << 10))
		default:
			return int64(1 + rng.Intn(2000))
		}
	}
	for step := range 4000 {
		switch op := rng.Intn(10); {
		case op < 4 || len(live) == 0:
			n := sizeOf()
			mem, err := a.Alloc(n)
			if err != nil {
				t.Fatal(err)
			}
			if n == 0 {
				continue
			}
			b := &blk{mem: mem, want: make([]byte, n)}
			if !bytes.Equal(mem, b.want) {
				t.Fatalf("step %d: a new block of %d bytes is not zeroed", step, n)
			}
			rng.Read(b.want)
			copy(b.mem, b.want)
			live = append(live, b)
		case op < 7:
			b := live[rng.Intn(len(live))]
			n := sizeOf()
			mem, err := a.Resize(b.mem, n)
			if err != nil {
				t.Fatal(err)
			}
			want := make([]byte, n)
			copy(want, b.want)
			if !bytes.Equal(mem, want) {
				t.Fatalf("step %d: resize %d → %d lost bytes or exposed old ones", step, len(b.want), n)
			}
			rng.Read(want)
			copy(mem, want)
			b.mem, b.want = mem, want
		default:
			i := rng.Intn(len(live))
			a.Free(live[i].mem)
			live = append(live[:i], live[i+1:]...)
		}
		if step%97 == 0 {
			for k, b := range live {
				if !bytes.Equal(b.mem, b.want) {
					t.Fatalf("step %d: block %d (%d bytes) changed", step, k, len(b.want))
				}
			}
		}
	}
	for k, b := range live {
		if !bytes.Equal(b.mem, b.want) {
			t.Fatalf("block %d (%d bytes) changed", k, len(b.want))
		}
	}
	if names := visibleFiles(t, dir); len(names) != 0 {
		t.Fatalf("the directory lists %v", names)
	}
	// What the files hold: their size less the freed blocks' pages that
	// were returned.
	var held, punched int64
	for r := range a.regions {
		held += r.size - r.punched
		punched += r.punched
	}
	if punched == 0 {
		t.Error("no freed block's pages were returned to the filesystem")
	}
	if st := a.Stats(); st.DiskBytes != held || st.Files != len(a.regions) {
		t.Fatalf("stats %+v, but the arena holds %d bytes in %d files", st, held, len(a.regions))
	}
	for _, b := range live {
		a.Free(b.mem)
	}
	if st := a.Stats(); st.DiskBytes != 0 || st.Files != 0 {
		t.Fatalf("after freeing everything: %+v", st)
	}
}

// TestArenaReserve: the caller's disk budget sees every byte before the
// arena takes it, and its refusal is returned unchanged with nothing
// taken.
func TestArenaReserve(t *testing.T) {
	var asked []int64
	limit := int64(3 << 20)
	errFull := errors.New("budget")
	var a *Arena
	a, dir := newArena(t, Options{SlabBytes: 1 << 20, Reserve: func(n int64) error {
		asked = append(asked, n)
		if a.Stats().DiskBytes+n > limit {
			return errFull
		}
		return nil
	}})
	if _, err := a.Alloc(100); err != nil {
		t.Fatal(err)
	}
	big, err := a.Alloc(1 << 20)
	if err != nil {
		t.Fatal(err)
	}
	if big, err = a.Resize(big, 2<<20); err != nil {
		t.Fatal(err)
	}
	if want := []int64{1 << 20, 1 << 20, 1 << 20}; len(asked) != 3 || asked[0] != want[0] || asked[1] != want[1] || asked[2] != want[2] {
		t.Fatalf("Reserve was asked for %v, want %v", asked, want)
	}
	if _, err := a.Alloc(1 << 20); !errors.Is(err, errFull) {
		t.Fatalf("Alloc over the budget: %v", err)
	}
	if _, err := a.Resize(big, 3<<20); !errors.Is(err, errFull) {
		t.Fatalf("Resize over the budget: %v", err)
	}
	// The refused block is untouched and still resizable.
	big[0] = 7
	if big, err = a.Resize(big, 1<<20); err != nil || big[0] != 7 {
		t.Fatalf("the block did not survive a refused growth: %v", err)
	}
	if st := a.Stats(); st.DiskBytes != 2<<20 {
		t.Fatalf("stats %+v", st)
	}
	if names := visibleFiles(t, dir); len(names) != 0 {
		t.Fatalf("the directory lists %v", names)
	}
}

// TestArenaGuardTurnsFaultsIntoErrors: a page the kernel cannot produce
// raises SIGBUS. A file cut short under its mapping stands in for a device
// that returns an I/O error; Guard reports it as a *FaultError, for a
// direct read and for a copy, instead of crashing. Another panic passes
// through.
func TestArenaGuardTurnsFaultsIntoErrors(t *testing.T) {
	for _, how := range []string{"read", "copy", "write"} {
		t.Run(how, func(t *testing.T) {
			a, _ := newArena(t, Options{SlabBytes: 1 << 20})
			mem, err := a.Alloc(2 << 20)
			if err != nil {
				t.Fatal(err)
			}
			mem[0] = 1
			for r := range a.regions {
				// Nothing else can do this: the file has no name.
				if err := r.f.Truncate(0); err != nil {
					t.Fatal(err)
				}
			}
			var sink byte
			err = a.Guard(func() error {
				switch how {
				case "read":
					sink = mem[1<<20]
				case "copy":
					buf := make([]byte, 4096)
					copy(buf, mem[1<<20:])
					sink = buf[0]
				case "write":
					mem[1<<20] = 9
				}
				return nil
			})
			var fault *FaultError
			if !errors.As(err, &fault) || !a.Contains(fault.Addr) {
				t.Fatalf("Guard returned %v (read %d), want a fault inside the arena", err, sink)
			}
		})
	}
	a, _ := newArena(t, Options{})
	defer func() {
		if r := recover(); r == nil {
			t.Fatal("a panic that is not a fault in the arena was swallowed")
		}
	}()
	_ = a.Guard(func() error {
		var none []int
		return fmt.Errorf("%d", none[len(a.regions)])
	})
}

// TestArenaGuardPassesResults: Guard returns fn's own error, and a nil
// arena guards nothing.
func TestArenaGuardPassesResults(t *testing.T) {
	a, _ := newArena(t, Options{})
	want := errors.New("x")
	if err := a.Guard(func() error { return want }); err != want {
		t.Fatalf("Guard returned %v", err)
	}
	var none *Arena
	if err := none.Guard(func() error { return want }); err != want || none.Check() != nil || none.Close() != nil || none.Contains(1) {
		t.Fatalf("a nil arena: %v", err)
	}
}
