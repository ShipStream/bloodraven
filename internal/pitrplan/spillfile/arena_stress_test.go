//go:build linux

package spillfile

import (
	"encoding/binary"
	"math/rand"
	"os"
	"strconv"
	"strings"
	"testing"
	"time"
)

// TestArenaUnderMemoryLimit measures what a memory-limited cgroup does to
// a process that writes mapped spill, with and without bounded
// write-back. It is opt-in, because it only means something inside a
// memory-limited cgroup on a disk filesystem:
//
//	CGO_ENABLED=0 go test -c -o /tmp/spillfile.test ./internal/pitrplan/spillfile
//	docker run --rm --memory=128m --memory-swap=128m \
//	  -v /tmp/spillfile.test:/t:ro -v <dir on ext4 or xfs>:/spill \
//	  -e SPILLFILE_STRESS_DIR=/spill -e SPILLFILE_STRESS_BYTES=2147483648 \
//	  -e SPILLFILE_STRESS_SYNC_BYTES=8388608 \
//	  <any image> /t -test.run TestArenaUnderMemoryLimit -test.v
//
// It fills SPILLFILE_STRESS_BYTES of blocks in order, rewrites words at
// random offsets (every write dirties another page), then reads every
// page back, and logs the cgroup's memory counters. With
// SPILLFILE_STRESS_SYNC_BYTES it calls WriteBack and Settle after that many bytes of
// pages written, as pitrplan.Budget does; with 0 it never does, which is
// the unbounded behaviour that gets the process OOM-killed. Surviving is
// the result.
func TestArenaUnderMemoryLimit(t *testing.T) {
	dir := os.Getenv("SPILLFILE_STRESS_DIR")
	if dir == "" {
		t.Skip("set SPILLFILE_STRESS_DIR (and run inside a memory-limited cgroup) to run")
	}
	total := int64(1 << 30)
	if v := os.Getenv("SPILLFILE_STRESS_BYTES"); v != "" {
		n, err := strconv.ParseInt(v, 10, 64)
		if err != nil || n < 1<<20 {
			t.Fatalf("SPILLFILE_STRESS_BYTES=%q", v)
		}
		total = n
	}
	syncEvery := int64(0)
	if v := os.Getenv("SPILLFILE_STRESS_SYNC_BYTES"); v != "" {
		n, err := strconv.ParseInt(v, 10, 64)
		if err != nil || n < 0 {
			t.Fatalf("SPILLFILE_STRESS_SYNC_BYTES=%q", v)
		}
		syncEvery = n
	}
	t.Logf("%d MiB of spill, Sync after every %d bytes written (0: never)", total>>20, syncEvery)
	logCgroup(t, "start")
	a, err := New(dir+"/stress-spill", Options{})
	if err != nil {
		t.Fatal(err)
	}
	defer a.Close()
	// Half in one large block, half in 4 MiB blocks cut from slabs.
	var blocks [][]byte
	big, err := a.Alloc(total / 2)
	if err != nil {
		t.Fatal(err)
	}
	blocks = append(blocks, big)
	for n := total / 2; n > 0; n -= 4 << 20 {
		mem, err := a.Alloc(4 << 20)
		if err != nil {
			t.Fatal(err)
		}
		blocks = append(blocks, mem)
	}
	word := func(b, i int) uint64 { return uint64(b)<<40 ^ uint64(i)*0x9e3779b97f4a7c15 }
	start := time.Now()
	err = a.Guard(func() error {
		for b, mem := range blocks {
			from := 0
			for i := 0; i+8 <= len(mem); i += 8 {
				binary.LittleEndian.PutUint64(mem[i:], word(b, i))
				if syncEvery > 0 && int64(i+8-from) >= syncEvery {
					if err := syncNow(a, mem, int64(from), int64(i+8-from)); err != nil {
						return err
					}
					from = i + 8
				}
			}
			if syncEvery > 0 {
				if err := syncNow(a, mem, int64(from), int64(len(mem)-from)); err != nil {
					return err
				}
			}
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("filled %d MiB in order in %s", total>>20, time.Since(start).Round(time.Millisecond))
	logCgroup(t, "after the fill")
	// Random rewrites: each lands on another page.
	rng := rand.New(rand.NewSource(1))
	writes := int(total / 4096)
	last := map[[2]int]uint64{}
	start = time.Now()
	err = a.Guard(func() error {
		// The pages written since the last Sync, as pitrplan.Budget
		// counts them: a page counts once.
		dirty := map[[2]int]bool{}
		for range writes {
			b := rng.Intn(len(blocks))
			i := rng.Intn(len(blocks[b])/8) * 8
			v := rng.Uint64()
			if syncEvery > 0 {
				if pg := [2]int{b, i / 4096}; !dirty[pg] {
					if int64(len(dirty)+1)*4096 > syncEvery {
						for _, mem := range blocks {
							if err := a.WriteBack(mem, 0, int64(len(mem))); err != nil {
								return err
							}
						}
						if err := a.Settle(); err != nil {
							return err
						}
						clear(dirty)
					}
					dirty[pg] = true
				}
			}
			binary.LittleEndian.PutUint64(blocks[b][i:], v)
			if len(last) < 1<<16 {
				last[[2]int{b, i}] = v
			} else if _, ok := last[[2]int{b, i}]; ok {
				last[[2]int{b, i}] = v
			}
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("%d random page writes in %s", writes, time.Since(start).Round(time.Millisecond))
	logCgroup(t, "after the random writes")
	// The tracked words hold their last value; a sample of the rest holds
	// the fill value or a random write (unknowable without tracking them
	// all), so the full pass checks only that every page reads back.
	start = time.Now()
	bad := 0
	err = a.Guard(func() error {
		for k, v := range last {
			if binary.LittleEndian.Uint64(blocks[k[0]][k[1]:]) != v {
				bad++
			}
		}
		var sum uint64
		for _, mem := range blocks {
			for i := 0; i+8 <= len(mem); i += 4096 {
				sum += binary.LittleEndian.Uint64(mem[i:])
			}
		}
		_ = sum
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if bad != 0 {
		t.Fatalf("%d of %d tracked words read back wrong", bad, len(last))
	}
	t.Logf("verified %d tracked words and read every page back in %s", len(last), time.Since(start).Round(time.Millisecond))
	if err := a.Check(); err != nil {
		t.Fatal(err)
	}
	logCgroup(t, "end")
}

// syncNow writes one range back and waits.
func syncNow(a *Arena, mem []byte, off, n int64) error {
	if err := a.WriteBack(mem, off, n); err != nil {
		return err
	}
	return a.Settle()
}

// logCgroup logs the calling process's cgroup v2 memory counters and its
// own resident sizes.
func logCgroup(t *testing.T, when string) {
	t.Helper()
	read := func(path string) string {
		b, err := os.ReadFile(path)
		if err != nil {
			return "?"
		}
		return strings.TrimSpace(string(b))
	}
	pick := func(text string, keys ...string) string {
		var out []string
		for _, line := range strings.Split(text, "\n") {
			f := strings.Fields(line)
			if len(f) < 2 {
				continue
			}
			for _, k := range keys {
				if strings.TrimSuffix(f[0], ":") == k {
					out = append(out, k+"="+strings.Join(f[1:], ""))
				}
			}
		}
		return strings.Join(out, " ")
	}
	t.Logf("%s: memory.max=%s current=%s peak=%s | %s | events: %s | %s", when,
		read("/sys/fs/cgroup/memory.max"), read("/sys/fs/cgroup/memory.current"), read("/sys/fs/cgroup/memory.peak"),
		pick(read("/sys/fs/cgroup/memory.stat"), "anon", "file", "file_mapped", "file_dirty", "file_writeback"),
		pick(read("/sys/fs/cgroup/memory.events"), "max", "oom", "oom_kill"),
		pick(read("/proc/self/status"), "VmHWM", "RssAnon", "RssFile"))
}
