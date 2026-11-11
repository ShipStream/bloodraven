package pitrplan

import (
	"context"
	"crypto/sha256"
	"encoding/binary"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/shipstream/bloodraven/internal/backupmeta"
	"github.com/shipstream/bloodraven/internal/pitrplan/spillfile"
)

// TestSpillLargeWindowPerf plans a large synthetic window and reports the
// time and the memory it took. It is opt-in (it is a measurement, and at
// its intended sizes it needs gigabytes of disk):
//
//	PITRPLAN_PERF_TXNS       transactions per site; unset skips the test
//	PITRPLAN_PERF_FILE_TXNS  transactions per binlog file (default 500000)
//	PITRPLAN_PERF_MEMORY     the planner budget in bytes (default 256 MiB)
//	PITRPLAN_PERF_SPILL_DIR  a directory on a disk filesystem for the spill
//	                         arena; unset plans in memory
//
// The window is the shape that needs the most: two sites archived the
// same transactions (site b as a replica, with other file boundaries), so
// every search position is in a two-file bucket with an exact-key class,
// and every transaction is compared between its copies and enters the
// order check. Each row is indexed when the planner fetches it, under the
// plan's budget, as pitr-download does (without the binlog parsing).
//
// To see what the container's memory limit does to it, build the test
// binary (CGO_ENABLED=0 go test -c ./internal/pitrplan) and run it in a
// memory-limited container with PITRPLAN_PERF_SPILL_DIR on a mounted disk
// directory.
func TestSpillLargeWindowPerf(t *testing.T) {
	env := func(name string, def int64) int64 {
		v := os.Getenv(name)
		if v == "" {
			return def
		}
		n, err := strconv.ParseInt(v, 10, 64)
		if err != nil || n <= 0 {
			t.Fatalf("%s=%q", name, v)
		}
		return n
	}
	total := env("PITRPLAN_PERF_TXNS", 0)
	if total == 0 {
		t.Skip("set PITRPLAN_PERF_TXNS to run")
	}
	perFile := env("PITRPLAN_PERF_FILE_TXNS", 500000)
	memory := env("PITRPLAN_PERF_MEMORY", 256<<20)
	budget := NewBudget(memory)
	budget.SetSpill(SpillOptions{})
	var arena *spillfile.Arena
	if dir := os.Getenv("PITRPLAN_PERF_SPILL_DIR"); dir != "" {
		var err error
		arena, err = spillfile.New(filepath.Join(dir, fmt.Sprintf("perf-spill-%d", os.Getpid())), spillfile.Options{})
		if err != nil {
			t.Fatal(err)
		}
		defer arena.Close()
		dirty := min(max(memory/8, 1<<20), 64<<20)
		budget.SetSpill(SpillOptions{Arena: arena, HeapMax: (memory - dirty) / 2, DirtyMax: dirty})
	}

	oct := func(g int64) int64 { return sec(float64(g)) }
	type span struct{ from, to int64 }
	spans := map[RowRef]span{}
	var archive Archive
	for k, site := range []string{"a", "b"} {
		m := SiteManifest{Site: site}
		// Site b's files are cut elsewhere.
		from := int64(1)
		first := perFile
		if k == 1 {
			first = perFile/3 + 1
		}
		for n := 1; from <= total; n++ {
			to := min(from+first-1, total)
			first = perFile
			name := fmt.Sprintf("%s-%06d", site, n)
			prev := Set{}
			if from > 1 {
				prev = MustParseSet(fmt.Sprintf("A:1-%d", from-1))
			}
			row := Row{Site: site, Name: name, ID: "id-" + name, Created: synthEpoch,
				Hints: Hints{Known: true, Previous: prev, GTIDs: MustParseSet(fmt.Sprintf("A:%d-%d", from, to)), Empty: Set{}, Min: oct(from), Max: oct(to)}}
			m.Rows = append(m.Rows, row)
			spans[RowRef{Site: site, Name: name, ID: row.ID}] = span{from, to}
			from = to + 1
		}
		archive.Sites = append(archive.Sites, m)
	}
	fetch := FetchFunc(func(_ context.Context, r RowRef) (*FileIndex, error) {
		sp, ok := spans[r]
		if !ok {
			return nil, fmt.Errorf("unknown row %v", r)
		}
		writer := uint32(101)
		if r.Site == "b" {
			writer = 102
		}
		ib := NewIndexBuilder(r.Site+"/"+r.Name, budget)
		ib.SetWriter(writer)
		prev := Set{}
		if sp.from > 1 {
			prev = MustParseSet(fmt.Sprintf("A:1-%d", sp.from-1))
		}
		if err := ib.SetPrevious(prev); err != nil {
			return nil, err
		}
		for g := sp.from; g <= sp.to; g++ {
			if err := ib.BeginTxn(GTID{"A", g}, oct(g), 101, 4+200*(g-sp.from+1)); err != nil {
				return nil, err
			}
			ib.MarkData()
			// Both copies carry the same digest, so every pair is
			// compared.
			var fx EffectDigest
			binary.LittleEndian.PutUint64(fx[:], uint64(g)*0x9e3779b97f4a7c15|1)
			ib.SetEffect(fx)
		}
		return ib.Finish(r.Name, 4+200*(sp.to-sp.from+2))
	})
	end := backupmeta.NewMicroTime(time.UnixMicro(oct(10) + 500000))
	dump := backupmeta.DumpMeta{Version: 1, GTIDExecuted: "A:1-10", End: end, ServerClock: backupmeta.ServerClock{Start: end, End: end},
		Stamp: backupmeta.Stamp{Site: "a", ServerID: 101, CreatedAt: end}}
	stop := time.UnixMicro(oct(total-5) + 500000).UTC()

	// The peak of the anonymous resident set, sampled: VmHWM also counts
	// the mapped spill, which is page cache.
	var peakAnon atomic.Int64
	done := make(chan struct{})
	go func() {
		tick := time.NewTicker(20 * time.Millisecond)
		defer tick.Stop()
		for {
			select {
			case <-done:
				return
			case <-tick.C:
				if kb := procStatusKB("RssAnon"); kb > peakAnon.Load() {
					peakAnon.Store(kb)
				}
			}
		}
	}()
	start := time.Now()
	var p *Plan
	err := arena.Guard(func() error {
		var err error
		p, err = Run(context.Background(), Request{Mode: ModeTimestamp, Stop: stop, Dump: dump, Budget: budget}, archive, fetch)
		return err
	})
	elapsed := time.Since(start)
	close(done)
	if err != nil {
		t.Fatal(err)
	}
	if cerr := arena.Check(); cerr != nil {
		t.Fatal(cerr)
	}
	if p.Witness == nil || p.Witness.GTID != (GTID{"A", total - 4}) || p.ReplayedTransactions() != total-15 || len(p.Segments) == 0 {
		t.Fatalf("plan: witness %+v, %d replayed, %d segments", p.Witness, p.ReplayedTransactions(), len(p.Segments))
	}
	cov, err := p.Coverage(CoverageArchive{}, CoverageDump{}, "").Marshal()
	if err != nil {
		t.Fatal(err)
	}
	st := arena.Stats()
	t.Logf("%d transactions per site in files of %d: planned in %s; %d rows downloaded, %d transactions indexed, %d segments; coverage sha256 %x",
		total, perFile, elapsed.Round(time.Millisecond), p.Downloaded, p.IndexedTransactions, len(p.Segments), sha256.Sum256(cov))
	t.Logf("memory budget %d B, peak charge %d B; spilled peak %d B in %d B of files (%d files made); %d write-back rounds",
		memory, budget.Peak(), budget.SpillPeak(), st.PeakBytes, st.Created, budget.flushes)
	t.Logf("process: peak RSS (VmHWM) %d kB, peak anonymous RSS %d kB; cgroup: memory.max=%s memory.peak=%s events: %s",
		procStatusKB("VmHWM"), peakAnon.Load(), readTrim("/sys/fs/cgroup/memory.max"), readTrim("/sys/fs/cgroup/memory.peak"),
		strings.Join(strings.Fields(readTrim("/sys/fs/cgroup/memory.events")), " "))
}

func readTrim(path string) string {
	b, err := os.ReadFile(path)
	if err != nil {
		return "?"
	}
	return strings.TrimSpace(string(b))
}

// procStatusKB reads a kB field of /proc/self/status, or 0.
func procStatusKB(field string) int64 {
	for _, line := range strings.Split(readTrim("/proc/self/status"), "\n") {
		f := strings.Fields(line)
		if len(f) >= 2 && f[0] == field+":" {
			n, _ := strconv.ParseInt(f[1], 10, 64)
			return n
		}
	}
	return 0
}
