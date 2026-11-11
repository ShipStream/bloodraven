package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/shipstream/bloodraven/internal/pitrarchive"
	"github.com/shipstream/bloodraven/internal/pitrplan"
	"github.com/shipstream/bloodraven/internal/sidecar"
)

// spillOnDisk makes the tests' temporary directories count as disk: on a
// developer machine /tmp is often tmpfs, where pitr-download leaves the
// spill off.
func spillOnDisk(t *testing.T, onDisk bool) {
	t.Helper()
	old := pitrStagingInMemory
	pitrStagingInMemory = func(string) bool { return !onDisk }
	t.Cleanup(func() { pitrStagingInMemory = old })
}

// windowArchive: sites a and b both archived the same n transactions of
// one writer (b as a replica), and a archived ten more. Every position of
// the two large files is in a two-file search bucket, and every
// transaction is in the order check.
func windowArchive(t *testing.T, n int64) *dlArchive {
	t.Helper()
	a := &dlArchive{store: stampTestStore(t), dest: downloadDest(t), manifests: map[string]*sidecar.Manifest{}}
	a.desc = putDescriptor(t, a.store, a.dest)
	for _, s := range []string{"a", "b"} {
		a.manifests[s] = &sidecar.Manifest{Version: 2, ArchiveID: a.desc.ArchiveID, Site: s}
	}
	a.tA = func(k int64) time.Time { return dlEpoch.Add(time.Duration(k) * time.Second) }
	a.manifests["a"].AppendPromotionRecord(pitrarchive.PromotionRecord{ServerUUID: sidA, ServerID: 101, ObservedAt: dlEpoch})
	a.put(t, "a", "mysql-bin.000001", 101, "", "", dlTxns(sidA, 1, n, a.tA(1), 101))
	a.put(t, "b", "mysql-bin.000001", 102, "", "", dlTxns(sidA, 1, n, a.tA(1), 101))
	a.put(t, "a", "mysql-bin.000002", 101, fmt.Sprintf("%s:1-%d", sidA, n), "", dlTxns(sidA, n+1, n+10, a.tA(n+1), 101))
	a.save(t)
	return a
}

func windowRequest(t *testing.T, a *dlArchive, n int64, memory int64) pitrDownloadRequest {
	t.Helper()
	return pitrDownloadRequest{
		archive: pitrArchiveRequest{prefix: downloadPrefix, expect: pitrarchive.Expectation{Role: pitrarchive.RoleReader, Destination: a.dest,
			Encryption: pitrarchive.EncryptionIdentity{Mode: pitrarchive.EncryptionNone}}},
		mode: pitrplan.ModeTimestamp, stop: a.tA(n).Add(500 * time.Millisecond), exclude: pitrplan.Set{}, outDir: t.TempDir(), delta: 5 * time.Second,
		memory: memory, dumpMetaPath: writeDumpJSON(t, t.TempDir(), sidA+":1-10", a.tA(10).Add(500*time.Millisecond), a),
	}
}

// stagingEntries lists everything under the staging directory, files and
// directories.
func stagingEntries(t *testing.T, dir string) []string {
	t.Helper()
	var out []string
	err := filepath.WalkDir(dir, func(p string, _ os.DirEntry, err error) error {
		if err != nil || p == dir {
			return err
		}
		rel, _ := filepath.Rel(dir, p)
		out = append(out, filepath.ToSlash(rel))
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	return out
}

// TestPlanAndStageSpillsLargeWindow is issue #266 end to end: a window
// whose index does not fit the planner's memory refuses PITRPlanTooLarge
// when the staging volume cannot take a spill, and with a spill it plans,
// stages the same files and writes the same coverage.json, byte for byte,
// as a run with memory to spare. Nothing of the spill is left in the
// staging directory.
func TestPlanAndStageSpillsLargeWindow(t *testing.T) {
	const n = 12000
	const small = 3 << 20
	a := windowArchive(t, n)
	quiet := slog.New(slog.NewTextHandler(io.Discard, nil))

	spillOnDisk(t, true)
	big := windowRequest(t, a, n, 512<<20)
	if _, err := planAndStage(context.Background(), a.store, big, quiet); err != nil {
		t.Fatal(err)
	}
	want, err := os.ReadFile(filepath.Join(big.outDir, pitrplan.CoverageFileName))
	if err != nil {
		t.Fatal(err)
	}
	wantFiles := stagingEntries(t, big.outDir)

	// No spill: the staging volume is memory.
	spillOnDisk(t, false)
	refused := windowRequest(t, a, n, small)
	_, err = planAndStage(context.Background(), a.store, refused, quiet)
	if tok, _ := pitrarchive.ParseToken(errString(err)); tok != pitrarchive.TokenPlanTooLarge || !strings.Contains(errString(err), "raise the Job's memory limit") {
		t.Fatalf("without a spill: %v, want PITRPlanTooLarge on memory", err)
	}
	if got := stagingEntries(t, refused.outDir); len(got) != 1 || got[0] != pitrplan.CoverageFileName {
		t.Fatalf("the refusal left %v", got)
	}

	// With a spill.
	spillOnDisk(t, true)
	var logged bytes.Buffer
	req := windowRequest(t, a, n, small)
	res, err := planAndStage(context.Background(), a.store, req, slog.New(slog.NewJSONHandler(&logged, nil)))
	if err != nil {
		t.Fatalf("with a spill: %v", err)
	}
	got, err := os.ReadFile(filepath.Join(req.outDir, pitrplan.CoverageFileName))
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(got, want) {
		t.Fatalf("the spilled run's coverage.json differs from the in-memory run's\nin memory:\n%s\nspilled:\n%s", want, got)
	}
	if entries := stagingEntries(t, req.outDir); strings.Join(entries, " ") != strings.Join(wantFiles, " ") {
		t.Fatalf("the spilled run staged %v, the in-memory run %v", entries, wantFiles)
	}
	for _, f := range res.plan.Files {
		staged, err := os.ReadFile(filepath.Join(req.outDir, filepath.FromSlash(f.StagedPath())))
		if err != nil || int64(len(staged)) != f.Size {
			t.Fatalf("staged %s: %d bytes, %v", f.StagedPath(), len(staged), err)
		}
	}
	var line struct {
		Msg                string `json:"msg"`
		MemoryPeakBytes    int64  `json:"memoryPeakBytes"`
		SpilledPeakBytes   int64  `json:"spilledPeakBytes"`
		SpillDiskPeakBytes int64  `json:"spillDiskPeakBytes"`
	}
	if err := json.Unmarshal(logged.Bytes(), &line); err != nil {
		t.Fatalf("log %q: %v", logged.String(), err)
	}
	if line.Msg != "pitr plan" || line.MemoryPeakBytes <= 0 || line.MemoryPeakBytes > small || line.SpilledPeakBytes < small || line.SpillDiskPeakBytes < line.SpilledPeakBytes {
		t.Fatalf("pitr plan log: %+v, want memory within %d and more than that spilled", line, small)
	}
	t.Logf("%d transactions on two sites: memory peak %d B of %d, %d B spilled in %d B of files", n, line.MemoryPeakBytes, small, line.SpilledPeakBytes, line.SpillDiskPeakBytes)

	// A staging volume without room for the spill: the refusal names both
	// limits and what to raise, and leaves only coverage.json.
	tight := windowRequest(t, a, n, small)
	tight.stagingLimit = stagingReserve + 2<<20
	_, err = planAndStage(context.Background(), a.store, tight, quiet)
	r, ok := pitrplan.RefusalOf(err)
	if !ok || r.Token != pitrarchive.TokenPlanTooLarge {
		t.Fatalf("a staging volume too small for the spill: %v", err)
	}
	for _, sub := range []string{"fit neither limit", "the memory budget of 3145728 bytes", "sizeLimit of 3.0 MiB", "raise spec.backup.stagingVolumeSizeLimit", "raise the Job's memory limit"} {
		if !strings.Contains(r.Message, sub) {
			t.Errorf("refusal %q lacks %q", r.Message, sub)
		}
	}
	if got := stagingEntries(t, tight.outDir); len(got) != 1 || got[0] != pitrplan.CoverageFileName {
		t.Fatalf("the refusal left %v", got)
	}
	if c := readCoverage(t, tight.outDir); !strings.HasPrefix(c.RefusalReason, "PITRPlanTooLarge: ") {
		t.Fatalf("coverage refusal %q", c.RefusalReason)
	}

	// A cancelled run leaves no spill behind either.
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	gone := windowRequest(t, a, n, small)
	if _, err := planAndStage(ctx, a.store, gone, quiet); err == nil {
		t.Fatal("a cancelled run planned")
	}
	for _, e := range stagingEntries(t, gone.outDir) {
		if e != pitrplan.CoverageFileName {
			t.Fatalf("a cancelled run left %s", e)
		}
	}
}

// TestStagingSpillReserve: the spill's disk budget. The spill, the tee
// cache and the reserve share the emptyDir's sizeLimit, and the node
// filesystem keeps its floor. Cached rows, largest first, give way to the
// spill; when none is left the refusal says which limit was hit and what
// to raise.
func TestStagingSpillReserve(t *testing.T) {
	spillOnDisk(t, true)
	out := t.TempDir()
	budget := pitrplan.NewBudget(64 << 20)
	rf := &rowFetcher{budget: budget, cacheDir: filepath.Join(out, ".pitr-cache"), cached: map[pitrplan.RowRef]cachedRow{},
		freeAfter: func(string, int64) (int64, int64) { return 1 << 40, 0 }}
	if err := os.MkdirAll(rf.cacheDir, 0o700); err != nil {
		t.Fatal(err)
	}
	cache := func(name string, size int64) pitrplan.RowRef {
		ref := pitrplan.RowRef{Site: "a", Name: name, ID: name}
		path := filepath.Join(rf.cacheDir, name)
		if err := os.WriteFile(path, make([]byte, size), 0o600); err != nil {
			t.Fatal(err)
		}
		rf.cached[ref] = cachedRow{path: path, size: size}
		rf.cachedBytes += blocks(size)
		return ref
	}
	rf.spill = openStagingSpill(pitrDownloadRequest{outDir: out, memory: 64 << 20}, rf, budget)
	if rf.spill == nil {
		t.Skip("no file-backed spill on this platform")
	}
	defer rf.spill.close()
	if !budget.Spilling() {
		t.Fatal("the budget has no arena")
	}
	smallRow, bigRow, midRow := cache("small", 1<<20), cache("big", 4<<20), cache("mid", 2<<20)
	rf.limit = stagingReserve + 12<<20 // the cache holds 7 MiB of it

	// Room without touching the cache.
	if err := rf.spill.reserve(4 << 20); err != nil || len(rf.cached) != 3 {
		t.Fatalf("reserve with room: %v, %d rows cached", err, len(rf.cached))
	}
	// The tee's allowance counts the spill: the arena holds 4 MiB now.
	mem, err := rf.spill.Alloc(4 << 20)
	if err != nil {
		t.Fatal(err)
	}
	if got, want := rf.cacheRoom(), int64(12<<20-7<<20-4<<20); got != want {
		t.Fatalf("room for a new copy %d beside a 4 MiB spill, want %d", got, want)
	}
	// A row that fits is allowed its own size, in whole blocks, and no more.
	if got := rf.cacheAllowance(1<<20 - 100); got != 1<<20 {
		t.Fatalf("cache allowance %d for a row of just under 1 MiB, want 1 MiB", got)
	}
	if got := rf.cacheAllowance(1<<20 + 1); got != 0 {
		t.Fatalf("cache allowance %d for a row larger than the room", got)
	}
	// 3 MiB more: the largest cached row goes, the others stay.
	if err := rf.spill.reserve(3 << 20); err != nil {
		t.Fatal(err)
	}
	if _, kept := rf.cached[bigRow]; kept || len(rf.cached) != 2 || rf.cachedBytes != 3<<20 {
		t.Fatalf("after making room: %d rows cached, %d bytes", len(rf.cached), rf.cachedBytes)
	}
	if _, err := os.Stat(filepath.Join(rf.cacheDir, "big")); !os.IsNotExist(err) {
		t.Fatalf("the dropped row's file is still there: %v", err)
	}
	// More than the volume has, even with the cache gone.
	err = rf.spill.reserve(9 << 20)
	r, ok := pitrplan.RefusalOf(err)
	if !ok || r.Token != pitrarchive.TokenPlanTooLarge {
		t.Fatalf("reserve past the sizeLimit: %v", err)
	}
	for _, sub := range []string{"planning must spill 9.0 MiB more", "already holds 4.0 MiB of spilled planning data", "sizeLimit of 13.0 MiB", "raise spec.backup.stagingVolumeSizeLimit", "raise the Job's memory limit"} {
		if !strings.Contains(r.Message, sub) {
			t.Errorf("refusal %q lacks %q", r.Message, sub)
		}
	}
	if len(rf.cached) != 0 || rf.cachedBytes != 0 {
		t.Fatalf("the cache was not given up before refusing: %v", rf.cached)
	}
	_, _ = smallRow, midRow

	// The node's floor.
	rf.limit = 0
	floor := int64(256 << 20)
	rf.freeAfter = func(_ string, n int64) (int64, int64) { return floor + 1<<20 - n, floor }
	if err := rf.spill.reserve(1 << 20); err != nil {
		t.Fatalf("reserve down to the floor: %v", err)
	}
	err = rf.spill.reserve(2 << 20)
	if r, ok := pitrplan.RefusalOf(err); !ok || r.Token != pitrarchive.TokenPlanTooLarge || !strings.Contains(r.Message, "floor of 256.0 MiB") || !strings.Contains(r.Message, "ephemeral storage") {
		t.Fatalf("reserve below the floor: %v", err)
	}

	// The volume filling under an allocation is the same refusal; any
	// other failure of the volume is an error.
	full := rf.spill.spillError(fmt.Errorf("spillfile: preallocate: %w", syscall.ENOSPC))
	if r, ok := pitrplan.RefusalOf(full); !ok || r.Token != pitrarchive.TokenPlanTooLarge || !strings.Contains(r.Message, "ran out of space") {
		t.Fatalf("ENOSPC: %v", full)
	}
	broken := fmt.Errorf("spillfile: map: %w", syscall.EIO)
	if got := rf.spill.spillError(broken); got != broken {
		t.Fatalf("EIO became %v", got)
	}
	rf.spill.Free(mem)

	// The spill's sizes follow the planner budget.
	for _, c := range []struct{ memory, dirty, heap int64 }{
		{4 << 20, 1 << 20, 3 << 19}, {600 << 20, 64 << 20, 268 << 20}, {64 << 20, 8 << 20, 28 << 20},
	} {
		if d, h := spillDirtyBytes(c.memory), spillHeapMax(c.memory); d != c.dirty || h != c.heap {
			t.Errorf("a %d byte budget: dirty window %d, heap part %d; want %d, %d", c.memory, d, h, c.dirty, c.heap)
		}
	}
}

// Review R3 finding 3: a row's index spills while the row itself is being
// downloaded and copied into the tee cache, so the two grow at once. The
// copy's allowance is the row's size, set aside when its download starts,
// and the spill's reservations count it, written or not; when both do not
// fit, the copy gives way like a cached row does. Before the fix the copy
// was allowed everything free at its start and the spill did not see it,
// so together they could pass the sizeLimit or the node's floor.
func TestStagingSpillCountsTheOpenCopy(t *testing.T) {
	spillOnDisk(t, true)
	out := t.TempDir()
	budget := pitrplan.NewBudget(64 << 20)
	rf := &rowFetcher{budget: budget, cacheDir: filepath.Join(out, ".pitr-cache"), cached: map[pitrplan.RowRef]cachedRow{},
		freeAfter: func(string, int64) (int64, int64) { return 1 << 40, 0 }}
	if err := os.MkdirAll(rf.cacheDir, 0o700); err != nil {
		t.Fatal(err)
	}
	rf.spill = openStagingSpill(pitrDownloadRequest{outDir: out, memory: 64 << 20}, rf, budget)
	if rf.spill == nil {
		t.Skip("no file-backed spill on this platform")
	}
	defer rf.spill.close()
	// open starts the copy of a row: allow bytes set aside, written of them
	// on disk.
	part := filepath.Join(rf.cacheDir, "1.part")
	open := func(allow, written int64) {
		t.Helper()
		f, err := os.Create(part)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := f.Write(make([]byte, written)); err != nil {
			t.Fatal(err)
		}
		rf.tee = &cacheTee{out: f, part: part, allow: allow, written: written}
	}
	dropped := func() bool {
		_, err := os.Stat(part)
		return rf.tee.out == nil && os.IsNotExist(err)
	}

	// The sizeLimit: 12 MiB beside the reserve, 8 MiB of it set aside for
	// the open copy, of which 1 MiB is written.
	rf.limit = stagingReserve + 12<<20
	open(8<<20, 1<<20)
	if err := rf.spill.reserve(3 << 20); err != nil || dropped() {
		t.Fatalf("3 MiB beside the open copy's 8: %v, copy dropped %v", err, dropped())
	}
	mem, err := rf.spill.Alloc(3 << 20)
	if err != nil {
		t.Fatal(err)
	}
	// A new copy sees the spill; the spill sees the open copy.
	if got := rf.spill.reserve(1 << 20); got != nil || dropped() {
		t.Fatalf("1 MiB more (3 + 8 + 1 = 12): %v, copy dropped %v", got, dropped())
	}
	if err := rf.spill.reserve(2 << 20); err != nil {
		t.Fatalf("2 MiB more, which fits once the open copy is gone: %v", err)
	}
	if !dropped() {
		t.Fatal("the spill took 2 MiB more beside 3 MiB spilled and a copy that may grow to 8 MiB: 13 MiB of a 12 MiB limit")
	}
	// The open copy goes before a cached row does.
	cachedPath := filepath.Join(rf.cacheDir, "row")
	if err := os.WriteFile(cachedPath, make([]byte, 1<<20), 0o600); err != nil {
		t.Fatal(err)
	}
	ref := pitrplan.RowRef{Site: "a", Name: "row", ID: "row"}
	rf.cached[ref] = cachedRow{path: cachedPath, size: 1 << 20}
	rf.cachedBytes = 1 << 20
	open(6<<20, 0)
	if err := rf.spill.reserve(4 << 20); err != nil || !dropped() || len(rf.cached) != 1 {
		t.Fatalf("4 MiB beside 3 spilled, 1 cached and an open copy of 6: %v, copy dropped %v, %d rows cached", err, dropped(), len(rf.cached))
	}
	// Past everything: both are given up, and the refusal counts neither.
	open(2<<20, 0)
	err = rf.spill.reserve(10 << 20)
	if r, ok := pitrplan.RefusalOf(err); !ok || r.Token != pitrarchive.TokenPlanTooLarge || !strings.Contains(r.Message, "0 B of cached binlog files") || !dropped() || len(rf.cached) != 0 {
		t.Fatalf("10 MiB beside 3 spilled: %v, copy dropped %v, %d rows cached", err, dropped(), len(rf.cached))
	}

	// The node's floor: the filesystem's free space shows what the copy
	// has written, not what it may still write.
	rf.limit, rf.cachedBytes = 0, 0
	floor := int64(256 << 20)
	rf.freeAfter = func(_ string, n int64) (int64, int64) {
		free := floor + 4<<20 - n
		if _, err := os.Stat(part); err != nil {
			free += 6 << 20 // the dropped copy's bytes are free again
		}
		return free, floor
	}
	open(8<<20, 6<<20)
	if err := rf.spill.reserve(2 << 20); err != nil || dropped() {
		t.Fatalf("2 MiB and the copy's last 2 MiB reach the floor exactly: %v, copy dropped %v", err, dropped())
	}
	if err := rf.spill.reserve(3 << 20); err != nil || !dropped() {
		t.Fatalf("3 MiB and the copy's last 2 MiB pass the floor: %v, copy dropped %v", err, dropped())
	}
	rf.spill.Free(mem)
	rf.tee = nil
}

// The same through a real download: the index of a large row spills while
// the row is copied into the cache. At every read of the object, the copy
// on disk, what is still set aside for it, the cache and the spill fit the
// sizeLimit; the copy is given up for the spill, and the row is indexed
// all the same.
func TestRowFetcherSpillBesideItsOwnCopy(t *testing.T) {
	spillOnDisk(t, true)
	const n = 12000
	a := windowArchive(t, n)
	fetch := func(limit int64) (peakSpill, size int64, cached bool) {
		t.Helper()
		store := &watchedStore{ArchiveStore: a.store}
		rf, refs, _ := fetcherFor(t, a, store)
		rf.budget = pitrplan.NewBudget(3 << 20)
		rf.limit = limit
		rf.spill = openStagingSpill(pitrDownloadRequest{outDir: rf.stagingDir(), memory: 3 << 20}, rf, rf.budget)
		if rf.spill == nil {
			t.Skip("no file-backed spill on this platform")
		}
		defer rf.spill.close()
		ref := refs["a/mysql-bin.000001"]
		size = rf.entries[ref].Size
		store.onRead = func() {
			spill := rf.spill.diskBytes()
			peakSpill = max(peakSpill, spill)
			var onDisk int64
			if st, err := os.Stat(filepath.Join(rf.cacheDir, "1.part")); err == nil {
				onDisk = blocks(st.Size())
			}
			if onDisk > rf.tee.reserved() {
				t.Fatalf("the open copy holds %d bytes, more than the %d set aside for it", onDisk, rf.tee.reserved())
			}
			if limit > 0 && spill+rf.tee.reserved()+rf.cachedBytes+stagingReserve > limit {
				t.Fatalf("%d spilled and %d set aside for the open copy (%d written), with the reserve, pass the sizeLimit of %d", spill, rf.tee.reserved(), onDisk, limit)
			}
		}
		idx, err := rf.FetchForPlan(context.Background(), ref, true)
		if err != nil || len(idx.Txns) != n {
			t.Fatalf("fetch with sizeLimit %d: %v", limit, err)
		}
		_, cached = rf.cached[ref]
		return peakSpill, size, cached
	}
	// With room for both, the row is cached and its index spilled.
	peak, size, cached := fetch(0)
	if peak == 0 || !cached {
		t.Fatalf("without a sizeLimit: %d bytes spilled, row cached %v; the test needs a spill beside a cached row", peak, cached)
	}
	// Room for the copy when the download starts, not for the copy and the
	// spill: the copy gives way.
	limit := stagingReserve + blocks(size) + peak/2
	if _, _, cached = fetch(limit); cached {
		t.Fatalf("a sizeLimit of %d holds a row of %d bytes and %d bytes of spill", limit, size, peak)
	}
	// Room for both again.
	if _, _, cached = fetch(stagingReserve + blocks(size) + 2*peak); !cached {
		t.Fatal("with room for the copy and the spill the row was not cached")
	}
}

// watchedStore calls onRead before every read of an object.
type watchedStore struct {
	sidecar.ArchiveStore
	onRead func()
}

func (w *watchedStore) Open(ctx context.Context, key string) (io.ReadCloser, error) {
	r, err := w.ArchiveStore.Open(ctx, key)
	if err != nil {
		return nil, err
	}
	return struct {
		io.Reader
		io.Closer
	}{readerFunc(func(p []byte) (int, error) {
		if w.onRead != nil {
			w.onRead()
		}
		return r.Read(p)
	}), r}, nil
}

type readerFunc func([]byte) (int, error)

func (f readerFunc) Read(p []byte) (int, error) { return f(p) }

// TestStagingInMemory: a memory-backed staging volume (an emptyDir with
// medium Memory) gets no spill, and neither does one that cannot be
// examined.
func TestStagingInMemory(t *testing.T) {
	if !stagingInMemory(filepath.Join(t.TempDir(), "missing")) {
		t.Error("a directory that cannot be examined counts as disk")
	}
	if _, err := os.Stat("/dev/shm"); err == nil && !stagingInMemory("/dev/shm") {
		t.Error("/dev/shm (tmpfs) counts as disk")
	}
	old := pitrStagingInMemory
	defer func() { pitrStagingInMemory = old }()
	pitrStagingInMemory = func(string) bool { return true }
	budget := pitrplan.NewBudget(64 << 20)
	if s := openStagingSpill(pitrDownloadRequest{outDir: t.TempDir(), memory: 64 << 20}, &rowFetcher{}, budget); s != nil || budget.Spilling() {
		t.Fatal("a memory-backed staging volume got a spill")
	}
	// A nil spill is inert.
	var none *stagingSpill
	if none.diskBytes() != 0 || none.finish() != nil || none.guard(func() error { return nil }) != nil {
		t.Fatal("a nil spill is not inert")
	}
	none.close()
}
