package main

import (
	"context"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"log/slog"
	"os"
	"sort"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/shipstream/bloodraven/internal/pitrarchive"
	"github.com/shipstream/bloodraven/internal/pitrplan"
	"github.com/shipstream/bloodraven/internal/sidecar"
)

// flakyStore fails Open, or the stream midway, for the first n calls on a
// key (n < 0: always), with a transport error.
type flakyStore struct {
	sidecar.ArchiveStore
	mu      sync.Mutex
	open    map[string]int // remaining Open failures per key
	midway  map[string]int // remaining mid-stream failures per key
	opens   map[string]int
	missing map[string]bool
	failErr error
}

func newFlakyStore(inner sidecar.ArchiveStore) *flakyStore {
	return &flakyStore{ArchiveStore: inner, open: map[string]int{}, midway: map[string]int{}, opens: map[string]int{}, missing: map[string]bool{},
		failErr: errors.New("read tcp 10.0.0.1:443: connection reset by peer")}
}

type brokenReader struct {
	r    io.Reader
	left int
	err  error
}

func (b *brokenReader) Read(p []byte) (int, error) {
	if b.left <= 0 {
		return 0, b.err
	}
	if len(p) > b.left {
		p = p[:b.left]
	}
	n, err := b.r.Read(p)
	b.left -= n
	return n, err
}

func (s *flakyStore) Open(ctx context.Context, key string) (io.ReadCloser, error) {
	s.mu.Lock()
	s.opens[key]++
	failOpen := s.open[key] != 0
	if s.open[key] > 0 {
		s.open[key]--
	}
	failMid := !failOpen && s.midway[key] != 0
	if failMid && s.midway[key] > 0 {
		s.midway[key]--
	}
	missing := s.missing[key]
	s.mu.Unlock()
	if missing {
		return nil, fmt.Errorf("open %s: %w", key, fs.ErrNotExist)
	}
	if failOpen {
		return nil, s.failErr
	}
	r, err := s.ArchiveStore.Open(ctx, key)
	if err != nil || !failMid {
		return r, err
	}
	return struct {
		io.Reader
		io.Closer
	}{&brokenReader{r: r, left: 100, err: io.ErrUnexpectedEOF}, r}, nil
}

func fetcherFor(t *testing.T, a *dlArchive, store sidecar.ArchiveStore) (*rowFetcher, map[string]pitrplan.RowRef, map[string]string) {
	t.Helper()
	rf := &rowFetcher{store: store, prefix: downloadPrefix, entries: map[pitrplan.RowRef]sidecar.ManifestEntry{}, budget: pitrplan.NewBudget(64 << 20),
		cacheDir: t.TempDir(), cached: map[pitrplan.RowRef]string{}, digests: map[pitrplan.RowRef]string{},
		freeAfter: func(string, int64) (int64, int64) { return 1 << 40, 0 }, backoff: []time.Duration{0, 0, 0}}
	refs, keys := map[string]pitrplan.RowRef{}, map[string]string{}
	for site, m := range a.manifests {
		for _, e := range m.Files {
			ref := pitrplan.RowRef{Site: site, Name: e.Name, ID: e.ID}
			rf.entries[ref] = e
			refs[site+"/"+e.Name] = ref
			k, err := sidecar.ArchiveEntryKey(downloadPrefix, site, e)
			if err != nil {
				t.Fatal(err)
			}
			keys[site+"/"+e.Name] = k
		}
	}
	return rf, refs, keys
}

// P4: transient store errors (at Open or midway through the stream) are
// retried; a missing object is not; persistent transient errors end as a
// *pitrplan.TransientError, never an unreadable row.
func TestRowFetcherRetriesTransientErrors(t *testing.T) {
	a := failbackArchive(t)
	store := newFlakyStore(a.store)
	rf, refs, keys := fetcherFor(t, a, store)
	row := "a/mysql-bin.000002"
	store.open[keys[row]] = 2
	store.midway[keys[row]] = 1
	used := rf.budget.Used()
	idx, err := rf.FetchForPlan(context.Background(), refs[row], true)
	if err != nil || idx == nil || store.opens[keys[row]] != 4 {
		t.Fatalf("after 2 open and 1 midway failures: %v, %d opens", err, store.opens[keys[row]])
	}
	// The failed attempts' partial charges were released: the retried
	// fetch charges what a clean one does.
	clean, _, _ := fetcherFor(t, a, a.store)
	cleanUsed := clean.budget.Used()
	if _, err := clean.FetchForPlan(context.Background(), refs[row], true); err != nil {
		t.Fatal(err)
	}
	if retried, once := rf.budget.Used()-used, clean.budget.Used()-cleanUsed; retried != once {
		t.Fatalf("retried fetch charged %d, a clean one %d", retried, once)
	}

	// Persistent: a TransientError after 1 + len(backoff) attempts.
	other := "a/mysql-bin.000003"
	store.open[keys[other]] = -1
	_, err = rf.FetchForPlan(context.Background(), refs[other], true)
	if !pitrplan.IsTransient(err) || store.opens[keys[other]] != 4 || !strings.Contains(err.Error(), "connection reset") {
		t.Fatalf("persistent failure: %v after %d opens", err, store.opens[keys[other]])
	}
	if _, ok := pitrplan.RefusalOf(err); ok {
		t.Fatalf("a transient error became a refusal: %v", err)
	}

	// A missing object is the planner's unreadable row: one attempt, no
	// TransientError.
	lost := "b/mysql-bin.000001"
	store.missing[keys[lost]] = true
	_, err = rf.FetchForPlan(context.Background(), refs[lost], true)
	if err == nil || pitrplan.IsTransient(err) || store.opens[keys[lost]] != 1 {
		t.Fatalf("missing object: %v after %d opens", err, store.opens[keys[lost]])
	}
}

// P4 end to end: a planner download that keeps failing transiently ends
// the Job retryably, without a token.
func TestPlanAndStageTransientErrorIsNotARefusal(t *testing.T) {
	a := failbackArchive(t)
	store := newFlakyStore(a.store)
	_, _, keys := fetcherFor(t, a, a.store)
	store.open[keys["a/mysql-bin.000003"]] = -1
	old := pitrFetchBackoff
	pitrFetchBackoff = []time.Duration{0}
	defer func() { pitrFetchBackoff = old }()
	out, metaDir := t.TempDir(), t.TempDir()
	req := pitrDownloadRequest{
		archive: pitrArchiveRequest{prefix: downloadPrefix, expect: pitrarchive.Expectation{Role: pitrarchive.RoleReader, Destination: a.dest,
			Encryption: pitrarchive.EncryptionIdentity{Mode: pitrarchive.EncryptionNone}}},
		mode: pitrplan.ModeTimestamp, stop: a.tB(36).Add(-500 * time.Millisecond), exclude: pitrplan.Set{}, outDir: out, delta: 5 * time.Second, memory: 64 << 20,
		dumpMetaPath: writeDumpJSON(t, metaDir, sidA+":1-40", a.tA(42), a),
	}
	_, err := planAndStage(context.Background(), store, req, slog.New(slog.NewTextHandler(io.Discard, nil)))
	if err == nil || !pitrplan.IsTransient(err) {
		t.Fatalf("err = %v, want a transient error", err)
	}
	if tok, ok := pitrarchive.ParseToken(err.Error()); ok {
		t.Fatalf("a transient error carries token %s: %v", tok, err)
	}
	if c := readCoverage(t, out); strings.Contains(c.RefusalReason, "quarantine") || c.RefusalReason == "" {
		t.Fatalf("coverage refusal %q", c.RefusalReason)
	}
}

// P2: the tee cache holds only rows that may be staged, never more than
// the staging emptyDir's sizeLimit allows, and is pruned to the plan's
// files before staging, whose total must fit.
func TestRowFetcherStagingLimit(t *testing.T) {
	a := failbackArchive(t)
	rf, refs, _ := fetcherFor(t, a, a.store)
	sizes := map[string]int64{}
	for name, ref := range refs {
		sizes[name] = rf.entries[ref].Size
	}
	r1, r2, r3 := "a/mysql-bin.000001", "a/mysql-bin.000002", "a/mysql-bin.000003"
	rf.limit = stagingReserve + blocks(sizes[r1]) + blocks(sizes[r2])
	fetch := func(name string, mayStage bool) {
		t.Helper()
		if _, err := rf.FetchForPlan(context.Background(), refs[name], mayStage); err != nil {
			t.Fatal(err)
		}
	}
	fetch("b/mysql-bin.000001", false) // never staged: verified, not written
	fetch(r1, true)
	fetch(r2, true)
	fetch(r3, true) // no room left under the limit
	cachedNames := func() []string {
		var out []string
		for ref := range rf.cached {
			out = append(out, ref.Site+"/"+ref.Name)
		}
		sort.Strings(out)
		return out
	}
	if got := strings.Join(cachedNames(), ","); got != r1+","+r2 {
		t.Fatalf("cached %s, want %s,%s", got, r1, r2)
	}
	if rf.cachedBytes > rf.limit-stagingReserve {
		t.Fatalf("cache uses %d of a %d limit", rf.cachedBytes, rf.limit)
	}
	file := func(name string) pitrplan.PlanFile {
		ref := refs[name]
		return pitrplan.PlanFile{Row: ref, Size: rf.entries[ref].Size, Digest: rf.digests[ref]}
	}
	// The staged set does not fit: a refusal, not an eviction.
	err := rf.prepareStaging([]pitrplan.PlanFile{file(r1), file(r2), file(r3)}, 1000)
	if r, ok := pitrplan.RefusalOf(err); !ok || r.Token != pitrarchive.TokenPlanTooLarge || !strings.Contains(r.Message, "stagingVolumeSizeLimit") {
		t.Fatalf("oversized staging: %v", err)
	}
	// It fits once r1 is pruned from the cache: r1 is not a plan file.
	rf.limit = stagingReserve + blocks(sizes[r2]) + blocks(sizes[r3]) + blocks(1000)
	if err := rf.prepareStaging([]pitrplan.PlanFile{file(r2), file(r3)}, 1000); err != nil {
		t.Fatal(err)
	}
	if got := strings.Join(cachedNames(), ","); got != r2 || rf.cachedBytes != blocks(sizes[r2]) {
		t.Fatalf("after pruning: cached %s (%d bytes)", got, rf.cachedBytes)
	}
	if entries, _ := os.ReadDir(rf.cacheDir); len(entries) != 1 {
		t.Fatalf("cache dir holds %d files", len(entries))
	}
	// A node filesystem without room for the uncached files refuses too.
	rf.freeAfter = func(string, int64) (int64, int64) { return -1, 0 }
	err = rf.prepareStaging([]pitrplan.PlanFile{file(r2), file(r3)}, 1000)
	if r, ok := pitrplan.RefusalOf(err); !ok || r.Token != pitrarchive.TokenPlanTooLarge {
		t.Fatalf("full node filesystem: %v", err)
	}
	// Staging still works from the pruned cache and by re-download.
	rf.freeAfter = func(string, int64) (int64, int64) { return 1 << 40, 0 }
	out := t.TempDir()
	for _, name := range []string{r2, r3} {
		if err := rf.stage(context.Background(), out, file(name)); err != nil {
			t.Fatal(err)
		}
	}
	if got := stagedFiles(t, out); len(got) != 2 {
		t.Fatalf("staged %v", got)
	}
}

// P2 end to end: a staging limit smaller than the plan's files refuses
// PITRPlanTooLarge with only coverage.json left behind.
func TestPlanAndStageStagingLimit(t *testing.T) {
	a := failbackArchive(t)
	out, metaDir := t.TempDir(), t.TempDir()
	req := pitrDownloadRequest{
		archive: pitrArchiveRequest{prefix: downloadPrefix, expect: pitrarchive.Expectation{Role: pitrarchive.RoleReader, Destination: a.dest,
			Encryption: pitrarchive.EncryptionIdentity{Mode: pitrarchive.EncryptionNone}}},
		mode: pitrplan.ModeTimestamp, stop: a.tB(36).Add(-500 * time.Millisecond), exclude: pitrplan.Set{}, outDir: out, delta: 5 * time.Second, memory: 64 << 20,
		dumpMetaPath: writeDumpJSON(t, metaDir, sidA+":1-40", a.tA(42), a), stagingLimit: stagingReserve + 4096,
	}
	_, err := planAndStage(context.Background(), a.store, req, slog.New(slog.NewTextHandler(io.Discard, nil)))
	if tok, _ := pitrarchive.ParseToken(errString(err)); tok != pitrarchive.TokenPlanTooLarge {
		t.Fatalf("err = %v, want PITRPlanTooLarge", err)
	}
	if got := stagedFiles(t, out); len(got) != 0 {
		t.Fatalf("refusal left %v", got)
	}
	if c := readCoverage(t, out); !strings.HasPrefix(c.RefusalReason, "PITRPlanTooLarge: ") {
		t.Fatalf("coverage refusal %q", c.RefusalReason)
	}
	// With room, the same plan stages.
	req.stagingLimit = 64 << 20
	if _, err := planAndStage(context.Background(), a.store, req, slog.New(slog.NewTextHandler(io.Discard, nil))); err != nil {
		t.Fatal(err)
	}
}
