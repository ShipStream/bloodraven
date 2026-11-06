package main

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"log/slog"
	"os"
	"path/filepath"
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
	// get and list fail Get and List (n < 0: always); listExtra adds
	// keys to every listing.
	get       map[string]int
	list      int
	listExtra []string
	// ctx, when set, is cancelled at the first Open of a key in cancelOn.
	cancel   context.CancelFunc
	cancelOn map[string]bool
}

func newFlakyStore(inner sidecar.ArchiveStore) *flakyStore {
	return &flakyStore{ArchiveStore: inner, open: map[string]int{}, midway: map[string]int{}, opens: map[string]int{}, missing: map[string]bool{},
		failErr: errors.New("read tcp 10.0.0.1:443: connection reset by peer"), get: map[string]int{}, cancelOn: map[string]bool{}}
}

func (s *flakyStore) Get(ctx context.Context, key string) ([]byte, bool, error) {
	s.mu.Lock()
	fail := s.get[key] != 0
	if s.get[key] > 0 {
		s.get[key]--
	}
	s.mu.Unlock()
	if fail {
		return nil, false, s.failErr
	}
	return s.ArchiveStore.Get(ctx, key)
}

func (s *flakyStore) List(ctx context.Context, prefix string) ([]string, error) {
	s.mu.Lock()
	fail := s.list != 0
	if s.list > 0 {
		s.list--
	}
	s.mu.Unlock()
	if fail {
		return nil, s.failErr
	}
	keys, err := s.ArchiveStore.List(ctx, prefix)
	return append(keys, s.listExtra...), err
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
	cancel := s.cancelOn[key] && s.cancel != nil
	s.mu.Unlock()
	if cancel {
		s.cancel()
		return nil, fmt.Errorf("open %s: %w", key, ctx.Err())
	}
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
		cacheDir: filepath.Join(t.TempDir(), ".pitr-cache"), cached: map[pitrplan.RowRef]cachedRow{},
		freeAfter: func(string, int64) (int64, int64) { return 1 << 40, 0 }, backoff: []time.Duration{0, 0, 0}}
	if err := os.MkdirAll(rf.cacheDir, 0o700); err != nil {
		t.Fatal(err)
	}
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
	digests := map[string]string{}
	fetch := func(name string, mayStage bool) {
		t.Helper()
		idx, err := rf.FetchForPlan(context.Background(), refs[name], mayStage)
		if err != nil {
			t.Fatal(err)
		}
		digests[name] = idx.Digest
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
		return pitrplan.PlanFile{Row: ref, Size: rf.entries[ref].Size, Digest: digests[name]}
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

func dlRequest(a *dlArchive, out, dumpMeta string, stop time.Time) pitrDownloadRequest {
	return pitrDownloadRequest{
		archive: pitrArchiveRequest{prefix: downloadPrefix, expect: pitrarchive.Expectation{Role: pitrarchive.RoleReader, Destination: a.dest,
			Encryption: pitrarchive.EncryptionIdentity{Mode: pitrarchive.EncryptionNone}}},
		mode: pitrplan.ModeTimestamp, stop: stop, exclude: pitrplan.Set{}, outDir: out, delta: 5 * time.Second, memory: 64 << 20,
		dumpMetaPath: dumpMeta,
	}
}

func dlOpenRequest(a *dlArchive) pitrArchiveRequest {
	return pitrArchiveRequest{prefix: downloadPrefix, expect: pitrarchive.Expectation{Role: pitrarchive.RoleReader, Destination: a.dest,
		Encryption: pitrarchive.EncryptionIdentity{Mode: pitrarchive.EncryptionNone}, SourceUID: a.desc.Source.UID, ArchiveID: a.desc.ArchiveID}}
}

// F020: the archive-open reads (descriptor, listing, each manifest) are
// retried like row reads, and a persistent transport failure ends
// token-less, never as list-permission or corruption refusals.
func TestOpenPITRArchiveRetriesTransientReads(t *testing.T) {
	a := failbackArchive(t)
	backoff := []time.Duration{0, 0}
	descKey, manifestKey := pitrarchive.DescriptorKey(downloadPrefix), sidecar.ManifestKey(downloadPrefix, "b")
	for _, tc := range []struct {
		name  string
		setup func(s *flakyStore)
	}{
		{name: "descriptor", setup: func(s *flakyStore) { s.get[descKey] = 2 }},
		{name: "listing", setup: func(s *flakyStore) { s.list = 2 }},
		{name: "manifest", setup: func(s *flakyStore) { s.get[manifestKey] = 2 }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			s := newFlakyStore(a.store)
			tc.setup(s)
			if opened, err := openPITRArchive(context.Background(), s, dlOpenRequest(a), backoff); err != nil || len(opened.manifests) != 2 {
				t.Fatalf("after two transient failures: %v", err)
			}
			s = newFlakyStore(a.store)
			tc.setup(s)
			s.get[descKey], s.get[manifestKey] = -s.get[descKey]/2, -s.get[manifestKey]/2
			if s.list != 0 {
				s.list = -1
			}
			_, err := openPITRArchive(context.Background(), s, dlOpenRequest(a), backoff)
			if !pitrplan.IsTransient(err) || !strings.Contains(err.Error(), "connection reset") {
				t.Fatalf("persistent failure: %v, want a transient error", err)
			}
			if tok, ok := pitrarchive.ParseToken(err.Error()); ok {
				t.Fatalf("a transport failure carries token %s: %v", tok, err)
			}
		})
	}
	// A listing refused for permission is a refusal with its remedy, at once.
	s := newFlakyStore(a.store)
	s.list, s.failErr = -1, fmt.Errorf("list: %w", fs.ErrPermission)
	_, err := openPITRArchive(context.Background(), s, dlOpenRequest(a), backoff)
	if tok, _ := pitrarchive.ParseToken(errString(err)); tok != pitrarchive.TokenArchiveListFailed {
		t.Fatalf("permission denied: %v", err)
	}
	// A manifest listed and then removed is a changed archive, not a
	// mixed one.
	s = newFlakyStore(a.store)
	s.listExtra = []string{sidecar.ManifestKey(downloadPrefix, "gone")}
	_, err = openPITRArchive(context.Background(), s, dlOpenRequest(a), backoff)
	if !pitrplan.IsTransient(err) || !strings.Contains(err.Error(), "listed but is gone") {
		t.Fatalf("manifest gone after listing: %v", err)
	}
}

// F021: an index refusal stands only for authentic bytes; corrupt bytes
// (an event size inflated past the decoder bound) are ObjectInvalid, not a
// memory problem.
func TestRowFetcherRefusalNeedsAuthenticBytes(t *testing.T) {
	a := failbackArchive(t)
	rf, refs, keys := fetcherFor(t, a, a.store)
	row := "a/mysql-bin.000002"
	data, _, err := a.store.Get(context.Background(), keys[row])
	if err != nil {
		t.Fatal(err)
	}
	corrupt := append([]byte(nil), data...)
	binary.LittleEndian.PutUint32(corrupt[4+9:], 1<<30) // the FORMAT_DESCRIPTION's size
	putObject(t, a.store, keys[row], corrupt)
	_, err = rf.FetchForPlan(context.Background(), refs[row], true)
	if r, ok := pitrplan.RefusalOf(err); !ok || r.Token != pitrarchive.TokenArchiveObjectInvalid || !strings.Contains(r.Message, "digest") {
		t.Fatalf("corrupt event size: %v, want ObjectInvalid (digest)", err)
	}
	// The same refusal on authentic bytes stands.
	putObject(t, a.store, keys[row], data)
	rf.budget = pitrplan.NewBudget(512) // MaxEventBytes 64 bytes
	_, err = rf.FetchForPlan(context.Background(), refs[row], true)
	if r, ok := pitrplan.RefusalOf(err); !ok || r.Token != pitrarchive.TokenPlanTooLarge {
		t.Fatalf("authentic bytes over the bound: %v, want PlanTooLarge", err)
	}
}

// F058: a missing object of a row retention retired after the planner's
// snapshot is a changed archive (transient), not a damaged row; one the
// live manifest still lists is the planner's unreadable row.
func TestRowFetcherRetiredRow(t *testing.T) {
	a := failbackArchive(t)
	store := newFlakyStore(a.store)
	rf, refs, keys := fetcherFor(t, a, store)
	row := "a/mysql-bin.000001"
	store.missing[keys[row]] = true
	_, err := rf.FetchForPlan(context.Background(), refs[row], true)
	if err == nil || pitrplan.IsTransient(err) {
		t.Fatalf("missing object of a listed row: %v, want an unreadable row", err)
	}
	// Retention retires the row from the live manifest.
	m := a.manifests["a"]
	m.Files = m.Files[1:]
	a.save(t)
	_, err = rf.FetchForPlan(context.Background(), refs[row], true)
	if !pitrplan.IsTransient(err) || !strings.Contains(err.Error(), "row retired") {
		t.Fatalf("missing object of a retired row: %v, want a transient error", err)
	}
	if _, ok := pitrplan.RefusalOf(err); ok {
		t.Fatalf("a retired row became a refusal: %v", err)
	}
}

// F023: cancellation while reading is neither a refusal nor damage.
func TestRowFetcherCancellation(t *testing.T) {
	a := failbackArchive(t)
	store := newFlakyStore(a.store)
	rf, refs, keys := fetcherFor(t, a, store)
	row := "a/mysql-bin.000002"
	idx, err := rf.FetchForPlan(context.Background(), refs[row], false)
	if err != nil {
		t.Fatal(err)
	}
	f := pitrplan.PlanFile{Row: refs[row], Size: idx.Size, Digest: idx.Digest}
	for _, phase := range []string{"plan", "stage"} {
		ctx, cancel := context.WithCancel(context.Background())
		store.cancel, store.cancelOn[keys[row]] = cancel, true
		if phase == "plan" {
			_, err = rf.FetchForPlan(ctx, refs[row], false)
		} else {
			err = rf.stage(ctx, t.TempDir(), f)
		}
		cancel()
		if err == nil || !errors.Is(err, context.Canceled) {
			t.Fatalf("%s: %v, want the context's error", phase, err)
		}
		if tok, ok := pitrarchive.ParseToken(err.Error()); ok {
			t.Fatalf("%s: cancellation carries token %s: %v", phase, tok, err)
		}
		store.cancelOn[keys[row]] = false
	}
}

// F129: the tee writes no more than its allowance, whatever the manifest
// says the row's size is, accounts the cache with the authenticated size,
// and refuses a row whose manifest size disagrees with its bytes.
func TestRowFetcherUnderstatedSize(t *testing.T) {
	a := failbackArchive(t)
	rf, refs, _ := fetcherFor(t, a, a.store)
	row := "a/mysql-bin.000002"
	ref := refs[row]
	actual := rf.entries[ref].Size
	if actual <= stagingBlock {
		t.Fatalf("fixture row is %d bytes; the test needs more than one block", actual)
	}
	e := rf.entries[ref]
	e.Size = 1
	rf.entries[ref] = e
	rf.limit = stagingReserve + stagingBlock // room for one block
	_, err := rf.FetchForPlan(context.Background(), ref, true)
	if r, ok := pitrplan.RefusalOf(err); !ok || r.Token != pitrarchive.TokenArchiveObjectInvalid || !strings.Contains(r.Message, "says 1 bytes") {
		t.Fatalf("understated size: %v", err)
	}
	if entries, _ := os.ReadDir(rf.cacheDir); len(entries) != 0 || len(rf.cached) != 0 || rf.cachedBytes != 0 {
		t.Fatalf("cache holds %d files, %d bytes", len(entries), rf.cachedBytes)
	}
	// A correct size caches, accounted with the authenticated size.
	e.Size = actual
	rf.entries[ref] = e
	rf.limit = 0
	if _, err := rf.FetchForPlan(context.Background(), ref, true); err != nil {
		t.Fatal(err)
	}
	if c, ok := rf.cached[ref]; !ok || c.size != actual || rf.cachedBytes != blocks(actual) {
		t.Fatalf("cached %+v, %d bytes", rf.cached[ref], rf.cachedBytes)
	}
}

// F022: the final staging admission keeps the node's floor, not just a
// positive balance.
func TestPrepareStagingKeepsNodeFloor(t *testing.T) {
	a := failbackArchive(t)
	rf, refs, _ := fetcherFor(t, a, a.store)
	row := "a/mysql-bin.000002"
	idx, err := rf.FetchForPlan(context.Background(), refs[row], false)
	if err != nil {
		t.Fatal(err)
	}
	files := []pitrplan.PlanFile{{Row: refs[row], Size: idx.Size, Digest: idx.Digest}}
	floor := int64(256 << 20)
	rf.freeAfter = func(dir string, n int64) (int64, int64) { return floor + 1<<20 - n, floor }
	err = rf.prepareStaging(files, 1000)
	if r, ok := pitrplan.RefusalOf(err); !ok || r.Token != pitrarchive.TokenPlanTooLarge || !strings.Contains(r.Message, "floor") {
		t.Fatalf("staging below the floor: %v", err)
	}
	rf.freeAfter = func(dir string, n int64) (int64, int64) { return floor + 8<<20 - n, floor }
	if err := rf.prepareStaging(files, 1000); err != nil {
		t.Fatal(err)
	}
	// The re-download rechecks the floor for its file.
	rf.freeAfter = func(dir string, n int64) (int64, int64) { return floor - 1, floor }
	if r, ok := pitrplan.RefusalOf(rf.stage(context.Background(), t.TempDir(), files[0])); !ok || r.Token != pitrarchive.TokenPlanTooLarge {
		t.Fatal("a re-download below the floor was staged")
	}
}

// F132: the staging guards for re-downloaded files.
func TestRowFetcherStageGuards(t *testing.T) {
	a := failbackArchive(t)
	store := newFlakyStore(a.store)
	rf, refs, keys := fetcherFor(t, a, store)
	row := "a/mysql-bin.000002"
	idx, err := rf.FetchForPlan(context.Background(), refs[row], false)
	if err != nil {
		t.Fatal(err)
	}
	f := pitrplan.PlanFile{Row: refs[row], Size: idx.Size, Digest: idx.Digest}
	key := keys[row]
	data, _, _ := a.store.Get(context.Background(), key)
	opensBefore := func() int { return store.opens[key] }

	// Transient failures are retried; persistent ones end as a
	// TransientError after 1 + len(backoff) opens.
	store.open[key] = 2
	n := opensBefore()
	if err := rf.stage(context.Background(), t.TempDir(), f); err != nil || store.opens[key]-n != 3 {
		t.Fatalf("after two transient failures: %v, %d opens", err, store.opens[key]-n)
	}
	store.open[key] = -1
	n = opensBefore()
	if err := rf.stage(context.Background(), t.TempDir(), f); !pitrplan.IsTransient(err) || store.opens[key]-n != 1+len(rf.backoff) {
		t.Fatalf("persistent failure: %v, %d opens", err, store.opens[key]-n)
	}
	store.open[key] = 0

	// An object that changed since planning, or became unreadable, is
	// refused, and nothing is left staged.
	for _, tc := range []struct {
		name, detail string
		setup, undo  func()
	}{
		{name: "changed", detail: "changed during staging", setup: func() { putObject(t, a.store, key, append(append([]byte(nil), data...), 0)) }, undo: func() { putObject(t, a.store, key, data) }},
		{name: "unreadable", detail: "became unreadable", setup: func() { store.missing[key] = true }, undo: func() { store.missing[key] = false }},
	} {
		tc.setup()
		out := t.TempDir()
		err := rf.stage(context.Background(), out, f)
		tc.undo()
		if tok, _ := pitrarchive.ParseToken(errString(err)); tok != pitrarchive.TokenArchiveObjectInvalid || !strings.Contains(errString(err), tc.detail) {
			t.Fatalf("%s: %v", tc.name, err)
		}
		if got := stagedFiles(t, out); len(got) != 0 {
			t.Fatalf("%s left %v", tc.name, got)
		}
	}

	// A manifest row whose name is not a plain file name never reaches the
	// store.
	bad := f
	bad.Row.Name = "../escape"
	rf.entries[bad.Row] = sidecar.ManifestEntry{Name: "../escape", ID: f.Row.ID}
	if err := rf.stage(context.Background(), t.TempDir(), bad); err == nil {
		t.Fatal("an unsafe row name was staged")
	}

	// A cache that cannot be written costs a re-download, not the row.
	rf2, refs2, keys2 := fetcherFor(t, a, store)
	if err := os.Chmod(rf2.cacheDir, 0o500); err != nil {
		t.Fatal(err)
	}
	defer func() { _ = os.Chmod(rf2.cacheDir, 0o700) }()
	n = store.opens[keys2[row]]
	idx2, err := rf2.FetchForPlan(context.Background(), refs2[row], true)
	if err != nil || len(rf2.cached) != 0 {
		t.Fatalf("unwritable cache: %v, %d cached", err, len(rf2.cached))
	}
	out := t.TempDir()
	if err := rf2.stage(context.Background(), out, pitrplan.PlanFile{Row: refs2[row], Size: idx2.Size, Digest: idx2.Digest}); err != nil || store.opens[keys2[row]]-n != 2 {
		t.Fatalf("staging after an unwritable cache: %v, %d opens", err, store.opens[keys2[row]]-n)
	}
}

// F123: the store env must address the destination the Job names.
func TestCheckStoreDestination(t *testing.T) {
	s3dest, err := pitrarchive.NewS3Destination("bucket-a", "orders", "")
	if err != nil {
		t.Fatal(err)
	}
	pvc := downloadDest(t)
	for _, tc := range []struct {
		name string
		cfg  sidecar.PITRConfig
		dest pitrarchive.Destination
		ok   bool
	}{
		{name: "s3 matches", cfg: sidecar.PITRConfig{StorageType: "S3", ManifestPrefix: s3dest.KeyPrefix, S3: &sidecar.PITRS3Config{Bucket: "bucket-a"}}, dest: s3dest, ok: true},
		{name: "another bucket", cfg: sidecar.PITRConfig{StorageType: "S3", ManifestPrefix: s3dest.KeyPrefix, S3: &sidecar.PITRS3Config{Bucket: "bucket-b"}}, dest: s3dest},
		{name: "another endpoint", cfg: sidecar.PITRConfig{StorageType: "S3", ManifestPrefix: s3dest.KeyPrefix, S3: &sidecar.PITRS3Config{Bucket: "bucket-a", EndpointURL: "https://minio.example:9000"}}, dest: s3dest},
		{name: "pvc env for an s3 destination", cfg: sidecar.PITRConfig{StorageType: "PVC", ManifestPrefix: s3dest.KeyPrefix, PVC: &sidecar.PITRPVCConfig{MountPath: "/a"}}, dest: s3dest},
		{name: "pvc matches", cfg: sidecar.PITRConfig{StorageType: "PVC", ManifestPrefix: pvc.KeyPrefix, PVC: &sidecar.PITRPVCConfig{MountPath: "/a"}}, dest: pvc, ok: true},
		{name: "s3 env for a pvc destination", cfg: sidecar.PITRConfig{StorageType: "S3", ManifestPrefix: pvc.KeyPrefix, S3: &sidecar.PITRS3Config{Bucket: "bucket-a"}}, dest: pvc},
		{name: "another prefix", cfg: sidecar.PITRConfig{StorageType: "PVC", ManifestPrefix: "other/binlogs", PVC: &sidecar.PITRPVCConfig{MountPath: "/a"}}, dest: pvc},
	} {
		cfg := tc.cfg
		if err := checkStoreDestination(&cfg, tc.dest); (err == nil) != tc.ok {
			t.Errorf("%s: %v", tc.name, err)
		}
	}
}

// F131: a refusal before the planner reads the dump still records the
// dump's baseline in coverage.json.
func TestPlanAndStageRefusalKeepsBaseline(t *testing.T) {
	a := failbackArchive(t)
	out, metaDir := t.TempDir(), t.TempDir()
	req := dlRequest(a, out, writeDumpJSON(t, metaDir, sidA+":1-40", a.tA(42), a), time.Time{})
	_, err := planAndStage(context.Background(), a.store, req, slog.New(slog.NewTextHandler(io.Discard, nil)))
	if tok, _ := pitrarchive.ParseToken(errString(err)); tok != pitrarchive.TokenTargetInvalid {
		t.Fatalf("err = %v, want PITRTargetInvalid", err)
	}
	if c := readCoverage(t, out); c.Baseline != sidA+":1-40" || !strings.HasPrefix(c.RefusalReason, "PITRTargetInvalid") {
		t.Fatalf("coverage baseline %q, refusal %q", c.Baseline, c.RefusalReason)
	}
}

// F211: the manifest rows are charged to the planner budget before the
// planner runs, so a huge manifest refuses PITRPlanTooLarge.
func TestPlanAndStageChargesManifests(t *testing.T) {
	a := failbackArchive(t)
	out, metaDir := t.TempDir(), t.TempDir()
	req := dlRequest(a, out, writeDumpJSON(t, metaDir, sidA+":1-40", a.tA(42), a), a.tA(80))
	req.memory = 2 << 10
	_, err := planAndStage(context.Background(), a.store, req, slog.New(slog.NewTextHandler(io.Discard, nil)))
	if r, ok := pitrplan.RefusalOf(err); !ok || r.Token != pitrarchive.TokenPlanTooLarge || !strings.Contains(r.Message, "manifest") {
		t.Fatalf("err = %v, want PITRPlanTooLarge charging the manifests", err)
	}
}

// F129: the tee drops its copy as soon as the bytes pass its allowance.
func TestCacheTeeAllowance(t *testing.T) {
	part := filepath.Join(t.TempDir(), "1.part")
	out, err := os.Create(part)
	if err != nil {
		t.Fatal(err)
	}
	data := make([]byte, 3*stagingBlock)
	tee := &cacheTee{r: bytes.NewReader(data), out: out, part: part, allow: stagingBlock}
	buf := make([]byte, 1024)
	read := 0
	for tee.out != nil {
		n, err := tee.Read(buf)
		read += n
		if err != nil {
			break
		}
	}
	if tee.out != nil || read > stagingBlock+len(buf) {
		t.Fatalf("tee kept writing: read %d, out %v", read, tee.out)
	}
	if _, err := os.Stat(part); !os.IsNotExist(err) {
		t.Fatalf("dropped copy left %s: %v", part, err)
	}
}
