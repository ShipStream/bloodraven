package sidecar

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"log/slog"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/shipstream/bloodraven/internal/pitrarchive"
)

// hookStore injects failures per operation and key around a real store
// and counts calls. A nil hook passes through.
type hookStore struct {
	ArchiveStore
	mu     sync.Mutex
	open   func(key string) error // before Open/Get
	list   func(prefix string) error
	opens  map[string]int
	lists  int
	putIfA func(key string) error
}

func newHookStore(inner ArchiveStore) *hookStore {
	return &hookStore{ArchiveStore: inner, opens: map[string]int{}}
}

func (s *hookStore) before(key string) error {
	s.mu.Lock()
	s.opens[key]++
	hook := s.open
	s.mu.Unlock()
	if hook != nil {
		return hook(key)
	}
	return nil
}

func (s *hookStore) Open(ctx context.Context, key string) (io.ReadCloser, error) {
	if err := s.before(key); err != nil {
		return nil, err
	}
	return s.ArchiveStore.Open(ctx, key)
}

func (s *hookStore) Get(ctx context.Context, key string) ([]byte, bool, error) {
	if err := s.before(key); err != nil {
		return nil, false, err
	}
	return s.ArchiveStore.Get(ctx, key)
}

func (s *hookStore) List(ctx context.Context, prefix string) ([]string, error) {
	s.mu.Lock()
	s.lists++
	hook := s.list
	s.mu.Unlock()
	if hook != nil {
		if err := hook(prefix); err != nil {
			return nil, err
		}
	}
	return s.ArchiveStore.List(ctx, prefix)
}

func (s *hookStore) PutIfAbsent(ctx context.Context, key string, r io.Reader, size int64) error {
	if s.putIfA != nil {
		if err := s.putIfA(key); err != nil {
			return err
		}
	}
	return PutIfAbsent(ctx, s.ArchiveStore, key, r, size)
}

func (s *hookStore) setOpen(hook func(key string) error) {
	s.mu.Lock()
	s.open = hook
	s.mu.Unlock()
}

func (s *hookStore) opened(key string) int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.opens[key]
}

var errTransport = errors.New("s3 get: 503 SlowDown")

// F072/F198: a manifest that cannot be read is a store failure, never
// legacy content; a manifest whose bytes are not a v2 manifest is.
func TestPrefixClassificationSeparatesTransportFromContent(t *testing.T) {
	ctx := context.Background()
	seedV2 := func(s ArchiveStore) {
		if err := saveManifest(ctx, s, "binlogs", &Manifest{Site: "iad", ArchiveID: testArchiveID}); err != nil {
			t.Fatal(err)
		}
	}
	readerExpect := pitrarchive.Expectation{Role: pitrarchive.RoleReader, Destination: testArchiveDestination,
		Encryption: pitrarchive.EncryptionIdentity{Mode: pitrarchive.EncryptionNone}}

	// Transport failure on the only (v2) manifest.
	inner := newMemStore()
	seedV2(inner)
	store := newHookStore(inner)
	store.setOpen(func(key string) error {
		if strings.HasSuffix(key, "manifest-iad.json") {
			return errTransport
		}
		return nil
	})
	s, err := OpenArchiveForWrite(ctx, store, "binlogs", testArchiveIdentity(store), "iad", nil)
	if _, refused := pitrarchive.TokenOf(err); refused || !errors.Is(err, errTransport) {
		t.Fatalf("writer open = %v, want the transport error unclassified", err)
	}
	if !s.transientFailure() || s.Status().State == pitrarchive.StateRefused {
		t.Fatalf("writer status = %+v transient=%t", s.Status(), s.transientFailure())
	}
	_, err = OpenArchiveForRead(ctx, store, "binlogs", readerExpect)
	if _, refused := pitrarchive.TokenOf(err); refused || !errors.Is(err, errTransport) {
		t.Fatalf("reader open = %v, want the transport error unclassified", err)
	}

	// Content failures stay legacy content.
	for name, body := range map[string]string{
		"undecodable": `{`,
		"version 1":   `{"version":1,"site":"iad"}`,
		"newer":       `{"version":9,"site":"iad"}`,
	} {
		m := newMemStore()
		_ = m.Put(ctx, "binlogs/manifest-iad.json", strings.NewReader(body), int64(len(body)))
		if _, err := OpenArchiveForRead(ctx, m, "binlogs", readerExpect); !isToken(err, pitrarchive.TokenArchiveLegacy) {
			t.Errorf("%s: reader = %v, want %s", name, err, pitrarchive.TokenArchiveLegacy)
		}
	}
}

// F072: with encryption, a manifest sealed with another key is content
// (legacy), but a read error while decrypting is transport.
func TestEncryptedClassificationSeparatesKeyFromTransport(t *testing.T) {
	ctx := context.Background()
	key := []byte("an archive passphrase of thirty-two bytes or more")
	other := []byte("a different passphrase, also long enough to be used")
	inner := newMemStore()
	if err := saveManifest(ctx, WrapWithEncryption(inner, other), "binlogs", &Manifest{Site: "iad", ArchiveID: testArchiveID}); err != nil {
		t.Fatal(err)
	}
	expect := pitrarchive.Expectation{Role: pitrarchive.RoleReader, Destination: testArchiveDestination,
		Encryption: pitrarchive.EncryptionIdentity{Mode: pitrarchive.EncryptionPassphrase, Algorithm: pitrarchive.AlgorithmAES256GCM}}
	if _, err := OpenArchiveForRead(ctx, WrapWithEncryption(inner, key), "binlogs", expect); !isToken(err, pitrarchive.TokenArchiveLegacy) {
		t.Fatalf("wrong-key manifest = %v, want %s", err, pitrarchive.TokenArchiveLegacy)
	}

	// The right key, but the body read fails half way.
	good := newMemStore()
	if err := saveManifest(ctx, WrapWithEncryption(good, key), "binlogs", &Manifest{Site: "iad", ArchiveID: testArchiveID}); err != nil {
		t.Fatal(err)
	}
	broken := &truncatingStore{ArchiveStore: good, key: "binlogs/manifest-iad.json"}
	_, err := OpenArchiveForRead(ctx, WrapWithEncryption(broken, key), "binlogs", expect)
	if _, refused := pitrarchive.TokenOf(err); refused || !errors.Is(err, errTransport) {
		t.Fatalf("mid-read failure = %v, want the transport error", err)
	}
	if errors.Is(err, errObjectUndecryptable) {
		t.Fatalf("mid-read failure classified as undecryptable: %v", err)
	}
}

// truncatingStore fails reads of key with errTransport after 8 bytes.
type truncatingStore struct {
	ArchiveStore
	key string
}

func (s *truncatingStore) Open(ctx context.Context, key string) (io.ReadCloser, error) {
	r, err := s.ArchiveStore.Open(ctx, key)
	if err != nil || key != s.key {
		return r, err
	}
	return io.NopCloser(io.MultiReader(io.LimitReader(r, 8), transportErrReader{errTransport})), nil
}

type transportErrReader struct{ err error }

func (r transportErrReader) Read([]byte) (int, error) { return 0, r.err }

func isToken(err error, want pitrarchive.Token) bool {
	got, ok := pitrarchive.TokenOf(err)
	return ok && got == want
}

// F073: the listing remedy follows the cause, and a reader keeps
// cancellation as cancellation.
func TestPrefixListRemedyByCause(t *testing.T) {
	ctx := context.Background()
	expect := pitrarchive.Expectation{Role: pitrarchive.RoleReader, Destination: testArchiveDestination,
		Encryption: pitrarchive.EncryptionIdentity{Mode: pitrarchive.EncryptionNone}}
	for name, tc := range map[string]struct {
		err        error
		want, deny string
	}{
		"s3 denied":   {err: forbiddenError{}, want: "s3:ListBucket"},
		"pvc denied":  {err: &fs.PathError{Op: "open", Path: "/run/archive/binlogs", Err: fs.ErrPermission}, want: "readable by the sidecar", deny: "s3:ListBucket"},
		"unavailable": {err: errTransport, want: "reachable", deny: "s3:ListBucket"},
	} {
		store := newHookStore(newMemStore())
		store.list = func(string) error { return tc.err }
		_, err := OpenArchiveForRead(ctx, store, "binlogs", expect)
		if !isToken(err, pitrarchive.TokenArchiveListFailed) || !strings.Contains(err.Error(), tc.want) ||
			(tc.deny != "" && strings.Contains(err.Error(), tc.deny)) {
			t.Errorf("%s: %v, want a remedy mentioning %q", name, err, tc.want)
		}
		if strings.Count(err.Error(), tc.want) != 1 {
			t.Errorf("%s: remedy repeated: %v", name, err)
		}
		w, _ := OpenArchiveForWrite(ctx, store, "binlogs", testArchiveIdentity(store), "iad", nil)
		if werr := w.Err(); werr == nil || strings.Count(werr.Error(), tc.want) != 1 {
			t.Errorf("%s: writer error %v, want one %q", name, werr, tc.want)
		}
	}
	cctx, cancel := context.WithCancel(ctx)
	cancel()
	store := newHookStore(newMemStore())
	store.list = func(string) error { return context.Canceled }
	if _, err := OpenArchiveForRead(cctx, store, "binlogs", expect); !errors.Is(err, context.Canceled) || isToken(err, pitrarchive.TokenArchiveListFailed) {
		t.Fatalf("cancelled reader = %v, want context.Canceled", err)
	}
}

// F194: a writer that cannot build its descriptor reports a configuration
// error, not a store failure, and probes nothing.
func TestEnsureCreatedIncompleteIdentityIsConfiguration(t *testing.T) {
	ctx := context.Background()
	store := newHookStore(newMemStore())
	probed := false
	store.putIfA = func(string) error { probed = true; return nil }
	id := testArchiveIdentity(store)
	id.Source.Name = "" // FAILOVER_GROUP unset
	s, err := newArchiveSession(store, "binlogs", id, "iad", nil)
	if err != nil {
		t.Fatal(err)
	}
	err = s.EnsureCreated(ctx)
	if err == nil || !strings.Contains(err.Error(), "archive identity") || !strings.Contains(err.Error(), "FAILOVER_GROUP") {
		t.Fatalf("EnsureCreated = %v", err)
	}
	if s.transientFailure() || probed {
		t.Fatalf("transient=%t probed=%t, want a configuration error before any probe", s.transientFailure(), probed)
	}

	cfg := &PITRConfig{ManifestPrefix: "binlogs", BinlogDir: t.TempDir(), BinlogIndex: "mysql-bin.index", Archive: id}
	a := NewBinlogArchiver(cfg, "iad", &stubRoleChecker{}, store, slog.Default())
	a.scanAndArchive(ctx)
	st := a.Snapshot(ctx)
	if strings.HasPrefix(st.LastError, "archive store init") || !strings.Contains(st.LastError, "archive identity") {
		t.Fatalf("archiver status = %+v", st)
	}
	// Like an identity rejected at construction, it reports StoreReady=false.
	if st.StoreReady == nil || *st.StoreReady {
		t.Fatalf("StoreReady = %v, want false for an unusable identity", st.StoreReady)
	}
}

// F195: a create whose read-back failed is still logged once, by the
// later Open that first verifies it.
func TestDescriptorCreatedLoggedAfterReadbackFailure(t *testing.T) {
	ctx := context.Background()
	inner := newMemStore()
	store := newHookStore(inner)
	var buf bytes.Buffer
	logger := slog.New(slog.NewJSONHandler(&buf, nil))
	s, err := newArchiveSession(store, "binlogs", testArchiveIdentity(store), "iad", logger)
	if err != nil {
		t.Fatal(err)
	}
	desc := pitrarchive.DescriptorKey("binlogs")
	// Fail only reads after the descriptor exists (the read-back).
	store.setOpen(func(key string) error {
		if _, ok, _ := inner.Get(ctx, desc); key == desc && ok {
			return errTransport
		}
		return nil
	})
	if err := s.EnsureCreated(ctx); !errors.Is(err, errTransport) {
		t.Fatalf("EnsureCreated = %v, want the read-back failure", err)
	}
	if strings.Contains(buf.String(), "archive descriptor created") {
		t.Fatal("created logged before verification")
	}
	store.setOpen(nil)
	if err := s.EnsureCreated(ctx); err != nil {
		t.Fatal(err)
	}
	if err := s.Open(ctx); err != nil {
		t.Fatal(err)
	}
	if n := strings.Count(buf.String(), `"msg":"archive descriptor created"`); n != 1 {
		t.Fatalf("created logged %d times:\n%s", n, buf.String())
	}
	// A sibling's descriptor read by a fresh session is never "created".
	var other bytes.Buffer
	s2, _ := newArchiveSession(store, "binlogs", testArchiveIdentity(store), "pdx", slog.New(slog.NewJSONHandler(&other, nil)))
	if err := s2.EnsureCreated(ctx); err != nil || strings.Contains(other.String(), "archive descriptor created") {
		t.Fatalf("sibling: %v %s", err, other.String())
	}
}

// F199: an oversized archive.json is refused by both paths without being
// buffered.
func TestDescriptorReadIsBounded(t *testing.T) {
	ctx := context.Background()
	store := newMemStore()
	big := bytes.Repeat([]byte(" "), pitrarchive.MaxDescriptorSize+1)
	_ = store.Put(ctx, pitrarchive.DescriptorKey("binlogs"), bytes.NewReader(big), int64(len(big)))
	s, err := OpenArchiveForWrite(ctx, store, "binlogs", testArchiveIdentity(store), "iad", nil)
	if !isToken(err, pitrarchive.TokenArchiveVersionUnsupported) || !strings.Contains(err.Error(), "larger than") || s.Status().State != pitrarchive.StateRefused {
		t.Fatalf("writer = %v", err)
	}
	_, err = OpenArchiveForRead(ctx, store, "binlogs", pitrarchive.Expectation{Role: pitrarchive.RoleReader,
		Destination: testArchiveDestination, Encryption: pitrarchive.EncryptionIdentity{Mode: pitrarchive.EncryptionNone}})
	if !isToken(err, pitrarchive.TokenArchiveVersionUnsupported) || !strings.Contains(err.Error(), " at "+testArchiveDestination.String()) {
		t.Fatalf("reader = %v", err)
	}
}

// F200: Reverify's failure branches block saves and deletes: a transient
// read keeps the verified state but refuses the write; a malformed
// replacement refuses; a creation read-back that differs fails.
func TestReverifyFailureBranches(t *testing.T) {
	ctx := context.Background()
	desc := pitrarchive.DescriptorKey("binlogs")

	inner := newMemStore()
	store := newHookStore(inner)
	s, err := testWriterSession(ctx, store, "binlogs")
	if err != nil {
		t.Fatal(err)
	}
	store.setOpen(func(key string) error {
		if key == desc {
			return errTransport
		}
		return nil
	})
	if err := s.SaveManifest(ctx, &Manifest{Site: "iad"}); !errors.Is(err, errTransport) {
		t.Fatalf("save during a transient descriptor read = %v", err)
	}
	if err := s.DeleteObject(ctx, "binlogs/iad/x"); !errors.Is(err, errTransport) {
		t.Fatalf("delete during a transient descriptor read = %v", err)
	}
	if _, ok, _ := inner.Get(ctx, "binlogs/manifest-iad.json"); ok {
		t.Fatal("a manifest was saved without a verified descriptor read")
	}
	if !s.transientFailure() || s.Status().State != pitrarchive.StateVerified || !s.Verified() {
		t.Fatalf("transient read changed the verified state: %+v", s.Status())
	}
	store.setOpen(nil)
	if err := s.SaveManifest(ctx, &Manifest{Site: "iad"}); err != nil {
		t.Fatalf("save after recovery = %v", err)
	}

	// A malformed replacement refuses and leaves the session Refused.
	_ = inner.Put(ctx, desc, strings.NewReader(`{`), 1)
	if err := s.SaveManifest(ctx, &Manifest{Site: "iad"}); !isToken(err, pitrarchive.TokenArchiveVersionUnsupported) {
		t.Fatalf("save over a malformed descriptor = %v", err)
	}
	if s.Verified() || s.Status().State != pitrarchive.StateRefused {
		t.Fatalf("status = %+v", s.Status())
	}

	// A create whose read-back returns other bytes fails without verifying.
	inner2 := newMemStore()
	swap := newHookStore(inner2)
	swap.putIfA = func(key string) error {
		if key != desc {
			return nil
		}
		// Another writer's bytes land first under our feet: simulate a
		// store that acknowledged our create but serves something else.
		swap.setOpen(func(k string) error {
			if k == desc {
				d, _ := pitrarchive.NewDescriptor(testArchiveSource, testArchiveDestination, pitrarchive.EncryptionIdentity{Mode: pitrarchive.EncryptionNone}, nil, "elsewhere", time.Unix(1, 0))
				b, _ := d.Canonical()
				_ = inner2.Put(ctx, desc, bytes.NewReader(b), int64(len(b)))
			}
			return nil
		})
		return nil
	}
	s3, _ := newArchiveSession(swap, "binlogs", testArchiveIdentity(swap), "iad", nil)
	if err := s3.EnsureCreated(ctx); err == nil || !strings.Contains(err.Error(), "read back different bytes") || s3.Verified() {
		t.Fatalf("EnsureCreated with a differing read-back = %v", err)
	}
}

// F196/F197: a stale manifest copy cannot resurrect an evicted record, an
// explicit new append is kept even after a backward clock step, and the
// eviction bookkeeping stays bounded.
func TestManifestGuardEvictionStableAndBounded(t *testing.T) {
	ctx := context.Background()
	store := newMemStore()
	s, err := testWriterSession(ctx, store, "binlogs")
	if err != nil {
		t.Fatal(err)
	}
	base := time.Date(2026, 11, 1, 0, 0, 0, 0, time.UTC)
	rec := func(i int) PromotionRecord {
		return PromotionRecord{ServerUUID: "u", ServerID: 1, GTIDExecuted: fmt.Sprintf("u:1-%d", i+1), ObservedAt: base.Add(time.Duration(i) * time.Second)}
	}
	m, err := s.LoadManifest(ctx, "iad")
	if err != nil {
		t.Fatal(err)
	}
	stale := *m // holds no records
	stale.Promotions = []PromotionRecord{rec(0)}
	for i := range 2*MaxPromotionRecords + 10 {
		m.AppendPromotionRecord(rec(i))
		if err := s.SaveManifest(ctx, m); err != nil {
			t.Fatalf("save %d: %v", i, err)
		}
	}
	g := s.guards["iad"]
	if len(g.evicted) > 1 || len(g.promotions) != MaxPromotionRecords {
		t.Fatalf("guard holds %d evicted keys and %d records", len(g.evicted), len(g.promotions))
	}

	// A backward clock step: the new record is the oldest, but it is the
	// explicit append, so it is kept.
	backstep := PromotionRecord{ServerUUID: "v", ServerID: 2, GTIDExecuted: "v:1", ObservedAt: base.Add(-time.Hour)}
	m.AppendPromotionRecord(backstep)
	if err := s.SaveManifest(ctx, m); err != nil {
		t.Fatal(err)
	}
	if !hasPromotionRecord(m.Promotions, backstep) {
		t.Fatal("the back-stepped append was dropped")
	}
	saved := append([]PromotionRecord(nil), m.Promotions...)

	// A stale copy whose last record was evicted long ago: it neither
	// comes back nor evicts a retained record.
	if err := s.SaveManifest(ctx, &stale); err != nil {
		t.Fatal(err)
	}
	got, err := loadManifest(ctx, store, "binlogs", "iad")
	if err != nil {
		t.Fatal(err)
	}
	if hasPromotionRecord(got.Promotions, rec(0)) {
		t.Fatal("a stale save resurrected an evicted record")
	}
	if len(got.Promotions) != len(saved) {
		t.Fatalf("stale save changed the record count %d -> %d", len(saved), len(got.Promotions))
	}
	for _, r := range saved {
		if !hasPromotionRecord(got.Promotions, r) {
			t.Fatalf("stale save evicted retained record %+v", r)
		}
	}
}
