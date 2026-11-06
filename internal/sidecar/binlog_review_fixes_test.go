package sidecar

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"

	"github.com/aws/smithy-go"

	"github.com/shipstream/bloodraven/internal/pitrarchive"
)

// F076: while a history-integrity marker exists, a deferred maintenance
// failure is appended to the unsafe-history LastError, never put in its
// place.
func TestMaintenanceErrorKeepsUnsafeHistoryPrimary(t *testing.T) {
	ctx := context.Background()
	store := newMemStore()
	cfg := promotionFixture(t)
	if err := seedManifest(ctx, store, cfg.ManifestPrefix, &Manifest{Site: "iad", Epoch: &ArchiveEpoch{ResetReason: "verified GTID namespace reuse; use a new manifest prefix and full backup"}}); err != nil {
		t.Fatal(err)
	}
	role := &stubRoleChecker{violation: pitrarchive.ViolationBinlogRowImage, detail: "binlog_row_image=MINIMAL"}
	a := newTestArchiver(cfg, "iad", role, store, slog.Default())
	a.scanAndArchive(ctx)
	st := a.Snapshot(ctx)
	if !strings.HasPrefix(st.LastError, string(pitrarchive.TokenArchiveUnsafeHistory)+": iad BinlogRowImage") {
		t.Fatalf("LastError = %q, want the unsafe-history refusal first", st.LastError)
	}
	if !strings.Contains(st.LastError, "also: site iad: verified GTID namespace reuse") {
		t.Fatalf("LastError = %q, want the audit failure appended", st.LastError)
	}
}

// F203: a failed establishment is not followed by a second prefix
// classification in the same scan.
func TestFailedEstablishmentIsNotReclassifiedByMaintenance(t *testing.T) {
	ctx := context.Background()
	inner := newMemStore()
	body := `{"version":1,"site":"iad","files":[]}`
	_ = inner.Put(ctx, "binlogs/manifest-iad.json", strings.NewReader(body), int64(len(body)))
	store := newHookStore(inner)
	a := NewBinlogArchiver(withTestArchive(&PITRConfig{ManifestPrefix: "binlogs", BinlogDir: t.TempDir(), BinlogIndex: "mysql-bin.index"}, store), "iad", &stubRoleChecker{}, store, slog.Default())
	a.scanAndArchive(ctx)
	if st := a.Snapshot(ctx); !strings.HasPrefix(st.LastError, string(pitrarchive.TokenArchiveLegacy)) {
		t.Fatalf("LastError = %q", st.LastError)
	}
	if store.lists != 1 || store.opened("binlogs/manifest-iad.json") != 1 {
		t.Fatalf("one scan listed the prefix %d times and read the manifest %d times, want 1 and 1",
			store.lists, store.opened("binlogs/manifest-iad.json"))
	}
	// A replica awaiting the descriptor classifies once per scan too.
	empty := newHookStore(newMemStore())
	r := NewBinlogArchiver(withTestArchive(&PITRConfig{ManifestPrefix: "binlogs", BinlogDir: t.TempDir(), BinlogIndex: "mysql-bin.index"}, empty), "pdx", &stubRoleChecker{readOnly: true}, empty, slog.Default())
	r.scanAndArchive(ctx)
	if empty.lists != 1 {
		t.Fatalf("an awaiting replica listed %d times in one scan", empty.lists)
	}
}

// F077: a Put that created a directory but failed to sync its parent
// leaves the entry undurable; the retry syncs it before reporting success.
func TestPVCRetrySyncsAncestorAfterFailedSync(t *testing.T) {
	ctx := context.Background()
	root := t.TempDir()
	boom := errors.New("sync failed")
	var mu sync.Mutex
	var synced []string
	failRoot := true
	p := &pvcStore{root: root, dirSync: func(dir string) error {
		mu.Lock()
		defer mu.Unlock()
		rel, _ := filepath.Rel(root, dir)
		if rel == "." && failRoot {
			failRoot = false
			return boom
		}
		synced = append(synced, filepath.ToSlash(rel))
		return nil
	}}
	if err := p.Put(ctx, "binlogs/iad/mysql-bin.000001", strings.NewReader("x"), 1); !errors.Is(err, boom) {
		t.Fatalf("first Put = %v, want the sync failure", err)
	}
	if err := p.Put(ctx, "binlogs/iad/mysql-bin.000002", strings.NewReader("y"), 1); err != nil {
		t.Fatal(err)
	}
	for _, want := range []string{".", "binlogs", "binlogs/iad"} {
		found := false
		for _, s := range synced {
			found = found || s == want
		}
		if !found {
			t.Fatalf("directory %q never synced after the retry; synced %v", want, synced)
		}
	}
	// Once durable, a chain is not synced again.
	synced = nil
	if err := p.Put(ctx, "binlogs/iad/mysql-bin.000003", strings.NewReader("z"), 1); err != nil {
		t.Fatal(err)
	}
	if len(synced) != 1 || synced[0] != "binlogs/iad" {
		t.Fatalf("steady-state Put synced %v, want only its own directory", synced)
	}
}

// F205: ENOSYS from link(2) means the filesystem cannot hard-link; EMLINK
// does not.
func TestMapLinkErrorUnsupportedErrnos(t *testing.T) {
	for errno, unsupported := range map[syscall.Errno]bool{
		syscall.ENOSYS: true, syscall.EPERM: true, syscall.EXDEV: true, syscall.ENOTSUP: true,
		syscall.EMLINK: false, syscall.EIO: false,
	} {
		err := mapLinkError(&os.LinkError{Op: "link", Old: "a", New: "b", Err: errno})
		if got := errors.Is(err, ErrConditionalWriteUnsupported); got != unsupported {
			t.Errorf("%v: unsupported = %t, want %t (%v)", errno, got, unsupported, err)
		}
	}
	if err := mapLinkError(&os.LinkError{Err: syscall.EEXIST}); !errors.Is(err, ErrObjectExists) {
		t.Errorf("EEXIST = %v", err)
	}
}

// publishThenFailStore publishes the first atomic create and then reports
// a failure, as a PVC link followed by a failed directory sync does.
type publishThenFailStore struct {
	*memStore
	failed bool
}

func (s *publishThenFailStore) PutIfAbsent(ctx context.Context, key string, r io.Reader, size int64) error {
	if err := s.memStore.PutIfAbsent(ctx, key, r, size); err != nil {
		return err
	}
	if !s.failed {
		s.failed = true
		return errors.New("sync binlogs: input/output error")
	}
	return nil
}

// blockingDeleteStore's Delete waits for its context.
type blockingDeleteStore struct {
	*memStore
	hadDeadline bool
}

func (s *blockingDeleteStore) Delete(ctx context.Context, _ string) error {
	_, s.hadDeadline = ctx.Deadline()
	<-ctx.Done()
	return ctx.Err()
}

// F206: the probe key is cleaned up when the first create published it and
// then failed, and the cleanup cannot block without a deadline.
func TestProbeAtomicCreateCleanup(t *testing.T) {
	ctx := context.Background()
	store := &publishThenFailStore{memStore: newMemStore()}
	if err := ProbeAtomicCreate(ctx, store, "binlogs"); err == nil {
		t.Fatal("probe succeeded after a failed create")
	}
	keys, _ := store.List(ctx, "binlogs/")
	for _, k := range keys {
		if strings.Contains(k, pitrarchive.CASProbeKeyPrefix) {
			t.Fatalf("probe key %s left behind", k)
		}
	}

	old := probeCleanupTimeout
	probeCleanupTimeout = 50 * time.Millisecond
	defer func() { probeCleanupTimeout = old }()
	blocking := &blockingDeleteStore{memStore: newMemStore()}
	done := make(chan error, 1)
	go func() { done <- ProbeAtomicCreate(ctx, blocking, "binlogs") }()
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("probe = %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("a stalled probe-key delete blocked the probe")
	}
	if !blocking.hadDeadline {
		t.Fatal("the probe-key delete ran without a deadline")
	}
}

// putCountingStore counts Puts per key on top of openErrStore.
type putCountingStore struct {
	*openErrStore
	mu   sync.Mutex
	puts map[string]int
}

func (s *putCountingStore) Put(ctx context.Context, key string, r io.Reader, size int64) error {
	s.mu.Lock()
	s.puts[key]++
	s.mu.Unlock()
	return s.openErrStore.Put(ctx, key, r, size)
}

func (s *putCountingStore) PutFile(ctx context.Context, key, path string) error {
	f, err := os.Open(path)
	if err != nil {
		return err
	}
	defer f.Close()
	return s.Put(ctx, key, f, -1)
}

// archiveOverQuarantine archives incarnation-a, quarantines it, and returns
// the archiver and the keys of the quarantined and the overlapping
// (same-header) objects.
func archiveOverQuarantine(t *testing.T, store *putCountingStore) (*BinlogArchiver, string, string, string) {
	t.Helper()
	ctx := context.Background()
	a := newTestArchiver(&PITRConfig{ManifestPrefix: "binlogs"}, "iad", &stubRoleChecker{}, store, slog.Default())
	first := filepath.Join("testdata", "incarnation-a.bin")
	firstID, err := a.localBinlogID(first)
	if err != nil {
		t.Fatal(err)
	}
	if err := a.archiveOne(ctx, first, "mysql-bin.000001", firstID); err != nil {
		t.Fatal(err)
	}
	if err := testQuarantine(ctx, store.memStore, "binlogs", "iad", "mysql-bin.000001", firstID, "lost at failover", coverageTestSID+":1-10", false); err != nil {
		t.Fatal(err)
	}
	secondID, err := a.localBinlogID(filepath.Join("testdata", "same-header.bin"))
	if err != nil {
		t.Fatal(err)
	}
	quarantinedKey, _ := ArchiveEntryKey("binlogs", "iad", ManifestEntry{Name: "mysql-bin.000001", ID: firstID})
	newKey, _ := ArchiveEntryKey("binlogs", "iad", ManifestEntry{Name: "mysql-bin.000002", ID: secondID})
	return a, quarantinedKey, newKey, secondID
}

// F204: only the quarantined prior's absence is waived; the new object
// vanishing before its issuance comparison fails the publication.
func TestReuseWaiverDoesNotCoverMissingNewObject(t *testing.T) {
	ctx := context.Background()
	store := &putCountingStore{openErrStore: &openErrStore{memStore: newMemStore()}, puts: map[string]int{}}
	a, _, newKey, secondID := archiveOverQuarantine(t, store)
	store.key, store.err = newKey, os.ErrNotExist
	if err := a.archiveOne(ctx, filepath.Join("testdata", "same-header.bin"), "mysql-bin.000002", secondID); err == nil {
		t.Fatal("a row was published although its own object could not be read for the issuance comparison")
	}
	m, err := loadManifest(ctx, store.memStore, "binlogs", "iad")
	if err != nil {
		t.Fatal(err)
	}
	for _, row := range m.Files {
		if row.Name == "mysql-bin.000002" {
			t.Fatalf("row published: %+v", row)
		}
	}
	if issuanceOperandMissing(&issuanceReadError{first: false, err: os.ErrNotExist}, true) ||
		!issuanceOperandMissing(&issuanceReadError{first: true, err: &smithy.GenericAPIError{Code: "NoSuchKey"}}, true) {
		t.Fatal("issuanceOperandMissing does not tell the operands apart")
	}
}

// F213: an uploaded object whose publication failed (an unreadable
// quarantined overlap) is not uploaded again on retry; the publication is
// retried and still refuses.
func TestUnpublishedUploadIsNotReuploaded(t *testing.T) {
	ctx := context.Background()
	store := &putCountingStore{openErrStore: &openErrStore{memStore: newMemStore()}, puts: map[string]int{}}
	a, quarantinedKey, newKey, secondID := archiveOverQuarantine(t, store)
	store.key, store.err = quarantinedKey, &smithy.GenericAPIError{Code: "AccessDenied", Message: "Access Denied"}
	path := filepath.Join("testdata", "same-header.bin")
	for i := range 3 {
		if err := a.archiveOne(ctx, path, "mysql-bin.000002", secondID); err == nil {
			t.Fatalf("attempt %d published over an unreadable quarantined overlap", i)
		}
	}
	if n := store.puts[newKey]; n != 1 {
		t.Fatalf("the candidate was uploaded %d times, want once", n)
	}
	// Once the quarantined object is readable again, the retry publishes
	// the already uploaded object.
	store.key, store.err = "", nil
	if err := a.archiveOne(ctx, path, "mysql-bin.000002", secondID); err != nil {
		t.Fatal(err)
	}
	if n := store.puts[newKey]; n != 1 {
		t.Fatalf("the candidate was uploaded %d times after recovery, want once", n)
	}
	m, err := loadManifest(ctx, store.memStore, "binlogs", "iad")
	if err != nil {
		t.Fatal(err)
	}
	if m.Epoch == nil || len(m.Epoch.Reuse) != 1 {
		t.Fatalf("publication after recovery did not verify reuse: %+v", m.Epoch)
	}
}
