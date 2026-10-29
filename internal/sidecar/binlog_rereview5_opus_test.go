package sidecar

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// Preserve the failure throughout cooldown and report the site-wide deferral.
// Quarantine is the explicit recovery path; never silently discard evidence.
func TestRR5CorruptLegacyRowDoesNotHaltAllRetention(t *testing.T) {
	ctx := context.Background()
	store := newMemStore()
	good, err := os.ReadFile("testdata/incarnation-a.bin")
	if err != nil {
		t.Fatal(err)
	}
	e, err := InspectBinlog(bytes.NewReader(good), time.Time{})
	if err != nil {
		t.Fatal(err)
	}
	cov := e.Metadata.GTIDCoverage
	cov.ContentID = e.Digest
	covered := ManifestEntry{Name: "mysql-bin.000002", ID: e.Digest, PreviousGTIDs: e.Metadata.PreviousGTIDs, FirstEventTime: e.Metadata.FirstEventTime, LastEventTime: e.Metadata.LastEventTime, Size: int64(len(good)), GTIDCoverage: cov}
	corrupt := ManifestEntry{Name: "mysql-bin.000001", LastEventTime: e.Metadata.LastEventTime.Add(-time.Hour), Size: 200}
	for _, item := range []struct {
		row  ManifestEntry
		data []byte
	}{{corrupt, good[:200]}, {covered, good}} {
		key, err := ArchiveEntryKey("binlogs", "iad", item.row)
		if err != nil {
			t.Fatal(err)
		}
		if err := store.Put(ctx, key, bytes.NewReader(item.data), int64(len(item.data))); err != nil {
			t.Fatal(err)
		}
	}
	if err := saveManifest(ctx, store, "binlogs", &Manifest{Site: "iad", Files: []ManifestEntry{corrupt, covered}}); err != nil {
		t.Fatal(err)
	}
	var logs bytes.Buffer
	a := retentionReviewArchiver(t, store, slog.New(slog.NewJSONHandler(&logs, nil)), retentionReviewProof())
	defer func() {
		if a.reuseWork != nil {
			a.reuseWork.close()
		}
	}()
	var errs []string
	for i := 0; i < 6; i++ {
		a.scanAndArchive(ctx)
		if a.reuseWork != nil {
			<-a.reuseWork.done
		}
		errs = append(errs, a.Snapshot(ctx).LastError)
		a.mu.Lock()
		a.lastRetentionRun = time.Time{}
		a.mu.Unlock()
	}
	m, err := loadManifest(ctx, store, "binlogs", "iad")
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("lastError per scan: %q", errs)
	t.Logf("files=%d pruned=%d", len(m.Files), len(m.Pruned))
	for _, text := range errs {
		if !strings.Contains(text, "verify archived GTID issuance") || !strings.Contains(text, "pitr-quarantine") {
			t.Fatalf("issuance failure disappeared during cooldown: %q", errs)
		}
	}
	if !strings.Contains(logs.String(), `"level":"WARN","msg":"retention deferred"`) || !strings.Contains(logs.String(), `"reason":"archiveIssuanceUnverified"`) {
		t.Fatalf("site-wide retention failure was not actionable at WARN: %s", &logs)
	}
	// Losing manifest access cannot establish that the recorded failure resolved.
	outage := &rr5FlakyManifestStore{ArchiveStore: store, outage: make(chan struct{}), excess: make(chan struct{})}
	outage.fail.Store(true)
	a.store = outage
	a.scanAndArchive(ctx)
	if !strings.Contains(a.Snapshot(ctx).LastError, "verify archived GTID issuance") {
		t.Fatal("manifest outage cleared an unresolved history error")
	}
	a.store = store
	if err := QuarantineArchive(ctx, store, "binlogs", "iad", corrupt.Name, "", "unrecoverable legacy stream; canonical backup verified", coverageTestSID+":1-11", false); err != nil {
		t.Fatal(err)
	}
	a.lastRetentionRun = time.Time{}
	a.scanAndArchive(ctx)
	m, err = loadManifest(ctx, store, "binlogs", "iad")
	if err != nil || len(m.Files) != 0 || len(m.Pruned) != 1 || len(m.Quarantined) != 1 {
		t.Fatalf("explicit repair did not resume retention: %+v %v", m, err)
	}
	if strings.Contains(a.Snapshot(ctx).LastError, "verify archived GTID issuance") {
		t.Fatal("resolved issuance error stayed in status")
	}
}

type rr5FlakyManifestStore struct {
	ArchiveStore
	fail   atomic.Bool
	gets   atomic.Int64
	scans  atomic.Int64
	outage chan struct{}
	excess chan struct{}
	once   sync.Once
}

func (s *rr5FlakyManifestStore) Put(ctx context.Context, key string, r io.Reader, n int64) error {
	err := s.ArchiveStore.Put(ctx, key, r, n)
	if key == ManifestKey("binlogs", "iad") {
		// The first reconcile save succeeds; afterwards the backend starts
		// failing (credential expiry, throttling, outage).
		s.fail.Store(true)
	}
	return err
}
func (s *rr5FlakyManifestStore) Get(ctx context.Context, key string) ([]byte, bool, error) {
	if key == ManifestKey("binlogs", "iad") {
		s.gets.Add(1)
		if s.fail.Load() {
			s.once.Do(func() { close(s.outage) })
			return nil, false, errors.New("503 SlowDown")
		}
	}
	return s.ArchiveStore.Get(ctx, key)
}

type rr5CountingRole struct {
	stubRoleChecker
	store *rr5FlakyManifestStore
}

func (r *rr5CountingRole) IsReadOnly(context.Context) (bool, error) {
	if r.store.scans.Add(1) == 6 {
		close(r.store.excess)
	}
	return true, nil
}

// A finished, unharvested reuse job must not turn a backend outage into a
// back-to-back scan loop: reuseReady() stays closed until auditArchive runs,
// and auditArchive cannot run while the manifest is unreadable.
func TestRR5FinishedReuseJobDoesNotSpinOnManifestOutage(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	raw, err := os.ReadFile("testdata/incarnation-a.bin")
	if err != nil {
		t.Fatal(err)
	}
	base := newMemStore()
	store := &rr5FlakyManifestStore{ArchiveStore: base, outage: make(chan struct{}), excess: make(chan struct{})}
	m := &Manifest{Site: "iad"}
	for i := 0; i < 12; i++ {
		row := ManifestEntry{Name: fmt.Sprintf("mysql-bin.%06d", i+1)}
		key, err := ArchiveEntryKey("binlogs", "iad", row)
		if err != nil {
			t.Fatal(err)
		}
		if err := base.Put(ctx, key, bytes.NewReader(raw), int64(len(raw))); err != nil {
			t.Fatal(err)
		}
		m.Files = append(m.Files, row)
	}
	if err := saveManifest(ctx, base, "binlogs", m); err != nil {
		t.Fatal(err)
	}
	cfg := &PITRConfig{ManifestPrefix: "binlogs", BinlogDir: t.TempDir(), BinlogIndex: "mysql-bin.index", PollInterval: time.Hour}
	a := NewBinlogArchiver(cfg, "iad", &rr5CountingRole{store: store}, store, slog.New(slog.NewTextHandler(io.Discard, nil)))
	done := make(chan struct{})
	go func() { defer close(done); a.Run(ctx) }()
	select {
	case <-store.outage:
	case <-time.After(5 * time.Second):
		cancel()
		<-done
		t.Fatal("fixture did not enter the manifest outage")
	}
	select {
	case <-store.excess:
	case <-time.After(100 * time.Millisecond):
	}
	cancel()
	<-done
	t.Logf("scans=%d manifestGets=%d during manifest outage with PollInterval=1h", store.scans.Load(), store.gets.Load())
	if store.scans.Load() > 5 {
		t.Fatalf("archiver spun %d back-to-back scans (%d manifest GETs) during manifest outage against a failing backend", store.scans.Load(), store.gets.Load())
	}
}
