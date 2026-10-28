package sidecar

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"os"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// Preserve the failure throughout cooldown and report the site-wide deferral.
// Quarantine is the explicit recovery path; never silently discard evidence.

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
