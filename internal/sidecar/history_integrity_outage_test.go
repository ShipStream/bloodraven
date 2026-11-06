package sidecar

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/shipstream/bloodraven/internal/pitrarchive"
)

// A manifest outage on a verified session does not stop the D-34 probe: a
// violation that comes and goes during the outage is held as the pending
// marker and persisted once the manifest is readable again (F086).
func TestHistoryIntegrityProbesDuringManifestOutage(t *testing.T) {
	ctx := context.Background()
	inner := newMemStore()
	store := &epStore{memStore: inner, failKey: ManifestKey("binlogs", "iad")}
	cfg := promotionFixture(t)
	cfg.StateDir = t.TempDir()
	role := &stubRoleChecker{serverUUID: hintSIDA, serverID: 102, gtidExecuted: hintSIDA + ":1-3"}
	a := newTestArchiver(cfg, "iad", role, store, slog.Default())
	at := time.Date(2026, 11, 4, 9, 0, 0, 0, time.UTC)
	a.now = fixedClock(at)
	a.scanAndArchive(ctx)
	if st := a.Snapshot(ctx); st.LastError != "" || st.FilesArchived != 1 {
		t.Fatalf("clean scan: %+v", st)
	}

	store.setFailGets(true)
	role.mu.Lock()
	role.violation, role.detail = pitrarchive.ViolationBinlogRowImage, "binlog_row_image=MINIMAL"
	role.mu.Unlock()
	a.scanAndArchive(ctx)
	st := a.Snapshot(ctx)
	if !strings.HasPrefix(st.LastError, "PITRArchiveUnsafeHistory: iad BinlogRowImage") || !strings.Contains(st.LastError, "load manifest") {
		t.Fatalf("LastError = %q, want the violation first and the manifest error", st.LastError)
	}
	if _, err := os.Stat(filepath.Join(cfg.StateDir, pendingMarkerFile)); err != nil {
		t.Fatalf("violation seen during the outage not kept: %v", err)
	}

	// The setting is reverted before storage recovers.
	role.mu.Lock()
	role.violation, role.detail = "", ""
	role.mu.Unlock()
	store.setFailGets(false)
	a.scanAndArchive(ctx)
	m, err := loadManifest(ctx, inner, "binlogs", "iad")
	if err != nil {
		t.Fatal(err)
	}
	if m.HistoryIntegrity == nil || m.HistoryIntegrity.Violation != pitrarchive.ViolationBinlogRowImage || !m.HistoryIntegrity.At.Equal(at) {
		t.Fatalf("manifest marker = %+v", m.HistoryIntegrity)
	}
}

// A manifest-version refusal keeps its token first in LastError through the
// history-integrity and upload-path wrapping (F087).
func TestHistoryIntegrityKeepsManifestRefusalToken(t *testing.T) {
	ctx := context.Background()
	store := newMemStore()
	if err := seedTestDescriptor(ctx, store, "binlogs"); err != nil {
		t.Fatal(err)
	}
	body := fmt.Sprintf(`{"version":3,"site":"iad","archiveId":%q}`, testArchiveID)
	if err := store.Put(ctx, ManifestKey("binlogs", "iad"), strings.NewReader(body), int64(len(body))); err != nil {
		t.Fatal(err)
	}
	a := newTestArchiver(promotionFixture(t), "iad", &stubRoleChecker{}, store, slog.Default())
	for i := 0; i < 2; i++ { // the first scan verifies the session, the second starts verified
		a.scanAndArchive(ctx)
		st := a.Snapshot(ctx)
		if tok, ok := pitrarchive.ParseToken(st.LastError); !ok || tok != pitrarchive.TokenArchiveVersionUnsupported {
			t.Fatalf("scan %d LastError = %q, want a leading %s", i, st.LastError, pitrarchive.TokenArchiveVersionUnsupported)
		}
		if st.FilesArchived != 0 {
			t.Fatalf("uploaded under a refused manifest: %+v", st)
		}
	}
}

func TestWrapKeepToken(t *testing.T) {
	base := fmt.Errorf("%w: manifest m has version 3", ErrManifestVersionUnsupported)
	tests := []struct {
		name string
		err  error
		want string
	}{
		{"token first", base, "PITRArchiveVersionUnsupported: history integrity: manifest m has version 3"},
		{"bare token", ErrManifestLegacy, "PITRArchiveLegacy: history integrity"},
		{"no token", errors.New("503 SlowDown"), "history integrity: 503 SlowDown"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := wrapKeepToken("history integrity", tt.err)
			if got.Error() != tt.want || !errors.Is(got, tt.err) {
				t.Fatalf("= %q (Is=%t), want %q", got, errors.Is(got, tt.err), tt.want)
			}
		})
	}
	joined := joinKeepToken(errors.New("history integrity: probe failed"), base)
	if tok, ok := pitrarchive.ParseToken(joined.Error()); !ok || tok != pitrarchive.TokenArchiveVersionUnsupported || !errors.Is(joined, ErrManifestVersionUnsupported) {
		t.Fatalf("joined = %q", joined)
	}
}

// A pending marker whose state-file write failed is written on a later
// scan, so a container restart after the setting is cleared still knows
// the violation (F088).
func TestHistoryIntegrityPendingMarkerWriteRetried(t *testing.T) {
	ctx := context.Background()
	store := newMemStore()
	if os.Geteuid() == 0 {
		t.Skip("root ignores directory permissions")
	}
	stateDir := t.TempDir()
	if err := os.Chmod(stateDir, 0o500); err != nil { // the marker file cannot be created
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.Chmod(stateDir, 0o700) })
	role := &stubRoleChecker{readOnly: true, violation: pitrarchive.ViolationCommitOrder, detail: "replica_preserve_commit_order=0"}
	cfg := withTestArchive(&PITRConfig{ManifestPrefix: "binlogs", BinlogDir: t.TempDir(), BinlogIndex: "mysql-bin.index", StateDir: stateDir}, store)
	at := time.Date(2026, 11, 4, 9, 0, 0, 0, time.UTC)
	a := NewBinlogArchiver(cfg, "pdx", role, store, slog.Default())
	a.now = fixedClock(at)
	a.scanAndArchive(ctx)
	if _, err := os.Stat(filepath.Join(stateDir, pendingMarkerFile)); err == nil {
		t.Fatal("marker written into a read-only state dir")
	}

	if err := os.Chmod(stateDir, 0o700); err != nil {
		t.Fatal(err)
	}
	a.scanAndArchive(ctx)
	if _, err := os.Stat(filepath.Join(stateDir, pendingMarkerFile)); err != nil {
		t.Fatalf("pending marker write not retried: %v", err)
	}

	// The setting is cleared and the container restarts: the violation
	// survives.
	role.mu.Lock()
	role.violation, role.detail = "", ""
	role.mu.Unlock()
	restarted := NewBinlogArchiver(cfg, "pdx", role, store, slog.Default())
	restarted.scanAndArchive(ctx)
	if st := restarted.Snapshot(ctx); st.HistoryIntegrity == nil || !st.HistoryIntegrity.At.Equal(at) {
		t.Fatalf("restarted marker = %+v", st.HistoryIntegrity)
	}
}
