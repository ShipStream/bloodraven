package sidecar

import (
	"context"
	"database/sql"
	"errors"
	"log/slog"
	"strings"
	"testing"
	"time"

	"github.com/shipstream/bloodraven/internal/pitrarchive"
)

func TestHistorySettingsViolation(t *testing.T) {
	v := func(s string) sql.NullString { return sql.NullString{String: s, Valid: true} }
	tests := []struct {
		name                       string
		format, rowImage, ordering sql.NullString
		want                       pitrarchive.HistoryViolation
		wantDetail                 string
	}{
		{"clean", v("ROW"), v("FULL"), v("1"), "", ""},
		{"clean lower case", v("row"), v("full"), v("ON"), "", ""},
		{"statement format", v("STATEMENT"), v("FULL"), v("1"), pitrarchive.ViolationBinlogFormat, "binlog_format=STATEMENT"},
		{"mixed format", v("MIXED"), v("FULL"), v("1"), pitrarchive.ViolationBinlogFormat, "binlog_format=MIXED"},
		{"no format variable", sql.NullString{}, v("FULL"), v("1"), pitrarchive.ViolationBinlogFormat, "binlog_format="},
		{"minimal row image", v("ROW"), v("MINIMAL"), v("1"), pitrarchive.ViolationBinlogRowImage, "binlog_row_image=MINIMAL"},
		{"noblob row image", v("ROW"), v("NOBLOB"), v("1"), pitrarchive.ViolationBinlogRowImage, "binlog_row_image=NOBLOB"},
		{"commit order off", v("ROW"), v("FULL"), v("0"), pitrarchive.ViolationCommitOrder, "replica_preserve_commit_order=0"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, detail, err := historySettingsViolation(tt.format, tt.rowImage, tt.ordering)
			if err != nil || got != tt.want || detail != tt.wantDetail {
				t.Fatalf("= %q %q %v, want %q %q", got, detail, err, tt.want, tt.wantDetail)
			}
		})
	}
}

// Each violation persists the marker in the site's own manifest and stops
// uploads (D-34).
func TestHistoryIntegrityMarkerStopsUploads(t *testing.T) {
	for _, violation := range pitrarchive.HistoryViolations {
		t.Run(string(violation), func(t *testing.T) {
			ctx := context.Background()
			store := newMemStore()
			role := &stubRoleChecker{violation: violation, detail: "detail for " + string(violation)}
			at := time.Date(2026, 11, 4, 9, 0, 0, 0, time.UTC)
			a := newTestArchiver(promotionFixture(t), "iad", role, store, slog.Default())
			a.now = fixedClock(at)
			a.scanAndArchive(ctx)
			st := a.Snapshot(ctx)
			wantErr := "PITRArchiveUnsafeHistory: iad " + string(violation) + " detail for " + string(violation) + "; "
			if !strings.HasPrefix(st.LastError, wantErr) || st.FilesArchived != 0 {
				t.Fatalf("status = %+v, want LastError %q and no uploads", st, wantErr)
			}
			if st.HistoryIntegrity == nil || st.HistoryIntegrity.Violation != violation || !st.HistoryIntegrity.At.Equal(at) {
				t.Fatalf("status marker = %+v", st.HistoryIntegrity)
			}
			m, err := loadManifest(ctx, store, "binlogs", "iad")
			if err != nil {
				t.Fatal(err)
			}
			if m.HistoryIntegrity == nil || *m.HistoryIntegrity != *st.HistoryIntegrity {
				t.Fatalf("manifest marker = %+v", m.HistoryIntegrity)
			}
			for key := range store.objects {
				if strings.HasPrefix(key, "binlogs/iad/") {
					t.Fatalf("a marked site uploaded %s", key)
				}
			}
		})
	}
}

// Clearing the setting never clears the marker, across a restart too.
func TestHistoryIntegrityMarkerIsPermanent(t *testing.T) {
	ctx := context.Background()
	store := newMemStore()
	cfg := promotionFixture(t)
	role := &stubRoleChecker{violation: pitrarchive.ViolationReplicationFilter, detail: "performance_schema.replication_applier_filters has 1 rows"}
	a := newTestArchiver(cfg, "iad", role, store, slog.Default())
	a.scanAndArchive(ctx)
	first, _ := loadManifest(ctx, store, "binlogs", "iad")
	if first.HistoryIntegrity == nil {
		t.Fatal("no marker")
	}
	role.mu.Lock()
	role.violation, role.detail = "", ""
	role.mu.Unlock()
	a.scanAndArchive(ctx)
	restarted := newTestArchiver(cfg, "iad", role, store, slog.Default())
	restarted.scanAndArchive(ctx)
	for _, arch := range []*BinlogArchiver{a, restarted} {
		st := arch.Snapshot(ctx)
		if !strings.HasPrefix(st.LastError, string(pitrarchive.TokenArchiveUnsafeHistory)+": ") || st.FilesArchived != 0 {
			t.Fatalf("after clearing the setting: %+v", st)
		}
	}
	m, _ := loadManifest(ctx, store, "binlogs", "iad")
	if m.HistoryIntegrity == nil || *m.HistoryIntegrity != *first.HistoryIntegrity {
		t.Fatalf("marker changed: %+v -> %+v", first.HistoryIntegrity, m.HistoryIntegrity)
	}
	// A later, different violation does not replace the first marker.
	role.mu.Lock()
	role.violation, role.detail = pitrarchive.ViolationBinlogFormat, "binlog_format=MIXED"
	role.mu.Unlock()
	newTestArchiver(cfg, "iad", role, store, slog.Default()).scanAndArchive(ctx)
	if m, _ := loadManifest(ctx, store, "binlogs", "iad"); m.HistoryIntegrity.Violation != pitrarchive.ViolationReplicationFilter {
		t.Fatalf("marker replaced: %+v", m.HistoryIntegrity)
	}
}

// A replica marks its own manifest, after the descriptor exists.
func TestHistoryIntegrityReplicaWritesItsOwnMarker(t *testing.T) {
	ctx := context.Background()
	store := newMemStore()
	role := &stubRoleChecker{readOnly: true, violation: pitrarchive.ViolationCommitOrder, detail: "replica_preserve_commit_order=0"}
	cfg := withTestArchive(&PITRConfig{ManifestPrefix: "binlogs", BinlogDir: t.TempDir(), BinlogIndex: "mysql-bin.index"}, store)
	replica := NewBinlogArchiver(cfg, "pdx", role, store, slog.Default())
	replica.scanAndArchive(ctx)
	st := replica.Snapshot(ctx)
	if !strings.HasPrefix(st.LastError, "PITRArchiveUnsafeHistory: pdx CommitOrder") || st.HistoryIntegrity == nil {
		t.Fatalf("replica before the descriptor: %+v", st)
	}
	if _, ok, _ := store.Get(ctx, "binlogs/manifest-pdx.json"); ok {
		t.Fatal("marker written without a descriptor")
	}
	if err := seedTestDescriptor(ctx, store, "binlogs"); err != nil {
		t.Fatal(err)
	}
	replica.scanAndArchive(ctx)
	m, err := loadManifest(ctx, store, "binlogs", "pdx")
	if err != nil {
		t.Fatal(err)
	}
	if m.HistoryIntegrity == nil || m.HistoryIntegrity.Violation != pitrarchive.ViolationCommitOrder {
		t.Fatalf("replica manifest = %+v", m)
	}
	if _, ok, _ := store.Get(ctx, "binlogs/manifest-iad.json"); ok {
		t.Fatal("the replica wrote another site's manifest")
	}
}

func TestHistoryIntegrityProbeFailureBlocksUploads(t *testing.T) {
	ctx := context.Background()
	store := newMemStore()
	role := &stubRoleChecker{integrityErr: errors.New("access denied for SHOW BINARY LOG STATUS")}
	a := newTestArchiver(promotionFixture(t), "iad", role, store, slog.Default())
	a.scanAndArchive(ctx)
	if st := a.Snapshot(ctx); !strings.HasPrefix(st.LastError, "history integrity: ") || st.FilesArchived != 0 || st.HistoryIntegrity != nil {
		t.Fatalf("status = %+v", st)
	}
}
