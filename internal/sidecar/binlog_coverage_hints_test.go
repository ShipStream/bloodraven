package sidecar

import (
	"bytes"
	"context"
	"log/slog"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/shipstream/bloodraven/internal/sidecar/binlogtest"
)

const (
	hintSIDA = "aaaaaaaa-0000-0000-0000-00000000000a"
	hintSIDB = "bbbbbbbb-0000-0000-0000-00000000000b"
)

// The manifest v2 row hints (D-4) come from the same inspection pass as the
// GTID coverage: original commit min/max in µs and the empty transactions.
func TestBinlogCoverageHints(t *testing.T) {
	t0 := time.Date(2026, 11, 4, 12, 0, 0, 123456000, time.UTC)
	tests := []struct {
		name         string
		file         binlogtest.File
		wantGTIDs    string
		wantEmpty    string
		wantMin      int64
		wantMax      int64
		wantComplete bool
	}{
		{
			name: "mixed origins with an injected empty transaction",
			file: binlogtest.File{ServerID: 101, Previous: hintSIDA + ":1-10", Txns: []binlogtest.Txn{
				{SID: hintSIDA, GNO: 11, Commit: t0.Add(time.Second), ServerID: 101},
				{SID: hintSIDB, GNO: 5, Commit: t0, ServerID: 102},
				{SID: hintSIDA, GNO: 12, Commit: t0.Add(2 * time.Second), ServerID: 101, Empty: true},
			}},
			wantGTIDs: hintSIDA + ":11-12," + hintSIDB + ":5", wantEmpty: hintSIDA + ":12",
			wantMin: t0.UnixMicro(), wantMax: t0.Add(2 * time.Second).UnixMicro(), wantComplete: true,
		},
		{
			name: "every transaction empty",
			file: binlogtest.File{ServerID: 101, Txns: []binlogtest.Txn{
				{SID: hintSIDA, GNO: 1, Commit: t0, ServerID: 101, Empty: true},
				{SID: hintSIDA, GNO: 2, Commit: t0, ServerID: 101, Empty: true},
			}},
			wantGTIDs: hintSIDA + ":1-2", wantEmpty: hintSIDA + ":1-2", wantMin: t0.UnixMicro(), wantMax: t0.UnixMicro(), wantComplete: true,
		},
		{
			name:      "a transaction without a commit timestamp leaves the time hints unset",
			file:      binlogtest.File{ServerID: 101, Txns: []binlogtest.Txn{{SID: hintSIDA, GNO: 1, Commit: t0, ServerID: 101}, {SID: hintSIDA, GNO: 2, ServerID: 101}}},
			wantGTIDs: hintSIDA + ":1-2", wantComplete: true,
		},
		{
			name:         "no transactions",
			file:         binlogtest.File{ServerID: 101, Previous: hintSIDA + ":1-3"},
			wantComplete: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			evidence, err := InspectBinlog(bytes.NewReader(tt.file.MustBytes()), time.Time{})
			if err != nil {
				t.Fatal(err)
			}
			c := evidence.Metadata.GTIDCoverage
			if c.Complete != tt.wantComplete || c.GTIDs != tt.wantGTIDs || c.EmptyGTIDs != tt.wantEmpty ||
				c.OriginalCommitMin != tt.wantMin || c.OriginalCommitMax != tt.wantMax {
				t.Fatalf("coverage = %+v, want gtids %q empty %q min %d max %d", c, tt.wantGTIDs, tt.wantEmpty, tt.wantMin, tt.wantMax)
			}
		})
	}
}

// Uploaded rows carry the hints into the manifest.
func TestArchivedRowCarriesHints(t *testing.T) {
	ctx := context.Background()
	t0 := time.Date(2026, 11, 4, 12, 0, 0, 0, time.UTC)
	dir := t.TempDir()
	data := binlogtest.File{ServerID: 101, Txns: []binlogtest.Txn{
		{SID: hintSIDA, GNO: 1, Commit: t0, ServerID: 101},
		{SID: hintSIDA, GNO: 2, Commit: t0.Add(time.Millisecond), ServerID: 101, Empty: true},
	}}.MustBytes()
	if err := os.WriteFile(filepath.Join(dir, "mysql-bin.000001"), data, 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, "mysql-bin.index"), []byte("mysql-bin.000001\nmysql-bin.000002\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	store := newMemStore()
	a := newTestArchiver(&PITRConfig{BinlogDir: dir, BinlogIndex: "mysql-bin.index", ManifestPrefix: "binlogs"}, "iad", &stubRoleChecker{}, store, slog.Default())
	a.scanAndArchive(ctx)
	m, err := loadManifest(ctx, store, "binlogs", "iad")
	if err != nil {
		t.Fatal(err)
	}
	if len(m.Files) != 1 || m.Files[0].GTIDCoverage == nil {
		t.Fatalf("manifest = %+v (lastError %q)", m, a.Snapshot(ctx).LastError)
	}
	c := m.Files[0].GTIDCoverage
	if c.OriginalCommitMin != t0.UnixMicro() || c.OriginalCommitMax != t0.Add(time.Millisecond).UnixMicro() || c.EmptyGTIDs != hintSIDA+":2" {
		t.Fatalf("row hints = %+v", c)
	}
	if m.ArchiveID != testArchiveID {
		t.Fatalf("manifest archiveId = %q, want %q", m.ArchiveID, testArchiveID)
	}
}
