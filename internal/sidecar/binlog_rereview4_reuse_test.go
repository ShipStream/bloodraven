package sidecar

import (
	"bytes"
	"context"
	"log/slog"
	"os"
	"testing"
	"time"
)

func TestRereview4LegacyReuseEventuallyGetsGuard(t *testing.T) {
	ctx := context.Background()
	store := newMemStore()
	raw, err := os.ReadFile("testdata/incarnation-a.bin")
	if err != nil {
		t.Fatal(err)
	}
	e, err := InspectBinlog(bytes.NewReader(raw), time.Time{})
	if err != nil {
		t.Fatal(err)
	}
	legacy := ManifestEntry{Name: "mysql-bin.000001", PreviousGTIDs: e.Metadata.PreviousGTIDs, GTIDCoverage: e.Metadata.GTIDCoverage, FirstEventTime: e.Metadata.FirstEventTime, LastEventTime: e.Metadata.LastEventTime, Size: int64(len(raw))}
	key, err := ArchiveEntryKey("binlogs", "iad", legacy)
	if err != nil {
		t.Fatal(err)
	}
	if err := store.Put(ctx, key, bytes.NewReader(raw), int64(len(raw))); err != nil {
		t.Fatal(err)
	}
	if err := appendManifestEntry(ctx, store, "binlogs", "iad", legacy); err != nil {
		t.Fatal(err)
	}
	a := NewBinlogArchiver(&PITRConfig{ManifestPrefix: "binlogs"}, "iad", &historyStateStub{uuid: coverageTestSID, executed: coverageTestSID + ":1-11"}, store, slog.Default())
	id, err := a.localBinlogID("testdata/same-header.bin")
	if err != nil {
		t.Fatal(err)
	}
	if err := a.archiveOne(ctx, "testdata/same-header.bin", "mysql-bin.000002", id); err != nil {
		t.Fatal(err)
	}
	m, err := loadManifest(ctx, store, "binlogs", "iad")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := a.backfillRetentionCoverage(ctx, m, time.Date(2030, 1, 1, 0, 0, 0, 0, time.UTC)); err != nil {
		t.Fatal(err)
	}
	if err := a.auditArchive(ctx, m); err != nil {
		t.Logf("audit error: %v", err)
	}
	for _, row := range m.Files {
		if !coverageBoundToEntry(row) {
			t.Fatal("fixture not fully backfilled")
		}
	}
	if m.Epoch == nil || m.Epoch.ResetReason == "" {
		t.Errorf("different query issuance for GTID 11 escaped reuse detection before and after legacy verification: epoch=%+v divergent=%+v", m.Epoch, m.Divergent)
		if err := a.detectArchiveReuse(ctx, m, m.Files[1]); err != nil {
			t.Fatal(err)
		}
		if m.Epoch == nil || m.Epoch.ResetReason == "" {
			t.Fatal("fixture does not establish differing original issuance")
		}
		t.Logf("explicit rerun with identical, now-bound evidence proves conflict: %s", m.Epoch.ResetReason)
	}
}
