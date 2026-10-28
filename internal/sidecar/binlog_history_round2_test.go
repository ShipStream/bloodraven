package sidecar

import (
	"context"
	"log/slog"
	"strings"
	"testing"
)

type historyStateStub struct {
	stubRoleChecker
	uuid, executed string
}

func (s *historyStateStub) BinlogState(context.Context) (string, string, error) {
	return s.uuid, s.executed, nil
}
func TestRound2LostHistoryIsDivergenceWithoutReissuance(t *testing.T) {
	ctx := context.Background()
	store := newMemStore()
	state := &historyStateStub{uuid: coverageTestSID, executed: coverageTestSID + ":1-21"}
	a := NewBinlogArchiver(&PITRConfig{ManifestPrefix: "binlogs"}, "iad", state, store, slog.Default())
	source := "testdata/incarnation-b.bin"
	id, err := a.localBinlogID(source)
	if err != nil {
		t.Fatal(err)
	}
	if err := a.archiveOne(ctx, source, "mysql-bin.000002", id); err != nil {
		t.Fatal(err)
	}
	state.executed = coverageTestSID + ":1-20"
	m, err := loadManifest(ctx, store, "binlogs", "iad")
	if err != nil {
		t.Fatal(err)
	}
	if err := a.auditArchive(ctx, m); err == nil {
		t.Fatal("forgotten archived GTID was not reported")
	}
	m, err = loadManifest(ctx, store, "binlogs", "iad")
	if err != nil {
		t.Fatal(err)
	}
	if len(m.Divergent) != 1 || m.Divergent[0].MissingGTIDs != coverageTestSID+":21" {
		t.Fatalf("missing durable divergence evidence: %+v", m)
	}
	if m.Epoch != nil && m.Epoch.ResetReason != "" {
		t.Fatal("forgotten GTIDs alone were mislabeled as reissuance")
	}
	state.uuid = "ffffffff-bbbb-cccc-dddd-eeeeeeeeeeee"
	state.executed = coverageTestSID + ":1-21"
	if err := a.auditArchive(ctx, m); err != nil || len(m.Divergent) != 0 {
		t.Fatalf("restored execution did not clear contradicted evidence: %v", err)
	}
	state.executed = coverageTestSID + ":1-20"
	if err := a.auditArchive(ctx, m); err == nil {
		t.Fatal("lost tail was not re-evaluated")
	}

	if err := QuarantineArchive(ctx, store, "binlogs", "iad", "mysql-bin.000002", id, "canonical branch selected", coverageTestSID+":1-21", false); err != nil {
		t.Fatal(err)
	}
	if err := QuarantineArchive(ctx, store, "binlogs", "iad", "mysql-bin.000002", id, "", "", true); err != nil {
		t.Fatal(err)
	}
	restored, err := loadManifest(ctx, store, "binlogs", "iad")
	if err != nil {
		t.Fatal(err)
	}
	if len(restored.Divergent) != 1 {
		t.Fatal("quarantine undo lost prior divergence evidence")
	}
}
func TestRound2DivergenceVisibleBeforeRestore(t *testing.T) {
	store := newMemStore()
	row := ManifestEntry{Name: "mysql-bin.000002", ID: strings.Repeat("a", 64), PreviousGTIDs: coverageTestSID + ":1-10", GTIDCoverage: &BinlogGTIDCoverage{Complete: true, GTIDs: coverageTestSID + ":11"}}
	if err := appendManifestEntry(context.Background(), store, "binlogs", "iad", row); err != nil {
		t.Fatal(err)
	}
	state := &historyStateStub{stubRoleChecker: stubRoleChecker{readOnly: true}, uuid: "bbbbbbbb-bbbb-cccc-dddd-eeeeeeeeeeee", executed: coverageTestSID + ":1-10"}
	a := NewBinlogArchiver(&PITRConfig{ManifestPrefix: "binlogs"}, "iad", state, store, slog.Default())
	a.scanAndArchive(context.Background())
	status := a.Snapshot(context.Background())
	if !strings.Contains(status.LastError, "divergent") || status.UploadFailures == 0 {
		t.Fatalf("divergence is invisible until restore: %+v", status)
	}
}

func TestRound2LostUnindexedLegacyVisibleOnReplica(t *testing.T) {
	store := newMemStore()
	row := ManifestEntry{Name: "mysql-bin.000002", PreviousGTIDs: coverageTestSID + ":1-10"}
	if err := appendManifestEntry(context.Background(), store, "binlogs", "iad", row); err != nil {
		t.Fatal(err)
	}
	a := NewBinlogArchiver(&PITRConfig{ManifestPrefix: "binlogs"}, "iad", &stubRoleChecker{readOnly: true}, store, slog.Default())
	a.scanAndArchive(context.Background())
	status := a.Snapshot(context.Background())
	if !strings.Contains(status.LastError, "digest unknown") || status.UploadFailures == 0 {
		t.Fatalf("lost legacy object is invisible until restore: %+v", status)
	}
}
func TestRound2QuarantinePreservesObjectsAndCanBeUndone(t *testing.T) {
	ctx := context.Background()
	store := newMemStore()
	row := ManifestEntry{Name: "mysql-bin.000001", ID: strings.Repeat("a", 64)}
	key, err := ArchiveEntryKey("binlogs", "iad", row)
	if err != nil {
		t.Fatal(err)
	}
	if err := store.Put(ctx, key, strings.NewReader("preserve for forensic recovery"), 30); err != nil {
		t.Fatal(err)
	}
	if err := appendManifestEntry(ctx, store, "binlogs", "iad", row); err != nil {
		t.Fatal(err)
	}
	if err := QuarantineArchive(ctx, store, "binlogs", "iad", row.Name, row.ID, "acknowledged abandoned tail", coverageTestSID+":1-20", false); err != nil {
		t.Fatal(err)
	}
	m, err := loadManifest(ctx, store, "binlogs", "iad")
	if err != nil || len(m.Files) != 0 || len(m.Quarantined) != 1 {
		t.Fatalf("quarantine metadata: %+v, %v", m, err)
	}
	if _, exists, err := store.Get(ctx, key); err != nil || !exists {
		t.Fatal("quarantine deleted the object")
	}
	if err := QuarantineArchive(ctx, store, "binlogs", "iad", row.Name, row.ID, "", "", true); err != nil {
		t.Fatal(err)
	}
	m, err = loadManifest(ctx, store, "binlogs", "iad")
	if err != nil || len(m.Files) != 1 || len(m.Quarantined) != 0 {
		t.Fatalf("undo: %+v, %v", m, err)
	}
}
