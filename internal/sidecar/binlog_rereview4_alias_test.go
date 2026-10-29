package sidecar

import (
	"bytes"
	"context"
	"log/slog"
	"os"
	"testing"
	"time"
)

func TestRereview4AuditPreservesLegacyOnlyDivergence(t *testing.T) {
	ctx := context.Background()
	data, err := os.ReadFile("testdata/incarnation-b.bin")
	if err != nil {
		t.Fatal(err)
	}
	e, err := InspectBinlog(bytes.NewReader(data), time.Time{})
	if err != nil {
		t.Fatal(err)
	}
	store := newMemStore()
	old := ManifestEntry{Name: "mysql-bin.000001", PreviousGTIDs: e.Metadata.PreviousGTIDs, Size: int64(len(data)), GTIDCoverage: e.Metadata.GTIDCoverage}
	newer := old
	newer.ID = e.Digest
	m := &Manifest{Site: "iad", Files: []ManifestEntry{old, newer}, Divergent: []ArchiveDivergence{{Name: old.Name, MissingGTIDs: coverageTestSID + ":21"}}}
	for _, row := range m.Files {
		key, err := ArchiveEntryKey("binlogs", "iad", row)
		if err != nil {
			t.Fatal(err)
		}
		if err := store.Put(ctx, key, bytes.NewReader(data), int64(len(data))); err != nil {
			t.Fatal(err)
		}
	}
	state := &historyStateStub{uuid: "ffffffff-bbbb-cccc-dddd-eeeeeeeeeeee", executed: coverageTestSID + ":1-21"}
	a := NewBinlogArchiver(&PITRConfig{ManifestPrefix: "binlogs"}, "iad", state, store, slog.Default())
	if err := a.auditArchive(ctx, m); err != nil || len(m.Divergent) != 0 {
		t.Fatalf("restored execution retained obsolete legacy divergence: %v %+v", err, m.Divergent)
	}
	state.executed = coverageTestSID + ":1-20"
	if err := a.auditArchive(ctx, m); err == nil {
		t.Fatal("actual missing legacy GTID was not flagged")
	}
	found := false
	for _, d := range m.Divergent {
		if d.Name == old.Name && d.ID == "" {
			found = true
		}
	}
	if !found {
		t.Fatal("real legacy-only evidence lost before alias verification")
	}
}
