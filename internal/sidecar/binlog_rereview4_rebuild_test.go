package sidecar

import (
	"context"
	"log/slog"
	"os"
	"testing"
)

// A site rebuilt on a fresh PVC runs mysqld with an empty gtid_executed until
// the operator's CLONE completes (BootstrapReplica; CloneTimeout default 1h).
// maintainArchive runs the audit on every scan, including read-only/pre-clone
// servers. After the clone the server again contains every archived
// transaction (the donor had them), so nothing is divergent. Persisted
// evidence must not outlive that transient pre-clone state.
//
// RR4_STORE=<dir> keeps the resulting PVC-layout archive for the cmd/bloodraven
// half of the probe (TestRR4ProbeRestoreRefusedAfterRebuild).
func TestRR4ProbeFreshPVCBeforeCloneLeavesPermanentDivergence(t *testing.T) {
	ctx := context.Background()
	dir := os.Getenv("RR4_STORE")
	if dir == "" {
		dir = t.TempDir()
	}
	store, err := NewArchiveStore(ctx, &PITRConfig{StorageType: "PVC", PVC: &PITRPVCConfig{MountPath: dir}})
	if err != nil {
		t.Fatal(err)
	}
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

	// Node replaced: fresh PVC, freshly initialised mysqld, read-only, awaiting CLONE.
	state.readOnly = true
	state.executed = ""
	a.scanAndArchive(ctx)
	before, err := loadManifest(ctx, store, "binlogs", "iad")
	if err != nil {
		t.Fatal(err)
	}
	if len(before.Divergent) != 0 {
		t.Fatal("pre-clone empty execution created divergence evidence")
	}

	// CLONE from the canonical primary completes; every archived GTID is back.
	state.executed = coverageTestSID + ":1-21"
	a.scanAndArchive(ctx)
	a.scanAndArchive(ctx)
	m, err := loadManifest(ctx, store, "binlogs", "iad")
	if err != nil {
		t.Fatal(err)
	}
	if len(m.Divergent) != 0 {
		t.Fatalf("transient pre-clone state left permanent divergence on intact history: divergent=%+v lastError=%q", m.Divergent, a.Snapshot(ctx).LastError)
	}
}
