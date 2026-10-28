package sidecar

import (
	"bytes"
	"context"
	"encoding/binary"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// A real replacement stream is still the active binlog, so the archival
// protocol has no second sealed identity to compare with the lost tail yet.
func TestReview5ActiveReissueMustNotEraseLostTailEvidence(t *testing.T) {
	ctx := context.Background()
	dir := os.Getenv("RR5_STORE")
	if dir == "" {
		dir = t.TempDir()
	}
	store, err := NewArchiveStore(ctx, &PITRConfig{StorageType: "PVC", PVC: &PITRPVCConfig{MountPath: dir}})
	if err != nil {
		t.Fatal(err)
	}
	logs := t.TempDir()
	cfg := &PITRConfig{ManifestPrefix: "binlogs", BinlogDir: logs, BinlogIndex: "mysql-bin.index"}
	state := &rr4BootstrapState{historyStateStub: historyStateStub{uuid: coverageTestSID, executed: coverageTestSID + ":1-11"}, ready: true}
	a := NewBinlogArchiver(cfg, "iad", state, store, slog.Default())
	id, err := a.localBinlogID("testdata/incarnation-a.bin")
	if err != nil {
		t.Fatal(err)
	}
	if err := a.archiveOne(ctx, "testdata/incarnation-a.bin", "mysql-bin.000001", id); err != nil {
		t.Fatal(err)
	}
	// RESET/recovery has removed GTID 11, retaining the same server UUID.
	// The replacement GTID 11 has different query bytes (the committed true
	// reissuance fixture) but has not rotated out of the active binlog.
	replacement, err := os.ReadFile("testdata/same-header.bin")
	if err != nil {
		t.Fatal(err)
	}
	// Strip STOP from the sealed fixture to model a currently open file.
	active := bytes.Clone(replacement[:4])
	headerEnd := 0
	for off := 4; off < len(replacement); {
		size := int(binary.LittleEndian.Uint32(replacement[off+9 : off+13]))
		kind := replacement[off+4]
		if kind == 33 && headerEnd == 0 {
			headerEnd = len(active)
		}
		if kind != 3 {
			active = append(active, replacement[off:off+size]...)
		}
		off += size
	}
	if headerEnd == 0 {
		t.Fatal("replacement fixture lacks GTID")
	}
	if err := os.WriteFile(filepath.Join(logs, "mysql-bin.000001"), active[:headerEnd], 0600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(logs, cfg.BinlogIndex), []byte("mysql-bin.000001\n"), 0600); err != nil {
		t.Fatal(err)
	}
	state.executed = coverageTestSID + ":1-10"
	a.scanAndArchive(ctx)
	m, err := loadManifest(ctx, store, "binlogs", "iad")
	if err != nil {
		t.Fatal(err)
	}
	if len(m.Divergent) != 1 {
		t.Fatalf("precondition: lost original tail not flagged: %+v", m)
	}
	t.Logf("before reissue: divergent=%+v", m.Divergent)
	if err := os.WriteFile(filepath.Join(logs, "mysql-bin.000001"), active, 0600); err != nil {
		t.Fatal(err)
	}
	state.executed = coverageTestSID + ":1-11"
	a.scanAndArchive(ctx)
	m, err = loadManifest(ctx, store, "binlogs", "iad")
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("active conflicting replacement: files=%d divergent=%+v epoch=%+v status=%+v", len(m.Files), m.Divergent, m.Epoch, a.Snapshot(ctx))
	if len(m.Divergent) == 0 && (m.Epoch == nil || m.Epoch.ResetReason == "") {
		t.Error("numeric reissuance erased known lost-tail evidence before issuance could be verified")
	}
}

func TestReview5KnownLossClearingRequiresDifferentIssuer(t *testing.T) {
	foreign := "ffffffff-bbbb-cccc-dddd-eeeeeeeeeeee"
	for _, tc := range []struct{ name, uuid, missing, want string }{
		{"same issuer", coverageTestSID, coverageTestSID + ":11", coverageTestSID + ":11"},
		{"fresh UUID", foreign, coverageTestSID + ":11", ""},
		{"tagged issuer", coverageTestSID, coverageTestSID + ":blue:11", coverageTestSID + ":blue:11"},
		{"mixed sources", coverageTestSID, coverageTestSID + ":11," + foreign + ":3", coverageTestSID + ":11"},
		{"unknown issuer", "", coverageTestSID + ":11", coverageTestSID + ":11"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctx := context.Background()
			store := newMemStore()
			row := ManifestEntry{Name: "mysql-bin.000001", ID: strings.Repeat("a", 64)}
			m := &Manifest{Site: "iad", Files: []ManifestEntry{row}, Divergent: []ArchiveDivergence{{Name: row.Name, ID: row.ID, MissingGTIDs: tc.missing}}}
			state := &rr4BootstrapState{historyStateStub: historyStateStub{uuid: tc.uuid, executed: coverageTestSID + ":1-11," + coverageTestSID + ":blue:1-11," + foreign + ":1-3"}, ready: true}
			a := NewBinlogArchiver(&PITRConfig{ManifestPrefix: "binlogs"}, "iad", state, store, slog.Default())
			err := a.auditArchive(ctx, m)
			if tc.want == "" {
				if err != nil || len(m.Divergent) != 0 {
					t.Fatalf("fresh issuer did not clear foreign loss: %v %+v", err, m.Divergent)
				}
				return
			}
			if err == nil || len(m.Divergent) != 1 || m.Divergent[0].MissingGTIDs != tc.want {
				t.Fatalf("known loss changed on numeric return: %v %+v", err, m.Divergent)
			}
		})
	}
}
