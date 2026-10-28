package main

import (
	"bytes"
	"context"
	"encoding/binary"
	"github.com/shipstream/bloodraven/internal/sidecar"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"testing"
	"time"
)

func TestReview5ActiveReissueMustNotReplayAbandonedTail(t *testing.T) {
	dir := os.Getenv("RR5_STORE")
	if dir == "" {
		dir = t.TempDir()
	}
	ctx := context.Background()
	store, err := sidecar.NewArchiveStore(ctx, &sidecar.PITRConfig{StorageType: "PVC", PVC: &sidecar.PITRPVCConfig{MountPath: dir}})
	if err != nil {
		t.Fatal(err)
	}
	if os.Getenv("RR5_STORE") == "" {
		row := rr5Put(t, store, "mysql-bin.000001", pitrFixture(t, "incarnation-a.bin"))
		round2Manifest(t, store, sidecar.Manifest{Site: "iad", Files: []sidecar.ManifestEntry{row}})
		logs := t.TempDir()
		cfg := &sidecar.PITRConfig{ManifestPrefix: "binlogs", BinlogDir: logs, BinlogIndex: "mysql-bin.index", PollInterval: time.Hour}
		replacement := pitrFixture(t, "same-header.bin")
		active := bytes.Clone(replacement[:4])
		headerEnd := 0
		for off := 4; off < len(replacement); {
			size := int(binary.LittleEndian.Uint32(replacement[off+9 : off+13]))
			if replacement[off+4] == 33 {
				headerEnd = len(active)
			}
			if replacement[off+4] != 3 {
				active = append(active, replacement[off:off+size]...)
			}
			off += size
		}
		if err := os.WriteFile(filepath.Join(logs, cfg.BinlogIndex), []byte("mysql-bin.000001\n"), 0600); err != nil {
			t.Fatal(err)
		}
		for i, executed := range []string{retentionTestSID + ":1-10", retentionTestSID + ":1-11"} {
			local := active[:headerEnd]
			if i == 1 {
				local = active
			}
			if err := os.WriteFile(filepath.Join(logs, "mysql-bin.000001"), local, 0600); err != nil {
				t.Fatal(err)
			}
			runCtx, cancel := context.WithCancel(ctx)
			observed := &rr4MaintenanceStore{ArchiveStore: store, cancel: cancel}
			a := sidecar.NewBinlogArchiver(cfg, "iad", rr5LiveIssuer{executed: executed}, observed, slog.New(slog.NewTextHandler(io.Discard, nil)))
			done := make(chan struct{})
			go func() { defer close(done); a.Run(runCtx) }()
			select {
			case <-done:
			case <-time.After(5 * time.Second):
				cancel()
				<-done
				t.Fatal("issuer audit failed to complete")
			}
			cancel()
		}
	}
	out := t.TempDir()
	n, err := downloadPITRWindow(ctx, store, "binlogs", "iad", time.Date(2030, 1, 1, 0, 0, 0, 0, time.UTC), out, retentionTestSID+":1-10", slog.Default())
	if err == nil {
		paths, _ := filepath.Glob(filepath.Join(out, "iad", "mysql-bin.*"))
		if len(paths) == 1 {
			data, readErr := os.ReadFile(paths[0])
			if readErr != nil {
				t.Fatal(readErr)
			}
			t.Logf("staged original INSERT marker A=%v; replacement marker B=%v", bytes.Contains(data, []byte("VALUES ('A')")), bytes.Contains(data, []byte("VALUES ('B')")))
		}
		t.Fatalf("replayed abandoned original GTID 11 after numeric reissue on live primary: n=%d err=%v", n, err)
	}
}

// Embed the live interface's private role method, overriding every SQL probe
// used by this active-only archiver cycle.
type rr5LiveIssuer struct {
	*sidecar.LiveMysql
	executed string
}

func (r rr5LiveIssuer) IsReadOnly(context.Context) (bool, error) { return false, nil }
func (r rr5LiveIssuer) BinlogState(context.Context) (string, string, error) {
	return retentionTestSID, r.executed, nil
}
func (r rr5LiveIssuer) BinlogHistoryReady(context.Context) (bool, error) { return true, nil }
