package main

import (
	"bytes"
	"context"
	"io"
	"log/slog"
	"os"
	"testing"
	"time"

	"github.com/shipstream/bloodraven/internal/sidecar"
)

type rr4RebuildReplica struct {
	*sidecar.LiveMysql
	executed string
}

func (r rr4RebuildReplica) IsReadOnly(context.Context) (bool, error) { return true, nil }
func (r rr4RebuildReplica) BinlogState(context.Context) (string, string, error) {
	return "ffffffff-bbbb-cccc-dddd-eeeeeeeeeeee", r.executed, nil
}
func (r rr4RebuildReplica) BinlogHistoryReady(context.Context) (bool, error) {
	return r.executed != "", nil
}

type rr4MaintenanceStore struct {
	sidecar.ArchiveStore
	reads  int
	cancel context.CancelFunc
}

func (s *rr4MaintenanceStore) Get(ctx context.Context, key string) ([]byte, bool, error) {
	data, exists, err := s.ArchiveStore.Get(ctx, key)
	if key == sidecar.ManifestKey("binlogs", "iad") {
		s.reads++
		if s.reads == 2 {
			s.cancel()
		}
	}
	return data, exists, err
}
func TestRR4ProbeRestoreRefusedAfterRebuild(t *testing.T) {
	ctx := context.Background()
	dir := os.Getenv("RR4_STORE")
	if dir == "" {
		dir = t.TempDir()
	}
	store, err := sidecar.NewArchiveStore(ctx, &sidecar.PITRConfig{StorageType: "PVC", PVC: &sidecar.PITRPVCConfig{MountPath: dir}})
	if err != nil {
		t.Fatal(err)
	}
	if os.Getenv("RR4_STORE") == "" {
		data := pitrFixture(t, "incarnation-b.bin")
		e, err := sidecar.InspectBinlog(bytes.NewReader(data), time.Time{})
		if err != nil {
			t.Fatal(err)
		}
		e.Metadata.GTIDCoverage.ContentID = e.Digest
		row := sidecar.ManifestEntry{Name: "mysql-bin.000002", ID: e.Digest, Size: int64(len(data)), PreviousGTIDs: e.Metadata.PreviousGTIDs, GTIDCoverage: e.Metadata.GTIDCoverage}
		key, err := sidecar.ArchiveEntryKey("binlogs", "iad", row)
		if err != nil {
			t.Fatal(err)
		}
		if err := store.Put(ctx, key, bytes.NewReader(data), int64(len(data))); err != nil {
			t.Fatal(err)
		}
		round2Manifest(t, store, sidecar.Manifest{Site: "iad", Files: []sidecar.ManifestEntry{row}})
		for _, executed := range []string{"", retentionTestSID + ":1-21"} {
			runCtx, cancel := context.WithCancel(ctx)
			observed := &rr4MaintenanceStore{ArchiveStore: store, cancel: cancel}
			a := sidecar.NewBinlogArchiver(&sidecar.PITRConfig{ManifestPrefix: "binlogs", BinlogDir: t.TempDir(), PollInterval: time.Hour}, "iad", rr4RebuildReplica{executed: executed}, observed, slog.New(slog.NewTextHandler(io.Discard, nil)))
			done := make(chan struct{})
			go func() { defer close(done); a.Run(runCtx) }()
			select {
			case <-done:
			case <-time.After(5 * time.Second):
				cancel()
				<-done
				t.Fatal("rebuild maintenance did not finish")
			}
			cancel()
		}
	}
	got, err := downloadPITRWindow(ctx, store, "binlogs", "iad", time.Date(2030, 1, 1, 0, 0, 0, 0, time.UTC), t.TempDir(), retentionTestSID+":1-20", slog.New(slog.NewTextHandler(io.Discard, nil)))
	if err != nil || got != 1 {
		t.Fatalf("PITR from pre-rebuild backup refused intact history: got=%d err=%v", got, err)
	}
}
