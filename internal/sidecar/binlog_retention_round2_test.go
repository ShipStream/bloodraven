package sidecar

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"os"
	"strings"
	"testing"
	"time"
)

func TestRereviewProbeUninspectableRowHotLoop(t *testing.T) {
	ctx := context.Background()
	pvc, err := newPVCStore(&PITRPVCConfig{MountPath: t.TempDir()})
	if err != nil {
		t.Fatal(err)
	}
	store := &retentionReviewStore{ArchiveStore: pvc}
	payload, err := os.ReadFile("testdata/incarnation-b.bin")
	if err != nil {
		t.Fatal(err)
	}
	row := ManifestEntry{Name: "mysql-bin.000001", ContentID: strings.Repeat("c", 64), LastEventTime: time.Date(2026, 4, 15, 9, 0, 0, 0, time.UTC)}
	key, _ := ArchiveEntryKey("binlogs", "iad", row)
	row.RemotePath = key
	if err := store.Put(ctx, key, bytes.NewReader(payload), int64(len(payload))); err != nil {
		t.Fatal(err)
	}
	if err := saveManifest(ctx, store, "binlogs", &Manifest{Site: "iad", Files: []ManifestEntry{row}}); err != nil {
		t.Fatal(err)
	}
	a := retentionReviewArchiver(t, store, slog.Default(), retentionReviewProof())
	for i := 0; i < 5; i++ {
		a.maybeRunRetention(ctx)
		next := time.Until(a.lastRetentionRun.Add(a.retentionCfg.Interval))
		t.Logf("sweep %d: reads=%d next sweep in %v", i, store.reads, next.Round(time.Second))
		a.lastRetentionRun = a.lastRetentionRun.Add(-31 * time.Second) // let 30s elapse
	}
	if store.reads > 1 {
		t.Fatalf("uninspectable row re-downloaded on every 30s sweep: %d reads in 5 sweeps (~2.5 min)", store.reads)
	}
}

type retentionRound2Store struct {
	ArchiveStore
	reads map[string]int
	deny  string
}

func (s *retentionRound2Store) Open(ctx context.Context, key string) (io.ReadCloser, error) {
	s.reads[key]++
	if key == s.deny {
		return nil, os.ErrPermission
	}
	return s.ArchiveStore.Open(ctx, key)
}
func retentionRound2Failures(t *testing.T, mode string, logger *slog.Logger) (*BinlogArchiver, *retentionRound2Store, ManifestEntry, string) {
	t.Helper()
	store := &retentionRound2Store{ArchiveStore: newMemStore(), reads: make(map[string]int)}
	row := ManifestEntry{Name: "mysql-bin.000000", ContentID: strings.Repeat("c", 64), LastEventTime: time.Date(2026, 4, 15, 9, 0, 0, 0, time.UTC)}
	if mode == "corrupt" {
		row.ContentID = ""
	}
	key, err := ArchiveEntryKey("binlogs", "iad", row)
	if err != nil {
		t.Fatal(err)
	}
	payload, err := os.ReadFile("testdata/incarnation-b.bin")
	if err != nil {
		t.Fatal(err)
	}
	if mode == "corrupt" {
		payload = []byte("truncated or malformed binlog")
	}
	if mode != "missing" {
		if err := store.Put(context.Background(), key, bytes.NewReader(payload), int64(len(payload))); err != nil {
			t.Fatal(err)
		}
	}
	if mode == "permission" {
		store.deny = key
	}
	if err := saveManifest(context.Background(), store, "binlogs", &Manifest{Site: "iad", Files: []ManifestEntry{row}}); err != nil {
		t.Fatal(err)
	}
	return retentionReviewArchiver(t, store, logger, retentionReviewProof()), store, row, key
}
func TestRound2FailedInspectionsCoolDownWithoutAuthorizingRetirement(t *testing.T) {
	for _, mode := range []string{"mismatch", "corrupt", "missing", "permission"} {
		t.Run(mode, func(t *testing.T) {
			a, store, _, key := retentionRound2Failures(t, mode, slog.Default())
			for i := 0; i < 5; i++ {
				a.lastRetentionRun = time.Time{}
				a.maybeRunRetention(context.Background())
			}
			if store.reads[key] != 1 {
				t.Fatalf("permanent %s error caused %d downloads", mode, store.reads[key])
			}
			m, err := loadManifest(context.Background(), store, "binlogs", "iad")
			if err != nil || len(m.Files) != 1 || len(m.Pruned) != 0 {
				t.Fatalf("failed inspection authorized retirement: %+v %v", m, err)
			}
			if next := time.Until(a.lastRetentionRun.Add(a.retentionCfg.Interval)); next < time.Hour-time.Minute {
				t.Fatalf("failed-only batch still retries soon: %v", next)
			}
			for id := range a.retentionInspectionFailures {
				a.retentionInspectionFailures[id] = time.Now().Add(-time.Minute)
			}
			a.lastRetentionRun = time.Time{}
			a.maybeRunRetention(context.Background())
			if store.reads[key] != 2 {
				t.Fatal("failed identity never retried after cooldown")
			}
		})
	}
}
func TestRound2FailedRowDoesNotRepeatDuringHealthyBackfill(t *testing.T) {
	var logs bytes.Buffer
	a, store, row, badKey := retentionRound2Failures(t, "mismatch", slog.New(slog.NewJSONHandler(&logs, nil)))
	payload, err := os.ReadFile("testdata/incarnation-b.bin")
	if err != nil {
		t.Fatal(err)
	}
	m := &Manifest{Site: "iad", Files: []ManifestEntry{row}}
	for i := 1; i <= 16; i++ {
		f := ManifestEntry{Name: fmt.Sprintf("mysql-bin.%06d", i), LastEventTime: row.LastEventTime}
		key, err := ArchiveEntryKey("binlogs", "iad", f)
		if err != nil {
			t.Fatal(err)
		}
		if err := store.Put(context.Background(), key, bytes.NewReader(payload), int64(len(payload))); err != nil {
			t.Fatal(err)
		}
		m.Files = append(m.Files, f)
	}
	if err := saveManifest(context.Background(), store, "binlogs", m); err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 3; i++ {
		a.lastRetentionRun = time.Time{}
		a.maybeRunRetention(context.Background())
		if i < 2 && time.Until(a.lastRetentionRun.Add(a.retentionCfg.Interval)) > 30*time.Second {
			t.Fatal("healthy backfill did not resume promptly")
		}
	}
	total := 0
	for _, reads := range store.reads {
		total += reads
	}
	if total != 17 || store.reads[badKey] != 1 {
		t.Fatalf("failed row monopolized backfill: total=%d bad=%d", total, store.reads[badKey])
	}
	info := false
	for _, line := range bytes.Split(bytes.TrimSpace(logs.Bytes()), []byte("\n")) {
		var record map[string]any
		if err := json.Unmarshal(line, &record); err != nil {
			t.Fatal(err)
		}
		if record["msg"] == "retention deferred" && record["reason"] == "binlogGTIDCoverageUnknown" && record["level"] == "INFO" {
			info = true
		}
	}
	if !info {
		t.Fatal("normal backfill was reported as a warning")
	}
}
func TestRound2InspectionFailureStateFollowsManifestIdentity(t *testing.T) {
	a, store, row, key := retentionRound2Failures(t, "mismatch", slog.Default())
	a.maybeRunRetention(context.Background())
	actual, err := fileContentID("testdata/incarnation-b.bin")
	if err != nil {
		t.Fatal(err)
	}
	row.ContentID = actual
	if err := saveManifest(context.Background(), store, "binlogs", &Manifest{Site: "iad", Files: []ManifestEntry{row}}); err != nil {
		t.Fatal(err)
	}
	a.lastRetentionRun = time.Time{}
	a.maybeRunRetention(context.Background())
	if store.reads[key] != 2 || len(a.retentionInspectionFailures) != 0 {
		t.Fatal("changed proof identity inherited stale failure cooldown")
	}
	m, err := loadManifest(context.Background(), store, "binlogs", "iad")
	if err != nil {
		t.Fatal(err)
	}
	if len(m.Files) != 1 || !coverageBoundToEntry(m.Files[0]) {
		t.Fatal("repaired object proof was not persisted")
	}
}
