package main

import (
	"bytes"
	"context"
	"encoding/binary"
	"encoding/json"
	"hash/crc32"
	"io"
	"log/slog"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/shipstream/bloodraven/internal/sidecar"
)

func round2Archive(t *testing.T, payloads ...[]byte) (sidecar.ArchiveStore, []sidecar.ManifestEntry) {
	t.Helper()
	ctx := context.Background()
	store, err := sidecar.NewArchiveStore(ctx, &sidecar.PITRConfig{StorageType: "PVC", PVC: &sidecar.PITRPVCConfig{MountPath: t.TempDir()}})
	if err != nil {
		t.Fatal(err)
	}
	var rows []sidecar.ManifestEntry
	for i, data := range payloads {
		e, err := sidecar.InspectBinlog(bytes.NewReader(data), time.Time{})
		if err != nil {
			t.Fatal(err)
		}
		row := sidecar.ManifestEntry{Name: []string{"mysql-bin.000001", "mysql-bin.000002", "mysql-bin.000003"}[i], ID: e.Digest, PreviousGTIDs: e.Metadata.PreviousGTIDs, FirstEventTime: e.Metadata.FirstEventTime, Size: int64(len(data))}
		key, err := sidecar.ArchiveEntryKey("binlogs", "iad", row)
		if err != nil {
			t.Fatal(err)
		}
		if err := store.Put(ctx, key, bytes.NewReader(data), int64(len(data))); err != nil {
			t.Fatal(err)
		}
		rows = append(rows, row)
	}
	round2Manifest(t, store, sidecar.Manifest{Site: "iad", Files: rows})
	return store, rows
}
func round2Manifest(t *testing.T, store sidecar.ArchiveStore, m sidecar.Manifest) {
	t.Helper()
	data, err := json.Marshal(m)
	if err != nil {
		t.Fatal(err)
	}
	if err := store.Put(context.Background(), sidecar.ManifestKey("binlogs", "iad"), bytes.NewReader(data), int64(len(data))); err != nil {
		t.Fatal(err)
	}
}
func TestRound2FutureBranchesDoNotBlockOlderWindow(t *testing.T) {
	store, rows := round2Archive(t, pitrFixture(t, "incarnation-a.bin"), pitrFixture(t, "review-order-b.bin"), pitrFixture(t, "incarnation-b.bin"))
	for _, other := range []string{"v:1-5", "w:1-5"} {
		rows = append(rows, sidecar.ManifestEntry{Name: "mysql-bin.future-" + other[:1], ID: strings.Repeat("a", 64), PreviousGTIDs: retentionTestSID + ":1-30," + other})
	}
	round2Manifest(t, store, sidecar.Manifest{Site: "iad", Files: rows})
	n, err := downloadSiteBinlogs(context.Background(), store, "binlogs", "iad", time.Date(2026, 4, 15, 9, 30, 0, 0, time.UTC), t.TempDir(), slog.Default())
	if err != nil || n != 3 {
		t.Fatalf("future branch blocked the validated prefix: %d, %v", n, err)
	}
}
func TestRound2QuarantinedLegacyRequiresCanonicalBackup(t *testing.T) {
	store, rows := round2Archive(t, pitrFixture(t, "incarnation-b.bin"))
	missing := sidecar.ManifestEntry{Name: "mysql-bin.legacy", PreviousGTIDs: retentionTestSID + ":1-10"}
	rows = append(rows, missing)
	round2Manifest(t, store, sidecar.Manifest{Site: "iad", Files: rows})
	if err := sidecar.QuarantineArchive(context.Background(), store, "binlogs", "iad", missing.Name, "", "lost before identity verification", retentionTestSID+":1-20", false); err != nil {
		t.Fatal(err)
	}
	if _, err := downloadSiteBinlogs(context.Background(), store, "binlogs", "iad", time.Now(), t.TempDir(), slog.Default()); err == nil {
		t.Fatal("quarantine silently applied to an older/unknown backup")
	}
	t.Setenv("BLOODRAVEN_PITR_START_GTIDS", retentionTestSID+":1-20")
	n, err := downloadSiteBinlogs(context.Background(), store, "binlogs", "iad", time.Now(), t.TempDir(), slog.Default())
	if err != nil || n != 1 {
		t.Fatalf("safe remedy did not restore PITR: %d, %v", n, err)
	}
}
func TestRound2RejectsGTIDSelfOverlap(t *testing.T) {
	data := bytes.Clone(pitrFixture(t, "incarnation-a.bin"))
	for offset := 4; offset < len(data); {
		size := int(binary.LittleEndian.Uint32(data[offset+9 : offset+13]))
		if data[offset+4] == 33 {
			binary.LittleEndian.PutUint64(data[offset+19+17:], 5)
			binary.LittleEndian.PutUint32(data[offset+size-4:offset+size], crc32.ChecksumIEEE(data[offset:offset+size-4]))
			break
		}
		offset += size
	}
	store, _ := round2Archive(t, data)
	_, err := downloadSiteBinlogs(context.Background(), store, "binlogs", "iad", time.Now(), t.TempDir(), slog.Default())
	if err == nil || !strings.Contains(err.Error(), "T intersects P") {
		t.Fatalf("reused GTID namespace was accepted: %v", err)
	}
}
func TestRound2LowerGTIDCutSkipsCoveredFiles(t *testing.T) {
	store, _ := round2Archive(t, pitrFixture(t, "incarnation-a.bin"), pitrFixture(t, "review-clock-b.bin"))
	t.Setenv("BLOODRAVEN_PITR_START_GTIDS", retentionTestSID+":1-11")
	n, err := downloadSiteBinlogs(context.Background(), store, "binlogs", "iad", time.Date(2026, 4, 15, 9, 0, 0, 0, time.UTC), t.TempDir(), slog.Default())
	if err != nil || n != 1 {
		t.Fatalf("lower cut: %d, %v", n, err)
	}
}
func TestRound2QuarantineCommandRequiresConfirmation(t *testing.T) {
	err := pitrQuarantine(context.Background(), []string{"--site", "iad", "--name", "mysql-bin.000001", "--id", "legacy"}, io.Discard, io.Discard)
	if err == nil {
		t.Fatal("metadata mutation accepted without explicit confirmation")
	}
	if err := pitrQuarantine(context.Background(), []string{"--help"}, io.Discard, io.Discard); err != nil {
		t.Fatal(err)
	}
}

// A digest-proven replacement must be usable even when an older writer left
// its missing legacy alias in the manifest.
func TestRereviewProbeMissingLegacyRepairRestores(t *testing.T) {
	store, rows := round2Archive(t, pitrFixture(t, "incarnation-a.bin"))
	legacy := rows[0]
	legacy.ContentID, legacy.ID = legacy.ID, ""
	rows = append(rows, legacy)
	round2Manifest(t, store, sidecar.Manifest{Site: "iad", Files: rows})
	n, err := downloadSiteBinlogs(context.Background(), store, "binlogs", "iad", time.Now(), t.TempDir(), slog.Default())
	if err != nil || n != 1 {
		t.Fatalf("repaired archive still cannot be restored (%d files): %v", n, err)
	}
}

type lowerCutStageObserver struct {
	sidecar.ArchiveStore
	dir, nextKey string
	t            *testing.T
}

func (s *lowerCutStageObserver) Open(ctx context.Context, key string) (io.ReadCloser, error) {
	if key == s.nextKey {
		parts, err := filepath.Glob(filepath.Join(s.dir, "iad", ".entry-*.part"))
		if err != nil {
			s.t.Fatal(err)
		}
		if len(parts) > 0 {
			s.t.Fatalf("covered history accumulated in staging: %v", parts)
		}
	}
	return s.ArchiveStore.Open(ctx, key)
}
func TestRound2LowerCutReleasesStagingImmediately(t *testing.T) {
	store, rows := round2Archive(t, pitrFixture(t, "incarnation-a.bin"), pitrFixture(t, "review-order-b.bin"))
	dir := t.TempDir()
	key, err := sidecar.ArchiveEntryKey("binlogs", "iad", rows[1])
	if err != nil {
		t.Fatal(err)
	}
	observer := &lowerCutStageObserver{ArchiveStore: store, dir: dir, nextKey: key, t: t}
	n, err := downloadPITRWindow(context.Background(), observer, "binlogs", "iad", time.Now(), dir, retentionTestSID+":1-11", slog.Default())
	if err != nil || n != 1 {
		t.Fatalf("lower cut: files=%d error=%v", n, err)
	}
}

func TestRound2QuarantineCommandAppliesAndUndoesPVCMetadata(t *testing.T) {
	ctx := context.Background()
	root := t.TempDir()
	t.Setenv("BLOODRAVEN_PITR_STORAGE_TYPE", "PVC")
	t.Setenv("BLOODRAVEN_PITR_PVC_MOUNT_PATH", root)
	t.Setenv("BLOODRAVEN_PITR_MANIFEST_PREFIX", "binlogs")
	t.Setenv("BLOODRAVEN_PITR_PASSPHRASE_FILE", "")
	cfg, err := pitrDownloadConfigFromEnv()
	if err != nil {
		t.Fatal(err)
	}
	store, err := sidecar.NewArchiveStore(ctx, cfg)
	if err != nil {
		t.Fatal(err)
	}
	row := sidecar.ManifestEntry{Name: "mysql-bin.legacy"}
	round2Manifest(t, store, sidecar.Manifest{Site: "iad", Files: []sidecar.ManifestEntry{row}})
	args := []string{"--site=iad", "--name=" + row.Name, "--id=legacy", "--reason=lost object; canonical backup verified", "--resume-gtids=" + retentionTestSID + ":1-20", "--confirm"}
	if err := pitrQuarantine(ctx, args, io.Discard, io.Discard); err != nil {
		t.Fatal(err)
	}
	m, err := sidecar.LoadManifest(ctx, store, "binlogs", "iad")
	if err != nil || len(m.Files) != 0 || len(m.Quarantined) != 1 {
		t.Fatalf("command failed to quarantine metadata: %+v, %v", m, err)
	}
	if err := pitrQuarantine(ctx, []string{"--site=iad", "--name=" + row.Name, "--id=legacy", "--undo", "--confirm"}, io.Discard, io.Discard); err != nil {
		t.Fatal(err)
	}
	m, err = sidecar.LoadManifest(ctx, store, "binlogs", "iad")
	if err != nil || len(m.Files) != 1 || len(m.Quarantined) != 0 {
		t.Fatalf("command failed to undo quarantine: %+v, %v", m, err)
	}
}
