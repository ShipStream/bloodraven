package main

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"log/slog"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/shipstream/bloodraven/internal/sidecar"
)

func TestReviewDownloadOrdersGTIDPredecessorsBeforeSuccessors(t *testing.T) {
	ctx := context.Background()
	store, err := sidecar.NewArchiveStore(ctx, &sidecar.PITRConfig{StorageType: "PVC", PVC: &sidecar.PITRPVCConfig{MountPath: t.TempDir()}})
	if err != nil {
		t.Fatal(err)
	}
	first := time.Date(2026, 4, 15, 9, 0, 0, 0, time.UTC)
	m := sidecar.Manifest{Version: 1, Site: "iad"}
	var contents [][]byte
	for i, name := range []string{"review-order-a.bin", "review-order-b.bin"} {
		data, err := os.ReadFile(filepath.Join("../../internal/sidecar/testdata", name))
		if err != nil {
			t.Fatal(err)
		}
		contents = append(contents, data)
		hash := sha256.Sum256(data)
		entry := sidecar.ManifestEntry{Name: "mysql-bin.000001", ID: hex.EncodeToString(hash[:]), FirstEventTime: first, PreviousGTIDs: "aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee" + []string{":1-10", ":1-11"}[i], ArchivedAt: first.Add(time.Duration(i) * time.Minute)}
		key, err := sidecar.ArchiveEntryKey("binlogs", "iad", entry)
		if err != nil {
			t.Fatal(err)
		}
		entry.RemotePath = key
		if err := store.Put(ctx, key, bytes.NewReader(data), int64(len(data))); err != nil {
			t.Fatal(err)
		}
		m.Files = append(m.Files, entry)
	}
	data, err := json.Marshal(m)
	if err != nil {
		t.Fatal(err)
	}
	if err := store.Put(ctx, sidecar.ManifestKey("binlogs", "iad"), bytes.NewReader(data), int64(len(data))); err != nil {
		t.Fatal(err)
	}
	out := t.TempDir()
	if _, err := downloadSiteBinlogs(ctx, store, "binlogs", "iad", first.Add(time.Hour), out, slog.Default()); err != nil {
		t.Fatal(err)
	}
	files, err := filepath.Glob(filepath.Join(out, "iad", "mysql-bin.*"))
	if err != nil {
		t.Fatal(err)
	}
	sort.Strings(files)
	got, err := os.ReadFile(files[0])
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(got, contents[0]) {
		t.Errorf("replay starts with GTID 12 (PreviousGTIDs 1-11) before its GTID 11 predecessor; SHA ordering reversed real CRC32 binlogs with equal first timestamps")
	}
}

func TestReviewDownloadKeepsFileSequenceAcrossClockRollback(t *testing.T) {
	ctx := context.Background()
	store, err := sidecar.NewArchiveStore(ctx, &sidecar.PITRConfig{StorageType: "PVC", PVC: &sidecar.PITRPVCConfig{MountPath: t.TempDir()}})
	if err != nil {
		t.Fatal(err)
	}
	first := time.Date(2026, 4, 15, 9, 0, 0, 0, time.UTC)
	m := sidecar.Manifest{Version: 1, Site: "iad"}
	var contents [][]byte
	for i, name := range []string{"review-order-a.bin", "review-clock-b.bin"} {
		data, err := os.ReadFile(filepath.Join("../../internal/sidecar/testdata", name))
		if err != nil {
			t.Fatal(err)
		}
		contents = append(contents, data)
		hash := sha256.Sum256(data)
		entry := sidecar.ManifestEntry{Name: []string{"mysql-bin.000001", "mysql-bin.000002"}[i], ID: hex.EncodeToString(hash[:]), FirstEventTime: first.Add(-time.Duration(i) * time.Second), PreviousGTIDs: "aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee" + []string{":1-10", ":1-11"}[i], ArchivedAt: first.Add(time.Duration(i) * time.Minute)}
		key, err := sidecar.ArchiveEntryKey("binlogs", "iad", entry)
		if err != nil {
			t.Fatal(err)
		}
		entry.RemotePath = key
		if err := store.Put(ctx, key, bytes.NewReader(data), int64(len(data))); err != nil {
			t.Fatal(err)
		}
		m.Files = append(m.Files, entry)
	}
	data, err := json.Marshal(m)
	if err != nil {
		t.Fatal(err)
	}
	if err := store.Put(ctx, sidecar.ManifestKey("binlogs", "iad"), bytes.NewReader(data), int64(len(data))); err != nil {
		t.Fatal(err)
	}
	out := t.TempDir()
	if _, err := downloadSiteBinlogs(ctx, store, "binlogs", "iad", first.Add(time.Hour), out, slog.Default()); err != nil {
		t.Fatal(err)
	}
	files, err := filepath.Glob(filepath.Join(out, "iad", "mysql-bin.*"))
	if err != nil {
		t.Fatal(err)
	}
	sort.Strings(files)
	got, err := os.ReadFile(files[0])
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(got, contents[0]) {
		t.Errorf("replay starts with GTID 12 (PreviousGTIDs 1-11) before its GTID 11 predecessor; timestamp ordering reversed real CRC32 binlogs whose event clock stepped backward by one second")
	}
}

func TestReviewDownloadRecoversIDStrippedRows(t *testing.T) {
	ctx := context.Background()
	store, err := sidecar.NewArchiveStore(ctx, &sidecar.PITRConfig{StorageType: "PVC", PVC: &sidecar.PITRPVCConfig{MountPath: t.TempDir()}})
	if err != nil {
		t.Fatal(err)
	}
	row := sidecar.ManifestEntry{Name: "mysql-bin.000001", RemotePath: "binlogs/iad/mysql-bin.000001." + pitrFixtureID(pitrFixture(t, "incarnation-a.bin"))}
	if err := store.Put(ctx, row.RemotePath, bytes.NewReader(pitrFixture(t, "incarnation-a.bin")), int64(len(pitrFixture(t, "incarnation-a.bin")))); err != nil {
		t.Fatal(err)
	}
	data, _ := json.Marshal(sidecar.Manifest{Files: []sidecar.ManifestEntry{row}})
	if err := store.Put(ctx, sidecar.ManifestKey("binlogs", "iad"), bytes.NewReader(data), int64(len(data))); err != nil {
		t.Fatal(err)
	}
	n, err := downloadSiteBinlogs(ctx, store, "binlogs", "iad", time.Now(), t.TempDir(), slog.Default())
	if err != nil || n != 1 {
		t.Fatalf("ID-stripped row cannot be restored: %d, %v", n, err)
	}
}

func TestReviewLegacyStreamKeepsSequenceAcrossClockRollback(t *testing.T) {
	ctx := context.Background()
	store, err := sidecar.NewArchiveStore(ctx, &sidecar.PITRConfig{StorageType: "PVC", PVC: &sidecar.PITRPVCConfig{MountPath: t.TempDir()}})
	if err != nil {
		t.Fatal(err)
	}
	m := sidecar.Manifest{Site: "iad"}
	first := time.Date(2026, 4, 15, 9, 0, 0, 0, time.UTC)
	for i, name := range []string{"mysql-bin.000010", "mysql-bin.000011"} {
		key := "binlogs/iad/" + name
		if err := store.Put(ctx, key, bytes.NewReader(pitrFixture(t, []string{"review-order-a.bin", "review-clock-b.bin"}[i])), int64(len(pitrFixture(t, []string{"review-order-a.bin", "review-clock-b.bin"}[i])))); err != nil {
			t.Fatal(err)
		}
		m.Files = append(m.Files, sidecar.ManifestEntry{Name: name, FirstEventTime: first.Add(-time.Duration(i) * time.Minute), RemotePath: key})
	}
	data, _ := json.Marshal(m)
	if err := store.Put(ctx, sidecar.ManifestKey("binlogs", "iad"), bytes.NewReader(data), int64(len(data))); err != nil {
		t.Fatal(err)
	}
	out := t.TempDir()
	if _, err := downloadSiteBinlogs(ctx, store, "binlogs", "iad", first.Add(time.Hour), out, slog.Default()); err != nil {
		t.Fatal(err)
	}
	files, err := filepath.Glob(filepath.Join(out, "iad", "mysql-bin.*"))
	if err != nil || len(files) != 2 {
		t.Fatalf("files: %v, %v", files, err)
	}
	got, err := os.ReadFile(files[0])
	if err != nil || !bytes.Equal(got, pitrFixture(t, "review-order-a.bin")) {
		t.Fatalf("legacy sequence reversed: %q, %v", got, err)
	}
}

func TestReviewDownloadRejectsAmbiguousIncarnations(t *testing.T) {
	for _, predecessors := range [][]string{{"", ""}, {"u:1-10", "u:1-10"}, {"u:1-10", "v:1-11"}, {"invalid", "u:1-10"}, {"u:1-5", "u:1-10", "u:1-10"}} {
		t.Run(strings.Join(predecessors, "/"), func(t *testing.T) {
			ctx := context.Background()
			store, err := sidecar.NewArchiveStore(ctx, &sidecar.PITRConfig{StorageType: "PVC", PVC: &sidecar.PITRPVCConfig{MountPath: t.TempDir()}})
			if err != nil {
				t.Fatal(err)
			}
			m := sidecar.Manifest{Site: "iad"}
			for i, prev := range predecessors {
				row := sidecar.ManifestEntry{Name: "mysql-bin.000001", ID: strings.Repeat(string(rune('a'+i)), 64), PreviousGTIDs: prev}
				if i > 1 {
					row.Name = "mysql-bin.000002"
				}
				m.Files = append(m.Files, row)
				key, err := sidecar.ArchiveEntryKey("binlogs", "iad", row)
				if err != nil {
					t.Fatal(err)
				}
				if err := store.Put(ctx, key, strings.NewReader("bytes"), 5); err != nil {
					t.Fatal(err)
				}
			}
			data, _ := json.Marshal(m)
			if err := store.Put(ctx, sidecar.ManifestKey("binlogs", "iad"), bytes.NewReader(data), int64(len(data))); err != nil {
				t.Fatal(err)
			}
			n, err := downloadSiteBinlogs(ctx, store, "binlogs", "iad", time.Now(), t.TempDir(), slog.Default())
			if err == nil || n != 0 {
				t.Fatalf("ambiguous history staged: %d, %v", n, err)
			}
		})
	}
}
