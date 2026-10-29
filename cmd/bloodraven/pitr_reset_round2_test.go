package main

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"hash/crc32"
	"log/slog"
	"os"
	"path/filepath"
	"sort"
	"testing"
	"time"

	"github.com/shipstream/bloodraven/internal/sidecar"
)

func TestReReviewNumberResetWithoutRetainedNameCollision(t *testing.T) {
	ctx := context.Background()
	store, err := sidecar.NewArchiveStore(ctx, &sidecar.PITRConfig{StorageType: "PVC", PVC: &sidecar.PITRPVCConfig{MountPath: t.TempDir()}})
	if err != nil {
		t.Fatal(err)
	}
	m := sidecar.Manifest{Version: 1, Site: "iad"}
	var original []byte
	for i, fixture := range []string{"review-order-a.bin", "review-order-b.bin"} {
		data, err := os.ReadFile(filepath.Join("../../internal/sidecar/testdata", fixture))
		if err != nil {
			t.Fatal(err)
		}
		// Make the successor's real event clock one second later as well.
		for offset := 4; i == 1 && offset < len(data); {
			size := int(binary.LittleEndian.Uint32(data[offset+9 : offset+13]))
			event := data[offset : offset+size]
			binary.LittleEndian.PutUint32(event[:4], binary.LittleEndian.Uint32(event[:4])+1)
			binary.LittleEndian.PutUint32(event[size-4:], crc32.ChecksumIEEE(event[:size-4]))
			offset += size
		}
		if i == 0 {
			original = data
		}
		hash := sha256.Sum256(data)
		entry := sidecar.ManifestEntry{Name: []string{"mysql-bin.000010", "mysql-bin.000001"}[i], ID: hex.EncodeToString(hash[:]), PreviousGTIDs: "aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee" + []string{":1-10", ":1-11"}[i], FirstEventTime: time.Date(2026, 4, 15, 9, 0, i, 0, time.UTC)}
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
	encoded, err := json.Marshal(m)
	if err != nil {
		t.Fatal(err)
	}
	if err := store.Put(ctx, sidecar.ManifestKey("binlogs", "iad"), bytes.NewReader(encoded), int64(len(encoded))); err != nil {
		t.Fatal(err)
	}
	out := t.TempDir()
	if _, err := downloadSiteBinlogs(ctx, store, "binlogs", "iad", time.Now(), out, slog.Default()); err != nil {
		t.Fatal(err)
	}
	files, err := filepath.Glob(filepath.Join(out, "iad", "mysql-bin.*"))
	if err != nil || len(files) != 2 {
		t.Fatalf("staged files: %v, %v", files, err)
	}
	sort.Strings(files)
	first, err := os.ReadFile(files[0])
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(first, original) {
		t.Fatalf("GTID 12 stages before its GTID 11 predecessor after a numbering reset without a retained name collision: %s", filepath.Base(files[0]))
	}
}
