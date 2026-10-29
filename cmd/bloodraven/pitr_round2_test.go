package main

import (
	"bytes"
	"context"
	"log/slog"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/shipstream/bloodraven/internal/sidecar"
)

// Port the rereview probes through the actual downloader with native captures,
// including the purged donor base of the final physically cloned incarnation.
func downloadNativeProbe(t *testing.T, indices ...int) {
	t.Helper()
	rows, payloads, _ := nativePITRRows(t)
	ctx := context.Background()
	store, err := sidecar.NewArchiveStore(ctx, &sidecar.PITRConfig{StorageType: "PVC", PVC: &sidecar.PITRPVCConfig{MountPath: t.TempDir()}})
	if err != nil {
		t.Fatal(err)
	}
	m := sidecar.Manifest{Site: "iad"}
	for _, i := range indices {
		key, err := sidecar.ArchiveEntryKey("binlogs", "iad", rows[i])
		if err != nil {
			t.Fatal(err)
		}
		if err := store.Put(ctx, key, bytes.NewReader(payloads[i]), int64(len(payloads[i]))); err != nil {
			t.Fatal(err)
		}
		m.Files = append(m.Files, rows[i])
	}
	round2Manifest(t, store, m)
	out := t.TempDir()
	n, err := downloadSiteBinlogs(ctx, store, "binlogs", "iad", time.Date(2030, 1, 1, 0, 0, 0, 0, time.UTC), out, slog.Default())
	if err != nil || n != len(indices) {
		t.Fatalf("valid history rejected: files=%d error=%v", n, err)
	}
	files, err := filepath.Glob(filepath.Join(out, "iad", "mysql-bin.*"))
	if err != nil {
		t.Fatal(err)
	}
	for i, file := range files {
		got, err := os.ReadFile(file)
		if err != nil || !bytes.Equal(got, payloads[indices[i]]) {
			t.Fatalf("causal replay differs at %d: %v", i, err)
		}
	}
}
func TestRereviewProbeFreshFirstBinlogWithReuse(t *testing.T) {
	rows, _, _ := nativePITRRows(t)
	downloadNativeProbe(t, 0, 1, len(rows)-1)
}
func TestRereviewProbeEmptyRotationWithReuse(t *testing.T) {
	rows, _, _ := nativePITRRows(t)
	downloadNativeProbe(t, 0, 1, 2, len(rows)-1)
}
