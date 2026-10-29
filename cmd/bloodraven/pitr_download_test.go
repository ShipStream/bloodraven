package main

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"os"
	"path"
	"path/filepath"
	"slices"
	"testing"

	"github.com/shipstream/bloodraven/internal/sidecar"
	"time"
)

func TestParsePITRStopTime(t *testing.T) {
	cases := []struct {
		in      string
		wantErr bool
	}{
		{"2026-04-15T09:30:00Z", false},
		{"2026-04-15T09:30:00+00:00", false},
		{"2026-04-15 09:30:00", false},
		{"  2026-04-15T09:30:00Z  ", false}, // whitespace trim
		{"not a time", true},
		{"", true},
	}
	for _, tc := range cases {
		got, err := parsePITRStopTime(tc.in)
		if (err != nil) != tc.wantErr {
			t.Errorf("parsePITRStopTime(%q) err=%v wantErr=%v", tc.in, err, tc.wantErr)
			continue
		}
		if !tc.wantErr && got.Location() != time.UTC {
			t.Errorf("parsePITRStopTime(%q) returned non-UTC time %s", tc.in, got)
		}
	}
}

func TestPitrDownloadConfigFromEnv_S3(t *testing.T) {
	t.Setenv("BLOODRAVEN_PITR_STORAGE_TYPE", "S3")
	t.Setenv("BLOODRAVEN_PITR_MANIFEST_PREFIX", "orders/binlogs")
	t.Setenv("BLOODRAVEN_PITR_S3_BUCKET", "my-bucket")
	t.Setenv("BLOODRAVEN_PITR_PASSPHRASE_FILE", "/etc/bloodraven/passphrase")
	cfg, err := pitrDownloadConfigFromEnv()
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if cfg.StorageType != "S3" || cfg.S3 == nil || cfg.S3.Bucket != "my-bucket" {
		t.Errorf("unexpected cfg: %+v", cfg)
	}
	if cfg.PassphraseFile != "/etc/bloodraven/passphrase" {
		t.Errorf("PassphraseFile = %q, want /etc/bloodraven/passphrase", cfg.PassphraseFile)
	}
}

func TestPitrDownloadConfigFromEnv_PVC_MissingMount(t *testing.T) {
	t.Setenv("BLOODRAVEN_PITR_STORAGE_TYPE", "PVC")
	t.Setenv("BLOODRAVEN_PITR_MANIFEST_PREFIX", "orders/binlogs")
	t.Setenv("BLOODRAVEN_PITR_PVC_MOUNT_PATH", "")
	if _, err := pitrDownloadConfigFromEnv(); err == nil {
		t.Errorf("expected error for missing PVC mount")
	}
}

func TestPitrDownloadConfigFromEnv_UnknownType(t *testing.T) {
	t.Setenv("BLOODRAVEN_PITR_STORAGE_TYPE", "gcs")
	if _, err := pitrDownloadConfigFromEnv(); err == nil {
		t.Errorf("expected error for unknown storage type")
	}
}

func TestDiscoverSitesAndDownloadPVCArchive(t *testing.T) {
	ctx := context.Background()
	store, err := sidecar.NewArchiveStore(ctx, &sidecar.PITRConfig{
		StorageType: "PVC", PVC: &sidecar.PITRPVCConfig{MountPath: t.TempDir()},
	})
	if err != nil {
		t.Fatal(err)
	}
	prefix := "orders/binlogs"
	stop := time.Date(2026, 4, 15, 9, 30, 0, 0, time.UTC)
	for _, site := range []string{"iad", "pdx"} {
		name := "mysql-bin.000001"
		key := path.Join(prefix, site, name)
		payload := []byte("archived bytes for " + site)
		if err := store.Put(ctx, key, bytes.NewReader(payload), int64(len(payload))); err != nil {
			t.Fatal(err)
		}
		manifest := sidecar.Manifest{Version: sidecar.ManifestVersion, Site: site, Files: []sidecar.ManifestEntry{{
			Name: name, RemotePath: key, Size: int64(len(payload)), FirstEventTime: stop.Add(-time.Hour),
		}}}
		data, err := json.Marshal(manifest)
		if err != nil {
			t.Fatal(err)
		}
		if err := store.Put(ctx, sidecar.ManifestKey(prefix, site), bytes.NewReader(data), int64(len(data))); err != nil {
			t.Fatal(err)
		}
	}
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	t.Run("explicit site control", func(t *testing.T) {
		out := t.TempDir()
		n, err := downloadSiteBinlogs(ctx, store, prefix, "iad", stop, out, logger)
		if err != nil || n != 1 {
			t.Fatalf("explicit download = %d, %v", n, err)
		}
		data, err := os.ReadFile(filepath.Join(out, "iad", "mysql-bin.000001"))
		if err != nil || string(data) != "archived bytes for iad" {
			t.Fatalf("explicit downloaded bytes = %q, %v", data, err)
		}
	})
	t.Run("discover then download", func(t *testing.T) {
		sites, err := discoverSites(ctx, store, prefix)
		if err != nil {
			t.Fatal(err)
		}
		slices.Sort(sites)
		if !slices.Equal(sites, []string{"iad", "pdx"}) {
			t.Fatalf("discovered sites = %v, want [iad pdx]", sites)
		}
		out := t.TempDir()
		for _, site := range sites {
			n, err := downloadSiteBinlogs(ctx, store, prefix, site, stop, out, logger)
			if err != nil || n != 1 {
				t.Fatalf("download %s = %d, %v", site, n, err)
			}
			data, err := os.ReadFile(filepath.Join(out, site, "mysql-bin.000001"))
			if err != nil || string(data) != "archived bytes for "+site {
				t.Fatalf("downloaded %s bytes = %q, %v", site, data, err)
			}
		}
	})
}
