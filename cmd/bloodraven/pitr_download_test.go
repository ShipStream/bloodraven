package main

import (
	"testing"
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
