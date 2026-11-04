package main

import (
	"context"
	"io"
	"runtime"
	"strings"
	"testing"

	"github.com/shipstream/bloodraven/internal/sidecar"
)

// TestStoreConfigBuildersSetPVCAccessMode pins each subcommand's PVC
// access mode: readers (whose restore volumes are mounted read-only)
// must never probe write access, writers must.
func TestStoreConfigBuildersSetPVCAccessMode(t *testing.T) {
	tests := []struct {
		name            string
		build           func() (*sidecar.PITRConfig, error)
		wantWritable    bool
		wantWritePrefix string
		wantManifest    string
	}{
		{
			name:         "pitr-download reads",
			build:        pitrDownloadStoreConfig,
			wantManifest: "orders/binlogs",
		},
		{
			name:         "pitr-quarantine writes manifests",
			build:        pitrQuarantineStoreConfig,
			wantWritable: true,
			wantManifest: "orders/binlogs",
		},
		{
			name: "encrypt-upload writes the backup",
			build: func() (*sidecar.PITRConfig, error) {
				cfg, _, _, err := encryptUploadStoreConfig("PVC", "/backups/orders/backup-1")
				return cfg, err
			},
			wantWritable:    true,
			wantWritePrefix: "orders/backup-1",
		},
		{
			name: "decrypt-download reads",
			build: func() (*sidecar.PITRConfig, error) {
				cfg, _, _, err := decryptDownloadStoreConfig("PVC", "/backups/orders/backup-1")
				return cfg, err
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Setenv("BLOODRAVEN_PITR_STORAGE_TYPE", "PVC")
			t.Setenv("BLOODRAVEN_PITR_PVC_MOUNT_PATH", "/run/bloodraven/pitr-archive")
			t.Setenv("BLOODRAVEN_PITR_MANIFEST_PREFIX", "orders/binlogs")
			t.Setenv("BLOODRAVEN_PVC_MOUNT_PATH", "/backups")
			cfg, err := tt.build()
			if err != nil {
				t.Fatal(err)
			}
			if cfg.PVC == nil {
				t.Fatalf("config = %+v, want a PVC config", cfg)
			}
			if cfg.PVC.Writable != tt.wantWritable || cfg.PVC.WritePrefix != tt.wantWritePrefix {
				t.Fatalf("Writable=%v WritePrefix=%q, want %v %q", cfg.PVC.Writable, cfg.PVC.WritePrefix, tt.wantWritable, tt.wantWritePrefix)
			}
			if cfg.PVC.MountVerifier != nil {
				t.Fatal("a subcommand replaced the production mount check")
			}
			if cfg.ManifestPrefix != tt.wantManifest {
				t.Fatalf("ManifestPrefix = %q, want %q", cfg.ManifestPrefix, tt.wantManifest)
			}
		})
	}
}

// TestPITRQuarantineRejectsUnmountedArchive runs pitr-quarantine against
// the production mount check: a maintenance Pod without the archive
// volume must fail instead of writing manifests into its own filesystem.
func TestPITRQuarantineRejectsUnmountedArchive(t *testing.T) {
	if runtime.GOOS != "linux" {
		t.Skip("mountinfo is Linux-only")
	}
	t.Setenv("BLOODRAVEN_PITR_STORAGE_TYPE", "PVC")
	t.Setenv("BLOODRAVEN_PITR_PVC_MOUNT_PATH", t.TempDir())
	t.Setenv("BLOODRAVEN_PITR_MANIFEST_PREFIX", "binlogs")
	t.Setenv("BLOODRAVEN_PITR_PASSPHRASE_FILE", "")
	args := []string{"--site=iad", "--name=mysql-bin.000001", "--id=legacy", "--reason=test", "--confirm"}
	err := pitrQuarantine(context.Background(), args, io.Discard, io.Discard)
	if err == nil || !strings.Contains(err.Error(), "is not a mount point") {
		t.Fatalf("pitrQuarantine = %v, want a not-a-mount-point error", err)
	}
}
