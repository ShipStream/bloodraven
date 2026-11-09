package sidecar

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/shipstream/bloodraven/internal/pitrarchive"
)

func TestNormalizeFenceDurations(t *testing.T) {
	tests := []struct {
		name      string
		lease     time.Duration
		peer      time.Duration
		wantLease time.Duration
		wantPeer  time.Duration
	}{
		{"defaults unchanged", 20 * time.Second, 5 * time.Second, 20 * time.Second, 5 * time.Second},
		{"peer clamps to one second", 20 * time.Second, 0, 20 * time.Second, time.Second},
		{"negative peer clamps", 20 * time.Second, -time.Second, 20 * time.Second, time.Second},
		{"lease clamps to three seconds", 0, time.Second, 3 * time.Second, time.Second},
		{"lease clamps to ratio", 5 * time.Second, 4 * time.Second, 12 * time.Second, 4 * time.Second},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			gotLease, gotPeer := normalizeFenceDurations(tt.lease, tt.peer)
			if gotLease != tt.wantLease || gotPeer != tt.wantPeer {
				t.Fatalf("normalizeFenceDurations() = (%s, %s), want (%s, %s)", gotLease, gotPeer, tt.wantLease, tt.wantPeer)
			}
		})
	}
}

func TestFencingEffectiveTimeoutBeforeDNSExpiry(t *testing.T) {
	for _, tt := range []struct {
		name, lease, peer string
		wantLease         time.Duration
		beforeDNS         bool
	}{
		{"production defaults", "", "", 20 * time.Second, true},
		{"minimum clamp", "0s", "0s", 3 * time.Second, true},
		{"ratio below DNS TTL", "20s", "19s", 57 * time.Second, true},
		{"ratio reaches DNS TTL", "20s", "20s", 60 * time.Second, false},
		{"configured reaches DNS TTL", "60s", "5s", 60 * time.Second, false},
	} {
		t.Run(tt.name, func(t *testing.T) {
			t.Setenv("MYSQL_USER", "root")
			t.Setenv("MYSQL_PASSWORD", "pass")
			t.Setenv("LEASE_TIMEOUT", tt.lease)
			t.Setenv("PEER_CHECK_INTERVAL", tt.peer)
			cfg, err := ConfigFromEnv()
			if err != nil {
				t.Fatal(err)
			}
			if cfg.LeaseTimeout != tt.wantLease {
				t.Fatalf("effective timeout = %s, want %s", cfg.LeaseTimeout, tt.wantLease)
			}
			if beforeDNS := cfg.LeaseTimeout < 60*time.Second; beforeDNS != tt.beforeDNS {
				t.Fatalf("effective timeout %s < 60s = %t, want %t", cfg.LeaseTimeout, beforeDNS, tt.beforeDNS)
			}
		})
	}
}

func TestConfigFromEnv_MYSQL_USER_PASSWORD(t *testing.T) {
	t.Setenv("MYSQL_USER", "operator")
	t.Setenv("MYSQL_PASSWORD", "secret")

	cfg, err := ConfigFromEnv()
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if cfg.MysqlDSN != "operator:secret@tcp(127.0.0.1:3306)/" {
		t.Errorf("unexpected DSN: %s", cfg.MysqlDSN)
	}
}

// The operator sets the MySQL password to the Secret's exact bytes, so the
// mounted files must not be trimmed on the way into the DSN.
func TestConfigFromEnv_MYSQL_CREDS_DIRKeepsExactBytes(t *testing.T) {
	dir := t.TempDir()
	if err := os.WriteFile(filepath.Join(dir, "username"), []byte("operator"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, "password"), []byte(" secret\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	t.Setenv("MYSQL_CREDS_DIR", dir)

	cfg, err := ConfigFromEnv()
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if cfg.MysqlDSN != "operator: secret\n@tcp(127.0.0.1:3306)/" {
		t.Errorf("unexpected DSN: %q", cfg.MysqlDSN)
	}
}

// The sidecar no longer reads a whole DSN (the removed spec.secretName
// mode): MYSQL_DSN alone is not a credential.
func TestConfigFromEnv_NeitherSet(t *testing.T) {
	t.Setenv("MYSQL_DSN", "root:pass@tcp(127.0.0.1:3306)/")
	t.Setenv("MYSQL_CREDS_DIR", "")
	t.Setenv("MYSQL_USER", "")
	t.Setenv("MYSQL_PASSWORD", "")

	_, err := ConfigFromEnv()
	if err == nil {
		t.Fatal("expected error when neither MYSQL_CREDS_DIR nor MYSQL_USER is set")
	}
}

// The archiver writes, so its PVC store must probe write access; the
// probe uses the manifest prefix because WritePrefix is left empty.
func TestPITRConfigFromEnvPVCIsWritable(t *testing.T) {
	t.Setenv("BLOODRAVEN_PITR_ENABLED", "1")
	t.Setenv("BLOODRAVEN_PITR_STORAGE_TYPE", "PVC")
	t.Setenv("BLOODRAVEN_PITR_PVC_MOUNT_PATH", "/run/bloodraven/pitr-archive")
	t.Setenv("BLOODRAVEN_PITR_MANIFEST_PREFIX", "orders/binlogs")
	setPITRIdentityEnv(t, "pvc://ns/archive/orders/binlogs", "None")
	cfg, err := pitrConfigFromEnv()
	if err != nil {
		t.Fatal(err)
	}
	if cfg == nil || cfg.PVC == nil {
		t.Fatalf("pitrConfigFromEnv = %+v, want a PVC config", cfg)
	}
	if !cfg.PVC.Writable || cfg.PVC.WritePrefix != "" || cfg.PVC.MountVerifier != nil {
		t.Fatalf("PVC config = %+v, want Writable, no WritePrefix override and the production mount check", cfg.PVC)
	}
	if cfg.ManifestPrefix != "orders/binlogs" {
		t.Fatalf("ManifestPrefix = %q", cfg.ManifestPrefix)
	}
}

const pitrConfigTestUID = "00000000-0000-4000-8000-0000000000aa"

func setPITRIdentityEnv(t *testing.T, destination, mode string) {
	t.Helper()
	t.Setenv("POD_NAMESPACE", "ns")
	t.Setenv("FAILOVER_GROUP", "orders")
	t.Setenv("BLOODRAVEN_FAILOVER_GROUP_UID", pitrConfigTestUID)
	t.Setenv("BLOODRAVEN_PITR_DESTINATION", destination)
	t.Setenv("BLOODRAVEN_PITR_ENCRYPTION_MODE", mode)
	if d, err := pitrarchive.ParseDestination(destination); err == nil {
		t.Setenv("BLOODRAVEN_PITR_ARCHIVE_ID", pitrarchive.CreateArchiveID(pitrConfigTestUID, d))
	}
}

// The writer's identity env must agree with the store it configures, so a
// sidecar can only ever write where its identity says it does (§3.7).
func TestPITRConfigArchiveIdentityFromEnv(t *testing.T) {
	s3Dest := "s3://bucket/orders/binlogs?endpoint=https%3A%2F%2Fminio.example"
	tests := []struct {
		name     string
		env      map[string]string
		wantErr  string
		wantMode pitrarchive.EncryptionMode
	}{
		{name: "s3 none", env: map[string]string{"BLOODRAVEN_PITR_DESTINATION": s3Dest}, wantMode: pitrarchive.EncryptionNone},
		{name: "s3 passphrase", env: map[string]string{"BLOODRAVEN_PITR_DESTINATION": s3Dest, "BLOODRAVEN_PITR_ENCRYPTION_MODE": "Passphrase", "BLOODRAVEN_PITR_PASSPHRASE_FILE": "/run/key"}, wantMode: pitrarchive.EncryptionPassphrase},
		{name: "passphrase without file", env: map[string]string{"BLOODRAVEN_PITR_DESTINATION": s3Dest, "BLOODRAVEN_PITR_ENCRYPTION_MODE": "Passphrase"}, wantErr: "passphrase file"},
		{name: "file without passphrase mode", env: map[string]string{"BLOODRAVEN_PITR_DESTINATION": s3Dest, "BLOODRAVEN_PITR_PASSPHRASE_FILE": "/run/key"}, wantErr: "passphrase file"},
		{name: "unknown mode", env: map[string]string{"BLOODRAVEN_PITR_DESTINATION": s3Dest, "BLOODRAVEN_PITR_ENCRYPTION_MODE": "Maybe"}, wantErr: "BLOODRAVEN_PITR_ENCRYPTION_MODE"},
		{name: "no destination", env: map[string]string{"BLOODRAVEN_PITR_DESTINATION": ""}, wantErr: "BLOODRAVEN_PITR_DESTINATION is required"},
		{name: "non-canonical destination", env: map[string]string{"BLOODRAVEN_PITR_DESTINATION": "s3://bucket//orders/binlogs?endpoint=https%3A%2F%2Fminio.example"}, wantErr: "parse BLOODRAVEN_PITR_DESTINATION"},
		{name: "other bucket", env: map[string]string{"BLOODRAVEN_PITR_DESTINATION": "s3://other/orders/binlogs?endpoint=https%3A%2F%2Fminio.example"}, wantErr: "S3 bucket"},
		{name: "other endpoint", env: map[string]string{"BLOODRAVEN_PITR_DESTINATION": "s3://bucket/orders/binlogs?endpoint=aws"}, wantErr: "endpoint"},
		{name: "other prefix", env: map[string]string{"BLOODRAVEN_PITR_DESTINATION": "s3://bucket/binlogs?endpoint=https%3A%2F%2Fminio.example"}, wantErr: "key prefix"},
		{name: "pvc destination for s3", env: map[string]string{"BLOODRAVEN_PITR_DESTINATION": "pvc://ns/archive/orders/binlogs"}, wantErr: "S3 bucket"},
		{name: "no uid", env: map[string]string{"BLOODRAVEN_PITR_DESTINATION": s3Dest, "BLOODRAVEN_FAILOVER_GROUP_UID": ""}, wantErr: "UID"},
		{name: "archive id of another derivation", env: map[string]string{"BLOODRAVEN_PITR_DESTINATION": s3Dest, "BLOODRAVEN_PITR_ARCHIVE_ID": "ar1-aaaaaaaaaaaaaaaaaaaaaaaaaa"}, wantErr: "not derived"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Setenv("BLOODRAVEN_PITR_ENABLED", "1")
			t.Setenv("BLOODRAVEN_PITR_STORAGE_TYPE", "S3")
			t.Setenv("BLOODRAVEN_PITR_S3_BUCKET", "bucket")
			t.Setenv("BLOODRAVEN_PITR_S3_ENDPOINT_URL", "https://MINIO.example:443/")
			t.Setenv("BLOODRAVEN_PITR_MANIFEST_PREFIX", "orders/binlogs")
			t.Setenv("BLOODRAVEN_PITR_PASSPHRASE_FILE", "")
			t.Setenv("BLOODRAVEN_PITR_KEY_REVISION", "mysql-orders-pitr-key-0123456789")
			setPITRIdentityEnv(t, s3Dest, "None")
			for k, v := range tt.env {
				t.Setenv(k, v)
			}
			if _, ok := tt.env["BLOODRAVEN_PITR_ARCHIVE_ID"]; !ok && os.Getenv("BLOODRAVEN_PITR_DESTINATION") != s3Dest {
				t.Setenv("BLOODRAVEN_PITR_ARCHIVE_ID", "")
			}
			cfg, err := pitrConfigFromEnv()
			if tt.wantErr != "" {
				if err == nil || !strings.Contains(err.Error(), tt.wantErr) {
					t.Fatalf("err = %v, want it to mention %q", err, tt.wantErr)
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			id := cfg.Archive
			if id.Source != (pitrarchive.Source{Namespace: "ns", Name: "orders", UID: pitrConfigTestUID}) || id.Destination.String() != s3Dest ||
				id.Encryption.Mode != tt.wantMode || id.KeyRevision != "mysql-orders-pitr-key-0123456789" {
				t.Fatalf("identity = %+v", id)
			}
			if tt.wantMode == pitrarchive.EncryptionPassphrase && id.Encryption.Algorithm != pitrarchive.AlgorithmAES256GCM {
				t.Fatalf("algorithm = %q", id.Encryption.Algorithm)
			}
		})
	}
}

// F215: the PVC identity guards (destination kind and namespace) refuse.
func TestPITRConfigArchiveIdentityPVCGuards(t *testing.T) {
	tests := []struct {
		name, destination, wantErr string
	}{
		{name: "pvc ok", destination: "pvc://ns/archive/orders/binlogs"},
		{name: "s3 destination for pvc", destination: "s3://bucket/orders/binlogs?endpoint=aws", wantErr: "is not a PVC destination"},
		{name: "pvc in another namespace", destination: "pvc://other/archive/orders/binlogs", wantErr: `is in namespace "other", not "ns"`},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Setenv("BLOODRAVEN_PITR_ENABLED", "1")
			t.Setenv("BLOODRAVEN_PITR_STORAGE_TYPE", "PVC")
			t.Setenv("BLOODRAVEN_PITR_PVC_MOUNT_PATH", "/run/bloodraven/pitr-archive")
			t.Setenv("BLOODRAVEN_PITR_MANIFEST_PREFIX", "orders/binlogs")
			t.Setenv("BLOODRAVEN_PITR_PASSPHRASE_FILE", "")
			setPITRIdentityEnv(t, tt.destination, "None")
			cfg := &PITRConfig{StorageType: "PVC", ManifestPrefix: "orders/binlogs", PVC: &PITRPVCConfig{MountPath: "/run/bloodraven/pitr-archive"}}
			id, err := ArchiveIdentityFromEnv(cfg)
			if tt.wantErr == "" {
				if err != nil || id.Destination.String() != tt.destination {
					t.Fatalf("ArchiveIdentityFromEnv = %+v, %v", id, err)
				}
				return
			}
			if err == nil || !strings.Contains(err.Error(), tt.wantErr) {
				t.Fatalf("err = %v, want it to mention %q", err, tt.wantErr)
			}
		})
	}
}
