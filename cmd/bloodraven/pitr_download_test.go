package main

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"io"
	"log/slog"
	"os"
	"os/exec"
	"path"
	"path/filepath"
	"slices"
	"testing"
	"time"

	"github.com/shipstream/bloodraven/internal/sidecar"
)

func pitrFixture(t *testing.T, name string) []byte {
	t.Helper()
	data, err := os.ReadFile(filepath.Join("../../internal/sidecar/testdata", name))
	if err != nil {
		t.Fatal(err)
	}
	return data
}
func pitrFixtureID(data []byte) string {
	hash := sha256.Sum256(data)
	return hex.EncodeToString(hash[:])
}

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
		payload := pitrFixture(t, "incarnation-a.bin")
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
		files, err := filepath.Glob(filepath.Join(out, "iad", "mysql-bin.*"))
		if err != nil || len(files) != 1 {
			t.Fatalf("downloaded files: %v, %v", files, err)
		}
		data, err := os.ReadFile(files[0])
		if err != nil || !bytes.Equal(data, pitrFixture(t, "incarnation-a.bin")) {
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
			files, err := filepath.Glob(filepath.Join(out, site, "mysql-bin.*"))
			if err != nil || len(files) != 1 {
				t.Fatalf("downloaded files: %v, %v", files, err)
			}
			data, err := os.ReadFile(files[0])
			if err != nil || !bytes.Equal(data, pitrFixture(t, "incarnation-a.bin")) {
				t.Fatalf("downloaded %s bytes = %q, %v", site, data, err)
			}
		}
	})
}

func TestDownloadPreservesReusedBinlogNames(t *testing.T) {
	ctx := context.Background()
	store, err := sidecar.NewArchiveStore(ctx, &sidecar.PITRConfig{StorageType: "PVC", PVC: &sidecar.PITRPVCConfig{MountPath: t.TempDir()}})
	if err != nil {
		t.Fatal(err)
	}
	prefix, site := "binlogs", "iad"
	base := time.Date(2026, 4, 15, 0, 0, 0, 0, time.UTC)
	var rows []map[string]any
	for i, name := range []string{"mysql-bin.000009", "mysql-bin.000001", "mysql-bin.000001"} {
		id := ""
		if i > 0 {
			id = pitrFixtureID(pitrFixture(t, []string{"incarnation-a.bin", "review-order-b.bin", "incarnation-b.bin"}[i]))
		}
		key := path.Join(prefix, site, name)
		if id != "" {
			key += "." + id
		}
		payload := pitrFixture(t, []string{"incarnation-a.bin", "review-order-b.bin", "incarnation-b.bin"}[i])
		if err := store.Put(ctx, key, bytes.NewReader(payload), int64(len(payload))); err != nil {
			t.Fatal(err)
		}
		rows = append(rows, map[string]any{"name": name, "id": id, "remotePath": key, "size": len(payload), "firstEventTime": base.Add(-time.Duration(i) * time.Hour), "previousGtids": "aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee" + []string{":1-10", ":1-20", ":1-30"}[i]})
	}
	// Remote manifests may arrive in any order.
	rows[0], rows[2] = rows[2], rows[0]
	data, err := json.Marshal(map[string]any{"version": 1, "site": site, "files": rows})
	if err != nil {
		t.Fatal(err)
	}
	if err := store.Put(ctx, sidecar.ManifestKey(prefix, site), bytes.NewReader(data), int64(len(data))); err != nil {
		t.Fatal(err)
	}
	out := t.TempDir()
	n, err := downloadSiteBinlogs(ctx, store, prefix, site, base.Add(24*time.Hour), out, slog.New(slog.NewTextHandler(io.Discard, nil)))
	if err != nil || n != 3 {
		t.Fatalf("downloaded %d: %v", n, err)
	}
	files, err := filepath.Glob(filepath.Join(out, site, "mysql-bin.*"))
	if err != nil {
		t.Fatal(err)
	}
	if len(files) != 3 {
		t.Fatalf("same-name versions overwrote each other: %v", files)
	}
	for i, file := range files {
		got, err := os.ReadFile(file)
		if err != nil || !bytes.Equal(got, pitrFixture(t, []string{"incarnation-a.bin", "review-order-b.bin", "incarnation-b.bin"}[i])) {
			t.Fatalf("replay order %d: got %q, %v", i, got, err)
		}
	}
	t.Run("existing replay collector", func(t *testing.T) {
		python, err := exec.LookPath("python3")
		if err != nil {
			t.Skip("python3 unavailable")
		}
		source, err := os.ReadFile(filepath.Join("..", "..", "internal", "controller", "restore_script.py"))
		if err != nil {
			t.Fatal(err)
		}
		program := `import ast, glob, os, sys
node = next(n for n in ast.parse(sys.stdin.read()).body if isinstance(n, ast.FunctionDef) and n.name == "_collect_binlogs")
namespace = {"glob": glob, "os": os}
exec(compile(ast.Module(body=[node], type_ignores=[]), "restore.py", "exec"), namespace)
sys.stdout.buffer.write(b"".join(open(f, "rb").read() for f in namespace["_collect_binlogs"](sys.argv[1])))`
		command := exec.Command(python, "-c", program, out)
		command.Stdin = bytes.NewReader(source)
		got, err := command.CombinedOutput()
		if err != nil || !bytes.Equal(got, append(append(bytes.Clone(pitrFixture(t, "incarnation-a.bin")), pitrFixture(t, "review-order-b.bin")...), pitrFixture(t, "incarnation-b.bin")...)) {
			t.Fatalf("replay collector order: %q, %v", got, err)
		}
	})
}

func TestDownloadRejectsUnsafeBinlogIdentity(t *testing.T) {
	ctx := context.Background()
	store, err := sidecar.NewArchiveStore(ctx, &sidecar.PITRConfig{StorageType: "PVC", PVC: &sidecar.PITRPVCConfig{MountPath: t.TempDir()}})
	if err != nil {
		t.Fatal(err)
	}
	m := sidecar.Manifest{Version: 1, Site: "iad", Files: []sidecar.ManifestEntry{{Name: "mysql-bin.000001", ID: "../../victim", RemotePath: "binlogs/iad/../../victim"}}}
	data, err := json.Marshal(m)
	if err != nil {
		t.Fatal(err)
	}
	if err := store.Put(ctx, sidecar.ManifestKey("binlogs", "iad"), bytes.NewReader(data), int64(len(data))); err != nil {
		t.Fatal(err)
	}
	n, err := downloadSiteBinlogs(ctx, store, "binlogs", "iad", time.Now(), t.TempDir(), slog.New(slog.NewTextHandler(io.Discard, nil)))
	if err == nil || n != 0 {
		t.Fatalf("unsafe identity download returned %d, %v", n, err)
	}
}
