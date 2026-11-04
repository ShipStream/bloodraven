package main

import (
	"bytes"
	"context"
	"errors"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"testing"

	"github.com/shipstream/bloodraven/internal/backupmeta"
	"github.com/shipstream/bloodraven/internal/sidecar"
)

func TestCleanupS3Prefix(t *testing.T) {
	tests := []struct {
		name, location, bucket, backup string
		want                           string
		wantErr                        string
	}{
		{name: "plain relative prefix", location: "prod/db/backup-1", bucket: "b", backup: "backup-1", want: "prod/db/backup-1/"},
		{name: "plain without prefix", location: "backup-1", bucket: "b", backup: "backup-1", want: "backup-1/"},
		{name: "encrypted same bucket", location: "s3://b/prod/backup-1/", bucket: "b", backup: "backup-1", want: "prod/backup-1/"},
		{name: "encrypted other bucket", location: "s3://other/prod/backup-1/", bucket: "b", backup: "backup-1", wantErr: "cross-bucket"},
		{name: "empty location", location: "", bucket: "b", backup: "backup-1", wantErr: "empty artifact path"},
		{name: "bucket root only", location: "s3://b/", bucket: "b", backup: "backup-1", wantErr: "empty artifact path"},
		{name: "dot-dot segment", location: "prod/../backup-1", bucket: "b", backup: "backup-1", wantErr: "invalid path segment"},
		{name: "double slash", location: "prod//backup-1", bucket: "b", backup: "backup-1", wantErr: "invalid path segment"},
		{name: "absolute key", location: "/prod/backup-1", bucket: "b", backup: "backup-1", wantErr: "absolute key"},
		{name: "parent prefix only", location: "prod", bucket: "b", backup: "backup-1", wantErr: "not the backup name"},
		{name: "sibling name", location: "prod/backup-10", bucket: "b", backup: "backup-1", wantErr: "not the backup name"},
		{name: "no bucket", location: "prod/backup-1", bucket: "", backup: "backup-1", wantErr: "requires a bucket"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got, err := cleanupS3Prefix(tc.location, tc.bucket, tc.backup)
			if tc.wantErr != "" {
				if err == nil || err.code != backupmeta.CleanupExitRejected || !strings.Contains(err.Error(), tc.wantErr) {
					t.Fatalf("err = %v, want rejection containing %q", err, tc.wantErr)
				}
				return
			}
			if err != nil || got != tc.want {
				t.Fatalf("got %q, %v; want %q", got, err, tc.want)
			}
		})
	}
}

func TestCleanupPVCPath(t *testing.T) {
	tests := []struct {
		name, location, mount string
		want, wantErr         string
	}{
		{name: "direct", location: "/backups/backup-1", mount: "/backups", want: "backup-1"},
		{name: "sub path", location: "/backups/team/a/backup-1", mount: "/backups", want: "team/a/backup-1"},
		{name: "mount root", location: "/backups", mount: "/backups", wantErr: "mount root"},
		{name: "outside mount", location: "/etc/backup-1", mount: "/backups", wantErr: "outside mount"},
		{name: "sibling mount", location: "/backups-evil/backup-1", mount: "/backups", wantErr: "outside mount"},
		{name: "relative", location: "backups/backup-1", mount: "/backups", wantErr: "not absolute"},
		{name: "other backup", location: "/backups/backup-10", mount: "/backups", wantErr: "not the backup name"},
		{name: "no mount", location: "/backups/backup-1", mount: "", wantErr: "absolute BLOODRAVEN_PVC_MOUNT_PATH"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got, err := cleanupPVCPath(tc.location, tc.mount, "backup-1")
			if tc.wantErr != "" {
				if err == nil || err.code != backupmeta.CleanupExitRejected || !strings.Contains(err.Error(), tc.wantErr) {
					t.Fatalf("err = %v, want rejection containing %q", err, tc.wantErr)
				}
				return
			}
			if err != nil || got != tc.want {
				t.Fatalf("got %q, %v; want %q", got, err, tc.want)
			}
		})
	}
}

type cleanupMemoryStore struct {
	mu        sync.Mutex
	keys      map[string]bool
	listErr   error
	deleteErr error
	sticky    string // a key Delete reports success for but keeps
	extra     string // a key List returns outside the requested prefix
}

func (s *cleanupMemoryStore) List(_ context.Context, prefix string) ([]string, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.listErr != nil {
		return nil, s.listErr
	}
	var out []string
	for k := range s.keys {
		if strings.HasPrefix(k, prefix) {
			out = append(out, k)
		}
	}
	if s.extra != "" {
		out = append(out, s.extra)
	}
	sort.Strings(out)
	return out, nil
}

func (s *cleanupMemoryStore) Delete(_ context.Context, key string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.deleteErr != nil {
		return s.deleteErr
	}
	if key != s.sticky {
		delete(s.keys, key)
	}
	return nil
}

func TestCleanupS3(t *testing.T) {
	seed := func() map[string]bool {
		keys := map[string]bool{"p/backup-10/@.json": true, "p/other/x": true}
		for i := range 40 {
			keys["p/backup-1/chunk-"+string(rune('a'+i%26))+string(rune('a'+i/26))] = true
		}
		return keys
	}
	tests := []struct {
		name      string
		store     *cleanupMemoryStore
		wantCode  int
		wantCount int
	}{
		{name: "deletes every key under the prefix", store: &cleanupMemoryStore{keys: seed()}, wantCount: 40},
		{name: "already gone", store: &cleanupMemoryStore{keys: map[string]bool{"p/backup-10/x": true}}, wantCount: 0},
		{name: "list error is transient", store: &cleanupMemoryStore{keys: seed(), listErr: errors.New("boom")}, wantCode: backupmeta.CleanupExitTransient},
		{name: "delete error is transient", store: &cleanupMemoryStore{keys: seed(), deleteErr: errors.New("denied")}, wantCode: backupmeta.CleanupExitTransient},
		{name: "leftover after re-list is transient", store: &cleanupMemoryStore{keys: seed(), sticky: "p/backup-1/chunk-aa"}, wantCode: backupmeta.CleanupExitTransient},
		{name: "foreign listed key is refused", store: &cleanupMemoryStore{keys: seed(), extra: "p/backup-10/@.json"}, wantCode: backupmeta.CleanupExitRejected},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			var outside []string
			for k := range tc.store.keys {
				if !strings.HasPrefix(k, "p/backup-1/") {
					outside = append(outside, k)
				}
			}
			n, err := cleanupS3(context.Background(), tc.store, "p/backup-1/")
			if tc.wantCode != 0 {
				if err == nil || err.code != tc.wantCode {
					t.Fatalf("err = %v, want exit %d", err, tc.wantCode)
				}
				return
			}
			if err != nil || n != tc.wantCount {
				t.Fatalf("deleted %d, err %v; want %d", n, err, tc.wantCount)
			}
			for _, k := range outside {
				if !tc.store.keys[k] {
					t.Fatalf("key %s outside the artifact was deleted", k)
				}
			}
			for k := range tc.store.keys {
				if strings.HasPrefix(k, "p/backup-1/") {
					t.Fatalf("key %s survived", k)
				}
			}
		})
	}
}

func TestCleanupPVC(t *testing.T) {
	t.Run("deletes the artifact tree and counts files", func(t *testing.T) {
		mount := t.TempDir()
		mustWrite(t, filepath.Join(mount, "sub", "backup-1", "@.json"))
		mustWrite(t, filepath.Join(mount, "sub", "backup-1", "db", "t.tsv.zst"))
		mustWrite(t, filepath.Join(mount, "sub", "backup-10", "@.json"))
		n, err := cleanupPVC(mount, "sub/backup-1")
		if err != nil || n != 2 {
			t.Fatalf("deleted %d, err %v; want 2", n, err)
		}
		if _, err := os.Stat(filepath.Join(mount, "sub", "backup-1")); !os.IsNotExist(err) {
			t.Fatalf("artifact still present: %v", err)
		}
		if _, err := os.Stat(filepath.Join(mount, "sub", "backup-10", "@.json")); err != nil {
			t.Fatalf("sibling removed: %v", err)
		}
	})
	t.Run("missing path is already gone", func(t *testing.T) {
		n, err := cleanupPVC(t.TempDir(), "backup-1")
		if err != nil || n != 0 {
			t.Fatalf("deleted %d, err %v; want 0, nil", n, err)
		}
	})
	t.Run("symlinked path component cannot escape the mount", func(t *testing.T) {
		mount := t.TempDir()
		outside := t.TempDir()
		mustWrite(t, filepath.Join(outside, "backup-1", "keep"))
		// A writer replaced a directory on the path with a symlink after
		// the location was recorded; os.Root resolves it at use time.
		if err := os.Symlink(outside, filepath.Join(mount, "sub")); err != nil {
			t.Fatal(err)
		}
		if _, err := cleanupPVC(mount, "sub/backup-1"); err == nil {
			t.Fatal("cleanup followed a symlink out of the mount")
		}
		if _, err := os.Stat(filepath.Join(outside, "backup-1", "keep")); err != nil {
			t.Fatalf("file outside the mount was deleted: %v", err)
		}
	})
}

func mustWrite(t *testing.T, p string) {
	t.Helper()
	if err := os.MkdirAll(filepath.Dir(p), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(p, []byte("x"), 0o644); err != nil {
		t.Fatal(err)
	}
}

func TestArtifactCleanup_VerdictAndExitCode(t *testing.T) {
	store := &cleanupMemoryStore{keys: map[string]bool{"p/backup-1/a": true, "p/backup-1/b": true}}
	var gotCfg *sidecar.PITRConfig
	newStore := func(_ context.Context, cfg *sidecar.PITRConfig) (cleanupStore, error) {
		gotCfg = cfg
		return store, nil
	}
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	tests := []struct {
		name     string
		req      artifactCleanupRequest
		wantCode int
		want     string
	}{
		{
			name: "success writes the object count",
			req:  artifactCleanupRequest{Name: "backup-1", StorageType: "S3", Location: "s3://b/p/backup-1/", Bucket: "b", Creds: "/creds"},
			want: "BLOODRAVEN_CLEANUP_COMPLETE objects=2",
		},
		{
			name:     "refusal writes the reason",
			req:      artifactCleanupRequest{Name: "backup-1", StorageType: "S3", Location: "p", Bucket: "b"},
			wantCode: backupmeta.CleanupExitRejected,
			want:     "BLOODRAVEN_CLEANUP_FAILED: S3 location \"p\": last path segment",
		},
		{
			name:     "unknown storage type",
			req:      artifactCleanupRequest{Name: "backup-1", StorageType: "NFS", Location: "x/backup-1"},
			wantCode: backupmeta.CleanupExitRejected,
			want:     "unknown storage type",
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			cleanupTerminationLogPath = filepath.Join(t.TempDir(), "termination-log")
			t.Cleanup(func() { cleanupTerminationLogPath = "/dev/termination-log" })
			var out bytes.Buffer
			code := artifactCleanup(context.Background(), tc.req, newStore, &out, logger)
			if code != tc.wantCode {
				t.Fatalf("exit %d, want %d (output %s)", code, tc.wantCode, out.String())
			}
			msg, err := os.ReadFile(cleanupTerminationLogPath)
			if err != nil || !strings.Contains(string(msg), tc.want) {
				t.Fatalf("termination log %q (%v), want %q", msg, err, tc.want)
			}
			if code == 0 {
				if n, ok := backupmeta.ParseCleanupComplete(string(msg)); !ok || n != 2 {
					t.Fatalf("verdict does not parse: %q", msg)
				}
				// Deleting by key never needs the backup passphrase.
				if gotCfg == nil || gotCfg.PassphraseFile != "" || gotCfg.S3 == nil || gotCfg.S3.AWSCredsDir != "/creds" {
					t.Fatalf("unexpected store config %+v", gotCfg)
				}
			}
		})
	}
}

// cleanupPagedStore lists in pages of three keys, like a paginated
// ListObjectsV2, and records how many pages it served.
type cleanupPagedStore struct {
	*cleanupMemoryStore
	pages int
}

func (s *cleanupPagedStore) ListPages(ctx context.Context, prefix string, fn func([]string) error) error {
	keys, err := s.List(ctx, prefix)
	if err != nil {
		return err
	}
	for i := 0; i < len(keys); i += 3 {
		s.pages++
		if err := fn(keys[i:min(i+3, len(keys))]); err != nil {
			return err
		}
	}
	return nil
}

func TestCleanupS3_DeletesPageByPage(t *testing.T) {
	keys := map[string]bool{"p/backup-10/x": true}
	for i := range 10 {
		keys["p/backup-1/"+string(rune('a'+i))] = true
	}
	store := &cleanupPagedStore{cleanupMemoryStore: &cleanupMemoryStore{keys: keys}}
	n, err := cleanupS3(context.Background(), store, "p/backup-1/")
	if err != nil || n != 10 {
		t.Fatalf("deleted %d, err %v", n, err)
	}
	if store.pages != 4 {
		t.Fatalf("served %d pages, want 4 (page-by-page)", store.pages)
	}
	if !store.keys["p/backup-10/x"] || len(store.keys) != 1 {
		t.Fatalf("remaining keys %v", store.keys)
	}

	foreign := &cleanupPagedStore{cleanupMemoryStore: &cleanupMemoryStore{keys: map[string]bool{"p/backup-1/a": true}, extra: "q/other"}}
	if _, err := cleanupS3(context.Background(), foreign, "p/backup-1/"); err == nil || err.code != backupmeta.CleanupExitRejected {
		t.Fatalf("a foreign key in a page was not refused: %v", err)
	}
}

func TestCleanupExitCodesAvoidRuntimeCodes(t *testing.T) {
	// os.Exit(1) paths and unrecovered panics (exit 2) must never read as a
	// refusal or a classified transient failure.
	for _, code := range []int{backupmeta.CleanupExitRejected, backupmeta.CleanupExitTransient} {
		if code == 1 || code == 2 {
			t.Fatalf("cleanup exit code %d collides with the Go runtime", code)
		}
	}
}
