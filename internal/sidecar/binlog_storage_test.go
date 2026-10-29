package sidecar

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"
)

// TestPVCStore_FullPath_Sanitizes proves the traversal guard in
// pvcStore.fullPath (called from every Put/Get/PutFile/GetFile/Delete
// path). This is security-critical for restore paths that may be fed
// attacker-controlled keys from object storage. AUDIT M15.
func TestPVCStore_FullPath_Sanitizes(t *testing.T) {
	p := &pvcStore{root: "/mnt/pvc"}

	cases := []struct {
		name    string
		key     string
		wantErr bool
	}{
		{"plain", "binlogs/iad/mysql-bin.000001", false},
		{"leading slash normalized", "/binlogs/iad/mysql-bin.000001", false},
		{"dotdot escape rejected", "../escape", true},
		{"dotdot nested rejected", "a/b/../../../escape", true},
		{"single dot rejected", ".", true},
		{"double dot rejected", "..", true},
		{"prefix collision rejected", "../pvc-evil/escape", true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			_, err := p.fullPath(tc.key)
			if (err != nil) != tc.wantErr {
				t.Errorf("fullPath(%q) err=%v wantErr=%v", tc.key, err, tc.wantErr)
			}
		})
	}
}

func TestValidateS3EndpointURL(t *testing.T) {
	for _, ok := range []string{"http://minio.bloodraven-playground:9000", "https://s3.example.com", "https://minio.minio.svc.cluster.local:9000/"} {
		if err := validateS3EndpointURL(ok); err != nil {
			t.Errorf("validateS3EndpointURL(%q) = %v, want nil", ok, err)
		}
	}
	for _, bad := range []string{"minio:9000", "file:///etc/passwd", "ftp://host", "http://", "https://user:pw@host", "://x"} {
		if err := validateS3EndpointURL(bad); err == nil {
			t.Errorf("validateS3EndpointURL(%q) = nil, want error", bad)
		}
	}
}

func TestArchiveStoreListPrefixes(t *testing.T) {
	ctx := context.Background()
	keys := []string{
		"binlogs/manifest-iad.json", "binlogs/manifest-pdx.json",
		"binlogs/iad/mysql-bin.000001", "binlogs/manifest-iad.json.extra",
		"binlogs2/manifest-other.json", "unrelated/file",
	}
	pvc, err := newPVCStore(&PITRPVCConfig{MountPath: t.TempDir()})
	if err != nil {
		t.Fatal(err)
	}
	for name, store := range map[string]ArchiveStore{"PVC": pvc, "memory": newMemStore()} {
		t.Run(name, func(t *testing.T) {
			for _, key := range keys {
				if err := store.Put(ctx, key, strings.NewReader(key), int64(len(key))); err != nil {
					t.Fatal(err)
				}
			}
			for _, prefix := range []string{"binlogs/manifest-", "binlogs/", "binlogs", "binlogs/i", "binlogs/manifest-iad.json", "", "missing/manifest-", "binlogs/missing", "binlogs/manifest-iad.json/nested/object"} {
				t.Run(prefix, func(t *testing.T) {
					var want []string
					for _, key := range keys {
						if strings.HasPrefix(key, prefix) {
							want = append(want, key)
						}
					}
					got, err := store.List(ctx, prefix)
					if err != nil {
						t.Fatal(err)
					}
					slices.Sort(got)
					slices.Sort(want)
					if !slices.Equal(got, want) {
						t.Fatalf("List(%q) = %v, want %v", prefix, got, want)
					}
				})
			}
		})
	}
}

func TestPVCStoreListNormalizationAndVisibility(t *testing.T) {
	ctx := context.Background()
	root := t.TempDir()
	pvc, err := newPVCStore(&PITRPVCConfig{MountPath: root})
	if err != nil {
		t.Fatal(err)
	}
	key := "binlogs/manifest-iad.json"
	if err := pvc.Put(ctx, key, strings.NewReader("complete"), 8); err != nil {
		t.Fatal(err)
	}
	for _, suffix := range []string{".tmp", ".part"} {
		if err := os.WriteFile(filepath.Join(root, key+suffix), []byte("incomplete"), 0o600); err != nil {
			t.Fatal(err)
		}
	}
	for _, prefix := range []string{"binlogs/manifest-", "/binlogs/manifest-", "binlogs/./manifest-", "binlogs/", ""} {
		got, err := pvc.List(ctx, prefix)
		if err != nil {
			t.Fatal(err)
		}
		if !slices.Equal(got, []string{key}) {
			t.Errorf("List(%q) = %v, want only the committed object", prefix, got)
		}
	}
	for _, prefix := range []string{"../escape", "binlogs/../../escape", ".", "..", "../" + filepath.Base(root) + "-evil"} {
		if _, err := pvc.List(ctx, prefix); err == nil {
			t.Errorf("List(%q) accepted an invalid prefix", prefix)
		}
	}
	missing := &pvcStore{root: filepath.Join(root, "not-created")}
	if got, err := missing.List(ctx, ""); err != nil || len(got) != 0 {
		t.Errorf("missing root listing = %v, %v", got, err)
	}
	broken := &pvcStore{root: filepath.Join(root, key)}
	if _, err := broken.List(ctx, "nested/object-"); err == nil {
		t.Error("non-directory storage error was hidden")
	}
}

func TestPVCStoreListReportsWalkErrors(t *testing.T) {
	if os.Geteuid() == 0 {
		t.Skip("root bypasses directory permission checks")
	}
	root := t.TempDir()
	blocked := filepath.Join(root, "blocked")
	if err := os.Mkdir(blocked, 0o700); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := os.Chmod(blocked, 0o700); err != nil {
			t.Error(err)
		}
	})
	if err := os.Chmod(blocked, 0); err != nil {
		t.Fatal(err)
	}
	pvc := &pvcStore{root: root}
	if _, err := pvc.List(context.Background(), "blocked/"); !errors.Is(err, os.ErrPermission) {
		t.Fatalf("listing unreadable storage returned %v, want permission error", err)
	}
	if got, err := pvc.List(context.Background(), "other-prefix"); err != nil || len(got) != 0 {
		t.Fatalf("unrelated unreadable subtree affected listing: %v, %v", got, err)
	}
}
