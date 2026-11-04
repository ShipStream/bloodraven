package sidecar

import (
	"context"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"sync/atomic"
	"testing"
)

// mountInfoLine renders one /proc/self/mountinfo line with the given
// mount point (already kernel-escaped), optional fields and fstype.
func mountInfoLine(mountPoint, optional, fsType string) string {
	fields := []string{"36", "25", "0:32", "/", mountPoint, "rw,relatime"}
	if optional != "" {
		fields = append(fields, strings.Fields(optional)...)
	}
	fields = append(fields, "-", fsType, "/dev/sda1", "rw")
	return strings.Join(fields, " ")
}

func resolvedTempDir(t *testing.T) string {
	t.Helper()
	dir, err := filepath.EvalSymlinks(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	return dir
}

func TestCheckMountPoint(t *testing.T) {
	base := resolvedTempDir(t)
	mount := filepath.Join(base, "archive")
	spaced := filepath.Join(base, "pitr archive\\x")
	sub := filepath.Join(mount, "sub")
	for _, dir := range []string{sub, spaced} {
		if err := os.MkdirAll(dir, 0o755); err != nil {
			t.Fatal(err)
		}
	}
	link := filepath.Join(base, "link")
	if err := os.Symlink(mount, link); err != nil {
		t.Fatal(err)
	}
	file := filepath.Join(base, "file")
	if err := os.WriteFile(file, nil, 0o600); err != nil {
		t.Fatal(err)
	}
	escapedSpaced := strings.NewReplacer(" ", `\040`, `\`, `\134`).Replace(spaced)
	root := mountInfoLine("/", "shared:1", "overlay")

	tests := []struct {
		name      string
		path      string
		mountinfo string
		wantErr   []string
	}{
		{name: "exact mount root", path: mount, mountinfo: root + "\n" + mountInfoLine(mount, "", "ext4")},
		{name: "two optional fields", path: mount, mountinfo: root + "\n" + mountInfoLine(mount, "shared:5 master:2", "ext4")},
		{name: "stacked mounts", path: mount, mountinfo: root + "\n" + mountInfoLine(mount, "", "ext4") + "\n" + mountInfoLine(mount, "", "tmpfs")},
		{name: "symlink to mount root", path: link, mountinfo: root + "\n" + mountInfoLine(mount, "", "ext4")},
		{name: "kernel escapes", path: spaced, mountinfo: root + "\n" + mountInfoLine(escapedSpaced, "", "nfs4")},
		{name: "blank lines ignored", path: mount, mountinfo: "\n" + root + "\n\n" + mountInfoLine(mount, "", "ext4") + "\n"},
		{
			name: "subdirectory of a mount", path: sub,
			mountinfo: root + "\n" + mountInfoLine(mount, "", "ext4"),
			wantErr:   []string{"is not a mount point", "inside " + mount, "fstype ext4"},
		},
		{
			name: "directory on the root filesystem", path: mount, mountinfo: root,
			wantErr: []string{"is not a mount point", "inside /", "fstype overlay", "archive volume is not mounted"},
		},
		{name: "root filesystem itself", path: "/", mountinfo: root, wantErr: []string{"resolves to the root filesystem"}},
		{name: "relative path", path: "archive", mountinfo: root, wantErr: []string{"not absolute"}},
		{name: "missing path", path: filepath.Join(base, "absent"), mountinfo: root, wantErr: []string{"is not a mount point", "does not exist", "archive volume is not mounted"}},
		{name: "regular file", path: file, mountinfo: root, wantErr: []string{"is not a directory"}},
		{name: "malformed line", path: mount, mountinfo: "36 25 0:32 / " + mount + " rw", wantErr: []string{"pvc archive store: cannot verify mount: malformed mountinfo line"}},
		{name: "missing separator", path: mount, mountinfo: "36 25 0:32 / " + mount + " rw shared:1 ext4 /dev/sda1 rw", wantErr: []string{"pvc archive store: cannot verify mount: malformed mountinfo line"}},
		{name: "empty mountinfo", path: mount, mountinfo: "", wantErr: []string{"pvc archive store: cannot verify mount: mountinfo lists no mounts"}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := checkMountPoint(tt.path, strings.NewReader(tt.mountinfo))
			if len(tt.wantErr) == 0 {
				if err != nil {
					t.Fatalf("checkMountPoint(%s) = %v, want nil", tt.path, err)
				}
				return
			}
			if err == nil {
				t.Fatalf("checkMountPoint(%s) = nil, want error containing %q", tt.path, tt.wantErr)
			}
			for _, want := range tt.wantErr {
				if !strings.Contains(err.Error(), want) {
					t.Fatalf("checkMountPoint(%s) = %q, want it to contain %q", tt.path, err, want)
				}
			}
		})
	}
}

func TestUnescapeMountInfo(t *testing.T) {
	tests := map[string]string{
		`/plain`:             "/plain",
		`/a\040b`:            "/a b",
		`/tab\011nl\012`:     "/tab\tnl\n",
		`/back\134slash`:     `/back\slash`,
		`/short\04`:          `/short\04`,
		`/not\999octal\0401`: "/not\\999octal 1",
	}
	for in, want := range tests {
		if got := unescapeMountInfo(in); got != want {
			t.Errorf("unescapeMountInfo(%q) = %q, want %q", in, got, want)
		}
	}
}

// TestVerifyPVCMountPointRealProc runs the production verifier against
// this process's real mount table: a fresh temporary directory is never
// a mount point, /proc always is.
func TestVerifyPVCMountPointRealProc(t *testing.T) {
	if runtime.GOOS != "linux" {
		t.Skip("mountinfo is Linux-only")
	}
	link := filepath.Join(t.TempDir(), "proc-link")
	if err := os.Symlink("/proc", link); err != nil {
		t.Fatal(err)
	}
	tests := []struct {
		name    string
		path    string
		wantErr string
	}{
		{name: "unmounted temporary directory", path: t.TempDir(), wantErr: "is not a mount point"},
		{name: "proc", path: "/proc"},
		{name: "symlink to proc", path: link},
		{name: "missing path", path: filepath.Join(t.TempDir(), "absent"), wantErr: "does not exist"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := verifyPVCMountPoint(tt.path)
			if tt.wantErr == "" {
				if err != nil {
					t.Fatalf("verifyPVCMountPoint(%s) = %v, want nil", tt.path, err)
				}
				return
			}
			if err == nil || !strings.Contains(err.Error(), tt.wantErr) {
				t.Fatalf("verifyPVCMountPoint(%s) = %v, want error containing %q", tt.path, err, tt.wantErr)
			}
		})
	}
}

// TestNewArchiveStoreRealVerifier is the issue #221 regression test: the
// public constructor, with no test verifier, refuses an unmounted
// directory for readers and writers and creates nothing in it.
func TestNewArchiveStoreRealVerifier(t *testing.T) {
	if runtime.GOOS != "linux" {
		t.Skip("mountinfo is Linux-only")
	}
	for _, writable := range []bool{false, true} {
		dir := t.TempDir()
		_, err := NewArchiveStore(context.Background(), &PITRConfig{
			StorageType:    "PVC",
			PVC:            &PITRPVCConfig{MountPath: dir, Writable: writable},
			ManifestPrefix: "pitr/orders",
		})
		if err == nil || !strings.Contains(err.Error(), "is not a mount point") {
			t.Fatalf("writable=%v: NewArchiveStore = %v, want a not-a-mount-point error", writable, err)
		}
		entries, readErr := os.ReadDir(dir)
		if readErr != nil {
			t.Fatal(readErr)
		}
		if len(entries) != 0 {
			t.Fatalf("writable=%v: store init left %d entries in %s", writable, len(entries), dir)
		}
	}
}

func TestPVCWriteProbe(t *testing.T) {
	acceptAll := func(string) error { return nil }
	type setup func(t *testing.T, root string) (prefix string)
	mkdir := func(t *testing.T, path string, mode os.FileMode) {
		t.Helper()
		if err := os.MkdirAll(path, 0o755); err != nil {
			t.Fatal(err)
		}
		if err := os.Chmod(path, mode); err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _ = os.Chmod(path, 0o755) })
	}
	symlink := func(t *testing.T, target, link string) {
		t.Helper()
		if err := os.Symlink(target, link); err != nil {
			t.Fatal(err)
		}
	}
	tests := []struct {
		name     string
		writable bool
		manifest string
		setup    setup
		wantErr  string
		// nonRoot marks rows that rely on directory permissions, which
		// root ignores; the structural rows run as root too.
		nonRoot bool
	}{
		{name: "writable root", writable: true, setup: func(*testing.T, string) string { return "" }},
		{name: "missing prefix probes the root", writable: true, setup: func(*testing.T, string) string { return "orders/binlogs" }},
		{
			name: "default prefix is the manifest prefix", nonRoot: true, writable: true, manifest: "orders/binlogs",
			setup: func(t *testing.T, root string) string {
				mkdir(t, filepath.Join(root, "orders"), 0o755)
				mkdir(t, root, 0o555)
				return ""
			},
		},
		{
			name: "read-only root", nonRoot: true, writable: true, wantErr: "is not writable",
			setup: func(t *testing.T, root string) string { mkdir(t, root, 0o555); return "orders" },
		},
		{
			name: "read-only root with writable prefix directory", nonRoot: true, writable: true,
			setup: func(t *testing.T, root string) string {
				mkdir(t, filepath.Join(root, "orders"), 0o755)
				mkdir(t, root, 0o555)
				return "orders/binlogs"
			},
		},
		{
			name: "reader skips the probe", nonRoot: true, writable: false,
			setup: func(t *testing.T, root string) string { mkdir(t, root, 0o555); return "orders" },
		},
		{
			name: "unwritable site directory under a writable manifest prefix", nonRoot: true, writable: true,
			manifest: "orders/binlogs", wantErr: "is not writable",
			setup: func(t *testing.T, root string) string {
				mkdir(t, filepath.Join(root, "orders", "binlogs", "iad"), 0o555)
				return "orders/binlogs/iad"
			},
		},
		{
			name: "unwritable manifest prefix with a writable site directory", nonRoot: true, writable: true,
			manifest: "orders/binlogs", wantErr: "is not writable",
			setup: func(t *testing.T, root string) string {
				mkdir(t, filepath.Join(root, "orders", "binlogs", "iad"), 0o755)
				mkdir(t, filepath.Join(root, "orders", "binlogs"), 0o555)
				return "orders/binlogs/iad"
			},
		},
		{name: "prefix escapes the root", writable: true, wantErr: "escapes", setup: func(*testing.T, string) string { return "../x" }},
		{
			name: "symlinked component leaves the volume", writable: true, wantErr: "outside the archive volume",
			setup: func(t *testing.T, root string) string {
				symlink(t, t.TempDir(), filepath.Join(root, "orders"))
				return "orders"
			},
		},
		{
			name: "symlink above a missing directory leaves the volume", writable: true, wantErr: "outside the archive volume",
			setup: func(t *testing.T, root string) string {
				symlink(t, t.TempDir(), filepath.Join(root, "orders"))
				return "orders/binlogs/iad"
			},
		},
		{
			name: "dangling symlink", writable: true, wantErr: "resolve",
			setup: func(t *testing.T, root string) string {
				symlink(t, filepath.Join(root, "absent"), filepath.Join(root, "orders"))
				return "orders/binlogs"
			},
		},
		{
			name: "symlink inside the volume", writable: true,
			setup: func(t *testing.T, root string) string {
				mkdir(t, filepath.Join(root, "real"), 0o755)
				symlink(t, filepath.Join(root, "real"), filepath.Join(root, "orders"))
				return "orders/binlogs"
			},
		},
		{
			name: "regular file in the prefix", writable: true, wantErr: "is not a directory",
			setup: func(t *testing.T, root string) string {
				if err := os.WriteFile(filepath.Join(root, "orders"), nil, 0o600); err != nil {
					t.Fatal(err)
				}
				return "orders/binlogs"
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if tt.nonRoot && os.Geteuid() == 0 {
				t.Skip("root ignores directory permissions")
			}
			root := t.TempDir()
			prefix := tt.setup(t, root)
			store, err := newPVCStore(&PITRPVCConfig{
				MountPath: root, Writable: tt.writable, WritePrefix: prefix, MountVerifier: acceptAll,
			}, tt.manifest)
			if tt.wantErr != "" {
				if err == nil || !strings.Contains(err.Error(), tt.wantErr) {
					t.Fatalf("newPVCStore = %v, want error containing %q", err, tt.wantErr)
				}
				return
			}
			if err != nil {
				t.Fatalf("newPVCStore = %v", err)
			}
			keys, err := store.List(context.Background(), "")
			if err != nil {
				t.Fatal(err)
			}
			for _, key := range keys {
				if strings.Contains(key, "write-probe") {
					t.Fatalf("store lists probe %q after init", key)
				}
			}
			probes, err := filepath.Glob(filepath.Join(root, "*", ".bloodraven-write-probe-*"))
			if err != nil {
				t.Fatal(err)
			}
			rootProbes, _ := filepath.Glob(filepath.Join(root, ".bloodraven-write-probe-*"))
			if leftover := append(probes, rootProbes...); len(leftover) != 0 {
				t.Fatalf("write probe left behind: %v", leftover)
			}
		})
	}
}

// A persistent write failure must read the same on every retry: the
// supervisor logs ERROR only when the text changes, and the operator's
// condition message would churn on a random probe file name.
func TestPVCWriteProbeErrorIsStable(t *testing.T) {
	if os.Geteuid() == 0 {
		t.Skip("root ignores directory permissions")
	}
	root := t.TempDir()
	if err := os.Chmod(root, 0o555); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.Chmod(root, 0o755) })
	cfg := &PITRPVCConfig{MountPath: root, Writable: true, MountVerifier: func(string) error { return nil }}
	_, first := newPVCStore(cfg, "")
	_, second := newPVCStore(cfg, "")
	if first == nil || second == nil {
		t.Fatalf("newPVCStore = %v, %v; want both to fail", first, second)
	}
	if first.Error() != second.Error() {
		t.Fatalf("retry errors differ:\n%s\n%s", first, second)
	}
	if strings.Contains(first.Error(), "write-probe") {
		t.Fatalf("error %q names the random probe file", first)
	}
	want := "pvc archive store: " + root + " is not writable: permission denied"
	if first.Error() != want {
		t.Fatalf("error = %q, want %q", first, want)
	}
}

// TestPVCWriteProbeLeftoversStayInvisible covers a probe abandoned by a
// crash between create and remove: List must never return it, or a
// decrypt-download would try to decrypt it as a backup object.
func TestPVCWriteProbeLeftoversStayInvisible(t *testing.T) {
	root := t.TempDir()
	if err := os.MkdirAll(filepath.Join(root, "orders", "backup-1"), 0o755); err != nil {
		t.Fatal(err)
	}
	for _, p := range []string{"orders/backup-1/.bloodraven-write-probe-123.part", "orders/backup-1/dump.json.enc"} {
		if err := os.WriteFile(filepath.Join(root, p), []byte{0}, 0o600); err != nil {
			t.Fatal(err)
		}
	}
	store, err := newPVCStore(&PITRPVCConfig{
		MountPath: root, Writable: true, WritePrefix: "orders/backup-1", MountVerifier: func(string) error { return nil },
	}, "")
	if err != nil {
		t.Fatal(err)
	}
	keys, err := store.List(context.Background(), "orders/backup-1/")
	if err != nil {
		t.Fatal(err)
	}
	if len(keys) != 1 || keys[0] != "orders/backup-1/dump.json.enc" {
		t.Fatalf("List = %v, want only the backup object", keys)
	}
}

func TestNewPVCStoreUsesMountVerifier(t *testing.T) {
	var calls atomic.Int32
	root := t.TempDir()
	_, err := newPVCStore(&PITRPVCConfig{MountPath: root, MountVerifier: func(path string) error {
		calls.Add(1)
		if path != root {
			t.Errorf("verifier got %q, want the configured mount path %q", path, root)
		}
		return nil
	}}, "")
	if err != nil || calls.Load() != 1 {
		t.Fatalf("newPVCStore = %v, verifier calls = %d, want nil and 1", err, calls.Load())
	}
	if _, err := newPVCStore(&PITRPVCConfig{}, ""); err == nil || !strings.Contains(err.Error(), "empty mount path") {
		t.Fatalf("empty mount path: %v", err)
	}
}

func TestTempDirMountVerifierForTest(t *testing.T) {
	if err := TempDirMountVerifierForTest(t.TempDir()); err != nil {
		t.Fatalf("temp dir rejected: %v", err)
	}
	if err := TempDirMountVerifierForTest(os.TempDir()); err == nil {
		t.Fatal("the temp root itself was accepted")
	}
	if err := TempDirMountVerifierForTest("/"); err == nil {
		t.Fatal("/ was accepted")
	}
}
