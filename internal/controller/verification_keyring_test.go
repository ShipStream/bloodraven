package controller

import (
	"encoding/json"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
)

// verificationBootstrapFixture is an image-like layout: a stub mysqld that
// prints the --verbose --help keys the script parses, and a plugin directory
// that is read-only both by file mode and by directory mode.
type verificationBootstrapFixture struct {
	root, bin, plugins, run, data string
}

func newVerificationBootstrapFixture(t *testing.T, component bool, helpOutput string) verificationBootstrapFixture {
	t.Helper()
	root := t.TempDir()
	f := verificationBootstrapFixture{
		root:    root,
		bin:     filepath.Join(root, "bin"),
		plugins: filepath.Join(root, "image-plugins"),
		run:     filepath.Join(root, "run"),
		data:    filepath.Join(root, "data"),
	}
	// run stands in for the emptyDir the Job mounts at /run/mysqld.
	for _, dir := range []string{f.bin, f.plugins, f.run} {
		if err := os.MkdirAll(dir, 0o755); err != nil {
			t.Fatal(err)
		}
	}
	write := func(path, text string, mode os.FileMode) {
		t.Helper()
		if err := os.WriteFile(path, []byte(text), mode); err != nil {
			t.Fatal(err)
		}
	}
	write(filepath.Join(f.bin, "mysqld"), "#!/bin/sh\n"+helpOutput, 0o755)
	if component {
		write(filepath.Join(f.plugins, "component_keyring_file.so"), "component", 0o444)
	}
	write(filepath.Join(f.plugins, "other.so"), "other", 0o444)
	write(filepath.Join(f.plugins, "component_keyring_file.cnf"), `{"path":"/live-escrow/keyring","read_only":true}`, 0o444)
	// Seal the plugin directory so a write into it fails like the image's
	// read-only root filesystem. Restored before TempDir removal.
	if err := os.Chmod(f.plugins, 0o555); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.Chmod(f.plugins, 0o755) })
	return f
}

// snapshot returns a deterministic listing of path -> mode+content.
func snapshotTree(t *testing.T, dir string) map[string]string {
	t.Helper()
	out := map[string]string{}
	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatal(err)
	}
	for _, e := range entries {
		info, err := e.Info()
		if err != nil {
			t.Fatal(err)
		}
		b, err := os.ReadFile(filepath.Join(dir, e.Name()))
		if err != nil {
			t.Fatal(err)
		}
		out[e.Name()] = info.Mode().String() + ":" + string(b)
	}
	return out
}

// Execute the production bootstrap with an image-like, read-only plugin tree.
// Real encrypted DDL/loadDump is exercised by the lab restore acceptance run.
func TestVerificationKeyringBootstrap(t *testing.T) {
	helpOK := "printf 'plugin-dir %s\\nbasedir %s\\n' \"$VERIFY_TEST_PLUGIN_DIR\" \"$VERIFY_TEST_BASEDIR\"\n"

	runBootstrap := func(t *testing.T, f verificationBootstrapFixture) (argv string, output string) {
		t.Helper()
		script, _, found := strings.Cut(BackupVerifyScript(), "if [[ ! -d \"${DATA_DIR}/mysql\" ]]; then")
		if !found {
			t.Fatal("verification initialization boundary missing")
		}
		script += "printf '%s\\n' \"${MYSQLD[@]}\" >\"$BLOODRAVEN_RUN_DIR/argv\"\n"
		cmd := exec.Command("bash", "-c", script)
		cmd.Env = append(os.Environ(),
			"PATH="+f.bin+":"+os.Getenv("PATH"),
			"BLOODRAVEN_DATA_DIR="+f.data, "BLOODRAVEN_RUN_DIR="+f.run,
			"VERIFY_TEST_PLUGIN_DIR="+f.plugins, "VERIFY_TEST_BASEDIR="+f.root)
		out, err := cmd.CombinedOutput()
		if err != nil {
			t.Fatalf("bootstrap must not fail: %v\n%s", err, out)
		}
		b, err := os.ReadFile(filepath.Join(f.run, "argv"))
		if err != nil {
			t.Fatal(err)
		}
		return string(b), string(out)
	}

	t.Run("available", func(t *testing.T) {
		f := newVerificationBootstrapFixture(t, true, helpOK)
		before := snapshotTree(t, f.plugins)
		argv, output := runBootstrap(t, f)

		after := snapshotTree(t, f.plugins)
		if len(after) != len(before) {
			t.Fatalf("image plugin tree gained or lost files: before %v after %v", before, after)
		}
		for name, want := range before {
			if after[name] != want {
				t.Fatalf("image plugin file %s changed: %q -> %q", name, want, after[name])
			}
		}
		if !strings.Contains(output, "using ephemeral verification keyring") {
			t.Fatalf("expected keyring log line, got:\n%s", output)
		}

		var config struct {
			Path     string `json:"path"`
			ReadOnly bool   `json:"read_only"`
		}
		readJSON := func(path string, v any) {
			t.Helper()
			b, err := os.ReadFile(path)
			if err != nil {
				t.Fatal(err)
			}
			if err := json.Unmarshal(b, v); err != nil {
				t.Fatal(err)
			}
		}
		readJSON(filepath.Join(f.run, "plugins/component_keyring_file.cnf"), &config)
		if config.Path != filepath.Join(f.data, "verification-keyring") || config.ReadOnly {
			t.Fatalf("verification must use its own writable keyring: %+v", config)
		}
		var manifest struct {
			Components string `json:"components"`
		}
		readJSON(filepath.Join(f.run, "mysqld-verify.my"), &manifest)
		if manifest.Components != "file://component_keyring_file" {
			t.Fatalf("wrong component manifest: %+v", manifest)
		}
		binary := filepath.Join(f.run, "mysqld-verify")
		image, err := os.ReadFile(filepath.Join(f.bin, "mysqld"))
		if err != nil {
			t.Fatal(err)
		}
		copied, err := os.ReadFile(binary)
		if err != nil {
			t.Fatal(err)
		}
		if string(copied) != string(image) {
			t.Fatal("runtime binary differs from image binary")
		}
		if got, err := os.Readlink(filepath.Join(f.run, "plugins/other.so")); err != nil || got != filepath.Join(f.plugins, "other.so") {
			t.Fatalf("other plugins unavailable: %q %v", got, err)
		}
		want := binary + "\n--basedir=" + f.root + "\n--plugin-dir=" + filepath.Join(f.run, "plugins") + "\n"
		if argv != want {
			t.Fatalf("runtime arguments %q, want %q", argv, want)
		}
	})

	// A custom image without the component must keep verifying unencrypted
	// dumps on the stock mysqld, with a warning naming the reason.
	for name, tc := range map[string]struct {
		component bool
		help      string
		reason    string
	}{
		"missing-component": {false, helpOK, "component_keyring_file.so not found"},
		"unparsable-help":   {true, "echo 'nothing useful'\n", "could not parse plugin-dir/basedir"},
		"help-fails":        {true, "echo 'boom' >&2\nexit 3\n", "mysqld --verbose --help failed: boom"},
	} {
		t.Run(name, func(t *testing.T) {
			f := newVerificationBootstrapFixture(t, tc.component, tc.help)
			argv, output := runBootstrap(t, f)
			if argv != "mysqld\n" {
				t.Fatalf("expected stock mysqld, got argv %q", argv)
			}
			if !strings.Contains(output, "WARNING: verification keyring unavailable: ") ||
				!strings.Contains(output, tc.reason) {
				t.Fatalf("warning with reason %q missing from output:\n%s", tc.reason, output)
			}
			if _, err := os.Stat(filepath.Join(f.run, "mysqld-verify")); err == nil {
				t.Fatal("fallback must not leave a private mysqld copy behind")
			}
		})
	}
}
