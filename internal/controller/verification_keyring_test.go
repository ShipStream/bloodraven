package controller

import (
	"encoding/json"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
)

// Execute the production bootstrap with an image-like, read-only plugin tree.
// Real encrypted DDL/loadDump is exercised by the lab restore acceptance run.
func TestVerificationKeyringBootstrap(t *testing.T) {
	for _, available := range []bool{true, false} {
		name := "available"
		if !available {
			name = "missing-component"
		}
		t.Run(name, func(t *testing.T) {
			root := t.TempDir()
			bin := filepath.Join(root, "bin")
			plugins := filepath.Join(root, "image-plugins")
			run := filepath.Join(root, "run")
			data := filepath.Join(root, "data")
			for _, dir := range []string{bin, plugins} {
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
			read := func(path string) []byte {
				t.Helper()
				b, err := os.ReadFile(path)
				if err != nil {
					t.Fatal(err)
				}
				return b
			}
			write(filepath.Join(bin, "mysqld"), "#!/bin/sh\nprintf 'plugin-dir %s\\nbasedir %s\\n' \"$VERIFY_TEST_PLUGIN_DIR\" \"$VERIFY_TEST_BASEDIR\"\n", 0o755)
			if available {
				write(filepath.Join(plugins, "component_keyring_file.so"), "component", 0o444)
			}
			write(filepath.Join(plugins, "other.so"), "other", 0o444)
			originalConfig := `{"path":"/live-escrow/keyring","read_only":true}`
			write(filepath.Join(plugins, "component_keyring_file.cnf"), originalConfig, 0o444)
			script, _, found := strings.Cut(BackupVerifyScript(), "if [[ ! -d \"${DATA_DIR}/mysql\" ]]; then")
			if !found {
				t.Fatal("verification initialization boundary missing")
			}
			script += "printf '%s\\n' \"${MYSQLD[@]}\" >\"$RUN_DIR/argv\"\n"
			cmd := exec.Command("bash", "-c", script)
			cmd.Env = append(os.Environ(),
				"PATH="+bin+":"+os.Getenv("PATH"),
				"BLOODRAVEN_DATA_DIR="+data, "BLOODRAVEN_RUN_DIR="+run,
				"VERIFY_TEST_PLUGIN_DIR="+plugins, "VERIFY_TEST_BASEDIR="+root)
			output, err := cmd.CombinedOutput()
			if !available {
				if err == nil {
					t.Fatal("missing keyring component must fail bootstrap")
				}
				return
			}
			if err != nil {
				t.Fatalf("bootstrap: %v\n%s", err, output)
			}
			if string(read(filepath.Join(plugins, "component_keyring_file.cnf"))) != originalConfig {
				t.Fatal("bootstrap changed the image's original keyring configuration")
			}
			var config struct {
				Path     string `json:"path"`
				ReadOnly bool   `json:"read_only"`
			}
			if err := json.Unmarshal(read(filepath.Join(run, "plugins/component_keyring_file.cnf")), &config); err != nil {
				t.Fatal(err)
			}
			if config.Path != filepath.Join(data, "verification-keyring") || config.ReadOnly {
				t.Fatalf("verification must use its own writable keyring: %+v", config)
			}
			var manifest struct {
				Components string `json:"components"`
			}
			if err := json.Unmarshal(read(filepath.Join(run, "mysqld-verify.my")), &manifest); err != nil {
				t.Fatal(err)
			}
			if manifest.Components != "file://component_keyring_file" {
				t.Fatalf("wrong component manifest: %+v", manifest)
			}
			binary := filepath.Join(run, "mysqld-verify")
			if string(read(binary)) != string(read(filepath.Join(bin, "mysqld"))) {
				t.Fatal("runtime binary differs from image binary")
			}
			if got, err := os.Readlink(filepath.Join(run, "plugins/other.so")); err != nil || got != filepath.Join(plugins, "other.so") {
				t.Fatalf("other plugins unavailable: %q %v", got, err)
			}
			want := binary + "\n--basedir=" + root + "\n--plugin-dir=" + filepath.Join(run, "plugins") + "\n"
			if got := string(read(filepath.Join(run, "argv"))); got != want {
				t.Fatalf("runtime arguments %q, want %q", got, want)
			}
		})
	}
}
