package controller

import (
	"encoding/json"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
)

// runEmbeddedBackupPython imports the actual Job script with mysqlsh stubbed.
// The supplied body can exercise helpers without opening a MySQL connection.
func runEmbeddedBackupPython(t *testing.T, script, body string, env map[string]string) ([]byte, error) {
	t.Helper()
	python, err := exec.LookPath("python3")
	if err != nil {
		if os.Getenv("CI") != "" {
			t.Fatal("python3 is required to test embedded Job scripts in CI")
		}
		t.Skip("python3 is unavailable")
	}
	dir := t.TempDir()
	for name, contents := range map[string]string{"job.py": script, "mysqlsh.py": "# Stub: helper tests do not connect to MySQL.\n"} {
		if err := os.WriteFile(filepath.Join(dir, name), []byte(contents), 0o600); err != nil {
			t.Fatal(err)
		}
	}
	cmd := exec.Command(python, "-c", "import runpy\njob = runpy.run_path('job.py')\n"+body)
	cmd.Dir = dir
	cmd.Env = []string{"PYTHONPATH=" + dir, "PYTHONDONTWRITEBYTECODE=1"}
	for key, value := range env {
		cmd.Env = append(cmd.Env, key+"="+value)
	}
	return cmd.CombinedOutput()
}

func TestEmbeddedScripts_PreserveMySQLCredentialContents(t *testing.T) {
	for name, script := range map[string]string{"dump": dumpScript, "restore": restoreScript} {
		t.Run(name, func(t *testing.T) {
			for _, tc := range []struct {
				name, user, password string
			}{
				{name: "ordinary", user: "backup", password: "s3cret"},
				{name: "leading-space", user: "backup", password: " secret"},
				{name: "trailing-space", user: "backup", password: "secret "},
				{name: "tabs", user: "backup", password: "\tsecret\t"},
				{name: "line-feed", user: "backup", password: "\nsecret\n"},
				{name: "carriage-return", user: "backup", password: "\rsecret\r"},
				{name: "crlf", user: "backup", password: "\r\nsecret\r\n"},
				{name: "internal-crlf", user: "backup", password: "sec\r\nret"},
				{name: "all-whitespace", user: "backup", password: " \t\r\n"},
				{name: "unicode", user: "備份", password: "\u2003密碼\u00a0"},
				{name: "empty-password", user: "backup", password: ""},
				{name: "username-whitespace", user: " backup\t\r\n", password: "secret"},
			} {
				t.Run(tc.name, func(t *testing.T) {
					dir := t.TempDir()
					for key, value := range map[string]string{"MYSQL_USER": tc.user, "MYSQL_PASSWORD": tc.password} {
						if err := os.WriteFile(filepath.Join(dir, key), []byte(value), 0o600); err != nil {
							t.Fatal(err)
						}
					}
					out, err := runEmbeddedBackupPython(t, script, "import json\nprint(json.dumps(job['_resolve_credentials']()))\n", map[string]string{
						"BLOODRAVEN_MYSQL_CREDS_DIR": dir, "MYSQL_USER": "stale-user", "MYSQL_PASSWORD": "stale-password",
					})
					if err != nil {
						t.Fatalf("resolve credentials: %v: %s", err, out)
					}
					var got [2]string
					if err := json.Unmarshal(out, &got); err != nil {
						t.Fatalf("decode credentials: %v: %s", err, out)
					}
					if got != [2]string{tc.user, tc.password} {
						t.Fatalf("credential contents changed: got %q want %q", got, [2]string{tc.user, tc.password})
					}
				})
			}
		})
	}
}

func TestEmbeddedScripts_MySQLCredentialFallback(t *testing.T) {
	for name, script := range map[string]string{"dump": dumpScript, "restore": restoreScript} {
		t.Run(name, func(t *testing.T) {
			for _, tc := range []string{"missing-files", "unreadable-files", "no-directory", "empty-user"} {
				t.Run(tc, func(t *testing.T) {
					dir := t.TempDir()
					env := map[string]string{"BLOODRAVEN_MYSQL_CREDS_DIR": dir, "MYSQL_USER": " env-user ", "MYSQL_PASSWORD": " env-password\r\n"}
					if tc == "no-directory" {
						delete(env, "BLOODRAVEN_MYSQL_CREDS_DIR")
					}
					if tc == "unreadable-files" {
						// Directories cause OSError even when the test runs as root.
						for _, key := range []string{"MYSQL_USER", "MYSQL_PASSWORD"} {
							if err := os.Mkdir(filepath.Join(dir, key), 0o700); err != nil {
								t.Fatal(err)
							}
						}
					}
					if tc == "empty-user" {
						if err := os.WriteFile(filepath.Join(dir, "MYSQL_USER"), nil, 0o600); err != nil {
							t.Fatal(err)
						}
					}
					out, err := runEmbeddedBackupPython(t, script, "import json\nprint(json.dumps(job['_resolve_credentials']()))\n", env)
					if tc == "empty-user" {
						if err == nil || !strings.Contains(string(out), "no MYSQL_USER") {
							t.Fatalf("empty mounted user must fail validation without env fallback: err=%v out=%s", err, out)
						}
						return
					}
					if err != nil {
						t.Fatalf("resolve fallback credentials: %v: %s", err, out)
					}
					var got [2]string
					if err := json.Unmarshal(out, &got); err != nil {
						t.Fatal(err)
					}
					if got != [2]string{env["MYSQL_USER"], env["MYSQL_PASSWORD"]} {
						t.Fatalf("fallback changed credentials: %q", got)
					}
				})
			}
		})
	}
}

func TestEmbeddedScripts_AWSCredentialsRemainNormalized(t *testing.T) {
	for name, script := range map[string]string{"dump": dumpScript, "restore": restoreScript} {
		t.Run(name, func(t *testing.T) {
			dir := t.TempDir()
			for key, value := range map[string]string{
				"AWS_ACCESS_KEY_ID": " access-key\r\n", "AWS_SECRET_ACCESS_KEY": " secret-key\t\n",
				"AWS_SESSION_TOKEN": " session-token\n", "AWS_REGION": " region-1\r\n",
			} {
				if err := os.WriteFile(filepath.Join(dir, key), []byte(value), 0o600); err != nil {
					t.Fatal(err)
				}
			}
			out, err := runEmbeddedBackupPython(t, script, `import json, os
job['_configure_aws_creds_dir']()
path = os.environ['AWS_SHARED_CREDENTIALS_FILE']
with open(path) as f:
    contents = f.read()
print(json.dumps([contents, os.environ['AWS_REGION'], oct(os.stat(path).st_mode & 0o777)]))
`, map[string]string{"BLOODRAVEN_AWS_CREDS_DIR": dir, "HOME": t.TempDir()})
			if err != nil {
				t.Fatalf("configure AWS credentials: %v: %s", err, out)
			}
			var got [3]string
			if err := json.Unmarshal(out, &got); err != nil {
				t.Fatal(err)
			}
			want := [3]string{"[default]\naws_access_key_id = access-key\naws_secret_access_key = secret-key\naws_session_token = session-token\n", "region-1", "0o600"}
			if got != want {
				t.Fatalf("AWS configuration changed: got %q want %q", got, want)
			}
		})
	}
}
