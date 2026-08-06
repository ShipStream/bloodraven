package controller

import (
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"

	v1alpha1 "github.com/shipstream/bloodraven/api/v1alpha1"
)

// credentialsModeGroup builds a group that exercises every optional secret so
// the generated credentials-mode script contains all create_user_with_grants
// call sites.
func credentialsModeGroup() *v1alpha1.MysqlFailoverGroup {
	fg := &v1alpha1.MysqlFailoverGroup{}
	fg.Spec.Credentials = &v1alpha1.CredentialsSpec{
		OperatorSecret: "op",
		AppSecret:      "app",
		ReadOnlySecret: "ro",
		MonitorSecret:  "mon",
		BackupSecret:   "bk",
	}
	return fg
}

func initScriptVariants() map[string]string {
	return map[string]string{
		"secretName":  generateSecretNameModeInitScript(),
		"credentials": generateCredentialsModeInitScript(credentialsModeGroup()),
	}
}

// TestInitUsersScriptUsesExplicitSocket guards the first-boot failure that
// wedged the nightly E2E: a bare `mysql -u root` resolves the socket from the
// image's [client] defaults, which is not always where the entrypoint's
// temporary server bound. Every client call must go through run_mysql(), which
// passes an explicitly resolved --socket.
func TestInitUsersScriptUsesExplicitSocket(t *testing.T) {
	for name, script := range initScriptVariants() {
		t.Run(name, func(t *testing.T) {
			if !strings.Contains(script, `--protocol=SOCKET --socket="$MYSQL_SOCKET"`) {
				t.Error("script does not define run_mysql with an explicit socket")
			}
			for _, line := range strings.Split(script, "\n") {
				trimmed := strings.TrimSpace(line)
				if strings.Contains(trimmed, "mysql -u root") || strings.Contains(trimmed, "mysql -uroot") {
					t.Errorf("bare mysql client invocation bypasses run_mysql(): %q", trimmed)
				}
			}
		})
	}
}

func TestInitUsersScriptIsValidBash(t *testing.T) {
	bash, err := exec.LookPath("bash")
	if err != nil {
		t.Skip("bash not available")
	}
	dir := t.TempDir()
	for name, script := range initScriptVariants() {
		t.Run(name, func(t *testing.T) {
			path := filepath.Join(dir, name+".sh")
			if err := os.WriteFile(path, []byte(script), 0o700); err != nil {
				t.Fatal(err)
			}
			out, err := exec.Command(bash, "-n", path).CombinedOutput()
			if err != nil {
				t.Fatalf("bash -n failed: %v\n%s", err, out)
			}
		})
	}
}

// TestInitScriptSocketResolution runs the resolver preamble under bash against
// a real Unix socket to prove it prefers the entrypoint's $SOCKET and fails
// loudly (rather than falling through to a client default) when nothing is
// listening.
func TestInitScriptSocketResolution(t *testing.T) {
	bash, err := exec.LookPath("bash")
	if err != nil {
		t.Skip("bash not available")
	}

	// A local MySQL install would put real sockets at the fallback paths and
	// make the negative case ambiguous.
	for _, fallback := range []string{"/var/run/mysqld/mysqld.sock", "/var/lib/mysql/mysql.sock", "/tmp/mysql.sock"} {
		if fi, err := os.Stat(fallback); err == nil && fi.Mode()&os.ModeSocket != 0 {
			t.Skipf("host has a real MySQL socket at %s", fallback)
		}
	}

	// Short base dir: Unix socket paths are capped near 108 bytes.
	base, err := os.MkdirTemp("", "br")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = os.RemoveAll(base) }()

	sockPath := filepath.Join(base, "s.sock")
	ln, err := net.Listen("unix", sockPath)
	if err != nil {
		t.Skipf("cannot create unix socket: %v", err)
	}
	defer func() { _ = ln.Close() }()

	notASocket := filepath.Join(base, "regular")
	if err := os.WriteFile(notASocket, nil, 0o600); err != nil {
		t.Fatal(err)
	}

	tests := []struct {
		name       string
		socketEnv  string
		wantErr    bool
		wantOutput string
	}{
		{name: "prefers entrypoint socket", socketEnv: sockPath, wantOutput: "using MySQL socket " + sockPath},
		{name: "unset falls through to failure", socketEnv: "", wantErr: true, wantOutput: "could not locate a MySQL server socket"},
		{name: "non-socket path is rejected", socketEnv: notASocket, wantErr: true, wantOutput: "could not locate a MySQL server socket"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cmd := exec.Command(bash, "-c", "set -euo pipefail\n"+initScriptSocketPreamble)
			cmd.Env = append(os.Environ(), "SOCKET="+tt.socketEnv)
			out, err := cmd.CombinedOutput()
			if tt.wantErr && err == nil {
				t.Fatalf("expected failure, got success:\n%s", out)
			}
			if !tt.wantErr && err != nil {
				t.Fatalf("unexpected failure: %v\n%s", err, out)
			}
			if !strings.Contains(string(out), tt.wantOutput) {
				t.Errorf("output %q does not contain %q", out, tt.wantOutput)
			}
		})
	}
}
