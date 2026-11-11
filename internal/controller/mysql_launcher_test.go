package controller

import (
	"bytes"
	"context"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"strings"
	"testing"

	appsv1 "k8s.io/api/apps/v1"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"

	v1alpha1 "github.com/shipstream/bloodraven/api/v1alpha1"
)

// mysqlLauncherRotationGolden is the server_uuid step every mysql
// container starts with; %s is the keyring data file ("" without
// encryption at rest) and %d whether the binary log is encrypted.
// TestMySQLLauncherNative proves against the real image that this text
// does what its comments say; this pin makes a change to it a deliberate
// one.
//
// Updating these golden texts has a cost for every user. The launcher's
// text and the mysqld flags are part of each site's spec hash
// (ComputeSpecHash), on purpose: a change that did not move the hash would
// never reach a running pod. So any edit here, a reworded log line
// included, makes every group roll every site once after the operator
// upgrade, with a handoff of the primary. Change the launcher only in a
// release whose notes announce the roll, never in a patch release, and
// batch such edits. The same holds for the flags TestMySQLServerArgs pins.
const mysqlLauncherRotationGolden = `log() { echo "bloodraven-launcher: $*" >&2; }
datadir='/var/lib/mysql'
keyring='%s'
binlog_encrypted=%d
clonedir="$datadir/#clone"
needs_binlog_key() {
  [ "$binlog_encrypted" = 1 ] && return 0
  [ -r "$datadir/mysqld-auto.cnf" ] || return 1
  while IFS= read -r line || [ -n "$line" ]; do
    case "$line" in *binlog_encryption*) return 0 ;; esac
  done < "$datadir/mysqld-auto.cnf"
  return 1
}
if [ -d "$clonedir" ] && { [ ! -r "$clonedir" ] || [ ! -x "$clonedir" ]; }; then
  log "WARNING: cannot inspect $clonedir as uid $(id -u); if a clone is staged there, server_uuid is NOT rotated"
elif [ -e "$clonedir/#replace_files" ]; then
  previous=unknown
  if [ -r "$datadir/auto.cnf" ]; then
    while IFS= read -r line || [ -n "$line" ]; do
      case "$line" in server-uuid=*|server_uuid=*) previous=${line#*=} ;; esac
    done < "$datadir/auto.cnf"
  fi
  if [ -e "$clonedir/#status_error" ] || [ -e "$clonedir/#status_in_progress" ]; then
    log "an interrupted clone is staged in $clonedir; mysqld rolls it back and server_uuid stays $previous"
  elif [ -n "$keyring" ] && [ -e "$keyring" ] && [ ! -w "$keyring" ] && needs_binlog_key; then
    log "WARNING: a finished clone is staged in $clonedir but the keyring $keyring is read-only (the site is sealed), and a new server_uuid needs a new binary log encryption key there; server_uuid is NOT rotated (stays $previous)"
  elif rm -f -- "$datadir/auto.cnf" 2>/dev/null && [ ! -e "$datadir/auto.cnf" ]; then
    sync "$datadir" 2>/dev/null || true
    log "a finished clone is staged in $clonedir; removed auto.cnf so mysqld generates a new server_uuid (previous: $previous)"
  else
    log "WARNING: a finished clone is staged in $clonedir but $datadir/auto.cnf could not be removed as uid $(id -u); server_uuid is NOT rotated (stays $previous)"
  fi
fi
`

const mysqlLauncherHandoffGolden = `entrypoint=
for candidate in /usr/local/bin/docker-entrypoint.sh /entrypoint.sh; do
  if [ -x "$candidate" ]; then
    entrypoint=$candidate
    break
  fi
done
if [ -z "$entrypoint" ]; then
  entrypoint=$(command -v docker-entrypoint.sh 2>/dev/null) || entrypoint=
fi
if [ -z "$entrypoint" ]; then
  log "no MySQL image entrypoint found (looked for /usr/local/bin/docker-entrypoint.sh /entrypoint.sh, then docker-entrypoint.sh on PATH); spec.image must provide one. This container cannot start until it does: set spec.image to an image that has it (the Docker Official Image and Oracle's community-server image do), or add the image's entrypoint script at one of those paths"
  exit 127
fi
exec "$entrypoint" "$0" "$@"
`

// mysqlLauncherEncryptionGolden is the encryption-at-rest part for the
// default keyring paths.
const mysqlLauncherEncryptionGolden = `src="/run/bloodraven/keyring-component-src"
mkdir -p "/opt/bloodraven/mysql/bin"
mkdir -p "/opt/bloodraven/mysql/lib64/mysql"
ln -sfn "/usr/lib64/mysql/plugin" "/opt/bloodraven/mysql/lib64/mysql/plugin"
ln -sfn "/usr/share" "/opt/bloodraven/mysql/share"
cp "/usr/sbin/mysqld" "/opt/bloodraven/mysql/bin/mysqld"
cp "$src/keyring-manifest.json" "/opt/bloodraven/mysql/bin/mysqld.my"
cp "$src/keyring-component.json" "/usr/lib64/mysql/plugin/component_keyring_file.cnf"
chmod 755 "/opt/bloodraven/mysql/bin/mysqld"
chmod 644 "/opt/bloodraven/mysql/bin/mysqld.my" "/usr/lib64/mysql/plugin/component_keyring_file.cnf"
test -x "/opt/bloodraven/mysql/bin/mysqld"
test -r "/opt/bloodraven/mysql/bin/mysqld.my"
test -r "/usr/lib64/mysql/plugin/component_keyring_file.cnf"
test -x "/usr/lib64/mysql/plugin/component_keyring_file.so"
export PATH="/opt/bloodraven/mysql/bin":"$PATH"
`

func TestMySQLLauncherRendering(t *testing.T) {
	flags := []string{"--server-id=7", "--gtid-mode=ON"}

	t.Run("without encryption", func(t *testing.T) {
		command, args := mysqlLauncher(newTestFG(), flags)
		want := "set -euo pipefail\n" + fmt.Sprintf(mysqlLauncherRotationGolden, "", 0) + mysqlLauncherHandoffGolden
		if len(command) != 3 || command[0] != "/bin/bash" || command[1] != "-ec" {
			t.Fatalf("command = %q, want /bin/bash -ec <script>", command)
		}
		if command[2] != want {
			t.Fatalf("launcher script\n got:\n%s\nwant:\n%s", command[2], want)
		}
		// $0 is "mysqld", so the entrypoint sees `mysqld --flag…`.
		if wantArgs := []string{"mysqld", "--server-id=7", "--gtid-mode=ON"}; !reflect.DeepEqual(args, wantArgs) {
			t.Fatalf("args = %q, want %q", args, wantArgs)
		}
	})

	t.Run("with encryption", func(t *testing.T) {
		command, args := mysqlLauncher(encTestFG(), flags)
		// The identity step comes first: it cannot fail the container,
		// and a failing keyring step must not skip it on a later start.
		want := "set -euo pipefail\n" + fmt.Sprintf(mysqlLauncherRotationGolden, "/run/mysql-keyring/keyring", 1) + mysqlLauncherEncryptionGolden + mysqlLauncherHandoffGolden
		if len(command) != 3 || command[0] != "/bin/bash" || command[1] != "-ec" {
			t.Fatalf("command = %q, want /bin/bash -ec <script>", command)
		}
		if command[2] != want {
			t.Fatalf("launcher script\n got:\n%s\nwant:\n%s", command[2], want)
		}
		wantArgs := []string{"mysqld", "--basedir=/usr", "--plugin-dir=/usr/lib64/mysql/plugin", "--server-id=7", "--gtid-mode=ON"}
		if !reflect.DeepEqual(args, wantArgs) {
			t.Fatalf("args = %q, want %q", args, wantArgs)
		}
	})

	// Without binary log encryption a new server_uuid needs no key, so the
	// sealed-keyring guard is rendered off (R4 B3).
	t.Run("with encryption, binary log not covered", func(t *testing.T) {
		fg := encTestFG()
		off := false
		fg.Spec.EncryptionAtRest.Coverage = &v1alpha1.EncryptionCoverageSpec{BinaryLog: &off}
		command, _ := mysqlLauncher(fg, flags)
		want := "set -euo pipefail\n" + fmt.Sprintf(mysqlLauncherRotationGolden, "/run/mysql-keyring/keyring", 0) + mysqlLauncherEncryptionGolden + mysqlLauncherHandoffGolden
		if command[2] != want {
			t.Fatalf("launcher script\n got:\n%s\nwant:\n%s", command[2], want)
		}
	})

	t.Run("flags are not aliased", func(t *testing.T) {
		in := append(make([]string, 0, 8), "--server-id=7")
		_, args := mysqlLauncher(encTestFG(), in)
		args[len(args)-1] = "mutated"
		if in[0] != "--server-id=7" {
			t.Fatalf("mysqlLauncher wrote into its caller's slice: %q", in)
		}
	})
}

// TestMySQLServerArgs pins the mysqld flags: the launcher test and the
// native test start servers with exactly these. They are part of every
// site's spec hash: a change here rolls every site of every group after
// the operator upgrade (see mysqlLauncherRotationGolden).
func TestMySQLServerArgs(t *testing.T) {
	want := []string{
		"--server-id=101",
		"--gtid-mode=ON",
		"--enforce-gtid-consistency=ON",
		"--log-bin=/var/lib/mysql/mysql-bin",
		"--relay-log=/var/lib/mysql/mysql-relay-bin",
		"--relay-log-index=/var/lib/mysql/mysql-relay-bin.index",
		"--log-replica-updates=ON",
		"--skip-replica-start=ON",
		"--plugin-load-add=mysql_clone.so",
		"--read-only=ON",
	}
	if got := mysqlServerArgs(101, false); !reflect.DeepEqual(got, want) {
		t.Fatalf("args = %q, want %q", got, want)
	}
	wantTLS := append(append([]string{}, want...),
		"--ssl-ca=/etc/mysql/tls/ca.crt", "--ssl-cert=/etc/mysql/tls/tls.crt", "--ssl-key=/etc/mysql/tls/tls.key", "--require-secure-transport=ON")
	if got := mysqlServerArgs(101, true); !reflect.DeepEqual(got, wantTLS) {
		t.Fatalf("TLS args = %q, want %q", got, wantTLS)
	}
}

// runLauncherStep runs a launcher script fragment the way the container
// does (bash -ec, $0 = mysqld) and returns its stdout, stderr and exit
// code.
func runLauncherStep(t *testing.T, script string, env []string, args ...string) (stdout, stderr string, code int) {
	t.Helper()
	bash, err := exec.LookPath("bash")
	if err != nil {
		t.Fatal("bash is required")
	}
	cmd := exec.Command(bash, append([]string{"-ec", "set -euo pipefail\n" + script, "mysqld"}, args...)...)
	cmd.Env = append([]string{"PATH=" + os.Getenv("PATH")}, env...)
	var out, errOut bytes.Buffer
	cmd.Stdout, cmd.Stderr = &out, &errOut
	err = cmd.Run()
	if exit, ok := err.(*exec.ExitError); ok {
		code = exit.ExitCode()
	} else if err != nil {
		t.Fatalf("run launcher step: %v", err)
	}
	return out.String(), errOut.String(), code
}

// TestServerUUIDRotationScript runs the rendered server_uuid step against
// data directories in each state TestMySQLLauncherNative observes on a
// real server, and in the ones it cannot produce there (a crash between
// the unlink and mysqld, an auto.cnf that cannot be removed, a #clone
// that cannot be read).
func TestServerUUIDRotationScript(t *testing.T) {
	const uuid = "8d8c6b2a-dd2f-11f1-87db-8e979c620586"
	autoCnf := "[auto]\nserver-uuid=" + uuid + "\n"
	// What a 9.7 server keeps in #clone after a clone it has installed.
	persisted := []string{"#view_status", "#view_progress", "#status_recovery"}
	// What a finished clone leaves for the restart, and an unfinished one.
	finished := []string{"#replace_files", "#new_files", "#old_files", "#status_fix", "#view_status", "#view_progress"}
	interrupted := []string{"#replace_files", "#new_files", "#old_files", "#status_error", "#view_status", "#view_progress"}

	for _, tc := range []struct {
		name string
		// autoCnf is the file's content; "" means it does not exist.
		autoCnf string
		// clone lists the files in #clone; nil means no #clone directory.
		clone []string
		// keyring is the keyring data file's mode with encryption at rest
		// (0: no encryption, no file).
		keyring os.FileMode
		// binlogOff renders the step for coverage.binaryLog=false; persisted
		// is the content of mysqld-auto.cnf ("" means no such file).
		binlogOff bool
		persisted string
		// lock makes the step's failure paths reachable (non-root only).
		lock        func(t *testing.T, datadir string)
		wantRemoved bool
		// wantLog is a substring of the single line the step logs; ""
		// means it logs nothing.
		wantLog string
	}{
		{name: "empty volume (first boot)"},
		{name: "initialized, never cloned", autoCnf: autoCnf},
		{name: "ordinary restart after a clone", autoCnf: autoCnf, clone: persisted},
		{name: "empty #clone", autoCnf: autoCnf, clone: []string{}},
		{name: "finished clone", autoCnf: autoCnf, clone: finished, wantRemoved: true,
			wantLog: "a finished clone is staged in DATADIR/#clone; removed auto.cnf so mysqld generates a new server_uuid (previous: " + uuid + ")"},
		{name: "finished clone, underscore key without a trailing newline", autoCnf: "[auto]\nserver_uuid=" + uuid, clone: finished, wantRemoved: true,
			wantLog: "(previous: " + uuid + ")"},
		{name: "finished clone, crash after the unlink", clone: finished, wantRemoved: true,
			wantLog: "removed auto.cnf so mysqld generates a new server_uuid (previous: unknown)"},
		{name: "interrupted clone", autoCnf: autoCnf, clone: interrupted,
			wantLog: "an interrupted clone is staged in DATADIR/#clone; mysqld rolls it back and server_uuid stays " + uuid},
		{name: "interrupted clone, older marker name", autoCnf: autoCnf, clone: []string{"#replace_files", "#status_in_progress"},
			wantLog: "an interrupted clone is staged in DATADIR/#clone"},
		{name: "failed clone that was rolled back in place", autoCnf: autoCnf, clone: []string{"#view_status", "#view_progress"}},
		{name: "finished clone, unsealed keyring", autoCnf: autoCnf, clone: finished, keyring: 0o600, wantRemoved: true,
			wantLog: "removed auto.cnf so mysqld generates a new server_uuid (previous: " + uuid + ")"},
		{name: "finished clone, sealed keyring", autoCnf: autoCnf, clone: finished, keyring: 0o444, lock: func(*testing.T, string) {},
			wantLog: "WARNING: a finished clone is staged in DATADIR/#clone but the keyring KEYRING is read-only (the site is sealed), and a new server_uuid needs a new binary log encryption key there; server_uuid is NOT rotated (stays " + uuid + ")"},
		// Without binary log encryption a new server_uuid needs no new key:
		// a sealed pod rotates (R4 B3).
		{name: "finished clone, sealed keyring, binary log not encrypted", autoCnf: autoCnf, clone: finished, keyring: 0o444, binlogOff: true,
			lock: func(*testing.T, string) {}, wantRemoved: true,
			wantLog: "removed auto.cnf so mysqld generates a new server_uuid (previous: " + uuid + ")"},
		{name: "finished clone, sealed keyring, binary log not encrypted, other variables persisted", autoCnf: autoCnf, clone: finished, keyring: 0o444, binlogOff: true,
			persisted: `{"Version": 2, "mysql_dynamic_variables": {"max_connections": {"Value": "500"}}}`,
			lock:      func(*testing.T, string) {}, wantRemoved: true,
			wantLog: "removed auto.cnf so mysqld generates a new server_uuid (previous: " + uuid + ")"},
		// ... unless SET PERSIST may have turned it on over the rendered
		// my.cnf: mysqld would abort on the key it cannot store.
		{name: "finished clone, sealed keyring, binlog_encryption persisted", autoCnf: autoCnf, clone: finished, keyring: 0o444, binlogOff: true,
			persisted: `{"Version": 2, "mysql_dynamic_variables": {"binlog_encryption": {"Value": "ON"}}}`,
			lock:      func(*testing.T, string) {},
			wantLog:   "is read-only (the site is sealed), and a new server_uuid needs a new binary log encryption key there; server_uuid is NOT rotated (stays " + uuid + ")"},
		{name: "finished clone, unsealed keyring, binlog_encryption persisted", autoCnf: autoCnf, clone: finished, keyring: 0o600, binlogOff: true,
			persisted: `{"Version": 2, "mysql_dynamic_variables": {"binlog_encryption": {"Value": "ON"}}}`, wantRemoved: true,
			wantLog: "removed auto.cnf so mysqld generates a new server_uuid (previous: " + uuid + ")"},
		{name: "interrupted clone, sealed keyring", autoCnf: autoCnf, clone: interrupted, keyring: 0o444,
			wantLog: "an interrupted clone is staged in DATADIR/#clone"},
		{name: "ordinary restart, sealed keyring", autoCnf: autoCnf, clone: persisted, keyring: 0o444},
		{name: "auto.cnf cannot be removed", autoCnf: autoCnf, clone: finished,
			lock: func(t *testing.T, datadir string) {
				if err := os.Chmod(datadir, 0o555); err != nil {
					t.Fatal(err)
				}
				t.Cleanup(func() { _ = os.Chmod(datadir, 0o755) })
			},
			wantLog: "WARNING: a finished clone is staged in DATADIR/#clone but DATADIR/auto.cnf could not be removed as uid " + strings.TrimSpace(mustOutput(t, "id", "-u")) + "; server_uuid is NOT rotated (stays " + uuid + ")"},
		{name: "#clone cannot be inspected", autoCnf: autoCnf, clone: finished,
			lock: func(t *testing.T, datadir string) {
				clone := filepath.Join(datadir, "#clone")
				if err := os.Chmod(clone, 0); err != nil {
					t.Fatal(err)
				}
				t.Cleanup(func() { _ = os.Chmod(clone, 0o755) })
			},
			wantLog: "WARNING: cannot inspect DATADIR/#clone as uid " + strings.TrimSpace(mustOutput(t, "id", "-u")) + "; if a clone is staged there, server_uuid is NOT rotated"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if tc.lock != nil && os.Getuid() == 0 {
				t.Skip("file permissions do not stop root")
			}
			datadir := t.TempDir()
			auto := filepath.Join(datadir, "auto.cnf")
			if tc.autoCnf != "" {
				if err := os.WriteFile(auto, []byte(tc.autoCnf), 0o640); err != nil {
					t.Fatal(err)
				}
			}
			if tc.clone != nil {
				if err := os.Mkdir(filepath.Join(datadir, "#clone"), 0o750); err != nil {
					t.Fatal(err)
				}
				for _, f := range tc.clone {
					if err := os.WriteFile(filepath.Join(datadir, "#clone", f), []byte("x\n"), 0o640); err != nil {
						t.Fatal(err)
					}
				}
			}
			keyring := ""
			if tc.keyring != 0 {
				keyring = filepath.Join(t.TempDir(), "keyring")
				if err := os.WriteFile(keyring, []byte("{}\n"), tc.keyring); err != nil {
					t.Fatal(err)
				}
			}
			if tc.persisted != "" {
				if err := os.WriteFile(filepath.Join(datadir, "mysqld-auto.cnf"), []byte(tc.persisted), 0o640); err != nil {
					t.Fatal(err)
				}
			}
			if tc.lock != nil {
				tc.lock(t, datadir)
			}
			binlogEncrypted := keyring != "" && !tc.binlogOff

			// The step is followed by the rest of the launcher: it must
			// reach it in every case, whatever it decided.
			stdout, stderr, code := runLauncherStep(t, serverUUIDRotationScript(datadir, keyring, binlogEncrypted)+"echo handoff\n", nil)
			if code != 0 || stdout != "handoff\n" {
				t.Fatalf("the step stopped the launcher: exit %d, stdout %q, stderr %q", code, stdout, stderr)
			}
			_, statErr := os.Stat(auto)
			removed := tc.autoCnf != "" && os.IsNotExist(statErr)
			if tc.autoCnf == "" {
				removed = tc.wantRemoved // nothing to remove; only the log tells
				if !os.IsNotExist(statErr) {
					t.Fatalf("the step created auto.cnf")
				}
			}
			if removed != tc.wantRemoved {
				t.Fatalf("auto.cnf removed = %v, want %v (stderr %q)", removed, tc.wantRemoved, stderr)
			}
			if !tc.wantRemoved && tc.autoCnf != "" {
				if got, err := os.ReadFile(auto); err != nil || string(got) != tc.autoCnf {
					t.Fatalf("auto.cnf changed: %q, %v", got, err)
				}
			}
			if tc.wantLog == "" {
				if stderr != "" {
					t.Fatalf("the step logged on a start with nothing staged: %q", stderr)
				}
				return
			}
			want := strings.NewReplacer("DATADIR", datadir, "KEYRING", keyring).Replace(tc.wantLog)
			lines := strings.Split(strings.TrimRight(stderr, "\n"), "\n")
			if len(lines) != 1 || !strings.HasPrefix(lines[0], mysqlLauncherLogPrefix+" ") || !strings.Contains(lines[0], want) {
				t.Fatalf("stderr = %q, want one %q line containing %q", stderr, mysqlLauncherLogPrefix, want)
			}
			// A second start after mysqld consumed the staged files and
			// wrote a new auto.cnf: nothing is staged, nothing rotates.
			if tc.wantRemoved {
				for _, f := range []string{"#replace_files", "#new_files", "#old_files", "#status_fix"} {
					_ = os.Remove(filepath.Join(datadir, "#clone", f))
				}
				if err := os.WriteFile(auto, []byte("[auto]\nserver-uuid=new\n"), 0o640); err != nil {
					t.Fatal(err)
				}
				_, stderr, code := runLauncherStep(t, serverUUIDRotationScript(datadir, keyring, binlogEncrypted), nil)
				if got, err := os.ReadFile(auto); code != 0 || stderr != "" || err != nil || string(got) != "[auto]\nserver-uuid=new\n" {
					t.Fatalf("the start after the clone was installed rotated again: exit %d, stderr %q, auto.cnf %q, %v", code, stderr, got, err)
				}
			}
		})
	}
}

// The paths in the step are data, never shell: a keyring path with command
// substitutions, backticks and quotes is compared as a path and nothing in
// it runs. (Admission restricts the keyring paths to [A-Za-z0-9._/-]; the
// script does not depend on that.)
func TestServerUUIDRotationScript_PathsAreNotShell(t *testing.T) {
	work := t.TempDir()
	datadir := filepath.Join(work, "data dir $(touch injected-datadir)")
	if err := os.MkdirAll(filepath.Join(datadir, "#clone"), 0o750); err != nil {
		t.Fatal(err)
	}
	for _, f := range []string{"#clone/#replace_files", "auto.cnf"} {
		if err := os.WriteFile(filepath.Join(datadir, f), []byte("[auto]\nserver-uuid=u\n"), 0o640); err != nil {
			t.Fatal(err)
		}
	}
	keyring := "/nonexistent/$(touch injected-subst)/`touch injected-backtick`/'; touch injected-quote; '/keyring"
	bash, err := exec.LookPath("bash")
	if err != nil {
		t.Fatal("bash is required")
	}
	cmd := exec.Command(bash, "-ec", "set -euo pipefail\n"+serverUUIDRotationScript(datadir, keyring, true)+"echo handoff\n", "mysqld")
	cmd.Dir = work
	out, err := cmd.CombinedOutput()
	if err != nil || !strings.HasSuffix(string(out), "handoff\n") {
		t.Fatalf("the step failed on unusual paths: %v\n%s", err, out)
	}
	entries, err := os.ReadDir(work)
	if err != nil {
		t.Fatal(err)
	}
	for _, e := range entries {
		if strings.HasPrefix(e.Name(), "injected") {
			t.Fatalf("a path was executed as shell: %s was created\n%s", e.Name(), out)
		}
	}
	// The step still did its work in the oddly named data directory.
	if _, err := os.Stat(filepath.Join(datadir, "auto.cnf")); !os.IsNotExist(err) {
		t.Fatalf("auto.cnf was not removed in %q: %v\n%s", datadir, err, out)
	}
	if got, want := shellSingleQuote("a'b"), `'a'\''b'`; got != want {
		t.Fatalf("shellSingleQuote = %s, want %s", got, want)
	}
}

func mustOutput(t *testing.T, name string, args ...string) string {
	t.Helper()
	out, err := exec.Command(name, args...).Output()
	if err != nil {
		t.Fatalf("%s: %v", name, err)
	}
	return string(out)
}

// TestMySQLEntrypointHandoffScript runs the rendered handoff against fake
// entrypoints: the first existing candidate wins, docker-entrypoint.sh on
// PATH is the fallback, the argv reaches the entrypoint unchanged, and an
// image with none fails the container with a line that says why.
func TestMySQLEntrypointHandoffScript(t *testing.T) {
	dir := t.TempDir()
	fake := func(name string) string {
		p := filepath.Join(dir, name)
		if err := os.MkdirAll(filepath.Dir(p), 0o755); err != nil {
			t.Fatal(err)
		}
		// Print which entrypoint ran and its argv, one argument per line.
		if err := os.WriteFile(p, []byte("#!/bin/sh\necho \"ran="+name+"\"\nfor a in \"$@\"; do echo \"arg=$a\"; done\n"), 0o755); err != nil {
			t.Fatal(err)
		}
		return p
	}
	first, second := fake("first.sh"), fake("second.sh")
	missing := filepath.Join(dir, "missing.sh")
	onPath := fake("bin/docker-entrypoint.sh")
	logStep := `log() { echo "` + mysqlLauncherLogPrefix + ` $*" >&2; }` + "\n"
	flags := []string{"--server-id=7", "--init-connect=SET NAMES utf8mb4"}
	wantArgv := "arg=mysqld\narg=--server-id=7\narg=--init-connect=SET NAMES utf8mb4\n"

	for _, tc := range []struct {
		name       string
		candidates []string
		path       string // prepended to PATH
		wantRan    string // "" means the handoff must fail
	}{
		{name: "first candidate", candidates: []string{first, second}, wantRan: "first.sh"},
		{name: "second candidate when the first is absent", candidates: []string{missing, second}, wantRan: "second.sh"},
		{name: "a candidate that is not executable is skipped", candidates: []string{filepath.Join(dir, "first.sh.txt"), second}, wantRan: "second.sh"},
		{name: "docker-entrypoint.sh on PATH", candidates: []string{missing}, path: filepath.Dir(onPath), wantRan: "bin/docker-entrypoint.sh"},
		{name: "no entrypoint", candidates: []string{missing}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if err := os.WriteFile(filepath.Join(dir, "first.sh.txt"), []byte("not executable\n"), 0o644); err != nil {
				t.Fatal(err)
			}
			var env []string
			if tc.path != "" {
				env = []string{"PATH=" + tc.path + ":" + os.Getenv("PATH")}
			}
			stdout, stderr, code := runLauncherStep(t, logStep+mysqlEntrypointHandoffScriptFor(tc.candidates), env, flags...)
			if tc.wantRan == "" {
				if code != 127 || stdout != "" {
					t.Fatalf("exit %d, stdout %q; want 127 and no entrypoint run", code, stdout)
				}
				// One line: what was looked for, and what to do about it.
				want := mysqlLauncherLogPrefix + " no MySQL image entrypoint found (looked for " + missing + ", then docker-entrypoint.sh on PATH); spec.image must provide one. " +
					"This container cannot start until it does: set spec.image to an image that has it (the Docker Official Image and Oracle's community-server image do), " +
					"or add the image's entrypoint script at one of those paths\n"
				if stderr != want {
					t.Fatalf("stderr = %q, want %q", stderr, want)
				}
				return
			}
			if want := "ran=" + tc.wantRan + "\n" + wantArgv; code != 0 || stdout != want {
				t.Fatalf("exit %d, stdout %q, stderr %q; want %q", code, stdout, stderr, want)
			}
		})
	}

	// The paths are written into the script unquoted.
	for _, c := range mysqlImageEntrypoints {
		if strings.ContainsAny(c, " \t\n\"'`$\\;&|<>(){}*?[]~#") || !filepath.IsAbs(c) {
			t.Errorf("mysqlImageEntrypoints entry %q must be an absolute path without shell metacharacters", c)
		}
	}
}

// TestReconcileDeployment_MySQLStartsThroughLauncher: every group's mysql
// container runs the launcher, not the image entrypoint directly, and the
// spec hash covers the launcher's text so a later change to it reaches
// pods that already run (an existing Deployment is only updated through
// the ordered update that a hash change starts).
func TestReconcileDeployment_MySQLStartsThroughLauncher(t *testing.T) {
	fg := newTestFG()
	r, c := newReconciler(fg)
	if _, err := r.Reconcile(context.Background(), ctrl.Request{
		NamespacedName: types.NamespacedName{Name: fg.Name, Namespace: fg.Namespace},
	}); err != nil {
		t.Fatalf("reconcile: %v", err)
	}
	var d appsv1.Deployment
	if err := c.Get(context.Background(), types.NamespacedName{Name: "mysql-lion-dc1", Namespace: fg.Namespace}, &d); err != nil {
		t.Fatalf("deployment: %v", err)
	}
	mysqlC := containerByName(d.Spec.Template.Spec.Containers, "mysql")
	if mysqlC == nil {
		t.Fatal("deployment has no mysql container")
	}
	site := fg.Spec.Sites[0]
	serverID, err := site.MySQLServerID()
	if err != nil {
		t.Fatal(err)
	}
	wantCommand, wantArgs := mysqlLauncher(fg, mysqlServerArgs(serverID, false))
	if !reflect.DeepEqual(mysqlC.Command, wantCommand) {
		t.Fatalf("mysql command = %q, want the launcher %q", mysqlC.Command, wantCommand)
	}
	if !reflect.DeepEqual(mysqlC.Args, wantArgs) || mysqlC.Args[0] != "mysqld" {
		t.Fatalf("mysql args = %q, want %q", mysqlC.Args, wantArgs)
	}
	if !strings.Contains(mysqlC.Command[2], "#replace_files") || !strings.Contains(mysqlC.Command[2], `exec "$entrypoint" "$0" "$@"`) {
		t.Fatalf("the rendered launcher lacks the server_uuid step or the entrypoint handoff:\n%s", mysqlC.Command[2])
	}
	if findMount(mysqlC.VolumeMounts, mysqlDataDir) == nil {
		t.Fatalf("the launcher reads %s, which is not mounted: %v", mysqlDataDir, mysqlC.VolumeMounts)
	}

	// The hash follows the launcher's content.
	before := ComputeSpecHash(fg, site, "", testPITRRender(fg))
	saved := mysqlImageEntrypoints
	mysqlImageEntrypoints = append(append([]string{}, saved...), "/opt/other/entrypoint.sh")
	after := ComputeSpecHash(fg, site, "", testPITRRender(fg))
	mysqlImageEntrypoints = saved
	if before == after {
		t.Fatal("the spec hash did not change with the launcher script: a launcher change would never roll out")
	}
	if again := ComputeSpecHash(fg, site, "", testPITRRender(fg)); again != before {
		t.Fatalf("the spec hash is not stable: %s then %s", before, again)
	}
}
