// Package doctor tests the bloodraven-doctor skill's shell scripts against
// a stub kubectl.
package doctor

import (
	"errors"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
)

const (
	primaryUUID = "d07c0c83-71ca-11f0-9e33-c80aa9429562"
	replicaUUID = "d1240b6c-71ca-11f0-9e33-c80aa9429563"
)

// stubKubectl answers the reads gtid-audit.sh makes for a two-site group
// "main" (dc1 active, dc2 follower). Like a real cluster whose CRD lacks
// the short name, it rejects the "mfg" resource name. STUB_GEN_BEFORE and
// STUB_GEN_AFTER drive the two status.topologyGeneration reads: a value,
// "" (field missing), or FAIL (the read fails).
const stubKubectl = `#!/usr/bin/env bash
args=" $* "
if [[ "$args" == *" mfg "* ]]; then
  echo 'error: the server does not have a resource type "mfg"' >&2; exit 1
fi
case "$args" in
  *" config view "*) echo ns; exit 0;;
  *" get mysqlfailovergroups.shipstream.io main "*topologyGeneration*)
    n=$(cat "$STUB_DIR/gen" 2>/dev/null || echo 0); echo $((n+1)) > "$STUB_DIR/gen"
    v="$STUB_GEN_BEFORE"; [[ "$n" -ge 1 ]] && v="$STUB_GEN_AFTER"
    [[ "$v" == FAIL ]] && { echo "error: connection refused" >&2; exit 1; }
    printf '%s' "$v"; exit 0;;
  *" get mysqlfailovergroups.shipstream.io main "*activeSite*) printf dc1; exit 0;;
  *" get mysqlfailovergroups.shipstream.io main "*spec.sites*) printf 'dc1 dc2'; exit 0;;
  *" get mysqlfailovergroups.shipstream.io main "*recoveryState*) exit 0;;
  *" get mysqlfailovergroups.shipstream.io main "*divergentGtid*) exit 0;;
  *" get mysqlfailovergroups.shipstream.io main "*divergenceDetectedBy*) exit 0;;
  *" get pods "*"site=dc1"*) printf 'mysql-dc1-0\t\n'; exit 0;;
  *" get pods "*"site=dc2"*) printf 'mysql-dc2-0\t\n'; exit 0;;
  *" get pod "*containers*) echo "mysql sidecar"; exit 0;;
  *" exec "*)
    sql="${@: -1}"
    case "$args" in *mysql-dc1-0*) pod=dc1;; *) pod=dc2;; esac
    case "$pod:$sql" in
      "dc1:SELECT @@GLOBAL.read_only") echo 0;;
      "dc1:SELECT @@server_uuid") echo "` + primaryUUID + `";;
      "dc2:SELECT @@server_uuid") echo "` + replicaUUID + `";;
      dc2:"SELECT @@server_uuid, REPLACE"*) printf '%s\t%s\n' "` + replicaUUID + `" "$STUB_FOLLOWER_GTID";;
      dc1:"SELECT @@server_uuid, @@GLOBAL.read_only, REPLACE(GTID_SUBTRACT"*) printf '%s\t0\t%s\n' "` + primaryUUID + `" "$STUB_EXTRA";;
      *) echo "stub: unhandled SQL on $pod: $sql" >&2; exit 1;;
    esac
    exit 0;;
esac
echo "stub: unhandled kubectl $*" >&2
exit 1
`

func runAudit(t *testing.T, env map[string]string) (int, string) {
	t.Helper()
	_, file, _, _ := runtime.Caller(0)
	script := filepath.Join(filepath.Dir(file), "..", "..", ".agents", "skills", "bloodraven-doctor", "scripts", "gtid-audit.sh")
	dir := t.TempDir()
	if err := os.WriteFile(filepath.Join(dir, "kubectl"), []byte(stubKubectl), 0o755); err != nil {
		t.Fatal(err)
	}
	cmd := exec.Command("bash", script, "-n", "ns", "main")
	cmd.Env = append(os.Environ(), "PATH="+dir+string(os.PathListSeparator)+os.Getenv("PATH"), "STUB_DIR="+dir)
	for k, v := range env {
		cmd.Env = append(cmd.Env, k+"="+v)
	}
	out, err := cmd.CombinedOutput()
	var exit *exec.ExitError
	switch {
	case err == nil:
		return 0, string(out)
	case errors.As(err, &exit):
		return exit.ExitCode(), string(out)
	default:
		t.Fatalf("run %s: %v", script, err)
		return 0, ""
	}
}

func TestGTIDAuditScript(t *testing.T) {
	if _, err := exec.LookPath("bash"); err != nil {
		t.Skip("bash not available")
	}
	clean := primaryUUID + ":1-240010," + replicaUUID + ":1-1741932"
	divergent := primaryUUID + ":1-240010," + replicaUUID + ":1-1742153"
	for _, tc := range []struct {
		name       string
		env        map[string]string
		wantExit   int
		wantOutput string
	}{
		{"clean follower", map[string]string{"STUB_GEN_BEFORE": "7", "STUB_GEN_AFTER": "7", "STUB_FOLLOWER_GTID": clean, "STUB_EXTRA": ""},
			0, "clean=1 divergent=0 unknown=0"},
		{"#202 divergent follower", map[string]string{"STUB_GEN_BEFORE": "7", "STUB_GEN_AFTER": "7", "STUB_FOLLOWER_GTID": divergent, "STUB_EXTRA": replicaUUID + ":1741933-1742153"},
			2, replicaUUID + ":1741933-1742153"},
		{"topology changed during the audit", map[string]string{"STUB_GEN_BEFORE": "7", "STUB_GEN_AFTER": "8", "STUB_FOLLOWER_GTID": clean, "STUB_EXTRA": ""},
			3, "clean=0 divergent=0 unknown=1"},
		{"initial generation read fails", map[string]string{"STUB_GEN_BEFORE": "FAIL", "STUB_GEN_AFTER": "7", "STUB_FOLLOWER_GTID": clean, "STUB_EXTRA": ""},
			3, "clean=0 divergent=0 unknown=1"},
		{"final generation read fails", map[string]string{"STUB_GEN_BEFORE": "7", "STUB_GEN_AFTER": "FAIL", "STUB_FOLLOWER_GTID": clean, "STUB_EXTRA": ""},
			3, "clean=0 divergent=0 unknown=1"},
		{"generation field missing", map[string]string{"STUB_GEN_BEFORE": "", "STUB_GEN_AFTER": "", "STUB_FOLLOWER_GTID": clean, "STUB_EXTRA": ""},
			3, "clean=0 divergent=0 unknown=1"},
		{"divergence found while authority is unverified", map[string]string{"STUB_GEN_BEFORE": "7", "STUB_GEN_AFTER": "", "STUB_FOLLOWER_GTID": divergent, "STUB_EXTRA": replicaUUID + ":1741933-1742153"},
			3, "clean=0 divergent=0 unknown=1"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			code, out := runAudit(t, tc.env)
			if code != tc.wantExit || !strings.Contains(out, tc.wantOutput) {
				t.Fatalf("exit %d, want %d with %q; output:\n%s", code, tc.wantExit, tc.wantOutput, out)
			}
		})
	}
}
