package controller

import (
	"errors"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
)

// The restore Job's last line of defence: with
// BLOODRAVEN_REQUIRE_OFFLINE_MODE set it exits 2 before dropping anything
// unless @@GLOBAL.offline_mode is ON. Runs the embedded script's function
// under python3 with a stub mysqlsh module.
func TestRestoreScript_RequireOfflineMode(t *testing.T) {
	py, err := exec.LookPath("python3")
	if err != nil {
		t.Skip("python3 not installed")
	}
	dir := t.TempDir()
	if err := os.WriteFile(filepath.Join(dir, "restore_script.py"), []byte(restoreScript), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, "mysqlsh.py"), []byte("globals = None\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	const driver = `
import sys
sys.path.insert(0, sys.argv[1])
import restore_script as rs

class Result:
    def __init__(self, row):
        self.row = row
    def fetch_one(self):
        return self.row

class Session:
    def __init__(self, value):
        self.value = value
        self.queries = []
    def run_sql(self, q):
        self.queries.append(q)
        if self.value == "error":
            raise RuntimeError("lost connection")
        return Result((self.value,))

s = Session(sys.argv[2])
rs._require_offline_mode(s)
print("RETURNED queries=%d" % len(s.queries))
`
	for _, tc := range []struct {
		name, require, value string
		wantExit             int
		wantOut              string
	}{
		{name: "not required", value: "0", wantOut: "RETURNED queries=0"},
		{name: "on", require: "1", value: "1", wantOut: "BLOODRAVEN_OFFLINE_FENCE_OK"},
		{name: "on as text", require: "1", value: "ON", wantOut: "BLOODRAVEN_OFFLINE_FENCE_OK"},
		{name: "off", require: "1", value: "0", wantExit: 2, wantOut: "BLOODRAVEN_OFFLINE_FENCE_MISSING: @@GLOBAL.offline_mode is OFF"},
		{name: "unreadable", require: "1", value: "error", wantExit: 2, wantOut: "BLOODRAVEN_OFFLINE_FENCE_MISSING: cannot read"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			cmd := exec.Command(py, "-c", driver, dir, tc.value)
			cmd.Env = append(os.Environ(), "BLOODRAVEN_REQUIRE_OFFLINE_MODE="+tc.require)
			out, err := cmd.CombinedOutput()
			exit := 0
			var ee *exec.ExitError
			if errors.As(err, &ee) {
				exit = ee.ExitCode()
			} else if err != nil {
				t.Fatal(err)
			}
			if exit != tc.wantExit || !strings.Contains(string(out), tc.wantOut) {
				t.Fatalf("exit %d output %q, want exit %d containing %q", exit, out, tc.wantExit, tc.wantOut)
			}
			if tc.wantExit != 0 && strings.Contains(string(out), "RETURNED") {
				t.Fatal("the script went on after the fence check failed")
			}
		})
	}

	// The check runs before anything is reset or dropped.
	check := strings.Index(restoreScript, "        _require_offline_mode(session)")
	reset := strings.Index(restoreScript, "            _maybe_reset_replication(session)")
	drops := strings.Index(restoreScript, "            _maybe_preflight_drops(session)")
	if check < 0 || reset < 0 || drops < 0 || check > reset || check > drops {
		t.Fatalf("_require_offline_mode must run before the reset and drops (offsets %d, %d, %d)", check, reset, drops)
	}
}
