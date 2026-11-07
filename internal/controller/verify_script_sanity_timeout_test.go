package controller

import (
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"

	v1alpha1 "github.com/shipstream/bloodraven/api/v1alpha1"
)

// TestVerifyScriptSanityTimeoutOutcome runs verify_script.sh's sanity-check
// block under bash against a fake mysql client. A query that outlives the
// timeout must report outcome TimedOut, not Failed: `timeout
// --preserve-status` returned the killed client's 143, never the 124 the
// branch tests, so TimedOut was unreachable.
func TestVerifyScriptSanityTimeoutOutcome(t *testing.T) {
	bash, err := exec.LookPath("bash")
	if err != nil {
		t.Skip("bash not available")
	}
	if _, err := exec.LookPath("timeout"); err != nil {
		t.Skip("timeout not available")
	}
	helpersStart := strings.Index(verifyScript, "log() {")
	helpersEnd := strings.Index(verifyScript, "cleanup_mysqld() {")
	blockStart := strings.Index(verifyScript, "# ---- Sanity check")
	blockEnd := strings.Index(verifyScript, "# Publishing the verdict")
	if helpersStart < 0 || helpersEnd < helpersStart || blockStart < 0 || blockEnd < blockStart {
		t.Fatal("verify_script.sh layout changed; update the section markers")
	}
	script := "set -euo pipefail\nSOCKET=/nonexistent\n" + verifyScript[helpersStart:helpersEnd] + verifyScript[blockStart:blockEnd] + "exit 0\n"

	tests := []struct {
		name        string
		fakeMySQL   string
		wantOutcome v1alpha1.SanityCheckOutcome
		wantFail    string
	}{
		{name: "slow query", fakeMySQL: "exec sleep 30", wantOutcome: v1alpha1.SanityCheckOutcomeTimedOut, wantFail: "SanityCheckTimeout"},
		{name: "query error", fakeMySQL: "echo 'ERROR 1146: no such table' >&2; exit 1", wantOutcome: v1alpha1.SanityCheckOutcomeFailed, wantFail: "SanityCheckFailed"},
		{name: "passes", fakeMySQL: "echo 42", wantOutcome: v1alpha1.SanityCheckOutcomePassed},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			dir := t.TempDir()
			bin := filepath.Join(dir, "bin")
			if err := os.Mkdir(bin, 0o700); err != nil {
				t.Fatal(err)
			}
			if err := os.WriteFile(filepath.Join(bin, "mysql"), []byte("#!/bin/bash\n"+tt.fakeMySQL+"\n"), 0o700); err != nil {
				t.Fatal(err)
			}
			termLog := filepath.Join(dir, "termination-log")
			cmd := exec.Command(bash, "-c", script)
			cmd.Env = append(os.Environ(),
				"PATH="+bin+":"+os.Getenv("PATH"),
				"BLOODRAVEN_VERIFY_SANITY_QUERY=SELECT COUNT(*) FROM orders",
				"BLOODRAVEN_VERIFY_SANITY_MAX_SECONDS=1",
				"BLOODRAVEN_TERMINATION_LOG="+termLog,
			)
			out, err := cmd.CombinedOutput()
			if tt.wantFail == "" && err != nil {
				t.Fatalf("script failed: %v\n%s", err, out)
			}
			if tt.wantFail != "" {
				if err == nil {
					t.Fatalf("script succeeded, want %s:\n%s", tt.wantFail, out)
				}
				msg, _ := os.ReadFile(termLog)
				if !strings.HasPrefix(string(msg), tt.wantFail+":") {
					t.Fatalf("termination message %q, want %s", msg, tt.wantFail)
				}
			}
			var sentinel string
			for _, line := range strings.Split(string(out), "\n") {
				if strings.HasPrefix(line, "BLOODRAVEN_VERIFY_SANITY_COMPLETE ") {
					sentinel = line
				}
			}
			res, ok := parseSanitySentinel(sentinel)
			if !ok {
				t.Fatalf("no sanity sentinel in:\n%s", out)
			}
			if res.Outcome != tt.wantOutcome {
				t.Fatalf("outcome %q, want %q (result %+v)\n%s", res.Outcome, tt.wantOutcome, res, out)
			}
		})
	}
}
