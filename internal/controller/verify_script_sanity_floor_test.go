package controller

import (
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"testing"

	v1alpha1 "github.com/shipstream/bloodraven/api/v1alpha1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// TestVerifyScriptSanityFloor runs verify_script.sh's sanity-check block
// against a fake mysql client returning signed and oversized integers. An
// explicit minResult (0 included) rejects a smaller result; an unset one
// compares nothing.
func TestVerifyScriptSanityFloor(t *testing.T) {
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
		name   string
		result string
		floor  *int64
		want   v1alpha1.SanityCheckOutcome
	}{
		{name: "negative, floor unset", result: "-1", want: v1alpha1.SanityCheckOutcomePassed},
		{name: "negative, floor 0", result: "-1", floor: new(int64(0)), want: v1alpha1.SanityCheckOutcomeFailed},
		{name: "negative, floor 5", result: "-1", floor: new(int64(5)), want: v1alpha1.SanityCheckOutcomeFailed},
		{name: "zero, floor 0", result: "0", floor: new(int64(0)), want: v1alpha1.SanityCheckOutcomePassed},
		{name: "below floor", result: "4", floor: new(int64(5)), want: v1alpha1.SanityCheckOutcomeFailed},
		{name: "at floor", result: "5", floor: new(int64(5)), want: v1alpha1.SanityCheckOutcomePassed},
		{name: "above floor", result: "12", floor: new(int64(5)), want: v1alpha1.SanityCheckOutcomePassed},
		{name: "beyond int64", result: "18446744073709551615", floor: new(int64(9223372036854775807)), want: v1alpha1.SanityCheckOutcomePassed},
		{name: "large negative", result: "-9223372036854775808", floor: new(int64(0)), want: v1alpha1.SanityCheckOutcomeFailed},
		{name: "non-integer", result: "abc", floor: new(int64(5)), want: v1alpha1.SanityCheckOutcomePassed},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			dir := t.TempDir()
			bin := filepath.Join(dir, "bin")
			if err := os.Mkdir(bin, 0o700); err != nil {
				t.Fatal(err)
			}
			if err := os.WriteFile(filepath.Join(bin, "mysql"), []byte("#!/bin/bash\necho '"+tt.result+"'\n"), 0o700); err != nil {
				t.Fatal(err)
			}
			cmd := exec.Command(bash, "-c", script)
			cmd.Env = append(os.Environ(),
				"PATH="+bin+":"+os.Getenv("PATH"),
				"BLOODRAVEN_VERIFY_SANITY_QUERY=SELECT x FROM t",
				"BLOODRAVEN_VERIFY_SANITY_MAX_SECONDS=5",
				"BLOODRAVEN_TERMINATION_LOG="+filepath.Join(dir, "termination-log"),
			)
			if tt.floor != nil {
				cmd.Env = append(cmd.Env, "BLOODRAVEN_VERIFY_SANITY_MIN_ROWS="+strconv.FormatInt(*tt.floor, 10))
			}
			out, err := cmd.CombinedOutput()
			if (tt.want == v1alpha1.SanityCheckOutcomeFailed) != (err != nil) {
				t.Fatalf("script error %v, want outcome %s:\n%s", err, tt.want, out)
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
			if res.Outcome != tt.want {
				t.Fatalf("outcome %q, want %q\n%s", res.Outcome, tt.want, out)
			}
		})
	}
}

// TestVerificationJobSanityFloorPresence pins that the floor env is
// rendered exactly when spec.sanityCheck.minResult is set, so an explicit
// 0 is distinguishable from unset.
func TestVerificationJobSanityFloorPresence(t *testing.T) {
	for _, tt := range []struct {
		name  string
		floor *int64
		want  string
		set   bool
	}{
		{name: "unset"},
		{name: "zero", floor: new(int64(0)), want: "0", set: true},
		{name: "five", floor: new(int64(5)), want: "5", set: true},
	} {
		t.Run(tt.name, func(t *testing.T) {
			fg := verifyFG()
			backup := successfulBackup("happy", "lion", "nightly-s3")
			v := &v1alpha1.MysqlBackupVerification{
				ObjectMeta: metav1.ObjectMeta{Name: "verify-floor", Namespace: "ns"},
				Spec: v1alpha1.MysqlBackupVerificationSpec{
					FailoverGroupRef: v1alpha1.LocalFailoverGroupRef{Name: "lion"},
					ProfileName:      "nightly-s3",
				},
			}
			v.Spec.SanityCheck = &v1alpha1.SanityCheckSpec{Query: "SELECT 1", MinResult: tt.floor, Timeout: &metav1.Duration{Duration: 60e9}}
			job, err := buildVerificationJob(verificationJobInputs{
				FailoverGroup: fg, Profile: fg.Spec.Backup.Profiles[0], Verification: v, Backup: backup,
				CredsSecretName: "c", ScriptsConfigMapName: "s",
			})
			if err != nil {
				t.Fatalf("buildVerificationJob: %v", err)
			}
			got, set := envValue(job.Spec.Template.Spec.Containers[0].Env, "BLOODRAVEN_VERIFY_SANITY_MIN_ROWS")
			if set != tt.set || got != tt.want {
				t.Fatalf("BLOODRAVEN_VERIFY_SANITY_MIN_ROWS = %q (set %v), want %q (set %v)", got, set, tt.want, tt.set)
			}
		})
	}
}
