package controller

import (
	"encoding/json"
	"strings"
	"testing"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	v1alpha1 "github.com/shipstream/bloodraven/api/v1alpha1"
)

// --- verify mysqld GTID flags (#101) -------------------------------------

// The verify mysqld must start gtid_mode=ON + enforce-gtid-consistency=ON so
// the raw `mysqlbinlog | mysql` PITR replay relies on server-side GTID dedup
// (mirroring the production in-place restore path), while keeping
// --skip-log-bin. The gtid_mode=ON server-side dedup behavior was validated on
// a live MySQL 8.0 server, as was the mysqlsh util.load_dump(updateGtidSet:
// "replace") path that restores gtid_purged (including the twice-replayed
// cross-site GTID that confirms dedup).
func TestBackupVerifyScript_StartsGtidModeOn(t *testing.T) {
	script := BackupVerifyScript()
	lines := strings.Split(script, "\n")

	// These flag strings also appear in the explanatory comment block, so a
	// plain strings.Contains over the whole script would pass even if the
	// runtime mysqld flags were deleted. Require each flag on a NON-comment
	// line (the actual mysqld invocation), so the test guards what it claims.
	for _, want := range []string{
		"--gtid-mode=ON",
		"--enforce-gtid-consistency=ON",
		"--skip-log-bin",
	} {
		found := false
		for _, line := range lines {
			if strings.HasPrefix(strings.TrimSpace(line), "#") {
				continue
			}
			if strings.Contains(line, want) {
				found = true
				break
			}
		}
		if !found {
			t.Errorf("verify_script.sh must contain %q on a non-comment (mysqld invocation) line", want)
		}
	}

	// The GTID flags belong to the runtime START invocation, not the
	// --initialize-insecure block (init does not run GTID logic). The init
	// invocation is a single `mysqld --initialize-insecure \` line; assert
	// the flags are NOT on that line. Also assert that line actually exists,
	// so the guard cannot silently become a no-op if the init block is
	// refactored.
	foundInitLine := false
	for _, line := range lines {
		if strings.Contains(line, "--initialize-insecure") {
			foundInitLine = true
			if strings.Contains(line, "--gtid-mode") || strings.Contains(line, "--enforce-gtid-consistency") {
				t.Errorf("GTID flags must not be on the --initialize-insecure line: %q", line)
			}
		}
	}
	if !foundInitLine {
		t.Error("expected a --initialize-insecure line in verify_script.sh (init/runtime guard would otherwise be a no-op)")
	}
}

// --- verification load options: updateGtidSet (#101) ---------------------

// The VERIFICATION load-options JSON must carry updateGtidSet:"replace" (to
// restore the dump's gtid_purged so the gtid_mode=ON verify server can dedup)
// while keeping skipBinlog:true. The shared backup/in-place-restore JSON
// (marshalLoadOptions) must stay free of updateGtidSet so its output is
// byte-for-byte unchanged.
func TestMarshalVerificationLoadOptions_InjectsUpdateGtidSet(t *testing.T) {
	verifyJSON, err := marshalVerificationLoadOptions(verificationLoadOptions())
	if err != nil {
		t.Fatalf("marshalVerificationLoadOptions: %v", err)
	}
	var verifyOpts map[string]any
	if err := json.Unmarshal([]byte(verifyJSON), &verifyOpts); err != nil {
		t.Fatalf("unmarshal verification load options %q: %v", verifyJSON, err)
	}
	if got, ok := verifyOpts["updateGtidSet"]; !ok || got != "replace" {
		t.Errorf("verification load options updateGtidSet=%v (ok=%v), want \"replace\"", got, ok)
	}
	if got, ok := verifyOpts["skipBinlog"]; !ok || got != true {
		t.Errorf("verification load options skipBinlog=%v (ok=%v), want true", got, ok)
	}

	// The backup / in-place-restore JSON must NOT carry updateGtidSet.
	baseJSON, err := marshalLoadOptions(verificationLoadOptions())
	if err != nil {
		t.Fatalf("marshalLoadOptions: %v", err)
	}
	var baseOpts map[string]any
	if err := json.Unmarshal([]byte(baseJSON), &baseOpts); err != nil {
		t.Fatalf("unmarshal base load options %q: %v", baseJSON, err)
	}
	if _, ok := baseOpts["updateGtidSet"]; ok {
		t.Errorf("backup/restore load options must not set updateGtidSet, got %q", baseJSON)
	}

	// Injecting updateGtidSet must not drop or perturb any base key: the verify
	// JSON must equal the base JSON plus the single updateGtidSet override.
	for k, want := range baseOpts {
		if got, ok := verifyOpts[k]; !ok || got != want {
			t.Errorf("verification load options key %q = %v (ok=%v), want %v (base key dropped/perturbed by injection)", k, got, ok, want)
		}
	}
	delete(verifyOpts, "updateGtidSet")
	if len(verifyOpts) != len(baseOpts) {
		t.Errorf("verification load options has %d keys besides updateGtidSet, want %d (same set as base)", len(verifyOpts), len(baseOpts))
	}
}

// The verification Job's BLOODRAVEN_LOAD_OPTIONS env var must carry the
// injected updateGtidSet so restore.py forwards it to util.load_dump
// (restore.py json.loads BLOODRAVEN_LOAD_OPTIONS into the options object it
// passes to load_dump).
func TestBuildVerificationJob_LoadOptionsEnvCarriesUpdateGtidSet(t *testing.T) {
	fg := verifyFG()
	backup := successfulBackup("gtid-happy", "lion", "nightly-s3")
	v := &v1alpha1.MysqlBackupVerification{
		ObjectMeta: metav1.ObjectMeta{Name: "verify-gtid", Namespace: "ns"},
		Spec: v1alpha1.MysqlBackupVerificationSpec{
			FailoverGroupRef: v1alpha1.LocalGroupRef{Name: "lion"},
			ProfileName:      "nightly-s3",
		},
	}
	job, err := buildVerificationJob(verificationJobInputs{
		FailoverGroup:        fg,
		Profile:              fg.Spec.Backup.Profiles[0],
		Verification:         v,
		Backup:               backup,
		CredsSecretName:      "c",
		ScriptsConfigMapName: "s",
	})
	if err != nil {
		t.Fatalf("buildVerificationJob: %v", err)
	}
	if len(job.Spec.Template.Spec.Containers) == 0 {
		t.Fatal("verification Job has no containers")
	}
	var loadOpts string
	for _, e := range job.Spec.Template.Spec.Containers[0].Env {
		if e.Name == "BLOODRAVEN_LOAD_OPTIONS" {
			loadOpts = e.Value
		}
	}
	if loadOpts == "" {
		t.Fatal("verification Job missing BLOODRAVEN_LOAD_OPTIONS env var")
	}
	var opts map[string]any
	if err := json.Unmarshal([]byte(loadOpts), &opts); err != nil {
		t.Fatalf("unmarshal BLOODRAVEN_LOAD_OPTIONS %q: %v", loadOpts, err)
	}
	if opts["updateGtidSet"] != "replace" {
		t.Errorf("BLOODRAVEN_LOAD_OPTIONS updateGtidSet=%v, want \"replace\" (%q)", opts["updateGtidSet"], loadOpts)
	}
	if opts["skipBinlog"] != true {
		t.Errorf("BLOODRAVEN_LOAD_OPTIONS skipBinlog=%v, want true (%q)", opts["skipBinlog"], loadOpts)
	}
}
