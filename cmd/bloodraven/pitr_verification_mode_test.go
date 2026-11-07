package main

import (
	"testing"
	"time"

	v1alpha1 "github.com/shipstream/bloodraven/api/v1alpha1"
	"github.com/shipstream/bloodraven/internal/controller"
	"github.com/shipstream/bloodraven/internal/pitrplan"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// F-10: a verification's CamelCase pointInTime.mode reaches pitr-download
// as a mode it accepts. The controller renders the replay request env (the
// same env its Job builder puts on pitr-download and restore.py); the
// download planner parses it.
func TestPITRDownloadAcceptsEveryVerificationMode(t *testing.T) {
	dest := downloadDest(t)
	for mode, want := range map[v1alpha1.PointInTimeMode]pitrplan.Mode{
		v1alpha1.PointInTimeModeLatest:    pitrplan.ModeLatest,
		v1alpha1.PointInTimeModeTimestamp: pitrplan.ModeTimestamp,
	} {
		t.Run(string(mode), func(t *testing.T) {
			spec := &v1alpha1.PointInTimeVerificationSpec{Mode: mode, ExcludeGtids: "3e11fa47-71ca-11e1-9e33-c80aa9429562:1"}
			if mode == v1alpha1.PointInTimeModeTimestamp {
				spec.StopTime = &metav1.Time{Time: time.Date(2030, 1, 1, 0, 0, 25, 0, time.UTC)}
			}
			env, err := controller.VerificationPITRRequestEnvForTest(spec)
			if err != nil {
				t.Fatal(err)
			}
			for k, v := range map[string]string{
				"BLOODRAVEN_PITR_DUMP_META": "/pitr-dump-meta/dump.json", "BLOODRAVEN_PITR_OUTPUT_DIR": "/pitr",
				"BLOODRAVEN_PITR_DESTINATION": dest.String(), "BLOODRAVEN_PITR_MANIFEST_PREFIX": dest.KeyPrefix,
				"BLOODRAVEN_PITR_ENCRYPTION_MODE": "None",
			} {
				env[k] = v
			}
			req, err := pitrDownloadRequestFromEnv(func(k string) string { return env[k] })
			if err != nil {
				t.Fatalf("pitr-download refused the rendered %s request %v: %v", mode, env, err)
			}
			if req.mode != want || req.allowPartial {
				t.Fatalf("%s: mode=%q allowPartial=%v, want %q and no partial", mode, req.mode, req.allowPartial, want)
			}
		})
	}
}
