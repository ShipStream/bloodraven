package bloodraven_test

import (
	"os/exec"
	"testing"
)

// TestRolloutValuesChart pins the env the operator reads for paced rollouts
// (rolloutPacingEnabled and rolloutPacerConfigFromEnv in cmd/bloodraven) and
// that the chart still renders when `rollout` is missing from the values.
//
// That is what `helm upgrade --reuse-values` from a release older than
// v1.8.0 does: helm renders the new templates over the old release's
// values, in which the key does not exist. The operations guide and the
// chart's NOTES.txt both tell users to upgrade that way, and a template
// that reads .Values.rollout.pacing directly fails there with
// "nil pointer evaluating interface {}.pacing". `--set rollout=null`
// removes the key the same way.
func TestRolloutValuesChart(t *testing.T) {
	requireHelm(t)
	for _, tt := range []struct {
		name string
		args []string
		want map[string]string
	}{
		{name: "defaults", want: map[string]string{
			"BLOODRAVEN_ROLLOUT_PACING": "true", "BLOODRAVEN_ROLLOUT_SOAK": "1m",
			"BLOODRAVEN_ROLLOUT_STALL_AFTER": "30m", "BLOODRAVEN_ROLLOUT_START_GRACE": "5m"}},
		{name: "pacing off", args: []string{"--set", "rollout.pacing=false"}, want: map[string]string{
			"BLOODRAVEN_ROLLOUT_PACING": "false", "BLOODRAVEN_ROLLOUT_SOAK": "1m"}},
		{name: "durations", args: []string{"--set", "rollout.soak=0s", "--set", "rollout.stallAfter=10m"}, want: map[string]string{
			"BLOODRAVEN_ROLLOUT_PACING": "true", "BLOODRAVEN_ROLLOUT_SOAK": "0s", "BLOODRAVEN_ROLLOUT_STALL_AFTER": "10m"}},
		// --reuse-values from v1.7.0: no rollout key. Pacing is on and the
		// operator's own defaults apply (an empty variable).
		{name: "values of a release before v1.8.0", args: []string{"--set", "rollout=null"}, want: map[string]string{
			"BLOODRAVEN_ROLLOUT_PACING": "true", "BLOODRAVEN_ROLLOUT_SOAK": "",
			"BLOODRAVEN_ROLLOUT_STALL_AFTER": "", "BLOODRAVEN_ROLLOUT_START_GRACE": ""}},
		// --reuse-values with one key set on the command line: the map
		// exists and is partial (a null removes a key).
		{name: "old values plus one key", args: []string{"--set", "rollout.pacing=null", "--set", "rollout.stallAfter=null",
			"--set", "rollout.startGrace=null", "--set", "rollout.soak=10s"}, want: map[string]string{
			"BLOODRAVEN_ROLLOUT_PACING": "true", "BLOODRAVEN_ROLLOUT_SOAK": "10s",
			"BLOODRAVEN_ROLLOUT_STALL_AFTER": "", "BLOODRAVEN_ROLLOUT_START_GRACE": ""}},
	} {
		t.Run(tt.name, func(t *testing.T) {
			args := append([]string{"template", "test", ".", "--namespace", "operator-system"}, tt.args...)
			out, err := exec.Command("helm", args...).CombinedOutput()
			if err != nil {
				t.Fatalf("helm template: %v: %s", err, out)
			}
			for name, want := range tt.want {
				if got := operatorEnv(t, out, name); got != want {
					t.Errorf("%s = %q, want %q", name, got, want)
				}
			}
		})
	}
}
