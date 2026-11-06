package doctorscripts

import (
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"testing"

	v1alpha1 "github.com/shipstream/bloodraven/api/v1alpha1"
	"github.com/shipstream/bloodraven/internal/controller"
)

// fakeTriageKubectl answers the read-only calls triage.sh makes. The
// planned-failover fields come from $STATE files named after them (marker,
// cond, holdrun, phase, pfrun, outcome); `-o json` returns an empty object
// so the group "exists"; everything else is empty.
const fakeTriageKubectl = `#!/usr/bin/env bash
set -u
st() { [[ -f "$STATE/$1" ]] && cat "$STATE/$1"; return 0; }
for a in "$@"; do [[ "$a" == json ]] && { echo '{}'; exit 0; }; done
expr=$(printf '%s\n' "$@" | sed -n 's/^jsonpath=//p')
case "$expr" in
  *planned-failover-terminal*) st marker ;;
  *'type=="AutomaticFailoverHeld"'*) st cond ;;
  '{.status.plannedFailoverHold.runID}{" reason="'*) h=$(st holdrun); [[ -n "$h" ]] && printf '%s reason=PromotionUnconfirmed a->b ambiguousSites=' "$h" ;;
  '{.status.plannedFailoverHold.runID}') st holdrun ;;
  '{.status.plannedFailover.phase}') st phase ;;
  '{.status.plannedFailover.runID}') st pfrun ;;
  '{.status.plannedFailover.result.outcome}') st outcome ;;
esac
exit 0
`

type triageCase struct {
	marker, cond, holdRun, phase, pfRun, outcome string
}

func runTriage(t *testing.T, c triageCase, withJQ bool) string {
	t.Helper()
	dir := t.TempDir()
	bin := filepath.Join(dir, "bin")
	state := filepath.Join(dir, "state")
	for _, d := range []string{bin, state} {
		if err := os.MkdirAll(d, 0o755); err != nil {
			t.Fatal(err)
		}
	}
	if err := os.WriteFile(filepath.Join(bin, "kubectl"), []byte(fakeTriageKubectl), 0o755); err != nil {
		t.Fatal(err)
	}
	for name, v := range map[string]string{"marker": c.marker, "cond": c.cond, "holdrun": c.holdRun,
		"phase": c.phase, "pfrun": c.pfRun, "outcome": c.outcome} {
		if v == "" {
			continue
		}
		if err := os.WriteFile(filepath.Join(state, name), []byte(v), 0o600); err != nil {
			t.Fatal(err)
		}
	}
	path := bin + ":" + os.Getenv("PATH")
	if !withJQ {
		// A PATH with only the tools the scripts need, and no jq: the marker
		// is parsed by the sed fallback.
		for _, tool := range []string{"bash", "sed", "grep", "awk", "tail", "dirname", "cat", "head", "tr"} {
			p, err := exec.LookPath(tool)
			if err != nil {
				t.Skipf("%s not on PATH", tool)
			}
			if err := os.Symlink(p, filepath.Join(bin, tool)); err != nil {
				t.Fatal(err)
			}
		}
		path = bin
	} else if _, err := exec.LookPath("jq"); err != nil {
		t.Skip("jq not on PATH")
	}
	_, file, _, _ := runtime.Caller(0)
	script := filepath.Join(filepath.Dir(file), "..", "..", ".agents", "skills", "bloodraven-doctor", "scripts", "triage.sh")
	bash, err := exec.LookPath("bash")
	if err != nil {
		t.Skip("bash not on PATH")
	}
	cmd := exec.Command(bash, script, "-n", "orders", "lion")
	cmd.Dir = dir
	cmd.Env = append(os.Environ(), "PATH="+path, "STATE="+state)
	out, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("triage.sh failed: %v\n%s", err, out)
	}
	return string(out)
}

func marker(runID string, outcome v1alpha1.PlannedFailoverOutcome, reason v1alpha1.PlannedFailoverReason) string {
	return controller.TerminalMarker{RunID: runID, Outcome: outcome, Reason: reason}.String()
}

// TestTriageReportsHolds pins that triage.sh reports the hold the operator
// enforces (controller.PlannedHoldFrom), including a hold carried only by a
// ManualRecoveryRequired terminal marker, with and without jq.
func TestTriageReportsHolds(t *testing.T) {
	mrr := marker("r1", v1alpha1.PlannedFailoverOutcomeManualRecoveryRequired, v1alpha1.PlannedFailoverReasonPromotionUnconfirmed)
	for _, tc := range []struct {
		name    string
		c       triageCase
		want    []string
		notWant []string
	}{
		{
			name: "marker-only hold (status writes stalled)",
			c:    triageCase{marker: mrr, phase: "Promoting", pfRun: "r1"},
			want: []string{"🔴 AutomaticFailoverHeld: held by the terminal marker only", "run=r1",
				"acknowledge-planned-failover-hold=r1", "could not explain the result; automation is held: see playbook 38"},
		},
		{
			name:    "non-hold marker",
			c:       triageCase{marker: marker("r1", v1alpha1.PlannedFailoverOutcomeSucceeded, ""), phase: "Publishing", pfRun: "r1"},
			notWant: []string{"🔴", "acknowledge-planned-failover-hold"},
		},
		{
			name:    "status record wins over another run's marker",
			c:       triageCase{marker: mrr, cond: "True PlannedFailoverManualRecoveryRequired", holdRun: "r2", phase: "Failed", pfRun: "r2", outcome: "ManualRecoveryRequired"},
			want:    []string{"plannedFailoverHold: run=r2", "acknowledge-planned-failover-hold=r2"},
			notWant: []string{"acknowledge-planned-failover-hold=r1", "terminal marker only"},
		},
		{
			name:    "condition-only hold stays nameless",
			c:       triageCase{marker: mrr, cond: "True PlannedFailoverManualRecoveryRequired"},
			want:    []string{"🔴 AutomaticFailoverHeld: True", "The condition names no run"},
			notWant: []string{"acknowledge-planned-failover-hold=r1"},
		},
		{
			name:    "malformed marker proves nothing",
			c:       triageCase{marker: strings.Replace(mrr, `"schemaVersion":1`, `"schemaVersion":2`, 1), phase: "Promoting", pfRun: "r1"},
			notWant: []string{"🔴", "acknowledge-planned-failover-hold"},
		},
	} {
		for _, withJQ := range []bool{true, false} {
			name := tc.name + map[bool]string{true: " (jq)", false: " (sed)"}[withJQ]
			t.Run(name, func(t *testing.T) {
				out := runTriage(t, tc.c, withJQ)
				for _, w := range tc.want {
					if !strings.Contains(out, w) {
						t.Errorf("output lacks %q:\n%s", w, out)
					}
				}
				for _, w := range tc.notWant {
					if strings.Contains(out, w) {
						t.Errorf("output contains %q:\n%s", w, out)
					}
				}
			})
		}
	}
}
