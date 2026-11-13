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

// triage.sh reads the current status shape: the lastFailover record, the
// sites[].replication enum, the EncryptionAtRestReady condition and the
// backups' status.artifactStorage. Each rule matches only the new path, so
// a script still reading an old field prints nothing for it.
func TestTriageReadsCurrentStatusShape(t *testing.T) {
	e := newRuleEnv(t)
	e.rule(t, "get mysqlfailovergroups.shipstream.io lion -n orders -o json", "{}", false)
	e.rule(t, "get mysqlfailovergroups.shipstream.io lion -n orders -o jsonpath={.status.lastFailover.time}{\" target=\"}{.status.lastFailover.target}", "2026-11-05T10:00:00Z target=dc2", false)
	e.rule(t, "get mysqlfailovergroups.shipstream.io lion -n orders -o jsonpath=*Replication: {.replication}*", "Site: dc1\n  Replication: Lagging\n", false)
	e.rule(t, "get mysqlfailovergroups.shipstream.io lion -n orders -o jsonpath=*EncryptionAtRestReady*", "True AllSitesSealed: all sites sealed", false)
	e.rule(t, "get mysqlbackups -n orders -l bloodraven.shipstream.io/failover-group=lion -o custom-columns=NAME:.metadata.name,DELETING:.metadata.deletionTimestamp,POLICY:.spec.deletionPolicy,CLEANUP_START:.status.cleanupStartTime,LOCATION:.status.artifactStorage.location,BUCKET:*",
		"NAME   DELETING   LOCATION\nb1     <none>     s3://bucket/lion/b1\n", false)
	out, err := e.run(t, scriptPath("triage.sh"), "-n", "orders", "lion")
	if err != nil {
		t.Fatalf("triage.sh failed: %v\n%s", err, out)
	}
	for _, want := range []string{
		"Last Failover:       2026-11-05T10:00:00Z target=dc2",
		"Replication: Lagging",
		"EncryptionAtRestReady: True AllSitesSealed",
		"s3://bucket/lion/b1",
	} {
		if !strings.Contains(out, want) {
			t.Errorf("triage output lacks %q:\n%s", want, out)
		}
	}
}

// TestTriageReportsAppliedGeneration pins that triage.sh compares
// status.observedGeneration with metadata.generation, and explains a
// Degraded=True/BootstrapFailed verdict.
func TestTriageReportsAppliedGeneration(t *testing.T) {
	const gap = "The topology manager has not applied the latest spec yet"
	for _, c := range []struct {
		name, spec, observed string
		wantGap              bool
	}{
		{name: "applied", spec: "7", observed: "7"},
		{name: "behind", spec: "8", observed: "7", wantGap: true},
		{name: "never applied", spec: "3", wantGap: true},
		{name: "no generation", observed: "2"},
	} {
		t.Run(c.name, func(t *testing.T) {
			e := newRuleEnv(t)
			e.rule(t, "get mysqlfailovergroups.shipstream.io lion -n orders -o json", "{}", false)
			e.rule(t, "get mysqlfailovergroups.shipstream.io lion -n orders -o jsonpath={.metadata.generation}", c.spec, false)
			e.rule(t, "get mysqlfailovergroups.shipstream.io lion -n orders -o jsonpath={.status.observedGeneration}", c.observed, false)
			e.rule(t, "get mysqlfailovergroups.shipstream.io lion -n orders -o jsonpath=*type==\"Degraded\"*",
				"True BootstrapFailed: Reclone into dc2 failed: clone_status has no new Completed row", false)
			out, err := e.run(t, scriptPath("triage.sh"), "-n", "orders", "lion")
			if err != nil {
				t.Fatalf("triage.sh failed: %v\n%s", err, out)
			}
			observed := c.observed
			if observed == "" {
				observed = "none"
			}
			spec := c.spec
			if spec == "" {
				spec = "unavailable"
			}
			if want := "Spec generation:     " + spec + " (applied by the topology manager: " + observed + ")"; !strings.Contains(out, want) {
				t.Errorf("triage output lacks %q:\n%s", want, out)
			}
			if got := strings.Contains(out, gap); got != c.wantGap {
				t.Errorf("generation gap reported = %v, want %v:\n%s", got, c.wantGap, out)
			}
			if !strings.Contains(out, "The last clone (bootstrap, reclone or auto-clone) failed") {
				t.Errorf("triage output does not explain BootstrapFailed:\n%s", out)
			}
		})
	}
}

// TestTriageReportsPacedRollout pins that triage.sh prints the group's
// status.rollout record and says what each phase means for its pods, and
// prints nothing for a group without a record.
func TestTriageReportsPacedRollout(t *testing.T) {
	const rule = "get mysqlfailovergroups.shipstream.io lion -n orders -o jsonpath={.status.rollout.phase}*"
	for _, c := range []struct {
		name, answer string
		want         []string
		absent       []string
	}{
		{
			name:   "stalled",
			answer: "Stalled reason=OrderedUpdateStalled renderer=0123456789abcdef message=Ordered update stalled: site dc2 was updated but its pod is not Ready",
			want: []string{
				"Paced rollout:       Stalled reason=OrderedUpdateStalled renderer=0123456789abcdef message=Ordered update stalled",
				"It keeps the rollout slot, so no other group is rolled: see playbook 50.",
			},
		},
		{
			name:   "halted behind another group",
			answer: "Pending reason=RolloutHalted renderer=0123456789abcdef message=the rollout is halted: the roll of orders/tiger stalled; this group keeps its pods",
			want: []string{
				"Paced rollout:       Pending reason=RolloutHalted",
				"The rollout is halted at the group named in the message.",
			},
		},
		{
			name:   "not stable",
			answer: "Pending reason=GroupNotStable renderer=0123456789abcdef message=the group is not stable (Restore); it is admitted once it is, and does not hold up other groups",
			want:   []string{"The group is passed over until it is stable"},
		},
		{
			name:   "waiting",
			answer: "Pending reason=WaitingForSlot renderer=0123456789abcdef message=orders/tiger holds the rollout slot",
			want:   []string{"The group waits for the rollout slot and keeps its old pods"},
		},
		{
			name:   "complete",
			answer: "Complete reason= renderer=0123456789abcdef message=",
			want:   []string{"Paced rollout:       Complete"},
			absent: []string{"playbook 50"},
		},
		{
			// kubectl prints the literals of the template for a missing field.
			name:   "no record",
			answer: " reason= renderer= message=",
			absent: []string{"Paced rollout:"},
		},
	} {
		t.Run(c.name, func(t *testing.T) {
			e := newRuleEnv(t)
			e.rule(t, "get mysqlfailovergroups.shipstream.io lion -n orders -o json", "{}", false)
			e.rule(t, rule, c.answer, false)
			out, err := e.run(t, scriptPath("triage.sh"), "-n", "orders", "lion")
			if err != nil {
				t.Fatalf("triage.sh failed: %v\n%s", err, out)
			}
			for _, w := range c.want {
				if !strings.Contains(out, w) {
					t.Errorf("triage output lacks %q:\n%s", w, out)
				}
			}
			for _, w := range c.absent {
				if strings.Contains(out, w) {
					t.Errorf("triage output contains %q:\n%s", w, out)
				}
			}
		})
	}
}
