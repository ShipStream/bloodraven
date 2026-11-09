package dst

import (
	"log/slog"
	"strings"
	"testing"
	"time"

	v1alpha1 "github.com/shipstream/bloodraven/api/v1alpha1"
	"github.com/shipstream/bloodraven/internal/controller"
)

// managerStartTrial is a fault-free two-site trial with actors on and the
// given actor ops.
func managerStartTrial(ops ...Op) Trial {
	tr := twoSiteHandTrial(15, true, 40)
	tr.Actors = Actors{Enabled: true, Ops: ops}
	return tr
}

// TestManagerStart_RestartFailsOnReadOutageAndRetries: a restarted
// process's start reads the group uncached through the store (K027). While
// that read fails the start fails, as the runner's startManager does with
// an empty slot, and the next reconciles retry it; the group then runs
// normally.
func TestManagerStart_RestartFailsOnReadOutageAndRetries(t *testing.T) {
	r := RunTrial(managerStartTrial(
		Op{At: 20, Kind: OpPFReadOutage, N: 3},
		Op{At: 20, Kind: OpRestartOperator},
	), false, nil)
	c := r.Planned.Counts
	if c["managerStartFailed"] == 0 || c["managerStartRetries"] == 0 {
		t.Fatalf("the restart's start never failed on the read outage (counts %v)", c)
	}
	if len(r.Violations) != 0 {
		t.Fatalf("violations: %v", r.Violations)
	}
	if r.FinalStatus.ActiveSite != "alpha" {
		t.Fatalf("active site %q after the retried start, want alpha", r.FinalStatus.ActiveSite)
	}
}

// TestManagerStart_ReplacementProceedsWithoutView: a replacement whose
// uncached read fails starts anyway with no view (no hydration, no planned
// restore), as plannedFailoverView returns nil for a start with a
// predecessor (K027).
func TestManagerStart_ReplacementProceedsWithoutView(t *testing.T) {
	r := RunTrial(managerStartTrial(
		Op{At: 20, Kind: OpPFReadOutage, N: 3},
		Op{At: 20, Kind: OpReplaceManager},
	), false, nil)
	c := r.Planned.Counts
	if c["managersReplaced"] != 1 || c["managerStartNoView"] == 0 || c["managerStartFailed"] != 0 {
		t.Fatalf("replacement under a read outage: counts %v, want one replacement started with no view", c)
	}
	if len(r.Violations) != 0 {
		t.Fatalf("violations: %v", r.Violations)
	}
}

// TestManagerStart_AbsenceEndStartsAFreshManager: an absence window retires
// the current manager, and its end starts a fresh one restored from the
// object (the runner's next successful startManager), not the old object
// coming back (K036 long absences rely on it: the fresh start names a
// source the run left fenced).
func TestManagerStart_AbsenceEndStartsAFreshManager(t *testing.T) {
	r := RunTrial(managerStartTrial(Op{At: 20, Kind: OpManagerAbsent, N: 5}), false, nil)
	c := r.Planned.Counts
	if c["managerStartRetries"] == 0 {
		t.Fatalf("no start after the absence window (counts %v)", c)
	}
	if len(r.Violations) != 0 {
		t.Fatalf("violations: %v", r.Violations)
	}
	if r.FinalStatus.ActiveSite != "alpha" {
		t.Fatalf("active site %q after the absence, want alpha", r.FinalStatus.ActiveSite)
	}
}

// TestConsumeRequestFailureIsRetried: an idempotent request (the active
// site) whose consumption fails is consumed by a later pass; the engine
// must requeue, as nothing else re-runs it for an annotation left behind
// (found by the K184 metadata faults; engine consumeRequest fix).
func TestConsumeRequestFailureIsRetried(t *testing.T) {
	r := RunTrial(managerStartTrial(
		Op{At: 20, Kind: OpPFMetaFault, N: 1, K: int(metaFaultForbidden)},
		Op{At: 20, Kind: OpPlannedFailover, Site: "alpha"},
	), false, nil)
	if len(r.Violations) != 0 {
		t.Fatalf("violations: %v", r.Violations)
	}
	if _, ok := r.Planned.Object.GetAnnotations()[v1alpha1.PlannedFailoverAnnotation]; ok {
		t.Fatal("the request annotation outlived its failed consumption")
	}
	forbidden := false
	for _, op := range r.Planned.StoreLog {
		if op.Op == "consumeRequest" && op.Outcome == "forbidden (injected)" {
			forbidden = true
		}
	}
	if !forbidden {
		t.Fatal("the injected fault did not hit the request's consumption")
	}
}

// TestEngineLogsReachTheTrialHandler: the engine's log.FromContext lines go
// to the trial's handler (K176), as the topology manager's do.
func TestEngineLogsReachTheTrialHandler(t *testing.T) {
	var buf strings.Builder
	h := slog.NewTextHandler(&buf, &slog.HandlerOptions{Level: slog.LevelDebug})
	RunTrial(managerStartTrial(
		Op{At: 20, Kind: OpPFMetaFault, N: 1, K: int(metaFaultForbidden)},
		Op{At: 20, Kind: OpPlannedFailover, Site: "alpha"},
	), false, h)
	if !strings.Contains(buf.String(), "planned failover: could not remove the request annotation") {
		t.Fatal("the engine's log line did not reach the trial's handler")
	}
}

// TestStalledClaimWithinDeadline: while a run holds the claim and its
// status writes are stalled, the manager's split-brain fence is deferred up
// to the stall deadline (§6.4); DualWritableUnresolved counts only past it.
func TestStalledClaimWithinDeadline(t *testing.T) {
	tr := twoSiteHandTrial(15, true, 20)
	tr.Actors = Actors{Enabled: true}
	r := &trialRunner{trial: tr, logger: discardLogger(), operatorDeadUntil: -1,
		kindsSeen: map[string]struct{}{}, reasonsSeen: map[string]struct{}{}}
	r.setup(false)
	pl := r.pl
	if pl.stalledClaimWithinDeadline() {
		t.Fatal("no claim held: not stalled")
	}
	pf := &v1alpha1.PlannedFailoverStatus{RunID: "r1", Revision: 1, Phase: v1alpha1.PlannedFailoverPhaseDraining,
		TargetSite: "beta", SourceSite: "alpha"}
	pl.reg.register(r.newManager(nil), controller.ManagerWiring{HydrateFrom: pf})
	if id, hold, ok := pl.reg.Slot(pl.nn).HeldForSim(); !ok || hold || id != "r1" {
		t.Fatalf("slot %q hold=%v ok=%v, want r1's run claim", id, hold, ok)
	}
	if pl.stalledClaimWithinDeadline() {
		t.Fatal("claim held without a stall-since: not stalled")
	}
	if err := pl.store.PatchAnnotations("test", map[string]string{
		v1alpha1.PlannedFailoverStallSinceAnnotation: controller.FormatDeadlineStart("r1", r.clk.Now()),
	}); err != nil {
		t.Fatal(err)
	}
	if !pl.stalledClaimWithinDeadline() {
		t.Fatal("stalled run holding the claim, inside the deadline: exempt")
	}
	r.clk.Advance(plannedStatusStallDeadline + stalledClaimSlack + time.Second)
	if pl.stalledClaimWithinDeadline() {
		t.Fatal("past the stall deadline the claim must have been released: not exempt")
	}
}
