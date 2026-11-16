package controller

import (
	"bytes"
	"context"
	"errors"
	"log/slog"
	"strings"
	"testing"

	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/tools/record"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	v1alpha1 "github.com/shipstream/bloodraven/api/v1alpha1"
	"github.com/shipstream/bloodraven/internal/state"
)

// The tests below pin #291: a topology with no writable site that the
// operator made itself (the fenced window of a maintenance handoff or of a
// planned failover) is reported as PlannedWritePause, without the ALERT log
// line and the NoPrimaryDetected Event, and the same topology from any
// other cause is still NoPrimary.

// writePauseManager is a two-site manager whose primary dc1 the operator
// has fenced, with its log captured.
func writePauseManager(t *testing.T) (*TopologyManager, *mockMySQL, *mockMySQL, *bytes.Buffer) {
	t.Helper()
	dc1 := &mockMySQL{readOnly: false}
	dc2 := &mockMySQL{readOnly: true}
	tm, _, _ := newTestTopologyManager(dc1, dc2)
	setRecoveredTopology(tm)
	var logs bytes.Buffer
	tm.logger = slog.New(slog.NewTextHandler(&logs, nil))
	return tm, dc1, dc2, &logs
}

func fence(m *mockMySQL) {
	m.mu.Lock()
	m.readOnly, m.superReadOnly = true, true
	m.mu.Unlock()
}

// startHandoff puts the manager's ordered update into its handoff phase.
func startHandoff(tm *TopologyManager) *UpdateController {
	u := NewUpdateController(tm.failover, testLogger())
	u.mu.Lock()
	u.updating = true
	u.mu.Unlock()
	u.setPhase(v1alpha1.UpdatePhaseFailover)
	tm.mu.Lock()
	tm.updater = u
	tm.mu.Unlock()
	return u
}

func endUpdate(u *UpdateController) {
	u.setPhase(UpdatePhaseNone)
	u.mu.Lock()
	u.updating = false
	u.mu.Unlock()
}

func requireVerdict(t *testing.T, tm *TopologyManager, wantReason, wantPauseContains string) TopologySnapshot {
	t.Helper()
	snap := tm.buildSnapshot(nil)
	if snap.DegradedReason != wantReason {
		t.Fatalf("degraded reason = %q (alert %q, write pause %q), want %q", snap.DegradedReason, snap.Alert, snap.WritePause, wantReason)
	}
	if wantReason == v1alpha1.ReasonPlannedWritePause {
		if snap.Alert != "" {
			t.Fatalf("a planned write pause carries the alert %q", snap.Alert)
		}
		if !strings.Contains(snap.WritePause, wantPauseContains) {
			t.Fatalf("write pause = %q, want it to name %q", snap.WritePause, wantPauseContains)
		}
	} else if snap.WritePause != "" {
		t.Fatalf("%s carries the write pause %q", wantReason, snap.WritePause)
	}
	return snap
}

func TestWritePause_HandoffFencedWindow(t *testing.T) {
	tm, dc1, _, logs := writePauseManager(t)
	ctx := context.Background()
	u := startHandoff(tm)
	fence(dc1)

	tm.Poll(ctx)
	snap := requireVerdict(t, tm, v1alpha1.ReasonPlannedWritePause, "maintenance handoff")
	if snap.ActiveSite != "" {
		t.Fatalf("active site = %q in the fenced window, want none: no site is writable", snap.ActiveSite)
	}
	if strings.Contains(logs.String(), "ALERT") {
		t.Fatalf("the fenced window of a handoff logged an ALERT:\n%s", logs.String())
	}
	tm.mu.RLock()
	msg := tm.topologyMessageLocked(nil, "")
	tm.mu.RUnlock()
	if !strings.Contains(msg.WritePause, "maintenance handoff") || msg.Alert != "" {
		t.Fatalf("/ws/status message = alert %q writePause %q, want the write pause and no alert", msg.Alert, msg.WritePause)
	}

	// The handoff is abandoned without unfencing (the worst case): the
	// same topology is now nobody's plan, and it is reported and logged.
	endUpdate(u)
	tm.Poll(ctx)
	requireVerdict(t, tm, v1alpha1.ReasonNoPrimary, "")
	if !strings.Contains(logs.String(), "NO PRIMARY") {
		t.Fatalf("no ALERT was logged once the pause was nobody's:\n%s", logs.String())
	}
}

// The handoff's phase also covers its wait before the fence: the source is
// writable then and nothing is overridden.
func TestWritePause_HandoffBeforeTheFenceIsHealthy(t *testing.T) {
	tm, _, _, _ := writePauseManager(t)
	startHandoff(tm)
	tm.Poll(context.Background())
	requireVerdict(t, tm, v1alpha1.ReasonHealthy, "")
}

func TestWritePause_PlannedFailoverRunAndHold(t *testing.T) {
	tm, dc1, _, logs := writePauseManager(t)
	ctx := context.Background()
	slot := &PlannedClaimSlot{}
	tm.mu.Lock()
	tm.plannedSlot = slot
	tm.mu.Unlock()
	if !slot.hydrate(plannedClaim{mode: claimModeRun, runID: "run-1", source: "dc1", target: "dc2"}) {
		t.Fatal("hydrate refused")
	}
	fence(dc1)

	tm.Poll(ctx)
	snap := requireVerdict(t, tm, v1alpha1.ReasonPlannedWritePause, "planned failover run-1 from dc1 to dc2")
	if strings.Contains(logs.String(), "ALERT") {
		t.Fatalf("the fenced window of a planned failover logged an ALERT:\n%s", logs.String())
	}
	_ = snap

	// The run ends ManualRecoveryRequired with its source left fenced: an
	// automation hold is not a pause that ends by itself.
	if !slot.toHold("run-1") {
		t.Fatal("toHold refused")
	}
	tm.Poll(ctx)
	requireVerdict(t, tm, v1alpha1.ReasonNoPrimary, "")
}

func TestWritePause_AllReadOnlyWithoutAnOperationIsNoPrimary(t *testing.T) {
	tm, dc1, _, logs := writePauseManager(t)
	fence(dc1)
	tm.Poll(context.Background())
	requireVerdict(t, tm, v1alpha1.ReasonNoPrimary, "")
	if !strings.Contains(logs.String(), "NO PRIMARY: both sites are read-only") {
		t.Fatalf("no ALERT for an unexplained topology without a writable site:\n%s", logs.String())
	}
}

// After the promotion that ends the pause, a poll that cannot read the
// promoted target says nothing about it; the wait for its first answer is
// the tail of the pause.
func TestWritePause_HandoffPromotionAwaitingItsFirstPoll(t *testing.T) {
	tm, dc1, dc2, logs := writePauseManager(t)
	ctx := context.Background()
	u := startHandoff(tm)
	fence(dc1)
	tm.Poll(ctx)
	requireVerdict(t, tm, v1alpha1.ReasonPlannedWritePause, "maintenance handoff")

	// The handoff promoted dc2 and moved on; dc2 does not answer the next
	// probe.
	dc2.mu.Lock()
	dc2.readOnly, dc2.err = false, errors.New("i/o timeout")
	dc2.mu.Unlock()
	tm.recordHandoffPromotion(ctx, "dc1", "dc2", "")
	u.setPhase(v1alpha1.UpdatePhaseUpdateOldPrimary)
	tm.Poll(ctx)
	requireVerdict(t, tm, v1alpha1.ReasonPlannedWritePause, "promotion of dc2")

	dc2.mu.Lock()
	dc2.err = nil
	dc2.mu.Unlock()
	tm.Poll(ctx)
	requireVerdict(t, tm, v1alpha1.ReasonHealthy, "")
	if strings.Contains(logs.String(), "ALERT") {
		t.Fatalf("a handoff whose target answered one poll late logged an ALERT:\n%s", logs.String())
	}
}

// The same wait after an emergency failover is not a planned pause: that
// promotion fenced nothing.
func TestWritePause_EmergencyPromotionAwaitingItsFirstPollIsNotPlanned(t *testing.T) {
	tm, _, _, _ := writePauseManager(t)
	for _, tc := range []struct {
		fencedSource string
		want         bool
	}{{fencedSource: "dc1", want: true}, {fencedSource: "", want: false}} {
		tm.recordPromotion(context.Background(), tm.clock.Now(), "dc2", "", tc.fencedSource)
		tm.mu.Lock()
		got := tm.ownWritePauseLocked(ownWritePauseSample{}, ownWritePauseSample{})
		tm.mu.Unlock()
		if (got != "") != tc.want {
			t.Errorf("promotion with fenced source %q awaiting its first poll: write pause = %q, want set=%v", tc.fencedSource, got, tc.want)
		}
	}
}

// An operation that begins and ends between the two readings of one poll
// still overlapped its probes: the counters show it.
func TestOwnWritePauseLocked(t *testing.T) {
	tm, _, _, _ := writePauseManager(t)
	run := PlannedClaimView{Mode: "Run", RunID: "run-1", Source: "dc1", Target: "dc2"}
	for _, tc := range []struct {
		name          string
		before, after ownWritePauseSample
		want          string
	}{
		{name: "nothing ran", want: ""},
		{name: "nothing ran since the last handoff", before: ownWritePauseSample{handoffs: 3, runs: 2}, after: ownWritePauseSample{handoffs: 3, runs: 2}, want: ""},
		{name: "handoff at the start only", before: ownWritePauseSample{handoff: true, handoffs: 1}, after: ownWritePauseSample{handoffs: 1}, want: "maintenance handoff"},
		{name: "handoff at the commit only", before: ownWritePauseSample{}, after: ownWritePauseSample{handoff: true, handoffs: 1}, want: "maintenance handoff"},
		{name: "handoff inside the poll", before: ownWritePauseSample{handoffs: 1}, after: ownWritePauseSample{handoffs: 2}, want: "maintenance handoff"},
		{name: "run at the commit", after: ownWritePauseSample{run: run, runHeld: true, runs: 1}, want: "planned failover run-1 from dc1 to dc2"},
		{name: "run at the start only", before: ownWritePauseSample{run: run, runHeld: true, runs: 1}, after: ownWritePauseSample{runs: 1}, want: "planned failover run-1 from dc1 to dc2"},
		{name: "run inside the poll", before: ownWritePauseSample{runs: 1}, after: ownWritePauseSample{runs: 2}, want: "planned failover"},
	} {
		tm.mu.Lock()
		got := tm.ownWritePauseLocked(tc.before, tc.after)
		tm.mu.Unlock()
		if (tc.want == "") != (got == "") || !strings.Contains(got, tc.want) {
			t.Errorf("%s: write pause = %q, want it to contain %q", tc.name, got, tc.want)
		}
	}
}

func TestUpdateCRStatus_PlannedWritePause(t *testing.T) {
	const pause = "Writes are paused by the maintenance handoff of an ordered update"
	snap := TopologySnapshot{
		Sites: []SiteSnapshot{
			{Name: "dc1", Role: state.SiteRolePrimaryCandidate, State: state.StateReadOnly},
			{Name: "dc2", Role: state.SiteRolePrimaryCandidate, State: state.StateReadOnly},
		},
		DegradedReason: v1alpha1.ReasonPlannedWritePause,
		WritePause:     pause,
	}
	for _, tc := range []struct {
		name         string
		prepare      func(*v1alpha1.MysqlFailoverGroup, *TopologySnapshot)
		wantUnstable string
	}{
		{name: "ordered update", prepare: func(_ *v1alpha1.MysqlFailoverGroup, s *TopologySnapshot) {
			s.UpdatePhase = v1alpha1.UpdatePhaseFailover
		},
			wantUnstable: v1alpha1.UnstableReasonOrderedUpdate},
		{name: "planned failover", prepare: func(fg *v1alpha1.MysqlFailoverGroup, _ *TopologySnapshot) {
			fg.Status.PlannedFailover = &v1alpha1.PlannedFailoverStatus{RunID: "run-1", Phase: v1alpha1.PlannedFailoverPhaseDraining, SourceSite: "dc1", TargetSite: "dc2"}
		}, wantUnstable: v1alpha1.UnstableReasonPlannedFailover},
		{name: "status shows neither", prepare: func(*v1alpha1.MysqlFailoverGroup, *TopologySnapshot) {}, wantUnstable: v1alpha1.UnstableReasonNotReady},
	} {
		t.Run(tc.name, func(t *testing.T) {
			fg := newTestFG()
			s := snap
			tc.prepare(fg, &s)
			c := fake.NewClientBuilder().WithScheme(testScheme()).
				WithStatusSubresource(&v1alpha1.MysqlFailoverGroup{}).
				WithObjects(fg).Build()
			runner := &TopologyManagerRunner{client: c, logger: testLogger(), managers: make(map[types.NamespacedName]*managedTopology)}
			nn := types.NamespacedName{Name: fg.Name, Namespace: fg.Namespace}
			if err := runner.updateCRStatus(context.Background(), nn, s); err != nil {
				t.Fatal(err)
			}
			var updated v1alpha1.MysqlFailoverGroup
			if err := c.Get(context.Background(), nn, &updated); err != nil {
				t.Fatal(err)
			}
			for _, typ := range []string{v1alpha1.ConditionReady, v1alpha1.ConditionDegraded} {
				cond := findCondition(updated.Status.Conditions, typ)
				if cond == nil || cond.Status != "False" || cond.Reason != v1alpha1.ReasonPlannedWritePause || cond.Message != pause {
					t.Fatalf("%s = %+v, want False/PlannedWritePause with the pause message", typ, cond)
				}
			}
			if updated.Status.ActiveSite != "" {
				t.Fatalf("status.activeSite = %q, want none", updated.Status.ActiveSite)
			}
			if got := DeploymentUnstableReason(&updated); got != tc.wantUnstable {
				t.Fatalf("DeploymentUnstableReason = %q, want %q", got, tc.wantUnstable)
			}
		})
	}
}

func TestEmitDegradedTransitionEvents_PlannedWritePauseIsSilent(t *testing.T) {
	rec := record.NewFakeRecorder(10)
	fg := newTestFG()
	pause := TopologySnapshot{DegradedReason: v1alpha1.ReasonPlannedWritePause, WritePause: "Writes are paused"}

	// A pause the operator opened and closed: no Warning, and no
	// SiteRecovered for a group that was never degraded.
	runner, nn := newDegradedTestRunner(rec, fg, "Healthy")
	runner.emitDegradedTransitionEvents(fg, nn, pause)
	runner.emitDegradedTransitionEvents(fg, nn, TopologySnapshot{DegradedReason: "Healthy"})
	if events := drainRunnerEvents(rec); len(events) != 0 {
		t.Fatalf("Healthy, pause, Healthy emitted %v, want no events", events)
	}

	// A pause that ends with no writable site is reported then.
	runner, nn = newDegradedTestRunner(rec, fg, "Healthy")
	runner.emitDegradedTransitionEvents(fg, nn, pause)
	runner.emitDegradedTransitionEvents(fg, nn, TopologySnapshot{DegradedReason: "NoPrimary", Alert: "NO PRIMARY: both sites are read-only"})
	if events := drainRunnerEvents(rec); len(events) != 1 || !strings.Contains(events[0], "NoPrimaryDetected") {
		t.Fatalf("Healthy, pause, NoPrimary emitted %v, want one NoPrimaryDetected", events)
	}
}
