package controller

import (
	"testing"
	"time"

	v1alpha1 "github.com/shipstream/bloodraven/api/v1alpha1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
)

const testRenderer = "renderer-new"

var rolloutT0 = time.Date(2026, 11, 12, 10, 0, 0, 0, time.UTC)

func rolloutKey(name string) types.NamespacedName {
	return types.NamespacedName{Namespace: "db", Name: name}
}

// pendingView is a stable group with both sites still on another renderer.
func pendingView(name string) rolloutGroupView {
	return rolloutGroupView{Key: rolloutKey(name), State: rolloutGroupState{Pending: []string{"mysql/iad", "mysql/pdx"}}}
}

// planOnce runs the planner and applies its writes to the views, as the
// pacer's status writes would, so a test can run pass after pass.
func planOnce(t *testing.T, now time.Time, identity string, cfg RolloutPacerConfig, views []rolloutGroupView) ([]rolloutAction, rolloutFleet) {
	t.Helper()
	actions, fleet := planRollout(now, identity, cfg, views)
	for _, a := range actions {
		found := false
		for i := range views {
			if views[i].Key == a.Key {
				views[i].Record = a.To.DeepCopy()
				found = true
			}
		}
		if !found {
			t.Fatalf("action for unknown group %s", a.Key)
		}
	}
	return actions, fleet
}

func recordOf(views []rolloutGroupView, name string) *v1alpha1.RolloutStatus {
	for i := range views {
		if views[i].Key.Name == name {
			return views[i].Record
		}
	}
	return nil
}

func requirePhase(t *testing.T, views []rolloutGroupView, name string, phase v1alpha1.RolloutPhase, reason string) {
	t.Helper()
	r := recordOf(views, name)
	if r == nil {
		if phase == "" {
			return
		}
		t.Fatalf("%s has no rollout record, want %s/%s", name, phase, reason)
	}
	if r.Phase != phase || (reason != "" && r.Reason != reason) {
		t.Fatalf("%s rollout = %s/%s (%s), want %s/%s", name, r.Phase, r.Reason, r.Message, phase, reason)
	}
}

func eventReasons(actions []rolloutAction) map[string]string {
	out := map[string]string{}
	for _, a := range actions {
		if a.EventReason != "" {
			out[a.Key.Name] = a.EventReason
		}
	}
	return out
}

// markStarted is what admitRolloutWrite does before the first workload
// write.
func markStarted(views []rolloutGroupView, name string, at time.Time) {
	r := recordOf(views, name)
	r.StartedTime = &metav1.Time{Time: at}
	r.Phase = v1alpha1.RolloutPhaseRolling
}

func TestPlanRollout_AdmitsOneGroupAtATime(t *testing.T) {
	views := []rolloutGroupView{pendingView("tiger"), pendingView("lion"), pendingView("puma")}
	actions, fleet := planOnce(t, rolloutT0, testRenderer, RolloutPacerConfig{}, views)

	requirePhase(t, views, "lion", v1alpha1.RolloutPhaseAdmitted, rolloutReasonAdmitted)
	requirePhase(t, views, "puma", v1alpha1.RolloutPhasePending, rolloutReasonWaitingForSlot)
	requirePhase(t, views, "tiger", v1alpha1.RolloutPhasePending, rolloutReasonWaitingForSlot)
	if fleet.Admitted != 1 || fleet.Pending != 2 {
		t.Fatalf("fleet = %+v, want 1 admitted and 2 pending", fleet)
	}
	if got := eventReasons(actions); len(got) != 1 || got["lion"] != v1alpha1.EventRolloutAdmitted {
		t.Fatalf("events = %v, want one RolloutAdmitted for lion", got)
	}
	if r := recordOf(views, "lion"); r.Renderer != testRenderer || r.AdmittedTime == nil || r.StartedTime != nil {
		t.Fatalf("lion record = %+v", r)
	}

	// Nothing changes while lion holds the slot.
	if actions, _ := planOnce(t, rolloutT0.Add(time.Minute), testRenderer, RolloutPacerConfig{}, views); len(actions) != 0 {
		t.Fatalf("a second pass wrote %d records, want none: %+v", len(actions), actions)
	}
}

// R-4: a roll that started and does not finish keeps the slot. The stall is
// latched and stays through later passes, whatever the updater reports by
// then; no other group is admitted.
func TestPlanRollout_StalledRollHaltsTheFleet(t *testing.T) {
	cfg := RolloutPacerConfig{StallAfter: 10 * time.Minute}
	views := []rolloutGroupView{pendingView("lion"), pendingView("tiger")}
	planOnce(t, rolloutT0, testRenderer, cfg, views)
	markStarted(views, "lion", rolloutT0.Add(time.Second))
	// The replica was rolled and is not Ready; the primary is not rolled.
	views[0].State = rolloutGroupState{Pending: []string{"mysql/pdx"}, Incomplete: []string{"mysql/iad"}}
	views[0].Unstable = v1alpha1.UnstableReasonOrderedUpdate
	views[0].UpdateStalled = "ordered update stalled: site iad was updated but its pod did not become Ready"

	actions, fleet := planOnce(t, rolloutT0.Add(time.Minute), testRenderer, cfg, views)
	requirePhase(t, views, "lion", v1alpha1.RolloutPhaseStalled, rolloutReasonUpdateStalled)
	requirePhase(t, views, "tiger", v1alpha1.RolloutPhasePending, rolloutReasonHalted)
	if got := eventReasons(actions); got["lion"] != v1alpha1.EventRolloutStalled {
		t.Fatalf("events = %v, want RolloutStalled for lion", got)
	}
	if fleet.Stalled != 1 || fleet.Pending != 1 {
		t.Fatalf("fleet = %+v", fleet)
	}
	stalledAt := recordOf(views, "lion").StalledTime.Time

	// The updater's stall report resets between its retries (its phase and
	// stall are in memory); the latch does not.
	views[0].UpdateStalled = ""
	for i := 1; i <= 3; i++ {
		actions, _ = planOnce(t, rolloutT0.Add(time.Duration(i)*time.Hour), testRenderer, cfg, views)
		if len(actions) != 0 {
			t.Fatalf("pass %d wrote %+v, want nothing", i, actions)
		}
	}
	if r := recordOf(views, "lion"); r.Phase != v1alpha1.RolloutPhaseStalled || !r.StalledTime.Time.Equal(stalledAt) || r.StartedTime == nil {
		t.Fatalf("lion record after later passes = %+v, want the same stall", r)
	}
	requirePhase(t, views, "tiger", v1alpha1.RolloutPhasePending, rolloutReasonHalted)
}

func TestPlanRollout_StallByTimeout(t *testing.T) {
	cfg := RolloutPacerConfig{StallAfter: 10 * time.Minute}
	views := []rolloutGroupView{pendingView("lion")}
	planOnce(t, rolloutT0, testRenderer, cfg, views)
	markStarted(views, "lion", rolloutT0)
	views[0].State = rolloutGroupState{Incomplete: []string{"mysql/iad"}}

	if actions, _ := planOnce(t, rolloutT0.Add(9*time.Minute), testRenderer, cfg, views); len(actions) != 0 {
		t.Fatalf("stalled before the timeout: %+v", actions)
	}
	planOnce(t, rolloutT0.Add(10*time.Minute), testRenderer, cfg, views)
	requirePhase(t, views, "lion", v1alpha1.RolloutPhaseStalled, rolloutReasonStallTimeout)
}

// R-5: an operator upgrade mid-rollout (another renderer) continues with
// the same group, and a stall is not forgotten.
func TestPlanRollout_SecondUpgradeKeepsHolderAndStall(t *testing.T) {
	views := []rolloutGroupView{pendingView("lion"), pendingView("tiger")}
	planOnce(t, rolloutT0, "renderer-1", RolloutPacerConfig{}, views)
	markStarted(views, "lion", rolloutT0)
	views[0].UpdateStalled = "pod not Ready"
	views[0].State = rolloutGroupState{Pending: []string{"mysql/pdx"}, Incomplete: []string{"mysql/iad"}}
	planOnce(t, rolloutT0.Add(time.Minute), "renderer-1", RolloutPacerConfig{}, views)
	requirePhase(t, views, "lion", v1alpha1.RolloutPhaseStalled, "")
	before := recordOf(views, "lion").DeepCopy()

	// The second release: every workload is pending again, lion's too.
	views[0].State = rolloutGroupState{Pending: []string{"mysql/iad", "mysql/pdx"}, Incomplete: []string{"mysql/iad"}}
	planOnce(t, rolloutT0.Add(2*time.Minute), "renderer-2", RolloutPacerConfig{}, views)
	after := recordOf(views, "lion")
	if after.Renderer != "renderer-2" || after.Phase != v1alpha1.RolloutPhaseStalled ||
		!after.StartedTime.Equal(before.StartedTime) || !after.StalledTime.Equal(before.StalledTime) || !after.AdmittedTime.Equal(before.AdmittedTime) {
		t.Fatalf("lion after the second upgrade = %+v, want the same admission retargeted to renderer-2", after)
	}
	requirePhase(t, views, "tiger", v1alpha1.RolloutPhasePending, rolloutReasonHalted)
	if r := recordOf(views, "tiger"); r.Renderer != "renderer-2" || r.InFlight() {
		t.Fatalf("tiger = %+v, want pending for renderer-2", r)
	}
}

// R-6: a group something else holds is passed over, does not block the
// others, and is not rolled outside the slot when its hold lifts.
func TestPlanRollout_UnstableGroupDoesNotBlockTheFleet(t *testing.T) {
	cfg := RolloutPacerConfig{Soak: -1}
	views := []rolloutGroupView{pendingView("lion"), pendingView("tiger")}
	views[0].Unstable = v1alpha1.UnstableReasonRestoreInPlace
	planOnce(t, rolloutT0, testRenderer, cfg, views)
	requirePhase(t, views, "lion", v1alpha1.RolloutPhasePending, rolloutReasonGroupNotStable)
	requirePhase(t, views, "tiger", v1alpha1.RolloutPhaseAdmitted, rolloutReasonAdmitted)

	// lion's restore ends while tiger rolls: lion waits for the slot.
	markStarted(views, "tiger", rolloutT0)
	views[0].Unstable = ""
	planOnce(t, rolloutT0.Add(time.Minute), testRenderer, cfg, views)
	requirePhase(t, views, "lion", v1alpha1.RolloutPhasePending, rolloutReasonWaitingForSlot)
	if recordOf(views, "lion").InFlight() {
		t.Fatal("lion was admitted while tiger holds the slot")
	}

	// tiger completes; lion is next.
	views[1].State = rolloutGroupState{}
	planOnce(t, rolloutT0.Add(2*time.Minute), testRenderer, cfg, views)
	requirePhase(t, views, "tiger", v1alpha1.RolloutPhaseComplete, "")
	planOnce(t, rolloutT0.Add(2*time.Minute), testRenderer, cfg, views)
	requirePhase(t, views, "lion", v1alpha1.RolloutPhaseAdmitted, rolloutReasonAdmitted)
}

// An admitted group that does not start (a migration lease, a hold that
// arrived after admission) gives the slot back after the grace and goes to
// the back of the queue; one that started is never deferred.
func TestPlanRollout_DefersAdmissionThatDoesNotStart(t *testing.T) {
	cfg := RolloutPacerConfig{StartGrace: 5 * time.Minute, Soak: -1}
	views := []rolloutGroupView{pendingView("lion"), pendingView("puma"), pendingView("tiger")}
	planOnce(t, rolloutT0, testRenderer, cfg, views)
	requirePhase(t, views, "lion", v1alpha1.RolloutPhaseAdmitted, "")

	if actions, _ := planOnce(t, rolloutT0.Add(4*time.Minute), testRenderer, cfg, views); len(actions) != 0 {
		t.Fatalf("deferred before the grace: %+v", actions)
	}
	actions, _ := planOnce(t, rolloutT0.Add(5*time.Minute), testRenderer, cfg, views)
	// The slot a withdrawal gives back is given out by the next pass: the
	// withdrawal is a conditional write that can lose to lion's start
	// latch, and puma must not be admitted beside a lion that is rolling.
	if got := eventReasons(actions); got["lion"] != v1alpha1.EventRolloutDeferred || got["puma"] != "" {
		t.Fatalf("events = %v, want lion deferred and nobody admitted in the same pass", got)
	}
	requirePhase(t, views, "puma", v1alpha1.RolloutPhasePending, rolloutReasonWaitingForSlot)
	requirePhase(t, views, "lion", v1alpha1.RolloutPhasePending, rolloutReasonNotStarted)
	actions, _ = planOnce(t, rolloutT0.Add(5*time.Minute), testRenderer, cfg, views)
	if got := eventReasons(actions); got["puma"] != v1alpha1.EventRolloutAdmitted {
		t.Fatalf("events = %v, want puma admitted by the pass after the deferral", got)
	}
	requirePhase(t, views, "puma", v1alpha1.RolloutPhaseAdmitted, "")

	// puma completes: tiger goes before the deferred lion.
	markStarted(views, "puma", rolloutT0.Add(5*time.Minute))
	views[1].State = rolloutGroupState{}
	planOnce(t, rolloutT0.Add(6*time.Minute), testRenderer, cfg, views)
	planOnce(t, rolloutT0.Add(6*time.Minute), testRenderer, cfg, views)
	requirePhase(t, views, "tiger", v1alpha1.RolloutPhaseAdmitted, "")
	requirePhase(t, views, "lion", v1alpha1.RolloutPhasePending, "")

	// A started roll is never deferred, however long it takes.
	markStarted(views, "tiger", rolloutT0.Add(6*time.Minute))
	planOnce(t, rolloutT0.Add(48*time.Hour), testRenderer, cfg, views)
	if r := recordOf(views, "tiger"); !r.Started() || r.Phase != v1alpha1.RolloutPhaseStalled {
		t.Fatalf("tiger = %+v, want it to keep the slot, stalled", r)
	}
}

func TestPlanRollout_SoakBetweenGroups(t *testing.T) {
	cfg := RolloutPacerConfig{Soak: 2 * time.Minute}
	views := []rolloutGroupView{pendingView("lion"), pendingView("tiger")}
	planOnce(t, rolloutT0, testRenderer, cfg, views)
	markStarted(views, "lion", rolloutT0)
	views[0].State = rolloutGroupState{}
	done := rolloutT0.Add(3 * time.Minute)
	actions, _ := planOnce(t, done, testRenderer, cfg, views)
	if got := eventReasons(actions); got["lion"] != v1alpha1.EventRolloutCompleted || got["tiger"] != "" {
		t.Fatalf("events = %v, want only lion completed", got)
	}
	planOnce(t, done.Add(time.Minute), testRenderer, cfg, views)
	requirePhase(t, views, "tiger", v1alpha1.RolloutPhasePending, rolloutReasonWaitingForSlot)
	planOnce(t, done.Add(2*time.Minute), testRenderer, cfg, views)
	requirePhase(t, views, "tiger", v1alpha1.RolloutPhaseAdmitted, "")
}

// A started roll completes only when nothing is pending, every rollout
// finished and the group is stable.
func TestPlanRollout_CompletionNeedsAStableGroup(t *testing.T) {
	views := []rolloutGroupView{pendingView("lion")}
	planOnce(t, rolloutT0, testRenderer, RolloutPacerConfig{}, views)
	markStarted(views, "lion", rolloutT0)
	for _, tc := range []struct {
		name  string
		state rolloutGroupState
		err   string
		unst  string
	}{
		{name: "a site not rolled", state: rolloutGroupState{Pending: []string{"mysql/pdx"}}},
		{name: "a rollout not finished", state: rolloutGroupState{Incomplete: []string{"dragonfly/iad"}}},
		{name: "group not stable", unst: v1alpha1.UnstableReasonReplicationNotRunning},
		{name: "workloads unreadable", err: "get deployment: timeout"},
	} {
		views[0].State, views[0].StateErr, views[0].Unstable = tc.state, tc.err, tc.unst
		planOnce(t, rolloutT0.Add(time.Minute), testRenderer, RolloutPacerConfig{}, views)
		if r := recordOf(views, "lion"); r == nil || !r.Started() {
			t.Fatalf("%s: lion = %+v, want the roll still in flight", tc.name, r)
		}
	}
	views[0].State, views[0].StateErr, views[0].Unstable = rolloutGroupState{}, "", ""
	planOnce(t, rolloutT0.Add(2*time.Minute), testRenderer, RolloutPacerConfig{}, views)
	requirePhase(t, views, "lion", v1alpha1.RolloutPhaseComplete, "")
	if r := recordOf(views, "lion"); r.CompletedTime == nil || r.StartedTime == nil || r.InFlight() {
		t.Fatalf("lion = %+v", r)
	}
}

// The administrator's annotations: rollout-admit admits past the queue and
// a halt; rollout-release-slot lets the fleet continue past a stalled group.
func TestPlanRollout_Annotations(t *testing.T) {
	cfg := RolloutPacerConfig{Soak: -1}
	views := []rolloutGroupView{pendingView("lion"), pendingView("puma"), pendingView("tiger")}
	planOnce(t, rolloutT0, testRenderer, cfg, views)
	markStarted(views, "lion", rolloutT0)
	views[0].UpdateStalled = "pod not Ready"
	planOnce(t, rolloutT0.Add(time.Minute), testRenderer, cfg, views)
	requirePhase(t, views, "puma", v1alpha1.RolloutPhasePending, rolloutReasonHalted)

	views[2].Admit = true
	planOnce(t, rolloutT0.Add(2*time.Minute), testRenderer, cfg, views)
	requirePhase(t, views, "tiger", v1alpha1.RolloutPhaseAdmitted, rolloutReasonAdmittedByAnnotation)
	requirePhase(t, views, "puma", v1alpha1.RolloutPhasePending, rolloutReasonHalted)

	// tiger completes; lion still holds the slot until it is released.
	markStarted(views, "tiger", rolloutT0.Add(2*time.Minute))
	views[2].State = rolloutGroupState{}
	planOnce(t, rolloutT0.Add(3*time.Minute), testRenderer, cfg, views)
	planOnce(t, rolloutT0.Add(3*time.Minute), testRenderer, cfg, views)
	requirePhase(t, views, "puma", v1alpha1.RolloutPhasePending, rolloutReasonHalted)

	views[0].ReleaseSlot = true
	planOnce(t, rolloutT0.Add(4*time.Minute), testRenderer, cfg, views)
	requirePhase(t, views, "puma", v1alpha1.RolloutPhaseAdmitted, rolloutReasonAdmitted)
	requirePhase(t, views, "lion", v1alpha1.RolloutPhaseStalled, "")
}

// A record is dropped when the group has nothing to roll any more (the
// release was rolled back before the group's turn, or before it started).
func TestPlanRollout_ClearsRecordsWithNothingToRoll(t *testing.T) {
	views := []rolloutGroupView{pendingView("lion"), pendingView("tiger")}
	planOnce(t, rolloutT0, testRenderer, RolloutPacerConfig{}, views)
	views[0].State, views[1].State = rolloutGroupState{}, rolloutGroupState{}
	planOnce(t, rolloutT0.Add(time.Minute), "renderer-old", RolloutPacerConfig{}, views)
	if recordOf(views, "lion") != nil || recordOf(views, "tiger") != nil {
		t.Fatalf("records = %+v / %+v, want none", recordOf(views, "lion"), recordOf(views, "tiger"))
	}
}

// A group whose workloads cannot be read is not admitted, and one that
// holds the slot keeps it.
func TestPlanRollout_UnreadableGroup(t *testing.T) {
	views := []rolloutGroupView{{Key: rolloutKey("lion"), StateErr: "digest key missing"}, pendingView("tiger")}
	planOnce(t, rolloutT0, testRenderer, RolloutPacerConfig{}, views)
	if recordOf(views, "lion") != nil {
		t.Fatalf("lion = %+v, want no record", recordOf(views, "lion"))
	}
	requirePhase(t, views, "tiger", v1alpha1.RolloutPhaseAdmitted, "")
	views[1].StateErr = "list deployments: timeout"
	if actions, _ := planOnce(t, rolloutT0.Add(4*time.Minute), testRenderer, RolloutPacerConfig{}, views); len(actions) != 0 {
		t.Fatalf("an unreadable holder was changed: %+v", actions)
	}
	// It does not hold the slot for ever without a signal: an unstarted
	// admission is withdrawn after the grace, a started one is recorded as
	// stalled after the stall timeout. Neither is ever completed unread.
	planOnce(t, rolloutT0.Add(5*time.Minute), testRenderer, RolloutPacerConfig{}, views)
	requirePhase(t, views, "tiger", v1alpha1.RolloutPhasePending, rolloutReasonNotStarted)

	started := []rolloutGroupView{pendingView("puma")}
	planOnce(t, rolloutT0, testRenderer, RolloutPacerConfig{}, started)
	markStarted(started, "puma", rolloutT0)
	started[0].State, started[0].StateErr = rolloutGroupState{}, "list deployments: timeout"
	if actions, _ := planOnce(t, rolloutT0.Add(29*time.Minute), testRenderer, RolloutPacerConfig{}, started); len(actions) != 0 {
		t.Fatalf("an unreadable started group was changed before the stall timeout: %+v", actions)
	}
	planOnce(t, rolloutT0.Add(30*time.Minute), testRenderer, RolloutPacerConfig{}, started)
	requirePhase(t, started, "puma", v1alpha1.RolloutPhaseStalled, rolloutReasonStallTimeout)
	planOnce(t, rolloutT0.Add(time.Hour), testRenderer, RolloutPacerConfig{}, started)
	requirePhase(t, started, "puma", v1alpha1.RolloutPhaseStalled, rolloutReasonStallTimeout)
}

// A group admitted by the annotation is never deferred; when it does not
// start within the grace it stops holding the slot, so the queue does not
// wait behind it without a halt.
func TestPlanRollout_IdleAnnotatedGroupDoesNotHoldTheSlot(t *testing.T) {
	cfg := RolloutPacerConfig{StartGrace: 5 * time.Minute, Soak: -1}
	views := []rolloutGroupView{pendingView("lion"), pendingView("tiger")}
	views[0].Admit = true
	planOnce(t, rolloutT0, testRenderer, cfg, views)
	requirePhase(t, views, "lion", v1alpha1.RolloutPhaseAdmitted, rolloutReasonAdmittedByAnnotation)
	requirePhase(t, views, "tiger", v1alpha1.RolloutPhasePending, rolloutReasonWaitingForSlot)
	planOnce(t, rolloutT0.Add(4*time.Minute), testRenderer, cfg, views)
	requirePhase(t, views, "tiger", v1alpha1.RolloutPhasePending, rolloutReasonWaitingForSlot)
	planOnce(t, rolloutT0.Add(5*time.Minute), testRenderer, cfg, views)
	requirePhase(t, views, "lion", v1alpha1.RolloutPhaseAdmitted, rolloutReasonAdmittedByAnnotation)
	requirePhase(t, views, "tiger", v1alpha1.RolloutPhaseAdmitted, rolloutReasonAdmitted)
}

func TestRolloutPacerConfigDefaults(t *testing.T) {
	c := RolloutPacerConfig{}.withDefaults().withDefaults()
	if c.Interval != defaultRolloutPacerInterval || c.Soak != defaultRolloutSoak || c.StallAfter != defaultRolloutStallAfter || c.StartGrace != defaultRolloutStartGrace {
		t.Fatalf("defaults = %+v", c)
	}
	if c := (RolloutPacerConfig{Soak: -1}).withDefaults().withDefaults(); c.Soak >= 0 {
		t.Fatalf("a disabled soak became %s", c.Soak)
	}
}
