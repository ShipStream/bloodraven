package scenarios

import (
	"regexp"
	"strings"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	v1alpha1 "github.com/shipstream/bloodraven/api/v1alpha1"
	"github.com/shipstream/bloodraven/internal/playground/runner"
)

// Scenarios 63 and 64 (#235): 63 is release-graded, 64 runs in full only.
// Both also run in the targeted planned-failover CI job.
func TestScenarios6364RegisteredAndProfiled(t *testing.T) {
	all := runner.DefaultRegistry.List()
	inRelease := map[string]bool{s63ID: true, s64ID: false}
	for id, release := range inRelease {
		s, ok := runner.DefaultRegistry.Get(id)
		if !ok {
			t.Errorf("scenario %s is not registered", id)
			continue
		}
		if s.Quarantine != "" || s.Timeout <= 0 || s.Precheck == nil || s.Cleanup == nil || len(s.Steps) == 0 {
			t.Errorf("scenario %s incomplete: quarantine=%q timeout=%s precheck=%v cleanup=%v steps=%d",
				id, s.Quarantine, s.Timeout, s.Precheck != nil, s.Cleanup != nil, len(s.Steps))
		}
		if s.Hypothesis == "" || s.DocLink == "" {
			t.Errorf("scenario %s is missing a hypothesis or doc link", id)
		}
		if s.CleanupTimeout < runner.DefaultCleanupTimeout {
			t.Errorf("scenario %s CleanupTimeout=%s is below the default %s", id, s.CleanupTimeout, runner.DefaultCleanupTimeout)
		}
		if !inventoryContains(runner.SelectForProfile(all, runner.ProfileFull), id) {
			t.Errorf("scenario %s must be in the full profile", id)
		}
		if inventoryContains(runner.SelectForProfile(all, runner.ProfileSmoke), id) {
			t.Errorf("scenario %s must not be in the smoke profile", id)
		}
		if got := inventoryContains(runner.SelectForProfile(all, runner.ProfileRelease), id); got != release {
			t.Errorf("scenario %s in release profile = %v, want %v", id, got, release)
		}
	}
}

func TestCurrentPlannedRunIgnoresBaselineAndEmptyRunID(t *testing.T) {
	mfg := &v1alpha1.MysqlFailoverGroup{}
	if currentPlannedRun(mfg, "") != nil {
		t.Fatal("no block must not be a current run")
	}
	mfg.Status.PlannedFailover = &v1alpha1.PlannedFailoverStatus{Phase: v1alpha1.PlannedFailoverPhaseDeferred}
	if currentPlannedRun(mfg, "") != nil {
		t.Fatal("a block without runId must not be a current run")
	}
	mfg.Status.PlannedFailover.RunID = "old"
	if currentPlannedRun(mfg, "old") != nil {
		t.Fatal("the baseline run must not be a current run")
	}
	if currentPlannedRun(mfg, "") == nil || currentPlannedRun(mfg, "other") == nil {
		t.Fatal("a run with a new runId must be current")
	}
}

func TestPlannedOwnsSitesMatchesSpecPhaseSet(t *testing.T) {
	owns := map[v1alpha1.PlannedFailoverPhase]bool{
		v1alpha1.PlannedFailoverPhaseAdmitting:               false,
		v1alpha1.PlannedFailoverPhaseDeferred:                false,
		v1alpha1.PlannedFailoverPhaseFencing:                 true,
		v1alpha1.PlannedFailoverPhaseDraining:                true,
		v1alpha1.PlannedFailoverPhaseWaitingForLag:           true,
		v1alpha1.PlannedFailoverPhaseWaitingForDragonflySync: true,
		v1alpha1.PlannedFailoverPhasePromotingDragonfly:      true,
		v1alpha1.PlannedFailoverPhasePromoting:               true,
		v1alpha1.PlannedFailoverPhaseRollingBack:             true,
		v1alpha1.PlannedFailoverPhasePublishing:              false,
		v1alpha1.PlannedFailoverPhaseSucceeded:               false,
		v1alpha1.PlannedFailoverPhaseFailed:                  false,
	}
	for phase, want := range owns {
		if got := plannedOwnsSites(phase); got != want {
			t.Errorf("plannedOwnsSites(%s)=%v, want %v", phase, got, want)
		}
	}
}

func TestS63HoldViolation(t *testing.T) {
	held := func() *v1alpha1.MysqlFailoverGroup {
		mfg := &v1alpha1.MysqlFailoverGroup{}
		mfg.Status.ActiveSite = ""
		mfg.Status.SetLastFailover(mfg.Status.LastFailoverTime(), "iad", mfg.Status.PromotionGtidExecuted())
		mfg.Status.PlannedFailover = &v1alpha1.PlannedFailoverStatus{RunID: "r1", Phase: v1alpha1.PlannedFailoverPhaseWaitingForLag, TargetSite: "pdx"}
		return mfg
	}
	if err := s63HoldViolation(held(), "r1", "pdx", "iad"); err != nil {
		t.Fatalf("held run reported as violation: %v", err)
	}
	cases := map[string]func(*v1alpha1.MysqlFailoverGroup){
		"block vanished": func(m *v1alpha1.MysqlFailoverGroup) { m.Status.PlannedFailover = nil },
		"new runId":      func(m *v1alpha1.MysqlFailoverGroup) { m.Status.PlannedFailover.RunID = "r2" },
		"phase advanced": func(m *v1alpha1.MysqlFailoverGroup) {
			m.Status.PlannedFailover.Phase = v1alpha1.PlannedFailoverPhasePromoting
		},
		"target promoted": func(m *v1alpha1.MysqlFailoverGroup) { m.Status.ActiveSite = "pdx" },
		"failover recorded": func(m *v1alpha1.MysqlFailoverGroup) {
			m.Status.SetLastFailover(m.Status.LastFailoverTime(), "pdx", m.Status.PromotionGtidExecuted())
		},
	}
	for name, mutate := range cases {
		mfg := held()
		mutate(mfg)
		if err := s63HoldViolation(mfg, "r1", "pdx", "iad"); err == nil {
			t.Errorf("%s: no violation reported", name)
		}
	}
}

func TestCountMFGEventsSumsDeduplicatedOccurrences(t *testing.T) {
	start := time.Date(2026, 11, 4, 12, 0, 0, 0, time.UTC)
	ev := func(reason, name string, at time.Time, count int32) corev1.Event {
		return corev1.Event{
			InvolvedObject: corev1.ObjectReference{Kind: "MysqlFailoverGroup", Name: name},
			Reason:         reason, LastTimestamp: metav1.NewTime(at), Count: count,
		}
	}
	events := []corev1.Event{
		ev(plannedClaimedEventReason, "playground", start.Add(time.Minute), 1),
		ev(plannedClaimedEventReason, "playground", start.Add(-time.Hour), 1), // before the window
		ev(plannedClaimedEventReason, "other", start.Add(time.Minute), 1),     // another group
		ev("PlannedFailoverStarted", "playground", start.Add(time.Minute), 1), // another reason
	}
	if n := countMFGEvents(events, "playground", start, plannedClaimedEventReason); n != 1 {
		t.Fatalf("count=%d, want 1", n)
	}
	events[0].Count = 2
	if n := countMFGEvents(events, "playground", start, plannedClaimedEventReason); n != 2 {
		t.Fatalf("deduplicated count=%d, want 2", n)
	}
	events[0].Count = 0
	events[0].Series = &corev1.EventSeries{Count: 3}
	if n := countMFGEvents(events, "playground", start, plannedClaimedEventReason); n != 3 {
		t.Fatalf("series count=%d, want 3", n)
	}
}

func TestResetReplicaAllRegexpMatchesOnlyTheStatement(t *testing.T) {
	re := regexp.MustCompile(resetReplicaAllRegexp)
	for _, stmt := range []string{"RESET REPLICA ALL", "  reset replica all", "RESET SLAVE ALL", "RESET REPLICA ALL FOR CHANNEL ''"} {
		if !re.MatchString(strings.ToUpper(stmt)) {
			t.Errorf("%q must match", stmt)
		}
	}
	for _, stmt := range []string{"RESET REPLICA", "STOP REPLICA", "SELECT COUNT(*) FROM mysql.general_log WHERE argument REGEXP 'RESET REPLICA ALL'"} {
		if re.MatchString(strings.ToUpper(stmt)) {
			t.Errorf("%q must not match", stmt)
		}
	}
	for v, want := range map[string]bool{"FILE": true, "TABLE": true, "FILE,TABLE": true, "NONE": true, "file": true, "": false, "FILE'; DROP": false} {
		if got := logOutputValue.MatchString(v); got != want {
			t.Errorf("logOutputValue(%q)=%v, want %v", v, got, want)
		}
	}
}

func TestReplacementPodSkipsKilledAndUnready(t *testing.T) {
	pod := func(name string, phase corev1.PodPhase, ready bool) corev1.Pod {
		status := corev1.ConditionFalse
		if ready {
			status = corev1.ConditionTrue
		}
		return corev1.Pod{
			ObjectMeta: metav1.ObjectMeta{Name: name},
			Status:     corev1.PodStatus{Phase: phase, Conditions: []corev1.PodCondition{{Type: corev1.PodReady, Status: status}}},
		}
	}
	pods := []corev1.Pod{
		pod("old", corev1.PodRunning, true),
		pod("starting", corev1.PodRunning, false),
		pod("pending", corev1.PodPending, false),
	}
	if got := replacementPod(pods, []string{"old"}); got != "" {
		t.Fatalf("replacementPod=%q, want none", got)
	}
	pods = append(pods, pod("new", corev1.PodRunning, true))
	if got := replacementPod(pods, []string{"old"}); got != "new" {
		t.Fatalf("replacementPod=%q, want new", got)
	}
}

func TestS64DeferredByHold(t *testing.T) {
	if done, _, err := s64DeferredByHold(nil); done || err != nil {
		t.Fatalf("no run: done=%v err=%v", done, err)
	}
	pf := &v1alpha1.PlannedFailoverStatus{RunID: "r1", Phase: v1alpha1.PlannedFailoverPhaseAdmitting}
	if done, _, err := s64DeferredByHold(pf); done || err != nil {
		t.Fatalf("admitting: done=%v err=%v", done, err)
	}
	pf.Phase = v1alpha1.PlannedFailoverPhaseDeferred
	pf.Admission = &v1alpha1.PlannedFailoverAdmission{DeferReason: v1alpha1.PlannedFailoverDeferCooldownActive}
	if done, _, err := s64DeferredByHold(pf); done || err != nil {
		t.Fatalf("cooldown deferral: done=%v err=%v", done, err)
	}
	pf.Admission.DeferReason = v1alpha1.PlannedFailoverDeferDeploymentHold
	if done, _, err := s64DeferredByHold(pf); !done || err != nil {
		t.Fatalf("hold deferral: done=%v err=%v", done, err)
	}
	pf.Phase = v1alpha1.PlannedFailoverPhaseFencing
	if _, _, err := s64DeferredByHold(pf); err == nil {
		t.Fatal("a run that owns sites under a live hold must fail the wait")
	}
}

func TestS64ClassifyOutcome(t *testing.T) {
	result := func(o v1alpha1.PlannedFailoverOutcome, r v1alpha1.PlannedFailoverReason) *v1alpha1.PlannedFailoverResult {
		return &v1alpha1.PlannedFailoverResult{Outcome: o, Reason: r}
	}
	cases := []struct {
		name    string
		pf      v1alpha1.PlannedFailoverStatus
		done    bool
		wantErr bool
	}{
		{"still deferred", v1alpha1.PlannedFailoverStatus{Phase: v1alpha1.PlannedFailoverPhaseDeferred}, false, false},
		{"admitting", v1alpha1.PlannedFailoverStatus{Phase: v1alpha1.PlannedFailoverPhaseAdmitting}, false, false},
		{"claimed new primary", v1alpha1.PlannedFailoverStatus{Phase: v1alpha1.PlannedFailoverPhaseDraining, SourceSite: "pdx"}, false, false},
		{"claimed lost primary", v1alpha1.PlannedFailoverStatus{Phase: v1alpha1.PlannedFailoverPhaseFencing, SourceSite: "iad"}, false, true},
		{"already active", v1alpha1.PlannedFailoverStatus{Phase: v1alpha1.PlannedFailoverPhaseFailed,
			Result: result(v1alpha1.PlannedFailoverOutcomeNotStarted, v1alpha1.PlannedFailoverReasonAlreadyActive)}, true, false},
		// The target is the emergency successor, so a success is impossible
		// and means the run moved the primary off it.
		{"succeeded from new primary", v1alpha1.PlannedFailoverStatus{Phase: v1alpha1.PlannedFailoverPhaseSucceeded, SourceSite: "pdx",
			Result: result(v1alpha1.PlannedFailoverOutcomeSucceeded, "")}, false, true},
		{"succeeded from lost primary", v1alpha1.PlannedFailoverStatus{Phase: v1alpha1.PlannedFailoverPhaseSucceeded, SourceSite: "iad",
			Result: result(v1alpha1.PlannedFailoverOutcomeSucceeded, "")}, false, true},
		{"admission timeout", v1alpha1.PlannedFailoverStatus{Phase: v1alpha1.PlannedFailoverPhaseFailed,
			Result: result(v1alpha1.PlannedFailoverOutcomeNotStarted, v1alpha1.PlannedFailoverReasonAdmissionTimeout)}, false, true},
		{"terminal without result", v1alpha1.PlannedFailoverStatus{Phase: v1alpha1.PlannedFailoverPhaseFailed}, false, true},
	}
	for _, tc := range cases {
		pf := tc.pf
		done, _, err := s64ClassifyOutcome(&pf, "iad", "pdx")
		if done != tc.done || (err != nil) != tc.wantErr {
			t.Errorf("%s: done=%v err=%v, want done=%v err=%v", tc.name, done, err, tc.done, tc.wantErr)
		}
	}
}

func TestDeployLeasesRevokedAndStopped(t *testing.T) {
	at := time.Date(2026, 11, 4, 12, 0, 0, 0, time.UTC)
	rec := func(action, kind string, status int, gen int64, errStr, reason string, ts time.Time) deployClientRecord {
		r := deployClientRecord{At: ts, Action: action, Kind: kind, Status: status}
		r.Body.ExpiresAt = at.Add(2 * time.Minute)
		r.Body.TopologyGeneration = gen
		r.Body.Error = errStr
		r.Body.Reason = reason
		return r
	}
	records := []deployClientRecord{
		rec("grant", "migration", 201, 5, "", "", at),
		rec("grant", "failover-hold", 201, 5, "", "", at),
		rec("renew", "migration", 409, 6, "revoked", "topology_changed", at.Add(30*time.Second)),
	}
	if deployLeasesRevokedAndStopped(records, 5, 2) {
		t.Fatal("one revoked lease and no stop must not satisfy two")
	}
	records = append(records,
		rec("renew", "failover-hold", 409, 6, "revoked", "operator_revoked", at.Add(31*time.Second)),
		deployClientRecord{Action: "stopped", Status: 409})
	if !deployLeasesRevokedAndStopped(records, 5, 2) {
		t.Fatal("both leases revoked by a newer generation and stopped must satisfy")
	}
	if deployLeasesRevokedAndStopped(records, 6, 2) {
		t.Fatal("a revocation at the grant's own generation must not count")
	}
}
