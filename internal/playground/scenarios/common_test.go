package scenarios

import (
	"context"
	"strings"
	"testing"
	"time"

	v1alpha1 "github.com/shipstream/bloodraven/api/v1alpha1"
	pgkube "github.com/shipstream/bloodraven/internal/playground/kube"
	"github.com/shipstream/bloodraven/internal/playground/runner"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/kubernetes/fake"
	ctrlfake "sigs.k8s.io/controller-runtime/pkg/client/fake"
)

func TestAssertDragonflyHealthyBaselineRequiresEnabledSpec(t *testing.T) {
	env := testScenarioEnv(t, testBaselineMFG(false))

	err := AssertDragonflyHealthyBaseline(context.Background(), env)
	if err == nil {
		t.Fatalf("AssertDragonflyHealthyBaseline returned nil; want spec.dragonfly error")
	}
	if !strings.Contains(err.Error(), "spec.dragonfly.enabled is not true") {
		t.Fatalf("error %q does not mention disabled dragonfly spec", err)
	}
}

func TestAssertDragonflyHealthyBaselineAcceptsReadyDragonfly(t *testing.T) {
	env := testScenarioEnv(t, testBaselineMFG(true))

	if err := AssertDragonflyHealthyBaseline(context.Background(), env); err != nil {
		t.Fatalf("AssertDragonflyHealthyBaseline: %v", err)
	}
}

func TestAssertDragonflyHealthyBaselineRejectsActiveSiteMismatch(t *testing.T) {
	mfg := testBaselineMFG(true)
	mfg.Status.Dragonfly.ActiveSite = "pdx"
	env := testScenarioEnv(t, mfg)

	err := AssertDragonflyHealthyBaseline(context.Background(), env)
	if err == nil {
		t.Fatalf("AssertDragonflyHealthyBaseline returned nil; want activeSite mismatch error")
	}
	if !strings.Contains(err.Error(), `status.dragonfly.activeSite="pdx" does not match status.activeSite="iad"`) {
		t.Fatalf("error %q does not mention activeSite mismatch", err)
	}
}

func TestAssertHealthyBaselineRejectsOrderedUpdateInProgress(t *testing.T) {
	env := testScenarioEnv(t, testBaselineMFG(true))
	mfg, err := env.Kube.GetMFGNamed(context.Background(), env.Namespace, env.FG)
	if err != nil {
		t.Fatalf("get mfg: %v", err)
	}
	mfg.Status.UpdatePhase = "UpdateReplica"
	if err := env.Kube.Controller.Status().Update(context.Background(), mfg); err != nil {
		t.Fatalf("status update: %v", err)
	}

	err = AssertHealthyBaseline(context.Background(), env)
	if err == nil {
		t.Fatalf("AssertHealthyBaseline returned nil; want updatePhase error")
	}
	if !strings.Contains(err.Error(), "ordered update still in phase") {
		t.Fatalf("error %q does not mention ordered update phase", err)
	}
}

func TestAssertHealthyBaselineUsesAnnotationFailoverRecord(t *testing.T) {
	mfg := testBaselineMFG(false)
	mfg.Annotations = map[string]string{
		v1alpha1.LastFailoverAnnotation:       time.Now().Add(-time.Minute).UTC().Format(time.RFC3339),
		v1alpha1.LastFailoverTargetAnnotation: "pdx",
	}
	env := testScenarioEnv(t, mfg)

	err := AssertHealthyBaseline(context.Background(), env)
	if err == nil || !strings.Contains(err.Error(), "anti-flap cooldown active") {
		t.Fatalf("annotation-only cooldown error = %v", err)
	}
}

// A standing automation hold fails the baseline with the acknowledgement
// command as the remediation (O-m2).
func TestAssertHealthyBaselineRejectsPlannedFailoverHold(t *testing.T) {
	mfg := testBaselineMFG(false)
	mfg.Status.PlannedFailoverHold = &v1alpha1.PlannedFailoverHold{RunID: "run-7",
		Reason: v1alpha1.PlannedFailoverReasonForeignWritable, HoldTime: metav1.Now()}
	err := AssertHealthyBaseline(context.Background(), testScenarioEnv(t, mfg))
	want := "kubectl bloodraven ack-planned-failover-hold " + mfg.Name + " -n " + mfg.Namespace
	if err == nil || !strings.Contains(err.Error(), "automation hold stands (run run-7") || !strings.Contains(err.Error(), want) {
		t.Fatalf("hold baseline error = %v, want the hold and %q", err, want)
	}

	// A hand-edited True condition without the field names no run.
	mfg = testBaselineMFG(false)
	mfg.Status.Conditions = append(mfg.Status.Conditions, metav1.Condition{Type: v1alpha1.ConditionAutomaticFailoverHeld,
		Status: metav1.ConditionTrue, Reason: v1alpha1.ReasonPlannedFailoverManualRecoveryRequired, LastTransitionTime: metav1.Now()})
	err = AssertHealthyBaseline(context.Background(), testScenarioEnv(t, mfg))
	if err == nil || !strings.Contains(err.Error(), "names no planned-failover run") {
		t.Fatalf("condition-only hold baseline error = %v", err)
	}
}

// Issue #262: run-all's suite-entry wait reports a healthy baseline by
// CheckBaseline, the predicate of every scenario's Precheck. A baseline
// whose non-active site is unreachable is not healthy, whatever the active
// site and the Ready condition say, and neither is one with a site the
// operator has not reported yet.
func TestCheckBaselineRejectsUnreachableNonActiveSite(t *testing.T) {
	withReader := func() *v1alpha1.MysqlFailoverGroup {
		mfg := testBaselineMFG(false)
		mfg.Spec.Sites = append(mfg.Spec.Sites, v1alpha1.SiteSpec{Name: "reader", ServerID: 103, Role: v1alpha1.SiteRoleReadOnly})
		mfg.Status.Sites = append(mfg.Status.Sites, v1alpha1.SiteStatus{Name: "reader", State: v1alpha1.SiteStateReadOnly, Replication: v1alpha1.SiteReplicationHealthy})
		return mfg
	}
	check := func(mfg *v1alpha1.MysqlFailoverGroup) error {
		env := testScenarioEnv(t, mfg)
		return CheckBaseline(context.Background(), env.Kube, env.Namespace, env.FG)
	}

	if err := check(withReader()); err != nil {
		t.Fatalf("healthy three-site baseline: %v", err)
	}

	// The run of #262: Ready=True, the active site writable, the reader
	// (not promotable, not active) unreachable.
	mfg := withReader()
	mfg.Status.Sites[2].State = v1alpha1.SiteStateUnreachable
	mfg.Status.Sites[2].Replication = ""
	err := check(mfg)
	if err == nil || !strings.Contains(err.Error(), `site reader state="Unreachable"`) {
		t.Fatalf("unreachable reader: baseline error = %v, want the reader's state", err)
	}

	// An unreachable promotable standby is the same.
	mfg = withReader()
	mfg.Status.Sites[1].State = v1alpha1.SiteStateUnreachable
	if err := check(mfg); err == nil || !strings.Contains(err.Error(), "site pdx") {
		t.Fatalf("unreachable standby: baseline error = %v, want site pdx", err)
	}

	// A site of the spec with no status entry yet (still coming up after
	// setup.sh) used to pass, because only status.sites was walked.
	mfg = withReader()
	mfg.Status.Sites = mfg.Status.Sites[:2]
	if err := check(mfg); err == nil || !strings.Contains(err.Error(), "site reader") || !strings.Contains(err.Error(), "no status reported yet") {
		t.Fatalf("unreported reader: baseline error = %v, want site reader with no status", err)
	}
}

// A paced rollout that waits, rolls or is stalled is not a baseline; a
// completed one is.
func TestCheckBaselineRejectsUnfinishedPacedRollout(t *testing.T) {
	for _, tc := range []struct {
		phase   v1alpha1.RolloutPhase
		healthy bool
	}{
		{v1alpha1.RolloutPhasePending, false},
		{v1alpha1.RolloutPhaseAdmitted, false},
		{v1alpha1.RolloutPhaseRolling, false},
		{v1alpha1.RolloutPhaseStalled, false},
		{v1alpha1.RolloutPhaseComplete, true},
	} {
		mfg := testBaselineMFG(false)
		mfg.Status.Rollout = &v1alpha1.RolloutStatus{Phase: tc.phase, Reason: "WaitingForSlot"}
		env := testScenarioEnv(t, mfg)
		err := CheckBaseline(context.Background(), env.Kube, env.Namespace, env.FG)
		if tc.healthy && err != nil {
			t.Errorf("phase %s: %v", tc.phase, err)
		}
		if !tc.healthy && (err == nil || !strings.Contains(err.Error(), "paced rollout in phase")) {
			t.Errorf("phase %s: baseline error = %v, want the paced rollout", tc.phase, err)
		}
	}
}

func testScenarioEnv(t *testing.T, mfg *v1alpha1.MysqlFailoverGroup) *runner.Env {
	t.Helper()
	scheme := runtime.NewScheme()
	if err := v1alpha1.AddToScheme(scheme); err != nil {
		t.Fatalf("AddToScheme: %v", err)
	}
	k := &pgkube.Client{
		Kubernetes: fake.NewSimpleClientset(),
		Controller: ctrlfake.NewClientBuilder().
			WithScheme(scheme).
			WithStatusSubresource(&v1alpha1.MysqlFailoverGroup{}).
			WithRuntimeObjects(mfg).
			Build(),
	}
	return &runner.Env{
		Namespace: mfg.Namespace,
		FG:        mfg.Name,
		Kube:      k,
	}
}

func testBaselineMFG(dragonflyEnabled bool) *v1alpha1.MysqlFailoverGroup {
	mfg := &v1alpha1.MysqlFailoverGroup{
		ObjectMeta: metav1.ObjectMeta{
			Name:      pgkube.FailoverGroupName,
			Namespace: pgkube.PlaygroundNamespace,
		},
		Spec: v1alpha1.MysqlFailoverGroupSpec{
			Sites: []v1alpha1.SiteSpec{
				{Name: "iad", ServerID: 101},
				{Name: "pdx", ServerID: 102},
			},
		},
		Status: v1alpha1.MysqlFailoverGroupStatus{
			ActiveSite: "iad",
			Conditions: []metav1.Condition{{
				Type:   "Ready",
				Status: metav1.ConditionTrue,
			}},
			Sites: []v1alpha1.SiteStatus{
				{Name: "iad", State: v1alpha1.SiteStateWritable},
				{Name: "pdx", State: v1alpha1.SiteStateReadOnly, Replication: v1alpha1.SiteReplicationHealthy},
			},
		},
	}
	if dragonflyEnabled {
		mfg.Spec.Dragonfly = &v1alpha1.DragonflySpec{Enabled: true}
		mfg.Status.Dragonfly = &v1alpha1.DragonflyStatus{
			ActiveSite: "iad",
			Phase:      v1alpha1.DragonflyPhaseReady,
			Sites: []v1alpha1.DragonflySiteStatus{
				{
					Name:      "iad",
					Role:      v1alpha1.DragonflyRoleMaster,
					Reachable: true,
				},
				{
					Name:             "pdx",
					Role:             v1alpha1.DragonflyRoleReplica,
					Reachable:        true,
					LinkStatus:       "up",
					LastIOSecondsAgo: new(int64(0)),
				},
			},
		}
	}
	return mfg
}
