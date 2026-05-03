package scenarios

import (
	"context"
	"strings"
	"testing"

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
				{Name: "iad"},
				{Name: "pdx"},
			},
		},
		Status: v1alpha1.MysqlFailoverGroupStatus{
			ActiveSite: "iad",
			Conditions: []metav1.Condition{{
				Type:   "Ready",
				Status: metav1.ConditionTrue,
			}},
			Sites: []v1alpha1.SiteStatus{
				{Name: "iad", State: "writable"},
				{Name: "pdx", State: "read-only", Replicating: true},
			},
		},
	}
	if dragonflyEnabled {
		mfg.Spec.Dragonfly = &v1alpha1.DragonflySpec{Enabled: true}
		mfg.Status.Dragonfly = &v1alpha1.DragonflyStatus{
			Enabled:    true,
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
					LastIOSecondsAgo: 0,
				},
			},
		}
	}
	return mfg
}
