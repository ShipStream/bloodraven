package runner

import (
	"context"
	"fmt"
	"log/slog"
	"strings"
	"testing"
	"time"

	v1alpha1 "github.com/shipstream/bloodraven/api/v1alpha1"
	pgkube "github.com/shipstream/bloodraven/internal/playground/kube"
	pgwait "github.com/shipstream/bloodraven/internal/playground/wait"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	ctrlfake "sigs.k8s.io/controller-runtime/pkg/client/fake"
)

func TestRegistryRegisterListGet(t *testing.T) {
	r := &Registry{scenarios: map[string]Scenario{}}
	r.Register(Scenario{ID: "02", Title: "second"})
	r.Register(Scenario{ID: "01", Title: "first"})

	list := r.List()
	if len(list) != 2 {
		t.Fatalf("List len = %d, want 2", len(list))
	}
	if list[0].ID != "01" || list[1].ID != "02" {
		t.Fatalf("List not lex-sorted: %v", []string{list[0].ID, list[1].ID})
	}

	if got, ok := r.Get("01"); !ok || got.Title != "first" {
		t.Fatalf("Get(01) = %+v ok=%v", got, ok)
	}
	if _, ok := r.Get("missing"); ok {
		t.Fatalf("Get(missing) should report ok=false")
	}
}

func TestRegistryDuplicateIDPanics(t *testing.T) {
	r := &Registry{scenarios: map[string]Scenario{}}
	r.Register(Scenario{ID: "01"})
	defer func() {
		got := recover()
		if got == nil {
			t.Fatalf("expected panic on duplicate ID")
		}
		if !strings.Contains(fmt.Sprint(got), "duplicate scenario ID") {
			t.Fatalf("unexpected panic: %v", got)
		}
	}()
	r.Register(Scenario{ID: "01"})
}

func TestRegistryEmptyIDPanics(t *testing.T) {
	r := &Registry{scenarios: map[string]Scenario{}}
	defer func() {
		if recover() == nil {
			t.Fatalf("expected panic on empty ID")
		}
	}()
	r.Register(Scenario{ID: ""})
}

func TestStepRunsSequentially(t *testing.T) {
	// Smoke test of the Step struct shape: ensure phases are
	// distinguishable. The full executor needs a live cluster and is
	// covered by scenario integration tests.
	var phases []Phase
	steps := []Step{
		{Phase: PhaseInject, Name: "a", Do: func(ctx context.Context, env *Env) error {
			phases = append(phases, PhaseInject)
			return nil
		}},
		{Phase: PhaseVerify, Name: "b", Do: func(ctx context.Context, env *Env) error {
			phases = append(phases, PhaseVerify)
			return nil
		}},
	}
	for _, s := range steps {
		if err := s.Do(context.Background(), nil); err != nil {
			t.Fatalf("step %s err: %v", s.Name, err)
		}
	}
	if len(phases) != 2 || phases[0] != PhaseInject || phases[1] != PhaseVerify {
		t.Fatalf("phase order = %v, want [inject verify]", phases)
	}
}

func TestWaitForClusterReconvergeRequiresDragonflyReady(t *testing.T) {
	env := testReconvergeEnv(t, testReconvergeMFG(v1alpha1.DragonflyPhaseDegraded))

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	err := waitForClusterReconvergeStable(ctx, env, time.Millisecond)
	if err == nil {
		t.Fatalf("waitForClusterReconverge returned nil; want timeout while dragonfly is degraded")
	}
	if !strings.Contains(err.Error(), "dragonfly.phase=Degraded") {
		t.Fatalf("error %q does not mention degraded dragonfly phase", err)
	}
}

func TestWaitForClusterReconvergeRequiresDragonflyActiveSiteMatch(t *testing.T) {
	mfg := testReconvergeMFG(v1alpha1.DragonflyPhaseReady)
	mfg.Status.Dragonfly.ActiveSite = "pdx"
	env := testReconvergeEnv(t, mfg)

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	err := waitForClusterReconvergeStable(ctx, env, time.Millisecond)
	if err == nil {
		t.Fatalf("waitForClusterReconverge returned nil; want timeout on dragonfly/mysql activeSite mismatch")
	}
	if !strings.Contains(err.Error(), "dragonfly.activeSite=pdx mysql.activeSite=iad") {
		t.Fatalf("error %q does not mention activeSite mismatch", err)
	}
}

func TestWaitForClusterReconvergeRequiresNoOrderedUpdate(t *testing.T) {
	mfg := testReconvergeMFG(v1alpha1.DragonflyPhaseReady)
	mfg.Status.UpdatePhase = "UpdateReplica"
	env := testReconvergeEnv(t, mfg)

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	err := waitForClusterReconvergeStable(ctx, env, time.Millisecond)
	if err == nil {
		t.Fatalf("waitForClusterReconverge returned nil; want timeout during ordered update")
	}
	if !strings.Contains(err.Error(), "updatePhase=UpdateReplica") {
		t.Fatalf("error %q does not mention update phase", err)
	}
}

func TestWaitForClusterReconvergeAcceptsDragonflyReady(t *testing.T) {
	env := testReconvergeEnv(t, testReconvergeMFG(v1alpha1.DragonflyPhaseReady))

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := waitForClusterReconvergeStable(ctx, env, time.Millisecond); err != nil {
		t.Fatalf("waitForClusterReconverge: %v", err)
	}
}

func TestWaitForClusterReconvergeRequiresStableHealthyWindow(t *testing.T) {
	env := testReconvergeEnv(t, testReconvergeMFG(v1alpha1.DragonflyPhaseReady))

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	err := waitForClusterReconvergeStable(ctx, env, time.Hour)
	if err == nil {
		t.Fatalf("waitForClusterReconvergeStable returned nil; want timeout before stable window elapses")
	}
	if !strings.Contains(err.Error(), "stableFor=") {
		t.Fatalf("error %q does not mention stable window", err)
	}
}

func testReconvergeEnv(t *testing.T, mfg *v1alpha1.MysqlFailoverGroup) *Env {
	t.Helper()
	scheme := runtime.NewScheme()
	if err := v1alpha1.AddToScheme(scheme); err != nil {
		t.Fatalf("AddToScheme: %v", err)
	}
	k := &pgkube.Client{
		Controller: ctrlfake.NewClientBuilder().WithScheme(scheme).WithRuntimeObjects(mfg).Build(),
	}
	return &Env{
		Namespace: mfg.Namespace,
		FG:        mfg.Name,
		Kube:      k,
		Wait: &pgwait.Helper{
			Kube:     k,
			Logger:   slog.Default(),
			Interval: time.Millisecond,
			FG:       mfg.Name,
		},
	}
}

func testReconvergeMFG(dfPhase v1alpha1.DragonflyPhase) *v1alpha1.MysqlFailoverGroup {
	return &v1alpha1.MysqlFailoverGroup{
		ObjectMeta: metav1.ObjectMeta{
			Name:      pgkube.FailoverGroupName,
			Namespace: pgkube.PlaygroundNamespace,
		},
		Spec: v1alpha1.MysqlFailoverGroupSpec{
			Dragonfly: &v1alpha1.DragonflySpec{Enabled: true},
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
			Dragonfly: &v1alpha1.DragonflyStatus{
				Enabled:    true,
				ActiveSite: "iad",
				Phase:      dfPhase,
				Sites: []v1alpha1.DragonflySiteStatus{
					{
						Name:      "iad",
						Role:      v1alpha1.DragonflyRoleMaster,
						Reachable: true,
						Ready:     true,
					},
					{
						Name:             "pdx",
						Role:             v1alpha1.DragonflyRoleReplica,
						Reachable:        true,
						Ready:            true,
						LinkStatus:       "up",
						LastIOSecondsAgo: 0,
					},
				},
			},
		},
	}
}
