package scenarios

import (
	"context"
	"fmt"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	v1alpha1 "github.com/shipstream/bloodraven/api/v1alpha1"
	pgkube "github.com/shipstream/bloodraven/internal/playground/kube"
	"github.com/shipstream/bloodraven/internal/playground/runner"
)

func init() {
	runner.Register(scenario29DragonflySnapshotUpgrade())
}

func scenario29DragonflySnapshotUpgrade() runner.Scenario {
	return runner.Scenario{
		ID:    "29-dragonfly-snapshot-upgrade",
		Title: "Dragonfly snapshot-restore upgrade via RustFS",
		Hypothesis: "With the playground RustFS bucket configured as spec.dragonfly.snapshot.dir, annotating " +
			"bloodraven.shipstream.io/dragonfly-snapshot-upgrade snapshots the active Dragonfly, restarts it, " +
			"restores the seeded key, reattaches replicas, and leaves status.dragonfly.upgrade=Succeeded.",
		Risk:     "medium",
		DocLink:  "PLANS-Dragonfly-Chaos-Scenarios.md (D6a)",
		Timeout:  8 * time.Minute,
		Precheck: AssertHealthyBaseline,
		Steps: []runner.Step{
			seedDragonflyCounterForSnapshotUpgrade(),
			annotateDragonflySnapshotUpgrade(),
			observeDragonflySnapshotUpgradeSucceeded(),
			verifyDragonflySnapshotUpgradeKeyRestored(),
			verifyDragonflySnapshotUpgradeImages(),
		},
	}
}

func seedDragonflyCounterForSnapshotUpgrade() runner.Step {
	return runner.Step{
		Phase: runner.PhaseInject,
		Name:  "seed dragonfly key before snapshot upgrade",
		Do: func(ctx context.Context, env *runner.Env) error {
			mfg, err := env.Kube.GetMFG(ctx, env.Namespace)
			if err != nil {
				return err
			}
			if mfg.Spec.Dragonfly == nil || mfg.Spec.Dragonfly.Snapshot == nil || mfg.Spec.Dragonfly.Snapshot.Dir == "" {
				return fmt.Errorf("spec.dragonfly.snapshot.dir is required for D6a")
			}
			active, err := dragonflyActiveSite(mfg)
			if err != nil {
				return err
			}
			if err := ctxStash(ctx, env, "s29Active", active); err != nil {
				return err
			}
			if err := ctxStash(ctx, env, "s29TargetImage", mfg.Spec.Dragonfly.Image); err != nil {
				return err
			}
			cli, err := env.Dragonfly(active)
			if err != nil {
				return err
			}
			value := fmt.Sprintf("scenario29-%d", env.StartTime.UnixNano())
			if _, err := cli.Set(ctx, "scenario29:counter", value); err != nil {
				return err
			}
			return ctxStash(ctx, env, "s29Value", value)
		},
	}
}

func annotateDragonflySnapshotUpgrade() runner.Step {
	return runner.Step{
		Phase: runner.PhaseInject,
		Name:  "annotate dragonfly snapshot upgrade",
		Do: func(ctx context.Context, env *runner.Env) error {
			image := ctxFetch(env, "s29TargetImage")
			env.Capture.Note(fmt.Sprintf("request Dragonfly snapshot upgrade to %s", image))
			return env.Kube.AnnotateMFG(ctx, env.Namespace, "bloodraven.shipstream.io/dragonfly-snapshot-upgrade", image)
		},
	}
}

func observeDragonflySnapshotUpgradeSucceeded() runner.Step {
	return runner.Step{
		Phase: runner.PhaseObserve,
		Name:  "status.dragonfly.upgrade reaches Succeeded",
		Do: func(ctx context.Context, env *runner.Env) error {
			waitCtx, cancel := context.WithTimeout(ctx, 6*time.Minute)
			defer cancel()
			_, err := env.Wait.UntilCR(waitCtx, env.Namespace,
				"status.dragonfly.upgrade.phase==Succeeded",
				func(mfg *v1alpha1.MysqlFailoverGroup) (bool, string, error) {
					if mfg.Status.Dragonfly == nil || mfg.Status.Dragonfly.Upgrade == nil {
						return false, "no status.dragonfly.upgrade", nil
					}
					up := mfg.Status.Dragonfly.Upgrade
					msg := fmt.Sprintf("phase=%q reason=%q msg=%q", up.Phase, up.Reason, up.Message)
					if up.Phase == v1alpha1.DragonflyUpgradePhaseFailed {
						return false, msg, fmt.Errorf("Dragonfly upgrade failed: %s", up.Message)
					}
					return up.Phase == v1alpha1.DragonflyUpgradePhaseSucceeded, msg, nil
				},
			)
			return err
		},
	}
}

func verifyDragonflySnapshotUpgradeKeyRestored() runner.Step {
	return runner.Step{
		Phase: runner.PhaseVerify,
		Name:  "seed key restored after snapshot upgrade",
		Do: func(ctx context.Context, env *runner.Env) error {
			active := ctxFetch(env, "s29Active")
			expected := ctxFetch(env, "s29Value")
			cli, err := env.Dragonfly(active)
			if err != nil {
				return err
			}
			got, ok, err := cli.Get(ctx, "scenario29:counter")
			if err != nil {
				return err
			}
			if !ok || got != expected {
				return fmt.Errorf("scenario29:counter restored=%v value=%q want %q", ok, got, expected)
			}
			return nil
		},
	}
}

func verifyDragonflySnapshotUpgradeImages() runner.Step {
	return runner.Step{
		Phase: runner.PhaseVerify,
		Name:  "all dragonfly StatefulSets use target image",
		Do: func(ctx context.Context, env *runner.Env) error {
			mfg, err := env.Kube.GetMFG(ctx, env.Namespace)
			if err != nil {
				return err
			}
			want := ctxFetch(env, "s29TargetImage")
			for _, site := range mfg.Spec.Sites {
				sts, err := env.Kube.Kubernetes.AppsV1().StatefulSets(env.Namespace).Get(ctx, pgkube.DragonflyStatefulSetName(env.FG, site.Name), metav1.GetOptions{})
				if err != nil {
					return err
				}
				if len(sts.Spec.Template.Spec.Containers) == 0 {
					return fmt.Errorf("site %s StatefulSet has no containers", site.Name)
				}
				if got := sts.Spec.Template.Spec.Containers[0].Image; got != want {
					return fmt.Errorf("site %s image=%q want %q", site.Name, got, want)
				}
			}
			return nil
		},
	}
}
