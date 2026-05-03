package scenarios

import (
	"context"
	"fmt"
	"time"

	v1alpha1 "github.com/shipstream/bloodraven/api/v1alpha1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	pgkube "github.com/shipstream/bloodraven/internal/playground/kube"
	"github.com/shipstream/bloodraven/internal/playground/runner"
)

func init() {
	runner.Register(scenario20SharedNodeSelectorIsolation())
}

const (
	s20InventoryGroup         = "inventory"
	s20InventoryGroupLabel    = "shipstream.io/failover-group.inventory"
	s20InventorySiteLabel     = "shipstream.io/site.inventory"
	s20PlaygroundReadOnly     = "shipstream.io/db-readonly-playground"
	s20InventoryReadOnly      = "shipstream.io/db-readonly-inventory"
	s20InventoryCanaryName    = "inventory-shared-node-canary"
	s20PostFailoverWaitWindow = 60 * time.Second
)

// scenario20SharedNodeSelectorIsolation locks down the per-failover-
// group taint scoping that lets one physical node serve multiple
// failover groups without their failovers spilling into each other's
// workloads. Concretely: a playground failover taints the playground
// site's node with `shipstream.io/db-readonly-playground:NoExecute`,
// and the operator must NOT also apply
// `shipstream.io/db-readonly-inventory:NoExecute` even though the same
// node also advertises membership in an `inventory` failover group.
//
// We simulate the multi-tenant deployment by labeling the active
// site's node with the inventory group + site labels (no inventory
// MysqlFailoverGroup CR is created — these labels are pure node
// metadata). A canary pod is created with a node selector for the
// inventory group and a toleration for the playground taint; if the
// operator's taint scoping is correct, the canary survives the
// playground failover unevicted.
func scenario20SharedNodeSelectorIsolation() runner.Scenario {
	return runner.Scenario{
		ID:    "20-shared-node-selector-isolation",
		Title: "Shared-node selector isolation: per-FG taint scope respects label keys",
		Hypothesis: "A node serving both `playground` and a fictional `inventory` failover group is " +
			"tainted ONLY with `shipstream.io/db-readonly-playground` after a playground failover; " +
			"`shipstream.io/db-readonly-inventory` is NOT applied; an inventory-tolerating canary stays Running.",
		Risk:     "low",
		DocLink:  "playground/chaos-scenarios.md#20-shared-node-selector-isolation",
		Timeout:  4 * time.Minute,
		Precheck: AssertHealthyBaseline,
		Steps: []runner.Step{
			s20InjectMultiTenantTopology(),
			s20InjectFailover(),
			s20ObserveActiveSiteFlip(),
			s20ObserveTaintApplied(),
			s20VerifyTaintScopedToPlayground(),
			s20VerifyCanaryStillRunning(),
		},
	}
}

func s20InjectMultiTenantTopology() runner.Step {
	return runner.Step{
		Phase: runner.PhaseInject,
		Name:  "label active-site node into a fake inventory group, deploy a tolerating canary",
		Do: func(ctx context.Context, env *runner.Env) error {
			mfg, err := env.Kube.GetMFGNamed(ctx, env.Namespace, env.FG)
			if err != nil {
				return err
			}
			active := mfg.Status.ActiveSite
			pod, err := env.Kube.GetSiteMysqlPod(ctx, env.Namespace, env.FG, active)
			if err != nil {
				return err
			}
			node := pod.Spec.NodeName
			if node == "" {
				return fmt.Errorf("active site %s pod %s has no node assignment", active, pod.Name)
			}
			env.Capture.Note(fmt.Sprintf("active=%s pod=%s node=%s", active, pod.Name, node))
			if err := ctxStash(ctx, env, "originalPrimary", active); err != nil {
				return err
			}
			if err := ctxStash(ctx, env, "primaryNode", node); err != nil {
				return err
			}

			labels := map[string]string{
				s20InventoryGroupLabel: "true",
				s20InventorySiteLabel:  active,
			}
			if err := env.Chaos.LabelNode(ctx, node, labels); err != nil {
				return err
			}

			canary := &corev1.Pod{
				ObjectMeta: metav1.ObjectMeta{
					Name:      s20InventoryCanaryName,
					Namespace: env.Namespace,
					Labels:    map[string]string{"app": s20InventoryCanaryName},
				},
				Spec: corev1.PodSpec{
					RestartPolicy: corev1.RestartPolicyNever,
					NodeSelector: map[string]string{
						s20InventoryGroupLabel: "true",
						s20InventorySiteLabel:  active,
					},
					Tolerations: []corev1.Toleration{{
						Key:      s20PlaygroundReadOnly,
						Operator: corev1.TolerationOpExists,
						Effect:   corev1.TaintEffectNoExecute,
					}},
					Containers: []corev1.Container{{
						Name:    "sleep",
						Image:   "busybox:1.36",
						Command: []string{"sh", "-c", "sleep 3600"},
					}},
				},
			}
			if err := env.Chaos.CreateCanaryPod(ctx, env.Namespace, canary); err != nil {
				return err
			}
			waitCtx, cancel := context.WithTimeout(ctx, 90*time.Second)
			defer cancel()
			return waitForPodReady(waitCtx, env, env.Namespace, s20InventoryCanaryName)
		},
	}
}

func s20InjectFailover() runner.Step {
	return runner.Step{
		Phase: runner.PhaseInject,
		Name:  "scale playground active site to 0 to trigger failover",
		Do: func(ctx context.Context, env *runner.Env) error {
			active := ctxFetch(env, "originalPrimary")
			return env.Chaos.ScaleSiteToZero(ctx, active)
		},
	}
}

func s20ObserveActiveSiteFlip() runner.Step {
	return runner.Step{
		Phase: runner.PhaseObserve,
		Name:  "wait for activeSite to flip away from the original",
		Do: func(ctx context.Context, env *runner.Env) error {
			original := ctxFetch(env, "originalPrimary")
			waitCtx, cancel := context.WithTimeout(ctx, 90*time.Second)
			defer cancel()
			_, err := env.Wait.UntilCR(waitCtx, env.Namespace,
				fmt.Sprintf("activeSite changes from %s", original),
				func(mfg *v1alpha1.MysqlFailoverGroup) (bool, string, error) {
					msg := fmt.Sprintf("activeSite=%q", mfg.Status.ActiveSite)
					return mfg.Status.ActiveSite != "" && mfg.Status.ActiveSite != original, msg, nil
				},
			)
			return err
		},
	}
}

// s20ObserveTaintApplied polls the node until the playground taint
// shows up (operator applies it asynchronously after the site state
// transitions to read-only). 60s is generous: the operator pushes
// taint deltas as part of the same reconcile that flips state.
func s20ObserveTaintApplied() runner.Step {
	return runner.Step{
		Phase: runner.PhaseObserve,
		Name:  "wait for playground taint to land on the old-primary node",
		Do: func(ctx context.Context, env *runner.Env) error {
			node := ctxFetch(env, "primaryNode")
			deadline := time.Now().Add(s20PostFailoverWaitWindow)
			tick := time.NewTicker(2 * time.Second)
			defer tick.Stop()
			var last string
			for {
				n, err := env.Kube.GetNode(ctx, node)
				if err == nil {
					var keys []string
					for _, t := range n.Spec.Taints {
						keys = append(keys, t.Key)
					}
					last = fmt.Sprintf("taintKeys=%v", keys)
					if pgkube.NodeHasTaint(n, s20PlaygroundReadOnly) {
						env.Capture.Note(fmt.Sprintf("playground taint applied on %s; %s", node, last))
						return nil
					}
				}
				if time.Now().After(deadline) {
					return fmt.Errorf("playground taint did not appear on %s within %s (last: %s)",
						node, s20PostFailoverWaitWindow, last)
				}
				select {
				case <-ctx.Done():
					return ctx.Err()
				case <-tick.C:
				}
			}
		},
	}
}

func s20VerifyTaintScopedToPlayground() runner.Step {
	return runner.Step{
		Phase: runner.PhaseVerify,
		Name:  "node has playground taint but NOT inventory taint",
		Do: func(ctx context.Context, env *runner.Env) error {
			node := ctxFetch(env, "primaryNode")
			n, err := env.Kube.GetNode(ctx, node)
			if err != nil {
				return err
			}
			if !pgkube.NodeHasTaint(n, s20PlaygroundReadOnly) {
				return fmt.Errorf("expected %s taint on %s, taints=%v", s20PlaygroundReadOnly, node, n.Spec.Taints)
			}
			if pgkube.NodeHasTaint(n, s20InventoryReadOnly) {
				return fmt.Errorf("operator leaked %s onto %s — taint scoping regression (taints=%v)",
					s20InventoryReadOnly, node, n.Spec.Taints)
			}
			env.Capture.Note(fmt.Sprintf("node %s correctly tainted only for playground", node))
			return nil
		},
	}
}

func s20VerifyCanaryStillRunning() runner.Step {
	return runner.Step{
		Phase: runner.PhaseVerify,
		Name:  "inventory canary is still Running on the same node",
		Do: func(ctx context.Context, env *runner.Env) error {
			node := ctxFetch(env, "primaryNode")
			pod, err := env.Kube.GetPod(ctx, env.Namespace, s20InventoryCanaryName)
			if err != nil {
				return fmt.Errorf("get canary pod: %w", err)
			}
			if pod.DeletionTimestamp != nil {
				return fmt.Errorf("canary pod has deletionTimestamp=%v — operator evicted an inventory pod over a playground taint",
					pod.DeletionTimestamp)
			}
			if pod.Status.Phase != corev1.PodRunning {
				return fmt.Errorf("canary pod phase=%q (want Running) — taint may have evicted it", pod.Status.Phase)
			}
			if pod.Spec.NodeName != node {
				return fmt.Errorf("canary pod on unexpected node: got %q, want %q", pod.Spec.NodeName, node)
			}
			env.Capture.Note(fmt.Sprintf("canary %s still Running on %s", s20InventoryCanaryName, node))
			return nil
		},
	}
}

// waitForPodReady polls until the named pod's Ready condition is True
// or the context expires. Pulled out of s20 because s21 needs the same
// helper.
func waitForPodReady(ctx context.Context, env *runner.Env, namespace, name string) error {
	tick := time.NewTicker(2 * time.Second)
	defer tick.Stop()
	var last string
	for {
		pod, err := env.Kube.GetPod(ctx, namespace, name)
		if err == nil {
			for _, c := range pod.Status.Conditions {
				if c.Type == corev1.PodReady && c.Status == corev1.ConditionTrue {
					return nil
				}
			}
			last = fmt.Sprintf("phase=%s", pod.Status.Phase)
		} else {
			last = "get pod failed: " + err.Error()
		}
		select {
		case <-ctx.Done():
			return fmt.Errorf("pod %s/%s did not become Ready (last: %s): %w", namespace, name, last, ctx.Err())
		case <-tick.C:
		}
	}
}
