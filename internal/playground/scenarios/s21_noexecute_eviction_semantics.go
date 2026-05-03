package scenarios

import (
	"context"
	"fmt"
	"time"

	v1alpha1 "github.com/shipstream/bloodraven/api/v1alpha1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	pgkube "github.com/shipstream/bloodraven/internal/playground/kube"
	"github.com/shipstream/bloodraven/internal/playground/runner"
)

func init() {
	runner.Register(scenario21NoExecuteEvictionSemantics())
}

const (
	s21EvictCanary             = "noexecute-evict-canary"
	s21TolerateCanary          = "noexecute-tolerate-canary"
	s21PlaygroundGroupLabel    = "shipstream.io/failover-group.playground"
	s21PlaygroundSiteLabel     = "shipstream.io/site.playground"
	s21EvictionObservationTime = 90 * time.Second
)

// scenario21NoExecuteEvictionSemantics asserts the eviction contract
// of the playground site's NoExecute taint: pods with the matching
// toleration survive a failover; pods without it are evicted by the
// kubelet. This pins down the operator's promise that the
// `db-readonly-<fg>:NoExecute` taint is the *only* mechanism gating
// non-MySQL workload placement on a read-only site, which lets
// downstream teams design tolerations against a stable contract.
//
// Two canaries are deployed on the active site's node before the
// chaos: one tolerating, one not. After the failover, the verify
// steps assert the tolerating canary is still Running and the
// non-tolerating canary has been deletion-marked or removed entirely
// by the eviction controller.
func scenario21NoExecuteEvictionSemantics() runner.Scenario {
	return runner.Scenario{
		ID:    "21-noexecute-eviction-semantics",
		Title: "NoExecute eviction semantics: tolerating pods survive, others are evicted",
		Hypothesis: "After a playground failover, the old-primary node's `db-readonly-playground:NoExecute` " +
			"taint evicts a non-tolerating canary (DeletionTimestamp set or pod gone) while a canary that " +
			"tolerates the same taint stays Running.",
		Risk:     "low",
		DocLink:  "playground/chaos-scenarios.md#21-noexecute-eviction-semantics",
		Timeout:  4 * time.Minute,
		Precheck: AssertHealthyBaseline,
		Steps: []runner.Step{
			s21InjectCanaries(),
			s21InjectFailover(),
			s21ObserveActiveSiteFlip(),
			s21ObserveTaintApplied(),
			s21VerifyNonTolerantEvicted(),
			s21VerifyTolerantSurvives(),
		},
	}
}

func s21InjectCanaries() runner.Step {
	return runner.Step{
		Phase: runner.PhaseInject,
		Name:  "deploy non-tolerating + tolerating canaries on the active site's node",
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
			env.Capture.Note(fmt.Sprintf("active=%s node=%s", active, node))
			if err := ctxStash(ctx, env, "originalPrimary", active); err != nil {
				return err
			}
			if err := ctxStash(ctx, env, "primaryNode", node); err != nil {
				return err
			}

			selector := map[string]string{
				s21PlaygroundGroupLabel: "true",
				s21PlaygroundSiteLabel:  active,
			}
			evictPod := s21CanaryPod(env.Namespace, s21EvictCanary, selector, nil)
			if err := env.Chaos.CreateCanaryPod(ctx, env.Namespace, evictPod); err != nil {
				return err
			}
			tolerantPod := s21CanaryPod(env.Namespace, s21TolerateCanary, selector, []corev1.Toleration{{
				Key:      s20PlaygroundReadOnly,
				Operator: corev1.TolerationOpExists,
				Effect:   corev1.TaintEffectNoExecute,
			}})
			if err := env.Chaos.CreateCanaryPod(ctx, env.Namespace, tolerantPod); err != nil {
				return err
			}
			waitCtx, cancel := context.WithTimeout(ctx, 90*time.Second)
			defer cancel()
			if err := waitForPodReady(waitCtx, env, env.Namespace, s21EvictCanary); err != nil {
				return fmt.Errorf("evict canary not Ready: %w", err)
			}
			if err := waitForPodReady(waitCtx, env, env.Namespace, s21TolerateCanary); err != nil {
				return fmt.Errorf("tolerate canary not Ready: %w", err)
			}
			return nil
		},
	}
}

func s21InjectFailover() runner.Step {
	return runner.Step{
		Phase: runner.PhaseInject,
		Name:  "scale playground active site to 0 to trigger failover",
		Do: func(ctx context.Context, env *runner.Env) error {
			active := ctxFetch(env, "originalPrimary")
			return env.Chaos.ScaleSiteToZero(ctx, active)
		},
	}
}

func s21ObserveActiveSiteFlip() runner.Step {
	return runner.Step{
		Phase: runner.PhaseObserve,
		Name:  "wait for activeSite to flip",
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

func s21ObserveTaintApplied() runner.Step {
	return runner.Step{
		Phase: runner.PhaseObserve,
		Name:  "wait for the playground taint to land on the old-primary node",
		Do: func(ctx context.Context, env *runner.Env) error {
			node := ctxFetch(env, "primaryNode")
			deadline := time.Now().Add(60 * time.Second)
			tick := time.NewTicker(2 * time.Second)
			defer tick.Stop()
			var last string
			for {
				n, err := env.Kube.GetNode(ctx, node)
				if err == nil {
					last = fmt.Sprintf("taintCount=%d", len(n.Spec.Taints))
					if pgkube.NodeHasTaint(n, s20PlaygroundReadOnly) {
						env.Capture.Note(fmt.Sprintf("playground taint applied on %s", node))
						return nil
					}
				}
				if time.Now().After(deadline) {
					return fmt.Errorf("playground taint did not appear on %s within 60s (last: %s)", node, last)
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

// s21VerifyNonTolerantEvicted polls until the non-tolerating canary is
// either marked for deletion (DeletionTimestamp set) or has fully
// disappeared. Eviction is asynchronous: the taint controller marks
// the pod, the kubelet honors the grace period, the pod terminates,
// the apiserver garbage-collects. Either intermediate or terminal
// state is a pass.
func s21VerifyNonTolerantEvicted() runner.Step {
	return runner.Step{
		Phase: runner.PhaseVerify,
		Name:  "non-tolerating canary is deletion-marked or already gone",
		Do: func(ctx context.Context, env *runner.Env) error {
			deadline := time.Now().Add(s21EvictionObservationTime)
			tick := time.NewTicker(2 * time.Second)
			defer tick.Stop()
			var last string
			for {
				pod, err := env.Kube.GetPod(ctx, env.Namespace, s21EvictCanary)
				switch {
				case apierrors.IsNotFound(err):
					env.Capture.Note(fmt.Sprintf("%s evicted (NotFound)", s21EvictCanary))
					return nil
				case err != nil:
					last = "get pod failed: " + err.Error()
				default:
					if pod.DeletionTimestamp != nil {
						env.Capture.Note(fmt.Sprintf("%s deletion-marked at %s", s21EvictCanary, pod.DeletionTimestamp))
						return nil
					}
					last = fmt.Sprintf("phase=%s deletionTimestamp=<nil>", pod.Status.Phase)
				}
				if time.Now().After(deadline) {
					return fmt.Errorf("%s was not evicted within %s (last: %s) — NoExecute taint enforcement broken",
						s21EvictCanary, s21EvictionObservationTime, last)
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

func s21VerifyTolerantSurvives() runner.Step {
	return runner.Step{
		Phase: runner.PhaseVerify,
		Name:  "tolerating canary remains Running on the same node",
		Do: func(ctx context.Context, env *runner.Env) error {
			node := ctxFetch(env, "primaryNode")
			pod, err := env.Kube.GetPod(ctx, env.Namespace, s21TolerateCanary)
			if err != nil {
				return fmt.Errorf("get tolerating canary: %w", err)
			}
			if pod.DeletionTimestamp != nil {
				return fmt.Errorf("tolerating canary deletion-marked at %s — taint scoping or toleration matching regression",
					pod.DeletionTimestamp)
			}
			if pod.Status.Phase != corev1.PodRunning {
				return fmt.Errorf("tolerating canary phase=%q (want Running) — eviction overreach", pod.Status.Phase)
			}
			if pod.Spec.NodeName != node {
				return fmt.Errorf("tolerating canary on unexpected node: got %q, want %q", pod.Spec.NodeName, node)
			}
			env.Capture.Note(fmt.Sprintf("%s still Running on %s", s21TolerateCanary, node))
			return nil
		},
	}
}

func s21CanaryPod(namespace, name string, selector map[string]string, tolerations []corev1.Toleration) *corev1.Pod {
	return &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      name,
			Namespace: namespace,
			Labels:    map[string]string{"app": name},
		},
		Spec: corev1.PodSpec{
			RestartPolicy: corev1.RestartPolicyNever,
			NodeSelector:  selector,
			Tolerations:   tolerations,
			Containers: []corev1.Container{{
				Name:    "sleep",
				Image:   "busybox:1.36",
				Command: []string{"sh", "-c", "sleep 3600"},
			}},
		},
	}
}
