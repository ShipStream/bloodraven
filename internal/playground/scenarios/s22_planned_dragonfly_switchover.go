package scenarios

import (
	"context"
	"fmt"
	"time"

	v1alpha1 "github.com/shipstream/bloodraven/api/v1alpha1"
	pgkube "github.com/shipstream/bloodraven/internal/playground/kube"
	pgmetrics "github.com/shipstream/bloodraven/internal/playground/metrics"
	"github.com/shipstream/bloodraven/internal/playground/runner"
)

func init() {
	runner.Register(scenario22PlannedDragonflySwitchover())
}

// scenario22PlannedDragonflySwitchover is the live-cluster validation
// for the slice-1/2/3 planned-failover Dragonfly work. It seeds a
// known counter on the active master, triggers a planned failover via
// annotation, waits for plannedFailover.phase=Succeeded, and then
// asserts:
//
//   - status.dragonfly.activeSite flipped to the failover target
//   - status.plannedFailover.dragonfly.PromotionMethod is "REPLTAKEOVER"
//   - status.plannedFailover.dragonfly.SessionsPreserved is true
//   - the seed counter is readable on the new master at the original
//     value (sessions preserved end-to-end via the active Service)
//   - the kube Endpoints object for the active Service has converged
//     to a single pod whose site matches the new active site
//   - bloodraven_dragonfly_promotions_total{result="success"} >= 1
//
// Mapped to PLANS-Dragonfly-Chaos-Scenarios.md scenario D3 plus the
// folded-in baseline assertions from D1/D2/D9.
//
// PLANS-Bloodraven-Dragonfly.md "Required Before Next Slice" called
// for a regression test asserting no `READONLY` mid-flight; the
// unit-level invariant is covered by
// TestPlannedFailoverPromotingDragonfly_NoReadOnlyMidFlight in
// internal/controller. This scenario adds the live-cluster
// confirmation that the labels we drive actually steer kube-proxy /
// Endpoints to the correct pod.
func scenario22PlannedDragonflySwitchover() runner.Scenario {
	return runner.Scenario{
		ID:    "22-planned-dragonfly-switchover",
		Title: "Planned MySQL+Dragonfly coordinated switchover preserves sessions",
		Hypothesis: "Annotating the MFG with bloodraven.shipstream.io/planned-failover=<peer> walks both MySQL " +
			"and Dragonfly through coordinated promotion: dragonfly.activeSite flips to the target, " +
			"PromotionMethod=REPLTAKEOVER, SessionsPreserved=true, the seed counter survives the failover " +
			"reachable through the active Service, and the active Service's Endpoints converge to the new master pod.",
		Risk:    "medium",
		DocLink: "PLANS-Dragonfly-Chaos-Scenarios.md (D3)",
		Timeout: 5 * time.Minute,
		Precheck: AssertHealthyBaseline,
		Steps: []runner.Step{
			seedDragonflyCounterOnActive(),
			injectPlannedFailoverForDragonfly(),
			observePlannedFailoverSucceeded(),
			verifyDragonflyActiveSiteFlipped(),
			verifyDragonflyPromotionMethodAndSessions(),
			verifyDragonflyCounterPreserved(),
			verifyDragonflyActiveEndpointsConverged(),
			verifyDragonflyPromotionsMetric(),
		},
	}
}

// seedDragonflyCounterOnActive writes a deterministic key (with the
// scenario start time as the value) on the active Dragonfly master via
// a per-site port-forward. Stashing the value lets the post-failover
// step assert exact session preservation rather than just "key
// exists".
func seedDragonflyCounterOnActive() runner.Step {
	return runner.Step{
		Phase: runner.PhaseInject,
		Name:  "seed dragonfly counter on active master",
		Do: func(ctx context.Context, env *runner.Env) error {
			mfg, err := env.Kube.GetMFG(ctx, env.Namespace)
			if err != nil {
				return err
			}
			active, err := dragonflyActiveSite(mfg)
			if err != nil {
				return err
			}
			if err := ctxStash(ctx, env, "dfActiveBefore", active); err != nil {
				return err
			}
			peer, err := PeerOf(mfg, mfg.Status.ActiveSite)
			if err != nil {
				return err
			}
			if err := ctxStash(ctx, env, "switchoverTarget", peer); err != nil {
				return err
			}
			cli, err := env.Dragonfly(active)
			if err != nil {
				return fmt.Errorf("open dragonfly on %s: %w", active, err)
			}
			val := fmt.Sprintf("scenario22-%d", env.StartTime.UnixNano())
			if _, err := cli.Set(ctx, "scenario22:counter", val); err != nil {
				return fmt.Errorf("SET on %s: %w", active, err)
			}
			env.Capture.Note(fmt.Sprintf("seeded dragonfly key on %s value=%s", active, val))
			return ctxStash(ctx, env, "dfSeedValue", val)
		},
	}
}

// injectPlannedFailoverForDragonfly sets the annotation that triggers
// a coordinated MySQL+Dragonfly switchover. Reuses the chaos action
// from s02_planned_switchover; the reverter clears the annotation on
// scenario teardown (no-op once the failover succeeded).
func injectPlannedFailoverForDragonfly() runner.Step {
	return runner.Step{
		Phase: runner.PhaseInject,
		Name:  "annotate planned-failover with peer site",
		Do: func(ctx context.Context, env *runner.Env) error {
			target := ctxFetch(env, "switchoverTarget")
			if target == "" {
				return fmt.Errorf("switchoverTarget not stashed")
			}
			env.Capture.Note(fmt.Sprintf("planned switchover target: %s", target))
			return env.Chaos.AnnotatePlannedFailover(ctx, target)
		},
	}
}

func verifyDragonflyActiveSiteFlipped() runner.Step {
	return runner.Step{
		Phase: runner.PhaseVerify,
		Name:  "status.dragonfly.activeSite flipped to target",
		Do: func(ctx context.Context, env *runner.Env) error {
			target := ctxFetch(env, "switchoverTarget")
			waitCtx, cancel := context.WithTimeout(ctx, 90*time.Second)
			defer cancel()
			_, err := env.Wait.UntilCR(waitCtx, env.Namespace,
				fmt.Sprintf("status.dragonfly.activeSite==%s", target),
				func(mfg *v1alpha1.MysqlFailoverGroup) (bool, string, error) {
					if mfg.Status.Dragonfly == nil {
						return false, "no status.dragonfly yet", nil
					}
					site := mfg.Status.Dragonfly.ActiveSite
					return site == target,
						fmt.Sprintf("status.dragonfly.activeSite=%q phase=%q", site, mfg.Status.Dragonfly.Phase),
						nil
				},
			)
			return err
		},
	}
}

func verifyDragonflyPromotionMethodAndSessions() runner.Step {
	return runner.Step{
		Phase: runner.PhaseVerify,
		Name:  "plannedFailover.dragonfly.PromotionMethod=REPLTAKEOVER, SessionsPreserved=true",
		Do: func(ctx context.Context, env *runner.Env) error {
			mfg, err := env.Kube.GetMFG(ctx, env.Namespace)
			if err != nil {
				return err
			}
			pf := mfg.Status.PlannedFailover
			if pf == nil || pf.Dragonfly == nil {
				return fmt.Errorf("status.plannedFailover.dragonfly missing on Succeeded run")
			}
			if pf.Dragonfly.PromotionMethod != "REPLTAKEOVER" {
				return fmt.Errorf("PromotionMethod=%q want REPLTAKEOVER (reason=%q msg=%q)",
					pf.Dragonfly.PromotionMethod, pf.Dragonfly.Reason, pf.Dragonfly.Message)
			}
			if pf.Dragonfly.SessionsPreserved == nil || !*pf.Dragonfly.SessionsPreserved {
				return fmt.Errorf("SessionsPreserved=%v want true (reason=%q msg=%q)",
					pf.Dragonfly.SessionsPreserved, pf.Dragonfly.Reason, pf.Dragonfly.Message)
			}
			return nil
		},
	}
}

func verifyDragonflyCounterPreserved() runner.Step {
	return runner.Step{
		Phase: runner.PhaseVerify,
		Name:  "seed counter is readable on the new master at the original value",
		Do: func(ctx context.Context, env *runner.Env) error {
			target := ctxFetch(env, "switchoverTarget")
			expected := ctxFetch(env, "dfSeedValue")
			if expected == "" {
				return fmt.Errorf("dfSeedValue not stashed")
			}
			cli, err := env.Dragonfly(target)
			if err != nil {
				return fmt.Errorf("open dragonfly on new master %s: %w", target, err)
			}
			got, ok, err := cli.Get(ctx, "scenario22:counter")
			if err != nil {
				return fmt.Errorf("GET on new master: %w", err)
			}
			if !ok {
				return fmt.Errorf("scenario22:counter missing on new master %s — sessions LOST despite SessionsPreserved=true", target)
			}
			if got != expected {
				return fmt.Errorf("scenario22:counter=%q want %q on new master %s", got, expected, target)
			}
			env.Capture.Note(fmt.Sprintf("session preserved across switchover: %q on %s", got, target))
			return nil
		},
	}
}

// verifyDragonflyActiveEndpointsConverged proves the labels-driven
// active Service selector flip lands at the kube-proxy /Endpoints
// layer — not just on the CR. After Succeeded the Endpoints set must
// be exactly {<target> pod}; the CLIENT KILL in step 4 of
// plannedFailoverPromotingDragonfly is best-effort, so a brief stall
// is tolerated by polling for up to 30s.
func verifyDragonflyActiveEndpointsConverged() runner.Step {
	return runner.Step{
		Phase: runner.PhaseVerify,
		Name:  "active Service endpoints converged to new master pod",
		Do: func(ctx context.Context, env *runner.Env) error {
			target := ctxFetch(env, "switchoverTarget")
			deadline := time.Now().Add(30 * time.Second)
			expectedPodName := pgkube.DragonflyStatefulSetName(env.FG, target) + "-0"
			var last []string
			for {
				pods, err := env.Kube.DragonflyActiveServiceEndpointPods(ctx, env.Namespace, env.FG)
				if err != nil {
					return err
				}
				last = pods
				if len(pods) == 1 && pods[0] == expectedPodName {
					return nil
				}
				if time.Now().After(deadline) {
					return fmt.Errorf("active Service endpoints did not converge to {%s}: last observed %v",
						expectedPodName, last)
				}
				select {
				case <-ctx.Done():
					return ctx.Err()
				case <-time.After(500 * time.Millisecond):
				}
			}
		},
	}
}

func verifyDragonflyPromotionsMetric() runner.Step {
	return runner.Step{
		Phase: runner.PhaseVerify,
		Name:  `bloodraven_dragonfly_promotions_total{result="success"} >= 1`,
		Do: func(ctx context.Context, env *runner.Env) error {
			target := ctxFetch(env, "switchoverTarget")
			waitCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
			defer cancel()
			return env.Wait.UntilMetric(waitCtx, env.Metrics,
				fmt.Sprintf(`dragonfly_promotions_total{target_site=%q,result="success"} >= 1`, target),
				func(snap *pgmetrics.Snapshot) (bool, string) {
					v, _ := snap.Counter("bloodraven_dragonfly_promotions_total", map[string]string{
						"target_site": target,
						"result":      "success",
					})
					return v >= 1, fmt.Sprintf("counter=%g", v)
				},
			)
		},
	}
}

// dragonflyActiveSite returns the Dragonfly master site name from
// status, falling back to status.activeSite for the very first
// reconcile race window when status.dragonfly.activeSite hasn't been
// populated yet.
func dragonflyActiveSite(mfg *v1alpha1.MysqlFailoverGroup) (string, error) {
	if mfg.Status.Dragonfly != nil && mfg.Status.Dragonfly.ActiveSite != "" {
		return mfg.Status.Dragonfly.ActiveSite, nil
	}
	if mfg.Status.ActiveSite != "" {
		return mfg.Status.ActiveSite, nil
	}
	return "", fmt.Errorf("no active site to seed Dragonfly counter on")
}
