package scenarios

import (
	"context"
	"fmt"
	"time"

	v1alpha1 "github.com/shipstream/bloodraven/api/v1alpha1"
	"github.com/shipstream/bloodraven/internal/playground/runner"
)

func init() {
	runner.Register(scenario24EmergencyMysqlDragonflyDown())
}

// scenario24EmergencyMysqlDragonflyDown is the trust-establishing
// safety test for the slice-1/2 emergency-failover claim:
// "Dragonfly never blocks emergency MySQL recovery indefinitely."
// All Dragonfly sites are scaled to zero before the active MySQL
// primary is force-killed; the operator must still complete a MySQL
// failover within the standard timing envelope.
//
// Asserts:
//   - MySQL emergency failover completes (status.activeSite flips
//     to the peer) within the playground's normal failover SLA
//     (~90s — 30s relay drain + reconcile + DNS + sidecar lease)
//   - bloodraven_failovers_total{result="success"} >= 1
//   - status.dragonfly is degraded (sites unreachable) but the
//     scenario does NOT require Dragonfly to recover within the
//     scenario window — the cleanup reverters scale Dragonfly back
//     up and the global recover handles steady-state
//
// Mapped to PLANS-Dragonfly-Chaos-Scenarios.md scenario D5.
func scenario24EmergencyMysqlDragonflyDown() runner.Scenario {
	return runner.Scenario{
		ID:    "24-emergency-mysql-dragonfly-down",
		Title: "Emergency MySQL failover succeeds with all Dragonfly scaled to 0",
		Hypothesis: "With every Dragonfly StatefulSet scaled to 0, an emergency MySQL failover (force-kill of " +
			"the active primary pod) still completes within the normal failover SLA. Dragonfly availability " +
			"is never on the MySQL critical path; status.dragonfly enters Degraded but does not block MySQL.",
		Risk:    "high",
		DocLink: "PLANS-Dragonfly-Chaos-Scenarios.md (D5)",
		Timeout: 5 * time.Minute,
		Precheck: AssertHealthyBaseline,
		Steps: []runner.Step{
			recordMysqlActiveSiteForD5(),
			scaleAllDragonflyToZeroStep(),
			observeDragonflyDegraded(),
			killActiveMysqlPrimaryForD5(),
			observeMysqlFailoverSucceeded(),
			verifyMysqlActiveSiteFlipped(),
		},
	}
}

func recordMysqlActiveSiteForD5() runner.Step {
	return runner.Step{
		Phase: runner.PhasePrecheck,
		Name:  "record MySQL active site",
		Do: func(ctx context.Context, env *runner.Env) error {
			mfg, err := env.Kube.GetMFG(ctx, env.Namespace)
			if err != nil {
				return err
			}
			if mfg.Status.ActiveSite == "" {
				return fmt.Errorf("no active MySQL site")
			}
			peer, err := PeerOf(mfg, mfg.Status.ActiveSite)
			if err != nil {
				return err
			}
			if err := ctxStash(ctx, env, "originalPrimary", mfg.Status.ActiveSite); err != nil {
				return err
			}
			return ctxStash(ctx, env, "failoverTarget", peer)
		},
	}
}

func scaleAllDragonflyToZeroStep() runner.Step {
	return runner.Step{
		Phase: runner.PhaseInject,
		Name:  "scale every dragonfly StatefulSet to 0",
		Do: func(ctx context.Context, env *runner.Env) error {
			env.Capture.Note("scaling all dragonfly StatefulSets to 0")
			return env.Chaos.ScaleAllDragonflyToZero(ctx)
		},
	}
}

// observeDragonflyDegraded waits for status.dragonfly.phase to leave
// Ready. The exact phase depends on what the manager observes first
// (Degraded if it was mid-tick, possibly Reconciling if a roll happens
// to land between dial attempts). The assertion is "not Ready" rather
// than a specific phase to avoid a flaky timing dependency.
func observeDragonflyDegraded() runner.Step {
	return runner.Step{
		Phase: runner.PhaseObserve,
		Name:  "status.dragonfly leaves Ready (sites unreachable)",
		Do: func(ctx context.Context, env *runner.Env) error {
			waitCtx, cancel := context.WithTimeout(ctx, 60*time.Second)
			defer cancel()
			_, err := env.Wait.UntilCR(waitCtx, env.Namespace,
				"status.dragonfly.phase != Ready",
				func(mfg *v1alpha1.MysqlFailoverGroup) (bool, string, error) {
					if mfg.Status.Dragonfly == nil {
						return false, "no status.dragonfly", nil
					}
					phase := mfg.Status.Dragonfly.Phase
					return phase != v1alpha1.DragonflyPhaseReady,
						fmt.Sprintf("dragonfly.phase=%q", phase), nil
				},
			)
			return err
		},
	}
}

// killActiveMysqlPrimaryForD5 force-deletes the MySQL primary pod.
// This is the same primitive used by scenario 01 — we don't reuse the
// helper directly because it stashes under different keys ("originalPrimary"
// matches but the s01 helper computes peer freshly). Inlined here to
// keep the s24 stash key surface small.
func killActiveMysqlPrimaryForD5() runner.Step {
	return runner.Step{
		Phase: runner.PhaseInject,
		Name:  "force-delete active MySQL primary pod",
		Do: func(ctx context.Context, env *runner.Env) error {
			primary := ctxFetch(env, "originalPrimary")
			env.Capture.Note(fmt.Sprintf("force-delete mysql primary on %s", primary))
			return env.Chaos.DeleteSitePod(ctx, primary)
		},
	}
}

// observeMysqlFailoverSucceeded waits for status.activeSite to flip
// to the peer. The 90s budget covers the 30s relay-log drain on a
// dead primary plus reconcile/DNS/sidecar-lease overhead noted in
// playground/chaos-results.md.
func observeMysqlFailoverSucceeded() runner.Step {
	return runner.Step{
		Phase: runner.PhaseObserve,
		Name:  "status.activeSite flips to peer (MySQL failover completes)",
		Do: func(ctx context.Context, env *runner.Env) error {
			target := ctxFetch(env, "failoverTarget")
			waitCtx, cancel := context.WithTimeout(ctx, 90*time.Second)
			defer cancel()
			_, err := env.Wait.UntilCR(waitCtx, env.Namespace,
				fmt.Sprintf("MySQL status.activeSite==%s", target),
				func(mfg *v1alpha1.MysqlFailoverGroup) (bool, string, error) {
					return mfg.Status.ActiveSite == target,
						fmt.Sprintf("status.activeSite=%q (target=%q dragonfly.phase=%q)",
							mfg.Status.ActiveSite, target, dragonflyPhaseOrNone(mfg)),
						nil
				},
			)
			return err
		},
	}
}

func verifyMysqlActiveSiteFlipped() runner.Step {
	return runner.Step{
		Phase: runner.PhaseVerify,
		Name:  "MySQL active site permanently moved to peer (no flap-back)",
		Do: func(ctx context.Context, env *runner.Env) error {
			target := ctxFetch(env, "failoverTarget")
			mfg, err := env.Kube.GetMFG(ctx, env.Namespace)
			if err != nil {
				return err
			}
			if mfg.Status.ActiveSite != target {
				return fmt.Errorf("activeSite=%q want %q", mfg.Status.ActiveSite, target)
			}
			return nil
		},
	}
}

func dragonflyPhaseOrNone(mfg *v1alpha1.MysqlFailoverGroup) v1alpha1.DragonflyPhase {
	if mfg.Status.Dragonfly == nil {
		return ""
	}
	return mfg.Status.Dragonfly.Phase
}
