package scenarios

import (
	"context"
	"fmt"
	"time"

	v1alpha1 "github.com/shipstream/bloodraven/api/v1alpha1"
	pglogs "github.com/shipstream/bloodraven/internal/playground/logs"
	pgmetrics "github.com/shipstream/bloodraven/internal/playground/metrics"
	"github.com/shipstream/bloodraven/internal/playground/runner"
)

func init() {
	runner.Register(scenario01CleanPrimaryKill())
}

// scenario01CleanPrimaryKill scales the active primary's MySQL
// deployment to 0 (per chaos-scenarios.md:13 — pod-delete races the
// Deployment respawn), waits for failover, then asserts the standard
// signals: status.activeSite flipped, failovers_total incremented,
// "failover complete" log emitted.
//
// Recovery of the old primary is intentionally NOT asserted here —
// that's scenario 12's job. See plan §10 risk #4.
func scenario01CleanPrimaryKill() runner.Scenario {
	return runner.Scenario{
		ID:    "01-clean-primary-kill",
		Title: "Clean primary kill — failover to peer",
		Hypothesis: "Scaling the active primary deployment to 0 triggers an emergency failover within ~45s. " +
			"Status.activeSite flips, bloodraven_failovers_total increments, and 'failover complete' is logged.",
		Risk:     "low",
		DocLink:  "playground/chaos-scenarios.md#1-clean-primary-failure",
		Timeout:  3 * time.Minute,
		Precheck: AssertHealthyBaseline,
		Steps: []runner.Step{
			injectScaleZero(),
			observeFailover(),
			verifyFailoverMetric(),
			verifyFailoverLog(),
		},
	}
}

func injectScaleZero() runner.Step {
	return runner.Step{
		Phase: runner.PhaseInject,
		Name:  "scale active primary to 0 replicas",
		Do: func(ctx context.Context, env *runner.Env) error {
			mfg, err := env.Kube.GetMFG(ctx, env.Namespace)
			if err != nil {
				return err
			}
			active := mfg.Status.ActiveSite
			peer, err := PeerOf(mfg, active)
			if err != nil {
				return err
			}
			env.Capture.Note(fmt.Sprintf("active primary at start: %s", active))
			if err := ctxStash(ctx, env, "expectedNewPrimary", peer); err != nil {
				return err
			}
			if err := stashMetricCounter(ctx, env, "failoversBefore", "bloodraven_failovers_total", map[string]string{"target_site": peer}); err != nil {
				return err
			}
			env.Logger.Info("injecting", "action", "scale=0", "site", active)
			if err := env.Chaos.ScaleSiteToZero(ctx, active); err != nil {
				return err
			}
			// Stash for later steps.
			env.Capture.Note("primary scaled to 0; waiting for failover")
			return ctxStash(ctx, env, "originalPrimary", active)
		},
	}
}

func observeFailover() runner.Step {
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
					msg := fmt.Sprintf("activeSite=%q lastFailoverTarget=%q", mfg.Status.ActiveSite, mfg.Status.LastFailoverTarget)
					if mfg.Status.ActiveSite != "" && mfg.Status.ActiveSite != original {
						return true, msg, nil
					}
					return false, msg, nil
				},
			)
			return err
		},
	}
}

func verifyFailoverMetric() runner.Step {
	return runner.Step{
		Phase: runner.PhaseVerify,
		Name:  "bloodraven_failovers_total increments for new primary",
		Do: func(ctx context.Context, env *runner.Env) error {
			mfg, err := env.Kube.GetMFG(ctx, env.Namespace)
			if err != nil {
				return err
			}
			newPrimary := mfg.Status.ActiveSite
			expected := ctxFetch(env, "expectedNewPrimary")
			if newPrimary != expected {
				return fmt.Errorf("activeSite=%q, want failover target %q", newPrimary, expected)
			}
			before, err := fetchStashedFloat(env, "failoversBefore")
			if err != nil {
				return err
			}
			waitCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
			defer cancel()
			return env.Wait.UntilMetric(waitCtx, env.Metrics,
				fmt.Sprintf("bloodraven_failovers_total{target_site=%q} increments from %g", newPrimary, before),
				func(snap *pgmetrics.Snapshot) (bool, string) {
					v, _ := snap.Counter("bloodraven_failovers_total", map[string]string{"target_site": newPrimary})
					return v > before, fmt.Sprintf("counter=%g before=%g delta=%g", v, before, v-before)
				},
			)
		},
	}
}

func verifyFailoverLog() runner.Step {
	return runner.Step{
		Phase: runner.PhaseVerify,
		Name:  `"failover complete" line appears in operator log`,
		Do: func(ctx context.Context, env *runner.Env) error {
			tail, err := env.Logs("operator")
			if err != nil {
				return err
			}
			waitCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
			defer cancel()
			_, err = env.Wait.UntilLog(waitCtx, tail, env.StartTime, `"failover complete"`,
				pglogs.Substring(`failover complete`))
			return err
		},
	}
}

// ctxStash / ctxFetch are tiny per-scenario state helpers. They live
// on the Capture's notes for forensic purposes and on a small in-memory
// map keyed by the env pointer. All map access is performed under the
// stashMu lock so concurrent scenario goroutines (or the race detector)
// see consistent state.
func ctxStash(_ context.Context, env *runner.Env, key, value string) error {
	stashSet(env, key, value)
	env.Capture.Note(fmt.Sprintf("stash %s=%s", key, value))
	return nil
}

func ctxFetch(env *runner.Env, key string) string {
	return stashGet(env, key)
}
