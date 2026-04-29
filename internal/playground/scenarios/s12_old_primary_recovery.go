package scenarios

import (
	"context"
	"fmt"
	"time"

	v1alpha1 "github.com/shipstream/bloodraven/api/v1alpha1"
	pglogs "github.com/shipstream/bloodraven/internal/playground/logs"
	"github.com/shipstream/bloodraven/internal/playground/runner"
)

func init() {
	runner.Register(scenario12OldPrimaryRecovery())
}

// scenario12OldPrimaryRecovery scales the active primary down, waits
// for failover, scales it back up, and asserts the operator's
// "no GTID divergence, auto-recovering old primary as replica" path:
//   1. The old primary's site state returns to "read-only".
//   2. Replication is configured and running on the old primary.
func scenario12OldPrimaryRecovery() runner.Scenario {
	return runner.Scenario{
		ID:    "12-old-primary-recovery-no-divergence",
		Title: "Old primary recovers without divergence after failover",
		Hypothesis: "After a clean failover, scaling the old primary back up triggers " +
			"'no GTID divergence, auto-recovering old primary as replica' and the site rejoins as a replica.",
		Risk:    "low",
		DocLink: "playground/chaos-scenarios.md#12-old-primary-recovery-no-divergence",
		Timeout: 5 * time.Minute,
		Precheck: AssertHealthyBaseline,
		Steps: []runner.Step{
			injectScaleZeroStash(),
			observeFailoverFlip(),
			injectScaleBackUp(),
			observeOldPrimaryReadOnly(),
			verifyAutoRecoveryLog(),
			verifyReplicationRunningOnOldPrimary(),
		},
	}
}

func injectScaleZeroStash() runner.Step {
	return runner.Step{
		Phase: runner.PhaseInject,
		Name:  "scale active primary to 0 and remember it",
		Do: func(ctx context.Context, env *runner.Env) error {
			mfg, err := env.Kube.GetMFG(ctx, env.Namespace)
			if err != nil {
				return err
			}
			active := mfg.Status.ActiveSite
			env.Capture.Note(fmt.Sprintf("active primary at start: %s", active))
			if err := ctxStash(ctx, env, "originalPrimary", active); err != nil {
				return err
			}
			// Open the operator log tailer NOW so we have visibility
			// from before the inject.
			if _, err := env.Logs("operator"); err != nil {
				return err
			}
			return env.Chaos.ScaleSiteToZero(ctx, active)
		},
	}
}

func observeFailoverFlip() runner.Step {
	return runner.Step{
		Phase: runner.PhaseObserve,
		Name:  "wait for activeSite flip",
		Do: func(ctx context.Context, env *runner.Env) error {
			original := ctxFetch(env, "originalPrimary")
			waitCtx, cancel := context.WithTimeout(ctx, 90*time.Second)
			defer cancel()
			_, err := env.Wait.UntilCR(waitCtx, env.Namespace,
				"activeSite changes",
				func(mfg *v1alpha1.MysqlFailoverGroup) (bool, string, error) {
					msg := fmt.Sprintf("activeSite=%q", mfg.Status.ActiveSite)
					return mfg.Status.ActiveSite != "" && mfg.Status.ActiveSite != original, msg, nil
				},
			)
			return err
		},
	}
}

func injectScaleBackUp() runner.Step {
	return runner.Step{
		Phase: runner.PhaseInject,
		Name:  "scale old primary back to 1",
		Do: func(ctx context.Context, env *runner.Env) error {
			// Replay the reverter manually so the old primary comes
			// back up while still in scenario scope.
			if err := env.Chaos.Revert(ctx); err != nil {
				return err
			}
			env.Capture.Note("old primary scaled back to 1; waiting for recovery")
			return nil
		},
	}
}

func observeOldPrimaryReadOnly() runner.Step {
	return runner.Step{
		Phase: runner.PhaseObserve,
		Name:  "old primary site returns to read-only state",
		Do: func(ctx context.Context, env *runner.Env) error {
			original := ctxFetch(env, "originalPrimary")
			waitCtx, cancel := context.WithTimeout(ctx, 3*time.Minute)
			defer cancel()
			_, err := env.Wait.UntilCR(waitCtx, env.Namespace,
				fmt.Sprintf("site %s state==read-only", original),
				func(mfg *v1alpha1.MysqlFailoverGroup) (bool, string, error) {
					st := stateOf(mfg, original)
					return st == "read-only", fmt.Sprintf("site %s state=%q", original, st), nil
				},
			)
			return err
		},
	}
}

func verifyAutoRecoveryLog() runner.Step {
	return runner.Step{
		Phase: runner.PhaseVerify,
		Name:  `"no GTID divergence, auto-recovering" log line emitted`,
		Do: func(ctx context.Context, env *runner.Env) error {
			tail, err := env.Logs("operator")
			if err != nil {
				return err
			}
			waitCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
			defer cancel()
			_, err = env.Wait.UntilLog(waitCtx, tail, time.Time{},
				`"no GTID divergence, auto-recovering" log msg`,
				pglogs.Substring(`no GTID divergence, auto-recovering`),
			)
			return err
		},
	}
}

func verifyReplicationRunningOnOldPrimary() runner.Step {
	return runner.Step{
		Phase: runner.PhaseVerify,
		Name:  "replication thread running on old primary",
		Do: func(ctx context.Context, env *runner.Env) error {
			original := ctxFetch(env, "originalPrimary")
			waitCtx, cancel := context.WithTimeout(ctx, 90*time.Second)
			defer cancel()
			_, err := env.Wait.UntilCR(waitCtx, env.Namespace,
				fmt.Sprintf("status.sites[%s].replicating==true", original),
				func(mfg *v1alpha1.MysqlFailoverGroup) (bool, string, error) {
					for _, s := range mfg.Status.Sites {
						if s.Name == original {
							return s.Replicating, fmt.Sprintf("replicating=%v", s.Replicating), nil
						}
					}
					return false, "site missing from status", nil
				},
			)
			return err
		},
	}
}

func stateOf(mfg *v1alpha1.MysqlFailoverGroup, site string) string {
	for _, s := range mfg.Status.Sites {
		if s.Name == site {
			return s.State
		}
	}
	return ""
}
