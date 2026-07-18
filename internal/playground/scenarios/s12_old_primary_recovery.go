package scenarios

import (
	"context"
	"fmt"
	"sort"
	"time"

	v1alpha1 "github.com/shipstream/bloodraven/api/v1alpha1"
	pglogs "github.com/shipstream/bloodraven/internal/playground/logs"
	"github.com/shipstream/bloodraven/internal/playground/runner"
	pgsidecar "github.com/shipstream/bloodraven/internal/playground/sidecar"
)

func init() {
	runner.Register(scenario12OldPrimaryRecovery())
}

// scenario12OldPrimaryRecovery scales the active primary down, waits
// for failover, scales it back up, and asserts the operator's
// "no GTID divergence, auto-recovering old primary as replica" path.
//
// The assertions are invariant-based, not identity-based on the
// originally-killed site. The operator may auto-fail-back to the
// returning site (we have observed this on the playground), in which
// case the *peer* — not the originally-killed site — ends up as the
// recovered replica. Either ordering is a correct outcome of the
// recovery codepath; the invariant we care about is "exactly one
// writable site, exactly one read-only site with replica IO+SQL
// threads running, and no divergent transactions reported anywhere."
func scenario12OldPrimaryRecovery() runner.Scenario {
	return runner.Scenario{
		ID:    "12-old-primary-recovery-no-divergence",
		Title: "Old primary recovers without divergence after failover",
		Hypothesis: "After a clean failover, scaling the old primary back up triggers " +
			"'no GTID divergence, auto-recovering old primary as replica' and the cluster " +
			"reconverges to one writable + one replicating read-only site, with no divergent transactions.",
		Risk:     "low",
		DocLink:  "playground/chaos-scenarios.md#12-old-primary-recovery-no-divergence",
		Timeout:  5 * time.Minute,
		Precheck: AssertHealthyBaseline,
		Steps: []runner.Step{
			injectScaleZeroStash(),
			observeFailoverFlip(),
			injectScaleBackUp(),
			observeClusterReconvergence(),
			verifyAutoRecoveryLog(),
			verifyReplicaThreadsRunning(),
		},
	}
}

func injectScaleZeroStash() runner.Step {
	return runner.Step{
		Phase: runner.PhaseInject,
		Name:  "scale active primary to 0 and remember it",
		Do: func(ctx context.Context, env *runner.Env) error {
			mfg, err := env.Kube.GetMFGNamed(ctx, env.Namespace, env.FG)
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

// observeClusterReconvergence waits for the cluster to settle into a
// healthy two-site state after the recovery, regardless of which site
// the operator picks as primary. This tolerates the operator's
// auto-fail-back behavior: after a returning site comes up writable,
// the operator may pick either site as primary; what we care about is
// that the system converges to a single primary + a single replicating
// replica, with no divergence reported.
func observeClusterReconvergence() runner.Step {
	return runner.Step{
		Phase: runner.PhaseObserve,
		Name:  "cluster reconverges to one writable + one read-only with no divergence",
		Do: func(ctx context.Context, env *runner.Env) error {
			waitCtx, cancel := context.WithTimeout(ctx, 3*time.Minute)
			defer cancel()
			_, err := env.Wait.UntilCR(waitCtx, env.Namespace,
				"sites: writable=1 read-only=N-1 divergent=0",
				func(mfg *v1alpha1.MysqlFailoverGroup) (bool, string, error) {
					var writable, readOnly, other []string
					var divergent int64
					var blocked []string
					for _, s := range mfg.Status.Sites {
						switch s.State {
						case "writable":
							writable = append(writable, s.Name)
						case "read-only":
							readOnly = append(readOnly, s.Name)
						default:
							other = append(other, fmt.Sprintf("%s=%s", s.Name, s.State))
						}
						if s.DivergentTransactionCount != nil {
							divergent += *s.DivergentTransactionCount
						}
						if s.RecoveryState == "RecoveryBlocked" {
							blocked = append(blocked, s.Name)
						}
					}
					sort.Strings(writable)
					sort.Strings(readOnly)
					msg := fmt.Sprintf(
						"writable=%v read-only=%v other=%v divergent=%d blocked=%v",
						writable, readOnly, other, divergent, blocked,
					)
					done := len(writable) == 1 && len(readOnly) == len(mfg.Status.Sites)-1 && divergent == 0 && len(blocked) == 0
					return done, msg, nil
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
			_, err = env.Wait.UntilLog(waitCtx, tail, env.StartTime,
				`"no GTID divergence, auto-recovering" log msg`,
				pglogs.Substring(`no GTID divergence, auto-recovering`),
			)
			return err
		},
	}
}

// verifyReplicaThreadsRunning probes the sidecar /status of whichever
// site is currently read-only and asserts replica_io_running &&
// replica_sql_running. We probe the sidecar (not the CR's
// status.sites[].replicating field) because the CR is enriched by the
// operator on a slow cadence and may lag the live MySQL state. We
// resolve the read-only site dynamically because auto-fail-back means
// either site could be the recovered replica — see the scenario
// docstring.
func verifyReplicaThreadsRunning() runner.Step {
	return runner.Step{
		Phase: runner.PhaseVerify,
		Name:  "replica site has replica_io_running && replica_sql_running",
		Do: func(ctx context.Context, env *runner.Env) error {
			mfg, err := env.Kube.GetMFGNamed(ctx, env.Namespace, env.FG)
			if err != nil {
				return err
			}
			var replica string
			for _, s := range mfg.Status.Sites {
				if s.State == "read-only" {
					replica = s.Name
					break
				}
			}
			if replica == "" {
				return fmt.Errorf("no read-only site present at verify time (sites=%+v)", mfg.Status.Sites)
			}
			env.Capture.Note(fmt.Sprintf("probing sidecar /status on read-only site: %s (originalPrimary=%s)",
				replica, ctxFetch(env, "originalPrimary")))
			probe, err := env.Sidecar(replica)
			if err != nil {
				return fmt.Errorf("open sidecar probe for %s: %w", replica, err)
			}
			waitCtx, cancel := context.WithTimeout(ctx, 90*time.Second)
			defer cancel()
			return env.Wait.UntilSidecarStatus(waitCtx, probe,
				fmt.Sprintf("site %s replica_io_running && replica_sql_running", replica),
				func(st *pgsidecar.StatusResponse) (bool, string) {
					msg := fmt.Sprintf(
						"role=%s read_only=%v replica_io=%v replica_sql=%v",
						st.Role, st.ReadOnly, st.ReplicaIORunning, st.ReplicaSQLRunning,
					)
					return st.ReplicaIORunning && st.ReplicaSQLRunning, msg
				},
			)
		},
	}
}
