package scenarios

import (
	"context"
	"fmt"
	"sort"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	v1alpha1 "github.com/shipstream/bloodraven/api/v1alpha1"
	"github.com/shipstream/bloodraven/internal/playground/runner"
	pgsidecar "github.com/shipstream/bloodraven/internal/playground/sidecar"
)

func init() {
	runner.Register(scenario13OperatorKillDuringBootstrap())
}

// scenario13OperatorKillDuringBootstrap wipes the read-only site's
// PVC to start a fresh bootstrap (same primitive as scenario 10),
// then kills the operator pod once the Bootstrapping condition turns
// True. The contract under test is that the new operator pod picks
// up the in-flight clone — either by resuming the wait for the
// existing clone (MySQL tracks clone state in
// performance_schema.clone_status) or by starting a fresh clone — and
// drives the cluster back to a healthy primary + replicating-replica
// pair without manual intervention.
//
// Like scenario 10, this exercises the operator's
// `tm.lastFailoverTarget == "" && isFreshDeploy(ctx)` branch, so the
// precheck requires a pristine cluster (lastFailoverTarget empty).
// In practice that means s13 must run BEFORE any failover scenario,
// or after `./playground/setup.sh` reset.
//
// Compared to s10 (no operator kill), this scenario asserts the same
// end-state invariants but tolerates the in-progress Bootstrapping
// condition disappearing or restarting from "Cloning" again — the new
// operator may legitimately decide to start a fresh clone if it cannot
// safely resume the previous one. We therefore wait on the *terminal*
// signal (Bootstrapping=False, Reason=Done) rather than tracking
// individual phase transitions.
func scenario13OperatorKillDuringBootstrap() runner.Scenario {
	return runner.Scenario{
		ID:    "13-operator-kill-during-bootstrap",
		Title: "Operator kill mid-bootstrap converges to healthy replica",
		Hypothesis: "Killing the operator while the Bootstrapping condition is True must NOT wedge the " +
			"clone: the restarted operator either resumes the in-flight clone or starts a fresh one, the " +
			"Bootstrapping condition eventually reaches Status=False Reason=Done, and the wiped site rejoins " +
			"as a replica with replica_io_running && replica_sql_running.",
		Risk:     "high",
		DocLink:  "playground/chaos-scenarios.md#13-operator-kill-during-bootstrap",
		Timeout:  10 * time.Minute,
		Precheck: s10AssertPristine,
		Steps: []runner.Step{
			s13InjectWipePVC(),
			s13ObserveBootstrapStarted(),
			s13InjectKillOperator(),
			s13ObserveBootstrapEventuallyDone(),
			s13ObserveClusterReconverged(),
			s13VerifyReplicaThreadsRunning(),
		},
	}
}

// s13InjectWipePVC mirrors s10's PVC-wipe primitive. We don't share
// the step body because the post-wipe behaviour we care about
// diverges (s10 watches the bootstrap progress unmolested; we want
// the bootstrap to start before the operator kill).
func s13InjectWipePVC() runner.Step {
	return runner.Step{
		Phase: runner.PhaseInject,
		Name:  "scale read-only site to 0, clear taints, delete PVC, scale back up",
		Do: func(ctx context.Context, env *runner.Env) error {
			mfg, err := env.Kube.GetMFG(ctx, env.Namespace)
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
				return fmt.Errorf("no read-only site present (sites=%+v)", mfg.Status.Sites)
			}
			env.Capture.Note(fmt.Sprintf("wiping replica %s to provoke bootstrap (active=%s)",
				replica, mfg.Status.ActiveSite))
			if err := ctxStash(ctx, env, "wipedSite", replica); err != nil {
				return err
			}
			if _, err := env.Logs("operator"); err != nil {
				return fmt.Errorf("open operator tailer: %w", err)
			}
			if err := env.Chaos.WipeSiteData(ctx, replica, env.FG); err != nil {
				return fmt.Errorf("wipe data on %s: %w", replica, err)
			}
			// Replay the scale-back-up reverter now (in scenario scope)
			// so the bootstrap actually starts; otherwise the runner's
			// cleanup-time Revert would defer it past our observation
			// window.
			if err := env.Chaos.Revert(ctx); err != nil {
				return fmt.Errorf("scale wiped site back up: %w", err)
			}
			env.Capture.Note(fmt.Sprintf("replica %s scaled back up; awaiting Bootstrapping=True", replica))
			return nil
		},
	}
}

// s13ObserveBootstrapStarted waits for the Bootstrapping condition to
// flip Status=True (any Reason). This is the trigger for the operator
// kill — we want the kill to land while the clone is in progress.
func s13ObserveBootstrapStarted() runner.Step {
	return runner.Step{
		Phase: runner.PhaseObserve,
		Name:  "Bootstrapping condition Status=True (clone in progress)",
		Do: func(ctx context.Context, env *runner.Env) error {
			waitCtx, cancel := context.WithTimeout(ctx, 90*time.Second)
			defer cancel()
			_, err := env.Wait.UntilCR(waitCtx, env.Namespace,
				"Bootstrapping condition Status=True",
				func(mfg *v1alpha1.MysqlFailoverGroup) (bool, string, error) {
					c := findCondition(mfg.Status.Conditions, "Bootstrapping")
					if c == nil {
						return false, "no Bootstrapping condition yet", nil
					}
					msg := fmt.Sprintf("status=%s reason=%s", c.Status, c.Reason)
					if c.Status == metav1.ConditionTrue {
						return true, msg, nil
					}
					return false, msg, nil
				},
			)
			return err
		},
	}
}

func s13InjectKillOperator() runner.Step {
	return runner.Step{
		Phase: runner.PhaseInject,
		Name:  "kill operator pod mid-bootstrap",
		Do: func(ctx context.Context, env *runner.Env) error {
			env.Capture.Note("Bootstrapping=True observed; killing operator pod now")
			return env.Chaos.KillOperator(ctx)
		},
	}
}

// s13ObserveBootstrapEventuallyDone waits for the terminal phase
// signal regardless of how the new operator gets there. The bootstrap
// can take 60–180s in the playground, plus operator-restart time
// (~10–30s) and a possible fresh-clone restart from scratch — we
// budget 7 minutes of headroom.
//
// Failure mode we explicitly catch: Bootstrapping settles at
// Status=False Reason=Failed. Without the explicit Reason check, the
// ConditionFalse predicate would consider Failed a successful match.
func s13ObserveBootstrapEventuallyDone() runner.Step {
	return runner.Step{
		Phase: runner.PhaseObserve,
		Name:  "Bootstrapping settles at Status=False Reason=Done",
		Do: func(ctx context.Context, env *runner.Env) error {
			waitCtx, cancel := context.WithTimeout(ctx, 7*time.Minute)
			defer cancel()
			_, err := env.Wait.UntilCR(waitCtx, env.Namespace,
				"Bootstrapping done after operator restart",
				func(mfg *v1alpha1.MysqlFailoverGroup) (bool, string, error) {
					c := findCondition(mfg.Status.Conditions, "Bootstrapping")
					if c == nil {
						return false, "no Bootstrapping condition yet", nil
					}
					msg := fmt.Sprintf("status=%s reason=%s message=%q", c.Status, c.Reason, c.Message)
					if c.Reason == "Failed" {
						return false, msg, fmt.Errorf("bootstrap failed after operator restart: %s", c.Message)
					}
					if c.Status == metav1.ConditionFalse && c.Reason == "Done" {
						return true, msg, nil
					}
					return false, msg, nil
				},
			)
			return err
		},
	}
}

func s13ObserveClusterReconverged() runner.Step {
	return runner.Step{
		Phase: runner.PhaseObserve,
		Name:  "cluster reconverges to one writable + one read-only with no divergence",
		Do: func(ctx context.Context, env *runner.Env) error {
			waitCtx, cancel := context.WithTimeout(ctx, 3*time.Minute)
			defer cancel()
			_, err := env.Wait.UntilCR(waitCtx, env.Namespace,
				"sites: writable=1 read-only=1 divergent=0 blocked=0",
				func(mfg *v1alpha1.MysqlFailoverGroup) (bool, string, error) {
					var writable, readOnly, other, blocked, divergent []string
					for _, s := range mfg.Status.Sites {
						switch s.State {
						case "writable":
							writable = append(writable, s.Name)
						case "read-only":
							readOnly = append(readOnly, s.Name)
						default:
							other = append(other, fmt.Sprintf("%s=%s", s.Name, s.State))
						}
						if s.RecoveryState == "RecoveryBlocked" {
							blocked = append(blocked, s.Name)
						}
						if s.DivergentGtid != "" {
							divergent = append(divergent, s.Name)
						}
					}
					sort.Strings(writable)
					sort.Strings(readOnly)
					msg := fmt.Sprintf("writable=%v read-only=%v other=%v blocked=%v divergent=%v",
						writable, readOnly, other, blocked, divergent)
					done := len(writable) == 1 && len(readOnly) == 1 && len(blocked) == 0 && len(divergent) == 0
					return done, msg, nil
				},
			)
			return err
		},
	}
}

// s13VerifyReplicaThreadsRunning probes the originally-wiped site and
// asserts it is the read-only replica with replica_io && replica_sql.
func s13VerifyReplicaThreadsRunning() runner.Step {
	return runner.Step{
		Phase: runner.PhaseVerify,
		Name:  "wiped site is replicating: replica_io_running && replica_sql_running",
		Do: func(ctx context.Context, env *runner.Env) error {
			wiped := ctxFetch(env, "wipedSite")
			mfg, err := env.Kube.GetMFG(ctx, env.Namespace)
			if err != nil {
				return err
			}
			var wipedState string
			for _, s := range mfg.Status.Sites {
				if s.Name == wiped {
					wipedState = s.State
					break
				}
			}
			if wipedState == "" {
				return fmt.Errorf("wiped site %q missing at verify time (sites=%+v)", wiped, mfg.Status.Sites)
			}
			if wipedState != "read-only" {
				return fmt.Errorf("wiped site %s state=%q, want read-only replicating replica", wiped, wipedState)
			}
			env.Capture.Note(fmt.Sprintf("probing wiped read-only site %s", wiped))
			probe, err := env.Sidecar(wiped)
			if err != nil {
				return fmt.Errorf("open sidecar probe for %s: %w", wiped, err)
			}
			waitCtx, cancel := context.WithTimeout(ctx, 90*time.Second)
			defer cancel()
			return env.Wait.UntilSidecarStatus(waitCtx, probe,
				fmt.Sprintf("site %s replica_io_running && replica_sql_running", wiped),
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
