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
	runner.Register(scenario10FullBootstrapAfterDataWipe())
}

// scenario10FullBootstrapAfterDataWipe wipes the read-only site's PVC,
// scales it back up, and asserts the operator drives the empty datadir
// through the bootstrap state machine: Bootstrapping condition cycles
// from `Cloning` → `WaitingForRestart` → `SetupReplication` → `Done`,
// and replication on the new replica reports IO+SQL threads running.
//
// This exercises the fresh-deploy detection path that scenario 19's
// reclone interlock skips: 19 manufactures divergence and rides the
// reclone-site annotation; 10 leaves replication metadata empty so the
// operator's `isFreshDeploy` heuristic (SHOW REPLICA STATUS empty +
// fresh datadir) triggers an unprompted bootstrap.
//
// We target the read-only site (not the writable one) for two reasons:
// 1) wiping the writable site loses uncommitted writes from any other
// scenario in a run-all sequence; 2) the operator's bootstrap source is
// the writable site, so wiping the replica means the clone fans out
// from the surviving primary — the realistic recovery shape.
func scenario10FullBootstrapAfterDataWipe() runner.Scenario {
	return runner.Scenario{
		ID:    "10-full-bootstrap-after-data-wipe",
		Title: "Full bootstrap after replica data wipe",
		Hypothesis: "Deleting the read-only site's PVC and scaling it back up triggers the operator's " +
			"fresh-deploy bootstrap: Bootstrapping condition cycles Cloning → WaitingForRestart → " +
			"SetupReplication → Done with replica replication threads ON at the end.",
		Risk:     "high",
		DocLink:  "playground/chaos-scenarios.md#10-full-bootstrap-after-data-wipe-clone-instance",
		Timeout:  8 * time.Minute,
		Precheck: s10AssertPristine,
		Steps: []runner.Step{
			s10InjectWipeReplicaPVC(),
			s10ObserveBootstrapStarted(),
			s10ObserveBootstrapDone(),
			s10ObserveClusterReconverged(),
			s10VerifyReplicaThreadsRunning(),
		},
	}
}

func s10InjectWipeReplicaPVC() runner.Step {
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
			env.Capture.Note(fmt.Sprintf("wiping replica site: %s (active=%s)", replica, mfg.Status.ActiveSite))
			if err := ctxStash(ctx, env, "wipedSite", replica); err != nil {
				return err
			}
			// Open the operator log tailer BEFORE inject so the
			// SinceTime filter covers bootstrap state transitions.
			if _, err := env.Logs("operator"); err != nil {
				return fmt.Errorf("open operator tailer: %w", err)
			}
			if err := env.Chaos.WipeSiteData(ctx, replica, env.FG); err != nil {
				return fmt.Errorf("wipe data on %s: %w", replica, err)
			}
			// Scaling back up (the reverter) needs to happen now, not
			// at executor cleanup, so we can observe the bootstrap.
			if err := env.Chaos.Revert(ctx); err != nil {
				return fmt.Errorf("scale wiped site back up: %w", err)
			}
			env.Capture.Note(fmt.Sprintf("replica %s scaled back up; awaiting bootstrap", replica))
			return nil
		},
	}
}

// s10ObserveBootstrapStarted waits for the Bootstrapping condition to
// flip Status=True with a phase Reason ("Cloning", "WaitingForRestart",
// or "SetupReplication"). This is the operator's first observable
// signal that the empty datadir was detected and bootstrap began. We
// accept any in-progress phase here because the runner's poll cadence
// (~1s) and the operator's phase-step cadence may race past the
// earliest "Cloning" entry on a fast cluster.
func s10ObserveBootstrapStarted() runner.Step {
	return runner.Step{
		Phase: runner.PhaseObserve,
		Name:  "Bootstrapping condition flips Status=True (any in-progress phase)",
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

// s10ObserveBootstrapDone waits for the Bootstrapping condition to
// settle at Status=False, Reason=Done. The operator emits this once
// the clone, restart, and SetupReplication phases have all completed
// and the replica has REPLICA threads running.
func s10ObserveBootstrapDone() runner.Step {
	return runner.Step{
		Phase: runner.PhaseObserve,
		Name:  "Bootstrapping condition settles at Status=False Reason=Done",
		Do: func(ctx context.Context, env *runner.Env) error {
			// Bootstrap can take 60-180s in the playground depending on
			// k3d storage speed and image-pull state.
			waitCtx, cancel := context.WithTimeout(ctx, 5*time.Minute)
			defer cancel()
			_, err := env.Wait.UntilCR(waitCtx, env.Namespace,
				"Bootstrapping done",
				func(mfg *v1alpha1.MysqlFailoverGroup) (bool, string, error) {
					c := findCondition(mfg.Status.Conditions, "Bootstrapping")
					if c == nil {
						return false, "no Bootstrapping condition yet", nil
					}
					msg := fmt.Sprintf("status=%s reason=%s message=%q", c.Status, c.Reason, c.Message)
					if c.Reason == "Failed" {
						return false, msg, fmt.Errorf("bootstrap failed: %s", c.Message)
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

func s10ObserveClusterReconverged() runner.Step {
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

// s10VerifyReplicaThreadsRunning re-uses scenario 12's pattern: probe
// the read-only site's sidecar for replica_io && replica_sql.
func s10VerifyReplicaThreadsRunning() runner.Step {
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

// s10AssertPristine extends the standard replication-running precheck
// with a hard requirement that no previous failover has happened in
// this cluster's lifetime: status.lastFailoverTarget must be empty.
//
// Why: the operator's `isFreshDeploy(ctx)` bootstrap branch is gated
// on `tm.lastFailoverTarget == ""`. After any prior failover the
// guard takes the `fence-returning-old-primary` path instead, which
// runs the post-failover recovery flow against the wiped site —
// and that flow flags "old primary's gtid_executed not contained
// in new primary's gtid_executed" as DivergentTransactions →
// RecoveryBlocked. So the only way to exercise the fresh-deploy
// bootstrap path on a real cluster is from a pristine post-setup
// state.
//
// In practice this means scenario 10 should be run as the FIRST
// chaos scenario after `./playground/setup.sh`, before any failover
// scenario sets lastFailoverTarget.
func s10AssertPristine(ctx context.Context, env *runner.Env) error {
	if err := assertReplicationRunningPrecheck(ctx, env); err != nil {
		return err
	}
	mfg, err := env.Kube.GetMFG(ctx, env.Namespace)
	if err != nil {
		return fmt.Errorf("precheck: get MFG: %w", err)
	}
	if mfg.Status.LastFailoverTarget != "" {
		return fmt.Errorf(
			"precheck: status.lastFailoverTarget=%q (cluster has had a failover); "+
				"fresh-deploy bootstrap detection requires a pristine cluster — "+
				"run `./playground/setup.sh` (or recreate the cluster) and run scenario 10 first",
			mfg.Status.LastFailoverTarget)
	}
	return nil
}

// findCondition returns a pointer to the first condition in conds with
// the given Type, or nil. Used by scenarios that want to inspect
// .Status / .Reason / .Message together (the existing
// pgkube.ReadyCondition helper only returns the Status string).
func findCondition(conds []metav1.Condition, typeName string) *metav1.Condition {
	for i := range conds {
		if conds[i].Type == typeName {
			return &conds[i]
		}
	}
	return nil
}
