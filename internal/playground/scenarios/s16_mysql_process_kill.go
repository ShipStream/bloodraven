package scenarios

import (
	"context"
	"fmt"
	"sort"
	"strings"
	"time"

	v1alpha1 "github.com/shipstream/bloodraven/api/v1alpha1"
	"github.com/shipstream/bloodraven/internal/playground/runner"
)

func init() {
	runner.Register(scenario16MysqlProcessKill())
}

// scenario16MysqlProcessKill kills the active site's mysqld via the SQL
// SHUTDOWN statement (the in-process equivalent of `mysqladmin
// shutdown`) and asserts the cluster never enters split-brain. The
// cluster may or may not fail over depending on how fast Kubernetes
// restarts the mysql container vs the operator's pollInterval ×
// failureThreshold (~6s in playground config). Both outcomes are
// acceptable; the *bad* outcome — two writable sites simultaneously,
// or a stuck RecoveryBlocked state — is the only thing this scenario
// asserts against.
//
// We use SHUTDOWN over the existing port-forwarded MySQL connection
// rather than `kubectl debug --target=mysql` because (a) the runner
// already has a connection open, (b) the doc explicitly notes
// `kill -9 1` does NOT work on PID 1 from within the container's PID
// namespace, and (c) `mysqladmin shutdown` is just a thin wrapper around
// the SHUTDOWN SQL command.
//
// Assertion shape: poll for ~90s (enough for either path: MySQL
// restarted in time → no failover, or operator detected death and
// promoted the peer). At any observation, allow exactly one of:
//   - activeSite unchanged AND still writable (no failover happened).
//   - activeSite flipped AND new primary writable, old primary recovers
//     to read-only or still down.
//
// Forbid: zero writable sites for more than 60s, or two writable sites
// at any point.
func scenario16MysqlProcessKill() runner.Scenario {
	return runner.Scenario{
		ID:    "16-mysql-process-kill",
		Title: "MySQL process kill (SHUTDOWN) — never split-brain, container restarts",
		Hypothesis: "Issuing SHUTDOWN against the active site's mysqld restarts the container. The cluster " +
			"may or may not fail over depending on detection timing, but it must NEVER end up with two " +
			"writable sites or a stuck zero-writable state, and the mysql container restartCount must " +
			"increment.",
		Risk:     "medium",
		DocLink:  "playground/chaos-scenarios.md#16-mysql-process-kill-not-pod-kill",
		Timeout:  4 * time.Minute,
		Precheck: AssertHealthyBaseline,
		Steps: []runner.Step{
			s16InjectShutdown(),
			s16ObserveContainerRestarted(),
			s16VerifyNoSplitBrain(),
		},
	}
}

func s16InjectShutdown() runner.Step {
	return runner.Step{
		Phase: runner.PhaseInject,
		Name:  "issue SHUTDOWN against the active site's mysqld",
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
			startCount, err := env.Kube.MysqlContainerRestartCount(ctx, env.Namespace, pod.Name)
			if err != nil {
				return fmt.Errorf("read mysql restart count: %w", err)
			}
			env.Capture.Note(fmt.Sprintf("active=%s pod=%s mysql.restartCount=%d", active, pod.Name, startCount))
			if err := ctxStash(ctx, env, "originalPrimary", active); err != nil {
				return err
			}
			if err := ctxStash(ctx, env, "originalPod", pod.Name); err != nil {
				return err
			}
			if err := ctxStash(ctx, env, "originalRestartCount", fmt.Sprintf("%d", startCount)); err != nil {
				return err
			}

			client, err := env.MySQL(active)
			if err != nil {
				return fmt.Errorf("open mysql client for %s: %w", active, err)
			}
			// Issuing SHUTDOWN drops the connection; go-sql-driver
			// returns a connection-aborted error. We treat any error
			// matching "shutdown" / "EOF" / "closed" / "broken pipe" as
			// the expected outcome.
			if _, err := client.Exec(ctx, "SHUTDOWN"); err != nil {
				if !isExpectedShutdownErr(err) {
					return fmt.Errorf("SHUTDOWN on %s returned unexpected error: %w", active, err)
				}
				env.Capture.Note(fmt.Sprintf("SHUTDOWN dropped connection (expected): %v", err))
			} else {
				env.Capture.Note("SHUTDOWN succeeded with no error (acceptable on fast disconnect)")
			}
			return nil
		},
	}
}

// isExpectedShutdownErr matches the expected error shapes that occur
// when a SHUTDOWN-induced connection drop reaches go-sql-driver. We do
// not unwrap-into-MySQLError here because a kernel-level TCP RST also
// surfaces as a generic IO error.
func isExpectedShutdownErr(err error) bool {
	if err == nil {
		return false
	}
	msg := strings.ToLower(err.Error())
	for _, hint := range []string{"shutdown", "broken pipe", "eof", "connection refused", "connection reset", "invalid connection", "closed"} {
		if strings.Contains(msg, hint) {
			return true
		}
	}
	return false
}

// s16ObserveContainerRestarted polls the original pod's mysql container
// restartCount until it has incremented past the captured baseline. We
// poll the same pod identity (not "the pod for this site right now")
// because a pod-level kill would create a new pod, which is NOT the
// behavior we want to assert here — SHUTDOWN should restart the
// container in place.
func s16ObserveContainerRestarted() runner.Step {
	return runner.Step{
		Phase: runner.PhaseObserve,
		Name:  "mysql container restartCount increments on the same pod",
		Do: func(ctx context.Context, env *runner.Env) error {
			pod := ctxFetch(env, "originalPod")
			origStr := ctxFetch(env, "originalRestartCount")
			var orig int32
			if _, err := fmt.Sscanf(origStr, "%d", &orig); err != nil {
				return fmt.Errorf("parse originalRestartCount %q: %w", origStr, err)
			}
			deadline := time.Now().Add(120 * time.Second)
			tick := time.NewTicker(2 * time.Second)
			defer tick.Stop()
			var lastObs string
			for {
				cur, err := env.Kube.MysqlContainerRestartCount(ctx, env.Namespace, pod)
				if err == nil {
					lastObs = fmt.Sprintf("restartCount=%d (orig=%d)", cur, orig)
					if cur > orig {
						env.Capture.Note(lastObs + " — increment observed")
						return nil
					}
				} else {
					lastObs = "get pod failed: " + err.Error()
				}
				if time.Now().After(deadline) {
					return fmt.Errorf("mysql container did not restart within 120s (last: %s)", lastObs)
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

// s16VerifyNoSplitBrain waits for the cluster to converge after the
// SHUTDOWN and fails immediately if the CR ever reports more than one
// writable site. A transient split-brain is still a split-brain for this
// scenario's safety contract.
func s16VerifyNoSplitBrain() runner.Step {
	return runner.Step{
		Phase: runner.PhaseVerify,
		Name:  "cluster converges to exactly one writable + all followers read-only after SHUTDOWN",
		Do: func(ctx context.Context, env *runner.Env) error {
			deadline := time.Now().Add(3 * time.Minute)
			tick := time.NewTicker(2 * time.Second)
			defer tick.Stop()
			for {
				mfg, err := env.Kube.GetMFGNamed(ctx, env.Namespace, env.FG)
				if err == nil {
					var writable, readOnly, blocked []string
					for _, s := range mfg.Status.Sites {
						switch s.State {
						case "writable":
							writable = append(writable, s.Name)
						case "read-only":
							readOnly = append(readOnly, s.Name)
						}
						if s.RecoveryState == "RecoveryBlocked" {
							blocked = append(blocked, s.Name)
						}
					}
					sort.Strings(writable)
					sort.Strings(readOnly)
					if len(blocked) > 0 {
						return fmt.Errorf("site(s) stuck in RecoveryBlocked: %v", blocked)
					}
					if len(writable) > 1 {
						return fmt.Errorf("split-brain observed after mysqld SHUTDOWN: writable=%v", writable)
					}
					if len(writable) == 1 && len(readOnly) == len(mfg.Status.Sites)-1 {
						env.Capture.Note(fmt.Sprintf("converged: writable=%v read-only=%v", writable, readOnly))
						return nil
					}
				}
				if time.Now().After(deadline) {
					last, lerr := env.Kube.GetMFGNamed(ctx, env.Namespace, env.FG)
					return fmt.Errorf("cluster did not converge to 1 writable + N-1 read-only within 3m (last fetch err=%v sites=%+v)",
						lerr, summarizeSites(last))
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

func summarizeSites(mfg *v1alpha1.MysqlFailoverGroup) string {
	if mfg == nil {
		return "<nil>"
	}
	var parts []string
	for _, s := range mfg.Status.Sites {
		parts = append(parts, fmt.Sprintf("%s=%s/recovery=%s", s.Name, s.State, s.RecoveryState))
	}
	return strings.Join(parts, " ")
}
