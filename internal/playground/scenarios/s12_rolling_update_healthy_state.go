package scenarios

import (
	"context"
	"fmt"
	"sort"
	"time"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"

	v1alpha1 "github.com/shipstream/bloodraven/api/v1alpha1"
	pglogs "github.com/shipstream/bloodraven/internal/playground/logs"
	"github.com/shipstream/bloodraven/internal/playground/runner"
)

func init() {
	runner.Register(scenario12RollingUpdateHealthyState())
}

const (
	s12NewMemoryValue = "300Mi"
)

// scenario12RollingUpdateHealthyState patches the per-site memory
// request and asserts the operator's UpdateController drives an ordered
// roll: the standby rolls first (UpdateReplica → WaitReplica), the
// operator fails over to it (Failover), then the old primary rolls
// (UpdateOldPrimary → WaitOldPrimary → Complete). At the end every pod
// runs with the new memory request, every site is in {writable,
// read-only}, and no TOTAL LOSS log line was emitted during the window.
//
// The assertion is the *invariant*, not the exact phase sequence:
// status.updatePhase must visit at least one non-empty value (proves
// the controller engaged) and must return to "" (proves it completed).
// We do not pin the path to "Complete" through the phases because the
// operator's tickInterval can race past phases on a fast cluster.
//
// Issue #46 safety net (April 2026, see chaos-scenarios.md §12) is
// covered indirectly: a healthy baseline going in means
// `isHealthyReplica()` is True, so the updater actually starts. If the
// safety net regressed and the updater refused to start in the healthy
// case, our s12ObserveUpdateStarted gate would time out.
func scenario12RollingUpdateHealthyState() runner.Scenario {
	return runner.Scenario{
		ID:    "12-rolling-update-healthy-state",
		Title: "Rolling update during healthy state",
		Hypothesis: "Patching spec.sites[*].resources.requests.memory triggers the UpdateController: " +
			"status.updatePhase visits at least one non-empty phase, returns to empty after completion, " +
			"every deployment ends at the new memory request, and no TOTAL LOSS log fires during the roll.",
		Risk:     "medium",
		DocLink:  "playground/chaos-scenarios.md#12-rolling-update-during-healthy-state",
		Timeout:  10 * time.Minute,
		Precheck: assertReplicationRunningPrecheck,
		Steps: []runner.Step{
			s12InjectMemoryPatch(),
			s12ObserveUpdateStarted(),
			s12ObserveUpdateComplete(),
			s12VerifyDeploymentsRolled(),
			s12VerifyClusterHealthy(),
			s12VerifyNoTotalLoss(),
		},
	}
}

func s12InjectMemoryPatch() runner.Step {
	return runner.Step{
		Phase: runner.PhaseInject,
		Name:  "patch spec.sites[*].resources.requests.memory to " + s12NewMemoryValue,
		Do: func(ctx context.Context, env *runner.Env) error {
			// Open the operator log tailer BEFORE the patch so the
			// SinceTime filter covers the rolling-update lifecycle.
			if _, err := env.Logs("operator"); err != nil {
				return fmt.Errorf("open operator tailer: %w", err)
			}
			originals, err := env.Chaos.PatchSitesMemoryRequest(ctx, s12NewMemoryValue)
			if err != nil {
				return err
			}
			env.Capture.Note(fmt.Sprintf("memory request patched: originals=%v new=%s", originals, s12NewMemoryValue))
			return ctxStash(ctx, env, "patchedAt", time.Now().UTC().Format(time.RFC3339Nano))
		},
	}
}

// s12ObserveUpdateStarted waits for status.updatePhase to flip to a
// non-empty value. The first phase the operator sets is
// `UpdateReplica`, but we accept any non-empty phase to tolerate fast
// transitions.
func s12ObserveUpdateStarted() runner.Step {
	return runner.Step{
		Phase: runner.PhaseObserve,
		Name:  "status.updatePhase becomes non-empty (ordered update engaged)",
		Do: func(ctx context.Context, env *runner.Env) error {
			waitCtx, cancel := context.WithTimeout(ctx, 90*time.Second)
			defer cancel()
			_, err := env.Wait.UntilCR(waitCtx, env.Namespace,
				"updatePhase non-empty",
				func(mfg *v1alpha1.MysqlFailoverGroup) (bool, string, error) {
					msg := fmt.Sprintf("updatePhase=%q", mfg.Status.UpdatePhase)
					return mfg.Status.UpdatePhase != "", msg, nil
				},
			)
			return err
		},
	}
}

// s12ObserveUpdateComplete waits for the cluster to settle: updatePhase
// returns to empty AND the cluster is in {1 writable, N-1 read-only}. The
// updater clears the phase on its own when the roll lands successfully;
// if it gets stuck in "Complete" or any non-empty value past 5 minutes
// we want to know.
func s12ObserveUpdateComplete() runner.Step {
	return runner.Step{
		Phase: runner.PhaseObserve,
		Name:  "status.updatePhase returns to empty and sites are 1 writable + all followers read-only",
		Do: func(ctx context.Context, env *runner.Env) error {
			waitCtx, cancel := context.WithTimeout(ctx, 8*time.Minute)
			defer cancel()
			_, err := env.Wait.UntilCR(waitCtx, env.Namespace,
				"updatePhase=\"\" writable=1 read-only=N-1",
				func(mfg *v1alpha1.MysqlFailoverGroup) (bool, string, error) {
					var writable, readOnly, other []string
					for _, s := range mfg.Status.Sites {
						switch s.State {
						case "writable":
							writable = append(writable, s.Name)
						case "read-only":
							readOnly = append(readOnly, s.Name)
						default:
							other = append(other, fmt.Sprintf("%s=%s", s.Name, s.State))
						}
					}
					sort.Strings(writable)
					sort.Strings(readOnly)
					msg := fmt.Sprintf("updatePhase=%q writable=%v read-only=%v other=%v",
						mfg.Status.UpdatePhase, writable, readOnly, other)
					done := mfg.Status.UpdatePhase == "" && len(writable) == 1 && len(readOnly) == len(mfg.Status.Sites)-1
					return done, msg, nil
				},
			)
			return err
		},
	}
}

// s12VerifyDeploymentsRolled confirms every site deployment now runs
// with the patched memory request. We compare quantities (not raw
// strings) so "300Mi" and "300M" differences would pass — the resource
// API canonicalizes the value.
func s12VerifyDeploymentsRolled() runner.Step {
	return runner.Step{
		Phase: runner.PhaseVerify,
		Name:  "every site deployment runs with the patched memory request",
		Do: func(ctx context.Context, env *runner.Env) error {
			expected, err := resource.ParseQuantity(s12NewMemoryValue)
			if err != nil {
				return fmt.Errorf("parse expected memory: %w", err)
			}
			mfg, err := env.Kube.GetMFGNamed(ctx, env.Namespace, env.FG)
			if err != nil {
				return err
			}
			var bad []string
			for _, s := range mfg.Spec.Sites {
				dep, err := env.Kube.GetDeployment(ctx, env.Namespace, fmt.Sprintf("mysql-%s-%s", env.FG, s.Name))
				if err != nil {
					return fmt.Errorf("get deployment for site %s: %w", s.Name, err)
				}
				var got *resource.Quantity
				for _, c := range dep.Spec.Template.Spec.Containers {
					if c.Name != "mysql" {
						continue
					}
					if mem, ok := c.Resources.Requests[corev1.ResourceMemory]; ok {
						q := mem
						got = &q
					}
				}
				if got == nil {
					bad = append(bad, fmt.Sprintf("%s=missing", s.Name))
					continue
				}
				if got.Cmp(expected) != 0 {
					bad = append(bad, fmt.Sprintf("%s=%s (want %s)", s.Name, got.String(), expected.String()))
				}
			}
			if len(bad) > 0 {
				return fmt.Errorf("deployments did not pick up the patched memory request: %v", bad)
			}
			env.Capture.Note(fmt.Sprintf("all deployments at memory=%s", expected.String()))
			return nil
		},
	}
}

func s12VerifyClusterHealthy() runner.Step {
	return runner.Step{
		Phase: runner.PhaseVerify,
		Name:  "cluster is healthy: Ready=True, no RecoveryBlocked",
		Do: func(ctx context.Context, env *runner.Env) error {
			mfg, err := env.Kube.GetMFGNamed(ctx, env.Namespace, env.FG)
			if err != nil {
				return err
			}
			c := findCondition(mfg.Status.Conditions, "Ready")
			if c == nil || string(c.Status) != "True" {
				status := "<missing>"
				if c != nil {
					status = string(c.Status)
				}
				return fmt.Errorf("Ready condition status=%q (want True)", status)
			}
			for _, s := range mfg.Status.Sites {
				if s.RecoveryState == "RecoveryBlocked" {
					return fmt.Errorf("site %s in RecoveryBlocked after roll", s.Name)
				}
			}
			env.Capture.Note("cluster Ready=True with no RecoveryBlocked")
			return nil
		},
	}
}

// s12VerifyNoTotalLoss scans the operator log buffer captured since
// start time for "TOTAL LOSS" — the rolling update's safety contract is
// "brief writer flip, never a window with no writable site." This is a
// negative assertion that can only be made post-hoc against the
// captured ring buffer.
func s12VerifyNoTotalLoss() runner.Step {
	return runner.Step{
		Phase: runner.PhaseVerify,
		Name:  `operator log contains no "TOTAL LOSS" line during the roll`,
		Do: func(ctx context.Context, env *runner.Env) error {
			tail, err := env.Logs("operator")
			if err != nil {
				return err
			}
			matched, line := firstMatchSince(tail, env.StartTime, pglogs.Substring("TOTAL LOSS"))
			if matched {
				return fmt.Errorf("unexpected TOTAL LOSS log emitted during rolling update: %s", line)
			}
			env.Capture.Note("no TOTAL LOSS log observed during the roll")
			return nil
		},
	}
}
