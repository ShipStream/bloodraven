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
	runner.Register(scenario09NetworkPartitionSelfFence())
}

// scenario09NetworkPartitionSelfFence applies a deny-all NetworkPolicy
// to the active primary's pod for >25s (>20s leaseTimeout). The
// partitioned sidecar should self-fence (SELF-FENCED log) and the
// operator should fail over to the peer.
func scenario09NetworkPartitionSelfFence() runner.Scenario {
	return runner.Scenario{
		ID:    "09-network-partition-self-fence",
		Title: "Network partition forces sidecar self-fence + failover",
		Hypothesis: "A deny-all NetworkPolicy on the active site for >leaseTimeout causes the sidecar to " +
			"self-fence (super_read_only=ON, SELF-FENCED log) and the operator to fail over to the peer.",
		Risk:    "medium",
		DocLink: "playground/chaos-scenarios.md#9-network-partition-self-fence",
		Timeout: 4 * time.Minute,
		Precheck: AssertHealthyBaseline,
		Steps: []runner.Step{
			injectPartitionActive(),
			observeFailoverDuringPartition(),
			verifySelfFenceLog(),
		},
	}
}

func injectPartitionActive() runner.Step {
	return runner.Step{
		Phase: runner.PhaseInject,
		Name:  "apply deny-all NetworkPolicy to active site",
		Do: func(ctx context.Context, env *runner.Env) error {
			mfg, err := env.Kube.GetMFG(ctx, env.Namespace)
			if err != nil {
				return err
			}
			active := mfg.Status.ActiveSite
			env.Capture.Note(fmt.Sprintf("partitioning active site %s", active))
			if err := ctxStash(ctx, env, "partitionedSite", active); err != nil {
				return err
			}
			// Open a sidecar log tailer BEFORE the partition lands so we
			// catch the SELF-FENCED line. The partition will block our
			// port-forward to the sidecar HTTP, but log streams come
			// from the kubelet API and stay reachable.
			if _, err := env.Logs("sidecar:" + active); err != nil {
				return fmt.Errorf("open sidecar log tailer: %w", err)
			}
			return env.Chaos.PartitionSite(ctx, active)
		},
	}
}

func observeFailoverDuringPartition() runner.Step {
	return runner.Step{
		Phase: runner.PhaseObserve,
		Name:  "operator fails over while partition holds",
		Do: func(ctx context.Context, env *runner.Env) error {
			original := ctxFetch(env, "partitionedSite")
			waitCtx, cancel := context.WithTimeout(ctx, 2*time.Minute)
			defer cancel()
			_, err := env.Wait.UntilCR(waitCtx, env.Namespace,
				fmt.Sprintf("activeSite flips away from partitioned site %s", original),
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

func verifySelfFenceLog() runner.Step {
	return runner.Step{
		Phase: runner.PhaseVerify,
		Name:  `"SELF-FENCED" line in partitioned sidecar log`,
		Do: func(ctx context.Context, env *runner.Env) error {
			site := ctxFetch(env, "partitionedSite")
			tail, err := env.Logs("sidecar:" + site)
			if err != nil {
				return err
			}
			waitCtx, cancel := context.WithTimeout(ctx, 60*time.Second)
			defer cancel()
			_, err = env.Wait.UntilLog(waitCtx, tail, env.StartTime,
				`SELF-FENCED log msg`,
				pglogs.Substring(`SELF-FENCED`),
			)
			return err
		},
	}
}
