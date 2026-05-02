package scenarios

import (
	"context"
	"fmt"
	"time"

	pglogs "github.com/shipstream/bloodraven/internal/playground/logs"
	"github.com/shipstream/bloodraven/internal/playground/runner"
)

func init() {
	runner.Register(scenario17PartitionReplicaNoFailover())
}

// scenario17PartitionReplicaNoFailover is the asymmetric counterpart
// to 09-network-partition-self-fence. Partitioning the read-only
// replica must NOT trigger failover (the primary is still healthy)
// and must NOT cause the replica's sidecar to self-fence — the
// sidecar's evaluate() skips read-only nodes by design, so the
// "every peer unreachable" backstop should never fire on a replica.
//
// This is a negative assertion suite: the goal is to detect a
// regression that "fixes" the fencing logic in a way that
// accidentally causes read-only sites to fence themselves under
// partition. Such a regression would create unnecessary cluster
// outages every time a replica's network blips.
func scenario17PartitionReplicaNoFailover() runner.Scenario {
	return runner.Scenario{
		ID:    "17-partition-replica-no-failover",
		Title: "Partitioning the replica neither fails over nor self-fences",
		Hypothesis: "A deny-all NetworkPolicy on the read-only site does NOT change activeSite and does " +
			"NOT cause the replica's sidecar to emit SELF-FENCING/SELF-FENCED — read-only sites are " +
			"excluded from the lease-timeout self-fence rule.",
		Risk:     "medium",
		DocLink:  "playground/chaos-scenarios.md#17-network-partition-of-replica-not-primary",
		Timeout:  3 * time.Minute,
		Precheck: AssertHealthyBaseline,
		Steps: []runner.Step{
			injectPartitionReplica(),
			settleAfterReplicaPartition(),
			verifyActivePrimaryUnchanged(),
			verifyReplicaDidNotSelfFence(),
		},
	}
}

func injectPartitionReplica() runner.Step {
	return runner.Step{
		Phase: runner.PhaseInject,
		Name:  "open replica sidecar tailer, apply deny-all NetworkPolicy to replica",
		Do: func(ctx context.Context, env *runner.Env) error {
			mfg, err := env.Kube.GetMFG(ctx, env.Namespace)
			if err != nil {
				return err
			}
			active := mfg.Status.ActiveSite
			replica, err := PeerOf(mfg, active)
			if err != nil {
				return err
			}
			env.Capture.Note(fmt.Sprintf("active=%s replica=%s; partitioning the replica", active, replica))
			if err := ctxStash(ctx, env, "originalPrimary", active); err != nil {
				return err
			}
			if err := ctxStash(ctx, env, "replicaSite", replica); err != nil {
				return err
			}
			// Sidecar tailer must be opened BEFORE the partition lands —
			// the NP will block port-forward to the sidecar HTTP, but
			// log streams come from the kubelet API and stay reachable.
			if _, err := env.Logs("sidecar:" + replica); err != nil {
				return fmt.Errorf("open sidecar tailer for %s: %w", replica, err)
			}
			return env.Chaos.PartitionSite(ctx, replica)
		},
	}
}

func settleAfterReplicaPartition() runner.Step {
	return runner.Step{
		Phase: runner.PhaseSettle,
		Name:  "wait 35s past partition (covers leaseTimeout + detection)",
		Do: func(ctx context.Context, env *runner.Env) error {
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-time.After(35 * time.Second):
				return nil
			}
		},
	}
}

func verifyActivePrimaryUnchanged() runner.Step {
	return runner.Step{
		Phase: runner.PhaseVerify,
		Name:  "status.activeSite is unchanged and primary is still writable",
		Do: func(ctx context.Context, env *runner.Env) error {
			original := ctxFetch(env, "originalPrimary")
			replica := ctxFetch(env, "replicaSite")
			mfg, err := env.Kube.GetMFG(ctx, env.Namespace)
			if err != nil {
				return err
			}
			if mfg.Status.ActiveSite != original {
				return fmt.Errorf("activeSite changed during replica partition: was %q, now %q (lastFailoverTarget=%q)",
					original, mfg.Status.ActiveSite, mfg.Status.LastFailoverTarget)
			}
			// The primary should still be writable; the replica's state
			// may be "unreachable" — that's the expected, correct
			// classification while the partition holds.
			var primaryState, replicaState string
			for _, s := range mfg.Status.Sites {
				switch s.Name {
				case original:
					primaryState = s.State
				case replica:
					replicaState = s.State
				}
			}
			if primaryState != "writable" {
				return fmt.Errorf("expected primary %s state=writable, got %q", original, primaryState)
			}
			if replicaState != "unreachable" {
				return fmt.Errorf("replica partition did not take effect: site %s state=%q, want unreachable", replica, replicaState)
			}
			env.Capture.Note(fmt.Sprintf("primary %s state=%s, replica %s state=%s", original, primaryState, replica, replicaState))
			// Sanity check: the operator must not have flipped the
			// failover-target field as if it were preparing one.
			if mfg.Status.LastFailoverTarget == replica {
				return fmt.Errorf("operator set lastFailoverTarget=%q (the partitioned replica) — implies an attempted promotion", replica)
			}
			return nil
		},
	}
}

func verifyReplicaDidNotSelfFence() runner.Step {
	return runner.Step{
		Phase: runner.PhaseVerify,
		Name:  "no SELF-FENCING line on the partitioned replica sidecar",
		Do: func(ctx context.Context, env *runner.Env) error {
			replica := ctxFetch(env, "replicaSite")
			tail, err := env.Logs("sidecar:" + replica)
			if err != nil {
				return fmt.Errorf("get sidecar tailer for %s: %w", replica, err)
			}
			if hit, line := firstMatchSince(tail, env.StartTime, pglogs.Substring("SELF-FENC")); hit {
				return fmt.Errorf("replica %s self-fenced under partition (read-only sites must not self-fence): %s", replica, line)
			}
			return nil
		},
	}
}
