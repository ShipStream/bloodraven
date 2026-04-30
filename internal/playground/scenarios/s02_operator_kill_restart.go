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
	runner.Register(scenario02OperatorKillRestart())
}

// scenario02OperatorKillRestart deletes the operator pod while the
// cluster is healthy and asserts the well-behaved restart contract:
// the Deployment respawns the pod, no spurious failover fires, and
// no sidecar self-fences during the gap.
//
// Settle window is 35s. The sidecar lease timeout in the playground
// is 20s; the longest plausible operator restart in this environment
// is ~10s. 35s covers both with margin and ensures any reaction to
// the gap (failover, self-fence) would have surfaced in a stable
// state by the time we sample it.
func scenario02OperatorKillRestart() runner.Scenario {
	return runner.Scenario{
		ID:    "02-operator-kill-restart",
		Title: "Operator kill — restart with no spurious failover",
		Hypothesis: "Killing the operator pod while the cluster is healthy is absorbed within the sidecar " +
			"lease timeout: the Deployment respawns the operator, status.activeSite does not change, and " +
			"neither sidecar self-fences.",
		Risk:    "low",
		DocLink: "playground/chaos-scenarios.md#2-operator-kill-and-restart",
		Timeout: 3 * time.Minute,
		Precheck: AssertHealthyBaseline,
		Steps: []runner.Step{
			injectOperatorKill(),
			settleAfterOperatorKill(),
			verifyActiveSiteUnchanged(),
			verifyNoSelfFenceAcrossSites(),
		},
	}
}

func injectOperatorKill() runner.Step {
	return runner.Step{
		Phase: runner.PhaseInject,
		Name:  "delete operator pod, open sidecar log tailers",
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
			env.Capture.Note(fmt.Sprintf("active=%s peer=%s; killing operator", active, peer))
			if err := ctxStash(ctx, env, "originalPrimary", active); err != nil {
				return err
			}
			if err := ctxStash(ctx, env, "peerSite", peer); err != nil {
				return err
			}
			// Open sidecar log tailers BEFORE the kill so the SinceTime
			// filter on the tailer covers the entire window we care
			// about (env.StartTime predates inject).
			if _, err := env.Logs("sidecar:" + active); err != nil {
				return fmt.Errorf("open sidecar tailer for %s: %w", active, err)
			}
			if _, err := env.Logs("sidecar:" + peer); err != nil {
				return fmt.Errorf("open sidecar tailer for %s: %w", peer, err)
			}
			return env.Chaos.KillOperator(ctx)
		},
	}
}

func settleAfterOperatorKill() runner.Step {
	return runner.Step{
		Phase: runner.PhaseSettle,
		Name:  "wait 35s past kill (covers leaseTimeout + restart)",
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

func verifyActiveSiteUnchanged() runner.Step {
	return runner.Step{
		Phase: runner.PhaseVerify,
		Name:  "status.activeSite is unchanged",
		Do: func(ctx context.Context, env *runner.Env) error {
			original := ctxFetch(env, "originalPrimary")
			mfg, err := env.Kube.GetMFG(ctx, env.Namespace)
			if err != nil {
				return err
			}
			if mfg.Status.ActiveSite != original {
				return fmt.Errorf("activeSite changed during operator restart: was %q, now %q (lastFailoverTarget=%q)",
					original, mfg.Status.ActiveSite, mfg.Status.LastFailoverTarget)
			}
			// Defense-in-depth: also confirm the cluster still reports
			// exactly one writable site so an in-flight unrelated
			// promotion doesn't slip past the activeSite check.
			var writable []string
			for _, s := range mfg.Status.Sites {
				if s.State == "writable" {
					writable = append(writable, s.Name)
				}
			}
			if len(writable) != 1 || writable[0] != original {
				return fmt.Errorf("expected single writable site %q, observed %v", original, writable)
			}
			env.Capture.Note(fmt.Sprintf("activeSite still %s; writable=%v", original, writable))
			return ensureMFGReady(mfg)
		},
	}
}

func verifyNoSelfFenceAcrossSites() runner.Step {
	return runner.Step{
		Phase: runner.PhaseVerify,
		Name:  "no SELF-FENCING line on either sidecar since scenario start",
		Do: func(ctx context.Context, env *runner.Env) error {
			active := ctxFetch(env, "originalPrimary")
			peer := ctxFetch(env, "peerSite")
			for _, site := range []string{active, peer} {
				tail, err := env.Logs("sidecar:" + site)
				if err != nil {
					return fmt.Errorf("get sidecar tailer for %s: %w", site, err)
				}
				if hit, line := firstMatchSince(tail, env.StartTime, pglogs.Substring("SELF-FENC")); hit {
					return fmt.Errorf("sidecar %s self-fenced during operator restart: %s", site, line)
				}
			}
			return nil
		},
	}
}

func ensureMFGReady(mfg *v1alpha1.MysqlFailoverGroup) error {
	for _, c := range mfg.Status.Conditions {
		if c.Type == "Ready" {
			if c.Status != "True" {
				return fmt.Errorf("Ready condition is %q (reason=%s message=%q)", c.Status, c.Reason, c.Message)
			}
			return nil
		}
	}
	return fmt.Errorf("no Ready condition present on MFG status")
}
