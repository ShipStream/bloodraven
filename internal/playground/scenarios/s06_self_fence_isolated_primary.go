package scenarios

import (
	"context"
	"fmt"
	"time"

	pglogs "github.com/shipstream/bloodraven/internal/playground/logs"
	"github.com/shipstream/bloodraven/internal/playground/runner"
	pgsidecar "github.com/shipstream/bloodraven/internal/playground/sidecar"
)

func init() {
	runner.Register(scenario06SelfFenceIsolatedPrimary())
}

// scenario06SelfFenceIsolatedPrimary scales the operator and the peer
// MySQL site to 0 replicas, leaving the active primary unable to talk
// to either Bloodraven or any peer sidecar. After leaseTimeout (20s in
// the playground) the primary's sidecar must self-fence — set
// super_read_only=ON and emit the SELF-FENCED log line — under the
// "every peer unreachable beyond lease timeout" backstop rule.
//
// This complements 09-network-partition-self-fence: that scenario
// triggers the same invariant via NetworkPolicy (pod-network
// isolation), while this one triggers it via the cleaner "true
// isolation" path (no operator, no peer to compare with). Both paths
// must produce the same self-fence outcome; running both protects
// against bugs that "fix" one and accidentally break the other.
func scenario06SelfFenceIsolatedPrimary() runner.Scenario {
	return runner.Scenario{
		ID:    "06-self-fence-isolated-primary",
		Title: "Isolated primary self-fences when operator and peer are gone",
		Hypothesis: "With the operator scaled to 0 AND the peer site scaled to 0, the active primary's " +
			"sidecar exceeds leaseTimeout with no reachable Bloodraven and no reachable peer, sets " +
			"super_read_only=ON, and logs SELF-FENCED.",
		Risk:     "medium",
		DocLink:  "playground/chaos-scenarios.md#6-self-fencing-validation",
		Timeout:  4 * time.Minute,
		Precheck: AssertHealthyBaseline,
		Steps: []runner.Step{
			injectIsolatePrimary(),
			observePrimarySelfFence(),
			verifyPrimarySuperReadOnly(),
		},
		Cleanup: restoreSelfFencedPrimary,
	}
}

func injectIsolatePrimary() runner.Step {
	return runner.Step{
		Phase: runner.PhaseInject,
		Name:  "open tailers, scale operator and peer to 0",
		Do: func(ctx context.Context, env *runner.Env) error {
			mfg, err := env.Kube.GetMFGNamed(ctx, env.Namespace, env.FG)
			if err != nil {
				return err
			}
			active := mfg.Status.ActiveSite
			peer, err := PeerOf(mfg, active)
			if err != nil {
				return err
			}
			env.Capture.Note(fmt.Sprintf("active=%s peer=%s; isolating primary by killing operator+peer", active, peer))
			if err := ctxStash(ctx, env, "primarySite", active); err != nil {
				return err
			}
			if err := ctxStash(ctx, env, "peerSite", peer); err != nil {
				return err
			}
			// Open tailer + sidecar probe BEFORE injection. The primary
			// pod stays running, so the port-forward survives — but
			// opening it after isolation would race the sidecar's
			// fence-time connection-kill behavior.
			if _, err := env.Logs("sidecar:" + active); err != nil {
				return fmt.Errorf("open sidecar tailer for %s: %w", active, err)
			}
			if _, err := env.Sidecar(active); err != nil {
				return fmt.Errorf("open sidecar probe for %s: %w", active, err)
			}
			// Order matters: kill the peer first so the primary's
			// peer-check timer starts ticking, then kill the operator.
			// Reverters are LIFO, so cleanup brings the operator back
			// before the peer — closer to a normal recovery shape than
			// the reverse.
			if err := env.Chaos.ScaleSiteToZero(ctx, peer); err != nil {
				return err
			}
			if err := env.Chaos.ScaleOperatorToZero(ctx); err != nil {
				return err
			}
			return nil
		},
	}
}

func observePrimarySelfFence() runner.Step {
	return runner.Step{
		Phase: runner.PhaseObserve,
		Name:  `wait for "SELF-FENCED" line in primary sidecar log`,
		Do: func(ctx context.Context, env *runner.Env) error {
			site := ctxFetch(env, "primarySite")
			tail, err := env.Logs("sidecar:" + site)
			if err != nil {
				return err
			}
			// 90s = leaseTimeout (20s) + scale propagation + a wide
			// margin for slow CI nodes. The fence usually fires within
			// 25–30s of the inject step.
			waitCtx, cancel := context.WithTimeout(ctx, 90*time.Second)
			defer cancel()
			_, err = env.Wait.UntilLog(waitCtx, tail, env.StartTime,
				`primary sidecar SELF-FENCED line`,
				pglogs.Substring(`SELF-FENCED`),
			)
			return err
		},
	}
}

func verifyPrimarySuperReadOnly() runner.Step {
	return runner.Step{
		Phase: runner.PhaseVerify,
		Name:  "primary sidecar /status reports super_read_only=true",
		Do: func(ctx context.Context, env *runner.Env) error {
			site := ctxFetch(env, "primarySite")
			probe, err := env.Sidecar(site)
			if err != nil {
				return fmt.Errorf("open sidecar probe for %s: %w", site, err)
			}
			waitCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
			defer cancel()
			return env.Wait.UntilSidecarStatus(waitCtx, probe,
				fmt.Sprintf("site %s super_read_only=true and read_only=true", site),
				func(st *pgsidecar.StatusResponse) (bool, string) {
					msg := fmt.Sprintf("super_read_only=%v read_only=%v role=%s",
						st.SuperReadOnly, st.ReadOnly, st.Role)
					return st.SuperReadOnly && st.ReadOnly, msg
				},
			)
		},
	}
}

func restoreSelfFencedPrimary(ctx context.Context, env *runner.Env) error {
	site := ctxFetch(env, "primarySite")
	if site == "" {
		return nil
	}
	waitCtx, cancel := context.WithTimeout(ctx, 90*time.Second)
	defer cancel()
	var lastErr error
	for {
		client, err := env.MySQL(site)
		if err == nil {
			if _, err = client.Exec(waitCtx, "SET GLOBAL super_read_only=OFF"); err == nil {
				_, err = client.Exec(waitCtx, "SET GLOBAL read_only=OFF")
			}
		}
		if err == nil {
			env.Capture.Note(fmt.Sprintf("cleanup: restored self-fenced primary %s to writable", site))
			return nil
		}
		lastErr = err
		select {
		case <-waitCtx.Done():
			return fmt.Errorf("cleanup: restore self-fenced primary %s: %w (last error: %v)", site, waitCtx.Err(), lastErr)
		case <-time.After(2 * time.Second):
		}
	}
}
