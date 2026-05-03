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
	runner.Register(scenario15SidecarCrashNoFailover())
}

// scenario15SidecarCrashNoFailover crashes the sidecar container
// (without disturbing the MySQL container) and asserts that the
// kubelet restart cycle is absorbed without triggering a failover or a
// SELF-FENCE. This is the inverse of scenario 01: it locks down the
// "operator must NOT be trigger-happy on a transient sidecar blip"
// property that protects against unnecessary cluster outages every
// time a sidecar pod is rescheduled, OOM-killed, or upgraded.
//
// Mechanism: attach an ephemeral container to the active site's
// MySQL pod that targets the sidecar's PID namespace and runs `kill 1`.
// SIGTERM (the default for `kill`) is the right tool here — the kernel
// silently drops SIGKILL on PID 1 inside a non-init PID namespace, but
// a sane Go binary handles SIGTERM and exits, prompting the kubelet to
// restart just the sidecar container.
//
// MySQL on port 3306 is unaffected because we don't touch the mysql
// container; the operator's reachability probe (which is what drives
// failover decisions, not the sidecar /status probe) keeps succeeding.
func scenario15SidecarCrashNoFailover() runner.Scenario {
	return runner.Scenario{
		ID:    "15-sidecar-crash-no-failover",
		Title: "Sidecar container crash absorbed without failover or self-fence",
		Hypothesis: "Killing PID 1 inside the active site's sidecar container increments " +
			"containerStatus.restartCount but does NOT change activeSite, does NOT emit " +
			"'failover complete', and does NOT emit any SELF-FENC line on the affected sidecar.",
		Risk:     "low",
		DocLink:  "playground/chaos-scenarios.md#15-sidecar-crash-container-kill-not-pod-kill",
		Timeout:  3 * time.Minute,
		Precheck: AssertHealthyBaseline,
		Steps: []runner.Step{
			s15InjectSidecarKill(),
			s15ObserveSidecarRestart(),
			s15SettlePostRestart(),
			s15VerifyActiveSiteUnchanged(),
			s15VerifyNoFailoverLog(),
			s15VerifyNoSelfFenceLog(),
			s15VerifySidecarHealthy(),
		},
	}
}

func s15InjectSidecarKill() runner.Step {
	return runner.Step{
		Phase: runner.PhaseInject,
		Name:  "open log tailers, record baseline restartCount, kill sidecar PID 1",
		Do: func(ctx context.Context, env *runner.Env) error {
			mfg, err := env.Kube.GetMFGNamed(ctx, env.Namespace, env.FG)
			if err != nil {
				return err
			}
			active := mfg.Status.ActiveSite
			env.Capture.Note(fmt.Sprintf("active=%s; killing sidecar PID 1 via ephemeral container", active))
			if err := ctxStash(ctx, env, "originalPrimary", active); err != nil {
				return err
			}
			// Open tailers BEFORE the kill so we have continuous coverage
			// of the operator + sidecar log streams across the restart.
			// The sidecar tailer reads from the kubelet, which keeps
			// streaming across container restarts on the same pod.
			if _, err := env.Logs("operator"); err != nil {
				return err
			}
			if _, err := env.Logs("sidecar:" + active); err != nil {
				return err
			}
			pod, err := env.Kube.GetSiteMysqlPod(ctx, env.Namespace, env.FG, active)
			if err != nil {
				return err
			}
			baseline, err := env.Kube.SidecarRestartCount(ctx, env.Namespace, pod.Name)
			if err != nil {
				return err
			}
			env.Capture.Note(fmt.Sprintf("pod=%s baseline sidecar restartCount=%d", pod.Name, baseline))
			if err := ctxStash(ctx, env, "podName", pod.Name); err != nil {
				return err
			}
			if err := ctxStash(ctx, env, "baselineRestartCount", fmt.Sprintf("%d", baseline)); err != nil {
				return err
			}
			_, err = env.Chaos.KillSidecarPID1(ctx, active)
			return err
		},
	}
}

func s15ObserveSidecarRestart() runner.Step {
	return runner.Step{
		Phase: runner.PhaseObserve,
		Name:  "sidecar restartCount increments by 1",
		Do: func(ctx context.Context, env *runner.Env) error {
			pod := ctxFetch(env, "podName")
			var baseline int32
			if _, err := fmt.Sscanf(ctxFetch(env, "baselineRestartCount"), "%d", &baseline); err != nil {
				return fmt.Errorf("parse baselineRestartCount: %w", err)
			}
			waitCtx, cancel := context.WithTimeout(ctx, 90*time.Second)
			defer cancel()
			tick := time.NewTicker(2 * time.Second)
			defer tick.Stop()
			lastSeen := baseline
			for {
				count, err := env.Kube.SidecarRestartCount(waitCtx, env.Namespace, pod)
				if err == nil {
					lastSeen = count
					if count > baseline {
						env.Capture.Note(fmt.Sprintf("sidecar restartCount %d → %d", baseline, count))
						return nil
					}
				}
				select {
				case <-waitCtx.Done():
					return fmt.Errorf("sidecar restartCount did not increment within 90s (baseline=%d, last=%d): %w", baseline, lastSeen, waitCtx.Err())
				case <-tick.C:
				}
			}
		},
	}
}

// s15SettlePostRestart waits past the operator's failover detection
// window so we can claim "no failover happened" rather than "no
// failover happened YET". Playground config: pollInterval=2s,
// failureThreshold=3 → ~6s to classify a probe as failed; clean
// failover runs ~37s end-to-end. We wait 30s, which is well past the
// detection threshold and short of a full failover cycle. If the
// operator was going to fail over on a sidecar blip, we'd see at
// least the LastFailoverTarget transition by then.
func s15SettlePostRestart() runner.Step {
	return runner.Step{
		Phase: runner.PhaseSettle,
		Name:  "wait 30s past sidecar restart for the operator's detection window to lapse",
		Do: func(ctx context.Context, env *runner.Env) error {
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-time.After(30 * time.Second):
				return nil
			}
		},
	}
}

func s15VerifyActiveSiteUnchanged() runner.Step {
	return runner.Step{
		Phase: runner.PhaseVerify,
		Name:  "status.activeSite unchanged across the sidecar restart",
		Do: func(ctx context.Context, env *runner.Env) error {
			original := ctxFetch(env, "originalPrimary")
			mfg, err := env.Kube.GetMFGNamed(ctx, env.Namespace, env.FG)
			if err != nil {
				return err
			}
			if mfg.Status.ActiveSite != original {
				return fmt.Errorf("activeSite changed during sidecar restart: was %q, now %q (lastFailoverTarget=%q)",
					original, mfg.Status.ActiveSite, mfg.Status.LastFailoverTarget)
			}
			if mfg.Status.LastFailoverTarget != "" && mfg.Status.LastFailoverTarget != original {
				return fmt.Errorf("operator set lastFailoverTarget=%q during sidecar restart — implies an attempted promotion",
					mfg.Status.LastFailoverTarget)
			}
			env.Capture.Note(fmt.Sprintf("activeSite=%q (unchanged)", mfg.Status.ActiveSite))
			return nil
		},
	}
}

func s15VerifyNoFailoverLog() runner.Step {
	return runner.Step{
		Phase: runner.PhaseVerify,
		Name:  `no "failover complete" line in operator log since scenario start`,
		Do: func(ctx context.Context, env *runner.Env) error {
			tail, err := env.Logs("operator")
			if err != nil {
				return err
			}
			if hit, line := firstMatchSince(tail, env.StartTime, pglogs.Substring("failover complete")); hit {
				return fmt.Errorf("operator emitted failover during sidecar restart: %s", line)
			}
			return nil
		},
	}
}

func s15VerifyNoSelfFenceLog() runner.Step {
	return runner.Step{
		Phase: runner.PhaseVerify,
		Name:  "no SELF-FENC line on the affected sidecar since scenario start",
		Do: func(ctx context.Context, env *runner.Env) error {
			active := ctxFetch(env, "originalPrimary")
			tail, err := env.Logs("sidecar:" + active)
			if err != nil {
				return err
			}
			if hit, line := firstMatchSince(tail, env.StartTime, pglogs.Substring("SELF-FENC")); hit {
				return fmt.Errorf("sidecar on %s self-fenced during restart cycle: %s", active, line)
			}
			return nil
		},
	}
}

// s15VerifySidecarHealthy probes the post-restart sidecar /status to
// confirm the new container is up and reporting role=primary,
// read_only=false. This catches a regression where the sidecar
// restarts but comes back in a degraded state (e.g. fencing timer
// stuck, peer probe wedged). UntilSidecarStatus polls so we tolerate
// the brief unreachable window while the sidecar HTTP listener is
// re-bound.
func s15VerifySidecarHealthy() runner.Step {
	return runner.Step{
		Phase: runner.PhaseVerify,
		Name:  "post-restart sidecar /status reports role=primary, read_only=false",
		Do: func(ctx context.Context, env *runner.Env) error {
			active := ctxFetch(env, "originalPrimary")
			probe, err := env.Sidecar(active)
			if err != nil {
				return fmt.Errorf("open sidecar probe for %s: %w", active, err)
			}
			waitCtx, cancel := context.WithTimeout(ctx, 60*time.Second)
			defer cancel()
			return env.Wait.UntilSidecarStatus(waitCtx, probe,
				fmt.Sprintf("site %s sidecar back to role=primary, read_only=false", active),
				func(st *pgsidecar.StatusResponse) (bool, string) {
					msg := fmt.Sprintf("role=%s read_only=%v uptime=%d", st.Role, st.ReadOnly, st.Uptime)
					return st.Role == "primary" && !st.ReadOnly, msg
				},
			)
		},
	}
}
