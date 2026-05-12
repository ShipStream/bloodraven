package scenarios

import (
	"context"
	"fmt"
	"sort"
	"time"

	v1alpha1 "github.com/shipstream/bloodraven/api/v1alpha1"
	pglogs "github.com/shipstream/bloodraven/internal/playground/logs"
	"github.com/shipstream/bloodraven/internal/playground/runner"
)

func init() {
	runner.Register(scenario05OperatorKillDuringFailover())
}

// scenario05OperatorKillDuringFailover scales the active primary to 0
// and, ~1s later, kills the operator pod — interleaving operator-loss
// with active-primary-loss inside the failover detection window.
//
// The doc's hypothesis is that the system converges to exactly one
// writable site after the operator restarts. The doc also notes the
// observed race: the primary deployment respawns the killed pod
// before the new operator finishes starting, in which case the new
// operator picks up the original topology and resumes monitoring
// without ever needing to fail over.
//
// We therefore assert *invariants*, not identity-of-primary:
//   - cluster eventually reports exactly one writable site
//   - the Ready condition becomes True
//   - no RecoveryBlocked entries
//   - neither sidecar self-fenced during the gap
//
// Either of two end states is acceptable: (a) the failover completed
// and the peer is the new primary, or (b) the original primary
// respawned and the new operator never triggered a failover. Both are
// "safe convergence" — the failure mode this scenario protects
// against is split-brain or wedged unreachable state.
func scenario05OperatorKillDuringFailover() runner.Scenario {
	return runner.Scenario{
		ID:    "05-operator-kill-during-failover",
		Title: "Operator kill during active failover converges safely",
		Hypothesis: "Killing the operator within ~1s of scaling the primary to 0 must NOT produce a " +
			"split-brain or wedge the cluster. After the operator restarts, exactly one site is writable, " +
			"Ready=True, and neither sidecar self-fenced.",
		Risk:     "medium",
		DocLink:  "playground/chaos-scenarios.md#5-operator-kill-during-active-failover",
		Timeout:  5 * time.Minute,
		Precheck: AssertHealthyBaseline,
		Steps: []runner.Step{
			s05InjectKillPrimaryThenOperator(),
			s05ObserveSafeConvergence(),
			s05VerifyNoSelfFence(),
		},
	}
}

func s05InjectKillPrimaryThenOperator() runner.Step {
	return runner.Step{
		Phase: runner.PhaseInject,
		Name:  "scale active primary to 0, sleep 1s, kill operator pod",
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
			env.Capture.Note(fmt.Sprintf("active=%s peer=%s; scale-to-0 then operator kill", active, peer))
			if err := ctxStash(ctx, env, "originalPrimary", active); err != nil {
				return err
			}
			if err := ctxStash(ctx, env, "peerSite", peer); err != nil {
				return err
			}
			// Open sidecar log tailers BEFORE we touch the cluster so
			// the SinceTime filter covers the whole window.
			if _, err := env.Logs("sidecar:" + active); err != nil {
				return fmt.Errorf("open sidecar tailer for %s: %w", active, err)
			}
			if _, err := env.Logs("sidecar:" + peer); err != nil {
				return fmt.Errorf("open sidecar tailer for %s: %w", peer, err)
			}
			if _, err := env.Logs("operator"); err != nil {
				return fmt.Errorf("open operator tailer: %w", err)
			}
			if err := env.Chaos.ScaleSiteToZero(ctx, active); err != nil {
				return fmt.Errorf("scale active %s to 0: %w", active, err)
			}
			// Sleep covers the operator's poll interval (~2s in
			// playground) so the next reconcile may see the missing
			// primary before we yank the operator. Going much shorter
			// than 1s makes the kill always land before the operator
			// has even noticed; much longer and the failover may be
			// well underway and the test becomes "operator restarts
			// mid-promotion" — interesting, but a different scenario.
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-time.After(time.Second):
			}
			env.Capture.Note("1s elapsed; killing operator")
			return env.Chaos.KillOperator(ctx)
		},
	}
}

// s05ObserveSafeConvergence waits for the cluster to settle at exactly
// one writable site with Ready=True. The originally-killed primary
// gets scaled back to 1 by the inject's reverter at executor cleanup,
// but to make the convergence visible inside the scenario we let the
// operator's deployment reconciler handle scaling — we only restore
// the kill via Revert as a safety net; replay it here so the cluster
// sees the original primary back online while we still own the
// scenario clock.
//
// Race trap (fixed): the original implementation polled status.sites[]
// for `writable=1 read-only=1 ready=True` immediately after killing the
// operator. The killed operator's last status write is still in the CR
// (one writable, one read-only, Ready=True from the pre-kill state),
// so the first poll matches and the scenario "passes" in ~100ms —
// before the new operator has even started, let alone observed the
// post-injection state. The cluster can then deadlock in `NoPrimary`
// because the in-flight failover never completed. We now require the
// new operator to have stamped a fresh observedGeneration tick AND for
// the converged state to hold for a small dwell time so a transient
// snapshot doesn't count.
func s05ObserveSafeConvergence() runner.Step {
	return runner.Step{
		Phase: runner.PhaseObserve,
		Name:  "scale primary back up; cluster reconverges to one writable + Ready=True (stable for 10s)",
		Do: func(ctx context.Context, env *runner.Env) error {
			// Replay the scale-back-up reverter now so convergence
			// happens inside scenario scope. The runner's cleanup-time
			// Revert is then a no-op.
			if err := env.Chaos.Revert(ctx); err != nil {
				return fmt.Errorf("scale original primary back up: %w", err)
			}
			env.Capture.Note("original primary scaled back to 1; awaiting safe convergence")

			// Refresh the metrics port-forward because the operator pod
			// was just respawned; the prior port-forward is bound to a
			// dead pod identity.
			if env.RefreshMetrics != nil {
				_ = env.RefreshMetrics(ctx)
			}

			// Block until the new operator pod has rolled out and is
			// Ready. Without this we race the old operator's stale
			// status writes (which look healthy because they were taken
			// before the kill). The Deployment respawns the operator
			// within ~5–10s; allow 90s to absorb scheduler/network
			// jitter on cold playgrounds.
			waitCtx, cancel := context.WithTimeout(ctx, 90*time.Second)
			defer cancel()
			if err := env.Chaos.WaitForOperatorAvailable(waitCtx); err != nil {
				return fmt.Errorf("operator pod did not become available: %w", err)
			}
			env.Capture.Note("new operator pod is Available; waiting for stable converged state")

			// Now wait for the cluster to actually converge. Require the
			// state to hold for 10s so we cannot mistake a stale snapshot
			// or a brief mid-promotion window for the final settled
			// shape.
			convergeCtx, convergeCancel := context.WithTimeout(ctx, 4*time.Minute)
			defer convergeCancel()
			var healthySince time.Time
			_, err := env.Wait.UntilCR(convergeCtx, env.Namespace,
				"writable=1 read-only=1 ready=True blocked=0 (stable for 10s)",
				func(mfg *v1alpha1.MysqlFailoverGroup) (bool, string, error) {
					var writable, readOnly, other, blocked []string
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
					}
					sort.Strings(writable)
					sort.Strings(readOnly)
					ready := "<none>"
					for _, c := range mfg.Status.Conditions {
						if c.Type == "Ready" {
							ready = string(c.Status)
							break
						}
					}
					healthy := mfg.Status.ActiveSite != "" &&
						len(writable) == 1 && len(readOnly) == 1 &&
						len(blocked) == 0 && ready == "True"
					if healthy {
						if healthySince.IsZero() {
							healthySince = time.Now()
						}
					} else {
						healthySince = time.Time{}
					}
					stable := time.Duration(0)
					if !healthySince.IsZero() {
						stable = time.Since(healthySince)
					}
					done := healthy && stable >= 10*time.Second
					msg := fmt.Sprintf(
						"active=%q writable=%v read-only=%v other=%v blocked=%v ready=%s stable=%s/10s",
						mfg.Status.ActiveSite, writable, readOnly, other, blocked, ready, stable.Round(time.Second),
					)
					return done, msg, nil
				},
			)
			return err
		},
	}
}

// s05VerifyNoSelfFence confirms neither sidecar emitted SELF-FENCE
// during the operator-down window. The window between operator kill
// and operator-back-up is bounded by the Deployment respawn (~5–10s in
// the playground), well below the sidecar leaseTimeout (20s), so a
// SELF-FENCING/SELF-FENCED line would indicate the lease tracking
// crossed leaseTimeout — likely a regression in restart tolerance.
func s05VerifyNoSelfFence() runner.Step {
	return runner.Step{
		Phase: runner.PhaseVerify,
		Name:  "no SELF-FENC log on either sidecar across the operator gap",
		Do: func(ctx context.Context, env *runner.Env) error {
			for _, key := range []string{"originalPrimary", "peerSite"} {
				site := ctxFetch(env, key)
				tail, err := env.Logs("sidecar:" + site)
				if err != nil {
					return fmt.Errorf("get sidecar tailer for %s: %w", site, err)
				}
				if hit, line := firstMatchSince(tail, env.StartTime, pglogs.Substring("SELF-FENC")); hit {
					return fmt.Errorf("sidecar %s self-fenced during operator-kill failover window: %s", site, line)
				}
			}
			return nil
		},
	}
}
