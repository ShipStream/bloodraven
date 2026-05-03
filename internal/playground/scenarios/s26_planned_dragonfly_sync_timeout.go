package scenarios

import (
	"context"
	"fmt"
	"time"

	v1alpha1 "github.com/shipstream/bloodraven/api/v1alpha1"
	pgmetrics "github.com/shipstream/bloodraven/internal/playground/metrics"
	"github.com/shipstream/bloodraven/internal/playground/runner"
)

func init() {
	runner.Register(scenario26PlannedDragonflySyncTimeout())
}

// scenario26PlannedDragonflySyncTimeout drives the
// WaitingForDragonflySync timeout branch deterministically by
// patching maxSyncWait=1ms and scaling the failover target's Dragonfly
// StatefulSet to 0 before annotating the planned-failover trigger.
//
// Asserts:
//   - PlannedFailoverStatus reaches Succeeded with target=peer
//   - status.activeSite (MySQL) flipped to target
//   - status.plannedFailover.dragonfly.SessionsPreserved == false
//     (the headline contract for D4: a Dragonfly sync timeout MUST
//     mark sessions as not preserved, never silently masquerade as
//     a clean session-preserving failover)
//   - status.plannedFailover.dragonfly.Reason is one of the documented
//     timeout/failure reasons (DragonflySyncTimeout when the
//     WaitingForDragonflySync handler fired the policy branch first;
//     DragonflyPromotionFailed when we instead reached PromotingDragonfly
//     and REPLTAKEOVER could not dial the scaled-to-zero target)
//   - bloodraven_dragonfly_promotions_total{result="failed"} >= 1
//
// Mapped to PLANS-Dragonfly-Chaos-Scenarios.md scenario D4.
//
// Settle: the source's Dragonfly server-side state is left as a
// stale-master with master_repl_offset > 0, which would otherwise
// fail the auto-rejoin gate (connected_slaves=0 AND master_repl_offset=0)
// and leave the cluster permanently degraded for the next scenario.
// We force-respawn the source pod so it comes up empty and the
// DragonflyManager reconciles it as a fresh replica of the new
// active master.
func scenario26PlannedDragonflySyncTimeout() runner.Scenario {
	return runner.Scenario{
		ID:    "26-planned-dragonfly-sync-timeout-proceed",
		Title: "Planned failover with Dragonfly sync timeout (onSyncTimeout=proceed) marks sessions lost but still succeeds",
		Hypothesis: "Patching maxSyncWait=1ms and scaling the target's Dragonfly StatefulSet to 0 forces both " +
			"the WaitingForDragonflySync target-poll and the PromotingDragonfly REPLTAKEOVER to fail. " +
			"With onSyncTimeout=proceed, the operator stamps SessionsPreserved=false, completes MySQL " +
			"failover anyway, and increments dragonfly_promotions_total{result=\"failed\"}.",
		Risk:     "medium",
		DocLink:  "PLANS-Dragonfly-Chaos-Scenarios.md (D4)",
		Timeout:  5 * time.Minute,
		Precheck: AssertDragonflyHealthyBaseline,
		Steps: []runner.Step{
			snapshotForS26(),
			tightenS26SyncBudget(),
			scaleS26TargetDragonflyToZero(),
			injectS26PlannedFailover(),
			observeS26PlannedFailoverSucceeded(),
			verifyS26MysqlActiveSiteFlipped(),
			verifyS26SessionsNotPreserved(),
			verifyS26PromotionsFailedMetric(),
			settleS26SourceDragonflyRejoinsAsReplica(),
		},
	}
}

// snapshotForS26 captures the active site (= the planned-failover
// source) and its peer (= the target) before any chaos is applied.
// Using stash keys distinct from s22's "switchoverTarget" / s24's
// "originalPrimary" keeps every Dragonfly scenario's per-run state
// independent.
func snapshotForS26() runner.Step {
	return runner.Step{
		Phase: runner.PhasePrecheck,
		Name:  "snapshot active+target sites",
		Do: func(ctx context.Context, env *runner.Env) error {
			mfg, err := env.Kube.GetMFG(ctx, env.Namespace)
			if err != nil {
				return err
			}
			active := mfg.Status.ActiveSite
			if active == "" {
				return fmt.Errorf("no active MySQL site")
			}
			peer, err := PeerOf(mfg, active)
			if err != nil {
				return err
			}
			if err := ctxStash(ctx, env, "s26Source", active); err != nil {
				return err
			}
			return ctxStash(ctx, env, "s26Target", peer)
		},
	}
}

// tightenS26SyncBudget patches the MFG so any non-trivial wait in
// WaitingForDragonflySync trips the timeout branch on the first
// post-offset-capture reconcile (poll interval is 1s, so 1ms is
// always behind). The reverter restores the original 30s/proceed
// values from the playground manifest.
func tightenS26SyncBudget() runner.Step {
	return runner.Step{
		Phase: runner.PhaseInject,
		Name:  "patch maxSyncWait=1ms onSyncTimeout=proceed",
		Do: func(ctx context.Context, env *runner.Env) error {
			env.Capture.Note("patching dragonfly sync budget to 1ms / proceed")
			return env.Chaos.PatchDragonflySyncBudget(ctx, "1ms", "proceed")
		},
	}
}

// scaleS26TargetDragonflyToZero is the deterministic timeout
// injection: with the target Dragonfly StatefulSet at 0 replicas,
// both the WaitingForDragonflySync poll-target step and the later
// PromotingDragonfly dial-target step fail with "no endpoints", which
// independently is enough to drive the SessionsPreserved=false path.
// The 1ms budget from the prior step adds defense-in-depth in case
// the operator manages to dial mid-scale-down.
func scaleS26TargetDragonflyToZero() runner.Step {
	return runner.Step{
		Phase: runner.PhaseInject,
		Name:  "scale target dragonfly StatefulSet to 0",
		Do: func(ctx context.Context, env *runner.Env) error {
			target := ctxFetch(env, "s26Target")
			env.Capture.Note(fmt.Sprintf("scale dragonfly %s to 0 (forces target unreachable)", target))
			return env.Chaos.ScaleDragonflyToZero(ctx, target)
		},
	}
}

func injectS26PlannedFailover() runner.Step {
	return runner.Step{
		Phase: runner.PhaseInject,
		Name:  "annotate planned-failover with target site",
		Do: func(ctx context.Context, env *runner.Env) error {
			target := ctxFetch(env, "s26Target")
			env.Capture.Note(fmt.Sprintf("planned-failover -> %s with dragonfly target down", target))
			return env.Chaos.AnnotatePlannedFailover(ctx, target)
		},
	}
}

// observeS26PlannedFailoverSucceeded waits for the in-flight planned
// failover to reach Succeeded. Mirrors observePlannedFailoverSucceeded
// (s02) but reads the s26-namespaced stash and uses a 4-minute
// budget — the WaitingForDragonflySync poll runs at 1s ticks plus the
// MySQL phases that follow, so this scenario is bounded by MySQL
// promotion latency rather than the Dragonfly budget.
func observeS26PlannedFailoverSucceeded() runner.Step {
	return runner.Step{
		Phase: runner.PhaseObserve,
		Name:  "PlannedFailoverStatus reaches Succeeded",
		Do: func(ctx context.Context, env *runner.Env) error {
			target := ctxFetch(env, "s26Target")
			waitCtx, cancel := context.WithTimeout(ctx, 4*time.Minute)
			defer cancel()
			_, err := env.Wait.UntilCR(waitCtx, env.Namespace,
				fmt.Sprintf("plannedFailover.phase==Succeeded with target=%s", target),
				func(mfg *v1alpha1.MysqlFailoverGroup) (bool, string, error) {
					pf := mfg.Status.PlannedFailover
					if pf == nil {
						return false, "no plannedFailover status yet", nil
					}
					staleCutoff := env.StartTime.Add(-2 * time.Second)
					if pf.StartTime == nil || pf.StartTime.Time.Before(staleCutoff) {
						return false, fmt.Sprintf("ignoring stale plannedFailover (startTime=%v, scenario startTime=%v)",
							pf.StartTime, env.StartTime), nil
					}
					msg := fmt.Sprintf("phase=%q target=%s reason=%q", pf.Phase, pf.Target, pf.Reason)
					if pf.Phase == v1alpha1.PlannedFailoverPhaseFailed {
						return false, msg, fmt.Errorf("planned failover entered Failed: %s (%s)", pf.Reason, pf.Message)
					}
					if pf.Phase == v1alpha1.PlannedFailoverPhaseSucceeded {
						if pf.Target != target {
							return false, msg, fmt.Errorf("plannedFailover succeeded but target=%q want %q", pf.Target, target)
						}
						return true, msg, nil
					}
					return false, msg, nil
				},
			)
			return err
		},
	}
}

func verifyS26MysqlActiveSiteFlipped() runner.Step {
	return runner.Step{
		Phase: runner.PhaseVerify,
		Name:  "MySQL status.activeSite flipped to target",
		Do: func(ctx context.Context, env *runner.Env) error {
			target := ctxFetch(env, "s26Target")
			mfg, err := env.Kube.GetMFG(ctx, env.Namespace)
			if err != nil {
				return err
			}
			if mfg.Status.ActiveSite != target {
				return fmt.Errorf("status.activeSite=%q want %q (Dragonfly degradation must NOT block MySQL failover)",
					mfg.Status.ActiveSite, target)
			}
			return nil
		},
	}
}

// verifyS26SessionsNotPreserved is the headline assertion for D4.
// A timeout/failure must never silently masquerade as a clean
// session-preserving failover, so SessionsPreserved is checked
// against an explicit `false`. The Reason field is checked against
// the two documented values for this code path: DragonflySyncTimeout
// (the WaitingForDragonflySync handler fired its policy branch first)
// or DragonflyPromotionFailed (the state advanced to PromotingDragonfly
// and REPLTAKEOVER then failed against the scaled-to-zero target).
// Either is correct; a third value would mean a new untested code
// path is now reachable.
func verifyS26SessionsNotPreserved() runner.Step {
	return runner.Step{
		Phase: runner.PhaseVerify,
		Name:  "plannedFailover.dragonfly: SessionsPreserved=false with documented timeout/failure reason",
		Do: func(ctx context.Context, env *runner.Env) error {
			mfg, err := env.Kube.GetMFG(ctx, env.Namespace)
			if err != nil {
				return err
			}
			pf := mfg.Status.PlannedFailover
			if pf == nil || pf.Dragonfly == nil {
				return fmt.Errorf("status.plannedFailover.dragonfly missing on Succeeded run")
			}
			if pf.Dragonfly.SessionsPreserved == nil || *pf.Dragonfly.SessionsPreserved {
				return fmt.Errorf("SessionsPreserved=%v want false (reason=%q msg=%q)",
					pf.Dragonfly.SessionsPreserved, pf.Dragonfly.Reason, pf.Dragonfly.Message)
			}
			switch pf.Dragonfly.Reason {
			case "DragonflySyncTimeout", "DragonflyPromotionFailed":
				return nil
			default:
				return fmt.Errorf("dragonfly.Reason=%q want DragonflySyncTimeout or DragonflyPromotionFailed (msg=%q)",
					pf.Dragonfly.Reason, pf.Dragonfly.Message)
			}
		},
	}
}

// verifyS26PromotionsFailedMetric asserts the promotion-failure
// counter incremented for the target. The metric is the same shape as
// scenario 22's success counter; the result label is the only
// difference. Either dragonflyPromoteFailHandler (REPLTAKEOVER dial
// failure) directly increments {result="failed"}, or the same handler
// is invoked via dragonflySyncTimeoutHandler → PromotingDragonfly →
// failed dial — both paths converge on this metric.
func verifyS26PromotionsFailedMetric() runner.Step {
	return runner.Step{
		Phase: runner.PhaseVerify,
		Name:  `bloodraven_dragonfly_promotions_total{result="failed"} >= 1`,
		Do: func(ctx context.Context, env *runner.Env) error {
			target := ctxFetch(env, "s26Target")
			waitCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
			defer cancel()
			return env.Wait.UntilMetric(waitCtx, env.Metrics,
				fmt.Sprintf(`dragonfly_promotions_total{target_site=%q,result="failed"} >= 1`, target),
				func(snap *pgmetrics.Snapshot) (bool, string) {
					v, _ := snap.Counter("bloodraven_dragonfly_promotions_total", map[string]string{
						"target_site": target,
						"result":      "failed",
					})
					return v >= 1, fmt.Sprintf("counter=%g", v)
				},
			)
		},
	}
}

// settleS26SourceDragonflyRejoinsAsReplica force-deletes the source's
// Dragonfly pod so it comes up empty (master_repl_offset=0,
// connected_slaves=0) and the DragonflyManager auto-rejoin gate fires
// (REPLICAOF target). Without this step the source would otherwise
// be classified as a stale-master forever — its in-memory state from
// before the failover carries master_repl_offset > 0, which the
// auto-rejoin gate refuses to touch on purpose. The next scenario's
// AssertHealthyBaseline would then trip on "expected 1 dragonfly
// master, got 2" or "site is unreachable".
//
// PhaseSettle (not PhaseCleanup) so a wait failure is reported as a
// scenario failure rather than a cleanup-only error. The 4-minute
// budget covers pod respawn + REPLICAOF + initial sync; this timeout
// path can briefly stall status polling while the operator is also
// reconciling MySQL recovery from the planned failover.
func settleS26SourceDragonflyRejoinsAsReplica() runner.Step {
	return runner.Step{
		Phase: runner.PhaseSettle,
		Name:  "force-respawn source dragonfly so it rejoins as replica",
		Do: func(ctx context.Context, env *runner.Env) error {
			source := ctxFetch(env, "s26Source")
			env.Capture.Note(fmt.Sprintf("force-respawn dragonfly on %s (clears stale-master state)", source))
			if err := env.Chaos.KillDragonflyPod(ctx, source); err != nil {
				return fmt.Errorf("kill source dragonfly pod: %w", err)
			}
			waitCtx, cancel := context.WithTimeout(ctx, 4*time.Minute)
			defer cancel()
			_, err := env.Wait.UntilCR(waitCtx, env.Namespace,
				fmt.Sprintf("dragonfly site %s rejoined as healthy replica", source),
				func(mfg *v1alpha1.MysqlFailoverGroup) (bool, string, error) {
					if mfg.Status.Dragonfly == nil {
						return false, "no status.dragonfly", nil
					}
					for _, s := range mfg.Status.Dragonfly.Sites {
						if s.Name == source {
							msg := fmt.Sprintf("site %s role=%q linkStatus=%q reachable=%v sync=%v",
								s.Name, s.Role, s.LinkStatus, s.Reachable, s.SyncInProgress)
							done := s.Role == v1alpha1.DragonflyRoleReplica &&
								s.LinkStatus == "up" &&
								!s.SyncInProgress &&
								s.Reachable
							return done, msg, nil
						}
					}
					return false, fmt.Sprintf("site %s not in status.dragonfly.sites yet", source), nil
				},
			)
			return err
		},
	}
}
