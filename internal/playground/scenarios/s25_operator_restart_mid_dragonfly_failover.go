package scenarios

import (
	"context"
	"fmt"
	"time"

	v1alpha1 "github.com/shipstream/bloodraven/api/v1alpha1"
	"github.com/shipstream/bloodraven/internal/playground/runner"
)

func init() {
	runner.Register(scenario25OperatorRestartMidDragonflyFailover())
}

// scenario25OperatorRestartMidDragonflyFailover validates the
// resume-from-CR-status contract for planned MySQL+Dragonfly
// failovers: the operator process can be killed mid-flight and the
// fresh operator must pick up the work-in-progress and drive it to
// terminal Succeeded.
//
// Mapped to PLANS-Dragonfly-Chaos-Scenarios.md scenario D11. The
// scenario deliberately targets the WaitingForDragonflySync phase
// because it is the longest "waiting" window on the planned-failover
// path (bounded by spec.dragonfly.plannedFailover.maxSyncWait, default
// 30s) AND because a fresh-replica capture inside that phase requires
// at least one round-trip per reconcile pass (status init, source
// offset capture, then sync-readiness polling) — giving the kill
// loop multiple seconds of valid race window even on an idle cluster
// where the actual catch-up is near-instant.
//
// Acceptance contract (the resume invariant):
//   - plannedFailover.phase eventually reaches Succeeded with the
//     pre-kill target site (the new operator MUST NOT silently swap
//     targets on resume — that would mask a load-balancing bug)
//   - status.activeSite (MySQL) flips to the target
//   - status.dragonfly.activeSite flips to the target
//   - status.dragonfly.phase returns to Ready (no permanent split-
//     brain, stale-master, or degraded site after the restart)
func scenario25OperatorRestartMidDragonflyFailover() runner.Scenario {
	return runner.Scenario{
		ID:    "25-operator-restart-mid-dragonfly-failover",
		Title: "Operator restart mid-Dragonfly-failover resumes from CR status",
		Hypothesis: "Killing the operator pod after plannedFailover.phase==WaitingForDragonflySync and " +
			"before Promoting causes the respawned operator to pick up the in-flight planned failover from CR " +
			"status and drive it to Succeeded. MySQL and Dragonfly active sites both end on the original target; " +
			"status.dragonfly.phase converges to Ready.",
		Risk:    "medium",
		DocLink: "PLANS-Dragonfly-Chaos-Scenarios.md (D11)",
		Timeout: 6 * time.Minute,
		Precheck: AssertHealthyBaseline,
		Steps: []runner.Step{
			injectPlannedFailoverForOperatorRestart(),
			killOperatorWhenWaitingForDragonflySync(),
			observePlannedFailoverConverges(),
			verifyMysqlAndDragonflyActiveFlipped(),
			verifyDragonflyReadyAfterResume(),
		},
	}
}

// injectPlannedFailoverForOperatorRestart computes the failover target
// and stamps the planned-failover annotation. The target is stashed for
// the verify steps; the original primary is stashed so the assertions
// can confirm the active site actually moved (not just appeared
// unchanged because the operator never started the work).
func injectPlannedFailoverForOperatorRestart() runner.Step {
	return runner.Step{
		Phase: runner.PhaseInject,
		Name:  "annotate planned-failover with peer site",
		Do: func(ctx context.Context, env *runner.Env) error {
			mfg, err := env.Kube.GetMFG(ctx, env.Namespace)
			if err != nil {
				return err
			}
			if mfg.Status.ActiveSite == "" {
				return fmt.Errorf("no active site on MFG status")
			}
			peer, err := PeerOf(mfg, mfg.Status.ActiveSite)
			if err != nil {
				return err
			}
			if err := ctxStash(ctx, env, "originalPrimary", mfg.Status.ActiveSite); err != nil {
				return err
			}
			if err := ctxStash(ctx, env, "switchoverTarget", peer); err != nil {
				return err
			}
			env.Capture.Note(fmt.Sprintf("planned switchover (D11): %s -> %s", mfg.Status.ActiveSite, peer))
			return env.Chaos.AnnotatePlannedFailover(ctx, peer)
		},
	}
}

// killOperatorWhenWaitingForDragonflySync polls the MFG at a tight
// 100ms cadence until plannedFailover.phase is one of the Dragonfly-
// preparation phases (WaitingForDragonflySync or PromotingDragonfly),
// then immediately deletes the operator pod. Using a custom polling
// loop rather than env.Wait.UntilCR because the latter's default 1s
// interval is too coarse for this race: if the cluster is idle, the
// phase can transit through WaitingForDragonflySync in well under a
// second and a 1s tick may miss the window entirely.
//
// PromotingDragonfly is included as a fallback because if the kill
// races into that phase the resume invariant still holds — REPLTAKEOVER
// is idempotent at the controller level (the resumed operator
// re-enters the same handler, observes the partially-promoted state,
// and converges).
//
// Hard deadline: 60s. The plan-side budget at WaitingForDragonflySync
// entry is maxSyncWait (default 30s); past that the operator advances
// per onSyncTimeout policy. 60s gives a 2x margin for cold reconciler
// caches at scenario start.
func killOperatorWhenWaitingForDragonflySync() runner.Step {
	return runner.Step{
		Phase: runner.PhaseInject,
		Name:  "kill operator pod once plannedFailover enters Dragonfly-prep phase",
		Do: func(ctx context.Context, env *runner.Env) error {
			deadline := time.Now().Add(60 * time.Second)
			tick := time.NewTicker(100 * time.Millisecond)
			defer tick.Stop()
			var lastPhase v1alpha1.PlannedFailoverPhase
			for {
				mfg, err := env.Kube.GetMFG(ctx, env.Namespace)
				if err != nil {
					return fmt.Errorf("get MFG while waiting for Dragonfly-prep phase: %w", err)
				}
				if pf := mfg.Status.PlannedFailover; pf != nil {
					lastPhase = pf.Phase
					switch pf.Phase {
					case v1alpha1.PlannedFailoverPhaseWaitingForDragonflySync,
						v1alpha1.PlannedFailoverPhasePromotingDragonfly:
						env.Capture.Note(fmt.Sprintf("observed phase=%q; killing operator now", pf.Phase))
						return env.Chaos.KillOperator(ctx)
					case v1alpha1.PlannedFailoverPhasePromoting,
						v1alpha1.PlannedFailoverPhaseResuming,
						v1alpha1.PlannedFailoverPhaseSucceeded,
						v1alpha1.PlannedFailoverPhaseFailed:
						return fmt.Errorf("planned failover advanced past Dragonfly-prep before the kill could fire (phase=%q) — "+
							"either Dragonfly is disabled, maxSyncWait is too small, or the 100ms poll missed the window; "+
							"drop the tick interval or fire the kill from the WaitingForLag predicate", pf.Phase)
					}
				}
				if time.Now().After(deadline) {
					return fmt.Errorf("plannedFailover did not enter WaitingForDragonflySync within 60s (last phase=%q)", lastPhase)
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

// observePlannedFailoverConverges waits for the resumed operator to
// drive the in-flight planned failover to Succeeded with the
// originally-requested target. A 4-minute deadline covers operator
// restart (~10s in this environment), CR-status re-read, and the
// remainder of the failover state machine (DrainingApps through
// Resuming).
//
// Re-uses the stale-cutoff guard from observePlannedFailoverSucceeded
// so a leftover pf block from a prior scenario can't satisfy the
// predicate.
func observePlannedFailoverConverges() runner.Step {
	return runner.Step{
		Phase: runner.PhaseObserve,
		Name:  "plannedFailover.phase reaches Succeeded after operator restart",
		Do: func(ctx context.Context, env *runner.Env) error {
			target := ctxFetch(env, "switchoverTarget")
			waitCtx, cancel := context.WithTimeout(ctx, 4*time.Minute)
			defer cancel()
			_, err := env.Wait.UntilCR(waitCtx, env.Namespace,
				fmt.Sprintf("plannedFailover.phase==Succeeded with target=%s after operator kill", target),
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
						return false, msg, fmt.Errorf("planned failover entered Failed after operator restart: %s (%s)",
							pf.Reason, pf.Message)
					}
					if pf.Phase == v1alpha1.PlannedFailoverPhaseSucceeded {
						if pf.Target != target {
							return false, msg, fmt.Errorf("plannedFailover succeeded but target=%q want %q "+
								"(operator silently swapped targets on resume)", pf.Target, target)
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

// verifyMysqlAndDragonflyActiveFlipped confirms the resume actually
// produced the desired observable side effects: both MySQL and
// Dragonfly active sites moved to the target. This is intentionally
// a separate step from observePlannedFailoverConverges so the runner's
// failure.txt pinpoints which side of the cutover regressed.
func verifyMysqlAndDragonflyActiveFlipped() runner.Step {
	return runner.Step{
		Phase: runner.PhaseVerify,
		Name:  "MySQL and Dragonfly active sites both flipped to target",
		Do: func(ctx context.Context, env *runner.Env) error {
			target := ctxFetch(env, "switchoverTarget")
			mfg, err := env.Kube.GetMFG(ctx, env.Namespace)
			if err != nil {
				return err
			}
			if mfg.Status.ActiveSite != target {
				return fmt.Errorf("MySQL status.activeSite=%q want %q", mfg.Status.ActiveSite, target)
			}
			if mfg.Status.Dragonfly == nil {
				return fmt.Errorf("status.dragonfly missing on Succeeded planned failover")
			}
			if mfg.Status.Dragonfly.ActiveSite != target {
				return fmt.Errorf("dragonfly status.activeSite=%q want %q (cutover skewed between MySQL and Dragonfly after resume)",
					mfg.Status.Dragonfly.ActiveSite, target)
			}
			return nil
		},
	}
}

// verifyDragonflyReadyAfterResume confirms the resumed operator left
// no half-promoted, stale-master, or otherwise degraded Dragonfly
// state behind. Polls up to 90s because the resumed operator may need
// extra reconcile passes to drive the old master back into a healthy
// replica role (REPLICAOF wiring + first INFO replication scrape).
func verifyDragonflyReadyAfterResume() runner.Step {
	return runner.Step{
		Phase: runner.PhaseVerify,
		Name:  "status.dragonfly.phase converges to Ready after resume",
		Do: func(ctx context.Context, env *runner.Env) error {
			target := ctxFetch(env, "switchoverTarget")
			waitCtx, cancel := context.WithTimeout(ctx, 90*time.Second)
			defer cancel()
			_, err := env.Wait.UntilCR(waitCtx, env.Namespace,
				"dragonfly.phase==Ready with single master on target after resume",
				func(mfg *v1alpha1.MysqlFailoverGroup) (bool, string, error) {
					if mfg.Status.Dragonfly == nil {
						return false, "no status.dragonfly", nil
					}
					df := mfg.Status.Dragonfly
					if df.Phase != v1alpha1.DragonflyPhaseReady {
						return false, fmt.Sprintf("dragonfly.phase=%q (want Ready)", df.Phase), nil
					}
					masters := 0
					var masterSite string
					for _, s := range df.Sites {
						if s.Role == v1alpha1.DragonflyRoleMaster {
							masters++
							masterSite = s.Name
						}
					}
					if masters != 1 {
						return false, fmt.Sprintf("expected 1 dragonfly master, got %d", masters), nil
					}
					if masterSite != target {
						return false, "", fmt.Errorf("dragonfly master is %q but expected target %q (resume left stale master)",
							masterSite, target)
					}
					return true, fmt.Sprintf("dragonfly.phase=Ready master=%s", masterSite), nil
				},
			)
			return err
		},
	}
}
