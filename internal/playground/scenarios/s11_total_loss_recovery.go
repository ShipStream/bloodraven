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
	runner.Register(scenario11TotalLossRecovery())
}

// scenario11TotalLossRecovery scales every MySQL site to 0 — the
// playground's only TOTAL-LOSS path — and asserts the operator
// recognises and surfaces it (`ALERT` log with `TOTAL LOSS: all sites
// are unreachable`) without crashing or wedging the reconciler. It
// then scales both back up and asserts the cluster reconverges to a
// single writable + single read-only site.
//
// The operator's resilience here is the property under test: a TOTAL
// LOSS event must not put the controller into a state from which the
// next reconcile cycle cannot recover. Crashes, deadlocks, or
// "stuck" RecoveryBlocked entries would be visible in the
// reconvergence step's polling loop.
func scenario11TotalLossRecovery() runner.Scenario {
	return runner.Scenario{
		ID:    "11-total-loss-recovery",
		Title: "Both sites scaled to 0 — TOTAL LOSS surfaced and recovered",
		Hypothesis: "When every MySQL site is unreachable simultaneously, the operator emits an `ALERT` " +
			"log line containing `TOTAL LOSS: all sites are unreachable` and does NOT crash. After both " +
			"sites are restored, the cluster reconverges to one writable + one read-only with no " +
			"RecoveryBlocked sites.",
		Risk:    "high",
		DocLink: "playground/chaos-scenarios.md#11-simultaneous-both-site-kill",
		Timeout: 6 * time.Minute,
		Precheck: AssertHealthyBaseline,
		Steps: []runner.Step{
			injectScaleAllSitesZero(),
			observeTotalLossAlert(),
			injectScaleAllSitesBackUp(),
			observeTotalLossReconvergence(),
		},
	}
}

func injectScaleAllSitesZero() runner.Step {
	return runner.Step{
		Phase: runner.PhaseInject,
		Name:  "scale every spec.sites MySQL deployment to 0",
		Do: func(ctx context.Context, env *runner.Env) error {
			mfg, err := env.Kube.GetMFG(ctx, env.Namespace)
			if err != nil {
				return err
			}
			// Open the operator log tailer BEFORE the inject so the
			// SinceTime filter covers the moment the alert fires.
			if _, err := env.Logs("operator"); err != nil {
				return fmt.Errorf("open operator tailer: %w", err)
			}
			var sites []string
			for _, s := range mfg.Spec.Sites {
				sites = append(sites, s.Name)
			}
			env.Capture.Note(fmt.Sprintf("scaling all sites to 0: %v", sites))
			for _, s := range sites {
				if err := env.Chaos.ScaleSiteToZero(ctx, s); err != nil {
					return fmt.Errorf("scale %s to 0: %w", s, err)
				}
			}
			return nil
		},
	}
}

func observeTotalLossAlert() runner.Step {
	return runner.Step{
		Phase: runner.PhaseObserve,
		Name:  `wait for operator "TOTAL LOSS: all sites are unreachable" alert`,
		Do: func(ctx context.Context, env *runner.Env) error {
			tail, err := env.Logs("operator")
			if err != nil {
				return err
			}
			// 90s = poll interval (2s) × failureThreshold (3) for both
			// sites + scale-down propagation + a wide margin. The alert
			// usually surfaces within ~15s of both pods going away.
			waitCtx, cancel := context.WithTimeout(ctx, 90*time.Second)
			defer cancel()
			_, err = env.Wait.UntilLog(waitCtx, tail, env.StartTime,
				`operator ALERT line for TOTAL LOSS`,
				pglogs.Substring(`TOTAL LOSS: all sites are unreachable`),
			)
			return err
		},
	}
}

func injectScaleAllSitesBackUp() runner.Step {
	return runner.Step{
		Phase: runner.PhaseInject,
		Name:  "scale every site back to 1 (replay reverters)",
		Do: func(ctx context.Context, env *runner.Env) error {
			// Replay the per-inject reverters now so the recovery
			// happens inside scenario scope. The runner's cleanup-time
			// Revert is then a no-op.
			if err := env.Chaos.Revert(ctx); err != nil {
				return fmt.Errorf("scale sites back up: %w", err)
			}
			env.Capture.Note("sites scaled back to 1; waiting for reconvergence")
			return nil
		},
	}
}

// observeTotalLossReconvergence asserts the operator drives the
// cluster back to a single writable + single read-only site without
// any RecoveryBlocked entries. Without splitBrainPolicy.sitePriorities
// the operator may pick either site as the new primary; what matters
// is that exactly one ends up writable and the other reaches
// read-only with replication running.
func observeTotalLossReconvergence() runner.Step {
	return runner.Step{
		Phase: runner.PhaseObserve,
		Name:  "cluster reconverges to one writable + one read-only after TOTAL LOSS",
		Do: func(ctx context.Context, env *runner.Env) error {
			// 4 minutes: bringing two MySQL pods back up, restarting the
			// replication topology, and re-establishing replication can
			// take noticeably longer than a single-site recovery. The
			// 5s relay-log drain timeout × 2, plus pod-startup time on
			// a busy k3d node, regularly pushes this past 60s.
			waitCtx, cancel := context.WithTimeout(ctx, 4*time.Minute)
			defer cancel()
			_, err := env.Wait.UntilCR(waitCtx, env.Namespace,
				"sites: writable=1 read-only=1 blocked=0 active!=\"\"",
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
					msg := fmt.Sprintf("active=%q writable=%v read-only=%v other=%v blocked=%v",
						mfg.Status.ActiveSite, writable, readOnly, other, blocked)
					done := mfg.Status.ActiveSite != "" && len(writable) == 1 && len(readOnly) == 1 && len(blocked) == 0
					return done, msg, nil
				},
			)
			return err
		},
	}
}
