package scenarios

import (
	"context"
	"fmt"
	"sort"
	"time"

	v1alpha1 "github.com/shipstream/bloodraven/api/v1alpha1"
	pgmetrics "github.com/shipstream/bloodraven/internal/playground/metrics"
	pgmysql "github.com/shipstream/bloodraven/internal/playground/mysql"
	"github.com/shipstream/bloodraven/internal/playground/runner"
)

func init() {
	runner.Register(scenario02PlannedSwitchover())
}

// scenario02PlannedSwitchover annotates the MFG with the planned-failover
// trigger and waits for the state machine to reach Succeeded. Asserts
// transactionsLost==0 (the planned-switchover invariant) and that
// bloodraven_planned_failovers_total{result="success"} incremented.
func scenario02PlannedSwitchover() runner.Scenario {
	return runner.Scenario{
		ID:    "02-planned-switchover",
		Title: "Planned switchover via annotation",
		Hypothesis: "Annotating the MFG with bloodraven.shipstream.io/planned-failover=<peer> walks the " +
			"PlannedFailoverStatus through Validating→Draining→WaitingForLag→Promoting→Resuming→Succeeded with transactionsLost==0.",
		Risk:     "low",
		DocLink:  "playground/chaos-scenarios.md#planned-switchover",
		Timeout:  4 * time.Minute,
		Precheck: AssertHealthyBaseline,
		Steps: []runner.Step{
			injectPlannedFailoverAnnotation(),
			observePlannedFailoverSucceeded(),
			verifyFollowersDirectlyFollowNewPrimary(),
			verifyTransactionsLostZero(),
			verifyPlannedFailoverMetric(),
		},
	}
}

func verifyFollowersDirectlyFollowNewPrimary() runner.Step {
	return runner.Step{
		Phase: runner.PhaseVerify,
		Name:  "every follower directly replicates from the new active primary",
		Do: func(ctx context.Context, env *runner.Env) error {
			target := ctxFetch(env, "switchoverTarget")
			original := ctxFetch(env, "originalPrimary")
			expectedHost := playgroundInternalSiteHost(env.FG, target, env.Namespace)
			deadline := time.Now().Add(2 * time.Minute)
			var last string
			for time.Now().Before(deadline) {
				mfg, err := env.Kube.GetMFGNamed(ctx, env.Namespace, env.FG)
				if err != nil {
					last = err.Error()
				} else if mfg.Status.ActiveSite != target {
					return fmt.Errorf("planned failover target changed after success: activeSite=%q want %q", mfg.Status.ActiveSite, target)
				} else {
					var bad []string
					foundOriginal := false
					foundReader := false
					for _, site := range mfg.Spec.Sites {
						if site.Name == target {
							continue
						}
						foundOriginal = foundOriginal || site.Name == original
						foundReader = foundReader || site.IsReadOnlyReader()
						client, openErr := pgmysql.Open(ctx, env.Kube, env.Namespace, env.FG, site.Name, env.Creds)
						if openErr != nil {
							bad = append(bad, fmt.Sprintf("%s=open:%v", site.Name, openErr))
							continue
						}
						rs, statusErr := client.ShowReplicaStatus(ctx)
						_ = client.Close()
						if statusErr != nil {
							bad = append(bad, fmt.Sprintf("%s=status:%v", site.Name, statusErr))
							continue
						}
						if !rs.Configured || !rs.IORunning || !rs.SQLRunning || canonicalMySQLHost(rs.SourceHost) != canonicalMySQLHost(expectedHost) {
							bad = append(bad, fmt.Sprintf("%s=configured:%v/io:%v/sql:%v/source:%q", site.Name, rs.Configured, rs.IORunning, rs.SQLRunning, rs.SourceHost))
						}
					}
					sort.Strings(bad)
					if !foundOriginal {
						return fmt.Errorf("demoted original primary %q is not a follower in spec", original)
					}
					if !foundReader {
						return fmt.Errorf("planned switchover topology has no read-only reader follower")
					}
					last = fmt.Sprintf("expectedSource=%s bad=%v", expectedHost, bad)
					if len(bad) == 0 {
						env.Capture.Note("all followers directly replicate from " + expectedHost)
						return nil
					}
				}
				select {
				case <-ctx.Done():
					return ctx.Err()
				case <-time.After(time.Second):
				}
			}
			return fmt.Errorf("followers did not converge directly to %s within 2m (last: %s)", expectedHost, last)
		},
	}
}

func injectPlannedFailoverAnnotation() runner.Step {
	return runner.Step{
		Phase: runner.PhaseInject,
		Name:  "annotate planned-failover with peer site",
		Do: func(ctx context.Context, env *runner.Env) error {
			mfg, err := env.Kube.GetMFGNamed(ctx, env.Namespace, env.FG)
			if err != nil {
				return err
			}
			if mfg.Status.ActiveSite == "" {
				return fmt.Errorf("no active site")
			}
			peer, err := PeerOf(mfg, mfg.Status.ActiveSite)
			if err != nil {
				return err
			}
			env.Capture.Note(fmt.Sprintf("planned switchover: %s -> %s", mfg.Status.ActiveSite, peer))
			if err := ctxStash(ctx, env, "originalPrimary", mfg.Status.ActiveSite); err != nil {
				return err
			}
			if err := ctxStash(ctx, env, "switchoverTarget", peer); err != nil {
				return err
			}
			if err := stashMetricCounter(ctx, env, "plannedFailoversBefore", "bloodraven_planned_failovers_total", map[string]string{
				"target_site": peer,
				"result":      "success",
			}); err != nil {
				return err
			}
			return env.Chaos.AnnotatePlannedFailover(ctx, peer)
		},
	}
}

func observePlannedFailoverSucceeded() runner.Step {
	return runner.Step{
		Phase: runner.PhaseObserve,
		Name:  "PlannedFailoverStatus reaches Succeeded",
		Do: func(ctx context.Context, env *runner.Env) error {
			target := ctxFetch(env, "switchoverTarget")
			waitCtx, cancel := context.WithTimeout(ctx, 3*time.Minute)
			defer cancel()
			_, err := env.Wait.UntilCR(waitCtx, env.Namespace,
				fmt.Sprintf("plannedFailover.phase==Succeeded with target=%s", target),
				func(mfg *v1alpha1.MysqlFailoverGroup) (bool, string, error) {
					pf := mfg.Status.PlannedFailover
					if pf == nil {
						return false, "no plannedFailover status yet", nil
					}
					// Ignore stale plannedFailover blocks left over from
					// prior runs / prior operator lifecycles. We only care
					// about phases for the in-flight attempt this scenario
					// just kicked off — identified by StartTime within a
					// tolerance window of the scenario's own start time.
					// PlannedFailoverStatus.StartTime is serialized as
					// metav1.Time which truncates to whole seconds, so a
					// pf.StartTime up to 1s before env.StartTime can still
					// be the new run; we use 2s of slack.
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

func verifyTransactionsLostZero() runner.Step {
	return runner.Step{
		Phase: runner.PhaseVerify,
		Name:  "transactionsLost == 0",
		Do: func(ctx context.Context, env *runner.Env) error {
			mfg, err := env.Kube.GetMFGNamed(ctx, env.Namespace, env.FG)
			if err != nil {
				return err
			}
			pf := mfg.Status.PlannedFailover
			if pf == nil {
				return fmt.Errorf("no plannedFailover status to inspect")
			}
			if pf.TransactionsLost == nil {
				return fmt.Errorf("transactionsLost not populated on Succeeded planned failover")
			}
			if *pf.TransactionsLost != 0 {
				return fmt.Errorf("expected transactionsLost=0, got %d", *pf.TransactionsLost)
			}
			return nil
		},
	}
}

func verifyPlannedFailoverMetric() runner.Step {
	return runner.Step{
		Phase: runner.PhaseVerify,
		Name:  `bloodraven_planned_failovers_total{result="success"} increments`,
		Do: func(ctx context.Context, env *runner.Env) error {
			target := ctxFetch(env, "switchoverTarget")
			before, err := fetchStashedFloat(env, "plannedFailoversBefore")
			if err != nil {
				return err
			}
			waitCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
			defer cancel()
			return env.Wait.UntilMetric(waitCtx, env.Metrics,
				fmt.Sprintf(`planned_failovers_total{target_site=%q,result="success"} increments from %g`, target, before),
				func(snap *pgmetrics.Snapshot) (bool, string) {
					v, _ := snap.Counter("bloodraven_planned_failovers_total", map[string]string{
						"target_site": target,
						"result":      "success",
					})
					return v > before, fmt.Sprintf("counter=%g before=%g delta=%g", v, before, v-before)
				},
			)
		},
	}
}
