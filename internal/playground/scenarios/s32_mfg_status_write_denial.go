package scenarios

import (
	"context"
	"fmt"
	"time"

	v1alpha1 "github.com/shipstream/bloodraven/api/v1alpha1"
	pgchaos "github.com/shipstream/bloodraven/internal/playground/chaos"
	pglogs "github.com/shipstream/bloodraven/internal/playground/logs"
	pgmetrics "github.com/shipstream/bloodraven/internal/playground/metrics"
	"github.com/shipstream/bloodraven/internal/playground/runner"
)

func init() {
	runner.Register(scenario32MFGStatusWriteDenial())
}

const s32ID = "32-mfg-status-write-denial-emergency-promotion"

// scenario32MFGStatusWriteDenial denies the operator patch/update on
// shipstream.io/mysqlfailovergroups/status, then triggers an emergency
// promotion by scaling the active MySQL Deployment to 0. MySQL promotion must
// still complete at the data layer (the operator's promotion is SQL, not a CR
// write) even though the CR status cannot be written. After the ClusterRole is
// restored the CR status must catch up to the actual writable site within 90s
// WITHOUT another failover — the self-heal the operator gained via the
// per-poll status-write retry (see internal/controller status-write fix).
func scenario32MFGStatusWriteDenial() runner.Scenario {
	var (
		activeSite      string
		peerSite        string
		failoversBefore float64
		restoreRBAC     func(context.Context) error
	)

	return runner.Scenario{
		ID:    s32ID,
		Title: "MFG status-write denial during emergency promotion self-heals after RBAC restore",
		Hypothesis: "Denying the operator patch/update on mysqlfailovergroups/status then scaling the active " +
			"primary to 0 still completes MySQL promotion (exactly one writable site, no split-brain, " +
			"failovers_total increments), while the CR status stays stale. After RBAC is restored the status " +
			"catches up to the promoted site within 90s without another failover.",
		Risk:              "high",
		DocLink:           "playground/chaos-scenarios.md#32-mfg-status-write-denial-during-emergency-promotion",
		Timeout:           8 * time.Minute,
		ResetBeforeRunAll: false,
		Precheck:          precheckHealthyWithOperatorRole,
		Steps: []runner.Step{
			{
				Phase: runner.PhaseInject,
				Name:  "deny status patch/update, then scale active primary to 0",
				Do: func(ctx context.Context, env *runner.Env) error {
					mfg, err := env.Kube.GetMFGNamed(ctx, env.Namespace, env.FG)
					if err != nil {
						return err
					}
					activeSite = mfg.Status.ActiveSite
					peerSite, err = PeerOf(mfg, activeSite)
					if err != nil {
						return err
					}
					env.Capture.Note(fmt.Sprintf("pre-injection: activeSite=%s peer=%s lastFailoverTarget=%q promotionGtid=%q ready=%s",
						activeSite, peerSite, mfg.Status.LastFailoverTarget, mfg.Status.PromotionGtidExecuted, readyOf(mfg)))

					// Open the operator tailer before the injection so the
					// SinceTime window covers the forbidden status-write log.
					if _, err := env.Logs("operator"); err != nil {
						env.Capture.Note("open operator tailer failed: " + err.Error())
					}

					failoversBefore, err = metricCounter(ctx, env, "bloodraven_failovers_total", map[string]string{"target_site": peerSite})
					if err != nil {
						return fmt.Errorf("read failovers_total before: %w", err)
					}

					res, restore, err := env.Chaos.DenyOperatorClusterRoleVerbs(ctx, env.Namespace, operatorServiceAccount,
						[]pgchaos.VerbDenial{{APIGroup: "shipstream.io", Resource: "mysqlfailovergroups/status", Verbs: []string{"patch", "update"}}})
					if err != nil {
						return fmt.Errorf("deny status writes: %w", err)
					}
					restoreRBAC = restore
					_ = env.Capture.WriteFile("s32-clusterrole-original.json", []byte(res.OriginalRules))
					_ = env.Capture.WriteFile("s32-clusterrole-patched.json", []byte(res.PatchedRules))
					env.Capture.Note(fmt.Sprintf("denied patch/update on mysqlfailovergroups/status on ClusterRole %s (binding %s)", res.RoleName, res.BindingName))

					env.Capture.Note(fmt.Sprintf("scaling active primary %s to 0 to force emergency promotion", activeSite))
					return env.Chaos.ScaleSiteToZero(ctx, activeSite)
				},
			},
			{
				Phase: runner.PhaseObserve,
				Name:  "MySQL promotes peer at the data layer while status writes are denied",
				Do: func(ctx context.Context, env *runner.Env) error {
					// Promotion is observed by directly probing the peer, not
					// the CR status (which is frozen by the denial).
					if err := waitSiteWritable(ctx, env, peerSite, 3*time.Minute); err != nil {
						return fmt.Errorf("peer %s did not become writable at the MySQL layer: %w", peerSite, err)
					}
					env.Capture.Note(fmt.Sprintf("peer %s is writable at the MySQL layer (promotion completed)", peerSite))

					// failovers_total increments even though status is denied.
					waitCtx, cancel := context.WithTimeout(ctx, 60*time.Second)
					defer cancel()
					if err := env.Wait.UntilMetric(waitCtx, env.Metrics,
						fmt.Sprintf("failovers_total{target_site=%q} increments from %g", peerSite, failoversBefore),
						func(snap *pgmetrics.Snapshot) (bool, string) {
							v, _ := snap.Counter("bloodraven_failovers_total", map[string]string{"target_site": peerSite})
							return v > failoversBefore, fmt.Sprintf("counter=%g before=%g", v, failoversBefore)
						}); err != nil {
						return err
					}

					// Evidence that status writes were actually denied.
					if tail, err := env.Logs("operator"); err == nil {
						if hit, line := firstMatchSince(tail, env.StartTime, pglogs.Substring("update fg status")); hit {
							env.Capture.Note("observed denied status write in operator log: " + line)
						} else {
							env.Capture.Note("did not observe an 'update fg status' error line (denial may have been briefer than the poll window)")
						}
					}

					// Snapshot the (expected-stale) CR status during denial.
					mfg, err := env.Kube.GetMFGNamed(ctx, env.Namespace, env.FG)
					if err != nil {
						return err
					}
					env.Capture.Note(fmt.Sprintf("CR status during denial (may be stale): activeSite=%q lastFailoverTarget=%q ready=%s",
						mfg.Status.ActiveSite, mfg.Status.LastFailoverTarget, readyOf(mfg)))
					return nil
				},
			},
			{
				Phase: runner.PhaseVerify,
				Name:  "restore RBAC; CR status catches up to the promoted site within 90s (no re-failover)",
				Do: func(ctx context.Context, env *runner.Env) error {
					if restoreRBAC == nil {
						return fmt.Errorf("internal: restoreRBAC closure not set")
					}
					if err := restoreRBAC(ctx); err != nil {
						return fmt.Errorf("restore ClusterRole rules: %w", err)
					}
					env.Capture.Note("restored status patch/update on the operator ClusterRole; awaiting status heal")

					waitCtx, cancel := context.WithTimeout(ctx, 90*time.Second)
					defer cancel()
					_, err := env.Wait.UntilCR(waitCtx, env.Namespace,
						fmt.Sprintf("status.activeSite==%s with lastFailoverTarget and promotionGtid populated", peerSite),
						func(mfg *v1alpha1.MysqlFailoverGroup) (bool, string, error) {
							msg := fmt.Sprintf("activeSite=%q lastFailoverTarget=%q promotionGtid=%q ready=%s",
								mfg.Status.ActiveSite, mfg.Status.LastFailoverTarget, truncGtid(mfg.Status.PromotionGtidExecuted), readyOf(mfg))
							for _, s := range mfg.Status.Sites {
								if s.RecoveryState == "RecoveryBlocked" {
									return false, msg, fmt.Errorf("site %s RecoveryBlocked during status heal", s.Name)
								}
							}
							done := mfg.Status.ActiveSite == peerSite &&
								mfg.Status.LastFailoverTarget == peerSite &&
								mfg.Status.PromotionGtidExecuted != "" &&
								readyOf(mfg) == "True"
							return done, msg, nil
						})
					if err != nil {
						return fmt.Errorf("status did not heal to promoted site within 90s after RBAC restore: %w", err)
					}
					env.Capture.Note("CR status healed to promoted site after RBAC restore")
					return nil
				},
			},
			{
				Phase: runner.PhaseSettle,
				Name:  "scale old primary back up; it rejoins without RecoveryBlocked",
				Do: func(ctx context.Context, env *runner.Env) error {
					if err := env.Chaos.ScaleSiteToOne(ctx, activeSite); err != nil {
						return fmt.Errorf("scale old primary %s back to 1: %w", activeSite, err)
					}
					waitCtx, cancel := context.WithTimeout(ctx, 3*time.Minute)
					defer cancel()
					_, err := env.Wait.UntilCR(waitCtx, env.Namespace,
						fmt.Sprintf("old primary %s rejoins as healthy replica or recovery progresses without block", activeSite),
						func(mfg *v1alpha1.MysqlFailoverGroup) (bool, string, error) {
							var old *v1alpha1.SiteStatus
							for i := range mfg.Status.Sites {
								if mfg.Status.Sites[i].Name == activeSite {
									old = &mfg.Status.Sites[i]
								}
							}
							if old == nil {
								return false, "old primary not in status yet", nil
							}
							msg := fmt.Sprintf("old=%s state=%s replicating=%v recovery=%q ready=%s",
								old.Name, old.State, old.Replicating, old.RecoveryState, readyOf(mfg))
							if old.RecoveryState == "RecoveryBlocked" {
								return false, msg, fmt.Errorf("old primary %s RecoveryBlocked", activeSite)
							}
							healthyReplica := old.State == "read-only" && old.Replicating && readyOf(mfg) == "True"
							recovering := old.RecoveryState == "RecoveryInProgress"
							return healthyReplica || recovering, msg, nil
						})
					return err
				},
			},
		},
		Cleanup: func(ctx context.Context, env *runner.Env) error {
			// Safety net: restore RBAC if the scenario bailed before the
			// explicit restore. Idempotent — a no-op after a successful run.
			if restoreRBAC != nil {
				if err := restoreRBAC(ctx); err != nil {
					env.Capture.Note("cleanup: restore RBAC: " + err.Error())
				}
			}
			return nil
		},
	}
}

// precheckHealthyWithOperatorRole is the shared precheck for the RBAC-denial
// scenarios: a healthy baseline plus a resolvable operator ClusterRole
// binding, so the scenario fails early and clearly if the operator's RBAC
// layout is not what the injection expects.
func precheckHealthyWithOperatorRole(ctx context.Context, env *runner.Env) error {
	if err := AssertHealthyBaseline(ctx, env); err != nil {
		return err
	}
	roleName, bindingName, err := env.Kube.ResolveBoundClusterRole(ctx, env.Namespace, operatorServiceAccount)
	if err != nil {
		return fmt.Errorf("precheck: resolve operator bound ClusterRole for SA %s/%s: %w", env.Namespace, operatorServiceAccount, err)
	}
	env.Capture.Note(fmt.Sprintf("operator ServiceAccount %s/%s is bound to ClusterRole %s via %s", env.Namespace, operatorServiceAccount, roleName, bindingName))
	return nil
}

func readyOf(mfg *v1alpha1.MysqlFailoverGroup) string {
	for _, c := range mfg.Status.Conditions {
		if c.Type == "Ready" {
			return string(c.Status)
		}
	}
	return "<none>"
}

func truncGtid(g string) string {
	if len(g) > 48 {
		return g[:48] + "…"
	}
	return g
}
