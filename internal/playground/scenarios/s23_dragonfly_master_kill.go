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
	runner.Register(scenario23DragonflyMasterKill())
}

// scenario23DragonflyMasterKill validates Dragonfly-only HA: killing
// the master Dragonfly pod (without touching MySQL) must:
//
//   - leave MySQL untouched (status.activeSite unchanged)
//   - cause Bloodraven to autonomously promote the surviving replica
//     and emit DragonflyPromotionStarted/Completed events
//   - flip status.dragonfly.activeSite to the peer site
//   - increment bloodraven_dragonfly_promotions_total{result="success"}
//   - re-attach the respawned old master as a replica with link=up
//
// Mapped to PLANS-Dragonfly-Chaos-Scenarios.md scenario D7.
func scenario23DragonflyMasterKill() runner.Scenario {
	return runner.Scenario{
		ID:    "23-dragonfly-master-kill",
		Title: "Dragonfly master kill — replica promoted, MySQL untouched",
		Hypothesis: "Force-deleting the active Dragonfly pod causes Bloodraven to autonomously promote the surviving " +
			"replica via TryEmergencyPromote: status.dragonfly.activeSite flips to the peer, MySQL status.activeSite " +
			"is unchanged, and once the StatefulSet respawns the old pod it re-attaches as a healthy replica.",
		Risk:    "low",
		DocLink: "PLANS-Dragonfly-Chaos-Scenarios.md (D7)",
		Timeout: 4 * time.Minute,
		Precheck: AssertHealthyBaseline,
		Steps: []runner.Step{
			seedDragonflyCounterForMasterKill(),
			injectDragonflyMasterKill(),
			observeDragonflyActiveSiteFlipped(),
			verifyMysqlActiveSiteUnchanged(),
			verifyDragonflyMasterKillCounterPreserved(),
			settleDragonflyOldMasterRejoinsAsReplica(),
			verifyDragonflyPromotionsMetricS23(),
		},
	}
}

// seedDragonflyCounterForMasterKill writes a known value on the
// master before the kill so the post-promotion read can prove the
// replica had it via replication (D9 silent-key-loss validation
// folded in).
func seedDragonflyCounterForMasterKill() runner.Step {
	return runner.Step{
		Phase: runner.PhaseInject,
		Name:  "seed counter on dragonfly master before kill",
		Do: func(ctx context.Context, env *runner.Env) error {
			mfg, err := env.Kube.GetMFG(ctx, env.Namespace)
			if err != nil {
				return err
			}
			active, err := dragonflyActiveSite(mfg)
			if err != nil {
				return err
			}
			peer, err := PeerOf(mfg, mfg.Status.ActiveSite)
			if err != nil {
				return err
			}
			if err := ctxStash(ctx, env, "originalMaster", active); err != nil {
				return err
			}
			if err := ctxStash(ctx, env, "promotionTarget", peer); err != nil {
				return err
			}
			if err := ctxStash(ctx, env, "mysqlActiveBefore", mfg.Status.ActiveSite); err != nil {
				return err
			}
			cli, err := env.Dragonfly(active)
			if err != nil {
				return fmt.Errorf("open dragonfly on %s: %w", active, err)
			}
			val := fmt.Sprintf("scenario23-%d", env.StartTime.UnixNano())
			if _, err := cli.Set(ctx, "scenario23:counter", val); err != nil {
				return fmt.Errorf("SET on %s: %w", active, err)
			}
			return ctxStash(ctx, env, "dfSeedValue", val)
		},
	}
}

func injectDragonflyMasterKill() runner.Step {
	return runner.Step{
		Phase: runner.PhaseInject,
		Name:  "force-delete dragonfly master pod",
		Do: func(ctx context.Context, env *runner.Env) error {
			master := ctxFetch(env, "originalMaster")
			env.Capture.Note(fmt.Sprintf("kill dragonfly master pod on %s", master))
			return env.Chaos.KillDragonflyPod(ctx, master)
		},
	}
}

func observeDragonflyActiveSiteFlipped() runner.Step {
	return runner.Step{
		Phase: runner.PhaseObserve,
		Name:  "status.dragonfly.activeSite flips to peer",
		Do: func(ctx context.Context, env *runner.Env) error {
			target := ctxFetch(env, "promotionTarget")
			waitCtx, cancel := context.WithTimeout(ctx, 90*time.Second)
			defer cancel()
			_, err := env.Wait.UntilCR(waitCtx, env.Namespace,
				fmt.Sprintf("status.dragonfly.activeSite==%s after master kill", target),
				func(mfg *v1alpha1.MysqlFailoverGroup) (bool, string, error) {
					if mfg.Status.Dragonfly == nil {
						return false, "no status.dragonfly", nil
					}
					return mfg.Status.Dragonfly.ActiveSite == target,
						fmt.Sprintf("dfActive=%q phase=%q", mfg.Status.Dragonfly.ActiveSite, mfg.Status.Dragonfly.Phase),
						nil
				},
			)
			return err
		},
	}
}

func verifyMysqlActiveSiteUnchanged() runner.Step {
	return runner.Step{
		Phase: runner.PhaseVerify,
		Name:  "MySQL status.activeSite is unchanged",
		Do: func(ctx context.Context, env *runner.Env) error {
			before := ctxFetch(env, "mysqlActiveBefore")
			mfg, err := env.Kube.GetMFG(ctx, env.Namespace)
			if err != nil {
				return err
			}
			if mfg.Status.ActiveSite != before {
				return fmt.Errorf("MySQL activeSite changed from %q to %q — Dragonfly kill must not affect MySQL",
					before, mfg.Status.ActiveSite)
			}
			return nil
		},
	}
}

func verifyDragonflyMasterKillCounterPreserved() runner.Step {
	return runner.Step{
		Phase: runner.PhaseVerify,
		Name:  "seed counter readable on promoted replica (replication path was healthy)",
		Do: func(ctx context.Context, env *runner.Env) error {
			target := ctxFetch(env, "promotionTarget")
			expected := ctxFetch(env, "dfSeedValue")
			cli, err := env.Dragonfly(target)
			if err != nil {
				return fmt.Errorf("open dragonfly on promoted replica %s: %w", target, err)
			}
			got, ok, err := cli.Get(ctx, "scenario23:counter")
			if err != nil {
				return fmt.Errorf("GET on promoted replica: %w", err)
			}
			if !ok {
				return fmt.Errorf("scenario23:counter missing on promoted replica %s — replication wasn't keeping up", target)
			}
			if got != expected {
				return fmt.Errorf("scenario23:counter=%q want %q on promoted replica %s", got, expected, target)
			}
			return nil
		},
	}
}

// settleDragonflyOldMasterRejoinsAsReplica waits for the StatefulSet
// to respawn the killed pod and the operator to reconfigure it as a
// replica of the new master with master_link_status=up. Surfaces D2
// (snapshot_cron restoration on promotion) implicitly: a pod that
// doesn't reach a stable replica state would block this step.
func settleDragonflyOldMasterRejoinsAsReplica() runner.Step {
	return runner.Step{
		Phase: runner.PhaseSettle,
		Name:  "old master pod respawns and rejoins as healthy replica",
		Do: func(ctx context.Context, env *runner.Env) error {
			oldMaster := ctxFetch(env, "originalMaster")
			waitCtx, cancel := context.WithTimeout(ctx, 2*time.Minute)
			defer cancel()
			_, err := env.Wait.UntilCR(waitCtx, env.Namespace,
				fmt.Sprintf("dragonfly site %s rejoined as replica with link=up", oldMaster),
				func(mfg *v1alpha1.MysqlFailoverGroup) (bool, string, error) {
					if mfg.Status.Dragonfly == nil {
						return false, "no status.dragonfly", nil
					}
					for _, s := range mfg.Status.Dragonfly.Sites {
						if s.Name == oldMaster {
							msg := fmt.Sprintf("site %s role=%q linkStatus=%q reachable=%v sync=%v",
								s.Name, s.Role, s.LinkStatus, s.Reachable, s.SyncInProgress)
							done := s.Role == v1alpha1.DragonflyRoleReplica &&
								s.LinkStatus == "up" &&
								!s.SyncInProgress &&
								s.Reachable
							return done, msg, nil
						}
					}
					return false, fmt.Sprintf("site %s not in status.dragonfly.sites yet", oldMaster), nil
				},
			)
			return err
		},
	}
}

// verifyDragonflyPromotionsMetric reuses the metric checker from s22
// — same metric, same shape — but re-declared because each scenario
// uses a different stash key for the target site. Inlined for
// clarity rather than parameterising across files.
func verifyDragonflyPromotionsMetricS23() runner.Step {
	return runner.Step{
		Phase: runner.PhaseVerify,
		Name:  `bloodraven_dragonfly_promotions_total{result="success"} >= 1`,
		Do: func(ctx context.Context, env *runner.Env) error {
			target := ctxFetch(env, "promotionTarget")
			waitCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
			defer cancel()
			return env.Wait.UntilMetric(waitCtx, env.Metrics,
				fmt.Sprintf(`dragonfly_promotions_total{target_site=%q,result="success"} >= 1`, target),
				func(snap *pgmetrics.Snapshot) (bool, string) {
					v, _ := snap.Counter("bloodraven_dragonfly_promotions_total", map[string]string{
						"target_site": target,
						"result":      "success",
					})
					return v >= 1, fmt.Sprintf("counter=%g", v)
				},
			)
		},
	}
}
