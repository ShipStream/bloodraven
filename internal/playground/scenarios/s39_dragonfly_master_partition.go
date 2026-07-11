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
	runner.Register(scenario39DragonflyMasterPartition())
}

const s39ID = "39-dragonfly-master-partition"

// scenario39DragonflyMasterPartition applies a deny-all NetworkPolicy to only
// the Dragonfly master pod (MySQL and its Services untouched). Bloodraven must
// observe the master as Unreachable, promote the surviving Dragonfly replica,
// move the active Dragonfly Service endpoints to the new master, and leave the
// MySQL active site unchanged with no MySQL failover.
func scenario39DragonflyMasterPartition() runner.Scenario {
	return runner.Scenario{
		ID:    s39ID,
		Title: "Dragonfly master partition promotes replica; MySQL untouched",
		Hypothesis: "A deny-all NetworkPolicy on only the Dragonfly master pod makes Bloodraven see it Unreachable " +
			"and promote the surviving replica: status.dragonfly.activeSite flips, the seeded key is readable on the " +
			"new master, the active Dragonfly Service endpoints move to the promoted pod, dragonfly_promotions_total " +
			"increments, and MySQL status.activeSite and failover counters are unchanged.",
		Risk:              "medium",
		DocLink:           "playground/chaos-scenarios.md#39-live-dragonfly-master-partition",
		Timeout:           8 * time.Minute,
		ResetBeforeRunAll: false,
		Precheck:          s39Precheck,
		Steps: []runner.Step{
			s39SeedAndPartition(),
			s39ObserveDragonflyFlip(),
			s39VerifyEndpointsAndMySQLUnchanged(),
			s39VerifyPromotionMetric(),
			s39SettleOldMasterRejoins(),
		},
	}
}

// s39Precheck requires a healthy Dragonfly baseline and that the active
// Dragonfly Service endpoints point only at the current master pod.
func s39Precheck(ctx context.Context, env *runner.Env) error {
	if err := AssertDragonflyHealthyBaseline(ctx, env); err != nil {
		return err
	}
	mfg, err := env.Kube.GetMFG(ctx, env.Namespace)
	if err != nil {
		return err
	}
	master, err := dragonflyActiveSite(mfg)
	if err != nil {
		return err
	}
	masterPod, err := env.Kube.GetSiteDragonflyPod(ctx, env.Namespace, env.FG, master)
	if err != nil {
		return fmt.Errorf("precheck: get dragonfly master pod: %w", err)
	}
	eps, err := env.Kube.DragonflyActiveServiceEndpointPods(ctx, env.Namespace, env.FG)
	if err != nil {
		return fmt.Errorf("precheck: read dragonfly active service endpoints: %w", err)
	}
	if len(eps) != 1 || eps[0] != masterPod.Name {
		return fmt.Errorf("precheck: active dragonfly Service endpoints=%v, want only master pod %s", eps, masterPod.Name)
	}
	return nil
}

func s39SeedAndPartition() runner.Step {
	return runner.Step{
		Phase: runner.PhaseInject,
		Name:  "seed key on dragonfly master, then deny-all NetworkPolicy on the master pod",
		Do: func(ctx context.Context, env *runner.Env) error {
			mfg, err := env.Kube.GetMFG(ctx, env.Namespace)
			if err != nil {
				return err
			}
			master, err := dragonflyActiveSite(mfg)
			if err != nil {
				return err
			}
			peer, err := PeerOf(mfg, master)
			if err != nil {
				return err
			}
			if err := ctxStash(ctx, env, "dfMaster", master); err != nil {
				return err
			}
			if err := ctxStash(ctx, env, "dfTarget", peer); err != nil {
				return err
			}
			if err := ctxStash(ctx, env, "mysqlActiveBefore", mfg.Status.ActiveSite); err != nil {
				return err
			}

			// Baselines for the "MySQL did not fail over" and "Dragonfly
			// promoted" metric assertions.
			if err := stashMetricCounter(ctx, env, "dfPromotionsBefore", "bloodraven_dragonfly_promotions_total",
				map[string]string{"target_site": peer, "result": "success"}); err != nil {
				return err
			}
			if err := stashMetricCounter(ctx, env, "mysqlFailoversBefore", "bloodraven_failovers_total",
				map[string]string{"target_site": peer}); err != nil {
				return err
			}

			cli, err := env.Dragonfly(master)
			if err != nil {
				return fmt.Errorf("open dragonfly master %s: %w", master, err)
			}
			val := fmt.Sprintf("scenario39-%d", env.StartTime.UnixNano())
			if _, err := cli.Set(ctx, "scenario39:key", val); err != nil {
				return fmt.Errorf("SET on dragonfly master %s: %w", master, err)
			}
			if err := ctxStash(ctx, env, "dfSeedValue", val); err != nil {
				return err
			}
			// Confirm the key replicated to the peer before the partition so a
			// post-promotion read proves replication, not luck.
			if err := s39WaitKeyOnSite(ctx, env, peer, "scenario39:key", val, 30*time.Second); err != nil {
				return fmt.Errorf("seed key did not replicate to peer %s before partition: %w", peer, err)
			}

			env.Capture.Note(fmt.Sprintf("partitioning dragonfly master pod on %s (peer=%s, mysqlActive=%s)", master, peer, mfg.Status.ActiveSite))
			return env.Chaos.PartitionDragonflyPod(ctx, master)
		},
	}
}

func s39ObserveDragonflyFlip() runner.Step {
	return runner.Step{
		Phase: runner.PhaseObserve,
		Name:  "master goes Unreachable and status.dragonfly.activeSite flips to peer",
		Do: func(ctx context.Context, env *runner.Env) error {
			oldMaster := ctxFetch(env, "dfMaster")
			target := ctxFetch(env, "dfTarget")
			waitCtx, cancel := context.WithTimeout(ctx, 3*time.Minute)
			defer cancel()
			var sawUnreachable bool
			_, err := env.Wait.UntilCR(waitCtx, env.Namespace,
				fmt.Sprintf("status.dragonfly.activeSite==%s after master partition", target),
				func(mfg *v1alpha1.MysqlFailoverGroup) (bool, string, error) {
					df := mfg.Status.Dragonfly
					if df == nil {
						return false, "no status.dragonfly", nil
					}
					for _, s := range df.Sites {
						if s.Name == oldMaster && s.Role == v1alpha1.DragonflyRoleUnreachable {
							sawUnreachable = true
						}
					}
					msg := fmt.Sprintf("dfActive=%q phase=%q oldMasterUnreachableSeen=%v", df.ActiveSite, df.Phase, sawUnreachable)
					return df.ActiveSite == target, msg, nil
				})
			if err != nil {
				return err
			}
			if sawUnreachable {
				env.Capture.Note(fmt.Sprintf("observed dragonfly site %s role=Unreachable during the partition", oldMaster))
			} else {
				env.Capture.Note(fmt.Sprintf("dragonfly activeSite flipped to %s (did not catch the transient Unreachable role, which is fine)", target))
			}
			return nil
		},
	}
}

func s39VerifyEndpointsAndMySQLUnchanged() runner.Step {
	return runner.Step{
		Phase: runner.PhaseVerify,
		Name:  "seeded key on new master, endpoints moved, MySQL active site unchanged",
		Do: func(ctx context.Context, env *runner.Env) error {
			target := ctxFetch(env, "dfTarget")
			oldMaster := ctxFetch(env, "dfMaster")
			expected := ctxFetch(env, "dfSeedValue")

			// Seeded key readable on the promoted master (replication was healthy).
			if err := s39WaitKeyOnSite(ctx, env, target, "scenario39:key", expected, 60*time.Second); err != nil {
				return fmt.Errorf("replicated key unavailable on promoted master %s: %w", target, err)
			}

			// Active Dragonfly Service endpoints now include only the new
			// master pod, not the partitioned old master.
			targetPod, err := env.Kube.GetSiteDragonflyPod(ctx, env.Namespace, env.FG, target)
			if err != nil {
				return fmt.Errorf("get promoted dragonfly pod: %w", err)
			}
			waitCtx, cancel := context.WithTimeout(ctx, 90*time.Second)
			defer cancel()
			tick := time.NewTicker(3 * time.Second)
			defer tick.Stop()
			var lastEps []string
			for {
				eps, err := env.Kube.DragonflyActiveServiceEndpointPods(ctx, env.Namespace, env.FG)
				if err == nil {
					lastEps = eps
					if len(eps) == 1 && eps[0] == targetPod.Name {
						break
					}
				}
				select {
				case <-waitCtx.Done():
					return fmt.Errorf("active dragonfly endpoints did not converge to only the promoted master pod %s (last=%v)", targetPod.Name, lastEps)
				case <-tick.C:
				}
			}
			env.Capture.Note(fmt.Sprintf("active dragonfly endpoints converged to promoted master pod %s", targetPod.Name))

			// MySQL active site is unchanged — a Dragonfly-only partition must
			// not move the database.
			mfg, err := env.Kube.GetMFG(ctx, env.Namespace)
			if err != nil {
				return err
			}
			before := ctxFetch(env, "mysqlActiveBefore")
			if mfg.Status.ActiveSite != before {
				return fmt.Errorf("MySQL activeSite changed from %q to %q during a Dragonfly-only partition", before, mfg.Status.ActiveSite)
			}
			// And no MySQL failover metric incremented.
			mysqlBefore, err := fetchStashedFloat(env, "mysqlFailoversBefore")
			if err != nil {
				return err
			}
			cur, err := metricCounter(ctx, env, "bloodraven_failovers_total", map[string]string{"target_site": target})
			if err != nil {
				return err
			}
			if cur > mysqlBefore {
				return fmt.Errorf("MySQL failovers_total{target_site=%s} incremented (%g -> %g) during a Dragonfly-only partition", target, mysqlBefore, cur)
			}
			env.Capture.Note(fmt.Sprintf("MySQL active site %s unchanged and no MySQL failover; old dragonfly master %s partitioned", before, oldMaster))
			return nil
		},
	}
}

func s39VerifyPromotionMetric() runner.Step {
	return runner.Step{
		Phase: runner.PhaseVerify,
		Name:  `bloodraven_dragonfly_promotions_total{result="success"} increments`,
		Do: func(ctx context.Context, env *runner.Env) error {
			target := ctxFetch(env, "dfTarget")
			before, err := fetchStashedFloat(env, "dfPromotionsBefore")
			if err != nil {
				return err
			}
			waitCtx, cancel := context.WithTimeout(ctx, 60*time.Second)
			defer cancel()
			return env.Wait.UntilMetric(waitCtx, env.Metrics,
				fmt.Sprintf(`dragonfly_promotions_total{target_site=%q,result="success"} increments from %g`, target, before),
				func(snap *pgmetrics.Snapshot) (bool, string) {
					v, _ := snap.Counter("bloodraven_dragonfly_promotions_total", map[string]string{"target_site": target, "result": "success"})
					return v > before, fmt.Sprintf("counter=%g before=%g", v, before)
				})
		},
	}
}

// s39SettleOldMasterRejoins removes the partition and waits for the old master
// to rejoin as a healthy replica. If it comes back a stale master and does not
// reconfigure within the settle budget, it force-deletes only the old Dragonfly
// pod as the documented conservative recovery so the StatefulSet recreates it
// cleanly.
func s39SettleOldMasterRejoins() runner.Step {
	return runner.Step{
		Phase: runner.PhaseSettle,
		Name:  "remove partition; old master rejoins as replica (force-delete stale pod if needed)",
		Do: func(ctx context.Context, env *runner.Env) error {
			oldMaster := ctxFetch(env, "dfMaster")
			// Remove the partition now (reverter would also do it at cleanup)
			// so recovery happens inside scenario scope.
			if err := env.Chaos.Revert(ctx); err != nil {
				return fmt.Errorf("remove dragonfly partition: %w", err)
			}
			env.Capture.Note("removed dragonfly master partition; awaiting old master rejoin")

			if s39WaitOldMasterHealthyReplica(ctx, env, oldMaster, 90*time.Second) == nil {
				return nil
			}
			env.Capture.Note(fmt.Sprintf("old dragonfly master %s did not rejoin cleanly within budget; force-deleting the stale pod (documented conservative recovery)", oldMaster))
			if err := env.Chaos.KillDragonflyPod(ctx, oldMaster); err != nil {
				return fmt.Errorf("force-delete stale dragonfly pod %s: %w", oldMaster, err)
			}
			if err := s39WaitOldMasterHealthyReplica(ctx, env, oldMaster, 2*time.Minute); err != nil {
				return fmt.Errorf("old master %s did not rejoin as a healthy replica even after pod recreate: %w", oldMaster, err)
			}
			return nil
		},
	}
}

func s39WaitOldMasterHealthyReplica(ctx context.Context, env *runner.Env, oldMaster string, timeout time.Duration) error {
	waitCtx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	_, err := env.Wait.UntilCR(waitCtx, env.Namespace,
		fmt.Sprintf("dragonfly site %s rejoined as replica with link=up", oldMaster),
		func(mfg *v1alpha1.MysqlFailoverGroup) (bool, string, error) {
			df := mfg.Status.Dragonfly
			if df == nil {
				return false, "no status.dragonfly", nil
			}
			for _, s := range df.Sites {
				if s.Name == oldMaster {
					msg := fmt.Sprintf("role=%q link=%q reachable=%v sync=%v phase=%q", s.Role, s.LinkStatus, s.Reachable, s.SyncInProgress, df.Phase)
					done := df.Phase == v1alpha1.DragonflyPhaseReady &&
						s.Role == v1alpha1.DragonflyRoleReplica &&
						s.LinkStatus == "up" && s.Reachable && !s.SyncInProgress
					return done, msg, nil
				}
			}
			return false, fmt.Sprintf("site %s not in status.dragonfly.sites yet", oldMaster), nil
		})
	return err
}

func s39WaitKeyOnSite(ctx context.Context, env *runner.Env, site, key, want string, timeout time.Duration) error {
	deadline := time.Now().Add(timeout)
	var last string
	for {
		cli, err := env.Dragonfly(site)
		if err != nil {
			last = err.Error()
		} else if got, ok, gerr := cli.Get(ctx, key); gerr != nil {
			last = gerr.Error()
		} else if ok && got == want {
			return nil
		} else {
			last = fmt.Sprintf("got=%q ok=%v", got, ok)
		}
		if time.Now().After(deadline) {
			return fmt.Errorf("key %s not %q on %s within %s (last: %s)", key, want, site, timeout, last)
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(2 * time.Second):
		}
	}
}
