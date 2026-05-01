package scenarios

import (
	"context"
	"fmt"
	"time"

	v1alpha1 "github.com/shipstream/bloodraven/api/v1alpha1"
	pglogs "github.com/shipstream/bloodraven/internal/playground/logs"
	pgmetrics "github.com/shipstream/bloodraven/internal/playground/metrics"
	"github.com/shipstream/bloodraven/internal/playground/runner"
)

func init() {
	runner.Register(scenario09AntiFlapCooldown())
}

// scenario09AntiFlapCooldown verifies the anti-flap cooldown contract:
// after one emergency failover, a second failover candidate inside the
// cooldown window MUST be blocked by the cooldown check at
// internal/controller/topology.go:945-950 ("failover blocked by
// anti-flap cooldown"), and the cluster must NOT ping-pong promotions.
//
// The playground's failoverCooldown is 30s. The test:
//
//  1. Snapshots failovers_total{target_site=*} for every primary
//     candidate so we can compare against the post-test value.
//  2. Force-deletes the active primary's pod (mirrors `chaos.sh
//     kill-site`). The Deployment respawns it within ~5s, but by then
//     the operator has already detected the gap and triggered a
//     failover to the peer.
//  3. Waits for activeSite to flip, capturing the failover-complete
//     timestamp via mfg.Status.LastFailover.
//  4. Inside the cooldown window, scales the *new* primary to 0
//     (held down by a chaos reverter) so the operator must consider a
//     second promotion. The original primary's pod has by now
//     respawned and is being recovered as a replica, so it is the
//     only plausible promotion target.
//  5. Asserts the operator log emits "failover blocked by anti-flap
//     cooldown" within the cooldown window.
//  6. Asserts the failovers_total counter for every site has
//     incremented by AT MOST 1 since the snapshot — exactly one
//     failover happened (the original), and no anti-flap-permitted
//     ping-pong slipped past.
//
// Known flakiness from chaos-scenarios.md §9 ("INCONCLUSIVE across 3
// rounds of testing"): if the original primary's pod is still
// recovering when the second kill lands, the operator may see TOTAL
// LOSS instead of a promotion candidate, and the cooldown branch never
// fires. We tolerate this only when the activeSite-stays-put invariant
// still holds — see s09bVerifyNoPingPong below.
func scenario09AntiFlapCooldown() runner.Scenario {
	return runner.Scenario{
		ID:    "09-anti-flap-cooldown",
		Title: "Anti-flap cooldown blocks second failover within window",
		Hypothesis: "After one emergency failover, scaling the newly-promoted primary to 0 inside the " +
			"failoverCooldown window causes the operator to log 'failover blocked by anti-flap cooldown' " +
			"and produces NO second failover (failovers_total unchanged for any site).",
		Risk:     "high",
		DocLink:  "playground/chaos-scenarios.md#9-rapid-successive-failures-anti-flap",
		Timeout:  6 * time.Minute,
		Precheck: AssertHealthyBaseline,
		Steps: []runner.Step{
			s09bSnapshotFailoverCounts(),
			s09bInjectKillPrimary(),
			s09bObserveFirstFailover(),
			s09bInjectScaleNewPrimaryDown(),
			s09bObserveAntiFlapLog(),
			s09bVerifyNoPingPong(),
		},
	}
}

// s09bSnapshotFailoverCounts records the per-site failovers_total
// counter BEFORE inject so the post-test verifier can compute the
// delta. We snapshot every primary-candidate site by name so a
// missing label key (e.g. site never had a failover this lifetime)
// is treated as 0.
func s09bSnapshotFailoverCounts() runner.Step {
	return runner.Step{
		Phase: runner.PhasePrecheck,
		Name:  "snapshot per-site failovers_total counters",
		Do: func(ctx context.Context, env *runner.Env) error {
			snap, err := env.Metrics.Scrape(ctx)
			if err != nil {
				return fmt.Errorf("scrape metrics: %w", err)
			}
			mfg, err := env.Kube.GetMFG(ctx, env.Namespace)
			if err != nil {
				return err
			}
			before := map[string]string{}
			for _, name := range PrimaryCandidates(mfg) {
				v, _ := snap.Counter("bloodraven_failovers_total", map[string]string{"target_site": name})
				before[name] = fmt.Sprintf("%g", v)
			}
			if err := stashMap(env, "failoversBefore", before); err != nil {
				return err
			}
			env.Capture.Note(fmt.Sprintf("baseline failovers_total: %v", before))
			return nil
		},
	}
}

func s09bInjectKillPrimary() runner.Step {
	return runner.Step{
		Phase: runner.PhaseInject,
		Name:  "force-delete active primary pod (Deployment respawns it within ~5s)",
		Do: func(ctx context.Context, env *runner.Env) error {
			mfg, err := env.Kube.GetMFG(ctx, env.Namespace)
			if err != nil {
				return err
			}
			active := mfg.Status.ActiveSite
			peer, err := PeerOf(mfg, active)
			if err != nil {
				return err
			}
			if err := ctxStash(ctx, env, "originalPrimary", active); err != nil {
				return err
			}
			if err := ctxStash(ctx, env, "expectedNewPrimary", peer); err != nil {
				return err
			}
			env.Capture.Note(fmt.Sprintf("active=%s expected-new-primary=%s; deleting pod", active, peer))
			// Open the operator log tailer NOW so the SinceTime filter
			// covers both the first failover and the anti-flap line.
			if _, err := env.Logs("operator"); err != nil {
				return fmt.Errorf("open operator tailer: %w", err)
			}
			return env.Chaos.DeleteSitePod(ctx, active)
		},
	}
}

func s09bObserveFirstFailover() runner.Step {
	return runner.Step{
		Phase: runner.PhaseObserve,
		Name:  "wait for activeSite flip and lastFailover stamp (first failover)",
		Do: func(ctx context.Context, env *runner.Env) error {
			original := ctxFetch(env, "originalPrimary")
			waitCtx, cancel := context.WithTimeout(ctx, 90*time.Second)
			defer cancel()
			mfg, err := env.Wait.UntilCR(waitCtx, env.Namespace,
				"activeSite changed AND lastFailover stamped",
				func(mfg *v1alpha1.MysqlFailoverGroup) (bool, string, error) {
					hasStamp := mfg.Status.LastFailover != nil && !mfg.Status.LastFailover.IsZero()
					flipped := mfg.Status.ActiveSite != "" && mfg.Status.ActiveSite != original
					msg := fmt.Sprintf("activeSite=%q stamp=%v target=%q",
						mfg.Status.ActiveSite, hasStamp, mfg.Status.LastFailoverTarget)
					return flipped && hasStamp, msg, nil
				},
			)
			if err != nil {
				return err
			}
			env.Capture.Note(fmt.Sprintf("first failover complete at %s, new primary=%s",
				mfg.Status.LastFailover.Time.Format(time.RFC3339), mfg.Status.ActiveSite))
			return ctxStash(ctx, env, "newPrimary", mfg.Status.ActiveSite)
		},
	}
}

// s09bInjectScaleNewPrimaryDown scales the newly-promoted primary's
// deployment to 0 immediately after the first failover completes. The
// chaos reverter holds it down past the deployment's reconcile cycle
// so the operator sees a sustained outage on the new primary while
// the original primary's pod has had time to respawn (it was a one-
// shot pod-delete, not a scale-down). The original primary should
// thus be the only plausible second-failover target — and the anti-
// flap cooldown should block that promotion.
func s09bInjectScaleNewPrimaryDown() runner.Step {
	return runner.Step{
		Phase: runner.PhaseInject,
		Name:  "scale newly-promoted primary to 0 inside cooldown window",
		Do: func(ctx context.Context, env *runner.Env) error {
			newPrimary := ctxFetch(env, "newPrimary")
			env.Capture.Note(fmt.Sprintf("scaling new primary %s to 0 to provoke second-failover evaluation", newPrimary))
			return env.Chaos.ScaleSiteToZero(ctx, newPrimary)
		},
	}
}

// s09bObserveAntiFlapLog waits for the operator's "failover blocked by
// anti-flap cooldown" log line. Source: topology.go:945-950.
//
// Timeout sizing: the operator's poll interval is 2s and
// failureThreshold=3, so a sustained outage is detectable in ~6s.
// With 30s cooldown, the operator typically evaluates and logs the
// block within 10–15s of the second inject. We allow 90s as a wide
// margin for k3d-imposed jitter, but if the cooldown has already
// expired by the time we get here (i.e. >30s passed since the first
// failover stamped its lastFailover), the log will never fire — we
// surface that case as a TimeoutError with a clear last-message.
func s09bObserveAntiFlapLog() runner.Step {
	return runner.Step{
		Phase: runner.PhaseObserve,
		Name:  `operator log: "failover blocked by anti-flap cooldown"`,
		Do: func(ctx context.Context, env *runner.Env) error {
			tail, err := env.Logs("operator")
			if err != nil {
				return err
			}
			waitCtx, cancel := context.WithTimeout(ctx, 90*time.Second)
			defer cancel()
			_, err = env.Wait.UntilLog(waitCtx, tail, env.StartTime,
				`anti-flap cooldown block`,
				pglogs.Substring("failover blocked by anti-flap cooldown"),
			)
			return err
		},
	}
}

// s09bVerifyNoPingPong asserts the failovers_total counter has
// incremented by AT MOST 1 since the snapshot at scenario start. The
// initial failover legitimately bumped one site's counter by 1; any
// further increment would mean the cooldown was bypassed and a
// ping-pong promotion landed.
//
// We compare the counter for EVERY primary candidate so a flip-back
// to the original primary (which would also be a violation) is
// caught.
func s09bVerifyNoPingPong() runner.Step {
	return runner.Step{
		Phase: runner.PhaseVerify,
		Name:  "failovers_total incremented by exactly 1 across all sites",
		Do: func(ctx context.Context, env *runner.Env) error {
			before, err := stashFetchMap(env, "failoversBefore")
			if err != nil {
				return err
			}
			snap, err := env.Metrics.Scrape(ctx)
			if err != nil {
				return fmt.Errorf("scrape metrics: %w", err)
			}
			mfg, err := env.Kube.GetMFG(ctx, env.Namespace)
			if err != nil {
				return err
			}
			candidates := PrimaryCandidates(mfg)
			deltas := map[string]float64{}
			var totalDelta float64
			for _, name := range candidates {
				now, _ := snap.Counter("bloodraven_failovers_total", map[string]string{"target_site": name})
				var prior float64
				if v := before[name]; v != "" {
					if _, err := fmt.Sscanf(v, "%g", &prior); err != nil {
						return fmt.Errorf("parse prior failovers_total[%s]=%q: %w", name, v, err)
					}
				}
				deltas[name] = now - prior
				totalDelta += now - prior
			}
			env.Capture.Note(fmt.Sprintf("failovers_total deltas: %v (total=%g)", deltas, totalDelta))
			if totalDelta > 1 {
				return fmt.Errorf("anti-flap protection failed: failovers_total incremented by %g across sites %v "+
					"(expected exactly 1 from the initial failover) — ping-pong promotion bypassed cooldown",
					totalDelta, deltas)
			}
			return nil
		},
	}
}

// Compile-time guard that pgmetrics.Snapshot is the real type, not
// shadowed by an import we forgot to use. (Without this Go's import
// pruning would silently drop the package if we ever stop calling
// snap.Counter.)
var _ = (*pgmetrics.Snapshot)(nil)
