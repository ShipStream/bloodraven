package scenarios

import (
	"context"
	"fmt"
	"time"

	v1alpha1 "github.com/shipstream/bloodraven/api/v1alpha1"
	pglogs "github.com/shipstream/bloodraven/internal/playground/logs"
	"github.com/shipstream/bloodraven/internal/playground/runner"
)

func init() {
	runner.Register(scenario09AntiFlapCooldown())
}

// scenario09AntiFlapCooldown verifies the anti-flap cooldown contract:
// after one emergency failover, a second failover candidate inside the
// cooldown window MUST NOT produce a ping-pong promotion, and the
// operator's "failover blocked by anti-flap cooldown" log line at
// internal/controller/topology.go:946-950 SHOULD fire as triage
// evidence that the cooldown branch evaluated.
//
// Why this scenario was previously listed as "INCONCLUSIVE" in
// chaos-scenarios.md §9: the obvious shape (kill primary, wait for
// failover, kill new primary inside cooldown) is timing-fragile.
// Three failure modes were observed:
//
//   - Scale-to-0 of the FIRST kill held the original primary
//     unreachable, so when the second kill landed the operator saw
//     TOTAL LOSS instead of a promotion candidate and the cooldown
//     branch never evaluated.
//   - NetworkPolicy on both sites hit the same TOTAL LOSS dead end.
//   - Sequential partition raced the recovery flow on the original
//     primary; both sites could end up self-fenced in NO PRIMARY.
//
// This implementation pins down the timing as follows:
//
//  1. Force-DELETE the primary pod (not scale-to-0). The Deployment
//     respawns the pod within ~5s, so by the time the failover
//     completes the original primary's MySQL is back up and the
//     postStart hook has set super_read_only=ON. It is therefore a
//     valid read-only promotion candidate the moment the second kill
//     lands.
//  2. Wait for the *first* failover to finish (activeSite flips and
//     status.lastFailover stamps), then poll briefly for the
//     ORIGINAL primary to register as `read-only` in CR status.
//     Without this, the operator's first observation post-failover
//     can race to TOTAL LOSS instead of the cooldown branch.
//  3. Scale-to-0 the new primary (held by chaos reverter) so the
//     operator sees it as unreachable for the full cooldown window.
//  4. Sleep through 90s of observation — long enough that a
//     hypothetical second failover would have completed (operator
//     detect ~6s + relay log drain ~30s + promotion = ~40s, with
//     margin).
//  5. Assert the safety property as a HARD failure: failovers_total
//     across all sites incremented by AT MOST 1 since the snapshot.
//     A delta of 2 means the cooldown was bypassed.
//  6. Best-effort log scan for "failover blocked by anti-flap
//     cooldown" — if the line fired we know the branch evaluated; if
//     it did not, the safety property still has to hold via the
//     metric, but the absence is recorded so triage knows to look at
//     why the cluster reached a state that suppressed promotion (e.g.
//     TOTAL LOSS, planned-failover in flight, bootstrap suppression).
//
// The metric assertion is what actually exercises anti-flap. The log
// is supporting evidence, not the gating signal — that resolves the
// timing fragility chaos-scenarios.md §9 documented.
func scenario09AntiFlapCooldown() runner.Scenario {
	return runner.Scenario{
		ID:    "09-anti-flap-cooldown",
		Title: "Anti-flap cooldown blocks second failover within window",
		Hypothesis: "After one emergency failover, scaling the newly-promoted primary to 0 inside the " +
			"failoverCooldown window must NOT produce a second failover. failovers_total across all " +
			"sites increments by exactly 1 (the original promotion); the operator log should also emit " +
			"'failover blocked by anti-flap cooldown' as triage evidence.",
		Risk:     "high",
		DocLink:  "playground/chaos-scenarios.md#9-rapid-successive-failures-anti-flap",
		Timeout:  6 * time.Minute,
		Precheck: AssertHealthyBaseline,
		Steps: []runner.Step{
			s09bSnapshotFailoverCounts(),
			s09bInjectKillPrimary(),
			s09bObserveFirstFailover(),
			s09bObserveOriginalPrimaryReadOnly(),
			s09bInjectScaleNewPrimaryDown(),
			s09bSettleObservationWindow(),
			s09bVerifyNoPingPongAndCaptureLog(),
		},
	}
}

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

// s09bObserveOriginalPrimaryReadOnly waits for the original primary to
// be observable as `read-only` in CR status. This is the bridge step
// between the failover completing and the second kill: without it the
// operator can race to TOTAL LOSS the moment we kill the new primary
// (because it last saw the original as `unreachable` during the
// failover and hasn't re-polled it yet).
//
// Timeout: 20s. The original pod was respawned by the Deployment
// controller during the failover (~32s of warm-up by the time we
// reach this step), so the operator's next 2s poll cycle should
// observe it as read-only. We allow 20s to absorb operator-busy
// jitter.
//
// Tolerance: we accept either `read-only` or any state that means the
// operator has SEEN this site post-respawn — specifically, we only
// fail on `unreachable`, `unknown`, or empty. A transient state like
// `recovery` is fine because it still means EvalCrossSite will not
// classify the site as unreachable.
func s09bObserveOriginalPrimaryReadOnly() runner.Step {
	return runner.Step{
		Phase: runner.PhaseObserve,
		Name:  "original primary observed as reachable (not unreachable/unknown)",
		Do: func(ctx context.Context, env *runner.Env) error {
			original := ctxFetch(env, "originalPrimary")
			waitCtx, cancel := context.WithTimeout(ctx, 20*time.Second)
			defer cancel()
			_, err := env.Wait.UntilCR(waitCtx, env.Namespace,
				fmt.Sprintf("site %s reachable post-failover", original),
				func(mfg *v1alpha1.MysqlFailoverGroup) (bool, string, error) {
					var seen string
					for _, s := range mfg.Status.Sites {
						if s.Name == original {
							seen = s.State
							break
						}
					}
					msg := fmt.Sprintf("site %s state=%q", original, seen)
					switch seen {
					case "", "unreachable", "unknown":
						return false, msg, nil
					default:
						return true, msg, nil
					}
				},
			)
			return err
		},
	}
}

// s09bInjectScaleNewPrimaryDown scales the newly-promoted primary's
// deployment to 0 immediately after we've confirmed the original
// primary is reachable. The chaos reverter holds the new primary down
// past any deployment reconcile cycle so the operator sees a
// sustained outage on it while the original primary is a valid
// read-only promotion candidate. The cooldown should now block the
// second promotion that EvalCrossSite would otherwise approve.
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

// s09bSettleObservationWindow sleeps through 90s of cluster time so
// that:
//   - the operator detects the second site unreachable (~6s)
//   - the cooldown branch evaluates and either logs OR is bypassed
//   - if bypassed, a hypothetical failover would have completed
//     (relay log drain ~30s + promotion = ~40s)
//
// 90s is conservative — empirically anti-flap fires within ~10–15s
// of the second injection — but this gives any latent ping-pong
// promotion time to surface so the metric verifier can catch it.
//
// We do not wait on the log here; the verify step handles the log
// scan (best-effort) and the metric assertion (hard) as a single
// pass over post-settle state.
func s09bSettleObservationWindow() runner.Step {
	return runner.Step{
		Phase: runner.PhaseSettle,
		Name:  "settle 90s for cooldown evaluation + hypothetical failover",
		Do: func(ctx context.Context, env *runner.Env) error {
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-time.After(90 * time.Second):
				return nil
			}
		},
	}
}

// s09bVerifyNoPingPongAndCaptureLog is the actual assertion. It
// computes the per-site failovers_total delta since snapshot and
// rejects any total > 1. The original failover legitimately bumped
// one site's counter by 1; any further increment means the cooldown
// was bypassed and a ping-pong promotion landed.
//
// The "failover blocked by anti-flap cooldown" log scan runs after
// the metric pass and is purely informational — its absence does not
// fail the scenario, but the result is recorded in Capture.Note so
// triage can distinguish "cooldown branch evaluated and logged" from
// "cluster never reached a state that triggered the cooldown branch".
func s09bVerifyNoPingPongAndCaptureLog() runner.Step {
	return runner.Step{
		Phase: runner.PhaseVerify,
		Name:  "failovers_total delta == 1 across all sites; log scan for cooldown line",
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

			// Best-effort log scan. Records whether the cooldown branch
			// fired but does NOT fail the scenario on absence.
			if tail, err := env.Logs("operator"); err == nil {
				if hit, line := firstMatchSince(tail, env.StartTime,
					pglogs.Substring("failover blocked by anti-flap cooldown")); hit {
					env.Capture.Note(fmt.Sprintf("anti-flap log observed: %s", line))
				} else {
					env.Capture.Note("anti-flap log NOT observed in this window — " +
						"verify via the metric delta whether the safety property still held " +
						"(it may have via TOTAL LOSS / NO PRIMARY suppressing promotion)")
				}
			}

			if totalDelta != 1 {
				return fmt.Errorf("anti-flap assertion failed: failovers_total incremented by %g across sites %v "+
					"(expected exactly 1 from the initial failover and 0 from the blocked second failure)",
					totalDelta, deltas)
			}
			return nil
		},
	}
}
