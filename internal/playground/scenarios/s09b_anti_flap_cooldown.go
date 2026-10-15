package scenarios

import (
	"context"
	"fmt"
	"time"

	v1alpha1 "github.com/shipstream/bloodraven/api/v1alpha1"
	pgkube "github.com/shipstream/bloodraven/internal/playground/kube"
	pglogs "github.com/shipstream/bloodraven/internal/playground/logs"
	"github.com/shipstream/bloodraven/internal/playground/runner"
)

func init() {
	runner.Register(scenario09AntiFlapCooldown())
}

// s09bCooldown is the failoverCooldown the scenario runs under. The
// playground default (30s) is shorter than the time it takes to finish
// the first failover, confirm the old primary is back and detect the
// second outage, so the second detection could land after the cooldown
// had already expired and the cooldown branch would never evaluate. 90s
// leaves the second detection well inside the window and still keeps
// the retry inside the scenario timeout.
const s09bCooldown = 90 * time.Second

// scenario09AntiFlapCooldown verifies both halves of the anti-flap
// contract: a second primary loss inside failoverCooldown is NOT
// promoted while the cooldown runs, and IS promoted once it expires.
//
// The second half is the regression this scenario was rewritten for
// (chaos round 2026-10-15, F10). Cross-site actions run on state
// transitions, and a dead primary is a stable state, so a failover
// refused by the cooldown was never evaluated again: the group stayed
// without a primary until the dead site came back, 4m40s past cooldown
// expiry in the original reproduction. The previous version of this
// scenario asserted "no second failover within 90s" against a 30s
// cooldown, which was asserting that bug.
//
// Timing:
//
//  1. Raise spec.failoverCooldown to s09bCooldown (restored in cleanup).
//     The operator restarts its topology manager on the config change
//     and rehydrates lastFailover from status, which is harmless here.
//  2. Scale the primary to 0 and wait for the first failover.
//  3. Scale the old primary back up and wait for it to be seen as
//     reachable, so it is a read-only candidate when the second fault
//     lands.
//  4. Scale the new primary to 0 (held until cleanup).
//  5. Hard: the operator logs "failover blocked by anti-flap cooldown",
//     and status.lastFailover does not move before the cooldown ends.
//  6. Hard: a second failover to the original primary completes after
//     the cooldown ends and within 45s of it.
func scenario09AntiFlapCooldown() runner.Scenario {
	return runner.Scenario{
		ID:    "09-anti-flap-cooldown",
		Title: "Anti-flap cooldown defers a second failover, then retries it at expiry",
		Hypothesis: "After one emergency failover, scaling the new primary to 0 inside failoverCooldown logs " +
			"'failover blocked by anti-flap cooldown' and promotes nothing while the cooldown runs. When the " +
			"cooldown expires the operator retries without any new state transition and promotes the original " +
			"primary within 45s.",
		Risk:     "high",
		DocLink:  "playground/chaos-scenarios.md#9-rapid-successive-failures-anti-flap",
		Timeout:  8 * time.Minute,
		Precheck: AssertHealthyBaseline,
		Steps: []runner.Step{
			s09bRaiseCooldown(),
			s09bSnapshotFailoverCounts(),
			s09bInjectKillPrimary(),
			s09bObserveFirstFailover(),
			s09bObserveOriginalPrimaryReadOnly(),
			s09bInjectScaleNewPrimaryDown(),
			s09bVerifyBlockedDuringCooldown(),
			s09bVerifyRetriedAtExpiry(),
		},
		Cleanup:        s09bCleanup,
		CleanupTimeout: 15 * time.Minute,
	}
}

func s09bRaiseCooldown() runner.Step {
	return runner.Step{
		Phase: runner.PhasePrecheck,
		Name:  fmt.Sprintf("raise spec.failoverCooldown to %s", s09bCooldown),
		Do: func(ctx context.Context, env *runner.Env) error {
			mfg, err := env.Kube.GetMFGNamed(ctx, env.Namespace, env.FG)
			if err != nil {
				return err
			}
			original := ""
			if mfg.Spec.FailoverCooldown != nil {
				original = mfg.Spec.FailoverCooldown.Duration.String()
			}
			if err := ctxStash(ctx, env, "originalCooldown", original); err != nil {
				return err
			}
			tail, err := env.Logs("operator")
			if err != nil {
				return fmt.Errorf("open operator tailer: %w", err)
			}
			patchedAt := time.Now()
			if err := env.Kube.PatchMFGNamed(ctx, env.Namespace, env.FG, []pgkube.JSONPatchOp{{
				Op: "add", Path: "/spec/failoverCooldown", Value: s09bCooldown.String(),
			}}); err != nil {
				return fmt.Errorf("patch failoverCooldown: %w", err)
			}
			// The config change restarts the topology manager. A fault
			// injected before the new manager has observed every site is
			// judged by a manager with no history (a primary pod that
			// respawns in that window is simply re-asserted), so wait for
			// the restart before anything else.
			restartCtx, cancelRestart := context.WithTimeout(ctx, 2*time.Minute)
			_, err = env.Wait.UntilLog(restartCtx, tail, patchedAt, "topology manager restarted with the new cooldown",
				pglogs.Substring("starting topology manager"))
			cancelRestart()
			if err != nil {
				return err
			}
			// The longer cooldown also stretches any cooldown left from an
			// earlier scenario's failover; wait that out with the rest of
			// the baseline. The restarted manager publishes status from
			// scratch, so require the baseline to hold for a while rather
			// than accept one read of the old manager's status.
			const stable = 10 * time.Second
			deadline := time.Now().Add(s09bCooldown + 30*time.Second)
			var healthySince time.Time
			for {
				err := AssertHealthyBaseline(ctx, env)
				if err != nil {
					healthySince = time.Time{}
				} else if healthySince.IsZero() {
					healthySince = time.Now()
				} else if time.Since(healthySince) >= stable {
					return nil
				}
				if time.Now().After(deadline) {
					if err == nil {
						err = fmt.Errorf("baseline did not stay healthy for %s", stable)
					}
					return err
				}
				select {
				case <-ctx.Done():
					return ctx.Err()
				case <-time.After(3 * time.Second):
				}
			}
		},
	}
}

// s09bCleanup restores the cooldown and repairs any divergence. Writes
// the new primary took after the rejoining site caught up for the last
// time are lost by design when it is scaled down (asynchronous RPO), so it
// can return RecoveryBlocked, and a reader that received them is Blocked.
func s09bCleanup(ctx context.Context, env *runner.Env) error {
	if err := s09bRestoreCooldown(ctx, env); err != nil {
		return err
	}
	return repairDivergentSites(ctx, env, 12*time.Minute)
}

func s09bRestoreCooldown(ctx context.Context, env *runner.Env) error {
	original := ctxFetch(env, "originalCooldown")
	op := pgkube.JSONPatchOp{Op: "remove", Path: "/spec/failoverCooldown"}
	if original != "" {
		op = pgkube.JSONPatchOp{Op: "add", Path: "/spec/failoverCooldown", Value: original}
	}
	if err := env.Kube.PatchMFGNamed(ctx, env.Namespace, env.FG, []pgkube.JSONPatchOp{op}); err != nil {
		return fmt.Errorf("restore failoverCooldown %q: %w", original, err)
	}
	return nil
}

func s09bSnapshotFailoverCounts() runner.Step {
	return runner.Step{
		Phase: runner.PhasePrecheck,
		Name:  "snapshot per-site failovers_total counters",
		Do: func(ctx context.Context, env *runner.Env) error {
			// The cooldown patch restarted the topology manager, not the
			// operator process, so the metrics endpoint is unchanged.
			snap, err := env.Metrics.Scrape(ctx)
			if err != nil {
				return fmt.Errorf("scrape metrics: %w", err)
			}
			mfg, err := env.Kube.GetMFGNamed(ctx, env.Namespace, env.FG)
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
		Name:  "scale the active primary to 0",
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
			if err := ctxStash(ctx, env, "originalPrimary", active); err != nil {
				return err
			}
			if err := ctxStash(ctx, env, "expectedNewPrimary", peer); err != nil {
				return err
			}
			env.Capture.Note(fmt.Sprintf("active=%s expected-new-primary=%s; scaling it to 0", active, peer))
			// Scale-to-0, not a pod delete: a deleted pod respawns in
			// ~8s, and while its stale endpoint black-holes probes the
			// operator can collect fewer than failureThreshold failures
			// before it is back, so no failover happens at all.
			return env.Chaos.ScaleSiteToZero(ctx, active)
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
			first := mfg.Status.LastFailover.Time
			env.Capture.Note(fmt.Sprintf("first failover complete at %s, new primary=%s",
				first.Format(time.RFC3339), mfg.Status.ActiveSite))
			if err := ctxStash(ctx, env, "firstFailoverAt", first.UTC().Format(time.RFC3339Nano)); err != nil {
				return err
			}
			return ctxStash(ctx, env, "newPrimary", mfg.Status.ActiveSite)
		},
	}
}

// s09bObserveOriginalPrimaryReadOnly brings the original primary back
// and waits for it to be observable as reachable in CR status. Without it the operator can
// still have the original primary marked unreachable from the first
// failover when the second fault lands, and it would see TOTAL LOSS
// instead of a promotable candidate.
func s09bObserveOriginalPrimaryReadOnly() runner.Step {
	return runner.Step{
		Phase: runner.PhaseObserve,
		Name:  "original primary scaled back up and caught up as a replica",
		Do: func(ctx context.Context, env *runner.Env) error {
			original := ctxFetch(env, "originalPrimary")
			if err := env.Chaos.ScaleSiteToOne(ctx, original); err != nil {
				return err
			}
			waitCtx, cancel := context.WithTimeout(ctx, 90*time.Second)
			defer cancel()
			_, err := env.Wait.UntilCR(waitCtx, env.Namespace,
				fmt.Sprintf("site %s reachable post-failover", original),
				func(mfg *v1alpha1.MysqlFailoverGroup) (bool, string, error) {
					// Caught up, not just reachable: the second fault loses
					// whatever this site has not received yet.
					s := statusSiteByName(mfg, original)
					if s == nil {
						return false, "site missing from status", nil
					}
					caughtUp := s.State == "read-only" && s.Replicating && s.RecoveryState == "" &&
						s.SourceConvergenceState == v1alpha1.SourceConvergenceConverged &&
						s.SecondsBehindSource != nil && *s.SecondsBehindSource == 0
					return caughtUp, fmt.Sprintf("site %s state=%q replicating=%v recovery=%q convergence=%q lag=%s",
						original, s.State, s.Replicating, s.RecoveryState, s.SourceConvergenceState,
						formatLag(s.SecondsBehindSource)), nil
				},
			)
			return err
		},
	}
}

// s09bInjectScaleNewPrimaryDown holds the new primary down (scale to 0)
// so the operator sees a sustained outage while the original primary is
// a valid read-only candidate.
func s09bInjectScaleNewPrimaryDown() runner.Step {
	return runner.Step{
		Phase: runner.PhaseInject,
		Name:  "scale newly-promoted primary to 0 inside cooldown window",
		Do: func(ctx context.Context, env *runner.Env) error {
			newPrimary := ctxFetch(env, "newPrimary")
			firstAt, err := time.Parse(time.RFC3339Nano, ctxFetch(env, "firstFailoverAt"))
			if err != nil {
				return fmt.Errorf("parse firstFailoverAt: %w", err)
			}
			if remaining := time.Until(firstAt.Add(s09bCooldown)); remaining < 30*time.Second {
				return fmt.Errorf("only %s of the cooldown left before the second fault; the cooldown branch "+
					"could not be exercised reliably", remaining.Round(time.Second))
			}
			env.Capture.Note(fmt.Sprintf("scaling new primary %s to 0 to provoke second-failover evaluation", newPrimary))
			return env.Chaos.ScaleSiteToZero(ctx, newPrimary)
		},
	}
}

// s09bVerifyBlockedDuringCooldown asserts the cooldown branch evaluated
// and held: the anti-flap line is logged, and status.lastFailover does
// not move until the cooldown is about to end.
func s09bVerifyBlockedDuringCooldown() runner.Step {
	return runner.Step{
		Phase: runner.PhaseVerify,
		Name:  "anti-flap cooldown logged; no failover before expiry",
		Do: func(ctx context.Context, env *runner.Env) error {
			firstAt, err := time.Parse(time.RFC3339Nano, ctxFetch(env, "firstFailoverAt"))
			if err != nil {
				return fmt.Errorf("parse firstFailoverAt: %w", err)
			}
			expiry := firstAt.Add(s09bCooldown)
			tail, err := env.Logs("operator")
			if err != nil {
				return fmt.Errorf("open operator tailer: %w", err)
			}
			logCtx, cancelLog := context.WithDeadline(ctx, expiry)
			m, err := env.Wait.UntilLog(logCtx, tail, firstAt, "anti-flap cooldown blocks the second failover",
				pglogs.Substring("failover blocked by anti-flap cooldown"))
			cancelLog()
			if err != nil {
				return fmt.Errorf("the second primary loss was never refused by the cooldown branch: %w", err)
			}
			env.Capture.Note("anti-flap log observed: " + m.Line)

			// Hold until 3s before expiry, checking that nothing promoted.
			holdCtx, cancelHold := context.WithDeadline(ctx, expiry.Add(-3*time.Second))
			defer cancelHold()
			for {
				mfg, err := env.Kube.GetMFGNamed(ctx, env.Namespace, env.FG)
				if err != nil {
					return err
				}
				if lf := mfg.Status.LastFailover; lf != nil && lf.Time.After(firstAt.Add(time.Second)) {
					return fmt.Errorf("anti-flap bypassed: a second failover to %q completed at %s, %s before the cooldown expired",
						mfg.Status.LastFailoverTarget, lf.Time.Format(time.RFC3339), expiry.Sub(lf.Time).Round(time.Second))
				}
				select {
				case <-holdCtx.Done():
					if ctx.Err() != nil {
						return ctx.Err()
					}
					env.Capture.Note(fmt.Sprintf("no failover through %s (cooldown expiry %s)",
						time.Now().Format(time.RFC3339), expiry.Format(time.RFC3339)))
					return nil
				case <-time.After(2 * time.Second):
				}
			}
		},
	}
}

// s09bVerifyRetriedAtExpiry asserts the operator promotes the original
// primary once the cooldown expires, with no new transition to prompt
// it: the new primary stays scaled to 0 throughout.
func s09bVerifyRetriedAtExpiry() runner.Step {
	return runner.Step{
		Phase: runner.PhaseVerify,
		Name:  "second failover to the original primary within 45s of cooldown expiry",
		Do: func(ctx context.Context, env *runner.Env) error {
			original := ctxFetch(env, "originalPrimary")
			firstAt, err := time.Parse(time.RFC3339Nano, ctxFetch(env, "firstFailoverAt"))
			if err != nil {
				return fmt.Errorf("parse firstFailoverAt: %w", err)
			}
			expiry := firstAt.Add(s09bCooldown)
			waitCtx, cancel := context.WithDeadline(ctx, expiry.Add(45*time.Second))
			defer cancel()
			mfg, err := env.Wait.UntilCR(waitCtx, env.Namespace,
				fmt.Sprintf("second failover back to %s after cooldown expiry", original),
				func(mfg *v1alpha1.MysqlFailoverGroup) (bool, string, error) {
					lf := mfg.Status.LastFailover
					second := lf != nil && lf.Time.After(firstAt.Add(time.Second))
					msg := fmt.Sprintf("activeSite=%q lastFailover=%v target=%q", mfg.Status.ActiveSite, lf, mfg.Status.LastFailoverTarget)
					return second && mfg.Status.ActiveSite == original, msg, nil
				},
			)
			if err != nil {
				return fmt.Errorf("no failover after cooldown expiry at %s; the refused failover was never retried: %w",
					expiry.Format(time.RFC3339), err)
			}
			second := mfg.Status.LastFailover.Time
			env.Capture.Note(fmt.Sprintf("second failover at %s, %s after cooldown expiry",
				second.Format(time.RFC3339), second.Sub(expiry).Round(time.Second)))
			// status.lastFailover is second-granular; allow that much.
			if second.Before(expiry.Add(-time.Second)) {
				return fmt.Errorf("second failover at %s preceded cooldown expiry %s", second, expiry)
			}

			before, err := stashFetchMap(env, "failoversBefore")
			if err != nil {
				return err
			}
			snap, err := env.Metrics.Scrape(ctx)
			if err != nil {
				return fmt.Errorf("scrape metrics: %w", err)
			}
			var total float64
			for _, name := range PrimaryCandidates(mfg) {
				now, _ := snap.Counter("bloodraven_failovers_total", map[string]string{"target_site": name})
				var prior float64
				if v := before[name]; v != "" {
					if _, err := fmt.Sscanf(v, "%g", &prior); err != nil {
						return fmt.Errorf("parse prior failovers_total[%s]=%q: %w", name, v, err)
					}
				}
				total += now - prior
			}
			if total != 2 {
				return fmt.Errorf("failovers_total incremented by %g, want exactly 2 (one per fault)", total)
			}
			return nil
		},
	}
}
