package scenarios

import (
	"context"
	"fmt"
	"strconv"
	"time"

	v1alpha1 "github.com/shipstream/bloodraven/api/v1alpha1"
	brcontroller "github.com/shipstream/bloodraven/internal/controller"
	"github.com/shipstream/bloodraven/internal/playground/runner"
)

// s23PostRestartStampDrift is the tolerance applied to
// status.lastFailover when comparing pre- and post-operator-restart
// values. The CR-level contract is "the stamp doesn't regress to zero
// and doesn't jump forward as if a fresh failover happened". A
// status-enrichment loop can legitimately rewrite the same field with
// a slightly later timestamp during the post-restart reconcile, so
// strict equality is the wrong assertion. 90s is comfortably wider
// than the observed enrichment cadence and matches what
// playground/chaos-scenarios.md §23 documents.
const s23PostRestartStampDrift = 90 * time.Second

func init() {
	runner.Register(scenario23FailoverStateDurability())
}

// scenario23FailoverStateDurability is the regression test the
// project wishlist asks for in WISHLIST.md item #38. The contract: the
// post-failover state stored in both CR status and the out-of-band
// annotations MUST survive an operator pod restart so the
// new operator process can rehydrate its in-memory anti-flap timer and
// fence-returning-old-primary state from CR ground truth instead of
// from a fresh-start (no failover ever happened) view.
//
// Concretely the scenario:
//
//  1. Triggers a clean failover (scale primary to 0, wait for flip).
//  2. Snapshots status.lastFailoverTarget and status.lastFailover.
//  3. Kills the operator pod and waits for the Deployment to bring it
//     back. The new operator pod is a fresh process — anything it
//     "knows" about the prior failover came from the CR.
//  4. Re-reads the CR after restart and asserts both fields are
//     unchanged. status.activeSite must also still match the post-
//     failover primary so the operator hasn't "forgotten" who is
//     primary and re-promoted from scratch.
//
// What this does NOT cover: the wishlist also flags "related anti-
// flap state and old-primary-recovery dispatch keys" as in-memory
// only and not durable across restart. Those are not surfaced in the
// CR, so a chaos scenario can only assert their CR-visible side
// effects (e.g., a planned-failover request still respects the
// cooldown after restart). Future scoping for s23 — for now we
// regression-test the bare CR contract.
func scenario23FailoverStateDurability() runner.Scenario {
	return runner.Scenario{
		ID:    "23-failover-state-durability",
		Title: "both durable failover-state copies survive operator restart",
		Hypothesis: "After a clean failover, killing and restarting the operator pod must NOT clear " +
			"the status or annotation anti-flap records. The post-restart operator must rehydrate " +
			"the effective record; activeSite must remain at the post-failover primary.",
		Risk:     "low",
		DocLink:  "playground/chaos-scenarios.md#23-failover-state-durability",
		Timeout:  4 * time.Minute,
		Precheck: AssertHealthyBaseline,
		Steps: []runner.Step{
			s23InjectKillPrimary(),
			s23ObserveFailoverComplete(),
			s23InjectKillOperator(),
			s23VerifyStateSurvivedRestart(),
		},
	}
}

func s23InjectKillPrimary() runner.Step {
	return runner.Step{
		Phase: runner.PhaseInject,
		Name:  "scale active primary to 0 to trigger failover",
		Do: func(ctx context.Context, env *runner.Env) error {
			mfg, err := env.Kube.GetMFGNamed(ctx, env.Namespace, env.FG)
			if err != nil {
				return err
			}
			active := mfg.Status.ActiveSite
			env.Capture.Note(fmt.Sprintf("active primary at start: %s", active))
			if err := ctxStash(ctx, env, "originalPrimary", active); err != nil {
				return err
			}
			return env.Chaos.ScaleSiteToZero(ctx, active)
		},
	}
}

func s23ObserveFailoverComplete() runner.Step {
	return runner.Step{
		Phase: runner.PhaseObserve,
		Name:  "wait for activeSite flip and both durable failover records",
		Do: func(ctx context.Context, env *runner.Env) error {
			original := ctxFetch(env, "originalPrimary")
			waitCtx, cancel := context.WithTimeout(ctx, 90*time.Second)
			defer cancel()
			mfg, err := env.Wait.UntilCR(waitCtx, env.Namespace,
				"activeSite flipped AND status/annotation failover records stamped",
				func(mfg *v1alpha1.MysqlFailoverGroup) (bool, string, error) {
					hasTarget := mfg.Status.LastFailoverTarget != ""
					hasStamp := mfg.Status.LastFailover != nil && !mfg.Status.LastFailover.IsZero()
					oob, oobErr := brcontroller.FailoverRecordFromAnnotations(mfg.GetAnnotations())
					if oobErr != nil {
						return false, "annotation record invalid", oobErr
					}
					oobMatches := hasStamp && oob.LastFailoverTarget == mfg.Status.LastFailoverTarget &&
						oob.LastFailover.Equal(mfg.Status.LastFailover.Time)
					flipped := mfg.Status.ActiveSite != "" && mfg.Status.ActiveSite != original
					msg := fmt.Sprintf("activeSite=%q target=%q stamp=%v annotationsMatch=%v",
						mfg.Status.ActiveSite, mfg.Status.LastFailoverTarget, hasStamp, oobMatches)
					return flipped && hasTarget && hasStamp && oobMatches, msg, nil
				},
			)
			if err != nil {
				return err
			}
			if err := ctxStash(ctx, env, "preRestartTarget", mfg.Status.LastFailoverTarget); err != nil {
				return err
			}
			if err := ctxStash(ctx, env, "preRestartActive", mfg.Status.ActiveSite); err != nil {
				return err
			}
			if err := ctxStash(ctx, env, "preRestartStamp", mfg.Status.LastFailover.Time.UTC().Format(time.RFC3339Nano)); err != nil {
				return err
			}
			if err := ctxStash(ctx, env, "preRestartStampUnix", fmt.Sprintf("%d", mfg.Status.LastFailover.Time.UTC().UnixNano())); err != nil {
				return err
			}
			env.Capture.Note(fmt.Sprintf("post-failover snapshot: active=%s target=%s stamp=%s",
				mfg.Status.ActiveSite, mfg.Status.LastFailoverTarget,
				mfg.Status.LastFailover.Time.UTC().Format(time.RFC3339Nano)))
			return nil
		},
	}
}

func s23InjectKillOperator() runner.Step {
	return runner.Step{
		Phase: runner.PhaseInject,
		Name:  "kill operator pod and wait for Deployment respawn",
		Do: func(ctx context.Context, env *runner.Env) error {
			env.Capture.Note("killing operator to force a fresh process to rehydrate from CR")
			if err := env.Chaos.KillOperator(ctx); err != nil {
				return err
			}
			// The operator Deployment respawns the pod within a few
			// seconds. We sleep enough to cover the kill + restart +
			// initial reconcile cycle so by the time the verify step
			// reads the CR the new operator has had a chance to (a)
			// observe the CR, (b) rehydrate its in-memory state, and
			// (c) rewrite status if it disagreed with what was there.
			//
			// 30s = pod respawn (~5–10s) + first reconcile (~5–15s) +
			// margin. Going much lower risks reading the CR before
			// the new operator has done anything; the assertion would
			// pass spuriously because the old operator's status writes
			// haven't been overwritten yet.
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-time.After(30 * time.Second):
			}
			env.Capture.Note("30s elapsed since operator kill; reading CR for post-restart state")
			return nil
		},
	}
}

func s23VerifyStateSurvivedRestart() runner.Step {
	return runner.Step{
		Phase: runner.PhaseVerify,
		Name:  "status, annotations, and activeSite preserved across restart",
		Do: func(ctx context.Context, env *runner.Env) error {
			expectedTarget := ctxFetch(env, "preRestartTarget")
			expectedActive := ctxFetch(env, "preRestartActive")
			expectedStamp := ctxFetch(env, "preRestartStamp")
			expectedStampUnixRaw := ctxFetch(env, "preRestartStampUnix")
			expectedStampUnix, err := strconv.ParseInt(expectedStampUnixRaw, 10, 64)
			if err != nil {
				return fmt.Errorf("parse stashed preRestartStampUnix=%q: %w", expectedStampUnixRaw, err)
			}
			mfg, err := env.Kube.GetMFGNamed(ctx, env.Namespace, env.FG)
			if err != nil {
				return err
			}
			if mfg.Status.LastFailoverTarget != expectedTarget {
				return fmt.Errorf("lastFailoverTarget changed across operator restart: pre=%q post=%q",
					expectedTarget, mfg.Status.LastFailoverTarget)
			}
			if mfg.Status.ActiveSite != expectedActive {
				return fmt.Errorf("activeSite changed across operator restart: pre=%q post=%q",
					expectedActive, mfg.Status.ActiveSite)
			}
			if mfg.Status.LastFailover == nil || mfg.Status.LastFailover.IsZero() {
				return fmt.Errorf("lastFailover cleared across operator restart: pre=%q post=<nil>", expectedStamp)
			}
			oob, err := brcontroller.FailoverRecordFromAnnotations(mfg.GetAnnotations())
			if err != nil {
				return fmt.Errorf("annotation failover record invalid after restart: %w", err)
			}
			if oob.LastFailoverTarget != expectedTarget {
				return fmt.Errorf("annotation last-failover-target changed across restart: pre=%q post=%q", expectedTarget, oob.LastFailoverTarget)
			}
			if oob.LastFailover.IsZero() {
				return fmt.Errorf("annotation last-failover cleared across operator restart: pre=%q post=<nil>", expectedStamp)
			}
			if !oob.LastFailover.Equal(mfg.Status.LastFailover.Time) {
				return fmt.Errorf("status and annotation last-failover differ after restart: status=%s annotation=%s",
					mfg.Status.LastFailover.Time.UTC().Format(time.RFC3339Nano),
					oob.LastFailover.Format(time.RFC3339Nano))
			}
			postStamp := mfg.Status.LastFailover.Time.UTC().Format(time.RFC3339Nano)
			postStampUnix := mfg.Status.LastFailover.Time.UTC().UnixNano()
			delta := time.Duration(postStampUnix - expectedStampUnix)
			if delta < 0 {
				return fmt.Errorf("lastFailover stamp regressed across operator restart: pre=%s post=%s (delta=%s)",
					expectedStamp, postStamp, delta)
			}
			if delta > s23PostRestartStampDrift {
				return fmt.Errorf("lastFailover stamp advanced by %s across operator restart (tolerance %s) — looks like a fresh failover, not a status-enrichment rewrite: pre=%s post=%s",
					delta, s23PostRestartStampDrift, expectedStamp, postStamp)
			}
			if annotationDelta := oob.LastFailover.Sub(time.Unix(0, expectedStampUnix)); annotationDelta < 0 || annotationDelta > s23PostRestartStampDrift {
				return fmt.Errorf("annotation last-failover changed outside tolerance across restart: pre=%s post=%s delta=%s",
					expectedStamp, oob.LastFailover.Format(time.RFC3339Nano), annotationDelta)
			}
			env.Capture.Note(fmt.Sprintf("post-restart CR matches: active=%s target=%s stamp=%s (drift=%s within %s)",
				mfg.Status.ActiveSite, mfg.Status.LastFailoverTarget, postStamp, delta, s23PostRestartStampDrift))
			return nil
		},
	}
}
