package scenarios

import (
	"context"
	"fmt"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	v1alpha1 "github.com/shipstream/bloodraven/api/v1alpha1"
	"github.com/shipstream/bloodraven/internal/playground/runner"
)

func init() {
	runner.Register(scenario64EmergencyFailoverDuringPlannedAdmission())
}

const (
	s64ID = "64-emergency-failover-during-planned-admission"
	// s64EmergencyWindow is the budget for the emergency failover, the same
	// one scenario 01 gives an unobstructed primary loss (detection ~6s plus
	// a relay drain bounded at 30s, with CI headroom). A pending planned
	// request owns nothing, so it must not stretch this.
	s64EmergencyWindow = 90 * time.Second
	// s64OutcomeWindow covers the admission deadline (2m from Admitting,
	// spec §6.1) plus a margin, once the hold is gone.
	s64OutcomeWindow = 3 * time.Minute

	s64KeyBaselineRunID = "s64.baselineRunID"
	s64KeyRunID         = "s64.runID"
	s64KeyKilledAt      = "s64.primaryKilledAt"
)

// scenario64EmergencyFailoverDuringPlannedAdmission proves that a planned
// request waiting in admission does not obstruct emergency failover (#235,
// spec §15.5, §6.1: a pending request owns nothing).
//
// A deployment client takes a renewing failover-hold lease (the scenario 53
// machinery, in its emergency mode: 120s TTL, renewed every 5s), so the
// planned request is stamped Deferred{DeploymentHold}. The primary is then
// held down (its Deployment scaled to 0, not a pod kill the Deployment would
// undo in seconds). Emergency failover must promote the peer inside the usual
// window, revoking the deployment leases as scenario 55 asserts, which also
// releases the hold. The same planned run must then end Failed{NotStarted,
// AlreadyActive} (its target is now the primary), or proceed against the new
// primary; it must never claim the dead source.
func scenario64EmergencyFailoverDuringPlannedAdmission() runner.Scenario {
	state := &deployScenarioState{}
	return runner.Scenario{
		ID:    s64ID,
		Title: "Emergency failover is not blocked by a planned request deferred in admission",
		Hypothesis: "With a planned request Deferred{DeploymentHold} by a live failover-hold lease, holding the " +
			"primary down still promotes the peer within the emergency window (a pending request owns nothing), " +
			"revokes the deployment leases, and the planned run then ends Failed{NotStarted, AlreadyActive} or " +
			"proceeds against the new primary, never claiming the dead source.",
		Risk:    "high",
		DocLink: "playground/chaos-scenarios.md#64-emergency-failover-during-planned-admission",
		Timeout: 14 * time.Minute,
		Precheck: func(ctx context.Context, env *runner.Env) error {
			*state = deployScenarioState{}
			if err := AssertHealthyBaseline(ctx, env); err != nil {
				return err
			}
			return requireDeployAPI(ctx, env)
		},
		Steps: []runner.Step{
			{Phase: runner.PhaseInject, Name: "create a renewing failover-hold deployment client", Do: func(ctx context.Context, env *runner.Env) error {
				return state.create(ctx, env, "emergency")
			}},
			s64StashBaselineRun(),
			injectPlannedFailoverAnnotation(),
			s64ObserveDeferredByHold(),
			s64HoldPrimaryDown(),
			s64ObserveEmergencyFailover(),
			verifyFailoverMetric(),
			{Phase: runner.PhaseVerify, Name: "emergency failover revokes the deployment leases, releasing the hold", Do: func(ctx context.Context, env *runner.Env) error {
				return s64WaitLeasesRevoked(ctx, env, state)
			}},
			s64ObservePlannedOutcome(),
		},
		Cleanup: state.cleanup,
		// Lease settle (up to TTL+1s when revocation was never observed), the
		// MysqlDatabase finalizer wait, then the old primary rejoining.
		CleanupTimeout: 8 * time.Minute,
	}
}

// requireDeployAPI is scenario 53's precheck: the operator serves the deploy
// API and the playground TLS Secret its clients trust exists.
func requireDeployAPI(ctx context.Context, env *runner.Env) error {
	dep, err := env.Kube.GetDeployment(ctx, env.Namespace, "bloodraven")
	if err != nil {
		return err
	}
	enabled := false
	for _, container := range dep.Spec.Template.Spec.Containers {
		for _, variable := range container.Env {
			if variable.Name == "BLOODRAVEN_DEPLOY_API_ENABLED" && variable.Value == "true" {
				enabled = true
			}
		}
	}
	if !enabled {
		return fmt.Errorf("deploy API not enabled; prepare with BLOODRAVEN_SETUP_DEPLOY_API=1 ./playground/setup.sh")
	}
	_, err = env.Kube.Kubernetes.CoreV1().Secrets(env.Namespace).Get(ctx, "mysql-playground-tls", metav1.GetOptions{})
	return err
}

func s64StashBaselineRun() runner.Step {
	return runner.Step{
		Phase: runner.PhaseInject,
		Name:  "record the planned-failover runId before the request",
		Do: func(ctx context.Context, env *runner.Env) error {
			mfg, err := env.Kube.GetMFGNamed(ctx, env.Namespace, env.FG)
			if err != nil {
				return err
			}
			baseline := ""
			if pf := mfg.Status.PlannedFailover; pf != nil {
				baseline = pf.RunID
			}
			return ctxStash(ctx, env, s64KeyBaselineRunID, baseline)
		},
	}
}

func s64ObserveDeferredByHold() runner.Step {
	return runner.Step{
		Phase: runner.PhaseVerify,
		Name:  "planned request is Deferred with deferReason DeploymentHold",
		Do: func(ctx context.Context, env *runner.Env) error {
			baseline := ctxFetch(env, s64KeyBaselineRunID)
			waitCtx, cancel := context.WithTimeout(ctx, 60*time.Second)
			defer cancel()
			mfg, err := env.Wait.UntilCR(waitCtx, env.Namespace, "Deferred{DeploymentHold}",
				func(mfg *v1alpha1.MysqlFailoverGroup) (bool, string, error) {
					return s64DeferredByHold(currentPlannedRun(mfg, baseline))
				})
			if err != nil {
				return err
			}
			pf := mfg.Status.PlannedFailover
			env.Capture.Note("planned request deferred by the hold: " + plannedRunSummary(pf))
			return ctxStash(ctx, env, s64KeyRunID, pf.RunID)
		},
	}
}

// s64DeferredByHold classifies the current run while waiting for the hold to
// defer it. Anything that owns sites or is terminal means the hold did not
// stop admission.
func s64DeferredByHold(pf *v1alpha1.PlannedFailoverStatus) (bool, string, error) {
	if pf == nil {
		return false, "no new planned run yet", nil
	}
	msg := plannedRunSummary(pf)
	if pf.Result != nil || plannedTerminal(pf.Phase) || plannedOwnsSites(pf.Phase) || pf.Phase == v1alpha1.PlannedFailoverPhasePublishing {
		return false, msg, fmt.Errorf("planned run went past admission despite a live failover-hold: %s", msg)
	}
	if pf.Phase != v1alpha1.PlannedFailoverPhaseDeferred {
		return false, msg, nil
	}
	if pf.Admission == nil || pf.Admission.DeferReason != v1alpha1.PlannedFailoverDeferDeploymentHold {
		return false, msg, nil
	}
	return true, msg, nil
}

func s64HoldPrimaryDown() runner.Step {
	inner := injectScaleZero()
	return runner.Step{
		Phase: runner.PhaseInject,
		Name:  "hold the primary down (scale to 0) while the request is deferred",
		Do: func(ctx context.Context, env *runner.Env) error {
			if err := ctxStash(ctx, env, s64KeyKilledAt, time.Now().UTC().Format(time.RFC3339Nano)); err != nil {
				return err
			}
			return inner.Do(ctx, env)
		},
	}
}

// s64ObserveEmergencyFailover waits for the peer to be promoted, and on every
// poll checks that the pending planned run never took the claim: with its
// source down, a run that owned sites would be exactly the obstruction this
// scenario guards against.
func s64ObserveEmergencyFailover() runner.Step {
	return runner.Step{
		Phase: runner.PhaseObserve,
		Name:  "emergency failover promotes the peer within the emergency window",
		Do: func(ctx context.Context, env *runner.Env) error {
			original := ctxFetch(env, "originalPrimary")
			runID := ctxFetch(env, s64KeyRunID)
			killedAt, err := time.Parse(time.RFC3339Nano, ctxFetch(env, s64KeyKilledAt))
			if err != nil {
				return fmt.Errorf("parse stash %s: %w", s64KeyKilledAt, err)
			}
			waitCtx, cancel := context.WithDeadline(ctx, killedAt.Add(s64EmergencyWindow))
			defer cancel()
			_, err = env.Wait.UntilCR(waitCtx, env.Namespace,
				fmt.Sprintf("activeSite moves off %s within %s with the planned run pending", original, s64EmergencyWindow),
				func(mfg *v1alpha1.MysqlFailoverGroup) (bool, string, error) {
					if pf := mfg.Status.PlannedFailover; pf != nil && pf.RunID == runID {
						if plannedOwnsSites(pf.Phase) && pf.SourceSite == original {
							return false, "", fmt.Errorf("pending planned run claimed the lost primary %s: %s", original, plannedRunSummary(pf))
						}
					}
					done, msg := failoverObserved(mfg, original)
					return done, msg, nil
				})
			if err != nil {
				return err
			}
			env.Capture.Note(fmt.Sprintf("emergency failover off %s observed %s after the primary was held down",
				original, time.Since(killedAt).Round(time.Second)))
			return nil
		},
	}
}

// s64WaitLeasesRevoked waits for the deployment client to see both leases
// answered 409 revoked by a newer topology generation and stop (scenario 55's
// evidence), then for the DeploymentLeaseRevoked Event. Revocation is what
// releases the hold on the planned request.
func s64WaitLeasesRevoked(ctx context.Context, env *runner.Env, state *deployScenarioState) error {
	if err := state.waitClient(ctx, env, func(records []deployClientRecord) bool {
		return deployLeasesRevokedAndStopped(records, state.grant.Body.TopologyGeneration, 2)
	}); err != nil {
		return err
	}
	state.leasesRevoked = true
	waitCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()
	_, err := waitForMFGEvent(waitCtx, env, env.StartTime, "DeploymentLeaseRevoked", "")
	return err
}

// deployLeasesRevokedAndStopped reports whether wanted distinct lease kinds
// were answered 409 revoked, before their last confirmed expiry, at a
// topology generation newer than the grant's, and the client then stopped.
func deployLeasesRevokedAndStopped(records []deployClientRecord, grantGeneration int64, wanted int) bool {
	revoked := map[string]bool{}
	expires := map[string]time.Time{}
	stopped := false
	for _, r := range records {
		if r.Status == 200 || r.Status == 201 {
			expires[r.Kind] = r.Body.ExpiresAt
		}
		if r.Action == "renew" && r.Status == 409 && r.Body.Error == "revoked" &&
			!r.At.IsZero() && r.At.Before(expires[r.Kind]) &&
			(r.Body.Reason == "topology_changed" || r.Body.Reason == "operator_revoked") &&
			r.Body.TopologyGeneration > grantGeneration {
			revoked[r.Kind] = true
		}
		stopped = stopped || r.Action == "stopped"
	}
	return len(revoked) == wanted && stopped
}

func s64ObservePlannedOutcome() runner.Step {
	return runner.Step{
		Phase: runner.PhaseObserve,
		Name:  "planned run ends Failed{NotStarted, AlreadyActive} or proceeds against the new primary",
		Do: func(ctx context.Context, env *runner.Env) error {
			runID := ctxFetch(env, s64KeyRunID)
			newPrimary := ctxFetch(env, "expectedNewPrimary")
			original := ctxFetch(env, "originalPrimary")
			waitCtx, cancel := context.WithTimeout(ctx, s64OutcomeWindow)
			defer cancel()
			mfg, err := env.Wait.UntilCR(waitCtx, env.Namespace, "planned run "+runID+" settles after the emergency failover",
				func(mfg *v1alpha1.MysqlFailoverGroup) (bool, string, error) {
					pf := mfg.Status.PlannedFailover
					if pf == nil {
						return false, "no plannedFailover status", nil
					}
					if pf.RunID != runID {
						return false, plannedRunSummary(pf), fmt.Errorf("planned run %s replaced by %q before it settled", runID, pf.RunID)
					}
					return s64ClassifyOutcome(pf, original, newPrimary)
				})
			if err != nil {
				return err
			}
			pf := mfg.Status.PlannedFailover
			if pf.Result.Outcome == v1alpha1.PlannedFailoverOutcomeNotStarted && mfg.Status.ActiveSite != newPrimary {
				return fmt.Errorf("planned run refused AlreadyActive but status.activeSite=%q, want %q", mfg.Status.ActiveSite, newPrimary)
			}
			env.Capture.Note("planned run settled: " + plannedRunSummary(pf))
			return nil
		},
	}
}

// s64ClassifyOutcome decides whether the planned run has settled acceptably
// after the emergency failover. Accepted: Failed{NotStarted, AlreadyActive},
// or Succeeded against the new primary (source == newPrimary). A run that
// claims the lost original primary, or any other terminal result, fails.
func s64ClassifyOutcome(pf *v1alpha1.PlannedFailoverStatus, original, newPrimary string) (bool, string, error) {
	msg := plannedRunSummary(pf)
	if pf.Result == nil {
		if plannedTerminal(pf.Phase) {
			return false, msg, fmt.Errorf("terminal phase %s without result: %s", pf.Phase, msg)
		}
		if (plannedOwnsSites(pf.Phase) || pf.Phase == v1alpha1.PlannedFailoverPhasePublishing) && pf.SourceSite != newPrimary {
			return false, msg, fmt.Errorf("planned run owns sites with source %q (lost primary %s, new primary %s): %s",
				pf.SourceSite, original, newPrimary, msg)
		}
		return false, msg, nil
	}
	switch {
	case pf.Result.Outcome == v1alpha1.PlannedFailoverOutcomeNotStarted &&
		pf.Result.Reason == v1alpha1.PlannedFailoverReasonAlreadyActive:
		return true, msg, nil
	case pf.Result.Outcome == v1alpha1.PlannedFailoverOutcomeSucceeded && pf.SourceSite == newPrimary:
		return true, msg, nil
	}
	return false, msg, fmt.Errorf("planned run ended %s/%s, want NotStarted/AlreadyActive or Succeeded from %s: %s (%s)",
		pf.Result.Outcome, pf.Result.Reason, newPrimary, msg, pf.Message)
}
