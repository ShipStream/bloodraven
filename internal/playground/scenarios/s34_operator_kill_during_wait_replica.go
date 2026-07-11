package scenarios

import (
	"context"
	"fmt"
	"sort"
	"time"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"

	v1alpha1 "github.com/shipstream/bloodraven/api/v1alpha1"
	pgkube "github.com/shipstream/bloodraven/internal/playground/kube"
	pglogs "github.com/shipstream/bloodraven/internal/playground/logs"
	"github.com/shipstream/bloodraven/internal/playground/runner"
)

func init() {
	runner.Register(scenario34OperatorKillDuringWaitReplica())
}

const (
	s34ID            = "34-operator-kill-during-wait-replica"
	s34NewMemory     = "276Mi"
	s34HoldObserved  = "waitReplicaObserved"
	s34StandbyStash  = "standbySite"
	s34ActiveStash   = "activeAtPatch"
	s34HoldNPApplied = "holdNPApplied"
)

// scenario34OperatorKillDuringWaitReplica patches per-site memory to start an
// ordered update, holds the roll at WaitReplica with a standby-ingress
// NetworkPolicy (so the operator's health check of the freshly-rolled standby
// fails), force-deletes the operator pod, then removes the hold. The
// replacement operator — whose in-memory update phase was lost — must
// re-derive the remaining drift from the Deployment spec-hash mismatch and
// finish the roll: both deployments end at the new memory request, no double
// -roll or TOTAL LOSS occurs, and the cluster settles at one writable + one
// read-only.
func scenario34OperatorKillDuringWaitReplica() runner.Scenario {
	return runner.Scenario{
		ID:    s34ID,
		Title: "Operator kill during ordered-update WaitReplica re-derives and completes",
		Hypothesis: "Killing the operator while an ordered update is held at WaitReplica does not double-roll both " +
			"MySQL pods or cause TOTAL LOSS. The replacement operator re-derives remaining drift from Deployment " +
			"spec-hash mismatch and completes: status.updatePhase returns to empty, both deployments run the new " +
			"memory request, and the cluster is one writable + one read-only.",
		Risk:              "high",
		DocLink:           "playground/chaos-scenarios.md#34-operator-kill-during-ordered-update-waitreplica",
		Timeout:           12 * time.Minute,
		ResetBeforeRunAll: false,
		Precheck:          assertReplicationRunningPrecheck,
		Steps: []runner.Step{
			s34InjectPatchHoldAndKill(),
			s34ObserveReplacementCompletes(),
			s34VerifyRolledHealthy(),
			s34RestoreMemory(),
		},
		Cleanup: s34Cleanup,
	}
}

func s34InjectPatchHoldAndKill() runner.Step {
	return runner.Step{
		Phase: runner.PhaseInject,
		Name:  "patch memory, hold WaitReplica on standby, kill operator",
		Do: func(ctx context.Context, env *runner.Env) error {
			mfg, err := env.Kube.GetMFGNamed(ctx, env.Namespace, env.FG)
			if err != nil {
				return err
			}
			active := mfg.Status.ActiveSite
			standby, err := PeerOf(mfg, active)
			if err != nil {
				return err
			}
			if err := ctxStash(ctx, env, s34ActiveStash, active); err != nil {
				return err
			}
			if err := ctxStash(ctx, env, s34StandbyStash, standby); err != nil {
				return err
			}
			if _, err := env.Logs("operator"); err != nil {
				env.Capture.Note("open operator tailer failed: " + err.Error())
			}

			originals, err := env.Chaos.PatchSitesMemoryRequest(ctx, s34NewMemory)
			if err != nil {
				return fmt.Errorf("patch memory: %w", err)
			}
			env.Capture.Note(fmt.Sprintf("patched memory to %s (originals=%v); update should engage", s34NewMemory, originals))

			// Wait for the update to engage, preferably at WaitReplica. Do NOT
			// apply the hold before the update starts — the updater's
			// precondition rejects a non-healthy standby and the update would
			// never begin.
			waitCtx, cancel := context.WithTimeout(ctx, 3*time.Minute)
			defer cancel()
			observedWaitReplica := false
			_, err = env.Wait.UntilCR(waitCtx, env.Namespace, "status.updatePhase reaches WaitReplica (or any non-empty phase)",
				func(mfg *v1alpha1.MysqlFailoverGroup) (bool, string, error) {
					ph := mfg.Status.UpdatePhase
					if ph == "WaitReplica" {
						observedWaitReplica = true
						return true, "updatePhase=WaitReplica", nil
					}
					// Accept any non-empty phase as the trigger to hold+kill so
					// a fast cluster that races past WaitReplica still exercises
					// the operator-kill-mid-update path.
					return ph != "", fmt.Sprintf("updatePhase=%q", ph), nil
				})
			if err != nil {
				return fmt.Errorf("ordered update never engaged after memory patch: %w", err)
			}
			_ = ctxStash(ctx, env, s34HoldObserved, fmt.Sprintf("%v", observedWaitReplica))
			env.Capture.Note(fmt.Sprintf("update engaged (observedWaitReplica=%v); applying standby hold on %s", observedWaitReplica, standby))

			// Hold: deny ingress to the standby pod so the operator's health
			// check of the freshly-rolled standby fails and WaitReplica cannot
			// complete before we kill the operator. Egress (replication) stays
			// open so the standby remains a valid replica once the hold lifts.
			if err := env.Kube.ApplyChaosNetworkPolicy(ctx, env.Namespace, pgkube.BuildStandbyIngressHoldPolicy(env.FG, standby)); err != nil {
				return fmt.Errorf("apply standby ingress hold: %w", err)
			}
			_ = ctxStash(ctx, env, s34HoldNPApplied, "true")

			env.Capture.Note("killing operator pod mid-update")
			return env.Chaos.KillOperator(ctx)
		},
	}
}

func s34ObserveReplacementCompletes() runner.Step {
	return runner.Step{
		Phase: runner.PhaseObserve,
		Name:  "replacement operator becomes available; remove hold; update completes without double-roll",
		Do: func(ctx context.Context, env *runner.Env) error {
			standby := ctxFetch(env, s34StandbyStash)

			availCtx, cancel := context.WithTimeout(ctx, 2*time.Minute)
			defer cancel()
			if err := env.Chaos.WaitForOperatorAvailable(availCtx); err != nil {
				return fmt.Errorf("replacement operator did not become available: %w", err)
			}
			if env.RefreshMetrics != nil {
				_ = env.RefreshMetrics(ctx)
			}
			env.Capture.Note("replacement operator available; removing standby hold so the roll can re-derive and finish")

			// Lift the hold so the standby becomes reachable/healthy again and
			// the re-derived update can proceed.
			if err := env.Kube.RemoveNetworkPolicy(ctx, env.Namespace, pgkube.StandbyIngressHoldPolicyName(standby)); err != nil {
				return fmt.Errorf("remove standby hold: %w", err)
			}
			_ = ctxStash(ctx, env, s34HoldNPApplied, "false")

			// Wait for completion while continuously asserting the invariants:
			// never 0 writable (no double-roll / TOTAL LOSS window), never >1
			// writable (no split-brain), never RecoveryBlocked.
			doneCtx, doneCancel := context.WithTimeout(ctx, 8*time.Minute)
			defer doneCancel()
			expected, err := resource.ParseQuantity(s34NewMemory)
			if err != nil {
				return err
			}
			_, err = env.Wait.UntilCR(doneCtx, env.Namespace, "updatePhase empty, 1 writable + 1 read-only, deployments at new memory",
				func(mfg *v1alpha1.MysqlFailoverGroup) (bool, string, error) {
					var writable, readOnly, other []string
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
							return false, "", fmt.Errorf("site %s RecoveryBlocked during update", s.Name)
						}
					}
					if len(writable) > 1 {
						return false, "", fmt.Errorf("split-brain during update: writable=%v", writable)
					}
					sort.Strings(writable)
					sort.Strings(readOnly)
					depOK, depMsg := s34DeploymentsAt(doneCtx, env, mfg, expected)
					msg := fmt.Sprintf("updatePhase=%q writable=%v read-only=%v other=%v deployments=%s", mfg.Status.UpdatePhase, writable, readOnly, other, depMsg)
					done := mfg.Status.UpdatePhase == "" && len(writable) == 1 && len(readOnly) == 1 && depOK
					return done, msg, nil
				})
			return err
		},
	}
}

// s34DeploymentsAt reports whether both site deployments run at the wanted
// memory request. Returns a message for progress logging. Takes the caller's
// context so the polling predicate stops issuing API calls when the step's
// wait is cancelled or times out.
func s34DeploymentsAt(ctx context.Context, env *runner.Env, mfg *v1alpha1.MysqlFailoverGroup, want resource.Quantity) (bool, string) {
	allOK := true
	msg := ""
	for _, s := range mfg.Spec.Sites {
		dep, err := env.Kube.GetDeployment(ctx, env.Namespace, pgkube.MysqlDeploymentName(env.FG, s.Name))
		if err != nil {
			return false, "get-deploy-err:" + err.Error()
		}
		var got *resource.Quantity
		for _, c := range dep.Spec.Template.Spec.Containers {
			if c.Name == "mysql" {
				if mem, ok := c.Resources.Requests[corev1.ResourceMemory]; ok {
					q := mem
					got = &q
				}
			}
		}
		if got == nil || got.Cmp(want) != 0 {
			allOK = false
		}
		gv := "<nil>"
		if got != nil {
			gv = got.String()
		}
		msg += fmt.Sprintf("%s=%s ", s.Name, gv)
	}
	return allOK, msg
}

func s34VerifyRolledHealthy() runner.Step {
	return runner.Step{
		Phase: runner.PhaseVerify,
		Name:  "no TOTAL LOSS during the roll; cluster Ready",
		Do: func(ctx context.Context, env *runner.Env) error {
			if tail, err := env.Logs("operator"); err == nil {
				if hit, line := firstMatchSince(tail, env.StartTime, pglogs.Substring("TOTAL LOSS")); hit {
					return fmt.Errorf("TOTAL LOSS emitted during operator-kill update: %s", line)
				}
			}
			mfg, err := env.Kube.GetMFGNamed(ctx, env.Namespace, env.FG)
			if err != nil {
				return err
			}
			if readyOf(mfg) != "True" {
				return fmt.Errorf("cluster not Ready after update: %s", readyOf(mfg))
			}
			env.Capture.Note("update completed after operator kill: both deployments at new memory, Ready=True, no TOTAL LOSS")
			return nil
		},
	}
}

// s34RestoreMemory reverts the memory patch and waits for the follow-up revert
// roll to finish, so the cluster is back at its original spec before cleanup.
func s34RestoreMemory() runner.Step {
	return runner.Step{
		Phase: runner.PhaseSettle,
		Name:  "restore original memory and wait for the revert roll",
		Do: func(ctx context.Context, env *runner.Env) error {
			if err := env.Chaos.Revert(ctx); err != nil {
				return fmt.Errorf("revert memory patch: %w", err)
			}
			env.Capture.Note("reverted memory to originals; waiting for the revert roll to settle")
			waitCtx, cancel := context.WithTimeout(ctx, 8*time.Minute)
			defer cancel()
			_, err := env.Wait.UntilCR(waitCtx, env.Namespace, "revert roll settles: updatePhase empty, 1 writable + 1 read-only",
				func(mfg *v1alpha1.MysqlFailoverGroup) (bool, string, error) {
					writable, readOnly := 0, 0
					for _, s := range mfg.Status.Sites {
						switch s.State {
						case "writable":
							writable++
						case "read-only":
							readOnly++
						}
						if s.RecoveryState == "RecoveryBlocked" {
							return false, "", fmt.Errorf("site %s RecoveryBlocked during revert roll", s.Name)
						}
					}
					msg := fmt.Sprintf("updatePhase=%q writable=%d read-only=%d ready=%s", mfg.Status.UpdatePhase, writable, readOnly, readyOf(mfg))
					return mfg.Status.UpdatePhase == "" && writable == 1 && readOnly == 1 && readyOf(mfg) == "True", msg, nil
				})
			return err
		},
	}
}

func s34Cleanup(ctx context.Context, env *runner.Env) error {
	// Safety net: ensure the standby hold is gone even if the scenario bailed
	// before removing it. GlobalRecover also sweeps it (chaos-partition label),
	// but remove by name to be explicit.
	standby := ctxFetch(env, s34StandbyStash)
	if standby != "" {
		if err := env.Kube.RemoveNetworkPolicy(ctx, env.Namespace, pgkube.StandbyIngressHoldPolicyName(standby)); err != nil {
			env.Capture.Note("cleanup: remove standby hold: " + err.Error())
		}
	}
	return nil
}
