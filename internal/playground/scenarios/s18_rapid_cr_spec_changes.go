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
	"github.com/shipstream/bloodraven/internal/playground/runner"
)

func init() {
	runner.Register(scenario18RapidCRSpecChanges())
}

const (
	s18PatchCount    = 5
	s18PatchInterval = 2 * time.Second
	s18PatchBaseMi   = 260 // 260Mi, 270Mi, 280Mi, 290Mi, 300Mi
)

// scenario18RapidCRSpecChanges hammers the MFG with five JSON patches
// during an active failover and asserts the reconciler still converges
// to exactly one writable site. The patches each replace
// spec.sites[*].resources.requests.memory with a slightly different
// value (260Mi, 270Mi, ..., 300Mi). The operator may log
// "Operation cannot be fulfilled on deployments.apps" optimistic-
// concurrency conflicts during the storm — we *expect* those and
// treat their absence as informational, not a failure (controller-
// runtime's retry behavior may dedupe them).
//
// The end-state invariants are the assertion:
//   - exactly one site has state=writable, every follower has state=read-only
//   - no sites are RecoveryBlocked
//   - the final memory request matches the LAST patch we applied
//     (s18PatchBaseMi + (s18PatchCount-1)*10)
//
// We do NOT assert the path through phases — what matters is that the
// reconciliation storm does not break the convergence guarantee.
func scenario18RapidCRSpecChanges() runner.Scenario {
	return runner.Scenario{
		ID:    "18-rapid-cr-spec-changes-during-failover",
		Title: "Rapid CR spec changes during failover converge cleanly",
		Hypothesis: "Triggering a failover and immediately applying five rapid memory-request patches " +
			"yields a clean converged state: exactly one writable site, every follower read-only, no " +
			"RecoveryBlocked, and final memory request matches the last patch applied.",
		Risk:     "high",
		DocLink:  "playground/chaos-scenarios.md#18-rapid-cr-spec-changes-during-active-failover",
		Timeout:  10 * time.Minute,
		Precheck: AssertHealthyBaseline,
		Steps: []runner.Step{
			s18InjectFailoverAndPatchStorm(),
			s18ObserveActiveSiteFlip(),
			s18ScaleOldPrimaryBackUp(),
			s18ObserveConvergence(),
			s18VerifyFinalMemoryRequest(),
		},
		Cleanup: s18RestoreOriginalMemory,
	}
}

func s18InjectFailoverAndPatchStorm() runner.Step {
	return runner.Step{
		Phase: runner.PhaseInject,
		Name:  "kill primary then apply 5 rapid memory-request patches",
		Do: func(ctx context.Context, env *runner.Env) error {
			mfg, err := env.Kube.GetMFGNamed(ctx, env.Namespace, env.FG)
			if err != nil {
				return err
			}
			active := mfg.Status.ActiveSite
			env.Capture.Note(fmt.Sprintf("active=%s; capturing original memory requests for cleanup", active))
			if err := ctxStash(ctx, env, "originalPrimary", active); err != nil {
				return err
			}

			// Snapshot original per-site memory requests so the cleanup
			// hook can restore them. We deliberately do NOT push these
			// reverters onto chaos.Revert — the storm patches drift the
			// CR several times, and a per-patch reverter would be wrong.
			originals := map[string]string{}
			for _, s := range mfg.Spec.Sites {
				if mem, ok := s.Resources.Requests[corev1.ResourceMemory]; ok {
					originals[s.Name] = mem.String()
				}
			}
			if err := stashMap(env, "originalMemoryRequests", originals); err != nil {
				return err
			}

			if _, err := env.Logs("operator"); err != nil {
				return fmt.Errorf("open operator tailer: %w", err)
			}
			// Trigger the failover via scale=0 (this pushes a reverter
			// that scales back to 1 at executor cleanup).
			if err := env.Chaos.ScaleSiteToZero(ctx, active); err != nil {
				return err
			}
			// Apply patches sequentially with sleeps; doing them in
			// parallel would create patch-vs-patch conflicts that
			// aren't representative of how a human or controller
			// hammers a CR.
			finalMi := s18PatchBaseMi + (s18PatchCount-1)*10
			for i := 0; i < s18PatchCount; i++ {
				select {
				case <-ctx.Done():
					return ctx.Err()
				case <-time.After(s18PatchInterval):
				}
				memMi := s18PatchBaseMi + i*10
				memValue := fmt.Sprintf("%dMi", memMi)
				current, err := env.Kube.GetMFGNamed(ctx, env.Namespace, env.FG)
				if err != nil {
					return fmt.Errorf("get MFG before patch %d: %w", i+1, err)
				}
				var ops []pgkube.JSONPatchOp
				for j := range current.Spec.Sites {
					ops = append(ops, pgkube.JSONPatchOp{
						Op:    "replace",
						Path:  fmt.Sprintf("/spec/sites/%d/resources/requests/memory", j),
						Value: memValue,
					})
				}
				if err := env.Kube.PatchMFGNamed(ctx, env.Namespace, env.FG, ops); err != nil {
					return fmt.Errorf("patch %d (%s): %w", i+1, memValue, err)
				}
				env.Capture.Note(fmt.Sprintf("patch %d/%d applied: memory=%s", i+1, s18PatchCount, memValue))
			}
			return ctxStash(ctx, env, "finalMemoryValue", fmt.Sprintf("%dMi", finalMi))
		},
	}
}

func s18ObserveActiveSiteFlip() runner.Step {
	return runner.Step{
		Phase: runner.PhaseObserve,
		Name:  "wait for activeSite to flip away from the original",
		Do: func(ctx context.Context, env *runner.Env) error {
			original := ctxFetch(env, "originalPrimary")
			waitCtx, cancel := context.WithTimeout(ctx, 3*time.Minute)
			defer cancel()
			_, err := env.Wait.UntilCR(waitCtx, env.Namespace,
				fmt.Sprintf("activeSite changes from %s", original),
				func(mfg *v1alpha1.MysqlFailoverGroup) (bool, string, error) {
					msg := fmt.Sprintf("activeSite=%q updatePhase=%q",
						mfg.Status.ActiveSite, mfg.Status.UpdatePhase)
					return mfg.Status.ActiveSite != "" && mfg.Status.ActiveSite != original, msg, nil
				},
			)
			return err
		},
	}
}

func s18ScaleOldPrimaryBackUp() runner.Step {
	return runner.Step{
		Phase: runner.PhaseInject,
		Name:  "scale old primary back to 1",
		Do: func(ctx context.Context, env *runner.Env) error {
			original := ctxFetch(env, "originalPrimary")
			if err := env.Chaos.ScaleSiteToOne(ctx, original); err != nil {
				return fmt.Errorf("scale old primary back up: %w", err)
			}
			env.Capture.Note("old primary scaled back to 1; waiting for convergence")
			return nil
		},
	}
}

// s18ObserveConvergence waits for the cluster to settle: exactly one
// writable site, every follower read-only, no RecoveryBlocked, and
// status.updatePhase empty (the rolling-update controller may engage
// during the patch storm to roll the new memory request).
func s18ObserveConvergence() runner.Step {
	return runner.Step{
		Phase: runner.PhaseObserve,
		Name:  "cluster converges to {1 writable, N-1 read-only} with updatePhase empty",
		Do: func(ctx context.Context, env *runner.Env) error {
			waitCtx, cancel := context.WithTimeout(ctx, 8*time.Minute)
			defer cancel()
			_, err := env.Wait.UntilCR(waitCtx, env.Namespace,
				"writable=1 read-only=N-1 blocked=0 updatePhase=\"\"",
				func(mfg *v1alpha1.MysqlFailoverGroup) (bool, string, error) {
					var writable, readOnly, other, blocked []string
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
							blocked = append(blocked, s.Name)
						}
					}
					sort.Strings(writable)
					sort.Strings(readOnly)
					msg := fmt.Sprintf("writable=%v read-only=%v other=%v blocked=%v updatePhase=%q",
						writable, readOnly, other, blocked, mfg.Status.UpdatePhase)
					done := len(writable) == 1 && len(readOnly) == len(mfg.Status.Sites)-1 && len(blocked) == 0 && mfg.Status.UpdatePhase == ""
					return done, msg, nil
				},
			)
			return err
		},
	}
}

func s18VerifyFinalMemoryRequest() runner.Step {
	return runner.Step{
		Phase: runner.PhaseVerify,
		Name:  "every site deployment ends at the last-applied memory request",
		Do: func(ctx context.Context, env *runner.Env) error {
			finalMem := ctxFetch(env, "finalMemoryValue")
			expected, err := resource.ParseQuantity(finalMem)
			if err != nil {
				return fmt.Errorf("parse finalMemoryValue %q: %w", finalMem, err)
			}
			waitCtx, cancel := context.WithTimeout(ctx, 8*time.Minute)
			defer cancel()
			_, err = env.Wait.UntilCR(waitCtx, env.Namespace,
				fmt.Sprintf("deployments memory=%s", expected.String()),
				func(mfg *v1alpha1.MysqlFailoverGroup) (bool, string, error) {
					var bad []string
					for _, s := range mfg.Spec.Sites {
						dep, err := env.Kube.GetDeployment(ctx, env.Namespace, fmt.Sprintf("mysql-%s-%s", env.FG, s.Name))
						if err != nil {
							bad = append(bad, fmt.Sprintf("%s=get-deployment: %v", s.Name, err))
							continue
						}
						var got *resource.Quantity
						for _, c := range dep.Spec.Template.Spec.Containers {
							if c.Name != "mysql" {
								continue
							}
							if mem, ok := c.Resources.Requests[corev1.ResourceMemory]; ok {
								q := mem
								got = &q
							}
						}
						if got == nil {
							bad = append(bad, fmt.Sprintf("%s=missing", s.Name))
							continue
						}
						if got.Cmp(expected) != 0 {
							bad = append(bad, fmt.Sprintf("%s=%s (want %s)", s.Name, got.String(), expected.String()))
						}
					}
					sort.Strings(bad)
					msg := fmt.Sprintf("updatePhase=%q bad=%v", mfg.Status.UpdatePhase, bad)
					return len(bad) == 0, msg, nil
				},
			)
			if err != nil {
				return err
			}
			env.Capture.Note(fmt.Sprintf("all deployments at final memory=%s", expected.String()))
			return nil
		},
	}
}

// s18RestoreOriginalMemory undoes the storm's last patch by restoring
// each site's pre-storm memory request. Best-effort: if the cluster is
// mid-recovery, we still issue the patch; controller-runtime will retry
// on next reconcile.
func s18RestoreOriginalMemory(ctx context.Context, env *runner.Env) error {
	originals, err := stashFetchMap(env, "originalMemoryRequests")
	if err != nil {
		return fmt.Errorf("cleanup: fetch original memory requests: %w", err)
	}
	if len(originals) == 0 {
		return nil
	}
	current, err := env.Kube.GetMFGNamed(ctx, env.Namespace, env.FG)
	if err != nil {
		return fmt.Errorf("cleanup: get MFG: %w", err)
	}
	var ops []pgkube.JSONPatchOp
	for i, s := range current.Spec.Sites {
		orig, ok := originals[s.Name]
		if !ok || orig == "" {
			continue
		}
		ops = append(ops, pgkube.JSONPatchOp{
			Op:    "replace",
			Path:  fmt.Sprintf("/spec/sites/%d/resources/requests/memory", i),
			Value: orig,
		})
	}
	if len(ops) == 0 {
		return nil
	}
	if err := env.Kube.PatchMFGNamed(ctx, env.Namespace, env.FG, ops); err != nil {
		return fmt.Errorf("cleanup: restore memory requests: %w", err)
	}
	env.Capture.Note(fmt.Sprintf("cleanup: restored memory originals %v", originals))
	return nil
}
