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
	runner.Register(scenario05SplitBrainAutoResolve())
}

// scenario05SplitBrainAutoResolve forces both sites writable by clearing
// super_read_only on the read-only site, then asserts that the operator
// fences the non-preferred site per spec.splitBrainPolicy.sitePriorities,
// emits the auto-resolve metric, and logs the canonical msg string.
//
// Precondition: spec.splitBrainPolicy.sitePriorities must be set on the
// MFG. The playground manifest does not set it by default; without it
// the precheck fails the scenario with a message that points at the
// missing config (the runner has no first-class skip mechanism today).
func scenario05SplitBrainAutoResolve() runner.Scenario {
	return runner.Scenario{
		ID:    "05-split-brain-auto-resolve",
		Title: "Split-brain auto-resolve picks the priority site",
		Hypothesis: "When two sites are simultaneously writable and spec.splitBrainPolicy.sitePriorities " +
			"is configured, the operator fences the non-preferred site, increments " +
			"bloodraven_split_brain_auto_resolve_total{prefer_site=...}, and logs 'split-brain auto-resolve'.",
		Risk:    "medium",
		DocLink: "playground/chaos-scenarios.md#5-split-brain-auto-resolve",
		Timeout: 3 * time.Minute,
		Precheck: func(ctx context.Context, env *runner.Env) error {
			if err := AssertHealthyBaseline(ctx, env); err != nil {
				return err
			}
			mfg, err := env.Kube.GetMFG(ctx, env.Namespace)
			if err != nil {
				return err
			}
			if mfg.Spec.SplitBrainPolicy == nil || len(mfg.Spec.SplitBrainPolicy.SitePriorities) == 0 {
				return fmt.Errorf(
					"precondition not met: spec.splitBrainPolicy.sitePriorities is empty " +
						"(this scenario requires an automated split-brain policy; add it to the playground MFG)",
				)
			}
			return nil
		},
		Steps: []runner.Step{
			injectForceBothWritable(),
			observeAutoResolve(),
			verifyAutoResolveMetric(),
			verifyAutoResolveLog(),
		},
		// No scenario-level Cleanup: the executor's GlobalRecover runs
		// unconditionally, and the operator re-asserts super_read_only
		// on the standby during its next reconcile pass — no extra
		// teardown is needed for the writable-flip we injected.
	}
}

func injectForceBothWritable() runner.Step {
	return runner.Step{
		Phase: runner.PhaseInject,
		Name:  "clear super_read_only on the read-only site",
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
			env.Capture.Note(fmt.Sprintf("active=%s peer=%s; forcing peer writable", active, peer))
			if err := ctxStash(ctx, env, "originalPrimary", active); err != nil {
				return err
			}
			if err := ctxStash(ctx, env, "splitBrainSite", peer); err != nil {
				return err
			}
			prefer := mfg.Spec.SplitBrainPolicy.SitePriorities[0]
			if err := stashMetricCounter(ctx, env, "splitBrainAutoResolveBefore", "bloodraven_split_brain_auto_resolve_total", map[string]string{
				"prefer_site": prefer,
			}); err != nil {
				return err
			}
			peerDB, err := env.MySQL(peer)
			if err != nil {
				return err
			}
			// Stop replication on the peer first so it stops following the
			// primary's super_read_only assertion, then clear the flag.
			if _, err := peerDB.Exec(ctx, "STOP REPLICA"); err != nil {
				return fmt.Errorf("stop replica on %s: %w", peer, err)
			}
			if err := peerDB.SetSuperReadOnly(ctx, false); err != nil {
				return fmt.Errorf("clear super_read_only on %s: %w", peer, err)
			}
			superReadOnly, err := peerDB.SuperReadOnly(ctx)
			if err != nil {
				return fmt.Errorf("verify super_read_only on %s: %w", peer, err)
			}
			readOnly, err := peerDB.ReadOnly(ctx)
			if err != nil {
				return fmt.Errorf("verify read_only on %s: %w", peer, err)
			}
			if superReadOnly || readOnly {
				return fmt.Errorf("split-brain injection did not make %s writable: super_read_only=%v read_only=%v", peer, superReadOnly, readOnly)
			}
			env.Capture.Note(fmt.Sprintf("split-brain injection verified: %s super_read_only=false read_only=false", peer))
			return nil
		},
	}
}

func observeAutoResolve() runner.Step {
	return runner.Step{
		Phase: runner.PhaseObserve,
		Name:  "operator fences the non-preferred site",
		Do: func(ctx context.Context, env *runner.Env) error {
			waitCtx, cancel := context.WithTimeout(ctx, 90*time.Second)
			defer cancel()
			_, err := env.Wait.UntilCR(waitCtx, env.Namespace,
				"non-preferred site returns to read-only",
				func(mfg *v1alpha1.MysqlFailoverGroup) (bool, string, error) {
					if mfg.Spec.SplitBrainPolicy == nil || len(mfg.Spec.SplitBrainPolicy.SitePriorities) == 0 {
						return false, "spec.splitBrainPolicy not configured", fmt.Errorf("split-brain policy disappeared")
					}
					prefer := mfg.Spec.SplitBrainPolicy.SitePriorities[0]
					losers := writableSitesExcept(mfg, prefer)
					msg := fmt.Sprintf("prefer=%s active=%s losers=%v", prefer, mfg.Status.ActiveSite, losers)
					return len(losers) == 0 && mfg.Status.ActiveSite == prefer, msg, nil
				},
			)
			return err
		},
	}
}

func verifyAutoResolveMetric() runner.Step {
	return runner.Step{
		Phase: runner.PhaseVerify,
		Name:  "bloodraven_split_brain_auto_resolve_total increments",
		Do: func(ctx context.Context, env *runner.Env) error {
			mfg, err := env.Kube.GetMFG(ctx, env.Namespace)
			if err != nil {
				return err
			}
			prefer := mfg.Spec.SplitBrainPolicy.SitePriorities[0]
			before, err := fetchStashedFloat(env, "splitBrainAutoResolveBefore")
			if err != nil {
				return err
			}
			waitCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
			defer cancel()
			return env.Wait.UntilMetric(waitCtx, env.Metrics,
				fmt.Sprintf("bloodraven_split_brain_auto_resolve_total{prefer_site=%q} increments from %g", prefer, before),
				func(snap *pgmetrics.Snapshot) (bool, string) {
					v, _ := snap.Counter("bloodraven_split_brain_auto_resolve_total", map[string]string{
						"prefer_site": prefer,
					})
					return v > before, fmt.Sprintf("counter=%g before=%g delta=%g", v, before, v-before)
				},
			)
		},
	}
}

func verifyAutoResolveLog() runner.Step {
	return runner.Step{
		Phase: runner.PhaseVerify,
		Name:  `"split-brain auto-resolve" line in operator log`,
		Do: func(ctx context.Context, env *runner.Env) error {
			tail, err := env.Logs("operator")
			if err != nil {
				return err
			}
			waitCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
			defer cancel()
			_, err = env.Wait.UntilLog(waitCtx, tail, env.StartTime,
				`"split-brain auto-resolve" log msg`,
				pglogs.Substring(`split-brain auto-resolve`),
			)
			return err
		},
	}
}

func writableSitesExcept(mfg *v1alpha1.MysqlFailoverGroup, prefer string) []string {
	var out []string
	for _, s := range mfg.Status.Sites {
		if s.Name == prefer {
			continue
		}
		if s.State == "writable" {
			out = append(out, s.Name)
		}
	}
	return out
}
