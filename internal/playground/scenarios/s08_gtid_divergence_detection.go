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
	runner.Register(scenario08GTIDDivergenceDetection())
}

// scenario08GTIDDivergenceDetection forces a failover, then writes a
// rogue transaction on the old primary so its gtid_executed contains
// a UUID:GNO the new primary has never seen. The operator's recovery
// path then computes divergent = old.Subtract(new) → non-empty, sets
// status.sites[old].recoveryState=RecoveryBlocked, populates
// divergentGtid + divergentTransactionCount, and logs "divergence
// detected".
//
// We reuse the inject and observation steps from scenario 19 (which
// manufactures the same divergent state as the precondition for its
// reclone-interlock checks). This scenario stops where 19 begins:
// asserting the bare detection-and-block contract.
//
// Cleanup: the scenario leaves the cluster with a RecoveryBlocked
// site, which would fail the executor's post-cleanup reconverge gate
// and break run-all. We drive the auto-reclone path (set the
// reclone-site annotation with the matching 8-char prefix) as a
// scenario.Cleanup hook so subsequent scenarios start from a clean
// baseline. Single-scenario `run` invocations get the same automatic
// recovery; if the reclone path is itself broken, cleanup surfaces
// that failure separately from the scenario's pass/fail.
func scenario08GTIDDivergenceDetection() runner.Scenario {
	return runner.Scenario{
		ID:    "08-gtid-divergence-detection",
		Title: "GTID divergence on old primary is detected and recovery is blocked",
		Hypothesis: "After a failover, a rogue write on the old primary populates status.sites[].divergentGtid, " +
			"sets recoveryState=RecoveryBlocked with divergentTransactionCount>0, and produces a 'divergence detected' " +
			"warning log line for the offending site.",
		Risk:     "medium",
		DocLink:  "playground/chaos-scenarios.md#8-gtid-divergence-detection",
		Timeout:  6 * time.Minute,
		Precheck: AssertHealthyBaseline,
		Steps: []runner.Step{
			injectDivergenceViaFailover(),
			observeDivergenceRecorded(),
			s08VerifyRecoveryBlocked(),
			s08VerifyDivergenceLog(),
		},
		Cleanup: s08AutoRecloneCleanup,
	}
}

func s08VerifyRecoveryBlocked() runner.Step {
	return runner.Step{
		Phase: runner.PhaseVerify,
		Name:  "divergent site has recoveryState=RecoveryBlocked and divergentTransactionCount>0",
		Do: func(ctx context.Context, env *runner.Env) error {
			site := ctxFetch(env, "divergentSite")
			waitCtx, cancel := context.WithTimeout(ctx, 90*time.Second)
			defer cancel()
			_, err := env.Wait.UntilCR(waitCtx, env.Namespace,
				fmt.Sprintf("site %s recoveryState=RecoveryBlocked && divergentCount>0", site),
				func(mfg *v1alpha1.MysqlFailoverGroup) (bool, string, error) {
					for _, s := range mfg.Status.Sites {
						if s.Name != site {
							continue
						}
						var count int64
						if s.DivergentTransactionCount != nil {
							count = *s.DivergentTransactionCount
						}
						msg := fmt.Sprintf("site=%s recoveryState=%q divergentCount=%d divergentGtid=%q",
							s.Name, s.RecoveryState, count, s.DivergentGtid)
						return s.RecoveryState == "RecoveryBlocked" && count > 0, msg, nil
					}
					return false, fmt.Sprintf("site %s not present in status.sites", site), nil
				},
			)
			return err
		},
	}
}

func s08VerifyDivergenceLog() runner.Step {
	return runner.Step{
		Phase: runner.PhaseVerify,
		Name:  `"divergence detected" log line emitted for the divergent site`,
		Do: func(ctx context.Context, env *runner.Env) error {
			site := ctxFetch(env, "divergentSite")
			tail, err := env.Logs("operator")
			if err != nil {
				return err
			}
			waitCtx, cancel := context.WithTimeout(ctx, 60*time.Second)
			defer cancel()
			// Require both the message and the site label so we cannot
			// match a stale line from an earlier scenario in run-all.
			_, err = env.Wait.UntilLog(waitCtx, tail, env.StartTime,
				fmt.Sprintf(`"divergence detected" site=%s`, site),
				pglogs.And(
					pglogs.Substring("divergence detected"),
					pglogs.Substring(fmt.Sprintf(`"site":"%s"`, site)),
				),
			)
			return err
		},
	}
}

// s08AutoRecloneCleanup drives the operator's reclone path to clear
// the divergent state the scenario intentionally manufactured. Without
// this, the executor's post-cleanup waitForClusterReconverge gate
// rejects RecoveryBlocked sites and the next scenario in a run-all
// session fails its precheck.
func s08AutoRecloneCleanup(ctx context.Context, env *runner.Env) error {
	mfg, err := env.Kube.GetMFGNamed(ctx, env.Namespace, env.FG)
	if err != nil {
		return fmt.Errorf("cleanup: get MFG: %w", err)
	}
	var divergent, gtid string
	for _, s := range mfg.Status.Sites {
		if s.DivergentGtid != "" {
			divergent = s.Name
			gtid = s.DivergentGtid
			break
		}
	}
	if divergent == "" {
		env.Capture.Note("cleanup: no divergent site found; skipping auto-reclone")
		return nil
	}
	if len(gtid) < 8 {
		return fmt.Errorf("cleanup: divergentGtid %q on site %s is shorter than 8 characters; cannot auto-reclone — run ./playground/reset-mysql.sh",
			gtid, divergent)
	}
	value := fmt.Sprintf("%s:%s", divergent, gtid[:8])
	env.Capture.Note(fmt.Sprintf("cleanup: submitting reclone-site=%s to clear divergence", value))
	if err := env.Kube.AnnotateMFGNamed(ctx, env.Namespace, env.FG, "bloodraven.shipstream.io/reclone-site", value); err != nil {
		return fmt.Errorf("cleanup: set reclone annotation: %w", err)
	}
	waitCtx, cancel := context.WithTimeout(ctx, 4*time.Minute)
	defer cancel()
	_, err = env.Wait.UntilCR(waitCtx, env.Namespace,
		"auto-reclone clears divergence",
		func(mfg *v1alpha1.MysqlFailoverGroup) (bool, string, error) {
			var writable, readOnly, blocked, div []string
			for _, s := range mfg.Status.Sites {
				switch s.State {
				case "writable":
					writable = append(writable, s.Name)
				case "read-only":
					readOnly = append(readOnly, s.Name)
				}
				if s.RecoveryState == "RecoveryBlocked" {
					blocked = append(blocked, s.Name)
				}
				if s.DivergentGtid != "" {
					div = append(div, s.Name)
				}
			}
			done := len(writable) == 1 && len(readOnly) == len(mfg.Status.Sites)-1 && len(blocked) == 0 && len(div) == 0
			return done, fmt.Sprintf("writable=%v read-only=%v blocked=%v divergent=%v",
				writable, readOnly, blocked, div), nil
		},
	)
	if err != nil {
		return fmt.Errorf("cleanup: auto-reclone did not clear divergence in 4m: %w", err)
	}
	env.Capture.Note(fmt.Sprintf("cleanup: divergence cleared on %s", divergent))
	return nil
}
