package scenarios

import (
	"context"
	"errors"
	"fmt"
	"time"

	v1alpha1 "github.com/shipstream/bloodraven/api/v1alpha1"
	pgmetrics "github.com/shipstream/bloodraven/internal/playground/metrics"
	pgmysql "github.com/shipstream/bloodraven/internal/playground/mysql"
	"github.com/shipstream/bloodraven/internal/playground/runner"
)

func init() {
	runner.Register(scenario35PlannedSwitchoverLagTimeout())
}

const (
	s35ID          = "35-planned-switchover-lag-timeout-rollback"
	s35DBName      = "chaos_s35"
	s35SourceDelay = 60
)

// scenario35PlannedSwitchoverLagTimeout drives the planned-failover state
// machine into its WaitingForLag → Failed{LagTimeout} rollback path. It sets
// SOURCE_DELAY=60 on the target replica (keeping both threads running so the
// operator's replica-health check stays true), writes a marker on the source
// so the fenced GTID cannot be covered in time, then annotates a switchover
// with maxLagWait=5s. The source must be unfenced and remain active; the
// switchover must not advance the failover history to the target.
func scenario35PlannedSwitchoverLagTimeout() runner.Scenario {
	return runner.Scenario{
		ID:    s35ID,
		Title: "Planned switchover rolls back on lag timeout, source stays active",
		Hypothesis: "With SOURCE_DELAY=60 on the target and maxLagWait=5s, a planned switchover fences the source, " +
			"waits for the target GTID to cover the fenced GTID, times out, and rolls back: plannedFailover.phase=Failed " +
			"reason=LagTimeout, planned_failovers_total{result=failed_timeout} increments, status.activeSite stays the " +
			"source, and the failover history does not advance to the target.",
		Risk:              "medium",
		DocLink:           "playground/chaos-scenarios.md#35-planned-switchover-lag-timeout-rollback",
		Timeout:           8 * time.Minute,
		ResetBeforeRunAll: false,
		Precheck:          assertReplicationRunningPrecheck,
		Steps: []runner.Step{
			s35InjectDelayMarkerAndAnnotate(),
			s35ObserveLagTimeoutRollback(),
			s35VerifySourceStaysActive(),
		},
		Cleanup: s35Cleanup,
	}
}

func s35InjectDelayMarkerAndAnnotate() runner.Step {
	return runner.Step{
		Phase: runner.PhaseInject,
		Name:  "SOURCE_DELAY=60 on target, write marker on source, annotate switchover maxLagWait=5s",
		Do: func(ctx context.Context, env *runner.Env) error {
			mfg, err := env.Kube.GetMFGNamed(ctx, env.Namespace, env.FG)
			if err != nil {
				return err
			}
			source := mfg.Status.ActiveSite
			target, err := PeerOf(mfg, source)
			if err != nil {
				return err
			}
			if err := ctxStash(ctx, env, "source", source); err != nil {
				return err
			}
			if err := ctxStash(ctx, env, "target", target); err != nil {
				return err
			}
			if err := ctxStash(ctx, env, "lastFailoverTargetBefore", mfg.Status.LastFailoverTarget); err != nil {
				return err
			}
			if err := stashMetricCounter(ctx, env, "plannedFailedTimeoutBefore", "bloodraven_planned_failovers_total",
				map[string]string{"target_site": target, "result": "failed_timeout"}); err != nil {
				return err
			}

			// 1) Delay the target FIRST so the marker written next cannot be
			//    applied on the target within the lag budget.
			targetClient, err := env.MySQL(target)
			if err != nil {
				return fmt.Errorf("open target %s: %w", target, err)
			}
			if err := setSourceDelay(ctx, targetClient, s35SourceDelay); err != nil {
				return err
			}
			env.Capture.Note(fmt.Sprintf("SOURCE_DELAY=%ds applied on target %s", s35SourceDelay, target))

			// 2) Write a marker on the source AFTER the delay is active.
			sourceClient, err := env.MySQL(source)
			if err != nil {
				return fmt.Errorf("open source %s: %w", source, err)
			}
			stmts := []string{
				"CREATE DATABASE IF NOT EXISTS " + s35DBName,
				"CREATE TABLE IF NOT EXISTS " + s35DBName + ".marker (id INT PRIMARY KEY, note VARCHAR(64))",
				fmt.Sprintf("INSERT INTO %s.marker (id, note) VALUES (1, 'lag-timeout-marker') ON DUPLICATE KEY UPDATE note=VALUES(note)", s35DBName),
			}
			for _, q := range stmts {
				if _, err := sourceClient.Exec(ctx, q); err != nil {
					return fmt.Errorf("marker stmt %q: %w", q, err)
				}
			}
			markerGtid, err := sourceClient.GtidExecuted(ctx)
			if err != nil {
				return fmt.Errorf("read marker gtid: %w", err)
			}
			if err := ctxStash(ctx, env, "markerGtid", markerGtid); err != nil {
				return err
			}
			env.Capture.Note(fmt.Sprintf("wrote marker on source %s; gtid=%s", source, truncGtid(markerGtid)))

			// 3) Annotate the switchover with a short lag budget.
			raw := fmt.Sprintf("%s:maxLagWait=5s", target)
			env.Capture.Note("annotating planned-failover=" + raw)
			return env.Chaos.AnnotatePlannedFailoverRaw(ctx, raw)
		},
	}
}

func s35ObserveLagTimeoutRollback() runner.Step {
	return runner.Step{
		Phase: runner.PhaseObserve,
		Name:  "plannedFailover reaches Failed with reason LagTimeout",
		Do: func(ctx context.Context, env *runner.Env) error {
			target := ctxFetch(env, "target")
			waitCtx, cancel := context.WithTimeout(ctx, 3*time.Minute)
			defer cancel()
			var sawWaitingForLag bool
			_, err := env.Wait.UntilCR(waitCtx, env.Namespace, "plannedFailover.phase==Failed reason==LagTimeout",
				func(mfg *v1alpha1.MysqlFailoverGroup) (bool, string, error) {
					pf := mfg.Status.PlannedFailover
					if pf == nil {
						return false, "no plannedFailover status yet", nil
					}
					// Ignore stale blocks from prior runs (metav1.Time truncates
					// to seconds, so allow 2s of slack like scenario 02).
					staleCutoff := env.StartTime.Add(-2 * time.Second)
					if pf.StartTime == nil || pf.StartTime.Time.Before(staleCutoff) {
						return false, fmt.Sprintf("ignoring stale plannedFailover (startTime=%v)", pf.StartTime), nil
					}
					if pf.Phase == v1alpha1.PlannedFailoverPhaseWaitingForLag {
						sawWaitingForLag = true
					}
					msg := fmt.Sprintf("phase=%q target=%q reason=%q waitingForLagSeen=%v", pf.Phase, pf.Target, pf.Reason, sawWaitingForLag)
					if pf.Phase == v1alpha1.PlannedFailoverPhaseSucceeded {
						return false, msg, fmt.Errorf("planned switchover unexpectedly Succeeded (lag budget should have expired): %s", msg)
					}
					if pf.Phase == v1alpha1.PlannedFailoverPhaseFailed {
						if pf.Reason != "LagTimeout" {
							return false, msg, fmt.Errorf("planned switchover Failed with reason %q, want LagTimeout: %s", pf.Reason, pf.Message)
						}
						if pf.Target != target {
							return false, msg, fmt.Errorf("plannedFailover target=%q, want %q", pf.Target, target)
						}
						return true, msg, nil
					}
					return false, msg, nil
				})
			if err != nil {
				return err
			}
			env.Capture.Note(fmt.Sprintf("planned switchover rolled back on lag timeout (observed WaitingForLag=%v; Validating not asserted as it may not persist)", sawWaitingForLag))
			return nil
		},
	}
}

func s35VerifySourceStaysActive() runner.Step {
	return runner.Step{
		Phase: runner.PhaseVerify,
		Name:  "source stays active and writable; history not advanced; metric increments",
		Do: func(ctx context.Context, env *runner.Env) error {
			source := ctxFetch(env, "source")
			target := ctxFetch(env, "target")
			before, err := fetchStashedFloat(env, "plannedFailedTimeoutBefore")
			if err != nil {
				return err
			}

			// Status.activeSite stays the source, but restoring it is eventual
			// (unfence clears super_read_only before read_only, and the
			// topology manager only reports a site as active once it polls it
			// writable) — status.activeSite can transiently read "" right after
			// the terminal Failed/LagTimeout phase. Poll deterministically for
			// the source to be restored as sole active site, before checking
			// metrics; the same poll fails fast if the rollback actually
			// advanced activeSite or the failover history to the target, so
			// the "history not advanced" contract is checked continuously
			// rather than weakened.
			activeCtx, cancel := context.WithTimeout(ctx, 90*time.Second)
			defer cancel()
			if _, err := env.Wait.UntilCR(activeCtx, env.Namespace,
				fmt.Sprintf("status.activeSite==%s restored as sole writer/active after rollback", source),
				func(mfg *v1alpha1.MysqlFailoverGroup) (bool, string, error) {
					return s35RollbackConverged(mfg.Status.ActiveSite, mfg.Status.LastFailoverTarget, source, target)
				}); err != nil {
				return fmt.Errorf("source %s not restored as active after rollback: %w", source, err)
			}

			// failed_timeout metric increments.
			metricCtx, cancel := context.WithTimeout(ctx, 45*time.Second)
			defer cancel()
			if err := env.Wait.UntilMetric(metricCtx, env.Metrics,
				fmt.Sprintf(`planned_failovers_total{target_site=%q,result="failed_timeout"} increments from %g`, target, before),
				func(snap *pgmetrics.Snapshot) (bool, string) {
					v, _ := snap.Counter("bloodraven_planned_failovers_total", map[string]string{"target_site": target, "result": "failed_timeout"})
					return v > before, fmt.Sprintf("counter=%g before=%g", v, before)
				}); err != nil {
				return err
			}

			// Source is unfenced and becomes writable again (eventual — unfence
			// clears super_read_only first, then read_only).
			if err := waitSiteWritable(ctx, env, source, 90*time.Second); err != nil {
				return fmt.Errorf("source %s not writable again after rollback: %w", source, err)
			}

			// Target stays a read-only, replicating candidate.
			targetClient, err := env.MySQL(target)
			if err != nil {
				return fmt.Errorf("open target %s: %w", target, err)
			}
			ro, err := targetClient.ReadOnly(ctx)
			if err != nil {
				return err
			}
			if !ro {
				return fmt.Errorf("target %s is not read-only after rollback", target)
			}
			rs, err := targetClient.ShowReplicaStatus(ctx)
			if err != nil {
				return fmt.Errorf("target %s replica status: %w", target, err)
			}
			if !rs.Configured || !rs.IORunning || !rs.SQLRunning {
				return fmt.Errorf("target %s replication not running after rollback (configured=%v io=%v sql=%v)", target, rs.Configured, rs.IORunning, rs.SQLRunning)
			}
			env.Capture.Note(fmt.Sprintf("source %s stayed active & writable; target %s read-only & replicating; failed_timeout metric incremented", source, target))
			return nil
		},
	}
}

// s35Cleanup clears SOURCE_DELAY on the site the scenario delayed (plus any
// other read-only site, as a backstop), waits for the marker to replicate, and
// drops the scenario schema.
func s35Cleanup(ctx context.Context, env *runner.Env) error {
	var errs []error
	mfg, err := env.Kube.GetMFGNamed(ctx, env.Namespace, env.FG)
	if err != nil {
		return fmt.Errorf("cleanup: get MFG: %w", err)
	}
	markerGtid := ctxFetch(env, "markerGtid")

	// Always clear the stashed target: SOURCE_DELAY was applied to THAT site,
	// and it has to come off whatever state the site ended up in. Keying the
	// clear off the observed state alone leaves a 60s-delayed replica behind
	// whenever the topology moved (target promoted, site unreachable at cleanup
	// time), which then poisons every scenario that follows. The read-only sweep
	// stays as a backstop for a delay applied to some other site.
	delayed := []string{}
	if target := ctxFetch(env, "target"); target != "" {
		delayed = append(delayed, target)
	}
	readOnly := map[string]bool{}
	for _, s := range mfg.Status.Sites {
		if s.State == "read-only" {
			readOnly[s.Name] = true
			if !stringsContain(delayed, s.Name) {
				delayed = append(delayed, s.Name)
			}
		}
	}

	for _, name := range delayed {
		c, err := env.MySQL(name)
		if err != nil {
			env.Capture.Note(fmt.Sprintf("cleanup: skip SOURCE_DELAY clear on %s: %v", name, err))
			errs = append(errs, fmt.Errorf("open %s to clear SOURCE_DELAY: %w", name, err))
			continue
		}
		if err := clearSourceDelay(ctx, c); err != nil {
			env.Capture.Note(fmt.Sprintf("cleanup: clear SOURCE_DELAY on %s: %v", name, err))
			errs = append(errs, fmt.Errorf("clear SOURCE_DELAY on %s: %w", name, err))
		}
		// Only a replica can wait for the marker to arrive over replication.
		if markerGtid != "" && readOnly[name] {
			if rc, err := c.ScalarInt(ctx, "SELECT WAIT_FOR_EXECUTED_GTID_SET(?, 60)", markerGtid); err != nil {
				env.Capture.Note(fmt.Sprintf("cleanup: wait marker gtid on %s: %v", name, err))
			} else if rc != 0 {
				env.Capture.Note(fmt.Sprintf("cleanup: replica %s did not catch up to marker within 60s (rc=%d)", name, rc))
			}
		}
	}
	// Drop the marker schema from the writable site.
	for _, s := range mfg.Status.Sites {
		if s.State != "writable" {
			continue
		}
		c, err := pgmysql.Open(ctx, env.Kube, env.Namespace, env.FG, s.Name, env.Creds)
		if err != nil {
			env.Capture.Note(fmt.Sprintf("cleanup: open writable %s: %v", s.Name, err))
			continue
		}
		if _, err := c.Exec(ctx, "DROP DATABASE IF EXISTS "+s35DBName); err != nil {
			env.Capture.Note(fmt.Sprintf("cleanup: drop %s on %s: %v", s35DBName, s.Name, err))
		}
		_ = c.Close()
	}
	return errors.Join(errs...)
}
