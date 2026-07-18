package scenarios

import (
	"context"
	"fmt"
	"time"

	v1alpha1 "github.com/shipstream/bloodraven/api/v1alpha1"
	pgkube "github.com/shipstream/bloodraven/internal/playground/kube"
	pglogs "github.com/shipstream/bloodraven/internal/playground/logs"
	pgmetrics "github.com/shipstream/bloodraven/internal/playground/metrics"
	"github.com/shipstream/bloodraven/internal/playground/runner"
)

const s44MarkerTable = "bloodraven_reader_e2e.convergence_marker"

func init() {
	runner.Register(scenario44ReaderSourceConvergenceInvariant())
}

type s44RunState struct {
	topo               readerTopology
	standbyHost        string
	lastFailoverBefore string
	injected           bool
	recovered          bool
}

// scenario44ReaderSourceConvergenceInvariant is chaos proposal R9 from
// issue #115: direct-source convergence is a periodic invariant of the
// poll loop, not a one-shot switchover event. ANY wrong-source state —
// operator drift, a partially-applied runbook, or a pre-fix chained
// reader left over from an upgrade — heals without a failover.
func scenario44ReaderSourceConvergenceInvariant() runner.Scenario {
	state := &s44RunState{}
	return runner.Scenario{
		ID:    "44-reader-source-convergence-invariant",
		Title: "Manually repointed reader converges back to the primary as a poll-loop invariant",
		Hypothesis: "Pointing the reader's replication at the promotable standby (a chained topology) is repaired " +
			"within a bounded number of poll cycles: the operator logs the documented convergence started/complete " +
			"events, repoints the reader directly at the active primary after the GTID containment check, and the " +
			"group sees no failover.",
		Risk:     "low",
		DocLink:  "playground/chaos-scenarios.md#44-reader-source-convergence-invariant",
		Timeout:  6 * time.Minute,
		Precheck: s44Precheck(state),
		Steps: []runner.Step{
			s44InjectWrongSource(state),
			s44ObserveConvergenceLogs(state),
			s44VerifyDirectSourceRestored(state),
		},
		Cleanup: s44Cleanup(state),
	}
}

func s44Precheck(state *s44RunState) func(context.Context, *runner.Env) error {
	return func(ctx context.Context, env *runner.Env) error {
		*state = s44RunState{}
		topo, err := resolveReaderTopology(ctx, env)
		if err != nil {
			return err
		}
		state.topo = topo
		state.standbyHost = playgroundInternalSiteHost(env.FG, topo.standby, env.Namespace)
		return nil
	}
}

func s44InjectWrongSource(state *s44RunState) runner.Step {
	return runner.Step{
		Phase: runner.PhaseInject,
		Name:  "repoint the reader's replication at the promotable standby (chained topology)",
		Do: func(ctx context.Context, env *runner.Env) error {
			if _, err := env.Logs("operator"); err != nil {
				return fmt.Errorf("open operator tailer: %w", err)
			}
			mfg, err := env.Kube.GetMFGNamed(ctx, env.Namespace, env.FG)
			if err != nil {
				return err
			}
			if mfg.Status.LastFailover != nil {
				state.lastFailoverBefore = mfg.Status.LastFailover.Time.UTC().Format(time.RFC3339)
			}
			reader, err := env.MySQL(state.topo.reader)
			if err != nil {
				return fmt.Errorf("open reader mysql: %w", err)
			}
			// SOURCE_HOST is the only changed option; user, password, and
			// GTID auto-positioning carry over from the existing channel.
			// One batch keeps the stopped window smaller than a poll cycle.
			stmt := fmt.Sprintf("STOP REPLICA; CHANGE REPLICATION SOURCE TO SOURCE_HOST='%s'; START REPLICA", state.standbyHost)
			if _, err := reader.Exec(ctx, stmt); err != nil {
				return fmt.Errorf("repoint reader at standby %s: %w", state.topo.standby, err)
			}
			state.injected = true
			env.Capture.Note(fmt.Sprintf("reader %s now chained through standby %s; expecting invariant repair to %s",
				state.topo.reader, state.standbyHost, state.topo.activeHost))
			return nil
		},
	}
}

func s44ObserveConvergenceLogs(state *s44RunState) runner.Step {
	return runner.Step{
		Phase: runner.PhaseObserve,
		Name:  "operator logs convergence started (wrong current source) then complete",
		Do: func(ctx context.Context, env *runner.Env) error {
			tail, err := env.Logs("operator")
			if err != nil {
				return err
			}
			startCtx, cancelStart := context.WithTimeout(ctx, 2*time.Minute)
			_, err = env.Wait.UntilLog(startCtx, tail, env.StartTime,
				"convergence started with currentSource=standby expectedSource=active",
				pglogs.Structured("replication source convergence started", map[string]string{
					"site":           state.topo.reader,
					"currentSource":  state.standbyHost,
					"expectedSource": state.topo.activeHost,
				}))
			cancelStart()
			if err != nil {
				return err
			}
			completeCtx, cancelComplete := context.WithTimeout(ctx, 2*time.Minute)
			_, err = env.Wait.UntilLog(completeCtx, tail, env.StartTime,
				"convergence complete back onto the active primary",
				pglogs.Structured("replication source convergence complete", map[string]string{
					"site":   state.topo.reader,
					"source": state.topo.activeHost,
				}))
			cancelComplete()
			return err
		},
	}
}

func s44VerifyDirectSourceRestored(state *s44RunState) runner.Step {
	return runner.Step{
		Phase: runner.PhaseVerify,
		Name:  "reader replicates directly from the primary again; no failover consumed",
		Do: func(ctx context.Context, env *runner.Env) error {
			waitCtx, cancel := context.WithTimeout(ctx, 2*time.Minute)
			mfg, err := env.Wait.UntilCR(waitCtx, env.Namespace,
				"reader serving status converged on the active primary",
				func(mfg *v1alpha1.MysqlFailoverGroup) (bool, string, error) {
					status := statusSiteByName(mfg, state.topo.reader)
					if status == nil {
						return false, "reader status missing", nil
					}
					err := assertReaderServingStatus(mfg, status, state.topo.activeHost)
					return err == nil, fmt.Sprintf("convergence=%s/%s source=%q lag=%v",
						status.SourceConvergenceState, status.SourceConvergenceReason, status.SourceHost, status.SecondsBehindSource), nil
				})
			cancel()
			if err != nil {
				return err
			}
			state.recovered = true

			if ready := pgkube.ReadyCondition(mfg); ready != "True" {
				return fmt.Errorf("group Ready=%q after convergence repair, want True", ready)
			}
			if mfg.Status.ActiveSite != state.topo.active {
				return fmt.Errorf("active site changed during convergence repair: %q -> %q", state.topo.active, mfg.Status.ActiveSite)
			}
			lastFailover := ""
			if mfg.Status.LastFailover != nil {
				lastFailover = mfg.Status.LastFailover.Time.UTC().Format(time.RFC3339)
			}
			if lastFailover != state.lastFailoverBefore {
				return fmt.Errorf("convergence repair consumed a failover: lastFailover %q -> %q", state.lastFailoverBefore, lastFailover)
			}

			reader, err := env.MySQL(state.topo.reader)
			if err != nil {
				return fmt.Errorf("open reader mysql: %w", err)
			}
			rs, err := reader.ShowReplicaStatus(ctx)
			if err != nil {
				return err
			}
			if !rs.Configured || !rs.IORunning || !rs.SQLRunning || canonicalMySQLHost(rs.SourceHost) != canonicalMySQLHost(state.topo.activeHost) {
				return fmt.Errorf("live replica status not direct: configured=%v io=%v sql=%v source=%q, want source %q",
					rs.Configured, rs.IORunning, rs.SQLRunning, rs.SourceHost, state.topo.activeHost)
			}

			metricCtx, cancelMetric := context.WithTimeout(ctx, 30*time.Second)
			defer cancelMetric()
			if err := env.Wait.UntilMetric(metricCtx, env.Metrics,
				fmt.Sprintf(`replication_source_state{site=%q,state="converged"} == 1`, state.topo.reader),
				func(snap *pgmetrics.Snapshot) (bool, string) {
					v, ok := snap.Gauge("bloodraven_replication_source_state", map[string]string{"site": state.topo.reader, "state": "converged"})
					return ok && v == 1, fmt.Sprintf("converged=%g(ok=%v)", v, ok)
				},
			); err != nil {
				return err
			}

			// Write-through proof over the repaired direct channel.
			marker := fmt.Sprintf("convergence-%d", time.Now().UnixNano())
			if err := seedMarkerRow(ctx, env, state.topo.active, s44MarkerTable, marker); err != nil {
				return err
			}
			if err := waitForMarkerOnSite(ctx, env, state.topo.reader, s44MarkerTable, marker, 60*time.Second); err != nil {
				return err
			}
			return waitReaderClientEndpoint(ctx, env, state.topo.reader, 90*time.Second)
		},
	}
}

// s44Cleanup restarts the reader's replication if the scenario failed
// mid-way with the channel stopped. The wrong-source state itself needs
// no cleanup — repairing it is exactly what the operator's convergence
// invariant does, and the executor's reconverge wait holds the runner
// until the group is healthy again.
func s44Cleanup(state *s44RunState) func(context.Context, *runner.Env) error {
	return func(ctx context.Context, env *runner.Env) error {
		if !state.injected || state.recovered {
			return nil
		}
		reader, err := env.MySQL(state.topo.reader)
		if err != nil {
			return fmt.Errorf("cleanup: open reader mysql: %w", err)
		}
		rs, err := reader.ShowReplicaStatus(ctx)
		if err != nil {
			return fmt.Errorf("cleanup: replica status: %w", err)
		}
		if rs.Configured && (!rs.IORunning || !rs.SQLRunning) {
			if _, err := reader.Exec(ctx, "START REPLICA"); err != nil {
				return fmt.Errorf("cleanup: start replica: %w", err)
			}
		}
		return nil
	}
}
