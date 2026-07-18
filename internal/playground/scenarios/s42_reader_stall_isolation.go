package scenarios

import (
	"context"
	"fmt"
	"time"

	v1alpha1 "github.com/shipstream/bloodraven/api/v1alpha1"
	pgkube "github.com/shipstream/bloodraven/internal/playground/kube"
	pgmetrics "github.com/shipstream/bloodraven/internal/playground/metrics"
	"github.com/shipstream/bloodraven/internal/playground/runner"
)

const (
	s42DBName    = "chaos_s42"
	s42TableName = s42DBName + ".stall_rows"
	// s42SourceDelaySeconds must exceed the soak duration so applied lag
	// keeps growing for the whole observation window. SOURCE_DELAY (not
	// STOP REPLICA SQL_THREAD) is the one lag injection the operator
	// will not heal: both replication threads keep running on the
	// correct source, so the source-convergence invariant sees a
	// converged-but-lagging reader instead of a stopped one.
	s42SourceDelaySeconds = 600
)

func init() {
	runner.Register(scenario42ReaderStallIsolation())
}

type s42RunState struct {
	topo readerTopology
	// lastFailoverBefore is the pre-injection status.lastFailover
	// timestamp ("" when unset). Any change during the soak means the
	// stalled reader consumed a failover / anti-flap cooldown.
	lastFailoverBefore string
	sawEndpointEmpty   bool
	maxObservedLag     int64
}

// scenario42ReaderStallIsolation is chaos proposal R4 from issue #115:
// a wedged OLAP reader is invisible to the failover machinery. Lag
// grows without bound with zero group-level effect; the only reaction
// is endpoint shedding plus alertable per-site status and metrics.
func scenario42ReaderStallIsolation() runner.Scenario {
	state := &s42RunState{}
	return runner.Scenario{
		ID:    "42-reader-stall-no-group-degradation",
		Title: "Stalled reader sheds its endpoint without degrading the group",
		Hypothesis: "SOURCE_DELAY on the reader grows replication lag past readOnlyMaxLagSeconds while both threads " +
			"keep running. Group Ready stays True for a soak of 3x maxLagSeconds with no failover and no cooldown " +
			"consumed; the reader leaves its client Service endpoints and status/metrics report the stall.",
		Risk:     "low",
		DocLink:  "playground/chaos-scenarios.md#42-reader-stall-does-not-degrade-the-group",
		Timeout:  10 * time.Minute,
		Precheck: s42Precheck(state),
		Steps: []runner.Step{
			s42InjectSourceDelay(state),
			s42SoakGroupUnaffected(state),
			s42VerifyStallObservability(state),
			s42ClearDelayAndRecover(state),
		},
		Cleanup: s42Cleanup(state),
	}
}

func s42Precheck(state *s42RunState) func(context.Context, *runner.Env) error {
	return func(ctx context.Context, env *runner.Env) error {
		*state = s42RunState{}
		topo, err := resolveReaderTopology(ctx, env)
		if err != nil {
			return err
		}
		state.topo = topo
		return nil
	}
}

func s42SetSourceDelay(ctx context.Context, env *runner.Env, reader string, seconds int) error {
	client, err := env.MySQL(reader)
	if err != nil {
		return fmt.Errorf("open reader mysql: %w", err)
	}
	// One multi-statement batch keeps the stopped-replica window tiny so
	// the operator's convergence pass has no time to observe (and
	// restart) a stopped thread mid-injection.
	stmt := fmt.Sprintf("STOP REPLICA; CHANGE REPLICATION SOURCE TO SOURCE_DELAY = %d; START REPLICA", seconds)
	if _, err := client.Exec(ctx, stmt); err != nil {
		return fmt.Errorf("set SOURCE_DELAY=%d on %s: %w", seconds, reader, err)
	}
	return nil
}

func s42InjectSourceDelay(state *s42RunState) runner.Step {
	return runner.Step{
		Phase: runner.PhaseInject,
		Name:  "apply SOURCE_DELAY on the reader only",
		Do: func(ctx context.Context, env *runner.Env) error {
			mfg, err := env.Kube.GetMFGNamed(ctx, env.Namespace, env.FG)
			if err != nil {
				return err
			}
			if mfg.Status.LastFailover != nil {
				state.lastFailoverBefore = mfg.Status.LastFailover.Time.UTC().Format(time.RFC3339)
			}
			primary, err := env.MySQL(state.topo.active)
			if err != nil {
				return fmt.Errorf("open primary mysql: %w", err)
			}
			if _, err := primary.Exec(ctx, "CREATE DATABASE IF NOT EXISTS "+s42DBName+
				"; CREATE TABLE IF NOT EXISTS "+s42TableName+" (id BIGINT PRIMARY KEY, written_at TIMESTAMP(3) DEFAULT CURRENT_TIMESTAMP(3))"); err != nil {
				return fmt.Errorf("create stall table: %w", err)
			}
			if err := s42SetSourceDelay(ctx, env, state.topo.reader, s42SourceDelaySeconds); err != nil {
				return err
			}
			env.Capture.Note(fmt.Sprintf("SOURCE_DELAY=%ds applied on reader %s; lastFailoverBefore=%q",
				s42SourceDelaySeconds, state.topo.reader, state.lastFailoverBefore))
			return nil
		},
	}
}

// s42SoakGroupUnaffected writes a row per second to the primary while
// continuously asserting the group is untouched by the reader stall:
// Ready stays True, the active site never changes, no failover happens
// and no anti-flap cooldown is consumed, and the reader never enters a
// blocked convergence or recovery state. The reader must leave its
// client Service endpoints once its lag exceeds readOnlyMaxLagSeconds.
func s42SoakGroupUnaffected(state *s42RunState) runner.Step {
	return runner.Step{
		Phase: runner.PhaseObserve,
		Name:  "soak 3x maxLagSeconds: group Ready, no failover, reader sheds endpoint as lag grows",
		Do: func(ctx context.Context, env *runner.Env) error {
			mfg, err := env.Kube.GetMFGNamed(ctx, env.Namespace, env.FG)
			if err != nil {
				return err
			}
			soak := time.Duration(3*mfg.Spec.EffectiveMaxLagSeconds()) * time.Second
			readerMaxLag := mfg.Spec.EffectiveReadOnlyMaxLagSeconds()
			env.Capture.Note(fmt.Sprintf("soaking %s (3x maxLagSeconds), reader lag threshold %ds", soak, readerMaxLag))

			primary, err := env.MySQL(state.topo.active)
			if err != nil {
				return fmt.Errorf("open primary mysql: %w", err)
			}
			deadline := time.Now().Add(soak)
			for i := 0; time.Now().Before(deadline); i++ {
				if _, err := primary.Exec(ctx, "INSERT INTO "+s42TableName+" (id) VALUES (?)", time.Now().UnixNano()); err != nil {
					return fmt.Errorf("soak write %d on primary: %w", i, err)
				}

				mfg, err := env.Kube.GetMFGNamed(ctx, env.Namespace, env.FG)
				if err != nil {
					return fmt.Errorf("soak read MFG: %w", err)
				}
				if ready := pgkube.ReadyCondition(mfg); ready != "True" {
					return fmt.Errorf("group Ready flipped to %q while only the reader was stalled", ready)
				}
				if mfg.Status.ActiveSite != state.topo.active {
					return fmt.Errorf("active site changed during reader stall: %q -> %q", state.topo.active, mfg.Status.ActiveSite)
				}
				lastFailover := ""
				if mfg.Status.LastFailover != nil {
					lastFailover = mfg.Status.LastFailover.Time.UTC().Format(time.RFC3339)
				}
				if lastFailover != state.lastFailoverBefore {
					return fmt.Errorf("failover recorded during reader stall: lastFailover %q -> %q", state.lastFailoverBefore, lastFailover)
				}
				status := statusSiteByName(mfg, state.topo.reader)
				if status == nil {
					return fmt.Errorf("reader %q missing from status during soak", state.topo.reader)
				}
				if status.State != "read-only" {
					return fmt.Errorf("reader state=%q during stall, want read-only", status.State)
				}
				if status.SourceConvergenceState == v1alpha1.SourceConvergenceBlocked {
					return fmt.Errorf("reader entered SourceConvergenceState=Blocked (reason=%s) during a pure lag stall", status.SourceConvergenceReason)
				}
				if status.RecoveryState == "RecoveryBlocked" {
					return fmt.Errorf("reader entered RecoveryBlocked during a pure lag stall")
				}
				if status.SecondsBehindSource != nil && *status.SecondsBehindSource > state.maxObservedLag {
					state.maxObservedLag = *status.SecondsBehindSource
				}
				standby := statusSiteByName(mfg, state.topo.standby)
				if standby == nil || !standby.Replicating {
					return fmt.Errorf("promotable standby %s stopped replicating during reader stall", state.topo.standby)
				}

				endpoints, err := env.Kube.ServiceEndpointState(ctx, env.Namespace, pgkube.MysqlDeploymentName(env.FG, state.topo.reader))
				if err != nil {
					return fmt.Errorf("soak read reader client EndpointSlices: %w", err)
				}
				if len(endpoints.ReadyPodNames("mysql")) == 0 {
					state.sawEndpointEmpty = true
				}

				select {
				case <-ctx.Done():
					return ctx.Err()
				case <-time.After(time.Second):
				}
			}

			if state.maxObservedLag <= readerMaxLag {
				return fmt.Errorf("reader lag never exceeded readOnlyMaxLagSeconds=%d during soak (max observed %d)", readerMaxLag, state.maxObservedLag)
			}
			if !state.sawEndpointEmpty {
				return fmt.Errorf("reader client Service never shed its endpoint although lag reached %ds (threshold %ds)", state.maxObservedLag, readerMaxLag)
			}
			env.Capture.Note(fmt.Sprintf("soak complete: max observed reader lag %ds, endpoint shed observed, group untouched", state.maxObservedLag))
			return nil
		},
	}
}

// s42VerifyStallObservability asserts the stall is alertable: the lag
// gauge is over threshold and the source-state metric still reports the
// stalled reader as converged (it is on the right source — just slow).
func s42VerifyStallObservability(state *s42RunState) runner.Step {
	return runner.Step{
		Phase: runner.PhaseVerify,
		Name:  "lag gauge exceeds reader threshold while source-state stays converged",
		Do: func(ctx context.Context, env *runner.Env) error {
			mfg, err := env.Kube.GetMFGNamed(ctx, env.Namespace, env.FG)
			if err != nil {
				return err
			}
			readerMaxLag := float64(mfg.Spec.EffectiveReadOnlyMaxLagSeconds())
			waitCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
			defer cancel()
			return env.Wait.UntilMetric(waitCtx, env.Metrics,
				fmt.Sprintf("replication_lag_seconds{site=%q} > %g and replication_source_state{site=%q,state=\"converged\"} == 1",
					state.topo.reader, readerMaxLag, state.topo.reader),
				func(snap *pgmetrics.Snapshot) (bool, string) {
					lag, lagOK := snap.Gauge("bloodraven_replication_lag_seconds", map[string]string{"site": state.topo.reader})
					converged, stateOK := snap.Gauge("bloodraven_replication_source_state", map[string]string{"site": state.topo.reader, "state": "converged"})
					msg := fmt.Sprintf("lag=%g(ok=%v) converged=%g(ok=%v)", lag, lagOK, converged, stateOK)
					return lagOK && lag > readerMaxLag && stateOK && converged == 1, msg
				},
			)
		},
	}
}

func s42ClearDelayAndRecover(state *s42RunState) runner.Step {
	return runner.Step{
		Phase: runner.PhaseSettle,
		Name:  "clear SOURCE_DELAY; reader catches up and rejoins its client Service",
		Do: func(ctx context.Context, env *runner.Env) error {
			if err := s42SetSourceDelay(ctx, env, state.topo.reader, 0); err != nil {
				return err
			}
			waitCtx, cancel := context.WithTimeout(ctx, 3*time.Minute)
			defer cancel()
			_, err := env.Wait.UntilCR(waitCtx, env.Namespace,
				"reader lag back under threshold with serving status",
				func(mfg *v1alpha1.MysqlFailoverGroup) (bool, string, error) {
					status := statusSiteByName(mfg, state.topo.reader)
					if status == nil {
						return false, "reader status missing", nil
					}
					err := assertReaderServingStatus(mfg, status, state.topo.activeHost)
					return err == nil, fmt.Sprintf("state=%s replicating=%v convergence=%s lag=%v",
						status.State, status.Replicating, status.SourceConvergenceState, status.SecondsBehindSource), nil
				})
			if err != nil {
				return err
			}
			return waitReaderClientEndpoint(ctx, env, state.topo.reader, 90*time.Second)
		},
	}
}

// s42Cleanup is defensive: on any failure path it clears the delay so a
// wedged reader cannot leak into the next scenario, then drops the soak
// database through the primary (the drop replicates to every follower).
func s42Cleanup(state *s42RunState) func(context.Context, *runner.Env) error {
	return func(ctx context.Context, env *runner.Env) error {
		if state.topo.reader != "" {
			if err := s42SetSourceDelay(ctx, env, state.topo.reader, 0); err != nil {
				return fmt.Errorf("cleanup: clear SOURCE_DELAY: %w", err)
			}
		}
		mfg, err := env.Kube.GetMFGNamed(ctx, env.Namespace, env.FG)
		if err != nil {
			return fmt.Errorf("cleanup: get MFG: %w", err)
		}
		if mfg.Status.ActiveSite == "" {
			return fmt.Errorf("cleanup: no active site to drop %s through", s42DBName)
		}
		primary, err := env.MySQL(mfg.Status.ActiveSite)
		if err != nil {
			return fmt.Errorf("cleanup: open primary mysql: %w", err)
		}
		if _, err := primary.Exec(ctx, "DROP DATABASE IF EXISTS "+s42DBName); err != nil {
			return fmt.Errorf("cleanup: drop %s: %w", s42DBName, err)
		}
		return nil
	}
}
