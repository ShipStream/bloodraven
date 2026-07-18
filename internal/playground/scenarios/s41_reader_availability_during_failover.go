package scenarios

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	v1alpha1 "github.com/shipstream/bloodraven/api/v1alpha1"
	pgmysql "github.com/shipstream/bloodraven/internal/playground/mysql"
	"github.com/shipstream/bloodraven/internal/playground/runner"
)

const s41MarkerTable = "bloodraven_reader_e2e.failover_marker"

func init() {
	runner.Register(scenario41ReaderAvailabilityDuringFailover())
}

type s41RunState struct {
	topo    readerTopology
	oldHost string
	newHost string
	marker  string
}

// s41Observation is the continuous reader-availability record taken
// through the unplanned-failover window. Staleness is allowed;
// availability is required — a single hard read failure (after one
// reconnect attempt) fails the scenario. The observer also tracks the
// reader's reported replication source so the scenario can assert the
// reader only ever moved old-primary → new-primary and never entered a
// blocked state.
type s41Observation struct {
	mu          sync.Mutex
	err         error
	reads       int
	sourceHosts []string
}

// scenario41ReaderAvailabilityDuringFailover is chaos proposal R3 from
// issue #115: during an unplanned failover the reader keeps serving
// (stale) reads, and after promotion the source-convergence pass
// repoints it directly at the new primary.
func scenario41ReaderAvailabilityDuringFailover() runner.Scenario {
	state := &s41RunState{}
	return runner.Scenario{
		ID:    "41-reader-availability-during-failover",
		Title: "Reader keeps serving through unplanned failover, then repoints to the new primary",
		Hypothesis: "Hard-killing the active primary (sustained scale-to-0) leaves the reader answering SELECTs " +
			"throughout the failover window; after promotion the operator repoints the reader directly at the " +
			"new primary with no blocked or chained intermediate state.",
		Risk:     "medium",
		DocLink:  "playground/chaos-scenarios.md#41-reader-availability-during-unplanned-failover",
		Timeout:  8 * time.Minute,
		Precheck: s41Precheck(state),
		Steps: []runner.Step{
			s41SeedMarker(state),
			s41KillPrimaryAndObserveReader(state),
			s41VerifyReaderEndpointRestored(state),
		},
		Cleanup: s08AutoRecloneCleanup,
	}
}

func s41Precheck(state *s41RunState) func(context.Context, *runner.Env) error {
	return func(ctx context.Context, env *runner.Env) error {
		*state = s41RunState{}
		topo, err := resolveReaderTopology(ctx, env)
		if err != nil {
			return err
		}
		state.topo = topo
		state.oldHost = topo.activeHost
		state.newHost = playgroundInternalSiteHost(env.FG, topo.standby, env.Namespace)
		return nil
	}
}

func s41SeedMarker(state *s41RunState) runner.Step {
	return runner.Step{
		Phase: runner.PhasePrecheck,
		Name:  "write a marker on the active primary and confirm it reaches the reader",
		Do: func(ctx context.Context, env *runner.Env) error {
			state.marker = fmt.Sprintf("reader-failover-%d", time.Now().UnixNano())
			if err := seedMarkerRow(ctx, env, state.topo.active, s41MarkerTable, state.marker); err != nil {
				return err
			}
			return waitForMarkerOnSite(ctx, env, state.topo.reader, s41MarkerTable, state.marker, 60*time.Second)
		},
	}
}

func s41KillPrimaryAndObserveReader(state *s41RunState) runner.Step {
	return runner.Step{
		Phase: runner.PhaseInject,
		Name:  "scale active primary to 0; reader must answer SELECTs continuously and repoint to the new primary",
		Do: func(ctx context.Context, env *runner.Env) error {
			observeCtx, stopObserve := context.WithCancel(ctx)
			observation := &s41Observation{}
			observeDone := make(chan struct{})
			go func() {
				defer close(observeDone)
				s41ObserveContinuously(observeCtx, env, state, observation)
			}()
			stopAndCheck := func() error {
				stopObserve()
				<-observeDone
				return observation.result(state)
			}

			env.Capture.Note(fmt.Sprintf("killing active primary %s (sustained scale-to-0); expecting failover to %s", state.topo.active, state.topo.standby))
			if err := env.Chaos.ScaleSiteToZero(ctx, state.topo.active); err != nil {
				_ = stopAndCheck()
				return err
			}

			flipCtx, cancelFlip := context.WithTimeout(ctx, 2*time.Minute)
			_, err := env.Wait.UntilCR(flipCtx, env.Namespace,
				fmt.Sprintf("activeSite flips %s -> %s", state.topo.active, state.topo.standby),
				func(mfg *v1alpha1.MysqlFailoverGroup) (bool, string, error) {
					msg := fmt.Sprintf("activeSite=%q lastFailoverTarget=%q", mfg.Status.ActiveSite, mfg.Status.LastFailoverTarget)
					if mfg.Status.ActiveSite == "" || mfg.Status.ActiveSite == state.topo.active {
						return false, msg, nil
					}
					if mfg.Status.ActiveSite != state.topo.standby {
						return false, msg, fmt.Errorf("failover landed on %q, want promotable standby %q", mfg.Status.ActiveSite, state.topo.standby)
					}
					return true, msg, nil
				})
			cancelFlip()
			if err != nil {
				_ = stopAndCheck()
				return err
			}

			repointCtx, cancelRepoint := context.WithTimeout(ctx, 3*time.Minute)
			_, err = env.Wait.UntilCR(repointCtx, env.Namespace,
				"reader converges directly onto the new primary",
				func(mfg *v1alpha1.MysqlFailoverGroup) (bool, string, error) {
					if mfg.Status.ActiveSite != state.topo.standby {
						return false, "", fmt.Errorf("active site changed again during reader repoint: %q", mfg.Status.ActiveSite)
					}
					status := statusSiteByName(mfg, state.topo.reader)
					if status == nil {
						return false, "reader status missing", nil
					}
					err := assertReaderServingStatus(mfg, status, state.newHost)
					return err == nil, fmt.Sprintf("state=%s replicating=%v source=%q convergence=%s/%s lag=%v",
						status.State, status.Replicating, status.SourceHost, status.SourceConvergenceState, status.SourceConvergenceReason, formatLag(status.SecondsBehindSource)), nil
				})
			cancelRepoint()
			if err != nil {
				_ = stopAndCheck()
				return err
			}

			// Live write-through: a marker on the new primary must reach the
			// reader via the repointed direct channel.
			postMarker := state.marker + "-post"
			if err := seedMarkerRow(ctx, env, state.topo.standby, s41MarkerTable, postMarker); err != nil {
				_ = stopAndCheck()
				return err
			}
			if err := waitForMarkerOnSite(ctx, env, state.topo.reader, s41MarkerTable, postMarker, 90*time.Second); err != nil {
				_ = stopAndCheck()
				return err
			}

			if err := stopAndCheck(); err != nil {
				return err
			}
			observation.mu.Lock()
			reads, hosts := observation.reads, observation.sourceHosts
			observation.mu.Unlock()
			env.Capture.Note(fmt.Sprintf("reader answered %d reads through the failover window; source history=%v", reads, hosts))
			return nil
		},
	}
}

// s41VerifyReaderEndpointRestored asserts the reader's client Service
// publishes exactly the reader pod again once it is serving from the
// new primary.
func s41VerifyReaderEndpointRestored(state *s41RunState) runner.Step {
	return runner.Step{
		Phase: runner.PhaseVerify,
		Name:  "reader client Service publishes exactly the reader pod again",
		Do: func(ctx context.Context, env *runner.Env) error {
			return waitReaderClientEndpoint(ctx, env, state.topo.reader, 90*time.Second)
		},
	}
}

func s41ObserveContinuously(ctx context.Context, env *runner.Env, state *s41RunState, observation *s41Observation) {
	var client *pgmysql.SiteClient
	defer func() {
		if client != nil {
			_ = client.Close()
		}
	}()
	readMarker := func() error {
		var err error
		if client == nil {
			client, err = pgmysql.Open(ctx, env.Kube, env.Namespace, env.FG, state.topo.reader, env.Creds)
			if err != nil {
				return fmt.Errorf("open reader connection: %w", err)
			}
		}
		count, err := client.ScalarInt(ctx, "SELECT COUNT(*) FROM "+s41MarkerTable+" WHERE marker=?", state.marker)
		if err != nil {
			return err
		}
		if count != 1 {
			return fmt.Errorf("marker row missing (count=%d)", count)
		}
		return nil
	}

	ticker := time.NewTicker(500 * time.Millisecond)
	defer ticker.Stop()
	for {
		if ctx.Err() != nil {
			return
		}
		if err := readMarker(); err != nil && ctx.Err() == nil {
			// One reconnect attempt: an idle port-forward tunnel can drop
			// without the reader itself being unavailable. Two consecutive
			// failures count as a real availability gap.
			if client != nil {
				_ = client.Close()
				client = nil
			}
			if retryErr := readMarker(); retryErr != nil && ctx.Err() == nil {
				observation.fail(fmt.Errorf("reader stopped answering reads during failover: %v (after reconnect: %v)", err, retryErr))
			}
		} else if err == nil {
			observation.mu.Lock()
			observation.reads++
			observation.mu.Unlock()
		}

		mfg, err := env.Kube.GetMFGNamed(ctx, env.Namespace, env.FG)
		if err != nil {
			if ctx.Err() == nil && !errors.Is(err, ctx.Err()) {
				observation.fail(fmt.Errorf("continuous observer read MFG: %w", err))
			}
		} else if status := statusSiteByName(mfg, state.topo.reader); status != nil {
			if status.SourceConvergenceState == v1alpha1.SourceConvergenceBlocked {
				observation.fail(fmt.Errorf("reader entered SourceConvergenceState=Blocked (reason=%s) during failover", status.SourceConvergenceReason))
			}
			if status.RecoveryState == "RecoveryBlocked" {
				observation.fail(fmt.Errorf("reader entered RecoveryBlocked during failover"))
			}
			observation.recordSourceHost(status.SourceHost)
		}

		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}
	}
}

func (o *s41Observation) fail(err error) {
	o.mu.Lock()
	defer o.mu.Unlock()
	if o.err == nil {
		o.err = err
	}
}

func (o *s41Observation) recordSourceHost(host string) {
	canonical := canonicalMySQLHost(host)
	if canonical == "" {
		return
	}
	o.mu.Lock()
	defer o.mu.Unlock()
	if n := len(o.sourceHosts); n == 0 || o.sourceHosts[n-1] != canonical {
		o.sourceHosts = append(o.sourceHosts, canonical)
	}
}

// result validates the whole observation window: no availability gap,
// enough samples to be meaningful, and a source-host history that only
// ever moves old-primary → new-primary. Any third host, or a return to
// the demoted primary after the repoint, means the reader applied
// events through a chained or stale channel.
func (o *s41Observation) result(state *s41RunState) error {
	o.mu.Lock()
	defer o.mu.Unlock()
	if o.err != nil {
		return o.err
	}
	if o.reads < 20 {
		return fmt.Errorf("availability observer completed only %d reads; too few to cover the failover window", o.reads)
	}
	oldHost := canonicalMySQLHost(state.oldHost)
	newHost := canonicalMySQLHost(state.newHost)
	sawNew := false
	for _, host := range o.sourceHosts {
		switch host {
		case oldHost:
			if sawNew {
				return fmt.Errorf("reader source flipped back to demoted primary after repoint: history=%v", o.sourceHosts)
			}
		case newHost:
			sawNew = true
		default:
			return fmt.Errorf("reader reported unexpected replication source %q (history=%v)", host, o.sourceHosts)
		}
	}
	if !sawNew {
		return fmt.Errorf("reader never reported the new primary as its source: history=%v", o.sourceHosts)
	}
	if n := len(o.sourceHosts); o.sourceHosts[n-1] != newHost {
		return fmt.Errorf("reader source history does not end on the new primary: %v", o.sourceHosts)
	}
	return nil
}
