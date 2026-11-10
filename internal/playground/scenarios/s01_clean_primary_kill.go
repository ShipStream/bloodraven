package scenarios

import (
	"context"
	"fmt"
	"time"

	v1alpha1 "github.com/shipstream/bloodraven/api/v1alpha1"
	brcontroller "github.com/shipstream/bloodraven/internal/controller"
	pglogs "github.com/shipstream/bloodraven/internal/playground/logs"
	pgmetrics "github.com/shipstream/bloodraven/internal/playground/metrics"
	pgmysql "github.com/shipstream/bloodraven/internal/playground/mysql"
	"github.com/shipstream/bloodraven/internal/playground/runner"
)

func init() {
	runner.Register(scenario01CleanPrimaryKill())
}

const (
	s01ID = "01-clean-primary-kill"
	s01DB = "chaos_s01"
	// s01LoadTable takes the write load on the primary; s01ProbeTable the
	// write probe's row on the failover target.
	s01LoadTable  = "load_acked"
	s01ProbeTable = "rto_probe"
	// s01LoadInterval is the load's pace: one single-row transaction every
	// 50 ms, about 20 a second, like the PITR scenarios' background writer.
	s01LoadInterval = 50 * time.Millisecond
	// s01ProbeInterval is how often the probe looks at the failover target,
	// and so the resolution of the measured RTO.
	s01ProbeInterval = 100 * time.Millisecond
	// s01LoadWarmup is how long the load runs before the fault.
	s01LoadWarmup = 3 * time.Second
	// s01LogWait bounds the wait for the probe's first write and for the
	// operator's rotation line once the failover has been observed. Both
	// precede the activeSite flip; the wait covers log delivery.
	s01LogWait = 30 * time.Second
	// s01RepairTimeout bounds cleanup's wait for the old primary to rejoin
	// (its pod start and the anti-flap cooldown), with a reclone if it came
	// back diverged. It is the wait s08AutoRecloneCleanup gives a reclone.
	s01RepairTimeout = 4 * time.Minute
)

// failoverObserveBudget is how long observeFailover gives an emergency
// failover to complete; scenario 01 also holds the first write on the new
// primary to it.
const failoverObserveBudget = 90 * time.Second

// faultInjectedAtKey stashes when injectScaleZero took the primary down
// (RFC 3339 with nanoseconds, the runner's clock).
const faultInjectedAtKey = "faultInjectedAt"

// s01State is the write load and probe of one run.
type s01State struct {
	load  *ackedWriter
	probe *writeProbe
}

// scenario01CleanPrimaryKill scales the active primary's MySQL
// deployment to 0 (per chaos-scenarios.md:13 — pod-delete races the
// Deployment respawn), waits for failover, then asserts the standard
// signals: status.activeSite flipped, failovers_total incremented,
// "failover complete" log emitted.
//
// The failover runs under a write load, and the scenario measures it
// (#229): the time from the fault to the first write the new primary
// acknowledges, and the duration of the promotion's FLUSH BINARY LOGS as
// the operator logged it. The flush must have been acknowledged under
// controller.BinlogRotationTimeout and the first write must come inside
// the failover budget the scenario already had.
//
// Recovery of the old primary is intentionally NOT asserted here —
// that's scenario 12's job. See plan §10 risk #4. Cleanup does wait for
// it, and reclones the old primary if the load left it diverged.
func scenario01CleanPrimaryKill() runner.Scenario {
	st := &s01State{}
	return runner.Scenario{
		ID:    s01ID,
		Title: "Clean primary kill under write load — failover to peer, RTO measured",
		Hypothesis: "Scaling the active primary deployment to 0 while it takes writes triggers an emergency failover within ~45s. " +
			"Status.activeSite flips, bloodraven_failovers_total increments, and 'failover complete' is logged. " +
			"The new primary acknowledges its first write within " + failoverObserveBudget.String() + " of the fault, and the promotion's " +
			"FLUSH BINARY LOGS is acknowledged in under " + brcontroller.BinlogRotationTimeout.String() + ".",
		Risk:    "low",
		DocLink: "playground/chaos-scenarios.md#1-clean-primary-failure",
		// One minute more than before the load and the measurement were
		// added: they take a few seconds before the fault and wait at most
		// s01LogWait twice after it, and the existing waits keep their
		// budgets.
		Timeout: 4 * time.Minute,
		// The repair wait, plus two minutes for the revert, the schema
		// drop and the executor's reconverge wait.
		CleanupTimeout: s01RepairTimeout + 2*time.Minute,
		Precheck: func(ctx context.Context, env *runner.Env) error {
			// A retried run (--auto-reset) starts without the last one's
			// load and probe.
			*st = s01State{}
			return AssertHealthyBaseline(ctx, env)
		},
		Steps: []runner.Step{
			s01StartLoad(st),
			injectScaleZero(),
			observeFailover(),
			verifyFailoverMetric(),
			verifyFailoverLog(),
			s01MeasureFailover(st),
		},
		Cleanup: s01Cleanup(st),
	}
}

// s01StartLoad starts the write load on the primary and the write probe on
// the failover target, both before the fault.
func s01StartLoad(st *s01State) runner.Step {
	return runner.Step{
		Phase: runner.PhaseInject,
		Name:  "start a write load on the primary and a write probe on the failover target",
		Do: func(ctx context.Context, env *runner.Env) error {
			mfg, err := env.Kube.GetMFGNamed(ctx, env.Namespace, env.FG)
			if err != nil {
				return err
			}
			active := mfg.Status.ActiveSite
			peer, err := PeerOf(mfg, active)
			if err != nil {
				return err
			}
			// Opened before the fault so the rotation line is buffered.
			if _, err := env.Logs("operator"); err != nil {
				return err
			}
			primary, err := pgmysql.Open(ctx, env.Kube, env.Namespace, env.FG, active, env.Creds)
			if err != nil {
				return fmt.Errorf("open %s: %w", active, err)
			}
			_, err = primary.Exec(ctx, writeProbeTableDDL(s01DB, s01ProbeTable))
			_ = primary.Close()
			if err != nil {
				return fmt.Errorf("create the write-probe table on %s: %w", active, err)
			}
			if st.load, err = startAckedWriter(ctx, env, active, s01DB, s01LoadTable, s01LoadInterval); err != nil {
				return err
			}
			// The probe's table, and the load, have reached the target.
			if err := waitForReplicaGTID(ctx, env, active, peer); err != nil {
				return err
			}
			if st.probe, err = startWriteProbe(ctx, env, peer, s01DB, s01ProbeTable, s01ProbeInterval); err != nil {
				return err
			}
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-time.After(s01LoadWarmup):
			}
			if len(st.load.ackedIDs()) == 0 {
				return fmt.Errorf("the load acknowledged nothing in %s: %s", s01LoadWarmup, st.load.summary())
			}
			env.Capture.Note("before the fault: " + st.load.summary() + "; " + st.probe.summary())
			return nil
		},
	}
}

func injectScaleZero() runner.Step {
	return runner.Step{
		Phase: runner.PhaseInject,
		Name:  "scale active primary to 0 replicas",
		Do: func(ctx context.Context, env *runner.Env) error {
			mfg, err := env.Kube.GetMFGNamed(ctx, env.Namespace, env.FG)
			if err != nil {
				return err
			}
			active := mfg.Status.ActiveSite
			peer, err := PeerOf(mfg, active)
			if err != nil {
				return err
			}
			env.Capture.Note(fmt.Sprintf("active primary at start: %s", active))
			if err := ctxStash(ctx, env, "expectedNewPrimary", peer); err != nil {
				return err
			}
			if err := stashMetricCounter(ctx, env, "failoversBefore", "bloodraven_failovers_total", map[string]string{"target_site": peer}); err != nil {
				return err
			}
			env.Logger.Info("injecting", "action", "scale=0", "site", active)
			// The fault's instant, for scenarios that measure from it.
			if err := ctxStash(ctx, env, faultInjectedAtKey, time.Now().UTC().Format(time.RFC3339Nano)); err != nil {
				return err
			}
			if err := env.Chaos.ScaleSiteToZero(ctx, active); err != nil {
				return err
			}
			// Stash for later steps.
			env.Capture.Note("primary scaled to 0; waiting for failover")
			return ctxStash(ctx, env, "originalPrimary", active)
		},
	}
}

func observeFailover() runner.Step {
	return runner.Step{
		Phase: runner.PhaseObserve,
		Name:  "wait for activeSite to flip",
		Do: func(ctx context.Context, env *runner.Env) error {
			original := ctxFetch(env, "originalPrimary")
			waitCtx, cancel := context.WithTimeout(ctx, failoverObserveBudget)
			defer cancel()
			_, err := env.Wait.UntilCR(waitCtx, env.Namespace,
				fmt.Sprintf("activeSite changes from %s and is observed writable", original),
				func(mfg *v1alpha1.MysqlFailoverGroup) (bool, string, error) {
					done, msg := failoverObserved(mfg, original)
					return done, msg, nil
				},
			)
			return err
		},
	}
}

func failoverObserved(mfg *v1alpha1.MysqlFailoverGroup, original string) (bool, string) {
	var writable []string
	for _, site := range mfg.Status.Sites {
		if site.State == v1alpha1.SiteStateWritable {
			writable = append(writable, site.Name)
		}
	}
	active := mfg.Status.ActiveSite
	msg := fmt.Sprintf("activeSite=%q lastFailoverTarget=%q writable=%v", active, mfg.Status.LastFailoverTarget(), writable)
	// Promotion authority can be published before the polled site state catches up.
	return active != "" && active != original && len(writable) == 1 && writable[0] == active, msg
}

func verifyFailoverMetric() runner.Step {
	return runner.Step{
		Phase: runner.PhaseVerify,
		Name:  "bloodraven_failovers_total increments for new primary",
		Do: func(ctx context.Context, env *runner.Env) error {
			mfg, err := env.Kube.GetMFGNamed(ctx, env.Namespace, env.FG)
			if err != nil {
				return err
			}
			newPrimary := mfg.Status.ActiveSite
			expected := ctxFetch(env, "expectedNewPrimary")
			if newPrimary != expected {
				return fmt.Errorf("activeSite=%q, want failover target %q", newPrimary, expected)
			}
			before, err := fetchStashedFloat(env, "failoversBefore")
			if err != nil {
				return err
			}
			waitCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
			defer cancel()
			return env.Wait.UntilMetric(waitCtx, env.Metrics,
				fmt.Sprintf("bloodraven_failovers_total{target_site=%q} increments from %g", newPrimary, before),
				func(snap *pgmetrics.Snapshot) (bool, string) {
					v, _ := snap.Counter("bloodraven_failovers_total", map[string]string{"target_site": newPrimary})
					return v > before, fmt.Sprintf("counter=%g before=%g delta=%g", v, before, v-before)
				},
			)
		},
	}
}

func verifyFailoverLog() runner.Step {
	return runner.Step{
		Phase: runner.PhaseVerify,
		Name:  `"failover complete" line appears in operator log`,
		Do: func(ctx context.Context, env *runner.Env) error {
			tail, err := env.Logs("operator")
			if err != nil {
				return err
			}
			waitCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
			defer cancel()
			_, err = env.Wait.UntilLog(waitCtx, tail, env.StartTime, `"failover complete"`,
				pglogs.Substring(`failover complete`))
			return err
		},
	}
}

// s01MeasureFailover reports the failover's RTO under load and the cost of
// the promotion's binary log rotation, and holds both to their bounds.
func s01MeasureFailover(st *s01State) runner.Step {
	return runner.Step{
		Phase: runner.PhaseVerify,
		Name:  "first write on the new primary within the failover budget; promotion FLUSH BINARY LOGS acknowledged under its bound",
		Do: func(ctx context.Context, env *runner.Env) error {
			oldPrimary, newPrimary := ctxFetch(env, "originalPrimary"), ctxFetch(env, "expectedNewPrimary")
			faultAt, err := time.Parse(time.RFC3339Nano, ctxFetch(env, faultInjectedAtKey))
			if err != nil {
				return fmt.Errorf("the fault's instant was not recorded: %w", err)
			}
			// The old primary is gone; its last acknowledged write stands.
			st.load.stop()
			firstAck, err := s01AwaitFirstWrite(ctx, st.probe)
			if err != nil {
				return err
			}
			tail, err := env.Logs("operator")
			if err != nil {
				return err
			}
			rotation, rotationErr := waitBinlogRotation(ctx, operatorLogSince(tail, faultAt.Add(-logClockSlack)), newPrimary,
				logPromotionBinlogRotated, logPromotionBinlogRotationFailed, brcontroller.BinlogRotationTimeout, s01LogWait)
			m := failoverRTO{
				Old: oldPrimary, New: newPrimary,
				FaultAt: faultAt, LastOldAck: st.load.lastAckAt(), FirstNewAck: firstAck,
				LoadAcked: len(st.load.ackedIDs()),
				Rotation:  rotation, RotationBound: brcontroller.BinlogRotationTimeout,
			}
			// Reported before it is judged, so a run over budget, or one
			// whose rotation failed, still shows its numbers.
			env.Capture.Measure(m.String())
			env.Logger.Info("failover under load measured", "oldPrimary", m.Old, "newPrimary", m.New,
				"rtoMs", m.rto().Milliseconds(), "writeOutageMs", m.outage().Milliseconds(),
				"promotionFlushMs", m.Rotation.Duration.Milliseconds(), "promotionFlushAcknowledged", rotationErr == nil,
				"promotionFlushBoundMs", m.RotationBound.Milliseconds(), "loadAcked", m.LoadAcked)
			if rotationErr != nil {
				return rotationErr
			}
			if err := m.check(failoverObserveBudget); err != nil {
				return err
			}
			// Replication is asynchronous: the newest acknowledged writes
			// may not have reached the new primary. Reported, not asserted.
			if missing, err := missingAckedIDs(ctx, env, newPrimary, quoteSQLIdentifier(s01DB)+"."+quoteSQLIdentifier(s01LoadTable), st.load.ackedIDs()); err != nil {
				env.Capture.Note("acknowledged writes on the new primary not counted: " + err.Error())
			} else {
				env.Capture.Measure(fmt.Sprintf("acknowledged writes missing on %s after the failover: %d of %d %s", newPrimary, len(missing), m.LoadAcked, formatIDs(missing)))
			}
			return nil
		},
	}
}

// s01AwaitFirstWrite waits for the probe's first acknowledged write. The
// failover has been observed by now, so the write is normally there.
func s01AwaitFirstWrite(ctx context.Context, probe *writeProbe) (time.Time, error) {
	waitCtx, cancel := context.WithTimeout(ctx, s01LogWait)
	defer cancel()
	tick := time.NewTicker(100 * time.Millisecond)
	defer tick.Stop()
	for {
		if at, ok := probe.firstAckAt(); ok {
			return at, nil
		}
		select {
		case <-waitCtx.Done():
			return time.Time{}, fmt.Errorf("the new primary acknowledged no write within %s of the observed failover: %s", s01LogWait, probe.summary())
		case <-tick.C:
		}
	}
}

// s01Cleanup stops the load and the probe, waits for the old primary to
// rejoin (the executor's revert has scaled it back up), reclones it if it
// came back diverged, and drops the scenario's schema. A primary killed
// while it takes writes can hold transactions its replica never received;
// left RecoveryBlocked it would fail this scenario's cleanup and the next
// scenario's precheck. A run that never started the load (a failed
// precheck) wrote nothing and cleans up as it did before the load: one
// look for a divergent site, no wait for a baseline that was not healthy
// to begin with.
func s01Cleanup(st *s01State) func(context.Context, *runner.Env) error {
	return func(ctx context.Context, env *runner.Env) error {
		if st.load == nil {
			return s08AutoRecloneCleanup(ctx, env)
		}
		st.load.stop()
		st.probe.stop()
		if err := repairDivergentSites(ctx, env, s01RepairTimeout); err != nil {
			return fmt.Errorf("s01 cleanup: %w", err)
		}
		if err := dropMarkerSchemaAndReplicate(ctx, env, s01DB); err != nil {
			return fmt.Errorf("s01 cleanup: %w", err)
		}
		return nil
	}
}

// ctxStash / ctxFetch are tiny per-scenario state helpers. They live
// on the Capture's notes for forensic purposes and on a small in-memory
// map keyed by the env pointer. All map access is performed under the
// stashMu lock so concurrent scenario goroutines (or the race detector)
// see consistent state.
func ctxStash(_ context.Context, env *runner.Env, key, value string) error {
	stashSet(env, key, value)
	env.Capture.Note(fmt.Sprintf("stash %s=%s", key, value))
	return nil
}

func ctxFetch(env *runner.Env, key string) string {
	return stashGet(env, key)
}
