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
// the operator logged it. The first write must come inside the failover
// budget the scenario already had. The flush is best effort in the
// operator, so it is a measurement here too: one that was not acknowledged,
// or not under controller.BinlogRotationTimeout, is reported with a
// warning and does not fail the run (a loaded CI cluster can be that slow,
// and the promotion is the same without it). When it was acknowledged, the
// scenario also checks on the promoted server that a binary log file
// starts at the recorded promotion GTID set (checkPromotionBinlogFile) and
// reports the result; a mismatch is a warning, not a failure
// (s01CheckPromotionBinlogFile says why).
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
			"The new primary acknowledges its first write within " + failoverObserveBudget.String() + " of the fault. The promotion's " +
			"FLUSH BINARY LOGS (best effort, bounded at " + brcontroller.BinlogRotationTimeout.String() + ") is measured and reported, " +
			"and when it was acknowledged a binary log file of the new primary starts at the recorded promotion GTID set.",
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

// s01MeasureFailover reports the failover's RTO under load and the
// promotion's binary log rotation. It holds the RTO to the failover budget;
// the rotation is a measurement (see scenario01CleanPrimaryKill).
func s01MeasureFailover(st *s01State) runner.Step {
	return runner.Step{
		Phase: runner.PhaseVerify,
		Name:  "first write on the new primary within the failover budget; promotion FLUSH BINARY LOGS measured",
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
				logPromotionBinlogRotated, logPromotionBinlogRotationFailed, s01LogWait)
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
				"promotionFlushMs", m.Rotation.Duration.Milliseconds(), "promotionFlushAcknowledged", rotation.acknowledged(),
				"promotionFlushBoundMs", m.RotationBound.Milliseconds(), "loadAcked", m.LoadAcked)
			if rotationErr != nil {
				// The operator logged neither rotation line, or one
				// that breaks the log schema: the step was not sent or
				// not reported. That is not a slow rotation.
				return rotationErr
			}
			if concern := rotation.concern(m.RotationBound); concern != "" {
				// Best effort in the operator, so a measurement here.
				env.Capture.Measure("WARNING: " + concern)
				env.Logger.Warn("promotion binlog rotation not acknowledged under its bound; reported, not failed",
					"site", newPrimary, "acknowledged", rotation.acknowledged(), "timedOut", rotation.TimedOut,
					"durationMs", rotation.Duration.Milliseconds(), "boundMs", m.RotationBound.Milliseconds())
			}
			if err := m.check(failoverObserveBudget); err != nil {
				return err
			}
			s01CheckPromotionBinlogFile(ctx, env, newPrimary, rotation)
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

// s01PromotionFileTimeout bounds the reads of the promotion binlog file
// check.
const s01PromotionFileTimeout = 15 * time.Second

// s01CheckPromotionBinlogFile compares the promoted server's binary log
// files with status.lastFailover.promotionGtidExecuted
// (checkPromotionBinlogFile) and reports the result. It is skipped, with a
// note, when the rotation was not acknowledged: nothing then says a file
// starts at the promotion. A check that could not be made is reported as
// such.
//
// A proven mismatch after an acknowledged rotation is reported with a
// warning and does not fail the run. The emergency promotion does not
// guarantee the property: it holds when nothing commits on the candidate
// between the operator's GTID read and its rotation, which the operator
// assumes of a read-only server without a replication channel and does not
// prove (FailoverController.promote lists the exceptions). This check has
// not run on a real cluster yet, so whether that assumption always holds in
// the playground is not known, and this scenario gates the smoke and
// release profiles. Once runs have shown it verified, make the mismatch an
// error: a rotation moved after the write grant would then fail here.
func s01CheckPromotionBinlogFile(ctx context.Context, env *runner.Env, newPrimary string, rotation binlogRotationLog) {
	if !rotation.acknowledged() {
		env.Capture.Measure("promotion binlog file on " + newPrimary + ": not checked: the operator's rotation was not acknowledged")
		return
	}
	checkCtx, cancel := context.WithTimeout(ctx, s01PromotionFileTimeout)
	defer cancel()
	res := s01PromotionBinlogFile(checkCtx, env, newPrimary)
	switch res.Verdict {
	case promotionFileVerified:
		env.Capture.Measure(fmt.Sprintf("promotion binlog file on %s: %s (status.lastFailover.promotionGtidExecuted)", newPrimary, res.Detail))
	case promotionFileMismatch:
		env.Capture.Measure(fmt.Sprintf("WARNING: promotion binlog file on %s: MISMATCH: the operator's FLUSH BINARY LOGS was acknowledged, but %s", newPrimary, res.Detail))
		env.Logger.Warn("promotion binlog file does not start at the recorded promotion GTID set; reported, not failed",
			"site", newPrimary, "detail", res.Detail)
	default:
		env.Capture.Measure(fmt.Sprintf("promotion binlog file on %s: not checked: %s", newPrimary, res.Detail))
	}
}

// s01PromotionBinlogFile reads the group's failover record and the promoted
// server's binary logs for the check.
func s01PromotionBinlogFile(ctx context.Context, env *runner.Env, newPrimary string) promotionFileCheck {
	notChecked := func(format string, args ...any) promotionFileCheck {
		return promotionFileCheck{Verdict: promotionFileNotChecked, Detail: fmt.Sprintf(format, args...)}
	}
	mfg, err := env.Kube.GetMFGNamed(ctx, env.Namespace, env.FG)
	if err != nil {
		return notChecked("read the group: %v", err)
	}
	rec := mfg.Status.LastFailover
	if rec == nil || mfg.Status.LastFailoverTarget() != newPrimary {
		return notChecked("status.lastFailover does not record the promotion of %s (target %q)", newPrimary, mfg.Status.LastFailoverTarget())
	}
	client, err := pgmysql.Open(ctx, env.Kube, env.Namespace, env.FG, newPrimary, env.Creds)
	if err != nil {
		return notChecked("open %s: %v", newPrimary, err)
	}
	defer client.Close()
	return checkPromotionBinlogFile(ctx, sqlBinlogHeaders{db: client.DB}, rec.PromotionGtidExecuted)
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
