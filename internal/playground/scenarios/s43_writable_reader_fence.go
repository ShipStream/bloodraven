package scenarios

import (
	"context"
	"fmt"
	"strconv"
	"strings"
	"time"

	v1alpha1 "github.com/shipstream/bloodraven/api/v1alpha1"
	pgkube "github.com/shipstream/bloodraven/internal/playground/kube"
	pglogs "github.com/shipstream/bloodraven/internal/playground/logs"
	pgmetrics "github.com/shipstream/bloodraven/internal/playground/metrics"
	pgmysql "github.com/shipstream/bloodraven/internal/playground/mysql"
	"github.com/shipstream/bloodraven/internal/playground/runner"
)

const s43DBName = "chaos_s43"

func init() {
	runner.Register(scenario43WritableReaderFence())
}

type s43RunState struct {
	topo           readerTopology
	readerUUID     string
	rejectedBefore float64
	errantSet      string
	fencedBy       string
	recovered      bool
}

// scenario43WritableReaderFence is chaos proposal R5 from issue #115:
// role semantics hold under the worst input. A reader that somehow
// becomes writable is fenced like a dr-only loser, its errant GTID
// blocks source convergence at the GTID gate, and it is never a
// promotion target — not even by explicit admin request.
func scenario43WritableReaderFence() runner.Scenario {
	state := &s43RunState{}
	return runner.Scenario{
		ID:    "43-writable-reader-fence",
		Title: "Anomalously writable reader is fenced, blocked on divergence, and never promoted",
		Hypothesis: "Turning off super_read_only on the reader and writing an errant row triggers an un-debounced " +
			"fence back to super_read_only=ON — from the operator's poll loop or the reader's own sidecar fencing " +
			"monitor, whichever ticks first; a planned failover targeting the reader is rejected with the role " +
			"error; and the errant GTID trips the convergence GTID gate into Blocked/GTIDDiverged instead of a " +
			"silent repoint.",
		Risk:     "medium",
		DocLink:  "playground/chaos-scenarios.md#43-writable-reader-fence",
		Timeout:  10 * time.Minute,
		Precheck: s43Precheck(state),
		Steps: []runner.Step{
			s43InjectWritableReaderWithErrantRow(state),
			s43ObserveFence(state),
			s43VerifyPlannedFailoverRejected(state),
			s43VerifyErrantGtidBlocksConvergence(state),
			s43ReconcileAndRecover(state),
		},
		Cleanup: s43Cleanup(state),
	}
}

func s43Precheck(state *s43RunState) func(context.Context, *runner.Env) error {
	return func(ctx context.Context, env *runner.Env) error {
		*state = s43RunState{}
		topo, err := resolveReaderTopology(ctx, env)
		if err != nil {
			return err
		}
		state.topo = topo
		return nil
	}
}

func s43InjectWritableReaderWithErrantRow(state *s43RunState) runner.Step {
	return runner.Step{
		Phase: runner.PhaseInject,
		Name:  "turn off super_read_only on the reader and write one errant row",
		Do: func(ctx context.Context, env *runner.Env) error {
			// Open both fencing actors' tailers before injecting so the
			// fence line cannot scroll past the SinceTime window.
			if _, err := env.Logs("operator"); err != nil {
				return fmt.Errorf("open operator tailer: %w", err)
			}
			if _, err := env.Logs("sidecar:" + state.topo.reader); err != nil {
				return fmt.Errorf("open reader sidecar tailer: %w", err)
			}
			before, err := metricCounter(ctx, env, "bloodraven_planned_failovers_total", map[string]string{
				"target_site": state.topo.reader,
				"result":      "rejected",
			})
			if err != nil {
				return err
			}
			state.rejectedBefore = before

			reader, err := env.MySQL(state.topo.reader)
			if err != nil {
				return fmt.Errorf("open reader mysql: %w", err)
			}
			state.readerUUID, err = reader.ScalarString(ctx, "SELECT @@server_uuid")
			if err != nil {
				return fmt.Errorf("read reader server_uuid: %w", err)
			}
			// One multi-statement batch: the operator fences a writable
			// non-promotable site without debounce, so the errant write has
			// to land before the next poll cycle can flip the flag back.
			stmt := "SET GLOBAL super_read_only = OFF; SET GLOBAL read_only = OFF" +
				"; CREATE DATABASE " + s43DBName +
				"; CREATE TABLE " + s43DBName + ".errant (id INT PRIMARY KEY)" +
				"; INSERT INTO " + s43DBName + ".errant (id) VALUES (1)"
			if _, err := reader.Exec(ctx, stmt); err != nil {
				return fmt.Errorf("errant write on reader: %w", err)
			}
			env.Capture.Note(fmt.Sprintf("reader %s made writable and holds errant transactions from %s", state.topo.reader, state.readerUUID))
			return nil
		},
	}
}

// s43ObserveFence asserts the safety property — the reader's writable
// window closes — without encoding which control plane closes it. Two
// independent actors race here and both are correct:
//
//   - the operator's poll loop (2s), which fences any writable
//     non-promotable site without debounce, and
//   - the reader's own sidecar fencing monitor (5s), whose
//     topology-mismatch rule self-fences any site that is writable
//     while the operator-authoritative active site is someone else.
//
// The sidecar wins roughly one run in five, and when it does the
// operator never observes a writable reader at all — no state
// transition, no fence, no log line. Asserting only on the operator's
// line made this step flaky (issue #119). So: wait on the invariant
// (super_read_only back ON), then require that *an* actor logged a
// deliberate fence, so the step still fails if the window closed for
// some other reason (a mysqld restart, a lost injection).
func s43ObserveFence(state *s43RunState) runner.Step {
	return runner.Step{
		Phase: runner.PhaseObserve,
		Name:  "reader is fenced back to super_read_only without group impact",
		Do: func(ctx context.Context, env *runner.Env) error {
			reader, err := env.MySQL(state.topo.reader)
			if err != nil {
				return fmt.Errorf("open reader mysql: %w", err)
			}
			deadline := time.Now().Add(60 * time.Second)
			for {
				on, err := reader.SuperReadOnly(ctx)
				if err == nil && on {
					break
				}
				if time.Now().After(deadline) {
					return fmt.Errorf("reader super_read_only not restored within 60s (on=%v err=%v)", on, err)
				}
				select {
				case <-ctx.Done():
					return ctx.Err()
				case <-time.After(time.Second):
				}
			}

			operatorTail, err := env.Logs("operator")
			if err != nil {
				return err
			}
			sidecarTail, err := env.Logs("sidecar:" + state.topo.reader)
			if err != nil {
				return err
			}
			// The fence itself is already established — super_read_only is
			// back ON, verified above. What is left is attribution: that a
			// control plane did this deliberately, rather than a mysqld
			// restart or a lost injection.
			//
			// So match each actor on whichever line it emits promptly. For
			// the sidecar that is the topology-mismatch decision, not the
			// terminal SELF-FENCED: the terminal line trails an unbounded
			// connection eviction, and waiting on it would fail this step
			// after 30s for a fence that demonstrably succeeded — the exact
			// class of flake this scenario was fixed for.
			logCtx, cancelLog := context.WithTimeout(ctx, 30*time.Second)
			_, state.fencedBy, err = env.Wait.UntilAnyLog(logCtx, env.StartTime,
				"writable non-promotable reader is fenced by the operator or its sidecar",
				pglogs.Watch{
					Label:  "operator",
					Tailer: operatorTail,
					Pred:   pglogs.Structured("fenced writable non-promotable site", map[string]string{"site": state.topo.reader}),
				},
				pglogs.Watch{
					Label:  "sidecar:" + state.topo.reader,
					Tailer: sidecarTail,
					Pred:   pglogs.Substring("SELF-FENCING: topology mismatch"),
				})
			cancelLog()
			if err != nil {
				return err
			}

			env.Capture.Note(fmt.Sprintf("writable reader %s was fenced by %s", state.topo.reader, state.fencedBy))

			// Fencing must not cost the reader its replication channel.
			// The sidecar's post-fence connection kill used to select the
			// replica's own `system user` threads, which stopped
			// replication on the site being fenced and — because the
			// reader is diverged — left convergence permanently Blocked
			// with no operator action to explain it (issue #119).
			//
			// The second sample is past the sidecar's eviction deadline
			// (evictionTimeout, 15s), so a kill cannot land after the last
			// look: whichever actor fenced, every KILL it was going to
			// issue has either happened or been abandoned by then.
			for _, delay := range []time.Duration{0, 20 * time.Second} {
				if delay > 0 {
					select {
					case <-ctx.Done():
						return ctx.Err()
					case <-time.After(delay):
					}
				}
				if err := s43AssertReaderReplicating(ctx, reader); err != nil {
					return err
				}
			}

			mfg, err := env.Kube.GetMFGNamed(ctx, env.Namespace, env.FG)
			if err != nil {
				return err
			}
			if ready := pgkube.ReadyCondition(mfg); ready != "True" {
				return fmt.Errorf("group Ready=%q after reader fence, want True", ready)
			}
			if mfg.Status.ActiveSite != state.topo.active {
				return fmt.Errorf("active site changed after reader fence: %q -> %q", state.topo.active, mfg.Status.ActiveSite)
			}
			return nil
		},
	}
}

// s43AssertReaderReplicating fails unless the reader's I/O and applier
// threads are both running. Query errors are retried rather than
// reported: the fence's connection kill legitimately takes out this very
// session, and the pool reconnects on the next attempt.
func s43AssertReaderReplicating(ctx context.Context, reader *pgmysql.SiteClient) error {
	var lastErr error
	for attempt := 0; attempt < 3; attempt++ {
		if attempt > 0 {
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-time.After(time.Second):
			}
		}
		repl, err := reader.ShowReplicaStatus(ctx)
		if err != nil {
			lastErr = err
			continue
		}
		if !repl.IORunning || !repl.SQLRunning {
			return fmt.Errorf("reader replication stopped by the fence (io=%v sql=%v lastIO=%q lastSQL=%q); fencing must not kill replication threads",
				repl.IORunning, repl.SQLRunning, repl.LastIOError, repl.LastSQLError)
		}
		return nil
	}
	return fmt.Errorf("reader replica status after fence: %w", lastErr)
}

func s43VerifyPlannedFailoverRejected(state *s43RunState) runner.Step {
	return runner.Step{
		Phase: runner.PhaseVerify,
		Name:  "planned failover targeting the reader is rejected with the role error",
		Do: func(ctx context.Context, env *runner.Env) error {
			if err := env.Chaos.AnnotatePlannedFailover(ctx, state.topo.reader); err != nil {
				return err
			}
			waitCtx, cancel := context.WithTimeout(ctx, 60*time.Second)
			defer cancel()
			_, err := env.Wait.UntilCR(waitCtx, env.Namespace,
				"plannedFailover Failed with the only-primary-candidate role error",
				func(mfg *v1alpha1.MysqlFailoverGroup) (bool, string, error) {
					pf := mfg.Status.PlannedFailover
					if pf == nil {
						return false, "no plannedFailover status yet", nil
					}
					// Ignore terminal blocks left over from earlier scenarios
					// (metav1.Time truncates to seconds; 2s of slack).
					staleCutoff := env.StartTime.Add(-2 * time.Second)
					if pf.StartTime == nil || pf.StartTime.Time.Before(staleCutoff) {
						return false, fmt.Sprintf("ignoring stale plannedFailover (startTime=%v)", pf.StartTime), nil
					}
					msg := fmt.Sprintf("phase=%q target=%q reason=%q message=%q", pf.Phase, pf.Target, pf.Reason, pf.Message)
					if pf.Phase != v1alpha1.PlannedFailoverPhaseFailed {
						if pf.Phase == v1alpha1.PlannedFailoverPhaseSucceeded {
							return false, msg, fmt.Errorf("planned failover to reader %q succeeded; role gate is broken", state.topo.reader)
						}
						return false, msg, nil
					}
					if pf.Target != state.topo.reader {
						return false, msg, fmt.Errorf("rejected plannedFailover has target=%q, want %q", pf.Target, state.topo.reader)
					}
					if !strings.Contains(pf.Message, "only primary-candidate sites may be promoted") {
						return false, msg, fmt.Errorf("rejection message %q does not carry the role error", pf.Message)
					}
					return true, msg, nil
				})
			if err != nil {
				return err
			}

			metricCtx, cancelMetric := context.WithTimeout(ctx, 30*time.Second)
			defer cancelMetric()
			return env.Wait.UntilMetric(metricCtx, env.Metrics,
				fmt.Sprintf(`planned_failovers_total{target_site=%q,result="rejected"} increments from %g`, state.topo.reader, state.rejectedBefore),
				func(snap *pgmetrics.Snapshot) (bool, string) {
					v, _ := snap.Counter("bloodraven_planned_failovers_total", map[string]string{
						"target_site": state.topo.reader,
						"result":      "rejected",
					})
					return v > state.rejectedBefore, fmt.Sprintf("counter=%g before=%g", v, state.rejectedBefore)
				},
			)
		},
	}
}

// s43VerifyErrantGtidBlocksConvergence stops the reader's replication so
// the source-convergence invariant has to act, and asserts the GTID
// containment gate refuses to restart a diverged follower: state goes
// Blocked/GTIDDiverged with the documented log line, the blocked metric
// flips, and the reader is shed from its client Service.
func s43VerifyErrantGtidBlocksConvergence(state *s43RunState) runner.Step {
	return runner.Step{
		Phase: runner.PhaseVerify,
		Name:  "errant GTID trips the convergence gate: Blocked/GTIDDiverged, never a silent restart",
		Do: func(ctx context.Context, env *runner.Env) error {
			active, err := env.MySQL(state.topo.active)
			if err != nil {
				return fmt.Errorf("open active mysql: %w", err)
			}
			reader, err := env.MySQL(state.topo.reader)
			if err != nil {
				return fmt.Errorf("open reader mysql: %w", err)
			}
			readerGtid, err := reader.GtidExecuted(ctx)
			if err != nil {
				return err
			}
			activeGtid, err := active.GtidExecuted(ctx)
			if err != nil {
				return err
			}
			state.errantSet, err = active.ScalarString(ctx, "SELECT GTID_SUBTRACT(?, ?)", readerGtid, activeGtid)
			if err != nil {
				return fmt.Errorf("compute errant GTID set: %w", err)
			}
			state.errantSet = strings.ReplaceAll(strings.TrimSpace(state.errantSet), "\n", "")
			if state.errantSet == "" {
				return fmt.Errorf("reader holds no errant transactions after the errant write; injection did not take")
			}
			env.Capture.Note(fmt.Sprintf("reader errant GTID set: %s", state.errantSet))

			tail, err := env.Logs("operator")
			if err != nil {
				return err
			}
			if _, err := reader.Exec(ctx, "STOP REPLICA"); err != nil {
				return fmt.Errorf("stop replica on reader: %w", err)
			}

			logCtx, cancelLog := context.WithTimeout(ctx, 90*time.Second)
			_, err = env.Wait.UntilLog(logCtx, tail, env.StartTime,
				"convergence blocked on GTID containment",
				pglogs.Structured("replication source convergence blocked", map[string]string{"site": state.topo.reader}))
			cancelLog()
			if err != nil {
				return err
			}

			waitCtx, cancel := context.WithTimeout(ctx, 90*time.Second)
			mfg, err := env.Wait.UntilCR(waitCtx, env.Namespace,
				"reader sourceConvergenceState=Blocked reason=GTIDDiverged",
				func(mfg *v1alpha1.MysqlFailoverGroup) (bool, string, error) {
					status := statusSiteByName(mfg, state.topo.reader)
					if status == nil {
						return false, "reader status missing", nil
					}
					msg := fmt.Sprintf("convergence=%s/%s replicating=%v", status.SourceConvergenceState, status.SourceConvergenceReason, status.Replicating)
					done := status.SourceConvergenceState == v1alpha1.SourceConvergenceBlocked && status.SourceConvergenceReason == "GTIDDiverged"
					return done, msg, nil
				})
			cancel()
			if err != nil {
				return err
			}
			if ready := pgkube.ReadyCondition(mfg); ready != "True" {
				return fmt.Errorf("group Ready=%q while only the reader is blocked, want True", ready)
			}
			if mfg.Status.ActiveSite != state.topo.active {
				return fmt.Errorf("active site changed while reader was blocked: %q -> %q", state.topo.active, mfg.Status.ActiveSite)
			}

			metricCtx, cancelMetric := context.WithTimeout(ctx, 30*time.Second)
			defer cancelMetric()
			if err := env.Wait.UntilMetric(metricCtx, env.Metrics,
				fmt.Sprintf(`replication_source_state{site=%q,state="blocked"} == 1`, state.topo.reader),
				func(snap *pgmetrics.Snapshot) (bool, string) {
					v, ok := snap.Gauge("bloodraven_replication_source_state", map[string]string{"site": state.topo.reader, "state": "blocked"})
					return ok && v == 1, fmt.Sprintf("blocked=%g(ok=%v)", v, ok)
				},
			); err != nil {
				return err
			}

			deadline := time.Now().Add(60 * time.Second)
			var last string
			for time.Now().Before(deadline) {
				endpoints, err := env.Kube.ServiceEndpointState(ctx, env.Namespace, pgkube.MysqlDeploymentName(env.FG, state.topo.reader))
				if err != nil {
					last = err.Error()
				} else {
					ready := endpoints.ReadyPodNames("mysql")
					last = fmt.Sprintf("ready=%v", ready)
					if len(ready) == 0 {
						return nil
					}
				}
				select {
				case <-ctx.Done():
					return ctx.Err()
				case <-time.After(time.Second):
				}
			}
			return fmt.Errorf("blocked reader was not shed from its client Service within 60s (last: %s)", last)
		},
	}
}

func s43ReconcileAndRecover(state *s43RunState) runner.Step {
	return runner.Step{
		Phase: runner.PhaseSettle,
		Name:  "reconcile errant GTIDs on the primary; convergence restarts the reader",
		Do: func(ctx context.Context, env *runner.Env) error {
			if err := s43RecoverErrantReader(ctx, env, state); err != nil {
				return err
			}
			state.recovered = true
			return nil
		},
	}
}

// s43RecoverErrantReader makes the reader's errant transactions
// non-errant by committing them as empty transactions on the active
// primary (the standard errant-GTID reconciliation), waits for the
// convergence invariant to restart the reader, then drops the scenario
// database through the primary so the errant row itself is removed from
// the reader by replication.
func s43RecoverErrantReader(ctx context.Context, env *runner.Env, state *s43RunState) error {
	txns, err := gtidSetTransactions(state.errantSet)
	if err != nil {
		return fmt.Errorf("parse errant GTID set %q: %w — run `kubectl bloodraven reclone %s` to recover", state.errantSet, err, state.topo.reader)
	}
	for _, txn := range txns {
		if uuid, _, _ := strings.Cut(txn, ":"); !strings.EqualFold(uuid, state.readerUUID) {
			return fmt.Errorf("errant set %q contains foreign UUID %q (reader is %s); refusing empty-transaction reconcile — run `kubectl bloodraven reclone %s`",
				state.errantSet, uuid, state.readerUUID, state.topo.reader)
		}
	}
	active, err := env.MySQL(state.topo.active)
	if err != nil {
		return fmt.Errorf("open active mysql: %w", err)
	}
	var b strings.Builder
	for _, txn := range txns {
		fmt.Fprintf(&b, "SET GTID_NEXT='%s'; BEGIN; COMMIT; ", txn)
	}
	b.WriteString("SET GTID_NEXT='AUTOMATIC'")
	if _, err := active.Exec(ctx, b.String()); err != nil {
		return fmt.Errorf("commit %d empty transactions on %s: %w", len(txns), state.topo.active, err)
	}
	env.Capture.Note(fmt.Sprintf("committed %d empty transactions on %s to reconcile %s", len(txns), state.topo.active, state.errantSet))

	waitCtx, cancel := context.WithTimeout(ctx, 3*time.Minute)
	_, err = env.Wait.UntilCR(waitCtx, env.Namespace,
		"convergence invariant restarts the reconciled reader",
		func(mfg *v1alpha1.MysqlFailoverGroup) (bool, string, error) {
			status := statusSiteByName(mfg, state.topo.reader)
			if status == nil {
				return false, "reader status missing", nil
			}
			err := assertReaderServingStatus(mfg, status, state.topo.activeHost)
			return err == nil, fmt.Sprintf("convergence=%s/%s replicating=%v lag=%v",
				status.SourceConvergenceState, status.SourceConvergenceReason, status.Replicating, formatLag(status.SecondsBehindSource)), nil
		})
	cancel()
	if err != nil {
		return err
	}

	if _, err := active.Exec(ctx, "DROP DATABASE IF EXISTS "+s43DBName); err != nil {
		return fmt.Errorf("drop %s through primary: %w", s43DBName, err)
	}
	return waitReaderClientEndpoint(ctx, env, state.topo.reader, 90*time.Second)
}

// s43Cleanup retries the errant-GTID reconcile if the settle step never
// ran — a Blocked reader passes the shared baseline (non-promotable
// sites are exempt from the replicating check) and would otherwise leak
// into the next scenario.
func s43Cleanup(state *s43RunState) func(context.Context, *runner.Env) error {
	return func(ctx context.Context, env *runner.Env) error {
		if state.recovered || state.topo.reader == "" {
			return nil
		}
		if state.errantSet == "" {
			// Failed before the errant set was computed: either the errant
			// write never landed (nothing to do beyond dropping the DB) or
			// it landed and must be recomputed here.
			active, activeErr := env.MySQL(state.topo.active)
			reader, readerErr := env.MySQL(state.topo.reader)
			if activeErr != nil || readerErr != nil {
				return fmt.Errorf("cleanup: open mysql (active=%v reader=%v)", activeErr, readerErr)
			}
			readerGtid, err := reader.GtidExecuted(ctx)
			if err != nil {
				return fmt.Errorf("cleanup: reader gtid: %w", err)
			}
			activeGtid, err := active.GtidExecuted(ctx)
			if err != nil {
				return fmt.Errorf("cleanup: active gtid: %w", err)
			}
			errant, err := active.ScalarString(ctx, "SELECT GTID_SUBTRACT(?, ?)", readerGtid, activeGtid)
			if err != nil {
				return fmt.Errorf("cleanup: compute errant set: %w", err)
			}
			state.errantSet = strings.ReplaceAll(strings.TrimSpace(errant), "\n", "")
			if state.errantSet == "" {
				_, err := active.Exec(ctx, "DROP DATABASE IF EXISTS "+s43DBName)
				return err
			}
		}
		return s43RecoverErrantReader(ctx, env, state)
	}
}

// gtidSetTransactions expands a GTID set ("uuid:1-3:7,uuid2:5") into
// individual transactions ("uuid:1", "uuid:2", ...). Errors on an
// implausibly large set so a bad parse can never flood the primary with
// empty transactions.
func gtidSetTransactions(set string) ([]string, error) {
	var out []string
	for _, uuidPart := range strings.Split(set, ",") {
		uuidPart = strings.TrimSpace(uuidPart)
		if uuidPart == "" {
			continue
		}
		uuid, intervals, ok := strings.Cut(uuidPart, ":")
		if !ok || uuid == "" {
			return nil, fmt.Errorf("malformed GTID set element %q", uuidPart)
		}
		for _, interval := range strings.Split(intervals, ":") {
			loRaw, hiRaw, ranged := strings.Cut(interval, "-")
			lo, err := strconv.ParseInt(loRaw, 10, 64)
			if err != nil {
				return nil, fmt.Errorf("malformed GTID interval %q: %w", interval, err)
			}
			hi := lo
			if ranged {
				hi, err = strconv.ParseInt(hiRaw, 10, 64)
				if err != nil {
					return nil, fmt.Errorf("malformed GTID interval %q: %w", interval, err)
				}
			}
			if hi < lo {
				return nil, fmt.Errorf("inverted GTID interval %q", interval)
			}
			for k := lo; k <= hi; k++ {
				out = append(out, fmt.Sprintf("%s:%d", uuid, k))
				if len(out) > 50 {
					return nil, fmt.Errorf("GTID set expands past 50 transactions; refusing")
				}
			}
		}
	}
	if len(out) == 0 {
		return nil, fmt.Errorf("empty GTID set")
	}
	return out, nil
}
