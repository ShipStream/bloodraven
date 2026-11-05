package scenarios

import (
	"context"
	"errors"
	"fmt"
	"regexp"
	"strings"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	v1alpha1 "github.com/shipstream/bloodraven/api/v1alpha1"
	pglogs "github.com/shipstream/bloodraven/internal/playground/logs"
	pgmetrics "github.com/shipstream/bloodraven/internal/playground/metrics"
	pgmysql "github.com/shipstream/bloodraven/internal/playground/mysql"
	"github.com/shipstream/bloodraven/internal/playground/runner"
)

func init() {
	runner.Register(scenario63PlannedFailoverOperatorRestartMidLag())
}

const (
	s63ID     = "63-planned-failover-operator-restart-mid-lag"
	s63DBName = "chaos_s63"
	// s63SourceDelay builds the backlog that gets the run into WaitingForLag.
	// It has to outlast the gap between the claim and the SQL-thread stop,
	// which is seconds; it is long so a slow drain cannot let it run out.
	s63SourceDelay = 600
	// s63MaxLagWait covers the hold: observing WaitingForLag, killing the
	// operator, waiting for its replacement, the log wait and the soak. A
	// LagTimeout here means the restart took far longer than it should.
	s63MaxLagWait = "8m"
	// s63HoldSoak is how long the hold is watched after the replacement
	// operator has proved it is running. The emergency debounce is about
	// 6s, so a manager that ignored the hydrated claim would promote well
	// inside this window.
	s63HoldSoak = 30 * time.Second

	// plannedClaimedEventReason is the Event the run emits once, when the
	// topology manager grants its claim (spec §11 Events).
	plannedClaimedEventReason = "PlannedFailoverClaimed"
	// plannedDeferredCrossSiteLog is the topology manager's line for a
	// cross-site action deferred while a planned failover holds its claim.
	plannedDeferredCrossSiteLog = "cross-site action deferred: planned failover in progress"

	// resetReplicaAllRegexp matches a RESET REPLICA ALL (or the legacy
	// RESET SLAVE ALL) statement at the start of a general-log argument,
	// so the counting SELECT itself never matches.
	resetReplicaAllRegexp = `^[[:space:]]*RESET[[:space:]]+(REPLICA|SLAVE)[[:space:]]+ALL`
)

// logOutputValue matches a valid @@log_output value.
var logOutputValue = regexp.MustCompile(`^(?i)(FILE|TABLE|NONE)(,(FILE|TABLE|NONE))*$`)

// Stash keys. originalPrimary and switchoverTarget are shared with scenario
// 02's verifyFollowersDirectlyFollowNewPrimary, which this scenario reuses.
const (
	s63KeySource          = "originalPrimary"
	s63KeyTarget          = "switchoverTarget"
	s63KeyBaselineRunID   = "s63.baselineRunID"
	s63KeyBaselineLFT     = "s63.baselineLastFailoverTarget"
	s63KeyRunID           = "s63.runID"
	s63KeyAnnotatedAt     = "s63.annotatedAt"
	s63KeyGeneralLog      = "s63.origGeneralLog"
	s63KeyLogOutput       = "s63.origLogOutput"
	s63KeyLogMark         = "s63.generalLogMark"
	s63KeyKilledAt        = "s63.operatorKilledAt"
	s63KeyNewOperatorPod  = "s63.newOperatorPod"
	s63KeyOldOperatorPods = "s63.oldOperatorPods"
)

// scenario63PlannedFailoverOperatorRestartMidLag proves that a planned
// failover's claim is durable across an operator restart (#235, spec §15.5).
//
// The run is held in WaitingForLag with the target's applier stopped. The
// target cannot have a stopped SQL thread before the claim: the grant's peer
// evidence (spec §4.1) requires both replication threads running, so a target
// stopped in advance is refused, not held. The backlog therefore comes from a
// SOURCE_DELAY applied before the annotation (both threads keep running, as in
// scenario 35), and STOP REPLICA SQL_THREAD lands once the run is in
// WaitingForLag. From then on nothing but this scenario can release the hold.
//
// With the run held, the operator pod is killed. The replacement must
// rehydrate the claim from status: the same runId, still WaitingForLag, the
// target still read-only (no emergency promotion of a fenced group's only
// candidate), the source still fenced, and its topology manager logging the
// cross-site deferral. Restarting the SQL thread must then complete the same
// run with exactly one PlannedFailoverClaimed Event, exactly one RESET
// REPLICA ALL on the target (counted from the MySQL general log, which
// catches a recovery path that re-pointed the stopped replica behind the
// claim's back) and both server-uuid records present and matching the live
// servers.
func scenario63PlannedFailoverOperatorRestartMidLag() runner.Scenario {
	return runner.Scenario{
		ID:    s63ID,
		Title: "Planned failover claim survives an operator restart mid WaitingForLag",
		Hypothesis: "With the target's SQL thread stopped mid WaitingForLag, killing the operator leaves the run " +
			"claimed: the replacement keeps the same runId in WaitingForLag, promotes nothing, and logs the " +
			"cross-site deferral. Restarting the SQL thread ends the run Succeeded with one PlannedFailoverClaimed " +
			"Event, one RESET REPLICA ALL on the target, and fence.sourceServerUUID and " +
			"promotion.targetServerUUID recorded.",
		Risk:     "medium",
		DocLink:  "playground/chaos-scenarios.md#63-planned-failover-operator-restart-mid-lag",
		Timeout:  14 * time.Minute,
		Precheck: AssertHealthyBaseline,
		Steps: []runner.Step{
			s63PrepareBacklogAndAnnotate(),
			s63HoldInWaitingForLag(),
			s63KillOperator(),
			s63VerifyReplacementDefers(),
			s63VerifyHoldSurvivesRestart(),
			s63RestartTargetApplier(),
			s63ObserveSucceeded(),
			s63VerifyRunRecords(),
			s63VerifyClaimedOnce(),
			s63VerifyOneResetReplicaAll(),
			verifyFollowersDirectlyFollowNewPrimary(),
		},
		Cleanup:        s63Cleanup,
		CleanupTimeout: 6 * time.Minute,
	}
}

func s63PrepareBacklogAndAnnotate() runner.Step {
	return runner.Step{
		Phase: runner.PhaseInject,
		Name:  "general log on target, SOURCE_DELAY backlog, marker on source, annotate maxLagWait=" + s63MaxLagWait,
		Do: func(ctx context.Context, env *runner.Env) error {
			mfg, err := env.Kube.GetMFGNamed(ctx, env.Namespace, env.FG)
			if err != nil {
				return err
			}
			source := mfg.Status.ActiveSite
			if source == "" {
				return fmt.Errorf("no active site")
			}
			target, err := PeerOf(mfg, source)
			if err != nil {
				return err
			}
			baselineRunID := ""
			if pf := mfg.Status.PlannedFailover; pf != nil {
				baselineRunID = pf.RunID
			}
			for k, v := range map[string]string{
				s63KeySource:        source,
				s63KeyTarget:        target,
				s63KeyBaselineRunID: baselineRunID,
				s63KeyBaselineLFT:   mfg.Status.LastFailoverTarget,
			} {
				if err := ctxStash(ctx, env, k, v); err != nil {
					return err
				}
			}

			targetClient, err := env.MySQL(target)
			if err != nil {
				return fmt.Errorf("open target %s: %w", target, err)
			}
			rs, err := targetClient.ShowReplicaStatus(ctx)
			if err != nil {
				return fmt.Errorf("target %s replica status: %w", target, err)
			}
			if !rs.Configured || !rs.IORunning || !rs.SQLRunning {
				return fmt.Errorf("target %s replication not running before the run (configured=%v io=%v sql=%v) — run ./playground/reset-mysql.sh",
					target, rs.Configured, rs.IORunning, rs.SQLRunning)
			}

			// 1) General log to a table on the target, so the verify step can
			//    count RESET REPLICA ALL statements by server time. The
			//    originals are stashed first so cleanup always restores them.
			if err := s63EnableGeneralLog(ctx, env, targetClient); err != nil {
				return err
			}

			// 2) Backlog: delay the target's applier while both threads keep
			//    running, so the claim's target evidence still passes.
			if err := setSourceDelay(ctx, targetClient, s63SourceDelay); err != nil {
				return err
			}
			env.Capture.Note(fmt.Sprintf("SOURCE_DELAY=%ds applied on target %s", s63SourceDelay, target))

			// 3) A marker written after the delay: the fenced set always holds
			//    at least one transaction the target has not applied.
			sourceClient, err := env.MySQL(source)
			if err != nil {
				return fmt.Errorf("open source %s: %w", source, err)
			}
			for _, q := range []string{
				"CREATE DATABASE IF NOT EXISTS " + s63DBName,
				"CREATE TABLE IF NOT EXISTS " + s63DBName + ".marker (id INT PRIMARY KEY, note VARCHAR(64))",
				fmt.Sprintf("INSERT INTO %s.marker (id, note) VALUES (1, 'restart-mid-lag') ON DUPLICATE KEY UPDATE note=VALUES(note)", s63DBName),
			} {
				if _, err := sourceClient.Exec(ctx, q); err != nil {
					return fmt.Errorf("marker stmt %q: %w", q, err)
				}
			}

			// 4) The request, with a lag budget wider than the whole hold.
			if err := ctxStash(ctx, env, s63KeyAnnotatedAt, time.Now().UTC().Format(time.RFC3339Nano)); err != nil {
				return err
			}
			raw := fmt.Sprintf("%s:maxLagWait=%s", target, s63MaxLagWait)
			env.Capture.Note(fmt.Sprintf("planned switchover %s -> %s; annotating planned-failover=%s", source, target, raw))
			return env.Chaos.AnnotatePlannedFailoverRaw(ctx, raw)
		},
	}
}

// s63EnableGeneralLog stashes the target's general_log and log_output, then
// routes the general log to mysql.general_log and records the server's own
// clock as the counting mark (no runner/server clock skew).
func s63EnableGeneralLog(ctx context.Context, env *runner.Env, c *pgmysql.SiteClient) error {
	origLog, err := c.ScalarString(ctx, "SELECT @@GLOBAL.general_log")
	if err != nil {
		return fmt.Errorf("read general_log: %w", err)
	}
	origOutput, err := c.ScalarString(ctx, "SELECT @@GLOBAL.log_output")
	if err != nil {
		return fmt.Errorf("read log_output: %w", err)
	}
	if err := ctxStash(ctx, env, s63KeyGeneralLog, origLog); err != nil {
		return err
	}
	if err := ctxStash(ctx, env, s63KeyLogOutput, origOutput); err != nil {
		return err
	}
	for _, q := range []string{"SET GLOBAL log_output = 'TABLE'", "SET GLOBAL general_log = ON"} {
		if _, err := c.Exec(ctx, q); err != nil {
			return fmt.Errorf("%s: %w", q, err)
		}
	}
	mark, err := c.ScalarString(ctx, "SELECT DATE_FORMAT(NOW(6), '%Y-%m-%d %H:%i:%s.%f')")
	if err != nil {
		return fmt.Errorf("read server time: %w", err)
	}
	env.Capture.Note(fmt.Sprintf("general log enabled on target (was general_log=%s log_output=%s); mark=%s", origLog, origOutput, mark))
	return ctxStash(ctx, env, s63KeyLogMark, mark)
}

// s63HoldInWaitingForLag waits for the new run to reach WaitingForLag and
// then stops the target's applier, so the hold no longer depends on the delay.
func s63HoldInWaitingForLag() runner.Step {
	return runner.Step{
		Phase: runner.PhaseInject,
		Name:  "run reaches WaitingForLag, then STOP REPLICA SQL_THREAD on the target",
		Do: func(ctx context.Context, env *runner.Env) error {
			target := ctxFetch(env, s63KeyTarget)
			baseline := ctxFetch(env, s63KeyBaselineRunID)
			waitCtx, cancel := context.WithTimeout(ctx, 3*time.Minute)
			defer cancel()
			mfg, err := env.Wait.UntilCR(waitCtx, env.Namespace, "new planned run reaches WaitingForLag",
				func(mfg *v1alpha1.MysqlFailoverGroup) (bool, string, error) {
					pf := currentPlannedRun(mfg, baseline)
					if pf == nil {
						return false, "no new planned run yet", nil
					}
					msg := plannedRunSummary(pf)
					switch {
					case pf.Phase == v1alpha1.PlannedFailoverPhaseWaitingForLag:
						return true, msg, nil
					case pf.Result != nil || plannedTerminal(pf.Phase):
						return false, msg, fmt.Errorf("planned run ended before WaitingForLag: %s", msg)
					case plannedPastLagWait(pf.Phase):
						return false, msg, fmt.Errorf("planned run passed WaitingForLag despite SOURCE_DELAY=%ds: %s", s63SourceDelay, msg)
					}
					return false, msg, nil
				})
			if err != nil {
				return err
			}
			pf := mfg.Status.PlannedFailover
			if pf.Target != target {
				return fmt.Errorf("planned run target=%q, want %q", pf.Target, target)
			}
			if err := ctxStash(ctx, env, s63KeyRunID, pf.RunID); err != nil {
				return err
			}
			client, err := env.MySQL(target)
			if err != nil {
				return fmt.Errorf("open target %s: %w", target, err)
			}
			if _, err := client.Exec(ctx, "STOP REPLICA SQL_THREAD"); err != nil {
				return fmt.Errorf("stop target SQL thread: %w", err)
			}
			env.Capture.Note(fmt.Sprintf("run %s held in WaitingForLag; SQL thread stopped on %s", pf.RunID, target))
			return nil
		},
	}
}

func s63KillOperator() runner.Step {
	return runner.Step{
		Phase: runner.PhaseInject,
		Name:  "kill the operator pod and wait for its replacement",
		Do: func(ctx context.Context, env *runner.Env) error {
			old, err := operatorPodNames(ctx, env)
			if err != nil {
				return err
			}
			if err := ctxStash(ctx, env, s63KeyOldOperatorPods, strings.Join(old, ",")); err != nil {
				return err
			}
			if err := ctxStash(ctx, env, s63KeyKilledAt, time.Now().UTC().Format(time.RFC3339Nano)); err != nil {
				return err
			}
			if err := env.Chaos.KillOperator(ctx); err != nil {
				return err
			}
			waitCtx, cancel := context.WithTimeout(ctx, 3*time.Minute)
			defer cancel()
			pod, err := waitReplacementOperatorPod(waitCtx, env, old)
			if err != nil {
				return err
			}
			env.Capture.Note(fmt.Sprintf("operator replaced: %v -> %s", old, pod))
			return ctxStash(ctx, env, s63KeyNewOperatorPod, pod)
		},
	}
}

// s63VerifyReplacementDefers tails the replacement operator pod (a fresh
// process: every line it logs is post-restart) for the topology manager's
// planned-failover deferral. That line is the proof the new manager hydrated
// the claim; the status checks that follow cannot tell a hydrated claim from
// a status block the dead operator left behind.
func s63VerifyReplacementDefers() runner.Step {
	return runner.Step{
		Phase: runner.PhaseVerify,
		Name:  "replacement topology manager logs " + plannedDeferredCrossSiteLog,
		Do: func(ctx context.Context, env *runner.Env) error {
			pod := ctxFetch(env, s63KeyNewOperatorPod)
			if pod == "" {
				return fmt.Errorf("stash %s not set", s63KeyNewOperatorPod)
			}
			tailCtx, cancel := context.WithTimeout(ctx, 2*time.Minute)
			defer cancel()
			tail := pglogs.New(pglogs.Source{Namespace: env.Namespace, Pod: pod, Container: "operator"}, 16384)
			tail.Start(tailCtx, env.Kube)
			m, err := env.Wait.UntilLog(tailCtx, tail, time.Time{}, "replacement operator defers cross-site action",
				pglogs.Substring(plannedDeferredCrossSiteLog))
			if err != nil {
				return err
			}
			env.Capture.Note("replacement operator deferred: " + m.Line)
			return nil
		},
	}
}

// s63VerifyHoldSurvivesRestart soaks the hold after the restart. Every poll
// must show the same run in WaitingForLag, and the live servers must show the
// source still fenced and the target still a read-only replica.
func s63VerifyHoldSurvivesRestart() runner.Step {
	return runner.Step{
		Phase: runner.PhaseVerify,
		Name:  "same runId stays WaitingForLag; no emergency promotion; source stays fenced",
		Do: func(ctx context.Context, env *runner.Env) error {
			source := ctxFetch(env, s63KeySource)
			target := ctxFetch(env, s63KeyTarget)
			runID := ctxFetch(env, s63KeyRunID)
			baselineLFT := ctxFetch(env, s63KeyBaselineLFT)
			// Neither MySQL pod restarts in this scenario, so the cached
			// port-forwarded clients stay valid across the operator restart.
			targetClient, err := env.MySQL(target)
			if err != nil {
				return fmt.Errorf("open target %s: %w", target, err)
			}
			sourceClient, err := env.MySQL(source)
			if err != nil {
				return fmt.Errorf("open source %s: %w", source, err)
			}
			deadline := time.Now().Add(s63HoldSoak)
			polls := 0
			for time.Now().Before(deadline) {
				mfg, err := env.Kube.GetMFGNamed(ctx, env.Namespace, env.FG)
				if err != nil {
					return err
				}
				if err := s63HoldViolation(mfg, runID, target, baselineLFT); err != nil {
					return err
				}
				targetRO, err := targetClient.ReadOnly(ctx)
				if err != nil {
					return fmt.Errorf("read target read_only: %w", err)
				}
				if !targetRO {
					return fmt.Errorf("target %s became writable while the run was held (emergency promotion behind the claim)", target)
				}
				sourceSRO, err := sourceClient.SuperReadOnly(ctx)
				if err != nil {
					return fmt.Errorf("read source super_read_only: %w", err)
				}
				if !sourceSRO {
					return fmt.Errorf("source %s was unfenced while the run was held in WaitingForLag", source)
				}
				polls++
				select {
				case <-ctx.Done():
					return ctx.Err()
				case <-time.After(time.Second):
				}
			}
			env.Capture.Note(fmt.Sprintf("hold intact for %s across the restart (%d polls): run %s WaitingForLag, %s fenced, %s read-only",
				s63HoldSoak, polls, runID, source, target))
			return nil
		},
	}
}

// s63HoldViolation checks one status snapshot of the held run. It returns an
// error when the run changed identity or phase, or when the group shows an
// emergency promotion of the target.
func s63HoldViolation(mfg *v1alpha1.MysqlFailoverGroup, runID, target, baselineLastFailoverTarget string) error {
	pf := mfg.Status.PlannedFailover
	if pf == nil {
		return fmt.Errorf("status.plannedFailover vanished while run %s was held", runID)
	}
	if pf.RunID != runID {
		return fmt.Errorf("planned run identity changed across the restart: runId=%q, want %q (%s)", pf.RunID, runID, plannedRunSummary(pf))
	}
	if pf.Phase != v1alpha1.PlannedFailoverPhaseWaitingForLag {
		return fmt.Errorf("planned run %s left WaitingForLag while the target applier was stopped: %s", runID, plannedRunSummary(pf))
	}
	if mfg.Status.ActiveSite == target {
		return fmt.Errorf("status.activeSite=%q while run %s was held: the target was promoted outside the run", target, runID)
	}
	if lft := mfg.Status.LastFailoverTarget; lft != baselineLastFailoverTarget {
		return fmt.Errorf("status.lastFailoverTarget changed %q -> %q while run %s was held", baselineLastFailoverTarget, lft, runID)
	}
	return nil
}

func s63RestartTargetApplier() runner.Step {
	return runner.Step{
		Phase: runner.PhaseInject,
		Name:  "clear SOURCE_DELAY and START REPLICA SQL_THREAD on the target",
		Do: func(ctx context.Context, env *runner.Env) error {
			target := ctxFetch(env, s63KeyTarget)
			client, err := env.MySQL(target)
			if err != nil {
				return fmt.Errorf("open target %s: %w", target, err)
			}
			// SOURCE_DELAY can be changed with only the SQL thread stopped,
			// which it is; the IO thread keeps running throughout.
			for _, q := range []string{
				"CHANGE REPLICATION SOURCE TO SOURCE_DELAY = 0",
				"START REPLICA SQL_THREAD",
			} {
				if _, err := client.Exec(ctx, q); err != nil {
					return fmt.Errorf("%s on %s: %w", q, target, err)
				}
			}
			env.Capture.Note("target applier restarted with SOURCE_DELAY=0")
			return nil
		},
	}
}

func s63ObserveSucceeded() runner.Step {
	return runner.Step{
		Phase: runner.PhaseObserve,
		Name:  "the same run ends Succeeded",
		Do: func(ctx context.Context, env *runner.Env) error {
			target := ctxFetch(env, s63KeyTarget)
			runID := ctxFetch(env, s63KeyRunID)
			waitCtx, cancel := context.WithTimeout(ctx, 3*time.Minute)
			defer cancel()
			_, err := env.Wait.UntilCR(waitCtx, env.Namespace, "run "+runID+" result.outcome==Succeeded",
				func(mfg *v1alpha1.MysqlFailoverGroup) (bool, string, error) {
					pf := mfg.Status.PlannedFailover
					if pf == nil {
						return false, "no plannedFailover status", nil
					}
					msg := plannedRunSummary(pf)
					if pf.RunID != runID {
						return false, msg, fmt.Errorf("planned run replaced before it finished: runId=%q, want %q", pf.RunID, runID)
					}
					if pf.Result == nil {
						if plannedTerminal(pf.Phase) {
							return false, msg, fmt.Errorf("terminal phase %s without result: %s", pf.Phase, msg)
						}
						return false, msg, nil
					}
					if pf.Result.Outcome != v1alpha1.PlannedFailoverOutcomeSucceeded || pf.Phase != v1alpha1.PlannedFailoverPhaseSucceeded {
						return false, msg, fmt.Errorf("planned run ended %s/%s, want Succeeded: %s", pf.Result.Outcome, pf.Result.Reason, pf.Message)
					}
					if pf.Target != target {
						return false, msg, fmt.Errorf("planned run succeeded with target=%q, want %q", pf.Target, target)
					}
					return true, msg, nil
				})
			return err
		},
	}
}

// s63VerifyRunRecords checks the run's identity records against the live
// servers: the fence names the source's server_uuid, the promotion names the
// target's, and they differ.
func s63VerifyRunRecords() runner.Step {
	return runner.Step{
		Phase: runner.PhaseVerify,
		Name:  "fence.sourceServerUUID and promotion.targetServerUUID present and match the servers",
		Do: func(ctx context.Context, env *runner.Env) error {
			source := ctxFetch(env, s63KeySource)
			target := ctxFetch(env, s63KeyTarget)
			mfg, err := env.Kube.GetMFGNamed(ctx, env.Namespace, env.FG)
			if err != nil {
				return err
			}
			pf := mfg.Status.PlannedFailover
			if pf == nil || pf.RunID != ctxFetch(env, s63KeyRunID) {
				return fmt.Errorf("planned run %s no longer in status", ctxFetch(env, s63KeyRunID))
			}
			if pf.Source != source {
				return fmt.Errorf("plannedFailover.source=%q, want %q", pf.Source, source)
			}
			if pf.Fence == nil || pf.Fence.SourceServerUUID == "" {
				return fmt.Errorf("plannedFailover.fence.sourceServerUUID missing: fence=%+v", pf.Fence)
			}
			if pf.Promotion == nil || pf.Promotion.TargetServerUUID == "" {
				return fmt.Errorf("plannedFailover.promotion.targetServerUUID missing: promotion=%+v", pf.Promotion)
			}
			if pf.Fence.SourceServerUUID == pf.Promotion.TargetServerUUID {
				return fmt.Errorf("fence and promotion name the same server %s", pf.Fence.SourceServerUUID)
			}
			for _, want := range []struct{ site, uuid, field string }{
				{source, pf.Fence.SourceServerUUID, "fence.sourceServerUUID"},
				{target, pf.Promotion.TargetServerUUID, "promotion.targetServerUUID"},
			} {
				c, err := env.MySQL(want.site)
				if err != nil {
					return fmt.Errorf("open %s: %w", want.site, err)
				}
				live, err := c.ScalarString(ctx, "SELECT @@GLOBAL.server_uuid")
				if err != nil {
					return fmt.Errorf("read server_uuid on %s: %w", want.site, err)
				}
				if !strings.EqualFold(live, want.uuid) {
					return fmt.Errorf("%s=%s but %s has server_uuid %s", want.field, want.uuid, want.site, live)
				}
			}
			env.Capture.Note(fmt.Sprintf("fence.sourceServerUUID=%s promotion.targetServerUUID=%s", pf.Fence.SourceServerUUID, pf.Promotion.TargetServerUUID))
			return nil
		},
	}
}

func s63VerifyClaimedOnce() runner.Step {
	return runner.Step{
		Phase: runner.PhaseVerify,
		Name:  "exactly one " + plannedClaimedEventReason + " Event for the run",
		Do: func(ctx context.Context, env *runner.Env) error {
			since, err := time.Parse(time.RFC3339Nano, ctxFetch(env, s63KeyAnnotatedAt))
			if err != nil {
				return fmt.Errorf("parse stash %s: %w", s63KeyAnnotatedAt, err)
			}
			events, err := env.Kube.RecentEvents(ctx, env.Namespace, 0)
			if err != nil {
				return err
			}
			// The run is the only one this scenario started, so the window
			// since the annotation scopes the count to it. A second Event
			// (or a recount of the first) would mean the replacement
			// operator re-granted a claim the run already held.
			n := countMFGEvents(events, env.FG, since, plannedClaimedEventReason)
			if n != 1 {
				return fmt.Errorf("%s Events since the annotation: %d, want exactly 1", plannedClaimedEventReason, n)
			}
			return nil
		},
	}
}

// countMFGEvents sums the occurrences of reason on fg's Events at or after
// notBefore. A deduplicated Event counts every occurrence it aggregates.
func countMFGEvents(events []corev1.Event, fg string, notBefore time.Time, reason string) int {
	n := 0
	for _, ev := range events {
		if !matchMFGEvent(ev, fg, notBefore, reason, "") {
			continue
		}
		count := int(ev.Count)
		if ev.Series != nil && int(ev.Series.Count) > count {
			count = int(ev.Series.Count)
		}
		if count < 1 {
			count = 1
		}
		n += count
	}
	return n
}

func s63VerifyOneResetReplicaAll() runner.Step {
	return runner.Step{
		Phase: runner.PhaseVerify,
		Name:  "exactly one RESET REPLICA ALL on the target (general log)",
		Do: func(ctx context.Context, env *runner.Env) error {
			target := ctxFetch(env, s63KeyTarget)
			mark := ctxFetch(env, s63KeyLogMark)
			if mark == "" {
				return fmt.Errorf("stash %s not set", s63KeyLogMark)
			}
			c, err := env.MySQL(target)
			if err != nil {
				return fmt.Errorf("open target %s: %w", target, err)
			}
			// Query and Execute rows carry the statement text; a prepared
			// statement also logs a Prepare row, which is not counted twice.
			n, err := c.ScalarInt(ctx,
				"SELECT COUNT(*) FROM mysql.general_log WHERE event_time >= ? "+
					"AND command_type IN ('Query','Execute') "+
					"AND UPPER(CONVERT(argument USING utf8mb4)) REGEXP ?",
				mark, resetReplicaAllRegexp)
			if err != nil {
				return fmt.Errorf("count RESET REPLICA ALL in the general log on %s: %w", target, err)
			}
			// Stop logging now; cleanup restores the original settings too.
			if err := s63RestoreGeneralLog(ctx, env, c); err != nil {
				env.Capture.Note("restore general log: " + err.Error())
			}
			if n != 1 {
				return fmt.Errorf("RESET REPLICA ALL on target %s since %s: %d, want exactly 1 (the promotion's)", target, mark, n)
			}
			env.Capture.Note("exactly one RESET REPLICA ALL on " + target)
			return nil
		},
	}
}

// s63RestoreGeneralLog puts general_log and log_output back to the stashed
// values. A missing stash means the scenario never changed them.
func s63RestoreGeneralLog(ctx context.Context, env *runner.Env, c *pgmysql.SiteClient) error {
	origLog := ctxFetch(env, s63KeyGeneralLog)
	origOutput := ctxFetch(env, s63KeyLogOutput)
	if origLog == "" || origOutput == "" {
		return nil
	}
	logValue := "OFF"
	if origLog == "1" || strings.EqualFold(origLog, "ON") {
		logValue = "ON"
	}
	var errs []error
	if _, err := c.Exec(ctx, "SET GLOBAL general_log = "+logValue); err != nil {
		errs = append(errs, fmt.Errorf("restore general_log=%s: %w", logValue, err))
	}
	// log_output is a SET of FILE, TABLE and NONE; refuse anything else
	// rather than splice it into the statement.
	if !logOutputValue.MatchString(origOutput) {
		return errors.Join(append(errs, fmt.Errorf("refusing to restore unexpected log_output %q", origOutput))...)
	}
	if _, err := c.Exec(ctx, "SET GLOBAL log_output = '"+origOutput+"'"); err != nil {
		errs = append(errs, fmt.Errorf("restore log_output=%s: %w", origOutput, err))
	}
	return errors.Join(errs...)
}

// s63Cleanup restores the target's applier and general-log settings and drops
// the marker schema. The SOURCE_DELAY is cleared only where it can still be:
// a promoted target has no channel left (RESET REPLICA ALL removed it with
// the delay), and a CHANGE REPLICATION SOURCE there would create an empty
// channel on the new primary.
func s63Cleanup(ctx context.Context, env *runner.Env) error {
	var errs []error
	if target := ctxFetch(env, s63KeyTarget); target != "" {
		c, err := pgmysql.Open(ctx, env.Kube, env.Namespace, env.FG, target, env.Creds)
		if err != nil {
			errs = append(errs, fmt.Errorf("open target %s: %w", target, err))
		} else {
			if rs, err := c.ShowReplicaStatus(ctx); err != nil {
				errs = append(errs, fmt.Errorf("target %s replica status: %w", target, err))
			} else if rs.Configured {
				if err := clearSourceDelay(ctx, c); err != nil {
					errs = append(errs, fmt.Errorf("clear SOURCE_DELAY on %s: %w", target, err))
				}
			}
			if err := s63RestoreGeneralLog(ctx, env, c); err != nil {
				errs = append(errs, err)
			}
			_ = c.Close()
		}
	}
	mfg, err := env.Kube.GetMFGNamed(ctx, env.Namespace, env.FG)
	if err != nil {
		return errors.Join(append(errs, fmt.Errorf("get MFG: %w", err))...)
	}
	for _, s := range mfg.Status.Sites {
		if s.State != "writable" {
			continue
		}
		c, err := pgmysql.Open(ctx, env.Kube, env.Namespace, env.FG, s.Name, env.Creds)
		if err != nil {
			env.Capture.Note(fmt.Sprintf("cleanup: open writable %s: %v", s.Name, err))
			continue
		}
		if _, err := c.Exec(ctx, "DROP DATABASE IF EXISTS "+s63DBName); err != nil {
			env.Capture.Note(fmt.Sprintf("cleanup: drop %s on %s: %v", s63DBName, s.Name, err))
		}
		_ = c.Close()
	}
	return errors.Join(errs...)
}

// --- shared planned-failover helpers (scenarios 63 and 64) -----------------

// currentPlannedRun returns the planned-failover block when it belongs to a
// run started after the baseline was read: a non-empty runId different from
// the one in status before the annotation. Keying on runId instead of
// startTime cannot be fooled by second-granularity timestamps.
func currentPlannedRun(mfg *v1alpha1.MysqlFailoverGroup, baselineRunID string) *v1alpha1.PlannedFailoverStatus {
	pf := mfg.Status.PlannedFailover
	if pf == nil || pf.RunID == "" || pf.RunID == baselineRunID {
		return nil
	}
	return pf
}

// plannedPastLagWait reports whether phase is after WaitingForLag on the
// success path (the Dragonfly phases, Promoting, Publishing).
func plannedPastLagWait(phase v1alpha1.PlannedFailoverPhase) bool {
	switch phase {
	case v1alpha1.PlannedFailoverPhaseWaitingForDragonflySync,
		v1alpha1.PlannedFailoverPhasePromotingDragonfly,
		v1alpha1.PlannedFailoverPhasePromoting,
		v1alpha1.PlannedFailoverPhasePublishing:
		return true
	}
	return false
}

// plannedOwnsSites mirrors the spec's plannedFailoverOwnsSites phase set
// (§2.2): the phases in which a run durably holds the claim.
func plannedOwnsSites(phase v1alpha1.PlannedFailoverPhase) bool {
	switch phase {
	case v1alpha1.PlannedFailoverPhaseFencing,
		v1alpha1.PlannedFailoverPhaseDraining,
		v1alpha1.PlannedFailoverPhaseWaitingForLag,
		v1alpha1.PlannedFailoverPhaseWaitingForDragonflySync,
		v1alpha1.PlannedFailoverPhasePromotingDragonfly,
		v1alpha1.PlannedFailoverPhasePromoting,
		v1alpha1.PlannedFailoverPhaseRollingBack:
		return true
	}
	return false
}

// plannedRunSummary is a one-line description of a run for wait messages.
func plannedRunSummary(pf *v1alpha1.PlannedFailoverStatus) string {
	var b strings.Builder
	fmt.Fprintf(&b, "runId=%q revision=%d phase=%q target=%q source=%q", pf.RunID, pf.Revision, pf.Phase, pf.Target, pf.Source)
	if a := pf.Admission; a != nil {
		fmt.Fprintf(&b, " admission{wait=%q defer=%q}", a.WaitReason, a.DeferReason)
	}
	if r := pf.Result; r != nil {
		fmt.Fprintf(&b, " result{outcome=%q reason=%q warnings=%v}", r.Outcome, r.Reason, r.Warnings)
	}
	return b.String()
}

// operatorPodNames lists the operator pods currently in the namespace.
func operatorPodNames(ctx context.Context, env *runner.Env) ([]string, error) {
	pods, err := env.Kube.Kubernetes.CoreV1().Pods(env.Namespace).List(ctx, metav1.ListOptions{LabelSelector: pgmetrics.OperatorPodSelector})
	if err != nil {
		return nil, fmt.Errorf("list operator pods: %w", err)
	}
	names := make([]string, 0, len(pods.Items))
	for i := range pods.Items {
		names = append(names, pods.Items[i].Name)
	}
	return names, nil
}

// waitReplacementOperatorPod waits for the operator Deployment to be
// Available again and returns a Running, Ready operator pod that is not one
// of the killed ones.
func waitReplacementOperatorPod(ctx context.Context, env *runner.Env, killed []string) (string, error) {
	if err := env.Chaos.WaitForOperatorAvailable(ctx); err != nil {
		return "", err
	}
	for {
		pods, err := env.Kube.Kubernetes.CoreV1().Pods(env.Namespace).List(ctx, metav1.ListOptions{LabelSelector: pgmetrics.OperatorPodSelector})
		if err == nil {
			if name := replacementPod(pods.Items, killed); name != "" {
				return name, nil
			}
		}
		select {
		case <-ctx.Done():
			return "", fmt.Errorf("no replacement operator pod (killed %v) became ready: %w (last list err=%v)", killed, ctx.Err(), err)
		case <-time.After(time.Second):
		}
	}
}

// replacementPod returns the first Running, Ready, not-deleting pod whose
// name is not in killed.
func replacementPod(pods []corev1.Pod, killed []string) string {
	for i := range pods {
		p := &pods[i]
		if stringsContain(killed, p.Name) || p.DeletionTimestamp != nil || p.Status.Phase != corev1.PodRunning {
			continue
		}
		for _, c := range p.Status.Conditions {
			if c.Type == corev1.PodReady && c.Status == corev1.ConditionTrue {
				return p.Name
			}
		}
	}
	return ""
}
