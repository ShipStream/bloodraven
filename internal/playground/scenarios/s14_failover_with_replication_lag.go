package scenarios

import (
	"context"
	"fmt"
	"time"

	"github.com/shipstream/bloodraven/internal/playground/runner"
)

func init() {
	runner.Register(scenario14FailoverWithReplicationLag())
}

const (
	s14DBName        = "chaos_s14"
	s14TableName     = "lag_test"
	s14RowCount      = 25
	s14WriteDuration = 5 * time.Second
)

// scenario14FailoverWithReplicationLag quantifies the relay-log drain
// behavior under async-replication lag. Procedure:
//
//  1. Pick the active primary and current read-only replica.
//  2. `STOP REPLICA SQL_THREAD` on the replica. The IO thread keeps
//     fetching from the primary so writes land in the relay log, but
//     the SQL applier is paused so they're not yet applied.
//  3. Write s14RowCount rows on the primary spread over s14WriteDuration.
//     Capture the resulting gtid_executed.
//  4. Verify the replica has the relay logs (Retrieved_Gtid_Set
//     contains the post-write GTID set) but has NOT yet applied them
//     (row count on the replica is below s14RowCount).
//  5. Scale the primary deployment to 0 — the relay logs are intact on
//     the replica, only the source is unavailable.
//  6. Wait for activeSite to flip and the operator to complete failover.
//  7. Assert on the new primary: GTID_SUBSET(post-write, gtid_executed)
//     == 1 AND COUNT(*) == s14RowCount. The relay-log drain phase of
//     promotion must have applied every transaction in the relay log
//     before flipping super_read_only off.
//
// We use `STOP REPLICA SQL_THREAD` (the IO thread keeps running) rather
// than the `SOURCE_DELAY` variant because the operator's checkRecovery
// path actively repairs broken replication. Specifically: when both
// IO and SQL threads stop on a read-only site, `RecoverOldPrimary`
// runs `RESET REPLICA ALL` and re-establishes replication, which would
// wipe any SOURCE_DELAY we set. With `STOP REPLICA SQL_THREAD` the IO
// thread stays running, so the operator's `IORunning || SQLRunning`
// idle check stays satisfied and it leaves us alone.
//
// Cleanup restarts the SQL thread on whichever site ends up read-only
// (auto-fail-back can make it the originally-primary site rather than
// the originally-replica site).
func scenario14FailoverWithReplicationLag() runner.Scenario {
	return runner.Scenario{
		ID:    "14-failover-with-replication-lag",
		Title: "Failover with replication lag — relay-log drain recovers all rows",
		Hypothesis: "With SOURCE_DELAY=10 on the replica, writes to the primary land in the replica's " +
			"relay log but are not yet applied. Scaling the primary to 0 forces failover; the relay-log " +
			"drain on the new primary applies every transaction so GTID_SUBSET(pre, post)=1 and the row " +
			"count is fully preserved.",
		Risk:     "medium",
		DocLink:  "playground/chaos-scenarios.md#14-failover-with-replication-lag-async-data-loss-window",
		Timeout:  6 * time.Minute,
		Precheck: assertReplicationRunningPrecheck,
		Steps: []runner.Step{
			s14InjectLagAndSeed(),
			observeFailover(),
			s14VerifyDrainRecoveredAll(),
		},
		Cleanup: s14ResetReplicationDelay,
	}
}

func s14InjectLagAndSeed() runner.Step {
	return runner.Step{
		Phase: runner.PhaseInject,
		Name:  "stop replica SQL_THREAD, write rows on primary, then scale primary to 0",
		Do: func(ctx context.Context, env *runner.Env) error {
			mfg, err := env.Kube.GetMFG(ctx, env.Namespace)
			if err != nil {
				return err
			}
			active := mfg.Status.ActiveSite
			replica, err := PeerOf(mfg, active)
			if err != nil {
				return err
			}
			env.Capture.Note(fmt.Sprintf("active=%s replica=%s; pausing replica SQL applier", active, replica))
			if err := ctxStash(ctx, env, "originalPrimary", active); err != nil {
				return err
			}
			if err := ctxStash(ctx, env, "originalReplica", replica); err != nil {
				return err
			}

			replicaClient, err := env.MySQL(replica)
			if err != nil {
				return fmt.Errorf("open replica mysql: %w", err)
			}
			// STOP REPLICA SQL_THREAD: stops only the applier, IO
			// continues fetching. The operator's checkRecovery condition
			// is `IORunning || SQLRunning`, so leaving IO running keeps
			// the operator from running RESET REPLICA ALL on us.
			if _, err := replicaClient.Exec(ctx, "STOP REPLICA SQL_THREAD"); err != nil {
				return fmt.Errorf("stop replica SQL_THREAD on %s: %w", replica, err)
			}

			primary, err := env.MySQL(active)
			if err != nil {
				return fmt.Errorf("open primary mysql: %w", err)
			}
			schemaStmts := []string{
				fmt.Sprintf("CREATE DATABASE IF NOT EXISTS %s", s14DBName),
				fmt.Sprintf("DROP TABLE IF EXISTS %s.%s", s14DBName, s14TableName),
				fmt.Sprintf("CREATE TABLE %s.%s (id INT PRIMARY KEY, payload VARCHAR(64), ts TIMESTAMP DEFAULT CURRENT_TIMESTAMP)",
					s14DBName, s14TableName),
			}
			for _, q := range schemaStmts {
				if _, err := primary.Exec(ctx, q); err != nil {
					return fmt.Errorf("schema setup %q: %w", q, err)
				}
			}
			// Spread writes across s14WriteDuration so the IO thread has
			// time to ferry them into the relay log. Without the spread,
			// fast writes followed by an immediate kill could outrace
			// the IO thread.
			pause := s14WriteDuration / time.Duration(s14RowCount)
			insertSQL := fmt.Sprintf("INSERT INTO %s.%s (id, payload) VALUES (?, ?)", s14DBName, s14TableName)
			writeStart := time.Now()
			for i := 1; i <= s14RowCount; i++ {
				if _, err := primary.Exec(ctx, insertSQL, i, fmt.Sprintf("row-%d", i)); err != nil {
					return fmt.Errorf("insert row %d: %w", i, err)
				}
				if pause > 0 && i < s14RowCount {
					select {
					case <-ctx.Done():
						return ctx.Err()
					case <-time.After(pause):
					}
				}
			}
			env.Capture.Note(fmt.Sprintf("seeded %d rows in %s", s14RowCount, time.Since(writeStart).Round(time.Millisecond)))

			postGtid, err := primary.GtidExecuted(ctx)
			if err != nil {
				return fmt.Errorf("read primary gtid_executed: %w", err)
			}
			env.Capture.Note(fmt.Sprintf("post-write gtid_executed=%q", postGtid))
			if err := ctxStash(ctx, env, "postWriteGtid", postGtid); err != nil {
				return err
			}

			// Verify the lag built up before killing the source. Without
			// this hard precondition the scenario could pass after testing
			// an already-caught-up replica instead of relay-log drain.
			rs, err := replicaClient.ShowReplicaStatus(ctx)
			if err != nil {
				return fmt.Errorf("show replica status on %s: %w", replica, err)
			}
			env.Capture.Note(fmt.Sprintf("replica retrieved=%q executed=%q running=io:%v sql:%v",
				rs.RetrievedGtidSet, rs.ExecutedGtidSet, rs.IORunning, rs.SQLRunning))
			if !rs.Configured {
				return fmt.Errorf("replica %s has no replication configured", replica)
			}
			if !rs.IORunning || rs.SQLRunning {
				return fmt.Errorf("lag precondition not met on %s: want IO running and SQL stopped, got io=%v sql=%v",
					replica, rs.IORunning, rs.SQLRunning)
			}
			subset, err := replicaClient.ScalarInt(ctx, "SELECT GTID_SUBSET(?, ?)", postGtid, rs.RetrievedGtidSet)
			if err != nil {
				return fmt.Errorf("verify retrieved GTID set on %s: %w", replica, err)
			}
			if subset != 1 {
				return fmt.Errorf("replica %s has not fetched all post-write GTIDs: GTID_SUBSET(post, retrieved)=%d post=%q retrieved=%q",
					replica, subset, postGtid, rs.RetrievedGtidSet)
			}
			rowsApplied, err := replicaClient.ScalarInt(ctx, fmt.Sprintf("SELECT COUNT(*) FROM %s.%s", s14DBName, s14TableName))
			if err != nil {
				// If the CREATE TABLE DDL is still only in the relay log,
				// the read fails. That is valid evidence that the applier
				// has not caught up.
				env.Capture.Note(fmt.Sprintf("replica row count pre-kill unavailable, treating as 0 applied: %v", err))
				rowsApplied = 0
			}
			if rowsApplied >= s14RowCount {
				return fmt.Errorf("lag precondition not met on %s: replica already applied %d/%d rows",
					replica, rowsApplied, s14RowCount)
			}
			env.Capture.Note(fmt.Sprintf("lag precondition verified: replica row count pre-kill %d/%d", rowsApplied, s14RowCount))

			env.Capture.Note(fmt.Sprintf("scaling primary %s to 0", active))
			return env.Chaos.ScaleSiteToZero(ctx, active)
		},
	}
}

func s14VerifyDrainRecoveredAll() runner.Step {
	return runner.Step{
		Phase: runner.PhaseVerify,
		Name:  "new primary preserves every seeded row (relay-log drain ran to completion)",
		Do: func(ctx context.Context, env *runner.Env) error {
			postGtid := ctxFetch(env, "postWriteGtid")
			if postGtid == "" {
				return fmt.Errorf("postWriteGtid not stashed")
			}
			mfg, err := env.Kube.GetMFG(ctx, env.Namespace)
			if err != nil {
				return err
			}
			var newPrimary string
			for _, s := range mfg.Status.Sites {
				if s.State == "writable" {
					newPrimary = s.Name
					break
				}
			}
			if newPrimary == "" {
				return fmt.Errorf("no writable site at verify time (sites=%+v)", mfg.Status.Sites)
			}
			env.Capture.Note(fmt.Sprintf("verifying drain on new primary: %s", newPrimary))
			client, err := env.MySQL(newPrimary)
			if err != nil {
				return fmt.Errorf("open new primary %s: %w", newPrimary, err)
			}
			subset, err := client.ScalarInt(ctx, "SELECT GTID_SUBSET(?, @@global.gtid_executed)", postGtid)
			if err != nil {
				return fmt.Errorf("GTID_SUBSET on %s: %w", newPrimary, err)
			}
			if subset != 1 {
				gotGtid, _ := client.GtidExecuted(ctx)
				return fmt.Errorf("relay-log drain did not recover lagged transactions: GTID_SUBSET(post, current)=%d post=%q current=%q",
					subset, postGtid, gotGtid)
			}
			count, err := client.ScalarInt(ctx, fmt.Sprintf("SELECT COUNT(*) FROM %s.%s", s14DBName, s14TableName))
			if err != nil {
				return fmt.Errorf("count rows: %w", err)
			}
			if count != s14RowCount {
				return fmt.Errorf("row count mismatch on %s: got %d, want %d", newPrimary, count, s14RowCount)
			}
			env.Capture.Note(fmt.Sprintf("new primary %s preserved %d/%d rows; GTID_SUBSET=1", newPrimary, count, s14RowCount))
			return nil
		},
	}
}

// s14RestartSQLThreadAndDropSchema starts the replica SQL thread on
// whichever site is currently read-only (in case auto-fail-back made
// the originally-primary site the new replica), and drops the test
// schema from the writable site. Runs as the scenario's Cleanup hook
// AFTER chaos.Revert (which scales the original primary back up) but
// BEFORE GlobalRecover.
//
// Best-effort: if either site is mid-recovery and we can't open a
// connection, we skip and let the next reset handle it. Failing here
// would obscure the actual scenario result.
func s14ResetReplicationDelay(ctx context.Context, env *runner.Env) error {
	mfg, err := env.Kube.GetMFG(ctx, env.Namespace)
	if err != nil {
		return fmt.Errorf("cleanup: get MFG: %w", err)
	}
	for _, s := range mfg.Status.Sites {
		if s.State != "read-only" {
			continue
		}
		client, err := env.MySQL(s.Name)
		if err != nil {
			env.Capture.Note(fmt.Sprintf("cleanup: skip SQL_THREAD restart on %s: %v", s.Name, err))
			continue
		}
		// START is idempotent: if the SQL thread is already running, MySQL
		// returns an error code we ignore. The operator may also resume
		// it via the recovery path; this is just a safety net.
		if _, err := client.Exec(ctx, "START REPLICA SQL_THREAD"); err != nil {
			env.Capture.Note(fmt.Sprintf("cleanup: START REPLICA SQL_THREAD on %s: %v", s.Name, err))
		}
	}
	for _, s := range mfg.Status.Sites {
		if s.State != "writable" {
			continue
		}
		client, err := env.MySQL(s.Name)
		if err != nil {
			env.Capture.Note(fmt.Sprintf("cleanup: skip schema drop on %s: %v", s.Name, err))
			continue
		}
		if _, err := client.Exec(ctx, fmt.Sprintf("DROP DATABASE IF EXISTS %s", s14DBName)); err != nil {
			env.Capture.Note(fmt.Sprintf("cleanup: drop database %s on %s: %v", s14DBName, s.Name, err))
		}
	}
	return nil
}
