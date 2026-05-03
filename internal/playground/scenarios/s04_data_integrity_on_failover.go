package scenarios

import (
	"context"
	"fmt"
	"time"

	v1alpha1 "github.com/shipstream/bloodraven/api/v1alpha1"
	pgkube "github.com/shipstream/bloodraven/internal/playground/kube"
	pgmysql "github.com/shipstream/bloodraven/internal/playground/mysql"
	"github.com/shipstream/bloodraven/internal/playground/runner"
)

func init() {
	runner.Register(scenario04DataIntegrityOnFailover())
}

const (
	s04DBName    = "chaos_s04"
	s04TableName = "marker"
	s04RowCount  = 100
)

// scenario04DataIntegrityOnFailover writes a known set of marker rows
// on the primary, waits for the replica to catch up via
// WAIT_FOR_EXECUTED_GTID_SET, captures the primary's gtid_executed,
// and then scales the primary to 0. Post-failover it asserts on the
// new primary that GTID_SUBSET(pre-failover, @@gtid_executed)=1 and
// the marker row count matches what we wrote.
//
// This is the data-integrity counterpart to scenario 01: 01 verifies
// the failover signals (activeSite flip, metric, log line); 04
// verifies that no committed and replicated transaction is lost
// across that failover. Replication-caught-up gating in the inject is
// load-bearing — without it, an async lag would manifest here as a
// "real" data loss bug.
func scenario04DataIntegrityOnFailover() runner.Scenario {
	return runner.Scenario{
		ID:    "04-data-integrity-on-failover",
		Title: "Data integrity preserved across emergency failover",
		Hypothesis: "Marker rows committed on the primary and replicated to the standby BEFORE a kill survive " +
			"failover with no GTID gap (GTID_SUBSET(pre, post)=1) and full row count.",
		Risk:     "low",
		DocLink:  "playground/chaos-scenarios.md#4-data-integrity-under-failover",
		Timeout:  4 * time.Minute,
		Precheck: assertReplicationRunningPrecheck,
		Steps: []runner.Step{
			s04InjectSeedAndKill(),
			observeFailover(),
			s04VerifyDataIntegrity(),
		},
		Cleanup: s04DropMarkerSchema,
	}
}

// assertReplicationRunningPrecheck wraps AssertHealthyBaseline with a
// live replication-thread check on the read-only site. If the SQL or
// IO thread is stopped, the inject's WAIT_FOR_EXECUTED_GTID_SET would
// time out anyway — but precheck failure is a clearer signal than a
// 30s wait inside the inject step, and points the operator at
// reset-mysql.sh.
func assertReplicationRunningPrecheck(ctx context.Context, env *runner.Env) error {
	if err := AssertHealthyBaseline(ctx, env); err != nil {
		return err
	}
	mfg, err := env.Kube.GetMFGNamed(ctx, env.Namespace, env.FG)
	if err != nil {
		return fmt.Errorf("precheck: get MFG: %w", err)
	}
	var replica string
	for _, s := range mfg.Status.Sites {
		if s.State == "read-only" {
			replica = s.Name
			break
		}
	}
	if replica == "" {
		return fmt.Errorf("precheck: no read-only site present (sites=%+v)", mfg.Status.Sites)
	}
	client, err := env.MySQL(replica)
	if err != nil {
		return fmt.Errorf("precheck: open replica mysql: %w", err)
	}
	rs, err := client.ShowReplicaStatus(ctx)
	if err != nil {
		return fmt.Errorf("precheck: show replica status on %s: %w", replica, err)
	}
	if !rs.Configured || !rs.IORunning || !rs.SQLRunning {
		return fmt.Errorf("precheck: replica %s replication not running (configured=%v io=%v sql=%v lastIO=%q lastSQL=%q) — run ./playground/reset-mysql.sh",
			replica, rs.Configured, rs.IORunning, rs.SQLRunning, rs.LastIOError, rs.LastSQLError)
	}
	return nil
}

func s04InjectSeedAndKill() runner.Step {
	return runner.Step{
		Phase: runner.PhaseInject,
		Name:  "seed marker rows on primary, wait for replica catch-up, scale primary to 0",
		Do: func(ctx context.Context, env *runner.Env) error {
			mfg, err := env.Kube.GetMFGNamed(ctx, env.Namespace, env.FG)
			if err != nil {
				return err
			}
			active := mfg.Status.ActiveSite
			replica, err := PeerOf(mfg, active)
			if err != nil {
				return err
			}
			env.Capture.Note(fmt.Sprintf("active=%s replica=%s; seeding %d marker rows", active, replica, s04RowCount))
			if err := ctxStash(ctx, env, "originalPrimary", active); err != nil {
				return err
			}

			primary, err := env.MySQL(active)
			if err != nil {
				return fmt.Errorf("open primary mysql: %w", err)
			}
			// Idempotent setup so a re-run after a previous abort doesn't
			// trip on a stale table from the previous attempt.
			schemaStmts := []string{
				fmt.Sprintf("CREATE DATABASE IF NOT EXISTS %s", s04DBName),
				fmt.Sprintf("DROP TABLE IF EXISTS %s.%s", s04DBName, s04TableName),
				fmt.Sprintf("CREATE TABLE %s.%s (id INT PRIMARY KEY, payload VARCHAR(64), ts TIMESTAMP DEFAULT CURRENT_TIMESTAMP)", s04DBName, s04TableName),
			}
			for _, q := range schemaStmts {
				if _, err := primary.Exec(ctx, q); err != nil {
					return fmt.Errorf("schema setup %q: %w", q, err)
				}
			}
			insertSQL := fmt.Sprintf("INSERT INTO %s.%s (id, payload) VALUES (?, ?)", s04DBName, s04TableName)
			for i := 1; i <= s04RowCount; i++ {
				if _, err := primary.Exec(ctx, insertSQL, i, fmt.Sprintf("row-%d", i)); err != nil {
					return fmt.Errorf("insert row %d: %w", i, err)
				}
			}

			preGtid, err := primary.GtidExecuted(ctx)
			if err != nil {
				return fmt.Errorf("read primary gtid_executed: %w", err)
			}
			env.Capture.Note(fmt.Sprintf("pre-failover gtid_executed=%q", preGtid))
			if err := ctxStash(ctx, env, "preFailoverGtid", preGtid); err != nil {
				return err
			}

			// Block until the replica has applied every transaction in the
			// pre-failover GTID set. Without this gate, async replication
			// lag would show up as a phantom data-loss assertion failure.
			replicaClient, err := env.MySQL(replica)
			if err != nil {
				return fmt.Errorf("open replica mysql: %w", err)
			}
			waitCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
			defer cancel()
			rc, err := replicaClient.ScalarInt(waitCtx, "SELECT WAIT_FOR_EXECUTED_GTID_SET(?, 30)", preGtid)
			if err != nil {
				return fmt.Errorf("WAIT_FOR_EXECUTED_GTID_SET on replica %s: %w", replica, err)
			}
			if rc != 0 {
				return fmt.Errorf("replica %s did not catch up to pre-failover gtid within 30s (rc=%d gtid=%q)", replica, rc, preGtid)
			}
			env.Capture.Note(fmt.Sprintf("replica %s caught up; scaling primary %s to 0", replica, active))
			return env.Chaos.ScaleSiteToZero(ctx, active)
		},
	}
}

func s04VerifyDataIntegrity() runner.Step {
	return runner.Step{
		Phase: runner.PhaseVerify,
		Name:  "new primary preserves pre-failover GTID set and marker row count",
		Do: func(ctx context.Context, env *runner.Env) error {
			preGtid := ctxFetch(env, "preFailoverGtid")
			if preGtid == "" {
				return fmt.Errorf("preFailoverGtid not stashed")
			}
			originalPrimary := ctxFetch(env, "originalPrimary")
			mfg, err := env.Kube.GetMFGNamed(ctx, env.Namespace, env.FG)
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
			if newPrimary == originalPrimary {
				return fmt.Errorf("active site did not flip: newPrimary=%q == originalPrimary=%q", newPrimary, originalPrimary)
			}
			env.Capture.Note(fmt.Sprintf("verifying integrity on new primary: %s", newPrimary))

			client, err := env.MySQL(newPrimary)
			if err != nil {
				return fmt.Errorf("open new primary %s: %w", newPrimary, err)
			}
			subset, err := client.ScalarInt(ctx, "SELECT GTID_SUBSET(?, @@global.gtid_executed)", preGtid)
			if err != nil {
				return fmt.Errorf("GTID_SUBSET on %s: %w", newPrimary, err)
			}
			if subset != 1 {
				postGtid, _ := client.GtidExecuted(ctx)
				return fmt.Errorf("new primary %s missing pre-failover transactions: GTID_SUBSET(pre, post)=%d pre=%q post=%q",
					newPrimary, subset, preGtid, postGtid)
			}
			count, err := client.ScalarInt(ctx, fmt.Sprintf("SELECT COUNT(*) FROM %s.%s", s04DBName, s04TableName))
			if err != nil {
				return fmt.Errorf("count marker rows on %s: %w", newPrimary, err)
			}
			if count != s04RowCount {
				return fmt.Errorf("row count mismatch on new primary %s: got %d, want %d", newPrimary, count, s04RowCount)
			}
			env.Capture.Note(fmt.Sprintf("new primary %s: %d/%d rows preserved, GTID_SUBSET=1", newPrimary, count, s04RowCount))
			return nil
		},
	}
}

// s04DropMarkerSchema runs as the scenario.Cleanup hook after
// Chaos.Revert scales the original primary back up. Wait for old-primary
// recovery first: dropping the schema on the promoted primary while the
// old primary is still offline creates a new GTID that the returning old
// primary lacks, which turns a cleanup step into RecoveryBlocked state.
func s04DropMarkerSchema(ctx context.Context, env *runner.Env) error {
	waitCtx, cancel := context.WithTimeout(ctx, 3*time.Minute)
	defer cancel()
	mfg, err := waitForS04CleanupBaseline(waitCtx, env)
	if err != nil {
		return err
	}
	writable := mfg.Status.ActiveSite
	replica, err := PeerOf(mfg, writable)
	if err != nil {
		return fmt.Errorf("cleanup: resolve replica for %s: %w", writable, err)
	}

	primary, err := pgmysql.Open(ctx, env.Kube, env.Namespace, env.FG, writable, env.Creds)
	if err != nil {
		return fmt.Errorf("cleanup: open %s: %w", writable, err)
	}
	defer primary.Close()
	if _, err := primary.Exec(ctx, fmt.Sprintf("DROP DATABASE IF EXISTS %s", s04DBName)); err != nil {
		return fmt.Errorf("cleanup: drop database %s on %s: %w", s04DBName, writable, err)
	}
	gtid, err := primary.GtidExecuted(ctx)
	if err != nil {
		return fmt.Errorf("cleanup: read post-drop gtid on %s: %w", writable, err)
	}
	replicaClient, err := pgmysql.Open(ctx, env.Kube, env.Namespace, env.FG, replica, env.Creds)
	if err != nil {
		return fmt.Errorf("cleanup: open replica %s: %w", replica, err)
	}
	defer replicaClient.Close()
	if rc, err := replicaClient.ScalarInt(waitCtx, "SELECT WAIT_FOR_EXECUTED_GTID_SET(?, 30)", gtid); err != nil {
		return fmt.Errorf("cleanup: wait for schema drop on replica %s: %w", replica, err)
	} else if rc != 0 {
		return fmt.Errorf("cleanup: replica %s did not apply schema drop gtid within 30s (rc=%d gtid=%q)", replica, rc, gtid)
	}
	env.Capture.Note(fmt.Sprintf("cleanup: dropped %s on %s and replicated to %s", s04DBName, writable, replica))
	return nil
}

func waitForS04CleanupBaseline(ctx context.Context, env *runner.Env) (*v1alpha1.MysqlFailoverGroup, error) {
	return env.Wait.UntilCR(ctx, env.Namespace, "s04 cleanup waits for old-primary recovery before schema drop",
		func(mfg *v1alpha1.MysqlFailoverGroup) (bool, string, error) {
			ready := pgkube.ReadyCondition(mfg) == "True"
			var bad []string
			for _, s := range mfg.Status.Sites {
				if s.State != "writable" && s.State != "read-only" {
					bad = append(bad, fmt.Sprintf("%s=%s", s.Name, s.State))
				}
				if s.RecoveryState == "RecoveryBlocked" {
					bad = append(bad, fmt.Sprintf("%s=blocked", s.Name))
				}
				if s.Name != mfg.Status.ActiveSite && isPromotableSite(mfg, s.Name) && !s.Replicating {
					bad = append(bad, fmt.Sprintf("%s=not-replicating", s.Name))
				}
			}
			return ready && mfg.Status.ActiveSite != "" && len(bad) == 0,
				fmt.Sprintf("ready=%v active=%q bad=%v", ready, mfg.Status.ActiveSite, bad), nil
		},
	)
}
