package mysql

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"time"
)

// ReplicaStatus holds key fields from SHOW REPLICA STATUS.
type ReplicaStatus struct {
	IORunning           bool
	SQLRunning          bool
	SecondsBehindSource *int64
	// LastError is the first non-empty of Last_Error, Last_IO_Error and
	// Last_SQL_Error. Last_Error is MySQL's alias for Last_SQL_Error, so this
	// is the applier error when there is one and the receiver error otherwise.
	// It is a summary for status reporting; code that must tell the threads
	// apart reads LastSQLError or LastIOError.
	LastError string
	// LastIOErrno and LastIOError are the receiver (IO) thread's last error.
	// A replica whose source is down reports a connection error here while
	// the relay log it already wrote is intact and still applicable.
	LastIOErrno int
	LastIOError string
	// LastSQLErrno and LastSQLError are the applier's last error: the SQL
	// thread, or on a multithreaded replica the coordinator, which also
	// reports worker failures. A non-empty value means the applier stopped
	// and cannot apply the relay log without intervention.
	LastSQLErrno int
	LastSQLError string
	SourceHost   string
	// SourceUUID is the server_uuid of the source the receiver last
	// connected to (Source_UUID). It changes when the source server is
	// replaced, even under the same host name.
	SourceUUID       string
	ExecutedGtidSet  string
	RetrievedGtidSet string
}

// ReplicationSourceOpts configures CHANGE REPLICATION SOURCE TO.
type ReplicationSourceOpts struct {
	Host     string
	User     string
	Password string
	UseSSL   bool
}

func (m *checker) SetSuperReadOnly(ctx context.Context, on bool) error {
	return setSuperReadOnly(ctx, m.db, on)
}

func (m *checker) KillAppConnections(ctx context.Context) (int, error) {
	rows, err := m.db.QueryContext(ctx,
		`SELECT id FROM information_schema.processlist
		 WHERE id != CONNECTION_ID()
		 AND command NOT IN ('Binlog Dump', 'Binlog Dump GTID')`)
	if err != nil {
		return 0, fmt.Errorf("list app connections: %w", err)
	}
	defer rows.Close()

	var killed int
	for rows.Next() {
		var id int64
		if err := rows.Scan(&id); err != nil {
			continue
		}
		if _, err := m.db.ExecContext(ctx, fmt.Sprintf("KILL %d", id)); err != nil {
			continue
		}
		killed++
	}
	return killed, rows.Err()
}

func (m *checker) StopReplica(ctx context.Context) error {
	return stopReplica(ctx, m.db)
}

func (m *checker) ResetReplicaAll(ctx context.Context) error {
	return resetReplicaAll(ctx, m.db)
}

func (m *checker) SetReadOnly(ctx context.Context, on bool) error {
	return setReadOnly(ctx, m.db, on)
}

func (m *checker) ShowReplicaStatus(ctx context.Context) (*ReplicaStatus, error) {
	rows, err := m.db.QueryContext(ctx, "SHOW REPLICA STATUS")
	if err != nil {
		return nil, fmt.Errorf("show replica status: %w", err)
	}
	defer rows.Close()

	if !rows.Next() {
		// No replication configured.
		return nil, nil
	}

	cols, err := rows.Columns()
	if err != nil {
		return nil, fmt.Errorf("get columns: %w", err)
	}

	// Build a slice of interface{} to scan into. We only care about specific columns.
	values := make([]interface{}, len(cols))
	for i := range values {
		values[i] = new(sql.NullString)
	}

	if err := rows.Scan(values...); err != nil {
		return nil, fmt.Errorf("scan replica status: %w", err)
	}

	// Map column name -> value.
	colMap := make(map[string]sql.NullString, len(cols))
	for i, col := range cols {
		colMap[col] = *values[i].(*sql.NullString)
	}

	rs := &ReplicaStatus{}

	if v, ok := colMap["Replica_IO_Running"]; ok {
		rs.IORunning = v.Valid && v.String == "Yes"
	}
	// Also check the older column name.
	if v, ok := colMap["Slave_IO_Running"]; ok && !rs.IORunning {
		rs.IORunning = v.Valid && v.String == "Yes"
	}

	if v, ok := colMap["Replica_SQL_Running"]; ok {
		rs.SQLRunning = v.Valid && v.String == "Yes"
	}
	if v, ok := colMap["Slave_SQL_Running"]; ok && !rs.SQLRunning {
		rs.SQLRunning = v.Valid && v.String == "Yes"
	}

	if v, ok := colMap["Seconds_Behind_Source"]; ok && v.Valid {
		var secs int64
		if _, err := fmt.Sscanf(v.String, "%d", &secs); err == nil {
			rs.SecondsBehindSource = &secs
		}
	}
	if v, ok := colMap["Seconds_Behind_Master"]; ok && v.Valid && rs.SecondsBehindSource == nil {
		var secs int64
		if _, err := fmt.Sscanf(v.String, "%d", &secs); err == nil {
			rs.SecondsBehindSource = &secs
		}
	}

	if v, ok := colMap["Last_Error"]; ok && v.Valid {
		rs.LastError = v.String
	}
	if v, ok := colMap["Last_IO_Error"]; ok && v.Valid && rs.LastError == "" {
		rs.LastError = v.String
	}
	if v, ok := colMap["Last_SQL_Error"]; ok && v.Valid && rs.LastError == "" {
		rs.LastError = v.String
	}

	rs.LastIOErrno = parseErrno(colMap["Last_IO_Errno"])
	if v, ok := colMap["Last_IO_Error"]; ok && v.Valid {
		rs.LastIOError = v.String
	}
	// Last_Errno/Last_Error alias the applier columns; they are the fallback
	// for a server that does not report Last_SQL_* separately.
	rs.LastSQLErrno = parseErrno(colMap["Last_SQL_Errno"])
	if rs.LastSQLErrno == 0 {
		rs.LastSQLErrno = parseErrno(colMap["Last_Errno"])
	}
	if v, ok := colMap["Last_SQL_Error"]; ok && v.Valid {
		rs.LastSQLError = v.String
	}
	if v, ok := colMap["Last_Error"]; ok && v.Valid && rs.LastSQLError == "" {
		rs.LastSQLError = v.String
	}

	if v, ok := colMap["Source_Host"]; ok && v.Valid {
		rs.SourceHost = v.String
	}
	if v, ok := colMap["Master_Host"]; ok && v.Valid && rs.SourceHost == "" {
		rs.SourceHost = v.String
	}

	if v, ok := colMap["Source_UUID"]; ok && v.Valid {
		rs.SourceUUID = strings.TrimSpace(v.String)
	}
	if v, ok := colMap["Master_UUID"]; ok && v.Valid && rs.SourceUUID == "" {
		rs.SourceUUID = strings.TrimSpace(v.String)
	}

	if v, ok := colMap["Executed_Gtid_Set"]; ok && v.Valid {
		rs.ExecutedGtidSet = v.String
	}

	if v, ok := colMap["Retrieved_Gtid_Set"]; ok && v.Valid {
		rs.RetrievedGtidSet = v.String
	}

	return rs, nil
}

func (m *checker) GetGtidExecuted(ctx context.Context) (string, error) {
	return getGtidExecuted(ctx, m.db)
}

func (m *checker) HasUserSchemas(ctx context.Context) (bool, error) {
	var exists int
	err := m.db.QueryRowContext(ctx, `
		SELECT EXISTS(
			SELECT 1
			FROM information_schema.schemata
			WHERE schema_name NOT IN ('information_schema', 'mysql', 'performance_schema', 'sys')
			LIMIT 1
		)
	`).Scan(&exists)
	if err != nil {
		return false, fmt.Errorf("query user schemas: %w", err)
	}
	return exists == 1, nil
}

// ReplicationSourceCAFile is the CA bundle a replica's mysqld uses to verify
// its source's server certificate on TLS replication channels. It is the
// path where the operator mounts spec.tls.secretName into every mysql
// container (the controller's mysqlTLSMountPath), so it exists on the
// replica whenever UseSSL is set.
const ReplicationSourceCAFile = "/etc/mysql/tls/ca.crt"

func (m *checker) ChangeReplicationSource(ctx context.Context, opts ReplicationSourceOpts) error {
	if _, err := m.db.ExecContext(ctx, changeReplicationSourceSQL(opts)); err != nil {
		return fmt.Errorf("change replication source: %w", err)
	}
	return nil
}

func changeReplicationSourceSQL(opts ReplicationSourceOpts) string {
	q := fmt.Sprintf(
		"CHANGE REPLICATION SOURCE TO SOURCE_HOST='%s', SOURCE_USER='%s', SOURCE_PASSWORD='%s', SOURCE_AUTO_POSITION=1",
		escapeSingleQuotes(opts.Host), escapeSingleQuotes(opts.User), escapeSingleQuotes(opts.Password),
	)
	if opts.UseSSL {
		// SOURCE_SSL_CA makes the channel VERIFY_CA: the source's
		// certificate must chain to the group's CA. Host name verification
		// (SOURCE_SSL_VERIFY_SERVER_CERT) is not enabled because replicas
		// dial the -internal per-site Service, which the documented
		// certificate SANs do not include.
		q += ", SOURCE_SSL=1, SOURCE_SSL_CA='" + ReplicationSourceCAFile + "'"
	} else {
		// MySQL 8's default caching_sha2_password authentication needs
		// the source's RSA public key for non-TLS replication channels.
		// Without this, START REPLICA succeeds but the IO thread exits
		// asynchronously, leaving the site permanently not-replicating.
		q += ", GET_SOURCE_PUBLIC_KEY=1"
	}
	return q
}

func (m *checker) StartReplica(ctx context.Context) error {
	_, err := m.db.ExecContext(ctx, "START REPLICA")
	if err != nil {
		return fmt.Errorf("start replica: %w", err)
	}
	return nil
}

func (m *checker) startReplicaSQLThread(ctx context.Context) error {
	_, err := m.db.ExecContext(ctx, "START REPLICA SQL_THREAD")
	if err != nil {
		return fmt.Errorf("start replica sql_thread: %w", err)
	}
	return nil
}

// WaitForRelayLogDrain waits, up to timeout, until the replica has applied
// every transaction its receiver had already written to the relay log when
// the drain began, so a following STOP REPLICA / RESET REPLICA ALL discards
// nothing that reached this replica.
//
// Completion is GTID-based: the Retrieved_Gtid_Set seen on the first poll is
// the target, and the drain is complete once Executed_Gtid_Set contains it
// (or contains the current Retrieved_Gtid_Set). Seconds_Behind_Source is not
// used: it has one-second granularity, measures the age of the event being
// applied rather than the queue depth, and is NULL whenever the IO thread is
// down. Fixing the target at the first poll keeps the drain finite when the
// IO thread is still connected and receiving (an operator-only partition).
//
// Only an applier error aborts the drain. A receiver (IO thread) error is the
// normal state of a candidate whose primary has just died, and the relay log
// it already wrote is still applicable. A stopped, error-free SQL thread with
// pending relay log is restarted once.
func (m *checker) WaitForRelayLogDrain(ctx context.Context, timeout time.Duration) error {
	deadline := time.Now().Add(timeout)
	interval := 500 * time.Millisecond
	const maxInterval = 4 * time.Second
	sqlThreadRestarted := false
	var target GTIDSet
	haveTarget := false

	timer := time.NewTimer(interval)
	defer timer.Stop()

	for {
		rs, err := m.ShowReplicaStatus(ctx)
		if err != nil {
			return fmt.Errorf("wait for relay log drain: %w", err)
		}
		if rs == nil {
			return nil
		}

		if rs.LastSQLError != "" {
			return fmt.Errorf("relay log drain aborted: SQL thread error: %s",
				formatReplicationError(rs.LastSQLErrno, rs.LastSQLError))
		}

		retrieved, executed, err := parseRelayLogSets(rs)
		if err != nil {
			return fmt.Errorf("relay log drain: check pending relay logs: %w", err)
		}
		if !haveTarget {
			target, haveTarget = retrieved, true
		}
		pending := unappliedRelayLog(target, retrieved, executed)
		if pending.IsEmpty() {
			return nil
		}

		if !rs.SQLRunning && !sqlThreadRestarted {
			// Pending relay logs and a stopped, error-free SQL thread:
			// restart it to apply them.
			if startErr := m.startReplicaSQLThread(ctx); startErr != nil {
				// Connection may be stale after failover turbulence.
				// Force a reconnect via Ping and retry once.
				if pingErr := m.db.PingContext(ctx); pingErr != nil {
					return fmt.Errorf("relay log drain: restart SQL thread: %w (ping also failed: %v)", startErr, pingErr)
				}
				if retryErr := m.startReplicaSQLThread(ctx); retryErr != nil {
					return fmt.Errorf("relay log drain: restart SQL thread after reconnect: %w", retryErr)
				}
			}
			sqlThreadRestarted = true
		}

		if time.Now().After(deadline) {
			msg := fmt.Sprintf("relay log drain timed out after %s with %d relay log transactions unapplied (%s)",
				timeout, pending.TransactionCount(), pending)
			if rs.LastIOError != "" {
				msg += "; IO thread error: " + formatReplicationError(rs.LastIOErrno, rs.LastIOError)
			}
			return errors.New(msg)
		}

		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-timer.C:
		}

		if interval < maxInterval {
			interval *= 2
		}
		timer.Reset(interval)
	}
}

func parseRelayLogSets(rs *ReplicaStatus) (retrieved, executed GTIDSet, err error) {
	retrieved, err = ParseGTIDSet(rs.RetrievedGtidSet)
	if err != nil {
		return nil, nil, fmt.Errorf("parse retrieved GTID set: %w", err)
	}
	executed, err = ParseGTIDSet(rs.ExecutedGtidSet)
	if err != nil {
		return nil, nil, fmt.Errorf("parse executed GTID set: %w", err)
	}
	return retrieved, executed, nil
}

// unappliedRelayLog returns the transactions of the drain target the replica
// has not executed yet. It is empty once the target is executed, and also
// once everything currently retrieved is executed, which covers a retrieved
// set that shrank under the drain (a relay log reset).
func unappliedRelayLog(target, retrieved, executed GTIDSet) GTIDSet {
	if executed.Contains(retrieved) {
		return GTIDSet{}
	}
	return target.Subtract(executed)
}

func formatReplicationError(errno int, msg string) string {
	if errno == 0 {
		return msg
	}
	return fmt.Sprintf("%d: %s", errno, msg)
}

func parseErrno(v sql.NullString) int {
	if !v.Valid {
		return 0
	}
	n, err := strconv.Atoi(strings.TrimSpace(v.String))
	if err != nil {
		return 0
	}
	return n
}
