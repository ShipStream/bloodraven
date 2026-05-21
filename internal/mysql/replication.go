package mysql

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
	"time"
)

// ReplicaStatus holds key fields from SHOW REPLICA STATUS.
type ReplicaStatus struct {
	IORunning           bool
	SQLRunning          bool
	SecondsBehindSource *int64
	LastError           string
	SourceHost          string
	ExecutedGtidSet     string
	RetrievedGtidSet    string
}

// ReplicationSourceOpts configures CHANGE REPLICATION SOURCE TO.
type ReplicationSourceOpts struct {
	Host     string
	User     string
	Password string
	UseSSL   bool
}

func (m *checker) SetSuperReadOnly(ctx context.Context, on bool) error {
	val := "OFF"
	if on {
		val = "ON"
	}
	_, err := m.db.ExecContext(ctx, "SET GLOBAL super_read_only = "+val)
	if err != nil {
		return fmt.Errorf("set super_read_only=%s: %w", val, err)
	}
	return nil
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
	_, err := m.db.ExecContext(ctx, "STOP REPLICA")
	if err != nil {
		return fmt.Errorf("stop replica: %w", err)
	}
	return nil
}

func (m *checker) ResetReplicaAll(ctx context.Context) error {
	_, err := m.db.ExecContext(ctx, "RESET REPLICA ALL")
	if err != nil {
		return fmt.Errorf("reset replica all: %w", err)
	}
	return nil
}

func (m *checker) SetReadOnly(ctx context.Context, on bool) error {
	val := "OFF"
	if on {
		val = "ON"
	}
	_, err := m.db.ExecContext(ctx, "SET GLOBAL read_only = "+val)
	if err != nil {
		return fmt.Errorf("set read_only=%s: %w", val, err)
	}
	return nil
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
	if v, ok := colMap["Last_Errno"]; ok && v.Valid && rs.LastError == "" {
		// fallback
	}

	if v, ok := colMap["Source_Host"]; ok && v.Valid {
		rs.SourceHost = v.String
	}
	if v, ok := colMap["Master_Host"]; ok && v.Valid && rs.SourceHost == "" {
		rs.SourceHost = v.String
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
	var gtid string
	err := m.db.QueryRowContext(ctx, "SELECT @@global.gtid_executed").Scan(&gtid)
	if err != nil {
		return "", fmt.Errorf("query gtid_executed: %w", err)
	}
	return strings.TrimSpace(gtid), nil
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

func (m *checker) ChangeReplicationSource(ctx context.Context, opts ReplicationSourceOpts) error {
	q := fmt.Sprintf(
		"CHANGE REPLICATION SOURCE TO SOURCE_HOST='%s', SOURCE_USER='%s', SOURCE_PASSWORD='%s', SOURCE_AUTO_POSITION=1",
		escapeSingleQuotes(opts.Host), escapeSingleQuotes(opts.User), escapeSingleQuotes(opts.Password),
	)
	if opts.UseSSL {
		q += ", SOURCE_SSL=1"
	} else {
		// MySQL 8's default caching_sha2_password authentication needs
		// the source's RSA public key for non-TLS replication channels.
		// Without this, START REPLICA succeeds but the IO thread exits
		// asynchronously, leaving the site permanently not-replicating.
		q += ", GET_SOURCE_PUBLIC_KEY=1"
	}
	if _, err := m.db.ExecContext(ctx, q); err != nil {
		return fmt.Errorf("change replication source: %w", err)
	}
	return nil
}

func (m *checker) StartReplica(ctx context.Context) error {
	_, err := m.db.ExecContext(ctx, "START REPLICA")
	if err != nil {
		return fmt.Errorf("start replica: %w", err)
	}
	return nil
}

func (m *checker) StartReplicaSQLThread(ctx context.Context) error {
	_, err := m.db.ExecContext(ctx, "START REPLICA SQL_THREAD")
	if err != nil {
		return fmt.Errorf("start replica sql_thread: %w", err)
	}
	return nil
}

func (m *checker) WaitForRelayLogDrain(ctx context.Context, timeout time.Duration) error {
	deadline := time.Now().Add(timeout)
	interval := 500 * time.Millisecond
	const maxInterval = 4 * time.Second
	sqlThreadRestarted := false

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

		if rs.LastError != "" {
			return fmt.Errorf("relay log drain aborted: SQL thread error: %s", rs.LastError)
		}

		if rs.SQLRunning {
			if rs.SecondsBehindSource != nil && *rs.SecondsBehindSource == 0 {
				return nil
			}
		} else {
			// SQL thread is stopped. Check for unapplied relay logs by comparing
			// Retrieved_Gtid_Set (fetched by IO thread) with Executed_Gtid_Set.
			hasPending, parseErr := hasUnappliedRelayLogs(rs.RetrievedGtidSet, rs.ExecutedGtidSet)
			if parseErr != nil {
				return fmt.Errorf("relay log drain: check pending relay logs: %w", parseErr)
			}
			if !hasPending {
				return nil
			}
			// Pending relay logs exist — restart the SQL thread to apply them.
			if !sqlThreadRestarted {
				if startErr := m.StartReplicaSQLThread(ctx); startErr != nil {
					// Connection may be stale after failover turbulence.
					// Force a reconnect via Ping and retry once.
					if pingErr := m.db.PingContext(ctx); pingErr != nil {
						return fmt.Errorf("relay log drain: restart SQL thread: %w (ping also failed: %v)", startErr, pingErr)
					}
					if retryErr := m.StartReplicaSQLThread(ctx); retryErr != nil {
						return fmt.Errorf("relay log drain: restart SQL thread after reconnect: %w", retryErr)
					}
				}
				sqlThreadRestarted = true
			}
		}

		if time.Now().After(deadline) {
			return fmt.Errorf("relay log drain timed out after %s", timeout)
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

func hasUnappliedRelayLogs(retrievedRaw, executedRaw string) (bool, error) {
	if strings.TrimSpace(retrievedRaw) == "" {
		return false, nil
	}
	retrieved, err := ParseGTIDSet(retrievedRaw)
	if err != nil {
		return false, fmt.Errorf("parse retrieved GTID set: %w", err)
	}
	if retrieved.IsEmpty() {
		return false, nil
	}
	executed, err := ParseGTIDSet(executedRaw)
	if err != nil {
		return false, fmt.Errorf("parse executed GTID set: %w", err)
	}
	return !executed.Contains(retrieved), nil
}
