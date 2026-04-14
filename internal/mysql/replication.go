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

func (m *checker) ChangeReplicationSource(ctx context.Context, opts ReplicationSourceOpts) error {
	q := fmt.Sprintf(
		"CHANGE REPLICATION SOURCE TO SOURCE_HOST='%s', SOURCE_USER='%s', SOURCE_PASSWORD='%s', SOURCE_AUTO_POSITION=1",
		escapeSingleQuotes(opts.Host), escapeSingleQuotes(opts.User), escapeSingleQuotes(opts.Password),
	)
	if opts.UseSSL {
		q += ", SOURCE_SSL=1"
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

func (m *checker) WaitForRelayLogDrain(ctx context.Context, timeout time.Duration) error {
	deadline := time.Now().Add(timeout)
	interval := 500 * time.Millisecond
	const maxInterval = 4 * time.Second

	timer := time.NewTimer(interval)
	defer timer.Stop()

	for {
		rs, err := m.ShowReplicaStatus(ctx)
		if err != nil {
			return fmt.Errorf("wait for relay log drain: %w", err)
		}
		if rs == nil {
			// No replication configured, nothing to drain.
			return nil
		}

		// Relay log is drained when SQL thread has caught up (Seconds_Behind_Source == 0)
		// or SQL thread has stopped (no more relay log to apply).
		if !rs.SQLRunning {
			return nil
		}
		if rs.SecondsBehindSource != nil && *rs.SecondsBehindSource == 0 {
			return nil
		}

		// SQL thread is running but has a permanent error — no point polling further.
		if rs.LastError != "" {
			return fmt.Errorf("relay log drain aborted: SQL thread error: %s", rs.LastError)
		}

		if time.Now().After(deadline) {
			return fmt.Errorf("relay log drain timed out after %s", timeout)
		}

		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-timer.C:
		}

		// Exponential backoff: 500ms → 1s → 2s → 4s (cap).
		if interval < maxInterval {
			interval *= 2
		}
		timer.Reset(interval)
	}
}
