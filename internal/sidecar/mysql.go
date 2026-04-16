package sidecar

import (
	"context"
	"database/sql"
	"fmt"
	"strconv"
	"strings"

	_ "github.com/go-sql-driver/mysql"
)

// StatusInfo holds the MySQL status information returned by the sidecar.
type StatusInfo struct {
	Role                string `json:"role"`
	ReadOnly            bool   `json:"read_only"`
	SuperReadOnly       bool   `json:"super_read_only"`
	GtidExecuted        string `json:"gtid_executed"`
	ReplicaIORunning    bool   `json:"replica_io_running"`
	ReplicaSQLRunning   bool   `json:"replica_sql_running"`
	SecondsBehindSource *int64 `json:"seconds_behind_source"`
	ServerID            int    `json:"server_id"`
	Uptime              int64  `json:"uptime"`
}

// mysqlQuerier abstracts MySQL queries for testing.
type mysqlQuerier interface {
	queryStatus(ctx context.Context) (*StatusInfo, error)
	isConnectable(ctx context.Context) bool
	IsReadOnly(ctx context.Context) (bool, error)
	SetSuperReadOnly(ctx context.Context) error
	ClearSuperReadOnly(ctx context.Context) error
}

// LiveMysql queries the actual local MySQL instance.
// It implements mysqlQuerier and Fencer interfaces.
type LiveMysql struct {
	db *sql.DB
}

// NewLiveMysqlFromDSN creates a new LiveMysql from a DSN string.
func NewLiveMysqlFromDSN(dsn string) (*LiveMysql, error) {
	db, err := sql.Open("mysql", dsn)
	if err != nil {
		return nil, fmt.Errorf("open mysql: %w", err)
	}
	db.SetMaxOpenConns(3)
	db.SetMaxIdleConns(1)
	return &LiveMysql{db: db}, nil
}

func (m *LiveMysql) queryStatus(ctx context.Context) (*StatusInfo, error) {
	info := &StatusInfo{}

	// Query global variables
	var readOnly, superReadOnly int
	var serverID int
	var gtidExecuted string
	err := m.db.QueryRowContext(ctx,
		"SELECT @@read_only, @@super_read_only, @@server_id, @@global.gtid_executed",
	).Scan(&readOnly, &superReadOnly, &serverID, &gtidExecuted)
	if err != nil {
		return nil, fmt.Errorf("query global vars: %w", err)
	}

	info.ReadOnly = readOnly == 1
	info.SuperReadOnly = superReadOnly == 1
	info.ServerID = serverID
	info.GtidExecuted = strings.TrimSpace(gtidExecuted)

	if info.ReadOnly {
		info.Role = "replica"
	} else {
		info.Role = "primary"
	}

	// Query replica status
	rows, err := m.db.QueryContext(ctx, "SHOW REPLICA STATUS")
	if err != nil {
		return nil, fmt.Errorf("show replica status: %w", err)
	}
	defer rows.Close()

	if rows.Next() {
		cols, err := rows.Columns()
		if err != nil {
			return nil, fmt.Errorf("get replica status columns: %w", err)
		}

		// Build a map from column name -> value
		vals := make([]any, len(cols))
		ptrs := make([]any, len(cols))
		for i := range vals {
			ptrs[i] = &vals[i]
		}
		if err := rows.Scan(ptrs...); err != nil {
			return nil, fmt.Errorf("scan replica status: %w", err)
		}

		colMap := make(map[string]any, len(cols))
		for i, col := range cols {
			colMap[col] = vals[i]
		}

		if v, ok := colMap["Replica_IO_Running"]; ok {
			info.ReplicaIORunning = asString(v) == "Yes"
		}
		if v, ok := colMap["Replica_SQL_Running"]; ok {
			info.ReplicaSQLRunning = asString(v) == "Yes"
		}
		if v, ok := colMap["Seconds_Behind_Source"]; ok && v != nil {
			if sbs, err := asInt64(v); err == nil {
				info.SecondsBehindSource = &sbs
			}
		}
	}

	// Query uptime
	var uptimeStr string
	err = m.db.QueryRowContext(ctx,
		"SELECT VARIABLE_VALUE FROM performance_schema.global_status WHERE VARIABLE_NAME = 'Uptime'",
	).Scan(&uptimeStr)
	if err != nil {
		// Non-fatal: uptime query may fail if performance_schema is disabled
		info.Uptime = 0
	} else {
		info.Uptime, _ = strconv.ParseInt(uptimeStr, 10, 64)
	}

	return info, nil
}

func (m *LiveMysql) isConnectable(ctx context.Context) bool {
	return m.db.PingContext(ctx) == nil
}

func (m *LiveMysql) IsReadOnly(ctx context.Context) (bool, error) {
	var readOnly int
	err := m.db.QueryRowContext(ctx, "SELECT @@read_only").Scan(&readOnly)
	if err != nil {
		return false, fmt.Errorf("query read_only: %w", err)
	}
	return readOnly == 1, nil
}

func (m *LiveMysql) SetSuperReadOnly(ctx context.Context) error {
	_, err := m.db.ExecContext(ctx, "SET GLOBAL super_read_only = ON")
	if err != nil {
		return fmt.Errorf("set super_read_only: %w", err)
	}
	return nil
}

func (m *LiveMysql) ClearSuperReadOnly(ctx context.Context) error {
	if _, err := m.db.ExecContext(ctx, "SET GLOBAL super_read_only = OFF"); err != nil {
		return fmt.Errorf("clear super_read_only: %w", err)
	}
	if _, err := m.db.ExecContext(ctx, "SET GLOBAL read_only = OFF"); err != nil {
		return fmt.Errorf("clear read_only: %w", err)
	}
	return nil
}

func (m *LiveMysql) KillConnections(ctx context.Context) (int, error) {
	rows, err := m.db.QueryContext(ctx,
		`SELECT id FROM information_schema.processlist
		 WHERE id != CONNECTION_ID()
		 AND command NOT IN ('Binlog Dump', 'Binlog Dump GTID')`)
	if err != nil {
		return 0, fmt.Errorf("list connections: %w", err)
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

// Close closes the underlying database connection.
func (m *LiveMysql) Close() error {
	return m.db.Close()
}

// asString converts a database value to string.
func asString(v any) string {
	if v == nil {
		return ""
	}
	switch val := v.(type) {
	case []byte:
		return string(val)
	case string:
		return val
	default:
		return fmt.Sprintf("%v", val)
	}
}

// asInt64 converts a database value to int64.
func asInt64(v any) (int64, error) {
	if v == nil {
		return 0, fmt.Errorf("nil value")
	}
	switch val := v.(type) {
	case []byte:
		return strconv.ParseInt(string(val), 10, 64)
	case string:
		return strconv.ParseInt(val, 10, 64)
	case int64:
		return val, nil
	case uint64:
		return int64(val), nil
	default:
		return 0, fmt.Errorf("unsupported type %T", v)
	}
}
