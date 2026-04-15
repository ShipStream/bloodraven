package mysql

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
	"time"

	_ "github.com/go-sql-driver/mysql"
)

// Checker checks MySQL read_only status and manages replication.
type Checker interface {
	CheckReadOnly(ctx context.Context) (readOnly bool, err error)
	Promote(ctx context.Context) error
	Close() error

	// Failover hardening methods:
	SetSuperReadOnly(ctx context.Context, on bool) error
	KillAppConnections(ctx context.Context) (killed int, err error)
	StopReplica(ctx context.Context) error
	ResetReplicaAll(ctx context.Context) error
	SetReadOnly(ctx context.Context, on bool) error
	ShowReplicaStatus(ctx context.Context) (*ReplicaStatus, error)
	ChangeReplicationSource(ctx context.Context, opts ReplicationSourceOpts) error
	StartReplica(ctx context.Context) error
	StartReplicaSQLThread(ctx context.Context) error
	WaitForRelayLogDrain(ctx context.Context, timeout time.Duration) error

	// GTID methods:
	GetGtidExecuted(ctx context.Context) (string, error)

	// Clone plugin methods:
	SetCloneDonorList(ctx context.Context, donor string) error
	CloneInstance(ctx context.Context, user, host, password string, useSSL bool, cloneTimeoutSec int) error
}

type checker struct {
	db *sql.DB
}

// NewChecker creates a checker for the given DSN.
func NewChecker(dsn string) (Checker, error) {
	db, err := sql.Open("mysql", dsn)
	if err != nil {
		return nil, fmt.Errorf("open mysql: %w", err)
	}
	db.SetMaxOpenConns(2)
	db.SetMaxIdleConns(1)
	return &checker{db: db}, nil
}

func (m *checker) CheckReadOnly(ctx context.Context) (bool, error) {
	var readOnly int
	err := m.db.QueryRowContext(ctx, "SELECT @@read_only").Scan(&readOnly)
	if err != nil {
		return false, fmt.Errorf("query read_only: %w", err)
	}
	return readOnly == 1, nil
}

func (m *checker) Promote(ctx context.Context) error {
	stmts := []string{
		"STOP REPLICA",
		"RESET REPLICA ALL",
		"SET GLOBAL read_only = 0",
	}
	for _, s := range stmts {
		if _, err := m.db.ExecContext(ctx, s); err != nil {
			return fmt.Errorf("promote (%s): %w", s, err)
		}
	}
	return nil
}

func (m *checker) Close() error {
	return m.db.Close()
}

// escapeSingleQuotes escapes single quotes for MySQL string literals
// used in statements that don't support parameterized queries (e.g. CLONE, CHANGE REPLICATION SOURCE).
func escapeSingleQuotes(s string) string {
	s = strings.ReplaceAll(s, "\\", "\\\\")
	return strings.ReplaceAll(s, "'", "''")
}
