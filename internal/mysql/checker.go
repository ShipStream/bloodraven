package mysql

import (
	"context"
	"database/sql"
	"fmt"

	_ "github.com/go-sql-driver/mysql"
)

// Checker checks MySQL read_only status.
type Checker interface {
	CheckReadOnly(ctx context.Context) (readOnly bool, err error)
	Promote(ctx context.Context) error
	Close() error
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
