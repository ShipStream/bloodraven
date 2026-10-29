package mysql

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
)

// Session runs a sequence of statements on one MySQL server connection.
// Checker methods each take whichever pooled connection is free, and the
// pool silently dials a new one when a connection breaks, so consecutive
// Checker calls can reach different servers if the address now routes
// elsewhere (a replaced pod behind the same Service). A Session never
// reconnects: once its connection breaks, every later call fails. Callers
// that must prove something about a server and then change that same
// server, such as a maintenance promotion, use one Session for both.
type Session interface {
	StopReplica(ctx context.Context) error
	ResetReplicaAll(ctx context.Context) error
	GetGtidExecuted(ctx context.Context) (string, error)
	SetSuperReadOnly(ctx context.Context, on bool) error
	SetReadOnly(ctx context.Context, on bool) error
	// Close releases the connection. It does not change server state.
	Close() error
}

// sqlRunner is the subset of *sql.DB and *sql.Conn the shared statement
// helpers need.
type sqlRunner interface {
	ExecContext(ctx context.Context, query string, args ...any) (sql.Result, error)
	QueryRowContext(ctx context.Context, query string, args ...any) *sql.Row
}

// PinSession holds one connection from the primary pool for a Session.
func (m *checker) PinSession(ctx context.Context) (Session, error) {
	conn, err := m.db.Conn(ctx)
	if err != nil {
		return nil, fmt.Errorf("pin connection: %w", err)
	}
	return &connSession{conn: conn}, nil
}

type connSession struct {
	conn *sql.Conn
}

func (s *connSession) StopReplica(ctx context.Context) error {
	return stopReplica(ctx, s.conn)
}

func (s *connSession) ResetReplicaAll(ctx context.Context) error {
	return resetReplicaAll(ctx, s.conn)
}

func (s *connSession) GetGtidExecuted(ctx context.Context) (string, error) {
	return getGtidExecuted(ctx, s.conn)
}

func (s *connSession) SetSuperReadOnly(ctx context.Context, on bool) error {
	return setSuperReadOnly(ctx, s.conn, on)
}

func (s *connSession) SetReadOnly(ctx context.Context, on bool) error {
	return setReadOnly(ctx, s.conn, on)
}

func (s *connSession) Close() error {
	return s.conn.Close()
}

// CheckerSession returns a Session that sends each call through c. It pins
// nothing, so it gives no single-server guarantee; it exists for Checker
// implementations that have no connection to pin, such as test fakes and
// simulations.
func CheckerSession(c Checker) Session {
	return checkerSession{c}
}

type checkerSession struct{ c Checker }

func (s checkerSession) StopReplica(ctx context.Context) error     { return s.c.StopReplica(ctx) }
func (s checkerSession) ResetReplicaAll(ctx context.Context) error { return s.c.ResetReplicaAll(ctx) }
func (s checkerSession) GetGtidExecuted(ctx context.Context) (string, error) {
	return s.c.GetGtidExecuted(ctx)
}
func (s checkerSession) SetSuperReadOnly(ctx context.Context, on bool) error {
	return s.c.SetSuperReadOnly(ctx, on)
}
func (s checkerSession) SetReadOnly(ctx context.Context, on bool) error {
	return s.c.SetReadOnly(ctx, on)
}
func (s checkerSession) Close() error { return nil }

func onOff(on bool) string {
	if on {
		return "ON"
	}
	return "OFF"
}

func setSuperReadOnly(ctx context.Context, r sqlRunner, on bool) error {
	val := onOff(on)
	if _, err := r.ExecContext(ctx, "SET GLOBAL super_read_only = "+val); err != nil {
		return fmt.Errorf("set super_read_only=%s: %w", val, err)
	}
	return nil
}

func setReadOnly(ctx context.Context, r sqlRunner, on bool) error {
	val := onOff(on)
	if _, err := r.ExecContext(ctx, "SET GLOBAL read_only = "+val); err != nil {
		return fmt.Errorf("set read_only=%s: %w", val, err)
	}
	return nil
}

func stopReplica(ctx context.Context, r sqlRunner) error {
	if _, err := r.ExecContext(ctx, "STOP REPLICA"); err != nil {
		return fmt.Errorf("stop replica: %w", err)
	}
	return nil
}

func resetReplicaAll(ctx context.Context, r sqlRunner) error {
	if _, err := r.ExecContext(ctx, "RESET REPLICA ALL"); err != nil {
		return fmt.Errorf("reset replica all: %w", err)
	}
	return nil
}

func getGtidExecuted(ctx context.Context, r sqlRunner) (string, error) {
	var gtid string
	if err := r.QueryRowContext(ctx, "SELECT @@global.gtid_executed").Scan(&gtid); err != nil {
		return "", fmt.Errorf("query gtid_executed: %w", err)
	}
	return strings.TrimSpace(gtid), nil
}
