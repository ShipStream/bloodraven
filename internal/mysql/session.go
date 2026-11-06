package mysql

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strings"
)

// ErrSessionUnsupported is returned by a CheckerSession whose Checker lacks
// the method. A pinned connSession never returns it.
var ErrSessionUnsupported = errors.New("mysql: session method not supported by this checker")

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
	// CheckReadOnly reads @@read_only.
	CheckReadOnly(ctx context.Context) (bool, error)
	// CheckSuperReadOnly reads @@super_read_only.
	CheckSuperReadOnly(ctx context.Context) (bool, error)
	// ShowReplicaStatus returns nil, nil when no channel is configured.
	ShowReplicaStatus(ctx context.Context) (*ReplicaStatus, error)
	// ReadGTIDSnapshot reads @@server_uuid and gtid_executed in one query.
	ReadGTIDSnapshot(ctx context.Context) (GTIDSnapshot, error)
	StartReplica(ctx context.Context) error
	// ChangeReplicationSource issues the statement
	// Checker.ChangeReplicationSource issues.
	ChangeReplicationSource(ctx context.Context, opts ReplicationSourceOpts) error
	// Close releases the connection. It does not change server state.
	Close() error
}

// sqlRunner is the subset of *sql.DB and *sql.Conn the shared statement
// helpers need.
type sqlRunner interface {
	ExecContext(ctx context.Context, query string, args ...any) (sql.Result, error)
	QueryContext(ctx context.Context, query string, args ...any) (*sql.Rows, error)
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

func (s *connSession) CheckReadOnly(ctx context.Context) (bool, error) {
	return checkReadOnly(ctx, s.conn)
}

func (s *connSession) CheckSuperReadOnly(ctx context.Context) (bool, error) {
	return checkSuperReadOnly(ctx, s.conn)
}

func (s *connSession) ShowReplicaStatus(ctx context.Context) (*ReplicaStatus, error) {
	return showReplicaStatus(ctx, s.conn)
}

func (s *connSession) ReadGTIDSnapshot(ctx context.Context) (GTIDSnapshot, error) {
	return readGTIDSnapshot(ctx, s.conn)
}

func (s *connSession) StartReplica(ctx context.Context) error {
	return startReplica(ctx, s.conn)
}

func (s *connSession) ChangeReplicationSource(ctx context.Context, opts ReplicationSourceOpts) error {
	return changeReplicationSource(ctx, s.conn, opts)
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
func (s checkerSession) CheckReadOnly(ctx context.Context) (bool, error) {
	return s.c.CheckReadOnly(ctx)
}
func (s checkerSession) ShowReplicaStatus(ctx context.Context) (*ReplicaStatus, error) {
	return s.c.ShowReplicaStatus(ctx)
}
func (s checkerSession) StartReplica(ctx context.Context) error { return s.c.StartReplica(ctx) }
func (s checkerSession) ChangeReplicationSource(ctx context.Context, opts ReplicationSourceOpts) error {
	return s.c.ChangeReplicationSource(ctx, opts)
}

// CheckSuperReadOnly and ReadGTIDSnapshot are not on Checker; a checker
// that does not implement them gets ErrSessionUnsupported.
func (s checkerSession) CheckSuperReadOnly(ctx context.Context) (bool, error) {
	c, ok := s.c.(interface {
		CheckSuperReadOnly(context.Context) (bool, error)
	})
	if !ok {
		return false, ErrSessionUnsupported
	}
	return c.CheckSuperReadOnly(ctx)
}
func (s checkerSession) ReadGTIDSnapshot(ctx context.Context) (GTIDSnapshot, error) {
	c, ok := s.c.(interface {
		ReadGTIDSnapshot(context.Context) (GTIDSnapshot, error)
	})
	if !ok {
		return GTIDSnapshot{}, ErrSessionUnsupported
	}
	return c.ReadGTIDSnapshot(ctx)
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

func checkReadOnly(ctx context.Context, r sqlRunner) (bool, error) {
	var readOnly int
	if err := r.QueryRowContext(ctx, "SELECT @@read_only").Scan(&readOnly); err != nil {
		return false, fmt.Errorf("query read_only: %w", err)
	}
	return readOnly == 1, nil
}

func checkSuperReadOnly(ctx context.Context, r sqlRunner) (bool, error) {
	var superReadOnly int
	if err := r.QueryRowContext(ctx, "SELECT @@super_read_only").Scan(&superReadOnly); err != nil {
		return false, fmt.Errorf("query super_read_only: %w", err)
	}
	return superReadOnly == 1, nil
}

func readGTIDSnapshot(ctx context.Context, r sqlRunner) (GTIDSnapshot, error) {
	var snap GTIDSnapshot
	if err := r.QueryRowContext(ctx, "SELECT @@server_uuid, @@global.gtid_executed").Scan(&snap.ServerUUID, &snap.GtidExecuted); err != nil {
		return GTIDSnapshot{}, fmt.Errorf("query server_uuid and gtid_executed: %w", err)
	}
	snap.ServerUUID = strings.TrimSpace(snap.ServerUUID)
	snap.GtidExecuted = strings.TrimSpace(snap.GtidExecuted)
	return snap, nil
}

func startReplica(ctx context.Context, r sqlRunner) error {
	if _, err := r.ExecContext(ctx, "START REPLICA"); err != nil {
		return fmt.Errorf("start replica: %w", err)
	}
	return nil
}

func changeReplicationSource(ctx context.Context, r sqlRunner, opts ReplicationSourceOpts) error {
	if _, err := r.ExecContext(ctx, changeReplicationSourceSQL(opts)); err != nil {
		return fmt.Errorf("change replication source: %w", err)
	}
	return nil
}
