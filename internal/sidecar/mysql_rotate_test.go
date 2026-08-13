package sidecar

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"strings"
	"sync"
	"testing"
)

// This file pins one property: a master-key rotation must not reach the
// binary log.
//
// MySQL replicates ALTER INSTANCE ROTATE INNODB MASTER KEY so a rotation
// on a source reaches its replicas. Bloodraven inverts that — it refuses
// to rotate the active primary and rotates the replica instead — so a
// binlogged rotation is written under the replica's OWN server UUID and
// leaves it permanently one transaction ahead of its source. The
// divergence is latent while replication keeps flowing and only surfaces
// the next time something restarts or re-points it, at which point the
// operator's GTID convergence check correctly refuses and the site needs
// a re-clone.
//
// Found by playground scenario 50: after scenario 48 rotated the
// replica, deleting that replica's pod left it stuck at
// "replication source convergence blocked", with its binlog showing
//
//	Gtid  SET @@SESSION.GTID_NEXT= '<replica-uuid>:7'
//	Query use `mysql`; ALTER INSTANCE ROTATE INNODB MASTER KEY

// rotateFakeConn records every statement it is asked to execute, in
// order, so the test can assert on the sequencing rather than just the
// final state.
type rotateFakeConn struct {
	mu    sync.Mutex
	execs []string

	// rotateErr, when set, is returned by the rotation statement.
	rotateErr error
	// restoreErr, when set, fails the sql_log_bin restore.
	restoreErr error
	// onRotate runs when the rotation statement is issued. Used to
	// cancel the caller's context mid-rotation, which is what the 30s
	// rotate deadline does in production.
	onRotate func()
	// restoreCtxErr records ctx.Err() as seen by the restore statement.
	restoreCtxErr error
}

func (c *rotateFakeConn) Prepare(string) (driver.Stmt, error) { return nil, errors.New("unused") }
func (c *rotateFakeConn) Close() error                        { return nil }
func (c *rotateFakeConn) Begin() (driver.Tx, error)           { return nil, errors.New("unused") }

func (c *rotateFakeConn) ExecContext(ctx context.Context, query string, _ []driver.NamedValue) (driver.Result, error) {
	c.mu.Lock()
	c.execs = append(c.execs, query)
	c.mu.Unlock()

	switch {
	case strings.Contains(query, "ROTATE INNODB MASTER KEY"):
		if c.onRotate != nil {
			c.onRotate()
		}
		if c.rotateErr != nil {
			return nil, c.rotateErr
		}
	case strings.Contains(query, "sql_log_bin = 1"):
		c.mu.Lock()
		c.restoreCtxErr = ctx.Err()
		c.mu.Unlock()
		if c.restoreErr != nil {
			return nil, c.restoreErr
		}
	}
	return driver.RowsAffected(0), nil
}

func (c *rotateFakeConn) statements() []string {
	c.mu.Lock()
	defer c.mu.Unlock()
	out := make([]string, len(c.execs))
	copy(out, c.execs)
	return out
}

type rotateFakeConnector struct{ conn *rotateFakeConn }

func (c rotateFakeConnector) Connect(context.Context) (driver.Conn, error) { return c.conn, nil }
func (c rotateFakeConnector) Driver() driver.Driver                        { return rotateFakeDriver{} }

type rotateFakeDriver struct{}

func (rotateFakeDriver) Open(string) (driver.Conn, error) { return nil, errors.New("use the connector") }

func newRotateFakeMysql(t *testing.T, conn *rotateFakeConn) *LiveMysql {
	t.Helper()
	db := sql.OpenDB(rotateFakeConnector{conn: conn})
	db.SetMaxOpenConns(1)
	t.Cleanup(func() { _ = db.Close() })
	return &LiveMysql{db: db}
}

func TestRotateInnoDBMasterKey_IsNotBinlogged(t *testing.T) {
	conn := &rotateFakeConn{}
	m := newRotateFakeMysql(t, conn)

	if err := m.RotateInnoDBMasterKey(context.Background()); err != nil {
		t.Fatalf("rotate: %v", err)
	}

	got := conn.statements()
	want := []string{
		"SET SESSION sql_log_bin = 0",
		"ALTER INSTANCE ROTATE INNODB MASTER KEY",
		"SET SESSION sql_log_bin = 1",
	}
	if len(got) != len(want) {
		t.Fatalf("statements = %q, want %q", got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("statement %d = %q, want %q (full sequence %q)", i, got[i], want[i], got)
		}
	}
}

// The restore has to happen even when the rotation itself fails, or the
// pooled connection goes back with binary logging off and every later
// statement on it silently stops replicating.
func TestRotateInnoDBMasterKey_RestoresBinlogAfterAFailedRotation(t *testing.T) {
	rotateFail := errors.New("ER_CANNOT_FIND_KEY_IN_KEYRING")
	conn := &rotateFakeConn{rotateErr: rotateFail}
	m := newRotateFakeMysql(t, conn)

	err := m.RotateInnoDBMasterKey(context.Background())
	if err == nil {
		t.Fatal("a failed rotation reported success")
	}
	if !errors.Is(err, rotateFail) {
		t.Errorf("rotation failure did not survive into the error: %v", err)
	}
	got := conn.statements()
	if len(got) == 0 || got[len(got)-1] != "SET SESSION sql_log_bin = 1" {
		t.Fatalf("binary logging was not restored after a failed rotation: %q", got)
	}
}

// The production case for the detached restore context: the agent gives
// the rotation a 30s deadline, so a slow ALTER INSTANCE has its context
// cancelled while the connection is already sitting at sql_log_bin = 0.
// If the restore inherited that cancellation it would be skipped, and the
// connection would return to the pool with binary logging off.
func TestRotateInnoDBMasterKey_RestoresBinlogWhenTheRotationDeadlineFires(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	conn := &rotateFakeConn{onRotate: cancel}
	m := newRotateFakeMysql(t, conn)

	_ = m.RotateInnoDBMasterKey(ctx)

	got := conn.statements()
	if len(got) == 0 || got[len(got)-1] != "SET SESSION sql_log_bin = 1" {
		t.Fatalf("binary logging was not restored when the rotation deadline fired: %q", got)
	}
	conn.mu.Lock()
	restoreCtxErr := conn.restoreCtxErr
	conn.mu.Unlock()
	if restoreCtxErr != nil {
		t.Errorf("the restore ran on a cancelled context (%v); it must be detached from the caller's deadline",
			restoreCtxErr)
	}
}

// If the restore fails the connection is unsafe to reuse, so that has to
// be surfaced rather than swallowed behind a successful rotation.
func TestRotateInnoDBMasterKey_ReportsAFailedRestore(t *testing.T) {
	restoreFail := errors.New("connection lost")
	conn := &rotateFakeConn{restoreErr: restoreFail}
	m := newRotateFakeMysql(t, conn)

	err := m.RotateInnoDBMasterKey(context.Background())
	if err == nil {
		t.Fatal("a failed sql_log_bin restore was swallowed")
	}
	if !errors.Is(err, restoreFail) {
		t.Errorf("restore failure did not survive into the error: %v", err)
	}
}

// EncryptSystemTablespace is the deliberate counter-case: ALTER
// TABLESPACE is a logical schema change that MUST replicate, and the
// agent only ever runs it on the writable primary. Suppressing its
// binlog would leave the replicas' `mysql` tablespace in the clear while
// status reported full coverage.
func TestEncryptSystemTablespace_StaysBinlogged(t *testing.T) {
	conn := &rotateFakeConn{}
	m := newRotateFakeMysql(t, conn)

	if err := m.EncryptSystemTablespace(context.Background()); err != nil {
		t.Fatalf("encrypt system tablespace: %v", err)
	}
	for _, stmt := range conn.statements() {
		if strings.Contains(stmt, "sql_log_bin") {
			t.Fatalf("system tablespace encryption must replicate, but it touched sql_log_bin: %q",
				conn.statements())
		}
	}
}
