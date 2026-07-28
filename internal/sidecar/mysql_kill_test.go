package sidecar

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"fmt"
	"io"
	"strings"
	"testing"

	mysqldriver "github.com/go-sql-driver/mysql"
)

// The unit tests around KillConnections' helpers pin the filter
// (killableConnection), the vanished-session rule (isUnknownThread) and
// the report shape (evictionError) in isolation. This file covers the
// assembly: that KillConnections actually routes each outcome into the
// right one of those, and that a pass which could not finish still
// returns the sessions it did evict alongside the reason it stopped
// short. A driver-level fake is used rather than a mock of the method,
// because the behaviours under test — a row that will not scan, an
// iteration that ends early — only exist at the database/sql boundary.

// killFakeRow is one information_schema.processlist row. Values are
// driver.Values so a test can plant a type the scan cannot convert.
type killFakeRow struct {
	id      driver.Value
	user    string
	command string
}

// killFakeConn serves the processlist query and the KILLs that follow.
type killFakeConn struct {
	rows []killFakeRow
	// iterErr, when set, is returned by Next after every row has been
	// delivered, standing in for a connection lost mid-result-set.
	iterErr error
	// killErrs maps a session id to the error its KILL returns; ids
	// absent from the map succeed.
	killErrs map[int64]error
	// listErr, when set, fails the processlist query outright.
	listErr error

	killed []int64
}

func (c *killFakeConn) Prepare(string) (driver.Stmt, error) { return nil, errors.New("unused") }
func (c *killFakeConn) Close() error                        { return nil }
func (c *killFakeConn) Begin() (driver.Tx, error)           { return nil, errors.New("unused") }

func (c *killFakeConn) QueryContext(_ context.Context, query string, _ []driver.NamedValue) (driver.Rows, error) {
	if !strings.Contains(query, "processlist") {
		return nil, fmt.Errorf("unexpected query: %s", query)
	}
	if c.listErr != nil {
		return nil, c.listErr
	}
	return &killFakeRows{conn: c}, nil
}

func (c *killFakeConn) ExecContext(_ context.Context, query string, _ []driver.NamedValue) (driver.Result, error) {
	var id int64
	if _, err := fmt.Sscanf(query, "KILL %d", &id); err != nil {
		return nil, fmt.Errorf("unexpected exec: %s", query)
	}
	if err, ok := c.killErrs[id]; ok {
		return nil, err
	}
	c.killed = append(c.killed, id)
	return driver.RowsAffected(0), nil
}

type killFakeRows struct {
	conn *killFakeConn
	next int
}

func (r *killFakeRows) Columns() []string { return []string{"id", "user", "command"} }
func (r *killFakeRows) Close() error      { return nil }

func (r *killFakeRows) Next(dest []driver.Value) error {
	if r.next >= len(r.conn.rows) {
		if r.conn.iterErr != nil {
			return r.conn.iterErr
		}
		return io.EOF
	}
	row := r.conn.rows[r.next]
	r.next++
	dest[0] = row.id
	dest[1] = row.user
	dest[2] = row.command
	return nil
}

type killFakeConnector struct{ conn *killFakeConn }

func (c killFakeConnector) Connect(context.Context) (driver.Conn, error) { return c.conn, nil }
func (c killFakeConnector) Driver() driver.Driver                        { return killFakeDriver{} }

type killFakeDriver struct{}

func (killFakeDriver) Open(string) (driver.Conn, error) { return nil, errors.New("use the connector") }

// newKillFakeMysql wires a LiveMysql onto conn. sql.OpenDB is used
// instead of sql.Open so no global driver name has to be registered, and
// the pool is pinned to one connection so every statement lands on the
// same fake.
func newKillFakeMysql(t *testing.T, conn *killFakeConn) *LiveMysql {
	t.Helper()
	db := sql.OpenDB(killFakeConnector{conn: conn})
	db.SetMaxOpenConns(1)
	t.Cleanup(func() { _ = db.Close() })
	return &LiveMysql{db: db}
}

func TestKillConnectionsEvictsOnlyApplicationSessions(t *testing.T) {
	conn := &killFakeConn{rows: []killFakeRow{
		{id: int64(7), user: "counter", command: "Query"},
		{id: int64(8), user: "system user", command: "Connect"},    // replica I/O
		{id: int64(9), user: "replicator", command: "Binlog Dump"}, // outbound feed
		{id: int64(10), user: "root", command: "Sleep"},
	}}
	m := newKillFakeMysql(t, conn)

	killed, err := m.KillConnections(context.Background())
	if err != nil {
		t.Fatalf("a complete pass reported an error: %v", err)
	}
	if killed != 2 {
		t.Errorf("killed = %d, want 2", killed)
	}
	if len(conn.killed) != 2 || conn.killed[0] != 7 || conn.killed[1] != 10 {
		t.Errorf("KILLed %v, want the two application sessions [7 10]", conn.killed)
	}
}

// A session that ended on its own between the SELECT and the KILL is the
// outcome the fence wanted. Reporting it would fire the partial-fence
// warning on nearly every fence, since sessions churn constantly.
func TestKillConnectionsIgnoresVanishedSessions(t *testing.T) {
	conn := &killFakeConn{
		rows: []killFakeRow{
			{id: int64(7), user: "counter", command: "Query"},
			{id: int64(8), user: "counter", command: "Query"},
		},
		killErrs: map[int64]error{7: &mysqldriver.MySQLError{Number: 1094, Message: "Unknown thread id: 7"}},
	}
	m := newKillFakeMysql(t, conn)

	killed, err := m.KillConnections(context.Background())
	if err != nil {
		t.Fatalf("a vanished session was reported as a failure: %v", err)
	}
	// The vanished session is not counted as killed either: it is not a
	// failure, but this sidecar did not evict it.
	if killed != 1 {
		t.Errorf("killed = %d, want 1", killed)
	}
}

// The contract that makes a partial fence legible: the sessions that
// could be evicted are, and the error names what stopped the rest.
func TestKillConnectionsReportsPartialEvictions(t *testing.T) {
	iterFail := errors.New("driver went away")
	conn := &killFakeConn{
		rows: []killFakeRow{
			{id: int64(7), user: "counter", command: "Query"},
			{id: "not-an-id", user: "counter", command: "Query"}, // will not scan
			{id: int64(9), user: "counter", command: "Query"},
		},
		iterErr:  iterFail,
		killErrs: map[int64]error{9: &mysqldriver.MySQLError{Number: 1095, Message: "You are not owner of thread 9"}},
	}
	m := newKillFakeMysql(t, conn)

	killed, err := m.KillConnections(context.Background())
	if err == nil {
		t.Fatal("an incomplete pass reported no error")
	}
	// Partial, not aborted: session 7 was still evicted.
	if killed != 1 {
		t.Errorf("killed = %d, want 1 — the readable session must still be evicted", killed)
	}
	if !errors.Is(err, iterFail) {
		t.Errorf("iteration failure did not survive into the report: %v", err)
	}
	msg := err.Error()
	for _, want := range []string{
		"skipped 1 unreadable processlist rows",
		"failed to kill 1 of 2 sessions",
		"kill 9:",
	} {
		if !strings.Contains(msg, want) {
			t.Errorf("report %q is missing %q", msg, want)
		}
	}
}

// When the processlist query itself fails no session is ever enumerated,
// so the count is zero and the fence reports that it evicted nothing —
// the case the log-schema note about `count=0` describes.
func TestKillConnectionsReportsListingFailures(t *testing.T) {
	listFail := errors.New("access denied")
	m := newKillFakeMysql(t, &killFakeConn{listErr: listFail})

	killed, err := m.KillConnections(context.Background())
	if err == nil {
		t.Fatal("a failed processlist query reported no error")
	}
	if killed != 0 {
		t.Errorf("killed = %d, want 0", killed)
	}
	if !errors.Is(err, listFail) {
		t.Errorf("listing failure did not survive into the report: %v", err)
	}
}
