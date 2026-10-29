package testutil

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"fmt"
	"io"
	"strconv"
	"strings"
	"sync"

	"github.com/shipstream/bloodraven/internal/mysql"
)

// ---------------------------------------------------------------------------
// FakeReplica — an in-memory MySQL replica served through database/sql
// ---------------------------------------------------------------------------

// FakeReplica models the replication state of one MySQL replica and serves it
// through a database/sql driver, so the real mysql.Checker (statement text,
// SHOW REPLICA STATUS column parsing, drain loop) runs against it unmodified.
// Only the statements the replica/promotion path issues are modelled; any
// other statement is an error so new SQL fails loudly instead of passing.
type FakeReplica struct {
	mu         sync.Mutex
	state      FakeReplicaState
	onPoll     func(poll int, s *FakeReplicaState)
	polls      int
	statements []string
}

// FakeReplicaState is the replica state SHOW REPLICA STATUS reports.
type FakeReplicaState struct {
	// Configured is false once RESET REPLICA ALL has removed the channel;
	// SHOW REPLICA STATUS then returns no rows.
	Configured bool
	// IORunning reports Replica_IO_Running=Yes. IOConnecting reports
	// "Connecting", the state of a receiver retrying a dead source.
	IORunning    bool
	IOConnecting bool
	SQLRunning   bool
	// SecondsBehindSource is reported as NULL when nil.
	SecondsBehindSource *int64
	LastIOErrno         int
	LastIOError         string
	// LastSQLErrno/LastSQLError are also reported as Last_Errno/Last_Error,
	// which MySQL defines as their aliases.
	LastSQLErrno     int
	LastSQLError     string
	SourceHost       string
	RetrievedGtidSet string
	ExecutedGtidSet  string
	ReadOnly         bool
	SuperReadOnly    bool
	// ServerUUID is reported as @@server_uuid.
	ServerUUID string
	// SourceUUID is reported as Source_UUID: the server_uuid of the source
	// the receiver last connected to.
	SourceUUID string
	// Discarded accumulates the retrieved-but-unexecuted transactions that
	// RESET REPLICA ALL deleted with the relay log.
	Discarded string
}

// NewFakeReplica returns a fake replica in the given state.
func NewFakeReplica(s FakeReplicaState) *FakeReplica {
	return &FakeReplica{state: s}
}

// OnPoll registers fn to run before each SHOW REPLICA STATUS is answered,
// with the 1-based poll count, so a test can advance the applier or receiver
// between polls.
func (f *FakeReplica) OnPoll(fn func(poll int, s *FakeReplicaState)) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.onPoll = fn
}

// Checker returns a real mysql.Checker connected to this fake.
func (f *FakeReplica) Checker() mysql.Checker {
	return mysql.NewCheckerFromPools(sql.OpenDB(f), sql.OpenDB(f))
}

// Mutate applies fn to the replica state under its lock, for a test that
// changes the server between statements.
func (f *FakeReplica) Mutate(fn func(s *FakeReplicaState)) {
	f.mu.Lock()
	defer f.mu.Unlock()
	fn(&f.state)
}

// State returns a copy of the current state.
func (f *FakeReplica) State() FakeReplicaState {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.state
}

// Polls returns how many times SHOW REPLICA STATUS was answered.
func (f *FakeReplica) Polls() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.polls
}

// Statements returns every statement executed or queried, in order.
func (f *FakeReplica) Statements() []string {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]string(nil), f.statements...)
}

// Connect implements driver.Connector.
func (f *FakeReplica) Connect(context.Context) (driver.Conn, error) {
	return &fakeReplicaConn{f: f}, nil
}

// Driver implements driver.Connector.
func (f *FakeReplica) Driver() driver.Driver { return fakeReplicaDriver{} }

type fakeReplicaDriver struct{}

func (fakeReplicaDriver) Open(string) (driver.Conn, error) {
	return nil, errors.New("fake replica: open through sql.OpenDB(*FakeReplica)")
}

type fakeReplicaConn struct{ f *FakeReplica }

func (c *fakeReplicaConn) Prepare(query string) (driver.Stmt, error) {
	return nil, fmt.Errorf("fake replica: prepared statements not modelled: %q", query)
}
func (c *fakeReplicaConn) Close() error { return nil }
func (c *fakeReplicaConn) Begin() (driver.Tx, error) {
	return nil, errors.New("fake replica: transactions not modelled")
}

func (c *fakeReplicaConn) ExecContext(_ context.Context, query string, args []driver.NamedValue) (driver.Result, error) {
	if len(args) > 0 {
		return nil, fmt.Errorf("fake replica: statement arguments not modelled: %q", query)
	}
	f := c.f
	f.mu.Lock()
	defer f.mu.Unlock()
	f.statements = append(f.statements, query)
	s := &f.state

	switch {
	case query == "START REPLICA SQL_THREAD":
		if !s.Configured {
			return nil, errors.New("Error 3080: replica is not configured")
		}
		// Starting the applier clears its last error.
		s.SQLRunning, s.LastSQLErrno, s.LastSQLError = true, 0, ""
	case query == "START REPLICA":
		if !s.Configured {
			return nil, errors.New("Error 3080: replica is not configured")
		}
		s.IORunning, s.SQLRunning, s.LastSQLErrno, s.LastSQLError = true, true, 0, ""
	case strings.HasPrefix(query, "CHANGE REPLICATION SOURCE TO "):
		if s.IORunning || s.IOConnecting || s.SQLRunning {
			return nil, errors.New("Error 3021: replica threads must be stopped before CHANGE REPLICATION SOURCE")
		}
		s.Configured = true
		if _, rest, ok := strings.Cut(query, "SOURCE_HOST='"); ok {
			s.SourceHost, _, _ = strings.Cut(rest, "'")
		}
	case query == "STOP REPLICA":
		s.IORunning, s.IOConnecting, s.SQLRunning = false, false, false
	case query == "RESET REPLICA ALL":
		if s.IORunning || s.IOConnecting || s.SQLRunning {
			return nil, errors.New("Error 3081: replica threads must be stopped before RESET REPLICA ALL")
		}
		lost, err := gtidSubtract(s.RetrievedGtidSet, s.ExecutedGtidSet)
		if err != nil {
			return nil, err
		}
		if s.Discarded, err = gtidUnion(s.Discarded, lost); err != nil {
			return nil, err
		}
		*s = FakeReplicaState{
			ExecutedGtidSet: s.ExecutedGtidSet, ReadOnly: s.ReadOnly,
			SuperReadOnly: s.SuperReadOnly, Discarded: s.Discarded,
			ServerUUID: s.ServerUUID,
		}
	case strings.HasPrefix(query, "SET GLOBAL super_read_only = "):
		on := strings.TrimPrefix(query, "SET GLOBAL super_read_only = ") == "ON"
		s.SuperReadOnly = on
		if on {
			s.ReadOnly = true
		}
	case strings.HasPrefix(query, "SET GLOBAL read_only = "):
		on := strings.TrimPrefix(query, "SET GLOBAL read_only = ") == "ON"
		s.ReadOnly = on
		if !on {
			s.SuperReadOnly = false
		}
	default:
		return nil, fmt.Errorf("fake replica: statement not modelled: %q", query)
	}
	return driver.RowsAffected(0), nil
}

func (c *fakeReplicaConn) QueryContext(_ context.Context, query string, args []driver.NamedValue) (driver.Rows, error) {
	if len(args) > 0 {
		return nil, fmt.Errorf("fake replica: query arguments not modelled: %q", query)
	}
	f := c.f
	f.mu.Lock()
	defer f.mu.Unlock()
	f.statements = append(f.statements, query)
	s := &f.state

	switch query {
	case "SHOW REPLICA STATUS":
		f.polls++
		if f.onPoll != nil {
			f.onPoll(f.polls, s)
		}
		if !s.Configured {
			return &fakeRows{cols: replicaStatusColumns}, nil
		}
		return &fakeRows{cols: replicaStatusColumns, rows: [][]driver.Value{replicaStatusRow(s)}}, nil
	case "SELECT @@global.gtid_executed":
		return &fakeRows{cols: []string{"@@global.gtid_executed"}, rows: [][]driver.Value{{s.ExecutedGtidSet}}}, nil
	case "SELECT @@server_uuid, @@global.gtid_executed":
		return &fakeRows{cols: []string{"@@server_uuid", "@@global.gtid_executed"}, rows: [][]driver.Value{{s.ServerUUID, s.ExecutedGtidSet}}}, nil
	case "SELECT @@read_only":
		v := int64(0)
		if s.ReadOnly {
			v = 1
		}
		return &fakeRows{cols: []string{"@@read_only"}, rows: [][]driver.Value{{v}}}, nil
	}
	return nil, fmt.Errorf("fake replica: query not modelled: %q", query)
}

var replicaStatusColumns = []string{
	"Source_Host", "Replica_IO_Running", "Replica_SQL_Running",
	"Last_Errno", "Last_Error", "Seconds_Behind_Source",
	"Last_IO_Errno", "Last_IO_Error", "Last_SQL_Errno", "Last_SQL_Error",
	"Retrieved_Gtid_Set", "Executed_Gtid_Set", "Source_UUID",
}

func replicaStatusRow(s *FakeReplicaState) []driver.Value {
	io := "No"
	switch {
	case s.IORunning:
		io = "Yes"
	case s.IOConnecting:
		io = "Connecting"
	}
	sqlRunning := "No"
	if s.SQLRunning {
		sqlRunning = "Yes"
	}
	var sbs driver.Value
	if s.SecondsBehindSource != nil {
		sbs = strconv.FormatInt(*s.SecondsBehindSource, 10)
	}
	return []driver.Value{
		s.SourceHost, io, sqlRunning,
		strconv.Itoa(s.LastSQLErrno), s.LastSQLError, sbs,
		strconv.Itoa(s.LastIOErrno), s.LastIOError, strconv.Itoa(s.LastSQLErrno), s.LastSQLError,
		s.RetrievedGtidSet, s.ExecutedGtidSet, s.SourceUUID,
	}
}

type fakeRows struct {
	cols []string
	rows [][]driver.Value
	i    int
}

func (r *fakeRows) Columns() []string { return r.cols }
func (r *fakeRows) Close() error      { return nil }
func (r *fakeRows) Next(dest []driver.Value) error {
	if r.i >= len(r.rows) {
		return io.EOF
	}
	copy(dest, r.rows[r.i])
	r.i++
	return nil
}

func gtidSubtract(a, b string) (string, error) {
	as, err := mysql.ParseGTIDSet(a)
	if err != nil {
		return "", err
	}
	bs, err := mysql.ParseGTIDSet(b)
	if err != nil {
		return "", err
	}
	return as.Subtract(bs).String(), nil
}

func gtidUnion(a, b string) (string, error) {
	switch {
	case a == "":
		return b, nil
	case b == "":
		return a, nil
	}
	as, err := mysql.ParseGTIDSet(a)
	if err != nil {
		return "", err
	}
	bs, err := mysql.ParseGTIDSet(b)
	if err != nil {
		return "", err
	}
	for uuid, ivs := range bs {
		as[uuid] = append(as[uuid], ivs...)
	}
	return as.String(), nil
}
