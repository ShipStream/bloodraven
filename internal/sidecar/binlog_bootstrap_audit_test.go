package sidecar

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"fmt"
	"io"
	"log/slog"
	"testing"
)

type auditSQLState struct{ readOnly, channel, cloning bool }
type auditConnector struct{ state auditSQLState }

func (c auditConnector) Connect(context.Context) (driver.Conn, error) {
	return auditConn(c), nil
}
func (c auditConnector) Driver() driver.Driver { return auditDriver(c) }

type auditDriver struct{ state auditSQLState }

func (d auditDriver) Open(string) (driver.Conn, error) { return auditConn(d), nil }

type auditConn struct{ state auditSQLState }

func (c auditConn) Prepare(string) (driver.Stmt, error) {
	return nil, fmt.Errorf("unexpected Prepare")
}
func (c auditConn) Close() error              { return nil }
func (c auditConn) Begin() (driver.Tx, error) { return nil, fmt.Errorf("unexpected Begin") }
func (c auditConn) QueryContext(_ context.Context, q string, _ []driver.NamedValue) (driver.Rows, error) {
	switch q {
	case "SELECT PID FROM performance_schema.clone_status WHERE STATE = 'In Progress' LIMIT 1":
		if c.state.cloning {
			return &auditRows{value: []driver.Value{int64(1)}}, nil
		}
		return &auditRows{}, nil
	case "SELECT @@read_only":
		value := int64(0)
		if c.state.readOnly {
			value = 1
		}
		return &auditRows{value: []driver.Value{value}}, nil
	case "SHOW REPLICA STATUS":
		if c.state.channel {
			return &auditRows{value: []driver.Value{"default"}}, nil
		}
		return &auditRows{}, nil
	default:
		return nil, fmt.Errorf("unexpected query %s", q)
	}
}

type auditRows struct{ value []driver.Value }

func (r *auditRows) Columns() []string { return []string{"value"} }
func (r *auditRows) Close() error      { return nil }
func (r *auditRows) Next(dst []driver.Value) error {
	if r.value == nil {
		return io.EOF
	}
	copy(dst, r.value)
	r.value = nil
	return nil
}
func TestAuditRequiresInitializedServer(t *testing.T) {
	for _, tc := range []struct {
		name  string
		state auditSQLState
		ready bool
	}{
		{"pre-clone recipient", auditSQLState{readOnly: true}, false},
		{"clone in progress", auditSQLState{cloning: true}, false},
		{"configured replica", auditSQLState{readOnly: true, channel: true}, true},
		{"writable initialized primary", auditSQLState{}, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			db := sql.OpenDB(auditConnector{tc.state})
			defer db.Close()
			got, err := (&LiveMysql{db: db}).BinlogHistoryReady(context.Background())
			if err != nil || got != tc.ready {
				t.Fatalf("ready=%v error=%v", got, err)
			}
		})
	}
}

type bootstrapAuditState struct {
	historyStateStub
	ready bool
}

func (s *bootstrapAuditState) BinlogHistoryReady(context.Context) (bool, error) { return s.ready, nil }
func TestInitialGTIDsAreNotCompletedBootstrap(t *testing.T) {
	ctx := context.Background()
	store := newMemStore()
	state := &bootstrapAuditState{historyStateStub: historyStateStub{uuid: coverageTestSID, executed: coverageTestSID + ":1-21"}, ready: true}
	a := newTestArchiver(&PITRConfig{ManifestPrefix: "binlogs"}, "iad", state, store, slog.Default())
	id, err := a.localBinlogID("testdata/incarnation-b.bin")
	if err != nil {
		t.Fatal(err)
	}
	if err := a.archiveOne(ctx, "testdata/incarnation-b.bin", "mysql-bin.000002", id); err != nil {
		t.Fatal(err)
	}
	state.readOnly = true
	state.uuid = "ffffffff-bbbb-cccc-dddd-eeeeeeeeeeee"
	state.executed = state.uuid + ":1-3"
	state.ready = false
	a.scanAndArchive(ctx)
	m, err := loadManifest(ctx, store, "binlogs", "iad")
	if err != nil {
		t.Fatal(err)
	}
	if len(m.Divergent) != 0 {
		t.Fatal("local initialization transactions were mistaken for completed clone state")
	}
	// A server with no history yet has no verdict on what it could reissue.
	if st := a.Snapshot(ctx); st.ReissuableGTIDs != nil || st.LastError != "" {
		t.Fatalf("a server awaiting its clone was audited: %+v", st)
	}
	// The completed bootstrap lacks an archived transaction. The server has
	// another server_uuid, so that is recorded and is not an error.
	state.ready = true
	state.executed = coverageTestSID + ":1-20"
	a.scanAndArchive(ctx)
	status := a.Snapshot(ctx)
	if status.DivergentRows != 1 || status.ReissuableGTIDs == nil || *status.ReissuableGTIDs != "" || status.LastError != "" || status.UploadFailures != 0 {
		t.Fatalf("completed bootstrap with a lost tail: %+v", status)
	}
	state.executed = coverageTestSID + ":1-21"
	a.scanAndArchive(ctx)
	status = a.Snapshot(ctx)
	if status.DivergentRows != 0 || status.LastError != "" || status.UploadFailures != 0 {
		t.Fatalf("restored execution kept stale divergence evidence: %+v", status)
	}
}
