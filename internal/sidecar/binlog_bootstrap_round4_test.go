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

type rr4AuditSQL struct{ readOnly, channel, cloning bool }
type rr4AuditConnector struct{ state rr4AuditSQL }

func (c rr4AuditConnector) Connect(context.Context) (driver.Conn, error) {
	return rr4AuditConn(c), nil
}
func (c rr4AuditConnector) Driver() driver.Driver { return rr4AuditDriver(c) }

type rr4AuditDriver struct{ state rr4AuditSQL }

func (d rr4AuditDriver) Open(string) (driver.Conn, error) { return rr4AuditConn(d), nil }

type rr4AuditConn struct{ state rr4AuditSQL }

func (c rr4AuditConn) Prepare(string) (driver.Stmt, error) {
	return nil, fmt.Errorf("unexpected Prepare")
}
func (c rr4AuditConn) Close() error              { return nil }
func (c rr4AuditConn) Begin() (driver.Tx, error) { return nil, fmt.Errorf("unexpected Begin") }
func (c rr4AuditConn) QueryContext(_ context.Context, q string, _ []driver.NamedValue) (driver.Rows, error) {
	switch q {
	case "SELECT PID FROM performance_schema.clone_status WHERE STATE = 'In Progress' LIMIT 1":
		if c.state.cloning {
			return &rr4AuditRows{value: []driver.Value{int64(1)}}, nil
		}
		return &rr4AuditRows{}, nil
	case "SELECT @@read_only":
		value := int64(0)
		if c.state.readOnly {
			value = 1
		}
		return &rr4AuditRows{value: []driver.Value{value}}, nil
	case "SHOW REPLICA STATUS":
		if c.state.channel {
			return &rr4AuditRows{value: []driver.Value{"default"}}, nil
		}
		return &rr4AuditRows{}, nil
	default:
		return nil, fmt.Errorf("unexpected query %s", q)
	}
}

type rr4AuditRows struct{ value []driver.Value }

func (r *rr4AuditRows) Columns() []string { return []string{"value"} }
func (r *rr4AuditRows) Close() error      { return nil }
func (r *rr4AuditRows) Next(dst []driver.Value) error {
	if r.value == nil {
		return io.EOF
	}
	copy(dst, r.value)
	r.value = nil
	return nil
}
func TestRereview4AuditRequiresInitializedServer(t *testing.T) {
	for _, tc := range []struct {
		name  string
		state rr4AuditSQL
		ready bool
	}{
		{"pre-clone recipient", rr4AuditSQL{readOnly: true}, false},
		{"clone in progress", rr4AuditSQL{cloning: true}, false},
		{"configured replica", rr4AuditSQL{readOnly: true, channel: true}, true},
		{"writable initialized primary", rr4AuditSQL{}, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			db := sql.OpenDB(rr4AuditConnector{tc.state})
			defer db.Close()
			got, err := (&LiveMysql{db: db}).BinlogHistoryReady(context.Background())
			if err != nil || got != tc.ready {
				t.Fatalf("ready=%v error=%v", got, err)
			}
		})
	}
}

type rr4BootstrapState struct {
	historyStateStub
	ready bool
}

func (s *rr4BootstrapState) BinlogHistoryReady(context.Context) (bool, error) { return s.ready, nil }
func TestRereview4InitialGTIDsAreNotCompletedBootstrap(t *testing.T) {
	ctx := context.Background()
	store := newMemStore()
	state := &rr4BootstrapState{historyStateStub: historyStateStub{uuid: coverageTestSID, executed: coverageTestSID + ":1-21"}, ready: true}
	a := NewBinlogArchiver(&PITRConfig{ManifestPrefix: "binlogs"}, "iad", state, store, slog.Default())
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
	state.ready = true
	state.executed = coverageTestSID + ":1-20"
	a.scanAndArchive(ctx)
	if a.Snapshot(ctx).LastError == "" {
		t.Fatal("completed bootstrap with a lost tail was not reported")
	}
	failures := a.Snapshot(ctx).UploadFailures
	state.executed = coverageTestSID + ":1-21"
	a.scanAndArchive(ctx)
	a.scanAndArchive(ctx)
	status := a.Snapshot(ctx)
	if status.LastError != "" || status.UploadFailures != failures {
		t.Fatalf("recovery kept stale divergence errors: %+v", status)
	}
}
