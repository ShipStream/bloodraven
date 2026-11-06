package sidecar

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"fmt"
	"io"
	"strings"
	"testing"

	"github.com/shipstream/bloodraven/internal/pitrarchive"
)

// roleSQLResult is one canned result set; err fails the query.
type roleSQLResult struct {
	cols []string
	rows [][]driver.Value
	err  error
}

// roleSQLConnector serves the production role and D-34 queries from
// canned results, so their SQL and column handling is tested without a
// server (F216).
type roleSQLConnector struct{ results map[string]roleSQLResult }

func (c roleSQLConnector) Connect(context.Context) (driver.Conn, error) { return roleSQLConn(c), nil }
func (c roleSQLConnector) Driver() driver.Driver                        { return roleSQLDriver(c) }

type roleSQLDriver roleSQLConnector

func (d roleSQLDriver) Open(string) (driver.Conn, error) { return roleSQLConn(d), nil }

type roleSQLConn roleSQLConnector

func (c roleSQLConn) Prepare(string) (driver.Stmt, error) {
	return nil, fmt.Errorf("unexpected Prepare")
}
func (c roleSQLConn) Close() error              { return nil }
func (c roleSQLConn) Begin() (driver.Tx, error) { return nil, fmt.Errorf("unexpected Begin") }
func (c roleSQLConn) QueryContext(_ context.Context, q string, _ []driver.NamedValue) (driver.Rows, error) {
	r, ok := c.results[q]
	if !ok {
		return nil, fmt.Errorf("unexpected query %s", q)
	}
	if r.err != nil {
		return nil, r.err
	}
	return &roleSQLRows{cols: r.cols, rows: r.rows}, nil
}

type roleSQLRows struct {
	cols []string
	rows [][]driver.Value
}

func (r *roleSQLRows) Columns() []string { return r.cols }
func (r *roleSQLRows) Close() error      { return nil }
func (r *roleSQLRows) Next(dst []driver.Value) error {
	if len(r.rows) == 0 {
		return io.EOF
	}
	copy(dst, r.rows[0])
	r.rows = r.rows[1:]
	return nil
}

func roleSQLMysql(t *testing.T, results map[string]roleSQLResult) *LiveMysql {
	t.Helper()
	db := sql.OpenDB(roleSQLConnector{results: results})
	t.Cleanup(func() { _ = db.Close() })
	return &LiveMysql{db: db}
}

const roleQuery = "SELECT @@global.read_only, @@server_uuid, @@server_id, @@global.gtid_executed"

func TestLiveMysqlProbeRole(t *testing.T) {
	cols := []string{"read_only", "server_uuid", "server_id", "gtid_executed"}
	tests := []struct {
		name    string
		row     []driver.Value
		want    RoleObservation
		wantErr string
	}{
		{"writable", []driver.Value{int64(0), hintSIDA, int64(102), hintSIDA + ":1-7"}, RoleObservation{ServerUUID: hintSIDA, ServerID: 102, GTIDExecuted: hintSIDA + ":1-7"}, ""},
		{"read-only", []driver.Value{int64(1), hintSIDA, int64(7), ""}, RoleObservation{ReadOnly: true, ServerUUID: hintSIDA, ServerID: 7}, ""},
		{"server_id out of range", []driver.Value{int64(0), hintSIDA, int64(1) << 33, ""}, RoleObservation{}, "out of range"},
		{"no row", nil, RoleObservation{}, "query role"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			res := roleSQLResult{cols: cols}
			if tt.row != nil {
				res.rows = [][]driver.Value{tt.row}
			}
			got, err := roleSQLMysql(t, map[string]roleSQLResult{roleQuery: res}).ProbeRole(context.Background())
			if tt.wantErr != "" {
				if err == nil || !strings.Contains(err.Error(), tt.wantErr) {
					t.Fatalf("err = %v, want %q", err, tt.wantErr)
				}
				return
			}
			if err != nil || got != tt.want {
				t.Fatalf("= %+v, %v; want %+v", got, err, tt.want)
			}
		})
	}
}

func TestLiveMysqlHistoryIntegrityViolation(t *testing.T) {
	count := func(n int64) roleSQLResult {
		return roleSQLResult{cols: []string{"COUNT(*)"}, rows: [][]driver.Value{{n}}}
	}
	status := func(cols []string, row ...driver.Value) roleSQLResult {
		r := roleSQLResult{cols: cols}
		if row != nil {
			r.rows = [][]driver.Value{row}
		}
		return r
	}
	statusCols := []string{"File", "Position", "Binlog_Do_DB", "Binlog_Ignore_DB", "Executed_Gtid_Set"}
	settings := func(format, image, order string) roleSQLResult {
		return roleSQLResult{cols: []string{"f", "i", "o"}, rows: [][]driver.Value{{format, image, order}}}
	}
	const (
		filters       = "SELECT COUNT(*) FROM performance_schema.replication_applier_filters"
		globalFilters = "SELECT COUNT(*) FROM performance_schema.replication_applier_global_filters"
		showStatus    = "SHOW BINARY LOG STATUS"
		settingsQuery = "SELECT @@global.binlog_format, @@global.binlog_row_image, @@global.replica_preserve_commit_order"
	)
	clean := func() map[string]roleSQLResult {
		return map[string]roleSQLResult{
			filters:       count(0),
			globalFilters: count(0),
			showStatus:    status(statusCols, "mysql-bin.000001", int64(4), "", "", ""),
			settingsQuery: settings("ROW", "FULL", "1"),
		}
	}
	tests := []struct {
		name       string
		edit       func(map[string]roleSQLResult)
		want       pitrarchive.HistoryViolation
		wantDetail string
		wantErr    string
	}{
		{"clean", func(map[string]roleSQLResult) {}, "", "", ""},
		{"channel filter", func(r map[string]roleSQLResult) { r[filters] = count(2) }, pitrarchive.ViolationReplicationFilter, "performance_schema.replication_applier_filters has 2 rows", ""},
		{"global filter", func(r map[string]roleSQLResult) { r[globalFilters] = count(1) }, pitrarchive.ViolationReplicationFilter, "performance_schema.replication_applier_global_filters has 1 rows", ""},
		{"binlog do db", func(r map[string]roleSQLResult) {
			r[showStatus] = status(statusCols, "mysql-bin.000001", int64(4), "app", "", "")
		}, pitrarchive.ViolationBinlogFilter, `Binlog_Do_DB="app" Binlog_Ignore_DB=""`, ""},
		{"binlog ignore db", func(r map[string]roleSQLResult) {
			r[showStatus] = status(statusCols, "mysql-bin.000001", int64(4), nil, "tmp", "")
		}, pitrarchive.ViolationBinlogFilter, `Binlog_Do_DB="" Binlog_Ignore_DB="tmp"`, ""},
		{"row image", func(r map[string]roleSQLResult) { r[settingsQuery] = settings("ROW", "MINIMAL", "1") }, pitrarchive.ViolationBinlogRowImage, "binlog_row_image=MINIMAL", ""},
		{"status without filter columns", func(r map[string]roleSQLResult) {
			r[showStatus] = status([]string{"File", "Position"}, "mysql-bin.000001", int64(4))
		}, "", "", "has no Binlog_Do_DB/Binlog_Ignore_DB columns"},
		{"status with no row (binary logging off)", func(r map[string]roleSQLResult) {
			r[showStatus] = status(statusCols)
		}, "", "", "binary logging is off"},
		{"filter table unreadable", func(r map[string]roleSQLResult) {
			r[filters] = roleSQLResult{err: fmt.Errorf("access denied")}
		}, "", "", "query replication_applier_filters"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			results := clean()
			tt.edit(results)
			got, detail, err := roleSQLMysql(t, results).HistoryIntegrityViolation(context.Background())
			if tt.wantErr != "" {
				if err == nil || !strings.Contains(err.Error(), tt.wantErr) {
					t.Fatalf("err = %v, want %q", err, tt.wantErr)
				}
				return
			}
			if err != nil || got != tt.want || detail != tt.wantDetail {
				t.Fatalf("= %q %q %v, want %q %q", got, detail, err, tt.want, tt.wantDetail)
			}
		})
	}
}
