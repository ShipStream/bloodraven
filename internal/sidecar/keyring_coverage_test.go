package sidecar

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"encoding/json"
	"fmt"
	"io"
	"strings"
	"testing"
)

func optionalBoolString(b *bool) string {
	if b == nil {
		return "unset"
	}
	if *b {
		return "true"
	}
	return "false"
}

// A-46/F-13: a coverage flag MySQL did not report stays unknown. NULL, a
// missing row or an unrecognized value is never reported as false.
func TestCoverageFlagParsing(t *testing.T) {
	for _, tc := range []struct {
		in   sql.NullString
		want string
	}{
		{sql.NullString{}, "unset"},
		{sql.NullString{Valid: true, String: ""}, "unset"},
		{sql.NullString{Valid: true, String: "maybe"}, "unset"},
		{sql.NullString{Valid: true, String: "ON"}, "true"},
		{sql.NullString{Valid: true, String: "1"}, "true"},
		{sql.NullString{Valid: true, String: " true "}, "true"},
		{sql.NullString{Valid: true, String: "OFF"}, "false"},
		{sql.NullString{Valid: true, String: "0"}, "false"},
		{sql.NullString{Valid: true, String: "false"}, "false"},
	} {
		if got := optionalBoolString(mysqlOptionalBool(tc.in)); got != tc.want {
			t.Errorf("mysqlOptionalBool(%+v) = %s, want %s", tc.in, got, tc.want)
		}
	}
	for in, want := range map[string]string{"Y": "true", "y": "true", "N": "false", "": "unset", "?": "unset"} {
		if got := optionalBoolString(tablespaceEncrypted(in)); got != want {
			t.Errorf("tablespaceEncrypted(%q) = %s, want %s", in, got, want)
		}
	}
}

// The /keyring/status wire omits an unknown flag and carries false and
// true as reported.
func TestKeyringCoverageWireOmitsUnknownFlags(t *testing.T) {
	f, tr := false, true
	raw, err := json.Marshal(KeyringCoverage{SystemTablespaceEncrypted: nil, RedoLogEncrypted: &f, UndoLogEncrypted: &tr})
	if err != nil {
		t.Fatal(err)
	}
	var got map[string]any
	if err := json.Unmarshal(raw, &got); err != nil {
		t.Fatal(err)
	}
	if _, ok := got["systemTablespaceEncrypted"]; ok {
		t.Errorf("an unknown flag is on the wire: %s", raw)
	}
	if _, ok := got["binlogEncrypted"]; ok {
		t.Errorf("an unknown flag is on the wire: %s", raw)
	}
	if got["redoLogEncrypted"] != false || got["undoLogEncrypted"] != true {
		t.Errorf("reported flags lost: %s", raw)
	}
	if _, ok := got["unencryptedTablespaces"]; !ok {
		t.Errorf("unencryptedTablespaces dropped: %s", raw)
	}
}

// coverageSQL answers EncryptionCoverage's three queries. A nil variable
// is NULL; sysEnc "" is no row for the mysql tablespace.
type coverageSQL struct {
	redo, undo, binlog *string
	sysEnc             string
	unencrypted        int64
}

type coverageConnector struct{ s coverageSQL }

func (c coverageConnector) Connect(context.Context) (driver.Conn, error) { return coverageConn(c), nil }
func (c coverageConnector) Driver() driver.Driver                        { return coverageDriver(c) }

type coverageDriver struct{ s coverageSQL }

func (d coverageDriver) Open(string) (driver.Conn, error) { return coverageConn(d), nil }

type coverageConn struct{ s coverageSQL }

func (c coverageConn) Prepare(string) (driver.Stmt, error) {
	return nil, fmt.Errorf("unexpected Prepare")
}
func (c coverageConn) Close() error              { return nil }
func (c coverageConn) Begin() (driver.Tx, error) { return nil, fmt.Errorf("unexpected Begin") }
func (c coverageConn) QueryContext(_ context.Context, q string, _ []driver.NamedValue) (driver.Rows, error) {
	nullable := func(v *string) driver.Value {
		if v == nil {
			return nil
		}
		return *v
	}
	switch {
	case strings.Contains(q, "@@innodb_redo_log_encrypt"):
		return &coverageRows{cols: 3, rows: [][]driver.Value{{nullable(c.s.redo), nullable(c.s.undo), nullable(c.s.binlog)}}}, nil
	case strings.Contains(q, "WHERE NAME = 'mysql'"):
		if c.s.sysEnc == "" {
			return &coverageRows{cols: 1}, nil
		}
		return &coverageRows{cols: 1, rows: [][]driver.Value{{c.s.sysEnc}}}, nil
	case strings.Contains(q, "COUNT(*)"):
		return &coverageRows{cols: 1, rows: [][]driver.Value{{c.s.unencrypted}}}, nil
	}
	return nil, fmt.Errorf("unexpected query %s", q)
}

type coverageRows struct {
	cols int
	rows [][]driver.Value
}

func (r *coverageRows) Columns() []string { return make([]string, r.cols) }
func (r *coverageRows) Close() error      { return nil }
func (r *coverageRows) Next(dst []driver.Value) error {
	if len(r.rows) == 0 {
		return io.EOF
	}
	copy(dst, r.rows[0])
	r.rows = r.rows[1:]
	return nil
}

// TestEncryptionCoverage_EachFlagMissingFalseTrue (F-13): every coverage
// flag the sidecar probes, missing, false and true, through
// EncryptionCoverage and onto the /keyring/status wire. A missing flag is
// omitted, never sent as false; the other flags are unaffected.
func TestEncryptionCoverage_EachFlagMissingFalseTrue(t *testing.T) {
	on, off := "ON", "OFF"
	flags := []struct {
		name string
		set  func(*coverageSQL, string) // "missing", "false" or "true"
	}{
		{"redoLogEncrypted", func(s *coverageSQL, v string) {
			s.redo = map[string]*string{"missing": nil, "false": &off, "true": &on}[v]
		}},
		{"undoLogEncrypted", func(s *coverageSQL, v string) {
			s.undo = map[string]*string{"missing": nil, "false": &off, "true": &on}[v]
		}},
		{"binlogEncrypted", func(s *coverageSQL, v string) {
			s.binlog = map[string]*string{"missing": nil, "false": &off, "true": &on}[v]
		}},
		{"systemTablespaceEncrypted", func(s *coverageSQL, v string) {
			s.sysEnc = map[string]string{"missing": "", "false": "N", "true": "Y"}[v]
		}},
	}
	for _, flag := range flags {
		for _, state := range []string{"missing", "false", "true"} {
			t.Run(flag.name+"/"+state, func(t *testing.T) {
				// Every other flag is reported on, so a leak shows.
				s := coverageSQL{redo: &on, undo: &on, binlog: &on, sysEnc: "Y", unencrypted: 2}
				flag.set(&s, state)
				db := sql.OpenDB(coverageConnector{s})
				defer db.Close()
				cov, err := (&LiveMysql{db: db}).EncryptionCoverage(context.Background())
				if err != nil {
					t.Fatal(err)
				}
				raw, err := json.Marshal(cov)
				if err != nil {
					t.Fatal(err)
				}
				var wire map[string]any
				if err := json.Unmarshal(raw, &wire); err != nil {
					t.Fatal(err)
				}
				for _, other := range flags {
					got, present := wire[other.name]
					want := "true"
					if other.name == flag.name {
						want = state
					}
					switch {
					case want == "missing" && present:
						t.Errorf("%s: an unreported flag is on the wire as %v: %s", other.name, got, raw)
					case want != "missing" && (!present || fmt.Sprint(got) != want):
						t.Errorf("%s = %v (present %t), want %s: %s", other.name, got, present, want, raw)
					}
				}
				if wire["unencryptedTablespaces"] != float64(2) {
					t.Errorf("unencryptedTablespaces lost: %s", raw)
				}
			})
		}
	}
}
