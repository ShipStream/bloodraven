package sidecar

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strconv"
	"strings"

	mysqldriver "github.com/go-sql-driver/mysql"
)

// StatusInfo holds the MySQL status information returned by the sidecar.
type StatusInfo struct {
	Role                string `json:"role"`
	ReadOnly            bool   `json:"read_only"`
	SuperReadOnly       bool   `json:"super_read_only"`
	GtidExecuted        string `json:"gtid_executed"`
	ReplicaIORunning    bool   `json:"replica_io_running"`
	ReplicaSQLRunning   bool   `json:"replica_sql_running"`
	SecondsBehindSource *int64 `json:"seconds_behind_source"`
	ServerID            int    `json:"server_id"`
	Uptime              int64  `json:"uptime"`
}

// mysqlQuerier abstracts MySQL queries for testing.
type mysqlQuerier interface {
	queryStatus(ctx context.Context) (*StatusInfo, error)
	isConnectable(ctx context.Context) bool
	IsReadOnly(ctx context.Context) (bool, error)
	SetSuperReadOnly(ctx context.Context) error
	ClearSuperReadOnly(ctx context.Context) error
}

// LiveMysql queries the actual local MySQL instance.
// It implements mysqlQuerier and Fencer interfaces.
type LiveMysql struct {
	db *sql.DB
}

// NewLiveMysqlFromDSN creates a new LiveMysql from a DSN string.
func NewLiveMysqlFromDSN(dsn string) (*LiveMysql, error) {
	db, err := sql.Open("mysql", dsn)
	if err != nil {
		return nil, fmt.Errorf("open mysql: %w", err)
	}
	db.SetMaxOpenConns(3)
	db.SetMaxIdleConns(1)
	return &LiveMysql{db: db}, nil
}

func (m *LiveMysql) queryStatus(ctx context.Context) (*StatusInfo, error) {
	info := &StatusInfo{}

	// Query global variables
	var readOnly, superReadOnly int
	var serverID int
	var gtidExecuted string
	err := m.db.QueryRowContext(ctx,
		"SELECT @@read_only, @@super_read_only, @@server_id, @@global.gtid_executed",
	).Scan(&readOnly, &superReadOnly, &serverID, &gtidExecuted)
	if err != nil {
		return nil, fmt.Errorf("query global vars: %w", err)
	}

	info.ReadOnly = readOnly == 1
	info.SuperReadOnly = superReadOnly == 1
	info.ServerID = serverID
	info.GtidExecuted = strings.TrimSpace(gtidExecuted)

	if info.ReadOnly {
		info.Role = "replica"
	} else {
		info.Role = "primary"
	}

	// Query replica status
	rows, err := m.db.QueryContext(ctx, "SHOW REPLICA STATUS")
	if err != nil {
		return nil, fmt.Errorf("show replica status: %w", err)
	}
	defer rows.Close()

	if rows.Next() {
		cols, err := rows.Columns()
		if err != nil {
			return nil, fmt.Errorf("get replica status columns: %w", err)
		}

		// Build a map from column name -> value
		vals := make([]any, len(cols))
		ptrs := make([]any, len(cols))
		for i := range vals {
			ptrs[i] = &vals[i]
		}
		if err := rows.Scan(ptrs...); err != nil {
			return nil, fmt.Errorf("scan replica status: %w", err)
		}

		colMap := make(map[string]any, len(cols))
		for i, col := range cols {
			colMap[col] = vals[i]
		}

		if v, ok := colMap["Replica_IO_Running"]; ok {
			info.ReplicaIORunning = asString(v) == "Yes"
		}
		if v, ok := colMap["Replica_SQL_Running"]; ok {
			info.ReplicaSQLRunning = asString(v) == "Yes"
		}
		if v, ok := colMap["Seconds_Behind_Source"]; ok && v != nil {
			if sbs, err := asInt64(v); err == nil {
				info.SecondsBehindSource = &sbs
			}
		}
	}

	// Query uptime
	var uptimeStr string
	err = m.db.QueryRowContext(ctx,
		"SELECT VARIABLE_VALUE FROM performance_schema.global_status WHERE VARIABLE_NAME = 'Uptime'",
	).Scan(&uptimeStr)
	if err != nil {
		// Non-fatal: uptime query may fail if performance_schema is disabled
		info.Uptime = 0
	} else {
		info.Uptime, _ = strconv.ParseInt(uptimeStr, 10, 64)
	}

	return info, nil
}

func (m *LiveMysql) isConnectable(ctx context.Context) bool {
	return m.db.PingContext(ctx) == nil
}

func (m *LiveMysql) IsReadOnly(ctx context.Context) (bool, error) {
	var readOnly int
	err := m.db.QueryRowContext(ctx, "SELECT @@read_only").Scan(&readOnly)
	if err != nil {
		return false, fmt.Errorf("query read_only: %w", err)
	}
	return readOnly == 1, nil
}

func (m *LiveMysql) CheckSuperReadOnly(ctx context.Context) (bool, error) {
	var superReadOnly int
	err := m.db.QueryRowContext(ctx, "SELECT @@super_read_only").Scan(&superReadOnly)
	if err != nil {
		return false, fmt.Errorf("query super_read_only: %w", err)
	}
	return superReadOnly == 1, nil
}

func (m *LiveMysql) SetSuperReadOnly(ctx context.Context) error {
	_, err := m.db.ExecContext(ctx, "SET GLOBAL super_read_only = ON")
	if err != nil {
		return fmt.Errorf("set super_read_only: %w", err)
	}
	return nil
}

func (m *LiveMysql) ClearSuperReadOnly(ctx context.Context) error {
	if _, err := m.db.ExecContext(ctx, "SET GLOBAL super_read_only = OFF"); err != nil {
		return fmt.Errorf("clear super_read_only: %w", err)
	}
	if _, err := m.db.ExecContext(ctx, "SET GLOBAL read_only = OFF"); err != nil {
		return fmt.Errorf("clear read_only: %w", err)
	}
	return nil
}

// killableConnection reports whether a processlist row is an
// application session that a fence must boot.
//
// Server-internal threads are recognized by user, not by command. On a
// replica the replication I/O and applier threads run as `system user`
// with command `Connect`/`Query`, so a command-only filter selects them
// and the KILL stops replication on the very site being fenced. That is
// collateral damage with no safety value: super_read_only does not stop
// the applier, and replicating from the authoritative primary is exactly
// what a fenced site should keep doing. Losing the channel instead
// stalls the site until the operator's convergence invariant restarts it
// — and on a diverged site convergence is Blocked, so it never comes
// back on its own (issue #119).
//
// `Binlog Dump`/`Binlog Dump GTID` keep their own command-based
// exemption, unchanged from before: those are a fenced primary's
// outbound feeds to its replicas, they authenticate as the ordinary
// replication user rather than `system user`, and cutting them would
// strand peers short of the transactions this site committed before the
// fence — the opposite of what fencing is for.
//
// The operator's mysql.Checker.KillAppConnections deliberately does NOT
// share this filter. Its call sites want the applier gone: the bootstrap
// path kills connections so CLONE's DROP DATA phase is not blocked on
// open table handles. Do not "unify" the two.
func killableConnection(user, command string) bool {
	switch user {
	case "", "system user", "event_scheduler":
		return false
	}
	switch command {
	case "Binlog Dump", "Binlog Dump GTID", "Daemon":
		return false
	}
	return true
}

func (m *LiveMysql) KillConnections(ctx context.Context) (int, error) {
	rows, err := m.db.QueryContext(ctx,
		`SELECT id, COALESCE(user, ''), COALESCE(command, '')
		 FROM information_schema.processlist
		 WHERE id != CONNECTION_ID()`)
	if err != nil {
		return 0, fmt.Errorf("list connections: %w", err)
	}
	defer rows.Close()

	var targets []int64
	var unreadable int
	for rows.Next() {
		var id int64
		var user, command string
		if err := rows.Scan(&id, &user, &command); err != nil {
			// A row we cannot read is a session we cannot kill. Count it
			// so a partial fence is reported rather than silently passing
			// for a complete one.
			unreadable++
			continue
		}
		if killableConnection(user, command) {
			targets = append(targets, id)
		}
	}
	iterErr := rows.Err()
	// Release the pooled connection before issuing the KILLs, and never
	// KILL while rows are still streaming — the session feeding this very
	// result set is a candidate. Close is idempotent with the defer above.
	rows.Close()

	var killed, failed int
	for _, id := range targets {
		if _, err := m.db.ExecContext(ctx, fmt.Sprintf("KILL %d", id)); err != nil {
			// A session that ended on its own between the SELECT and the
			// KILL is the outcome the fence wanted, not a failure to
			// report. Sessions churn constantly, so counting these would
			// fire the partial-fence warning on nearly every fence and
			// bury the cases that matter (a privilege error, a connection
			// lost mid-batch).
			if isUnknownThread(err) {
				continue
			}
			failed++
			continue
		}
		killed++
	}

	return killed, evictionError(iterErr, unreadable, failed, len(targets))
}

// evictionError aggregates every reason an eviction pass was incomplete,
// and returns nil when it covered every session.
//
// Enumeration and eviction problems do not cancel the fence: the sessions
// that could be identified are evicted first, and this only reports the
// gap. The causes are independent — an iteration that ended early can
// coincide with rows that would not scan and KILLs that were refused — so
// they are joined rather than ranked. Reporting only the first would hide
// the rest.
func evictionError(iterErr error, unreadable, failed, targets int) error {
	var problems []error
	if iterErr != nil {
		problems = append(problems, fmt.Errorf("iterate connections: %w", iterErr))
	}
	if unreadable > 0 {
		problems = append(problems, fmt.Errorf("skipped %d unreadable processlist rows", unreadable))
	}
	if failed > 0 {
		problems = append(problems, fmt.Errorf("failed to kill %d of %d sessions", failed, targets))
	}
	return errors.Join(problems...)
}

// isUnknownThread reports whether err is MySQL's ER_NO_SUCH_THREAD, i.e.
// the session named by a KILL no longer exists.
func isUnknownThread(err error) bool {
	const erNoSuchThread = 1094
	var mysqlErr *mysqldriver.MySQLError
	return errors.As(err, &mysqlErr) && mysqlErr.Number == erNoSuchThread
}

// KeyringComponentStatus reads performance_schema.keyring_component_status,
// which is the authoritative view of whether a keyring component loaded
// and whether it is read-only. The table is a key/value shape, so the
// rows are folded into a struct here.
//
// An empty table means no keyring component is installed; that is
// returned as a zero-valued status rather than an error so the operator
// can distinguish "no keyring" from "cannot reach MySQL".
func (m *LiveMysql) KeyringComponentStatus(ctx context.Context) (*KeyringComponentStatus, error) {
	rows, err := m.db.QueryContext(ctx,
		"SELECT STATUS_KEY, STATUS_VALUE FROM performance_schema.keyring_component_status")
	if err != nil {
		return nil, fmt.Errorf("query keyring_component_status: %w", err)
	}
	defer rows.Close()

	out := &KeyringComponentStatus{}
	for rows.Next() {
		var k, v string
		if err := rows.Scan(&k, &v); err != nil {
			return nil, fmt.Errorf("scan keyring_component_status: %w", err)
		}
		switch k {
		case "Component_name":
			out.Name = v
		case "Component_status":
			out.Status = v
		case "Data_file":
			out.DataFile = v
		case "Read_only":
			out.ReadOnly = strings.EqualFold(v, "Yes")
		}
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate keyring_component_status: %w", err)
	}
	return out, nil
}

// EncryptionCoverage reports what is actually encrypted on this
// instance. It deliberately reads the live dictionary rather than
// trusting my.cnf: a site adopted from an unencrypted cluster can have
// every variable set correctly and still hold plaintext tables that were
// never rebuilt.
//
// The `mysql` system tablespace is called out separately because
// default_table_encryption does not cover it — it needs an explicit
// ALTER TABLESPACE, and leaving it plaintext exposes the data
// dictionary (schema, table, and column names) on disk.
func (m *LiveMysql) EncryptionCoverage(ctx context.Context) (*KeyringCoverage, error) {
	out := &KeyringCoverage{}

	var redo, undo, binlog sql.NullString
	if err := m.db.QueryRowContext(ctx,
		"SELECT @@innodb_redo_log_encrypt, @@innodb_undo_log_encrypt, @@binlog_encryption",
	).Scan(&redo, &undo, &binlog); err != nil {
		return nil, fmt.Errorf("query encryption variables: %w", err)
	}
	out.RedoLogEncrypted = mysqlBool(redo)
	out.UndoLogEncrypted = mysqlBool(undo)
	out.BinlogEncrypted = mysqlBool(binlog)

	var sysEnc string
	err := m.db.QueryRowContext(ctx,
		"SELECT ENCRYPTION FROM information_schema.INNODB_TABLESPACES WHERE NAME = 'mysql'",
	).Scan(&sysEnc)
	switch {
	case err == nil:
		out.SystemTablespaceEncrypted = strings.EqualFold(sysEnc, "Y")
	case errors.Is(err, sql.ErrNoRows):
		// No row means the general tablespace isn't present in this
		// deployment shape; leave it false rather than failing the whole
		// coverage sample.
	default:
		return nil, fmt.Errorf("query system tablespace encryption: %w", err)
	}

	// Count user tablespaces still reporting ENCRYPTION='N'. InnoDB
	// system/temporary tablespaces are excluded: they are not
	// file-per-table and are governed by the redo/undo settings above.
	if err := m.db.QueryRowContext(ctx,
		`SELECT COUNT(*) FROM information_schema.INNODB_TABLESPACES
		  WHERE ENCRYPTION = 'N'
		    AND SPACE_TYPE = 'Single'
		    AND NAME NOT LIKE 'mysql/%'
		    AND NAME NOT LIKE 'sys/%'`,
	).Scan(&out.UnencryptedTablespaces); err != nil {
		return nil, fmt.Errorf("count unencrypted tablespaces: %w", err)
	}

	return out, nil
}

func mysqlBool(value sql.NullString) bool {
	if !value.Valid {
		return false
	}
	s := strings.TrimSpace(value.String)
	return s == "1" || strings.EqualFold(s, "on") || strings.EqualFold(s, "true")
}

// RotateInnoDBMasterKey issues the master-key rotation. It only
// succeeds against a writable keyring: with the sealed (read_only)
// rendering MySQL rejects it with ER_CANNOT_FIND_KEY_IN_KEYRING, which
// is precisely the guardrail that stops an ad-hoc rotation from
// stranding data behind a key nobody escrowed.
func (m *LiveMysql) RotateInnoDBMasterKey(ctx context.Context) error {
	if _, err := m.db.ExecContext(ctx, "ALTER INSTANCE ROTATE INNODB MASTER KEY"); err != nil {
		return fmt.Errorf("alter instance rotate innodb master key: %w", err)
	}
	return nil
}

// EncryptSystemTablespace encrypts the `mysql` tablespace. It reuses the
// existing master key rather than creating one, so it is safe to run
// against a sealed, read-only keyring.
func (m *LiveMysql) EncryptSystemTablespace(ctx context.Context) error {
	if _, err := m.db.ExecContext(ctx, "ALTER TABLESPACE mysql ENCRYPTION='Y'"); err != nil {
		return fmt.Errorf("alter tablespace mysql encryption: %w", err)
	}
	return nil
}

// Close closes the underlying database connection.
func (m *LiveMysql) Close() error {
	return m.db.Close()
}

// asString converts a database value to string.
func asString(v any) string {
	if v == nil {
		return ""
	}
	switch val := v.(type) {
	case []byte:
		return string(val)
	case string:
		return val
	default:
		return fmt.Sprintf("%v", val)
	}
}

// asInt64 converts a database value to int64.
func asInt64(v any) (int64, error) {
	if v == nil {
		return 0, fmt.Errorf("nil value")
	}
	switch val := v.(type) {
	case []byte:
		return strconv.ParseInt(string(val), 10, 64)
	case string:
		return strconv.ParseInt(val, 10, 64)
	case int64:
		return val, nil
	case uint64:
		return int64(val), nil
	default:
		return 0, fmt.Errorf("unsupported type %T", v)
	}
}
