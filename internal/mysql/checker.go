package mysql

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"fmt"
	"strconv"
	"strings"
	"time"

	mysqldriver "github.com/go-sql-driver/mysql"
)

// Checker checks MySQL read_only status and manages replication.
type Checker interface {
	CheckReadOnly(ctx context.Context) (readOnly bool, err error)
	Promote(ctx context.Context) error
	Close() error

	// Failover hardening methods:
	SetSuperReadOnly(ctx context.Context, on bool) error
	KillAppConnections(ctx context.Context) (killed int, err error)
	StopReplica(ctx context.Context) error
	ResetReplicaAll(ctx context.Context) error
	SetReadOnly(ctx context.Context, on bool) error
	ShowReplicaStatus(ctx context.Context) (*ReplicaStatus, error)
	ChangeReplicationSource(ctx context.Context, opts ReplicationSourceOpts) error
	StartReplica(ctx context.Context) error
	WaitForRelayLogDrain(ctx context.Context, timeout time.Duration) error

	// GTID methods:
	GetGtidExecuted(ctx context.Context) (string, error)

	// FlushBinaryLogs runs FLUSH BINARY LOGS: the server closes its active
	// binary log file and opens the next one, whose Previous_gtids header
	// is the server's gtid_executed at that moment. The statement is never
	// written to the binary log, adds no GTID, and works under
	// super_read_only; it needs the RELOAD privilege.
	//
	// It runs on a connection of its own, never on the primary pool and
	// never on a pinned Session, so a slow or cancelled flush cannot break
	// a pinned promotion session or hold a pooled connection a health probe
	// needs. Callers on the failover path bound it and carry on when it
	// fails. Cancelling ctx abandons the wait, not necessarily the
	// statement: the server may still rotate afterwards.
	FlushBinaryLogs(ctx context.Context) error

	// RotateBinaryLog is FlushBinaryLogs followed, on the same connection,
	// by a read of the binary log file the server writes now (SHOW BINARY
	// LOG STATUS). That is the file the FLUSH opened or, when the server
	// rotated again in between, a later one; every file before it is
	// sealed. The pre-reclone wait compares the name with the active file
	// of the site's sidecar's last scan (BinlogFileSeq), which tells a
	// scan that saw this rotation from one that read the index before it.
	//
	// An error means the rotation is unconfirmed, not that it did not
	// happen: the FLUSH may have run and only the read failed, or the
	// statement may still complete after a cancelled wait.
	RotateBinaryLog(ctx context.Context) (activeFile string, err error)

	// PinSession holds one server connection for statements that must all
	// reach the same server; see Session.
	PinSession(ctx context.Context) (Session, error)

	// Clone plugin methods:
	EnsureClonePlugin(ctx context.Context) error
	SetCloneDonorList(ctx context.Context, donor string) error
	CloneInstance(ctx context.Context, user, host, password string, useSSL bool, cloneTimeoutSec int) error
	// CloneStatus returns the recipient's most recent clone outcome, or nil
	// when the server has none (or no clone plugin tables).
	CloneStatus(ctx context.Context) (*CloneStatus, error)

	// EnsurePartialRevokes turns @@GLOBAL.partial_revokes ON if it is
	// OFF. A replica with it OFF stops its SQL thread on a replicated
	// partial revoke, so the operator confirms it on every site first.
	EnsurePartialRevokes(ctx context.Context) error
}

type checker struct {
	// db is the primary pool used by every probe and mutation: liveness
	// (CheckReadOnly), replica-status and GTID reads, promotion, fencing,
	// replication setup, and the relay-log-drain poll loop. It carries a hard
	// driver-level dial/read/write deadline (statusNetTimeout) so a soft
	// network partition can never park an operator query forever — see
	// statusNetTimeout for why that matters.
	db *sql.DB
	// cloneDB is a dedicated pool WITHOUT an I/O deadline, used only by
	// CloneInstance. CLONE INSTANCE is a single statement that legitimately
	// blocks for minutes while the donor dataset is copied, so it cannot share
	// db's short read deadline. It is pinned to a single connection so the
	// per-session net_read_timeout/net_write_timeout set just before the clone
	// apply to the connection that actually runs it.
	cloneDB *sql.DB
	// oneShot dials a connection that is used for one statement and then
	// closed (FlushBinaryLogs). It carries the primary pool's hard I/O
	// deadlines, and shares nothing with db or cloneDB.
	oneShot driver.Connector
}

// statusNetTimeout is the hard driver-level dial/read/write deadline applied to
// the primary pool. It is strictly less than the operator's per-probe context
// timeout (5s in the topology poller) so the driver's own socket deadline fires
// first and a probe returns a clean network error within budget.
//
// Why this exists (failover correctness, not tuning): the operator polls each
// site over pooled TCP connections — not only CheckReadOnly, but also
// ShowReplicaStatus/GetGtidExecuted (e.g. detectEmptySite, run every cycle) and
// the fencing mutations issued during a failover. When a deny-all NetworkPolicy
// or a stateful firewall (Calico/iptables conntrack, a security-group change)
// blackholes a connection, an in-flight read — or the handshake read of a fresh
// dial — blocks with no response. database/sql context cancellation does not
// reliably abort a read already parked on such a socket, so without a
// driver-level read deadline the call hangs indefinitely. Because Poll() waits
// for every site's probe, one hung call freezes the whole detection loop and no
// failover ever fires. A ReadTimeout makes go-sql-driver/mysql set a deadline
// on every socket read so the call always returns and the site trips the
// failure threshold. (CNIs that flush conntrack on policy change, e.g.
// k3s/kube-router, break the established flow and mask this — which is why it
// only surfaced on Calico.)
const statusNetTimeout = 4 * time.Second

// connMaxLifetime bounds how long a pooled connection is reused before the pool
// recycles it. This is connection hygiene (avoid indefinitely-old TCP flows and
// server-side session accumulation); it is NOT what guarantees a soft-
// partitioned primary is detected. A connection parked in a blocked read is
// never returned to the pool, so SetConnMaxLifetime cannot recycle it — the
// detection guarantee comes from statusNetTimeout instead.
const connMaxLifetime = 10 * time.Second

// NewChecker creates a checker for the given DSN. It opens two pools against the
// same server: the primary pool (db) with hard I/O deadlines used by every
// probe and mutation, and a separate unbounded clone pool (cloneDB) used only
// by the multi-minute CLONE INSTANCE statement. See the checker field docs and
// statusNetTimeout for why the split is required.
func NewChecker(dsn string) (Checker, error) {
	db, err := openPrimaryDB(dsn)
	if err != nil {
		return nil, err
	}
	cloneDB, err := openCloneDB(dsn)
	if err != nil {
		db.Close()
		return nil, err
	}
	oneShot, err := oneShotConnector(dsn)
	if err != nil {
		db.Close()
		cloneDB.Close()
		return nil, err
	}
	return &checker{db: db, cloneDB: cloneDB, oneShot: oneShot}, nil
}

// NewCheckerFromPools returns a Checker over pools the caller opened, such as
// pools on a fake database/sql driver in tests. The checker takes ownership
// and closes both in Close. NewChecker is the production constructor: it is
// what applies the primary pool's I/O deadlines and pins the clone pool to
// one connection, and callers of this one are responsible for equivalents.
// oneShot dials the connections FlushBinaryLogs uses, one per call.
func NewCheckerFromPools(db, cloneDB *sql.DB, oneShot driver.Connector) Checker {
	return &checker{db: db, cloneDB: cloneDB, oneShot: oneShot}
}

// oneShotConnector returns the connector for single-statement connections.
// Its DSN is the primary pool's (primaryDSN), so a dial, read or write on a
// blackholed address ends within statusNetTimeout whatever the caller's
// context does; the TLS configuration the DSN names is resolved here, once.
func oneShotConnector(dsn string) (driver.Connector, error) {
	pdsn, err := primaryDSN(dsn)
	if err != nil {
		return nil, err
	}
	cfg, err := ParseDSN(pdsn)
	if err != nil {
		return nil, fmt.Errorf("parse dsn: %w", err)
	}
	connector, err := mysqldriver.NewConnector(cfg)
	if err != nil {
		return nil, fmt.Errorf("open mysql connector: %w", err)
	}
	return connector, nil
}

// FlushBinaryLogs implements Checker. Each call dials its own connection
// (TCP, the MySQL handshake, TLS when the DSN has it, authentication:
// a few round trips to the site), runs the statement and closes it. The
// connection is not pooled: a statement abandoned on a cancelled context
// has its connection closed by the driver, and here that is the only
// thing the connection was for.
func (m *checker) FlushBinaryLogs(ctx context.Context) error {
	if m.oneShot == nil {
		return fmt.Errorf("flush binary logs: no one-shot connector configured")
	}
	db := sql.OpenDB(m.oneShot)
	db.SetMaxOpenConns(1)
	db.SetMaxIdleConns(0)
	defer db.Close()
	if _, err := db.ExecContext(ctx, "FLUSH BINARY LOGS"); err != nil {
		return fmt.Errorf("flush binary logs: %w", err)
	}
	return nil
}

// RotateBinaryLog implements Checker. Like FlushBinaryLogs it dials a
// connection of its own and closes it; both statements run on that one
// connection, so the name read is of the server that rotated.
func (m *checker) RotateBinaryLog(ctx context.Context) (string, error) {
	if m.oneShot == nil {
		return "", fmt.Errorf("rotate binary log: no one-shot connector configured")
	}
	db := sql.OpenDB(m.oneShot)
	db.SetMaxOpenConns(1)
	db.SetMaxIdleConns(0)
	defer db.Close()
	conn, err := db.Conn(ctx)
	if err != nil {
		return "", fmt.Errorf("rotate binary log: %w", err)
	}
	defer conn.Close()
	if _, err := conn.ExecContext(ctx, "FLUSH BINARY LOGS"); err != nil {
		return "", fmt.Errorf("rotate binary log: flush binary logs: %w", err)
	}
	file, err := activeBinlogFile(ctx, conn)
	if err != nil {
		return "", fmt.Errorf("rotate binary log: the flush was acknowledged, but the active file's name was not read: %w", err)
	}
	return file, nil
}

// activeBinlogFile reads the File column of SHOW BINARY LOG STATUS, by
// column name: the binary log file the server writes now.
func activeBinlogFile(ctx context.Context, conn *sql.Conn) (string, error) {
	pos, err := readBinlogPosition(ctx, conn)
	if pos.File != "" {
		// Only the name matters here.
		return pos.File, nil
	}
	return "", err
}

// BinlogPosition is where a server's binary log ends, with the executed
// GTID set it had at that point: one row of SHOW BINARY LOG STATUS, which
// the server fills under its log lock, so every transaction of
// GtidExecuted that this server logged is logged before File:Pos.
type BinlogPosition struct {
	File         string
	Pos          uint64
	GtidExecuted string
}

// ReadBinlogPosition reads the server's current binary log coordinates
// together with its executed GTID set (SHOW BINARY LOG STATUS; it needs
// the REPLICATION CLIENT privilege). The replica GTID audit uses it to
// tell a replica that is behind from one whose receiver dropped
// transactions (ReplicaStatus.ExecutedPast).
func (m *checker) ReadBinlogPosition(ctx context.Context) (BinlogPosition, error) {
	return readBinlogPosition(ctx, m.db)
}

// readBinlogPosition reads SHOW BINARY LOG STATUS by column name.
func readBinlogPosition(ctx context.Context, r sqlRunner) (BinlogPosition, error) {
	rows, err := r.QueryContext(ctx, "SHOW BINARY LOG STATUS")
	if err != nil {
		return BinlogPosition{}, fmt.Errorf("show binary log status: %w", err)
	}
	defer rows.Close()
	cols, err := rows.Columns()
	if err != nil {
		return BinlogPosition{}, fmt.Errorf("show binary log status: %w", err)
	}
	if !rows.Next() {
		if err := rows.Err(); err != nil {
			return BinlogPosition{}, fmt.Errorf("show binary log status: %w", err)
		}
		return BinlogPosition{}, fmt.Errorf("show binary log status returned no row; binary logging is off")
	}
	values := make([]sql.NullString, len(cols))
	dest := make([]any, len(cols))
	for i := range values {
		dest[i] = &values[i]
	}
	if err := rows.Scan(dest...); err != nil {
		return BinlogPosition{}, fmt.Errorf("show binary log status: %w", err)
	}
	var pos BinlogPosition
	hasFile := false
	var posErr error
	for i, col := range cols {
		switch {
		case strings.EqualFold(col, "File"):
			hasFile = true
			pos.File = strings.TrimSpace(values[i].String)
		case strings.EqualFold(col, "Position"):
			pos.Pos, posErr = strconv.ParseUint(strings.TrimSpace(values[i].String), 10, 64)
		case strings.EqualFold(col, "Executed_Gtid_Set"):
			pos.GtidExecuted = strings.TrimSpace(values[i].String)
		}
	}
	if !hasFile {
		return BinlogPosition{}, fmt.Errorf("show binary log status has no File column (columns: %s)", strings.Join(cols, ", "))
	}
	if pos.File == "" {
		return BinlogPosition{}, fmt.Errorf("show binary log status reports no file name")
	}
	if posErr != nil {
		// The file name is good for RotateBinaryLog; the position is not
		// used there. A caller that compares positions sees Pos 0 only
		// with this error.
		return BinlogPosition{File: pos.File}, fmt.Errorf("show binary log status: position: %w", posErr)
	}
	return pos, nil
}

// BinlogFileSeq splits a binary log file name into its base name and its
// numeric suffix ("mysql-bin.000042" is "mysql-bin", 42). The server
// numbers the files of one log in order, so within one base name a higher
// suffix is a later file; RESET BINARY LOGS AND GTIDS, or a new data
// directory, starts the numbering again. ok is false for a name that has no
// numeric suffix.
func BinlogFileSeq(name string) (base string, seq uint64, ok bool) {
	i := strings.LastIndexByte(name, '.')
	if i <= 0 || i == len(name)-1 {
		return "", 0, false
	}
	digits := name[i+1:]
	for _, r := range digits {
		if r < '0' || r > '9' {
			return "", 0, false
		}
	}
	seq, err := strconv.ParseUint(digits, 10, 64)
	if err != nil {
		return "", 0, false
	}
	return name[:i], seq, true
}

// primaryDSN derives the primary pool's DSN from the operational DSN by
// stamping a hard dial/read/write deadline (statusNetTimeout) onto it.
func primaryDSN(dsn string) (string, error) {
	cfg, err := ParseDSN(dsn)
	if err != nil {
		return "", fmt.Errorf("parse dsn: %w", err)
	}
	cfg.Timeout = statusNetTimeout
	cfg.ReadTimeout = statusNetTimeout
	cfg.WriteTimeout = statusNetTimeout
	return cfg.FormatDSN(), nil
}

// openPrimaryDB opens the primary pool, stamping a hard dial/read/write deadline
// (statusNetTimeout) onto the DSN so every probe and mutation is time-bounded.
func openPrimaryDB(dsn string) (*sql.DB, error) {
	pdsn, err := primaryDSN(dsn)
	if err != nil {
		return nil, err
	}
	db, err := sql.Open("mysql", pdsn)
	if err != nil {
		return nil, fmt.Errorf("open mysql: %w", err)
	}
	db.SetMaxOpenConns(2)
	db.SetMaxIdleConns(1)
	db.SetConnMaxIdleTime(30 * time.Second)
	db.SetConnMaxLifetime(connMaxLifetime)
	return db, nil
}

// openCloneDB opens the clone pool and pins it to a single connection.
// CloneInstance sets session-scoped net_read_timeout/net_write_timeout
// immediately before issuing CLONE INSTANCE; MaxOpenConns=1 guarantees those
// settings and the clone run on the same connection.
//
// The clone pool must NOT carry socket read/write deadlines: CLONE INSTANCE is a
// single statement that legitimately blocks for minutes, so any ReadTimeout /
// WriteTimeout inherited from a caller-supplied DSN (the operator's own DSNs
// never set them) is stripped. The initial dial must still
// be bounded so an unreachable host fails fast instead of wedging bootstrap, so
// an explicit caller dial timeout is preserved and a default supplied when the
// DSN sets none.
func cloneDSN(dsn string) (string, error) {
	cfg, err := ParseDSN(dsn)
	if err != nil {
		return "", fmt.Errorf("parse clone dsn: %w", err)
	}
	cfg.ReadTimeout = 0
	cfg.WriteTimeout = 0
	if cfg.Timeout == 0 {
		cfg.Timeout = statusNetTimeout
	}
	return cfg.FormatDSN(), nil
}

func openCloneDB(dsn string) (*sql.DB, error) {
	cdsn, err := cloneDSN(dsn)
	if err != nil {
		return nil, err
	}
	db, err := sql.Open("mysql", cdsn)
	if err != nil {
		return nil, fmt.Errorf("open clone mysql: %w", err)
	}
	db.SetMaxOpenConns(1)
	db.SetMaxIdleConns(1)
	// No ConnMaxLifetime: a clone holds its connection for minutes.
	return db, nil
}

func (m *checker) CheckReadOnly(ctx context.Context) (bool, error) {
	return checkReadOnly(ctx, m.db)
}

// CheckSuperReadOnly reads @@super_read_only. It distinguishes a fence
// (super_read_only=ON) from the read_only=ON mysqld boots with.
func (m *checker) CheckSuperReadOnly(ctx context.Context) (bool, error) {
	return checkSuperReadOnly(ctx, m.db)
}

// ReadGTIDSnapshot reads @@server_uuid and @@global.gtid_executed in one
// statement, so the set is known to belong to the server that reported
// the identity.
func (m *checker) ReadGTIDSnapshot(ctx context.Context) (GTIDSnapshot, error) {
	return readGTIDSnapshot(ctx, m.db)
}

// GTIDSnapshot is one server's identity and executed GTID set, read
// together.
type GTIDSnapshot struct {
	ServerUUID   string
	GtidExecuted string
}

func (m *checker) Promote(ctx context.Context) error {
	stmts := []string{
		"STOP REPLICA",
		"RESET REPLICA ALL",
		"SET GLOBAL read_only = 0",
	}
	for _, s := range stmts {
		if _, err := m.db.ExecContext(ctx, s); err != nil {
			return fmt.Errorf("promote (%s): %w", s, err)
		}
	}
	return nil
}

func (m *checker) Close() error {
	err := m.db.Close()
	if cerr := m.cloneDB.Close(); cerr != nil && err == nil {
		err = cerr
	}
	return err
}

// escapeSingleQuotes escapes single quotes for MySQL string literals
// used in statements that don't support parameterized queries (e.g. CLONE, CHANGE REPLICATION SOURCE).
func escapeSingleQuotes(s string) string {
	s = strings.ReplaceAll(s, "\\", "\\\\")
	return strings.ReplaceAll(s, "'", "''")
}
