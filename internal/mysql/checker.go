package mysql

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
	"time"

	"github.com/go-sql-driver/mysql"
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
	StartReplicaSQLThread(ctx context.Context) error
	WaitForRelayLogDrain(ctx context.Context, timeout time.Duration) error

	// GTID methods:
	GetGtidExecuted(ctx context.Context) (string, error)

	// Clone plugin methods:
	EnsureClonePlugin(ctx context.Context) error
	SetCloneDonorList(ctx context.Context, donor string) error
	CloneInstance(ctx context.Context, user, host, password string, useSSL bool, cloneTimeoutSec int) error
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
	return &checker{db: db, cloneDB: cloneDB}, nil
}

// primaryDSN derives the primary pool's DSN from the operational DSN by
// stamping a hard dial/read/write deadline (statusNetTimeout) onto it.
func primaryDSN(dsn string) (string, error) {
	cfg, err := mysql.ParseDSN(dsn)
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
// WriteTimeout inherited from a caller-supplied DSN (credentials mode never sets
// them, but a raw `dsn` secret could) is stripped. The initial dial must still
// be bounded so an unreachable host fails fast instead of wedging bootstrap, so
// an explicit caller dial timeout is preserved and a default supplied when the
// DSN sets none.
func cloneDSN(dsn string) (string, error) {
	cfg, err := mysql.ParseDSN(dsn)
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
	var readOnly int
	err := m.db.QueryRowContext(ctx, "SELECT @@read_only").Scan(&readOnly)
	if err != nil {
		return false, fmt.Errorf("query read_only: %w", err)
	}
	return readOnly == 1, nil
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
