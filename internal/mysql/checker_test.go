package mysql

import (
	"testing"
	"time"

	"github.com/go-sql-driver/mysql"
)

// defaultLeaseTimeout mirrors api/v1alpha1 LeaseTimeout default (20s).
// Duplicated here as a literal to avoid a dependency from this low-level
// package onto the API types.
const defaultLeaseTimeout = 20 * time.Second

// pollContextTimeout mirrors the per-site poll context deadline in
// internal/controller/topology.go (Poll/pollSite). Duplicated as a literal so
// the bounded-pool invariant below can be asserted without importing the
// controller package.
const pollContextTimeout = 5 * time.Second

// TestStatusNetTimeoutBoundsPoll guards the failover-detection invariant: the
// primary pool's driver-level I/O deadline must fire strictly inside the poll
// context budget, and comfortably inside the sidecar lease window. Otherwise a
// soft network partition (a deny-all NetworkPolicy / stateful firewall that
// blackholes a connection) parks a probe forever, Poll()'s per-site wait never
// returns, and no failover is triggered.
func TestStatusNetTimeoutBoundsPoll(t *testing.T) {
	if statusNetTimeout <= 0 {
		t.Fatalf("statusNetTimeout must be positive, got %s", statusNetTimeout)
	}
	if statusNetTimeout >= pollContextTimeout {
		t.Fatalf("statusNetTimeout (%s) must be < pollContextTimeout (%s) so the driver's "+
			"socket deadline fires before the poll context and probes return within budget",
			statusNetTimeout, pollContextTimeout)
	}
	if statusNetTimeout >= defaultLeaseTimeout {
		t.Fatalf("statusNetTimeout (%s) must be well below LeaseTimeout (%s) so a soft "+
			"partition is detected before the lease expires", statusNetTimeout, defaultLeaseTimeout)
	}
}

// TestConnMaxLifetimeBelowLease keeps the pooled-connection recycle interval
// comfortably under the lease window. This is connection hygiene, not the
// detection guarantee (see TestStatusNetTimeoutBoundsPoll); a connection parked
// in a blocked read is never recycled.
func TestConnMaxLifetimeBelowLease(t *testing.T) {
	if connMaxLifetime <= 0 {
		t.Fatalf("connMaxLifetime must be positive, got %s", connMaxLifetime)
	}
	if connMaxLifetime >= defaultLeaseTimeout {
		t.Fatalf("connMaxLifetime (%s) must be below LeaseTimeout (%s)", connMaxLifetime, defaultLeaseTimeout)
	}
}

// TestPrimaryDSNStampsIOTimeouts verifies primaryDSN puts a hard
// dial/read/write deadline onto the primary pool's DSN, derived from the
// operational DSN. ReadTimeout is the load-bearing field: it is what makes
// go-sql-driver/mysql abort a read blocked on a blackholed socket.
func TestPrimaryDSNStampsIOTimeouts(t *testing.T) {
	// Operational DSN with only a dial timeout, as buildSiteDSNFromCreds emits.
	pdsn, err := primaryDSN("user:pass@tcp(127.0.0.1:3306)/?timeout=5s")
	if err != nil {
		t.Fatalf("primaryDSN returned error: %v", err)
	}
	cfg, err := mysql.ParseDSN(pdsn)
	if err != nil {
		t.Fatalf("ParseDSN(primary dsn) returned error: %v", err)
	}
	if cfg.ReadTimeout != statusNetTimeout {
		t.Errorf("primary ReadTimeout = %s, want %s", cfg.ReadTimeout, statusNetTimeout)
	}
	if cfg.WriteTimeout != statusNetTimeout {
		t.Errorf("primary WriteTimeout = %s, want %s", cfg.WriteTimeout, statusNetTimeout)
	}
	if cfg.Timeout != statusNetTimeout {
		t.Errorf("primary dial Timeout = %s, want %s", cfg.Timeout, statusNetTimeout)
	}
}

// TestPrimaryDSNRejectsGarbage ensures NewChecker surfaces an error (rather than
// panicking or silently dropping the read deadline) when handed an unparseable
// DSN.
func TestPrimaryDSNRejectsGarbage(t *testing.T) {
	if _, err := primaryDSN("::::not-a-dsn::::"); err == nil {
		t.Fatal("primaryDSN accepted an unparseable DSN, want error")
	}
}

// TestCloneDSNStripsIOTimeoutsAndBoundsDial verifies cloneDSN removes any
// read/write deadline inherited from a caller-supplied DSN — CLONE INSTANCE
// legitimately blocks for minutes — while keeping the dial bounded: an explicit
// caller dial timeout is preserved, and a bounded default is supplied when the
// DSN sets none so an unreachable host cannot wedge bootstrap.
func TestCloneDSNStripsIOTimeoutsAndBoundsDial(t *testing.T) {
	// A DSN carrying read/write deadlines plus an explicit dial timeout.
	cdsn, err := cloneDSN("user:pass@tcp(127.0.0.1:3306)/?timeout=5s&readTimeout=10s&writeTimeout=10s")
	if err != nil {
		t.Fatalf("cloneDSN returned error: %v", err)
	}
	cfg, err := mysql.ParseDSN(cdsn)
	if err != nil {
		t.Fatalf("ParseDSN(clone dsn) returned error: %v", err)
	}
	if cfg.ReadTimeout != 0 {
		t.Errorf("clone ReadTimeout = %s, want 0 (CLONE INSTANCE blocks for minutes)", cfg.ReadTimeout)
	}
	if cfg.WriteTimeout != 0 {
		t.Errorf("clone WriteTimeout = %s, want 0", cfg.WriteTimeout)
	}
	if cfg.Timeout != 5*time.Second {
		t.Errorf("clone dial Timeout = %s, want 5s (explicit caller value preserved)", cfg.Timeout)
	}

	// A DSN with no dial timeout gets a bounded default.
	cdsn2, err := cloneDSN("user:pass@tcp(127.0.0.1:3306)/")
	if err != nil {
		t.Fatalf("cloneDSN (no timeout) returned error: %v", err)
	}
	cfg2, err := mysql.ParseDSN(cdsn2)
	if err != nil {
		t.Fatalf("ParseDSN returned error: %v", err)
	}
	if cfg2.Timeout != statusNetTimeout {
		t.Errorf("clone dial Timeout = %s, want %s (bounded default when DSN sets none)", cfg2.Timeout, statusNetTimeout)
	}
	if cfg2.ReadTimeout != 0 || cfg2.WriteTimeout != 0 {
		t.Errorf("clone R/W timeouts must be 0, got read=%s write=%s", cfg2.ReadTimeout, cfg2.WriteTimeout)
	}
}

// TestNewCheckerConfiguresPools verifies NewChecker builds a usable checker with
// both pools wired (sql.Open is lazy, so no server is needed): the primary pool
// carries the read deadline, the clone pool deliberately does not, and Close
// closes both.
func TestNewCheckerConfiguresPools(t *testing.T) {
	c, err := NewChecker("user:pass@tcp(127.0.0.1:3306)/?timeout=5s")
	if err != nil {
		t.Fatalf("NewChecker returned error: %v", err)
	}
	if c == nil {
		t.Fatal("NewChecker returned nil checker")
	}
	impl, ok := c.(*checker)
	if !ok {
		t.Fatalf("NewChecker returned %T, want *checker", c)
	}
	if impl.db == nil || impl.cloneDB == nil {
		t.Fatalf("checker pools not both initialized: db=%v cloneDB=%v", impl.db != nil, impl.cloneDB != nil)
	}
	if err := c.Close(); err != nil {
		t.Fatalf("Close returned error: %v", err)
	}
}
