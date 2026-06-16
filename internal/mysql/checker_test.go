package mysql

import (
	"testing"
	"time"
)

// defaultLeaseTimeout mirrors api/v1alpha1 LeaseTimeout default (20s).
// Duplicated here as a literal to avoid a dependency from this low-level
// package onto the API types.
const defaultLeaseTimeout = 20 * time.Second

// TestConnMaxLifetimeBelowLease guards the failover-correctness invariant
// behind SetConnMaxLifetime in NewChecker: the pooled connection must be
// recycled comfortably before the sidecar lease expires, otherwise the
// operator can ride a stale-but-firewall-kept-alive connection across a
// soft network partition and never detect the dead primary.
func TestConnMaxLifetimeBelowLease(t *testing.T) {
	if connMaxLifetime <= 0 {
		t.Fatalf("connMaxLifetime must be positive, got %s", connMaxLifetime)
	}
	if connMaxLifetime >= defaultLeaseTimeout {
		t.Fatalf("connMaxLifetime (%s) must be well below LeaseTimeout (%s) so a soft "+
			"partition is detected before the lease expires", connMaxLifetime, defaultLeaseTimeout)
	}
}

// TestNewCheckerConfiguresPool verifies NewChecker builds a usable checker
// without dialing (sql.Open is lazy) and that Close is safe.
func TestNewCheckerConfiguresPool(t *testing.T) {
	c, err := NewChecker("user:pass@tcp(127.0.0.1:3306)/?timeout=5s")
	if err != nil {
		t.Fatalf("NewChecker returned error: %v", err)
	}
	if c == nil {
		t.Fatal("NewChecker returned nil checker")
	}
	if err := c.Close(); err != nil {
		t.Fatalf("Close returned error: %v", err)
	}
}
