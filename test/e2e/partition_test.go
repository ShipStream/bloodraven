package e2e

import (
	"context"
	"log/slog"
	"os"
	"testing"
	"time"

	"github.com/shipstream/bloodraven/internal/sidecar"
)

// mockSidecarFencer implements the fencer interface used by sidecar.FencingMonitor.
// Since the sidecar fencer interface is unexported, we test through the exported
// FencingMonitor API by using NewFencingMonitor with test HTTP servers.
//
// For these tests, we exercise the fencing decision matrix by:
// 1. Creating a FencingMonitor with unreachable addresses (no servers running).
// 2. Manipulating the internal lastBloodravenOK / lastPeerOK timestamps
//    is not possible from outside the package, so we use real HTTP servers
//    or test via the Run method with short intervals.
//
// However, the fencing tests in internal/sidecar/fencing_test.go already
// thoroughly test the evaluate() method using the package-internal access.
//
// These E2E tests verify the behaviour through the public interface by running
// the fencing monitor with real HTTP endpoints.

func sidecarTestLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelError}))
}

func TestPartition_SidecarSelfFences_WhenIsolated(t *testing.T) {
	// Start a fencing monitor pointing at non-existent addresses.
	// Both Bloodraven and peer will be unreachable.
	// With a very short lease timeout, the monitor should self-fence.

	// We need a MySQL endpoint that reports read_only=0 (primary).
	// We use a real FencingMonitor via its Run() method with short intervals.
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Create a mock MySQL sidecar server that returns read_only=0.
	mysqlSrv := newMockSidecarMySQL(false) // not read-only = primary

	fm := sidecar.NewFencingMonitor(
		mysqlSrv,
		"127.0.0.1:19999", // unreachable bloodraven
		"127.0.0.1:19998", // unreachable peer
		50*time.Millisecond,
		100*time.Millisecond, // very short lease timeout
		sidecarTestLogger(),
	)

	go fm.Run(ctx)

	// Wait for the monitor to detect both unreachable and self-fence.
	// Grace period on startup is leaseTimeout, plus a few check intervals.
	time.Sleep(300 * time.Millisecond)

	if !fm.IsFenced() {
		t.Error("sidecar should self-fence when both Bloodraven and peer are unreachable")
	}
	if !mysqlSrv.isSuperReadOnly() {
		t.Error("sidecar should set super_read_only=ON when self-fencing")
	}
}

func TestPartition_SidecarHoldsReady_WhenOnlyBloodravenDown(t *testing.T) {
	// Bloodraven unreachable, peer reachable.
	// The sidecar should NOT self-fence.

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	mysqlSrv := newMockSidecarMySQL(false) // primary

	// Start a real HTTP server for the peer.
	peerAddr := startMockHTTPServer(t, "/peer/ping")

	fm := sidecar.NewFencingMonitor(
		mysqlSrv,
		"127.0.0.1:19999", // unreachable bloodraven
		peerAddr,           // reachable peer
		50*time.Millisecond,
		100*time.Millisecond,
		sidecarTestLogger(),
	)

	go fm.Run(ctx)

	// Wait long enough for several checks.
	time.Sleep(300 * time.Millisecond)

	if fm.IsFenced() {
		t.Error("sidecar should NOT self-fence when peer is still reachable (Bloodraven maintenance)")
	}
}

func TestPartition_SidecarNeverUnfences(t *testing.T) {
	// After self-fencing, even if connectivity is restored, the sidecar
	// should NOT un-fence. Only Bloodraven can restore.

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	mysqlSrv := newMockSidecarMySQL(false) // primary

	fm := sidecar.NewFencingMonitor(
		mysqlSrv,
		"127.0.0.1:19999", // unreachable
		"127.0.0.1:19998", // unreachable
		50*time.Millisecond,
		100*time.Millisecond,
		sidecarTestLogger(),
	)

	go fm.Run(ctx)

	// Wait for self-fencing.
	time.Sleep(300 * time.Millisecond)

	if !fm.IsFenced() {
		t.Fatal("sidecar should have self-fenced")
	}

	// "Restore" connectivity by stopping and creating a new monitor
	// is not feasible. Instead, we just verify that IsFenced stays true
	// after more poll cycles (the fencing monitor never clears the fenced flag).
	time.Sleep(200 * time.Millisecond)

	if !fm.IsFenced() {
		t.Error("sidecar should remain fenced (never un-fences on its own)")
	}
}

func TestPartition_ReplicaDoesNotSelfFence(t *testing.T) {
	// A read-only replica should never self-fence, even if completely isolated.

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	mysqlSrv := newMockSidecarMySQL(true) // read-only = replica

	fm := sidecar.NewFencingMonitor(
		mysqlSrv,
		"127.0.0.1:19999", // unreachable
		"127.0.0.1:19998", // unreachable
		50*time.Millisecond,
		100*time.Millisecond,
		sidecarTestLogger(),
	)

	go fm.Run(ctx)
	time.Sleep(300 * time.Millisecond)

	if fm.IsFenced() {
		t.Error("replica should never self-fence")
	}
}
