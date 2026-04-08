//go:build integration

package e2e

import (
	"context"
	"log/slog"
	"os"
	"testing"
	"time"

	"github.com/shipstream/bloodraven/internal/sidecar"
)

func sidecarTestLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelError}))
}

// waitForFenced polls fm.IsFenced() until it returns the expected value or times out.
func waitForFenced(t *testing.T, fm *sidecar.FencingMonitor, want bool, timeout time.Duration) {
	t.Helper()
	deadline := time.After(timeout)
	for fm.IsFenced() != want {
		select {
		case <-deadline:
			t.Fatalf("timed out waiting for IsFenced()=%v", want)
		default:
			time.Sleep(10 * time.Millisecond)
		}
	}
}

func TestPartition_SidecarSelfFences_WhenIsolated(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

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
	waitForFenced(t, fm, true, 2*time.Second)

	if !mysqlSrv.isSuperReadOnly() {
		t.Error("sidecar should set super_read_only=ON when self-fencing")
	}
}

func TestPartition_SidecarHoldsReady_WhenOnlyBloodravenDown(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	mysqlSrv := newMockSidecarMySQL(false) // primary

	peerAddr := startMockHTTPServer(t, "/peer/ping")

	fm := sidecar.NewFencingMonitor(
		mysqlSrv,
		"127.0.0.1:19999", // unreachable bloodraven
		peerAddr,          // reachable peer
		50*time.Millisecond,
		100*time.Millisecond,
		sidecarTestLogger(),
	)

	go fm.Run(ctx)

	// Give the monitor enough time to run several check cycles.
	// With peer reachable, it should NOT self-fence.
	time.Sleep(300 * time.Millisecond)

	if fm.IsFenced() {
		t.Error("sidecar should NOT self-fence when peer is still reachable (Bloodraven maintenance)")
	}
}

func TestPartition_SidecarNeverUnfences(t *testing.T) {
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
	waitForFenced(t, fm, true, 2*time.Second)

	// Verify that IsFenced stays true after more poll cycles
	// (the fencing monitor never clears the fenced flag).
	time.Sleep(200 * time.Millisecond)

	if !fm.IsFenced() {
		t.Error("sidecar should remain fenced (never un-fences on its own)")
	}
}

func TestPartition_ReplicaDoesNotSelfFence(t *testing.T) {
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

	// Give several check cycles; replica should never self-fence.
	time.Sleep(300 * time.Millisecond)

	if fm.IsFenced() {
		t.Error("replica should never self-fence")
	}
}
