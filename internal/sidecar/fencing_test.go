package sidecar

import (
	"context"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"testing"
	"time"

	"github.com/shipstream/bloodraven/internal/clock"
)

// mockFencer implements the Fencer interface for testing.
type mockFencer struct {
	readOnly      bool
	readOnlyErr   error
	superReadOnly bool
	setReadOnlyCh chan struct{} // closed on SetSuperReadOnly call
}

func newMockFencer(readOnly bool) *mockFencer {
	return &mockFencer{
		readOnly:      readOnly,
		setReadOnlyCh: make(chan struct{}, 1),
	}
}

func (m *mockFencer) IsReadOnly(_ context.Context) (bool, error) {
	if m.readOnlyErr != nil {
		return false, m.readOnlyErr
	}
	return m.readOnly, nil
}

func (m *mockFencer) SetSuperReadOnly(_ context.Context) error {
	m.superReadOnly = true
	select {
	case m.setReadOnlyCh <- struct{}{}:
	default:
	}
	return nil
}

func (m *mockFencer) KillConnections(_ context.Context) (int, error) {
	return 0, nil
}

func testLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelError}))
}

const testPeerAddr = "127.0.0.1:8080"

// newTestFencingMonitor creates a FencingMonitor with a FakeClock and a stub
// transport for deterministic, socket-free testing.
func newTestFencingMonitor(f Fencer, clk *clock.FakeClock) *FencingMonitor {
	client := &http.Client{Transport: noopTransport{}}
	return NewFencingMonitorFull(f, "127.0.0.1:8081", []string{testPeerAddr}, 5*time.Second, 20*time.Second, testLogger(), clk, client)
}

// setPeerLastOK overwrites the last-seen time for every peer in fm,
// mirroring the old fm.lastPeerOK = t assignment so existing tests can
// express "every peer is fresh" or "every peer is stale" in one line.
// It seeds entries for every configured peer, overriding any zero-time
// initialisation from construction.
func setPeerLastOK(fm *FencingMonitor, t time.Time) {
	for _, addr := range fm.peerAddrs {
		fm.lastPeerOK[addr] = t
	}
}

func TestEvaluateDoesNothingWhenBothReachable(t *testing.T) {
	start := time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC)
	clk := clock.NewFakeClock(start)
	f := newMockFencer(false) // primary (not read-only)
	fm := newTestFencingMonitor(f, clk)

	fm.lastBloodravenOK = clk.Now()
	setPeerLastOK(fm, clk.Now())

	fm.evaluate(context.Background())

	if fm.fenced {
		t.Error("should not fence when both are reachable")
	}
	if f.superReadOnly {
		t.Error("should not set super_read_only when both are reachable")
	}
}

func TestEvaluateDoesNothingWhenOnlyBloodravenDown(t *testing.T) {
	start := time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC)
	clk := clock.NewFakeClock(start)
	f := newMockFencer(false) // primary
	fm := newTestFencingMonitor(f, clk)

	fm.lastBloodravenOK = clk.Now().Add(-30 * time.Second)
	setPeerLastOK(fm, clk.Now())

	fm.evaluate(context.Background())

	if fm.fenced {
		t.Error("should not fence when only Bloodraven is down (hold steady)")
	}
	if f.superReadOnly {
		t.Error("should not set super_read_only when only Bloodraven is down")
	}
}

func TestEvaluateDoesNothingWhenOnlyPeerDown(t *testing.T) {
	start := time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC)
	clk := clock.NewFakeClock(start)
	f := newMockFencer(false) // primary
	fm := newTestFencingMonitor(f, clk)

	fm.lastBloodravenOK = clk.Now()
	setPeerLastOK(fm, clk.Now().Add(-30*time.Second))

	fm.evaluate(context.Background())

	if fm.fenced {
		t.Error("should not fence when only peer is down (Bloodraven handles it)")
	}
	if f.superReadOnly {
		t.Error("should not set super_read_only when only peer is down")
	}
}

func TestEvaluateFencesWhenBothUnreachablePastTimeout(t *testing.T) {
	start := time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC)
	clk := clock.NewFakeClock(start)
	f := newMockFencer(false) // primary
	fm := newTestFencingMonitor(f, clk)

	fm.lastBloodravenOK = clk.Now().Add(-30 * time.Second)
	setPeerLastOK(fm, clk.Now().Add(-30*time.Second))

	fm.evaluate(context.Background())

	if !fm.fenced {
		t.Error("should fence when both are unreachable past timeout")
	}
	if !f.superReadOnly {
		t.Error("should set super_read_only when both are unreachable past timeout")
	}
}

func TestEvaluateDoesNotFenceWhenBothDownButWithinTimeout(t *testing.T) {
	start := time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC)
	clk := clock.NewFakeClock(start)
	f := newMockFencer(false) // primary
	fm := newTestFencingMonitor(f, clk)

	fm.lastBloodravenOK = clk.Now().Add(-10 * time.Second)
	setPeerLastOK(fm, clk.Now().Add(-10*time.Second))

	fm.evaluate(context.Background())

	if fm.fenced {
		t.Error("should not fence when both are unreachable but within timeout")
	}
	if f.superReadOnly {
		t.Error("should not set super_read_only when within timeout")
	}
}

func TestEvaluateDoesNotFenceWhenAlreadyReadOnly(t *testing.T) {
	start := time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC)
	clk := clock.NewFakeClock(start)
	f := newMockFencer(true) // replica (read-only)
	fm := newTestFencingMonitor(f, clk)

	fm.lastBloodravenOK = clk.Now().Add(-30 * time.Second)
	setPeerLastOK(fm, clk.Now().Add(-30*time.Second))

	fm.evaluate(context.Background())

	if fm.fenced {
		t.Error("should not fence a replica (already read-only)")
	}
	if f.superReadOnly {
		t.Error("should not set super_read_only on a replica")
	}
}

func TestEvaluateDoesNotReFenceWhenAlreadyFenced(t *testing.T) {
	start := time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC)
	clk := clock.NewFakeClock(start)
	f := newMockFencer(false) // primary
	fm := newTestFencingMonitor(f, clk)

	fm.lastBloodravenOK = clk.Now().Add(-30 * time.Second)
	setPeerLastOK(fm, clk.Now().Add(-30*time.Second))

	fm.evaluate(context.Background())
	if !fm.fenced {
		t.Fatal("should have fenced")
	}

	f.superReadOnly = false

	fm.evaluate(context.Background())

	if f.superReadOnly {
		t.Error("should not re-fence when already fenced (setSuperReadOnly should not be called again)")
	}
}

func TestFencingExecutesSetSuperReadOnly(t *testing.T) {
	start := time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC)
	clk := clock.NewFakeClock(start)
	f := newMockFencer(false) // primary
	fm := newTestFencingMonitor(f, clk)

	fm.lastBloodravenOK = clk.Now().Add(-30 * time.Second)
	setPeerLastOK(fm, clk.Now().Add(-30*time.Second))

	fm.evaluate(context.Background())

	if !f.superReadOnly {
		t.Error("fencing should execute SET GLOBAL super_read_only=ON")
	}
}

func TestEvaluateHandlesReadOnlyError(t *testing.T) {
	start := time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC)
	clk := clock.NewFakeClock(start)
	f := newMockFencer(false)
	f.readOnlyErr = fmt.Errorf("connection refused")
	fm := newTestFencingMonitor(f, clk)

	fm.lastBloodravenOK = clk.Now().Add(-30 * time.Second)
	setPeerLastOK(fm, clk.Now().Add(-30*time.Second))

	fm.evaluate(context.Background())

	if fm.fenced {
		t.Error("should not fence when read_only check fails")
	}
	if f.superReadOnly {
		t.Error("should not set super_read_only when read_only check fails")
	}
}

// TestCheckStepFunction verifies the exported Check() method works for
// deterministic step-driven testing without Run().
func TestCheckStepFunction(t *testing.T) {
	start := time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC)
	clk := clock.NewFakeClock(start)
	f := newMockFencer(false) // primary
	fm := newTestFencingMonitor(f, clk)

	fm.lastBloodravenOK = clk.Now()
	setPeerLastOK(fm, clk.Now())

	fm.Check(context.Background())
	if fm.fenced {
		t.Error("should not fence when both reachable")
	}

	clk.Advance(30 * time.Second)
	fm.Check(context.Background())

	if !fm.fenced {
		t.Error("should fence after clock advances past lease timeout with both unreachable")
	}
}

// TestEvaluateRequiresAllPeersDown verifies the N-site quorum rule:
// self-fencing requires the operator AND every peer to be silent.
func TestEvaluateRequiresAllPeersDown(t *testing.T) {
	start := time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC)
	clk := clock.NewFakeClock(start)
	f := newMockFencer(false)
	client := &http.Client{Transport: noopTransport{}}
	fm := NewFencingMonitorFull(f, "127.0.0.1:8081",
		[]string{"peer-a:8080", "peer-b:8080", "peer-c:8080"},
		5*time.Second, 20*time.Second, testLogger(), clk, client)

	fm.lastBloodravenOK = clk.Now().Add(-30 * time.Second)
	for addr := range fm.lastPeerOK {
		fm.lastPeerOK[addr] = clk.Now().Add(-30 * time.Second)
	}
	// One peer is still alive.
	fm.lastPeerOK["peer-c:8080"] = clk.Now()

	fm.evaluate(context.Background())

	if fm.fenced {
		t.Fatal("should not self-fence when at least one peer is still reachable")
	}
}
