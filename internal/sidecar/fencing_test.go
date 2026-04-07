package sidecar

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"testing"
	"time"
)

// mockFencer implements the Fencer interface for testing.
type mockFencer struct {
	readOnly       bool
	readOnlyErr    error
	superReadOnly  bool
	setReadOnlyCh  chan struct{} // closed on SetSuperReadOnly call
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

func testLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelError}))
}

func TestEvaluateDoesNothingWhenBothReachable(t *testing.T) {
	f := newMockFencer(false) // primary (not read-only)
	fm := NewFencingMonitor(f, "bloodraven:8081", "peer:8080", 5*time.Second, 20*time.Second, testLogger())

	// Both recently OK
	fm.lastBloodravenOK = time.Now()
	fm.lastPeerOK = time.Now()

	fm.evaluate(context.Background())

	if fm.fenced {
		t.Error("should not fence when both are reachable")
	}
	if f.superReadOnly {
		t.Error("should not set super_read_only when both are reachable")
	}
}

func TestEvaluateDoesNothingWhenOnlyBloodravenDown(t *testing.T) {
	f := newMockFencer(false) // primary
	fm := NewFencingMonitor(f, "bloodraven:8081", "peer:8080", 5*time.Second, 20*time.Second, testLogger())

	// Bloodraven down past timeout, peer OK
	fm.lastBloodravenOK = time.Now().Add(-30 * time.Second)
	fm.lastPeerOK = time.Now()

	fm.evaluate(context.Background())

	if fm.fenced {
		t.Error("should not fence when only Bloodraven is down (hold steady)")
	}
	if f.superReadOnly {
		t.Error("should not set super_read_only when only Bloodraven is down")
	}
}

func TestEvaluateDoesNothingWhenOnlyPeerDown(t *testing.T) {
	f := newMockFencer(false) // primary
	fm := NewFencingMonitor(f, "bloodraven:8081", "peer:8080", 5*time.Second, 20*time.Second, testLogger())

	// Bloodraven OK, peer down past timeout
	fm.lastBloodravenOK = time.Now()
	fm.lastPeerOK = time.Now().Add(-30 * time.Second)

	fm.evaluate(context.Background())

	if fm.fenced {
		t.Error("should not fence when only peer is down (Bloodraven handles it)")
	}
	if f.superReadOnly {
		t.Error("should not set super_read_only when only peer is down")
	}
}

func TestEvaluateFencesWhenBothUnreachablePastTimeout(t *testing.T) {
	f := newMockFencer(false) // primary
	fm := NewFencingMonitor(f, "bloodraven:8081", "peer:8080", 5*time.Second, 20*time.Second, testLogger())

	// Both down past timeout
	fm.lastBloodravenOK = time.Now().Add(-30 * time.Second)
	fm.lastPeerOK = time.Now().Add(-30 * time.Second)

	fm.evaluate(context.Background())

	if !fm.fenced {
		t.Error("should fence when both are unreachable past timeout")
	}
	if !f.superReadOnly {
		t.Error("should set super_read_only when both are unreachable past timeout")
	}
}

func TestEvaluateDoesNotFenceWhenBothDownButWithinTimeout(t *testing.T) {
	f := newMockFencer(false) // primary
	fm := NewFencingMonitor(f, "bloodraven:8081", "peer:8080", 5*time.Second, 20*time.Second, testLogger())

	// Both down but within timeout
	fm.lastBloodravenOK = time.Now().Add(-10 * time.Second)
	fm.lastPeerOK = time.Now().Add(-10 * time.Second)

	fm.evaluate(context.Background())

	if fm.fenced {
		t.Error("should not fence when both are unreachable but within timeout")
	}
	if f.superReadOnly {
		t.Error("should not set super_read_only when within timeout")
	}
}

func TestEvaluateDoesNotFenceWhenAlreadyReadOnly(t *testing.T) {
	f := newMockFencer(true) // replica (read-only)
	fm := NewFencingMonitor(f, "bloodraven:8081", "peer:8080", 5*time.Second, 20*time.Second, testLogger())

	// Both down past timeout
	fm.lastBloodravenOK = time.Now().Add(-30 * time.Second)
	fm.lastPeerOK = time.Now().Add(-30 * time.Second)

	fm.evaluate(context.Background())

	if fm.fenced {
		t.Error("should not fence a replica (already read-only)")
	}
	if f.superReadOnly {
		t.Error("should not set super_read_only on a replica")
	}
}

func TestEvaluateDoesNotReFenceWhenAlreadyFenced(t *testing.T) {
	f := newMockFencer(false) // primary
	fm := NewFencingMonitor(f, "bloodraven:8081", "peer:8080", 5*time.Second, 20*time.Second, testLogger())

	// Both down past timeout
	fm.lastBloodravenOK = time.Now().Add(-30 * time.Second)
	fm.lastPeerOK = time.Now().Add(-30 * time.Second)

	// First fence
	fm.evaluate(context.Background())
	if !fm.fenced {
		t.Fatal("should have fenced")
	}

	// Reset the mock to track second call
	f.superReadOnly = false

	// Evaluate again
	fm.evaluate(context.Background())

	if f.superReadOnly {
		t.Error("should not re-fence when already fenced (setSuperReadOnly should not be called again)")
	}
}

func TestFencingExecutesSetSuperReadOnly(t *testing.T) {
	f := newMockFencer(false) // primary
	fm := NewFencingMonitor(f, "bloodraven:8081", "peer:8080", 5*time.Second, 20*time.Second, testLogger())

	fm.lastBloodravenOK = time.Now().Add(-30 * time.Second)
	fm.lastPeerOK = time.Now().Add(-30 * time.Second)

	fm.evaluate(context.Background())

	if !f.superReadOnly {
		t.Error("fencing should execute SET GLOBAL super_read_only=ON")
	}
}

func TestEvaluateHandlesReadOnlyError(t *testing.T) {
	f := newMockFencer(false)
	f.readOnlyErr = fmt.Errorf("connection refused")
	fm := NewFencingMonitor(f, "bloodraven:8081", "peer:8080", 5*time.Second, 20*time.Second, testLogger())

	fm.lastBloodravenOK = time.Now().Add(-30 * time.Second)
	fm.lastPeerOK = time.Now().Add(-30 * time.Second)

	fm.evaluate(context.Background())

	if fm.fenced {
		t.Error("should not fence when read_only check fails")
	}
	if f.superReadOnly {
		t.Error("should not set super_read_only when read_only check fails")
	}
}
