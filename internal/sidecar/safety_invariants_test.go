package sidecar

import (
	"context"
	"net/http"
	"testing"
	"time"

	"github.com/shipstream/bloodraven/internal/clock"
)

// ---------------------------------------------------------------------------
// Safety Invariant Tests for FencingMonitor (Testing 2.0)
//
// These tests exercise the actual FencingMonitor code path, not just
// the topology manager. They verify the fencing monitor's self-fencing
// decisions with full transport isolation (no real HTTP).
// ---------------------------------------------------------------------------

// noopTransport always returns an error, simulating unreachable endpoints
// without opening any network connections.
type noopTransport struct{}

func (noopTransport) RoundTrip(_ *http.Request) (*http.Response, error) {
	return nil, context.DeadlineExceeded
}

// okTransport always returns 200 OK.
type okTransport struct{}

func (okTransport) RoundTrip(_ *http.Request) (*http.Response, error) {
	return &http.Response{
		StatusCode: http.StatusOK,
		Body:       http.NoBody,
	}, nil
}

// newIsolatedFencingMonitor creates a FencingMonitor with injected transport
// and clock for fully deterministic, socket-free testing.
func newIsolatedFencingMonitor(f Fencer, clk *clock.FakeClock, transport http.RoundTripper) *FencingMonitor {
	client := &http.Client{Transport: transport}
	return NewFencingMonitorFull(f, "bloodraven:8081", "peer:8080", 5*time.Second, 20*time.Second, testLogger(), clk, client)
}

// INVARIANT: Never self-fence a replica.
// Exercises actual FencingMonitor.Check() with transport isolation.
func TestFencingInvariant_NeverSelfFenceReplica(t *testing.T) {
	start := time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC)
	clk := clock.NewFakeClock(start)
	f := newMockFencer(true) // replica (read-only)
	fm := newIsolatedFencingMonitor(f, clk, noopTransport{})

	// Initialize times
	fm.lastBloodravenOK = clk.Now()
	fm.lastPeerOK = clk.Now()

	// Advance past lease timeout so both endpoints are "down"
	clk.Advance(30 * time.Second)

	// Run full Check cycle (checkBloodraven + checkPeer + evaluate)
	fm.Check(context.Background())

	if fm.fenced {
		t.Error("SAFETY VIOLATION: FencingMonitor self-fenced a replica")
	}
	if f.superReadOnly {
		t.Error("SAFETY VIOLATION: SetSuperReadOnly called on a replica")
	}
}

// INVARIANT: Never auto-unfence after self-fencing.
// Once fenced, the monitor must stay fenced permanently.
func TestFencingInvariant_NeverAutoUnfence(t *testing.T) {
	start := time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC)
	clk := clock.NewFakeClock(start)
	f := newMockFencer(false) // primary
	fm := newIsolatedFencingMonitor(f, clk, noopTransport{})

	// Initialize and advance past timeout
	fm.lastBloodravenOK = clk.Now()
	fm.lastPeerOK = clk.Now()
	clk.Advance(30 * time.Second)

	// First check: should fence
	fm.Check(context.Background())
	if !fm.fenced {
		t.Fatal("should have self-fenced")
	}

	// Now switch transport to "everything reachable"
	fm.httpClient = &http.Client{Transport: okTransport{}}

	// Multiple checks with both endpoints reachable
	for i := 0; i < 10; i++ {
		clk.Advance(5 * time.Second)
		fm.Check(context.Background())
	}

	// Must still be fenced — only Bloodraven can restore
	if !fm.fenced {
		t.Error("SAFETY VIOLATION: FencingMonitor auto-unfenced after self-fencing")
	}
}

// INVARIANT: Self-fence only when BOTH bloodraven AND peer are down past timeout.
func TestFencingInvariant_RequiresBothDown(t *testing.T) {
	start := time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC)
	clk := clock.NewFakeClock(start)
	f := newMockFencer(false) // primary

	// Transport where bloodraven is reachable but peer is not.
	// Use okTransport for the Check and manually manage lastPeerOK.
	fm := newIsolatedFencingMonitor(f, clk, okTransport{})
	fm.lastBloodravenOK = clk.Now()
	fm.lastPeerOK = clk.Now()

	// Advance past timeout
	clk.Advance(30 * time.Second)

	// Check - bloodraven responds OK (transport returns 200),
	// peer also responds OK with this transport. But we test
	// the scenario where only one is down by directly calling evaluate.
	// Let's set lastBloodravenOK to now (reachable) and leave peer expired.
	fm.lastBloodravenOK = clk.Now()
	// peer was last seen 30 seconds ago at start

	fm.evaluate(context.Background())

	if fm.fenced {
		t.Error("SAFETY VIOLATION: fenced when only peer is down (bloodraven still reachable)")
	}

	// Now test other direction: peer OK, bloodraven expired
	fm.lastPeerOK = clk.Now()
	fm.lastBloodravenOK = clk.Now().Add(-30 * time.Second)

	fm.evaluate(context.Background())

	if fm.fenced {
		t.Error("SAFETY VIOLATION: fenced when only bloodraven is down (peer still reachable)")
	}
}

// INVARIANT: Fencing occurs exactly once.
// SetSuperReadOnly should be called exactly once even with multiple Check cycles.
func TestFencingInvariant_FencesExactlyOnce(t *testing.T) {
	start := time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC)
	clk := clock.NewFakeClock(start)
	f := newMockFencer(false) // primary
	fm := newIsolatedFencingMonitor(f, clk, noopTransport{})

	fm.lastBloodravenOK = clk.Now()
	fm.lastPeerOK = clk.Now()
	clk.Advance(30 * time.Second)

	// Multiple check cycles
	for i := 0; i < 5; i++ {
		fm.Check(context.Background())
		clk.Advance(5 * time.Second)
	}

	if !fm.fenced {
		t.Fatal("should have self-fenced")
	}

	// Count how many times SetSuperReadOnly was called.
	// The channel has capacity 1 and was signaled once.
	// After first fence, f.superReadOnly is true, and subsequent
	// evaluations are short-circuited by the fenced flag.
	// Reset and verify no additional calls.
	f.superReadOnly = false
	fm.Check(context.Background())
	if f.superReadOnly {
		t.Error("SAFETY VIOLATION: SetSuperReadOnly called more than once")
	}
}
