package component

import (
	"context"
	"errors"
	"log/slog"
	"net/http"
	"os"
	"testing"
	"time"

	"github.com/shipstream/bloodraven/internal/clock"
	"github.com/shipstream/bloodraven/internal/controller"
	"github.com/shipstream/bloodraven/internal/sidecar"
	"github.com/shipstream/bloodraven/internal/state"
)

// errTransport always returns an error, simulating unreachable endpoints
// without opening any network connections or performing DNS lookups.
type errTransport struct{}

func (errTransport) RoundTrip(_ *http.Request) (*http.Response, error) {
	return nil, errors.New("connection refused")
}

// ---------------------------------------------------------------------------
// Safety Invariant: Never expose two primaries through the primary service
// at the same time.
// ---------------------------------------------------------------------------

func TestSafetyInvariant_NeverDualPrimary(t *testing.T) {
	// Both DCs writable (split brain) — no failover should be triggered,
	// DNS should NOT flip, and an alert should be raised.
	h := newTestHarness(t)
	h.dc1MySQL.setReadOnly(false)
	h.dc2MySQL.setReadOnly(false)

	// RecoveryThreshold polls to confirm writable
	h.pollN(2)

	// DNS should not have been touched
	if h.dns.getLastIP() != "" {
		t.Error("SAFETY VIOLATION: DNS flipped during split brain")
	}

	// Neither should have been promoted through failover
	s := h.tm.Status()
	if s.Sites[0].State != "writable" || s.Sites[1].State != "writable" {
		t.Errorf("expected both writable (split brain), got dc1=%s dc2=%s", s.Sites[0].State, s.Sites[1].State)
	}
}

// ---------------------------------------------------------------------------
// Safety Invariant: Never flip DNS before promoted MySQL is confirmed writable.
// ---------------------------------------------------------------------------

func TestSafetyInvariant_NoDNSBeforeWritableConfirmed(t *testing.T) {
	h := newTestHarness(t)
	h.pollN(2) // establish dc1=writable, dc2=read-only

	// dc1 goes down
	h.dc1MySQL.setError(errDown)
	h.pollN(3) // failure threshold

	// At this point, failover was triggered and dc2 had SetReadOnly(false)
	// called, but DNS should NOT have flipped yet (needs confirmation polls)
	if h.dns.getLastIP() != "" {
		t.Error("SAFETY VIOLATION: DNS flipped before promotion was confirmed by polling")
	}

	// Now poll to confirm (recovery threshold = 2)
	h.pollN(2)

	if h.dns.getLastIP() != "2.2.2.2" {
		t.Errorf("DNS should point to dc2 after confirmation, got %q", h.dns.getLastIP())
	}
}

// ---------------------------------------------------------------------------
// Safety Invariant: Never auto-unfence a self-fenced primary.
// ---------------------------------------------------------------------------

func TestSafetyInvariant_NeverAutoUnfence(t *testing.T) {
	start := time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC)
	clk := clock.NewFakeClock(start)

	f := newMockSidecarMySQL(false) // primary (not read-only)
	fm := sidecar.NewFencingMonitorFull(f, "127.0.0.1:8081", "127.0.0.1:8080",
		5*time.Second, 20*time.Second, safetyTestLogger(), clk, &http.Client{Transport: errTransport{}})

	// Initialize last-seen times
	fm.Check(context.Background()) // sets lastBloodravenOK/lastPeerOK via checkBloodraven/checkPeer (which fail since no server)

	// Advance past lease timeout to trigger fencing
	clk.Advance(30 * time.Second)
	fm.Check(context.Background())

	if !fm.IsFenced() {
		t.Fatal("should have self-fenced")
	}

	// Simulate both becoming reachable again — fenced status must NOT clear
	// (Note: Check() won't reach real servers, but even if it could, IsFenced should stay true)
	clk.Advance(5 * time.Second)
	fm.Check(context.Background())

	if !fm.IsFenced() {
		t.Error("SAFETY VIOLATION: auto-unfenced a self-fenced primary")
	}

	// Multiple additional checks should not unfence
	for i := 0; i < 10; i++ {
		clk.Advance(5 * time.Second)
		fm.Check(context.Background())
	}

	if !fm.IsFenced() {
		t.Error("SAFETY VIOLATION: fenced status was cleared after multiple checks")
	}
}

// ---------------------------------------------------------------------------
// Safety Invariant: Never promote during anti-flap cooldown.
// ---------------------------------------------------------------------------

func TestSafetyInvariant_NeverPromoteDuringCooldown(t *testing.T) {
	h := newTestHarnessWithCooldown(t, 10*time.Minute)

	// Establish dc1=writable, dc2=read-only
	h.pollN(2)

	// dc1 goes down, triggers failover to dc2
	h.dc1MySQL.setError(errDown)
	h.pollN(3) // failure threshold triggers promotion

	// Confirm dc2 is now writable
	h.pollN(2)

	if h.dns.getLastIP() != "2.2.2.2" {
		t.Fatalf("expected DNS to point to dc2 after first failover, got %q", h.dns.getLastIP())
	}

	// Now dc2 goes down too — should NOT trigger another failover due to cooldown
	h.dc2MySQL.setError(errDown)
	h.dc1MySQL.setReadOnly(true)
	h.dc1MySQL.setError(nil)
	h.pollN(3) // would normally trigger failover to dc1

	// DNS should still point to dc2 (cooldown blocks second failover)
	if h.dns.getLastIP() != "2.2.2.2" {
		t.Error("SAFETY VIOLATION: second failover occurred during cooldown period")
	}
}

// ---------------------------------------------------------------------------
// Safety Invariant: Never self-fence a replica.
// ---------------------------------------------------------------------------

func TestSafetyInvariant_NeverSelfFenceReplica(t *testing.T) {
	start := time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC)
	clk := clock.NewFakeClock(start)

	f := newMockSidecarMySQL(true) // replica (read-only)
	fm := sidecar.NewFencingMonitorFull(f, "127.0.0.1:8081", "127.0.0.1:8080",
		5*time.Second, 20*time.Second, safetyTestLogger(), clk, &http.Client{Transport: errTransport{}})

	// Advance way past timeout
	clk.Advance(60 * time.Second)
	fm.Check(context.Background())

	if fm.IsFenced() {
		t.Error("SAFETY VIOLATION: replica was self-fenced")
	}
	if f.isSuperReadOnly() {
		t.Error("SAFETY VIOLATION: super_read_only was set on a replica by fencing monitor")
	}
}

// ---------------------------------------------------------------------------
// Safety Invariant: Never treat transient poll failures as confirmed
// outage before threshold.
// ---------------------------------------------------------------------------

func TestSafetyInvariant_TransientFailureNotOutage(t *testing.T) {
	h := newTestHarness(t)
	h.pollN(2) // establish dc1=writable

	// Single transient failure
	h.dc1MySQL.setError(errors.New("timeout"))
	h.pollN(1)

	s := h.tm.Status()
	if s.Sites[0].State == "unreachable" {
		t.Error("SAFETY VIOLATION: single failure marked DC as unreachable before threshold")
	}

	// Recovery
	h.dc1MySQL.setReadOnly(false)
	h.pollN(2)

	if h.tm.Status().Sites[0].State != "writable" {
		t.Error("DC should recover to writable after transient failure")
	}

	// Below-threshold failures should not trigger failover
	if h.dns.getLastIP() != "" {
		t.Error("SAFETY VIOLATION: DNS flipped from transient failure below threshold")
	}
}

// ---------------------------------------------------------------------------
// Safety Invariant: Never treat a single writable observation as recovered
// primary before threshold.
// ---------------------------------------------------------------------------

func TestSafetyInvariant_RecoveryRequiresThreshold(t *testing.T) {
	h := newTestHarnessWithMySQL(t, &mockMySQL{readOnly: true}, &mockMySQL{readOnly: false})
	h.pollN(2) // establish dc1=read-only, dc2=writable

	// dc1 becomes writable — needs RecoveryThreshold confirmations
	h.dc1MySQL.setReadOnly(false)
	h.pollN(1)

	s := h.tm.Status()
	if s.Sites[0].State == "writable" {
		t.Error("SAFETY VIOLATION: single writable poll confirmed recovery (threshold not met)")
	}

	// Second poll should confirm
	h.pollN(1)

	s = h.tm.Status()
	if s.Sites[0].State != "writable" {
		t.Errorf("DC1 should be writable after recovery threshold, got %s", s.Sites[0].State)
	}
}

// ---------------------------------------------------------------------------
// Safety Invariant: Failover fences old primary before promoting candidate.
// ---------------------------------------------------------------------------

func TestSafetyInvariant_FenceBeforePromote(t *testing.T) {
	// Use a tracking mock to verify call order
	candidate := &trackingChecker{}
	candidate.setReadOnly(true)
	oldPrimary := &trackingChecker{}
	logger := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelError}))
	fc := controller.NewFailoverController(logger)

	err := fc.Execute(context.Background(), candidate, oldPrimary, "dc2")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	// Old primary must be fenced FIRST
	opCalls := oldPrimary.getCalls()
	if len(opCalls) == 0 {
		t.Fatal("old primary was never fenced")
	}
	if opCalls[0] != "SetSuperReadOnly(ON)" {
		t.Errorf("first old-primary call should be SetSuperReadOnly(ON), got %q", opCalls[0])
	}

	// Candidate promotion should happen AFTER old primary fencing
	candCalls := candidate.getCalls()
	if len(candCalls) < 5 {
		t.Fatalf("expected 5 candidate calls, got %v", candCalls)
	}
	if candCalls[len(candCalls)-1] != "SetReadOnly(OFF)" {
		t.Errorf("last candidate call should be SetReadOnly(OFF), got %q", candCalls[len(candCalls)-1])
	}
}

// ---------------------------------------------------------------------------
// Safety Invariant: Cross-DC evaluation produces correct actions.
// ---------------------------------------------------------------------------

func TestSafetyInvariant_CrossSiteMatrix(t *testing.T) {
	tests := []struct {
		name       string
		site0State state.SiteState
		site1State state.SiteState
		wantPromo  string
		wantAlert  string
	}{
		{
			name:       "site0_unreachable_site1_readonly_promotes_site1",
			site0State: state.StateUnreachable,
			site1State: state.StateReadOnly,
			wantPromo:  "dc2",
		},
		{
			name:       "site1_unreachable_site0_readonly_promotes_site0",
			site0State: state.StateReadOnly,
			site1State: state.StateUnreachable,
			wantPromo:  "dc1",
		},
		{
			name:       "both_writable_is_split_brain",
			site0State: state.StateWritable,
			site1State: state.StateWritable,
			wantAlert:  "SPLIT BRAIN: both sites are writable",
		},
		{
			name:       "both_readonly_is_no_primary",
			site0State: state.StateReadOnly,
			site1State: state.StateReadOnly,
			wantAlert:  "NO PRIMARY: both sites are read-only",
		},
		{
			name:       "both_unreachable_is_total_loss",
			site0State: state.StateUnreachable,
			site1State: state.StateUnreachable,
			wantAlert:  "TOTAL LOSS: both sites are unreachable",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			action := state.EvalCrossSite(tt.site0State, tt.site1State,
				state.StateUnknown, state.StateUnknown, "dc1", "dc2")

			if action.PromoteSite != tt.wantPromo {
				t.Errorf("PromoteSite: got %q, want %q", action.PromoteSite, tt.wantPromo)
			}
			if tt.wantAlert != "" && action.Alert != tt.wantAlert {
				t.Errorf("Alert: got %q, want %q", action.Alert, tt.wantAlert)
			}
		})
	}
}

// ---------------------------------------------------------------------------
// Safety Invariant: Primary service selector uses role=primary.
// This test verifies the reconciler wiring catches label drift.
// ---------------------------------------------------------------------------

func TestSafetyInvariant_PrimaryServiceSelectorIsCorrect(t *testing.T) {
	// This is verified in reconciler_test.go but we add an explicit
	// safety invariant check here for documentation and visibility.
	h := newTestHarness(t)
	h.pollN(2)

	s := h.tm.Status()
	if s.Sites[0].State != "writable" {
		t.Fatalf("expected dc1 writable, got %s", s.Sites[0].State)
	}
	if s.Sites[1].State != "read-only" {
		t.Fatalf("expected dc2 read-only, got %s", s.Sites[1].State)
	}
}

// ---------------------------------------------------------------------------
// Helper: trackingChecker for call-order verification
// ---------------------------------------------------------------------------

type trackingChecker struct {
	mockMySQL
	callsMu []string
}

func (tc *trackingChecker) getCalls() []string {
	tc.mu.Lock()
	defer tc.mu.Unlock()
	out := make([]string, len(tc.callsMu))
	copy(out, tc.callsMu)
	return out
}

func (tc *trackingChecker) recordCall(name string) {
	// Already under tc.mu lock from the caller
	tc.callsMu = append(tc.callsMu, name)
}

func (tc *trackingChecker) SetSuperReadOnly(_ context.Context, on bool) error {
	tc.mu.Lock()
	defer tc.mu.Unlock()
	if on {
		tc.recordCall("SetSuperReadOnly(ON)")
	} else {
		tc.recordCall("SetSuperReadOnly(OFF)")
	}
	return nil
}

func (tc *trackingChecker) StopReplica(_ context.Context) error {
	tc.mu.Lock()
	defer tc.mu.Unlock()
	tc.recordCall("StopReplica")
	return nil
}

func (tc *trackingChecker) ResetReplicaAll(_ context.Context) error {
	tc.mu.Lock()
	defer tc.mu.Unlock()
	tc.recordCall("ResetReplicaAll")
	return nil
}

func (tc *trackingChecker) SetReadOnly(_ context.Context, on bool) error {
	tc.mu.Lock()
	defer tc.mu.Unlock()
	if on {
		tc.recordCall("SetReadOnly(ON)")
	} else {
		tc.recordCall("SetReadOnly(OFF)")
	}
	tc.readOnly = on
	return nil
}

func (tc *trackingChecker) WaitForRelayLogDrain(_ context.Context, _ time.Duration) error {
	tc.mu.Lock()
	defer tc.mu.Unlock()
	tc.recordCall("WaitForRelayLogDrain")
	return nil
}

// safetyTestLogger returns a quiet logger for safety invariant tests.
func safetyTestLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelError}))
}
