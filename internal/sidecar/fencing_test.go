package sidecar

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/url"
	"os"
	"strings"
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
	m.readOnly = true
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

	if fm.fenced.Load() {
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

	if fm.fenced.Load() {
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

	if fm.fenced.Load() {
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

	if !fm.fenced.Load() {
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

	if fm.fenced.Load() {
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

	if fm.fenced.Load() {
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
	if !fm.fenced.Load() {
		t.Fatal("should have fenced")
	}

	f.superReadOnly = false

	fm.evaluate(context.Background())

	if f.superReadOnly {
		t.Error("should not re-fence when already fenced (setSuperReadOnly should not be called again)")
	}
}

func TestEvaluateRearmsAfterExternalRestore(t *testing.T) {
	start := time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC)
	clk := clock.NewFakeClock(start)
	f := newMockFencer(false) // primary
	fm := newTestFencingMonitor(f, clk)

	fm.lastBloodravenOK = clk.Now().Add(-30 * time.Second)
	setPeerLastOK(fm, clk.Now().Add(-30*time.Second))

	fm.evaluate(context.Background())
	if !fm.fenced.Load() {
		t.Fatal("should have fenced")
	}

	// Bloodraven is the only actor allowed to restore writability. Once
	// that external restore is visible, the sidecar must re-arm with a
	// fresh lease window: no instant re-fence on the pre-outage
	// timestamps (that would fight the operator that just restored the
	// site), but a later isolation event must self-fence again.
	f.readOnly = false
	f.superReadOnly = false
	fm.lastBloodravenOK = clk.Now().Add(-30 * time.Second)
	setPeerLastOK(fm, clk.Now().Add(-30*time.Second))

	fm.evaluate(context.Background())

	if fm.fenced.Load() {
		t.Fatal("should rearm with a fresh lease window, not instantly re-fence on pre-restore timestamps")
	}
	if f.superReadOnly {
		t.Error("should not set super_read_only in the same cycle as the rearm")
	}
	if !fm.lastBloodravenOK.Equal(clk.Now()) {
		t.Errorf("rearm should reset lastBloodravenOK to now; got %v, want %v", fm.lastBloodravenOK, clk.Now())
	}

	// Renewed isolation: no probe succeeds for a full lease window after
	// the rearm — the safety backstop must fire again.
	clk.Advance(30 * time.Second)

	fm.evaluate(context.Background())

	if !fm.fenced.Load() {
		t.Fatal("should have re-fenced after external restore and renewed isolation past the lease timeout")
	}
	if !f.superReadOnly {
		t.Error("should set super_read_only again after external restore and renewed isolation")
	}
}

func TestEvaluateRearmClearsStaleTopologyCache(t *testing.T) {
	start := time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC)
	clk := clock.NewFakeClock(start)
	f := newMockFencer(false) // externally restored to writable
	cache := &TopologyCache{}
	cache.Set("pdx", clk.Now().Add(-10*time.Second))
	fm := newTopologyFencingMonitor(f, clk, cache, nil)
	fm.fenced.Store(true)
	fm.lastBloodravenOK = clk.Now()
	setPeerLastOK(fm, clk.Now())

	fm.evaluate(context.Background())

	if fm.fenced.Load() {
		t.Fatal("should rearm without immediately fencing on stale topology")
	}
	if snap := cache.Snapshot(); snap.ActiveSite != "" {
		t.Fatalf("topology cache activeSite = %q, want cleared on rearm", snap.ActiveSite)
	}
	if f.superReadOnly {
		t.Error("should not set super_read_only from stale topology after external restore")
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

	if fm.fenced.Load() {
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
	if fm.fenced.Load() {
		t.Error("should not fence when both reachable")
	}

	clk.Advance(30 * time.Second)
	fm.Check(context.Background())

	if !fm.fenced.Load() {
		t.Error("should fence after clock advances past lease timeout with both unreachable")
	}
}

// routingTransport lets a single *http.Client dispatch to different
// stub handlers based on the request path. This keeps the tests
// socket-free while exercising the full checkActiveSite /
// checkPeerTopology code paths.
type routingTransport struct {
	// routes keys are request-path suffix prefixes matched with
	// strings.HasPrefix. The first matching route wins, so order in
	// the slice matters for overlapping prefixes.
	routes []routingRoute
}

type routingRoute struct {
	pathPrefix string
	handler    func(r *http.Request) (*http.Response, error)
}

func (t *routingTransport) RoundTrip(r *http.Request) (*http.Response, error) {
	for _, route := range t.routes {
		if strings.HasPrefix(r.URL.Path, route.pathPrefix) {
			return route.handler(r)
		}
	}
	return nil, fmt.Errorf("no stub for %s", r.URL.Path)
}

func jsonResponse(status int, body any) func(*http.Request) (*http.Response, error) {
	return func(_ *http.Request) (*http.Response, error) {
		buf, _ := json.Marshal(body)
		return &http.Response{
			StatusCode: status,
			Body:       io.NopCloser(bytes.NewReader(buf)),
			Header:     http.Header{"Content-Type": []string{"application/json"}},
		}, nil
	}
}

func errorResponse(err error) func(*http.Request) (*http.Response, error) {
	return func(_ *http.Request) (*http.Response, error) { return nil, err }
}

// newTopologyFencingMonitor builds a FencingMonitor wired to a
// TopologyCache and identity fields, with a routing transport so
// tests can inject /active-site and /peer/active-site responses
// without touching the network.
func newTopologyFencingMonitor(
	f Fencer,
	clk *clock.FakeClock,
	cache *TopologyCache,
	routes []routingRoute,
) *FencingMonitor {
	client := &http.Client{Transport: &routingTransport{routes: routes}}
	fm := NewFencingMonitorFull(
		f,
		"127.0.0.1:8081",
		[]string{testPeerAddr},
		5*time.Second,
		20*time.Second,
		testLogger(),
		clk,
		client,
	).WithTopology("iad", "default", "orders", cache)
	return fm
}

// TestEvaluate_TopologyMismatchFencesImmediately verifies rule #1:
// when the operator-authoritative active site disagrees with mySite,
// the monitor fences even though the operator and peer are both
// currently reachable. Closes WISHLIST #4 — a stale primary that
// returns from a partition must fence itself as soon as it learns a
// failover has happened, without waiting for the lease to expire.
func TestEvaluate_TopologyMismatchFencesImmediately(t *testing.T) {
	start := time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC)
	clk := clock.NewFakeClock(start)
	f := newMockFencer(false) // primary

	cache := &TopologyCache{}
	cache.Set("pdx", clk.Now()) // operator says active site is pdx
	fm := newTopologyFencingMonitor(f, clk, cache, nil)

	// Operator + peer both reachable — rule #2 would never fire.
	fm.lastBloodravenOK = clk.Now()
	setPeerLastOK(fm, clk.Now())

	fm.evaluate(context.Background())

	if !fm.fenced.Load() {
		t.Fatal("should fence when topology says active site != mySite")
	}
	if !f.superReadOnly {
		t.Error("should SET super_read_only on topology mismatch")
	}
}

// TestEvaluate_TopologyMatchDoesNotFence verifies the hot-path
// case: operator confirms this is the active site, lease counters
// fresh → no fence.
func TestEvaluate_TopologyMatchDoesNotFence(t *testing.T) {
	start := time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC)
	clk := clock.NewFakeClock(start)
	f := newMockFencer(false) // primary

	cache := &TopologyCache{}
	cache.Set("iad", clk.Now()) // iad == our site
	fm := newTopologyFencingMonitor(f, clk, cache, nil)

	fm.lastBloodravenOK = clk.Now()
	setPeerLastOK(fm, clk.Now())

	fm.evaluate(context.Background())

	if fm.fenced.Load() {
		t.Error("should not fence when topology confirms this is the active site")
	}
}

// TestEvaluate_EmptyCacheDoesNotFence verifies that an unpopulated
// cache does not accidentally trigger rule #1. Until the sidecar
// has successfully observed any authoritative topology, lease-only
// behavior applies.
func TestEvaluate_EmptyCacheDoesNotFence(t *testing.T) {
	start := time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC)
	clk := clock.NewFakeClock(start)
	f := newMockFencer(false)

	cache := &TopologyCache{}
	fm := newTopologyFencingMonitor(f, clk, cache, nil)

	fm.lastBloodravenOK = clk.Now()
	setPeerLastOK(fm, clk.Now())

	fm.evaluate(context.Background())

	if fm.fenced.Load() {
		t.Error("empty topology cache should not trigger fencing")
	}
}

// TestCheckActiveSite_PopulatesCache verifies that a successful
// /active-site response from the operator overwrites the cache.
func TestCheckActiveSite_PopulatesCache(t *testing.T) {
	start := time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC)
	clk := clock.NewFakeClock(start)
	f := newMockFencer(false)
	cache := &TopologyCache{}

	routes := []routingRoute{
		{pathPrefix: "/active-site", handler: jsonResponse(200, map[string]string{
			"activeSite": "pdx",
		})},
	}
	fm := newTopologyFencingMonitor(f, clk, cache, routes)

	fm.checkActiveSite(context.Background())

	snap := cache.Snapshot()
	if snap.ActiveSite != "pdx" {
		t.Errorf("activeSite = %q, want pdx", snap.ActiveSite)
	}
	if !snap.ObservedAt.Equal(clk.Now()) {
		t.Errorf("observedAt = %v, want %v", snap.ObservedAt, clk.Now())
	}
}

func TestCheckActiveSite_ClearsCacheOnEmptyActiveSite(t *testing.T) {
	start := time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC)
	clk := clock.NewFakeClock(start)
	f := newMockFencer(false)
	cache := &TopologyCache{}
	cache.Set("pdx", start.Add(-5*time.Second))

	routes := []routingRoute{
		{pathPrefix: "/active-site", handler: jsonResponse(200, map[string]string{
			"activeSite": "",
		})},
	}
	fm := newTopologyFencingMonitor(f, clk, cache, routes)

	fm.checkActiveSite(context.Background())

	snap := cache.Snapshot()
	if snap.ActiveSite != "" {
		t.Errorf("activeSite = %q, want cache cleared", snap.ActiveSite)
	}
	if !snap.ObservedAt.Equal(clk.Now()) {
		t.Errorf("observedAt = %v, want %v", snap.ObservedAt, clk.Now())
	}
}

// TestCheckActiveSite_PassesIdentityParams verifies that the
// /active-site URL carries the namespace and group as query params.
// The operator returns 400 without them.
func TestCheckActiveSite_PassesIdentityParams(t *testing.T) {
	start := time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC)
	clk := clock.NewFakeClock(start)
	f := newMockFencer(false)
	cache := &TopologyCache{}

	var captured url.Values
	routes := []routingRoute{
		{pathPrefix: "/active-site", handler: func(r *http.Request) (*http.Response, error) {
			captured = r.URL.Query()
			return jsonResponse(200, map[string]string{"activeSite": "iad"})(r)
		}},
	}
	fm := newTopologyFencingMonitor(f, clk, cache, routes)

	fm.checkActiveSite(context.Background())

	if got := captured.Get("namespace"); got != "default" {
		t.Errorf("namespace query = %q, want default", got)
	}
	if got := captured.Get("group"); got != "orders" {
		t.Errorf("group query = %q, want orders", got)
	}
}

// TestCheckActiveSite_LeavesCacheOnFailure verifies that an
// unreachable operator does not wipe the cache.
func TestCheckActiveSite_LeavesCacheOnFailure(t *testing.T) {
	start := time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC)
	clk := clock.NewFakeClock(start)
	f := newMockFencer(false)
	cache := &TopologyCache{}
	cache.Set("iad", start.Add(-5*time.Second))

	routes := []routingRoute{
		{pathPrefix: "/active-site", handler: errorResponse(fmt.Errorf("connection refused"))},
	}
	fm := newTopologyFencingMonitor(f, clk, cache, routes)

	fm.checkActiveSite(context.Background())

	snap := cache.Snapshot()
	if snap.ActiveSite != "iad" {
		t.Errorf("cache clobbered on failure: %q", snap.ActiveSite)
	}
}

// TestCheckPeerTopology_AdoptsNewerSnapshot verifies the
// partition-tolerant path: if the operator is unreachable but a
// peer's /peer/active-site returns a strictly newer view, we adopt.
func TestCheckPeerTopology_AdoptsNewerSnapshot(t *testing.T) {
	start := time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC)
	clk := clock.NewFakeClock(start)
	f := newMockFencer(false)
	cache := &TopologyCache{}
	cache.Set("iad", start.Add(-30*time.Second)) // stale "I'm active"

	peerSnap := TopologySnapshot{
		ActiveSite: "pdx",
		ObservedAt: start, // strictly newer
	}
	routes := []routingRoute{
		{pathPrefix: "/peer/active-site", handler: jsonResponse(200, peerSnap)},
	}
	fm := newTopologyFencingMonitor(f, clk, cache, routes)

	fm.checkPeerTopology(context.Background(), testPeerAddr)

	snap := cache.Snapshot()
	if snap.ActiveSite != "pdx" {
		t.Errorf("expected adoption of peer view pdx, got %q", snap.ActiveSite)
	}
}

// TestCheckPeerTopology_IgnoresOlderSnapshot verifies that a peer
// with a stale cache cannot drag us backward.
func TestCheckPeerTopology_IgnoresOlderSnapshot(t *testing.T) {
	start := time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC)
	clk := clock.NewFakeClock(start)
	f := newMockFencer(false)
	cache := &TopologyCache{}
	cache.Set("pdx", start) // fresh

	peerSnap := TopologySnapshot{
		ActiveSite: "iad", // stale
		ObservedAt: start.Add(-30 * time.Second),
	}
	routes := []routingRoute{
		{pathPrefix: "/peer/active-site", handler: jsonResponse(200, peerSnap)},
	}
	fm := newTopologyFencingMonitor(f, clk, cache, routes)

	fm.checkPeerTopology(context.Background(), testPeerAddr)

	snap := cache.Snapshot()
	if snap.ActiveSite != "pdx" {
		t.Errorf("peer older snapshot should have been ignored, got %q", snap.ActiveSite)
	}
}

// TestCheckPeerTopology_OldSidecarReturns404 verifies the
// rolling-upgrade compatibility path: an older peer that does not
// yet serve /peer/active-site returns 404, which we silently
// ignore. The cache stays intact.
func TestCheckPeerTopology_OldSidecarReturns404(t *testing.T) {
	start := time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC)
	clk := clock.NewFakeClock(start)
	f := newMockFencer(false)
	cache := &TopologyCache{}
	cache.Set("iad", start)

	routes := []routingRoute{
		{pathPrefix: "/peer/active-site", handler: func(_ *http.Request) (*http.Response, error) {
			return &http.Response{StatusCode: 404, Body: io.NopCloser(strings.NewReader(""))}, nil
		}},
	}
	fm := newTopologyFencingMonitor(f, clk, cache, routes)

	fm.checkPeerTopology(context.Background(), testPeerAddr)

	snap := cache.Snapshot()
	if snap.ActiveSite != "iad" {
		t.Errorf("cache clobbered by 404: %q", snap.ActiveSite)
	}
}

// TestEvaluate_StalePrimaryLearnsViaPeer is the end-to-end flow
// that closes the WISHLIST #4 gap: operator unreachable, peer
// reachable with a newer view that names a different site as
// active. A single Check cycle should adopt the peer's view and
// fence immediately, without waiting for the lease to expire.
func TestEvaluate_StalePrimaryLearnsViaPeer(t *testing.T) {
	start := time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC)
	clk := clock.NewFakeClock(start)
	f := newMockFencer(false) // primary (still writable)
	cache := &TopologyCache{}
	cache.Set("iad", start.Add(-30*time.Second)) // pre-failover view

	peerSnap := TopologySnapshot{
		ActiveSite: "pdx",
		ObservedAt: start,
	}
	routes := []routingRoute{
		{pathPrefix: "/healthz", handler: errorResponse(fmt.Errorf("operator partitioned"))},
		{pathPrefix: "/active-site", handler: errorResponse(fmt.Errorf("operator partitioned"))},
		{pathPrefix: "/peer/ping", handler: jsonResponse(200, "pong")},
		{pathPrefix: "/peer/active-site", handler: jsonResponse(200, peerSnap)},
	}
	fm := newTopologyFencingMonitor(f, clk, cache, routes)

	// Lease counters are fresh — rule #2 cannot fire.
	fm.lastBloodravenOK = clk.Now()
	setPeerLastOK(fm, clk.Now())

	fm.Check(context.Background())

	if !fm.fenced.Load() {
		t.Fatal("stale primary should fence after adopting peer's fresher active-site view")
	}
	snap := cache.Snapshot()
	if snap.ActiveSite != "pdx" {
		t.Errorf("cache should have adopted peer's view, got %q", snap.ActiveSite)
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

	if fm.fenced.Load() {
		t.Fatal("should not self-fence when at least one peer is still reachable")
	}
}

// blockingFencer holds KillConnections open until released, so a test
// can observe the monitor's state while a fence is mid-eviction.
type blockingFencer struct {
	*mockFencer
	entered  chan struct{}
	release  chan struct{}
	released bool
}

func (b *blockingFencer) KillConnections(ctx context.Context) (int, error) {
	close(b.entered)
	<-b.release
	return 0, nil
}

// The fence is super_read_only=ON, so IsFenced must be true from the
// moment that write lands — not after the eviction, which has no bound.
// Otherwise /status reports self_fenced=false for a site that is
// demonstrably fenced, for as long as a slow KILL takes to drain.
func TestFencingReportsFencedBeforeEvictionCompletes(t *testing.T) {
	clk := clock.NewFakeClock(time.Now())
	m := newMockFencer(false)
	b := &blockingFencer{mockFencer: m, entered: make(chan struct{}), release: make(chan struct{})}
	fm := newTestFencingMonitor(b, clk)
	fm.WithTopology("iad", "ns", "fg", &TopologyCache{})
	fm.topology.Set("pdx", clk.Now())
	setPeerLastOK(fm, clk.Now())
	fm.lastBloodravenOK = clk.Now()

	done := make(chan struct{})
	go func() {
		defer close(done)
		fm.evaluate(context.Background())
	}()

	select {
	case <-b.entered:
	case <-time.After(5 * time.Second):
		t.Fatal("eviction never started")
	}

	if !fm.IsFenced() {
		t.Error("IsFenced() is false while the fence is mid-eviction; super_read_only is already ON")
	}

	close(b.release)
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("evaluate did not return after the eviction was released")
	}
	if !fm.IsFenced() {
		t.Error("IsFenced() is false after a completed fence")
	}
}

// self_fenced is process-scoped by design. A restarted sidecar meeting an
// instance that is still read-only from an earlier fence reports not
// fenced: evaluate() returns on the read-only check without
// reconstructing why, and nothing persists the earlier decision.
// super_read_only answers "is it fenced"; this answers "did this monitor
// fence it".
func TestFencingIsFencedIsScopedToTheProcess(t *testing.T) {
	clk := clock.NewFakeClock(time.Now())
	// Fresh monitor, instance still read-only from a fence a previous
	// container performed.
	m := newMockFencer(true)
	fm := newTestFencingMonitor(m, clk)
	fm.WithTopology("iad", "ns", "fg", &TopologyCache{})
	fm.topology.Set("pdx", clk.Now())
	setPeerLastOK(fm, clk.Now())
	fm.lastBloodravenOK = clk.Now()

	fm.evaluate(context.Background())

	if fm.IsFenced() {
		t.Error("a fresh monitor must not claim a fence it did not perform")
	}
	if m.superReadOnly {
		t.Error("a read-only instance must not be re-fenced")
	}
}

// The fence sequence runs on the monitor's own goroutine, so both the
// super_read_only write and the eviction must be bounded: either one
// blocking on an unresponsive server would otherwise stop every
// subsequent fencing check for this site forever. A fence that times out
// is retried next tick; a wedged loop never fences again.
func TestFencingBoundsTheFenceSequence(t *testing.T) {
	clk := clock.NewFakeClock(time.Now())
	m := newMockFencer(false)
	d := &deadlineRecordingFencer{mockFencer: m}
	fm := newTestFencingMonitor(d, clk)
	fm.WithTopology("iad", "ns", "fg", &TopologyCache{})
	fm.topology.Set("pdx", clk.Now())
	setPeerLastOK(fm, clk.Now())
	fm.lastBloodravenOK = clk.Now()

	fm.evaluate(context.Background())

	if !d.called {
		t.Fatal("eviction never ran")
	}
	if !d.hadDeadline {
		t.Error("KillConnections got an unbounded context; a hung KILL would wedge the monitor loop")
	}
	if !d.setHadDeadline {
		t.Error("SetSuperReadOnly got an unbounded context; a hung write would wedge the monitor loop")
	}
}

type deadlineRecordingFencer struct {
	*mockFencer
	called         bool
	hadDeadline    bool
	setHadDeadline bool
}

func (d *deadlineRecordingFencer) SetSuperReadOnly(ctx context.Context) error {
	_, d.setHadDeadline = ctx.Deadline()
	return d.mockFencer.SetSuperReadOnly(ctx)
}

func (d *deadlineRecordingFencer) KillConnections(ctx context.Context) (int, error) {
	d.called = true
	_, d.hadDeadline = ctx.Deadline()
	return 0, nil
}

// ambiguousFencer applies the fence write and then reports an error, the
// shape of a SET GLOBAL that the server executed before the client's
// context deadline tore the connection down.
type ambiguousFencer struct {
	*mockFencer
	applyWrite bool
	killed     bool
}

func (a *ambiguousFencer) SetSuperReadOnly(_ context.Context) error {
	if a.applyWrite {
		a.superReadOnly = true
		a.readOnly = true
	}
	return context.DeadlineExceeded
}

func (a *ambiguousFencer) KillConnections(_ context.Context) (int, error) {
	a.killed = true
	return 0, nil
}

// A fence write that returns an error may still have landed — cancelling
// the context drops the client connection, it does not roll back a SET
// GLOBAL the server already applied. Treating that as "not fenced" leaves
// the flag false against a read-only instance, so evaluate() skips the
// rearm branch when the operator restores writability, never refreshes
// its lease timestamps, and re-fences the site it just promoted.
func TestFencingResolvesAnAmbiguousFenceWrite(t *testing.T) {
	newMonitor := func(a *ambiguousFencer) (*FencingMonitor, *clock.FakeClock) {
		clk := clock.NewFakeClock(time.Now())
		fm := newTestFencingMonitor(a, clk)
		fm.WithTopology("iad", "ns", "fg", &TopologyCache{})
		fm.topology.Set("pdx", clk.Now())
		setPeerLastOK(fm, clk.Now())
		fm.lastBloodravenOK = clk.Now()
		return fm, clk
	}

	t.Run("write landed", func(t *testing.T) {
		a := &ambiguousFencer{mockFencer: newMockFencer(false), applyWrite: true}
		fm, _ := newMonitor(a)

		fm.evaluate(context.Background())

		if !fm.IsFenced() {
			t.Error("IsFenced() is false after a fence write that errored but landed; the instance is read-only")
		}
		if a.killed {
			t.Error("eviction ran on a spent fence context; every KILL would fail immediately")
		}
	})

	// The rearm branch is the whole reason the flag has to be right: a
	// monitor that missed its own fence never refreshes the lease window
	// the operator's restore is supposed to grant.
	t.Run("rearms after the operator restores writability", func(t *testing.T) {
		a := &ambiguousFencer{mockFencer: newMockFencer(false), applyWrite: true}
		fm, clk := newMonitor(a)
		fm.evaluate(context.Background())

		// The operator promotes this site: writable again, and the stale
		// active-site view that tripped rule #1 is cleared with it.
		a.readOnly = false
		a.superReadOnly = false
		stale := fm.lastBloodravenOK
		clk.Advance(time.Minute)
		fm.topology.Set("", clk.Now())

		fm.evaluate(context.Background())

		if fm.IsFenced() {
			t.Error("monitor did not rearm after the restore")
		}
		if !fm.lastBloodravenOK.After(stale) {
			t.Error("rearm did not refresh the lease window; rule #2 would re-fence against pre-outage timestamps")
		}
	})

	// The other half of the contract: a write that genuinely failed must
	// not be promoted to a fence just because it errored.
	t.Run("write did not land", func(t *testing.T) {
		a := &ambiguousFencer{mockFencer: newMockFencer(false), applyWrite: false}
		fm, _ := newMonitor(a)

		fm.evaluate(context.Background())

		if fm.IsFenced() {
			t.Error("IsFenced() is true after a fence write that failed and left the instance writable")
		}
	})

	// An unreadable instance is unknown, not fenced. Claiming the fence
	// here would suppress the retry that is the actual remedy.
	t.Run("probe unavailable", func(t *testing.T) {
		a := &ambiguousFencer{mockFencer: newMockFencer(false), applyWrite: true}
		fm, _ := newMonitor(a)
		a.readOnlyErr = fmt.Errorf("connection refused")

		fm.doFence(context.Background())

		if fm.IsFenced() {
			t.Error("IsFenced() is true although the confirming read failed; the outcome is unknown")
		}
	})
}
