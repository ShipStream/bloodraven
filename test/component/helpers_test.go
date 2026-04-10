package component

import (
	"context"
	"errors"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/shipstream/bloodraven/internal/clock"
	"github.com/shipstream/bloodraven/internal/controller"
	"github.com/shipstream/bloodraven/internal/mysql"
	"github.com/shipstream/bloodraven/internal/platform"
)

// ---------------------------------------------------------------------------
// mockMySQL implements mysql.Checker with configurable behaviour.
// ---------------------------------------------------------------------------

type mockMySQL struct {
	mu       sync.Mutex
	readOnly bool
	err      error
	promoted bool

	// Tracking fields for verification.
	superReadOnly         bool
	stoppedReplica        bool
	resetReplicaAll       bool
	replicationSourceSet  bool
	replicaStarted        bool
	cloneDonorList        string
	cloneInstanceCalled   bool
	cloneInstanceErr      error
	changeReplicationOpts mysql.ReplicationSourceOpts

	// replicaStatus is returned from ShowReplicaStatus. nil (default) means
	// "never had replication configured", which is the fresh-deploy signature.
	replicaStatus *mysql.ReplicaStatus
}

func (m *mockMySQL) CheckReadOnly(_ context.Context) (bool, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.readOnly, m.err
}

func (m *mockMySQL) Promote(_ context.Context) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.promoted = true
	m.readOnly = false
	return nil
}

func (m *mockMySQL) Close() error { return nil }

func (m *mockMySQL) SetSuperReadOnly(_ context.Context, on bool) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.superReadOnly = on
	if on {
		m.readOnly = true
	}
	return nil
}

func (m *mockMySQL) StopReplica(_ context.Context) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.stoppedReplica = true
	return nil
}

func (m *mockMySQL) ResetReplicaAll(_ context.Context) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.resetReplicaAll = true
	return nil
}

func (m *mockMySQL) SetReadOnly(_ context.Context, on bool) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.readOnly = on
	return nil
}

func (m *mockMySQL) ShowReplicaStatus(_ context.Context) (*mysql.ReplicaStatus, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.replicaStatus, nil
}

func (m *mockMySQL) ChangeReplicationSource(_ context.Context, opts mysql.ReplicationSourceOpts) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.replicationSourceSet = true
	m.changeReplicationOpts = opts
	return nil
}

func (m *mockMySQL) StartReplica(_ context.Context) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.replicaStarted = true
	return nil
}

func (m *mockMySQL) WaitForRelayLogDrain(_ context.Context, _ time.Duration) error {
	return nil
}

func (m *mockMySQL) SetCloneDonorList(_ context.Context, donor string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.cloneDonorList = donor
	return nil
}

func (m *mockMySQL) CloneInstance(_ context.Context, _, _, _ string, _ bool, _ int) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.cloneInstanceCalled = true
	return m.cloneInstanceErr
}

// setReadOnly sets read-only state and clears any error.
func (m *mockMySQL) setReadOnly(ro bool) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.readOnly = ro
	m.err = nil
}

// setError makes subsequent CheckReadOnly calls return this error.
func (m *mockMySQL) setError(err error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.err = err
}

// isPromoted returns whether Promote was called.
func (m *mockMySQL) isPromoted() bool {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.promoted
}

// isReadOnly returns the current readOnly state.
func (m *mockMySQL) isReadOnly() bool {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.readOnly
}

// ---------------------------------------------------------------------------
// mockTainter implements platform.NodeTainter.
// ---------------------------------------------------------------------------

type mockTainter struct {
	mu     sync.Mutex
	taints map[string]bool // selector -> tainted
}

func newMockTainter() *mockTainter {
	return &mockTainter{taints: make(map[string]bool)}
}

func (m *mockTainter) SetTaint(_ context.Context, selector string, taint bool) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.taints[selector] = taint
	return nil
}

func (m *mockTainter) isTainted(selector string) bool {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.taints[selector]
}

// ---------------------------------------------------------------------------
// mockDNS implements platform.DNSUpdater.
// ---------------------------------------------------------------------------

type mockDNS struct {
	mu     sync.Mutex
	lastIP string
	calls  int
	err    error
}

func (m *mockDNS) UpdateDNSRecord(_ context.Context, ip string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.err != nil {
		return m.err
	}
	m.lastIP = ip
	m.calls++
	return nil
}

func (m *mockDNS) getLastIP() string {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.lastIP
}

func (m *mockDNS) getCalls() int {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.calls
}

// ---------------------------------------------------------------------------
// testHarness wires up all components for integration testing.
// ---------------------------------------------------------------------------

type testHarness struct {
	tm       *controller.TopologyManager
	dc1MySQL *mockMySQL
	dc2MySQL *mockMySQL
	tainter  *mockTainter
	dns      *mockDNS
	hub      *platform.Hub
	logger   *slog.Logger
	clock    *clock.FakeClock
}

func newTestHarness(t *testing.T) *testHarness {
	t.Helper()
	return newTestHarnessWithMySQL(t, &mockMySQL{readOnly: false}, &mockMySQL{readOnly: true})
}

func newTestHarnessWithMySQL(t *testing.T, dc1, dc2 *mockMySQL) *testHarness {
	t.Helper()

	logger := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelWarn}))
	tainter := newMockTainter()
	hub := platform.NewHub(logger)
	dns := &mockDNS{}
	fc := controller.NewFailoverController(logger)
	clk := clock.NewFakeClock(time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC))

	cfg := controller.TopologyConfig{
		Name:  "lion",
		Sites: [2]controller.SiteTopologyConfig{
			{Name: "dc1", Zone: "lion-dc1", LBIP: "1.1.1.1"},
			{Name: "dc2", Zone: "lion-dc2", LBIP: "2.2.2.2"},
		},
		SiteHosts:         [2]string{"mysql-lion-dc1.default.svc.cluster.local", "mysql-lion-dc2.default.svc.cluster.local"},
		PollInterval:      int64(50 * time.Millisecond),
		FailureThreshold:  3,
		RecoveryThreshold: 2,
		FailoverCooldown:  0, // no cooldown by default
	}

	tm := controller.NewTopologyManagerWithClock(cfg, dc1, dc2, fc, nil, controller.BootstrapConfig{}, tainter, hub, dns, logger, clk)

	return &testHarness{
		tm:       tm,
		dc1MySQL: dc1,
		dc2MySQL: dc2,
		tainter:  tainter,
		dns:      dns,
		hub:      hub,
		logger:   logger,
		clock:    clk,
	}
}

// newTestHarnessWithCooldown creates a harness with a specified failover cooldown.
func newTestHarnessWithCooldown(t *testing.T, cooldown time.Duration) *testHarness {
	t.Helper()

	logger := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelWarn}))
	dc1 := &mockMySQL{readOnly: false}
	dc2 := &mockMySQL{readOnly: true}
	tainter := newMockTainter()
	hub := platform.NewHub(logger)
	dns := &mockDNS{}
	fc := controller.NewFailoverController(logger)
	clk := clock.NewFakeClock(time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC))

	cfg := controller.TopologyConfig{
		Name:  "lion",
		Sites: [2]controller.SiteTopologyConfig{
			{Name: "dc1", Zone: "lion-dc1", LBIP: "1.1.1.1"},
			{Name: "dc2", Zone: "lion-dc2", LBIP: "2.2.2.2"},
		},
		SiteHosts:         [2]string{"mysql-lion-dc1.default.svc.cluster.local", "mysql-lion-dc2.default.svc.cluster.local"},
		PollInterval:      int64(50 * time.Millisecond),
		FailureThreshold:  3,
		RecoveryThreshold: 2,
		FailoverCooldown:  int64(cooldown),
	}

	tm := controller.NewTopologyManagerWithClock(cfg, dc1, dc2, fc, nil, controller.BootstrapConfig{}, tainter, hub, dns, logger, clk)

	return &testHarness{
		tm:       tm,
		dc1MySQL: dc1,
		dc2MySQL: dc2,
		tainter:  tainter,
		dns:      dns,
		hub:      hub,
		logger:   logger,
		clock:    clk,
	}
}

// pollN runs n poll cycles synchronously.
func (h *testHarness) pollN(n int) {
	ctx := context.Background()
	for i := 0; i < n; i++ {
		h.tm.Poll(ctx)
	}
}

// errDown is a convenience error for simulating a down MySQL instance.
var errDown = errors.New("connection refused")

// ---------------------------------------------------------------------------
// mockSidecarMySQL implements sidecar.Fencer for partition tests.
// ---------------------------------------------------------------------------

type mockSidecarMySQL struct {
	mu            sync.Mutex
	readOnly      bool
	superReadOnly bool
}

func newMockSidecarMySQL(readOnly bool) *mockSidecarMySQL {
	return &mockSidecarMySQL{readOnly: readOnly}
}

func (m *mockSidecarMySQL) IsReadOnly(_ context.Context) (bool, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.readOnly, nil
}

func (m *mockSidecarMySQL) SetSuperReadOnly(_ context.Context) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.superReadOnly = true
	m.readOnly = true
	return nil
}

func (m *mockSidecarMySQL) isSuperReadOnly() bool {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.superReadOnly
}

// ---------------------------------------------------------------------------
// startMockHTTPServer starts a simple test HTTP server that responds 200 OK
// on the given path. Returns the "host:port" address.
// ---------------------------------------------------------------------------

func startMockHTTPServer(t *testing.T, path string) string {
	t.Helper()

	mux := http.NewServeMux()
	mux.HandleFunc(path, func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
		w.Write([]byte("ok"))
	})

	srv := httptest.NewServer(mux)
	t.Cleanup(srv.Close)

	// httptest.Server URL is "http://host:port"; extract "host:port".
	return srv.Listener.Addr().String()
}
