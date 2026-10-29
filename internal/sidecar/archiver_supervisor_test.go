package sidecar

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// storeInitScript is an ArchiveStoreFactory that fails its first
// failures calls and then returns a memStore. Every call is reported on
// calls so tests can step the supervisor without sleeping.
type storeInitScript struct {
	failures int
	n        atomic.Int32
	calls    chan int
}

func newStoreInitScript(failures int) *storeInitScript {
	return &storeInitScript{failures: failures, calls: make(chan int, 64)}
}

func (s *storeInitScript) factory(_ context.Context, _ *PITRConfig) (ArchiveStore, error) {
	n := int(s.n.Add(1))
	s.calls <- n
	if n <= s.failures {
		return nil, fmt.Errorf("boom %d", n)
	}
	return newMemStore(), nil
}

// storeInitBackoff replaces the supervisor's backoff wait: it reports
// each requested delay and blocks until the test releases it or ctx ends.
type storeInitBackoff struct {
	delays  chan time.Duration
	release chan struct{}
}

func newStoreInitBackoff() *storeInitBackoff {
	return &storeInitBackoff{delays: make(chan time.Duration, 64), release: make(chan struct{})}
}

func (b *storeInitBackoff) wait(ctx context.Context, d time.Duration) bool {
	b.delays <- d
	select {
	case <-b.release:
		return true
	case <-ctx.Done():
		return false
	}
}

// storeInitArchivers counts archiver starts and blocks each one until
// ctx ends, standing in for BinlogArchiver.Run.
type storeInitArchivers struct {
	started chan *BinlogArchiver
	count   atomic.Int32
}

func newStoreInitArchivers() *storeInitArchivers {
	return &storeInitArchivers{started: make(chan *BinlogArchiver, 8)}
}

func (r *storeInitArchivers) run(ctx context.Context, a *BinlogArchiver) {
	r.count.Add(1)
	r.started <- a
	<-ctx.Done()
}

func storeInitPITRConfig(t *testing.T) *PITRConfig {
	t.Helper()
	return &PITRConfig{
		StorageType:    "PVC",
		PVC:            &PITRPVCConfig{MountPath: t.TempDir()},
		ManifestPrefix: "pitr/orders",
		BinlogDir:      t.TempDir(),
		BinlogIndex:    "mysql-bin.index",
		PollInterval:   time.Minute,
	}
}

func recvWithin[T any](t *testing.T, ch <-chan T, what string) T {
	t.Helper()
	select {
	case v := <-ch:
		return v
	case <-time.After(5 * time.Second):
		t.Fatalf("timed out waiting for %s", what)
		var zero T
		return zero
	}
}

func getArchiverStatus(t *testing.T, srv *Server) Status {
	t.Helper()
	req := httptest.NewRequest(http.MethodGet, "/archiver/status", nil)
	w := httptest.NewRecorder()
	srv.httpServer.Handler.ServeHTTP(w, req)
	if w.Code != http.StatusOK {
		t.Fatalf("/archiver/status = %d, want 200", w.Code)
	}
	var raw map[string]any
	if err := json.Unmarshal(w.Body.Bytes(), &raw); err != nil {
		t.Fatalf("decode raw: %v", err)
	}
	if _, ok := raw["storeReady"]; !ok {
		t.Fatalf("/archiver/status omits storeReady while PITR is configured: %s", w.Body.String())
	}
	var st Status
	if err := json.Unmarshal(w.Body.Bytes(), &st); err != nil {
		t.Fatalf("decode: %v", err)
	}
	return st
}

func TestArchiverSupervisorReportsInitFailureAndStartsOneArchiver(t *testing.T) {
	cfg := storeInitPITRConfig(t)
	// A read-only site: the archiver's own scan clears lastError here, so
	// the init failure must be reported from outside that path.
	role := &stubRoleChecker{readOnly: true, connectableVal: true}
	script := newStoreInitScript(2)
	backoff := newStoreInitBackoff()
	archivers := newStoreInitArchivers()

	sup := NewArchiverSupervisor(cfg, "iad", role, script.factory, testLogger())
	sup.wait = backoff.wait
	sup.runArchiver = archivers.run
	srv := NewServer(&mockMysqlQuerier{connectable: true, readOnly: true}, ":0", testLogger())
	srv.SetArchiver(sup)

	before := getArchiverStatus(t, srv)
	if !before.Enabled || before.StoreReady == nil || *before.StoreReady || before.LastError != "" {
		t.Fatalf("before the first attempt: %+v, want enabled, storeReady=false, no error", before)
	}

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		defer close(done)
		sup.Run(ctx)
	}()

	for attempt := 1; attempt <= 2; attempt++ {
		if got := recvWithin(t, script.calls, "store attempt"); got != attempt {
			t.Fatalf("attempt = %d, want %d", got, attempt)
		}
		recvWithin(t, backoff.delays, "backoff wait")
		st := getArchiverStatus(t, srv)
		if !st.Enabled {
			t.Fatalf("attempt %d: enabled=false; a configured archiver must not look unconfigured", attempt)
		}
		if st.StoreReady == nil || *st.StoreReady {
			t.Fatalf("attempt %d: storeReady = %v, want false", attempt, st.StoreReady)
		}
		if want := fmt.Sprintf("archive store init: boom %d", attempt); st.LastError != want {
			t.Fatalf("attempt %d: lastError = %q, want %q", attempt, st.LastError, want)
		}
		if st.UploadFailures != int64(attempt) {
			t.Fatalf("attempt %d: uploadFailures = %d, want %d", attempt, st.UploadFailures, attempt)
		}
		if st.Site != "iad" || st.StorageType != "PVC" || st.ManifestPrefix != "pitr/orders" {
			t.Fatalf("attempt %d: static fields = %q/%q/%q", attempt, st.Site, st.StorageType, st.ManifestPrefix)
		}
		if st.Primary {
			t.Fatalf("attempt %d: primary = true on a read-only site", attempt)
		}
		if archivers.count.Load() != 0 {
			t.Fatalf("attempt %d: archiver started before the store was ready", attempt)
		}
		backoff.release <- struct{}{}
	}

	if got := recvWithin(t, script.calls, "successful store attempt"); got != 3 {
		t.Fatalf("attempt = %d, want 3", got)
	}
	a := recvWithin(t, archivers.started, "archiver start")
	if a == nil || a.site != "iad" {
		t.Fatalf("started archiver = %+v", a)
	}
	st := getArchiverStatus(t, srv)
	if st.StoreReady == nil || !*st.StoreReady {
		t.Fatalf("after success: storeReady = %v, want true", st.StoreReady)
	}
	if st.LastError != "" {
		t.Fatalf("after success: lastError = %q, want empty", st.LastError)
	}
	if st.UploadFailures != 2 {
		t.Fatalf("after success: uploadFailures = %d, want the 2 init failures carried over", st.UploadFailures)
	}

	cancel()
	recvWithin(t, done, "supervisor exit")
	if n := archivers.count.Load(); n != 1 {
		t.Fatalf("archiver started %d times, want exactly 1", n)
	}
	if n := script.n.Load(); n != 3 {
		t.Fatalf("store factory called %d times, want 3", n)
	}
}

func TestArchiverSupervisorBackoffIsCappedWithJitter(t *testing.T) {
	cfg := storeInitPITRConfig(t)
	script := newStoreInitScript(1 << 20)
	backoff := newStoreInitBackoff()
	sup := NewArchiverSupervisor(cfg, "iad", &stubRoleChecker{}, script.factory, testLogger())
	sup.wait = backoff.wait
	sup.runArchiver = func(context.Context, *BinlogArchiver) { t.Error("archiver must not start") }

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		defer close(done)
		sup.Run(ctx)
	}()

	nominal := archiveStoreRetryInitial
	for i := 0; i < 12; i++ {
		recvWithin(t, script.calls, "store attempt")
		d := recvWithin(t, backoff.delays, "backoff wait")
		if d > nominal || d < nominal*4/5 {
			t.Fatalf("wait %d = %v, want within [%v, %v]", i, d, nominal*4/5, nominal)
		}
		if d > archiveStoreRetryMax {
			t.Fatalf("wait %d = %v exceeds cap %v", i, d, archiveStoreRetryMax)
		}
		nominal = min(nominal*2, archiveStoreRetryMax)
		backoff.release <- struct{}{}
	}
	if nominal != archiveStoreRetryMax {
		t.Fatalf("backoff never reached the cap: %v", nominal)
	}
	cancel()
	recvWithin(t, done, "supervisor exit")
}

func TestArchiverSupervisorShutdownDuringBackoffReturnsPromptly(t *testing.T) {
	cfg := storeInitPITRConfig(t)
	script := newStoreInitScript(1 << 20)
	// Default wait: a real timer of at least 4s. Returning well inside
	// that proves cancellation interrupts the backoff.
	sup := NewArchiverSupervisor(cfg, "iad", &stubRoleChecker{}, script.factory, testLogger())
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		defer close(done)
		sup.Run(ctx)
	}()
	recvWithin(t, script.calls, "store attempt")
	start := time.Now()
	cancel()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("supervisor did not return within 2s of shutdown during backoff")
	}
	if took := time.Since(start); took >= archiveStoreRetryInitial*4/5 {
		t.Fatalf("shutdown took %v; it waited out the backoff", took)
	}
}

func TestArchiverSupervisorShutdownDuringStoreInit(t *testing.T) {
	cfg := storeInitPITRConfig(t)
	entered := make(chan struct{})
	factory := func(ctx context.Context, _ *PITRConfig) (ArchiveStore, error) {
		close(entered)
		<-ctx.Done()
		return nil, ctx.Err()
	}
	sup := NewArchiverSupervisor(cfg, "iad", &stubRoleChecker{}, factory, testLogger())
	sup.wait = func(context.Context, time.Duration) bool {
		t.Error("supervisor backed off after shutdown")
		return false
	}
	sup.runArchiver = func(context.Context, *BinlogArchiver) { t.Error("archiver must not start") }
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		defer close(done)
		sup.Run(ctx)
	}()
	recvWithin(t, entered, "store attempt")
	cancel()
	recvWithin(t, done, "supervisor exit")
}

// daemonFakeMySQL satisfies every MySQL surface the sidecar daemon wires.
type daemonFakeMySQL struct {
	mu       sync.Mutex
	readOnly bool
}

func (m *daemonFakeMySQL) queryStatus(context.Context) (*StatusInfo, error) {
	return &StatusInfo{Role: "replica", ReadOnly: true, SuperReadOnly: true}, nil
}
func (m *daemonFakeMySQL) isConnectable(context.Context) bool { return true }
func (m *daemonFakeMySQL) IsReadOnly(context.Context) (bool, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.readOnly, nil
}
func (m *daemonFakeMySQL) IsSuperReadOnly(context.Context) (bool, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.readOnly, nil
}
func (m *daemonFakeMySQL) SetSuperReadOnly(context.Context) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.readOnly = true
	return nil
}
func (m *daemonFakeMySQL) ClearSuperReadOnly(context.Context) error      { return nil }
func (m *daemonFakeMySQL) KillConnections(context.Context) (int, error)  { return 0, nil }
func (m *daemonFakeMySQL) RotateInnoDBMasterKey(context.Context) error   { return nil }
func (m *daemonFakeMySQL) EncryptSystemTablespace(context.Context) error { return nil }
func (m *daemonFakeMySQL) EncryptionCoverage(context.Context) (*KeyringCoverage, error) {
	return &KeyringCoverage{}, nil
}
func (m *daemonFakeMySQL) KeyringComponentStatus(context.Context) (*KeyringComponentStatus, error) {
	return &KeyringComponentStatus{}, nil
}

func daemonGet(t *testing.T, base, path string) (int, []byte) {
	t.Helper()
	resp, err := http.Get(base + path)
	if err != nil {
		t.Fatalf("GET %s: %v", path, err)
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatalf("read %s: %v", path, err)
	}
	return resp.StatusCode, body
}

// TestRunDaemonServesWhileArchiveStoreFails is the regression test for
// the startup crash loop: an archive store that cannot be built used to
// exit the process before HTTP, readiness and fencing started.
func TestRunDaemonServesWhileArchiveStoreFails(t *testing.T) {
	// The fencing monitor polls the operator's /healthz on every tick;
	// that request is the signal that fencing is running.
	fencingTicks := make(chan struct{}, 64)
	operator := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/healthz" {
			select {
			case fencingTicks <- struct{}{}:
			default:
			}
		}
		w.WriteHeader(http.StatusOK)
	}))
	defer operator.Close()

	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	base := "http://" + ln.Addr().String()

	cfg := &Config{
		ListenAddr:        ln.Addr().String(),
		BloodravenAddress: operator.Listener.Addr().String(),
		LeaseTimeout:      time.Hour,
		PeerCheckInterval: 10 * time.Millisecond,
		MySite:            "iad",
		PITR:              storeInitPITRConfig(t),
	}
	script := newStoreInitScript(2)
	backoff := newStoreInitBackoff()
	archivers := newStoreInitArchivers()

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() {
		done <- runDaemon(ctx, cfg, &daemonFakeMySQL{readOnly: true}, testLogger(), daemonOptions{
			listener:       ln,
			newStore:       script.factory,
			storeRetryWait: backoff.wait,
			runArchiver:    archivers.run,
		})
	}()

	for attempt := 1; attempt <= 2; attempt++ {
		recvWithin(t, script.calls, "store attempt")
		recvWithin(t, backoff.delays, "backoff wait")
		if code, body := daemonGet(t, base, "/health"); code != http.StatusOK {
			t.Fatalf("attempt %d: /health = %d %s, want 200 (readiness must not follow the archive store)", attempt, code, body)
		}
		recvWithin(t, fencingTicks, "fencing monitor tick")
		code, body := daemonGet(t, base, "/livez")
		var livez LivezStatus
		if err := json.Unmarshal(body, &livez); err != nil || code != http.StatusOK {
			t.Fatalf("attempt %d: /livez = %d %s (%v)", attempt, code, body, err)
		}
		if livez.FencingMonitor == nil || !livez.FencingMonitor.Started {
			t.Fatalf("attempt %d: fencing monitor not started: %s", attempt, body)
		}
		code, body = daemonGet(t, base, "/archiver/status")
		var st Status
		if err := json.Unmarshal(body, &st); err != nil || code != http.StatusOK {
			t.Fatalf("attempt %d: /archiver/status = %d %s (%v)", attempt, code, body, err)
		}
		if !st.Enabled || st.StoreReady == nil || *st.StoreReady ||
			st.LastError != fmt.Sprintf("archive store init: boom %d", attempt) || st.UploadFailures != int64(attempt) {
			t.Fatalf("attempt %d: /archiver/status = %s", attempt, body)
		}
		backoff.release <- struct{}{}
	}

	recvWithin(t, script.calls, "successful store attempt")
	recvWithin(t, archivers.started, "archiver start")
	_, body := daemonGet(t, base, "/archiver/status")
	var st Status
	if err := json.Unmarshal(body, &st); err != nil || st.StoreReady == nil || !*st.StoreReady {
		t.Fatalf("after success: /archiver/status = %s (%v)", body, err)
	}

	cancel()
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("runDaemon: %v", err)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("runDaemon did not return after shutdown")
	}
	if n := archivers.count.Load(); n != 1 {
		t.Fatalf("archiver started %d times, want 1", n)
	}
}

func TestRunDaemonRejectsBadRetentionInterval(t *testing.T) {
	t.Setenv("BLOODRAVEN_PITR_PROFILE_NAME", "nightly")
	t.Setenv("BLOODRAVEN_PITR_RETENTION_INTERVAL", "hourly")
	cfg := &Config{
		ListenAddr:        "127.0.0.1:0",
		BloodravenAddress: "127.0.0.1:1",
		PodNamespace:      "default",
		FailoverGroup:     "orders",
		MySite:            "iad",
		PeerCheckInterval: time.Second,
		LeaseTimeout:      time.Hour,
		PITR:              storeInitPITRConfig(t),
	}
	called := false
	err := runDaemon(context.Background(), cfg, &daemonFakeMySQL{}, testLogger(), daemonOptions{
		newStore: func(context.Context, *PITRConfig) (ArchiveStore, error) {
			called = true
			return nil, errors.New("unreachable")
		},
	})
	if err == nil {
		t.Fatal("runDaemon accepted an unparseable BLOODRAVEN_PITR_RETENTION_INTERVAL")
	}
	if called {
		t.Fatal("runDaemon started services before rejecting operator-rendered configuration")
	}
}
