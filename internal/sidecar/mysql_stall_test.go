package sidecar

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/shipstream/bloodraven/internal/clock"
)

// hangingMysql models a mysqld that accepted the connection and then
// stopped answering (SIGSTOP, a long crash recovery, a CLONE restart): every
// call blocks until its context ends. A caller that passes an unbounded
// context never gets an answer.
type hangingMysql struct{}

func (hangingMysql) block(ctx context.Context) error {
	<-ctx.Done()
	return ctx.Err()
}

func (h hangingMysql) queryStatus(ctx context.Context) (*StatusInfo, error) {
	return nil, h.block(ctx)
}
func (h hangingMysql) isConnectable(ctx context.Context) bool { _ = h.block(ctx); return false }
func (h hangingMysql) IsReadOnly(ctx context.Context) (bool, error) {
	return false, h.block(ctx)
}
func (h hangingMysql) IsSuperReadOnly(ctx context.Context) (bool, error) {
	return false, h.block(ctx)
}
func (h hangingMysql) SetSuperReadOnly(ctx context.Context) error   { return h.block(ctx) }
func (h hangingMysql) ClearSuperReadOnly(ctx context.Context) error { return h.block(ctx) }
func (h hangingMysql) KillConnections(ctx context.Context) (int, error) {
	return 0, h.block(ctx)
}

// serveWithin runs one request against the server's handler and fails the
// test if the handler has not answered within d.
func serveWithin(t *testing.T, srv *Server, path string, d time.Duration) *httptest.ResponseRecorder {
	t.Helper()
	w := httptest.NewRecorder()
	done := make(chan struct{})
	go func() {
		defer close(done)
		srv.httpServer.Handler.ServeHTTP(w, httptest.NewRequest(http.MethodGet, path, nil))
	}()
	select {
	case <-done:
		return w
	case <-time.After(d):
		t.Fatalf("GET %s did not answer within %s", path, d)
		return nil
	}
}

// Liveness must not depend on MySQL. The sidecar is the component that
// fences MySQL when it misbehaves, so a liveness signal coupled to MySQL
// kills the fencing agent exactly when it is needed.
func TestLivezAnswersWhileMySQLHangs(t *testing.T) {
	srv := NewServer(hangingMysql{}, ":0", testLogger())

	w := serveWithin(t, srv, "/livez", 500*time.Millisecond)
	if w.Code != http.StatusOK {
		t.Fatalf("/livez = %d while MySQL hangs, want 200 (body %q)", w.Code, w.Body.String())
	}
}

// Readiness does report MySQL, but within its own bound, so the kubelet's
// probe timeout sees a 503 instead of a timeout.
func TestHealthAnswersWithinItsBoundWhileMySQLHangs(t *testing.T) {
	srv := NewServer(hangingMysql{}, ":0", testLogger())

	w := serveWithin(t, srv, "/health", 5*time.Second)
	if w.Code != http.StatusServiceUnavailable {
		t.Fatalf("/health = %d while MySQL hangs, want 503", w.Code)
	}
}

// A fencing tick against a hung mysqld must return, so the loop is still
// running when MySQL resumes. With an unbounded read_only probe the loop
// would sit on the stalled connection, and a SIGSTOPped mysqld can hold it
// for as long as it stays stopped.
func TestFencingCheckIsBoundedWhileMySQLHangs(t *testing.T) {
	fm := NewFencingMonitorFull(hangingMysql{}, "127.0.0.1:8081", []string{testPeerAddr},
		5*time.Second, 20*time.Second, testLogger(), clock.RealClock{},
		&http.Client{Transport: noopTransport{}})

	done := make(chan struct{})
	go func() {
		defer close(done)
		fm.Check(context.Background())
	}()
	select {
	case <-done:
	case <-time.After(15 * time.Second):
		t.Fatal("fencing Check did not return while MySQL hangs; the monitor loop is wedged")
	}
}

// The startup safety net runs before the fencing monitor starts. A fence
// write against a hung mysqld must not hold the process there: until it
// returns, nothing else on this node can fence.
func TestSafetyNetReturnsWithinItsBoundWhenMySQLHangs(t *testing.T) {
	op := fakeOperator("site1", http.StatusOK)
	defer op.Close()

	srv := NewServer(hangingMysql{}, ":0", testLogger())
	cache := &TopologyCache{}
	srv.SetTopology(cache)

	done := make(chan struct{})
	start := time.Now()
	go func() {
		defer close(done)
		srv.RunSafetyNet(context.Background(), safetyNetConfig(op.Listener.Addr().String(), "site2"))
	}()

	select {
	case <-done:
	case <-time.After(15 * time.Second):
		t.Fatal("RunSafetyNet did not return while MySQL hangs; startup is wedged before the fencing monitor runs")
	}
	if elapsed := time.Since(start); elapsed > 10*time.Second {
		t.Errorf("RunSafetyNet took %s against a hung MySQL", elapsed)
	}
	// The operator answer is still recorded, so the monitor and peers have
	// an authoritative view from the first tick.
	if got := cache.Snapshot().ActiveSite; got != "site1" {
		t.Errorf("topology cache = %q after a timed-out fence, want site1", got)
	}
}
