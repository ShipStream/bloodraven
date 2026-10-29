package platform

import (
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/gorilla/websocket"
)

type wsDialResult struct {
	conn   *websocket.Conn
	status int
	err    error
}

func dialHub(srvURL string) wsDialResult {
	c, resp, err := websocket.DefaultDialer.Dial("ws"+strings.TrimPrefix(srvURL, "http"), nil)
	res := wsDialResult{conn: c, err: err}
	if resp != nil {
		res.status = resp.StatusCode
		if resp.Body != nil {
			_ = resp.Body.Close()
		}
	}
	return res
}

// awaitClientCount waits for the hub to register want clients: the dialer
// returns once it reads the 101 response, slightly before the handler
// registers the client.
func awaitClientCount(t *testing.T, hub *Hub, want int) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for hub.ClientCount() != want {
		if time.Now().After(deadline) {
			t.Fatalf("ClientCount() = %d, want %d", hub.ClientCount(), want)
		}
		time.Sleep(time.Millisecond)
	}
}

func cappedHub(t *testing.T, max int) *Hub {
	t.Helper()
	t.Setenv("BLOODRAVEN_WS_ALLOWED_ORIGINS", "")
	t.Setenv("BLOODRAVEN_WS_MAX_CLIENTS", "")
	hub := NewHub(slog.New(slog.NewTextHandler(io.Discard, nil)))
	hub.maxClients = max
	return hub
}

// An upgrade that has been admitted but not yet registered holds its slot:
// with the cap at 1, a second client arriving while the first is still in
// its handshake must be refused with 429, not upgraded past the cap.
func TestHub_InFlightUpgradeHoldsCapSlot(t *testing.T) {
	hub := cappedHub(t, 1)
	inHandshake := make(chan struct{})
	release := make(chan struct{})
	var checks atomic.Int32
	// CheckOrigin runs inside Upgrade, after admission and before the
	// client is registered: pause the first handshake there.
	hub.upgrader.CheckOrigin = func(*http.Request) bool {
		if checks.Add(1) == 1 {
			close(inHandshake)
			<-release
		}
		return true
	}
	srv := httptest.NewServer(http.HandlerFunc(hub.HandleWS))
	defer srv.Close()

	first := make(chan wsDialResult, 1)
	go func() { first <- dialHub(srv.URL) }()
	<-inHandshake

	second := dialHub(srv.URL)
	if second.conn != nil {
		_ = second.conn.Close()
	}
	close(release)
	a := <-first
	if a.err != nil {
		t.Fatalf("first client: %v", a.err)
	}
	defer a.conn.Close()

	if second.err == nil || second.status != http.StatusTooManyRequests {
		t.Fatalf("second client during the first handshake: err=%v status=%d, want 429", second.err, second.status)
	}
	awaitClientCount(t, hub, 1)
}

// A failed upgrade gives its reserved slot back.
func TestHub_FailedUpgradeReleasesSlot(t *testing.T) {
	hub := cappedHub(t, 1)
	srv := httptest.NewServer(http.HandlerFunc(hub.HandleWS))
	defer srv.Close()

	resp, err := http.Get(srv.URL) // not a websocket handshake
	if err != nil {
		t.Fatalf("plain GET: %v", err)
	}
	_ = resp.Body.Close()
	if resp.StatusCode == http.StatusSwitchingProtocols {
		t.Fatal("plain GET was upgraded")
	}

	res := dialHub(srv.URL)
	if res.err != nil {
		t.Fatalf("dial after a failed upgrade: err=%v status=%d, want the slot released", res.err, res.status)
	}
	defer res.conn.Close()
	awaitClientCount(t, hub, 1)
}

// A disconnected client frees its slot for the next one, and the cap still
// refuses a client beyond it.
func TestHub_CapAdmitsAfterDisconnect(t *testing.T) {
	hub := cappedHub(t, 1)
	srv := httptest.NewServer(http.HandlerFunc(hub.HandleWS))
	defer srv.Close()

	a := dialHub(srv.URL)
	if a.err != nil {
		t.Fatalf("first client: %v", a.err)
	}
	awaitClientCount(t, hub, 1)
	if over := dialHub(srv.URL); over.err == nil || over.status != http.StatusTooManyRequests {
		if over.conn != nil {
			_ = over.conn.Close()
		}
		t.Fatalf("client over the cap: err=%v status=%d, want 429", over.err, over.status)
	}

	_ = a.conn.Close()
	awaitClientCount(t, hub, 0)
	b := dialHub(srv.URL)
	if b.err != nil {
		t.Fatalf("client after disconnect: err=%v status=%d", b.err, b.status)
	}
	defer b.conn.Close()
	awaitClientCount(t, hub, 1)
}

// A non-positive cap means unlimited, including while handshakes overlap.
func TestHub_UnlimitedCapAdmitsOverlappingHandshakes(t *testing.T) {
	hub := cappedHub(t, 0)
	inHandshake := make(chan struct{})
	release := make(chan struct{})
	var checks atomic.Int32
	hub.upgrader.CheckOrigin = func(*http.Request) bool {
		if checks.Add(1) == 1 {
			close(inHandshake)
			<-release
		}
		return true
	}
	srv := httptest.NewServer(http.HandlerFunc(hub.HandleWS))
	defer srv.Close()

	first := make(chan wsDialResult, 1)
	go func() { first <- dialHub(srv.URL) }()
	<-inHandshake
	second := dialHub(srv.URL)
	close(release)
	a := <-first
	for i, r := range []wsDialResult{a, second} {
		if r.err != nil {
			t.Fatalf("client %d with no cap: err=%v status=%d", i, r.err, r.status)
		}
		defer r.conn.Close()
	}
	awaitClientCount(t, hub, 2)
}
