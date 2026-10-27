package platform

import (
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"net/http/httputil"
	"net/url"
	"strings"
	"testing"

	"github.com/gorilla/websocket"
)

func dialWithOrigin(t *testing.T, srvURL, origin string) bool {
	t.Helper()
	h := http.Header{}
	if origin != "" {
		h.Set("Origin", origin)
	}
	c, resp, err := websocket.DefaultDialer.Dial("ws"+strings.TrimPrefix(srvURL, "http"), h)
	if resp != nil && resp.Body != nil {
		_ = resp.Body.Close()
	}
	if err != nil {
		return false
	}
	_ = c.Close()
	return true
}

// TestHubOriginPolicy pins the /ws/status Origin defaults: same-origin
// when unset, any origin only when "*" is set explicitly, and an exact
// allowlist otherwise.
func TestHubOriginPolicy(t *testing.T) {
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	for _, tc := range []struct {
		name    string
		env     string
		origin  func(srvURL string) string
		allowed bool
	}{
		{"default rejects cross origin", "", func(string) string { return "https://evil.example" }, false},
		{"default allows same origin", "", func(u string) string { return u }, true},
		{"default allows missing origin", "", func(string) string { return "" }, true},
		{"wildcard allows cross origin", "*", func(string) string { return "https://evil.example" }, true},
		{"allowlist allows listed", "https://dash.example", func(string) string { return "https://dash.example" }, true},
		{"allowlist rejects unlisted", "https://dash.example", func(string) string { return "https://evil.example" }, false},
		{"allowlist rejects missing origin", "https://dash.example", func(string) string { return "" }, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Setenv("BLOODRAVEN_WS_ALLOWED_ORIGINS", tc.env)
			hub := NewHub(logger)
			srv := httptest.NewServer(http.HandlerFunc(hub.HandleWS))
			defer srv.Close()
			if got := dialWithOrigin(t, srv.URL, tc.origin(srv.URL)); got != tc.allowed {
				t.Errorf("upgrade allowed = %v, want %v", got, tc.allowed)
			}
		})
	}
}

// TestHubDefaultOriginBehindReverseProxy mirrors the playground
// dashboard, which serves the page and proxies /ws/status to the
// operator with httputil.ReverseProxy (Host header preserved).
func TestHubDefaultOriginBehindReverseProxy(t *testing.T) {
	t.Setenv("BLOODRAVEN_WS_ALLOWED_ORIGINS", "")
	hub := NewHub(slog.New(slog.NewTextHandler(io.Discard, nil)))
	backend := httptest.NewServer(http.HandlerFunc(hub.HandleWS))
	defer backend.Close()
	target, _ := url.Parse(backend.URL)
	front := httptest.NewServer(httputil.NewSingleHostReverseProxy(target))
	defer front.Close()
	if !dialWithOrigin(t, front.URL, front.URL) {
		t.Fatal("same-origin dashboard upgrade through reverse proxy was rejected")
	}
}
