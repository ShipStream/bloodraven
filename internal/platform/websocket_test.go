//go:build integration

package platform

import (
	"encoding/json"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/gorilla/websocket"
)

// waitForClientCount polls hub.ClientCount until it matches want or the deadline expires.
func waitForClientCount(t *testing.T, hub *Hub, want int) {
	t.Helper()
	deadline := time.After(2 * time.Second)
	for hub.ClientCount() != want {
		select {
		case <-deadline:
			t.Fatalf("timed out waiting for %d clients, got %d", want, hub.ClientCount())
		default:
			time.Sleep(5 * time.Millisecond)
		}
	}
}

func TestHub_BroadcastToClients(t *testing.T) {
	logger := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelWarn}))
	hub := NewHub(logger)

	srv := httptest.NewServer(http.HandlerFunc(hub.HandleWS))
	defer srv.Close()

	wsURL := "ws" + strings.TrimPrefix(srv.URL, "http") + "/ws/status"

	// Connect two clients
	c1, _, err := websocket.DefaultDialer.Dial(wsURL, nil)
	if err != nil {
		t.Fatalf("dial c1: %v", err)
	}
	defer c1.Close()

	c2, _, err := websocket.DefaultDialer.Dial(wsURL, nil)
	if err != nil {
		t.Fatalf("dial c2: %v", err)
	}
	defer c2.Close()

	// Wait for connections to register
	waitForClientCount(t, hub, 2)

	// Broadcast a message
	hub.Broadcast(WSMessage{Site: "site1", Status: "offline"})

	// Both clients should receive it
	for i, c := range []*websocket.Conn{c1, c2} {
		c.SetReadDeadline(time.Now().Add(2 * time.Second))
		_, data, err := c.ReadMessage()
		if err != nil {
			t.Fatalf("client %d read: %v", i, err)
		}
		var msg WSMessage
		if err := json.Unmarshal(data, &msg); err != nil {
			t.Fatalf("client %d unmarshal: %v", i, err)
		}
		if msg.Site != "site1" || msg.Status != "offline" {
			t.Errorf("client %d: got %+v, want site1/offline", i, msg)
		}
	}
}

func TestHub_ClientDisconnect(t *testing.T) {
	logger := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelWarn}))
	hub := NewHub(logger)

	srv := httptest.NewServer(http.HandlerFunc(hub.HandleWS))
	defer srv.Close()

	wsURL := "ws" + strings.TrimPrefix(srv.URL, "http")

	c, _, err := websocket.DefaultDialer.Dial(wsURL, nil)
	if err != nil {
		t.Fatalf("dial: %v", err)
	}

	waitForClientCount(t, hub, 1)

	c.Close()

	waitForClientCount(t, hub, 0)
}
