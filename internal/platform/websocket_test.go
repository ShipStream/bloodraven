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
	time.Sleep(50 * time.Millisecond)

	if hub.ClientCount() != 2 {
		t.Fatalf("expected 2 clients, got %d", hub.ClientCount())
	}

	// Broadcast a message
	hub.Broadcast(WSMessage{DC: "dc1", Status: "offline"})

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
		if msg.DC != "dc1" || msg.Status != "offline" {
			t.Errorf("client %d: got %+v, want dc1/offline", i, msg)
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

	time.Sleep(50 * time.Millisecond)
	if hub.ClientCount() != 1 {
		t.Fatalf("expected 1 client, got %d", hub.ClientCount())
	}

	c.Close()
	time.Sleep(100 * time.Millisecond)

	if hub.ClientCount() != 0 {
		t.Errorf("expected 0 clients after disconnect, got %d", hub.ClientCount())
	}
}
