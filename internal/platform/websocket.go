package platform

import (
	"encoding/json"
	"log/slog"
	"net/http"
	"os"
	"strconv"
	"strings"
	"sync"

	"github.com/gorilla/websocket"
)

// TopologyMessage is the rich WebSocket broadcast payload describing the
// current state of a single failover group. It is sent to all connected
// /ws/status clients on every poll cycle.
type TopologyMessage struct {
	Namespace             string         `json:"namespace"`
	Group                 string         `json:"group"`
	ActiveSite            string         `json:"activeSite"`
	Sites                 []TopologySite `json:"sites"`
	LastFailover          string         `json:"lastFailover,omitempty"`
	LastFailoverTarget    string         `json:"lastFailoverTarget,omitempty"`
	Alert                 string         `json:"alert,omitempty"`
	UpdatePhase           string         `json:"updatePhase,omitempty"`
	PollTime              string         `json:"pollTime"`
	PromotionGtidExecuted string         `json:"promotionGtidExecuted,omitempty"`
}

// TopologySite is a single site entry inside a TopologyMessage.
type TopologySite struct {
	Name                      string `json:"name"`
	Role                      string `json:"role,omitempty"`
	State                     string `json:"state"`
	LastSeen                  string `json:"lastSeen,omitempty"`
	Replicating               bool   `json:"replicating"`
	SecondsBehindSource       *int64 `json:"secondsBehindSource,omitempty"`
	GtidExecuted              string `json:"gtidExecuted,omitempty"`
	RecoveryState             string `json:"recoveryState,omitempty"`
	DivergentGtid             string `json:"divergentGtid,omitempty"`
	DivergentTransactionCount *int64 `json:"divergentTransactionCount,omitempty"`
}

// Hub manages websocket connections and broadcasts state changes.
type Hub struct {
	mu             sync.RWMutex
	clients        map[*websocket.Conn]struct{}
	logger         *slog.Logger
	upgrader       websocket.Upgrader
	allowedOrigins map[string]struct{} // nil == allow all (legacy)
	maxClients     int                 // 0 == unlimited
}

// NewHub builds a Hub and applies the AUDIT H2 hardening:
//
//   - BLOODRAVEN_WS_ALLOWED_ORIGINS (comma-separated) narrows the
//     Origin header allowlist; default "*" preserves the pre-hardening
//     behavior so dashboards that don't send Origin keep working.
//   - BLOODRAVEN_WS_MAX_CLIENTS caps the concurrent connection count;
//     extra upgrades are rejected with 429. Default 100.
func NewHub(logger *slog.Logger) *Hub {
	h := &Hub{
		clients:    make(map[*websocket.Conn]struct{}),
		logger:     logger,
		maxClients: envInt("BLOODRAVEN_WS_MAX_CLIENTS", 100),
	}
	if v := strings.TrimSpace(os.Getenv("BLOODRAVEN_WS_ALLOWED_ORIGINS")); v != "" && v != "*" {
		h.allowedOrigins = make(map[string]struct{})
		for _, o := range strings.Split(v, ",") {
			o = strings.TrimSpace(o)
			if o != "" {
				h.allowedOrigins[o] = struct{}{}
			}
		}
	}
	h.upgrader = websocket.Upgrader{CheckOrigin: h.checkOrigin}
	return h
}

func (h *Hub) checkOrigin(r *http.Request) bool {
	if h.allowedOrigins == nil {
		return true
	}
	origin := r.Header.Get("Origin")
	if origin == "" {
		// Non-browser clients typically omit Origin. Accept only when
		// the allowlist is unset (handled above); otherwise reject.
		return false
	}
	_, ok := h.allowedOrigins[origin]
	return ok
}

// HandleWS is the HTTP handler for /ws/status.
func (h *Hub) HandleWS(w http.ResponseWriter, r *http.Request) {
	if h.maxClients > 0 {
		h.mu.RLock()
		n := len(h.clients)
		h.mu.RUnlock()
		if n >= h.maxClients {
			http.Error(w, "too many websocket clients", http.StatusTooManyRequests)
			return
		}
	}
	conn, err := h.upgrader.Upgrade(w, r, nil)
	if err != nil {
		h.logger.Error("websocket upgrade failed", "error", err)
		return
	}

	h.mu.Lock()
	h.clients[conn] = struct{}{}
	h.mu.Unlock()

	h.logger.Info("websocket client connected", "remote", conn.RemoteAddr())

	// Read loop to detect disconnects.
	go func() {
		defer func() {
			h.mu.Lock()
			delete(h.clients, conn)
			h.mu.Unlock()
			conn.Close()
			h.logger.Info("websocket client disconnected", "remote", conn.RemoteAddr())
		}()
		for {
			if _, _, err := conn.ReadMessage(); err != nil {
				return
			}
		}
	}()
}

// Broadcast sends a topology message to all connected clients.
func (h *Hub) Broadcast(msg TopologyMessage) {
	data, err := json.Marshal(msg)
	if err != nil {
		h.logger.Error("marshal ws message", "error", err)
		return
	}

	h.mu.Lock()
	defer h.mu.Unlock()

	for conn := range h.clients {
		if err := conn.WriteMessage(websocket.TextMessage, data); err != nil {
			h.logger.Warn("write to ws client failed", "remote", conn.RemoteAddr(), "error", err)
		}
	}
}

// ClientCount returns the number of connected websocket clients.
func (h *Hub) ClientCount() int {
	h.mu.RLock()
	defer h.mu.RUnlock()
	return len(h.clients)
}

func envInt(name string, def int) int {
	v := strings.TrimSpace(os.Getenv(name))
	if v == "" {
		return def
	}
	n, err := strconv.Atoi(v)
	if err != nil || n < 0 {
		return def
	}
	return n
}
