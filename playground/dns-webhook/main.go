// dns-webhook is a minimal external-dns webhook provider for the Bloodraven
// playground. It stores DNS records in memory and exposes them via a REST API
// so the dashboard can display what external-dns would write in production.
//
// Implements the external-dns webhook provider protocol:
//   GET  /              → negotiate API version
//   GET  /records       → return current records
//   POST /records       → apply record changes
//   POST /adjustendpoints → passthrough (no adjustment)
//
// Extra endpoint for the dashboard:
//   GET  /api/records   → same as /records but with CORS headers
package main

import (
	"encoding/json"
	"log"
	"net/http"
	"os"
	"sync"
	"time"
)

// Endpoint mirrors the external-dns endpoint type.
type Endpoint struct {
	DNSName    string   `json:"dnsName"`
	Targets    []string `json:"targets"`
	RecordType string   `json:"recordType"`
	RecordTTL  int      `json:"recordTTL,omitempty"`
}

// Changes mirrors the external-dns plan changes.
type Changes struct {
	Create    []*Endpoint `json:"Create"`
	UpdateOld []*Endpoint `json:"UpdateOld"`
	UpdateNew []*Endpoint `json:"UpdateNew"`
	Delete    []*Endpoint `json:"Delete"`
}

// RecordEvent is logged for the dashboard event stream.
type RecordEvent struct {
	Time   string `json:"time"`
	Action string `json:"action"` // "create", "update", "delete"
	Record Endpoint `json:"record"`
}

type store struct {
	mu      sync.RWMutex
	records map[string]*Endpoint // key: "type/dnsName"
	events  []RecordEvent
}

func newStore() *store {
	return &store{
		records: make(map[string]*Endpoint),
	}
}

func (s *store) key(ep *Endpoint) string {
	return ep.RecordType + "/" + ep.DNSName
}

func (s *store) apply(changes Changes) {
	s.mu.Lock()
	defer s.mu.Unlock()

	now := time.Now().UTC().Format(time.RFC3339)

	for _, ep := range changes.Create {
		s.records[s.key(ep)] = ep
		s.events = append(s.events, RecordEvent{Time: now, Action: "create", Record: *ep})
		log.Printf("CREATE %s %s -> %v (TTL %d)", ep.RecordType, ep.DNSName, ep.Targets, ep.RecordTTL)
	}
	for _, ep := range changes.UpdateNew {
		s.records[s.key(ep)] = ep
		s.events = append(s.events, RecordEvent{Time: now, Action: "update", Record: *ep})
		log.Printf("UPDATE %s %s -> %v (TTL %d)", ep.RecordType, ep.DNSName, ep.Targets, ep.RecordTTL)
	}
	for _, ep := range changes.Delete {
		delete(s.records, s.key(ep))
		s.events = append(s.events, RecordEvent{Time: now, Action: "delete", Record: *ep})
		log.Printf("DELETE %s %s", ep.RecordType, ep.DNSName)
	}

	// Keep last 100 events
	if len(s.events) > 100 {
		s.events = s.events[len(s.events)-100:]
	}
}

func (s *store) list() []*Endpoint {
	s.mu.RLock()
	defer s.mu.RUnlock()
	out := make([]*Endpoint, 0, len(s.records))
	for _, ep := range s.records {
		out = append(out, ep)
	}
	return out
}

func (s *store) listEvents() []RecordEvent {
	s.mu.RLock()
	defer s.mu.RUnlock()
	out := make([]RecordEvent, len(s.events))
	copy(out, s.events)
	return out
}

func main() {
	addr := os.Getenv("LISTEN_ADDR")
	if addr == "" {
		addr = ":8888"
	}

	apiAddr := os.Getenv("API_LISTEN_ADDR")
	if apiAddr == "" {
		apiAddr = ":8889"
	}

	s := newStore()

	// ── Webhook provider endpoints (port 8888) ─────────────────────────
	webhookMux := http.NewServeMux()

	// Negotiate: return supported media type
	webhookMux.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodGet {
			w.Header().Set("Content-Type", "application/external.dns.webhook+json;version=1")
			json.NewEncoder(w).Encode(map[string]string{})
			return
		}
		http.NotFound(w, r)
	})

	// GET /records — return all records
	webhookMux.HandleFunc("/records", func(w http.ResponseWriter, r *http.Request) {
		switch r.Method {
		case http.MethodGet:
			w.Header().Set("Content-Type", "application/external.dns.webhook+json;version=1")
			json.NewEncoder(w).Encode(s.list())

		case http.MethodPost:
			// Apply changes
			var changes Changes
			if err := json.NewDecoder(r.Body).Decode(&changes); err != nil {
				http.Error(w, err.Error(), http.StatusBadRequest)
				return
			}
			s.apply(changes)
			w.WriteHeader(http.StatusNoContent)

		default:
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		}
	})

	// POST /adjustendpoints — passthrough, no adjustment needed
	webhookMux.HandleFunc("/adjustendpoints", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}
		var endpoints []*Endpoint
		if err := json.NewDecoder(r.Body).Decode(&endpoints); err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		// Return unmodified
		w.Header().Set("Content-Type", "application/external.dns.webhook+json;version=1")
		json.NewEncoder(w).Encode(endpoints)
	})

	// Healthz
	webhookMux.HandleFunc("/healthz", func(w http.ResponseWriter, _ *http.Request) {
		w.Write([]byte("ok"))
	})

	// ── Dashboard API endpoints (port 8889) ────────────────────────────
	apiMux := http.NewServeMux()

	apiMux.HandleFunc("/api/records", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.Header().Set("Access-Control-Allow-Origin", "*")
		json.NewEncoder(w).Encode(s.list())
	})

	apiMux.HandleFunc("/api/events", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.Header().Set("Access-Control-Allow-Origin", "*")
		json.NewEncoder(w).Encode(s.listEvents())
	})

	apiMux.HandleFunc("/healthz", func(w http.ResponseWriter, _ *http.Request) {
		w.Write([]byte("ok"))
	})

	// Start both servers
	go func() {
		log.Printf("dns-webhook provider listening on %s", addr)
		log.Fatal(http.ListenAndServe(addr, webhookMux))
	}()

	log.Printf("dns-webhook API listening on %s", apiAddr)
	log.Fatal(http.ListenAndServe(apiAddr, apiMux))
}
