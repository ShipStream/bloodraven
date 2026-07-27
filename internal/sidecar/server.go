package sidecar

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"net/http"
	"time"

	"github.com/shipstream/bloodraven/internal/metrics"
)

// Server is the sidecar HTTP server.
type Server struct {
	mysql      mysqlQuerier
	logger     *slog.Logger
	httpServer *http.Server
	archiver   *BinlogArchiver
	topology   *TopologyCache
	keyring    *KeyringAgent
}

// NewServer creates a new sidecar HTTP server.
func NewServer(mysql mysqlQuerier, listenAddr string, logger *slog.Logger) *Server {
	s := &Server{
		mysql:  mysql,
		logger: logger,
	}

	mux := http.NewServeMux()
	mux.HandleFunc("GET /health", s.instrument("/health", s.handleHealth))
	mux.HandleFunc("GET /status", s.instrument("/status", s.handleStatus))
	mux.HandleFunc("GET /peer/ping", s.instrument("/peer/ping", s.handlePeerPing))
	mux.HandleFunc("GET /peer/active-site", s.instrument("/peer/active-site", s.handlePeerActiveSite))
	mux.HandleFunc("GET /archiver/status", s.instrument("/archiver/status", s.handleArchiverStatus))
	mux.HandleFunc("GET /keyring/status", s.instrument("/keyring/status", s.handleKeyringStatus))

	s.httpServer = &http.Server{
		Addr:              listenAddr,
		Handler:           mux,
		ReadHeaderTimeout: 5 * time.Second,
		WriteTimeout:      10 * time.Second,
		IdleTimeout:       60 * time.Second,
	}

	return s
}

// instrument wraps a handler with Prometheus RED metrics for the
// sidecar HTTP surface (AUDIT M6). The handler label is fixed rather
// than derived from URL to keep the cardinality bounded.
func (s *Server) instrument(handlerName string, h http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		start := time.Now()
		sr := &sidecarStatusRecorder{ResponseWriter: w, status: http.StatusOK}
		h(sr, r)
		dur := time.Since(start)
		metrics.HTTPRequestsTotal.WithLabelValues("sidecar", handlerName, r.Method, metrics.StatusClass(sr.status)).Inc()
		metrics.HTTPRequestDurationSeconds.WithLabelValues("sidecar", handlerName, r.Method).Observe(dur.Seconds())
	}
}

type sidecarStatusRecorder struct {
	http.ResponseWriter
	status int
}

func (r *sidecarStatusRecorder) WriteHeader(code int) {
	r.status = code
	r.ResponseWriter.WriteHeader(code)
}

// SetTopology attaches the shared TopologyCache so the server can
// relay this sidecar's last-known authoritative view to peers via
// /peer/active-site. Optional: when unset, /peer/active-site returns
// 204 No Content, which peers treat as "no view available".
func (s *Server) SetTopology(c *TopologyCache) { s.topology = c }

// SetArchiver wires a BinlogArchiver into the server so its state can
// be exposed through /archiver/status. Optional: when unset, the
// endpoint returns a disabled payload.
func (s *Server) SetArchiver(a *BinlogArchiver) { s.archiver = a }

// Run starts the HTTP server and blocks until the context is cancelled.
func (s *Server) Run(ctx context.Context) error {
	errCh := make(chan error, 1)
	go func() {
		s.logger.Info("sidecar HTTP server starting", "addr", s.httpServer.Addr)
		if err := s.httpServer.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			errCh <- err
		}
		close(errCh)
	}()

	select {
	case <-ctx.Done():
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		return s.httpServer.Shutdown(shutdownCtx)
	case err := <-errCh:
		return err
	}
}

func (s *Server) handleHealth(w http.ResponseWriter, r *http.Request) {
	ctx, cancel := context.WithTimeout(r.Context(), 2*time.Second)
	defer cancel()

	if s.mysql.isConnectable(ctx) {
		w.WriteHeader(http.StatusOK)
		w.Write([]byte("ok"))
	} else {
		w.WriteHeader(http.StatusServiceUnavailable)
		w.Write([]byte("mysql unreachable"))
	}
}

func (s *Server) handleStatus(w http.ResponseWriter, r *http.Request) {
	ctx, cancel := context.WithTimeout(r.Context(), 2*time.Second)
	defer cancel()

	status, err := s.mysql.queryStatus(ctx)
	if err != nil {
		s.logger.Error("failed to query mysql status", "error", err)
		w.WriteHeader(http.StatusServiceUnavailable)
		json.NewEncoder(w).Encode(map[string]string{"error": err.Error()})
		return
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(status)
}

func (s *Server) handlePeerPing(w http.ResponseWriter, _ *http.Request) {
	w.WriteHeader(http.StatusOK)
	w.Write([]byte("pong"))
}

// handlePeerActiveSite relays this sidecar's last-known authoritative
// view of the failover group's active site to a peer. Peers use this
// as a partition-tolerant fallback when their own link to the
// operator is broken. The payload mirrors TopologySnapshot. Returns
// 204 when no view is cached yet (either topology-aware fencing is
// disabled, or the sidecar has never successfully queried the
// operator — the peer should treat either as "no hint available").
func (s *Server) handlePeerActiveSite(w http.ResponseWriter, _ *http.Request) {
	if s.topology == nil {
		w.WriteHeader(http.StatusNoContent)
		return
	}
	snap := s.topology.Snapshot()
	if snap.ActiveSite == "" || snap.ObservedAt.IsZero() {
		w.WriteHeader(http.StatusNoContent)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(snap)
}

// handleArchiverStatus returns the BinlogArchiver's Snapshot. When the
// archiver is disabled (PITR not configured for this failover group)
// we still return 200 + enabled:false so polling callers can tell
// "no archiver" apart from "archiver crashed".
func (s *Server) handleArchiverStatus(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	if s.archiver == nil {
		json.NewEncoder(w).Encode(Status{Enabled: false})
		return
	}
	ctx, cancel := context.WithTimeout(r.Context(), 2*time.Second)
	defer cancel()
	json.NewEncoder(w).Encode(s.archiver.Snapshot(ctx))
}

// SetKeyring wires the encryption-at-rest escrow agent into the server
// so the operator can read the live keyring digest and coverage through
// /keyring/status. Optional: when unset, the endpoint reports
// enabled:false, which the operator reads as "this site is not running
// encryption-at-rest".
func (s *Server) SetKeyring(a *KeyringAgent) { s.keyring = a }

// handleKeyringStatus reports the live keyring digest, the last escrow
// the operator confirmed, and the instance's observed encryption
// coverage.
//
// The payload deliberately contains no key material — only digests. The
// operator uses it to decide whether a site is safe to seal, and the
// answer must never depend on shipping the keyring over this
// unauthenticated endpoint.
func (s *Server) handleKeyringStatus(w http.ResponseWriter, _ *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	if s.keyring == nil {
		_ = json.NewEncoder(w).Encode(KeyringStatus{Enabled: false})
		return
	}
	_ = json.NewEncoder(w).Encode(s.keyring.Snapshot())
}

// activeSiteResponse is the JSON response from the operator's /active-site endpoint.
type activeSiteResponse struct {
	ActiveSite string `json:"activeSite"`
}

// RunSafetyNet prevents GTID divergence when a previously-primary pod restarts.
// It immediately fences MySQL (super_read_only=ON) before querying the operator,
// then clears the fence only if the operator confirms this is the active site.
// This "fence first, ask questions later" approach closes the race window where
// MySQL could commit internal transactions before the operator detects and fences
// the returning pod.
func (s *Server) RunSafetyNet(ctx context.Context, cfg *Config) {
	if cfg.MySite == "" || cfg.PodNamespace == "" || cfg.FailoverGroup == "" || cfg.BloodravenAddress == "" {
		s.logger.Info("safety net skipped: required identity not configured",
			"site", cfg.MySite, "namespace", cfg.PodNamespace,
			"fg", cfg.FailoverGroup, "bloodravenAddress", cfg.BloodravenAddress)
		return
	}

	// Fence immediately — prevent any writes until we confirm our role.
	if err := s.mysql.SetSuperReadOnly(ctx); err != nil {
		s.logger.Warn("safety net: could not set initial super_read_only, continuing", "error", err)
	} else {
		s.logger.Info("safety net: set super_read_only=ON as precaution on startup")
	}

	activeSite, err := s.queryActiveSite(ctx, cfg)
	if err != nil {
		s.logger.Warn("safety net: could not query active site, staying fenced", "error", err)
		return
	}

	if activeSite == "" {
		s.logger.Info("safety net: no active site reported by operator, staying fenced")
		return
	}

	// Seed the shared topology cache with the operator's answer.
	// The fencing monitor re-polls on its own cadence, but this
	// guarantees the cache is warm from the first tick onward and
	// gives peers a usable /peer/active-site response immediately.
	if s.topology != nil {
		s.topology.Set(activeSite, time.Now())
	}

	if cfg.MySite != activeSite {
		s.logger.Info("safety net: confirmed standby site, staying fenced",
			"site", cfg.MySite, "activeSite", activeSite)
		return
	}

	// We are the active site — clear the fence so the primary can accept writes.
	s.logger.Info("safety net: this is the active site, clearing super_read_only", "site", cfg.MySite)
	if err := s.mysql.ClearSuperReadOnly(ctx); err != nil {
		s.logger.Error("safety net: failed to clear super_read_only on active site", "error", err)
	}
}

func (s *Server) queryActiveSite(ctx context.Context, cfg *Config) (string, error) {
	reqCtx, cancel := context.WithTimeout(ctx, 2*time.Second)
	defer cancel()

	url := fmt.Sprintf("http://%s/active-site?namespace=%s&group=%s",
		cfg.BloodravenAddress, cfg.PodNamespace, cfg.FailoverGroup)
	req, err := http.NewRequestWithContext(reqCtx, http.MethodGet, url, nil)
	if err != nil {
		return "", fmt.Errorf("create request: %w", err)
	}

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return "", fmt.Errorf("request operator: %w", err)
	}
	defer resp.Body.Close()

	switch resp.StatusCode {
	case http.StatusOK:
		var result activeSiteResponse
		if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
			return "", fmt.Errorf("decode response: %w", err)
		}
		return result.ActiveSite, nil
	case http.StatusServiceUnavailable:
		return "", fmt.Errorf("operator not ready (503)")
	case http.StatusNotFound:
		return "", fmt.Errorf("failover group %s/%s not found (404)", cfg.PodNamespace, cfg.FailoverGroup)
	default:
		return "", fmt.Errorf("unexpected status %d", resp.StatusCode)
	}
}
