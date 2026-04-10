package sidecar

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"net/http"
	"time"
)

// Server is the sidecar HTTP server.
type Server struct {
	mysql      mysqlQuerier
	logger     *slog.Logger
	httpServer *http.Server
}

// NewServer creates a new sidecar HTTP server.
func NewServer(mysql mysqlQuerier, listenAddr string, logger *slog.Logger) *Server {
	s := &Server{
		mysql:  mysql,
		logger: logger,
	}

	mux := http.NewServeMux()
	mux.HandleFunc("GET /health", s.handleHealth)
	mux.HandleFunc("GET /status", s.handleStatus)
	mux.HandleFunc("GET /peer/ping", s.handlePeerPing)

	s.httpServer = &http.Server{
		Addr:              listenAddr,
		Handler:           mux,
		ReadHeaderTimeout: 5 * time.Second,
		WriteTimeout:      10 * time.Second,
		IdleTimeout:       60 * time.Second,
	}

	return s
}

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

// activeSiteResponse is the JSON response from the operator's /active-site endpoint.
type activeSiteResponse struct {
	ActiveSite string `json:"active_site"`
}

// RunSafetyNet queries the operator for the active site and sets super_read_only=ON
// if this sidecar is on a standby site but MySQL has read_only=OFF.
func (s *Server) RunSafetyNet(ctx context.Context, cfg *Config) {
	if cfg.MySite == "" || cfg.PodNamespace == "" || cfg.FailoverGroup == "" || cfg.BloodravenAddress == "" {
		s.logger.Info("safety net skipped: required identity not configured",
			"my_site", cfg.MySite, "namespace", cfg.PodNamespace,
			"failover_group", cfg.FailoverGroup, "bloodraven_address", cfg.BloodravenAddress)
		return
	}

	activeSite, err := s.queryActiveSite(ctx, cfg)
	if err != nil {
		s.logger.Warn("safety net skipped: could not query active site from operator", "error", err)
		return
	}

	if activeSite == "" {
		s.logger.Info("safety net skipped: no active site reported by operator")
		return
	}

	if cfg.MySite == activeSite {
		s.logger.Info("safety net: this is the active site, no action needed", "site", cfg.MySite)
		return
	}

	readOnly, err := s.mysql.IsReadOnly(ctx)
	if err != nil {
		s.logger.Warn("safety net: could not query read_only, will retry on next startup", "error", err)
		return
	}

	if !readOnly {
		s.logger.Warn("SAFETY NET: standby site has read_only=OFF, setting super_read_only=ON",
			"my_site", cfg.MySite, "active_site", activeSite)
		if err := s.mysql.SetSuperReadOnly(ctx); err != nil {
			s.logger.Error("safety net: failed to set super_read_only", "error", err)
		} else {
			s.logger.Info("safety net: super_read_only=ON set successfully")
		}
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
