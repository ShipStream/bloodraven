package sidecar

import (
	"context"
	"encoding/json"
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

// RunSafetyNet checks MySQL state on startup and sets super_read_only=ON
// if this sidecar is not in the active site but MySQL has read_only=OFF.
func (s *Server) RunSafetyNet(ctx context.Context, mySite, activeSite string) {
	if mySite == "" || activeSite == "" {
		s.logger.Info("safety net skipped: MY_SITE or ACTIVE_SITE not set")
		return
	}

	if mySite == activeSite {
		s.logger.Info("safety net: this is the active site, no action needed", "site", mySite)
		return
	}

	readOnly, err := s.mysql.IsReadOnly(ctx)
	if err != nil {
		s.logger.Warn("safety net: could not query read_only, will retry on next startup", "error", err)
		return
	}

	if !readOnly {
		s.logger.Warn("SAFETY NET: standby site has read_only=OFF, setting super_read_only=ON",
			"my_site", mySite, "active_site", activeSite)
		if err := s.mysql.SetSuperReadOnly(ctx); err != nil {
			s.logger.Error("safety net: failed to set super_read_only", "error", err)
		} else {
			s.logger.Info("safety net: super_read_only=ON set successfully")
		}
	}
}
