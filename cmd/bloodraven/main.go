package main

import (
	"context"
	"encoding/json"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"

	"github.com/shipstream/bloodraven/internal/config"
	"github.com/shipstream/bloodraven/internal/controller"
	"github.com/shipstream/bloodraven/internal/metrics"
	"github.com/shipstream/bloodraven/internal/mysql"
	"github.com/shipstream/bloodraven/internal/platform"
)

func main() {
	logger := slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelInfo}))

	cfg, err := config.LoadConfig()
	if err != nil {
		logger.Error("failed to load config", "error", err)
		os.Exit(1)
	}

	// MySQL connections
	dc1MySQL, err := mysql.NewChecker(cfg.DC1.MysqlDSN)
	if err != nil {
		logger.Error("failed to connect to dc1 mysql", "error", err)
		os.Exit(1)
	}
	defer dc1MySQL.Close()

	dc2MySQL, err := mysql.NewChecker(cfg.DC2.MysqlDSN)
	if err != nil {
		logger.Error("failed to connect to dc2 mysql", "error", err)
		os.Exit(1)
	}
	defer dc2MySQL.Close()

	// Kubernetes client
	k8sCfg, err := rest.InClusterConfig()
	if err != nil {
		logger.Error("failed to get k8s config", "error", err)
		os.Exit(1)
	}
	k8sClient, err := kubernetes.NewForConfig(k8sCfg)
	if err != nil {
		logger.Error("failed to create k8s client", "error", err)
		os.Exit(1)
	}

	// Register metrics
	metrics.Register(prometheus.DefaultRegisterer)

	tainter := platform.NewNodeTainter(k8sClient, logger)
	hub := platform.NewHub(logger)
	dns := platform.NewCloudflareDNS(cfg.CloudflareAPIToken, cfg.CloudflareZoneID, cfg.AZ)

	tm := controller.NewTopologyManager(cfg, dc1MySQL, dc2MySQL, tainter, hub, dns, logger)

	// HTTP server
	mux := http.NewServeMux()
	mux.HandleFunc("/healthz", func(rw http.ResponseWriter, r *http.Request) {
		rw.WriteHeader(http.StatusOK)
		rw.Write([]byte("ok"))
	})
	mux.HandleFunc("/readyz", func(rw http.ResponseWriter, r *http.Request) {
		if tm.Ready() {
			rw.WriteHeader(http.StatusOK)
			rw.Write([]byte("ok"))
		} else {
			rw.WriteHeader(http.StatusServiceUnavailable)
			rw.Write([]byte("not ready"))
		}
	})
	mux.HandleFunc("/status", func(rw http.ResponseWriter, r *http.Request) {
		rw.Header().Set("Content-Type", "application/json")
		json.NewEncoder(rw).Encode(tm.Status())
	})
	mux.HandleFunc("/ws/status", hub.HandleWS)
	mux.Handle("/metrics", promhttp.Handler())

	srv := &http.Server{
		Addr:    cfg.ListenAddr,
		Handler: mux,
	}

	errCh := make(chan error, 1)
	go func() {
		logger.Info("starting HTTP server", "addr", cfg.ListenAddr)
		if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			errCh <- err
		}
	}()

	// Start topology manager
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	go tm.Run(ctx)

	logger.Info("bloodraven started",
		"az", cfg.AZ,
		"dc1", cfg.DC1.Name,
		"dc2", cfg.DC2.Name,
		"poll_interval", cfg.PollInterval,
	)

	// Wait for shutdown signal or HTTP server error
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGTERM, syscall.SIGINT)

	select {
	case <-sigCh:
		logger.Info("shutting down")
	case err := <-errCh:
		logger.Error("http server error", "error", err)
	}

	cancel()

	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer shutdownCancel()
	srv.Shutdown(shutdownCtx)
}
