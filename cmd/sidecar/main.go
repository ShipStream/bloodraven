package main

import (
	"context"
	"log/slog"
	"os"
	"os/signal"
	"sync"
	"syscall"

	"github.com/shipstream/bloodraven/internal/sidecar"
)

func main() {
	logger := slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelInfo}))

	cfg, err := sidecar.ConfigFromEnv()
	if err != nil {
		logger.Error("failed to load config", "error", err)
		os.Exit(1)
	}

	logger = logger.With("pod", cfg.PodName)
	logger.Info("sidecar starting",
		"listen_addr", cfg.ListenAddr,
		"peer_address", cfg.PeerAddress,
		"bloodraven_address", cfg.BloodravenAddress,
		"lease_timeout", cfg.LeaseTimeout,
		"peer_check_interval", cfg.PeerCheckInterval,
		"my_site", cfg.MySite,
		"namespace", cfg.PodNamespace,
		"failover_group", cfg.FailoverGroup,
	)

	mysql, err := sidecar.NewLiveMysqlFromDSN(cfg.MysqlDSN)
	if err != nil {
		logger.Error("failed to connect to mysql", "error", err)
		os.Exit(1)
	}
	defer mysql.Close()

	// Set up signal handling for graceful shutdown
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGTERM, syscall.SIGINT)
	go func() {
		sig := <-sigCh
		logger.Info("received signal, shutting down", "signal", sig)
		cancel()
	}()

	// Create the HTTP server
	srv := sidecar.NewServer(mysql, cfg.ListenAddr, logger)

	// Run startup safety net
	srv.RunSafetyNet(ctx, cfg)

	// Create the fencing monitor
	fm := sidecar.NewFencingMonitor(
		mysql,
		cfg.BloodravenAddress,
		cfg.PeerAddress,
		cfg.PeerCheckInterval,
		cfg.LeaseTimeout,
		logger,
	)

	// Run both the HTTP server and fencing monitor concurrently
	var wg sync.WaitGroup

	wg.Add(1)
	go func() {
		defer wg.Done()
		if err := srv.Run(ctx); err != nil {
			logger.Error("HTTP server error", "error", err)
			cancel()
		}
	}()

	wg.Add(1)
	go func() {
		defer wg.Done()
		fm.Run(ctx)
	}()

	wg.Wait()
	logger.Info("sidecar stopped")
}
