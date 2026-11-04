package main

import (
	"context"
	"log/slog"
	"os"
	"os/signal"
	"syscall"

	"github.com/shipstream/bloodraven/internal/sidecar"
	"github.com/shipstream/bloodraven/internal/util"
)

func main() {
	logger := util.NewJSONLogger(os.Stdout, slog.LevelInfo)

	cfg, err := sidecar.ConfigFromEnv()
	if err != nil {
		logger.Error("failed to load config", "error", err)
		os.Exit(1)
	}

	logger = logger.With("pod", cfg.PodName)
	logger.Info("sidecar starting",
		"listenAddr", cfg.ListenAddr,
		"peerAddresses", cfg.PeerAddresses,
		"bloodravenAddress", cfg.BloodravenAddress,
		"leaseTimeout", cfg.LeaseTimeout,
		"peerCheckInterval", cfg.PeerCheckInterval,
		"site", cfg.MySite,
		"namespace", cfg.PodNamespace,
		"fg", cfg.FailoverGroup,
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

	// RunDaemon logs its own startup errors. An archive store that
	// cannot be built is not one of them: the sidecar keeps serving
	// readiness and fencing and retries the store in the background.
	if err := sidecar.RunDaemon(ctx, cfg, mysql, logger); err != nil {
		os.Exit(1)
	}
	logger.Info("sidecar stopped")
}
