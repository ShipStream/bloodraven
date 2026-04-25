package main

import (
	"context"
	"log/slog"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

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

	// Optional binlog archiver. Built before the HTTP server so the
	// server can expose its Snapshot() via /archiver/status.
	var archiver *sidecar.BinlogArchiver
	if cfg.PITR != nil {
		store, err := sidecar.NewArchiveStore(ctx, cfg.PITR)
		if err != nil {
			logger.Error("failed to init archive store", "error", err)
			os.Exit(1)
		}
		archiver = sidecar.NewBinlogArchiver(cfg.PITR, cfg.MySite, mysql, store, logger)

		// Opt in to operator-driven retention when we have the
		// minimum identity fields (namespace, group, profile) + the
		// operator address. Missing any of them simply leaves
		// retention disabled: binlogs still upload, just forever.
		profile := os.Getenv("BLOODRAVEN_PITR_PROFILE_NAME")
		if profile != "" && cfg.BloodravenAddress != "" &&
			cfg.PodNamespace != "" && cfg.FailoverGroup != "" {
			retentionInterval := time.Hour
			if v := os.Getenv("BLOODRAVEN_PITR_RETENTION_INTERVAL"); v != "" {
				// Silently defaulting an invalid value hides
				// operator misconfiguration; fail loud so a typo
				// doesn't quietly leave retention stuck at the
				// default hourly cadence (AUDIT L7).
				d, err := time.ParseDuration(v)
				if err != nil {
					logger.Error("parse BLOODRAVEN_PITR_RETENTION_INTERVAL", "value", v, "error", err)
					os.Exit(1)
				}
				retentionInterval = d
			}
			archiver.SetRetentionClient(
				cfg.BloodravenAddress,
				cfg.PodNamespace,
				cfg.FailoverGroup,
				profile,
				retentionInterval,
			)
		}
	}

	// Shared topology cache: written by the fencing monitor (from
	// operator /active-site polls and peer-relay reads) and served
	// by the HTTP server for peers' /peer/active-site requests.
	topology := &sidecar.TopologyCache{}

	// Create the HTTP server
	srv := sidecar.NewServer(mysql, cfg.ListenAddr, logger)
	srv.SetTopology(topology)
	if archiver != nil {
		srv.SetArchiver(archiver)
	}

	// Run startup safety net (also seeds the topology cache).
	srv.RunSafetyNet(ctx, cfg)

	// Create the fencing monitor
	fm := sidecar.NewFencingMonitor(
		mysql,
		cfg.BloodravenAddress,
		cfg.PeerAddresses,
		cfg.PeerCheckInterval,
		cfg.LeaseTimeout,
		logger,
	).WithTopology(cfg.MySite, cfg.PodNamespace, cfg.FailoverGroup, topology)

	// Run the HTTP server, fencing monitor, and (optionally) the
	// archiver concurrently. Any of them failing cancels the shared
	// context, bringing the others down cleanly.
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

	if archiver != nil {
		wg.Add(1)
		go func() {
			defer wg.Done()
			archiver.Run(ctx)
		}()
	}

	wg.Wait()
	logger.Info("sidecar stopped")
}
