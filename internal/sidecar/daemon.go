package sidecar

import (
	"context"
	"fmt"
	"log/slog"
	"net"
	"os"
	"sync"
	"time"
)

// daemonMySQL is the local MySQL surface the sidecar's services share.
// *LiveMysql implements it.
type daemonMySQL interface {
	mysqlQuerier
	Fencer
	keyringQuerier
}

// daemonOptions are test seams for runDaemon. Zero values select the
// production behaviour.
type daemonOptions struct {
	listener       net.Listener
	newStore       ArchiveStoreFactory
	storeRetryWait func(ctx context.Context, d time.Duration) bool
	runArchiver    func(ctx context.Context, a *BinlogArchiver)
}

// RunDaemon wires and runs the sidecar's services until ctx is cancelled
// or the HTTP server fails. It returns an error, already logged, only for
// operator-rendered configuration it cannot act on, before any service
// starts.
//
// The HTTP server starts first and nothing optional can stop the rest
// from starting: readiness, the startup safety net and the fencing
// monitor run whether or not the PITR archive store can be built (see
// ArchiverSupervisor).
func RunDaemon(ctx context.Context, cfg *Config, mysql *LiveMysql, logger *slog.Logger) error {
	return runDaemon(ctx, cfg, mysql, logger, daemonOptions{})
}

func runDaemon(ctx context.Context, cfg *Config, mysql daemonMySQL, logger *slog.Logger, opts daemonOptions) error {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	// Optional binlog archiver, supervised so an archive store that
	// cannot be built leaves the sidecar serving. Built before the HTTP
	// server so the server can expose its Snapshot() via /archiver/status.
	var archiver *ArchiverSupervisor
	if cfg.PITR != nil {
		retention, err := retentionClientFromEnv(cfg)
		if err != nil {
			logger.Error("parse BLOODRAVEN_PITR_RETENTION_INTERVAL",
				"value", os.Getenv("BLOODRAVEN_PITR_RETENTION_INTERVAL"), "error", err)
			return fmt.Errorf("parse BLOODRAVEN_PITR_RETENTION_INTERVAL: %w", err)
		}
		archiver = NewArchiverSupervisor(cfg.PITR, cfg.MySite, mysql, opts.newStore, logger)
		if opts.storeRetryWait != nil {
			archiver.wait = opts.storeRetryWait
		}
		if opts.runArchiver != nil {
			archiver.runArchiver = opts.runArchiver
		}
		if retention != nil {
			archiver.SetConfigure(func(a *BinlogArchiver) {
				a.SetRetentionClient(retention.BloodravenAddress, retention.Namespace,
					retention.FailoverGroup, retention.ProfileName, retention.Interval)
			})
		}
	}

	// Shared topology cache: written by the fencing monitor (from
	// operator /active-site polls and peer-relay reads) and served
	// by the HTTP server for peers' /peer/active-site requests.
	topology := &TopologyCache{}

	// Optional encryption-at-rest keyring agent. Built before the HTTP
	// server so /keyring/status can serve its Snapshot().
	var keyringAgent *KeyringAgent
	if cfg.Keyring != nil {
		var err error
		keyringAgent, err = NewKeyringAgent(cfg.Keyring, cfg, mysql, logger)
		if err != nil {
			logger.Error("unable to configure keyring agent", "error", err)
			return err
		}
		keyringAgent.WithTopology(topology)
		logger.Info("keyring agent enabled",
			"path", cfg.Keyring.Path,
			"escrowArmed", cfg.Keyring.EscrowArmed,
			"rotate", cfg.Keyring.Rotate,
		)
	}

	srv := NewServer(mysql, cfg.ListenAddr, logger)
	srv.SetTopology(topology)
	if archiver != nil {
		srv.SetArchiver(archiver)
	}
	if keyringAgent != nil {
		srv.SetKeyring(keyringAgent)
	}

	// Create the fencing monitor. Construction touches nothing external;
	// it is attached to the server before the server starts because
	// /status and /livez read it.
	fm := NewFencingMonitor(
		mysql,
		cfg.BloodravenAddress,
		cfg.PeerAddresses,
		cfg.PeerCheckInterval,
		cfg.LeaseTimeout,
		logger,
	).WithTopology(cfg.MySite, cfg.PodNamespace, cfg.FailoverGroup, topology).
		WithSafetyNet(mysql)
	srv.SetFencing(fm)

	// Run the HTTP server, fencing monitor, and the optional archiver
	// and keyring agent concurrently. The HTTP server failing cancels
	// the shared context, bringing the others down cleanly.
	var wg sync.WaitGroup

	// The HTTP server starts before anything that talks to MySQL, so
	// the liveness probe and peers get answers while MySQL is down or
	// hung. Nothing it serves depends on the safety net having run.
	wg.Add(1)
	go func() {
		defer wg.Done()
		var err error
		if opts.listener != nil {
			err = srv.Serve(ctx, opts.listener)
		} else {
			err = srv.Run(ctx)
		}
		if err != nil {
			logger.Error("HTTP server error", "error", err)
			cancel()
		}
	}()

	// Run the startup safety net (it also seeds the topology cache). It
	// is bounded, so a hung mysqld cannot hold startup here. If it could
	// not settle, the monitor finishes it once MySQL answers.
	if !srv.RunSafetyNet(ctx, cfg) {
		fm.ArmSafetyNet()
	}

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

	if keyringAgent != nil {
		wg.Add(1)
		go func() {
			defer wg.Done()
			keyringAgent.Run(ctx)
		}()
	}

	wg.Wait()
	return nil
}

// retentionClientFromEnv returns the operator-driven retention wiring,
// or nil when the identity fields it needs are not all present. Missing
// any of them simply leaves retention disabled: binlogs still upload,
// just forever.
func retentionClientFromEnv(cfg *Config) (*retentionClientConfig, error) {
	profile := os.Getenv("BLOODRAVEN_PITR_PROFILE_NAME")
	if profile == "" || cfg.BloodravenAddress == "" || cfg.PodNamespace == "" || cfg.FailoverGroup == "" {
		return nil, nil
	}
	interval := time.Hour
	if v := os.Getenv("BLOODRAVEN_PITR_RETENTION_INTERVAL"); v != "" {
		// Silently defaulting an invalid value hides operator
		// misconfiguration; fail loud so a typo doesn't quietly leave
		// retention stuck at the default hourly cadence (AUDIT L7).
		d, err := time.ParseDuration(v)
		if err != nil {
			return nil, err
		}
		interval = d
	}
	return &retentionClientConfig{
		BloodravenAddress: cfg.BloodravenAddress,
		Namespace:         cfg.PodNamespace,
		FailoverGroup:     cfg.FailoverGroup,
		ProfileName:       profile,
		Interval:          interval,
	}, nil
}
