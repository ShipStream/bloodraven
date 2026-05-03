package controller

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	"github.com/shipstream/bloodraven/internal/mysql"
)

// BootstrapController handles bootstrapping new MySQL replicas via the clone plugin.
type BootstrapController struct {
	logger *slog.Logger
}

func NewBootstrapController(logger *slog.Logger) *BootstrapController {
	return &BootstrapController{logger: logger}
}

// BootstrapReplica performs a full clone and replication setup for a new replica.
// It assumes the primary is healthy and writable.
//
// Steps:
//  1. Verify primary is writable
//  2. Set clone_valid_donor_list on replica to primary host
//  3. Execute CLONE INSTANCE FROM primary
//  4. After clone (MySQL auto-restarts, caller must reconnect), set up replication:
//     CHANGE REPLICATION SOURCE TO ... SOURCE_AUTO_POSITION=1
//  5. START REPLICA
func (b *BootstrapController) BootstrapReplica(ctx context.Context, opts BootstrapOpts) error {
	// Higher-level "starting bootstrap" event is emitted by the
	// topology manager so it carries source/donor/recipient context.
	// Step 1: Verify primary is writable
	readOnly, err := opts.Primary.CheckReadOnly(ctx)
	if err != nil {
		return fmt.Errorf("check primary read_only: %w", err)
	}
	if readOnly {
		return fmt.Errorf("primary is read-only, cannot bootstrap from it")
	}

	// Step 2: CLONE INSTANCE is a destructive administrative operation, but
	// MySQL rejects it while the recipient has super_read_only enabled.
	if err := opts.Replica.SetSuperReadOnly(ctx, false); err != nil {
		return fmt.Errorf("disable replica super_read_only for clone: %w", err)
	}
	if err := opts.Replica.SetReadOnly(ctx, false); err != nil {
		return fmt.Errorf("disable replica read_only for clone: %w", err)
	}

	// Step 3: Set clone donor list
	donorAddr := fmt.Sprintf("%s:3306", opts.PrimaryHost)
	if err := opts.Replica.SetCloneDonorList(ctx, donorAddr); err != nil {
		if fenceErr := opts.Replica.SetSuperReadOnly(ctx, true); fenceErr != nil {
			b.logger.Warn("failed to re-enable super_read_only after clone donor setup failed", "error", fenceErr)
		}
		return fmt.Errorf("set clone donor list: %w", err)
	}

	// Step 4: Kill other connections on the replica so CLONE's DROP DATA
	// phase can proceed without waiting on open table handles.
	if killed, err := opts.Replica.KillAppConnections(ctx); err != nil {
		b.logger.Warn("failed to kill connections before clone", "error", err)
	} else if killed > 0 {
		b.logger.Info("killed connections before clone", "count", killed)
	}

	// Step 5: Clone from primary (this may take a long time)
	b.logger.Info("cloning from primary", "donor", opts.PrimaryHost)
	cloneTimeout := opts.CloneTimeout
	if cloneTimeout <= 0 {
		cloneTimeout = time.Hour
	}
	cloneCtx, cancel := context.WithTimeout(ctx, cloneTimeout)
	defer cancel()
	cloneTimeoutSec := int(cloneTimeout.Seconds())
	if err := opts.Replica.CloneInstance(cloneCtx, opts.ReplUser, opts.PrimaryHost, opts.ReplPassword, opts.UseSSL, cloneTimeoutSec); err != nil {
		if !isCloneConnectionDrop(err) {
			if fenceErr := opts.Replica.SetSuperReadOnly(ctx, true); fenceErr != nil {
				b.logger.Warn("failed to re-enable super_read_only after clone failed", "error", fenceErr)
			}
		}
		return fmt.Errorf("clone instance: %w", err)
	}

	// Note: After clone, MySQL auto-restarts. The caller is responsible for:
	// - Detecting the restart (sidecar goes unreachable, then comes back)
	// - Reconnecting the mysql.Checker
	// - Calling SetupReplication() after reconnection

	b.logger.Info("clone completed successfully", "replica", opts.ReplicaSite)
	return nil
}

// SetupReplication configures and starts replication after a clone or recovery.
func (b *BootstrapController) SetupReplication(ctx context.Context, replica mysql.Checker, opts ReplicationSetupOpts) error {
	b.logger.Info("setting up replication", "source", opts.SourceHost)

	// Ensure replica is read-only
	if err := replica.SetSuperReadOnly(ctx, true); err != nil {
		return fmt.Errorf("set super_read_only: %w", err)
	}

	if err := replica.StopReplica(ctx); err != nil {
		b.logger.Info("stop replica before replication setup skipped", "error", err)
	}
	if err := replica.ResetReplicaAll(ctx); err != nil {
		b.logger.Info("reset replica metadata before replication setup skipped", "error", err)
	}

	// Configure replication source
	if err := replica.ChangeReplicationSource(ctx, mysql.ReplicationSourceOpts{
		Host:     opts.SourceHost,
		User:     opts.ReplUser,
		Password: opts.ReplPassword,
		UseSSL:   opts.UseSSL,
	}); err != nil {
		return fmt.Errorf("change replication source: %w", err)
	}

	// Start replication
	if err := replica.StartReplica(ctx); err != nil {
		return fmt.Errorf("start replica: %w", err)
	}

	b.logger.Info("replication started successfully", "source", opts.SourceHost)
	return nil
}

// BootstrapOpts holds configuration for bootstrapping a replica.
type BootstrapOpts struct {
	Primary      mysql.Checker
	Replica      mysql.Checker
	PrimaryHost  string // hostname of the primary MySQL
	ReplicaSite  string // name of the replica site (for logging)
	ReplUser     string // replication user
	ReplPassword string // replication password
	UseSSL       bool
	CloneTimeout time.Duration // timeout for clone operation (default 30m)
}

// ReplicationSetupOpts holds configuration for setting up replication.
type ReplicationSetupOpts struct {
	SourceHost   string
	ReplUser     string
	ReplPassword string
	UseSSL       bool
}
