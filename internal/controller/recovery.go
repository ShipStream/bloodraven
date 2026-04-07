package controller

import (
	"context"

	"github.com/shipstream/bloodraven/internal/mysql"
)

// RecoverOldPrimary reconfigures an old primary as a replica of the new primary.
func (f *FailoverController) RecoverOldPrimary(ctx context.Context, oldPrimary mysql.Checker, newPrimaryHost string, replUser string, replPassword string, useSSL bool) error {
	// 1. SET GLOBAL super_read_only = ON (defensive).
	if err := oldPrimary.SetSuperReadOnly(ctx, true); err != nil {
		return err
	}

	// 2. STOP REPLICA (in case it was partially configured).
	if err := oldPrimary.StopReplica(ctx); err != nil {
		return err
	}

	// 3. CHANGE REPLICATION SOURCE TO.
	if err := oldPrimary.ChangeReplicationSource(ctx, mysql.ReplicationSourceOpts{
		Host:     newPrimaryHost,
		User:     replUser,
		Password: replPassword,
		UseSSL:   useSSL,
	}); err != nil {
		return err
	}

	// 4. START REPLICA.
	if err := oldPrimary.StartReplica(ctx); err != nil {
		return err
	}

	f.logger.Info("old primary recovery complete", "newSource", newPrimaryHost)
	return nil
}
