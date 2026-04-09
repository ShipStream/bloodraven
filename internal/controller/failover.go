package controller

import (
	"context"
	"log/slog"
	"time"

	"github.com/shipstream/bloodraven/internal/mysql"
)

// FailoverController orchestrates MySQL failover between sites.
type FailoverController struct {
	logger *slog.Logger
}

// NewFailoverController creates a new FailoverController.
func NewFailoverController(logger *slog.Logger) *FailoverController {
	return &FailoverController{logger: logger}
}

// Execute performs a failover: fences old primary, drains relay logs on candidate, promotes candidate.
func (f *FailoverController) Execute(ctx context.Context, candidate mysql.Checker, oldPrimary mysql.Checker, candidateSite string) error {
	// 1. Fence old primary: SET GLOBAL super_read_only=ON (ignore error if unreachable).
	if oldPrimary != nil {
		if err := oldPrimary.SetSuperReadOnly(ctx, true); err != nil {
			f.logger.Warn("failed to fence old primary (may be unreachable)", "error", err)
		} else {
			f.logger.Info("fenced old primary with super_read_only=ON")
		}
	}

	// 2. On candidate: WaitForRelayLogDrain (30s timeout).
	if err := candidate.WaitForRelayLogDrain(ctx, 30*time.Second); err != nil {
		f.logger.Warn("relay log drain did not complete cleanly, proceeding with promotion", "error", err)
	} else {
		f.logger.Info("relay log drain complete")
	}

	// 3. STOP REPLICA.
	if err := candidate.StopReplica(ctx); err != nil {
		return err
	}

	// 4. RESET REPLICA ALL.
	if err := candidate.ResetReplicaAll(ctx); err != nil {
		return err
	}

	// 5. Clear super_read_only (may have been set by sidecar fencing or previous state).
	if err := candidate.SetSuperReadOnly(ctx, false); err != nil {
		return err
	}

	// 6. SET GLOBAL read_only = 0.
	if err := candidate.SetReadOnly(ctx, false); err != nil {
		return err
	}

	f.logger.Info("failover complete", "promotedSite", candidateSite)
	return nil
}
