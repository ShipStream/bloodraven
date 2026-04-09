package controller

import (
	"context"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"github.com/shipstream/bloodraven/internal/mysql"
)

// UpdatePhase tracks the current phase of an ordered update.
type UpdatePhase string

const (
	UpdatePhaseNone             UpdatePhase = ""
	UpdatePhaseUpdateReplica    UpdatePhase = "UpdateReplica"
	UpdatePhaseWaitReplica      UpdatePhase = "WaitReplica"
	UpdatePhaseFailover         UpdatePhase = "Failover"
	UpdatePhaseUpdateOldPrimary UpdatePhase = "UpdateOldPrimary"
	UpdatePhaseWaitOldPrimary   UpdatePhase = "WaitOldPrimary"
	UpdatePhaseComplete         UpdatePhase = "Complete"
)

// UpdateController manages ordered rolling updates for a MysqlReplicaPair.
// It ensures updates are applied replica-first, then failover, then old primary,
// to avoid simultaneous downtime of both DCs.
type UpdateController struct {
	failover *FailoverController
	logger   *slog.Logger

	mu       sync.Mutex
	phase    UpdatePhase
	updating bool
}

// NewUpdateController creates a new UpdateController.
func NewUpdateController(failover *FailoverController, logger *slog.Logger) *UpdateController {
	return &UpdateController{
		failover: failover,
		logger:   logger,
	}
}

// IsUpdating returns true if an ordered update is in progress.
func (u *UpdateController) IsUpdating() bool {
	u.mu.Lock()
	defer u.mu.Unlock()
	return u.updating
}

// Phase returns the current update phase.
func (u *UpdateController) Phase() UpdatePhase {
	u.mu.Lock()
	defer u.mu.Unlock()
	return u.phase
}

// Execute performs an ordered update. It should be called when a spec drift is detected.
// primaryDCName and replicaDCName identify which DC is currently primary/replica.
// replicaChecker and primaryChecker are the MySQL connections for each DC.
// applyUpdate is a callback that applies the spec change to a specific DC's deployment.
func (u *UpdateController) Execute(ctx context.Context,
	primaryDCName, replicaDCName string,
	replicaChecker, primaryChecker mysql.Checker,
	applyUpdate func(ctx context.Context, dcName string) error) error {

	u.mu.Lock()
	if u.updating {
		u.mu.Unlock()
		return fmt.Errorf("update already in progress")
	}
	u.updating = true
	u.mu.Unlock()

	defer func() {
		u.mu.Lock()
		u.updating = false
		u.phase = UpdatePhaseNone
		u.mu.Unlock()
	}()

	// Phase 1: Update replica DC
	u.setPhase(UpdatePhaseUpdateReplica)
	u.logger.Info("ordered update: updating replica", "dc", replicaDCName)
	if err := applyUpdate(ctx, replicaDCName); err != nil {
		return fmt.Errorf("update replica deployment: %w", err)
	}

	// Phase 2: Wait for replica to be ready and replication caught up
	u.setPhase(UpdatePhaseWaitReplica)
	u.logger.Info("ordered update: waiting for replica to be ready", "dc", replicaDCName)
	if err := u.waitForReplicaReady(ctx, replicaChecker, 5*time.Minute); err != nil {
		return fmt.Errorf("wait for replica ready: %w", err)
	}

	// Phase 3: Failover to the (now-updated) replica
	u.setPhase(UpdatePhaseFailover)
	u.logger.Info("ordered update: failing over to updated replica", "dc", replicaDCName)
	if err := u.failover.Execute(ctx, replicaChecker, primaryChecker, replicaDCName); err != nil {
		return fmt.Errorf("failover during update: %w", err)
	}

	// Phase 4: Update the old primary (now a replica)
	u.setPhase(UpdatePhaseUpdateOldPrimary)
	u.logger.Info("ordered update: updating old primary", "dc", primaryDCName)
	if err := applyUpdate(ctx, primaryDCName); err != nil {
		return fmt.Errorf("update old primary deployment: %w", err)
	}

	// Phase 5: Wait for old primary to be ready
	u.setPhase(UpdatePhaseWaitOldPrimary)
	u.logger.Info("ordered update: waiting for old primary to be ready", "dc", primaryDCName)
	if err := u.waitForDCReady(ctx, primaryChecker, 5*time.Minute); err != nil {
		u.logger.Warn("old primary not ready after update, continuing", "dc", primaryDCName, "error", err)
	}

	u.setPhase(UpdatePhaseComplete)
	u.logger.Info("ordered update complete")
	return nil
}

func (u *UpdateController) setPhase(p UpdatePhase) {
	u.mu.Lock()
	u.phase = p
	u.mu.Unlock()
}

// waitForReplicaReady waits for the replica to have replication running and caught up.
func (u *UpdateController) waitForReplicaReady(ctx context.Context, checker mysql.Checker, timeout time.Duration) error {
	deadline := time.Now().Add(timeout)
	ticker := time.NewTicker(5 * time.Second)
	defer ticker.Stop()

	for {
		// Check if MySQL is reachable
		_, err := checker.CheckReadOnly(ctx)
		if err == nil {
			// Check replication status
			rs, err := checker.ShowReplicaStatus(ctx)
			if err == nil && rs != nil && rs.IORunning && rs.SQLRunning {
				if rs.SecondsBehindSource != nil && *rs.SecondsBehindSource < 5 {
					return nil
				}
			}
		}

		if time.Now().After(deadline) {
			return fmt.Errorf("timeout waiting for replica to be ready")
		}

		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
		}
	}
}

// waitForDCReady waits for a DC to be reachable.
func (u *UpdateController) waitForDCReady(ctx context.Context, checker mysql.Checker, timeout time.Duration) error {
	deadline := time.Now().Add(timeout)
	ticker := time.NewTicker(5 * time.Second)
	defer ticker.Stop()

	for {
		_, err := checker.CheckReadOnly(ctx)
		if err == nil {
			return nil
		}

		if time.Now().After(deadline) {
			return fmt.Errorf("timeout waiting for DC to be ready")
		}

		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
		}
	}
}
