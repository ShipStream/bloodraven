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

// UpdateController manages ordered rolling updates for a MysqlFailoverGroup.
// It ensures updates are applied replica-first, then failover, then old primary,
// to avoid simultaneous downtime of both DCs.
type UpdateController struct {
	failover *FailoverController
	logger   *slog.Logger

	// tickInterval controls how often waitForReplicaReady / waitForSiteReady poll.
	// Exposed for tests; production code uses 5s.
	tickInterval time.Duration

	// failFastDuration is how long waitForReplicaReady tolerates a writable standby
	// with no replication source before aborting. Derived at call time from
	// tickInterval so the threshold scales with the polling cadence.
	failFastDuration time.Duration

	mu       sync.Mutex
	phase    UpdatePhase
	updating bool
}

// NewUpdateController creates a new UpdateController.
func NewUpdateController(failover *FailoverController, logger *slog.Logger) *UpdateController {
	return &UpdateController{
		failover:         failover,
		logger:           logger,
		tickInterval:     5 * time.Second,
		failFastDuration: 30 * time.Second,
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
// activeSiteName and standbySiteName identify which site is currently active/standby.
// standbyChecker and activeChecker are the MySQL connections for each site.
// applyUpdate is a callback that applies the spec change to a specific site's deployment.
func (u *UpdateController) Execute(ctx context.Context,
	activeSiteName, standbySiteName string,
	standbyChecker, activeChecker mysql.Checker,
	applyUpdate func(ctx context.Context, siteName string) error) error {

	// Precondition: refuse to start if the standby is not actually replicating.
	// Runs before u.updating=true so an aborted attempt leaves no lock state behind.
	// A probe error is tolerated — the standby may be briefly unreachable; let the
	// rest of the flow discover that rather than adding a new transient-error path.
	// Each probe is bounded independently so a hung MySQL cannot stall the reconciler.
	probeCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	if ro, err := standbyChecker.CheckReadOnly(probeCtx); err == nil {
		if !ro {
			cancel()
			return fmt.Errorf("precondition: standby %s is writable; refusing to start ordered update", standbySiteName)
		}
		if rs, err := standbyChecker.ShowReplicaStatus(probeCtx); err == nil {
			if rs == nil || !rs.IORunning || !rs.SQLRunning || rs.SourceHost == "" {
				cancel()
				return fmt.Errorf("precondition: standby %s is not replicating", standbySiteName)
			}
		}
	}
	cancel()

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

	// Phase 1: Update standby site
	u.setPhase(UpdatePhaseUpdateReplica)
	u.logger.Info("ordered update: updating standby", "site", standbySiteName)
	if err := applyUpdate(ctx, standbySiteName); err != nil {
		return fmt.Errorf("update standby deployment: %w", err)
	}

	// Phase 2: Wait for standby to be ready and replication caught up
	u.setPhase(UpdatePhaseWaitReplica)
	u.logger.Info("ordered update: waiting for standby to be ready", "site", standbySiteName)
	if err := u.waitForReplicaReady(ctx, standbyChecker, 5*time.Minute); err != nil {
		return fmt.Errorf("wait for standby ready: %w", err)
	}

	// Phase 3: Failover to the (now-updated) standby
	u.setPhase(UpdatePhaseFailover)
	u.logger.Info("ordered update: failing over to updated standby", "site", standbySiteName)
	if _, err := u.failover.Execute(ctx, standbyChecker, activeChecker, standbySiteName); err != nil {
		return fmt.Errorf("failover during update: %w", err)
	}

	// Phase 4: Update the old active (now a standby)
	u.setPhase(UpdatePhaseUpdateOldPrimary)
	u.logger.Info("ordered update: updating old active", "site", activeSiteName)
	if err := applyUpdate(ctx, activeSiteName); err != nil {
		return fmt.Errorf("update old active deployment: %w", err)
	}

	// Phase 5: Wait for old active to be ready
	u.setPhase(UpdatePhaseWaitOldPrimary)
	u.logger.Info("ordered update: waiting for old active to be ready", "site", activeSiteName)
	if err := u.waitForSiteReady(ctx, activeChecker, 5*time.Minute); err != nil {
		u.logger.Warn("old active not ready after update, continuing", "site", activeSiteName, "error", err)
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
// It aborts early (~30s) when the standby is observed writable with no replication source,
// since that condition will never satisfy the happy path and holding updating=true blocks
// cross-site split-brain recovery.
func (u *UpdateController) waitForReplicaReady(ctx context.Context, checker mysql.Checker, timeout time.Duration) error {
	// Derive the fail-fast tick threshold from duration ÷ tickInterval so the
	// behaviour is consistent regardless of how tests override the tick cadence.
	failFastThreshold := 1
	if u.tickInterval > 0 {
		failFastThreshold = int((u.failFastDuration + u.tickInterval - 1) / u.tickInterval)
		if failFastThreshold < 1 {
			failFastThreshold = 1
		}
	}
	deadline := time.Now().Add(timeout)
	ticker := time.NewTicker(u.tickInterval)
	defer ticker.Stop()

	// writableObservations counts ticks where we confirmed the standby is
	// writable. It is deliberately not a strict "streak": probe errors leave
	// it alone rather than resetting, so a stale connection pool whose dial
	// errors alternate with successful "writable with no source" reads
	// cannot pin the counter below the fail-fast threshold and mask a
	// genuinely broken standby until the outer 5-minute deadline.
	writableObservations := 0

	for {
		// Check if MySQL is reachable
		ro, err := checker.CheckReadOnly(ctx)
		if err == nil {
			if !ro {
				// Writable standby counts regardless of whether
				// ShowReplicaStatus succeeds: cross-site recovery is
				// suppressed while the update runs, so nothing will start
				// the replica for us once the node is writable. The data
				// directory often retains master.info across a pod rollout,
				// leaving SourceHost populated with threads stopped — that
				// shape matters just as much as "no source at all" and
				// must also drive the fail-fast abort. Skipping the
				// replication probe here also avoids letting an
				// intermittent ShowReplicaStatus error swallow the !ro
				// observation and pin the counter below the threshold.
				writableObservations++
				if writableObservations >= failFastThreshold {
					return fmt.Errorf("standby is writable but replication is not running; aborting ordered update")
				}
			} else {
				// Read-only: check replication progress. A successful probe
				// that shows threads still starting up is real progress, so
				// reset the counter. A ShowReplicaStatus error with ro=true
				// is a probe blip — leave the counter alone and retry.
				rs, rsErr := checker.ShowReplicaStatus(ctx)
				if rsErr == nil {
					if rs != nil && rs.IORunning && rs.SQLRunning &&
						rs.SecondsBehindSource != nil && *rs.SecondsBehindSource < 5 {
						return nil
					}
					if rs != nil && rs.SourceHost != "" && (!rs.IORunning || !rs.SQLRunning) {
						// A standby pod restart preserves replication metadata but
						// --skip-replica-start intentionally leaves the IO/SQL
						// threads stopped. Ordered updates own this restart window,
						// so restart the existing channel before continuing to wait.
						if err := checker.StartReplica(ctx); err != nil {
							u.logger.Warn("ordered update: failed to start standby replica", "error", err)
						}
					}
					writableObservations = 0
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

// waitForSiteReady waits for a site to be reachable.
func (u *UpdateController) waitForSiteReady(ctx context.Context, checker mysql.Checker, timeout time.Duration) error {
	deadline := time.Now().Add(timeout)
	ticker := time.NewTicker(u.tickInterval)
	defer ticker.Stop()

	for {
		_, err := checker.CheckReadOnly(ctx)
		if err == nil {
			return nil
		}

		if time.Now().After(deadline) {
			return fmt.Errorf("timeout waiting for site to be ready")
		}

		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
		}
	}
}
