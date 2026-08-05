package controller

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/shipstream/bloodraven/internal/metrics"
	"github.com/shipstream/bloodraven/internal/mysql"
	"github.com/shipstream/bloodraven/internal/state"
)

const sourceConvergenceOperationTimeout = 20 * time.Second

func canonicalSourceHost(host string) string {
	host = strings.ToLower(strings.TrimSpace(host))
	host = strings.TrimSuffix(host, ":3306")
	return strings.TrimSuffix(host, ".")
}

// confirmedActivePrimary returns the unique observed writable promotable site
// after directly confirming it still accepts writes.
func (tm *TopologyManager) confirmedActivePrimary(ctx context.Context) (*siteTracker, error) {
	tm.mu.RLock()
	name := tm.activeSiteLocked()
	tm.mu.RUnlock()
	if name == "" {
		return nil, fmt.Errorf("no unique writable primary-candidate")
	}
	site := tm.getSite(name)
	if site == nil || !site.isPromotable() || site.host == "" {
		return nil, fmt.Errorf("active site %q is not a valid donor", name)
	}
	if err := tm.confirmWritable(ctx, site); err != nil {
		return nil, err
	}
	return site, nil
}

func (tm *TopologyManager) sourceConvergenceSuppressed() bool {
	tm.mu.RLock()
	defer tm.mu.RUnlock()
	return tm.promotedSite != "" || tm.autoBootstrapSuppressed || tm.topologyFrozen ||
		tm.plannedFailoverActive || !bootstrapIdlePhase(tm.bootstrapPhase) ||
		(tm.updater != nil && tm.updater.IsUpdating())
}

// checkSourceConvergence converges followers with existing replication
// metadata directly onto the confirmed active primary. Source-less followers
// remain the responsibility of bootstrap/old-primary recovery.
func (tm *TopologyManager) checkSourceConvergence(ctx context.Context, siteRepl []*mysql.ReplicaStatus) (map[string]struct{}, bool) {
	handled := make(map[string]struct{})
	active, err := tm.confirmedActivePrimary(ctx)
	if err != nil {
		return handled, false
	}
	suppressed := tm.sourceConvergenceSuppressed()
	changed := false

	for i := range tm.sites {
		site := &tm.sites[i]
		if site.name == active.name || site.state != state.StateReadOnly {
			continue
		}
		var repl *mysql.ReplicaStatus
		if i < len(siteRepl) {
			repl = siteRepl[i]
		}
		if repl == nil || strings.TrimSpace(repl.SourceHost) == "" {
			continue
		}
		handled[site.name] = struct{}{}
		current := canonicalSourceHost(repl.SourceHost)
		expected := canonicalSourceHost(active.host)
		if current == expected && repl.IORunning && repl.SQLRunning {
			changed = tm.setSourceConvergence(site, repl, sourceConvergenceConverged, sourceReasonDirectSource, active.host) || changed
			continue
		}

		if suppressed {
			changed = tm.setSourceConvergence(site, repl, sourceConvergencePending, sourceReasonSourceMismatch, active.host) || changed
			continue
		}

		opCtx, cancel := context.WithTimeout(ctx, sourceConvergenceOperationTimeout)
		if current == expected {
			err = tm.restartDirectReplica(opCtx, active, site)
		} else {
			err = tm.repointReplica(opCtx, active, site, repl.SourceHost)
		}
		if err != nil {
			cancel()
			stateValue, reason := sourceConvergencePending, sourceReasonMutationFailed
			if strings.Contains(err.Error(), sourceReasonGTIDDiverged) {
				stateValue, reason = sourceConvergenceBlocked, sourceReasonGTIDDiverged
			} else if strings.Contains(err.Error(), sourceReasonProbeFailed) {
				reason = sourceReasonProbeFailed
			}
			changed = tm.setSourceConvergence(site, repl, stateValue, reason, active.host) || changed
			continue
		}

		verified, verifyErr := tm.verifyDirectReplica(opCtx, site, active.host)
		cancel()
		if verifyErr != nil {
			tm.logSourceFailure(site, active, "verify", verifyErr)
			changed = tm.setSourceConvergence(site, repl, sourceConvergencePending, sourceReasonMutationFailed, active.host) || changed
			continue
		}
		if i < len(siteRepl) {
			siteRepl[i] = verified
		}
		tm.logger.Info("replication source convergence complete", "site", site.name, "source", active.host, "fg", tm.cfg.Name)
		changed = tm.setSourceConvergence(site, verified, sourceConvergenceConverged, sourceReasonDirectSource, active.host) || changed
	}
	return handled, changed
}

func (tm *TopologyManager) restartDirectReplica(ctx context.Context, active, follower *siteTracker) error {
	if err := tm.ensureGTIDContained(ctx, active, follower, "pre-start-gtid"); err != nil {
		return err
	}
	tm.logger.Info("replication source convergence started",
		"site", follower.name, "activeSite", active.name, "currentSource", active.host,
		"expectedSource", active.host, "fg", tm.cfg.Name)
	if err := follower.mysql.StartReplica(ctx); err != nil {
		tm.logSourceFailure(follower, active, "start", err)
		return err
	}
	return nil
}

func (tm *TopologyManager) repointReplica(ctx context.Context, active, follower *siteTracker, currentSource string) error {
	if err := tm.ensureGTIDContained(ctx, active, follower, "pre-stop-gtid"); err != nil {
		return err
	}
	tm.logger.Info("replication source convergence started",
		"site", follower.name, "activeSite", active.name, "currentSource", currentSource,
		"expectedSource", active.host, "fg", tm.cfg.Name)
	// Rollback restarts must not reuse the operation context: if the probe or
	// source change below consumed its deadline, StartReplica(ctx) would fail
	// immediately and leave the unchanged channel stopped. Derive a fresh
	// bounded rollback context before stopping replication.
	rollbackCtx, rollbackCancel := context.WithTimeout(context.WithoutCancel(ctx), sourceConvergenceOperationTimeout)
	defer rollbackCancel()
	if err := follower.mysql.StopReplica(ctx); err != nil {
		tm.logSourceFailure(follower, active, "stop", err)
		return err
	}
	if err := tm.ensureGTIDContained(ctx, active, follower, "post-stop-gtid"); err != nil {
		// The unchanged channel is safe to resume when the failure was a probe;
		// divergence deliberately remains stopped for operator review.
		if !strings.Contains(err.Error(), sourceReasonGTIDDiverged) {
			_ = follower.mysql.StartReplica(rollbackCtx)
		}
		return err
	}
	if err := follower.mysql.ChangeReplicationSource(ctx, mysql.ReplicationSourceOpts{
		Host: active.host, User: tm.bootstrapCfg.ReplUser, Password: tm.bootstrapCfg.ReplPassword, UseSSL: tm.bootstrapCfg.UseSSL,
	}); err != nil {
		tm.logSourceFailure(follower, active, "change-source", err)
		_ = follower.mysql.StartReplica(rollbackCtx)
		return err
	}
	if err := follower.mysql.StartReplica(ctx); err != nil {
		tm.logSourceFailure(follower, active, "start", err)
		return err
	}
	return nil
}

func (tm *TopologyManager) ensureGTIDContained(ctx context.Context, active, follower *siteTracker, stage string) error {
	activeRaw, err := active.mysql.GetGtidExecuted(ctx)
	if err != nil {
		tm.logSourceFailure(follower, active, stage, err)
		return fmt.Errorf("%s: %w", sourceReasonProbeFailed, err)
	}
	followerRaw, err := follower.mysql.GetGtidExecuted(ctx)
	if err != nil {
		tm.logSourceFailure(follower, active, stage, err)
		return fmt.Errorf("%s: %w", sourceReasonProbeFailed, err)
	}
	activeGTID, activeErr := mysql.ParseGTIDSet(activeRaw)
	followerGTID, followerErr := mysql.ParseGTIDSet(followerRaw)
	if activeErr != nil || followerErr != nil {
		parseErr := activeErr
		if parseErr == nil {
			parseErr = followerErr
		}
		tm.logSourceFailure(follower, active, stage, parseErr)
		return fmt.Errorf("%s: %w", sourceReasonProbeFailed, parseErr)
	}
	if !activeGTID.Contains(followerGTID) {
		tm.logger.Warn("replication source convergence blocked",
			"site", follower.name, "activeSite", active.name, "stage", stage,
			"followerGtid", followerRaw, "activeGtid", activeRaw, "fg", tm.cfg.Name)
		return fmt.Errorf("%s: active does not contain follower GTID", sourceReasonGTIDDiverged)
	}
	return nil
}

func (tm *TopologyManager) verifyDirectReplica(ctx context.Context, follower *siteTracker, expectedHost string) (*mysql.ReplicaStatus, error) {
	var lastErr error
	for attempt := 0; attempt < 3; attempt++ {
		rs, err := follower.mysql.ShowReplicaStatus(ctx)
		if err == nil && rs != nil && rs.IORunning && rs.SQLRunning && canonicalSourceHost(rs.SourceHost) == canonicalSourceHost(expectedHost) {
			return rs, nil
		}
		if err != nil {
			lastErr = err
		} else {
			lastErr = fmt.Errorf("replica source or threads not converged")
		}
		if attempt < 2 {
			tm.sleep(100 * time.Millisecond)
		}
	}
	return nil, lastErr
}

func (tm *TopologyManager) logSourceFailure(follower, active *siteTracker, stage string, err error) {
	tm.logger.Error("replication source convergence failed",
		"site", follower.name, "activeSite", active.name, "stage", stage, "error", err, "fg", tm.cfg.Name)
}

func (tm *TopologyManager) setSourceConvergence(site *siteTracker, repl *mysql.ReplicaStatus, convergence SourceConvergenceState, reason, expectedHost string) bool {
	host := ""
	if repl != nil {
		host = repl.SourceHost
	}
	serving := site.role == state.SiteRoleReadOnly && site.state == state.StateReadOnly && site.replicating &&
		convergence == sourceConvergenceConverged && canonicalSourceHost(host) == canonicalSourceHost(expectedHost) &&
		repl != nil && repl.SecondsBehindSource != nil && *repl.SecondsBehindSource <= tm.cfg.ReadOnlyMaxLagSeconds
	tm.mu.Lock()
	defer tm.mu.Unlock()
	changed := site.sourceHost != host || site.sourceConvergenceState != convergence ||
		site.sourceConvergenceReason != reason || site.servingHealthy != serving
	site.sourceHost = host
	site.sourceConvergenceState = convergence
	site.sourceConvergenceReason = reason
	site.servingHealthy = serving
	return changed
}

func (tm *TopologyManager) markSourceProbeFailed(site *siteTracker) bool {
	tm.mu.Lock()
	defer tm.mu.Unlock()
	changed := site.sourceConvergenceState != sourceConvergencePending ||
		site.sourceConvergenceReason != sourceReasonProbeFailed || site.servingHealthy
	site.sourceConvergenceState = sourceConvergencePending
	site.sourceConvergenceReason = sourceReasonProbeFailed
	site.servingHealthy = false
	return changed
}

func (tm *TopologyManager) emitSourceConvergenceMetrics() {
	tm.mu.RLock()
	defer tm.mu.RUnlock()
	active := tm.activeSiteLocked()
	for i := range tm.sites {
		site := &tm.sites[i]
		if site.name == active {
			for _, value := range metrics.AllSourceStates {
				metrics.ReplicationSourceState.DeleteLabelValues(tm.cfg.Namespace, tm.cfg.Name, site.name, value)
			}
			continue
		}
		current := strings.ToLower(string(site.sourceConvergenceState))
		for _, value := range metrics.AllSourceStates {
			v := 0.0
			if current == value {
				v = 1
			}
			metrics.ReplicationSourceState.WithLabelValues(tm.cfg.Namespace, tm.cfg.Name, site.name, value).Set(v)
		}
	}
}
