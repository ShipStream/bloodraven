package controller

import (
	"context"
	"fmt"
	"log/slog"
	"strings"
	"sync"
	"time"

	"github.com/shipstream/bloodraven/internal/clock"
	"github.com/shipstream/bloodraven/internal/metrics"
	"github.com/shipstream/bloodraven/internal/mysql"
	"github.com/shipstream/bloodraven/internal/platform"
	"github.com/shipstream/bloodraven/internal/state"
	"github.com/shipstream/bloodraven/internal/util"
)

// SiteTopologyConfig holds per-site configuration for the topology manager.
type SiteTopologyConfig struct {
	Name string
	Zone string
	LBIP string
}

// TopologyConfig holds topology manager configuration, decoupled from config source.
type TopologyConfig struct {
	Namespace string // failover group namespace (from CR metadata.namespace)
	Name      string // failover group name (from CR metadata.name)
	Sites     [2]SiteTopologyConfig

	// SiteHosts are the MySQL service hostnames (without port) for each site.
	// Used as the donor/source host for clone and replication setup.
	SiteHosts [2]string

	PollInterval      int64 // nanoseconds
	FailureThreshold  int
	RecoveryThreshold int
	FailoverCooldown  int64 // nanoseconds, default 5m
}

// BootstrapConfig holds configuration for auto-bootstrap of fresh-deploy replicas.
// When ReplUser is empty, auto-bootstrap is disabled.
type BootstrapConfig struct {
	ReplUser     string
	ReplPassword string
	UseSSL       bool
	CloneTimeout time.Duration
}

// BootstrapPhase tracks the lifecycle of an auto-bootstrap operation.
type BootstrapPhase string

const (
	BootstrapPhaseNone       BootstrapPhase = ""
	BootstrapPhaseCloning    BootstrapPhase = "Cloning"
	BootstrapPhaseRestarting BootstrapPhase = "WaitingForRestart"
	BootstrapPhaseSetupRepl  BootstrapPhase = "SetupReplication"
	BootstrapPhaseDone       BootstrapPhase = "Done"
	BootstrapPhaseFailed     BootstrapPhase = "Failed"
)

// PollIntervalDuration returns the poll interval as a time.Duration.
func (c TopologyConfig) PollIntervalDuration() time.Duration {
	return time.Duration(c.PollInterval)
}

// TopologySnapshot captures the topology state at a point in time.
// It is passed to the StatusCallback after each poll cycle that produces a state change.
type TopologySnapshot struct {
	SiteNames       [2]string
	SiteStates      [2]state.SiteState
	SiteLastSeen    [2]time.Time
	SiteReplication [2]*mysql.ReplicaStatus // nil if site is primary or unreachable
	ActiveSite      string                  // name of the writable site, empty if none
	LastFailover       time.Time
	LastFailoverTarget string
	Alert              string // non-empty if a cross-site alert fired this cycle
	UpdatePhase        string // non-empty if an ordered update is in progress
	BootstrapPhase     string // non-empty if a fresh-deploy bootstrap is in progress or finished
	BootstrapError     string // non-empty if bootstrap failed

	PromotionGtidExecuted string // GTID set at the moment of the most recent promotion
	RecoverySite          string // site name when recovery is blocked due to divergence
	RecoveryState         string // "" or "RecoveryBlocked"
	DivergentGtid         string
	DivergentTxnCount     int64
}

// siteTracker tracks debounce counters and current state for one site.
type siteTracker struct {
	name          string
	zone          string // topology.kubernetes.io/zone value
	lbIP          string
	mysql         mysql.Checker
	state         state.SiteState
	failCount     int
	recoveryCount int
	lastSeen      time.Time // last successful poll
}

// TopologyManager is the main control loop.
type TopologyManager struct {
	cfg   TopologyConfig
	sites [2]siteTracker
	tainter platform.NodeTainter
	hub     *platform.Hub
	dns     platform.DNSUpdater
	logger  *slog.Logger
	clock   clock.Clock

	// Failover orchestration.
	failover           *FailoverController
	lastFailover       time.Time
	lastFailoverTarget string
	failoverCooldown   time.Duration

	// Promotion state: tracks which site was promoted and is pending DNS flip.
	promotedSite          string // empty = no pending promotion
	promotionGtidExecuted string // GTID set at last promotion

	// Recovery state for old primary after failover.
	recoveryPendingSite    string // site name with blocked recovery ("" = none)
	recoveryDivergentGtid  string
	recoveryDivergentCount int64

	// Bootstrap orchestration. When bootstrap is nil or bootstrapCfg.ReplUser is
	// empty, auto-bootstrap is disabled. bootstrapPhase/bootstrapErr are protected by mu.
	bootstrap      *BootstrapController
	bootstrapCfg   BootstrapConfig
	bootstrapPhase BootstrapPhase
	bootstrapErr   error

	// autoBootstrapSuppressed is set by the runner while a one-shot
	// initFromBackup restore is in flight. It prevents the fresh-deploy
	// auto-clone path from racing the restore Job to populate the
	// primary. Protected by mu. See runner.sync().
	autoBootstrapSuppressed bool

	// StatusCallback is invoked after each poll cycle that produces a state change.
	// The runner sets this to push status updates to the CR.
	StatusCallback func(TopologySnapshot)

	// BootstrapStatusCallback is invoked from the bootstrap goroutine when the
	// bootstrap phase changes. Unlike StatusCallback it pushes only the
	// Bootstrapping condition so that unrelated conditions (Degraded,
	// ReplicationBroken, Updating, ...) are not inadvertently cleared by a
	// partially-populated TopologySnapshot during an async bootstrap run.
	BootstrapStatusCallback func(phase, errMsg string)

	mu           sync.RWMutex
	lastPollTime time.Time
	ready        bool
	cancelFunc   context.CancelFunc
}

// SiteStatusEntry is a single site's status in the StatusResponse.
type SiteStatusEntry struct {
	Name  string `json:"name"`
	State string `json:"state"`
}

// StatusResponse is returned by the /status endpoint.
type StatusResponse struct {
	ActiveSite string             `json:"active_site"`
	Sites      [2]SiteStatusEntry `json:"sites"`
	PollTime   string             `json:"poll_time"`
}

func NewTopologyManager(cfg TopologyConfig, site0MySQL, site1MySQL mysql.Checker, failover *FailoverController, bootstrap *BootstrapController, bootstrapCfg BootstrapConfig, tainter platform.NodeTainter, hub *platform.Hub, dns platform.DNSUpdater, logger *slog.Logger) *TopologyManager {
	return NewTopologyManagerWithClock(cfg, site0MySQL, site1MySQL, failover, bootstrap, bootstrapCfg, tainter, hub, dns, logger, clock.RealClock{})
}

// NewTopologyManagerWithClock creates a TopologyManager with an injectable clock for testing.
func NewTopologyManagerWithClock(cfg TopologyConfig, site0MySQL, site1MySQL mysql.Checker, failover *FailoverController, bootstrap *BootstrapController, bootstrapCfg BootstrapConfig, tainter platform.NodeTainter, hub *platform.Hub, dns platform.DNSUpdater, logger *slog.Logger, clk clock.Clock) *TopologyManager {
	cooldown := time.Duration(cfg.FailoverCooldown)
	if cooldown == 0 {
		cooldown = 5 * time.Minute
	}
	return &TopologyManager{
		cfg: cfg,
		sites: [2]siteTracker{
			{
				name:  cfg.Sites[0].Name,
				zone:  cfg.Sites[0].Zone,
				lbIP:  cfg.Sites[0].LBIP,
				mysql: site0MySQL,
				state: state.StateUnknown,
			},
			{
				name:  cfg.Sites[1].Name,
				zone:  cfg.Sites[1].Zone,
				lbIP:  cfg.Sites[1].LBIP,
				mysql: site1MySQL,
				state: state.StateUnknown,
			},
		},
		failover:         failover,
		failoverCooldown: cooldown,
		bootstrap:        bootstrap,
		bootstrapCfg:     bootstrapCfg,
		tainter:          tainter,
		hub:              hub,
		dns:              dns,
		logger:           logger,
		clock:            clk,
	}
}

// activeSiteLocked returns the name of the single writable site, or "" if zero
// or more than one site is writable. Must be called with tm.mu held.
func (tm *TopologyManager) activeSiteLocked() string {
	var name string
	var count int
	for i := range tm.sites {
		if tm.sites[i].state == state.StateWritable {
			name = tm.sites[i].name
			count++
		}
	}
	if count == 1 {
		return name
	}
	return ""
}

func (tm *TopologyManager) Status() StatusResponse {
	tm.mu.RLock()
	defer tm.mu.RUnlock()
	return StatusResponse{
		ActiveSite: tm.activeSiteLocked(),
		Sites: [2]SiteStatusEntry{
			{Name: tm.sites[0].name, State: tm.sites[0].state.String()},
			{Name: tm.sites[1].name, State: tm.sites[1].state.String()},
		},
		PollTime: tm.lastPollTime.Format(time.RFC3339),
	}
}

// Ready returns true after the first successful poll cycle.
func (tm *TopologyManager) Ready() bool {
	tm.mu.RLock()
	defer tm.mu.RUnlock()
	return tm.ready
}

// Run starts the polling loop. Blocks until ctx is cancelled.
// maxPollBackoffExponent caps the exponential backoff at 2^4 = 16x the base
// interval, subject to the 30s hard cap in adaptivePollInterval (e.g. 2s base
// → 30s effective cap). This keeps detection latency reasonable while still
// reducing waste when a DC is down for extended periods.
const maxPollBackoffExponent = 4

func (tm *TopologyManager) Run(ctx context.Context) {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	tm.mu.Lock()
	tm.cancelFunc = cancel
	tm.mu.Unlock()

	base := tm.cfg.PollIntervalDuration()

	// Do an initial poll immediately.
	tm.Poll(ctx)

	for {
		interval := tm.adaptivePollInterval(base)
		timer := tm.clock.NewTimer(interval)
		select {
		case <-ctx.Done():
			timer.Stop()
			return
		case <-timer.C():
			tm.Poll(ctx)
		}
	}
}

// adaptivePollInterval returns the next poll interval based on the worst-case
// failure count across both sites. When both sites are healthy the base interval
// is used; when either is failing, the interval increases exponentially up to
// base * 2^maxPollBackoffExponent (capped at 30s).
func (tm *TopologyManager) adaptivePollInterval(base time.Duration) time.Duration {
	maxFail := 0
	for i := range tm.sites {
		if fc := tm.sites[i].failCount; fc > maxFail {
			maxFail = fc
		}
	}
	// Only start backing off after the failure threshold is reached (DC is
	// confirmed unreachable, not just experiencing transient errors).
	backoffFails := maxFail - tm.cfg.FailureThreshold
	if backoffFails <= 0 {
		return base
	}
	if backoffFails > maxPollBackoffExponent {
		backoffFails = maxPollBackoffExponent
	}
	interval := base * time.Duration(1<<uint(backoffFails))
	if cap := 30 * time.Second; interval > cap {
		return cap
	}
	return interval
}

// Poll executes a single poll cycle: checks both sites, applies debounce,
// evaluates transitions, and triggers any necessary actions.
func (tm *TopologyManager) Poll(ctx context.Context) {
	// Poll both sites in parallel.
	type pollResult struct {
		readOnly bool
		err      error
		duration time.Duration
	}

	pollSite := func(site *siteTracker) pollResult {
		start := tm.clock.Now()
		pollCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
		defer cancel()
		ro, err := site.mysql.CheckReadOnly(pollCtx)
		return pollResult{readOnly: ro, err: err, duration: tm.clock.Since(start)}
	}

	var r [2]pollResult
	var wg sync.WaitGroup
	wg.Add(2)
	go func() { defer wg.Done(); r[0] = pollSite(&tm.sites[0]) }()
	go func() { defer wg.Done(); r[1] = pollSite(&tm.sites[1]) }()
	wg.Wait()

	for i := range tm.sites {
		metrics.PollLatency.WithLabelValues(tm.sites[i].name).Observe(r[i].duration.Seconds())
	}

	now := tm.clock.Now()

	// Compute new debounced states.
	var newStates [2]state.SiteState
	for i := range tm.sites {
		newStates[i] = tm.computeState(&tm.sites[i], r[i].readOnly, r[i].err)
	}

	// Update lastSeen and state under the lock to avoid races with Status()
	// and any goroutines that may read siteTracker fields concurrently.
	tm.mu.Lock()
	var prevStates [2]state.SiteState
	for i := range tm.sites {
		if r[i].err == nil {
			tm.sites[i].lastSeen = now
		}
		prevStates[i] = tm.sites[i].state
		if newStates[i] != prevStates[i] {
			tm.sites[i].state = newStates[i]
		}
	}
	tm.mu.Unlock()

	// Apply per-site transitions (outside the lock — these may do I/O).
	for i := range tm.sites {
		if newStates[i] != prevStates[i] {
			tm.logger.Info("state transition", "site", tm.sites[i].name, "from", prevStates[i], "to", newStates[i])
			metrics.StateTransitions.WithLabelValues(tm.sites[i].name, prevStates[i].String(), newStates[i].String()).Inc()
			tm.applyPerSiteAction(ctx, &tm.sites[i], state.EvalPerSiteTransition(prevStates[i], newStates[i]))
		}
	}

	// Check for pending promotion confirmation: if we promoted a site and it's
	// now writable, flip DNS.
	if tm.promotedSite != "" {
		site := tm.getSite(tm.promotedSite)
		if site != nil && site.state == state.StateWritable {
			tm.logger.Info("promotion confirmed, flipping DNS", "site", site.name, "ip", site.lbIP)
			if err := tm.dns.UpdateDNSRecord(ctx, site.lbIP); err != nil {
				tm.logger.Error("DNS flip failed", "site", site.name, "error", err)
			} else {
				metrics.DNSFlipCount.WithLabelValues(site.name).Inc()
			}
			tm.promotedSite = ""
		}
	}

	// Cross-site evaluation (only on state transitions to avoid repeated actions).
	var alertMsg string
	anyTransition := newStates[0] != prevStates[0] || newStates[1] != prevStates[1]
	if anyTransition {
		cross := state.EvalCrossSite(tm.sites[0].state, tm.sites[1].state, prevStates[0], prevStates[1], tm.sites[0].name, tm.sites[1].name)
		alertMsg = cross.Alert
		tm.applyCrossSiteAction(ctx, cross)
	}

	// Update status.
	tm.mu.Lock()
	tm.lastPollTime = tm.clock.Now()
	tm.ready = true
	tm.mu.Unlock()

	metrics.WSClientCount.Set(float64(tm.hub.ClientCount()))

	// Emit site state metrics every poll cycle.
	for i := range tm.sites {
		currentState := tm.sites[i].state.String()
		for _, s := range metrics.AllStates {
			val := 0.0
			if s == currentState {
				val = 1.0
			}
			metrics.SiteState.WithLabelValues(tm.sites[i].name, s).Set(val)
		}
	}

	// Check replication status on the read-only site (the replica).
	var siteRepl [2]*mysql.ReplicaStatus
	for i := range tm.sites {
		if tm.sites[i].state == state.StateReadOnly {
			rs, err := tm.sites[i].mysql.ShowReplicaStatus(ctx)
			if err != nil {
				tm.logger.Warn("failed to check replica status", "site", tm.sites[i].name, "error", err)
			} else {
				siteRepl[i] = rs
			}
		}
	}

	// Check if old primary recovery is needed.
	recoveryChanged := tm.checkRecovery(ctx, siteRepl)

	// Emit replication metrics.
	for i := range tm.sites {
		name := tm.sites[i].name
		if siteRepl[i] != nil {
			if siteRepl[i].SecondsBehindSource != nil {
				metrics.ReplicationLag.WithLabelValues(name).Set(float64(*siteRepl[i].SecondsBehindSource))
			} else {
				metrics.ReplicationLag.WithLabelValues(name).Set(-1)
			}
			ioVal := 0.0
			if siteRepl[i].IORunning {
				ioVal = 1.0
			}
			sqlVal := 0.0
			if siteRepl[i].SQLRunning {
				sqlVal = 1.0
			}
			metrics.ReplicationRunning.WithLabelValues(name, "io").Set(ioVal)
			metrics.ReplicationRunning.WithLabelValues(name, "sql").Set(sqlVal)
		} else if tm.sites[i].state == state.StateWritable {
			// Primary site: clear replication metrics (not a replica).
			metrics.ReplicationLag.DeleteLabelValues(name)
			metrics.ReplicationRunning.DeleteLabelValues(name, "io")
			metrics.ReplicationRunning.DeleteLabelValues(name, "sql")
		}
	}

	// Notify the status callback on any state change or recovery event.
	if (anyTransition || recoveryChanged) && tm.StatusCallback != nil {
		tm.mu.RLock()
		activeSite := tm.activeSiteLocked()
		bootstrapPhase := string(tm.bootstrapPhase)
		bootstrapErrStr := ""
		if tm.bootstrapErr != nil {
			bootstrapErrStr = tm.bootstrapErr.Error()
		}
		recoverySite := tm.recoveryPendingSite
		recoveryState := tm.recoveryStateLocked()
		divergentGtid := tm.recoveryDivergentGtid
		divergentTxnCount := tm.recoveryDivergentCount
		promotionGtid := tm.promotionGtidExecuted
		tm.mu.RUnlock()
		tm.StatusCallback(TopologySnapshot{
			SiteNames:          [2]string{tm.sites[0].name, tm.sites[1].name},
			SiteStates:         [2]state.SiteState{tm.sites[0].state, tm.sites[1].state},
			SiteLastSeen:       [2]time.Time{tm.sites[0].lastSeen, tm.sites[1].lastSeen},
			SiteReplication:    siteRepl,
			ActiveSite:         activeSite,
			LastFailover:       tm.lastFailover,
			LastFailoverTarget: tm.lastFailoverTarget,
			Alert:              alertMsg,
			BootstrapPhase:     bootstrapPhase,
			BootstrapError:     bootstrapErrStr,

			PromotionGtidExecuted: promotionGtid,
			RecoverySite:          recoverySite,
			RecoveryState:         recoveryState,
			DivergentGtid:         divergentGtid,
			DivergentTxnCount:     divergentTxnCount,
		})
	}

	// Broadcast full topology to WebSocket clients on every poll cycle.
	tm.broadcastTopology(siteRepl, alertMsg)
}

// broadcastTopology builds a full TopologyMessage from the current locked
// state and pushes it to all WebSocket clients. Called at the end of every
// poll cycle so dashboards get live updates (not just on transitions).
func (tm *TopologyManager) broadcastTopology(siteRepl [2]*mysql.ReplicaStatus, alertMsg string) {
	tm.mu.RLock()
	defer tm.mu.RUnlock()

	sites := make([]platform.TopologySite, 0, len(tm.sites))
	for i := range tm.sites {
		s := platform.TopologySite{
			Name:  tm.sites[i].name,
			State: tm.sites[i].state.String(),
		}
		if !tm.sites[i].lastSeen.IsZero() {
			s.LastSeen = tm.sites[i].lastSeen.Format(time.RFC3339)
		}
		if siteRepl[i] != nil {
			s.Replicating = siteRepl[i].IORunning && siteRepl[i].SQLRunning
			s.SecondsBehindSource = siteRepl[i].SecondsBehindSource
			s.GtidExecuted = siteRepl[i].ExecutedGtidSet
		}
		sites = append(sites, s)
	}

	msg := platform.TopologyMessage{
		Namespace:          tm.cfg.Namespace,
		Group:              tm.cfg.Name,
		ActiveSite:         tm.activeSiteLocked(),
		Sites:              sites,
		LastFailoverTarget: tm.lastFailoverTarget,
		Alert:              alertMsg,
		PollTime:           tm.lastPollTime.Format(time.RFC3339),
	}
	if !tm.lastFailover.IsZero() {
		msg.LastFailover = tm.lastFailover.Format(time.RFC3339)
	}
	tm.hub.Broadcast(msg)
}

// computeState applies debounce logic and returns the new state for a site.
func (tm *TopologyManager) computeState(site *siteTracker, readOnly bool, err error) state.SiteState {
	if err != nil {
		site.recoveryCount = 0
		site.failCount++
		if site.failCount >= tm.cfg.FailureThreshold {
			return state.StateUnreachable
		}
		return site.state // not enough failures yet, keep current state
	}

	// Successful poll.
	site.failCount = 0

	if readOnly {
		site.recoveryCount = 0
		return state.StateReadOnly
	}

	// read_only=0 (writable)
	if site.state != state.StateWritable {
		site.recoveryCount++
		if site.recoveryCount >= tm.cfg.RecoveryThreshold {
			return state.StateWritable
		}
		return site.state // not enough recoveries yet
	}

	return state.StateWritable
}

// taintSelector returns the label selector for tainting nodes belonging to a site.
func (tm *TopologyManager) taintSelector(site *siteTracker) string {
	return fmt.Sprintf("shipstream.io/failover-group=%s,shipstream.io/site=%s", tm.cfg.Name, site.name)
}

func (tm *TopologyManager) applyPerSiteAction(ctx context.Context, site *siteTracker, action state.Action) {
	if action.Taint != nil {
		selector := tm.taintSelector(site)
		if err := tm.tainter.SetTaint(ctx, selector, *action.Taint); err != nil {
			tm.logger.Error("taint operation failed", "site", site.name, "apply", *action.Taint, "error", err)
		} else {
			op := "remove"
			if *action.Taint {
				op = "apply"
			}
			metrics.TaintOperations.WithLabelValues(site.name, op).Inc()
		}
	}

}

func (tm *TopologyManager) applyCrossSiteAction(ctx context.Context, action state.CrossSiteAction) {
	if action.Alert != "" {
		tm.logger.Warn("ALERT", "message", action.Alert)
	}

	// Suppress any cross-site actions while a bootstrap is in progress: the
	// replica will appear unreachable during the MySQL restart that follows
	// CLONE INSTANCE, and we do not want to initiate a failover on that signal.
	if tm.isBootstrapping() {
		tm.mu.RLock()
		phase := tm.bootstrapPhase
		tm.mu.RUnlock()
		tm.logger.Info("cross-site action suppressed during bootstrap", "phase", phase)
		return
	}

	// Detect fresh-deploy split-brain and auto-bootstrap the replica.
	if action.SplitBrain && tm.bootstrap != nil && tm.bootstrapCfg.ReplUser != "" {
		tm.mu.RLock()
		phase := tm.bootstrapPhase
		suppressed := tm.autoBootstrapSuppressed
		tm.mu.RUnlock()
		if suppressed {
			tm.logger.Info("auto-bootstrap suppressed (initFromBackup restore in flight)")
		} else if phase == BootstrapPhaseNone || phase == BootstrapPhaseFailed {
			if tm.isFreshDeploy(ctx) {
				tm.startBootstrap(ctx)
				return
			}
		}
	}

	// If both sites are writable after a prior failover and this is not a
	// fresh deploy, the old primary has returned. Fence it immediately so it
	// stops accepting writes; recovery proceeds in checkRecovery once it
	// transitions to read-only.
	if action.SplitBrain && tm.lastFailoverTarget != "" && !tm.isBootstrapping() {
		oldPrimarySiteName := tm.otherSiteName(tm.lastFailoverTarget)
		if site := tm.getSite(oldPrimarySiteName); site != nil && site.state == state.StateWritable {
			tm.logger.Info("fencing returning old primary (split brain after failover)", "site", oldPrimarySiteName)
			if err := site.mysql.SetSuperReadOnly(ctx, true); err != nil {
				tm.logger.Error("failed to fence returning old primary", "site", oldPrimarySiteName, "error", err)
			}
		}
	}

	if action.PromoteSite != "" && tm.promotedSite == "" {
		// Check anti-flap cooldown.
		if !tm.lastFailover.IsZero() && tm.clock.Since(tm.lastFailover) < tm.failoverCooldown {
			tm.logger.Info("failover blocked by anti-flap cooldown",
				"lastFailover", tm.lastFailover, "cooldown", tm.failoverCooldown)
			return
		}

		candidate := tm.getSite(action.PromoteSite)
		oldPrimaryName := tm.otherSiteName(action.PromoteSite)
		oldPrimary := tm.getSite(oldPrimaryName)

		if candidate != nil {
			tm.logger.Info("initiating failover", "candidate", candidate.name, "oldPrimary", oldPrimaryName)

			var oldPrimaryChecker mysql.Checker
			if oldPrimary != nil {
				oldPrimaryChecker = oldPrimary.mysql
			}

			promotionGtid, err := tm.failover.Execute(ctx, candidate.mysql, oldPrimaryChecker, candidate.name)
			if err != nil {
				tm.logger.Error("failover failed", "error", err)
				return
			}
			tm.promotionGtidExecuted = promotionGtid
			// DNS flip deferred until next poll confirms read_only=0.
			tm.promotedSite = candidate.name
			tm.lastFailover = tm.clock.Now()
			tm.lastFailoverTarget = candidate.name
		}
	}
}

// SetLastFailoverForTest allows tests to manipulate the cooldown timer
// without sleeping. This should only be used in tests.
func (tm *TopologyManager) SetLastFailoverForTest(t time.Time) {
	tm.lastFailover = t
}

func (tm *TopologyManager) otherSiteName(name string) string {
	if name == tm.sites[0].name {
		return tm.sites[1].name
	}
	return tm.sites[0].name
}

// Stop cancels the TopologyManager's context, causing the Run loop to exit.
// It is safe to call even if the manager is not running.
func (tm *TopologyManager) Stop() {
	tm.mu.Lock()
	defer tm.mu.Unlock()
	if tm.cancelFunc != nil {
		tm.cancelFunc()
		tm.cancelFunc = nil
	}
}

func (tm *TopologyManager) getSite(name string) *siteTracker {
	for i := range tm.sites {
		if tm.sites[i].name == name {
			return &tm.sites[i]
		}
	}
	return nil
}

// BootstrapPhase returns the current bootstrap phase (empty string if no
// bootstrap has run). Intended for tests and status reporting.
func (tm *TopologyManager) BootstrapPhase() BootstrapPhase {
	tm.mu.RLock()
	defer tm.mu.RUnlock()
	return tm.bootstrapPhase
}

// SetAutoBootstrapSuppressed toggles the gate that prevents the
// fresh-deploy clone path from racing a one-shot initFromBackup
// restore. The runner calls this on every sync based on the CR's
// status.restore.phase.
func (tm *TopologyManager) SetAutoBootstrapSuppressed(v bool) {
	tm.mu.Lock()
	tm.autoBootstrapSuppressed = v
	tm.mu.Unlock()
}

// isBootstrapping reports whether an auto-bootstrap is currently running.
func (tm *TopologyManager) isBootstrapping() bool {
	tm.mu.RLock()
	defer tm.mu.RUnlock()
	switch tm.bootstrapPhase {
	case BootstrapPhaseCloning, BootstrapPhaseRestarting, BootstrapPhaseSetupRepl:
		return true
	}
	return false
}

// isFreshDeploy reports whether both sites are writable AND neither has ever
// had replication configured. This is the signature of a fresh deployment —
// as opposed to a "true" split-brain where at least one side previously had
// replication set up and may now hold diverged writes.
func (tm *TopologyManager) isFreshDeploy(ctx context.Context) bool {
	if tm.sites[0].state != state.StateWritable || tm.sites[1].state != state.StateWritable {
		return false
	}
	for i := range tm.sites {
		rs, err := tm.sites[i].mysql.ShowReplicaStatus(ctx)
		if err != nil {
			// Be conservative: if we can't determine replication state, don't bootstrap.
			tm.logger.Warn("fresh-deploy check: could not read replica status", "site", tm.sites[i].name, "error", err)
			return false
		}
		if rs != nil {
			// Replication was configured at some point — not a fresh deploy.
			return false
		}
	}
	return true
}

// startBootstrap kicks off the async bootstrap goroutine. Caller must hold no locks.
// Site 0 (first in the spec) is chosen as the primary for fresh-deploy bootstrap.
func (tm *TopologyManager) startBootstrap(ctx context.Context) {
	primaryIdx := 0
	replicaIdx := 1

	tm.mu.Lock()
	tm.bootstrapPhase = BootstrapPhaseCloning
	tm.bootstrapErr = nil
	tm.mu.Unlock()

	tm.logger.Info("starting fresh-deploy bootstrap",
		"primary", tm.sites[primaryIdx].name,
		"replica", tm.sites[replicaIdx].name,
		"primaryHost", tm.cfg.SiteHosts[primaryIdx])

	tm.emitBootstrapStatus()

	go func() {
		err := tm.runBootstrap(ctx, primaryIdx, replicaIdx)
		tm.mu.Lock()
		if err != nil {
			tm.bootstrapPhase = BootstrapPhaseFailed
			tm.bootstrapErr = err
			tm.logger.Error("bootstrap failed", "error", err)
		} else {
			tm.bootstrapPhase = BootstrapPhaseDone
			tm.logger.Info("bootstrap completed successfully")
		}
		tm.mu.Unlock()
		tm.emitBootstrapStatus()
	}()
}

// runBootstrap performs the clone, waits for the MySQL restart, and sets up replication.
func (tm *TopologyManager) runBootstrap(ctx context.Context, primaryIdx, replicaIdx int) error {
	primary := tm.sites[primaryIdx].mysql
	replica := tm.sites[replicaIdx].mysql
	primaryHost := tm.cfg.SiteHosts[primaryIdx]
	if primaryHost == "" {
		return fmt.Errorf("bootstrap: primary host not configured for site %s", tm.sites[primaryIdx].name)
	}

	// Phase 1: Clone from primary. MySQL auto-restarts at the end of a
	// successful clone, which typically causes the in-flight CLONE INSTANCE
	// query to return a connection error. We treat such errors as potential
	// success and proceed to the wait phase, where we verify the replica comes
	// back online. A true clone failure will surface during the wait or the
	// subsequent SetupReplication call (e.g. GTID set mismatch, missing data).
	err := tm.bootstrap.BootstrapReplica(ctx, BootstrapOpts{
		Primary:      primary,
		Replica:      replica,
		PrimaryHost:  primaryHost,
		ReplicaSite:  tm.sites[replicaIdx].name,
		ReplUser:     tm.bootstrapCfg.ReplUser,
		ReplPassword: tm.bootstrapCfg.ReplPassword,
		UseSSL:       tm.bootstrapCfg.UseSSL,
		CloneTimeout: tm.bootstrapCfg.CloneTimeout,
	})
	if err != nil && !isCloneConnectionDrop(err) {
		return fmt.Errorf("clone: %w", err)
	}
	if err != nil {
		tm.logger.Info("clone returned expected connection drop, waiting for restart", "error", err)
	}

	// Phase 2: Wait for MySQL to come back online after the post-clone restart.
	tm.mu.Lock()
	tm.bootstrapPhase = BootstrapPhaseRestarting
	tm.mu.Unlock()
	tm.emitBootstrapStatus()

	waitErr := util.RetryWithBackoff(ctx, tm.logger, 10, 2*time.Second, func() error {
		// Bound each probe so a hung MySQL cannot block the bootstrap goroutine
		// indefinitely; matches the poll loop's 5s per-attempt budget.
		probeCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
		defer cancel()
		_, checkErr := replica.CheckReadOnly(probeCtx)
		return checkErr
	})
	if waitErr != nil {
		return fmt.Errorf("wait for replica restart: %w", waitErr)
	}

	// Phase 3: Configure and start replication.
	tm.mu.Lock()
	tm.bootstrapPhase = BootstrapPhaseSetupRepl
	tm.mu.Unlock()
	tm.emitBootstrapStatus()

	if err := tm.bootstrap.SetupReplication(ctx, replica, ReplicationSetupOpts{
		SourceHost:   primaryHost,
		ReplUser:     tm.bootstrapCfg.ReplUser,
		ReplPassword: tm.bootstrapCfg.ReplPassword,
		UseSSL:       tm.bootstrapCfg.UseSSL,
	}); err != nil {
		return fmt.Errorf("setup replication: %w", err)
	}

	return nil
}

// isCloneConnectionDrop reports whether an error from CLONE INSTANCE looks
// like the expected connection drop caused by the post-clone MySQL restart.
func isCloneConnectionDrop(err error) bool {
	if err == nil {
		return false
	}
	msg := err.Error()
	for _, needle := range []string{
		"invalid connection",
		"connection reset",
		"broken pipe",
		"EOF",
		"bad connection",
		"Lost connection",
	} {
		if strings.Contains(msg, needle) {
			return true
		}
	}
	return false
}

// recoveryStateLocked returns the current recovery state string for status
// reporting. Must be called with tm.mu held (at least RLock).
func (tm *TopologyManager) recoveryStateLocked() string {
	if tm.recoveryPendingSite != "" {
		return "RecoveryBlocked"
	}
	return ""
}

// checkRecovery detects an old primary that has come back after failover and
// either auto-rejoins it (no divergence) or blocks with metadata (divergence
// detected). Returns true if recovery state changed this cycle.
func (tm *TopologyManager) checkRecovery(ctx context.Context, siteRepl [2]*mysql.ReplicaStatus) bool {
	if tm.lastFailoverTarget == "" || tm.isBootstrapping() {
		return false
	}
	if tm.bootstrapCfg.ReplUser == "" {
		return false
	}

	// Find the active (writable) site.
	activeIdx := -1
	for i := range tm.sites {
		if tm.sites[i].state == state.StateWritable {
			if activeIdx != -1 {
				return false // both writable, handled by split-brain logic
			}
			activeIdx = i
		}
	}
	if activeIdx == -1 {
		return false
	}

	otherIdx := 1 - activeIdx
	otherSite := &tm.sites[otherIdx]

	if otherSite.state != state.StateReadOnly {
		return false
	}

	// Already replicating — recovery was already completed.
	if siteRepl[otherIdx] != nil && (siteRepl[otherIdx].IORunning || siteRepl[otherIdx].SQLRunning) {
		// If we had a previous recovery-blocked state that is now resolved
		// (e.g. admin wiped and re-cloned), clear it.
		if tm.recoveryPendingSite == otherSite.name {
			tm.mu.Lock()
			tm.recoveryPendingSite = ""
			tm.recoveryDivergentGtid = ""
			tm.recoveryDivergentCount = 0
			tm.mu.Unlock()
			metrics.DivergentTransactions.WithLabelValues(otherSite.name).Set(0)
			tm.logger.Info("recovery state cleared (site is now replicating)", "site", otherSite.name)
			return true
		}
		return false
	}

	// Already detected and blocked — nothing to do.
	if tm.recoveryPendingSite == otherSite.name {
		return false
	}

	// Read-only site with no active replication after a prior failover — initiate recovery.
	return tm.initiateRecovery(ctx, otherIdx, activeIdx)
}

// initiateRecovery fences the old primary, compares GTID sets, and either
// auto-recovers (no divergence) or blocks with metadata (divergence).
// Returns true if recovery state changed.
func (tm *TopologyManager) initiateRecovery(ctx context.Context, oldPrimaryIdx, newPrimaryIdx int) bool {
	oldPrimary := &tm.sites[oldPrimaryIdx]
	newPrimary := &tm.sites[newPrimaryIdx]

	tm.logger.Info("initiating old primary recovery", "oldPrimary", oldPrimary.name, "newPrimary", newPrimary.name)

	// Defensive fence.
	if err := oldPrimary.mysql.SetSuperReadOnly(ctx, true); err != nil {
		tm.logger.Error("recovery: failed to fence old primary", "site", oldPrimary.name, "error", err)
		return false
	}

	oldGtidStr, err := oldPrimary.mysql.GetGtidExecuted(ctx)
	if err != nil {
		tm.logger.Error("recovery: failed to get old primary GTID", "site", oldPrimary.name, "error", err)
		return false
	}
	newGtidStr, err := newPrimary.mysql.GetGtidExecuted(ctx)
	if err != nil {
		tm.logger.Error("recovery: failed to get new primary GTID", "site", newPrimary.name, "error", err)
		return false
	}

	oldGtid, err := mysql.ParseGTIDSet(oldGtidStr)
	if err != nil {
		tm.logger.Error("recovery: failed to parse old primary GTID", "site", oldPrimary.name, "error", err)
		return false
	}
	newGtid, err := mysql.ParseGTIDSet(newGtidStr)
	if err != nil {
		tm.logger.Error("recovery: failed to parse new primary GTID", "site", newPrimary.name, "error", err)
		return false
	}

	if newGtid.Contains(oldGtid) {
		tm.logger.Info("no GTID divergence, auto-recovering old primary as replica", "site", oldPrimary.name)
		tm.executeRecovery(ctx, oldPrimaryIdx, newPrimaryIdx)
		return true
	}

	divergent := oldGtid.Subtract(newGtid)
	count := divergent.TransactionCount()
	tm.logger.Warn("GTID divergence detected — old primary has transactions not on new primary",
		"site", oldPrimary.name,
		"divergentTransactions", count,
		"divergentGtid", divergent.String(),
		"oldPrimaryGtid", oldGtidStr,
		"newPrimaryGtid", newGtidStr)

	tm.mu.Lock()
	tm.recoveryPendingSite = oldPrimary.name
	tm.recoveryDivergentGtid = divergent.String()
	tm.recoveryDivergentCount = count
	tm.mu.Unlock()

	metrics.DivergentTransactions.WithLabelValues(oldPrimary.name).Set(float64(count))
	return true
}

// executeRecovery reconfigures the old primary as a replica of the new primary.
func (tm *TopologyManager) executeRecovery(ctx context.Context, oldPrimaryIdx, newPrimaryIdx int) {
	oldPrimary := &tm.sites[oldPrimaryIdx]
	newPrimaryHost := tm.cfg.SiteHosts[newPrimaryIdx]

	err := tm.failover.RecoverOldPrimary(ctx, oldPrimary.mysql, newPrimaryHost,
		tm.bootstrapCfg.ReplUser, tm.bootstrapCfg.ReplPassword, tm.bootstrapCfg.UseSSL)
	if err != nil {
		tm.logger.Error("old primary recovery failed", "site", oldPrimary.name, "error", err)
		return
	}

	tm.mu.Lock()
	tm.recoveryPendingSite = ""
	tm.recoveryDivergentGtid = ""
	tm.recoveryDivergentCount = 0
	tm.mu.Unlock()

	metrics.DivergentTransactions.WithLabelValues(oldPrimary.name).Set(0)
	tm.logger.Info("old primary recovery complete — now replicating from new primary", "site", oldPrimary.name, "source", newPrimaryHost)
}

// emitBootstrapStatus notifies the runner that the bootstrap phase changed.
// It uses a dedicated BootstrapStatusCallback so that only the Bootstrapping
// condition is updated on the CR — a full TopologySnapshot from this path
// would lack SiteReplication/UpdatePhase and could inadvertently clear
// Degraded/Updating conditions that were set by the most recent Poll cycle.
func (tm *TopologyManager) emitBootstrapStatus() {
	if tm.BootstrapStatusCallback == nil {
		return
	}
	tm.mu.RLock()
	phase := string(tm.bootstrapPhase)
	errMsg := ""
	if tm.bootstrapErr != nil {
		errMsg = tm.bootstrapErr.Error()
	}
	tm.mu.RUnlock()
	tm.BootstrapStatusCallback(phase, errMsg)
}
