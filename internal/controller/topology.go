package controller

import (
	"context"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"github.com/shipstream/bloodraven/internal/clock"
	"github.com/shipstream/bloodraven/internal/metrics"
	"github.com/shipstream/bloodraven/internal/mysql"
	"github.com/shipstream/bloodraven/internal/platform"
	"github.com/shipstream/bloodraven/internal/state"
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

	PollInterval      int64 // nanoseconds
	FailureThreshold  int
	RecoveryThreshold int
	FailoverCooldown  int64 // nanoseconds, default 5m
}

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
	promotedSite string // empty = no pending promotion

	// StatusCallback is invoked after each poll cycle that produces a state change.
	// The runner sets this to push status updates to the CR.
	StatusCallback func(TopologySnapshot)

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

func NewTopologyManager(cfg TopologyConfig, site0MySQL, site1MySQL mysql.Checker, failover *FailoverController, tainter platform.NodeTainter, hub *platform.Hub, dns platform.DNSUpdater, logger *slog.Logger) *TopologyManager {
	return NewTopologyManagerWithClock(cfg, site0MySQL, site1MySQL, failover, tainter, hub, dns, logger, clock.RealClock{})
}

// NewTopologyManagerWithClock creates a TopologyManager with an injectable clock for testing.
func NewTopologyManagerWithClock(cfg TopologyConfig, site0MySQL, site1MySQL mysql.Checker, failover *FailoverController, tainter platform.NodeTainter, hub *platform.Hub, dns platform.DNSUpdater, logger *slog.Logger, clk clock.Clock) *TopologyManager {
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
func (tm *TopologyManager) Run(ctx context.Context) {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	tm.mu.Lock()
	tm.cancelFunc = cancel
	tm.mu.Unlock()

	ticker := tm.clock.NewTicker(tm.cfg.PollIntervalDuration())
	defer ticker.Stop()

	// Do an initial poll immediately.
	tm.Poll(ctx)

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C():
			tm.Poll(ctx)
		}
	}
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

	// Track last successful poll time per site.
	now := tm.clock.Now()
	for i := range tm.sites {
		if r[i].err == nil {
			tm.sites[i].lastSeen = now
		}
	}

	// Compute new debounced states.
	var newStates [2]state.SiteState
	for i := range tm.sites {
		newStates[i] = tm.computeState(&tm.sites[i], r[i].readOnly, r[i].err)
	}

	// Read and update states under the lock to avoid races with Status().
	tm.mu.Lock()
	var prevStates [2]state.SiteState
	for i := range tm.sites {
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

	// Notify the status callback on any state change.
	if anyTransition && tm.StatusCallback != nil {
		tm.StatusCallback(TopologySnapshot{
			SiteNames:          [2]string{tm.sites[0].name, tm.sites[1].name},
			SiteStates:         [2]state.SiteState{tm.sites[0].state, tm.sites[1].state},
			SiteLastSeen:       [2]time.Time{tm.sites[0].lastSeen, tm.sites[1].lastSeen},
			SiteReplication:    siteRepl,
			ActiveSite:         tm.activeSiteLocked(),
			LastFailover:       tm.lastFailover,
			LastFailoverTarget: tm.lastFailoverTarget,
			Alert:              alertMsg,
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

			if err := tm.failover.Execute(ctx, candidate.mysql, oldPrimaryChecker, candidate.name); err != nil {
				tm.logger.Error("failover failed", "error", err)
				return
			}
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
