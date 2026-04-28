package controller

import (
	"context"
	"fmt"
	"log/slog"
	"reflect"
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
	Role state.SiteRole
	// TaintSelector is the Kubernetes label selector used to find nodes
	// that should receive this failover group's readonly taint for the site.
	TaintSelector string
	// Host is the MySQL service hostname (without port) for this site.
	// Used as the donor/source host for clone and replication setup.
	Host string
}

// TopologyConfig holds topology manager configuration, decoupled from
// CR-type specifics. Callers build this from MysqlFailoverGroupSpec via
// CRConfigToTopologyConfig.
type TopologyConfig struct {
	Namespace string // failover group namespace (from CR metadata.namespace)
	Name      string // failover group name (from CR metadata.name)

	// Sites lists every site that participates in this topology, in
	// declared order. The list length must equal the length of
	// spec.sites.
	Sites []SiteTopologyConfig

	PollInterval      int64 // nanoseconds
	FailureThreshold  int
	RecoveryThreshold int
	FailoverCooldown  int64 // nanoseconds, default 5m

	// SitePriorities is the ordered list of primary-candidate site
	// names that win split-brain ties. An empty slice means manual
	// resolution (alert only).
	SitePriorities []string

	// CredentialHash is a hash of the operator secret data. A change
	// triggers a topology manager restart with new MySQL connections.
	CredentialHash string

	// DragonflyEnabled mirrors spec.dragonfly.enabled at the time the
	// config was extracted. A change triggers a manager restart so the
	// DragonflyManager goroutine is started or stopped to match.
	DragonflyEnabled bool
}

// Equal reports whether two TopologyConfig values have the same field
// set. Used by the runner to detect when a CR change requires restart.
// It is equivalent to reflect.DeepEqual but documents the intent at
// call sites.
func (c TopologyConfig) Equal(other TopologyConfig) bool {
	return reflect.DeepEqual(c, other)
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

// SiteSnapshot captures one site's observation at a point in time.
type SiteSnapshot struct {
	Name        string
	Role        state.SiteRole
	State       state.SiteState
	LastSeen    time.Time
	Replication *mysql.ReplicaStatus // nil if site is primary or unreachable
}

// TopologySnapshot captures the topology state at a point in time.
// It is passed to the StatusCallback after each poll cycle that
// produces a state change.
type TopologySnapshot struct {
	// Sites is the per-site snapshot, parallel to cfg.Sites.
	Sites []SiteSnapshot

	ActiveSite         string
	LastFailover       time.Time
	LastFailoverTarget string
	Alert              string // non-empty if a cross-site alert fired this cycle
	DegradedReason     string // one of "Healthy", "Degraded", "SplitBrain", "NoPrimary", "TotalLoss", or ""
	UpdatePhase        string // non-empty if an ordered update is in progress
	BootstrapPhase     string // non-empty if a fresh-deploy bootstrap is in progress or finished
	BootstrapError     string // non-empty if bootstrap failed
	BootstrapSource    string // "fresh-deploy", "auto-clone", or "reclone"

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
	role          state.SiteRole
	taintSelector string
	host          string
	mysql         mysql.Checker
	state         state.SiteState
	failCount     int
	recoveryCount int
	lastSeen      time.Time // last successful poll

	// replicating is true only when the site is a read-only replica with IO+SQL
	// threads running and a configured source host, observed for replicatingStreak
	// consecutive poll ticks. Used by checkUpdate to refuse ordered updates against
	// a stale standby whose super_read_only=ON but replication is not actually running.
	replicating       bool
	replicatingStreak int
}

// isHealthyReplica reports whether the site is a read-only replica with debounced
// replication health. Used as the precondition for starting an ordered update.
func (t *siteTracker) isHealthyReplica() bool {
	return t.state == state.StateReadOnly && t.replicating
}

// isPromotable reports whether this site is eligible for auto-promotion.
func (t *siteTracker) isPromotable() bool {
	return t.role == state.SiteRolePrimaryCandidate
}

// observation returns this tracker's current state as a SiteObservation
// suitable for feeding EvalCrossSite. Caller must hold tm.mu (read).
func (t *siteTracker) observation() state.SiteObservation {
	return state.SiteObservation{Name: t.name, Role: t.role, State: t.state}
}

// TopologyManager is the main control loop.
type TopologyManager struct {
	cfg     TopologyConfig
	sites   []siteTracker
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

	// Ordered update orchestration.
	updater *UpdateController

	// ApplyUpdate is a callback provided by the runner that patches a single
	// site's Deployment to match the desired spec. The topology manager calls
	// this from the UpdateController when a rolling update is in progress.
	ApplyUpdate func(ctx context.Context, siteName string) error

	// Bootstrap orchestration. When bootstrap is nil or bootstrapCfg.ReplUser is
	// empty, auto-bootstrap is disabled. bootstrapPhase/bootstrapErr are protected by mu.
	bootstrap      *BootstrapController
	bootstrapCfg   BootstrapConfig
	bootstrapPhase BootstrapPhase
	bootstrapErr   error

	// bootstrapSource records how the current bootstrap was initiated
	// ("fresh-deploy", "auto-clone", or "reclone"). Propagated through
	// BootstrapStatusCallback into condition messages. Protected by mu.
	bootstrapSource string

	// reclonePendingSite is set by the runner when an admin annotation
	// requests a reclone of a specific site. Processed during the next
	// poll cycle then cleared. Protected by mu.
	reclonePendingSite string

	// autoBootstrapSuppressed is set by the runner while a one-shot
	// initFromBackup restore is in flight. It prevents the fresh-deploy
	// auto-clone path from racing the restore Job to populate the
	// primary. Protected by mu. See runner.sync().
	autoBootstrapSuppressed bool

	// topologyFrozen is set by the runner while an in-place restore
	// (spec.restoreInPlace) is actively mutating the cluster. Unlike
	// autoBootstrapSuppressed (which only gates fresh-deploy cloning),
	// this flag blocks every cross-site decision — promotion, reclone,
	// auto-bootstrap, even fencing of a returning old primary. The
	// topology manager observes state normally (so status still
	// reflects reality) but takes no corrective action until the
	// in-place restore reconciler clears the flag. Protected by mu.
	// See runner.sync() and MysqlFailoverGroupReconciler.reconcileInPlaceRestore.
	topologyFrozen bool

	// plannedFailoverActive is set by the runner while the reconciler
	// is driving a planned-failover state machine. Blocks automatic
	// cross-site action in applyCrossSiteAction so an emergency
	// promotion cannot race the admin-triggered switchover. Cleared on
	// Succeeded or Failed. Protected by mu. See
	// planned_failover_reconciler.go.
	plannedFailoverActive bool

	// specDriftSites lists site names whose Deployment spec-hash differs
	// from the desired hash. Set by the runner, consumed by checkUpdate.
	// Protected by mu.
	specDriftSites []string

	// StatusCallback is invoked after each poll cycle that produces a state change.
	// The runner sets this to push status updates to the CR.
	StatusCallback func(TopologySnapshot)

	// BootstrapStatusCallback is invoked from the bootstrap goroutine when the
	// bootstrap phase changes. Unlike StatusCallback it pushes only the
	// Bootstrapping condition so that unrelated conditions (Degraded,
	// ReplicationBroken, Updating, ...) are not inadvertently cleared by a
	// partially-populated TopologySnapshot during an async bootstrap run.
	BootstrapStatusCallback func(phase, errMsg, source string)

	// EmergencyFailoverCallback is invoked synchronously after a
	// successful emergency failover Execute, on the same goroutine that
	// drove the failover. The runner wires this to a best-effort
	// DragonflyManager.TryEmergencyPromote so the cache subsystem
	// follows the durable promotion. The callback must not return errors
	// — emergency MySQL failover safety is unconditional.
	EmergencyFailoverCallback func(ctx context.Context, target, oldPrimary string)

	mu           sync.RWMutex
	lastPollTime time.Time
	ready        bool
	cancelFunc   context.CancelFunc
}

// SiteStatusEntry is a single site's status in the StatusResponse.
type SiteStatusEntry struct {
	Name                      string `json:"name"`
	Role                      string `json:"role,omitempty"`
	State                     string `json:"state"`
	RecoveryState             string `json:"recoveryState,omitempty"`
	DivergentGtid             string `json:"divergentGtid,omitempty"`
	DivergentTransactionCount *int64 `json:"divergentTransactionCount,omitempty"`
}

// StatusResponse is returned by the /status endpoint.
type StatusResponse struct {
	ActiveSite            string            `json:"activeSite"`
	Sites                 []SiteStatusEntry `json:"sites"`
	PollTime              string            `json:"pollTime"`
	PromotionGtidExecuted string            `json:"promotionGtidExecuted,omitempty"`
}

// NewTopologyManager creates a TopologyManager for the given configuration.
// siteCheckers must be parallel to cfg.Sites.
func NewTopologyManager(cfg TopologyConfig, siteCheckers []mysql.Checker, failover *FailoverController, updater *UpdateController, bootstrap *BootstrapController, bootstrapCfg BootstrapConfig, tainter platform.NodeTainter, hub *platform.Hub, dns platform.DNSUpdater, logger *slog.Logger) *TopologyManager {
	return NewTopologyManagerWithClock(cfg, siteCheckers, failover, updater, bootstrap, bootstrapCfg, tainter, hub, dns, logger, clock.RealClock{})
}

// NewTopologyManagerWithClock creates a TopologyManager with an injectable clock for testing.
func NewTopologyManagerWithClock(cfg TopologyConfig, siteCheckers []mysql.Checker, failover *FailoverController, updater *UpdateController, bootstrap *BootstrapController, bootstrapCfg BootstrapConfig, tainter platform.NodeTainter, hub *platform.Hub, dns platform.DNSUpdater, logger *slog.Logger, clk clock.Clock) *TopologyManager {
	cooldown := time.Duration(cfg.FailoverCooldown)
	if cooldown == 0 {
		cooldown = 5 * time.Minute
	}
	if len(siteCheckers) != len(cfg.Sites) {
		panic(fmt.Sprintf("topology manager: got %d MySQL checkers for %d configured sites", len(siteCheckers), len(cfg.Sites)))
	}
	sites := make([]siteTracker, len(cfg.Sites))
	for i, s := range cfg.Sites {
		role := s.Role
		if role == "" {
			role = state.SiteRolePrimaryCandidate
		}
		sites[i] = siteTracker{
			name:          s.Name,
			zone:          s.Zone,
			lbIP:          s.LBIP,
			role:          role,
			taintSelector: s.TaintSelector,
			host:          s.Host,
			mysql:         siteCheckers[i],
			state:         state.StateUnknown,
		}
	}
	return &TopologyManager{
		cfg:              cfg,
		sites:            sites,
		failover:         failover,
		failoverCooldown: cooldown,
		updater:          updater,
		bootstrap:        bootstrap,
		bootstrapCfg:     bootstrapCfg,
		tainter:          tainter,
		hub:              hub,
		dns:              dns,
		logger:           logger,
		clock:            clk,
	}
}

// SetLastFailoverTarget restores the failover target from persisted CR status.
// Called once at startup so recovery logic works across operator restarts.
func (tm *TopologyManager) SetLastFailoverTarget(target string) {
	tm.lastFailoverTarget = target
}

// activeSiteLocked returns the name of the single writable site, or ""
// if zero or more than one site is writable. Must be called with tm.mu
// held.
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
	sites := make([]SiteStatusEntry, len(tm.sites))
	for i := range tm.sites {
		sites[i] = SiteStatusEntry{
			Name:  tm.sites[i].name,
			Role:  string(tm.sites[i].role),
			State: tm.sites[i].state.String(),
		}
		if tm.recoveryPendingSite == tm.sites[i].name {
			sites[i].RecoveryState = tm.recoveryStateLocked()
			sites[i].DivergentGtid = tm.recoveryDivergentGtid
			if tm.recoveryDivergentCount > 0 {
				c := tm.recoveryDivergentCount
				sites[i].DivergentTransactionCount = &c
			}
		}
	}
	return StatusResponse{
		ActiveSite:            tm.activeSiteLocked(),
		Sites:                 sites,
		PollTime:              tm.lastPollTime.Format(time.RFC3339),
		PromotionGtidExecuted: tm.promotionGtidExecuted,
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

// adaptivePollInterval returns the next poll interval based on the
// worst-case failure count across all sites. When every site is healthy
// the base interval is used; when any site is failing, the interval
// increases exponentially up to base * 2^maxPollBackoffExponent (capped
// at 30s).
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

// Poll executes a single poll cycle: checks every site, applies
// debounce, evaluates transitions, and triggers any necessary actions.
func (tm *TopologyManager) Poll(ctx context.Context) {
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

	// Poll every site in parallel.
	results := make([]pollResult, len(tm.sites))
	var wg sync.WaitGroup
	wg.Add(len(tm.sites))
	for i := range tm.sites {
		i := i
		go func() {
			defer wg.Done()
			results[i] = pollSite(&tm.sites[i])
		}()
	}
	wg.Wait()

	for i := range tm.sites {
		metrics.PollLatency.WithLabelValues(tm.sites[i].name).Observe(results[i].duration.Seconds())
	}

	now := tm.clock.Now()

	// Compute new debounced states.
	newStates := make([]state.SiteState, len(tm.sites))
	for i := range tm.sites {
		newStates[i] = tm.computeState(&tm.sites[i], results[i].readOnly, results[i].err)
	}

	// Update lastSeen and state under the lock.
	tm.mu.Lock()
	prevStates := make([]state.SiteState, len(tm.sites))
	for i := range tm.sites {
		if results[i].err == nil {
			tm.sites[i].lastSeen = now
		}
		prevStates[i] = tm.sites[i].state
		if newStates[i] != prevStates[i] {
			tm.sites[i].state = newStates[i]
			// Leaving StateReadOnly invalidates any prior replication-health signal.
			if newStates[i] != state.StateReadOnly {
				tm.sites[i].replicating = false
				tm.sites[i].replicatingStreak = 0
			}
		}
	}
	tm.mu.Unlock()

	// Apply per-site transitions (outside the lock).
	anyTransition := false
	for i := range tm.sites {
		if newStates[i] != prevStates[i] {
			anyTransition = true
			tm.logger.Info("state transition", "site", tm.sites[i].name, "from", prevStates[i], "to", newStates[i])
			metrics.StateTransitions.WithLabelValues(tm.sites[i].name, prevStates[i].String(), newStates[i].String()).Inc()
			tm.applyPerSiteAction(ctx, &tm.sites[i], state.EvalPerSiteTransition(prevStates[i], newStates[i]))
		}
	}

	// Check for pending promotion confirmation: DNS was already flipped at
	// failover trigger time; this block confirms the promoted site is writable
	// and clears the guard flag to allow future failovers.
	if tm.promotedSite != "" {
		siteName := tm.promotedSite
		site := tm.getSite(siteName)
		if site != nil && site.state == state.StateWritable {
			tm.logger.Info("promotion confirmed: site is writable", "site", site.name)
			tm.promotedSite = ""
		}
	}

	// Cross-site evaluation (only on state transitions to avoid repeated actions).
	var alertMsg, degradedReason string
	if anyTransition {
		observations := tm.observations()
		action := state.EvalCrossSite(observations, tm.cfg.SitePriorities)
		alertMsg = action.Alert
		degradedReason = action.Reason
		tm.applyCrossSiteAction(ctx, action)
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

	// Check replication status on every read-only site.
	siteRepl := make([]*mysql.ReplicaStatus, len(tm.sites))
	const replicatingStreakThreshold = 2
	for i := range tm.sites {
		if tm.sites[i].state != state.StateReadOnly {
			continue
		}
		replCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
		rs, err := tm.sites[i].mysql.ShowReplicaStatus(replCtx)
		cancel()
		if err != nil {
			tm.logger.Warn("failed to check replica status", "site", tm.sites[i].name, "error", err)
			tm.mu.Lock()
			tm.sites[i].replicating = false
			tm.sites[i].replicatingStreak = 0
			tm.mu.Unlock()
			continue
		}
		siteRepl[i] = rs
		healthy := rs != nil && rs.IORunning && rs.SQLRunning && rs.SourceHost != ""
		tm.mu.Lock()
		if healthy {
			tm.sites[i].replicatingStreak++
			if tm.sites[i].replicatingStreak >= replicatingStreakThreshold {
				tm.sites[i].replicating = true
			}
		} else {
			tm.sites[i].replicating = false
			tm.sites[i].replicatingStreak = 0
		}
		tm.mu.Unlock()
	}

	// Check if old primary recovery is needed.
	recoveryChanged := tm.checkRecovery(ctx, siteRepl)

	// Process pending reclone annotation.
	recloneStarted := tm.checkReclone(ctx)

	// Check for ordered rolling update trigger.
	updateStarted := tm.checkUpdate(ctx)

	// Auto-clone any empty site even outside of split-brain (e.g. the
	// sidecar fenced the empty site to read-only after a PVC wipe).
	autoCloneStarted := false
	if !recloneStarted && !tm.isBootstrapping() && !tm.isTopologyFrozen() && tm.bootstrap != nil && tm.bootstrapCfg.ReplUser != "" {
		tm.mu.RLock()
		suppressed := tm.autoBootstrapSuppressed
		phase := tm.bootstrapPhase
		tm.mu.RUnlock()
		if !suppressed && (phase == BootstrapPhaseNone || phase == BootstrapPhaseFailed) {
			if donor, empty := tm.detectEmptySite(ctx); donor != "" && empty != "" {
				tm.startBootstrapByName(ctx, donor, empty, "auto-clone")
				autoCloneStarted = true
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

	// Notify the status callback on any state change, recovery event, or update event.
	if (anyTransition || recoveryChanged || recloneStarted || autoCloneStarted || updateStarted) && tm.StatusCallback != nil {
		tm.StatusCallback(tm.buildSnapshot(siteRepl, alertMsg, degradedReason))
	}

	// Broadcast full topology to WebSocket clients on every poll cycle.
	tm.broadcastTopology(siteRepl, alertMsg)
}

// observations returns a SiteObservation for every site. Caller must
// hold no locks (we acquire RLock here).
func (tm *TopologyManager) observations() []state.SiteObservation {
	tm.mu.RLock()
	defer tm.mu.RUnlock()
	out := make([]state.SiteObservation, len(tm.sites))
	for i := range tm.sites {
		out[i] = tm.sites[i].observation()
	}
	return out
}

// buildSnapshot produces a TopologySnapshot from current tracker state.
// Callers must provide the most recent siteRepl values and any alert
// computed this cycle.
func (tm *TopologyManager) buildSnapshot(siteRepl []*mysql.ReplicaStatus, alert, degradedReason string) TopologySnapshot {
	tm.mu.RLock()
	defer tm.mu.RUnlock()

	sites := make([]SiteSnapshot, len(tm.sites))
	for i := range tm.sites {
		var repl *mysql.ReplicaStatus
		if i < len(siteRepl) {
			repl = siteRepl[i]
		}
		sites[i] = SiteSnapshot{
			Name:        tm.sites[i].name,
			Role:        tm.sites[i].role,
			State:       tm.sites[i].state,
			LastSeen:    tm.sites[i].lastSeen,
			Replication: repl,
		}
	}

	var updatePhase string
	if tm.updater != nil {
		updatePhase = string(tm.updater.Phase())
	}

	snap := TopologySnapshot{
		Sites:                 sites,
		ActiveSite:            tm.activeSiteLocked(),
		LastFailover:          tm.lastFailover,
		LastFailoverTarget:    tm.lastFailoverTarget,
		Alert:                 alert,
		DegradedReason:        degradedReason,
		UpdatePhase:           updatePhase,
		BootstrapPhase:        string(tm.bootstrapPhase),
		BootstrapSource:       tm.bootstrapSource,
		PromotionGtidExecuted: tm.promotionGtidExecuted,
		RecoverySite:          tm.recoveryPendingSite,
		RecoveryState:         tm.recoveryStateLocked(),
		DivergentGtid:         tm.recoveryDivergentGtid,
		DivergentTxnCount:     tm.recoveryDivergentCount,
	}
	if tm.bootstrapErr != nil {
		snap.BootstrapError = tm.bootstrapErr.Error()
	}
	return snap
}

// broadcastTopology builds a full TopologyMessage from the current locked
// state and pushes it to all WebSocket clients. Called at the end of every
// poll cycle so dashboards get live updates (not just on transitions).
func (tm *TopologyManager) broadcastTopology(siteRepl []*mysql.ReplicaStatus, alertMsg string) {
	tm.mu.RLock()
	defer tm.mu.RUnlock()

	sites := make([]platform.TopologySite, 0, len(tm.sites))
	for i := range tm.sites {
		var repl *mysql.ReplicaStatus
		if i < len(siteRepl) {
			repl = siteRepl[i]
		}
		s := platform.TopologySite{
			Name:  tm.sites[i].name,
			Role:  string(tm.sites[i].role),
			State: tm.sites[i].state.String(),
		}
		if !tm.sites[i].lastSeen.IsZero() {
			s.LastSeen = tm.sites[i].lastSeen.Format(time.RFC3339)
		}
		if repl != nil {
			s.Replicating = repl.IORunning && repl.SQLRunning
			s.SecondsBehindSource = repl.SecondsBehindSource
			s.GtidExecuted = repl.ExecutedGtidSet
		}
		if tm.recoveryPendingSite == tm.sites[i].name {
			s.RecoveryState = tm.recoveryStateLocked()
			s.DivergentGtid = tm.recoveryDivergentGtid
			if tm.recoveryDivergentCount > 0 {
				c := tm.recoveryDivergentCount
				s.DivergentTransactionCount = &c
			}
		}
		sites = append(sites, s)
	}

	msg := platform.TopologyMessage{
		Namespace:             tm.cfg.Namespace,
		Group:                 tm.cfg.Name,
		ActiveSite:            tm.activeSiteLocked(),
		Sites:                 sites,
		LastFailoverTarget:    tm.lastFailoverTarget,
		Alert:                 alertMsg,
		PollTime:              tm.lastPollTime.Format(time.RFC3339),
		PromotionGtidExecuted: tm.promotionGtidExecuted,
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
	return site.taintSelector
}

func (tm *TopologyManager) applyPerSiteAction(ctx context.Context, site *siteTracker, action state.Action) {
	if action.Taint != nil {
		selector := tm.taintSelector(site)
		if err := tm.tainter.SetTaint(ctx, selector, tm.cfg.Name, *action.Taint); err != nil {
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

	// In-place restore is mutating the cluster right now; every
	// cross-site decision (promotion, fencing a "returning" primary,
	// auto-cloning an empty peer) would actively fight the restore.
	// Defer until the in-place restore reconciler clears the flag.
	if tm.isTopologyFrozen() {
		tm.logger.Info("cross-site action deferred: in-place restore in progress")
		return
	}

	// A planned failover is driving its own fence/promote sequence
	// from the reconciler. The topology manager must not race it by
	// fencing the source itself, auto-promoting a different candidate,
	// or resolving the transient split-brain window as if it were a
	// real one.
	if tm.isPlannedFailoverActive() {
		tm.logger.Info("cross-site action deferred: planned failover in progress")
		return
	}

	// Suppress any cross-site actions while a bootstrap or ordered update is in
	// progress: during an update the standby is restarting and will appear
	// unreachable, and we do not want to initiate a spurious failover.
	if tm.isBootstrapping() || tm.isUpdating() {
		tm.mu.RLock()
		phase := tm.bootstrapPhase
		tm.mu.RUnlock()
		tm.logger.Info("cross-site action suppressed during bootstrap", "phase", phase)
		return
	}

	// Detect split-brain requiring auto-bootstrap: one or more sites are
	// empty (post-PVC-wipe or new site) or every site is empty (fresh deploy).
	if action.SplitBrain && tm.bootstrap != nil && tm.bootstrapCfg.ReplUser != "" {
		tm.mu.RLock()
		phase := tm.bootstrapPhase
		suppressed := tm.autoBootstrapSuppressed
		tm.mu.RUnlock()
		if suppressed {
			tm.logger.Info("auto-bootstrap suppressed (initFromBackup restore in flight)")
		} else if phase == BootstrapPhaseNone || phase == BootstrapPhaseFailed {
			if donor, empty := tm.detectEmptySite(ctx); donor != "" && empty != "" {
				tm.startBootstrapByName(ctx, donor, empty, "auto-clone")
				return
			}
			// A recorded lastFailoverTarget means a failover has already
			// happened in this cluster's lifetime, so the split-brain state
			// here is "old primary respawned writable after failover" —
			// handled by the fence-returning-old-primary branch below, not
			// a fresh deploy. Without this guard the fresh-deploy bootstrap
			// reverts the failover by re-cloning the promoted site back to
			// the respawned old primary.
			if tm.lastFailoverTarget == "" && tm.isFreshDeploy(ctx) {
				tm.startBootstrap(ctx)
				return
			}
		}
	}

	// If any site is writable after a prior failover and this is not a
	// fresh deploy, the old primary(s) may have returned. Fence every
	// writable site except the current failover target so they stop
	// accepting writes; recovery proceeds in checkRecovery for each
	// fenced site once it transitions to read-only.
	if action.SplitBrain && tm.lastFailoverTarget != "" && !tm.isBootstrapping() {
		for i := range tm.sites {
			if tm.sites[i].name == tm.lastFailoverTarget {
				continue
			}
			if tm.sites[i].state != state.StateWritable {
				continue
			}
			tm.logger.Info("fencing returning old primary (split brain after failover)", "site", tm.sites[i].name)
			if err := tm.sites[i].mysql.SetSuperReadOnly(ctx, true); err != nil {
				tm.logger.Error("failed to fence returning old primary", "site", tm.sites[i].name, "error", err)
			}
		}
	}

	// Resolve a concrete promotion target. The state machine emits an
	// ordered candidate list; we pick the freshest one by GTID (most-
	// caught-up replica wins). Priority order is only a tiebreaker.
	//
	// Split-brain with no prior failover target (fresh deploy past
	// bootstrap, or operator restart that lost in-memory state) takes
	// a separate path: when the user configured
	// spec.splitBrainPolicy.sitePriorities we fence every non-winning
	// writable site and synthesize a promotion of the policy winner.
	// GTID freshness is intentionally not consulted here — split-brain
	// winner selection is policy-driven because every writable side
	// may carry unique writes, and the operator's designated
	// authority is what matters.
	promote := ""
	if action.SplitBrain && tm.lastFailoverTarget == "" && !tm.isBootstrapping() && len(tm.cfg.SitePriorities) > 0 {
		writable := tm.writableObservations()
		winner, losers := state.ResolveSplitBrain(writable, tm.cfg.SitePriorities)
		if winner != "" {
			for _, loser := range losers {
				if site := tm.getSite(loser); site != nil && site.state == state.StateWritable {
					tm.logger.Warn("split-brain auto-resolve: fencing non-preferred site per spec.splitBrainPolicy.sitePriorities",
						"winner", winner, "fencedSite", loser)
					if err := site.mysql.SetSuperReadOnly(ctx, true); err != nil {
						tm.logger.Error("failed to fence non-preferred site", "site", loser, "error", err)
					} else {
						metrics.SplitBrainAutoResolveTotal.WithLabelValues(winner).Inc()
					}
				}
			}
			promote = winner
		}
	}
	if promote == "" && len(action.PromotionCandidates) > 0 {
		promote = tm.pickFreshestCandidate(ctx, action.PromotionCandidates)
		if promote == "" {
			tm.logger.Warn("promotion candidates present but GTID picker returned no winner — no reachable candidate?",
				"candidates", action.PromotionCandidates)
		}
	}

	if promote != "" && tm.promotedSite == "" {
		if !tm.lastFailover.IsZero() && tm.clock.Since(tm.lastFailover) < tm.failoverCooldown {
			tm.logger.Info("failover blocked by anti-flap cooldown",
				"lastFailover", tm.lastFailover, "cooldown", tm.failoverCooldown)
			return
		}

		candidate := tm.getSite(promote)
		if candidate == nil {
			tm.logger.Error("promotion requested for unknown site", "site", promote)
			return
		}
		if !candidate.isPromotable() {
			tm.logger.Error("promotion target is not a primary-candidate — refusing",
				"site", candidate.name, "role", candidate.role)
			return
		}

		// Pick an old primary for fencing: prefer the last failover
		// target if it is still known; otherwise fence any site that
		// recently looked writable. For N sites we simply call
		// Execute with no old-primary checker when one isn't known —
		// fencing is best-effort by design.
		var oldPrimaryChecker mysql.Checker
		oldPrimaryName := tm.previousPrimary(candidate.name)
		if oldPrimaryName != "" {
			if site := tm.getSite(oldPrimaryName); site != nil {
				oldPrimaryChecker = site.mysql
			}
		}

		tm.logger.Info("initiating failover", "candidate", candidate.name, "oldPrimary", oldPrimaryName)

		// DNS flip FIRST: start propagation now so it overlaps with
		// the relay-log drain and MySQL promotion steps.
		if err := tm.dns.UpdateDNSRecord(ctx, candidate.lbIP); err != nil {
			tm.logger.Error("DNS flip failed", "site", candidate.name, "error", err)
		} else {
			metrics.DNSFlipCount.WithLabelValues(candidate.name).Inc()
		}

		promotionGtid, err := tm.failover.Execute(ctx, candidate.mysql, oldPrimaryChecker, candidate.name)
		if err != nil {
			tm.logger.Error("failover failed", "error", err)
			return
		}
		metrics.FailoversTotal.WithLabelValues(candidate.name).Inc()
		tm.promotionGtidExecuted = promotionGtid
		tm.promotedSite = candidate.name
		tm.lastFailover = tm.clock.Now()
		tm.lastFailoverTarget = candidate.name

		// Best-effort: ask the Dragonfly subsystem (when wired) to
		// follow the MySQL promotion. The callback enforces its own
		// budget and never blocks the topology poll.
		if tm.EmergencyFailoverCallback != nil {
			tm.EmergencyFailoverCallback(ctx, candidate.name, oldPrimaryName)
		}
	}
}

// previousPrimary returns the name of the most likely "old primary" —
// the last failover target if it is different from newPrimary, or
// otherwise the first writable site other than newPrimary. Returns ""
// if there is no plausible old primary to fence.
func (tm *TopologyManager) previousPrimary(newPrimary string) string {
	if tm.lastFailoverTarget != "" && tm.lastFailoverTarget != newPrimary {
		return tm.lastFailoverTarget
	}
	tm.mu.RLock()
	defer tm.mu.RUnlock()
	for i := range tm.sites {
		if tm.sites[i].name == newPrimary {
			continue
		}
		if tm.sites[i].state == state.StateWritable {
			return tm.sites[i].name
		}
	}
	return ""
}

// writableObservations returns observations for every currently writable site.
func (tm *TopologyManager) writableObservations() []state.SiteObservation {
	tm.mu.RLock()
	defer tm.mu.RUnlock()
	var out []state.SiteObservation
	for i := range tm.sites {
		if tm.sites[i].state == state.StateWritable {
			out = append(out, tm.sites[i].observation())
		}
	}
	return out
}

// pickFreshestCandidate queries GTID_EXECUTED from every candidate in
// parallel and returns the name of the replica with the most-caught-up
// set. GTID freshness is the primary selector (minimise data loss on
// promotion); ties or incomparable sets fall back to candidate order,
// which the caller has already sorted by priority list + declared
// order. Unreachable candidates are skipped; returns "" when no
// candidate can be reached or parsed.
func (tm *TopologyManager) pickFreshestCandidate(ctx context.Context, candidates []string) string {
	type candidateInfo struct {
		pos  int
		name string
		gtid mysql.GTIDSet
		ok   bool
	}

	infos := make([]candidateInfo, len(candidates))
	var wg sync.WaitGroup
	for i, name := range candidates {
		site := tm.getSite(name)
		if site == nil {
			infos[i] = candidateInfo{pos: i, name: name}
			continue
		}
		infos[i] = candidateInfo{pos: i, name: name}
		checker := site.mysql
		i := i
		wg.Add(1)
		go func() {
			defer wg.Done()
			queryCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
			defer cancel()
			raw, err := checker.GetGtidExecuted(queryCtx)
			if err != nil {
				tm.logger.Warn("failover picker: failed to query GTID", "site", infos[i].name, "error", err)
				return
			}
			parsed, err := mysql.ParseGTIDSet(raw)
			if err != nil {
				tm.logger.Warn("failover picker: failed to parse GTID", "site", infos[i].name, "error", err)
				return
			}
			infos[i].gtid = parsed
			infos[i].ok = true
		}()
	}
	wg.Wait()

	best := -1
	for i := range infos {
		if !infos[i].ok {
			continue
		}
		if best < 0 {
			best = i
			continue
		}
		// A strictly-newer GTID set wins over the current best;
		// otherwise keep the earlier (higher-priority) candidate.
		if infos[i].gtid.Contains(infos[best].gtid) && !infos[best].gtid.Contains(infos[i].gtid) {
			best = i
		}
	}
	if best < 0 {
		return ""
	}
	tm.logger.Info("failover picker: selected promotion target by GTID freshness",
		"site", infos[best].name, "gtid", infos[best].gtid.String(), "candidates", candidates)
	return infos[best].name
}

// SetLastFailoverForTest allows tests to manipulate the cooldown timer
// without sleeping. This should only be used in tests.
func (tm *TopologyManager) SetLastFailoverForTest(t time.Time) {
	tm.lastFailover = t
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

// SetTopologyFrozen toggles the gate that blocks every cross-site
// action (promotion, reclone, auto-bootstrap, returning-old-primary
// fencing) while an in-place restore is actively mutating the cluster.
// The runner sets this from the CR's status.restoreInPlace.phase.
//
// This is a stronger signal than SetAutoBootstrapSuppressed: polling
// and metrics emission continue normally so the operator still has
// visibility into site health, but decisions that would reshape the
// cluster are deferred until the in-place restore reconciler clears
// the flag.
func (tm *TopologyManager) SetTopologyFrozen(v bool) {
	tm.mu.Lock()
	tm.topologyFrozen = v
	tm.mu.Unlock()
}

// isTopologyFrozen reports whether an in-place restore is currently
// holding the topology manager still.
func (tm *TopologyManager) isTopologyFrozen() bool {
	tm.mu.RLock()
	defer tm.mu.RUnlock()
	return tm.topologyFrozen
}

// SetPlannedFailoverActive toggles the guard that blocks automatic
// cross-site action while a reconciler-driven planned failover is in
// flight. The runner sets it from the CR's status.plannedFailover.phase.
func (tm *TopologyManager) SetPlannedFailoverActive(v bool) {
	tm.mu.Lock()
	tm.plannedFailoverActive = v
	tm.mu.Unlock()
}

// isPlannedFailoverActive reports whether a planned failover is
// currently running against this CR.
func (tm *TopologyManager) isPlannedFailoverActive() bool {
	tm.mu.RLock()
	defer tm.mu.RUnlock()
	return tm.plannedFailoverActive
}

// FenceSite sets super_read_only=ON on the named site and returns the
// site's GTID_EXECUTED captured after the fence takes effect. Returns
// errSiteNotFound when the site name is unknown.
//
// Transactional: if the GTID read fails after the fence succeeded, the
// site is best-effort unfenced before the error is returned. Leaving a
// healthy primary stuck in super_read_only on a transient metadata
// read is worse than failing the whole operation — the caller can
// retry and the site stays writable in the meantime.
func (tm *TopologyManager) FenceSite(ctx context.Context, name string) (string, error) {
	site := tm.getSite(name)
	if site == nil {
		return "", errSiteNotFound
	}
	if err := site.mysql.SetSuperReadOnly(ctx, true); err != nil {
		return "", fmt.Errorf("set super_read_only=ON on %q: %w", name, err)
	}
	gtid, err := site.mysql.GetGtidExecuted(ctx)
	if err != nil {
		// Best-effort unfence using a fresh short-lived context so a
		// cancelled parent ctx doesn't block the rollback attempt.
		unfenceCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		if unfenceErr := site.mysql.SetSuperReadOnly(unfenceCtx, false); unfenceErr != nil {
			tm.logger.Error("fence rollback failed: site left super_read_only=ON", "site", name, "error", unfenceErr)
		}
		cancel()
		return "", fmt.Errorf("read GTID_EXECUTED on %q after fence: %w", name, err)
	}
	return gtid, nil
}

// UnfenceSite clears super_read_only on the named site. Used by the
// planned-failover rollback path when the target fails to catch up.
func (tm *TopologyManager) UnfenceSite(ctx context.Context, name string) error {
	site := tm.getSite(name)
	if site == nil {
		return errSiteNotFound
	}
	if err := site.mysql.SetSuperReadOnly(ctx, false); err != nil {
		return fmt.Errorf("set super_read_only=OFF on %q: %w", name, err)
	}
	return nil
}

// GetSiteGtidExecuted returns the named site's current GTID_EXECUTED.
func (tm *TopologyManager) GetSiteGtidExecuted(ctx context.Context, name string) (string, error) {
	site := tm.getSite(name)
	if site == nil {
		return "", errSiteNotFound
	}
	gtid, err := site.mysql.GetGtidExecuted(ctx)
	if err != nil {
		return "", fmt.Errorf("read GTID_EXECUTED on %q: %w", name, err)
	}
	return gtid, nil
}

// KillSiteAppConnections kills non-replication application connections
// on the named site and returns the count killed. Used by the
// planned-failover Draining loop to drain in-flight transactions.
func (tm *TopologyManager) KillSiteAppConnections(ctx context.Context, name string) (int, error) {
	site := tm.getSite(name)
	if site == nil {
		return 0, errSiteNotFound
	}
	killed, err := site.mysql.KillAppConnections(ctx)
	if err != nil {
		return 0, fmt.Errorf("kill app connections on %q: %w", name, err)
	}
	return killed, nil
}

// PlannedPromote runs FailoverController.Execute against the target,
// flips DNS to the target's LB IP, and updates the in-memory
// lastFailover/lastFailoverTarget fields so the anti-flap cooldown
// applies to any follow-on emergency or planned failover. Callers must
// have already set plannedFailoverActive to true and completed the
// zero-lag gate.
//
// Returns the promotion GTID recorded immediately before the target
// accepted writes. That value matches what the automatic path records
// into status.promotionGtidExecuted.
func (tm *TopologyManager) PlannedPromote(ctx context.Context, target, source string) (string, error) {
	targetSite := tm.getSite(target)
	if targetSite == nil {
		return "", errSiteNotFound
	}
	if !targetSite.isPromotable() {
		return "", fmt.Errorf("planned promote: site %q has role %q; only primary-candidate sites may be promoted", target, targetSite.role)
	}

	var sourceChecker mysql.Checker
	if source != "" {
		if src := tm.getSite(source); src != nil {
			sourceChecker = src.mysql
		}
	}

	// DNS flip first, mirroring applyCrossSiteAction: start propagation
	// now so it overlaps with the MySQL steps.
	if err := tm.dns.UpdateDNSRecord(ctx, targetSite.lbIP); err != nil {
		tm.logger.Error("planned-failover DNS flip failed", "site", target, "error", err)
	} else {
		metrics.DNSFlipCount.WithLabelValues(target).Inc()
	}

	promotionGtid, err := tm.failover.Execute(ctx, targetSite.mysql, sourceChecker, target)
	if err != nil {
		return "", err
	}

	tm.mu.Lock()
	tm.promotionGtidExecuted = promotionGtid
	tm.promotedSite = target
	tm.lastFailover = tm.clock.Now()
	tm.lastFailoverTarget = target
	tm.mu.Unlock()

	return promotionGtid, nil
}

// errSiteNotFound is returned by the planned-failover primitives when a
// name does not match any configured site. Sentinel so callers can
// decide whether to retry (transient lookup failure is impossible — the
// topology manager only forgets sites when it restarts).
var errSiteNotFound = fmt.Errorf("planned-failover: site not found in topology manager")

// SetRecloneSite requests that the given site be recloned from the current
// primary. Called by the runner when it detects the reclone annotation.
// The topology manager processes this during the next poll cycle.
func (tm *TopologyManager) SetRecloneSite(site string) {
	tm.mu.Lock()
	tm.reclonePendingSite = site
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

// isUpdating reports whether an ordered update is currently running.
func (tm *TopologyManager) isUpdating() bool {
	return tm.updater != nil && tm.updater.IsUpdating()
}

// checkUpdate detects spec drift and triggers an ordered rolling
// update. Returns true if an update was started this cycle.
//
// For N sites, the update is ordered: all non-active sites are updated
// one at a time, and the active site is updated last (after a planned
// handoff — currently still pending in the non-active rollout set).
func (tm *TopologyManager) checkUpdate(ctx context.Context) bool {
	if tm.updater == nil || tm.ApplyUpdate == nil {
		return false
	}
	if tm.isUpdating() || tm.isBootstrapping() {
		return false
	}

	// Identify the active site and each healthy standby.
	var activeName string
	var activeChecker mysql.Checker
	var standbyName string
	var standbyChecker mysql.Checker
	tm.mu.RLock()
	for i := range tm.sites {
		switch tm.sites[i].state {
		case state.StateWritable:
			if activeName != "" {
				// Split-brain — let that path recover before touching updates.
				tm.mu.RUnlock()
				return false
			}
			activeName = tm.sites[i].name
			activeChecker = tm.sites[i].mysql
		case state.StateReadOnly:
			if tm.sites[i].isHealthyReplica() && standbyName == "" {
				standbyName = tm.sites[i].name
				standbyChecker = tm.sites[i].mysql
			}
		}
	}
	tm.mu.RUnlock()
	if activeName == "" || standbyName == "" {
		return false
	}

	tm.mu.RLock()
	driftSites := tm.specDriftSites
	tm.mu.RUnlock()
	if len(driftSites) == 0 {
		return false
	}

	tm.logger.Info("ordered update: spec drift detected, starting ordered update",
		"driftSites", driftSites, "active", activeName, "standby", standbyName)

	applyUpdate := tm.ApplyUpdate
	go func() {
		if err := tm.updater.Execute(ctx, activeName, standbyName, standbyChecker, activeChecker, applyUpdate); err != nil {
			tm.logger.Error("ordered update failed", "error", err)
		}
		// Clear drift sites after update completes (success or failure).
		tm.mu.Lock()
		tm.specDriftSites = nil
		tm.mu.Unlock()
		// Trigger a status callback to clear the Updating condition.
		if tm.StatusCallback != nil {
			tm.emitStatusSnapshot()
		}
	}()

	return true
}

// emitStatusSnapshot sends a TopologySnapshot through the status callback
// using current state. Safe to call from any goroutine.
func (tm *TopologyManager) emitStatusSnapshot() {
	if tm.StatusCallback == nil {
		return
	}
	// Collect replication status for read-only sites.
	ctx := context.Background()
	siteRepl := make([]*mysql.ReplicaStatus, len(tm.sites))
	for i := range tm.sites {
		if tm.sites[i].state == state.StateReadOnly {
			rs, err := tm.sites[i].mysql.ShowReplicaStatus(ctx)
			if err == nil {
				siteRepl[i] = rs
			}
		}
	}
	tm.StatusCallback(tm.buildSnapshot(siteRepl, "", ""))
}

// SetSpecDriftSites records which sites have spec drift (Deployment hash != desired hash).
// Called by the runner after detecting drift.
func (tm *TopologyManager) SetSpecDriftSites(sites []string) {
	tm.mu.Lock()
	defer tm.mu.Unlock()
	tm.specDriftSites = sites
}

// isFreshDeploy reports whether every site is writable AND none has
// ever had replication configured. This is the signature of a fresh
// deployment — as opposed to a "true" split-brain where at least one
// site previously had replication set up and may now hold diverged
// writes.
func (tm *TopologyManager) isFreshDeploy(ctx context.Context) bool {
	for i := range tm.sites {
		if tm.sites[i].state != state.StateWritable {
			return false
		}
	}
	for i := range tm.sites {
		rs, err := tm.sites[i].mysql.ShowReplicaStatus(ctx)
		if err != nil {
			tm.logger.Warn("fresh-deploy check: could not read replica status", "site", tm.sites[i].name, "error", err)
			return false
		}
		if rs != nil {
			return false
		}
	}
	return true
}

// detectEmptySite looks for exactly one donor/recipient pair where a
// single site is reachable but completely empty (empty GTID_EXECUTED,
// no replication configured). Works for any number of sites; the
// donor is the writable site with data, and the recipient is any
// reachable empty site. When multiple sites are empty the operator
// clones one per poll cycle. Returns ("", "") when no eligible pair
// exists.
func (tm *TopologyManager) detectEmptySite(ctx context.Context) (donor, empty string) {
	// Every site must be reachable.
	for i := range tm.sites {
		switch tm.sites[i].state {
		case state.StateUnreachable, state.StateUnknown:
			return "", ""
		}
	}

	replStatus := make([]*mysql.ReplicaStatus, len(tm.sites))
	gtidSets := make([]mysql.GTIDSet, len(tm.sites))
	for i := range tm.sites {
		rs, err := tm.sites[i].mysql.ShowReplicaStatus(ctx)
		if err != nil {
			return "", ""
		}
		replStatus[i] = rs

		raw, err := tm.sites[i].mysql.GetGtidExecuted(ctx)
		if err != nil {
			return "", ""
		}
		parsed, err := mysql.ParseGTIDSet(raw)
		if err != nil {
			return "", ""
		}
		gtidSets[i] = parsed
	}

	// Locate the writable site with data (our donor). We only clone
	// from a writable site to avoid copying from a stale replica.
	donorIdx := -1
	for i := range tm.sites {
		if tm.sites[i].state == state.StateWritable && !gtidSets[i].IsEmpty() {
			donorIdx = i
			break
		}
	}
	if donorIdx < 0 {
		return "", ""
	}

	// Locate any empty, reachable non-donor site.
	for i := range tm.sites {
		if i == donorIdx {
			continue
		}
		reachable := tm.sites[i].state == state.StateWritable || tm.sites[i].state == state.StateReadOnly
		if !reachable {
			continue
		}
		if gtidSets[i].IsEmpty() && replStatus[i] == nil {
			return tm.sites[donorIdx].name, tm.sites[i].name
		}
	}
	return "", ""
}

// selectSeedSite determines which site should be seeded as the initial
// primary during fresh-deploy bootstrap. The winner is the highest-
// priority primary-candidate in cfg.SitePriorities, falling back to the
// first primary-candidate in declared order.
func (tm *TopologyManager) selectSeedSite() string {
	for _, name := range tm.cfg.SitePriorities {
		if site := tm.getSite(name); site != nil && site.isPromotable() {
			return site.name
		}
	}
	for i := range tm.sites {
		if tm.sites[i].isPromotable() {
			return tm.sites[i].name
		}
	}
	return ""
}

// startBootstrap kicks off the async bootstrap goroutine for a
// fresh-deploy scenario: the first primary-candidate is chosen as the
// initial primary, and every other site is cloned from it one per poll
// cycle (this function schedules the first clone; subsequent cycles
// pick up additional empty sites via detectEmptySite → auto-clone).
// Caller must hold no locks.
func (tm *TopologyManager) startBootstrap(ctx context.Context) {
	seed := tm.selectSeedSite()
	if seed == "" {
		err := fmt.Errorf("no primary-candidate site available to seed")
		tm.mu.Lock()
		tm.bootstrapPhase = BootstrapPhaseFailed
		tm.bootstrapErr = err
		tm.mu.Unlock()
		tm.emitBootstrapStatus()
		return
	}

	// Pick any non-seed site as the first clone recipient. We prefer
	// primary-candidate sites first so that post-fresh-deploy there is
	// at least one replica ready for failover before dr-only followers
	// are cloned; they will be picked up on subsequent poll cycles.
	recipient := ""
	for i := range tm.sites {
		if tm.sites[i].name == seed {
			continue
		}
		if tm.sites[i].isPromotable() {
			recipient = tm.sites[i].name
			break
		}
	}
	if recipient == "" {
		for i := range tm.sites {
			if tm.sites[i].name != seed {
				recipient = tm.sites[i].name
				break
			}
		}
	}
	if recipient == "" {
		err := fmt.Errorf("no recipient site available for bootstrap")
		tm.mu.Lock()
		tm.bootstrapPhase = BootstrapPhaseFailed
		tm.bootstrapErr = err
		tm.mu.Unlock()
		tm.emitBootstrapStatus()
		return
	}

	tm.startBootstrapByName(ctx, seed, recipient, "fresh-deploy")
}

// startBootstrapByName kicks off the async bootstrap goroutine for the
// named donor/recipient pair. Caller must hold no locks.
func (tm *TopologyManager) startBootstrapByName(ctx context.Context, donor, recipient, source string) {
	donorSite := tm.getSite(donor)
	recipientSite := tm.getSite(recipient)
	if donorSite == nil || recipientSite == nil {
		tm.logger.Error("bootstrap aborted: unknown site", "donor", donor, "recipient", recipient)
		return
	}
	if donorSite.host == "" {
		err := fmt.Errorf("bootstrap: donor host not configured for site %s", donor)
		tm.mu.Lock()
		tm.bootstrapPhase = BootstrapPhaseFailed
		tm.bootstrapErr = err
		tm.mu.Unlock()
		tm.emitBootstrapStatus()
		return
	}

	tm.mu.Lock()
	tm.bootstrapPhase = BootstrapPhaseCloning
	tm.bootstrapErr = nil
	tm.bootstrapSource = source
	tm.mu.Unlock()

	tm.logger.Info("starting bootstrap",
		"source", source,
		"donor", donor,
		"recipient", recipient,
		"donorHost", donorSite.host)

	tm.emitBootstrapStatus()

	go func() {
		err := tm.runBootstrap(ctx, donorSite.mysql, recipientSite.mysql, donorSite.host, recipient)
		tm.mu.Lock()
		if err != nil {
			tm.bootstrapPhase = BootstrapPhaseFailed
			tm.bootstrapErr = err
			tm.logger.Error("bootstrap failed", "source", source, "error", err)
		} else {
			tm.bootstrapPhase = BootstrapPhaseDone
			tm.logger.Info("bootstrap completed successfully", "source", source)
		}
		tm.mu.Unlock()
		tm.emitBootstrapStatus()
	}()
}

// runBootstrap performs the clone, waits for the MySQL restart, and
// sets up replication of recipient from donor.
func (tm *TopologyManager) runBootstrap(ctx context.Context, primary, replica mysql.Checker, primaryHost, replicaSite string) error {
	// Check if the clone already completed (e.g. prior bootstrap succeeded at
	// CLONE but failed at SetupReplication). If the primary's GTID set contains
	// the replica's, the data is already in sync and we can skip directly to
	// replication setup.
	if tm.canSkipClone(ctx, primary, replica) {
		tm.logger.Info("replica already has primary data (prior clone detected), skipping clone phase")
		tm.mu.Lock()
		tm.bootstrapPhase = BootstrapPhaseSetupRepl
		tm.mu.Unlock()
		tm.emitBootstrapStatus()
		return tm.setupReplicationForBootstrap(ctx, replica, primaryHost)
	}

	// Phase 1: Clone from primary.
	err := tm.bootstrap.BootstrapReplica(ctx, BootstrapOpts{
		Primary:      primary,
		Replica:      replica,
		PrimaryHost:  primaryHost,
		ReplicaSite:  replicaSite,
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

// canSkipClone returns true when the replica already contains the primary's
// data (or a superset), indicating a prior CLONE INSTANCE succeeded.
func (tm *TopologyManager) canSkipClone(ctx context.Context, primary, replica mysql.Checker) bool {
	pRaw, err := primary.GetGtidExecuted(ctx)
	if err != nil {
		return false
	}
	rRaw, err := replica.GetGtidExecuted(ctx)
	if err != nil {
		return false
	}
	pGtid, err := mysql.ParseGTIDSet(pRaw)
	if err != nil || pGtid.IsEmpty() {
		return false
	}
	rGtid, err := mysql.ParseGTIDSet(rRaw)
	if err != nil || rGtid.IsEmpty() {
		return false
	}
	return rGtid.Contains(pGtid) || pGtid.Contains(rGtid)
}

// setupReplicationForBootstrap runs only the replication setup phase of bootstrap.
func (tm *TopologyManager) setupReplicationForBootstrap(ctx context.Context, replica mysql.Checker, primaryHost string) error {
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
		// Error 3707: mysqld is not managed by a supervisor process (e.g. Docker
		// without mysqld_safe). The clone data transfer succeeded but the
		// automatic restart failed — Kubernetes will restart the container.
		"Restart server failed",
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

// checkRecovery detects an old primary that has come back after a
// failover and either auto-rejoins it (no divergence) or blocks with
// metadata (divergence detected). For N sites we scan every read-only
// non-active site and pick the first one that needs recovery; multiple
// divergent sites are reported sequentially across poll cycles.
// Returns true if recovery state changed this cycle.
func (tm *TopologyManager) checkRecovery(ctx context.Context, siteRepl []*mysql.ReplicaStatus) bool {
	if tm.lastFailoverTarget == "" || tm.isBootstrapping() {
		return false
	}
	if tm.isTopologyFrozen() {
		return false
	}
	if tm.bootstrapCfg.ReplUser == "" {
		return false
	}

	// Find the active (writable) site; refuse to proceed on split-brain.
	activeIdx := -1
	for i := range tm.sites {
		if tm.sites[i].state == state.StateWritable {
			if activeIdx != -1 {
				return false
			}
			activeIdx = i
		}
	}
	if activeIdx == -1 {
		return false
	}

	// Scan non-active sites for one that looks like an old primary.
	for i := range tm.sites {
		if i == activeIdx {
			continue
		}
		other := &tm.sites[i]
		if other.state != state.StateReadOnly {
			continue
		}
		var repl *mysql.ReplicaStatus
		if i < len(siteRepl) {
			repl = siteRepl[i]
		}
		if repl != nil && (repl.IORunning || repl.SQLRunning) {
			// Already replicating. If this site previously had
			// recovery-blocked state (e.g. admin re-cloned), clear it.
			if tm.recoveryPendingSite == other.name {
				tm.mu.Lock()
				tm.recoveryPendingSite = ""
				tm.recoveryDivergentGtid = ""
				tm.recoveryDivergentCount = 0
				tm.mu.Unlock()
				metrics.DivergentTransactions.WithLabelValues(other.name).Set(0)
				tm.logger.Info("recovery state cleared (site is now replicating)", "site", other.name)
				return true
			}
			continue
		}
		// Already recorded as blocked — nothing to do.
		if tm.recoveryPendingSite == other.name {
			continue
		}
		// Read-only with no active replication after a prior failover: start recovery.
		return tm.initiateRecovery(ctx, i, activeIdx)
	}
	return false
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
	tm.logger.Warn("divergence detected",
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
	newPrimaryHost := tm.sites[newPrimaryIdx].host

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
	tm.logger.Info("old primary recovery complete", "site", oldPrimary.name, "source", newPrimaryHost)
}

// checkReclone processes a pending reclone annotation. If a reclone was
// requested for a specific site, validates preconditions and initiates the
// bootstrap. Returns true if a reclone was started.
func (tm *TopologyManager) checkReclone(ctx context.Context) bool {
	tm.mu.RLock()
	site := tm.reclonePendingSite
	tm.mu.RUnlock()

	if site == "" {
		return false
	}

	if tm.isBootstrapping() {
		tm.logger.Info("reclone request deferred, bootstrap already in progress", "site", site)
		return false
	}

	if tm.isTopologyFrozen() {
		tm.logger.Info("reclone request deferred, in-place restore in progress", "site", site)
		return false
	}

	if tm.bootstrap == nil || tm.bootstrapCfg.ReplUser == "" {
		tm.logger.Error("reclone requested but bootstrap is not configured (missing replication credentials)")
		tm.mu.Lock()
		tm.reclonePendingSite = ""
		tm.mu.Unlock()
		return false
	}

	recipient := tm.getSite(site)
	if recipient == nil {
		tm.logger.Error("reclone requested for unknown site", "site", site)
		tm.mu.Lock()
		tm.reclonePendingSite = ""
		tm.mu.Unlock()
		return false
	}

	var donor *siteTracker
	for i := range tm.sites {
		if tm.sites[i].state == state.StateWritable {
			donor = &tm.sites[i]
			break
		}
	}
	if donor == nil {
		tm.logger.Error("reclone requested but no writable primary found")
		return false
	}
	if donor.name == recipient.name {
		tm.logger.Error("cannot reclone the active primary", "site", site)
		tm.mu.Lock()
		tm.reclonePendingSite = ""
		tm.mu.Unlock()
		return false
	}

	tm.mu.Lock()
	tm.reclonePendingSite = ""
	if tm.recoveryPendingSite == site {
		tm.recoveryPendingSite = ""
		tm.recoveryDivergentGtid = ""
		tm.recoveryDivergentCount = 0
	}
	tm.mu.Unlock()

	metrics.DivergentTransactions.WithLabelValues(site).Set(0)
	metrics.RecloneOperations.WithLabelValues(site).Inc()

	tm.startBootstrapByName(ctx, donor.name, recipient.name, "reclone")
	return true
}

// emitBootstrapStatus notifies the runner that the bootstrap phase changed.
// It uses a dedicated BootstrapStatusCallback so that only the Bootstrapping
// condition is updated on the CR.
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
	source := tm.bootstrapSource
	tm.mu.RUnlock()
	tm.BootstrapStatusCallback(phase, errMsg, source)
}
