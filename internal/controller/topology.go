package controller

import (
	"context"
	"fmt"
	"log/slog"
	"reflect"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"k8s.io/apimachinery/pkg/types"

	"github.com/shipstream/bloodraven/internal/clock"
	"github.com/shipstream/bloodraven/internal/metrics"
	"github.com/shipstream/bloodraven/internal/mysql"
	"github.com/shipstream/bloodraven/internal/platform"
	"github.com/shipstream/bloodraven/internal/state"
	"github.com/shipstream/bloodraven/internal/util"
)

const mysqlSafetyProbeTimeout = 5 * time.Second

func withMySQLSafetyTimeout(ctx context.Context, probe func(context.Context) error) error {
	probeCtx, cancel := context.WithTimeout(ctx, mysqlSafetyProbeTimeout)
	defer cancel()
	return probe(probeCtx)
}

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

	PollInterval           int64 // nanoseconds
	FailureThreshold       int
	RecoveryThreshold      int
	FailoverCooldown       int64 // nanoseconds, default 5m
	ConnectionDrainTimeout int64 // nanoseconds, default 30s
	MaxLagSeconds          int64
	ReadOnlyMaxLagSeconds  int64

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

const pendingPromotionActiveSiteTTL = 30 * time.Second

func bootstrapIdlePhase(phase BootstrapPhase) bool {
	return phase == BootstrapPhaseNone || phase == BootstrapPhaseDone || phase == BootstrapPhaseFailed
}

// PollIntervalDuration returns the poll interval as a time.Duration.
func (c TopologyConfig) PollIntervalDuration() time.Duration {
	return time.Duration(c.PollInterval)
}

// SiteSnapshot captures one site's observation at a point in time.
type SiteSnapshot struct {
	Name                    string
	Role                    state.SiteRole
	State                   state.SiteState
	LastSeen                time.Time
	Replication             *mysql.ReplicaStatus // nil if site is primary or unreachable
	SourceHost              string
	SourceConvergenceState  SourceConvergenceState
	SourceConvergenceReason string
	ServingHealthy          bool
	ReplicationHealthy      bool

	// Per-site old-primary recovery state ("", RecoveryInProgress, or
	// RecoveryBlocked) with the divergence report when blocked. Multiple
	// sites can carry recovery state at once.
	RecoveryState     string
	DivergentGtid     string
	DivergentTxnCount int64
}

// SourceConvergenceState is the topology manager's internal representation of
// the persisted per-follower source state.
type SourceConvergenceState string

const (
	sourceConvergenceConverged SourceConvergenceState = "Converged"
	sourceConvergencePending   SourceConvergenceState = "Pending"
	sourceConvergenceBlocked   SourceConvergenceState = "Blocked"

	sourceReasonDirectSource   = "DirectSource"
	sourceReasonSourceMismatch = "SourceMismatch"
	sourceReasonProbeFailed    = "ProbeFailed"
	sourceReasonMutationFailed = "MutationFailed"
	sourceReasonGTIDDiverged   = "GTIDDiverged"
)

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

	// RecoverySite/RecoveryState/DivergentGtid/DivergentTxnCount summarize
	// one site with active recovery state for the RecoveryPending condition
	// message; a RecoveryBlocked site wins over a RecoveryInProgress one
	// regardless of declared order, so the condition cannot hide divergence
	// behind a routine rejoin. Events are emitted per site and do NOT read
	// these fields. The authoritative per-site recovery data lives in
	// Sites[].RecoveryState/DivergentGtid/DivergentTxnCount — consult those
	// when more than one site can be in recovery at once.
	RecoverySite      string
	RecoveryState     string // "", "RecoveryInProgress", or "RecoveryBlocked"
	DivergentGtid     string
	DivergentTxnCount int64

	// KeyringPromotionSkipped / KeyringPromotionRefused name sites the
	// last applyCrossSiteAction decision skipped or refused because they
	// were mid-keyring-rotation. Empty when that cycle did not reach a
	// promotion decision. Used by the runner to emit Events; not persisted
	// on the CR.
	KeyringPromotionSkipped []string
	KeyringPromotionRefused []string
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
	replicating             bool
	replicatingStreak       int
	sourceHost              string
	sourceConvergenceState  SourceConvergenceState
	sourceConvergenceReason string
	servingHealthy          bool
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

// siteMetricLabels returns the identity labels for site-scoped gauges.
func (tm *TopologyManager) siteMetricLabels(site *siteTracker) (namespace, group, name, role string) {
	return tm.cfg.Namespace, tm.cfg.Name, site.name, string(site.role)
}

// HaltSiteMetrics prevents further site-gauge emission from this manager.
// The runner calls this before waiting for Run to exit so a timed-out
// join can still drop series without a late Poll rewriting them.
func (tm *TopologyManager) HaltSiteMetrics() {
	if tm == nil {
		return
	}
	tm.metricsStopped.Store(true)
}

func (tm *TopologyManager) siteMetricsHalted() bool {
	return tm != nil && tm.metricsStopped.Load()
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
	// lastFailoverLocal distinguishes a promotion observed by this process
	// from a restored durable timestamp. A real local promotion is
	// authoritative even after a backward clock step or a skewed restored
	// record; after that, timestamp ordering protects concurrent local
	// promotions from applying out of order. Protected by mu once running.
	lastFailoverLocal bool
	failoverCooldown  time.Duration

	// failoverState persists lastFailover/lastFailoverTarget out of band
	// from the CR status subresource, so a status-write outage that spans
	// an operator restart cannot reset the anti-flap cooldown. Nil in tests
	// and wherever no store is wired; every call site is nil-safe.
	//
	// desiredFailoverState is the newest record any promotion has produced,
	// and failoverStateDirty says it may not be in the store yet. While
	// dirty, Poll retries every cycle, mirroring the statusWriteFailed
	// retry — the two durable paths fail for different reasons and each has
	// to heal on its own. Both protected by mu.
	//
	// Promotions are not all issued from the poll goroutine: the
	// ordered-update handoff records one from its own goroutine while Poll
	// keeps running. Carrying a single newest-record slot rather than a
	// queue of pending writes is what makes that safe — a flush always
	// writes the newest record it can see, so a slow write can never land
	// an older record over a newer one.
	failoverState        FailoverStateRecorder
	desiredFailoverState *FailoverRecord
	failoverStateDirty   bool

	// failoverStateWriteMu serializes out-of-band writes so two concurrent
	// flushes cannot reach the API server out of order. Never held with mu.
	failoverStateWriteMu sync.Mutex

	// Promotion state: tracks which site was promoted and is pending DNS flip.
	promotedSite          string // empty = no pending promotion
	promotedAt            time.Time
	promotionGtidExecuted string // GTID set at last promotion

	// authorityEpoch advances whenever a promotion completes outside or
	// inside the poll goroutine. Poll captures it before probing MySQL and
	// discards observations from an older epoch, preventing a concurrent
	// ordered-update handoff from being overwritten by pre-promotion reads.
	// Protected by mu.
	authorityEpoch uint64

	// DNS steering state. reconcileDNS drives the record to the CURRENT
	// active site on every poll; these two fields only memoize what this
	// process knows about the record so a converged cluster does not
	// re-write it every tick. They are deliberately NOT a "pending target"
	// to re-apply later: a memoized target can be superseded by a planned
	// promote or a re-assert and would then replay a stale (read-only) site.
	// The desired target is always re-derived from live topology instead —
	// which is also what makes the heal survive an operator restart, since
	// the live record is read back via platform.DNSRecordReader.
	//
	//   dnsAppliedTarget — last target this process is known to have applied
	//                      ("" = unknown, e.g. right after a restart).
	//   dnsWriteFailed   — a DNS write was attempted and rejected, and none
	//                      has succeeded since; forces a re-apply attempt on
	//                      every poll even when the record cannot be read.
	//
	// Protected by mu.
	dnsAppliedTarget string
	dnsWriteFailed   bool

	// statusWriteFailed records that the most recent CR /status write was
	// rejected (e.g. mysqlfailovergroups/status patch+update RBAC-denied
	// during a failover). While set, Poll re-fires StatusCallback every
	// cycle — even with no fresh state transition — so status catches up
	// once the write is permitted again. updateCRStatus already
	// DeepEqual-skips no-op writes, so this retry only actually writes while
	// the live CR diverges from the desired snapshot. Protected by mu.
	statusWriteFailed   bool
	statusRetrySnapshot *TopologySnapshot

	// Recovery state for old primaries after failover, per site. More than
	// one site can need recovery at once (e.g. two former primaries after
	// consecutive failovers in an N-site group), and each carries its own
	// divergence report — a single slot would let one site's blocked report
	// overwrite another's, silently under-reporting lost transactions.
	// Protected by mu.
	recovery map[string]*siteRecovery

	// lastReassert is when checkPrimaryReassert last restored (or attempted
	// to restore) writability on a fenced promoted primary. Rate-limits the
	// re-assert to once per failoverCooldown so a sidecar that keeps
	// fencing for a persistent reason is not fought at poll frequency.
	lastReassert time.Time

	// lastFenceRetryLog rate-limits the poll-driven split-brain fence
	// retry's log lines: the retry itself runs every poll while the
	// condition persists, but repeating the same Info/Error at poll
	// frequency would fill the log with a fact already surfaced. Protected
	// by mu.
	lastFenceRetryLog time.Time

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

	// bootstrapRecipient records the site currently being cloned into.
	// Consulted only while bootstrapPhase is in-flight, so a stale value
	// left behind by a finished bootstrap is inert. Protected by mu.
	bootstrapRecipient string

	// reclonePendingSite is set by the runner when an admin annotation
	// requests a reclone of a specific site. Processed during the next
	// poll cycle then cleared. Protected by mu.
	reclonePendingSite string

	// keyringGate, when set, is consulted before every clone into a
	// site. Encryption-at-rest sites normally run with a read-only
	// keyring, but a CLONE INSTANCE recipient re-encrypts the donor's
	// tablespace keys under a new master key of its own — which a
	// read-only keyring cannot accept. The gate asks the reconciler to
	// unseal the recipient and reports whether it is ready yet; a clone
	// that starts against a sealed recipient fails partway and leaves
	// the data directory unusable. RequestKeyringUnseal is a no-op when
	// encryption is off, so the gate is wired even on plaintext groups.
	// Nil only in tests that do not exercise the gate.
	keyringGate KeyringGate

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

	// keyringRotationBlocked is the set of sites whose UnsealReason is
	// Rotation. They stay in topology tallies but must not be promoted.
	// keyringRotationBlockedDirty is set when the set changes so Poll
	// re-runs applyCrossSiteAction without waiting for a MySQL state
	// transition. Protected by mu.
	keyringRotationBlocked      []string
	keyringRotationBlockedDirty bool
	keyringPromotionSkipped     []string
	keyringPromotionRefused     []string

	// specDriftSites lists site names whose Deployment spec-hash differs
	// from the desired hash. Set by the runner, consumed by checkUpdate.
	// Protected by mu.
	specDriftSites []string

	// recloneCompletedSite prevents an annotation-cleanup outage from
	// repeating an already-successful destructive clone. Poll retries
	// only the durable annotation removal while this is set.
	recloneCompletedSite string

	// cloneHoldReleaseSite retries NotifyCloneComplete after a finished
	// bootstrap whose release write failed. Poll retries only the
	// release — it must not start another CLONE INSTANCE.
	cloneHoldReleaseSite string

	// StatusCallback is invoked after each poll cycle that produces a state change.
	// The runner sets this to push status updates to the CR.
	StatusCallback func(TopologySnapshot)

	// BootstrapStatusCallback is invoked from the bootstrap goroutine when the
	// bootstrap phase changes. Unlike StatusCallback it pushes only the
	// Bootstrapping condition so that unrelated conditions (Degraded,
	// ReplicationBroken, Updating, ...) are not inadvertently cleared by a
	// partially-populated TopologySnapshot during an async bootstrap run.
	BootstrapStatusCallback func(phase, errMsg, source string)

	// RecloneCompleteCallback durably consumes the reclone annotation
	// after the requested bootstrap attempt completes. Keeping it until
	// then lets an operator restart retry an interrupted attempt.
	RecloneCompleteCallback func(ctx context.Context, site string) error

	// EmergencyFailoverCallback is invoked synchronously after a
	// successful emergency failover Execute, on the same goroutine that
	// drove the failover. The runner wires this to a best-effort
	// DragonflyManager.TryEmergencyPromote so the cache subsystem
	// follows the durable promotion. The callback must not return errors
	// — emergency MySQL failover safety is unconditional.
	EmergencyFailoverCallback func(ctx context.Context, target, oldPrimary string)

	// sleep is time.Sleep in production. The deterministic simulation
	// harness replaces it so short in-poll retry backoffs (e.g.
	// verifyDirectReplica) do not consume wall-clock time per trial.
	sleep func(time.Duration)

	mu           sync.RWMutex
	lastPollTime time.Time
	ready        bool
	cancelFunc   context.CancelFunc

	// metricsStopped is set when the runner is shutting this manager
	// down. Poll skips site-gauge emission so a late cycle after a
	// timed-out join cannot resurrect deleted series.
	metricsStopped atomic.Bool
}

// SiteStatusEntry is a single site's status in the StatusResponse.
type SiteStatusEntry struct {
	Name                      string `json:"name"`
	Role                      string `json:"role,omitempty"`
	State                     string `json:"state"`
	SourceHost                string `json:"sourceHost,omitempty"`
	SourceConvergenceState    string `json:"sourceConvergenceState,omitempty"`
	SourceConvergenceReason   string `json:"sourceConvergenceReason,omitempty"`
	ServingHealthy            bool   `json:"servingHealthy,omitempty"`
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

const (
	recoveryStateInProgress       = "RecoveryInProgress"
	recoveryStateBlocked          = "RecoveryBlocked"
	defaultConnectionDrainTimeout = 30 * time.Second
	// recoveryRetryDelay is the stabilization window after RecoverOldPrimary.
	// During this window the operator waits for replication to report healthy
	// instead of immediately resetting replica metadata again on the next poll.
	recoveryRetryDelay = 30 * time.Second
)

// siteRecovery tracks one site's old-primary recovery lifecycle.
type siteRecovery struct {
	state          string // recoveryStateInProgress or recoveryStateBlocked
	retryAfter     time.Time
	divergentGtid  string
	divergentCount int64
	drainStartedAt time.Time
	drainComplete  bool
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
		sleep:            time.Sleep,
		recovery:         make(map[string]*siteRecovery),
	}
}

// SetSleepForTest replaces the in-poll retry sleep (used by short verify
// backoffs) so deterministic simulation tests do not spend wall-clock time.
// This should only be used in tests, and only before Run/Poll starts: tm.sleep
// is read from the poll path without synchronization, so calling it on a
// running manager is a data race.
func (tm *TopologyManager) SetSleepForTest(fn func(time.Duration)) {
	tm.sleep = fn
}

// SetLastFailoverTarget restores the failover target from persisted state.
// Called once at startup so recovery logic works across operator restarts.
func (tm *TopologyManager) SetLastFailoverTarget(target string) {
	tm.lastFailoverTarget = target
}

// SetLastFailover restores the failover timestamp from persisted state.
// Called once at startup so the anti-flap cooldown window survives operator
// restart instead of silently resetting to zero (which would let a fresh
// operator process ping-pong promote inside the cooldown that the prior
// process intended to enforce).
func (tm *TopologyManager) SetLastFailover(t time.Time) {
	tm.lastFailover = t
	tm.lastFailoverLocal = false
}

// SetFailoverStateRecorder wires the out-of-band anti-flap store. Call once
// at startup, before Run/Poll: the field is read from the poll path without
// synchronization.
func (tm *TopologyManager) SetFailoverStateRecorder(rec FailoverStateRecorder) {
	tm.failoverState = rec
}

// recordFailover stamps the in-memory promotion state for a promotion that
// has already been confirmed writable, then persists the anti-flap half of
// it out of band.
//
// Every path that promotes — emergency failover, planned switchover, and the
// ordered-update handoff — goes through here, so the durable record can
// never be written by one and skipped by another.
func (tm *TopologyManager) recordFailover(ctx context.Context, now time.Time, target, promotionGtid string) {
	rec := FailoverRecord{LastFailover: now, LastFailoverTarget: target}

	tm.mu.Lock()
	tm.authorityEpoch++
	tm.promotionGtidExecuted = promotionGtid
	tm.promotedSite = target
	tm.promotedAt = now
	// A promotion observed by this process supersedes restored state even if
	// the wall clock moved backward. Thereafter, only advance the anti-flap
	// pair: the ordered-update and poll goroutines sample `now` before this
	// lock, so an older local sample can arrive second. The bookkeeping
	// fields above stay last-writer-wins because they describe what this
	// process just did and feed old-primary recovery.
	ignoredOlderLocal := false
	currentFailover, currentTarget := tm.lastFailover, tm.lastFailoverTarget
	if !tm.lastFailoverLocal || !tm.lastFailover.After(now) {
		tm.lastFailover = now
		tm.lastFailoverTarget = target
		tm.lastFailoverLocal = true
	} else {
		ignoredOlderLocal = true
	}
	// Same monotone rule for the durable record.
	if tm.desiredFailoverState == nil || !tm.desiredFailoverState.LastFailover.After(now) {
		cp := rec
		tm.desiredFailoverState = &cp
		tm.failoverStateDirty = true
	}
	tm.mu.Unlock()
	if ignoredOlderLocal {
		tm.logger.Warn("ignored out-of-order local failover record",
			"target", target, "lastFailover", now,
			"currentTarget", currentTarget, "currentLastFailover", currentFailover)
	}

	tm.flushFailoverState(ctx, true)
}

// flushFailoverState writes the newest anti-flap record through the
// out-of-band store, leaving the dirty flag set when the write is rejected
// so the next poll retries.
//
// Deliberately best-effort and never fatal to the promotion: the site is
// already writable by the time this runs, and refusing to acknowledge a
// completed promotion because a bookkeeping write failed would trade a
// timing guarantee for an availability outage. The retry closes the window
// instead.
//
// wait chooses how to handle a flush already in progress. A promotion waits,
// because its record is the one that matters; the per-poll retry does not,
// because blocking the poll loop behind another goroutine's API call buys
// nothing — the record stays dirty and the next poll picks it up.
func (tm *TopologyManager) flushFailoverState(ctx context.Context, wait bool) {
	if tm.failoverState == nil {
		return
	}
	tm.mu.RLock()
	dirty := tm.failoverStateDirty
	tm.mu.RUnlock()
	if !dirty {
		return
	}

	if wait {
		tm.failoverStateWriteMu.Lock()
	} else if !tm.failoverStateWriteMu.TryLock() {
		return
	}
	defer tm.failoverStateWriteMu.Unlock()

	// Read the record only after winning the write lock: a flush that
	// queued behind another must write whatever is newest NOW, not what was
	// newest when it was called.
	tm.mu.RLock()
	rec, dirty := tm.desiredFailoverState, tm.failoverStateDirty
	tm.mu.RUnlock()
	if !dirty || rec == nil {
		return
	}
	write := *rec

	// A manager config restart cancels the poll context, but the status write
	// uses the longer-lived runner context. Detach this already-started
	// bookkeeping write as well so the two durable paths do not diverge only
	// because one manager generation was replaced. The timeout still bounds
	// shutdown work.
	writeCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), failoverStateWriteTimeout)
	defer cancel()
	err := tm.failoverState.RecordFailoverState(writeCtx, write)
	if err != nil {
		tm.logger.Error("out-of-band anti-flap state write failed; retrying every poll",
			"target", write.LastFailoverTarget, "lastFailover", write.LastFailover, "error", err)
		return
	}

	// Clear the flag only if the desired record is still exactly what was
	// written. Comparing the whole record rather than just the timestamp
	// matters under a coarse clock: a promotion that supersedes this one
	// inside the same tick carries an equal LastFailover but a different
	// target, and clearing the flag for it would drop that target with
	// nothing scheduled to retry.
	tm.mu.Lock()
	if tm.desiredFailoverState == nil || *tm.desiredFailoverState != write {
		tm.mu.Unlock()
		return
	}
	tm.failoverStateDirty = false
	tm.mu.Unlock()
}

// SetRecoveryBlocked restores a persisted divergent-recovery marker from CR
// status into the in-memory topology manager after an operator restart. The
// zero retry timestamp makes the restarted process re-verify the divergence
// report on its first qualifying poll.
func (tm *TopologyManager) SetRecoveryBlocked(site, divergentGtid string, divergentCount int64) {
	tm.mu.Lock()
	defer tm.mu.Unlock()
	tm.recovery[site] = &siteRecovery{
		state:          recoveryStateBlocked,
		divergentGtid:  divergentGtid,
		divergentCount: divergentCount,
	}
}

// SetRecoveryInProgress restores a persisted no-divergence recovery marker
// from CR status into the in-memory topology manager after an operator restart.
// The retry timestamp is deliberately left zero so the restarted process can
// immediately either clear the marker if replication is healthy or retry the
// idempotent recovery sequence if it is not.
func (tm *TopologyManager) SetRecoveryInProgress(site string) {
	tm.mu.Lock()
	defer tm.mu.Unlock()
	tm.recovery[site] = &siteRecovery{state: recoveryStateInProgress}
}

// SetSourceConvergence restores persisted source state across an operator
// restart. Live polling remains authoritative and will clear or retry it.
func (tm *TopologyManager) SetSourceConvergence(site, host string, convergence SourceConvergenceState, reason string) {
	tm.mu.Lock()
	defer tm.mu.Unlock()
	if s := tm.getSite(site); s != nil {
		s.sourceHost = host
		s.sourceConvergenceState = convergence
		s.sourceConvergenceReason = reason
	}
}

// activeSiteLocked returns the unique writable primary-candidate. Any second
// writable site, including a reader or dr-only anomaly, makes authority
// ambiguous and returns empty. Must be called with tm.mu held.
func (tm *TopologyManager) activeSiteLocked() string {
	var active *siteTracker
	for i := range tm.sites {
		// A reader that is writable *because the operator is cloning into it*
		// is an expected phase of that clone, not evidence of lost authority:
		// CLONE INSTANCE restarts mysqld and the fresh datadir comes up before
		// super_read_only is reapplied. Counting that window dropped
		// status.activeSite to "" on every reader reclone — a spurious
		// NoPrimary published to DNS steering and every status consumer —
		// while the promotable primary stayed healthy and unambiguous.
		//
		// A reader that turns writable on its own still invalidates authority
		// immediately (see TestPoll_FirstWritableNonPromotableObservation-
		// FencesImmediately); that signal is deliberate and unchanged. Either
		// way the reader is alerted and fenced by the non-promotable path.
		if tm.sites[i].role == state.SiteRoleReadOnly &&
			tm.sites[i].name == tm.bootstrapRecipient &&
			!bootstrapIdlePhase(tm.bootstrapPhase) {
			continue
		}
		if tm.sites[i].state == state.StateWritable {
			if active != nil {
				return ""
			}
			active = &tm.sites[i]
		}
	}
	if active != nil && active.isPromotable() {
		return active.name
	}
	return ""
}

func (tm *TopologyManager) Status() StatusResponse {
	tm.mu.RLock()
	defer tm.mu.RUnlock()
	sites := make([]SiteStatusEntry, len(tm.sites))
	for i := range tm.sites {
		sites[i] = SiteStatusEntry{
			Name:                    tm.sites[i].name,
			Role:                    string(tm.sites[i].role),
			State:                   tm.sites[i].state.String(),
			SourceHost:              tm.sites[i].sourceHost,
			SourceConvergenceState:  string(tm.sites[i].sourceConvergenceState),
			SourceConvergenceReason: tm.sites[i].sourceConvergenceReason,
			ServingHealthy:          tm.sites[i].servingHealthy,
		}
		if rec := tm.recovery[tm.sites[i].name]; rec != nil {
			sites[i].RecoveryState = rec.state
			if rec.state == recoveryStateBlocked {
				sites[i].DivergentGtid = rec.divergentGtid
				if rec.divergentCount > 0 {
					c := rec.divergentCount
					sites[i].DivergentTransactionCount = &c
				}
			}
		}
	}
	return StatusResponse{
		ActiveSite:            tm.effectiveActiveSiteLocked(),
		Sites:                 sites,
		PollTime:              tm.lastPollTime.Format(time.RFC3339),
		PromotionGtidExecuted: tm.promotionGtidExecuted,
	}
}

func (tm *TopologyManager) effectiveActiveSiteLocked() string {
	if active := tm.activeSiteLocked(); active != "" {
		return active
	}
	if tm.pendingPromotionFreshLocked() && tm.pendingPromotionUnambiguousLocked() {
		return tm.promotedSite
	}
	return ""
}

func (tm *TopologyManager) pendingPromotionUnambiguousLocked() bool {
	target := tm.getSite(tm.promotedSite)
	if target == nil || !target.isPromotable() {
		return false
	}
	for i := range tm.sites {
		if tm.sites[i].state == state.StateWritable && tm.sites[i].name != tm.promotedSite {
			return false
		}
	}
	return true
}

func (tm *TopologyManager) pendingPromotionFreshLocked() bool {
	if tm.promotedSite == "" {
		return false
	}
	if tm.promotedAt.IsZero() {
		return false
	}
	return tm.clock.Since(tm.promotedAt) <= pendingPromotionActiveSiteTTL
}

// reconcilePendingPromotionLocked clears the short-lived promotion guard once
// topology observations have caught up with the promotion result. It also
// drops stale or superseded pending promotions so a future failover is not
// blocked forever if recovery made a different site writable.
func (tm *TopologyManager) reconcilePendingPromotionLocked() {
	if tm.promotedSite == "" {
		return
	}
	pending := tm.promotedSite
	active := tm.activeSiteLocked()
	switch {
	case active == pending:
		tm.logger.Info("promotion confirmed: site is writable", "site", pending)
		tm.clearPendingPromotionLocked()
	case active != "":
		tm.logger.Warn("pending promotion superseded by different writable site; clearing guard",
			"pendingSite", pending, "activeSite", active)
		tm.clearPendingPromotionLocked()
	case !tm.pendingPromotionFreshLocked():
		tm.logger.Warn("pending promotion expired before writable confirmation; clearing guard",
			"pendingSite", pending, "age", tm.clock.Since(tm.promotedAt))
		tm.clearPendingPromotionLocked()
	}
}

func (tm *TopologyManager) clearPendingPromotionLocked() {
	tm.promotedSite = ""
	tm.promotedAt = time.Time{}
}

func (tm *TopologyManager) confirmWritable(ctx context.Context, site *siteTracker) error {
	confirmCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	readOnly, err := site.mysql.CheckReadOnly(confirmCtx)
	if err != nil {
		return fmt.Errorf("confirm writable on %q: %w", site.name, err)
	}
	if readOnly {
		return fmt.Errorf("site %q still read_only after promotion", site.name)
	}
	return nil
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

	tm.mu.RLock()
	pollAuthorityEpoch := tm.authorityEpoch
	tm.mu.RUnlock()

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
		// A successful writable observation on a non-promotable site is an
		// immediate safety fact. Do not debounce authority invalidation or
		// fencing behind the normal recovery threshold.
		if results[i].err == nil && !results[i].readOnly && !tm.sites[i].isPromotable() {
			newStates[i] = state.StateWritable
		}
	}

	// Update lastSeen and state under the lock.
	tm.mu.Lock()
	if tm.authorityEpoch != pollAuthorityEpoch {
		tm.mu.Unlock()
		return
	}
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

	// A writable non-promotable site is never authoritative. Enforce this on
	// every poll so a failed fence is retried without waiting for a transition.
	fencedNonPromotable := tm.fenceWritableNonPromotableSites(ctx)

	// Check for pending promotion confirmation: DNS was flipped after successful
	// promotion and writable confirmation; this block verifies the promoted site
	// is still writable and clears the guard flag to allow future failovers.
	tm.mu.Lock()
	if tm.authorityEpoch != pollAuthorityEpoch {
		tm.mu.Unlock()
		return
	}
	if tm.promotedSite != "" {
		tm.reconcilePendingPromotionLocked()
	}
	tm.mu.Unlock()

	// Evaluate every poll so all status snapshots carry the current topology
	// condition. Mutating cross-site actions remain transition-driven, plus
	// a keyring-rotation block-set change: otherwise a 2-site refuse never
	// heals after the replica reaches Sealed (no MySQL state transition).
	blockedSetChanged := tm.peekKeyringRotationBlockedDirty()
	action := state.EvalCrossSite(tm.observations(), tm.cfg.SitePriorities)
	alertMsg := action.Alert
	if anyTransition || blockedSetChanged {
		tm.applyCrossSiteAction(ctx, action)
	}

	// Fencing a returning old primary must be retried on every poll, not only
	// on the poll that observed the state transition: fencing is best-effort,
	// and a fence attempt that fails transiently (network blip, brief
	// connection error during the site's recovery turbulence) would otherwise
	// never be retried — a stable split-brain produces no further transitions,
	// and every other corrective path (recovery, re-assert, convergence)
	// requires a unique writable primary and therefore stays inert. Without
	// this, one lost fence write leaves two writable sites diverging until a
	// human intervenes. Transition polls already fenced in
	// applyCrossSiteAction; retry only between transitions.
	fencedOldPrimary := false
	if !anyTransition {
		fencedOldPrimary = tm.fenceReturningOldPrimary(ctx, action)
	}

	// Update status.
	tm.mu.Lock()
	tm.lastPollTime = tm.clock.Now()
	tm.ready = true
	tm.mu.Unlock()

	metrics.WSClientCount.Set(float64(tm.hub.ClientCount()))

	// Emit site state metrics every poll cycle. Delete first so a role
	// change cannot leave a stale one-hot set under the previous role.
	if !tm.siteMetricsHalted() {
		for i := range tm.sites {
			ns, group, name, role := tm.siteMetricLabels(&tm.sites[i])
			metrics.DeleteSiteState(ns, group, name)
			currentState := tm.sites[i].state.String()
			for _, s := range metrics.AllStates {
				val := 0.0
				if s == currentState {
					val = 1.0
				}
				metrics.SiteState.WithLabelValues(ns, group, name, role, s).Set(val)
			}
		}
	}

	// Check replication status on every read-only site.
	siteRepl := make([]*mysql.ReplicaStatus, len(tm.sites))
	replicaProbeFailed := make(map[string]struct{})
	replicationChanged := false
	sourceProbeChanged := false
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
			wasReplicating := tm.sites[i].replicating
			tm.sites[i].replicating = false
			tm.sites[i].replicatingStreak = 0
			if wasReplicating {
				replicationChanged = true
			}
			tm.mu.Unlock()
			sourceProbeChanged = tm.markSourceProbeFailed(&tm.sites[i]) || sourceProbeChanged
			replicaProbeFailed[tm.sites[i].name] = struct{}{}
			continue
		}
		siteRepl[i] = rs
		healthy := rs != nil && rs.IORunning && rs.SQLRunning && rs.SourceHost != ""
		tm.mu.Lock()
		wasReplicating := tm.sites[i].replicating
		if healthy {
			tm.sites[i].replicatingStreak++
			if tm.sites[i].replicatingStreak >= replicatingStreakThreshold {
				tm.sites[i].replicating = true
			}
		} else {
			tm.sites[i].replicating = false
			tm.sites[i].replicatingStreak = 0
		}
		if tm.sites[i].replicating != wasReplicating {
			replicationChanged = true
		}
		tm.mu.Unlock()
	}

	// Converge every configured follower directly onto the unique confirmed
	// active primary before old-primary recovery considers source-less sites.
	handledConvergence, convergenceChanged := tm.checkSourceConvergence(ctx, siteRepl)
	for site := range replicaProbeFailed {
		handledConvergence[site] = struct{}{}
	}
	convergenceChanged = convergenceChanged || sourceProbeChanged

	// Check if old primary recovery is needed.
	recoveryChanged := tm.checkRecoveryWithConvergence(ctx, siteRepl, handledConvergence)

	// Restore writability on the promoted primary if its sidecar fenced
	// it back to read-only and no writable site remains (poll-driven so
	// the wedge heals even without further state transitions).
	reasserted := tm.checkPrimaryReassert(ctx)

	// Steer DNS at the current active site. Poll-driven and idempotent, so a
	// promotion-time flip that was rejected (e.g. an RBAC-denied DNSEndpoint
	// write) heals once the write is permitted again — without a fresh
	// failover, and without replaying a target that a later promotion has
	// already superseded.
	tm.reconcileDNS(ctx)

	// Retry a failed Clone-hold release before starting any new clone.
	tm.releaseCloneHold(ctx)

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
		if !suppressed && bootstrapIdlePhase(phase) {
			if donor, empty := tm.detectEmptySite(ctx); donor != "" && empty != "" {
				tm.startBootstrapByName(ctx, donor, empty, "auto-clone")
				autoCloneStarted = true
			}
		}
	}

	// Emit replication metrics.
	if !tm.siteMetricsHalted() {
		for i := range tm.sites {
			ns, group, name, role := tm.siteMetricLabels(&tm.sites[i])
			if siteRepl[i] != nil {
				// Drop any previous role's series before writing the current one.
				metrics.DeleteReplicationGauges(ns, group, name)
				if siteRepl[i].SecondsBehindSource != nil {
					metrics.ReplicationLag.WithLabelValues(ns, group, name, role).Set(float64(*siteRepl[i].SecondsBehindSource))
				} else {
					metrics.ReplicationLag.WithLabelValues(ns, group, name, role).Set(-1)
				}
				ioVal := 0.0
				if siteRepl[i].IORunning {
					ioVal = 1.0
				}
				sqlVal := 0.0
				if siteRepl[i].SQLRunning {
					sqlVal = 1.0
				}
				metrics.ReplicationRunning.WithLabelValues(ns, group, name, role, "io").Set(ioVal)
				metrics.ReplicationRunning.WithLabelValues(ns, group, name, role, "sql").Set(sqlVal)
			} else if tm.sites[i].state == state.StateWritable {
				// Primary site: clear replication metrics (not a replica).
				metrics.DeleteReplicationGauges(ns, group, name)
			}
		}
	}
	tm.emitSourceConvergenceMetrics()

	// Re-attempt a rejected out-of-band anti-flap write. Independent of the
	// status retry below: the two durable paths have separate RBAC rules and
	// separate admission chains, so one can be denied while the other is
	// healthy, and each has to heal on its own cadence.
	tm.flushFailoverState(ctx, false)

	// Notify the status callback on any state change, recovery event, or
	// update event — and additionally whenever a prior /status write was
	// rejected, so a transiently-denied status update self-heals once the
	// write is permitted again (updateCRStatus no-op-skips when the CR
	// already matches, so a healthy cluster pays only a DeepEqual per poll).
	tm.mu.RLock()
	statusRetry := tm.statusWriteFailed
	tm.mu.RUnlock()
	statusChanged := anyTransition || blockedSetChanged || fencedNonPromotable || fencedOldPrimary || replicationChanged || convergenceChanged || recoveryChanged || recloneStarted || autoCloneStarted || updateStarted || reasserted
	if (statusChanged || statusRetry) && tm.StatusCallback != nil {
		var snapshot TopologySnapshot
		if statusRetry && !statusChanged {
			tm.mu.RLock()
			if tm.statusRetrySnapshot != nil {
				snapshot = *tm.statusRetrySnapshot
			}
			tm.mu.RUnlock()
		} else {
			snapshot = tm.buildSnapshot(siteRepl)
		}
		tm.mu.Lock()
		tm.statusRetrySnapshot = &snapshot
		tm.mu.Unlock()
		tm.StatusCallback(snapshot)
	}

	// Broadcast full topology to WebSocket clients on every poll cycle.
	tm.broadcastTopology(siteRepl, alertMsg)
}

// desiredDNSSiteLocked returns the site DNS should point at, or "" when the
// answer is not unambiguous and DNS must therefore be left alone. Must be
// called with tm.mu held.
//
// A promotion this process just performed wins over the observed states: the
// demoted source can still read as writable for a tick or two, and steering
// DNS back at it would replay a stale target. Otherwise the answer is the
// single writable site — zero writable (mid-failover, NoPrimary) or more than
// one (split brain, or an old primary that has not been fenced yet) both
// return "" so no guess is ever published.
func (tm *TopologyManager) desiredDNSSiteLocked() string {
	if tm.pendingPromotionFreshLocked() {
		return tm.promotedSite
	}
	return tm.activeSiteLocked()
}

// applyDNS is the ONLY place the DNS record is written. Centralizing the
// write keeps three properties that were previously spread across the
// promotion paths and easy to get wrong:
//
//   - bloodraven_dns_flips_total is incremented for the site whose target was
//     actually applied, and only when the record's value really changed.
//   - a rejected write is remembered (dnsWriteFailed), so reconcileDNS keeps
//     retrying it even without a fresh state transition.
//   - a successful write is remembered (dnsAppliedTarget), so a converged
//     cluster does not re-write the record on every poll.
func (tm *TopologyManager) applyDNS(ctx context.Context, site, target string) error {
	if tm.dns == nil || target == "" {
		return nil
	}
	if err := tm.dns.UpdateDNSRecord(ctx, target); err != nil {
		tm.mu.Lock()
		tm.dnsWriteFailed = true
		tm.mu.Unlock()
		return err
	}
	tm.mu.Lock()
	changed := tm.dnsAppliedTarget != target
	tm.dnsAppliedTarget = target
	tm.dnsWriteFailed = false
	tm.mu.Unlock()
	if changed {
		metrics.DNSFlipCount.WithLabelValues(site).Inc()
	}
	return nil
}

// reconcileDNS steers the DNS record at the site that is the primary RIGHT
// NOW. It runs on every poll, so a promotion-time flip that failed (an
// RBAC-denied DNSEndpoint apply, a DNS provider outage) heals on its own once
// the write is permitted again — no second failover, no MySQL mutation.
//
// The desired target is re-derived from live topology every time rather than
// memoized at promotion, which gives two properties the previous
// retry-the-pending-target approach could not:
//
//   - Restart-safe: an operator that restarts mid-denial re-reads the live
//     record (platform.DNSRecordReader) and repairs it against the current
//     active site. Nothing needs to survive in memory.
//   - Cannot replay a stale target: if a planned promote or a primary
//     re-assert has since moved the primary elsewhere, the reconcile applies
//     THAT site — a superseded promotion target can never be published to a
//     now-read-only site.
//
// When the updater cannot read the live record, the reconcile falls back to
// this process's own knowledge: it re-applies only when a write is known to
// have failed, or when the last target it applied is no longer the desired
// one. It never writes speculatively.
func (tm *TopologyManager) reconcileDNS(ctx context.Context) {
	if tm.dns == nil {
		return
	}
	// A planned failover owns the DNS record for the duration of its own
	// promotion sequence; let it finish rather than racing it mid-flight.
	// Any divergence it leaves behind is repaired by the next poll.
	if tm.isPlannedFailoverActive() {
		return
	}

	tm.mu.RLock()
	site := tm.desiredDNSSiteLocked()
	applied := tm.dnsAppliedTarget
	writeFailed := tm.dnsWriteFailed
	tm.mu.RUnlock()
	if site == "" {
		return
	}
	target := ""
	if s := tm.getSite(site); s != nil {
		target = s.lbIP
	}
	if target == "" {
		return
	}

	// Prefer the live record when the updater can read it back: it is the
	// only source of truth that survives an operator restart.
	live, liveKnown := "", false
	if reader, ok := tm.dns.(platform.DNSRecordReader); ok {
		cur, found, err := reader.CurrentDNSRecord(ctx)
		switch {
		case err != nil:
			// Unreadable (e.g. get is denied too) — fall through to the
			// in-process state below rather than guessing.
			tm.logger.Debug("DNS record read failed; falling back to in-process state",
				"site", site, "error", err)
		case !found:
			// No record yet: an absent record diverges from every target.
			live, liveKnown = "", true
		default:
			live, liveKnown = cur, true
		}
	}

	switch {
	case liveKnown && live == target:
		// Converged. Record that so a later write is only counted as a flip
		// if it genuinely changes the record.
		tm.mu.Lock()
		tm.dnsAppliedTarget = target
		tm.dnsWriteFailed = false
		tm.mu.Unlock()
		return
	case liveKnown:
		// Live record diverges from the active site — repair it. Seed the
		// applied target with what is really out there so applyDNS counts
		// the flip against the true previous value.
		tm.mu.Lock()
		tm.dnsAppliedTarget = live
		tm.mu.Unlock()
	case writeFailed:
		// Cannot read the record, but we know our last write did not land.
	case applied != "" && applied != target:
		// Cannot read the record; the last target we applied is no longer
		// the primary (superseded promotion), so re-point it.
	default:
		// Nothing known to be stale — never write speculatively.
		return
	}

	if err := tm.applyDNS(ctx, site, target); err != nil {
		if writeFailed {
			// Already known to be failing: the first failure was logged (here,
			// or as "DNS flip failed after successful promotion"). Do not
			// repeat it every poll — a persistent DNS-provider or RBAC problem
			// would otherwise fill the log at poll frequency.
			tm.logger.Debug("DNS reconcile still failing", "site", site, "target", target, "error", err)
		} else {
			tm.logger.Warn("DNS reconcile failed", "site", site, "target", target, "error", err)
		}
		return
	}
	tm.logger.Info("DNS reconciled to active site", "site", site, "target", target)
}

// MarkStatusWriteResult records whether the most recent CR /status write
// succeeded. A failed write (err != nil) arms a per-poll retry so a
// transiently-denied status update self-heals once the write is permitted
// again; a success (or confirmed no-op) disarms it. Called by the runner's
// StatusCallback with the result of updateCRStatus.
func (tm *TopologyManager) MarkStatusWriteResult(err error) {
	tm.mu.Lock()
	tm.statusWriteFailed = err != nil
	tm.mu.Unlock()
}

// observations returns a SiteObservation for every site. Caller must
// hold no locks (we acquire RLock here).
func (tm *TopologyManager) observations() []state.SiteObservation {
	tm.mu.RLock()
	defer tm.mu.RUnlock()
	blocked := make(map[string]struct{}, len(tm.keyringRotationBlocked))
	for _, name := range tm.keyringRotationBlocked {
		blocked[name] = struct{}{}
	}
	out := make([]state.SiteObservation, len(tm.sites))
	for i := range tm.sites {
		obs := tm.sites[i].observation()
		_, obs.PromotionBlocked = blocked[tm.sites[i].name]
		out[i] = obs
	}
	return out
}

// buildSnapshot produces a TopologySnapshot from current tracker state.
// Callers must provide the most recent siteRepl values.
func (tm *TopologyManager) buildSnapshot(siteRepl []*mysql.ReplicaStatus) TopologySnapshot {
	// Snapshot callbacks also originate outside Poll (for example update
	// completion and status retries). Derive topology health from current
	// trackers every time so those callbacks cannot clear persistent alerts.
	action := state.EvalCrossSite(tm.observations(), tm.cfg.SitePriorities)
	alert, degradedReason := action.Alert, action.Reason
	tm.mu.RLock()
	defer tm.mu.RUnlock()

	sites := make([]SiteSnapshot, len(tm.sites))
	for i := range tm.sites {
		var repl *mysql.ReplicaStatus
		if i < len(siteRepl) {
			repl = siteRepl[i]
		}
		sites[i] = SiteSnapshot{
			Name:                    tm.sites[i].name,
			Role:                    tm.sites[i].role,
			State:                   tm.sites[i].state,
			LastSeen:                tm.sites[i].lastSeen,
			Replication:             repl,
			SourceHost:              tm.sites[i].sourceHost,
			SourceConvergenceState:  tm.sites[i].sourceConvergenceState,
			SourceConvergenceReason: tm.sites[i].sourceConvergenceReason,
			ServingHealthy:          tm.sites[i].servingHealthy,
			ReplicationHealthy:      tm.sites[i].replicating,
		}
		if rec := tm.recovery[tm.sites[i].name]; rec != nil {
			sites[i].RecoveryState = rec.state
			if rec.state == recoveryStateBlocked {
				sites[i].DivergentGtid = rec.divergentGtid
				sites[i].DivergentTxnCount = rec.divergentCount
			}
		}
	}

	var updatePhase string
	if tm.updater != nil {
		updatePhase = string(tm.updater.Phase())
	}

	snap := TopologySnapshot{
		Sites:                   sites,
		ActiveSite:              tm.activeSiteLocked(),
		LastFailover:            tm.lastFailover,
		LastFailoverTarget:      tm.lastFailoverTarget,
		Alert:                   alert,
		DegradedReason:          degradedReason,
		UpdatePhase:             updatePhase,
		BootstrapPhase:          string(tm.bootstrapPhase),
		BootstrapSource:         tm.bootstrapSource,
		PromotionGtidExecuted:   tm.promotionGtidExecuted,
		KeyringPromotionSkipped: append([]string(nil), tm.keyringPromotionSkipped...),
		KeyringPromotionRefused: append([]string(nil), tm.keyringPromotionRefused...),
	}
	// Summarize one recovering site into the top-level fields for condition
	// messages and event emission. A RecoveryBlocked site wins over a
	// RecoveryInProgress one regardless of declared order: blocked means
	// divergent transactions await a human, and the RecoveryPending
	// condition's reason must not mask that behind a routine rejoin.
	for i := range sites {
		if sites[i].RecoveryState == "" {
			continue
		}
		if snap.RecoveryState == "" || (sites[i].RecoveryState == recoveryStateBlocked && snap.RecoveryState != recoveryStateBlocked) {
			snap.RecoverySite = sites[i].Name
			snap.RecoveryState = sites[i].RecoveryState
			snap.DivergentGtid = sites[i].DivergentGtid
			snap.DivergentTxnCount = sites[i].DivergentTxnCount
		}
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
		if rec := tm.recovery[tm.sites[i].name]; rec != nil {
			s.RecoveryState = rec.state
			if rec.state == recoveryStateBlocked {
				s.DivergentGtid = rec.divergentGtid
				if rec.divergentCount > 0 {
					c := rec.divergentCount
					s.DivergentTransactionCount = &c
				}
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
	if site.role == state.SiteRoleReadOnly || site.taintSelector == "" {
		return
	}
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

func (tm *TopologyManager) fenceWritableNonPromotableSites(ctx context.Context) bool {
	attempted := false
	for i := range tm.sites {
		site := &tm.sites[i]
		if site.state != state.StateWritable || site.isPromotable() {
			continue
		}
		attempted = true
		err := withMySQLSafetyTimeout(ctx, func(probeCtx context.Context) error {
			return site.mysql.SetSuperReadOnly(probeCtx, true)
		})
		if err != nil {
			tm.logger.Error("failed to fence writable non-promotable site", "site", site.name, "role", site.role, "error", err)
		} else {
			tm.logger.Warn("fenced writable non-promotable site", "site", site.name, "role", site.role)
		}
	}
	return attempted
}

func (tm *TopologyManager) applyCrossSiteAction(ctx context.Context, action state.CrossSiteAction) {
	if action.Alert != "" {
		tm.logger.Warn("ALERT", "message", action.Alert)
	}

	tm.mu.RLock()
	lastFailoverTarget := tm.lastFailoverTarget
	tm.mu.RUnlock()

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

	// Suppress any cross-site actions while a topology-relevant bootstrap or
	// an ordered update is in progress: during an update the standby is
	// restarting and will appear unreachable, and we do not want to initiate
	// a spurious failover. A clone into a non-promotable reader is excluded —
	// see bootstrapBlocksCrossSite.
	if tm.bootstrapBlocksCrossSite() || tm.isUpdating() {
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
		} else if bootstrapIdlePhase(phase) {
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
			if lastFailoverTarget == "" && tm.isFreshDeploy(ctx) {
				tm.startBootstrap(ctx)
				return
			}
		}
	}

	// If any site is writable after a prior failover and this is not a
	// fresh deploy, the old primary(s) may have returned. Fence every
	// writable site except the current failover target so they stop
	// accepting writes; recovery proceeds in checkRecovery for each
	// fenced site once it transitions to read-only. Poll retries this via
	// fenceReturningOldPrimary on non-transition polls, so a failed fence
	// here is not the last attempt.
	if action.SplitBrain && lastFailoverTarget != "" && !tm.bootstrapBlocksCrossSite() {
		// Same usability guard as the poll-driven retry: only a live,
		// promotable, writable target justifies fencing everything else.
		if keepSite := tm.getSite(lastFailoverTarget); keepSite != nil && keepSite.state == state.StateWritable && keepSite.isPromotable() {
			tm.fenceSitesExcept(ctx, lastFailoverTarget, false)
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
	if action.SplitBrain && lastFailoverTarget == "" && !tm.bootstrapBlocksCrossSite() && len(tm.cfg.SitePriorities) > 0 {
		writable := tm.writableObservations()
		winner, losers := state.ResolveSplitBrain(writable, tm.cfg.SitePriorities)
		if winner != "" {
			tm.mu.RLock()
			winnerBlocked := tm.keyringRotationBlockedLocked(winner)
			tm.mu.RUnlock()
			if winnerBlocked {
				tm.logger.Warn("promotion refused: split-brain winner is mid-keyring-rotation",
					"site", winner, "unsealReason", "Rotation")
				tm.recordKeyringPromotionDecision(nil, []string{winner})
				tm.incrementKeyringPromotionBlocked([]string{winner}, "refused")
				tm.clearKeyringRotationBlockedDirty()
				return
			}
			for _, loser := range losers {
				if site := tm.getSite(loser); site != nil && site.state == state.StateWritable {
					tm.logger.Warn("split-brain auto-resolve: fencing non-preferred site per spec.splitBrainPolicy.sitePriorities",
						"winner", winner, "fencedSite", loser)
					err := withMySQLSafetyTimeout(ctx, func(probeCtx context.Context) error {
						return site.mysql.SetSuperReadOnly(probeCtx, true)
					})
					if err != nil {
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

	if promote == "" && (strings.Contains(action.Alert, "UnsealReason=Rotation") || len(tm.rotationBlockedCandidates()) > 0 && action.Reason == "NoPrimary") {
		refused := tm.rotationBlockedCandidates()
		if len(refused) > 0 {
			tm.logger.Warn("promotion refused: every remaining candidate is mid-keyring-rotation",
				"sites", refused)
			tm.recordKeyringPromotionDecision(nil, refused)
			tm.incrementKeyringPromotionBlocked(refused, "refused")
			tm.clearKeyringRotationBlockedDirty()
		}
		return
	}

	if promote != "" {
		tm.mu.RLock()
		promotedSite := tm.promotedSite
		lastFailover := tm.lastFailover
		tm.mu.RUnlock()
		if promotedSite != "" {
			tm.clearKeyringRotationBlockedDirty()
			return
		}
		if !lastFailover.IsZero() && tm.clock.Since(lastFailover) < tm.failoverCooldown {
			tm.logger.Info("failover blocked by anti-flap cooldown",
				"lastFailover", lastFailover, "cooldown", tm.failoverCooldown)
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
		tm.mu.RLock()
		blocked := tm.keyringRotationBlockedLocked(candidate.name)
		tm.mu.RUnlock()
		skipped := tm.rotationBlockedCandidates()
		filtered := skipped[:0]
		for _, name := range skipped {
			if name != candidate.name {
				filtered = append(filtered, name)
			}
		}
		skipped = filtered
		if blocked {
			tm.logger.Warn("promotion refused: candidate is mid-keyring-rotation",
				"site", candidate.name, "unsealReason", "Rotation")
			tm.recordKeyringPromotionDecision(nil, []string{candidate.name})
			tm.incrementKeyringPromotionBlocked([]string{candidate.name}, "refused")
			tm.clearKeyringRotationBlockedDirty()
			return
		}
		for _, name := range skipped {
			tm.logger.Warn("skipping promotion candidate: site is mid-keyring-rotation",
				"site", name, "unsealReason", "Rotation")
		}
		if len(skipped) > 0 {
			tm.recordKeyringPromotionDecision(skipped, nil)
			tm.incrementKeyringPromotionBlocked(skipped, "skipped")
		} else {
			tm.recordKeyringPromotionDecision(nil, nil)
		}
		tm.clearKeyringRotationBlockedDirty()

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

		promotionGtid, err := tm.failover.Execute(ctx, candidate.mysql, oldPrimaryChecker, candidate.name)
		if err != nil {
			tm.logger.Error("failover failed", "error", err)
			return
		}
		if err := tm.confirmWritable(ctx, candidate); err != nil {
			tm.logger.Error("promotion succeeded but writable confirmation failed; DNS not flipped",
				"site", candidate.name, "error", err)
			return
		}

		// Promotion + writable confirmation both succeeded: the candidate
		// is the authoritative primary now, regardless of whether DNS has
		// caught up. Record the failover state (anti-flap cooldown,
		// split-brain fencing target, status GTID) and count the failover
		// BEFORE the DNS flip so a DNS-provider outage cannot erase the
		// fact that a promotion happened. The DNS flip below is then
		// best-effort for state tracking — its failure is logged and the
		// success-only DNSFlipCount metric is left unincremented, but the
		// operator no longer forgets the promotion.
		tm.recordFailover(ctx, tm.clock.Now(), candidate.name, promotionGtid)
		metrics.FailoversTotal.WithLabelValues(candidate.name).Inc()

		if err := tm.applyDNS(ctx, candidate.name, candidate.lbIP); err != nil {
			tm.logger.Error("DNS flip failed after successful promotion", "site", candidate.name, "error", err)
			// Not fatal, and nothing to remember: reconcileDNS re-derives the
			// desired target from live topology on every poll, so DNS heals to
			// whichever site is primary once the write is permitted again.
			// The MySQL promotion state above is already durable.
		}

		// Best-effort: ask the Dragonfly subsystem (when wired) to
		// follow the MySQL promotion. The callback enforces its own
		// budget and never blocks the topology poll.
		if tm.EmergencyFailoverCallback != nil {
			tm.EmergencyFailoverCallback(ctx, candidate.name, oldPrimaryName)
		}
		return
	}
	tm.clearKeyringRotationBlockedDirty()
}

// fenceSitesExcept sets super_read_only=ON on every writable site other than
// keep. Best-effort: failures are logged and retried by the caller's cadence.
// quiet demotes the per-attempt lines to Debug (used by the poll-driven retry
// between its rate-limited Info windows).
func (tm *TopologyManager) fenceSitesExcept(ctx context.Context, keep string, quiet bool) {
	for i := range tm.sites {
		if tm.sites[i].name == keep {
			continue
		}
		if tm.sites[i].state != state.StateWritable {
			continue
		}
		if quiet {
			tm.logger.Debug("fencing returning old primary (split brain after failover)", "site", tm.sites[i].name)
		} else {
			tm.logger.Info("fencing returning old primary (split brain after failover)", "site", tm.sites[i].name)
		}
		err := withMySQLSafetyTimeout(ctx, func(probeCtx context.Context) error {
			return tm.sites[i].mysql.SetSuperReadOnly(probeCtx, true)
		})
		if err != nil {
			if quiet {
				tm.logger.Debug("failed to fence returning old primary", "site", tm.sites[i].name, "error", err)
			} else {
				tm.logger.Error("failed to fence returning old primary", "site", tm.sites[i].name, "error", err)
			}
		}
	}
}

// fenceReturningOldPrimary is the poll-driven retry of split-brain fencing in
// applyCrossSiteAction. It runs only when the observations show a split-brain
// and no stronger process (planned failover, in-place restore, topology-
// relevant bootstrap, ordered update) owns the cluster. Two variants:
//
//   - Failover history known: fence every writable site except the failover
//     target (the returning-old-primary case).
//   - No history but spec.splitBrainPolicy.sitePriorities configured: fence
//     the policy losers, deferring to the transition-driven path in
//     applyCrossSiteAction for the promotion stamp. Bootstrap precedence is
//     preserved — an empty-site pair or a fresh deploy is a clone/bootstrap
//     concern and must not have its recipient or seed fenced from here.
//
// Both are required to be poll-driven: fencing is best-effort, and a fence
// attempt that fails transiently would otherwise never be retried, because a
// stable split-brain produces no further state transitions. Returns true when
// a fence was attempted so Poll surfaces it through the status callback.
func (tm *TopologyManager) fenceReturningOldPrimary(ctx context.Context, action state.CrossSiteAction) bool {
	// EvalCrossSite reports SplitBrain=false whenever a writable
	// non-promotable site needs fencing first (its FenceSites early return),
	// which would leave a concurrent split-brain among primary-candidates
	// unfenced for as long as the reader fence keeps failing. Count writable
	// candidates directly so this retry is gated by the actual hazard, not
	// by the reader's fencing progress.
	writableCandidates := 0
	tm.mu.RLock()
	for i := range tm.sites {
		if tm.sites[i].state == state.StateWritable && tm.sites[i].isPromotable() {
			writableCandidates++
		}
	}
	lastFailoverTarget := tm.lastFailoverTarget
	tm.mu.RUnlock()
	if !action.SplitBrain && writableCandidates < 2 {
		return false
	}
	if tm.isTopologyFrozen() || tm.isPlannedFailoverActive() || tm.bootstrapBlocksCrossSite() || tm.isUpdating() {
		return false
	}

	keep := lastFailoverTarget
	if keep != "" {
		// The history branch protects the failover target's authority by
		// fencing everything else — which only makes sense while the target
		// IS a live writable authority. A rehydrated target that names no
		// configured site, or one that is currently not writable (crashed
		// after promotion, demoted, still recovering), must not cause every
		// OTHER writable site to be fenced into a zero-primary outage.
		// Fall through to the priorities policy instead of giving up
		// entirely, so a stale target cannot disable both resolution paths.
		keepSite := tm.getSite(keep)
		if keepSite == nil || keepSite.state != state.StateWritable || !keepSite.isPromotable() {
			tm.logger.Debug("split-brain fence retry: failover target is not a live promotable writable authority; deferring to priorities policy",
				"target", keep)
			keep = ""
		}
	}
	if keep == "" {
		if len(tm.cfg.SitePriorities) == 0 {
			return false // manual resolution by design
		}
		// Mirror applyCrossSiteAction's fresh-deploy precedence with the
		// read-only, data-aware isFreshDeploy check. An empty writable site
		// among the losers is NOT a reason to defer: fencing a clone
		// recipient is exactly what detectAndFenceEmptyWritableSite does
		// before cloning it, so deferring here would only leave the
		// split-brain running while a stuck bootstrap made no progress.
		if tm.bootstrap != nil && tm.bootstrapCfg.ReplUser != "" && tm.isFreshDeploy(ctx) {
			return false
		}
		winner, losers := state.ResolveSplitBrain(tm.writableObservations(), tm.cfg.SitePriorities)
		if winner == "" {
			return false
		}
		tm.mu.RLock()
		winnerBlocked := tm.keyringRotationBlockedLocked(winner)
		tm.mu.RUnlock()
		if winnerBlocked {
			return false
		}
		// Poll-driven retry of the policy resolution: same documented log
		// message as the transition-driven path, rate-limited to the shared
		// 30s window. The split_brain_auto_resolve_total metric counts
		// resolution EVENTS and is incremented only by the transition-driven
		// path — this retry re-attempts the same resolution, so incrementing
		// here would inflate the count at poll frequency.
		tm.mu.Lock()
		quiet := !tm.lastFenceRetryLog.IsZero() && tm.clock.Since(tm.lastFenceRetryLog) < 30*time.Second
		if !quiet {
			tm.lastFenceRetryLog = tm.clock.Now()
		}
		tm.mu.Unlock()
		attempted := false
		for _, loser := range losers {
			site := tm.getSite(loser)
			if site == nil || site.state != state.StateWritable {
				continue
			}
			attempted = true
			if quiet {
				tm.logger.Debug("split-brain auto-resolve: fencing non-preferred site per spec.splitBrainPolicy.sitePriorities",
					"winner", winner, "fencedSite", loser)
			} else {
				tm.logger.Warn("split-brain auto-resolve: fencing non-preferred site per spec.splitBrainPolicy.sitePriorities",
					"winner", winner, "fencedSite", loser)
			}
			err := withMySQLSafetyTimeout(ctx, func(probeCtx context.Context) error {
				return site.mysql.SetSuperReadOnly(probeCtx, true)
			})
			if err != nil {
				if quiet {
					tm.logger.Debug("failed to fence non-preferred site", "site", loser, "error", err)
				} else {
					tm.logger.Error("failed to fence non-preferred site", "site", loser, "error", err)
				}
			}
		}
		return attempted
	}

	attempted := false
	for i := range tm.sites {
		if tm.sites[i].name != keep && tm.sites[i].state == state.StateWritable {
			attempted = true
			break
		}
	}
	if attempted {
		tm.mu.Lock()
		quiet := !tm.lastFenceRetryLog.IsZero() && tm.clock.Since(tm.lastFenceRetryLog) < 30*time.Second
		if !quiet {
			tm.lastFenceRetryLog = tm.clock.Now()
		}
		tm.mu.Unlock()
		tm.fenceSitesExcept(ctx, keep, quiet)
	}
	return attempted
}

// previousPrimary returns the name of the most likely "old primary" —
// the last failover target if it is different from newPrimary, or
// otherwise the first writable site other than newPrimary. Returns ""
// if there is no plausible old primary to fence.
func (tm *TopologyManager) previousPrimary(newPrimary string) string {
	tm.mu.RLock()
	defer tm.mu.RUnlock()
	if tm.lastFailoverTarget != "" && tm.lastFailoverTarget != newPrimary {
		return tm.lastFailoverTarget
	}
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
// confirms the target is writable, flips DNS to the target's LB IP,
// and updates the in-memory
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
	tm.mu.RLock()
	blocked := tm.keyringRotationBlockedLocked(target)
	tm.mu.RUnlock()
	if blocked {
		return "", fmt.Errorf("%w: site %q", errKeyringRotationBlocked, target)
	}

	var sourceChecker mysql.Checker
	if source != "" {
		if src := tm.getSite(source); src != nil {
			sourceChecker = src.mysql
		}
	}

	promotionGtid, err := tm.failover.Execute(ctx, targetSite.mysql, sourceChecker, target)
	if err != nil {
		return "", err
	}
	if err := tm.confirmWritable(ctx, targetSite); err != nil {
		return "", fmt.Errorf("promotion succeeded but writable confirmation failed: %w", err)
	}

	// Record the failover-tracking state before the DNS flip: the target
	// is writable now, so a DNS-provider outage must not erase the
	// cooldown / split-brain / status-GTID fields the reconciler relies
	// on. The DNS flip below is best-effort for state tracking; on
	// failure we still return the promotion GTID (not "") so the caller
	// can surface what was promoted rather than mistaking a stale-DNS
	// condition for a failed promotion.
	tm.recordFailover(ctx, tm.clock.Now(), target, promotionGtid)

	if err := tm.applyDNS(ctx, target, targetSite.lbIP); err != nil {
		return promotionGtid, fmt.Errorf("DNS flip failed after successful promotion: %w", err)
	}

	return promotionGtid, nil
}

// errSiteNotFound is returned by the planned-failover primitives when a
// name does not match any configured site. Sentinel so callers can
// decide whether to retry (transient lookup failure is impossible — the
// topology manager only forgets sites when it restarts).
var errSiteNotFound = fmt.Errorf("planned-failover: site not found in topology manager")

// KeyringGate is consulted before a clone into a site that may be
// running encryption-at-rest. RequestKeyringUnseal returns true when the
// site's keyring is already writable and the clone may proceed; false
// means the operator has recorded the unseal request and the caller
// should retry after the pod has rolled. NotifyCloneComplete releases
// the Clone hold after bootstrap finishes so the site can reseal.
//
// Implemented by MysqlFailoverGroupReconciler. Nil in tests that do not
// exercise the gate. RequestKeyringUnseal is a no-op when encryption is
// disabled, so production always wires the gate.
type KeyringGate interface {
	RequestKeyringUnseal(ctx context.Context, nn types.NamespacedName, site string) (bool, error)
	NotifyCloneComplete(ctx context.Context, nn types.NamespacedName, site string) error
}

// SetKeyringGate wires the encryption-at-rest clone gate. Safe to call
// with nil to disable gating.
func (tm *TopologyManager) SetKeyringGate(g KeyringGate) {
	tm.mu.Lock()
	tm.keyringGate = g
	tm.mu.Unlock()
}

func (tm *TopologyManager) getKeyringGate() KeyringGate {
	tm.mu.RLock()
	defer tm.mu.RUnlock()
	return tm.keyringGate
}

// SetRecloneSite requests that the given site be recloned from the current
// primary. Called by the runner when it detects the reclone annotation.
// The topology manager processes this during the next poll cycle.
func (tm *TopologyManager) SetRecloneSite(site string) bool {
	tm.mu.Lock()
	defer tm.mu.Unlock()
	if tm.reclonePendingSite == site || tm.recloneCompletedSite == site {
		return false
	}
	tm.reclonePendingSite = site
	return true
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

// bootstrapBlocksCrossSite reports whether the in-flight bootstrap (if
// any) is one that must suppress cross-site actions.
//
// A clone into a promotable or dr-only site does suppress: that site is
// part of the replication topology the failover decision reasons about,
// it goes unreachable across the CLONE INSTANCE restart, and promoting
// or fencing around it mid-clone risks diverging data.
//
// A clone into a non-promotable reader does NOT suppress. A reader can
// never be a failover target, so a reader clone carries no information
// about where the primary should live. Suppressing on it means losing a
// reader's disk can block promoting a new primary — the group stays in
// NoPrimary with no writable site for as long as the reader takes to
// clone, which for a large dataset is unbounded. Readers are already
// treated as non-blocking elsewhere for the same reason: detectEmptySite
// deliberately skips readers in its reachability guard so "a missing
// reader must not block recovery of a promotable site".
func (tm *TopologyManager) bootstrapBlocksCrossSite() bool {
	if !tm.isBootstrapping() {
		return false
	}
	tm.mu.RLock()
	recipient := tm.bootstrapRecipient
	tm.mu.RUnlock()
	if recipient == "" {
		// Recipient unknown (older state or a bootstrap started before
		// this field was tracked): fall back to the conservative
		// behaviour of suppressing.
		return true
	}
	site := tm.getSite(recipient)
	if site == nil {
		return true
	}
	// Only an explicit read-only reader is exempt. Any other role — including
	// one this build does not recognise — keeps the conservative behaviour,
	// so a misconfigured or newly-introduced role cannot silently open the
	// cross-site path during a topology-relevant clone.
	return site.role != state.SiteRoleReadOnly
}

// cloningReaderSite returns the name of the non-promotable reader whose
// in-flight clone the current poll cycle is deliberately tolerating, or ""
// when no such clone is running.
//
// Callers that already passed bootstrapBlocksCrossSite use this to exclude
// that one site from peer checks which would otherwise re-block the very
// path the carve-out exists to keep open — a reader is unreachable across
// its CLONE INSTANCE restart and its GTID set is unreadable while the clone
// overwrites it. The exclusion is deliberately scoped to the single site
// being cloned, and only while that clone is in flight: every other site,
// and that same reader at any other time, is checked exactly as before.
func (tm *TopologyManager) cloningReaderSite() string {
	if !tm.isBootstrapping() {
		return ""
	}
	tm.mu.RLock()
	recipient := tm.bootstrapRecipient
	tm.mu.RUnlock()
	if recipient == "" {
		return ""
	}
	site := tm.getSite(recipient)
	if site == nil || site.role != state.SiteRoleReadOnly {
		return ""
	}
	return recipient
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

	tm.mu.RLock()
	activeName := tm.activeSiteLocked()
	driftSites := append([]string(nil), tm.specDriftSites...)
	tm.mu.RUnlock()
	if activeName == "" {
		return false
	}
	if len(driftSites) == 0 {
		return false
	}
	drifted := make(map[string]bool, len(driftSites))
	for _, name := range driftSites {
		drifted[name] = true
	}
	activeSite := tm.getSite(activeName)
	active := UpdateTarget{
		Name: activeName, Host: activeSite.host, Checker: activeSite.mysql, Promotable: true, Drifted: drifted[activeName],
		ReplUser: tm.bootstrapCfg.ReplUser, ReplPassword: tm.bootstrapCfg.ReplPassword, UseSSL: tm.bootstrapCfg.UseSSL,
	}
	followers := make([]UpdateTarget, 0, len(tm.sites)-1)
	for i := range tm.sites {
		site := &tm.sites[i]
		if site.name == activeName {
			continue
		}
		tm.mu.RLock()
		rotationBlocked := tm.keyringRotationBlockedLocked(site.name)
		tm.mu.RUnlock()
		followers = append(followers, UpdateTarget{
			Name: site.name, Host: site.host, Checker: site.mysql, Promotable: site.isPromotable(),
			Drifted: drifted[site.name], ExpectedSource: activeSite.host,
			RotationBlocked: rotationBlocked,
		})
	}
	if drifted[activeName] {
		if tm.bootstrapCfg.ReplUser == "" {
			return false
		}
		haveStandby := false
		for i := range tm.sites {
			if tm.sites[i].name == activeName || !tm.sites[i].isPromotable() || !tm.sites[i].isHealthyReplica() {
				continue
			}
			tm.mu.RLock()
			blocked := tm.keyringRotationBlockedLocked(tm.sites[i].name)
			tm.mu.RUnlock()
			if blocked {
				continue
			}
			haveStandby = true
			break
		}
		if !haveStandby {
			return false
		}
	}

	tm.logger.Info("ordered update: spec drift detected, starting ordered update",
		"driftSites", driftSites, "active", activeName)

	applyUpdate := tm.ApplyUpdate
	go func() {
		processed, err := tm.updater.ExecuteTargets(ctx, active, followers, applyUpdate, func(target, promotionGTID string) {
			targetSite := tm.getSite(target)
			tm.recordFailover(ctx, tm.clock.Now(), target, promotionGTID)
			metrics.FailoversTotal.WithLabelValues(target).Inc()
			if targetSite != nil {
				if dnsErr := tm.applyDNS(ctx, target, targetSite.lbIP); dnsErr != nil {
					tm.logger.Error("ordered update: DNS flip failed after handoff", "site", target, "error", dnsErr)
				}
			}
		})
		if err != nil {
			tm.logger.Error("ordered update failed", "error", err)
		}
		tm.mu.Lock()
		completed := make(map[string]bool, len(processed))
		for _, name := range processed {
			completed[name] = true
		}
		remaining := tm.specDriftSites[:0]
		for _, name := range tm.specDriftSites {
			if !completed[name] {
				remaining = append(remaining, name)
			}
		}
		tm.specDriftSites = remaining
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
	tm.StatusCallback(tm.buildSnapshot(siteRepl))
}

// SetSpecDriftSites records which sites have spec drift (Deployment hash != desired hash).
// Called by the runner after detecting drift.
func (tm *TopologyManager) SetSpecDriftSites(sites []string) {
	tm.mu.Lock()
	defer tm.mu.Unlock()
	tm.specDriftSites = sites
}

// isFreshDeploy reports whether every site is writable, none has ever had
// replication configured, AND none holds data. This is the signature of a
// fresh deployment — as opposed to a "true" split-brain where at least one
// site previously had replication set up and may now hold diverged writes.
//
// The emptiness requirement is load-bearing, not cosmetic: a populated
// cluster can reach the all-writable, no-metadata state through restart
// amnesia — a failover whose status write was rejected, an operator restart
// that rehydrated the stale CR (no lastFailoverTarget), and an old primary
// that respawned writable. The promoted primary's own RESET REPLICA ALL
// erased its channel metadata, so metadata absence alone cannot distinguish
// that cluster from a fresh one — and treating it as fresh would seed by
// sitePriorities and CLONE the newer side from the stale one, destroying
// every post-failover write. A site with user schemas (or a non-empty GTID
// set when the schema check is unavailable) is never part of a fresh deploy.
func (tm *TopologyManager) isFreshDeploy(ctx context.Context) bool {
	for i := range tm.sites {
		if tm.sites[i].state != state.StateWritable {
			return false
		}
	}
	for i := range tm.sites {
		// Every probe is bounded: this runs on the split-brain retry path, so a
		// single stalled MySQL must not hold the poll (and every site behind it)
		// hostage. Same discipline as detectEmptySite.
		var rs *mysql.ReplicaStatus
		err := withMySQLSafetyTimeout(ctx, func(probeCtx context.Context) error {
			var probeErr error
			rs, probeErr = tm.sites[i].mysql.ShowReplicaStatus(probeCtx)
			return probeErr
		})
		if err != nil {
			tm.logger.Warn("fresh-deploy check: could not read replica status", "site", tm.sites[i].name, "error", err)
			return false
		}
		if rs != nil {
			return false
		}
		var raw string
		err = withMySQLSafetyTimeout(ctx, func(probeCtx context.Context) error {
			var probeErr error
			raw, probeErr = tm.sites[i].mysql.GetGtidExecuted(probeCtx)
			return probeErr
		})
		if err != nil {
			tm.logger.Warn("fresh-deploy check: could not read GTID set", "site", tm.sites[i].name, "error", err)
			return false
		}
		gtid, err := mysql.ParseGTIDSet(raw)
		if err != nil {
			return false
		}
		hasData := !gtid.IsEmpty()
		if checker, ok := tm.sites[i].mysql.(userSchemaChecker); ok {
			var hs bool
			schemaErr := withMySQLSafetyTimeout(ctx, func(probeCtx context.Context) error {
				var probeErr error
				hs, probeErr = checker.HasUserSchemas(probeCtx)
				return probeErr
			})
			if schemaErr == nil {
				hasData = hs
			}
		}
		if hasData {
			// Poll-frequency path during a split-brain, so Debug — but the
			// refusal must be discoverable when someone asks why a deploy
			// did not bootstrap.
			tm.logger.Debug("fresh-deploy check: site holds data; refusing fresh-deploy bootstrap (restart amnesia or populated cluster)",
				"site", tm.sites[i].name)
			return false
		}
	}
	return true
}

type userSchemaChecker interface {
	HasUserSchemas(context.Context) (bool, error)
}

// detectEmptySite looks for exactly one donor/recipient pair where a
// single site is reachable but has no replication metadata and a genuinely
// fresh datadir. A freshly initialized MySQL datadir may have local GTIDs
// from setup statements, so GTID emptiness alone is not a reliable
// emptiness signal — and neither is "no user schemas" alone: a cluster that
// has never created one (or dropped its last) still has returning members
// that share the cluster's GTID UUIDs and must not be cloned over. Schema
// absence is consulted only when the site's GTIDs share no UUID with the
// donor. Works for any number of sites; the donor is the writable site, and
// the recipient is any reachable empty site. When multiple sites are empty
// the operator clones one per poll cycle. Returns ("", "") when no eligible
// pair exists.
func (tm *TopologyManager) detectEmptySite(ctx context.Context) (donor, empty string) {
	active, err := tm.confirmedActivePrimary(ctx)
	if err != nil {
		// A freshly recreated empty candidate starts writable. Identify and
		// fence that empty recipient before establishing unique donor authority.
		return tm.detectAndFenceEmptyWritableSite(ctx)
	}

	// Preserve the core-site reachability guard, but a missing reader must not
	// block recovery of a promotable or dr-only site.
	for i := range tm.sites {
		if tm.sites[i].role == state.SiteRoleReadOnly {
			continue
		}
		switch tm.sites[i].state {
		case state.StateUnreachable, state.StateUnknown:
			return "", ""
		}
	}

	// Candidates are populated before dr-only sites, and readers last.
	for _, role := range []state.SiteRole{state.SiteRolePrimaryCandidate, state.SiteRoleDROnly, state.SiteRoleReadOnly} {
		for i := range tm.sites {
			site := &tm.sites[i]
			if site.name == active.name || site.role != role ||
				(site.state != state.StateWritable && site.state != state.StateReadOnly) {
				continue
			}
			// A site already under recovery must never be auto-cloned: the
			// divergence comparison (or in-progress rejoin) owns it. Without
			// this guard a RecoveryBlocked schemaless member would be wiped
			// on the same poll that correctly reported the block.
			tm.mu.RLock()
			rec := tm.recovery[site.name]
			inRecovery := rec != nil && (rec.state == recoveryStateBlocked || rec.state == recoveryStateInProgress)
			tm.mu.RUnlock()
			if inRecovery {
				continue
			}
			var rs *mysql.ReplicaStatus
			var gtid mysql.GTIDSet
			var hasSchemas bool
			probeErr := withMySQLSafetyTimeout(ctx, func(probeCtx context.Context) error {
				var err error
				rs, err = site.mysql.ShowReplicaStatus(probeCtx)
				if err != nil {
					return err
				}
				raw, err := site.mysql.GetGtidExecuted(probeCtx)
				if err != nil {
					return err
				}
				gtid, err = mysql.ParseGTIDSet(raw)
				if err != nil {
					return err
				}
				hasSchemas = !gtid.IsEmpty()
				if checker, ok := site.mysql.(userSchemaChecker); ok {
					hasSchemas, err = checker.HasUserSchemas(probeCtx)
				}
				return err
			})
			if probeErr != nil {
				if site.role == state.SiteRoleReadOnly {
					continue
				}
				return "", ""
			}
			// Mirror initiateRecovery's empty-datadir discriminator: schema
			// absence only counts for a read-only site whose GTID UUIDs are
			// foreign to the donor. sharesHistory fails safe toward "not
			// empty" on donor probe failure so an unreachable primary cannot
			// open a clone-over window. The ReadOnly gate matches the prior
			// freshInitialized check so a writable anomaly is not silently
			// adopted as a clone recipient.
			emptyDatadir := gtid.IsEmpty()
			if !emptyDatadir && site.state == state.StateReadOnly && !tm.sharesHistory(ctx, gtid, active) {
				emptyDatadir = !hasSchemas
			}
			if rs == nil && emptyDatadir {
				return active.name, site.name
			}
		}
	}
	return "", ""
}

func (tm *TopologyManager) detectAndFenceEmptyWritableSite(ctx context.Context) (string, string) {
	var donor, recipient *siteTracker
	for i := range tm.sites {
		site := &tm.sites[i]
		if site.state != state.StateWritable || site.role == state.SiteRoleReadOnly {
			continue
		}
		var rs *mysql.ReplicaStatus
		var hasSchemas bool
		err := withMySQLSafetyTimeout(ctx, func(probeCtx context.Context) error {
			var err error
			rs, err = site.mysql.ShowReplicaStatus(probeCtx)
			if err != nil || rs != nil {
				return err
			}
			raw, err := site.mysql.GetGtidExecuted(probeCtx)
			if err != nil {
				return err
			}
			gtid, err := mysql.ParseGTIDSet(raw)
			if err != nil {
				return err
			}
			hasSchemas = !gtid.IsEmpty()
			if checker, ok := site.mysql.(userSchemaChecker); ok {
				hasSchemas, err = checker.HasUserSchemas(probeCtx)
			}
			return err
		})
		if err != nil || rs != nil {
			return "", ""
		}
		if hasSchemas {
			if donor != nil || !site.isPromotable() {
				return "", ""
			}
			donor = site
		} else {
			if recipient != nil {
				return "", ""
			}
			recipient = site
		}
	}
	if donor == nil || recipient == nil || donor.host == "" {
		return "", ""
	}
	var readOnly bool
	err := withMySQLSafetyTimeout(ctx, func(probeCtx context.Context) error {
		if err := recipient.mysql.SetSuperReadOnly(probeCtx, true); err != nil {
			return err
		}
		var err error
		readOnly, err = recipient.mysql.CheckReadOnly(probeCtx)
		return err
	})
	if err != nil || !readOnly || tm.confirmWritable(ctx, donor) != nil {
		return "", ""
	}
	return donor.name, recipient.name
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
	seedSite := tm.getSite(seed)
	for i := range tm.sites {
		if tm.sites[i].name == seed {
			continue
		}
		var readOnly bool
		err := withMySQLSafetyTimeout(ctx, func(probeCtx context.Context) error {
			if err := tm.sites[i].mysql.SetSuperReadOnly(probeCtx, true); err != nil {
				return err
			}
			var err error
			readOnly, err = tm.sites[i].mysql.CheckReadOnly(probeCtx)
			return err
		})
		if err != nil {
			tm.logger.Error("fresh-deploy bootstrap: failed to fence non-seed site", "site", tm.sites[i].name, "error", err)
			return
		}
		if !readOnly {
			tm.logger.Error("fresh-deploy bootstrap: non-seed fence not confirmed", "site", tm.sites[i].name, "error", err)
			return
		}
	}
	if seedSite == nil || !seedSite.isPromotable() || seedSite.host == "" || tm.confirmWritable(ctx, seedSite) != nil {
		tm.logger.Error("fresh-deploy bootstrap: seed authority could not be confirmed", "site", seed)
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
func (tm *TopologyManager) startBootstrapByName(ctx context.Context, donor, recipient, source string) bool {
	donorSite := tm.getSite(donor)
	recipientSite := tm.getSite(recipient)
	if donorSite == nil || recipientSite == nil {
		tm.logger.Error("bootstrap aborted: unknown site", "donor", donor, "recipient", recipient)
		return false
	}
	if donorSite.host == "" {
		err := fmt.Errorf("bootstrap: donor host not configured for site %s", donor)
		tm.mu.Lock()
		tm.bootstrapPhase = BootstrapPhaseFailed
		tm.bootstrapErr = err
		tm.mu.Unlock()
		tm.emitBootstrapStatus()
		return false
	}
	if source != "fresh-deploy" {
		confirmed, err := tm.confirmedActivePrimary(ctx)
		if err != nil || confirmed.name != donorSite.name {
			tm.logger.Error("bootstrap aborted: donor is not the confirmed active primary", "donor", donor, "error", err)
			return false
		}
	}

	// Encryption-at-rest: a clone recipient must have a writable keyring
	// before CLONE INSTANCE runs. Deferring here (rather than failing)
	// is deliberate — the reconciler unseals the site and rolls its pod,
	// and the next poll cycle picks the bootstrap back up.
	nn := types.NamespacedName{Namespace: tm.cfg.Namespace, Name: tm.cfg.Name}
	if gate := tm.getKeyringGate(); gate != nil {
		ready, err := gate.RequestKeyringUnseal(ctx, nn, recipient)
		if err != nil {
			tm.logger.Error("bootstrap deferred: keyring unseal request failed",
				"recipient", recipient, "error", err)
			return false
		}
		if !ready {
			tm.logger.Info("bootstrap deferred: waiting for the recipient keyring to be unsealed",
				"recipient", recipient, "source", source)
			return false
		}
	}
	tm.mu.Lock()
	tm.bootstrapPhase = BootstrapPhaseCloning
	tm.bootstrapErr = nil
	tm.bootstrapSource = source
	tm.bootstrapRecipient = recipient
	// A leftover release from a previous attempt must not fire while
	// this clone is running — that would reseal the pod mid-CLONE.
	if tm.cloneHoldReleaseSite == recipient {
		tm.cloneHoldReleaseSite = ""
	}
	tm.mu.Unlock()

	tm.logger.Info("starting bootstrap",
		"source", source,
		"donor", donor,
		"recipient", recipient,
		"donorHost", donorSite.host)

	tm.emitBootstrapStatus()

	go func() {
		allowSkipClone := source != "reclone"
		var err error
		if source != "fresh-deploy" {
			confirmed, confirmErr := tm.confirmedActivePrimary(ctx)
			if confirmErr != nil {
				err = fmt.Errorf("bootstrap donor authority lost before clone: %w", confirmErr)
			} else if confirmed.name != donorSite.name {
				err = fmt.Errorf("bootstrap donor authority moved from %s to %s", donorSite.name, confirmed.name)
			}
		}
		if err == nil {
			err = tm.runBootstrap(ctx, donorSite.mysql, recipientSite.mysql, donorSite.host, recipient, allowSkipClone)
		}
		if gate := tm.getKeyringGate(); gate != nil {
			notifyCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 30*time.Second)
			notifyErr := gate.NotifyCloneComplete(notifyCtx, nn, recipient)
			cancel()
			if notifyErr != nil {
				tm.logger.Error("could not release clone keyring hold",
					"recipient", recipient, "error", notifyErr)
				tm.mu.Lock()
				tm.cloneHoldReleaseSite = recipient
				tm.mu.Unlock()
			}
		}
		var recloneCallbackErr error
		if source == "reclone" && tm.RecloneCompleteCallback != nil {
			if callbackErr := tm.RecloneCompleteCallback(ctx, recipient); callbackErr != nil {
				tm.logger.Error("could not consume reclone annotation after bootstrap attempt",
					"recipient", recipient, "error", callbackErr)
				recloneCallbackErr = callbackErr
			}
		}
		tm.mu.Lock()
		if source == "reclone" && tm.reclonePendingSite == recipient {
			tm.reclonePendingSite = ""
		}
		if source == "reclone" && err == nil && recloneCallbackErr != nil {
			tm.recloneCompletedSite = recipient
		}
		if err != nil {
			tm.bootstrapPhase = BootstrapPhaseFailed
			tm.bootstrapErr = err
			tm.logger.Error("bootstrap failed", "source", source, "error", err)
		} else {
			tm.bootstrapPhase = BootstrapPhaseDone
			recipientSite.sourceHost = ""
			recipientSite.sourceConvergenceState = ""
			recipientSite.sourceConvergenceReason = ""
			recipientSite.servingHealthy = false
			tm.logger.Info("bootstrap completed successfully", "source", source)
		}
		tm.mu.Unlock()
		tm.emitBootstrapStatus()
	}()
	return true
}

// runBootstrap performs the clone, waits for the MySQL restart, and
// sets up replication of recipient from donor.
func (tm *TopologyManager) runBootstrap(ctx context.Context, primary, replica mysql.Checker, primaryHost, replicaSite string, allowSkipClone bool) error {
	// Check if the clone already completed (e.g. prior bootstrap succeeded at
	// CLONE but failed at SetupReplication). If the primary's GTID set contains
	// the replica's, the data is already in sync and we can skip directly to
	// replication setup.
	if allowSkipClone && tm.canSkipClone(ctx, primary, replica) {
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

// canSkipClone returns true when the donor/primary GTID set contains the
// recipient/replica GTID set, meaning the recipient is equal to or behind the
// donor and cannot contain extra divergent transactions. Recipient supersets,
// disjoint sets, empty sets, and malformed sets force a destructive clone.
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
	return pGtid.Contains(rGtid)
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

// checkRecovery detects an old primary that has come back after a
// failover and either auto-rejoins it (no divergence) or blocks with
// metadata (divergence detected). For N sites we scan every read-only
// non-active site and pick the first one that needs recovery; multiple
// divergent sites are reported sequentially across poll cycles.
// Returns true if recovery state changed this cycle.
func (tm *TopologyManager) checkRecovery(ctx context.Context, siteRepl []*mysql.ReplicaStatus) bool {
	return tm.checkRecoveryWithConvergence(ctx, siteRepl, nil)
}

func (tm *TopologyManager) checkRecoveryWithConvergence(ctx context.Context, siteRepl []*mysql.ReplicaStatus, convergenceHandled map[string]struct{}) bool {
	if tm.isBootstrapping() {
		return false
	}
	if tm.isTopologyFrozen() {
		return false
	}
	if tm.clearHealthyRecoverySites(ctx, siteRepl) {
		return true
	}
	if tm.bootstrapCfg.ReplUser == "" {
		return false
	}
	// Deliberately NOT gated on lastFailoverTarget: a primary can change
	// hands without a recorded failover — e.g. the old primary's pod crashed
	// and came back fenced while a replica respawned writable and was adopted
	// as the de-facto primary, or the failover record was lost to a status-
	// write outage plus operator restart. The orphaned ex-primary (read-only,
	// no replication metadata) still needs recovery. Safety comes from the
	// gates below, not from history: a unique directly-confirmed writable
	// primary must exist, empty sites are left to bootstrap/auto-clone, and
	// the GTID comparison in initiateRecovery blocks on any divergence.

	// Recovery is destructive to replication metadata, so it uses the same
	// unique, directly confirmed, promotable authority gate as convergence and
	// clone/reclone paths.
	active, err := tm.confirmedActivePrimary(ctx)
	if err != nil {
		return false
	}
	activeIdx := -1
	for i := range tm.sites {
		if &tm.sites[i] == active {
			activeIdx = i
			break
		}
	}
	if activeIdx < 0 {
		return false
	}

	// Scan non-active sites for one that looks like an old primary.
	for i := range tm.sites {
		if i == activeIdx {
			continue
		}
		other := &tm.sites[i]
		if _, handled := convergenceHandled[other.name]; handled {
			continue
		}
		if other.state != state.StateReadOnly {
			continue
		}
		var repl *mysql.ReplicaStatus
		if i < len(siteRepl) {
			repl = siteRepl[i]
		}
		if repl != nil && replicaStatusHealthy(repl) {
			continue
		}
		// Rate-limit per site: a blocked divergence report is re-verified on
		// the recovery retry cadence rather than frozen forever — the site
		// can diverge *further* after the report was recorded (e.g. it
		// respawned writable, took a few writes, and was re-fenced), and a
		// stale status.divergentGtid under-reports what a human must extract
		// before discarding the site. The re-check also auto-recovers if the
		// divergence has since been resolved externally (the new primary now
		// contains the old one's set). RecoveryInProgress uses the same
		// stabilization window before retrying the idempotent sequence.
		// Copy the field, not the pointer: stampRecoveryBackoff mutates
		// retryAfter in place under tm.mu, so a *siteRecovery that escapes the
		// critical section is no longer mu-protected.
		tm.mu.RLock()
		var retryAfter time.Time
		if rec := tm.recovery[other.name]; rec != nil {
			retryAfter = rec.retryAfter
		}
		tm.mu.RUnlock()
		if !retryAfter.IsZero() && tm.clock.Now().Before(retryAfter) {
			continue
		}
		// Read-only with no active replication while another site is the
		// confirmed primary: start (or re-verify) recovery. One recovery
		// mutation per poll cycle; additional sites are picked up on
		// subsequent polls. A site that is merely SKIPPED (empty datadir,
		// failed pre-probe) must NOT consume the cycle — `continue`, so a
		// persistently-skipped site cannot starve a divergent site behind
		// it in scan order out of ever being reported.
		if changed, acted := tm.initiateRecovery(ctx, i, activeIdx, siteRepl); acted {
			return changed
		}
	}
	return false
}

// checkPrimaryReassert detects and heals the "promoted primary got
// fenced back to read-only and nothing is writable" wedge.
//
// After a successful promotion the new primary's own sidecar can
// re-fence it: the sidecar's fencing lease may still be stale when the
// promotion lands — e.g. the operator restarted after a full-site
// outage and promoted before its auxiliary Service endpoint became
// Ready, so the sidecar's operator probes kept failing while the
// operator was already driving MySQL. The group then settles with
// every site reachable and read-only. EvalCrossSite deliberately
// refuses to elect a primary from that state (all-read-only without
// history is a startup condition that needs human input) and
// checkRecovery requires a writable active site, so nothing restores
// the target and the group stays wedged until a human runs SET GLOBAL
// read_only=OFF.
//
// The topology manager, however, has the history the pure state matrix
// refuses to assume: lastFailoverTarget names the site this operator
// (or a predecessor, via status rehydration) made authoritative. When
// that site is reachable, read-only, and promotable, every other site
// is also reachable and read-only, and the target is GTID-complete (it
// contains every peer's GTID_EXECUTED and the recorded promotion GTID
// set), restoring its writability cannot lose transactions or create a
// second primary. Any unreachable site instead leaves the decision to
// the normal unreachable+read-only promotion path in EvalCrossSite.
//
// Called once per poll cycle. Returns true when a re-assert was
// attempted (successful or not) so Poll surfaces the attempt through
// the status callback; attempts are rate-limited by failoverCooldown.
func (tm *TopologyManager) checkPrimaryReassert(ctx context.Context) bool {
	// A clone into a non-promotable reader must not hold this wedge-healing
	// path shut: re-asserting the promoted primary's writability is exactly
	// what rescues a group stuck with no writable site, and a reader clone
	// says nothing about where the primary belongs. See
	// bootstrapBlocksCrossSite.
	if tm.bootstrapBlocksCrossSite() || tm.isUpdating() || tm.isTopologyFrozen() || tm.isPlannedFailoverActive() {
		return false
	}

	tm.mu.RLock()
	target := tm.lastFailoverTarget
	promotedSite := tm.promotedSite
	lastReassert := tm.lastReassert
	promotionGtidStr := tm.promotionGtidExecuted
	tm.mu.RUnlock()

	if target == "" {
		return false
	}
	// A pending promotion is still being confirmed by
	// reconcilePendingPromotionLocked; let that pipeline finish (or
	// expire) before second-guessing the topology.
	if promotedSite != "" {
		return false
	}
	if !lastReassert.IsZero() && tm.clock.Since(lastReassert) < tm.failoverCooldown {
		return false
	}

	// The one reader whose clone this cycle tolerates is excluded from the
	// peer checks below: it is unreachable across its CLONE INSTANCE restart
	// and its GTID set is unreadable mid-clone, so consulting it would
	// re-block the wedge-healing path the carve-out exists to keep open. Any
	// errant transactions it held are being overwritten by the clone anyway.
	skipCloningReader := tm.cloningReaderSite()

	var targetSite *siteTracker
	for i := range tm.sites {
		s := &tm.sites[i]
		if s.name == target {
			targetSite = s
			continue
		}
		if s.name == skipCloningReader {
			continue
		}
		// A writable site means there is no wedge (split-brain repair
		// belongs to applyCrossSiteAction); an unreachable or unknown
		// site hands the decision to the normal promotion path.
		if s.state != state.StateReadOnly {
			return false
		}
	}
	if targetSite == nil || targetSite.state != state.StateReadOnly || !targetSite.isPromotable() {
		return false
	}
	tm.mu.RLock()
	targetBlocked := tm.keyringRotationBlockedLocked(target)
	tm.mu.RUnlock()
	if targetBlocked {
		tm.logger.Warn("primary re-assert refused: target is mid-keyring-rotation",
			"site", target, "unsealReason", "Rotation")
		tm.incrementKeyringPromotionBlocked([]string{target}, "refused")
		return false
	}

	gtidCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	targetGtidStr, err := targetSite.mysql.GetGtidExecuted(gtidCtx)
	cancel()
	if err != nil {
		tm.logger.Warn("primary re-assert: failed to read target GTID_EXECUTED", "site", target, "error", err)
		return false
	}
	targetGtid, err := mysql.ParseGTIDSet(targetGtidStr)
	if err != nil {
		tm.logger.Warn("primary re-assert: failed to parse target GTID_EXECUTED", "site", target, "error", err)
		return false
	}
	if promotionGtidStr != "" {
		promotionGtid, perr := mysql.ParseGTIDSet(promotionGtidStr)
		if perr != nil {
			// The operator wrote this value from MySQL itself, so a parse
			// failure means corruption or manual status tampering. The
			// safety argument depends on the recorded invariant being
			// trustworthy — refuse rather than silently skip the gate.
			tm.logger.Warn("primary re-assert refused: recorded promotion GTID set failed to parse — status corrupted or manually edited?",
				"site", target, "promotionGtid", promotionGtidStr, "error", perr)
			return false
		}
		if !targetGtid.Contains(promotionGtid) {
			tm.logger.Warn("primary re-assert refused: target no longer contains the recorded promotion GTID set (wiped or restored since promotion?)",
				"site", target, "promotionGtid", promotionGtidStr, "targetGtid", targetGtidStr)
			return false
		}
	}
	for i := range tm.sites {
		s := &tm.sites[i]
		if s.name == target || s.name == skipCloningReader {
			continue
		}
		peerCtx, peerCancel := context.WithTimeout(ctx, 5*time.Second)
		peerGtidStr, gerr := s.mysql.GetGtidExecuted(peerCtx)
		peerCancel()
		if gerr != nil {
			tm.logger.Warn("primary re-assert: failed to read peer GTID_EXECUTED", "site", s.name, "error", gerr)
			return false
		}
		peerGtid, gerr := mysql.ParseGTIDSet(peerGtidStr)
		if gerr != nil {
			tm.logger.Warn("primary re-assert: failed to parse peer GTID_EXECUTED", "site", s.name, "error", gerr)
			return false
		}
		if !targetGtid.Contains(peerGtid) {
			tm.logger.Warn("primary re-assert refused: peer has transactions the target lacks — divergence needs human review",
				"site", s.name, "peerGtid", peerGtidStr, "targetGtid", targetGtidStr)
			return false
		}
	}

	// Stamp the attempt before mutating MySQL so failures are also
	// rate-limited by the cooldown instead of retried at poll frequency.
	tm.mu.Lock()
	tm.lastReassert = tm.clock.Now()
	tm.mu.Unlock()

	tm.logger.Warn("re-asserting fenced promoted primary: no site is writable and the last failover target is GTID-complete; restoring writability",
		"site", target)

	if err := targetSite.mysql.SetSuperReadOnly(ctx, false); err != nil {
		tm.logger.Error("primary re-assert: failed to clear super_read_only", "site", target, "error", err)
		return true
	}
	if err := targetSite.mysql.SetReadOnly(ctx, false); err != nil {
		tm.logger.Error("primary re-assert: failed to clear read_only", "site", target, "error", err)
		return true
	}
	if err := tm.confirmWritable(ctx, targetSite); err != nil {
		tm.logger.Error("primary re-assert: writable confirmation failed", "site", target, "error", err)
		return true
	}
	metrics.PrimaryReassertTotal.WithLabelValues(target).Inc()

	// Best-effort DNS flip: idempotent when DNS already points at the
	// target, and heals the case where the original promotion recorded
	// its state but the flip itself failed. A failure here is not fatal —
	// MySQL is writable again, and reconcileDNS re-derives the target from
	// live topology on every poll, so DNS catches up on its own once the
	// write is permitted.
	if err := tm.applyDNS(ctx, target, targetSite.lbIP); err != nil {
		tm.logger.Error("primary re-assert: DNS flip failed after restoring writability — the poll-driven DNS reconcile will keep retrying",
			"site", target, "error", err)
	}
	return true
}

// clearHealthyRecoverySites clears recovery markers whose site is now
// writable or is a healthy, source-converged replica. Before a recovered
// replica is marked done, the operator performs the post-fence drain that
// closes stale sessions surviving the sidecar's best-effort eviction. This
// also covers RecoveryInProgress restored after an operator restart.
func (tm *TopologyManager) clearHealthyRecoverySites(ctx context.Context, siteRepl []*mysql.ReplicaStatus) bool {
	cleared := false
	for i := range tm.sites {
		name := tm.sites[i].name
		tm.mu.RLock()
		rec := tm.recovery[name]
		var recState string
		if rec != nil {
			recState = rec.state
		}
		isRecordedTarget := name == tm.lastFailoverTarget
		tm.mu.RUnlock()
		if recState == "" {
			continue // probe-backoff marker, not a recovery state
		}
		if tm.sites[i].state == state.StateWritable {
			if recState == recoveryStateBlocked {
				// A remembered target is not authority by itself. Preserve the
				// divergence report through split-brain and only clear it when
				// this site is the unique, directly confirmed writable target.
				active, err := tm.confirmedActivePrimary(ctx)
				if !isRecordedTarget || err != nil || active.name != name {
					tm.mu.Lock()
					rec.drainStartedAt = time.Time{}
					rec.drainComplete = false
					tm.mu.Unlock()
					continue
				}
			}
			tm.mu.Lock()
			delete(tm.recovery, name)
			tm.mu.Unlock()
			metrics.DivergentTransactions.WithLabelValues(name).Set(0)
			tm.logger.Info("recovery state cleared (site is writable)", "site", name)
			cleared = true
			continue
		}
		if tm.sites[i].state != state.StateReadOnly {
			continue
		}
		if i >= len(siteRepl) || !replicaStatusHealthy(siteRepl[i]) {
			continue
		}
		if tm.sites[i].sourceConvergenceState != sourceConvergenceConverged {
			continue
		}
		if !tm.advanceSiteAppConnectionDrain(ctx, &tm.sites[i]) {
			continue
		}
		tm.mu.Lock()
		delete(tm.recovery, name)
		tm.mu.Unlock()
		metrics.DivergentTransactions.WithLabelValues(name).Set(0)
		tm.logger.Info("recovery state cleared (site is now replicating)", "site", name)
		cleared = true
	}
	return cleared
}

// advanceSiteAppConnectionDrain performs at most one eviction pass for a
// fenced site. State is retained in siteRecovery so the serialized poll loop
// remains free to observe failures and process failover thresholds between
// passes. It returns true after an empty pass or when the best-effort budget
// expires.
func (tm *TopologyManager) advanceSiteAppConnectionDrain(ctx context.Context, site *siteTracker) bool {
	timeout := time.Duration(tm.cfg.ConnectionDrainTimeout)
	if timeout <= 0 {
		timeout = defaultConnectionDrainTimeout
	}

	now := tm.clock.Now()
	tm.mu.Lock()
	rec := tm.recovery[site.name]
	if rec == nil {
		rec = &siteRecovery{}
		tm.recovery[site.name] = rec
	}
	if rec.drainComplete {
		tm.mu.Unlock()
		return true
	}
	if rec.drainStartedAt.IsZero() {
		rec.drainStartedAt = now
	}
	deadline := rec.drainStartedAt.Add(timeout)
	tm.mu.Unlock()

	remaining := deadline.Sub(now)
	if remaining <= 0 {
		tm.completeSiteAppConnectionDrain(site.name)
		tm.logger.Warn("post-fence application connection drain timed out",
			"site", site.name, "timeout", timeout, "fg", tm.cfg.Name)
		return true
	}
	attemptTimeout := min(5*time.Second, remaining)
	killCtx, cancelKill := context.WithTimeout(ctx, attemptTimeout)
	killed, err := site.mysql.KillAppConnections(killCtx)
	cancelKill()
	if err == nil && killed == 0 {
		tm.completeSiteAppConnectionDrain(site.name)
		tm.logger.Info("post-fence application connection drain complete",
			"site", site.name, "fg", tm.cfg.Name)
		return true
	}
	if err != nil {
		tm.logger.Warn("post-fence application connection drain retry failed",
			"site", site.name, "error", err, "fg", tm.cfg.Name)
	} else {
		tm.logger.Info("post-fence application connections evicted",
			"site", site.name, "count", killed, "fg", tm.cfg.Name)
	}
	if !tm.clock.Now().Before(deadline) {
		tm.completeSiteAppConnectionDrain(site.name)
		tm.logger.Warn("post-fence application connection drain timed out",
			"site", site.name, "timeout", timeout, "fg", tm.cfg.Name)
		return true
	}
	return false
}

func (tm *TopologyManager) completeSiteAppConnectionDrain(site string) {
	tm.mu.Lock()
	defer tm.mu.Unlock()
	if rec := tm.recovery[site]; rec != nil {
		rec.drainComplete = true
	}
}

func replicaStatusHealthy(repl *mysql.ReplicaStatus) bool {
	return repl != nil && repl.IORunning && repl.SQLRunning && repl.SourceHost != ""
}

// stampRecoveryBackoff records a probe backoff for a site whose recovery
// attempt failed before reaching a decision, preserving any existing state.
// A bare marker (state "") is invisible in status and exists only to
// rate-limit re-probing.
func (tm *TopologyManager) stampRecoveryBackoff(name string) {
	tm.mu.Lock()
	defer tm.mu.Unlock()
	if rec := tm.recovery[name]; rec != nil {
		rec.retryAfter = tm.clock.Now().Add(recoveryRetryDelay)
		return
	}
	tm.recovery[name] = &siteRecovery{retryAfter: tm.clock.Now().Add(recoveryRetryDelay)}
}

// sharesHistory reports whether gtid carries any GTID UUID the new primary
// also has — i.e. whether the site ever participated in this cluster's
// transaction history. A probe failure answers "shares" so an unreachable or
// slow new primary can never demote a returning old primary to a fresh-datadir
// verdict; recovery's own bounded comparison is the safe place to decide.
func (tm *TopologyManager) sharesHistory(ctx context.Context, gtid mysql.GTIDSet, newPrimary *siteTracker) bool {
	var raw string
	err := withMySQLSafetyTimeout(ctx, func(probeCtx context.Context) error {
		var probeErr error
		raw, probeErr = newPrimary.mysql.GetGtidExecuted(probeCtx)
		return probeErr
	})
	if err != nil {
		// Debug, not Warn: the conservative "shares" verdict lets the caller
		// proceed, and the caller's own read of the same GTID fails moments
		// later at ERROR with a probe backoff. One unreachable new primary
		// should produce one log line per cycle, not two.
		tm.logger.Debug("recovery: could not read new primary GTID for the fresh-datadir check",
			"site", newPrimary.name, "error", err)
		return true
	}
	newGtid, err := mysql.ParseGTIDSet(raw)
	if err != nil {
		return true
	}
	return gtid.HasCommonUUIDs(newGtid)
}

// initiateRecovery fences the old primary, compares GTID sets, and either
// auto-recovers (no divergence) or blocks with metadata (divergence).
//
// Returns (changed, acted): acted=false means the site was merely SKIPPED
// (empty datadir, pre-probe failure) without spending the one-recovery-
// mutation-per-poll budget — the caller continues scanning other sites, so a
// persistently-skipped site can never starve a divergent site behind it in
// scan order. changed=true means recovery state changed this cycle.
func (tm *TopologyManager) initiateRecovery(ctx context.Context, oldPrimaryIdx, newPrimaryIdx int, siteRepl []*mysql.ReplicaStatus) (changed, acted bool) {
	oldPrimary := &tm.sites[oldPrimaryIdx]
	newPrimary := &tm.sites[newPrimaryIdx]

	// An empty site is a bootstrap/auto-clone concern, not a returning old
	// primary: replicating it from scratch would depend on the donor
	// retaining every binlog since server init, which CLONE exists to avoid.
	// "Empty" matches detectEmptySite: GTID emptiness, or no user schemas
	// only when the site's GTID UUIDs share nothing with the new primary
	// (server-init noise under a brand-new server_uuid). Checked before the
	// defensive fence so pre-bootstrap empty sites are not mutated at poll
	// frequency.
	preGtidStr, preErr := oldPrimary.mysql.GetGtidExecuted(ctx)
	if preErr != nil {
		tm.logger.Error("recovery: failed to get old primary GTID", "site", oldPrimary.name, "error", preErr)
		// Back off rather than re-probing (and re-logging at ERROR) every poll.
		// The scan still advances past this site — acted=false — so a site that
		// cannot answer the pre-probe never starves a divergent site behind it.
		tm.stampRecoveryBackoff(oldPrimary.name)
		return false, false
	}
	empty := false
	if preGtid, parseErr := mysql.ParseGTIDSet(preGtidStr); parseErr == nil {
		empty = preGtid.IsEmpty()
		// A non-empty GTID set is only fresh-datadir noise when none of its
		// UUIDs appear in the new primary's history: server initialization
		// commits local transactions under a brand-new server_uuid, which is
		// why GTID emptiness alone cannot spot a wiped site. "No user schemas"
		// cannot stand in for it either — a cluster that has not created one
		// yet, or dropped its last, still has real old primaries. Taking the
		// schema answer unconditionally sent a returning old primary that
		// shared every cluster GTID down the auto-clone path, skipping the
		// divergence comparison that is the whole point of recovery: a
		// diverged site would have been cloned over instead of reported as
		// RecoveryBlocked. A returning member always shares the cluster's
		// UUIDs; a fresh datadir never does.
		if !empty && !tm.sharesHistory(ctx, preGtid, newPrimary) {
			if checker, ok := oldPrimary.mysql.(userSchemaChecker); ok {
				if hasSchemas, err := checker.HasUserSchemas(ctx); err == nil {
					empty = !hasSchemas
				}
			}
		}
	}
	if empty {
		return false, false
	}

	tm.mu.RLock()
	rec := tm.recovery[oldPrimary.name]
	drainStarted := rec != nil && (!rec.drainStartedAt.IsZero() || rec.drainComplete)
	tm.mu.RUnlock()
	if !drainStarted {
		tm.logger.Info("initiating old primary recovery", "oldPrimary", oldPrimary.name, "newPrimary", newPrimary.name)

		// Defensive fence. From here on the cycle's recovery budget is spent,
		// and every failure exit stamps a probe backoff so one persistently-
		// failing site cannot monopolize the budget at poll frequency.
		if err := oldPrimary.mysql.SetSuperReadOnly(ctx, true); err != nil {
			tm.logger.Error("recovery: failed to fence old primary", "site", oldPrimary.name, "error", err)
			tm.stampRecoveryBackoff(oldPrimary.name)
			return false, true
		}
	}

	// Promotion is complete before recovery is considered, and the defensive
	// fence above guarantees survivors cannot write. Each poll performs one
	// drain pass; GTID comparison and recovery mutation resume only after the
	// drain completes or its best-effort timeout expires.
	if !tm.advanceSiteAppConnectionDrain(ctx, oldPrimary) {
		return false, true
	}
	// Re-read after the fence: this is the authoritative set the divergence
	// comparison runs against (the fence guarantees it can no longer grow).
	oldGtidStr, err := oldPrimary.mysql.GetGtidExecuted(ctx)
	if err != nil {
		tm.logger.Error("recovery: failed to get old primary GTID", "site", oldPrimary.name, "error", err)
		tm.stampRecoveryBackoff(oldPrimary.name)
		return false, true
	}
	newGtidStr, err := newPrimary.mysql.GetGtidExecuted(ctx)
	if err != nil {
		tm.logger.Error("recovery: failed to get new primary GTID", "site", newPrimary.name, "error", err)
		tm.stampRecoveryBackoff(oldPrimary.name)
		return false, true
	}

	oldGtid, err := mysql.ParseGTIDSet(oldGtidStr)
	if err != nil {
		tm.logger.Error("recovery: failed to parse old primary GTID", "site", oldPrimary.name, "error", err)
		tm.stampRecoveryBackoff(oldPrimary.name)
		return false, true
	}
	newGtid, err := mysql.ParseGTIDSet(newGtidStr)
	if err != nil {
		tm.logger.Error("recovery: failed to parse new primary GTID", "site", newPrimary.name, "error", err)
		tm.stampRecoveryBackoff(oldPrimary.name)
		return false, true
	}

	if oldGtid.IsEmpty() {
		// Reached only after the defensive fence has already run, so without a
		// backoff the site would be re-fenced at poll frequency (the pre-probe
		// empty guard above is what normally keeps empty sites off this path).
		tm.stampRecoveryBackoff(oldPrimary.name)
		return false, false
	}

	if newGtid.Contains(oldGtid) {
		tm.logger.Info("no GTID divergence, auto-recovering old primary as replica", "site", oldPrimary.name)
		tm.mu.Lock()
		rec := tm.recovery[oldPrimary.name]
		if rec == nil {
			rec = &siteRecovery{}
			tm.recovery[oldPrimary.name] = rec
		}
		rec.state = recoveryStateInProgress
		rec.retryAfter = tm.clock.Now().Add(recoveryRetryDelay)
		rec.divergentGtid = ""
		rec.divergentCount = 0
		tm.mu.Unlock()
		metrics.DivergentTransactions.WithLabelValues(oldPrimary.name).Set(0)
		// Persist RecoveryInProgress before RecoverOldPrimary starts. The
		// poll-level callback also fires after initiateRecovery returns, but this
		// early write is the durable handoff for operator restarts that happen
		// inside the STOP/RESET/CHANGE/START sequence.
		if tm.StatusCallback != nil {
			tm.StatusCallback(tm.buildSnapshot(siteRepl))
		}
		tm.executeRecovery(ctx, oldPrimaryIdx, newPrimaryIdx)
		return true, true
	}

	divergent := oldGtid.Subtract(newGtid)
	count := divergent.TransactionCount()

	tm.mu.RLock()
	var prevState, prevDivergentGtid string
	if prev := tm.recovery[oldPrimary.name]; prev != nil {
		prevState, prevDivergentGtid = prev.state, prev.divergentGtid
	}
	tm.mu.RUnlock()
	sameReport := prevState == recoveryStateBlocked && prevDivergentGtid == divergent.String()
	// The periodic re-verification confirms an unchanged report every
	// recoveryRetryDelay; re-emitting the WARN each time would fill the log
	// with duplicates of a fact already surfaced. Warn on new or changed
	// divergence only.
	if !sameReport {
		tm.logger.Warn("divergence detected",
			"site", oldPrimary.name,
			"divergentTransactions", count,
			"divergentGtid", divergent.String(),
			"oldPrimaryGtid", oldGtidStr,
			"newPrimaryGtid", newGtidStr)
	}

	tm.mu.Lock()
	// Blocked reports are periodically re-verified (see
	// checkRecoveryWithConvergence) so further divergence refreshes the
	// report; rate-limit the re-check to the recovery retry cadence.
	rec = tm.recovery[oldPrimary.name]
	if rec == nil {
		rec = &siteRecovery{}
		tm.recovery[oldPrimary.name] = rec
	}
	rec.state = recoveryStateBlocked
	rec.retryAfter = tm.clock.Now().Add(recoveryRetryDelay)
	rec.divergentGtid = divergent.String()
	rec.divergentCount = count
	tm.mu.Unlock()

	metrics.DivergentTransactions.WithLabelValues(oldPrimary.name).Set(float64(count))
	return !sameReport, true
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

	metrics.DivergentTransactions.WithLabelValues(oldPrimary.name).Set(0)
	tm.logger.Info("old primary recovery complete", "site", oldPrimary.name, "source", newPrimaryHost)
}

// releaseCloneHold retries NotifyCloneComplete after a bootstrap whose
// release write failed. It never starts a clone.
func (tm *TopologyManager) releaseCloneHold(ctx context.Context) {
	tm.mu.RLock()
	site := tm.cloneHoldReleaseSite
	tm.mu.RUnlock()
	if site == "" {
		return
	}
	if tm.isBootstrapping() {
		tm.mu.RLock()
		recipient := tm.bootstrapRecipient
		tm.mu.RUnlock()
		if recipient == site {
			return
		}
	}
	gate := tm.getKeyringGate()
	if gate == nil {
		tm.mu.Lock()
		if tm.cloneHoldReleaseSite == site {
			tm.cloneHoldReleaseSite = ""
		}
		tm.mu.Unlock()
		return
	}
	nn := types.NamespacedName{Namespace: tm.cfg.Namespace, Name: tm.cfg.Name}
	notifyCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 30*time.Second)
	err := gate.NotifyCloneComplete(notifyCtx, nn, site)
	cancel()
	if err != nil {
		tm.logger.Error("retrying clone keyring hold release", "site", site, "error", err)
		return
	}
	tm.mu.Lock()
	if tm.cloneHoldReleaseSite == site {
		tm.cloneHoldReleaseSite = ""
	}
	tm.mu.Unlock()
}

// checkReclone processes a pending reclone annotation. If a reclone was
// requested for a specific site, validates preconditions and initiates the
// bootstrap. Returns true if a reclone was started.
func (tm *TopologyManager) checkReclone(ctx context.Context) bool {
	tm.mu.RLock()
	site := tm.reclonePendingSite
	completedSite := tm.recloneCompletedSite
	tm.mu.RUnlock()
	if completedSite != "" {
		if tm.RecloneCompleteCallback == nil {
			return false
		}
		if err := tm.RecloneCompleteCallback(ctx, completedSite); err != nil {
			tm.logger.Error("reclone complete; annotation cleanup will retry",
				"site", completedSite, "error", err)
			return false
		}
		tm.mu.Lock()
		if tm.recloneCompletedSite == completedSite {
			tm.recloneCompletedSite = ""
		}
		tm.mu.Unlock()
		return false
	}

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

	donor, donorErr := tm.confirmedActivePrimary(ctx)
	if donorErr != nil {
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

	if !tm.startBootstrapByName(ctx, donor.name, recipient.name, "reclone") {
		return false
	}
	metrics.RecloneOperations.WithLabelValues(site).Inc()
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
