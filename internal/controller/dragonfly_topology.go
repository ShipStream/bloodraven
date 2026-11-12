package controller

import (
	"context"
	"fmt"
	"log/slog"
	"runtime/debug"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/equality"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/tools/record"
	k8sretry "k8s.io/client-go/util/retry"
	"sigs.k8s.io/controller-runtime/pkg/client"

	v1alpha1 "github.com/shipstream/bloodraven/api/v1alpha1"
	"github.com/shipstream/bloodraven/internal/dragonfly"
	"github.com/shipstream/bloodraven/internal/metrics"
)

// DragonflyManager observes per-site Dragonfly state and steers
// replication topology. It runs as a goroutine alongside the MySQL
// TopologyManager and is paused while a planned failover owns the group
// (its claim or hold) or a snapshot upgrade is in flight.
type DragonflyManager struct {
	client       client.Client
	recorder     record.EventRecorder
	logger       *slog.Logger
	fgKey        types.NamespacedName
	pollInterval time.Duration

	// connector dials the Dragonfly client for a given site, so tests
	// can inject a fake. Production wires this to net.Dial via
	// dragonfly.New.
	connector DragonflyConnector

	// The manager pauses (observes and publishes status, but sends no
	// role or replication command) while either independent source says
	// so. plannedOwner is the group's planned-failover claim slot, read
	// live: true while a run holds the claim or a hold stands. It is set
	// once at construction by the runner; nil means none. upgradePaused is
	// set from the snapshot upgrade's own status. Neither writes the
	// other, so an upgrade step can never revoke a planned failover's
	// pause, nor the reverse.
	plannedOwner  atomic.Pointer[func() bool]
	upgradePaused atomic.Bool

	// stale-master events are deduplicated per (site, role) so we don't
	// spam the Event stream while the state persists.
	mu                  sync.Mutex
	lastStaleMasterAt   map[string]time.Time
	staleMasterEventTTL time.Duration

	// The recorded active site (status.dragonfly.activeSite) keeps the
	// role for activeHoldGrace while it cannot be observed and the only
	// other master is empty; see holdUnobservedActive.
	// activeUnobservedSite and activeUnobservedSince are that site and
	// when this process first failed to observe it; activeHoldLogged is
	// set once the hold has been logged for this outage. Guarded by mu.
	activeHoldGrace       time.Duration
	activeUnobservedSite  string
	activeUnobservedSince time.Time
	activeHoldLogged      bool
	// The Dragonfly-only promotion waits for the active master to be
	// missed in dragonflyMasterLossThreshold consecutive observations; see
	// noteActiveMaster and dragonflyOnlyPromotionCandidate.
	// masterMissSite is the active site being missed, masterMisses the
	// consecutive observations that missed it, masterMissAt when the last
	// of them was made, and masterWaitLogged is set once the wait has been
	// logged for this outage. Guarded by mu.
	masterMissSite   string
	masterMisses     int
	masterMissAt     time.Time
	masterWaitLogged bool
	// replicaLinked is, per site, the last observation of it as a replica
	// in stable sync: whom it followed and when. It is what says that a
	// replica found holding the cache of an earlier instance of its master
	// had that cache up to the instance's end; see
	// dragonflyKeepsCacheOverRestartedMaster. Guarded by mu.
	replicaLinked map[string]dragonflyLinkSighting
	// now is the clock the miss count and the sightings are timed with; a
	// field so tests can set it.
	now func() time.Time
	// unobservedSites holds the sites whose failed observation has been
	// logged, so a site that stays down is logged once; keyCountFailed
	// is the same for a site whose key count cannot be read. Guarded by
	// mu.
	unobservedSites map[string]bool
	keyCountFailed  map[string]bool

	// gate is the group's Dragonfly ownership gate (see dragonflyGate).
	// The runner shares one gate across every manager it starts for the
	// group; a standalone manager gets its own.
	gate *dragonflyGate
	// reader, when set, is the uncached reader every read of the group
	// under the gate goes through (the runner passes the manager's
	// APIReader). The informer cache can lag the previous gate holder's
	// writes, and acting on a lagging copy undoes them.
	reader client.Reader

	// pending is the latest emergency promotion request, executed by Run
	// between Ticks. Enqueue replaces it (latest wins) and wakes Run
	// without blocking. Guarded by pendingMu.
	pendingMu sync.Mutex
	pending   *dragonflyPromotionRequest
	wake      chan struct{}

	// Lifecycle; see BeginStop and Stop. stopping closes admission: no new
	// request, Tick or promotion step starts. stopCh wakes Run so it
	// returns after its current unit of work; runDone closes when it has.
	stopping   atomic.Bool
	stopOnce   sync.Once
	stopCh     chan struct{}
	runStarted atomic.Bool
	runDone    chan struct{}

	// Promotion budgets; see promoteDragonflyTarget. Fields so tests can
	// shrink them.
	emergencyTakeoverTimeout    time.Duration
	emergencyWaits              func(takeover time.Duration) (takeoverWait, fallbackWait time.Duration)
	emergencySettle             func(takeover time.Duration) time.Duration
	emergencyStepTimeout        time.Duration
	emergencySourceProbeTimeout time.Duration
}

// DragonflyConnector dials a Dragonfly client. Tests can replace this
// with a fake that returns a scripted *dragonfly.Client equivalent.
type DragonflyConnector func(ctx context.Context, addr, password string) (DragonflyConnection, error)

// DragonflyConnection is the subset of *dragonfly.Client the manager
// uses. Defined as an interface so tests can stub it out.
type DragonflyConnection interface {
	Ping(ctx context.Context) error
	InfoReplication(ctx context.Context) (dragonfly.ReplicationInfo, error)
	InfoPersistence(ctx context.Context) (dragonfly.PersistenceInfo, error)
	// KeyCount returns the number of keys in all databases.
	KeyCount(ctx context.Context) (int64, error)
	// ReplicationPosition returns the instance's position in the
	// replication journal, per flow (see dragonfly.Position). An error
	// means no position, never position zero.
	ReplicationPosition(ctx context.Context) (dragonfly.Position, error)
	ReplicaOf(ctx context.Context, host string, port int32) error
	ReplicaOfNoOne(ctx context.Context) error
	ReplTakeover(ctx context.Context, timeout time.Duration) error
	Save(ctx context.Context) error
	// ClientKillType issues `CLIENT KILL TYPE <kind>` against the
	// connected instance. Used best-effort to evict in-flight client
	// connections from the (now-demoted) old master after a planned
	// failover, mirroring upstream Dragonfly operator PR #436.
	ClientKillType(ctx context.Context, kind string) error
	// HasCommand reports whether the instance advertises name in its
	// command table. Used to probe REPLTAKEOVER support.
	HasCommand(ctx context.Context, name string) (bool, error)
	Close() error
}

// realDragonflyConnector dials a real Dragonfly using the package client.
// Used by production wiring.
func realDragonflyConnector(ctx context.Context, addr, password string) (DragonflyConnection, error) {
	return dragonfly.New(ctx, dragonfly.Config{Addr: addr, Password: password})
}

// NewDragonflyManager creates a manager. The caller is responsible for
// calling Start to launch the goroutine and Stop on shutdown.
func NewDragonflyManager(c client.Client, recorder record.EventRecorder, logger *slog.Logger, fgKey types.NamespacedName, pollInterval time.Duration) *DragonflyManager {
	return &DragonflyManager{
		gate:    newDragonflyGate(),
		wake:    make(chan struct{}, 1),
		stopCh:  make(chan struct{}),
		runDone: make(chan struct{}),

		client:              c,
		recorder:            recorder,
		logger:              logger,
		fgKey:               fgKey,
		pollInterval:        pollInterval,
		connector:           realDragonflyConnector,
		lastStaleMasterAt:   make(map[string]time.Time),
		staleMasterEventTTL: 5 * time.Minute,
		activeHoldGrace:     defaultDragonflyActiveHoldGrace,

		replicaLinked:   make(map[string]dragonflyLinkSighting),
		now:             time.Now,
		unobservedSites: make(map[string]bool),
		keyCountFailed:  make(map[string]bool),

		emergencyTakeoverTimeout:    defaultEmergencyTakeoverTimeout,
		emergencyWaits:              defaultEmergencyWaits,
		emergencySettle:             defaultEmergencySettle,
		emergencyStepTimeout:        defaultEmergencyStepTimeout,
		emergencySourceProbeTimeout: defaultEmergencySourceProbeTimeout,
	}
}

// SetPlannedOwner sets the planned-failover pause source: owned reports
// whether a planned failover holds the group's claim or a hold stands.
// Call before Run.
func (m *DragonflyManager) SetPlannedOwner(owned func() bool) {
	if owned == nil {
		m.plannedOwner.Store(nil)
		return
	}
	m.plannedOwner.Store(&owned)
}

// SetUpgradePaused sets the snapshot-upgrade pause source. While either
// source pauses the manager, it continues to observe and update status,
// but does NOT issue REPLICAOF commands.
func (m *DragonflyManager) SetUpgradePaused(paused bool) {
	m.upgradePaused.Store(paused)
}

// plannedOwned reports the planned-failover pause source.
func (m *DragonflyManager) plannedOwned() bool {
	if f := m.plannedOwner.Load(); f != nil {
		return (*f)()
	}
	return false
}

// paused reports whether either pause source holds.
func (m *DragonflyManager) paused() bool {
	return m.upgradePaused.Load() || m.plannedOwned()
}

// plannedFailoverOwnsDragonfly is the status form of Dragonfly ownership,
// for a caller with no hydrated claim slot to ask: the claim slot holds a
// run or a hold (claimHeld), fg's run (read under the gate) still needs the
// claim (PlannedRunNeedsClaim), or fg carries durable hold evidence
// (PlannedHoldFrom): a hold the slot has not been hydrated with yet, after
// a restart, owns Dragonfly as the hydrated hold will. A request that is
// only waiting (Admitting, Deferred) owns nothing, and neither does a run
// that released its claim once its promotion was recorded (DST-4, O-m1).
func plannedFailoverOwnsDragonfly(fg *v1alpha1.MysqlFailoverGroup, claimHeld bool) bool {
	return claimHeld || PlannedRunNeedsClaim(fg, time.Now()) || PlannedHoldFrom(fg) != nil
}

// plannedOwnsDragonfly reports whether a planned failover owns the group's
// Dragonfly. With a planned owner wired (the runner's claim slot, hydrated
// from an uncached read before this manager starts) the slot alone
// decides: status still reads Promoting after the confirmed tail released
// the claim, and an emergency follow-up in that window must run. Without
// one, fg decides.
func (m *DragonflyManager) plannedOwnsDragonfly(fg *v1alpha1.MysqlFailoverGroup) bool {
	if f := m.plannedOwner.Load(); f != nil {
		return (*f)()
	}
	return plannedFailoverOwnsDragonfly(fg, false)
}

// SetConnector overrides the default connector. Tests use this to inject
// a fake without standing up a real RESP server.
func (m *DragonflyManager) SetConnector(c DragonflyConnector) {
	m.connector = c
}

// SetAuthorityReader makes the manager read the group it acts on through r,
// an uncached reader, instead of its (possibly cached) client.
func (m *DragonflyManager) SetAuthorityReader(r client.Reader) {
	m.reader = r
}

// authority returns the reader for the group's authoritative state.
func (m *DragonflyManager) authority() client.Reader {
	if m.reader != nil {
		return m.reader
	}
	return m.client
}

// SetGate makes the manager use the group's shared ownership gate. The
// runner calls it before Run starts.
func (m *DragonflyManager) SetGate(g *dragonflyGate) {
	m.gate = g
}

// Run launches the polling loop. Returns when ctx is canceled.
//
// Each Tick is wrapped in a panic-recovery guard so a single bad
// observation (NPE in observe, missing nilcheck in reconcileReplication,
// etc.) cannot kill the goroutine for the operator's lifetime — the
// runner's start/stop logic only fires on TopologyConfig changes, so
// without recovery a panic would silently stop all Dragonfly
// observation+replication until the operator pod restarts.
//
// Run is also the only goroutine that executes the manager's promotions:
// requests queued with Enqueue run between Ticks, so a Tick (observation,
// status publication, replica repair) and a promotion never overlap. Every
// unit of work holds the group's gate, which excludes the other Dragonfly
// writers (see dragonflyGate). Run returns after its current unit of work
// once ctx ends or Stop is called; a promotion that has sent a
// role-changing command always finishes recording it first.
func (m *DragonflyManager) Run(ctx context.Context) {
	m.runStarted.Store(true)
	defer close(m.runDone)
	if m.stopping.Load() {
		return
	}
	if m.pollInterval <= 0 {
		m.pollInterval = 2 * time.Second
	}
	// Waiting for the gate ends on stop or ctx; the work itself runs on a
	// context that neither cancels, so a command already on the wire and
	// its completion are never cut short (they have their own budgets).
	loopCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	go func() {
		select {
		case <-m.stopCh:
			cancel()
		case <-loopCtx.Done():
		}
	}()
	ticker := time.NewTicker(m.pollInterval)
	defer ticker.Stop()
	// Tick immediately so freshly-created Dragonfly resources get their
	// first status observation without waiting a full pollInterval.
	m.safeTick(loopCtx)
	for {
		select {
		case <-loopCtx.Done():
			return
		case <-m.wake:
			m.runPending(loopCtx)
		case <-ticker.C:
			m.safeTick(loopCtx)
		}
	}
}

// BeginStop closes admission: Enqueue drops new requests, a queued request
// is discarded, and no new Tick or promotion step starts. A promotion that
// has already sent a role-changing command still finishes recording it.
func (m *DragonflyManager) BeginStop() {
	m.stopping.Store(true)
	m.stopOnce.Do(func() { close(m.stopCh) })
	m.pendingMu.Lock()
	m.pending = nil
	m.pendingMu.Unlock()
}

// Stop closes admission and waits up to wait for Run to return. It reports
// whether Run returned (or never started). If the wait times out, Run's
// last unit of work still holds the group's gate, so no other writer,
// including a replacement manager, can mutate until it finishes.
func (m *DragonflyManager) Stop(wait time.Duration) bool {
	m.BeginStop()
	if !m.runStarted.Load() {
		return true
	}
	timer := time.NewTimer(wait)
	defer timer.Stop()
	select {
	case <-m.runDone:
		return true
	case <-timer.C:
		return false
	}
}

// safeTick wraps Tick in a panic-recovery guard. Panics are logged with
// the recovered value and a stack trace; the loop continues with the
// next tick.
func (m *DragonflyManager) safeTick(ctx context.Context) {
	defer func() {
		if r := recover(); r != nil {
			m.logger.Error("dragonfly: panic recovered in Tick",
				"recovered", fmt.Sprintf("%v", r),
				"stack", string(debug.Stack()),
			)
			metrics.DragonflyManagerPanicsTotal.WithLabelValues(m.fgKey.Namespace, m.fgKey.Name).Inc()
		}
	}()
	m.Tick(ctx)
}

// Tick performs one observation+reconciliation cycle. Exposed so tests
// can drive the manager deterministically.
//
// Tick holds the group's gate for the whole cycle and reads the group
// after acquiring it, so the status it publishes and the commands it sends
// are based on an observation no other writer can invalidate. If another
// writer holds the gate, the cycle is skipped. If a planned failover or
// rollout starts waiting for the gate mid-cycle, Tick stops at its next
// safe point instead of publishing or acting.
func (m *DragonflyManager) Tick(ctx context.Context) {
	if m.stopping.Load() {
		return
	}
	release, ok := m.gate.tryAcquire()
	if !ok {
		return
	}
	defer release()

	var fg v1alpha1.MysqlFailoverGroup
	if err := m.authority().Get(ctx, m.fgKey, &fg); err != nil {
		m.logger.Debug("dragonfly tick: get fg", "error", err)
		return
	}
	if !dragonflyEnabled(&fg) {
		return
	}

	observedAt := m.now()
	snap := m.observe(ctx, &fg)
	// What was observed is noted before the cycle can yield: an
	// observation that reached the master ends the miss count whether or
	// not this cycle goes on to act.
	hold := m.holdUnobservedActive(&fg, snap)
	m.noteActiveMaster(&fg, snap, hold, observedAt)
	m.noteLinkedReplicas(snap)
	if m.shouldYield() {
		return
	}
	if master := dragonflyMasterOverEmptyRecord(&fg, snap); master != "" && plannedTakeoverMasterSite(&fg) == "" {
		m.logger.Info("dragonfly: recorded active site holds no keys; following the master on the MySQL active site",
			"site", recordedDragonflyActiveSite(&fg), "master", master)
	}
	m.patchStatus(ctx, snap, hold)

	if m.paused() || m.shouldYield() {
		return
	}
	// Commands run on a context the stop signal does not cancel: once a
	// role change is sent it is recorded (it has its own budgets).
	workCtx := context.WithoutCancel(ctx)
	if target, oldSource, ok := m.mysqlActivePromotionCandidate(&fg, snap); ok {
		m.logger.Info("dragonfly/mysql active-site drift: promoting Dragonfly replica to match MySQL",
			"oldSource", oldSource, "target", target, "mysqlActiveSite", fg.Status.ActiveSite, "fg", m.fgKey.String())
		if m.recorder != nil {
			m.recorder.Eventf(&fg, corev1.EventTypeNormal, v1alpha1.EventDragonflyPromotionStarted,
				"mysql active site %q differs from Dragonfly master %q; promoting Dragonfly replica %q",
				fg.Status.ActiveSite, oldSource, target)
		}
		m.promoteLocked(workCtx, &fg, target, oldSource, m.emergencyTakeoverTimeout)
		return
	}
	if target, oldSource, ok := m.restartedMasterPromotionCandidate(&fg, snap, hold); ok {
		m.logger.Info("dragonfly: active master restarted without the cache; promoting the replica that holds it",
			"oldSource", oldSource, "target", target, "fg", m.fgKey.String())
		if m.recorder != nil {
			m.recorder.Eventf(&fg, corev1.EventTypeNormal, v1alpha1.EventDragonflyPromotionStarted,
				"active Dragonfly master %q is a new instance without the cache that replica %q still holds; promoting the replica instead of re-attaching it",
				oldSource, target)
		}
		m.promoteLocked(workCtx, &fg, target, oldSource, noDragonflyTakeover)
		return
	}
	if target, oldSource, ok := m.dragonflyOnlyPromotionCandidate(&fg, snap, hold); ok {
		m.logger.Info("dragonfly-only emergency: active master unreachable; promoting replica", "oldSource", oldSource, "target", target, "fg", m.fgKey.String())
		if m.recorder != nil {
			m.recorder.Eventf(&fg, corev1.EventTypeNormal, v1alpha1.EventDragonflyPromotionStarted,
				"dragonfly-only emergency: active master %q unreachable; promoting replica %q",
				oldSource, target)
		}
		m.promoteLocked(workCtx, &fg, target, oldSource, m.emergencyTakeoverTimeout)
		return
	}
	m.reconcileReplication(workCtx, &fg, snap, hold)
}

// shouldYield reports whether the current unit of work should stop at a
// safe point: the manager is stopping, or a planned failover or rollout is
// waiting for the gate.
func (m *DragonflyManager) shouldYield() bool {
	return m.stopping.Load() || m.gate.contended()
}

func (m *DragonflyManager) mysqlActivePromotionCandidate(fg *v1alpha1.MysqlFailoverGroup, snaps []DragonflySiteSnapshot) (target, oldSource string, ok bool) {
	mysqlActive := fg.Status.ActiveSite
	if mysqlActive == "" {
		return "", "", false
	}
	// While a run is taking over Dragonfly, promoting MySQL or publishing,
	// or rolling back after a takeover, Dragonfly and MySQL disagree on
	// purpose: the run decides, and Dragonfly follows MySQL after release.
	// A Promoting or Publishing run whose claim the slot no longer holds
	// released it after recording its promotion (DST-4): Dragonfly follows
	// MySQL again, unless status.activeSite still names the run's source,
	// which is the pre-promotion authority a stalled status write left.
	if pf, marked := plannedFailoverRun(fg); pf != nil && !marked {
		owner := m.plannedOwner.Load()
		switch pf.Phase {
		case v1alpha1.PlannedFailoverPhasePromotingDragonfly,
			v1alpha1.PlannedFailoverPhaseRollingBack:
			return "", "", false
		case v1alpha1.PlannedFailoverPhasePromoting,
			v1alpha1.PlannedFailoverPhasePublishing:
			if owner == nil || (*owner)() || mysqlActive == pf.SourceSite {
				return "", "", false
			}
		}
	}
	if df := fg.Status.Dragonfly; df != nil &&
		df.ActiveSite != "" &&
		df.ActiveSite != mysqlActive &&
		df.LastPromotionTime != nil &&
		time.Since(df.LastPromotionTime.Time) <= pendingPromotionActiveSiteTTL {
		return "", "", false
	}

	rawMasters := make([]string, 0, 1)
	var targetSnap *DragonflySiteSnapshot
	for i := range snaps {
		snap := &snaps[i]
		if snap.Reachable && snap.Info.Role == "master" {
			rawMasters = append(rawMasters, snap.Name)
		}
		if snap.Name == mysqlActive {
			targetSnap = snap
		}
	}
	if len(rawMasters) != 1 || rawMasters[0] == mysqlActive || targetSnap == nil {
		return "", "", false
	}
	role := dragonfly.ClassifySiteRole(targetSnap.Info, targetSnap.Reachable, targetSnap.Name, mysqlActive)
	if !dragonflyOnlyPromotionReady(*targetSnap, role) {
		return "", "", false
	}
	return mysqlActive, rawMasters[0], true
}

// DragonflySiteSnapshot is the internal observation for one site at one tick.
type DragonflySiteSnapshot struct {
	Name      string
	Reachable bool
	Info      dragonfly.ReplicationInfo
	Persist   dragonfly.PersistenceInfo
	// Keys is the number of keys in all databases (`INFO keyspace`), nil
	// when it could not be read. It is what tells an instance that holds
	// the cache from one that restarted empty: both report role=master,
	// and a Dragonfly master reports no replication offset (v1.38).
	Keys *int64
	// ReplTakeover is the COMMAND INFO probe for this tick. Nil means
	// the site was not probed (unreachable, INFO failed, or probe I/O
	// error). True/false are confirmed results and must not flip
	// Reachable or DragonflySiteUp.
	ReplTakeover *bool
}

// observe collects per-site Dragonfly state. Connections are opened and
// closed within this call; the manager does not pool connections because
// pollInterval is short and Dragonfly handles reconnection cheaply.
func (m *DragonflyManager) observe(ctx context.Context, fg *v1alpha1.MysqlFailoverGroup) []DragonflySiteSnapshot {
	password := m.fetchPassword(ctx, fg)
	out := make([]DragonflySiteSnapshot, 0, len(fg.Spec.Sites))
	for _, site := range fg.Spec.Sites {
		snap := DragonflySiteSnapshot{Name: site.Name}
		addr := dragonflyAddr(fg, site.Name)
		dialCtx, cancel := context.WithTimeout(ctx, dragonfly.DefaultDialTimeout+dragonfly.DefaultIOTimeout)
		conn, err := m.connector(dialCtx, addr, password)
		cancel()
		if err != nil {
			metrics.DragonflySiteUp.WithLabelValues(fg.Name, site.Name).Set(0)
			m.noteObservation(site.Name, err)
			out = append(out, snap)
			continue
		}
		// We re-use ctx (no extra deadline) for the actual command —
		// the client itself enforces an IOTimeout per call.
		info, err := conn.InfoReplication(ctx)
		if err != nil {
			// Dial succeeded but INFO failed: the Dragonfly process is
			// up enough to accept TCP but not enough to answer commands
			// (e.g. AUTH wedge, mid-load, deadlocked). The gauge MUST
			// flip to 0 here — Prometheus gauges retain their last
			// value, so without this the up-but-stuck condition this
			// metric exists to alert on would silently report up=1
			// indefinitely.
			metrics.DragonflySiteUp.WithLabelValues(fg.Name, site.Name).Set(0)
			m.noteObservation(site.Name, err)
			_ = conn.Close()
			out = append(out, snap)
			continue
		}
		persist, err := conn.InfoPersistence(ctx)
		if err != nil {
			// Persistence info is best-effort; an error here does not
			// disqualify the site, but loading state may be missed.
			persist = dragonfly.PersistenceInfo{}
		}
		keys, keysErr := conn.KeyCount(ctx)
		if keysErr == nil {
			snap.Keys = &keys
		}
		m.noteKeyCount(site.Name, keysErr)
		// Probe last so a parser bug cannot desync INFO replies.
		// A probe error leaves ReplTakeover nil and must not flip
		// reachability or the up gauge.
		if supported, probeErr := conn.HasCommand(ctx, "REPLTAKEOVER"); probeErr != nil {
			m.logger.Debug("dragonfly: REPLTAKEOVER capability probe failed", "site", site.Name, "error", probeErr)
		} else {
			snap.ReplTakeover = &supported
		}
		_ = conn.Close()
		snap.Reachable = true
		snap.Info = info
		snap.Persist = persist
		metrics.DragonflySiteUp.WithLabelValues(fg.Name, site.Name).Set(1)
		m.noteObservation(site.Name, nil)
		out = append(out, snap)
	}
	return out
}

// fetchPassword reads the Dragonfly auth password from the configured
// Secret. Returns empty string when auth is not configured. Secret-read
// failures and missing keys are logged at Warn (not Debug) so a wedged
// AUTH never disappears silently — but the manager still returns an
// empty password so it can dial and fail loudly via the resulting AUTH
// error rather than swallowing the cause entirely.
func (m *DragonflyManager) fetchPassword(ctx context.Context, fg *v1alpha1.MysqlFailoverGroup) string {
	auth := fg.Spec.Dragonfly.Auth
	if auth == nil || auth.PasswordSecretKeyRef.Name == "" {
		return ""
	}
	ref := auth.PasswordSecretKeyRef
	key := ref.KeyOrDefault()
	var secret corev1.Secret
	if err := m.client.Get(ctx, types.NamespacedName{Namespace: fg.Namespace, Name: ref.Name}, &secret); err != nil {
		m.logger.Warn("dragonfly: read auth secret", "secret", ref.Name, "error", err)
		return ""
	}
	raw, ok := secret.Data[key]
	if !ok {
		m.logger.Warn("dragonfly: auth secret missing key", "secret", ref.Name, "key", key)
		return ""
	}
	return string(raw)
}

// patchStatus writes the observed snapshot into status.dragonfly. We
// only patch when there is a meaningful change, to keep API-server load
// proportional to actual state transitions.
func (m *DragonflyManager) patchStatus(ctx context.Context, snaps []DragonflySiteSnapshot, holdUnobserved bool) {
	var (
		patched       bool
		prevSupported *bool
		nextSupported *bool
		eventFG       v1alpha1.MysqlFailoverGroup
	)
	err := k8sretry.RetryOnConflict(k8sretry.DefaultRetry, func() error {
		var fg v1alpha1.MysqlFailoverGroup
		if err := m.authority().Get(ctx, m.fgKey, &fg); err != nil {
			return err
		}
		if !dragonflyEnabled(&fg) {
			return nil
		}

		desired := buildDragonflyStatus(&fg, snaps, holdUnobserved)
		if dragonflyStatusEqual(fg.Status.Dragonfly, &desired) {
			return nil
		}

		patched = true
		prevSupported = nil
		if fg.Status.Dragonfly != nil {
			prevSupported = fg.Status.Dragonfly.ReplTakeoverSupported
		}
		nextSupported = desired.ReplTakeoverSupported
		eventFG = fg

		base := fg.DeepCopy()
		fg.Status.Dragonfly = &desired
		patch := client.MergeFrom(base)
		return m.client.Status().Patch(ctx, &fg, patch)
	})
	if err != nil {
		m.logger.Error("dragonfly: status patch failed", "error", err)
		return
	}
	if patched {
		m.emitReplTakeoverTransition(&eventFG, prevSupported, nextSupported)
	}
}

// buildDragonflyStatus converts per-site snapshots into the API-shaped
// status block. holdUnobserved is activeDragonflySiteFromObservation's.
func buildDragonflyStatus(fg *v1alpha1.MysqlFailoverGroup, snaps []DragonflySiteSnapshot, holdUnobserved bool) v1alpha1.DragonflyStatus {
	expectedActive := activeDragonflySiteFromObservation(fg, snaps, holdUnobserved)
	st := v1alpha1.DragonflyStatus{
		ActiveSite: expectedActive,
	}
	mastersOnExpected := 0
	stale := 0
	unreachable := 0
	allReady := len(snaps) > 0
	siteOut := make([]v1alpha1.DragonflySiteStatus, 0, len(snaps))
	for _, s := range snaps {
		ready := false
		linkStatus := s.Info.MasterLinkStatus
		role := dragonfly.ClassifySiteRole(s.Info, s.Reachable, s.Name, expectedActive)
		switch role {
		case dragonfly.RoleMaster:
			mastersOnExpected++
			ready = s.Reachable
		case dragonfly.RoleReplica:
			ready = s.Reachable && s.Info.MasterLinkStatus == "up" && s.Info.MasterLastIOSecondsAgo >= 0 && !s.Info.MasterSyncInProgress && !s.Persist.Loading
		case dragonfly.RoleStaleMaster:
			stale++
		case dragonfly.RoleUnreachable:
			unreachable++
		}
		if !ready {
			allReady = false
		}
		siteOut = append(siteOut, v1alpha1.DragonflySiteStatus{
			Name:             s.Name,
			Role:             apiDragonflyRole(role),
			Reachable:        s.Reachable,
			ServiceName:      dragonflySiteServiceName(fg.Name, s.Name),
			ReplicationState: s.Info.Role,
			LinkStatus:       linkStatus,
			SyncInProgress:   dragonflySyncInProgress(s.Info),
			LastIOSecondsAgo: dragonflyLastIOSecondsAgo(s.Info),
			Ready:            ready,
			Message:          dragonflySiteMessage(s, role),
		})
	}
	st.Sites = siteOut

	switch {
	case mastersOnExpected == 0 && expectedActive != "":
		st.Phase = v1alpha1.DragonflyPhaseConfiguringReplication
		st.Message = fmt.Sprintf("no master observed on active site %s", expectedActive)
	case stale > 0:
		st.Phase = v1alpha1.DragonflyPhaseDegraded
		st.Message = fmt.Sprintf("stale master detected on %d site(s)", stale)
	case unreachable > 0:
		st.Phase = v1alpha1.DragonflyPhaseDegraded
		st.Message = fmt.Sprintf("%d site(s) unreachable", unreachable)
	case allReady:
		st.Phase = v1alpha1.DragonflyPhaseReady
		st.Message = "all sites ready"
	default:
		st.Phase = v1alpha1.DragonflyPhaseConfiguringReplication
	}

	// Preserve historical lastPromotion fields if the existing status
	// already had them; the planned/emergency promotion paths stamp
	// them on success.
	if fg.Status.Dragonfly != nil {
		st.LastPromotionTime = fg.Status.Dragonfly.LastPromotionTime
		st.LastPromotionTarget = fg.Status.Dragonfly.LastPromotionTarget
		st.Upgrade = fg.Status.Dragonfly.Upgrade
		// The reconciler observes the image from the StatefulSets.
		st.Image = fg.Status.Dragonfly.Image
	}
	applyReplTakeoverStatus(&st, fg, snaps)
	return st
}

// applyReplTakeoverStatus folds per-site COMMAND INFO probes into the
// group-level capability fields. Pessimistic: false if any probed site
// lacks the command; true only if at least one probe succeeded and
// every probe was true. A tick with no probes keeps the previous
// result so a transient outage cannot wipe a known-false warning.
// ProbeTime is stamped only when the boolean or message changes.
func applyReplTakeoverStatus(st *v1alpha1.DragonflyStatus, fg *v1alpha1.MysqlFailoverGroup, snaps []DragonflySiteSnapshot) {
	supported, msg := foldReplTakeover(snaps)
	var prev *v1alpha1.DragonflyStatus
	if fg.Status.Dragonfly != nil {
		prev = fg.Status.Dragonfly
	}
	if supported == nil {
		if prev != nil {
			st.ReplTakeoverSupported = prev.ReplTakeoverSupported
			st.ReplTakeoverProbeTime = prev.ReplTakeoverProbeTime
			st.ReplTakeoverProbeMessage = prev.ReplTakeoverProbeMessage
		}
		return
	}
	// A known-false result must survive a partial probe (one site
	// restarting, mixed-image rollout). Only accept true — or a
	// rewritten false message — when every snapshot was probed.
	if prev != nil && prev.ReplTakeoverSupported != nil && !*prev.ReplTakeoverSupported && probeIncomplete(snaps) {
		st.ReplTakeoverSupported = prev.ReplTakeoverSupported
		st.ReplTakeoverProbeTime = prev.ReplTakeoverProbeTime
		st.ReplTakeoverProbeMessage = prev.ReplTakeoverProbeMessage
		return
	}
	st.ReplTakeoverSupported = supported
	st.ReplTakeoverProbeMessage = msg
	if prev != nil && boolPtrEqual(prev.ReplTakeoverSupported, supported) && prev.ReplTakeoverProbeMessage == msg {
		st.ReplTakeoverProbeTime = prev.ReplTakeoverProbeTime
		return
	}
	now := metav1.Now()
	st.ReplTakeoverProbeTime = &now
}

func probeIncomplete(snaps []DragonflySiteSnapshot) bool {
	for _, s := range snaps {
		if s.ReplTakeover == nil {
			return true
		}
	}
	return false
}

func foldReplTakeover(snaps []DragonflySiteSnapshot) (supported *bool, msg string) {
	var missing []string
	anyTrue := false
	for _, s := range snaps {
		if s.ReplTakeover == nil {
			continue
		}
		if !*s.ReplTakeover {
			missing = append(missing, s.Name)
			continue
		}
		anyTrue = true
	}
	if len(missing) > 0 {
		v := false
		return &v, fmt.Sprintf("REPLTAKEOVER not advertised on %s", strings.Join(missing, ", "))
	}
	if anyTrue {
		v := true
		return &v, ""
	}
	return nil, ""
}

func boolPtrEqual(a, b *bool) bool {
	if a == nil || b == nil {
		return a == nil && b == nil
	}
	return *a == *b
}

func (m *DragonflyManager) emitReplTakeoverTransition(fg *v1alpha1.MysqlFailoverGroup, prev, next *bool) {
	if boolPtrEqual(prev, next) {
		return
	}
	if next != nil && !*next {
		m.logger.Warn("dragonfly: REPLTAKEOVER not advertised", "fg", m.fgKey.String())
		if m.recorder != nil {
			m.recorder.Eventf(fg, corev1.EventTypeWarning, v1alpha1.EventDragonflyReplTakeoverUnsupported,
				"Dragonfly does not advertise REPLTAKEOVER; emergency promotion will fall back to REPLICAOF NO ONE (session continuity not guaranteed)")
		}
		return
	}
	if prev != nil && !*prev && next != nil && *next {
		m.logger.Info("dragonfly: REPLTAKEOVER capability restored", "fg", m.fgKey.String())
		if m.recorder != nil {
			m.recorder.Eventf(fg, corev1.EventTypeNormal, v1alpha1.EventDragonflyReplTakeoverSupported,
				"Dragonfly now advertises REPLTAKEOVER")
		}
	}
}

// activeDragonflySiteFromObservation chooses the site that should be treated
// as Dragonfly master for status, label reconciliation, and replica wiring.
// A planned run's succeeded takeover (plannedTakeoverMasterSite) names its
// target, unless the target is observed as a replica: the takeover made it
// master, so a replica target means a later promotion moved the master on.
// A persisted status.dragonfly.activeSite is honored only when that site is
// still observed as a raw Dragonfly master; otherwise a single observed raw
// master self-heals stale status after operator upgrades or restarts.
//
// Two cases are excepted, both on the key count: with holdUnobserved, a
// persisted active site that could not be observed at all keeps the role
// when the single raw master holds no keys (dragonflyHeldOverEmptyMaster);
// and a persisted active site that holds no keys gives way to the master
// on the MySQL active site that holds the cache
// (dragonflyMasterOverEmptyRecord).
func activeDragonflySiteFromObservation(fg *v1alpha1.MysqlFailoverGroup, snaps []DragonflySiteSnapshot, holdUnobserved bool) string {
	if site := plannedTakeoverMasterSite(fg); site != "" {
		demoted := false
		for _, s := range snaps {
			if s.Name == site && s.Reachable && s.Info.Role == "slave" {
				demoted = true
			}
		}
		if !demoted {
			return site
		}
	}
	recorded := recordedDragonflyActiveSite(fg)
	if holdUnobserved && dragonflyHeldOverEmptyMaster(fg, snaps) != "" {
		return recorded
	}
	rawMasters := make([]string, 0, 1)
	for _, s := range snaps {
		if s.Reachable && s.Info.Role == "master" {
			rawMasters = append(rawMasters, s.Name)
		}
	}
	if recorded != "" {
		for _, site := range rawMasters {
			if site == recorded {
				if master := dragonflyMasterOverEmptyRecord(fg, snaps); master != "" {
					return master
				}
				return site
			}
		}
	}
	if len(rawMasters) == 1 {
		return rawMasters[0]
	}
	if recorded != "" {
		return recorded
	}
	return fg.Status.ActiveSite
}

// dragonflyHeldOverEmptyMaster returns the site that reports the master
// role and holds no keys while the recorded active site
// (status.dragonfly.activeSite) could not be observed and no other site
// reports the role, or "". That is the observation in which the recorded
// site keeps the role for a grace period (holdUnobservedActive).
//
// A failed observation is not evidence that a site stopped being the
// master, and a site that reports the master role is not thereby the
// master: a Dragonfly pod that restarted without a snapshot reports it
// too, holding nothing, and the source of a takeover is recreated that
// way. Without the hold, one missed observation of the master while such
// a pod is up makes the empty pod the active site: every replica is
// pointed at it, and so is the master in the next cycle.
//
// A single master that holds keys, or whose key count could not be read,
// is not held off: it replaces the unobserved site at once, as it always
// did, which repairs a status whose promotion record was lost.
func dragonflyHeldOverEmptyMaster(fg *v1alpha1.MysqlFailoverGroup, snaps []DragonflySiteSnapshot) string {
	recorded := recordedDragonflyActiveSite(fg)
	if recorded == "" || plannedTakeoverMasterSite(fg) != "" || dragonflySiteObserved(snaps, recorded) {
		return ""
	}
	other := ""
	for i := range snaps {
		s := &snaps[i]
		if !s.Reachable || s.Info.Role != "master" {
			continue
		}
		if other != "" || !s.holdsNoKeys() {
			return ""
		}
		other = s.Name
	}
	return other
}

// dragonflyMasterOverEmptyRecord returns the MySQL active site when its
// Dragonfly reports the master role and holds keys while the recorded
// active site (status.dragonfly.activeSite) is another site that reports
// the master role and holds none, or "".
//
// That is a promotion whose record was lost: the operator promoted the
// Dragonfly on the MySQL active site and died before recording it, and the
// old master, whose process exits in a takeover, restarted empty. The
// record then names the empty instance. Honoring it leaves the group with
// two masters for good, since the site with the cache is not reattached to
// an empty one (reconcileReplication), and before that check existed it
// emptied the cache. The site that has both MySQL and the cache is the
// master; the empty one is reattached to it.
//
// Both conditions are needed. A recorded site on the MySQL active site is
// never displaced, whatever another master holds (a pod restarted from a
// snapshot holds keys too), and an unread key count decides nothing.
func dragonflyMasterOverEmptyRecord(fg *v1alpha1.MysqlFailoverGroup, snaps []DragonflySiteSnapshot) string {
	recorded := recordedDragonflyActiveSite(fg)
	mysqlActive := fg.Status.ActiveSite
	if recorded == "" || mysqlActive == "" || mysqlActive == recorded {
		return ""
	}
	rec, master := dragonflySnapshotOf(snaps, recorded), dragonflySnapshotOf(snaps, mysqlActive)
	if rec == nil || master == nil {
		return ""
	}
	if !rec.Reachable || rec.Info.Role != "master" || !rec.holdsNoKeys() {
		return ""
	}
	if !master.Reachable || master.Info.Role != "master" || !master.holdsKeys() {
		return ""
	}
	return mysqlActive
}

// holdsKeys reports whether the instance was observed with at least one
// key; holdsNoKeys, with none. Both are false when the key count could not
// be read, so neither "it has the cache" nor "it is empty" is concluded
// from a failed read.
func (s *DragonflySiteSnapshot) holdsKeys() bool   { return s.Keys != nil && *s.Keys > 0 }
func (s *DragonflySiteSnapshot) holdsNoKeys() bool { return s.Keys != nil && *s.Keys == 0 }

// recordedDragonflyActiveSite is status.dragonfly.activeSite, or "".
func recordedDragonflyActiveSite(fg *v1alpha1.MysqlFailoverGroup) string {
	if fg.Status.Dragonfly == nil {
		return ""
	}
	return fg.Status.Dragonfly.ActiveSite
}

// dragonflySiteObserved reports whether site answered this cycle's
// observation, whatever role it reported.
func dragonflySiteObserved(snaps []DragonflySiteSnapshot, site string) bool {
	for _, s := range snaps {
		if s.Name == site {
			return s.Reachable
		}
	}
	return false
}

// dragonflySnapshotOf returns site's snapshot of this cycle, or nil.
func dragonflySnapshotOf(snaps []DragonflySiteSnapshot, site string) *DragonflySiteSnapshot {
	for i := range snaps {
		if snaps[i].Name == site {
			return &snaps[i]
		}
	}
	return nil
}

// defaultDragonflyActiveHoldGrace is how long the recorded active site keeps
// the role while it cannot be observed (holdUnobservedActive). It has to
// outlast a few failed observations: one costs up to the dial timeout
// (dragonfly.DefaultDialTimeout) or, when the dial succeeds and the reply
// does not come, the I/O timeout (dragonfly.DefaultIOTimeout), and the
// cycles follow each other without a pause while one is slow.
const defaultDragonflyActiveHoldGrace = 15 * time.Second

// holdUnobservedActive reports whether the recorded active site
// (status.dragonfly.activeSite) has been unobservable to this process for
// less than activeHoldGrace, counted from its first failed observation.
// For that long it keeps the role against a single empty master
// (dragonflyHeldOverEmptyMaster), which is logged once per outage.
//
// After the grace the observation decides again, as before: a single site
// that reports the master role becomes the active site, whatever it holds.
// That is what brings the cache back when the recorded site never returns.
//
// The time is this process's: a restarted operator starts a new grace.
func (m *DragonflyManager) holdUnobservedActive(fg *v1alpha1.MysqlFailoverGroup, snaps []DragonflySiteSnapshot) bool {
	recorded := recordedDragonflyActiveSite(fg)
	m.mu.Lock()
	defer m.mu.Unlock()
	if recorded == "" || dragonflySiteObserved(snaps, recorded) {
		m.activeUnobservedSite = ""
		m.activeUnobservedSince = time.Time{}
		m.activeHoldLogged = false
		return false
	}
	now := time.Now()
	if m.activeUnobservedSite != recorded {
		m.activeUnobservedSite = recorded
		m.activeUnobservedSince = now
		m.activeHoldLogged = false
	}
	if now.Sub(m.activeUnobservedSince) >= m.activeHoldGrace {
		return false
	}
	if other := dragonflyHeldOverEmptyMaster(fg, snaps); other != "" && !m.activeHoldLogged {
		m.activeHoldLogged = true
		m.logger.Info("dragonfly: active site not observed; keeping its role", "site", recorded, "emptyMaster", other, "grace", m.activeHoldGrace.String())
	}
	return true
}

// noteObservation logs a site's failed observation once, when it starts
// failing, with the error. The gauge (DragonflySiteUp) says that a site is
// down; this line says why the operator could not tell, which is what
// explains a role decision made in that cycle.
func (m *DragonflyManager) noteObservation(site string, err error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if err == nil {
		delete(m.unobservedSites, site)
		return
	}
	if m.unobservedSites[site] {
		return
	}
	m.unobservedSites[site] = true
	m.logger.Info("dragonfly: site observation failed", "site", site, "error", err)
}

// noteKeyCount logs, once when it starts, that a site answers but its key
// count cannot be read. Without the count the manager cannot tell a master
// that holds the cache from one that restarted empty, and the two checks
// that rest on it (dragonflyHeldOverEmptyMaster, the stale-master
// reattach in reconcileReplication) do not apply to that site.
func (m *DragonflyManager) noteKeyCount(site string, err error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if err == nil {
		delete(m.keyCountFailed, site)
		return
	}
	if m.keyCountFailed[site] {
		return
	}
	m.keyCountFailed[site] = true
	m.logger.Warn("dragonfly: key count unavailable", "site", site, "error", err)
}

func dragonflySiteMessage(s DragonflySiteSnapshot, role dragonfly.SiteRole) string {
	switch role {
	case dragonfly.RoleMaster:
		return "serving writes"
	case dragonfly.RoleReplica:
		if s.Info.MasterSyncInProgress {
			return "full sync in progress"
		}
		if s.Info.MasterLinkStatus == "up" {
			return "replicating from active master"
		}
		return "master link down"
	case dragonfly.RoleStaleMaster:
		return "stale master on non-active site"
	case dragonfly.RoleUnconfigured:
		return "fresh pod; replication not yet wired"
	case dragonfly.RoleUnreachable:
		return "unreachable"
	default:
		return ""
	}
}

// reconcileReplication ensures non-active sites are replicas pointing at
// the active master, and detects stale masters. Reconfiguration is
// best-effort — failures are logged and retried on the next tick.
// The caller holds the group's gate; each site's change is skipped once the
// unit of work should yield.
func (m *DragonflyManager) reconcileReplication(ctx context.Context, fg *v1alpha1.MysqlFailoverGroup, snaps []DragonflySiteSnapshot, holdUnobserved bool) {
	active := activeDragonflySiteFromObservation(fg, snaps, holdUnobserved)
	if active == "" {
		// Nothing to align against until MySQL has picked an active site.
		return
	}
	// The active site answered as a master that holds no keys.
	activeSnap := dragonflySnapshotOf(snaps, active)
	activeEmpty := activeSnap != nil && activeSnap.Reachable && activeSnap.Info.Role == "master" && activeSnap.holdsNoKeys()
	// The active site is held although unobserved, over an empty master.
	heldOverEmpty := holdUnobserved && dragonflyHeldOverEmptyMaster(fg, snaps) != ""
	password := m.fetchPassword(ctx, fg)
	masterAddr := dragonflyAddr(fg, active)
	masterHost, masterPort := splitHostPort(masterAddr, dragonflyPort(fg.Spec.Dragonfly))

	for _, snap := range snaps {
		if m.shouldYield() {
			return
		}
		role := dragonfly.ClassifySiteRole(snap.Info, snap.Reachable, snap.Name, active)
		switch role {
		case dragonfly.RoleStaleMaster:
			m.recordStaleMaster(fg, snap.Name)
			// Defense-in-depth: ensure the stale master is shed from
			// the active Service even if its labels somehow drift.
			// This is independent of the auto-reconfigure attempt below
			// — a stale master that fails the connected_slaves gate must
			// still not serve client traffic.
			if err := m.setTrafficLabel(ctx, fg, snap.Name, false); err != nil {
				m.logger.Info("dragonfly: strip stale-master traffic", "site", snap.Name, "error", err)
			}
			// Auto-reconfigure only a stale master no replica is
			// attached to (connected_slaves=0): one with replicas is
			// serving somebody, and REPLICAOF would take them with it.
			// That is all the gate says. It is not a sign of an instance
			// that never accepted writes: a master that holds the whole
			// cache with no replica attached, like a target just
			// promoted from a replica, has connected_slaves=0 too, and a
			// Dragonfly master reports no replication offset to tell
			// them apart (v1.38, v2.1; the gate once also required
			// master_repl_offset=0, which is always the case).
			//
			// So one case is refused: the stale master holds keys and
			// the active site holds none. REPLICAOF would discard the
			// only copy of the cache in favour of an empty instance,
			// which is what a wrongly recorded active site looks like.
			// The stale master stays as it is (out of the active
			// Service) until the active site holds a key or someone
			// decides. When the stale master is on the MySQL active
			// site it does not come to this: it is the active site
			// (dragonflyMasterOverEmptyRecord).
			//
			// And nothing is reattached to an active site that is held
			// although unobserved: if it never returns, the empty
			// master becomes the active site when the hold ends, which
			// it could not as a replica of a site that is gone.
			keepsOnlyCopy := snap.holdsKeys() && activeEmpty
			if !keepsOnlyCopy && !heldOverEmpty && snap.Info.ConnectedSlaves == 0 {
				m.attemptStaleMasterReconfigure(ctx, fg, snap.Name, password, masterHost, masterPort)
			}
			continue
		case dragonfly.RoleReplica:
			// Already a replica; re-issue REPLICAOF if either:
			//  (a) it points at the wrong master, or
			//  (b) the link to the right master is down.
			//
			// (b) covers the master-pod-restart case: the Service-backed
			// hostname stays the same but the underlying TCP connection to
			// the killed pod's IP dies. Dragonfly's auto-reconnect doesn't
			// always recover (especially after the operator's
			// --break_replication_on_master_restart kicks in on the new
			// master pod), so without this branch the replica silently
			// stays disconnected forever — same shape as upstream issue
			// #2044 (orphaned downstream replicas after topology change).
			// While the master is the instance the replica synced from,
			// or does not answer, REPLICAOF to the same target costs the
			// replica nothing: it is refused in under 200ms against a
			// dead master (`ERR replication cancelled`) and the replica
			// keeps its keys and the replication id it followed.
			//
			// It is not safe against a master that came back as a new
			// instance. With --break_replication_on_master_restart the
			// replica does not attach to it by itself: it stays a
			// replica with the link down, holding every key, and
			// REPLICAOF is what makes it discard them for whatever the
			// new instance holds. That replica is not re-attached while
			// it holds the cache the master lost
			// (dragonflyKeepsCacheOverRestartedMaster); Tick promotes it
			// instead, before this point.
			pointedRight := snap.Info.MasterHost == masterHost && snap.Info.MasterPort == int(masterPort)
			linkUp := snap.Info.MasterLinkStatus == "up"
			if pointedRight && linkUp {
				continue
			}
			if why := m.dragonflyKeepsCacheOverRestartedMaster(fg, active, activeSnap, &snap); why != "" {
				m.logger.Info("dragonfly: replica holds the cache its restarted master lost; not re-attaching it",
					"site", snap.Name, "master", active, "reason", why, "fg", m.fgKey.String())
				continue
			}
			m.applyReplicaOf(ctx, fg, snap.Name, password, masterHost, masterPort)
		case dragonfly.RoleUnconfigured:
			if snap.Name == active {
				// Fresh master; no replication to wire.
				continue
			}
			m.applyReplicaOf(ctx, fg, snap.Name, password, masterHost, masterPort)
		}
	}
}

// defaultDragonflyMasterLossThreshold is how many consecutive observations
// must miss the active Dragonfly master before the Dragonfly-only promotion
// replaces it, when spec.failureThreshold is not set. It is the default of
// that field, which is what the MySQL monitor counts before it calls a site
// Unreachable: the cache does not fail over on less evidence than the
// database.
//
// There is one threshold. A replica that reports its link to the master
// down does not shorten it: that is a state, not an event (a replica whose
// path to a live master is refused reports it for hours), and one missed
// observation of a healthy master beside it would replace the master with
// an old copy. What the shorter wait was for, a killed master whose pod
// returns empty before the threshold, is decided on what the returned
// instance and the replica report (dragonflyKeepsCacheOverRestartedMaster),
// not on how fast the operator counts.
const defaultDragonflyMasterLossThreshold = 3

// dragonflyMasterLossThreshold is the number of consecutive observations
// that must miss the active master before a replica is promoted in its
// place on the operator's observation alone: spec.failureThreshold.
func dragonflyMasterLossThreshold(fg *v1alpha1.MysqlFailoverGroup) int {
	if fg.Spec.FailureThreshold > 0 {
		return int(fg.Spec.FailureThreshold)
	}
	return defaultDragonflyMasterLossThreshold
}

// dragonflyMasterMissed reports whether this cycle's observation missed the
// active master: the site did not answer, or answered without saying that
// it is a master or a replica. An answer as a replica is not a miss: the
// site was reached and says it is no longer the master.
func dragonflyMasterMissed(s *DragonflySiteSnapshot) bool {
	if s == nil {
		return false
	}
	return !s.Reachable || (s.Info.Role != "master" && s.Info.Role != "slave")
}

// noteActiveMaster counts the consecutive observations that missed the
// active Dragonfly master. Any observation that reaches it, as a master or
// as a replica, ends the count, and so does a change of the active site.
//
// The misses have to follow each other. Every cycle that observes notes its
// result here, also one that then yields to another owner, so a cycle that
// reached the master always ends the count. A cycle that did not observe at
// all (the group's gate was busy) notes nothing, and the count does not
// survive that for long: a miss made more than dragonflyMissCountMaxGap
// after the one before it starts a new count. observedAt is when this
// cycle's observation began.
//
// The count is this process's: a restarted operator starts from zero, so a
// master that was already gone is replaced one threshold after the restart.
func (m *DragonflyManager) noteActiveMaster(fg *v1alpha1.MysqlFailoverGroup, snaps []DragonflySiteSnapshot, holdUnobserved bool, observedAt time.Time) {
	active := activeDragonflySiteFromObservation(fg, snaps, holdUnobserved)
	missed := active != "" && dragonflyMasterMissed(dragonflySnapshotOf(snaps, active))
	m.mu.Lock()
	defer m.mu.Unlock()
	if !missed {
		m.masterMissSite, m.masterMisses, m.masterWaitLogged = "", 0, false
		return
	}
	if m.masterMissSite != active {
		m.masterMissSite, m.masterMisses, m.masterWaitLogged = active, 0, false
	}
	if m.masterMisses > 0 && observedAt.Sub(m.masterMissAt) > m.missCountMaxGap() {
		m.masterMisses, m.masterWaitLogged = 0, false
	}
	m.masterMisses++
	m.masterMissAt = m.now()
}

// dragonflyMissCountMaxGapPolls is how many poll intervals may pass between
// the end of one cycle that missed the active master and the start of the
// next one for the two misses to count as consecutive. Cycles follow each
// other one poll interval apart, or without a pause when an observation
// ran into a timeout, so two intervals allow for one late cycle and no
// more.
const dragonflyMissCountMaxGapPolls = 2

// missCountMaxGap is dragonflyMissCountMaxGapPolls poll intervals.
func (m *DragonflyManager) missCountMaxGap() time.Duration {
	return dragonflyMissCountMaxGapPolls * m.effectivePollInterval()
}

// effectivePollInterval is the manager's poll interval, with Run's default.
func (m *DragonflyManager) effectivePollInterval() time.Duration {
	if m.pollInterval <= 0 {
		return 2 * time.Second
	}
	return m.pollInterval
}

// dragonflyLinkSighting is an observation of a site as a replica in stable
// sync: the replication id and address of the master it followed, and when.
type dragonflyLinkSighting struct {
	replID string
	host   string
	port   int
	at     time.Time
}

// noteLinkedReplicas records, for every site observed as a replica in
// stable sync (link up, no full sync in progress, not loading, following a
// replication id), whom it follows and when it was seen.
func (m *DragonflyManager) noteLinkedReplicas(snaps []DragonflySiteSnapshot) {
	now := m.now()
	m.mu.Lock()
	defer m.mu.Unlock()
	for i := range snaps {
		s := &snaps[i]
		if !s.Reachable || s.Info.Role != "slave" || s.Info.MasterLinkStatus != "up" ||
			s.Info.MasterSyncInProgress || s.Persist.Loading || s.Info.MasterReplID == "" {
			continue
		}
		m.replicaLinked[s.Name] = dragonflyLinkSighting{replID: s.Info.MasterReplID, host: s.Info.MasterHost, port: s.Info.MasterPort, at: now}
	}
}

// linkedSightingMaxAge is how old the last sighting of a replica in sync
// may be for it to say that the replica had its master's cache when the
// master's instance ended: the longest a master can stay unobserved before
// the threshold promotion has replaced it (the threshold's worth of cycles,
// each at most one poll interval and one failed observation long), plus one
// cycle for the sighting itself.
//
// It also bounds what a wrong answer costs. A replica that lost its link
// right after the sighting, to a master that went on taking writes and was
// then restarted from a snapshot of them, is promoted without the writes
// of at most this long.
func (m *DragonflyManager) linkedSightingMaxAge(fg *v1alpha1.MysqlFailoverGroup) time.Duration {
	cycle := m.effectivePollInterval() + dragonfly.DefaultDialTimeout + dragonfly.DefaultIOTimeout
	return time.Duration(dragonflyMasterLossThreshold(fg)+1) * cycle
}

// dragonflyReplicaOfEarlierMaster reports whether r is a replica left
// behind by a restart of the active master: it replicates from the active
// site's address, it follows a replication id other than the one the
// instance at that address has now, its link is down, it is not in a full
// sync or a load, and it holds keys.
//
// That is what a restart of the master leaves, on Dragonfly v1.38 and v2.1
// with --break_replication_on_master_restart (which the operator always
// sets): the new instance has a new master_replid, the replica keeps the
// old one, keeps every key and does not attach to the new instance by
// itself (the master lists it in state=preparation). A replica whose link
// dropped while the master lived on reports the master's own id and is not
// this. Any part that could not be read (a key count, a replication id on
// either side) makes it false.
func dragonflyReplicaOfEarlierMaster(fg *v1alpha1.MysqlFailoverGroup, active string, master, r *DragonflySiteSnapshot) bool {
	if master == nil || r == nil || r.Name == active {
		return false
	}
	if !master.Reachable || master.Info.Role != "master" || master.Info.MasterReplID == "" {
		return false
	}
	if !r.Reachable || r.Info.Role != "slave" || r.Info.MasterReplID == "" || r.Info.MasterReplID == master.Info.MasterReplID {
		return false
	}
	if r.Info.MasterLinkStatus != "down" || r.Info.MasterSyncInProgress || r.Persist.Loading || !r.holdsKeys() {
		return false
	}
	host, port := splitHostPort(dragonflyAddr(fg, active), dragonflyPort(fg.Spec.Dragonfly))
	return r.Info.MasterHost == host && r.Info.MasterPort == int(port)
}

// dragonflyKeepsCacheOverRestartedMaster returns why the replica r must not
// be re-attached to the active master, or "" when it may be. It is the
// answer for a replica left behind by a restart of the master
// (dragonflyReplicaOfEarlierMaster) when re-attaching it would discard
// cache that the master's new instance does not have:
//
//   - the new instance holds no keys: the replica holds the only copy;
//   - or this process saw the replica in stable sync with the earlier
//     instance (the same replication id, the same address) no longer ago
//     than linkedSightingMaxAge. The replica then holds what the earlier
//     instance held when it ended. Whatever the new instance holds is no
//     newer than that, apart from what was written to it since it started:
//     it either started empty and has taken a few writes (a busy cache
//     writes to a recreated master within a second of its return, so "no
//     keys" alone would miss this), or it loaded a snapshot the earlier
//     instance wrote.
//
// REPLICAOF would make the replica discard all of it for the new instance's
// content. Tick promotes the replica instead
// (restartedMasterPromotionCandidate): REPLICAOF NO ONE keeps its keys, and
// the restarted instance is then attached to it. What is given up is what
// was written to the restarted instance before the promotion.
//
// Without either piece of evidence the replica is re-attached as before:
// an operator that restarted during the outage has no sighting, and a
// master whose key count cannot be read is neither "empty" nor known to
// hold anything.
func (m *DragonflyManager) dragonflyKeepsCacheOverRestartedMaster(fg *v1alpha1.MysqlFailoverGroup, active string, master, r *DragonflySiteSnapshot) string {
	if !dragonflyReplicaOfEarlierMaster(fg, active, master, r) || master.Keys == nil {
		return ""
	}
	if master.holdsNoKeys() {
		return "the restarted master holds no keys"
	}
	m.mu.Lock()
	seen, ok := m.replicaLinked[r.Name]
	m.mu.Unlock()
	if !ok || seen.replID != r.Info.MasterReplID || seen.host != r.Info.MasterHost || seen.port != r.Info.MasterPort {
		return ""
	}
	if age := m.now().Sub(seen.at); age <= m.linkedSightingMaxAge(fg) {
		return fmt.Sprintf("the replica was in sync with the master's earlier instance %s ago", age.Truncate(time.Second))
	}
	return ""
}

// restartedMasterPromotionCandidate returns the replica to promote in place
// of an active master that restarted without the cache the replica holds
// (dragonflyKeepsCacheOverRestartedMaster), and the active site it replaces.
// With several such replicas it is the one that executed the most of the
// earlier instance's journal (slave_repl_offset), the first in spec order
// among equals; the others are then attached to it.
//
// This is not a failure-threshold decision: the active master answered, as
// another instance than the one the replica followed. It needs no missed
// observation and waits for none.
func (m *DragonflyManager) restartedMasterPromotionCandidate(fg *v1alpha1.MysqlFailoverGroup, snaps []DragonflySiteSnapshot, holdUnobserved bool) (target, oldSource string, ok bool) {
	active := activeDragonflySiteFromObservation(fg, snaps, holdUnobserved)
	if active == "" {
		return "", "", false
	}
	master := dragonflySnapshotOf(snaps, active)
	best := -1
	for i := range snaps {
		if m.dragonflyKeepsCacheOverRestartedMaster(fg, active, master, &snaps[i]) == "" {
			continue
		}
		if best < 0 || snaps[i].Info.SlaveReplOffset > snaps[best].Info.SlaveReplOffset {
			best = i
		}
	}
	if best < 0 {
		return "", "", false
	}
	return snaps[best].Name, active, true
}

// dragonflyOnlyPromotionCandidate returns the single healthy replica to
// promote when the current Dragonfly master is unreachable but MySQL has not
// failed over. The emergency gate is intentionally looser than planned
// failover: if the old master is gone, the replica's link is usually "down",
// so the durable signal is that it had previously received data
// (master_last_io_seconds_ago >= 0) and is not in a full sync/load cycle.
//
// No replica is promoted while the active site is held over an empty
// master (dragonflyHeldOverEmptyMaster): the active site was missed for
// less than the grace, and a promotion would add a third master. Before
// the hold existed that observation made the empty master the active
// site, so no replica was promoted in it either.
//
// One missed observation of the master is not its loss. The promotion
// waits until dragonflyMasterLossThreshold consecutive observations have
// missed it (noteActiveMaster), as the MySQL monitor does before it calls a
// site Unreachable; the wait is logged once per outage. One observation is
// evidence and does not wait for more: the active site answers as a
// replica. It was reached and is not the master.
//
// Nothing shortens the wait. A master that comes back within it, as a new
// instance without the cache, is handled when it answers
// (restartedMasterPromotionCandidate).
//
// The MySQL-follow promotions (Enqueue, mysqlActivePromotionCandidate) rest
// on MySQL's own decision and do not come through here.
func (m *DragonflyManager) dragonflyOnlyPromotionCandidate(fg *v1alpha1.MysqlFailoverGroup, snaps []DragonflySiteSnapshot, holdUnobserved bool) (target, oldSource string, ok bool) {
	active := activeDragonflySiteFromObservation(fg, snaps, holdUnobserved)
	if active == "" {
		return "", "", false
	}
	if holdUnobserved && dragonflyHeldOverEmptyMaster(fg, snaps) != "" {
		return "", "", false
	}

	activeObserved := false
	activeHealthyMaster := false
	var candidates []string
	for _, snap := range snaps {
		role := dragonfly.ClassifySiteRole(snap.Info, snap.Reachable, snap.Name, active)
		if snap.Name == active {
			activeObserved = true
			activeHealthyMaster = snap.Reachable && role == dragonfly.RoleMaster
			continue
		}
		if dragonflyOnlyPromotionReady(snap, role) {
			candidates = append(candidates, snap.Name)
		}
	}
	if !activeObserved || activeHealthyMaster || len(candidates) != 1 {
		return "", "", false
	}
	if !dragonflyMasterMissed(dragonflySnapshotOf(snaps, active)) {
		// Reached, and not a master: evidence, not a miss.
		return candidates[0], active, true
	}
	need := dragonflyMasterLossThreshold(fg)
	m.mu.Lock()
	defer m.mu.Unlock()
	misses := 0
	if m.masterMissSite == active {
		misses = m.masterMisses
	}
	if misses < need {
		if !m.masterWaitLogged {
			m.masterWaitLogged = true
			m.logger.Info("dragonfly: active master not observed; waiting before promoting a replica",
				"site", active, "target", candidates[0], "misses", misses, "threshold", need, "fg", m.fgKey.String())
		}
		return "", "", false
	}
	return candidates[0], active, true
}

func dragonflyOnlyPromotionReady(s DragonflySiteSnapshot, role dragonfly.SiteRole) bool {
	return s.Reachable &&
		role == dragonfly.RoleReplica &&
		s.Info.MasterLastIOSecondsAgo >= 0 &&
		!s.Info.MasterSyncInProgress &&
		!s.Persist.Loading
}

// attemptStaleMasterReconfigure tries to attach a stale master back to the
// active master as a replica, which discards what it holds. The caller
// (reconcileReplication) has already established that this is safe.
//
// Best-effort: errors are logged but not propagated. The next tick
// retries until the stale master becomes a replica or the operator
// manually intervenes.
func (m *DragonflyManager) attemptStaleMasterReconfigure(ctx context.Context, fg *v1alpha1.MysqlFailoverGroup, siteName, password, masterHost string, masterPort int32) {
	addr := dragonflyAddr(fg, siteName)
	conn, err := m.connector(ctx, addr, password)
	if err != nil {
		m.logger.Info("stale-master reconfigure: dial failed", "site", siteName, "error", err)
		return
	}
	defer func() { _ = conn.Close() }()
	if m.shouldYield() {
		return
	}
	if err := conn.ReplicaOf(ctx, masterHost, masterPort); err != nil {
		m.logger.Warn("stale-master reconfigure: REPLICAOF failed", "site", siteName, "host", masterHost, "port", masterPort, "error", err)
		return
	}
	m.logger.Info("stale-master reconfigure: REPLICAOF applied", "site", siteName, "host", masterHost, "port", masterPort)
	if m.recorder != nil {
		m.recorder.Eventf(fg, corev1.EventTypeNormal, v1alpha1.EventDragonflyOldSiteReconfigured,
			"stale Dragonfly master on site %q auto-reconfigured as replica of %s:%d (no replica was attached to it)",
			siteName, masterHost, masterPort)
	}
}

// applyReplicaOf opens a one-shot connection to the named site and
// issues REPLICAOF. Errors are logged but not surfaced to the caller —
// the next tick will retry.
func (m *DragonflyManager) applyReplicaOf(ctx context.Context, fg *v1alpha1.MysqlFailoverGroup, siteName, password, masterHost string, masterPort int32) {
	addr := dragonflyAddr(fg, siteName)
	conn, err := m.connector(ctx, addr, password)
	if err != nil {
		m.logger.Debug("dragonfly: dial replica for REPLICAOF", "site", siteName, "error", err)
		return
	}
	defer func() { _ = conn.Close() }()
	if m.shouldYield() {
		return
	}
	if err := conn.ReplicaOf(ctx, masterHost, masterPort); err != nil {
		m.logger.Warn("dragonfly: REPLICAOF failed", "site", siteName, "host", masterHost, "port", masterPort, "error", err)
		return
	}
	m.logger.Info("dragonfly: configured replica", "site", siteName, "host", masterHost, "port", masterPort)
}

// dragonflyPromotionRequest is a queued emergency promotion: the target to
// promote, the site it takes over from, and when it was requested.
type dragonflyPromotionRequest struct {
	target    string
	oldSource string
	issued    time.Time
}

// Enqueue asks the manager to promote target, taking over from oldSource,
// and returns at once. It never waits on Dragonfly or on the group's gate:
// it only replaces the pending request (the latest request wins, since it
// reflects the latest MySQL decision) and wakes Run, which executes it
// between Ticks and revalidates it against fresh state first. The MySQL
// emergency failover calls this, so the MySQL poll never waits for the
// cache. Requests made while the manager is stopping are dropped.
func (m *DragonflyManager) Enqueue(target, oldSource string) {
	if m.stopping.Load() {
		m.logger.Info("dragonfly promotion: manager stopping; request dropped", "site", target, "fg", m.fgKey.String())
		return
	}
	req := &dragonflyPromotionRequest{target: target, oldSource: oldSource, issued: time.Now()}
	m.pendingMu.Lock()
	prev := m.pending
	m.pending = req
	m.pendingMu.Unlock()
	if prev != nil {
		m.logger.Info("dragonfly promotion: request coalesced", "site", target, "replacedSite", prev.target, "fg", m.fgKey.String())
	}
	select {
	case m.wake <- struct{}{}:
	default:
	}
}

// runPending executes the pending request, if any, on the Run goroutine.
// Waiting for the gate ends if the manager stops; the promotion itself
// runs on a context the stop does not cancel.
func (m *DragonflyManager) runPending(ctx context.Context) {
	defer func() {
		if r := recover(); r != nil {
			m.logger.Error("dragonfly: panic recovered in promotion",
				"recovered", fmt.Sprintf("%v", r),
				"stack", string(debug.Stack()),
			)
			metrics.DragonflyManagerPanicsTotal.WithLabelValues(m.fgKey.Namespace, m.fgKey.Name).Inc()
		}
	}()
	// The request stays in the coalescing slot while Run waits for the
	// gate, so a newer MySQL decision made meanwhile replaces it; only the
	// request current when the gate is held is taken and run.
	if m.peekPending() == nil || m.stopping.Load() {
		return
	}
	release, err := m.gate.acquire(ctx)
	if err != nil {
		if req := m.takePending(); req != nil {
			m.logger.Info("dragonfly promotion: manager stopping; request dropped", "site", req.target, "fg", m.fgKey.String())
		}
		return
	}
	defer release()
	req := m.takePending()
	if req == nil {
		return
	}
	if m.stopping.Load() {
		m.logger.Info("dragonfly promotion: manager stopping; request dropped", "site", req.target, "fg", m.fgKey.String())
		return
	}
	m.executeRequest(context.WithoutCancel(ctx), req)
}

func (m *DragonflyManager) peekPending() *dragonflyPromotionRequest {
	m.pendingMu.Lock()
	defer m.pendingMu.Unlock()
	return m.pending
}

func (m *DragonflyManager) takePending() *dragonflyPromotionRequest {
	m.pendingMu.Lock()
	defer m.pendingMu.Unlock()
	req := m.pending
	m.pending = nil
	return req
}

// TryEmergencyPromote runs a best-effort Dragonfly promotion synchronously:
// it waits for the group's gate (bounded by ctx), then does what Run does
// for a queued request. The contract is strictly best-effort: this function
// never returns an error to the caller, every step is bounded, and it
// never leaves Dragonfly in a state that affects MySQL durability.
//
// REPLTAKEOVER gets the short emergencyTakeoverTimeout budget rather than
// the planned-failover maxSyncWait: in an emergency the old master is
// usually unreachable or stalled, and a long takeover only delays the
// REPLICAOF NO ONE fallback. See promoteLocked for the sequence.
func (m *DragonflyManager) TryEmergencyPromote(ctx context.Context, target, oldSource string) bool {
	req := &dragonflyPromotionRequest{target: target, oldSource: oldSource, issued: time.Now()}
	if m.stopping.Load() {
		return false
	}
	release, err := m.gate.acquire(ctx)
	if err != nil {
		return false
	}
	defer release()
	return m.executeRequest(context.WithoutCancel(ctx), req)
}

// executeRequest revalidates a request against the group as it is now
// and runs the promotion. The caller holds the gate.
func (m *DragonflyManager) executeRequest(ctx context.Context, req *dragonflyPromotionRequest) bool {
	getCtx, cancel := context.WithTimeout(ctx, m.emergencyStepTimeout)
	defer cancel()
	var fg v1alpha1.MysqlFailoverGroup
	if err := m.authority().Get(getCtx, m.fgKey, &fg); err != nil {
		m.logger.Error("dragonfly emergency: get fg", "error", err)
		return false
	}
	if !dragonflyEnabled(&fg) {
		return false
	}
	if m.paused() || m.plannedOwnsDragonfly(&fg) || dragonflyUpgradeInFlight(dragonflyUpgradeStatus(&fg)) {
		// A planned failover or snapshot upgrade owns Dragonfly. During an
		// upgrade nothing is stored for later: the upgrade reconcile
		// re-derives the follow-up from MySQL authority (§8.3).
		m.logger.Info("dragonfly promotion: planned failover in flight; skipping", "site", req.target, "fg", m.fgKey.String())
		return false
	}
	if why := supersededPromotionRequest(&fg, req); why != "" {
		m.logger.Info("dragonfly promotion: request superseded by a newer decision; skipping", "site", req.target, "reason", why, "fg", m.fgKey.String())
		return false
	}
	if df := fg.Status.Dragonfly; df != nil && df.ActiveSite == req.target && df.LastPromotionTarget == req.target && df.LastPromotionTime != nil &&
		!df.LastPromotionTime.Time.Before(req.issued.Truncate(time.Second)) {
		// Another promotion to the same target finished after this request
		// was made; repeating it would only duplicate events and metrics.
		m.logger.Info("dragonfly promotion: target already promoted since the request; skipping", "site", req.target, "fg", m.fgKey.String())
		return true
	}
	if df := fg.Status.Dragonfly; df != nil && df.ActiveSite == req.target {
		// Status already names the target. If it is still master, an
		// earlier promotion or Tick did the work: promoting a master again
		// would only fall through to REPLICAOF NO ONE and report sessions
		// lost that were kept.
		if role, err := m.observeRole(ctx, &fg, req.target, m.fetchPassword(getCtx, &fg)); err == nil && role == "master" {
			m.logger.Info("dragonfly promotion: target already promoted since the request; skipping", "site", req.target, "fg", m.fgKey.String())
			return true
		}
	}
	return m.promoteLocked(ctx, &fg, req.target, req.oldSource, m.emergencyTakeoverTimeout)
}

// TryRolloutPromote promotes target for a Dragonfly rollout whose target
// has already been proven in sync with a live source. It waits for the
// group's gate (bounded by ctx) and keeps the planned takeover budget
// (maxSyncWait/2) so sessions are preserved whenever the source can
// complete the handshake.
func (m *DragonflyManager) TryRolloutPromote(ctx context.Context, target, oldSource string) bool {
	if m.stopping.Load() {
		return false
	}
	release, err := m.gate.acquire(ctx)
	if err != nil {
		return false
	}
	defer release()
	return m.rolloutPromoteLocked(context.WithoutCancel(ctx), target, oldSource)
}

// rolloutPromoteLocked is TryRolloutPromote for a caller that already holds
// the group's gate.
func (m *DragonflyManager) rolloutPromoteLocked(work context.Context, target, oldSource string) bool {
	getCtx, cancel := context.WithTimeout(work, m.emergencyStepTimeout)
	defer cancel()
	var fg v1alpha1.MysqlFailoverGroup
	if err := m.authority().Get(getCtx, m.fgKey, &fg); err != nil {
		m.logger.Error("dragonfly rollout: get fg", "error", err)
		return false
	}
	why := rolloutPromotionBlocked(&fg, target, oldSource, m.plannedOwnsDragonfly(&fg))
	if why != "" {
		m.logger.Info("dragonfly rollout: promotion no longer valid; skipping", "site", target, "reason", why, "fg", m.fgKey.String())
		return false
	}
	return m.rolloutPromoteFG(work, &fg, target, oldSource)
}

// rolloutPromoteFG promotes target for a rollout from fg, a read of the
// group the caller made under the gate and validated with
// rolloutPromotionBlocked.
func (m *DragonflyManager) rolloutPromoteFG(work context.Context, fg *v1alpha1.MysqlFailoverGroup, target, oldSource string) bool {
	return m.promoteLocked(work, fg, target, oldSource, effectiveDragonflyMaxSyncWait(fg)/2)
}

// rolloutPromotionBlocked returns why a rollout promotion of target, taking
// over from oldSource, must not run against fg (read under the gate), or ""
// if it may. The rollout chose target and oldSource from an earlier read; by
// the time it holds the gate another owner may have moved Dragonfly.
func rolloutPromotionBlocked(fg *v1alpha1.MysqlFailoverGroup, target, oldSource string, plannedOwns bool) string {
	switch {
	case !dragonflyEnabled(fg):
		return "dragonfly disabled"
	case plannedOwns:
		return "planned failover in flight"
	case dragonflyUpgradeInFlight(dragonflyUpgradeStatus(fg)):
		return "snapshot upgrade in flight"
	}
	if master := effectiveDragonflyMasterSite(fg); master != oldSource {
		return fmt.Sprintf("active Dragonfly site is now %q, not %q", master, oldSource)
	}
	if target == oldSource {
		return "target is the active site"
	}
	return ""
}

// supersededPromotionRequest returns why a queued MySQL-follow request is
// obsolete against fg (read under the gate), or "" if it still stands. The
// request reflects a MySQL decision; it is obsolete once a later decision,
// recorded durably after it was made, points elsewhere:
//   - a planned failover claimed at or after the request (a run that is
//     only waiting has decided nothing);
//   - a MySQL failover to another site recorded at or after the request;
//   - a successful Dragonfly promotion to another site stamped at or after
//     the request's second (status times have one-second resolution),
//     while MySQL does not name the request's target as its active site.
//     Once MySQL has published the target, the request is its latest
//     published decision: a newer unpublished one would already have
//     replaced it in the slot, and a newer published one is caught above.
//
// The callback runs before MySQL publishes its own decision, so a status
// still naming the old source is not by itself a reason to drop it.
// Dragonfly-only promotions are decided by Tick from its own observation
// and never come through here.
func supersededPromotionRequest(fg *v1alpha1.MysqlFailoverGroup, req *dragonflyPromotionRequest) string {
	issued := req.issued.Truncate(time.Second)
	if pf := fg.Status.PlannedFailover; pf != nil {
		if at := pf.ClaimTime; at != nil && !at.Time.Before(issued) {
			return fmt.Sprintf("planned failover to %q claimed the group after the request", pf.TargetSite)
		}
	}
	if t := fg.Status.LastFailoverTarget(); t != "" && t != req.target && fg.Status.LastFailoverTime() != nil && !fg.Status.LastFailoverTime().Time.Before(issued) {
		return fmt.Sprintf("MySQL failed over to %q after the request", t)
	}
	if df := fg.Status.Dragonfly; df != nil && df.LastPromotionTime != nil && df.LastPromotionTarget != "" &&
		df.LastPromotionTarget != req.target && df.ActiveSite == df.LastPromotionTarget {
		stamped := df.LastPromotionTime.Time
		if fg.Status.ActiveSite != req.target && !stamped.Before(issued) {
			return fmt.Sprintf("Dragonfly was promoted to %q after the request", df.LastPromotionTarget)
		}
	}
	return ""
}

// defaultEmergencyWaits sizes the client-side waits around a REPLTAKEOVER
// carrying takeover:
//   - takeoverWait bounds the REPLTAKEOVER call itself: the whole-second
//     budget the server gets, plus a little slack for the reply.
//   - fallbackWait bounds the re-dial and REPLICAOF NO ONE after a failed
//     takeover. Dragonfly's replica keeps holding the lock REPLICAOF needs
//     until its own takeover round trip ends (budget + 10s), so the
//     fallback must be allowed to wait behind it.
func defaultEmergencyWaits(takeover time.Duration) (takeoverWait, fallbackWait time.Duration) {
	return dragonfly.ReplTakeoverWireBudget(takeover) + emergencyTakeoverReplySlack,
		dragonfly.ReplTakeoverServerHold(takeover) + emergencyFallbackGrace
}

// defaultEmergencySettle is how long after a role-changing command was
// sent the server may still be acting on it: for a REPLTAKEOVER, until the
// replica's takeover round trip ends (budget + 10s); for a REPLICAOF NO
// ONE sent without a takeover (takeover == 0), a second. An unconfirmed
// outcome is resolved by observing the target's role only after this.
func defaultEmergencySettle(takeover time.Duration) time.Duration {
	if takeover <= 0 {
		return time.Second
	}
	return dragonfly.ReplTakeoverServerHold(takeover) + time.Second
}

// noDragonflyTakeover is promoteLocked's takeover budget for a promotion
// that must not send REPLTAKEOVER: the target is promoted with REPLICAOF NO
// ONE at once.
const noDragonflyTakeover time.Duration = -1

const (
	// defaultEmergencyTakeoverTimeout is the REPLTAKEOVER budget for an
	// emergency promotion. A takeover against a healthy, in-sync master
	// completes in milliseconds; one that needs longer is waiting on a
	// master that is not answering.
	defaultEmergencyTakeoverTimeout = 2 * time.Second
	emergencyTakeoverReplySlack     = 3 * time.Second
	emergencyFallbackGrace          = 2 * time.Second
	// defaultEmergencyStepTimeout bounds each non-takeover phase: the
	// preliminary API reads and target dial, the direct REPLICAOF NO ONE
	// when no takeover was sent, the post-promotion label and CLIENT KILL
	// work, the role observation that resolves an unconfirmed outcome, and
	// the status stamp. Each phase has its own context so one that runs
	// long cannot starve the next.
	defaultEmergencyStepTimeout = 5 * time.Second
	// defaultEmergencySourceProbeTimeout bounds the dial+PING of the old
	// master that decides whether a takeover is worth attempting.
	defaultEmergencySourceProbeTimeout = time.Second
)

// promoteLocked promotes target to Dragonfly master. The caller holds the
// group's gate and passes a fresh read of the group; ctx is not cancelled
// by a manager stop, so once a role-changing command is sent the sequence
// runs to the end of its budgets and records the outcome.
//
//  1. Best-effort strip the old source's traffic label so the active
//     Service sheds it before we promote. Skipped silently if the
//     source pod is gone.
//  2. Probe the old source. TAKEOVER is coordinated by the master, so it
//     cannot succeed against an unreachable one, and while it waits the
//     replica holds the lock REPLICAOF NO ONE needs. If the old source does
//     not answer, skip straight to step 4.
//  3. Try REPLTAKEOVER on the target, bounded by its own context. If that
//     succeeds, sessions are preserved (best case).
//  4. Otherwise REPLICAOF NO ONE on the target, on a fresh connection and
//     its own context. The target becomes a master and keeps every key it
//     holds (300 of 300 on v1.38 and v2.1); what is lost is what the old
//     master had written and the target had not received. The log line
//     and the Event still say "sessions lost" for that.
//     With takeover == noDragonflyTakeover steps 2 and 3 are skipped: the
//     target does not follow the instance that answers at the old source's
//     address, so there is nothing to take over from.
//  5. If the outcome is unconfirmed (the fallback could not be sent or its
//     reply was lost), wait until the server can no longer be acting on
//     the commands, then observe the target's role: a master is a
//     completed promotion, anything else a failed one.
//  6. On a completed promotion, stamp role+traffic on the target pod,
//     best-effort CLIENT KILL the old source, recreate it, and stamp
//     status. On a failed one, restore the old source's traffic label.
//
// The manager yields (stopping, or a planned failover or rollout waiting
// for the gate) only before step 1 and before step 3, never after a
// role-changing command has been sent.
func (m *DragonflyManager) promoteLocked(ctx context.Context, fg *v1alpha1.MysqlFailoverGroup, target, oldSource string, takeover time.Duration) bool {
	fgKey := m.fgKey.String()
	if m.shouldYield() {
		m.logger.Info("dragonfly promotion: yielding to another owner; skipping", "site", target, "fg", fgKey)
		return false
	}
	prepCtx, cancelPrep := context.WithTimeout(ctx, m.emergencyStepTimeout)
	defer cancelPrep()

	oldSource = inferDragonflyPromotionSource(fg, target, oldSource)
	password := m.fetchPassword(prepCtx, fg)
	addr := dragonflyAddr(fg, target)

	// Step 1: strip old source traffic. Always safe even if the source
	// is dead — the K8s API call still succeeds (label patch on a pod
	// that may already be gone is a no-op).
	if oldSource != "" {
		if err := m.setTrafficLabel(prepCtx, fg, oldSource, false); err != nil {
			m.logger.Info("dragonfly emergency: strip old source traffic failed (proceeding)", "site", oldSource, "error", err)
		}
	}

	conn, err := m.connector(prepCtx, addr, password)
	if err != nil {
		m.logger.Warn("dragonfly emergency: target unreachable; skipping promotion", "site", target, "error", err, "fg", fgKey)
		metrics.DragonflyPromotionsTotal.WithLabelValues(fg.Name, target, "failed").Inc()
		if m.recorder != nil {
			m.recorder.Eventf(fg, corev1.EventTypeWarning, v1alpha1.EventDragonflyPromotionFailed,
				"emergency: target Dragonfly %q unreachable; cache continuity unavailable", target)
		}
		m.stampPromotionWithin(ctx, target, false)
		return false
	}

	if m.shouldYield() {
		_ = conn.Close()
		m.logger.Info("dragonfly promotion: yielding to another owner; skipping", "site", target, "fg", fgKey)
		m.restoreSourceTraffic(ctx, fg, oldSource)
		return false
	}

	takeoverWait, fallbackWait := m.emergencyWaits(takeover)
	var settleAt time.Time
	takeoverAttempted := false
	takeoverFailure := "REPLTAKEOVER failed"
	if takeover == noDragonflyTakeover {
		takeoverFailure = "REPLTAKEOVER not sent: the target follows an earlier instance of the old master"
		// Nothing holds the replica's REPLICAOF lock and the open
		// connection carries no pending reply.
		fallbackWait = m.emergencyStepTimeout
	} else if m.dragonflyReachable(ctx, fg, oldSource, password) {
		takeoverAttempted = true
		settleAt = time.Now().Add(m.emergencySettle(takeover))
		takeoverCtx, cancelTakeover := context.WithTimeout(ctx, takeoverWait)
		takeoverErr := conn.ReplTakeover(takeoverCtx, takeover)
		cancelTakeover()
		// RESP has no req/reply correlation. After a REPLTAKEOVER error the
		// underlying connection may have a late server reply pending in the
		// read buffer; reusing it for REPLICAOF NO ONE would either consume
		// that stale reply or interleave with one. Close and dial fresh.
		_ = conn.Close()
		if takeoverErr == nil {
			m.logger.Info("dragonfly emergency: REPLTAKEOVER succeeded", "site", target, "fg", fgKey)
			metrics.DragonflyPromotionsTotal.WithLabelValues(fg.Name, target, "success").Inc()
			if m.recorder != nil {
				m.recorder.Eventf(fg, corev1.EventTypeNormal, v1alpha1.EventDragonflyPromotionSucceeded,
					"emergency: target Dragonfly %q promoted via REPLTAKEOVER (best-effort)", target)
			}
			m.finishDragonflyPromotion(ctx, fg, target, oldSource, password)
			return true
		}
		m.logger.Warn("dragonfly emergency: REPLTAKEOVER failed; falling back", "site", target, "error", takeoverErr, "fg", fgKey)
		// Whatever the error, this path does not conclude that no role
		// changed (a takeover can complete on the master after the
		// replica answered `Operation canceled`, or after the reply was
		// lost): it sends REPLICAOF NO ONE, which makes the target a
		// master holding its keys if it is not one already, and an old
		// master that then exits and returns empty finds the target
		// promoted.
	} else {
		takeoverFailure = "REPLTAKEOVER skipped: old master unreachable"
		m.logger.Warn("dragonfly emergency: old master unreachable; skipping REPLTAKEOVER", "site", target, "oldSource", oldSource, "fg", fgKey)
		// No takeover was sent, so nothing holds the replica's REPLICAOF
		// lock and the open connection carries no pending reply.
		fallbackWait = m.emergencyStepTimeout
	}

	fallbackCtx, cancelFallback := context.WithTimeout(ctx, fallbackWait)
	defer cancelFallback()
	if takeoverAttempted {
		freshConn, freshErr := m.connector(fallbackCtx, addr, password)
		if freshErr != nil {
			// The fallback was never sent, but the takeover's outcome is
			// unknown: it may still complete on the server.
			m.logger.Warn("dragonfly emergency: re-dial for REPLICAOF NO ONE failed", "site", target, "error", freshErr, "fg", fgKey)
			return m.resolveUnconfirmedPromotion(ctx, fg, target, oldSource, password, settleAt,
				fmt.Sprintf("REPLTAKEOVER failed; re-dial for fallback also failed: %v", freshErr))
		}
		conn = freshConn
	}
	if settleAt.IsZero() {
		settleAt = time.Now().Add(m.emergencySettle(0))
	}
	noOneErr := conn.ReplicaOfNoOne(fallbackCtx)
	_ = conn.Close()
	if noOneErr != nil {
		m.logger.Warn("dragonfly emergency: REPLICAOF NO ONE failed", "site", target, "error", noOneErr, "fg", fgKey)
		return m.resolveUnconfirmedPromotion(ctx, fg, target, oldSource, password, settleAt,
			fmt.Sprintf("%s; REPLICAOF NO ONE also failed: %v", takeoverFailure, noOneErr))
	}
	m.logger.Info("dragonfly emergency: target promoted via REPLICAOF NO ONE (sessions lost)", "site", target, "fg", fgKey)
	what := "promoted via REPLICAOF NO ONE (sessions lost)"
	if takeover == noDragonflyTakeover {
		what = fmt.Sprintf("promoted via REPLICAOF NO ONE with the keys it holds; what was written to the restarted master %q since it started is discarded", oldSource)
	}
	m.recordSessionsLostPromotion(fg, target, what)
	m.finishDragonflyPromotion(ctx, fg, target, oldSource, password)
	return true
}

// resolveUnconfirmedPromotion settles a promotion whose role-changing
// commands may or may not have taken effect: a reply was lost, the fallback
// could not be sent after a takeover, or the fallback returned an error the
// server may not have acted on. It waits until settleAt, when the server
// can no longer be acting on what was sent, then observes the target's
// role. A master is a completed promotion (recorded in full); anything
// else is a failed one, which restores the old source's traffic label.
func (m *DragonflyManager) resolveUnconfirmedPromotion(ctx context.Context, fg *v1alpha1.MysqlFailoverGroup, target, oldSource, password string, settleAt time.Time, cause string) bool {
	fgKey := m.fgKey.String()
	if wait := time.Until(settleAt); wait > 0 {
		timer := time.NewTimer(wait)
		<-timer.C
	}
	role, err := m.observeRole(ctx, fg, target, password)
	if err == nil && role == "master" {
		m.logger.Warn("dragonfly emergency: promotion outcome unconfirmed; target observed as master", "site", target, "cause", cause, "fg", fgKey)
		m.recordSessionsLostPromotion(fg, target, "promoted; the reply was lost, confirmed by observing its role (sessions may be lost)")
		m.finishDragonflyPromotion(ctx, fg, target, oldSource, password)
		return true
	}
	if err != nil {
		role = "unknown: " + err.Error()
	}
	m.logger.Warn("dragonfly emergency: promotion failed; target not observed as master", "site", target, "role", role, "cause", cause, "fg", fgKey)
	metrics.DragonflyPromotionsTotal.WithLabelValues(fg.Name, target, "failed").Inc()
	if m.recorder != nil {
		m.recorder.Eventf(fg, corev1.EventTypeWarning, v1alpha1.EventDragonflyPromotionFailed,
			"emergency: target Dragonfly %q failed to promote (%s); observed role %s", target, cause, role)
	}
	m.restoreSourceTraffic(ctx, fg, oldSource)
	m.stampPromotionWithin(ctx, target, false)
	return false
}

// recordSessionsLostPromotion emits the metric and Events for a promotion
// that went through REPLICAOF NO ONE or whose outcome was only confirmed by
// observation.
func (m *DragonflyManager) recordSessionsLostPromotion(fg *v1alpha1.MysqlFailoverGroup, target, what string) {
	metrics.DragonflyPromotionsTotal.WithLabelValues(fg.Name, target, "sessions_lost").Inc()
	if m.recorder != nil {
		m.recorder.Eventf(fg, corev1.EventTypeWarning, v1alpha1.EventDragonflySessionsLost,
			"emergency: target Dragonfly %q %s", target, what)
	}
}

// observeRole reads site's replication role on a fresh connection within
// the step budget.
func (m *DragonflyManager) observeRole(ctx context.Context, fg *v1alpha1.MysqlFailoverGroup, site, password string) (string, error) {
	obsCtx, cancel := context.WithTimeout(ctx, m.emergencyStepTimeout)
	defer cancel()
	conn, err := m.connector(obsCtx, dragonflyAddr(fg, site), password)
	if err != nil {
		return "", err
	}
	defer func() { _ = conn.Close() }()
	info, err := conn.InfoReplication(obsCtx)
	if err != nil {
		return "", err
	}
	return info.Role, nil
}

// restoreSourceTraffic gives the old source back its traffic label after a
// promotion that did not happen, so the active Service is not left empty.
func (m *DragonflyManager) restoreSourceTraffic(ctx context.Context, fg *v1alpha1.MysqlFailoverGroup, oldSource string) {
	if oldSource == "" {
		return
	}
	restoreCtx, cancel := context.WithTimeout(ctx, m.emergencyStepTimeout)
	defer cancel()
	if err := m.setTrafficLabel(restoreCtx, fg, oldSource, true); err != nil {
		m.logger.Info("dragonfly emergency: restore old source traffic failed", "site", oldSource, "error", err)
	}
}

// dragonflyReachable reports whether site's Dragonfly answers a dial and
// PING within the source-probe budget. An empty site (no known old
// source) counts as reachable so the takeover is still attempted.
func (m *DragonflyManager) dragonflyReachable(ctx context.Context, fg *v1alpha1.MysqlFailoverGroup, site, password string) bool {
	if site == "" {
		return true
	}
	probeCtx, cancel := context.WithTimeout(ctx, m.emergencySourceProbeTimeout)
	defer cancel()
	conn, err := m.connector(probeCtx, dragonflyAddr(fg, site), password)
	if err != nil {
		return false
	}
	defer func() { _ = conn.Close() }()
	return conn.Ping(probeCtx) == nil
}

// finishDragonflyPromotion runs the post-promotion label, CLIENT KILL and
// source-recreate work, then stamps the successful promotion. The two get
// separate step budgets so slow label patches cannot drop the stamp.
func (m *DragonflyManager) finishDragonflyPromotion(ctx context.Context, fg *v1alpha1.MysqlFailoverGroup, target, oldSource, password string) {
	postCtx, cancel := context.WithTimeout(ctx, m.emergencyStepTimeout)
	sourceDemoted := m.applyEmergencyPromotionLabels(postCtx, fg, target, oldSource)
	m.bestEffortEmergencyClientKill(postCtx, fg, oldSource, password)
	if sourceDemoted {
		m.bestEffortRecreateTakeoverSource(postCtx, fg, oldSource)
	}
	cancel()
	m.stampPromotionWithin(ctx, target, true)
}

// stampPromotionWithin stamps the promotion outcome on its own step budget.
func (m *DragonflyManager) stampPromotionWithin(ctx context.Context, target string, success bool) {
	stampCtx, cancel := context.WithTimeout(ctx, m.emergencyStepTimeout)
	defer cancel()
	m.stampPromotion(stampCtx, target, success)
}

// applyEmergencyPromotionLabels stamps role=master+traffic=true on
// the target site and demotes the old source to role=replica with its
// traffic label restored.
//
// Demote MUST succeed before we restore the source's traffic label.
// Otherwise the source pod stays labelled dragonfly-role=master with
// traffic=true and gets selected by the active Service alongside
// the newly-promoted target — split-brain at the routing layer.
//
// Other patches are logged-and-dropped because the reconciler's
// syncDragonflyPodLabels sweep will re-converge any cosmetic drift
// once the role flip succeeded.
func (m *DragonflyManager) applyEmergencyPromotionLabels(ctx context.Context, fg *v1alpha1.MysqlFailoverGroup, target, oldSource string) bool {
	if err := m.setRoleLabel(ctx, fg, target, "master"); err != nil {
		m.logger.Info("emergency: stamp target role=master", "site", target, "error", err)
	}
	if err := m.setTrafficLabel(ctx, fg, target, true); err != nil {
		m.logger.Info("emergency: stamp target traffic=true", "site", target, "error", err)
	}
	if oldSource == "" {
		return true
	}
	if err := m.setRoleLabel(ctx, fg, oldSource, "replica"); err != nil {
		m.logger.Warn("emergency: stamp old source role=replica failed; leaving traffic stripped to avoid split-brain", "site", oldSource, "error", err)
		if m.recorder != nil {
			m.recorder.Eventf(fg, corev1.EventTypeWarning, v1alpha1.EventDragonflyPromotionFailed,
				"emergency: demote of old source %q failed (%v); source kept out of active Service to avoid split-brain. Manual intervention required.",
				oldSource, err)
		}
		return false
	}
	if err := m.setTrafficLabel(ctx, fg, oldSource, true); err != nil {
		// Demote succeeded so the active Service ignores the source
		// (role=replica). Traffic-label drift is cosmetic; log only.
		m.logger.Info("emergency: restore old source traffic=true", "site", oldSource, "error", err)
	}
	return true
}

// bestEffortEmergencyClientKill issues CLIENT KILL TYPE NORMAL against
// the old source, if reachable, to evict any clients still attached.
// Bounded context, all errors logged and dropped.
//
// Log lines include `fg` per the site/content/docs/8.observability/7.log-schema.md Event
// reference contract.
func (m *DragonflyManager) bestEffortEmergencyClientKill(ctx context.Context, fg *v1alpha1.MysqlFailoverGroup, oldSource, password string) {
	if oldSource == "" {
		return
	}
	fgKey := fg.Namespace + "/" + fg.Name
	addr := dragonflyAddr(fg, oldSource)
	dialCtx, cancel := context.WithTimeout(ctx, 2*time.Second)
	defer cancel()
	conn, err := m.connector(dialCtx, addr, password)
	if err != nil {
		m.logger.Info("emergency client-kill: old source unreachable; skipping", "fg", fgKey, "site", oldSource, "error", err)
		return
	}
	defer func() { _ = conn.Close() }()
	if err := conn.ClientKillType(dialCtx, "NORMAL"); err != nil {
		m.logger.Info("emergency client-kill: old source rejected CLIENT KILL", "fg", fgKey, "site", oldSource, "error", err)
		return
	}
	m.logger.Info("emergency client-kill: evicted clients from old source", "fg", fgKey, "site", oldSource)
}

func inferDragonflyPromotionSource(fg *v1alpha1.MysqlFailoverGroup, target, oldSource string) string {
	if oldSource != "" {
		return oldSource
	}
	if fg.Status.Dragonfly != nil && fg.Status.Dragonfly.ActiveSite != "" && fg.Status.Dragonfly.ActiveSite != target {
		return fg.Status.Dragonfly.ActiveSite
	}
	if fg.Status.ActiveSite != "" && fg.Status.ActiveSite != target {
		return fg.Status.ActiveSite
	}
	if len(fg.Spec.Sites) == 2 {
		for _, site := range fg.Spec.Sites {
			if site.Name != target {
				return site.Name
			}
		}
	}
	return ""
}

// bestEffortRecreateTakeoverSource deletes the demoted source pod after a
// successful REPLTAKEOVER. Current Dragonfly releases exit the source
// process as part of takeover; deleting the pod creates a fresh UID and
// avoids waiting for kubelet's CrashLoopBackOff delay before the site can
// rejoin as a replica.
func (m *DragonflyManager) bestEffortRecreateTakeoverSource(ctx context.Context, fg *v1alpha1.MysqlFailoverGroup, oldSource string) {
	if oldSource == "" {
		return
	}
	if err := deleteDragonflyPodsForSite(ctx, m.client, fg, oldSource); err != nil {
		m.logger.Info("dragonfly takeover source recreate: delete pod failed", "site", oldSource, "error", err)
		return
	}
	m.logger.Info("dragonfly takeover source recreate: deleted demoted source pod", "site", oldSource)
}

// setRoleLabel patches the dragonfly-role label on every pod for the
// given site. Delegates to the package-level helper so the manager and
// reconciler share one implementation.
func (m *DragonflyManager) setRoleLabel(ctx context.Context, fg *v1alpha1.MysqlFailoverGroup, siteName, role string) error {
	return setDragonflyRoleOnSite(ctx, m.client, fg, siteName, role)
}

// setTrafficLabel sets or removes the dragonfly-traffic label on every
// pod for the given site. Delegates to the package-level helper.
func (m *DragonflyManager) setTrafficLabel(ctx context.Context, fg *v1alpha1.MysqlFailoverGroup, siteName string, set bool) error {
	return setDragonflyTrafficOnSite(ctx, m.client, fg, siteName, set)
}

// stampPromotion writes lastPromotionTime/Target, and writes
// status.dragonfly.activeSite only after a successful promotion. Wrapped in RetryOnConflict so a concurrent
// patchStatus write from the manager's own poll loop or the
// reconciler's syncDragonflyPodLabels sweep doesn't silently drop the
// promotion status.
func (m *DragonflyManager) stampPromotion(ctx context.Context, target string, success bool) {
	now := metav1.Now()
	if err := stampDragonflyPromotionStatus(ctx, m.authority(), m.client, m.fgKey, target, now, success); err != nil {
		m.logger.Warn("stampPromotion: status patch", "error", err)
	}
}

// stampDragonflyPromotionStatus reads the group through reader (uncached
// in production, so the merge patch is computed against current status)
// and patches it through c.
func stampDragonflyPromotionStatus(ctx context.Context, reader client.Reader, c client.Client, nn types.NamespacedName, target string, when metav1.Time, success bool) error {
	return k8sretry.RetryOnConflict(k8sretry.DefaultRetry, func() error {
		var fg v1alpha1.MysqlFailoverGroup
		if err := reader.Get(ctx, nn, &fg); err != nil {
			return err
		}
		base := fg.DeepCopy()
		if fg.Status.Dragonfly == nil {
			fg.Status.Dragonfly = &v1alpha1.DragonflyStatus{}
		}
		if success {
			if fg.Status.Dragonfly.ActiveSite != target {
				restateDragonflyStatusForPromotion(fg.Status.Dragonfly, target)
			}
			fg.Status.Dragonfly.ActiveSite = target
		}
		fg.Status.Dragonfly.LastPromotionTime = &when
		fg.Status.Dragonfly.LastPromotionTarget = target
		return c.Status().Patch(ctx, &fg, client.MergeFrom(base))
	})
}

// restateDragonflyStatusForPromotion takes a promotion that moves
// status.dragonfly.activeSite to target out of the Ready phase. The phase
// and sites describe the last observation, made against the previous
// active site and before the promotion; with activeSite moved they would
// read Ready with the master on the old site until the next observation
// rebuilds them. ConfiguringReplication is what the topology is after a
// promotion: the new master is up and the old one has to rejoin as its
// replica. The sites stay as observed.
func restateDragonflyStatusForPromotion(st *v1alpha1.DragonflyStatus, target string) {
	st.Phase = v1alpha1.DragonflyPhaseConfiguringReplication
	st.Message = fmt.Sprintf("promoted %s; waiting for the next observation of the sites", target)
}

// recordStaleMaster emits a deduplicated Event for a stale master on a
// non-active site. The dedup window keeps the Event stream readable when
// the state persists across many polls.
func (m *DragonflyManager) recordStaleMaster(fg *v1alpha1.MysqlFailoverGroup, siteName string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	now := time.Now()
	if last, ok := m.lastStaleMasterAt[siteName]; ok && now.Sub(last) < m.staleMasterEventTTL {
		return
	}
	m.lastStaleMasterAt[siteName] = now
	m.logger.Warn("dragonfly: stale master on non-active site", "site", siteName, "active", fg.Status.ActiveSite)
	if m.recorder != nil {
		// Auto-reconfigure is attempted in reconcileReplication under
		// its gate; this Event fires on every detection regardless of
		// whether the auto-rejoin gate passed.
		m.recorder.Eventf(fg, corev1.EventTypeWarning, v1alpha1.EventDragonflyStaleMasterDetected,
			"stale Dragonfly master detected on site %q (active=%q); auto-rejoin is attempted only when no replica is attached to it, and not when it holds keys while the active site holds none",
			siteName, fg.Status.ActiveSite)
	}
}

// dragonflyAddr returns "<svc>.<ns>.svc.cluster.local:<port>" for a site.
// Going through DNS rather than caching pod IPs keeps us oblivious to
// pod restarts and IP rotations.
func dragonflyAddr(fg *v1alpha1.MysqlFailoverGroup, siteName string) string {
	return fmt.Sprintf("%s.%s.svc.cluster.local:%d",
		dragonflySiteServiceName(fg.Name, siteName), fg.Namespace, dragonflyPort(fg.Spec.Dragonfly))
}

// splitHostPort returns the hostname and parsed port for a
// dragonflyAddr. Used when issuing REPLICAOF on a follower site.
// Falls back to defaultPort if the addr has no `:` separator or the
// suffix is not a valid integer port.
func splitHostPort(addr string, defaultPort int32) (string, int32) {
	// dragonflyAddr always renders ":<port>" suffix, but be defensive.
	for i := len(addr) - 1; i >= 0; i-- {
		if addr[i] == ':' {
			port, err := strconv.Atoi(addr[i+1:])
			if err != nil || port <= 0 || port > 65535 {
				return addr[:i], defaultPort
			}
			return addr[:i], int32(port)
		}
	}
	return addr, defaultPort
}

// dragonflyStatusEqual is a shallow equality check used to decide
// whether to issue a status patch. Intentionally not deep — we want
// patches on any substantive change in the per-site arrays as well as
// phase/message transitions.
func dragonflyStatusEqual(a, b *v1alpha1.DragonflyStatus) bool {
	if a == nil || b == nil {
		return a == nil && b == nil
	}
	if a.ActiveSite != b.ActiveSite || a.Phase != b.Phase || a.Message != b.Message || a.Image != b.Image {
		return false
	}
	if (a.LastPromotionTime == nil) != (b.LastPromotionTime == nil) {
		return false
	}
	if a.LastPromotionTime != nil && b.LastPromotionTime != nil && !a.LastPromotionTime.Equal(b.LastPromotionTime) {
		return false
	}
	if a.LastPromotionTarget != b.LastPromotionTarget {
		return false
	}
	if !boolPtrEqual(a.ReplTakeoverSupported, b.ReplTakeoverSupported) {
		return false
	}
	if a.ReplTakeoverProbeMessage != b.ReplTakeoverProbeMessage {
		return false
	}
	if (a.ReplTakeoverProbeTime == nil) != (b.ReplTakeoverProbeTime == nil) {
		return false
	}
	if a.ReplTakeoverProbeTime != nil && b.ReplTakeoverProbeTime != nil && !a.ReplTakeoverProbeTime.Equal(b.ReplTakeoverProbeTime) {
		return false
	}
	if len(a.Sites) != len(b.Sites) {
		return false
	}
	for i := range a.Sites {
		// The optional observations are pointers: compare their values.
		if !equality.Semantic.DeepEqual(a.Sites[i], b.Sites[i]) {
			return false
		}
	}
	return true
}

// dragonflySyncInProgress is status.dragonfly.sites[].syncInProgress: nil
// when Dragonfly did not report master_sync_in_progress.
func dragonflySyncInProgress(info dragonfly.ReplicationInfo) *bool {
	if !info.MasterSyncInProgressReported {
		return nil
	}
	v := info.MasterSyncInProgress
	return &v
}

// dragonflyLastIOSecondsAgo is status.dragonfly.sites[].lastIOSecondsAgo:
// nil when Dragonfly did not report master_last_io_seconds_ago.
func dragonflyLastIOSecondsAgo(info dragonfly.ReplicationInfo) *int64 {
	if !info.MasterLastIOReported {
		return nil
	}
	v := int64(info.MasterLastIOSecondsAgo)
	return &v
}
