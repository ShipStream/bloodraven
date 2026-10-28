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
// TopologyManager and is paused while a planned-failover state machine
// is driving its own Dragonfly promotion.
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

	// paused is set to true while a planned-failover state machine is
	// running so we don't fight its promotion sequence.
	paused atomic.Bool

	// stale-master events are deduplicated per (site, role) so we don't
	// spam the Event stream while the state persists.
	mu                  sync.Mutex
	lastStaleMasterAt   map[string]time.Time
	staleMasterEventTTL time.Duration

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

		emergencyTakeoverTimeout:    defaultEmergencyTakeoverTimeout,
		emergencyWaits:              defaultEmergencyWaits,
		emergencySettle:             defaultEmergencySettle,
		emergencyStepTimeout:        defaultEmergencyStepTimeout,
		emergencySourceProbeTimeout: defaultEmergencySourceProbeTimeout,
	}
}

// SetPaused toggles the planned-failover guard. While paused, the manager
// continues to observe and update status, but does NOT issue REPLICAOF
// commands.
func (m *DragonflyManager) SetPaused(paused bool) {
	m.paused.Store(paused)
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

	snap := m.observe(ctx, &fg)
	if m.shouldYield() {
		return
	}
	m.patchStatus(ctx, snap)

	if m.paused.Load() || m.shouldYield() {
		return
	}
	// Commands run on a context the stop signal does not cancel: once a
	// role change is sent it is recorded (it has its own budgets).
	workCtx := context.WithoutCancel(ctx)
	if target, oldSource, ok := m.mysqlActivePromotionCandidate(&fg, snap); ok {
		m.logger.Info("dragonfly/mysql active-site drift: promoting Dragonfly replica to match MySQL",
			"oldSource", oldSource, "target", target, "mysqlActiveSite", fg.Status.ActiveSite, "fg", m.fgKey.String())
		if m.recorder != nil {
			m.recorder.Eventf(&fg, corev1.EventTypeNormal, ReasonDragonflyPromotionStarted,
				"mysql active site %q differs from Dragonfly master %q; promoting Dragonfly replica %q",
				fg.Status.ActiveSite, oldSource, target)
		}
		m.promoteLocked(workCtx, &fg, target, oldSource, m.emergencyTakeoverTimeout)
		return
	}
	if target, oldSource, ok := m.dragonflyOnlyPromotionCandidate(&fg, snap); ok {
		m.logger.Info("dragonfly-only emergency: active master unreachable; promoting replica", "oldSource", oldSource, "target", target, "fg", m.fgKey.String())
		if m.recorder != nil {
			m.recorder.Eventf(&fg, corev1.EventTypeNormal, ReasonDragonflyPromotionStarted,
				"dragonfly-only emergency: active master %q unreachable; promoting replica %q",
				oldSource, target)
		}
		m.promoteLocked(workCtx, &fg, target, oldSource, m.emergencyTakeoverTimeout)
		return
	}
	m.reconcileReplication(workCtx, &fg, snap)
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
	if pf := fg.Status.PlannedFailover; pf != nil {
		switch pf.Phase {
		case v1alpha1.PlannedFailoverPhasePromotingDragonfly,
			v1alpha1.PlannedFailoverPhasePromoting,
			v1alpha1.PlannedFailoverPhaseResuming:
			return "", "", false
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
	if auth == nil || auth.SecretName == "" {
		return ""
	}
	key := auth.PasswordKey
	if key == "" {
		key = "password"
	}
	var secret corev1.Secret
	if err := m.client.Get(ctx, types.NamespacedName{Namespace: fg.Namespace, Name: auth.SecretName}, &secret); err != nil {
		m.logger.Warn("dragonfly: read auth secret", "secret", auth.SecretName, "error", err)
		return ""
	}
	raw, ok := secret.Data[key]
	if !ok {
		m.logger.Warn("dragonfly: auth secret missing key", "secret", auth.SecretName, "key", key)
		return ""
	}
	return string(raw)
}

// patchStatus writes the observed snapshot into status.dragonfly. We
// only patch when there is a meaningful change, to keep API-server load
// proportional to actual state transitions.
func (m *DragonflyManager) patchStatus(ctx context.Context, snaps []DragonflySiteSnapshot) {
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

		desired := buildDragonflyStatus(&fg, snaps)
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
// status block.
func buildDragonflyStatus(fg *v1alpha1.MysqlFailoverGroup, snaps []DragonflySiteSnapshot) v1alpha1.DragonflyStatus {
	expectedActive := activeDragonflySiteFromObservation(fg, snaps)
	st := v1alpha1.DragonflyStatus{
		Enabled:    true,
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
			Role:             v1alpha1.DragonflyRole(role),
			Reachable:        s.Reachable,
			ServiceName:      dragonflySiteServiceName(fg.Name, s.Name),
			ReplicationState: s.Info.Role,
			LinkStatus:       linkStatus,
			SyncInProgress:   s.Info.MasterSyncInProgress,
			LastIOSecondsAgo: s.Info.MasterLastIOSecondsAgo,
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
			m.recorder.Eventf(fg, corev1.EventTypeWarning, ReasonDragonflyReplTakeoverUnsupported,
				"Dragonfly does not advertise REPLTAKEOVER; emergency promotion will fall back to REPLICAOF NO ONE (session continuity not guaranteed)")
		}
		return
	}
	if prev != nil && !*prev && next != nil && *next {
		m.logger.Info("dragonfly: REPLTAKEOVER capability restored", "fg", m.fgKey.String())
		if m.recorder != nil {
			m.recorder.Eventf(fg, corev1.EventTypeNormal, ReasonDragonflyReplTakeoverSupported,
				"Dragonfly now advertises REPLTAKEOVER")
		}
	}
}

// activeDragonflySiteFromObservation chooses the site that should be treated
// as Dragonfly master for status, label reconciliation, and replica wiring.
// A persisted status.dragonfly.activeSite is honored only when that site is
// still observed as a raw Dragonfly master; otherwise a single observed raw
// master self-heals stale status after operator upgrades or restarts.
func activeDragonflySiteFromObservation(fg *v1alpha1.MysqlFailoverGroup, snaps []DragonflySiteSnapshot) string {
	if pf := fg.Status.PlannedFailover; pf != nil {
		switch pf.Phase {
		case v1alpha1.PlannedFailoverPhasePromotingDragonfly,
			v1alpha1.PlannedFailoverPhasePromoting,
			v1alpha1.PlannedFailoverPhaseResuming:
			if pf.Target != "" && pf.Dragonfly != nil && pf.Dragonfly.PromotionMethod != "" {
				return pf.Target
			}
		}
	}

	rawMasters := make([]string, 0, 1)
	for _, s := range snaps {
		if s.Reachable && s.Info.Role == "master" {
			rawMasters = append(rawMasters, s.Name)
		}
	}
	if fg.Status.Dragonfly != nil && fg.Status.Dragonfly.ActiveSite != "" {
		for _, site := range rawMasters {
			if site == fg.Status.Dragonfly.ActiveSite {
				return site
			}
		}
	}
	if len(rawMasters) == 1 {
		return rawMasters[0]
	}
	if fg.Status.Dragonfly != nil && fg.Status.Dragonfly.ActiveSite != "" {
		return fg.Status.Dragonfly.ActiveSite
	}
	return fg.Status.ActiveSite
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
func (m *DragonflyManager) reconcileReplication(ctx context.Context, fg *v1alpha1.MysqlFailoverGroup, snaps []DragonflySiteSnapshot) {
	active := activeDragonflySiteFromObservation(fg, snaps)
	if active == "" {
		// Nothing to align against until MySQL has picked an active site.
		return
	}
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
			// — a stale master that fails the connected_slaves/offset
			// gate must still not serve client traffic.
			if err := m.setTrafficLabel(ctx, fg, snap.Name, false); err != nil {
				m.logger.Info("dragonfly: strip stale-master traffic", "site", snap.Name, "error", err)
			}
			// Auto-reconfigure only when the stale master provably
			// never accepted writes since restart: connected_slaves=0
			// AND master_repl_offset=0. Anything else risks rebinding
			// a master that has divergent data, so we leave it for
			// human intervention.
			if snap.Info.ConnectedSlaves == 0 && snap.Info.MasterReplOffset == 0 {
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
			// REPLICAOF to the same target is idempotent at the engine
			// level, so re-issuing on every tick while the link is down
			// is safe.
			pointedRight := snap.Info.MasterHost == masterHost && snap.Info.MasterPort == int(masterPort)
			linkUp := snap.Info.MasterLinkStatus == "up"
			if pointedRight && linkUp {
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

// dragonflyOnlyPromotionCandidate returns the single healthy replica to
// promote when the current Dragonfly master is unreachable but MySQL has not
// failed over. The emergency gate is intentionally looser than planned
// failover: if the old master is gone, the replica's link is usually "down",
// so the durable signal is that it had previously received data
// (master_last_io_seconds_ago >= 0) and is not in a full sync/load cycle.
func (m *DragonflyManager) dragonflyOnlyPromotionCandidate(fg *v1alpha1.MysqlFailoverGroup, snaps []DragonflySiteSnapshot) (target, oldSource string, ok bool) {
	active := activeDragonflySiteFromObservation(fg, snaps)
	if active == "" {
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
	return candidates[0], active, true
}

func dragonflyOnlyPromotionReady(s DragonflySiteSnapshot, role dragonfly.SiteRole) bool {
	return s.Reachable &&
		role == dragonfly.RoleReplica &&
		s.Info.MasterLastIOSecondsAgo >= 0 &&
		!s.Info.MasterSyncInProgress &&
		!s.Persist.Loading
}

// attemptStaleMasterReconfigure tries to attach a provably-empty stale
// master back to the active master as a replica. The caller must have
// already gated this on connected_slaves=0 AND master_repl_offset=0
// (snap.Info), which is the upstream-blessed signal that the pod has
// not accepted any writes since restart.
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
		m.recorder.Eventf(fg, corev1.EventTypeNormal, ReasonDragonflyOldSiteReconfigured,
			"stale Dragonfly master on site %q auto-reconfigured as replica of %s:%d (connected_slaves=0, master_repl_offset=0 — provably never accepted writes)",
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
	if m.paused.Load() || plannedFailoverInFlight(fg.Status.PlannedFailover) || dragonflyUpgradeInFlight(dragonflyUpgradeStatus(&fg)) {
		// A planned failover or snapshot upgrade owns Dragonfly.
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
	if why := rolloutPromotionBlocked(&fg, target, oldSource); why != "" {
		m.logger.Info("dragonfly rollout: promotion no longer valid; skipping", "site", target, "reason", why, "fg", m.fgKey.String())
		return false
	}
	return m.rolloutPromoteFG(work, &fg, target, oldSource)
}

// rolloutPromoteFG promotes target for a rollout from fg, a read of the
// group the caller made under the gate and validated with
// rolloutPromotionBlocked.
func (m *DragonflyManager) rolloutPromoteFG(work context.Context, fg *v1alpha1.MysqlFailoverGroup, target, oldSource string) bool {
	return m.promoteLocked(work, fg, target, oldSource, effectiveDragonflyMaxSyncWaitFromFG(fg)/2)
}

// rolloutPromotionBlocked returns why a rollout promotion of target, taking
// over from oldSource, must not run against fg (read under the gate), or ""
// if it may. The rollout chose target and oldSource from an earlier read; by
// the time it holds the gate another owner may have moved Dragonfly.
func rolloutPromotionBlocked(fg *v1alpha1.MysqlFailoverGroup, target, oldSource string) string {
	switch {
	case !dragonflyEnabled(fg):
		return "dragonfly disabled"
	case plannedFailoverInFlight(fg.Status.PlannedFailover):
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
//   - a planned failover started at or after the request;
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
	if pf := fg.Status.PlannedFailover; pf != nil && pf.StartTime != nil && !pf.StartTime.Time.Before(issued) {
		return fmt.Sprintf("planned failover to %q started after the request", pf.Target)
	}
	if t := fg.Status.LastFailoverTarget; t != "" && t != req.target && fg.Status.LastFailover != nil && !fg.Status.LastFailover.Time.Before(issued) {
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
//     its own context, so application traffic can resume against an empty
//     master. Sessions are lost in this branch.
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
			m.recorder.Eventf(fg, corev1.EventTypeWarning, ReasonDragonflyPromotionFailed,
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
	if m.dragonflyReachable(ctx, fg, oldSource, password) {
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
				m.recorder.Eventf(fg, corev1.EventTypeNormal, ReasonDragonflyPromotionCompleted,
					"emergency: target Dragonfly %q promoted via REPLTAKEOVER (best-effort)", target)
			}
			m.finishDragonflyPromotion(ctx, fg, target, oldSource, password)
			return true
		}
		m.logger.Warn("dragonfly emergency: REPLTAKEOVER failed; falling back", "site", target, "error", takeoverErr, "fg", fgKey)
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
	m.recordSessionsLostPromotion(fg, target, "promoted via REPLICAOF NO ONE (sessions lost)")
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
		m.recorder.Eventf(fg, corev1.EventTypeWarning, ReasonDragonflyPromotionFailed,
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
		m.recorder.Eventf(fg, corev1.EventTypeWarning, ReasonDragonflyPromotionCompleted,
			"emergency: target Dragonfly %q %s", target, what)
		m.recorder.Eventf(fg, corev1.EventTypeWarning, ReasonDragonflySessionsLost,
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

// applyEmergencyPromotionLabels stamps role=master+traffic=enabled on
// the target site and demotes the old source to role=replica with its
// traffic label restored.
//
// Demote MUST succeed before we restore the source's traffic label.
// Otherwise the source pod stays labelled dragonfly-role=master with
// traffic=enabled and gets selected by the active Service alongside
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
		m.logger.Info("emergency: stamp target traffic=enabled", "site", target, "error", err)
	}
	if oldSource == "" {
		return true
	}
	if err := m.setRoleLabel(ctx, fg, oldSource, "replica"); err != nil {
		m.logger.Warn("emergency: stamp old source role=replica failed; leaving traffic stripped to avoid split-brain", "site", oldSource, "error", err)
		if m.recorder != nil {
			m.recorder.Eventf(fg, corev1.EventTypeWarning, ReasonDragonflyPromotionFailed,
				"emergency: demote of old source %q failed (%v); source kept out of active Service to avoid split-brain. Manual intervention required.",
				oldSource, err)
		}
		return false
	}
	if err := m.setTrafficLabel(ctx, fg, oldSource, true); err != nil {
		// Demote succeeded so the active Service ignores the source
		// (role=replica). Traffic-label drift is cosmetic; log only.
		m.logger.Info("emergency: restore old source traffic=enabled", "site", oldSource, "error", err)
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

// effectiveDragonflyMaxSyncWaitFromFG mirrors the planned-failover
// helper but works against a freshly-fetched FG. Returns
// defaultDragonflySyncWait when no override is set.
func effectiveDragonflyMaxSyncWaitFromFG(fg *v1alpha1.MysqlFailoverGroup) time.Duration {
	if d := fg.Spec.Dragonfly; d != nil && d.PlannedFailover != nil && d.PlannedFailover.MaxSyncWait != nil && d.PlannedFailover.MaxSyncWait.Duration > 0 {
		return d.PlannedFailover.MaxSyncWait.Duration
	}
	return defaultDragonflySyncWait
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
			fg.Status.Dragonfly = &v1alpha1.DragonflyStatus{Enabled: true}
		}
		if success {
			fg.Status.Dragonfly.ActiveSite = target
		}
		fg.Status.Dragonfly.LastPromotionTime = &when
		fg.Status.Dragonfly.LastPromotionTarget = target
		return c.Status().Patch(ctx, &fg, client.MergeFrom(base))
	})
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
		// Auto-reconfigure is attempted in reconcileReplication when
		// connected_slaves=0 AND master_repl_offset=0; this Event fires
		// on every detection regardless of whether the auto-rejoin gate
		// passed.
		m.recorder.Eventf(fg, corev1.EventTypeWarning, ReasonDragonflyStaleMasterDetected,
			"stale Dragonfly master detected on site %q (active=%q); auto-rejoin attempted only when connected_slaves=0 AND master_repl_offset=0",
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
	if a.Enabled != b.Enabled || a.ActiveSite != b.ActiveSite || a.Phase != b.Phase || a.Message != b.Message {
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
		if a.Sites[i] != b.Sites[i] {
			return false
		}
	}
	return true
}
