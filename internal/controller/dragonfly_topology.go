package controller

import (
	"context"
	"fmt"
	"log/slog"
	"runtime/debug"
	"strconv"
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
		client:              c,
		recorder:            recorder,
		logger:              logger,
		fgKey:               fgKey,
		pollInterval:        pollInterval,
		connector:           realDragonflyConnector,
		lastStaleMasterAt:   make(map[string]time.Time),
		staleMasterEventTTL: 5 * time.Minute,
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

// Run launches the polling loop. Returns when ctx is canceled.
//
// Each Tick is wrapped in a panic-recovery guard so a single bad
// observation (NPE in observe, missing nilcheck in reconcileReplication,
// etc.) cannot kill the goroutine for the operator's lifetime — the
// runner's start/stop logic only fires on TopologyConfig changes, so
// without recovery a panic would silently stop all Dragonfly
// observation+replication until the operator pod restarts.
func (m *DragonflyManager) Run(ctx context.Context) {
	if m.pollInterval <= 0 {
		m.pollInterval = 2 * time.Second
	}
	ticker := time.NewTicker(m.pollInterval)
	defer ticker.Stop()
	// Tick immediately so freshly-created Dragonfly resources get their
	// first status observation without waiting a full pollInterval.
	m.safeTick(ctx)
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			m.safeTick(ctx)
		}
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
func (m *DragonflyManager) Tick(ctx context.Context) {
	var fg v1alpha1.MysqlFailoverGroup
	if err := m.client.Get(ctx, m.fgKey, &fg); err != nil {
		m.logger.Debug("dragonfly tick: get fg", "error", err)
		return
	}
	if !dragonflyEnabled(&fg) {
		return
	}

	snap := m.observe(ctx, &fg)
	m.patchStatus(ctx, snap)

	if !m.paused.Load() {
		m.reconcileReplication(ctx, &fg, snap)
	}
}

// DragonflySiteSnapshot is the internal observation for one site at one tick.
type DragonflySiteSnapshot struct {
	Name           string
	Reachable      bool
	Info           dragonfly.ReplicationInfo
	Persist        dragonfly.PersistenceInfo
	ClassifiedRole dragonfly.SiteRole
}

// observe collects per-site Dragonfly state. Connections are opened and
// closed within this call; the manager does not pool connections because
// pollInterval is short and Dragonfly handles reconnection cheaply.
func (m *DragonflyManager) observe(ctx context.Context, fg *v1alpha1.MysqlFailoverGroup) []DragonflySiteSnapshot {
	password := m.fetchPassword(ctx, fg)
	expectedActive := fg.Status.ActiveSite
	out := make([]DragonflySiteSnapshot, 0, len(fg.Spec.Sites))
	for _, site := range fg.Spec.Sites {
		snap := DragonflySiteSnapshot{Name: site.Name}
		addr := dragonflyAddr(fg, site.Name)
		dialCtx, cancel := context.WithTimeout(ctx, dragonfly.DefaultDialTimeout+dragonfly.DefaultIOTimeout)
		conn, err := m.connector(dialCtx, addr, password)
		cancel()
		if err != nil {
			snap.ClassifiedRole = dragonfly.RoleUnreachable
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
			snap.ClassifiedRole = dragonfly.RoleUnreachable
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
		_ = conn.Close()
		snap.Reachable = true
		snap.Info = info
		snap.Persist = persist
		snap.ClassifiedRole = dragonfly.ClassifySiteRole(info, true, site.Name, expectedActive)
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
	err := k8sretry.RetryOnConflict(k8sretry.DefaultRetry, func() error {
		var fg v1alpha1.MysqlFailoverGroup
		if err := m.client.Get(ctx, m.fgKey, &fg); err != nil {
			return err
		}
		if !dragonflyEnabled(&fg) {
			return nil
		}

		desired := buildDragonflyStatus(&fg, snaps)
		if dragonflyStatusEqual(fg.Status.Dragonfly, &desired) {
			return nil
		}

		base := fg.DeepCopy()
		fg.Status.Dragonfly = &desired
		patch := client.MergeFrom(base)
		return m.client.Status().Patch(ctx, &fg, patch)
	})
	if err != nil {
		m.logger.Error("dragonfly: status patch failed", "error", err)
	}
}

// buildDragonflyStatus converts per-site snapshots into the API-shaped
// status block.
func buildDragonflyStatus(fg *v1alpha1.MysqlFailoverGroup, snaps []DragonflySiteSnapshot) v1alpha1.DragonflyStatus {
	expectedActive := fg.Status.ActiveSite
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
		switch s.ClassifiedRole {
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
			Role:             v1alpha1.DragonflyRole(s.ClassifiedRole),
			Reachable:        s.Reachable,
			ServiceName:      dragonflySiteServiceName(fg.Name, s.Name),
			ReplicationState: s.Info.Role,
			LinkStatus:       linkStatus,
			SyncInProgress:   s.Info.MasterSyncInProgress,
			LastIOSecondsAgo: s.Info.MasterLastIOSecondsAgo,
			Ready:            ready,
			Message:          dragonflySiteMessage(s),
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
	return st
}

func dragonflySiteMessage(s DragonflySiteSnapshot) string {
	switch s.ClassifiedRole {
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
func (m *DragonflyManager) reconcileReplication(ctx context.Context, fg *v1alpha1.MysqlFailoverGroup, snaps []DragonflySiteSnapshot) {
	active := fg.Status.ActiveSite
	if active == "" {
		// Nothing to align against until MySQL has picked an active site.
		return
	}
	password := m.fetchPassword(ctx, fg)
	masterAddr := dragonflyAddr(fg, active)
	masterHost, masterPort := splitHostPort(masterAddr, dragonflyPort(fg.Spec.Dragonfly))

	for _, snap := range snaps {
		switch snap.ClassifiedRole {
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
	if err := conn.ReplicaOf(ctx, masterHost, masterPort); err != nil {
		m.logger.Warn("dragonfly: REPLICAOF failed", "site", siteName, "host", masterHost, "port", masterPort, "error", err)
		return
	}
	m.logger.Info("dragonfly: configured replica", "site", siteName, "host", masterHost, "port", masterPort)
}

// TryEmergencyPromote runs a best-effort Dragonfly promotion after the
// MySQL emergency failover has already succeeded. The contract is
// strictly best-effort: this function never returns an error to the
// caller, never blocks longer than a small bounded budget, and never
// leaves Dragonfly in a state that affects MySQL durability.
//
// Strategy mirrors the planned-failover sequence with looser
// timeouts and tolerance for an unreachable old source:
//  1. Best-effort strip the old source's traffic label so the active
//     Service sheds it before we promote. Skipped silently if the
//     source pod is gone.
//  2. Try REPLTAKEOVER on the target. If that succeeds, sessions are
//     preserved (best case).
//  3. On failure, try REPLICAOF NO ONE on the target so application
//     traffic can resume against an empty master. Sessions are lost
//     in this branch.
//  4. On any successful target promotion, stamp role+traffic on the
//     target pod and best-effort CLIENT KILL the old source.
//  5. If the target is unreachable, give up; the next reconcile cycle
//     of the DragonflyManager will continue trying.
//
// In all cases, status.dragonfly.lastPromotionTime/lastPromotionTarget
// is stamped so the operator timeline reflects what happened.
func (m *DragonflyManager) TryEmergencyPromote(ctx context.Context, target, oldSource string) {
	const budget = 10 * time.Second
	emCtx, cancel := context.WithTimeout(ctx, budget)
	defer cancel()

	var fg v1alpha1.MysqlFailoverGroup
	if err := m.client.Get(emCtx, m.fgKey, &fg); err != nil {
		m.logger.Error("dragonfly emergency: get fg", "error", err)
		return
	}
	if !dragonflyEnabled(&fg) {
		return
	}
	password := m.fetchPassword(emCtx, &fg)
	addr := dragonflyAddr(&fg, target)

	// Step 1: strip old source traffic. Always safe even if the source
	// is dead — the K8s API call still succeeds (label patch on a pod
	// that may already be gone is a no-op).
	if oldSource != "" {
		if err := m.setTrafficLabel(emCtx, &fg, oldSource, false); err != nil {
			m.logger.Info("dragonfly emergency: strip old source traffic failed (proceeding)", "site", oldSource, "error", err)
		}
	}

	conn, err := m.connector(emCtx, addr, password)
	if err != nil {
		m.logger.Warn("dragonfly emergency: target unreachable; skipping promotion", "site", target, "error", err)
		metrics.DragonflyPromotionsTotal.WithLabelValues(fg.Name, target, "failed").Inc()
		if m.recorder != nil {
			m.recorder.Eventf(&fg, corev1.EventTypeWarning, ReasonDragonflyPromotionFailed,
				"emergency: target Dragonfly %q unreachable; cache continuity unavailable", target)
		}
		m.stampPromotion(emCtx, target)
		return
	}

	maxWait := effectiveDragonflyMaxSyncWaitFromFG(&fg)
	takeoverErr := conn.ReplTakeover(emCtx, maxWait/2)
	// RESP has no req/reply correlation. After a REPLTAKEOVER error the
	// underlying connection may have a late server reply pending in the
	// read buffer; reusing it for REPLICAOF NO ONE would either consume
	// that stale reply or interleave with one. Close and dial fresh.
	_ = conn.Close()
	if takeoverErr == nil {
		m.logger.Info("dragonfly emergency: REPLTAKEOVER succeeded", "site", target)
		metrics.DragonflyPromotionsTotal.WithLabelValues(fg.Name, target, "success").Inc()
		if m.recorder != nil {
			m.recorder.Eventf(&fg, corev1.EventTypeNormal, ReasonDragonflyPromotionCompleted,
				"emergency: target Dragonfly %q promoted via REPLTAKEOVER (best-effort)", target)
		}
		m.applyEmergencyPromotionLabels(emCtx, &fg, target, oldSource)
		m.bestEffortEmergencyClientKill(emCtx, &fg, oldSource, password)
		m.stampPromotion(emCtx, target)
		return
	}
	m.logger.Warn("dragonfly emergency: REPLTAKEOVER failed; falling back", "site", target, "error", takeoverErr)

	freshConn, freshErr := m.connector(emCtx, addr, password)
	if freshErr != nil {
		m.logger.Warn("dragonfly emergency: re-dial for REPLICAOF NO ONE failed", "site", target, "error", freshErr)
		metrics.DragonflyPromotionsTotal.WithLabelValues(fg.Name, target, "failed").Inc()
		if m.recorder != nil {
			m.recorder.Eventf(&fg, corev1.EventTypeWarning, ReasonDragonflyPromotionFailed,
				"emergency: target Dragonfly %q failed to promote (REPLTAKEOVER failed; re-dial for fallback also failed)", target)
		}
		m.stampPromotion(emCtx, target)
		return
	}
	noOneErr := freshConn.ReplicaOfNoOne(emCtx)
	_ = freshConn.Close()
	if noOneErr != nil {
		m.logger.Warn("dragonfly emergency: REPLICAOF NO ONE failed", "site", target, "error", noOneErr)
		metrics.DragonflyPromotionsTotal.WithLabelValues(fg.Name, target, "failed").Inc()
		if m.recorder != nil {
			m.recorder.Eventf(&fg, corev1.EventTypeWarning, ReasonDragonflyPromotionFailed,
				"emergency: target Dragonfly %q failed to promote (REPLTAKEOVER and REPLICAOF NO ONE both failed)", target)
		}
		m.stampPromotion(emCtx, target)
		return
	}
	m.logger.Info("dragonfly emergency: target promoted via REPLICAOF NO ONE (sessions lost)", "site", target)
	metrics.DragonflyPromotionsTotal.WithLabelValues(fg.Name, target, "success").Inc()
	if m.recorder != nil {
		m.recorder.Eventf(&fg, corev1.EventTypeWarning, ReasonDragonflyPromotionCompleted,
			"emergency: target Dragonfly %q promoted via REPLICAOF NO ONE (sessions lost)", target)
	}
	m.applyEmergencyPromotionLabels(emCtx, &fg, target, oldSource)
	m.bestEffortEmergencyClientKill(emCtx, &fg, oldSource, password)
	m.stampPromotion(emCtx, target)
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
func (m *DragonflyManager) applyEmergencyPromotionLabels(ctx context.Context, fg *v1alpha1.MysqlFailoverGroup, target, oldSource string) {
	if err := m.setRoleLabel(ctx, fg, target, "master"); err != nil {
		m.logger.Info("emergency: stamp target role=master", "site", target, "error", err)
	}
	if err := m.setTrafficLabel(ctx, fg, target, true); err != nil {
		m.logger.Info("emergency: stamp target traffic=enabled", "site", target, "error", err)
	}
	if oldSource == "" {
		return
	}
	if err := m.setRoleLabel(ctx, fg, oldSource, "replica"); err != nil {
		m.logger.Warn("emergency: stamp old source role=replica failed; leaving traffic stripped to avoid split-brain", "site", oldSource, "error", err)
		if m.recorder != nil {
			m.recorder.Eventf(fg, corev1.EventTypeWarning, ReasonDragonflyPromotionFailed,
				"emergency: demote of old source %q failed (%v); source kept out of active Service to avoid split-brain. Manual intervention required.",
				oldSource, err)
		}
		return
	}
	if err := m.setTrafficLabel(ctx, fg, oldSource, true); err != nil {
		// Demote succeeded so the active Service ignores the source
		// (role=replica). Traffic-label drift is cosmetic; log only.
		m.logger.Info("emergency: restore old source traffic=enabled", "site", oldSource, "error", err)
	}
}

// bestEffortEmergencyClientKill issues CLIENT KILL TYPE NORMAL against
// the old source, if reachable, to evict any clients still attached.
// Bounded context, all errors logged and dropped.
//
// Log lines include `fg` per the docs/docs/log-schema.mdx Event
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

// stampPromotion writes status.dragonfly.lastPromotionTime/Target as a
// best-effort patch. Wrapped in RetryOnConflict so a concurrent
// patchStatus write from the manager's own poll loop or the
// reconciler's syncDragonflyPodLabels sweep doesn't silently drop the
// promotion timestamp.
func (m *DragonflyManager) stampPromotion(ctx context.Context, target string) {
	now := metav1.Now()
	err := k8sretry.RetryOnConflict(k8sretry.DefaultRetry, func() error {
		var fg v1alpha1.MysqlFailoverGroup
		if err := m.client.Get(ctx, m.fgKey, &fg); err != nil {
			return err
		}
		base := fg.DeepCopy()
		if fg.Status.Dragonfly == nil {
			fg.Status.Dragonfly = &v1alpha1.DragonflyStatus{Enabled: true}
		}
		fg.Status.Dragonfly.LastPromotionTime = &now
		fg.Status.Dragonfly.LastPromotionTarget = target
		return m.client.Status().Patch(ctx, &fg, client.MergeFrom(base))
	})
	if err != nil {
		m.logger.Warn("stampPromotion: status patch", "error", err)
	}
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
