package controller

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"log/slog"
	"slices"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/go-sql-driver/mysql"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/equality"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	apimeta "k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/tools/record"
	k8sretry "k8s.io/client-go/util/retry"
	"sigs.k8s.io/controller-runtime/pkg/client"

	v1alpha1 "github.com/shipstream/bloodraven/api/v1alpha1"
	"github.com/shipstream/bloodraven/internal/metrics"
	internalmysql "github.com/shipstream/bloodraven/internal/mysql"
	"github.com/shipstream/bloodraven/internal/platform"
	"github.com/shipstream/bloodraven/internal/state"
)

// managedTopology tracks a running TopologyManager and its cancellation.
type managedTopology struct {
	tm     *TopologyManager
	cancel context.CancelFunc
	// done is closed when the manager goroutine returns. nil in tests
	// that inject a manager without starting Run.
	done chan struct{}
	cfg  TopologyConfig
	// uid is the UID of the CR the manager was started for ("" in tests
	// that inject a manager). A CR deleted and recreated under the same
	// name is another incarnation: it inherits neither the group's claim
	// slot nor a retired manager's failover record (K002).
	uid types.UID
	// viewSlot and viewSlotGen stamp the group's claim slot just before
	// startManager's uncached read: registration hydrates nothing from
	// that read when the slot has released a claim since (K004).
	viewSlot    *PlannedClaimSlot
	viewSlotGen uint64

	// archiver polls each sidecar's /archiver/status endpoint and caches
	// the latest snapshot for updateCRStatus to copy into status.pitr.
	// nil only if the runner hasn't finished wiring yet.
	archiver *archiverPoller
	// archiverDone is closed when the poller's Run returns. nil in tests
	// that inject a poller without starting Run.
	archiverDone chan struct{}

	// dragonfly observes per-site Dragonfly state and reconciles
	// replication wiring. nil when spec.dragonfly is disabled at start
	// time. Currently the runner does not respawn this on enable=true
	// flips during a manager's lifetime; a CR edit that toggles
	// dragonfly.enabled changes the TopologyConfig hash and restarts
	// the manager via sync().
	dragonfly *DragonflyManager

	// lastTopologyDegradedReason tracks the most recent topology-level
	// Degraded reason so transition events are not confused by replication
	// reasons that overwrite the shared Degraded condition.
	lastTopologyDegradedReason string

	// lastKeyringPromotionSkipped / lastKeyringPromotionRefused are the
	// last Event-emitted sets. In-memory only: one re-fire after process
	// restart is expected and wanted.
	lastKeyringPromotionSkipped []string
	lastKeyringPromotionRefused []string
	// lastRestoreReclonePromotionRefused is the same for promotions
	// refused because the candidates await their post-restore reclone.
	lastRestoreReclonePromotionRefused []string
	// lastBootstrapRefusedRestoreHold is the last site a
	// BootstrapRefusedRestoreHold Event was emitted for.
	lastBootstrapRefusedRestoreHold string

	// materialUnreadable is set while sync keeps this manager running
	// because its connection secrets cannot be read, so the warning is
	// logged once per episode. Read and written only by sync.
	materialUnreadable bool
}

// DeploymentReconciler is the subset of the reconciler that the runner needs
// for ordered updates: reconciling a single site's Deployment, and reporting
// whether that site's pod is Ready. They come as a pair so an ordered update
// can never roll sites without its pod-readiness gate.
type DeploymentReconciler interface {
	ReconcileSiteDeployment(ctx context.Context, fgName types.NamespacedName, siteName string) error
	SitePodReadiness(ctx context.Context, fgName types.NamespacedName, siteName string) (PodReadiness, error)
}

// TopologyManagerRunner manages TopologyManager instances for all MysqlFailoverGroup resources.
// It implements manager.Runnable and runs only on the leader-elected instance.
type TopologyManagerRunner struct {
	deploymentLeases *DeploymentLeaseManager
	client           client.Client
	clientset        kubernetes.Interface
	hub              *platform.Hub
	recorder         record.EventRecorder
	logger           *slog.Logger

	// deployReconciler is set after the reconciler is created (circular dependency).
	// Used by the ordered update callback to reconcile a single site's Deployment.
	deployReconciler DeploymentReconciler

	mu            sync.RWMutex
	leaderContext context.Context
	managers      map[types.NamespacedName]*managedTopology
	// retiring holds managers unregistered from managers whose ordered-
	// update goroutines may still be running. Rotation admission consults
	// them until those goroutines exit (see retireManagerLocked).
	retiring map[types.NamespacedName][]*managedTopology
	// dragonflyGates holds each group's Dragonfly ownership gate. Entries
	// are never removed: a gate must outlive every manager that used it,
	// including one whose stop timed out, so a recreated group cannot get a
	// second gate while old work still holds the first.
	dragonflyGates map[types.NamespacedName]*dragonflyGate
	// plannedClaims holds each group's planned-failover claim slot,
	// shared by every manager generation (see planned_failover_claim.go).
	// A slot is removed only when its CR is deleted and no manager,
	// current or retiring, remains.
	plannedClaims map[types.NamespacedName]*PlannedClaimSlot
	// retiredFailover is the newest failover record a retired manager of
	// each group held when it last stopped (DST-1), with the UID of the CR
	// incarnation it belongs to. A replacement of the same incarnation
	// registered after the retired manager left retiring still receives
	// it; the timestamp-monotonic carry makes a stale entry harmless.
	// Guarded by mu; removed with the group's claim slot.
	retiredFailover map[types.NamespacedName]retiredFailoverRecord
	// incarnations is the CR UID each group's claim slot and retired record
	// belong to. Guarded by mu; removed with the group's claim slot.
	incarnations map[types.NamespacedName]types.UID
	// planned is the process's planned-failover topology facade, built on
	// first use (see PlannedTopology). Guarded by mu.
	planned *runnerPlannedTopology
	// apiReader is the uncached reader the Dragonfly managers use for the
	// group's authoritative state (see DragonflyManager.SetAuthorityReader).
	apiReader client.Reader

	// reservationMu guards rotationReservations, each group's admitted but
	// not yet finished keyring rotations, and orders them against manager
	// registration. Taken before mu. See ReserveKeyringRotation.
	reservationMu        sync.Mutex
	rotationReservations map[types.NamespacedName][]string
	// startFailures tracks groups whose topology manager failed to start,
	// and lastStartWarning when each was last warned about; both drive the
	// rate-limited TopologyManagerStartFailed Event. Guarded by mu.
	startFailures    map[types.NamespacedName]*startFailureState
	lastStartWarning map[types.NamespacedName]startWarning
	// now is the clock for start-failure Events; nil means time.Now.
	now func() time.Time
}

// NewTopologyManagerRunner creates a new runner.
func NewTopologyManagerRunner(c client.Client, clientset kubernetes.Interface, hub *platform.Hub, recorder record.EventRecorder, logger *slog.Logger) *TopologyManagerRunner {
	return &TopologyManagerRunner{
		client:    c,
		clientset: clientset,
		hub:       hub,
		recorder:  recorder,
		logger:    logger,
		managers:  make(map[types.NamespacedName]*managedTopology),
	}
}

// SetDeploymentReconciler wires the reconciler back-reference. Call after both
// the runner and reconciler are constructed.
func (r *TopologyManagerRunner) SetDeploymentReconciler(dr DeploymentReconciler) {
	r.deployReconciler = dr
}

// PlannedClaimHeld reports whether the group's planned-failover claim slot
// is held, in either mode (a provisional grant included).
func (r *TopologyManagerRunner) PlannedClaimHeld(nn types.NamespacedName) bool {
	return r.PlannedTopology().PlannedClaimHeld(nn)
}

// SetDragonflyUpgradePaused sets the Dragonfly manager's upgrade pause
// source; false when no Dragonfly manager runs for nn.
func (r *TopologyManagerRunner) SetDragonflyUpgradePaused(nn types.NamespacedName, paused bool) bool {
	df := r.dragonflyManager(nn)
	if df == nil {
		return false
	}
	df.SetUpgradePaused(paused)
	return true
}

// syncDragonflyUpgradePause reconciles df's upgrade pause with the cached
// upgrade state, but never writes the cached value itself (K121): the
// snapshot-upgrade reconcile sets the pause from the state it reads under
// the Dragonfly gate, and a lagging cache would undo that (pausing again
// after an abandonment, so a queued follow-up is dropped). Only when the
// cache disagrees with the pause is the group re-read uncached and the
// pause set from that read, which still repairs a pause left behind by a
// status edited by hand.
func (r *TopologyManagerRunner) syncDragonflyUpgradePause(ctx context.Context, nn types.NamespacedName, df *DragonflyManager, cachedPaused bool) {
	if df == nil || df.upgradePaused.Load() == cachedPaused {
		return
	}
	paused := cachedPaused
	if r.apiReader != nil {
		var fresh v1alpha1.MysqlFailoverGroup
		if err := r.apiReader.Get(ctx, nn, &fresh); err != nil {
			r.logger.Debug("could not read the group uncached to check the Dragonfly upgrade pause; keeping it", "fg", nn, "error", err)
			return
		}
		paused = dragonflyUpgradeInFlight(dragonflyUpgradeStatus(&fresh))
	}
	df.SetUpgradePaused(paused)
}

// ActiveSite is the registered manager's effective active site, which
// reflects a promotion at once; ok=false when no manager is registered.
func (r *TopologyManagerRunner) ActiveSite(nn types.NamespacedName) (site string, ok bool) {
	return r.PlannedTopology().ActiveSite(nn)
}

// EnqueueDragonflyFollowUp hands an emergency follow-up to the group's
// Dragonfly manager, which revalidates it under the gate against the
// uncached state; false when no Dragonfly manager runs.
func (r *TopologyManagerRunner) EnqueueDragonflyFollowUp(nn types.NamespacedName, activeSite, previousSite string) bool {
	df := r.dragonflyManager(nn)
	if df == nil {
		return false
	}
	df.Enqueue(activeSite, previousSite)
	return true
}

// ClaimPlannedFailover asks the group's topology manager for the
// planned-failover claim (§3.2).
func (r *TopologyManagerRunner) ClaimPlannedFailover(ctx context.Context, nn types.NamespacedName,
	req PlannedClaimRequest) (PlannedClaimDecision, error) {
	return r.PlannedTopology().ClaimPlannedFailover(ctx, nn, req)
}

// ReleasePlannedFailoverClaim releases the claim only when it is held by
// runID, and records why.
func (r *TopologyManagerRunner) ReleasePlannedFailoverClaim(nn types.NamespacedName, runID string,
	why ClaimReleaseReason) bool {
	return r.PlannedTopology().ReleasePlannedFailoverClaim(nn, runID, why)
}

// PlannedTopology returns the process's planned-failover topology facade.
func (r *TopologyManagerRunner) PlannedTopology() PlannedFailoverTopology {
	return r.plannedFacade()
}

func (r *TopologyManagerRunner) plannedFacade() *runnerPlannedTopology {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.planned == nil {
		r.planned = newRunnerPlannedTopology(r)
	}
	return r.planned
}

var _ ManagerRegistry = (*TopologyManagerRunner)(nil)

// Snapshot implements ManagerRegistry: the group's claim slot (created
// under r.mu.Lock on first use, see Slot), then the current and retiring
// managers under one r.mu.RLock. It returns pointers; the caller inspects
// them with no registry lock held.
func (r *TopologyManagerRunner) Snapshot(nn types.NamespacedName) RegistrySnapshot {
	slot := r.Slot(nn)
	r.mu.RLock()
	defer r.mu.RUnlock()
	snap := RegistrySnapshot{Slot: slot}
	if mt := r.managers[nn]; mt != nil {
		snap.Current = mt.tm
	}
	for _, mt := range r.retiring[nn] {
		if mt.tm != nil {
			snap.Retiring = append(snap.Retiring, mt.tm)
		}
	}
	return snap
}

// Slot implements ManagerRegistry: nn's claim slot, created on first use.
func (r *TopologyManagerRunner) Slot(nn types.NamespacedName) *PlannedClaimSlot {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.slotLocked(nn)
}

func (r *TopologyManagerRunner) slotLocked(nn types.NamespacedName) *PlannedClaimSlot {
	if r.plannedClaims == nil {
		r.plannedClaims = make(map[types.NamespacedName]*PlannedClaimSlot)
	}
	slot, ok := r.plannedClaims[nn]
	if !ok {
		slot = &PlannedClaimSlot{}
		r.plannedClaims[nn] = slot
	}
	return slot
}

// retiredFailoverRecord is a stashed retired-manager failover record and
// the CR incarnation it belongs to.
type retiredFailoverRecord struct {
	uid types.UID
	rec FailoverRecord
}

// sameIncarnation reports whether two CR UIDs may be the same incarnation
// (an unknown UID, from a test-injected manager, matches anything).
func sameIncarnation(a, b types.UID) bool {
	return a == "" || b == "" || a == b
}

// LeaderContext implements ManagerRegistry: the context Start received,
// cancelled when leadership is lost. Before Start it is Background.
func (r *TopologyManagerRunner) LeaderContext() context.Context {
	r.mu.RLock()
	defer r.mu.RUnlock()
	if r.leaderContext == nil {
		return context.Background()
	}
	return r.leaderContext
}

// SetDNSRecordSpec pushes spec.dns.hostname and spec.dns.ttl onto the
// running topology manager's DNS updater. Returns true when a manager
// is running for nn. A false return is safe — the next startManager
// constructs the updater from the live spec.
func (r *TopologyManagerRunner) SetDNSRecordSpec(nn types.NamespacedName, hostname string, ttl int64, generation int64) bool {
	r.mu.RLock()
	mt, ok := r.managers[nn]
	r.mu.RUnlock()
	if !ok {
		return false
	}
	mt.tm.SetDNSRecordSpec(hostname, ttl, generation)
	return true
}

// SetCloneTimeout pushes spec.cloneTimeout onto the running manager.
// Returns true when a manager is running for nn.
func (r *TopologyManagerRunner) SetCloneTimeout(nn types.NamespacedName, d time.Duration) bool {
	r.mu.RLock()
	mt, ok := r.managers[nn]
	r.mu.RUnlock()
	if !ok {
		return false
	}
	mt.tm.SetCloneTimeout(d)
	return true
}

// SetAPIReader sets the uncached reader the runner's Dragonfly managers read
// the group through once they hold its gate. Call before Start.
func (r *TopologyManagerRunner) SetAPIReader(reader client.Reader) {
	r.apiReader = reader
}

// DragonflyGate returns the group's stable Dragonfly ownership gate,
// creating it on first use. Every Dragonfly routing writer for the group
// (its managers' Run goroutines, planned failover, rollout, the label
// sweep) acquires this gate; see dragonflyGate.
func (r *TopologyManagerRunner) DragonflyGate(nn types.NamespacedName) *dragonflyGate {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.dragonflyGates == nil {
		r.dragonflyGates = make(map[types.NamespacedName]*dragonflyGate)
	}
	g, ok := r.dragonflyGates[nn]
	if !ok {
		g = newDragonflyGate()
		r.dragonflyGates[nn] = g
	}
	return g
}

func (r *TopologyManagerRunner) dragonflyManager(nn types.NamespacedName) *DragonflyManager {
	r.mu.RLock()
	defer r.mu.RUnlock()
	if mt, ok := r.managers[nn]; ok {
		return mt.dragonfly
	}
	return nil
}

// EnsurePartialRevokes turns partial_revokes ON on every site of the group;
// see TopologyManager.EnsurePartialRevokes.
func (r *TopologyManagerRunner) EnsurePartialRevokes(ctx context.Context, nn types.NamespacedName) error {
	r.mu.RLock()
	mt, ok := r.managers[nn]
	r.mu.RUnlock()
	if !ok {
		return fmt.Errorf("no topology manager running for %s", nn)
	}
	return mt.tm.EnsurePartialRevokes(ctx)
}

// NeedLeaderElection implements manager.LeaderElectionRunnable.
// Topology polling and failover must only run on the leader.
func (r *TopologyManagerRunner) NeedLeaderElection() bool {
	return true
}

// Start implements manager.Runnable. It discovers MysqlFailoverGroup resources,
// starts a TopologyManager per group, and re-syncs periodically.
func (r *TopologyManagerRunner) Start(ctx context.Context) error {
	r.mu.Lock()
	r.leaderContext = ctx
	r.mu.Unlock()
	r.logger.Info("topology manager runner starting")
	if r.deploymentLeases != nil {
		go r.deploymentLeases.run(ctx, r.logger)
	}

	// Initial sync.
	if err := r.sync(ctx); err != nil {
		r.logger.Error("initial sync failed", "error", err)
	}

	// Re-sync every 30 seconds to pick up new/changed/deleted CRs.
	ticker := time.NewTicker(30 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			r.stopAll()
			return nil
		case <-ticker.C:
			if err := r.sync(ctx); err != nil {
				r.logger.Error("sync failed", "error", err)
			}
		}
	}
}

// cloneTimeoutFromSpec returns the clone budget from spec.cloneTimeout.
// Zero means the bootstrap path applies its own default.
func cloneTimeoutFromSpec(fg *v1alpha1.MysqlFailoverGroup) time.Duration {
	if fg == nil || fg.Spec.CloneTimeout <= 0 {
		return 0
	}
	return time.Duration(fg.Spec.CloneTimeout) * time.Second
}

// connectionMaterialHash fingerprints the secrets the operator's MySQL
// checkers are built from. TLS-off groups hash only the operator secret
// (same bytes as before this helper existed) so an upgrade does not
// bounce every manager. TLS-on groups also mix the TLS secret name and
// bytes so adoption, secretName edits, and cert rotation restart the
// manager with new connections.
//
// A secret that cannot be read (deleted, not yet recreated, API error)
// returns an error rather than a hash: its content is unknown, not
// changed, and must not be mistaken for a rotation.
func connectionMaterialHash(ctx context.Context, c client.Client, fg *v1alpha1.MysqlFailoverGroup) (string, error) {
	if fg == nil || c == nil {
		return "", nil
	}
	h := sha256.New()
	wrote := false

	if name := fg.Spec.EffectiveOperatorSecretName(); name != "" {
		var secret corev1.Secret
		nn := types.NamespacedName{Namespace: fg.Namespace, Name: name}
		if err := c.Get(ctx, nn, &secret); err != nil {
			return "", fmt.Errorf("get operator secret %s: %w", nn, err)
		}
		writeSecretDataHash(h, secret.Data)
		wrote = true
	}

	if fg.Spec.TLS != nil && fg.Spec.TLS.SecretName != "" {
		fmt.Fprintf(h, "tls.secret=%s\n", fg.Spec.TLS.SecretName)
		var secret corev1.Secret
		nn := types.NamespacedName{Namespace: fg.Namespace, Name: fg.Spec.TLS.SecretName}
		if err := c.Get(ctx, nn, &secret); err != nil {
			return "", fmt.Errorf("get TLS secret %s: %w", nn, err)
		}
		writeSecretDataHash(h, secret.Data)
		wrote = true
	}

	if !wrote {
		return "", nil
	}
	return hex.EncodeToString(h.Sum(nil))[:16], nil
}

// managerRunning reports whether mt's Run goroutine is still running. A nil
// done channel (a test-injected manager) counts as running.
func managerRunning(mt *managedTopology) bool {
	if mt.done == nil {
		return true
	}
	select {
	case <-mt.done:
		return false
	default:
		return true
	}
}

func writeSecretDataHash(h interface{ Write([]byte) (int, error) }, data map[string][]byte) {
	keys := make([]string, 0, len(data))
	for k := range data {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	for _, k := range keys {
		fmt.Fprintf(h, "%s=%x\n", k, sha256.Sum256(data[k]))
	}
}

// sync lists all MysqlFailoverGroup resources and ensures a topology manager
// is running for each one. Stale managers are stopped.
func (r *TopologyManagerRunner) sync(ctx context.Context) error {
	var groups v1alpha1.MysqlFailoverGroupList
	if err := r.client.List(ctx, &groups); err != nil {
		return fmt.Errorf("list MysqlFailoverGroups: %w", err)
	}

	seen := make(map[types.NamespacedName]struct{})

	for i := range groups.Items {
		fg := &groups.Items[i]
		nn := FailoverGroupNamespacedName(fg)
		seen[nn] = struct{}{}

		cfg := CRConfigToTopologyConfig(fg)

		// Include operator secret + TLS material so credential or
		// cert rotation (or TLS adoption) restarts the manager with
		// new MySQL connections. cloneTimeout is live-updated below
		// and must not be part of this hash.
		credentialHash, materialErr := connectionMaterialHash(ctx, r.client, fg)
		cfg.CredentialHash = credentialHash

		r.mu.RLock()
		existing, ok := r.managers[nn]
		r.mu.RUnlock()
		// A manager whose Run has returned is registered but dead; it
		// never polls again, so it must be replaced, not updated.
		live := ok && managerRunning(existing)

		if live && materialErr != nil {
			// Unreadable material is unknown, not changed: the running
			// manager's pools still hold working credentials, and a
			// restart could not read the secret either. Keep it unless the
			// topology itself changed.
			cfg.CredentialHash = existing.cfg.CredentialHash
			if existing.cfg.Equal(cfg) && !existing.materialUnreadable {
				r.logger.Warn("connection material unreadable; keeping the running topology manager", "fg", nn, "error", materialErr)
			}
			existing.materialUnreadable = true
		} else if live {
			existing.materialUnreadable = false
		}

		suppress := restoreInFlight(fg)
		// The planned-failover claim is never touched here: a sync reads
		// the cache, and only the owning run releases its claim.
		upgradePaused := dragonflyUpgradeInFlight(dragonflyUpgradeStatus(fg))

		recreated := live && !sameIncarnation(existing.uid, fg.UID)
		if live && existing.cfg.Equal(cfg) && !recreated {
			existing.tm.SetAutoBootstrapSuppressed(suppress)
			existing.tm.SetKeyringRotationBlocked(rotationBlockedSites(fg))
			_ = r.applyRestoreProtection(ctx, existing.tm, fg, false)
			existing.tm.SetDNSRecordSpec(fg.Spec.DNS.Hostname, fg.Spec.DNS.TTL, fg.Generation)
			existing.tm.SetCloneTimeout(cloneTimeoutFromSpec(fg))
			r.syncDragonflyUpgradePause(ctx, nn, existing.dragonfly, upgradePaused)
			r.handleRecloneAnnotation(ctx, fg, nn, existing.tm)
			// Detect spec drift for ordered rolling updates.
			r.checkSpecDrift(ctx, fg, existing.tm)
			continue
		}

		if ok {
			switch {
			case recreated:
				r.logger.Info("group recreated, restarting topology manager", "fg", nn,
					"previousUid", string(existing.uid), "uid", string(fg.UID))
			case live:
				r.logger.Info("config changed, restarting topology manager", "fg", nn)
			default:
				r.logger.Warn("topology manager exited; restarting", "fg", nn)
			}
			// Unregister before stopping, as for a deleted CR: if the
			// replacement fails to start, no stopped manager is left for
			// a later matching config to mistake for a live one, and the
			// next sync retries the start.
			r.mu.Lock()
			if r.managers[nn] == existing {
				r.retireManagerLocked(nn, existing)
			}
			r.mu.Unlock()
			// Wait for the old poll loop to exit before dropping its
			// series, otherwise an in-flight Poll can resurrect them.
			stopManagedTopology(existing)
			clearManagedSiteGauges(existing.tm)
		}

		if materialErr != nil {
			r.logger.Error("failed to start topology manager", "fg", nn, "error", materialErr)
			r.noteStartFailure(fg, nn, materialErr, ok)
			continue
		}
		// A stopped manager passes its PITR archive history on.
		var prev *managedTopology
		if ok {
			prev = existing
		}
		if err := r.startManager(ctx, fg, cfg, prev); err != nil {
			r.logger.Error("failed to start topology manager", "fg", nn, "error", err)
			r.noteStartFailure(fg, nn, err, ok)
			continue
		}
		r.noteStartSuccess(fg, nn)
		// Apply the suppression flag on the freshly-started manager so the
		// first poll cycle already respects an in-flight restore.
		r.mu.RLock()
		mt, started := r.managers[nn]
		r.mu.RUnlock()
		if started {
			mt.tm.SetAutoBootstrapSuppressed(suppress)
			mt.tm.SetKeyringRotationBlocked(rotationBlockedSites(fg))
			_ = r.applyRestoreProtection(ctx, mt.tm, fg, false)
			r.syncDragonflyUpgradePause(ctx, nn, mt.dragonfly, upgradePaused)
			r.handleRecloneAnnotation(ctx, fg, nn, mt.tm)
		}
	}

	// Stop managers for deleted CRs.
	var stopping []*managedTopology
	r.reservationMu.Lock()
	for nn := range r.rotationReservations {
		if _, ok := seen[nn]; !ok {
			delete(r.rotationReservations, nn)
		}
	}
	r.reservationMu.Unlock()
	r.mu.Lock()
	for nn, mt := range r.managers {
		if _, ok := seen[nn]; !ok {
			r.logger.Info("stopping topology manager for deleted fg", "fg", nn)
			r.retireManagerLocked(nn, mt)
			stopping = append(stopping, mt)
		}
	}
	for nn := range r.plannedClaims {
		if _, ok := seen[nn]; !ok && r.managers[nn] == nil && len(r.retiring[nn]) == 0 {
			delete(r.plannedClaims, nn)
			delete(r.retiredFailover, nn)
			delete(r.incarnations, nn)
		}
	}
	r.mu.Unlock()
	for _, mt := range stopping {
		stopManagedTopology(mt)
		clearManagedSiteGauges(mt.tm)
	}
	r.pruneStartFailures(seen)

	return nil
}

// handleRecloneAnnotation validates and queues the durable reclone annotation
// under the safety interlock described in reclone.go. Invalid
// annotations are rejected with a RecloneRejected Event and the
// annotation is cleared so the admin can retry with a fixed value
// rather than receive one rejection event per reconcile.
func (r *TopologyManagerRunner) handleRecloneAnnotation(ctx context.Context, fg *v1alpha1.MysqlFailoverGroup, nn types.NamespacedName, tm *TopologyManager) {
	raw := fg.GetAnnotations()[RecloneAnnotation]
	if raw == "" {
		return
	}
	req := parseRecloneAnnotation(raw)
	if err := validateRecloneRequest(fg, req); err != nil {
		r.logger.Warn("reclone annotation rejected", "fg", nn, "value", raw, "error", err.Error())
		if r.recorder != nil {
			r.recorder.Eventf(fg, corev1.EventTypeWarning, "RecloneRejected", "%s", err.Error())
		}
		// Clear the annotation so the admin sees a single rejection and
		// can re-apply with the correct value. Leaving it would spam
		// the same event on every sync interval.
		r.removeRecloneAnnotation(ctx, nn)
		return
	}
	if slices.Contains(restoreRecloneBlockedSites(fg), req.Site) {
		// The site awaits its post-restore reclone and its queue clones
		// nothing now: Armed while its restore runs, Held while the
		// primary is no trusted donor. That includes a request the queue
		// itself wrote before it was superseded.
		msg := fmt.Sprintf("reclone of %s refused: it awaits its post-restore reclone and status.restoreReclones is %s, so nothing is cloned to it now. "+
			"Once the primary's data is good, set status.restoreReclones.phase to Draining instead", req.Site, fg.Status.RestoreReclones.Phase)
		r.logger.Warn("reclone annotation rejected", "fg", nn, "value", raw, "error", msg)
		if r.recorder != nil {
			r.recorder.Eventf(fg, corev1.EventTypeWarning, "RecloneRejected", "%s", msg)
		}
		tm.cancelPendingReclone(req.Site)
		r.removeRecloneAnnotation(ctx, nn)
		return
	}
	// The restore queue records Requested before it writes the
	// annotation, so a request for its Requested site is the queue's.
	owner := ""
	if q := fg.Status.RestoreReclones; q != nil && q.Requested == req.Site {
		owner = q.ConfirmToken
	}
	if tm.SetRecloneSiteFor(req.Site, owner) && r.recorder != nil {
		if owner != "" {
			r.recorder.Eventf(fg, corev1.EventTypeNormal, "RecloneRequested",
				"in-place restore (confirm=%s) requested CLONE INSTANCE of site %q", owner, req.Site)
		} else {
			r.recorder.Eventf(fg, corev1.EventTypeNormal, "RecloneRequested",
				"admin requested CLONE INSTANCE of site %q", req.Site)
		}
	}
}

// removeRecloneAnnotation removes a rejected one-shot reclone annotation.
func (r *TopologyManagerRunner) removeRecloneAnnotation(ctx context.Context, nn types.NamespacedName) {
	if err := r.removeRecloneAnnotationForSite(ctx, nn, ""); err != nil {
		r.logger.Error("failed to remove reclone annotation", "fg", nn, "error", err)
	}
}

func (r *TopologyManagerRunner) removeRecloneAnnotationForSite(ctx context.Context, nn types.NamespacedName, expectedSite string) error {
	return k8sretry.RetryOnConflict(k8sretry.DefaultRetry, func() error {
		var fresh v1alpha1.MysqlFailoverGroup
		if err := r.client.Get(ctx, nn, &fresh); err != nil {
			return err
		}
		annotations := fresh.GetAnnotations()
		if annotations == nil {
			return nil
		}
		if _, ok := annotations[RecloneAnnotation]; !ok {
			return nil
		}
		if expectedSite != "" && parseRecloneAnnotation(annotations[RecloneAnnotation]).Site != expectedSite {
			return fmt.Errorf("reclone annotation changed before start")
		}
		delete(annotations, RecloneAnnotation)
		fresh.SetAnnotations(annotations)
		return r.client.Update(ctx, &fresh)
	})
}

// checkSpecDrift compares the desired spec hash for each site against the live
// Deployment annotation. If they differ, it records the drifted sites on the
// topology manager so the next poll cycle can trigger an ordered update.
func (r *TopologyManagerRunner) checkSpecDrift(ctx context.Context, fg *v1alpha1.MysqlFailoverGroup, tm *TopologyManager) {
	// Skip if the updater is already running.
	if tm.isUpdating() {
		return
	}
	// Recreate is owned by the bulk reconciler, which patches every site
	// Deployment in one pass. Never let a partial or transient drift snapshot
	// route this strategy back through the ordered updater.
	if fg.Spec.UpdateStrategy == "Recreate" {
		tm.SetSpecDriftSites(nil)
		return
	}

	// The reconciler creates the digest key before the first Deployment;
	// until it exists no live hash can match, so keep the current set.
	digestKey, err := readDigestKey(ctx, r.client, fg)
	if err != nil {
		if err != errDigestKeyMissing {
			r.logger.Warn("failed to read secret digest key; preserving current drift set", "error", err)
		}
		return
	}
	secretInputs, err := specSecretInputs(ctx, r.client, fg)
	if err != nil {
		r.logger.Warn("failed to read spec secrets; preserving current drift set", "error", err)
		return
	}
	secretDigest := secretContentDigest(digestKey, secretInputs)

	var driftSites []string
	for _, site := range fg.Spec.Sites {
		desiredHash := ComputeSpecHash(fg, site, secretDigest)

		var deploy appsv1.Deployment
		deployNN := types.NamespacedName{
			Namespace: fg.Namespace,
			Name:      resourceName(fg.Name, site.Name),
		}
		if err := r.client.Get(ctx, deployNN, &deploy); err != nil {
			if apierrors.IsNotFound(err) {
				continue // Deployment doesn't exist yet — reconciler will create it
			}
			r.logger.Warn("failed to inspect deployment spec drift; preserving current drift set", "site", site.Name, "error", err)
			return
		}
		liveHash := deploy.Annotations[specHashAnnotation]
		if liveHash != desiredHash {
			driftSites = append(driftSites, site.Name)
		}
	}

	tm.SetSpecDriftSites(driftSites)
}

// startManager creates and starts a TopologyManager for a single
// MysqlFailoverGroup. prev is the manager it replaces, already stopped,
// or nil.
func (r *TopologyManagerRunner) startManager(ctx context.Context, fg *v1alpha1.MysqlFailoverGroup, cfg TopologyConfig, prev *managedTopology) error {
	nn := FailoverGroupNamespacedName(fg)

	// Read the operator credentials secret.
	operatorSecretName := fg.Spec.EffectiveOperatorSecretName()
	var secret corev1.Secret
	secretNN := types.NamespacedName{Namespace: fg.Namespace, Name: operatorSecretName}
	if err := r.client.Get(ctx, secretNN, &secret); err != nil {
		return fmt.Errorf("get secret %s: %w", secretNN, err)
	}

	siteMySQL := make([]internalmysql.Checker, len(fg.Spec.Sites))
	for i, site := range fg.Spec.Sites {
		var dsn string
		var err error
		tlsConfigName := ""
		if fg.Spec.TLS != nil {
			// Dial the always-published internal Service while retaining the
			// client-facing hostname as TLS ServerName for existing certificates.
			tlsConfigName, err = mysqlTLSConfig(ctx, r.client, fg, siteServiceHost(fg.Name, site.Name, fg.Namespace))
			if err != nil {
				for j := 0; j < i; j++ {
					siteMySQL[j].Close()
				}
				return fmt.Errorf("configure TLS for site %s: %w", site.Name, err)
			}
		}
		if fg.Spec.UsesCredentials() {
			dsn = buildSiteDSNFromCreds(
				string(secret.Data["username"]),
				string(secret.Data["password"]),
				fg, site, tlsConfigName,
			)
		} else {
			dsnBytes, ok := secret.Data["dsn"]
			if !ok {
				return fmt.Errorf("secret %s missing 'dsn' key", secretNN)
			}
			dsn, err = buildSiteDSN(string(dsnBytes), fg, site, tlsConfigName)
			if err != nil {
				for j := 0; j < i; j++ {
					siteMySQL[j].Close()
				}
				return fmt.Errorf("build site %s DSN: %w", site.Name, err)
			}
		}

		checker, err := internalmysql.NewChecker(dsn)
		if err != nil {
			for j := 0; j < i; j++ {
				siteMySQL[j].Close()
			}
			return fmt.Errorf("create site %s MySQL checker: %w", site.Name, err)
		}
		siteMySQL[i] = checker
	}

	failoverCtl := NewFailoverController(r.logger.With("fg", nn.String()))
	tainter := platform.NewNodeTainter(r.clientset, r.logger.With("fg", nn.String()))

	dns := platform.NewDNSEndpointUpdater(r.client, fg.Name, string(fg.UID), fg.Namespace, fg.Name, fg.Spec.DNS.Hostname, fg.Spec.DNS.TTL)
	if spec, ok := dns.(platform.DNSSpecController); ok {
		spec.SetRecordSpec(fg.Spec.DNS.Hostname, fg.Spec.DNS.TTL, fg.Generation)
	}

	// Build bootstrap configuration. In credentials mode, use operator
	// username/password as replication credentials unless overridden.
	var replUser, replPassword string
	if fg.Spec.UsesCredentials() {
		replUser = string(secret.Data["MYSQL_REPLICATION_USER"])
		replPassword = string(secret.Data["MYSQL_REPLICATION_PASSWORD"])
		if replUser == "" {
			replUser = string(secret.Data["username"])
		}
		if replPassword == "" {
			replPassword = string(secret.Data["password"])
		}
	} else {
		replUser = string(secret.Data["MYSQL_REPLICATION_USER"])
		replPassword = string(secret.Data["MYSQL_REPLICATION_PASSWORD"])
	}

	var bootstrapCtl *BootstrapController
	var bootstrapCfg BootstrapConfig
	if replUser == "" || replPassword == "" {
		missing := "MYSQL_REPLICATION_USER"
		if replUser != "" {
			missing = "MYSQL_REPLICATION_PASSWORD"
		}
		r.logger.Warn("auto-bootstrap disabled: secret missing replication credentials", "fg", nn, "missing", missing)
	} else {
		bootstrapCtl = NewBootstrapController(r.logger.With("fg", nn.String()))
		bootstrapCfg = BootstrapConfig{
			ReplUser:     replUser,
			ReplPassword: replPassword,
			UseSSL:       fg.Spec.TLS != nil,
		}
		bootstrapCfg.CloneTimeout = cloneTimeoutFromSpec(fg)
	}

	updateCtl := NewUpdateController(failoverCtl, r.logger.With("fg", nn.String()))
	tm := NewTopologyManager(cfg, siteMySQL, failoverCtl, updateCtl, bootstrapCtl, bootstrapCfg, tainter, r.hub, dns,
		r.logger.With("fg", nn.String()))

	// Encryption-at-rest clone gate: a CLONE INSTANCE recipient needs a
	// writable keyring to rewrap the donor's tablespace keys, so the
	// topology manager must ask the reconciler to unseal the site first.
	// deployReconciler is the reconciler; the type assertion keeps the
	// runner from depending on the concrete type.
	//
	// Always wired. RequestKeyringUnseal is a no-op when encryption is
	// off, and TopologyConfig does not change when encryption is adopted,
	// so a construction-time EncryptionEnabled() check would miss the
	// adoption path. UnsealReason=Clone is sticky until
	// NotifyCloneComplete, so wiring the gate cannot livelock a reclone.
	if gate, ok := r.deployReconciler.(KeyringGate); ok {
		tm.SetKeyringGate(gate)
	}

	// Must land before Run: the first Poll otherwise sees an empty
	// blocked set and can promote a mid-rotation replica.
	tm.SetKeyringRotationBlocked(rotationBlockedSites(fg))
	// The planned-failover claim is hydrated from an uncached read only
	// (registerManager, before Run): the RecoveryInProgress markers
	// restored below would otherwise let the first Poll drain or recover
	// sites a planned failover that survived the restart is using. A read
	// error fails the start, as initRestoreProtection's does.
	viewSlot := r.Slot(nn)
	viewSlotGen := viewSlot.generation()
	plannedView, err := r.plannedFailoverView(ctx, fg, prev)
	if err != nil {
		for _, c := range siteMySQL {
			c.Close()
		}
		return err
	}
	// Must land before Run, like the set above: a held peer's stopped
	// receiver must not be restarted, nor a held site cloned into, by the
	// first poll. Read uncached: the cached list may predate the queue.
	if err := r.initRestoreProtection(ctx, tm, fg, prev); err != nil {
		for _, c := range siteMySQL {
			c.Close()
		}
		return err
	}

	r.restoreFailoverState(tm, fg, nn)
	r.restorePlannedFailoverState(tm, plannedView, nn)
	if plannedView == nil && prev != nil {
		// The uncached read failed, so nothing re-derived the planned
		// naming; the predecessor's live naming (a source a run left
		// fenced, not yet re-asserted) carries over instead (K024).
		tm.carryNamedActiveSite(prev.tm)
	}
	for _, site := range fg.Status.Sites {
		if site.SourceConvergenceState != "" || site.SourceHost != "" {
			tm.SetSourceConvergence(site.Name, site.SourceHost, SourceConvergenceState(site.SourceConvergenceState), site.SourceConvergenceReason)
		}
		if site.RecoveryState != recoveryStateBlocked && site.RecoveryState != recoveryStateInProgress && site.DivergentGtid == "" {
			continue
		}
		if site.RecoveryState == recoveryStateInProgress {
			tm.SetRecoveryInProgress(site.Name)
			r.logger.Info("restored recovery in-progress state from CR status", "fg", nn, "site", site.Name)
			continue
		}
		var count int64
		if site.DivergentTransactionCount != nil {
			count = *site.DivergentTransactionCount
		}
		tm.SetRecoveryBlockedFrom(site.Name, site.DivergentGtid, count, site.DivergenceDetectedBy, site.DivergenceComparedTo)
		r.logger.Info("restored recovery blocked state from CR status",
			"fg", nn, "site", site.Name, "divergentGtid", site.DivergentGtid, "divergentTransactionCount", count,
			"detectedBy", site.DivergenceDetectedBy, "comparedTo", site.DivergenceComparedTo)
	}

	tmCtx, cancel := context.WithCancel(ctx)
	// Set the status callback to update the CR status subresource on state
	// changes. Feed the write result back to the manager so a rejected
	// /status write (e.g. RBAC-denied mid-failover) arms a per-poll retry
	// that self-heals once the write is permitted again.
	tm.StatusCallback = func(snap TopologySnapshot) {
		r.mu.RLock()
		current := r.managers[nn]
		r.mu.RUnlock()
		if current == nil || current.tm != tm {
			return
		}
		err := r.updateCRStatus(tmCtx, nn, snap)
		tm.mu.Lock()
		tm.statusWriteFailed = err != nil || tm.deploymentEpoch != snap.DeploymentEpoch || tm.topologyGeneration != snap.TopologyGeneration
		tm.mu.Unlock()
	}
	// BootstrapStatusCallback updates only the Bootstrapping condition so that
	// unrelated conditions set by the most recent Poll cycle (Degraded,
	// ReplicationBroken, Updating, ...) are not clobbered by a partially
	// populated snapshot from the async bootstrap goroutine.
	tm.BootstrapStatusCallback = func(phase, errMsg, source string) {
		r.updateBootstrappingCondition(ctx, nn, phase, errMsg, source)
	}
	tm.RecloneCompleteCallback = func(completeCtx context.Context, site string, outcome RecloneOutcome) error {
		// Record the outcome on the restore queue first: once the
		// annotation is gone the queue may already request its next clone.
		if err := r.recordRestoreRecloneOutcome(completeCtx, nn, site, outcome); err != nil {
			return err
		}
		return r.removeRecloneAnnotationForSite(completeCtx, nn, site)
	}

	// Wire the ordered update callbacks so the UpdateController can reconcile
	// one site's Deployment at a time and wait for its pod to be Ready.
	if r.deployReconciler != nil {
		tm.ApplyUpdate = func(applyCtx context.Context, siteName string) error {
			return r.deployReconciler.ReconcileSiteDeployment(applyCtx, nn, siteName)
		}
		tm.PodReady = func(probeCtx context.Context, siteName string) (PodReadiness, error) {
			return r.deployReconciler.SitePodReadiness(probeCtx, nn, siteName)
		}
	}

	var dfMgr *DragonflyManager
	if dragonflyEnabled(fg) {
		dfPollInterval := time.Duration(cfg.PollInterval)
		if dfPollInterval <= 0 {
			dfPollInterval = 2 * time.Second
		}
		dfMgr = r.newDragonflyManager(fg, nn, dfPollInterval)
	}

	// Wire the best-effort emergency Dragonfly promotion. Runs only
	// after MySQL emergency failover Execute has succeeded; failures
	// here are logged and never propagate back to MySQL. The callback only
	// queues the request: the Dragonfly manager's own Run goroutine
	// executes it between Ticks, so a stalled old master (a takeover, then
	// a fallback that waits behind it) never holds up the MySQL poll.
	if dfMgr != nil {
		tm.EmergencyFailoverCallback = emergencyDragonflyCallback(dfMgr)
	}

	archiver := r.newPITRArchiverPoller(ctx, fg, nn, buildSidecarClients(fg), prev)
	archiver.awaitingRollout = tm.SpecDriftSites
	archiver.rolloutKnown = tm.specDriftComputed

	done := make(chan struct{})
	archiverDone := make(chan struct{})
	r.registerManagerWith(nn, plannedView, &managedTopology{
		tm:           tm,
		cancel:       cancel,
		done:         done,
		cfg:          cfg,
		uid:          fg.UID,
		viewSlot:     viewSlot,
		viewSlotGen:  viewSlotGen,
		archiver:     archiver,
		archiverDone: archiverDone,
		dragonfly:    dfMgr,
	})

	go func() {
		defer close(done)
		r.logger.Info("starting topology manager", "fg", nn)
		tm.Run(tmCtx)
		for i := range siteMySQL {
			siteMySQL[i].Close()
		}
		r.logger.Info("topology manager stopped", "fg", nn)
	}()

	go func() {
		defer close(archiverDone)
		archiver.Run(tmCtx)
	}()

	if dfMgr != nil {
		go func() {
			r.logger.Info("starting dragonfly manager", "fg", nn)
			dfMgr.Run(tmCtx)
			r.logger.Info("dragonfly manager stopped", "fg", nn)
		}()
	}

	return nil
}

// newDragonflyManager builds a group's Dragonfly manager: it shares the
// group's stable gate, reads the group through the runner's uncached
// reader, and has two independent pause sources. The planned owner is the
// group's claim slot (a running planned failover or a hold), read live, so
// no sync or upgrade step can revoke it; the upgrade pause comes from the
// snapshot upgrade's own status and starts set while one is in flight.
func (r *TopologyManagerRunner) newDragonflyManager(fg *v1alpha1.MysqlFailoverGroup, nn types.NamespacedName, pollInterval time.Duration) *DragonflyManager {
	dfMgr := NewDragonflyManager(r.client, r.recorder, r.logger.With("fg", nn.String(), "subsystem", "dragonfly"), nn, pollInterval)
	dfMgr.SetGate(r.DragonflyGate(nn))
	if r.apiReader != nil {
		dfMgr.SetAuthorityReader(r.apiReader)
	}
	dfMgr.SetPlannedOwner(func() bool { return r.PlannedClaimHeld(nn) })
	dfMgr.SetUpgradePaused(dragonflyUpgradeInFlight(dragonflyUpgradeStatus(fg)))
	return dfMgr
}

// newPITRArchiverPoller builds the group's archiver poller, seeded by
// seedPITRArchiveTracker (prev is the stopped manager it replaces, or
// nil), and wired to write PITR status after every completed poll round
// while it is this group's current poller. clients is aligned with
// fg.Spec.Sites.
func (r *TopologyManagerRunner) newPITRArchiverPoller(ctx context.Context, fg *v1alpha1.MysqlFailoverGroup, nn types.NamespacedName, clients []*internalmysql.SidecarClient, prev *managedTopology) *archiverPoller {
	siteNames := make([]string, len(fg.Spec.Sites))
	for i, s := range fg.Spec.Sites {
		siteNames[i] = s.Name
	}
	tracker, lastGood, readErr := r.seedPITRArchiveTracker(ctx, fg, nn, siteNames, prev)
	tracker.scanInterval = pitrArchiveScanInterval(fg)
	p := newArchiverPoller(fg.Namespace, fg.Name, siteNames, clients, tracker, r.logger.With("fg", nn.String()))
	if readErr != nil {
		r.notePITRArchiveReadFailed(nn, p, readErr)
	}
	p.uid = fg.UID
	p.rolloutRenders = pitrSpecRendersArchiver(fg)
	for site, st := range lastGood {
		p.lastGood[site] = st
	}
	p.onRound = func(ctx context.Context) {
		r.mu.RLock()
		current := r.managers[nn]
		r.mu.RUnlock()
		if current == nil || current.archiver != p {
			return
		}
		_ = r.updatePITRArchiveStatus(ctx, nn)
	}
	return p
}

// emergencyDragonflyCallback returns the TopologyManager callback that asks
// the Dragonfly manager to follow a MySQL emergency failover. It only
// queues the request (DragonflyManager.Enqueue, which never blocks), so the
// MySQL poll never waits for the cache.
func emergencyDragonflyCallback(dfMgr *DragonflyManager) func(context.Context, string, string) {
	return func(_ context.Context, target, oldPrimary string) {
		dfMgr.Enqueue(target, oldPrimary)
	}
}

// restoreFailoverState wires the out-of-band anti-flap store and restores
// failover history into a fresh TopologyManager, so recovery logic and the
// anti-flap cooldown work across operator restarts — without this,
// the topology.go cooldown branch becomes a no-op until the next failover
// stamps a fresh value, so a fast restart-then-peer-failure could ping-pong
// promotions that the original process would have blocked.
//
// Two durable copies exist and either can be the fresher one: status is
// ahead when the annotation write was rejected, the annotations are ahead
// when the status write was. Rehydrating from whichever is newer is what
// makes the cooldown survive an outage on one path — restoring only from
// status is exactly the CooldownViolated(restart) window the simulator
// reproduces.
func (r *TopologyManagerRunner) restoreFailoverState(tm *TopologyManager, fg *v1alpha1.MysqlFailoverGroup, nn types.NamespacedName) {
	tm.deploymentLeases = r.deploymentLeases
	// Wire the store before anything can promote. The annotations it writes
	// are the second durable copy of the record below, on an API path that
	// fails independently of the status subresource.
	reader := r.apiReader
	if reader == nil {
		reader = r.client
	}
	tm.SetFailoverStateRecorder(NewAnnotationFailoverStateRecorder(r.client, reader, nn))
	tm.restoreFailoverRecordFrom(fg, nn, r.logger)
}

// restoreFailoverRecordFrom is the manager-side part of restoreFailoverState:
// the observed authority, the watermark and the newer failover record of
// fg's two durable copies. The simulator calls it through
// RestoreFailoverStateForSim.
func (tm *TopologyManager) restoreFailoverRecordFrom(fg *v1alpha1.MysqlFailoverGroup, nn types.NamespacedName, logger *slog.Logger) {
	tm.topologyGeneration = fg.Status.TopologyGeneration
	tm.deploymentActiveSite = fg.Status.ActiveSite
	// Observed, not named: this process did not make that site primary.
	tm.lastActiveSite = fg.Status.ActiveSite
	// The promotion GTID gates which candidates are safe to promote and how
	// a returning old primary is judged, so it must survive a restart. It
	// is taken from the newer record below; this is the status copy.
	tm.promotionGtidExecuted = fg.Status.PromotionGtidExecuted
	tm.groupHasData = strings.TrimSpace(fg.Status.PromotionGtidExecuted) != ""
	for _, site := range fg.Status.Sites {
		if strings.TrimSpace(site.GtidExecuted) != "" {
			tm.groupHasData = true
		}
	}
	if tm.deploymentLeases != nil {
		tm.deploymentTopologyPending()
	}

	statusRecord := FailoverRecordFromStatus(fg)
	failoverRecord, fromAnnotations, err := EffectiveFailoverRecord(fg, tm.clock.Now())
	if err != nil {
		var readErr *FailoverRecordReadError
		if errors.As(err, &readErr) {
			if readErr.Annotations != nil {
				// Corrupt or unsafe annotation: fall back to the other valid
				// copy rather than silently resetting or poisoning the cooldown.
				logger.Error("out-of-band anti-flap annotation unreadable; falling back to CR status",
					"fg", nn.String(), "error", readErr.Annotations)
			}
			if readErr.Status != nil {
				logger.Error("CR status anti-flap state unreadable; ignoring unsafe copy",
					"fg", nn.String(), "error", readErr.Status)
			}
		} else {
			logger.Error("durable anti-flap state unreadable; ignoring unsafe copy",
				"fg", nn.String(), "error", err)
		}
	}

	// The two restores below are logged under distinct msg strings rather
	// than one msg with a source field: `restored lastFailoverTarget from CR
	// status` is a documented stable event (site/content/docs/8.observability/7.log-schema.md), and
	// the out-of-band case is worth alerting on in its own right — it means
	// this group's status writes were failing when it last promoted.
	if failoverRecord.LastFailoverTarget != "" {
		tm.SetLastFailoverTarget(failoverRecord.LastFailoverTarget)
		if fromAnnotations {
			logger.Warn("restored lastFailoverTarget from out-of-band annotations",
				"fg", nn.String(), "target", failoverRecord.LastFailoverTarget,
				"statusTarget", statusRecord.LastFailoverTarget)
		} else {
			logger.Info("restored lastFailoverTarget from CR status", "fg", nn.String(), "target", failoverRecord.LastFailoverTarget)
		}
	}
	if !failoverRecord.LastFailover.IsZero() {
		tm.SetLastFailover(failoverRecord.LastFailover)
		if fromAnnotations {
			logger.Warn("restored lastFailover from out-of-band annotations",
				"fg", nn.String(), "lastFailover", failoverRecord.LastFailover,
				"statusLastFailover", statusRecord.LastFailover)
		} else {
			logger.Info("restored lastFailover from CR status", "fg", nn.String(), "lastFailover", failoverRecord.LastFailover)
		}
	}
	// The watermark goes with the newer record. An out-of-band record
	// without one (written before it carried the watermark, or one too
	// large for an annotation) falls back to the status copy.
	if fromAnnotations && failoverRecord.PromotionGtidExecuted != "" {
		tm.promotionGtidExecuted = failoverRecord.PromotionGtidExecuted
		tm.groupHasData = true
	}
	if fromAnnotations && !failoverRecord.sameEvent(statusRecord) {
		// A promotion survived in annotations but its generation did not reach
		// /status. Invalidate old leases even if authority returned to the same site.
		tm.topologyGeneration++
		tm.deploymentActiveSite = failoverRecord.LastFailoverTarget
	}
}

// stopAll cancels all running topology managers.
func (r *TopologyManagerRunner) stopAll() {
	r.mu.Lock()
	stopping := make([]*managedTopology, 0, len(r.managers))
	for nn, mt := range r.managers {
		r.logger.Info("stopping topology manager", "fg", nn)
		r.retireManagerLocked(nn, mt)
		stopping = append(stopping, mt)
	}
	r.startFailures = nil
	r.lastStartWarning = nil
	r.mu.Unlock()
	for _, mt := range stopping {
		stopManagedTopology(mt)
		clearManagedSiteGauges(mt.tm)
	}
}

// Status returns the StatusResponse for a named failover group.
func (r *TopologyManagerRunner) Status(nn types.NamespacedName) (StatusResponse, bool) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	mt, ok := r.managers[nn]
	if !ok {
		return StatusResponse{}, false
	}
	return mt.tm.Status(), true
}

// AllStatuses returns a map of all active topology manager statuses.
func (r *TopologyManagerRunner) AllStatuses() map[string]StatusResponse {
	r.mu.RLock()
	defer r.mu.RUnlock()
	result := make(map[string]StatusResponse, len(r.managers))
	for nn, mt := range r.managers {
		result[nn.String()] = mt.tm.Status()
	}
	return result
}

// Ready returns true if at least one topology manager is running and ready.
func (r *TopologyManagerRunner) Ready() bool {
	r.mu.RLock()
	defer r.mu.RUnlock()
	for _, mt := range r.managers {
		if mt.tm.Ready() {
			return true
		}
	}
	return false
}

// retireManagerLocked unregisters mt and keeps it admission-visible until
// its mutation workers have exited. The manager is retired first, so it
// starts no ordered update once it can no longer be found in managers;
// one already running is found in retiring, and rotation admission refuses
// while it is in flight. Caller holds r.mu.
func (r *TopologyManagerRunner) retireManagerLocked(nn types.NamespacedName, mt *managedTopology) {
	if mt == nil {
		return
	}
	if mt.tm != nil {
		mt.tm.retire()
	}
	if r.managers[nn] == mt {
		delete(r.managers, nn)
	}
	if mt.tm == nil {
		return
	}
	if r.retiring == nil {
		r.retiring = make(map[types.NamespacedName][]*managedTopology)
	}
	r.retiring[nn] = append(r.retiring[nn], mt)
	r.stashRetiredFailoverLocked(nn, mt)
	// A retired manager adds no update worker, so the first wait ends when
	// the last one exits, whether or not its Run has returned; that
	// releases the replacement's predecessor guard. The entry itself stays
	// until Run has returned too, so rotation admission still sees a
	// promotion the old Run may be finishing (mutationInFlight).
	//
	// An ordered update that finished on the retired manager recorded its
	// handoff there only; the current manager receives that record after
	// the workers exit, and again once Run has returned (DST-1).
	go func() {
		mt.tm.waitUpdateWorkers()
		r.mu.Lock()
		r.stashRetiredFailoverLocked(nn, mt)
		if cur := r.managers[nn]; cur != nil {
			if sameIncarnation(cur.uid, mt.uid) {
				CarryRetiredFailoverRecord(cur.tm, mt.tm)
			}
			if !r.retiringUpdateInFlightLocked(nn) {
				cur.tm.setPredecessorUpdateInFlight(false)
			}
		}
		r.mu.Unlock()
		if mt.done != nil {
			<-mt.done
		}
		r.mu.Lock()
		defer r.mu.Unlock()
		r.stashRetiredFailoverLocked(nn, mt)
		if cur := r.managers[nn]; cur != nil && sameIncarnation(cur.uid, mt.uid) {
			CarryRetiredFailoverRecord(cur.tm, mt.tm)
		}
		r.retiring[nn] = slices.DeleteFunc(r.retiring[nn], func(m *managedTopology) bool { return m == mt })
		if len(r.retiring[nn]) == 0 {
			delete(r.retiring, nn)
		}
	}()
}

// registerManager makes mt nn's running manager with nothing to hydrate
// (see registerManagerWith).
func (r *TopologyManagerRunner) registerManager(nn types.NamespacedName, mt *managedTopology) {
	r.registerManagerWith(nn, nil, mt)
}

// registerManagerWith makes mt nn's running manager. Call it before mt's
// Run starts. It registers under reservationMu with everything
// WireRegisteredManager applies already in place: the group's pending
// rotation reservations (a rotation admitted against the manager this one
// replaces must block promotion here before Run can claim an ordered
// update), the predecessor guards (a predecessor's ordered update can
// outlive its Run: until it exits, retireManagerLocked clears the flag, mt
// leaves the sites to it), and the group's planned-failover claim slot,
// shared with the predecessor and hydrated from view, an uncached read
// (nil: nothing to hydrate).
//
// A manager for a new incarnation of the CR (another UID under the same
// name) gets a fresh claim slot and no retired record or predecessor
// record: the old incarnation's hold or run, and its failover history,
// are not the new object's. The predecessors' mutation barriers still
// apply until their workers exit, as for any replacement. A view read
// before the slot released a claim hydrates nothing: the release (an
// acknowledgement, a terminal record) happened after the read.
func (r *TopologyManagerRunner) registerManagerWith(nn types.NamespacedName, view *v1alpha1.MysqlFailoverGroup, mt *managedTopology) {
	r.reservationMu.Lock()
	defer r.reservationMu.Unlock()
	pending := r.pendingKeyringRotationReservations(nn)
	r.mu.Lock()
	defer r.mu.Unlock()
	r.adoptIncarnationLocked(nn, mt.uid)
	w := ManagerWiring{
		Slot:                   r.slotLocked(nn),
		PendingRotations:       pending,
		RetiringUpdateInFlight: r.retiringUpdateInFlightLocked(nn),
	}
	if len(r.retiring[nn]) > 0 {
		w.RetiringBusy = func() bool { return r.retiringChangingSites(nn) }
	}
	for _, old := range r.retiring[nn] {
		if sameIncarnation(old.uid, mt.uid) {
			w.Predecessors = append(w.Predecessors, old.tm)
		}
	}
	if st, ok := r.retiredFailover[nn]; ok && sameIncarnation(st.uid, mt.uid) {
		rec := st.rec
		w.RetiredFailover = &rec
	}
	if view != nil {
		// A claim the slot released after the view was read (an
		// acknowledged hold, a run's terminal release) is not hydrated
		// back from it; anything else hydrates, failing closed.
		stale := func(runID string) bool {
			return mt.viewSlot == w.Slot && w.Slot.releasedSince(mt.viewSlotGen, runID)
		}
		if h := PlannedHoldFrom(view); h != nil && !stale(h.RunID) {
			w.HoldFrom = h
		}
		if PlannedRunNeedsClaim(view, mt.tm.clock.Now()) && !stale(view.Status.PlannedFailover.RunID) {
			w.HydrateFrom = view.Status.PlannedFailover
		}
	}
	WireRegisteredManager(mt.tm, w)
	r.managers[nn] = mt
}

// stashRetiredFailoverLocked keeps the newer of nn's stashed record and
// mt's in-memory failover record. A manager of an earlier incarnation of
// the CR stashes nothing, and its record replaces nothing. Caller holds
// r.mu.
func (r *TopologyManagerRunner) stashRetiredFailoverLocked(nn types.NamespacedName, mt *managedTopology) {
	if !sameIncarnation(mt.uid, r.incarnations[nn]) {
		return
	}
	rec, ok := mt.tm.failoverRecordSnapshot()
	if !ok {
		return
	}
	if r.retiredFailover == nil {
		r.retiredFailover = make(map[types.NamespacedName]retiredFailoverRecord)
	}
	if cur, ok := r.retiredFailover[nn]; ok && sameIncarnation(cur.uid, mt.uid) && !rec.LastFailover.After(cur.rec.LastFailover) {
		return
	}
	r.retiredFailover[nn] = retiredFailoverRecord{uid: mt.uid, rec: rec}
}

// adoptIncarnationLocked records uid as nn's CR incarnation. When it
// replaces another, the group's claim slot is replaced by an empty one (the
// old slot stays with the old incarnation's retiring managers) and the
// retired record is dropped. Caller holds r.mu.
func (r *TopologyManagerRunner) adoptIncarnationLocked(nn types.NamespacedName, uid types.UID) {
	if uid == "" {
		return
	}
	if r.incarnations == nil {
		r.incarnations = make(map[types.NamespacedName]types.UID)
	}
	prev, known := r.incarnations[nn]
	r.incarnations[nn] = uid
	if !known || prev == uid {
		return
	}
	if r.plannedClaims != nil {
		if old, ok := r.plannedClaims[nn]; ok && old.Held() {
			r.logger.Warn("planned failover: the group was recreated; its claim slot starts empty",
				"fg", nn.String(), "previousUid", string(prev), "uid", string(uid))
		}
		delete(r.plannedClaims, nn)
	}
	delete(r.retiredFailover, nn)
}

// retiringChangingSites reports whether a retiring manager of nn is still
// changing the sites (see TopologyManager.changingSites).
func (r *TopologyManagerRunner) retiringChangingSites(nn types.NamespacedName) bool {
	r.mu.RLock()
	retiring := append([]*managedTopology(nil), r.retiring[nn]...)
	r.mu.RUnlock()
	for _, mt := range retiring {
		if mt.tm.changingSites() {
			return true
		}
	}
	return false
}

// retiringUpdateInFlightLocked reports whether a retiring manager of nn
// still has a claimed ordered update. Caller holds r.mu.
func (r *TopologyManagerRunner) retiringUpdateInFlightLocked(nn types.NamespacedName) bool {
	for _, mt := range r.retiring[nn] {
		if mt.tm.orderedUpdateClaimed() {
			return true
		}
	}
	return false
}

// SetManagerForTest injects a TopologyManager for testing. Not for production use.
// The manager is wired to the group's planned-failover claim slot, as
// registerManager wires every production manager.
func (r *TopologyManagerRunner) SetManagerForTest(nn types.NamespacedName, tm *TopologyManager) {
	r.mu.Lock()
	defer r.mu.Unlock()
	WireRegisteredManager(tm, ManagerWiring{Slot: r.slotLocked(nn)})
	r.managers[nn] = &managedTopology{tm: tm}
}

// WriteStatusForTest writes snap to the CR status the way a running
// manager's status callback does. Not for production use.
func (r *TopologyManagerRunner) WriteStatusForTest(ctx context.Context, nn types.NamespacedName, snap TopologySnapshot) error {
	return r.updateCRStatus(ctx, nn, snap)
}

// StopManager stops and removes the topology manager for the given NamespacedName.
// This is called from the reconciler's finalizer handling when a CR is being deleted.
func (r *TopologyManagerRunner) StopManager(nn types.NamespacedName) {
	r.mu.Lock()
	mt, ok := r.managers[nn]
	if !ok {
		r.mu.Unlock()
		r.logger.Info("no topology manager found to stop", "fg", nn)
		return
	}
	r.logger.Info("stopping topology manager via StopManager", "fg", nn)
	r.retireManagerLocked(nn, mt)
	r.mu.Unlock()
	stopManagedTopology(mt)
	clearManagedSiteGauges(mt.tm)
}

// clearManagedSiteGauges drops site-scoped gauge series for every site
// still tracked by tm. Safe to call with a nil manager.
func clearManagedSiteGauges(tm *TopologyManager) {
	if tm == nil {
		return
	}
	for i := range tm.sites {
		metrics.DeleteSiteGauges(tm.cfg.Namespace, tm.cfg.Name, tm.sites[i].name)
	}
	metrics.SetRestoreReclonePending(tm.cfg.Namespace, tm.cfg.Name, "", nil)
}

// managerStopWait bounds how long stopManagedTopology waits for Run to
// exit. A hung MySQL call that ignores its probe context must not wedge
// the runner's sync / StopManager / stopAll path forever.
const managerStopWait = 30 * time.Second

// stopManagedTopology closes the Dragonfly manager's admission, cancels the
// manager and waits (bounded) for its goroutine to return so a subsequent
// series delete cannot be undone by an in-flight Poll, then joins the
// Dragonfly manager's Run goroutine within the same bound. A nil done
// channel (test-injected managers) is a no-op wait.
func stopManagedTopology(mt *managedTopology) {
	stopManagedTopologyWait(mt, managerStopWait)
}

func stopManagedTopologyWait(mt *managedTopology, wait time.Duration) {
	if mt == nil {
		return
	}
	// Close Dragonfly admission first: from here on the old manager starts
	// no new request, Tick or promotion step.
	if mt.dragonfly != nil {
		mt.dragonfly.BeginStop()
	}
	if mt.tm != nil {
		mt.tm.deploymentTopologyPending()
		mt.tm.HaltSiteMetrics()
	}
	if mt.cancel != nil {
		mt.cancel()
	}
	if mt.done != nil {
		timer := time.NewTimer(wait)
		select {
		case <-mt.done:
		case <-timer.C:
			if mt.tm != nil && mt.tm.logger != nil {
				mt.tm.logger.Warn("timed out waiting for topology manager to stop; dropping site gauges anyway")
			}
		}
		timer.Stop()
	}
	// Join the archiver poller too, so its last status write lands before
	// a replacement reads its history (seedPITRArchiveTracker). A poller
	// that does not stop in time is not handed over.
	if mt.archiverDone != nil {
		timer := time.NewTimer(wait)
		select {
		case <-mt.archiverDone:
		case <-timer.C:
		}
		timer.Stop()
	}
	// Join the Dragonfly manager's Run goroutine, which owns any promotion
	// in progress: one that has sent a role-changing command finishes
	// recording it (labels, status, source cleanup) before Run returns. If
	// the wait times out, that last unit of work still holds the group's
	// gate, so a replacement manager or a planned failover waits for it
	// rather than mutating alongside it.
	if mt.dragonfly != nil && !mt.dragonfly.Stop(wait) {
		mt.dragonfly.logger.Warn("timed out waiting for the Dragonfly manager to stop; its last step finishes under the group's Dragonfly gate",
			"fg", mt.dragonfly.fgKey.String())
	}
}

// updateCRStatus writes topology state to the CR status subresource. It
// returns nil when the desired status is persisted (or is a confirmed no-op,
// or the CR is gone) and a non-nil error when the write could not be
// confirmed — for example an RBAC-denied /status update. The caller uses the
// error to arm/disarm a per-poll retry so a transiently-denied write heals
// once permissions return.
func (r *TopologyManagerRunner) updateCRStatus(ctx context.Context, nn types.NamespacedName, snap TopologySnapshot) error {
	// Fetch the CR to get the latest resourceVersion and generation for the status update.
	var freshFG v1alpha1.MysqlFailoverGroup
	if err := r.client.Get(ctx, nn, &freshFG); err != nil {
		if apierrors.IsNotFound(err) {
			r.logger.Warn("CR deleted, skipping status update", "fg", nn)
			return nil
		}
		r.logger.Error("get fg for status update", "fg", nn, "error", err)
		return err
	}

	// Save existing status before modification for comparison.
	existingStatus := freshFG.Status.DeepCopy()
	if !MergeTopologySnapshotStatus(&freshFG.Status, snap) {
		r.hydrateDeploymentGeneration(&freshFG)
		return nil
	}

	// Evaluate replication health.
	hasWritable := validSnapshotActiveSite(snap) != ""
	replicationHealthy := true
	for _, s := range snap.Sites {
		if s.Role == state.SiteRoleReadOnly || s.Name == snap.ActiveSite {
			continue
		}
		replicationHealthy = replicationHealthy && s.ReplicationHealthy &&
			s.SourceConvergenceState == sourceConvergenceConverged
	}

	// Update conditions using freshFG.Generation so ObservedGeneration reflects
	// the generation of the object we are actually writing against.
	now := metav1.Now()
	reason := degradedReason(snap)
	readyStatus := conditionBool(hasWritable && replicationHealthy)
	readyReason := "TopologyPolled"
	readyMessage := "At least one site is writable and replication is healthy"
	if !hasWritable {
		readyMessage = "No site is writable"
	} else if !replicationHealthy {
		readyMessage = "Replication is not healthy"
		if q := freshFG.Status.RestoreReclones; q != nil && len(q.Pending) > 0 {
			readyMessage = fmt.Sprintf("Replication is not healthy; %s await their post-restore reclone with replication stopped",
				strings.Join(q.Pending, ", "))
		}
	}
	degradedStatus, degradedMessage := metav1.ConditionTrue, snap.Alert
	switch reason {
	case "Healthy":
		degradedStatus, degradedMessage = metav1.ConditionFalse, "No cross-site alerts"
	case "Initializing":
		// Neither is known yet. Unknown keeps Ready=False and Degraded=True
		// consumers from reading every operator start as an outage.
		readyStatus, readyReason, readyMessage = metav1.ConditionUnknown, reason, initializingMessage(snap)
		degradedStatus, degradedMessage = metav1.ConditionUnknown, readyMessage
	}
	setCondition(&freshFG.Status.Conditions, metav1.Condition{
		Type:               "Ready",
		Status:             readyStatus,
		ObservedGeneration: freshFG.Generation,
		LastTransitionTime: now,
		Reason:             readyReason,
		Message:            readyMessage,
	})
	setCondition(&freshFG.Status.Conditions, metav1.Condition{
		Type:               "Degraded",
		Status:             degradedStatus,
		ObservedGeneration: freshFG.Generation,
		LastTransitionTime: now,
		Reason:             reason,
		Message:            degradedMessage,
	})

	// Add replication-specific degraded conditions.
	maxLagSeconds := freshFG.Spec.EffectiveMaxLagSeconds()

	for _, s := range snap.Sites {
		if s.Role == state.SiteRoleReadOnly {
			continue
		}
		if restoreReclonePending(&freshFG, s.Name) {
			// Awaiting its post-restore reclone with replication held
			// stopped by design; RestoreReclonePending below reports it.
			continue
		}
		// Evaluate source convergence before the nil-replication guard: a
		// failed probe leaves Replication nil while the convergence state is
		// Pending/ProbeFailed, and skipping here would suppress the Degraded
		// condition. Only read-only followers carry convergence state — the
		// writable primary is excluded.
		if reason == "Healthy" && s.State == state.StateReadOnly && s.SourceConvergenceState != sourceConvergenceConverged {
			setCondition(&freshFG.Status.Conditions, metav1.Condition{
				Type: "Degraded", Status: metav1.ConditionTrue,
				ObservedGeneration: freshFG.Generation, LastTransitionTime: now,
				Reason:  "ReplicationSourceMismatch",
				Message: fmt.Sprintf("Replication source on %s is not the active primary", s.Name),
			})
		}
		repl := s.Replication
		if repl == nil {
			continue
		}
		siteName := s.Name
		if !repl.IORunning || !repl.SQLRunning {
			setCondition(&freshFG.Status.Conditions, metav1.Condition{
				Type:               "Degraded",
				Status:             metav1.ConditionTrue,
				ObservedGeneration: freshFG.Generation,
				LastTransitionTime: now,
				Reason:             "ReplicationBroken",
				Message:            fmt.Sprintf("Replication IO/SQL thread not running on %s", siteName),
			})
		}
		degraded := apimeta.FindStatusCondition(freshFG.Status.Conditions, "Degraded")
		if repl.SecondsBehindSource != nil && *repl.SecondsBehindSource > maxLagSeconds &&
			(degraded == nil || degraded.Status != metav1.ConditionTrue || degraded.Reason == "ReplicationLagging") {
			setCondition(&freshFG.Status.Conditions, metav1.Condition{
				Type:               "Degraded",
				Status:             metav1.ConditionTrue,
				ObservedGeneration: freshFG.Generation,
				LastTransitionTime: now,
				Reason:             "ReplicationLagging",
				Message:            fmt.Sprintf("Replication lag on %s is %ds (threshold %ds)", siteName, *repl.SecondsBehindSource, maxLagSeconds),
			})
		}
		if repl.LastError != "" {
			setCondition(&freshFG.Status.Conditions, metav1.Condition{
				Type:               "Degraded",
				Status:             metav1.ConditionTrue,
				ObservedGeneration: freshFG.Generation,
				LastTransitionTime: now,
				Reason:             "ReplicationError",
				Message:            fmt.Sprintf("Replication error on %s: %s", siteName, repl.LastError),
			})
		}
	}

	// Sites awaiting their post-restore reclone hold pre-restore data,
	// are never promoted automatically and do not replicate until they
	// are recloned, so the group runs without them as failover targets.
	// Any other problem reported above takes precedence. It also applies
	// during Initializing (turning Unknown into True): the queue is
	// persisted status, not a topology observation, so it is known before
	// the topology is classified, and True keeps the deployment API
	// refusing leases while peers are held.
	if q := freshFG.Status.RestoreReclones; q != nil && len(q.Pending) > 0 && (reason == "Healthy" || reason == "Initializing") {
		if degraded := apimeta.FindStatusCondition(freshFG.Status.Conditions, "Degraded"); degraded == nil || degraded.Status != metav1.ConditionTrue {
			setCondition(&freshFG.Status.Conditions, metav1.Condition{
				Type: "Degraded", Status: metav1.ConditionTrue,
				ObservedGeneration: freshFG.Generation, LastTransitionTime: now,
				Reason: "RestoreReclonePending",
				Message: fmt.Sprintf("%s await their post-restore reclone (status.restoreReclones phase %s); their replication is stopped until then",
					strings.Join(q.Pending, ", "), q.Phase),
			})
		}
	}

	// A divergence block outranks the per-site replication reasons above,
	// which are last-writer-wins: a lagging or broken replica must not hide
	// that a site holds transactions the primary lacks. Topology alerts
	// (split brain, no primary, total loss) still win; Initializing is not
	// an alert and does not. Ready is untouched: the group serves, and Ready
	// gates application rollouts.
	if reason == "Healthy" || reason == "Initializing" {
		if msg := divergenceDegradedMessage(snap); msg != "" {
			setCondition(&freshFG.Status.Conditions, metav1.Condition{
				Type: "Degraded", Status: metav1.ConditionTrue,
				ObservedGeneration: freshFG.Generation, LastTransitionTime: now,
				Reason:  "DivergentTransactions",
				Message: msg,
			})
		}
	}

	// A promotion of a divergent site is recorded once and kept until an
	// administrator removes the condition after reviewing the transactions.
	newDivergentPromotion := false
	if note := snap.DivergentPromotion; note != nil {
		msg := divergentPromotionMessage(*note)
		existing := apimeta.FindStatusCondition(freshFG.Status.Conditions, divergentPromotionCondition)
		if existing == nil || !strings.Contains(existing.Message, msg) {
			newDivergentPromotion = true
			// Earlier promotions an administrator has not reviewed (and
			// removed) stay in the record; the newest is last.
			combined := msg
			if existing != nil && existing.Status == metav1.ConditionTrue && existing.Message != "" {
				combined = appendDivergentPromotion(existing.Message, msg)
			}
			setCondition(&freshFG.Status.Conditions, metav1.Condition{
				Type: divergentPromotionCondition, Status: metav1.ConditionTrue,
				ObservedGeneration: freshFG.Generation, LastTransitionTime: now,
				Reason:  "DivergentSitePromoted",
				Message: combined,
			})
		}
	}

	// Bootstrapping condition reflects fresh-deploy bootstrap progress.
	setBootstrappingCondition(&freshFG.Status.Conditions, freshFG.Generation, now, snap.BootstrapPhase, snap.BootstrapError, snap.BootstrapSource)

	// Update phase tracking (status.updatePhase itself is merged above).
	if snap.UpdatePhase != "" {
		setCondition(&freshFG.Status.Conditions, metav1.Condition{
			Type:               "Updating",
			Status:             metav1.ConditionTrue,
			ObservedGeneration: freshFG.Generation,
			LastTransitionTime: now,
			Reason:             snap.UpdatePhase,
			Message:            fmt.Sprintf("Ordered update in progress: %s", snap.UpdatePhase),
		})
	} else if snap.UpdateStalled != "" {
		setCondition(&freshFG.Status.Conditions, metav1.Condition{
			Type:               "Updating",
			Status:             metav1.ConditionFalse,
			ObservedGeneration: freshFG.Generation,
			LastTransitionTime: now,
			Reason:             ReasonOrderedUpdateStalled,
			Message:            orderedUpdateStalledMessage(snap.UpdateStalled),
		})
	} else {
		setCondition(&freshFG.Status.Conditions, metav1.Condition{
			Type:               "Updating",
			Status:             metav1.ConditionFalse,
			ObservedGeneration: freshFG.Generation,
			LastTransitionTime: now,
			Reason:             "NotUpdating",
			Message:            "No update in progress",
		})
	}

	// RecoveryPending condition.
	if snap.RecoveryState == recoveryStateInProgress {
		setCondition(&freshFG.Status.Conditions, metav1.Condition{
			Type:               "RecoveryPending",
			Status:             metav1.ConditionTrue,
			ObservedGeneration: freshFG.Generation,
			LastTransitionTime: now,
			Reason:             "RecoveryInProgress",
			Message:            fmt.Sprintf("Old primary %s is being reconfigured as a replica", snap.RecoverySite),
		})
	} else if snap.RecoveryState == recoveryStateBlocked {
		msg := fmt.Sprintf("Old primary %s has %d divergent transactions — annotate with bloodraven.shipstream.io/reclone-site=%s to recover", snap.RecoverySite, snap.DivergentTxnCount, snap.RecoverySite)
		if snap.DivergenceDetectedBy == divergenceDetectedByAudit {
			msg = fmt.Sprintf("Replica %s has %d transactions the primary%s lacks — reclone it (kubectl bloodraven reclone %s %s) or make the primary contain them",
				snap.RecoverySite, snap.DivergentTxnCount, comparedToSuffix(snap.DivergenceComparedTo), freshFG.Name, snap.RecoverySite)
		}
		setCondition(&freshFG.Status.Conditions, metav1.Condition{
			Type:               "RecoveryPending",
			Status:             metav1.ConditionTrue,
			ObservedGeneration: freshFG.Generation,
			LastTransitionTime: now,
			Reason:             "DivergentTransactions",
			Message:            msg,
		})
	} else {
		setCondition(&freshFG.Status.Conditions, metav1.Condition{
			Type:               "RecoveryPending",
			Status:             metav1.ConditionFalse,
			ObservedGeneration: freshFG.Generation,
			LastTransitionTime: now,
			Reason:             "NoRecoveryPending",
			Message:            "No old primary recovery pending",
		})
	}

	// Populate status.pitr and PITRArchiveReady from the archiver
	// poller's cache. The poller's own writer (updatePITRArchiveStatus)
	// also sets them after every poll round; both derive them from the
	// poller's current state rather than from a snapshot copy.
	r.applyPITRArchiveStatus(nn, &freshFG)

	// Skip no-op writes to avoid bumping resourceVersion unnecessarily.
	// Keyring promotion Events compare in-memory last-seen, so they must
	// still run here: after an operator restart the CR already carries
	// the overlay Alert and a DeepEqual skip would otherwise stay silent.
	if equality.Semantic.DeepEqual(existingStatus, &freshFG.Status) {
		r.logger.Debug("status unchanged, skipping update", "fg", nn)
		r.emitKeyringPromotionEvents(&freshFG, snap)
		if r.deploymentLeases != nil {
			r.deploymentLeases.confirmTopology(&freshFG, snap.DeploymentEpoch)
		}
		// Unchanged includes the DivergentPromotion condition already
		// carrying this note.
		r.ackDivergentPromotion(nn, snap)
		return nil
	}

	superseded := false
	var pitrArchivePrev *metav1.Condition
	if err := k8sretry.RetryOnConflict(k8sretry.DefaultRetry, func() error {
		// Re-fetch the CR to get the latest resource version.
		var fresh v1alpha1.MysqlFailoverGroup
		if err := r.client.Get(ctx, nn, &fresh); err != nil {
			return err
		}
		pitrArchivePrev = pitrArchiveCondition(&fresh.Status)
		if fresh.Status.TopologyGeneration > snap.TopologyGeneration {
			superseded = true
			r.hydrateDeploymentGeneration(&fresh)
			return nil
		}
		// Apply status changes to the freshly-fetched object.
		//
		// freshFG.Status is a snapshot taken before this function started
		// computing, so assigning it wholesale would carry stale copies of
		// every field this writer does not own back over the top of
		// concurrent writes. The encryption state machine and the
		// DragonflyManager each merge-patch only their own subtree, so
		// those fields are re-read here rather than replayed from the
		// snapshot. Clobbering encryption is not cosmetic: losing an
		// Escrowed/Sealed advance re-renders the site unsealed and rolls
		// the pod, and losing a Failed hides a site whose key custody is
		// gone. Clobbering Dragonfly status drops capability probes and
		// lastPromotion* stamps written on the other goroutine. The
		// post-restore reclone queue is the reconciler's too: a stale copy
		// would undo its phase changes (a succeeded restore's queue
		// written back as Armed is discarded, a Draining one written over
		// a newer Armed one clones from the new restore's primary) and
		// revert an administrator's status patch. Likewise the restore's
		// offline_mode fence record: a copy without the prior value makes
		// the release leave offline_mode ON.
		encryption := fresh.Status.EncryptionAtRest
		reconcilerConds := make([]*metav1.Condition, len(reconcilerOwnedConditions))
		for i, t := range reconcilerOwnedConditions {
			reconcilerConds[i] = apimeta.FindStatusCondition(fresh.Status.Conditions, t)
		}
		dragonfly := fresh.Status.Dragonfly
		planned := fresh.Status.PlannedFailover
		plannedHold := fresh.Status.PlannedFailoverHold
		initialRestore := fresh.Status.Restore
		restore := fresh.Status.RestoreInPlace
		reclones := fresh.Status.RestoreReclones
		offlineFence := fresh.Status.RestoreOfflineFence
		pitr := fresh.Status.PITR
		// Only an administrator removes DivergentPromotion; unless this
		// write records a new note, keep whatever is live so a removal
		// that raced this write is not undone.
		var liveDivergentPromotion *metav1.Condition
		if !newDivergentPromotion {
			if c := apimeta.FindStatusCondition(fresh.Status.Conditions, divergentPromotionCondition); c != nil {
				cp := *c
				liveDivergentPromotion = &cp
			}
		}
		// The failover triple is re-merged over the live one (K058): a
		// newer record committed since freshFG was read (the planned
		// engine's terminal write, at the same topology generation) must
		// not be replaced by freshFG's pre-merged copy.
		live := fresh.Status
		fresh.Status = *freshFG.Status.DeepCopy()
		fresh.Status.LastFailover, fresh.Status.LastFailoverTarget, fresh.Status.PromotionGtidExecuted =
			live.LastFailover, live.LastFailoverTarget, live.PromotionGtidExecuted
		mergeFailoverTriple(&fresh.Status, snap)
		if !newDivergentPromotion {
			if liveDivergentPromotion != nil {
				setCondition(&fresh.Status.Conditions, *liveDivergentPromotion)
			} else {
				removeCondition(&fresh.Status.Conditions, divergentPromotionCondition)
			}
		}
		fresh.Status.PlannedFailover = planned
		fresh.Status.PlannedFailoverHold = plannedHold
		fresh.Status.Restore = initialRestore
		fresh.Status.RestoreInPlace = restore
		fresh.Status.RestoreReclones = reclones
		fresh.Status.RestoreOfflineFence = offlineFence
		fresh.Status.EncryptionAtRest = encryption
		fresh.Status.Dragonfly = dragonfly
		for i, c := range reconcilerConds {
			if c != nil {
				setCondition(&fresh.Status.Conditions, *c)
			} else {
				removeCondition(&fresh.Status.Conditions, reconcilerOwnedConditions[i])
			}
		}
		// status.pitr and PITRArchiveReady are re-derived on the fetched
		// object: the poller's writer may have advanced them since
		// freshFG was read.
		fresh.Status.PITR = pitr
		if pitrArchivePrev != nil {
			setCondition(&fresh.Status.Conditions, *pitrArchivePrev)
		} else {
			removeCondition(&fresh.Status.Conditions, conditionPITRArchiveReady)
		}
		r.applyPITRArchiveStatus(nn, &fresh)
		if err := r.client.Status().Update(ctx, &fresh); err != nil {
			return err
		}
		freshFG = fresh
		return nil
	}); err != nil {
		if apierrors.IsNotFound(err) {
			r.logger.Warn("CR deleted during status update, skipping", "fg", nn)
			return nil
		}
		r.logger.Error("update fg status", "fg", nn, "error", err)
		return err
	}

	// Emit Kubernetes Events only after the status update succeeds,
	// so events are not emitted for transitions that failed to persist.
	if superseded {
		return nil
	}
	r.ackDivergentPromotion(nn, snap)
	if newDivergentPromotion && r.recorder != nil {
		r.recorder.Eventf(&freshFG, corev1.EventTypeWarning, "DivergentSitePromoted", "%s", divergentPromotionMessage(*snap.DivergentPromotion))
	}
	r.emitFailoverEvents(&freshFG, existingStatus, snap)
	r.emitOrderedUpdateStallEvent(&freshFG, existingStatus)
	r.emitPITRArchiveEvents(&freshFG, pitrArchivePrev)
	r.emitKeyringPromotionEvents(&freshFG, snap)
	r.emitDegradedTransitionEvents(&freshFG, nn, snap)
	if r.deploymentLeases != nil {
		r.deploymentLeases.confirmTopology(&freshFG, snap.DeploymentEpoch)
	}
	return nil
}

// MergeTopologySnapshotStatus merges the topology manager's snapshot into
// st: the active site, topologyGeneration, the per-site observations and
// recovery fields, the failover record {lastFailover, lastFailoverTarget,
// promotionGtidExecuted} and updatePhase. It returns false, leaving st
// untouched, when the snapshot is older than the stored topologyGeneration.
// Conditions and the subtrees other writers own are not touched.
func MergeTopologySnapshotStatus(st *v1alpha1.MysqlFailoverGroupStatus, snap TopologySnapshot) bool {
	if snap.TopologyGeneration < st.TopologyGeneration {
		return false
	}

	st.ActiveSite = snap.ActiveSite
	if snap.TopologyGeneration > st.TopologyGeneration && snap.DeploymentActiveSite != "" {
		st.ActiveSite = snap.DeploymentActiveSite
	}
	st.TopologyGeneration = snap.TopologyGeneration
	// Ensure the Sites slice is allocated to match the number of sites.
	if len(st.Sites) != len(snap.Sites) {
		st.Sites = make([]v1alpha1.SiteStatus, len(snap.Sites))
	}
	for i, s := range snap.Sites {
		st.Sites[i] = siteStatusFromSnapshot(s)
	}

	mergeFailoverTriple(st, snap)

	// Write per-site recovery fields. Every site carries its own recovery
	// state and divergence report — multiple sites can be in recovery at
	// once (e.g. two former primaries after consecutive failovers).
	for i, s := range snap.Sites {
		st.Sites[i].RecoveryState = s.RecoveryState
		st.Sites[i].DivergentGtid = s.DivergentGtid
		if s.RecoveryState != "" && s.DivergentTxnCount > 0 {
			c := s.DivergentTxnCount
			st.Sites[i].DivergentTransactionCount = &c
		} else {
			st.Sites[i].DivergentTransactionCount = nil
		}
		st.Sites[i].DivergenceDetectedBy = ""
		st.Sites[i].DivergenceComparedTo = ""
		if s.RecoveryState == recoveryStateBlocked {
			st.Sites[i].DivergenceDetectedBy = s.DivergenceDetectedBy
			st.Sites[i].DivergenceComparedTo = s.DivergenceComparedTo
		}
	}

	st.UpdatePhase = snap.UpdatePhase
	return true
}

// mergeFailoverTriple moves the failover record {lastFailover,
// lastFailoverTarget, promotionGtidExecuted} of st forward to snap's: a
// snapshot whose lastFailover is strictly older (second precision) than
// the stored one leaves the triple alone, and an equal one may change only
// the target (a planned failover naming an unconfirmed target). The status
// write's conflict retry applies it again over the live triple, so a newer
// record committed in between (the planned engine's) is not overwritten.
func mergeFailoverTriple(st *v1alpha1.MysqlFailoverGroupStatus, snap TopologySnapshot) {
	snapAt := snap.LastFailover.UTC().Truncate(time.Second)
	var storedAt time.Time
	if st.LastFailover != nil {
		storedAt = st.LastFailover.UTC().Truncate(time.Second)
	}
	if !snapAt.IsZero() && failoverStampFutureDated(storedAt, snap.ObservedAt) {
		// A stored record implausibly ahead of the writer's clock is no
		// record (K023): restore ignores it, so it must not block this one.
		storedAt = time.Time{}
	}
	switch {
	case snapAt.After(storedAt):
		t := metav1.NewTime(snap.LastFailover)
		st.LastFailover = &t
		st.LastFailoverTarget = snap.LastFailoverTarget
		st.PromotionGtidExecuted = snap.PromotionGtidExecuted
	case snapAt.Equal(storedAt):
		st.LastFailoverTarget = snap.LastFailoverTarget
	}
}

// MergePlannedAuthority merges a manager's authority into st with
// MergeTopologySnapshotStatus's rule for status.activeSite and
// status.topologyGeneration: never from an older generation, and from a
// newer one the deployment's active site when it names one. It returns
// false, touching nothing, for an older generation.
func MergePlannedAuthority(st *v1alpha1.MysqlFailoverGroupStatus, a PlannedAuthority) bool {
	if a.TopologyGeneration < st.TopologyGeneration {
		return false
	}
	st.ActiveSite = a.ActiveSite
	if a.TopologyGeneration > st.TopologyGeneration && a.DeploymentActiveSite != "" {
		st.ActiveSite = a.DeploymentActiveSite
	}
	st.TopologyGeneration = a.TopologyGeneration
	return true
}

func validSnapshotActiveSite(snap TopologySnapshot) string {
	if snap.ActiveSite != "" {
		for _, site := range snap.Sites {
			if site.Name == snap.ActiveSite && site.State == state.StateWritable && site.Role != state.SiteRoleDROnly && site.Role != state.SiteRoleReadOnly {
				return snap.ActiveSite
			}
		}
		return ""
	}
	active := ""
	for _, site := range snap.Sites {
		if site.State != state.StateWritable {
			continue
		}
		if active != "" || site.Role == state.SiteRoleDROnly || site.Role == state.SiteRoleReadOnly {
			return ""
		}
		active = site.Name
	}
	return active
}

// populatePITRStatus fills freshFG.Status.PITR from the cached
// per-site archiver snapshots. Aggregates across sites by taking the
// oldest first-event time, newest last-event time, maximum file
// count/bytes, and latest upload timestamp — values the user cares
// about are coverage bounds, not per-site breakdowns.
//
// A site whose latest poll failed contributes its last successful
// snapshot, so coverage stays last-known rather than dropping that site.
// While a site has not answered at all in this process (after an
// operator restart, say), the persisted count, bytes and oldest time are
// kept, since aggregating the other sites alone would understate them,
// while the newest and last archived times still advance with the
// answering sites. The message names the sites
// either way. Clears the field when PITR is disabled in spec, so a
// disabled flip doesn't keep showing bounds from a previous
// configuration. Before the first poll round the field is left alone.
func (r *TopologyManagerRunner) populatePITRStatus(nn types.NamespacedName, fg *v1alpha1.MysqlFailoverGroup) {
	if !pitrEnabled(fg) {
		fg.Status.PITR = nil
		return
	}

	r.mu.RLock()
	mt, ok := r.managers[nn]
	r.mu.RUnlock()
	if !ok || mt.archiver == nil {
		return
	}

	out := &v1alpha1.PITRStatus{
		Enabled:     true,
		ProfileName: fg.Spec.Backup.PITR.ProfileName,
	}
	// Iterate in site-name order so the derived Message (and any future
	// order-sensitive field) is reproducible across reconciles — map
	// iteration in Go is randomized, which would otherwise flap the
	// CR status on every poll when multiple sites report LastError.
	snaps, lastGood, pollErrors := mt.archiver.view()
	sites := mt.archiver.siteNames()

	var (
		haveAny   bool
		lastKnown []string
		missing   []string
		errorMsgs []string
	)
	for _, name := range sites {
		s := snaps[name]
		if s == nil {
			if perr, failed := pollErrors[name]; failed {
				errorMsgs = append(errorMsgs, fmt.Sprintf("%s: archiver status unavailable: %s", name, perr))
			}
			// The last good snapshot's lastError is not current, so
			// only its coverage is used.
			s = lastGood[name]
			if s == nil {
				missing = append(missing, name)
				continue
			}
			lastKnown = append(lastKnown, name)
		} else if s.LastError != "" {
			errorMsgs = append(errorMsgs, fmt.Sprintf("%s: %s", name, s.LastError))
		}
		haveAny = true
		// ArchivedFileCount and ArchivedBytes sum across sites: each
		// site maintains its own manifest for the binlogs produced
		// while it was primary, and those sets don't overlap. So
		// "files held in storage across all sites" is the sum.
		out.ArchivedFileCount += int32(s.ManifestFileCount)
		out.ArchivedBytes += s.ManifestBytes
		if !s.OldestArchivedTime.IsZero() &&
			(out.OldestArchivedTime == nil || s.OldestArchivedTime.Before(out.OldestArchivedTime.Time)) {
			t := metav1.NewTime(s.OldestArchivedTime)
			out.OldestArchivedTime = &t
		}
		if !s.NewestArchivedTime.IsZero() &&
			(out.NewestArchivedTime == nil || s.NewestArchivedTime.After(out.NewestArchivedTime.Time)) {
			t := metav1.NewTime(s.NewestArchivedTime)
			out.NewestArchivedTime = &t
		}
		if !s.LastUploadAt.IsZero() &&
			(out.LastArchivedTime == nil || s.LastUploadAt.After(out.LastArchivedTime.Time)) {
			t := metav1.NewTime(s.LastUploadAt)
			out.LastArchivedTime = &t
		}
	}
	if !haveAny && len(pollErrors) == 0 {
		// Nothing observed in this process yet.
		return
	}
	switch {
	case len(missing) > 0 && fg.Status.PITR != nil:
		// Counts, bytes and the oldest time cannot be split per site, so
		// they keep their recorded values. The newest and last archived
		// times are maxima: the answering sites can only move them
		// forward, and a stale value would read as a stopped archive.
		kept := fg.Status.PITR.DeepCopy()
		kept.Enabled, kept.ProfileName = true, fg.Spec.Backup.PITR.ProfileName
		if out.NewestArchivedTime != nil && (kept.NewestArchivedTime == nil || out.NewestArchivedTime.After(kept.NewestArchivedTime.Time)) {
			kept.NewestArchivedTime = out.NewestArchivedTime
		}
		if out.LastArchivedTime != nil && (kept.LastArchivedTime == nil || out.LastArchivedTime.After(kept.LastArchivedTime.Time)) {
			kept.LastArchivedTime = out.LastArchivedTime
		}
		out = kept
		errorMsgs = append(errorMsgs, fmt.Sprintf("file count, bytes and oldest archived time keep their last recorded values until every site answers (no status yet from: %s)", strings.Join(missing, ", ")))
	default:
		if len(missing) > 0 {
			errorMsgs = append(errorMsgs, fmt.Sprintf("coverage fields exclude: %s (no status yet)", strings.Join(missing, ", ")))
		}
		if len(lastKnown) > 0 {
			errorMsgs = append(errorMsgs, fmt.Sprintf("coverage fields are last-known for: %s", strings.Join(lastKnown, ", ")))
		}
	}
	// Join all per-site errors so nothing is silently dropped. Empty
	// when every site is clean.
	out.Message = strings.Join(errorMsgs, "; ")
	fg.Status.PITR = out
}

// emitFailoverEvents fires Kubernetes Events when significant failover or
// recovery state transitions occur. It compares the existing persisted status
// against the snapshot being written to detect one-shot transitions.
func (r *TopologyManagerRunner) emitFailoverEvents(fg *v1alpha1.MysqlFailoverGroup, existingStatus *v1alpha1.MysqlFailoverGroupStatus, snap TopologySnapshot) {
	if r.recorder == nil {
		return
	}

	// Failover executed: new failover target differs from previously persisted target.
	if snap.LastFailoverTarget != "" && snap.LastFailoverTarget != existingStatus.LastFailoverTarget {
		r.recorder.Eventf(fg, corev1.EventTypeNormal, "FailoverExecuted",
			"Failover completed: %s promoted as new primary", snap.LastFailoverTarget)
	}

	// Per-site recovery transitions: compare the previously persisted state
	// against the new snapshot for each site independently, since multiple
	// sites can be in recovery at once.
	oldState := make(map[string]string, len(existingStatus.Sites))
	oldDivergentCount := make(map[string]int64, len(existingStatus.Sites))
	oldDetectedBy := make(map[string]string, len(existingStatus.Sites))
	oldSet := make(map[string]string, len(existingStatus.Sites))
	oldComparedTo := make(map[string]string, len(existingStatus.Sites))
	for _, s := range existingStatus.Sites {
		oldState[s.Name] = s.RecoveryState
		oldDetectedBy[s.Name] = s.DivergenceDetectedBy
		oldSet[s.Name], oldComparedTo[s.Name] = s.DivergentGtid, s.DivergenceComparedTo
		if s.DivergentTransactionCount != nil {
			oldDivergentCount[s.Name] = *s.DivergentTransactionCount
		}
	}
	recoveryActive := func(st string) bool {
		return st == recoveryStateInProgress || st == recoveryStateBlocked
	}
	for _, s := range snap.Sites {
		// Data loss detected: RecoveryBlocked appeared where it wasn't before,
		// or the periodic re-verification found the divergent set has CHANGED
		// while the site stayed blocked (it respawned writable, took writes,
		// and was re-fenced). Re-emitting on a changed count keeps the Event
		// stream in step with status.divergentTransactionCount — an admin who
		// extracts only the first-reported set would lose the rest. An
		// unchanged count is not re-emitted, and a status that never recorded
		// a count is treated as unchanged rather than as a new report.
		countChanged := false
		if prev, ok := oldDivergentCount[s.Name]; ok && prev != s.DivergentTxnCount {
			countChanged = true
		}
		// The same count can hide a different set (one extra reconciled
		// while another appeared) or a comparison against another primary;
		// both are new evidence. A field the old status never recorded
		// (written before provenance existed) is not.
		evidenceChanged := oldState[s.Name] == recoveryStateBlocked &&
			((oldSet[s.Name] != "" && oldSet[s.Name] != s.DivergentGtid) ||
				(oldComparedTo[s.Name] != "" && s.DivergenceComparedTo != "" && oldComparedTo[s.Name] != s.DivergenceComparedTo))
		if s.RecoveryState == recoveryStateBlocked &&
			(oldState[s.Name] != recoveryStateBlocked || countChanged || evidenceChanged) {
			if s.DivergenceDetectedBy == divergenceDetectedByAudit {
				// A replica, usually replicating normally, holds history
				// the primary lacks; "did not replicate before failover"
				// would send the reader to the wrong playbook.
				r.recorder.Eventf(fg, corev1.EventTypeWarning, "DivergentReplicaDetected",
					"%d transactions on %s are missing from the primary%s (%s); ordered updates and planned failovers will not promote it. Reclone it or make the primary contain them",
					s.DivergentTxnCount, s.Name, comparedToSuffix(s.DivergenceComparedTo), truncateGtidHint(s.DivergentGtid))
			} else {
				r.recorder.Eventf(fg, corev1.EventTypeWarning, "DataLossDetected",
					"%d divergent transactions on %s did not replicate before failover",
					s.DivergentTxnCount, s.Name)
			}
		}
		// Recovery complete: RecoveryInProgress/RecoveryBlocked cleared. A
		// marker cleared because the site became WRITABLE (promotion or
		// re-assert) is not a recovery completion — the site did not rejoin
		// as a replica — so no Normal event is emitted for it.
		if recoveryActive(oldState[s.Name]) && !recoveryActive(s.RecoveryState) && s.State != state.StateWritable {
			if oldState[s.Name] == recoveryStateBlocked && oldDetectedBy[s.Name] == divergenceDetectedByAudit {
				r.recorder.Eventf(fg, corev1.EventTypeNormal, "DivergenceResolved",
					"Replica %s no longer holds transactions the primary lacks", s.Name)
			} else {
				r.recorder.Eventf(fg, corev1.EventTypeNormal, "RecoveryComplete",
					"Old primary %s recovered and is now replicating", s.Name)
			}
		}
	}
}

// ReasonOrderedUpdateStalled is the Updating condition reason, and the
// Warning Event reason, for an ordered update stopped on a site whose pod
// is not Ready.
const ReasonOrderedUpdateStalled = "OrderedUpdateStalled"

func orderedUpdateStalledMessage(stall string) string {
	return fmt.Sprintf("Ordered update stalled: %s. No other site is rolled and the active site is not handed off until that pod has been Ready for %s; the update retries every %s",
		stall, podStableFor, orderedUpdateStallRetry)
}

// emitOrderedUpdateStallEvent fires a Warning Event when the Updating
// condition just written reports a stall that the previous status did not:
// a new stall, or one with a different cause. A retry that finds the same
// cause writes nothing and fires nothing. The pod-readiness reasons are
// worded to stay the same while the cause does (a crash loop reads the same
// in every phase of the loop), which is what keeps this to one Event per
// stall.
func (r *TopologyManagerRunner) emitOrderedUpdateStallEvent(fg *v1alpha1.MysqlFailoverGroup, existingStatus *v1alpha1.MysqlFailoverGroupStatus) {
	if r.recorder == nil {
		return
	}
	cur := apimeta.FindStatusCondition(fg.Status.Conditions, "Updating")
	if cur == nil || cur.Reason != ReasonOrderedUpdateStalled {
		return
	}
	if prev := apimeta.FindStatusCondition(existingStatus.Conditions, "Updating"); prev != nil && prev.Reason == cur.Reason && prev.Message == cur.Message {
		return
	}
	r.recorder.Eventf(fg, corev1.EventTypeWarning, ReasonOrderedUpdateStalled, "%s", cur.Message)
}

// emitKeyringPromotionEvents fires Warning Events when the promotion
// path skipped or refused a mid-rotation site. Comparison is against
// in-memory last-seen on managedTopology so a process restart re-fires
// once. Metric increments live in applyCrossSiteAction (actual decision),
// not here (Event reminder).
func (r *TopologyManagerRunner) emitKeyringPromotionEvents(fg *v1alpha1.MysqlFailoverGroup, snap TopologySnapshot) {
	if r.recorder == nil {
		return
	}
	nn := FailoverGroupNamespacedName(fg)
	r.mu.RLock()
	mt, ok := r.managers[nn]
	r.mu.RUnlock()
	if !ok {
		return
	}
	if !stringSlicesEqual(snap.KeyringPromotionSkipped, mt.lastKeyringPromotionSkipped) {
		for _, site := range snap.KeyringPromotionSkipped {
			r.recorder.Eventf(fg, corev1.EventTypeWarning, "KeyringPromotionSkipped",
				"skipping promotion candidate %s: site is mid-keyring-rotation (UnsealReason=Rotation)", site)
		}
		mt.lastKeyringPromotionSkipped = append([]string(nil), snap.KeyringPromotionSkipped...)
	}
	if !stringSlicesEqual(snap.KeyringPromotionRefused, mt.lastKeyringPromotionRefused) {
		if len(snap.KeyringPromotionRefused) > 0 {
			r.recorder.Eventf(fg, corev1.EventTypeWarning, "KeyringPromotionRefused",
				"promotion refused: %s mid-keyring-rotation (UnsealReason=Rotation); finish the rotation before this site can be promoted",
				strings.Join(snap.KeyringPromotionRefused, ", "))
		}
		mt.lastKeyringPromotionRefused = append([]string(nil), snap.KeyringPromotionRefused...)
	}
	if !stringSlicesEqual(snap.RestoreReclonePromotionRefused, mt.lastRestoreReclonePromotionRefused) {
		if len(snap.RestoreReclonePromotionRefused) > 0 {
			r.recorder.Eventf(fg, corev1.EventTypeWarning, "PromotionRefusedPendingReclone",
				"automatic promotion refused: %s not yet recloned after the in-place restore and hold pre-restore data; "+
					"bring the primary back, or accept that data by removing status.restoreReclones "+
					"(kubectl patch mfg <group> --subresource=status --type=json -p '[{\"op\":\"remove\",\"path\":\"/status/restoreReclones\"}]')",
				strings.Join(snap.RestoreReclonePromotionRefused, ", "))
		}
		mt.lastRestoreReclonePromotionRefused = append([]string(nil), snap.RestoreReclonePromotionRefused...)
	}
	if snap.BootstrapRefusedRestoreHold != mt.lastBootstrapRefusedRestoreHold {
		if snap.BootstrapRefusedRestoreHold != "" {
			r.recorder.Eventf(fg, corev1.EventTypeWarning, "BootstrapRefusedRestoreHold",
				"refused to clone into %s: an in-place restore holds it (status.restoreReclones is Armed or Held, or the topology is frozen); "+
					"its data and stopped replication are kept until the restore queue is resolved",
				snap.BootstrapRefusedRestoreHold)
		}
		mt.lastBootstrapRefusedRestoreHold = snap.BootstrapRefusedRestoreHold
	}
}

// emitDegradedTransitionEvents fires Kubernetes Events when the topology-level
// Degraded reason transitions (e.g. Healthy → SplitBrain, TotalLoss → Healthy).
// It tracks the last topology reason on the managedTopology struct rather than
// reading from the persisted Degraded condition, which is shared with replication
// reasons and could produce false transitions.
func (r *TopologyManagerRunner) emitDegradedTransitionEvents(fg *v1alpha1.MysqlFailoverGroup, nn types.NamespacedName, snap TopologySnapshot) {
	if r.recorder == nil {
		return
	}

	newReason := degradedReason(snap)
	if newReason == "Initializing" {
		// Not a topology verdict: neither raise an event nor record it, so
		// the first real verdict compares against what came before (no
		// SiteRecovered for a group that was never reported degraded).
		return
	}

	r.mu.RLock()
	mt, ok := r.managers[nn]
	r.mu.RUnlock()
	if !ok {
		return
	}

	oldReason := mt.lastTopologyDegradedReason
	if oldReason == newReason {
		return
	}

	switch newReason {
	case "SplitBrain":
		r.recorder.Eventf(fg, corev1.EventTypeWarning, "SplitBrainDetected",
			"Both sites are writable: %s", snap.Alert)
	case "NoPrimary":
		if strings.Contains(snap.Alert, "UnsealReason=Rotation") || strings.Contains(snap.Alert, "pending post-restore reclone") {
			// KeyringPromotionRefused / PromotionRefusedPendingReclone is
			// the specific Event; the generic "Both sites are read-only"
			// wording is wrong for this shape (usually one unreachable
			// primary + one rotating or not-yet-recloned replica).
			break
		}
		r.recorder.Eventf(fg, corev1.EventTypeWarning, "NoPrimaryDetected",
			"Both sites are read-only: %s", snap.Alert)
	case "TotalLoss":
		r.recorder.Eventf(fg, corev1.EventTypeWarning, "TotalLossDetected",
			"Both sites are unreachable: %s", snap.Alert)
	case "Healthy":
		if oldReason != "" {
			r.recorder.Event(fg, corev1.EventTypeNormal, "SiteRecovered",
				"Degraded condition cleared, topology is healthy")
		}
	}

	mt.lastTopologyDegradedReason = newReason
}

// degradedReason returns the Degraded condition reason for a topology
// snapshot. Prefers the pre-computed DegradedReason from the state
// machine; falls back to inspecting aggregate site states when the
// snapshot is a partial update (e.g. bootstrap-only).
func degradedReason(snap TopologySnapshot) string {
	if snap.DegradedReason != "" {
		return snap.DegradedReason
	}
	if snap.Alert == "" {
		return "Healthy"
	}
	var writable, readOnly, unreachable, coreSites int
	for _, s := range snap.Sites {
		if s.Role == state.SiteRoleReadOnly {
			continue
		}
		coreSites++
		switch s.State {
		case state.StateWritable:
			writable++
		case state.StateReadOnly:
			readOnly++
		case state.StateUnreachable:
			unreachable++
		}
	}
	if coreSites == 0 {
		return "Alert"
	}
	switch {
	case writable > 1:
		return "SplitBrain"
	case unreachable == coreSites:
		return "TotalLoss"
	case writable == 0 && readOnly > 0:
		return "NoPrimary"
	}
	return "Alert"
}

// initializingMessage names the primary-candidates not yet classified since
// the operator started, for the Initializing Ready and Degraded conditions.
func initializingMessage(snap TopologySnapshot) string {
	var parts []string
	for _, s := range snap.Sites {
		// Same filter as EvalCrossSite's unclassified set.
		if s.State != state.StateUnknown || s.Role != state.SiteRolePrimaryCandidate || s.ClassificationOverdue {
			continue
		}
		if s.PendingWritable {
			parts = append(parts, s.Name+" answered writable and awaits recoveryThreshold confirmation")
		} else {
			parts = append(parts, s.Name+" has not answered a probe yet")
		}
	}
	if len(parts) == 0 {
		return "Classifying sites after operator start"
	}
	return "Classifying sites after operator start: " + strings.Join(parts, "; ")
}

// divergentPromotionCondition records that a site holding transactions its
// former primary lacked was promoted. Set by the operator, removed only by
// an administrator.
const divergentPromotionCondition = "DivergentPromotion"

// divergenceDegradedMessage describes every site carrying a divergence
// block, or returns "" when none does.
func divergenceDegradedMessage(snap TopologySnapshot) string {
	var parts []string
	for _, s := range snap.Sites {
		if s.RecoveryState != recoveryStateBlocked {
			continue
		}
		parts = append(parts, fmt.Sprintf("%s holds %d transactions the primary%s lacks (%s)",
			s.Name, s.DivergentTxnCount, comparedToSuffix(s.DivergenceComparedTo), truncateGtidHint(s.DivergentGtid)))
	}
	if len(parts) == 0 {
		return ""
	}
	return strings.Join(parts, "; ") +
		". Replication may still be running. Ordered updates and planned failovers will not promote it: reclone it (kubectl bloodraven reclone) to discard the transactions, or make the primary contain them to keep them."
}

func divergentPromotionMessage(note DivergentPromotion) string {
	return fmt.Sprintf("%s was promoted while holding %d transactions the primary%s lacked (%s); they are now part of the primary's history. Review them, then remove this condition",
		note.Site, note.DivergentTransactions, comparedToSuffix(note.ComparedTo), truncateGtidSet(note.DivergentGtid))
}

// maxDivergentPromotionRecords bounds the DivergentPromotion condition:
// the newest records are kept, and the Event stream and operator log hold
// every one.
const maxDivergentPromotionRecords = 5

const divergentPromotionOmitted = "(earlier divergent promotions omitted; see DivergentSitePromoted Events and the operator log)"

// appendDivergentPromotion adds record to the newline-separated records in
// existing, keeping the newest maxDivergentPromotionRecords.
func appendDivergentPromotion(existing, record string) string {
	var records []string
	for _, line := range strings.Split(existing, "\n") {
		if line != "" && line != divergentPromotionOmitted {
			records = append(records, line)
		}
	}
	omitted := strings.Contains(existing, divergentPromotionOmitted)
	records = append(records, record)
	if len(records) > maxDivergentPromotionRecords {
		records = records[len(records)-maxDivergentPromotionRecords:]
		omitted = true
	}
	if omitted {
		records = append([]string{divergentPromotionOmitted}, records...)
	}
	return strings.Join(records, "\n")
}

func comparedToSuffix(site string) string {
	if site == "" {
		return ""
	}
	return " " + site
}

// ackDivergentPromotion tells the manager its pending divergent-promotion
// note is persisted.
func (r *TopologyManagerRunner) ackDivergentPromotion(nn types.NamespacedName, snap TopologySnapshot) {
	if snap.DivergentPromotion == nil {
		return
	}
	r.mu.RLock()
	mt := r.managers[nn]
	r.mu.RUnlock()
	if mt != nil && mt.tm != nil {
		mt.tm.AckDivergentPromotion(*snap.DivergentPromotion)
	}
}

// setBootstrappingCondition writes the Bootstrapping condition based on the
// current phase. It is shared between the full-status writer (updateCRStatus)
// and the dedicated bootstrap-only writer (updateBootstrappingCondition) so
// they stay in sync.
func setBootstrappingCondition(conditions *[]metav1.Condition, generation int64, now metav1.Time, phase, errMsg, source string) {
	label := "Bootstrap"
	switch source {
	case "reclone":
		label = "Reclone"
	case "auto-clone":
		label = "Auto-clone"
	}

	switch phase {
	case "":
		setCondition(conditions, metav1.Condition{
			Type:               "Bootstrapping",
			Status:             metav1.ConditionFalse,
			ObservedGeneration: generation,
			LastTransitionTime: now,
			Reason:             "NotBootstrapping",
			Message:            "No bootstrap in progress",
		})
	case string(BootstrapPhaseDone):
		setCondition(conditions, metav1.Condition{
			Type:               "Bootstrapping",
			Status:             metav1.ConditionFalse,
			ObservedGeneration: generation,
			LastTransitionTime: now,
			Reason:             string(BootstrapPhaseDone),
			Message:            fmt.Sprintf("%s completed successfully", label),
		})
	case string(BootstrapPhaseFailed):
		msg := fmt.Sprintf("%s failed", label)
		if errMsg != "" {
			msg = fmt.Sprintf("%s failed: %s", label, errMsg)
		}
		setCondition(conditions, metav1.Condition{
			Type:               "Bootstrapping",
			Status:             metav1.ConditionTrue,
			ObservedGeneration: generation,
			LastTransitionTime: now,
			Reason:             "Failed",
			Message:            msg,
		})
	default:
		setCondition(conditions, metav1.Condition{
			Type:               "Bootstrapping",
			Status:             metav1.ConditionTrue,
			ObservedGeneration: generation,
			LastTransitionTime: now,
			Reason:             phase,
			Message:            fmt.Sprintf("%s in progress: %s", label, phase),
		})
	}
}

// updateBootstrappingCondition updates ONLY the Bootstrapping condition on the
// CR. It is invoked from the bootstrap goroutine on phase transitions; using a
// dedicated path (rather than updateCRStatus with a partially populated
// TopologySnapshot) prevents inadvertently clearing unrelated conditions.
func (r *TopologyManagerRunner) updateBootstrappingCondition(ctx context.Context, nn types.NamespacedName, phase, errMsg, source string) {
	err := k8sretry.RetryOnConflict(k8sretry.DefaultRetry, func() error {
		var fresh v1alpha1.MysqlFailoverGroup
		if err := r.client.Get(ctx, nn, &fresh); err != nil {
			return err
		}
		before := fresh.Status.DeepCopy()
		setBootstrappingCondition(&fresh.Status.Conditions, fresh.Generation, metav1.Now(), phase, errMsg, source)
		if equality.Semantic.DeepEqual(before, &fresh.Status) {
			return nil
		}
		return r.client.Status().Update(ctx, &fresh)
	})
	if err != nil {
		if apierrors.IsNotFound(err) {
			r.logger.Warn("CR deleted during bootstrapping condition update, skipping", "fg", nn)
			return
		}
		r.logger.Error("update bootstrapping condition", "fg", nn, "error", err)
	}
}

func siteStatusFromSnapshot(s SiteSnapshot) v1alpha1.SiteStatus {
	status := v1alpha1.SiteStatus{
		Name:                    s.Name,
		State:                   s.State.String(),
		SourceHost:              s.SourceHost,
		SourceConvergenceState:  v1alpha1.SourceConvergenceState(s.SourceConvergenceState),
		SourceConvergenceReason: s.SourceConvergenceReason,
	}
	if !s.LastSeen.IsZero() {
		t := metav1.NewTime(s.LastSeen)
		status.LastSeen = &t
	}
	if s.Replication != nil {
		status.Replicating = s.ReplicationHealthy
		status.SecondsBehindSource = s.Replication.SecondsBehindSource
		status.GtidExecuted = s.Replication.ExecutedGtidSet
	}
	return status
}

func conditionBool(v bool) metav1.ConditionStatus {
	if v {
		return metav1.ConditionTrue
	}
	return metav1.ConditionFalse
}

// reconcilerOwnedConditions are the condition types the group reconciler
// writes. updateCRStatus re-reads them instead of replaying its snapshot's
// copy, which would undo a concurrent reconciler write.
// AutomaticFailoverHeld is the planned-failover automation hold: a runner
// status write must never drop it.
var reconcilerOwnedConditions = []string{conditionEncryptionReady, conditionS3CredentialsReady, v1alpha1.ConditionAutomaticFailoverHeld}

// setCondition sets or updates a condition in the slice, preserving LastTransitionTime
// if the status has not changed.
func setCondition(conditions *[]metav1.Condition, c metav1.Condition) {
	for i, existing := range *conditions {
		if existing.Type == c.Type {
			if existing.Status == c.Status {
				c.LastTransitionTime = existing.LastTransitionTime
			}
			(*conditions)[i] = c
			return
		}
	}
	*conditions = append(*conditions, c)
}

// removeCondition drops a condition type from the slice. Used when a
// writer that does not own a condition has to restore "absent" rather
// than replay a stale copy of it.
func removeCondition(conditions *[]metav1.Condition, condType string) {
	for i := range *conditions {
		if (*conditions)[i].Type == condType {
			*conditions = append((*conditions)[:i], (*conditions)[i+1:]...)
			return
		}
	}
}

// buildSiteDSN takes a base DSN and replaces the host with the site
// service endpoint. tlsConfigName, when non-empty, names a MySQL driver
// TLS config registered for the site's service host.
//
// The TLS config is only stamped on when the supplied DSN does not
// already choose one: this is the legacy spec.secretName mode where the
// DSN is user-authored, so an explicit `tls=` parameter is a deliberate
// choice and wins. Without this, a group that turns on spec.tls — which
// sets require_secure_transport=ON — would have every site probe fail
// with Error 3159, and no failover could ever run.
func buildSiteDSN(baseDSN string, fg *v1alpha1.MysqlFailoverGroup, site v1alpha1.SiteSpec, tlsConfigName string) (string, error) {
	parsed, err := internalmysql.ParseDSN(baseDSN)
	if err != nil {
		return "", fmt.Errorf("parse DSN: %w", err)
	}
	parsed.Addr = fmt.Sprintf("%s:%d", internalSiteServiceHost(fg.Name, site.Name, fg.Namespace), mysqlPort)
	if tlsConfigName != "" && parsed.TLSConfig == "" {
		parsed.TLSConfig = tlsConfigName
	}
	return parsed.FormatDSN(), nil
}

// buildSiteDSNFromCreds constructs a DSN from username/password credentials
// and the site service endpoint (used in credentials mode). tlsConfigName, when
// non-empty, names a MySQL driver TLS config registered for the site's service host.
func buildSiteDSNFromCreds(username, password string, fg *v1alpha1.MysqlFailoverGroup, site v1alpha1.SiteSpec, tlsConfigName string) string {
	cfg := mysql.NewConfig()
	cfg.User = username
	cfg.Passwd = password
	cfg.Net = "tcp"
	cfg.Addr = fmt.Sprintf("%s:%d", internalSiteServiceHost(fg.Name, site.Name, fg.Namespace), mysqlPort)
	cfg.Timeout = 5 * time.Second
	if tlsConfigName != "" {
		cfg.TLSConfig = tlsConfigName
	}
	return cfg.FormatDSN()
}
