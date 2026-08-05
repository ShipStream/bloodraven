package controller

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"log/slog"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/go-sql-driver/mysql"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/equality"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/tools/record"
	k8sretry "k8s.io/client-go/util/retry"
	"sigs.k8s.io/controller-runtime/pkg/client"

	v1alpha1 "github.com/shipstream/bloodraven/api/v1alpha1"
	internalmysql "github.com/shipstream/bloodraven/internal/mysql"
	"github.com/shipstream/bloodraven/internal/platform"
	"github.com/shipstream/bloodraven/internal/state"
)

// managedTopology tracks a running TopologyManager and its cancellation.
type managedTopology struct {
	tm     *TopologyManager
	cancel context.CancelFunc
	cfg    TopologyConfig

	// archiver polls each sidecar's /archiver/status endpoint and caches
	// the latest snapshot for updateCRStatus to copy into status.pitr.
	// nil only if the runner hasn't finished wiring yet.
	archiver *archiverPoller

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
}

// DeploymentReconciler is the subset of the reconciler that the runner needs
// for ordered updates — specifically, reconciling a single site's Deployment.
type DeploymentReconciler interface {
	ReconcileSiteDeployment(ctx context.Context, fgName types.NamespacedName, siteName string) error
}

// TopologyManagerRunner manages TopologyManager instances for all MysqlFailoverGroup resources.
// It implements manager.Runnable and runs only on the leader-elected instance.
type TopologyManagerRunner struct {
	client    client.Client
	clientset kubernetes.Interface
	hub       *platform.Hub
	recorder  record.EventRecorder
	logger    *slog.Logger

	// deployReconciler is set after the reconciler is created (circular dependency).
	// Used by the ordered update callback to reconcile a single site's Deployment.
	deployReconciler DeploymentReconciler

	mu       sync.RWMutex
	managers map[types.NamespacedName]*managedTopology
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

// HasManager reports whether a topology manager is currently running for the
// given CR. The reconciler uses this to decide whether Deployment updates can
// be safely deferred to the ordered update path; when no manager exists (fresh
// deploy, or operator just started), the reconciler must apply updates itself.
func (r *TopologyManagerRunner) HasManager(nn types.NamespacedName) bool {
	r.mu.RLock()
	defer r.mu.RUnlock()
	_, ok := r.managers[nn]
	return ok
}

// SetTopologyFrozen flips the topology-freeze flag on the managed
// TopologyManager for the given CR, immediately (without waiting for
// the runner's 30-second re-sync tick). The reconciler calls this at
// every in-place restore phase transition so there is no window where
// the topology manager could fire a cross-site action against a
// cluster that is actively being restored.
//
// Returns true when the flag was applied; false when no manager is
// running for this CR (for example during fresh deploy, operator
// restart, or CR deletion). A return of false is expected in those
// cases and is not an error — the runner's next re-sync will observe
// status.restoreInPlace and apply the correct flag when it starts the
// manager.
func (r *TopologyManagerRunner) SetTopologyFrozen(nn types.NamespacedName, frozen bool) bool {
	r.mu.RLock()
	mt, ok := r.managers[nn]
	r.mu.RUnlock()
	if !ok {
		return false
	}
	mt.tm.SetTopologyFrozen(frozen)
	return true
}

// SetPlannedFailoverActive toggles the planned-failover guard on the
// managed TopologyManager so the automatic cross-site evaluator stands
// down while the reconciler drives its own fence/promote sequence.
// Returns true when the flag was applied; false when no manager is
// running for this CR. A false return is safe — see the comment on
// SetTopologyFrozen.
func (r *TopologyManagerRunner) SetPlannedFailoverActive(nn types.NamespacedName, active bool) bool {
	r.mu.RLock()
	mt, ok := r.managers[nn]
	r.mu.RUnlock()
	if !ok {
		return false
	}
	mt.tm.SetPlannedFailoverActive(active)
	if mt.dragonfly != nil {
		mt.dragonfly.SetPaused(active)
	}
	return true
}

// plannedFailoverManager returns the managed TopologyManager for the
// given CR, or an error when no manager is running. The caller must not
// cache the returned pointer; a reconfiguration can replace the manager
// at any time.
func (r *TopologyManagerRunner) plannedFailoverManager(nn types.NamespacedName) (*TopologyManager, error) {
	r.mu.RLock()
	mt, ok := r.managers[nn]
	r.mu.RUnlock()
	if !ok {
		return nil, fmt.Errorf("planned-failover: no topology manager running for %s", nn)
	}
	return mt.tm, nil
}

func (r *TopologyManagerRunner) dragonflyManager(nn types.NamespacedName) *DragonflyManager {
	r.mu.RLock()
	defer r.mu.RUnlock()
	if mt, ok := r.managers[nn]; ok {
		return mt.dragonfly
	}
	return nil
}

// PlannedFailoverFence applies super_read_only=ON on the named site
// and returns its GTID_EXECUTED after the fence takes effect.
func (r *TopologyManagerRunner) PlannedFailoverFence(ctx context.Context, nn types.NamespacedName, site string) (string, error) {
	tm, err := r.plannedFailoverManager(nn)
	if err != nil {
		return "", err
	}
	return tm.FenceSite(ctx, site)
}

// PlannedFailoverUnfence clears super_read_only on the named site.
// Used by the rollback path when the target fails to catch up.
func (r *TopologyManagerRunner) PlannedFailoverUnfence(ctx context.Context, nn types.NamespacedName, site string) error {
	tm, err := r.plannedFailoverManager(nn)
	if err != nil {
		return err
	}
	return tm.UnfenceSite(ctx, site)
}

// PlannedFailoverGtidExecuted returns the named site's current
// GTID_EXECUTED. Used to poll the target during the zero-lag gate.
func (r *TopologyManagerRunner) PlannedFailoverGtidExecuted(ctx context.Context, nn types.NamespacedName, site string) (string, error) {
	tm, err := r.plannedFailoverManager(nn)
	if err != nil {
		return "", err
	}
	return tm.GetSiteGtidExecuted(ctx, site)
}

// PlannedFailoverDrainConnections kills non-replication connections on
// the named site and returns the count killed. Polled by the Draining
// state until it returns zero (clean drain) or the drainTimeout budget
// elapses.
func (r *TopologyManagerRunner) PlannedFailoverDrainConnections(ctx context.Context, nn types.NamespacedName, site string) (int, error) {
	tm, err := r.plannedFailoverManager(nn)
	if err != nil {
		return 0, err
	}
	return tm.KillSiteAppConnections(ctx, site)
}

// PlannedFailoverPromote runs FailoverController.Execute against the
// named target and flips DNS. Returns the promotion GTID.
func (r *TopologyManagerRunner) PlannedFailoverPromote(ctx context.Context, nn types.NamespacedName, target, source string) (string, error) {
	tm, err := r.plannedFailoverManager(nn)
	if err != nil {
		return "", err
	}
	return tm.PlannedPromote(ctx, target, source)
}

// NeedLeaderElection implements manager.LeaderElectionRunnable.
// Topology polling and failover must only run on the leader.
func (r *TopologyManagerRunner) NeedLeaderElection() bool {
	return true
}

// Start implements manager.Runnable. It discovers MysqlFailoverGroup resources,
// starts a TopologyManager per group, and re-syncs periodically.
func (r *TopologyManagerRunner) Start(ctx context.Context) error {
	r.logger.Info("topology manager runner starting")

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

		// Include operator secret data hash so credential changes
		// trigger a topology manager restart with new connections.
		operatorSecretName := fg.Spec.EffectiveOperatorSecretName()
		if operatorSecretName != "" {
			var secret corev1.Secret
			if err := r.client.Get(ctx, types.NamespacedName{Namespace: fg.Namespace, Name: operatorSecretName}, &secret); err == nil {
				h := sha256.New()
				keys := make([]string, 0, len(secret.Data))
				for k := range secret.Data {
					keys = append(keys, k)
				}
				sort.Strings(keys)
				for _, k := range keys {
					fmt.Fprintf(h, "%s=%x\n", k, sha256.Sum256(secret.Data[k]))
				}
				cfg.CredentialHash = hex.EncodeToString(h.Sum(nil))[:16]
			}
		}

		r.mu.RLock()
		existing, ok := r.managers[nn]
		r.mu.RUnlock()

		suppress := restoreInFlight(fg)
		frozen := inPlaceRestoreInFlight(fg)
		plannedActive := plannedFailoverInFlight(fg.Status.PlannedFailover)

		if ok && existing.cfg.Equal(cfg) {
			existing.tm.SetAutoBootstrapSuppressed(suppress)
			existing.tm.SetTopologyFrozen(frozen)
			existing.tm.SetPlannedFailoverActive(plannedActive)
			if existing.dragonfly != nil {
				existing.dragonfly.SetPaused(plannedActive)
			}
			r.handleRecloneAnnotation(ctx, fg, nn, existing.tm)
			// Detect spec drift for ordered rolling updates.
			r.checkSpecDrift(ctx, fg, existing.tm)
			continue
		}

		if ok {
			r.logger.Info("config changed, restarting topology manager", "fg", nn)
			existing.cancel()
		}

		if err := r.startManager(ctx, fg, cfg); err != nil {
			r.logger.Error("failed to start topology manager", "fg", nn, "error", err)
			continue
		}
		// Apply the suppression flag on the freshly-started manager so the
		// first poll cycle already respects an in-flight restore.
		r.mu.RLock()
		mt, started := r.managers[nn]
		r.mu.RUnlock()
		if started {
			mt.tm.SetAutoBootstrapSuppressed(suppress)
			mt.tm.SetTopologyFrozen(frozen)
			mt.tm.SetPlannedFailoverActive(plannedActive)
			if mt.dragonfly != nil {
				mt.dragonfly.SetPaused(plannedActive)
			}
			r.handleRecloneAnnotation(ctx, fg, nn, mt.tm)
		}
	}

	// Stop managers for deleted CRs.
	r.mu.Lock()
	for nn, mt := range r.managers {
		if _, ok := seen[nn]; !ok {
			r.logger.Info("stopping topology manager for deleted fg", "fg", nn)
			mt.cancel()
			delete(r.managers, nn)
		}
	}
	r.mu.Unlock()

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
	if tm.SetRecloneSite(req.Site) && r.recorder != nil {
		r.recorder.Eventf(fg, corev1.EventTypeNormal, "RecloneRequested",
			"admin requested CLONE INSTANCE of site %q", req.Site)
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

	// Fetch TLS/credential secret data for spec hash computation.
	var tlsSecretData map[string][]byte
	if fg.Spec.TLS != nil {
		var tlsSecret corev1.Secret
		tlsSecretKey := types.NamespacedName{Namespace: fg.Namespace, Name: fg.Spec.TLS.SecretName}
		if err := r.client.Get(ctx, tlsSecretKey, &tlsSecret); err == nil {
			tlsSecretData = tlsSecret.Data
		}
	}
	var credSecretData map[string]map[string][]byte
	if fg.Spec.UsesCredentials() {
		credSecretData = make(map[string]map[string][]byte)
		for _, name := range fg.Spec.AllReferencedSecretNames() {
			var s corev1.Secret
			if err := r.client.Get(ctx, types.NamespacedName{Namespace: fg.Namespace, Name: name}, &s); err == nil {
				credSecretData[name] = s.Data
			}
		}
	}

	var driftSites []string
	for _, site := range fg.Spec.Sites {
		desiredHash := ComputeSpecHash(fg, site, tlsSecretData, credSecretData)

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

// startManager creates and starts a TopologyManager for a single MysqlFailoverGroup.
func (r *TopologyManagerRunner) startManager(ctx context.Context, fg *v1alpha1.MysqlFailoverGroup, cfg TopologyConfig) error {
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
		if fg.Spec.UsesCredentials() {
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
			dsn, err = buildSiteDSN(string(dsnBytes), fg, site)
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
		if fg.Spec.CloneTimeout > 0 {
			bootstrapCfg.CloneTimeout = time.Duration(fg.Spec.CloneTimeout) * time.Second
		}
	}

	updateCtl := NewUpdateController(failoverCtl, r.logger.With("fg", nn.String()))
	tm := NewTopologyManager(cfg, siteMySQL, failoverCtl, updateCtl, bootstrapCtl, bootstrapCfg, tainter, r.hub, dns,
		r.logger.With("fg", nn.String()))

	// Encryption-at-rest clone gate: a CLONE INSTANCE recipient needs a
	// writable keyring to rewrap the donor's tablespace keys, so the
	// topology manager must ask the reconciler to unseal the site first.
	// deployReconciler is the reconciler; the type assertion keeps the
	// runner from depending on the concrete type.
	if gate, ok := r.deployReconciler.(KeyringGate); ok && fg.Spec.EncryptionEnabled() {
		tm.SetKeyringGate(gate)
	}

	r.restoreFailoverState(tm, fg, nn)
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
		tm.SetRecoveryBlocked(site.Name, site.DivergentGtid, count)
		r.logger.Info("restored recovery blocked state from CR status",
			"fg", nn, "site", site.Name, "divergentGtid", site.DivergentGtid, "divergentTransactionCount", count)
	}

	// Set the status callback to update the CR status subresource on state
	// changes. Feed the write result back to the manager so a rejected
	// /status write (e.g. RBAC-denied mid-failover) arms a per-poll retry
	// that self-heals once the write is permitted again.
	tm.StatusCallback = func(snap TopologySnapshot) {
		err := r.updateCRStatus(ctx, nn, snap)
		tm.MarkStatusWriteResult(err)
	}
	// BootstrapStatusCallback updates only the Bootstrapping condition so that
	// unrelated conditions set by the most recent Poll cycle (Degraded,
	// ReplicationBroken, Updating, ...) are not clobbered by a partially
	// populated snapshot from the async bootstrap goroutine.
	tm.BootstrapStatusCallback = func(phase, errMsg, source string) {
		r.updateBootstrappingCondition(ctx, nn, phase, errMsg, source)
	}
	tm.RecloneCompleteCallback = func(completeCtx context.Context, site string) error {
		return r.removeRecloneAnnotationForSite(completeCtx, nn, site)
	}

	// Wire the ordered update callback so the UpdateController can reconcile
	// one site's Deployment at a time.
	if r.deployReconciler != nil {
		tm.ApplyUpdate = func(applyCtx context.Context, siteName string) error {
			return r.deployReconciler.ReconcileSiteDeployment(applyCtx, nn, siteName)
		}
	}

	var dfMgr *DragonflyManager
	if dragonflyEnabled(fg) {
		dfPollInterval := time.Duration(cfg.PollInterval)
		if dfPollInterval <= 0 {
			dfPollInterval = 2 * time.Second
		}
		dfMgr = NewDragonflyManager(r.client, r.recorder, r.logger.With("fg", nn.String(), "subsystem", "dragonfly"), nn, dfPollInterval)
		// Mirror the planned-failover guard so the dragonfly manager
		// stops issuing REPLICAOF while a planned switchover is in
		// flight (the state machine handles its own promotion).
		dfMgr.SetPaused(plannedFailoverInFlight(fg.Status.PlannedFailover))
	}

	// Wire the best-effort emergency Dragonfly promotion. Runs only
	// after MySQL emergency failover Execute has succeeded; failures
	// here are logged and never propagate back to MySQL.
	if dfMgr != nil {
		dfMgrLocal := dfMgr
		tm.EmergencyFailoverCallback = func(emCtx context.Context, target, oldPrimary string) {
			dfMgrLocal.TryEmergencyPromote(emCtx, target, oldPrimary)
		}
	}

	tmCtx, cancel := context.WithCancel(ctx)

	siteNames := make([]string, len(fg.Spec.Sites))
	for i, s := range fg.Spec.Sites {
		siteNames[i] = s.Name
	}
	archiver := newArchiverPoller(fg.Namespace, fg.Name, siteNames, buildSidecarClients(fg),
		r.logger.With("fg", nn.String()))

	r.mu.Lock()
	r.managers[nn] = &managedTopology{
		tm:        tm,
		cancel:    cancel,
		cfg:       cfg,
		archiver:  archiver,
		dragonfly: dfMgr,
	}
	r.mu.Unlock()

	go func() {
		r.logger.Info("starting topology manager", "fg", nn)
		tm.Run(tmCtx)
		for i := range siteMySQL {
			siteMySQL[i].Close()
		}
		r.logger.Info("topology manager stopped", "fg", nn)
	}()

	go archiver.Run(tmCtx)

	if dfMgr != nil {
		go func() {
			r.logger.Info("starting dragonfly manager", "fg", nn)
			dfMgr.Run(tmCtx)
			r.logger.Info("dragonfly manager stopped", "fg", nn)
		}()
	}

	return nil
}

// stopAll cancels all running topology managers.
// restoreFailoverState wires the out-of-band anti-flap store and restores
// failover history into a fresh TopologyManager, so recovery logic and the
// anti-flap cooldown work across operator restarts — without this,
// checkRecovery() returns early because lastFailoverTarget is empty after a
// fresh TopologyManager, and the topology.go cooldown branch becomes a
// no-op until the next failover stamps a fresh value, so a fast
// restart-then-peer-failure could ping-pong promotions that the original
// process would have blocked.
//
// Two durable copies exist and either can be the fresher one: status is
// ahead when the annotation write was rejected, the annotations are ahead
// when the status write was. Rehydrating from whichever is newer is what
// makes the cooldown survive an outage on one path — restoring only from
// status is exactly the CooldownViolated(restart) window the simulator
// reproduces.
func (r *TopologyManagerRunner) restoreFailoverState(tm *TopologyManager, fg *v1alpha1.MysqlFailoverGroup, nn types.NamespacedName) {
	// Wire the store before anything can promote. The annotations it writes
	// are the second durable copy of the record below, on an API path that
	// fails independently of the status subresource.
	tm.SetFailoverStateRecorder(NewAnnotationFailoverStateRecorder(r.client, nn))

	statusRecord := FailoverRecord{LastFailoverTarget: fg.Status.LastFailoverTarget}
	if fg.Status.LastFailover != nil && !fg.Status.LastFailover.IsZero() {
		statusRecord.LastFailover = fg.Status.LastFailover.Time
	}
	failoverRecord, fromAnnotations := statusRecord, false
	if oob, err := FailoverRecordFromAnnotations(fg.GetAnnotations()); err != nil {
		// Corrupt annotation: fall back to status rather than treating it as
		// "no history", and say so loudly — a silently dropped record is the
		// failure mode this whole path exists to prevent.
		r.logger.Error("out-of-band anti-flap annotation unreadable; falling back to CR status",
			"fg", nn, "error", err)
	} else if newer := NewerFailoverRecord(statusRecord, oob); newer != statusRecord {
		failoverRecord, fromAnnotations = newer, true
	}

	// The two restores below are logged under distinct msg strings rather
	// than one msg with a source field: `restored lastFailoverTarget from CR
	// status` is a documented stable event (docs/docs/log-schema.mdx), and
	// the out-of-band case is worth alerting on in its own right — it means
	// this group's status writes were failing when it last promoted.
	if failoverRecord.LastFailoverTarget != "" {
		tm.SetLastFailoverTarget(failoverRecord.LastFailoverTarget)
		if fromAnnotations {
			r.logger.Warn("restored lastFailoverTarget from out-of-band annotations",
				"fg", nn, "target", failoverRecord.LastFailoverTarget,
				"statusTarget", statusRecord.LastFailoverTarget)
		} else {
			r.logger.Info("restored lastFailoverTarget from CR status", "fg", nn, "target", failoverRecord.LastFailoverTarget)
		}
	}
	if !failoverRecord.LastFailover.IsZero() {
		tm.SetLastFailover(failoverRecord.LastFailover)
		if fromAnnotations {
			r.logger.Warn("restored lastFailover from out-of-band annotations",
				"fg", nn, "lastFailover", failoverRecord.LastFailover,
				"statusLastFailover", statusRecord.LastFailover)
		} else {
			r.logger.Info("restored lastFailover from CR status", "fg", nn, "lastFailover", failoverRecord.LastFailover)
		}
	}
}

func (r *TopologyManagerRunner) stopAll() {
	r.mu.Lock()
	defer r.mu.Unlock()
	for nn, mt := range r.managers {
		r.logger.Info("stopping topology manager", "fg", nn)
		mt.cancel()
	}
	r.managers = make(map[types.NamespacedName]*managedTopology)
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

// SetManagerForTest injects a TopologyManager for testing. Not for production use.
func (r *TopologyManagerRunner) SetManagerForTest(nn types.NamespacedName, tm *TopologyManager) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.managers[nn] = &managedTopology{tm: tm}
}

// StopManager stops and removes the topology manager for the given NamespacedName.
// This is called from the reconciler's finalizer handling when a CR is being deleted.
func (r *TopologyManagerRunner) StopManager(nn types.NamespacedName) {
	r.mu.Lock()
	defer r.mu.Unlock()
	mt, ok := r.managers[nn]
	if !ok {
		r.logger.Info("no topology manager found to stop", "fg", nn)
		return
	}
	r.logger.Info("stopping topology manager via StopManager", "fg", nn)
	mt.cancel()
	delete(r.managers, nn)
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

	freshFG.Status.ActiveSite = snap.ActiveSite
	// Ensure the Sites slice is allocated to match the number of sites.
	if len(freshFG.Status.Sites) != len(snap.Sites) {
		freshFG.Status.Sites = make([]v1alpha1.SiteStatus, len(snap.Sites))
	}
	for i, s := range snap.Sites {
		freshFG.Status.Sites[i] = siteStatusFromSnapshot(s)
	}

	if !snap.LastFailover.IsZero() {
		t := metav1.NewTime(snap.LastFailover)
		freshFG.Status.LastFailover = &t
	}
	freshFG.Status.LastFailoverTarget = snap.LastFailoverTarget
	freshFG.Status.PromotionGtidExecuted = snap.PromotionGtidExecuted

	// Write per-site recovery fields. Every site carries its own recovery
	// state and divergence report — multiple sites can be in recovery at
	// once (e.g. two former primaries after consecutive failovers).
	for i, s := range snap.Sites {
		freshFG.Status.Sites[i].RecoveryState = s.RecoveryState
		freshFG.Status.Sites[i].DivergentGtid = s.DivergentGtid
		if s.RecoveryState != "" && s.DivergentTxnCount > 0 {
			c := s.DivergentTxnCount
			freshFG.Status.Sites[i].DivergentTransactionCount = &c
		} else {
			freshFG.Status.Sites[i].DivergentTransactionCount = nil
		}
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
	readyStatus := hasWritable && replicationHealthy
	readyMessage := "At least one site is writable and replication is healthy"
	if !hasWritable {
		readyMessage = "No site is writable"
	} else if !replicationHealthy {
		readyMessage = "Replication is not healthy"
	}
	setCondition(&freshFG.Status.Conditions, metav1.Condition{
		Type:               "Ready",
		Status:             conditionBool(readyStatus),
		ObservedGeneration: freshFG.Generation,
		LastTransitionTime: now,
		Reason:             "TopologyPolled",
		Message:            readyMessage,
	})

	reason := degradedReason(snap)
	if reason != "Healthy" {
		setCondition(&freshFG.Status.Conditions, metav1.Condition{
			Type:               "Degraded",
			Status:             metav1.ConditionTrue,
			ObservedGeneration: freshFG.Generation,
			LastTransitionTime: now,
			Reason:             reason,
			Message:            snap.Alert,
		})
	} else {
		setCondition(&freshFG.Status.Conditions, metav1.Condition{
			Type:               "Degraded",
			Status:             metav1.ConditionFalse,
			ObservedGeneration: freshFG.Generation,
			LastTransitionTime: now,
			Reason:             "Healthy",
			Message:            "No cross-site alerts",
		})
	}

	// Add replication-specific degraded conditions.
	maxLagSeconds := freshFG.Spec.EffectiveMaxLagSeconds()

	for _, s := range snap.Sites {
		if s.Role == state.SiteRoleReadOnly {
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
		if repl.SecondsBehindSource != nil && *repl.SecondsBehindSource > maxLagSeconds {
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

	// Bootstrapping condition reflects fresh-deploy bootstrap progress.
	setBootstrappingCondition(&freshFG.Status.Conditions, freshFG.Generation, now, snap.BootstrapPhase, snap.BootstrapError, snap.BootstrapSource)

	// Update phase tracking.
	freshFG.Status.UpdatePhase = snap.UpdatePhase
	if snap.UpdatePhase != "" {
		setCondition(&freshFG.Status.Conditions, metav1.Condition{
			Type:               "Updating",
			Status:             metav1.ConditionTrue,
			ObservedGeneration: freshFG.Generation,
			LastTransitionTime: now,
			Reason:             snap.UpdatePhase,
			Message:            fmt.Sprintf("Ordered update in progress: %s", snap.UpdatePhase),
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
		setCondition(&freshFG.Status.Conditions, metav1.Condition{
			Type:               "RecoveryPending",
			Status:             metav1.ConditionTrue,
			ObservedGeneration: freshFG.Generation,
			LastTransitionTime: now,
			Reason:             "DivergentTransactions",
			Message:            fmt.Sprintf("Old primary %s has %d divergent transactions — annotate with bloodraven.shipstream.io/reclone-site=%s to recover", snap.RecoverySite, snap.DivergentTxnCount, snap.RecoverySite),
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

	// Populate status.pitr from the archiver poller's cache. Done here
	// (rather than on every reconcile) so the topology callback is the
	// single writer of freshFG.Status and we stay on one resource
	// version.
	r.populatePITRStatus(nn, &freshFG)

	// Skip no-op writes to avoid bumping resourceVersion unnecessarily.
	if equality.Semantic.DeepEqual(existingStatus, &freshFG.Status) {
		r.logger.Debug("status unchanged, skipping update", "fg", nn)
		return nil
	}

	if err := k8sretry.RetryOnConflict(k8sretry.DefaultRetry, func() error {
		// Re-fetch the CR to get the latest resource version.
		var fresh v1alpha1.MysqlFailoverGroup
		if err := r.client.Get(ctx, nn, &fresh); err != nil {
			return err
		}
		// Apply status changes to the freshly-fetched object.
		fresh.Status = freshFG.Status
		return r.client.Status().Update(ctx, &fresh)
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
	r.emitFailoverEvents(&freshFG, existingStatus, snap)
	r.emitDegradedTransitionEvents(&freshFG, nn, snap)
	return nil
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
// No-ops when PITR is disabled in spec or when no archiver snapshot
// has been observed yet (e.g. the poller just started and both sites
// are unreachable). In that case we clear stale data so a disabled
// flip doesn't keep showing bounds from a previous configuration.
func (r *TopologyManagerRunner) populatePITRStatus(nn types.NamespacedName, fg *v1alpha1.MysqlFailoverGroup) {
	enabled := fg.Spec.Backup != nil && fg.Spec.Backup.PITR != nil && fg.Spec.Backup.PITR.Enabled
	if !enabled {
		fg.Status.PITR = nil
		return
	}

	r.mu.RLock()
	mt, ok := r.managers[nn]
	r.mu.RUnlock()
	if !ok || mt.archiver == nil {
		return
	}

	profileName := ""
	if fg.Spec.Backup.PITR.ProfileName != "" {
		profileName = fg.Spec.Backup.PITR.ProfileName
	}

	out := &v1alpha1.PITRStatus{
		Enabled:     true,
		ProfileName: profileName,
	}
	// Iterate in site-name order so the derived Message (and any future
	// order-sensitive field) is reproducible across reconciles — map
	// iteration in Go is randomized, which would otherwise flap the
	// CR status on every poll when multiple sites report LastError.
	snaps := mt.archiver.Snapshots()
	sites := make([]string, 0, len(snaps))
	for name := range snaps {
		sites = append(sites, name)
	}
	sort.Strings(sites)

	var (
		haveAny   bool
		errorMsgs []string
	)
	for _, name := range sites {
		s := snaps[name]
		if s == nil {
			continue
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
		if s.LastError != "" {
			errorMsgs = append(errorMsgs, fmt.Sprintf("%s: %s", name, s.LastError))
		}
	}
	if !haveAny {
		// No sidecar has responded yet; keep existing status to avoid
		// flapping between populated and empty while polls warm up.
		return
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
	for _, s := range existingStatus.Sites {
		oldState[s.Name] = s.RecoveryState
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
		if s.RecoveryState == recoveryStateBlocked &&
			(oldState[s.Name] != recoveryStateBlocked || countChanged) {
			r.recorder.Eventf(fg, corev1.EventTypeWarning, "DataLossDetected",
				"%d divergent transactions on %s did not replicate before failover",
				s.DivergentTxnCount, s.Name)
		}
		// Recovery complete: RecoveryInProgress/RecoveryBlocked cleared. A
		// marker cleared because the site became WRITABLE (promotion or
		// re-assert) is not a recovery completion — the site did not rejoin
		// as a replica — so no Normal event is emitted for it.
		if recoveryActive(oldState[s.Name]) && !recoveryActive(s.RecoveryState) && s.State != state.StateWritable {
			r.recorder.Eventf(fg, corev1.EventTypeNormal, "RecoveryComplete",
				"Old primary %s recovered and is now replicating", s.Name)
		}
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

// buildSiteDSN takes a base DSN and replaces the host with the site service endpoint.
func buildSiteDSN(baseDSN string, fg *v1alpha1.MysqlFailoverGroup, site v1alpha1.SiteSpec) (string, error) {
	parsed, err := mysql.ParseDSN(baseDSN)
	if err != nil {
		return "", fmt.Errorf("parse DSN: %w", err)
	}
	parsed.Addr = fmt.Sprintf("%s:%d", internalSiteServiceHost(fg.Name, site.Name, fg.Namespace), mysqlPort)
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
