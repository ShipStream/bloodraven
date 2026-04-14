package controller

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"log/slog"
	"sort"
	"sync"
	"time"

	"github.com/go-sql-driver/mysql"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/equality"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/kubernetes"
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
}

// TopologyManagerRunner manages TopologyManager instances for all MysqlFailoverGroup resources.
// It implements manager.Runnable and runs only on the leader-elected instance.
type TopologyManagerRunner struct {
	client    client.Client
	clientset kubernetes.Interface
	hub       *platform.Hub
	logger    *slog.Logger

	mu       sync.RWMutex
	managers map[types.NamespacedName]*managedTopology
}

// NewTopologyManagerRunner creates a new runner.
func NewTopologyManagerRunner(c client.Client, clientset kubernetes.Interface, hub *platform.Hub, logger *slog.Logger) *TopologyManagerRunner {
	return &TopologyManagerRunner{
		client:    c,
		clientset: clientset,
		hub:       hub,
		logger:    logger,
		managers:  make(map[types.NamespacedName]*managedTopology),
	}
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

		if ok && existing.cfg == cfg {
			existing.tm.SetAutoBootstrapSuppressed(suppress)
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
		if mt, ok := r.managers[nn]; ok {
			mt.tm.SetAutoBootstrapSuppressed(suppress)
		}
		r.mu.RUnlock()
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

	var siteMySQL [2]internalmysql.Checker
	for i, site := range fg.Spec.Sites {
		var dsn string
		var err error
		if fg.Spec.UsesCredentials() {
			dsn = buildSiteDSNFromCreds(
				string(secret.Data["username"]),
				string(secret.Data["password"]),
				fg, site,
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

	tm := NewTopologyManager(cfg, siteMySQL[0], siteMySQL[1], failoverCtl, bootstrapCtl, bootstrapCfg, tainter, r.hub, dns,
		r.logger.With("fg", nn.String()))

	// Set the status callback to update the CR status subresource on state changes.
	tm.StatusCallback = func(snap TopologySnapshot) {
		r.updateCRStatus(ctx, nn, snap)
	}
	// BootstrapStatusCallback updates only the Bootstrapping condition so that
	// unrelated conditions set by the most recent Poll cycle (Degraded,
	// ReplicationBroken, Updating, ...) are not clobbered by a partially
	// populated snapshot from the async bootstrap goroutine.
	tm.BootstrapStatusCallback = func(phase, errMsg string) {
		r.updateBootstrappingCondition(ctx, nn, phase, errMsg)
	}

	tmCtx, cancel := context.WithCancel(ctx)

	r.mu.Lock()
	r.managers[nn] = &managedTopology{
		tm:     tm,
		cancel: cancel,
		cfg:    cfg,
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

	return nil
}

// stopAll cancels all running topology managers.
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

// updateCRStatus writes topology state to the CR status subresource.
func (r *TopologyManagerRunner) updateCRStatus(ctx context.Context, nn types.NamespacedName, snap TopologySnapshot) {
	// Fetch the CR to get the latest resourceVersion and generation for the status update.
	var freshFG v1alpha1.MysqlFailoverGroup
	if err := r.client.Get(ctx, nn, &freshFG); err != nil {
		if apierrors.IsNotFound(err) {
			r.logger.Warn("CR deleted, skipping status update", "fg", nn)
			return
		}
		r.logger.Error("get fg for status update", "fg", nn, "error", err)
		return
	}

	// Save existing status before modification for comparison.
	existingStatus := freshFG.Status.DeepCopy()

	freshFG.Status.ActiveSite = snap.ActiveSite
	// Ensure the Sites slice is allocated to match the number of sites.
	if len(freshFG.Status.Sites) != len(snap.SiteNames) {
		freshFG.Status.Sites = make([]v1alpha1.SiteStatus, len(snap.SiteNames))
	}
	for i := range freshFG.Status.Sites {
		freshFG.Status.Sites[i] = siteStatusFromSnapshot(snap.SiteNames[i], snap.SiteStates[i], snap.SiteLastSeen[i], snap.SiteReplication[i])
	}

	if !snap.LastFailover.IsZero() {
		t := metav1.NewTime(snap.LastFailover)
		freshFG.Status.LastFailover = &t
	}
	freshFG.Status.LastFailoverTarget = snap.LastFailoverTarget

	// Evaluate replication health.
	hasWritable := snap.SiteStates[0] == state.StateWritable || snap.SiteStates[1] == state.StateWritable
	replicationHealthy := true
	for i := range snap.SiteReplication {
		if snap.SiteReplication[i] != nil {
			replicationHealthy = replicationHealthy && snap.SiteReplication[i].IORunning && snap.SiteReplication[i].SQLRunning
		}
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

	if snap.Alert != "" {
		reason := "Alert"
		if snap.SiteStates[0] == state.StateWritable && snap.SiteStates[1] == state.StateWritable {
			reason = "SplitBrain"
		} else if snap.SiteStates[0] == state.StateReadOnly && snap.SiteStates[1] == state.StateReadOnly {
			reason = "NoPrimary"
		} else if snap.SiteStates[0] == state.StateUnreachable && snap.SiteStates[1] == state.StateUnreachable {
			reason = "TotalLoss"
		}
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
	maxLagSeconds := int64(300)
	if freshFG.Spec.Replication != nil && freshFG.Spec.Replication.MaxLagSeconds > 0 {
		maxLagSeconds = freshFG.Spec.Replication.MaxLagSeconds
	}

	for i := range snap.SiteReplication {
		repl := snap.SiteReplication[i]
		if repl == nil {
			continue
		}
		siteName := snap.SiteNames[i]
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
	setBootstrappingCondition(&freshFG.Status.Conditions, freshFG.Generation, now, snap.BootstrapPhase, snap.BootstrapError)

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

	// Skip no-op writes to avoid bumping resourceVersion unnecessarily.
	if equality.Semantic.DeepEqual(existingStatus, &freshFG.Status) {
		r.logger.Debug("status unchanged, skipping update", "fg", nn)
		return
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
			return
		}
		r.logger.Error("update fg status", "fg", nn, "error", err)
	}
}

// setBootstrappingCondition writes the Bootstrapping condition based on the
// current phase. It is shared between the full-status writer (updateCRStatus)
// and the dedicated bootstrap-only writer (updateBootstrappingCondition) so
// they stay in sync.
func setBootstrappingCondition(conditions *[]metav1.Condition, generation int64, now metav1.Time, phase, errMsg string) {
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
			Message:            "Fresh-deploy bootstrap completed successfully",
		})
	case string(BootstrapPhaseFailed):
		msg := "Bootstrap failed"
		if errMsg != "" {
			msg = fmt.Sprintf("Bootstrap failed: %s", errMsg)
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
			Message:            fmt.Sprintf("Fresh-deploy bootstrap in progress: %s", phase),
		})
	}
}

// updateBootstrappingCondition updates ONLY the Bootstrapping condition on the
// CR. It is invoked from the bootstrap goroutine on phase transitions; using a
// dedicated path (rather than updateCRStatus with a partially populated
// TopologySnapshot) prevents inadvertently clearing unrelated conditions.
func (r *TopologyManagerRunner) updateBootstrappingCondition(ctx context.Context, nn types.NamespacedName, phase, errMsg string) {
	err := k8sretry.RetryOnConflict(k8sretry.DefaultRetry, func() error {
		var fresh v1alpha1.MysqlFailoverGroup
		if err := r.client.Get(ctx, nn, &fresh); err != nil {
			return err
		}
		before := fresh.Status.DeepCopy()
		setBootstrappingCondition(&fresh.Status.Conditions, fresh.Generation, metav1.Now(), phase, errMsg)
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

func siteStatusFromSnapshot(name string, s state.SiteState, lastSeen time.Time, repl *internalmysql.ReplicaStatus) v1alpha1.SiteStatus {
	status := v1alpha1.SiteStatus{
		Name:  name,
		State: s.String(),
	}
	if !lastSeen.IsZero() {
		t := metav1.NewTime(lastSeen)
		status.LastSeen = &t
	}
	if repl != nil {
		status.Replicating = repl.IORunning && repl.SQLRunning
		status.SecondsBehindSource = repl.SecondsBehindSource
		status.GtidExecuted = repl.ExecutedGtidSet
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
	parsed.Addr = fmt.Sprintf("mysql-%s-%s.%s.svc.cluster.local:%d", fg.Name, site.Name, fg.Namespace, mysqlPort)
	return parsed.FormatDSN(), nil
}

// buildSiteDSNFromCreds constructs a DSN from username/password credentials
// and the site service endpoint (used in credentials mode).
func buildSiteDSNFromCreds(username, password string, fg *v1alpha1.MysqlFailoverGroup, site v1alpha1.SiteSpec) string {
	cfg := mysql.NewConfig()
	cfg.User = username
	cfg.Passwd = password
	cfg.Net = "tcp"
	cfg.Addr = fmt.Sprintf("mysql-%s-%s.%s.svc.cluster.local:%d", fg.Name, site.Name, fg.Namespace, mysqlPort)
	cfg.Timeout = 5 * time.Second
	return cfg.FormatDSN()
}
