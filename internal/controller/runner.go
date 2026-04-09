package controller

import (
	"context"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"github.com/go-sql-driver/mysql"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/equality"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/kubernetes"
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

// TopologyManagerRunner manages TopologyManager instances for all MysqlReplicaPair resources.
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

// Start implements manager.Runnable. It discovers MysqlReplicaPair resources,
// starts a TopologyManager per pair, and re-syncs periodically.
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

// sync lists all MysqlReplicaPair resources and ensures a topology manager
// is running for each one. Stale managers are stopped.
func (r *TopologyManagerRunner) sync(ctx context.Context) error {
	var pairs v1alpha1.MysqlReplicaPairList
	if err := r.client.List(ctx, &pairs); err != nil {
		return fmt.Errorf("list MysqlReplicaPairs: %w", err)
	}

	seen := make(map[types.NamespacedName]struct{})

	for i := range pairs.Items {
		pair := &pairs.Items[i]
		nn := PairNamespacedName(pair)
		seen[nn] = struct{}{}

		cfg := CRConfigToTopologyConfig(pair)

		r.mu.RLock()
		existing, ok := r.managers[nn]
		r.mu.RUnlock()

		if ok && existing.cfg == cfg {
			continue
		}

		if ok {
			r.logger.Info("config changed, restarting topology manager", "pair", nn)
			existing.cancel()
		}

		if err := r.startManager(ctx, pair, cfg); err != nil {
			r.logger.Error("failed to start topology manager", "pair", nn, "error", err)
		}
	}

	// Stop managers for deleted CRs.
	r.mu.Lock()
	for nn, mt := range r.managers {
		if _, ok := seen[nn]; !ok {
			r.logger.Info("stopping topology manager for deleted pair", "pair", nn)
			mt.cancel()
			delete(r.managers, nn)
		}
	}
	r.mu.Unlock()

	return nil
}

// startManager creates and starts a TopologyManager for a single MysqlReplicaPair.
func (r *TopologyManagerRunner) startManager(ctx context.Context, pair *v1alpha1.MysqlReplicaPair, cfg TopologyConfig) error {
	nn := PairNamespacedName(pair)

	// Read the MySQL credentials secret.
	var secret corev1.Secret
	secretNN := types.NamespacedName{Namespace: pair.Namespace, Name: pair.Spec.SecretName}
	if err := r.client.Get(ctx, secretNN, &secret); err != nil {
		return fmt.Errorf("get secret %s: %w", secretNN, err)
	}

	dsnBytes, ok := secret.Data["dsn"]
	if !ok {
		return fmt.Errorf("secret %s missing 'dsn' key", secretNN)
	}

	dc1DSN, err := buildDCDSN(string(dsnBytes), pair, pair.Spec.DC1)
	if err != nil {
		return fmt.Errorf("build DC1 DSN: %w", err)
	}
	dc2DSN, err := buildDCDSN(string(dsnBytes), pair, pair.Spec.DC2)
	if err != nil {
		return fmt.Errorf("build DC2 DSN: %w", err)
	}

	dc1MySQL, err := internalmysql.NewChecker(dc1DSN)
	if err != nil {
		return fmt.Errorf("create DC1 MySQL checker: %w", err)
	}

	dc2MySQL, err := internalmysql.NewChecker(dc2DSN)
	if err != nil {
		dc1MySQL.Close()
		return fmt.Errorf("create DC2 MySQL checker: %w", err)
	}

	failoverCtl := NewFailoverController(r.logger.With("pair", nn.String()))
	tainter := platform.NewNodeTainter(r.clientset, r.logger.With("pair", nn.String()))

	// Read Cloudflare API token.
	var cfSecret corev1.Secret
	cfSecretNN := types.NamespacedName{
		Namespace: pair.Namespace,
		Name:      pair.Spec.Cloudflare.APITokenSecretRef.Name,
	}
	if err := r.client.Get(ctx, cfSecretNN, &cfSecret); err != nil {
		dc1MySQL.Close()
		dc2MySQL.Close()
		return fmt.Errorf("get Cloudflare secret %s: %w", cfSecretNN, err)
	}
	cfToken := string(cfSecret.Data[pair.Spec.Cloudflare.APITokenSecretRef.Key])
	if cfToken == "" {
		dc1MySQL.Close()
		dc2MySQL.Close()
		return fmt.Errorf("Cloudflare secret %s key %s is empty", cfSecretNN, pair.Spec.Cloudflare.APITokenSecretRef.Key)
	}

	dns := platform.NewCloudflareDNS(cfToken, pair.Spec.Cloudflare.ZoneID, pair.Spec.AZ)

	tm := NewTopologyManager(cfg, dc1MySQL, dc2MySQL, failoverCtl, tainter, r.hub, dns,
		r.logger.With("pair", nn.String()))

	// Set the status callback to update the CR status subresource on state changes.
	tm.StatusCallback = func(snap TopologySnapshot) {
		r.updateCRStatus(ctx, nn, snap)
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
		r.logger.Info("starting topology manager", "pair", nn)
		tm.Run(tmCtx)
		dc1MySQL.Close()
		dc2MySQL.Close()
		r.logger.Info("topology manager stopped", "pair", nn)
	}()

	return nil
}

// stopAll cancels all running topology managers.
func (r *TopologyManagerRunner) stopAll() {
	r.mu.Lock()
	defer r.mu.Unlock()
	for nn, mt := range r.managers {
		r.logger.Info("stopping topology manager", "pair", nn)
		mt.cancel()
	}
	r.managers = make(map[types.NamespacedName]*managedTopology)
}

// Status returns the StatusResponse for a named pair.
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

// StopManager stops and removes the topology manager for the given NamespacedName.
// This is called from the reconciler's finalizer handling when a CR is being deleted.
func (r *TopologyManagerRunner) StopManager(nn types.NamespacedName) {
	r.mu.Lock()
	defer r.mu.Unlock()
	mt, ok := r.managers[nn]
	if !ok {
		r.logger.Info("no topology manager found to stop", "pair", nn)
		return
	}
	r.logger.Info("stopping topology manager via StopManager", "pair", nn)
	mt.cancel()
	delete(r.managers, nn)
}

// updateCRStatus writes topology state to the CR status subresource.
func (r *TopologyManagerRunner) updateCRStatus(ctx context.Context, nn types.NamespacedName, snap TopologySnapshot) {
	// Fetch the CR to get the latest resourceVersion and generation for the status update.
	var freshPair v1alpha1.MysqlReplicaPair
	if err := r.client.Get(ctx, nn, &freshPair); err != nil {
		if apierrors.IsNotFound(err) {
			r.logger.Warn("CR deleted, skipping status update", "pair", nn)
			return
		}
		r.logger.Error("get pair for status update", "pair", nn, "error", err)
		return
	}

	// Save existing status before modification for comparison.
	existingStatus := freshPair.Status.DeepCopy()

	freshPair.Status.PrimaryDC = snap.PrimaryDC
	freshPair.Status.DC1 = dcInstanceStatusFromSnapshot(snap.DC1State, snap.DC1LastSeen, snap.DC1Replication)
	freshPair.Status.DC2 = dcInstanceStatusFromSnapshot(snap.DC2State, snap.DC2LastSeen, snap.DC2Replication)

	if !snap.LastFailover.IsZero() {
		t := metav1.NewTime(snap.LastFailover)
		freshPair.Status.LastFailover = &t
	}
	freshPair.Status.LastFailoverTarget = snap.LastFailoverTarget

	// Evaluate replication health.
	hasWritable := snap.DC1State == state.StateWritable || snap.DC2State == state.StateWritable
	replicationHealthy := true
	if snap.DC1Replication != nil {
		replicationHealthy = snap.DC1Replication.IORunning && snap.DC1Replication.SQLRunning
	}
	if snap.DC2Replication != nil {
		replicationHealthy = replicationHealthy && snap.DC2Replication.IORunning && snap.DC2Replication.SQLRunning
	}

	// Update conditions using freshPair.Generation so ObservedGeneration reflects
	// the generation of the object we are actually writing against.
	now := metav1.Now()
	readyStatus := hasWritable && replicationHealthy
	readyMessage := "At least one DC is writable and replication is healthy"
	if !hasWritable {
		readyMessage = "No DC is writable"
	} else if !replicationHealthy {
		readyMessage = "Replication is not healthy"
	}
	setCondition(&freshPair.Status.Conditions, metav1.Condition{
		Type:               "Ready",
		Status:             conditionBool(readyStatus),
		ObservedGeneration: freshPair.Generation,
		LastTransitionTime: now,
		Reason:             "TopologyPolled",
		Message:            readyMessage,
	})

	if snap.Alert != "" {
		reason := "Alert"
		if snap.DC1State == state.StateWritable && snap.DC2State == state.StateWritable {
			reason = "SplitBrain"
		} else if snap.DC1State == state.StateReadOnly && snap.DC2State == state.StateReadOnly {
			reason = "NoPrimary"
		} else if snap.DC1State == state.StateUnreachable && snap.DC2State == state.StateUnreachable {
			reason = "TotalLoss"
		}
		setCondition(&freshPair.Status.Conditions, metav1.Condition{
			Type:               "Degraded",
			Status:             metav1.ConditionTrue,
			ObservedGeneration: freshPair.Generation,
			LastTransitionTime: now,
			Reason:             reason,
			Message:            snap.Alert,
		})
	} else {
		setCondition(&freshPair.Status.Conditions, metav1.Condition{
			Type:               "Degraded",
			Status:             metav1.ConditionFalse,
			ObservedGeneration: freshPair.Generation,
			LastTransitionTime: now,
			Reason:             "Healthy",
			Message:            "No cross-DC alerts",
		})
	}

	// Add replication-specific degraded conditions.
	maxLagSeconds := int64(300)
	if freshPair.Spec.Replication != nil && freshPair.Spec.Replication.MaxLagSeconds > 0 {
		maxLagSeconds = freshPair.Spec.Replication.MaxLagSeconds
	}

	replChecks := []struct {
		name string
		repl *internalmysql.ReplicaStatus
	}{
		{snap.DC1Name, snap.DC1Replication},
		{snap.DC2Name, snap.DC2Replication},
	}

	for _, rc := range replChecks {
		if rc.repl == nil {
			continue
		}
		if !rc.repl.IORunning || !rc.repl.SQLRunning {
			setCondition(&freshPair.Status.Conditions, metav1.Condition{
				Type:               "Degraded",
				Status:             metav1.ConditionTrue,
				ObservedGeneration: freshPair.Generation,
				LastTransitionTime: now,
				Reason:             "ReplicationBroken",
				Message:            fmt.Sprintf("Replication IO/SQL thread not running on %s", rc.name),
			})
		}
		if rc.repl.SecondsBehindSource != nil && *rc.repl.SecondsBehindSource > maxLagSeconds {
			setCondition(&freshPair.Status.Conditions, metav1.Condition{
				Type:               "Degraded",
				Status:             metav1.ConditionTrue,
				ObservedGeneration: freshPair.Generation,
				LastTransitionTime: now,
				Reason:             "ReplicationLagging",
				Message:            fmt.Sprintf("Replication lag on %s is %ds (threshold %ds)", rc.name, *rc.repl.SecondsBehindSource, maxLagSeconds),
			})
		}
		if rc.repl.LastError != "" {
			setCondition(&freshPair.Status.Conditions, metav1.Condition{
				Type:               "Degraded",
				Status:             metav1.ConditionTrue,
				ObservedGeneration: freshPair.Generation,
				LastTransitionTime: now,
				Reason:             "ReplicationError",
				Message:            fmt.Sprintf("Replication error on %s: %s", rc.name, rc.repl.LastError),
			})
		}
	}

	// Skip no-op writes to avoid bumping resourceVersion unnecessarily.
	if equality.Semantic.DeepEqual(existingStatus, &freshPair.Status) {
		r.logger.Debug("status unchanged, skipping update", "pair", nn)
		return
	}

	if err := r.client.Status().Update(ctx, &freshPair); err != nil {
		if apierrors.IsNotFound(err) {
			r.logger.Warn("CR deleted during status update, skipping", "pair", nn)
			return
		}
		r.logger.Error("update pair status", "pair", nn, "error", err)
	}
}

func dcInstanceStatusFromSnapshot(s state.DCState, lastSeen time.Time, repl *internalmysql.ReplicaStatus) v1alpha1.DCInstanceStatus {
	status := v1alpha1.DCInstanceStatus{
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

// buildDCDSN takes a base DSN and replaces the host with the DC service endpoint.
func buildDCDSN(baseDSN string, pair *v1alpha1.MysqlReplicaPair, dc v1alpha1.DCInstanceSpec) (string, error) {
	parsed, err := mysql.ParseDSN(baseDSN)
	if err != nil {
		return "", fmt.Errorf("parse DSN: %w", err)
	}
	parsed.Addr = fmt.Sprintf("mysql-%s-%s.%s.svc.cluster.local:%d", pair.Name, dc.Name, pair.Namespace, mysqlPort)
	return parsed.FormatDSN(), nil
}
