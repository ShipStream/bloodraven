package controller

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"sort"
	"strings"
	"time"

	appsv1 "k8s.io/api/apps/v1"
	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	policyv1 "k8s.io/api/policy/v1"
	"k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/intstr"
	"k8s.io/client-go/tools/record"
	k8sretry "k8s.io/client-go/util/retry"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
	"sigs.k8s.io/controller-runtime/pkg/handler"
	"sigs.k8s.io/controller-runtime/pkg/log"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	v1alpha1 "github.com/shipstream/bloodraven/api/v1alpha1"
	"github.com/shipstream/bloodraven/internal/platform"
	"github.com/shipstream/bloodraven/internal/state"
)

const (
	finalizerName = "shipstream.io/graceful-shutdown"

	defaultMySQLImage = "mysql:9.6"

	labelAppName       = "app.kubernetes.io/name"
	labelInstance      = "app.kubernetes.io/instance"
	labelFailoverGroup = "shipstream.io/failover-group"
	labelSite          = "shipstream.io/site"
	labelRole          = "shipstream.io/role"
	labelHealthy       = "shipstream.io/healthy"
	labelManagedBy     = "app.kubernetes.io/managed-by"
	managerName        = "bloodraven"

	specHashAnnotation = "shipstream.io/spec-hash"

	// RecloneAnnotation is set by an admin to trigger a reclone of a
	// specific site from the current primary via CLONE INSTANCE.
	RecloneAnnotation = "bloodraven.shipstream.io/reclone-site"

	mysqlPort   = 3306
	sidecarPort = 8080
)

// MysqlFailoverGroupReconciler reconciles a MysqlFailoverGroup object.
type MysqlFailoverGroupReconciler struct {
	client.Client
	Scheme   *runtime.Scheme
	Recorder record.EventRecorder
	Runner   *TopologyManagerRunner
	Tainter  platform.NodeTainter // optional, for taint cleanup during deletion

	// APIReader is an uncached reader for paths that cannot tolerate a stale
	// cache view — specifically waitForDeploymentRollout, which must observe
	// the post-patch Generation before it can meaningfully check rollout
	// progress. SetupWithManager defaults this to mgr.GetAPIReader() when it
	// has not been injected explicitly, so manager-backed production wiring
	// cannot silently fall back to the cached client. Tests that construct
	// the reconciler directly may leave it nil; the cached client is used in
	// that case.
	APIReader client.Reader

	// rolloutPollInterval overrides the production-default 2s cadence used by
	// waitForDeploymentRollout. Tests set this to a small value so the timeout
	// path exercises the ticker quickly.
	rolloutPollInterval time.Duration
}

// +kubebuilder:rbac:groups=shipstream.io,resources=mysqlfailovergroups,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=shipstream.io,resources=mysqlfailovergroups/status,verbs=get;update;patch
// +kubebuilder:rbac:groups=shipstream.io,resources=mysqlfailovergroups/finalizers,verbs=update
// +kubebuilder:rbac:groups="",resources=configmaps;services;persistentvolumeclaims,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=apps,resources=deployments,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=policy,resources=poddisruptionbudgets,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups="",resources=pods,verbs=get;list;watch;update;patch
// +kubebuilder:rbac:groups="",resources=nodes,verbs=get;list;watch;update;patch
// +kubebuilder:rbac:groups="",resources=secrets,verbs=get;list;watch
// +kubebuilder:rbac:groups="",resources=events,verbs=create;patch
// +kubebuilder:rbac:groups=externaldns.k8s.io,resources=dnsendpoints,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=batch,resources=jobs,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=batch,resources=cronjobs,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=shipstream.io,resources=mysqlbackups,verbs=get;list;watch;create;update;patch;delete
// Leader-election lease. The operator runs a single-replica deployment
// today but still uses leader election so a fresh pod doesn't step on
// a not-yet-drained predecessor. Without this marker,
// `kubectl apply -f config/rbac/` installs (the documented non-Helm
// path) produce an operator that crash-loops at startup. AUDIT M3.
// +kubebuilder:rbac:groups=coordination.k8s.io,resources=leases,verbs=get;list;watch;create;update;patch;delete

func (r *MysqlFailoverGroupReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	logger := log.FromContext(ctx)

	var fg v1alpha1.MysqlFailoverGroup
	if err := r.Get(ctx, req.NamespacedName, &fg); err != nil {
		if errors.IsNotFound(err) {
			return ctrl.Result{}, nil
		}
		return ctrl.Result{}, err
	}

	// Handle deletion with finalizer
	if !fg.DeletionTimestamp.IsZero() {
		if controllerutil.ContainsFinalizer(&fg, finalizerName) {
			logger.Info("CR being deleted, running finalizer cleanup", "name", fg.Name)

			// Stop the topology manager for this CR.
			if r.Runner != nil {
				r.Runner.StopManager(req.NamespacedName)
			}

			if err := r.handleDeletion(ctx, &fg); err != nil {
				return ctrl.Result{}, fmt.Errorf("handle deletion: %w", err)
			}
			controllerutil.RemoveFinalizer(&fg, finalizerName)
			if err := r.Update(ctx, &fg); err != nil {
				return ctrl.Result{}, fmt.Errorf("remove finalizer: %w", err)
			}
		}
		return ctrl.Result{}, nil
	}

	// Ensure finalizer is present.
	if !controllerutil.ContainsFinalizer(&fg, finalizerName) {
		controllerutil.AddFinalizer(&fg, finalizerName)
		if err := r.Update(ctx, &fg); err != nil {
			return ctrl.Result{}, fmt.Errorf("add finalizer: %w", err)
		}
	}

	logger.Info("reconciling MysqlFailoverGroup", "name", fg.Name)

	image := fg.Spec.Image
	if image == "" {
		image = defaultMySQLImage
	}

	// Validate that the referenced credential secret(s) exist and contain expected keys.
	if result, err := r.validateCredentialSecrets(ctx, &fg); err != nil || result.RequeueAfter > 0 {
		return result, err
	}

	// Reconcile ConfigMap
	if err := r.reconcileConfigMap(ctx, &fg); err != nil {
		return ctrl.Result{}, fmt.Errorf("reconcile configmap: %w", err)
	}

	// Reconcile init-users ConfigMap for MySQL user creation on first boot.
	if err := r.reconcileInitUsersConfigMap(ctx, &fg); err != nil {
		return ctrl.Result{}, fmt.Errorf("reconcile init-users configmap: %w", err)
	}

	// Reconcile backup assets (shared scripts ConfigMap + owned PVCs for
	// PVC-backed profiles). No-op when spec.backup is nil.
	if err := r.reconcileBackupAssets(ctx, &fg); err != nil {
		return ctrl.Result{}, fmt.Errorf("reconcile backup assets: %w", err)
	}

	// Skip Deployment reconciliation when an ordered update is in progress.
	// The UpdateController manages site-by-site Deployment updates to avoid
	// simultaneous restarts of both sites (which causes a TOTAL LOSS window).
	orderedUpdateActive := fg.Status.UpdatePhase != ""

	// When a topology manager is already running for this CR, defer Deployment
	// updates to the ordered update path: the reconciler firing on a CR spec
	// change must not restart both sites simultaneously. The runner's
	// checkSpecDrift compares the desired hash against the live Deployment
	// annotation, so leaving the existing Deployment untouched is what causes
	// drift to be observed and the ordered update to start. New Deployments
	// (initial bootstrap) are always created here since there's no manager yet.
	managerRunning := r.Runner != nil && r.Runner.HasManager(req.NamespacedName)

	// Reconcile per-site resources
	for i, site := range fg.Spec.Sites {
		serverID := int32(101 + i)

		if err := r.reconcilePVC(ctx, &fg, site); err != nil {
			return ctrl.Result{}, fmt.Errorf("reconcile pvc %s: %w", site.Name, err)
		}
		if !orderedUpdateActive {
			deferDeployment := false
			if managerRunning {
				var existing appsv1.Deployment
				deployNN := types.NamespacedName{
					Namespace: fg.Namespace,
					Name:      resourceName(fg.Name, site.Name),
				}
				if err := r.Get(ctx, deployNN, &existing); err == nil {
					deferDeployment = true
				} else if !errors.IsNotFound(err) {
					return ctrl.Result{}, fmt.Errorf("get deployment %s: %w", site.Name, err)
				}
			}
			if !deferDeployment {
				if err := r.reconcileDeployment(ctx, &fg, site, serverID, image); err != nil {
					return ctrl.Result{}, fmt.Errorf("reconcile deployment %s: %w", site.Name, err)
				}
			}
		}
		if err := r.reconcileSiteService(ctx, &fg, site); err != nil {
			return ctrl.Result{}, fmt.Errorf("reconcile site service %s: %w", site.Name, err)
		}
	}

	// Reconcile shared services
	if err := r.reconcilePrimaryService(ctx, &fg); err != nil {
		return ctrl.Result{}, fmt.Errorf("reconcile primary service: %w", err)
	}
	if err := r.reconcileReplicasService(ctx, &fg); err != nil {
		return ctrl.Result{}, fmt.Errorf("reconcile replicas service: %w", err)
	}
	if err := r.reconcilePDB(ctx, &fg); err != nil {
		return ctrl.Result{}, fmt.Errorf("reconcile pdb: %w", err)
	}

	// Reconcile MySQL users for credentials mode.
	if fg.Spec.UsesCredentials() {
		if err := r.reconcileCredentials(ctx, &fg); err != nil {
			logger.Error(err, "credential reconciliation failed, will retry")
			r.Recorder.Eventf(&fg, corev1.EventTypeWarning, "CredentialReconcileFailed",
				"failed to reconcile MySQL users: %v", err)
		}
	}

	// Drive the one-shot restore Job when spec.initFromBackup is set.
	// If a restore is still in flight we requeue early and skip pod
	// label sync. A parallel safety gate lives in the topology runner:
	// TopologyManagerRunner.sync() calls
	// TopologyManager.SetAutoBootstrapSuppressed(restoreInFlight) every
	// 30s, which prevents the fresh-deploy auto-clone path in
	// applyCrossSiteAction from racing the restore Job for the primary
	// data directory. Together the two gates ensure the replica is only
	// cloned after the primary has been loaded from the dump.
	restoreRequeue, err := r.reconcileRestoreJob(ctx, &fg)
	if err != nil {
		return ctrl.Result{}, fmt.Errorf("reconcile restore job: %w", err)
	}
	if restoreRequeue > 0 {
		return ctrl.Result{RequeueAfter: restoreRequeue}, nil
	}

	// Drive the re-triggerable in-place restore state machine when
	// spec.restoreInPlace is set. Unlike reconcileRestoreJob (one-shot,
	// greenfield), this path operates on a live cluster and coordinates
	// with syncPodLabels (for Service-layer fencing) and the topology
	// runner (for SetTopologyFrozen). We run it BEFORE syncPodLabels so
	// label transitions land on the same reconcile that advances the
	// state machine.
	if inPlaceRequeue, err := r.reconcileInPlaceRestore(ctx, &fg); err != nil {
		return ctrl.Result{}, fmt.Errorf("reconcile in-place restore: %w", err)
	} else if inPlaceRequeue > 0 {
		// Still sync labels before we bounce: the fence needs pod labels
		// re-applied on the same pass that transitions into Fencing.
		if err := r.syncPodLabels(ctx, &fg); err != nil {
			return ctrl.Result{}, fmt.Errorf("sync pod labels during in-place restore: %w", err)
		}
		return ctrl.Result{RequeueAfter: inPlaceRequeue}, nil
	}

	// Drive the planned-failover state machine when
	// spec / status / annotation indicate a graceful switchover is armed
	// or in flight. This must run AFTER reconcileInPlaceRestore (the two
	// guard against each other) and BEFORE syncPodLabels (so the source
	// primary's role label is stripped on the same reconcile that
	// stamps Draining). The state machine is driven by
	// status.plannedFailover.phase, not by the annotation alone, so
	// operator restarts resume cleanly.
	if pfRequeue, err := r.reconcilePlannedFailover(ctx, &fg); err != nil {
		return ctrl.Result{}, fmt.Errorf("reconcile planned failover: %w", err)
	} else if pfRequeue > 0 {
		if err := r.syncPodLabels(ctx, &fg); err != nil {
			return ctrl.Result{}, fmt.Errorf("sync pod labels during planned failover: %w", err)
		}
		return ctrl.Result{RequeueAfter: pfRequeue}, nil
	}

	// Reconcile scheduled backups (one CronJob per schedule entry).
	if err := r.reconcileBackupSchedules(ctx, &fg); err != nil {
		return ctrl.Result{}, fmt.Errorf("reconcile backup schedules: %w", err)
	}

	// Reconcile scheduled verification runs (one CronJob per profile
	// whose .verification.enabled=true).
	if err := r.reconcileVerificationSchedules(ctx, &fg); err != nil {
		return ctrl.Result{}, fmt.Errorf("reconcile verification schedules: %w", err)
	}

	// Roll up backup status (per-schedule LastBackupTime etc). The
	// returned duration is the minimum wake-up across schedules —
	// non-zero when a pending retry backoff hasn't elapsed yet, so
	// the reconciler wakes up exactly when the retry is due.
	backupRequeue, err := r.updateBackupStatus(ctx, &fg)
	if err != nil {
		return ctrl.Result{}, fmt.Errorf("update backup status: %w", err)
	}

	// Sync pod labels based on status
	if err := r.syncPodLabels(ctx, &fg); err != nil {
		return ctrl.Result{}, fmt.Errorf("sync pod labels: %w", err)
	}

	if backupRequeue > 0 {
		return ctrl.Result{RequeueAfter: backupRequeue}, nil
	}
	return ctrl.Result{}, nil
}

func (r *MysqlFailoverGroupReconciler) SetupWithManager(mgr ctrl.Manager) error {
	// Defend against wiring paths that forget to inject APIReader: under a
	// manager the uncached reader is always available, and falling back to
	// the cached client would re-introduce the rolling-update rollout race
	// that waitForDeploymentRollout exists to close.
	if r.APIReader == nil {
		r.APIReader = mgr.GetAPIReader()
	}
	return ctrl.NewControllerManagedBy(mgr).
		For(&v1alpha1.MysqlFailoverGroup{}).
		Owns(&appsv1.Deployment{}).
		Owns(&corev1.Service{}).
		Owns(&corev1.ConfigMap{}).
		Owns(&corev1.PersistentVolumeClaim{}).
		Owns(&batchv1.Job{}).
		Owns(&batchv1.CronJob{}).
		Watches(&corev1.Secret{}, handler.EnqueueRequestsFromMapFunc(r.secretToFailoverGroup)).
		Watches(&v1alpha1.MysqlBackup{}, handler.EnqueueRequestsFromMapFunc(r.mapBackupToGroup)).
		Complete(r)
}

// secretToFailoverGroup maps a Secret change to the MysqlFailoverGroups that reference it
// (via spec.secretName, spec.credentials.*, or spec.tls.secretName), triggering
// reconciliation on credential or cert rotation.
func (r *MysqlFailoverGroupReconciler) secretToFailoverGroup(ctx context.Context, obj client.Object) []ctrl.Request {
	var fgList v1alpha1.MysqlFailoverGroupList
	if err := r.List(ctx, &fgList, client.InNamespace(obj.GetNamespace())); err != nil {
		log.FromContext(ctx).Error(err, "unable to list MysqlFailoverGroups for Secret watch")
		return nil
	}
	secretName := obj.GetName()
	var requests []ctrl.Request
	for _, fg := range fgList.Items {
		if fg.Spec.TLS != nil && fg.Spec.TLS.SecretName == secretName {
			requests = append(requests, ctrl.Request{
				NamespacedName: types.NamespacedName{Name: fg.Name, Namespace: fg.Namespace},
			})
			continue
		}
		for _, ref := range fg.Spec.AllReferencedSecretNames() {
			if ref == secretName {
				requests = append(requests, ctrl.Request{
					NamespacedName: types.NamespacedName{Name: fg.Name, Namespace: fg.Namespace},
				})
				break
			}
		}
	}
	return requests
}

// mapBackupToGroup enqueues the MysqlFailoverGroup referenced by a
// MysqlBackup so that spec.status.backupSchedules[] stays fresh.
func (r *MysqlFailoverGroupReconciler) mapBackupToGroup(_ context.Context, obj client.Object) []reconcile.Request {
	backup, ok := obj.(*v1alpha1.MysqlBackup)
	if !ok || backup.Spec.FailoverGroupRef.Name == "" {
		return nil
	}
	return []reconcile.Request{
		{NamespacedName: types.NamespacedName{Namespace: backup.Namespace, Name: backup.Spec.FailoverGroupRef.Name}},
	}
}

func (r *MysqlFailoverGroupReconciler) handleDeletion(ctx context.Context, fg *v1alpha1.MysqlFailoverGroup) error {
	logger := log.FromContext(ctx)
	logger.Info("starting graceful shutdown", "fg", fg.Name)

	// Record event
	r.Recorder.Event(fg, corev1.EventTypeNormal, "GracefulShutdown", "Starting graceful shutdown sequence")

	// Remove taints for both site selectors
	if r.Tainter != nil {
		for _, site := range fg.Spec.Sites {
			selector := fmt.Sprintf("shipstream.io/failover-group=%s,shipstream.io/site=%s", fg.Name, site.Name)
			if err := r.Tainter.SetTaint(ctx, selector, fg.Name, false); err != nil {
				logger.Error(err, "failed to remove taint during shutdown", "site", site.Name)
				// Continue cleanup despite taint removal failure
			}
		}
	}

	// DNSEndpoint has an owner reference and will be garbage-collected automatically.
	logger.Info("CR deleted — DNSEndpoint will be garbage-collected",
		"hostname", fg.Spec.DNS.Hostname)

	r.Recorder.Event(fg, corev1.EventTypeNormal, "GracefulShutdown", "Graceful shutdown complete, removing finalizer")
	return nil
}

// validateCredentialSecrets validates that the referenced credential
// secrets exist and contain the expected keys for the active mode.
func (r *MysqlFailoverGroupReconciler) validateCredentialSecrets(ctx context.Context, fg *v1alpha1.MysqlFailoverGroup) (ctrl.Result, error) {
	if fg.Spec.UsesCredentials() {
		return r.validateCredentialsMode(ctx, fg)
	}
	return r.validateLegacySecret(ctx, fg)
}

func (r *MysqlFailoverGroupReconciler) validateLegacySecret(ctx context.Context, fg *v1alpha1.MysqlFailoverGroup) (ctrl.Result, error) {
	var secret corev1.Secret
	secretKey := types.NamespacedName{Namespace: fg.Namespace, Name: fg.Spec.SecretName}
	if err := r.Get(ctx, secretKey, &secret); err != nil {
		if !errors.IsNotFound(err) {
			return ctrl.Result{}, fmt.Errorf("get secret %s: %w", fg.Spec.SecretName, err)
		}
		r.Recorder.Eventf(fg, corev1.EventTypeWarning, "SecretNotFound",
			"secret %q not found", fg.Spec.SecretName)
		return ctrl.Result{RequeueAfter: 30 * time.Second}, nil
	}
	if _, ok := secret.Data["dsn"]; !ok {
		r.Recorder.Eventf(fg, corev1.EventTypeWarning, "SecretMissingKey",
			"secret %q does not contain required key 'dsn'", fg.Spec.SecretName)
		return ctrl.Result{RequeueAfter: 30 * time.Second}, nil
	}
	return ctrl.Result{}, nil
}

func (r *MysqlFailoverGroupReconciler) validateCredentialsMode(ctx context.Context, fg *v1alpha1.MysqlFailoverGroup) (ctrl.Result, error) {
	operatorSecretName := fg.Spec.Credentials.OperatorSecret
	var secret corev1.Secret
	secretKey := types.NamespacedName{Namespace: fg.Namespace, Name: operatorSecretName}
	if err := r.Get(ctx, secretKey, &secret); err != nil {
		if !errors.IsNotFound(err) {
			return ctrl.Result{}, fmt.Errorf("get operator secret %s: %w", operatorSecretName, err)
		}
		r.Recorder.Eventf(fg, corev1.EventTypeWarning, "SecretNotFound",
			"operator secret %q not found", operatorSecretName)
		return ctrl.Result{RequeueAfter: 30 * time.Second}, nil
	}
	for _, key := range []string{"username", "password", "MYSQL_ROOT_PASSWORD"} {
		if _, ok := secret.Data[key]; !ok {
			r.Recorder.Eventf(fg, corev1.EventTypeWarning, "SecretMissingKey",
				"operator secret %q missing required key %q", operatorSecretName, key)
			return ctrl.Result{RequeueAfter: 30 * time.Second}, nil
		}
	}

	// Validate optional credential secrets have required keys.
	type optionalSecret struct {
		name string
		role string
	}
	for _, opt := range []optionalSecret{
		{fg.Spec.Credentials.AppSecret, "app"},
		{fg.Spec.Credentials.ReadOnlySecret, "read-only"},
		{fg.Spec.Credentials.MonitorSecret, "monitor"},
		{fg.Spec.Credentials.BackupSecret, "backup"},
	} {
		if opt.name == "" {
			continue
		}
		var s corev1.Secret
		if err := r.Get(ctx, types.NamespacedName{Namespace: fg.Namespace, Name: opt.name}, &s); err != nil {
			if !errors.IsNotFound(err) {
				return ctrl.Result{}, fmt.Errorf("get %s secret %s: %w", opt.role, opt.name, err)
			}
			r.Recorder.Eventf(fg, corev1.EventTypeWarning, "SecretNotFound",
				"%s secret %q not found", opt.role, opt.name)
			return ctrl.Result{RequeueAfter: 30 * time.Second}, nil
		}
		for _, key := range []string{"username", "password"} {
			if _, ok := s.Data[key]; !ok {
				r.Recorder.Eventf(fg, corev1.EventTypeWarning, "SecretMissingKey",
					"%s secret %q missing required key %q", opt.role, opt.name, key)
				return ctrl.Result{RequeueAfter: 30 * time.Second}, nil
			}
		}
	}
	return ctrl.Result{}, nil
}

// resourceName returns a deterministic name for a per-site resource.
func resourceName(fgName, siteName string) string {
	return fmt.Sprintf("mysql-%s-%s", fgName, siteName)
}

// commonLabels returns the labels applied to all resources for a failover group/site.
func commonLabels(fgName, siteName string) map[string]string {
	return map[string]string{
		labelAppName:       "mysql",
		labelInstance:      fgName,
		labelFailoverGroup: fgName,
		labelSite:          siteName,
		labelManagedBy:     managerName,
	}
}

func (r *MysqlFailoverGroupReconciler) reconcileConfigMap(ctx context.Context, fg *v1alpha1.MysqlFailoverGroup) error {
	cm := &corev1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{
			Name:      fmt.Sprintf("mysql-%s-config", fg.Name),
			Namespace: fg.Namespace,
		},
	}

	_, err := controllerutil.CreateOrUpdate(ctx, r.Client, cm, func() error {
		if err := controllerutil.SetControllerReference(fg, cm, r.Scheme); err != nil {
			return err
		}
		cm.Labels = map[string]string{
			labelAppName:       "mysql",
			labelInstance:      fg.Name,
			labelFailoverGroup: fg.Name,
			labelManagedBy:     managerName,
		}
		cm.Data = map[string]string{
			"my.cnf": generateMyCnf(fg),
		}
		return nil
	})
	return err
}

func generateMyCnf(fg *v1alpha1.MysqlFailoverGroup) string {
	// Base config
	settings := map[string]string{
		"gtid-mode":                      "ON",
		"enforce-gtid-consistency":       "ON",
		"log-bin":                        "/var/lib/mysql/mysql-bin",
		"log-replica-updates":            "ON",
		"skip-replica-start":             "ON",
		"sync-binlog":                    "1",
		"binlog-expire-logs-seconds":     "1209600",
		"plugin-load-add":                "mysql_clone.so",
		"default-storage-engine":         "InnoDB",
		"innodb-flush-method":            "O_DIRECT",
		"innodb-flush-log-at-trx-commit": "2",
		"innodb-file-per-table":          "1",
		"max-allowed-packet":             "64M",
		"max-connect-errors":             "1000000",
		"skip-name-resolve":              "",
		"max-connections":                "500",
		"thread-cache-size":              "50",
		"character-set-server":           "utf8mb4",
		"collation-server":               "utf8mb4_unicode_ci",
	}

	// clone_ddl_timeout was removed in MySQL 9.x; only set it for older versions.
	// TODO: detect MySQL version and conditionally apply clone settings.

	// Apply TLS settings if configured
	if fg.Spec.TLS != nil {
		settings["ssl-ca"] = "/etc/mysql/tls/ca.crt"
		settings["ssl-cert"] = "/etc/mysql/tls/tls.crt"
		settings["ssl-key"] = "/etc/mysql/tls/tls.key"
		settings["require-secure-transport"] = "ON"
	}

	// PITR: set max_binlog_size so the rotation cadence (and therefore
	// the archival cadence — and therefore the RPO window) is driven by
	// the CRD rather than MySQL's 1 GB default. Placed before user
	// overrides so operators can still override via spec.mysqlConf if
	// they want something different from what spec.backup.pitr configures.
	if fg.Spec.Backup != nil && fg.Spec.Backup.PITR != nil && fg.Spec.Backup.PITR.Enabled {
		maxSize := fg.Spec.Backup.PITR.MaxBinlogSize
		if maxSize == "" {
			maxSize = defaultPITRMaxBinlogSize
		}
		settings["max-binlog-size"] = maxSize
	}

	// Apply user overrides
	for k, v := range fg.Spec.MysqlConf {
		settings[k] = v
	}

	// Build sorted output for deterministic ConfigMap content
	keys := make([]string, 0, len(settings))
	for k := range settings {
		keys = append(keys, k)
	}
	sort.Strings(keys)

	var b strings.Builder
	b.WriteString("[mysqld]\n")
	for _, k := range keys {
		v := settings[k]
		if v == "" {
			b.WriteString(k + "\n")
		} else {
			b.WriteString(k + "=" + v + "\n")
		}
	}

	return b.String()
}

func (r *MysqlFailoverGroupReconciler) reconcilePVC(ctx context.Context, fg *v1alpha1.MysqlFailoverGroup, site v1alpha1.SiteSpec) error {
	pvc := &corev1.PersistentVolumeClaim{
		ObjectMeta: metav1.ObjectMeta{
			Name:      resourceName(fg.Name, site.Name) + "-data",
			Namespace: fg.Namespace,
		},
	}

	_, err := controllerutil.CreateOrUpdate(ctx, r.Client, pvc, func() error {
		if err := controllerutil.SetControllerReference(fg, pvc, r.Scheme); err != nil {
			return err
		}
		pvc.Labels = commonLabels(fg.Name, site.Name)

		// Only set spec fields on creation (PVC spec is largely immutable)
		if pvc.CreationTimestamp.IsZero() {
			pvc.Spec = corev1.PersistentVolumeClaimSpec{
				AccessModes:      []corev1.PersistentVolumeAccessMode{corev1.ReadWriteOnce},
				StorageClassName: &site.Storage.StorageClassName,
				Resources: corev1.VolumeResourceRequirements{
					Requests: corev1.ResourceList{
						corev1.ResourceStorage: site.Storage.Size,
					},
				},
			}
		}
		return nil
	})
	return err
}

func (r *MysqlFailoverGroupReconciler) reconcileDeployment(ctx context.Context, fg *v1alpha1.MysqlFailoverGroup, site v1alpha1.SiteSpec, serverID int32, image string) error {
	deploy := &appsv1.Deployment{
		ObjectMeta: metav1.ObjectMeta{
			Name:      resourceName(fg.Name, site.Name),
			Namespace: fg.Namespace,
		},
	}

	// Fetch TLS Secret data so certificate changes are reflected in the spec hash.
	var tlsSecretData map[string][]byte
	if fg.Spec.TLS != nil {
		var tlsSecret corev1.Secret
		tlsSecretKey := types.NamespacedName{Namespace: fg.Namespace, Name: fg.Spec.TLS.SecretName}
		if err := r.Get(ctx, tlsSecretKey, &tlsSecret); err != nil {
			if !errors.IsNotFound(err) {
				return fmt.Errorf("get TLS secret %s: %w", fg.Spec.TLS.SecretName, err)
			}
			log.FromContext(ctx).Info("TLS secret not found yet, skipping TLS hash", "secret", fg.Spec.TLS.SecretName)
		} else {
			tlsSecretData = tlsSecret.Data
		}
	}

	// Fetch credential secret data so password changes trigger rolling restart.
	var credSecretData map[string]map[string][]byte
	if fg.Spec.UsesCredentials() {
		credSecretData = make(map[string]map[string][]byte)
		for _, name := range fg.Spec.AllReferencedSecretNames() {
			var s corev1.Secret
			if err := r.Get(ctx, types.NamespacedName{Namespace: fg.Namespace, Name: name}, &s); err == nil {
				credSecretData[name] = s.Data
			}
		}
	}
	specHash := computeSpecHash(fg, site, tlsSecretData, credSecretData)

	_, err := controllerutil.CreateOrUpdate(ctx, r.Client, deploy, func() error {
		if err := controllerutil.SetControllerReference(fg, deploy, r.Scheme); err != nil {
			return err
		}

		labels := commonLabels(fg.Name, site.Name)
		deploy.Labels = labels

		// Store spec hash as annotation for drift detection.
		if deploy.Annotations == nil {
			deploy.Annotations = make(map[string]string)
		}
		deploy.Annotations[specHashAnnotation] = specHash

		var replicas int32 = 1
		deploy.Spec.Replicas = &replicas
		deploy.Spec.Strategy = appsv1.DeploymentStrategy{
			Type: appsv1.RecreateDeploymentStrategyType,
		}
		deploy.Spec.Selector = &metav1.LabelSelector{
			MatchLabels: map[string]string{
				labelAppName:  "mysql",
				labelInstance: fg.Name,
				labelSite:     site.Name,
			},
		}

		podLabels := make(map[string]string)
		for k, v := range fg.Spec.PodLabels {
			podLabels[k] = v
		}
		// Operator labels take precedence over user-supplied labels.
		for k, v := range labels {
			podLabels[k] = v
		}
		// Static defaults — syncPodLabels sets the real values on live pods.
		// These must NOT depend on fg.Status.ActiveSite or any mutable state,
		// otherwise every status change triggers a Deployment rollout.
		podLabels[labelRole] = "replica"
		podLabels[labelHealthy] = "no"

		sidecarImage := fg.Spec.SidecarImage

		configMapName := fmt.Sprintf("mysql-%s-config", fg.Name)

		// Build the list of peer sidecar addresses. The sidecar treats
		// "all peers unreachable" as one half of the self-fencing
		// quorum, so every non-self site needs to be listed here.
		peerNames := fg.Spec.PeerSiteNames(site.Name)
		peerAddrs := make([]string, len(peerNames))
		for i, name := range peerNames {
			peerAddrs[i] = fmt.Sprintf("mysql-%s-%s.%s.svc.cluster.local:%d",
				fg.Name, name, fg.Namespace, sidecarPort)
		}
		peerAddresses := strings.Join(peerAddrs, ",")

		bloodravenAddress := fg.Spec.Sidecar.BloodravenAddress
		if bloodravenAddress == "" {
			bloodravenAddress = fmt.Sprintf("bloodraven.%s.svc.cluster.local:8082", fg.Namespace)
		}

		leaseTimeout := "20s"
		if fg.Spec.Sidecar.LeaseTimeout != nil {
			leaseTimeout = fg.Spec.Sidecar.LeaseTimeout.Duration.String()
		}

		peerCheckInterval := "5s"
		if fg.Spec.Sidecar.PeerCheckInterval != nil {
			peerCheckInterval = fg.Spec.Sidecar.PeerCheckInterval.Duration.String()
		}

		volumeMounts := []corev1.VolumeMount{
			{Name: "data", MountPath: "/var/lib/mysql"},
			{Name: "conf", MountPath: "/etc/mysql/conf.d"},
		}
		if fg.Spec.TLS != nil {
			volumeMounts = append(volumeMounts, corev1.VolumeMount{
				Name:      "tls",
				MountPath: "/etc/mysql/tls",
				ReadOnly:  true,
			})
		}

		sidecarVolumeMounts := []corev1.VolumeMount{}
		if fg.Spec.TLS != nil {
			sidecarVolumeMounts = append(sidecarVolumeMounts, corev1.VolumeMount{
				Name:      "tls",
				MountPath: "/etc/mysql/tls",
				ReadOnly:  true,
			})
		}

		mysqlContainer := corev1.Container{
			Name:  "mysql",
			Image: image,
			Ports: []corev1.ContainerPort{
				{
					Name:          "mysql",
					ContainerPort: mysqlPort,
					Protocol:      corev1.ProtocolTCP,
				},
			},
			VolumeMounts: volumeMounts,
			Resources:    site.Resources,
			LivenessProbe: &corev1.Probe{
				ProbeHandler: corev1.ProbeHandler{
					TCPSocket: &corev1.TCPSocketAction{
						Port: intstr.FromInt32(mysqlPort),
					},
				},
				InitialDelaySeconds: 30,
				PeriodSeconds:       10,
			},
			ReadinessProbe: &corev1.Probe{
				ProbeHandler: corev1.ProbeHandler{
					TCPSocket: &corev1.TCPSocketAction{
						Port: intstr.FromInt32(mysqlPort),
					},
				},
				InitialDelaySeconds: 5,
				PeriodSeconds:       5,
			},
		}

		operatorSecretName := fg.Spec.EffectiveOperatorSecretName()

		sidecarEnv := []corev1.EnvVar{
			{Name: "MY_POD_NAME", ValueFrom: &corev1.EnvVarSource{
				FieldRef: &corev1.ObjectFieldSelector{FieldPath: "metadata.name"},
			}},
			{Name: "LISTEN_ADDR", Value: fmt.Sprintf(":%d", sidecarPort)},
			{Name: "PEER_ADDRESSES", Value: peerAddresses},
			{Name: "BLOODRAVEN_ADDRESS", Value: bloodravenAddress},
			{Name: "MY_SITE", Value: site.Name},
			{Name: "POD_NAMESPACE", ValueFrom: &corev1.EnvVarSource{
				FieldRef: &corev1.ObjectFieldSelector{FieldPath: "metadata.namespace"},
			}},
			{Name: "FAILOVER_GROUP", Value: fg.Name},
			{Name: "LEASE_TIMEOUT", Value: leaseTimeout},
			{Name: "PEER_CHECK_INTERVAL", Value: peerCheckInterval},
		}

		if fg.Spec.UsesCredentials() {
			mysqlContainer.Env = []corev1.EnvVar{
				{Name: "MYSQL_ROOT_PASSWORD", ValueFrom: &corev1.EnvVarSource{
					SecretKeyRef: &corev1.SecretKeySelector{
						LocalObjectReference: corev1.LocalObjectReference{Name: operatorSecretName},
						Key:                  "MYSQL_ROOT_PASSWORD",
					},
				}},
			}
			mysqlContainer.Lifecycle = &corev1.Lifecycle{
				PreStop: &corev1.LifecycleHandler{
					Exec: &corev1.ExecAction{
						Command: []string{
							"sh", "-c",
							`mysql -u "$(cat /etc/mysql/creds/operator/username)" -p"$(cat /etc/mysql/creds/operator/password)" -e 'SET GLOBAL super_read_only=ON' 2>/dev/null || true`,
						},
					},
				},
			}
			mysqlContainer.VolumeMounts = append(mysqlContainer.VolumeMounts,
				corev1.VolumeMount{Name: "creds-operator", MountPath: "/etc/mysql/creds/operator", ReadOnly: true},
				corev1.VolumeMount{Name: "init-users", MountPath: "/docker-entrypoint-initdb.d", ReadOnly: true},
			)
			appendCredVolume := func(name string) {
				mysqlContainer.VolumeMounts = append(mysqlContainer.VolumeMounts,
					corev1.VolumeMount{Name: "creds-" + name, MountPath: "/etc/mysql/creds/" + name, ReadOnly: true})
			}
			if fg.Spec.Credentials.AppSecret != "" {
				appendCredVolume("app")
			}
			if fg.Spec.Credentials.ReadOnlySecret != "" {
				appendCredVolume("readonly")
			}
			if fg.Spec.Credentials.MonitorSecret != "" {
				appendCredVolume("monitor")
			}
			if fg.Spec.Credentials.BackupSecret != "" {
				appendCredVolume("backup")
			}

			sidecarEnv = append([]corev1.EnvVar{
				{Name: "MYSQL_USER", ValueFrom: &corev1.EnvVarSource{
					SecretKeyRef: &corev1.SecretKeySelector{
						LocalObjectReference: corev1.LocalObjectReference{Name: operatorSecretName},
						Key:                  "username",
					},
				}},
				{Name: "MYSQL_PASSWORD", ValueFrom: &corev1.EnvVarSource{
					SecretKeyRef: &corev1.SecretKeySelector{
						LocalObjectReference: corev1.LocalObjectReference{Name: operatorSecretName},
						Key:                  "password",
					},
				}},
			}, sidecarEnv...)
		} else {
			mysqlContainer.EnvFrom = []corev1.EnvFromSource{
				{
					SecretRef: &corev1.SecretEnvSource{
						LocalObjectReference: corev1.LocalObjectReference{
							Name: fg.Spec.SecretName,
						},
					},
				},
			}
			mysqlContainer.VolumeMounts = append(mysqlContainer.VolumeMounts,
				corev1.VolumeMount{Name: "init-users", MountPath: "/docker-entrypoint-initdb.d", ReadOnly: true},
			)
			mysqlContainer.Lifecycle = &corev1.Lifecycle{
				PreStop: &corev1.LifecycleHandler{
					Exec: &corev1.ExecAction{
						// MYSQL_PWD keeps the password off argv so it
						// doesn't show up in /proc/<pid>/cmdline where
						// anything sharing the pod PID namespace could
						// read it. `mysql --help` explicitly warns
						// against the -p"$..." argv form (AUDIT M1).
						Command: []string{
							"sh", "-c",
							`MYSQL_PWD="${MYSQL_ROOT_PASSWORD}" mysql -u root -e 'SET GLOBAL super_read_only=ON' 2>/dev/null || true`,
						},
					},
				},
			}

			sidecarEnv = append([]corev1.EnvVar{
				{Name: "MYSQL_DSN", ValueFrom: &corev1.EnvVarSource{
					SecretKeyRef: &corev1.SecretKeySelector{
						LocalObjectReference: corev1.LocalObjectReference{Name: fg.Spec.SecretName},
						Key:                  "dsn",
					},
				}},
			}, sidecarEnv...)
		}

		// PITR sidecar wiring (no-op when spec.backup.pitr.enabled=false).
		pitrFrags, err := buildPITRSidecarFragments(fg)
		if err != nil {
			return fmt.Errorf("build pitr sidecar fragments: %w", err)
		}
		sidecarEnv = append(sidecarEnv, pitrFrags.SidecarEnv...)
		sidecarVolumeMounts = append(sidecarVolumeMounts, pitrFrags.SidecarVolumeMounts...)
		sidecarContainer := corev1.Container{
			Name:  "sidecar",
			Image: sidecarImage,
			Ports: []corev1.ContainerPort{
				{
					Name:          "sidecar",
					ContainerPort: sidecarPort,
					Protocol:      corev1.ProtocolTCP,
				},
			},
			Env:          sidecarEnv,
			VolumeMounts: sidecarVolumeMounts,
			Resources:    fg.Spec.SidecarResources,
			LivenessProbe: &corev1.Probe{
				ProbeHandler: corev1.ProbeHandler{
					HTTPGet: &corev1.HTTPGetAction{
						Path: "/health",
						Port: intstr.FromInt32(sidecarPort),
					},
				},
				InitialDelaySeconds: 5,
				PeriodSeconds:       10,
			},
			ReadinessProbe: &corev1.Probe{
				ProbeHandler: corev1.ProbeHandler{
					HTTPGet: &corev1.HTTPGetAction{
						Path: "/health",
						Port: intstr.FromInt32(sidecarPort),
					},
				},
				InitialDelaySeconds: 3,
				PeriodSeconds:       5,
			},
		}

		containers := []corev1.Container{mysqlContainer, sidecarContainer}

		volumes := []corev1.Volume{
			{
				Name: "data",
				VolumeSource: corev1.VolumeSource{
					PersistentVolumeClaim: &corev1.PersistentVolumeClaimVolumeSource{
						ClaimName: resourceName(fg.Name, site.Name) + "-data",
					},
				},
			},
			{
				Name: "conf",
				VolumeSource: corev1.VolumeSource{
					EmptyDir: &corev1.EmptyDirVolumeSource{},
				},
			},
			{
				Name: "config",
				VolumeSource: corev1.VolumeSource{
					ConfigMap: &corev1.ConfigMapVolumeSource{
						LocalObjectReference: corev1.LocalObjectReference{
							Name: configMapName,
						},
					},
				},
			},
			{
				Name: "init-users",
				VolumeSource: corev1.VolumeSource{
					ConfigMap: &corev1.ConfigMapVolumeSource{
						LocalObjectReference: corev1.LocalObjectReference{
							Name: fmt.Sprintf("mysql-%s-init-users", fg.Name),
						},
						DefaultMode: int32Ptr(0555),
					},
				},
			},
		}

		if fg.Spec.TLS != nil {
			volumes = append(volumes, corev1.Volume{
				Name: "tls",
				VolumeSource: corev1.VolumeSource{
					Secret: &corev1.SecretVolumeSource{
						SecretName: fg.Spec.TLS.SecretName,
					},
				},
			})
		}

		if fg.Spec.UsesCredentials() {
			credSecretVolume := func(volName, secretName string) corev1.Volume {
				return corev1.Volume{
					Name: volName,
					VolumeSource: corev1.VolumeSource{
						Secret: &corev1.SecretVolumeSource{
							SecretName: secretName,
							Items: []corev1.KeyToPath{
								{Key: "username", Path: "username"},
								{Key: "password", Path: "password"},
							},
						},
					},
				}
			}
			volumes = append(volumes,
				credSecretVolume("creds-operator", fg.Spec.Credentials.OperatorSecret),
			)
			if s := fg.Spec.Credentials.AppSecret; s != "" {
				volumes = append(volumes, credSecretVolume("creds-app", s))
			}
			if s := fg.Spec.Credentials.ReadOnlySecret; s != "" {
				volumes = append(volumes, credSecretVolume("creds-readonly", s))
			}
			if s := fg.Spec.Credentials.MonitorSecret; s != "" {
				volumes = append(volumes, credSecretVolume("creds-monitor", s))
			}
			if s := fg.Spec.Credentials.BackupSecret; s != "" {
				volumes = append(volumes, credSecretVolume("creds-backup", s))
			}
		}

		// PITR contributions (AWS creds secret or backup PVC volume).
		volumes = append(volumes, pitrFrags.PodVolumes...)

		podAnnotations := make(map[string]string)
		for k, v := range fg.Spec.PodAnnotations {
			podAnnotations[k] = v
		}
		// Operator annotations take precedence over user-supplied annotations.
		podAnnotations[specHashAnnotation] = specHash

		deploy.Spec.Template = corev1.PodTemplateSpec{
			ObjectMeta: metav1.ObjectMeta{
				Labels:      podLabels,
				Annotations: podAnnotations,
			},
			Spec: corev1.PodSpec{
				NodeSelector: map[string]string{
					"topology.kubernetes.io/zone": site.Zone,
				},
				// MySQL pods must tolerate the db-readonly taint since they
				// run on both primary and replica nodes.
				Tolerations: []corev1.Toleration{
					{
						Key:      platform.TaintKeyForGroup(fg.Name),
						Operator: corev1.TolerationOpExists,
						Effect:   corev1.TaintEffectNoSchedule,
					},
				},
				InitContainers: append([]corev1.Container{
					{
						Name:  "init",
						Image: image,
						Command: []string{
							"sh", "-c",
							fmt.Sprintf("cp /etc/mysql/config-map/* /etc/mysql/conf.d/ && printf '[mysqld]\\nserver-id=%d\\n' > /etc/mysql/conf.d/server-id.cnf", serverID),
						},
						VolumeMounts: []corev1.VolumeMount{
							{Name: "config", MountPath: "/etc/mysql/config-map"},
							{Name: "conf", MountPath: "/etc/mysql/conf.d"},
						},
					},
				}, fg.Spec.ExtraInitContainers...),
				Containers: append(containers, fg.Spec.ExtraContainers...),
				Volumes:    volumes,
			},
		}
		if fg.Spec.TerminationGracePeriodSeconds != nil {
			deploy.Spec.Template.Spec.TerminationGracePeriodSeconds = fg.Spec.TerminationGracePeriodSeconds
		}
		return nil
	})
	return err
}

func (r *MysqlFailoverGroupReconciler) reconcileSiteService(ctx context.Context, fg *v1alpha1.MysqlFailoverGroup, site v1alpha1.SiteSpec) error {
	svc := &corev1.Service{
		ObjectMeta: metav1.ObjectMeta{
			Name:      resourceName(fg.Name, site.Name),
			Namespace: fg.Namespace,
		},
	}

	_, err := controllerutil.CreateOrUpdate(ctx, r.Client, svc, func() error {
		if err := controllerutil.SetControllerReference(fg, svc, r.Scheme); err != nil {
			return err
		}
		svc.Labels = commonLabels(fg.Name, site.Name)
		applyServiceAnnotations(svc, fg.Spec.ServiceTemplate)
		svc.Spec = corev1.ServiceSpec{
			Type: serviceType(fg.Spec.ServiceTemplate),
			Selector: map[string]string{
				labelAppName:  "mysql",
				labelInstance: fg.Name,
				labelSite:     site.Name,
			},
			Ports: []corev1.ServicePort{
				{
					Name:       "mysql",
					Port:       mysqlPort,
					TargetPort: intstr.FromInt32(mysqlPort),
					Protocol:   corev1.ProtocolTCP,
				},
				{
					Name:       "sidecar",
					Port:       sidecarPort,
					TargetPort: intstr.FromInt32(sidecarPort),
					Protocol:   corev1.ProtocolTCP,
				},
			},
		}
		return nil
	})
	return err
}

func (r *MysqlFailoverGroupReconciler) reconcilePrimaryService(ctx context.Context, fg *v1alpha1.MysqlFailoverGroup) error {
	svc := &corev1.Service{
		ObjectMeta: metav1.ObjectMeta{
			Name:      fmt.Sprintf("mysql-%s-primary", fg.Name),
			Namespace: fg.Namespace,
		},
	}

	_, err := controllerutil.CreateOrUpdate(ctx, r.Client, svc, func() error {
		if err := controllerutil.SetControllerReference(fg, svc, r.Scheme); err != nil {
			return err
		}
		svc.Labels = map[string]string{
			labelAppName:       "mysql",
			labelInstance:      fg.Name,
			labelFailoverGroup: fg.Name,
			labelManagedBy:     managerName,
		}
		applyServiceAnnotations(svc, fg.Spec.ServiceTemplate)
		svc.Spec = corev1.ServiceSpec{
			Type: serviceType(fg.Spec.ServiceTemplate),
			Selector: map[string]string{
				labelInstance: fg.Name,
				labelRole:     "primary",
			},
			Ports: []corev1.ServicePort{
				{
					Name:       "mysql",
					Port:       mysqlPort,
					TargetPort: intstr.FromInt32(mysqlPort),
					Protocol:   corev1.ProtocolTCP,
				},
			},
		}
		return nil
	})
	return err
}

func (r *MysqlFailoverGroupReconciler) reconcileReplicasService(ctx context.Context, fg *v1alpha1.MysqlFailoverGroup) error {
	svc := &corev1.Service{
		ObjectMeta: metav1.ObjectMeta{
			Name:      fmt.Sprintf("mysql-%s-replicas", fg.Name),
			Namespace: fg.Namespace,
		},
	}

	_, err := controllerutil.CreateOrUpdate(ctx, r.Client, svc, func() error {
		if err := controllerutil.SetControllerReference(fg, svc, r.Scheme); err != nil {
			return err
		}
		svc.Labels = map[string]string{
			labelAppName:       "mysql",
			labelInstance:      fg.Name,
			labelFailoverGroup: fg.Name,
			labelManagedBy:     managerName,
		}
		applyServiceAnnotations(svc, fg.Spec.ServiceTemplate)
		svc.Spec = corev1.ServiceSpec{
			Type: serviceType(fg.Spec.ServiceTemplate),
			Selector: map[string]string{
				labelInstance: fg.Name,
				labelRole:     "replica",
				labelHealthy:  "yes",
			},
			Ports: []corev1.ServicePort{
				{
					Name:       "mysql",
					Port:       mysqlPort,
					TargetPort: intstr.FromInt32(mysqlPort),
					Protocol:   corev1.ProtocolTCP,
				},
			},
		}
		return nil
	})
	return err
}

func (r *MysqlFailoverGroupReconciler) reconcilePDB(ctx context.Context, fg *v1alpha1.MysqlFailoverGroup) error {
	minAvailable := intstr.FromInt32(1)
	pdb := &policyv1.PodDisruptionBudget{
		ObjectMeta: metav1.ObjectMeta{
			Name:      fmt.Sprintf("mysql-%s", fg.Name),
			Namespace: fg.Namespace,
		},
	}

	_, err := controllerutil.CreateOrUpdate(ctx, r.Client, pdb, func() error {
		if err := controllerutil.SetControllerReference(fg, pdb, r.Scheme); err != nil {
			return err
		}
		pdb.Labels = map[string]string{
			labelAppName:       "mysql",
			labelInstance:      fg.Name,
			labelFailoverGroup: fg.Name,
			labelManagedBy:     managerName,
		}
		pdb.Spec = policyv1.PodDisruptionBudgetSpec{
			MinAvailable: &minAvailable,
			Selector: &metav1.LabelSelector{
				MatchLabels: map[string]string{
					labelAppName:  "mysql",
					labelInstance: fg.Name,
				},
			},
		}
		return nil
	})
	return err
}

// serviceType returns the configured service type or ClusterIP as default.
func serviceType(tmpl *v1alpha1.ServiceTemplate) corev1.ServiceType {
	if tmpl != nil && tmpl.Type != "" {
		return tmpl.Type
	}
	return corev1.ServiceTypeClusterIP
}

// applyServiceAnnotations merges user-supplied service annotations into the Service.
func applyServiceAnnotations(svc *corev1.Service, tmpl *v1alpha1.ServiceTemplate) {
	if tmpl == nil || len(tmpl.Annotations) == 0 {
		return
	}
	if svc.Annotations == nil {
		svc.Annotations = make(map[string]string, len(tmpl.Annotations))
	}
	for k, v := range tmpl.Annotations {
		svc.Annotations[k] = v
	}
}

// syncPodLabels updates pod labels based on the CR status.
// It updates replicas first, then primary, to prevent dual-primary in Service endpoints.
func (r *MysqlFailoverGroupReconciler) syncPodLabels(ctx context.Context, fg *v1alpha1.MysqlFailoverGroup) error {
	logger := log.FromContext(ctx)

	if fg.Status.ActiveSite == "" {
		return nil
	}

	// Guard: status may not be populated yet
	if len(fg.Status.Sites) < len(fg.Spec.Sites) {
		return nil
	}

	// Determine which site is primary, which is replica
	type siteInfo struct {
		spec   v1alpha1.SiteSpec
		status v1alpha1.SiteStatus
		role   string
	}

	sites := make([]siteInfo, len(fg.Spec.Sites))
	for i := range fg.Spec.Sites {
		sites[i] = siteInfo{
			spec:   fg.Spec.Sites[i],
			status: fg.Status.Sites[i],
			role:   "replica",
		}
		if fg.Status.ActiveSite == fg.Spec.Sites[i].Name {
			sites[i].role = "primary"
		}
	}

	// During a full-instance in-place restore we strip the primary role
	// label so the -primary Service sheds endpoints. Clients connected
	// to the Service see immediate disconnects, which is the intended
	// fence (the alternative — letting them read from a half-loaded
	// datadir — is strictly worse). Per-schema restores skip this:
	// other tenants continue writing, and app-level maintenance-mode
	// handles the affected schema.
	if inPlaceRestoreFencesPrimaryService(fg) {
		for i := range sites {
			if sites[i].role == "primary" {
				sites[i].role = "fenced"
			}
		}
	}

	// During a planned failover we strip the source primary's role
	// label as soon as we enter Draining so the -primary Service stops
	// directing writes to it. At that point status.activeSite still
	// names the source (it is not rewritten until Resuming), so the
	// matcher below is the fenced site. Once Resuming writes
	// status.activeSite=<target>, this branch skips over the new
	// primary and the label sweep on the following reconcile re-adds
	// the primary label to the target's pod.
	if plannedFailoverFencesSourcePrimary(fg) {
		for i := range sites {
			if sites[i].role == "primary" {
				sites[i].role = "fenced"
			}
		}
	}

	// Sort: replicas first, then primary
	sort.Slice(sites, func(i, j int) bool {
		if sites[i].role == "replica" && sites[j].role == "primary" {
			return true
		}
		return false
	})

	for _, si := range sites {
		pods := &corev1.PodList{}
		if err := r.List(ctx, pods,
			client.InNamespace(fg.Namespace),
			client.MatchingLabels{
				labelAppName:  "mysql",
				labelInstance: fg.Name,
				labelSite:     si.spec.Name,
			},
		); err != nil {
			return fmt.Errorf("list pods for site %s: %w", si.spec.Name, err)
		}

		healthy := "no"
		if si.status.State == "writable" || si.status.State == "read-only" {
			healthy = "yes"
		}

		for i := range pods.Items {
			pod := &pods.Items[i]
			needsUpdate := false

			if pod.Labels[labelRole] != si.role {
				if pod.Labels == nil {
					pod.Labels = make(map[string]string)
				}
				pod.Labels[labelRole] = si.role
				needsUpdate = true
			}

			if pod.Labels[labelHealthy] != healthy {
				if pod.Labels == nil {
					pod.Labels = make(map[string]string)
				}
				pod.Labels[labelHealthy] = healthy
				needsUpdate = true
			}

			if needsUpdate {
				logger.Info("updating pod labels", "pod", pod.Name, "role", si.role, "healthy", healthy)
				podName := pod.Name
				podNamespace := pod.Namespace
				if err := k8sretry.RetryOnConflict(k8sretry.DefaultRetry, func() error {
					// Re-fetch the pod to get the latest resource version.
					var fresh corev1.Pod
					if err := r.Get(ctx, types.NamespacedName{Name: podName, Namespace: podNamespace}, &fresh); err != nil {
						return err
					}
					if fresh.Labels == nil {
						fresh.Labels = make(map[string]string)
					}
					fresh.Labels[labelRole] = si.role
					fresh.Labels[labelHealthy] = healthy
					return r.Update(ctx, &fresh)
				}); err != nil {
					return fmt.Errorf("update pod %s labels: %w", podName, err)
				}
			}
		}
	}

	return nil
}

// computeSpecHash returns a short hash of the spec fields that should trigger a deployment update.
// tlsSecretData is the raw data from the TLS Secret (nil when TLS is not configured).
// credSecretData is a map of secret-name→data for credential secrets (nil in legacy mode).
func computeSpecHash(fg *v1alpha1.MysqlFailoverGroup, site v1alpha1.SiteSpec, tlsSecretData map[string][]byte, credSecretData map[string]map[string][]byte) string {
	h := sha256.New()
	fmt.Fprintf(h, "image=%s\n", fg.Spec.Image)
	fmt.Fprintf(h, "sidecar=%s\n", fg.Spec.SidecarImage)
	fmt.Fprintf(h, "resources=%v\n", site.Resources)
	fmt.Fprintf(h, "sidecarResources=%v\n", fg.Spec.SidecarResources)
	// Sort mysqlConf keys for deterministic hash
	keys := make([]string, 0, len(fg.Spec.MysqlConf))
	for k := range fg.Spec.MysqlConf {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	for _, k := range keys {
		fmt.Fprintf(h, "mysql.%s=%s\n", k, fg.Spec.MysqlConf[k])
	}
	// PITR settings affect both my.cnf (max_binlog_size) and the
	// sidecar env (archiver config). Either change must roll the pod.
	// Hash the EFFECTIVE values (with defaults filled in) rather than
	// the raw spec: otherwise a release that changes the default
	// MaxBinlogSize or ArchivePollInterval in code would fail to
	// produce a hash change and pods wouldn't roll to pick up the
	// new value.
	if fg.Spec.Backup != nil && fg.Spec.Backup.PITR != nil {
		p := fg.Spec.Backup.PITR
		effMaxBinlogSize := p.MaxBinlogSize
		if effMaxBinlogSize == "" {
			effMaxBinlogSize = defaultPITRMaxBinlogSize
		}
		effPollInterval := defaultPITRArchivePollInterval
		if p.ArchivePollInterval != nil {
			effPollInterval = p.ArchivePollInterval.Duration
		}
		fmt.Fprintf(h, "pitr.enabled=%t\n", p.Enabled)
		fmt.Fprintf(h, "pitr.profile=%s\n", p.ProfileName)
		fmt.Fprintf(h, "pitr.maxBinlogSize=%s\n", effMaxBinlogSize)
		fmt.Fprintf(h, "pitr.archivePollInterval=%s\n", effPollInterval)
	}
	// Include TLS certificate data so cert rotation triggers a rolling update.
	if len(tlsSecretData) > 0 {
		tlsKeys := make([]string, 0, len(tlsSecretData))
		for k := range tlsSecretData {
			tlsKeys = append(tlsKeys, k)
		}
		sort.Strings(tlsKeys)
		for _, k := range tlsKeys {
			fmt.Fprintf(h, "tls.%s=%x\n", k, sha256.Sum256(tlsSecretData[k]))
		}
	}
	// Include credential secret data so password rotation triggers pod rolling restart.
	if len(credSecretData) > 0 {
		credNames := make([]string, 0, len(credSecretData))
		for name := range credSecretData {
			credNames = append(credNames, name)
		}
		sort.Strings(credNames)
		for _, name := range credNames {
			data := credSecretData[name]
			dataKeys := make([]string, 0, len(data))
			for k := range data {
				dataKeys = append(dataKeys, k)
			}
			sort.Strings(dataKeys)
			for _, k := range dataKeys {
				fmt.Fprintf(h, "cred.%s.%s=%x\n", name, k, sha256.Sum256(data[k]))
			}
		}
	}
	return hex.EncodeToString(h.Sum(nil))[:16]
}

func int32Ptr(i int32) *int32 { return &i }

// CRConfigToTopologyConfig extracts topology manager configuration from a CR.
// This bridges the CRD spec to the internal config used by TopologyManager.
func CRConfigToTopologyConfig(fg *v1alpha1.MysqlFailoverGroup) TopologyConfig {
	pollInterval := int64(2 * time.Second)
	if fg.Spec.PollInterval != nil {
		pollInterval = int64(fg.Spec.PollInterval.Duration)
	}

	failureThreshold := int32(3)
	if fg.Spec.FailureThreshold > 0 {
		failureThreshold = fg.Spec.FailureThreshold
	}

	recoveryThreshold := int32(2)
	if fg.Spec.RecoveryThreshold > 0 {
		recoveryThreshold = fg.Spec.RecoveryThreshold
	}

	var failoverCooldown int64
	if fg.Spec.FailoverCooldown != nil {
		failoverCooldown = int64(fg.Spec.FailoverCooldown.Duration)
	}

	var sitePriorities []string
	if fg.Spec.SplitBrainPolicy != nil && len(fg.Spec.SplitBrainPolicy.SitePriorities) > 0 {
		sitePriorities = append(sitePriorities, fg.Spec.SplitBrainPolicy.SitePriorities...)
	}

	sites := make([]SiteTopologyConfig, len(fg.Spec.Sites))
	for i, s := range fg.Spec.Sites {
		sites[i] = SiteTopologyConfig{
			Name: s.Name,
			Zone: s.Zone,
			LBIP: s.LBIP,
			Role: state.SiteRole(s.EffectiveRole()),
			Host: fmt.Sprintf("mysql-%s-%s.%s.svc.cluster.local", fg.Name, s.Name, fg.Namespace),
		}
	}

	return TopologyConfig{
		Namespace:         fg.Namespace,
		Name:              fg.Name,
		Sites:             sites,
		PollInterval:      pollInterval,
		FailureThreshold:  int(failureThreshold),
		RecoveryThreshold: int(recoveryThreshold),
		FailoverCooldown:  failoverCooldown,
		SitePriorities:    sitePriorities,
	}
}

// ReconcileSiteDeployment reconciles a single site's Deployment to match the
// desired CR spec, then blocks until the Deployment's rollout has completed
// (new pod Ready, old pod gone). Used by the ordered update controller so the
// "update standby → failover → update old active" sequence actually runs in
// order; without the rollout wait, both sites' pods end up rolling in parallel
// and the cluster briefly has no reachable MySQL.
func (r *MysqlFailoverGroupReconciler) ReconcileSiteDeployment(ctx context.Context, fgName types.NamespacedName, siteName string) error {
	var fg v1alpha1.MysqlFailoverGroup
	if err := r.Get(ctx, fgName, &fg); err != nil {
		return fmt.Errorf("get CR: %w", err)
	}
	image := fg.Spec.Image
	if image == "" {
		image = defaultMySQLImage
	}
	siteFound := false
	for i, site := range fg.Spec.Sites {
		if site.Name == siteName {
			siteFound = true
			serverID := int32(101 + i)
			if err := r.reconcileDeployment(ctx, &fg, site, serverID, image); err != nil {
				return err
			}
			break
		}
	}
	if !siteFound {
		return fmt.Errorf("site %q not found in CR %s", siteName, fgName)
	}
	deployName := types.NamespacedName{Namespace: fgName.Namespace, Name: resourceName(fgName.Name, siteName)}
	return r.waitForDeploymentRollout(ctx, deployName, 5*time.Minute)
}

// waitForDeploymentRollout polls a Deployment until its rollout is complete or
// the timeout expires. A rollout is complete when the Deployment controller has
// observed the latest spec AND the desired number of replicas are both updated
// and available (new pod Ready) AND no extra pods from a prior ReplicaSet remain.
//
// Reads bypass the controller-runtime cache via APIReader when available: the
// cached client lags behind the apiserver, and a pre-patch cached snapshot
// will satisfy the rollout-complete check before Kubernetes has even started
// rolling the new pod — the exact race that motivates this wait.
//
// rolloutPollInterval can be overridden by tests; zero uses the production default.
func (r *MysqlFailoverGroupReconciler) waitForDeploymentRollout(ctx context.Context, nn types.NamespacedName, timeout time.Duration) error {
	ctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()

	interval := r.rolloutPollInterval
	if interval <= 0 {
		interval = 2 * time.Second
	}
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	reader := client.Reader(r.Client)
	if r.APIReader != nil {
		reader = r.APIReader
	}

	for {
		var dep appsv1.Deployment
		err := reader.Get(ctx, nn, &dep)
		if err == nil {
			desired := int32(1)
			if dep.Spec.Replicas != nil {
				desired = *dep.Spec.Replicas
			}
			if dep.Status.ObservedGeneration >= dep.Generation &&
				dep.Status.UpdatedReplicas == desired &&
				dep.Status.AvailableReplicas == desired &&
				dep.Status.Replicas == dep.Status.UpdatedReplicas {
				return nil
			}
		}
		// Transient API errors fall through to the next tick; NotFound is
		// possible during the brief window after a Deployment delete and is
		// treated the same — the wait will fail on timeout if it persists.

		select {
		case <-ctx.Done():
			cerr := ctx.Err()
			switch cerr {
			case context.DeadlineExceeded:
				return fmt.Errorf("timeout waiting for deployment %s rollout: %w", nn, cerr)
			case context.Canceled:
				return fmt.Errorf("stopped waiting for deployment %s rollout: %w", nn, cerr)
			default:
				return fmt.Errorf("context done while waiting for deployment %s rollout: %w", nn, cerr)
			}
		case <-ticker.C:
		}
	}
}

// FailoverGroupNamespacedName creates a NamespacedName from a failover group.
func FailoverGroupNamespacedName(fg *v1alpha1.MysqlFailoverGroup) types.NamespacedName {
	return types.NamespacedName{
		Namespace: fg.Namespace,
		Name:      fg.Name,
	}
}
