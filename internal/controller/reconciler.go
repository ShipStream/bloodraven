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

	// Reconcile per-site resources
	for i, site := range fg.Spec.Sites {
		serverID := int32(101 + i)
		peerSite := fg.Spec.Sites[1-i]

		if err := r.reconcilePVC(ctx, &fg, site); err != nil {
			return ctrl.Result{}, fmt.Errorf("reconcile pvc %s: %w", site.Name, err)
		}
		if err := r.reconcileDeployment(ctx, &fg, site, peerSite, serverID, image); err != nil {
			return ctrl.Result{}, fmt.Errorf("reconcile deployment %s: %w", site.Name, err)
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

	// Reconcile scheduled backups (one CronJob per schedule entry).
	if err := r.reconcileBackupSchedules(ctx, &fg); err != nil {
		return ctrl.Result{}, fmt.Errorf("reconcile backup schedules: %w", err)
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
			if err := r.Tainter.SetTaint(ctx, selector, false); err != nil {
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

func (r *MysqlFailoverGroupReconciler) reconcileDeployment(ctx context.Context, fg *v1alpha1.MysqlFailoverGroup, site, peerSite v1alpha1.SiteSpec, serverID int32, image string) error {
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
		// Set role/healthy labels so Services have endpoints from pod creation.
		// syncPodLabels refines these once topology polling confirms state.
		if fg.Status.ActiveSite == site.Name {
			podLabels[labelRole] = "primary"
			podLabels[labelHealthy] = "yes"
		} else {
			podLabels[labelRole] = "replica"
			if fg.Status.ActiveSite != "" {
				podLabels[labelHealthy] = "yes"
			} else {
				podLabels[labelHealthy] = "no"
			}
		}

		sidecarImage := fg.Spec.SidecarImage

		configMapName := fmt.Sprintf("mysql-%s-config", fg.Name)

		peerAddress := fmt.Sprintf("mysql-%s-%s.%s.svc.cluster.local:%d",
			fg.Name, peerSite.Name, fg.Namespace, sidecarPort)

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
				{Name: "PEER_ADDRESS", Value: peerAddress},
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
							Command: []string{
								"sh", "-c",
								`mysql -u root -p"${MYSQL_ROOT_PASSWORD}" -e 'SET GLOBAL super_read_only=ON' 2>/dev/null || true`,
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
				// run on both primary and replica nodes. Only application
				// pods should be evicted by this taint.
				Tolerations: []corev1.Toleration{
					{
						Key:      platform.TaintKey,
						Operator: corev1.TolerationOpExists,
						Effect:   corev1.TaintEffectNoExecute,
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

	sites := []siteInfo{
		{fg.Spec.Sites[0], fg.Status.Sites[0], "replica"},
		{fg.Spec.Sites[1], fg.Status.Sites[1], "replica"},
	}

	for i := range sites {
		if fg.Status.ActiveSite == fg.Spec.Sites[i].Name {
			sites[i].role = "primary"
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

	return TopologyConfig{
		Namespace: fg.Namespace,
		Name:      fg.Name,
		Sites: [2]SiteTopologyConfig{
			{
				Name: fg.Spec.Sites[0].Name,
				Zone: fg.Spec.Sites[0].Zone,
				LBIP: fg.Spec.Sites[0].LBIP,
			},
			{
				Name: fg.Spec.Sites[1].Name,
				Zone: fg.Spec.Sites[1].Zone,
				LBIP: fg.Spec.Sites[1].LBIP,
			},
		},
		SiteHosts: [2]string{
			fmt.Sprintf("mysql-%s-%s.%s.svc.cluster.local", fg.Name, fg.Spec.Sites[0].Name, fg.Namespace),
			fmt.Sprintf("mysql-%s-%s.%s.svc.cluster.local", fg.Name, fg.Spec.Sites[1].Name, fg.Namespace),
		},
		PollInterval:      pollInterval,
		FailureThreshold:  int(failureThreshold),
		RecoveryThreshold: int(recoveryThreshold),
		FailoverCooldown:  failoverCooldown,
	}
}

// FailoverGroupNamespacedName creates a NamespacedName from a failover group.
func FailoverGroupNamespacedName(fg *v1alpha1.MysqlFailoverGroup) types.NamespacedName {
	return types.NamespacedName{
		Namespace: fg.Namespace,
		Name:      fg.Name,
	}
}
