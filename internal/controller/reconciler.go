package controller

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"os"
	"sort"
	"strings"
	"sync"
	"time"

	appsv1 "k8s.io/api/apps/v1"
	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	policyv1 "k8s.io/api/policy/v1"
	"k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/intstr"
	"k8s.io/client-go/kubernetes"
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

	defaultMySQLImage = "mysql:9.7"

	labelAppName       = "app.kubernetes.io/name"
	labelInstance      = "app.kubernetes.io/instance"
	labelFailoverGroup = "shipstream.io/failover-group"
	labelSite          = "shipstream.io/site"
	labelRole          = "shipstream.io/role"
	labelHealthy       = "shipstream.io/healthy"
	labelManagedBy     = "app.kubernetes.io/managed-by"
	managerName        = "bloodraven"

	specHashAnnotation        = "shipstream.io/spec-hash"
	managedServiceAnnotations = "shipstream.io/managed-service-annotations"
	configMapRenderVersion    = "site-config-v2-content-addressed-encryption"

	// Bump when the rendered MySQL Deployment pod spec changes without a
	// corresponding user-facing spec field change, so existing pods roll
	// forward to the new safe defaults.
	deploymentPodRenderVersion = "deployment-pod-render-v3-site-config-internal-service"

	// RecloneAnnotation is set by an admin to trigger a reclone of a
	// specific site from the current primary via CLONE INSTANCE.
	RecloneAnnotation = "bloodraven.shipstream.io/reclone-site"

	mysqlPort   = 3306
	sidecarPort = 8080
)

func defaultBloodravenAddress(fg *v1alpha1.MysqlFailoverGroup) string {
	if fg.Spec.Sidecar.BloodravenAddress != "" {
		return fg.Spec.Sidecar.BloodravenAddress
	}
	if addr := strings.TrimSpace(os.Getenv("BLOODRAVEN_DEFAULT_AUXILIARY_ADDRESS")); addr != "" {
		return addr
	}
	return fmt.Sprintf("bloodraven.%s.svc.cluster.local:8082", fg.Namespace)
}

// MysqlFailoverGroupReconciler reconciles a MysqlFailoverGroup object.
type MysqlFailoverGroupReconciler struct {
	client.Client
	Scheme    *runtime.Scheme
	Recorder  record.EventRecorder
	Runner    *TopologyManagerRunner
	Tainter   platform.NodeTainter // optional, for taint cleanup during deletion
	Clientset kubernetes.Interface // optional, for tailing restore Job logs

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

	// dragonflyConnector overrides the production net.Dial-based dialer
	// used by the planned-failover Dragonfly handlers. Tests inject a
	// scripted fake; production wiring leaves this nil so realDragonflyConnector
	// is used.
	dragonflyConnector DragonflyConnector

	// keyringStatus overrides the production sidecar-backed fetcher used
	// by the encryption-at-rest state machine. Tests inject a scripted
	// fake; production wiring leaves this nil so
	// defaultKeyringStatusFetcher is used.
	keyringStatus keyringStatusFetcher

	dragonflyRolloutMu      sync.Mutex
	dragonflyRolloutBackoff map[dragonflyRolloutKey]dragonflyRolloutState
}

type dragonflyRolloutKey struct {
	fg     types.NamespacedName
	target string
	source string
}

type dragonflyRolloutState struct {
	attempts    int
	lastFailure time.Time
}

// +kubebuilder:rbac:groups=shipstream.io,resources=mysqlfailovergroups,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=shipstream.io,resources=mysqlfailovergroups/status,verbs=get;update;patch
// +kubebuilder:rbac:groups=shipstream.io,resources=mysqlfailovergroups/finalizers,verbs=update
// +kubebuilder:rbac:groups="",resources=configmaps;services;persistentvolumeclaims,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=apps,resources=deployments,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=apps,resources=statefulsets,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=policy,resources=poddisruptionbudgets,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups="",resources=pods,verbs=get;list;watch;update;patch;delete
// +kubebuilder:rbac:groups="",resources=nodes,verbs=get;list;watch;update;patch
// Secrets: read for credentials/TLS, and create/delete for the
// encryption-at-rest keyring escrow (immutable per-site keyring
// versions) and its per-site escrow tokens. Update is deliberately
// absent — escrow Secrets are immutable by construction, so the
// operator only ever mints a new version or prunes an old one.
// +kubebuilder:rbac:groups="",resources=secrets,verbs=get;list;watch;create;delete
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

	r.warnOnSidecarVersionSkew(ctx, &fg)

	// Encryption-at-rest must settle before anything renders: both the
	// per-site ConfigMap (which carries "read_only" for the keyring
	// component) and the Deployment (which picks a Secret projection or
	// a memory-backed emptyDir for the keyring volume) are driven by the
	// per-site keyring phase this call maintains.
	encryptionRequeueAfter, err := r.reconcileEncryptionAtRest(ctx, &fg)
	if err != nil {
		return ctrl.Result{}, fmt.Errorf("reconcile encryption at rest: %w", err)
	}

	// Desired per-site ConfigMaps are created before any Deployment reference
	// changes. This ordering makes migration from the legacy shared map safe.
	if err := r.reconcileSiteConfigMaps(ctx, &fg); err != nil {
		return ctrl.Result{}, fmt.Errorf("reconcile site configmaps: %w", err)
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

	// Defer every existing Deployment to the ordered update path: the reconciler firing on a CR spec
	// change must not restart both sites simultaneously. The runner's
	// checkSpecDrift compares the desired hash against the live Deployment
	// annotation, so leaving the existing Deployment untouched is what causes
	// drift to be observed and the ordered update to start. New Deployments
	// (initial bootstrap) are always created here. This guard deliberately does
	// not depend on runner wiring or manager registration: existing Deployments
	// are never safe to patch from the bulk reconciliation loop.
	deploymentReader := client.Reader(r.Client)
	if r.APIReader != nil {
		deploymentReader = r.APIReader
	}

	// Reconcile per-site resources
	for i, site := range fg.Spec.Sites {
		serverID := int32(101 + i)

		if err := r.reconcilePVC(ctx, &fg, site); err != nil {
			return ctrl.Result{}, fmt.Errorf("reconcile pvc %s: %w", site.Name, err)
		}
		if !orderedUpdateActive {
			deferDeployment := false
			var existing appsv1.Deployment
			deployNN := types.NamespacedName{
				Namespace: fg.Namespace,
				Name:      resourceName(fg.Name, site.Name),
			}
			if err := deploymentReader.Get(ctx, deployNN, &existing); err == nil {
				deferDeployment = true
			} else if !errors.IsNotFound(err) {
				return ctrl.Result{}, fmt.Errorf("get deployment %s: %w", site.Name, err)
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
		if err := r.reconcileInternalSiteService(ctx, &fg, site); err != nil {
			return ctrl.Result{}, fmt.Errorf("reconcile internal site service %s: %w", site.Name, err)
		}
	}
	if err := r.cleanupObsoleteSiteConfigMaps(ctx, &fg); err != nil {
		return ctrl.Result{}, fmt.Errorf("cleanup obsolete site configmaps: %w", err)
	}
	if err := r.cleanupLegacyConfigMap(ctx, &fg); err != nil {
		return ctrl.Result{}, fmt.Errorf("cleanup legacy configmap: %w", err)
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

	if dfUpgradeRequeue, err := r.reconcileDragonflySnapshotUpgrade(ctx, &fg); err != nil {
		return ctrl.Result{}, fmt.Errorf("reconcile dragonfly snapshot upgrade: %w", err)
	} else if dfUpgradeRequeue > 0 {
		return ctrl.Result{RequeueAfter: dfUpgradeRequeue}, nil
	}

	// Reconcile per-site Dragonfly resources when spec.dragonfly is enabled.
	// When Dragonfly is disabled, reconcileDragonflyResources actively removes
	// any previously managed Dragonfly resources so MySQL-only deployments are
	// unaffected.
	deferredRequeue := time.Duration(0)
	if dragonflyRequeue, err := r.reconcileDragonflyResources(ctx, &fg); err != nil {
		return ctrl.Result{}, fmt.Errorf("reconcile dragonfly resources: %w", err)
	} else if dragonflyRequeue > 0 {
		deferredRequeue = minPositiveDuration(deferredRequeue, dragonflyRequeue)
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

	// Mirror the same sweep for Dragonfly pods so the active service
	// selector follows the active site (or the in-flight planned-
	// failover target). No-op when spec.dragonfly is disabled.
	if err := r.syncDragonflyPodLabels(ctx, &fg); err != nil {
		return ctrl.Result{}, fmt.Errorf("sync dragonfly pod labels: %w", err)
	}

	deferredRequeue = minPositiveDuration(deferredRequeue, backupRequeue)
	// A site mid-keyring-lifecycle is waiting on a pod roll or on the
	// sidecar's escrow push, neither of which enqueues the CR.
	deferredRequeue = minPositiveDuration(deferredRequeue, encryptionRequeueAfter)
	if deferredRequeue > 0 {
		return ctrl.Result{RequeueAfter: deferredRequeue}, nil
	}
	return ctrl.Result{}, nil
}

func minPositiveDuration(a, b time.Duration) time.Duration {
	if a <= 0 {
		return b
	}
	if b <= 0 || a < b {
		return a
	}
	return b
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
		Owns(&appsv1.StatefulSet{}).
		Owns(&corev1.Service{}).
		Owns(&corev1.ConfigMap{}).
		Owns(&corev1.PersistentVolumeClaim{}).
		Owns(&policyv1.PodDisruptionBudget{}).
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

	// Remove taints for all configured site selectors.
	if r.Tainter != nil {
		for _, site := range fg.Spec.Sites {
			if site.IsReadOnlyReader() {
				continue
			}
			selector := taintNodeSelectorString(site.TaintNodeSelector)
			if selector == "" {
				continue
			}
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

func siteConfigMapName(group, site string) string {
	return fmt.Sprintf("mysql-%s-%s-config", group, site)
}

// mysqlSecurityContext returns the mysql container security context.
// When encryption is enabled, AppArmor and seccomp are set Unconfined:
// GHA kind runners otherwise surface "Can't find error-message file"
// for a path that exists and fail to load component_keyring_file
// (MY-013712) despite valid global and local manifests. User-supplied
// containerSecurityContext is still honored and only these two fields
// are filled in when unset.
func mysqlSecurityContext(fg *v1alpha1.MysqlFailoverGroup) *corev1.SecurityContext {
	sc := fg.Spec.ContainerSecurityContext.DeepCopy()
	if !fg.Spec.EncryptionEnabled() {
		return sc
	}
	if sc == nil {
		sc = &corev1.SecurityContext{}
	}
	if sc.AppArmorProfile == nil {
		sc.AppArmorProfile = &corev1.AppArmorProfile{
			Type: corev1.AppArmorProfileTypeUnconfined,
		}
	}
	if sc.SeccompProfile == nil {
		sc.SeccompProfile = &corev1.SeccompProfile{
			Type: corev1.SeccompProfileTypeUnconfined,
		}
	}
	return sc
}

// configInitScript copies the operator-managed my.cnf fragments into the
// conf emptyDir. When encryption is enabled it also plants a local
// component manifest in the datadir (see encryptionConfigInitSnippet).
func configInitScript(fg *v1alpha1.MysqlFailoverGroup, serverID int32) string {
	script := fmt.Sprintf(
		"cp /etc/mysql/config-map/bloodraven.cnf /etc/mysql/conf.d/bloodraven.cnf && printf '[mysqld]\\nserver-id=%d\\n' > /etc/mysql/conf.d/server-id.cnf",
		serverID,
	)
	if fg.Spec.EncryptionEnabled() {
		script += encryptionConfigInitSnippet()
	}
	return script
}

// configInitVolumeMounts returns the volume mounts for the config init
// container. The datadir is only mounted when encryption needs to write
// a local mysqld.my there.
func configInitVolumeMounts(fg *v1alpha1.MysqlFailoverGroup) []corev1.VolumeMount {
	mounts := []corev1.VolumeMount{
		{Name: "config", MountPath: "/etc/mysql/config-map"},
		{Name: "conf", MountPath: "/etc/mysql/conf.d"},
	}
	if fg.Spec.EncryptionEnabled() {
		mounts = append(mounts, corev1.VolumeMount{
			Name:      "data",
			MountPath: "/var/lib/mysql",
		})
	}
	return mounts
}

// desiredSiteConfigData renders the complete ConfigMap payload for one site.
// Keeping naming and reconciliation on this shared value prevents a
// content-addressed name from ever disagreeing with the bytes mounted by the
// pod revision.
func desiredSiteConfigData(fg *v1alpha1.MysqlFailoverGroup, site v1alpha1.SiteSpec) map[string]string {
	data := map[string]string{
		"bloodraven.cnf": generateMyCnf(fg, site),
	}
	for k, v := range keyringConfigMapData(fg, fg.SiteKeyringSealed(site.Name)) {
		data[k] = v
	}
	return data
}

// desiredSiteConfigMapName content-addresses encrypted configurations. During
// live adoption, existing pods keep referencing the old unencrypted canonical
// ConfigMap while the ordered updater atomically switches one Deployment's
// ConfigMap reference and keyring wiring in the same PodTemplate patch.
func desiredSiteConfigMapName(fg *v1alpha1.MysqlFailoverGroup, site v1alpha1.SiteSpec) string {
	base := siteConfigMapName(fg.Name, site.Name)
	if !fg.Spec.EncryptionEnabled() {
		return base
	}
	data := desiredSiteConfigData(fg, site)
	keys := make([]string, 0, len(data))
	for key := range data {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	h := sha256.New()
	for _, key := range keys {
		fmt.Fprintf(h, "%s=%s\n", key, data[key])
	}
	return fmt.Sprintf("%s-%s", base, hex.EncodeToString(h.Sum(nil))[:12])
}

func (r *MysqlFailoverGroupReconciler) reconcileSiteConfigMaps(ctx context.Context, fg *v1alpha1.MysqlFailoverGroup) error {
	for _, site := range fg.Spec.Sites {
		if err := r.reconcileSiteConfigMap(ctx, fg, site); err != nil {
			return err
		}
	}
	return nil
}

func (r *MysqlFailoverGroupReconciler) reconcileSiteConfigMap(ctx context.Context, fg *v1alpha1.MysqlFailoverGroup, site v1alpha1.SiteSpec) error {
	versioned := fg.Spec.EncryptionEnabled()
	cm := &corev1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{
			Name:      desiredSiteConfigMapName(fg, site),
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
			labelSite:          site.Name,
			labelManagedBy:     managerName,
		}
		cm.Data = desiredSiteConfigData(fg, site)
		if versioned {
			immutable := true
			cm.Immutable = &immutable
		}
		return nil
	})
	return err
}

func normalizedMySQLSettings(raw map[string]string) map[string]string {
	keys := make([]string, 0, len(raw))
	for key := range raw {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	out := make(map[string]string, len(raw))
	for _, key := range keys {
		out[strings.ReplaceAll(key, "_", "-")] = raw[key]
	}
	return out
}

func generateMyCnf(fg *v1alpha1.MysqlFailoverGroup, sites ...v1alpha1.SiteSpec) string {
	// Base config
	settings := map[string]string{
		"gtid-mode":                       "ON",
		"enforce-gtid-consistency":        "ON",
		"log-bin":                         "/var/lib/mysql/mysql-bin",
		"log-bin-trust-function-creators": "1",
		"log-replica-updates":             "ON",
		"skip-replica-start":              "ON",
		"sync-binlog":                     "1",
		"binlog-expire-logs-seconds":      "1209600",
		"plugin-load-add":                 "mysql_clone.so",
		"default-storage-engine":          "InnoDB",
		"innodb-flush-method":             "O_DIRECT",
		"innodb-flush-log-at-trx-commit":  "2",
		"innodb-file-per-table":           "1",
		"max-allowed-packet":              "64M",
		"max-connect-errors":              "1000000",
		"skip-name-resolve":               "",
		"max-connections":                 "500",
		"thread-cache-size":               "50",
		"character-set-server":            "utf8mb4",
		"collation-server":                "utf8mb4_unicode_ci",
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

	for k, v := range normalizedMySQLSettings(fg.Spec.MysqlConf) {
		settings[k] = v
	}
	if len(sites) > 0 {
		for k, v := range normalizedMySQLSettings(sites[0].MysqlConf) {
			settings[k] = v
		}
	}

	// These settings are operator-owned safety invariants and cannot be
	// weakened by group or site overrides.
	for k, v := range map[string]string{
		"gtid-mode": "ON", "enforce-gtid-consistency": "ON",
		"log-bin": "/var/lib/mysql/mysql-bin", "log-replica-updates": "ON",
		"skip-replica-start": "ON", "plugin-load-add": "mysql_clone.so",
	} {
		settings[k] = v
	}

	// Encryption coverage is likewise operator-owned: the security claim
	// depends on these settings being exactly what spec.encryptionAtRest
	// asked for, so a spec.mysqlConf override for the same keys must not
	// be able to silently downgrade a site to plaintext redo logs.
	for k, v := range encryptionMySQLSettings(fg) {
		settings[k] = v
	}

	// skip-log-bin / disable-log-bin (underscore spellings are normalized to
	// hyphens by normalizedMySQLSettings) would silently defeat the enforced
	// log-bin invariant: the sorted render places them after log-bin and
	// MySQL honors the last occurrence. Strip both aliases outright so group
	// or site overrides cannot reintroduce them.
	delete(settings, "skip-log-bin")
	delete(settings, "disable-log-bin")

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
	// Encryption-at-rest rendering is driven by the site's observed
	// keyring phase, not by the spec alone: a site is only rendered
	// sealed once the operator has verified that its keyring is durably
	// escrowed. buildEncryptionFragments returns empty fragments when
	// spec.encryptionAtRest is absent or disabled.
	encFrags := buildEncryptionFragments(
		fg, site,
		fg.SiteKeyringSealed(site.Name),
		siteEscrowSecretName(fg, site.Name),
		siteKeyringRotating(fg, site.Name),
	)

	specHash := ComputeSpecHash(fg, site, tlsSecretData, credSecretData)

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

		configMapName := desiredSiteConfigMapName(fg, site)

		// Build the list of peer sidecar addresses. The sidecar treats
		// "all peers unreachable" as one half of the self-fencing
		// quorum, so every non-self site needs to be listed here.
		peerAddresses := strings.Join(sitePeerAddresses(fg, site.Name), ",")

		bloodravenAddress := defaultBloodravenAddress(fg)

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

		volumeMounts = append(volumeMounts, encFrags.MysqlVolumeMounts...)

		sidecarVolumeMounts := []corev1.VolumeMount{}
		if fg.Spec.TLS != nil {
			sidecarVolumeMounts = append(sidecarVolumeMounts, corev1.VolumeMount{
				Name:      "tls",
				MountPath: "/etc/mysql/tls",
				ReadOnly:  true,
			})
		}
		sidecarVolumeMounts = append(sidecarVolumeMounts, encFrags.SidecarVolumeMounts...)

		mysqlArgs := []string{
			fmt.Sprintf("--server-id=%d", serverID),
			"--gtid-mode=ON",
			"--enforce-gtid-consistency=ON",
			"--log-bin=/var/lib/mysql/mysql-bin",
			"--relay-log=/var/lib/mysql/mysql-relay-bin",
			"--relay-log-index=/var/lib/mysql/mysql-relay-bin.index",
			"--log-replica-updates=ON",
			"--skip-replica-start=ON",
			"--plugin-load-add=mysql_clone.so",
		}
		// Encryption needs a stable basedir/plugin_dir/lc-messages-dir.
		// On GHA kind, mysqld has been observed to report
		// "Can't find error-message file '/usr/share/mysql-9.7/...'" and
		// skip component_keyring_file load (MY-013712) even when those
		// paths exist. Pin the install paths on the command line so
		// component loading cannot depend on broken auto-detection.
		if fg.Spec.EncryptionEnabled() {
			kr := fg.Spec.EffectiveKeyring()
			mysqlArgs = append(mysqlArgs,
				"--basedir=/usr",
				"--plugin-dir="+kr.PluginDir,
				"--lc-messages-dir=/usr/share/mysql-9.7",
			)
		}
		if fg.Spec.TLS != nil {
			// Keep the server-side TLS contract on the mysqld command line as
			// well as in bloodraven.cnf. The sidecar verifies the per-site
			// Service SAN from its first connection; allowing mysqld to fall
			// back to its auto-generated server-cert.pem makes that first
			// health check fail and the sidecar liveness probe crash-loop.
			mysqlArgs = append(mysqlArgs,
				"--ssl-ca=/etc/mysql/tls/ca.crt",
				"--ssl-cert=/etc/mysql/tls/tls.crt",
				"--ssl-key=/etc/mysql/tls/tls.key",
				"--require-secure-transport=ON",
			)
		}

		mysqlContainer := corev1.Container{
			Name:  "mysql",
			Image: image,
			Args:  mysqlArgs,
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
			// Container-level security context is applied verbatim from
			// spec.containerSecurityContext (opt-in; nil-by-default to
			// preserve backward-compat with existing clusters). The
			// operator does not merge with hardened defaults — see
			// docs/docs/production-hardening.mdx for the rationale.
			// DeepCopy so each container has an independent pointer
			// (the cached spec pointer must not be shared across
			// containers in the rendered PodSpec).
			// Encryption additionally unconfines AppArmor/seccomp so the
			// keyring component can load on hosts whose runtime defaults
			// block mysqld from reading errmsg.sys / loading components.
			SecurityContext: mysqlSecurityContext(fg),
		}
		// Encryption: copy component files onto image-owned paths, then
		// exec the official entrypoint. Replaces plain Args so the
		// launcher owns argv0. See encryptionMysqlLauncher.
		if fg.Spec.EncryptionEnabled() {
			cmd, args := encryptionMysqlLauncher(fg, mysqlArgs)
			mysqlContainer.Command = cmd
			mysqlContainer.Args = args
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
		sidecarEnv = append(sidecarEnv, encFrags.SidecarEnv...)

		// spec.tls makes generateMyCnf set require_secure_transport=ON,
		// which rejects every plaintext TCP connection — including the
		// sidecar's loopback one. Unlike libmysqlclient (and so mysqlsh
		// and the backup/restore jobs), which negotiates TLS on its own
		// under the default ssl-mode=PREFERRED, go-sql-driver/mysql sends
		// plaintext unless the DSN names a registered TLS config. Without
		// this wiring every sidecar query fails with Error 3159, /health
		// returns 503, and the liveness probe crash-loops the container —
		// taking the self-fencing monitor and the super_read_only safety
		// net down with it while the group still reports only Degraded.
		sidecarEnv = append(sidecarEnv, mysqlTLSSidecarEnv(fg, site)...)
		sidecarVolumeMounts = append(sidecarVolumeMounts, pitrFrags.SidecarVolumeMounts...)
		sidecarSecurityContext := fg.Spec.ContainerSecurityContext.DeepCopy()
		// The keyring file is mode 0600 owned by mysqld's uid so it is
		// not world-readable inside the pod; the escrow agent has to run
		// as that same uid to read it. Same defaulting rule as PITR:
		// only fill in what the user left unset.
		if (fg.Spec.Backup != nil && fg.Spec.Backup.PITR != nil && fg.Spec.Backup.PITR.Enabled) ||
			fg.Spec.EncryptionEnabled() {
			if sidecarSecurityContext == nil {
				sidecarSecurityContext = &corev1.SecurityContext{}
			}
			if sidecarSecurityContext.RunAsUser == nil {
				uid := mysqlDataUID
				sidecarSecurityContext.RunAsUser = &uid
			}
			if sidecarSecurityContext.RunAsGroup == nil {
				gid := mysqlDataGID
				sidecarSecurityContext.RunAsGroup = &gid
			}
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
			// Apply the same user-supplied container security context
			// to the sidecar so the pod meets the same Restricted PSS
			// gate as the mysql container (nil = backward compatible).
			// DeepCopy to avoid pointer aliasing with the mysql
			// container's SecurityContext (same source pointer
			// otherwise — see F4 in WISHLIST #39 review).
			SecurityContext: sidecarSecurityContext,
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

		// Encryption contributions (keyring volume, escrow token, seed).
		volumes = append(volumes, encFrags.PodVolumes...)

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
					},
				},
				// Order: keyring-init first — the keyring must exist
				// before mysqld starts, and it is cheaper to fail the
				// pod there than to have InnoDB abort startup with
				// "Check keyring fail". encFrags.InitContainers is
				// empty when encryption is off or the site is sealed.
				InitContainers: append(append(append([]corev1.Container{}, encFrags.InitContainers...), corev1.Container{
					Name:  "init",
					Image: image,
					Command: []string{
						"sh", "-c",
						configInitScript(fg, serverID),
					},
					VolumeMounts: configInitVolumeMounts(fg),
					// Mirror the main mysql container's user-supplied
					// SecurityContext onto the operator-injected init
					// container. Without this, Restricted PSS admission
					// rejects the pod because the init container has no
					// drop-ALL / runAsNonRoot / seccomp settings. nil
					// stays nil (backward-compat). DeepCopy per F4 so
					// the init container does not alias the mysql
					// container's pointer.
					SecurityContext: fg.Spec.ContainerSecurityContext.DeepCopy(),
					// Default init-container resources so LimitRange
					// admission accepts the pod. Users who need a
					// different shape can override via the spec; this
					// init container is operator-managed so we don't
					// expose a separate field for it.
					Resources: defaultInitContainerResources(),
				}), fg.Spec.ExtraInitContainers...),
				Containers: append(containers, fg.Spec.ExtraContainers...),
				Volumes:    volumes,
				// Pod-level security context is applied verbatim from
				// spec.podSecurityContext (opt-in; nil-by-default to
				// preserve backward-compat with existing clusters whose
				// /var/lib/mysql PVCs were created without FSGroup). See
				// docs/docs/production-hardening.mdx for the migration
				// procedure to enable Restricted PSS. DeepCopy so the
				// rendered PodSpec does not alias the cached spec
				// pointer.
				SecurityContext: fg.Spec.PodSecurityContext.DeepCopy(),
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
		applyManagedServiceAnnotations(svc, effectiveSiteServiceAnnotations(fg.Spec.ServiceTemplate, site.ServiceTemplate))
		selector := map[string]string{
			labelAppName:  "mysql",
			labelInstance: fg.Name,
			labelSite:     site.Name,
		}
		if site.IsReadOnlyReader() {
			selector[labelHealthy] = "yes"
		}
		serviceType := effectiveSiteServiceType(fg.Spec.ServiceTemplate, site.ServiceTemplate)
		mutateServiceSpec(svc, serviceType, effectiveSiteExternalTrafficPolicy(fg.Spec.ServiceTemplate, site.ServiceTemplate), selector, []corev1.ServicePort{
			{
				Name:       "mysql",
				Port:       mysqlPort,
				TargetPort: intstr.FromInt32(mysqlPort),
				Protocol:   corev1.ProtocolTCP,
				NodePort:   siteServiceNodePort(site.ServiceTemplate),
			},
		})
		if serviceType == corev1.ServiceTypeLoadBalancer {
			svc.Spec.LoadBalancerIP = site.LBIP
		}
		svc.Spec.PublishNotReadyAddresses = false
		return nil
	})
	return err
}

func (r *MysqlFailoverGroupReconciler) reconcileInternalSiteService(ctx context.Context, fg *v1alpha1.MysqlFailoverGroup, site v1alpha1.SiteSpec) error {
	svc := &corev1.Service{ObjectMeta: metav1.ObjectMeta{Name: internalSiteServiceName(fg.Name, site.Name), Namespace: fg.Namespace}}
	_, err := controllerutil.CreateOrUpdate(ctx, r.Client, svc, func() error {
		if err := controllerutil.SetControllerReference(fg, svc, r.Scheme); err != nil {
			return err
		}
		svc.Labels = commonLabels(fg.Name, site.Name)
		applyManagedServiceAnnotations(svc, nil)
		mutateServiceSpec(svc, corev1.ServiceTypeClusterIP, "", map[string]string{
			labelAppName: "mysql", labelInstance: fg.Name, labelSite: site.Name,
		}, []corev1.ServicePort{
			{Name: "mysql", Port: mysqlPort, TargetPort: intstr.FromInt32(mysqlPort), Protocol: corev1.ProtocolTCP},
			{Name: "sidecar", Port: sidecarPort, TargetPort: intstr.FromInt32(sidecarPort), Protocol: corev1.ProtocolTCP},
		})
		svc.Spec.PublishNotReadyAddresses = true
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
		applyManagedServiceAnnotations(svc, serviceAnnotations(fg.Spec.ServiceTemplate))
		mutateServiceSpec(svc, serviceType(fg.Spec.ServiceTemplate), serviceExternalTrafficPolicy(fg.Spec.ServiceTemplate), map[string]string{
			labelInstance: fg.Name,
			labelRole:     "primary",
		}, []corev1.ServicePort{
			{
				Name:       "mysql",
				Port:       mysqlPort,
				TargetPort: intstr.FromInt32(mysqlPort),
				Protocol:   corev1.ProtocolTCP,
			},
		})
		svc.Spec.PublishNotReadyAddresses = false
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
		applyManagedServiceAnnotations(svc, serviceAnnotations(fg.Spec.ServiceTemplate))
		mutateServiceSpec(svc, serviceType(fg.Spec.ServiceTemplate), serviceExternalTrafficPolicy(fg.Spec.ServiceTemplate), map[string]string{
			labelInstance: fg.Name,
			labelRole:     "replica",
			labelHealthy:  "yes",
		}, []corev1.ServicePort{
			{
				Name:       "mysql",
				Port:       mysqlPort,
				TargetPort: intstr.FromInt32(mysqlPort),
				Protocol:   corev1.ProtocolTCP,
			},
		})
		svc.Spec.PublishNotReadyAddresses = false
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
func serviceExternalTrafficPolicy(tmpl *v1alpha1.ServiceTemplate) corev1.ServiceExternalTrafficPolicyType {
	if tmpl != nil {
		return tmpl.ExternalTrafficPolicy
	}
	return ""
}

func effectiveSiteServiceType(group *v1alpha1.ServiceTemplate, site *v1alpha1.SiteServiceTemplate) corev1.ServiceType {
	if site != nil && site.Type != "" {
		return site.Type
	}
	return serviceType(group)
}

func effectiveSiteExternalTrafficPolicy(group *v1alpha1.ServiceTemplate, site *v1alpha1.SiteServiceTemplate) corev1.ServiceExternalTrafficPolicyType {
	if site != nil && site.ExternalTrafficPolicy != "" {
		return site.ExternalTrafficPolicy
	}
	return serviceExternalTrafficPolicy(group)
}

func siteServiceNodePort(site *v1alpha1.SiteServiceTemplate) int32 {
	if site != nil {
		return site.NodePort
	}
	return 0
}

func serviceAnnotations(tmpl *v1alpha1.ServiceTemplate) map[string]string {
	if tmpl == nil {
		return nil
	}
	return tmpl.Annotations
}

// applyServiceAnnotations is retained for Dragonfly Services, whose resource
// lifecycle is separate from the MySQL Service mutation contract.
func applyServiceAnnotations(svc *corev1.Service, tmpl *v1alpha1.ServiceTemplate) {
	if tmpl == nil {
		return
	}
	if svc.Annotations == nil {
		svc.Annotations = make(map[string]string)
	}
	for k, v := range tmpl.Annotations {
		svc.Annotations[k] = v
	}
}

func effectiveSiteServiceAnnotations(group *v1alpha1.ServiceTemplate, site *v1alpha1.SiteServiceTemplate) map[string]string {
	out := make(map[string]string)
	if group != nil {
		for k, v := range group.Annotations {
			out[k] = v
		}
	}
	if site != nil {
		for k, v := range site.Annotations {
			out[k] = v
		}
	}
	return out
}

func applyManagedServiceAnnotations(svc *corev1.Service, desired map[string]string) {
	if svc.Annotations == nil {
		svc.Annotations = make(map[string]string)
	}
	var previous []string
	_ = json.Unmarshal([]byte(svc.Annotations[managedServiceAnnotations]), &previous)
	for _, key := range previous {
		delete(svc.Annotations, key)
	}
	keys := make([]string, 0, len(desired))
	for k, v := range desired {
		svc.Annotations[k] = v
		keys = append(keys, k)
	}
	sort.Strings(keys)
	encoded, _ := json.Marshal(keys)
	svc.Annotations[managedServiceAnnotations] = string(encoded)
}

func mutateServiceSpec(svc *corev1.Service, serviceType corev1.ServiceType, etp corev1.ServiceExternalTrafficPolicyType, selector map[string]string, ports []corev1.ServicePort) {
	oldPorts := make(map[string]corev1.ServicePort, len(svc.Spec.Ports))
	for _, port := range svc.Spec.Ports {
		oldPorts[port.Name] = port
	}
	external := serviceType == corev1.ServiceTypeNodePort || serviceType == corev1.ServiceTypeLoadBalancer
	if external && etp == "" {
		etp = corev1.ServiceExternalTrafficPolicyCluster
	}
	for i := range ports {
		if external && ports[i].NodePort == 0 {
			ports[i].NodePort = oldPorts[ports[i].Name].NodePort
		}
		if !external {
			ports[i].NodePort = 0
		}
	}
	svc.Spec.Type = serviceType
	svc.Spec.Selector = selector
	svc.Spec.Ports = ports
	if external {
		svc.Spec.ExternalTrafficPolicy = etp
	} else {
		svc.Spec.ExternalTrafficPolicy = ""
	}
	if serviceType != corev1.ServiceTypeLoadBalancer {
		svc.Spec.LoadBalancerIP = ""
		svc.Spec.LoadBalancerSourceRanges = nil
		svc.Spec.LoadBalancerClass = nil
		svc.Spec.AllocateLoadBalancerNodePorts = nil
	}
	if serviceType != corev1.ServiceTypeLoadBalancer || etp != corev1.ServiceExternalTrafficPolicyLocal {
		svc.Spec.HealthCheckNodePort = 0
	}
}

// syncPodLabels updates pod labels based on the CR status.
// It updates replicas first, then primary, to prevent dual-primary in Service endpoints.
func (r *MysqlFailoverGroupReconciler) syncPodLabels(ctx context.Context, fg *v1alpha1.MysqlFailoverGroup) error {
	logger := log.FromContext(ctx)

	statusByName := make(map[string]v1alpha1.SiteStatus, len(fg.Status.Sites))
	for _, siteStatus := range fg.Status.Sites {
		statusByName[siteStatus.Name] = siteStatus
	}
	statusComplete := len(statusByName) >= len(fg.Spec.Sites)
	writableSites := 0
	for _, site := range fg.Spec.Sites {
		status, found := statusByName[site.Name]
		if !found {
			statusComplete = false
			continue
		}
		if status.State == "writable" {
			writableSites++
		}
	}
	authorityValid := false
	if statusComplete && fg.Status.ActiveSite != "" && writableSites == 1 {
		for _, site := range fg.Spec.Sites {
			if site.Name == fg.Status.ActiveSite && site.IsPromotable() && statusByName[site.Name].State == "writable" {
				authorityValid = true
				break
			}
		}
	}

	// Determine which site is primary, which is replica. Invalid or incomplete
	// authority deliberately leaves every site non-primary and every reader
	// non-serving, shedding stale Service endpoints rather than returning early.
	type siteInfo struct {
		spec   v1alpha1.SiteSpec
		status v1alpha1.SiteStatus
		role   string
	}

	sites := make([]siteInfo, len(fg.Spec.Sites))
	for i := range fg.Spec.Sites {
		siteStatus := statusByName[fg.Spec.Sites[i].Name]
		sites[i] = siteInfo{
			spec:   fg.Spec.Sites[i],
			status: siteStatus,
			role:   "replica",
		}
		if authorityValid && fg.Status.ActiveSite == fg.Spec.Sites[i].Name {
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
		if si.spec.IsReadOnlyReader() {
			if authorityValid && si.status.State == "read-only" && si.status.SourceConvergenceState == v1alpha1.SourceConvergenceConverged &&
				si.status.Replicating && si.status.SecondsBehindSource != nil &&
				canonicalSourceHost(si.status.SourceHost) == canonicalSourceHost(internalSiteServiceHost(fg.Name, fg.Status.ActiveSite, fg.Namespace)) &&
				*si.status.SecondsBehindSource <= fg.Spec.EffectiveReadOnlyMaxLagSeconds() {
				healthy = "yes"
			}
		} else if si.status.State == "writable" || si.status.State == "read-only" {
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

// ComputeSpecHash returns a short hash of the spec fields that should trigger a deployment update.
// tlsSecretData is the raw data from the TLS Secret (nil when TLS is not configured).
// credSecretData is a map of secret-name→data for credential secrets (nil in legacy mode).
func ComputeSpecHash(fg *v1alpha1.MysqlFailoverGroup, site v1alpha1.SiteSpec, tlsSecretData map[string][]byte, credSecretData map[string]map[string][]byte) string {
	h := sha256.New()
	fmt.Fprintf(h, "deploymentPodRenderVersion=%s\n", deploymentPodRenderVersion)
	fmt.Fprintf(h, "image=%s\n", fg.Spec.Image)
	fmt.Fprintf(h, "sidecar=%s\n", fg.Spec.SidecarImage)
	fmt.Fprintf(h, "resources=%v\n", site.Resources)
	fmt.Fprintf(h, "sidecarResources=%v\n", fg.Spec.SidecarResources)
	// Opt-in security contexts must be part of the rolling-hash so that
	// toggling spec.{pod,container}SecurityContext actually rolls the
	// pods. The %v rendering of these corev1 structs is deterministic
	// (no maps), which matches the hash-stability contract this helper
	// already relies on for resources/sidecarResources.
	fmt.Fprintf(h, "podSC=%v\n", fg.Spec.PodSecurityContext)
	fmt.Fprintf(h, "containerSC=%v\n", fg.Spec.ContainerSecurityContext)
	fmt.Fprintf(h, "configMapRenderVersion=%s\n", configMapRenderVersion)
	fmt.Fprintf(h, "configMapName=%s\n", desiredSiteConfigMapName(fg, site))
	fmt.Fprintf(h, "effectiveMyCnf=%s\n", generateMyCnf(fg, site))
	fmt.Fprintf(h, "peerAddresses=%s\n", strings.Join(sitePeerAddresses(fg, site.Name), ","))
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
		fmt.Fprintf(h, "pitr.podRenderVersion=%s\n", pitrPodRenderVersion)
		if p.Enabled {
			if profile := findProfile(fg, p.ProfileName); profile != nil {
				fmt.Fprintf(h, "pitr.profile.storage.type=%s\n", profile.Storage.Type)
				if s3 := profile.Storage.S3; s3 != nil {
					fmt.Fprintf(h, "pitr.profile.s3.bucket=%s\n", s3.Bucket)
					fmt.Fprintf(h, "pitr.profile.s3.prefix=%s\n", s3.Prefix)
					fmt.Fprintf(h, "pitr.profile.s3.region=%s\n", s3.Region)
					fmt.Fprintf(h, "pitr.profile.s3.endpointURL=%s\n", s3.EndpointURL)
					fmt.Fprintf(h, "pitr.profile.s3.credentialsSecret=%s\n", s3.CredentialsSecret)
				}
				if pvc := profile.Storage.PVC; pvc != nil {
					fmt.Fprintf(h, "pitr.profile.pvc.claimName=%s\n", pvc.ClaimName)
					fmt.Fprintf(h, "pitr.profile.pvc.storageClassName=%s\n", pvc.StorageClassName)
					fmt.Fprintf(h, "pitr.profile.pvc.size=%s\n", pvc.Size.String())
					fmt.Fprintf(h, "pitr.profile.pvc.subPath=%s\n", pvc.SubPath)
				}
			}
		}
	}
	// Encryption-at-rest rendering depends on the site's observed
	// keyring phase, not just on the spec, so the hash has to include
	// it: flipping a site from unsealed to sealed changes the keyring
	// volume from a memory emptyDir to a Secret projection and the
	// component config from read_only:false to true, and that has to
	// roll the pod through the ordered-update path. Hashing the
	// escrow Secret NAME (not its contents) is deliberate — versions are
	// immutable, so the name uniquely identifies the bytes, and the
	// operator never needs to read key material to compute a hash.
	if fg.Spec.EncryptionEnabled() {
		kr := fg.Spec.EffectiveKeyring()
		sealed := fg.SiteKeyringSealed(site.Name)
		fmt.Fprintf(h, "encryption.renderVersion=%s\n", encryptionPodRenderVersion)
		fmt.Fprintf(h, "encryption.sealed=%t\n", sealed)
		fmt.Fprintf(h, "encryption.escrowSecret=%s\n", siteEscrowSecretName(fg, site.Name))
		fmt.Fprintf(h, "encryption.rotate=%t\n", siteKeyringRotating(fg, site.Name))
		fmt.Fprintf(h, "encryption.escrowURL=%s\n", defaultKeyringEscrowURL(fg))
		fmt.Fprintf(h, "encryption.dataFileDir=%s\n", kr.DataFileDir)
		fmt.Fprintf(h, "encryption.mysqldDir=%s\n", kr.MysqldDir)
		fmt.Fprintf(h, "encryption.pluginDir=%s\n", kr.PluginDir)
		fmt.Fprintf(h, "encryption.keyringConfig=%s\n",
			keyringComponentConfigJSON(fg.Spec.KeyringDataFilePath(), sealed))
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

func sitePeerAddresses(fg *v1alpha1.MysqlFailoverGroup, siteName string) []string {
	peerNames := fg.Spec.PeerSiteNames(siteName)
	peerAddrs := make([]string, len(peerNames))
	for i, name := range peerNames {
		peerAddrs[i] = fmt.Sprintf("%s:%d", internalSiteServiceHost(fg.Name, name, fg.Namespace), sidecarPort)
	}
	return peerAddrs
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
	var connectionDrainTimeout int64
	if fg.Spec.ConnectionDrainTimeout != nil {
		connectionDrainTimeout = int64(fg.Spec.ConnectionDrainTimeout.Duration)
	}

	var sitePriorities []string
	if fg.Spec.SplitBrainPolicy != nil && len(fg.Spec.SplitBrainPolicy.SitePriorities) > 0 {
		sitePriorities = append(sitePriorities, fg.Spec.SplitBrainPolicy.SitePriorities...)
	}

	sites := make([]SiteTopologyConfig, len(fg.Spec.Sites))
	for i, s := range fg.Spec.Sites {
		sites[i] = SiteTopologyConfig{
			Name:          s.Name,
			Zone:          s.Zone,
			LBIP:          s.LBIP,
			Role:          state.SiteRole(s.EffectiveRole()),
			TaintSelector: taintNodeSelectorString(s.TaintNodeSelector),
			Host:          internalSiteServiceHost(fg.Name, s.Name, fg.Namespace),
		}
	}

	return TopologyConfig{
		Namespace:              fg.Namespace,
		Name:                   fg.Name,
		Sites:                  sites,
		PollInterval:           pollInterval,
		FailureThreshold:       int(failureThreshold),
		RecoveryThreshold:      int(recoveryThreshold),
		FailoverCooldown:       failoverCooldown,
		ConnectionDrainTimeout: connectionDrainTimeout,
		MaxLagSeconds:          fg.Spec.EffectiveMaxLagSeconds(),
		ReadOnlyMaxLagSeconds:  fg.Spec.EffectiveReadOnlyMaxLagSeconds(),
		SitePriorities:         sitePriorities,
		DragonflyEnabled:       dragonflyEnabled(fg),
	}
}

func (r *MysqlFailoverGroupReconciler) cleanupLegacyConfigMap(ctx context.Context, fg *v1alpha1.MysqlFailoverGroup) error {
	reader := client.Reader(r.Client)
	if r.APIReader != nil {
		reader = r.APIReader
	}
	legacy := fmt.Sprintf("mysql-%s-config", fg.Name)
	desiredConfigs := make(map[string]string, len(fg.Spec.Sites))
	for _, site := range fg.Spec.Sites {
		desiredConfig := desiredSiteConfigMapName(fg, site)
		var cm corev1.ConfigMap
		if err := reader.Get(ctx, types.NamespacedName{Namespace: fg.Namespace, Name: desiredConfig}, &cm); err != nil {
			return nil
		}
		desiredConfigs[resourceName(fg.Name, site.Name)] = desiredConfig
	}

	var deployments appsv1.DeploymentList
	if err := reader.List(ctx, &deployments,
		client.InNamespace(fg.Namespace),
		client.MatchingLabels{labelAppName: "mysql", labelInstance: fg.Name},
	); err != nil {
		return nil
	}
	seenDesired := make(map[string]bool, len(desiredConfigs))
	for i := range deployments.Items {
		deployment := &deployments.Items[i]
		configName := deploymentConfigMapName(deployment)
		if expected, desired := desiredConfigs[deployment.Name]; desired {
			seenDesired[deployment.Name] = true
			if configName != expected {
				return nil
			}
			continue
		}
		if configName == legacy {
			return nil
		}
	}
	for deploymentName := range desiredConfigs {
		if !seenDesired[deploymentName] {
			return nil
		}
	}
	cm := &corev1.ConfigMap{ObjectMeta: metav1.ObjectMeta{Name: legacy, Namespace: fg.Namespace}}
	if err := r.Delete(ctx, cm); err != nil && !errors.IsNotFound(err) {
		return err
	}
	return nil
}

func deploymentConfigMapName(deployment *appsv1.Deployment) string {
	for _, volume := range deployment.Spec.Template.Spec.Volumes {
		if volume.Name == "config" && volume.ConfigMap != nil {
			return volume.ConfigMap.Name
		}
	}
	return ""
}

func deploymentRolloutComplete(dep *appsv1.Deployment) bool {
	desired := int32(1)
	if dep.Spec.Replicas != nil {
		desired = *dep.Spec.Replicas
	}
	return dep.Status.ObservedGeneration >= dep.Generation &&
		dep.Status.UpdatedReplicas == desired &&
		dep.Status.AvailableReplicas == desired &&
		dep.Status.Replicas == dep.Status.UpdatedReplicas
}

// cleanupObsoleteSiteConfigMaps removes superseded encrypted ConfigMap
// revisions only after the Deployment has completed the switch to the desired
// revision and no old pod remains. An interrupted rollout therefore preserves
// the old pod's exact configuration and remains restartable.
func (r *MysqlFailoverGroupReconciler) cleanupObsoleteSiteConfigMaps(ctx context.Context, fg *v1alpha1.MysqlFailoverGroup) error {
	if !fg.Spec.EncryptionEnabled() {
		return nil
	}
	reader := client.Reader(r.Client)
	if r.APIReader != nil {
		reader = r.APIReader
	}
	for _, site := range fg.Spec.Sites {
		var dep appsv1.Deployment
		nn := types.NamespacedName{
			Namespace: fg.Namespace,
			Name:      resourceName(fg.Name, site.Name),
		}
		if err := reader.Get(ctx, nn, &dep); err != nil {
			if errors.IsNotFound(err) {
				continue
			}
			return err
		}
		desired := desiredSiteConfigMapName(fg, site)
		if deploymentConfigMapName(&dep) != desired || !deploymentRolloutComplete(&dep) {
			continue
		}

		var configs corev1.ConfigMapList
		if err := r.List(ctx, &configs,
			client.InNamespace(fg.Namespace),
			client.MatchingLabels{
				labelFailoverGroup: fg.Name,
				labelSite:          site.Name,
				labelManagedBy:     managerName,
			}); err != nil {
			return err
		}
		for i := range configs.Items {
			if configs.Items[i].Name == desired {
				continue
			}
			if err := r.Delete(ctx, &configs.Items[i]); err != nil && !errors.IsNotFound(err) {
				return err
			}
		}

		// Pre-v1 encrypted groups used the mutable canonical name and did
		// not label it with the site, so remove it explicitly after the
		// content-addressed rollout is complete.
		canonical := &corev1.ConfigMap{ObjectMeta: metav1.ObjectMeta{
			Name: siteConfigMapName(fg.Name, site.Name), Namespace: fg.Namespace,
		}}
		if canonical.Name != desired {
			if err := r.Delete(ctx, canonical); err != nil && !errors.IsNotFound(err) {
				return err
			}
		}
	}
	return nil
}

func taintNodeSelectorString(selector map[string]string) string {
	return labels.SelectorFromSet(labels.Set(selector)).String()
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
			if err := r.reconcileSiteConfigMap(ctx, &fg, site); err != nil {
				return fmt.Errorf("reconcile site configmap: %w", err)
			}
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
		if err == nil && deploymentRolloutComplete(&dep) {
			return nil
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
