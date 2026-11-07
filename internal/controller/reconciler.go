package controller

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	stderrors "errors"
	"fmt"
	"log/slog"
	"os"
	"regexp"
	"slices"
	"sort"
	"strconv"
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
	"sigs.k8s.io/controller-runtime/pkg/builder"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
	"sigs.k8s.io/controller-runtime/pkg/handler"
	"sigs.k8s.io/controller-runtime/pkg/log"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	v1alpha1 "github.com/shipstream/bloodraven/api/v1alpha1"
	"github.com/shipstream/bloodraven/internal/clock"
	"github.com/shipstream/bloodraven/internal/license"
	"github.com/shipstream/bloodraven/internal/platform"
	"github.com/shipstream/bloodraven/internal/sidecar"
)

const (
	defaultMySQLImage = "mysql:9.7"

	labelAppName   = "app.kubernetes.io/name"
	labelInstance  = "app.kubernetes.io/instance"
	labelManagedBy = "app.kubernetes.io/managed-by"
	managerName    = "bloodraven"

	configMapRenderVersion = "site-config-v2-content-addressed-encryption"

	// Bump when the rendered MySQL Deployment pod spec changes without a
	// corresponding user-facing spec field change, so existing pods roll
	// forward to the new safe defaults.
	deploymentPodRenderVersion = "deployment-pod-render-v7-prestop-mysql-pwd"

	// sidecarLivenessPath is the sidecar's liveness probe target. It must
	// not depend on MySQL: the sidecar fences MySQL when MySQL misbehaves,
	// and a liveness probe that follows MySQL (as /health once did)
	// restarts the fencing agent during the stalls it exists to handle.
	//
	// /peer/ping answers unconditionally from the HTTP server and is served
	// by every sidecar release. The sidecar's /livez also covers a wedged
	// fencing loop, but an older sidecar answers 404 to it, and a pinned
	// spec.sidecar.image may be older than the operator (the upgrade
	// policy allows a one-minor skew). A /livez probe would crash-loop
	// every such sidecar. Switch to /livez once every supported sidecar
	// serves it.
	sidecarLivenessPath = "/peer/ping"

	// sidecarReadinessPath reports local MySQL connectivity. Readiness is
	// meant to follow MySQL; liveness is not.
	sidecarReadinessPath = "/health"

	mysqlPort   = 3306
	sidecarPort = 8080
)

func defaultOperatorEndpoint(fg *v1alpha1.MysqlFailoverGroup) string {
	if fg.Spec.Sidecar.OperatorEndpoint != "" {
		return fg.Spec.Sidecar.OperatorEndpoint
	}
	if addr := strings.TrimSpace(os.Getenv("BLOODRAVEN_DEFAULT_OPERATOR_ENDPOINT")); addr != "" {
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
	// cache view — including backup retry conflict transitions and
	// waitForDeploymentRollout, which must observe
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
	// dragonflyGateWait overrides how long a reconcile waits for the
	// group's Dragonfly gate (default defaultDragonflyGateWait). Tests shorten it.
	dragonflyGateWait time.Duration

	// PlannedTopology is the planned-failover engine's topology facade. Nil
	// means production: the facade over Runner. Tests inject a fake.
	PlannedTopology PlannedFailoverTopology
	// plannedClock overrides the planned-failover engine's clock (tests).
	plannedClock clock.Clock
	// encryptionClock overrides the clock stamped on encryption status
	// (tests that compare status writes byte for byte across passes).
	encryptionClock clock.Clock
	// plannedEngine is built on first use (plannedFailoverEngine) and kept:
	// its memo of what this process sent must survive between passes.
	plannedEngineOnce sync.Once
	plannedEngine     *PlannedFailoverEngine

	// keyringStatus overrides the production sidecar-backed fetcher used
	// by the encryption-at-rest state machine. Tests inject a scripted
	// fake; production wiring leaves this nil so
	// defaultKeyringStatusFetcher is used.
	keyringStatus keyringStatusFetcher

	// restoreRecloneIdle records, per group, when this process first saw
	// the restore reclone queue's request not running (see
	// restoreRecloneStalled).
	restoreRecloneMu   sync.Mutex
	restoreRecloneIdle map[types.NamespacedName]restoreRecloneWait

	// dragonflyUpgradeDeferNote is the last cause each group's waiting
	// snapshot-restore upgrade reported (noteDragonflyUpgradeDeferral), so
	// DragonflyUpgradeDeferred is emitted when the cause changes, not on
	// every retry.
	dragonflyUpgradeDeferMu   sync.Mutex
	dragonflyUpgradeDeferNote map[types.NamespacedName]string

	// Logger is the operational slog stream. Nil is treated as discard
	// so existing tests that construct the reconciler without a logger
	// do not panic when license observation runs.
	Logger *slog.Logger

	// OperatorLicense is the optional operator-level JWT (flag/env).
	OperatorLicense string

	// LicenseKeys overrides the production trust store. Nil uses
	// license.ProductionKey.
	LicenseKeys license.KeyLookup

	// Now overrides time.Now for tests: license verification and the
	// encryption coverage LastCheckTime stamp.
	Now func() time.Time

	licenseMu  sync.Mutex
	licenseObs map[types.NamespacedName]licenseObservation

	dragonflyRolloutMu      sync.Mutex
	dragonflyRolloutBackoff map[dragonflyRolloutKey]dragonflyRolloutState

	restorePrivMu       sync.Mutex
	restorePrivRejected map[types.NamespacedName]restorePrivilegeCheck

	// restorePeerStopStart records, per group, when this process first
	// tried to stop the peers' replication receivers for a restore run
	// (see stopRestorePeerReplication).
	restorePeerStopMu    sync.Mutex
	restorePeerStopStart map[types.NamespacedName]restorePeerStopWait
	// restoreSettleNoted is the (previous queue, requested confirm) a
	// RestoreInPlaceWaitingForPreviousQueue Event was last emitted for.
	// Guarded by restorePeerStopMu.
	restoreSettleNoted map[types.NamespacedName]string
	// restoreGTIDReadFailures records the first failed read of the
	// primary's gtid_executed per settlement (restorePrimaryUnchanged).
	// Guarded by restorePeerStopMu.
	restoreGTIDReadFailures map[types.NamespacedName]restoreGTIDReadFailure

	// pitrRenderLog is what logPITRRender last logged per group, so the
	// render log lines are written on change only.
	pitrRenderLogMu sync.Mutex
	pitrRenderLog   map[string]pitrRenderLogState

	// pitrRenderLocks serializes, per group, the ordered updater's
	// Deployment render (applySiteDeployment) with a PITR binding
	// release (G1): a release must not delete a key snapshot between
	// its in-use check and a render that starts mounting it. Guarded by
	// pitrRenderLocksMu; entries are reference counted.
	pitrRenderLocksMu sync.Mutex
	pitrRenderLocks   map[types.NamespacedName]*pitrRenderLock

	// scheduleRefused is the read-only reason a BackupScheduleRefused
	// Event was last emitted with, per group schedule.
	scheduleRefusedMu sync.Mutex
	scheduleRefused   map[string]string
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
// versions) and its per-site escrow tokens. Update and patch are for the
// PITR archive key snapshots (#203): their data is immutable, but the
// operator writes the acceptance annotation and verification holds on
// their metadata, and deletes them only on an explicit release.
// +kubebuilder:rbac:groups="",resources=secrets,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=shipstream.io,resources=mysqlbackupverifications,verbs=get;list;watch
// +kubebuilder:rbac:groups="",resources=events,verbs=create;patch
// +kubebuilder:rbac:groups=externaldns.k8s.io,resources=dnsendpoints,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=batch,resources=jobs,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=batch,resources=cronjobs,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=shipstream.io,resources=mysqlbackups,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=authentication.k8s.io,resources=tokenreviews,verbs=create
// Leader-election lease. The operator runs a single-replica deployment
// today but still uses leader election so a fresh pod doesn't step on
// a not-yet-drained predecessor. Without this marker,
// `kubectl apply -f config/rbac/` installs (the documented non-Helm
// path) produce an operator that crash-loops at startup. AUDIT M3.
// +kubebuilder:rbac:groups=coordination.k8s.io,resources=leases,verbs=get;list;watch;create;update;patch;delete

func (r *MysqlFailoverGroupReconciler) Reconcile(ctx context.Context, req ctrl.Request) (result ctrl.Result, retErr error) {
	logger := log.FromContext(ctx)

	var fg v1alpha1.MysqlFailoverGroup
	if err := r.Get(ctx, req.NamespacedName, &fg); err != nil {
		if errors.IsNotFound(err) {
			r.forgetLicense(req.NamespacedName)
			r.forgetGroupMemos(req.NamespacedName)
			return ctrl.Result{}, nil
		}
		return ctrl.Result{}, err
	}

	// Handle deletion with finalizer
	if !fg.DeletionTimestamp.IsZero() {
		r.forgetLicense(req.NamespacedName)
		r.forgetGroupMemos(req.NamespacedName)
		if controllerutil.ContainsFinalizer(&fg, v1alpha1.FailoverGroupFinalizer) {
			logger.Info("CR being deleted, running finalizer cleanup", "name", fg.Name)

			// Stop the topology manager for this CR.
			if r.Runner != nil {
				r.Runner.StopManager(req.NamespacedName)
			}

			if err := r.handleDeletion(ctx, &fg); err != nil {
				return ctrl.Result{}, fmt.Errorf("handle deletion: %w", err)
			}
			controllerutil.RemoveFinalizer(&fg, v1alpha1.FailoverGroupFinalizer)
			if err := r.Update(ctx, &fg); err != nil {
				return ctrl.Result{}, fmt.Errorf("remove finalizer: %w", err)
			}
		}
		return ctrl.Result{}, nil
	}

	// The planned-failover engine is the only driver of an in-flight run's
	// deadlines (fence, lag, promotion, rollback, the status stall, manager
	// absence) and of a hold's acknowledgement. A reconcile that returns
	// before the regular planned-failover step below, on an error or a
	// requeue of an earlier step, still runs one pass for such a run, or
	// a fenced source could stay fenced for as long as that step fails.
	// New requests are admitted only by the regular step.
	pfDriven := false
	defer func() {
		if !pfDriven {
			result, retErr = r.drivePlannedFailoverOnEarlyReturn(ctx, &fg, result, retErr)
		}
	}()

	if m := r.deploymentLeaseManager(); m != nil {
		if err := m.reconcileRevocation(ctx, &fg); err != nil {
			return ctrl.Result{}, fmt.Errorf("revoke deployment leases: %w", err)
		}
	}

	// Push hostname/TTL before any later early return so a DNS rename is
	// visible to the next topology poll even if this reconcile bails on
	// restore, planned failover, or another subsystem.
	if r.Runner != nil {
		r.Runner.SetDNSRecordSpec(req.NamespacedName, fg.Spec.DNS.Hostname, fg.Spec.DNS.TTLSeconds, fg.Generation)
		r.Runner.SetCloneTimeout(req.NamespacedName, cloneTimeoutFromSpec(&fg))
	}

	// Ensure finalizer is present.
	if !controllerutil.ContainsFinalizer(&fg, v1alpha1.FailoverGroupFinalizer) {
		controllerutil.AddFinalizer(&fg, v1alpha1.FailoverGroupFinalizer)
		if err := r.Update(ctx, &fg); err != nil {
			return ctrl.Result{}, fmt.Errorf("add finalizer: %w", err)
		}
	}

	logger.Info("reconciling MysqlFailoverGroup", "name", fg.Name)

	// License observation is advisory only. It never returns an error
	// and must not change the rest of reconcile.
	r.observeLicense(&fg)

	image := fg.Spec.Image
	if image == "" {
		image = defaultMySQLImage
	}

	// Validate that the referenced credential secret(s) exist and contain expected keys.
	if result, err := r.validateCredentialSecrets(ctx, &fg); err != nil || result.RequeueAfter > 0 {
		return result, err
	}

	r.warnOnSidecarVersionSkew(ctx, &fg)

	// The runner's drift check only reads the digest key, so it must exist
	// even when every Deployment is deferred to the ordered update.
	if _, err := ensureDigestKey(ctx, r.Client, r.APIReader, r.Scheme, &fg); err != nil {
		return ctrl.Result{}, fmt.Errorf("ensure secret digest key: %w", err)
	}

	// Encryption-at-rest must settle before anything renders: both the
	// per-site ConfigMap (which carries "read_only" for the keyring
	// component) and the Deployment (which picks a Secret projection or
	// a memory-backed emptyDir for the keyring volume) are driven by the
	// per-site keyring phase this call maintains.
	encryptionRequeueAfter, err := r.reconcileEncryptionAtRest(ctx, &fg)
	if stderrors.Is(err, errEncryptionStatusMoved) {
		// fg's keyring state was computed from a stale baseline and not
		// persisted. Rendering it could move a site's keyring projection
		// against the durable lifecycle (sealing a site mid-rotation, or
		// unsealing a sealed one), so nothing renders this pass.
		logger.V(1).Info("encryption status moved under this reconcile; requeueing before rendering")
		return ctrl.Result{RequeueAfter: encryptionRequeueAfter}, nil
	}
	if err != nil {
		return ctrl.Result{}, fmt.Errorf("reconcile encryption at rest: %w", err)
	}

	// PITR archive bindings (#203) settle before anything renders: the
	// render below reads status.pitrArchiveBindings, which this pass fills
	// in memory (and persists).
	bindingRequeue, err := r.reconcilePITRArchiveBindings(ctx, &fg)
	if stderrors.Is(err, errPITRBindingStatusMoved) {
		// The pass read a cached status older than the stored one: its
		// bindings were not written and must not drive a render.
		logger.V(1).Info("pitr archive binding status moved under this reconcile; requeueing before rendering")
		return ctrl.Result{RequeueAfter: pitrBindingMovedRequeue}, nil
	}
	if err != nil {
		return ctrl.Result{}, fmt.Errorf("reconcile pitr archive bindings: %w", err)
	}

	// The PITR render is resolved once per group (§4.6) and applied to
	// every site, including sites being created.
	pitrRender, err := r.resolveGroupPITRRender(ctx, &fg)
	if err != nil {
		return ctrl.Result{}, err
	}

	// Desired per-site ConfigMaps are created before any Deployment reference
	// changes. This ordering makes migration from the legacy shared map safe.
	if err := r.reconcileSiteConfigMaps(ctx, &fg, pitrRender); err != nil {
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
	recreateUpdates := fg.Spec.UpdateStrategy.EffectiveType() == v1alpha1.UpdateStrategyRecreate

	// With OrderedUpdate (including its defaulted empty value in unit tests),
	// defer every existing Deployment to the ordered update path: the
	// reconciler firing on a CR spec change must not restart both sites
	// simultaneously. The runner's
	// checkSpecDrift compares the desired hash against the live Deployment
	// annotation, so leaving the existing Deployment untouched is what causes
	// drift to be observed and the ordered update to start. New Deployments
	// (initial bootstrap) are always created here. Recreate explicitly opts into
	// bulk reconciliation, so all existing site Deployments are patched in one
	// pass and their pod restarts may overlap. An already-running ordered update
	// remains authoritative even if the field changes mid-rollout.
	deploymentReader := client.Reader(r.Client)
	if r.APIReader != nil {
		deploymentReader = r.APIReader
	}

	// Reconcile per-site resources
	for _, site := range fg.Spec.Sites {
		serverID, err := site.MySQLServerID()
		if err != nil {
			return ctrl.Result{}, err
		}

		if err := r.reconcilePVC(ctx, &fg, site); err != nil {
			return ctrl.Result{}, fmt.Errorf("reconcile pvc %s: %w", site.Name, err)
		}
		if !orderedUpdateActive {
			deferDeployment := !recreateUpdates
			var existing appsv1.Deployment
			if deferDeployment {
				deployNN := types.NamespacedName{
					Namespace: fg.Namespace,
					Name:      resourceName(fg.Name, site.Name),
				}
				if err := deploymentReader.Get(ctx, deployNN, &existing); errors.IsNotFound(err) {
					deferDeployment = false
				} else if err != nil {
					return ctrl.Result{}, fmt.Errorf("get deployment %s: %w", site.Name, err)
				}
			}
			deployName := resourceName(fg.Name, site.Name)
			if deferDeployment {
				r.noteWorkloadSelector(&fg, "Deployment", deployName, deploymentSelectorMismatch(&existing, &fg, site.Name))
			} else {
				err := r.reconcileDeployment(ctx, &fg, site, serverID, image, pitrRender)
				var mismatch *workloadSelectorMismatchError
				if stderrors.As(err, &mismatch) {
					// Refused, not retried: the next reconcile checks again
					// without writing.
					r.noteWorkloadSelector(&fg, "Deployment", deployName, mismatch)
				} else if err != nil {
					return ctrl.Result{}, fmt.Errorf("reconcile deployment %s: %w", site.Name, err)
				} else {
					r.noteWorkloadSelector(&fg, "Deployment", deployName, nil)
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
	if err := r.cleanupObsoleteSiteConfigMaps(ctx, &fg, pitrRender.inputs); err != nil {
		return ctrl.Result{}, fmt.Errorf("cleanup obsolete site configmaps: %w", err)
	}
	if err := r.cleanupLegacyConfigMap(ctx, &fg, pitrRender.inputs); err != nil {
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

	// A snapshot-restore upgrade run in flight owns the Dragonfly sites and
	// holds the reconcile here; a run that waits for admission only
	// contributes a requeue, so the planned failover it may wait for is
	// still driven below.
	dfUpgrade, err := r.reconcileDragonflySnapshotUpgrade(ctx, &fg)
	if err != nil {
		return ctrl.Result{}, fmt.Errorf("reconcile dragonfly snapshot upgrade: %w", err)
	} else if dfUpgrade.hold > 0 {
		return ctrl.Result{RequeueAfter: dfUpgrade.hold}, nil
	}

	// Reconcile per-site Dragonfly resources when spec.dragonfly is enabled.
	// When Dragonfly is disabled, reconcileDragonflyResources actively removes
	// any previously managed Dragonfly resources so MySQL-only deployments are
	// unaffected.
	deferredRequeue := minPositiveDuration(bindingRequeue, dfUpgrade.wait)
	if dragonflyRequeue, err := r.reconcileDragonflyResources(ctx, &fg); err != nil {
		return ctrl.Result{}, fmt.Errorf("reconcile dragonfly resources: %w", err)
	} else if dragonflyRequeue > 0 {
		deferredRequeue = minPositiveDuration(deferredRequeue, dragonflyRequeue)
	}

	// Reconcile MySQL users.
	if err := r.reconcileCredentials(ctx, &fg); err != nil {
		logger.Error(err, "credential reconciliation failed, will retry")
		r.Recorder.Eventf(&fg, corev1.EventTypeWarning, v1alpha1.EventCredentialReconcileFailed,
			"failed to reconcile MySQL users: %v", err)
		// Nothing else wakes this up: the step can be waiting on a
		// site that comes back without touching the CR.
		deferredRequeue = minPositiveDuration(deferredRequeue, credentialRetryInterval)
	}

	// Drive the one-shot restore Job when spec.initFromBackup is set.
	// If a restore is still in flight we requeue early after the pod
	// label sync, which keeps applications off the group until the
	// restore succeeds (as do the per-site Services above and the
	// topology manager's DNS gate). A parallel safety gate lives in the
	// topology runner:
	// TopologyManagerRunner.sync() calls
	// TopologyManager.SetAutoBootstrapSuppressed(restoreInFlight) every
	// 30s, which prevents the fresh-deploy auto-clone path in
	// applyCrossSiteAction from racing the restore Job for the primary
	// data directory, while still letting it seed (fence the other sites,
	// make the seed writable and publish it active), which the Job waits
	// for. Together the two gates ensure the replica is only cloned after
	// the primary has been loaded from the dump.
	restoreRequeue, err := r.reconcileRestoreJob(ctx, &fg)
	if err != nil {
		return ctrl.Result{}, fmt.Errorf("reconcile restore job: %w", err)
	}
	// The restore phase feeds the Ready condition, which the topology
	// manager writes only on topology changes: have it rewrite status now
	// if the phase moved (the runner's sync also catches it).
	if r.Runner != nil {
		r.Runner.RefreshInitRestoreReadiness(req.NamespacedName, &fg)
	}
	if restoreRequeue > 0 {
		// Still sweep the labels: while the restore has not succeeded
		// syncPodLabels takes every pod out of the application Services,
		// and that must hold for pods recreated mid-restore too.
		if err := r.syncPodLabels(ctx, &fg); err != nil {
			return ctrl.Result{}, fmt.Errorf("sync pod labels during initFromBackup restore: %w", err)
		}
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
		// A restore in Preflight waits for a planned failover already in
		// flight, and for a held claim or automation hold
		// (inPlacePreflight). Keep driving the engine on this pass (its
		// deadlines, a hold's acknowledgement), or the two wait for each
		// other. A new request waits until the restore returns.
		pfDriven = true
		pfRequeue, pfRan, err := r.reconcilePlannedFailoverInFlight(ctx, &fg)
		if err != nil {
			return ctrl.Result{}, fmt.Errorf("reconcile planned failover during in-place restore: %w", err)
		}
		inPlaceRequeue = minPositiveDuration(inPlaceRequeue, pfRequeue)
		// Still sync labels before we bounce: the fence needs pod labels
		// re-applied on the same pass that transitions into Fencing.
		if err := r.syncPodLabels(ctx, r.plannedPostPassView(ctx, &fg, pfRan)); err != nil {
			return ctrl.Result{}, fmt.Errorf("sync pod labels during in-place restore: %w", err)
		}
		return ctrl.Result{RequeueAfter: inPlaceRequeue}, nil
	}
	// A standing privilege rejection is re-checked on a timer: nothing
	// else wakes a quiet group once the grant is in place.
	deferredRequeue = minPositiveDuration(deferredRequeue, r.restorePrivilegeRecheckRequeue(&fg))

	// The offline_mode fence of a finished full-instance restore is
	// released (the prior value put back) after the restore ends. It runs
	// on every reconcile and only contributes a requeue; it never holds up
	// the rest of the pass.
	fenceRequeue, err := r.reconcileRestoreOfflineFence(ctx, &fg)
	if err != nil {
		return ctrl.Result{}, fmt.Errorf("release restore offline_mode fence: %w", err)
	}
	deferredRequeue = minPositiveDuration(deferredRequeue, fenceRequeue)
	// Likewise the peer reclone queue drains after that restore has
	// finished. It runs after the release: a replication account without
	// CONNECTION_ADMIN cannot clone from a primary still under the fence.
	reclonesRequeue, err := r.reconcileRestoreReclones(ctx, &fg)
	if err != nil {
		return ctrl.Result{}, fmt.Errorf("reconcile restore reclone queue: %w", err)
	}
	deferredRequeue = minPositiveDuration(deferredRequeue, reclonesRequeue)

	// Drive the planned-failover engine when a request, a run, a hold or
	// a held claim is pending. This must run AFTER reconcileInPlaceRestore
	// (the two guard against each other) and BEFORE syncPodLabels, which
	// reads the group again after the pass, so the source's role label is
	// stripped on the same reconcile that records the fence. Each pass
	// reads the group uncached, so operator restarts resume from the
	// durable records. Only a run that owns the sites or is Publishing, or
	// a Run-mode claim, returns early: a pass for a run that owns nothing
	// (Admitting or Deferred without a claim, a standing automation hold,
	// a marker retry) only contributes a requeue and does not hold up the
	// rest of the reconcile, so backups, keyring and label work keep
	// running for as long as it waits.
	pfDriven = true
	pfRan := r.plannedFailoverPassRuns(&fg)
	if pfRequeue, err := r.reconcilePlannedFailover(ctx, &fg); err != nil {
		return ctrl.Result{}, fmt.Errorf("reconcile planned failover: %w", err)
	} else if pfRequeue > 0 && r.plannedFailoverHoldsReconcile(&fg) {
		if err := r.syncPodLabels(ctx, r.plannedPostPassView(ctx, &fg, pfRan)); err != nil {
			return ctrl.Result{}, fmt.Errorf("sync pod labels during planned failover: %w", err)
		}
		return ctrl.Result{RequeueAfter: pfRequeue}, nil
	} else {
		deferredRequeue = minPositiveDuration(deferredRequeue, pfRequeue)
	}

	// Sync pod labels based on status
	if err := r.syncPodLabels(ctx, r.plannedPostPassView(ctx, &fg, pfRan)); err != nil {
		return ctrl.Result{}, fmt.Errorf("sync pod labels: %w", err)
	}

	// Mirror the same sweep for Dragonfly pods so the active service
	// selector follows the active site (or the in-flight planned-
	// failover target). No-op when spec.dragonfly is disabled.
	if err := r.syncDragonflyPodLabels(ctx, &fg); err != nil {
		return ctrl.Result{}, fmt.Errorf("sync dragonfly pod labels: %w", err)
	}

	// Backup work runs after the label syncs above: those labels route the
	// primary and replica Services, so a failing backup step must not
	// leave them stale.
	if err := r.reconcileS3CredentialsCondition(ctx, &fg); err != nil {
		return ctrl.Result{}, fmt.Errorf("check S3 credentials secrets: %w", err)
	}
	if err := r.reconcilePITRMySQLConfCondition(ctx, &fg); err != nil {
		return ctrl.Result{}, fmt.Errorf("report refused PITR mysqlConf: %w", err)
	}

	if err := r.reconcileScheduleTriggerRBAC(ctx, &fg); err != nil {
		return ctrl.Result{}, fmt.Errorf("reconcile schedule trigger rbac: %w", err)
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
		// PITR archive bindings (#203): a verification adds or drops a
		// read target, and an archive key snapshot (ownerless, so Owns
		// would see nothing) is mapped back by its group label.
		Watches(&v1alpha1.MysqlBackupVerification{}, handler.EnqueueRequestsFromMapFunc(r.mapVerificationToGroup)).
		Watches(&corev1.Secret{}, handler.EnqueueRequestsFromMapFunc(r.mapPITRKeySnapshotToGroup),
			builder.WithPredicates(pitrKeySnapshotPredicate())).
		Complete(r)
}

// secretToFailoverGroup maps a Secret change to the MysqlFailoverGroups that reference it
// (via spec.credentials.*, spec.tls.secretName or a backup passphrase), triggering
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
	r.Recorder.Event(fg, corev1.EventTypeNormal, v1alpha1.EventGracefulShutdown, "Starting graceful shutdown sequence")

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
			if err := r.Tainter.SetTaint(ctx, selector, fg.Namespace, fg.Name, false); err != nil {
				logger.Error(err, "failed to remove taint during shutdown", "site", site.Name)
				// Continue cleanup despite taint removal failure
			}
		}
	}

	// DNSEndpoint has an owner reference and will be garbage-collected automatically.
	logger.Info("CR deleted — DNSEndpoint will be garbage-collected",
		"hostname", fg.Spec.DNS.Hostname)

	r.Recorder.Event(fg, corev1.EventTypeNormal, v1alpha1.EventGracefulShutdown, "Graceful shutdown complete, removing finalizer")
	return nil
}

// validateCredentialSecrets validates that the referenced credential
// secrets exist and contain the expected keys.
func (r *MysqlFailoverGroupReconciler) validateCredentialSecrets(ctx context.Context, fg *v1alpha1.MysqlFailoverGroup) (ctrl.Result, error) {
	operatorSecretName := fg.Spec.Credentials.OperatorSecretName
	var secret corev1.Secret
	secretKey := types.NamespacedName{Namespace: fg.Namespace, Name: operatorSecretName}
	if err := r.Get(ctx, secretKey, &secret); err != nil {
		if !errors.IsNotFound(err) {
			return ctrl.Result{}, fmt.Errorf("get operator secret %s: %w", operatorSecretName, err)
		}
		r.Recorder.Eventf(fg, corev1.EventTypeWarning, v1alpha1.EventSecretNotFound,
			"operator secret %q not found", operatorSecretName)
		return ctrl.Result{RequeueAfter: 30 * time.Second}, nil
	}
	for _, key := range []string{"username", "password", "MYSQL_ROOT_PASSWORD"} {
		// An empty username or password is refused like a missing one: the
		// manager would dial an empty account and report every site
		// unreachable. MYSQL_ROOT_PASSWORD only has to be present, as the
		// admin connection skips root when it is empty.
		if problem := credentialKeyProblem(secret.Data, key, key != "MYSQL_ROOT_PASSWORD"); problem != "" {
			r.Recorder.Eventf(fg, corev1.EventTypeWarning, v1alpha1.EventSecretMissingKey,
				"operator secret %q %s %q", operatorSecretName, problem, key)
			return ctrl.Result{RequeueAfter: 30 * time.Second}, nil
		}
	}
	if msg := replicationAccountConflict(secret.Data); msg != "" {
		r.Recorder.Eventf(fg, corev1.EventTypeWarning, v1alpha1.EventSecretMissingKey,
			"operator secret %q: %s", operatorSecretName, msg)
		return ctrl.Result{RequeueAfter: 30 * time.Second}, nil
	}

	// Validate optional credential secrets have required keys.
	type optionalSecret struct {
		name string
		role string
	}
	for _, opt := range []optionalSecret{
		{fg.Spec.Credentials.AppSecretName, "app"},
		{fg.Spec.Credentials.ReadOnlySecretName, "read-only"},
		{fg.Spec.Credentials.MonitorSecretName, "monitor"},
		{fg.Spec.Credentials.BackupSecretName, "backup"},
	} {
		if opt.name == "" {
			continue
		}
		var s corev1.Secret
		if err := r.Get(ctx, types.NamespacedName{Namespace: fg.Namespace, Name: opt.name}, &s); err != nil {
			if !errors.IsNotFound(err) {
				return ctrl.Result{}, fmt.Errorf("get %s secret %s: %w", opt.role, opt.name, err)
			}
			r.Recorder.Eventf(fg, corev1.EventTypeWarning, v1alpha1.EventSecretNotFound,
				"%s secret %q not found", opt.role, opt.name)
			return ctrl.Result{RequeueAfter: 30 * time.Second}, nil
		}
		for _, key := range []string{"username", "password"} {
			if problem := credentialKeyProblem(s.Data, key, true); problem != "" {
				r.Recorder.Eventf(fg, corev1.EventTypeWarning, v1alpha1.EventSecretMissingKey,
					"%s secret %q %s %q", opt.role, opt.name, problem, key)
				return ctrl.Result{RequeueAfter: 30 * time.Second}, nil
			}
		}
	}
	return ctrl.Result{}, nil
}

// credentialKeyProblem describes why key in a credential Secret's data is
// unusable ("missing required key", or "has an empty value for key" when
// nonEmpty is set), or returns "" when it is usable.
func credentialKeyProblem(data map[string][]byte, key string, nonEmpty bool) string {
	v, ok := data[key]
	switch {
	case !ok:
		return "missing required key"
	case nonEmpty && len(v) == 0:
		return "has an empty value for key"
	}
	return ""
}

// replicationAccountConflict refuses an operator Secret whose
// MYSQL_REPLICATION_USER names the operator account with a different
// MYSQL_REPLICATION_PASSWORD: the two keys would set two passwords on one
// MySQL account, and replication would present the one MySQL does not
// hold. It returns "" when there is no conflict.
func replicationAccountConflict(data map[string][]byte) string {
	user, pass := string(data[replicationUserKey]), string(data[replicationPasswordKey])
	if user == "" || pass == "" || user != string(data["username"]) || pass == string(data["password"]) {
		return ""
	}
	return fmt.Sprintf("%s names the operator account %q, but %s differs from its password; omit both keys to replicate as the operator, or name a dedicated account",
		replicationUserKey, user, replicationPasswordKey)
}

// resourceName returns a deterministic name for a per-site resource.
func resourceName(fgName, siteName string) string {
	return fmt.Sprintf("mysql-%s-%s", fgName, siteName)
}

// commonLabels returns the labels applied to all resources for a failover group/site.
func commonLabels(fgName, siteName string) map[string]string {
	return map[string]string{
		labelAppName:                "mysql",
		labelInstance:               fgName,
		v1alpha1.FailoverGroupLabel: fgName,
		v1alpha1.SiteLabel:          siteName,
		labelManagedBy:              managerName,
	}
}

func siteConfigMapName(group, site string) string {
	return fmt.Sprintf("mysql-%s-%s-config", group, site)
}

// mysqlSecurityContext returns the mysql container security context:
// spec.containerSecurityContext as given. Encryption used to default
// AppArmor and seccomp to Unconfined for GHA kind runners; the private
// mysqld copy (see mysqlRuntimeBinDir) is what avoids the host AppArmor
// profile, so a cluster that still needs Unconfined sets it explicitly.
func mysqlSecurityContext(fg *v1alpha1.MysqlFailoverGroup) *corev1.SecurityContext {
	return fg.Spec.ContainerSecurityContext.DeepCopy()
}

// configInitScript copies the operator-managed my.cnf fragments into the
// conf emptyDir. When encryption is enabled it also plants a local
// component manifest in the datadir (see encryptionConfigInitSnippet).
func configInitScript(fg *v1alpha1.MysqlFailoverGroup, serverID uint32) string {
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
//
// pitr is the group's resolved PITR render: my.cnf's PITR settings follow its
// Enabled, not the spec's (Δ33), so enabling PITR rolls the pods once.
func desiredSiteConfigData(fg *v1alpha1.MysqlFailoverGroup, site v1alpha1.SiteSpec, pitr pitrRenderInputs) map[string]string {
	data := map[string]string{
		"bloodraven.cnf": generateMyCnf(fg, pitr.Enabled, site),
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
func desiredSiteConfigMapName(fg *v1alpha1.MysqlFailoverGroup, site v1alpha1.SiteSpec, pitr pitrRenderInputs) string {
	base := siteConfigMapName(fg.Name, site.Name)
	if !fg.Spec.EncryptionEnabled() {
		return base
	}
	data := desiredSiteConfigData(fg, site, pitr)
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

// reconcileSiteConfigMaps renders every site's ConfigMap from the group's
// PITR render. A site whose existing Deployment the render defers keeps
// its ConfigMap as it is: my.cnf follows the render (max_binlog_size), and
// a restarted pod of the untouched Deployment must start with the
// configuration it was rendered with (U09).
func (r *MysqlFailoverGroupReconciler) reconcileSiteConfigMaps(ctx context.Context, fg *v1alpha1.MysqlFailoverGroup, pitr groupPITRRender) error {
	for _, site := range fg.Spec.Sites {
		if keys := unsafeMySQLConfKeys(fg, site); len(keys) > 0 && r.Recorder != nil {
			r.Recorder.Eventf(fg, corev1.EventTypeWarning, v1alpha1.EventMysqlConfIgnored,
				"site %s: ignoring mysqlConf keys %s: keys must match [A-Za-z0-9_.-]+ and values must not contain line breaks",
				site.Name, strings.Join(keys, ", "))
		}
		if keys := shadowedMySQLConfKeys(fg, site); len(keys) > 0 && r.Recorder != nil {
			r.Recorder.Eventf(fg, corev1.EventTypeWarning, v1alpha1.EventMysqlConfIgnored,
				"site %s: ignoring mysqlConf keys %s: they override an operator-owned setting",
				site.Name, strings.Join(keys, ", "))
		}
		if pitr.defers(resourceName(fg.Name, site.Name)) {
			continue
		}
		if err := r.reconcileSiteConfigMap(ctx, fg, site, pitr.inputs); err != nil {
			return err
		}
	}
	return nil
}

func (r *MysqlFailoverGroupReconciler) reconcileSiteConfigMap(ctx context.Context, fg *v1alpha1.MysqlFailoverGroup, site v1alpha1.SiteSpec, pitr pitrRenderInputs) error {
	versioned := fg.Spec.EncryptionEnabled()
	cm := &corev1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{
			Name:      desiredSiteConfigMapName(fg, site, pitr),
			Namespace: fg.Namespace,
		},
	}

	_, err := controllerutil.CreateOrUpdate(ctx, r.Client, cm, func() error {
		if err := controllerutil.SetControllerReference(fg, cm, r.Scheme); err != nil {
			return err
		}
		cm.Labels = map[string]string{
			labelAppName:                "mysql",
			labelInstance:               fg.Name,
			v1alpha1.FailoverGroupLabel: fg.Name,
			v1alpha1.SiteLabel:          site.Name,
			labelManagedBy:              managerName,
		}
		cm.Data = desiredSiteConfigData(fg, site, pitr)
		if versioned {
			immutable := true
			cm.Immutable = &immutable
		}
		return nil
	})
	return err
}

// mysqlOptionKeyPattern is the shape of a my.cnf option name.
var mysqlOptionKeyPattern = regexp.MustCompile(`^[A-Za-z0-9_.-]+$`)

// unsafeMySQLSetting reports a spec.mysqlConf entry that cannot be written
// as one my.cnf line: a line break in the key or value would inject further
// directives or [section] headers, overriding operator-owned settings.
func unsafeMySQLSetting(key, value string) bool {
	return !mysqlOptionKeyPattern.MatchString(key) || strings.ContainsAny(value, "\r\n\x00")
}

// unsafeMySQLConfKeys lists the spec.mysqlConf keys, group-level then the
// site's, that generateMyCnf drops as unsafe.
func unsafeMySQLConfKeys(fg *v1alpha1.MysqlFailoverGroup, site v1alpha1.SiteSpec) []string {
	var out []string
	for _, raw := range []map[string]string{fg.Spec.MysqlConf, site.MysqlConf} {
		for k, v := range raw {
			if unsafeMySQLSetting(k, v) {
				out = append(out, strconv.Quote(k))
			}
		}
	}
	sort.Strings(out)
	return out
}

func normalizedMySQLSettings(raw map[string]string) map[string]string {
	keys := make([]string, 0, len(raw))
	for key := range raw {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	out := make(map[string]string, len(raw))
	for _, key := range keys {
		if unsafeMySQLSetting(key, raw[key]) {
			continue
		}
		out[canonicalMySQLOptionName(strings.ReplaceAll(key, "_", "-"))] = raw[key]
	}
	return out
}

// legacyMySQLOptionAliases maps the deprecated spellings MySQL 9.7 still
// accepts for operator-owned options to the current names. MySQL applies
// an alias exactly like the option itself and honours the last occurrence,
// so in the sorted my.cnf "slave-preserve-commit-order=OFF" after
// "replica-preserve-commit-order=ON" turns commit ordering off, and
// "log-slave-updates=OFF" turns off log_replica_updates (verified against
// mysql:9.7.4, F153).
var legacyMySQLOptionAliases = map[string]string{
	"log-slave-updates":           "log-replica-updates",
	"skip-slave-start":            "skip-replica-start",
	"slave-preserve-commit-order": "replica-preserve-commit-order",
}

// canonicalMySQLOptionName rewrites a hyphenated option name that is a
// legacy alias, alone or behind option prefixes (loose-, skip-, ...), to
// the current name with the same prefixes, so the owned-setting override
// and shadowing checks see it.
func canonicalMySQLOptionName(key string) string {
	prefix, k := "", key
	for {
		if current, ok := legacyMySQLOptionAliases[k]; ok {
			return prefix + current
		}
		stripped := false
		for _, p := range mysqlOptionPrefixes {
			if strings.HasPrefix(k, p) {
				prefix, k, stripped = prefix+p, strings.TrimPrefix(k, p), true
				break
			}
		}
		if !stripped {
			return key
		}
	}
}

// mysqlOptionPrefixes are the modifiers MySQL accepts in front of an option
// name: loose- ignores an unknown option, skip-/disable-/enable- set a
// boolean, maximum- caps a variable.
var mysqlOptionPrefixes = []string{"loose-", "skip-", "disable-", "enable-", "maximum-"}

// shadowsOwnedSetting reports whether key is a prefixed spelling of an
// operator-owned option (loose-gtid-mode, skip-log-bin, ...). The rendered
// my.cnf is sorted and MySQL honours the last occurrence, so such a key
// could override the owned value.
//
// loose-plugin-load-add is exempt: plugin-load-add appends to the plugin
// list rather than replacing it, so it cannot remove mysql_clone.so, and the
// loose- spelling is how a group loads extra plugins next to the owned one.
func shadowsOwnedSetting(key string, owned map[string]string) bool {
	if _, ok := owned[key]; ok {
		return false
	}
	if key == "loose-plugin-load-add" {
		return false
	}
	for k := key; ; {
		trimmed := k
		for _, p := range mysqlOptionPrefixes {
			trimmed = strings.TrimPrefix(trimmed, p)
			if trimmed != k {
				break
			}
		}
		if trimmed == k {
			return false
		}
		if _, ok := owned[trimmed]; ok {
			return true
		}
		k = trimmed
	}
}

// ownedMySQLSettings returns the my.cnf settings that group and site
// mysqlConf overrides cannot change.
func ownedMySQLSettings(fg *v1alpha1.MysqlFailoverGroup) map[string]string {
	// These settings are operator-owned safety invariants and cannot be
	// weakened by group or site overrides.
	owned := map[string]string{
		"gtid-mode": "ON", "enforce-gtid-consistency": "ON",
		"log-bin": "/var/lib/mysql/mysql-bin", "log-replica-updates": "ON",
		"skip-replica-start": "ON", "plugin-load-add": "mysql_clone.so",
		// Grant names treat `_` and `%` literally, and the app role's
		// mysql schema revoke can replicate; see appSystemSchemaRevoke.
		"partial-revokes": "ON",
		// PITR replay certifies a history from its binlog order, so a
		// multi-threaded applier must commit in source order (D-33).
		// Overriding it is refused rather than left to the sidecar's
		// history-integrity marker, which is permanent for the prefix.
		"replica-preserve-commit-order": "ON",
	}

	// Encryption coverage is likewise operator-owned: the security claim
	// depends on these settings being exactly what spec.encryptionAtRest
	// asked for, so a spec.mysqlConf override for the same keys must not
	// be able to silently downgrade a site to plaintext redo logs.
	for k, v := range encryptionMySQLSettings(fg) {
		owned[k] = v
	}
	return owned
}

// shadowedMySQLConfKeys lists the spec.mysqlConf keys, group-level then the
// site's, that generateMyCnf drops as prefixed spellings of an owned option.
func shadowedMySQLConfKeys(fg *v1alpha1.MysqlFailoverGroup, site v1alpha1.SiteSpec) []string {
	owned := ownedMySQLSettings(fg)
	var out []string
	for _, raw := range []map[string]string{fg.Spec.MysqlConf, site.MysqlConf} {
		for k := range normalizedMySQLSettings(raw) {
			if shadowsOwnedSetting(k, owned) {
				out = append(out, strconv.Quote(k))
			}
		}
	}
	sort.Strings(out)
	return slices.Compact(out)
}

// generateMyCnf renders a site's my.cnf. pitrOn is the group's resolved PITR
// render's Enabled (Δ33), which decides max_binlog_size.
func generateMyCnf(fg *v1alpha1.MysqlFailoverGroup, pitrOn bool, sites ...v1alpha1.SiteSpec) string {
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

	// clone_ddl_timeout is not written here; CloneInstance applies it at clone
	// time (internal/mysql/clone.go).

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
	if pitrOn && fg.Spec.Backup != nil && fg.Spec.Backup.PITR != nil {
		maxSize := fg.Spec.Backup.PITR.MaxBinlogSize
		if maxSize == "" {
			maxSize = defaultPITRMaxBinlogSize
		}
		settings["max-binlog-size"] = maxSize
	}

	for k, v := range normalizedMySQLSettings(fg.Spec.MysqlConf) {
		settings[k] = v
	}
	var site v1alpha1.SiteSpec
	if len(sites) > 0 {
		site = sites[0]
		for k, v := range normalizedMySQLSettings(site.MysqlConf) {
			settings[k] = v
		}
	}
	// While PITR is enabled, an option that breaks replay certification
	// is refused here rather than left to the sidecar's history-integrity
	// marker, which is permanent for the prefix (N6). MySQL's defaults
	// for the refused options (ROW, FULL, no filter) are the safe values.
	// PITRMysqlConfAccepted and a PITRMysqlConfRefused Event report it.
	for _, refused := range pitrRefusedMySQLConf(fg, site) {
		delete(settings, refused.key)
	}

	owned := ownedMySQLSettings(fg)

	// Prefixed spellings of an owned option (skip-log-bin, disable-log-bin,
	// loose-innodb-redo-log-encrypt, ...; underscore spellings are already
	// normalized to hyphens) would sort after the owned key, and MySQL
	// honors the last occurrence. Strip them so group or site overrides
	// cannot reintroduce them.
	for k := range settings {
		if shadowsOwnedSetting(k, owned) {
			delete(settings, k)
		}
	}
	for k, v := range owned {
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

func (r *MysqlFailoverGroupReconciler) reconcileDeployment(ctx context.Context, fg *v1alpha1.MysqlFailoverGroup, site v1alpha1.SiteSpec, serverID uint32, image string, pitr groupPITRRender) error {
	deploy := &appsv1.Deployment{
		ObjectMeta: metav1.ObjectMeta{
			Name:      resourceName(fg.Name, site.Name),
			Namespace: fg.Namespace,
		},
	}
	sidecarImage, err := effectiveSidecarImage(fg)
	if err != nil {
		return err
	}

	// Secret contents enter the spec hash as a keyed digest so password
	// and certificate changes roll the pods (see secret_digest.go).
	digestKey, err := ensureDigestKey(ctx, r.Client, r.APIReader, r.Scheme, fg)
	if err != nil {
		return err
	}
	secretInputs, err := specSecretInputs(ctx, r.Client, fg)
	if err != nil {
		return err
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

	specHash := ComputeSpecHash(fg, site, secretContentDigest(digestKey, secretInputs), pitr.inputs)
	pitrCanonical := pitr.inputs.Canonical()

	_, err = controllerutil.CreateOrUpdate(ctx, r.Client, deploy, func() error {
		// The selector is immutable: a live Deployment with another one
		// is refused, not updated (workload_selector.go).
		if mismatch := deploymentSelectorMismatch(deploy, fg, site.Name); mismatch != nil {
			return mismatch
		}
		// §4.6 row 4d (K3): while the group's Deployments carry no
		// decodable pitr-render annotation and no check has authenticated
		// the candidate, an existing Deployment is not updated at all; its
		// writer may hold another key in memory. Sites being created
		// render the candidate (row 4a). Returning without a change makes
		// CreateOrUpdate skip the write.
		// An object CreateOrUpdate read back has a resourceVersion; one
		// about to be created has none.
		if pitr.action == pitrRenderDefer && deploy.ResourceVersion != "" {
			return nil
		}
		if err := controllerutil.SetControllerReference(fg, deploy, r.Scheme); err != nil {
			return err
		}

		labels := commonLabels(fg.Name, site.Name)
		deploy.Labels = labels

		// Store spec hash as annotation for drift detection.
		if deploy.Annotations == nil {
			deploy.Annotations = make(map[string]string)
		}
		deploy.Annotations[v1alpha1.SpecHashAnnotation] = specHash
		// The render this Deployment carries: the group hold's source
		// (groupPITRHold) and the rendered identity backups stamp (K5).
		deploy.Annotations[v1alpha1.PITRRenderAnnotation] = pitrCanonical

		var replicas int32 = 1
		deploy.Spec.Replicas = &replicas
		deploy.Spec.Strategy = appsv1.DeploymentStrategy{
			Type: appsv1.RecreateDeploymentStrategyType,
		}
		deploy.Spec.Selector = mysqlDeploymentSelector(fg, site.Name)

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
		podLabels[v1alpha1.RoleLabel] = "replica"
		podLabels[v1alpha1.HealthyLabel] = v1alpha1.LabelValueFalse

		configMapName := desiredSiteConfigMapName(fg, site, pitr.inputs)

		// Build the list of peer sidecar addresses. The sidecar treats
		// "all peers unreachable" as one half of the self-fencing
		// quorum, so every non-self site needs to be listed here.
		peerAddresses := strings.Join(sitePeerAddresses(fg, site.Name), ",")

		operatorEndpoint := defaultOperatorEndpoint(fg)

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
			// Fail-closed boot: mysqld never accepts application writes
			// before the operator (or the sidecar, when the operator names
			// this site active) lifts the fence. Without it, a restarted
			// MySQL is writable until the sidecar's next tick: an old
			// primary next to the active one, or a reader whose datadir was
			// replaced, reads as a second writable site. read_only, not
			// super_read_only: the image's first-boot init runs its SQL as
			// root through a server started with these same flags, and
			// super_read_only fails it with ERROR 1290. A command-line flag
			// also outranks a spec.mysqlConf override.
			"--read-only=ON",
		}

		if fg.Spec.TLS != nil {
			// Keep the server-side TLS contract on the mysqld command line as
			// well as in bloodraven.cnf. The sidecar verifies the per-site
			// Service SAN from its first connection; allowing mysqld to fall
			// back to its auto-generated server-cert.pem fails every sidecar
			// query, leaving the pod unready and the sidecar unable to fence.
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
			// site/content/docs/4.configuration/10.production-hardening.md for the rationale.
			// DeepCopy so each container has an independent pointer
			// (the cached spec pointer must not be shared across
			// containers in the rendered PodSpec).
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

		operatorSecretName := fg.Spec.Credentials.OperatorSecretName

		sidecarEnv := []corev1.EnvVar{
			{Name: "MY_POD_NAME", ValueFrom: &corev1.EnvVarSource{
				FieldRef: &corev1.ObjectFieldSelector{FieldPath: "metadata.name"},
			}},
			{Name: "LISTEN_ADDR", Value: fmt.Sprintf(":%d", sidecarPort)},
			{Name: "PEER_ADDRESSES", Value: peerAddresses},
			{Name: "BLOODRAVEN_ADDRESS", Value: operatorEndpoint},
			{Name: "MY_SITE", Value: site.Name},
			{Name: "POD_NAMESPACE", ValueFrom: &corev1.EnvVarSource{
				FieldRef: &corev1.ObjectFieldSelector{FieldPath: "metadata.namespace"},
			}},
			{Name: "FAILOVER_GROUP", Value: fg.Name},
			{Name: "LEASE_TIMEOUT", Value: leaseTimeout},
			{Name: "PEER_CHECK_INTERVAL", Value: peerCheckInterval},
		}

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
					// MYSQL_PWD keeps the password off argv so it
					// doesn't show up in /proc/<pid>/cmdline where
					// anything sharing the pod PID namespace could
					// read it. `mysql --help` explicitly warns
					// against the -p"$..." argv form (AUDIT M1).
					Command: []string{
						"sh", "-c",
						`MYSQL_PWD="$(cat /etc/mysql/creds/operator/password)" mysql -u "$(cat /etc/mysql/creds/operator/username)" -e 'SET GLOBAL super_read_only=ON' 2>/dev/null || true`,
					},
				},
			},
		}
		mysqlContainer.VolumeMounts = append(mysqlContainer.VolumeMounts,
			corev1.VolumeMount{Name: "creds-operator", MountPath: "/etc/mysql/creds/operator", ReadOnly: true},
			corev1.VolumeMount{Name: "creds-replication", MountPath: "/etc/mysql/creds/replication", ReadOnly: true},
			corev1.VolumeMount{Name: "init-users", MountPath: "/docker-entrypoint-initdb.d", ReadOnly: true},
		)
		appendCredVolume := func(name string) {
			mysqlContainer.VolumeMounts = append(mysqlContainer.VolumeMounts,
				corev1.VolumeMount{Name: "creds-" + name, MountPath: "/etc/mysql/creds/" + name, ReadOnly: true})
		}
		if fg.Spec.Credentials.AppSecretName != "" {
			appendCredVolume("app")
		}
		if fg.Spec.Credentials.ReadOnlySecretName != "" {
			appendCredVolume("readonly")
		}
		if fg.Spec.Credentials.MonitorSecretName != "" {
			appendCredVolume("monitor")
		}
		if fg.Spec.Credentials.BackupSecretName != "" {
			appendCredVolume("backup")
		}

		// Files rather than MYSQL_USER/MYSQL_PASSWORD env so the
		// operator password stays out of the pod spec's env and
		// /proc/<pid>/environ. Rotation still rolls the pod through
		// the spec hash, and the sidecar re-reads the files on start.
		sidecarEnv = append([]corev1.EnvVar{
			{Name: "MYSQL_CREDS_DIR", Value: "/etc/mysql/creds/operator"},
		}, sidecarEnv...)
		sidecarVolumeMounts = append(sidecarVolumeMounts, corev1.VolumeMount{
			Name: "creds-operator", MountPath: "/etc/mysql/creds/operator", ReadOnly: true,
		})

		// PITR sidecar wiring from the group's resolved render (no-op when
		// it is disabled).
		pitrFrags, err := buildPITRSidecarFragments(fg, pitr.inputs)
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
		// this wiring every sidecar query fails with Error 3159: /health
		// returns 503, so the pod is never ready, and the self-fencing
		// monitor and the super_read_only safety net cannot reach MySQL,
		// while the group still reports only Degraded.
		sidecarEnv = append(sidecarEnv, mysqlTLSSidecarEnv(fg, site)...)
		sidecarVolumeMounts = append(sidecarVolumeMounts, pitrFrags.SidecarVolumeMounts...)
		sidecarSecurityContext := fg.Spec.ContainerSecurityContext.DeepCopy()
		// The keyring file is mode 0600 owned by mysqld's uid so it is
		// not world-readable inside the pod; the escrow agent has to run
		// as that same uid to read it. Same defaulting rule as PITR:
		// only fill in what the user left unset.
		if pitr.inputs.Enabled || fg.Spec.EncryptionEnabled() {
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
			Resources:    fg.Spec.Sidecar.Resources,
			LivenessProbe: &corev1.Probe{
				ProbeHandler: corev1.ProbeHandler{
					HTTPGet: &corev1.HTTPGetAction{
						Path: sidecarLivenessPath,
						Port: intstr.FromInt32(sidecarPort),
					},
				},
				InitialDelaySeconds: 5,
				PeriodSeconds:       10,
				TimeoutSeconds:      2,
			},
			ReadinessProbe: &corev1.Probe{
				ProbeHandler: corev1.ProbeHandler{
					HTTPGet: &corev1.HTTPGetAction{
						Path: sidecarReadinessPath,
						Port: intstr.FromInt32(sidecarPort),
					},
				},
				InitialDelaySeconds: 3,
				PeriodSeconds:       5,
				// One second more than /health's own MySQL bound, so
				// a slow MySQL reads as a 503 rather than a probe
				// timeout.
				TimeoutSeconds: int32(sidecar.HealthMySQLTimeout/time.Second) + 1,
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
		// The optional replication account comes from the operator
		// Secret's MYSQL_REPLICATION_USER / MYSQL_REPLICATION_PASSWORD
		// keys. Optional, so a Secret without them mounts an empty
		// directory and the init script falls back to the operator
		// account, as the topology manager does.
		replicationOptional := true
		volumes = append(volumes,
			credSecretVolume("creds-operator", fg.Spec.Credentials.OperatorSecretName),
			corev1.Volume{
				Name: "creds-replication",
				VolumeSource: corev1.VolumeSource{
					Secret: &corev1.SecretVolumeSource{
						SecretName: fg.Spec.Credentials.OperatorSecretName,
						Items: []corev1.KeyToPath{
							{Key: replicationUserKey, Path: "username"},
							{Key: replicationPasswordKey, Path: "password"},
						},
						Optional: &replicationOptional,
					},
				},
			},
		)
		if s := fg.Spec.Credentials.AppSecretName; s != "" {
			volumes = append(volumes, credSecretVolume("creds-app", s))
		}
		if s := fg.Spec.Credentials.ReadOnlySecretName; s != "" {
			volumes = append(volumes, credSecretVolume("creds-readonly", s))
		}
		if s := fg.Spec.Credentials.MonitorSecretName; s != "" {
			volumes = append(volumes, credSecretVolume("creds-monitor", s))
		}
		if s := fg.Spec.Credentials.BackupSecretName; s != "" {
			volumes = append(volumes, credSecretVolume("creds-backup", s))
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
		podAnnotations[v1alpha1.SpecHashAnnotation] = specHash

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
				Tolerations: groupReadOnlyTolerations(fg),
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
				// site/content/docs/4.configuration/10.production-hardening.md for the migration
				// procedure to enable Restricted PSS. DeepCopy so the
				// rendered PodSpec does not alias the cached spec
				// pointer.
				SecurityContext: fg.Spec.PodSecurityContext.DeepCopy(),
			},
		}
		if fg.Spec.TerminationGracePeriodSeconds != nil {
			deploy.Spec.Template.Spec.TerminationGracePeriodSeconds = fg.Spec.TerminationGracePeriodSeconds
		}
		deploy.Spec.Template.Spec.AutomountServiceAccountToken = mysqlPodAutomountToken(fg)
		return nil
	})
	return err
}

// mysqlPodAutomountToken keeps the namespace's default ServiceAccount
// token out of MySQL pods: neither mysqld nor the sidecar talks to the
// Kubernetes API. User-supplied extra containers may (a log shipper
// enriching with pod metadata), so pods that carry any keep the
// Kubernetes default.
func mysqlPodAutomountToken(fg *v1alpha1.MysqlFailoverGroup) *bool {
	if len(fg.Spec.ExtraContainers) > 0 || len(fg.Spec.ExtraInitContainers) > 0 {
		return nil
	}
	return boolPtr(false)
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
			labelAppName:       "mysql",
			labelInstance:      fg.Name,
			v1alpha1.SiteLabel: site.Name,
		}
		// A reader serves only while healthy. So does every site while an
		// initFromBackup restore has not succeeded: syncPodLabels marks
		// every pod unhealthy until then, so this application-facing
		// Service (often a LoadBalancer the group's DNS record points at)
		// has no endpoints while the seed is being loaded or holds a
		// failed load. The restore Job uses the internal Service instead.
		if site.IsReadOnlyReader() || restoreInFlight(fg) {
			selector[v1alpha1.HealthyLabel] = v1alpha1.LabelValueTrue
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
			labelAppName: "mysql", labelInstance: fg.Name, v1alpha1.SiteLabel: site.Name,
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
			labelAppName:                "mysql",
			labelInstance:               fg.Name,
			v1alpha1.FailoverGroupLabel: fg.Name,
			labelManagedBy:              managerName,
		}
		applyManagedServiceAnnotations(svc, serviceAnnotations(fg.Spec.ServiceTemplate))
		mutateServiceSpec(svc, serviceType(fg.Spec.ServiceTemplate), serviceExternalTrafficPolicy(fg.Spec.ServiceTemplate), map[string]string{
			labelInstance:      fg.Name,
			v1alpha1.RoleLabel: "primary",
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
			labelAppName:                "mysql",
			labelInstance:               fg.Name,
			v1alpha1.FailoverGroupLabel: fg.Name,
			labelManagedBy:              managerName,
		}
		applyManagedServiceAnnotations(svc, serviceAnnotations(fg.Spec.ServiceTemplate))
		mutateServiceSpec(svc, serviceType(fg.Spec.ServiceTemplate), serviceExternalTrafficPolicy(fg.Spec.ServiceTemplate), map[string]string{
			labelInstance:         fg.Name,
			v1alpha1.RoleLabel:    "replica",
			v1alpha1.HealthyLabel: v1alpha1.LabelValueTrue,
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
			labelAppName:                "mysql",
			labelInstance:               fg.Name,
			v1alpha1.FailoverGroupLabel: fg.Name,
			labelManagedBy:              managerName,
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
	_ = json.Unmarshal([]byte(svc.Annotations[v1alpha1.ManagedServiceAnnotationsAnnotation]), &previous)
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
	svc.Annotations[v1alpha1.ManagedServiceAnnotationsAnnotation] = string(encoded)
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

// podsNotLabelled reports whether any of pods does not carry key=value
// (a different value, or no such label yet).
func podsNotLabelled(pods []corev1.Pod, key, value string) bool {
	for i := range pods {
		if pods[i].Labels[key] != value {
			return true
		}
	}
	return false
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
		if status.State == v1alpha1.SiteStateWritable {
			writableSites++
		}
	}
	// While an initFromBackup restore has not succeeded the seed is
	// writable and published active so the restore Job can load into it,
	// but it is not the application's database yet (a load in progress,
	// or a failed load's partial data): no site is labelled primary or
	// healthy, which leaves the -primary, -replicas and per-site Services
	// without endpoints. The restore Job reaches the seed through its
	// internal Service, which selects on the site alone.
	restoreWithheld := restoreInFlight(fg)
	authorityValid := false
	if !restoreWithheld && statusComplete && fg.Status.ActiveSite != "" && writableSites == 1 {
		for _, site := range fg.Spec.Sites {
			if site.Name == fg.Status.ActiveSite && site.IsPromotable() && statusByName[site.Name].State == v1alpha1.SiteStateWritable {
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
	// label so the -primary Service sheds endpoints and stops taking new
	// connections. That alone does not disconnect established sessions
	// or clients of the per-site Service and DNS name; the restore's
	// offline_mode fence (restore_offline_fence.go) does. Per-schema
	// restores skip this: other tenants continue writing, and app-level
	// maintenance-mode handles the affected schema.
	if inPlaceRestoreFencesPrimaryService(fg) {
		for i := range sites {
			if sites[i].role == "primary" {
				sites[i].role = "fenced"
			}
		}
	}

	// While a planned-failover run owns the sites (Fencing through
	// Promoting, and RollingBack until it ends) the -primary Service must
	// not send writes to the run's source, so its primary label is
	// stripped. The source is matched by name (the run records it), not by
	// "whichever site status calls primary": once the promotion has moved
	// status.activeSite to the target, the target keeps its primary label.
	// A run that is terminal or marked fences nothing.
	if fenced := plannedFailoverFencedSite(fg); fenced != "" {
		for i := range sites {
			if sites[i].spec.Name == fenced && sites[i].role == "primary" {
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

	// confirmPending reports whether site is queued for a post-restore
	// reclone according to an uncached read, taken at most once per call.
	// An unreadable group counts as queued.
	var fresh *v1alpha1.MysqlFailoverGroup
	var freshErr error
	confirmPending := func(site string) bool {
		if r.APIReader == nil {
			return false
		}
		if fresh == nil && freshErr == nil {
			var got v1alpha1.MysqlFailoverGroup
			if freshErr = r.APIReader.Get(ctx, client.ObjectKeyFromObject(fg), &got); freshErr == nil {
				fresh = &got
			}
		}
		return freshErr != nil || restoreReclonePending(fresh, site)
	}

	for _, si := range sites {
		pods := &corev1.PodList{}
		if err := r.List(ctx, pods,
			client.InNamespace(fg.Namespace),
			client.MatchingLabels{
				labelAppName:       "mysql",
				labelInstance:      fg.Name,
				v1alpha1.SiteLabel: si.spec.Name,
			},
		); err != nil {
			return fmt.Errorf("list pods for site %s: %w", si.spec.Name, err)
		}

		healthy := v1alpha1.LabelValueFalse
		if si.spec.IsReadOnlyReader() {
			if authorityValid && si.status.State == v1alpha1.SiteStateReadOnly && si.status.SourceConvergenceState == v1alpha1.SourceConvergenceConverged &&
				si.status.Replication.Running() && si.status.SecondsBehindSource != nil &&
				canonicalSourceHost(si.status.SourceHost) == canonicalSourceHost(internalSiteServiceHost(fg.Name, fg.Status.ActiveSite, fg.Namespace)) &&
				*si.status.SecondsBehindSource <= fg.Spec.EffectiveReadOnlyMaxLagSeconds() {
				healthy = v1alpha1.LabelValueTrue
			}
		} else if si.status.State == v1alpha1.SiteStateWritable || si.status.State == v1alpha1.SiteStateReadOnly {
			healthy = v1alpha1.LabelValueTrue
		}
		if restoreWithheld {
			healthy = v1alpha1.LabelValueFalse
		}
		// A peer the in-place restore has not recloned yet still serves
		// pre-restore data: keep it out of the reader Services.
		if si.role != "primary" && restoreReclonePending(fg, si.spec.Name) {
			healthy = v1alpha1.LabelValueFalse
		}
		// The cached view may predate the queue: before a pod that is not
		// in the reader Services yet (labelled unhealthy, or a new pod not
		// labelled at all) is put in, confirm uncached that the site is not
		// queued.
		if healthy == v1alpha1.LabelValueTrue && si.role != "primary" && podsNotLabelled(pods.Items, v1alpha1.HealthyLabel, v1alpha1.LabelValueTrue) && confirmPending(si.spec.Name) {
			healthy = v1alpha1.LabelValueFalse
		}

		for i := range pods.Items {
			pod := &pods.Items[i]
			needsUpdate := false

			if pod.Labels[v1alpha1.RoleLabel] != si.role {
				if pod.Labels == nil {
					pod.Labels = make(map[string]string)
				}
				pod.Labels[v1alpha1.RoleLabel] = si.role
				needsUpdate = true
			}

			if pod.Labels[v1alpha1.HealthyLabel] != healthy {
				if pod.Labels == nil {
					pod.Labels = make(map[string]string)
				}
				pod.Labels[v1alpha1.HealthyLabel] = healthy
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
					fresh.Labels[v1alpha1.RoleLabel] = si.role
					fresh.Labels[v1alpha1.HealthyLabel] = healthy
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
// secretDigest is secretContentDigest over specSecretInputs ("" when no Secret is referenced).
// pitr is the group's resolved PITR render (resolvePITRRender).
func ComputeSpecHash(fg *v1alpha1.MysqlFailoverGroup, site v1alpha1.SiteSpec, secretDigest string, pitr pitrRenderInputs) string {
	h := sha256.New()
	fmt.Fprintf(h, "deploymentPodRenderVersion=%s\n", deploymentPodRenderVersion)
	fmt.Fprintf(h, "image=%s\n", fg.Spec.Image)
	// The effective sidecar image: a pin, or the operator's default, so a
	// new default rolls the groups that do not pin one. An unset default
	// hashes as empty; the render refuses it (effectiveSidecarImage).
	sidecarImage, _ := effectiveSidecarImage(fg)
	fmt.Fprintf(h, "sidecar=%s\n", sidecarImage)
	fmt.Fprintf(h, "resources=%v\n", site.Resources)
	fmt.Fprintf(h, "sidecarResources=%v\n", fg.Spec.Sidecar.Resources)
	// Opt-in security contexts must be part of the rolling-hash so that
	// toggling spec.{pod,container}SecurityContext actually rolls the
	// pods. The %v rendering of these corev1 structs is deterministic
	// (no maps), which matches the hash-stability contract this helper
	// already relies on for resources/sidecarResources.
	fmt.Fprintf(h, "podSC=%v\n", fg.Spec.PodSecurityContext)
	fmt.Fprintf(h, "containerSC=%v\n", fg.Spec.ContainerSecurityContext)
	fmt.Fprintf(h, "configMapRenderVersion=%s\n", configMapRenderVersion)
	fmt.Fprintf(h, "configMapName=%s\n", desiredSiteConfigMapName(fg, site, pitr))
	fmt.Fprintf(h, "effectiveMyCnf=%s\n", generateMyCnf(fg, pitr.Enabled, site))
	fmt.Fprintf(h, "peerAddresses=%s\n", strings.Join(sitePeerAddresses(fg, site.Name), ","))
	// PITR: the group's resolved render (§4.6), canonical JSON of the
	// archive identity and access fields every PITR env, volume and mount
	// is built from. The profile name, the encryption selector and the
	// backup PVC's size and storage class are not inputs (K7, Δ29): a
	// same-prefix alias with identical bytes renders identically, and the
	// snapshot name in the render is the content address of the key.
	// maxBinlogSize is the effective value (default filled in), so a
	// release that changes the default rolls the pods.
	fmt.Fprintf(h, "pitr.render=%s\n", pitr.Canonical())
	if pitr.Enabled {
		effMaxBinlogSize := defaultPITRMaxBinlogSize
		if fg.Spec.Backup != nil && fg.Spec.Backup.PITR != nil && fg.Spec.Backup.PITR.MaxBinlogSize != "" {
			effMaxBinlogSize = fg.Spec.Backup.PITR.MaxBinlogSize
		}
		fmt.Fprintf(h, "pitr.podRenderVersion=%s\n", pitrPodRenderVersion)
		fmt.Fprintf(h, "pitr.maxBinlogSize=%s\n", effMaxBinlogSize)
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

	// Secret contents (TLS material and every referenced credential
	// Secret) enter only as a keyed digest, so
	// rotating a password or certificate rolls the pods while the
	// published hash cannot be used to check password guesses.
	fmt.Fprintf(h, "secrets=%s\n", secretDigest)
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
// It fails on a site role the operator cannot interpret (topologySiteRole),
// so no topology manager runs on a role it might promote or fence wrongly.
func CRConfigToTopologyConfig(fg *v1alpha1.MysqlFailoverGroup) (TopologyConfig, error) {
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

	failoverCooldown := int64(failoverCooldownFromSpec(fg))
	connectionDrainTimeout := int64(defaultConnectionDrainTimeout)
	if fg.Spec.ConnectionDrainTimeout != nil {
		connectionDrainTimeout = int64(fg.Spec.ConnectionDrainTimeout.Duration)
	}

	var sitePriorities []string
	if fg.Spec.SplitBrainPolicy != nil && len(fg.Spec.SplitBrainPolicy.SitePriorities) > 0 {
		sitePriorities = append(sitePriorities, fg.Spec.SplitBrainPolicy.SitePriorities...)
	}

	sites := make([]SiteTopologyConfig, len(fg.Spec.Sites))
	for i, s := range fg.Spec.Sites {
		role, err := topologySiteRole(s.Role)
		if err != nil {
			return TopologyConfig{}, fmt.Errorf("site %q: %w", s.Name, err)
		}
		sites[i] = SiteTopologyConfig{
			Name:          s.Name,
			Zone:          s.Zone,
			LBIP:          s.LBIP,
			Role:          role,
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
	}, nil
}

func (r *MysqlFailoverGroupReconciler) cleanupLegacyConfigMap(ctx context.Context, fg *v1alpha1.MysqlFailoverGroup, pitr pitrRenderInputs) error {
	reader := client.Reader(r.Client)
	if r.APIReader != nil {
		reader = r.APIReader
	}
	legacy := fmt.Sprintf("mysql-%s-config", fg.Name)
	desiredConfigs := make(map[string]string, len(fg.Spec.Sites))
	for _, site := range fg.Spec.Sites {
		desiredConfig := desiredSiteConfigMapName(fg, site, pitr)
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
func (r *MysqlFailoverGroupReconciler) cleanupObsoleteSiteConfigMaps(ctx context.Context, fg *v1alpha1.MysqlFailoverGroup, pitr pitrRenderInputs) error {
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
		desired := desiredSiteConfigMapName(fg, site, pitr)
		if deploymentConfigMapName(&dep) != desired || !deploymentRolloutComplete(&dep) {
			continue
		}

		var configs corev1.ConfigMapList
		if err := r.List(ctx, &configs,
			client.InNamespace(fg.Namespace),
			client.MatchingLabels{
				v1alpha1.FailoverGroupLabel: fg.Name,
				v1alpha1.SiteLabel:          site.Name,
				labelManagedBy:              managerName,
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
	if err := r.applySiteDeployment(ctx, fgName, siteName); err != nil {
		return err
	}
	deployName := types.NamespacedName{Namespace: fgName.Namespace, Name: resourceName(fgName.Name, siteName)}
	if err := r.waitForDeploymentRollout(ctx, deployName, 5*time.Minute); err != nil {
		return &RolloutIncompleteError{Site: siteName, Err: err}
	}
	return nil
}

// applySiteDeployment renders and applies one site's ConfigMap and
// Deployment from a fresh read of the group. It holds the group's PITR
// render lock from that read until the Deployment is written, so a PITR
// binding release (which takes the same lock for its in-use check, its
// status write and its snapshot deletions) either sees this render or is
// seen by it (G1).
func (r *MysqlFailoverGroupReconciler) applySiteDeployment(ctx context.Context, fgName types.NamespacedName, siteName string) error {
	unlock := r.lockPITRRender(fgName)
	defer unlock()
	// Uncached: the render depends on status.pitrArchiveBindings, which
	// the group reconciler may have just written.
	reader := client.Reader(r.Client)
	if r.APIReader != nil {
		reader = r.APIReader
	}
	var fg v1alpha1.MysqlFailoverGroup
	if err := reader.Get(ctx, fgName, &fg); err != nil {
		return fmt.Errorf("get CR: %w", err)
	}
	image := fg.Spec.Image
	if image == "" {
		image = defaultMySQLImage
	}
	// The ordered updater renders from the fresh copy above, resolved
	// against the Deployments as they are now (uncached).
	pitrRender, err := r.resolveGroupPITRRender(ctx, &fg)
	if err != nil {
		return err
	}
	siteFound := false
	for _, site := range fg.Spec.Sites {
		if site.Name == siteName {
			siteFound = true
			if !pitrRender.defers(resourceName(fg.Name, site.Name)) {
				if err := r.reconcileSiteConfigMap(ctx, &fg, site, pitrRender.inputs); err != nil {
					return fmt.Errorf("reconcile site configmap: %w", err)
				}
			}
			serverID, err := site.MySQLServerID()
			if err != nil {
				return err
			}
			if err := r.reconcileDeployment(ctx, &fg, site, serverID, image, pitrRender); err != nil {
				return err
			}
			break
		}
	}
	if !siteFound {
		return fmt.Errorf("site %q not found in CR %s", siteName, fgName)
	}
	return nil
}

// pitrRenderLock is one group's PITR render lock (pitrRenderLocks).
type pitrRenderLock struct {
	mu   sync.Mutex
	refs int
}

// lockPITRRender takes fg's PITR render lock and returns its release.
// Never nest it: the holders (applySiteDeployment and a binding release)
// call nothing that takes it again.
func (r *MysqlFailoverGroupReconciler) lockPITRRender(key types.NamespacedName) func() {
	r.pitrRenderLocksMu.Lock()
	if r.pitrRenderLocks == nil {
		r.pitrRenderLocks = map[types.NamespacedName]*pitrRenderLock{}
	}
	l := r.pitrRenderLocks[key]
	if l == nil {
		l = &pitrRenderLock{}
		r.pitrRenderLocks[key] = l
	}
	l.refs++
	r.pitrRenderLocksMu.Unlock()
	l.mu.Lock()
	return func() {
		l.mu.Unlock()
		r.pitrRenderLocksMu.Lock()
		l.refs--
		if l.refs == 0 {
			delete(r.pitrRenderLocks, key)
		}
		r.pitrRenderLocksMu.Unlock()
	}
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
