package controller

import (
	"context"
	"fmt"
	"log/slog"
	"strconv"
	"strings"
	"time"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	policyv1 "k8s.io/api/policy/v1"
	"k8s.io/apimachinery/pkg/api/equality"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/intstr"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
	"sigs.k8s.io/controller-runtime/pkg/log"

	v1alpha1 "github.com/shipstream/bloodraven/api/v1alpha1"
	"github.com/shipstream/bloodraven/internal/dragonfly"
	"github.com/shipstream/bloodraven/internal/platform"
)

// Dragonfly-specific labels and constants.
const (
	dragonflyAppName = "dragonfly"

	// labelDragonflyRole identifies the operator-assigned Dragonfly role
	// on a pod. Values: "master", "replica". The active Service AND-gates
	// this with labelDragonflyTraffic, so the role label alone is not
	// enough to attract client traffic.
	labelDragonflyRole = "shipstream.io/dragonfly-role"

	// labelDragonflyTraffic gates whether a pod is eligible for the
	// active app-facing Service. Default value "enabled" is set by the
	// StatefulSet template; the planned-failover sequence transiently
	// removes it from the source pod before issuing REPLTAKEOVER so the
	// Service atomically sheds the old endpoint without depending on
	// label-flip ordering. Restored once the new master is in place.
	//
	// Only "enabled" is recognized by the active-Service selector;
	// "disabled" or absent both shed traffic.
	labelDragonflyTraffic = "shipstream.io/dragonfly-traffic"

	// dragonflyTrafficEnabled is the only meaningful value of
	// labelDragonflyTraffic. Centralized so we don't repeat the literal.
	dragonflyTrafficEnabled = "enabled"

	dragonflyDefaultPort      = int32(6379)
	dragonflyDefaultAdminPort = int32(9999)

	dragonflyContainerName = "dragonfly"

	// DragonflyAuthEnvVar is the env var name into which the operator
	// projects the Dragonfly auth password. Used to render --requirepass
	// from a Secret without exposing the value in the pod spec.
	DragonflyAuthEnvVar = "DRAGONFLY_PASSWORD"
)

// dragonflyStatefulSetName returns the deterministic StatefulSet name
// for a per-site Dragonfly instance.
func dragonflyStatefulSetName(fgName, siteName string) string {
	return fmt.Sprintf("%s-dragonfly-%s", fgName, siteName)
}

// dragonflySiteServiceName returns the deterministic site-local Service
// name for a per-site Dragonfly instance. Used for replication wiring,
// debugging, and per-site readiness probes.
func dragonflySiteServiceName(fgName, siteName string) string {
	return fmt.Sprintf("%s-dragonfly-%s", fgName, siteName)
}

// dragonflyActiveServiceName returns the deterministic active Service
// name. App pods point at this name; the Service selector follows
// whichever pod is currently labeled dragonfly-role=master.
func dragonflyActiveServiceName(fgName string) string {
	return fmt.Sprintf("%s-dragonfly", fgName)
}

// dragonflyCommonLabels returns the labels shared by all per-site
// Dragonfly resources. Mirrors commonLabels() for MySQL but with a
// distinct app.kubernetes.io/name so client tooling can list them.
func dragonflyCommonLabels(fgName, siteName string) map[string]string {
	return map[string]string{
		labelAppName:       dragonflyAppName,
		labelInstance:      fgName,
		labelFailoverGroup: fgName,
		labelSite:          siteName,
		labelManagedBy:     managerName,
	}
}

// dragonflyPort returns the configured Dragonfly client port, defaulting
// to 6379. Spec defaults usually populate this via kubebuilder, but the
// helper guards against zero-value fixtures.
func dragonflyPort(spec *v1alpha1.DragonflySpec) int32 {
	if spec != nil && spec.Port > 0 {
		return spec.Port
	}
	return dragonflyDefaultPort
}

// dragonflyAdminPort returns the configured admin port, defaulting to 9999.
func dragonflyAdminPort(spec *v1alpha1.DragonflySpec) int32 {
	if spec != nil && spec.AdminPort > 0 {
		return spec.AdminPort
	}
	return dragonflyDefaultAdminPort
}

// dragonflyEnabled reports whether a failover group has Dragonfly
// configured and enabled. Centralized so callers do not duplicate the
// nil-and-bool dance.
func dragonflyEnabled(fg *v1alpha1.MysqlFailoverGroup) bool {
	return fg != nil && fg.Spec.Dragonfly != nil && fg.Spec.Dragonfly.Enabled
}

// reconcileDragonflyStatefulSet creates or updates the per-site
// Dragonfly StatefulSet. Single replica, ephemeral storage (no PVC),
// placed on the same site nodes as MySQL.
func (r *MysqlFailoverGroupReconciler) reconcileDragonflyStatefulSet(ctx context.Context, fg *v1alpha1.MysqlFailoverGroup, site v1alpha1.SiteSpec) error {
	spec := fg.Spec.Dragonfly
	if spec == nil {
		return fmt.Errorf("reconcileDragonflyStatefulSet called with nil spec.dragonfly")
	}
	sts := &appsv1.StatefulSet{
		ObjectMeta: metav1.ObjectMeta{
			Name:      dragonflyStatefulSetName(fg.Name, site.Name),
			Namespace: fg.Namespace,
		},
	}

	_, err := controllerutil.CreateOrUpdate(ctx, r.Client, sts, func() error {
		return r.applyDragonflyStatefulSetSpec(fg, site, sts)
	})
	return err
}

func (r *MysqlFailoverGroupReconciler) applyDragonflyStatefulSetSpec(fg *v1alpha1.MysqlFailoverGroup, site v1alpha1.SiteSpec, sts *appsv1.StatefulSet) error {
	spec := fg.Spec.Dragonfly
	if err := controllerutil.SetControllerReference(fg, sts, r.Scheme); err != nil {
		return err
	}

	labels := dragonflyCommonLabels(fg.Name, site.Name)
	sts.Labels = labels

	var replicas int32 = 1
	port := dragonflyPort(spec)
	adminPort := dragonflyAdminPort(spec)

	args := buildDragonflyArgs(spec, port, adminPort)
	env := buildDragonflyEnv(spec)

	container := corev1.Container{
		Name:  dragonflyContainerName,
		Image: spec.Image,
		Args:  args,
		Env:   env,
		Ports: []corev1.ContainerPort{
			{Name: "client", ContainerPort: port, Protocol: corev1.ProtocolTCP},
			{Name: "admin", ContainerPort: adminPort, Protocol: corev1.ProtocolTCP},
		},
		VolumeMounts: []corev1.VolumeMount{
			{Name: "data", MountPath: "/data"},
		},
		Resources: spec.Resources,
		LivenessProbe: &corev1.Probe{
			ProbeHandler: corev1.ProbeHandler{
				TCPSocket: &corev1.TCPSocketAction{Port: intstr.FromInt32(port)},
			},
			InitialDelaySeconds: 5,
			PeriodSeconds:       10,
		},
		ReadinessProbe: &corev1.Probe{
			ProbeHandler: corev1.ProbeHandler{
				TCPSocket: &corev1.TCPSocketAction{Port: intstr.FromInt32(port)},
			},
			InitialDelaySeconds: 2,
			PeriodSeconds:       5,
		},
	}

	// Selector is immutable on a StatefulSet. Only set it on create.
	if sts.Spec.Selector == nil {
		sts.Spec.Selector = &metav1.LabelSelector{
			MatchLabels: map[string]string{
				labelAppName:  dragonflyAppName,
				labelInstance: fg.Name,
				labelSite:     site.Name,
			},
		}
	}

	// Pod labels include the operator-assigned dragonfly-role (default
	// "replica") and dragonfly-traffic=enabled. The DragonflyManager /
	// planned-failover handlers rewrite these on the active site during
	// promotion. Static labels here must not depend on mutable status,
	// otherwise every status change would trigger a rollout.
	podLabels := make(map[string]string, len(labels)+2)
	for k, v := range labels {
		podLabels[k] = v
	}
	podLabels[labelDragonflyRole] = "replica"
	podLabels[labelDragonflyTraffic] = dragonflyTrafficEnabled

	sts.Spec.Replicas = &replicas
	sts.Spec.ServiceName = dragonflySiteServiceName(fg.Name, site.Name)
	sts.Spec.Template = corev1.PodTemplateSpec{
		ObjectMeta: metav1.ObjectMeta{
			Labels: podLabels,
		},
		Spec: corev1.PodSpec{
			ServiceAccountName: dragonflyServiceAccountName(spec),
			NodeSelector: map[string]string{
				"topology.kubernetes.io/zone": site.Zone,
			},
			Tolerations: []corev1.Toleration{
				{
					Key:      platform.TaintKeyForGroup(fg.Name),
					Operator: corev1.TolerationOpExists,
				},
			},
			Containers: []corev1.Container{container},
			Volumes: []corev1.Volume{
				{
					Name: "data",
					VolumeSource: corev1.VolumeSource{
						EmptyDir: &corev1.EmptyDirVolumeSource{},
					},
				},
			},
		},
	}
	return nil
}

// buildDragonflyArgs assembles the Dragonfly container command-line. The
// operator owns --port, --admin_port, --maxmemory, --proactor_threads,
// --break_replication_on_master_restart, and --requirepass (when auth
// is configured); user-supplied args are appended last but with
// safety-critical flags filtered out (see dragonflyOperatorOwnedFlags).
func buildDragonflyArgs(spec *v1alpha1.DragonflySpec, port, adminPort int32) []string {
	args := []string{
		"--port=" + strconv.Itoa(int(port)),
		"--admin_port=" + strconv.Itoa(int(adminPort)),
		// Bind on all interfaces; cluster networking handles isolation.
		"--bind=0.0.0.0",
		// Without this flag, a restarted master pod silently re-attaches
		// its old replicas — and because Dragonfly does not version-check
		// the replication stream, the replicas accept divergent data
		// from the new master with no warning. This is the canonical
		// "split brain after master crash-restart" failure described in
		// upstream PR dragonflydb/dragonfly#386. We rely on Kubernetes +
		// the planned/emergency failover state machines (not Dragonfly's
		// own re-attach behavior) to drive recovery, so the right
		// posture is to break replication on master restart and let the
		// DragonflyManager re-issue REPLICAOF on the next tick.
		"--break_replication_on_master_restart",
	}
	if spec.MaxMemoryMb > 0 {
		args = append(args, fmt.Sprintf("--maxmemory=%dmb", spec.MaxMemoryMb))
	}
	if spec.ProactorThreads > 0 {
		args = append(args, fmt.Sprintf("--proactor_threads=%d", spec.ProactorThreads))
	}
	if spec.Snapshot != nil && spec.Snapshot.Dir != "" {
		args = append(args, "--dir="+spec.Snapshot.Dir)
		if spec.Snapshot.S3Endpoint != "" {
			args = append(args, "--s3_endpoint="+spec.Snapshot.S3Endpoint)
		}
		if spec.Snapshot.S3UseHTTPS != nil {
			args = append(args, "--s3_use_https="+strconv.FormatBool(*spec.Snapshot.S3UseHTTPS))
		}
		if spec.Snapshot.S3SignPayload != nil {
			args = append(args, "--s3_sign_payload="+strconv.FormatBool(*spec.Snapshot.S3SignPayload))
		}
	}
	// Auth flag emitted only when a usable Secret reference exists; an
	// empty SecretName would cause the pod to fail to start with a
	// secret-not-found event rather than a clean reconcile error.
	if spec.Auth != nil && spec.Auth.SecretName != "" {
		// $(VAR) syntax is expanded by the kubelet from the env block.
		args = append(args, "--requirepass=$("+DragonflyAuthEnvVar+")")
	}
	args = append(args, filterDragonflyUserArgs(spec.Args)...)
	return args
}

func dragonflyServiceAccountName(spec *v1alpha1.DragonflySpec) string {
	if spec == nil || spec.Snapshot == nil {
		return ""
	}
	return spec.Snapshot.ServiceAccountName
}

// dragonflyOperatorOwnedFlags lists Dragonfly flags whose values are a
// safety contract owned by Bloodraven. User-supplied spec.Args matching
// any of these prefixes are stripped before being appended to the
// operator's args. Dragonfly uses gflags (last-wins parsing), so a user
// who set `spec.Args = ["--break_replication_on_master_restart=false"]`
// would otherwise override the safety guard the operator just emitted.
var dragonflyOperatorOwnedFlags = []string{
	"--break_replication_on_master_restart",
	"--bind",
	"--port",
	"--admin_port",
	"--dir",
	"--s3_endpoint",
	"--s3_use_https",
	"--s3_sign_payload",
	"--requirepass",
}

// filterDragonflyUserArgs drops args that target operator-owned flags.
// Match is by exact equality on the flag verb or by "<flag>=" prefix
// (gflags accepts both `--flag value` and `--flag=value` forms).
func filterDragonflyUserArgs(in []string) []string {
	if len(in) == 0 {
		return nil
	}
	out := make([]string, 0, len(in))
	skipNext := false
	for _, arg := range in {
		if skipNext {
			skipNext = false
			continue
		}
		drop := false
		for _, owned := range dragonflyOperatorOwnedFlags {
			if arg == owned {
				// `--flag value` form: drop this arg AND the next one.
				drop = true
				skipNext = true
				break
			}
			if strings.HasPrefix(arg, owned+"=") {
				// `--flag=value` form: drop only this arg.
				drop = true
				break
			}
		}
		if !drop {
			out = append(out, arg)
		}
	}
	return out
}

// buildDragonflyEnv wires the auth password into the container's env from
// the configured Secret, leaving env empty when auth is not set or when
// the SecretName is empty (CRD validation typically catches the latter
// but partial in-place patches can still produce it; emitting an
// EnvVar with an empty SecretName makes the pod fail to start with a
// secret-not-found event rather than a clean reconcile error).
func buildDragonflyEnv(spec *v1alpha1.DragonflySpec) []corev1.EnvVar {
	if spec.Auth == nil || spec.Auth.SecretName == "" {
		return buildDragonflySnapshotEnv(spec)
	}
	key := spec.Auth.PasswordKey
	if key == "" {
		key = "password"
	}
	env := []corev1.EnvVar{
		{
			Name: DragonflyAuthEnvVar,
			ValueFrom: &corev1.EnvVarSource{
				SecretKeyRef: &corev1.SecretKeySelector{
					LocalObjectReference: corev1.LocalObjectReference{Name: spec.Auth.SecretName},
					Key:                  key,
				},
			},
		},
	}
	return append(env, buildDragonflySnapshotEnv(spec)...)
}

func buildDragonflySnapshotEnv(spec *v1alpha1.DragonflySpec) []corev1.EnvVar {
	if spec == nil || spec.Snapshot == nil || spec.Snapshot.CredentialsSecretName == "" {
		return nil
	}
	secret := spec.Snapshot.CredentialsSecretName
	keys := []string{"AWS_ACCESS_KEY_ID", "AWS_SECRET_ACCESS_KEY", "AWS_SESSION_TOKEN", "AWS_REGION"}
	env := make([]corev1.EnvVar, 0, len(keys))
	for _, key := range keys {
		env = append(env, corev1.EnvVar{
			Name: key,
			ValueFrom: &corev1.EnvVarSource{
				SecretKeyRef: &corev1.SecretKeySelector{
					LocalObjectReference: corev1.LocalObjectReference{Name: secret},
					Key:                  key,
					Optional:             dragonflyBoolPtr(true),
				},
			},
		})
	}
	return env
}

func dragonflyBoolPtr(v bool) *bool { return &v }

// reconcileDragonflySiteService creates or updates the per-site headless
// Service used for replication wiring and debugging.
func (r *MysqlFailoverGroupReconciler) reconcileDragonflySiteService(ctx context.Context, fg *v1alpha1.MysqlFailoverGroup, site v1alpha1.SiteSpec) error {
	spec := fg.Spec.Dragonfly
	if spec == nil {
		return fmt.Errorf("reconcileDragonflySiteService called with nil spec.dragonfly")
	}
	port := dragonflyPort(spec)
	adminPort := dragonflyAdminPort(spec)

	svc := &corev1.Service{
		ObjectMeta: metav1.ObjectMeta{
			Name:      dragonflySiteServiceName(fg.Name, site.Name),
			Namespace: fg.Namespace,
		},
	}
	_, err := controllerutil.CreateOrUpdate(ctx, r.Client, svc, func() error {
		if err := controllerutil.SetControllerReference(fg, svc, r.Scheme); err != nil {
			return err
		}
		svc.Labels = dragonflyCommonLabels(fg.Name, site.Name)
		applyServiceAnnotations(svc, fg.Spec.ServiceTemplate)
		svc.Spec = corev1.ServiceSpec{
			// ClusterIP keeps the service stable across pod restarts.
			// Do not use ClusterIPNone here — the operator dials by
			// service name from outside the StatefulSet.
			Type: corev1.ServiceTypeClusterIP,
			Selector: map[string]string{
				labelAppName:  dragonflyAppName,
				labelInstance: fg.Name,
				labelSite:     site.Name,
			},
			Ports: []corev1.ServicePort{
				{Name: "client", Port: port, TargetPort: intstr.FromInt32(port), Protocol: corev1.ProtocolTCP},
				{Name: "admin", Port: adminPort, TargetPort: intstr.FromInt32(adminPort), Protocol: corev1.ProtocolTCP},
			},
		}
		return nil
	})
	return err
}

// reconcileDragonflyActiveService creates or updates the singleton
// app-facing Service. Selector includes dragonfly-role=master so the
// endpoint set follows whichever pod the operator has labeled master.
func (r *MysqlFailoverGroupReconciler) reconcileDragonflyActiveService(ctx context.Context, fg *v1alpha1.MysqlFailoverGroup) error {
	spec := fg.Spec.Dragonfly
	if spec == nil {
		return fmt.Errorf("reconcileDragonflyActiveService called with nil spec.dragonfly")
	}
	port := dragonflyPort(spec)

	svc := &corev1.Service{
		ObjectMeta: metav1.ObjectMeta{
			Name:      dragonflyActiveServiceName(fg.Name),
			Namespace: fg.Namespace,
		},
	}
	_, err := controllerutil.CreateOrUpdate(ctx, r.Client, svc, func() error {
		if err := controllerutil.SetControllerReference(fg, svc, r.Scheme); err != nil {
			return err
		}
		svc.Labels = map[string]string{
			labelAppName:       dragonflyAppName,
			labelInstance:      fg.Name,
			labelFailoverGroup: fg.Name,
			labelManagedBy:     managerName,
		}
		applyServiceAnnotations(svc, fg.Spec.ServiceTemplate)
		svc.Spec = corev1.ServiceSpec{
			Type: corev1.ServiceTypeClusterIP,
			// AND-gate of role+traffic. The traffic label is the
			// canonical "this pod serves writes" gate: removing it
			// atomically sheds the endpoint, which is how the
			// planned-failover sequence avoids a window where both the
			// old and new master would match the selector during a
			// REPLTAKEOVER.
			Selector: map[string]string{
				labelAppName:          dragonflyAppName,
				labelInstance:         fg.Name,
				labelDragonflyRole:    "master",
				labelDragonflyTraffic: dragonflyTrafficEnabled,
			},
			Ports: []corev1.ServicePort{
				{Name: "client", Port: port, TargetPort: intstr.FromInt32(port), Protocol: corev1.ProtocolTCP},
			},
		}
		return nil
	})
	return err
}

func (r *MysqlFailoverGroupReconciler) reconcileDragonflyPDB(ctx context.Context, fg *v1alpha1.MysqlFailoverGroup, site v1alpha1.SiteSpec) error {
	pdb := &policyv1.PodDisruptionBudget{
		ObjectMeta: metav1.ObjectMeta{
			Name:      dragonflyStatefulSetName(fg.Name, site.Name),
			Namespace: fg.Namespace,
		},
	}
	_, err := controllerutil.CreateOrUpdate(ctx, r.Client, pdb, func() error {
		if err := controllerutil.SetControllerReference(fg, pdb, r.Scheme); err != nil {
			return err
		}
		pdb.Labels = map[string]string{
			labelAppName:       dragonflyAppName,
			labelInstance:      fg.Name,
			labelFailoverGroup: fg.Name,
			labelSite:          site.Name,
			labelManagedBy:     managerName,
		}
		pdb.Spec = policyv1.PodDisruptionBudgetSpec{
			MinAvailable: &intstr.IntOrString{Type: intstr.Int, IntVal: 1},
			Selector: &metav1.LabelSelector{
				MatchLabels: map[string]string{
					labelAppName:  dragonflyAppName,
					labelInstance: fg.Name,
					labelSite:     site.Name,
				},
			},
		}
		return nil
	})
	return err
}

// reconcileDragonflyResources is the entry point called from Reconcile().
// It creates or updates per-site StatefulSets/Services and the active
// Service when spec.dragonfly is enabled.
//
// When disabled, it actively tears down any Dragonfly resources that
// were previously created. K8s owner-reference GC only fires when the
// owning object is deleted, not when its .spec mutates — flipping
// spec.dragonfly.enabled=false would otherwise leave orphan
// StatefulSets, per-site Services, and the active Service routing live
// traffic to a Dragonfly the operator no longer manages.
func (r *MysqlFailoverGroupReconciler) reconcileDragonflyResources(ctx context.Context, fg *v1alpha1.MysqlFailoverGroup) (time.Duration, error) {
	if !dragonflyEnabled(fg) {
		return 0, r.teardownDragonflyResources(ctx, fg)
	}
	for _, site := range fg.Spec.Sites {
		if err := r.reconcileDragonflySiteService(ctx, fg, site); err != nil {
			return 0, fmt.Errorf("reconcile dragonfly site service %s: %w", site.Name, err)
		}
	}
	requeue, err := r.reconcileDragonflyStatefulSetsSerial(ctx, fg)
	if err != nil {
		return 0, err
	}
	if err := r.reconcileDragonflyActiveService(ctx, fg); err != nil {
		return 0, fmt.Errorf("reconcile dragonfly active service: %w", err)
	}
	if err := r.deleteLegacyDragonflyGroupPDB(ctx, fg); err != nil {
		return 0, fmt.Errorf("delete legacy dragonfly pdb: %w", err)
	}
	for _, site := range fg.Spec.Sites {
		if err := r.reconcileDragonflyPDB(ctx, fg, site); err != nil {
			return 0, fmt.Errorf("reconcile dragonfly pdb %s: %w", site.Name, err)
		}
	}
	return requeue, nil
}

func (r *MysqlFailoverGroupReconciler) deleteLegacyDragonflyGroupPDB(ctx context.Context, fg *v1alpha1.MysqlFailoverGroup) error {
	pdb := &policyv1.PodDisruptionBudget{
		ObjectMeta: metav1.ObjectMeta{
			Name:      dragonflyActiveServiceName(fg.Name),
			Namespace: fg.Namespace,
		},
	}
	return client.IgnoreNotFound(r.Delete(ctx, pdb))
}

func (r *MysqlFailoverGroupReconciler) reconcileDragonflyStatefulSetsSerial(ctx context.Context, fg *v1alpha1.MysqlFailoverGroup) (time.Duration, error) {
	const requeueAfter = 2 * time.Second

	activeSite := effectiveDragonflyMasterSite(fg)
	var activeDrift *v1alpha1.SiteSpec

	for i := range fg.Spec.Sites {
		site := fg.Spec.Sites[i]
		var current appsv1.StatefulSet
		key := types.NamespacedName{Namespace: fg.Namespace, Name: dragonflyStatefulSetName(fg.Name, site.Name)}
		if err := r.Get(ctx, key, &current); err != nil {
			if apierrors.IsNotFound(err) {
				if err := r.reconcileDragonflyStatefulSet(ctx, fg, site); err != nil {
					return 0, fmt.Errorf("reconcile dragonfly statefulset %s: %w", site.Name, err)
				}
				continue
			}
			return 0, fmt.Errorf("get dragonfly statefulset %s: %w", site.Name, err)
		}

		desired, err := r.desiredDragonflyStatefulSet(fg, site)
		if err != nil {
			return 0, fmt.Errorf("build desired dragonfly statefulset %s: %w", site.Name, err)
		}
		if !dragonflyStatefulSetTemplateEqual(&current, desired) {
			if site.Name == activeSite {
				activeDrift = &fg.Spec.Sites[i]
				continue
			}
			if err := r.reconcileDragonflyStatefulSet(ctx, fg, site); err != nil {
				return 0, fmt.Errorf("reconcile dragonfly statefulset %s: %w", site.Name, err)
			}
			return requeueAfter, nil
		}
	}

	if activeDrift != nil {
		if plannedFailoverInFlight(fg.Status.PlannedFailover) {
			return requeueAfter, nil
		}
		promotionTarget := ""
		for _, site := range fg.Spec.Sites {
			if site.Name == activeSite {
				continue
			}
			var current appsv1.StatefulSet
			key := types.NamespacedName{Namespace: fg.Namespace, Name: dragonflyStatefulSetName(fg.Name, site.Name)}
			if err := r.Get(ctx, key, &current); err != nil {
				return 0, fmt.Errorf("get dragonfly statefulset %s before active rollout: %w", site.Name, err)
			}
			if !dragonflyStatefulSetRolloutComplete(&current) {
				return requeueAfter, nil
			}
			if promotionTarget == "" {
				promotionTarget = site.Name
			}
		}
		if promotionTarget != "" {
			ready, err := r.dragonflyRolloutCandidateSyncReady(ctx, fg, promotionTarget, activeSite)
			if err != nil {
				log.FromContext(ctx).Info("dragonfly rollout: sync-readiness check failed", "target", promotionTarget, "source", activeSite, "error", err)
				return requeueAfter, nil
			}
			if !ready {
				return requeueAfter, nil
			}
			if wait := r.dragonflyRolloutPromotionBackoff(fg, promotionTarget, activeSite); wait > 0 {
				return wait, nil
			}
			if r.promoteDragonflyForRollout(ctx, fg, promotionTarget, activeSite) {
				r.clearDragonflyRolloutPromotionBackoff(fg, promotionTarget, activeSite)
			} else if r.recordDragonflyRolloutPromotionFailure(fg, promotionTarget, activeSite) >= 3 {
				log.FromContext(ctx).Info("dragonfly rollout: promotion failed repeatedly; applying active StatefulSet update", "target", promotionTarget, "source", activeSite)
				if err := r.reconcileDragonflyStatefulSet(ctx, fg, *activeDrift); err != nil {
					return 0, fmt.Errorf("reconcile active dragonfly statefulset %s after promotion failures: %w", activeDrift.Name, err)
				}
				r.clearDragonflyRolloutPromotionBackoff(fg, promotionTarget, activeSite)
			}
			return requeueAfter, nil
		}
		if err := r.reconcileDragonflyStatefulSet(ctx, fg, *activeDrift); err != nil {
			return 0, fmt.Errorf("reconcile active dragonfly statefulset %s: %w", activeDrift.Name, err)
		}
		return requeueAfter, nil
	}

	return 0, nil
}

func (r *MysqlFailoverGroupReconciler) promoteDragonflyForRollout(ctx context.Context, fg *v1alpha1.MysqlFailoverGroup, target, oldSource string) bool {
	nn := types.NamespacedName{Namespace: fg.Namespace, Name: fg.Name}
	if r.Runner != nil {
		if mgr := r.Runner.dragonflyManager(nn); mgr != nil {
			return mgr.TryEmergencyPromote(ctx, target, oldSource)
		}
	}
	mgr := NewDragonflyManager(r.Client, r.Recorder, slog.Default().With("fg", nn.String(), "subsystem", "dragonfly-rollout"), nn, 0)
	if r.dragonflyConnector != nil {
		mgr.SetConnector(r.dragonflyConnector)
	}
	return mgr.TryEmergencyPromote(ctx, target, oldSource)
}

func (r *MysqlFailoverGroupReconciler) dragonflyRolloutCandidateSyncReady(ctx context.Context, fg *v1alpha1.MysqlFailoverGroup, target, source string) (bool, error) {
	sourceConn, err := r.dragonflyDial(ctx, fg, source)
	if err != nil {
		return false, fmt.Errorf("dial source: %w", err)
	}
	sourceInfo, err := sourceConn.InfoReplication(ctx)
	_ = sourceConn.Close()
	if err != nil {
		return false, fmt.Errorf("source INFO replication: %w", err)
	}

	targetConn, err := r.dragonflyDial(ctx, fg, target)
	if err != nil {
		return false, fmt.Errorf("dial target: %w", err)
	}
	targetInfo, err := targetConn.InfoReplication(ctx)
	if err != nil {
		_ = targetConn.Close()
		return false, fmt.Errorf("target INFO replication: %w", err)
	}
	targetPersist, _ := targetConn.InfoPersistence(ctx)
	_ = targetConn.Close()

	return dragonfly.CandidateSyncReady(targetInfo, targetPersist, sourceInfo.MasterReplOffset), nil
}

func (r *MysqlFailoverGroupReconciler) dragonflyRolloutPromotionBackoff(fg *v1alpha1.MysqlFailoverGroup, target, source string) time.Duration {
	key := dragonflyRolloutKey{fg: types.NamespacedName{Namespace: fg.Namespace, Name: fg.Name}, target: target, source: source}
	r.dragonflyRolloutMu.Lock()
	defer r.dragonflyRolloutMu.Unlock()
	st, ok := r.dragonflyRolloutBackoff[key]
	if !ok || st.attempts <= 0 {
		return 0
	}
	wait := time.Duration(1<<minInt(st.attempts-1, 5)) * time.Second
	if remaining := wait - time.Since(st.lastFailure); remaining > 0 {
		return remaining
	}
	return 0
}

func (r *MysqlFailoverGroupReconciler) recordDragonflyRolloutPromotionFailure(fg *v1alpha1.MysqlFailoverGroup, target, source string) int {
	key := dragonflyRolloutKey{fg: types.NamespacedName{Namespace: fg.Namespace, Name: fg.Name}, target: target, source: source}
	r.dragonflyRolloutMu.Lock()
	defer r.dragonflyRolloutMu.Unlock()
	if r.dragonflyRolloutBackoff == nil {
		r.dragonflyRolloutBackoff = make(map[dragonflyRolloutKey]dragonflyRolloutState)
	}
	st := r.dragonflyRolloutBackoff[key]
	st.attempts++
	st.lastFailure = time.Now()
	r.dragonflyRolloutBackoff[key] = st
	return st.attempts
}

func (r *MysqlFailoverGroupReconciler) clearDragonflyRolloutPromotionBackoff(fg *v1alpha1.MysqlFailoverGroup, target, source string) {
	key := dragonflyRolloutKey{fg: types.NamespacedName{Namespace: fg.Namespace, Name: fg.Name}, target: target, source: source}
	r.dragonflyRolloutMu.Lock()
	defer r.dragonflyRolloutMu.Unlock()
	delete(r.dragonflyRolloutBackoff, key)
}

func minInt(a, b int) int {
	if a < b {
		return a
	}
	return b
}

func (r *MysqlFailoverGroupReconciler) desiredDragonflyStatefulSet(fg *v1alpha1.MysqlFailoverGroup, site v1alpha1.SiteSpec) (*appsv1.StatefulSet, error) {
	sts := &appsv1.StatefulSet{
		ObjectMeta: metav1.ObjectMeta{
			Name:      dragonflyStatefulSetName(fg.Name, site.Name),
			Namespace: fg.Namespace,
		},
	}
	if err := r.applyDragonflyStatefulSetSpec(fg, site, sts); err != nil {
		return nil, err
	}
	return sts, nil
}

func dragonflyStatefulSetTemplateEqual(current, desired *appsv1.StatefulSet) bool {
	if !equality.Semantic.DeepEqual(current.Spec.Template.Labels, desired.Spec.Template.Labels) {
		return false
	}
	curSpec := current.Spec.Template.Spec
	wantSpec := desired.Spec.Template.Spec
	if curSpec.ServiceAccountName != wantSpec.ServiceAccountName {
		return false
	}
	if !equality.Semantic.DeepEqual(curSpec.NodeSelector, wantSpec.NodeSelector) {
		return false
	}
	if !equality.Semantic.DeepEqual(curSpec.Tolerations, wantSpec.Tolerations) {
		return false
	}
	if !equality.Semantic.DeepEqual(curSpec.Volumes, wantSpec.Volumes) {
		return false
	}
	cur, ok := dragonflyContainerFromTemplate(curSpec)
	if !ok {
		return false
	}
	want, ok := dragonflyContainerFromTemplate(wantSpec)
	if !ok {
		return false
	}
	return dragonflyContainersOwnedFieldsEqual(cur, want)
}

func dragonflyContainerFromTemplate(spec corev1.PodSpec) (corev1.Container, bool) {
	for _, c := range spec.Containers {
		if c.Name == dragonflyContainerName {
			return c, true
		}
	}
	return corev1.Container{}, false
}

func dragonflyContainersOwnedFieldsEqual(cur, want corev1.Container) bool {
	return cur.Image == want.Image &&
		equality.Semantic.DeepEqual(cur.Args, want.Args) &&
		equality.Semantic.DeepEqual(cur.Env, want.Env) &&
		equality.Semantic.DeepEqual(cur.Ports, want.Ports) &&
		equality.Semantic.DeepEqual(cur.VolumeMounts, want.VolumeMounts) &&
		equality.Semantic.DeepEqual(cur.Resources, want.Resources) &&
		equality.Semantic.DeepEqual(cur.LivenessProbe, want.LivenessProbe) &&
		equality.Semantic.DeepEqual(cur.ReadinessProbe, want.ReadinessProbe)
}

func dragonflyStatefulSetRolloutComplete(sts *appsv1.StatefulSet) bool {
	desired := int32(1)
	if sts.Spec.Replicas != nil {
		desired = *sts.Spec.Replicas
	}
	return sts.Status.ObservedGeneration >= sts.Generation &&
		sts.Status.Replicas == desired &&
		sts.Status.UpdatedReplicas == desired &&
		sts.Status.ReadyReplicas == desired &&
		sts.Status.CurrentReplicas == desired
}

// teardownDragonflyResources deletes Dragonfly StatefulSets, per-site
// Services, and the active Service for the given FG, identified by the
// Bloodraven app=dragonfly + instance=<fg> label pair. Deletes are
// label-scoped so only resources Bloodraven created are removed.
//
// Idempotent: an FG that never had Dragonfly enabled has no matching
// resources, and DeleteAllOf returns no error in that case.
func (r *MysqlFailoverGroupReconciler) teardownDragonflyResources(ctx context.Context, fg *v1alpha1.MysqlFailoverGroup) error {
	selector := client.MatchingLabels{
		labelAppName:  dragonflyAppName,
		labelInstance: fg.Name,
	}
	inNs := client.InNamespace(fg.Namespace)
	if err := r.DeleteAllOf(ctx, &appsv1.StatefulSet{}, inNs, selector); err != nil {
		return fmt.Errorf("teardown dragonfly statefulsets: %w", err)
	}
	if err := r.DeleteAllOf(ctx, &policyv1.PodDisruptionBudget{}, inNs, selector); err != nil {
		return fmt.Errorf("teardown dragonfly pdbs: %w", err)
	}
	// Service has no DeleteAllOf support in client-go for typed
	// resources; list-and-delete instead.
	var svcs corev1.ServiceList
	if err := r.List(ctx, &svcs, inNs, selector); err != nil {
		return fmt.Errorf("teardown: list dragonfly services: %w", err)
	}
	for i := range svcs.Items {
		if err := r.Delete(ctx, &svcs.Items[i]); err != nil && !apierrors.IsNotFound(err) {
			return fmt.Errorf("teardown: delete service %s: %w", svcs.Items[i].Name, err)
		}
	}
	return nil
}

// Compile-time guard: ensure StatefulSet ownership compiles cleanly when
// SetupWithManager is updated to Owns(StatefulSet).
var _ client.Object = &appsv1.StatefulSet{}

// dragonflyDial opens a one-shot Dragonfly connection to the named site.
// Used by the planned-failover state machine to issue INFO/REPLTAKEOVER
// commands. Reads the auth password from the configured Secret each
// call; the operator polls infrequently and Dragonfly handles short-
// lived connections cheaply.
//
// Secret read errors are surfaced — silently dialing with an empty
// password against an auth-enabled Dragonfly would AUTH-fail anyway,
// but with a less obvious error and no log of the underlying RBAC or
// missing-secret cause.
//
// Tests inject a fake by overwriting r.dragonflyConnector.
func (r *MysqlFailoverGroupReconciler) dragonflyDial(ctx context.Context, fg *v1alpha1.MysqlFailoverGroup, siteName string) (DragonflyConnection, error) {
	if !dragonflyEnabled(fg) {
		return nil, fmt.Errorf("dragonfly disabled")
	}
	connector := r.dragonflyConnector
	if connector == nil {
		connector = realDragonflyConnector
	}
	password := ""
	if auth := fg.Spec.Dragonfly.Auth; auth != nil && auth.SecretName != "" {
		key := auth.PasswordKey
		if key == "" {
			key = "password"
		}
		var s corev1.Secret
		if err := r.Get(ctx, types.NamespacedName{Namespace: fg.Namespace, Name: auth.SecretName}, &s); err != nil {
			return nil, fmt.Errorf("dragonfly: read auth secret %s: %w", auth.SecretName, err)
		}
		raw, ok := s.Data[key]
		if !ok {
			return nil, fmt.Errorf("dragonfly: auth secret %s missing key %q", auth.SecretName, key)
		}
		password = string(raw)
	}
	addr := dragonflyAddr(fg, siteName)
	return connector(ctx, addr, password)
}

// effectiveDragonflyMasterSite returns the site name whose Dragonfly pod
// should currently carry dragonfly-role=master.
//
// Normally that's status.activeSite. During the planned-failover window
// where we have promoted the target Dragonfly but have not yet flipped
// status.activeSite (PromotingDragonfly through Resuming), the target
// already holds the master, so we return the target instead. A
// Dragonfly-only emergency promotion can also intentionally diverge from
// MySQL; in that case status.dragonfly.activeSite is authoritative for
// cache routing until a later MySQL/Dragonfly promotion moves it again.
// Without these branches syncDragonflyPodLabels would re-apply the label
// to the stale source and fight the promotion path.
func effectiveDragonflyMasterSite(fg *v1alpha1.MysqlFailoverGroup) string {
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
	if fg.Status.Dragonfly != nil && fg.Status.Dragonfly.ActiveSite != "" {
		return fg.Status.Dragonfly.ActiveSite
	}
	return fg.Status.ActiveSite
}
