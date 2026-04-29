package controller

import (
	"context"
	"fmt"
	"strconv"
	"strings"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/intstr"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"

	v1alpha1 "github.com/shipstream/bloodraven/api/v1alpha1"
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

		// Pod labels include the operator-assigned dragonfly-role
		// (default "replica") and dragonfly-traffic=enabled. The
		// DragonflyManager / planned-failover handlers rewrite these on
		// the active site during promotion. Static labels here must not
		// depend on mutable status, otherwise every status change would
		// trigger a rollout. A pod that loses the traffic label
		// transiently during a strip→takeover→restore sequence is
		// re-stamped at the end of that sequence; if the StatefulSet
		// recreates the pod for any reason, the freshly-rendered
		// template's traffic=enabled brings it back into the Service
		// without operator intervention.
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
				NodeSelector: map[string]string{
					"topology.kubernetes.io/zone": site.Zone,
				},
				// Tolerate the failover group's db-readonly taint so
				// Dragonfly stays resident through site read-only states
				// — same contract as MySQL.
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
	})
	return err
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
		return nil
	}
	key := spec.Auth.PasswordKey
	if key == "" {
		key = "password"
	}
	return []corev1.EnvVar{
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
}

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
func (r *MysqlFailoverGroupReconciler) reconcileDragonflyResources(ctx context.Context, fg *v1alpha1.MysqlFailoverGroup) error {
	if !dragonflyEnabled(fg) {
		return r.teardownDragonflyResources(ctx, fg)
	}
	for _, site := range fg.Spec.Sites {
		if err := r.reconcileDragonflyStatefulSet(ctx, fg, site); err != nil {
			return fmt.Errorf("reconcile dragonfly statefulset %s: %w", site.Name, err)
		}
		if err := r.reconcileDragonflySiteService(ctx, fg, site); err != nil {
			return fmt.Errorf("reconcile dragonfly site service %s: %w", site.Name, err)
		}
	}
	if err := r.reconcileDragonflyActiveService(ctx, fg); err != nil {
		return fmt.Errorf("reconcile dragonfly active service: %w", err)
	}
	return nil
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
// already holds the master, so we return the target instead. Without
// this branch syncDragonflyPodLabels would re-apply the label to the
// stale source on every reconcile and fight the planned-failover state
// machine.
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
	return fg.Status.ActiveSite
}

