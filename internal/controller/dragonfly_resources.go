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
)

// Dragonfly-specific labels and constants.
const (
	dragonflyAppName = "dragonfly"

	// dragonflyTrafficEnabled is the only value of
	// v1alpha1.DragonflyTrafficLabel; an ineligible pod has no label.
	dragonflyTrafficEnabled = v1alpha1.LabelValueTrue

	dragonflyDefaultPort      = int32(6379)
	dragonflyDefaultAdminPort = int32(9999)

	dragonflyContainerName = "dragonfly"

	// DragonflyAuthEnvVar and DragonflyMasterAuthEnvVar set Dragonfly's
	// --requirepass and --masterauth through its DFLY_<flag> env
	// mechanism, so the password is neither in the pod spec nor in the
	// process argv.
	DragonflyAuthEnvVar       = "DFLY_requirepass"
	DragonflyMasterAuthEnvVar = "DFLY_masterauth"
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
		labelAppName:                dragonflyAppName,
		labelInstance:               fgName,
		v1alpha1.FailoverGroupLabel: fgName,
		v1alpha1.SiteLabel:          siteName,
		labelManagedBy:              managerName,
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

// dragonflyRenderImage is the image the ordinary render gives a site's
// Dragonfly container. Under upgradeStrategy Rolling it is
// spec.dragonfly.image. Under SnapshotRestore the render never moves an
// image: an existing StatefulSet (sts as read back) keeps its live image,
// including while a run waits for admission and after a run failed, and
// only the snapshot upgrade's own phases change it. A StatefulSet being
// created starts on spec.dragonfly.image.
func dragonflyRenderImage(spec *v1alpha1.DragonflySpec, sts *appsv1.StatefulSet) string {
	if dragonflyUpgradeStrategy(spec) == v1alpha1.DragonflyUpgradeStrategySnapshotRestore {
		if live := dragonflyTemplateImage(sts); live != "" {
			return live
		}
	}
	return spec.Image
}

func (r *MysqlFailoverGroupReconciler) applyDragonflyStatefulSetSpec(fg *v1alpha1.MysqlFailoverGroup, site v1alpha1.SiteSpec, sts *appsv1.StatefulSet) error {
	spec := fg.Spec.Dragonfly
	if err := controllerutil.SetControllerReference(fg, sts, r.Scheme); err != nil {
		return err
	}

	labels := dragonflyCommonLabels(fg.Name, site.Name)
	sts.Labels = labels
	// The operator release that wrote this template (rollout.go).
	stampRenderedBy(sts)

	var replicas int32 = 1
	port := dragonflyPort(spec)
	adminPort := dragonflyAdminPort(spec)

	args := buildDragonflyArgs(spec, port, adminPort)
	env := buildDragonflyEnv(spec)

	container := corev1.Container{
		Name:  dragonflyContainerName,
		Image: dragonflyRenderImage(spec, sts),
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
			TimeoutSeconds:      1,
			SuccessThreshold:    1,
			FailureThreshold:    3,
		},
		ReadinessProbe: &corev1.Probe{
			ProbeHandler: corev1.ProbeHandler{
				TCPSocket: &corev1.TCPSocketAction{Port: intstr.FromInt32(port)},
			},
			InitialDelaySeconds: 2,
			PeriodSeconds:       5,
			TimeoutSeconds:      1,
			SuccessThreshold:    1,
			FailureThreshold:    3,
		},
		// Container-level security context applied verbatim from
		// spec.dragonfly.containerSecurityContext (opt-in; nil-by-default
		// to preserve backward-compat). See
		// site/content/docs/4.configuration/10.production-hardening.md for the rationale.
		// DeepCopy so the cached spec pointer is not aliased in the
		// rendered PodSpec (matches F4 fix on MySQL wiring).
		SecurityContext: spec.ContainerSecurityContext.DeepCopy(),
	}

	// Selector is immutable on a StatefulSet: set it on create, and refuse
	// a live one that differs (workload_selector.go).
	if sts.Spec.Selector == nil {
		sts.Spec.Selector = dragonflyStatefulSetSelector(fg, site.Name)
	} else if mismatch := statefulSetSelectorMismatch(sts, fg, site.Name); mismatch != nil {
		return mismatch
	}

	// Pod labels include the operator-assigned dragonfly-role (default
	// "replica") and dragonfly-traffic=true. The DragonflyManager /
	// planned-failover handlers rewrite these on the active site during
	// promotion. Static labels here must not depend on mutable status,
	// otherwise every status change would trigger a rollout.
	podLabels := make(map[string]string, len(labels)+2)
	for k, v := range labels {
		podLabels[k] = v
	}
	podLabels[v1alpha1.DragonflyRoleLabel] = "replica"
	podLabels[v1alpha1.DragonflyTrafficLabel] = dragonflyTrafficEnabled

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
			Tolerations: groupReadOnlyTolerations(fg),
			Containers:  []corev1.Container{container},
			Volumes: []corev1.Volume{
				{
					Name: "data",
					VolumeSource: corev1.VolumeSource{
						EmptyDir: &corev1.EmptyDirVolumeSource{},
					},
				},
			},
			// Pod-level security context applied verbatim from
			// spec.dragonfly.podSecurityContext (opt-in; nil-by-default).
			// DeepCopy so the rendered PodSpec does not alias the
			// cached spec pointer.
			SecurityContext: spec.PodSecurityContext.DeepCopy(),
		},
	}
	return nil
}

// buildDragonflyArgs assembles the Dragonfly container command-line. The
// operator owns the flags in dragonflyOperatorOwnedFlags; user-supplied
// args are appended last with those filtered out.
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
	// Admission requires maxMemory > 0 and proactorThreads >= 1; unset leaves
	// Dragonfly's defaults. --maxmemory takes bytes; Value rounds a
	// fractional quantity up to the next byte.
	if spec.MaxMemory != nil && spec.MaxMemory.Sign() > 0 {
		args = append(args, fmt.Sprintf("--maxmemory=%d", spec.MaxMemory.Value()))
	}
	if spec.ProactorThreads != nil && *spec.ProactorThreads > 0 {
		args = append(args, fmt.Sprintf("--proactor_threads=%d", *spec.ProactorThreads))
	}
	if spec.Snapshot != nil && spec.Snapshot.Dir != "" {
		args = append(args, "--dir="+spec.Snapshot.Dir)
		args = append(args, dragonflyS3EndpointArgs(spec.Snapshot.S3EndpointURL)...)
		if spec.Snapshot.S3SignPayload != nil {
			args = append(args, "--s3_sign_payload="+strconv.FormatBool(*spec.Snapshot.S3SignPayload))
		}
	}
	// Auth (--requirepass/--masterauth) is set through env in
	// buildDragonflyEnv, not here, so the password stays off argv.
	args = append(args, filterDragonflyUserArgs(spec.Args)...)
	return args
}

// dragonflyS3EndpointArgs adapts spec.dragonfly.snapshot.s3EndpointURL to
// Dragonfly's two flags: --s3_endpoint takes host[:port] without a scheme,
// and --s3_use_https takes the scheme as a boolean. An empty URL means AWS
// S3 with Dragonfly's defaults, so neither flag is emitted. Admission only
// admits an http:// or https:// URL with a host, an optional port from 1 to
// 65535 and no path; anything else (reachable only through a client that
// skips admission) also emits nothing rather than a flag Dragonfly would
// misread.
func dragonflyS3EndpointArgs(endpointURL string) []string {
	var useHTTPS bool
	var hostPort string
	switch {
	case strings.HasPrefix(endpointURL, "https://"):
		useHTTPS, hostPort = true, strings.TrimPrefix(endpointURL, "https://")
	case strings.HasPrefix(endpointURL, "http://"):
		useHTTPS, hostPort = false, strings.TrimPrefix(endpointURL, "http://")
	default:
		return nil
	}
	hostPort = strings.TrimSuffix(hostPort, "/")
	if hostPort == "" || strings.ContainsAny(hostPort, "/?#@ \t\r\n") || !validDragonflyEndpointHostPort(hostPort) {
		return nil
	}
	return []string{
		"--s3_endpoint=" + hostPort,
		"--s3_use_https=" + strconv.FormatBool(useHTTPS),
	}
}

// validDragonflyEndpointHostPort reports whether hostPort is a host (a
// name, an IPv4 address, or an IPv6 address in brackets) with an optional
// port from 1 to 65535: no empty or non-numeric port.
func validDragonflyEndpointHostPort(hostPort string) bool {
	host, port := hostPort, ""
	if strings.HasPrefix(hostPort, "[") {
		end := strings.Index(hostPort, "]")
		if end < 0 {
			return false
		}
		host, port = hostPort[:end+1], hostPort[end+1:]
		if port != "" && !strings.HasPrefix(port, ":") {
			return false
		}
		port = strings.TrimPrefix(port, ":")
		if len(host) <= 2 || port == "" && strings.HasSuffix(hostPort, ":") {
			return false
		}
	} else if i := strings.IndexByte(hostPort, ':'); i >= 0 {
		host, port = hostPort[:i], hostPort[i+1:]
		if port == "" {
			return false
		}
	}
	if host == "" || strings.ContainsAny(strings.Trim(host, "[]"), "[]") {
		return false
	}
	if port == "" {
		return true
	}
	n, err := strconv.Atoi(port)
	return err == nil && n >= 1 && n <= 65535 && strconv.Itoa(n) == port
}

func dragonflyServiceAccountName(spec *v1alpha1.DragonflySpec) string {
	if spec == nil || spec.Snapshot == nil {
		return ""
	}
	return spec.Snapshot.ServiceAccountName
}

// dragonflyOperatorOwnedFlags lists Dragonfly flags whose values are a
// safety contract owned by Bloodraven, by name without dashes; the value
// says whether the flag is a boolean (it takes no separate value
// argument). User-supplied spec.Args that set any of them are stripped
// before being appended to the operator's args. Dragonfly parses flags with
// Abseil (last one wins), so a user who set
// `spec.Args = ["--nobreak_replication_on_master_restart"]` would otherwise
// override the safety guard the operator just emitted. flagfile, fromenv
// and tryfromenv read flags from elsewhere, so they could set any of the
// others. Admission refuses the same set (the spec.dragonfly.args rule;
// TestDragonflyArgsRuleCoversOwnedFlags keeps the two in step).
var dragonflyOperatorOwnedFlags = map[string]bool{
	"break_replication_on_master_restart": true,
	"bind":                                false,
	"port":                                false,
	"admin_port":                          false,
	"dir":                                 false,
	"maxmemory":                           false,
	"proactor_threads":                    false,
	"replicaof":                           false,
	"s3_endpoint":                         false,
	"s3_use_https":                        true,
	"s3_sign_payload":                     true,
	"requirepass":                         false,
	"masterauth":                          false,
	"flagfile":                            false,
	"fromenv":                             false,
	"tryfromenv":                          false,
}

// dragonflyOwnedFlag reports whether arg sets an operator-owned flag in a
// spelling Abseil accepts (-flag, --flag, -flag=v, --flag=v, and --noflag
// for a boolean), and whether the flag's value is the next argument (the
// `--flag value` form of a non-boolean flag).
func dragonflyOwnedFlag(arg string) (owned, valueFollows bool) {
	name, ok := strings.CutPrefix(arg, "-")
	if !ok {
		return false, false
	}
	name = strings.TrimPrefix(name, "-")
	name, _, hasValue := strings.Cut(name, "=")
	if boolean, ok := dragonflyOperatorOwnedFlags[name]; ok {
		return true, !hasValue && !boolean
	}
	if negated, ok := strings.CutPrefix(name, "no"); ok {
		if _, ok := dragonflyOperatorOwnedFlags[negated]; ok {
			return true, false
		}
	}
	return false, false
}

// filterDragonflyUserArgs drops args that target operator-owned flags,
// together with the value argument of the `--flag value` form.
func filterDragonflyUserArgs(in []string) []string {
	if len(in) == 0 {
		return nil
	}
	out := make([]string, 0, len(in))
	for i := 0; i < len(in); i++ {
		owned, valueFollows := dragonflyOwnedFlag(in[i])
		if !owned {
			out = append(out, in[i])
			continue
		}
		if valueFollows {
			i++
		}
	}
	return out
}

// buildDragonflyEnv wires the auth password into the container's env from
// the configured Secret, leaving env empty when auth is not set or when
// the Secret name is empty (CRD validation catches the latter, but a
// typed client that skips admission, such as a fake client, can still
// produce it; emitting an EnvVar with an empty Secret name makes the pod
// fail to start with a secret-not-found event rather than a clean
// reconcile error).
func buildDragonflyEnv(spec *v1alpha1.DragonflySpec) []corev1.EnvVar {
	if spec.Auth == nil || spec.Auth.PasswordSecretKeyRef.Name == "" {
		return buildDragonflySnapshotEnv(spec)
	}
	ref := spec.Auth.PasswordSecretKeyRef
	secretRef := func(name string) corev1.EnvVar {
		return corev1.EnvVar{
			Name: name,
			ValueFrom: &corev1.EnvVarSource{
				SecretKeyRef: &corev1.SecretKeySelector{
					LocalObjectReference: corev1.LocalObjectReference{Name: ref.Name},
					Key:                  ref.KeyOrDefault(),
				},
			},
		}
	}
	// --masterauth must match --requirepass: REPLICAOF authenticates to
	// the master with masterauth. Without it, password-protected masters
	// reject the replica PING with NOAUTH and Dragonfly surfaces
	// "ERR replication cancelled", leaving non-active sites as stale
	// masters forever.
	env := []corev1.EnvVar{
		secretRef(DragonflyAuthEnvVar),
		secretRef(DragonflyMasterAuthEnvVar),
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
				labelAppName:       dragonflyAppName,
				labelInstance:      fg.Name,
				v1alpha1.SiteLabel: site.Name,
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
			labelAppName:                dragonflyAppName,
			labelInstance:               fg.Name,
			v1alpha1.FailoverGroupLabel: fg.Name,
			labelManagedBy:              managerName,
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
				labelAppName:                   dragonflyAppName,
				labelInstance:                  fg.Name,
				v1alpha1.DragonflyRoleLabel:    "master",
				v1alpha1.DragonflyTrafficLabel: dragonflyTrafficEnabled,
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
			labelAppName:                dragonflyAppName,
			labelInstance:               fg.Name,
			v1alpha1.FailoverGroupLabel: fg.Name,
			v1alpha1.SiteLabel:          site.Name,
			labelManagedBy:              managerName,
		}
		pdb.Spec = policyv1.PodDisruptionBudgetSpec{
			MinAvailable: &intstr.IntOrString{Type: intstr.Int, IntVal: 1},
			Selector: &metav1.LabelSelector{
				MatchLabels: map[string]string{
					labelAppName:       dragonflyAppName,
					labelInstance:      fg.Name,
					v1alpha1.SiteLabel: site.Name,
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
		r.clearDragonflySelectorRefusals(fg)
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
	if err := r.observeDragonflyImage(ctx, fg); err != nil {
		return 0, fmt.Errorf("record dragonfly image: %w", err)
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

// observeDragonflyImage records status.dragonfly.image: the Dragonfly
// container image of the active Dragonfly site's StatefulSet. It is an
// observation only; nothing compares spec.dragonfly.image with it.
func (r *MysqlFailoverGroupReconciler) observeDragonflyImage(ctx context.Context, fg *v1alpha1.MysqlFailoverGroup) error {
	master := effectiveDragonflyMasterSite(fg)
	if master == "" {
		return nil
	}
	var sts appsv1.StatefulSet
	key := types.NamespacedName{Namespace: fg.Namespace, Name: dragonflyStatefulSetName(fg.Name, master)}
	if err := r.Get(ctx, key, &sts); err != nil {
		return client.IgnoreNotFound(err)
	}
	image := dragonflyTemplateImage(&sts)
	if image == "" || (fg.Status.Dragonfly != nil && fg.Status.Dragonfly.Image == image) {
		return nil
	}
	patch := client.MergeFrom(fg.DeepCopy())
	if fg.Status.Dragonfly == nil {
		fg.Status.Dragonfly = &v1alpha1.DragonflyStatus{}
	}
	fg.Status.Dragonfly.Image = image
	return client.IgnoreNotFound(r.Status().Patch(ctx, fg, patch))
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

	reader := client.Reader(r.Client)
	if r.APIReader != nil {
		reader = r.APIReader
	}

	activeSite := effectiveDragonflyMasterSite(fg)
	var activeDrift *v1alpha1.SiteSpec

	for i := range fg.Spec.Sites {
		site := fg.Spec.Sites[i]
		var current appsv1.StatefulSet
		key := types.NamespacedName{Namespace: fg.Namespace, Name: dragonflyStatefulSetName(fg.Name, site.Name)}
		if err := reader.Get(ctx, key, &current); err != nil {
			if apierrors.IsNotFound(err) {
				r.noteWorkloadSelector(fg, "StatefulSet", key.Name, nil)
				if err := r.reconcileDragonflyStatefulSet(ctx, fg, site); err != nil {
					return 0, fmt.Errorf("reconcile dragonfly statefulset %s: %w", site.Name, err)
				}
				continue
			}
			return 0, fmt.Errorf("get dragonfly statefulset %s: %w", site.Name, err)
		}
		// A StatefulSet whose immutable selector differs is refused: the
		// site is skipped, not updated, and not retried in a loop.
		mismatch := statefulSetSelectorMismatch(&current, fg, site.Name)
		r.noteWorkloadSelector(fg, "StatefulSet", key.Name, mismatch)
		if mismatch != nil {
			continue
		}

		desired, err := r.desiredDragonflyStatefulSet(fg, site, &current)
		if err != nil {
			return 0, fmt.Errorf("build desired dragonfly statefulset %s: %w", site.Name, err)
		}
		if !dragonflyStatefulSetTemplateEqual(&current, desired) {
			// A template another operator release rendered moves only once
			// the group is admitted to the paced rollout (rollout.go).
			if held, err := r.gateDragonflyRollout(ctx, fg, &current); err != nil {
				return 0, fmt.Errorf("paced rollout of dragonfly statefulset %s: %w", site.Name, err)
			} else if held {
				continue
			}
			if site.Name == activeSite {
				activeDrift = &fg.Spec.Sites[i]
				continue
			}
			if requeue, err := r.rollDragonflyReplicaStatefulSet(ctx, fg, site); err != nil || requeue > 0 {
				return requeue, err
			}
			return requeueAfter, nil
		}
		if r.RolloutPacing && renderedByOther(&current) {
			if err := r.restampWorkload(ctx, &current); err != nil {
				return 0, fmt.Errorf("restamp dragonfly statefulset %s: %w", site.Name, err)
			}
		}
	}

	if activeDrift != nil {
		// A planned failover that owns the group's Dragonfly owns its active
		// site too (plannedOwnsDragonfly): with the topology manager
		// registered, the claim slot decides (a run, its provisional grant,
		// or a hold; a run that released its claim after recording its
		// promotion owns nothing, whatever status says); before that, the
		// status form does (a run that still needs the claim, or a durable
		// hold). A request that is only waiting owns nothing.
		if r.plannedOwnsDragonfly(fg) {
			return requeueAfter, nil
		}
		promotionTarget := ""
		for _, site := range fg.Spec.Sites {
			if site.Name == activeSite {
				continue
			}
			var current appsv1.StatefulSet
			key := types.NamespacedName{Namespace: fg.Namespace, Name: dragonflyStatefulSetName(fg.Name, site.Name)}
			if err := reader.Get(ctx, key, &current); err != nil {
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
			check := r.dragonflyRolloutCandidateSyncReady(ctx, fg, promotionTarget, activeSite)
			r.noteDragonflyRolloutReadiness(ctx, fg, promotionTarget, activeSite, check)
			if !check.Ready {
				return requeueAfter, nil
			}
			if wait := r.dragonflyRolloutPromotionBackoff(fg, promotionTarget, activeSite); wait > 0 {
				return wait, nil
			}
			if promoted, deferred := r.promoteDragonflyForRollout(ctx, fg, promotionTarget, activeSite); deferred {
				return dragonflyGateRetry, nil
			} else if promoted {
				r.clearDragonflyRolloutPromotionBackoff(fg, promotionTarget, activeSite)
			} else if r.recordDragonflyRolloutPromotionFailure(fg, promotionTarget, activeSite) >= 3 {
				// The last resort: the takeover to a target that was
				// found ready failed three times, so the active site's pod
				// is restarted where it is. No REPLICAOF NO ONE is sent
				// over the live master for this (promoteLocked); the
				// Dragonfly manager then handles the restarted master as
				// it handles any other (a replica that holds the cache
				// the restarted instance lost is promoted, one known to be
				// stale is not).
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

// rollDragonflyReplicaStatefulSet applies the template change, and so the
// pod restart, of a site chosen as a replica from the reconcile's earlier
// read. It does so under the group's Dragonfly gate after re-reading the
// group uncached: a promotion that has just made this site the Dragonfly
// master finishes first and the restart is held back (the next reconcile
// treats the site as the active one). requeue > 0 means it was held back.
func (r *MysqlFailoverGroupReconciler) rollDragonflyReplicaStatefulSet(ctx context.Context, fg *v1alpha1.MysqlFailoverGroup, site v1alpha1.SiteSpec) (requeue time.Duration, err error) {
	nn := types.NamespacedName{Namespace: fg.Namespace, Name: fg.Name}
	release, ok := r.acquireDragonflyGate(ctx, nn)
	if !ok {
		return dragonflyGateRetry, nil
	}
	defer release()
	fresh, err := r.freshFailoverGroup(ctx, nn)
	if err != nil {
		return 0, fmt.Errorf("re-read failover group before dragonfly rollout of %s: %w", site.Name, err)
	}
	if master := effectiveDragonflyMasterSite(fresh); master == site.Name {
		log.FromContext(ctx).Info("dragonfly rollout: site became the active Dragonfly master; holding its restart", "site", site.Name, "fg", nn)
		return dragonflyGateRetry, nil
	}
	if pf := fresh.Status.PlannedFailover; pf != nil && pf.TargetSite == site.Name && r.plannedOwnsDragonfly(fresh) {
		// The planned run is about to take the Dragonfly master over to this
		// site: a restart now brings it back as an empty standalone master
		// (no --replicaof, emptyDir data), and the paused Dragonfly manager
		// would not re-attach it before the takeover.
		log.FromContext(ctx).Info("dragonfly rollout: site is the planned failover's Dragonfly target; holding its restart", "site", site.Name, "fg", nn)
		return dragonflyGateRetry, nil
	}
	if err := r.reconcileDragonflyStatefulSet(ctx, fg, site); err != nil {
		return 0, fmt.Errorf("reconcile dragonfly statefulset %s: %w", site.Name, err)
	}
	return 0, nil
}

// promoteDragonflyForRollout runs the rollout promotion in the reconcile,
// authorized by the group's Dragonfly gate (not by whether a manager is
// registered). It waits up to defaultDragonflyGateWait for the gate and
// holds it for the whole promotion, so it never overlaps the manager's
// Ticks and promotions, a stopping manager's last step, a planned failover
// or a snapshot upgrade. The target and source were chosen from an earlier,
// possibly cached, read: under the gate they are revalidated against an
// uncached read, and the target's sync is proven again. deferred reports
// that the promotion was not attempted (the gate stayed busy, or the choice
// no longer holds); that is not a failed promotion.
func (r *MysqlFailoverGroupReconciler) promoteDragonflyForRollout(ctx context.Context, fg *v1alpha1.MysqlFailoverGroup, target, oldSource string) (promoted, deferred bool) {
	nn := types.NamespacedName{Namespace: fg.Namespace, Name: fg.Name}
	release, ok := r.acquireDragonflyGate(ctx, nn)
	if !ok {
		return false, true
	}
	defer release()
	logger := log.FromContext(ctx)
	fresh, err := r.freshFailoverGroup(ctx, nn)
	if err != nil {
		logger.Info("dragonfly rollout: re-read failover group failed", "fg", nn, "error", err)
		return false, true
	}
	if why := rolloutPromotionBlocked(fresh, target, oldSource, r.plannedOwnsDragonfly(fresh)); why != "" {
		logger.Info("dragonfly rollout: promotion no longer valid; skipping", "site", target, "reason", why, "fg", nn)
		return false, true
	}
	// Under the gate: a planned-failover claim (a run, its provisional
	// grant, or an automation hold) or a standing hold owns the active
	// Dragonfly. The temporary promoter has no planned owner of its own,
	// so the check is here.
	if r.plannedClaimHeld(nn) || PlannedHoldFrom(fresh) != nil {
		logger.Info("dragonfly rollout: promotion no longer valid; skipping", "site", target,
			"reason", "a planned failover claim or automation hold owns the sites", "fg", nn)
		return false, true
	}
	check := r.dragonflyRolloutCandidateSyncReady(ctx, fresh, target, oldSource)
	r.noteDragonflyRolloutReadiness(ctx, fresh, target, oldSource, check)
	if !check.Ready {
		logger.Info("dragonfly rollout: target no longer in sync; skipping", "site", target, "error", check.Err,
			"state", check.State, "detail", check.Detail, "fg", nn)
		return false, true
	}
	// A target whose sync rests on its link alone is promoted with
	// REPLTAKEOVER or not at all.
	fallback := dragonflyFallbackUnlessSourceServes
	if check.TakeoverOnly {
		fallback = dragonflyTakeoverOnly
	}
	promoter := NewDragonflyManager(r.Client, r.Recorder, slog.Default().With("fg", nn.String(), "subsystem", "dragonfly-rollout"), nn, 0)
	promoter.SetAuthorityReader(r.authorityReader())
	if r.dragonflyConnector != nil {
		promoter.SetConnector(r.dragonflyConnector)
	}
	return promoter.rolloutPromoteFG(context.WithoutCancel(ctx), fresh, target, oldSource, fallback), false
}

// dragonflyRolloutSyncWait is how long one rollout readiness check waits
// for the target to reach the position it read from the source, and
// dragonflyRolloutSyncPoll how often it looks. The source takes writes all
// the while, and a replica is always a network round trip behind it, so a
// target read once right after the source would often be "behind" on a
// busy cache and the rollout would not move.
const (
	dragonflyRolloutSyncWait = 2 * time.Second
	dragonflyRolloutSyncPoll = 200 * time.Millisecond
)

// dragonflyRolloutCheck is the result of one rollout readiness check.
type dragonflyRolloutCheck = dragonflySyncCheck

// dragonflyRolloutCandidateSyncReady reports whether target can take over
// from source for a rollout (dragonflyProveSync): it reads the source's
// replication position, then looks at the target.
//
// It runs on the group controller's reconcile worker, which serves every
// group, so it waits only where waiting can help: a target that is linked
// and behind is looked at again every dragonflyRolloutSyncPoll for up to
// dragonflyRolloutSyncWait, or until ctx ends. Everything else returns at
// once: a target that is not in stable sync (link down, full sync,
// loading) and a site that cannot be read. The caller comes back on its
// next pass.
//
// A position that can never be compared (a site refuses the position
// command) does not hold the rollout for good: when the link checks hold,
// the target is ready for a takeover with no fallback (TakeoverOnly).
// Before, such a rollout never completed: the check returned "not ready"
// on every pass, ahead of the promotion whose three failures are what end
// a rollout that cannot promote, so a group whose Dragonfly user or image
// refuses `DEBUG` or `DFLY` kept its old active pod indefinitely.
func (r *MysqlFailoverGroupReconciler) dragonflyRolloutCandidateSyncReady(ctx context.Context, fg *v1alpha1.MysqlFailoverGroup, target, source string) dragonflyRolloutCheck {
	return dragonflyProveSync(ctx, r.dragonflySiteDialer(fg), target, source, dragonflyRolloutSyncWait, dragonflyRolloutSyncPoll)
}

// dragonflyRolloutStallAfter is how long a rollout's target may stay not
// ready before the stall is reported in a DragonflyRolloutStalled Event. A
// replica restarted by the same rollout needs a full sync first, which
// takes this long on a large cache without anything being wrong.
const dragonflyRolloutStallAfter = 2 * time.Minute

// noteDragonflyRolloutReadiness makes a rollout that waits for its target
// visible without a line every pass (the rollout requeues every two
// seconds): the state is logged when it changes, with the detail, and a
// target that has not been ready for dragonflyRolloutStallAfter is reported
// once in a Warning Event (DragonflyRolloutStalled), as an ordered MySQL
// update that waits for a pod is (OrderedUpdateStalled). A ready check ends
// the record.
func (r *MysqlFailoverGroupReconciler) noteDragonflyRolloutReadiness(ctx context.Context, fg *v1alpha1.MysqlFailoverGroup, target, source string, check dragonflyRolloutCheck) {
	key := dragonflyRolloutKey{fg: types.NamespacedName{Namespace: fg.Namespace, Name: fg.Name}, target: target, source: source}
	now := r.now()
	r.dragonflyRolloutMu.Lock()
	if r.dragonflyRolloutBackoff == nil {
		r.dragonflyRolloutBackoff = make(map[dragonflyRolloutKey]dragonflyRolloutState)
	}
	st := r.dragonflyRolloutBackoff[key]
	was := st.notReady
	if check.Ready {
		st.notReady, st.notReadySince, st.stallReported = "", time.Time{}, false
	} else {
		if st.notReadySince.IsZero() {
			st.notReadySince = now
		}
		st.notReady = check.State
	}
	waited := now.Sub(st.notReadySince)
	report := !check.Ready && !st.stallReported && waited >= dragonflyRolloutStallAfter
	if report {
		st.stallReported = true
	}
	// A target that is ready on its link alone is said to be so once per
	// rollout; a later check that proves it by position ends that.
	reportUnproven := check.Ready && check.TakeoverOnly && !st.unprovenReported
	if check.Ready {
		st.unprovenReported = check.TakeoverOnly
	}
	if st == (dragonflyRolloutState{}) {
		delete(r.dragonflyRolloutBackoff, key)
	} else {
		r.dragonflyRolloutBackoff[key] = st
	}
	r.dragonflyRolloutMu.Unlock()

	logger := log.FromContext(ctx)
	if reportUnproven {
		logger.Info("dragonfly: sync could not be proven by position; relying on REPLTAKEOVER",
			"source", source, "target", target, "error", check.Detail, "fg", key.fg)
		if r.Recorder != nil {
			r.Recorder.Eventf(fg, corev1.EventTypeWarning, v1alpha1.EventDragonflySyncUnproven, "%s",
				dragonflySyncUnprovenMessage(target, source, check.Detail))
		}
	}
	switch {
	case check.Ready && was != "":
		logger.Info("dragonfly rollout: target in sync", "target", target, "source", source, "fg", key.fg)
	case !check.Ready && was != check.State && check.Err != nil:
		logger.Info("dragonfly rollout: sync-readiness check failed", "target", target, "source", source,
			"state", check.State, "error", check.Err, "fg", key.fg)
	case !check.Ready && was != check.State:
		logger.Info("dragonfly rollout: target not ready for promotion", "target", target, "source", source,
			"state", check.State, "detail", check.Detail, "fg", key.fg)
	}
	if report {
		logger.Info("dragonfly rollout: stalled waiting for the target", "target", target, "source", source,
			"state", check.State, "detail", check.Detail, "waited", waited.Truncate(time.Second).String(), "fg", key.fg)
		if r.Recorder != nil {
			r.Recorder.Eventf(fg, corev1.EventTypeWarning, v1alpha1.EventDragonflyRolloutStalled,
				"the rollout of active Dragonfly site %q has waited %s for replica %q to be in sync with it (%s): %s",
				source, waited.Truncate(time.Second), target, check.State, check.Detail)
		}
	}
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

// desiredDragonflyStatefulSet renders site's StatefulSet as the ordinary
// render would update current (nil when it does not exist yet): the same
// mutation CreateOrUpdate applies to the object it reads back.
func (r *MysqlFailoverGroupReconciler) desiredDragonflyStatefulSet(fg *v1alpha1.MysqlFailoverGroup, site v1alpha1.SiteSpec, current *appsv1.StatefulSet) (*appsv1.StatefulSet, error) {
	sts := &appsv1.StatefulSet{
		ObjectMeta: metav1.ObjectMeta{
			Name:      dragonflyStatefulSetName(fg.Name, site.Name),
			Namespace: fg.Namespace,
		},
	}
	if current != nil {
		sts = current.DeepCopy()
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
	// Pod-level SecurityContext is an opt-in user field
	// (spec.dragonfly.podSecurityContext); a drifted live STS that
	// silently kept the old value would defeat the user's hardening, so
	// the comparison must include it.
	if !podSecurityContextEqual(curSpec.SecurityContext, wantSpec.SecurityContext) {
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
		equality.Semantic.DeepEqual(cur.ReadinessProbe, want.ReadinessProbe) &&
		// Container-level SecurityContext is opt-in
		// (spec.dragonfly.containerSecurityContext) but must be
		// drift-detected so a user's hardening actually rolls out.
		equality.Semantic.DeepEqual(cur.SecurityContext, want.SecurityContext)
}

func podSecurityContextEqual(cur, want *corev1.PodSecurityContext) bool {
	empty := func(sc *corev1.PodSecurityContext) bool {
		return sc == nil || equality.Semantic.DeepEqual(sc, &corev1.PodSecurityContext{})
	}
	if empty(cur) && empty(want) {
		return true
	}
	return equality.Semantic.DeepEqual(cur, want)
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

// teardownDragonflyResources deletes Dragonfly StatefulSets,
// PodDisruptionBudgets, per-site Services, and the active Service for the
// given FG, identified by the Bloodraven app=dragonfly + instance=<fg>
// label pair. Deletes are label-scoped so only resources Bloodraven
// created are removed.
//
// It lists and deletes rather than calling DeleteAllOf: DeleteAllOf needs
// the deletecollection verb, which the operator's RBAC does not grant, so
// every reconcile of a group with Dragonfly disabled used to fail here.
//
// Idempotent: an FG that never had Dragonfly enabled has no matching
// resources, and the lists come back empty.
func (r *MysqlFailoverGroupReconciler) teardownDragonflyResources(ctx context.Context, fg *v1alpha1.MysqlFailoverGroup) error {
	selector := client.MatchingLabels{
		labelAppName:  dragonflyAppName,
		labelInstance: fg.Name,
	}
	inNs := client.InNamespace(fg.Namespace)
	var stss appsv1.StatefulSetList
	if err := r.List(ctx, &stss, inNs, selector); err != nil {
		return fmt.Errorf("teardown: list dragonfly statefulsets: %w", err)
	}
	for i := range stss.Items {
		if err := r.Delete(ctx, &stss.Items[i]); err != nil && !apierrors.IsNotFound(err) {
			return fmt.Errorf("teardown: delete statefulset %s: %w", stss.Items[i].Name, err)
		}
	}
	var pdbs policyv1.PodDisruptionBudgetList
	if err := r.List(ctx, &pdbs, inNs, selector); err != nil {
		return fmt.Errorf("teardown: list dragonfly pdbs: %w", err)
	}
	for i := range pdbs.Items {
		if err := r.Delete(ctx, &pdbs.Items[i]); err != nil && !apierrors.IsNotFound(err) {
			return fmt.Errorf("teardown: delete pdb %s: %w", pdbs.Items[i].Name, err)
		}
	}
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
	if auth := fg.Spec.Dragonfly.Auth; auth != nil && auth.PasswordSecretKeyRef.Name != "" {
		ref := auth.PasswordSecretKeyRef
		key := ref.KeyOrDefault()
		var s corev1.Secret
		if err := r.Get(ctx, types.NamespacedName{Namespace: fg.Namespace, Name: ref.Name}, &s); err != nil {
			return nil, fmt.Errorf("dragonfly: read auth secret %s: %w", ref.Name, err)
		}
		raw, ok := s.Data[key]
		if !ok {
			return nil, fmt.Errorf("dragonfly: auth secret %s missing key %q", ref.Name, key)
		}
		password = string(raw)
	}
	addr := dragonflyAddr(fg, siteName)
	return connector(ctx, addr, password)
}

// dragonflyDialPod is dragonflyDial with the connection settings pod was
// started with rather than spec.dragonfly's: its client container port
// and the password its DFLY_requirepass env carries. A snapshot-restore
// run holds the render, so while it runs (and until the render rolls a new
// template out after it) the live pod and its site Service can keep a port
// or auth Secret that spec.dragonfly has since replaced; dialing them with
// the spec's settings would fail against a pod that is fine. A pod with no
// client port uses the spec port, and one with no password env is dialed
// without a password, as it was rendered.
func (r *MysqlFailoverGroupReconciler) dragonflyDialPod(ctx context.Context, fg *v1alpha1.MysqlFailoverGroup, siteName string, pod *corev1.Pod) (DragonflyConnection, error) {
	connector := r.dragonflyConnector
	if connector == nil {
		connector = realDragonflyConnector
	}
	port := dragonflyPort(fg.Spec.Dragonfly)
	var container *corev1.Container
	for i := range pod.Spec.Containers {
		if pod.Spec.Containers[i].Name == dragonflyContainerName {
			container = &pod.Spec.Containers[i]
			break
		}
	}
	password := ""
	if container != nil {
		for _, p := range container.Ports {
			if p.Name == "client" && p.ContainerPort > 0 {
				port = p.ContainerPort
			}
		}
		for _, env := range container.Env {
			if env.Name != DragonflyAuthEnvVar {
				continue
			}
			switch {
			case env.ValueFrom != nil && env.ValueFrom.SecretKeyRef != nil:
				ref := env.ValueFrom.SecretKeyRef
				var s corev1.Secret
				if err := r.Get(ctx, types.NamespacedName{Namespace: fg.Namespace, Name: ref.Name}, &s); err != nil {
					return nil, fmt.Errorf("dragonfly: read auth secret %s of pod %s: %w", ref.Name, pod.Name, err)
				}
				raw, ok := s.Data[ref.Key]
				if !ok {
					return nil, fmt.Errorf("dragonfly: auth secret %s of pod %s missing key %q", ref.Name, pod.Name, ref.Key)
				}
				password = string(raw)
			case env.ValueFrom == nil:
				password = env.Value
			}
		}
	}
	addr := fmt.Sprintf("%s.%s.svc.cluster.local:%d", dragonflySiteServiceName(fg.Name, siteName), fg.Namespace, port)
	return connector(ctx, addr, password)
}

// effectiveDragonflyMasterSite returns the site name whose Dragonfly pod
// should currently carry dragonfly-role=master.
//
// Normally that's status.activeSite. Once a planned-failover run's
// Dragonfly takeover has succeeded, the target already holds the master
// while the run is in PromotingDragonfly, Promoting, Publishing or
// RollingBack and no later promotion was stamped
// (plannedTakeoverMasterSite), so we return the target instead. A
// Dragonfly-only emergency promotion can also intentionally diverge from
// MySQL; in that case status.dragonfly.activeSite is authoritative for
// cache routing until a later MySQL/Dragonfly promotion moves it again.
// Without these branches syncDragonflyPodLabels would re-apply the label
// to the stale source and fight the promotion path.
func effectiveDragonflyMasterSite(fg *v1alpha1.MysqlFailoverGroup) string {
	if site := plannedTakeoverMasterSite(fg); site != "" {
		return site
	}
	if fg.Status.Dragonfly != nil && fg.Status.Dragonfly.ActiveSite != "" {
		return fg.Status.Dragonfly.ActiveSite
	}
	return fg.Status.ActiveSite
}
