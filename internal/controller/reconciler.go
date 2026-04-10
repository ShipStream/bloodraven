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
	corev1 "k8s.io/api/core/v1"
	policyv1 "k8s.io/api/policy/v1"
	"k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/intstr"
	k8sretry "k8s.io/client-go/util/retry"
	"k8s.io/client-go/tools/record"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
	"sigs.k8s.io/controller-runtime/pkg/log"

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

	// Validate that sidecarImage is explicitly set. Falling back to the MySQL
	// image is almost always wrong in production since the sidecar binary is
	// a separate build target (bloodraven-sidecar).
	if fg.Spec.SidecarImage == "" {
		r.Recorder.Eventf(&fg, corev1.EventTypeWarning, "MissingSidecarImage",
			"spec.sidecarImage is not set; falling back to %q which is likely incorrect", image)
		logger.Info("WARNING: sidecarImage not set, falling back to MySQL image", "image", image)
	}

	// Validate that the referenced secret contains the expected 'dsn' key.
	var secret corev1.Secret
	secretKey := types.NamespacedName{Namespace: fg.Namespace, Name: fg.Spec.SecretName}
	if err := r.Get(ctx, secretKey, &secret); err != nil {
		if !errors.IsNotFound(err) {
			return ctrl.Result{}, fmt.Errorf("get secret %s: %w", fg.Spec.SecretName, err)
		}
		r.Recorder.Eventf(&fg, corev1.EventTypeWarning, "SecretNotFound",
			"secret %q not found", fg.Spec.SecretName)
		return ctrl.Result{RequeueAfter: 30 * time.Second}, nil
	}
	if _, ok := secret.Data["dsn"]; !ok {
		r.Recorder.Eventf(&fg, corev1.EventTypeWarning, "SecretMissingKey",
			"secret %q does not contain required key 'dsn'", fg.Spec.SecretName)
		return ctrl.Result{RequeueAfter: 30 * time.Second}, nil
	}

	// Reconcile ConfigMap
	if err := r.reconcileConfigMap(ctx, &fg); err != nil {
		return ctrl.Result{}, fmt.Errorf("reconcile configmap: %w", err)
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

	// Sync pod labels based on status
	if err := r.syncPodLabels(ctx, &fg); err != nil {
		return ctrl.Result{}, fmt.Errorf("sync pod labels: %w", err)
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
		Complete(r)
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

	specHash := computeSpecHash(fg, site)

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

		sidecarImage := fg.Spec.SidecarImage
		if sidecarImage == "" {
			sidecarImage = image
		}

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

		containers := []corev1.Container{
			{
				Name:  "mysql",
				Image: image,
				Ports: []corev1.ContainerPort{
					{
						Name:          "mysql",
						ContainerPort: mysqlPort,
						Protocol:      corev1.ProtocolTCP,
					},
				},
				EnvFrom: []corev1.EnvFromSource{
					{
						SecretRef: &corev1.SecretEnvSource{
							LocalObjectReference: corev1.LocalObjectReference{
								Name: fg.Spec.SecretName,
							},
						},
					},
				},
				VolumeMounts: volumeMounts,
				Resources: site.Resources,
				Lifecycle: &corev1.Lifecycle{
					PreStop: &corev1.LifecycleHandler{
						Exec: &corev1.ExecAction{
							Command: []string{
								"sh", "-c",
								`mysql -u root -p"${MYSQL_ROOT_PASSWORD}" -e 'SET GLOBAL super_read_only=ON' 2>/dev/null || true`,
							},
						},
					},
				},
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
			},
			{
				Name:  "sidecar",
				Image: sidecarImage,
				Ports: []corev1.ContainerPort{
					{
						Name:          "sidecar",
						ContainerPort: sidecarPort,
						Protocol:      corev1.ProtocolTCP,
					},
				},
				Env: []corev1.EnvVar{
					{Name: "MYSQL_DSN", ValueFrom: &corev1.EnvVarSource{
						SecretKeyRef: &corev1.SecretKeySelector{
							LocalObjectReference: corev1.LocalObjectReference{Name: fg.Spec.SecretName},
							Key:                  "dsn",
						},
					}},
					{Name: "MY_POD_NAME", ValueFrom: &corev1.EnvVarSource{
						FieldRef: &corev1.ObjectFieldSelector{FieldPath: "metadata.name"},
					}},
					{Name: "LISTEN_ADDR", Value: fmt.Sprintf(":%d", sidecarPort)},
					{Name: "PEER_ADDRESS", Value: peerAddress},
					{Name: "BLOODRAVEN_ADDRESS", Value: bloodravenAddress},
					{Name: "MY_SITE", Value: site.Name},
					// ACTIVE_SITE is intentionally omitted from the deployment spec
					// to avoid triggering rollouts when the active site changes.
					// The sidecar's safety net will be skipped (gracefully) on startup.
					{Name: "LEASE_TIMEOUT", Value: leaseTimeout},
					{Name: "PEER_CHECK_INTERVAL", Value: peerCheckInterval},
				},
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
			},
		}

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
							fmt.Sprintf("cp /etc/mysql/config-map/* /etc/mysql/conf.d/ && echo '[mysqld]\\nserver-id=%d' > /etc/mysql/conf.d/server-id.cnf", serverID),
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
func computeSpecHash(fg *v1alpha1.MysqlFailoverGroup, site v1alpha1.SiteSpec) string {
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
	return hex.EncodeToString(h.Sum(nil))[:16]
}

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
		Name: fg.Name,
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
