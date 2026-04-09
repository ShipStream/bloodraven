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

	labelAppName   = "app.kubernetes.io/name"
	labelInstance  = "app.kubernetes.io/instance"
	labelDC        = "shipstream.io/dc"
	labelRole      = "shipstream.io/role"
	labelHealthy   = "shipstream.io/healthy"
	labelManagedBy = "app.kubernetes.io/managed-by"
	managerName    = "bloodraven"

	specHashAnnotation = "shipstream.io/spec-hash"

	mysqlPort   = 3306
	sidecarPort = 8080
)

// MysqlReplicaPairReconciler reconciles a MysqlReplicaPair object.
type MysqlReplicaPairReconciler struct {
	client.Client
	Scheme   *runtime.Scheme
	Recorder record.EventRecorder
	Runner   *TopologyManagerRunner
	Tainter  platform.NodeTainter // optional, for taint cleanup during deletion
}

// +kubebuilder:rbac:groups=shipstream.io,resources=mysqlreplicapairs,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=shipstream.io,resources=mysqlreplicapairs/status,verbs=get;update;patch
// +kubebuilder:rbac:groups=shipstream.io,resources=mysqlreplicapairs/finalizers,verbs=update
// +kubebuilder:rbac:groups="",resources=configmaps;services;persistentvolumeclaims,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=apps,resources=deployments,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups="",resources=pods,verbs=get;list;watch;update;patch
// +kubebuilder:rbac:groups="",resources=nodes,verbs=get;list;watch;update;patch
// +kubebuilder:rbac:groups="",resources=secrets,verbs=get;list;watch
// +kubebuilder:rbac:groups="",resources=events,verbs=create;patch

func (r *MysqlReplicaPairReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	logger := log.FromContext(ctx)

	var pair v1alpha1.MysqlReplicaPair
	if err := r.Get(ctx, req.NamespacedName, &pair); err != nil {
		if errors.IsNotFound(err) {
			return ctrl.Result{}, nil
		}
		return ctrl.Result{}, err
	}

	// Handle deletion with finalizer
	if !pair.DeletionTimestamp.IsZero() {
		if controllerutil.ContainsFinalizer(&pair, finalizerName) {
			logger.Info("CR being deleted, running finalizer cleanup", "name", pair.Name)

			// Stop the topology manager for this CR.
			if r.Runner != nil {
				r.Runner.StopManager(req.NamespacedName)
			}

			if err := r.handleDeletion(ctx, &pair); err != nil {
				return ctrl.Result{}, fmt.Errorf("handle deletion: %w", err)
			}
			controllerutil.RemoveFinalizer(&pair, finalizerName)
			if err := r.Update(ctx, &pair); err != nil {
				return ctrl.Result{}, fmt.Errorf("remove finalizer: %w", err)
			}
		}
		return ctrl.Result{}, nil
	}

	// Ensure finalizer is present.
	if !controllerutil.ContainsFinalizer(&pair, finalizerName) {
		controllerutil.AddFinalizer(&pair, finalizerName)
		if err := r.Update(ctx, &pair); err != nil {
			return ctrl.Result{}, fmt.Errorf("add finalizer: %w", err)
		}
	}

	logger.Info("reconciling MysqlReplicaPair", "name", pair.Name)

	image := pair.Spec.Image
	if image == "" {
		image = defaultMySQLImage
	}

	// Validate that sidecarImage is explicitly set. Falling back to the MySQL
	// image is almost always wrong in production since the sidecar binary is
	// a separate build target (bloodraven-sidecar).
	if pair.Spec.SidecarImage == "" {
		r.Recorder.Eventf(&pair, corev1.EventTypeWarning, "MissingSidecarImage",
			"spec.sidecarImage is not set; falling back to %q which is likely incorrect", image)
		logger.Info("WARNING: sidecarImage not set, falling back to MySQL image", "image", image)
	}

	// Validate that the referenced secret contains the expected 'dsn' key.
	var secret corev1.Secret
	secretKey := types.NamespacedName{Namespace: pair.Namespace, Name: pair.Spec.SecretName}
	if err := r.Get(ctx, secretKey, &secret); err != nil {
		if !errors.IsNotFound(err) {
			return ctrl.Result{}, fmt.Errorf("get secret %s: %w", pair.Spec.SecretName, err)
		}
		r.Recorder.Eventf(&pair, corev1.EventTypeWarning, "SecretNotFound",
			"secret %q not found", pair.Spec.SecretName)
		return ctrl.Result{RequeueAfter: 30 * time.Second}, nil
	}
	if _, ok := secret.Data["dsn"]; !ok {
		r.Recorder.Eventf(&pair, corev1.EventTypeWarning, "SecretMissingKey",
			"secret %q does not contain required key 'dsn'", pair.Spec.SecretName)
		return ctrl.Result{RequeueAfter: 30 * time.Second}, nil
	}

	// Reconcile ConfigMap
	if err := r.reconcileConfigMap(ctx, &pair); err != nil {
		return ctrl.Result{}, fmt.Errorf("reconcile configmap: %w", err)
	}

	// Reconcile per-DC resources
	for _, dc := range []struct {
		spec     v1alpha1.DCInstanceSpec
		serverID int32
		peerSpec v1alpha1.DCInstanceSpec
	}{
		{pair.Spec.DC1, 101, pair.Spec.DC2},
		{pair.Spec.DC2, 102, pair.Spec.DC1},
	} {
		if err := r.reconcilePVC(ctx, &pair, dc.spec); err != nil {
			return ctrl.Result{}, fmt.Errorf("reconcile pvc %s: %w", dc.spec.Name, err)
		}
		if err := r.reconcileDeployment(ctx, &pair, dc.spec, dc.peerSpec, dc.serverID, image); err != nil {
			return ctrl.Result{}, fmt.Errorf("reconcile deployment %s: %w", dc.spec.Name, err)
		}
		if err := r.reconcileDCService(ctx, &pair, dc.spec); err != nil {
			return ctrl.Result{}, fmt.Errorf("reconcile dc service %s: %w", dc.spec.Name, err)
		}
	}

	// Reconcile shared services
	if err := r.reconcilePrimaryService(ctx, &pair); err != nil {
		return ctrl.Result{}, fmt.Errorf("reconcile primary service: %w", err)
	}
	if err := r.reconcileReplicasService(ctx, &pair); err != nil {
		return ctrl.Result{}, fmt.Errorf("reconcile replicas service: %w", err)
	}

	// Sync pod labels based on status
	if err := r.syncPodLabels(ctx, &pair); err != nil {
		return ctrl.Result{}, fmt.Errorf("sync pod labels: %w", err)
	}

	return ctrl.Result{}, nil
}

func (r *MysqlReplicaPairReconciler) SetupWithManager(mgr ctrl.Manager) error {
	return ctrl.NewControllerManagedBy(mgr).
		For(&v1alpha1.MysqlReplicaPair{}).
		Owns(&appsv1.Deployment{}).
		Owns(&corev1.Service{}).
		Owns(&corev1.ConfigMap{}).
		Owns(&corev1.PersistentVolumeClaim{}).
		Complete(r)
}

func (r *MysqlReplicaPairReconciler) handleDeletion(ctx context.Context, pair *v1alpha1.MysqlReplicaPair) error {
	logger := log.FromContext(ctx)
	logger.Info("starting graceful shutdown", "pair", pair.Name)

	// Record event
	r.Recorder.Event(pair, corev1.EventTypeNormal, "GracefulShutdown", "Starting graceful shutdown sequence")

	// Remove taints for both DC zones
	if r.Tainter != nil {
		for _, zone := range []string{pair.Spec.DC1.Zone, pair.Spec.DC2.Zone} {
			if err := r.Tainter.SetTaint(ctx, zone, false); err != nil {
				logger.Error(err, "failed to remove taint during shutdown", "zone", zone)
				// Continue cleanup despite taint removal failure
			}
		}
	}

	// Log DNS cleanup warning (full cleanup would require knowing the current DNS state)
	logger.Info("CR deleted — DNS records may need manual cleanup",
		"az", pair.Spec.AZ,
		"dc1IP", pair.Spec.DC1.LBIP,
		"dc2IP", pair.Spec.DC2.LBIP)
	r.Recorder.Event(pair, corev1.EventTypeWarning, "DNSCleanup",
		"DNS records may need manual cleanup after CR deletion")

	r.Recorder.Event(pair, corev1.EventTypeNormal, "GracefulShutdown", "Graceful shutdown complete, removing finalizer")
	return nil
}

// resourceName returns a deterministic name for a per-DC resource.
func resourceName(pairName, dcName string) string {
	return fmt.Sprintf("mysql-%s-%s", pairName, dcName)
}

// commonLabels returns the labels applied to all resources for a pair/dc.
func commonLabels(pairName, dcName string) map[string]string {
	return map[string]string{
		labelAppName:   "mysql",
		labelInstance:  pairName,
		labelDC:        dcName,
		labelManagedBy: managerName,
	}
}

func (r *MysqlReplicaPairReconciler) reconcileConfigMap(ctx context.Context, pair *v1alpha1.MysqlReplicaPair) error {
	cm := &corev1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{
			Name:      fmt.Sprintf("mysql-%s-config", pair.Name),
			Namespace: pair.Namespace,
		},
	}

	_, err := controllerutil.CreateOrUpdate(ctx, r.Client, cm, func() error {
		if err := controllerutil.SetControllerReference(pair, cm, r.Scheme); err != nil {
			return err
		}
		cm.Labels = map[string]string{
			labelAppName:   "mysql",
			labelInstance:  pair.Name,
			labelManagedBy: managerName,
		}
		cm.Data = map[string]string{
			"my.cnf": generateMyCnf(pair),
		}
		return nil
	})
	return err
}

func generateMyCnf(pair *v1alpha1.MysqlReplicaPair) string {
	// Base config
	settings := map[string]string{
		"gtid-mode":                      "ON",
		"enforce-gtid-consistency":       "ON",
		"log-bin":                        "/var/lib/mysql/mysql-bin",
		"log-replica-updates":            "ON",
		"skip-replica-start":             "ON",
		"binlog-format":                  "ROW",
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
		"skip-host-cache":                "",
		"max-connections":                "500",
		"thread-cache-size":              "50",
		"character-set-server":           "utf8mb4",
		"collation-server":               "utf8mb4_unicode_ci",
	}

	// Apply clone_ddl_timeout from spec (default 3600s)
	cloneTimeout := 3600
	if pair.Spec.CloneTimeout > 0 {
		cloneTimeout = pair.Spec.CloneTimeout
	}
	settings["clone_ddl_timeout"] = fmt.Sprintf("%d", cloneTimeout)

	// Apply TLS settings if configured
	if pair.Spec.TLS != nil {
		settings["ssl-ca"] = "/etc/mysql/tls/ca.crt"
		settings["ssl-cert"] = "/etc/mysql/tls/tls.crt"
		settings["ssl-key"] = "/etc/mysql/tls/tls.key"
		settings["require-secure-transport"] = "ON"
	}

	// Apply user overrides
	for k, v := range pair.Spec.MysqlConf {
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

func (r *MysqlReplicaPairReconciler) reconcilePVC(ctx context.Context, pair *v1alpha1.MysqlReplicaPair, dc v1alpha1.DCInstanceSpec) error {
	pvc := &corev1.PersistentVolumeClaim{
		ObjectMeta: metav1.ObjectMeta{
			Name:      resourceName(pair.Name, dc.Name) + "-data",
			Namespace: pair.Namespace,
		},
	}

	_, err := controllerutil.CreateOrUpdate(ctx, r.Client, pvc, func() error {
		if err := controllerutil.SetControllerReference(pair, pvc, r.Scheme); err != nil {
			return err
		}
		pvc.Labels = commonLabels(pair.Name, dc.Name)

		// Only set spec fields on creation (PVC spec is largely immutable)
		if pvc.CreationTimestamp.IsZero() {
			pvc.Spec = corev1.PersistentVolumeClaimSpec{
				AccessModes:      []corev1.PersistentVolumeAccessMode{corev1.ReadWriteOnce},
				StorageClassName: &dc.Storage.StorageClassName,
				Resources: corev1.VolumeResourceRequirements{
					Requests: corev1.ResourceList{
						corev1.ResourceStorage: dc.Storage.Size,
					},
				},
			}
		}
		return nil
	})
	return err
}

func (r *MysqlReplicaPairReconciler) reconcileDeployment(ctx context.Context, pair *v1alpha1.MysqlReplicaPair, dc, peerDC v1alpha1.DCInstanceSpec, serverID int32, image string) error {
	deploy := &appsv1.Deployment{
		ObjectMeta: metav1.ObjectMeta{
			Name:      resourceName(pair.Name, dc.Name),
			Namespace: pair.Namespace,
		},
	}

	specHash := computeSpecHash(pair, dc)

	_, err := controllerutil.CreateOrUpdate(ctx, r.Client, deploy, func() error {
		if err := controllerutil.SetControllerReference(pair, deploy, r.Scheme); err != nil {
			return err
		}

		labels := commonLabels(pair.Name, dc.Name)
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
				labelInstance: pair.Name,
				labelDC:       dc.Name,
			},
		}

		podLabels := make(map[string]string)
		for k, v := range labels {
			podLabels[k] = v
		}

		sidecarImage := pair.Spec.SidecarImage
		if sidecarImage == "" {
			sidecarImage = image
		}

		configMapName := fmt.Sprintf("mysql-%s-config", pair.Name)

		peerAddress := fmt.Sprintf("mysql-%s-%s.%s.svc.cluster.local:%d",
			pair.Name, peerDC.Name, pair.Namespace, sidecarPort)

		bloodravenAddress := pair.Spec.Sidecar.BloodravenAddress
		if bloodravenAddress == "" {
			bloodravenAddress = fmt.Sprintf("bloodraven.%s.svc.cluster.local:8082", pair.Namespace)
		}

		leaseTimeout := "20s"
		if pair.Spec.Sidecar.LeaseTimeout != nil {
			leaseTimeout = pair.Spec.Sidecar.LeaseTimeout.Duration.String()
		}

		peerCheckInterval := "5s"
		if pair.Spec.Sidecar.PeerCheckInterval != nil {
			peerCheckInterval = pair.Spec.Sidecar.PeerCheckInterval.Duration.String()
		}

		volumeMounts := []corev1.VolumeMount{
			{Name: "data", MountPath: "/var/lib/mysql"},
			{Name: "conf", MountPath: "/etc/mysql/conf.d"},
		}
		if pair.Spec.TLS != nil {
			volumeMounts = append(volumeMounts, corev1.VolumeMount{
				Name:      "tls",
				MountPath: "/etc/mysql/tls",
				ReadOnly:  true,
			})
		}

		sidecarVolumeMounts := []corev1.VolumeMount{}
		if pair.Spec.TLS != nil {
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
								Name: pair.Spec.SecretName,
							},
						},
					},
				},
				VolumeMounts: volumeMounts,
				Resources:    dc.Resources,
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
							LocalObjectReference: corev1.LocalObjectReference{Name: pair.Spec.SecretName},
							Key:                  "dsn",
						},
					}},
					{Name: "MY_POD_NAME", ValueFrom: &corev1.EnvVarSource{
						FieldRef: &corev1.ObjectFieldSelector{FieldPath: "metadata.name"},
					}},
					{Name: "LISTEN_ADDR", Value: fmt.Sprintf(":%d", sidecarPort)},
					{Name: "PEER_ADDRESS", Value: peerAddress},
					{Name: "BLOODRAVEN_ADDRESS", Value: bloodravenAddress},
					{Name: "MY_DC", Value: dc.Name},
					{Name: "PRIMARY_DC", Value: pair.Status.PrimaryDC},
					{Name: "LEASE_TIMEOUT", Value: leaseTimeout},
					{Name: "PEER_CHECK_INTERVAL", Value: peerCheckInterval},
				},
				VolumeMounts: sidecarVolumeMounts,
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
						ClaimName: resourceName(pair.Name, dc.Name) + "-data",
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

		if pair.Spec.TLS != nil {
			volumes = append(volumes, corev1.Volume{
				Name: "tls",
				VolumeSource: corev1.VolumeSource{
					Secret: &corev1.SecretVolumeSource{
						SecretName: pair.Spec.TLS.SecretName,
					},
				},
			})
		}

		deploy.Spec.Template = corev1.PodTemplateSpec{
			ObjectMeta: metav1.ObjectMeta{
				Labels: podLabels,
				Annotations: map[string]string{
					specHashAnnotation: specHash,
				},
			},
			Spec: corev1.PodSpec{
				NodeSelector: map[string]string{
					"topology.kubernetes.io/zone": dc.Zone,
				},
				InitContainers: []corev1.Container{
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
				},
				Containers: containers,
				Volumes:    volumes,
			},
		}
		if pair.Spec.TerminationGracePeriodSeconds != nil {
			deploy.Spec.Template.Spec.TerminationGracePeriodSeconds = pair.Spec.TerminationGracePeriodSeconds
		}
		return nil
	})
	return err
}

func (r *MysqlReplicaPairReconciler) reconcileDCService(ctx context.Context, pair *v1alpha1.MysqlReplicaPair, dc v1alpha1.DCInstanceSpec) error {
	svc := &corev1.Service{
		ObjectMeta: metav1.ObjectMeta{
			Name:      resourceName(pair.Name, dc.Name),
			Namespace: pair.Namespace,
		},
	}

	_, err := controllerutil.CreateOrUpdate(ctx, r.Client, svc, func() error {
		if err := controllerutil.SetControllerReference(pair, svc, r.Scheme); err != nil {
			return err
		}
		svc.Labels = commonLabels(pair.Name, dc.Name)
		svc.Spec = corev1.ServiceSpec{
			Type: corev1.ServiceTypeClusterIP,
			Selector: map[string]string{
				labelAppName:  "mysql",
				labelInstance: pair.Name,
				labelDC:       dc.Name,
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

func (r *MysqlReplicaPairReconciler) reconcilePrimaryService(ctx context.Context, pair *v1alpha1.MysqlReplicaPair) error {
	svc := &corev1.Service{
		ObjectMeta: metav1.ObjectMeta{
			Name:      fmt.Sprintf("mysql-%s-primary", pair.Name),
			Namespace: pair.Namespace,
		},
	}

	_, err := controllerutil.CreateOrUpdate(ctx, r.Client, svc, func() error {
		if err := controllerutil.SetControllerReference(pair, svc, r.Scheme); err != nil {
			return err
		}
		svc.Labels = map[string]string{
			labelAppName:   "mysql",
			labelInstance:  pair.Name,
			labelManagedBy: managerName,
		}
		svc.Spec = corev1.ServiceSpec{
			Type: corev1.ServiceTypeClusterIP,
			Selector: map[string]string{
				labelInstance: pair.Name,
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

func (r *MysqlReplicaPairReconciler) reconcileReplicasService(ctx context.Context, pair *v1alpha1.MysqlReplicaPair) error {
	svc := &corev1.Service{
		ObjectMeta: metav1.ObjectMeta{
			Name:      fmt.Sprintf("mysql-%s-replicas", pair.Name),
			Namespace: pair.Namespace,
		},
	}

	_, err := controllerutil.CreateOrUpdate(ctx, r.Client, svc, func() error {
		if err := controllerutil.SetControllerReference(pair, svc, r.Scheme); err != nil {
			return err
		}
		svc.Labels = map[string]string{
			labelAppName:   "mysql",
			labelInstance:  pair.Name,
			labelManagedBy: managerName,
		}
		svc.Spec = corev1.ServiceSpec{
			Type: corev1.ServiceTypeClusterIP,
			Selector: map[string]string{
				labelInstance: pair.Name,
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

// syncPodLabels updates pod labels based on the CR status.
// It updates replicas first, then primary, to prevent dual-primary in Service endpoints.
func (r *MysqlReplicaPairReconciler) syncPodLabels(ctx context.Context, pair *v1alpha1.MysqlReplicaPair) error {
	logger := log.FromContext(ctx)

	if pair.Status.PrimaryDC == "" {
		return nil
	}

	// Determine which DC is primary, which is replica
	dcs := []struct {
		spec   v1alpha1.DCInstanceSpec
		status v1alpha1.DCInstanceStatus
		role   string
	}{
		{pair.Spec.DC1, pair.Status.DC1, "replica"},
		{pair.Spec.DC2, pair.Status.DC2, "replica"},
	}

	if pair.Status.PrimaryDC == pair.Spec.DC1.Name {
		dcs[0].role = "primary"
	} else if pair.Status.PrimaryDC == pair.Spec.DC2.Name {
		dcs[1].role = "primary"
	}

	// Sort: replicas first, then primary
	sort.Slice(dcs, func(i, j int) bool {
		if dcs[i].role == "replica" && dcs[j].role == "primary" {
			return true
		}
		return false
	})

	for _, dc := range dcs {
		pods := &corev1.PodList{}
		if err := r.List(ctx, pods,
			client.InNamespace(pair.Namespace),
			client.MatchingLabels{
				labelAppName:  "mysql",
				labelInstance: pair.Name,
				labelDC:       dc.spec.Name,
			},
		); err != nil {
			return fmt.Errorf("list pods for dc %s: %w", dc.spec.Name, err)
		}

		healthy := "no"
		if dc.status.State == "writable" || dc.status.State == "read-only" {
			healthy = "yes"
		}

		for i := range pods.Items {
			pod := &pods.Items[i]
			needsUpdate := false

			if pod.Labels[labelRole] != dc.role {
				if pod.Labels == nil {
					pod.Labels = make(map[string]string)
				}
				pod.Labels[labelRole] = dc.role
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
				logger.Info("updating pod labels", "pod", pod.Name, "role", dc.role, "healthy", healthy)
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
					fresh.Labels[labelRole] = dc.role
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
func computeSpecHash(pair *v1alpha1.MysqlReplicaPair, dc v1alpha1.DCInstanceSpec) string {
	h := sha256.New()
	fmt.Fprintf(h, "image=%s\n", pair.Spec.Image)
	fmt.Fprintf(h, "sidecar=%s\n", pair.Spec.SidecarImage)
	fmt.Fprintf(h, "resources=%v\n", dc.Resources)
	// Sort mysqlConf keys for deterministic hash
	keys := make([]string, 0, len(pair.Spec.MysqlConf))
	for k := range pair.Spec.MysqlConf {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	for _, k := range keys {
		fmt.Fprintf(h, "mysql.%s=%s\n", k, pair.Spec.MysqlConf[k])
	}
	return hex.EncodeToString(h.Sum(nil))[:16]
}

// CRConfigToTopologyConfig extracts topology manager configuration from a CR.
// This bridges the CRD spec to the internal config used by TopologyManager.
func CRConfigToTopologyConfig(pair *v1alpha1.MysqlReplicaPair) TopologyConfig {
	pollInterval := int64(2 * time.Second)
	if pair.Spec.PollInterval != nil {
		pollInterval = int64(pair.Spec.PollInterval.Duration)
	}

	failureThreshold := int32(3)
	if pair.Spec.FailureThreshold > 0 {
		failureThreshold = pair.Spec.FailureThreshold
	}

	recoveryThreshold := int32(2)
	if pair.Spec.RecoveryThreshold > 0 {
		recoveryThreshold = pair.Spec.RecoveryThreshold
	}

	var failoverCooldown int64
	if pair.Spec.FailoverCooldown != nil {
		failoverCooldown = int64(pair.Spec.FailoverCooldown.Duration)
	}

	return TopologyConfig{
		AZ: pair.Spec.AZ,
		DC1: DCTopologyConfig{
			Name: pair.Spec.DC1.Name,
			Zone: pair.Spec.DC1.Zone,
			LBIP: pair.Spec.DC1.LBIP,
		},
		DC2: DCTopologyConfig{
			Name: pair.Spec.DC2.Name,
			Zone: pair.Spec.DC2.Zone,
			LBIP: pair.Spec.DC2.LBIP,
		},
		PollInterval:      pollInterval,
		FailureThreshold:  int(failureThreshold),
		RecoveryThreshold: int(recoveryThreshold),
		FailoverCooldown:  failoverCooldown,
	}
}

// StatusFromTopology creates a NamespacedName from a pair for use with the topology manager.
func PairNamespacedName(pair *v1alpha1.MysqlReplicaPair) types.NamespacedName {
	return types.NamespacedName{
		Namespace: pair.Namespace,
		Name:      pair.Name,
	}
}
