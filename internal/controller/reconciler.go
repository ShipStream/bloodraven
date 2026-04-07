package controller

import (
	"context"
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
	"k8s.io/client-go/tools/record"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
	"sigs.k8s.io/controller-runtime/pkg/log"

	v1alpha1 "github.com/shipstream/bloodraven/api/v1alpha1"
)

const (
	defaultMySQLImage = "mysql:9.6"

	labelAppName    = "app.kubernetes.io/name"
	labelInstance   = "app.kubernetes.io/instance"
	labelDC         = "shipstream.io/dc"
	labelRole       = "shipstream.io/role"
	labelHealthy    = "shipstream.io/healthy"
	labelManagedBy  = "app.kubernetes.io/managed-by"
	managerName     = "bloodraven"

	mysqlPort = 3306
)

// MysqlReplicaPairReconciler reconciles a MysqlReplicaPair object.
type MysqlReplicaPairReconciler struct {
	client.Client
	Scheme   *runtime.Scheme
	Recorder record.EventRecorder
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

	logger.Info("reconciling MysqlReplicaPair", "name", pair.Name)

	image := pair.Spec.Image
	if image == "" {
		image = defaultMySQLImage
	}

	// Reconcile ConfigMap
	if err := r.reconcileConfigMap(ctx, &pair); err != nil {
		return ctrl.Result{}, fmt.Errorf("reconcile configmap: %w", err)
	}

	// Reconcile per-DC resources
	for _, dc := range []struct {
		spec     v1alpha1.DCInstanceSpec
		serverID int32
	}{
		{pair.Spec.DC1, 101},
		{pair.Spec.DC2, 102},
	} {
		if err := r.reconcilePVC(ctx, &pair, dc.spec); err != nil {
			return ctrl.Result{}, fmt.Errorf("reconcile pvc %s: %w", dc.spec.Name, err)
		}
		if err := r.reconcileDeployment(ctx, &pair, dc.spec, dc.serverID, image); err != nil {
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
		"gtid-mode":                   "ON",
		"enforce-gtid-consistency":    "ON",
		"log-bin":                     "/var/lib/mysql/mysql-bin",
		"log-replica-updates":         "ON",
		"skip-replica-start":          "ON",
		"binlog-format":               "ROW",
		"sync-binlog":                 "1",
		"binlog-expire-logs-seconds":  "1209600",
		"plugin-load-add":             "mysql_clone.so",
		"default-storage-engine":      "InnoDB",
		"innodb-flush-method":         "O_DIRECT",
		"innodb-flush-log-at-trx-commit": "2",
		"innodb-file-per-table":       "1",
		"max-allowed-packet":          "64M",
		"max-connect-errors":          "1000000",
		"skip-name-resolve":           "",
		"skip-host-cache":             "",
		"max-connections":             "500",
		"thread-cache-size":           "50",
		"character-set-server":        "utf8mb4",
		"collation-server":            "utf8mb4_unicode_ci",
	}

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

func (r *MysqlReplicaPairReconciler) reconcileDeployment(ctx context.Context, pair *v1alpha1.MysqlReplicaPair, dc v1alpha1.DCInstanceSpec, serverID int32, image string) error {
	deploy := &appsv1.Deployment{
		ObjectMeta: metav1.ObjectMeta{
			Name:      resourceName(pair.Name, dc.Name),
			Namespace: pair.Namespace,
		},
	}

	_, err := controllerutil.CreateOrUpdate(ctx, r.Client, deploy, func() error {
		if err := controllerutil.SetControllerReference(pair, deploy, r.Scheme); err != nil {
			return err
		}

		labels := commonLabels(pair.Name, dc.Name)
		deploy.Labels = labels

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

		deploy.Spec.Template = corev1.PodTemplateSpec{
			ObjectMeta: metav1.ObjectMeta{
				Labels: podLabels,
			},
			Spec: corev1.PodSpec{
				NodeSelector: map[string]string{
					"topology.kubernetes.io/zone": dc.Zone,
				},
				InitContainers: []corev1.Container{
					{
						Name:  "init",
						Image: sidecarImage,
						Command: []string{
							"sh", "-c",
							fmt.Sprintf("echo '[mysqld]\nserver-id=%d' > /etc/mysql/conf.d/server-id.cnf", serverID),
						},
						VolumeMounts: []corev1.VolumeMount{
							{
								Name:      "conf",
								MountPath: "/etc/mysql/conf.d",
							},
						},
					},
				},
				Containers: []corev1.Container{
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
						VolumeMounts: []corev1.VolumeMount{
							{
								Name:      "data",
								MountPath: "/var/lib/mysql",
							},
							{
								Name:      "config",
								MountPath: "/etc/mysql/conf.d",
								ReadOnly:  true,
							},
						},
						Resources: dc.Resources,
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
				},
				Volumes: []corev1.Volume{
					{
						Name: "data",
						VolumeSource: corev1.VolumeSource{
							PersistentVolumeClaim: &corev1.PersistentVolumeClaimVolumeSource{
								ClaimName: resourceName(pair.Name, dc.Name) + "-data",
							},
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
				},
			},
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
				if err := r.Update(ctx, pod); err != nil {
					return fmt.Errorf("update pod %s labels: %w", pod.Name, err)
				}
			}
		}
	}

	return nil
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
