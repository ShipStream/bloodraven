package main

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"time"

	_ "github.com/go-sql-driver/mysql"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	apiresource "k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/yaml"

	v1alpha1 "github.com/shipstream/bloodraven/api/v1alpha1"
	pgkube "github.com/shipstream/bloodraven/internal/playground/kube"
	pgmysql "github.com/shipstream/bloodraven/internal/playground/mysql"
	"github.com/shipstream/bloodraven/internal/playground/scenarios"
)

const (
	operatorDeployment = "bloodraven"
	mysqlSelector      = "app.kubernetes.io/name=mysql"
	operatorSelector   = "app.kubernetes.io/name=bloodraven"
	readonlyTaintKey   = "shipstream.io/db-readonly-playground"
	legacyTaintKey     = "shipstream.io/db-readonly"
)

type resetter struct {
	k          *pgkube.Client
	namespace  string
	fg         string
	resultsDir string
	logger     *slog.Logger
	repoRoot   string
	runtime    string
}

func resetPlayground(kubeconfig, kctx, namespace, fg, resultsDir string, logger *slog.Logger) int {
	k, err := loadKube(kubeconfig, kctx, false)
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		if isGuardErr(err) {
			return exitGuard
		}
		return exitEnvironment
	}
	ctx, stop := context.WithTimeout(context.Background(), 15*time.Minute)
	defer stop()

	repoRoot, err := os.Getwd()
	if err != nil {
		fmt.Fprintln(os.Stderr, "reset: get cwd:", err)
		return exitEnvironment
	}
	r := &resetter{
		k:          k,
		namespace:  namespace,
		fg:         fg,
		resultsDir: resultsDir,
		logger:     logger,
		repoRoot:   repoRoot,
		runtime:    containerRuntime(),
	}
	if err := r.run(ctx); err != nil {
		fmt.Fprintln(os.Stderr, "reset:", err)
		if capErr := r.persistFailure(context.Background(), err); capErr != nil {
			fmt.Fprintln(os.Stderr, "reset: forensic capture failed:", capErr)
		}
		return exitFailure
	}
	return exitOK
}

func (r *resetter) run(ctx context.Context) error {
	r.info("context %s", r.k.CurrentCtx)
	mfg, err := r.k.GetMFG(ctx, r.namespace)
	if err != nil {
		return fmt.Errorf("get MFG: %w", err)
	}
	sites := mfg.Spec.Sites
	if len(sites) == 0 {
		return fmt.Errorf("MFG has no spec.sites")
	}

	phases := []struct {
		name string
		fn   func(context.Context, []v1alpha1.SiteSpec) error
	}{
		{"scale operator down", r.scaleOperatorDown},
		{"scale MySQL down", r.scaleMysqlDown},
		{"clear chaos marker and taints", r.clearTransientState},
		{"delete PVCs and PVs", r.deleteStorage},
		{"wipe k3d node storage", r.wipeNodeStorage},
		{"restart local-path-provisioner", r.restartLocalPathProvisioner},
		{"reapply mysql secret", r.reapplyMysqlSecret},
		{"clear MFG status", r.clearMFGStatus},
		{"recreate PVCs", r.recreatePVCs},
		{"bind deterministic PVs", r.bindPVs},
		{"start MySQL", r.startMySQL},
		{"create replication users", r.createReplicationUsers},
		{"start operator", r.startOperator},
		{"wait healthy baseline", r.waitBaseline},
	}
	for _, phase := range phases {
		r.info("%s...", phase.name)
		if err := phase.fn(ctx, sites); err != nil {
			return fmt.Errorf("%s: %w", phase.name, err)
		}
	}
	r.info("reset complete")
	return nil
}

func (r *resetter) scaleOperatorDown(ctx context.Context, _ []v1alpha1.SiteSpec) error {
	if err := r.scaleDeploymentIfExists(ctx, operatorDeployment, 0); err != nil {
		return err
	}
	return r.waitPodsGone(ctx, operatorSelector, 45*time.Second)
}

func (r *resetter) scaleMysqlDown(ctx context.Context, sites []v1alpha1.SiteSpec) error {
	for _, site := range sites {
		if err := r.scaleDeploymentIfExists(ctx, pgkube.MysqlDeploymentName(r.fg, site.Name), 0); err != nil {
			return err
		}
	}
	if err := r.waitPodsGone(ctx, mysqlSelector, 90*time.Second); err != nil {
		r.warn("force-deleting lingering MySQL pods after scale-down timeout: %v", err)
		zero := int64(0)
		pods, listErr := r.k.Kubernetes.CoreV1().Pods(r.namespace).List(ctx, metav1.ListOptions{LabelSelector: mysqlSelector})
		if listErr != nil {
			return listErr
		}
		for i := range pods.Items {
			if delErr := r.k.Kubernetes.CoreV1().Pods(r.namespace).Delete(ctx, pods.Items[i].Name, metav1.DeleteOptions{GracePeriodSeconds: &zero}); delErr != nil && !apierrors.IsNotFound(delErr) {
				return delErr
			}
		}
		return r.waitPodsGone(ctx, mysqlSelector, 45*time.Second)
	}
	return nil
}

func (r *resetter) clearTransientState(ctx context.Context, _ []v1alpha1.SiteSpec) error {
	var errs []error
	if err := r.k.ClearChaosMarker(ctx, r.namespace); err != nil {
		errs = append(errs, fmt.Errorf("clear chaos marker: %w", err))
	}
	if err := r.clearReadOnlyTaints(ctx); err != nil {
		errs = append(errs, err)
	}
	return errors.Join(errs...)
}

func (r *resetter) deleteStorage(ctx context.Context, _ []v1alpha1.SiteSpec) error {
	var errs []error
	pvcs, err := r.k.Kubernetes.CoreV1().PersistentVolumeClaims(r.namespace).List(ctx, metav1.ListOptions{LabelSelector: mysqlSelector})
	if err != nil {
		return err
	}
	for i := range pvcs.Items {
		pvc := &pvcs.Items[i]
		if len(pvc.Finalizers) > 0 {
			if err := r.patchPVCFinalizers(ctx, pvc.Name, nil); err != nil {
				errs = append(errs, fmt.Errorf("patch pvc %s finalizers: %w", pvc.Name, err))
			}
		}
		if err := r.k.Kubernetes.CoreV1().PersistentVolumeClaims(r.namespace).Delete(ctx, pvc.Name, metav1.DeleteOptions{}); err != nil && !apierrors.IsNotFound(err) {
			errs = append(errs, fmt.Errorf("delete pvc %s: %w", pvc.Name, err))
		}
	}
	if err := r.waitPVCsGone(ctx, 60*time.Second); err != nil {
		errs = append(errs, err)
	}
	pvs, err := r.k.Kubernetes.CoreV1().PersistentVolumes().List(ctx, metav1.ListOptions{})
	if err != nil {
		return errors.Join(append(errs, err)...)
	}
	for i := range pvs.Items {
		pv := &pvs.Items[i]
		if pv.Spec.ClaimRef == nil || pv.Spec.ClaimRef.Namespace != r.namespace {
			continue
		}
		if len(pv.Finalizers) > 0 {
			if err := r.patchPVFinalizers(ctx, pv.Name, nil); err != nil {
				errs = append(errs, fmt.Errorf("patch pv %s finalizers: %w", pv.Name, err))
			}
		}
		if err := r.k.Kubernetes.CoreV1().PersistentVolumes().Delete(ctx, pv.Name, metav1.DeleteOptions{}); err != nil && !apierrors.IsNotFound(err) {
			errs = append(errs, fmt.Errorf("delete pv %s: %w", pv.Name, err))
		}
	}
	return errors.Join(errs...)
}

func (r *resetter) wipeNodeStorage(ctx context.Context, _ []v1alpha1.SiteSpec) error {
	if r.runtime == "" {
		r.warn("no docker/podman runtime found; skipping k3d hostPath wipe")
		return nil
	}
	nodes, err := r.k.ListNodes(ctx)
	if err != nil {
		return err
	}
	var errs []error
	for _, node := range nodes.Items {
		if !strings.HasPrefix(node.Name, "k3d-") {
			continue
		}
		cmd := exec.CommandContext(ctx, r.runtime, "exec", node.Name, "sh", "-c", "rm -rf /var/lib/rancher/k3s/storage/pvc-* /var/lib/rancher/k3s/storage/manual-mysql-playground-*")
		if out, err := cmd.CombinedOutput(); err != nil {
			errs = append(errs, fmt.Errorf("%s exec %s: %w: %s", r.runtime, node.Name, err, strings.TrimSpace(string(out))))
		}
	}
	return errors.Join(errs...)
}

func (r *resetter) restartLocalPathProvisioner(ctx context.Context, _ []v1alpha1.SiteSpec) error {
	dep, err := r.k.Kubernetes.AppsV1().Deployments("kube-system").Get(ctx, "local-path-provisioner", metav1.GetOptions{})
	if apierrors.IsNotFound(err) {
		r.warn("local-path-provisioner not found; skipping")
		return nil
	}
	if err != nil {
		return err
	}
	patch := []byte(fmt.Sprintf(`{"spec":{"template":{"metadata":{"annotations":{"kubectl.kubernetes.io/restartedAt":%q}}}}}`, time.Now().UTC().Format(time.RFC3339Nano)))
	if _, err := r.k.Kubernetes.AppsV1().Deployments("kube-system").Patch(ctx, dep.Name, types.StrategicMergePatchType, patch, metav1.PatchOptions{}); err != nil {
		return err
	}
	return r.waitDeploymentAvailable(ctx, "kube-system", dep.Name, 60*time.Second)
}

func (r *resetter) reapplyMysqlSecret(ctx context.Context, _ []v1alpha1.SiteSpec) error {
	path := filepath.Join(r.repoRoot, "playground", "manifests", "mysql-secret.yaml")
	body, err := os.ReadFile(path)
	if err != nil {
		return err
	}
	secret := &corev1.Secret{}
	if err := yaml.Unmarshal(body, secret); err != nil {
		return err
	}
	if secret.Namespace == "" {
		secret.Namespace = r.namespace
	}
	secret.ResourceVersion = ""
	secret.UID = ""
	secret.ManagedFields = nil
	_, err = r.k.Kubernetes.CoreV1().Secrets(secret.Namespace).Create(ctx, secret, metav1.CreateOptions{})
	if apierrors.IsAlreadyExists(err) {
		current, getErr := r.k.Kubernetes.CoreV1().Secrets(secret.Namespace).Get(ctx, secret.Name, metav1.GetOptions{})
		if getErr != nil {
			return getErr
		}
		secret.ResourceVersion = current.ResourceVersion
		_, err = r.k.Kubernetes.CoreV1().Secrets(secret.Namespace).Update(ctx, secret, metav1.UpdateOptions{})
	}
	return err
}

func (r *resetter) clearMFGStatus(ctx context.Context, _ []v1alpha1.SiteSpec) error {
	mfg, err := r.k.GetMFG(ctx, r.namespace)
	if apierrors.IsNotFound(err) {
		r.warn("MFG %s not found; skipping status clear", r.fg)
		return nil
	}
	if err != nil {
		return err
	}
	status, err := json.Marshal(mfg.Status)
	if err != nil {
		return err
	}
	var fields map[string]any
	if err := json.Unmarshal(status, &fields); err != nil {
		return err
	}
	var ops []pgkube.JSONPatchOp
	for _, field := range []string{"activeSite", "sites", "conditions", "lastFailover", "lastFailoverTarget", "promotionGtidExecuted", "plannedFailover", "updatePhase", "recovery", "dragonfly"} {
		if _, ok := fields[field]; ok {
			ops = append(ops, pgkube.JSONPatchOp{Op: "remove", Path: "/status/" + field})
		}
	}
	if len(ops) == 0 {
		return nil
	}
	body, err := json.Marshal(ops)
	if err != nil {
		return err
	}
	target := &v1alpha1.MysqlFailoverGroup{ObjectMeta: metav1.ObjectMeta{Name: r.fg, Namespace: r.namespace}}
	return r.k.Controller.Status().Patch(ctx, target, client.RawPatch(types.JSONPatchType, body))
}

func (r *resetter) recreatePVCs(ctx context.Context, sites []v1alpha1.SiteSpec) error {
	if err := r.scaleDeploymentIfExists(ctx, operatorDeployment, 1); err != nil {
		return err
	}
	if err := r.waitDeploymentAvailable(ctx, r.namespace, operatorDeployment, 90*time.Second); err != nil {
		r.warn("operator rollout did not become available while recreating PVCs: %v", err)
	}
	if err := r.waitPVCsPresent(ctx, sites, 90*time.Second); err != nil {
		return err
	}
	if err := r.scaleOperatorDown(ctx, sites); err != nil {
		return err
	}
	return r.clearReadOnlyTaints(ctx)
}

func (r *resetter) bindPVs(ctx context.Context, sites []v1alpha1.SiteSpec) error {
	for _, site := range sites {
		node, err := r.siteNode(ctx, site.Name)
		if err != nil {
			return err
		}
		if r.runtime != "" && strings.HasPrefix(node, "k3d-") {
			hostPath := fmt.Sprintf("/var/lib/rancher/k3s/storage/manual-%s", pgkube.MysqlPVCName(r.fg, site.Name))
			cmd := exec.CommandContext(ctx, r.runtime, "exec", node, "sh", "-c", "mkdir -p '"+hostPath+"' && chmod 0777 '"+hostPath+"'")
			if out, err := cmd.CombinedOutput(); err != nil {
				return fmt.Errorf("prepare hostPath on %s: %w: %s", node, err, strings.TrimSpace(string(out)))
			}
		}
		if err := r.createManualPV(ctx, site, node); err != nil {
			return err
		}
	}
	return r.waitPVCsBound(ctx, sites, 90*time.Second)
}

func (r *resetter) startMySQL(ctx context.Context, sites []v1alpha1.SiteSpec) error {
	for _, site := range sites {
		if err := r.scaleDeploymentIfExists(ctx, pgkube.MysqlDeploymentName(r.fg, site.Name), 1); err != nil {
			return err
		}
	}
	return r.waitMysqlReady(ctx, sites, 5*time.Minute)
}

func (r *resetter) createReplicationUsers(ctx context.Context, sites []v1alpha1.SiteSpec) error {
	creds, err := r.loadReplicationCredentials(ctx)
	if err != nil {
		return err
	}
	root := pgmysql.Credentials{RootUser: "root", RootPassword: creds.rootPassword}
	for _, site := range sites {
		c, err := pgmysql.Open(ctx, r.k, r.namespace, r.fg, site.Name, root)
		if err != nil {
			return err
		}
		if err := r.createReplicationUser(ctx, c.DB, creds.replUser, creds.replPassword); err != nil {
			_ = c.Close()
			return fmt.Errorf("site %s: %w", site.Name, err)
		}
		_ = c.Close()
	}
	return nil
}

func (r *resetter) startOperator(ctx context.Context, _ []v1alpha1.SiteSpec) error {
	if err := r.clearReadOnlyTaints(ctx); err != nil {
		return err
	}
	if err := r.scaleDeploymentIfExists(ctx, operatorDeployment, 1); err != nil {
		return err
	}
	return r.waitDeploymentAvailable(ctx, r.namespace, operatorDeployment, 2*time.Minute)
}

func (r *resetter) waitBaseline(ctx context.Context, _ []v1alpha1.SiteSpec) error {
	deadline := time.Now().Add(4 * time.Minute)
	var last error
	for time.Now().Before(deadline) {
		if err := scenarios.CheckBaseline(ctx, r.k, r.namespace, r.fg); err == nil {
			mfg, _ := r.k.GetMFG(ctx, r.namespace)
			if mfg != nil {
				r.info("baseline ok: activeSite=%s", mfg.Status.ActiveSite)
			}
			return nil
		} else {
			last = err
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(2 * time.Second):
		}
	}
	return fmt.Errorf("timed out waiting for healthy baseline: %w", last)
}

func (r *resetter) scaleDeploymentIfExists(ctx context.Context, name string, replicas int32) error {
	_, err := r.k.Kubernetes.AppsV1().Deployments(r.namespace).Get(ctx, name, metav1.GetOptions{})
	if apierrors.IsNotFound(err) {
		r.warn("deployment %s not found; skipping scale", name)
		return nil
	}
	if err != nil {
		return err
	}
	return r.k.ScaleDeployment(ctx, r.namespace, name, replicas)
}

func (r *resetter) waitDeploymentAvailable(ctx context.Context, namespace, name string, timeout time.Duration) error {
	return pollUntil(ctx, timeout, time.Second, func(ctx context.Context) (bool, string, error) {
		dep, err := r.k.Kubernetes.AppsV1().Deployments(namespace).Get(ctx, name, metav1.GetOptions{})
		if err != nil {
			return false, "get deployment failed: " + err.Error(), nil
		}
		return deploymentAvailable(dep), deploymentSummary(dep), nil
	})
}

func deploymentAvailable(dep *appsv1.Deployment) bool {
	for _, cond := range dep.Status.Conditions {
		if cond.Type == appsv1.DeploymentAvailable && cond.Status == corev1.ConditionTrue {
			return true
		}
	}
	return dep.Status.AvailableReplicas >= desiredReplicas(dep)
}

func desiredReplicas(dep *appsv1.Deployment) int32 {
	if dep.Spec.Replicas == nil {
		return 1
	}
	return *dep.Spec.Replicas
}

func deploymentSummary(dep *appsv1.Deployment) string {
	return fmt.Sprintf("desired=%d available=%d updated=%d", desiredReplicas(dep), dep.Status.AvailableReplicas, dep.Status.UpdatedReplicas)
}

func (r *resetter) waitPodsGone(ctx context.Context, selector string, timeout time.Duration) error {
	return pollUntil(ctx, timeout, time.Second, func(ctx context.Context) (bool, string, error) {
		pods, err := r.k.Kubernetes.CoreV1().Pods(r.namespace).List(ctx, metav1.ListOptions{LabelSelector: selector})
		if err != nil {
			return false, "list pods failed: " + err.Error(), nil
		}
		return len(pods.Items) == 0, fmt.Sprintf("%d pods remain", len(pods.Items)), nil
	})
}

func (r *resetter) waitPVCsGone(ctx context.Context, timeout time.Duration) error {
	return pollUntil(ctx, timeout, time.Second, func(ctx context.Context) (bool, string, error) {
		pvcs, err := r.k.Kubernetes.CoreV1().PersistentVolumeClaims(r.namespace).List(ctx, metav1.ListOptions{LabelSelector: mysqlSelector})
		if err != nil {
			return false, "list pvcs failed: " + err.Error(), nil
		}
		return len(pvcs.Items) == 0, fmt.Sprintf("%d mysql PVCs remain", len(pvcs.Items)), nil
	})
}

func (r *resetter) waitPVCsPresent(ctx context.Context, sites []v1alpha1.SiteSpec, timeout time.Duration) error {
	return pollUntil(ctx, timeout, time.Second, func(ctx context.Context) (bool, string, error) {
		missing := r.missingPVCs(ctx, sites, false)
		return len(missing) == 0, fmt.Sprintf("missing=%v", missing), nil
	})
}

func (r *resetter) waitPVCsBound(ctx context.Context, sites []v1alpha1.SiteSpec, timeout time.Duration) error {
	return pollUntil(ctx, timeout, time.Second, func(ctx context.Context) (bool, string, error) {
		missing := r.missingPVCs(ctx, sites, true)
		return len(missing) == 0, fmt.Sprintf("notBound=%v", missing), nil
	})
}

func (r *resetter) missingPVCs(ctx context.Context, sites []v1alpha1.SiteSpec, requireBound bool) []string {
	var missing []string
	for _, site := range sites {
		name := pgkube.MysqlPVCName(r.fg, site.Name)
		pvc, err := r.k.Kubernetes.CoreV1().PersistentVolumeClaims(r.namespace).Get(ctx, name, metav1.GetOptions{})
		if err != nil {
			missing = append(missing, name+":missing")
			continue
		}
		if requireBound && pvc.Status.Phase != corev1.ClaimBound {
			missing = append(missing, name+":"+string(pvc.Status.Phase))
		}
	}
	return missing
}

func (r *resetter) waitMysqlReady(ctx context.Context, sites []v1alpha1.SiteSpec, timeout time.Duration) error {
	want := len(sites)
	return pollUntil(ctx, timeout, 2*time.Second, func(ctx context.Context) (bool, string, error) {
		_ = r.clearReadOnlyTaints(ctx)
		pods, err := r.k.Kubernetes.CoreV1().Pods(r.namespace).List(ctx, metav1.ListOptions{LabelSelector: mysqlSelector})
		if err != nil {
			return false, "list pods failed: " + err.Error(), nil
		}
		ready := 0
		var states []string
		for _, pod := range pods.Items {
			if podReady(&pod) {
				ready++
			}
			states = append(states, fmt.Sprintf("%s=%s/%v", pod.Name, pod.Status.Phase, podReady(&pod)))
		}
		return ready >= want, fmt.Sprintf("ready=%d/%d states=%v", ready, want, states), nil
	})
}

func podReady(pod *corev1.Pod) bool {
	if pod.Status.Phase != corev1.PodRunning || len(pod.Status.ContainerStatuses) == 0 {
		return false
	}
	for _, cs := range pod.Status.ContainerStatuses {
		if !cs.Ready {
			return false
		}
	}
	return true
}

func (r *resetter) siteNode(ctx context.Context, site string) (string, error) {
	nodes, err := r.k.Kubernetes.CoreV1().Nodes().List(ctx, metav1.ListOptions{LabelSelector: "shipstream.io/site.playground=" + site})
	if err != nil {
		return "", err
	}
	if len(nodes.Items) == 0 {
		return "", fmt.Errorf("no node has shipstream.io/site.playground=%s", site)
	}
	return nodes.Items[0].Name, nil
}

func (r *resetter) createManualPV(ctx context.Context, site v1alpha1.SiteSpec, node string) error {
	pvcName := pgkube.MysqlPVCName(r.fg, site.Name)
	pvName := "manual-" + pvcName
	if err := r.k.Kubernetes.CoreV1().PersistentVolumes().Delete(ctx, pvName, metav1.DeleteOptions{}); err != nil && !apierrors.IsNotFound(err) {
		return err
	}
	capacity := site.Storage.Size
	if capacity.IsZero() {
		capacity = apiresource.MustParse("1Gi")
	}
	storageClass := site.Storage.StorageClassName
	pv := &corev1.PersistentVolume{
		ObjectMeta: metav1.ObjectMeta{Name: pvName},
		Spec: corev1.PersistentVolumeSpec{
			Capacity:                      corev1.ResourceList{corev1.ResourceStorage: capacity},
			AccessModes:                   []corev1.PersistentVolumeAccessMode{corev1.ReadWriteOnce},
			PersistentVolumeReclaimPolicy: corev1.PersistentVolumeReclaimDelete,
			StorageClassName:              storageClass,
			VolumeMode:                    ptr(corev1.PersistentVolumeFilesystem),
			ClaimRef:                      &corev1.ObjectReference{Kind: "PersistentVolumeClaim", Namespace: r.namespace, Name: pvcName},
			PersistentVolumeSource: corev1.PersistentVolumeSource{HostPath: &corev1.HostPathVolumeSource{
				Path: fmt.Sprintf("/var/lib/rancher/k3s/storage/%s", pvName),
				Type: ptr(corev1.HostPathDirectoryOrCreate),
			}},
			NodeAffinity: &corev1.VolumeNodeAffinity{Required: &corev1.NodeSelector{NodeSelectorTerms: []corev1.NodeSelectorTerm{{MatchExpressions: []corev1.NodeSelectorRequirement{{
				Key:      "kubernetes.io/hostname",
				Operator: corev1.NodeSelectorOpIn,
				Values:   []string{node},
			}}}}}},
		},
	}
	_, err := r.k.Kubernetes.CoreV1().PersistentVolumes().Create(ctx, pv, metav1.CreateOptions{})
	return err
}

func (r *resetter) clearReadOnlyTaints(ctx context.Context) error {
	nodes, err := r.k.ListNodes(ctx)
	if err != nil {
		return err
	}
	var errs []error
	for _, node := range nodes.Items {
		for _, key := range []string{readonlyTaintKey, legacyTaintKey} {
			if err := r.k.RemoveNodeTaint(ctx, node.Name, key); err != nil {
				errs = append(errs, fmt.Errorf("remove taint %s from %s: %w", key, node.Name, err))
			}
		}
	}
	return errors.Join(errs...)
}

func (r *resetter) loadReplicationCredentials(ctx context.Context) (struct{ rootPassword, replUser, replPassword string }, error) {
	secret, err := r.k.Kubernetes.CoreV1().Secrets(r.namespace).Get(ctx, "mysql-credentials", metav1.GetOptions{})
	if err != nil {
		return struct{ rootPassword, replUser, replPassword string }{}, err
	}
	creds := struct{ rootPassword, replUser, replPassword string }{
		rootPassword: string(secret.Data["MYSQL_ROOT_PASSWORD"]),
		replUser:     string(secret.Data["MYSQL_REPLICATION_USER"]),
		replPassword: string(secret.Data["MYSQL_REPLICATION_PASSWORD"]),
	}
	if creds.rootPassword == "" || creds.replUser == "" || creds.replPassword == "" {
		return creds, fmt.Errorf("mysql-credentials missing root or replication keys")
	}
	return creds, nil
}

func (r *resetter) createReplicationUser(ctx context.Context, db *sql.DB, replUser, replPassword string) error {
	stmt := fmt.Sprintf("SET GLOBAL super_read_only=OFF; SET GLOBAL read_only=OFF; CREATE USER IF NOT EXISTS '%s'@'%%' IDENTIFIED BY '%s'; GRANT REPLICATION SLAVE, REPLICATION CLIENT, BACKUP_ADMIN, CLONE_ADMIN ON *.* TO '%s'@'%%'; FLUSH PRIVILEGES;",
		escapeSQLString(replUser), escapeSQLString(replPassword), escapeSQLString(replUser))
	_, err := db.ExecContext(ctx, stmt)
	return err
}

func escapeSQLString(s string) string {
	s = strings.ReplaceAll(s, `\\`, `\\\\`)
	return strings.ReplaceAll(s, `'`, `''`)
}

func (r *resetter) patchPVCFinalizers(ctx context.Context, name string, finalizers []string) error {
	body, _ := json.Marshal(map[string]any{"metadata": map[string]any{"finalizers": finalizers}})
	_, err := r.k.Kubernetes.CoreV1().PersistentVolumeClaims(r.namespace).Patch(ctx, name, types.MergePatchType, body, metav1.PatchOptions{})
	return err
}

func (r *resetter) patchPVFinalizers(ctx context.Context, name string, finalizers []string) error {
	body, _ := json.Marshal(map[string]any{"metadata": map[string]any{"finalizers": finalizers}})
	_, err := r.k.Kubernetes.CoreV1().PersistentVolumes().Patch(ctx, name, types.MergePatchType, body, metav1.PatchOptions{})
	return err
}

func (r *resetter) persistFailure(ctx context.Context, failure error) error {
	ctx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()
	dir := filepath.Join(r.resultsDir, "reset-"+time.Now().UTC().Format("20060102T150405Z"))
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return err
	}
	write := func(name string, body []byte) {
		_ = os.WriteFile(filepath.Join(dir, name), body, 0o644)
	}
	write("failure.txt", []byte(failure.Error()+"\n"))
	if mfg, err := r.k.GetMFG(ctx, r.namespace); err == nil {
		if body, err := yaml.Marshal(mfg); err == nil {
			write("cluster.yaml", body)
		}
	}
	if pods, err := r.k.Kubernetes.CoreV1().Pods(r.namespace).List(ctx, metav1.ListOptions{}); err == nil {
		if body, err := yaml.Marshal(pods); err == nil {
			write("pods.yaml", body)
		}
	}
	if pvcs, err := r.k.Kubernetes.CoreV1().PersistentVolumeClaims(r.namespace).List(ctx, metav1.ListOptions{}); err == nil {
		if body, err := yaml.Marshal(pvcs); err == nil {
			write("pvcs.yaml", body)
		}
	}
	if pvs, err := r.k.Kubernetes.CoreV1().PersistentVolumes().List(ctx, metav1.ListOptions{}); err == nil {
		if body, err := yaml.Marshal(pvs); err == nil {
			write("pvs.yaml", body)
		}
	}
	if nodes, err := r.k.Kubernetes.CoreV1().Nodes().List(ctx, metav1.ListOptions{}); err == nil {
		if body, err := yaml.Marshal(nodes); err == nil {
			write("nodes.yaml", body)
		}
	}
	if events, err := r.k.RecentEvents(ctx, r.namespace, 200); err == nil {
		if body, err := yaml.Marshal(events); err == nil {
			write("events.yaml", body)
		}
	}
	r.info("forensics written to %s", dir)
	return nil
}

func pollUntil(ctx context.Context, timeout, interval time.Duration, fn func(context.Context) (bool, string, error)) error {
	deadline := time.NewTimer(timeout)
	defer deadline.Stop()
	tick := time.NewTicker(interval)
	defer tick.Stop()
	var last string
	for {
		done, msg, err := fn(ctx)
		if err != nil {
			return err
		}
		if done {
			return nil
		}
		last = msg
		select {
		case <-ctx.Done():
			return fmt.Errorf("context done while waiting: %w (last: %s)", ctx.Err(), last)
		case <-deadline.C:
			return fmt.Errorf("timed out after %s (last: %s)", timeout, last)
		case <-tick.C:
		}
	}
}

func ptr[T any](v T) *T { return &v }

func containerRuntime() string {
	if v := strings.TrimSpace(os.Getenv("BLOODRAVEN_CONTAINER_RUNTIME")); v != "" {
		return v
	}
	if _, err := exec.LookPath("docker"); err == nil {
		return "docker"
	}
	if _, err := exec.LookPath("podman"); err == nil {
		return "podman"
	}
	return ""
}

func (r *resetter) info(format string, args ...any) {
	line := fmt.Sprintf(format, args...)
	fmt.Fprintln(os.Stderr, "==> "+line)
	if r.logger != nil {
		r.logger.Info(line)
	}
}

func (r *resetter) warn(format string, args ...any) {
	line := fmt.Sprintf(format, args...)
	fmt.Fprintln(os.Stderr, "!! "+line)
	if r.logger != nil {
		r.logger.Warn(line)
	}
}
