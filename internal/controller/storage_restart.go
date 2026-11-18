package controller

import (
	"context"
	"fmt"
	"strings"

	v1beta1 "github.com/shipstream/bloodraven/api/v1beta1"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

// requestedStorageRestart validates an administrator's request against the
// claim currently mounted by this site. A pending filesystem operation may
// still finish online while a requested restart waits for a healthy peer.
func requestedStorageRestart(ctx context.Context, reader client.Reader, fg *v1beta1.MysqlFailoverGroup, site v1beta1.SiteSpec) (string, error) {
	request := fg.Annotations[v1beta1.StorageRestartAnnotation]
	if !strings.HasPrefix(request, site.Name+"/") || fg.Spec.UpdateStrategy.EffectiveType() != v1beta1.UpdateStrategyOrderedUpdate {
		return "", nil
	}
	var pvc corev1.PersistentVolumeClaim
	key := client.ObjectKey{Namespace: fg.Namespace, Name: resourceName(fg.Name, site.Name) + "-data"}
	if err := reader.Get(ctx, key, &pvc); err != nil {
		return "", client.IgnoreNotFound(err)
	}
	if pvc.UID == "" || pvc.DeletionTimestamp != nil || !metav1.IsControlledBy(&pvc, fg) {
		return "", nil
	}
	desired, err := site.Storage.Size.Quantity()
	if err != nil {
		return "", err
	}
	size, ok := pvcStorageRequest(&pvc)
	if !ok || !size.Equal(desired) || request != storageRestartToken(site.Name, string(pvc.UID), size.String()) {
		return "", nil
	}
	if pending, _ := siteFileSystemResizePending(&pvc); pending {
		return request, nil
	}
	return "", nil
}

func storageRestartToken(site, uid, size string) string {
	return site + "/" + uid + "/" + size
}

func (r *MysqlFailoverGroupReconciler) describeStorageRestart(ctx context.Context, fg *v1beta1.MysqlFailoverGroup, st *v1beta1.SiteStorageStatus) {
	token := storageRestartToken(st.Name, string(st.ClaimUID), st.RequestedSize)
	if fg.Spec.UpdateStrategy.EffectiveType() == v1beta1.UpdateStrategyRecreate {
		st.Message += ". Select OrderedUpdate before requesting a remount"
		return
	}
	var deployment appsv1.Deployment
	key := client.ObjectKey{Namespace: fg.Namespace, Name: resourceName(fg.Name, st.Name)}
	if err := r.Get(ctx, key, &deployment); err == nil && deployment.Spec.Template.Annotations[v1beta1.StorageRestartAnnotation] == token {
		st.Message += ". The restart was submitted for this claim and size; check the rollout and the storage driver if the filesystem remains pending"
		return
	} else if err != nil && !apierrors.IsNotFound(err) {
		st.Message += ". Could not read the site's Deployment to confirm the last restart"
		return
	}
	if fg.Annotations[v1beta1.StorageRestartAnnotation] == token {
		st.Message += ". Restart requested; waiting for the ordered updater and a safe site to restart"
	} else {
		st.Message += fmt.Sprintf(". If the driver requires a remount, set the group annotation %s=%s", v1beta1.StorageRestartAnnotation, token)
	}
}
