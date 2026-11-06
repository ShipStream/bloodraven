package controller

import (
	"context"

	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/predicate"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	"github.com/shipstream/bloodraven/api/v1alpha1"
)

// mapVerificationToGroup enqueues the group a MysqlBackupVerification
// references directly (spec.failoverGroupRef): its PITR read target (§4.1)
// appears and disappears with it. No field index is involved; the binding
// pass lists the namespace's verifications and filters them in memory
// (listPITRVerifications).
func (r *MysqlFailoverGroupReconciler) mapVerificationToGroup(_ context.Context, obj client.Object) []reconcile.Request {
	v, ok := obj.(*v1alpha1.MysqlBackupVerification)
	if !ok || v.Spec.FailoverGroupRef.Name == "" {
		return nil
	}
	return []reconcile.Request{{NamespacedName: types.NamespacedName{Namespace: v.Namespace, Name: v.Spec.FailoverGroupRef.Name}}}
}

// mapPITRKeySnapshotToGroup enqueues the group an archive key snapshot
// belongs to, by its group-name label (snapshots have no owner reference, so
// Owns would see nothing). The pass itself scopes snapshots by the group-UID
// label, so a predecessor's snapshot only causes a no-op reconcile.
func (r *MysqlFailoverGroupReconciler) mapPITRKeySnapshotToGroup(_ context.Context, obj client.Object) []reconcile.Request {
	l := obj.GetLabels()
	if l[labelAppName] != appNamePITRKeySnapshot || l[labelFailoverGroup] == "" {
		return nil
	}
	return []reconcile.Request{{NamespacedName: types.NamespacedName{Namespace: obj.GetNamespace(), Name: l[labelFailoverGroup]}}}
}

// pitrKeySnapshotPredicate admits only archive key snapshots
// (app.kubernetes.io/name=mysql-pitr-key) to the Secret watch.
func pitrKeySnapshotPredicate() predicate.Predicate {
	return predicate.NewPredicateFuncs(func(obj client.Object) bool {
		return obj.GetLabels()[labelAppName] == appNamePITRKeySnapshot
	})
}
