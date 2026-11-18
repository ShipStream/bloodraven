package controller

import (
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/types"

	v1beta1 "github.com/shipstream/bloodraven/api/v1beta1"
)

// The operator's check at start (cmd/bloodraven, internal/crdcompat) reads
// the five CustomResourceDefinitions by name. It lists the objects of the
// five kinds with the permissions the reconcilers already have.
//
// +kubebuilder:rbac:groups=apiextensions.k8s.io,resources=customresourcedefinitions,resourceNames=mysqlfailovergroups.shipstream.io;mysqldatabases.shipstream.io;mysqlbackups.shipstream.io;mysqlbackupverifications.shipstream.io;mysqlstandbyclusters.shipstream.io,verbs=get

// noteNewerCRDs records, once per group for each operator start, that the
// installed CRDs are newer than this operator. The operator runs: a newer
// CRD is what it sees between the two steps of an upgrade and after a
// rollback of the operator alone. The API server keeps the fields this
// build does not know, but this build neither reads nor maintains them.
func (r *MysqlFailoverGroupReconciler) noteNewerCRDs(fg *v1beta1.MysqlFailoverGroup) {
	if r.NewerCRDs == "" {
		return
	}
	nn := types.NamespacedName{Namespace: fg.Namespace, Name: fg.Name}
	r.newerCRDsNotedMu.Lock()
	noted := r.newerCRDsNoted[nn]
	if !noted {
		if r.newerCRDsNoted == nil {
			r.newerCRDsNoted = map[types.NamespacedName]bool{}
		}
		r.newerCRDsNoted[nn] = true
	}
	r.newerCRDsNotedMu.Unlock()
	if noted {
		return
	}
	if r.Recorder != nil {
		r.Recorder.Eventf(fg, corev1.EventTypeWarning, v1beta1.EventNewerCRDDetected,
			"The installed CRDs are newer than this operator, which neither reads nor maintains what it does not know: %s. Upgrade the operator to the release the CRDs belong to", r.NewerCRDs)
	}
}
