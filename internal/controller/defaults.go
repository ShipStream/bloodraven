package controller

import (
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
)

// defaultRestoreResources returns the resource requests applied to a
// restore Job's main mysqlsh container when the failover group has no
// `spec.backup` block to copy `Resources` from (typically a bootstrap
// restore on a fresh deploy). Values match the operator's own request
// (see charts/bloodraven/values.yaml) so clusters with a LimitRange that
// requires `resources.requests` will admit the Job.
//
// No CPU/memory limit is set: the kernel scheduler can burst when the
// host has spare capacity, which keeps decrypt/load throughput high on
// large dumps. Users override by setting `spec.backup.resources`.
func defaultRestoreResources() corev1.ResourceRequirements {
	return corev1.ResourceRequirements{
		Requests: corev1.ResourceList{
			corev1.ResourceCPU:    resource.MustParse("100m"),
			corev1.ResourceMemory: resource.MustParse("128Mi"),
		},
	}
}

// defaultInitContainerResources returns the resource requests applied to
// restore/PITR/verification init containers (`pitr-download`,
// `decrypt-download`) when the failover group's `spec.backup.resources`
// does not specify any requests. The init containers run the operator
// binary streaming AES-GCM decrypt/download — 100m/128Mi matches the
// operator's own request to give the stream room to breathe without
// starving the node.
//
// No limit is set; users override by setting `spec.backup.resources`.
func defaultInitContainerResources() corev1.ResourceRequirements {
	return corev1.ResourceRequirements{
		Requests: corev1.ResourceList{
			corev1.ResourceCPU:    resource.MustParse("100m"),
			corev1.ResourceMemory: resource.MustParse("128Mi"),
		},
	}
}

// effectiveBackupResources returns the resource requirements that should
// be applied to backup-family containers (restore, cleanup, init
// containers). It prefers the user override on `spec.backup.resources`
// when the user has populated **either** Requests or Limits; otherwise
// it returns the supplied fallback.
//
// Rationale: a user who sets only `resources.limits.memory` (Kubernetes
// will then default Requests to Limits server-side at admission) should
// not have their Limits silently dropped by this helper. "Has any user
// intent" is the right gate, not "has Requests".
//
// "Set" means at least one non-zero quantity in `Requests` OR `Limits`.
// The empty `corev1.ResourceRequirements{}` carries no information and
// would defeat LimitRange admission, so we still fall back in that case.
func effectiveBackupResources(bspec *backupResourcesSource, fallback corev1.ResourceRequirements) corev1.ResourceRequirements {
	if bspec == nil {
		return fallback
	}
	if hasAnyResourceSpec(bspec.Resources) {
		return bspec.Resources
	}
	return fallback
}

// backupResourcesSource is the minimal slice of BackupSpec that
// effectiveBackupResources cares about; declared locally to avoid
// importing the API package's interior types into this helper.
type backupResourcesSource struct {
	Resources corev1.ResourceRequirements
}

// hasResourceRequests reports whether the supplied ResourceRequirements
// carries at least one non-zero entry in `Requests`. A nil/zero block
// is treated as "unset". Limits are NOT considered — use
// hasAnyResourceSpec for the broader "user supplied anything" gate.
func hasResourceRequests(r corev1.ResourceRequirements) bool {
	return hasAnyNonZero(r.Requests)
}

// hasAnyResourceSpec reports whether the supplied ResourceRequirements
// carries any non-zero quantity in either Requests or Limits. Used to
// preserve user intent when only Limits are set (Kubernetes will then
// default Requests to Limits at admission time).
func hasAnyResourceSpec(r corev1.ResourceRequirements) bool {
	return hasAnyNonZero(r.Requests) || hasAnyNonZero(r.Limits)
}

func hasAnyNonZero(rl corev1.ResourceList) bool {
	if len(rl) == 0 {
		return false
	}
	for _, q := range rl {
		if !q.IsZero() {
			return true
		}
	}
	return false
}
