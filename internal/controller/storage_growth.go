package controller

import (
	"context"
	stderrors "errors"
	"fmt"
	"sync"
	"time"

	corev1 "k8s.io/api/core/v1"
	storagev1 "k8s.io/api/storage/v1"
	"k8s.io/apimachinery/pkg/api/equality"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	k8sretry "k8s.io/client-go/util/retry"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/log"

	v1beta1 "github.com/shipstream/bloodraven/api/v1beta1"
)

// Site storage growth (#302).
//
// spec.sites[].storage.size can increase (admission refuses a decrease).
// reconcilePVC creates a missing claim at that size and, for a bound claim
// that requests less, raises the claim's request when the claim's own
// StorageClass allows volume expansion. It never lowers a request: a claim
// expanded directly beyond the spec is kept and reported Underspecified,
// because its replacement after a loss would be created at the smaller
// spec size.
//
// Growth never blocks anything else. A claim that cannot grow, an
// expansion in progress, a refused request or a transient error is
// reported in status.storage.sites (and by an Event on the transition)
// and requeued; the reconcile goes on with the site's Deployment, the
// other sites and the topology runner as before. Nothing here restarts a
// pod: FileSystemResizePending is reported for the restart coordination
// to act on (siteFileSystemResizePending, and the phase of the same
// name).
//
// status.storage is the group reconciler's alone. It is written once per
// pass, after every site was observed, on a fresh copy of the group with
// RetryOnConflict and only when it changed; the topology runner's
// wholesale status write re-reads it (updateCRStatus) instead of replaying
// a stale copy.

const (
	// storageRetryInterval requeues a site whose observation or request
	// hit a transient error, or whose request lost a write race.
	storageRetryInterval = 15 * time.Second
	// storageRecheckInterval requeues a site that cannot grow (an
	// unsupported class, a refused request): nothing the operator watches
	// changes when an administrator fixes the class or a quota.
	storageRecheckInterval = 5 * time.Minute
	// storageClassVerdictTTL bounds how long a StorageClass read is reused.
	// The class is read only for a claim that needs growth, uncached (the
	// operator has get, not list/watch, on storageclasses), so this keeps a
	// pending growth from reading it on every reconcile of every group.
	storageClassVerdictTTL = time.Minute
	// storageStatusPrunedRetry is how long status.storage writes are
	// skipped after the installed CRD pruned the field (a CRD older than
	// the operator): a status that carries it would never compare equal
	// and would be written on every reconcile.
	storageStatusPrunedRetry = 10 * time.Minute
)

// storageClassVerdicts caches allowVolumeExpansion per class name.
type storageClassVerdicts struct {
	mu      sync.Mutex
	entries map[string]storageClassVerdict
	// prunedUntil is set when the installed CRD pruned status.storage.
	prunedUntil time.Time
}

type storageClassVerdict struct {
	found  bool
	allows bool
	at     time.Time
}

// siteStorageResult is one site's observation and when to look again.
type siteStorageResult struct {
	status  v1beta1.SiteStorageStatus
	requeue time.Duration
}

// pvcStorageRequest returns the claim's requested storage.
func pvcStorageRequest(pvc *corev1.PersistentVolumeClaim) (resource.Quantity, bool) {
	q, ok := pvc.Spec.Resources.Requests[corev1.ResourceStorage]
	return q, ok
}

// pvcCondition returns the claim's condition of type t when it is True.
func pvcCondition(pvc *corev1.PersistentVolumeClaim, t corev1.PersistentVolumeClaimConditionType) *corev1.PersistentVolumeClaimCondition {
	for i := range pvc.Status.Conditions {
		if pvc.Status.Conditions[i].Type == t && pvc.Status.Conditions[i].Status == corev1.ConditionTrue {
			return &pvc.Status.Conditions[i]
		}
	}
	return nil
}

func pvcResizeStatus(pvc *corev1.PersistentVolumeClaim) corev1.ClaimResourceStatus {
	return pvc.Status.AllocatedResourceStatuses[corev1.ResourceStorage]
}

// siteFileSystemResizePending reports whether the claim's volume has grown
// and its file system waits for the node to expand it: the
// FileSystemResizePending condition, or the NodeResizePending resize
// status. A driver with online expansion clears it while the pod runs; one
// without clears it only when the volume is mounted again, which takes a
// restart of the site's pod. since is when the condition became True
// (zero when only the resize status says so).
func siteFileSystemResizePending(pvc *corev1.PersistentVolumeClaim) (pending bool, since time.Time) {
	if pvc == nil || pvc.Status.Phase != corev1.ClaimBound {
		return false, time.Time{}
	}
	if pvcCondition(pvc, corev1.PersistentVolumeClaimResizing) != nil || pvcResizeErrorSuffix(pvc) != "" {
		return false, time.Time{}
	}
	request, ok := pvcStorageRequest(pvc)
	if allocated, present := pvc.Status.AllocatedResources[corev1.ResourceStorage]; present && (!ok || !allocated.Equal(request)) {
		return false, time.Time{}
	}
	switch pvcResizeStatus(pvc) {
	case "", corev1.PersistentVolumeClaimNodeResizePending:
	default:
		return false, time.Time{}
	}
	if c := pvcCondition(pvc, corev1.PersistentVolumeClaimFileSystemResizePending); c != nil {
		return true, c.LastTransitionTime.Time
	}
	return pvcResizeStatus(pvc) == corev1.PersistentVolumeClaimNodeResizePending, time.Time{}
}

// observeSiteClaim classifies a site's claim against the desired size. It
// is pure (no reads): grow reports that the claim is bound, requests less
// than desired, and nothing rules a request out, so the caller checks the
// class and raises the request.
func observeSiteClaim(siteName, desiredRaw string, desired resource.Quantity, pvc *corev1.PersistentVolumeClaim) (st v1beta1.SiteStorageStatus, grow bool) {
	st = v1beta1.SiteStorageStatus{
		Name:        siteName,
		ClaimName:   pvc.Name,
		ClaimUID:    pvc.UID,
		DesiredSize: desiredRaw,
	}
	if pvc.Spec.StorageClassName != nil {
		st.StorageClassName = *pvc.Spec.StorageClassName
	}
	requested, hasRequest := pvcStorageRequest(pvc)
	if hasRequest {
		st.RequestedSize = requested.String()
	}
	capacity, hasCapacity := pvc.Status.Capacity[corev1.ResourceStorage]
	if hasCapacity {
		st.Capacity = capacity.String()
	}

	if pvc.DeletionTimestamp != nil {
		st.Phase = v1beta1.SiteStoragePending
		st.Message = fmt.Sprintf("claim %s is being deleted; its replacement is created at %s", pvc.Name, desiredRaw)
		return st, false
	}
	if pvc.Status.Phase != corev1.ClaimBound {
		st.Phase = v1beta1.SiteStoragePending
		st.Message = fmt.Sprintf("claim %s is not bound yet", pvc.Name)
		if hasRequest && requested.Cmp(desired) < 0 {
			st.Message += fmt.Sprintf("; it requests %s, and growth to %s is requested once it is bound", st.RequestedSize, desiredRaw)
		}
		return st, false
	}

	// A claim requesting more than the spec is never shrunk.
	if hasRequest && requested.Cmp(desired) > 0 {
		st.Phase = v1beta1.SiteStorageUnderspecified
		st.Message = fmt.Sprintf("claim %s requests %s, more than spec.sites[%s].storage.size %s; the claim is kept, but a replacement after its loss would be created at %s: raise the size to at least %s",
			pvc.Name, st.RequestedSize, siteName, desiredRaw, desiredRaw, st.RequestedSize)
		return st, false
	}

	resize := pvcResizeStatus(pvc)
	if resize == corev1.PersistentVolumeClaimControllerResizeInfeasible || resize == corev1.PersistentVolumeClaimNodeResizeInfeasible {
		st.Phase = v1beta1.SiteStorageFailed
		st.Message = fmt.Sprintf("the storage driver reports the expansion of claim %s infeasible (%s)%s; the operator does not raise the request again until the driver clears it",
			pvc.Name, resize, pvcResizeErrorSuffix(pvc))
		return st, false
	}

	if !hasRequest || requested.Cmp(desired) < 0 {
		// The caller decides: the class may not allow it.
		st.Phase = v1beta1.SiteStorageExpanding
		return st, true
	}

	if pending, _ := siteFileSystemResizePending(pvc); pending {
		st.Phase = v1beta1.SiteStorageFileSystemResizePending
		st.Message = fmt.Sprintf("the volume of claim %s has grown; its file system grows when the node expands it, which needs a restart of the site's pod on a driver without online expansion", pvc.Name)
		return st, false
	}
	if failure := pvcResizeErrorSuffix(pvc); failure != "" {
		st.Phase = v1beta1.SiteStorageFailed
		st.Message = fmt.Sprintf("the storage driver reported an error expanding claim %s%s; check the PVC events and driver before requesting a remount", pvc.Name, failure)
		return st, false
	}
	if !hasCapacity || capacity.Cmp(requested) < 0 ||
		resize == corev1.PersistentVolumeClaimControllerResizeInProgress ||
		resize == corev1.PersistentVolumeClaimNodeResizeInProgress ||
		pvcCondition(pvc, corev1.PersistentVolumeClaimResizing) != nil ||
		pvcCondition(pvc, corev1.PersistentVolumeClaimFileSystemResizePending) != nil || resize == corev1.PersistentVolumeClaimNodeResizePending {
		st.Phase = v1beta1.SiteStorageExpanding
		st.Message = fmt.Sprintf("claim %s requests %s; the storage driver is expanding the volume", pvc.Name, st.RequestedSize)
		return st, false
	}
	st.Phase = v1beta1.SiteStorageSatisfied
	return st, false
}

// pvcResizeErrorSuffix quotes the resizer's last error, if any.
func pvcResizeErrorSuffix(pvc *corev1.PersistentVolumeClaim) string {
	for _, t := range []corev1.PersistentVolumeClaimConditionType{corev1.PersistentVolumeClaimControllerResizeError, corev1.PersistentVolumeClaimNodeResizeError} {
		if c := pvcCondition(pvc, t); c != nil {
			if c.Message == "" {
				return fmt.Sprintf(" (%s)", t)
			}
			return fmt.Sprintf(" (%s: %s)", t, c.Message)
		}
	}
	return ""
}

// storageClassAllowsExpansion reads the class (uncached, get only) or
// reuses a verdict younger than storageClassVerdictTTL. found is false for
// a class that does not exist.
func (r *MysqlFailoverGroupReconciler) storageClassAllowsExpansion(ctx context.Context, name string) (found, allows bool, err error) {
	now := r.now()
	v := &r.storageClasses
	v.mu.Lock()
	if e, ok := v.entries[name]; ok && now.Sub(e.at) < storageClassVerdictTTL && !now.Before(e.at) {
		v.mu.Unlock()
		return e.found, e.allows, nil
	}
	v.mu.Unlock()

	reader := client.Reader(r.Client)
	if r.APIReader != nil {
		reader = r.APIReader
	}
	var sc storagev1.StorageClass
	err = reader.Get(ctx, types.NamespacedName{Name: name}, &sc)
	if err != nil && !apierrors.IsNotFound(err) {
		return false, false, err
	}
	verdict := storageClassVerdict{at: now, found: err == nil, allows: sc.AllowVolumeExpansion != nil && *sc.AllowVolumeExpansion}
	v.mu.Lock()
	if v.entries == nil {
		v.entries = map[string]storageClassVerdict{}
	}
	for class, entry := range v.entries {
		if now.Sub(entry.at) >= storageClassVerdictTTL {
			delete(v.entries, class)
		}
	}
	v.entries[name] = verdict
	v.mu.Unlock()
	return verdict.found, verdict.allows, nil
}

// storageErrorTransient reports whether a failed read or request is worth
// a quick retry rather than a Failed report.
func storageErrorTransient(err error) bool {
	var status apierrors.APIStatus
	if !stderrors.As(err, &status) {
		// Not an API answer: a connection or context error.
		return true
	}
	return apierrors.IsConflict(err) || apierrors.IsServerTimeout(err) || apierrors.IsTimeout(err) ||
		apierrors.IsTooManyRequests(err) || apierrors.IsServiceUnavailable(err) || apierrors.IsInternalError(err) ||
		apierrors.IsUnexpectedServerError(err)
}

// growSiteClaim checks the claim's own class and raises its request to
// desired. It never returns an error: every outcome is a status for the
// site and a requeue.
func (r *MysqlFailoverGroupReconciler) growSiteClaim(ctx context.Context, fg *v1beta1.MysqlFailoverGroup, st v1beta1.SiteStorageStatus, desired resource.Quantity, pvc *corev1.PersistentVolumeClaim) siteStorageResult {
	logger := log.FromContext(ctx)
	from := st.RequestedSize
	if from == "" {
		from = "no size"
	}
	if st.StorageClassName == "" {
		st.Phase = v1beta1.SiteStorageUnsupported
		st.Message = fmt.Sprintf("claim %s requests %s, less than %s, and has no StorageClass, so it cannot be expanded; it is kept as it is",
			pvc.Name, from, st.DesiredSize)
		return siteStorageResult{status: st, requeue: storageRecheckInterval}
	}
	found, allows, err := r.storageClassAllowsExpansion(ctx, st.StorageClassName)
	if err != nil {
		st.Phase = v1beta1.SiteStorageRetrying
		st.Message = fmt.Sprintf("could not read StorageClass %s to grow claim %s to %s; retrying: %v", st.StorageClassName, pvc.Name, st.DesiredSize, err)
		return siteStorageResult{status: st, requeue: storageRetryInterval}
	}
	if !found || !allows {
		why := "does not allow volume expansion (allowVolumeExpansion is not true)"
		if !found {
			why = "does not exist"
		}
		st.Phase = v1beta1.SiteStorageUnsupported
		st.Message = fmt.Sprintf("claim %s requests %s, less than %s, and its StorageClass %s %s; the claim is kept as it is, and a replacement would be created at %s",
			pvc.Name, from, st.DesiredSize, st.StorageClassName, why, st.DesiredSize)
		return siteStorageResult{status: st, requeue: storageRecheckInterval}
	}

	base := pvc.DeepCopy()
	if pvc.Spec.Resources.Requests == nil {
		pvc.Spec.Resources.Requests = corev1.ResourceList{}
	}
	pvc.Spec.Resources.Requests[corev1.ResourceStorage] = desired
	// The optimistic lock makes the request apply only to the claim it was
	// computed from: a request raised directly in between must not be
	// replaced by a smaller one (the API server lets a request go down to
	// the capacity when recovering from a failed expansion).
	if err := r.Patch(ctx, pvc, client.MergeFromWithOptions(base, client.MergeFromWithOptimisticLock{})); err != nil {
		*pvc = *base
		if storageErrorTransient(err) {
			st.Phase = v1beta1.SiteStorageRetrying
			st.Message = fmt.Sprintf("could not raise the request of claim %s from %s to %s; retrying: %v", pvc.Name, from, st.DesiredSize, err)
			return siteStorageResult{status: st, requeue: storageRetryInterval}
		}
		st.Phase = v1beta1.SiteStorageFailed
		st.Message = fmt.Sprintf("the API server refused raising the request of claim %s from %s to %s (a ResourceQuota, a LimitRange or an admission policy can); the operator tries again: %v",
			pvc.Name, from, st.DesiredSize, err)
		return siteStorageResult{status: st, requeue: storageRecheckInterval}
	}
	logger.Info("site storage expansion requested", "site", st.Name, "claim", pvc.Name, "from", from, "to", st.DesiredSize)
	r.Recorder.Eventf(fg, corev1.EventTypeNormal, v1beta1.EventStorageExpansionRequested,
		"raised the request of claim %s (site %s) from %s to %s", pvc.Name, st.Name, from, st.DesiredSize)
	// The claim as patched: the driver has not acted on it yet.
	after, _ := observeSiteClaim(st.Name, st.DesiredSize, desired, pvc)
	return siteStorageResult{status: after}
}

// buildStorageStatus orders the observations as spec.sites and carries
// each entry's lastTransitionTime from live while its phase is unchanged.
func buildStorageStatus(live *v1beta1.MysqlFailoverGroupStatus, sites []v1beta1.SiteStorageStatus, now metav1.Time) *v1beta1.StorageStatus {
	if len(sites) == 0 {
		return nil
	}
	out := &v1beta1.StorageStatus{Sites: make([]v1beta1.SiteStorageStatus, len(sites))}
	for i, s := range sites {
		if prev := live.SiteStorage(s.Name); prev != nil && prev.Phase == s.Phase && prev.LastTransitionTime != nil {
			t := *prev.LastTransitionTime
			s.LastTransitionTime = &t
		} else {
			t := now
			s.LastTransitionTime = &t
		}
		out.Sites[i] = s
	}
	return out
}

// storageTransitionEvent is the Warning a site's entry into phase records.
func storageTransitionEvent(phase v1beta1.SiteStoragePhase) string {
	switch phase {
	case v1beta1.SiteStorageUnsupported:
		return v1beta1.EventStorageExpansionUnsupported
	case v1beta1.SiteStorageFailed:
		return v1beta1.EventStorageExpansionFailed
	case v1beta1.SiteStorageUnderspecified:
		return v1beta1.EventStorageSizeUnderspecified
	}
	return ""
}

// writeStorageStatus stores the pass's observations in status.storage on a
// fresh copy of the group, and nothing else. It skips the write when the
// stored status already says the same, and when the stored group has a
// newer spec (the reconcile that spec triggers writes against it). fg's
// in-memory status follows what is stored.
func (r *MysqlFailoverGroupReconciler) writeStorageStatus(ctx context.Context, fg *v1beta1.MysqlFailoverGroup, sites []v1beta1.SiteStorageStatus) error {
	logger := log.FromContext(ctx)
	now := r.now()
	if equality.Semantic.DeepEqual(fg.Status.Storage, buildStorageStatus(&fg.Status, sites, metav1.NewTime(now))) {
		return nil
	}
	r.storageClasses.mu.Lock()
	pruned := now.Before(r.storageClasses.prunedUntil)
	r.storageClasses.mu.Unlock()
	if pruned {
		return nil
	}
	reader := client.Reader(r.Client)
	if r.APIReader != nil {
		reader = r.APIReader
	}
	var (
		written  *v1beta1.StorageStatus
		previous *v1beta1.StorageStatus
		wrote    bool
	)
	err := k8sretry.RetryOnConflict(k8sretry.DefaultRetry, func() error {
		wrote = false
		var fresh v1beta1.MysqlFailoverGroup
		if err := reader.Get(ctx, client.ObjectKeyFromObject(fg), &fresh); err != nil {
			return err
		}
		if fresh.UID != fg.UID || fresh.DeletionTimestamp != nil {
			return nil
		}
		if fresh.Generation != fg.Generation {
			written = fresh.Status.Storage
			return nil
		}
		next := buildStorageStatus(&fresh.Status, sites, metav1.NewTime(now))
		if equality.Semantic.DeepEqual(fresh.Status.Storage, next) {
			written = fresh.Status.Storage
			return nil
		}
		previous = fresh.Status.Storage.DeepCopy()
		fresh.Status.Storage = next
		if err := r.Status().Update(ctx, &fresh); err != nil {
			return err
		}
		if next != nil && fresh.Status.Storage == nil {
			r.storageClasses.mu.Lock()
			r.storageClasses.prunedUntil = now.Add(storageStatusPrunedRetry)
			r.storageClasses.mu.Unlock()
			logger.Info("the installed CRD prunes status.storage; apply this release's CRDs to see site storage status")
		}
		written, wrote = fresh.Status.Storage, true
		return nil
	})
	if err != nil {
		if apierrors.IsNotFound(err) {
			return nil
		}
		return fmt.Errorf("write storage status: %w", err)
	}
	fg.Status.Storage = written.DeepCopy()
	if !wrote || written == nil {
		return nil
	}
	prevStatus := v1beta1.MysqlFailoverGroupStatus{Storage: previous}
	for _, s := range written.Sites {
		reason := storageTransitionEvent(s.Phase)
		if reason == "" {
			continue
		}
		if prev := prevStatus.SiteStorage(s.Name); prev != nil && prev.Phase == s.Phase {
			continue
		}
		r.Recorder.Eventf(fg, corev1.EventTypeWarning, reason, "site %s: %s", s.Name, s.Message)
	}
	return nil
}
