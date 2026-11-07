package controller

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"io"
	"net/url"
	"slices"
	"strconv"
	"strings"
	"time"

	appsv1 "k8s.io/api/apps/v1"
	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
	"sigs.k8s.io/controller-runtime/pkg/log"

	v1alpha1 "github.com/shipstream/bloodraven/api/v1alpha1"
	bmetrics "github.com/shipstream/bloodraven/internal/metrics"
	"github.com/shipstream/bloodraven/internal/pitrarchive"
	"github.com/shipstream/bloodraven/internal/state"
)

// filterOutVolumeByName returns a copy of vols with the entry whose
// Name matches removed. Used by the decrypt-init-container wiring to
// replace the source-PVC mount with the decrypted-staging mount.
func filterOutVolumeByName(vols []corev1.Volume, name string) []corev1.Volume {
	out := vols[:0]
	for _, v := range vols {
		if v.Name == name {
			continue
		}
		out = append(out, v)
	}
	return out
}

// filterOutMountByName mirrors filterOutVolumeByName for VolumeMount slices.
func filterOutMountByName(mounts []corev1.VolumeMount, name string) []corev1.VolumeMount {
	out := mounts[:0]
	for _, m := range mounts {
		if m.Name == name {
			continue
		}
		out = append(out, m)
	}
	return out
}

// ensureTrailingSlash normalizes a local/PVC dump prefix/location so it ends
// with a forward slash. S3 inputs must use mysqlShellDumpInput instead.
func ensureTrailingSlash(s string) string {
	if s == "" {
		return s
	}
	if strings.HasSuffix(s, "/") {
		return s
	}
	return s + "/"
}

// mysqlShellDumpInput normalizes a dump location for mysqlsh util.loadDump.
// Local/PVC paths historically need a trailing slash, but mysqlsh's S3 client
// appends its own separator while listing object prefixes. S3-compatible stores
// such as RustFS reject the resulting double-slash prefix, so S3 inputs must be
// passed without a trailing slash.
func mysqlShellDumpInput(location string, storageType v1alpha1.BackupStorageType) string {
	if storageType == v1alpha1.BackupStorageS3 {
		return strings.TrimRight(location, "/")
	}
	return ensureTrailingSlash(location)
}

// isS3Location heuristically detects when a MysqlBackup.status.location
// points at an S3 bucket. Used as a fallback when the CR does not carry
// the explicit StorageType field (old CRs). util.dumpInstance() stores
// S3 outputs as a bare prefix (e.g. "lion/seed/") rather than an
// "s3://" URL, so we have to infer from the absence of a local
// filesystem path.
func isS3Location(loc string) bool {
	if loc == "" {
		return false
	}
	if strings.HasPrefix(loc, "s3://") {
		return true
	}
	if strings.HasPrefix(loc, "/") || strings.HasPrefix(loc, "pvc://") {
		return false
	}
	// Relative prefix with no leading slash => S3-style.
	return true
}

// splitS3Location turns an S3 MysqlBackup.status.location into the
// bucket it names and the object-key prefix inside that bucket, without
// surrounding slashes. mysqlsh dumps record a bare key prefix
// ("lion/nightly-abc"), for which bucket is "". The encrypt-upload
// container records a URL ("s3://bucket/lion/nightly-abc/"), which must
// not be passed through as a key prefix: S3-compatible stores reject it
// with InvalidArgument.
func splitS3Location(loc string) (bucket, key string) {
	if rest, ok := strings.CutPrefix(loc, "s3://"); ok {
		bucket, key, _ = strings.Cut(rest, "/")
	} else {
		key = loc
	}
	return bucket, strings.Trim(key, "/")
}

// restoreInFlight reports whether spec.initFromBackup is set and the
// one-shot restore has not yet reached Succeeded (pending, running, or
// failed). Callers use it to gate side effects that would race the restore
// Job (notably the topology manager's fresh-deploy auto-clone) or copy a
// failed load's partial data, and to keep applications off the group: no
// DNS record, no primary/healthy pod labels and no endpoints in the
// application Services until the restore succeeds.
func restoreInFlight(fg *v1alpha1.MysqlFailoverGroup) bool {
	if fg.Spec.InitFromBackup == nil {
		return false
	}
	if fg.Status.Restore == nil {
		return true
	}
	return fg.Status.Restore.Phase != v1alpha1.BackupPhaseSucceeded
}

// initRestoreSeedSite returns the site an initFromBackup restore that has
// not succeeded loads into, as the persisted status records it: the
// restore Job's target once the Job exists (the operator only creates it
// against the seed it published and observed writable), else the published
// active site. "" when no restore is unfinished or no seed is recorded.
//
// The Job's target is preferred because it outlives status.activeSite,
// which reads "" as soon as a status write observes the seed fenced. It is
// the authority the topology manager uses to lift such a fence after an
// operator restart, when the seed already holds part of the load and is
// no longer a fresh-deploy seed.
func initRestoreSeedSite(fg *v1alpha1.MysqlFailoverGroup) string {
	if !restoreInFlight(fg) {
		return ""
	}
	if rs := fg.Status.Restore; rs != nil && rs.JobName != "" && rs.TargetSite != "" {
		return rs.TargetSite
	}
	return fg.Status.ActiveSite
}

// initRestoreRecovery is the recovery procedure for a failed
// initFromBackup restore, shared by the RestoreFailed Event and the Ready
// condition so both say the same thing.
const initRestoreRecovery = "The group stays out of service (no DNS record, no Service endpoints) and no site is cloned. " +
	"To retry: if the load had started (BLOODRAVEN_LOAD_START in the Job log), empty the target first by deleting and recreating the group, " +
	"or by deleting the group's MySQL PVCs; then delete the restore Job and the operator rebuilds it"

// initRestoreReadyCondition returns the Ready=False reason and message for
// a group whose initFromBackup restore has not succeeded, or "" when the
// restore succeeded or none is configured.
func initRestoreReadyCondition(fg *v1alpha1.MysqlFailoverGroup) (reason, message string) {
	if !restoreInFlight(fg) {
		return "", ""
	}
	rs := fg.Status.Restore
	if rs != nil && rs.Phase == v1alpha1.BackupPhaseFailed {
		detail := rs.Message
		if rs.Reason != "" {
			detail = rs.Reason + ": " + detail
		}
		if rs.JobName == "" {
			return v1alpha1.ReasonInitFromBackupFailed, fmt.Sprintf(
				"initFromBackup restore failed before its Job was created: %s. The group stays out of service (no DNS record, no Service endpoints); the operator retries building the Job on each reconcile once the cause is fixed.", detail)
		}
		return v1alpha1.ReasonInitFromBackupFailed, fmt.Sprintf("initFromBackup restore failed (Job %s): %s. %s.", rs.JobName, detail, initRestoreRecovery)
	}
	phase := "Pending"
	if rs != nil && rs.Phase != "" {
		phase = string(rs.Phase)
	}
	return v1alpha1.ReasonInitFromBackupInProgress, fmt.Sprintf(
		"initFromBackup restore has not succeeded (phase %s); applications are kept off the group (no DNS record, no Service endpoints) until it does", phase)
}

type restoreCompletionMetadata struct {
	SourceSizeBytes    int64
	SourceGtidExecuted string
	SourceBinlogFile   string
	SourceBinlogPos    int64
	TargetGtidExecuted string
	TargetBinlogFile   string
	TargetBinlogPos    int64
	// PointInTime is the certified replay result, only ever set from a
	// PITR Job's verdict (pitrJobVerdict), never from the log.
	PointInTime *v1alpha1.PointInTimeResult
}

// pitrPartial reports whether meta certifies a Partial point-in-time outcome.
func (m restoreCompletionMetadata) pitrPartial() bool {
	return m.PointInTime != nil && m.PointInTime.Outcome == v1alpha1.PointInTimePartial
}

// Keep restoreCompletionMetadata, the status copy helpers below, and the
// BLOODRAVEN_RESTORE_COMPLETE parser/script fields in lockstep.
func restoreMetadataFromBackupStatus(status v1alpha1.MysqlBackupStatus) restoreCompletionMetadata {
	return restoreCompletionMetadata{
		SourceSizeBytes:    status.SizeBytes,
		SourceGtidExecuted: status.GtidExecuted,
		SourceBinlogFile:   status.BinlogFile,
		SourceBinlogPos:    status.BinlogPos,
	}
}

func (r *MysqlFailoverGroupReconciler) restoreSourceMetadata(ctx context.Context, fg *v1alpha1.MysqlFailoverGroup, src v1alpha1.InitFromBackupSource) restoreCompletionMetadata {
	if src.MysqlBackupRef == nil {
		return restoreCompletionMetadata{}
	}
	var ref v1alpha1.MysqlBackup
	if err := r.Get(ctx, types.NamespacedName{Namespace: fg.Namespace, Name: src.MysqlBackupRef.Name}, &ref); err != nil {
		return restoreCompletionMetadata{}
	}
	return restoreMetadataFromBackupStatus(ref.Status)
}

func applyRestoreMetadataToStatus(rs *v1alpha1.RestoreStatus, meta restoreCompletionMetadata) {
	rs.SourceSizeBytes = meta.SourceSizeBytes
	rs.SourceGtidExecuted = meta.SourceGtidExecuted
	rs.SourceBinlogFile = meta.SourceBinlogFile
	rs.SourceBinlogPos = meta.SourceBinlogPos
	rs.TargetGtidExecuted = meta.TargetGtidExecuted
	rs.TargetBinlogFile = meta.TargetBinlogFile
	rs.TargetBinlogPos = meta.TargetBinlogPos
	rs.PointInTime = meta.PointInTime
	if meta.pitrPartial() {
		rs.Reason = "PointInTimePartial"
	}
}

func applyRestoreMetadataToInPlaceStatus(rs *v1alpha1.RestoreInPlaceStatus, meta restoreCompletionMetadata) {
	rs.SourceSizeBytes = meta.SourceSizeBytes
	rs.SourceGtidExecuted = meta.SourceGtidExecuted
	rs.SourceBinlogFile = meta.SourceBinlogFile
	rs.SourceBinlogPos = meta.SourceBinlogPos
	rs.TargetGtidExecuted = meta.TargetGtidExecuted
	rs.TargetBinlogFile = meta.TargetBinlogFile
	rs.TargetBinlogPos = meta.TargetBinlogPos
	rs.PointInTime = meta.PointInTime
}

func restoreMetadataFromInPlaceStatus(rs *v1alpha1.RestoreInPlaceStatus) restoreCompletionMetadata {
	if rs == nil {
		return restoreCompletionMetadata{}
	}
	return restoreCompletionMetadata{
		SourceSizeBytes:    rs.SourceSizeBytes,
		SourceGtidExecuted: rs.SourceGtidExecuted,
		SourceBinlogFile:   rs.SourceBinlogFile,
		SourceBinlogPos:    rs.SourceBinlogPos,
		TargetGtidExecuted: rs.TargetGtidExecuted,
		TargetBinlogFile:   rs.TargetBinlogFile,
		TargetBinlogPos:    rs.TargetBinlogPos,
		PointInTime:        rs.PointInTime,
	}
}

func parseRestoreCompleteLine(line string) (restoreCompletionMetadata, bool) {
	const prefix = "BLOODRAVEN_RESTORE_COMPLETE"
	if !strings.HasPrefix(line, prefix) {
		return restoreCompletionMetadata{}, false
	}
	var meta restoreCompletionMetadata
	for _, f := range strings.Fields(strings.TrimSpace(strings.TrimPrefix(line, prefix))) {
		eq := strings.IndexByte(f, '=')
		if eq <= 0 {
			continue
		}
		key := f[:eq]
		rawVal := f[eq+1:]
		switch key {
		case "sourceSizeBytes":
			if n, err := strconv.ParseInt(rawVal, 10, 64); err == nil {
				meta.SourceSizeBytes = n
			}
		case "sourceGtidExecuted":
			if val, ok := decodeRestoreToken(rawVal); ok {
				meta.SourceGtidExecuted = strings.ReplaceAll(val, " ", "")
			}
		case "sourceBinlogFile":
			if val, ok := decodeRestoreToken(rawVal); ok {
				meta.SourceBinlogFile = val
			}
		case "sourceBinlogPos":
			if n, err := strconv.ParseInt(rawVal, 10, 64); err == nil {
				meta.SourceBinlogPos = n
			}
		case "targetGtidExecuted":
			if val, ok := decodeRestoreToken(rawVal); ok {
				meta.TargetGtidExecuted = strings.ReplaceAll(val, " ", "")
			}
		case "targetBinlogFile":
			if val, ok := decodeRestoreToken(rawVal); ok {
				meta.TargetBinlogFile = val
			}
		case "targetBinlogPos":
			if n, err := strconv.ParseInt(rawVal, 10, 64); err == nil {
				meta.TargetBinlogPos = n
			}
		}
	}
	return meta, true
}

func decodeRestoreToken(raw string) (string, bool) {
	val, err := url.QueryUnescape(raw)
	if err != nil {
		return "", false
	}
	return val, true
}

// tailRestoreCompletion reads best-effort GTID coordinates from the main
// container log. It never decides a Job's outcome.
func (r *MysqlFailoverGroupReconciler) tailRestoreCompletion(ctx context.Context, job *batchv1.Job) (restoreCompletionMetadata, bool) {
	if r.Clientset == nil {
		return restoreCompletionMetadata{}, false
	}
	pods, err := restoreJobPods(ctx, r.Client, job)
	if err != nil {
		return restoreCompletionMetadata{}, false
	}
	var pod *corev1.Pod
	for i := range pods {
		p := &pods[i]
		if p.Status.Phase == corev1.PodSucceeded && podBelongsToJob(p, job) {
			pod = p
			break
		}
	}
	if pod == nil {
		return restoreCompletionMetadata{}, false
	}
	candidates := []string{backupJobContainerName}
	if len(pod.Spec.Containers) > 0 && pod.Spec.Containers[0].Name != backupJobContainerName {
		candidates = append(candidates, pod.Spec.Containers[0].Name)
	}
	for _, container := range candidates {
		if meta, ok := r.tailRestoreContainer(ctx, job.Namespace, pod.Name, container); ok {
			return meta, true
		}
	}
	return restoreCompletionMetadata{}, false
}

func restoreJobPods(ctx context.Context, c client.Client, job *batchv1.Job) ([]corev1.Pod, error) {
	if job == nil {
		return nil, nil
	}
	var out []corev1.Pod
	selectors := []client.MatchingLabels{
		{"batch.kubernetes.io/job-name": job.Name},
		{"job-name": job.Name},
	}
	for _, selector := range selectors {
		var pods corev1.PodList
		if err := c.List(ctx, &pods, client.InNamespace(job.Namespace), selector); err != nil {
			return nil, err
		}
		out = append(out, pods.Items...)
	}
	return out, nil
}

func podBelongsToJob(pod *corev1.Pod, job *batchv1.Job) bool {
	if pod == nil || job == nil {
		return false
	}
	for _, owner := range pod.OwnerReferences {
		if owner.Kind == "Job" && owner.UID != "" && job.UID != "" {
			return owner.UID == job.UID
		}
	}

	if pod.Labels["batch.kubernetes.io/job-name"] == job.Name || pod.Labels["job-name"] == job.Name {
		return true
	}
	for _, owner := range pod.OwnerReferences {
		if job.UID != "" && owner.UID == job.UID {
			return true
		}
	}
	return false
}

func (r *MysqlFailoverGroupReconciler) tailRestoreContainer(ctx context.Context, namespace, podName, container string) (restoreCompletionMetadata, bool) {
	req := r.Clientset.CoreV1().Pods(namespace).GetLogs(podName, &corev1.PodLogOptions{
		Container: container,
		TailLines: ptr64(50),
	})
	stream, err := req.Stream(ctx)
	if err != nil {
		return restoreCompletionMetadata{}, false
	}
	defer func() { _ = stream.Close() }()

	var (
		last restoreCompletionMetadata
		ok   bool
	)
	sc := bufio.NewScanner(stream)
	sc.Buffer(make([]byte, 64*1024), 16*1024*1024)
	for sc.Scan() {
		if meta, hit := parseRestoreCompleteLine(sc.Text()); hit {
			last = meta
			ok = true
		}
	}
	if err := sc.Err(); err != nil && err != io.EOF {
		log.FromContext(ctx).Error(err, "tail restore completion log", "pod", podName, "container", container)
		return restoreCompletionMetadata{}, false
	}
	return last, ok
}

func emitRestoreSuccessMetrics(fg *v1alpha1.MysqlFailoverGroup, restoreKind, targetSite string, sourceSizeBytes int64, start *metav1.Time, completion metav1.Time) {
	if fg == nil || targetSite == "" {
		return
	}
	labels := []string{fg.Namespace, fg.Name, restoreKind, targetSite}
	if start != nil && completion.Time.After(start.Time) {
		bmetrics.RestoreDurationSeconds.WithLabelValues(labels...).Observe(completion.Time.Sub(start.Time).Seconds())
	}
	bmetrics.RestoreLastSuccessTimestamp.WithLabelValues(labels...).Set(float64(completion.Unix()))
	if sourceSizeBytes > 0 {
		bmetrics.RestoreLastSourceSizeBytes.WithLabelValues(labels...).Set(float64(sourceSizeBytes))
	} else {
		bmetrics.RestoreLastSourceSizeBytes.DeleteLabelValues(labels...)
	}
}

// restoreTargetSite chooses the site that will receive a bootstrap
// restore load: status.activeSite once the operator has published one, or,
// before that, the site the fresh-deploy bootstrap will seed (the default
// seed site: the highest-priority primary-candidate according to
// spec.splitBrainPolicy.sitePriorities, or the first primary-candidate in
// declared order, falling back to spec.sites[0]).
//
// ready reports whether the restore Job may start now. The load needs a
// writable server, and every site boots fenced (read_only=ON, plus the
// sidecar's super_read_only until the operator names it active), so the
// Job waits until the operator has seeded the target (topology manager
// maybeStartFreshDeployBootstrap, which only seeds while the restore is in
// flight) and the target is observed writable. Starting earlier fails the
// load with ERROR 1290, and a failed restore Job is never retried.
//
// When ready is false, wait is the Pending message, and refuse is true
// when the published active site is in a state no seeding will fix
// (unreachable, unknown): the operator can't accidentally overwrite a
// recovering site with a stale dump, and the user should look.
func restoreTargetSite(fg *v1alpha1.MysqlFailoverGroup) (site string, ready bool, wait string, refuse bool) {
	if fg == nil || len(fg.Spec.Sites) == 0 {
		return "", false, "no sites configured", true
	}

	active := fg.Status.ActiveSite
	if active == "" {
		seed := fg.Spec.Sites[0].Name
		if s := fg.Spec.DefaultSeedSite(); s != nil {
			seed = s.Name
		}
		return seed, false, fmt.Sprintf("waiting for the operator to seed site %q (make it writable and publish it as the active site)", seed), false
	}
	for i := range fg.Status.Sites {
		s := &fg.Status.Sites[i]
		if s.Name != active {
			continue
		}
		switch s.State {
		case state.StateWritable.String():
			return active, true, "", false
		case "", state.StateReadOnly.String():
			// The seed is published before the poll that observes it
			// writable, so read-only here is normally that window.
			return active, false, fmt.Sprintf("waiting for active site %q to be observed writable", active), false
		}
		return active, false, fmt.Sprintf("waiting for a writable target site (activeSite=%q is %s)", active, s.State), true
	}
	return active, false, fmt.Sprintf("waiting for active site %q to be observed writable", active), false
}

// reconcileRestoreJob creates and observes the one-shot restore Job when
// spec.initFromBackup is set. The returned duration is non-zero when the
// caller should requeue and NOT proceed to the pod-label sync / topology
// gate in the main reconcile loop.
func (r *MysqlFailoverGroupReconciler) reconcileRestoreJob(ctx context.Context, fg *v1alpha1.MysqlFailoverGroup) (time.Duration, error) {
	if fg.Spec.InitFromBackup == nil {
		// Clear stale restore status if the user removed the field.
		if fg.Status.Restore != nil {
			patch := client.MergeFrom(fg.DeepCopy())
			fg.Status.Restore = nil
			_ = r.Status().Patch(ctx, fg, patch)
		}
		return 0, nil
	}
	if fg.Status.Restore != nil && fg.Status.Restore.Phase == v1alpha1.BackupPhaseSucceeded {
		return 0, nil
	}

	if len(fg.Spec.Sites) == 0 {
		return 0, fmt.Errorf("initFromBackup set but no sites configured")
	}

	target, ready, wait, refuse := restoreTargetSite(fg)

	// A Job already created is observed whatever its target's state is
	// now, against the site it was created for: a primary fenced or
	// failed over mid-load must not turn a running restore back into a
	// Pending one that loses its outcome.
	var job batchv1.Job
	targetSite := target
	jobKey := types.NamespacedName{Namespace: fg.Namespace, Name: restoreJobName(fg.Name, target)}
	if prev := fg.Status.Restore; prev != nil && prev.JobName != "" && prev.TargetSite != "" {
		targetSite = prev.TargetSite
		jobKey.Name = prev.JobName
	}
	if err := r.Get(ctx, jobKey, &job); err != nil {
		if !apierrors.IsNotFound(err) {
			return 0, fmt.Errorf("get restore job: %w", err)
		}
		return r.createRestoreJob(ctx, fg, target, ready, wait, refuse)
	}

	// Observe the Job.
	phase, message := jobPhase(&job, "restore")
	if phase == "" {
		return 15 * time.Second, nil
	}

	previous := fg.Status.Restore
	// A Job's terminal outcome is read once; a recorded failure is final.
	if previous != nil && previous.Phase == v1alpha1.BackupPhaseFailed && sameRestoreJob(previous, &job) {
		return 0, nil
	}
	pitr := jobHasPITR(&job)
	var pod *corev1.Pod
	if pitr {
		var err error
		if pod, err = latestJobPod(ctx, r.Client, &job); err != nil {
			return 0, fmt.Errorf("list restore Job pods: %w", err)
		}
	}
	if phase == v1alpha1.BackupPhaseFailed && pitr {
		if detail := pitrJobFailure(pod); detail != "" {
			message = detail
		}
	}

	now := metav1.Now()
	meta := r.restoreSourceMetadata(ctx, fg, fg.Spec.InitFromBackup.Source)
	if phase == v1alpha1.BackupPhaseSucceeded {
		// GTID coordinates stay best-effort log metadata and never decide
		// the outcome; a PITR Job is certified only by its verdict.
		if parsed, ok := r.tailRestoreCompletion(ctx, &job); ok {
			meta = parsed
		}
		if pitr {
			result, err := pitrJobVerdict(pod, &job)
			if err != nil {
				phase = v1alpha1.BackupPhaseFailed
				message = "PITRCoverageInvalid: " + err.Error()
				meta = r.restoreSourceMetadata(ctx, fg, fg.Spec.InitFromBackup.Source)
			} else {
				meta.PointInTime = result
			}
		}
	}

	rs := &v1alpha1.RestoreStatus{
		Phase:          phase,
		JobName:        job.Name,
		JobUID:         string(job.UID),
		TargetSite:     targetSite,
		StartTime:      job.Status.StartTime,
		CompletionTime: &now,
		Message:        message,
	}
	applyRestoreMetadataToStatus(rs, meta)
	if phase == v1alpha1.BackupPhaseFailed {
		rs.Reason = pitrFailureReason(message)
	}
	if meta.pitrPartial() {
		rs.Message = pointInTimePartialMessage(meta.PointInTime)
	}
	if rs.StartTime == nil {
		if fg.Status.Restore != nil && fg.Status.Restore.StartTime != nil {
			rs.StartTime = fg.Status.Restore.StartTime
		} else {
			rs.StartTime = &now
		}
	}
	patched := r.setRestoreStatus(ctx, fg, rs)

	if phase == v1alpha1.BackupPhaseSucceeded {
		if patched {
			emitRestoreSuccessMetrics(fg, "init_from_backup", targetSite, rs.SourceSizeBytes, rs.StartTime, now)
		}
		if !patched {
			// The outcome Events describe a recorded status.
			return 15 * time.Second, nil
		}
		emitPointInTimeEvents(r.Recorder, fg, meta.PointInTime)
		if meta.pitrPartial() {
			return 0, nil
		}
		r.Recorder.Eventf(fg, corev1.EventTypeNormal, v1alpha1.EventRestoreSucceeded,
			"restore Job %s completed: %s", job.Name, message)
		return 0, nil
	}

	if !patched {
		return 15 * time.Second, nil
	}
	r.Recorder.Eventf(fg, corev1.EventTypeWarning, v1alpha1.EventRestoreFailed,
		"restore Job %s failed: %s. %s", job.Name, rs.Message, initRestoreRecovery)
	// No automatic retry. Operator must delete the Job to re-trigger.
	return 0, nil
}

// createRestoreJob creates the restore Job against targetSite once
// restoreTargetSite reports it ready and its Deployment is rolled out, and
// records Pending with the reason it waits otherwise.
func (r *MysqlFailoverGroupReconciler) createRestoreJob(ctx context.Context, fg *v1alpha1.MysqlFailoverGroup, targetSite string, ready bool, wait string, refuse bool) (time.Duration, error) {
	if !ready {
		if refuse {
			r.Recorder.Eventf(fg, corev1.EventTypeWarning, v1alpha1.EventRestoreTargetUnavailable,
				"refusing to run restore: active site %q is not writable / not ready",
				fg.Status.ActiveSite)
		}
		r.setRestorePending(ctx, fg, targetSite, wait)
		if refuse {
			return 30 * time.Second, nil
		}
		return 10 * time.Second, nil
	}

	// Wait for the target site's Deployment to be fully rolled out
	// before creating the restore Job; mysqlsh util.loadDump() needs a
	// live server. We accept Generation<=1 (fresh deploy where the
	// ObservedGeneration may still lag), OR the standard rolled-out
	// check (ObservedGeneration>=Generation && UpdatedReplicas>=1 &&
	// ReadyReplicas>=1). This guards against firing a load against a
	// Deployment in the middle of a rolling update.
	deployName := resourceName(fg.Name, targetSite)
	var deploy appsv1.Deployment
	if err := r.Get(ctx, types.NamespacedName{Namespace: fg.Namespace, Name: deployName}, &deploy); err != nil {
		if apierrors.IsNotFound(err) {
			return 10 * time.Second, nil
		}
		return 0, fmt.Errorf("get target deployment: %w", err)
	}
	if !deploymentRolledOut(&deploy) {
		r.setRestorePending(ctx, fg, targetSite, "waiting for target MySQL Deployment to become ready")
		return 10 * time.Second, nil
	}

	// Ensure derived creds Secret for the restore Job.
	credsName := restoreCredsSecretName(fg.Name)
	if err := r.ensureRestoreCredsSecret(ctx, fg, credsName); err != nil {
		return 0, fmt.Errorf("ensure restore creds secret: %w", err)
	}

	sourceMeta := r.restoreSourceMetadata(ctx, fg, fg.Spec.InitFromBackup.Source)
	built, err := r.buildRestoreJob(ctx, fg, targetSite, credsName)
	// The archive's read binding is not usable yet (Δ22): a fixable state
	// (Pending, KeyMismatch, EncryptionMismatch, ListFailed, the binding
	// limit) waits in Pending, naming it; an archive that can never verify
	// fails with PITRArchiveBindingNotReady below.
	var notReady *errPITRBindingNotReady
	if errors.As(err, &notReady) && !notReady.terminal {
		r.setRestorePending(ctx, fg, targetSite, notReady.Error())
		return pitrReadBindingRequeue, nil
	}
	if err != nil {
		r.Recorder.Eventf(fg, corev1.EventTypeWarning, v1alpha1.EventRestoreBuildFailed, "%s", err.Error())
		r.setRestoreStatus(ctx, fg, &v1alpha1.RestoreStatus{
			Phase:      v1alpha1.BackupPhaseFailed,
			Reason:     pitrFailureReason(err.Error()),
			TargetSite: targetSite,
			Message:    err.Error(),
		})
		return 0, nil
	}
	if err := controllerutil.SetControllerReference(fg, built, r.Scheme); err != nil {
		return 0, fmt.Errorf("set restore job owner ref: %w", err)
	}
	if err := r.Create(ctx, built); err != nil {
		return 0, fmt.Errorf("create restore job: %w", err)
	}
	now := metav1.Now()
	running := &v1alpha1.RestoreStatus{
		Phase:      v1alpha1.BackupPhaseRunning,
		JobName:    built.Name,
		JobUID:     string(built.UID),
		TargetSite: targetSite,
		StartTime:  &now,
		Message:    "restore Job created",
	}
	applyRestoreMetadataToStatus(running, sourceMeta)
	r.setRestoreStatus(ctx, fg, running)
	r.Recorder.Eventf(fg, corev1.EventTypeNormal, v1alpha1.EventRestoreStarted,
		"created restore Job %s targeting site %s", built.Name, targetSite)
	return 15 * time.Second, nil
}

// setRestorePending records a Pending restore with msg, skipping the
// write when status.restore already says exactly that.
func (r *MysqlFailoverGroupReconciler) setRestorePending(ctx context.Context, fg *v1alpha1.MysqlFailoverGroup, targetSite, msg string) {
	if prev := fg.Status.Restore; prev != nil && prev.Phase == v1alpha1.BackupPhasePending &&
		prev.TargetSite == targetSite && prev.Message == msg && prev.JobName == "" {
		return
	}
	r.setRestoreStatus(ctx, fg, &v1alpha1.RestoreStatus{
		Phase:      v1alpha1.BackupPhasePending,
		TargetSite: targetSite,
		Message:    msg,
	})
}

// deploymentRolledOut returns true when a Deployment is fully rolled
// out and has at least one ready replica. Generation<=1 is a fresh
// deploy where ObservedGeneration may briefly lag; we accept as ready
// if ReadyReplicas>=1 in that case.
func deploymentRolledOut(deploy *appsv1.Deployment) bool {
	if deploy.Status.ReadyReplicas < 1 {
		return false
	}
	if deploy.Generation <= 1 {
		return true
	}
	if deploy.Status.ObservedGeneration < deploy.Generation {
		return false
	}
	if deploy.Status.UpdatedReplicas < 1 {
		return false
	}
	return true
}

// setRestoreStatus patches fg.status.restore. IsNotFound is treated as
// a no-op (the CR was deleted out from under us) and other errors are
// logged but not propagated: this function is called from multiple
// places inside reconcileRestoreJob and we want the main Job-observation
// path to keep running rather than crash-looping the reconciler on a
// transient status write failure.
func (r *MysqlFailoverGroupReconciler) setRestoreStatus(ctx context.Context, fg *v1alpha1.MysqlFailoverGroup, rs *v1alpha1.RestoreStatus) bool {
	patch := client.MergeFrom(fg.DeepCopy())
	fg.Status.Restore = rs
	if err := r.Status().Patch(ctx, fg, patch); err != nil && !apierrors.IsNotFound(err) {
		log.FromContext(ctx).Error(err, "update restore status",
			"fg", fg.Name, "phase", rs.Phase)
		return false
	} else if apierrors.IsNotFound(err) {
		return false
	}
	return true
}

// ensureRestoreCredsSecret creates or updates the derived Secret used by
// the restore Job. Restore Jobs run as the operator account, not the
// backup role: they DROP and CREATE schemas, set global variables
// (local_infile) and, for a full-instance in-place restore, must keep
// working under the offline_mode fence, none of which the backup role's
// grants allow.
func (r *MysqlFailoverGroupReconciler) ensureRestoreCredsSecret(ctx context.Context, fg *v1alpha1.MysqlFailoverGroup, secretName string) error {
	operatorSecretName := fg.Spec.Credentials.OperatorSecret
	var srcSecret corev1.Secret
	if err := r.Get(ctx, types.NamespacedName{Namespace: fg.Namespace, Name: operatorSecretName}, &srcSecret); err != nil {
		return fmt.Errorf("get credential secret %s: %w", operatorSecretName, err)
	}

	user := string(srcSecret.Data["username"])
	password := string(srcSecret.Data["password"])
	if user == "" {
		return fmt.Errorf("secret %s has empty username", operatorSecretName)
	}

	derived := &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{
			Name:      secretName,
			Namespace: fg.Namespace,
		},
	}
	_, err := controllerutil.CreateOrUpdate(ctx, r.Client, derived, func() error {
		if err := controllerutil.SetControllerReference(fg, derived, r.Scheme); err != nil {
			return err
		}
		if derived.Labels == nil {
			derived.Labels = map[string]string{}
		}
		derived.Labels[v1alpha1.FailoverGroupLabel] = fg.Name
		derived.Labels[labelManagedBy] = managerName
		derived.Labels[v1alpha1.ResourceLabel] = "restore"
		derived.Type = corev1.SecretTypeOpaque
		derived.Data = map[string][]byte{
			"MYSQL_USER":     []byte(user),
			"MYSQL_PASSWORD": []byte(password),
		}
		return nil
	})
	return err
}

// restoreJobInputs captures the knobs that differ between a bootstrap
// restore (spec.initFromBackup) and an in-place restore
// (spec.restoreInPlace). Both paths build the same underlying Job
// shape; only the source, load options, job name, and a handful of
// extra env vars (drop-schemas, reset-replication, PITR database
// filter) vary.
type restoreJobInputs struct {
	JobName     string
	TargetSite  string
	CredsName   string
	Source      v1alpha1.InitFromBackupSource
	LoadOptions *v1alpha1.LoadOptions
	PointInTime *v1alpha1.PointInTimeSpec
	// Decryption, when set, provides the passphrase Secret the restore
	// will use to decrypt the source artifact. When unset, the
	// resolver falls back to the profile's own PassphraseSecret when
	// the source is a MysqlBackupRef pointing at an encrypted backup.
	Decryption *v1alpha1.BackupDecryptionSpec
	// FieldPath is the root CR field name used in error messages
	// ("initFromBackup" for bootstrap restore, "restoreInPlace" for
	// in-place restore). The shared builder formats
	// "<FieldPath>.source.…" so operators see the field they actually
	// need to edit. Defaults to "initFromBackup" when empty.
	FieldPath string
	// ExtraEnv is appended to the container env verbatim. Used by
	// in-place restore to inject BLOODRAVEN_DROP_ALL_USER_SCHEMAS,
	// BLOODRAVEN_DROP_SCHEMAS, BLOODRAVEN_RESET_REPLICATION and
	// BLOODRAVEN_REQUIRE_OFFLINE_MODE.
	ExtraEnv []corev1.EnvVar
	// FreshTarget marks a restore into an empty instance with no GTID
	// history of its own (bootstrap). Only then may the load install the
	// dump's GTID set (see dumpGtidBaselineLoadOptions). In-place restores
	// target a live primary and must leave it false.
	FreshTarget bool
}

// buildRestoreJob resolves the initFromBackup source and constructs the
// one-shot batchv1.Job used by the bootstrap restore path. The Job shape
// mirrors BuildBackupJob: creds are mounted as files rather than
// injected via envFrom, the container runs with the hardened pod- and
// container-level security context defaults from mergeSecurityContexts,
// and writable mysqlsh home + /tmp emptyDirs are attached so
// ReadOnlyRootFilesystem=true is compatible with mysqlsh.
func (r *MysqlFailoverGroupReconciler) buildRestoreJob(ctx context.Context, fg *v1alpha1.MysqlFailoverGroup, targetSite, credsName string) (*batchv1.Job, error) {
	return r.buildRestoreJobSpec(ctx, fg, restoreJobInputs{
		JobName:     restoreJobName(fg.Name, targetSite),
		TargetSite:  targetSite,
		CredsName:   credsName,
		Source:      fg.Spec.InitFromBackup.Source,
		LoadOptions: fg.Spec.InitFromBackup.LoadOptions,
		PointInTime: fg.Spec.InitFromBackup.PointInTime,
		Decryption:  fg.Spec.InitFromBackup.Decryption,
		FreshTarget: true,
	})
}

// backupRefProfileStorageError reports why a mysqlBackupRef's profile can
// no longer locate the backup's artifact on the backend it was written to
// (want), or returns nil when profile.Storage carries that backend's
// configuration. A non-nil profile with a nil error guarantees the
// matching profile.Storage.S3 / profile.Storage.PVC pointer is non-nil.
func backupRefProfileStorageError(fp string, ref *v1alpha1.MysqlBackup, profile *v1alpha1.BackupProfile, want v1alpha1.BackupStorageType) error {
	kind, field := "an S3", "s3"
	if want == v1alpha1.BackupStoragePVC {
		kind, field = "a PVC", "pvc"
	}
	switch {
	case profile == nil:
		return fmt.Errorf(
			"%s.source.mysqlBackupRef=%q resolves to %s location (%q) but profile %q is missing from spec.backup.profiles; "+
				"either restore the profile or set %s.source.%s explicitly",
			fp, ref.Name, kind, ref.Status.Location, ref.Spec.ProfileName, fp, field)
	case profile.Storage.Type != want:
		return fmt.Errorf(
			"%s.source.mysqlBackupRef=%q resolves to %s location (%q) but profile %q now stores to %s; "+
				"either restore the profile's %s storage or set %s.source.%s explicitly",
			fp, ref.Name, kind, ref.Status.Location, ref.Spec.ProfileName, profile.Storage.Type, want, fp, field)
	case want == v1alpha1.BackupStorageS3 && profile.Storage.S3 == nil,
		want == v1alpha1.BackupStoragePVC && profile.Storage.PVC == nil:
		return fmt.Errorf(
			"%s.source.mysqlBackupRef=%q resolves to %s location (%q) but profile %q has no storage.%s configuration; "+
				"either restore the profile's %s storage or set %s.source.%s explicitly",
			fp, ref.Name, kind, ref.Status.Location, ref.Spec.ProfileName, field, want, fp, field)
	}
	return nil
}

// inPlacePreflightEnv lists the destructive restore.py preflight switches
// only an in-place restore of a live primary sets.
var inPlacePreflightEnv = []string{
	"BLOODRAVEN_DROP_SCHEMAS",
	"BLOODRAVEN_DROP_ALL_USER_SCHEMAS",
	"BLOODRAVEN_RESET_REPLICATION",
}

// restoreLoadOptionsJSON returns the util.loadDump() options for a
// restore Job. A PITR restore into a fresh target installs the dump's
// GTID set first (marshalDumpGtidBaselineLoadOptions): the archived
// binlogs replayed afterwards keep their original GTIDs and include
// transactions the dump already contains, which MySQL skips only when
// their GTIDs are already in gtid_executed. Every other restore keeps
// the plain serialization. A live target (in-place) never gets the
// baseline: its own executed history already holds those GTIDs, and
// "replace" requires the dump set to be disjoint from it.
func restoreLoadOptionsJSON(fp string, in restoreJobInputs) (string, error) {
	if in.FreshTarget {
		for _, e := range in.ExtraEnv {
			if slices.Contains(inPlacePreflightEnv, e.Name) {
				return "", fmt.Errorf("internal error: fresh-target restore Job carries in-place preflight %s", e.Name)
			}
		}
	}
	if !in.FreshTarget || in.PointInTime == nil {
		return marshalLoadOptions(in.LoadOptions)
	}
	opts, err := marshalDumpGtidBaselineLoadOptions(in.LoadOptions)
	if errors.Is(err, errDumpGtidBaselineNeedsSkipBinlog) {
		return "", fmt.Errorf(
			"%s.loadOptions.skipBinlog=false is incompatible with %s.pointInTime: the binlog replay needs the dump's "+
				"GTID set installed on the target, and MySQL Shell pairs that with an unlogged load (a logged load would "+
				"also binlog and archive the whole dump under new local GTIDs); remove skipBinlog or set it to true",
			fp, fp)
	}
	return opts, err
}

// buildRestoreJobSpec is the shared core of both bootstrap and in-place
// restore Job construction. See restoreJobInputs for the per-caller
// parameters.
func (r *MysqlFailoverGroupReconciler) buildRestoreJobSpec(ctx context.Context, fg *v1alpha1.MysqlFailoverGroup, in restoreJobInputs) (*batchv1.Job, error) {
	src := in.Source
	targetSite := in.TargetSite
	credsName := in.CredsName
	fp := in.FieldPath
	if fp == "" {
		fp = "initFromBackup"
	}

	var (
		inputURL       string
		extraEnv       []corev1.EnvVar
		extraVolumes   []corev1.Volume
		extraMounts    []corev1.VolumeMount
		awsCredsSecret string
		// Encryption-aware fields, populated when the source is an
		// encrypted MysqlBackup or a direct S3/PVC source where the
		// caller supplied a decryption passphrase. decryptSourceKind
		// is "" when no decrypt step is needed.
		decryptSourceKind         string // "" | "S3" | "PVC"
		decryptS3Bucket           string
		decryptS3EndpointOverride string
		decryptS3Region           string
		decryptS3Prefix           string
		decryptAWSCredsSecret     string
		decryptPVCClaim           string
		decryptPVCPrefix          string
		// Resolved passphrase Secret reference for the source (may
		// come from in.Decryption or from the source profile itself).
		passphraseRef *v1alpha1.PassphraseSecretRef
		sourceMeta    restoreCompletionMetadata
		// dumpSrc is where dump-meta reads the dump's identity; set only
		// for a point-in-time restore.
		dumpSrc *pitrDumpSource
	)

	// A point-in-time target that does not parse, or a schema-filtered
	// load, refuses before anything is built (§7.1 reconcile tokens).
	pitrReq, err := pitrRequestFromSpec(in.PointInTime)
	if err != nil {
		return nil, err
	}
	if pitrReq != nil {
		if err := pitrLoadFilterRefusal(fp, in.LoadOptions); err != nil {
			return nil, err
		}
	}

	switch {
	case src.MysqlBackupRef != nil:
		var ref v1alpha1.MysqlBackup
		if err := r.Get(ctx, types.NamespacedName{Namespace: fg.Namespace, Name: src.MysqlBackupRef.Name}, &ref); err != nil {
			return nil, fmt.Errorf("get referenced mysqlbackup: %w", err)
		}
		if ref.Status.Phase != v1alpha1.BackupPhaseSucceeded || ref.Status.Location == "" {
			return nil, fmt.Errorf("referenced mysqlbackup %s is not Succeeded or has no location", ref.Name)
		}
		// D-44 (a target before the dump ended) is enforced by dump-meta
		// against the dump's own serverClock.end. status.completionTime is
		// no bound: it includes the upload, which can end long after the
		// dump did, so it would refuse valid targets in between (F034).
		sourceMeta = restoreMetadataFromBackupStatus(ref.Status)

		// Prefer the structured StorageType set by the backup
		// reconciler; fall back to the old heuristic for pre-upgrade
		// CRs that don't carry it.
		wantsS3 := ref.Status.StorageType == v1alpha1.BackupStorageS3
		wantsPVC := ref.Status.StorageType == v1alpha1.BackupStoragePVC
		if ref.Status.StorageType == "" {
			wantsS3 = isS3Location(ref.Status.Location)
			wantsPVC = !wantsS3
		}
		if wantsS3 {
			inputURL = mysqlShellDumpInput(ref.Status.Location, v1alpha1.BackupStorageS3)
		} else {
			inputURL = mysqlShellDumpInput(ref.Status.Location, v1alpha1.BackupStoragePVC)
		}

		// Resolve the backend the backup was written to from the profile
		// before anything reads its storage block: the profile may have
		// been deleted or moved to the other backend since the backup ran,
		// and an explicit decryption Secret must not bypass that check.
		profile := findProfile(fg, ref.Spec.ProfileName)
		var (
			profileS3       *v1alpha1.S3Storage
			profilePVCClaim string
		)
		switch {
		case wantsS3:
			if err := backupRefProfileStorageError(fp, &ref, profile, v1alpha1.BackupStorageS3); err != nil {
				return nil, err
			}
			profileS3 = profile.Storage.S3
		case wantsPVC:
			if err := backupRefProfileStorageError(fp, &ref, profile, v1alpha1.BackupStoragePVC); err != nil {
				return nil, err
			}
			profilePVCClaim = profile.Storage.PVC.ClaimName
			if profilePVCClaim == "" {
				profilePVCClaim = ownedBackupPVCName(fg.Name, profile.Name)
			}
		}

		if ref.Status.Encrypted {
			// The dump is decrypted with an explicit decryption Secret,
			// else the key the backup recorded (its dump-key snapshot),
			// never the profile's live Secret (Δ17, #203): that may hold
			// other bytes since the backup ran.
			switch {
			case in.Decryption != nil && in.Decryption.PassphraseSecret.Name != "":
				ref := in.Decryption.PassphraseSecret
				passphraseRef = &ref
			case ref.Status.ArtifactStorage != nil && ref.Status.ArtifactStorage.PassphraseSecret != nil:
				recorded := ref.Status.ArtifactStorage.PassphraseSecret
				var key corev1.Secret
				if err := r.Get(ctx, types.NamespacedName{Namespace: fg.Namespace, Name: recorded.Name}, &key); err != nil {
					if !apierrors.IsNotFound(err) {
						return nil, fmt.Errorf("get recorded passphrase secret %s: %w", recorded.Name, err)
					}
					return nil, fmt.Errorf(
						"%s: %s.source.mysqlBackupRef=%q is encrypted and the Secret %q it recorded as its dump key is gone; "+
							"restore that Secret or set %s.decryption.passphraseSecret",
						pitrarchive.TokenBackupDumpKeyMissing, fp, ref.Name, recorded.Name, fp)
				}
				passphraseRef = &v1alpha1.PassphraseSecretRef{Name: recorded.Name, Key: recorded.Key}
			default:
				return nil, fmt.Errorf(
					"%s: %s.source.mysqlBackupRef=%q is encrypted but recorded no dump key; "+
						"set %s.decryption.passphraseSecret",
					pitrarchive.TokenBackupDumpKeyMissing, fp, ref.Name, fp)
			}
			if profileS3 != nil {
				decryptSourceKind = "S3"
				decryptS3Bucket = profileS3.Bucket
				decryptS3EndpointOverride = profileS3.EndpointURL
				decryptS3Region = profileS3.Region
				var locBucket string
				locBucket, decryptS3Prefix = splitS3Location(ref.Status.Location)
				if locBucket != "" {
					decryptS3Bucket = locBucket
				}
				decryptAWSCredsSecret = profileS3.CredentialsSecret
			} else if profilePVCClaim != "" {
				decryptSourceKind = "PVC"
				decryptPVCClaim = profilePVCClaim
				// inputURL is the in-pod absolute path the backup
				// wrote to (e.g. "/backups/<backup-name>"). Derive a
				// storage-relative prefix from the PVC mount path
				// the decrypt init container will use.
				decryptPVCPrefix = strings.TrimSuffix(strings.TrimPrefix(inputURL, backupPVCMountPath+"/"), "/")
			}
		}
		if pitrReq != nil {
			ds, err := pitrDumpSourceForBackup(fg, &ref, profile, passphraseRef)
			if err != nil {
				return nil, wrapTokenFirst(fp+".source.mysqlBackupRef", err)
			}
			dumpSrc = &ds
		}
		if profileS3 != nil {
			extraEnv = append(extraEnv,
				corev1.EnvVar{Name: "BLOODRAVEN_S3_BUCKET", Value: profileS3.Bucket},
			)
			if profileS3.EndpointURL != "" {
				extraEnv = append(extraEnv, corev1.EnvVar{
					Name: "BLOODRAVEN_S3_ENDPOINT_OVERRIDE", Value: profileS3.EndpointURL,
				})
			}
			if profileS3.Region != "" {
				extraEnv = append(extraEnv, corev1.EnvVar{Name: "AWS_REGION", Value: profileS3.Region})
			}
			awsCredsSecret = profileS3.CredentialsSecret
		} else if profilePVCClaim != "" {
			// MysqlBackup.status.location stores the in-pod dump path that the
			// backup Job used. For PVC-backed backups that path lives under
			// /backups, so the restore Job must mount the same claim there.
			extraVolumes = append(extraVolumes, corev1.Volume{
				Name: "restore-src",
				VolumeSource: corev1.VolumeSource{
					PersistentVolumeClaim: &corev1.PersistentVolumeClaimVolumeSource{
						ClaimName: profilePVCClaim,
						ReadOnly:  true,
					},
				},
			})
			extraMounts = append(extraMounts, corev1.VolumeMount{
				Name: "restore-src", MountPath: backupPVCMountPath, ReadOnly: true,
			})
		}

	case src.S3 != nil:
		// One spelling of the prefix for loading, decryption and
		// dump-meta, the one the backup writer uses ("/prod" and "prod"
		// name the same keys, F036).
		s3Prefix := strings.Trim(src.S3.Prefix, "/")
		inputURL = mysqlShellDumpInput(s3Prefix, v1alpha1.BackupStorageS3)
		extraEnv = append(extraEnv,
			corev1.EnvVar{Name: "BLOODRAVEN_S3_BUCKET", Value: src.S3.Bucket},
		)
		if src.S3.EndpointURL != "" {
			extraEnv = append(extraEnv, corev1.EnvVar{
				Name: "BLOODRAVEN_S3_ENDPOINT_OVERRIDE", Value: src.S3.EndpointURL,
			})
		}
		if src.S3.Region != "" {
			extraEnv = append(extraEnv, corev1.EnvVar{Name: "AWS_REGION", Value: src.S3.Region})
		}
		awsCredsSecret = src.S3.CredentialsSecret

		if in.Decryption != nil && in.Decryption.PassphraseSecret.Name != "" {
			ref := in.Decryption.PassphraseSecret
			passphraseRef = &ref
			decryptSourceKind = "S3"
			decryptS3Bucket = src.S3.Bucket
			decryptS3EndpointOverride = src.S3.EndpointURL
			decryptS3Region = src.S3.Region
			decryptS3Prefix = s3Prefix
			decryptAWSCredsSecret = src.S3.CredentialsSecret
		}
		if pitrReq != nil {
			dumpSrc = &pitrDumpSource{
				StorageType: v1alpha1.BackupStorageS3, Prefix: s3Prefix,
				S3Bucket: src.S3.Bucket, S3EndpointURL: src.S3.EndpointURL, S3Region: src.S3.Region,
				AWSCredsSecret: src.S3.CredentialsSecret, Passphrase: passphraseRef,
			}
		}

	case src.PVC != nil:
		claim := src.PVC.ClaimName
		if claim == "" {
			return nil, fmt.Errorf(
				"%s.source.pvc.claimName is required; the restore source PVC must be created out of band and populated with the dump",
				fp)
		}
		if err := checkPVCSubPath(src.PVC.SubPath); err != nil {
			return nil, fmt.Errorf("%s.source.pvc: %w", fp, err)
		}
		mountPath := "/restore"
		sub := strings.TrimLeft(src.PVC.SubPath, "/")
		inputURL = mountPath
		if sub != "" {
			inputURL = mountPath + "/" + sub
		}
		extraVolumes = append(extraVolumes, corev1.Volume{
			Name: "restore-src",
			VolumeSource: corev1.VolumeSource{
				PersistentVolumeClaim: &corev1.PersistentVolumeClaimVolumeSource{
					ClaimName: claim,
					ReadOnly:  true,
				},
			},
		})
		extraMounts = append(extraMounts, corev1.VolumeMount{
			Name: "restore-src", MountPath: mountPath, ReadOnly: true,
		})

		if in.Decryption != nil && in.Decryption.PassphraseSecret.Name != "" {
			ref := in.Decryption.PassphraseSecret
			passphraseRef = &ref
			decryptSourceKind = "PVC"
			decryptPVCClaim = claim
			decryptPVCPrefix = strings.TrimPrefix(sub, "/")
		}
		if pitrReq != nil {
			dumpSrc = &pitrDumpSource{StorageType: v1alpha1.BackupStoragePVC, Prefix: strings.Trim(sub, "/"), PVCClaim: claim, Passphrase: passphraseRef}
		}

	default:
		return nil, fmt.Errorf("%s.source must set mysqlBackupRef, s3, or pvc", fp)
	}

	loadOptsJSON, err := restoreLoadOptionsJSON(fp, in)
	if err != nil {
		return nil, err
	}

	// PITR replay: dump-meta identifies the dump, pitr-download opens the
	// source archive, plans and stages the replay, and restore.py replays
	// and proves it. See buildRestorePITRFragments for the shape.
	if err := validateRestoreArchiveProfile(fg, in.PointInTime); err != nil {
		return nil, err
	}
	pitrFrags, err := buildRestorePITRFragmentsFor(fg, pitrReq)
	if err != nil {
		return nil, err
	}
	var dumpMeta *corev1.Container
	if pitrReq != nil {
		if dumpSrc == nil {
			return nil, fmt.Errorf("internal error: %s.pointInTime: no dump source rendered", fp)
		}
		container, volumes, err := buildDumpMetaInitContainer(fg, *dumpSrc)
		if err != nil {
			return nil, wrapTokenFirst(fp+".pointInTime", err)
		}
		dumpMeta = &container
		extraVolumes = append(extraVolumes, volumes...)
		if dumpSrc.ExpectedSourceUID != "" {
			extraEnv = append(extraEnv, corev1.EnvVar{Name: "BLOODRAVEN_PITR_EXPECTED_SOURCE_UID", Value: string(dumpSrc.ExpectedSourceUID)})
		}
	}
	extraEnv = append(extraEnv, pitrFrags.MainEnv...)
	extraVolumes = append(extraVolumes, pitrFrags.PodVolumes...)
	extraMounts = append(extraMounts, pitrFrags.MainMounts...)

	labels := map[string]string{
		labelAppName:                "mysql-restore",
		labelInstance:               fg.Name,
		v1alpha1.FailoverGroupLabel: fg.Name,
		v1alpha1.SiteLabel:          targetSite,
		labelManagedBy:              managerName,
		v1alpha1.ResourceLabel:      "restore",
	}
	if pitrFrags.KeySecretName != "" {
		// Correlation label (Δ18): a release of this archive's binding
		// sees the Job and refuses while it runs.
		labels[v1alpha1.PITRKeySecretLabel] = pitrFrags.KeySecretName
	}

	env := []corev1.EnvVar{
		{Name: "BLOODRAVEN_MYSQL_HOST", Value: backupMySQLHost(fg, targetSite)},
		{Name: "BLOODRAVEN_INPUT_URL", Value: literalArtifactEnvValue(inputURL)},
		{Name: "BLOODRAVEN_LOAD_OPTIONS", Value: loadOptsJSON},
		// Disable mysqlsh load-progress tracking. Its default progressFile
		// is written beside the dump: that fails on every read-only local
		// source (source PVCs and the decrypted staging dir), and for an
		// unencrypted S3 source it puts restore state into the backup
		// prefix, where a later load of the same dump would resume from
		// it and skip data an in-place restore has just dropped. Keeping
		// restore state out of backup storage costs resume: a bootstrap
		// restore that fails part-way loads the whole dump again, so its
		// target has to be emptied first (documented on the restore page).
		{Name: "BLOODRAVEN_LOAD_PROGRESS_FILE", Value: ""},
		{Name: "BLOODRAVEN_MYSQL_CREDS_DIR", Value: backupCredsMountPath},
		{Name: "BLOODRAVEN_SOURCE_SIZE_BYTES", Value: strconv.FormatInt(sourceMeta.SourceSizeBytes, 10)},
		{Name: "BLOODRAVEN_SOURCE_GTID_EXECUTED", Value: sourceMeta.SourceGtidExecuted},
		{Name: "BLOODRAVEN_SOURCE_BINLOG_FILE", Value: sourceMeta.SourceBinlogFile},
		{Name: "BLOODRAVEN_SOURCE_BINLOG_POS", Value: strconv.FormatInt(sourceMeta.SourceBinlogPos, 10)},
		{Name: "HOME", Value: mysqlshHomeMountPath},
	}
	if fg.Spec.TLS != nil {
		env = append(env,
			corev1.EnvVar{Name: "BLOODRAVEN_TLS", Value: "1"},
			// Verifies both the mysqlsh session and the mysqlbinlog|mysql
			// PITR replay against the operator-mounted CA.
			corev1.EnvVar{Name: "BLOODRAVEN_TLS_CA_FILE", Value: mysqlTLSMountPath + "/ca.crt"},
		)
	}
	env = append(env, extraEnv...)
	env = append(env, in.ExtraEnv...)

	// MySQL creds mounted as files (mode 0400).
	volumes := []corev1.Volume{
		{
			Name: "mysql-creds",
			VolumeSource: corev1.VolumeSource{
				Secret: &corev1.SecretVolumeSource{
					SecretName:  credsName,
					DefaultMode: ptr32(0o400),
				},
			},
		},
		{
			Name: "scripts",
			VolumeSource: corev1.VolumeSource{
				ConfigMap: &corev1.ConfigMapVolumeSource{
					LocalObjectReference: corev1.LocalObjectReference{Name: backupScriptsConfigMapName(fg.Name)},
				},
			},
		},
		{
			Name:         "mysqlsh-home",
			VolumeSource: corev1.VolumeSource{EmptyDir: &corev1.EmptyDirVolumeSource{}},
		},
		{
			Name:         "tmp",
			VolumeSource: corev1.VolumeSource{EmptyDir: &corev1.EmptyDirVolumeSource{}},
		},
	}
	mounts := []corev1.VolumeMount{
		{Name: "mysql-creds", MountPath: backupCredsMountPath, ReadOnly: true},
		{Name: "scripts", MountPath: backupScriptsMountPath, ReadOnly: true},
		{Name: "mysqlsh-home", MountPath: mysqlshHomeMountPath},
		{Name: "tmp", MountPath: tmpMountPath},
	}

	if awsCredsSecret != "" {
		volumes = append(volumes, corev1.Volume{
			Name: "aws-creds",
			VolumeSource: corev1.VolumeSource{
				Secret: &corev1.SecretVolumeSource{
					SecretName:  awsCredsSecret,
					DefaultMode: ptr32(0o400),
				},
			},
		})
		mounts = append(mounts, corev1.VolumeMount{
			Name: "aws-creds", MountPath: backupAWSCredsMountPath, ReadOnly: true,
		})
		env = append(env, corev1.EnvVar{Name: "BLOODRAVEN_AWS_CREDS_DIR", Value: backupAWSCredsMountPath})
	}

	volumes = append(volumes, extraVolumes...)
	mounts = append(mounts, extraMounts...)

	if fg.Spec.TLS != nil {
		volumes = append(volumes, corev1.Volume{
			Name: "tls",
			VolumeSource: corev1.VolumeSource{
				Secret: &corev1.SecretVolumeSource{SecretName: fg.Spec.TLS.SecretName},
			},
		})
		mounts = append(mounts, corev1.VolumeMount{
			Name: "tls", MountPath: "/etc/mysql/tls", ReadOnly: true,
		})
	}

	image := backupImage(fg)

	activeDeadline := int64(7200)
	backoff := int32(0)
	var (
		pullSecrets    []corev1.LocalObjectReference
		podSCSrc       *corev1.PodSecurityContext
		containerSCSrc *corev1.SecurityContext
	)
	if fg.Spec.Backup != nil {
		if fg.Spec.Backup.ActiveDeadlineSeconds > 0 {
			activeDeadline = fg.Spec.Backup.ActiveDeadlineSeconds
		}
		pullSecrets = fg.Spec.Backup.ImagePullSecrets
		podSCSrc = fg.Spec.Backup.PodSecurityContext
		containerSCSrc = fg.Spec.Backup.ContainerSecurityContext
	}
	podSC, containerSC := mergeSecurityContexts(podSCSrc, containerSCSrc)

	// Init order (§6.3): dump-meta → pitr-download → decrypt-download.
	var initContainers []corev1.Container
	if dumpMeta != nil {
		initContainers = append(initContainers, *dumpMeta)
	}
	if pitrFrags.InitContainer != nil {
		initContainers = append(initContainers, *pitrFrags.InitContainer)
	}

	// -------- Decrypt init container + volume wiring -----------------
	//
	// When the source is encrypted we run `bloodraven decrypt-download`
	// as an additional init container. It downloads ciphertext from
	// S3 or the source PVC, decrypts into a shared emptyDir
	// (/restore-decrypted), and the main mysqlsh container then runs
	// loadDump against that local path instead of hitting storage
	// itself. This keeps the passphrase off the mysqlsh side and lets
	// mysqlsh's loadDump stay completely unaware of encryption.
	if passphraseRef != nil {
		if operatorImageFromEnv == "" {
			return nil, fmt.Errorf("%s: operator image is not configured (SetOperatorImageDefaults); required for encrypted restore", fp)
		}
		const decryptDir = "/restore-decrypted"
		decryptVolume := corev1.Volume{
			Name:         "restore-decrypted",
			VolumeSource: corev1.VolumeSource{EmptyDir: stagingEmptyDirSource(fg.Spec.Backup)},
		}
		passphraseVolume := corev1.Volume{
			Name: "backup-passphrase",
			VolumeSource: corev1.VolumeSource{
				Secret: &corev1.SecretVolumeSource{
					SecretName:  passphraseRef.Name,
					DefaultMode: ptr32(0o400),
					Items: []corev1.KeyToPath{
						{
							Key:  passphraseRef.PassphraseSecretKeyOrDefault(),
							Path: backupPassphraseFileName,
						},
					},
				},
			},
		}

		decEnv := []corev1.EnvVar{
			{Name: "BLOODRAVEN_TARGET_DIR", Value: decryptDir},
			{Name: "BLOODRAVEN_STORAGE_TYPE", Value: decryptSourceKind},
			{Name: "BLOODRAVEN_BACKUP_PASSPHRASE_FILE", Value: backupPassphraseMountPath + "/" + backupPassphraseFileName},
		}
		decMounts := []corev1.VolumeMount{
			{Name: "restore-decrypted", MountPath: decryptDir},
			{Name: "backup-passphrase", MountPath: backupPassphraseMountPath, ReadOnly: true},
			{Name: "tmp", MountPath: tmpMountPath},
		}

		switch decryptSourceKind {
		case "S3":
			decEnv = append(decEnv,
				corev1.EnvVar{Name: "BLOODRAVEN_SOURCE_PREFIX", Value: literalArtifactEnvValue(decryptS3Prefix)},
				corev1.EnvVar{Name: "BLOODRAVEN_S3_BUCKET", Value: decryptS3Bucket},
			)
			if decryptS3EndpointOverride != "" {
				decEnv = append(decEnv, corev1.EnvVar{
					Name: "BLOODRAVEN_S3_ENDPOINT_OVERRIDE", Value: decryptS3EndpointOverride,
				})
			}
			if decryptS3Region != "" {
				decEnv = append(decEnv, corev1.EnvVar{Name: "AWS_REGION", Value: decryptS3Region})
			}
			if decryptAWSCredsSecret != "" {
				// The init container reads the AWS creds from the same
				// volume that the main container would have used, so
				// no extra volume is needed — we just add the mount on
				// the init side. The main-container mount is dropped
				// below together with the S3 env.
				decMounts = append(decMounts, corev1.VolumeMount{
					Name: "aws-creds", MountPath: backupAWSCredsMountPath, ReadOnly: true,
				})
				decEnv = append(decEnv, corev1.EnvVar{
					Name: "BLOODRAVEN_AWS_CREDS_DIR", Value: backupAWSCredsMountPath,
				})
			}
		case "PVC":
			// Mount the source PVC read-only into the init container
			// under a dedicated path so the decrypt step doesn't
			// confuse filepath resolution with the mysqlsh restore-src
			// mount.
			const decPVCMount = "/restore-src-encrypted"
			volumes = append(volumes, corev1.Volume{
				Name: "decrypt-src",
				VolumeSource: corev1.VolumeSource{
					PersistentVolumeClaim: &corev1.PersistentVolumeClaimVolumeSource{
						ClaimName: decryptPVCClaim,
						ReadOnly:  true,
					},
				},
			})
			decMounts = append(decMounts, corev1.VolumeMount{
				Name: "decrypt-src", MountPath: decPVCMount, ReadOnly: true,
			})
			decEnv = append(decEnv,
				corev1.EnvVar{Name: "BLOODRAVEN_PVC_MOUNT_PATH", Value: decPVCMount},
				corev1.EnvVar{Name: "BLOODRAVEN_SOURCE_PREFIX", Value: literalArtifactEnvValue(decryptPVCPrefix)},
			)
		default:
			return nil, fmt.Errorf("%s: decryption is set but source storage kind is empty", fp)
		}

		_, initSC := mergeSecurityContexts(nil, nil)
		var initBackupSrc *backupResourcesSource
		if fg.Spec.Backup != nil {
			initBackupSrc = &backupResourcesSource{Resources: fg.Spec.Backup.Resources}
		}
		initResources := effectiveBackupResources(initBackupSrc, defaultInitContainerResources())
		initContainers = append(initContainers, corev1.Container{
			Name:                     "decrypt-download",
			TerminationMessagePolicy: corev1.TerminationMessageFallbackToLogsOnError,
			Image:                    operatorImageFromEnv,
			Command:                  operatorCommand("decrypt-download"),
			Env:                      decEnv,
			Resources:                initResources,
			VolumeMounts:             decMounts,
			SecurityContext:          initSC,
		})

		// Rewire the main mysqlsh container to read the decrypted
		// staging dir. Drop any storage-env that would have pointed
		// loadDump at the encrypted source, including the aws-creds
		// mount on the main container (mysqlsh reads local files now).
		newEnv := env[:0]
		for _, e := range env {
			switch e.Name {
			case "BLOODRAVEN_S3_BUCKET",
				"BLOODRAVEN_S3_ENDPOINT_OVERRIDE",
				"AWS_REGION",
				"BLOODRAVEN_AWS_CREDS_DIR":
				continue
			case "BLOODRAVEN_INPUT_URL":
				newEnv = append(newEnv, corev1.EnvVar{
					Name:  "BLOODRAVEN_INPUT_URL",
					Value: decryptDir,
				})
				continue
			}
			newEnv = append(newEnv, e)
		}
		env = newEnv

		// Drop restore-src from the main container mounts; it is no
		// longer the mysqlsh source. aws-creds stays on the pod
		// volumes list (the init container uses it) but we remove it
		// from the main container's mounts since mysqlsh no longer
		// needs S3 auth.
		mounts = filterOutMountByName(mounts, "restore-src")
		mounts = filterOutMountByName(mounts, "aws-creds")

		// Attach the decrypted staging volume + passphrase Secret.
		volumes = append(volumes, decryptVolume, passphraseVolume)
		mounts = append(mounts, corev1.VolumeMount{
			Name: "restore-decrypted", MountPath: decryptDir, ReadOnly: true,
		})
	}

	jobName := in.JobName
	if jobName == "" {
		jobName = restoreJobName(fg.Name, targetSite)
	}
	// Main mysqlsh container resources flow through from
	// spec.backup.resources when set; otherwise the helper returns the
	// defaultRestoreResources() floor (100m/128Mi) so the Job is admitted
	// on LimitRange-gated clusters even during a fresh-deploy bootstrap
	// when spec.backup is unset.
	var mainBackupSrc *backupResourcesSource
	if fg.Spec.Backup != nil {
		mainBackupSrc = &backupResourcesSource{Resources: fg.Spec.Backup.Resources}
	}
	mainResources := effectiveBackupResources(mainBackupSrc, defaultRestoreResources())

	job := &batchv1.Job{
		ObjectMeta: metav1.ObjectMeta{
			Name:      jobName,
			Namespace: fg.Namespace,
			Labels:    labels,
		},
		Spec: batchv1.JobSpec{
			ActiveDeadlineSeconds: &activeDeadline,
			BackoffLimit:          &backoff,
			Template: corev1.PodTemplateSpec{
				ObjectMeta: metav1.ObjectMeta{Labels: labels},
				Spec: corev1.PodSpec{
					RestartPolicy:    corev1.RestartPolicyNever,
					ImagePullSecrets: pullSecrets,
					SecurityContext:  podSC,
					// Restore Jobs (bootstrap and in-place) run mysqlsh
					// loadDump and the optional decrypt/pitr-download
					// init containers; none of them talk to the
					// Kubernetes API.
					AutomountServiceAccountToken: boolPtr(false),
					// A restore loads into the target site over its Service.
					// Evicting it mid-load because a failover tainted its
					// node would leave a half-dropped or half-loaded schema;
					// let it finish or fail on its own instead.
					Tolerations:    groupReadOnlyTolerations(fg.Name),
					InitContainers: initContainers,
					Containers: []corev1.Container{
						{
							Name: backupJobContainerName,
							// restore.py writes its PITR verdict and refusals here;
							// other failures fall back to the log tail.
							TerminationMessagePolicy: corev1.TerminationMessageFallbackToLogsOnError,
							Image:                    image,
							Command:                  []string{"mysqlsh", "--no-wizard", "--py", "-f", backupScriptsMountPath + "/restore.py"},
							Env:                      env,
							Resources:                mainResources,
							VolumeMounts:             mounts,
							SecurityContext:          containerSC,
						},
					},
					Volumes: volumes,
				},
			},
		},
	}
	setTerminationMessageLimit(&job.Spec.Template.Spec)
	return job, nil
}

// terminationMessageLimitEnv tells restore.py and verify.sh how many bytes
// of the main container's termination message the kubelet keeps, so the
// PITR verdict is built to fit rather than cut (review F040).
const terminationMessageLimitEnv = "BLOODRAVEN_TERMINATION_MESSAGE_LIMIT"

// setTerminationMessageLimit renders terminationMessageLimitEnv on the
// pod's main container: the kubelet divides 12 KiB of termination messages
// evenly among a pod's containers, init containers included, and keeps at
// most 4 KiB of each.
func setTerminationMessageLimit(spec *corev1.PodSpec) {
	n := len(spec.InitContainers) + len(spec.Containers)
	if n == 0 {
		return
	}
	limit := min(4096, 12288/n)
	for i := range spec.Containers {
		if spec.Containers[i].Name == backupJobContainerName {
			spec.Containers[i].Env = append(spec.Containers[i].Env, corev1.EnvVar{Name: terminationMessageLimitEnv, Value: strconv.Itoa(limit)})
		}
	}
}

func sameRestoreJob(previous *v1alpha1.RestoreStatus, job *batchv1.Job) bool {
	if previous == nil || previous.JobName != job.Name {
		return false
	}
	if previous.JobUID != "" && job.UID != "" {
		return previous.JobUID == string(job.UID)
	}
	return job.CreationTimestamp.IsZero() || (previous.CompletionTime != nil && !job.CreationTimestamp.After(previous.CompletionTime.Time))
}
