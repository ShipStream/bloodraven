package controller

import (
	"context"
	"fmt"
	"sort"
	"strings"
	"time"

	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/uuid"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/log"

	v1alpha1 "github.com/shipstream/bloodraven/api/v1alpha1"
)

// Restore holds (#275). A restore that reads a MysqlBackup through
// source.backupRef is a reader of that backup exactly as a verification is:
// retention, the artifact-cleanup finalizer and the PITR binlog cutoff leave
// the backup alone until the restore is Succeeded or Failed and its pods
// have stopped. The protocol mirrors the verification hold's:
//
//  1. pin: the restore records the backup's name and UID, and a hold ID
//     new to this attempt, in status.restore(InPlace).backupRef;
//  2. stamp: only then it annotates the backup
//     restore-hold.bloodraven.shipstream.io/<hold ID>=<group name>, with
//     the backup's resourceVersion as a precondition, and refuses a backup
//     that is not Succeeded, is being deleted, or is no longer the pinned
//     object. Once stamped, the hold outlasts a delete: the backup stays
//     Terminating and the restore goes on reading it;
//  3. only then is the restore Job created, from that backup (the Job
//     carries the UID it was built from, RestoreBackupUIDAnnotation);
//  4. release: once the terminal status is persisted and the group's
//     restore pods have stopped, the restore removes the annotation.
//
// A hold is live while the group's status records a restore that is not
// terminal under its hold ID, and afterwards (or when the group is gone)
// while restore pods of that group name still run. Because the pin is
// persisted before the stamp and every attempt takes a new hold ID, a hold
// never turns live again once it was seen dead, so a dead hold can be
// stripped at any time (stripDeadRestoreHolds, stripDeadHolds): nothing
// leaks when the operator restarts between any two steps, the group is
// deleted mid-restore, or a release is lost.

const (
	// restoreKindInit and restoreKindInPlace name a group's two restores
	// by their spec field.
	restoreKindInit    = "initFromBackup"
	restoreKindInPlace = "restoreInPlace"

	// restoreJobAppName is the app.kubernetes.io/name of every restore
	// Job and its pods (buildRestoreJobSpec).
	restoreJobAppName = "mysql-restore"

	// restoreHoldRetry is how soon a restore looks again when its hold
	// could not be confirmed yet (a concurrent write to the backup, or a
	// Job built from a cached copy that is behind the API server).
	restoreHoldRetry = 2 * time.Second

	// restoreHoldRecheck is how often a live backup that carries a restore
	// hold is looked at again, so a hold whose group was deleted is
	// stripped without waiting for the next event on the backup.
	restoreHoldRecheck = time.Minute
)

func restoreHoldKey(holdID string) string {
	return v1alpha1.RestoreHoldAnnotationPrefix + holdID
}

// BackupHasReadHold reports whether any verification or restore hold is
// recorded on the backup, live or not. The PITR cutoff uses it to keep the
// binlog coverage of a backup that is being deleted while it is read.
func BackupHasReadHold(b *v1alpha1.MysqlBackup) bool {
	for key := range b.Annotations {
		if strings.HasPrefix(key, v1alpha1.VerificationHoldAnnotationPrefix) ||
			strings.HasPrefix(key, v1alpha1.RestoreHoldAnnotationPrefix) {
			return true
		}
	}
	return false
}

func backupHasRestoreHold(b *v1alpha1.MysqlBackup) bool {
	for key := range b.Annotations {
		if strings.HasPrefix(key, v1alpha1.RestoreHoldAnnotationPrefix) {
			return true
		}
	}
	return false
}

// initRestoreTerminal reports whether an initFromBackup restore status is
// Succeeded or Failed. A Failed restore is retried (by deleting its Job,
// or on the next reconcile when the Job could not be built), but every
// retry is a new attempt that pins and holds its backup again.
func initRestoreTerminal(rs *v1alpha1.RestoreStatus) bool {
	return rs != nil && (rs.Phase == v1alpha1.BackupPhaseSucceeded || rs.Phase == v1alpha1.BackupPhaseFailed)
}

// restoreRun is one restore of a group, as far as backup holds go.
type restoreRun struct {
	kind string
	// ref is the pinned backup; nil while the run has not pinned one.
	ref *v1alpha1.RestoreBackupRef
	// specName is spec.<kind>.source.backupRef.name: "" for another
	// source, or when the spec was removed.
	specName string
	// terminal says the run is Succeeded or Failed.
	terminal bool
	// jobCreated says the run's Job exists, or may.
	jobCreated bool
}

// groupRestoreRuns returns the group's restores as its spec and status
// record them. An in-place run is described by its status once it has one:
// removing or editing spec.restoreInPlace does not stop a running Job. A
// terminal in-place status whose spec carries a newer confirm is still
// terminal here; the run that confirm starts pins and holds its backup in
// Preflight, before anything is fenced.
func groupRestoreRuns(fg *v1alpha1.MysqlFailoverGroup) []restoreRun {
	var runs []restoreRun
	if spec := fg.Spec.InitFromBackup; spec != nil {
		run := restoreRun{kind: restoreKindInit}
		if spec.Source.BackupRef != nil {
			run.specName = spec.Source.BackupRef.Name
		}
		if rs := fg.Status.Restore; rs != nil {
			run.ref, run.terminal, run.jobCreated = rs.BackupRef, initRestoreTerminal(rs), rs.JobName != ""
		}
		runs = append(runs, run)
	}
	spec, cur := fg.Spec.RestoreInPlace, fg.Status.RestoreInPlace
	if spec != nil || cur != nil {
		run := restoreRun{kind: restoreKindInPlace}
		if spec != nil && spec.Source.BackupRef != nil {
			run.specName = spec.Source.BackupRef.Name
		}
		if cur != nil {
			run.ref, run.terminal, run.jobCreated = cur.BackupRef, !inPlaceRestoreInFlight(fg), cur.JobName != "" || cur.JobCreating
		}
		runs = append(runs, run)
	}
	return runs
}

// restorePin returns the backup the group's restore of kind has pinned,
// while that restore is not terminal. A terminal restore's pin belongs to
// an attempt that is over: the next attempt pins again.
func restorePin(fg *v1alpha1.MysqlFailoverGroup, kind string) *v1alpha1.RestoreBackupRef {
	for _, run := range groupRestoreRuns(fg) {
		if run.kind == kind && !run.terminal {
			return run.ref
		}
	}
	return nil
}

// sameRestorePinState reports whether a and b agree on what decides a hold
// for the restore of kind: whether it is terminal, and what it pinned.
func sameRestorePinState(a, b *v1alpha1.MysqlFailoverGroup, kind string) bool {
	state := func(fg *v1alpha1.MysqlFailoverGroup) (terminal bool, ref v1alpha1.RestoreBackupRef) {
		for _, run := range groupRestoreRuns(fg) {
			if run.kind == kind {
				if run.ref != nil {
					ref = *run.ref
				}
				return run.terminal, ref
			}
		}
		return false, ref
	}
	aTerminal, aRef := state(a)
	bTerminal, bRef := state(b)
	return aTerminal == bTerminal && aRef == bRef
}

// groupRestoreReads reports whether a restore of fg may read backup, from
// the group's spec and status alone (stamped holds are checked by
// restoreHoldLive). A pinned restore reads exactly the object it pinned:
// the UID decides, so a backup created again under the same name is not
// protected by a restore of its predecessor.
//
// A restore that has not pinned yet is matched by name. Without a Job it is
// a prospective reader of a backup it could still pin (pickable: live and
// Succeeded; holdRestoreBackup refuses any other). With a Job it was started
// by an operator that did not pin, and the name is all that is known, so it
// also keeps a backup that is already being deleted.
func groupRestoreReads(fg *v1alpha1.MysqlFailoverGroup, backup *v1alpha1.MysqlBackup, pickable bool) bool {
	for _, run := range groupRestoreRuns(fg) {
		if run.terminal {
			continue
		}
		if run.ref != nil {
			if run.ref.Name == backup.Name && run.ref.UID == string(backup.UID) {
				return true
			}
			continue
		}
		if run.specName == backup.Name && (run.jobCreated || pickable) {
			return true
		}
	}
	return false
}

// restoreExecutionStopped reports whether no restore Job of the group named
// group can still read a backup: each one is gone or carries an explicit
// Complete or Failed condition (after which the Job controller starts no
// pods), and none of the group's restore pods is pending or running. Pods
// are checked directly because Job status counters are updated
// asynchronously.
//
// It does not tell the group's restores apart: a later restore of the same
// group name that is still running keeps an earlier restore's ended hold
// live until it stops too. That only ever keeps a backup longer.
func restoreExecutionStopped(ctx context.Context, reader client.Reader, namespace, group string) (bool, error) {
	selector := client.MatchingLabels{labelAppName: restoreJobAppName, v1alpha1.FailoverGroupLabel: group}
	var jobs batchv1.JobList
	if err := reader.List(ctx, &jobs, client.InNamespace(namespace), selector); err != nil {
		return false, fmt.Errorf("list restore jobs of group %s: %w", group, err)
	}
	for i := range jobs.Items {
		if !jobFinished(&jobs.Items[i]) {
			return false, nil
		}
	}
	var pods corev1.PodList
	if err := reader.List(ctx, &pods, client.InNamespace(namespace), selector); err != nil {
		return false, fmt.Errorf("list restore pods of group %s: %w", group, err)
	}
	for i := range pods.Items {
		if phase := pods.Items[i].Status.Phase; phase != corev1.PodSucceeded && phase != corev1.PodFailed {
			return false, nil
		}
	}
	return true, nil
}

// restoreHoldLive reports whether the restore hold holdID that group
// stamped on backup still protects it: the group's status records a
// restore that is not terminal under that hold ID, or restore pods of that
// group name still run (a terminal restore whose pods have not stopped, or
// a group deleted mid-restore).
func restoreHoldLive(ctx context.Context, reader client.Reader, backup *v1alpha1.MysqlBackup, holdID, group string) (bool, error) {
	var fg v1alpha1.MysqlFailoverGroup
	err := reader.Get(ctx, types.NamespacedName{Namespace: backup.Namespace, Name: group}, &fg)
	switch {
	case err == nil:
		for _, run := range groupRestoreRuns(&fg) {
			if !run.terminal && run.ref != nil && run.ref.HoldID == holdID && run.ref.UID == string(backup.UID) {
				return true, nil
			}
		}
	case !apierrors.IsNotFound(err):
		return false, fmt.Errorf("get failover group %s: %w", group, err)
	}
	stopped, err := restoreExecutionStopped(ctx, reader, backup.Namespace, group)
	return !stopped, err
}

// readHoldLive reports whether annotation key=value on backup is a read
// hold (a verification's or a restore's) and, when it is, whether the hold
// is live.
func readHoldLive(ctx context.Context, reader client.Reader, backup *v1alpha1.MysqlBackup, key, value string) (hold, live bool, err error) {
	if uid, ok := strings.CutPrefix(key, v1alpha1.VerificationHoldAnnotationPrefix); ok {
		live, err = holdLive(ctx, reader, backup.Namespace, uid, value)
		return true, live, err
	}
	if holdID, ok := strings.CutPrefix(key, v1alpha1.RestoreHoldAnnotationPrefix); ok {
		live, err = restoreHoldLive(ctx, reader, backup, holdID, value)
		return true, live, err
	}
	return false, false, nil
}

// describeReaders names the readers of a backup for an Event or a
// condition message: "verification a, b", "the restore of group g", or
// both joined by "and".
func describeReaders(refs []artifactReader) string {
	verifications, restores := splitReaders(refs)
	var parts []string
	if len(verifications) > 0 {
		parts = append(parts, "verification "+strings.Join(readerNames(verifications), ", "))
	}
	switch len(restores) {
	case 0:
	case 1:
		parts = append(parts, "the restore of group "+restores[0].Name)
	default:
		parts = append(parts, "the restores of groups "+strings.Join(readerNames(restores), ", "))
	}
	return strings.Join(parts, " and ")
}

// splitReaders separates the verifications from the restoring groups.
func splitReaders(refs []artifactReader) (verifications, restores []artifactReader) {
	for _, ref := range refs {
		if ref.Kind == readerRestore {
			restores = append(restores, ref)
		} else {
			verifications = append(verifications, ref)
		}
	}
	return verifications, restores
}

// stripDeadRestoreHolds removes the restore holds on a live backup whose
// restore can no longer read it: the restore ended and its release was
// lost, or the restoring group was deleted. A hold that is still live is
// left, and the returned duration says when to look again.
//
// A dead hold protects nothing (readers are decided by liveness, not by
// the annotation), so this only keeps the backup's metadata honest and
// lets the removal's update event start the retention pass that was
// deferred. The patch carries the resourceVersion this pass read, so a hold
// stamped since is never dropped.
func (r *MysqlBackupReconciler) stripDeadRestoreHolds(ctx context.Context, backup *v1alpha1.MysqlBackup) (time.Duration, error) {
	if !backupHasRestoreHold(backup) {
		return 0, nil
	}
	dead, err := deadHoldKeys(ctx, r.apiReader(), backup, v1alpha1.RestoreHoldAnnotationPrefix)
	if err != nil {
		return restoreHoldRecheck, err
	}
	if len(dead) > 0 {
		patch := client.MergeFromWithOptions(backup.DeepCopy(), client.MergeFromWithOptimisticLock{})
		for _, key := range dead {
			delete(backup.Annotations, key)
		}
		if err := r.Patch(ctx, backup, patch); err != nil {
			if apierrors.IsConflict(err) || apierrors.IsNotFound(err) {
				return restoreHoldRetry, nil
			}
			return restoreHoldRecheck, fmt.Errorf("remove dead restore holds: %w", err)
		}
		log.FromContext(ctx).Info("removed dead restore holds", "namespace", backup.Namespace, "backup", backup.Name, "holds", dead)
	}
	if backupHasRestoreHold(backup) {
		return restoreHoldRecheck, nil
	}
	return 0, nil
}

// restoreHoldReader is the uncached reader hold decisions are made with.
func (r *MysqlFailoverGroupReconciler) restoreHoldReader() client.Reader {
	if r.APIReader != nil {
		return r.APIReader
	}
	return r.Client
}

// holdRestoreBackup pins the MysqlBackup named name as the source of the
// group's restore of kind and stamps the restore's hold on it. It is
// idempotent, and it is called again right before the restore Job is
// created: a nil result then means that the backup, read uncached, is the
// pinned object, is Succeeded and carries this restore's hold. Retention
// deletes with the resourceVersion it read before it looked for readers,
// so a delete that raced the stamp either lost (it conflicts) or won (the
// stamp conflicts, and the next pass refuses a backup that is being
// deleted).
//
// A backup that is being deleted is refused unless it already carries this
// restore's hold. The stamp is a write conditioned on the resourceVersion
// of a backup that was not being deleted, so a hold found on a Terminating
// backup was there before the deletion: the delete lost. From the stamp on
// the hold is live (the pin is on a status that is not terminal, as the
// uncached read below confirms), and every path that releases a
// Terminating backup waits for its readers (finalize, retainArtifact). The
// hold exists so that the restore can finish; refusing here would end a
// restore, for an in-place run after its fence, over a backup that is
// still protected.
//
// builtFromUID is the backup UID the Job about to be created was built
// from (RestoreBackupUIDAnnotation), or "". The Job is built through the
// cache: when the cache still shows another object under this name the
// answer is retry, so the Job is never created from a backup other than
// the one held.
//
// The pin is decided from the group as the API server has it, not as this
// pass read it from the cache: a pass that still sees an attempt as
// unfinished after its terminal status was written would otherwise keep
// using that attempt's hold ID, which readers may already have seen dead.
// When the two disagree the answer is retry.
//
// It returns one of:
//
//   - refusal != "": the backup cannot be held (it is gone, not Succeeded,
//     being deleted without this restore's hold on it, or was replaced by
//     another object of the same name since this restore pinned it). The
//     caller ends the restore.
//   - retry: look again after restoreHoldRetry.
//   - err: an API error; nothing may be created.
func (r *MysqlFailoverGroupReconciler) holdRestoreBackup(ctx context.Context, fg *v1alpha1.MysqlFailoverGroup, kind, name, builtFromUID, targetSite string) (refusal string, retry bool, err error) {
	reader := r.restoreHoldReader()
	var live v1alpha1.MysqlFailoverGroup
	if err := reader.Get(ctx, client.ObjectKeyFromObject(fg), &live); err != nil {
		return "", false, fmt.Errorf("get failover group %s: %w", fg.Name, err)
	}
	if live.UID != fg.UID || !sameRestorePinState(fg, &live, kind) {
		return "", true, nil
	}
	var backup v1alpha1.MysqlBackup
	if err := reader.Get(ctx, types.NamespacedName{Namespace: fg.Namespace, Name: name}, &backup); err != nil {
		if apierrors.IsNotFound(err) {
			return "the backup is gone", false, nil
		}
		return "", false, fmt.Errorf("get referenced mysqlbackup %s: %w", name, err)
	}
	pin := restorePin(fg, kind)
	if pin != nil && pin.Name != name {
		// The source was edited before the Job exists (the Job is built
		// from the current spec): this run no longer reads the backup it
		// pinned first.
		if err := r.releaseRestoreHold(ctx, fg, pin); err != nil {
			log.FromContext(ctx).Error(err, "release restore hold of a replaced source", "fg", fg.Name, "backup", pin.Name)
		}
		pin = nil
	}
	held := pin != nil && pin.HoldID != "" && backup.Annotations[restoreHoldKey(pin.HoldID)] == fg.Name
	switch {
	case pin != nil && pin.UID != string(backup.UID):
		return "the backup was replaced by another backup of the same name after this restore pinned it", false, nil
	case backup.Status.Phase != v1alpha1.BackupPhaseSucceeded:
		return "the backup is not Succeeded", false, nil
	case !backup.DeletionTimestamp.IsZero() && !held:
		return "the backup is being deleted", false, nil
	case builtFromUID != "" && builtFromUID != string(backup.UID):
		return "", true, nil
	}
	if pin == nil || pin.HoldID == "" {
		pin = &v1alpha1.RestoreBackupRef{Name: backup.Name, UID: string(backup.UID), HoldID: string(uuid.NewUUID())}
		recorded, err := r.recordRestorePin(ctx, fg, &live, kind, pin, targetSite)
		if err != nil {
			return "", false, err
		}
		if !recorded {
			return "", true, nil
		}
	}
	key := restoreHoldKey(pin.HoldID)
	if backup.Annotations[key] == fg.Name {
		return "", false, nil
	}
	patch := client.MergeFromWithOptions(backup.DeepCopy(), client.MergeFromWithOptimisticLock{})
	if backup.Annotations == nil {
		backup.Annotations = map[string]string{}
	}
	backup.Annotations[key] = fg.Name
	if err := r.Patch(ctx, &backup, patch); err != nil {
		switch {
		case apierrors.IsConflict(err):
			return "", true, nil
		case apierrors.IsNotFound(err):
			return "the backup is gone", false, nil
		}
		return "", false, fmt.Errorf("stamp restore hold on mysqlbackup %s: %w", name, err)
	}
	return "", false, nil
}

// holdInPlaceRestoreBackup is holdRestoreBackup for the in-place run in
// flight, when its source is a backupRef. ended says the run was ended
// Failed because its backup cannot be held: nothing was restored, and the
// confirm counts as used, as for a Job that could not be built (a backup
// that turns up later under the awaited name must not start a destructive
// restore on its own). wait is the requeue when the hold could not be
// confirmed yet.
func (r *MysqlFailoverGroupReconciler) holdInPlaceRestoreBackup(ctx context.Context, fg *v1alpha1.MysqlFailoverGroup, builtFromUID string) (ended bool, wait time.Duration, err error) {
	spec, cur := fg.Spec.RestoreInPlace, fg.Status.RestoreInPlace
	if spec == nil || cur == nil || spec.Source.BackupRef == nil {
		return false, 0, nil
	}
	name := spec.Source.BackupRef.Name
	refusal, retry, err := r.holdRestoreBackup(ctx, fg, restoreKindInPlace, name, builtFromUID, cur.TargetSite)
	if err != nil {
		return false, 0, fmt.Errorf("hold the in-place restore's source backup: %w", err)
	}
	if retry {
		return false, restoreHoldRetry, nil
	}
	if refusal != "" {
		cur = fg.Status.RestoreInPlace
		r.stampTerminalFailure(ctx, fg, cur.TargetSite, cur.Scope, inPlaceRunConfirm(cur, spec),
			fmt.Sprintf("restoreInPlace.source.backupRef=%q cannot be held for the restore: %s; nothing was restored", name, refusal),
			v1alpha1.EventRestoreInPlaceBuildFailed)
		return true, 0, nil
	}
	return false, 0, nil
}

// recordRestorePin persists pin on the status of the group's restore of
// kind. The hold is stamped only after this write succeeded: a hold whose
// pin is not on the status would read as dead. recorded is false when the
// write lost a race and the caller should look again.
//
// An initFromBackup attempt starts here, Pending: the write replaces
// status.restore on the group as just read uncached (live), with its
// resourceVersion as a precondition, so what the API server holds
// afterwards is exactly a restore that is not terminal and carries the
// pin, whatever the cache showed. An in-place run adds the pin to its
// status with the optimistic update every in-place status write uses.
func (r *MysqlFailoverGroupReconciler) recordRestorePin(ctx context.Context, fg, live *v1alpha1.MysqlFailoverGroup, kind string, pin *v1alpha1.RestoreBackupRef, targetSite string) (recorded bool, err error) {
	switch kind {
	case restoreKindInit:
		base := live.DeepCopy()
		live.Status.Restore = &v1alpha1.RestoreStatus{
			Phase:      v1alpha1.BackupPhasePending,
			TargetSite: targetSite,
			Message:    fmt.Sprintf("holding MysqlBackup %s for the restore Job", pin.Name),
			BackupRef:  pin,
		}
		if err := r.Status().Patch(ctx, live, client.MergeFromWithOptions(base, client.MergeFromWithOptimisticLock{})); err != nil {
			if apierrors.IsConflict(err) {
				return false, nil
			}
			return false, fmt.Errorf("record the pinned MysqlBackup %s of %s before holding it: %w", pin.Name, kind, err)
		}
		fg.Status.Restore, fg.ResourceVersion = live.Status.Restore.DeepCopy(), live.ResourceVersion
		return true, nil
	case restoreKindInPlace:
		if cur := fg.Status.RestoreInPlace; cur != nil {
			next := cur.DeepCopy()
			next.BackupRef = pin
			if r.setInPlaceRestoreStatus(ctx, fg, next) {
				return true, nil
			}
		}
	}
	return false, fmt.Errorf("record the pinned MysqlBackup %s of %s before holding it", pin.Name, kind)
}

// releaseTerminalRestoreHold removes the hold a terminal restore of kind
// left on its pinned backup. It runs at the start of every pass, so the
// terminal status it reads was persisted by an earlier one. A release that
// cannot finish yet (pods still stopping, a conflict) is not waited for:
// the hold is dead once the pods stop, and the backup's reconciler strips
// it (stripDeadRestoreHolds).
func (r *MysqlFailoverGroupReconciler) releaseTerminalRestoreHold(ctx context.Context, fg *v1alpha1.MysqlFailoverGroup, kind string) {
	for _, run := range groupRestoreRuns(fg) {
		if run.kind != kind || !run.terminal || run.ref == nil || run.ref.HoldID == "" {
			continue
		}
		if err := r.releaseRestoreHold(ctx, fg, run.ref); err != nil {
			log.FromContext(ctx).Error(err, "release restore hold", "fg", fg.Name, "backup", run.ref.Name)
		}
	}
}

// releaseRestoreHold removes the hold ref names from its backup once no
// restore pod of the group runs. The cached look comes first so that a
// group whose restore ended long ago costs no API read per reconcile.
func (r *MysqlFailoverGroupReconciler) releaseRestoreHold(ctx context.Context, fg *v1alpha1.MysqlFailoverGroup, ref *v1alpha1.RestoreBackupRef) error {
	key := restoreHoldKey(ref.HoldID)
	nn := types.NamespacedName{Namespace: fg.Namespace, Name: ref.Name}
	var backup v1alpha1.MysqlBackup
	if err := r.Get(ctx, nn, &backup); err != nil {
		return client.IgnoreNotFound(err)
	}
	if _, held := backup.Annotations[key]; !held {
		return nil
	}
	reader := r.restoreHoldReader()
	stopped, err := restoreExecutionStopped(ctx, reader, fg.Namespace, fg.Name)
	if err != nil || !stopped {
		return err
	}
	if err := reader.Get(ctx, nn, &backup); err != nil {
		return client.IgnoreNotFound(err)
	}
	if _, held := backup.Annotations[key]; !held {
		return nil
	}
	patch := client.MergeFromWithOptions(backup.DeepCopy(), client.MergeFromWithOptimisticLock{})
	delete(backup.Annotations, key)
	if err := r.Patch(ctx, &backup, patch); err != nil && !apierrors.IsNotFound(err) && !apierrors.IsConflict(err) {
		return fmt.Errorf("release restore hold on mysqlbackup %s: %w", ref.Name, err)
	}
	return nil
}

// sortedReaders returns the readers, verifications first, each kind sorted
// by name.
func sortedReaders(verifications, restores map[string]string) []artifactReader {
	out := make([]artifactReader, 0, len(verifications)+len(restores))
	for name, uid := range verifications {
		out = append(out, artifactReader{Kind: readerVerification, Name: name, UID: uid})
	}
	for name, uid := range restores {
		out = append(out, artifactReader{Kind: readerRestore, Name: name, UID: uid})
	}
	sort.SliceStable(out, func(i, j int) bool {
		if out[i].Kind != out[j].Kind {
			return out[i].Kind == readerVerification
		}
		return out[i].Name < out[j].Name
	})
	return out
}
