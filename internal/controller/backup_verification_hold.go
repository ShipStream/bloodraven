package controller

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"strings"

	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/log"

	v1alpha1 "github.com/shipstream/bloodraven/api/v1alpha1"
)

// annotationVerificationHoldPrefix marks a MysqlBackup that a
// MysqlBackupVerification is reading. The full key is the prefix plus the
// verification's UID; the value is its name. Retention and the artifact
// cleanup finalizer leave a backup alone while any hold on it is live.
//
// The hold lives on the held object so that taking it and deleting the
// backup are ordered by the backup's resourceVersion: the verification
// stamps it with an optimistic lock, and retention deletes with a
// resourceVersion precondition, so exactly one of them wins.
const annotationVerificationHoldPrefix = "hold.verification.shipstream.io/"

var (
	errHoldBackupGone        = errors.New("backup is gone")
	errHoldBackupReplaced    = errors.New("backup was replaced by another object with the same name")
	errHoldBackupTerminating = errors.New("backup is being deleted")
)

func verificationHoldKey(v *v1alpha1.MysqlBackupVerification) string {
	return annotationVerificationHoldPrefix + string(v.UID)
}

// BackupHasVerificationHold reports whether any verification hold is
// recorded on the backup, live or not. The PITR cutoff uses it to keep the
// binlog coverage of a backup that is being deleted while it is read.
func BackupHasVerificationHold(b *v1alpha1.MysqlBackup) bool {
	for key := range b.Annotations {
		if strings.HasPrefix(key, annotationVerificationHoldPrefix) {
			return true
		}
	}
	return false
}

// verificationExecutionStopped reports whether nothing of the verification
// named name can still read its backup, and no pod labelled with the
// verification is pending or running. Pods are checked directly because Job
// status counters are updated asynchronously.
//
// With requireGone (a verification being deleted) the Job must be gone: a
// Job under foreground deletion can still have a pod creation in flight.
// Otherwise the Job may remain if it carries an explicit Complete or Failed
// condition, after which the Job controller starts no pods.
func verificationExecutionStopped(ctx context.Context, reader client.Reader, namespace, name string, requireGone bool) (bool, error) {
	var job batchv1.Job
	jobErr := reader.Get(ctx, types.NamespacedName{Namespace: namespace, Name: verificationJobName(name)}, &job)
	switch {
	case apierrors.IsNotFound(jobErr):
	case jobErr != nil:
		return false, fmt.Errorf("get verification job: %w", jobErr)
	case requireGone || !jobFinished(&job):
		return false, nil
	}
	var pods corev1.PodList
	if err := reader.List(ctx, &pods, client.InNamespace(namespace), client.MatchingLabels{labelMysqlBackupVerification: name}); err != nil {
		return false, fmt.Errorf("list verification pods: %w", err)
	}
	for i := range pods.Items {
		if phase := pods.Items[i].Status.Phase; phase != corev1.PodSucceeded && phase != corev1.PodFailed {
			return false, nil
		}
	}
	return true, nil
}

// jobFinished reports an explicit terminal Job condition. The counter
// fallback in jobPhase is not enough to know no pod will start.
func jobFinished(job *batchv1.Job) bool {
	for _, c := range job.Status.Conditions {
		if (c.Type == batchv1.JobComplete || c.Type == batchv1.JobFailed) && c.Status == corev1.ConditionTrue {
			return true
		}
	}
	return false
}

// verificationReading reports whether v may still read its backup. A live,
// non-terminal verification always may, even before its Job exists; a
// terminal or deleted one may until its execution has stopped.
func verificationReading(ctx context.Context, reader client.Reader, v *v1alpha1.MysqlBackupVerification) (bool, error) {
	if v.DeletionTimestamp.IsZero() && !verificationTerminal(v.Status.Phase) {
		return true, nil
	}
	stopped, err := verificationExecutionStopped(ctx, reader, v.Namespace, v.Name, !v.DeletionTimestamp.IsZero())
	return !stopped, err
}

// artifactReader identifies a verification that may still read a backup.
type artifactReader struct {
	Name string
	UID  string
}

// holdLive reports whether the hold recorded for verification name/uid
// still protects the backup. A holder that exists is live while it may be
// reading; a holder that is gone (or was replaced by a same-name successor)
// is live only while pods under that name still run.
func holdLive(ctx context.Context, reader client.Reader, namespace, uid, name string) (bool, error) {
	var v v1alpha1.MysqlBackupVerification
	err := reader.Get(ctx, types.NamespacedName{Namespace: namespace, Name: name}, &v)
	switch {
	case err == nil && string(v.UID) == uid:
		return verificationReading(ctx, reader, &v)
	case err == nil || apierrors.IsNotFound(err):
		stopped, serr := verificationExecutionStopped(ctx, reader, namespace, name, false)
		return !stopped, serr
	default:
		return false, fmt.Errorf("get verification %s: %w", name, err)
	}
}

// artifactReaders returns the names of the verifications that may still be
// reading the backup's artifact. See artifactReaderRefs.
func artifactReaders(ctx context.Context, reader client.Reader, backup *v1alpha1.MysqlBackup) ([]string, error) {
	refs, err := artifactReaderRefs(ctx, reader, backup)
	if err != nil {
		return nil, err
	}
	return readerNames(refs), nil
}

func readerNames(refs []artifactReader) []string {
	names := make([]string, 0, len(refs))
	for _, ref := range refs {
		names = append(names, ref.Name)
	}
	return names
}

func readerUIDs(refs []artifactReader) []string {
	uids := make([]string, 0, len(refs))
	for _, ref := range refs {
		uids = append(uids, ref.UID)
	}
	return uids
}

// artifactReaderRefs returns the verifications that may still be reading
// the backup's artifact, read through the uncached reader, sorted by name.
// Any API error is returned so that callers fail closed.
//
// It honours stamped holds and also scans verifications directly, which
// covers verifications started by an operator that did not stamp holds and
// the window before a new one stamps.
func artifactReaderRefs(ctx context.Context, reader client.Reader, backup *v1alpha1.MysqlBackup) ([]artifactReader, error) {
	var list v1alpha1.MysqlBackupVerificationList
	if err := reader.List(ctx, &list, client.InNamespace(backup.Namespace)); err != nil {
		return nil, fmt.Errorf("list verifications: %w", err)
	}
	return artifactReaderRefsFromList(ctx, reader, backup, &list)
}

// artifactReaderRefsFromList is artifactReaderRefs with the namespace's
// verifications already listed, so several backups share one List.
func artifactReaderRefsFromList(ctx context.Context, reader client.Reader, backup *v1alpha1.MysqlBackup, list *v1alpha1.MysqlBackupVerificationList) ([]artifactReader, error) {
	readers := map[string]string{}
	seen := map[types.UID]bool{}
	// An unpinned verification only ever pins a live Succeeded backup
	// (resolveBackup skips Terminating ones and stampVerificationHold
	// refuses them), so it is a prospective reader of nothing else.
	pickable := backup.DeletionTimestamp.IsZero() && backup.Status.Phase == v1alpha1.BackupPhaseSucceeded
	for key, name := range backup.Annotations {
		uid, ok := strings.CutPrefix(key, annotationVerificationHoldPrefix)
		if !ok {
			continue
		}
		seen[types.UID(uid)] = true
		live, err := holdLive(ctx, reader, backup.Namespace, uid, name)
		if err != nil {
			return nil, err
		}
		if live {
			readers[name] = uid
		}
	}

	for i := range list.Items {
		v := &list.Items[i]
		if _, found := readers[v.Name]; found || seen[v.UID] {
			continue
		}
		switch ref := v.Status.BackupRef; {
		case ref != nil && ref.UID == string(backup.UID):
			reading, err := verificationReading(ctx, reader, v)
			if err != nil {
				return nil, err
			}
			if reading {
				readers[v.Name] = string(v.UID)
			}
		case ref == nil && pickable && v.DeletionTimestamp.IsZero() && !verificationTerminal(v.Status.Phase) &&
			v.Spec.FailoverGroupRef.Name == backup.Spec.FailoverGroupRef.Name &&
			v.Spec.ProfileName == backup.Spec.ProfileName &&
			(v.Spec.BackupRef == nil || v.Spec.BackupRef.Name == "" || v.Spec.BackupRef.Name == backup.Name):
			// Not yet pinned: it may pick this backup.
			readers[v.Name] = string(v.UID)
		}
	}
	out := make([]artifactReader, 0, len(readers))
	for name, uid := range readers {
		out = append(out, artifactReader{Name: name, UID: uid})
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Name < out[j].Name })
	return out, nil
}

// deadHoldKeys returns the hold annotations on the backup whose holder can
// no longer read it (for example a verification whose finalizer was
// removed by hand). They would otherwise keep the PITR cutoff pinned.
func deadHoldKeys(ctx context.Context, reader client.Reader, backup *v1alpha1.MysqlBackup) ([]string, error) {
	var dead []string
	for key, name := range backup.Annotations {
		uid, ok := strings.CutPrefix(key, annotationVerificationHoldPrefix)
		if !ok {
			continue
		}
		live, err := holdLive(ctx, reader, backup.Namespace, uid, name)
		if err != nil {
			return nil, err
		}
		if !live {
			dead = append(dead, key)
		}
	}
	sort.Strings(dead)
	return dead, nil
}

// stripDeadHolds removes dead holds from a backup being deleted, with an
// optimistic lock so a hold stamped concurrently is never dropped. done is
// false when the patch lost a race and the caller should look again.
func (r *MysqlBackupReconciler) stripDeadHolds(ctx context.Context, backup *v1alpha1.MysqlBackup) (done bool, err error) {
	dead, err := deadHoldKeys(ctx, r.apiReader(), backup)
	if err != nil || len(dead) == 0 {
		return err == nil, err
	}
	patch := client.MergeFromWithOptions(backup.DeepCopy(), client.MergeFromWithOptimisticLock{})
	for _, key := range dead {
		delete(backup.Annotations, key)
	}
	if err := r.Patch(ctx, backup, patch); err != nil {
		if apierrors.IsConflict(err) {
			return false, nil
		}
		return false, fmt.Errorf("remove dead verification holds: %w", err)
	}
	log.FromContext(ctx).Info("removed dead verification holds", "namespace", backup.Namespace, "backup", backup.Name, "holds", dead)
	return true, nil
}

// BackupReadChecker answers whether verifications may still be reading
// backups, through an (uncached) reader. It lists each namespace's
// verifications once, so checking many backups costs one List plus the
// reads for the verifications that reference them. API errors are
// returned so that callers fail closed.
type BackupReadChecker struct {
	reader client.Reader
	lists  map[string]*v1alpha1.MysqlBackupVerificationList
}

// NewBackupReadChecker returns a checker for one decision (one request).
func NewBackupReadChecker(reader client.Reader) *BackupReadChecker {
	return &BackupReadChecker{reader: reader, lists: map[string]*v1alpha1.MysqlBackupVerificationList{}}
}

// BeingRead reports whether any verification may still read the backup.
func (c *BackupReadChecker) BeingRead(ctx context.Context, backup *v1alpha1.MysqlBackup) (bool, error) {
	readers, err := c.readers(ctx, backup)
	return len(readers) > 0, err
}

// readers returns the verifications that may still read the backup.
func (c *BackupReadChecker) readers(ctx context.Context, backup *v1alpha1.MysqlBackup) ([]artifactReader, error) {
	list, ok := c.lists[backup.Namespace]
	if !ok {
		list = &v1alpha1.MysqlBackupVerificationList{}
		if err := c.reader.List(ctx, list, client.InNamespace(backup.Namespace)); err != nil {
			return nil, fmt.Errorf("list verifications: %w", err)
		}
		c.lists[backup.Namespace] = list
	}
	return artifactReaderRefsFromList(ctx, c.reader, backup, list)
}

// stampVerificationHold records v's hold on its pinned backup. The patch
// carries the backup's resourceVersion, so it fails with a Conflict if the
// backup changed (including being marked for deletion) since it was read.
// allowTerminating is set for a verification that is already running: its
// hold still defers artifact cleanup and keeps the PITR coverage.
func (r *MysqlBackupVerificationReconciler) stampVerificationHold(ctx context.Context, v *v1alpha1.MysqlBackupVerification, allowTerminating bool) error {
	ref := v.Status.BackupRef
	if ref == nil || ref.Name == "" {
		return nil
	}
	var backup v1alpha1.MysqlBackup
	if err := r.apiReader().Get(ctx, types.NamespacedName{Namespace: v.Namespace, Name: ref.Name}, &backup); err != nil {
		if apierrors.IsNotFound(err) {
			return errHoldBackupGone
		}
		return fmt.Errorf("get pinned backup: %w", err)
	}
	if ref.UID != "" && string(backup.UID) != ref.UID {
		return errHoldBackupReplaced
	}
	if !allowTerminating && !backup.DeletionTimestamp.IsZero() {
		return errHoldBackupTerminating
	}
	key := verificationHoldKey(v)
	if backup.Annotations[key] == v.Name {
		return nil
	}
	patch := client.MergeFromWithOptions(backup.DeepCopy(), client.MergeFromWithOptimisticLock{})
	if backup.Annotations == nil {
		backup.Annotations = map[string]string{}
	}
	backup.Annotations[key] = v.Name
	if err := r.Patch(ctx, &backup, patch); err != nil {
		if apierrors.IsNotFound(err) {
			return errHoldBackupGone
		}
		return fmt.Errorf("stamp verification hold: %w", err)
	}
	return nil
}

// launchHoldConfirmed re-reads the pinned backup right before the Job is
// created: it must be the pinned object, not being deleted, and carry v's
// hold. Returns a reason for refusing the launch, or "".
func (r *MysqlBackupVerificationReconciler) launchHoldConfirmed(ctx context.Context, v *v1alpha1.MysqlBackupVerification) (string, error) {
	ref := v.Status.BackupRef
	var backup v1alpha1.MysqlBackup
	if err := r.apiReader().Get(ctx, types.NamespacedName{Namespace: v.Namespace, Name: ref.Name}, &backup); err != nil {
		if apierrors.IsNotFound(err) {
			return fmt.Sprintf("pinned backup %q is gone", ref.Name), nil
		}
		return "", fmt.Errorf("get pinned backup: %w", err)
	}
	switch {
	case string(backup.UID) != ref.UID:
		return fmt.Sprintf("pinned backup %q was replaced", ref.Name), nil
	case !backup.DeletionTimestamp.IsZero():
		return fmt.Sprintf("pinned backup %q is being deleted", ref.Name), nil
	case backup.Annotations[verificationHoldKey(v)] != v.Name:
		return fmt.Sprintf("pinned backup %q does not carry this verification's hold", ref.Name), nil
	}
	return "", nil
}

// releaseVerificationHold removes v's hold from its pinned backup once v's
// execution has stopped. done is false while readers may still run.
func (r *MysqlBackupVerificationReconciler) releaseVerificationHold(ctx context.Context, v *v1alpha1.MysqlBackupVerification) (done bool, err error) {
	ref := v.Status.BackupRef
	if ref == nil || ref.Name == "" {
		return true, nil
	}
	var backup v1alpha1.MysqlBackup
	if err := r.apiReader().Get(ctx, types.NamespacedName{Namespace: v.Namespace, Name: ref.Name}, &backup); err != nil {
		if apierrors.IsNotFound(err) {
			return true, nil
		}
		return false, fmt.Errorf("get pinned backup: %w", err)
	}
	key := verificationHoldKey(v)
	if _, held := backup.Annotations[key]; !held {
		return true, nil
	}
	stopped, err := verificationExecutionStopped(ctx, r.apiReader(), v.Namespace, v.Name, !v.DeletionTimestamp.IsZero())
	if err != nil || !stopped {
		return false, err
	}
	patch := client.MergeFromWithOptions(backup.DeepCopy(), client.MergeFromWithOptimisticLock{})
	delete(backup.Annotations, key)
	if err := r.Patch(ctx, &backup, patch); err != nil && !apierrors.IsNotFound(err) {
		return false, fmt.Errorf("release verification hold: %w", err)
	}
	return true, nil
}

func (r *MysqlBackupVerificationReconciler) apiReader() client.Reader {
	if r.APIReader != nil {
		return r.APIReader
	}
	return r.Client
}

func (r *MysqlBackupReconciler) apiReader() client.Reader {
	if r.APIReader != nil {
		return r.APIReader
	}
	return r.Client
}

// deleteUnreadBackup deletes a retention victim unless a verification may
// still be reading it. It re-reads the victim uncached, refuses to act on
// a same-name replacement, and deletes with UID and resourceVersion
// preconditions so a hold stamped after the read wins. checker is shared
// across one prune pass so the namespace's verifications are listed once;
// a verification that pins after that List must stamp a hold (bumping the
// resourceVersion) before it reads, so the precondition still protects it.
func (r *MysqlBackupReconciler) deleteUnreadBackup(ctx context.Context, victim *v1alpha1.MysqlBackup, checker *BackupReadChecker) error {
	var fresh v1alpha1.MysqlBackup
	if err := r.apiReader().Get(ctx, client.ObjectKeyFromObject(victim), &fresh); err != nil {
		return client.IgnoreNotFound(err)
	}
	if fresh.UID != victim.UID || !fresh.DeletionTimestamp.IsZero() || fresh.Status.Phase != v1alpha1.BackupPhaseSucceeded {
		return nil
	}
	readers, err := checker.readers(ctx, &fresh)
	if err != nil {
		return fmt.Errorf("check readers of backup %s: %w", fresh.Name, err)
	}
	if len(readers) > 0 {
		log.FromContext(ctx).Info("backup prune held by verification",
			"namespace", fresh.Namespace, "backup", fresh.Name, "backupUID", string(fresh.UID),
			"fg", fresh.Spec.FailoverGroupRef.Name, "profile", fresh.Spec.ProfileName,
			"verifications", readerNames(readers), "verificationUIDs", readerUIDs(readers))
		r.Recorder.Eventf(&fresh, corev1.EventTypeNormal, "RetentionDeferred",
			"retention keeps this backup while verification %s may be reading it", strings.Join(readerNames(readers), ", "))
		return nil
	}
	uid, rv := fresh.UID, fresh.ResourceVersion
	err = r.Delete(ctx, &fresh, client.Preconditions{UID: &uid, ResourceVersion: &rv})
	if apierrors.IsConflict(err) || apierrors.IsNotFound(err) {
		return nil
	}
	if err != nil {
		return fmt.Errorf("delete old succeeded backup %s: %w", fresh.Name, err)
	}
	return nil
}
