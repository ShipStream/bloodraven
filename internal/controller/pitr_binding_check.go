package controller

// PITR archive check Jobs (#203, IMPL-203 §4.4): a one-shot Job that probes
// one archive destination with one candidate key snapshot and reports the
// verdict on its termination log. Check Jobs run only for read targets that
// are not Bound (C1) and for write targets that are not Bound while a
// writer runs another identity or a pre-#203 Deployment (C2).

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"net/url"
	"strings"
	"sync"
	"time"

	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"

	v1alpha1 "github.com/shipstream/bloodraven/api/v1alpha1"
	"github.com/shipstream/bloodraven/internal/pitrarchive"
)

const (
	// pitrCheckContainerName is the check Job's only container.
	pitrCheckContainerName = "pitr-archive-check"
	// pitrCheckPVCMountPath is where a check Job mounts a PVC archive's
	// claim (read-only).
	pitrCheckPVCMountPath = "/pitr-archive"
	// pitrCheckKeyVolume is the candidate snapshot's volume.
	pitrCheckKeyVolume = "pitr-passphrase"
)

// PITRArchiveCheckResult is a check Job's verdict: the fields of its
// PITRArchiveCheckLinePrefix termination line.
type PITRArchiveCheckResult struct {
	// State is PITRArchiveCheckStateAbsent, …Verified or …Refused.
	State string
	// Reason is the refusal token when State is Refused, else "".
	Reason pitrarchive.Token
	// ArchiveID is the descriptor's archive ID, when a descriptor was read.
	ArchiveID string
	// Source is the descriptor's source, when a descriptor was read (also
	// on ArchiveForeign, so the binding can name the owner).
	Source v1alpha1.PITRArchiveSource
	// Destination is the normalized destination probed.
	Destination string
}

// pitrCheckLineKeys are the termination line's keys, in order.
var pitrCheckLineKeys = []string{"state", "reason", "archiveID", "sourceUID", "sourceNamespace", "sourceName", "destination"}

// FormatPITRArchiveCheckLine renders r as the check Job's termination line:
// PITRArchiveCheckLinePrefix, then key=value pairs separated by single
// spaces with every value percent-escaped (url.PathEscape), so a value
// never contains a space and the line parses back exactly.
func FormatPITRArchiveCheckLine(r PITRArchiveCheckResult) string {
	values := []string{r.State, string(r.Reason), r.ArchiveID, string(r.Source.UID), r.Source.Namespace, r.Source.Name, r.Destination}
	var b strings.Builder
	b.WriteString(PITRArchiveCheckLinePrefix)
	for i, k := range pitrCheckLineKeys {
		b.WriteString(" ")
		b.WriteString(k)
		b.WriteString("=")
		b.WriteString(url.PathEscape(values[i]))
	}
	return b.String()
}

// errPITRCheckNoLine: the termination message has no check line.
var errPITRCheckNoLine = errors.New("no " + PITRArchiveCheckLinePrefix + " line")

// parsePITRArchiveCheckLine parses a check Job's termination message. The
// first line starting with PITRArchiveCheckLinePrefix is the verdict; it
// must carry exactly the documented keys in order, a known state, a
// catalog token iff Refused, and a well-formed archive ID and destination.
func parsePITRArchiveCheckLine(msg string) (PITRArchiveCheckResult, error) {
	var line string
	for _, l := range strings.Split(msg, "\n") {
		l = strings.TrimSpace(l)
		if l == PITRArchiveCheckLinePrefix || strings.HasPrefix(l, PITRArchiveCheckLinePrefix+" ") {
			line = l
			break
		}
	}
	if line == "" {
		return PITRArchiveCheckResult{}, errPITRCheckNoLine
	}
	fields := strings.Split(strings.TrimPrefix(line, PITRArchiveCheckLinePrefix+" "), " ")
	if len(fields) != len(pitrCheckLineKeys) {
		return PITRArchiveCheckResult{}, fmt.Errorf("check line has %d fields, want %d: %q", len(fields), len(pitrCheckLineKeys), line)
	}
	values := make([]string, len(fields))
	for i, f := range fields {
		k, v, ok := strings.Cut(f, "=")
		if !ok || k != pitrCheckLineKeys[i] {
			return PITRArchiveCheckResult{}, fmt.Errorf("check line field %d is %q, want %s=…", i, f, pitrCheckLineKeys[i])
		}
		u, err := url.PathUnescape(v)
		if err != nil {
			return PITRArchiveCheckResult{}, fmt.Errorf("check line %s: %w", k, err)
		}
		values[i] = u
	}
	r := PITRArchiveCheckResult{
		State: values[0], Reason: pitrarchive.Token(values[1]), ArchiveID: values[2],
		Source:      v1alpha1.PITRArchiveSource{UID: types.UID(values[3]), Namespace: values[4], Name: values[5]},
		Destination: values[6],
	}
	switch r.State {
	case PITRArchiveCheckStateAbsent, PITRArchiveCheckStateVerified:
		if r.Reason != "" {
			return PITRArchiveCheckResult{}, fmt.Errorf("check line state %s carries reason %q", r.State, r.Reason)
		}
	case PITRArchiveCheckStateRefused:
		if !r.Reason.Known() {
			return PITRArchiveCheckResult{}, fmt.Errorf("check line reason %q is not a refusal token", r.Reason)
		}
	default:
		return PITRArchiveCheckResult{}, fmt.Errorf("check line state %q is unknown", r.State)
	}
	if r.ArchiveID != "" && !pitrarchive.ValidArchiveID(r.ArchiveID) {
		return PITRArchiveCheckResult{}, fmt.Errorf("check line archiveID %q is not an archive ID", r.ArchiveID)
	}
	if r.State == PITRArchiveCheckStateVerified &&
		(r.ArchiveID == "" || r.Source.UID == "" || r.Source.Namespace == "" || r.Source.Name == "") {
		return PITRArchiveCheckResult{}, errors.New("a Verified check line needs the archive ID and source")
	}
	if _, err := pitrarchive.ParseDestination(r.Destination); err != nil {
		return PITRArchiveCheckResult{}, fmt.Errorf("check line destination: %w", err)
	}
	return r, nil
}

// pitrCheckJobName is the deterministic name of the check Job for binding
// id and candidate snapshot ("" for plaintext).
func pitrCheckJobName(fgName, id, candidate string) string {
	h := sha256.New()
	h.Write([]byte(id))
	h.Write([]byte{0})
	h.Write([]byte(candidate))
	return truncateDNS1123("mysql-" + fgName + "-pitr-check-" + hex.EncodeToString(h.Sum(nil))[:8])
}

// pitrCheckRole is the check role of a target: Writer for a write target
// (own UID and derived archive ID; an empty prefix is Absent), Reader
// otherwise.
func pitrCheckRole(t *pitrBindingTarget) string {
	if t.write {
		return PITRArchiveCheckRoleWriter
	}
	return PITRArchiveCheckRoleReader
}

// buildPITRArchiveCheckJob renders the check Job for entry's candidate
// identity (entry.Encryption, whose KeySecretName is candidate) against
// target's destination. The pod has the artifact-cleanup Job's shape
// (Δ26): the operator image, storage access only (S3 credentials or the
// claim, read-only), no service account token, the group's read-only
// tolerations. The candidate snapshot is mounted read-only, 0400, required.
func buildPITRArchiveCheckJob(fg *v1alpha1.MysqlFailoverGroup, entry *v1alpha1.PITRArchiveBinding, target *pitrBindingTarget, candidate string) (*batchv1.Job, error) {
	if fg == nil || entry == nil || target == nil || target.profile == nil {
		return nil, errors.New("check job: missing group, binding or target")
	}
	if operatorImageFromEnv == "" {
		return nil, fmt.Errorf("check job: %w", errOperatorImageUnset)
	}
	if fg.UID == "" {
		return nil, errors.New("check job: the group has no UID")
	}
	enc := entry.Encryption
	ident := pitrarchive.EncryptionIdentity{Mode: pitrarchive.EncryptionMode(enc.Mode), Algorithm: string(enc.Algorithm)}
	if err := ident.Validate(); err != nil {
		return nil, fmt.Errorf("check job: %w", err)
	}
	if (ident.Mode == pitrarchive.EncryptionPassphrase) != (candidate != "") || candidate != enc.KeySecretName {
		return nil, fmt.Errorf("check job: candidate %q does not match the %s identity under test", candidate, ident.Mode)
	}
	dest := target.dest
	profile := target.profile

	labels := map[string]string{
		labelAppName:       appNameBackupJob,
		labelInstance:      fg.Name,
		labelFailoverGroup: fg.Name,
		labelManagedBy:     managerName,
		labelResourceKind:  resourcePITRArchiveCheck,
		labelPITRBindingID: entry.ID,
	}
	if candidate != "" {
		labels[labelPITRKeySecret] = candidate
	}

	env := []corev1.EnvVar{
		{Name: "BLOODRAVEN_PITR_DESTINATION", Value: literalArtifactEnvValue(dest.String())},
		{Name: "BLOODRAVEN_PITR_MANIFEST_PREFIX", Value: literalArtifactEnvValue(dest.KeyPrefix)},
		{Name: "BLOODRAVEN_PITR_ENCRYPTION_MODE", Value: string(ident.Mode)},
		{Name: "BLOODRAVEN_PITR_ALGORITHM", Value: ident.Algorithm},
		{Name: PITRArchiveCheckRoleEnv, Value: pitrCheckRole(target)},
		{Name: "BLOODRAVEN_FAILOVER_GROUP_UID", Value: string(fg.UID)},
	}
	var (
		volumes []corev1.Volume
		mounts  []corev1.VolumeMount
	)
	switch dest.Kind {
	case pitrarchive.DestinationS3:
		s3 := profile.Storage.S3
		if profile.Storage.Type != v1alpha1.BackupStorageS3 || s3 == nil {
			return nil, fmt.Errorf("check job: profile %q does not describe the S3 destination %s", profile.Name, dest)
		}
		env = append(env,
			corev1.EnvVar{Name: "BLOODRAVEN_PITR_STORAGE_TYPE", Value: string(v1alpha1.BackupStorageS3)},
			corev1.EnvVar{Name: "BLOODRAVEN_PITR_S3_BUCKET", Value: literalArtifactEnvValue(dest.Bucket)},
		)
		if s3.EndpointURL != "" {
			env = append(env, corev1.EnvVar{Name: "BLOODRAVEN_PITR_S3_ENDPOINT_URL", Value: literalArtifactEnvValue(s3.EndpointURL)})
		}
		if s3.Region != "" {
			env = append(env, corev1.EnvVar{Name: "BLOODRAVEN_PITR_S3_REGION", Value: literalArtifactEnvValue(s3.Region)})
		}
		if s3.CredentialsSecret != "" {
			volumes = append(volumes, corev1.Volume{Name: "pitr-aws-creds", VolumeSource: corev1.VolumeSource{
				Secret: &corev1.SecretVolumeSource{SecretName: s3.CredentialsSecret, DefaultMode: ptr32(0o400)},
			}})
			mounts = append(mounts, corev1.VolumeMount{Name: "pitr-aws-creds", MountPath: pitrAWSCredsMountDir, ReadOnly: true})
			env = append(env, corev1.EnvVar{Name: "BLOODRAVEN_PITR_AWS_CREDS_DIR", Value: pitrAWSCredsMountDir})
		}
	case pitrarchive.DestinationPVC:
		if profile.Storage.Type != v1alpha1.BackupStoragePVC || profile.Storage.PVC == nil {
			return nil, fmt.Errorf("check job: profile %q does not describe the PVC destination %s", profile.Name, dest)
		}
		volumes = append(volumes, corev1.Volume{Name: "pitr-archive", VolumeSource: corev1.VolumeSource{
			PersistentVolumeClaim: &corev1.PersistentVolumeClaimVolumeSource{ClaimName: dest.Claim, ReadOnly: true},
		}})
		mounts = append(mounts, corev1.VolumeMount{Name: "pitr-archive", MountPath: pitrCheckPVCMountPath, ReadOnly: true})
		env = append(env,
			corev1.EnvVar{Name: "BLOODRAVEN_PITR_STORAGE_TYPE", Value: string(v1alpha1.BackupStoragePVC)},
			corev1.EnvVar{Name: "BLOODRAVEN_PITR_PVC_MOUNT_PATH", Value: pitrCheckPVCMountPath},
		)
	default:
		return nil, fmt.Errorf("check job: destination %s has an unknown kind", dest)
	}
	if candidate != "" {
		volumes = append(volumes, corev1.Volume{Name: pitrCheckKeyVolume, VolumeSource: corev1.VolumeSource{
			Secret: &corev1.SecretVolumeSource{
				SecretName:  candidate,
				DefaultMode: ptr32(0o400),
				Items:       []corev1.KeyToPath{{Key: pitrKeySnapshotDataKey, Path: backupPassphraseFileName}},
				Optional:    boolPtr(false),
			},
		}})
		mounts = append(mounts, corev1.VolumeMount{Name: pitrCheckKeyVolume, MountPath: backupPassphraseMountPath, ReadOnly: true})
		env = append(env, corev1.EnvVar{Name: "BLOODRAVEN_PITR_PASSPHRASE_FILE", Value: backupPassphraseMountPath + "/" + backupPassphraseFileName})
	}
	volumes = append(volumes, corev1.Volume{Name: "tmp", VolumeSource: corev1.VolumeSource{EmptyDir: &corev1.EmptyDirVolumeSource{}}})
	mounts = append(mounts, corev1.VolumeMount{Name: "tmp", MountPath: tmpMountPath})

	bspec := fg.Spec.Backup
	var (
		podSCSrc       *corev1.PodSecurityContext
		containerSCSrc *corev1.SecurityContext
		pullSecrets    []corev1.LocalObjectReference
		resources      = defaultRestoreResources()
	)
	if bspec != nil {
		podSCSrc, containerSCSrc, pullSecrets = bspec.PodSecurityContext, bspec.ContainerSecurityContext, bspec.ImagePullSecrets
		resources = effectiveBackupResources(&backupResourcesSource{Resources: bspec.Resources}, defaultRestoreResources())
	}
	podSC, containerSC := mergeSecurityContexts(podSCSrc, containerSCSrc)
	deadline := pitrCheckJobActiveDeadlineSeconds
	backoff := int32(0)
	return &batchv1.Job{
		ObjectMeta: metav1.ObjectMeta{
			Name:            pitrCheckJobName(fg.Name, entry.ID, candidate),
			Namespace:       fg.Namespace,
			Labels:          labels,
			OwnerReferences: []metav1.OwnerReference{*metav1.NewControllerRef(fg, v1alpha1.GroupVersion.WithKind("MysqlFailoverGroup"))},
		},
		Spec: batchv1.JobSpec{
			ActiveDeadlineSeconds: &deadline,
			BackoffLimit:          &backoff,
			Template: corev1.PodTemplateSpec{
				ObjectMeta: metav1.ObjectMeta{Labels: labels},
				Spec: corev1.PodSpec{
					RestartPolicy:    corev1.RestartPolicyNever,
					ImagePullSecrets: pullSecrets,
					SecurityContext:  podSC,
					// The check only talks to storage, never to the
					// Kubernetes API.
					AutomountServiceAccountToken: boolPtr(false),
					Tolerations:                  groupReadOnlyTolerations(fg.Name),
					Containers: []corev1.Container{{
						Name:                     pitrCheckContainerName,
						Image:                    operatorImageFromEnv,
						Command:                  operatorCommand("pitr-archive-check"),
						Env:                      env,
						Resources:                resources,
						VolumeMounts:             mounts,
						SecurityContext:          containerSC,
						TerminationMessagePolicy: corev1.TerminationMessageFallbackToLogsOnError,
					}},
					Volumes: volumes,
				},
			},
		},
	}, nil
}

// pitrCheckRetryGate holds, per group and binding, when a check Job that
// could not start (or failed without a verdict) may be retried
// (pitrCheckRetryAfter). It is process memory: after an operator restart
// the check is retried at once, which is harmless.
var pitrCheckRetryGate sync.Map // key: fg UID + "/" + binding id → time.Time

func pitrCheckRetryKey(fg *v1alpha1.MysqlFailoverGroup, id string) string {
	return string(fg.UID) + "/" + id
}

// pitrCheckRetryWait returns how long the binding's next check must still
// wait (0: may start now).
func pitrCheckRetryWait(fg *v1alpha1.MysqlFailoverGroup, id string, now time.Time) time.Duration {
	v, ok := pitrCheckRetryGate.Load(pitrCheckRetryKey(fg, id))
	if !ok {
		return 0
	}
	if wait := v.(time.Time).Sub(now); wait > 0 {
		return wait
	}
	pitrCheckRetryGate.Delete(pitrCheckRetryKey(fg, id))
	return 0
}

// ensurePITRArchiveCheck makes sure the check Job for entry's candidate
// exists and is the one entry.Check records. It returns the Job (nil when
// it is still being replaced) and whether status must record a new Job.
// A Job of the deterministic name whose UID status does not record is a
// leftover (an operator crash between Create and the status write, or an
// earlier candidate round): it is deleted and re-created on a later pass.
func (r *MysqlFailoverGroupReconciler) ensurePITRArchiveCheck(ctx context.Context, fg *v1alpha1.MysqlFailoverGroup, entry *v1alpha1.PITRArchiveBinding, target *pitrBindingTarget, candidate string) (*batchv1.Job, error) {
	name := pitrCheckJobName(fg.Name, entry.ID, candidate)
	var job batchv1.Job
	err := r.bindingReader().Get(ctx, types.NamespacedName{Namespace: fg.Namespace, Name: name}, &job)
	switch {
	case err == nil:
		if entry.Check != nil && entry.Check.JobName == name && entry.Check.JobUID == job.UID {
			return &job, nil
		}
		if job.DeletionTimestamp == nil {
			if err := r.deletePITRCheckJob(ctx, &job); err != nil {
				return nil, err
			}
		}
		entry.Check = nil
		return nil, nil
	case !apierrors.IsNotFound(err):
		return nil, fmt.Errorf("get check job %s: %w", name, err)
	}
	built, err := buildPITRArchiveCheckJob(fg, entry, target, candidate)
	if err != nil {
		return nil, err
	}
	if err := r.Create(ctx, built); err != nil {
		if apierrors.IsAlreadyExists(err) {
			entry.Check = nil
			return nil, nil
		}
		return nil, fmt.Errorf("create check job %s: %w", name, err)
	}
	entry.Check = &v1alpha1.PITRArchiveBindingCheck{JobName: built.Name, JobUID: built.UID}
	return built, nil
}

// pitrCheckOutcome is what observePITRArchiveCheck found.
type pitrCheckOutcome struct {
	// done is true when the check ended: result or failure is set.
	done bool
	// result is the parsed verdict.
	result *PITRArchiveCheckResult
	// failure explains a check that could not start, failed or ended
	// without a verdict.
	failure string
	// job is the observed Job (nil when it is gone).
	job *batchv1.Job
}

// observePITRArchiveCheck reads the verdict of the Job entry.Check names,
// only from the Job whose UID it records. A pod Pending past
// pitrCheckPendingTimeout, a Job that never created a pod, a failed Job and
// a finished Job without a parseable line are failures.
func (r *MysqlFailoverGroupReconciler) observePITRArchiveCheck(ctx context.Context, fg *v1alpha1.MysqlFailoverGroup, entry *v1alpha1.PITRArchiveBinding, now time.Time) (pitrCheckOutcome, error) {
	if entry.Check == nil {
		return pitrCheckOutcome{}, nil
	}
	var job batchv1.Job
	err := r.bindingReader().Get(ctx, types.NamespacedName{Namespace: fg.Namespace, Name: entry.Check.JobName}, &job)
	switch {
	case apierrors.IsNotFound(err):
		return pitrCheckOutcome{done: true, failure: fmt.Sprintf("check Job %s disappeared before it reported", entry.Check.JobName)}, nil
	case err != nil:
		return pitrCheckOutcome{}, fmt.Errorf("get check job %s: %w", entry.Check.JobName, err)
	}
	if entry.Check.JobUID != "" && job.UID != entry.Check.JobUID {
		return pitrCheckOutcome{done: true, job: &job, failure: fmt.Sprintf("check Job %s was replaced by another object", job.Name)}, nil
	}
	pod, err := latestJobPod(ctx, r.Client, &job)
	if err != nil {
		return pitrCheckOutcome{}, fmt.Errorf("read check pod: %w", err)
	}
	code, message, terminated := cleanupPodVerdict(pod)
	if terminated && code == 0 {
		if res, perr := parsePITRArchiveCheckLine(message); perr == nil {
			return pitrCheckOutcome{done: true, job: &job, result: &res}, nil
		}
	}
	phase, detail := jobPhase(&job, "check")
	switch phase {
	case v1alpha1.BackupPhaseSucceeded:
		return pitrCheckOutcome{done: true, job: &job, failure: fmt.Sprintf("check Job %s ended without a verdict line", job.Name)}, nil
	case v1alpha1.BackupPhaseFailed:
		if terminated && message != "" {
			detail = compactPITRCheckMessage(message)
		}
		return pitrCheckOutcome{done: true, job: &job, failure: fmt.Sprintf("check Job %s failed: %s", job.Name, detail)}, nil
	}
	if stuck := cleanupPodStuck(pod, now); stuck != "" {
		return pitrCheckOutcome{done: true, job: &job, failure: fmt.Sprintf("check Job %s could not start: %s", job.Name, stuck)}, nil
	}
	if created := job.CreationTimestamp; pod == nil && !created.IsZero() && now.Sub(created.Time) >= pitrCheckPendingTimeout {
		return pitrCheckOutcome{done: true, job: &job, failure: fmt.Sprintf("check Job %s could not start: no pod created since %s; check the Job's FailedCreate events (kubectl describe job %s)",
			job.Name, created.UTC().Format(time.RFC3339), job.Name)}, nil
	}
	return pitrCheckOutcome{job: &job}, nil
}

// compactPITRCheckMessage bounds a termination message quoted in a binding
// message.
func compactPITRCheckMessage(s string) string {
	s = strings.Join(strings.Fields(s), " ")
	const limit = 512
	if len(s) > limit {
		return s[:limit] + "…"
	}
	return s
}

// deletePITRCheckJob deletes a check Job and its pods (background
// propagation). NotFound is success.
func (r *MysqlFailoverGroupReconciler) deletePITRCheckJob(ctx context.Context, job *batchv1.Job) error {
	if err := r.Delete(ctx, job, client.PropagationPolicy(metav1.DeletePropagationBackground)); err != nil && !apierrors.IsNotFound(err) {
		return fmt.Errorf("delete check job %s: %w", job.Name, err)
	}
	return nil
}

// bindingReader is the uncached reader when one is wired, else the cached
// client (tests).
func (r *MysqlFailoverGroupReconciler) bindingReader() client.Reader {
	if r.APIReader != nil {
		return r.APIReader
	}
	return r.Client
}

// ParsePITRArchiveCheckLineForTest is parsePITRArchiveCheckLine, for the
// command's and the component tests: the line the command writes must be
// the line the controller reads. Not for production use.
func ParsePITRArchiveCheckLineForTest(msg string) (PITRArchiveCheckResult, error) {
	return parsePITRArchiveCheckLine(msg)
}
