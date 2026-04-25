package controller

import (
	"context"
	"fmt"
	"strings"
	"time"

	mysqldriver "github.com/go-sql-driver/mysql"
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

// ensureTrailingSlash normalizes a dump prefix/location so it ends with
// a forward slash. mysqlsh util.loadDump() expects the directory-style
// prefix produced by util.dumpInstance().
func ensureTrailingSlash(s string) string {
	if s == "" {
		return s
	}
	if strings.HasSuffix(s, "/") {
		return s
	}
	return s + "/"
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

// restoreInFlight reports whether spec.initFromBackup is set and the
// one-shot restore has not yet reached Succeeded. Callers use it to gate
// side effects that would race the restore Job (notably the topology
// manager's fresh-deploy auto-clone).
func restoreInFlight(fg *v1alpha1.MysqlFailoverGroup) bool {
	if fg.Spec.InitFromBackup == nil {
		return false
	}
	if fg.Status.Restore == nil {
		return true
	}
	return fg.Status.Restore.Phase != v1alpha1.BackupPhaseSucceeded
}

// restoreTargetSite chooses the site that will receive a bootstrap
// restore load. Resolution order:
//
//  1. If status.ActiveSite is set AND that site is observed to be
//     writable (or has no observed state / empty state yet), use it.
//  2. If status.ActiveSite is set but that site is observed in any
//     other state (read-only, unreachable, etc.), refuse — return "".
//  3. Otherwise (fresh deploy) fall back to the default seed site —
//     the highest-priority primary-candidate according to
//     spec.splitBrainPolicy.sitePriorities, or the first primary-
//     candidate in declared order.
//
// The caller fails fast with a clear error when this returns empty so
// the operator can't accidentally overwrite a recovering replica with
// a stale dump.
func restoreTargetSite(fg *v1alpha1.MysqlFailoverGroup) string {
	if fg == nil || len(fg.Spec.Sites) == 0 {
		return ""
	}

	active := fg.Status.ActiveSite
	if active != "" {
		for i := range fg.Status.Sites {
			s := &fg.Status.Sites[i]
			if s.Name != active {
				continue
			}
			// Allow writable AND allow not-yet-observed (empty state)
			// because during a fresh deploy the status may lag briefly
			// behind the first Deployment becoming Ready.
			if s.State == "" || s.State == "writable" {
				return active
			}
			// Any other observed state (read-only, unreachable,
			// unknown) means something else has already started up
			// on that site — refuse to load into it.
			return ""
		}
		// ActiveSite is set but the matching status entry is missing.
		// Treat as "fresh" for compatibility.
		return active
	}

	// Fresh deploy: no observed sites yet, no active site — target the
	// default seed site. Falls back to spec.sites[0] when the spec has
	// no primary-candidate (an invalid-but-handled configuration).
	if seed := fg.Spec.DefaultSeedSite(); seed != nil {
		return seed.Name
	}
	return fg.Spec.Sites[0].Name
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

	targetSite := restoreTargetSite(fg)
	if targetSite == "" {
		r.Recorder.Eventf(fg, corev1.EventTypeWarning, "RestoreTargetUnavailable",
			"refusing to run restore: active site %q is not writable / not ready",
			fg.Status.ActiveSite)
		r.setRestoreStatus(ctx, fg, &v1alpha1.RestoreStatus{
			Phase:   v1alpha1.BackupPhasePending,
			Message: fmt.Sprintf("waiting for a writable target site (activeSite=%q)", fg.Status.ActiveSite),
		})
		return 30 * time.Second, nil
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
		r.setRestoreStatus(ctx, fg, &v1alpha1.RestoreStatus{
			Phase:      v1alpha1.BackupPhasePending,
			TargetSite: targetSite,
			Message:    "waiting for target MySQL Deployment to become ready",
		})
		return 10 * time.Second, nil
	}

	// Ensure derived creds Secret for the restore Job.
	credsName := restoreCredsSecretName(fg.Name)
	if err := r.ensureRestoreCredsSecret(ctx, fg, credsName); err != nil {
		return 0, fmt.Errorf("ensure restore creds secret: %w", err)
	}

	// Ensure the restore Job.
	var job batchv1.Job
	jobName := restoreJobName(fg.Name, targetSite)
	jobKey := types.NamespacedName{Namespace: fg.Namespace, Name: jobName}
	if err := r.Get(ctx, jobKey, &job); err != nil {
		if !apierrors.IsNotFound(err) {
			return 0, fmt.Errorf("get restore job: %w", err)
		}

		built, err := r.buildRestoreJob(ctx, fg, targetSite, credsName)
		if err != nil {
			r.Recorder.Eventf(fg, corev1.EventTypeWarning, "RestoreBuildFailed", "%s", err.Error())
			r.setRestoreStatus(ctx, fg, &v1alpha1.RestoreStatus{
				Phase:      v1alpha1.BackupPhaseFailed,
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
		r.setRestoreStatus(ctx, fg, &v1alpha1.RestoreStatus{
			Phase:      v1alpha1.BackupPhaseRunning,
			JobName:    built.Name,
			TargetSite: targetSite,
			StartTime:  &now,
			Message:    "restore Job created",
		})
		r.Recorder.Eventf(fg, corev1.EventTypeNormal, "RestoreStarted",
			"created restore Job %s targeting site %s", built.Name, targetSite)
		return 15 * time.Second, nil
	}

	// Observe the Job.
	phase, message := jobPhase(&job, "restore")
	if phase == "" {
		return 15 * time.Second, nil
	}

	now := metav1.Now()
	rs := &v1alpha1.RestoreStatus{
		Phase:          phase,
		JobName:        job.Name,
		TargetSite:     targetSite,
		StartTime:      job.Status.StartTime,
		CompletionTime: &now,
		Message:        message,
	}
	if rs.StartTime == nil {
		if fg.Status.Restore != nil && fg.Status.Restore.StartTime != nil {
			rs.StartTime = fg.Status.Restore.StartTime
		} else {
			rs.StartTime = &now
		}
	}
	r.setRestoreStatus(ctx, fg, rs)

	if phase == v1alpha1.BackupPhaseSucceeded {
		r.Recorder.Eventf(fg, corev1.EventTypeNormal, "RestoreSucceeded",
			"restore Job %s completed: %s", job.Name, message)
		return 0, nil
	}

	r.Recorder.Eventf(fg, corev1.EventTypeWarning, "RestoreFailed",
		"restore Job %s failed: %s", job.Name, message)
	// No automatic retry. Operator must delete the Job to re-trigger.
	return 0, nil
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
func (r *MysqlFailoverGroupReconciler) setRestoreStatus(ctx context.Context, fg *v1alpha1.MysqlFailoverGroup, rs *v1alpha1.RestoreStatus) {
	patch := client.MergeFrom(fg.DeepCopy())
	fg.Status.Restore = rs
	if err := r.Status().Patch(ctx, fg, patch); err != nil && !apierrors.IsNotFound(err) {
		log.FromContext(ctx).Error(err, "update restore status",
			"fg", fg.Name, "phase", rs.Phase)
	}
}

// ensureRestoreCredsSecret creates or updates the derived Secret used by
// the restore Job. In credentials mode, reads from the effective backup
// secret. In legacy mode, parses the DSN secret.
func (r *MysqlFailoverGroupReconciler) ensureRestoreCredsSecret(ctx context.Context, fg *v1alpha1.MysqlFailoverGroup, secretName string) error {
	var user, password string

	backupSecretName := fg.Spec.EffectiveBackupSecretName()
	var srcSecret corev1.Secret
	if err := r.Get(ctx, types.NamespacedName{Namespace: fg.Namespace, Name: backupSecretName}, &srcSecret); err != nil {
		return fmt.Errorf("get credential secret %s: %w", backupSecretName, err)
	}

	if fg.Spec.UsesCredentials() {
		user = string(srcSecret.Data["username"])
		password = string(srcSecret.Data["password"])
	} else {
		dsnBytes, ok := srcSecret.Data["dsn"]
		if !ok {
			return fmt.Errorf("secret %s missing 'dsn' key", backupSecretName)
		}
		parsed, err := mysqldriver.ParseDSN(string(dsnBytes))
		if err != nil {
			return fmt.Errorf("parse dsn: %w", err)
		}
		user = parsed.User
		password = parsed.Passwd
	}

	if user == "" {
		return fmt.Errorf("secret %s has empty username", backupSecretName)
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
		derived.Labels[labelFailoverGroup] = fg.Name
		derived.Labels[labelManagedBy] = managerName
		derived.Labels[labelResourceKind] = "restore"
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
	// BLOODRAVEN_DROP_SCHEMAS, BLOODRAVEN_RESET_REPLICATION, and
	// BLOODRAVEN_PITR_FILTER_DATABASE.
	ExtraEnv []corev1.EnvVar
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
	})
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
		inputURL           string
		extraEnv           []corev1.EnvVar
		extraVolumes       []corev1.Volume
		extraMounts        []corev1.VolumeMount
		awsCredsSecret     string
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
	)

	switch {
	case src.MysqlBackupRef != nil:
		var ref v1alpha1.MysqlBackup
		if err := r.Get(ctx, types.NamespacedName{Namespace: fg.Namespace, Name: src.MysqlBackupRef.Name}, &ref); err != nil {
			return nil, fmt.Errorf("get referenced mysqlbackup: %w", err)
		}
		if ref.Status.Phase != v1alpha1.BackupPhaseSucceeded || ref.Status.Location == "" {
			return nil, fmt.Errorf("referenced mysqlbackup %s is not Succeeded or has no location", ref.Name)
		}
		inputURL = ensureTrailingSlash(ref.Status.Location)

		// Prefer the structured StorageType set by the backup
		// reconciler; fall back to the old heuristic for pre-upgrade
		// CRs that don't carry it.
		wantsS3 := ref.Status.StorageType == v1alpha1.BackupStorageS3
		wantsPVC := ref.Status.StorageType == v1alpha1.BackupStoragePVC
		if ref.Status.StorageType == "" {
			wantsS3 = isS3Location(ref.Status.Location)
			wantsPVC = !wantsS3
		}

		profile := findProfile(fg, ref.Spec.ProfileName)
		if ref.Status.Encrypted {
			switch {
			case in.Decryption != nil && in.Decryption.PassphraseSecret.Name != "":
				ref := in.Decryption.PassphraseSecret
				passphraseRef = &ref
			case profile != nil && profile.EncryptionEnabled():
				ref := profile.Encryption.PassphraseSecret
				passphraseRef = &ref
			default:
				return nil, fmt.Errorf(
					"%s.source.mysqlBackupRef=%q is encrypted but no passphrase is available; "+
						"set %s.decryption.passphraseSecret or restore the profile's encryption.passphraseSecret",
					fp, ref.Name, fp)
			}
			if wantsS3 {
				s3 := profile.Storage.S3
				decryptSourceKind = "S3"
				decryptS3Bucket = s3.Bucket
				decryptS3EndpointOverride = s3.EndpointURL
				decryptS3Region = s3.Region
				decryptS3Prefix = strings.TrimSuffix(inputURL, "/")
				decryptAWSCredsSecret = s3.CredentialsSecret
			} else if wantsPVC {
				pvc := profile.Storage.PVC
				claim := pvc.ClaimName
				if claim == "" {
					claim = ownedBackupPVCName(fg.Name, profile.Name)
				}
				decryptSourceKind = "PVC"
				decryptPVCClaim = claim
				// inputURL is the in-pod absolute path the backup
				// wrote to (e.g. "/backups/<backup-name>"). Derive a
				// storage-relative prefix from the PVC mount path
				// the decrypt init container will use.
				decryptPVCPrefix = strings.TrimSuffix(strings.TrimPrefix(inputURL, backupPVCMountPath+"/"), "/")
			}
		}
		if wantsS3 && (profile == nil || profile.Storage.Type != v1alpha1.BackupStorageS3 || profile.Storage.S3 == nil) {
			return nil, fmt.Errorf(
				"%s.source.mysqlBackupRef=%q resolves to an S3 location (%q) but profile %q is missing from spec.backup.profiles; "+
					"either restore the profile or set %s.source.s3 explicitly",
				fp, ref.Name, ref.Status.Location, ref.Spec.ProfileName, fp)
		}
		if wantsPVC && (profile == nil || profile.Storage.Type != v1alpha1.BackupStoragePVC || profile.Storage.PVC == nil) {
			return nil, fmt.Errorf(
				"%s.source.mysqlBackupRef=%q resolves to a PVC location (%q) but profile %q is missing from spec.backup.profiles; "+
					"either restore the profile or set %s.source.pvc explicitly",
				fp, ref.Name, ref.Status.Location, ref.Spec.ProfileName, fp)
		}
		if wantsS3 && profile != nil && profile.Storage.Type == v1alpha1.BackupStorageS3 && profile.Storage.S3 != nil {
			extraEnv = append(extraEnv,
				corev1.EnvVar{Name: "BLOODRAVEN_S3_BUCKET", Value: profile.Storage.S3.Bucket},
			)
			if profile.Storage.S3.EndpointURL != "" {
				extraEnv = append(extraEnv, corev1.EnvVar{
					Name: "BLOODRAVEN_S3_ENDPOINT_OVERRIDE", Value: profile.Storage.S3.EndpointURL,
				})
			}
			if profile.Storage.S3.Region != "" {
				extraEnv = append(extraEnv, corev1.EnvVar{Name: "AWS_REGION", Value: profile.Storage.S3.Region})
			}
			awsCredsSecret = profile.Storage.S3.CredentialsSecret
		} else if wantsPVC && profile != nil && profile.Storage.Type == v1alpha1.BackupStoragePVC && profile.Storage.PVC != nil {
			claim := profile.Storage.PVC.ClaimName
			if claim == "" {
				claim = ownedBackupPVCName(fg.Name, profile.Name)
			}
			// MysqlBackup.status.location stores the in-pod dump path that the
			// backup Job used. For PVC-backed backups that path lives under
			// /backups, so the restore Job must mount the same claim there.
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
				Name: "restore-src", MountPath: backupPVCMountPath, ReadOnly: true,
			})
		}

	case src.S3 != nil:
		inputURL = ensureTrailingSlash(src.S3.Prefix)
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
			decryptS3Prefix = strings.TrimSuffix(src.S3.Prefix, "/")
			decryptAWSCredsSecret = src.S3.CredentialsSecret
		}

	case src.PVC != nil:
		claim := src.PVC.ClaimName
		if claim == "" {
			return nil, fmt.Errorf(
				"%s.source.pvc.claimName is required; the restore source PVC must be created out of band and populated with the dump",
				fp)
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

	default:
		return nil, fmt.Errorf("%s.source must set mysqlBackupRef, s3, or pvc", fp)
	}

	loadOptsJSON, err := marshalLoadOptions(in.LoadOptions)
	if err != nil {
		return nil, err
	}

	// PITR replay. The download and replay phases are split across an
	// init container (bloodraven pitr-download, downloads binlogs from
	// S3/PVC into a shared emptyDir) and the main container
	// (restore.py, runs mysqlbinlog | mysql on the downloaded files).
	// See buildRestorePITRFragments for the shape.
	pitrFrags, err := buildRestorePITRFragmentsFor(fg, in.PointInTime)
	if err != nil {
		return nil, err
	}
	extraEnv = append(extraEnv, pitrFrags.MainEnv...)
	extraVolumes = append(extraVolumes, pitrFrags.PodVolumes...)
	extraMounts = append(extraMounts, pitrFrags.MainMounts...)

	labels := map[string]string{
		labelAppName:       "mysql-restore",
		labelInstance:      fg.Name,
		labelFailoverGroup: fg.Name,
		labelSite:          targetSite,
		labelManagedBy:     managerName,
		labelResourceKind:  "restore",
	}

	env := []corev1.EnvVar{
		{Name: "BLOODRAVEN_MYSQL_HOST", Value: backupMySQLHost(fg, targetSite)},
		{Name: "BLOODRAVEN_INPUT_URL", Value: inputURL},
		{Name: "BLOODRAVEN_LOAD_OPTIONS", Value: loadOptsJSON},
		{Name: "BLOODRAVEN_MYSQL_CREDS_DIR", Value: backupCredsMountPath},
		{Name: "HOME", Value: mysqlshHomeMountPath},
	}
	if fg.Spec.TLS != nil {
		env = append(env, corev1.EnvVar{Name: "BLOODRAVEN_TLS", Value: "1"})
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

	var initContainers []corev1.Container
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
				corev1.EnvVar{Name: "BLOODRAVEN_SOURCE_PREFIX", Value: decryptS3Prefix},
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
				corev1.EnvVar{Name: "BLOODRAVEN_SOURCE_PREFIX", Value: decryptPVCPrefix},
			)
		default:
			return nil, fmt.Errorf("%s: decryption is set but source storage kind is empty", fp)
		}

		_, initSC := mergeSecurityContexts(nil, nil)
		initContainers = append(initContainers, corev1.Container{
			Name:            "decrypt-download",
			Image:           operatorImageFromEnv,
			Command:         []string{"bloodraven", "decrypt-download"},
			Env:             decEnv,
			VolumeMounts:    decMounts,
			SecurityContext: initSC,
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
					InitContainers:   initContainers,
					Containers: []corev1.Container{
						{
							Name:            backupJobContainerName,
							Image:           image,
							Command:         []string{"mysqlsh", "--no-wizard", "--py", "-f", backupScriptsMountPath + "/restore.py"},
							Env:             env,
							VolumeMounts:    mounts,
							SecurityContext: containerSC,
						},
					},
					Volumes: volumes,
				},
			},
		},
	}
	return job, nil
}
