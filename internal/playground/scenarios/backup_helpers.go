package scenarios

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"strconv"
	"strings"
	"time"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	apimeta "k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	ctrlclient "sigs.k8s.io/controller-runtime/pkg/client"

	v1alpha1 "github.com/shipstream/bloodraven/api/v1alpha1"
	"github.com/shipstream/bloodraven/internal/backupmeta"
	"github.com/shipstream/bloodraven/internal/pitrarchive/pitrprofile"
	pgkube "github.com/shipstream/bloodraven/internal/playground/kube"
	pgmysql "github.com/shipstream/bloodraven/internal/playground/mysql"
	"github.com/shipstream/bloodraven/internal/playground/runner"
	pgsidecar "github.com/shipstream/bloodraven/internal/playground/sidecar"
)

const (
	backupE2EBucket       = "bloodraven-backup-e2e"
	backupE2EProfile      = "rustfs-e2e"
	backupE2EEndpoint     = "http://rustfs.bloodraven-playground.svc.cluster.local:9000"
	backupE2ECredsSecret  = "dragonfly-s3-credentials"
	backupOriginalSpecKey = "backupOriginalSpecJSON"
	backupOriginalHadKey  = "backupOriginalHadSpec"
)

// backupE2EPassphraseSecretPrefix names the throwaway AES-256-GCM
// passphrase Secret for the encrypted backup scenario. Each run appends
// its run stamp so it never reuses (or later deletes) a Secret it did not
// create.
const backupE2EPassphraseSecretPrefix = "bloodraven-backup-e2e-passphrase-"

// Stash keys recording the passphrase Secret this run created, so cleanup
// deletes exactly that object and nothing else.
const (
	backupPassphraseSecretKey    = "backupPassphraseSecret"
	backupPassphraseSecretUIDKey = "backupPassphraseSecretUID"
)

func backupRunStamp(env *runner.Env) string {
	start := env.StartTime.UTC()
	if start.IsZero() {
		start = time.Now().UTC()
	}
	return strings.ToLower(start.Format("20060102T150405Z"))
}

func backupProfileSpec(prefix string, pitr bool) v1alpha1.BackupSpec {
	consistent := true
	spec := v1alpha1.BackupSpec{
		Image:                  v1alpha1.DefaultBackupImage,
		MaxLagSecondsForSource: new(int64(60)),
		ActiveDeadlineSeconds:  1800,
		BackoffLimit:           new(int32(0)),
		Profiles: []v1alpha1.BackupProfile{{
			Name: backupE2EProfile,
			Retention: v1alpha1.BackupRetention{
				KeepLast:       2,
				KeepWithinDays: new(int32(1)),
				KeepFailed:     new(int32(2)),
			},
			Storage: v1alpha1.BackupStorage{
				Type: v1alpha1.BackupStorageS3,
				S3: &v1alpha1.S3Storage{
					Bucket:                backupE2EBucket,
					Prefix:                prefix,
					EndpointURL:           backupE2EEndpoint,
					CredentialsSecretName: backupE2ECredsSecret,
					Region:                "us-east-1",
				},
			},
			Dump: &v1alpha1.DumpOptions{
				Threads:       2,
				BytesPerChunk: "64M",
				Compression:   "zstd",
				Consistent:    &consistent,
			},
		}},
	}
	if pitr {
		spec.PITR = &v1alpha1.PITRSpec{
			Enabled:             true,
			ProfileName:         backupE2EProfile,
			MaxBinlogSize:       "1M",
			ArchivePollInterval: &metav1.Duration{Duration: 5 * time.Second},
		}
	}
	return spec
}

// pvcBackupProfileSpec is a spec.backup with one profile named profile
// that writes to an operator-provisioned claim of storageClass
// (mysql-<group>-backup-<profile>). With passphraseSecret set the profile
// encrypts its backups with that Secret's passphrase, so a restore from
// it runs the decrypt-download init container against the read-only claim.
func pvcBackupProfileSpec(profile, storageClass, size, passphraseSecret string) v1alpha1.BackupSpec {
	consistent := true
	spec := v1alpha1.BackupSpec{
		Image:                  v1alpha1.DefaultBackupImage,
		MaxLagSecondsForSource: new(int64(60)),
		ActiveDeadlineSeconds:  1800,
		BackoffLimit:           new(int32(0)),
		Profiles: []v1alpha1.BackupProfile{{
			Name: profile,
			Retention: v1alpha1.BackupRetention{
				KeepLast:       2,
				KeepWithinDays: new(int32(1)),
				KeepFailed:     new(int32(2)),
			},
			Storage: v1alpha1.BackupStorage{
				Type: v1alpha1.BackupStoragePVC,
				PVC: &v1alpha1.PVCStorage{Provisioned: &v1alpha1.ProvisionedVolumeSpec{
					StorageClassName: &storageClass,
					Size:             v1alpha1.StorageQuantity(size),
				}},
			},
			Dump: &v1alpha1.DumpOptions{
				Threads:     2,
				Compression: "zstd",
				Consistent:  &consistent,
			},
		}},
	}
	if passphraseSecret != "" {
		spec.Profiles[0].Encryption = backupE2EEncryption(passphraseSecret)
	}
	return spec
}

// waitForPVCBackupProfile is waitForBackupProfile for a profile written
// by pvcBackupProfileSpec.
func waitForPVCBackupProfile(ctx context.Context, env *runner.Env, profile string) error {
	_, err := env.Wait.UntilCR(ctx, env.Namespace, "PVC backup profile observed", func(mfg *v1alpha1.MysqlFailoverGroup) (bool, string, error) {
		var found *v1alpha1.BackupProfile
		if mfg.Spec.Backup != nil {
			for i := range mfg.Spec.Backup.Profiles {
				if mfg.Spec.Backup.Profiles[i].Name == profile {
					found = &mfg.Spec.Backup.Profiles[i]
				}
			}
		}
		if found == nil || found.Storage.Type != v1alpha1.BackupStoragePVC || found.Storage.PVC == nil {
			return false, profile + " PVC profile not observed", nil
		}
		rolloutReady := backupProfileRolloutReady(mfg)
		return rolloutReady, fmt.Sprintf("profile %s storage=%s encrypted=%v rolloutReady=%v active=%q updatePhase=%q",
			profile, found.Storage.Type, found.Encryption != nil, rolloutReady, mfg.Status.ActiveSite, mfg.Status.UpdatePhase), nil
	})
	return err
}

func backupE2EEncryption(secretName string) *v1alpha1.BackupEncryptionSpec {
	return &v1alpha1.BackupEncryptionSpec{
		Algorithm:              "AES-256-GCM",
		PassphraseSecretKeyRef: v1alpha1.PassphraseSecretKeyRef{Name: secretName},
	}
}

// ensureBackupPassphraseSecret creates this run's passphrase Secret and
// stashes its name and UID. It refuses to adopt an existing Secret: the
// name is unique per run, so a collision means another run or a user owns
// it, and its passphrase is unknown to this one.
func ensureBackupPassphraseSecret(ctx context.Context, env *runner.Env) (string, error) {
	name := backupE2EPassphraseSecretPrefix + backupRunStamp(env)
	passphrase := make([]byte, 32)
	if _, err := rand.Read(passphrase); err != nil {
		return "", fmt.Errorf("generate backup passphrase: %w", err)
	}
	text := hex.EncodeToString(passphrase)
	sec := &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{
			Name:   name,
			Labels: map[string]string{"playground.bloodraven.shipstream.io/created-by-e2e": "true"},
		},
		StringData: map[string]string{
			backupE2EEncryption(name).PassphraseSecretKeyRef.PassphraseSecretKeyOrDefault(): text,
		},
	}
	created, err := env.Kube.Kubernetes.CoreV1().Secrets(env.Namespace).Create(ctx, sec, metav1.CreateOptions{})
	if err != nil {
		return "", fmt.Errorf("create backup passphrase secret %s: %w", name, err)
	}
	if err := ctxStash(ctx, env, backupPassphraseSecretKey, name); err != nil {
		return "", err
	}
	if err := ctxStash(ctx, env, backupPassphraseSecretUIDKey, string(created.UID)); err != nil {
		return "", err
	}
	// Archive objects are decrypted with these bytes, never with the live
	// Secret, which a scenario may rotate (s69).
	pitrStashArchivePassphrase(env, []byte(text))
	return name, nil
}

// deleteBackupPassphraseSecret deletes the Secret this run created, and
// only that object (UID precondition). A no-op when the run never
// created one.
func deleteBackupPassphraseSecret(ctx context.Context, env *runner.Env) error {
	name := ctxFetch(env, backupPassphraseSecretKey)
	if name == "" {
		return nil
	}
	uid := types.UID(ctxFetch(env, backupPassphraseSecretUIDKey))
	err := env.Kube.Kubernetes.CoreV1().Secrets(env.Namespace).Delete(ctx, name, metav1.DeleteOptions{
		Preconditions: &metav1.Preconditions{UID: &uid},
	})
	if err != nil && !apierrors.IsNotFound(err) {
		return fmt.Errorf("delete backup passphrase secret %s: %w", name, err)
	}
	env.Capture.Note("deleted backup passphrase secret " + name)
	return nil
}

func patchBackupSpec(ctx context.Context, env *runner.Env, spec v1alpha1.BackupSpec) error {
	mfg, err := env.Kube.GetMFGNamed(ctx, env.Namespace, env.FG)
	if err != nil {
		return fmt.Errorf("read MFG before backup spec patch: %w", err)
	}
	hadOriginal := mfg.Spec.Backup != nil
	originalJSON := ""
	if hadOriginal {
		body, err := json.Marshal(mfg.Spec.Backup.DeepCopy())
		if err != nil {
			return fmt.Errorf("marshal original backup spec: %w", err)
		}
		originalJSON = string(body)
	}
	op := "add"
	if hadOriginal {
		op = "replace"
	}
	if err := env.Kube.PatchMFGNamed(ctx, env.Namespace, env.FG, []pgkube.JSONPatchOp{{
		Op:    op,
		Path:  "/spec/backup",
		Value: spec,
	}}); err != nil {
		return fmt.Errorf("patch backup spec: %w", err)
	}
	if err := ctxStash(ctx, env, backupOriginalHadKey, strconv.FormatBool(hadOriginal)); err != nil {
		return err
	}
	if hadOriginal {
		if err := ctxStash(ctx, env, backupOriginalSpecKey, originalJSON); err != nil {
			return err
		}
	}
	var profiles []string
	for _, p := range spec.Profiles {
		profiles = append(profiles, fmt.Sprintf("%s(%s)", p.Name, p.Storage.Type))
	}
	env.Capture.Note(fmt.Sprintf("patched spec.backup: hadOriginal=%v profiles=%v", hadOriginal, profiles))
	return nil
}

func restoreOriginalBackupSpec(ctx context.Context, env *runner.Env) error {
	rawHad := ctxFetch(env, backupOriginalHadKey)
	if rawHad == "" {
		return nil
	}
	hadOriginal, err := strconv.ParseBool(rawHad)
	if err != nil {
		return fmt.Errorf("parse original backup spec marker %q: %w", rawHad, err)
	}
	if !hadOriginal {
		if err := env.Kube.PatchMFGNamed(ctx, env.Namespace, env.FG, []pgkube.JSONPatchOp{{Op: "remove", Path: "/spec/backup"}}); err != nil {
			return fmt.Errorf("remove scenario backup spec: %w", err)
		}
		env.Capture.Note("restored spec.backup by removing scenario-added config")
		return nil
	}
	var original v1alpha1.BackupSpec
	if err := json.Unmarshal([]byte(ctxFetch(env, backupOriginalSpecKey)), &original); err != nil {
		return fmt.Errorf("unmarshal original backup spec: %w", err)
	}
	if err := env.Kube.PatchMFGNamed(ctx, env.Namespace, env.FG, []pgkube.JSONPatchOp{{
		Op:    "replace",
		Path:  "/spec/backup",
		Value: original,
	}}); err != nil {
		return fmt.Errorf("restore original backup spec: %w", err)
	}
	env.Capture.Note("restored original spec.backup")
	return nil
}

// waitForBackupProfile waits until a plain (non-PITR) backup profile is in
// the spec and the topology is stable: starting a backup while a failover
// or rollout is in progress can select the wrong source. A plain profile
// does not change MySQL pods, so it does not wait for Ready at the patched
// generation: status is not rewritten for the otherwise inert spec update,
// and that wait could last forever. A PITR profile rolls every pod behind
// an archive binding check; pitrWaitArchiveBound is its wait.
func waitForBackupProfile(ctx context.Context, env *runner.Env, prefix string) error {
	_, err := env.Wait.UntilCR(ctx, env.Namespace, "backup profile observed", func(mfg *v1alpha1.MysqlFailoverGroup) (bool, string, error) {
		found := backupE2EProfileOf(mfg)
		if found == nil || found.Storage.S3 == nil {
			return false, "rustfs-e2e profile not observed", nil
		}
		s3 := found.Storage.S3
		matches := backupProfileMatches(found, prefix)
		rolloutReady := backupProfileRolloutReady(mfg)
		return matches && rolloutReady, fmt.Sprintf("profile storage=%s bucket=%q prefix=%q endpoint=%q rolloutReady=%v generation=%d active=%q updatePhase=%q", found.Storage.Type, s3.Bucket, s3.Prefix, s3.EndpointURL, rolloutReady, mfg.Generation, mfg.Status.ActiveSite, mfg.Status.UpdatePhase), nil
	})
	return err
}

// backupE2EProfileOf is the group's rustfs-e2e profile, or nil.
func backupE2EProfileOf(mfg *v1alpha1.MysqlFailoverGroup) *v1alpha1.BackupProfile {
	if mfg.Spec.Backup == nil {
		return nil
	}
	for i := range mfg.Spec.Backup.Profiles {
		if mfg.Spec.Backup.Profiles[i].Name == backupE2EProfile {
			return &mfg.Spec.Backup.Profiles[i]
		}
	}
	return nil
}

// backupProfileMatches reports whether p is the e2e S3 profile at prefix.
func backupProfileMatches(p *v1alpha1.BackupProfile, prefix string) bool {
	if p == nil || p.Storage.S3 == nil {
		return false
	}
	s3 := p.Storage.S3
	return p.Storage.Type == v1alpha1.BackupStorageS3 && s3.Bucket == backupE2EBucket && s3.Prefix == prefix && s3.EndpointURL == backupE2EEndpoint
}

// backupProfileRolloutReady reports a stable topology: an active site and
// no ordered update in progress.
func backupProfileRolloutReady(mfg *v1alpha1.MysqlFailoverGroup) bool {
	return mfg.Status.ActiveSite != "" && mfg.Status.UpdatePhase == ""
}

func createMysqlBackup(ctx context.Context, env *runner.Env, name, scenarioID string) error {
	return createMysqlBackupForProfile(ctx, env, name, scenarioID, backupE2EProfile)
}

// createMysqlBackupForProfile creates a manual MysqlBackup of profile.
func createMysqlBackupForProfile(ctx context.Context, env *runner.Env, name, scenarioID, profile string) error {
	b := &v1alpha1.MysqlBackup{
		ObjectMeta: metav1.ObjectMeta{
			Name:      name,
			Namespace: env.Namespace,
			Labels: map[string]string{
				v1alpha1.FailoverGroupLabel:                          env.FG,
				v1alpha1.BackupProfileLabel:                          profile,
				"playground.bloodraven.shipstream.io/scenario":       scenarioID,
				"playground.bloodraven.shipstream.io/created-by-e2e": "true",
			},
			Annotations: map[string]string{v1alpha1.TriggeredByAnnotation: "chaos/" + scenarioID},
		},
		Spec: v1alpha1.MysqlBackupSpec{
			FailoverGroupRef: v1alpha1.LocalFailoverGroupRef{Name: env.FG},
			ProfileName:      profile,
		},
	}
	if err := env.Kube.Controller.Create(ctx, b); err != nil {
		return fmt.Errorf("create MysqlBackup %s: %w", name, err)
	}
	env.Capture.Note(fmt.Sprintf("created MysqlBackup %s of profile %s", name, profile))
	return nil
}

func createMysqlBackupVerification(ctx context.Context, env *runner.Env, name, backupName, scenarioID string, pit *v1alpha1.PointInTimeVerificationSpec, query string, minResult int64) error {
	keep := true
	v := &v1alpha1.MysqlBackupVerification{
		ObjectMeta: metav1.ObjectMeta{
			Name:      name,
			Namespace: env.Namespace,
			Labels: map[string]string{
				v1alpha1.FailoverGroupLabel:                          env.FG,
				v1alpha1.BackupProfileLabel:                          backupE2EProfile,
				"playground.bloodraven.shipstream.io/scenario":       scenarioID,
				"playground.bloodraven.shipstream.io/created-by-e2e": "true",
			},
			Annotations: map[string]string{v1alpha1.TriggeredByAnnotation: "chaos/" + scenarioID},
		},
		Spec: v1alpha1.MysqlBackupVerificationSpec{
			FailoverGroupRef:        v1alpha1.LocalFailoverGroupRef{Name: env.FG},
			ProfileName:             backupE2EProfile,
			BackupRef:               &v1alpha1.LocalMysqlBackupRef{Name: backupName},
			KeepOnFailure:           &keep,
			TTLSecondsAfterFinished: new(int32(0)),
			PointInTime:             pit,
			SanityCheck: &v1alpha1.SanityCheckSpec{
				Query:     query,
				MinResult: new(minResult),
				Timeout:   &metav1.Duration{Duration: 30 * time.Second},
			},
		},
	}
	if err := env.Kube.Controller.Create(ctx, v); err != nil {
		return fmt.Errorf("create MysqlBackupVerification %s: %w", name, err)
	}
	env.Capture.Note(fmt.Sprintf("created MysqlBackupVerification %s pinnedTo=%s", name, backupName))
	return nil
}

func getMysqlBackup(ctx context.Context, env *runner.Env, name string) (*v1alpha1.MysqlBackup, error) {
	b := &v1alpha1.MysqlBackup{}
	if err := env.Kube.Controller.Get(ctx, ctrlclient.ObjectKey{Namespace: env.Namespace, Name: name}, b); err != nil {
		return nil, err
	}
	return b, nil
}

func getMysqlBackupVerification(ctx context.Context, env *runner.Env, name string) (*v1alpha1.MysqlBackupVerification, error) {
	v := &v1alpha1.MysqlBackupVerification{}
	if err := env.Kube.Controller.Get(ctx, ctrlclient.ObjectKey{Namespace: env.Namespace, Name: name}, v); err != nil {
		return nil, err
	}
	return v, nil
}

func waitForBackupPhase(ctx context.Context, env *runner.Env, name string, want v1alpha1.BackupPhase) (*v1alpha1.MysqlBackup, error) {
	start := time.Now()
	tick := time.NewTicker(time.Second)
	defer tick.Stop()
	progress := time.NewTicker(10 * time.Second)
	defer progress.Stop()
	var last string
	for {
		b, err := getMysqlBackup(ctx, env, name)
		if err != nil {
			last = err.Error()
		} else {
			last = backupStatusSummary(b)
			if b.Status.Phase == want {
				return b, nil
			}
			if b.Status.Phase == v1alpha1.BackupPhaseFailed {
				return b, fmt.Errorf("MysqlBackup %s failed: %s", name, last)
			}
		}
		select {
		case <-ctx.Done():
			return nil, fmt.Errorf("wait for MysqlBackup %s phase %s timed out after %s: %s", name, want, time.Since(start).Round(time.Second), last)
		case <-progress.C:
			env.Logger.Info("wait", "what", "MysqlBackup "+name, "last", last, "elapsed", time.Since(start).Round(time.Second))
		case <-tick.C:
		}
	}
}

func waitForVerificationPhase(ctx context.Context, env *runner.Env, name string, want v1alpha1.VerificationPhase) (*v1alpha1.MysqlBackupVerification, error) {
	start := time.Now()
	tick := time.NewTicker(time.Second)
	defer tick.Stop()
	progress := time.NewTicker(10 * time.Second)
	defer progress.Stop()
	var last string
	for {
		v, err := getMysqlBackupVerification(ctx, env, name)
		if err != nil {
			last = err.Error()
		} else {
			last = verificationStatusSummary(v)
			if v.Status.Phase == want {
				return v, nil
			}
			if v.Status.Phase == v1alpha1.VerificationPhaseFailed {
				captureVerificationJobLogs(ctx, env, v)
				return v, fmt.Errorf("MysqlBackupVerification %s failed: %s", name, last)
			}
			// An expected refusal that succeeds is the regression itself;
			// report it at once, with forensics, instead of timing out.
			if v.Status.Phase == v1alpha1.VerificationPhaseSucceeded {
				captureVerificationJobLogs(ctx, env, v)
				return v, fmt.Errorf("MysqlBackupVerification %s succeeded, want %s: %s", name, want, last)
			}
		}
		select {
		case <-ctx.Done():
			return nil, fmt.Errorf("wait for MysqlBackupVerification %s phase %s timed out after %s: %s", name, want, time.Since(start).Round(time.Second), last)
		case <-progress.C:
			env.Logger.Info("wait", "what", "MysqlBackupVerification "+name, "last", last, "elapsed", time.Since(start).Round(time.Second))
		case <-tick.C:
		}
	}
}

func captureVerificationJobLogs(ctx context.Context, env *runner.Env, v *v1alpha1.MysqlBackupVerification) {
	if v.Status.JobName == "" {
		env.Capture.Note("verification failed before status.jobName was set")
		return
	}
	pods, err := env.Kube.Kubernetes.CoreV1().Pods(env.Namespace).List(ctx, metav1.ListOptions{LabelSelector: "job-name=" + v.Status.JobName})
	if err != nil {
		env.Capture.Note("list verification pods failed: " + err.Error())
		return
	}
	if len(pods.Items) == 0 {
		env.Capture.Note("no verification pods found for job " + v.Status.JobName)
		return
	}
	for _, pod := range pods.Items {
		containers := append([]corev1.Container{}, pod.Spec.InitContainers...)
		containers = append(containers, pod.Spec.Containers...)
		for _, c := range containers {
			req := env.Kube.Kubernetes.CoreV1().Pods(env.Namespace).GetLogs(pod.Name, &corev1.PodLogOptions{Container: c.Name, Previous: false})
			r, err := req.Stream(ctx)
			if err != nil {
				env.Capture.Note(fmt.Sprintf("read verification pod log %s/%s failed: %v", pod.Name, c.Name, err))
				continue
			}
			body, err := io.ReadAll(r)
			_ = r.Close()
			if err != nil {
				env.Capture.Note(fmt.Sprintf("read verification pod log stream %s/%s failed: %v", pod.Name, c.Name, err))
				continue
			}
			name := fmt.Sprintf("verification-%s-%s.log", pod.Name, c.Name)
			if err := env.Capture.WriteFile(name, body); err != nil {
				env.Capture.Note(fmt.Sprintf("write verification pod log %s failed: %v", name, err))
			}
		}
	}
}

func deleteBackupCRs(ctx context.Context, env *runner.Env, backupName, verificationName string) error {
	var errs []error
	if verificationName != "" {
		if err := deleteAndWait(ctx, env, &v1alpha1.MysqlBackupVerification{ObjectMeta: metav1.ObjectMeta{Name: verificationName, Namespace: env.Namespace}}, "MysqlBackupVerification", verificationName); err != nil {
			errs = append(errs, err)
		}
	}
	if backupName != "" {
		if err := deleteBackupAndAssertArtifactGone(ctx, env, backupName); err != nil {
			errs = append(errs, err)
		}
	}
	if len(errs) > 0 {
		return fmt.Errorf("delete backup CRs: %v", errs)
	}
	return nil
}

// deleteBackupAndAssertArtifactGone deletes a MysqlBackup and, for an S3
// backup, checks the artifact in RustFS itself: objects exist under its
// prefix before the delete, and none remain once the CR is gone. A cleanup
// that deleted nothing but exited successfully would otherwise pass.
func deleteBackupAndAssertArtifactGone(ctx context.Context, env *runner.Env, name string) error {
	var backup v1alpha1.MysqlBackup
	err := env.Kube.Controller.Get(ctx, types.NamespacedName{Namespace: env.Namespace, Name: name}, &backup)
	if apierrors.IsNotFound(err) {
		return nil
	}
	if err != nil {
		return fmt.Errorf("get MysqlBackup %s: %w", name, err)
	}
	bucket, prefix, ok, err := backupArtifactPrefix(&backup)
	if err != nil {
		return fmt.Errorf("MysqlBackup %s: %w", name, err)
	}
	if ok {
		keys, err := env.Chaos.ListRustFSKeys(ctx, bucket, prefix)
		if err != nil {
			return fmt.Errorf("list artifact of MysqlBackup %s before delete: %w", name, err)
		}
		if len(keys) == 0 {
			return fmt.Errorf("MysqlBackup %s: no objects under s3://%s/%s before delete; the storage assertion would prove nothing", name, bucket, prefix)
		}
		env.Capture.Note(fmt.Sprintf("MysqlBackup %s artifact: %d objects under s3://%s/%s", name, len(keys), bucket, prefix))
	}
	if err := deleteAndWait(ctx, env, &v1alpha1.MysqlBackup{ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: env.Namespace}}, "MysqlBackup", name); err != nil {
		return err
	}
	if !ok {
		return nil
	}
	keys, err := env.Chaos.ListRustFSKeys(ctx, bucket, prefix)
	if err != nil {
		return fmt.Errorf("list artifact of MysqlBackup %s after delete: %w", name, err)
	}
	if len(keys) > 0 {
		return fmt.Errorf("MysqlBackup %s is gone but %d objects remain under s3://%s/%s (first: %s)", name, len(keys), bucket, prefix, keys[0])
	}
	env.Capture.Note(fmt.Sprintf("MysqlBackup %s artifact removed from s3://%s/%s", name, bucket, prefix))
	return nil
}

// backupArtifactPrefix returns the bucket and object-key prefix (ending in
// "/") of an S3 backup's artifact, from the recorded destination. ok is
// false for a backup with no S3 artifact to check: one that did not
// succeed, one recorded on a PVC, or one with no recorded destination at
// all (a Succeeded backup whose destination could not be recovered, which
// the finalizer releases with ArtifactCleanupSkipped). An S3 destination
// that is recorded but lacks its location or bucket is an error.
func backupArtifactPrefix(b *v1alpha1.MysqlBackup) (bucket, prefix string, ok bool, err error) {
	storage := b.Status.ArtifactStorage
	if b.Status.Phase != v1alpha1.BackupPhaseSucceeded || storage == nil || storage.Type != v1alpha1.BackupStorageS3 {
		return "", "", false, nil
	}
	if storage.Location == "" || storage.Bucket == "" {
		return "", "", false, fmt.Errorf("succeeded S3 backup records status.artifactStorage without a location or bucket")
	}
	locBucket, key, inBucket := backupmeta.SplitS3Location(storage.Location)
	if inBucket && locBucket != storage.Bucket {
		return "", "", false, fmt.Errorf("location bucket %q differs from recorded bucket %q", locBucket, storage.Bucket)
	}
	key = strings.TrimRight(key, "/")
	if key == "" {
		return "", "", false, fmt.Errorf("empty artifact key in %q", storage.Location)
	}
	return storage.Bucket, key + "/", true, nil
}

func deleteAndWait(ctx context.Context, env *runner.Env, obj ctrlclient.Object, kind, name string) error {
	if err := env.Kube.Controller.Delete(ctx, obj); err != nil && !apierrors.IsNotFound(err) {
		return fmt.Errorf("delete %s %s: %w", kind, name, err)
	}
	deleteTimeout := 90 * time.Second
	if kind == "MysqlBackup" {
		// The finalizer runs an artifact-cleanup Job from the operator
		// image; allow for the first pull of that image in the namespace.
		deleteTimeout = 5 * time.Minute
	}
	waitCtx, cancel := context.WithTimeout(ctx, deleteTimeout)
	defer cancel()
	tick := time.NewTicker(time.Second)
	defer tick.Stop()
	for {
		fresh := obj.DeepCopyObject().(ctrlclient.Object)
		if err := env.Kube.Controller.Get(waitCtx, types.NamespacedName{Namespace: env.Namespace, Name: name}, fresh); apierrors.IsNotFound(err) {
			env.Capture.Note(fmt.Sprintf("deleted %s %s", kind, name))
			return nil
		} else if err != nil {
			return fmt.Errorf("get %s %s after delete: %w", kind, name, err)
		}
		select {
		case <-waitCtx.Done():
			if backup, ok := fresh.(*v1alpha1.MysqlBackup); ok {
				// Artifact cleanup did not finish: fail the scenario with
				// the operator's reason, then unstick the CR so later
				// scenarios start clean.
				cause := "no ArtifactCleanup condition"
				if c := apimeta.FindStatusCondition(backup.Status.Conditions, v1alpha1.ConditionArtifactCleanup); c != nil {
					cause = c.Reason + ": " + c.Message
				}
				if err := forceRemoveFinalizers(ctx, env, fresh, kind, name); err != nil {
					env.Capture.Note(fmt.Sprintf("force finalizer removal for %s %s failed: %v", kind, name, err))
				}
				return fmt.Errorf("artifact cleanup for %s %s did not finish within %s (%s)", kind, name, deleteTimeout, cause)
			}
			return fmt.Errorf("wait for %s %s deletion: %w", kind, name, waitCtx.Err())
		case <-tick.C:
		}
	}
}

func forceRemoveFinalizers(ctx context.Context, env *runner.Env, obj ctrlclient.Object, kind, name string) error {
	fresh := obj.DeepCopyObject().(ctrlclient.Object)
	if err := env.Kube.Controller.Get(ctx, types.NamespacedName{Namespace: env.Namespace, Name: name}, fresh); err != nil {
		if apierrors.IsNotFound(err) {
			return nil
		}
		return err
	}
	if len(fresh.GetFinalizers()) == 0 {
		return nil
	}
	fresh.SetFinalizers(nil)
	if err := env.Kube.Controller.Update(ctx, fresh); err != nil {
		return err
	}
	env.Capture.Note(fmt.Sprintf("force-removed finalizers from %s %s after cleanup timeout", kind, name))
	return nil
}

func dropMarkerSchemaAndReplicate(ctx context.Context, env *runner.Env, dbName string) error {
	waitCtx, cancel := context.WithTimeout(ctx, 3*time.Minute)
	defer cancel()
	mfg, err := waitForS04CleanupBaseline(waitCtx, env)
	if err != nil {
		return err
	}
	writable := mfg.Status.ActiveSite
	replica, err := PeerOf(mfg, writable)
	if err != nil {
		return fmt.Errorf("cleanup: resolve replica for %s: %w", writable, err)
	}
	primary, err := pgmysql.Open(ctx, env.Kube, env.Namespace, env.FG, writable, env.Creds)
	if err != nil {
		return fmt.Errorf("cleanup: open %s: %w", writable, err)
	}
	defer primary.Close()
	if _, err := primary.Exec(ctx, fmt.Sprintf("DROP DATABASE IF EXISTS %s", dbName)); err != nil {
		return fmt.Errorf("cleanup: drop database %s on %s: %w", dbName, writable, err)
	}
	gtid, err := primary.GtidExecuted(ctx)
	if err != nil {
		return fmt.Errorf("cleanup: read post-drop gtid on %s: %w", writable, err)
	}
	replicaClient, err := pgmysql.Open(ctx, env.Kube, env.Namespace, env.FG, replica, env.Creds)
	if err != nil {
		return fmt.Errorf("cleanup: open replica %s: %w", replica, err)
	}
	defer replicaClient.Close()
	if rc, err := replicaClient.ScalarInt(waitCtx, "SELECT WAIT_FOR_EXECUTED_GTID_SET(?, 30)", gtid); err != nil {
		return fmt.Errorf("cleanup: wait for schema drop on replica %s: %w", replica, err)
	} else if rc != 0 {
		return fmt.Errorf("cleanup: replica %s did not apply schema drop gtid within 30s (rc=%d gtid=%q)", replica, rc, gtid)
	}
	env.Capture.Note(fmt.Sprintf("cleanup: dropped %s on %s and replicated to %s", dbName, writable, replica))
	return nil
}

func activeAndReplica(ctx context.Context, env *runner.Env) (string, string, error) {
	waitCtx, cancel := context.WithTimeout(ctx, 45*time.Second)
	defer cancel()
	mfg, err := env.Wait.UntilCR(waitCtx, env.Namespace, "active site available", func(mfg *v1alpha1.MysqlFailoverGroup) (bool, string, error) {
		active := mfg.Status.ActiveSite
		if active == "" {
			return false, "activeSite is empty", nil
		}
		if _, err := PeerOf(mfg, active); err != nil {
			return false, "activeSite peer not available: " + err.Error(), nil
		}
		return true, "activeSite=" + active, nil
	})
	if err != nil {
		return "", "", err
	}
	active := mfg.Status.ActiveSite
	if active == "" {
		return "", "", fmt.Errorf("no active site in MFG status")
	}
	replica, err := PeerOf(mfg, active)
	if err != nil {
		return "", "", err
	}
	return active, replica, nil
}

func quoteSQLString(s string) string {
	return "'" + strings.ReplaceAll(s, "'", "''") + "'"
}

func waitForReplicaGTID(ctx context.Context, env *runner.Env, primarySite, replicaSite string) error {
	waitCtx, cancel := context.WithTimeout(ctx, 90*time.Second)
	defer cancel()
	var last string
	for {
		primary, err := pgmysql.Open(waitCtx, env.Kube, env.Namespace, env.FG, primarySite, env.Creds)
		if err != nil {
			last = fmt.Sprintf("open primary mysql: %v", err)
		} else {
			gtid, err := primary.GtidExecuted(waitCtx)
			_ = primary.Close()
			if err != nil {
				last = fmt.Sprintf("read primary gtid_executed: %v", err)
			} else {
				replica, err := pgmysql.Open(waitCtx, env.Kube, env.Namespace, env.FG, replicaSite, env.Creds)
				if err != nil {
					last = fmt.Sprintf("open replica mysql: %v", err)
				} else {
					rc, err := replica.ScalarInt(waitCtx, "SELECT WAIT_FOR_EXECUTED_GTID_SET(?, 30)", gtid)
					_ = replica.Close()
					if err != nil {
						last = fmt.Sprintf("WAIT_FOR_EXECUTED_GTID_SET on replica %s: %v", replicaSite, err)
					} else if rc != 0 {
						last = fmt.Sprintf("replica %s did not catch up within 30s (rc=%d gtid=%q)", replicaSite, rc, gtid)
					} else {
						env.Capture.Note(fmt.Sprintf("replica %s caught up to %s gtid=%q", replicaSite, primarySite, gtid))
						return nil
					}
				}
			}
		}
		select {
		case <-waitCtx.Done():
			return fmt.Errorf("wait for replica %s to catch up to %s: %w (last: %s)", replicaSite, primarySite, waitCtx.Err(), last)
		case <-time.After(2 * time.Second):
		}
	}
}

func originalPITRExpected(env *runner.Env) (enabled bool, profile string, err error) {
	rawHad := ctxFetch(env, backupOriginalHadKey)
	if rawHad == "" {
		return false, "", nil
	}
	hadOriginal, err := strconv.ParseBool(rawHad)
	if err != nil {
		return false, "", fmt.Errorf("parse original backup spec marker %q: %w", rawHad, err)
	}
	if !hadOriginal {
		return false, "", nil
	}
	var original v1alpha1.BackupSpec
	if err := json.Unmarshal([]byte(ctxFetch(env, backupOriginalSpecKey)), &original); err != nil {
		return false, "", fmt.Errorf("unmarshal original backup spec for PITR expectation: %w", err)
	}
	if original.PITR == nil || !original.PITR.Enabled {
		return false, "", nil
	}
	return true, original.PITR.ProfileName, nil
}

// pitrBaselineRestored is the PITR scenarios' cleanup predicate after
// restoreOriginalBackupSpec: the operator has reconciled the restored spec
// and finished rolling it out, and its PITR observation matches it.
//
// Reconciled: Ready=True at the current generation, an active site, no
// update phase, and Updating=False/NotUpdating at the current generation.
// A stalled ordered update (Updating=True/OrderedUpdateStalled, a pod that
// is not Ready) clears updatePhase while the rollout is unfinished, and
// MySQL can stay healthy throughout, so Ready alone does not end it.
//
// Rolled out (pitrRolloutRestored): every site Deployment renders the
// restored PITR state and has finished its rollout. Updating=False does not
// prove that on its own: drift the ordered updater has not started on yet
// (it waits for a standby, a stall retry, a bootstrap) has no phase either.
//
// PITR: with PITR restored off, status.pitr is gone (the operator removes
// it once PITR is disabled). With PITR restored on, the spec names the
// original profile and status.pitr.archive observes that profile's archive
// destination, not one the scenario left behind. Spec reads alone are true
// as soon as the patch lands, before the operator has acted on it.
//
// deployments maps each spec.sites name to its Deployment, nil when it
// could not be found.
func pitrBaselineRestored(mfg *v1alpha1.MysqlFailoverGroup, deployments map[string]*appsv1.Deployment, wantPITR bool, wantProfile string) (bool, string) {
	ready := conditionTrueForGeneration(mfg.Status.Conditions, v1alpha1.ConditionReady, mfg.Generation)
	idle, updating := orderedUpdateIdleAtGeneration(mfg)
	settled := backupProfileRolloutReady(mfg)
	observed, why := pitrObservationRestored(mfg, wantPITR, wantProfile)
	rolled, rollout := pitrRolloutRestored(mfg, deployments, wantPITR, wantProfile)
	return ready && idle && settled && observed && rolled, fmt.Sprintf("readyAtGeneration=%v generation=%d active=%q updatePhase=%q updating=%s pitr=%v (%s) rollout=%v (%s) wantPITR=%v wantProfile=%q",
		ready, mfg.Generation, mfg.Status.ActiveSite, mfg.Status.UpdatePhase, updating, observed, why, rolled, rollout, wantPITR, wantProfile)
}

// pitrBaselineRestoredLive is pitrBaselineRestored with the site
// Deployments read from the cluster. A failed read is a wait, not an
// error: the cleanup retries it.
func pitrBaselineRestoredLive(ctx context.Context, env *runner.Env, mfg *v1alpha1.MysqlFailoverGroup, wantPITR bool, wantProfile string) (bool, string) {
	deployments := make(map[string]*appsv1.Deployment, len(mfg.Spec.Sites))
	for _, site := range mfg.Spec.Sites {
		dep, err := env.Kube.GetDeployment(ctx, env.Namespace, fmt.Sprintf("mysql-%s-%s", mfg.Name, site.Name))
		if err != nil && !apierrors.IsNotFound(err) {
			return false, fmt.Sprintf("read Deployment of site %s: %v", site.Name, err)
		}
		if err != nil {
			dep = nil
		}
		deployments[site.Name] = dep
	}
	return pitrBaselineRestored(mfg, deployments, wantPITR, wantProfile)
}

// orderedUpdateIdleAtGeneration reports Updating=False/NotUpdating for the
// current generation, with a summary of the condition.
func orderedUpdateIdleAtGeneration(mfg *v1alpha1.MysqlFailoverGroup) (bool, string) {
	c := apimeta.FindStatusCondition(mfg.Status.Conditions, v1alpha1.ConditionUpdating)
	if c == nil {
		return false, "<none>"
	}
	summary := fmt.Sprintf("%s/%s@%d", c.Status, c.Reason, c.ObservedGeneration)
	return c.Status == metav1.ConditionFalse && c.Reason == v1alpha1.ReasonNotUpdating && c.ObservedGeneration == mfg.Generation, summary
}

// pitrRolloutRestored reports whether every spec.sites Deployment renders
// the restored PITR state (its pitr-render annotation: enabled as wanted
// and, when on, the restored profile's destination) and has finished
// rolling it out: the Deployment controller observed its generation and
// every replica is updated, ready and available.
func pitrRolloutRestored(mfg *v1alpha1.MysqlFailoverGroup, deployments map[string]*appsv1.Deployment, wantPITR bool, wantProfile string) (bool, string) {
	wantDest := ""
	if wantPITR {
		dest, err := pitrRestoredDestination(mfg, wantProfile)
		if err != nil {
			return false, err.Error()
		}
		wantDest = dest
	}
	var pending []string
	for _, site := range mfg.Spec.Sites {
		dep := deployments[site.Name]
		if dep == nil {
			pending = append(pending, site.Name+": no Deployment")
			continue
		}
		var render struct {
			Enabled     bool   `json:"enabled"`
			Destination string `json:"destination"`
		}
		if err := json.Unmarshal([]byte(dep.Annotations[v1alpha1.PITRRenderAnnotation]), &render); err != nil {
			pending = append(pending, fmt.Sprintf("%s: %s does not decode", site.Name, v1alpha1.PITRRenderAnnotation))
			continue
		}
		if render.Enabled != wantPITR || render.Destination != wantDest {
			pending = append(pending, fmt.Sprintf("%s: renders enabled=%v destination=%q", site.Name, render.Enabled, render.Destination))
			continue
		}
		want := int32(1)
		if dep.Spec.Replicas != nil {
			want = *dep.Spec.Replicas
		}
		st := dep.Status
		if st.ObservedGeneration < dep.Generation || st.UpdatedReplicas != want || st.ReadyReplicas != want ||
			st.AvailableReplicas != want || st.Replicas != want || st.UnavailableReplicas != 0 {
			pending = append(pending, fmt.Sprintf("%s: rollout observed=%d/%d updated=%d ready=%d available=%d replicas=%d want=%d",
				site.Name, st.ObservedGeneration, dep.Generation, st.UpdatedReplicas, st.ReadyReplicas, st.AvailableReplicas, st.Replicas, want))
		}
	}
	if len(pending) > 0 {
		return false, strings.Join(pending, "; ")
	}
	return true, "rolled out"
}

// pitrRestoredDestination is the normalized archive destination of the
// restored PITR profile.
func pitrRestoredDestination(mfg *v1alpha1.MysqlFailoverGroup, wantProfile string) (string, error) {
	var profile *v1alpha1.BackupProfile
	if mfg.Spec.Backup != nil {
		for i := range mfg.Spec.Backup.Profiles {
			if mfg.Spec.Backup.Profiles[i].Name == wantProfile {
				profile = &mfg.Spec.Backup.Profiles[i]
			}
		}
	}
	if profile == nil {
		return "", fmt.Errorf("profile %q not in spec", wantProfile)
	}
	dest, err := pitrprofile.DestinationFor(mfg, profile)
	if err != nil {
		return "", fmt.Errorf("restored profile destination: %v", err)
	}
	return dest.String(), nil
}

// pitrObservationRestored is the PITR half of pitrBaselineRestored.
func pitrObservationRestored(mfg *v1alpha1.MysqlFailoverGroup, wantPITR bool, wantProfile string) (bool, string) {
	if !wantPITR {
		if pitrSpecProfile(mfg) != "" {
			return false, "spec still enables PITR"
		}
		if mfg.Status.PITR != nil {
			return false, "status.pitr not yet removed"
		}
		return true, "disabled"
	}
	if got := pitrSpecProfile(mfg); got != wantProfile {
		return false, fmt.Sprintf("spec PITR profile %q", got)
	}
	dest, err := pitrRestoredDestination(mfg, wantProfile)
	if err != nil {
		return false, err.Error()
	}
	if mfg.Status.PITR == nil || mfg.Status.PITR.Archive == nil {
		return false, "no status.pitr.archive observation yet"
	}
	if got := mfg.Status.PITR.Archive.Destination; got != dest {
		return false, fmt.Sprintf("status.pitr.archive observes %s, want %s", got, dest)
	}
	return true, "archive=" + pitrArchiveSummary(mfg)
}

func backupStatusSummary(b *v1alpha1.MysqlBackup) string {
	location, storageType := "", v1alpha1.BackupStorageType("")
	if st := b.Status.ArtifactStorage; st != nil {
		location, storageType = st.Location, st.Type
	}
	binlog := "-"
	if bp := b.Status.BinlogPosition; bp != nil {
		binlog = fmt.Sprintf("%s:%d", bp.File, bp.Position)
	}
	return fmt.Sprintf("name=%s phase=%s message=%q job=%q source=%q location=%q storage=%s sizeBytes=%d gtid=%q binlog=%s conditions=%s",
		b.Name, b.Status.Phase, b.Status.Message, b.Status.JobName, b.Status.SourceSite, location, storageType, b.Status.SizeBytes, b.Status.GtidExecuted, binlog, conditionsSummary(b.Status.Conditions))
}

func verificationStatusSummary(v *v1alpha1.MysqlBackupVerification) string {
	backupRef := "<nil>"
	if v.Status.BackupRef != nil {
		backupRef = fmt.Sprintf("%s/%s", v.Status.BackupRef.Name, v.Status.BackupRef.UID)
	}
	return fmt.Sprintf("name=%s phase=%s message=%q backupRef=%s job=%q pointInTime=%v sanity=%v conditions=%s",
		v.Name, v.Status.Phase, v.Status.Message, backupRef, v.Status.JobName, v.Status.PointInTime, v.Status.SanityCheck, conditionsSummary(v.Status.Conditions))
}

func conditionsSummary(conds []metav1.Condition) string {
	parts := make([]string, 0, len(conds))
	for _, c := range conds {
		parts = append(parts, fmt.Sprintf("%s=%s/%s", c.Type, c.Status, c.Reason))
	}
	return strings.Join(parts, ",")
}

func conditionTrue(conds []metav1.Condition, typ string) bool {
	for _, c := range conds {
		if c.Type == typ && c.Status == metav1.ConditionTrue {
			return true
		}
	}
	return false
}

func conditionTrueForGeneration(conds []metav1.Condition, typ string, generation int64) bool {
	for _, c := range conds {
		if c.Type == typ && c.Status == metav1.ConditionTrue && c.ObservedGeneration == generation {
			return true
		}
	}
	return false
}

func archiverSummary(st *pgsidecar.ArchiverStatusResponse) string {
	if st == nil {
		return "<nil>"
	}
	return fmt.Sprintf("enabled=%v primary=%v site=%s storage=%s prefix=%s lastScan=%s files=%d manifestFiles=%d backlog=%d newest=%s lastUpload=%s failures=%d lastError=%q tail=%s",
		st.Enabled, st.Primary, st.Site, st.StorageType, st.ManifestPrefix, st.LastScanAt.Format(time.RFC3339Nano), st.FilesArchived, st.ManifestFileCount, st.BacklogFiles, st.NewestArchivedTime.Format(time.RFC3339Nano), st.LastUploadAt.Format(time.RFC3339Nano), st.UploadFailures, st.LastError, archiverTailSummary(st.Tail))
}

// archiverTailSummary renders the tail block of /archiver/status (#264).
func archiverTailSummary(t *pgsidecar.ArchiverTailResponse) string {
	if t == nil {
		return "<none>"
	}
	return fmt.Sprintf("{%s active=%s newestSealed=%s pending=%d reason=%q}", t.State, t.ActiveFile, t.NewestSealedFile, t.PendingFiles, t.Reason)
}
