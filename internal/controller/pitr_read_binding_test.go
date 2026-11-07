package controller

import (
	"context"
	"strings"
	"testing"

	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"

	v1alpha1 "github.com/shipstream/bloodraven/api/v1alpha1"
	"github.com/shipstream/bloodraven/internal/pitrarchive"
)

const testArchiveSnapshot = "mysql-lion-pitr-key-0123456789"

// readBindingVerification is a PITR verification of a plaintext backup in
// a group whose nightly-s3 archive is encrypted and bound in state/reason.
func readBindingVerification(t *testing.T, state v1alpha1.PITRArchiveBindingState, reason v1alpha1.PITRArchiveBindingReason, extra ...client.Object) (*MysqlBackupVerificationReconciler, client.Client, *v1alpha1.MysqlBackupVerification) {
	t.Helper()
	fg := fgWithEncryptedBackup()
	fg.UID = "lion-uid"
	fg.Spec.Backup.PITR = &v1alpha1.PITRSpec{Enabled: true, ProfileName: "nightly-s3"}
	e := bindTestPITRArchive(t, fg, "nightly-s3")
	e.State, e.Reason = state, reason
	backup := successfulBackup("seed", "lion", "nightly-s3")
	backup.Status.FailoverGroupUID = "lion-uid"
	v := verifyCR("verify-binding", "lion", "nightly-s3")
	v.UID = "verify-binding-uid"
	v.Spec.PointInTime = &v1alpha1.PointInTimeVerificationSpec{Mode: v1alpha1.PointInTimeModeTimestamp, StopTime: stopAtPtr(outcomeStop)}
	r, c := newVerificationReconciler(t, append([]client.Object{fg, backup, v, operatorCredsSecret()}, extra...)...)
	return r, c, v
}

func archiveSnapshot(fgUID string) *corev1.Secret {
	return &corev1.Secret{ObjectMeta: metav1.ObjectMeta{Namespace: "ns", Name: testArchiveSnapshot,
		Labels: map[string]string{labelAppName: appNamePITRKeySnapshot, v1alpha1.FailoverGroupUIDLabel: fgUID, v1alpha1.FailoverGroupLabel: "lion"}},
		Data: map[string][]byte{pitrKeySnapshotDataKey: []byte("k")}}
}

func getVerification(t *testing.T, c client.Client, v *v1alpha1.MysqlBackupVerification) *v1alpha1.MysqlBackupVerification {
	t.Helper()
	var got v1alpha1.MysqlBackupVerification
	if err := c.Get(context.Background(), client.ObjectKeyFromObject(v), &got); err != nil {
		t.Fatal(err)
	}
	return &got
}

// Δ22 for verifications (rows 26, 26a): fixable binding states wait (no
// Job, message names the binding), unfixable ones and BindingLimitReached
// fail with PITRArchiveBindingNotReady.
func TestVerificationReadBindingWaitOrFail(t *testing.T) {
	for _, tc := range []struct {
		state  v1alpha1.PITRArchiveBindingState
		reason v1alpha1.PITRArchiveBindingReason
		fail   bool
	}{
		{v1alpha1.PITRArchiveBindingPending, v1alpha1.PITRArchiveBindingReasonAuthenticating, false},
		{v1alpha1.PITRArchiveBindingRefused, v1alpha1.PITRArchiveBindingReasonKeyMismatch, false},
		{v1alpha1.PITRArchiveBindingRefused, v1alpha1.PITRArchiveBindingReasonArchiveListFailed, false},
		{v1alpha1.PITRArchiveBindingBound, v1alpha1.PITRArchiveBindingReasonKeyMaterialLost, false},
		{v1alpha1.PITRArchiveBindingRefused, v1alpha1.PITRArchiveBindingReasonArchiveNotFound, true},
		{v1alpha1.PITRArchiveBindingRefused, v1alpha1.PITRArchiveBindingReasonArchiveLegacy, true},
	} {
		t.Run(string(tc.state)+"/"+string(tc.reason), func(t *testing.T) {
			SetOperatorImageDefaults("bloodraven:test", "bloodraven")
			defer SetOperatorImageDefaults("", "")
			r, c, v := readBindingVerification(t, tc.state, tc.reason)
			reconcileVerifUntilStable(t, r, v.Name)
			got := getVerification(t, c, v)
			var job batchv1.Job
			if err := c.Get(context.Background(), types.NamespacedName{Namespace: "ns", Name: verificationJobName(v.Name)}, &job); err == nil {
				t.Fatal("a Job was created on a binding that is not ready")
			}
			if tc.fail {
				if got.Status.Phase != v1alpha1.VerificationPhaseFailed || verifiedReason(got) != string(v1alpha1.PITRTokenArchiveBindingNotReady) {
					t.Fatalf("phase %s reason %q, want Failed/%s", got.Status.Phase, verifiedReason(got), string(v1alpha1.PITRTokenArchiveBindingNotReady))
				}
				return
			}
			if verificationTerminal(got.Status.Phase) || !strings.HasPrefix(got.Status.Message, string(v1alpha1.PITRTokenArchiveBindingNotReady)) {
				t.Fatalf("phase %s message %q, want a wait naming the binding", got.Status.Phase, got.Status.Message)
			}
		})
	}

	t.Run("BindingLimitReached fails a verification", func(t *testing.T) {
		SetOperatorImageDefaults("bloodraven:test", "bloodraven")
		defer SetOperatorImageDefaults("", "")
		fg := fgWithEncryptedBackup()
		fg.Spec.Backup.PITR = &v1alpha1.PITRSpec{Enabled: true, ProfileName: "nightly-s3"}
		for i := 0; i < pitrArchiveBindingsMax; i++ {
			fg.Status.PITRArchiveBindings = append(fg.Status.PITRArchiveBindings, v1alpha1.PITRArchiveBinding{ID: "pab-000000000000000" + string(rune('a'+i%6)),
				State: v1alpha1.PITRArchiveBindingBound, Reason: v1alpha1.PITRArchiveBindingReasonBound})
		}
		backup := successfulBackup("seed", "lion", "nightly-s3")
		v := verifyCR("verify-limit", "lion", "nightly-s3")
		v.Spec.PointInTime = &v1alpha1.PointInTimeVerificationSpec{Mode: v1alpha1.PointInTimeModeLatest}
		r, c := newVerificationReconciler(t, fg, backup, v, operatorCredsSecret())
		reconcileVerifUntilStable(t, r, v.Name)
		if got := getVerification(t, c, v); got.Status.Phase != v1alpha1.VerificationPhaseFailed || verifiedReason(got) != string(v1alpha1.PITRTokenArchiveBindingNotReady) {
			t.Fatalf("phase %s reason %q", got.Status.Phase, verifiedReason(got))
		}
	})

	// U10: a list full of entries that are not Bound frees a slot on the
	// group's next binding pass (unused ones are dropped): the
	// verification waits instead of failing on a transient count.
	t.Run("a list full of non-Bound entries waits", func(t *testing.T) {
		SetOperatorImageDefaults("bloodraven:test", "bloodraven")
		defer SetOperatorImageDefaults("", "")
		fg := fgWithEncryptedBackup()
		fg.Spec.Backup.PITR = &v1alpha1.PITRSpec{Enabled: true, ProfileName: "nightly-s3"}
		for i := 0; i < pitrArchiveBindingsMax; i++ {
			state, reason := v1alpha1.PITRArchiveBindingBound, v1alpha1.PITRArchiveBindingReasonBound
			if i == 0 {
				state, reason = v1alpha1.PITRArchiveBindingPending, v1alpha1.PITRArchiveBindingReasonAuthenticating
			}
			fg.Status.PITRArchiveBindings = append(fg.Status.PITRArchiveBindings, v1alpha1.PITRArchiveBinding{ID: "pab-000000000000000" + string(rune('a'+i%6)),
				State: state, Reason: reason})
		}
		backup := successfulBackup("seed", "lion", "nightly-s3")
		v := verifyCR("verify-limit", "lion", "nightly-s3")
		v.Spec.PointInTime = &v1alpha1.PointInTimeVerificationSpec{Mode: v1alpha1.PointInTimeModeLatest}
		r, c := newVerificationReconciler(t, fg, backup, v, operatorCredsSecret())
		reconcileVerifUntilStable(t, r, v.Name)
		got := getVerification(t, c, v)
		if verificationTerminal(got.Status.Phase) || !strings.Contains(got.Status.Message, "make room") {
			t.Fatalf("phase %s message %q, want a wait", got.Status.Phase, got.Status.Message)
		}
	})
}

// K9, §4.3: a verification holds the archive key snapshot its Job mounts
// (stamped before the Job, confirmed, label on the Job), releases the hold
// once its execution stopped, and waits when a release deleted the snapshot
// first (row 28a).
func TestVerificationHoldsArchiveKeySnapshot(t *testing.T) {
	SetOperatorImageDefaults("bloodraven:test", "bloodraven")
	defer SetOperatorImageDefaults("", "")
	ctx := context.Background()

	t.Run("stamped, labelled, released", func(t *testing.T) {
		r, c, v := readBindingVerification(t, v1alpha1.PITRArchiveBindingBound, v1alpha1.PITRArchiveBindingReasonBound, archiveSnapshot("lion-uid"))
		reconcileVerifUntilStable(t, r, v.Name)
		var job batchv1.Job
		if err := c.Get(ctx, types.NamespacedName{Namespace: "ns", Name: verificationJobName(v.Name)}, &job); err != nil {
			t.Fatal(err)
		}
		if job.Labels[v1alpha1.PITRKeySecretLabel] != testArchiveSnapshot {
			t.Fatalf("Job label %s = %q", v1alpha1.PITRKeySecretLabel, job.Labels[v1alpha1.PITRKeySecretLabel])
		}
		if vol := findVolume(job.Spec.Template.Spec.Volumes, "pitr-passphrase"); vol == nil || vol.Secret.SecretName != testArchiveSnapshot ||
			vol.Secret.Optional != nil || vol.Secret.Items[0].Key != pitrKeySnapshotDataKey {
			t.Fatalf("pitr-download mounts %+v, want the required snapshot", vol)
		}
		var snap corev1.Secret
		if err := c.Get(ctx, types.NamespacedName{Namespace: "ns", Name: testArchiveSnapshot}, &snap); err != nil {
			t.Fatal(err)
		}
		got := getVerification(t, c, v)
		if snap.Annotations[verificationHoldKey(got)] != v.Name {
			t.Fatalf("snapshot hold annotations %v", snap.Annotations)
		}
		// Execution stopped: the Job finished and no pod runs.
		job.Status.Conditions = []batchv1.JobCondition{{Type: batchv1.JobFailed, Status: corev1.ConditionTrue}}
		if err := c.Status().Update(ctx, &job); err != nil {
			t.Fatal(err)
		}
		if done, err := r.releaseVerificationHold(ctx, got); err != nil || !done {
			t.Fatalf("release = %t, %v", done, err)
		}
		if err := c.Get(ctx, types.NamespacedName{Namespace: "ns", Name: testArchiveSnapshot}, &snap); err != nil {
			t.Fatal(err)
		}
		if _, held := snap.Annotations[verificationHoldKey(got)]; held {
			t.Fatal("hold kept after execution stopped")
		}
	})

	t.Run("row 28a: snapshot released first, verification waits", func(t *testing.T) {
		r, c, v := readBindingVerification(t, v1alpha1.PITRArchiveBindingBound, v1alpha1.PITRArchiveBindingReasonBound)
		reconcileVerifUntilStable(t, r, v.Name)
		var job batchv1.Job
		if err := c.Get(ctx, types.NamespacedName{Namespace: "ns", Name: verificationJobName(v.Name)}, &job); err == nil {
			t.Fatal("a Job was created without the key snapshot")
		}
		got := getVerification(t, c, v)
		if verificationTerminal(got.Status.Phase) || !strings.Contains(got.Status.Message, testArchiveSnapshot) {
			t.Fatalf("phase %s message %q, want a wait naming the snapshot", got.Status.Phase, got.Status.Message)
		}
	})

	t.Run("another group's Secret of that name is not stamped", func(t *testing.T) {
		r, c, v := readBindingVerification(t, v1alpha1.PITRArchiveBindingBound, v1alpha1.PITRArchiveBindingReasonBound, archiveSnapshot("other-uid"))
		for i := 0; i < 6; i++ {
			_, _ = r.Reconcile(ctx, ctrl.Request{NamespacedName: client.ObjectKeyFromObject(v)})
		}
		var snap corev1.Secret
		if err := c.Get(ctx, types.NamespacedName{Namespace: "ns", Name: testArchiveSnapshot}, &snap); err != nil {
			t.Fatal(err)
		}
		if len(snap.Annotations) != 0 {
			t.Fatalf("stamped another group's Secret: %v", snap.Annotations)
		}
	})
}

// Δ28, row 41: archiveProfileName reads a retained archive through its own
// Bound binding, even with that profile's live Secret gone.
func TestVerificationArchiveProfileName(t *testing.T) {
	fg := fgWithEncryptedBackup()
	fg.UID = "lion-uid"
	fg.Spec.Backup.PITR = &v1alpha1.PITRSpec{Enabled: true, ProfileName: "nightly-s3"}
	retained := *fg.Spec.Backup.Profiles[0].DeepCopy()
	retained.Name = "retained"
	retained.Storage.S3.Prefix = "old-prefix"
	retained.Encryption.PassphraseSecretKeyRef.Name = "deleted-secret"
	fg.Spec.Backup.Profiles = append(fg.Spec.Backup.Profiles, retained)
	e := bindTestPITRArchive(t, fg, "retained")
	e.Encryption.KeySecretName = "mysql-lion-pitr-key-retained00"
	v := verifyCR("verify-retained", "lion", "nightly-s3")
	v.Spec.PointInTime = &v1alpha1.PointInTimeVerificationSpec{Mode: v1alpha1.PointInTimeModeLatest, ArchiveProfileName: "retained"}
	backup := successfulBackup("seed", "lion", "nightly-s3")
	job, err := buildVerificationJob(verificationJobInputs{FailoverGroup: fg, Profile: fg.Spec.Backup.Profiles[0], Verification: v, Backup: backup, CredsSecretName: "c", ScriptsConfigMapName: "s"})
	if err != nil {
		t.Fatal(err)
	}
	dl := containerEnv(job.Spec.Template.Spec.InitContainers, restorePITRInitContainerName)
	if dl["BLOODRAVEN_PITR_MANIFEST_PREFIX"] != "old-prefix/binlogs" {
		t.Fatalf("read prefix %q", dl["BLOODRAVEN_PITR_MANIFEST_PREFIX"])
	}
	if vol := findVolume(job.Spec.Template.Spec.Volumes, "pitr-passphrase"); vol == nil || vol.Secret.SecretName != "mysql-lion-pitr-key-retained00" {
		t.Fatalf("mounts %+v, want the retained binding's snapshot", vol)
	}
}

// Δ17 (row 32): an encrypted backup is verified with the key it recorded,
// never the profile's (since edited) Secret; with none recorded the
// verification is refused PITRBackupDumpKeyMissing.
func TestVerificationDumpKeyIsRecorded(t *testing.T) {
	SetOperatorImageDefaults("bloodraven:test", "bloodraven")
	defer SetOperatorImageDefaults("", "")
	fg := fgWithEncryptedBackup()
	fg.Spec.Backup.Profiles[0].Encryption.PassphraseSecretKeyRef.Name = "edited-since"
	backup := successfulBackup("seed", "lion", "nightly-s3")
	setTestArtifactLocation(backup, "s3://bloodraven-backups/lion/seed/")
	setTestArtifactType(backup, v1alpha1.BackupStorageS3)
	backup.Status.Encryption = &v1alpha1.BackupEncryptionStatus{Mode: v1alpha1.EncryptionModePassphrase, Algorithm: v1alpha1.EncryptionAlgorithmAES256GCM}
	ensureTestArtifact(backup).PassphraseSecretKeyRef = &v1alpha1.SecretKeyRef{Name: "mysqlbackup-seed-dump-key-uid-seed", Key: pitrKeySnapshotDataKey}
	v := verifyCR("verify-dump", "lion", "nightly-s3")
	job, err := buildVerificationJob(verificationJobInputs{FailoverGroup: fg, Profile: fg.Spec.Backup.Profiles[0], Verification: v, Backup: backup, CredsSecretName: "c", ScriptsConfigMapName: "s"})
	if err != nil {
		t.Fatal(err)
	}
	if vol := findVolume(job.Spec.Template.Spec.Volumes, "backup-passphrase"); vol == nil || vol.Secret.SecretName != "mysqlbackup-seed-dump-key-uid-seed" || vol.Secret.Items[0].Key != pitrKeySnapshotDataKey {
		t.Fatalf("decrypt mounts %+v, want the recorded snapshot", vol)
	}
	backup.Status.ArtifactStorage.PassphraseSecretKeyRef = nil
	if _, err := buildVerificationJob(verificationJobInputs{FailoverGroup: fg, Profile: fg.Spec.Backup.Profiles[0], Verification: v, Backup: backup, CredsSecretName: "c", ScriptsConfigMapName: "s"}); err == nil ||
		!strings.Contains(err.Error(), string(pitrarchive.TokenBackupDumpKeyMissing)) {
		t.Fatalf("no recorded key: %v", err)
	}
}

// Δ22 for initFromBackup restores (rows 26, 26a): fixable states keep the
// restore Pending with the binding named; unfixable ones fail with
// PITRArchiveBindingNotReady. A Bound binding mounts its snapshot and labels
// the Job (Δ18).
func TestRestoreReadBindingWaitOrFail(t *testing.T) {
	ctx := context.Background()
	for _, tc := range []struct {
		state  v1alpha1.PITRArchiveBindingState
		reason v1alpha1.PITRArchiveBindingReason
		fail   bool
		bound  bool
	}{
		{state: v1alpha1.PITRArchiveBindingPending, reason: v1alpha1.PITRArchiveBindingReasonAuthenticating},
		{state: v1alpha1.PITRArchiveBindingRefused, reason: v1alpha1.PITRArchiveBindingReasonKeyMismatch},
		{state: v1alpha1.PITRArchiveBindingRefused, reason: v1alpha1.PITRArchiveBindingReasonArchiveDescriptorMissing, fail: true},
		{state: v1alpha1.PITRArchiveBindingRefused, reason: v1alpha1.PITRArchiveBindingReasonArchiveVersionUnsupported, fail: true},
		{state: v1alpha1.PITRArchiveBindingBound, reason: v1alpha1.PITRArchiveBindingReasonBound, bound: true},
	} {
		t.Run(string(tc.state)+"/"+string(tc.reason), func(t *testing.T) {
			fg := fgInitFromMysqlBackup()
			seedPublished(fg)
			seed := succeededSeedBackup()
			seed.Status.FailoverGroupUID = outcomeSourceUID
			fg.Spec.Backup.PITR = &v1alpha1.PITRSpec{Enabled: true, ProfileName: seed.Spec.ProfileName}
			name := addSourceArchiveProfile(fg)
			src := findProfile(fg, name)
			src.Encryption = &v1alpha1.BackupEncryptionSpec{PassphraseSecretKeyRef: v1alpha1.PassphraseSecretKeyRef{Name: "source-key"}}
			fg.Status.PITRArchiveBindings = nil
			e := bindTestPITRArchive(t, fg, name)
			e.State, e.Reason = tc.state, tc.reason
			fg.Spec.InitFromBackup.PointInTime = &v1alpha1.PointInTimeSpec{StopTime: stopAt(outcomeStop), ArchiveProfileName: name}
			r, c := newReconciler(fg, seed, primaryReadyDeployment("lion", fg.Spec.Sites[0].Name), operatorCredsSecret())
			if _, err := r.reconcileRestoreJob(ctx, fg); err != nil {
				t.Fatal(err)
			}
			var got v1alpha1.MysqlFailoverGroup
			if err := c.Get(ctx, client.ObjectKeyFromObject(fg), &got); err != nil {
				t.Fatal(err)
			}
			var job batchv1.Job
			jobErr := c.Get(ctx, client.ObjectKey{Namespace: "ns", Name: restoreJobName("lion", fg.Spec.Sites[0].Name)}, &job)
			switch {
			case tc.bound:
				if jobErr != nil {
					t.Fatal(jobErr)
				}
				if job.Labels[v1alpha1.PITRKeySecretLabel] != e.Encryption.KeySecretName {
					t.Fatalf("restore Job label %q, want %q", job.Labels[v1alpha1.PITRKeySecretLabel], e.Encryption.KeySecretName)
				}
				if vol := findVolume(job.Spec.Template.Spec.Volumes, "pitr-passphrase"); vol == nil || vol.Secret.SecretName != e.Encryption.KeySecretName || vol.Secret.Optional != nil {
					t.Fatalf("restore mounts %+v, want the required snapshot", vol)
				}
			case tc.fail:
				if got.Status.Restore == nil || got.Status.Restore.Phase != v1alpha1.BackupPhaseFailed || got.Status.Restore.Reason != v1alpha1.PITRTokenArchiveBindingNotReady {
					t.Fatalf("restore %+v, want Failed/%s", got.Status.Restore, string(v1alpha1.PITRTokenArchiveBindingNotReady))
				}
			default:
				if jobErr == nil {
					t.Fatal("a restore Job was created on a binding that is not ready")
				}
				if got.Status.Restore == nil || got.Status.Restore.Phase != v1alpha1.BackupPhasePending ||
					!strings.HasPrefix(got.Status.Restore.Message, string(v1alpha1.PITRTokenArchiveBindingNotReady)) {
					t.Fatalf("restore %+v, want Pending naming the binding", got.Status.Restore)
				}
			}
		})
	}
}

// Δ17: an encrypted backupRef restore decrypts with the explicit
// decryption Secret, else the recorded key (which must exist), and never
// with the profile's live Secret.
func TestRestoreDumpKeyOrder(t *testing.T) {
	SetOperatorImageDefaults("bloodraven:test", "bloodraven")
	defer SetOperatorImageDefaults("", "")
	ctx := context.Background()
	fg := fgWithEncryptedBackup()
	fg.Spec.InitFromBackup = &v1alpha1.InitFromBackupSpec{Source: v1alpha1.RestoreSource{BackupRef: &v1alpha1.LocalMysqlBackupRef{Name: "seed"}}}
	seed := encryptedRefSeed("nightly-s3", v1alpha1.BackupStorageS3)
	seed.Status.ArtifactStorage.PassphraseSecretKeyRef = &v1alpha1.SecretKeyRef{Name: "seed-dump-key", Key: pitrKeySnapshotDataKey}
	dumpKey := &corev1.Secret{ObjectMeta: metav1.ObjectMeta{Namespace: "ns", Name: "seed-dump-key"}, Data: map[string][]byte{pitrKeySnapshotDataKey: []byte("k")}}

	decryptSecret := func(job *batchv1.Job) string {
		if vol := findVolume(job.Spec.Template.Spec.Volumes, "backup-passphrase"); vol != nil && vol.Secret != nil {
			return vol.Secret.SecretName
		}
		return ""
	}
	r, _ := newReconciler(fg, seed, dumpKey)
	job, err := r.buildRestoreJob(ctx, fg, fg.Spec.Sites[0].Name, "creds")
	if err != nil {
		t.Fatal(err)
	}
	if got := decryptSecret(job); got != "seed-dump-key" {
		t.Fatalf("decrypts with %q, want the recorded key", got)
	}

	r, _ = newReconciler(fg, seed) // recorded key gone
	if _, err := r.buildRestoreJob(ctx, fg, fg.Spec.Sites[0].Name, "creds"); err == nil || !strings.Contains(err.Error(), string(pitrarchive.TokenBackupDumpKeyMissing)) {
		t.Fatalf("recorded key gone: %v, want %s (no profile fallback)", err, pitrarchive.TokenBackupDumpKeyMissing)
	}

	explicit := fg.DeepCopy()
	explicit.Spec.InitFromBackup.Decryption = &v1alpha1.BackupDecryptionSpec{PassphraseSecretKeyRef: v1alpha1.PassphraseSecretKeyRef{Name: "restore-pass", Key: "passphrase"}}
	r, _ = newReconciler(explicit, seed)
	job, err = r.buildRestoreJob(ctx, explicit, explicit.Spec.Sites[0].Name, "creds")
	if err != nil {
		t.Fatal(err)
	}
	if got := decryptSecret(job); got != "restore-pass" {
		t.Fatalf("decrypts with %q, want the explicit decryption Secret", got)
	}
}
