package controller

import (
	"context"
	"strings"
	"testing"

	appsv1 "k8s.io/api/apps/v1"
	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/tools/record"
	"sigs.k8s.io/controller-runtime/pkg/client"

	v1alpha1 "github.com/shipstream/bloodraven/api/v1alpha1"
	"github.com/shipstream/bloodraven/internal/backupmeta"
)

// encryptedBackupFixture is an encrypted, writable group and a MysqlBackup
// of its encrypted profile, ready to launch.
func encryptedBackupFixture(t *testing.T, extra ...client.Object) (*MysqlBackupReconciler, client.Client, *v1alpha1.MysqlFailoverGroup, *v1alpha1.MysqlBackup) {
	t.Helper()
	SetOperatorImageDefaults("bloodraven:test", "bloodraven")
	t.Cleanup(func() { SetOperatorImageDefaults("", "") })
	fg := fgWithEncryptedBackup()
	fg.UID = "lion-uid"
	fg.Status.ActiveSite = "iad"
	fg.Status.Sites = []v1alpha1.SiteStatus{{Name: "iad", State: "writable"}}
	mb := backupCR("dk", fg.Name, "nightly-s3")
	mb.UID = "0123abcd-dk-uid"
	r, c := newBackupReconciler(t, append([]client.Object{fg, mb, dsnSecret()}, extra...)...)
	return r, c, fg, mb
}

func getBackup(t *testing.T, c client.Client, mb *v1alpha1.MysqlBackup) *v1alpha1.MysqlBackup {
	t.Helper()
	var got v1alpha1.MysqlBackup
	if err := c.Get(context.Background(), client.ObjectKeyFromObject(mb), &got); err != nil {
		t.Fatal(err)
	}
	return &got
}

// I-17, Δ1, Δ24: an encrypted backup's Job mounts its own immutable,
// ownerless dump-key snapshot holding the trimmed live bytes, created
// before the Job and recorded in status.artifactStorage.passphraseSecret.
func TestDumpKeySnapshotCreatedBeforeJob(t *testing.T) {
	ctx := context.Background()
	r, c, fg, mb := encryptedBackupFixture(t)
	reconcileUntilStable(t, r, mb.Name)

	name := dumpKeySnapshotName(mb)
	if name != "mysqlbackup-dk-dump-key-0123abcd" {
		t.Fatalf("snapshot name %q", name)
	}
	var snap corev1.Secret
	if err := c.Get(ctx, types.NamespacedName{Namespace: mb.Namespace, Name: name}, &snap); err != nil {
		t.Fatal(err)
	}
	if snap.Immutable == nil || !*snap.Immutable || len(snap.OwnerReferences) != 0 {
		t.Fatalf("snapshot immutable=%v owners=%v, want immutable and ownerless", snap.Immutable, snap.OwnerReferences)
	}
	if string(snap.Data[pitrKeySnapshotDataKey]) != testPassphrase {
		t.Fatalf("snapshot bytes %q, want the trimmed live bytes", snap.Data[pitrKeySnapshotDataKey])
	}
	for k, v := range map[string]string{labelAppName: appNameDumpKeySnapshot, v1alpha1.MysqlBackupUIDLabel: string(mb.UID),
		v1alpha1.FailoverGroupUIDLabel: string(fg.UID), v1alpha1.FailoverGroupLabel: fg.Name, v1alpha1.MysqlBackupLabel: mb.Name} {
		if snap.Labels[k] != v {
			t.Fatalf("label %s = %q, want %q", k, snap.Labels[k], v)
		}
	}
	var job batchv1.Job
	if err := c.Get(ctx, types.NamespacedName{Namespace: mb.Namespace, Name: backupJobName(mb.Name)}, &job); err != nil {
		t.Fatal(err)
	}
	if got := backupJobPassphrase(&job); got == nil || got.Name != name || got.Key != pitrKeySnapshotDataKey {
		t.Fatalf("Job mounts %+v, want the snapshot", got)
	}
	if got := getBackup(t, c, mb).Status.ArtifactStorage; got == nil || got.PassphraseSecret == nil || got.PassphraseSecret.Name != name {
		t.Fatalf("recorded dump key %+v, want %s", got, name)
	}
}

// K2 (row 50): an existing snapshot labelled with this backup's UID is
// adopted as is, without comparing its bytes with the (since edited) live
// Secret; another backup's Secret of that name is a conflict; a missing live
// Secret is a pend.
func TestDumpKeySnapshotAdoptConflictMissing(t *testing.T) {
	ctx := context.Background()
	for _, tc := range []struct {
		name       string
		existing   func(mb *v1alpha1.MysqlBackup) *corev1.Secret
		dropSecret bool
		wantReason string
	}{
		{name: "row 50: adopted without a byte comparison", existing: func(mb *v1alpha1.MysqlBackup) *corev1.Secret {
			return &corev1.Secret{ObjectMeta: metav1.ObjectMeta{Namespace: mb.Namespace, Name: dumpKeySnapshotName(mb),
				Labels: map[string]string{labelAppName: appNameDumpKeySnapshot, v1alpha1.MysqlBackupUIDLabel: string(mb.UID)}},
				Data: map[string][]byte{pitrKeySnapshotDataKey: []byte("bytes-at-snapshot-time")}}
		}},
		{name: "another backup's Secret", existing: func(mb *v1alpha1.MysqlBackup) *corev1.Secret {
			return &corev1.Secret{ObjectMeta: metav1.ObjectMeta{Namespace: mb.Namespace, Name: dumpKeySnapshotName(mb),
				Labels: map[string]string{labelAppName: appNameDumpKeySnapshot, v1alpha1.MysqlBackupUIDLabel: "someone-else"}}}
		}, wantReason: v1alpha1.ReasonDumpKeySnapshotConflict},
		{name: "live Secret missing", dropSecret: true, wantReason: v1alpha1.ReasonPassphraseSecretMissing},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var extra []client.Object
			mbProbe := backupCR("dk", "lion", "nightly-s3")
			mbProbe.UID = "0123abcd-dk-uid"
			if tc.existing != nil {
				extra = append(extra, tc.existing(mbProbe))
			}
			if tc.dropSecret {
				// A Secret of the profile's name without the key.
				extra = append(extra, &corev1.Secret{ObjectMeta: metav1.ObjectMeta{Namespace: "ns", Name: "backup-passphrase"}})
			}
			r, c, _, mb := encryptedBackupFixture(t, extra...)
			reconcileUntilStable(t, r, mb.Name)
			got := getBackup(t, c, mb)
			var job batchv1.Job
			jobErr := c.Get(ctx, types.NamespacedName{Namespace: mb.Namespace, Name: backupJobName(mb.Name)}, &job)
			if tc.wantReason != "" {
				if jobErr == nil {
					t.Fatal("a Job was created")
				}
				if got.Status.Phase != v1alpha1.BackupPhasePending || !strings.Contains(conditionReasons(got.Status.Conditions), tc.wantReason) {
					t.Fatalf("phase %s conditions %v, want Pending/%s", got.Status.Phase, got.Status.Conditions, tc.wantReason)
				}
				return
			}
			if jobErr != nil {
				t.Fatal(jobErr)
			}
			var snap corev1.Secret
			if err := c.Get(ctx, types.NamespacedName{Namespace: mb.Namespace, Name: dumpKeySnapshotName(mb)}, &snap); err != nil {
				t.Fatal(err)
			}
			if string(snap.Data[pitrKeySnapshotDataKey]) != "bytes-at-snapshot-time" {
				t.Fatal("the adopted snapshot was rewritten")
			}
		})
	}
}

func conditionReasons(conds []metav1.Condition) string {
	var parts []string
	for _, c := range conds {
		parts = append(parts, c.Reason)
	}
	return strings.Join(parts, ",")
}

// K5 (row 49): while the site Deployments render different archives, a
// backup waits Pending/PITRArchiveRolloutPending instead of stamping one.
func TestBackupWaitsForPITRRollout(t *testing.T) {
	fg := fgWithBackup()
	fg.UID = "lion-uid"
	fg.Status.ActiveSite = "iad"
	fg.Status.Sites = []v1alpha1.SiteStatus{{Name: "iad", State: "writable"}}
	fg.Spec.Backup.PITR = &v1alpha1.PITRSpec{Enabled: true, ProfileName: "nightly-s3"}
	mb := backupCR("rollout", fg.Name, "nightly-s3")
	mb.UID = "rollout-uid"
	render := testPITRRender(fg)
	var deps []client.Object
	for i, site := range fg.Spec.Sites {
		ann := pitrRenderDisabled()
		if i == 0 {
			ann = render
		}
		d := siteDeploy(fg, site.Name, ann)
		deps = append(deps, &d)
	}
	if !render.Enabled {
		t.Fatal("fixture: the group renders no archive")
	}
	r, c := newBackupReconciler(t, append([]client.Object{fg, mb, dsnSecret()}, deps...)...)
	reconcileUntilStable(t, r, mb.Name)
	got := getBackup(t, c, mb)
	if got.Status.Phase != v1alpha1.BackupPhasePending || !strings.Contains(conditionReasons(got.Status.Conditions), v1alpha1.ReasonPITRArchiveRolloutPending) {
		t.Fatalf("phase %s conditions %v, want Pending/%s", got.Status.Phase, got.Status.Conditions, v1alpha1.ReasonPITRArchiveRolloutPending)
	}

	// Once they agree, the stamp names the rendered archive.
	var d appsv1.Deployment
	if err := c.Get(context.Background(), types.NamespacedName{Namespace: fg.Namespace, Name: resourceName(fg.Name, fg.Spec.Sites[1].Name)}, &d); err != nil {
		t.Fatal(err)
	}
	d.Annotations[v1alpha1.PITRRenderAnnotation] = render.Canonical()
	if err := c.Update(context.Background(), &d); err != nil {
		t.Fatal(err)
	}
	reconcileUntilStable(t, r, mb.Name)
	if got := getBackup(t, c, mb); got.Status.PITRArchiveID != render.ArchiveID {
		t.Fatalf("stamped archive %q, want the rendered %q", got.Status.PITRArchiveID, render.ArchiveID)
	}
}

// D-10 (row 40): a backup of a read-only profile fails ProfileReadOnly.
func TestBackupOfReadOnlyProfileFails(t *testing.T) {
	fg := fgWithBackup()
	fg.UID = "lion-uid"
	fg.Status.ActiveSite = "iad"
	fg.Status.Sites = []v1alpha1.SiteStatus{{Name: "iad", State: "writable"}}
	e := bindTestPITRArchive(t, fg, "nightly-s3")
	e.Source.UID = "another-group"
	mb := backupCR("ro", fg.Name, "nightly-s3")
	r, c := newBackupReconciler(t, fg, mb, dsnSecret())
	reconcileUntilStable(t, r, mb.Name)
	got := getBackup(t, c, mb)
	if got.Status.Phase != v1alpha1.BackupPhaseFailed || !strings.Contains(conditionReasons(got.Status.Conditions), v1alpha1.ReasonProfileReadOnly) {
		t.Fatalf("phase %s conditions %v, want Failed/%s", got.Status.Phase, got.Status.Conditions, v1alpha1.ReasonProfileReadOnly)
	}
}

// Δ24 (row 51): the dump-key snapshot is deleted once the cleanup Job
// removed the artifacts, and kept (and named) when cleanup is skipped.
func TestDumpKeySnapshotDeletedOnlyAfterCleanup(t *testing.T) {
	ctx := context.Background()
	snapFor := func(mb *v1alpha1.MysqlBackup) *corev1.Secret {
		return &corev1.Secret{ObjectMeta: metav1.ObjectMeta{Namespace: mb.Namespace, Name: dumpKeySnapshotName(mb),
			Labels: map[string]string{labelAppName: appNameDumpKeySnapshot, v1alpha1.MysqlBackupUIDLabel: string(mb.UID)}}}
	}
	exists := func(t *testing.T, c client.Client, mb *v1alpha1.MysqlBackup) bool {
		var s corev1.Secret
		return c.Get(ctx, types.NamespacedName{Namespace: mb.Namespace, Name: dumpKeySnapshotName(mb)}, &s) == nil
	}

	t.Run("cleanup succeeded", func(t *testing.T) {
		f := newCleanupFixture(t)
		if err := f.c.Create(ctx, snapFor(f.mb)); err != nil {
			t.Fatal(err)
		}
		f.finalize(t)
		job, _ := f.cleanupJob(t)
		job.UID = "cleanup-ok"
		job.Status.Conditions = []batchv1.JobCondition{{Type: batchv1.JobComplete, Status: corev1.ConditionTrue}}
		if err := f.c.Status().Update(ctx, job); err != nil {
			t.Fatal(err)
		}
		if exists(t, f.c, f.mb) == false {
			t.Fatal("snapshot deleted while cleanup ran")
		}
		if err := f.c.Create(ctx, cleanupPodFor(job, corev1.PodSucceeded, 0, backupmeta.FormatCleanupComplete(3))); err != nil {
			t.Fatal(err)
		}
		if done, _ := f.finalize(t); !done {
			t.Fatal("cleanup did not finish")
		}
		if exists(t, f.c, f.mb) {
			t.Fatal("snapshot kept after its artifacts were removed")
		}
	})

	t.Run("group gone: skipped, kept and named", func(t *testing.T) {
		f := newCleanupFixture(t)
		recordTestDumpKey(f.mb)
		f.mb.Status.ArtifactStorage.PassphraseSecret.Name = dumpKeySnapshotName(f.mb)
		if err := f.c.Status().Update(ctx, f.mb); err != nil {
			t.Fatal(err)
		}
		if err := f.c.Create(ctx, snapFor(f.mb)); err != nil {
			t.Fatal(err)
		}
		if err := f.c.Delete(ctx, f.fg); err != nil {
			t.Fatal(err)
		}
		drainEvents(f.rec)
		if done, _ := f.finalize(t); !done {
			t.Fatal("skipped cleanup held the finalizer")
		}
		if !exists(t, f.c, f.mb) {
			t.Fatal("snapshot deleted although the artifacts may remain")
		}
		if countEvents(f.rec, dumpKeySnapshotName(f.mb)) != 1 {
			t.Fatal("the skip Event does not name the kept snapshot")
		}
	})

	t.Run("another backup's same-name Secret is never deleted", func(t *testing.T) {
		f := newCleanupFixture(t)
		s := snapFor(f.mb)
		s.Labels[v1alpha1.MysqlBackupUIDLabel] = "predecessor"
		if err := f.c.Create(ctx, s); err != nil {
			t.Fatal(err)
		}
		if err := f.r.deleteDumpKeySnapshot(ctx, f.mb); err != nil {
			t.Fatal(err)
		}
		if !exists(t, f.c, f.mb) {
			t.Fatal("deleted a Secret labelled for another backup")
		}
	})
}

// Δ17: an abandoned cleanup reads the stamp with the recorded key, which
// must exist; a missing one blocks (skips, abandoned) as
// PassphraseSecretMissing naming it, never falling back to the profile.
func TestAbandonedCleanupRecordedKeyMissing(t *testing.T) {
	r, _ := newBackupReconciler(t)
	mb := backupCR("ab", "lion", "nightly-s3")
	storage := &v1alpha1.BackupArtifactStorage{PassphraseSecret: &v1alpha1.BackupArtifactPassphrase{Name: "gone", Key: "passphrase"}}
	profile := fgWithEncryptedBackup().Spec.Backup.Profiles[0]
	vol, missing, err := r.abandonedCleanupPassphrase(context.Background(), mb, &profile, storage, nil)
	if err != nil || vol != nil || !strings.Contains(missing, `"gone"`) {
		t.Fatalf("got %v %q %v, want a missing report naming the recorded Secret", vol, missing, err)
	}
	// No attribution at all: the profile's Secret is the only fallback.
	mb.Status.Encrypted = true
	vol, missing, err = r.abandonedCleanupPassphrase(context.Background(), mb, &profile, nil, nil)
	if err != nil || missing != "" || vol == nil || vol.SecretName != profile.Encryption.PassphraseSecret.Name {
		t.Fatalf("no-attribution fallback: %v %q %v", vol, missing, err)
	}
}

// G2: an operator crash between the backup Job's create and the status
// patch leaves status with no Job and no artifact destination; the Job
// may have uploaded encrypted artifacts and then gone (foreground
// deletion). Finalization has no proof the artifacts are gone, so it keeps
// the dump-key snapshot (possibly their only key) and names it, instead of
// deleting it as if no Job had ever run.
func TestDumpKeySnapshotKeptWithoutProducerAttribution(t *testing.T) {
	ctx := context.Background()
	r, c, _, mb := encryptedBackupFixture(t)
	rec := record.NewFakeRecorder(20)
	r.Recorder = rec
	reconcileUntilStable(t, r, mb.Name)
	name := dumpKeySnapshotName(mb)
	var job batchv1.Job
	if err := c.Get(ctx, types.NamespacedName{Namespace: mb.Namespace, Name: backupJobName(mb.Name)}, &job); err != nil {
		t.Fatalf("the backup Job was not created: %v", err)
	}

	// The status patch after the create is lost (crash), and the Job,
	// having uploaded, is gone.
	got := getBackup(t, c, mb)
	got.Status = v1alpha1.MysqlBackupStatus{Phase: v1alpha1.BackupPhasePending}
	if err := c.Status().Update(ctx, got); err != nil {
		t.Fatal(err)
	}
	if err := c.Delete(ctx, &job); err != nil {
		t.Fatal(err)
	}
	drainEvents(rec)

	got = getBackup(t, c, mb)
	done, _, err := r.finalize(ctx, got)
	if err != nil || !done {
		t.Fatalf("finalize = %v, %v; want released", done, err)
	}
	var snap corev1.Secret
	if err := c.Get(ctx, types.NamespacedName{Namespace: mb.Namespace, Name: name}, &snap); err != nil {
		t.Fatalf("the dump-key snapshot was deleted without proof that its artifacts are gone: %v", err)
	}
	if countEvents(rec, name) != 1 {
		t.Fatal("no Event names the kept dump-key snapshot")
	}
}

// F-B: backups never pend on a disagreement that only the user can end,
// and a dump-only profile never pends on PITR at all. Each case runs the
// backup Job without an archive stamp and says so (PITRArchiveNotStamped);
// only a PITR-profile backup during a roll waits (TestBackupWaitsForPITRRollout).
func TestBackupRunsUnstampedWhenWaitingCannotHelp(t *testing.T) {
	for _, tc := range []struct {
		name    string
		profile string
		// renders are the site Deployments' pitr-render annotations ("" for
		// a pre-#203 Deployment); nil means the default roll in progress.
		renders func(fg *v1alpha1.MysqlFailoverGroup) []any
	}{
		{name: "pre-#203 deferral, PITR profile", profile: "nightly-s3", renders: func(*v1alpha1.MysqlFailoverGroup) []any { return []any{nil, nil} }},
		{name: "pre-#203 deferral, dump-only profile", profile: "daily-local", renders: func(*v1alpha1.MysqlFailoverGroup) []any { return []any{nil, nil} }},
		{name: "roll in progress, dump-only profile", profile: "daily-local", renders: func(fg *v1alpha1.MysqlFailoverGroup) []any {
			return []any{testPITRRender(fg), pitrRenderDisabled()}
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			fg := fgWithBackup()
			fg.UID = "lion-uid"
			fg.Status.ActiveSite = "iad"
			fg.Status.Sites = []v1alpha1.SiteStatus{{Name: "iad", State: "writable"}}
			fg.Spec.Backup.PITR = &v1alpha1.PITRSpec{Enabled: true, ProfileName: "nightly-s3"}
			mb := backupCR("unstamped", fg.Name, tc.profile)
			mb.UID = "unstamped-uid"
			objs := []client.Object{fg, mb, dsnSecret()}
			for i, render := range tc.renders(fg) {
				d := siteDeploy(fg, fg.Spec.Sites[i].Name, render)
				objs = append(objs, &d)
			}
			r, c := newBackupReconciler(t, objs...)
			rec := record.NewFakeRecorder(20)
			r.Recorder = rec
			reconcileUntilStable(t, r, mb.Name)
			got := getBackup(t, c, mb)
			if got.Status.Phase == v1alpha1.BackupPhasePending || got.Status.JobName == "" {
				t.Fatalf("phase %s conditions %v: the backup waits on PITR", got.Status.Phase, got.Status.Conditions)
			}
			if got.Status.PITRArchiveID != "" {
				t.Fatalf("stamped archive %q, want none", got.Status.PITRArchiveID)
			}
			if countEvents(rec, v1alpha1.EventPITRArchiveNotStamped) != 1 {
				t.Fatal("no PITRArchiveNotStamped Event")
			}
		})
	}
}
