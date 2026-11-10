package controller

import (
	"context"
	"fmt"
	"strings"
	"testing"

	"sigs.k8s.io/controller-runtime/pkg/client"

	v1alpha1 "github.com/shipstream/bloodraven/api/v1alpha1"
)

// recreateRemedy is the phrase every initFromBackup refusal whose cause is
// in the block uses (restoreRemedy).
const recreateRemedy = "delete the group and create it again"

// editRemedies are the edit instructions a restoreInPlace refusal gives.
// spec.initFromBackup is fixed at creation, so none of them is executable
// there.
var editRemedies = []string{
	"remove skipBinlog",
	"remove the schema filters",
	"name the dump's directory in",
	"exclude fewer GTIDs",
	"set initFromBackup.",
	"correct initFromBackup.",
}

// C12: every RestoreBuildFailed refusal on the initFromBackup path whose
// cause is in spec.initFromBackup gives the recreate remedy once, never an
// edit admission refuses, and the Ready condition built from it states the
// remedy and the recovery URL once.
func TestInitFromBackupRefusalsGiveTheRecreateRemedyOnce(t *testing.T) {
	SetOperatorImageDefaults("bloodraven:test", "bloodraven")
	defer SetOperatorImageDefaults("", "")
	f := false
	const u = "3e11fa47-71ca-11e1-9e33-c80aa9429562"
	manyTags := func(n int) string {
		var b strings.Builder
		b.WriteString(u)
		for i := 0; i < n; i++ {
			fmt.Fprintf(&b, ":t%d:1", i)
		}
		return b.String()
	}
	pitrFromBackupRef := func(mut func(*v1alpha1.InitFromBackupSpec)) (*v1alpha1.MysqlFailoverGroup, []client.Object) {
		fg := fgInitFromMysqlBackup()
		fg.Spec.Backup.PITR = &v1alpha1.PITRSpec{Enabled: true, ProfileName: "nightly-s3"}
		fg.Spec.InitFromBackup.PointInTime = &v1alpha1.PointInTimeSpec{StopTime: stopAt("2026-04-15T09:30:00Z"), ArchiveProfileName: addSourceArchiveProfile(fg)}
		mut(fg.Spec.InitFromBackup)
		return fg, []client.Object{succeededSeedBackup()}
	}
	for _, tc := range []struct {
		name      string
		build     func() (*v1alpha1.MysqlFailoverGroup, []client.Object)
		wantToken string
		wantCause string
	}{
		{
			name: "skipBinlog false with pointInTime",
			build: func() (*v1alpha1.MysqlFailoverGroup, []client.Object) {
				return pitrFromBackupRef(func(in *v1alpha1.InitFromBackupSpec) {
					in.LoadOptions = &v1alpha1.LoadOptions{SkipBinlog: &f}
				})
			},
			wantCause: "initFromBackup.loadOptions.skipBinlog=false is incompatible",
		},
		{
			name: "schema filter with pointInTime",
			build: func() (*v1alpha1.MysqlFailoverGroup, []client.Object) {
				return pitrFromBackupRef(func(in *v1alpha1.InitFromBackupSpec) {
					in.LoadOptions = &v1alpha1.LoadOptions{IncludeSchemas: []string{"app"}}
				})
			},
			wantToken: "PITRBackupFiltered",
			wantCause: "cannot be combined with initFromBackup.pointInTime",
		},
		{
			name: "excludeGtids over the env limit",
			build: func() (*v1alpha1.MysqlFailoverGroup, []client.Object) {
				return pitrFromBackupRef(func(in *v1alpha1.InitFromBackupSpec) {
					in.PointInTime.ExcludeGtids = manyTags(4000)
				})
			},
			wantToken: "PITRTargetInvalid",
			wantCause: "over the 65536-byte limit",
		},
		{
			name: "direct S3 dump at the root of its store",
			build: func() (*v1alpha1.MysqlFailoverGroup, []client.Object) {
				fg := fgWithBackup()
				fg.Spec.Backup.PITR = &v1alpha1.PITRSpec{Enabled: true, ProfileName: "nightly-s3"}
				fg.Spec.InitFromBackup = &v1alpha1.InitFromBackupSpec{Source: v1alpha1.RestoreSource{
					S3: &v1alpha1.S3Storage{Bucket: "b", CredentialsSecretName: "aws"},
				}}
				fg.Spec.InitFromBackup.PointInTime = &v1alpha1.PointInTimeSpec{StopTime: stopAt("2026-04-15T09:30:00Z"), ArchiveProfileName: addSourceArchiveProfile(fg)}
				return fg, nil
			},
			wantToken: "PITRBackupIdentityUnknown",
			wantCause: "root of its store",
		},
		{
			name: "backupRef profile no longer supplies S3 credentials",
			build: func() (*v1alpha1.MysqlFailoverGroup, []client.Object) {
				fg := fgInitFromMysqlBackup()
				seed := succeededSeedBackup()
				seed.Spec.ProfileName = "gone"
				return fg, []client.Object{seed}
			},
			wantCause: `profile "gone" is missing from spec.backup.profiles`,
		},
		{
			name: "encrypted backupRef whose recorded dump key is gone",
			build: func() (*v1alpha1.MysqlFailoverGroup, []client.Object) {
				fg := fgWithEncryptedBackup()
				fg.Spec.InitFromBackup = &v1alpha1.InitFromBackupSpec{Source: v1alpha1.RestoreSource{BackupRef: &v1alpha1.LocalMysqlBackupRef{Name: "seed"}}}
				seed := encryptedRefSeed("nightly-s3", v1alpha1.BackupStorageS3)
				seed.Status.ArtifactStorage.PassphraseSecretKeyRef = &v1alpha1.SecretKeyRef{Name: "seed-dump-key", Key: pitrKeySnapshotDataKey}
				return fg, []client.Object{seed}
			},
			wantToken: "PITRBackupDumpKeyMissing",
			wantCause: "it recorded as its dump key is gone",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			fg, extra := tc.build()
			r, _ := newReconciler(append([]client.Object{fg}, extra...)...)
			_, err := r.buildRestoreJob(context.Background(), fg, fg.Spec.Sites[0].Name, "creds")
			if err == nil {
				t.Fatal("buildRestoreJob succeeded, want a refusal")
			}
			msg := err.Error()
			if tc.wantToken != "" && !strings.HasPrefix(msg, tc.wantToken+": ") {
				t.Errorf("refusal %q does not start with %s", msg, tc.wantToken)
			}
			if !strings.Contains(msg, tc.wantCause) {
				t.Errorf("refusal %q does not name the cause %q", msg, tc.wantCause)
			}
			assertOneRecreateRemedy(t, "RestoreBuildFailed", msg)

			fg.Status.Restore = &v1alpha1.RestoreStatus{Phase: v1alpha1.BackupPhaseFailed, Reason: pitrFailureReason(msg), Message: msg}
			_, ready := initRestoreReadyCondition(fg)
			assertOneRecreateRemedy(t, "Ready", ready)
		})
	}
}

// A refusal whose cause may lie outside spec.initFromBackup names no
// remedy; the Ready condition adds the fixed-at-creation note once.
func TestInitRestoreReadyConditionAddsTheNoteOnce(t *testing.T) {
	fg := fgInitFromMysqlBackup()
	fg.Status.Restore = &v1alpha1.RestoreStatus{Phase: v1alpha1.BackupPhaseFailed, Message: `get referenced mysqlbackup: mysqlbackups "seed" not found`}
	_, ready := initRestoreReadyCondition(fg)
	if strings.Count(ready, initFromBackupRecoveryDoc) != 1 || strings.Count(ready, recreateRemedy) != 1 {
		t.Fatalf("Ready message %q: want the recovery URL and the recreate remedy once each", ready)
	}
}

// C21: a failed Job's recovery names the pods and the data claims to
// delete with the labels the operator puts on them, and the URL once.
func TestInitRestoreRecoveryEmptiesPodsAndClaims(t *testing.T) {
	fg := fgInitFromMysqlBackup()
	fg.Status.Restore = &v1alpha1.RestoreStatus{Phase: v1alpha1.BackupPhaseFailed, JobName: "mysql-lion-dc1-restore", TargetSite: "dc1", Message: "load failed"}
	_, ready := initRestoreReadyCondition(fg)
	want := fmt.Sprintf("delete the group's MySQL pods and PVCs (kubectl delete pvc,pod -l app.kubernetes.io/instance=%s,app.kubernetes.io/name=mysql -n %s), "+
		"wait until the operator has re-created every claim Bound and every pod Running, then delete the restore Job", fg.Name, fg.Namespace)
	if !strings.Contains(ready, want) {
		t.Fatalf("Ready message %q\nwant it to contain %q", ready, want)
	}
	if strings.Count(ready, initFromBackupRecoveryDoc) != 1 {
		t.Fatalf("Ready message %q: want the recovery URL once", ready)
	}
	labels := commonLabels(fg.Name, "dc1")
	if labels[labelInstance] != fg.Name || labels[labelAppName] != "mysql" {
		t.Fatalf("data PVC and pod labels %v no longer carry the selector the remedy names", labels)
	}
}

// restoreInPlace stays editable, so its refusals keep the edit remedies.
func TestRestoreInPlaceRefusalsKeepEditRemedies(t *testing.T) {
	for _, tc := range []struct{ got, want string }{
		{restoreSpecFix("restoreInPlace", "source.s3"), "set restoreInPlace.source.s3"},
		{restoreRemedy("restoreInPlace", "remove skipBinlog or set it to true", "x"), "remove skipBinlog or set it to true"},
		{pitrLoadFilterRefusal("restoreInPlace", &v1alpha1.LoadOptions{ExcludeSchemas: []string{"a"}}).Error(), "; remove the schema filters"},
	} {
		if !strings.HasSuffix(tc.got, tc.want) || strings.Contains(tc.got, recreateRemedy) || strings.Contains(tc.got, initFromBackupRecoveryDoc) {
			t.Errorf("restoreInPlace remedy %q, want it to end %q and not recreate the group", tc.got, tc.want)
		}
	}
}

func assertOneRecreateRemedy(t *testing.T, where, msg string) {
	t.Helper()
	if n := strings.Count(msg, recreateRemedy); n != 1 {
		t.Errorf("%s message carries the recreate remedy %d times, want 1: %q", where, n, msg)
	}
	if n := strings.Count(msg, initFromBackupRecoveryDoc); n != 1 {
		t.Errorf("%s message carries the recovery URL %d times, want 1: %q", where, n, msg)
	}
	for _, edit := range editRemedies {
		if strings.Contains(msg, edit) {
			t.Errorf("%s message tells the user to edit the fixed block (%q): %q", where, edit, msg)
		}
	}
}
