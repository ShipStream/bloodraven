package controller

import (
	"context"
	"fmt"
	"reflect"
	"strings"
	"testing"
	"time"

	v1alpha1 "github.com/shipstream/bloodraven/api/v1alpha1"
	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	apiMeta "k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"
)

func TestVerificationSource_RemainsPinnedWhenNewBackupCompletes(t *testing.T) {
	for _, beforeJob := range []bool{false, true} {
		t.Run(map[bool]string{false: "running-job", true: "before-job-creation"}[beforeJob], func(t *testing.T) {
			ctx := context.Background()
			fg := verifyFG()
			a := successfulBackup("backup-a", fg.Name, "nightly-s3")
			a.Status.Location = "prod_backups/backup-a"
			a.Status.CompletionTime = &metav1.Time{Time: time.Now().Add(-time.Hour)}
			v := verifyCR("pinned", fg.Name, "nightly-s3")
			v.UID = "pinned-verification-uid"
			v.Spec.TTLSecondsAfterFinished = 3600
			r, c := newVerificationReconciler(t, fg, a, v, dsnSecret())
			key := client.ObjectKeyFromObject(v)
			if beforeJob {
				for i := 0; i < 6; i++ {
					if _, err := r.Reconcile(ctx, ctrl.Request{NamespacedName: key}); err != nil {
						t.Fatal(err)
					}
					if err := c.Get(ctx, key, v); err != nil {
						t.Fatal(err)
					}
					if v.Status.Phase == v1alpha1.VerificationPhaseProvisioning {
						break
					}
				}
			} else {
				reconcileVerifUntilStable(t, r, v.Name)
			}
			want := &v1alpha1.VerificationBackupRef{Name: a.Name, UID: string(a.UID), Location: a.Status.Location, StorageType: a.Status.StorageType}
			b := successfulBackup("backup-b", fg.Name, "nightly-s3")
			b.Status.Location = "prod_backups/backup-b"
			if err := c.Create(ctx, b); err != nil {
				t.Fatal(err)
			}
			r = &MysqlBackupVerificationReconciler{Client: c, Scheme: r.Scheme, Recorder: r.Recorder}
			reconcileVerifUntilStable(t, r, v.Name)
			if err := c.Get(ctx, key, v); err != nil {
				t.Fatal(err)
			}
			if !reflect.DeepEqual(v.Status.BackupRef, want) {
				t.Fatalf("new backup changed execution attribution: got %+v want %+v", v.Status.BackupRef, want)
			}
			var job batchv1.Job
			if err := c.Get(ctx, client.ObjectKey{Name: verificationJobName(v.Name), Namespace: v.Namespace}, &job); err != nil {
				t.Fatal(err)
			}
			if envMap(job.Spec.Template.Spec.Containers[0].Env)["BLOODRAVEN_INPUT_URL"] != a.Status.Location {
				t.Fatal("Job loaded a different backup than the pinned reference")
			}
			job.Status.Conditions = []batchv1.JobCondition{{Type: batchv1.JobComplete, Status: corev1.ConditionTrue, LastTransitionTime: metav1.Now()}}
			if err := c.Status().Update(ctx, &job); err != nil {
				t.Fatal(err)
			}
			reconcileVerifUntilStable(t, r, v.Name)
			if err := c.Get(ctx, key, v); err != nil {
				t.Fatal(err)
			}
			if v.Status.Phase != v1alpha1.VerificationPhaseSucceeded || !reflect.DeepEqual(v.Status.BackupRef, want) {
				t.Fatalf("verification certified an untested backup: %+v", v.Status)
			}
		})
	}
}

func provisionVerificationSource(t *testing.T, r *MysqlBackupVerificationReconciler, c client.Client, v *v1alpha1.MysqlBackupVerification) {
	t.Helper()
	for i := 0; i < 6; i++ {
		if _, err := r.Reconcile(context.Background(), ctrl.Request{NamespacedName: client.ObjectKeyFromObject(v)}); err != nil {
			t.Fatal(err)
		}
		if err := c.Get(context.Background(), client.ObjectKeyFromObject(v), v); err != nil {
			t.Fatal(err)
		}
		if v.Status.Phase == v1alpha1.VerificationPhaseProvisioning {
			return
		}
	}
	t.Fatalf("verification never provisioned: %+v", v.Status)
}

func TestVerificationSource_RejectsMissingOrReplacedPinBeforeLaunch(t *testing.T) {
	for _, mode := range []string{"deleted-latest", "replaced-latest", "replaced-explicit"} {
		t.Run(mode, func(t *testing.T) {
			ctx := context.Background()
			fg := verifyFG()
			a := successfulBackup("backup-a", fg.Name, "nightly-s3")
			v := verifyCR("pinned", fg.Name, "nightly-s3")
			if mode == "replaced-explicit" {
				v.Spec.BackupRef = &corev1.LocalObjectReference{Name: a.Name}
			}
			r, c := newVerificationReconciler(t, fg, a, v, dsnSecret())
			provisionVerificationSource(t, r, c, v)
			if err := c.Delete(ctx, a); err != nil {
				t.Fatal(err)
			}
			if strings.HasPrefix(mode, "replaced") {
				b := a.DeepCopy()
				b.ResourceVersion = ""
				b.UID = "replacement-uid"
				b.Status.Location = "other/artifact"
				if err := c.Create(ctx, b); err != nil {
					t.Fatal(err)
				}
			}
			reconcileVerifUntilStable(t, r, v.Name)
			if err := c.Get(ctx, client.ObjectKeyFromObject(v), v); err != nil {
				t.Fatal(err)
			}
			condition := apiMeta.FindStatusCondition(v.Status.Conditions, ConditionVerified)
			if v.Status.Phase != v1alpha1.VerificationPhaseFailed || v.Status.BackupRef.UID != string(a.UID) || condition == nil || condition.Reason != "BackupNotAvailable" {
				t.Fatalf("unavailable pin was replaced: %+v", v.Status)
			}
			var jobs batchv1.JobList
			if err := c.List(ctx, &jobs); err != nil || len(jobs.Items) != 0 {
				t.Fatalf("Job launched for a different backup: err=%v jobs=%d", err, len(jobs.Items))
			}
		})
	}
}

func TestVerificationSource_BuildsFromPinnedDestination(t *testing.T) {
	ctx := context.Background()
	fg := verifyFG()
	a := successfulBackup("backup-a", fg.Name, "nightly-s3")
	a.Status.Location = "original/artifact"
	v := verifyCR("pinned", fg.Name, "nightly-s3")
	r, c := newVerificationReconciler(t, fg, a, v, dsnSecret())
	provisionVerificationSource(t, r, c, v)
	a.Status.Location = "changed/artifact"
	a.Status.StorageType = v1alpha1.BackupStoragePVC
	if err := c.Status().Update(ctx, a); err != nil {
		t.Fatal(err)
	}
	v.Spec.BackupRef = &corev1.LocalObjectReference{Name: "new-explicit-ref"}
	if err := c.Update(ctx, v); err != nil {
		t.Fatal(err)
	}
	reconcileVerifUntilStable(t, r, v.Name)
	var job batchv1.Job
	if err := c.Get(ctx, client.ObjectKey{Namespace: v.Namespace, Name: verificationJobName(v.Name)}, &job); err != nil {
		t.Fatal(err)
	}
	if envMap(job.Spec.Template.Spec.Containers[0].Env)["BLOODRAVEN_INPUT_URL"] != "original/artifact" {
		t.Fatal("launch used mutable status/spec instead of the pinned destination")
	}
}

func TestVerificationSource_JobSnapshotAndLegacySafety(t *testing.T) {
	for _, mode := range []string{"matching-legacy", "mismatched-legacy", "missing-status", "drifted-status", "invalid-snapshot"} {
		t.Run(mode, func(t *testing.T) {
			ctx := context.Background()
			fg := verifyFG()
			a := successfulBackup("backup-a", fg.Name, "nightly-s3")
			a.Status.Location = "backup-a/artifact"
			v := verifyCR("pinned", fg.Name, "nightly-s3")
			v.Spec.TTLSecondsAfterFinished = 3600
			r, c := newVerificationReconciler(t, fg, a, v, dsnSecret())
			reconcileVerifUntilStable(t, r, v.Name)
			if err := c.Get(ctx, client.ObjectKeyFromObject(v), v); err != nil {
				t.Fatal(err)
			}
			var job batchv1.Job
			if err := c.Get(ctx, client.ObjectKey{Namespace: v.Namespace, Name: verificationJobName(v.Name)}, &job); err != nil {
				t.Fatal(err)
			}
			if strings.Contains(mode, "legacy") {
				delete(job.Spec.Template.Annotations, annotationVerificationBackup)
			}
			if mode == "invalid-snapshot" {
				job.Spec.Template.Annotations[annotationVerificationBackup] = "invalid JSON"
			}
			if err := c.Update(ctx, &job); err != nil {
				t.Fatal(err)
			}
			job.Status.Conditions = []batchv1.JobCondition{{Type: batchv1.JobComplete, Status: corev1.ConditionTrue, LastTransitionTime: metav1.Now()}}
			if err := c.Status().Update(ctx, &job); err != nil {
				t.Fatal(err)
			}
			if mode == "missing-status" {
				v.Status.BackupRef = nil
			} else if mode == "drifted-status" || mode == "mismatched-legacy" {
				v.Status.BackupRef = &v1alpha1.VerificationBackupRef{Name: "backup-b", UID: "uid-b", Location: "backup-b/artifact", StorageType: v1alpha1.BackupStorageS3}
			}
			if err := c.Status().Update(ctx, v); err != nil {
				t.Fatal(err)
			}
			if err := c.Delete(ctx, a); err != nil {
				t.Fatal(err)
			}
			reconcileVerifUntilStable(t, r, v.Name)
			if err := c.Get(ctx, client.ObjectKeyFromObject(v), v); err != nil {
				t.Fatal(err)
			}
			if mode == "mismatched-legacy" || mode == "invalid-snapshot" {
				condition := apiMeta.FindStatusCondition(v.Status.Conditions, ConditionVerified)
				if v.Status.Phase != v1alpha1.VerificationPhaseFailed || condition == nil || condition.Reason != "BackupIdentityMismatch" {
					t.Fatalf("unprovable execution source was certified: %+v", v.Status)
				}
			} else if v.Status.Phase != v1alpha1.VerificationPhaseSucceeded || v.Status.BackupRef.Name != a.Name || v.Status.BackupRef.UID != string(a.UID) {
				t.Fatalf("Job execution identity was lost: %+v", v.Status)
			}
		})
	}
}

func TestVerificationSource_StorageInputMatchesSnapshot(t *testing.T) {
	SetOperatorImageDefaults("bloodraven:test", "bloodraven")
	defer SetOperatorImageDefaults("", "")
	for _, storage := range []v1alpha1.BackupStorageType{v1alpha1.BackupStorageS3, v1alpha1.BackupStoragePVC} {
		for _, encrypted := range []bool{false, true} {
			t.Run(string(storage)+map[bool]string{true: "-encrypted", false: "-plain"}[encrypted], func(t *testing.T) {
				fg := verifyFG()
				if encrypted {
					fg = fgWithEncryptedBackup()
				}
				a := successfulBackup("backup-a", fg.Name, "nightly-s3")
				a.Status.StorageType = storage
				a.Status.Encrypted = encrypted
				if storage == v1alpha1.BackupStoragePVC {
					fg.Spec.Backup.Profiles[0].Storage = v1alpha1.BackupStorage{Type: storage, PVC: &v1alpha1.PVCStorage{ClaimName: "backups"}}
					a.Status.Location = "/backups/prod_backups/db dumps/a/"
				} else {
					a.Status.Location = "s3://bloodraven-backups/prod_backups/a/"
				}
				job, err := buildVerificationJob(verificationJobInputs{FailoverGroup: fg, Profile: fg.Spec.Backup.Profiles[0], Backup: a, Verification: verifyCR("verify", fg.Name, "nightly-s3"), CredsSecretName: "creds", ScriptsConfigMapName: "scripts"})
				if err != nil {
					t.Fatal(err)
				}
				ref := verificationBackupSnapshot(a)
				if !verificationJobReadsBackup(job, ref) {
					t.Fatalf("source did not match original storage inputs: %+v", ref)
				}
				ref.Location = "wrong/destination"
				if verificationJobReadsBackup(job, ref) {
					t.Fatal("accepted a different source location")
				}
			})
		}
	}
}

func TestVerificationSource_StatusFailuresRemainRetryable(t *testing.T) {
	for _, stage := range []string{"before-launch", "after-job-create", "observe-job"} {
		t.Run(stage, func(t *testing.T) {
			ctx := context.Background()
			fg := verifyFG()
			a := successfulBackup("backup-a", fg.Name, "nightly-s3")
			v := verifyCR("pinned", fg.Name, "nightly-s3")
			r, c := newVerificationReconciler(t, fg, a, v, dsnSecret())
			if stage == "observe-job" {
				reconcileVerifUntilStable(t, r, v.Name)
				if err := c.Get(ctx, client.ObjectKeyFromObject(v), v); err != nil {
					t.Fatal(err)
				}
				v.Status.BackupRef = nil
				if err := c.Status().Update(ctx, v); err != nil {
					t.Fatal(err)
				}
			}
			r.Client = interceptor.NewClient(c.(client.WithWatch), interceptor.Funcs{
				SubResourcePatch: func(ctx context.Context, c client.Client, sub string, obj client.Object, patch client.Patch, opts ...client.SubResourcePatchOption) error {
					if verification, ok := obj.(*v1alpha1.MysqlBackupVerification); ok && sub == "status" {
						if stage != "after-job-create" || verification.Status.Phase == v1alpha1.VerificationPhaseRestoring {
							return fmt.Errorf("injected source status failure")
						}
					}
					return c.SubResource(sub).Patch(ctx, obj, patch, opts...)
				},
			})
			var failure error
			for i := 0; i < 6; i++ {
				if _, failure = r.Reconcile(ctx, ctrl.Request{NamespacedName: client.ObjectKeyFromObject(v)}); failure != nil {
					break
				}
			}
			if failure == nil || !strings.Contains(failure.Error(), "injected source status failure") {
				t.Fatalf("expected transient status error, got %v", failure)
			}
			if err := c.Get(ctx, client.ObjectKeyFromObject(v), v); err != nil {
				t.Fatal(err)
			}
			if v.Status.Phase == v1alpha1.VerificationPhaseFailed {
				t.Fatal("transient status failure terminally failed verification")
			}
			if stage == "before-launch" {
				var jobs batchv1.JobList
				if err := c.List(ctx, &jobs); err != nil || len(jobs.Items) != 0 {
					t.Fatal("Job launched before its source pin was persisted")
				}
			}
			r.Client = c
			reconcileVerifUntilStable(t, r, v.Name)
			if err := c.Get(ctx, client.ObjectKeyFromObject(v), v); err != nil {
				t.Fatal(err)
			}
			if v.Status.Phase != v1alpha1.VerificationPhaseRestoring || v.Status.BackupRef == nil || v.Status.BackupRef.UID != string(a.UID) {
				t.Fatalf("verification did not recover after status writes returned: %+v", v.Status)
			}
		})
	}
}

func TestVerificationSource_ObservesJobAfterLaunchInputsDisappear(t *testing.T) {
	ctx := context.Background()
	fg := verifyFG()
	a := successfulBackup("backup-a", fg.Name, "nightly-s3")
	v := verifyCR("pinned", fg.Name, "nightly-s3")
	v.Spec.BackupRef = &corev1.LocalObjectReference{Name: a.Name}
	r, c := newVerificationReconciler(t, fg, a, v, dsnSecret())
	reconcileVerifUntilStable(t, r, v.Name)
	if err := c.Delete(ctx, a); err != nil {
		t.Fatal(err)
	}
	if err := c.Get(ctx, client.ObjectKeyFromObject(fg), fg); err != nil {
		t.Fatal(err)
	}
	fg.Spec.Backup.Profiles = nil
	if err := c.Update(ctx, fg); err != nil {
		t.Fatal(err)
	}
	for _, secret := range []string{dsnSecret().Name, verificationCredsSecretName(v.Name)} {
		if err := c.Delete(ctx, &corev1.Secret{ObjectMeta: metav1.ObjectMeta{Name: secret, Namespace: v.Namespace}}); err != nil {
			t.Fatal(err)
		}
	}
	reconcileVerifUntilStable(t, r, v.Name)
	if err := c.Get(ctx, client.ObjectKeyFromObject(v), v); err != nil {
		t.Fatal(err)
	}
	if v.Status.Phase != v1alpha1.VerificationPhaseRestoring || v.Status.BackupRef == nil || v.Status.BackupRef.UID != string(a.UID) {
		t.Fatalf("launch-input removal interrupted an existing Job: %+v", v.Status)
	}
}
