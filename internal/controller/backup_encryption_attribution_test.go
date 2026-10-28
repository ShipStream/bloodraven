package controller

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"reflect"
	"strings"
	"testing"
	"time"

	v1alpha1 "github.com/shipstream/bloodraven/api/v1alpha1"
	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/record"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"
)

func TestMysqlBackupReconciler_EncryptionFollowsLaunchedJob(t *testing.T) {
	SetOperatorImageDefaults("bloodraven:test", "bloodraven")
	defer SetOperatorImageDefaults("", "")
	for _, tc := range []struct {
		name                        string
		encrypted, logs, lostStatus bool
		removeProfile               bool
	}{
		{name: "plaintext-profile-enabled"},
		{name: "plaintext-sentinel-profile-enabled", logs: true},
		{name: "encrypted-profile-disabled", encrypted: true},
		{name: "encrypted-profile-removed", encrypted: true, removeProfile: true},
		{name: "encrypted-create-before-status", encrypted: true, lostStatus: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctx := context.Background()
			fg := fgWithBackup()
			if tc.encrypted {
				fg = fgWithEncryptedBackup()
			}
			fg.Status.ActiveSite = "iad"
			fg.Status.Sites = []v1alpha1.SiteStatus{{Name: "iad", State: "writable"}}
			mb := backupCR("attribution", fg.Name, "nightly-s3")
			mb.UID = types.UID("backup-attribution")
			r, c := newBackupReconciler(t, fg, mb, dsnSecret())
			key := types.NamespacedName{Name: mb.Name, Namespace: mb.Namespace}
			if tc.lostStatus {
				r.Client = interceptor.NewClient(c.(client.WithWatch), interceptor.Funcs{
					SubResourcePatch: func(ctx context.Context, c client.Client, sub string, obj client.Object, patch client.Patch, opts ...client.SubResourcePatchOption) error {
						if _, ok := obj.(*v1alpha1.MysqlBackup); ok && sub == "status" {
							return fmt.Errorf("injected launch status failure")
						}
						return c.SubResource(sub).Patch(ctx, obj, patch, opts...)
					},
				})
				var launchErr error
				for i := 0; i < 5; i++ {
					if _, launchErr = r.Reconcile(ctx, ctrl.Request{NamespacedName: key}); launchErr != nil {
						break
					}
				}
				if launchErr == nil || !strings.Contains(launchErr.Error(), "injected launch status failure") {
					t.Fatalf("expected launch status failure, got %v", launchErr)
				}
				r.Client = c
			} else {
				reconcileUntilStable(t, r, mb.Name)
			}
			var job batchv1.Job
			if err := c.Get(ctx, types.NamespacedName{Name: backupJobName(mb.Name), Namespace: mb.Namespace}, &job); err != nil {
				t.Fatal(err)
			}
			originalSpec := job.Spec.DeepCopy()
			if err := c.Get(ctx, client.ObjectKeyFromObject(fg), fg); err != nil {
				t.Fatal(err)
			}
			if tc.removeProfile {
				fg.Spec.Backup.Profiles = nil
			} else if tc.encrypted {
				fg.Spec.Backup.Profiles[0].Encryption = nil
			} else {
				fg.Spec.Backup.Profiles[0].Encryption = fgWithEncryptedBackup().Spec.Backup.Profiles[0].Encryption
			}
			if err := c.Update(ctx, fg); err != nil {
				t.Fatal(err)
			}
			if tc.logs {
				pod := &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: "completed", Namespace: mb.Namespace, Labels: map[string]string{labelMysqlBackup: mb.Name}}, Status: corev1.PodStatus{Phase: corev1.PodSucceeded}}
				if err := c.Create(ctx, pod); err != nil {
					t.Fatal(err)
				}
				server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
					_, _ = fmt.Fprintln(w, "BLOODRAVEN_DUMP_COMPLETE location=lion/attribution sizeBytes=42")
				}))
				defer server.Close()
				var err error
				r.Clientset, err = kubernetes.NewForConfig(&rest.Config{Host: server.URL})
				if err != nil {
					t.Fatal(err)
				}
			}
			completed := metav1.NewTime(time.Date(2026, 10, 28, 12, 0, 0, 0, time.UTC))
			job.Status.Conditions = []batchv1.JobCondition{{Type: batchv1.JobComplete, Status: corev1.ConditionTrue, LastTransitionTime: completed}}
			if err := c.Status().Update(ctx, &job); err != nil {
				t.Fatal(err)
			}
			reconcileUntilStable(t, r, mb.Name)
			var got v1alpha1.MysqlBackup
			if err := c.Get(ctx, key, &got); err != nil {
				t.Fatal(err)
			}
			wantAlgorithm := ""
			if tc.encrypted {
				wantAlgorithm = "AES-256-GCM"
			}
			if got.Status.Phase != v1alpha1.BackupPhaseSucceeded || got.Status.Encrypted != tc.encrypted || got.Status.EncryptionAlgorithm != wantAlgorithm {
				t.Fatalf("phase=%s encrypted=%v algorithm=%q; want Succeeded/%v/%q", got.Status.Phase, got.Status.Encrypted, got.Status.EncryptionAlgorithm, tc.encrypted, wantAlgorithm)
			}
			if tc.logs && got.Status.Location != "lion/attribution" {
				t.Fatalf("plaintext sentinel was not collected: location=%q", got.Status.Location)
			}
			if err := c.Get(ctx, client.ObjectKeyFromObject(&job), &job); err != nil {
				t.Fatal(err)
			}
			if !reflect.DeepEqual(originalSpec, &job.Spec) {
				t.Fatal("launched Job spec changed")
			}
			if !tc.encrypted {
				// Location collection without logs is covered by a separate finding.
				got.Status.Location = "lion/attribution"
				if err := c.Status().Update(ctx, &got); err != nil {
					t.Fatal(err)
				}
				fg.Spec.InitFromBackup = &v1alpha1.InitFromBackupSpec{Source: v1alpha1.InitFromBackupSource{MysqlBackupRef: &corev1.LocalObjectReference{Name: mb.Name}}}
				rr, _ := newReconciler(fg, &got)
				restore, err := rr.buildRestoreJob(ctx, fg, "iad", "creds")
				if err != nil {
					t.Fatal(err)
				}
				for _, container := range restore.Spec.Template.Spec.InitContainers {
					if container.Name == "decrypt-download" {
						t.Fatal("plaintext backup restore attempts decryption")
					}
				}
			}
		})
	}
}

func TestMysqlBackupReconciler_RepairsTerminalEncryptionAttribution(t *testing.T) {
	ctx := context.Background()
	fg := fgWithBackup()
	mb := backupCR("misattributed", fg.Name, "nightly-s3")
	mb.UID = types.UID("misattributed-uid")
	fg.Status.ActiveSite = "iad"
	fg.Status.Sites = []v1alpha1.SiteStatus{{Name: "iad", State: "writable"}}
	r, c := newBackupReconciler(t, fg, mb, dsnSecret())
	reconcileUntilStable(t, r, mb.Name)
	if err := c.Get(ctx, client.ObjectKeyFromObject(mb), mb); err != nil {
		t.Fatal(err)
	}
	mb.Status.Phase = v1alpha1.BackupPhaseSucceeded
	mb.Status.Encrypted = true
	mb.Status.EncryptionAlgorithm = "AES-256-GCM"
	mb.Status.Location = "lion/misattributed"
	if err := c.Status().Update(ctx, mb); err != nil {
		t.Fatal(err)
	}
	original := mb.Status.DeepCopy()
	recorder := record.NewFakeRecorder(10)
	r.Recorder = recorder
	reconcileUntilStable(t, r, mb.Name)
	if err := c.Get(ctx, client.ObjectKeyFromObject(mb), mb); err != nil {
		t.Fatal(err)
	}
	original.Encrypted = false
	original.EncryptionAlgorithm = ""
	if !reflect.DeepEqual(original, &mb.Status) {
		t.Fatalf("unexpected repaired status: got %+v want %+v", mb.Status, *original)
	}
	select {
	case event := <-recorder.Events:
		if !strings.Contains(event, "Warning EncryptionAttributionCorrected") {
			t.Fatalf("unexpected event: %s", event)
		}
	default:
		t.Fatal("missing correction event")
	}
	select {
	case event := <-recorder.Events:
		t.Fatalf("duplicate or terminal event during repair: %s", event)
	default:
	}
}

func TestBackupJobEncryption(t *testing.T) {
	for _, tc := range []struct {
		name, label, container, algorithm string
		wantEncrypted                     bool
		wantAlgorithm                     string
	}{
		{name: "plaintext"},
		{name: "label-only", label: "true", wantEncrypted: true, wantAlgorithm: "AES-256-GCM"},
		{name: "uploader-without-label", container: backupEncryptUploadContainerName, wantEncrypted: true, wantAlgorithm: "AES-256-GCM"},
		{name: "pinned-algorithm", container: backupEncryptUploadContainerName, algorithm: "future-algorithm", wantEncrypted: true, wantAlgorithm: "future-algorithm"},
		{name: "unrelated-container", container: backupJobContainerName, algorithm: "AES-256-GCM"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			job := &batchv1.Job{ObjectMeta: metav1.ObjectMeta{Labels: map[string]string{labelBackupEncrypted: tc.label}}}
			job.Spec.Template.Spec.Containers = []corev1.Container{{Name: tc.container, Env: []corev1.EnvVar{{Name: "BLOODRAVEN_ENCRYPTION_ALGORITHM", Value: tc.algorithm}}}}
			encrypted, algorithm := backupJobEncryption(job)
			if encrypted != tc.wantEncrypted || algorithm != tc.wantAlgorithm {
				t.Fatalf("got %v/%q, want %v/%q", encrypted, algorithm, tc.wantEncrypted, tc.wantAlgorithm)
			}
		})
	}
}

func TestMysqlBackupReconciler_PreservesAttributionWithoutOwnedJob(t *testing.T) {
	for _, foreignJob := range []bool{false, true} {
		t.Run(fmt.Sprintf("foreign-job-%v", foreignJob), func(t *testing.T) {
			mb := backupCR("retained", "lion", "nightly-s3")
			mb.UID = types.UID("retained-uid")
			mb.Finalizers = []string{mysqlBackupFinalizer}
			mb.Status = v1alpha1.MysqlBackupStatus{Phase: v1alpha1.BackupPhaseSucceeded, Encrypted: true, EncryptionAlgorithm: "AES-256-GCM"}
			objects := []client.Object{mb}
			if foreignJob {
				controlled := true
				objects = append(objects, &batchv1.Job{ObjectMeta: metav1.ObjectMeta{
					Name: backupJobName(mb.Name), Namespace: mb.Namespace,
					OwnerReferences: []metav1.OwnerReference{{APIVersion: v1alpha1.GroupVersion.String(), Kind: "MysqlBackup", Name: mb.Name, UID: types.UID("old-backup-uid"), Controller: &controlled}},
				}})
			}
			r, c := newBackupReconciler(t, objects...)
			reconcileUntilStable(t, r, mb.Name)
			var got v1alpha1.MysqlBackup
			if err := c.Get(context.Background(), client.ObjectKeyFromObject(mb), &got); err != nil {
				t.Fatal(err)
			}
			if !reflect.DeepEqual(mb.Status, got.Status) {
				t.Fatalf("attribution changed without an owned producing Job: %+v", got.Status)
			}
		})
	}
}
