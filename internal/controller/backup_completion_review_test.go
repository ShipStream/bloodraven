package controller

import (
	"context"
	"fmt"
	"net/http"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus/testutil"
	v1alpha1 "github.com/shipstream/bloodraven/api/v1alpha1"
	"github.com/shipstream/bloodraven/internal/backupmeta"
	bmetrics "github.com/shipstream/bloodraven/internal/metrics"
	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	apiMeta "k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/tools/record"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"
)

func TestBackupMetadataReview_UnrecoverableHistoricalDestination(t *testing.T) {
	for _, deleting := range []bool{false, true} {
		t.Run(fmt.Sprintf("deleting-%v", deleting), func(t *testing.T) {
			ctx := context.Background()
			r, c, _, mb, job := backupMetadataFixture(t, false)
			job.Spec.Template.Spec.Containers[0].Env = nil
			if err := c.Update(ctx, job); err != nil {
				t.Fatal(err)
			}
			mb.Status.Phase = v1alpha1.BackupPhaseSucceeded
			mb.Status.Location = ""
			if err := c.Status().Update(ctx, mb); err != nil {
				t.Fatal(err)
			}
			recorder := record.NewFakeRecorder(20)
			r.Recorder = recorder
			retentionReads := 0
			r.Client = interceptor.NewClient(c.(client.WithWatch), interceptor.Funcs{List: func(ctx context.Context, c client.WithWatch, list client.ObjectList, opts ...client.ListOption) error {
				if _, ok := list.(*v1alpha1.MysqlBackupList); ok {
					retentionReads++
				}
				return c.List(ctx, list, opts...)
			}})
			if deleting {
				if err := c.Delete(ctx, mb); err != nil {
					t.Fatal(err)
				}
			}
			for i := 0; i < 3; i++ {
				result, err := r.Reconcile(ctx, ctrl.Request{NamespacedName: client.ObjectKeyFromObject(mb)})
				if err != nil || result.RequeueAfter != 0 {
					t.Fatalf("historical metadata repair did not terminate: result=%+v err=%v", result, err)
				}
			}
			var got v1alpha1.MysqlBackup
			err := c.Get(ctx, client.ObjectKeyFromObject(mb), &got)
			if deleting {
				if !apierrors.IsNotFound(err) {
					t.Fatalf("unrecoverable artifact wedged deletion: err=%v finalizers=%v", err, got.Finalizers)
				}
			} else {
				condition := apiMeta.FindStatusCondition(got.Status.Conditions, ConditionBackupMetadataComplete)
				if err != nil || retentionReads == 0 || condition == nil || condition.Reason != "DestinationUnavailable" {
					t.Fatalf("repair did not record the outcome and run retention: reads=%d err=%v status=%+v", retentionReads, err, got.Status)
				}
			}
			incomplete, skipped := 0, 0
			for len(recorder.Events) > 0 {
				event := <-recorder.Events
				if strings.Contains(event, "BackupMetadataIncomplete") {
					incomplete++
				}
				if strings.Contains(event, "ArtifactCleanupSkipped") && strings.Contains(event, job.Name) {
					skipped++
				}
			}
			if incomplete != 1 || deleting && skipped != 1 {
				t.Fatalf("warnings missing or repeated: incomplete=%d skipped=%d", incomplete, skipped)
			}
		})
	}
}

func TestBackupMetadataReview_ReadyConditionPreservedDuringCleanup(t *testing.T) {
	for _, phase := range []v1alpha1.BackupPhase{v1alpha1.BackupPhaseSucceeded, v1alpha1.BackupPhaseRunning, v1alpha1.BackupPhaseFailed} {
		t.Run(string(phase), func(t *testing.T) {
			r, c, _, mb, _ := backupMetadataFixture(t, false)
			mb.Status.Phase = phase
			status := metav1.ConditionFalse
			if phase == v1alpha1.BackupPhaseSucceeded {
				status = metav1.ConditionTrue
			}
			original := metav1.Condition{Type: ConditionBackupReady, Status: status, Reason: "Original", Message: "original Job readiness message", LastTransitionTime: metav1.Now()}
			mb.Status.Conditions = append(mb.Status.Conditions, original)
			if err := c.Status().Update(context.Background(), mb); err != nil {
				t.Fatal(err)
			}
			if err := c.Get(context.Background(), client.ObjectKeyFromObject(mb), mb); err != nil {
				t.Fatal(err)
			}
			original = *apiMeta.FindStatusCondition(mb.Status.Conditions, ConditionBackupReady)
			if done, err := r.finalize(context.Background(), mb); err != nil || done {
				t.Fatalf("expected artifact cleanup in progress: done=%v err=%v", done, err)
			}
			if err := c.Get(context.Background(), client.ObjectKeyFromObject(mb), mb); err != nil {
				t.Fatal(err)
			}
			got := apiMeta.FindStatusCondition(mb.Status.Conditions, ConditionBackupReady)
			if !reflect.DeepEqual(got, &original) || mb.Status.Phase != phase {
				t.Fatalf("cleanup changed readiness: got %+v want %+v phase=%s", got, original, mb.Status.Phase)
			}
		})
	}
}

func TestBackupMetadataReview_OlderRecoveryDoesNotReplaceLatestSize(t *testing.T) {
	r, c, _, mb, job := backupMetadataFixture(t, false)
	mb.Status.Phase = v1alpha1.BackupPhaseSucceeded
	mb.Status.CompletionTime = job.Status.CompletionTime
	if err := c.Status().Update(context.Background(), mb); err != nil {
		t.Fatal(err)
	}
	backupMetadataLogs(t, r, func(w http.ResponseWriter, _ *http.Request) {
		_, _ = fmt.Fprintln(w, backupmeta.FormatDumpComplete(backupmeta.DumpComplete{Location: "prod_backups/db dumps/metadata", SizeBytes: 42}))
	})
	group, profile := mb.Spec.FailoverGroupRef.Name, mb.Spec.ProfileName
	latest := bmetrics.BackupLastSuccessTimestamp.WithLabelValues(group, profile)
	size := bmetrics.BackupLastSizeBytes.WithLabelValues(group, profile)
	latestTime := float64(job.Status.CompletionTime.Add(time.Hour).Unix())
	latest.Set(latestTime)
	size.Set(999)
	counter := bmetrics.BackupRunsTotal.WithLabelValues(group, profile, "success")
	before := testutil.ToFloat64(counter)
	if _, err := r.Reconcile(context.Background(), ctrl.Request{NamespacedName: client.ObjectKeyFromObject(mb)}); err != nil {
		t.Fatal(err)
	}
	if err := c.Get(context.Background(), client.ObjectKeyFromObject(mb), mb); err != nil {
		t.Fatal(err)
	}
	if mb.Status.SizeBytes != 42 || testutil.ToFloat64(size) != 999 || testutil.ToFloat64(latest) != latestTime || testutil.ToFloat64(counter) != before {
		t.Fatal("historical size recovery changed newer gauges or terminal counters")
	}
}

func TestBackupMetadataReview_RuntimeExpandedDestinations(t *testing.T) {
	SetOperatorImageDefaults("bloodraven:test", "bloodraven")
	defer SetOperatorImageDefaults("", "")
	for _, storage := range []v1alpha1.BackupStorageType{v1alpha1.BackupStorageS3, v1alpha1.BackupStoragePVC} {
		for _, encrypted := range []bool{false, true} {
			for _, logs := range []bool{false, true} {
				t.Run(fmt.Sprintf("%s/encrypted-%v/logs-%v", storage, encrypted, logs), func(t *testing.T) {
					fg := fgWithBackup()
					if encrypted {
						fg = fgWithEncryptedBackup()
					}
					const raw = "$(BLOODRAVEN_STORAGE_TYPE)/$$cash/$$(BLOODRAVEN_STORAGE_TYPE)"
					if storage == v1alpha1.BackupStoragePVC {
						fg.Spec.Backup.Profiles[0].Storage = v1alpha1.BackupStorage{Type: storage, PVC: &v1alpha1.PVCStorage{ClaimName: "backups", SubPath: raw}}
					} else {
						fg.Spec.Backup.Profiles[0].Storage.S3.Prefix = raw
					}
					fg.Status.ActiveSite = "iad"
					fg.Status.Sites = []v1alpha1.SiteStatus{{Name: "iad", State: "writable"}}
					mb := backupCR("runtime", fg.Name, "nightly-s3")
					mb.UID = "runtime-backup"
					r, c := newBackupReconciler(t, fg, mb, dsnSecret())
					reconcileUntilStable(t, r, mb.Name)
					var job batchv1.Job
					if err := c.Get(context.Background(), client.ObjectKey{Namespace: mb.Namespace, Name: backupJobName(mb.Name)}, &job); err != nil {
						t.Fatal(err)
					}
					job.Status.Conditions = []batchv1.JobCondition{{Type: batchv1.JobComplete, Status: corev1.ConditionTrue, LastTransitionTime: metav1.Now()}}
					if err := c.Status().Update(context.Background(), &job); err != nil {
						t.Fatal(err)
					}
					want := string(storage) + "/$cash/$(BLOODRAVEN_STORAGE_TYPE)/runtime"
					if storage == v1alpha1.BackupStoragePVC {
						want = "/backups/" + want
					} else if encrypted {
						want = "s3://bloodraven-backups/" + want + "/"
					}
					if logs {
						pod := &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: "runtime", Namespace: mb.Namespace, Labels: map[string]string{labelMysqlBackup: mb.Name}}, Spec: job.Spec.Template.Spec, Status: corev1.PodStatus{Phase: corev1.PodSucceeded}}
						if err := c.Create(context.Background(), pod); err != nil {
							t.Fatal(err)
						}
						backupMetadataLogs(t, r, func(w http.ResponseWriter, _ *http.Request) {
							_, _ = fmt.Fprintln(w, backupmeta.FormatDumpComplete(backupmeta.DumpComplete{Location: want}))
						})
					}
					if _, err := r.Reconcile(context.Background(), ctrl.Request{NamespacedName: client.ObjectKeyFromObject(mb)}); err != nil {
						t.Fatal(err)
					}
					if err := c.Get(context.Background(), client.ObjectKeyFromObject(mb), mb); err != nil {
						t.Fatal(err)
					}
					if mb.Status.Location != want {
						t.Fatalf("recorded raw env template: got %q want runtime %q", mb.Status.Location, want)
					}
					if logs && !apiMeta.IsStatusConditionTrue(mb.Status.Conditions, ConditionBackupMetadataComplete) {
						t.Fatalf("valid runtime sentinel was rejected: %+v", mb.Status.Conditions)
					}
				})
			}
		}
	}
}

func TestBackupMetadataReview_UnknownRuntimeDependency(t *testing.T) {
	for _, historical := range []bool{false, true} {
		for _, logs := range []bool{false, true} {
			t.Run(fmt.Sprintf("historical-%v/logs-%v", historical, logs), func(t *testing.T) {
				ctx := context.Background()
				r, c, _, mb, job := backupMetadataFixture(t, false)
				cenv := job.Spec.Template.Spec.Containers[0].Env
				for i := range cenv {
					if cenv[i].Name == "BLOODRAVEN_OUTPUT_URL" {
						cenv[i].Value = "$(RUNTIME_PREFIX)/metadata"
					}
				}
				job.Spec.Template.Spec.Containers[0].Env = append([]corev1.EnvVar{{Name: "RUNTIME_PREFIX", ValueFrom: &corev1.EnvVarSource{SecretKeyRef: &corev1.SecretKeySelector{LocalObjectReference: corev1.LocalObjectReference{Name: "not-read"}, Key: "prefix"}}}}, cenv...)
				if err := c.Update(ctx, job); err != nil {
					t.Fatal(err)
				}
				if historical {
					mb.Status.Phase = v1alpha1.BackupPhaseSucceeded
					mb.Status.Location = "$(RUNTIME_PREFIX)/metadata"
					mb.Status.Conditions = append(mb.Status.Conditions, metav1.Condition{Type: ConditionBackupMetadataComplete, Status: metav1.ConditionFalse, Reason: "SentinelUnavailable", Message: "previous Job fallback", LastTransitionTime: metav1.Now()})
					if err := c.Status().Update(ctx, mb); err != nil {
						t.Fatal(err)
					}
				}
				if logs {
					backupMetadataLogs(t, r, func(w http.ResponseWriter, _ *http.Request) {
						_, _ = fmt.Fprintln(w, backupmeta.FormatDumpComplete(backupmeta.DumpComplete{Location: "runtime-prefix/metadata"}))
					})
				}
				_, err := r.Reconcile(ctx, ctrl.Request{NamespacedName: client.ObjectKeyFromObject(mb)})
				if (err != nil) != (!historical && !logs) {
					t.Fatalf("unexpected runtime dependency outcome: %v", err)
				}
				if err := c.Get(ctx, client.ObjectKeyFromObject(mb), mb); err != nil {
					t.Fatal(err)
				}
				if logs {
					if mb.Status.Location != "runtime-prefix/metadata" || !apiMeta.IsStatusConditionTrue(mb.Status.Conditions, ConditionBackupMetadataComplete) {
						t.Fatalf("valid producer location was not retained: %+v", mb.Status)
					}
				} else if mb.Status.Location != "" || !apiMeta.IsStatusConditionFalse(mb.Status.Conditions, ConditionBackupReady) {
					t.Fatalf("unknown template was claimed as a destination: %+v", mb.Status)
				}
			})
		}
	}
}

func TestBackupMetadataReview_RepairsPersistedUnexpandedTemplate(t *testing.T) {
	ctx := context.Background()
	r, c, _, mb, job := backupMetadataFixture(t, false)
	for i := range job.Spec.Template.Spec.Containers[0].Env {
		if job.Spec.Template.Spec.Containers[0].Env[i].Name == "BLOODRAVEN_OUTPUT_URL" {
			job.Spec.Template.Spec.Containers[0].Env[i].Value = "$(BLOODRAVEN_STORAGE_TYPE)/metadata"
		}
	}
	if err := c.Update(ctx, job); err != nil {
		t.Fatal(err)
	}
	mb.Status.Phase = v1alpha1.BackupPhaseSucceeded
	mb.Status.Location = "$(BLOODRAVEN_STORAGE_TYPE)/metadata"
	mb.Status.Conditions = append(mb.Status.Conditions, metav1.Condition{Type: ConditionBackupMetadataComplete, Status: metav1.ConditionFalse, Reason: "SentinelMissing", Message: "previous Job fallback", LastTransitionTime: metav1.Now()})
	if err := c.Status().Update(ctx, mb); err != nil {
		t.Fatal(err)
	}
	if _, err := r.Reconcile(ctx, ctrl.Request{NamespacedName: client.ObjectKeyFromObject(mb)}); err != nil {
		t.Fatal(err)
	}
	if err := c.Get(ctx, client.ObjectKeyFromObject(mb), mb); err != nil {
		t.Fatal(err)
	}
	if mb.Status.Location != "S3/metadata" {
		t.Fatalf("previous raw template not repaired: %q", mb.Status.Location)
	}
}
