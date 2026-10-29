package controller

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus/testutil"
	v1alpha1 "github.com/shipstream/bloodraven/api/v1alpha1"
	"github.com/shipstream/bloodraven/internal/backupmeta"
	bmetrics "github.com/shipstream/bloodraven/internal/metrics"
	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	apiMeta "k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/record"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"
)

func backupMetadataFixture(t *testing.T, encrypted bool) (*MysqlBackupReconciler, client.Client, *v1alpha1.MysqlFailoverGroup, *v1alpha1.MysqlBackup, *batchv1.Job) {
	t.Helper()
	fg := fgWithBackup()
	if encrypted {
		fg = fgWithEncryptedBackup()
	}
	fg.Spec.Backup.Profiles[0].Storage.S3.Prefix = "prod_backups/db dumps"
	fg.Status.ActiveSite = "iad"
	fg.Status.Sites = []v1alpha1.SiteStatus{{Name: "iad", State: "writable"}}
	mb := backupCR("metadata", fg.Name, "nightly-s3")
	mb.UID = types.UID("metadata-backup-uid")
	r, c := newBackupReconciler(t, fg, mb, dsnSecret())
	reconcileUntilStable(t, r, mb.Name)
	if err := c.Get(context.Background(), client.ObjectKeyFromObject(mb), mb); err != nil {
		t.Fatal(err)
	}
	var job batchv1.Job
	if err := c.Get(context.Background(), client.ObjectKey{Name: backupJobName(mb.Name), Namespace: mb.Namespace}, &job); err != nil {
		t.Fatal(err)
	}
	complete := metav1.Now()
	job.Status.Conditions = []batchv1.JobCondition{{Type: batchv1.JobComplete, Status: corev1.ConditionTrue, LastTransitionTime: complete}}
	job.Status.CompletionTime = &complete
	if err := c.Status().Update(context.Background(), &job); err != nil {
		t.Fatal(err)
	}
	pod := &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: "metadata-pod", Namespace: mb.Namespace, Labels: map[string]string{labelMysqlBackup: mb.Name}}, Spec: job.Spec.Template.Spec, Status: corev1.PodStatus{Phase: corev1.PodSucceeded}}
	if err := c.Create(context.Background(), pod); err != nil {
		t.Fatal(err)
	}
	return r, c, fg, mb, &job
}

func backupMetadataLogs(t *testing.T, r *MysqlBackupReconciler, handler http.HandlerFunc) {
	t.Helper()
	server := httptest.NewServer(handler)
	t.Cleanup(server.Close)
	var err error
	r.Clientset, err = kubernetes.NewForConfig(&rest.Config{Host: server.URL})
	if err != nil {
		t.Fatal(err)
	}
}

func TestMysqlBackupReconciler_RecoversCompletionAfterLogFailure(t *testing.T) {
	r, c, fg, mb, job := backupMetadataFixture(t, false)
	var available atomic.Bool
	var requests atomic.Int32
	location := "prod_backups/db dumps/metadata"
	backupMetadataLogs(t, r, func(w http.ResponseWriter, req *http.Request) {
		requests.Add(1)
		if !available.Load() {
			http.Error(w, "temporary log outage", http.StatusInternalServerError)
			return
		}
		_, _ = fmt.Fprintln(w, backupmeta.FormatDumpComplete(backupmeta.DumpComplete{Location: location, SizeBytes: 42, GtidExecuted: "uuid:my_tag:1-3", BinlogFile: "mysql_bin.000001", BinlogPos: 118}))
	})
	key := client.ObjectKeyFromObject(mb)
	recorder := record.NewFakeRecorder(20)
	r.Recorder = recorder
	counter := bmetrics.BackupRunsTotal.WithLabelValues(fg.Name, mb.Spec.ProfileName, "success")
	sizeGauge := bmetrics.BackupLastSizeBytes.WithLabelValues(fg.Name, mb.Spec.ProfileName)
	sizeGauge.Set(12345)
	before := testutil.ToFloat64(counter)
	result, err := r.Reconcile(context.Background(), ctrl.Request{NamespacedName: key})
	if err != nil {
		t.Fatal(err)
	}
	if err := c.Get(context.Background(), key, mb); err != nil {
		t.Fatal(err)
	}
	if mb.Status.Location != location || result.RequeueAfter <= 0 {
		t.Fatalf("successful Job lost its destination/retry: location=%q requeue=%s", mb.Status.Location, result.RequeueAfter)
	}
	firstCompletion := mb.Status.CompletionTime.DeepCopy()
	available.Store(true)
	r = &MysqlBackupReconciler{Client: c, Scheme: r.Scheme, Recorder: recorder, Clientset: r.Clientset}
	if _, err := r.Reconcile(context.Background(), ctrl.Request{NamespacedName: key}); err != nil {
		t.Fatal(err)
	}
	if err := c.Get(context.Background(), key, mb); err != nil {
		t.Fatal(err)
	}
	condition := apiMeta.FindStatusCondition(mb.Status.Conditions, "MetadataComplete")
	if mb.Status.Phase != v1alpha1.BackupPhaseSucceeded || mb.Status.Location != location || mb.Status.GtidExecuted != "uuid:my_tag:1-3" || mb.Status.BinlogFile != "mysql_bin.000001" || mb.Status.BinlogPos != 118 || mb.Status.SizeBytes != 42 || condition == nil || condition.Status != metav1.ConditionTrue {
		t.Fatalf("completion metadata not recovered: %+v", mb.Status)
	}
	if !mb.Status.CompletionTime.Equal(firstCompletion) || !mb.Status.CompletionTime.Equal(job.Status.CompletionTime) {
		t.Fatal("metadata repair changed completion time")
	}
	if got := testutil.ToFloat64(sizeGauge); got != 42 {
		t.Fatalf("deferred metadata left the last-size gauge stale: got %v want 42", got)
	}
	requestCount := requests.Load()
	if _, err := r.Reconcile(context.Background(), ctrl.Request{NamespacedName: key}); err != nil {
		t.Fatal(err)
	}
	if requests.Load() != requestCount || testutil.ToFloat64(counter) != before+1 {
		t.Fatalf("repair repeated log collection or terminal metrics: requests=%d counter=%f", requests.Load(), testutil.ToFloat64(counter))
	}
	successEvents := 0
	for len(recorder.Events) > 0 {
		if strings.Contains(<-recorder.Events, "Normal BackupSucceeded ") {
			successEvents++
		}
	}
	if successEvents != 1 {
		t.Fatalf("BackupSucceeded events=%d, want 1", successEvents)
	}
	cleanup, err := buildCleanupJob(cleanupJobInputs{FailoverGroup: fg, Profile: &fg.Spec.Backup.Profiles[0], Backup: mb, CredsSecretName: "creds", ScriptsConfigMapName: "scripts"})
	if err != nil || envMap(cleanup.Spec.Template.Spec.Containers[0].Env)["BLOODRAVEN_OUTPUT_URL"] != location {
		t.Fatalf("cleanup did not use recovered artifact: err=%v", err)
	}
	fg.Spec.InitFromBackup = &v1alpha1.InitFromBackupSpec{Source: v1alpha1.InitFromBackupSource{MysqlBackupRef: &corev1.LocalObjectReference{Name: mb.Name}}}
	rr, _ := newReconciler(fg, mb)
	if _, err := rr.buildRestoreJob(context.Background(), fg, "iad", "creds"); err != nil {
		t.Fatalf("recovered backup cannot build a restore: %v", err)
	}
}

func TestMysqlBackupReconciler_RecoversHistoricalMissingLocation(t *testing.T) {
	r, c, _, mb, _ := backupMetadataFixture(t, false)
	mb.Status.Phase = v1alpha1.BackupPhaseSucceeded
	mb.Status.Location = ""
	if err := c.Status().Update(context.Background(), mb); err != nil {
		t.Fatal(err)
	}
	if _, err := r.Reconcile(context.Background(), ctrl.Request{NamespacedName: client.ObjectKeyFromObject(mb)}); err != nil {
		t.Fatal(err)
	}
	if err := c.Get(context.Background(), client.ObjectKeyFromObject(mb), mb); err != nil {
		t.Fatal(err)
	}
	if mb.Status.Location != "prod_backups/db dumps/metadata" {
		t.Fatalf("historical successful artifact remains unaddressable: %+v", mb.Status)
	}
}

func TestMysqlBackupReconciler_CompletionFallbackUsesJob(t *testing.T) {
	SetOperatorImageDefaults("bloodraven:test", "bloodraven")
	defer SetOperatorImageDefaults("", "")
	for _, tc := range []struct {
		name, reason       string
		encrypted, requeue bool
	}{
		{name: "nil-client", reason: "SentinelUnavailable"},
		{name: "encrypted-nil-client", encrypted: true, reason: "SentinelUnavailable"},
		{name: "missing-sentinel", reason: "SentinelMissing"},
		{name: "expired-outage", reason: "SentinelUnavailable"},
		{name: "pod-not-visible", reason: "SentinelUnavailable", requeue: true},
		{name: "pod-list-error", reason: "SentinelUnavailable", requeue: true},
		{name: "oversize-logs", reason: "SentinelUnavailable", requeue: true},
		{name: "interrupted-stream", reason: "SentinelUnavailable", requeue: true},
		{name: "wrong-location", reason: "LocationMismatch"},
		{name: "zero-observations", reason: "Collected"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			r, c, fg, mb, job := backupMetadataFixture(t, tc.encrypted)
			location := "prod_backups/db dumps/metadata"
			if tc.encrypted {
				location = "s3://bloodraven-backups/" + location + "/"
			}
			// A profile edit and lost launch status must not change the destination.
			if err := c.Get(context.Background(), client.ObjectKeyFromObject(fg), fg); err != nil {
				t.Fatal(err)
			}
			fg.Spec.Backup.Profiles[0].Storage.S3.Prefix = "wrong-prefix"
			fg.Spec.Backup.Profiles[0].Storage.S3.Bucket = "wrong-bucket"
			if err := c.Update(context.Background(), fg); err != nil {
				t.Fatal(err)
			}
			mb.Status.StorageType = ""
			if err := c.Status().Update(context.Background(), mb); err != nil {
				t.Fatal(err)
			}
			if tc.name == "expired-outage" {
				old := metav1.NewTime(time.Now().Add(-time.Hour))
				job.Status.CompletionTime = &old
				job.Status.Conditions[0].LastTransitionTime = old
				if err := c.Status().Update(context.Background(), job); err != nil {
					t.Fatal(err)
				}
			}
			var requests atomic.Int32
			if !strings.Contains(tc.name, "nil-client") {
				backupMetadataLogs(t, r, func(w http.ResponseWriter, req *http.Request) {
					requests.Add(1)
					switch tc.name {
					case "expired-outage":
						http.Error(w, "logs permanently lost", 500)
					case "oversize-logs":
						_, _ = fmt.Fprintln(w, strings.Repeat("x", 70*1024))
					case "interrupted-stream":
						w.Header().Set("Content-Length", "4096")
						_, _ = fmt.Fprintln(w, backupmeta.FormatDumpComplete(backupmeta.DumpComplete{Location: location, SizeBytes: 999}))
					case "wrong-location":
						_, _ = fmt.Fprintln(w, backupmeta.FormatDumpComplete(backupmeta.DumpComplete{Location: "wrong/location"}))
					case "zero-observations":
						_, _ = fmt.Fprintln(w, backupmeta.FormatDumpComplete(backupmeta.DumpComplete{Location: location}))
					default:
						_, _ = fmt.Fprintln(w, "completed without sentinel")
					}
				})
			}
			if tc.name == "pod-not-visible" {
				if err := c.Delete(context.Background(), &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: "metadata-pod", Namespace: mb.Namespace}}); err != nil {
					t.Fatal(err)
				}
			}
			if tc.name == "pod-list-error" {
				r.Client = interceptor.NewClient(c.(client.WithWatch), interceptor.Funcs{List: func(ctx context.Context, c client.WithWatch, list client.ObjectList, opts ...client.ListOption) error {
					if _, ok := list.(*corev1.PodList); ok {
						return fmt.Errorf("injected pod list failure")
					}
					return c.List(ctx, list, opts...)
				}})
			}
			recorder := record.NewFakeRecorder(20)
			r.Recorder = recorder
			result, err := r.Reconcile(context.Background(), ctrl.Request{NamespacedName: client.ObjectKeyFromObject(mb)})
			if err != nil {
				t.Fatal(err)
			}
			if err := c.Get(context.Background(), client.ObjectKeyFromObject(mb), mb); err != nil {
				t.Fatal(err)
			}
			condition := apiMeta.FindStatusCondition(mb.Status.Conditions, "MetadataComplete")
			ready := apiMeta.FindStatusCondition(mb.Status.Conditions, ConditionBackupReady)
			if mb.Status.Phase != v1alpha1.BackupPhaseSucceeded || mb.Status.Location != location || mb.Status.StorageType != v1alpha1.BackupStorageS3 || condition == nil || condition.Reason != tc.reason || ready == nil || ready.Status != metav1.ConditionTrue || (result.RequeueAfter > 0) != tc.requeue {
				t.Fatalf("incorrect completion fallback: result=%+v status=%+v", result, mb.Status)
			}
			if tc.name == "interrupted-stream" && mb.Status.SizeBytes != 0 {
				t.Fatal("accepted metadata from an interrupted read")
			}
			if !tc.requeue {
				before := requests.Load()
				if _, err := r.Reconcile(context.Background(), ctrl.Request{NamespacedName: client.ObjectKeyFromObject(mb)}); err != nil {
					t.Fatal(err)
				}
				if requests.Load() != before {
					t.Fatal("collection retried after a final metadata outcome")
				}
			}
			warnings := 0
			for len(recorder.Events) > 0 {
				if strings.Contains(<-recorder.Events, "BackupMetadataIncomplete") {
					warnings++
				}
			}
			wantWarnings := 1
			if tc.reason == "Collected" {
				wantWarnings = 0
			}
			if warnings != wantWarnings {
				t.Fatalf("incomplete metadata warnings=%d want %d", warnings, wantWarnings)
			}
		})
	}
}

func TestMysqlBackupReconciler_RecoversLocationBeforeFinalization(t *testing.T) {
	for _, mode := range []string{"empty", "legacy-space-corruption", "failed-job"} {
		t.Run(mode, func(t *testing.T) {
			r, c, _, mb, job := backupMetadataFixture(t, false)
			if mode == "legacy-space-corruption" {
				mb.Status.Phase = v1alpha1.BackupPhaseSucceeded
				mb.Status.Location = "prod backups/db dumps/metadata"
			}
			if mode == "failed-job" {
				job.Status.Conditions[0].Type = batchv1.JobFailed
				if err := c.Status().Update(context.Background(), job); err != nil {
					t.Fatal(err)
				}
				mb.Status.Phase = v1alpha1.BackupPhaseFailed
			}
			if err := c.Status().Update(context.Background(), mb); err != nil {
				t.Fatal(err)
			}
			done, err := r.finalize(context.Background(), mb)
			if err != nil {
				t.Fatal(err)
			}
			if mode == "failed-job" {
				if !done || mb.Status.Location != "" {
					t.Fatal("failed Job acquired a planned cleanup destination")
				}
				return
			}
			var cleanup batchv1.Job
			if err := c.Get(context.Background(), client.ObjectKey{Namespace: mb.Namespace, Name: cleanupJobName(mb.Name)}, &cleanup); err != nil {
				t.Fatal(err)
			}
			if done || envMap(cleanup.Spec.Template.Spec.Containers[0].Env)["BLOODRAVEN_OUTPUT_URL"] != "prod_backups/db dumps/metadata" {
				t.Fatalf("successful artifact released without correct cleanup: done=%v", done)
			}
		})
	}
}

func TestBackupJobDestinationUsesMainContainer(t *testing.T) {
	SetOperatorImageDefaults("bloodraven:test", "bloodraven")
	defer SetOperatorImageDefaults("", "")
	for _, storage := range []v1alpha1.BackupStorageType{v1alpha1.BackupStorageS3, v1alpha1.BackupStoragePVC} {
		for _, encrypted := range []bool{false, true} {
			t.Run(fmt.Sprintf("%s-encrypted-%v", storage, encrypted), func(t *testing.T) {
				fg := fgWithBackup()
				if encrypted {
					fg = fgWithEncryptedBackup()
				}
				profile := fg.Spec.Backup.Profiles[0]
				want := "prod_backups/db dumps/metadata"
				if storage == v1alpha1.BackupStoragePVC {
					profile.Storage = v1alpha1.BackupStorage{Type: storage, PVC: &v1alpha1.PVCStorage{ClaimName: "backups", SubPath: "prod_backups/db dumps"}}
					want = "/backups/" + want
				} else {
					profile.Storage.S3.Prefix = "prod_backups/db dumps"
					if encrypted {
						want = "s3://bloodraven-backups/" + want + "/"
					}
				}
				job, err := BuildBackupJob(BackupJobInputs{FailoverGroup: fg, Profile: profile, Backup: backupCR("metadata", fg.Name, profile.Name), SourceSite: "iad", CredsSecretName: "creds", ScriptsConfigMapName: "scripts"})
				if err != nil {
					t.Fatal(err)
				}
				gotStorage, location, err := backupJobDestination(job)
				if err != nil || gotStorage != storage || location != want {
					t.Fatalf("got %s/%q/%v, want %s/%q", gotStorage, location, err, storage, want)
				}
			})
		}
	}
}

func TestMysqlBackupReconciler_UnknownDestinationNeverBecomesReady(t *testing.T) {
	r, c, _, mb, job := backupMetadataFixture(t, false)
	job.Spec.Template.Spec.Containers[0].Env = nil
	if err := c.Update(context.Background(), job); err != nil {
		t.Fatal(err)
	}
	_, err := r.Reconcile(context.Background(), ctrl.Request{NamespacedName: client.ObjectKeyFromObject(mb)})
	if err == nil || !strings.Contains(err.Error(), "without a recoverable destination") {
		t.Fatalf("expected destination error, got %v", err)
	}
	if err := c.Get(context.Background(), client.ObjectKeyFromObject(mb), mb); err != nil {
		t.Fatal(err)
	}
	ready := apiMeta.FindStatusCondition(mb.Status.Conditions, ConditionBackupReady)
	if mb.Status.Phase == v1alpha1.BackupPhaseSucceeded || ready == nil || ready.Status != metav1.ConditionFalse {
		t.Fatalf("unknown artifact became usable: %+v", mb.Status)
	}
}
