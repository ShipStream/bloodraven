//go:build envtest

package envtest

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"

	v1alpha1 "github.com/shipstream/bloodraven/api/v1alpha1"
	"github.com/shipstream/bloodraven/internal/controller"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/tools/record"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"
)

func TestEnvtest_BackupRetryLineageSurvivesLostStatus(t *testing.T) {
	ns := createNamespace(t, "backup-retry-lineage")
	ensureSecret(t, ns)
	fg := newVerificationFG(ns)
	fg.Spec.Backup.Retry = &v1alpha1.BackupRetrySpec{MaxAttempts: 3, InitialBackoffSeconds: 1, MaxBackoffSeconds: 60}
	fg.Spec.Backup.Schedules = []v1alpha1.BackupSchedule{{Name: "nightly", ProfileName: "nightly-s3", Schedule: "0 2 * * *"}}
	if err := k8sClient.Create(ctx, fg); err != nil {
		t.Fatal(err)
	}
	fg.Status.ActiveSite = "dc1"
	fg.Status.Sites = []v1alpha1.SiteStatus{{Name: "dc1", State: "writable"}}
	if err := k8sClient.Status().Update(ctx, fg); err != nil {
		t.Fatal(err)
	}
	makeRun := func(name string, attempt int32, phase v1alpha1.BackupPhase, started time.Time) *v1alpha1.MysqlBackup {
		b := newSucceededBackup(ns, fg.Name, "nightly-s3")
		b.Name = name
		b.Labels["shipstream.io/backup-schedule"] = "nightly"
		if err := k8sClient.Create(ctx, b); err != nil {
			t.Fatal(err)
		}
		start, end := metav1.NewTime(started), metav1.NewTime(started.Add(time.Minute))
		b.Status = v1alpha1.MysqlBackupStatus{Phase: phase, Attempt: attempt, StartTime: &start, CompletionTime: &end}
		if err := k8sClient.Status().Update(ctx, b); err != nil {
			t.Fatal(err)
		}
		return b
	}
	makeRun("yesterday", 2, v1alpha1.BackupPhaseSucceeded, time.Now().Add(-24*time.Hour))
	failed := makeRun("today", 1, v1alpha1.BackupPhaseFailed, time.Now().Add(-time.Hour))
	watchClient, err := client.NewWithWatch(cfg, client.Options{Scheme: scheme})
	if err != nil {
		t.Fatal(err)
	}
	lagging := interceptor.NewClient(watchClient, interceptor.Funcs{
		List: func(ctx context.Context, c client.WithWatch, list client.ObjectList, opts ...client.ListOption) error {
			if err := c.List(ctx, list, opts...); err != nil {
				return err
			}
			if backups, ok := list.(*v1alpha1.MysqlBackupList); ok {
				visible := backups.Items[:0]
				for _, b := range backups.Items {
					if b.Annotations["shipstream.io/backup-attempt"] == "" {
						visible = append(visible, b)
					}
				}
				backups.Items = visible
			}
			return nil
		},
		SubResourcePatch: func(ctx context.Context, c client.Client, sub string, obj client.Object, patch client.Patch, opts ...client.SubResourcePatchOption) error {
			if _, ok := obj.(*v1alpha1.MysqlBackup); ok && sub == "status" {
				return fmt.Errorf("injected lost retry status")
			}
			return c.SubResource(sub).Patch(ctx, obj, patch, opts...)
		},
	})
	for i := 0; i < 2; i++ {
		r := &controller.MysqlFailoverGroupReconciler{Client: lagging, Scheme: scheme, Recorder: record.NewFakeRecorder(100)}
		if _, err := r.Reconcile(ctx, ctrl.Request{NamespacedName: client.ObjectKeyFromObject(fg)}); err != nil {
			t.Fatal(err)
		}
	}
	var backups v1alpha1.MysqlBackupList
	if err := k8sClient.List(ctx, &backups, client.InNamespace(ns)); err != nil {
		t.Fatal(err)
	}
	if len(backups.Items) != 3 {
		t.Fatalf("expected retained history, failed run and exactly one retry; got %d", len(backups.Items))
	}
	var retry *v1alpha1.MysqlBackup
	for i := range backups.Items {
		if strings.HasPrefix(backups.Items[i].Name, "backup-retry-") {
			retry = &backups.Items[i]
		}
	}
	if retry == nil || retry.Labels["shipstream.io/backup-retry-of"] != string(failed.UID) || retry.Annotations["shipstream.io/backup-retry-root-uid"] != string(failed.UID) || retry.Annotations["shipstream.io/backup-attempt"] != "2" || retry.Status.Attempt != 0 {
		t.Fatalf("retry launch metadata not durable after lost status: %+v", retry)
	}
	br := &controller.MysqlBackupReconciler{Client: k8sClient, Scheme: scheme, Recorder: record.NewFakeRecorder(20)}
	for i := 0; i < 5; i++ {
		if _, err := br.Reconcile(ctx, ctrl.Request{NamespacedName: client.ObjectKeyFromObject(retry)}); err != nil {
			t.Fatal(err)
		}
	}
	if err := k8sClient.Get(ctx, client.ObjectKeyFromObject(retry), retry); err != nil {
		t.Fatal(err)
	}
	if retry.Status.Attempt != 2 || retry.Status.Phase != v1alpha1.BackupPhaseRunning {
		t.Fatalf("backup reconciler lost persisted retry attempt: %+v", retry.Status)
	}
}

func TestEnvtest_BackupRetryConflictWarningPersistsAcrossRestarts(t *testing.T) {
	ns := createNamespace(t, "retry-conflict-warning")
	ensureSecret(t, ns)
	fg := newVerificationFG(ns)
	fg.Spec.Backup.Retry = &v1alpha1.BackupRetrySpec{MaxAttempts: 3, InitialBackoffSeconds: 1, MaxBackoffSeconds: 60}
	fg.Spec.Backup.Schedules = []v1alpha1.BackupSchedule{{Name: "nightly", ProfileName: "nightly-s3", Schedule: "0 2 * * *"}}
	if err := k8sClient.Create(ctx, fg); err != nil {
		t.Fatal(err)
	}
	failed := newSucceededBackup(ns, fg.Name, "nightly-s3")
	failed.Labels["shipstream.io/backup-schedule"] = "nightly"
	if err := k8sClient.Create(ctx, failed); err != nil {
		t.Fatal(err)
	}
	completed := metav1.NewTime(time.Now().Add(-time.Hour))
	failed.Status = v1alpha1.MysqlBackupStatus{Phase: v1alpha1.BackupPhaseFailed, Attempt: 1, CompletionTime: &completed}
	if err := k8sClient.Status().Update(ctx, failed); err != nil {
		t.Fatal(err)
	}
	recorder := record.NewFakeRecorder(100)
	reconcile := func() {
		t.Helper()
		r := &controller.MysqlFailoverGroupReconciler{Client: k8sClient, Scheme: scheme, Recorder: recorder}
		if _, err := r.Reconcile(ctx, ctrl.Request{NamespacedName: client.ObjectKeyFromObject(fg)}); err != nil {
			t.Fatal(err)
		}
		if err := k8sClient.Get(ctx, client.ObjectKeyFromObject(fg), fg); err != nil {
			t.Fatal(err)
		}
	}
	reconcile()
	var successors v1alpha1.MysqlBackupList
	if err := k8sClient.List(ctx, &successors, client.InNamespace(ns), client.MatchingLabels{"shipstream.io/backup-retry-of": string(failed.UID)}); err != nil || len(successors.Items) != 1 {
		t.Fatalf("expected one retry: backups=%+v err=%v", successors.Items, err)
	}
	successor := &successors.Items[0]
	setGroup := func(group string) {
		t.Helper()
		if err := k8sClient.Get(ctx, client.ObjectKeyFromObject(successor), successor); err != nil {
			t.Fatal(err)
		}
		successor.Labels["shipstream.io/failover-group"] = group
		if err := k8sClient.Update(ctx, successor); err != nil {
			t.Fatal(err)
		}
	}
	for len(recorder.Events) > 0 {
		<-recorder.Events
	}
	setGroup("foreign-group")
	for i := 0; i < 5; i++ {
		reconcile()
	}
	if len(fg.Status.BackupSchedules) != 1 || fg.Status.BackupSchedules[0].RetryConflict != successor.Name {
		t.Fatalf("API server did not retain the conflict marker: %+v", fg.Status.BackupSchedules)
	}
	assertWarnings := func(want int) {
		t.Helper()
		warnings := 0
		for len(recorder.Events) > 0 {
			if strings.Contains(<-recorder.Events, "Warning BackupRetryConflict") {
				warnings++
			}
		}
		if warnings != want {
			t.Fatalf("conflict warnings=%d want %d", warnings, want)
		}
	}
	assertWarnings(1)
	setGroup(fg.Name)
	reconcile()
	if fg.Status.BackupSchedules[0].RetryConflict != "" {
		t.Fatalf("resolved conflict marker not cleared: %+v", fg.Status.BackupSchedules)
	}
	assertWarnings(0)
	setGroup("foreign-again")
	reconcile()
	assertWarnings(1)
}
