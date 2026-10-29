package controller

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"

	v1alpha1 "github.com/shipstream/bloodraven/api/v1alpha1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/validation"
	"k8s.io/client-go/tools/record"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"
)

func failedRetryRun(name, uid string, attempt int32, created time.Time) *v1alpha1.MysqlBackup {
	completed := metav1.NewTime(created.Add(time.Minute))
	return &v1alpha1.MysqlBackup{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: "ns", UID: types.UID(uid), CreationTimestamp: metav1.NewTime(created),
			Labels: map[string]string{labelFailoverGroup: "lion", labelBackupProfile: "nightly-s3", labelBackupSchedule: "nightly"}},
		Spec:   v1alpha1.MysqlBackupSpec{FailoverGroupRef: v1alpha1.LocalGroupRef{Name: "lion"}, ProfileName: "nightly-s3"},
		Status: v1alpha1.MysqlBackupStatus{Phase: v1alpha1.BackupPhaseFailed, Attempt: attempt, CompletionTime: &completed},
	}
}

func TestBackupRetry_HistoryDoesNotSuppressNewRun(t *testing.T) {
	for _, tc := range []struct {
		name, schedule string
		phase          v1alpha1.BackupPhase
		attempt        int32
	}{
		{name: "old-success", phase: v1alpha1.BackupPhaseSucceeded, schedule: "nightly", attempt: 2},
		{name: "old-failure", phase: v1alpha1.BackupPhaseFailed, schedule: "nightly", attempt: 3},
		{name: "different-schedule", phase: v1alpha1.BackupPhaseSucceeded, schedule: "other", attempt: 3},
	} {
		t.Run(tc.name, func(t *testing.T) {
			fg := fgWithRetrySpec(3, 1)
			now := time.Now()
			failed := failedRetryRun("today", "today-uid", 1, now.Add(-time.Hour))
			old := failedRetryRun("yesterday", "yesterday-uid", tc.attempt, now.Add(-24*time.Hour))
			old.Status.Phase = tc.phase
			old.Labels[labelBackupSchedule] = tc.schedule
			br, c := newBackupReconciler(t, fg, old, failed)
			r := &MysqlFailoverGroupReconciler{Client: c, Scheme: br.Scheme, Recorder: record.NewFakeRecorder(10)}
			if _, err := r.updateBackupStatus(context.Background(), fg); err != nil {
				t.Fatal(err)
			}
			var backups v1alpha1.MysqlBackupList
			if err := c.List(context.Background(), &backups); err != nil {
				t.Fatal(err)
			}
			for _, b := range backups.Items {
				if b.Spec.TriggeredBy == "retry/nightly/attempt-2" {
					if b.Labels["shipstream.io/backup-retry-of"] != string(failed.UID) || b.Annotations["shipstream.io/backup-attempt"] != "2" {
						t.Fatalf("retry lacks durable predecessor/attempt: %+v", b.ObjectMeta)
					}
					return
				}
			}
			t.Fatalf("retained history suppressed today's retry; backups=%d", len(backups.Items))
		})
	}
}

func TestBackupRetry_CreateConvergesAfterLostStatusAndStaleList(t *testing.T) {
	fg := fgWithRetrySpec(3, 1)
	failed := failedRetryRun("today", "today-uid", 1, time.Now().Add(-time.Hour))
	br, c := newBackupReconciler(t, fg, failed)
	stale := interceptor.NewClient(c.(client.WithWatch), interceptor.Funcs{
		List: func(ctx context.Context, c client.WithWatch, list client.ObjectList, opts ...client.ListOption) error {
			if backups, ok := list.(*v1alpha1.MysqlBackupList); ok {
				backups.Items = nil
				return nil
			}
			return c.List(ctx, list, opts...)
		},
		SubResourcePatch: func(ctx context.Context, c client.Client, sub string, obj client.Object, patch client.Patch, opts ...client.SubResourcePatchOption) error {
			if _, ok := obj.(*v1alpha1.MysqlBackup); ok && sub == "status" {
				return fmt.Errorf("injected lost retry status")
			}
			return c.SubResource(sub).Patch(ctx, obj, patch, opts...)
		},
	})
	for i := 0; i < 2; i++ {
		r := &MysqlFailoverGroupReconciler{Client: stale, Scheme: br.Scheme, Recorder: record.NewFakeRecorder(10)}
		if _, _, _, err := r.maybeScheduleRetry(context.Background(), fg, fg.Spec.Backup.Schedules[0], failed); err != nil {
			t.Fatal(err)
		}
	}
	var backups v1alpha1.MysqlBackupList
	if err := c.List(context.Background(), &backups); err != nil {
		t.Fatal(err)
	}
	if len(backups.Items) != 2 {
		t.Fatalf("created duplicate successors after lost status/stale list: backups=%d", len(backups.Items))
	}
	for _, b := range backups.Items {
		if b.Name != failed.Name && b.Annotations["shipstream.io/backup-attempt"] != "2" {
			t.Fatalf("retry attempt was lost with status: %+v", b)
		}
	}
}

func TestBackupRetry_AttemptSurvivesLaunchStatusLoss(t *testing.T) {
	fg := fgWithRetrySpec(3, 1)
	fg.Status.ActiveSite = "iad"
	fg.Status.Sites = []v1alpha1.SiteStatus{{Name: "iad", State: "writable"}}
	backup := backupCR("retry-launch", fg.Name, "nightly-s3")
	backup.Annotations = map[string]string{annotationBackupAttempt: "2"}
	r, c := newBackupReconciler(t, fg, backup, dsnSecret())
	reconcileUntilStable(t, r, backup.Name)
	if err := c.Get(context.Background(), client.ObjectKeyFromObject(backup), backup); err != nil {
		t.Fatal(err)
	}
	if backup.Status.Attempt != 2 || backup.Status.Phase != v1alpha1.BackupPhaseRunning {
		t.Fatalf("retry launch reset attempt: %+v", backup.Status)
	}
}

func TestBackupRetry_PropagatesRootAndHonorsDurableLimit(t *testing.T) {
	fg := fgWithRetrySpec(3, 1)
	failed := failedRetryRun("second-attempt", "second-uid", 1, time.Now().Add(-time.Hour))
	failed.Annotations = map[string]string{annotationBackupAttempt: "2", annotationBackupRetryRoot: "original", annotationBackupRetryRootUID: "original-uid"}
	br, c := newBackupReconciler(t, fg, failed)
	r := &MysqlFailoverGroupReconciler{Client: c, Scheme: br.Scheme, Recorder: record.NewFakeRecorder(10)}
	if _, err := r.updateBackupStatus(context.Background(), fg); err != nil {
		t.Fatal(err)
	}
	if len(fg.Status.BackupSchedules) != 1 || fg.Status.BackupSchedules[0].LastRetryAttempt != 2 {
		t.Fatalf("rollup lost durable attempt: %+v", fg.Status.BackupSchedules)
	}
	var successor v1alpha1.MysqlBackup
	if err := c.Get(context.Background(), client.ObjectKey{Namespace: failed.Namespace, Name: retryBackupName(failed, 3)}, &successor); err != nil {
		t.Fatal(err)
	}
	if successor.Labels[labelBackupRetryOf] != "second-uid" || successor.Annotations[annotationBackupAttempt] != "3" || successor.Annotations[annotationBackupRetryRootUID] != "original-uid" || successor.Annotations[annotationBackupRetryRoot] != "original" {
		t.Fatalf("retry lineage changed: %+v", successor.ObjectMeta)
	}
	successor.Status.Attempt = 1
	if _, _, _, err := r.maybeScheduleRetry(context.Background(), fg, fg.Spec.Backup.Schedules[0], &successor); err != nil {
		t.Fatal(err)
	}
	var all v1alpha1.MysqlBackupList
	if err := c.List(context.Background(), &all); err != nil {
		t.Fatal(err)
	}
	if len(all.Items) != 2 {
		t.Fatalf("durable maxAttempts ignored: backups=%d", len(all.Items))
	}
}

func TestBackupRetry_ExistingNameRequiresMatchingLineage(t *testing.T) {
	for _, mode := range []string{"matching", "predecessor", "attempt", "root", "profile", "schedule"} {
		t.Run(mode, func(t *testing.T) {
			fg := fgWithRetrySpec(3, 1)
			failed := failedRetryRun("today", "today-uid", 1, time.Now().Add(-time.Hour))
			br, c := newBackupReconciler(t, fg, failed)
			r := &MysqlFailoverGroupReconciler{Client: c, Scheme: br.Scheme, Recorder: record.NewFakeRecorder(10)}
			if _, _, _, err := r.maybeScheduleRetry(context.Background(), fg, fg.Spec.Backup.Schedules[0], failed); err != nil {
				t.Fatal(err)
			}
			var existing v1alpha1.MysqlBackup
			if err := c.Get(context.Background(), client.ObjectKey{Namespace: failed.Namespace, Name: retryBackupName(failed, 2)}, &existing); err != nil {
				t.Fatal(err)
			}
			switch mode {
			case "predecessor":
				existing.Labels[labelBackupRetryOf] = "different-uid"
			case "attempt":
				existing.Annotations[annotationBackupAttempt] = "3"
			case "root":
				existing.Annotations[annotationBackupRetryRootUID] = "different-root"
			case "profile":
				existing.Spec.ProfileName = "different-profile"
			case "schedule":
				existing.Labels[labelBackupSchedule] = "different-schedule"
			}
			if err := c.Update(context.Background(), &existing); err != nil {
				t.Fatal(err)
			}
			recorder := record.NewFakeRecorder(10)
			r.Recorder = recorder
			_, wait, conflict, err := r.maybeScheduleRetry(context.Background(), fg, fg.Spec.Backup.Schedules[0], failed)
			if err != nil || wait != 0 {
				t.Fatalf("existing retry identity check: mode=%s wait=%s err=%v", mode, wait, err)
			}
			if mode != "matching" {
				if conflict != existing.Name {
					t.Fatalf("different lineage was silently ignored: conflict=%q", conflict)
				}
			} else if conflict != "" {
				t.Fatalf("matching retry reported a conflict: %q", conflict)
			}
			if len(recorder.Events) != 0 {
				t.Fatal("retry decision emitted an event before the status rollup")
			}
		})
	}
}

func TestBackupRetry_NameUsesPredecessorUID(t *testing.T) {
	a := failedRetryRun(strings.Repeat("a", 240), "uid-a", 1, time.Now())
	b := a.DeepCopy()
	b.UID = "uid-b"
	name := retryBackupName(a, 10)
	if len(name) > 63 || len(validation.IsDNS1123Label(name)) != 0 || name != retryBackupName(a, 10) || name == retryBackupName(b, 10) || name == retryBackupName(a, 9) {
		t.Fatalf("invalid, unstable or overlapping retry name %q", name)
	}
}
