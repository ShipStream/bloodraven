package controller

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"

	v1alpha1 "github.com/shipstream/bloodraven/api/v1alpha1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/tools/record"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"
)

func TestBackupRetryReview_ConflictWarningsOnlyOnTransition(t *testing.T) {
	ctx := context.Background()
	fg := fgWithRetrySpec(3, 1)
	fg.Spec.Backup.Schedules = append(fg.Spec.Backup.Schedules, v1alpha1.BackupSchedule{Name: "later", ProfileName: "nightly-s3", Schedule: "0 3 * * *"})
	first := failedRetryRun("first", "first-uid", 1, time.Now().Add(-time.Hour))
	later := failedRetryRun("later", "later-uid", 1, time.Now().Add(-time.Hour))
	later.Labels[labelBackupSchedule] = "later"
	br, c := newBackupReconciler(t, fg, first, later)
	recorder := record.NewFakeRecorder(100)
	r := &MysqlFailoverGroupReconciler{Client: c, Scheme: br.Scheme, Recorder: recorder}
	var originals []*v1alpha1.MysqlBackup
	for i, failed := range []*v1alpha1.MysqlBackup{first, later} {
		if _, _, _, err := r.maybeScheduleRetry(ctx, fg, fg.Spec.Backup.Schedules[i], failed); err != nil {
			t.Fatal(err)
		}
		var successor v1alpha1.MysqlBackup
		if err := c.Get(ctx, client.ObjectKey{Namespace: failed.Namespace, Name: retryBackupName(failed, 2)}, &successor); err != nil {
			t.Fatal(err)
		}
		originals = append(originals, successor.DeepCopy())
		successor.Labels[labelFailoverGroup] = "foreign-group"
		if err := c.Update(ctx, &successor); err != nil {
			t.Fatal(err)
		}
	}
	for len(recorder.Events) > 0 {
		<-recorder.Events
	}
	patches := 0
	observed := interceptor.NewClient(c.(client.WithWatch), interceptor.Funcs{SubResourcePatch: func(ctx context.Context, c client.Client, sub string, obj client.Object, patch client.Patch, opts ...client.SubResourcePatchOption) error {
		if _, ok := obj.(*v1alpha1.MysqlFailoverGroup); ok && sub == "status" {
			patches++
		}
		return c.SubResource(sub).Patch(ctx, obj, patch, opts...)
	}})
	reconcile := func() {
		t.Helper()
		if err := c.Get(ctx, client.ObjectKeyFromObject(fg), fg); err != nil {
			t.Fatal(err)
		}
		// A fresh reconciler has no in-memory history of prior warnings.
		r = &MysqlFailoverGroupReconciler{Client: observed, Scheme: br.Scheme, Recorder: recorder}
		if _, err := r.updateBackupStatus(ctx, fg); err != nil {
			t.Fatal(err)
		}
	}
	assertWarnings := func(want int) {
		t.Helper()
		got := 0
		for len(recorder.Events) > 0 {
			if strings.Contains(<-recorder.Events, "Warning BackupRetryConflict") {
				got++
			}
		}
		if got != want {
			t.Fatalf("conflict warnings=%d want %d", got, want)
		}
	}
	for i := 0; i < 10; i++ {
		reconcile()
	}
	assertWarnings(2)
	if patches != 1 {
		t.Fatalf("unchanged conflicts churned status: patches=%d want 1", patches)
	}
	// Resolving one conflict clears only that schedule's marker.
	var firstSuccessor v1alpha1.MysqlBackup
	if err := c.Get(ctx, client.ObjectKeyFromObject(originals[0]), &firstSuccessor); err != nil {
		t.Fatal(err)
	}
	firstSuccessor.Labels = originals[0].Labels
	if err := c.Update(ctx, &firstSuccessor); err != nil {
		t.Fatal(err)
	}
	reconcile()
	assertWarnings(0)
	firstSuccessor.Labels[labelFailoverGroup] = "foreign-again"
	if err := c.Update(ctx, &firstSuccessor); err != nil {
		t.Fatal(err)
	}
	reconcile()
	assertWarnings(1)
	// A new failed predecessor can report a distinct conflict immediately.
	newFailure := failedRetryRun("new-failure", "new-uid", 1, time.Now().Add(-30*time.Minute))
	if err := c.Create(ctx, newFailure); err != nil {
		t.Fatal(err)
	}
	if err := c.Status().Update(ctx, newFailure); err != nil {
		t.Fatal(err)
	}
	if _, _, _, err := r.maybeScheduleRetry(ctx, fg, fg.Spec.Backup.Schedules[0], newFailure); err != nil {
		t.Fatal(err)
	}
	var next v1alpha1.MysqlBackup
	if err := c.Get(ctx, client.ObjectKey{Namespace: newFailure.Namespace, Name: retryBackupName(newFailure, 2)}, &next); err != nil {
		t.Fatal(err)
	}
	next.Labels[labelFailoverGroup] = "foreign-group"
	if err := c.Update(ctx, &next); err != nil {
		t.Fatal(err)
	}
	reconcile()
	assertWarnings(1)
	reconcile()
	assertWarnings(0)
}

func TestBackupRetryReview_ConflictWarningFollowsStatusPersistence(t *testing.T) {
	ctx := context.Background()
	fg := fgWithRetrySpec(3, 1)
	failed := failedRetryRun("failed", "failed-uid", 1, time.Now().Add(-time.Hour))
	br, c := newBackupReconciler(t, fg, failed)
	r := &MysqlFailoverGroupReconciler{Client: c, Scheme: br.Scheme, Recorder: record.NewFakeRecorder(10)}
	if _, _, _, err := r.maybeScheduleRetry(ctx, fg, fg.Spec.Backup.Schedules[0], failed); err != nil {
		t.Fatal(err)
	}
	var successor v1alpha1.MysqlBackup
	if err := c.Get(ctx, client.ObjectKey{Namespace: failed.Namespace, Name: retryBackupName(failed, 2)}, &successor); err != nil {
		t.Fatal(err)
	}
	successor.Labels[labelFailoverGroup] = "foreign-group"
	if err := c.Update(ctx, &successor); err != nil {
		t.Fatal(err)
	}
	recorder := record.NewFakeRecorder(10)
	r.Recorder = recorder
	r.Client = interceptor.NewClient(c.(client.WithWatch), interceptor.Funcs{SubResourcePatch: func(ctx context.Context, c client.Client, sub string, obj client.Object, patch client.Patch, opts ...client.SubResourcePatchOption) error {
		return fmt.Errorf("injected status persistence failure")
	}})
	if _, err := r.updateBackupStatus(ctx, fg); err == nil {
		t.Fatal("status persistence failure was swallowed")
	}
	if len(recorder.Events) != 0 {
		t.Fatal("unpersisted conflict emitted a warning")
	}
	if err := c.Get(ctx, client.ObjectKeyFromObject(fg), fg); err != nil {
		t.Fatal(err)
	}
	stale := fg.DeepCopy()
	r.Client = c
	if _, err := r.updateBackupStatus(ctx, fg); err != nil {
		t.Fatal(err)
	}
	if len(recorder.Events) != 1 {
		t.Fatalf("persisted conflict did not warn once: events=%d", len(recorder.Events))
	}
	<-recorder.Events
	if _, err := r.updateBackupStatus(ctx, stale); err != nil {
		t.Fatalf("stale status was not refreshed: %v", err)
	}
	if len(recorder.Events) != 0 {
		t.Fatal("stale status replayed a conflict warning")
	}
}

func TestBackupRetryReview_LocalIssuesDoNotBlockOtherSchedules(t *testing.T) {
	for _, mode := range []string{"lineage-conflict", "cache-not-found"} {
		t.Run(mode, func(t *testing.T) {
			ctx := context.Background()
			fg := fgWithRetrySpec(3, 600)
			fg.Spec.Backup.Schedules = append(fg.Spec.Backup.Schedules, v1alpha1.BackupSchedule{Name: "later", ProfileName: "nightly-s3", Schedule: "0 3 * * *"})
			fg.Spec.Backup.Schedules = append(fg.Spec.Backup.Schedules, v1alpha1.BackupSchedule{Name: "pending", ProfileName: "nightly-s3", Schedule: "0 4 * * *"})
			now := time.Now().Truncate(time.Second)
			first := failedRetryRun("first", "first-uid", 1, now.Add(-time.Hour))
			later := failedRetryRun("later", "later-uid", 1, now.Add(-time.Hour))
			later.Labels[labelBackupSchedule] = "later"
			pending := failedRetryRun("pending", "pending-uid", 1, now.Add(-time.Hour))
			pending.Labels[labelBackupSchedule] = "pending"
			pending.Status.CompletionTime = &metav1.Time{Time: now}
			old := failedRetryRun("old-success", "old-uid", 1, now.Add(-24*time.Hour))
			old.Status.Phase = v1alpha1.BackupPhaseSucceeded
			br, c := newBackupReconciler(t, fg, first, later, pending, old)
			r := &MysqlFailoverGroupReconciler{Client: c, Scheme: br.Scheme, Recorder: record.NewFakeRecorder(10)}
			if _, _, _, err := r.maybeScheduleRetry(ctx, fg, fg.Spec.Backup.Schedules[0], first); err != nil {
				t.Fatal(err)
			}
			key := client.ObjectKey{Namespace: first.Namespace, Name: retryBackupName(first, 2)}
			var existing v1alpha1.MysqlBackup
			if err := c.Get(ctx, key, &existing); err != nil {
				t.Fatal(err)
			}
			if mode == "lineage-conflict" {
				existing.Labels[labelFailoverGroup] = "foreign-group"
				if err := c.Update(ctx, &existing); err != nil {
					t.Fatal(err)
				}
			}
			r.Client = interceptor.NewClient(c.(client.WithWatch), interceptor.Funcs{
				List: func(ctx context.Context, c client.WithWatch, list client.ObjectList, opts ...client.ListOption) error {
					if err := c.List(ctx, list, opts...); err != nil {
						return err
					}
					if backups, ok := list.(*v1alpha1.MysqlBackupList); ok {
						visible := backups.Items[:0]
						for _, b := range backups.Items {
							if b.Name != key.Name {
								visible = append(visible, b)
							}
						}
						backups.Items = visible
					}
					return nil
				},
				Get: func(ctx context.Context, c client.WithWatch, k client.ObjectKey, obj client.Object, opts ...client.GetOption) error {
					if mode == "cache-not-found" && k == key {
						return apierrors.NewNotFound(v1alpha1.GroupVersion.WithResource("mysqlbackups").GroupResource(), k.Name)
					}
					return c.Get(ctx, k, obj, opts...)
				},
			})
			recorder := record.NewFakeRecorder(10)
			r.Recorder = recorder
			wait, err := r.updateBackupStatus(ctx, fg)
			if err != nil {
				t.Fatalf("local retry issue aborted group rollup: %v", err)
			}
			if mode == "cache-not-found" && (wait <= 0 || wait > 15*time.Second) {
				t.Fatalf("cache miss must request a short wake-up: %s", wait)
			}
			if mode == "lineage-conflict" && wait <= 15*time.Second {
				t.Fatalf("conflict lost another schedule's pending backoff: %s", wait)
			}
			var got v1alpha1.MysqlFailoverGroup
			if err := c.Get(ctx, client.ObjectKeyFromObject(fg), &got); err != nil {
				t.Fatal(err)
			}
			if len(got.Status.BackupSchedules) != 3 || got.Status.LastBackupTime == nil || !got.Status.LastBackupTime.Equal(old.Status.CompletionTime) {
				t.Fatalf("schedule rollup/freshness was not persisted: %+v", got.Status)
			}
			if got.Status.BackupSchedules[2].Name != "pending" || got.Status.BackupSchedules[2].NextRetryTime == nil {
				t.Fatalf("pending schedule retry time was lost: %+v", got.Status.BackupSchedules)
			}
			var successor v1alpha1.MysqlBackup
			if err := c.Get(ctx, client.ObjectKey{Namespace: later.Namespace, Name: retryBackupName(later, 2)}, &successor); err != nil {
				t.Fatalf("later schedule lost its retry: %v", err)
			}
			var unchanged v1alpha1.MysqlBackup
			if err := c.Get(ctx, key, &unchanged); err != nil || !retryBackupMatches(&unchanged, &existing) {
				t.Fatalf("existing retry was changed or replaced: %v", err)
			}
			conflicts, scheduled := 0, 0
			for len(recorder.Events) > 0 {
				event := <-recorder.Events
				if strings.Contains(event, "Warning BackupRetryConflict") && strings.Contains(event, key.Name) {
					conflicts++
				}
				if strings.Contains(event, "Normal BackupRetryScheduled") {
					scheduled++
				}
			}
			wantConflicts := 0
			if mode == "lineage-conflict" {
				wantConflicts = 1
			}
			if conflicts != wantConflicts || scheduled != 1 {
				t.Fatalf("wrong retry events: conflicts=%d scheduled=%d", conflicts, scheduled)
			}
		})
	}
}

func TestBackupRetryReview_ReadFailuresRemainErrors(t *testing.T) {
	ctx := context.Background()
	fg := fgWithRetrySpec(3, 1)
	failed := failedRetryRun("failed", "failed-uid", 1, time.Now().Add(-time.Hour))
	br, c := newBackupReconciler(t, fg, failed)
	r := &MysqlFailoverGroupReconciler{Client: c, Scheme: br.Scheme, Recorder: record.NewFakeRecorder(10)}
	if _, _, _, err := r.maybeScheduleRetry(ctx, fg, fg.Spec.Backup.Schedules[0], failed); err != nil {
		t.Fatal(err)
	}
	r.Client = interceptor.NewClient(c.(client.WithWatch), interceptor.Funcs{Get: func(ctx context.Context, c client.WithWatch, k client.ObjectKey, obj client.Object, opts ...client.GetOption) error {
		return apierrors.NewForbidden(v1alpha1.GroupVersion.WithResource("mysqlbackups").GroupResource(), k.Name, nil)
	}})
	if _, _, _, err := r.maybeScheduleRetry(ctx, fg, fg.Spec.Backup.Schedules[0], failed); !apierrors.IsForbidden(err) {
		t.Fatalf("non-cache API failure was swallowed: %v", err)
	}
}
