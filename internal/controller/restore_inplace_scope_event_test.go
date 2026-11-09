package controller

import (
	"context"
	"strings"
	"testing"
	"time"

	v1alpha1 "github.com/shipstream/bloodraven/api/v1alpha1"
	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/client-go/tools/record"
)

// TestInPlaceRestoreEventsNameScopeReadably pins K068: the in-place
// restore Events describe the scope as Full or Schema "x", never as a
// formatted pointer or struct.
func TestInPlaceRestoreEventsNameScopeReadably(t *testing.T) {
	ctx := context.Background()
	wantScope := func(t *testing.T, events []string, reason, scope string) {
		t.Helper()
		for _, e := range events {
			if !strings.Contains(e, reason) {
				continue
			}
			if !strings.Contains(e, "scope="+scope) {
				t.Fatalf("%s Event %q does not name scope=%s", reason, e, scope)
			}
			return
		}
		t.Fatalf("no %s Event in %q", reason, events)
	}

	t.Run("started", func(t *testing.T) {
		fg := fgInPlaceRestore("orders")
		r, _ := newReconciler(fg, succeededSeedBackup(), operatorCredsSecret())
		if _, err := r.reconcileInPlaceRestore(ctx, fg); err != nil {
			t.Fatal(err)
		}
		wantScope(t, drainEvents(r.Recorder.(*record.FakeRecorder)), v1alpha1.EventRestoreInPlaceStarted, `Schema "orders"`)
	})

	t.Run("succeeded", func(t *testing.T) {
		fg := fgInPlaceRestore("orders")
		site := fg.Spec.Sites[0].Name
		fg.Status.RestoreInPlace = &v1alpha1.RestoreInPlaceStatus{
			Phase:      v1alpha1.RestoreInPlaceRestoring,
			TargetSite: site,
			Scope:      &v1alpha1.RestoreScope{Type: v1alpha1.RestoreScopeSchema, Schema: "orders"},
			JobName:    inPlaceRestoreJobName(fg.Name, site),
			StartTime:  ptrTime(time.Now()),
		}
		done := inPlaceRestoreJobObj(fg, fg.Spec.RestoreInPlace.Confirm, false)
		done.Status.Conditions = []batchv1.JobCondition{{Type: batchv1.JobComplete, Status: corev1.ConditionTrue}}
		r, _ := newReconciler(fg, succeededSeedBackup(), operatorCredsSecret(), done)
		for i := 0; i < 2; i++ {
			if _, err := r.reconcileInPlaceRestore(ctx, fg); err != nil {
				t.Fatal(err)
			}
		}
		if fg.Status.RestoreInPlace.Phase != v1alpha1.RestoreInPlaceSucceeded {
			t.Fatalf("phase %s, want Succeeded", fg.Status.RestoreInPlace.Phase)
		}
		wantScope(t, drainEvents(r.Recorder.(*record.FakeRecorder)), v1alpha1.EventRestoreInPlaceSucceeded, `Schema "orders"`)
	})
}
