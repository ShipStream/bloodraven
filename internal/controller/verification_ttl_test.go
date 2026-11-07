package controller

import (
	"context"
	"testing"
	"time"

	batchv1 "k8s.io/api/batch/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"

	v1alpha1 "github.com/shipstream/bloodraven/api/v1alpha1"
)

// TestVerificationTTLNilKeepsZeroCleans (A-37, F-54): an unset
// ttlSecondsAfterFinished keeps a Succeeded verification's Job until
// retention deletes the CR, 0 deletes it at once, and a positive TTL defers
// it. A Failed run with keepOnFailure=false is cleaned up at once when the
// TTL is unset.
func TestVerificationTTLNilKeepsZeroCleans(t *testing.T) {
	no := false
	for _, tc := range []struct {
		name        string
		phase       v1alpha1.VerificationPhase
		ttl         *int32
		keep        *bool
		wantDeleted bool
		wantRequeue bool
	}{
		{name: "succeeded unset", phase: v1alpha1.VerificationPhaseSucceeded},
		{name: "succeeded zero", phase: v1alpha1.VerificationPhaseSucceeded, ttl: new(int32(0)), wantDeleted: true},
		{name: "succeeded pending ttl", phase: v1alpha1.VerificationPhaseSucceeded, ttl: new(int32(3600)), wantRequeue: true},
		{name: "failed kept", phase: v1alpha1.VerificationPhaseFailed, ttl: new(int32(0))},
		{name: "failed not kept unset", phase: v1alpha1.VerificationPhaseFailed, keep: &no, wantDeleted: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			v := verifyCR("ttl", "lion", "nightly-s3")
			v.Spec.TTLSecondsAfterFinished = tc.ttl
			v.Spec.KeepOnFailure = tc.keep
			done := metav1.NewTime(time.Now())
			v.Status.Phase = tc.phase
			v.Status.CompletionTime = &done
			job := &batchv1.Job{ObjectMeta: metav1.ObjectMeta{Name: verificationJobName(v.Name), Namespace: "ns"}}
			r, c := newVerificationReconciler(t, v, job)
			requeue, err := r.maybeCleanupAfterTerminal(context.Background(), v)
			if err != nil {
				t.Fatal(err)
			}
			err = c.Get(context.Background(), types.NamespacedName{Namespace: "ns", Name: job.Name}, &batchv1.Job{})
			if deleted := apierrors.IsNotFound(err); deleted != tc.wantDeleted {
				t.Fatalf("job deleted = %v (err %v), want %v", deleted, err, tc.wantDeleted)
			}
			if (requeue > 0) != tc.wantRequeue {
				t.Fatalf("requeue = %v, want requeue %v", requeue, tc.wantRequeue)
			}
		})
	}
}
