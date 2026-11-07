package controller

import (
	"context"
	"errors"
	"testing"
	"time"

	v1alpha1 "github.com/shipstream/bloodraven/api/v1alpha1"
	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	kfake "k8s.io/client-go/kubernetes/fake"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"
)

// TestVerificationSanitySkippedNeedsProof pins that a failed run with a
// configured query and no sanity completion sentinel reports Skipped only
// when the pod proves the sanity phase was never reached. A container
// stopped mid-query (its termination message still carries verify.sh's
// start marker), a missing pod, or an unreadable log leave sanityCheck
// unset instead of claiming the query never ran.
func TestVerificationSanitySkippedNeedsProof(t *testing.T) {
	terminated := func(name string, code int32, message string) corev1.ContainerStatus {
		return corev1.ContainerStatus{Name: name, State: corev1.ContainerState{
			Terminated: &corev1.ContainerStateTerminated{ExitCode: code, Message: message},
		}}
	}
	tests := []struct {
		name       string
		query      bool
		pod        *corev1.PodStatus
		clientset  bool
		wantSkip   bool
		wantReason string
	}{
		{name: "load failed", query: true, pod: &corev1.PodStatus{ContainerStatuses: []corev1.ContainerStatus{
			terminated(backupJobContainerName, 1, "restore.py exited with rc=1: load failed")}}, wantSkip: true},
		{name: "load failed, logs read without sentinel", query: true, clientset: true, pod: &corev1.PodStatus{ContainerStatuses: []corev1.ContainerStatus{
			terminated(backupJobContainerName, 1, "restore.py exited with rc=1: load failed")}}, wantSkip: true},
		{name: "init container failed", query: true, pod: &corev1.PodStatus{
			InitContainerStatuses: []corev1.ContainerStatus{terminated("decrypt-download", 2, "decrypt failed")},
			ContainerStatuses:     []corev1.ContainerStatus{{Name: backupJobContainerName}},
		}, wantSkip: true},
		{name: "killed mid-query", query: true, pod: &corev1.PodStatus{ContainerStatuses: []corev1.ContainerStatus{
			terminated(backupJobContainerName, 137, sanityStartedMarker+" the verification ended while the sanity query ran")}}},
		{name: "killed mid-query, logs read without sentinel", query: true, clientset: true, pod: &corev1.PodStatus{ContainerStatuses: []corev1.ContainerStatus{
			terminated(backupJobContainerName, 137, sanityStartedMarker+" the verification ended while the sanity query ran")}}},
		{name: "sanity failure whose sentinel left the tail", query: true, pod: &corev1.PodStatus{ContainerStatuses: []corev1.ContainerStatus{
			terminated(backupJobContainerName, 1, "SanityCheckFailed: sanity scalar 1 < minResult 5")}}},
		{name: "main container still running", query: true, pod: &corev1.PodStatus{ContainerStatuses: []corev1.ContainerStatus{
			{Name: backupJobContainerName, State: corev1.ContainerState{Running: &corev1.ContainerStateRunning{}}}}}},
		{name: "pod gone", query: true},
		{name: "no query configured", pod: &corev1.PodStatus{ContainerStatuses: []corev1.ContainerStatus{
			terminated(backupJobContainerName, 1, "restore.py exited with rc=1: load failed")}}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			fg := verifyFG()
			backup := successfulBackup("nightly-happy", "lion", "nightly-s3")
			v := verifyCR("verify-skip", "lion", "nightly-s3")
			if tt.query {
				v.Spec.SanityCheck = &v1alpha1.SanityCheckSpec{Query: "SELECT 1"}
			}
			r, c := newVerificationReconciler(t, fg, backup, v, operatorCredsSecret())
			if tt.clientset {
				// The fake log stream returns text without a sentinel.
				r.Clientset = kfake.NewClientset()
			}
			reconcileVerifUntilStable(t, r, "verify-skip")

			ctx := context.Background()
			var job batchv1.Job
			if err := c.Get(ctx, types.NamespacedName{Name: verificationJobName("verify-skip"), Namespace: "ns"}, &job); err != nil {
				t.Fatalf("job: %v", err)
			}
			if tt.pod != nil {
				pod := &corev1.Pod{
					ObjectMeta: metav1.ObjectMeta{
						Name: job.Name + "-abcde", Namespace: "ns",
						Labels: map[string]string{
							"batch.kubernetes.io/job-name":        job.Name,
							v1alpha1.MysqlBackupVerificationLabel: v.Name,
						},
					},
					Status: *tt.pod,
				}
				pod.Status.Phase = corev1.PodFailed
				if err := c.Create(ctx, pod); err != nil {
					t.Fatalf("create pod: %v", err)
				}
				if tt.clientset {
					if _, err := r.Clientset.CoreV1().Pods("ns").Create(ctx, pod, metav1.CreateOptions{}); err != nil {
						t.Fatalf("create clientset pod: %v", err)
					}
				}
			}
			job.Status.Conditions = []batchv1.JobCondition{{Type: batchv1.JobFailed, Status: corev1.ConditionTrue, LastTransitionTime: metav1.Now(), Message: "boom"}}
			job.Status.Failed = 1
			job.Status.StartTime = &metav1.Time{Time: time.Now().Add(-time.Minute)}
			if err := c.Status().Update(ctx, &job); err != nil {
				t.Fatalf("update job: %v", err)
			}
			reconcileVerifUntilStable(t, r, "verify-skip")

			var got v1alpha1.MysqlBackupVerification
			if err := c.Get(ctx, types.NamespacedName{Name: "verify-skip", Namespace: "ns"}, &got); err != nil {
				t.Fatal(err)
			}
			if got.Status.Phase != v1alpha1.VerificationPhaseFailed {
				t.Fatalf("phase %q, want Failed", got.Status.Phase)
			}
			switch {
			case tt.wantSkip && (got.Status.SanityCheck == nil || got.Status.SanityCheck.Outcome != v1alpha1.SanityCheckOutcomeSkipped):
				t.Fatalf("sanityCheck %+v, want Skipped", got.Status.SanityCheck)
			case !tt.wantSkip && got.Status.SanityCheck != nil:
				t.Fatalf("sanityCheck %+v, want unset", got.Status.SanityCheck)
			}
		})
	}
}

// TestVerificationGraceRecordedBeforeTerminalWrite pins K010 for
// verifications: the retention grace starts when the verdict is written,
// even when the status Patch commits but its response is lost, so a retry
// cannot prune a verification observed long after its Job completed.
func TestVerificationGraceRecordedBeforeTerminalWrite(t *testing.T) {
	ctx := context.Background()
	fg := verifyFG()
	backup := successfulBackup("nightly-happy", "lion", "nightly-s3")
	v := verifyCR("verify-lost-write", "lion", "nightly-s3")
	r, c := newVerificationReconciler(t, fg, backup, v, operatorCredsSecret())
	reconcileVerifUntilStable(t, r, "verify-lost-write")

	var job batchv1.Job
	if err := c.Get(ctx, types.NamespacedName{Name: verificationJobName("verify-lost-write"), Namespace: "ns"}, &job); err != nil {
		t.Fatal(err)
	}
	longAgo := metav1.NewTime(time.Now().Add(-time.Hour))
	job.Status.Conditions = []batchv1.JobCondition{{Type: batchv1.JobFailed, Status: corev1.ConditionTrue, LastTransitionTime: longAgo, Message: "boom"}}
	job.Status.StartTime = &metav1.Time{Time: longAgo.Add(-time.Minute)}
	if err := c.Status().Update(ctx, &job); err != nil {
		t.Fatal(err)
	}
	lost := false
	r.Client = interceptor.NewClient(c.(client.WithWatch), interceptor.Funcs{
		SubResourcePatch: func(ctx context.Context, c client.Client, sub string, obj client.Object, patch client.Patch, opts ...client.SubResourcePatchOption) error {
			if err := c.SubResource(sub).Patch(ctx, obj, patch, opts...); err != nil {
				return err
			}
			if mv, ok := obj.(*v1alpha1.MysqlBackupVerification); ok && !lost && mv.Status.Phase == v1alpha1.VerificationPhaseFailed {
				lost = true
				return errors.New("injected: response lost after commit")
			}
			return nil
		},
	})
	if _, err := r.Reconcile(ctx, ctrl.Request{NamespacedName: client.ObjectKeyFromObject(v)}); err == nil {
		t.Fatal("want the injected lost-response error")
	}
	if !lost {
		t.Fatal("the terminal status Patch was never attempted")
	}
	var got v1alpha1.MysqlBackupVerification
	if err := c.Get(ctx, client.ObjectKeyFromObject(v), &got); err != nil {
		t.Fatal(err)
	}
	if got.Status.Phase != v1alpha1.VerificationPhaseFailed {
		t.Fatalf("phase %q, want the committed Failed", got.Status.Phase)
	}
	if left := r.graceClock.left(&got, got.Status.CompletionTime, time.Now()); left < terminalRetentionGrace-time.Minute {
		t.Fatalf("grace left %s, want about %s counted from the write", left, terminalRetentionGrace)
	}
}
