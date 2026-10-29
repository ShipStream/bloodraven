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

	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/record"
)

func TestRound4LateFirstFailureGetsOneAttempt(t *testing.T) {
	for _, source := range []string{"termination", "logs", "unavailable"} {
		t.Run(source, func(t *testing.T) {
			r, c, fg, job := round1RestoreJob(t)
			finished := metav1.NewTime(time.Now().Add(-11 * time.Minute))
			job.Status.Conditions = []batchv1.JobCondition{{Type: batchv1.JobFailed, Status: corev1.ConditionTrue, LastTransitionTime: finished}}
			if err := c.Status().Update(context.Background(), job); err != nil {
				t.Fatal(err)
			}
			pod := &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: "failed-late", Namespace: job.Namespace, Labels: map[string]string{"job-name": job.Name}}, Status: corev1.PodStatus{Phase: corev1.PodFailed}}
			detail := "PITRTargetNotCovered: late-observed archive refusal"
			if source == "termination" {
				pod.Status.InitContainerStatuses = []corev1.ContainerStatus{{Name: restorePITRInitContainerName, State: corev1.ContainerState{Terminated: &corev1.ContainerStateTerminated{ExitCode: 2, Message: detail}}}}
			}
			if err := c.Create(context.Background(), pod); err != nil {
				t.Fatal(err)
			}
			var reads atomic.Int64
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
				reads.Add(1)
				if source == "unavailable" {
					http.Error(w, "unavailable", 503)
					return
				}
				_, _ = fmt.Fprintln(w, detail)
			}))
			defer server.Close()
			r.Clientset, _ = kubernetes.NewForConfig(&rest.Config{Host: server.URL})
			rec := record.NewFakeRecorder(20)
			r.Recorder = rec
			delay, err := r.reconcileRestoreJob(context.Background(), fg)
			if err != nil {
				t.Fatal(err)
			}
			if source != "unavailable" && (delay != 0 || fg.Status.Restore.Reason != "PITRTargetNotCovered" || !strings.Contains(fg.Status.Restore.Message, detail)) {
				t.Fatalf("late first failure was not enriched: %+v delay=%s", fg.Status.Restore, delay)
			}
			if source == "logs" && reads.Load() == 0 {
				t.Fatal("late first log lookup was skipped")
			}
			if source == "unavailable" && (reads.Load() == 0 || !fg.Status.Restore.FailureDetailResolved || delay != 0) {
				t.Fatalf("late attempt missing or scheduled past expiry: %+v reads=%d delay=%s", fg.Status.Restore, reads.Load(), delay)
			}
			before := reads.Load()
			if delay, err := r.reconcileRestoreJob(context.Background(), fg); err != nil || delay != 0 {
				t.Fatalf("late failure was not cached: %s %v", delay, err)
			}
			if reads.Load() != before {
				t.Fatal("late failure reread after initial attempt")
			}
			count := 0
			for len(rec.Events) > 0 {
				if strings.Contains(<-rec.Events, "RestoreFailed") {
					count++
				}
			}
			if count != 1 {
				t.Fatalf("RestoreFailed Events=%d", count)
			}
		})
	}
}
