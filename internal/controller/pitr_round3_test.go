package controller

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
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
)

// A PITR restore Job that fails for a non-PITR reason (readable logs, no PITR token).
func TestRereview3NonPITRFailureEnrichmentNeverStops(t *testing.T) {
	r, c, fg, job := round1RestoreJob(t)
	job.UID = types.UID("job-1")
	if err := c.Update(context.Background(), job); err != nil {
		t.Fatal(err)
	}
	job.Status.Conditions = []batchv1.JobCondition{{Type: batchv1.JobFailed, Status: corev1.ConditionTrue, LastTransitionTime: metav1.Now()}}
	if err := c.Status().Update(context.Background(), job); err != nil {
		t.Fatal(err)
	}
	pod := &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: "p", Namespace: "ns", Labels: map[string]string{"job-name": job.Name}}, Status: corev1.PodStatus{Phase: corev1.PodFailed}}
	if err := c.Create(context.Background(), pod); err != nil {
		t.Fatal(err)
	}
	var reads atomic.Int64
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		reads.Add(1)
		_, _ = fmt.Fprintln(w, "ERROR: util.loadDump: Duplicate entry for key PRIMARY")
	}))
	defer server.Close()
	r.Clientset, _ = kubernetes.NewForConfig(&rest.Config{Host: server.URL})
	r.Recorder = record.NewFakeRecorder(50)
	for i := 0; i < 4; i++ {
		delay, err := r.reconcileRestoreJob(context.Background(), fg)
		if err != nil {
			t.Fatal(err)
		}
		t.Logf("iter=%d delay=%v reason=%q message=%q reads=%d", i, delay, fg.Status.Restore.Reason, fg.Status.Restore.Message, reads.Load())
		old := metav1.NewTime(time.Now().Add(-time.Minute))
		fg.Status.Restore.FailureDetailLastAttemptTime = &old
	}
	// Now the pod disappears (node drain / GC).
	if err := c.Delete(context.Background(), pod); err != nil {
		t.Fatal(err)
	}
	delay, _ := r.reconcileRestoreJob(context.Background(), fg)
	if delay != 0 || fg.Status.Restore.Reason == "PITRCoverageInvalid" || !strings.Contains(fg.Status.Restore.Message, "Duplicate entry") || reads.Load() > 2 {
		t.Fatalf("generic failure was not resolved: delay=%v status=%+v reads=%d", delay, fg.Status.Restore, reads.Load())
	}
}

func TestRound3UnverifiedCompletionMetadata(t *testing.T) {
	meta, ok := parseRestoreCompleteLine("BLOODRAVEN_RESTORE_COMPLETE pitrCoverageVersion=1 pitrTargetCovered=false pitrPartial=true pitrCrossSiteUnverified=true pitrReplaySites=iad%2Cpdx")
	var status v1alpha1.RestoreStatus
	applyRestoreMetadataToStatus(&status, meta)
	if !ok || status.Reason != "PointInTimeCrossSiteUnverified" || !status.PitrPartial {
		t.Fatalf("unverified result mislabeled: %+v", status)
	}
}

func TestRound3UnverifiedRestoreOnlyRendering(t *testing.T) {
	fg := fgInitFromMysqlBackup()
	seed := succeededSeedBackup()
	fg.Spec.Backup.PITR = &v1alpha1.PITRSpec{Enabled: true, ProfileName: seed.Spec.ProfileName}
	var spec v1alpha1.PointInTimeSpec
	if err := json.Unmarshal([]byte(`{"stopDatetime":"2030-01-01T00:00:00Z","allowUnverifiedCrossSite":true}`), &spec); err != nil {
		t.Fatal(err)
	}
	fg.Spec.InitFromBackup.PointInTime = &spec
	r, _ := newReconciler(fg, seed)
	job, err := r.buildRestoreJob(context.Background(), fg, fg.Spec.Sites[0].Name, "creds")
	if err != nil {
		t.Fatal(err)
	}
	raw, _ := json.Marshal(job.Spec.Template.Spec)
	if !strings.Contains(string(raw), `"name":"BLOODRAVEN_PITR_ALLOW_UNVERIFIED_CROSS_SITE","value":"true"`) {
		t.Fatalf("opt-in not rendered: %s", raw)
	}
}

func TestRound3UnverifiedStatusAndWarnings(t *testing.T) {
	for _, partial := range []bool{false, true} {
		t.Run(fmt.Sprint(partial), func(t *testing.T) {
			r, c, fg, job := round1RestoreJob(t)
			job.Status.Conditions = []batchv1.JobCondition{{Type: batchv1.JobComplete, Status: corev1.ConditionTrue, LastTransitionTime: metav1.Now()}}
			if err := c.Status().Update(context.Background(), job); err != nil {
				t.Fatal(err)
			}
			pod := &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: "completed", Namespace: job.Namespace, Labels: map[string]string{"job-name": job.Name}}, Status: corev1.PodStatus{Phase: corev1.PodSucceeded}}
			if err := c.Create(context.Background(), pod); err != nil {
				t.Fatal(err)
			}
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
				_, _ = fmt.Fprintf(w, "BLOODRAVEN_RESTORE_COMPLETE pitrCoverageVersion=1 pitrTargetCovered=%t pitrPartial=%t pitrCrossSiteUnverified=true pitrReplaySites=iad%%2Cpdx\n", !partial, partial)
			}))
			defer server.Close()
			r.Clientset, _ = kubernetes.NewForConfig(&rest.Config{Host: server.URL})
			recorder := record.NewFakeRecorder(20)
			r.Recorder = recorder
			if _, err := r.reconcileRestoreJob(context.Background(), fg); err != nil {
				t.Fatal(err)
			}
			status := fg.Status.Restore
			if status.Phase != v1alpha1.BackupPhaseSucceeded || status.Reason != "PointInTimeCrossSiteUnverified" || !status.PitrCrossSiteUnverified || status.PitrPartial != partial || strings.Join(status.PitrReplaySites, ",") != "iad,pdx" {
				t.Fatalf("outcome lost: %+v", status)
			}
			warnings := map[string]bool{}
			for len(recorder.Events) > 0 {
				event := <-recorder.Events
				if strings.Contains(event, "RestoreSucceeded") {
					t.Fatalf("plain success emitted: %s", event)
				}
				for _, reason := range []string{"PointInTimeCrossSiteUnverified", "PointInTimePartial"} {
					if strings.Contains(event, "Warning "+reason) {
						warnings[reason] = true
					}
				}
				if strings.Contains(event, "Warning PointInTimeCrossSiteUnverified") && (!strings.Contains(event, "staged sites in replay order [iad, pdx]") || !strings.Contains(event, "cross-site completeness was not verified")) {
					t.Fatal(event)
				}
			}
			if !warnings["PointInTimeCrossSiteUnverified"] || warnings["PointInTimePartial"] != partial {
				t.Fatalf("warnings lost: %v", warnings)
			}
		})
	}
}

func TestRound3FailureEnrichmentEndsWithoutCoverageRelabel(t *testing.T) {
	for _, shape := range []string{"expired", "no pod", "termination message"} {
		t.Run(shape, func(t *testing.T) {
			r, c, fg, job := round1RestoreJob(t)
			now := metav1.Now()
			finished := now
			if shape == "expired" {
				finished = metav1.NewTime(time.Now().Add(-11 * time.Minute))
			}
			job.Status.Conditions = []batchv1.JobCondition{{Type: batchv1.JobFailed, Status: corev1.ConditionTrue, LastTransitionTime: finished}}
			if err := c.Status().Update(context.Background(), job); err != nil {
				t.Fatal(err)
			}
			fg.Status.Restore = &v1alpha1.RestoreStatus{Phase: v1alpha1.BackupPhaseFailed, JobName: job.Name, Message: "restore job failed: OOMKilled", CompletionTime: &finished}
			if err := c.Status().Update(context.Background(), fg); err != nil {
				t.Fatal(err)
			}
			if shape != "no pod" {
				pod := &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: "failed", Namespace: job.Namespace, Labels: map[string]string{"job-name": job.Name}}, Status: corev1.PodStatus{Phase: corev1.PodFailed}}
				if shape == "termination message" {
					pod.Status.ContainerStatuses = []corev1.ContainerStatus{{Name: backupJobContainerName, State: corev1.ContainerState{Terminated: &corev1.ContainerStateTerminated{ExitCode: 137, Message: "OOMKilled"}}}}
				}
				if err := c.Create(context.Background(), pod); err != nil {
					t.Fatal(err)
				}
			}
			var reads atomic.Int64
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) { reads.Add(1); http.Error(w, "unavailable", 503) }))
			defer server.Close()
			r.Clientset, _ = kubernetes.NewForConfig(&rest.Config{Host: server.URL})
			if delay, err := r.reconcileRestoreJob(context.Background(), fg); err != nil || delay != 0 {
				t.Fatalf("enrichment did not end: %v %v", delay, err)
			}
			if fg.Status.Restore.Reason != "" || !strings.Contains(fg.Status.Restore.Message, "OOMKilled") || !fg.Status.Restore.FailureDetailResolved || reads.Load() != 0 {
				t.Fatalf("failure relabeled or reread: %+v reads=%d", fg.Status.Restore, reads.Load())
			}
		})
	}
}
