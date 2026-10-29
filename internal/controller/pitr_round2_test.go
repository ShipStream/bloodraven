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
	"sigs.k8s.io/controller-runtime/pkg/client"
)

func TestRound2FailureEnrichment(t *testing.T) {
	r, c, fg, job := round1RestoreJob(t)
	job.UID = types.UID("old-job")
	if err := c.Update(context.Background(), job); err != nil {
		t.Fatal(err)
	}
	job.Status.Conditions = []batchv1.JobCondition{{Type: batchv1.JobFailed, Status: corev1.ConditionTrue, LastTransitionTime: metav1.Now()}}
	if err := c.Status().Update(context.Background(), job); err != nil {
		t.Fatal(err)
	}
	pod := &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: "round2-failed", Namespace: "ns", Labels: map[string]string{"job-name": job.Name}}, Status: corev1.PodStatus{Phase: corev1.PodFailed}}
	if err := c.Create(context.Background(), pod); err != nil {
		t.Fatal(err)
	}
	var ready atomic.Bool
	var reads atomic.Int64
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		reads.Add(1)
		if !ready.Load() {
			http.Error(w, "unavailable", 503)
			return
		}
		_, _ = fmt.Fprintln(w, "PITRTargetNotCovered: source archive too short")
	}))
	defer server.Close()
	r.Clientset, _ = kubernetes.NewForConfig(&rest.Config{Host: server.URL})
	rec := record.NewFakeRecorder(20)
	r.Recorder = rec
	delay, err := r.reconcileRestoreJob(context.Background(), fg)
	if err != nil {
		t.Fatal(err)
	}
	if delay <= 0 {
		t.Fatal("unresolved failure enrichment must schedule a bounded retry")
	}
	ready.Store(true)
	// Clear the retry timestamp through JSON so this regression builds on HEAD.
	raw := []byte(`{"phase":"Failed","jobName":"` + job.Name + `","jobUID":"old-job","message":"restore job failed","completionTime":"2030-01-01T00:00:00Z","failureDetailLastAttemptTime":"2000-01-01T00:00:00Z"}`)
	if err := json.Unmarshal(raw, &fg.Status.Restore); err != nil {
		t.Fatal(err)
	}
	if _, err = r.reconcileRestoreJob(context.Background(), fg); err != nil {
		t.Fatal(err)
	}
	if fg.Status.Restore.Reason != "PITRTargetNotCovered" {
		t.Fatalf("transient detail miss cached forever: %+v", fg.Status.Restore)
	}
	before := reads.Load()
	if _, err = r.reconcileRestoreJob(context.Background(), fg); err != nil {
		t.Fatal(err)
	}
	if reads.Load() != before {
		t.Fatal("resolved detail was reread")
	}
	events := 0
	for len(rec.Events) > 0 {
		if strings.Contains(<-rec.Events, "RestoreFailed") {
			events++
		}
	}
	if events != 1 {
		t.Fatalf("failure Events=%d", events)
	}
}

func TestRound2CompletionLogsPermanentOrBounded(t *testing.T) {
	for _, shape := range []string{"no pod", "expired stream", "missing clientset"} {
		t.Run(shape, func(t *testing.T) {
			r, c, fg, job := round1RestoreJob(t)
			finished := metav1.NewTime(time.Now().Add(-11 * time.Minute))
			job.Status.Conditions = []batchv1.JobCondition{{Type: batchv1.JobComplete, Status: corev1.ConditionTrue, LastTransitionTime: finished}}
			if err := c.Status().Update(context.Background(), job); err != nil {
				t.Fatal(err)
			}
			if shape != "no pod" {
				pod := &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: "round2-completed", Namespace: "ns", Labels: map[string]string{"job-name": job.Name}}, Status: corev1.PodStatus{Phase: corev1.PodSucceeded}}
				if err := c.Create(context.Background(), pod); err != nil {
					t.Fatal(err)
				}
			}
			if shape != "missing clientset" {
				server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) { http.Error(w, "unavailable", 503) }))
				defer server.Close()
				r.Clientset, _ = kubernetes.NewForConfig(&rest.Config{Host: server.URL})
			}
			if _, err := r.reconcileRestoreJob(context.Background(), fg); err != nil {
				t.Fatal(err)
			}
			if fg.Status.Restore.Phase != v1alpha1.BackupPhaseFailed || !strings.Contains(fg.Status.Restore.Message, "completion logs unavailable") {
				t.Fatalf("permanently missing completion log waited forever: %+v", fg.Status.Restore)
			}
		})
	}
}

func TestRound2RenderMissingBound(t *testing.T) {
	fg := fgInitFromMysqlBackup()
	seed := succeededSeedBackup()
	seed.Status.GtidExecuted = ""
	fg.Spec.Backup.PITR = &v1alpha1.PITRSpec{Enabled: true, ProfileName: seed.Spec.ProfileName}
	fg.Spec.InitFromBackup.PointInTime = &v1alpha1.PointInTimeSpec{StopDatetime: "2030-01-01T00:00:00Z"}
	r, _ := newReconciler(fg, seed)
	if _, err := r.buildRestoreJob(context.Background(), fg, fg.Spec.Sites[0].Name, "creds"); err == nil || !strings.Contains(err.Error(), "status.gtidExecuted") {
		t.Fatalf("unknown strict bound rendered: %v", err)
	}
	backup := successfulBackup("seed", "lion", "nightly-s3")
	backup.Status.GtidExecuted = ""
	vfg := verifyFG()
	vfg.Spec.Backup.PITR = &v1alpha1.PITRSpec{Enabled: true, ProfileName: "nightly-s3"}
	v := verifyCR("round2-verify", "lion", "nightly-s3")
	v.Spec.PointInTime = &v1alpha1.PointInTimeVerificationSpec{Mode: "timestamp", Timestamp: "2030-01-01T00:00:00Z"}
	if _, err := buildVerificationJob(verificationJobInputs{FailoverGroup: vfg, Profile: vfg.Spec.Backup.Profiles[0], Verification: v, Backup: backup, CredsSecretName: "c", ScriptsConfigMapName: "s"}); err == nil || !strings.Contains(err.Error(), "status.gtidExecuted") {
		t.Fatalf("unknown verification bound rendered: %v", err)
	}
}

func TestRound2CachedFailureJobIdentity(t *testing.T) {
	r, c, fg, job := round1RestoreJob(t)
	old := metav1.NewTime(time.Now().Add(-time.Hour))
	fg.Status.Restore = &v1alpha1.RestoreStatus{Phase: v1alpha1.BackupPhaseFailed, JobName: job.Name, Reason: "PITRTargetNotCovered", Message: "old failure", CompletionTime: &old}
	job.CreationTimestamp = metav1.Now()
	if err := c.Update(context.Background(), job); err != nil {
		t.Fatal(err)
	}
	job.Status.Conditions = []batchv1.JobCondition{{Type: batchv1.JobFailed, Status: corev1.ConditionTrue, Message: "replacement job failed", LastTransitionTime: metav1.Now()}}
	if err := c.Status().Update(context.Background(), job); err != nil {
		t.Fatal(err)
	}
	if _, err := r.reconcileRestoreJob(context.Background(), fg); err != nil {
		t.Fatal(err)
	}
	if fg.Status.Restore.Message == "old failure" {
		t.Fatal("a replacement Job inherited the old name's cached failure")
	}
}

func TestRound2VerificationLogDeadline(t *testing.T) {
	for _, noPod := range []bool{false, true} {
		t.Run(map[bool]string{false: "clientset unavailable", true: "no job pod"}[noPod], func(t *testing.T) {
			fg := verifyFG()
			fg.Spec.Backup.PITR = &v1alpha1.PITRSpec{Enabled: true, ProfileName: "nightly-s3"}
			backup := successfulBackup("seed", "lion", "nightly-s3")
			v := verifyCR("round2-log-deadline", "lion", "nightly-s3")
			v.Spec.PointInTime = &v1alpha1.PointInTimeVerificationSpec{Mode: "timestamp", Timestamp: "2030-01-01T00:00:00Z"}
			r, c := newVerificationReconciler(t, fg, backup, v, dsnSecret())
			reconcileVerifUntilStable(t, r, v.Name)
			job := &batchv1.Job{}
			if err := c.Get(context.Background(), client.ObjectKey{Namespace: "ns", Name: verificationJobName(v.Name)}, job); err != nil {
				t.Fatal(err)
			}
			finished := metav1.NewTime(time.Now().Add(-11 * time.Minute))
			job.Status.Conditions = []batchv1.JobCondition{{Type: batchv1.JobComplete, Status: corev1.ConditionTrue, LastTransitionTime: finished}}
			if err := c.Status().Update(context.Background(), job); err != nil {
				t.Fatal(err)
			}
			if !noPod {
				pod := &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: "round2-verify-pod", Namespace: "ns", Labels: map[string]string{labelMysqlBackupVerification: v.Name, "job-name": job.Name}}, Status: corev1.PodStatus{Phase: corev1.PodSucceeded}}
				if err := c.Create(context.Background(), pod); err != nil {
					t.Fatal(err)
				}
			}
			if err := c.Get(context.Background(), client.ObjectKeyFromObject(v), v); err != nil {
				t.Fatal(err)
			}
			if _, err := r.observeVerificationJob(context.Background(), v, job); err != nil {
				t.Fatal(err)
			}
			if v.Status.Phase != v1alpha1.VerificationPhaseFailed || !strings.Contains(v.Status.Message, "completion logs unavailable") {
				t.Fatalf("verification log wait unbounded: %+v", v.Status)
			}
		})
	}
}
