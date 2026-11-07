package controller

import (
	"context"
	"testing"
	"time"

	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"

	v1alpha1 "github.com/shipstream/bloodraven/api/v1alpha1"
)

// TestRetentionGraceClock pins when terminalRetentionGrace starts: the
// latest of the run's completion, the verdict this process wrote, and (once
// countFromFirstUse is set) this process's first grace evaluation.
func TestRetentionGraceClock(t *testing.T) {
	now := time.Now()
	obj := func(name, uid string) *v1alpha1.MysqlBackup {
		return &v1alpha1.MysqlBackup{ObjectMeta: metav1.ObjectMeta{Namespace: "ns", Name: name, UID: types.UID(uid)}}
	}
	at := func(d time.Duration) *metav1.Time { tm := metav1.NewTime(now.Add(d)); return &tm }
	near := func(t *testing.T, got, want time.Duration) {
		t.Helper()
		if got < want-time.Second || got > want+time.Second {
			t.Fatalf("grace left = %s, want about %s", got, want)
		}
	}

	t.Run("completion alone", func(t *testing.T) {
		var c retentionGraceClock
		near(t, c.left(obj("a", "1"), at(-30*time.Second), now), terminalRetentionGrace-30*time.Second)
		if got := c.left(obj("a", "1"), at(-time.Hour), now); got != 0 {
			t.Fatalf("an hour-old completion with no verdict record: grace left = %s, want 0", got)
		}
		if got := c.left(obj("a", "1"), nil, now); got != 0 {
			t.Fatalf("no completion time: grace left = %s, want 0", got)
		}
	})

	t.Run("a late verdict counts from its write, not the Job", func(t *testing.T) {
		var c retentionGraceClock
		c.recordVerdict(obj("a", "1"), now.Add(-10*time.Second))
		near(t, c.left(obj("a", "1"), at(-time.Hour), now), terminalRetentionGrace-10*time.Second)
		// After the grace the verdict no longer protects it.
		if got := c.left(obj("a", "1"), at(-time.Hour), now.Add(terminalRetentionGrace)); got != 0 {
			t.Fatalf("after the grace: left = %s, want 0", got)
		}
	})

	t.Run("a re-created object does not inherit the verdict", func(t *testing.T) {
		var c retentionGraceClock
		c.recordVerdict(obj("a", "1"), now)
		if got := c.left(obj("a", "2"), at(-time.Hour), now); got != 0 {
			t.Fatalf("new UID: grace left = %s, want 0", got)
		}
		if got := c.left(obj("b", "1"), at(-time.Hour), now); got != 0 {
			t.Fatalf("other name: grace left = %s, want 0", got)
		}
	})

	t.Run("records past their grace are dropped", func(t *testing.T) {
		var c retentionGraceClock
		c.recordVerdict(obj("old", "1"), now.Add(-terminalRetentionGrace))
		c.recordVerdict(obj("new", "2"), now)
		if len(c.verdicts) != 1 {
			t.Fatalf("verdicts = %v, want only the one inside its grace", c.verdicts)
		}
	})

	t.Run("first use covers a verdict a previous leader wrote", func(t *testing.T) {
		var c retentionGraceClock
		c.countFromFirstUse()
		near(t, c.left(obj("a", "1"), at(-time.Hour), now), terminalRetentionGrace)
		// The anchor is fixed at the first use.
		near(t, c.left(obj("b", "2"), at(-time.Hour), now.Add(time.Minute)), terminalRetentionGrace-time.Minute)
		if got := c.left(obj("a", "1"), at(-time.Hour), now.Add(terminalRetentionGrace)); got != 0 {
			t.Fatalf("after the grace: left = %s, want 0", got)
		}
	})
}

// R3-06: with keepFailed: 0, a backup whose Job failed long before the
// operator observed it (restart, leader change, backlog) is not deleted in
// the pass that writes its verdict: the grace counts from that write.
func TestMysqlBackupReconciler_LateFailedVerdictKeepsTerminalGrace(t *testing.T) {
	fg := fgWithBackup()
	fg.Spec.Backup.Profiles[0].Retention = v1alpha1.BackupRetention{KeepLast: 7, KeepFailed: new(int32(0))}
	fg.Status = v1alpha1.MysqlFailoverGroupStatus{
		ActiveSite: "iad",
		Sites: []v1alpha1.SiteStatus{
			{Name: "iad", State: v1alpha1.SiteStateWritable},
			{Name: "pdx", State: v1alpha1.SiteStateReadOnly, Replication: v1alpha1.SiteReplicationHealthy},
		},
	}
	mb := backupCR("late", "lion", "nightly-s3")
	r, c := newBackupReconciler(t, fg, mb, operatorCredsSecret())
	reconcileUntilStable(t, r, "late")

	var job batchv1.Job
	if err := c.Get(context.Background(), types.NamespacedName{Name: backupJobName("late"), Namespace: "ns"}, &job); err != nil {
		t.Fatalf("get job: %v", err)
	}
	failedAt := metav1.NewTime(time.Now().Add(-10 * time.Minute))
	job.Status.Conditions = []batchv1.JobCondition{{
		Type: batchv1.JobFailed, Status: corev1.ConditionTrue,
		Reason: "BackoffLimitExceeded", Message: "dump hit BackoffLimit",
		LastProbeTime: failedAt, LastTransitionTime: failedAt,
	}}
	if err := c.Status().Update(context.Background(), &job); err != nil {
		t.Fatalf("update job status: %v", err)
	}

	res, err := r.Reconcile(context.Background(), ctrl.Request{NamespacedName: types.NamespacedName{Name: "late", Namespace: "ns"}})
	if err != nil {
		t.Fatalf("terminal reconcile: %v", err)
	}
	var got v1alpha1.MysqlBackup
	if err := c.Get(context.Background(), types.NamespacedName{Name: "late", Namespace: "ns"}, &got); err != nil {
		t.Fatalf("the backup was deleted in its own terminal pass: %v", err)
	}
	if got.DeletionTimestamp != nil {
		t.Fatal("the backup was deleted in its own terminal pass")
	}
	if got.Status.Phase != v1alpha1.BackupPhaseFailed {
		t.Fatalf("phase = %s, want Failed", got.Status.Phase)
	}
	if !got.Status.CompletionTime.Time.Equal(failedAt.Rfc3339Copy().Time) {
		t.Fatalf("completionTime = %s, want the Job's %s", got.Status.CompletionTime, failedAt)
	}
	if res.RequeueAfter <= terminalRetentionGrace-10*time.Second || res.RequeueAfter > terminalRetentionGrace {
		t.Fatalf("requeueAfter = %s, want about the full %s grace", res.RequeueAfter, terminalRetentionGrace)
	}

	// The requeued pass after the grace prunes it.
	if _, err := r.pruneFailed(context.Background(), []v1alpha1.MysqlBackup{got}, 0, nil, time.Now().Add(terminalRetentionGrace)); err != nil {
		t.Fatalf("pruneFailed: %v", err)
	}
	if err := c.Get(context.Background(), types.NamespacedName{Name: "late", Namespace: "ns"}, &got); err == nil && got.DeletionTimestamp == nil {
		t.Fatal("the backup outlived its grace")
	}
}

// R3-06 for verifications: a verification whose Job failed long before the
// operator observed it keeps the grace from the verdict write.
func TestVerificationReconciler_LateVerdictKeepsTerminalGrace(t *testing.T) {
	fg := verifyFG()
	fg.Spec.Backup.Profiles[0].Verification = &v1alpha1.VerificationSpec{
		Retention: v1alpha1.VerificationRetention{KeepLast: new(int32(0)), KeepFailed: new(int32(0))},
	}
	backup := successfulBackup("nightly-happy", "lion", "nightly-s3")
	v := verifyCR("verify-late", "lion", "nightly-s3")
	r, c := newVerificationReconciler(t, fg, backup, v, operatorCredsSecret())
	reconcileVerifUntilStable(t, r, "verify-late")

	var job batchv1.Job
	if err := c.Get(context.Background(), types.NamespacedName{Name: verificationJobName("verify-late"), Namespace: "ns"}, &job); err != nil {
		t.Fatalf("get job: %v", err)
	}
	failedAt := metav1.NewTime(time.Now().Add(-10 * time.Minute))
	job.Status.Conditions = []batchv1.JobCondition{
		{Type: batchv1.JobFailed, Status: corev1.ConditionTrue, LastTransitionTime: failedAt, Message: "boom"},
	}
	job.Status.Failed = 5
	if err := c.Status().Update(context.Background(), &job); err != nil {
		t.Fatalf("update job status: %v", err)
	}

	reconcileVerifUntilStable(t, r, "verify-late")
	var got v1alpha1.MysqlBackupVerification
	if err := c.Get(context.Background(), types.NamespacedName{Name: "verify-late", Namespace: "ns"}, &got); err != nil {
		t.Fatalf("the verification was deleted in its own terminal pass: %v", err)
	}
	if got.DeletionTimestamp != nil {
		t.Fatal("the verification was deleted in its own terminal pass")
	}
	if got.Status.Phase != v1alpha1.VerificationPhaseFailed {
		t.Fatalf("phase = %s, want Failed", got.Status.Phase)
	}
	if got.Status.CompletionTime == nil || time.Since(got.Status.CompletionTime.Time) < 9*time.Minute {
		t.Fatalf("completionTime = %v, want the Job's failure time", got.Status.CompletionTime)
	}

	if _, err := r.pruneVerificationsByPhase(context.Background(), []v1alpha1.MysqlBackupVerification{got}, v1alpha1.VerificationPhaseFailed, 0, time.Now().Add(terminalRetentionGrace)); err != nil {
		t.Fatalf("pruneVerificationsByPhase: %v", err)
	}
	if err := c.Get(context.Background(), types.NamespacedName{Name: "verify-late", Namespace: "ns"}, &got); err == nil && got.DeletionTimestamp == nil {
		t.Fatal("the verification outlived its grace")
	}
}
