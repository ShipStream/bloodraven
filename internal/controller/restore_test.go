package controller

import (
	"context"
	"testing"
	"time"

	appsv1 "k8s.io/api/apps/v1"
	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"

	v1alpha1 "github.com/shipstream/bloodraven/api/v1alpha1"
)

// helper: a MysqlFailoverGroup with initFromBackup pointing at a MysqlBackup.
func fgInitFromMysqlBackup() *v1alpha1.MysqlFailoverGroup {
	fg := fgWithBackup()
	fg.Spec.InitFromBackup = &v1alpha1.InitFromBackupSpec{
		Source: v1alpha1.InitFromBackupSource{
			MysqlBackupRef: &corev1.LocalObjectReference{Name: "seed"},
		},
	}
	return fg
}

// succeededSeedBackup is a previously-completed MysqlBackup the restore
// can use as its source.
func succeededSeedBackup() *v1alpha1.MysqlBackup {
	now := metav1.NewTime(time.Now())
	return &v1alpha1.MysqlBackup{
		ObjectMeta: metav1.ObjectMeta{
			Name: "seed", Namespace: "ns",
			Labels: map[string]string{
				labelFailoverGroup: "lion",
				labelBackupProfile: "nightly-s3",
			},
		},
		Spec: v1alpha1.MysqlBackupSpec{
			FailoverGroupRef: v1alpha1.LocalGroupRef{Name: "lion"},
			ProfileName:      "nightly-s3",
		},
		Status: v1alpha1.MysqlBackupStatus{
			Phase:          v1alpha1.BackupPhaseSucceeded,
			CompletionTime: &now,
			Location:       "lion/seed/",
		},
	}
}

// primaryReadyDeployment returns a Deployment matching the first site with
// ReadyReplicas=1 so reconcileRestoreJob will proceed.
func primaryReadyDeployment(fgName, site string) *appsv1.Deployment {
	return &appsv1.Deployment{
		ObjectMeta: metav1.ObjectMeta{
			Name: resourceName(fgName, site), Namespace: "ns",
		},
		Status: appsv1.DeploymentStatus{
			ReadyReplicas: 1,
		},
	}
}

func TestReconcileRestoreJob_NoInitFromBackup_IsNoOp(t *testing.T) {
	fg := fgWithBackup()
	r, _ := newReconciler(fg)
	d, err := r.reconcileRestoreJob(context.Background(), fg)
	if err != nil {
		t.Fatalf("unexpected err: %v", err)
	}
	if d != 0 {
		t.Errorf("want no requeue, got %s", d)
	}
}

func TestReconcileRestoreJob_WaitsForPrimaryReady(t *testing.T) {
	fg := fgInitFromMysqlBackup()
	seed := succeededSeedBackup()
	// No deployment object -> not found path.
	r, _ := newReconciler(fg, seed, dsnSecret())

	d, err := r.reconcileRestoreJob(context.Background(), fg)
	if err != nil {
		t.Fatalf("unexpected err: %v", err)
	}
	if d == 0 {
		t.Errorf("expected requeue when primary not ready")
	}
}

func TestReconcileRestoreJob_CreatesRestoreJob(t *testing.T) {
	fg := fgInitFromMysqlBackup()
	seed := succeededSeedBackup()
	deploy := primaryReadyDeployment("lion", fg.Spec.Sites[0].Name)

	r, c := newReconciler(fg, seed, deploy, dsnSecret())

	if _, err := r.reconcileRestoreJob(context.Background(), fg); err != nil {
		t.Fatalf("reconcileRestoreJob: %v", err)
	}

	var job batchv1.Job
	if err := c.Get(context.Background(), types.NamespacedName{
		Name: restoreJobName("lion", fg.Spec.Sites[0].Name), Namespace: "ns",
	}, &job); err != nil {
		t.Fatalf("restore job not created: %v", err)
	}

	container := job.Spec.Template.Spec.Containers[0]
	envMap := map[string]string{}
	for _, e := range container.Env {
		envMap[e.Name] = e.Value
	}
	if envMap["BLOODRAVEN_INPUT_URL"] != "lion/seed/" {
		t.Errorf("want input url resolved from seed.status.location, got %q", envMap["BLOODRAVEN_INPUT_URL"])
	}
	if envMap["BLOODRAVEN_S3_BUCKET"] != "bloodraven-backups" {
		t.Errorf("want S3 bucket resolved from profile, got %q", envMap["BLOODRAVEN_S3_BUCKET"])
	}
	if len(container.Command) == 0 || container.Command[len(container.Command)-1] != backupScriptsMountPath+"/restore.py" {
		t.Errorf("want restore script command, got %v", container.Command)
	}

	// Status should reflect Running phase.
	if fg.Status.Restore == nil || fg.Status.Restore.Phase != v1alpha1.BackupPhaseRunning {
		t.Errorf("want Restore.Running, got %+v", fg.Status.Restore)
	}
}

func TestReconcileRestoreJob_JobSucceeded_UpdatesStatus(t *testing.T) {
	fg := fgInitFromMysqlBackup()
	seed := succeededSeedBackup()
	deploy := primaryReadyDeployment("lion", fg.Spec.Sites[0].Name)

	r, c := newReconciler(fg, seed, deploy, dsnSecret())

	if _, err := r.reconcileRestoreJob(context.Background(), fg); err != nil {
		t.Fatalf("first pass: %v", err)
	}

	// Mark the job succeeded and re-reconcile.
	var job batchv1.Job
	if err := c.Get(context.Background(), types.NamespacedName{
		Name: restoreJobName("lion", fg.Spec.Sites[0].Name), Namespace: "ns",
	}, &job); err != nil {
		t.Fatalf("get job: %v", err)
	}
	now := metav1.NewTime(time.Now())
	job.Status.Conditions = []batchv1.JobCondition{{
		Type: batchv1.JobComplete, Status: corev1.ConditionTrue,
		LastProbeTime: now, LastTransitionTime: now,
	}}
	job.Status.Succeeded = 1
	if err := c.Status().Update(context.Background(), &job); err != nil {
		t.Fatalf("update job status: %v", err)
	}

	// Re-fetch fg to mirror the state the reconciler would see.
	var fresh v1alpha1.MysqlFailoverGroup
	_ = c.Get(context.Background(), types.NamespacedName{Name: fg.Name, Namespace: fg.Namespace}, &fresh)

	if _, err := r.reconcileRestoreJob(context.Background(), &fresh); err != nil {
		t.Fatalf("second pass: %v", err)
	}

	if fresh.Status.Restore == nil || fresh.Status.Restore.Phase != v1alpha1.BackupPhaseSucceeded {
		t.Errorf("want Succeeded, got %+v", fresh.Status.Restore)
	}
}

