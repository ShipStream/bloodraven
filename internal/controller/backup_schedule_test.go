package controller

import (
	"context"
	"testing"

	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

func TestReconcileBackupSchedules_CreatesCronJob(t *testing.T) {
	SetOperatorImageDefaults("bloodraven:test", "bloodraven")
	defer SetOperatorImageDefaults("", "")

	fg := fgWithBackup()
	r, c := newReconciler(fg)

	if err := r.reconcileBackupSchedules(context.Background(), fg); err != nil {
		t.Fatalf("reconcileBackupSchedules: %v", err)
	}

	var cj batchv1.CronJob
	if err := c.Get(context.Background(), types.NamespacedName{
		Name: scheduleCronJobName("lion", "nightly"), Namespace: "ns",
	}, &cj); err != nil {
		t.Fatalf("cronjob not created: %v", err)
	}
	if cj.Spec.Schedule != "0 2 * * *" {
		t.Errorf("want schedule 0 2 * * *, got %q", cj.Spec.Schedule)
	}
	if cj.Spec.ConcurrencyPolicy != batchv1.ForbidConcurrent {
		t.Errorf("want Forbid, got %s", cj.Spec.ConcurrencyPolicy)
	}
	containers := cj.Spec.JobTemplate.Spec.Template.Spec.Containers
	if len(containers) != 1 {
		t.Fatalf("want 1 container in trigger job, got %d", len(containers))
	}
	if containers[0].Image != "bloodraven:test" {
		t.Errorf("want operator image bloodraven:test, got %s", containers[0].Image)
	}
	cmd := containers[0].Command
	if len(cmd) == 0 || cmd[0] != "/bloodraven" || cmd[1] != "trigger-backup" {
		t.Errorf("unexpected trigger command: %v", cmd)
	}

	// Reconcile again with the same schedule to confirm idempotence.
	if err := r.reconcileBackupSchedules(context.Background(), fg); err != nil {
		t.Fatalf("second reconcile: %v", err)
	}
}

func TestReconcileBackupSchedules_PrunesOrphanCronJobs(t *testing.T) {
	SetOperatorImageDefaults("bloodraven:test", "bloodraven")
	defer SetOperatorImageDefaults("", "")

	fg := fgWithBackup()
	r, c := newReconciler(fg)

	// First create the nightly CronJob.
	if err := r.reconcileBackupSchedules(context.Background(), fg); err != nil {
		t.Fatalf("initial reconcile: %v", err)
	}

	// Now pre-create a stray CronJob with the orphan labels.
	stray := &batchv1.CronJob{
		ObjectMeta: metav1.ObjectMeta{
			Name:      scheduleCronJobName("lion", "gone"),
			Namespace: "ns",
			Labels: map[string]string{
				labelFailoverGroup: "lion",
				labelResourceKind:  "backup-schedule",
			},
		},
		Spec: batchv1.CronJobSpec{
			Schedule: "* * * * *",
			JobTemplate: batchv1.JobTemplateSpec{
				Spec: batchv1.JobSpec{
					Template: corev1.PodTemplateSpec{
						Spec: corev1.PodSpec{
							RestartPolicy: corev1.RestartPolicyOnFailure,
							Containers:    []corev1.Container{{Name: "x", Image: "busybox"}},
						},
					},
				},
			},
		},
	}
	if err := c.Create(context.Background(), stray); err != nil {
		t.Fatalf("seed stray: %v", err)
	}

	if err := r.reconcileBackupSchedules(context.Background(), fg); err != nil {
		t.Fatalf("reconcile after orphan: %v", err)
	}

	// Stray should be gone.
	var list batchv1.CronJobList
	_ = c.List(context.Background(), &list, client.InNamespace("ns"))
	for _, item := range list.Items {
		if item.Name == scheduleCronJobName("lion", "gone") {
			t.Errorf("orphan cronjob was not pruned")
		}
	}
}

func TestReconcileBackupAssets_CreatesScriptsConfigMap(t *testing.T) {
	fg := fgWithBackup()
	r, c := newReconciler(fg)

	if err := r.reconcileBackupAssets(context.Background(), fg); err != nil {
		t.Fatalf("reconcileBackupAssets: %v", err)
	}

	var cm corev1.ConfigMap
	if err := c.Get(context.Background(), types.NamespacedName{
		Name: backupScriptsConfigMapName("lion"), Namespace: "ns",
	}, &cm); err != nil {
		t.Fatalf("scripts configmap not created: %v", err)
	}
	if cm.Data["dump.py"] == "" || cm.Data["restore.py"] == "" {
		t.Errorf("scripts not embedded in configmap")
	}
}

func TestReconcileBackupAssets_CreatesOwnedPVC(t *testing.T) {
	fg := fgWithBackup()
	r, c := newReconciler(fg)

	if err := r.reconcileBackupAssets(context.Background(), fg); err != nil {
		t.Fatalf("reconcileBackupAssets: %v", err)
	}

	var pvc corev1.PersistentVolumeClaim
	if err := c.Get(context.Background(), types.NamespacedName{
		Name: ownedBackupPVCName("lion", "daily-local"), Namespace: "ns",
	}, &pvc); err != nil {
		t.Fatalf("owned backup PVC not created: %v", err)
	}
	if pvc.Spec.StorageClassName == nil || *pvc.Spec.StorageClassName != "fast" {
		t.Errorf("want storage class fast, got %v", pvc.Spec.StorageClassName)
	}
}
