//go:build envtest

package envtest

import (
	"strings"
	"testing"
	"time"

	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/tools/record"
	ctrl "sigs.k8s.io/controller-runtime"

	v1alpha1 "github.com/shipstream/bloodraven/api/v1alpha1"
	"github.com/shipstream/bloodraven/internal/controller"
)

// newVerificationFG returns a MysqlFailoverGroup with an S3 backup
// profile suitable for driving a verification reconcile. The S3
// endpoint/bucket are fake — the reconciler only needs them to reach
// the Job-creation step; nothing in envtest actually hits S3.
func newVerificationFG(namespace string) *v1alpha1.MysqlFailoverGroup {
	return &v1alpha1.MysqlFailoverGroup{
		ObjectMeta: metav1.ObjectMeta{Name: "lion", Namespace: namespace},
		Spec: v1alpha1.MysqlFailoverGroupSpec{
			Image:      "mysql:9.7",
			SecretName: "mysql-credentials",
			Sites: []v1alpha1.SiteSpec{
				{Name: "dc1", Zone: "lion-dc1", LBIP: "203.0.113.1", TaintNodeSelector: map[string]string{"shipstream.io/failover-group.lion": "true", "shipstream.io/site.lion": "dc1"},
					Storage: v1alpha1.StorageSpec{StorageClassName: "standard", Size: resource.MustParse("10Gi")}},
				{Name: "dc2", Zone: "lion-dc2", LBIP: "203.0.113.2", TaintNodeSelector: map[string]string{"shipstream.io/failover-group.lion": "true", "shipstream.io/site.lion": "dc2"},
					Storage: v1alpha1.StorageSpec{StorageClassName: "standard", Size: resource.MustParse("10Gi")}},
			},
			DNS: v1alpha1.DNSSpec{Hostname: "lion.az.example.com", TTL: 60},
			Backup: &v1alpha1.BackupSpec{
				Image: "container-registry.oracle.com/mysql/community-server:9.7",
				Profiles: []v1alpha1.BackupProfile{
					{
						Name: "nightly-s3",
						Storage: v1alpha1.BackupStorage{
							Type: v1alpha1.BackupStorageS3,
							S3: &v1alpha1.S3Storage{
								Bucket:            "bloodraven",
								Prefix:            "lion",
								CredentialsSecret: "s3-creds",
							},
						},
					},
				},
			},
		},
	}
}

// newSucceededBackup returns a MysqlBackup already in the Succeeded
// phase with the metadata the verification reconciler needs to resolve
// a target and build a Job (status.location + storageType).
func newSucceededBackup(namespace, fgName, profile string) *v1alpha1.MysqlBackup {
	return &v1alpha1.MysqlBackup{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "lion-nightly-ok",
			Namespace: namespace,
			Labels: map[string]string{
				"shipstream.io/failover-group": fgName,
				"shipstream.io/backup-profile": profile,
				"app.kubernetes.io/managed-by": "bloodraven",
			},
		},
		Spec: v1alpha1.MysqlBackupSpec{
			FailoverGroupRef: v1alpha1.LocalGroupRef{Name: fgName},
			ProfileName:      profile,
		},
	}
}

// reconcileUntil runs Reconcile in a loop until pred returns true or a
// safety cap of 20 iterations is hit. Each pass is followed by a fresh
// Get so tests can assert against the latest API-server state. Pattern
// intentionally mirrors TestEnvtest_ReconcilerIdempotent above: envtest
// doesn't run the controller manager, so the test drives reconcile
// ticks manually.
func reconcileUntil(t *testing.T, r *controller.MysqlBackupVerificationReconciler, nn types.NamespacedName, pred func() bool) {
	t.Helper()
	for i := 0; i < 20; i++ {
		if _, err := r.Reconcile(ctx, ctrl.Request{NamespacedName: nn}); err != nil {
			t.Fatalf("reconcile iter %d: %v", i, err)
		}
		if pred() {
			return
		}
	}
	t.Fatalf("reconcileUntil: predicate never satisfied after 20 iterations (nn=%s)", nn)
}

func TestEnvtest_Verification_CreationAndSchemaAcceptance(t *testing.T) {
	ns := "envtest-verify-schema"
	ensureNamespace(t, ns)

	v := &v1alpha1.MysqlBackupVerification{
		ObjectMeta: metav1.ObjectMeta{Name: "verify-a", Namespace: ns},
		Spec: v1alpha1.MysqlBackupVerificationSpec{
			FailoverGroupRef: v1alpha1.LocalGroupRef{Name: "lion"},
			ProfileName:      "nightly-s3",
			PointInTime: &v1alpha1.PointInTimeVerificationSpec{
				Mode: "timestamp",
				// RFC3339 with an explicit offset — exercises the
				// CRD's string validation without any runtime parsing.
				Timestamp: "2026-04-15T09:30:00+00:00",
			},
			SanityCheck: &v1alpha1.SanityCheckSpec{
				Query: "SELECT COUNT(*) FROM information_schema.tables",
				Expect: &v1alpha1.SanityCheckExpectation{
					MinRows:            1,
					MaxDurationSeconds: 30,
				},
			},
		},
	}
	if err := k8sClient.Create(ctx, v); err != nil {
		t.Fatalf("create MysqlBackupVerification: %v", err)
	}

	var fetched v1alpha1.MysqlBackupVerification
	if err := k8sClient.Get(ctx, types.NamespacedName{Name: "verify-a", Namespace: ns}, &fetched); err != nil {
		t.Fatalf("get: %v", err)
	}
	if fetched.Spec.PointInTime == nil || fetched.Spec.PointInTime.Mode != "timestamp" {
		t.Errorf("pointInTime.mode not persisted: %+v", fetched.Spec.PointInTime)
	}
	if fetched.Spec.SanityCheck == nil || fetched.Spec.SanityCheck.Query == "" {
		t.Errorf("sanityCheck.query not persisted: %+v", fetched.Spec.SanityCheck)
	}
}

func TestEnvtest_Verification_ReconcilerProvisionsEphemeralResources(t *testing.T) {
	ns := "envtest-verify-provision"
	ensureNamespace(t, ns)
	ensureSecret(t, ns)

	fg := newVerificationFG(ns)
	if err := k8sClient.Create(ctx, fg); err != nil {
		t.Fatalf("create fg: %v", err)
	}

	backup := newSucceededBackup(ns, fg.Name, "nightly-s3")
	if err := k8sClient.Create(ctx, backup); err != nil {
		t.Fatalf("create backup: %v", err)
	}
	// Status is a subresource — write it separately.
	backup.Status = v1alpha1.MysqlBackupStatus{
		Phase:          v1alpha1.BackupPhaseSucceeded,
		CompletionTime: &metav1.Time{Time: time.Now().Add(-1 * time.Hour)},
		Location:       "s3://bloodraven/lion/lion-nightly-ok/",
		StorageType:    v1alpha1.BackupStorageS3,
		SizeBytes:      512 * 1024 * 1024,
	}
	if err := k8sClient.Status().Update(ctx, backup); err != nil {
		t.Fatalf("update backup status: %v", err)
	}

	// The verification reconciler expects a backup-scripts ConfigMap to
	// exist for the group (normally rendered by the fg reconciler). A
	// stub with the expected name is enough — the verification path
	// only references it by name when building the Job.
	scriptsCM := &corev1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "mysql-lion-backup-scripts",
			Namespace: ns,
		},
		Data: map[string]string{"verify.sh": "#!/bin/bash\ntrue"},
	}
	if err := k8sClient.Create(ctx, scriptsCM); err != nil {
		t.Fatalf("create scripts configmap: %v", err)
	}

	verify := &v1alpha1.MysqlBackupVerification{
		ObjectMeta: metav1.ObjectMeta{Name: "verify-provision", Namespace: ns},
		Spec: v1alpha1.MysqlBackupVerificationSpec{
			FailoverGroupRef: v1alpha1.LocalGroupRef{Name: fg.Name},
			ProfileName:      "nightly-s3",
		},
	}
	if err := k8sClient.Create(ctx, verify); err != nil {
		t.Fatalf("create verify: %v", err)
	}

	r := &controller.MysqlBackupVerificationReconciler{
		Client:   k8sClient,
		Scheme:   scheme,
		Recorder: record.NewFakeRecorder(10),
	}
	nn := types.NamespacedName{Name: verify.Name, Namespace: ns}

	// Reconcile until the Job exists — at that point the creds Secret must
	// have been created and the phase must be at least Restoring. The
	// ephemeral datadir is an emptyDir baked into the Job (no PVC is
	// provisioned anymore), so no verify PVC object should exist.
	jobKey := types.NamespacedName{Name: "mysqlverify-verify-provision", Namespace: ns}
	reconcileUntil(t, r, nn, func() bool {
		var j batchv1.Job
		return k8sClient.Get(ctx, jobKey, &j) == nil
	})

	var pvc corev1.PersistentVolumeClaim
	pvcKey := types.NamespacedName{Name: "mysqlverify-verify-provision-data", Namespace: ns}
	if err := k8sClient.Get(ctx, pvcKey, &pvc); err == nil {
		t.Errorf("verify PVC must NOT be provisioned (datadir is an emptyDir), but %s exists", pvc.Name)
	}

	var secret corev1.Secret
	secretKey := types.NamespacedName{Name: "mysqlverify-verify-provision-creds", Namespace: ns}
	if err := k8sClient.Get(ctx, secretKey, &secret); err != nil {
		t.Fatalf("derived creds secret not created: %v", err)
	}

	var job batchv1.Job
	if err := k8sClient.Get(ctx, jobKey, &job); err != nil {
		t.Fatalf("verification Job not created: %v", err)
	}
	if len(job.Spec.Template.Spec.Containers) != 1 {
		t.Errorf("verification Job: want 1 container, got %d", len(job.Spec.Template.Spec.Containers))
	}
	cmd := strings.Join(job.Spec.Template.Spec.Containers[0].Command, " ")
	if !strings.Contains(cmd, "verify.sh") {
		t.Errorf("verification Job container should invoke verify.sh, got %q", cmd)
	}

	// The ephemeral datadir must be an emptyDir (fsGroup makes it writable to
	// the non-root verify user) and must NOT set a SizeLimit — a size-limited
	// emptyDir is set up as a separate mount that is not always fsGroup-chowned.
	var datadirVol *corev1.Volume
	for i := range job.Spec.Template.Spec.Volumes {
		if job.Spec.Template.Spec.Volumes[i].Name == "datadir" {
			datadirVol = &job.Spec.Template.Spec.Volumes[i]
			break
		}
	}
	if datadirVol == nil || datadirVol.EmptyDir == nil {
		t.Errorf("verification Job datadir volume must be an emptyDir, got %+v", datadirVol)
	} else if datadirVol.EmptyDir.SizeLimit != nil {
		t.Errorf("datadir emptyDir must not set SizeLimit (breaks fsGroup on CI): %+v", datadirVol.EmptyDir)
	}

	// The verification Job mounts the backup source read-only, so
	// mysqlsh must not try to write its default load-progress file
	// next to the dump. The reconciler signals "disabled" by setting
	// BLOODRAVEN_LOAD_PROGRESS_FILE to an empty string; restore.py
	// then passes progressFile="" to util.loadDump (which mysqlsh
	// treats as "disable progress tracking"). Dropping this env entry
	// silently regresses the fix from 4e120d6, so assert it
	// explicitly.
	var gotProgress *corev1.EnvVar
	for i := range job.Spec.Template.Spec.Containers[0].Env {
		e := &job.Spec.Template.Spec.Containers[0].Env[i]
		if e.Name == "BLOODRAVEN_LOAD_PROGRESS_FILE" {
			gotProgress = e
			break
		}
	}
	if gotProgress == nil {
		t.Errorf("verification Job must set BLOODRAVEN_LOAD_PROGRESS_FILE (empty string disables mysqlsh progress tracking on the read-only backup mount); env missing")
	} else if gotProgress.Value != "" {
		t.Errorf("verification Job must set BLOODRAVEN_LOAD_PROGRESS_FILE=\"\" to disable mysqlsh progress tracking on the read-only backup mount; got %q", gotProgress.Value)
	}

	var after v1alpha1.MysqlBackupVerification
	if err := k8sClient.Get(ctx, nn, &after); err != nil {
		t.Fatalf("get verify after reconcile: %v", err)
	}
	if after.Status.Phase != v1alpha1.VerificationPhaseRestoring {
		t.Errorf("want phase=Restoring once Job is created, got %q", after.Status.Phase)
	}
	if after.Status.JobName != jobKey.Name {
		t.Errorf("status.jobName not stamped: %q", after.Status.JobName)
	}
	if after.Status.BackupRef == nil || after.Status.BackupRef.Name != backup.Name {
		t.Errorf("status.backupRef not resolved: %+v", after.Status.BackupRef)
	}
	if !containsFinalizer(after.Finalizers, "shipstream.io/mysqlbackup-verification") {
		t.Errorf("finalizer not stamped: %v", after.Finalizers)
	}
}

func TestEnvtest_Verification_ReconcilerTransitionsToSucceededOnJobComplete(t *testing.T) {
	ns := "envtest-verify-succeeded"
	ensureNamespace(t, ns)
	ensureSecret(t, ns)

	fg := newVerificationFG(ns)
	if err := k8sClient.Create(ctx, fg); err != nil {
		t.Fatalf("create fg: %v", err)
	}

	backup := newSucceededBackup(ns, fg.Name, "nightly-s3")
	if err := k8sClient.Create(ctx, backup); err != nil {
		t.Fatalf("create backup: %v", err)
	}
	backup.Status = v1alpha1.MysqlBackupStatus{
		Phase:          v1alpha1.BackupPhaseSucceeded,
		CompletionTime: &metav1.Time{Time: time.Now().Add(-1 * time.Hour)},
		Location:       "s3://bloodraven/lion/lion-nightly-ok/",
		StorageType:    v1alpha1.BackupStorageS3,
		SizeBytes:      512 * 1024 * 1024,
	}
	if err := k8sClient.Status().Update(ctx, backup); err != nil {
		t.Fatalf("update backup status: %v", err)
	}

	cm := &corev1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{Name: "mysql-lion-backup-scripts", Namespace: ns},
		Data:       map[string]string{"verify.sh": "#!/bin/bash\ntrue"},
	}
	if err := k8sClient.Create(ctx, cm); err != nil {
		t.Fatalf("create scripts configmap: %v", err)
	}

	verify := &v1alpha1.MysqlBackupVerification{
		ObjectMeta: metav1.ObjectMeta{Name: "verify-ok", Namespace: ns},
		Spec: v1alpha1.MysqlBackupVerificationSpec{
			FailoverGroupRef: v1alpha1.LocalGroupRef{Name: fg.Name},
			ProfileName:      "nightly-s3",
		},
	}
	if err := k8sClient.Create(ctx, verify); err != nil {
		t.Fatalf("create verify: %v", err)
	}

	r := &controller.MysqlBackupVerificationReconciler{
		Client:   k8sClient,
		Scheme:   scheme,
		Recorder: record.NewFakeRecorder(10),
	}
	nn := types.NamespacedName{Name: verify.Name, Namespace: ns}

	jobKey := types.NamespacedName{Name: "mysqlverify-verify-ok", Namespace: ns}
	reconcileUntil(t, r, nn, func() bool {
		var j batchv1.Job
		return k8sClient.Get(ctx, jobKey, &j) == nil
	})

	// Simulate the Job completing successfully. The reconciler reads
	// the terminal condition via status.conditions on the Job. k8s
	// 1.35+ requires startTime to be set and both SuccessCriteriaMet
	// and Complete conditions for a valid terminal write.
	var job batchv1.Job
	if err := k8sClient.Get(ctx, jobKey, &job); err != nil {
		t.Fatalf("get job: %v", err)
	}
	now := metav1.Now()
	start := metav1.Time{Time: now.Add(-5 * time.Minute)}
	job.Status.StartTime = &start
	job.Status.Conditions = append(job.Status.Conditions,
		batchv1.JobCondition{
			Type:               batchv1.JobSuccessCriteriaMet,
			Status:             corev1.ConditionTrue,
			LastTransitionTime: now,
		},
		batchv1.JobCondition{
			Type:               batchv1.JobComplete,
			Status:             corev1.ConditionTrue,
			LastTransitionTime: now,
		},
	)
	job.Status.Succeeded = 1
	job.Status.CompletionTime = &now
	if err := k8sClient.Status().Update(ctx, &job); err != nil {
		t.Fatalf("update job status: %v", err)
	}

	reconcileUntil(t, r, nn, func() bool {
		var got v1alpha1.MysqlBackupVerification
		if err := k8sClient.Get(ctx, nn, &got); err != nil {
			return false
		}
		return got.Status.Phase == v1alpha1.VerificationPhaseSucceeded
	})

	var terminal v1alpha1.MysqlBackupVerification
	if err := k8sClient.Get(ctx, nn, &terminal); err != nil {
		t.Fatalf("get final: %v", err)
	}
	if terminal.Status.CompletionTime == nil {
		t.Error("status.completionTime not stamped on Succeeded")
	}
	var seenVerified bool
	for _, c := range terminal.Status.Conditions {
		if c.Type == controller.ConditionVerified {
			seenVerified = true
			if c.Status != metav1.ConditionTrue {
				t.Errorf("Verified condition status: want True, got %s", c.Status)
			}
		}
	}
	if !seenVerified {
		t.Errorf("Verified condition missing: %+v", terminal.Status.Conditions)
	}
}

func TestEnvtest_Verification_BackupNotFound_FailsWithReason(t *testing.T) {
	ns := "envtest-verify-nobackup"
	ensureNamespace(t, ns)

	// Create the fg but no MysqlBackup — resolveBackup should fail and
	// the reconciler should transition the CR to Failed with a specific
	// reason so operators can filter on it.
	fg := newVerificationFG(ns)
	if err := k8sClient.Create(ctx, fg); err != nil {
		t.Fatalf("create fg: %v", err)
	}

	verify := &v1alpha1.MysqlBackupVerification{
		ObjectMeta: metav1.ObjectMeta{Name: "verify-no-backup", Namespace: ns},
		Spec: v1alpha1.MysqlBackupVerificationSpec{
			FailoverGroupRef: v1alpha1.LocalGroupRef{Name: fg.Name},
			ProfileName:      "nightly-s3",
		},
	}
	if err := k8sClient.Create(ctx, verify); err != nil {
		t.Fatalf("create verify: %v", err)
	}

	r := &controller.MysqlBackupVerificationReconciler{
		Client:   k8sClient,
		Scheme:   scheme,
		Recorder: record.NewFakeRecorder(10),
	}
	nn := types.NamespacedName{Name: verify.Name, Namespace: ns}

	reconcileUntil(t, r, nn, func() bool {
		var got v1alpha1.MysqlBackupVerification
		if err := k8sClient.Get(ctx, nn, &got); err != nil {
			return false
		}
		return got.Status.Phase == v1alpha1.VerificationPhaseFailed
	})

	var terminal v1alpha1.MysqlBackupVerification
	if err := k8sClient.Get(ctx, nn, &terminal); err != nil {
		t.Fatalf("get final: %v", err)
	}
	var reason string
	for _, c := range terminal.Status.Conditions {
		if c.Type == controller.ConditionVerified {
			reason = c.Reason
		}
	}
	if reason != "BackupNotAvailable" {
		t.Errorf("Verified condition reason: want BackupNotAvailable, got %q (msg=%q)", reason, terminal.Status.Message)
	}
}

func containsFinalizer(fs []string, want string) bool {
	for _, f := range fs {
		if f == want {
			return true
		}
	}
	return false
}
