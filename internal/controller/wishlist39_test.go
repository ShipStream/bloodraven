package controller

// Tests for WISHLIST #39: default resource requests on
// cleanup/restore/init containers, AutomountServiceAccountToken=false on
// execution Jobs, and opt-in MySQL Deployment + Dragonfly StatefulSet
// security contexts.
//
// Helper-name collisions: envMap, fgWithBackup, fgWithEncryptedBackup,
// fgWithDragonfly, fgInitFromMysqlBackup, succeededSeedBackup,
// successfulBackup, newReconciler, verifyFG already live in other test
// files. This file does not redeclare them.

import (
	"context"
	"testing"

	appsv1 "k8s.io/api/apps/v1"
	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/equality"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"

	v1alpha1 "github.com/shipstream/bloodraven/api/v1alpha1"
)

// hasResourceRequests is a test-only helper that reports whether the
// supplied ResourceRequirements carries at least one non-zero entry in
// Requests. Limits are NOT considered — use hasAnyResourceSpec for the
// broader "user supplied anything" gate. This helper is kept in test
// code because it has no production callers.
func hasResourceRequests(r corev1.ResourceRequirements) bool {
	return hasAnyNonZero(r.Requests)
}

// --- defaults.go --------------------------------------------------------

func TestDefaultRestoreResources_ReturnsExpectedRequests(t *testing.T) {
	r := defaultRestoreResources()
	if got, want := r.Requests.Cpu().String(), "100m"; got != want {
		t.Errorf("cpu request: got %q want %q", got, want)
	}
	if got, want := r.Requests.Memory().String(), "128Mi"; got != want {
		t.Errorf("memory request: got %q want %q", got, want)
	}
	if len(r.Limits) != 0 {
		t.Errorf("limits should be empty, got %v", r.Limits)
	}
}

func TestDefaultInitContainerResources_ReturnsExpectedRequests(t *testing.T) {
	r := defaultInitContainerResources()
	if got, want := r.Requests.Cpu().String(), "100m"; got != want {
		t.Errorf("cpu request: got %q want %q", got, want)
	}
	if got, want := r.Requests.Memory().String(), "128Mi"; got != want {
		t.Errorf("memory request: got %q want %q", got, want)
	}
	if len(r.Limits) != 0 {
		t.Errorf("limits should be empty, got %v", r.Limits)
	}
}

func TestEffectiveBackupResources_TableDriven(t *testing.T) {
	overrideReq := corev1.ResourceList{
		corev1.ResourceCPU:    resource.MustParse("500m"),
		corev1.ResourceMemory: resource.MustParse("512Mi"),
	}
	fallback := defaultInitContainerResources()

	cases := []struct {
		name    string
		src     *backupResourcesSource
		wantCPU string
		wantMem string
	}{
		{"nil-src-uses-fallback", nil, "100m", "128Mi"},
		{"empty-src-uses-fallback", &backupResourcesSource{}, "100m", "128Mi"},
		{"override-wins", &backupResourcesSource{Resources: corev1.ResourceRequirements{Requests: overrideReq}}, "500m", "512Mi"},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			got := effectiveBackupResources(c.src, fallback)
			if g, w := got.Requests.Cpu().String(), c.wantCPU; g != w {
				t.Errorf("cpu: got %q want %q", g, w)
			}
			if g, w := got.Requests.Memory().String(), c.wantMem; g != w {
				t.Errorf("mem: got %q want %q", g, w)
			}
		})
	}
}

func TestHasResourceRequests_TableDriven(t *testing.T) {
	cases := []struct {
		name string
		req  corev1.ResourceRequirements
		want bool
	}{
		{"empty", corev1.ResourceRequirements{}, false},
		{"zero-cpu-only", corev1.ResourceRequirements{Requests: corev1.ResourceList{corev1.ResourceCPU: resource.MustParse("0")}}, false},
		{"cpu-set", corev1.ResourceRequirements{Requests: corev1.ResourceList{corev1.ResourceCPU: resource.MustParse("100m")}}, true},
		{"memory-set", corev1.ResourceRequirements{Requests: corev1.ResourceList{corev1.ResourceMemory: resource.MustParse("64Mi")}}, true},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			if got := hasResourceRequests(c.req); got != c.want {
				t.Errorf("hasResourceRequests(%v) = %v, want %v", c.req, got, c.want)
			}
		})
	}
}

// --- cleanup Job: Resources flow + AutomountServiceAccountToken ---------

func TestBuildCleanupJob_AppliesBackupResourcesAndDropsSAToken(t *testing.T) {
	fg := fgWithBackup()
	// Stamp an explicit override on spec.backup.resources so the cleanup
	// Job has a deterministic value to match.
	fg.Spec.Backup.Resources = corev1.ResourceRequirements{
		Requests: corev1.ResourceList{
			corev1.ResourceCPU:    resource.MustParse("250m"),
			corev1.ResourceMemory: resource.MustParse("256Mi"),
		},
	}
	profile := fg.Spec.Backup.Profiles[0]
	backup := &v1alpha1.MysqlBackup{
		ObjectMeta: metav1.ObjectMeta{Name: "lion-cleanup", Namespace: "ns"},
		Status: v1alpha1.MysqlBackupStatus{
			Location:    "lion/lion-cleanup/",
			StorageType: v1alpha1.BackupStorageS3,
		},
	}
	job, err := buildCleanupJob(cleanupJobInputs{
		FailoverGroup:        fg,
		Profile:              &profile,
		Backup:               backup,
		CredsSecretName:      "x",
		ScriptsConfigMapName: "y",
	})
	if err != nil {
		t.Fatalf("buildCleanupJob: %v", err)
	}
	c := job.Spec.Template.Spec.Containers[0]
	if g, w := c.Resources.Requests.Cpu().String(), "250m"; g != w {
		t.Errorf("cleanup cpu: got %q want %q", g, w)
	}
	if g, w := c.Resources.Requests.Memory().String(), "256Mi"; g != w {
		t.Errorf("cleanup memory: got %q want %q", g, w)
	}
	amt := job.Spec.Template.Spec.AutomountServiceAccountToken
	if amt == nil || *amt {
		t.Errorf("AutomountServiceAccountToken want false, got %v", amt)
	}
}

func TestBuildCleanupJob_NilBackupSpec_UsesDefaultResources(t *testing.T) {
	fg := fgWithBackup()
	// Drop the BackupSpec to exercise the fallback path. Cleanup builder
	// only consumes BackupSpec for ImagePullSecrets and SecurityContext.
	fg.Spec.Backup = nil
	profile := v1alpha1.BackupProfile{
		Name: "fallback",
		Storage: v1alpha1.BackupStorage{
			Type: v1alpha1.BackupStorageS3,
			S3: &v1alpha1.S3Storage{
				Bucket:            "bucket",
				CredentialsSecret: "s3-creds",
			},
		},
	}
	backup := &v1alpha1.MysqlBackup{
		ObjectMeta: metav1.ObjectMeta{Name: "lion-cleanup", Namespace: "ns"},
		Status: v1alpha1.MysqlBackupStatus{
			Location:    "bucket/key/",
			StorageType: v1alpha1.BackupStorageS3,
		},
	}
	job, err := buildCleanupJob(cleanupJobInputs{
		FailoverGroup:        fg,
		Profile:              &profile,
		Backup:               backup,
		CredsSecretName:      "x",
		ScriptsConfigMapName: "y",
	})
	if err != nil {
		t.Fatalf("buildCleanupJob: %v", err)
	}
	c := job.Spec.Template.Spec.Containers[0]
	if g, w := c.Resources.Requests.Cpu().String(), "100m"; g != w {
		t.Errorf("cleanup default cpu: got %q want %q", g, w)
	}
	if g, w := c.Resources.Requests.Memory().String(), "128Mi"; g != w {
		t.Errorf("cleanup default memory: got %q want %q", g, w)
	}
}

func TestBuildCleanupJob_HardenedSecurityContextStillApplied(t *testing.T) {
	fg := fgWithBackup()
	profile := fg.Spec.Backup.Profiles[0]
	backup := &v1alpha1.MysqlBackup{
		ObjectMeta: metav1.ObjectMeta{Name: "lion-cleanup", Namespace: "ns"},
		Status: v1alpha1.MysqlBackupStatus{
			Location:    "lion/lion-cleanup/",
			StorageType: v1alpha1.BackupStorageS3,
		},
	}
	job, err := buildCleanupJob(cleanupJobInputs{
		FailoverGroup:        fg,
		Profile:              &profile,
		Backup:               backup,
		CredsSecretName:      "x",
		ScriptsConfigMapName: "y",
	})
	if err != nil {
		t.Fatalf("buildCleanupJob: %v", err)
	}
	podSC := job.Spec.Template.Spec.SecurityContext
	if podSC == nil || podSC.RunAsNonRoot == nil || !*podSC.RunAsNonRoot {
		t.Errorf("regression: pod SC RunAsNonRoot should be true: %+v", podSC)
	}
	cSC := job.Spec.Template.Spec.Containers[0].SecurityContext
	if cSC == nil || cSC.ReadOnlyRootFilesystem == nil || !*cSC.ReadOnlyRootFilesystem {
		t.Errorf("regression: container SC ReadOnlyRootFilesystem should be true: %+v", cSC)
	}
}

// --- backup execution Job: AutomountServiceAccountToken=false -----------

func TestBuildBackupJob_AutomountServiceAccountToken_False(t *testing.T) {
	fg := fgWithBackup()
	mb := &v1alpha1.MysqlBackup{
		ObjectMeta: metav1.ObjectMeta{Name: "lion-nightly-abc", Namespace: "ns"},
		Spec:       v1alpha1.MysqlBackupSpec{FailoverGroupRef: v1alpha1.LocalGroupRef{Name: "lion"}, ProfileName: "nightly-s3"},
	}
	job, err := BuildBackupJob(BackupJobInputs{
		FailoverGroup:        fg,
		Profile:              fg.Spec.Backup.Profiles[0],
		Backup:               mb,
		SourceSite:           "pdx",
		CredsSecretName:      "creds",
		ScriptsConfigMapName: "scripts",
	})
	if err != nil {
		t.Fatalf("BuildBackupJob: %v", err)
	}
	amt := job.Spec.Template.Spec.AutomountServiceAccountToken
	if amt == nil || *amt {
		t.Errorf("AutomountServiceAccountToken want false, got %v", amt)
	}
}

// --- verification Job: AutomountServiceAccountToken + decrypt init ------

func TestBuildVerificationJob_AutomountServiceAccountToken_False(t *testing.T) {
	fg := verifyFG()
	backup := successfulBackup("verify-amt", "lion", "nightly-s3")
	v := &v1alpha1.MysqlBackupVerification{
		ObjectMeta: metav1.ObjectMeta{Name: "verify-amt", Namespace: "ns"},
		Spec: v1alpha1.MysqlBackupVerificationSpec{
			FailoverGroupRef: v1alpha1.LocalGroupRef{Name: "lion"},
			ProfileName:      "nightly-s3",
		},
	}
	job, err := buildVerificationJob(verificationJobInputs{
		FailoverGroup:        fg,
		Profile:              fg.Spec.Backup.Profiles[0],
		Verification:         v,
		Backup:               backup,
		CredsSecretName:      "x",
		ScriptsConfigMapName: "y",
	})
	if err != nil {
		t.Fatalf("buildVerificationJob: %v", err)
	}
	amt := job.Spec.Template.Spec.AutomountServiceAccountToken
	if amt == nil || *amt {
		t.Errorf("AutomountServiceAccountToken want false, got %v", amt)
	}
}

func TestBuildVerificationJob_EncryptedSource_InitContainerCarriesResources(t *testing.T) {
	SetOperatorImageDefaults("bloodraven:test", "bloodraven")
	defer SetOperatorImageDefaults("", "")

	fg := fgWithEncryptedBackup()
	backup := successfulBackup("verify-enc", "lion", "nightly-s3")
	backup.Status.Encrypted = true
	backup.Status.EncryptionAlgorithm = "AES-256-GCM"

	v := &v1alpha1.MysqlBackupVerification{
		ObjectMeta: metav1.ObjectMeta{Name: "verify-enc", Namespace: "ns"},
		Spec: v1alpha1.MysqlBackupVerificationSpec{
			FailoverGroupRef: v1alpha1.LocalGroupRef{Name: "lion"},
			ProfileName:      "nightly-s3",
		},
	}
	job, err := buildVerificationJob(verificationJobInputs{
		FailoverGroup:        fg,
		Profile:              fg.Spec.Backup.Profiles[0],
		Verification:         v,
		Backup:               backup,
		CredsSecretName:      "x",
		ScriptsConfigMapName: "y",
	})
	if err != nil {
		t.Fatalf("buildVerificationJob: %v", err)
	}
	var saw bool
	for _, c := range job.Spec.Template.Spec.InitContainers {
		if c.Name != "decrypt-download" {
			continue
		}
		saw = true
		if g, w := c.Resources.Requests.Cpu().String(), "100m"; g != w {
			t.Errorf("decrypt-download cpu: got %q want %q", g, w)
		}
		if g, w := c.Resources.Requests.Memory().String(), "128Mi"; g != w {
			t.Errorf("decrypt-download memory: got %q want %q", g, w)
		}
	}
	if !saw {
		t.Fatal("decrypt-download init container missing")
	}
}

// --- restore Job (bootstrap + in-place share buildRestoreJobSpec) -------

func TestBuildRestoreJob_MainContainerCarriesBackupResources(t *testing.T) {
	fg := fgInitFromMysqlBackup()
	fg.Spec.Backup.Resources = corev1.ResourceRequirements{
		Requests: corev1.ResourceList{
			corev1.ResourceCPU:    resource.MustParse("500m"),
			corev1.ResourceMemory: resource.MustParse("512Mi"),
		},
	}
	seed := succeededSeedBackup()
	r, _ := newReconciler(fg, seed)

	job, err := r.buildRestoreJob(context.Background(), fg, fg.Spec.Sites[0].Name, "creds")
	if err != nil {
		t.Fatalf("buildRestoreJob: %v", err)
	}
	c := job.Spec.Template.Spec.Containers[0]
	if g, w := c.Resources.Requests.Cpu().String(), "500m"; g != w {
		t.Errorf("restore cpu: got %q want %q", g, w)
	}
	if g, w := c.Resources.Requests.Memory().String(), "512Mi"; g != w {
		t.Errorf("restore memory: got %q want %q", g, w)
	}
	amt := job.Spec.Template.Spec.AutomountServiceAccountToken
	if amt == nil || *amt {
		t.Errorf("AutomountServiceAccountToken want false, got %v", amt)
	}
}

func TestBuildRestoreJob_NilBackupSpec_UsesDefaultResources(t *testing.T) {
	fg := fgInitFromMysqlBackup()
	// Replace the InitFromBackupSource so the build path doesn't need
	// fg.Spec.Backup at all. S3-direct source is the cleanest.
	fg.Spec.InitFromBackup = &v1alpha1.InitFromBackupSpec{
		Source: v1alpha1.InitFromBackupSource{
			S3: &v1alpha1.S3Storage{
				Bucket:            "external",
				Prefix:            "dumps/x",
				CredentialsSecret: "s3-creds",
			},
		},
	}
	fg.Spec.Backup = nil

	r, _ := newReconciler(fg)
	job, err := r.buildRestoreJob(context.Background(), fg, fg.Spec.Sites[0].Name, "creds")
	if err != nil {
		t.Fatalf("buildRestoreJob: %v", err)
	}
	c := job.Spec.Template.Spec.Containers[0]
	if g, w := c.Resources.Requests.Cpu().String(), "100m"; g != w {
		t.Errorf("restore default cpu: got %q want %q", g, w)
	}
	if g, w := c.Resources.Requests.Memory().String(), "128Mi"; g != w {
		t.Errorf("restore default memory: got %q want %q", g, w)
	}
}

func TestBuildRestoreJob_DecryptInitContainerCarriesResources(t *testing.T) {
	SetOperatorImageDefaults("bloodraven:test", "bloodraven")
	defer SetOperatorImageDefaults("", "")

	fg := fgWithEncryptedBackup()
	fg.Spec.InitFromBackup = &v1alpha1.InitFromBackupSpec{
		Source: v1alpha1.InitFromBackupSource{
			MysqlBackupRef: &corev1.LocalObjectReference{Name: "seed"},
		},
	}
	seed := &v1alpha1.MysqlBackup{
		ObjectMeta: metav1.ObjectMeta{Name: "seed", Namespace: "ns"},
		Spec: v1alpha1.MysqlBackupSpec{
			FailoverGroupRef: v1alpha1.LocalGroupRef{Name: "lion"},
			ProfileName:      "nightly-s3",
		},
		Status: v1alpha1.MysqlBackupStatus{
			Phase:               v1alpha1.BackupPhaseSucceeded,
			Location:            "lion/seed/",
			StorageType:         v1alpha1.BackupStorageS3,
			Encrypted:           true,
			EncryptionAlgorithm: "AES-256-GCM",
		},
	}
	r, _ := newReconciler(fg, seed)
	job, err := r.buildRestoreJob(context.Background(), fg, fg.Spec.Sites[0].Name, "creds")
	if err != nil {
		t.Fatalf("buildRestoreJob: %v", err)
	}
	var saw bool
	for _, c := range job.Spec.Template.Spec.InitContainers {
		if c.Name != "decrypt-download" {
			continue
		}
		saw = true
		if g, w := c.Resources.Requests.Cpu().String(), "100m"; g != w {
			t.Errorf("decrypt-download cpu: got %q want %q", g, w)
		}
		if g, w := c.Resources.Requests.Memory().String(), "128Mi"; g != w {
			t.Errorf("decrypt-download memory: got %q want %q", g, w)
		}
	}
	if !saw {
		t.Fatal("decrypt-download init container missing")
	}
}

// --- backup schedule + verification schedule CronJob templates ----------
// The trigger pods *must* keep the SA token mounted — they create
// MysqlBackup / MysqlBackupVerification CRs via the API.

func TestReconcileBackupSchedules_TriggerCronJob_LeavesAutomountUnset(t *testing.T) {
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
		t.Fatalf("cronjob: %v", err)
	}
	amt := cj.Spec.JobTemplate.Spec.Template.Spec.AutomountServiceAccountToken
	if amt != nil {
		t.Errorf("trigger CronJob must NOT set AutomountServiceAccountToken (got %v); "+
			"the trigger needs the SA token to POST a MysqlBackup CR", amt)
	}
}

func TestReconcileVerificationSchedules_TriggerCronJob_LeavesAutomountUnset(t *testing.T) {
	SetOperatorImageDefaults("bloodraven:test", "bloodraven")
	defer SetOperatorImageDefaults("", "")

	fg := fgWithBackup()
	// Enable verification on the first profile so reconcileVerificationSchedules
	// renders a CronJob for it.
	fg.Spec.Backup.Profiles[0].Verification = &v1alpha1.VerificationSpec{
		Enabled:  true,
		Schedule: "0 3 * * 0",
	}
	r, c := newReconciler(fg)
	if err := r.reconcileVerificationSchedules(context.Background(), fg); err != nil {
		t.Fatalf("reconcileVerificationSchedules: %v", err)
	}
	var cj batchv1.CronJob
	if err := c.Get(context.Background(), types.NamespacedName{
		Name: verificationScheduleCronJobName("lion", "nightly-s3"), Namespace: "ns",
	}, &cj); err != nil {
		t.Fatalf("verification cronjob: %v", err)
	}
	amt := cj.Spec.JobTemplate.Spec.Template.Spec.AutomountServiceAccountToken
	if amt != nil {
		t.Errorf("verification trigger CronJob must NOT set AutomountServiceAccountToken (got %v)", amt)
	}
}

// --- MySQL Deployment security context (B3) ----------------------------

func TestReconcileDeployment_NilSecurityContext_PreservesLegacyShape(t *testing.T) {
	fg := newTestFG()
	r, c := newReconciler(fg)
	if _, err := r.Reconcile(context.Background(), ctrl.Request{
		NamespacedName: types.NamespacedName{Name: fg.Name, Namespace: fg.Namespace},
	}); err != nil {
		t.Fatalf("reconcile: %v", err)
	}
	for _, site := range fg.Spec.Sites {
		var d appsv1.Deployment
		if err := c.Get(context.Background(), types.NamespacedName{
			Name: "mysql-lion-" + site.Name, Namespace: fg.Namespace,
		}, &d); err != nil {
			t.Fatalf("deployment %s: %v", site.Name, err)
		}
		if d.Spec.Template.Spec.SecurityContext != nil {
			t.Errorf("%s: pod SC should be nil when spec.podSecurityContext is unset, got %+v",
				site.Name, d.Spec.Template.Spec.SecurityContext)
		}
		for _, ct := range d.Spec.Template.Spec.Containers {
			if ct.SecurityContext != nil {
				t.Errorf("%s/%s: container SC should be nil when spec.containerSecurityContext is unset, got %+v",
					site.Name, ct.Name, ct.SecurityContext)
			}
		}
	}
}

func TestReconcileDeployment_PITRSidecarDefaultsToMysqlDataUser(t *testing.T) {
	fg := newTestFG()
	fg.Spec.Backup = &v1alpha1.BackupSpec{
		PITR: &v1alpha1.PITRSpec{Enabled: true, ProfileName: "nightly"},
		Profiles: []v1alpha1.BackupProfile{{
			Name: "nightly",
			Storage: v1alpha1.BackupStorage{
				Type: v1alpha1.BackupStorageS3,
				S3: &v1alpha1.S3Storage{
					Bucket:            "backups",
					CredentialsSecret: "aws-creds",
				},
			},
		}},
	}
	r, c := newReconciler(fg)
	if _, err := r.Reconcile(context.Background(), ctrl.Request{
		NamespacedName: types.NamespacedName{Name: fg.Name, Namespace: fg.Namespace},
	}); err != nil {
		t.Fatalf("reconcile: %v", err)
	}

	var d appsv1.Deployment
	if err := c.Get(context.Background(), types.NamespacedName{
		Name: "mysql-lion-dc1", Namespace: fg.Namespace,
	}, &d); err != nil {
		t.Fatalf("deployment: %v", err)
	}

	var mysqlSC, sidecarSC *corev1.SecurityContext
	for _, ct := range d.Spec.Template.Spec.Containers {
		switch ct.Name {
		case "mysql":
			mysqlSC = ct.SecurityContext
		case "sidecar":
			sidecarSC = ct.SecurityContext
		}
	}
	if mysqlSC != nil {
		t.Errorf("mysql container SC should remain nil when spec.containerSecurityContext is unset, got %+v", mysqlSC)
	}
	if sidecarSC == nil || sidecarSC.RunAsUser == nil || *sidecarSC.RunAsUser != mysqlDataUID ||
		sidecarSC.RunAsGroup == nil || *sidecarSC.RunAsGroup != mysqlDataGID {
		t.Fatalf("sidecar SC should default to mysql data uid/gid for PITR, got %+v", sidecarSC)
	}
}

func TestReconcileDeployment_AppliesPodAndContainerSecurityContextVerbatim(t *testing.T) {
	t1 := true
	f1 := false
	uid := int64(999)
	gid := int64(999)
	wantPod := &corev1.PodSecurityContext{
		RunAsNonRoot: &t1,
		RunAsUser:    &uid,
		RunAsGroup:   &gid,
		FSGroup:      &gid,
		SeccompProfile: &corev1.SeccompProfile{
			Type: corev1.SeccompProfileTypeRuntimeDefault,
		},
	}
	wantContainer := &corev1.SecurityContext{
		AllowPrivilegeEscalation: &f1,
		ReadOnlyRootFilesystem:   &f1, // MySQL needs to write to runtime paths outside the PVC
		RunAsNonRoot:             &t1,
		Capabilities: &corev1.Capabilities{
			Drop: []corev1.Capability{"ALL"},
		},
		SeccompProfile: &corev1.SeccompProfile{
			Type: corev1.SeccompProfileTypeRuntimeDefault,
		},
	}

	fg := newTestFG()
	fg.Spec.PodSecurityContext = wantPod.DeepCopy()
	fg.Spec.ContainerSecurityContext = wantContainer.DeepCopy()
	r, c := newReconciler(fg)
	if _, err := r.Reconcile(context.Background(), ctrl.Request{
		NamespacedName: types.NamespacedName{Name: fg.Name, Namespace: fg.Namespace},
	}); err != nil {
		t.Fatalf("reconcile: %v", err)
	}
	var d appsv1.Deployment
	if err := c.Get(context.Background(), types.NamespacedName{
		Name: "mysql-lion-dc1", Namespace: fg.Namespace,
	}, &d); err != nil {
		t.Fatalf("deployment: %v", err)
	}
	gotPod := d.Spec.Template.Spec.SecurityContext
	if !equality.Semantic.DeepEqual(gotPod, wantPod) {
		t.Errorf("pod SC: full struct must match verbatim\nwant: %+v\n got: %+v", wantPod, gotPod)
	}
	// Verify both mysql and sidecar containers received the same SC.
	gotContainers := map[string]*corev1.SecurityContext{}
	for _, ct := range d.Spec.Template.Spec.Containers {
		gotContainers[ct.Name] = ct.SecurityContext
	}
	for _, name := range []string{"mysql", "sidecar"} {
		sc := gotContainers[name]
		if sc == nil {
			t.Errorf("%s: container SC is nil but should be set", name)
			continue
		}
		if !equality.Semantic.DeepEqual(sc, wantContainer) {
			t.Errorf("%s: container SC must match verbatim (no operator merge)\nwant: %+v\n got: %+v", name, wantContainer, sc)
		}
	}
}

// --- Dragonfly StatefulSet security context (B4) ------------------------

func TestReconcileDragonflyStatefulSet_NilSecurityContext_PreservesLegacyShape(t *testing.T) {
	fg := fgWithDragonfly()
	r, c := newReconciler(fg)
	if _, err := r.reconcileDragonflyResources(context.Background(), fg); err != nil {
		t.Fatalf("reconcileDragonflyResources: %v", err)
	}
	var sts appsv1.StatefulSet
	if err := c.Get(context.Background(), types.NamespacedName{
		Name: "lion-dragonfly-dc1", Namespace: fg.Namespace,
	}, &sts); err != nil {
		t.Fatalf("statefulset: %v", err)
	}
	if sts.Spec.Template.Spec.SecurityContext != nil {
		t.Errorf("dragonfly pod SC should be nil when spec.dragonfly.podSecurityContext is unset, got %+v",
			sts.Spec.Template.Spec.SecurityContext)
	}
	for _, ct := range sts.Spec.Template.Spec.Containers {
		if ct.SecurityContext != nil {
			t.Errorf("dragonfly/%s: container SC should be nil when spec.dragonfly.containerSecurityContext is unset, got %+v",
				ct.Name, ct.SecurityContext)
		}
	}
}

func TestReconcileDragonflyStatefulSet_AppliesSecurityContextVerbatim(t *testing.T) {
	t1 := true
	f1 := false
	uid := int64(999)
	gid := int64(999)

	wantPod := &corev1.PodSecurityContext{
		RunAsNonRoot: &t1,
		RunAsUser:    &uid,
		RunAsGroup:   &gid,
		FSGroup:      &gid,
	}
	wantContainer := &corev1.SecurityContext{
		AllowPrivilegeEscalation: &f1,
		ReadOnlyRootFilesystem:   &t1,
		RunAsNonRoot:             &t1,
		Capabilities: &corev1.Capabilities{
			Drop: []corev1.Capability{"ALL"},
		},
	}

	fg := fgWithDragonfly(func(fg *v1alpha1.MysqlFailoverGroup) {
		fg.Spec.Dragonfly.PodSecurityContext = wantPod.DeepCopy()
		fg.Spec.Dragonfly.ContainerSecurityContext = wantContainer.DeepCopy()
	})
	r, c := newReconciler(fg)
	if _, err := r.reconcileDragonflyResources(context.Background(), fg); err != nil {
		t.Fatalf("reconcileDragonflyResources: %v", err)
	}
	var sts appsv1.StatefulSet
	if err := c.Get(context.Background(), types.NamespacedName{
		Name: "lion-dragonfly-dc1", Namespace: fg.Namespace,
	}, &sts); err != nil {
		t.Fatalf("statefulset: %v", err)
	}
	gotPod := sts.Spec.Template.Spec.SecurityContext
	if !equality.Semantic.DeepEqual(gotPod, wantPod) {
		t.Errorf("dragonfly pod SC: full struct must match verbatim\nwant: %+v\n got: %+v", wantPod, gotPod)
	}
	if len(sts.Spec.Template.Spec.Containers) == 0 {
		t.Fatal("no dragonfly container rendered")
	}
	gotContainer := sts.Spec.Template.Spec.Containers[0].SecurityContext
	if !equality.Semantic.DeepEqual(gotContainer, wantContainer) {
		t.Errorf("dragonfly container SC: full struct must match verbatim (no operator merge)\nwant: %+v\n got: %+v", wantContainer, gotContainer)
	}
}

// --- F1: Dragonfly drift detection includes new SecurityContext fields --

// TestDragonflyStatefulSetTemplateEqual_DetectsSecurityContextDrift verifies
// that dragonflyStatefulSetTemplateEqual returns false when only the
// pod-level or container-level SecurityContext differs. Without this, an
// existing STS would silently keep its old (nil) SC after a user
// opts in via spec.dragonfly.{pod,container}SecurityContext, defeating
// the opt-in.
func TestDragonflyStatefulSetTemplateEqual_DetectsSecurityContextDrift(t *testing.T) {
	t1 := true
	uid := int64(999)

	// Render the "current" STS at reconcile-A (no SC fields).
	fg := fgWithDragonfly()
	r, _ := newReconciler(fg)
	stsA := &appsv1.StatefulSet{ObjectMeta: metav1.ObjectMeta{Name: "a", Namespace: fg.Namespace}}
	if err := r.applyDragonflyStatefulSetSpec(fg, fg.Spec.Sites[0], stsA); err != nil {
		t.Fatalf("apply A: %v", err)
	}

	// Render the "desired" STS at reconcile-B with podSecurityContext set.
	fgB := fgWithDragonfly(func(fg *v1alpha1.MysqlFailoverGroup) {
		fg.Spec.Dragonfly.PodSecurityContext = &corev1.PodSecurityContext{
			RunAsNonRoot: &t1,
			RunAsUser:    &uid,
		}
	})
	stsB := &appsv1.StatefulSet{ObjectMeta: metav1.ObjectMeta{Name: "b", Namespace: fgB.Namespace}}
	if err := r.applyDragonflyStatefulSetSpec(fgB, fgB.Spec.Sites[0], stsB); err != nil {
		t.Fatalf("apply B: %v", err)
	}
	if dragonflyStatefulSetTemplateEqual(stsA, stsB) {
		t.Errorf("expected drift detected when podSecurityContext is added; got equal=true")
	}

	// Render reconcile-C with containerSecurityContext set instead.
	fgC := fgWithDragonfly(func(fg *v1alpha1.MysqlFailoverGroup) {
		fg.Spec.Dragonfly.ContainerSecurityContext = &corev1.SecurityContext{
			RunAsUser:    &uid,
			RunAsNonRoot: &t1,
		}
	})
	stsC := &appsv1.StatefulSet{ObjectMeta: metav1.ObjectMeta{Name: "c", Namespace: fgC.Namespace}}
	if err := r.applyDragonflyStatefulSetSpec(fgC, fgC.Spec.Sites[0], stsC); err != nil {
		t.Fatalf("apply C: %v", err)
	}
	if dragonflyStatefulSetTemplateEqual(stsA, stsC) {
		t.Errorf("expected drift detected when containerSecurityContext is added; got equal=true")
	}

	// Sanity: STS-A vs STS-A is equal (no false-positive drift).
	if !dragonflyStatefulSetTemplateEqual(stsA, stsA) {
		t.Errorf("self-compare must be equal")
	}
}

// --- F2: ComputeSpecHash includes pod + container SecurityContext ------

// TestComputeSpecHash_IncludesSecurityContexts verifies the rolling-hash
// reacts to changes in spec.{pod,container}SecurityContext. Without
// this, ensureSiteDeployment would not roll the pod when the user
// opts in to a security context, so the new SC would never take effect.
func TestComputeSpecHash_IncludesSecurityContexts(t *testing.T) {
	t1 := true
	uid1 := int64(999)
	uid2 := int64(27)

	fg := newTestFG()
	site := fg.Spec.Sites[0]

	hNil := ComputeSpecHash(fg, site, nil, nil)

	fg2 := fg.DeepCopy()
	fg2.Spec.PodSecurityContext = &corev1.PodSecurityContext{
		RunAsNonRoot: &t1,
		RunAsUser:    &uid1,
	}
	hPod := ComputeSpecHash(fg2, site, nil, nil)
	if hPod == hNil {
		t.Errorf("hash must change when PodSecurityContext goes nil->set; got %q == %q", hPod, hNil)
	}

	fg3 := fg2.DeepCopy()
	fg3.Spec.ContainerSecurityContext = &corev1.SecurityContext{
		RunAsNonRoot: &t1,
		RunAsUser:    &uid1,
	}
	hBoth := ComputeSpecHash(fg3, site, nil, nil)
	if hBoth == hPod {
		t.Errorf("hash must change when ContainerSecurityContext goes nil->set; got %q == %q", hBoth, hPod)
	}

	// Different value in the same field must also produce a new hash.
	fg4 := fg3.DeepCopy()
	fg4.Spec.ContainerSecurityContext.RunAsUser = &uid2
	hBoth2 := ComputeSpecHash(fg4, site, nil, nil)
	if hBoth2 == hBoth {
		t.Errorf("hash must change when ContainerSecurityContext.RunAsUser changes; got %q == %q", hBoth2, hBoth)
	}
}

// --- F3: effectiveBackupResources preserves user Limits ----------------

// TestEffectiveBackupResources_LimitsOnlyPreserved ensures that a user
// who supplies only Limits (no Requests) has those Limits flow through
// rather than be silently replaced by the fallback. Kubernetes will
// default Requests to Limits at admission, so this is the correct
// "user intent wins" behavior.
func TestEffectiveBackupResources_LimitsOnlyPreserved(t *testing.T) {
	src := &backupResourcesSource{
		Resources: corev1.ResourceRequirements{
			Limits: corev1.ResourceList{
				corev1.ResourceMemory: resource.MustParse("1Gi"),
			},
		},
	}
	fallback := defaultInitContainerResources()
	got := effectiveBackupResources(src, fallback)
	if got.Limits == nil || got.Limits.Memory().String() != "1Gi" {
		t.Errorf("limits-only user value must be preserved; got %+v", got)
	}
	if len(got.Requests) != 0 {
		t.Errorf("user did not set Requests; helper must not synthesize them; got %+v", got.Requests)
	}
}

// TestHasAnyResourceSpec_TableDriven exercises the broader gate used by
// effectiveBackupResources.
func TestHasAnyResourceSpec_TableDriven(t *testing.T) {
	cases := []struct {
		name string
		req  corev1.ResourceRequirements
		want bool
	}{
		{"empty", corev1.ResourceRequirements{}, false},
		{"zero-requests-only", corev1.ResourceRequirements{Requests: corev1.ResourceList{corev1.ResourceCPU: resource.MustParse("0")}}, false},
		{"requests-set", corev1.ResourceRequirements{Requests: corev1.ResourceList{corev1.ResourceCPU: resource.MustParse("100m")}}, true},
		{"limits-only-set", corev1.ResourceRequirements{Limits: corev1.ResourceList{corev1.ResourceMemory: resource.MustParse("1Gi")}}, true},
		{"both-set", corev1.ResourceRequirements{
			Requests: corev1.ResourceList{corev1.ResourceCPU: resource.MustParse("100m")},
			Limits:   corev1.ResourceList{corev1.ResourceMemory: resource.MustParse("1Gi")},
		}, true},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			if got := hasAnyResourceSpec(c.req); got != c.want {
				t.Errorf("hasAnyResourceSpec(%+v) = %v, want %v", c.req, got, c.want)
			}
		})
	}
}

// --- F4: Pointer aliasing of SecurityContext fields --------------------

// TestReconcileDeployment_SecurityContextPointersAreIndependent asserts
// the two containers (mysql + sidecar) in the rendered Deployment hold
// distinct pointers for their SecurityContext fields. Without DeepCopy
// they would alias fg.Spec.ContainerSecurityContext, which means a
// later mutation of one container's SC would silently mutate the other.
// The init container's SC pointer must also be independent.
func TestReconcileDeployment_SecurityContextPointersAreIndependent(t *testing.T) {
	t1 := true
	uid := int64(999)

	wantPod := &corev1.PodSecurityContext{RunAsNonRoot: &t1, RunAsUser: &uid}
	wantContainer := &corev1.SecurityContext{RunAsNonRoot: &t1, RunAsUser: &uid}

	fg := newTestFG()
	fg.Spec.PodSecurityContext = wantPod.DeepCopy()
	fg.Spec.ContainerSecurityContext = wantContainer.DeepCopy()

	r, c := newReconciler(fg)
	if _, err := r.Reconcile(context.Background(), ctrl.Request{
		NamespacedName: types.NamespacedName{Name: fg.Name, Namespace: fg.Namespace},
	}); err != nil {
		t.Fatalf("reconcile: %v", err)
	}
	var d appsv1.Deployment
	if err := c.Get(context.Background(), types.NamespacedName{
		Name: "mysql-lion-dc1", Namespace: fg.Namespace,
	}, &d); err != nil {
		t.Fatalf("deployment: %v", err)
	}

	// Pod-level: must not alias fg.Spec.PodSecurityContext.
	podSC := d.Spec.Template.Spec.SecurityContext
	if podSC == nil {
		t.Fatal("pod SC nil; cannot test pointer independence")
	}
	if podSC == fg.Spec.PodSecurityContext {
		t.Errorf("pod SC aliases fg.Spec.PodSecurityContext; DeepCopy missing")
	}

	// Containers: each container's SC must be a unique pointer.
	gotContainers := map[string]*corev1.SecurityContext{}
	for _, ct := range d.Spec.Template.Spec.Containers {
		gotContainers[ct.Name] = ct.SecurityContext
	}
	mysqlSC, sidecarSC := gotContainers["mysql"], gotContainers["sidecar"]
	if mysqlSC == nil || sidecarSC == nil {
		t.Fatalf("mysql or sidecar SC nil: mysql=%v sidecar=%v", mysqlSC, sidecarSC)
	}
	if mysqlSC == sidecarSC {
		t.Errorf("mysql and sidecar containers share the same *SecurityContext pointer; DeepCopy missing")
	}
	if mysqlSC == fg.Spec.ContainerSecurityContext {
		t.Errorf("mysql container SC aliases fg.Spec.ContainerSecurityContext; DeepCopy missing")
	}
	if sidecarSC == fg.Spec.ContainerSecurityContext {
		t.Errorf("sidecar container SC aliases fg.Spec.ContainerSecurityContext; DeepCopy missing")
	}

	// Init container SC pointer (F5 wiring) must also be independent.
	if len(d.Spec.Template.Spec.InitContainers) == 0 {
		t.Fatal("no init container rendered")
	}
	initSC := d.Spec.Template.Spec.InitContainers[0].SecurityContext
	if initSC == nil {
		t.Fatal("init container SC nil; expected mirror of ContainerSecurityContext")
	}
	if initSC == fg.Spec.ContainerSecurityContext || initSC == mysqlSC || initSC == sidecarSC {
		t.Errorf("init container SC aliases another container's pointer; DeepCopy missing")
	}
}

// --- F5: Operator-injected init container picks up SC + default Resources

// TestReconcileDeployment_InitContainerInheritsSecurityContextAndResources
// verifies the operator-injected `init` container (cp config-map) gets:
//   - the same user-supplied container SecurityContext when set
//   - nil SC when the user has not opted in (backward compat)
//   - default Resources unconditionally (so LimitRange admission passes)
//
// Without this, opting in to Restricted PSS would fail admission because
// the init container would lack drop-ALL / runAsNonRoot / seccomp.
func TestReconcileDeployment_InitContainerInheritsSecurityContextAndResources(t *testing.T) {
	// Case 1: nil ContainerSecurityContext -> init SC stays nil.
	t.Run("nil-keeps-nil", func(t *testing.T) {
		fg := newTestFG()
		r, c := newReconciler(fg)
		if _, err := r.Reconcile(context.Background(), ctrl.Request{
			NamespacedName: types.NamespacedName{Name: fg.Name, Namespace: fg.Namespace},
		}); err != nil {
			t.Fatalf("reconcile: %v", err)
		}
		var d appsv1.Deployment
		if err := c.Get(context.Background(), types.NamespacedName{
			Name: "mysql-lion-dc1", Namespace: fg.Namespace,
		}, &d); err != nil {
			t.Fatalf("deployment: %v", err)
		}
		if len(d.Spec.Template.Spec.InitContainers) == 0 {
			t.Fatal("no init containers rendered")
		}
		ic := d.Spec.Template.Spec.InitContainers[0]
		if ic.Name != "init" {
			t.Fatalf("expected operator init container first; got %q", ic.Name)
		}
		if ic.SecurityContext != nil {
			t.Errorf("init container SC must stay nil when fg has no ContainerSecurityContext; got %+v", ic.SecurityContext)
		}
		// Resources are unconditional (matches defaultInitContainerResources).
		if g, w := ic.Resources.Requests.Cpu().String(), "100m"; g != w {
			t.Errorf("init container cpu request: got %q want %q", g, w)
		}
		if g, w := ic.Resources.Requests.Memory().String(), "128Mi"; g != w {
			t.Errorf("init container memory request: got %q want %q", g, w)
		}
	})

	// Case 2: non-nil ContainerSecurityContext -> init container picks it up.
	t.Run("set-flows-through", func(t *testing.T) {
		t1 := true
		f1 := false
		uid := int64(999)
		wantContainer := &corev1.SecurityContext{
			RunAsNonRoot:             &t1,
			RunAsUser:                &uid,
			AllowPrivilegeEscalation: &f1,
			Capabilities: &corev1.Capabilities{
				Drop: []corev1.Capability{"ALL"},
			},
		}
		fg := newTestFG()
		fg.Spec.ContainerSecurityContext = wantContainer.DeepCopy()
		r, c := newReconciler(fg)
		if _, err := r.Reconcile(context.Background(), ctrl.Request{
			NamespacedName: types.NamespacedName{Name: fg.Name, Namespace: fg.Namespace},
		}); err != nil {
			t.Fatalf("reconcile: %v", err)
		}
		var d appsv1.Deployment
		if err := c.Get(context.Background(), types.NamespacedName{
			Name: "mysql-lion-dc1", Namespace: fg.Namespace,
		}, &d); err != nil {
			t.Fatalf("deployment: %v", err)
		}
		if len(d.Spec.Template.Spec.InitContainers) == 0 {
			t.Fatal("no init containers rendered")
		}
		ic := d.Spec.Template.Spec.InitContainers[0]
		if ic.Name != "init" {
			t.Fatalf("expected operator init container first; got %q", ic.Name)
		}
		if !equality.Semantic.DeepEqual(ic.SecurityContext, wantContainer) {
			t.Errorf("init container SC must match ContainerSecurityContext verbatim\nwant: %+v\n got: %+v", wantContainer, ic.SecurityContext)
		}
	})
}
