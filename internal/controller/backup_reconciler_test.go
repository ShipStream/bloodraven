package controller

import (
	"context"
	"strings"
	"testing"
	"time"

	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/tools/record"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	v1alpha1 "github.com/shipstream/bloodraven/api/v1alpha1"
)

// --- selectSourceSite ----------------------------------------------------

func backupTestGroup(status *v1alpha1.MysqlFailoverGroupStatus) *v1alpha1.MysqlFailoverGroup {
	return &v1alpha1.MysqlFailoverGroup{
		Spec:   v1alpha1.MysqlFailoverGroupSpec{Sites: []v1alpha1.SiteSpec{{Name: "iad"}, {Name: "pdx"}}},
		Status: *status,
	}
}

func TestSelectSourceSite_ReplicaHealthy_PicksReplica(t *testing.T) {
	lag := int64(30)
	status := &v1alpha1.MysqlFailoverGroupStatus{
		ActiveSite: "iad",
		Sites: []v1alpha1.SiteStatus{
			{Name: "iad", State: "writable"},
			{Name: "pdx", State: "read-only", Replicating: true, SecondsBehindSource: &lag},
		},
	}
	site, reason, err := selectSourceSite(backupTestGroup(status), "", 300)
	if err != nil {
		t.Fatalf("unexpected err: %v", err)
	}
	if site != "pdx" {
		t.Errorf("want pdx, got %s", site)
	}
	if reason != "replica-preferred" {
		t.Errorf("want reason replica-preferred, got %s", reason)
	}
}

func TestSelectSourceSite_ReplicaLagging_FallsBackToPrimary(t *testing.T) {
	lag := int64(1000)
	status := &v1alpha1.MysqlFailoverGroupStatus{
		ActiveSite: "iad",
		Sites: []v1alpha1.SiteStatus{
			{Name: "iad", State: "writable"},
			{Name: "pdx", State: "read-only", Replicating: true, SecondsBehindSource: &lag},
		},
	}
	site, reason, err := selectSourceSite(backupTestGroup(status), "", 300)
	if err != nil {
		t.Fatalf("unexpected err: %v", err)
	}
	if site != "iad" {
		t.Errorf("want iad, got %s", site)
	}
	if reason != "primary-fallback" {
		t.Errorf("want reason primary-fallback, got %s", reason)
	}
}

func TestSelectSourceSite_ReplicaUnreachable_FallsBackToPrimary(t *testing.T) {
	status := &v1alpha1.MysqlFailoverGroupStatus{
		ActiveSite: "iad",
		Sites: []v1alpha1.SiteStatus{
			{Name: "iad", State: "writable"},
			{Name: "pdx", State: "unreachable"},
		},
	}
	site, _, err := selectSourceSite(backupTestGroup(status), "", 300)
	if err != nil || site != "iad" {
		t.Fatalf("want iad, got site=%s err=%v", site, err)
	}
}

func TestSelectSourceSite_NoHealthySource_Error(t *testing.T) {
	status := &v1alpha1.MysqlFailoverGroupStatus{
		ActiveSite: "iad",
		Sites: []v1alpha1.SiteStatus{
			{Name: "iad", State: "unreachable"},
			{Name: "pdx", State: "unreachable"},
		},
	}
	if _, _, err := selectSourceSite(backupTestGroup(status), "", 300); err == nil {
		t.Fatal("expected error when both sites unhealthy")
	}
}

func TestSelectSourceSite_OverrideWins(t *testing.T) {
	status := &v1alpha1.MysqlFailoverGroupStatus{
		ActiveSite: "iad",
		Sites: []v1alpha1.SiteStatus{
			{Name: "iad", State: "writable"},
			{Name: "pdx", State: "unreachable"},
		},
	}
	site, reason, err := selectSourceSite(backupTestGroup(status), "pdx", 300)
	if err != nil {
		t.Fatalf("unexpected err: %v", err)
	}
	if site != "pdx" || reason != "override" {
		t.Errorf("got site=%s reason=%s", site, reason)
	}
}

func TestSelectSourceSite_OverrideUnknown_Error(t *testing.T) {
	status := &v1alpha1.MysqlFailoverGroupStatus{
		ActiveSite: "iad",
		Sites: []v1alpha1.SiteStatus{
			{Name: "iad", State: "writable"},
			{Name: "pdx", State: "read-only"},
		},
	}
	if _, _, err := selectSourceSite(backupTestGroup(status), "nope", 300); err == nil {
		t.Fatal("expected error for unknown override site")
	}
}

func TestSelectSourceSite_ExcludesReadOnlyRole(t *testing.T) {
	lag := int64(0)
	fg := &v1alpha1.MysqlFailoverGroup{
		Spec: v1alpha1.MysqlFailoverGroupSpec{Sites: []v1alpha1.SiteSpec{
			{Name: "iad"}, {Name: "pdx", Role: v1alpha1.SiteRoleDROnly}, {Name: "reader", Role: v1alpha1.SiteRoleReadOnly},
		}},
		Status: v1alpha1.MysqlFailoverGroupStatus{ActiveSite: "iad", Sites: []v1alpha1.SiteStatus{
			{Name: "iad", State: "writable"},
			{Name: "reader", State: "read-only", Replicating: true, SecondsBehindSource: &lag},
			{Name: "pdx", State: "read-only", Replicating: true, SecondsBehindSource: &lag},
		}},
	}
	site, _, err := selectSourceSite(fg, "", 300)
	if err != nil || site != "pdx" {
		t.Fatalf("selected %q, err=%v", site, err)
	}
	if _, _, err := selectSourceSite(fg, "reader", 300); err == nil {
		t.Fatal("explicit reader backup source must be rejected")
	}
}

// --- BuildBackupJob ------------------------------------------------------

func fgWithBackup() *v1alpha1.MysqlFailoverGroup {
	t := true
	return &v1alpha1.MysqlFailoverGroup{
		ObjectMeta: metav1.ObjectMeta{Name: "lion", Namespace: "ns"},
		Spec: v1alpha1.MysqlFailoverGroupSpec{
			Image:      "mysql:9.7",
			SecretName: "mysql-credentials",
			Sites: []v1alpha1.SiteSpec{
				{Name: "iad", Zone: "az-iad", LBIP: "10.0.0.1",
					Storage: v1alpha1.StorageSpec{StorageClassName: "gp3", Size: resource.MustParse("10Gi")}},
				{Name: "pdx", Zone: "az-pdx", LBIP: "10.0.0.2",
					Storage: v1alpha1.StorageSpec{StorageClassName: "gp3", Size: resource.MustParse("10Gi")}},
			},
			DNS: v1alpha1.DNSSpec{Hostname: "lion.example.com", TTL: 60},
			Backup: &v1alpha1.BackupSpec{
				Image:                  "mysql/mysql-shell:8.0.34",
				MaxLagSecondsForSource: 60,
				ActiveDeadlineSeconds:  3600,
				BackoffLimit:           1,
				Profiles: []v1alpha1.BackupProfile{
					{
						Name: "nightly-s3",
						Storage: v1alpha1.BackupStorage{
							Type: v1alpha1.BackupStorageS3,
							S3: &v1alpha1.S3Storage{
								Bucket:            "bloodraven-backups",
								Prefix:            "lion",
								Region:            "us-east-1",
								EndpointURL:       "https://minio.local",
								CredentialsSecret: "s3-creds",
							},
						},
						Dump: &v1alpha1.DumpOptions{
							Threads:       8,
							BytesPerChunk: "128M",
							Compression:   "zstd",
							Consistent:    &t,
						},
						Retention: 5,
					},
					{
						Name: "daily-local",
						Storage: v1alpha1.BackupStorage{
							Type: v1alpha1.BackupStoragePVC,
							PVC: &v1alpha1.PVCStorage{
								StorageClassName: "fast",
								Size:             resource.MustParse("50Gi"),
							},
						},
					},
				},
				Schedules: []v1alpha1.BackupSchedule{
					{Name: "nightly", ProfileName: "nightly-s3", Schedule: "0 2 * * *"},
				},
			},
		},
	}
}

func TestBuildBackupJob_S3_EnvAndConfig(t *testing.T) {
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
		CredsSecretName:      "mysqlbackup-lion-nightly-abc-creds",
		ScriptsConfigMapName: "mysql-lion-backup-scripts",
	})
	if err != nil {
		t.Fatalf("BuildBackupJob: %v", err)
	}
	if job.Spec.Template.Spec.RestartPolicy != corev1.RestartPolicyNever {
		t.Errorf("want RestartPolicy=Never, got %s", job.Spec.Template.Spec.RestartPolicy)
	}
	if len(job.Spec.Template.Spec.Containers) != 1 {
		t.Fatalf("want 1 container, got %d", len(job.Spec.Template.Spec.Containers))
	}
	c := job.Spec.Template.Spec.Containers[0]
	if c.Image != "mysql/mysql-shell:8.0.34" {
		t.Errorf("want image mysql/mysql-shell:8.0.34, got %s", c.Image)
	}
	envMap := map[string]string{}
	for _, e := range c.Env {
		envMap[e.Name] = e.Value
	}
	if h := envMap["BLOODRAVEN_MYSQL_HOST"]; !strings.Contains(h, "mysql-lion-pdx-internal.ns.svc.cluster.local:3306") {
		t.Errorf("bad host env: %q", h)
	}
	if envMap["BLOODRAVEN_S3_BUCKET"] != "bloodraven-backups" {
		t.Errorf("want bucket env; got %v", envMap)
	}
	if envMap["BLOODRAVEN_S3_ENDPOINT_OVERRIDE"] != "https://minio.local" {
		t.Errorf("want endpoint override env; got %v", envMap)
	}
	if envMap["AWS_REGION"] != "us-east-1" {
		t.Errorf("want region env; got %v", envMap)
	}
	if !strings.HasPrefix(envMap["BLOODRAVEN_OUTPUT_URL"], "lion/lion-nightly-abc") {
		t.Errorf("bad output url: %q", envMap["BLOODRAVEN_OUTPUT_URL"])
	}
	if envMap["BLOODRAVEN_DUMP_OPTIONS"] == "" || !strings.Contains(envMap["BLOODRAVEN_DUMP_OPTIONS"], `"threads":8`) {
		t.Errorf("dump options not encoded: %q", envMap["BLOODRAVEN_DUMP_OPTIONS"])
	}

	// Creds are now mounted as files, not injected via envFrom.
	if len(c.EnvFrom) != 0 {
		t.Errorf("want 0 envFrom entries (creds are files now), got %d", len(c.EnvFrom))
	}
	if envMap["BLOODRAVEN_MYSQL_CREDS_DIR"] != backupCredsMountPath {
		t.Errorf("want BLOODRAVEN_MYSQL_CREDS_DIR=%s, got %q", backupCredsMountPath, envMap["BLOODRAVEN_MYSQL_CREDS_DIR"])
	}
	if envMap["BLOODRAVEN_AWS_CREDS_DIR"] != backupAWSCredsMountPath {
		t.Errorf("want BLOODRAVEN_AWS_CREDS_DIR=%s, got %q", backupAWSCredsMountPath, envMap["BLOODRAVEN_AWS_CREDS_DIR"])
	}
	if envMap["BLOODRAVEN_STORAGE_TYPE"] != string(v1alpha1.BackupStorageS3) {
		t.Errorf("want BLOODRAVEN_STORAGE_TYPE=S3, got %q", envMap["BLOODRAVEN_STORAGE_TYPE"])
	}

	// Both creds volumes must be mounted at the expected paths.
	mountByName := map[string]corev1.VolumeMount{}
	for _, m := range c.VolumeMounts {
		mountByName[m.Name] = m
	}
	if m, ok := mountByName["mysql-creds"]; !ok || m.MountPath != backupCredsMountPath {
		t.Errorf("mysql-creds not mounted at %s: %+v", backupCredsMountPath, m)
	}
	if m, ok := mountByName["aws-creds"]; !ok || m.MountPath != backupAWSCredsMountPath {
		t.Errorf("aws-creds not mounted at %s: %+v", backupAWSCredsMountPath, m)
	}
	if m, ok := mountByName["scripts"]; !ok || m.MountPath != backupScriptsMountPath {
		t.Errorf("scripts ConfigMap not mounted at %s: %+v", backupScriptsMountPath, m)
	}

	// Hardened SecurityContext defaults.
	podSC := job.Spec.Template.Spec.SecurityContext
	if podSC == nil || podSC.RunAsNonRoot == nil || !*podSC.RunAsNonRoot {
		t.Errorf("pod SecurityContext.RunAsNonRoot should be true: %+v", podSC)
	}
	cSC := c.SecurityContext
	if cSC == nil || cSC.AllowPrivilegeEscalation == nil || *cSC.AllowPrivilegeEscalation {
		t.Errorf("container SecurityContext.AllowPrivilegeEscalation should be false: %+v", cSC)
	}
	if cSC == nil || cSC.ReadOnlyRootFilesystem == nil || !*cSC.ReadOnlyRootFilesystem {
		t.Errorf("container SecurityContext.ReadOnlyRootFilesystem should be true: %+v", cSC)
	}
}

func TestBuildBackupJob_PVC_MountsClaim(t *testing.T) {
	fg := fgWithBackup()
	mb := &v1alpha1.MysqlBackup{
		ObjectMeta: metav1.ObjectMeta{Name: "lion-daily-xyz", Namespace: "ns"},
		Spec:       v1alpha1.MysqlBackupSpec{FailoverGroupRef: v1alpha1.LocalGroupRef{Name: "lion"}, ProfileName: "daily-local"},
	}
	job, err := BuildBackupJob(BackupJobInputs{
		FailoverGroup:        fg,
		Profile:              fg.Spec.Backup.Profiles[1],
		Backup:               mb,
		SourceSite:           "iad",
		CredsSecretName:      "mysqlbackup-lion-daily-xyz-creds",
		ScriptsConfigMapName: "mysql-lion-backup-scripts",
	})
	if err != nil {
		t.Fatalf("BuildBackupJob: %v", err)
	}
	c := job.Spec.Template.Spec.Containers[0]
	var outputURL string
	for _, e := range c.Env {
		if e.Name == "BLOODRAVEN_OUTPUT_URL" {
			outputURL = e.Value
		}
	}
	if !strings.HasPrefix(outputURL, backupPVCMountPath+"/lion-daily-xyz") {
		t.Errorf("expected PVC output url, got %q", outputURL)
	}

	var hasBackupVolume, hasAWSVolume, hasMysqlCreds bool
	for _, v := range job.Spec.Template.Spec.Volumes {
		switch v.Name {
		case "backups":
			if v.PersistentVolumeClaim == nil || v.PersistentVolumeClaim.ClaimName != "mysql-lion-backup-daily-local" {
				t.Errorf("want owned claim name, got %+v", v.PersistentVolumeClaim)
			}
			hasBackupVolume = true
		case "aws-creds":
			hasAWSVolume = true
		case "mysql-creds":
			hasMysqlCreds = true
		}
	}
	if !hasBackupVolume {
		t.Errorf("PVC volume not attached")
	}
	if hasAWSVolume {
		t.Errorf("PVC profile must not mount aws-creds")
	}
	if !hasMysqlCreds {
		t.Errorf("mysql-creds volume must be mounted for PVC profile")
	}

	// No envFrom at all — creds are file-mounted.
	if len(c.EnvFrom) != 0 {
		t.Errorf("want 0 envFrom entries (creds are files now), got %d", len(c.EnvFrom))
	}
}

func TestMarshalDumpOptions_DefaultEmpty(t *testing.T) {
	got, err := marshalDumpOptions(nil)
	if err != nil || got != "{}" {
		t.Errorf("got %q err %v", got, err)
	}
}

func TestMarshalLoadOptions_DefaultEmpty(t *testing.T) {
	got, err := marshalLoadOptions(nil)
	if err != nil || got != "{}" {
		t.Errorf("got %q err %v", got, err)
	}
}

func TestMarshalLoadOptions_IncludeSchemas(t *testing.T) {
	l := &v1alpha1.LoadOptions{
		IncludeSchemas: []string{"tenant_42"},
	}
	got, err := marshalLoadOptions(l)
	if err != nil {
		t.Fatalf("unexpected err: %v", err)
	}
	if !strings.Contains(got, `"includeSchemas":["tenant_42"]`) {
		t.Errorf("want includeSchemas in output, got %q", got)
	}
}

func TestMarshalLoadOptions_ExcludeSchemas(t *testing.T) {
	l := &v1alpha1.LoadOptions{
		ExcludeSchemas: []string{"tmp", "scratch"},
	}
	got, err := marshalLoadOptions(l)
	if err != nil {
		t.Fatalf("unexpected err: %v", err)
	}
	if !strings.Contains(got, `"excludeSchemas":["tmp","scratch"]`) {
		t.Errorf("want excludeSchemas in output, got %q", got)
	}
}

func TestMarshalLoadOptions_AllFields(t *testing.T) {
	reset := true
	skipBin := false
	loadIdx := true
	l := &v1alpha1.LoadOptions{
		Threads:        8,
		ResetProgress:  &reset,
		SkipBinlog:     &skipBin,
		LoadIndexes:    &loadIdx,
		IncludeSchemas: []string{"orders"},
	}
	got, err := marshalLoadOptions(l)
	if err != nil {
		t.Fatalf("unexpected err: %v", err)
	}
	for _, want := range []string{
		`"threads":8`,
		`"resetProgress":true`,
		`"skipBinlog":false`,
		`"loadIndexes":true`,
		`"includeSchemas":["orders"]`,
	} {
		if !strings.Contains(got, want) {
			t.Errorf("want %s in output, got %q", want, got)
		}
	}
}

// --- jobPhase: failed-counter fallback + kind parameterization ----------

func TestJobPhase_UsesKindString(t *testing.T) {
	j := &batchv1.Job{
		Status: batchv1.JobStatus{
			Conditions: []batchv1.JobCondition{{Type: batchv1.JobComplete, Status: corev1.ConditionTrue}},
		},
	}
	if _, msg := jobPhase(j, "backup"); !strings.Contains(msg, "backup completed") {
		t.Errorf("backup kind: got %q", msg)
	}
	if _, msg := jobPhase(j, "restore"); !strings.Contains(msg, "restore completed") {
		t.Errorf("restore kind: got %q", msg)
	}
}

func TestJobPhase_FailedCounterFallback(t *testing.T) {
	limit := int32(2)
	j := &batchv1.Job{
		Spec:   batchv1.JobSpec{BackoffLimit: &limit},
		Status: batchv1.JobStatus{Failed: 3}, // past the limit, no conditions yet
	}
	phase, msg := jobPhase(j, "backup")
	if phase != v1alpha1.BackupPhaseFailed {
		t.Errorf("want Failed, got %s (msg=%s)", phase, msg)
	}
	if !strings.Contains(msg, "backoffLimit=2") {
		t.Errorf("want backoff limit in msg, got %q", msg)
	}
}

func TestJobPhase_FailedBelowLimit_StillRunning(t *testing.T) {
	limit := int32(3)
	j := &batchv1.Job{
		Spec:   batchv1.JobSpec{BackoffLimit: &limit},
		Status: batchv1.JobStatus{Failed: 1},
	}
	phase, _ := jobPhase(j, "backup")
	if phase != "" {
		t.Errorf("want empty (still running), got %s", phase)
	}
}

// --- truncateDNS1123 ----------------------------------------------------

func TestTruncateDNS1123_ShortNameUnchanged(t *testing.T) {
	if got := truncateDNS1123("mysql-lion-daily"); got != "mysql-lion-daily" {
		t.Errorf("want unchanged, got %q", got)
	}
}

func TestTruncateDNS1123_TrimsTrailingDash(t *testing.T) {
	if got := truncateDNS1123("abc-"); got != "abc" {
		t.Errorf("want 'abc', got %q", got)
	}
}

func TestTruncateDNS1123_LongNameTruncatedWithHash(t *testing.T) {
	raw := strings.Repeat("a", 80)
	got := truncateDNS1123(raw)
	if len(got) > 63 {
		t.Errorf("result exceeds 63 chars: %d", len(got))
	}
	if !strings.Contains(got, "-") {
		t.Errorf("want hash suffix, got %q", got)
	}
	// Second call with a different name must produce a different result.
	other := truncateDNS1123(strings.Repeat("a", 79) + "b")
	if other == got {
		t.Errorf("different inputs collided: %q", got)
	}
	// Must not end in '-'.
	if strings.HasSuffix(got, "-") {
		t.Errorf("result ends in '-': %q", got)
	}
}

// --- MysqlBackupReconciler end-to-end (fake client) ----------------------

func newBackupReconciler(t *testing.T, objs ...client.Object) (*MysqlBackupReconciler, client.Client) {
	t.Helper()
	scheme := testScheme()
	cb := fake.NewClientBuilder().
		WithScheme(scheme).
		WithStatusSubresource(&v1alpha1.MysqlBackup{}, &v1alpha1.MysqlFailoverGroup{}).
		WithObjects(objs...)
	c := cb.Build()
	r := &MysqlBackupReconciler{
		Client:   c,
		Scheme:   scheme,
		Recorder: record.NewFakeRecorder(10),
	}
	return r, c
}

func backupCR(name, fgName, profile string) *v1alpha1.MysqlBackup {
	return &v1alpha1.MysqlBackup{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: "ns"},
		Spec: v1alpha1.MysqlBackupSpec{
			FailoverGroupRef: v1alpha1.LocalGroupRef{Name: fgName},
			ProfileName:      profile,
		},
	}
}

func dsnSecret() *corev1.Secret {
	return &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{Name: "mysql-credentials", Namespace: "ns"},
		Data:       map[string][]byte{"dsn": []byte("backup:s3cret@tcp(host:3306)/")},
	}
}

func reconcileUntilStable(t *testing.T, r *MysqlBackupReconciler, name string) {
	t.Helper()
	for i := 0; i < 5; i++ {
		_, err := r.Reconcile(context.Background(), ctrl.Request{
			NamespacedName: types.NamespacedName{Name: name, Namespace: "ns"},
		})
		if err != nil {
			t.Fatalf("reconcile #%d failed: %v", i, err)
		}
	}
}

func TestMysqlBackupReconciler_GroupNotFound_TransitionsToFailed(t *testing.T) {
	mb := backupCR("orphan", "ghost", "x")
	r, c := newBackupReconciler(t, mb, dsnSecret())
	reconcileUntilStable(t, r, "orphan")

	var got v1alpha1.MysqlBackup
	if err := c.Get(context.Background(), types.NamespacedName{Name: "orphan", Namespace: "ns"}, &got); err != nil {
		t.Fatalf("get: %v", err)
	}
	if got.Status.Phase != v1alpha1.BackupPhaseFailed {
		t.Errorf("want Failed phase, got %q (message=%q)", got.Status.Phase, got.Status.Message)
	}
}

func TestMysqlBackupReconciler_ProfileNotFound_TransitionsToFailed(t *testing.T) {
	fg := fgWithBackup()
	fg.Status = v1alpha1.MysqlFailoverGroupStatus{
		ActiveSite: "iad",
		Sites: []v1alpha1.SiteStatus{
			{Name: "iad", State: "writable"},
			{Name: "pdx", State: "read-only", Replicating: true},
		},
	}
	mb := backupCR("bad-profile", "lion", "does-not-exist")
	r, c := newBackupReconciler(t, fg, mb, dsnSecret())
	reconcileUntilStable(t, r, "bad-profile")

	var got v1alpha1.MysqlBackup
	_ = c.Get(context.Background(), types.NamespacedName{Name: "bad-profile", Namespace: "ns"}, &got)
	if got.Status.Phase != v1alpha1.BackupPhaseFailed {
		t.Errorf("want Failed phase, got %q", got.Status.Phase)
	}
}

func TestMysqlBackupReconciler_CreatesJob_PicksReplica(t *testing.T) {
	fg := fgWithBackup()
	fg.Status = v1alpha1.MysqlFailoverGroupStatus{
		ActiveSite: "iad",
		Sites: []v1alpha1.SiteStatus{
			{Name: "iad", State: "writable"},
			{Name: "pdx", State: "read-only", Replicating: true},
		},
	}
	mb := backupCR("happy-path", "lion", "nightly-s3")
	r, c := newBackupReconciler(t, fg, mb, dsnSecret())
	reconcileUntilStable(t, r, "happy-path")

	var job batchv1.Job
	if err := c.Get(context.Background(), types.NamespacedName{
		Name: backupJobName("happy-path"), Namespace: "ns",
	}, &job); err != nil {
		t.Fatalf("job not created: %v", err)
	}

	var got v1alpha1.MysqlBackup
	_ = c.Get(context.Background(), types.NamespacedName{Name: "happy-path", Namespace: "ns"}, &got)
	if got.Status.Phase != v1alpha1.BackupPhaseRunning {
		t.Errorf("want Running, got %s", got.Status.Phase)
	}
	if got.Status.SourceSite != "pdx" {
		t.Errorf("want source pdx, got %s", got.Status.SourceSite)
	}
	if job.Labels[labelSite] != "pdx" {
		t.Errorf("want Job source-site label pdx, got %q", job.Labels[labelSite])
	}
	// Phase 2: capture the active-site MySQL image tag for
	// version-pinned verification.
	if got.Status.MysqlImage != "mysql:9.7" {
		t.Errorf("want status.mysqlImage=mysql:9.7, got %q", got.Status.MysqlImage)
	}

	// Derived creds Secret should exist.
	var creds corev1.Secret
	if err := c.Get(context.Background(), types.NamespacedName{
		Name: backupCredsSecretName("happy-path"), Namespace: "ns",
	}, &creds); err != nil {
		t.Errorf("creds secret not created: %v", err)
	}
	if string(creds.Data["MYSQL_USER"]) != "backup" {
		t.Errorf("parsed user wrong: %q", creds.Data["MYSQL_USER"])
	}
	if string(creds.Data["MYSQL_PASSWORD"]) != "s3cret" {
		t.Errorf("parsed password wrong: %q", creds.Data["MYSQL_PASSWORD"])
	}
}

func TestMysqlBackupReconciler_NoSourceStaysPendingAndRetries(t *testing.T) {
	fg := fgWithBackup()
	fg.Status = v1alpha1.MysqlFailoverGroupStatus{
		Sites: []v1alpha1.SiteStatus{
			{Name: "iad", State: "read-only"},
			{Name: "pdx", State: "read-only", Replicating: true},
		},
	}
	mb := backupCR("wait-for-source", "lion", "nightly-s3")
	r, c := newBackupReconciler(t, fg, mb, dsnSecret())
	reconcileUntilStable(t, r, "wait-for-source")

	var got v1alpha1.MysqlBackup
	key := types.NamespacedName{Name: "wait-for-source", Namespace: "ns"}
	if err := c.Get(context.Background(), key, &got); err != nil {
		t.Fatalf("get pending backup: %v", err)
	}
	if got.Status.Phase != v1alpha1.BackupPhasePending {
		t.Fatalf("phase=%q, want Pending (message=%q)", got.Status.Phase, got.Status.Message)
	}
	if got.Status.CompletionTime != nil {
		t.Fatalf("pending backup has completionTime=%v", got.Status.CompletionTime)
	}
	var job batchv1.Job
	if err := c.Get(context.Background(), types.NamespacedName{
		Name: backupJobName(got.Name), Namespace: got.Namespace,
	}, &job); !apierrors.IsNotFound(err) {
		t.Fatalf("Job should not exist without a source, get err=%v", err)
	}

	var liveFG v1alpha1.MysqlFailoverGroup
	fgKey := types.NamespacedName{Name: "lion", Namespace: "ns"}
	if err := c.Get(context.Background(), fgKey, &liveFG); err != nil {
		t.Fatalf("get failover group: %v", err)
	}
	liveFG.Status.ActiveSite = "iad"
	liveFG.Status.Sites = []v1alpha1.SiteStatus{
		{Name: "iad", State: "writable"},
		{Name: "pdx", State: "read-only", Replicating: true},
	}
	if err := c.Status().Update(context.Background(), &liveFG); err != nil {
		t.Fatalf("make source healthy: %v", err)
	}
	reconcileUntilStable(t, r, "wait-for-source")

	if err := c.Get(context.Background(), key, &got); err != nil {
		t.Fatalf("get running backup: %v", err)
	}
	if got.Status.Phase != v1alpha1.BackupPhaseRunning {
		t.Fatalf("phase=%q, want Running after source recovery (message=%q)", got.Status.Phase, got.Status.Message)
	}
	if err := c.Get(context.Background(), types.NamespacedName{
		Name: backupJobName(got.Name), Namespace: got.Namespace,
	}, &job); err != nil {
		t.Fatalf("Job not created after source recovery: %v", err)
	}
}

func TestMysqlBackupReconciler_RunningJobSurvivesTransientNoActiveSite(t *testing.T) {
	fg := fgWithBackup()
	fg.Status = v1alpha1.MysqlFailoverGroupStatus{
		ActiveSite: "iad",
		Sites: []v1alpha1.SiteStatus{
			{Name: "iad", State: "writable"},
			{Name: "pdx", State: "read-only", Replicating: true},
		},
	}
	mb := backupCR("in-flight-failover", "lion", "nightly-s3")
	r, c := newBackupReconciler(t, fg, mb, dsnSecret())
	reconcileUntilStable(t, r, "in-flight-failover")

	backupKey := types.NamespacedName{Name: mb.Name, Namespace: mb.Namespace}
	var running v1alpha1.MysqlBackup
	if err := c.Get(context.Background(), backupKey, &running); err != nil {
		t.Fatalf("get running backup: %v", err)
	}
	if running.Status.Phase != v1alpha1.BackupPhaseRunning || running.Status.SourceSite != "pdx" {
		t.Fatalf("initial status phase=%q source=%q, want Running/pdx", running.Status.Phase, running.Status.SourceSite)
	}

	// Reproduce the nightly race: the backup Job already targets pdx, while
	// an ordered update briefly publishes no active site on the group.
	var liveFG v1alpha1.MysqlFailoverGroup
	fgKey := types.NamespacedName{Name: "lion", Namespace: "ns"}
	if err := c.Get(context.Background(), fgKey, &liveFG); err != nil {
		t.Fatalf("get failover group: %v", err)
	}
	liveFG.Status.ActiveSite = ""
	for i := range liveFG.Status.Sites {
		liveFG.Status.Sites[i].State = "read-only"
	}
	if err := c.Status().Update(context.Background(), &liveFG); err != nil {
		t.Fatalf("publish transient no-primary state: %v", err)
	}
	if _, err := r.Reconcile(context.Background(), ctrl.Request{NamespacedName: backupKey}); err != nil {
		t.Fatalf("reconcile during no-primary window: %v", err)
	}
	if err := c.Get(context.Background(), backupKey, &running); err != nil {
		t.Fatalf("get backup after no-primary reconcile: %v", err)
	}
	if running.Status.Phase != v1alpha1.BackupPhaseRunning {
		t.Fatalf("in-flight backup became %q during no-primary window (message=%q)", running.Status.Phase, running.Status.Message)
	}

	// The pinned Job remains authoritative and can complete while the live
	// topology is still between active sites.
	var job batchv1.Job
	jobKey := types.NamespacedName{Name: backupJobName(mb.Name), Namespace: mb.Namespace}
	if err := c.Get(context.Background(), jobKey, &job); err != nil {
		t.Fatalf("get backup Job: %v", err)
	}
	now := metav1.NewTime(time.Now())
	job.Status.Succeeded = 1
	job.Status.Conditions = []batchv1.JobCondition{{
		Type: batchv1.JobComplete, Status: corev1.ConditionTrue,
		LastProbeTime: now, LastTransitionTime: now,
	}}
	if err := c.Status().Update(context.Background(), &job); err != nil {
		t.Fatalf("complete backup Job: %v", err)
	}
	reconcileUntilStable(t, r, mb.Name)

	var done v1alpha1.MysqlBackup
	if err := c.Get(context.Background(), backupKey, &done); err != nil {
		t.Fatalf("get completed backup: %v", err)
	}
	if done.Status.Phase != v1alpha1.BackupPhaseSucceeded {
		t.Fatalf("phase=%q, want Succeeded (message=%q)", done.Status.Phase, done.Status.Message)
	}
	if done.Status.SourceSite != "pdx" {
		t.Fatalf("source=%q, want pinned source pdx", done.Status.SourceSite)
	}
}

func TestMysqlBackupReconciler_JobSucceeded_MarksSucceeded(t *testing.T) {
	fg := fgWithBackup()
	fg.Status = v1alpha1.MysqlFailoverGroupStatus{
		ActiveSite: "iad",
		Sites: []v1alpha1.SiteStatus{
			{Name: "iad", State: "writable"},
			{Name: "pdx", State: "read-only", Replicating: true},
		},
	}
	mb := backupCR("done", "lion", "nightly-s3")
	r, c := newBackupReconciler(t, fg, mb, dsnSecret())

	// First pass: creates Job.
	reconcileUntilStable(t, r, "done")

	// Mutate Job status to simulate completion.
	var job batchv1.Job
	if err := c.Get(context.Background(), types.NamespacedName{
		Name: backupJobName("done"), Namespace: "ns",
	}, &job); err != nil {
		t.Fatalf("get job: %v", err)
	}
	now := metav1.NewTime(time.Now())
	job.Status.Conditions = append(job.Status.Conditions, batchv1.JobCondition{
		Type: batchv1.JobComplete, Status: corev1.ConditionTrue,
		LastProbeTime: now, LastTransitionTime: now,
	})
	job.Status.Succeeded = 1
	if err := c.Status().Update(context.Background(), &job); err != nil {
		t.Fatalf("update job status: %v", err)
	}

	// Second pass: observes success.
	reconcileUntilStable(t, r, "done")

	var got v1alpha1.MysqlBackup
	_ = c.Get(context.Background(), types.NamespacedName{Name: "done", Namespace: "ns"}, &got)
	if got.Status.Phase != v1alpha1.BackupPhaseSucceeded {
		t.Errorf("want Succeeded, got %s (msg=%s)", got.Status.Phase, got.Status.Message)
	}
	if got.Status.CompletionTime == nil {
		t.Errorf("CompletionTime not set")
	}
}

func TestMysqlBackupReconciler_JobFailed_MarksFailed(t *testing.T) {
	fg := fgWithBackup()
	fg.Status = v1alpha1.MysqlFailoverGroupStatus{
		ActiveSite: "iad",
		Sites: []v1alpha1.SiteStatus{
			{Name: "iad", State: "writable"},
			{Name: "pdx", State: "read-only", Replicating: true},
		},
	}
	mb := backupCR("boom", "lion", "nightly-s3")
	r, c := newBackupReconciler(t, fg, mb, dsnSecret())
	reconcileUntilStable(t, r, "boom")

	var job batchv1.Job
	if err := c.Get(context.Background(), types.NamespacedName{
		Name: backupJobName("boom"), Namespace: "ns",
	}, &job); err != nil {
		t.Fatalf("get job: %v", err)
	}
	now := metav1.NewTime(time.Now())
	job.Status.Conditions = []batchv1.JobCondition{{
		Type: batchv1.JobFailed, Status: corev1.ConditionTrue,
		Reason: "BackoffLimitExceeded", Message: "dump hit BackoffLimit",
		LastProbeTime: now, LastTransitionTime: now,
	}}
	if err := c.Status().Update(context.Background(), &job); err != nil {
		t.Fatalf("update job status: %v", err)
	}

	reconcileUntilStable(t, r, "boom")

	var got v1alpha1.MysqlBackup
	_ = c.Get(context.Background(), types.NamespacedName{Name: "boom", Namespace: "ns"}, &got)
	if got.Status.Phase != v1alpha1.BackupPhaseFailed {
		t.Errorf("want Failed, got %s", got.Status.Phase)
	}
	if !strings.Contains(got.Status.Message, "BackoffLimit") {
		t.Errorf("want failure message, got %q", got.Status.Message)
	}
}
