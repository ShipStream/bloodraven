package controller

import (
	"context"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
	dto "github.com/prometheus/client_model/go"
	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"

	v1alpha1 "github.com/shipstream/bloodraven/api/v1alpha1"
	bmetrics "github.com/shipstream/bloodraven/internal/metrics"
)

func TestParseRestoreCompleteLine(t *testing.T) {
	line := "BLOODRAVEN_RESTORE_COMPLETE sourceSizeBytes=123 sourceGtidExecuted=uuid:1-5 " +
		"sourceBinlogFile=mysql_bin.000001 sourceBinlogPos=44 targetGtidExecuted=uuid%3A1-9%2Cuuid_b%3A1-2 " +
		"targetBinlogFile=mysql_bin.000002 targetBinlogPos=88 pitrStopDatetime=2026-04-15%2009%3A30%3A00 " +
		"pitrReplayedBinlogFile=site_a%2Fmysql_bin.000003 pitrReplayedBinlogCount=7"
	meta, ok := parseRestoreCompleteLine(line)
	if !ok {
		t.Fatal("expected sentinel match")
	}
	if meta.SourceSizeBytes != 123 || meta.SourceBinlogPos != 44 || meta.TargetBinlogPos != 88 || meta.PitrReplayedBinlogCount != 7 {
		t.Fatalf("numeric fields not parsed: %+v", meta)
	}
	if meta.PitrStopDatetime != "2026-04-15 09:30:00" {
		t.Errorf("pitrStopDatetime = %q", meta.PitrStopDatetime)
	}
	if meta.SourceBinlogFile != "mysql_bin.000001" || meta.TargetGtidExecuted != "uuid:1-9,uuid_b:1-2" || meta.PitrReplayedBinlogFile != "site_a/mysql_bin.000003" {
		t.Errorf("string fields not parsed: %+v", meta)
	}
}

func TestParseRestoreCompleteLine_ReversibleEncodingRoundTrip(t *testing.T) {
	line := "BLOODRAVEN_RESTORE_COMPLETE targetBinlogFile=mysql_bin%20with%20space%2525 " +
		"pitrReplayedBinlogFile=site_a%2Fmysql_bin_000003 pitrStopDatetime=2026-04-15%2009%3A30%3A00"
	meta, ok := parseRestoreCompleteLine(line)
	if !ok {
		t.Fatal("expected sentinel match")
	}
	if meta.TargetBinlogFile != "mysql_bin with space%25" {
		t.Fatalf("targetBinlogFile = %q", meta.TargetBinlogFile)
	}
	if meta.PitrReplayedBinlogFile != "site_a/mysql_bin_000003" {
		t.Fatalf("pitrReplayedBinlogFile = %q", meta.PitrReplayedBinlogFile)
	}
}

func TestParseRestoreCompleteLine_MismatchAndMalformedInts(t *testing.T) {
	if _, ok := parseRestoreCompleteLine("BLOODRAVEN_LOAD_COMPLETE input=x"); ok {
		t.Fatal("non-restore sentinel matched")
	}
	meta, ok := parseRestoreCompleteLine("BLOODRAVEN_RESTORE_COMPLETE sourceSizeBytes=bad targetBinlogPos=nope pitrReplayedBinlogCount=x")
	if !ok {
		t.Fatal("expected restore sentinel match")
	}
	if meta.SourceSizeBytes != 0 || meta.TargetBinlogPos != 0 || meta.PitrReplayedBinlogCount != 0 {
		t.Fatalf("malformed ints should stay zero: %+v", meta)
	}
}

func TestBuildRestoreJob_AddsSourceMetadataEnv(t *testing.T) {
	fg := fgInitFromMysqlBackup()
	seed := succeededSeedBackup()
	seed.Status.SizeBytes = 1024
	seed.Status.GtidExecuted = "uuid:1-10"
	seed.Status.BinlogFile = "mysql-bin.000004"
	seed.Status.BinlogPos = 99
	r, _ := newReconciler(fg, seed)

	job, err := r.buildRestoreJob(context.Background(), fg, fg.Spec.Sites[0].Name, "creds")
	if err != nil {
		t.Fatalf("buildRestoreJob: %v", err)
	}
	env := envMap(job.Spec.Template.Spec.Containers[0].Env)
	if env["BLOODRAVEN_SOURCE_SIZE_BYTES"] != "1024" || env["BLOODRAVEN_SOURCE_GTID_EXECUTED"] != "uuid:1-10" ||
		env["BLOODRAVEN_SOURCE_BINLOG_FILE"] != "mysql-bin.000004" || env["BLOODRAVEN_SOURCE_BINLOG_POS"] != "99" {
		t.Fatalf("source metadata env not set: %v", env)
	}
}

func TestPodBelongsToJobMatchesExactJob(t *testing.T) {
	job := &batchv1.Job{ObjectMeta: metav1.ObjectMeta{Name: "restore-new", UID: types.UID("new-uid")}}
	old := &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Labels: map[string]string{"batch.kubernetes.io/job-name": "restore-old"}}}
	byModernLabel := &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Labels: map[string]string{"batch.kubernetes.io/job-name": "restore-new"}}}
	byLegacyLabel := &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Labels: map[string]string{"job-name": "restore-new"}}}
	byOwner := &corev1.Pod{ObjectMeta: metav1.ObjectMeta{OwnerReferences: []metav1.OwnerReference{{UID: types.UID("new-uid")}}}}
	if podBelongsToJob(old, job) {
		t.Fatal("old job pod matched")
	}
	for name, pod := range map[string]*corev1.Pod{"modern": byModernLabel, "legacy": byLegacyLabel, "owner": byOwner} {
		if !podBelongsToJob(pod, job) {
			t.Fatalf("%s pod did not match job", name)
		}
	}
}

func TestReconcileRestoreJob_JobSucceeded_EmitsMetricsOnce(t *testing.T) {
	fggroup := "lion-metrics-init"
	fg := fgInitFromMysqlBackup()
	fg.Name = fggroup
	fg.Spec.Sites[0].Name = "iad"
	fg.Spec.Sites[1].Name = "pdx"
	fg.Spec.Backup.Profiles[0].Storage.S3.Prefix = fggroup
	seed := succeededSeedBackup()
	seed.Labels[labelFailoverGroup] = fggroup
	seed.Spec.FailoverGroupRef.Name = fggroup
	seed.Status.Location = fggroup + "/seed/"
	seed.Status.SizeBytes = 2048
	deploy := primaryReadyDeployment(fggroup, fg.Spec.Sites[0].Name)
	r, c := newReconciler(fg, seed, deploy, dsnSecret())
	defer bmetrics.RestoreDurationSeconds.DeleteLabelValues("ns", fggroup, "init_from_backup", "iad")
	defer bmetrics.RestoreLastSuccessTimestamp.DeleteLabelValues("ns", fggroup, "init_from_backup", "iad")
	defer bmetrics.RestoreLastSourceSizeBytes.DeleteLabelValues("ns", fggroup, "init_from_backup", "iad")

	if _, err := r.reconcileRestoreJob(context.Background(), fg); err != nil {
		t.Fatalf("first pass: %v", err)
	}
	var job batchv1.Job
	if err := c.Get(context.Background(), types.NamespacedName{Name: restoreJobName(fggroup, "iad"), Namespace: "ns"}, &job); err != nil {
		t.Fatalf("get job: %v", err)
	}
	start := metav1.NewTime(time.Now().Add(-2 * time.Minute))
	job.Status.StartTime = &start
	job.Status.Conditions = []batchv1.JobCondition{{Type: batchv1.JobComplete, Status: corev1.ConditionTrue}}
	job.Status.Succeeded = 1
	if err := c.Status().Update(context.Background(), &job); err != nil {
		t.Fatalf("update job: %v", err)
	}
	var fresh v1alpha1.MysqlFailoverGroup
	if err := c.Get(context.Background(), types.NamespacedName{Name: fggroup, Namespace: "ns"}, &fresh); err != nil {
		t.Fatalf("get fg: %v", err)
	}
	if _, err := r.reconcileRestoreJob(context.Background(), &fresh); err != nil {
		t.Fatalf("second pass: %v", err)
	}
	if got := testutil.ToFloat64(bmetrics.RestoreLastSourceSizeBytes.WithLabelValues("ns", fggroup, "init_from_backup", "iad")); got != 2048 {
		t.Fatalf("source size metric = %v", got)
	}
	if got := testutil.ToFloat64(bmetrics.RestoreLastSuccessTimestamp.WithLabelValues("ns", fggroup, "init_from_backup", "iad")); got <= 0 {
		t.Fatalf("last success timestamp not set: %v", got)
	}
	count, sum := restoreHistogramStats(t, "ns", fggroup, "init_from_backup", "iad")
	if count != 1 {
		t.Fatalf("duration observations = %d, want 1", count)
	}
	if sum < 110 || sum > 130 {
		t.Fatalf("duration sum = %v, want about 120s", sum)
	}

	var terminal v1alpha1.MysqlFailoverGroup
	if err := c.Get(context.Background(), types.NamespacedName{Name: fggroup, Namespace: "ns"}, &terminal); err != nil {
		t.Fatalf("get terminal fg: %v", err)
	}
	if d, err := r.reconcileRestoreJob(context.Background(), &terminal); err != nil || d != 0 {
		t.Fatalf("terminal pass = (%s, %v)", d, err)
	}
	afterCount, afterSum := restoreHistogramStats(t, "ns", fggroup, "init_from_backup", "iad")
	if afterCount != count || afterSum != sum {
		t.Fatalf("duration metric changed after terminal reconcile: before=(%d,%v) after=(%d,%v)", count, sum, afterCount, afterSum)
	}
}

func TestReconcileRestoreJob_JobFailed_DoesNotEmitSuccessMetrics(t *testing.T) {
	fggroup := "lion-metrics-failed-init"
	fg := fgInitFromMysqlBackup()
	fg.Name = fggroup
	fg.Spec.Sites[0].Name = "iad"
	fg.Spec.Sites[1].Name = "pdx"
	fg.Spec.Backup.Profiles[0].Storage.S3.Prefix = fggroup
	seed := succeededSeedBackup()
	seed.Labels[labelFailoverGroup] = fggroup
	seed.Spec.FailoverGroupRef.Name = fggroup
	seed.Status.Location = fggroup + "/seed/"
	seed.Status.SizeBytes = 4096
	deploy := primaryReadyDeployment(fggroup, "iad")
	r, c := newReconciler(fg, seed, deploy, dsnSecret())
	defer bmetrics.RestoreDurationSeconds.DeleteLabelValues("ns", fggroup, "init_from_backup", "iad")
	defer bmetrics.RestoreLastSuccessTimestamp.DeleteLabelValues("ns", fggroup, "init_from_backup", "iad")
	defer bmetrics.RestoreLastSourceSizeBytes.DeleteLabelValues("ns", fggroup, "init_from_backup", "iad")

	if _, err := r.reconcileRestoreJob(context.Background(), fg); err != nil {
		t.Fatalf("first pass: %v", err)
	}
	var job batchv1.Job
	if err := c.Get(context.Background(), types.NamespacedName{Name: restoreJobName(fggroup, "iad"), Namespace: "ns"}, &job); err != nil {
		t.Fatalf("get job: %v", err)
	}
	job.Status.StartTime = ptrTime(time.Now().Add(-time.Minute))
	job.Status.Conditions = []batchv1.JobCondition{{Type: batchv1.JobFailed, Status: corev1.ConditionTrue, Reason: "Failed"}}
	job.Status.Failed = 1
	if err := c.Status().Update(context.Background(), &job); err != nil {
		t.Fatalf("update job: %v", err)
	}
	var fresh v1alpha1.MysqlFailoverGroup
	if err := c.Get(context.Background(), types.NamespacedName{Name: fggroup, Namespace: "ns"}, &fresh); err != nil {
		t.Fatalf("get fg: %v", err)
	}
	if _, err := r.reconcileRestoreJob(context.Background(), &fresh); err != nil {
		t.Fatalf("second pass: %v", err)
	}
	if got := testutil.ToFloat64(bmetrics.RestoreLastSuccessTimestamp.WithLabelValues("ns", fggroup, "init_from_backup", "iad")); got != 0 {
		t.Fatalf("last success timestamp = %v, want 0", got)
	}
	if got := testutil.ToFloat64(bmetrics.RestoreLastSourceSizeBytes.WithLabelValues("ns", fggroup, "init_from_backup", "iad")); got != 0 {
		t.Fatalf("source size metric = %v, want 0", got)
	}
	count, sum := restoreHistogramStats(t, "ns", fggroup, "init_from_backup", "iad")
	if count != 0 || sum != 0 {
		t.Fatalf("duration metric = (%d,%v), want zero", count, sum)
	}
}

func TestInPlaceRestorePreservesMetadataThroughResuming(t *testing.T) {
	fg := fgInPlaceRestore("")
	fg.Name = "lion-metrics-inplace"
	fg.Spec.Sites[0].Name = "iad"
	fg.Spec.Sites[1].Name = "pdx"
	fg.Status.ActiveSite = "iad"
	fg.Status.Sites[0].Name = "iad"
	fg.Status.Sites[1].Name = "pdx"
	fg.Status.RestoreInPlace = &v1alpha1.RestoreInPlaceStatus{
		Phase:              v1alpha1.RestoreInPlaceResuming,
		JobName:            "job",
		TargetSite:         "iad",
		Scope:              "full",
		StartTime:          ptrTime(time.Now()),
		SourceSizeBytes:    777,
		TargetGtidExecuted: "uuid:1-7",
	}
	seed := succeededSeedBackup()
	r, c := newReconciler(fg, seed, dsnSecret())

	if _, err := r.reconcileInPlaceRestore(context.Background(), fg); err != nil {
		t.Fatalf("reconcile: %v", err)
	}
	var fresh v1alpha1.MysqlFailoverGroup
	if err := c.Get(context.Background(), types.NamespacedName{Name: fg.Name, Namespace: fg.Namespace}, &fresh); err != nil {
		t.Fatalf("get fg: %v", err)
	}
	if fresh.Status.RestoreInPlace.SourceSizeBytes != 777 || fresh.Status.RestoreInPlace.TargetGtidExecuted != "uuid:1-7" {
		t.Fatalf("metadata not preserved: %+v", fresh.Status.RestoreInPlace)
	}
}

func restoreHistogramStats(t *testing.T, labels ...string) (uint64, float64) {
	t.Helper()
	metric := &dto.Metric{}
	observer := bmetrics.RestoreDurationSeconds.WithLabelValues(labels...)
	promMetric, ok := observer.(prometheus.Metric)
	if !ok {
		t.Fatal("restore duration observer is not a prometheus.Metric")
	}
	if err := promMetric.Write(metric); err != nil {
		t.Fatalf("write histogram metric: %v", err)
	}
	if metric.GetHistogram() == nil {
		return 0, 0
	}
	return metric.GetHistogram().GetSampleCount(), metric.GetHistogram().GetSampleSum()
}
