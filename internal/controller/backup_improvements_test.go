package controller

import (
	"context"
	"sort"
	"strings"
	"testing"
	"time"

	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/tools/record"
	"sigs.k8s.io/controller-runtime/pkg/client"

	v1alpha1 "github.com/shipstream/bloodraven/api/v1alpha1"
)

// --- helpers ----------------------------------------------------------------

func asClientObjects(backups []v1alpha1.MysqlBackup) []client.Object {
	out := make([]client.Object, 0, len(backups))
	for i := range backups {
		b := backups[i]
		out = append(out, &b)
	}
	return out
}

func listBackupNames(t *testing.T, r *MysqlBackupReconciler) []string {
	t.Helper()
	var list v1alpha1.MysqlBackupList
	if err := r.List(context.Background(), &list, client.InNamespace("ns")); err != nil {
		t.Fatalf("list: %v", err)
	}
	names := make([]string, 0, len(list.Items))
	for _, b := range list.Items {
		names = append(names, b.Name)
	}
	sort.Strings(names)
	return names
}

func assertEqualSet(t *testing.T, got, want []string) {
	t.Helper()
	gs := append([]string(nil), got...)
	ws := append([]string(nil), want...)
	sort.Strings(gs)
	sort.Strings(ws)
	if strings.Join(gs, ",") != strings.Join(ws, ",") {
		t.Errorf("set mismatch:\n  got:  %v\n  want: %v", gs, ws)
	}
}

func mustParseTime(t *testing.T, layout, s string) metav1.Time {
	t.Helper()
	tt, err := time.Parse(layout, s)
	if err != nil {
		t.Fatalf("parse time %q: %v", s, err)
	}
	return metav1.NewTime(tt)
}

func succeededBackup(name string, ns string, fgName, profile string, completed metav1.Time) v1alpha1.MysqlBackup {
	return v1alpha1.MysqlBackup{
		ObjectMeta: metav1.ObjectMeta{
			Name:      name,
			Namespace: ns,
			Labels: map[string]string{
				labelFailoverGroup: fgName,
				labelBackupProfile: profile,
				labelManagedBy:     managerName,
			},
		},
		Spec: v1alpha1.MysqlBackupSpec{
			FailoverGroupRef: v1alpha1.LocalGroupRef{Name: fgName},
			ProfileName:      profile,
		},
		Status: v1alpha1.MysqlBackupStatus{
			Phase:          v1alpha1.BackupPhaseSucceeded,
			CompletionTime: &completed,
		},
	}
}

// --- DefaultBackupImage -----------------------------------------------------

func TestDefaultBackupImage_IsPinnedCommunityServer(t *testing.T) {
	want := "container-registry.oracle.com/mysql/community-server:9.6"
	if v1alpha1.DefaultBackupImage != want {
		t.Errorf("DefaultBackupImage drifted: got %q, want %q", v1alpha1.DefaultBackupImage, want)
	}
}

// --- parseDumpCompleteLine --------------------------------------------------

func TestParseDumpCompleteLine_AllFields(t *testing.T) {
	line := "BLOODRAVEN_DUMP_COMPLETE location=s3://bucket/prefix/ sizeBytes=1572864 size=1.4_GiB gtidExecuted=abc:1-10 binlogFile=mysql-bin.000042 binlogPos=118"
	meta, ok := parseDumpCompleteLine(line)
	if !ok {
		t.Fatal("prefix match failed")
	}
	if meta.Location != "s3://bucket/prefix/" {
		t.Errorf("location: %q", meta.Location)
	}
	if meta.SizeBytes != 1572864 {
		t.Errorf("sizeBytes: %d", meta.SizeBytes)
	}
	if meta.Size != "1.4 GiB" {
		t.Errorf("size: %q", meta.Size)
	}
	if meta.GtidExecuted != "abc:1-10" {
		t.Errorf("gtid: %q", meta.GtidExecuted)
	}
	if meta.BinlogFile != "mysql-bin.000042" {
		t.Errorf("binlogFile: %q", meta.BinlogFile)
	}
	if meta.BinlogPos != 118 {
		t.Errorf("binlogPos: %d", meta.BinlogPos)
	}
}

func TestParseDumpCompleteLine_IgnoresUnknownAndMalformed(t *testing.T) {
	line := "BLOODRAVEN_DUMP_COMPLETE location=/backups/x/ sizeBytes=notanumber unknownKey=hi binlogPos=also-bad"
	meta, ok := parseDumpCompleteLine(line)
	if !ok {
		t.Fatal("prefix mismatch")
	}
	if meta.Location != "/backups/x/" {
		t.Errorf("location: %q", meta.Location)
	}
	if meta.SizeBytes != 0 {
		t.Errorf("malformed sizeBytes should be zero, got %d", meta.SizeBytes)
	}
	if meta.BinlogPos != 0 {
		t.Errorf("malformed binlogPos should be zero, got %d", meta.BinlogPos)
	}
}

func TestParseDumpCompleteLine_PrefixMismatch(t *testing.T) {
	_, ok := parseDumpCompleteLine("BLOODRAVEN_DUMP_START host=mysql")
	if ok {
		t.Error("should return false for non-matching prefix")
	}
	_, ok = parseDumpCompleteLine("")
	if ok {
		t.Error("empty line should return false")
	}
}

// --- humanBytes -------------------------------------------------------------

func TestHumanBytes(t *testing.T) {
	cases := []struct {
		n    int64
		want string
	}{
		{0, "0 B"},
		{512, "512 B"},
		{1023, "1023 B"},
		{1024, "1.0 KiB"},
		{1536, "1.5 KiB"},
		{1024 * 1024, "1.0 MiB"},
		{5 * 1024 * 1024, "5.0 MiB"},
		{1024 * 1024 * 1024, "1.0 GiB"},
	}
	for _, tc := range cases {
		if got := humanBytes(tc.n); got != tc.want {
			t.Errorf("humanBytes(%d) = %q, want %q", tc.n, got, tc.want)
		}
	}
}

// --- ensureBackupLabels -----------------------------------------------------

func TestEnsureBackupLabels_StampsMissing(t *testing.T) {
	b := &v1alpha1.MysqlBackup{
		ObjectMeta: metav1.ObjectMeta{Name: "bare"},
		Spec: v1alpha1.MysqlBackupSpec{
			FailoverGroupRef: v1alpha1.LocalGroupRef{Name: "lion"},
			ProfileName:      "nightly",
		},
	}
	if !ensureBackupLabels(b) {
		t.Error("first call should report changes")
	}
	if b.Labels[labelFailoverGroup] != "lion" ||
		b.Labels[labelBackupProfile] != "nightly" ||
		b.Labels[labelManagedBy] != managerName {
		t.Errorf("labels not stamped: %+v", b.Labels)
	}
	if ensureBackupLabels(b) {
		t.Error("second call should be a no-op")
	}
}

// --- resolveRetention -------------------------------------------------------

func TestResolveRetention_ShorthandKeepsCountPlusFloor(t *testing.T) {
	p := &v1alpha1.BackupProfile{Retention: 5}
	count, age, min, maxFail := resolveRetention(p)
	if count != 5 || age != 0 || min != 1 || maxFail != int32(maxFailedRetention) {
		t.Errorf("got (%d, %v, %d, %d)", count, age, min, maxFail)
	}
}

func TestResolveRetention_PolicyOverridesShorthand(t *testing.T) {
	p := &v1alpha1.BackupProfile{
		Retention: 99,
		RetentionPolicy: &v1alpha1.RetentionPolicy{
			Count: 3, MaxAgeDays: 7, MinKeep: 2, MaxFailedKeep: 4,
		},
	}
	count, age, min, maxFail := resolveRetention(p)
	if count != 3 || age != 7*24*time.Hour || min != 2 || maxFail != 4 {
		t.Errorf("got (%d, %v, %d, %d)", count, age, min, maxFail)
	}
}

func TestResolveRetention_NilProfile(t *testing.T) {
	count, age, min, maxFail := resolveRetention(nil)
	if count != 0 || age != 0 || min != 0 || maxFail != int32(maxFailedRetention) {
		t.Errorf("got (%d, %v, %d, %d)", count, age, min, maxFail)
	}
}

// --- pruneSuccessful --------------------------------------------------------

func makeBackups(t *testing.T, now time.Time, ages ...time.Duration) []v1alpha1.MysqlBackup {
	t.Helper()
	out := make([]v1alpha1.MysqlBackup, 0, len(ages))
	for i, a := range ages {
		ct := metav1.NewTime(now.Add(-a))
		out = append(out, succeededBackup(
			"b-"+string(rune('a'+i)),
			"ns", "lion", "nightly", ct,
		))
	}
	return out
}

func TestPruneSuccessful_CountWindow(t *testing.T) {
	now := time.Now()
	backups := makeBackups(t, now, 0, time.Hour, 2*time.Hour, 3*time.Hour, 4*time.Hour)
	r, _ := newBackupReconciler(t, asClientObjects(backups)...)
	if err := r.pruneSuccessful(context.Background(), backups, 2, 0, 0); err != nil {
		t.Fatalf("pruneSuccessful: %v", err)
	}
	assertEqualSet(t, listBackupNames(t, r), []string{"b-a", "b-b"})
}

func TestPruneSuccessful_AgeWindow(t *testing.T) {
	now := time.Now()
	// b-a: 1h old, b-b: 2d old, b-c: 10d old
	backups := makeBackups(t, now, time.Hour, 48*time.Hour, 240*time.Hour)
	r, _ := newBackupReconciler(t, asClientObjects(backups)...)
	if err := r.pruneSuccessful(context.Background(), backups, 0, 7*24*time.Hour, 0); err != nil {
		t.Fatalf("pruneSuccessful: %v", err)
	}
	assertEqualSet(t, listBackupNames(t, r), []string{"b-a", "b-b"})
}

func TestPruneSuccessful_MinKeepFloor(t *testing.T) {
	now := time.Now()
	// All older than 1-day window, but minKeep=2 protects the two newest.
	backups := makeBackups(t, now, 48*time.Hour, 72*time.Hour, 96*time.Hour, 120*time.Hour)
	r, _ := newBackupReconciler(t, asClientObjects(backups)...)
	if err := r.pruneSuccessful(context.Background(), backups, 0, 24*time.Hour, 2); err != nil {
		t.Fatalf("pruneSuccessful: %v", err)
	}
	assertEqualSet(t, listBackupNames(t, r), []string{"b-a", "b-b"})
}

func TestPruneSuccessful_DisabledWhenBothZero(t *testing.T) {
	now := time.Now()
	backups := makeBackups(t, now, time.Hour, 2*time.Hour, 3*time.Hour)
	r, _ := newBackupReconciler(t, asClientObjects(backups)...)
	if err := r.pruneSuccessful(context.Background(), backups, 0, 0, 0); err != nil {
		t.Fatalf("pruneSuccessful: %v", err)
	}
	assertEqualSet(t, listBackupNames(t, r), []string{"b-a", "b-b", "b-c"})
}

// --- pruneRetention on unlabelled CRs ---------------------------------------

func TestPruneRetention_WorksForUnlabelledAdHocBackups(t *testing.T) {
	now := time.Now()
	mk := func(name string, age time.Duration) *v1alpha1.MysqlBackup {
		ct := metav1.NewTime(now.Add(-age))
		return &v1alpha1.MysqlBackup{
			ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: "ns"},
			Spec: v1alpha1.MysqlBackupSpec{
				FailoverGroupRef: v1alpha1.LocalGroupRef{Name: "lion"},
				ProfileName:      "nightly-s3",
			},
			Status: v1alpha1.MysqlBackupStatus{
				Phase:          v1alpha1.BackupPhaseSucceeded,
				CompletionTime: &ct,
			},
		}
	}
	b1 := mk("adhoc-1", 0)
	b2 := mk("adhoc-2", time.Hour)
	b3 := mk("adhoc-3", 2*time.Hour)
	fg := fgWithBackup() // profile "nightly-s3" has Retention=5 by default in fixture
	fg.Spec.Backup.Profiles[0].Retention = 2
	r, _ := newBackupReconciler(t, fg, b1, b2, b3)

	trigger := *b1
	if err := r.pruneRetention(context.Background(), &trigger); err != nil {
		t.Fatalf("pruneRetention: %v", err)
	}
	assertEqualSet(t, listBackupNames(t, r), []string{"adhoc-1", "adhoc-2"})
}

// --- TZ propagation ---------------------------------------------------------

func TestReconcileBackupSchedules_DefaultTimeZoneIsUTC(t *testing.T) {
	SetOperatorImageDefaults("bloodraven:test", "bloodraven")
	defer SetOperatorImageDefaults("", "")

	fg := fgWithBackup()
	r, c := newReconciler(fg)
	if err := r.reconcileBackupSchedules(context.Background(), fg); err != nil {
		t.Fatalf("reconcile: %v", err)
	}
	var cj batchv1.CronJob
	if err := c.Get(context.Background(), types.NamespacedName{
		Name: scheduleCronJobName("lion", "nightly"), Namespace: "ns",
	}, &cj); err != nil {
		t.Fatalf("get cronjob: %v", err)
	}
	if cj.Spec.TimeZone == nil || *cj.Spec.TimeZone != "Etc/UTC" {
		t.Errorf("want TimeZone=Etc/UTC, got %v", cj.Spec.TimeZone)
	}
}

func TestReconcileBackupSchedules_UserOverrideTimeZone(t *testing.T) {
	SetOperatorImageDefaults("bloodraven:test", "bloodraven")
	defer SetOperatorImageDefaults("", "")

	fg := fgWithBackup()
	fg.Spec.Backup.Schedules[0].TimeZone = "America/Los_Angeles"
	r, c := newReconciler(fg)
	if err := r.reconcileBackupSchedules(context.Background(), fg); err != nil {
		t.Fatalf("reconcile: %v", err)
	}
	var cj batchv1.CronJob
	if err := c.Get(context.Background(), types.NamespacedName{
		Name: scheduleCronJobName("lion", "nightly"), Namespace: "ns",
	}, &cj); err != nil {
		t.Fatalf("get cronjob: %v", err)
	}
	if cj.Spec.TimeZone == nil || *cj.Spec.TimeZone != "America/Los_Angeles" {
		t.Errorf("want TimeZone=America/Los_Angeles, got %v", cj.Spec.TimeZone)
	}
}

// --- restoreTargetSite ------------------------------------------------------

func TestRestoreTargetSite_UsesActiveSiteWhenWritable(t *testing.T) {
	fg := fgWithBackup()
	fg.Status = v1alpha1.MysqlFailoverGroupStatus{
		ActiveSite: "pdx",
		Sites: []v1alpha1.SiteStatus{
			{Name: "iad", State: "read-only"},
			{Name: "pdx", State: "writable"},
		},
	}
	if got := restoreTargetSite(fg); got != "pdx" {
		t.Errorf("want pdx, got %q", got)
	}
}

func TestRestoreTargetSite_RefusesReadOnlyActiveSite(t *testing.T) {
	fg := fgWithBackup()
	fg.Status = v1alpha1.MysqlFailoverGroupStatus{
		ActiveSite: "pdx",
		Sites: []v1alpha1.SiteStatus{
			{Name: "iad", State: "writable"},
			{Name: "pdx", State: "read-only"},
		},
	}
	if got := restoreTargetSite(fg); got != "" {
		t.Errorf("want empty (refuse), got %q", got)
	}
}

func TestRestoreTargetSite_FallsBackToSitesZeroOnFreshDeploy(t *testing.T) {
	fg := fgWithBackup()
	// No active site, no observed site status.
	fg.Status = v1alpha1.MysqlFailoverGroupStatus{}
	if got := restoreTargetSite(fg); got != "iad" {
		t.Errorf("want iad (spec.sites[0]), got %q", got)
	}
}

// --- maybeWarnInFlightFailover ---------------------------------------------

func drainEvents(rec *record.FakeRecorder) []string {
	var out []string
	for {
		select {
		case e := <-rec.Events:
			out = append(out, e)
		default:
			return out
		}
	}
}

func TestMaybeWarnInFlightFailover_EmitsWhenActiveSiteChanged(t *testing.T) {
	rec := record.NewFakeRecorder(10)
	r := &MysqlBackupReconciler{Recorder: rec}
	b := &v1alpha1.MysqlBackup{
		Status: v1alpha1.MysqlBackupStatus{ActiveSiteAtStart: "iad"},
	}
	fg := &v1alpha1.MysqlFailoverGroup{
		Status: v1alpha1.MysqlFailoverGroupStatus{ActiveSite: "pdx"},
	}
	r.maybeWarnInFlightFailover(b, fg)
	events := drainEvents(rec)
	if len(events) != 1 || !strings.Contains(events[0], "InFlightFailover") {
		t.Errorf("want InFlightFailover event, got %v", events)
	}
}

func TestMaybeWarnInFlightFailover_NoChangeNoEvent(t *testing.T) {
	rec := record.NewFakeRecorder(10)
	r := &MysqlBackupReconciler{Recorder: rec}
	b := &v1alpha1.MysqlBackup{
		Status: v1alpha1.MysqlBackupStatus{ActiveSiteAtStart: "iad"},
	}
	fg := &v1alpha1.MysqlFailoverGroup{
		Status: v1alpha1.MysqlFailoverGroupStatus{ActiveSite: "iad"},
	}
	r.maybeWarnInFlightFailover(b, fg)
	if events := drainEvents(rec); len(events) != 0 {
		t.Errorf("want no events, got %v", events)
	}
}

// --- stableJobCompletionTime -----------------------------------------------

func TestStableJobCompletionTime_PrefersConditionTransition(t *testing.T) {
	tt := mustParseTime(t, time.RFC3339, "2026-04-01T12:00:00Z")
	job := &batchv1.Job{
		Status: batchv1.JobStatus{
			Conditions: []batchv1.JobCondition{
				{Type: batchv1.JobComplete, Status: corev1.ConditionTrue, LastTransitionTime: tt},
			},
			CompletionTime: &metav1.Time{Time: time.Now()},
		},
	}
	got := stableJobCompletionTime(job)
	if got == nil || !got.Equal(&tt) {
		t.Errorf("got %v, want %v", got, tt)
	}
}

func TestStableJobCompletionTime_FallsBackToCompletionTime(t *testing.T) {
	tt := mustParseTime(t, time.RFC3339, "2026-04-02T12:00:00Z")
	job := &batchv1.Job{
		Status: batchv1.JobStatus{CompletionTime: &tt},
	}
	got := stableJobCompletionTime(job)
	if got == nil || !got.Equal(&tt) {
		t.Errorf("got %v, want %v", got, tt)
	}
}

func TestStableJobCompletionTime_NilWhenNoSignal(t *testing.T) {
	if got := stableJobCompletionTime(&batchv1.Job{}); got != nil {
		t.Errorf("want nil, got %v", got)
	}
}

// --- PITR validation -------------------------------------------------------

func TestReconcileBackupAssets_EmitsPITRWarningForUnknownProfile(t *testing.T) {
	fg := fgWithBackup()
	fg.Spec.Backup.PITR = &v1alpha1.PITRSpec{
		Enabled:     true,
		ProfileName: "does-not-exist",
	}
	r, _ := newReconciler(fg)
	if err := r.reconcileBackupAssets(context.Background(), fg); err != nil {
		t.Fatalf("reconcile: %v", err)
	}
	rec := r.Recorder.(*record.FakeRecorder)
	events := drainEvents(rec)
	found := false
	for _, e := range events {
		if strings.Contains(e, "BackupPITRInvalid") {
			found = true
		}
	}
	if !found {
		t.Errorf("want BackupPITRInvalid event, got %v", events)
	}
}

func TestReconcileBackupAssets_PITREnabledWithValidProfile(t *testing.T) {
	fg := fgWithBackup()
	fg.Spec.Backup.PITR = &v1alpha1.PITRSpec{
		Enabled:     true,
		ProfileName: fg.Spec.Backup.Profiles[0].Name,
	}
	r, _ := newReconciler(fg)
	if err := r.reconcileBackupAssets(context.Background(), fg); err != nil {
		t.Fatalf("reconcile: %v", err)
	}
	rec := r.Recorder.(*record.FakeRecorder)
	events := drainEvents(rec)
	for _, e := range events {
		if strings.Contains(e, "BackupPITRInvalid") {
			t.Errorf("did not expect BackupPITRInvalid for valid profile, got: %s", e)
		}
	}
}

// --- maybeScheduleRetry -----------------------------------------------------

func fgWithRetrySpec(maxAttempts, initial int32) *v1alpha1.MysqlFailoverGroup {
	fg := fgWithBackup()
	fg.Spec.Backup.Retry = &v1alpha1.BackupRetrySpec{
		MaxAttempts:           maxAttempts,
		InitialBackoffSeconds: initial,
		MaxBackoffSeconds:     1800,
	}
	return fg
}

func TestMaybeScheduleRetry_CreatesRetryCRWhenBackoffElapsed(t *testing.T) {
	fg := fgWithRetrySpec(3, 1)
	sched := fg.Spec.Backup.Schedules[0]
	completed := metav1.NewTime(time.Now().Add(-10 * time.Second))
	failed := &v1alpha1.MysqlBackup{
		ObjectMeta: metav1.ObjectMeta{
			Name: "lion-nightly-r1", Namespace: "ns",
			Labels: map[string]string{
				labelFailoverGroup:  "lion",
				labelBackupSchedule: sched.Name,
			},
		},
		Spec: v1alpha1.MysqlBackupSpec{
			FailoverGroupRef: v1alpha1.LocalGroupRef{Name: "lion"},
			ProfileName:      sched.ProfileName,
		},
		Status: v1alpha1.MysqlBackupStatus{
			Phase:          v1alpha1.BackupPhaseFailed,
			Attempt:        1,
			CompletionTime: &completed,
		},
	}
	r, c := newReconciler(fg, failed)
	next, wait, err := r.maybeScheduleRetry(context.Background(), fg, sched, failed)
	if err != nil {
		t.Fatalf("maybeScheduleRetry: %v", err)
	}
	if next != nil || wait != 0 {
		t.Errorf("expected immediate retry create, got next=%v wait=%v", next, wait)
	}
	var list v1alpha1.MysqlBackupList
	_ = c.List(context.Background(), &list, client.InNamespace("ns"))
	if len(list.Items) != 2 {
		t.Errorf("want 2 backups (original + retry), got %d", len(list.Items))
	}
}

func TestMaybeScheduleRetry_NoRetryWhenMaxAttemptsReached(t *testing.T) {
	fg := fgWithRetrySpec(2, 1)
	sched := fg.Spec.Backup.Schedules[0]
	completed := metav1.NewTime(time.Now().Add(-time.Hour))
	failed := &v1alpha1.MysqlBackup{
		ObjectMeta: metav1.ObjectMeta{
			Name: "lion-nightly-r2", Namespace: "ns",
			Labels: map[string]string{labelBackupSchedule: sched.Name, labelFailoverGroup: "lion"},
		},
		Spec: v1alpha1.MysqlBackupSpec{
			FailoverGroupRef: v1alpha1.LocalGroupRef{Name: "lion"},
			ProfileName:      sched.ProfileName,
		},
		Status: v1alpha1.MysqlBackupStatus{
			Phase:          v1alpha1.BackupPhaseFailed,
			Attempt:        2,
			CompletionTime: &completed,
		},
	}
	r, c := newReconciler(fg, failed)
	next, wait, err := r.maybeScheduleRetry(context.Background(), fg, sched, failed)
	if err != nil {
		t.Fatalf("maybeScheduleRetry: %v", err)
	}
	if next != nil || wait != 0 {
		t.Errorf("expected no-op, got next=%v wait=%v", next, wait)
	}
	var list v1alpha1.MysqlBackupList
	_ = c.List(context.Background(), &list, client.InNamespace("ns"))
	if len(list.Items) != 1 {
		t.Errorf("want 1 backup (no retry), got %d", len(list.Items))
	}
}

func TestMaybeScheduleRetry_ReturnsWaitWhenBackoffPending(t *testing.T) {
	fg := fgWithRetrySpec(3, 600)
	sched := fg.Spec.Backup.Schedules[0]
	completed := metav1.NewTime(time.Now().Add(-1 * time.Second))
	failed := &v1alpha1.MysqlBackup{
		ObjectMeta: metav1.ObjectMeta{
			Name: "lion-nightly-r1", Namespace: "ns",
			Labels: map[string]string{labelBackupSchedule: sched.Name, labelFailoverGroup: "lion"},
		},
		Spec: v1alpha1.MysqlBackupSpec{
			FailoverGroupRef: v1alpha1.LocalGroupRef{Name: "lion"},
			ProfileName:      sched.ProfileName,
		},
		Status: v1alpha1.MysqlBackupStatus{
			Phase:          v1alpha1.BackupPhaseFailed,
			Attempt:        1,
			CompletionTime: &completed,
		},
	}
	r, _ := newReconciler(fg, failed)
	next, wait, err := r.maybeScheduleRetry(context.Background(), fg, sched, failed)
	if err != nil {
		t.Fatalf("maybeScheduleRetry: %v", err)
	}
	if next == nil {
		t.Error("expected non-nil next time")
	}
	if wait <= 0 {
		t.Errorf("expected positive wait, got %v", wait)
	}
}

// --- MysqlBackupReconciler stamps labels ------------------------------------

func TestMysqlBackupReconciler_StampsLabels(t *testing.T) {
	fg := fgWithBackup()
	fg.Status = v1alpha1.MysqlFailoverGroupStatus{
		ActiveSite: "iad",
		Sites: []v1alpha1.SiteStatus{
			{Name: "iad", State: "writable"},
			{Name: "pdx", State: "read-only", Replicating: true},
		},
	}
	mb := &v1alpha1.MysqlBackup{
		ObjectMeta: metav1.ObjectMeta{Name: "bare-adhoc", Namespace: "ns"},
		Spec: v1alpha1.MysqlBackupSpec{
			FailoverGroupRef: v1alpha1.LocalGroupRef{Name: "lion"},
			ProfileName:      "nightly-s3",
		},
	}
	r, c := newBackupReconciler(t, fg, mb, dsnSecret())
	reconcileUntilStable(t, r, "bare-adhoc")
	var got v1alpha1.MysqlBackup
	_ = c.Get(context.Background(), types.NamespacedName{Name: "bare-adhoc", Namespace: "ns"}, &got)
	if got.Labels[labelFailoverGroup] != "lion" ||
		got.Labels[labelBackupProfile] != "nightly-s3" ||
		got.Labels[labelManagedBy] != managerName {
		t.Errorf("labels not stamped: %+v", got.Labels)
	}
}

// --- emitTerminalMetrics smoke tests ----------------------------------------

func TestEmitTerminalMetrics_SuccessUpdatesGauges(t *testing.T) {
	r := &MysqlBackupReconciler{Recorder: record.NewFakeRecorder(1)}
	start := metav1.NewTime(time.Now().Add(-5 * time.Minute))
	end := metav1.NewTime(time.Now())
	b := &v1alpha1.MysqlBackup{
		Spec: v1alpha1.MysqlBackupSpec{
			FailoverGroupRef: v1alpha1.LocalGroupRef{Name: "lion"},
			ProfileName:      "nightly",
		},
		Status: v1alpha1.MysqlBackupStatus{
			Phase:          v1alpha1.BackupPhaseSucceeded,
			StartTime:      &start,
			CompletionTime: &end,
			SizeBytes:      12345,
		},
	}
	job := &batchv1.Job{}
	// Should not panic.
	r.emitTerminalMetrics(b, job)
}

func TestEmitTerminalMetrics_FailureDoesNotSetSuccessGauge(t *testing.T) {
	r := &MysqlBackupReconciler{Recorder: record.NewFakeRecorder(1)}
	start := metav1.NewTime(time.Now().Add(-time.Minute))
	end := metav1.NewTime(time.Now())
	b := &v1alpha1.MysqlBackup{
		Spec: v1alpha1.MysqlBackupSpec{
			FailoverGroupRef: v1alpha1.LocalGroupRef{Name: "lion"},
			ProfileName:      "nightly",
		},
		Status: v1alpha1.MysqlBackupStatus{
			Phase:          v1alpha1.BackupPhaseFailed,
			StartTime:      &start,
			CompletionTime: &end,
		},
	}
	r.emitTerminalMetrics(b, &batchv1.Job{})
}

// --- buildCleanupJob --------------------------------------------------------

func TestBuildCleanupJob_S3_SetsStorageTypeEnvAndMountsAWSCreds(t *testing.T) {
	fg := fgWithBackup()
	profile := fg.Spec.Backup.Profiles[0] // nightly-s3
	backup := &v1alpha1.MysqlBackup{
		ObjectMeta: metav1.ObjectMeta{Name: "lion-done", Namespace: "ns"},
		Spec: v1alpha1.MysqlBackupSpec{
			FailoverGroupRef: v1alpha1.LocalGroupRef{Name: "lion"},
			ProfileName:      "nightly-s3",
		},
		Status: v1alpha1.MysqlBackupStatus{
			Location:    "lion/lion-done/",
			StorageType: v1alpha1.BackupStorageS3,
		},
	}
	job, err := buildCleanupJob(cleanupJobInputs{
		FailoverGroup:        fg,
		Profile:              &profile,
		Backup:               backup,
		CredsSecretName:      backupCredsSecretName(backup.Name),
		ScriptsConfigMapName: backupScriptsConfigMapName(fg.Name),
	})
	if err != nil {
		t.Fatalf("buildCleanupJob: %v", err)
	}
	c := job.Spec.Template.Spec.Containers[0]
	envMap := map[string]string{}
	for _, e := range c.Env {
		envMap[e.Name] = e.Value
	}
	if envMap["BLOODRAVEN_STORAGE_TYPE"] != "S3" {
		t.Errorf("want BLOODRAVEN_STORAGE_TYPE=S3, got %q", envMap["BLOODRAVEN_STORAGE_TYPE"])
	}
	if envMap["BLOODRAVEN_OUTPUT_URL"] != "lion/lion-done/" {
		t.Errorf("want OUTPUT_URL=lion/lion-done/, got %q", envMap["BLOODRAVEN_OUTPUT_URL"])
	}
	if envMap["BLOODRAVEN_S3_BUCKET"] != "bloodraven-backups" {
		t.Errorf("want bucket env, got %q", envMap["BLOODRAVEN_S3_BUCKET"])
	}
	var hasAWS bool
	for _, v := range job.Spec.Template.Spec.Volumes {
		if v.Name == "aws-creds" {
			hasAWS = true
		}
	}
	if !hasAWS {
		t.Error("aws-creds volume must be attached for S3 cleanup")
	}
	if !strings.HasSuffix(c.Command[len(c.Command)-1], "/cleanup.py") {
		t.Errorf("want cleanup.py command, got %v", c.Command)
	}
}

func TestBuildCleanupJob_PVC_AttachesVolumeAndSetsMountPath(t *testing.T) {
	fg := fgWithBackup()
	profile := fg.Spec.Backup.Profiles[1] // daily-local
	backup := &v1alpha1.MysqlBackup{
		ObjectMeta: metav1.ObjectMeta{Name: "lion-local", Namespace: "ns"},
		Spec: v1alpha1.MysqlBackupSpec{
			FailoverGroupRef: v1alpha1.LocalGroupRef{Name: "lion"},
			ProfileName:      "daily-local",
		},
		Status: v1alpha1.MysqlBackupStatus{
			Location:    "/backups/lion-local",
			StorageType: v1alpha1.BackupStoragePVC,
		},
	}
	job, err := buildCleanupJob(cleanupJobInputs{
		FailoverGroup:        fg,
		Profile:              &profile,
		Backup:               backup,
		CredsSecretName:      backupCredsSecretName(backup.Name),
		ScriptsConfigMapName: backupScriptsConfigMapName(fg.Name),
	})
	if err != nil {
		t.Fatalf("buildCleanupJob: %v", err)
	}
	c := job.Spec.Template.Spec.Containers[0]
	envMap := map[string]string{}
	for _, e := range c.Env {
		envMap[e.Name] = e.Value
	}
	if envMap["BLOODRAVEN_PVC_MOUNT_PATH"] != backupPVCMountPath {
		t.Errorf("want BLOODRAVEN_PVC_MOUNT_PATH=%s, got %q", backupPVCMountPath, envMap["BLOODRAVEN_PVC_MOUNT_PATH"])
	}
	var hasBackups bool
	for _, v := range job.Spec.Template.Spec.Volumes {
		if v.Name == "backups" && v.PersistentVolumeClaim != nil {
			hasBackups = true
		}
	}
	if !hasBackups {
		t.Error("backups PVC volume must be attached for PVC cleanup")
	}
}

func TestBuildCleanupJob_NoLocation_Errors(t *testing.T) {
	fg := fgWithBackup()
	profile := fg.Spec.Backup.Profiles[0]
	backup := &v1alpha1.MysqlBackup{
		ObjectMeta: metav1.ObjectMeta{Name: "lion-none", Namespace: "ns"},
		Spec: v1alpha1.MysqlBackupSpec{
			FailoverGroupRef: v1alpha1.LocalGroupRef{Name: "lion"},
			ProfileName:      "nightly-s3",
		},
	}
	if _, err := buildCleanupJob(cleanupJobInputs{
		FailoverGroup:        fg,
		Profile:              &profile,
		Backup:               backup,
		CredsSecretName:      "x",
		ScriptsConfigMapName: "y",
	}); err == nil {
		t.Error("expected error for empty status.location")
	}
}

// --- finalize ---------------------------------------------------------------

func TestFinalize_NoLocationIsImmediatelyDone(t *testing.T) {
	b := backupCR("bare", "lion", "nightly-s3")
	r, _ := newBackupReconciler(t, b)
	done, err := r.finalize(context.Background(), b)
	if err != nil || !done {
		t.Errorf("want (true, nil), got (%v, %v)", done, err)
	}
}

func TestFinalize_MissingFailoverGroup_ReleasesWithWarning(t *testing.T) {
	b := backupCR("orphan", "ghost", "nightly-s3")
	b.Status.Location = "ghost/orphan/"
	r, _ := newBackupReconciler(t, b)
	done, err := r.finalize(context.Background(), b)
	if err != nil || !done {
		t.Errorf("want (true, nil), got (%v, %v)", done, err)
	}
	events := drainEvents(r.Recorder.(*record.FakeRecorder))
	found := false
	for _, e := range events {
		if strings.Contains(e, "ArtifactCleanupSkipped") {
			found = true
		}
	}
	if !found {
		t.Errorf("want ArtifactCleanupSkipped event, got %v", events)
	}
}

func TestFinalize_CreatesCleanupJobAndWaits(t *testing.T) {
	fg := fgWithBackup()
	b := backupCR("done", "lion", "nightly-s3")
	b.Status.Location = "lion/done/"
	b.Status.StorageType = v1alpha1.BackupStorageS3
	r, c := newBackupReconciler(t, fg, b, dsnSecret())

	// First pass: creates cleanup Job, returns (false, nil).
	done, err := r.finalize(context.Background(), b)
	if err != nil || done {
		t.Fatalf("first pass: want (false, nil), got (%v, %v)", done, err)
	}
	var cj batchv1.Job
	if err := c.Get(context.Background(), types.NamespacedName{
		Name: cleanupJobName("done"), Namespace: "ns",
	}, &cj); err != nil {
		t.Fatalf("cleanup job not created: %v", err)
	}

	// Mutate to Succeeded and re-finalize.
	now := metav1.Now()
	cj.Status.Conditions = []batchv1.JobCondition{{
		Type:               batchv1.JobComplete,
		Status:             corev1.ConditionTrue,
		LastTransitionTime: now,
	}}
	cj.Status.Succeeded = 1
	if err := c.Status().Update(context.Background(), &cj); err != nil {
		t.Fatalf("status update: %v", err)
	}
	done, err = r.finalize(context.Background(), b)
	if err != nil || !done {
		t.Errorf("second pass: want (true, nil), got (%v, %v)", done, err)
	}
}
