package controller

import (
	"context"
	"fmt"
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
				v1alpha1.FailoverGroupLabel: fgName,
				v1alpha1.BackupProfileLabel: profile,
				labelManagedBy:              managerName,
			},
		},
		Spec: v1alpha1.MysqlBackupSpec{
			FailoverGroupRef: v1alpha1.LocalFailoverGroupRef{Name: fgName},
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
	want := "container-registry.oracle.com/mysql/community-server:9.7"
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
			FailoverGroupRef: v1alpha1.LocalFailoverGroupRef{Name: "lion"},
			ProfileName:      "nightly",
		},
	}
	if !ensureBackupLabels(b) {
		t.Error("first call should report changes")
	}
	if b.Labels[v1alpha1.FailoverGroupLabel] != "lion" ||
		b.Labels[v1alpha1.BackupProfileLabel] != "nightly" ||
		b.Labels[labelManagedBy] != managerName {
		t.Errorf("labels not stamped: %+v", b.Labels)
	}
	if ensureBackupLabels(b) {
		t.Error("second call should be a no-op")
	}
}

// --- resolveRetention / pruneRetention -------------------------------------

func TestResolveRetention(t *testing.T) {
	day := 24 * time.Hour
	for _, tc := range []struct {
		name    string
		profile *v1alpha1.BackupProfile
		want    backupRetentionPolicy
	}{
		{name: "missing profile prunes no success", profile: nil,
			want: backupRetentionPolicy{keepFailed: defaultBackupKeepFailed}},
		{name: "omitted fields take the defaults", profile: &v1alpha1.BackupProfile{},
			want: backupRetentionPolicy{pruneSucceeded: true, keepLast: defaultBackupKeepLast, keepFailed: defaultBackupKeepFailed}},
		{name: "every field set", profile: &v1alpha1.BackupProfile{Retention: v1alpha1.BackupRetention{
			KeepLast: 3, KeepWithinDays: new(int32(7)), KeepFailed: new(int32(4))}},
			want: backupRetentionPolicy{pruneSucceeded: true, keepLast: 3, keepWithin: 7 * day, keepFailed: 4}},
		// An explicit 0 is kept, never rewritten to the default (A-37, F-26).
		{name: "explicit keepFailed 0 keeps none", profile: &v1alpha1.BackupProfile{Retention: v1alpha1.BackupRetention{
			KeepLast: 1, KeepFailed: new(int32(0))}},
			want: backupRetentionPolicy{pruneSucceeded: true, keepLast: 1, keepFailed: 0}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if got := resolveRetention(tc.profile); got != tc.want {
				t.Fatalf("resolveRetention = %+v, want %+v", got, tc.want)
			}
		})
	}
}

// failedBackup is a Failed MysqlBackup of fgName/profile that completed at
// completed.
func failedBackup(name, fgName, profile string, completed metav1.Time) v1alpha1.MysqlBackup {
	b := succeededBackup(name, "ns", fgName, profile, completed)
	b.Status.Phase = v1alpha1.BackupPhaseFailed
	return b
}

// TestPruneRetention_KeepIfAny is the A-55 table: a successful backup is
// deleted only when it is outside keepLast and older than keepWithinDays;
// failed backups are capped by keepFailed, where an explicit 0 keeps none
// and an omitted field means the default.
func TestPruneRetention_KeepIfAny(t *testing.T) {
	h, d := time.Hour, 24*time.Hour
	ages := func(n int, step time.Duration) []time.Duration {
		// Starts one step back: a backup that completed within
		// terminalRetentionGrace is kept whatever the counts say.
		out := make([]time.Duration, n)
		for i := range out {
			out[i] = time.Duration(i+1) * step
		}
		return out
	}
	names := func(prefix string, n int) []string {
		out := make([]string, n)
		for i := range out {
			out[i] = fmt.Sprintf("%s-%02d", prefix, i)
		}
		return out
	}
	for _, tc := range []struct {
		name          string
		retention     *v1alpha1.BackupRetention // nil: the profile is gone
		succeeded     []time.Duration
		failed        []time.Duration
		wantSucceeded int // how many of the newest are kept
		wantFailed    int
	}{
		{name: "omitted: keepLast 7, keepFailed 10", retention: &v1alpha1.BackupRetention{},
			succeeded: ages(9, h), failed: ages(12, h), wantSucceeded: 7, wantFailed: 10},
		{name: "keepLast only", retention: &v1alpha1.BackupRetention{KeepLast: 2},
			succeeded: ages(5, h), wantSucceeded: 2},
		{name: "age keeps beyond the count", retention: &v1alpha1.BackupRetention{KeepLast: 1, KeepWithinDays: new(int32(7))},
			succeeded: []time.Duration{h, 2 * d, 10 * d, 20 * d}, wantSucceeded: 2},
		{name: "count keeps beyond the age", retention: &v1alpha1.BackupRetention{KeepLast: 3, KeepWithinDays: new(int32(1))},
			succeeded: []time.Duration{2 * d, 3 * d, 4 * d, 5 * d}, wantSucceeded: 3},
		{name: "keepLast 0 from Go means the default", retention: &v1alpha1.BackupRetention{KeepLast: 0},
			succeeded: ages(8, h), wantSucceeded: 7},
		{name: "explicit keepFailed 0 keeps none", retention: &v1alpha1.BackupRetention{KeepLast: 7, KeepFailed: new(int32(0))},
			succeeded: ages(2, h), failed: ages(3, h), wantSucceeded: 2, wantFailed: 0},
		{name: "keepFailed 2", retention: &v1alpha1.BackupRetention{KeepLast: 7, KeepFailed: new(int32(2))},
			failed: ages(5, h), wantFailed: 2},
		{name: "missing profile keeps every success", retention: nil,
			succeeded: ages(9, d), failed: ages(12, h), wantSucceeded: 9, wantFailed: 10},
	} {
		t.Run(tc.name, func(t *testing.T) {
			now := time.Now()
			fg := fgWithBackup()
			profile := "nightly-s3"
			if tc.retention != nil {
				fg.Spec.Backup.Profiles[0].Retention = *tc.retention
			} else {
				profile = "removed"
			}
			var objs []client.Object
			objs = append(objs, fg)
			sNames, fNames := names("s", len(tc.succeeded)), names("f", len(tc.failed))
			for i, age := range tc.succeeded {
				b := succeededBackup(sNames[i], "ns", fg.Name, profile, metav1.NewTime(now.Add(-age)))
				objs = append(objs, &b)
			}
			for i, age := range tc.failed {
				b := failedBackup(fNames[i], fg.Name, profile, metav1.NewTime(now.Add(-age)))
				objs = append(objs, &b)
			}
			r, _ := newBackupReconciler(t, objs...)
			trigger := v1alpha1.MysqlBackup{ObjectMeta: metav1.ObjectMeta{Name: "trigger", Namespace: "ns"},
				Spec: v1alpha1.MysqlBackupSpec{FailoverGroupRef: v1alpha1.LocalFailoverGroupRef{Name: fg.Name}, ProfileName: profile}}
			if _, err := r.pruneRetention(context.Background(), &trigger); err != nil {
				t.Fatalf("pruneRetention: %v", err)
			}
			// Both buckets are named in age order, newest first.
			want := append(append([]string(nil), sNames[:tc.wantSucceeded]...), fNames[:tc.wantFailed]...)
			assertEqualSet(t, listBackupNames(t, r), want)
		})
	}
}

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

// --- pruneRetention on unlabelled CRs ---------------------------------------

func TestPruneRetention_WorksForUnlabelledAdHocBackups(t *testing.T) {
	now := time.Now()
	mk := func(name string, age time.Duration) *v1alpha1.MysqlBackup {
		ct := metav1.NewTime(now.Add(-age))
		return &v1alpha1.MysqlBackup{
			ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: "ns"},
			Spec: v1alpha1.MysqlBackupSpec{
				FailoverGroupRef: v1alpha1.LocalFailoverGroupRef{Name: "lion"},
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
	fg := fgWithBackup() // profile "nightly-s3" keeps 5 in the fixture
	fg.Spec.Backup.Profiles[0].Retention = v1alpha1.BackupRetention{KeepLast: 2}
	r, _ := newBackupReconciler(t, fg, b1, b2, b3)

	trigger := *b1
	if _, err := r.pruneRetention(context.Background(), &trigger); err != nil {
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

// TestRestoreTargetSite covers the gate on creating the initFromBackup
// restore Job: every site boots fenced, so the load may start only once the
// operator has seeded the target and it is observed writable.
func TestRestoreTargetSite(t *testing.T) {
	cases := []struct {
		name       string
		status     v1alpha1.MysqlFailoverGroupStatus
		wantSite   string
		wantReady  bool
		wantRefuse bool
	}{
		{
			name:     "fresh deploy waits for the seed, which is spec.sites[0]",
			status:   v1alpha1.MysqlFailoverGroupStatus{},
			wantSite: "iad",
		},
		{
			name: "fresh deploy waits even when the seed is observed",
			status: v1alpha1.MysqlFailoverGroupStatus{Sites: []v1alpha1.SiteStatus{
				{Name: "iad", State: v1alpha1.SiteStateReadOnly}, {Name: "pdx", State: v1alpha1.SiteStateReadOnly},
			}},
			wantSite: "iad",
		},
		{
			name: "writable active site is ready",
			status: v1alpha1.MysqlFailoverGroupStatus{ActiveSite: "pdx", Sites: []v1alpha1.SiteStatus{
				{Name: "iad", State: v1alpha1.SiteStateReadOnly}, {Name: "pdx", State: v1alpha1.SiteStateWritable},
			}},
			wantSite: "pdx", wantReady: true,
		},
		{
			name: "seed published but not yet observed writable waits",
			status: v1alpha1.MysqlFailoverGroupStatus{ActiveSite: "pdx", Sites: []v1alpha1.SiteStatus{
				{Name: "iad", State: v1alpha1.SiteStateWritable}, {Name: "pdx", State: v1alpha1.SiteStateReadOnly},
			}},
			wantSite: "pdx",
		},
		{
			name:     "active site without a status entry waits",
			status:   v1alpha1.MysqlFailoverGroupStatus{ActiveSite: "pdx"},
			wantSite: "pdx",
		},
		{
			name: "unreachable active site refuses",
			status: v1alpha1.MysqlFailoverGroupStatus{ActiveSite: "pdx", Sites: []v1alpha1.SiteStatus{
				{Name: "iad", State: v1alpha1.SiteStateReadOnly}, {Name: "pdx", State: v1alpha1.SiteStateUnreachable},
			}},
			wantSite: "pdx", wantRefuse: true,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			fg := fgWithBackup()
			fg.Status = tc.status
			site, ready, wait, refuse := restoreTargetSite(fg)
			if site != tc.wantSite || ready != tc.wantReady || refuse != tc.wantRefuse {
				t.Fatalf("got (%q, ready=%v, refuse=%v), want (%q, ready=%v, refuse=%v)",
					site, ready, refuse, tc.wantSite, tc.wantReady, tc.wantRefuse)
			}
			if ready != (wait == "") {
				t.Fatalf("ready=%v with wait message %q", ready, wait)
			}
		})
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
		MaxAttempts:    maxAttempts,
		InitialBackoff: &metav1.Duration{Duration: time.Duration(initial) * time.Second},
		MaxBackoff:     &metav1.Duration{Duration: 30 * time.Minute},
	}
	return fg
}

func TestMaybeScheduleRetry_CreatesRetryCRWhenBackoffElapsed(t *testing.T) {
	fg := fgWithRetrySpec(3, 1)
	sched := fg.Spec.Backup.Schedules[0]
	completed := metav1.NewTime(time.Now().Add(-10 * time.Second))
	failed := &v1alpha1.MysqlBackup{
		ObjectMeta: metav1.ObjectMeta{
			Name: "lion-nightly-r1", Namespace: "ns", UID: "failed-backup-uid",
			Labels: map[string]string{
				v1alpha1.FailoverGroupLabel:  "lion",
				v1alpha1.BackupScheduleLabel: sched.Name,
			},
		},
		Spec: v1alpha1.MysqlBackupSpec{
			FailoverGroupRef: v1alpha1.LocalFailoverGroupRef{Name: "lion"},
			ProfileName:      sched.ProfileName,
		},
		Status: v1alpha1.MysqlBackupStatus{
			Phase:          v1alpha1.BackupPhaseFailed,
			Attempt:        1,
			CompletionTime: &completed,
		},
	}
	r, c := newReconciler(fg, failed)
	next, wait, _, err := r.maybeScheduleRetry(context.Background(), fg, sched, failed)
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

// A suspended schedule makes no retry backups: suspending every schedule
// is how the group-deletion procedure stops its producers.
func TestMaybeScheduleRetry_NoRetryWhileScheduleSuspended(t *testing.T) {
	fg := fgWithRetrySpec(3, 1)
	fg.Spec.Backup.Schedules[0].Suspend = true
	sched := fg.Spec.Backup.Schedules[0]
	completed := metav1.NewTime(time.Now().Add(-10 * time.Second))
	failed := &v1alpha1.MysqlBackup{
		ObjectMeta: metav1.ObjectMeta{
			Name: "lion-nightly-r1", Namespace: "ns", UID: "failed-backup-uid",
			Labels: map[string]string{v1alpha1.FailoverGroupLabel: "lion", v1alpha1.BackupScheduleLabel: sched.Name},
		},
		Spec: v1alpha1.MysqlBackupSpec{
			FailoverGroupRef: v1alpha1.LocalFailoverGroupRef{Name: "lion"},
			ProfileName:      sched.ProfileName,
		},
		Status: v1alpha1.MysqlBackupStatus{Phase: v1alpha1.BackupPhaseFailed, Attempt: 1, CompletionTime: &completed},
	}
	r, c := newReconciler(fg, failed)
	next, wait, _, err := r.maybeScheduleRetry(context.Background(), fg, sched, failed)
	if err != nil {
		t.Fatalf("maybeScheduleRetry: %v", err)
	}
	if next != nil || wait != 0 {
		t.Errorf("suspended schedule: next=%v wait=%v, want no retry pending", next, wait)
	}
	var list v1alpha1.MysqlBackupList
	_ = c.List(context.Background(), &list, client.InNamespace("ns"))
	if len(list.Items) != 1 {
		t.Errorf("a suspended schedule created a retry backup: %d backups", len(list.Items))
	}
}

func TestMaybeScheduleRetry_NoRetryWhenMaxAttemptsReached(t *testing.T) {
	fg := fgWithRetrySpec(2, 1)
	sched := fg.Spec.Backup.Schedules[0]
	completed := metav1.NewTime(time.Now().Add(-time.Hour))
	failed := &v1alpha1.MysqlBackup{
		ObjectMeta: metav1.ObjectMeta{
			Name: "lion-nightly-r2", Namespace: "ns",
			Labels: map[string]string{v1alpha1.BackupScheduleLabel: sched.Name, v1alpha1.FailoverGroupLabel: "lion"},
		},
		Spec: v1alpha1.MysqlBackupSpec{
			FailoverGroupRef: v1alpha1.LocalFailoverGroupRef{Name: "lion"},
			ProfileName:      sched.ProfileName,
		},
		Status: v1alpha1.MysqlBackupStatus{
			Phase:          v1alpha1.BackupPhaseFailed,
			Attempt:        2,
			CompletionTime: &completed,
		},
	}
	r, c := newReconciler(fg, failed)
	next, wait, _, err := r.maybeScheduleRetry(context.Background(), fg, sched, failed)
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
			Labels: map[string]string{v1alpha1.BackupScheduleLabel: sched.Name, v1alpha1.FailoverGroupLabel: "lion"},
		},
		Spec: v1alpha1.MysqlBackupSpec{
			FailoverGroupRef: v1alpha1.LocalFailoverGroupRef{Name: "lion"},
			ProfileName:      sched.ProfileName,
		},
		Status: v1alpha1.MysqlBackupStatus{
			Phase:          v1alpha1.BackupPhaseFailed,
			Attempt:        1,
			CompletionTime: &completed,
		},
	}
	r, _ := newReconciler(fg, failed)
	next, wait, _, err := r.maybeScheduleRetry(context.Background(), fg, sched, failed)
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
			{Name: "iad", State: v1alpha1.SiteStateWritable},
			{Name: "pdx", State: v1alpha1.SiteStateReadOnly, Replication: v1alpha1.SiteReplicationHealthy},
		},
	}
	mb := &v1alpha1.MysqlBackup{
		ObjectMeta: metav1.ObjectMeta{Name: "bare-adhoc", Namespace: "ns"},
		Spec: v1alpha1.MysqlBackupSpec{
			FailoverGroupRef: v1alpha1.LocalFailoverGroupRef{Name: "lion"},
			ProfileName:      "nightly-s3",
		},
	}
	r, c := newBackupReconciler(t, fg, mb, operatorCredsSecret())
	reconcileUntilStable(t, r, "bare-adhoc")
	var got v1alpha1.MysqlBackup
	_ = c.Get(context.Background(), types.NamespacedName{Name: "bare-adhoc", Namespace: "ns"}, &got)
	if got.Labels[v1alpha1.FailoverGroupLabel] != "lion" ||
		got.Labels[v1alpha1.BackupProfileLabel] != "nightly-s3" ||
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
			FailoverGroupRef: v1alpha1.LocalFailoverGroupRef{Name: "lion"},
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
			FailoverGroupRef: v1alpha1.LocalFailoverGroupRef{Name: "lion"},
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
			FailoverGroupRef: v1alpha1.LocalFailoverGroupRef{Name: "lion"},
			ProfileName:      "nightly-s3",
		},
		Status: v1alpha1.MysqlBackupStatus{
			ArtifactStorage: testArtifactStorage(v1alpha1.BackupStorageS3, "lion/lion-done/"),
		},
	}
	job, err := buildCleanupJob(cleanupTestInputs(t, fg, &profile, backup))
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
	if strings.Join(c.Command, " ") != operatorBinaryPath+" artifact-cleanup" || c.Image != cleanupTestImage {
		t.Errorf("want operator-image artifact-cleanup, got %s %v", c.Image, c.Command)
	}
	if envMap["BLOODRAVEN_BACKUP_NAME"] != "lion-done" {
		t.Errorf("want BLOODRAVEN_BACKUP_NAME=lion-done, got %q", envMap["BLOODRAVEN_BACKUP_NAME"])
	}
}

func TestBuildCleanupJob_PVC_AttachesVolumeAndSetsMountPath(t *testing.T) {
	fg := fgWithBackup()
	profile := fg.Spec.Backup.Profiles[1] // daily-local
	backup := &v1alpha1.MysqlBackup{
		ObjectMeta: metav1.ObjectMeta{Name: "lion-local", Namespace: "ns"},
		Spec: v1alpha1.MysqlBackupSpec{
			FailoverGroupRef: v1alpha1.LocalFailoverGroupRef{Name: "lion"},
			ProfileName:      "daily-local",
		},
		Status: v1alpha1.MysqlBackupStatus{
			ArtifactStorage: testArtifactStorage(v1alpha1.BackupStoragePVC, "/backups/lion-local"),
		},
	}
	job, err := buildCleanupJob(cleanupTestInputs(t, fg, &profile, backup))
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
			FailoverGroupRef: v1alpha1.LocalFailoverGroupRef{Name: "lion"},
			ProfileName:      "nightly-s3",
		},
	}
	if _, err := buildCleanupJob(cleanupTestInputs(t, fg, &profile, backup)); err == nil {
		t.Error("expected error for empty status.location")
	}
}

// --- finalize ---------------------------------------------------------------

func TestFinalize_NoLocationIsImmediatelyDone(t *testing.T) {
	b := backupCR("bare", "lion", "nightly-s3")
	r, _ := newBackupReconciler(t, b)
	done, _, err := r.finalize(context.Background(), b)
	if err != nil || !done {
		t.Errorf("want (true, nil), got (%v, %v)", done, err)
	}
}

func TestFinalize_MissingFailoverGroup_ReleasesWithWarning(t *testing.T) {
	b := backupCR("orphan", "ghost", "nightly-s3")
	b.Status.Phase = v1alpha1.BackupPhaseSucceeded
	setTestArtifactLocation(b, "ghost/orphan/")
	r, _ := newBackupReconciler(t, b)
	done, _, err := r.finalize(context.Background(), b)
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
	SetOperatorImageDefaults(cleanupTestImage, "bloodraven")
	t.Cleanup(func() { SetOperatorImageDefaults("", "") })
	fg := fgWithBackup()
	b := backupCR("done", "lion", "nightly-s3")
	b.Status.Phase = v1alpha1.BackupPhaseSucceeded
	b.Status.ArtifactStorage = &v1alpha1.BackupArtifactStorage{Type: v1alpha1.BackupStorageS3, Location: "lion/done/", Bucket: "bloodraven-backups"}
	// No MySQL credentials Secret: cleanup only talks to storage.
	r, c := newBackupReconciler(t, fg, b)

	// First pass: creates cleanup Job, returns (false, nil).
	done, _, err := r.finalize(context.Background(), b)
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
	done, _, err = r.finalize(context.Background(), b)
	if err != nil || !done {
		t.Errorf("second pass: want (true, nil), got (%v, %v)", done, err)
	}
}

// TestPruneRetention_KeepsScheduleLatestFailed (O-05): keepFailed 0 deletes
// failed backups, but never one that is still its schedule's latest: the
// operator retry (spec.backup.retry) is created from it, and the schedule
// status reports it. Once a newer backup of the schedule exists (its retry,
// or the next run), it is pruned like any other.
func TestPruneRetention_KeepsScheduleLatestFailed(t *testing.T) {
	now := time.Now()
	fg := fgWithBackup()
	fg.Spec.Backup.Profiles[0].Retention = v1alpha1.BackupRetention{KeepLast: 7, KeepFailed: new(int32(0))}
	fg.Spec.Backup.Schedules = []v1alpha1.BackupSchedule{
		{Name: "nightly", ProfileName: "nightly-s3", Schedule: "0 2 * * *"},
		{Name: "hourly", ProfileName: "nightly-s3", Schedule: "0 * * * *"},
	}
	scheduled := func(b v1alpha1.MysqlBackup, sched string, created time.Duration) *v1alpha1.MysqlBackup {
		b.Labels[v1alpha1.BackupScheduleLabel] = sched
		b.CreationTimestamp = metav1.NewTime(now.Add(-created).Truncate(time.Second))
		return &b
	}
	objs := []client.Object{fg,
		// nightly: an old failure, then a success, then the failure a
		// retry is due for.
		scheduled(failedBackup("nightly-old-failed", fg.Name, "nightly-s3", metav1.NewTime(now.Add(-47*time.Hour))), "nightly", 48*time.Hour),
		scheduled(succeededBackup("nightly-ok", "ns", fg.Name, "nightly-s3", metav1.NewTime(now.Add(-23*time.Hour))), "nightly", 24*time.Hour),
		scheduled(failedBackup("nightly-latest-failed", fg.Name, "nightly-s3", metav1.NewTime(now.Add(-time.Minute))), "nightly", time.Hour),
		// hourly: its failure already has a newer attempt (the retry).
		scheduled(failedBackup("hourly-failed", fg.Name, "nightly-s3", metav1.NewTime(now.Add(-50*time.Minute))), "hourly", time.Hour),
		scheduled(succeededBackup("hourly-retry", "ns", fg.Name, "nightly-s3", metav1.NewTime(now.Add(-10*time.Minute))), "hourly", 30*time.Minute),
	}
	adhoc := failedBackup("adhoc-failed", fg.Name, "nightly-s3", metav1.NewTime(now.Add(-time.Hour)))
	// N-09: the latest backup of a schedule the group no longer names has
	// no retry and no rollup entry, so it is pruned like any other.
	retired := scheduled(failedBackup("retired-latest-failed", fg.Name, "nightly-s3", metav1.NewTime(now.Add(-2*time.Hour))), "weekly", 3*time.Hour)
	objs = append(objs, &adhoc, retired)
	r, _ := newBackupReconciler(t, objs...)
	trigger := v1alpha1.MysqlBackup{ObjectMeta: metav1.ObjectMeta{Name: "trigger", Namespace: "ns"},
		Spec: v1alpha1.MysqlBackupSpec{FailoverGroupRef: v1alpha1.LocalFailoverGroupRef{Name: fg.Name}, ProfileName: "nightly-s3"}}
	if _, err := r.pruneRetention(context.Background(), &trigger); err != nil {
		t.Fatalf("pruneRetention: %v", err)
	}
	assertEqualSet(t, listBackupNames(t, r), []string{"nightly-ok", "nightly-latest-failed", "hourly-retry"})
}

// N-08: keep-none (keepFailed: 0) must not delete a backup in its own
// terminal pass, before `kubectl bloodraven backup --wait` reads the
// verdict. A Failed backup is kept for terminalRetentionGrace after it
// completed; the pass asks to be requeued when the grace ends, and the
// pass after it prunes the backup.
func TestPruneRetention_KeepNoneWaitsOutTerminalGrace(t *testing.T) {
	now := time.Now()
	fg := fgWithBackup()
	fg.Spec.Backup.Profiles[0].Retention = v1alpha1.BackupRetention{KeepLast: 7, KeepFailed: new(int32(0))}
	justFailed := failedBackup("adhoc-just-failed", fg.Name, "nightly-s3", metav1.NewTime(now.Add(-10*time.Second)))
	oldFailed := failedBackup("adhoc-old-failed", fg.Name, "nightly-s3", metav1.NewTime(now.Add(-time.Hour)))
	r, c := newBackupReconciler(t, fg, &justFailed, &oldFailed)

	requeue, err := r.pruneRetention(context.Background(), &justFailed)
	if err != nil {
		t.Fatalf("pruneRetention: %v", err)
	}
	assertEqualSet(t, listBackupNames(t, r), []string{"adhoc-just-failed"})
	if requeue <= 0 || requeue > terminalRetentionGrace {
		t.Fatalf("requeue = %s, want the rest of the %s grace", requeue, terminalRetentionGrace)
	}

	// The grace has ended: the requeued pass prunes it.
	var b v1alpha1.MysqlBackup
	if err := c.Get(context.Background(), client.ObjectKeyFromObject(&justFailed), &b); err != nil {
		t.Fatal(err)
	}
	ended := metav1.NewTime(now.Add(-terminalRetentionGrace - time.Second))
	b.Status.CompletionTime = &ended
	if err := c.Status().Update(context.Background(), &b); err != nil {
		t.Fatal(err)
	}
	requeue, err = r.pruneRetention(context.Background(), &b)
	if err != nil {
		t.Fatalf("pruneRetention: %v", err)
	}
	assertEqualSet(t, listBackupNames(t, r), nil)
	if requeue != 0 {
		t.Fatalf("requeue = %s after the grace, want 0", requeue)
	}
}
