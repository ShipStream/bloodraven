package controller

import (
	"context"
	"errors"
	"fmt"
	"net/url"
	"strings"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus/testutil"
	v1alpha1 "github.com/shipstream/bloodraven/api/v1alpha1"
	bmetrics "github.com/shipstream/bloodraven/internal/metrics"
	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/tools/record"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"
)

const outcomeStop = "2030-01-01T00:00:00Z"

type verdictFields struct {
	mode, stop, newest, lastFile, sites string
	covered, partial, cross             bool
	files                               int
	version                             string
}

// testVerdict mirrors restore.py's _write_pitr_verdict.
func testVerdict(v verdictFields) string {
	if v.mode == "" {
		v.mode = "timestamp"
	}
	if v.stop == "" {
		v.stop = "2030-01-01 00:00:00"
	}
	if v.version == "" {
		v.version = "1"
	}
	return fmt.Sprintf("BLOODRAVEN_PITR_VERDICT v=%s mode=%s stop=%s targetCovered=%t partial=%t crossSiteUnverified=%t newest=%s files=%d lastFile=%s sites=%s",
		v.version, v.mode, url.PathEscape(v.stop), v.covered, v.partial, v.cross, url.PathEscape(v.newest), v.files, url.PathEscape(v.lastFile), url.PathEscape(v.sites))
}

func outcomeRestore(t *testing.T, pit v1alpha1.PointInTimeSpec) (*MysqlFailoverGroupReconciler, client.Client, *v1alpha1.MysqlFailoverGroup, *batchv1.Job) {
	t.Helper()
	fg := fgInitFromMysqlBackup()
	seed := succeededSeedBackup()
	fg.Spec.Backup.PITR = &v1alpha1.PITRSpec{Enabled: true, ProfileName: seed.Spec.ProfileName}
	if pit.StopDatetime == "" {
		pit.StopDatetime = outcomeStop
	}
	fg.Spec.InitFromBackup.PointInTime = &pit
	r, c := newReconciler(fg, seed, primaryReadyDeployment("lion", fg.Spec.Sites[0].Name), dsnSecret())
	if _, err := r.reconcileRestoreJob(context.Background(), fg); err != nil {
		t.Fatal(err)
	}
	job := &batchv1.Job{}
	if err := c.Get(context.Background(), client.ObjectKey{Namespace: "ns", Name: restoreJobName("lion", fg.Spec.Sites[0].Name)}, job); err != nil {
		t.Fatal(err)
	}
	job.UID = types.UID("job-" + t.Name())
	if err := c.Update(context.Background(), job); err != nil {
		t.Fatal(err)
	}
	return r, c, fg, job
}

func finishJob(t *testing.T, c client.Client, job *batchv1.Job, failed bool) {
	t.Helper()
	condition := batchv1.JobComplete
	if failed {
		condition = batchv1.JobFailed
	}
	job.Status.Conditions = []batchv1.JobCondition{{Type: condition, Status: corev1.ConditionTrue, LastTransitionTime: metav1.Now()}}
	if err := c.Status().Update(context.Background(), job); err != nil {
		t.Fatal(err)
	}
}

func terminated(name string, code int32, message string) corev1.ContainerStatus {
	return corev1.ContainerStatus{Name: name, State: corev1.ContainerState{Terminated: &corev1.ContainerStateTerminated{ExitCode: code, Message: message}}}
}

func jobPodFor(job *batchv1.Job, name string, phase corev1.PodPhase, labels map[string]string) *corev1.Pod {
	all := map[string]string{"job-name": job.Name}
	for k, v := range labels {
		all[k] = v
	}
	return &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: job.Namespace, Labels: all,
		OwnerReferences: []metav1.OwnerReference{{APIVersion: "batch/v1", Kind: "Job", Name: job.Name, UID: job.UID}}},
		Status: corev1.PodStatus{Phase: phase}}
}

func drainEventsMatching(rec *record.FakeRecorder, needle string) int {
	count := 0
	for len(rec.Events) > 0 {
		if strings.Contains(<-rec.Events, needle) {
			count++
		}
	}
	return count
}

// The verdict alone certifies a PITR restore: no Clientset, so no log can be
// read, and the outcome is still decided from pod status.
func TestPITRRestoreVerdictOutcomes(t *testing.T) {
	for _, tc := range []struct {
		name     string
		pit      v1alpha1.PointInTimeSpec
		verdict  string
		noPod    bool
		podPhase corev1.PodPhase
		reason   string
		failed   bool
		warning  string
	}{
		{name: "covered", verdict: testVerdict(verdictFields{covered: true, files: 2, newest: "2029-12-31T23:59:50Z", lastFile: "mysql-bin.000002", sites: "iad"})},
		{name: "covered idle", verdict: testVerdict(verdictFields{covered: true})},
		{name: "partial", pit: v1alpha1.PointInTimeSpec{AllowPartial: true}, verdict: testVerdict(verdictFields{partial: true, files: 1, newest: "2029-12-31T23:59:50Z", sites: "iad"}), reason: "PointInTimePartial", warning: "Warning PointInTimePartial"},
		{name: "unverified", pit: v1alpha1.PointInTimeSpec{AllowUnverifiedCrossSite: true}, verdict: testVerdict(verdictFields{covered: true, cross: true, files: 2, sites: "iad,pdx"}), reason: "PointInTimeCrossSiteUnverified", warning: "staged sites in replay order [iad, pdx]"},
		{name: "fractional stop", pit: v1alpha1.PointInTimeSpec{StopDatetime: "2029-12-31T23:59:59.5Z"}, verdict: testVerdict(verdictFields{covered: true})},
		{name: "missing verdict", verdict: "", reason: "PITRCoverageInvalid", failed: true},
		{name: "old version", verdict: testVerdict(verdictFields{version: "0", covered: true}), reason: "PITRCoverageInvalid", failed: true},
		{name: "stop mismatch", verdict: testVerdict(verdictFields{stop: "2029-01-01 00:00:00", covered: true}), reason: "PITRCoverageInvalid", failed: true},
		{name: "mode mismatch", verdict: testVerdict(verdictFields{mode: "latest", covered: true, files: 1}), reason: "PITRCoverageInvalid", failed: true},
		{name: "unauthorized partial", verdict: testVerdict(verdictFields{partial: true}), reason: "PITRCoverageInvalid", failed: true},
		{name: "unauthorized cross-site", verdict: testVerdict(verdictFields{covered: true, cross: true}), reason: "PITRCoverageInvalid", failed: true},
		{name: "uncovered", verdict: testVerdict(verdictFields{}), reason: "PITRCoverageInvalid", failed: true},
		{name: "pod gone", noPod: true, reason: "PITRCoverageInvalid", failed: true},
		{name: "pod not succeeded", podPhase: corev1.PodRunning, verdict: testVerdict(verdictFields{covered: true}), reason: "PITRCoverageInvalid", failed: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			r, c, fg, job := outcomeRestore(t, tc.pit)
			r.Clientset = nil
			rec := record.NewFakeRecorder(30)
			r.Recorder = rec
			finishJob(t, c, job, false)
			if !tc.noPod {
				phase := tc.podPhase
				if phase == "" {
					phase = corev1.PodSucceeded
				}
				pod := jobPodFor(job, "restore-pod", phase, nil)
				pod.Status.InitContainerStatuses = []corev1.ContainerStatus{terminated(restorePITRInitContainerName, 0, "")}
				pod.Status.ContainerStatuses = []corev1.ContainerStatus{terminated(backupJobContainerName, 0, tc.verdict)}
				if err := c.Create(context.Background(), pod); err != nil {
					t.Fatal(err)
				}
			}
			if _, err := r.reconcileRestoreJob(context.Background(), fg); err != nil {
				t.Fatal(err)
			}
			status := fg.Status.Restore
			if (status.Phase == v1alpha1.BackupPhaseFailed) != tc.failed || status.Reason != tc.reason {
				t.Fatalf("outcome: %+v", status)
			}
			if tc.failed {
				if !strings.HasPrefix(status.Message, "PITRCoverageInvalid: ") || status.PitrPartial || status.PitrCrossSiteUnverified {
					t.Fatalf("invalid verdict leaked into status: %+v", status)
				}
				return
			}
			if status.PitrStopDatetime == "" || status.PitrReplayedBinlogCount < 0 {
				t.Fatalf("verdict fields lost: %+v", status)
			}
			if tc.name == "covered" && (status.PitrNewestEventTime != "2029-12-31T23:59:50Z" || status.PitrReplayedBinlogCount != 2 || status.PitrReplayedBinlogFile != "mysql-bin.000002") {
				t.Fatalf("verdict replay fields lost: %+v", status)
			}
			if tc.warning != "" && drainEventsMatching(rec, tc.warning) == 0 {
				t.Fatalf("missing warning %q", tc.warning)
			}
		})
	}
}

// Failure detail comes from the earliest failing container's termination
// message. A successful step's last line is never the failure.
func TestPITRRestoreFailureDetail(t *testing.T) {
	for _, tc := range []struct {
		name      string
		init      []corev1.ContainerStatus
		main      []corev1.ContainerStatus
		want      string
		notWant   string
		reason    string
		withDecry bool
	}{
		{name: "init refusal", init: []corev1.ContainerStatus{terminated(restorePITRInitContainerName, 2, "PITRArchiveSiteOmitted: omitted manifest keys [binlogs/manifest-old.json]")}, want: "omitted manifest keys", reason: "PITRArchiveSiteOmitted"},
		{name: "main refusal after successful init", init: []corev1.ContainerStatus{terminated(restorePITRInitContainerName, 0, `{"msg":"pitr-download complete"}`)}, main: []corev1.ContainerStatus{terminated(backupJobContainerName, 2, "PITRTargetNotCovered: site iad: the backup already contains transactions committed at or after the target: a:5")}, want: "already contains", reason: "PITRTargetNotCovered"},
		{name: "main killed without message", init: []corev1.ContainerStatus{terminated(restorePITRInitContainerName, 0, `{"msg":"pitr-download complete"}`)}, main: []corev1.ContainerStatus{{Name: backupJobContainerName, State: corev1.ContainerState{Terminated: &corev1.ContainerStateTerminated{ExitCode: 137, Reason: "OOMKilled"}}}}, want: "exited 137 (OOMKilled)", notWant: "pitr-download complete"},
		{name: "decrypt failure", init: []corev1.ContainerStatus{terminated(restorePITRInitContainerName, 0, `{"msg":"pitr-download complete"}`), terminated("decrypt-download", 1, "decrypt: authentication failed")}, want: "authentication failed", notWant: "pitr-download complete"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			r, c, fg, job := outcomeRestore(t, v1alpha1.PointInTimeSpec{})
			r.Clientset = nil
			rec := record.NewFakeRecorder(30)
			r.Recorder = rec
			finishJob(t, c, job, true)
			pod := jobPodFor(job, "restore-pod", corev1.PodFailed, nil)
			pod.Status.InitContainerStatuses = tc.init
			pod.Status.ContainerStatuses = tc.main
			if err := c.Create(context.Background(), pod); err != nil {
				t.Fatal(err)
			}
			for i := 0; i < 3; i++ {
				delay, err := r.reconcileRestoreJob(context.Background(), fg)
				if err != nil || delay != 0 {
					t.Fatalf("a failed restore must not hold group reconciliation: %v %v", delay, err)
				}
			}
			status := fg.Status.Restore
			if status.Phase != v1alpha1.BackupPhaseFailed || !strings.Contains(status.Message, tc.want) || (tc.notWant != "" && strings.Contains(status.Message, tc.notWant)) || status.Reason != tc.reason {
				t.Fatalf("failure detail: %+v", status)
			}
			if events := drainEventsMatching(rec, "RestoreFailed"); events != 1 {
				t.Fatalf("RestoreFailed emitted %d times", events)
			}
		})
	}
}

// A stale pod left by an earlier Job with the same name never supplies the
// replacement Job's verdict.
func TestPITRRestoreIgnoresStalePodOfSameName(t *testing.T) {
	r, c, fg, job := outcomeRestore(t, v1alpha1.PointInTimeSpec{})
	finishJob(t, c, job, false)
	stale := jobPodFor(job, "old-pod", corev1.PodSucceeded, nil)
	stale.OwnerReferences[0].UID = "previous-job"
	stale.Status.ContainerStatuses = []corev1.ContainerStatus{terminated(backupJobContainerName, 0, testVerdict(verdictFields{covered: true}))}
	if err := c.Create(context.Background(), stale); err != nil {
		t.Fatal(err)
	}
	if _, err := r.reconcileRestoreJob(context.Background(), fg); err != nil {
		t.Fatal(err)
	}
	if fg.Status.Restore.Phase != v1alpha1.BackupPhaseFailed || !strings.Contains(fg.Status.Restore.Message, "pod is gone") {
		t.Fatalf("stale pod certified the replacement Job: %+v", fg.Status.Restore)
	}
}

// A pod list error is transient: nothing terminal is recorded.
func TestPITRRestorePodListErrorIsTransient(t *testing.T) {
	r, c, fg, job := outcomeRestore(t, v1alpha1.PointInTimeSpec{})
	finishJob(t, c, job, false)
	r.Client = interceptor.NewClient(c.(client.WithWatch), interceptor.Funcs{List: func(ctx context.Context, c client.WithWatch, list client.ObjectList, opts ...client.ListOption) error {
		if _, ok := list.(*corev1.PodList); ok {
			return errors.New("apiserver unavailable")
		}
		return c.List(ctx, list, opts...)
	}})
	if _, err := r.reconcileRestoreJob(context.Background(), fg); err == nil {
		t.Fatal("pod list error was not surfaced")
	}
	if fg.Status.Restore != nil && (fg.Status.Restore.Phase == v1alpha1.BackupPhaseFailed || fg.Status.Restore.Phase == v1alpha1.BackupPhaseSucceeded) {
		t.Fatalf("transient error recorded a terminal outcome: %+v", fg.Status.Restore)
	}
}

// Render-time refusals keep their stable reason in status.
func TestPITRRestoreBuildFailureKeepsReason(t *testing.T) {
	fg := fgInitFromMysqlBackup()
	seed := succeededSeedBackup()
	seed.Status.GtidExecuted = ""
	fg.Spec.Backup.PITR = &v1alpha1.PITRSpec{Enabled: true, ProfileName: seed.Spec.ProfileName}
	fg.Spec.InitFromBackup.PointInTime = &v1alpha1.PointInTimeSpec{StopDatetime: outcomeStop}
	r, _ := newReconciler(fg, seed, primaryReadyDeployment("lion", fg.Spec.Sites[0].Name), dsnSecret())
	if _, err := r.reconcileRestoreJob(context.Background(), fg); err != nil {
		t.Fatal(err)
	}
	if fg.Status.Restore == nil || fg.Status.Restore.Reason != "PITRBackupBoundUnknown" {
		t.Fatalf("render refusal lost its reason: %+v", fg.Status.Restore)
	}
}

// The dump can hold anything committed before mysqlsh's snapshot, which
// follows the backup Job's start, so a target is admitted only once the
// backup completed.
func TestPITRTargetMustFollowBackupCompletion(t *testing.T) {
	start := metav1.NewTime(time.Date(2030, 1, 1, 0, 0, 0, 0, time.UTC))
	completed := metav1.NewTime(time.Date(2030, 1, 1, 0, 30, 0, 0, time.UTC))
	for _, tc := range []struct {
		stop  string
		allow bool
	}{
		{stop: "2029-12-31T23:59:00Z"},
		{stop: "2030-01-01T00:10:00Z"},
		{stop: "2030-01-01T00:30:00Z", allow: true},
		{stop: " 2030-01-01T01:00:00Z\n", allow: true},
	} {
		status := v1alpha1.MysqlBackupStatus{StartTime: &start, CompletionTime: &completed}
		err := validatePITRBackupBefore(&v1alpha1.PointInTimeSpec{StopDatetime: tc.stop}, status)
		if (err == nil) != tc.allow || (err != nil && pitrFailureReason(err.Error()) != "PITRTargetNotCovered") {
			t.Fatalf("%q: %v", tc.stop, err)
		}
	}
	if err := validatePITRBackupBefore(&v1alpha1.PointInTimeSpec{StopDatetime: outcomeStop}, v1alpha1.MysqlBackupStatus{StartTime: &start}); err == nil {
		t.Fatal("backup without completion time admitted")
	}
	if err := validatePITRBackupBefore(&v1alpha1.PointInTimeSpec{StopDatetime: "2030-01-01T00:00:00.0000000001Z"}, v1alpha1.MysqlBackupStatus{CompletionTime: &start}); err == nil {
		t.Fatal("sub-nanosecond stop accepted")
	}
	vfg := verifyFG()
	vfg.Spec.Backup.PITR = &v1alpha1.PITRSpec{Enabled: true, ProfileName: "nightly-s3"}
	backup := successfulBackup("seed", "lion", "nightly-s3")
	backup.Status.StartTime, backup.Status.CompletionTime = &start, &completed
	v := verifyCR("verify-before-completion", "lion", "nightly-s3")
	v.Spec.PointInTime = &v1alpha1.PointInTimeVerificationSpec{Mode: "timestamp", Timestamp: "2030-01-01T00:10:00Z"}
	if _, err := buildVerificationJob(verificationJobInputs{FailoverGroup: vfg, Profile: vfg.Spec.Backup.Profiles[0], Verification: v, Backup: backup, CredsSecretName: "c", ScriptsConfigMapName: "s"}); err == nil || !strings.Contains(err.Error(), "earlier than the backup's completion") {
		t.Fatalf("verification inside the dump window accepted: %v", err)
	}
}

func TestPITRRenderingCarriesPolicyAndExclusions(t *testing.T) {
	fg := verifyFG()
	fg.Spec.Backup.PITR = &v1alpha1.PITRSpec{Enabled: true, ProfileName: "nightly-s3"}
	pit := &v1alpha1.PointInTimeSpec{StopDatetime: outcomeStop, Sites: []string{"source-old", "removed"}, AllowPartial: true, AllowUnverifiedCrossSite: true, ExcludeGtids: "other:9"}
	frags, err := buildRestorePITRFragmentsFor(fg, pit)
	if err != nil {
		t.Fatal(err)
	}
	assertEnvEquals(t, frags.InitContainer.Env, "BLOODRAVEN_PITR_SITES", "source-old,removed")
	assertEnvEquals(t, frags.InitContainer.Env, "BLOODRAVEN_PITR_ALLOW_PARTIAL", "true")
	assertEnvEquals(t, frags.InitContainer.Env, "BLOODRAVEN_PITR_ALLOW_UNVERIFIED_CROSS_SITE", "true")
	// The planner must exempt exactly what mysqlbinlog --exclude-gtids skips.
	assertEnvEquals(t, frags.InitContainer.Env, "BLOODRAVEN_PITR_EXCLUDE_GTIDS", "other:9")
	assertEnvEquals(t, frags.MainEnv, "BLOODRAVEN_PITR_EXCLUDE_GTIDS", "other:9")
	if frags.InitContainer.TerminationMessagePolicy != corev1.TerminationMessageFallbackToLogsOnError {
		t.Fatal("init refusals need a termination message")
	}
	for _, mode := range []string{"timestamp", "latest"} {
		v := &v1alpha1.MysqlBackupVerification{ObjectMeta: metav1.ObjectMeta{Name: "verify-pitr", Namespace: "ns"}, Spec: v1alpha1.MysqlBackupVerificationSpec{PointInTime: &v1alpha1.PointInTimeVerificationSpec{Mode: mode, Timestamp: outcomeStop, Sites: pit.Sites}}}
		backup := successfulBackup("seed", "lion", "nightly-s3")
		job, err := buildVerificationJob(verificationJobInputs{FailoverGroup: fg, Profile: fg.Spec.Backup.Profiles[0], Verification: v, Backup: backup, CredsSecretName: "c", ScriptsConfigMapName: "s"})
		if err != nil {
			t.Fatal(err)
		}
		spec := job.Spec.Template.Spec
		assertEnvEquals(t, spec.InitContainers[0].Env, "BLOODRAVEN_PITR_MODE", mode)
		assertEnvEquals(t, spec.InitContainers[0].Env, "BLOODRAVEN_PITR_ALLOW_PARTIAL", "false")
		assertEnvEquals(t, spec.InitContainers[0].Env, "BLOODRAVEN_PITR_ALLOW_UNVERIFIED_CROSS_SITE", "false")
		assertEnvEquals(t, spec.Containers[0].Env, "BLOODRAVEN_SOURCE_GTID_EXECUTED", backup.Status.GtidExecuted)
		if spec.Containers[0].TerminationMessagePolicy != corev1.TerminationMessageFallbackToLogsOnError {
			t.Fatal("verify.sh failures need a termination message")
		}
	}
}

func outcomeVerification(t *testing.T, mode string) (*MysqlBackupVerificationReconciler, client.Client, *v1alpha1.MysqlBackupVerification, *batchv1.Job) {
	t.Helper()
	fg := verifyFG()
	fg.Spec.Backup.PITR = &v1alpha1.PITRSpec{Enabled: true, ProfileName: "nightly-s3"}
	backup := successfulBackup("seed", "lion", "nightly-s3")
	v := verifyCR("verify-outcome", "lion", "nightly-s3")
	v.Spec.PointInTime = &v1alpha1.PointInTimeVerificationSpec{Mode: mode, Timestamp: outcomeStop}
	r, c := newVerificationReconciler(t, fg, backup, v, dsnSecret())
	reconcileVerifUntilStable(t, r, v.Name)
	job := &batchv1.Job{}
	if err := c.Get(context.Background(), client.ObjectKey{Namespace: "ns", Name: verificationJobName(v.Name)}, job); err != nil {
		t.Fatal(err)
	}
	if err := c.Get(context.Background(), client.ObjectKeyFromObject(v), v); err != nil {
		t.Fatal(err)
	}
	return r, c, v, job
}

func verifiedReason(v *v1alpha1.MysqlBackupVerification) string {
	for _, cond := range v.Status.Conditions {
		if cond.Type == ConditionVerified {
			return cond.Reason
		}
	}
	return ""
}

func TestPITRVerificationVerdictOutcomes(t *testing.T) {
	latestStop := "9999-12-31 23:59:59"
	for _, tc := range []struct {
		name, mode, verdict, reason string
		failed                      bool
		podFailed                   bool
		init, main                  string
		lag                         bool
	}{
		{name: "timestamp covered", mode: "timestamp", verdict: testVerdict(verdictFields{covered: true, files: 1, newest: "2029-12-31T23:59:50Z", lastFile: "mysql-bin.000007"}), reason: "Succeeded"},
		{name: "latest replay", mode: "latest", verdict: testVerdict(verdictFields{mode: "latest", stop: latestStop, files: 2, newest: "2029-12-31T23:59:50Z", lastFile: "mysql-bin.000007"}), reason: "Succeeded", lag: true},
		{name: "latest without replay", mode: "latest", verdict: testVerdict(verdictFields{mode: "latest", stop: latestStop}), reason: "PITRCoverageInvalid", failed: true},
		{name: "partial is never verified", mode: "timestamp", verdict: testVerdict(verdictFields{partial: true, files: 1}), reason: "PITRCoverageInvalid", failed: true},
		{name: "missing verdict", mode: "timestamp", reason: "PITRCoverageInvalid", failed: true},
		{name: "init refusal", mode: "timestamp", podFailed: true, init: "PITRTargetNotCovered: requested 2030-01-01T00:00:00Z", reason: "PITRTargetNotCovered", failed: true},
		{name: "sanity failure", mode: "latest", podFailed: true, main: "SanityCheckFailed: sanity scalar 0 < minRows 1", reason: "SanityCheckFailed", failed: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			r, c, v, job := outcomeVerification(t, tc.mode)
			r.Clientset = nil
			finishJob(t, c, job, tc.podFailed)
			phase := corev1.PodSucceeded
			if tc.podFailed {
				phase = corev1.PodFailed
			}
			pod := jobPodFor(job, "verify-pod", phase, map[string]string{labelMysqlBackupVerification: v.Name})
			initCode, mainCode := int32(0), int32(0)
			if tc.init != "" {
				initCode = 2
			}
			if tc.main != "" {
				mainCode = 1
			}
			pod.Status.InitContainerStatuses = []corev1.ContainerStatus{terminated(restorePITRInitContainerName, initCode, tc.init)}
			message := tc.verdict
			if tc.main != "" {
				message = tc.main
			}
			if tc.init == "" {
				pod.Status.ContainerStatuses = []corev1.ContainerStatus{terminated(backupJobContainerName, mainCode, message)}
			}
			if err := c.Create(context.Background(), pod); err != nil {
				t.Fatal(err)
			}
			group, profile := v.Spec.FailoverGroupRef.Name, v.Spec.ProfileName
			bmetrics.BackupVerificationReplayLagSeconds.WithLabelValues(group, profile).Set(-1)
			if _, err := r.observeVerificationJob(context.Background(), v, job); err != nil {
				t.Fatal(err)
			}
			if (v.Status.Phase == v1alpha1.VerificationPhaseFailed) != tc.failed || verifiedReason(v) != tc.reason {
				t.Fatalf("verification outcome: %+v", v.Status)
			}
			if !tc.failed && (v.Status.ReplayedThroughBinlog == nil || v.Status.ReplayedThroughBinlog.File != "mysql-bin.000007" || v.Status.ReplayedThroughBinlog.Timestamp == nil) {
				t.Fatalf("replay mark lost: %+v", v.Status.ReplayedThroughBinlog)
			}
			// Replay lag measures archive freshness: only a latest drill sets it.
			lag := testutil.ToFloat64(bmetrics.BackupVerificationReplayLagSeconds.WithLabelValues(group, profile))
			if (lag != -1) != tc.lag {
				t.Fatalf("replay lag gauge %v for %s", lag, tc.name)
			}
		})
	}
}

// Verification has no retry once terminal, so a pod list error must leave
// it non-terminal for the next reconcile.
func TestPITRVerificationPodListErrorIsTransient(t *testing.T) {
	r, c, v, job := outcomeVerification(t, "timestamp")
	finishJob(t, c, job, true)
	r.Client = interceptor.NewClient(c.(client.WithWatch), interceptor.Funcs{List: func(ctx context.Context, c client.WithWatch, list client.ObjectList, opts ...client.ListOption) error {
		if _, ok := list.(*corev1.PodList); ok {
			return errors.New("apiserver unavailable")
		}
		return c.List(ctx, list, opts...)
	}})
	if _, err := r.observeVerificationJob(context.Background(), v, job); err == nil {
		t.Fatal("pod list error was not surfaced")
	}
	if v.Status.Phase == v1alpha1.VerificationPhaseFailed || v.Status.Phase == v1alpha1.VerificationPhaseSucceeded {
		t.Fatalf("transient error recorded a terminal outcome: %+v", v.Status)
	}
}

func TestPITRFailureReasonPrefersFirstToken(t *testing.T) {
	for message, want := range map[string]string{
		"PITRArchiveSiteOmitted: add the source site for replay validation; overlapping streams are refused with PITRCrossSiteOrder": "PITRArchiveSiteOmitted",
		"PITRCoverageInvalid: PITRTargetNotCovered: missing upper coverage":                                                          "PITRCoverageInvalid",
		`{"msg":"download site binlogs","error":"PITRCrossSiteOrder: east and iad overlap"}`:                                         "PITRCrossSiteOrder",
	} {
		if got := pitrFailureReason(message); got != want {
			t.Errorf("%q: got %s, want %s", message, got, want)
		}
	}
}

// A restore can read another group's archive through its own profile, so the
// restored group archives into its own prefix.
func TestPITRArchiveProfileSeparatesPrefixes(t *testing.T) {
	fg := fgInitFromMysqlBackup()
	source := *fg.Spec.Backup.Profiles[0].DeepCopy()
	source.Name = "source-archive"
	source.Storage.S3.Prefix = "source-group"
	fg.Spec.Backup.Profiles = append(fg.Spec.Backup.Profiles, source)
	fg.Spec.Backup.PITR = nil
	pit := &v1alpha1.PointInTimeSpec{StopDatetime: outcomeStop, Sites: []string{"old-east"}, ArchiveProfileName: "source-archive"}
	frags, err := buildRestorePITRFragmentsFor(fg, pit)
	if err != nil {
		t.Fatal(err)
	}
	assertEnvEquals(t, frags.InitContainer.Env, "BLOODRAVEN_PITR_MANIFEST_PREFIX", "source-group/binlogs")
	pit.ArchiveProfileName = "missing"
	if _, err := buildRestorePITRFragmentsFor(fg, pit); err == nil {
		t.Fatal("unknown archive profile accepted")
	}

	// Reading foreign sites from the group's own prefix warns.
	_, _, fg2, _ := outcomeRestore(t, v1alpha1.PointInTimeSpec{Sites: []string{"old-east"}})
	if foreign := pitrForeignReplaySites(fg2); len(foreign) != 1 || foreign[0] != "old-east" {
		t.Fatalf("foreign sites: %v", foreign)
	}
}
