package controller

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"net/url"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus/testutil"
	v1alpha1 "github.com/shipstream/bloodraven/api/v1alpha1"
	bmetrics "github.com/shipstream/bloodraven/internal/metrics"
	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	apiequality "k8s.io/apimachinery/pkg/api/equality"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/tools/record"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"
)

const (
	outcomeStop          = "2030-01-01T00:00:00Z"
	outcomeStopCanonical = "2030-01-01T00:00:00.000000Z"
	outcomeArchiveID     = "ar1-abcdefghijklmnopqrstuvwxyz"
	outcomeSourceUID     = "source-group-uid"
	outcomeExecuted      = "aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee:1-42"
)

// verdictFields is one restore.py verdict; zero values take the defaults of
// a covered timestamp replay of two segments.
type verdictFields struct {
	version, mode, stop, outcome, archive, encryption string
	sourceUID, lastFile, newest, sites, executed      string
	segments, files, transactions                     int
	divergent, onHistory                              int
	onHistorySet                                      string
	digestOnly, noResult, badDigest, noSegments       bool
}

// testVerdict mirrors restore.py's _pitr_verdict_message.
func testVerdict(v verdictFields) string {
	def := func(p *string, d string) {
		if *p == "" {
			*p = d
		}
	}
	def(&v.version, "2")
	def(&v.mode, "timestamp")
	if v.mode == "timestamp" {
		def(&v.stop, outcomeStopCanonical)
		def(&v.outcome, "TargetCovered")
	} else {
		def(&v.outcome, "ArchiveHead")
	}
	def(&v.archive, outcomeArchiveID)
	def(&v.encryption, "None")
	def(&v.sourceUID, outcomeSourceUID)
	def(&v.executed, outcomeExecuted)
	if !v.noSegments && v.segments == 0 {
		v.segments, v.files, v.transactions = 2, 2, 32
		def(&v.lastFile, "iad/mysql-bin.000007")
		def(&v.newest, "2029-12-31T23:59:50.123456Z")
		def(&v.sites, "dfw,iad")
	}
	sum := sha256.Sum256([]byte(v.executed))
	digest := hex.EncodeToString(sum[:])
	if v.badDigest {
		digest = strings.Repeat("0", 64)
	}
	lines := []string{fmt.Sprintf("BLOODRAVEN_PITR_VERDICT v=%s mode=%s stop=%s outcome=%s archive=%s encryptionMode=%s sourceUID=%s sourceNamespace=ns sourceName=src segments=%d files=%d transactions=%d lastFile=%s newest=%s divergent=%d onHistoryExcluded=%d executedSha256=%s sites=%s",
		v.version, v.mode, url.QueryEscape(v.stop), v.outcome, v.archive, v.encryption, v.sourceUID, v.segments, v.files, v.transactions,
		url.QueryEscape(v.lastFile), url.QueryEscape(v.newest), v.divergent, v.onHistory, digest, url.QueryEscape(v.sites))}
	switch {
	case v.noResult:
	case v.digestOnly:
		lines = append(lines, "BLOODRAVEN_PITR_RESULT v=1 executedSha256="+digest)
	default:
		lines = append(lines, "BLOODRAVEN_PITR_RESULT v=1 executed="+v.executed)
	}
	if v.onHistorySet != "" {
		lines = append(lines, "BLOODRAVEN_PITR_ON_HISTORY_EXCLUSIONS gtids="+v.onHistorySet)
	}
	return strings.Join(lines, "\n")
}

func outcomeRestore(t *testing.T, pit v1alpha1.PointInTimeSpec) (*MysqlFailoverGroupReconciler, client.Client, *v1alpha1.MysqlFailoverGroup, *batchv1.Job) {
	t.Helper()
	fg := fgInitFromMysqlBackup()
	seedPublished(fg)
	seed := succeededSeedBackup()
	seed.UID = "uid-seed"
	seed.Status.FailoverGroupUID = outcomeSourceUID
	fg.Spec.Backup.PITR = &v1alpha1.PITRSpec{Enabled: true, ProfileName: seed.Spec.ProfileName}
	if pit.StopDatetime == "" {
		pit.StopDatetime = outcomeStop
	}
	if pit.ArchiveProfileName == "" {
		pit.ArchiveProfileName = addSourceArchiveProfile(fg)
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
		warnings []string
		check    func(t *testing.T, res *v1alpha1.PointInTimeResult)
	}{
		{name: "covered", verdict: testVerdict(verdictFields{}), check: func(t *testing.T, res *v1alpha1.PointInTimeResult) {
			want := &v1alpha1.PointInTimeResult{
				Mode: v1alpha1.PointInTimeModeTimestamp, Outcome: v1alpha1.PointInTimeTargetCovered,
				StopTime: microPtr(time.Date(2030, 1, 1, 0, 0, 0, 0, time.UTC)),
				Archive: &v1alpha1.PITRArchiveIdentity{ArchiveID: outcomeArchiveID, Source: v1alpha1.PITRArchiveSource{Namespace: "ns", Name: "src", UID: outcomeSourceUID},
					EncryptionMode: v1alpha1.PITRArchiveEncryptionNone},
				ReplaySites: []string{"dfw", "iad"}, SegmentCount: 2, ReplayedBinlogCount: 2,
				LastReplayedBinlog:       &v1alpha1.ReplayedBinlogRef{Site: "iad", Name: "mysql-bin.000007"},
				ReplayedTransactionCount: 32, NewestReplayedCommitTime: microPtr(time.Date(2029, 12, 31, 23, 59, 50, 123456000, time.UTC)),
				GtidExecuted: outcomeExecuted, GtidExecutedSHA256: sha256Hex(outcomeExecuted),
			}
			got := res.DeepCopy()
			if got.Archive != nil {
				got.Archive.Destination = ""
			}
			if !equalResults(got, want) {
				t.Fatalf("result:\n got %+v\nwant %+v", got, want)
			}
			if res.Archive.Destination == "" || !strings.HasPrefix(res.Archive.Destination, "s3://") {
				t.Fatalf("destination not taken from the rendered Job: %q", res.Archive.Destination)
			}
		}},
		{name: "covered with no replay", verdict: testVerdict(verdictFields{noSegments: true})},
		{name: "digest only", verdict: testVerdict(verdictFields{digestOnly: true}), check: func(t *testing.T, res *v1alpha1.PointInTimeResult) {
			if res.GtidExecuted != "" || res.GtidExecutedSHA256 != sha256Hex(outcomeExecuted) {
				t.Fatalf("digest-only result: %+v", res)
			}
		}},
		{name: "partial", pit: v1alpha1.PointInTimeSpec{AllowPartial: true}, verdict: testVerdict(verdictFields{outcome: "Partial"}), reason: "PointInTimePartial", warnings: []string{"Warning PointInTimePartial"}},
		{name: "divergent and on-history exclusions", pit: v1alpha1.PointInTimeSpec{ExcludeGtids: "3e11fa47-71ca-11e1-9e33-c80aa9429562:1-3"},
			verdict:  testVerdict(verdictFields{divergent: 2, onHistory: 1, onHistorySet: "3e11fa47-71ca-11e1-9e33-c80aa9429562:3"}),
			warnings: []string{"Warning PITRDivergentHistoryExcluded replay excluded 2 transactions", "Warning PITROnHistoryTransactionsExcluded excluded 3e11fa47-71ca-11e1-9e33-c80aa9429562:3 precedes"},
			check: func(t *testing.T, res *v1alpha1.PointInTimeResult) {
				if res.ExcludedDivergentTransactionCount != 2 || res.OnHistoryExclusionCount != 1 || res.OnHistoryExclusions != "3e11fa47-71ca-11e1-9e33-c80aa9429562:3" {
					t.Fatalf("exclusion counts: %+v", res)
				}
			}},
		{name: "MySQL-form stop", pit: v1alpha1.PointInTimeSpec{StopDatetime: "2029-12-31 23:59:59"}, verdict: testVerdict(verdictFields{stop: "2029-12-31T23:59:59.000000Z"})},
		{name: "missing verdict", verdict: "", reason: "PITRCoverageInvalid", failed: true},
		{name: "version 1", verdict: testVerdict(verdictFields{version: "1"}), reason: "PITRCoverageInvalid", failed: true},
		{name: "stop mismatch", verdict: testVerdict(verdictFields{stop: "2029-01-01T00:00:00.000000Z"}), reason: "PITRCoverageInvalid", failed: true},
		{name: "fractional echo of a whole-second stop", pit: v1alpha1.PointInTimeSpec{StopDatetime: "2029-12-31T23:59:59Z"}, verdict: testVerdict(verdictFields{stop: "2029-12-31T23:59:59.500000Z"}), reason: "PITRCoverageInvalid", failed: true},
		{name: "mode mismatch", verdict: testVerdict(verdictFields{mode: "latest"}), reason: "PITRCoverageInvalid", failed: true},
		{name: "unauthorized partial", verdict: testVerdict(verdictFields{outcome: "Partial"}), reason: "PITRCoverageInvalid", failed: true},
		{name: "partial without replay", pit: v1alpha1.PointInTimeSpec{AllowPartial: true}, verdict: testVerdict(verdictFields{outcome: "Partial", noSegments: true}), reason: "PITRCoverageInvalid", failed: true},
		{name: "archive head in timestamp mode", verdict: testVerdict(verdictFields{outcome: "ArchiveHead"}), reason: "PITRCoverageInvalid", failed: true},
		{name: "foreign source", verdict: testVerdict(verdictFields{sourceUID: "another-group"}), reason: "PITRCoverageInvalid", failed: true},
		{name: "encryption mismatch", verdict: testVerdict(verdictFields{encryption: "Passphrase"}), reason: "PITRCoverageInvalid", failed: true},
		{name: "malformed archive", verdict: testVerdict(verdictFields{archive: "ar1-short"}), reason: "PITRCoverageInvalid", failed: true},
		{name: "result line missing", verdict: testVerdict(verdictFields{noResult: true}), reason: "PITRCoverageInvalid", failed: true},
		{name: "result disagrees with digest", verdict: testVerdict(verdictFields{badDigest: true}), reason: "PITRCoverageInvalid", failed: true},
		{name: "files without segments", verdict: testVerdict(verdictFields{noSegments: true, files: 1}), reason: "PITRCoverageInvalid", failed: true},
		{name: "lastFile without site", verdict: testVerdict(verdictFields{lastFile: "mysql-bin.000007"}), reason: "PITRCoverageInvalid", failed: true},
		{name: "pod gone", noPod: true, reason: "PITRCoverageInvalid", failed: true},
		{name: "pod not succeeded", podPhase: corev1.PodRunning, verdict: testVerdict(verdictFields{}), reason: "PITRCoverageInvalid", failed: true},
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
				pod.Status.InitContainerStatuses = []corev1.ContainerStatus{terminated(restorePITRDumpMetaContainerName, 0, ""), terminated(restorePITRInitContainerName, 0, "")}
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
				if !strings.HasPrefix(status.Message, "PITRCoverageInvalid: ") || status.PointInTime != nil {
					t.Fatalf("invalid verdict leaked into status: %+v", status)
				}
				return
			}
			if status.PointInTime == nil || status.PointInTime.Archive == nil {
				t.Fatalf("verdict result lost: %+v", status)
			}
			if tc.check != nil {
				tc.check(t, status.PointInTime)
			}
			events := []string{}
			for len(rec.Events) > 0 {
				events = append(events, <-rec.Events)
			}
			for _, want := range tc.warnings {
				found := false
				for _, e := range events {
					found = found || strings.HasPrefix(e, want)
				}
				if !found {
					t.Fatalf("missing event %q in %v", want, events)
				}
			}
			if len(tc.warnings) == 0 {
				for _, e := range events {
					if strings.HasPrefix(e, "Warning PointInTime") {
						t.Fatalf("unexpected warning %q", e)
					}
				}
			}
		})
	}
}

func microPtr(t time.Time) *metav1.MicroTime {
	m := metav1.NewMicroTime(t)
	return &m
}

func sha256Hex(s string) string {
	sum := sha256.Sum256([]byte(s))
	return hex.EncodeToString(sum[:])
}

func equalResults(a, b *v1alpha1.PointInTimeResult) bool {
	return apiequality.Semantic.DeepEqual(a, b)
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
		{name: "dump-meta refusal", init: []corev1.ContainerStatus{terminated(restorePITRDumpMetaContainerName, 2, "PITRBackupIdentityUnknown: the dump has no backup stamp; take a new backup")}, want: "no backup stamp", reason: "PITRBackupIdentityUnknown"},
		{name: "init refusal", init: []corev1.ContainerStatus{terminated(restorePITRDumpMetaContainerName, 0, ""), terminated(restorePITRInitContainerName, 2, "PITRArchiveDivergentHistory: 2 archived transactions committed between the backup's newest transaction and the target are not in the history of a:9")}, want: "not in the history", reason: "PITRArchiveDivergentHistory"},
		{name: "main refusal after successful init", init: []corev1.ContainerStatus{terminated(restorePITRInitContainerName, 0, `{"msg":"pitr plan"}`)}, main: []corev1.ContainerStatus{terminated(backupJobContainerName, 2, "PITRReplayMismatch: R6: replay left gtid_executed a:1-9 but the target history minus excludeGtids is a:1-10")}, want: "R6", reason: "PITRReplayMismatch"},
		{name: "mysqlbinlog failure", init: []corev1.ContainerStatus{terminated(restorePITRInitContainerName, 0, "")}, main: []corev1.ContainerStatus{terminated(backupJobContainerName, 2, "BLOODRAVEN_PITR_FAILED: mysqlbinlog exit=1 segment=1")}, want: "segment=1", reason: "BLOODRAVEN_PITR_FAILED"},
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
	stale.Status.ContainerStatuses = []corev1.ContainerStatus{terminated(backupJobContainerName, 0, testVerdict(verdictFields{}))}
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

// Render-time refusals keep their stable reason in status (§7.1
// reconcile tokens): an unparsable target, a schema-filtered load, and a
// target before the backup finished.
func TestPITRRestoreBuildFailureKeepsReason(t *testing.T) {
	for _, tc := range []struct {
		name   string
		mutate func(fg *v1alpha1.MysqlFailoverGroup)
		seed   func(mb *v1alpha1.MysqlBackup)
		reason string
	}{
		// B4: a backup with no recorded source identity has no stamp;
		// refuse before scheduling dump-meta.
		{name: "backup without dump identity", mutate: func(*v1alpha1.MysqlFailoverGroup) {}, seed: func(mb *v1alpha1.MysqlBackup) {
			mb.Status.FailoverGroupUID = ""
		}, reason: "PITRBackupIdentityUnknown"},
		{name: "archive profile missing", mutate: func(fg *v1alpha1.MysqlFailoverGroup) {
			fg.Spec.InitFromBackup.PointInTime.ArchiveProfileName = ""
		}, reason: "PITRTargetInvalid"},
		{name: "archive profile unknown", mutate: func(fg *v1alpha1.MysqlFailoverGroup) {
			fg.Spec.InitFromBackup.PointInTime.ArchiveProfileName = "ghost"
		}, reason: "PITRTargetInvalid"},
		{name: "seven fractional digits", mutate: func(fg *v1alpha1.MysqlFailoverGroup) {
			fg.Spec.InitFromBackup.PointInTime.StopDatetime = "2030-01-01T00:00:00.1234567Z"
		}, reason: "PITRTargetInvalid"},
		{name: "garbage target", mutate: func(fg *v1alpha1.MysqlFailoverGroup) {
			fg.Spec.InitFromBackup.PointInTime.StopDatetime = "yesterday"
		}, reason: "PITRTargetInvalid"},
		{name: "include filter", mutate: func(fg *v1alpha1.MysqlFailoverGroup) {
			fg.Spec.InitFromBackup.LoadOptions = &v1alpha1.LoadOptions{IncludeSchemas: []string{"app"}}
		}, reason: "PITRBackupFiltered"},
		{name: "exclude filter", mutate: func(fg *v1alpha1.MysqlFailoverGroup) {
			fg.Spec.InitFromBackup.LoadOptions = &v1alpha1.LoadOptions{ExcludeSchemas: []string{"audit"}}
		}, reason: "PITRBackupFiltered"},
		// A target before the backup is refused PITRBackupAfterTarget by
		// dump-meta against the dump's serverClock.end, not here (F034).
		{name: "shared destination", mutate: func(fg *v1alpha1.MysqlFailoverGroup) {
			fg.Spec.InitFromBackup.PointInTime.ArchiveProfileName = fg.Spec.Backup.PITR.ProfileName
		}, reason: "PITRArchiveDestinationShared"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			fg := fgInitFromMysqlBackup()
			seedPublished(fg)
			seed := succeededSeedBackup()
			fg.Spec.Backup.PITR = &v1alpha1.PITRSpec{Enabled: true, ProfileName: seed.Spec.ProfileName}
			fg.Spec.InitFromBackup.PointInTime = &v1alpha1.PointInTimeSpec{StopDatetime: outcomeStop, ArchiveProfileName: addSourceArchiveProfile(fg)}
			tc.mutate(fg)
			if tc.seed != nil {
				tc.seed(seed)
			}
			r, _ := newReconciler(fg, seed, primaryReadyDeployment("lion", fg.Spec.Sites[0].Name), dsnSecret())
			if _, err := r.reconcileRestoreJob(context.Background(), fg); err != nil {
				t.Fatal(err)
			}
			if fg.Status.Restore == nil || fg.Status.Restore.Reason != tc.reason || !strings.HasPrefix(fg.Status.Restore.Message, tc.reason+": ") {
				t.Fatalf("render refusal: %+v", fg.Status.Restore)
			}
		})
	}
}

// The parser accepts exactly the CRD pattern: whole seconds (C2 / A-39).
func TestParsePITRTarget(t *testing.T) {
	for in, want := range map[string]string{
		"2030-01-01T00:00:00Z":      "2030-01-01T00:00:00.000000Z",
		"2030-01-01 00:00:00":       "2030-01-01T00:00:00.000000Z",
		"2030-01-01T00:00:00":       "2030-01-01T00:00:00.000000Z",
		"2030-01-01T02:00:00+02:00": "2030-01-01T00:00:00.000000Z",
		"2029-12-31T19:00:01-05:00": "2030-01-01T00:00:01.000000Z",
	} {
		got, err := parsePITRTarget(in)
		if err != nil || canonicalPITRTarget(got) != want {
			t.Errorf("%q: got %v %v, want %s", in, canonicalPITRTarget(got), err, want)
		}
	}
	for _, bad := range []string{"", "2030-01-01", "2030-01-01T00:00:00.1234567Z",
		"2030-01-01 00:00:00.123456", "2030-01-01T02:00:00.1+02:00", "2030-01-01T00:00:00.5Z", "2030-01-01T00:00:00+25:00", "2030-01-01T00:00:00+05:99", "2030-01-01T00:00:00,5Z", " 2030-01-01T00:00:00Z", "2030-01-01T00:00:00+0200", "2030-13-01T00:00:00Z"} {
		if _, err := parsePITRTarget(bad); err == nil {
			t.Errorf("%q accepted", bad)
		}
	}
}

// F034: the controller does not bound the target by status.completionTime,
// which includes the upload of an encrypted dump and can be hours after the
// dump ended; dump-meta enforces D-44 against the dump's serverClock.end.
// A target between the two is rendered.
func TestPITRTargetNotBoundedByBackupCompletion(t *testing.T) {
	completed := metav1.NewTime(time.Date(2030, 1, 1, 14, 0, 0, 0, time.UTC))
	vfg := verifyFG()
	vfg.Spec.Backup.PITR = &v1alpha1.PITRSpec{Enabled: true, ProfileName: "nightly-s3"}
	backup := successfulBackup("seed", "lion", "nightly-s3")
	backup.Status.CompletionTime = &completed
	v := verifyCR("verify-before-completion", "lion", "nightly-s3")
	v.Spec.PointInTime = &v1alpha1.PointInTimeVerificationSpec{Mode: "timestamp", Timestamp: "2030-01-01T13:00:00Z"}
	job, err := buildVerificationJob(verificationJobInputs{FailoverGroup: vfg, Profile: vfg.Spec.Backup.Profiles[0], Verification: v, Backup: backup, CredsSecretName: "c", ScriptsConfigMapName: "s"})
	if err != nil {
		t.Fatalf("target after the dump but before the upload finished refused: %v", err)
	}
	if got := renderedPITRRequest(job).Stop; got != "2030-01-01T13:00:00.000000Z" {
		t.Fatalf("rendered stop %q", got)
	}
}

func TestPITRRenderingCarriesPolicyAndExclusions(t *testing.T) {
	fg := verifyFG()
	fg.Spec.Backup.PITR = &v1alpha1.PITRSpec{Enabled: true, ProfileName: "nightly-s3"}
	req, err := pitrRequestFromSpec(&v1alpha1.PointInTimeSpec{StopDatetime: "2030-01-01 00:00:25", AllowPartial: true, ExcludeGtids: " 3e11fa47-71ca-11e1-9e33-c80aa9429562:9 "})
	if err != nil {
		t.Fatal(err)
	}
	frags, err := buildRestorePITRFragmentsFor(fg, req)
	if err != nil {
		t.Fatal(err)
	}
	// The request is rendered identically for pitr-download and restore.py,
	// with the target in its one canonical spelling.
	for _, env := range [][]corev1.EnvVar{frags.InitContainer.Env, frags.MainEnv} {
		assertEnvEquals(t, env, "BLOODRAVEN_PITR_MODE", "timestamp")
		assertEnvEquals(t, env, "BLOODRAVEN_PITR_STOP_DATETIME", "2030-01-01T00:00:25.000000Z")
		assertEnvEquals(t, env, "BLOODRAVEN_PITR_ALLOW_PARTIAL", "true")
		assertEnvEquals(t, env, "BLOODRAVEN_PITR_EXCLUDE_GTIDS", "3e11fa47-71ca-11e1-9e33-c80aa9429562:9")
	}
	assertEnvEquals(t, frags.InitContainer.Env, "BLOODRAVEN_PITR_DUMP_META", "/pitr-dump-meta/dump.json")
	// D-9 and B from dump.json: no site list, cross-site switch or start set.
	for _, removed := range []string{"BLOODRAVEN_PITR_SITES", "BLOODRAVEN_PITR_ALLOW_UNVERIFIED_CROSS_SITE", "BLOODRAVEN_PITR_START_GTIDS"} {
		if _, ok := envValue(append(append([]corev1.EnvVar{}, frags.InitContainer.Env...), frags.MainEnv...), removed); ok {
			t.Fatalf("%s is still rendered", removed)
		}
	}
	if frags.InitContainer.TerminationMessagePolicy != corev1.TerminationMessageFallbackToLogsOnError {
		t.Fatal("init refusals need a termination message")
	}
	for _, mode := range []string{"timestamp", "latest"} {
		v := &v1alpha1.MysqlBackupVerification{ObjectMeta: metav1.ObjectMeta{Name: "verify-pitr", Namespace: "ns"}, Spec: v1alpha1.MysqlBackupVerificationSpec{PointInTime: &v1alpha1.PointInTimeVerificationSpec{Mode: mode, Timestamp: outcomeStop, ExcludeGtids: "3e11fa47-71ca-11e1-9e33-c80aa9429562:1"}}}
		backup := successfulBackup("seed", "lion", "nightly-s3")
		backup.Status.FailoverGroupUID = outcomeSourceUID
		job, err := buildVerificationJob(verificationJobInputs{FailoverGroup: fg, Profile: fg.Spec.Backup.Profiles[0], Verification: v, Backup: backup, CredsSecretName: "c", ScriptsConfigMapName: "s"})
		if err != nil {
			t.Fatal(err)
		}
		spec := job.Spec.Template.Spec
		dl := containerEnv(spec.InitContainers, restorePITRInitContainerName)
		if dl["BLOODRAVEN_PITR_MODE"] != mode || dl["BLOODRAVEN_PITR_ALLOW_PARTIAL"] != "false" || dl["BLOODRAVEN_PITR_EXCLUDE_GTIDS"] != "3e11fa47-71ca-11e1-9e33-c80aa9429562:1" {
			t.Fatalf("%s pitr-download env: %v", mode, dl)
		}
		if _, ok := dl["BLOODRAVEN_PITR_STOP_DATETIME"]; ok != (mode == "timestamp") {
			t.Fatalf("%s: stop rendered=%v", mode, ok)
		}
		main := containerEnv(spec.Containers, spec.Containers[0].Name)
		if main["BLOODRAVEN_PITR_EXPECTED_SOURCE_UID"] != outcomeSourceUID || main["BLOODRAVEN_VERIFY_PITR_MODE"] != mode {
			t.Fatalf("%s main env: %v", mode, main)
		}
		if dm := containerEnv(spec.InitContainers, restorePITRDumpMetaContainerName); dm["BLOODRAVEN_DUMP_EXPECTED_SOURCE_UID"] != outcomeSourceUID {
			t.Fatalf("%s dump-meta env: %v", mode, dm)
		}
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
	backup.Status.FailoverGroupUID = outcomeSourceUID
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
	for _, tc := range []struct {
		name, mode, verdict, reason string
		failed                      bool
		podFailed                   bool
		init, main                  string
		lag                         bool
		events                      []string
	}{
		{name: "timestamp covered", mode: "timestamp", verdict: testVerdict(verdictFields{}), reason: "Succeeded"},
		{name: "latest replay", mode: "latest", verdict: testVerdict(verdictFields{mode: "latest"}), reason: "Succeeded", lag: true},
		// F150: a latest plan succeeds only with no divergent set (loss
		// evidence refuses PITRArchiveDivergentHistory), so a verdict that
		// reports one is not certified.
		{name: "latest reporting divergent transactions", mode: "latest", verdict: testVerdict(verdictFields{mode: "latest", divergent: 3}), reason: "PITRCoverageInvalid", failed: true},
		{name: "latest without replay", mode: "latest", verdict: testVerdict(verdictFields{mode: "latest", noSegments: true}), reason: "PITRCoverageInvalid", failed: true},
		{name: "partial is never verified", mode: "timestamp", verdict: testVerdict(verdictFields{outcome: "Partial"}), reason: "PITRCoverageInvalid", failed: true},
		{name: "latest carries no stop", mode: "latest", verdict: testVerdict(verdictFields{mode: "latest", stop: outcomeStopCanonical}), reason: "PITRCoverageInvalid", failed: true},
		{name: "missing verdict", mode: "timestamp", reason: "PITRCoverageInvalid", failed: true},
		{name: "init refusal", mode: "timestamp", podFailed: true, init: "PITRTargetNotCovered: the archive holds no transaction at or after 2030-01-01T00:00:00.000000Z", reason: "PITRTargetNotCovered", failed: true},
		{name: "sanity failure", mode: "latest", podFailed: true, main: "SanityCheckFailed: sanity scalar 0 < minRows 1", reason: "SanityCheckFailed", failed: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			r, c, v, job := outcomeVerification(t, tc.mode)
			r.Clientset = nil
			rec := record.NewFakeRecorder(30)
			r.Recorder = rec
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
			pod.Status.InitContainerStatuses = []corev1.ContainerStatus{terminated(restorePITRDumpMetaContainerName, 0, ""), terminated(restorePITRInitContainerName, initCode, tc.init)}
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
			if tc.failed {
				if v.Status.PointInTime != nil {
					t.Fatalf("failed verification kept a result: %+v", v.Status.PointInTime)
				}
			} else {
				res := v.Status.PointInTime
				if res == nil || res.GtidExecuted != outcomeExecuted || res.Archive == nil || res.Archive.ArchiveID != outcomeArchiveID {
					t.Fatalf("result: %+v", res)
				}
				wantMode := v1alpha1.PointInTimeModeTimestamp
				if tc.mode == "latest" {
					wantMode = v1alpha1.PointInTimeModeLatest
				}
				if res.Mode != wantMode || (res.StopTime == nil) != (tc.mode == "latest") {
					t.Fatalf("result mode/stop: %+v", res)
				}
				if v.Status.ReplayedThroughBinlog == nil || v.Status.ReplayedThroughBinlog.File != "mysql-bin.000007" || v.Status.ReplayedThroughBinlog.Timestamp == nil {
					t.Fatalf("replay mark lost: %+v", v.Status.ReplayedThroughBinlog)
				}
			}
			for _, want := range tc.events {
				if drainEventsMatching(rec, want) == 0 {
					t.Fatalf("missing event %q", want)
				}
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
		"PITRArchiveDivergentHistory: 3 archived transactions ...; or use a backup taken after PITRTargetNotCovered": "PITRArchiveDivergentHistory",
		"PITRCoverageInvalid: PITRTargetNotCovered: missing upper coverage":                                          "PITRCoverageInvalid",
		`{"msg":"download site binlogs","error":"PITRArchiveGap: the target history needs a:5"}`:                     "PITRArchiveGap",
		"BLOODRAVEN_PITR_FAILED: mysqlbinlog exit=1 segment=0":                                                       "BLOODRAVEN_PITR_FAILED",
		"container restore exited 137 (OOMKilled)":                                                                   "",
		"PITRArchiveGapish is not a token":                                                                           "",
		"PITRCrossSiteOrder: removed in v2":                                                                          "",
	} {
		if got := pitrFailureReason(message); got != want {
			t.Errorf("%q: got %q, want %q", message, got, want)
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
	req := &pitrReplayRequest{Mode: "timestamp", Stop: outcomeStopCanonical, ArchiveProfileName: "source-archive"}
	frags, err := buildRestorePITRFragmentsFor(fg, req)
	if err != nil {
		t.Fatal(err)
	}
	assertEnvEquals(t, frags.InitContainer.Env, "BLOODRAVEN_PITR_MANIFEST_PREFIX", "source-group/binlogs")
	req.ArchiveProfileName = "missing"
	if _, err := buildRestorePITRFragmentsFor(fg, req); err == nil {
		t.Fatal("unknown archive profile accepted")
	}
}

// Round-1 env additions (P2/P3): pitr-download gets the staging emptyDir's
// sizeLimit and its own memory limit when they are set, and neither when
// they are not (it then falls back to statfs and the cgroup limit).
func TestPITRDownloadBudgetEnv(t *testing.T) {
	limit := resource.MustParse("5Gi")
	for _, tc := range []struct {
		name          string
		staging       *resource.Quantity
		resources     corev1.ResourceRequirements
		wantStaging   string
		wantMemoryEnv string
	}{
		{name: "unset"},
		{name: "staging limit", staging: &limit, wantStaging: "5368709120"},
		{name: "memory limit", resources: corev1.ResourceRequirements{Limits: corev1.ResourceList{corev1.ResourceMemory: resource.MustParse("1Gi")}}, wantMemoryEnv: "1073741824"},
		{name: "memory request only", resources: corev1.ResourceRequirements{Requests: corev1.ResourceList{corev1.ResourceMemory: resource.MustParse("1Gi")}}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			fg := verifyFG()
			fg.Spec.Backup.PITR = &v1alpha1.PITRSpec{Enabled: true, ProfileName: "nightly-s3"}
			fg.Spec.Backup.StagingVolumeSizeLimit = tc.staging
			fg.Spec.Backup.Resources = tc.resources
			req, err := pitrRequestFromSpec(&v1alpha1.PointInTimeSpec{StopDatetime: "2030-01-01T00:00:00Z"})
			if err != nil {
				t.Fatal(err)
			}
			frags, err := buildRestorePITRFragmentsFor(fg, req)
			if err != nil {
				t.Fatal(err)
			}
			env := envMap(frags.InitContainer.Env)
			if got, ok := env["BLOODRAVEN_PITR_STAGING_LIMIT_BYTES"]; got != tc.wantStaging || ok != (tc.wantStaging != "") {
				t.Fatalf("staging limit env = %q (set=%t), want %q", got, ok, tc.wantStaging)
			}
			if got, ok := env["BLOODRAVEN_PITR_MEMORY_LIMIT_BYTES"]; got != tc.wantMemoryEnv || ok != (tc.wantMemoryEnv != "") {
				t.Fatalf("memory limit env = %q (set=%t), want %q", got, ok, tc.wantMemoryEnv)
			}
			for _, e := range frags.MainEnv {
				if strings.HasPrefix(e.Name, "BLOODRAVEN_PITR_STAGING_LIMIT") || strings.HasPrefix(e.Name, "BLOODRAVEN_PITR_MEMORY_LIMIT") {
					t.Fatalf("budget env %s leaked onto the main container", e.Name)
				}
			}
		})
	}
}

// F148, F149: the verdict parser's guards, each isolated against an
// otherwise valid verdict: site list and its "+N" marker (kept as
// ReplaySitesOmitted), count overflow, the executedSha256 format, and the
// on-history line's count and alphabet.
func TestParsePITRVerdictGuards(t *testing.T) {
	req := pitrRenderedRequest{Mode: "timestamp", Stop: outcomeStopCanonical, EncryptionMode: "None", ExpectedSourceUID: outcomeSourceUID}
	sites17 := make([]string, 17)
	for i := range sites17 {
		sites17[i] = fmt.Sprintf("s%02d", i)
	}
	replace := func(v, old, new string) string {
		if !strings.Contains(v, old) {
			t.Fatalf("fixture lacks %q", old)
		}
		return strings.Replace(v, old, new, 1)
	}
	base := testVerdict(verdictFields{})
	tests := []struct {
		name        string
		verdict     string
		wantErr     string
		wantSites   []string
		wantOmitted int32
	}{
		{name: "plain", verdict: base, wantSites: []string{"dfw", "iad"}},
		{name: "truncated list keeps its omitted count", verdict: testVerdict(verdictFields{sites: "a,b,c,d,e,f,g,h,+1"}),
			wantSites: []string{"a", "b", "c", "d", "e", "f", "g", "h"}, wantOmitted: 1},
		{name: "count only", verdict: testVerdict(verdictFields{sites: "+9"}), wantOmitted: 9},
		{name: "over the status bound", verdict: testVerdict(verdictFields{sites: strings.Join(sites17, ",")}), wantSites: sites17[:16], wantOmitted: 1},
		{name: "marker not last", verdict: testVerdict(verdictFields{sites: "a,+1,b"}), wantErr: "site marker"},
		{name: "marker of zero", verdict: testVerdict(verdictFields{sites: "a,+0"}), wantErr: "site marker"},
		{name: "marker not a number", verdict: testVerdict(verdictFields{sites: "a,+x"}), wantErr: "site marker"},
		{name: "marker with a sign", verdict: testVerdict(verdictFields{sites: "a,++1"}), wantErr: "site marker"},
		{name: "invalid site", verdict: testVerdict(verdictFields{sites: "a,B_C"}), wantErr: "site \"B_C\" is invalid"},
		{name: "segment count overflow", verdict: replace(base, "segments=2 files=2", "segments=2147483648 files=2"), wantErr: "counts overflow"},
		{name: "negative count", verdict: replace(base, "transactions=32", "transactions=-1"), wantErr: "invalid transactions"},
		{name: "malformed digest", verdict: replace(base, "executedSha256="+sha256Hex(outcomeExecuted), "executedSha256="+strings.ToUpper(sha256Hex(outcomeExecuted))), wantErr: "executedSha256 is malformed"},
		{name: "on-history line without a count", verdict: testVerdict(verdictFields{onHistorySet: "aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee:3"}), wantErr: "on-history exclusions line is malformed"},
		{name: "on-history line with foreign characters", verdict: testVerdict(verdictFields{onHistory: 1, onHistorySet: "aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee:3;drop"}), wantErr: "on-history exclusions line is malformed"},
		{name: "on-history line", verdict: testVerdict(verdictFields{onHistory: 1, onHistorySet: "aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee:3"}), wantSites: []string{"dfw", "iad"}},
		// F045 (AREA=S cross-scope): the source must fit the status fields.
		{name: "source name outside Kubernetes bounds", verdict: replace(base, "sourceName=src", "sourceName=Src_Bad"), wantErr: "archive source"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			res, err := parsePITRVerdict(tt.verdict, req)
			if tt.wantErr != "" {
				if err == nil || !strings.Contains(err.Error(), tt.wantErr) {
					t.Fatalf("err = %v, want %q", err, tt.wantErr)
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			if !slices.Equal(res.ReplaySites, tt.wantSites) || res.ReplaySitesOmitted != tt.wantOmitted {
				t.Fatalf("sites %v omitted %d, want %v and %d", res.ReplaySites, res.ReplaySitesOmitted, tt.wantSites, tt.wantOmitted)
			}
		})
	}
}
