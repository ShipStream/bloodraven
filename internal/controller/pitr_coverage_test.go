package controller

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"testing"
	"time"

	v1alpha1 "github.com/shipstream/bloodraven/api/v1alpha1"
	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/record"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

func TestPITRScriptRequiresCoverage(t *testing.T) {
	out, err := runEmbeddedBackupPython(t, restoreScript, `import tempfile
job['_run_pitr']('db', 3306, 'root', 'secret', '2030-01-01 00:00:00', '', tempfile.mkdtemp(), '')
`, nil)
	if err == nil || !strings.Contains(string(out), "PITRCoverageInvalid") {
		t.Fatalf("missing coverage accepted: %s %v", out, err)
	}
}

func TestPITRScriptPartialPolicy(t *testing.T) {
	for _, tc := range []struct {
		name                           string
		allow, partial, covered, files bool
		wantError                      bool
		noop                           bool
	}{
		{name: "strict uncovered", wantError: true},
		{name: "partial not authorized", partial: true, wantError: true},
		{name: "partial noop", allow: true, partial: true, noop: true},
		{name: "covered zero files", covered: true},
		{name: "partial replay", allow: true, partial: true, files: true},
		{name: "covered replay", covered: true, files: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			policy := map[string]bool{"allow": tc.allow, "partial": tc.partial, "covered": tc.covered, "files": tc.files}
			raw, _ := json.Marshal(policy)
			body := `import os, json, tempfile, hashlib, io
cfg=json.loads(os.environ['TEST_POLICY'])
d=tempfile.mkdtemp()
sites=[]
if cfg['files']:
    os.mkdir(os.path.join(d,'iad'))
    with open(os.path.join(d,'iad','mysql-bin.000001'),'wb') as f: f.write(b'verified bytes')
    sites=[{'site':'iad','stopReached':cfg['covered'],'anchored':cfg['covered'],'stagedStopReached':cfg['covered'],'newestReplayEventTime':'2029-12-31T23:59:50Z','hasReplayTransactions':True,'staged':[{'path':'iad/mysql-bin.000001','sha256':hashlib.sha256(b'verified bytes').hexdigest()}]}]
if cfg['covered'] and not sites:
    sites=[{'site':'iad','stopReached':True,'anchored':True,'stagedStopReached':False,'newestReplayEventTime':'0001-01-01T00:00:00Z','hasReplayTransactions':False,'staged':[]}]
coverage={'version':1,'mode':'timestamp','stopDatetime':'2030-01-01T00:00:00Z','allowPartial':cfg['allow'],'targetCovered':cfg['covered'],'partial':cfg['partial'],'sites':sites}
with open(os.path.join(d,'coverage.json'),'w') as f: json.dump(coverage,f)
class Process:
    def __init__(self,*args,**kwargs): self.stdout=io.BytesIO()
    def wait(self): return 0
g=job['_run_pitr'].__globals__
g['subprocess'].Popen=Process
last,count=job['_run_pitr']('db',3306,'root','secret','2030-01-01 00:00:00','',d,'')
g['_target_coordinates']=lambda session: ('target:1','mysql-bin.target',42)
job['_print_restore_complete'](None,'2030-01-01 00:00:00',last,count,job['_read_pitr_coverage'](d,'2030-01-01 00:00:00'))
`
			out, err := runEmbeddedBackupPython(t, restoreScript, body, map[string]string{"TEST_POLICY": string(raw), "BLOODRAVEN_PITR_ALLOW_PARTIAL": strconv.FormatBool(tc.allow)})
			if (err != nil) != tc.wantError {
				t.Fatalf("%v: %s", err, out)
			}
			if tc.wantError {
				return
			}
			if strings.Contains(string(out), "BLOODRAVEN_PITR_NOOP") != tc.noop {
				t.Fatalf("noop policy: %s", out)
			}
			if strings.Contains(string(out), "BLOODRAVEN_PITR_PARTIAL") != tc.partial {
				t.Fatalf("partial policy: %s", out)
			}
			var meta restoreCompletionMetadata
			var ok bool
			for _, line := range strings.Split(string(out), "\n") {
				if parsed, hit := parseRestoreCompleteLine(line); hit {
					meta, ok = parsed, true
				}
			}
			if !ok || meta.PitrCoverageVersion != 1 || meta.PitrPartial != tc.partial {
				t.Fatalf("metadata lost: %+v %s", meta, out)
			}
			if tc.files && meta.PitrNewestEventTime != "2029-12-31T23:59:50Z" {
				t.Fatalf("requested time recorded as reached: %+v", meta)
			}
		})
	}
}

func TestPITRCoverageRendering(t *testing.T) {
	fg := verifyFG()
	fg.Spec.Backup.PITR = &v1alpha1.PITRSpec{Enabled: true, ProfileName: "nightly-s3"}
	pit := &v1alpha1.PointInTimeSpec{StopDatetime: "2030-01-01T00:00:00Z", Sites: []string{"source-old", "removed"}, AllowPartial: true}
	frags, err := buildRestorePITRFragmentsFor(fg, pit)
	if err != nil {
		t.Fatal(err)
	}
	assertEnvEquals(t, frags.InitContainer.Env, "BLOODRAVEN_PITR_SITES", "source-old,removed")
	assertEnvEquals(t, frags.InitContainer.Env, "BLOODRAVEN_PITR_ALLOW_PARTIAL", "true")
	assertEnvEquals(t, frags.MainEnv, "BLOODRAVEN_PITR_ALLOW_PARTIAL", "true")
	for _, mode := range []string{"timestamp", "latest"} {
		v := &v1alpha1.MysqlBackupVerification{ObjectMeta: metav1.ObjectMeta{Name: "verify-pitr", Namespace: "ns"}, Spec: v1alpha1.MysqlBackupVerificationSpec{PointInTime: &v1alpha1.PointInTimeVerificationSpec{Mode: mode, Timestamp: pit.StopDatetime, Sites: pit.Sites}}}
		backup := successfulBackup("seed", "lion", "nightly-s3")
		job, err := buildVerificationJob(verificationJobInputs{FailoverGroup: fg, Profile: fg.Spec.Backup.Profiles[0], Verification: v, Backup: backup, CredsSecretName: "c", ScriptsConfigMapName: "s"})
		if err != nil {
			t.Fatal(err)
		}
		assertEnvEquals(t, job.Spec.Template.Spec.InitContainers[0].Env, "BLOODRAVEN_PITR_MODE", mode)
		assertEnvEquals(t, job.Spec.Template.Spec.InitContainers[0].Env, "BLOODRAVEN_PITR_SITES", "source-old,removed")
		assertEnvEquals(t, job.Spec.Template.Spec.InitContainers[0].Env, "BLOODRAVEN_PITR_ALLOW_PARTIAL", "false")
		assertEnvEquals(t, job.Spec.Template.Spec.InitContainers[0].Env, "BLOODRAVEN_PITR_ALLOW_UNVERIFIED_CROSS_SITE", "false")
		assertEnvEquals(t, job.Spec.Template.Spec.Containers[0].Env, "BLOODRAVEN_SOURCE_GTID_EXECUTED", backup.Status.GtidExecuted)
	}
}

func TestPITRBeforeBackupStartRejected(t *testing.T) {
	fg := fgInitFromMysqlBackup()
	seed := succeededSeedBackup()
	start := metav1.NewTime(time.Date(2030, 1, 2, 0, 0, 0, 0, time.UTC))
	seed.Status.StartTime = &start
	fg.Spec.InitFromBackup.PointInTime = &v1alpha1.PointInTimeSpec{StopDatetime: "2030-01-01T00:00:00Z"}
	r, _ := newReconciler(fg, seed)
	if _, err := r.buildRestoreJob(context.Background(), fg, fg.Spec.Sites[0].Name, "creds"); err == nil || !strings.Contains(err.Error(), "earlier than backup start") {
		t.Fatalf("earlier target accepted: %v", err)
	}
	vfg := verifyFG()
	vfg.Spec.Backup.PITR = &v1alpha1.PITRSpec{Enabled: true, ProfileName: "nightly-s3"}
	backup := successfulBackup("seed", "lion", "nightly-s3")
	backup.Status.StartTime = &start
	v := verifyCR("verify-before-start", "lion", "nightly-s3")
	v.Spec.PointInTime = &v1alpha1.PointInTimeVerificationSpec{Mode: "timestamp", Timestamp: "2030-01-01T00:00:00Z"}
	if _, err := buildVerificationJob(verificationJobInputs{FailoverGroup: vfg, Profile: vfg.Spec.Backup.Profiles[0], Verification: v, Backup: backup, CredsSecretName: "c", ScriptsConfigMapName: "s"}); err == nil || !strings.Contains(err.Error(), "earlier than backup start") {
		t.Fatalf("verification earlier target accepted: %v", err)
	}

}

func TestPITRRestoreOutcomes(t *testing.T) {
	for _, tc := range []struct {
		name, reason             string
		failed, missing, invalid bool
	}{
		{name: "partial", reason: "PointInTimePartial"},
		{name: "omitted", reason: "PITRArchiveSiteOmitted", failed: true},
		{name: "uncovered", reason: "PITRTargetNotCovered", failed: true},
		{name: "list", reason: "PITRArchiveListFailed", failed: true},
		{name: "missing metadata", reason: "PITRCoverageInvalid", missing: true},
		{name: "invalid metadata", reason: "PITRCoverageInvalid", invalid: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			fg := fgInitFromMysqlBackup()
			seed := succeededSeedBackup()
			deploy := primaryReadyDeployment("lion", fg.Spec.Sites[0].Name)
			fg.Spec.Backup.PITR = &v1alpha1.PITRSpec{Enabled: true, ProfileName: seed.Spec.ProfileName}
			fg.Spec.InitFromBackup.PointInTime = &v1alpha1.PointInTimeSpec{StopDatetime: "2030-01-01T00:00:00Z", AllowPartial: true}
			r, c := newReconciler(fg, seed, deploy, dsnSecret())
			rec := record.NewFakeRecorder(30)
			r.Recorder = rec
			if _, err := r.reconcileRestoreJob(context.Background(), fg); err != nil {
				t.Fatal(err)
			}
			var job batchv1.Job
			if err := c.Get(context.Background(), types.NamespacedName{Name: restoreJobName("lion", fg.Spec.Sites[0].Name), Namespace: "ns"}, &job); err != nil {
				t.Fatal(err)
			}
			condition := batchv1.JobComplete
			podPhase := corev1.PodSucceeded
			if tc.failed {
				condition = batchv1.JobFailed
				podPhase = corev1.PodFailed
			}
			job.Status.Conditions = []batchv1.JobCondition{{Type: condition, Status: corev1.ConditionTrue}}
			if err := c.Status().Update(context.Background(), &job); err != nil {
				t.Fatal(err)
			}
			pod := &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: "restore-pod", Namespace: "ns", Labels: map[string]string{"job-name": job.Name}}, Status: corev1.PodStatus{Phase: podPhase}}
			if tc.failed {
				pod.Status.InitContainerStatuses = []corev1.ContainerStatus{{Name: restorePITRInitContainerName, State: corev1.ContainerState{Terminated: &corev1.ContainerStateTerminated{ExitCode: 2, Message: tc.reason + ": details from init container"}}}}
			}
			if err := c.Create(context.Background(), pod); err != nil {
				t.Fatal(err)
			}
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
				if tc.invalid {
					_, _ = fmt.Fprintln(w, "BLOODRAVEN_RESTORE_COMPLETE pitrCoverageVersion=0 pitrPartial=true")
				} else if !tc.missing {
					_, _ = fmt.Fprintln(w, "BLOODRAVEN_RESTORE_COMPLETE pitrCoverageVersion=1 pitrPartial=true pitrStopDatetime=2030-01-01%2000%3A00%3A00 pitrNewestEventTime=2029-12-31T23%3A59%3A50Z")
				}
			}))
			defer server.Close()
			r.Clientset, _ = kubernetes.NewForConfig(&rest.Config{Host: server.URL})
			var fresh v1alpha1.MysqlFailoverGroup
			if err := c.Get(context.Background(), client.ObjectKeyFromObject(fg), &fresh); err != nil {
				t.Fatal(err)
			}
			if _, err := r.reconcileRestoreJob(context.Background(), &fresh); err != nil {
				t.Fatal(err)
			}
			if fresh.Status.Restore == nil || fresh.Status.Restore.Reason != tc.reason {
				t.Fatalf("reason lost: %+v", fresh.Status.Restore)
			}
			if tc.failed && !strings.Contains(fresh.Status.Restore.Message, "details from init container") {
				t.Fatal("init failure lost")
			}
			if tc.invalid && (fresh.Status.Restore.PitrPartial || !strings.Contains(fresh.Status.Restore.Message, "PITRCoverageInvalid")) {
				t.Fatalf("invalid metadata reported as partial: %+v", fresh.Status.Restore)
			}
			if tc.name == "partial" {
				if fresh.Status.Restore.PitrNewestEventTime != "2029-12-31T23:59:50Z" || !fresh.Status.Restore.PitrPartial {
					t.Fatalf("reached point lost: %+v", fresh.Status.Restore)
				}
				found := false
				for len(rec.Events) > 0 {
					if strings.Contains(<-rec.Events, "Warning PointInTimePartial") {
						found = true
					}
				}
				if !found {
					t.Fatal("partial warning missing")
				}
			}
		})
	}
}

func TestPITRVerificationOutcomes(t *testing.T) {
	for _, mode := range []string{"timestamp", "latest"} {
		for _, outcome := range []string{"success", "missing metadata", "init failure", "generic failure"} {
			t.Run(mode+"/"+outcome, func(t *testing.T) {
				fg := verifyFG()
				fg.Spec.Backup.PITR = &v1alpha1.PITRSpec{Enabled: true, ProfileName: "nightly-s3"}
				backup := successfulBackup("seed", "lion", "nightly-s3")
				v := verifyCR("verify-coverage", "lion", "nightly-s3")
				v.Spec.PointInTime = &v1alpha1.PointInTimeVerificationSpec{Mode: mode, Timestamp: "2030-01-01T00:00:00Z"}
				r, c := newVerificationReconciler(t, fg, backup, v, dsnSecret())
				reconcileVerifUntilStable(t, r, v.Name)
				var job batchv1.Job
				if err := c.Get(context.Background(), client.ObjectKey{Namespace: "ns", Name: verificationJobName(v.Name)}, &job); err != nil {
					t.Fatal(err)
				}
				condition := batchv1.JobComplete
				podPhase := corev1.PodSucceeded
				reason := "Succeeded"
				if outcome == "init failure" {
					condition = batchv1.JobFailed
					podPhase = corev1.PodFailed
					reason = "PITRTargetNotCovered"
					if mode == "latest" {
						reason = "PITRNoArchivedReplay"
					}
				}
				if outcome == "missing metadata" {
					reason = "PITRCoverageInvalid"
				}
				if outcome == "generic failure" {
					condition = batchv1.JobFailed
					podPhase = corev1.PodFailed
					reason = "Failed"
				}
				job.Status.Conditions = []batchv1.JobCondition{{Type: condition, Status: corev1.ConditionTrue, LastTransitionTime: metav1.Now()}}
				if err := c.Status().Update(context.Background(), &job); err != nil {
					t.Fatal(err)
				}
				pod := &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: "verify-pod", Namespace: "ns", Labels: map[string]string{labelMysqlBackupVerification: v.Name, "job-name": job.Name}}, Status: corev1.PodStatus{Phase: podPhase}}
				if outcome == "init failure" {
					pod.Status.InitContainerStatuses = []corev1.ContainerStatus{{Name: restorePITRInitContainerName, State: corev1.ContainerState{Terminated: &corev1.ContainerStateTerminated{ExitCode: 2, Message: reason + ": init details"}}}}
				}
				if outcome == "generic failure" {
					pod.Status.ContainerStatuses = []corev1.ContainerStatus{{Name: backupJobContainerName, State: corev1.ContainerState{Terminated: &corev1.ContainerStateTerminated{ExitCode: 137, Message: "OOMKilled"}}}}
				}
				if err := c.Create(context.Background(), pod); err != nil {
					t.Fatal(err)
				}
				server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
					if outcome == "success" {
						_, _ = fmt.Fprintf(w, "BLOODRAVEN_VERIFY_REPLAY_COMPLETE coverage=1 mode=%s targetCovered=true partial=false files=1 file=mysql-bin.000001 position=0 timestamp=2029-12-31T23%%3A59%%3A50Z\n", mode)
					}
				}))
				defer server.Close()
				r.Clientset, _ = kubernetes.NewForConfig(&rest.Config{Host: server.URL})
				reconcileVerifUntilStable(t, r, v.Name)
				var got v1alpha1.MysqlBackupVerification
				if err := c.Get(context.Background(), client.ObjectKeyFromObject(v), &got); err != nil {
					t.Fatal(err)
				}
				found := false
				for _, cond := range got.Status.Conditions {
					if cond.Type == ConditionVerified {
						found = true
						if cond.Reason != reason {
							t.Fatalf("reason %s, want %s: %+v", cond.Reason, reason, got.Status)
						}
					}
				}
				if !found {
					t.Fatal("no Verified condition")
				}
				if outcome == "success" && (got.Status.ReplayedThroughBinlog == nil || got.Status.ReplayedThroughBinlog.Timestamp == nil || got.Status.ReplayedThroughBinlog.Timestamp.UTC().Format(time.RFC3339) != "2029-12-31T23:59:50Z") {
					t.Fatalf("newest replay time lost: %+v", got.Status)
				}
				if outcome == "init failure" && !strings.Contains(got.Status.Message, "init details") {
					t.Fatalf("init reason lost: %+v", got.Status)
				}
			})
		}
	}
}

func TestPITRFractionalTargetMatchesEventResolution(t *testing.T) {
	out, err := runEmbeddedBackupPython(t, restoreScript, `for value in ['2030-01-01T00:00:00.5Z','2030-01-01T00:00:00.000000001Z','2030-01-01 00:00:00.5']:
    assert job['_parse_pitr_target'](value) == '2030-01-01 00:00:01'
`, nil)
	if err != nil {
		t.Fatalf("%v: %s", err, out)
	}
}

func TestPITRScriptRejectsStagingCorruptionUnderPartial(t *testing.T) {
	out, err := runEmbeddedBackupPython(t, restoreScript, `import tempfile, os, json
local=tempfile.mkdtemp()
os.mkdir(os.path.join(local,'iad'))
with open(os.path.join(local,'iad','mysql-bin.000001'),'wb') as f: f.write(b'changed after download')
coverage={'version':1,'mode':'timestamp','stopDatetime':'2030-01-01T00:00:00Z','allowPartial':True,'partial':True,'targetCovered':False,'sites':[{'site':'iad','stopReached':False,'anchored':False,'stagedStopReached':False,'hasReplayTransactions':True,'newestReplayEventTime':'2029-12-31T23:59:50Z','staged':[{'path':'iad/mysql-bin.000001','sha256':'wrong'}]}]}
with open(os.path.join(local,'coverage.json'),'w') as f: json.dump(coverage,f)
job['_run_pitr']('db',3306,'root','secret','2030-01-01 00:00:00','',local,'')
`, map[string]string{"BLOODRAVEN_PITR_ALLOW_PARTIAL": "true"})
	if err == nil || !strings.Contains(string(out), "staged identity differs") {
		t.Fatalf("staging corruption accepted: %v %s", err, out)
	}
}
