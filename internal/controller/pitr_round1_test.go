package controller

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	v1alpha1 "github.com/shipstream/bloodraven/api/v1alpha1"
	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/record"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"
)

func TestRound1CoveredIdleScript(t *testing.T) {
	out, err := runEmbeddedBackupPython(t, restoreScript, `import tempfile,os,json
local=tempfile.mkdtemp()
coverage={'version':1,'mode':'timestamp','stopDatetime':'2030-01-01T00:00:00Z','allowPartial':False,'partial':False,'targetCovered':True,'sites':[{'site':'iad','anchored':True,'stopReached':True,'stagedStopReached':False,'hasReplayTransactions':False,'newestReplayEventTime':'0001-01-01T00:00:00Z','staged':[]}]}
with open(os.path.join(local,'coverage.json'),'w') as f:json.dump(coverage,f)
assert job['_run_pitr']('db',3306,'root','secret','2030-01-01 00:00:00','',local,'') == ('',0)
`, nil)
	if err != nil || strings.Contains(string(out), "BLOODRAVEN_PITR_NOOP") || strings.Contains(string(out), "BLOODRAVEN_PITR_PARTIAL") {
		t.Fatalf("covered idle rejected or mislabeled: %v %s", err, out)
	}
}

func round1RestoreJob(t *testing.T) (*MysqlFailoverGroupReconciler, client.Client, *v1alpha1.MysqlFailoverGroup, *batchv1.Job) {
	t.Helper()
	fg := fgInitFromMysqlBackup()
	seed := succeededSeedBackup()
	fg.Spec.Backup.PITR = &v1alpha1.PITRSpec{Enabled: true, ProfileName: seed.Spec.ProfileName}
	fg.Spec.InitFromBackup.PointInTime = &v1alpha1.PointInTimeSpec{StopDatetime: "2030-01-01T00:00:00Z"}
	r, c := newReconciler(fg, seed, primaryReadyDeployment("lion", fg.Spec.Sites[0].Name), dsnSecret())
	if _, err := r.reconcileRestoreJob(context.Background(), fg); err != nil {
		t.Fatal(err)
	}
	job := &batchv1.Job{}
	if err := c.Get(context.Background(), client.ObjectKey{Namespace: "ns", Name: restoreJobName("lion", fg.Spec.Sites[0].Name)}, job); err != nil {
		t.Fatal(err)
	}
	return r, c, fg, job
}

func TestRound1RestoreLogRetry(t *testing.T) {
	r, c, fg, job := round1RestoreJob(t)
	job.Status.Conditions = []batchv1.JobCondition{{Type: batchv1.JobComplete, Status: corev1.ConditionTrue, LastTransitionTime: metav1.Now()}}
	if err := c.Status().Update(context.Background(), job); err != nil {
		t.Fatal(err)
	}
	pod := &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: "restore-round1", Namespace: "ns", Labels: map[string]string{"job-name": job.Name}}, Status: corev1.PodStatus{Phase: corev1.PodSucceeded}}
	if err := c.Create(context.Background(), pod); err != nil {
		t.Fatal(err)
	}
	var available atomic.Bool
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		if !available.Load() {
			http.Error(w, "kubelet temporarily unavailable", 503)
			return
		}
		_, _ = fmt.Fprintln(w, "BLOODRAVEN_RESTORE_COMPLETE pitrCoverageVersion=1 pitrTargetCovered=true pitrPartial=false pitrNewestEventTime=")
	}))
	defer server.Close()
	r.Clientset, _ = kubernetes.NewForConfig(&rest.Config{Host: server.URL})
	rec := record.NewFakeRecorder(20)
	r.Recorder = rec
	delay, err := r.reconcileRestoreJob(context.Background(), fg)
	if err != nil {
		t.Fatal(err)
	}
	if delay <= 0 || fg.Status.Restore.Phase == v1alpha1.BackupPhaseFailed || len(rec.Events) != 1 || !strings.Contains(fg.Status.Restore.Message, "waiting") {
		t.Fatalf("unreadable logs recorded failure: delay=%v status=%+v events=%d", delay, fg.Status.Restore, len(rec.Events))
	}
	available.Store(true)
	if _, err = r.reconcileRestoreJob(context.Background(), fg); err != nil {
		t.Fatal(err)
	}
	if fg.Status.Restore.Phase != v1alpha1.BackupPhaseSucceeded || fg.Status.Restore.PitrNewestEventTime != "" {
		t.Fatalf("recovered idle metadata: %+v", fg.Status.Restore)
	}
}

func TestRound1RestoreFailureCached(t *testing.T) {
	r, c, fg, job := round1RestoreJob(t)
	job.Status.Conditions = []batchv1.JobCondition{{Type: batchv1.JobFailed, Status: corev1.ConditionTrue}}
	if err := c.Status().Update(context.Background(), job); err != nil {
		t.Fatal(err)
	}
	pod := &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: "restore-round1", Namespace: "ns", Labels: map[string]string{"job-name": job.Name}}, Status: corev1.PodStatus{Phase: corev1.PodFailed}}
	if err := c.Create(context.Background(), pod); err != nil {
		t.Fatal(err)
	}
	var reads atomic.Int64
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		reads.Add(1)
		_, _ = fmt.Fprintln(w, "PITRCrossSiteOrder: east and iad overlap")
	}))
	defer server.Close()
	r.Clientset, _ = kubernetes.NewForConfig(&rest.Config{Host: server.URL})
	for i := 0; i < 3; i++ {
		if _, err := r.reconcileRestoreJob(context.Background(), fg); err != nil {
			t.Fatal(err)
		}
	}
	if reads.Load() != 1 || fg.Status.Restore.Reason != "PITRCrossSiteOrder" {
		t.Fatalf("failure detail not cached: reads=%d status=%+v", reads.Load(), fg.Status.Restore)
	}
}

func TestRound1BackupStartTrimsInput(t *testing.T) {
	start := metav1.NewTime(time.Date(2030, 1, 1, 0, 0, 0, 0, time.UTC))
	if err := validatePITRBackupStart(&v1alpha1.PointInTimeSpec{StopDatetime: " 2030-01-01T01:00:00Z\n"}, &start); err != nil {
		t.Fatalf("runtime-valid time rejected by renderer: %v", err)
	}
}

func TestRound1VerificationCoverageAndLogRetry(t *testing.T) {
	for _, tc := range []struct {
		name, mode, metadata, reason string
		retry                        bool
	}{
		{name: "timestamp idle", mode: "timestamp", metadata: "coverage=1 mode=timestamp targetCovered=true partial=false files=0", reason: "Succeeded"},
		{name: "latest empty replay", mode: "latest", metadata: "coverage=1 mode=latest targetCovered=false partial=false files=0", reason: "PITRCoverageInvalid"},
		{name: "timestamp no certificate", mode: "timestamp", metadata: "files=0", reason: "PITRCoverageInvalid"},
		{name: "timestamp wrong version", mode: "timestamp", metadata: "coverage=2 mode=timestamp targetCovered=true partial=false files=0", reason: "PITRCoverageInvalid"},
		{name: "timestamp not covered", mode: "timestamp", metadata: "coverage=1 mode=timestamp targetCovered=false partial=false files=0", reason: "PITRCoverageInvalid"},
		{name: "retry unreadable logs", mode: "timestamp", metadata: "coverage=1 mode=timestamp targetCovered=true partial=false files=0", reason: "Succeeded", retry: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			fg := verifyFG()
			fg.Spec.Backup.PITR = &v1alpha1.PITRSpec{Enabled: true, ProfileName: "nightly-s3"}
			backup := successfulBackup("seed", "lion", "nightly-s3")
			v := verifyCR("round1-verification", "lion", "nightly-s3")
			v.Spec.PointInTime = &v1alpha1.PointInTimeVerificationSpec{Mode: tc.mode, Timestamp: "2030-01-01T00:00:00Z"}
			r, c := newVerificationReconciler(t, fg, backup, v, dsnSecret())
			reconcileVerifUntilStable(t, r, v.Name)
			job := &batchv1.Job{}
			if err := c.Get(context.Background(), client.ObjectKey{Namespace: "ns", Name: verificationJobName(v.Name)}, job); err != nil {
				t.Fatal(err)
			}
			job.Status.Conditions = []batchv1.JobCondition{{Type: batchv1.JobComplete, Status: corev1.ConditionTrue, LastTransitionTime: metav1.Now()}}
			if err := c.Status().Update(context.Background(), job); err != nil {
				t.Fatal(err)
			}
			pod := &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: "verification-round1", Namespace: "ns", Labels: map[string]string{labelMysqlBackupVerification: v.Name, "job-name": job.Name}}, Status: corev1.PodStatus{Phase: corev1.PodSucceeded}}
			if err := c.Create(context.Background(), pod); err != nil {
				t.Fatal(err)
			}
			var available atomic.Bool
			available.Store(!tc.retry)
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
				if !available.Load() {
					http.Error(w, "logs unavailable", 503)
					return
				}
				_, _ = fmt.Fprintf(w, "BLOODRAVEN_VERIFY_REPLAY_COMPLETE %s file= position=0 timestamp=\n", tc.metadata)
			}))
			defer server.Close()
			r.Clientset, _ = kubernetes.NewForConfig(&rest.Config{Host: server.URL})
			rec := record.NewFakeRecorder(20)
			r.Recorder = rec
			if err := c.Get(context.Background(), client.ObjectKeyFromObject(v), v); err != nil {
				t.Fatal(err)
			}
			result, err := r.observeVerificationJob(context.Background(), v, job)
			if err != nil {
				t.Fatal(err)
			}
			if tc.retry {
				if result.RequeueAfter <= 0 || v.Status.Phase == v1alpha1.VerificationPhaseFailed || len(rec.Events) != 1 || !strings.Contains(v.Status.Message, "waiting") {
					t.Fatalf("unreadable logs recorded failure: result=%+v status=%+v events=%d", result, v.Status, len(rec.Events))
				}
				available.Store(true)
				if _, err = r.observeVerificationJob(context.Background(), v, job); err != nil {
					t.Fatal(err)
				}
			}
			reason := ""
			for _, cond := range v.Status.Conditions {
				if cond.Type == ConditionVerified {
					reason = cond.Reason
				}
			}
			if reason != tc.reason {
				t.Fatalf("reason %s, want %s: %+v", reason, tc.reason, v.Status)
			}
			if v.Status.ReplayedThroughBinlog != nil && v.Status.ReplayedThroughBinlog.Timestamp != nil {
				t.Fatal("requested time substituted for absent replay time")
			}
		})
	}
}

func TestRound1VerificationSentinelBridge(t *testing.T) {
	start := strings.Index(verifyScript, "PITR_MODE=\"")
	end := strings.Index(verifyScript[start:], "# ---- Sanity check") + start
	dir := t.TempDir()
	path := filepath.Join(dir, "restore.log")
	line := "BLOODRAVEN_RESTORE_COMPLETE pitrCoverageVersion=1 pitrTargetCovered=true pitrPartial=false pitrReplayedBinlogCount=0 pitrReplayedBinlogFile= pitrNewestEventTime=\n"
	if err := os.WriteFile(path, []byte(line), 0600); err != nil {
		t.Fatal(err)
	}
	cmd := exec.Command("bash", "-c", verifyScript[start:end])
	cmd.Env = []string{"PATH=" + os.Getenv("PATH"), "RESTORE_LOG=" + path, "BLOODRAVEN_VERIFY_PITR_MODE=timestamp"}
	out, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("bridge: %v %s", err, out)
	}
	if !strings.Contains(string(out), "coverage=1 mode=timestamp targetCovered=true partial=false files=0") || !strings.Contains(string(out), "timestamp=\n") {
		t.Fatalf("coverage evidence lost at shell bridge: %s", out)
	}
}

func TestRound1FailurePodSelectors(t *testing.T) {
	r, c, fg, job := round1RestoreJob(t)
	job.Status.Conditions = []batchv1.JobCondition{{Type: batchv1.JobFailed, Status: corev1.ConditionTrue}}
	if err := c.Status().Update(context.Background(), job); err != nil {
		t.Fatal(err)
	}
	var lists int
	r.Client = interceptor.NewClient(c.(client.WithWatch), interceptor.Funcs{List: func(ctx context.Context, c client.WithWatch, list client.ObjectList, opts ...client.ListOption) error {
		if _, ok := list.(*corev1.PodList); ok {
			parsed := &client.ListOptions{}
			for _, opt := range opts {
				opt.ApplyToList(parsed)
			}
			if parsed.LabelSelector == nil || parsed.LabelSelector.Empty() {
				t.Fatal("namespace-wide pod list")
			}
			lists++
		}
		return c.List(ctx, list, opts...)
	}})
	if _, err := r.reconcileRestoreJob(context.Background(), fg); err != nil {
		t.Fatal(err)
	}
	if lists == 0 {
		t.Fatal("expected selector lookup for failure detail")
	}
}

func TestRound1StagedBarrierWithoutEarlierEvents(t *testing.T) {
	out, err := runEmbeddedBackupPython(t, restoreScript, `import tempfile,os,json,hashlib,io
local=tempfile.mkdtemp();os.mkdir(os.path.join(local,'iad'))
path=os.path.join(local,'iad','mysql-bin.000001')
with open(path,'wb') as f:f.write(b'verified barrier bytes')
coverage={'version':1,'mode':'timestamp','stopDatetime':'2030-01-01T00:00:00Z','allowPartial':False,'partial':False,'targetCovered':True,'sites':[{'site':'iad','anchored':True,'stopReached':True,'stagedStopReached':True,'hasReplayTransactions':False,'newestReplayEventTime':'0001-01-01T00:00:00Z','staged':[{'path':'iad/mysql-bin.000001','sha256':hashlib.sha256(b'verified barrier bytes').hexdigest()}]}]}
with open(os.path.join(local,'coverage.json'),'w') as f:json.dump(coverage,f)
class Process:
    def __init__(self,*args,**kwargs):self.stdout=io.BytesIO()
    def wait(self):return 0
g=job['_run_pitr'].__globals__;g['subprocess'].Popen=Process
g['_target_coordinates']=lambda session: ('source:1','mysql-bin.target',42)
certificate=job['_read_pitr_coverage'](local,'2030-01-01 00:00:00')
last,count=job['_run_pitr']('db',3306,'root','secret','2030-01-01 00:00:00','',local,'',certificate)
job['_print_restore_complete'](None,'2030-01-01 00:00:00',last,count,certificate)
`, nil)
	if err != nil || !strings.Contains(string(out), "pitrTargetCovered=true") || !strings.Contains(string(out), "pitrNewestEventTime=\n") {
		t.Fatalf("barrier-only replay certificate lost: %v %s", err, out)
	}
}

func TestRound1PrimaryFailureReason(t *testing.T) {
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
