package controller

import (
	"context"
	"strings"
	"testing"
	"time"

	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	apimeta "k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/tools/record"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	v1alpha1 "github.com/shipstream/bloodraven/api/v1alpha1"
	"github.com/shipstream/bloodraven/internal/state"
)

// initRestoreGateFG is a two-site group with a reader whose initFromBackup
// restore is in the given phase ("" = not observed yet), with the seed dc1
// published as the writable active site and the other sites healthy: the
// state that would label dc1 primary and every site healthy without the
// restore.
func initRestoreGateFG(phase v1alpha1.BackupPhase) *v1alpha1.MysqlFailoverGroup {
	lag0 := int64(0)
	fg := newTestFG()
	fg.Spec.Sites = append(fg.Spec.Sites, v1alpha1.SiteSpec{Name: "reader", ServerID: 103, Role: v1alpha1.SiteRoleReadOnly})
	fg.Spec.InitFromBackup = &v1alpha1.InitFromBackupSpec{
		Source: v1alpha1.RestoreSource{BackupRef: &v1alpha1.LocalMysqlBackupRef{Name: "seed"}},
	}
	fg.Status.ActiveSite = "dc1"
	fg.Status.Sites = []v1alpha1.SiteStatus{
		{Name: "dc1", State: v1alpha1.SiteStateWritable},
		{Name: "dc2", State: v1alpha1.SiteStateReadOnly, Replicating: true},
		{Name: "reader", State: v1alpha1.SiteStateReadOnly, Replicating: true, SourceHost: internalSiteServiceHost(fg.Name, "dc1", fg.Namespace),
			SourceConvergenceState: v1alpha1.SourceConvergenceConverged, SecondsBehindSource: &lag0},
	}
	if phase != "" {
		fg.Status.Restore = &v1alpha1.RestoreStatus{Phase: phase, TargetSite: "dc1", JobName: "restore-lion-dc1"}
	}
	return fg
}

// sitePod is a MySQL pod of site already labelled as a serving primary
// (role=primary, healthy=yes), so a gate that fails to strip the labels
// shows up as an endpoint.
func sitePod(fg *v1alpha1.MysqlFailoverGroup, site string) *corev1.Pod {
	return &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: site + "-pod", Namespace: fg.Namespace, Labels: map[string]string{
		labelAppName: "mysql", labelInstance: fg.Name, v1alpha1.SiteLabel: site, v1alpha1.RoleLabel: "primary", v1alpha1.HealthyLabel: "yes",
	}}}
}

// serviceEndpoints returns the pods the named Service's selector matches:
// the endpoints Kubernetes would publish for it.
func serviceEndpoints(t *testing.T, c client.Client, ns, name string) []string {
	t.Helper()
	var svc corev1.Service
	if err := c.Get(context.Background(), types.NamespacedName{Namespace: ns, Name: name}, &svc); err != nil {
		t.Fatalf("get service %s: %v", name, err)
	}
	var pods corev1.PodList
	if err := c.List(context.Background(), &pods, client.InNamespace(ns), client.MatchingLabelsSelector{Selector: labels.SelectorFromSet(svc.Spec.Selector)}); err != nil {
		t.Fatal(err)
	}
	var names []string
	for _, p := range pods.Items {
		names = append(names, p.Name)
	}
	return names
}

// TestInitRestore_ApplicationServicesWithheldUntilSucceeded: while an
// initFromBackup restore has not succeeded (not observed yet, Pending,
// Running, or Failed with partial data) the writable seed must not be
// reachable through any application Service: not the -primary or -replicas
// Service, not its own per-site Service (often a LoadBalancer the DNS
// record points at), not a reader's. The restore Job's internal Service
// keeps reaching it. Once the restore succeeds the normal labels return.
func TestInitRestore_ApplicationServicesWithheldUntilSucceeded(t *testing.T) {
	for _, tc := range []struct {
		phase    v1alpha1.BackupPhase
		withheld bool
	}{
		{"", true},
		{v1alpha1.BackupPhasePending, true},
		{v1alpha1.BackupPhaseRunning, true},
		{v1alpha1.BackupPhaseFailed, true},
		{v1alpha1.BackupPhaseSucceeded, false},
	} {
		name := string(tc.phase)
		if name == "" {
			name = "unobserved"
		}
		t.Run(name, func(t *testing.T) {
			ctx := context.Background()
			fg := initRestoreGateFG(tc.phase)
			r, c := newReconciler(fg, sitePod(fg, "dc1"), sitePod(fg, "dc2"), sitePod(fg, "reader"))
			for _, site := range fg.Spec.Sites {
				if err := r.reconcileSiteService(ctx, fg, site); err != nil {
					t.Fatal(err)
				}
				if err := r.reconcileInternalSiteService(ctx, fg, site); err != nil {
					t.Fatal(err)
				}
			}
			if err := r.reconcilePrimaryService(ctx, fg); err != nil {
				t.Fatal(err)
			}
			if err := r.reconcileReplicasService(ctx, fg); err != nil {
				t.Fatal(err)
			}
			if err := r.syncPodLabels(ctx, fg); err != nil {
				t.Fatal(err)
			}

			ns := fg.Namespace
			want := map[string]string{
				"mysql-lion-primary":  "dc1-pod",
				"mysql-lion-replicas": "dc2-pod,reader-pod",
				"mysql-lion-dc1":      "dc1-pod",
				"mysql-lion-dc2":      "dc2-pod",
				"mysql-lion-reader":   "reader-pod",
			}
			for svc, pods := range want {
				got := strings.Join(serviceEndpoints(t, c, ns, svc), ",")
				if tc.withheld {
					pods = ""
				}
				if got != pods {
					t.Errorf("Service %s endpoints = %q, want %q", svc, got, pods)
				}
			}
			// The restore Job's path to the seed stays open whatever the phase.
			if got := strings.Join(serviceEndpoints(t, c, ns, internalSiteServiceName(fg.Name, "dc1")), ","); got != "dc1-pod" {
				t.Errorf("internal Service of the seed endpoints = %q, want dc1-pod", got)
			}
		})
	}
}

func TestInitRestoreSeedSite(t *testing.T) {
	for _, tc := range []struct {
		name    string
		restore *v1alpha1.RestoreStatus
		spec    bool
		active  string
		want    string
	}{
		{name: "no initFromBackup", spec: false, active: "dc1", want: ""},
		{name: "not seeded", spec: true, want: ""},
		{name: "seeded, Job not created", spec: true, active: "dc1",
			restore: &v1alpha1.RestoreStatus{Phase: v1alpha1.BackupPhasePending, TargetSite: "dc1"}, want: "dc1"},
		{name: "running, active site cleared by the fence", spec: true,
			restore: &v1alpha1.RestoreStatus{Phase: v1alpha1.BackupPhaseRunning, JobName: "j", TargetSite: "dc1"}, want: "dc1"},
		{name: "Job target outranks active site", spec: true, active: "dc2",
			restore: &v1alpha1.RestoreStatus{Phase: v1alpha1.BackupPhaseRunning, JobName: "j", TargetSite: "dc1"}, want: "dc1"},
		{name: "failed", spec: true,
			restore: &v1alpha1.RestoreStatus{Phase: v1alpha1.BackupPhaseFailed, JobName: "j", TargetSite: "dc1"}, want: "dc1"},
		{name: "succeeded", spec: true, active: "dc1",
			restore: &v1alpha1.RestoreStatus{Phase: v1alpha1.BackupPhaseSucceeded, JobName: "j", TargetSite: "dc1"}, want: ""},
	} {
		t.Run(tc.name, func(t *testing.T) {
			fg := newTestFG()
			if tc.spec {
				fg.Spec.InitFromBackup = &v1alpha1.InitFromBackupSpec{}
			}
			fg.Status.ActiveSite = tc.active
			fg.Status.Restore = tc.restore
			if got := initRestoreSeedSite(fg); got != tc.want {
				t.Fatalf("initRestoreSeedSite = %q, want %q", got, tc.want)
			}
		})
	}
}

// TestUpdateCRStatus_ReadyFalseUntilInitRestoreSucceeds: a seeded group
// whose topology is otherwise ready reports Ready=False while its
// initFromBackup restore has not succeeded, and after a failed load the
// condition carries the recovery procedure.
func TestUpdateCRStatus_ReadyFalseUntilInitRestoreSucceeds(t *testing.T) {
	for _, tc := range []struct {
		phase      v1alpha1.BackupPhase
		wantStatus metav1.ConditionStatus
		wantReason string
		wantText   string
	}{
		{v1alpha1.BackupPhaseRunning, metav1.ConditionFalse, v1alpha1.ReasonInitFromBackupInProgress, "applications are kept off the group"},
		{v1alpha1.BackupPhaseFailed, metav1.ConditionFalse, v1alpha1.ReasonInitFromBackupFailed, "delete the restore Job"},
		{v1alpha1.BackupPhaseSucceeded, metav1.ConditionTrue, "TopologyPolled", ""},
	} {
		t.Run(string(tc.phase), func(t *testing.T) {
			fg := newTestFG()
			fg.Spec.Sites = fg.Spec.Sites[:1]
			fg.Spec.InitFromBackup = &v1alpha1.InitFromBackupSpec{}
			fg.Status.Restore = &v1alpha1.RestoreStatus{Phase: tc.phase, JobName: "restore-lion-dc1", TargetSite: "dc1", Message: "load failed"}
			c := fake.NewClientBuilder().WithScheme(testScheme()).WithStatusSubresource(&v1alpha1.MysqlFailoverGroup{}).WithObjects(fg).Build()
			runner := &TopologyManagerRunner{client: c, logger: testLogger(), managers: map[types.NamespacedName]*managedTopology{}}
			nn := client.ObjectKeyFromObject(fg)
			if err := runner.updateCRStatus(context.Background(), nn, TopologySnapshot{
				ActiveSite: "dc1",
				Sites:      []SiteSnapshot{{Name: "dc1", State: state.StateWritable}},
			}); err != nil {
				t.Fatal(err)
			}
			var got v1alpha1.MysqlFailoverGroup
			if err := c.Get(context.Background(), nn, &got); err != nil {
				t.Fatal(err)
			}
			ready := apimeta.FindStatusCondition(got.Status.Conditions, "Ready")
			if ready == nil || ready.Status != tc.wantStatus || ready.Reason != tc.wantReason || !strings.Contains(ready.Message, tc.wantText) {
				t.Fatalf("Ready = %+v, want %s/%s containing %q", ready, tc.wantStatus, tc.wantReason, tc.wantText)
			}
		})
	}
}

// TestReconcileRestoreJob_FailedEventCarriesRecovery: a failed load leaves
// the seed out of service with partial data; the RestoreFailed Event must
// say so and how to retry.
func TestReconcileRestoreJob_FailedEventCarriesRecovery(t *testing.T) {
	ctx := context.Background()
	fg := fgInitFromMysqlBackup()
	seedPublished(fg)
	r, c := newReconciler(fg, succeededSeedBackup(), primaryReadyDeployment("lion", "iad"), operatorCredsSecret())
	rec := record.NewFakeRecorder(20)
	r.Recorder = rec
	if _, err := r.reconcileRestoreJob(ctx, fg); err != nil {
		t.Fatal(err)
	}
	var job batchv1.Job
	if err := c.Get(ctx, types.NamespacedName{Name: restoreJobName("lion", "iad"), Namespace: "ns"}, &job); err != nil {
		t.Fatal(err)
	}
	now := metav1.Now()
	job.Status.Conditions = []batchv1.JobCondition{{Type: batchv1.JobFailed, Status: corev1.ConditionTrue, LastProbeTime: now, LastTransitionTime: now, Message: "BackoffLimitExceeded"}}
	job.Status.Failed = 1
	if err := c.Status().Update(ctx, &job); err != nil {
		t.Fatal(err)
	}
	if d, err := r.reconcileRestoreJob(ctx, fg); err != nil || d != 0 {
		t.Fatalf("reconcileRestoreJob = %v, %v", d, err)
	}
	if !restoreInFlight(fg) {
		t.Fatal("a failed restore must keep the group gated as unfinished")
	}
	deadline := time.After(time.Second)
	for {
		select {
		case ev := <-rec.Events:
			if !strings.Contains(ev, "RestoreFailed") {
				continue
			}
			for _, want := range []string{"out of service", "no site is cloned", "deleting and recreating the group", "delete the restore Job"} {
				if !strings.Contains(ev, want) {
					t.Errorf("RestoreFailed event %q missing %q", ev, want)
				}
			}
			return
		case <-deadline:
			t.Fatal("no RestoreFailed event")
		}
	}
}

// TestStartManager_HydratesInitRestoreSeedBeforeRun: an operator restarted
// while an initFromBackup restore is unfinished must hold clones and DNS
// from its first poll, and must know the seed from the persisted status
// even after status.activeSite went "" because the seed was fenced.
func TestStartManager_HydratesInitRestoreSeedBeforeRun(t *testing.T) {
	r, _, fg, _ := lifecycleRunner(t)
	nn := FailoverGroupNamespacedName(fg)
	fg.Spec.InitFromBackup = &v1alpha1.InitFromBackupSpec{}
	fg.Status.ActiveSite = ""
	fg.Status.Restore = &v1alpha1.RestoreStatus{Phase: v1alpha1.BackupPhaseRunning, JobName: "restore-lion-dc1", TargetSite: "dc1"}

	if err := r.startManager(context.Background(), fg, mustTopologyConfig(fg), nil); err != nil {
		t.Fatalf("startManager: %v", err)
	}
	mt := registeredManager(r, nn)
	if mt == nil {
		t.Fatal("no manager registered")
	}
	mt.tm.mu.RLock()
	suppressed := mt.tm.autoBootstrapSuppressed
	mt.tm.mu.RUnlock()
	if !suppressed {
		t.Fatal("clone/DNS suppression not set when Run started")
	}
	if got := mt.tm.initRestoreSeedSite(); got != "dc1" {
		t.Fatalf("restore seed = %q, want dc1 from status.restore", got)
	}
}
