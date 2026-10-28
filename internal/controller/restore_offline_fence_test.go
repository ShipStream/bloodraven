package controller

import (
	"context"
	"errors"
	"slices"
	"strings"
	"testing"
	"time"

	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/tools/record"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"

	v1alpha1 "github.com/shipstream/bloodraven/api/v1alpha1"
	"github.com/shipstream/bloodraven/internal/mysql"
	"github.com/shipstream/bloodraven/internal/platform"
	"github.com/shipstream/bloodraven/internal/state"
)

// fenceTopologyManager is a topology manager over mock checkers named
// after fg's sites, so the restore fence can be driven through a runner.
func fenceTopologyManager(fg *v1alpha1.MysqlFailoverGroup) (*TopologyManager, map[string]*mockMySQL) {
	cfg := testTopologyConfig()
	cfg.Sites = nil
	mocks := map[string]*mockMySQL{}
	var checkers []mysql.Checker
	for i, s := range fg.Spec.Sites {
		cfg.Sites = append(cfg.Sites, SiteTopologyConfig{Name: s.Name, Role: state.SiteRolePrimaryCandidate, Host: "mysql-" + s.Name})
		m := &mockMySQL{readOnly: i != 0}
		mocks[s.Name] = m
		checkers = append(checkers, m)
	}
	tm := NewTopologyManager(cfg, checkers, NewFailoverController(testLogger()), nil, nil,
		BootstrapConfig{ReplUser: "repl"}, newMockTainter(), platform.NewHub(testLogger()), &mockDNS{}, testLogger())
	return tm, mocks
}

// withFenceRunner gives r a runner managing fg with fenceTopologyManager.
func withFenceRunner(r *MysqlFailoverGroupReconciler, c client.Client, fg *v1alpha1.MysqlFailoverGroup) (*TopologyManager, map[string]*mockMySQL) {
	tm, mocks := fenceTopologyManager(fg)
	nn := types.NamespacedName{Name: fg.Name, Namespace: fg.Namespace}
	if r.Runner == nil {
		r.Runner = &TopologyManagerRunner{client: c, recorder: r.Recorder, logger: testLogger()}
	}
	if r.Runner.managers == nil {
		r.Runner.managers = map[types.NamespacedName]*managedTopology{}
	}
	r.Runner.managers[nn] = &managedTopology{tm: tm}
	return tm, mocks
}

func fencingRestore(t *testing.T) (*MysqlFailoverGroupReconciler, client.Client, *v1alpha1.MysqlFailoverGroup, *mockMySQL) {
	t.Helper()
	fg := fgInPlaceRestore("")
	site := fg.Spec.Sites[0].Name
	fg.Status.RestoreInPlace = &v1alpha1.RestoreInPlaceStatus{
		Phase: v1alpha1.RestoreInPlaceFencing, TargetSite: site, Scope: "full", StartTime: ptrTime(time.Now()),
	}
	fg.Status.RestoreOfflineFence = &v1alpha1.RestoreOfflineFenceStatus{Site: site, ConfirmToken: fg.Spec.RestoreInPlace.Confirm}
	r, c := newReconciler(fg, succeededSeedBackup(), dsnSecret(), primaryFencedPod(fg.Name, site))
	_, mocks := withFenceRunner(r, c, fg)
	return r, c, fg, mocks[site]
}

// The pod-label fence only stops new connections through the -primary
// Service. A full-instance restore must not leave Fencing until
// offline_mode is on and no application session from before it remains.
func TestInPlaceFencing_WaitsForOfflineFenceAndSessionEviction(t *testing.T) {
	r, c, fg, primary := fencingRestore(t)
	primary.sessionsRemaining = 2

	if _, err := r.reconcileInPlaceRestore(context.Background(), fg); err != nil {
		t.Fatal(err)
	}
	fresh := getFG(t, c, fg)
	if fresh.Status.RestoreInPlace.Phase != v1alpha1.RestoreInPlaceFencing {
		t.Fatalf("phase = %s, want Fencing while application sessions remain", fresh.Status.RestoreInPlace.Phase)
	}
	if !strings.Contains(fresh.Status.RestoreInPlace.Message, "2 application session(s)") {
		t.Errorf("message = %q", fresh.Status.RestoreInPlace.Message)
	}
	q := fresh.Status.RestoreOfflineFence
	if q.PriorOfflineMode == nil || *q.PriorOfflineMode || q.SessionsEvicted {
		t.Fatalf("fence record = %+v, want prior=false recorded and sessions not yet evicted", q)
	}
	if !primary.offlineMode || !slices.Contains(primary.evictKeepUsers, "repl") {
		t.Fatalf("offline_mode=%v keep=%v, want the fence on with the replication user kept", primary.offlineMode, primary.evictKeepUsers)
	}

	primary.sessionsRemaining = 0
	*fg = *fresh
	if _, err := r.reconcileInPlaceRestore(context.Background(), fg); err != nil {
		t.Fatal(err)
	}
	fresh = getFG(t, c, fg)
	if fresh.Status.RestoreInPlace.Phase != v1alpha1.RestoreInPlaceRestoring || !fresh.Status.RestoreOfflineFence.SessionsEvicted {
		t.Fatalf("status = %+v fence %+v, want Restoring after the eviction is verified", fresh.Status.RestoreInPlace, fresh.Status.RestoreOfflineFence)
	}
	if !strings.Contains(strings.Join(drainEvents(r.Recorder.(*record.FakeRecorder)), "\n"), "RestoreOfflineFenceApplied") {
		t.Error("want a RestoreOfflineFenceApplied event")
	}
}

// offline_mode is never touched before its prior value is on record.
func TestInPlaceFencing_NoFenceBeforePriorIsRecorded(t *testing.T) {
	r, c, fg, primary := fencingRestore(t)
	primary.fenceErr = errors.New("connection refused")
	if _, err := r.reconcileInPlaceRestore(context.Background(), fg); err != nil {
		t.Fatal(err)
	}
	fresh := getFG(t, c, fg)
	if len(primary.offlineModeSets) != 0 {
		t.Fatalf("offline_mode was set %v before its prior value was read", primary.offlineModeSets)
	}
	if fresh.Status.RestoreInPlace.Phase != v1alpha1.RestoreInPlaceFencing || fresh.Status.RestoreOfflineFence.PriorOfflineMode != nil {
		t.Fatalf("status %+v fence %+v", fresh.Status.RestoreInPlace, fresh.Status.RestoreOfflineFence)
	}
}

func TestBuildInPlaceRestoreJob_FullInstanceRequiresOfflineMode(t *testing.T) {
	SetOperatorImageDefaults("bloodraven:test", "bloodraven")
	defer SetOperatorImageDefaults("", "")
	for _, tc := range []struct {
		schema string
		want   bool
	}{{"", true}, {"orders", false}} {
		fg := fgInPlaceRestore(tc.schema)
		r, _ := newReconciler(fg, succeededSeedBackup())
		job, err := r.buildInPlaceRestoreJob(context.Background(), fg, fg.Spec.Sites[0].Name, "creds", tc.schema == "", tc.schema)
		if err != nil {
			t.Fatal(err)
		}
		_, got := envMap(job.Spec.Template.Spec.Containers[0].Env)["BLOODRAVEN_REQUIRE_OFFLINE_MODE"]
		if got != tc.want {
			t.Errorf("schema=%q: BLOODRAVEN_REQUIRE_OFFLINE_MODE present=%v, want %v", tc.schema, got, tc.want)
		}
	}
}

// A full-instance restore needs the operator account to survive
// offline_mode. Without CONNECTION_ADMIN it is rejected before anything is
// fenced, without consuming confirm, and the rejection is stable.
func TestReconcileInPlaceRestore_RejectsOperatorWithoutConnectionAdmin(t *testing.T) {
	for _, tc := range []struct {
		name  string
		phase v1alpha1.RestoreInPlacePhase
	}{{name: "new request"}, {name: "at preflight", phase: v1alpha1.RestoreInPlacePreflight}} {
		t.Run(tc.name, func(t *testing.T) {
			fg := fgInPlaceRestore("")
			if tc.phase != "" {
				fg.Status.RestoreInPlace = &v1alpha1.RestoreInPlaceStatus{Phase: tc.phase, Scope: "full", StartTime: ptrTime(time.Now())}
			}
			r, c := newReconciler(fg, succeededSeedBackup(), dsnSecret())
			_, mocks := withFenceRunner(r, c, fg)
			mocks[fg.Spec.Sites[0].Name].noConnectionAdmin = true

			for i := 0; i < 2; i++ {
				if _, err := r.reconcileInPlaceRestore(context.Background(), fg); err != nil {
					t.Fatal(err)
				}
			}
			fresh := getFG(t, c, fg)
			st := fresh.Status.RestoreInPlace
			if st.Phase != v1alpha1.RestoreInPlaceFailed || !strings.Contains(st.Message, "CONNECTION_ADMIN") || st.ConfirmTokenUsed != "" {
				t.Fatalf("status = %+v, want a CONNECTION_ADMIN rejection that consumes nothing", st)
			}
			if fresh.Status.RestoreOfflineFence != nil || fresh.Status.RestoreReclones != nil || inPlaceRestoreFreezesTopology(fresh) {
				t.Fatal("a rejected restore must not fence or queue anything")
			}
			rv := fresh.ResourceVersion
			if _, err := r.reconcileInPlaceRestore(context.Background(), fresh); err != nil {
				t.Fatal(err)
			}
			if getFG(t, c, fg).ResourceVersion != rv {
				t.Fatal("a repeated rejection must not rewrite status")
			}
		})
	}
}

// Per-schema restores keep the Service up and are not offline_mode
// fenced, so they do not need the privilege.
func TestReconcileInPlaceRestore_PerSchemaNeedsNoConnectionAdmin(t *testing.T) {
	fg := fgInPlaceRestore("orders")
	fg.Status.RestoreInPlace = &v1alpha1.RestoreInPlaceStatus{Phase: v1alpha1.RestoreInPlacePreflight, Scope: "schema:orders", StartTime: ptrTime(time.Now())}
	r, c := newReconciler(fg, succeededSeedBackup(), dsnSecret())
	_, mocks := withFenceRunner(r, c, fg)
	mocks[fg.Spec.Sites[0].Name].noConnectionAdmin = true
	if _, err := r.reconcileInPlaceRestore(context.Background(), fg); err != nil {
		t.Fatal(err)
	}
	fresh := getFG(t, c, fg)
	if fresh.Status.RestoreInPlace.Phase != v1alpha1.RestoreInPlaceFencing || fresh.Status.RestoreOfflineFence != nil {
		t.Fatalf("status = %+v fence %+v, want Fencing without an offline_mode fence", fresh.Status.RestoreInPlace, fresh.Status.RestoreOfflineFence)
	}
}

func TestReconcileInPlaceRestore_PreflightWaitsWhenPrivilegeCannotBeChecked(t *testing.T) {
	fg := fgInPlaceRestore("")
	fg.Status.RestoreInPlace = &v1alpha1.RestoreInPlaceStatus{Phase: v1alpha1.RestoreInPlacePreflight, Scope: "full", StartTime: ptrTime(time.Now())}
	r, c := newReconciler(fg, succeededSeedBackup(), dsnSecret())
	_, mocks := withFenceRunner(r, c, fg)
	mocks[fg.Spec.Sites[0].Name].fenceErr = errors.New("i/o timeout")
	d, err := r.reconcileInPlaceRestore(context.Background(), fg)
	if err != nil {
		t.Fatal(err)
	}
	st := getFG(t, c, fg).Status.RestoreInPlace
	if d <= 0 || st.Phase != v1alpha1.RestoreInPlacePreflight || !strings.Contains(st.Message, "waiting to verify") {
		t.Fatalf("requeue %s status %+v, want Preflight waiting", d, st)
	}
}

// The fence is released, and the prior offline_mode restored, only once
// the restore has finished.
func TestRestoreOfflineFence_ReleasedAfterRestoreEnds(t *testing.T) {
	yes, no := true, false
	for _, tc := range []struct {
		name        string
		phase       v1alpha1.RestoreInPlacePhase
		prior       *bool
		releaseErr  error
		wantOffline bool
		wantCleared bool
	}{
		{name: "succeeded", phase: v1alpha1.RestoreInPlaceSucceeded, prior: &no, wantCleared: true},
		{name: "failed", phase: v1alpha1.RestoreInPlaceFailed, prior: &no, wantCleared: true},
		{name: "was already on", phase: v1alpha1.RestoreInPlaceSucceeded, prior: &yes, wantOffline: true, wantCleared: true},
		{name: "never applied", phase: v1alpha1.RestoreInPlaceFailed, wantOffline: true, wantCleared: true},
		{name: "still restoring", phase: v1alpha1.RestoreInPlaceRestoring, prior: &no, wantOffline: true},
		{name: "release fails", phase: v1alpha1.RestoreInPlaceSucceeded, prior: &no, releaseErr: errors.New("unreachable"), wantOffline: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			fg := fgInPlaceRestore("")
			site := fg.Spec.Sites[0].Name
			fg.Status.RestoreInPlace = &v1alpha1.RestoreInPlaceStatus{Phase: tc.phase, TargetSite: site, Scope: "full"}
			fg.Status.RestoreOfflineFence = &v1alpha1.RestoreOfflineFenceStatus{
				Site: site, ConfirmToken: testRestoreConfirm, PriorOfflineMode: tc.prior, SessionsEvicted: tc.prior != nil,
			}
			r, c := newReconciler(fg, dsnSecret())
			tm, mocks := withFenceRunner(r, c, fg)
			mocks[site].offlineMode = true
			mocks[site].fenceErr = tc.releaseErr
			tm.SetRestoreOfflineFence(site, testRestoreConfirm)

			d, err := r.reconcileRestoreOfflineFence(context.Background(), fg)
			if err != nil {
				t.Fatal(err)
			}
			fresh := getFG(t, c, fg)
			if mocks[site].offlineMode != tc.wantOffline {
				t.Errorf("offline_mode = %v, want %v", mocks[site].offlineMode, tc.wantOffline)
			}
			if cleared := fresh.Status.RestoreOfflineFence == nil; cleared != tc.wantCleared {
				t.Errorf("record cleared = %v, want %v", cleared, tc.wantCleared)
			}
			if tc.releaseErr != nil && d <= 0 {
				t.Errorf("a failed release must retry, got requeue %s", d)
			}
			tm.restoreFenceMu.Lock()
			reasserting := tm.restoreFenceSite
			tm.restoreFenceMu.Unlock()
			if tc.wantCleared && reasserting != "" {
				t.Errorf("the topology manager still re-asserts the fence on %q", reasserting)
			}
		})
	}
}

// A restarted mysqld comes back with offline_mode OFF; the topology
// manager re-asserts the fence on its next poll. A released fence is not
// re-armed by a stale sync.
func TestTopologyManager_ReassertsRestoreOfflineFence(t *testing.T) {
	fg := fgInPlaceRestore("")
	site := fg.Spec.Sites[0].Name
	tm, mocks := fenceTopologyManager(fg)

	tm.SetRestoreOfflineFence(site, "c1")
	tm.reassertRestoreOfflineFence(context.Background())
	if !mocks[site].offlineMode {
		t.Fatal("the fence was not re-asserted")
	}

	off := false
	if err := tm.ReleaseRestoreOfflineFence(context.Background(), site, "c1", &off); err != nil {
		t.Fatal(err)
	}
	tm.SetRestoreOfflineFence(site, "c1") // stale runner sync
	tm.reassertRestoreOfflineFence(context.Background())
	if mocks[site].offlineMode {
		t.Fatal("a released fence was re-armed")
	}
	if _, _, err := tm.EnforceRestoreOfflineFence(context.Background(), site, "c1"); err == nil {
		t.Fatal("enforcing a released fence must fail")
	}
}

func TestRestoreOfflineFenceSite_FromStatus(t *testing.T) {
	no := false
	for _, tc := range []struct {
		name  string
		phase v1alpha1.RestoreInPlacePhase
		scope string
		prior *bool
		want  string
	}{
		{name: "restoring", phase: v1alpha1.RestoreInPlaceRestoring, scope: "full", prior: &no, want: "iad"},
		{name: "fencing", phase: v1alpha1.RestoreInPlaceFencing, scope: "full", prior: &no, want: "iad"},
		{name: "prior not recorded", phase: v1alpha1.RestoreInPlaceRestoring, scope: "full"},
		{name: "per-schema", phase: v1alpha1.RestoreInPlaceRestoring, scope: "schema:orders", prior: &no},
		{name: "finished", phase: v1alpha1.RestoreInPlaceSucceeded, scope: "full", prior: &no},
	} {
		t.Run(tc.name, func(t *testing.T) {
			fg := &v1alpha1.MysqlFailoverGroup{}
			fg.Status.RestoreInPlace = &v1alpha1.RestoreInPlaceStatus{Phase: tc.phase, TargetSite: "iad", Scope: tc.scope}
			fg.Status.RestoreOfflineFence = &v1alpha1.RestoreOfflineFenceStatus{Site: "iad", ConfirmToken: "c", PriorOfflineMode: tc.prior}
			if got, _ := restoreOfflineFenceSite(fg); got != tc.want {
				t.Fatalf("site = %q, want %q", got, tc.want)
			}
		})
	}
}

// Restore Jobs run as the operator account: the backup role cannot DROP,
// CREATE or SET GLOBAL, and would be locked out by offline_mode.
func TestEnsureRestoreCredsSecret_UsesOperatorAccount(t *testing.T) {
	fg := fgWithBackup()
	fg.Spec.SecretName = ""
	fg.Spec.Credentials = &v1alpha1.CredentialsSpec{OperatorSecret: "op-creds", BackupSecret: "backup-creds"}
	op := &corev1.Secret{}
	op.Name, op.Namespace = "op-creds", fg.Namespace
	op.Data = map[string][]byte{"username": []byte("operator"), "password": []byte("op-pass")}
	bk := &corev1.Secret{}
	bk.Name, bk.Namespace = "backup-creds", fg.Namespace
	bk.Data = map[string][]byte{"username": []byte("backup"), "password": []byte("bk-pass")}
	r, c := newReconciler(fg, op, bk)
	if err := r.ensureRestoreCredsSecret(context.Background(), fg, "restore-creds"); err != nil {
		t.Fatal(err)
	}
	var got corev1.Secret
	if err := c.Get(context.Background(), types.NamespacedName{Name: "restore-creds", Namespace: fg.Namespace}, &got); err != nil {
		t.Fatal(err)
	}
	if string(got.Data["MYSQL_USER"]) != "operator" {
		t.Fatalf("restore credentials = %v, want the operator account", got.Data)
	}
}

// One full-instance restore end to end: the fence is recorded at
// acceptance, enabled before the Job exists, held while it runs, and
// lifted (prior value restored) once the restore succeeds.
func TestFullInstanceRestore_OfflineFenceLifecycle(t *testing.T) {
	SetOperatorImageDefaults("bloodraven:test", "bloodraven")
	defer SetOperatorImageDefaults("", "")
	fg := fgInPlaceRestore("")
	site := fg.Spec.Sites[0].Name
	fg.Status.RestoreInPlace = &v1alpha1.RestoreInPlaceStatus{Phase: v1alpha1.RestoreInPlacePreflight, Scope: "full", StartTime: ptrTime(time.Now())}
	r, c := newReconciler(fg, succeededSeedBackup(), dsnSecret(), primaryFencedPod(fg.Name, site))
	tm, mocks := withFenceRunner(r, c, fg)
	primary := mocks[site]
	step := func() {
		t.Helper()
		*fg = *getFG(t, c, fg)
		if _, err := r.reconcileInPlaceRestore(context.Background(), fg); err != nil {
			t.Fatal(err)
		}
		if _, err := r.reconcileRestoreOfflineFence(context.Background(), fg); err != nil {
			t.Fatal(err)
		}
		*fg = *getFG(t, c, fg)
	}

	step() // Preflight -> Fencing
	if fg.Status.RestoreInPlace.Phase != v1alpha1.RestoreInPlaceFencing || fg.Status.RestoreOfflineFence == nil || primary.offlineMode {
		t.Fatalf("after acceptance: phase %s fence %+v offline=%v; want Fencing, fence recorded, offline_mode untouched",
			fg.Status.RestoreInPlace.Phase, fg.Status.RestoreOfflineFence, primary.offlineMode)
	}
	step() // Fencing -> Restoring
	if fg.Status.RestoreInPlace.Phase != v1alpha1.RestoreInPlaceRestoring || !primary.offlineMode {
		t.Fatalf("phase %s offline=%v, want Restoring with the fence on", fg.Status.RestoreInPlace.Phase, primary.offlineMode)
	}
	step() // creates the Job
	job := runningInPlaceJob(fg, site, fg.Spec.RestoreInPlace.Confirm, time.Now())
	if env := envMap(getJob(t, c, job).Spec.Template.Spec.Containers[0].Env); env["BLOODRAVEN_REQUIRE_OFFLINE_MODE"] != "1" {
		t.Fatalf("restore Job env = %v, want BLOODRAVEN_REQUIRE_OFFLINE_MODE=1", env)
	}
	if s, _ := restoreOfflineFenceSite(fg); s != site {
		t.Fatalf("runner sync would not re-assert the fence: site %q", s)
	}

	// mysqld restarted mid-restore: the next poll re-arms the fence.
	primary.offlineMode = false
	tm.reassertRestoreOfflineFence(context.Background())
	if !primary.offlineMode {
		t.Fatal("the fence was not re-asserted while the restore runs")
	}

	completeJob(t, r, job)
	step() // Restoring -> Resuming
	step() // Resuming -> Succeeded, fence released
	if fg.Status.RestoreInPlace.Phase != v1alpha1.RestoreInPlaceSucceeded {
		t.Fatalf("phase = %s", fg.Status.RestoreInPlace.Phase)
	}
	if primary.offlineMode || fg.Status.RestoreOfflineFence != nil {
		t.Fatalf("after success offline=%v fence %+v, want released", primary.offlineMode, fg.Status.RestoreOfflineFence)
	}
}

func getJob(t *testing.T, c client.Client, job *batchv1.Job) *batchv1.Job {
	t.Helper()
	var got batchv1.Job
	if err := c.Get(context.Background(), types.NamespacedName{Name: job.Name, Namespace: job.Namespace}, &got); err != nil {
		t.Fatalf("get job %s: %v", job.Name, err)
	}
	return &got
}

// stepFullRestore runs one in-place restore reconcile and the fence
// release, as one Reconcile pass does, and refreshes fg.
func stepFullRestore(t *testing.T, r *MysqlFailoverGroupReconciler, c client.Client, fg *v1alpha1.MysqlFailoverGroup) {
	t.Helper()
	*fg = *getFG(t, c, fg)
	if _, err := r.reconcileInPlaceRestore(context.Background(), fg); err != nil {
		t.Fatal(err)
	}
	if _, err := r.reconcileRestoreOfflineFence(context.Background(), fg); err != nil {
		t.Fatal(err)
	}
	*fg = *getFG(t, c, fg)
}

// A full restore cancelled after its fence went up keeps its confirm
// unconsumed, so the same manifest can be applied again. That is a new
// run: it must apply and verify its own fence, not find the cancelled
// run's fence "already released" and wait in Fencing forever with the
// primary Service emptied. If the cancelled run's release has not gone
// through yet, the new run waits for it and then fences afresh.
func TestFullRestore_ReappliedAfterCancelGetsItsOwnFence(t *testing.T) {
	for _, tc := range []struct {
		name         string
		releaseFails bool
	}{
		{name: "released"},
		{name: "release still retrying", releaseFails: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			fg := fgInPlaceRestore("")
			site := fg.Spec.Sites[0].Name
			manifest := fg.Spec.RestoreInPlace.DeepCopy()
			r, c := newReconciler(fg, succeededSeedBackup(), dsnSecret(), primaryFencedPod(fg.Name, site))
			_, mocks := withFenceRunner(r, c, fg)
			primary := mocks[site]

			for i := 0; i < 3; i++ { // new -> Preflight -> Fencing -> Restoring
				stepFullRestore(t, r, c, fg)
			}
			if fg.Status.RestoreInPlace.Phase != v1alpha1.RestoreInPlaceRestoring || fg.Status.RestoreInPlace.JobName != "" || !primary.offlineMode {
				t.Fatalf("status %+v offline=%v, want Restoring before the Job with the fence on", fg.Status.RestoreInPlace, primary.offlineMode)
			}

			fg.Spec.RestoreInPlace = nil
			if err := c.Update(context.Background(), fg); err != nil {
				t.Fatal(err)
			}
			if tc.releaseFails {
				primary.fenceErr = errors.New("connection refused")
			}
			stepFullRestore(t, r, c, fg)
			if st := fg.Status.RestoreInPlace; st.Phase != v1alpha1.RestoreInPlaceFailed || !strings.Contains(st.Message, "cancelled") {
				t.Fatalf("status = %+v, want cancelled", st)
			}
			if got := fg.Status.RestoreOfflineFence != nil; got != tc.releaseFails {
				t.Fatalf("fence record kept = %v, want %v", got, tc.releaseFails)
			}

			fg.Spec.RestoreInPlace = manifest
			if err := c.Update(context.Background(), fg); err != nil {
				t.Fatal(err)
			}
			evictionsBefore := primary.evictCalls
			stepFullRestore(t, r, c, fg) // new run -> Preflight
			if tc.releaseFails {
				stepFullRestore(t, r, c, fg)
				// The primary is unreachable, so the new run waits in
				// Preflight; the cancelled run's record is not taken over.
				if st := fg.Status.RestoreInPlace; st.Phase != v1alpha1.RestoreInPlacePreflight {
					t.Fatalf("status = %+v, want Preflight waiting while the cancelled run's fence is unreleased", st)
				}
				if q := fg.Status.RestoreOfflineFence; q == nil || !q.SessionsEvicted {
					t.Fatalf("fence record = %+v, want the cancelled run's record kept until it is released", q)
				}
				primary.fenceErr = nil
			}
			for i := 0; i < 4 && fg.Status.RestoreInPlace.Phase != v1alpha1.RestoreInPlaceRestoring; i++ {
				stepFullRestore(t, r, c, fg)
			}
			st := fg.Status.RestoreInPlace
			if st.Phase != v1alpha1.RestoreInPlaceRestoring || !primary.offlineMode {
				t.Fatalf("re-applied run: status %+v offline=%v, want Restoring with offline_mode ON", st, primary.offlineMode)
			}
			if primary.evictCalls == evictionsBefore {
				t.Fatal("the re-applied run reached Restoring without evicting sessions under its own fence")
			}
		})
	}
}

// The release puts back the recorded prior value even when it is ON: a
// mysqld that restarted during the restore came back with offline_mode
// OFF. The record stays until that worked.
func TestRestoreOfflineFence_ReleaseRestoresPriorOnAfterRestart(t *testing.T) {
	yes := true
	fg := fgInPlaceRestore("")
	site := fg.Spec.Sites[0].Name
	fg.Status.RestoreInPlace = &v1alpha1.RestoreInPlaceStatus{Phase: v1alpha1.RestoreInPlaceFailed, TargetSite: site, Scope: "full"}
	fg.Status.RestoreOfflineFence = &v1alpha1.RestoreOfflineFenceStatus{
		Site: site, ConfirmToken: testRestoreConfirm, PriorOfflineMode: &yes, SessionsEvicted: true,
	}
	r, c := newReconciler(fg, dsnSecret())
	_, mocks := withFenceRunner(r, c, fg)
	primary := mocks[site]
	primary.offlineMode = false // restarted
	primary.fenceErr = errors.New("connection refused")

	if d, err := r.reconcileRestoreOfflineFence(context.Background(), fg); err != nil || d <= 0 {
		t.Fatalf("requeue %s err %v, want a retry while the primary is unreachable", d, err)
	}
	if getFG(t, c, fg).Status.RestoreOfflineFence == nil {
		t.Fatal("the fence record was cleared before the prior ON was put back")
	}

	primary.fenceErr = nil
	*fg = *getFG(t, c, fg)
	if _, err := r.reconcileRestoreOfflineFence(context.Background(), fg); err != nil {
		t.Fatal(err)
	}
	if !primary.offlineMode {
		t.Fatal("offline_mode is OFF after release; the prior value was ON")
	}
	if getFG(t, c, fg).Status.RestoreOfflineFence != nil {
		t.Fatal("the fence record must be cleared once the prior value is back")
	}
}

// The preflight check covers every privilege the fence and the restore Job
// need, not just CONNECTION_ADMIN, and names the missing ones.
func TestReconcileInPlaceRestore_RejectionNamesMissingFencePrivileges(t *testing.T) {
	fg := fgInPlaceRestore("")
	r, c := newReconciler(fg, succeededSeedBackup(), dsnSecret())
	_, mocks := withFenceRunner(r, c, fg)
	mocks[fg.Spec.Sites[0].Name].missingFencePrivs = []string{"PROCESS", "REPLICATION_SLAVE_ADMIN"}
	if _, err := r.reconcileInPlaceRestore(context.Background(), fg); err != nil {
		t.Fatal(err)
	}
	st := getFG(t, c, fg).Status.RestoreInPlace
	if st.Phase != v1alpha1.RestoreInPlaceFailed || !strings.Contains(st.Message, "PROCESS, REPLICATION_SLAVE_ADMIN") {
		t.Fatalf("status = %+v, want a rejection naming PROCESS and REPLICATION_SLAVE_ADMIN", st)
	}
}

// countingPrivMock counts privilege checks.
type countingPrivMock struct {
	*mockMySQL
	checks int
}

func (m *countingPrivMock) MissingRestoreFencePrivileges(ctx context.Context) ([]string, error) {
	m.checks++
	return m.mockMySQL.MissingRestoreFencePrivileges(ctx)
}

// A standing privilege rejection does not consume the confirm, so every
// reconcile reaches the check. It queries MySQL (up to the safety timeout
// when the primary is unreachable), so it is repeated at most once a
// minute; a new confirm or spec edit is checked at once.
func TestReconcileInPlaceRestore_PrivilegeRejectionIsRateLimited(t *testing.T) {
	fg := fgInPlaceRestore("")
	site := fg.Spec.Sites[0].Name
	r, c := newReconciler(fg, succeededSeedBackup(), dsnSecret())
	tm, mocks := withFenceRunner(r, c, fg)
	counting := &countingPrivMock{mockMySQL: mocks[site]}
	counting.noConnectionAdmin = true
	tm.sites[0].mysql = counting

	for i := 0; i < 3; i++ {
		*fg = *getFG(t, c, fg)
		if _, err := r.reconcileInPlaceRestore(context.Background(), fg); err != nil {
			t.Fatal(err)
		}
	}
	if st := getFG(t, c, fg).Status.RestoreInPlace; st.Phase != v1alpha1.RestoreInPlaceFailed {
		t.Fatalf("status = %+v, want the privilege rejection", st)
	}
	if counting.checks != 1 {
		t.Fatalf("privilege checks = %d over 3 reconciles, want 1", counting.checks)
	}

	nn := types.NamespacedName{Name: fg.Name, Namespace: fg.Namespace}
	r.restorePrivMu.Lock()
	last := r.restorePrivRejected[nn]
	last.at = time.Now().Add(-2 * restorePrivilegeRecheck)
	r.restorePrivRejected[nn] = last
	r.restorePrivMu.Unlock()
	counting.noConnectionAdmin = false
	*fg = *getFG(t, c, fg)
	if _, err := r.reconcileInPlaceRestore(context.Background(), fg); err != nil {
		t.Fatal(err)
	}
	if counting.checks != 2 {
		t.Fatalf("privilege checks = %d, want a re-check once the interval passed", counting.checks)
	}
	if st := getFG(t, c, fg).Status.RestoreInPlace; st.Phase != v1alpha1.RestoreInPlacePreflight {
		t.Fatalf("status = %+v, want the restore to start once the grant is in place", st)
	}
}

// SYSTEM_USER sessions the operator account may not kill do not hold the
// fence forever: offline_mode already refuses them unless they hold
// CONNECTION_ADMIN, which would let them reconnect anyway. A Warning
// names them.
func TestInPlaceFencing_UnkillableSystemUserSessionsDoNotBlock(t *testing.T) {
	r, c, fg, primary := fencingRestore(t)
	primary.sessionsProtected = []string{"root"}
	for i := 0; i < 2; i++ {
		*fg = *getFG(t, c, fg)
		if _, err := r.reconcileInPlaceRestore(context.Background(), fg); err != nil {
			t.Fatal(err)
		}
	}
	if st := getFG(t, c, fg).Status.RestoreInPlace; st.Phase != v1alpha1.RestoreInPlaceRestoring {
		t.Fatalf("status = %+v, want Restoring", st)
	}
	if events := strings.Join(drainEvents(r.Recorder.(*record.FakeRecorder)), "\n"); !strings.Contains(events, "RestoreOfflineFenceSessionsKept") || !strings.Contains(events, "root") {
		t.Fatalf("events %q, want a RestoreOfflineFenceSessionsKept warning naming root", events)
	}
}

// The re-assert runs from Poll itself, frozen topology or not.
func TestTopologyManager_PollReassertsRestoreOfflineFence(t *testing.T) {
	fg := fgInPlaceRestore("")
	site := fg.Spec.Sites[0].Name
	tm, mocks := fenceTopologyManager(fg)
	tm.SetTopologyFrozen(true)
	tm.SetRestoreOfflineFence(site, "run-1")
	tm.Poll(context.Background())
	if !mocks[site].offlineMode {
		t.Fatal("Poll did not re-assert the restore offline_mode fence")
	}
}

// The peers are not cloned while the restore's fence is still up: a
// replication account without CONNECTION_ADMIN is refused under it.
func TestRestoreReclone_WaitsForOfflineFenceRelease(t *testing.T) {
	no := false
	fg := fgDrainingRestore("pdx")
	fg.Status.RestoreOfflineFence = &v1alpha1.RestoreOfflineFenceStatus{Site: "iad", ConfirmToken: testRestoreConfirm, PriorOfflineMode: &no}
	r, c := newReconciler(fg, dsnSecret())
	if d, err := r.reconcileRestoreReclones(context.Background(), fg); err != nil || d <= 0 {
		t.Fatalf("requeue %s err %v, want a wait", d, err)
	}
	if got := getFG(t, c, fg).GetAnnotations()[RecloneAnnotation]; got != "" {
		t.Fatalf("a reclone was requested under the fence: %q", got)
	}
}

// Reconcile itself releases a finished restore's fence, and does so before
// the reclone drain runs, so the first peer clone of the pass does not hit
// offline_mode.
func TestReconcile_ReleasesRestoreFenceBeforeReclones(t *testing.T) {
	no := false
	fg := newTestFG()
	fg.Status.ActiveSite = "dc1"
	fg.Status.RestoreInPlace = &v1alpha1.RestoreInPlaceStatus{
		Phase: v1alpha1.RestoreInPlaceSucceeded, TargetSite: "dc1", Scope: "full", ConfirmTokenUsed: testRestoreConfirm,
	}
	fg.Status.RestoreOfflineFence = &v1alpha1.RestoreOfflineFenceStatus{
		Site: "dc1", ConfirmToken: testRestoreConfirm, PriorOfflineMode: &no, SessionsEvicted: true,
	}
	fg.Status.RestoreReclones = &v1alpha1.RestoreReclonesStatus{
		ConfirmToken: testRestoreConfirm, Phase: v1alpha1.RestoreReclonesDraining, Pending: []string{"dc2"},
	}
	r, c := newReconciler(fg)
	_, mocks := withFenceRunner(r, c, fg)
	mocks["dc1"].offlineMode = true

	if _, err := r.Reconcile(context.Background(), ctrl.Request{NamespacedName: types.NamespacedName{Name: fg.Name, Namespace: fg.Namespace}}); err != nil {
		t.Fatal(err)
	}
	fresh := getFG(t, c, fg)
	if mocks["dc1"].offlineMode || fresh.Status.RestoreOfflineFence != nil {
		t.Fatalf("offline_mode=%v fence %+v, want released by Reconcile", mocks["dc1"].offlineMode, fresh.Status.RestoreOfflineFence)
	}
	if got := fresh.GetAnnotations()[RecloneAnnotation]; !strings.HasPrefix(got, "dc2") {
		t.Fatalf("reclone annotation = %q, want dc2 requested in the same pass, after the release", got)
	}
}

// A standing privilege rejection must schedule its own re-check: on a
// quiet group nothing else triggers a reconcile once the grant is in
// place, and the docs promise the restore then starts by itself.
func TestReconcile_PrivilegeRejectionSchedulesRecheck(t *testing.T) {
	fg := fgInPlaceRestore("")
	r, c := newReconciler(fg, succeededSeedBackup(), dsnSecret())
	_, mocks := withFenceRunner(r, c, fg)
	primary := mocks[fg.Spec.Sites[0].Name]
	primary.noConnectionAdmin = true
	nn := types.NamespacedName{Name: fg.Name, Namespace: fg.Namespace}

	res, err := r.Reconcile(context.Background(), ctrl.Request{NamespacedName: nn})
	if err != nil {
		t.Fatal(err)
	}
	if st := getFG(t, c, fg).Status.RestoreInPlace; st == nil || !strings.HasPrefix(st.Message, restorePrivilegeRejectionPrefix) {
		t.Fatalf("status = %+v, want the privilege rejection", st)
	}
	if res.RequeueAfter <= 0 || res.RequeueAfter > restorePrivilegeRecheck+time.Second {
		t.Fatalf("RequeueAfter = %s, want a re-check within %s", res.RequeueAfter, restorePrivilegeRecheck)
	}

	// The grant lands; the scheduled reconcile after the interval starts
	// the restore with the same confirm.
	primary.noConnectionAdmin = false
	r.restorePrivMu.Lock()
	last := r.restorePrivRejected[nn]
	last.at = last.at.Add(-2 * restorePrivilegeRecheck)
	r.restorePrivRejected[nn] = last
	r.restorePrivMu.Unlock()
	if _, err := r.Reconcile(context.Background(), ctrl.Request{NamespacedName: nn}); err != nil {
		t.Fatal(err)
	}
	if st := getFG(t, c, fg).Status.RestoreInPlace; st.Phase != v1alpha1.RestoreInPlacePreflight {
		t.Fatalf("status = %+v, want the restore started once the grant is in place", st)
	}
}

func TestRestorePrivilegeRecheckRequeue(t *testing.T) {
	rejected := func() *v1alpha1.MysqlFailoverGroup {
		fg := fgInPlaceRestore("")
		fg.Status.RestoreInPlace = &v1alpha1.RestoreInPlaceStatus{
			Phase: v1alpha1.RestoreInPlaceFailed, Message: restorePrivilegeRejectionPrefix + "CONNECTION_ADMIN on iad",
		}
		return fg
	}
	r := &MysqlFailoverGroupReconciler{}
	if d := r.restorePrivilegeRecheckRequeue(rejected()); d <= 0 {
		t.Errorf("unrecorded rejection: requeue %s, want a prompt re-check", d)
	}
	fg := rejected()
	r.noteRestorePrivilegeCheck(fg, fg.Spec.RestoreInPlace, true)
	if d := r.restorePrivilegeRecheckRequeue(fg); d < restorePrivilegeRecheck-time.Second || d > restorePrivilegeRecheck+time.Second {
		t.Errorf("fresh rejection: requeue %s, want about %s", d, restorePrivilegeRecheck)
	}
	other := rejected()
	other.Status.RestoreInPlace.Message = "restoreInPlace.pointInTime is not supported"
	if d := r.restorePrivilegeRecheckRequeue(other); d != 0 {
		t.Errorf("other rejection: requeue %s, want none", d)
	}
	consumed := rejected()
	consumed.Status.RestoreInPlace.ConfirmTokenUsed = consumed.Spec.RestoreInPlace.Confirm
	if d := r.restorePrivilegeRecheckRequeue(consumed); d != 0 {
		t.Errorf("confirm no longer advances: requeue %s, want none", d)
	}
	removed := rejected()
	removed.Spec.RestoreInPlace = nil
	if d := r.restorePrivilegeRecheckRequeue(removed); d != 0 {
		t.Errorf("spec removed: requeue %s, want none", d)
	}
}

// The topology poller must not write a stale status.restoreOfflineFence
// back over the reconciler's (here, the prior value recorded while the
// poll ran), nor bring back a record the release cleared.
func TestUpdateCRStatus_DoesNotClobberRestoreOfflineFence(t *testing.T) {
	off := false
	t.Run("concurrent prior write survives", func(t *testing.T) {
		fg := encTestFG()
		fg.Status.RestoreOfflineFence = &v1alpha1.RestoreOfflineFenceStatus{Site: "dc1", ConfirmToken: "B", RunID: "run-b"}
		after := pollAfterConcurrentWrite(t, fg, func(l *v1alpha1.MysqlFailoverGroup) {
			l.Status.RestoreOfflineFence.PriorOfflineMode = &off
			l.Status.RestoreOfflineFence.SessionsEvicted = true
		})
		if q := after.Status.RestoreOfflineFence; q == nil || q.PriorOfflineMode == nil || !q.SessionsEvicted {
			t.Fatalf("the poller wrote the fence record back to %+v", q)
		}
	})
	t.Run("released record stays gone", func(t *testing.T) {
		fg := encTestFG()
		fg.Status.RestoreOfflineFence = &v1alpha1.RestoreOfflineFenceStatus{Site: "dc1", ConfirmToken: "B", RunID: "run-b", PriorOfflineMode: &off}
		after := pollAfterConcurrentWrite(t, fg, func(l *v1alpha1.MysqlFailoverGroup) {
			l.Status.RestoreOfflineFence = nil
		})
		if q := after.Status.RestoreOfflineFence; q != nil {
			t.Fatalf("the poller resurrected a released fence record: %+v", q)
		}
	})
}

// staleFenceReader returns c with its first group read carrying stale as
// status.restoreOfflineFence, as an informer cache that has not seen the
// reconciler's last fence write.
func staleFenceReader(c client.WithWatch, stale *v1alpha1.RestoreOfflineFenceStatus) client.Client {
	fired := false
	return interceptor.NewClient(c, interceptor.Funcs{
		Get: func(ctx context.Context, cl client.WithWatch, key client.ObjectKey, obj client.Object, opts ...client.GetOption) error {
			if err := cl.Get(ctx, key, obj, opts...); err != nil {
				return err
			}
			if fg, ok := obj.(*v1alpha1.MysqlFailoverGroup); ok && !fired {
				fired = true
				fg.Status.RestoreOfflineFence = stale.DeepCopy()
			}
			return nil
		},
	})
}

// End to end: a poll whose cached read predates the prior-value write must
// not leave offline_mode ON after the restore succeeds.
func TestFullInstanceRestore_StalePollDoesNotLeaveOfflineModeOn(t *testing.T) {
	SetOperatorImageDefaults("bloodraven:test", "bloodraven")
	defer SetOperatorImageDefaults("", "")
	fg := fgInPlaceRestore("")
	site := fg.Spec.Sites[0].Name
	fg.Status.RestoreInPlace = &v1alpha1.RestoreInPlaceStatus{Phase: v1alpha1.RestoreInPlacePreflight, Scope: "full", StartTime: ptrTime(time.Now())}
	r, c := newReconciler(fg, succeededSeedBackup(), dsnSecret(), primaryFencedPod(fg.Name, site))
	_, mocks := withFenceRunner(r, c, fg)
	primary := mocks[site]

	stepFullRestore(t, r, c, fg) // Preflight -> Fencing: fence recorded, no prior yet
	stale := fg.Status.RestoreOfflineFence.DeepCopy()
	stepFullRestore(t, r, c, fg) // Fencing -> Restoring: prior recorded, offline_mode ON
	if !primary.offlineMode || fg.Status.RestoreOfflineFence.PriorOfflineMode == nil {
		t.Fatalf("setup: offline=%v fence=%+v", primary.offlineMode, fg.Status.RestoreOfflineFence)
	}
	poller := &TopologyManagerRunner{client: staleFenceReader(c.(client.WithWatch), stale), logger: testLogger(),
		managers: map[types.NamespacedName]*managedTopology{}}
	if err := poller.updateCRStatus(context.Background(), client.ObjectKeyFromObject(fg), TopologySnapshot{
		Sites:      []SiteSnapshot{{Name: site, State: state.StateWritable}, {Name: fg.Spec.Sites[1].Name, State: state.StateReadOnly}},
		ActiveSite: site, TopologyGeneration: fg.Status.TopologyGeneration,
	}); err != nil {
		t.Fatal(err)
	}

	stepFullRestore(t, r, c, fg) // creates the Job
	job := runningInPlaceJob(fg, site, fg.Spec.RestoreInPlace.Confirm, time.Now())
	completeJob(t, r, job)
	stepFullRestore(t, r, c, fg) // Restoring -> Resuming
	stepFullRestore(t, r, c, fg) // Resuming -> Succeeded, fence released
	if fg.Status.RestoreInPlace.Phase != v1alpha1.RestoreInPlaceSucceeded || fg.Status.RestoreOfflineFence != nil || primary.offlineMode {
		t.Fatalf("phase=%s fence=%+v offline_mode=%v, want Succeeded with the fence released and offline_mode OFF",
			fg.Status.RestoreInPlace.Phase, fg.Status.RestoreOfflineFence, primary.offlineMode)
	}
}

// A run an older operator accepted (no privilege check, no fence record)
// and left in Fencing across the upgrade is checked before offline_mode is
// touched: without CONNECTION_ADMIN the fence would lock the operator out.
func TestInPlaceFencing_UpgradedRunIsPrivilegeChecked(t *testing.T) {
	r, c, fg, primary := fencingRestore(t)
	fg.Status.RestoreOfflineFence = nil
	if err := c.Status().Update(context.Background(), fg); err != nil {
		t.Fatal(err)
	}
	primary.noConnectionAdmin = true
	for i := 0; i < 4; i++ {
		*fg = *getFG(t, c, fg)
		if _, err := r.reconcileInPlaceRestore(context.Background(), fg); err != nil {
			t.Fatal(err)
		}
	}
	fresh := getFG(t, c, fg)
	if len(primary.offlineModeSets) != 0 {
		t.Fatalf("offline_mode was set %v for an operator account without CONNECTION_ADMIN", primary.offlineModeSets)
	}
	if st := fresh.Status.RestoreInPlace; st.Phase != v1alpha1.RestoreInPlaceFailed || !strings.HasPrefix(st.Message, restorePrivilegeRejectionPrefix) {
		t.Fatalf("status = %+v, want the privilege rejection", st)
	}
	if fresh.Status.RestoreOfflineFence != nil {
		t.Fatalf("fence record = %+v, want none for a rejected run", fresh.Status.RestoreOfflineFence)
	}
}

// The full-instance form of the reverted-manifest cancel: no Job is built
// and the fence and queue settle as for a run that never executed.
func TestFullRestore_RevertedManifestBeforeJobCancels(t *testing.T) {
	SetOperatorImageDefaults("bloodraven:test", "bloodraven")
	defer SetOperatorImageDefaults("", "")
	fg := fgInPlaceRestore("")
	site := fg.Spec.Sites[0].Name
	executed := fg.Spec.RestoreInPlace.Confirm
	fg.Status.RestoreInPlace = &v1alpha1.RestoreInPlaceStatus{
		Phase: v1alpha1.RestoreInPlacePreflight, Scope: "full", StartTime: ptrTime(time.Now()),
		ConfirmTokenUsed: executed, RunConfirm: "2026-04-18T09:00:00Z", RunID: "run-c3",
	}
	r, c := newReconciler(fg, succeededSeedBackup(), dsnSecret(), primaryFencedPod(fg.Name, site))
	_, mocks := withFenceRunner(r, c, fg)
	for i := 0; i < 6; i++ {
		stepFullRestore(t, r, c, fg)
	}
	var job batchv1.Job
	if err := c.Get(context.Background(), types.NamespacedName{Namespace: fg.Namespace, Name: inPlaceRestoreJobName(fg.Name, site)}, &job); err == nil {
		t.Fatalf("a destructive Job was built from the reverted spec (job confirm %s)", inPlaceRestoreJobConfirm(&job))
	}
	if st := fg.Status.RestoreInPlace; st.Phase != v1alpha1.RestoreInPlaceFailed || st.ConfirmTokenUsed != executed {
		t.Fatalf("status = %+v, want a cancellation keeping %s", st, executed)
	}
	if mocks[site].offlineMode || fg.Status.RestoreOfflineFence != nil {
		t.Fatalf("offline_mode=%v fence=%+v, want no fence left", mocks[site].offlineMode, fg.Status.RestoreOfflineFence)
	}
}
