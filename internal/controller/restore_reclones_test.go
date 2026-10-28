package controller

import (
	"context"
	"errors"
	"slices"
	"strings"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/tools/record"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"

	v1alpha1 "github.com/shipstream/bloodraven/api/v1alpha1"
	"github.com/shipstream/bloodraven/internal/mysql"
	"github.com/shipstream/bloodraven/internal/platform"
	"github.com/shipstream/bloodraven/internal/state"
)

const testRestoreConfirm = "2026-04-17T14:00:00Z"

// fgInPlaceDC returns fgInPlaceRestore with its sites renamed to the
// dc1/dc2 names newTestTopologyManagerWithBootstrap uses, so the real
// reclone consumer can be driven against the reconciler's output.
func fgInPlaceDC() *v1alpha1.MysqlFailoverGroup {
	fg := fgInPlaceRestore("")
	fg.Spec.Sites[0].Name, fg.Spec.Sites[1].Name = "dc1", "dc2"
	fg.Status.ActiveSite = "dc1"
	fg.Status.Sites[0].Name, fg.Status.Sites[1].Name = "dc1", "dc2"
	return fg
}

// fgThreeSiteRestore is a full-instance in-place restore target (iad)
// with two peers, pdx and sea.
func fgThreeSiteRestore() *v1alpha1.MysqlFailoverGroup {
	fg := fgInPlaceRestore("")
	third := fg.Spec.Sites[1]
	third.Name, third.LBIP = "sea", "10.0.0.3"
	fg.Spec.Sites = append(fg.Spec.Sites, third)
	fg.Status.Sites = append(fg.Status.Sites, v1alpha1.SiteStatus{Name: "sea", State: "read-only"})
	return fg
}

// fgDrainingRestore is fgThreeSiteRestore after its restore succeeded,
// with pending still queued for reclone.
func fgDrainingRestore(pending ...string) *v1alpha1.MysqlFailoverGroup {
	fg := fgThreeSiteRestore()
	fg.Status.RestoreInPlace = &v1alpha1.RestoreInPlaceStatus{
		Phase: v1alpha1.RestoreInPlaceSucceeded, TargetSite: "iad", Scope: "full", ConfirmTokenUsed: testRestoreConfirm,
	}
	fg.Status.RestoreReclones = drainingQueue(pending...)
	return fg
}

func drainingQueue(pending ...string) *v1alpha1.RestoreReclonesStatus {
	return &v1alpha1.RestoreReclonesStatus{
		ConfirmToken: testRestoreConfirm,
		Phase:        v1alpha1.RestoreReclonesDraining,
		Pending:      pending,
	}
}

func getFG(t *testing.T, c client.Client, fg *v1alpha1.MysqlFailoverGroup) *v1alpha1.MysqlFailoverGroup {
	t.Helper()
	var fresh v1alpha1.MysqlFailoverGroup
	if err := c.Get(context.Background(), types.NamespacedName{Name: fg.Name, Namespace: fg.Namespace}, &fresh); err != nil {
		t.Fatalf("get fg: %v", err)
	}
	return &fresh
}

// reconcileRestoreStep runs the in-place phase step and the queue step,
// as one MysqlFailoverGroup reconcile does.
func reconcileRestoreStep(t *testing.T, r *MysqlFailoverGroupReconciler, fg *v1alpha1.MysqlFailoverGroup) time.Duration {
	t.Helper()
	if _, err := r.reconcileInPlaceRestore(context.Background(), fg); err != nil {
		t.Fatalf("reconcileInPlaceRestore: %v", err)
	}
	d, err := r.reconcileRestoreReclones(context.Background(), fg)
	if err != nil {
		t.Fatalf("reconcileRestoreReclones: %v", err)
	}
	return d
}

// finishReclone plays the runner's RecloneCompleteCallback for site.
func finishReclone(t *testing.T, c client.Client, rec record.EventRecorder, fg *v1alpha1.MysqlFailoverGroup, site string, outcome RecloneOutcome) {
	t.Helper()
	runner := &TopologyManagerRunner{client: c, recorder: rec, logger: testLogger()}
	nn := types.NamespacedName{Name: fg.Name, Namespace: fg.Namespace}
	if err := runner.recordRestoreRecloneOutcome(context.Background(), nn, site, outcome); err != nil {
		t.Fatalf("recordRestoreRecloneOutcome: %v", err)
	}
	if err := runner.removeRecloneAnnotationForSite(context.Background(), nn, site); err != nil {
		t.Fatalf("removeRecloneAnnotationForSite: %v", err)
	}
	*fg = *getFG(t, c, fg)
}

// A full-instance in-place restore must hand its peer to the reclone
// consumer in a form the consumer's safety interlock accepts. It used to
// write the bare site name, which validateRecloneRequest always rejects
// (cold reclones need "<site>:confirm=<group>", divergent ones the
// divergent-GTID prefix), so the handoff was dropped while the restore
// reported Succeeded.
func TestRestoreReclone_ConsumerAcceptsRestoreRequest(t *testing.T) {
	for _, tc := range []struct {
		name      string
		divergent string
	}{
		{name: "cold"},
		{name: "divergent", divergent: "3e11fa47-71ca-11e1-9e33-c80aa9429562:23-25,\n4f22ab58-71ca-11e1-9e33-c80aa9429562:1-2"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			fg := fgInPlaceDC()
			fg.Status.Sites[1].DivergentGtid = tc.divergent
			fg.Status.RestoreInPlace = &v1alpha1.RestoreInPlaceStatus{
				Phase: v1alpha1.RestoreInPlaceResuming, TargetSite: "dc1", Scope: "full",
				ConfirmTokenUsed: testRestoreConfirm, StartTime: ptrTime(time.Now()),
			}
			fg.Status.RestoreReclones = &v1alpha1.RestoreReclonesStatus{
				ConfirmToken: testRestoreConfirm, Phase: v1alpha1.RestoreReclonesArmed, Pending: []string{"dc2"},
			}
			r, c := newReconciler(fg, succeededSeedBackup(), dsnSecret())

			// Resuming -> Succeeded releases the queue; the drain requests dc2.
			reconcileRestoreStep(t, r, fg)
			fresh := getFG(t, c, fg)
			if fresh.Status.RestoreInPlace.Phase != v1alpha1.RestoreInPlaceSucceeded {
				t.Fatalf("phase = %s, want Succeeded (reclones are asynchronous)", fresh.Status.RestoreInPlace.Phase)
			}
			raw := fresh.GetAnnotations()[RecloneAnnotation]
			if raw == "" {
				t.Fatal("no reclone request was written for the peer")
			}
			req := parseRecloneAnnotation(raw)
			if req.Site != "dc2" {
				t.Fatalf("reclone request names %q, want dc2", req.Site)
			}
			if err := validateRecloneRequest(fresh, req); err != nil {
				t.Fatalf("the reclone consumer rejects the restore's request %q: %v", raw, err)
			}
			if strings.ContainsAny(raw, "\n,") {
				t.Errorf("annotation value %q should carry a single-line GTID prefix", raw)
			}

			tm, _, _ := newTestTopologyManagerWithBootstrap(&mockMySQL{}, &mockMySQL{readOnly: true})
			rec := record.NewFakeRecorder(10)
			runner := &TopologyManagerRunner{client: c, recorder: rec, logger: testLogger()}
			runner.handleRecloneAnnotation(context.Background(), fresh, types.NamespacedName{Name: fg.Name, Namespace: fg.Namespace}, tm)
			for _, e := range drainEvents(rec) {
				if strings.Contains(e, "RecloneRejected") {
					t.Errorf("consumer rejected the request: %s", e)
				}
			}
			tm.mu.RLock()
			pending, owner := tm.reclonePendingSite, tm.reclonePendingOwner
			tm.mu.RUnlock()
			if pending != "dc2" || owner != testRestoreConfirm {
				t.Fatalf("topology manager pending reclone = %q owner %q, want dc2 owned by %s", pending, owner, testRestoreConfirm)
			}
		})
	}
}

func TestRestoreRecloneRecipients_PromotableFirst(t *testing.T) {
	fg := fgWithBackup()
	fg.Spec.Sites = []v1alpha1.SiteSpec{
		{Name: "a"},
		{Name: "reader", Role: v1alpha1.SiteRoleReadOnly},
		{Name: "dr", Role: v1alpha1.SiteRoleDROnly},
		{Name: "b"},
		{Name: "c"},
	}
	fg.Spec.SplitBrainPolicy = &v1alpha1.SplitBrainPolicySpec{SitePriorities: []string{"c", "a"}}
	got := restoreRecloneRecipients(fg, "a")
	want := []string{"c", "b", "dr", "reader"}
	if !slices.Equal(got, want) {
		t.Fatalf("recipients = %v, want %v", got, want)
	}
}

// The recipient set is fixed when the restore is accepted, before the
// fence or any destructive work. A per-schema restore replicates through
// the binlog and queues nothing.
func TestRestoreReclone_ArmedWhenRestoreIsAccepted(t *testing.T) {
	for _, tc := range []struct {
		name   string
		schema string
		want   []string
	}{
		{name: "full-instance", want: []string{"pdx", "sea"}},
		{name: "per-schema", schema: "orders"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			fg := fgThreeSiteRestore()
			if tc.schema != "" {
				fg.Spec.RestoreInPlace.LoadOptions = &v1alpha1.LoadOptions{IncludeSchemas: []string{tc.schema}}
			}
			fg.Status.RestoreInPlace = &v1alpha1.RestoreInPlaceStatus{
				Phase: v1alpha1.RestoreInPlacePreflight, Scope: "full", StartTime: ptrTime(time.Now()),
			}
			r, c := newReconciler(fg, succeededSeedBackup(), dsnSecret())
			if _, err := r.reconcileInPlaceRestore(context.Background(), fg); err != nil {
				t.Fatal(err)
			}
			fresh := getFG(t, c, fg)
			if fresh.Status.RestoreInPlace.Phase != v1alpha1.RestoreInPlaceFencing {
				t.Fatalf("phase = %s, want Fencing", fresh.Status.RestoreInPlace.Phase)
			}
			q := fresh.Status.RestoreReclones
			if tc.want == nil {
				if q != nil {
					t.Fatalf("per-schema restore must not queue reclones, got %+v", q)
				}
				return
			}
			if q == nil || q.Phase != v1alpha1.RestoreReclonesArmed || q.ConfirmToken != fg.Spec.RestoreInPlace.Confirm || !slices.Equal(q.Pending, tc.want) {
				t.Fatalf("queue = %+v, want Armed %v for confirm %s", q, tc.want, fg.Spec.RestoreInPlace.Confirm)
			}
			if fresh.GetAnnotations()[RecloneAnnotation] != "" {
				t.Error("nothing may be recloned before the restore succeeds")
			}
		})
	}
}

// Every peer is recloned, one at a time, and leaves the queue only when
// its clone succeeded.
func TestRestoreReclone_DrainsEveryPeerOneAtATime(t *testing.T) {
	fg := fgDrainingRestore("pdx", "sea")
	r, c := newReconciler(fg, dsnSecret())
	rec := r.Recorder.(*record.FakeRecorder)

	for _, site := range []string{"pdx", "sea"} {
		if d := reconcileRestoreStep(t, r, fg); d <= 0 {
			t.Fatalf("%s: want a requeue while the reclone runs, got %s", site, d)
		}
		fresh := getFG(t, c, fg)
		if got := fresh.GetAnnotations()[RecloneAnnotation]; got != site+":confirm=lion" {
			t.Fatalf("reclone annotation = %q, want %s:confirm=lion", got, site)
		}
		if fresh.Status.RestoreReclones.Requested != site {
			t.Fatalf("requested = %q, want %s", fresh.Status.RestoreReclones.Requested, site)
		}
		// A second pass while the clone runs changes nothing.
		reconcileRestoreStep(t, r, fg)
		if got := getFG(t, c, fg).GetAnnotations()[RecloneAnnotation]; got != site+":confirm=lion" {
			t.Fatalf("annotation changed while the reclone of %s runs: %q", site, got)
		}
		finishReclone(t, c, rec, fg, site, RecloneOutcome{Owner: testRestoreConfirm})
	}
	if q := getFG(t, c, fg).Status.RestoreReclones; q != nil {
		t.Fatalf("queue should be cleared after every peer was recloned, got %+v", q)
	}
	events := strings.Join(drainEvents(rec), "\n")
	for _, want := range []string{"RestoreRecloneRequested", "RestoreRecloneSucceeded", "RestoreReclonesComplete"} {
		if !strings.Contains(events, want) {
			t.Errorf("missing %s event in:\n%s", want, events)
		}
	}
}

// The reclone consumer drops the annotation even when the clone fails.
// The site stays queued, but one peer that cannot be cloned must not hold
// up the others: the next site is requested at once and the failed one is
// retried after its backoff.
func TestRestoreReclone_FailedCloneDoesNotBlockOtherPeers(t *testing.T) {
	fg := fgDrainingRestore("pdx", "sea")
	r, c := newReconciler(fg, dsnSecret())
	rec := r.Recorder.(*record.FakeRecorder)

	reconcileRestoreStep(t, r, fg)
	finishReclone(t, c, rec, fg, "pdx", RecloneOutcome{Owner: testRestoreConfirm, Err: errors.New("clone timed out")})

	q := fg.Status.RestoreReclones
	if q == nil || !slices.Equal(q.Pending, []string{"pdx", "sea"}) || q.Requested != "" ||
		len(q.Failures) != 1 || q.Failures[0].Site != "pdx" || !strings.Contains(q.Failures[0].Message, "clone timed out") {
		t.Fatalf("failed reclone must stay queued with its failure recorded, got %+v", q)
	}
	if !strings.Contains(strings.Join(drainEvents(rec), "\n"), "RestoreRecloneFailed") {
		t.Error("want a RestoreRecloneFailed event")
	}

	reconcileRestoreStep(t, r, fg)
	if got := getFG(t, c, fg).GetAnnotations()[RecloneAnnotation]; got != "sea:confirm=lion" {
		t.Fatalf("the next queued site must be requested while pdx backs off, got annotation %q", got)
	}
	finishReclone(t, c, rec, fg, "sea", RecloneOutcome{Owner: testRestoreConfirm})

	if d := reconcileRestoreStep(t, r, fg); d <= 0 || d > restoreRecloneRetryBackoff {
		t.Errorf("want a backoff requeue while only pdx is left, got %s", d)
	}
	if got := getFG(t, c, fg).GetAnnotations()[RecloneAnnotation]; got != "" {
		t.Fatalf("retried pdx inside its backoff: annotation %q", got)
	}

	fg.Status.RestoreReclones.Failures[0].Time = metav1.NewTime(time.Now().Add(-2 * restoreRecloneRetryBackoff))
	if err := c.Status().Update(context.Background(), fg); err != nil {
		t.Fatal(err)
	}
	reconcileRestoreStep(t, r, fg)
	if got := getFG(t, c, fg).GetAnnotations()[RecloneAnnotation]; got != "pdx:confirm=lion" {
		t.Fatalf("after the backoff the failed site is requested again, got annotation %q", got)
	}
	finishReclone(t, c, rec, fg, "pdx", RecloneOutcome{Owner: testRestoreConfirm})
	if q := getFG(t, c, fg).Status.RestoreReclones; q != nil {
		t.Fatalf("queue = %+v, want cleared", q)
	}
}

// A request whose clone never starts (unreachable recipient, keyring
// unseal never ready, ...) gives up its slot after the start timeout.
func TestRestoreReclone_StalledRequestMovesOn(t *testing.T) {
	fg := fgDrainingRestore("pdx", "sea")
	r, c := newReconciler(fg, dsnSecret())
	tm, _, _ := newTestTopologyManagerWithBootstrap(&mockMySQL{}, &mockMySQL{readOnly: true})
	nn := types.NamespacedName{Name: fg.Name, Namespace: fg.Namespace}
	r.Runner = &TopologyManagerRunner{client: c, recorder: r.Recorder, logger: testLogger(),
		managers: map[types.NamespacedName]*managedTopology{nn: {tm: tm}}}

	reconcileRestoreStep(t, r, fg)
	if got := getFG(t, c, fg).GetAnnotations()[RecloneAnnotation]; got != "pdx:confirm=lion" {
		t.Fatalf("annotation = %q, want pdx requested", got)
	}
	reconcileRestoreStep(t, r, fg)
	if getFG(t, c, fg).Status.RestoreReclones.Requested != "pdx" {
		t.Fatal("a fresh request must not time out")
	}

	// An old requestedAt alone does not stall the request: this process
	// has not watched it sit unstarted yet (as after an operator restart,
	// while the new topology manager picks the annotation up again).
	old := metav1.NewTime(time.Now().Add(-2 * restoreRecloneStartTimeout))
	fg.Status.RestoreReclones.RequestedAt = &old
	if err := c.Status().Update(context.Background(), fg); err != nil {
		t.Fatal(err)
	}
	reconcileRestoreStep(t, r, fg)
	if q := getFG(t, c, fg).Status.RestoreReclones; q.Requested != "pdx" || len(q.Failures) != 0 {
		t.Fatalf("queue = %+v, want pdx still requested on the first observation after a restart", q)
	}

	ageRestoreRecloneIdle(r, nn, 2*restoreRecloneStartTimeout)
	reconcileRestoreStep(t, r, fg)
	q := getFG(t, c, fg).Status.RestoreReclones
	if q.Requested != "" || len(q.Failures) != 1 || q.Failures[0].Site != "pdx" || !strings.Contains(q.Failures[0].Message, "did not start") {
		t.Fatalf("queue = %+v, want the stalled pdx request failed", q)
	}
	reconcileRestoreStep(t, r, fg)
	if got := getFG(t, c, fg).GetAnnotations()[RecloneAnnotation]; got != "sea:confirm=lion" {
		t.Fatalf("annotation = %q, want sea requested after pdx stalled", got)
	}
}

// ageRestoreRecloneIdle backdates when r first saw nn's request unstarted.
func ageRestoreRecloneIdle(r *MysqlFailoverGroupReconciler, nn types.NamespacedName, by time.Duration) {
	r.restoreRecloneMu.Lock()
	defer r.restoreRecloneMu.Unlock()
	idle := r.restoreRecloneIdle[nn]
	idle.since = idle.since.Add(-by)
	r.restoreRecloneIdle[nn] = idle
}

// A clone that succeeded but whose completion callback failed is retried
// by the topology manager (recloneCompletedSite). The start timeout must
// not fail it meanwhile: that would drop the success and clone the site
// again.
func TestRestoreReclone_CompletedCloneAwaitingCallbackIsNotStalled(t *testing.T) {
	fg := fgDrainingRestore("pdx", "sea")
	r, c := newReconciler(fg, dsnSecret())
	tm, _, _ := newTestTopologyManagerWithBootstrap(&mockMySQL{}, &mockMySQL{readOnly: true})
	nn := types.NamespacedName{Name: fg.Name, Namespace: fg.Namespace}
	r.Runner = &TopologyManagerRunner{client: c, recorder: r.Recorder, logger: testLogger(),
		managers: map[types.NamespacedName]*managedTopology{nn: {tm: tm}}}

	reconcileRestoreStep(t, r, fg)
	old := metav1.NewTime(time.Now().Add(-2 * restoreRecloneStartTimeout))
	fg.Status.RestoreReclones.RequestedAt = &old
	if err := c.Status().Update(context.Background(), fg); err != nil {
		t.Fatal(err)
	}
	tm.mu.Lock()
	tm.recloneCompletedSite, tm.recloneCompletedOwner = "pdx", testRestoreConfirm
	tm.mu.Unlock()
	for i := 0; i < 2; i++ {
		reconcileRestoreStep(t, r, fg)
		r.restoreRecloneMu.Lock()
		for k, v := range r.restoreRecloneIdle {
			v.since = v.since.Add(-2 * restoreRecloneStartTimeout)
			r.restoreRecloneIdle[k] = v
		}
		r.restoreRecloneMu.Unlock()
	}
	if q := getFG(t, c, fg).Status.RestoreReclones; q.Requested != "pdx" || len(q.Failures) != 0 {
		t.Fatalf("queue = %+v, want the completed pdx clone left to its callback retry", q)
	}
}

// A clone the queue did not start (an administrator's reclone of the same
// site that was already running) does not complete the site: it may have
// started before the restore.
func TestRestoreReclone_ForeignCloneDoesNotCompleteSite(t *testing.T) {
	fg := fgDrainingRestore("pdx", "sea")
	r, c := newReconciler(fg, dsnSecret())

	reconcileRestoreStep(t, r, fg)
	finishReclone(t, c, r.Recorder, fg, "pdx", RecloneOutcome{})

	q := fg.Status.RestoreReclones
	if q == nil || !slices.Equal(q.Pending, []string{"pdx", "sea"}) || q.Requested != "" || len(q.Failures) != 0 {
		t.Fatalf("a foreign clone must leave pdx queued without a failure, got %+v", q)
	}
	reconcileRestoreStep(t, r, fg)
	if got := getFG(t, c, fg).GetAnnotations()[RecloneAnnotation]; got != "pdx:confirm=lion" {
		t.Fatalf("pdx should be requested again at once, annotation %q", got)
	}
}

func TestRestoreReclone_NeverOverwritesAnotherRequest(t *testing.T) {
	fg := fgDrainingRestore("pdx", "sea")
	fg.Annotations = map[string]string{RecloneAnnotation: "sea:confirm=lion"}
	r, c := newReconciler(fg, dsnSecret())

	if d := reconcileRestoreStep(t, r, fg); d <= 0 {
		t.Errorf("want a requeue while another request is pending, got %s", d)
	}
	fresh := getFG(t, c, fg)
	if got := fresh.GetAnnotations()[RecloneAnnotation]; got != "sea:confirm=lion" {
		t.Fatalf("the administrator's request was overwritten: %q", got)
	}
	if fresh.Status.RestoreReclones.Requested != "" {
		t.Errorf("requested = %q while another request owns the annotation", fresh.Status.RestoreReclones.Requested)
	}
}

func TestRestoreReclone_LostRequestIsRetried(t *testing.T) {
	fg := fgDrainingRestore("pdx", "sea")
	fg.Status.RestoreReclones.Requested = "pdx"
	r, c := newReconciler(fg, dsnSecret())

	reconcileRestoreStep(t, r, fg)
	q := getFG(t, c, fg).Status.RestoreReclones
	if q.Requested != "" || len(q.Failures) != 1 || q.Failures[0].Site != "pdx" || !strings.Contains(q.Failures[0].Message, "removed before the clone finished") {
		t.Fatalf("a request whose annotation vanished must be recorded and retried, got %+v", q)
	}
	if !slices.Equal(q.Pending, []string{"pdx", "sea"}) {
		t.Fatalf("pending = %v", q.Pending)
	}
}

// Cloning from a primary whose restore failed would destroy the peers'
// copies, which are then the only intact data.
func TestRestoreReclone_DiscardedWhenRestoreFails(t *testing.T) {
	for _, tc := range []struct {
		name string
		rip  *v1alpha1.RestoreInPlaceStatus
	}{
		{name: "restore failed", rip: &v1alpha1.RestoreInPlaceStatus{Phase: v1alpha1.RestoreInPlaceFailed, ConfirmTokenUsed: testRestoreConfirm}},
		{name: "restore status gone"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			fg := fgThreeSiteRestore()
			fg.Status.RestoreInPlace = tc.rip
			fg.Status.RestoreReclones = &v1alpha1.RestoreReclonesStatus{
				ConfirmToken: testRestoreConfirm, Phase: v1alpha1.RestoreReclonesArmed, Pending: []string{"pdx", "sea"},
			}
			r, c := newReconciler(fg, dsnSecret())
			if _, err := r.reconcileRestoreReclones(context.Background(), fg); err != nil {
				t.Fatal(err)
			}
			fresh := getFG(t, c, fg)
			if fresh.Status.RestoreReclones != nil || fresh.GetAnnotations()[RecloneAnnotation] != "" {
				t.Fatalf("queue must be discarded without recloning, got %+v annotations %v", fresh.Status.RestoreReclones, fresh.GetAnnotations())
			}
			if !strings.Contains(strings.Join(drainEvents(r.Recorder.(*record.FakeRecorder)), "\n"), "RestoreReclonesDiscarded") {
				t.Error("want a RestoreReclonesDiscarded event")
			}
		})
	}
}

func TestRestoreReclone_ArmedQueueKeptWhileRestoreRuns(t *testing.T) {
	fg := fgThreeSiteRestore()
	fg.Status.RestoreInPlace = &v1alpha1.RestoreInPlaceStatus{Phase: v1alpha1.RestoreInPlaceRestoring}
	fg.Status.RestoreReclones = &v1alpha1.RestoreReclonesStatus{
		ConfirmToken: testRestoreConfirm, Phase: v1alpha1.RestoreReclonesArmed, Pending: []string{"pdx", "sea"},
	}
	r, c := newReconciler(fg, dsnSecret())
	if _, err := r.reconcileRestoreReclones(context.Background(), fg); err != nil {
		t.Fatal(err)
	}
	if q := getFG(t, c, fg).Status.RestoreReclones; q == nil || q.Phase != v1alpha1.RestoreReclonesArmed {
		t.Fatalf("queue changed while its restore runs: %+v", q)
	}
}

// When a queued site becomes the primary some other way, cloning the
// rest from it would spread its pre-restore data.
func TestRestoreReclone_AbandonedWhenQueuedSiteBecomesPrimary(t *testing.T) {
	fg := fgDrainingRestore("pdx", "sea")
	fg.Status.ActiveSite = "pdx"
	r, c := newReconciler(fg, dsnSecret())
	reconcileRestoreStep(t, r, fg)
	fresh := getFG(t, c, fg)
	if fresh.Status.RestoreReclones != nil || fresh.GetAnnotations()[RecloneAnnotation] != "" {
		t.Fatalf("queue must be abandoned, got %+v", fresh.Status.RestoreReclones)
	}
	if !strings.Contains(strings.Join(drainEvents(r.Recorder.(*record.FakeRecorder)), "\n"), "RestoreReclonesAbandoned") {
		t.Error("want a RestoreReclonesAbandoned event")
	}
}

// A planned failover to a queued site can still fail (lag timeout, fence
// or drain failure) and roll back. The queue must survive that: the
// restored primary stays the primary and the peers still need recloning.
func TestPlannedFailover_RollbackKeepsRecloneQueue(t *testing.T) {
	fg := plannedFailoverFG("")
	fg.Status.PlannedFailover = &v1alpha1.PlannedFailoverStatus{
		Phase: v1alpha1.PlannedFailoverPhaseValidating, Target: "pdx", SourcePrimary: "iad",
	}
	fg.Status.RestoreReclones = drainingQueue("pdx")
	r, c := newReconciler(fg)
	r.Runner = &TopologyManagerRunner{}

	if _, err := r.plannedFailoverValidating(context.Background(), fg, fgNN(fg)); err != nil {
		t.Fatal(err)
	}
	if q := getFG(t, c, fg).Status.RestoreReclones; q == nil {
		t.Fatal("validating a planned failover to a queued site abandoned the queue before any promotion")
	}
	*fg = *getFG(t, c, fg)
	if _, err := r.plannedFailoverRollback(context.Background(), fg, fgNN(fg), "LagTimeout", "timeout", "failed_timeout"); err != nil {
		t.Fatal(err)
	}
	if q := getFG(t, c, fg).Status.RestoreReclones; q == nil || !slices.Equal(q.Pending, []string{"pdx"}) {
		t.Fatalf("queue after a rolled-back planned failover = %+v, want pdx still queued", q)
	}
}

// No reclone is requested while a planned failover is in flight: it may be
// making a stale queued site the primary, which a new clone would copy.
func TestRestoreReclone_NoRequestDuringPlannedFailover(t *testing.T) {
	fg := fgDrainingRestore("pdx", "sea")
	fg.Status.PlannedFailover = &v1alpha1.PlannedFailoverStatus{Phase: v1alpha1.PlannedFailoverPhaseDraining, Target: "pdx", SourcePrimary: "iad"}
	r, c := newReconciler(fg, dsnSecret())
	if d := reconcileRestoreStep(t, r, fg); d <= 0 {
		t.Errorf("want a requeue, got %s", d)
	}
	if got := getFG(t, c, fg).GetAnnotations()[RecloneAnnotation]; got != "" {
		t.Fatalf("a reclone was requested during a planned failover: %q", got)
	}
}

// Restore A succeeded and its queue still drains when restore B is
// accepted. If B is cancelled or fails before it changes anything, A's
// peers must stay queued: they still hold data from before A.
func TestRestoreReclone_FailedSecondRestoreKeepsEarlierQueue(t *testing.T) {
	const confirmB = "2026-04-18T10:00:00Z"
	fg := fgDrainingRestore("pdx", "sea")
	r, c := newReconciler(fg, dsnSecret())

	if err := r.armRestoreReclones(context.Background(), fg, "iad", confirmB); err != nil {
		t.Fatal(err)
	}
	q := getFG(t, c, fg).Status.RestoreReclones
	if q.Phase != v1alpha1.RestoreReclonesArmed || q.ConfirmToken != confirmB || q.Superseded == nil ||
		q.Superseded.ConfirmToken != testRestoreConfirm || !slices.Equal(q.Superseded.Pending, []string{"pdx", "sea"}) {
		t.Fatalf("queue after arming restore B = %+v, want B armed with A's queue superseded", q)
	}

	// B cancelled before its Job (spec removed): Failed.
	fg.Status.RestoreInPlace = &v1alpha1.RestoreInPlaceStatus{Phase: v1alpha1.RestoreInPlaceFailed, Message: "cancelled"}
	if err := c.Status().Update(context.Background(), fg); err != nil {
		t.Fatal(err)
	}
	if _, err := r.reconcileRestoreReclones(context.Background(), fg); err != nil {
		t.Fatal(err)
	}
	q = getFG(t, c, fg).Status.RestoreReclones
	if q == nil || q.Phase != v1alpha1.RestoreReclonesDraining || q.ConfirmToken != testRestoreConfirm || !slices.Equal(q.Pending, []string{"pdx", "sea"}) {
		t.Fatalf("queue after restore B failed = %+v, want A's queue draining again", q)
	}
}

// Restore B created its Job on the primary (it may have dropped schemas
// or half-loaded them) and then failed. Restore A's peers may be the only
// intact copies: A's queue is put back Held, never drained from that
// primary, and its sites stay excluded until an administrator decides.
func TestRestoreReclone_ExecutedFailedSecondRestoreHoldsEarlierQueue(t *testing.T) {
	const confirmB = "2026-04-18T10:00:00Z"
	for _, tc := range []struct {
		name string
		rip  *v1alpha1.RestoreInPlaceStatus
	}{
		{name: "job failed", rip: &v1alpha1.RestoreInPlaceStatus{Phase: v1alpha1.RestoreInPlaceFailed, JobName: "mysql-lion-iad-inplace-restore", ConfirmTokenUsed: confirmB}},
		{name: "job lost", rip: &v1alpha1.RestoreInPlaceStatus{Phase: v1alpha1.RestoreInPlaceFailed, JobName: "mysql-lion-iad-inplace-restore", Message: "outcome is unknown"}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			fg := fgDrainingRestore("pdx", "sea")
			r, c := newReconciler(fg, dsnSecret())
			rec := r.Recorder.(*record.FakeRecorder)
			if err := r.armRestoreReclones(context.Background(), fg, "iad", confirmB); err != nil {
				t.Fatal(err)
			}
			fg.Status.RestoreInPlace = tc.rip
			if err := c.Status().Update(context.Background(), fg); err != nil {
				t.Fatal(err)
			}
			for i := 0; i < 3; i++ {
				*fg = *getFG(t, c, fg)
				if _, err := r.reconcileRestoreReclones(context.Background(), fg); err != nil {
					t.Fatal(err)
				}
			}
			fresh := getFG(t, c, fg)
			if got := fresh.GetAnnotations()[RecloneAnnotation]; got != "" {
				t.Fatalf("a peer was recloned from the primary of a failed restore: annotation %q", got)
			}
			q := fresh.Status.RestoreReclones
			if q == nil || q.Phase != v1alpha1.RestoreReclonesHeld || q.ConfirmToken != testRestoreConfirm || !slices.Equal(q.Pending, []string{"pdx", "sea"}) {
				t.Fatalf("queue = %+v, want A's queue held with pdx and sea pending", q)
			}
			if !slices.Equal(restoreReclonePendingSites(fresh), []string{"pdx", "sea"}) {
				t.Fatal("held sites must stay excluded from promotion and the reader Services")
			}
			if events := strings.Join(drainEvents(rec), "\n"); !strings.Contains(events, "RestoreReclonesHeld") {
				t.Fatalf("events %q, want RestoreReclonesHeld", events)
			}
		})
	}
}

// An administrator who has restored the primary resumes a held queue by
// setting its phase to Draining.
func TestRestoreReclone_HeldQueueResumesWhenSetToDraining(t *testing.T) {
	fg := fgDrainingRestore("pdx")
	fg.Status.RestoreReclones.Phase = v1alpha1.RestoreReclonesHeld
	r, c := newReconciler(fg, dsnSecret())
	reconcileRestoreStep(t, r, fg)
	if got := getFG(t, c, fg).GetAnnotations()[RecloneAnnotation]; got != "" {
		t.Fatalf("held queue requested %q", got)
	}
	*fg = *getFG(t, c, fg)
	fg.Status.RestoreReclones.Phase = v1alpha1.RestoreReclonesDraining
	if err := c.Status().Update(context.Background(), fg); err != nil {
		t.Fatal(err)
	}
	reconcileRestoreStep(t, r, fg)
	if got := getFG(t, c, fg).GetAnnotations()[RecloneAnnotation]; got != "pdx:confirm=lion" {
		t.Fatalf("annotation = %q, want pdx requested once the queue drains again", got)
	}
}

// A held queue stays held across a later restore C that ends before its
// Job, and is replaced for good when C succeeds.
func TestRestoreReclone_HeldQueueSurvivesAnotherFailedRestore(t *testing.T) {
	const confirmC = "2026-04-19T10:00:00Z"
	fg := fgDrainingRestore("pdx")
	fg.Status.RestoreReclones.Phase = v1alpha1.RestoreReclonesHeld
	r, c := newReconciler(fg, dsnSecret())
	if err := r.armRestoreReclones(context.Background(), fg, "iad", confirmC); err != nil {
		t.Fatal(err)
	}
	if sup := getFG(t, c, fg).Status.RestoreReclones.Superseded; sup == nil || !sup.Held {
		t.Fatalf("superseded = %+v, want the held queue carried as held", sup)
	}
	fg.Status.RestoreInPlace = &v1alpha1.RestoreInPlaceStatus{Phase: v1alpha1.RestoreInPlaceFailed, Message: "cancelled"}
	if err := c.Status().Update(context.Background(), fg); err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 2; i++ {
		*fg = *getFG(t, c, fg)
		if _, err := r.reconcileRestoreReclones(context.Background(), fg); err != nil {
			t.Fatal(err)
		}
	}
	fresh := getFG(t, c, fg)
	if q := fresh.Status.RestoreReclones; q == nil || q.Phase != v1alpha1.RestoreReclonesHeld || fresh.GetAnnotations()[RecloneAnnotation] != "" {
		t.Fatalf("queue = %+v annotation %q, want still held and nothing requested", q, fresh.GetAnnotations()[RecloneAnnotation])
	}
}

// A successful newer restore replaces the older queue for good.
func TestRestoreReclone_SuccessfulSecondRestoreDropsSuperseded(t *testing.T) {
	fg := fgDrainingRestore("pdx")
	r, c := newReconciler(fg, dsnSecret())
	if err := r.armRestoreReclones(context.Background(), fg, "iad", "2026-04-18T10:00:00Z"); err != nil {
		t.Fatal(err)
	}
	if err := r.releaseRestoreReclones(context.Background(), fg, "iad", "2026-04-18T10:00:00Z"); err != nil {
		t.Fatal(err)
	}
	q := getFG(t, c, fg).Status.RestoreReclones
	if q.Phase != v1alpha1.RestoreReclonesDraining || q.Superseded != nil || !slices.Equal(q.Pending, []string{"pdx", "sea"}) {
		t.Fatalf("queue = %+v, want restore B's queue draining with nothing superseded", q)
	}
}

// The primary re-assert must not make a queued site writable.
func TestPrimaryReassert_RefusesPendingRecloneTarget(t *testing.T) {
	site0 := &mockMySQL{readOnly: true, gtidExecuted: "aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa:1-10"}
	site1 := &mockMySQL{readOnly: true, gtidExecuted: "aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa:1-8"}
	tm, _, _ := newTestTopologyManager(site0, site1)
	setWedgedTopology(tm, "dc1")
	tm.mu.Lock()
	tm.promotionGtidExecuted = "aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa:1-9"
	tm.mu.Unlock()
	tm.SetRestoreReclonePending([]string{"dc1"})

	if tm.checkPrimaryReassert(context.Background()) {
		t.Fatal("re-assert restored a target that awaits its post-restore reclone")
	}
	if ro, _ := site0.CheckReadOnly(context.Background()); !ro {
		t.Fatal("the pending target was made writable")
	}
	tm.mu.RLock()
	refused := append([]string(nil), tm.restoreReclonePromotionRefused...)
	tm.mu.RUnlock()
	if !slices.Equal(refused, []string{"dc1"}) {
		t.Fatalf("refusal record = %v, want [dc1]", refused)
	}
}

// An ordered update of the active site needs a standby to hand off to; a
// queued (stale) standby does not count, so the update waits.
func TestCheckUpdate_PendingRecloneStandbyIsNotAHandoffTarget(t *testing.T) {
	primary := &mockMySQL{readOnly: false}
	standby := &mockMySQL{readOnly: true, replicaStatusVal: &mysql.ReplicaStatus{
		IORunning: true, SQLRunning: true, SourceHost: "mysql-primary-internal.ns.svc.cluster.local",
	}}
	tm := newConvergenceManager(t, []state.SiteRole{
		state.SiteRolePrimaryCandidate, state.SiteRolePrimaryCandidate,
	}, primary, standby)
	tm.sites[1].replicating = true
	tm.sites[1].sourceHost = "mysql-primary-internal.ns.svc.cluster.local"
	tm.updater = NewUpdateController(NewFailoverController(testLogger()), testLogger())
	tm.ApplyUpdate = func(_ context.Context, site string) error {
		t.Errorf("unexpected apply of %s", site)
		return nil
	}
	tm.SetSpecDriftSites([]string{"primary"})
	tm.SetRestoreReclonePending([]string{"follower-a"})
	if tm.checkUpdate(context.Background()) {
		t.Fatal("an active-site update must not start when the only standby awaits its post-restore reclone")
	}
}

// A queued peer still serves pre-restore data, so it is taken out of the
// reader Services until it has been recloned.
func TestSyncPodLabels_QueuedRecloneSiteServesNoReads(t *testing.T) {
	lag0 := int64(0)
	for _, tc := range []struct {
		name        string
		queued      bool
		wantHealthy string
	}{
		{name: "queued", queued: true, wantHealthy: "no"},
		{name: "recloned", wantHealthy: "yes"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			fg := newTestFG()
			fg.Status.ActiveSite = "dc1"
			fg.Status.Sites = []v1alpha1.SiteStatus{
				{Name: "dc1", State: "writable"},
				{Name: "dc2", State: "read-only", Replicating: true, SecondsBehindSource: &lag0},
			}
			if tc.queued {
				fg.Status.RestoreReclones = &v1alpha1.RestoreReclonesStatus{
					ConfirmToken: testRestoreConfirm, Phase: v1alpha1.RestoreReclonesDraining, Pending: []string{"dc2"},
				}
			}
			pod := &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: "dc2-pod", Namespace: fg.Namespace, Labels: map[string]string{
				labelAppName: "mysql", labelInstance: fg.Name, labelSite: "dc2", labelRole: "replica", labelHealthy: "yes",
			}}}
			r, c := newReconciler(fg, pod)
			if err := r.syncPodLabels(context.Background(), fg); err != nil {
				t.Fatal(err)
			}
			var got corev1.Pod
			if err := c.Get(context.Background(), types.NamespacedName{Name: pod.Name, Namespace: fg.Namespace}, &got); err != nil {
				t.Fatal(err)
			}
			if got.Labels[labelHealthy] != tc.wantHealthy {
				t.Fatalf("healthy label = %q, want %q", got.Labels[labelHealthy], tc.wantHealthy)
			}
		})
	}
}

func TestRecloneAnnotationValue_PassesInterlock(t *testing.T) {
	for _, divergent := range []string{"", "3e11fa47-71ca-11e1-9e33-c80aa9429562:23-25", "3e11fa47-71ca-11e1-9e33-c80aa9429562:1-5,\n4f22ab58-71ca-11e1-9e33-c80aa9429562:7"} {
		fg := fgThreeSiteRestore()
		fg.Status.Sites[1].DivergentGtid = divergent
		value := recloneAnnotationValue(fg, "pdx")
		if err := validateRecloneRequest(fg, parseRecloneAnnotation(value)); err != nil {
			t.Errorf("divergent=%q: value %q rejected: %v", divergent, value, err)
		}
	}
}

// The owner of a reclone request travels with the clone to the
// completion callback, so the restore queue only completes clones it
// started.
func TestTopologyManager_RecloneOwnerReachesCompletionCallback(t *testing.T) {
	for _, tc := range []struct {
		name      string
		request   func(tm *TopologyManager)
		wantOwner string
	}{
		{name: "restore request", request: func(tm *TopologyManager) { tm.SetRecloneSiteFor("dc2", "c1") }, wantOwner: "c1"},
		{name: "administrator request", request: func(tm *TopologyManager) { tm.SetRecloneSite("dc2") }},
		{name: "restore adopts a queued administrator request", request: func(tm *TopologyManager) {
			tm.SetRecloneSite("dc2")
			tm.SetRecloneSiteFor("dc2", "c1")
		}, wantOwner: "c1"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			tm, _, _ := newTestTopologyManagerWithBootstrap(&mockMySQL{readOnly: false}, &mockMySQL{readOnly: true})
			tm.autoBootstrapSuppressed = true
			pollN(tm, 2)
			done := make(chan RecloneOutcome, 1)
			tm.RecloneCompleteCallback = func(_ context.Context, site string, outcome RecloneOutcome) error {
				if site == "dc2" {
					done <- outcome
				}
				return nil
			}
			tc.request(tm)
			if !tm.checkReclone(context.Background()) {
				t.Fatal("expected the reclone to start")
			}
			select {
			case outcome := <-done:
				if outcome.Owner != tc.wantOwner || outcome.Err != nil {
					t.Fatalf("outcome = %+v, want owner %q and no error", outcome, tc.wantOwner)
				}
			case <-time.After(10 * time.Second):
				t.Fatal("reclone did not complete")
			}
		})
	}
}

// A request repeated while the site's clone is already running must not
// claim that clone: it began before the request.
func TestTopologyManager_RunningCloneKeepsItsOwner(t *testing.T) {
	tm, _, _ := newTestTopologyManagerWithBootstrap(&mockMySQL{readOnly: false}, &mockMySQL{readOnly: true})
	tm.SetRecloneSite("dc2")
	tm.mu.Lock()
	tm.bootstrapPhase, tm.bootstrapRecipient = BootstrapPhaseCloning, "dc2"
	tm.mu.Unlock()
	tm.SetRecloneSiteFor("dc2", "c1")
	tm.mu.RLock()
	owner := tm.reclonePendingOwner
	tm.mu.RUnlock()
	if owner != "" {
		t.Fatalf("running administrator clone was re-owned by %q", owner)
	}
}

func TestRunner_EmitsPendingRecloneRefusalOnce(t *testing.T) {
	fg := fgThreeSiteRestore()
	nn := types.NamespacedName{Name: fg.Name, Namespace: fg.Namespace}
	rec := record.NewFakeRecorder(10)
	runner := &TopologyManagerRunner{recorder: rec, managers: map[types.NamespacedName]*managedTopology{nn: {}}}
	snap := TopologySnapshot{RestoreReclonePromotionRefused: []string{"pdx"}}

	runner.emitKeyringPromotionEvents(fg, snap)
	events := drainEvents(rec)
	if len(events) != 1 || !strings.Contains(events[0], "Warning PromotionRefusedPendingReclone") || !strings.Contains(events[0], "pdx") {
		t.Fatalf("events = %q, want one PromotionRefusedPendingReclone warning naming pdx", events)
	}
	runner.emitKeyringPromotionEvents(fg, snap)
	if again := drainEvents(rec); len(again) != 0 {
		t.Fatalf("an unchanged refusal must not re-emit, got %q", again)
	}
}

func TestRestoreReclone_SiteRemovedFromSpecLeavesQueue(t *testing.T) {
	fg := fgDrainingRestore("pdx", "sea")
	fg.Spec.Sites = fg.Spec.Sites[:2] // drop sea
	fg.Status.RestoreReclones.Pending = []string{"sea", "pdx"}
	r, c := newReconciler(fg, dsnSecret())
	reconcileRestoreStep(t, r, fg)
	fresh := getFG(t, c, fg)
	if q := fresh.Status.RestoreReclones; q == nil || !slices.Equal(q.Pending, []string{"pdx"}) {
		t.Fatalf("queue = %+v, want only pdx", q)
	}
	if got := fresh.GetAnnotations()[RecloneAnnotation]; got != "pdx:confirm=lion" {
		t.Fatalf("annotation = %q, want pdx requested", got)
	}
}

// Queue changes reach the running topology manager at once, so the
// promotion block starts at acceptance and lifts as each site is recloned.
func TestRestoreReclone_PendingSetPushedToTopologyManager(t *testing.T) {
	fg := fgInPlaceDC()
	fg.Status.RestoreInPlace = &v1alpha1.RestoreInPlaceStatus{
		Phase: v1alpha1.RestoreInPlacePreflight, Scope: "full", StartTime: ptrTime(time.Now()),
	}
	r, c := newReconciler(fg, succeededSeedBackup(), dsnSecret())
	tm, _, _ := newTestTopologyManagerWithBootstrap(&mockMySQL{}, &mockMySQL{readOnly: true})
	nn := types.NamespacedName{Name: fg.Name, Namespace: fg.Namespace}
	r.Runner = &TopologyManagerRunner{client: c, recorder: r.Recorder, logger: testLogger(),
		managers: map[types.NamespacedName]*managedTopology{nn: {tm: tm}}}
	pending := func() []string {
		tm.mu.RLock()
		defer tm.mu.RUnlock()
		return append([]string(nil), tm.restoreReclonePending...)
	}

	if _, err := r.reconcileInPlaceRestore(context.Background(), fg); err != nil {
		t.Fatal(err)
	}
	if got := pending(); !slices.Equal(got, []string{"dc2"}) {
		t.Fatalf("after acceptance the topology manager blocks %v, want [dc2]", got)
	}

	fg.Status.RestoreReclones.Phase = v1alpha1.RestoreReclonesDraining
	fg.Status.RestoreReclones.Requested = "dc2"
	fg.Status.RestoreInPlace.Phase = v1alpha1.RestoreInPlaceSucceeded
	if err := c.Status().Update(context.Background(), fg); err != nil {
		t.Fatal(err)
	}
	if err := r.Runner.recordRestoreRecloneOutcome(context.Background(), nn, "dc2", RecloneOutcome{Owner: fg.Spec.RestoreInPlace.Confirm}); err != nil {
		t.Fatal(err)
	}
	if got := pending(); len(got) != 0 {
		t.Fatalf("after the reclone the topology manager still blocks %v", got)
	}
}

func TestRunner_PendingRecloneRefusalNamesWorkingRemedy(t *testing.T) {
	fg := fgThreeSiteRestore()
	nn := types.NamespacedName{Name: fg.Name, Namespace: fg.Namespace}
	rec := record.NewFakeRecorder(10)
	runner := &TopologyManagerRunner{recorder: rec, managers: map[types.NamespacedName]*managedTopology{nn: {}}}
	runner.emitKeyringPromotionEvents(fg, TopologySnapshot{RestoreReclonePromotionRefused: []string{"pdx"}})
	events := strings.Join(drainEvents(rec), "\n")
	if strings.Contains(events, "kubectl bloodraven promote") || !strings.Contains(events, "--subresource=status") {
		t.Fatalf("event %q must name bringing the primary back or removing status.restoreReclones", events)
	}
}

// reclonesTopologyManager is a topology manager over mock checkers named
// after fg's sites, with bootstrap configured so reclones can start, and
// a runner on r that manages it.
func reclonesTopologyManager(t *testing.T, r *MysqlFailoverGroupReconciler, c client.Client, fg *v1alpha1.MysqlFailoverGroup) (*TopologyManager, map[string]*mockMySQL) {
	t.Helper()
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
	tm := NewTopologyManager(cfg, checkers, NewFailoverController(testLogger()), nil, NewBootstrapController(testLogger()),
		BootstrapConfig{ReplUser: "repl", ReplPassword: "replpass", CloneTimeout: 10 * time.Second},
		newMockTainter(), platform.NewHub(testLogger()), &mockDNS{}, testLogger())
	nn := types.NamespacedName{Name: fg.Name, Namespace: fg.Namespace}
	r.Runner = &TopologyManagerRunner{client: c, recorder: r.Recorder, logger: testLogger(),
		managers: map[types.NamespacedName]*managedTopology{nn: {tm: tm}}}
	return tm, mocks
}

// stepInPlace runs one in-place restore reconcile and refreshes fg.
func stepInPlace(t *testing.T, r *MysqlFailoverGroupReconciler, c client.Client, fg *v1alpha1.MysqlFailoverGroup) {
	t.Helper()
	*fg = *getFG(t, c, fg)
	if _, err := r.reconcileInPlaceRestore(context.Background(), fg); err != nil {
		t.Fatal(err)
	}
	*fg = *getFG(t, c, fg)
}

// Restore A's queue has requested pdx (annotation written, topology
// manager holding the request) when restore B is accepted. The request is
// withdrawn while A's queue still owns it: otherwise it survives B and,
// if B fails, clones pdx from B's broken primary although A's queue is
// put back Held.
func TestRestoreReclone_NewRestoreWithdrawsOutstandingRequest(t *testing.T) {
	const confirmB = "2026-04-18T10:00:00Z"
	fg := fgDrainingRestore("pdx", "sea")
	r, c := newReconciler(fg, succeededSeedBackup(), dsnSecret())
	rec := r.Recorder.(*record.FakeRecorder)
	tm, _ := reclonesTopologyManager(t, r, c, fg)

	reconcileRestoreStep(t, r, fg)
	tm.mu.RLock()
	pending := tm.reclonePendingSite
	tm.mu.RUnlock()
	if got := getFG(t, c, fg).GetAnnotations()[RecloneAnnotation]; got != "pdx:confirm=lion" || pending != "pdx" {
		t.Fatalf("annotation %q, manager request %q: want restore A's pdx request outstanding", got, pending)
	}

	*fg = *getFG(t, c, fg)
	fg.Spec.RestoreInPlace.Confirm = confirmB
	if err := c.Update(context.Background(), fg); err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 3 && (fg.Status.RestoreInPlace.Phase != v1alpha1.RestoreInPlaceFencing); i++ {
		stepInPlace(t, r, c, fg)
	}
	if fg.Status.RestoreInPlace.Phase != v1alpha1.RestoreInPlaceFencing {
		t.Fatalf("status = %+v, want restore B in Fencing", fg.Status.RestoreInPlace)
	}
	tm.mu.RLock()
	pending = tm.reclonePendingSite
	tm.mu.RUnlock()
	if got := fg.GetAnnotations()[RecloneAnnotation]; got != "" || pending != "" {
		t.Fatalf("after restore B was accepted: annotation %q, manager request %q; want restore A's request withdrawn", got, pending)
	}
	q := fg.Status.RestoreReclones
	if q.Phase != v1alpha1.RestoreReclonesArmed || q.ConfirmToken != confirmB || q.Requested != "" ||
		q.Superseded == nil || !slices.Equal(q.Superseded.Pending, []string{"pdx", "sea"}) {
		t.Fatalf("queue = %+v, want B armed with A's whole queue superseded", q)
	}
	if !strings.Contains(strings.Join(drainEvents(rec), "\n"), "RestoreRecloneWithdrawn") {
		t.Error("want a RestoreRecloneWithdrawn event")
	}
}

// A clone that is already running must finish before a newer full
// restore drops and reloads its donor.
func TestRestoreReclone_NewRestoreWaitsForRunningClone(t *testing.T) {
	const confirmB = "2026-04-18T10:00:00Z"
	fg := fgDrainingRestore("pdx", "sea")
	fg.Spec.RestoreInPlace.Confirm = confirmB
	r, c := newReconciler(fg, succeededSeedBackup(), dsnSecret())
	tm, _ := reclonesTopologyManager(t, r, c, fg)
	tm.mu.Lock()
	tm.bootstrapPhase, tm.bootstrapRecipient, tm.bootstrapSource = BootstrapPhaseCloning, "pdx", "reclone"
	tm.mu.Unlock()

	for i := 0; i < 3; i++ {
		stepInPlace(t, r, c, fg)
	}
	if st := fg.Status.RestoreInPlace; st.Phase != v1alpha1.RestoreInPlacePreflight || !strings.Contains(st.Message, "running clone of pdx") {
		t.Fatalf("status = %+v, want Preflight waiting for the running clone", st)
	}
	if q := fg.Status.RestoreReclones; q.ConfirmToken != testRestoreConfirm {
		t.Fatalf("queue = %+v, want A's queue untouched while the clone runs", q)
	}

	tm.mu.Lock()
	tm.bootstrapPhase = BootstrapPhaseDone
	tm.mu.Unlock()
	stepInPlace(t, r, c, fg)
	if st := fg.Status.RestoreInPlace; st.Phase != v1alpha1.RestoreInPlaceFencing {
		t.Fatalf("status = %+v, want Fencing once the clone finished", st)
	}
}

// A request for a held site, including one restore A wrote before it was
// superseded and that a restarted operator re-reads, is refused by the
// annotation consumer and by the topology manager: a Held queue clones
// nothing from the failed restore's primary.
func TestRestoreReclone_HeldQueueRefusesOutstandingRequest(t *testing.T) {
	fg := fgDrainingRestore("pdx")
	fg.Status.RestoreReclones.Phase = v1alpha1.RestoreReclonesHeld
	fg.Annotations = map[string]string{RecloneAnnotation: "pdx:confirm=lion"}
	r, c := newReconciler(fg, dsnSecret())
	rec := r.Recorder.(*record.FakeRecorder)
	tm, mocks := reclonesTopologyManager(t, r, c, fg)
	tm.SetRestoreRecloneBlocked(restoreRecloneBlockedSites(fg)) // as the runner does at start
	nn := types.NamespacedName{Name: fg.Name, Namespace: fg.Namespace}

	r.Runner.handleRecloneAnnotation(context.Background(), fg, nn, tm)
	tm.mu.RLock()
	pending := tm.reclonePendingSite
	tm.mu.RUnlock()
	if pending != "" {
		t.Fatalf("the topology manager accepted a reclone of held site %s", pending)
	}
	if got := getFG(t, c, fg).GetAnnotations()[RecloneAnnotation]; got != "" {
		t.Fatalf("annotation %q kept; want the refused request removed", got)
	}
	if !strings.Contains(strings.Join(drainEvents(rec), "\n"), "RecloneRejected") {
		t.Error("want a RecloneRejected event")
	}

	// An in-memory request consumed before the queue was held.
	tm.mu.Lock()
	tm.reclonePendingSite = "pdx"
	tm.mu.Unlock()
	if tm.checkReclone(context.Background()) {
		t.Fatal("checkReclone started a clone of a held site")
	}
	tm.mu.RLock()
	pending = tm.reclonePendingSite
	tm.mu.RUnlock()
	if pending != "" || mocks["pdx"].cloneCalls != 0 {
		t.Fatalf("manager request %q, clone calls %d: want the held request dropped and nothing cloned", pending, mocks["pdx"].cloneCalls)
	}
}

// pollAfterConcurrentWrite runs one topology status write while mutate,
// standing in for a reconciler write, lands between the poller's read and
// its write.
func pollAfterConcurrentWrite(t *testing.T, fg *v1alpha1.MysqlFailoverGroup, mutate func(*v1alpha1.MysqlFailoverGroup)) *v1alpha1.MysqlFailoverGroup {
	t.Helper()
	nn := types.NamespacedName{Name: fg.Name, Namespace: fg.Namespace}
	fired := false
	c := fake.NewClientBuilder().WithScheme(testScheme()).
		WithStatusSubresource(&v1alpha1.MysqlFailoverGroup{}).
		WithObjects(fg).
		WithInterceptorFuncs(interceptor.Funcs{
			SubResourceUpdate: func(ctx context.Context, cl client.Client, sub string, obj client.Object, opts ...client.SubResourceUpdateOption) error {
				if !fired {
					fired = true
					var latest v1alpha1.MysqlFailoverGroup
					if err := cl.Get(ctx, nn, &latest); err != nil {
						return err
					}
					mutate(&latest)
					if err := cl.Status().Update(ctx, &latest); err != nil {
						return err
					}
				}
				return cl.Status().Update(ctx, obj, opts...)
			},
		}).Build()
	runner := &TopologyManagerRunner{client: c, logger: testLogger(), managers: map[types.NamespacedName]*managedTopology{}}
	if err := runner.updateCRStatus(context.Background(), nn, TopologySnapshot{
		Sites:      []SiteSnapshot{{Name: "dc1", State: state.StateWritable}, {Name: "dc2", State: state.StateReadOnly}},
		ActiveSite: "dc1",
	}); err != nil {
		t.Fatalf("updateCRStatus: %v", err)
	}
	var after v1alpha1.MysqlFailoverGroup
	if err := c.Get(context.Background(), nn, &after); err != nil {
		t.Fatal(err)
	}
	return &after
}

// The topology poller must not write a stale status.restoreReclones back
// over the reconciler's: a queue released to Draining while the poll ran
// stays Draining.
func TestUpdateCRStatus_DoesNotClobberConcurrentRestoreReclones(t *testing.T) {
	fg := encTestFG()
	fg.Status.RestoreReclones = &v1alpha1.RestoreReclonesStatus{ConfirmToken: "B", Phase: v1alpha1.RestoreReclonesArmed, Pending: []string{"dc2"}}
	after := pollAfterConcurrentWrite(t, fg, func(l *v1alpha1.MysqlFailoverGroup) {
		l.Status.RestoreReclones.Phase = v1alpha1.RestoreReclonesDraining
	})
	if q := after.Status.RestoreReclones; q == nil || q.Phase != v1alpha1.RestoreReclonesDraining {
		t.Fatalf("the poller wrote restoreReclones back to %+v", q)
	}
}

// A queue removed while the poll ran (the documented status patch that
// accepts the stale copies) must not come back.
func TestUpdateCRStatus_DoesNotResurrectClearedRestoreReclones(t *testing.T) {
	fg := encTestFG()
	fg.Status.RestoreReclones = &v1alpha1.RestoreReclonesStatus{ConfirmToken: "A", Phase: v1alpha1.RestoreReclonesHeld, Pending: []string{"dc2"}}
	after := pollAfterConcurrentWrite(t, fg, func(l *v1alpha1.MysqlFailoverGroup) {
		l.Status.RestoreReclones = nil
	})
	if q := after.Status.RestoreReclones; q != nil {
		t.Fatalf("the poller resurrected a removed queue: %+v", q)
	}
}

// An Armed queue whose own restore already Succeeded (a release lost to a
// crash, or an Armed copy written back over it) is released, not
// discarded: its peers still hold pre-restore data.
func TestRestoreReclone_ArmedQueueOfSucceededRestoreIsReleased(t *testing.T) {
	fg := fgDrainingRestore("pdx", "sea")
	fg.Status.RestoreReclones.Phase = v1alpha1.RestoreReclonesArmed
	r, c := newReconciler(fg, dsnSecret())
	if _, err := r.reconcileRestoreReclones(context.Background(), fg); err != nil {
		t.Fatal(err)
	}
	q := getFG(t, c, fg).Status.RestoreReclones
	if q == nil || q.Phase != v1alpha1.RestoreReclonesDraining || !slices.Equal(q.Pending, []string{"pdx", "sea"}) {
		t.Fatalf("queue = %+v, want the succeeded restore's queue released to Draining", q)
	}
}
