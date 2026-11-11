package controller

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/tools/record"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"

	v1alpha1 "github.com/shipstream/bloodraven/api/v1alpha1"
	"github.com/shipstream/bloodraven/internal/clock"
	"github.com/shipstream/bloodraven/internal/dragonfly"
)

// Regression tests for the Dragonfly findings of the ultra review of #235
// (K005, K007, K033, K049, K064, K065, K084, K086, K089, K121, K122, K136,
// K137).

// pfDFReconcilerWithFuncs is pfDFReconciler over a fake client with
// interceptors.
func pfDFReconcilerWithFuncs(t *testing.T, fg *v1alpha1.MysqlFailoverGroup, funcs interceptor.Funcs, objs ...client.Object) (*MysqlFailoverGroupReconciler, client.Client, *pfFakeTopology) {
	t.Helper()
	scheme := testScheme()
	c := fake.NewClientBuilder().WithScheme(scheme).WithStatusSubresource(&v1alpha1.MysqlFailoverGroup{}).
		WithObjects(append([]client.Object{fg, newTestSecret()}, objs...)...).WithInterceptorFuncs(funcs).Build()
	r := &MysqlFailoverGroupReconciler{Client: c, Scheme: scheme, Recorder: record.NewFakeRecorder(64)}
	topo := newPFFakeTopology()
	topo.setHeld("run-1", false)
	r.PlannedTopology = topo
	r.plannedClock = clock.NewFakeClock(time.Now())
	pfDFOwnIntent(r, fg)
	return r, c, topo
}

// pfDFIntended is a run in PromotingDragonfly with its takeover Intended at
// intent, the source offset 100 captured.
func pfDFIntended(policy string, intent time.Time) *v1alpha1.MysqlFailoverGroup {
	return pfDFTakeoverFixture(policy, intent)
}

func pfDFPodGone(t *testing.T, c client.Client, pod *corev1.Pod) bool {
	t.Helper()
	var got corev1.Pod
	err := c.Get(context.Background(), client.ObjectKeyFromObject(pod), &got)
	return apierrors.IsNotFound(err)
}

func pfDFPodLabels(t *testing.T, c client.Client, pod *corev1.Pod) map[string]string {
	t.Helper()
	var got corev1.Pod
	if err := c.Get(context.Background(), client.ObjectKeyFromObject(pod), &got); err != nil {
		t.Fatalf("get pod %s: %v", pod.Name, err)
	}
	return got.Labels
}

// K005: a target that restarted mid-run as an empty standalone master is not
// a completed takeover: this process got no REPLTAKEOVER success reply for
// it, so the takeover fails and the source keeps serving: it is not
// demoted, CLIENT KILLed or deleted. Both before and past the deadline.
func TestPlannedFailoverDragonfly_RestartedTargetIsNotATakeover(t *testing.T) {
	for _, tc := range []struct {
		name string
		late bool
	}{{"full", false}, {"finish-only past the deadline", true}} {
		t.Run(tc.name, func(t *testing.T) {
			intent := time.Now()
			if tc.late {
				probe := pfDFIntended("Proceed", intent)
				intent = intent.Add(-plannedDragonflyTakeoverDeadline(probe.Status.PlannedFailover) - 5*time.Second)
			}
			fg := pfDFIntended("Proceed", intent)
			sourcePod := makeDragonflyPod(fg.Name, "iad", "master", true)
			targetPod := makeDragonflyPod(fg.Name, "pdx", "replica", true)
			r, c, _ := pfDFReconciler(t, fg, sourcePod, targetPod)
			source := &fakeDragonflyConn{info: dragonfly.ReplicationInfo{Role: "master", MasterReplOffset: 500}}
			target := &pfTakeoverConn{fakeDragonflyConn: &fakeDragonflyConn{info: dragonfly.ReplicationInfo{Role: "master", MasterReplOffset: 0}}}
			installFakeDragonflyConn(t, r, map[string]DragonflyConnection{pfDFPdx: target, pfDFIad: source})

			pfDFTakeoverPasses(t, r, fgNN(fg))
			got := fetchFG(t, r, fgNN(fg))
			pf := got.Status.PlannedFailover
			wantMsg := "empty standalone master"
			if tc.late {
				wantMsg = "did not finish within"
			}
			if pf.Phase != v1alpha1.PlannedFailoverPhasePromoting || pf.Dragonfly.Takeover.State != v1alpha1.DragonflyTakeoverFailed ||
				!strings.Contains(pf.Dragonfly.Message, wantMsg) {
				t.Fatalf("block = %s %+v %q, want Promoting with the takeover Failed", pf.Phase, pf.Dragonfly.Takeover, pf.Dragonfly.Message)
			}
			if target.takeovers != 0 {
				t.Errorf("REPLTAKEOVER sent %d times to a target already reporting master", target.takeovers)
			}
			if len(source.clientKillTypes) != 0 || pfDFPodGone(t, c, sourcePod) {
				t.Errorf("source evicted (CLIENT KILL %v) or deleted after a takeover that never happened", source.clientKillTypes)
			}
			if l := pfDFPodLabels(t, c, sourcePod); l[v1alpha1.DragonflyRoleLabel] != "master" || l[v1alpha1.DragonflyTrafficLabel] != dragonflyTrafficEnabled {
				t.Errorf("source labels = %v, want it kept as the serving master", l)
			}
			if l := pfDFPodLabels(t, c, targetPod); l[v1alpha1.DragonflyRoleLabel] == "master" {
				t.Errorf("empty target relabelled master: %v", l)
			}
			if got := effectiveDragonflyMasterSite(got); got != "iad" {
				t.Errorf("Dragonfly master = %q, want the source", got)
			}
		})
	}
}

// K005: a replica rollout never restarts the planned run's Dragonfly target
// while the run owns Dragonfly: it would come back as an empty standalone
// master.
func TestRollDragonflyReplicaStatefulSet_HoldsPlannedTarget(t *testing.T) {
	fg := plannedFailoverFGWithDragonfly("Proceed")
	r, c := newReconciler(fg)
	requeue, err := r.rollDragonflyReplicaStatefulSet(context.Background(), fg, fg.Spec.Sites[1])
	if err != nil || requeue != dragonflyGateRetry {
		t.Fatalf("roll of the planned target = (%s, %v), want held with a %s requeue", requeue, err, dragonflyGateRetry)
	}
	var sts appsv1.StatefulSet
	key := types.NamespacedName{Namespace: fg.Namespace, Name: dragonflyStatefulSetName(fg.Name, "pdx")}
	if err := c.Get(context.Background(), key, &sts); !apierrors.IsNotFound(err) {
		t.Fatalf("the planned target's StatefulSet was applied (err %v)", err)
	}
	// A run that no longer owns Dragonfly (Admitting owns nothing) does not
	// hold it.
	fg.Status.PlannedFailover.Phase = v1alpha1.PlannedFailoverPhaseAdmitting
	if err := c.Status().Update(context.Background(), fg); err != nil {
		t.Fatal(err)
	}
	if requeue, err := r.rollDragonflyReplicaStatefulSet(context.Background(), fg, fg.Spec.Sites[1]); err != nil || requeue != 0 {
		t.Fatalf("roll with no planned owner = (%s, %v), want applied", requeue, err)
	}
}

// pfLostReplyConn is a target whose REPLTAKEOVER reply is lost: the server
// promotes it (or not) and the client sees a transport error.
type pfLostReplyConn struct {
	*fakeDragonflyConn
	promote   bool
	takeovers *atomic.Int32
}

func (c *pfLostReplyConn) ReplTakeover(context.Context, time.Duration) error {
	c.takeovers.Add(1)
	if c.promote {
		c.info.Role = "master"
	}
	return io.ErrUnexpectedEOF
}

// K033, narrowed in fix round 5: a REPLTAKEOVER whose reply is lost is an
// unconfirmed takeover whatever the target did: the takeover is Failed, no
// Dragonfly role changes (the source is not demoted, evicted or deleted, and
// goes back into the active Service), and onSyncTimeout applies. A server
// error is a definite failure.
func TestPlannedFailoverDragonfly_LostTakeoverReply(t *testing.T) {
	for _, tc := range []struct {
		name    string
		promote bool
	}{{"target promoted", true}, {"target still a replica", false}} {
		t.Run(tc.name, func(t *testing.T) {
			fg := pfDFIntended("Fail", time.Now().Add(-5*time.Second))
			sourcePod := makeDragonflyPod(fg.Name, "iad", "master", true)
			targetPod := makeDragonflyPod(fg.Name, "pdx", "replica", true)
			r, c, _ := pfDFReconciler(t, fg, sourcePod, targetPod)
			var n atomic.Int32
			source := &fakeDragonflyConn{info: dragonfly.ReplicationInfo{Role: "master"}}
			target := &pfLostReplyConn{fakeDragonflyConn: &fakeDragonflyConn{info: dragonfly.ReplicationInfo{Role: "slave"}}, promote: tc.promote, takeovers: &n}
			installFakeDragonflyConn(t, r, map[string]DragonflyConnection{pfDFPdx: target, pfDFIad: source})
			pfDFPass(t, r, fgNN(fg))
			pf := fetchFG(t, r, fgNN(fg)).Status.PlannedFailover
			if pf.Phase != v1alpha1.PlannedFailoverPhaseRollingBack || pf.Dragonfly.Takeover.State != v1alpha1.DragonflyTakeoverFailed ||
				pf.Dragonfly.Reason != v1alpha1.DragonflyPlannedReasonPromotionFailed || !strings.Contains(pf.Dragonfly.Message, "unconfirmed") ||
				pf.Rollback == nil || pf.Rollback.Reason != v1alpha1.PlannedFailoverReasonDragonflyPromotionFailed {
				t.Fatalf("block = %s %+v %q, want the takeover Failed (unconfirmed) and a rollback", pf.Phase, pf.Dragonfly.Takeover, pf.Dragonfly.Message)
			}
			if got := n.Load(); got != 1 {
				t.Errorf("REPLTAKEOVER sent %d times, want 1", got)
			}
			pfDFAssertSourceKept(t, c, source, sourcePod, targetPod)
		})
	}
	t.Run("server error", func(t *testing.T) {
		fg := pfDFIntended("Proceed", time.Now())
		r, _, _ := pfDFReconciler(t, fg, makeDragonflyPod(fg.Name, "iad", "master", true), makeDragonflyPod(fg.Name, "pdx", "replica", true))
		target := &pfTakeoverConn{fakeDragonflyConn: &fakeDragonflyConn{info: dragonfly.ReplicationInfo{Role: "slave"},
			replTakeoverErr: &dragonfly.ServerError{Message: "ERR takeover timed out"}}}
		installFakeDragonflyConn(t, r, map[string]DragonflyConnection{pfDFPdx: target, pfDFIad: &fakeDragonflyConn{info: dragonfly.ReplicationInfo{Role: "master"}}})
		pfDFTakeoverPasses(t, r, fgNN(fg))
		if pf := fetchFG(t, r, fgNN(fg)).Status.PlannedFailover; pf.Dragonfly.Takeover.State != v1alpha1.DragonflyTakeoverFailed {
			t.Fatalf("takeover = %+v, want Failed on a server error", pf.Dragonfly.Takeover)
		}
	})
}

// K064: the takeover's result is recorded in its own write, so a target
// MySQL that cannot be read afterwards does not leave a completed takeover
// Intended and re-run every pass (re-demoting, CLIENT KILLing and deleting
// the source each time).
func TestPlannedFailoverDragonfly_ResultRecordedBeforePromotingIntent(t *testing.T) {
	fg := pfDFIntended("Proceed", time.Now())
	r, _, topo := pfDFReconciler(t, fg, makeDragonflyPod(fg.Name, "iad", "master", true), makeDragonflyPod(fg.Name, "pdx", "replica", true))
	topo.uuid = func(PlannedRunRef) (string, error) { return "", errors.New("target MySQL unreachable") }
	source := &fakeDragonflyConn{info: dragonfly.ReplicationInfo{Role: "master", MasterReplOffset: 100}}
	target := &pfTakeoverConn{fakeDragonflyConn: &fakeDragonflyConn{info: dragonfly.ReplicationInfo{Role: "slave", SlaveReplOffset: 100}}}
	installFakeDragonflyConn(t, r, map[string]DragonflyConnection{pfDFPdx: target, pfDFIad: source})
	for range 3 {
		pfDFPass(t, r, fgNN(fg))
	}
	pf := fetchFG(t, r, fgNN(fg)).Status.PlannedFailover
	if pf.Phase != v1alpha1.PlannedFailoverPhasePromotingDragonfly || pf.Dragonfly.Takeover.State != v1alpha1.DragonflyTakeoverSucceeded {
		t.Fatalf("block = %s %+v, want the takeover recorded Succeeded while the Promoting intent waits", pf.Phase, pf.Dragonfly.Takeover)
	}
	if target.takeovers != 1 || len(source.clientKillTypes) != 1 {
		t.Errorf("REPLTAKEOVER sent %d times, source CLIENT KILLed %d times; want each once", target.takeovers, len(source.clientKillTypes))
	}
}

// K049: a takeover result this process established but could not write is
// recorded before the MySQL loss windows are checked, so a source-loss
// rollback keeps the takeover Succeeded (and the target's Dragonfly
// authority); a terminal record built while the result is pending carries
// it and the sessions-lost warning.
func TestPlannedFailoverDragonfly_PendingResultRecordedBeforeLossExit(t *testing.T) {
	fg := pfDFIntended("Proceed", time.Now())
	var failStatus atomic.Bool
	failStatus.Store(true)
	funcs := interceptor.Funcs{
		SubResourceUpdate: func(ctx context.Context, c client.Client, sub string, obj client.Object, opts ...client.SubResourceUpdateOption) error {
			if _, ok := obj.(*v1alpha1.MysqlFailoverGroup); ok && failStatus.Load() {
				return errors.New("simulated status outage")
			}
			return c.SubResource(sub).Update(ctx, obj, opts...)
		},
		SubResourcePatch: func(ctx context.Context, c client.Client, sub string, obj client.Object, patch client.Patch, opts ...client.SubResourcePatchOption) error {
			if _, ok := obj.(*v1alpha1.MysqlFailoverGroup); ok && failStatus.Load() {
				return errors.New("simulated status outage")
			}
			return c.SubResource(sub).Patch(ctx, obj, patch, opts...)
		},
	}
	r, _, topo := pfDFReconcilerWithFuncs(t, fg, funcs, makeDragonflyPod(fg.Name, "iad", "master", true), makeDragonflyPod(fg.Name, "pdx", "replica", true))
	topo.probe = func(PlannedRunRef) (LagProbe, error) { return LagProbe{TargetReachable: true}, nil } // the MySQL source is unreachable
	target := &pfTakeoverConn{fakeDragonflyConn: &fakeDragonflyConn{info: dragonfly.ReplicationInfo{Role: "slave", SlaveReplOffset: 100}}}
	installFakeDragonflyConn(t, r, map[string]DragonflyConnection{pfDFPdx: target,
		pfDFIad: &fakeDragonflyConn{info: dragonfly.ReplicationInfo{Role: "master", MasterReplOffset: 100}}})

	pfDFPass(t, r, fgNN(fg)) // takes over; the result write fails
	if target.takeovers != 1 {
		t.Fatalf("REPLTAKEOVER sent %d times, want 1", target.takeovers)
	}
	pf := fetchFG(t, r, fgNN(fg)).Status.PlannedFailover
	if pf.Dragonfly.Takeover.State != v1alpha1.DragonflyTakeoverIntended {
		t.Fatalf("fixture: the result write committed: %+v", pf.Dragonfly.Takeover)
	}
	// A terminal record built now carries the pending result.
	rec := r.plannedFailoverEngine().terminalRecord(pf, plannedEnd{outcome: v1alpha1.PlannedFailoverOutcomeSourceLeftFenced,
		reason: v1alpha1.PlannedFailoverReasonStatusStalled})
	if rec.Dragonfly.Takeover.State != v1alpha1.DragonflyTakeoverSucceeded ||
		!strings.Contains(fmt.Sprint(rec.Result.Warnings), string(v1alpha1.PlannedFailoverWarningDragonflySessionsLost)) {
		t.Errorf("terminal record = takeover %+v warnings %v, want Succeeded with DragonflySessionsLost", rec.Dragonfly.Takeover, rec.Result.Warnings)
	}

	failStatus.Store(false)
	r.plannedClock.(*clock.FakeClock).Advance(plannedFailoverLossWindow + time.Second) // the source-loss window has run out
	for i := 0; i < 4 && fetchFG(t, r, fgNN(fg)).Status.PlannedFailover.Phase != v1alpha1.PlannedFailoverPhaseRollingBack; i++ {
		pfDFPass(t, r, fgNN(fg))
	}
	got := fetchFG(t, r, fgNN(fg))
	pf = got.Status.PlannedFailover
	if pf.Phase != v1alpha1.PlannedFailoverPhaseRollingBack || pf.Dragonfly.Takeover.State != v1alpha1.DragonflyTakeoverSucceeded {
		t.Fatalf("block = %s %+v, want a rollback with the takeover kept Succeeded", pf.Phase, pf.Dragonfly.Takeover)
	}
	if target.takeovers != 1 {
		t.Errorf("REPLTAKEOVER sent %d times, want 1", target.takeovers)
	}
	if m := effectiveDragonflyMasterSite(got); m != "pdx" {
		t.Errorf("Dragonfly master = %q during the rollback, want the target that took over", m)
	}
}

// K086, narrowed in fix round 5: past the takeover deadline a busy gate
// fails the takeover at once, whatever the target reports: nothing past the
// deadline could finish it (only a success reply this process receives
// does), so waiting for the gate would only keep the source fenced.
func TestPlannedFailoverDragonfly_GateBusyPastDeadline(t *testing.T) {
	for _, role := range []string{"master", "slave"} {
		t.Run("target "+role, func(t *testing.T) {
			probe := pfDFIntended("Fail", time.Now())
			late := time.Now().Add(-plannedDragonflyTakeoverDeadline(probe.Status.PlannedFailover) - 5*time.Second)
			fg := pfDFIntended("Fail", late)
			r, _, _ := pfDFReconciler(t, fg)
			runner := &TopologyManagerRunner{}
			r.Runner = runner
			r.dragonflyGateWait = 10 * time.Millisecond
			release, ok := runner.DragonflyGate(fgNN(fg)).tryAcquire()
			if !ok {
				t.Fatal("gate busy")
			}
			defer release()
			installFakeDragonflyConn(t, r, map[string]DragonflyConnection{pfDFPdx: &fakeDragonflyConn{info: dragonfly.ReplicationInfo{Role: role}}})
			if _, err := pfDragonflyTakeoverPass(t, r, context.Background(), fgNN(fg)); err != nil {
				t.Fatal(err)
			}
			pf := fetchFG(t, r, fgNN(fg)).Status.PlannedFailover
			if pf.Dragonfly.Takeover.State != v1alpha1.DragonflyTakeoverFailed || pf.Phase != v1alpha1.PlannedFailoverPhaseRollingBack {
				t.Fatalf("block = %s %+v, want the takeover Failed", pf.Phase, pf.Dragonfly.Takeover)
			}
		})
	}
}

// K136: the takeover refuses when the Dragonfly master is a third site:
// stripping and demoting the MySQL source would leave the real master in the
// active Service beside the promoted target.
func TestPlannedFailoverDragonfly_RefusesWhenDragonflyMasterIsAThirdSite(t *testing.T) {
	fg := pfDFIntended("Proceed", time.Now())
	third := *fg.Spec.Sites[1].DeepCopy()
	third.Name = "ord"
	fg.Spec.Sites = append(fg.Spec.Sites, third)
	fg.Status.Dragonfly = &v1alpha1.DragonflyStatus{ActiveSite: "ord"}
	sourcePod := makeDragonflyPod(fg.Name, "iad", "replica", true)
	ordPod := makeDragonflyPod(fg.Name, "ord", "master", true)
	r, c, _ := pfDFReconciler(t, fg, sourcePod, ordPod, makeDragonflyPod(fg.Name, "pdx", "replica", true))
	target := &pfTakeoverConn{fakeDragonflyConn: &fakeDragonflyConn{info: dragonfly.ReplicationInfo{Role: "slave"}}}
	installFakeDragonflyConn(t, r, map[string]DragonflyConnection{pfDFPdx: target})
	pfDFTakeoverPasses(t, r, fgNN(fg))
	pf := fetchFG(t, r, fgNN(fg)).Status.PlannedFailover
	if pf.Phase != v1alpha1.PlannedFailoverPhasePromoting || pf.Dragonfly.Takeover.State != v1alpha1.DragonflyTakeoverFailed ||
		!strings.Contains(pf.Dragonfly.Message, `the Dragonfly master is "ord"`) {
		t.Fatalf("block = %s %+v %q, want the takeover refused", pf.Phase, pf.Dragonfly.Takeover, pf.Dragonfly.Message)
	}
	if target.takeovers != 0 {
		t.Errorf("REPLTAKEOVER sent %d times", target.takeovers)
	}
	if l := pfDFPodLabels(t, c, ordPod); l[v1alpha1.DragonflyRoleLabel] != "master" || l[v1alpha1.DragonflyTrafficLabel] != dragonflyTrafficEnabled {
		t.Errorf("the real Dragonfly master was touched: %v", l)
	}
}

// pfDFSucceededBlock is fg's run past a succeeded takeover to pdx.
func pfDFSucceededBlock(phase v1alpha1.PlannedFailoverPhase, completed time.Time) *v1alpha1.MysqlFailoverGroup {
	fg := plannedFailoverFGWithDragonfly("Proceed")
	pf := fg.Status.PlannedFailover
	pf.Phase = phase
	at := metav1.NewTime(completed)
	pf.Dragonfly.Takeover = &v1alpha1.DragonflyTakeoverRecord{State: v1alpha1.DragonflyTakeoverSucceeded, IntentTime: at, CompletionTime: &at}
	return fg
}

// K007, K137, K084: one takeover predicate decides both the label sweep's
// master and the observed master. A later successful Dragonfly promotion to
// another site supersedes the run's takeover; a target observed as a
// replica is not pinned; a terminal marker does not drop a succeeded
// takeover (its block is the only durable record); a run without an ID
// never pins.
func TestPlannedTakeoverMasterSite(t *testing.T) {
	completed := time.Now().Add(-time.Minute).Truncate(time.Second)
	fg := pfDFSucceededBlock(v1alpha1.PlannedFailoverPhasePromoting, completed)
	snaps := []DragonflySiteSnapshot{{Name: "iad", Reachable: true, Info: dragonfly.ReplicationInfo{Role: "master"}},
		{Name: "pdx", Reachable: true, Info: dragonfly.ReplicationInfo{Role: "master"}}}
	if effectiveDragonflyMasterSite(fg) != "pdx" || activeDragonflySiteFromObservation(fg, snaps, false) != "pdx" {
		t.Fatal("a succeeded takeover does not pin its target")
	}

	t.Run("superseded by a later promotion", func(t *testing.T) {
		fg := fg.DeepCopy()
		later := metav1.NewTime(completed.Add(5 * time.Second))
		fg.Status.Dragonfly = &v1alpha1.DragonflyStatus{ActiveSite: "iad", LastPromotionTarget: "iad", LastPromotionTime: &later}
		if got := effectiveDragonflyMasterSite(fg); got != "iad" {
			t.Errorf("label master = %q after an emergency promotion to iad, want iad", got)
		}
		if got := activeDragonflySiteFromObservation(fg, snaps, false); got != "iad" {
			t.Errorf("observed master = %q after an emergency promotion to iad, want iad", got)
		}
		earlier := metav1.NewTime(completed.Add(-time.Minute))
		fg.Status.Dragonfly.LastPromotionTime = &earlier
		if got := effectiveDragonflyMasterSite(fg); got != "pdx" {
			t.Errorf("a promotion stamped before the takeover superseded it: %q", got)
		}
	})
	t.Run("target observed as a replica", func(t *testing.T) {
		demoted := []DragonflySiteSnapshot{{Name: "iad", Reachable: true, Info: dragonfly.ReplicationInfo{Role: "master"}},
			{Name: "pdx", Reachable: true, Info: dragonfly.ReplicationInfo{Role: "slave"}}}
		if got := activeDragonflySiteFromObservation(fg, demoted, false); got != "iad" {
			t.Errorf("observed master = %q with the target a replica, want iad", got)
		}
	})
	t.Run("marker-ended run keeps its takeover", func(t *testing.T) {
		fg := fg.DeepCopy()
		fg.Annotations = map[string]string{v1alpha1.PlannedFailoverTerminalAnnotation: `{"schemaVersion":1,"runID":"run-1","outcome":"SourceLeftFenced","reason":"StatusStalled"}`}
		if got := effectiveDragonflyMasterSite(fg); got != "pdx" {
			t.Errorf("label master = %q for a marker-ended run whose takeover succeeded, want pdx", got)
		}
		if plannedFailoverDragonflyStripActive(fg) {
			t.Error("a marker-ended run strips the source")
		}
	})
	t.Run("runID-less block", func(t *testing.T) {
		fg := fg.DeepCopy()
		fg.Status.PlannedFailover.RunID = ""
		if a, b := effectiveDragonflyMasterSite(fg), activeDragonflySiteFromObservation(fg, snaps[:1], false); a != "iad" || b != "iad" {
			t.Errorf("runID-less block: label master %q, observed master %q; want both iad", a, b)
		}
	})
	t.Run("terminal block", func(t *testing.T) {
		fg := pfDFSucceededBlock(v1alpha1.PlannedFailoverPhaseFailed, completed)
		if got := effectiveDragonflyMasterSite(fg); got != "iad" {
			t.Errorf("terminal block pins %q", got)
		}
	})
}

// K089: with no hydrated slot, a durable hold owns the group's Dragonfly:
// a snapshot upgrade that spec drift wants during the hold waits.
func TestDragonflySnapshotUpgrade_WaitsDuringUnhydratedHold(t *testing.T) {
	fg := fgWithDragonflySnapshotUpgrade()
	setPlannedHold(&fg.Status, v1alpha1.PlannedFailoverHold{RunID: "run-1", Reason: v1alpha1.PlannedFailoverReasonAuthorityChanged,
		SourceSite: "dc1", TargetSite: "dc2", HoldTime: metav1.Now()}, fg.Generation)
	if !plannedFailoverOwnsDragonfly(fg, false) {
		t.Fatal("a durable hold does not own the group's Dragonfly")
	}
	r, c := newReconciler(fg)
	seedDragonflyStatefulSets(t, r, c, fg, bothSites(dfSourceImage))
	step, err := r.reconcileDragonflySnapshotUpgrade(context.Background(), fg)
	if err != nil || step.wait != dragonflyUpgradeDeferRetry || step.hold != 0 {
		t.Fatalf("= (%+v, %v), want a wait", step, err)
	}
	got := getFGByKey(t, c, fgNN(fg))
	if got.Status.Dragonfly.Upgrade != nil {
		t.Fatalf("upgrade started during a hold: %+v", got.Status.Dragonfly.Upgrade)
	}
}

// K121: the upgrade pause follows the upgrade state read under the gate. A
// stale cached in-flight upgrade neither re-pauses the manager after the
// upgrade ended nor leaves a stale pause in place.
func TestDragonflySnapshotUpgrade_PauseFollowsUncachedState(t *testing.T) {
	fg := fgWithDragonflySnapshotUpgrade()
	start := metav1.Now()
	fg.Status.Dragonfly.Upgrade = &v1alpha1.DragonflyUpgradeStatus{Phase: v1alpha1.DragonflyUpgradePhaseFailed, Reason: "ActiveSiteChanged",
		ActiveSite: "dc1", StartTime: &start, CompletionTime: &start}
	r, c := newReconciler(fg)
	nn := fgNN(fg)
	runner := &TopologyManagerRunner{logger: slog.Default(), managers: make(map[types.NamespacedName]*managedTopology)}
	tm, _, _ := newTestTopologyManager(&mockMySQL{readOnly: true}, &mockMySQL{})
	df := NewDragonflyManager(c, record.NewFakeRecorder(8), slog.Default(), nn, time.Second)
	runner.registerManager(nn, &managedTopology{tm: tm, dragonfly: df})
	r.Runner = runner

	stale := fg.DeepCopy()
	stale.Status.Dragonfly.Upgrade.Phase = v1alpha1.DragonflyUpgradePhaseReattachingReplicas // the informer has not seen the abandonment
	df.SetUpgradePaused(true)                                                                // left by the stale read
	if _, err := r.reconcileDragonflySnapshotUpgrade(context.Background(), stale); err != nil {
		t.Fatal(err)
	}
	if df.upgradePaused.Load() {
		t.Fatal("a stale cached in-flight upgrade left the manager paused after the upgrade ended")
	}
}

// K122: a snapshot upgrade is not admitted while the Dragonfly master
// differs from the MySQL active site (a planned takeover failed after MySQL
// moved, before drift repair): it would save and restart a replica. The
// run waits, and starts once they match.
func TestDragonflySnapshotUpgrade_DeferredWhileDragonflyMasterDiffers(t *testing.T) {
	fg := fgWithDragonflySnapshotUpgrade()
	fg.Status.ActiveSite = "dc2"
	fg.Status.Dragonfly.ActiveSite = "dc1"
	r, c := newReconciler(fg)
	seedDragonflyStatefulSets(t, r, c, fg, bothSites(dfSourceImage))
	step, err := r.reconcileDragonflySnapshotUpgrade(context.Background(), fg)
	if err != nil || step.wait != dragonflyUpgradeDeferRetry || step.hold != 0 {
		t.Fatalf("= (%+v, %v), want a %s wait", step, err, dragonflyUpgradeDeferRetry)
	}
	got := getFGByKey(t, c, fgNN(fg))
	if got.Status.Dragonfly.Upgrade != nil {
		t.Fatalf("upgrade admitted against the wrong site: %+v", got.Status.Dragonfly.Upgrade)
	}
	got.Status.Dragonfly.ActiveSite = "dc2"
	if err := c.Status().Update(context.Background(), got); err != nil {
		t.Fatal(err)
	}
	if step, err := r.reconcileDragonflySnapshotUpgrade(context.Background(), getFGByKey(t, c, fgNN(fg))); err != nil || step.hold == 0 {
		t.Fatalf("once they match = (%+v, %v), want a started run", step, err)
	}
	if up := getFGByKey(t, c, fgNN(fg)).Status.Dragonfly.Upgrade; up == nil || up.ActiveSite != "dc2" {
		t.Fatalf("upgrade = %+v, want a run retaining dc2", up)
	}
}

// K065 (ported from the pre-v1beta1 suite): a failed source demote leaves the
// source out of the active Service rather than restoring its traffic label
// beside the new master.
func TestPlannedFailoverDragonfly_DemoteFailsDoesNotRestoreSourceTraffic(t *testing.T) {
	fg := pfDFIntended("Proceed", time.Now())
	sourcePod := makeDragonflyPod(fg.Name, "iad", "master", true)
	funcs := interceptor.Funcs{
		Update: func(ctx context.Context, c client.WithWatch, obj client.Object, opts ...client.UpdateOption) error {
			if pod, ok := obj.(*corev1.Pod); ok && pod.Name == sourcePod.Name && pod.Labels[v1alpha1.DragonflyRoleLabel] == "replica" {
				return errors.New("simulated demote-patch failure")
			}
			return c.Update(ctx, obj, opts...)
		},
	}
	r, c, _ := pfDFReconcilerWithFuncs(t, fg, funcs, sourcePod, makeDragonflyPod(fg.Name, "pdx", "replica", true))
	target := &pfTakeoverConn{fakeDragonflyConn: &fakeDragonflyConn{info: dragonfly.ReplicationInfo{Role: "slave", SlaveReplOffset: 100}}}
	installFakeDragonflyConn(t, r, map[string]DragonflyConnection{pfDFPdx: target, pfDFIad: &fakeDragonflyConn{info: dragonfly.ReplicationInfo{Role: "master"}}})
	pfDFPass(t, r, fgNN(fg))
	if target.takeovers != 1 {
		t.Fatalf("REPLTAKEOVER sent %d times", target.takeovers)
	}
	if l := pfDFPodLabels(t, c, sourcePod); l[v1alpha1.DragonflyTrafficLabel] == dragonflyTrafficEnabled {
		t.Errorf("source traffic restored after a failed demote (it still reads role=%q): %v", l[v1alpha1.DragonflyRoleLabel], l)
	}
}

// pfHookTakeoverConn flips the real Dragonfly master inside REPLTAKEOVER.
type pfHookTakeoverConn struct {
	*fakeDragonflyConn
	onTakeover func()
}

func (c *pfHookTakeoverConn) ReplTakeover(ctx context.Context, d time.Duration) error {
	c.onTakeover()
	c.info.Role = "master"
	return c.fakeDragonflyConn.ReplTakeover(ctx, d)
}

// K065 (ported): during strip -> REPLTAKEOVER -> relabel, the active Service
// selector (role=master AND traffic=true) never matches a pod that is
// not the real Dragonfly master at that instant.
func TestPlannedFailoverDragonfly_NoReadOnlyMidFlight(t *testing.T) {
	fg := pfDFIntended("Proceed", time.Now())
	var (
		mu         sync.Mutex
		realMaster = "iad"
		violations []string
		checks     int
	)
	check := func(c client.Reader, label string) {
		var pods corev1.PodList
		if err := c.List(context.Background(), &pods, client.InNamespace(fg.Namespace), client.MatchingLabels{
			labelAppName: dragonflyAppName, labelInstance: fg.Name, v1alpha1.DragonflyRoleLabel: "master", v1alpha1.DragonflyTrafficLabel: dragonflyTrafficEnabled,
		}); err != nil {
			return
		}
		mu.Lock()
		defer mu.Unlock()
		checks++
		for _, p := range pods.Items {
			if site := p.Labels[v1alpha1.SiteLabel]; site != realMaster {
				violations = append(violations, fmt.Sprintf("after %s: pod %s (site %s) is in the active Service but the real master is %s", label, p.Name, site, realMaster))
			}
		}
	}
	funcs := interceptor.Funcs{
		Update: func(ctx context.Context, c client.WithWatch, obj client.Object, opts ...client.UpdateOption) error {
			if err := c.Update(ctx, obj, opts...); err != nil {
				return err
			}
			if pod, ok := obj.(*corev1.Pod); ok {
				check(c, "update "+pod.Name)
			}
			return nil
		},
	}
	r, c, _ := pfDFReconcilerWithFuncs(t, fg, funcs, makeDragonflyPod(fg.Name, "iad", "master", true), makeDragonflyPod(fg.Name, "pdx", "replica", true))
	target := &pfHookTakeoverConn{fakeDragonflyConn: &fakeDragonflyConn{info: dragonfly.ReplicationInfo{Role: "slave", SlaveReplOffset: 100}},
		onTakeover: func() { mu.Lock(); realMaster = "pdx"; mu.Unlock() }}
	installFakeDragonflyConn(t, r, map[string]DragonflyConnection{pfDFPdx: target, pfDFIad: &fakeDragonflyConn{info: dragonfly.ReplicationInfo{Role: "master"}}})
	check(c, "initial")
	pfDFTakeoverPasses(t, r, fgNN(fg))
	check(c, "final")
	mu.Lock()
	defer mu.Unlock()
	for _, v := range violations {
		t.Error(v)
	}
	if checks < 4 {
		t.Errorf("selector checked %d times; the takeover did not relabel", checks)
	}
	if pf := fetchFG(t, r, fgNN(fg)).Status.PlannedFailover; pf.Dragonfly.Takeover.State != v1alpha1.DragonflyTakeoverSucceeded {
		t.Fatalf("takeover = %+v", pf.Dragonfly.Takeover)
	}
}

// pfDFAssertSourceKept checks the source was never demoted, evicted or
// deleted and serves again, and the target was not made master.
func pfDFAssertSourceKept(t *testing.T, c client.Client, source *fakeDragonflyConn, sourcePod, targetPod *corev1.Pod) {
	t.Helper()
	if len(source.clientKillTypes) != 0 || pfDFPodGone(t, c, sourcePod) {
		t.Errorf("source evicted (CLIENT KILL %v) or deleted after a takeover that never happened", source.clientKillTypes)
	}
	if l := pfDFPodLabels(t, c, sourcePod); l[v1alpha1.DragonflyRoleLabel] != "master" || l[v1alpha1.DragonflyTrafficLabel] != dragonflyTrafficEnabled {
		t.Errorf("source labels = %v, want it kept as the serving master", l)
	}
	if l := pfDFPodLabels(t, c, targetPod); l[v1alpha1.DragonflyRoleLabel] == "master" {
		t.Errorf("empty target relabelled master: %v", l)
	}
}
