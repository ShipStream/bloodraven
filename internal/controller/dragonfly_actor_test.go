package controller

import (
	"context"
	"log/slog"
	"maps"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/tools/record"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"

	v1alpha1 "github.com/shipstream/bloodraven/api/v1alpha1"
	"github.com/shipstream/bloodraven/internal/dragonfly"
)

// Tests for the Dragonfly manager as an actor: emergency requests are
// queued without blocking and executed by the manager's own Run goroutine
// between Ticks, and every Dragonfly routing writer for a group holds the
// group's stable gate. Several tests are ports of the review reproductions
// for finding 4155662865055545 (rounds 1-4); their names say which.

// waitUntil polls cond until it holds or 5s pass.
func waitUntil(t *testing.T, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for !cond() {
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for %s", what)
		}
		time.Sleep(time.Millisecond)
	}
}

// returnsWithin fails the test unless fn returns within a second.
func returnsWithin(t *testing.T, what string, fn func()) {
	t.Helper()
	done := make(chan struct{})
	go func() { fn(); close(done) }()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatalf("%s blocked", what)
	}
}

// startActorWithRequest runs mgr.Run with its first Tick skipped (the test
// holds the gate while Run starts), queues target/oldSource, and lets the
// gate go only once Run is waiting for it to execute the request. Run is
// stopped at cleanup.
func startActorWithRequest(t *testing.T, mgr *DragonflyManager, target, oldSource string) {
	t.Helper()
	mgr.pollInterval = time.Hour
	release, ok := mgr.gate.tryAcquire()
	if !ok {
		t.Fatal("gate busy before Run started")
	}
	go mgr.Run(context.Background())
	t.Cleanup(func() {
		if !mgr.Stop(10 * time.Second) {
			t.Error("Dragonfly manager Run did not stop")
		}
	})
	mgr.Enqueue(target, oldSource)
	waitUntil(t, "Run to wait for the gate", mgr.gate.contended)
	release()
}

// ctxCheckingFuncs makes the fake API client refuse calls on an ended
// context, as a real client does. listHook, if set, runs first on every List.
func ctxCheckingFuncs(listHook func(ctx context.Context)) interceptor.Funcs {
	return interceptor.Funcs{
		Get: func(ctx context.Context, cl client.WithWatch, key client.ObjectKey, obj client.Object, opts ...client.GetOption) error {
			if err := ctx.Err(); err != nil {
				return err
			}
			return cl.Get(ctx, key, obj, opts...)
		},
		List: func(ctx context.Context, cl client.WithWatch, list client.ObjectList, opts ...client.ListOption) error {
			if listHook != nil {
				listHook(ctx)
			}
			if err := ctx.Err(); err != nil {
				return err
			}
			return cl.List(ctx, list, opts...)
		},
		Update: func(ctx context.Context, cl client.WithWatch, obj client.Object, opts ...client.UpdateOption) error {
			if err := ctx.Err(); err != nil {
				return err
			}
			return cl.Update(ctx, obj, opts...)
		},
		Patch: func(ctx context.Context, cl client.WithWatch, obj client.Object, patch client.Patch, opts ...client.PatchOption) error {
			if err := ctx.Err(); err != nil {
				return err
			}
			return cl.Patch(ctx, obj, patch, opts...)
		},
		Delete: func(ctx context.Context, cl client.WithWatch, obj client.Object, opts ...client.DeleteOption) error {
			if err := ctx.Err(); err != nil {
				return err
			}
			return cl.Delete(ctx, obj, opts...)
		},
		SubResourcePatch: func(ctx context.Context, cl client.Client, sub string, obj client.Object, patch client.Patch, opts ...client.SubResourcePatchOption) error {
			if err := ctx.Err(); err != nil {
				return err
			}
			return cl.SubResource(sub).Patch(ctx, obj, patch, opts...)
		},
	}
}

func getFGByKey(t *testing.T, c client.Client, key types.NamespacedName) *v1alpha1.MysqlFailoverGroup {
	t.Helper()
	var fg v1alpha1.MysqlFailoverGroup
	if err := c.Get(context.Background(), key, &fg); err != nil {
		t.Fatalf("get fg: %v", err)
	}
	return &fg
}

func dragonflyActiveSite(fg *v1alpha1.MysqlFailoverGroup) string {
	if fg.Status.Dragonfly == nil {
		return ""
	}
	return fg.Status.Dragonfly.ActiveSite
}

func dragonflyPromotedTo(c client.Client, key types.NamespacedName, site string) func() bool {
	return func() bool {
		var fg v1alpha1.MysqlFailoverGroup
		if err := c.Get(context.Background(), key, &fg); err != nil {
			return false
		}
		df := fg.Status.Dragonfly
		return df != nil && df.ActiveSite == site && df.LastPromotionTarget == site
	}
}

// podLabels returns every pod's labels, keyed by pod name.
func podLabels(t *testing.T, c client.Client, ns string) map[string]map[string]string {
	t.Helper()
	var pods corev1.PodList
	if err := c.List(context.Background(), &pods, client.InNamespace(ns)); err != nil {
		t.Fatal(err)
	}
	out := map[string]map[string]string{}
	for _, p := range pods.Items {
		out[p.Name] = maps.Clone(p.Labels)
	}
	return out
}

// stallingTakeoverConn's REPLTAKEOVER blocks until release is closed (then
// succeeds) or its context ends.
type stallingTakeoverConn struct {
	*fakeDragonflyConn
	started chan struct{}
	release chan struct{}
}

func (c *stallingTakeoverConn) ReplTakeover(ctx context.Context, _ time.Duration) error {
	close(c.started)
	select {
	case <-c.release:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

// serverSideTakeoverConn runs onTakeover in place of REPLTAKEOVER.
type serverSideTakeoverConn struct {
	*fakeDragonflyConn
	onTakeover func(ctx context.Context) error
}

func (c *serverSideTakeoverConn) ReplTakeover(ctx context.Context, _ time.Duration) error {
	return c.onTakeover(ctx)
}

// --- queue -------------------------------------------------------------------

// Enqueue keeps only the latest request (the latest MySQL decision) and a
// single wake-up; Run executes it once.
func TestDragonflyManager_EnqueueCoalescesLatestWins(t *testing.T) {
	mgr, get := newEmergencyManager(t)
	mgr.Enqueue("dc1", "dc2")
	mgr.Enqueue("dc2", "dc1")
	mgr.pendingMu.Lock()
	p := mgr.pending
	mgr.pendingMu.Unlock()
	if p == nil || p.target != "dc2" || p.oldSource != "dc1" {
		t.Fatalf("pending = %+v, want the latest request dc1->dc2", p)
	}
	if n := len(mgr.wake); n != 1 {
		t.Fatalf("wake-ups queued = %d, want 1", n)
	}
	conns := &emergencyConnector{}
	conns.queue(emergencyTargetAddr, &fakeDragonflyConn{})
	conns.queue(emergencySourceAddr, &fakeDragonflyConn{}, &fakeDragonflyConn{})
	mgr.SetConnector(conns.connect)

	mgr.runPending(context.Background())
	mgr.runPending(context.Background()) // nothing left to run

	if got := get().Status.Dragonfly; got == nil || got.ActiveSite != "dc2" {
		t.Fatalf("status.dragonfly = %+v, want the latest request promoted", got)
	}
	conns.mu.Lock()
	defer conns.mu.Unlock()
	if conns.dials[emergencyTargetAddr] != 1 {
		t.Errorf("target dials = %d, want one promotion", conns.dials[emergencyTargetAddr])
	}
}

// A stopping manager admits nothing: Enqueue drops the request, and
// stopping discards one already queued.
func TestDragonflyManager_EnqueueDroppedWhileStopping(t *testing.T) {
	mgr, _ := newEmergencyManager(t)
	mgr.Enqueue("dc2", "dc1")
	mgr.BeginStop()
	mgr.pendingMu.Lock()
	queued := mgr.pending
	mgr.pendingMu.Unlock()
	if queued != nil {
		t.Fatalf("BeginStop kept the queued request %+v", queued)
	}
	mgr.Enqueue("dc2", "dc1")
	mgr.pendingMu.Lock()
	queued = mgr.pending
	mgr.pendingMu.Unlock()
	if queued != nil {
		t.Fatalf("stopping manager queued %+v", queued)
	}
}

// The MySQL emergency callback never waits for Dragonfly: it returns while
// another writer holds the gate, and while the manager's Run goroutine is
// stuck in a Dragonfly command.
func TestEmergencyDragonflyCallback_ReturnsWhileDragonflyCommandStalled(t *testing.T) {
	mgr, get := newEmergencyManager(t)
	mgr.pollInterval = time.Hour
	mgr.emergencyWaits = func(time.Duration) (time.Duration, time.Duration) { return time.Minute, time.Minute }
	stalled := &stallingTakeoverConn{fakeDragonflyConn: &fakeDragonflyConn{}, started: make(chan struct{}), release: make(chan struct{})}
	var releaseOnce sync.Once
	unstall := func() { releaseOnce.Do(func() { close(stalled.release) }) }
	t.Cleanup(unstall)
	conns := &emergencyConnector{}
	conns.queue(emergencyTargetAddr, stalled)
	conns.queue(emergencySourceAddr, &fakeDragonflyConn{}, &fakeDragonflyConn{})
	mgr.SetConnector(conns.connect)
	callback := emergencyDragonflyCallback(mgr)

	// A planned failover (say) holds the gate.
	release, ok := mgr.gate.tryAcquire()
	if !ok {
		t.Fatal("gate busy")
	}
	go mgr.Run(context.Background())
	t.Cleanup(func() { mgr.Stop(10 * time.Second) })
	returnsWithin(t, "emergency callback while the gate is held", func() {
		callback(context.Background(), "dc2", "dc1")
	})
	waitUntil(t, "Run to wait for the gate", mgr.gate.contended)
	release()

	<-stalled.started // Run is now stuck in REPLTAKEOVER.
	returnsWithin(t, "emergency callback while a Dragonfly command is stalled", func() {
		callback(context.Background(), "dc2", "dc1")
	})
	unstall()
	waitUntil(t, "the promotion to be recorded", func() bool { return dragonflyActiveSite(get()) == "dc2" })
	if !mgr.Stop(5 * time.Second) {
		t.Fatal("Run did not stop")
	}
	if stalled.closed != true {
		t.Error("takeover connection left open")
	}
}

// --- lifecycle ---------------------------------------------------------------

// Stop joins Run; a stopped manager starts no Tick or promotion.
func TestDragonflyManager_StopJoinsRunAndAdmitsNothing(t *testing.T) {
	mgr, get := newEmergencyManager(t)
	conns := &emergencyConnector{}
	mgr.SetConnector(conns.connect)
	if !NewDragonflyManager(mgr.client, nil, slog.Default(), mgr.fgKey, time.Second).Stop(time.Second) {
		t.Fatal("Stop() = false for a manager whose Run never started")
	}

	mgr.pollInterval = time.Hour
	go mgr.Run(context.Background())
	waitUntil(t, "Run to start", mgr.runStarted.Load)
	if !mgr.Stop(5 * time.Second) {
		t.Fatal("Stop() = false, want Run joined")
	}
	select {
	case <-mgr.runDone:
	default:
		t.Fatal("Stop returned before Run did")
	}
	conns.mu.Lock()
	before := maps.Clone(conns.dials)
	conns.mu.Unlock()

	mgr.Enqueue("dc2", "dc1")
	if mgr.TryEmergencyPromote(context.Background(), "dc2", "dc1") {
		t.Error("stopped manager promoted")
	}
	mgr.Tick(context.Background())
	conns.mu.Lock()
	after := maps.Clone(conns.dials)
	conns.mu.Unlock()
	if !maps.Equal(before, after) {
		t.Errorf("stopped manager dialled Dragonfly: before %v, after %v", before, after)
	}
	if df := get().Status.Dragonfly; df != nil && df.LastPromotionTarget != "" {
		t.Errorf("stopped manager stamped a promotion: %+v", df)
	}
}

// Round-4 review (Opus R1, TestRereview4RetireAbortsAcknowledgedRoleChange):
// the manager is stopped (config or credential restart) just as a
// REPLTAKEOVER reply lands. The acknowledged role change must still be
// labelled and stamped before Stop returns, so the replacement starts from
// a recorded topology with dc2 behind the active Service.
func TestDragonflyManager_StopAfterAcknowledgedRoleChangeRecordsIt(t *testing.T) {
	fg := fgWithDragonflyEnabledAndActive()
	fg.Status.Dragonfly = &v1alpha1.DragonflyStatus{Enabled: true, ActiveSite: "dc1"}
	source := makeDragonflyPod(fg.Name, "dc1", "master", true)
	target := makeDragonflyPod(fg.Name, "dc2", "replica", true)
	cb, key := newDragonflyFakeClient(fg)

	var mgr *DragonflyManager
	var tookOver atomic.Bool
	var once sync.Once
	stopResult := make(chan bool, 1)
	c := cb.WithObjects(source, target).WithInterceptorFuncs(ctxCheckingFuncs(func(context.Context) {
		if !tookOver.Load() {
			return
		}
		once.Do(func() {
			go func() { stopResult <- mgr.Stop(30 * time.Second) }()
			waitUntil(t, "the stop to begin", mgr.stopping.Load)
		})
	})).Build()
	mgr = NewDragonflyManager(c, record.NewFakeRecorder(32), slog.Default(), key, time.Hour)
	mgr.SetConnector(func(ctx context.Context, addr, _ string) (DragonflyConnection, error) {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		if addr == emergencyTargetAddr {
			return &serverSideTakeoverConn{fakeDragonflyConn: &fakeDragonflyConn{}, onTakeover: func(context.Context) error {
				tookOver.Store(true)
				return nil
			}}, nil
		}
		return &fakeDragonflyConn{}, nil
	})

	startActorWithRequest(t, mgr, "dc2", "dc1")
	select {
	case joined := <-stopResult:
		if !joined {
			t.Fatal("Stop() = false, want the promotion to finish within the stop wait")
		}
	case <-time.After(10 * time.Second):
		t.Fatal("the stop never began")
	}
	got := getFGByKey(t, c, key)
	if got.Status.Dragonfly.ActiveSite != "dc2" {
		t.Errorf("acknowledged takeover to dc2 not recorded before Stop returned: activeSite=%q", got.Status.Dragonfly.ActiveSite)
	}
	if eps := writableEndpoints(t, c, fg); !slices.Equal(eps, []string{"dc2"}) {
		t.Errorf("active-Service endpoints after the stop = %v, want [dc2]", eps)
	}
}

// --- yielding and waiting ------------------------------------------------------

// Round-4 review (Opus R2, TestRereview4AcquireDuringTakeoverLeavesUnrecordedPromotion):
// a planned owner asks for the gate while an emergency REPLTAKEOVER is on
// the wire, and the server completes the takeover although the client
// stops waiting for the reply. The promotion is not cut short: it falls
// back, records the outcome, and only then hands over the gate, so the
// planned owner starts from labels and status that match the servers.
func TestDragonflyManager_OwnerWaitingDuringTakeoverGetsRecordedState(t *testing.T) {
	fg := fgWithDragonflyEnabledAndActive()
	fg.Status.Dragonfly = &v1alpha1.DragonflyStatus{Enabled: true, ActiveSite: "dc1"}
	cb, key := newDragonflyFakeClient(fg)
	c := cb.WithObjects(
		makeDragonflyPod(fg.Name, "dc1", "master", true),
		makeDragonflyPod(fg.Name, "dc2", "replica", true),
	).WithInterceptorFuncs(ctxCheckingFuncs(nil)).Build()
	mgr := NewDragonflyManager(c, record.NewFakeRecorder(32), slog.Default(), key, time.Hour)
	shrinkEmergencyWaits(mgr)

	started := make(chan struct{})
	var startOnce sync.Once
	var serverPromoted atomic.Bool
	mgr.SetConnector(func(ctx context.Context, addr, _ string) (DragonflyConnection, error) {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		if addr == emergencyTargetAddr {
			return &serverSideTakeoverConn{fakeDragonflyConn: &fakeDragonflyConn{}, onTakeover: func(ctx context.Context) error {
				startOnce.Do(func() { close(started) })
				<-ctx.Done()
				serverPromoted.Store(true) // the server finishes; the reply is lost
				return ctx.Err()
			}}, nil
		}
		return &fakeDragonflyConn{}, nil
	})

	startActorWithRequest(t, mgr, "dc2", "dc1")
	<-started
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	release, err := mgr.gate.acquire(ctx)
	if err != nil {
		t.Fatalf("planned owner never got the gate: %v", err)
	}
	defer release()

	if !serverPromoted.Load() {
		t.Fatal("fixture: server-side takeover did not happen")
	}
	if got := getFGByKey(t, c, key); got.Status.Dragonfly.ActiveSite != "dc2" {
		t.Errorf("planned owner starts with status.dragonfly.activeSite=%q while dc2 is the Dragonfly master", got.Status.Dragonfly.ActiveSite)
	}
	if eps := writableEndpoints(t, c, fg); !slices.Equal(eps, []string{"dc2"}) {
		t.Errorf("active-Service endpoints handed to the planned owner = %v, want [dc2]", eps)
	}
}

// Before any role-changing command is sent, a promotion yields to an owner
// waiting for the gate: it sends nothing, gives the old source its traffic
// back, and releases the gate.
func TestDragonflyManager_PromotionYieldsBeforeRoleChange(t *testing.T) {
	mgr, c, fg := newEmergencyManagerWithPods(t)
	entered, proceed := make(chan struct{}), make(chan struct{})
	target := &noOneRecorder{fakeDragonflyConn: &fakeDragonflyConn{}}
	var targetDials atomic.Int32
	mgr.SetConnector(func(_ context.Context, addr, _ string) (DragonflyConnection, error) {
		if addr == emergencyTargetAddr && targetDials.Add(1) == 1 {
			close(entered)
			<-proceed
			return target, nil
		}
		return &fakeDragonflyConn{}, nil
	})
	result := make(chan bool, 1)
	go func() { result <- mgr.TryEmergencyPromote(context.Background(), "dc2", "dc1") }()
	<-entered

	acquired := make(chan func(), 1)
	go func() {
		release, err := mgr.gate.acquire(context.Background())
		if err == nil {
			acquired <- release
		}
	}()
	waitUntil(t, "the owner to wait for the gate", mgr.gate.contended)
	close(proceed)

	if <-result {
		t.Fatal("promotion went ahead while an owner was waiting")
	}
	select {
	case release := <-acquired:
		release()
	case <-time.After(5 * time.Second):
		t.Fatal("waiting owner never got the gate")
	}
	target.mu.Lock()
	sent := target.takeovers > 0 || target.called
	target.mu.Unlock()
	if sent {
		t.Error("a role-changing command was sent after the promotion should have yielded")
	}
	if !target.closed {
		t.Error("target connection left open")
	}
	if eps := writableEndpoints(t, c, fg); !slices.Equal(eps, []string{"dc1"}) {
		t.Errorf("active-Service endpoints = %v, want the old source dc1 restored", eps)
	}
	if got := getFGByKey(t, c, mgr.fgKey); got.Status.Dragonfly.ActiveSite != "dc1" || got.Status.Dragonfly.LastPromotionTarget != "" {
		t.Errorf("yielded promotion touched status: %+v", got.Status.Dragonfly)
	}
}

// Tick skips its cycle while another writer holds the gate.
func TestDragonflyManager_TickSkipsWhileGateHeld(t *testing.T) {
	mgr, get := newEmergencyManager(t)
	conns := &emergencyConnector{}
	mgr.SetConnector(conns.connect)
	release, ok := mgr.gate.tryAcquire()
	if !ok {
		t.Fatal("gate busy")
	}
	defer release()
	mgr.Tick(context.Background())
	conns.mu.Lock()
	dials := len(conns.dials)
	conns.mu.Unlock()
	if dials != 0 {
		t.Errorf("Tick dialled Dragonfly without the gate: %v", conns.dials)
	}
	if df := get().Status.Dragonfly; df != nil {
		t.Errorf("Tick wrote status without the gate: %+v", df)
	}
}

// A Tick that is observing when an owner starts waiting for the gate stops
// before publishing status or acting on its observation.
func TestDragonflyManager_TickYieldsBeforePublishing(t *testing.T) {
	mgr, c, _ := newEmergencyManagerWithPods(t)
	entered, proceed := make(chan struct{}), make(chan struct{})
	var dials atomic.Int32
	mgr.SetConnector(func(_ context.Context, addr, _ string) (DragonflyConnection, error) {
		if dials.Add(1) == 1 {
			close(entered)
			<-proceed
		}
		if addr == emergencySourceAddr {
			return &fakeDragonflyConn{info: dragonfly.ReplicationInfo{Role: "master", MasterReplOffset: 100}}, nil
		}
		return &fakeDragonflyConn{info: dragonfly.ReplicationInfo{Role: "slave", MasterHost: "wrong-source", MasterPort: 6379, MasterLinkStatus: "down"}}, nil
	})
	tickDone := make(chan struct{})
	go func() { mgr.Tick(context.Background()); close(tickDone) }()
	<-entered
	acquired := make(chan func(), 1)
	go func() {
		release, err := mgr.gate.acquire(context.Background())
		if err == nil {
			acquired <- release
		}
	}()
	waitUntil(t, "the owner to wait for the gate", mgr.gate.contended)
	close(proceed)
	<-tickDone
	select {
	case release := <-acquired:
		release()
	case <-time.After(5 * time.Second):
		t.Fatal("waiting owner never got the gate")
	}
	if got := getFGByKey(t, c, mgr.fgKey).Status.Dragonfly; len(got.Sites) != 0 {
		t.Errorf("Tick published its observation after an owner started waiting: %+v", got)
	}
	if n := dials.Load(); n != 2 {
		t.Errorf("dials = %d, want only the two observations (no replica repair)", n)
	}
}

// --- revalidation ------------------------------------------------------------

// Review round 1 (TestReviewQueuedEmergencyDoesNotOverridePlannedPromotion):
// an emergency request is queued while another writer holds the gate, and
// a newer planned failover promotes Dragonfly back to dc1 meanwhile. When
// the request finally runs it is revalidated and dropped: it neither
// promotes dc2 nor deletes the planned primary's pod.
func TestDragonflyManager_QueuedEmergencyDoesNotOverridePlannedPromotion(t *testing.T) {
	mgr, get := newEmergencyManager(t)
	fg := get()
	fg.Status.ActiveSite = "dc2"
	fg.Status.Dragonfly = &v1alpha1.DragonflyStatus{Enabled: true, ActiveSite: "dc1"}
	if err := mgr.client.Status().Update(context.Background(), fg); err != nil {
		t.Fatal(err)
	}
	conns := &emergencyConnector{}
	conns.queue(emergencyTargetAddr, &fakeDragonflyConn{})
	conns.queue(emergencySourceAddr, &fakeDragonflyConn{}, &fakeDragonflyConn{})
	mgr.SetConnector(conns.connect)

	release, ok := mgr.gate.tryAcquire()
	if !ok {
		t.Fatal("gate busy")
	}
	returnsWithin(t, "Enqueue while the gate is held", func() { mgr.Enqueue("dc2", "dc1") })
	ran := make(chan struct{})
	go func() { mgr.runPending(context.Background()); close(ran) }()
	waitUntil(t, "the request to wait for the gate", mgr.gate.contended)

	fg = get()
	fg.Status.PlannedFailover = &v1alpha1.PlannedFailoverStatus{
		Phase: v1alpha1.PlannedFailoverPhasePromoting, SourcePrimary: "dc2", Target: "dc1",
		Dragonfly: &v1alpha1.PlannedFailoverDragonflyStatus{PromotionMethod: "REPLTAKEOVER"},
	}
	now := metav1.Now()
	fg.Status.Dragonfly = &v1alpha1.DragonflyStatus{Enabled: true, ActiveSite: "dc1", LastPromotionTime: &now, LastPromotionTarget: "dc1"}
	if err := mgr.client.Status().Update(context.Background(), fg); err != nil {
		t.Fatal(err)
	}
	pod := makeDragonflyPod(fg.Name, "dc1", "master", true)
	if err := mgr.client.Create(context.Background(), pod); err != nil {
		t.Fatal(err)
	}
	mgr.SetPaused(true)
	release()
	<-ran

	conns.mu.Lock()
	dials := conns.dials[emergencyTargetAddr]
	conns.mu.Unlock()
	if dials != 0 {
		t.Errorf("stale request dialled the target %d times", dials)
	}
	var currentPod corev1.Pod
	if err := mgr.client.Get(context.Background(), client.ObjectKeyFromObject(pod), &currentPod); apierrors.IsNotFound(err) {
		t.Error("stale emergency deleted the newer planned primary's Dragonfly pod")
	}
	if got := dragonflyActiveSite(get()); got != "dc1" {
		t.Errorf("stale emergency moved status.dragonfly.activeSite to %q", got)
	}
}

// A request for a target that another promotion finished after the request
// was made is a duplicate. It must not re-run the promotion: with the old
// master already gone it would report sessions lost although they were
// kept.
func TestDragonflyManager_RequestSkipsTargetPromotedAfterIt(t *testing.T) {
	mgr, get := newEmergencyManager(t)
	conns := &emergencyConnector{}
	conns.queue(emergencyTargetAddr, &noOneRecorder{fakeDragonflyConn: &fakeDragonflyConn{}})
	mgr.SetConnector(conns.connect)
	rec := mgr.recorder.(*record.FakeRecorder)

	mgr.Enqueue("dc2", "dc1")
	if err := stampDragonflyPromotionStatus(context.Background(), mgr.client, mgr.client, mgr.fgKey, "dc2", metav1.Now(), true); err != nil {
		t.Fatalf("stamp: %v", err)
	}
	mgr.runPending(context.Background())

	conns.mu.Lock()
	dials := conns.dials[emergencyTargetAddr]
	conns.mu.Unlock()
	if dials != 0 {
		t.Errorf("duplicate request re-ran the promotion (%d target dials)", dials)
	}
	for _, ev := range drainEventsCh(rec.Events, 4, 50*time.Millisecond) {
		if strings.Contains(ev, ReasonDragonflySessionsLost) {
			t.Errorf("duplicate request reported sessions lost: %q", ev)
		}
	}
	if got := dragonflyActiveSite(get()); got != "dc2" {
		t.Errorf("status.dragonfly.activeSite = %q, want dc2 kept", got)
	}
}

// A request whose target status already names, and which is still master,
// has nothing to do, however long ago that promotion was stamped.
func TestDragonflyManager_RequestSkipsTargetObservedAsMaster(t *testing.T) {
	mgr, get := newEmergencyManager(t)
	fg := get()
	earlier := metav1.NewTime(time.Now().Add(-time.Hour).Truncate(time.Second))
	fg.Status.Dragonfly = &v1alpha1.DragonflyStatus{Enabled: true, ActiveSite: "dc2", LastPromotionTarget: "dc2", LastPromotionTime: &earlier}
	if err := mgr.client.Status().Update(context.Background(), fg); err != nil {
		t.Fatal(err)
	}
	again := &noOneRecorder{fakeDragonflyConn: &fakeDragonflyConn{}}
	conns := &emergencyConnector{}
	conns.queue(emergencyTargetAddr, &fakeDragonflyConn{info: dragonfly.ReplicationInfo{Role: "master", MasterReplOffset: 100}}, again)
	mgr.SetConnector(conns.connect)

	mgr.Enqueue("dc2", "dc1")
	mgr.runPending(context.Background())

	again.mu.Lock()
	defer again.mu.Unlock()
	if again.takeovers != 0 || again.called {
		t.Error("request re-promoted a target that is already master")
	}
	if got := get().Status.Dragonfly; !got.LastPromotionTime.Equal(&earlier) {
		t.Errorf("request re-stamped the promotion: %+v", got)
	}
}

// --- Tick and promotion serialisation (ports of rounds 2-4) ----------------

// promotionLogBarrier pauses the goroutine that logs Tick's promotion
// decision.
type promotionLogBarrier struct {
	slog.Handler
	entered chan struct{}
	resume  chan struct{}
}

func (h promotionLogBarrier) Handle(ctx context.Context, r slog.Record) error {
	if strings.HasPrefix(r.Message, "dragonfly/mysql active-site drift:") {
		close(h.entered)
		<-h.resume
	}
	return h.Handler.Handle(ctx, r)
}

// Rounds 2-3 (TestRereviewTickDuplicateAfterPassingGuard): Tick decides to
// promote and stalls, while the MySQL failover asks for the same promotion.
// The callback returns at once; Tick's promotion runs; the queued request,
// executed after it on the same goroutine, sees the target already promoted
// and does not repeat it with REPLICAOF NO ONE or report sessions lost.
func TestDragonflyManager_TickPromotionAndQueuedRequestDoNotDuplicate(t *testing.T) {
	mgr, get := newEmergencyManager(t)
	mgr.pollInterval = time.Hour
	fg := get()
	fg.Status.ActiveSite = "dc2"
	if err := mgr.client.Status().Update(context.Background(), fg); err != nil {
		t.Fatal(err)
	}
	entered, resume := make(chan struct{}), make(chan struct{})
	mgr.logger = slog.New(promotionLogBarrier{Handler: slog.Default().Handler(), entered: entered, resume: resume})
	duplicate := &noOneRecorder{fakeDragonflyConn: &fakeDragonflyConn{}}
	conns := &emergencyConnector{}
	conns.queue(emergencyTargetAddr,
		&fakeDragonflyConn{info: dragonfly.ReplicationInfo{Role: "slave", MasterHost: "lion-dragonfly-dc1.shared-lion.svc.cluster.local", MasterPort: 6379, MasterLinkStatus: "up"}},
		&fakeDragonflyConn{}, duplicate)
	conns.queue(emergencySourceAddr,
		&fakeDragonflyConn{info: dragonfly.ReplicationInfo{Role: "master", MasterReplOffset: 100}},
		&fakeDragonflyConn{}, &fakeDragonflyConn{})
	mgr.SetConnector(conns.connect)

	go mgr.Run(context.Background())
	t.Cleanup(func() { mgr.Stop(10 * time.Second) })
	<-entered // Tick has decided to promote dc2.
	returnsWithin(t, "emergency callback during Tick", func() {
		emergencyDragonflyCallback(mgr)(context.Background(), "dc2", "dc1")
	})
	close(resume)
	waitUntil(t, "Tick's promotion to be recorded", dragonflyPromotedTo(mgr.client, mgr.fgKey, "dc2"))
	waitUntil(t, "the queued request to run", func() bool {
		mgr.pendingMu.Lock()
		defer mgr.pendingMu.Unlock()
		return mgr.pending == nil && len(mgr.wake) == 0
	})
	if !mgr.Stop(5 * time.Second) {
		t.Fatal("Run did not stop")
	}
	duplicate.mu.Lock()
	defer duplicate.mu.Unlock()
	if duplicate.called || duplicate.takeovers != 0 {
		t.Fatal("the queued request repeated Tick's successful takeover")
	}
}

// orderRecorder notes the role-changing commands sent to a site.
type orderRecorder struct {
	*fakeDragonflyConn
	note func(string)
}

func (c *orderRecorder) ReplicaOf(context.Context, string, int32) error {
	c.note("replicaof")
	return nil
}

func (c *orderRecorder) ReplTakeover(context.Context, time.Duration) error {
	c.note("takeover")
	return nil
}

// Rounds 2-3 (TestRereviewTickRepointsPromotedTarget): Tick is repairing a
// replica when the MySQL failover asks for that replica to be promoted.
// The repair, from an observation that is still current, finishes first;
// the promotion follows it. Nothing re-points dc2 once it is master.
func TestDragonflyManager_TickRepairAndPromotionAreOrdered(t *testing.T) {
	mgr, get := newEmergencyManager(t)
	mgr.pollInterval = time.Hour
	repointing, resumeTick := make(chan struct{}), make(chan struct{})
	var once sync.Once
	unblock := func() { once.Do(func() { close(resumeTick) }) }
	t.Cleanup(unblock)
	var mu sync.Mutex
	var order []string
	note := func(s string) { mu.Lock(); order = append(order, s); mu.Unlock() }
	var targetDials atomic.Int32
	mgr.SetConnector(func(ctx context.Context, addr, _ string) (DragonflyConnection, error) {
		if addr == emergencySourceAddr {
			return &fakeDragonflyConn{info: dragonfly.ReplicationInfo{Role: "master", MasterReplOffset: 100}}, nil
		}
		switch targetDials.Add(1) {
		case 1: // Tick's observation
			return &fakeDragonflyConn{info: dragonfly.ReplicationInfo{
				Role: "slave", MasterHost: "wrong-source", MasterPort: 6379, MasterLinkStatus: "down",
			}}, nil
		case 2: // Tick's replica repair
			close(repointing)
			select {
			case <-resumeTick:
			case <-ctx.Done():
				return nil, ctx.Err()
			}
		}
		return &orderRecorder{fakeDragonflyConn: &fakeDragonflyConn{}, note: note}, nil
	})
	go mgr.Run(context.Background())
	t.Cleanup(func() { mgr.Stop(10 * time.Second) })
	<-repointing
	fg := get()
	fg.Status.ActiveSite = "dc2"
	if err := mgr.client.Status().Update(context.Background(), fg); err != nil {
		t.Fatal(err)
	}
	returnsWithin(t, "emergency callback during replica repair", func() {
		emergencyDragonflyCallback(mgr)(context.Background(), "dc2", "dc1")
	})
	unblock()
	waitUntil(t, "the promotion to be recorded", dragonflyPromotedTo(mgr.client, mgr.fgKey, "dc2"))
	if !mgr.Stop(5 * time.Second) {
		t.Fatal("Run did not stop")
	}
	mu.Lock()
	defer mu.Unlock()
	if !slices.Equal(order, []string{"replicaof", "takeover"}) {
		t.Fatalf("commands to dc2 = %v, want the repair strictly before the promotion and nothing after it", order)
	}
}

// Round 4 (TestRereview4StaleTickCannotOverwritePromotionAuthority): Tick
// stalls between its observation and status publication while the MySQL
// failover asks for a promotion. Tick publishes an observation that is
// still current; the promotion runs after it, so its stamp stands and the
// label sweep keeps dc2 behind the active Service.
func TestDragonflyManager_TickPublicationCannotOverwritePromotion(t *testing.T) {
	fg := fgWithDragonflyEnabledAndActive()
	fg.Status.Dragonfly = &v1alpha1.DragonflyStatus{Enabled: true, ActiveSite: "dc1"}
	source := makeDragonflyPod(fg.Name, "dc1", "master", true)
	target := makeDragonflyPod(fg.Name, "dc2", "replica", true)
	cb, key := newDragonflyFakeClient(fg)
	type tickKey struct{}
	entered, resume := make(chan struct{}), make(chan struct{})
	var once sync.Once
	unblock := func() { once.Do(func() { close(resume) }) }
	t.Cleanup(unblock)
	var tickGets atomic.Int32
	c := cb.WithObjects(source, target).WithInterceptorFuncs(interceptor.Funcs{
		Get: func(ctx context.Context, cl client.WithWatch, k client.ObjectKey, obj client.Object, opts ...client.GetOption) error {
			if _, ok := obj.(*v1alpha1.MysqlFailoverGroup); ok && ctx.Value(tickKey{}) != nil && tickGets.Add(1) == 2 {
				close(entered) // observation done; status not yet published
				<-resume
			}
			return cl.Get(ctx, k, obj, opts...)
		},
	}).Build()
	mgr := NewDragonflyManager(c, record.NewFakeRecorder(64), slog.Default(), key, time.Hour)
	conns := &emergencyConnector{}
	conns.queue(emergencySourceAddr,
		&fakeDragonflyConn{info: dragonfly.ReplicationInfo{Role: "master", MasterReplOffset: 100}},
		&fakeDragonflyConn{}, &fakeDragonflyConn{})
	conns.queue(emergencyTargetAddr,
		&fakeDragonflyConn{info: dragonfly.ReplicationInfo{Role: "slave", MasterHost: "lion-dragonfly-dc1.shared-lion.svc.cluster.local", MasterPort: 6379, MasterLinkStatus: "up"}},
		&fakeDragonflyConn{})
	mgr.SetConnector(conns.connect)

	go mgr.Run(context.WithValue(context.Background(), tickKey{}, true))
	t.Cleanup(func() { mgr.Stop(10 * time.Second) })
	<-entered
	mysqlPromoted := getFGByKey(t, c, key)
	mysqlPromoted.Status.ActiveSite = "dc2"
	if err := c.Status().Update(context.Background(), mysqlPromoted); err != nil {
		t.Fatal(err)
	}
	returnsWithin(t, "emergency callback during Tick publication", func() {
		emergencyDragonflyCallback(mgr)(context.Background(), "dc2", "dc1")
	})
	unblock()
	waitUntil(t, "the promotion to be recorded", dragonflyPromotedTo(c, key, "dc2"))
	if !mgr.Stop(5 * time.Second) {
		t.Fatal("Run did not stop")
	}
	current := getFGByKey(t, c, key)
	if current.Status.Dragonfly.ActiveSite != "dc2" {
		t.Errorf("Tick overwrote the dc2 promotion: %+v", current.Status.Dragonfly)
	}
	r := &MysqlFailoverGroupReconciler{Client: c}
	if err := r.syncDragonflyPodLabels(context.Background(), current); err != nil {
		t.Fatal(err)
	}
	var pod corev1.Pod
	if err := c.Get(context.Background(), client.ObjectKeyFromObject(target), &pod); err != nil {
		t.Fatal(err)
	}
	if pod.Labels[labelDragonflyRole] != "master" {
		t.Errorf("label sweep removed the sole master from the active Service: %v", pod.Labels)
	}
}

// --- replacement ---------------------------------------------------------------

// blockingSourceDialer returns fakeDragonflyConns, except that the second
// dial of the old source (the post-takeover CLIENT KILL) signals entered and
// waits for resume.
func blockingSourceDialer(entered, resume chan struct{}) DragonflyConnector {
	var sourceDials atomic.Int32
	return func(ctx context.Context, addr, _ string) (DragonflyConnection, error) {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		if addr == emergencySourceAddr && sourceDials.Add(1) == 2 {
			close(entered)
			select {
			case <-resume:
			case <-ctx.Done():
				return nil, ctx.Err()
			}
		}
		return &fakeDragonflyConn{}, nil
	}
}

// Round 3 (TestRereview3RetiredManagerCannotDeleteNewOwnerMaster): a config
// restart replaces the managers while the old Dragonfly manager is in the
// completion tail of a promotion, and the stop wait runs out. The old tail
// still holds the group's gate, so the replacement can neither Tick nor
// promote until it finishes; once it has, the replacement's promotion
// stands and nothing from the old manager touches it.
func TestDragonflyManager_StopTimeoutKeepsReplacementOut(t *testing.T) {
	fg := fgWithDragonflyEnabledAndActive()
	cb, key := newDragonflyFakeClient(fg)
	c := cb.WithObjects(
		makeDragonflyPod(fg.Name, "dc1", "master", true),
		makeDragonflyPod(fg.Name, "dc2", "replica", true),
	).WithInterceptorFuncs(ctxCheckingFuncs(nil)).Build()
	runner := &TopologyManagerRunner{}
	gate := runner.DragonflyGate(key)

	old := NewDragonflyManager(c, record.NewFakeRecorder(32), slog.Default(), key, time.Hour)
	old.SetGate(gate)
	entered, resume := make(chan struct{}), make(chan struct{})
	var once sync.Once
	unblock := func() { once.Do(func() { close(resume) }) }
	t.Cleanup(unblock)
	old.SetConnector(blockingSourceDialer(entered, resume))
	startActorWithRequest(t, old, "dc2", "dc1")
	<-entered

	mysqlDone := make(chan struct{})
	close(mysqlDone)
	stopManagedTopologyWait(&managedTopology{cancel: func() {}, done: mysqlDone, dragonfly: old}, 20*time.Millisecond)

	fresh := NewDragonflyManager(c, record.NewFakeRecorder(32), slog.Default(), key, time.Hour)
	fresh.SetGate(runner.DragonflyGate(key))
	var freshDials atomic.Int32
	fresh.SetConnector(func(ctx context.Context, _, _ string) (DragonflyConnection, error) {
		freshDials.Add(1)
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		return &fakeDragonflyConn{}, nil
	})
	fresh.Tick(context.Background())
	short, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	if fresh.TryEmergencyPromote(short, "dc1", "dc2") {
		t.Fatal("replacement promoted while the old manager's tail held the gate")
	}
	if n := freshDials.Load(); n != 0 {
		t.Fatalf("replacement dialled Dragonfly %d times while the old tail held the gate", n)
	}

	unblock()
	if !old.Stop(5 * time.Second) {
		t.Fatal("old manager never finished")
	}
	if got := dragonflyActiveSite(getFGByKey(t, c, key)); got != "dc2" {
		t.Fatalf("old promotion not recorded before the gate passed on: activeSite=%q", got)
	}
	// The old tail recreated its demoted source; its StatefulSet brings the
	// pod back before the replacement promotes it.
	if err := c.Create(context.Background(), makeDragonflyPod(fg.Name, "dc1", "replica", true)); err != nil {
		t.Fatal(err)
	}
	if !fresh.TryEmergencyPromote(context.Background(), "dc1", "dc2") {
		t.Fatal("replacement owner's promotion failed")
	}
	if got := dragonflyActiveSite(getFGByKey(t, c, key)); got != "dc1" {
		t.Fatalf("replacement promotion not recorded: activeSite=%q", got)
	}
	var pod corev1.Pod
	if err := c.Get(context.Background(), types.NamespacedName{Namespace: fg.Namespace, Name: makeDragonflyPod(fg.Name, "dc1", "", false).Name}, &pod); err != nil {
		t.Fatalf("replacement owner's dc1 master: %v", err)
	}
	if pod.Labels[labelDragonflyRole] != "master" {
		t.Errorf("replacement did not label dc1 master: %v", pod.Labels)
	}
}

// Round 4 (TestRereview4PlannedPromotionDuringRunnerUnregister): while the
// runner replaces a group's managers it unregisters the old one before
// joining it. A planned failover, a rollout promotion and the label sweep
// reaching the group in that window find the group's gate held by the old
// manager's tail: they requeue or skip instead of mutating alongside it,
// and the planned failover then promotes from the recorded state.
func TestDragonflyWriters_WaitForOldManagerDuringReplacement(t *testing.T) {
	fg := fgWithDragonflyEnabledAndActive()
	cb, key := newDragonflyFakeClient(fg)
	c := cb.WithObjects(
		makeDragonflyPod(fg.Name, "dc1", "master", true),
		makeDragonflyPod(fg.Name, "dc2", "replica", true),
	).WithInterceptorFuncs(ctxCheckingFuncs(nil)).Build()

	old := NewDragonflyManager(c, record.NewFakeRecorder(64), slog.Default(), key, time.Hour)
	entered, resume := make(chan struct{}), make(chan struct{})
	var unblockOnce sync.Once
	unblock := func() { unblockOnce.Do(func() { close(resume) }) }
	t.Cleanup(unblock)
	old.SetConnector(blockingSourceDialer(entered, resume))

	mysqlDone := make(chan struct{})
	var closeDone sync.Once
	finishMySQL := func() { closeDone.Do(func() { close(mysqlDone) }) }
	t.Cleanup(finishMySQL)
	stopped := make(chan struct{})
	runner := &TopologyManagerRunner{client: c, logger: slog.Default(), managers: map[types.NamespacedName]*managedTopology{
		key: {dragonfly: old, done: mysqlDone, cancel: func() { close(stopped) }},
	}}
	old.SetGate(runner.DragonflyGate(key))
	startActorWithRequest(t, old, "dc2", "dc1")
	<-entered

	// Make replacement startup fail cleanly after the stop; only the
	// unregister/join window is under test.
	if err := c.Delete(context.Background(), newTestSecret()); err != nil {
		t.Fatal(err)
	}
	syncDone := make(chan error, 1)
	go func() { syncDone <- runner.sync(context.Background()) }()
	<-stopped
	if runner.dragonflyManager(key) != nil {
		t.Fatal("setup: sync has not unregistered the old manager")
	}

	live := getFGByKey(t, c, key)
	live.Status.ActiveSite = "dc2"
	live.Status.PlannedFailover = &v1alpha1.PlannedFailoverStatus{
		Phase: v1alpha1.PlannedFailoverPhasePromotingDragonfly, SourcePrimary: "dc2", Target: "dc1",
		Dragonfly: &v1alpha1.PlannedFailoverDragonflyStatus{Enabled: true},
	}
	if err := c.Status().Update(context.Background(), live); err != nil {
		t.Fatal(err)
	}
	r := &MysqlFailoverGroupReconciler{Client: c, Recorder: record.NewFakeRecorder(64), Runner: runner, dragonflyGateWait: 20 * time.Millisecond}
	var plannedDials atomic.Int32
	r.dragonflyConnector = func(_ context.Context, addr, _ string) (DragonflyConnection, error) {
		plannedDials.Add(1)
		if addr == emergencySourceAddr {
			return &fakeDragonflyConn{info: dragonfly.ReplicationInfo{Role: "master", MasterReplOffset: 200}}, nil
		}
		return &fakeDragonflyConn{}, nil
	}
	labelsBefore := podLabels(t, c, fg.Namespace)

	requeue, err := r.plannedFailoverPromotingDragonfly(context.Background(), live, key)
	if err != nil || requeue != dragonflyGateRetry {
		t.Fatalf("planned phase with the gate held = (%s, %v), want a %s requeue", requeue, err, dragonflyGateRetry)
	}
	if promoted, deferred := r.promoteDragonflyForRollout(context.Background(), live, "dc1", "dc2"); promoted || !deferred {
		t.Fatalf("rollout promotion with the gate held = (promoted %v, deferred %v), want deferred", promoted, deferred)
	}
	if err := r.syncDragonflyPodLabels(context.Background(), live); err != nil {
		t.Fatalf("label sweep with the gate held: %v", err)
	}
	if n := plannedDials.Load(); n != 0 {
		t.Fatalf("reconciler dialled Dragonfly %d times while the old tail held the gate", n)
	}
	if after := podLabels(t, c, fg.Namespace); !maps.EqualFunc(labelsBefore, after, maps.Equal) {
		t.Fatalf("reconciler relabelled pods while the old tail held the gate:\nbefore %v\nafter  %v", labelsBefore, after)
	}
	if df := getFGByKey(t, c, key).Status.Dragonfly; df != nil && df.LastPromotionTarget != "" {
		t.Fatalf("a promotion was stamped while the old tail was blocked: %+v", df)
	}

	unblock()
	finishMySQL()
	if err := <-syncDone; err != nil {
		t.Fatal(err)
	}
	if got := dragonflyActiveSite(getFGByKey(t, c, key)); got != "dc2" {
		t.Fatalf("old promotion not recorded before the gate passed on: activeSite=%q", got)
	}
	// The old tail recreated its demoted source; its StatefulSet brings the
	// pod back before the planned failover promotes it.
	if err := c.Create(context.Background(), makeDragonflyPod(fg.Name, "dc1", "replica", true)); err != nil {
		t.Fatal(err)
	}
	if _, err := r.plannedFailoverPromotingDragonfly(context.Background(), getFGByKey(t, c, key), key); err != nil {
		t.Fatal(err)
	}
	if got := dragonflyActiveSite(getFGByKey(t, c, key)); got != "dc1" {
		t.Fatalf("planned promotion not recorded: activeSite=%q", got)
	}
	var pod corev1.Pod
	if err := c.Get(context.Background(), types.NamespacedName{Namespace: fg.Namespace, Name: makeDragonflyPod(fg.Name, "dc1", "", false).Name}, &pod); err != nil {
		t.Fatalf("planned dc1 master: %v", err)
	}
	if pod.Labels[labelDragonflyRole] != "master" {
		t.Errorf("planned failover did not label dc1 master: %v", pod.Labels)
	}
}

// The planned failover's Dragonfly phase waits for an emergency promotion
// whose takeover is already on the wire, rather than cancelling it: the
// emergency falls back on its own budget and records dc2 before the
// planned phase dials anything, so the planned phase starts from it.
func TestPlannedFailoverPromotingDragonfly_WaitsForSentEmergencyTakeover(t *testing.T) {
	fg := plannedFailoverFGWithDragonfly("proceed")
	fg.Status.PlannedFailover.Phase = v1alpha1.PlannedFailoverPhasePromotingDragonfly
	fg.Status.PlannedFailover.Dragonfly = &v1alpha1.PlannedFailoverDragonflyStatus{Enabled: true}
	stored := fg.DeepCopy()
	stored.Status.PlannedFailover = nil // the emergency started before the planned failover got here
	scheme := testScheme()
	c := fake.NewClientBuilder().WithScheme(scheme).
		WithStatusSubresource(&v1alpha1.MysqlFailoverGroup{}).
		WithObjects(stored, newTestSecret(), makeDragonflyPod(fg.Name, "iad", "master", true), makeDragonflyPod(fg.Name, "pdx", "replica", true)).
		Build()
	nn := fgNN(fg)
	const pdx, iad = "lion-dragonfly-pdx.shared-lion.svc.cluster.local:6379", "lion-dragonfly-iad.shared-lion.svc.cluster.local:6379"

	var mu sync.Mutex
	var order []string
	note := func(s string) { mu.Lock(); order = append(order, s); mu.Unlock() }
	runner := &TopologyManagerRunner{}
	r := &MysqlFailoverGroupReconciler{Client: c, Scheme: scheme, Recorder: record.NewFakeRecorder(64), Runner: runner}
	var plannedSawPromotion atomic.Bool
	r.dragonflyConnector = func(_ context.Context, addr, _ string) (DragonflyConnection, error) {
		note("planned dial")
		if df := getFGByKey(t, c, nn).Status.Dragonfly; df != nil && df.LastPromotionTarget == "pdx" {
			plannedSawPromotion.Store(true)
		}
		if addr == pdx {
			return &fakeDragonflyConn{info: dragonfly.ReplicationInfo{Role: "master", MasterReplOffset: 200}}, nil
		}
		return &fakeDragonflyConn{info: dragonfly.ReplicationInfo{Role: "master"}}, nil
	}

	mgr := NewDragonflyManager(c, record.NewFakeRecorder(64), slog.Default(), nn, time.Hour)
	mgr.SetGate(runner.DragonflyGate(nn))
	mgr.emergencyWaits = func(time.Duration) (time.Duration, time.Duration) { return 200 * time.Millisecond, 2 * time.Second }
	hung := &blockingTakeoverConn{fakeDragonflyConn: &fakeDragonflyConn{}, started: make(chan struct{})}
	hung.onReturn = func(error) { note("emergency takeover returned") }
	fallback := &noOneRecorder{fakeDragonflyConn: &fakeDragonflyConn{}}
	conns := &emergencyConnector{}
	conns.queue(pdx, hung, fallback)
	conns.queue(iad, &fakeDragonflyConn{}, &fakeDragonflyConn{})
	mgr.SetConnector(conns.connect)
	startActorWithRequest(t, mgr, "pdx", "iad")
	<-hung.started

	live := getFGByKey(t, c, nn)
	live.Status.PlannedFailover = fg.Status.PlannedFailover.DeepCopy()
	if err := c.Status().Update(context.Background(), live); err != nil {
		t.Fatalf("stamp planned failover: %v", err)
	}
	if _, err := r.plannedFailoverPromotingDragonfly(context.Background(), getFGByKey(t, c, nn), nn); err != nil {
		t.Fatalf("plannedFailoverPromotingDragonfly: %v", err)
	}

	mu.Lock()
	got := append([]string(nil), order...)
	mu.Unlock()
	returned, dialled := slices.Index(got, "emergency takeover returned"), slices.Index(got, "planned dial")
	if returned < 0 || dialled < 0 || returned > dialled {
		t.Fatalf("order = %v, want the emergency takeover finished before the planned phase dialled", got)
	}
	if !plannedSawPromotion.Load() {
		t.Error("the planned phase started before the emergency promotion was recorded")
	}
	fallback.mu.Lock()
	defer fallback.mu.Unlock()
	if !fallback.called || fallback.ctxErr != nil {
		t.Errorf("emergency fallback called=%v ctxErr=%v, want it run on its own live budget", fallback.called, fallback.ctxErr)
	}
}

// --- label sweep ---------------------------------------------------------------

// The label sweep skips its pass while another writer holds the group's
// gate: a promotion that has relabelled the pods but not yet stamped
// status must not be undone from the status it is about to replace.
func TestSyncDragonflyPodLabels_SkipsWhileGateHeld(t *testing.T) {
	fg := fgWithDragonflyEnabledAndActive()
	fg.Status.Dragonfly = &v1alpha1.DragonflyStatus{Enabled: true, ActiveSite: "dc1"}
	cb, key := newDragonflyFakeClient(fg)
	c := cb.WithObjects(
		makeDragonflyPod(fg.Name, "dc1", "replica", true),
		makeDragonflyPod(fg.Name, "dc2", "master", true),
	).Build()
	runner := &TopologyManagerRunner{}
	r := &MysqlFailoverGroupReconciler{Client: c, Runner: runner}
	before := podLabels(t, c, fg.Namespace)

	release, ok := runner.DragonflyGate(key).tryAcquire()
	if !ok {
		t.Fatal("gate busy")
	}
	if err := r.syncDragonflyPodLabels(context.Background(), getFGByKey(t, c, key)); err != nil {
		t.Fatal(err)
	}
	if after := podLabels(t, c, fg.Namespace); !maps.EqualFunc(before, after, maps.Equal) {
		t.Fatalf("label sweep relabelled pods while the gate was held:\nbefore %v\nafter  %v", before, after)
	}
	release()

	// With the gate free the same pass converges on status (dc1), which
	// shows the skipped pass would have relabelled.
	if err := r.syncDragonflyPodLabels(context.Background(), getFGByKey(t, c, key)); err != nil {
		t.Fatal(err)
	}
	if eps := writableEndpoints(t, c, fg); !slices.Equal(eps, []string{"dc1"}) {
		t.Fatalf("label sweep with the gate free left endpoints %v, want [dc1]", eps)
	}
}

// The label sweep reads the group after taking the gate, so a promotion
// stamped after the reconcile read its copy is honoured.
func TestSyncDragonflyPodLabels_ReadsStatusAfterTakingGate(t *testing.T) {
	fg := fgWithDragonflyEnabledAndActive()
	fg.Status.Dragonfly = &v1alpha1.DragonflyStatus{Enabled: true, ActiveSite: "dc1"}
	cb, key := newDragonflyFakeClient(fg)
	c := cb.WithObjects(
		makeDragonflyPod(fg.Name, "dc1", "replica", true),
		makeDragonflyPod(fg.Name, "dc2", "master", true),
	).Build()
	stale := getFGByKey(t, c, key) // the reconcile's copy
	if err := stampDragonflyPromotionStatus(context.Background(), c, c, key, "dc2", metav1.Now(), true); err != nil {
		t.Fatal(err)
	}
	r := &MysqlFailoverGroupReconciler{Client: c, Runner: &TopologyManagerRunner{}}
	if err := r.syncDragonflyPodLabels(context.Background(), stale); err != nil {
		t.Fatal(err)
	}
	if eps := writableEndpoints(t, c, fg); !slices.Equal(eps, []string{"dc2"}) {
		t.Fatalf("label sweep acted on its stale copy: endpoints %v, want [dc2]", eps)
	}
}
