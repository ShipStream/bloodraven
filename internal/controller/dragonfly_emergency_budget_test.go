package controller

import (
	"context"
	"errors"
	"log/slog"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/client-go/tools/record"
	"sigs.k8s.io/controller-runtime/pkg/client"

	v1alpha1 "github.com/shipstream/bloodraven/api/v1alpha1"
	"github.com/shipstream/bloodraven/internal/dragonfly"
)

const (
	emergencyTargetAddr = "lion-dragonfly-dc2.shared-lion.svc.cluster.local:6379"
	emergencySourceAddr = "lion-dragonfly-dc1.shared-lion.svc.cluster.local:6379"
)

// blockingTakeoverConn models a replica whose REPLTAKEOVER hangs: its master
// link looks alive but the master never answers the TAKEOVER handshake. The
// call returns only when the caller's context ends.
type blockingTakeoverConn struct {
	*fakeDragonflyConn
	mu              sync.Mutex
	takeoverTimeout time.Duration
	takeoverCalls   int
	// started, when set, is closed as the first takeover begins.
	started chan struct{}
	// onReturn, when set, is called with the error each takeover returns.
	onReturn func(error)
}

func (c *blockingTakeoverConn) ReplTakeover(ctx context.Context, timeout time.Duration) error {
	c.mu.Lock()
	c.takeoverCalls++
	c.takeoverTimeout = timeout
	if c.started != nil && c.takeoverCalls == 1 {
		close(c.started)
	}
	c.mu.Unlock()
	<-ctx.Done()
	if c.onReturn != nil {
		c.onReturn(ctx.Err())
	}
	return ctx.Err()
}

// noOneRecorder records the context REPLICAOF NO ONE ran under and, like a
// real client, fails if that context is already done.
type noOneRecorder struct {
	*fakeDragonflyConn
	mu        sync.Mutex
	called    bool
	ctxErr    error
	remaining time.Duration
	takeovers int
}

func (c *noOneRecorder) ReplicaOfNoOne(ctx context.Context) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.called = true
	c.ctxErr = ctx.Err()
	if d, ok := ctx.Deadline(); ok {
		c.remaining = time.Until(d)
	}
	return ctx.Err()
}

func (c *noOneRecorder) ReplTakeover(context.Context, time.Duration) error {
	c.mu.Lock()
	c.takeovers++
	c.mu.Unlock()
	return nil
}

// emergencyConnector hands out scripted connections per address and, like
// the real dialer, refuses an already-expired context.
type emergencyConnector struct {
	mu    sync.Mutex
	conns map[string][]DragonflyConnection
	dials map[string]int
}

func (e *emergencyConnector) queue(addr string, conns ...DragonflyConnection) {
	e.mu.Lock()
	defer e.mu.Unlock()
	if e.conns == nil {
		e.conns = map[string][]DragonflyConnection{}
	}
	e.conns[addr] = append(e.conns[addr], conns...)
}

func (e *emergencyConnector) connect(ctx context.Context, addr, _ string) (DragonflyConnection, error) {
	e.mu.Lock()
	defer e.mu.Unlock()
	if e.dials == nil {
		e.dials = map[string]int{}
	}
	e.dials[addr]++
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	q := e.conns[addr]
	if len(q) == 0 {
		return nil, context.DeadlineExceeded
	}
	e.conns[addr] = q[1:]
	return q[0], nil
}

// shrinkEmergencyWaits keeps the tests fast: the production waits are
// whole seconds because Dragonfly's takeover budget is.
func shrinkEmergencyWaits(m *DragonflyManager) {
	m.emergencyWaits = func(time.Duration) (takeover, fallback time.Duration) {
		return 50 * time.Millisecond, 2 * time.Second
	}
	m.emergencySettle = func(time.Duration) time.Duration { return 10 * time.Millisecond }
}

func newEmergencyManager(t *testing.T) (*DragonflyManager, func() *v1alpha1.MysqlFailoverGroup) {
	t.Helper()
	fg := fgWithDragonflyEnabledAndActive()
	cb, key := newDragonflyFakeClient(fg)
	c := cb.Build()
	mgr := NewDragonflyManager(c, record.NewFakeRecorder(32), slog.Default(), key, 50*time.Millisecond)
	shrinkEmergencyWaits(mgr)
	get := func() *v1alpha1.MysqlFailoverGroup {
		var got v1alpha1.MysqlFailoverGroup
		if err := c.Get(context.Background(), key, &got); err != nil {
			t.Fatalf("get fg: %v", err)
		}
		return &got
	}
	return mgr, get
}

// A takeover that hangs must not use up the time the REPLICAOF NO ONE
// fallback and the status stamp need: the fallback runs on a live context
// and the target is promoted.
func TestTryEmergencyPromote_HungTakeoverLeavesFallbackALiveBudget(t *testing.T) {
	mgr, get := newEmergencyManager(t)
	hung := &blockingTakeoverConn{fakeDragonflyConn: &fakeDragonflyConn{}}
	fallback := &noOneRecorder{fakeDragonflyConn: &fakeDragonflyConn{}}
	conns := &emergencyConnector{}
	conns.queue(emergencyTargetAddr, hung, fallback)
	// The old master still answers a dial and PING; only TAKEOVER hangs.
	conns.queue(emergencySourceAddr, &fakeDragonflyConn{}, &fakeDragonflyConn{})
	mgr.SetConnector(conns.connect)

	if ok := mgr.TryEmergencyPromote(context.Background(), "dc2", "dc1"); !ok {
		t.Fatal("TryEmergencyPromote() = false, want the REPLICAOF NO ONE fallback to promote")
	}
	if !fallback.called {
		t.Fatal("REPLICAOF NO ONE was never sent")
	}
	if fallback.ctxErr != nil || fallback.remaining <= 0 {
		t.Fatalf("REPLICAOF NO ONE ran on a dead context (err=%v remaining=%s)", fallback.ctxErr, fallback.remaining)
	}
	if hung.takeoverTimeout <= 0 || hung.takeoverTimeout > defaultEmergencyTakeoverTimeout {
		t.Errorf("emergency REPLTAKEOVER budget = %s, want at most %s", hung.takeoverTimeout, defaultEmergencyTakeoverTimeout)
	}
	if got := get().Status.Dragonfly; got == nil || got.ActiveSite != "dc2" {
		t.Errorf("status.dragonfly = %+v, want activeSite dc2 stamped", got)
	}
}

// TAKEOVER is coordinated by the master, so it cannot succeed against an
// unreachable one, and while it waits the replica holds the lock REPLICAOF
// NO ONE needs. With the old master gone, promote directly.
func TestTryEmergencyPromote_UnreachableOldMasterSkipsTakeover(t *testing.T) {
	mgr, get := newEmergencyManager(t)
	target := &noOneRecorder{fakeDragonflyConn: &fakeDragonflyConn{}}
	conns := &emergencyConnector{}
	conns.queue(emergencyTargetAddr, target)
	mgr.SetConnector(conns.connect)

	if ok := mgr.TryEmergencyPromote(context.Background(), "dc2", "dc1"); !ok {
		t.Fatal("TryEmergencyPromote() = false, want promotion via REPLICAOF NO ONE")
	}
	if target.takeovers != 0 {
		t.Errorf("REPLTAKEOVER sent %d times against an unreachable old master", target.takeovers)
	}
	if !target.called || target.ctxErr != nil {
		t.Fatalf("REPLICAOF NO ONE called=%v ctxErr=%v, want a live call", target.called, target.ctxErr)
	}
	if got := get().Status.Dragonfly; got == nil || got.ActiveSite != "dc2" {
		t.Errorf("status.dragonfly = %+v, want activeSite dc2 stamped", got)
	}
}

// A rollout promotion is sync-gated and keeps the planned budget
// (maxSyncWait/2); only the emergency path uses the short takeover budget.
func TestTryRolloutPromote_KeepsPlannedTakeoverBudget(t *testing.T) {
	mgr, _ := newEmergencyManager(t)
	hung := &blockingTakeoverConn{fakeDragonflyConn: &fakeDragonflyConn{}}
	fallback := &noOneRecorder{fakeDragonflyConn: &fakeDragonflyConn{}}
	conns := &emergencyConnector{}
	conns.queue(emergencyTargetAddr, hung, fallback)
	conns.queue(emergencySourceAddr, &fakeDragonflyConn{}, &fakeDragonflyConn{})
	mgr.SetConnector(conns.connect)

	mgr.TryRolloutPromote(context.Background(), "dc2", "dc1")

	if want := defaultDragonflySyncWait / 2; hung.takeoverTimeout != want {
		t.Errorf("rollout REPLTAKEOVER budget = %s, want %s", hung.takeoverTimeout, want)
	}
}

// The emergency waits follow Dragonfly's whole-second budget: the takeover
// call gets the budget plus slack, and the fallback outlasts the replica's
// own hold of its REPLICAOF lock (budget + 10s).
func TestDefaultEmergencyWaits(t *testing.T) {
	takeover, fallback := defaultEmergencyWaits(defaultEmergencyTakeoverTimeout)
	if takeover <= defaultEmergencyTakeoverTimeout || takeover >= fallback {
		t.Errorf("takeover wait %s, fallback wait %s: want budget < takeover < fallback", takeover, fallback)
	}
	if hold := dragonfly.ReplTakeoverServerHold(defaultEmergencyTakeoverTimeout); fallback <= hold {
		t.Errorf("fallback wait %s does not outlast the server's takeover hold %s", fallback, hold)
	}
}

// The settle time outlasts the server's own hold of a takeover, so an
// unconfirmed outcome is observed only once the server can no longer be
// acting on the command.
func TestDefaultEmergencySettle(t *testing.T) {
	if got, hold := defaultEmergencySettle(defaultEmergencyTakeoverTimeout), dragonfly.ReplTakeoverServerHold(defaultEmergencyTakeoverTimeout); got <= hold {
		t.Errorf("settle after a takeover = %s, want more than the server hold %s", got, hold)
	}
	if got := defaultEmergencySettle(0); got <= 0 {
		t.Errorf("settle without a takeover = %s, want > 0", got)
	}
}

// Cancelling the caller while it waits for the gate sends nothing.
func TestTryEmergencyPromote_CancelWhileWaitingForGateSendsNothing(t *testing.T) {
	mgr, get := newEmergencyManager(t)
	conns := &emergencyConnector{}
	mgr.SetConnector(conns.connect)
	release, ok := mgr.gate.tryAcquire()
	if !ok {
		t.Fatal("gate busy")
	}
	defer release()
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	if mgr.TryEmergencyPromote(ctx, "dc2", "dc1") {
		t.Fatal("TryEmergencyPromote() = true without the gate")
	}
	conns.mu.Lock()
	dials := len(conns.dials)
	conns.mu.Unlock()
	if dials != 0 {
		t.Errorf("dialled Dragonfly without the gate: %v", conns.dials)
	}
	if df := get().Status.Dragonfly; df != nil && df.LastPromotionTarget != "" {
		t.Errorf("stamped a promotion without the gate: %+v", df)
	}
}

// Once REPLTAKEOVER is on the wire, cancelling the caller neither cuts the
// takeover short nor the fallback: each runs to its own budget and the
// outcome is recorded (the original expired-context fallback must not come
// back).
func TestTryEmergencyPromote_CancelAfterTakeoverSentKeepsBudgets(t *testing.T) {
	mgr, get := newEmergencyManager(t)
	hung := &blockingTakeoverConn{fakeDragonflyConn: &fakeDragonflyConn{}, started: make(chan struct{})}
	fallback := &noOneRecorder{fakeDragonflyConn: &fakeDragonflyConn{}}
	conns := &emergencyConnector{}
	conns.queue(emergencyTargetAddr, hung, fallback)
	conns.queue(emergencySourceAddr, &fakeDragonflyConn{}, &fakeDragonflyConn{})
	mgr.SetConnector(conns.connect)
	mgr.emergencyWaits = func(time.Duration) (time.Duration, time.Duration) { return 200 * time.Millisecond, 2 * time.Second }
	var takeoverErr error
	hungDone := make(chan struct{})
	hung.onReturn = func(err error) { takeoverErr = err; close(hungDone) }

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan bool, 1)
	go func() { done <- mgr.TryEmergencyPromote(ctx, "dc2", "dc1") }()
	<-hung.started
	cancel()

	select {
	case ok := <-done:
		if !ok {
			t.Fatal("TryEmergencyPromote() = false, want the fallback to promote despite the cancel")
		}
	case <-time.After(5 * time.Second):
		t.Fatal("TryEmergencyPromote did not finish")
	}
	<-hungDone
	if !errors.Is(takeoverErr, context.DeadlineExceeded) {
		t.Errorf("takeover ended with %v, want its own budget (deadline exceeded), not the caller's cancel", takeoverErr)
	}
	if !fallback.called || fallback.ctxErr != nil || fallback.remaining <= 0 {
		t.Errorf("REPLICAOF NO ONE called=%v ctxErr=%v remaining=%s, want a live call", fallback.called, fallback.ctxErr, fallback.remaining)
	}
	if df := get().Status.Dragonfly; df == nil || df.ActiveSite != "dc2" {
		t.Errorf("status.dragonfly = %+v, want activeSite dc2 stamped", df)
	}
}

// --- unconfirmed outcomes (round-4 review, Opus R2) --------------------------

// dialStep is one scripted dial result.
type dialStep struct {
	conn DragonflyConnection
	err  error
}

// scriptedConnector hands out dial results in order per address and
// records when each dial happened. An unscripted dial fails.
type scriptedConnector struct {
	mu    sync.Mutex
	steps map[string][]dialStep
	at    map[string][]time.Time
}

func (s *scriptedConnector) script(addr string, steps ...dialStep) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.steps == nil {
		s.steps = map[string][]dialStep{}
	}
	s.steps[addr] = append(s.steps[addr], steps...)
}

func (s *scriptedConnector) dialTimes(addr string) []time.Time {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]time.Time(nil), s.at[addr]...)
}

func (s *scriptedConnector) connect(ctx context.Context, addr, _ string) (DragonflyConnection, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.at == nil {
		s.at = map[string][]time.Time{}
	}
	s.at[addr] = append(s.at[addr], time.Now())
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	q := s.steps[addr]
	if len(q) == 0 {
		return nil, errors.New("unscripted dial to " + addr)
	}
	s.steps[addr] = q[1:]
	return q[0].conn, q[0].err
}

// writableEndpoints lists the sites whose Dragonfly pods the active
// Service selects (role=master and traffic enabled).
func writableEndpoints(t *testing.T, c client.Client, fg *v1alpha1.MysqlFailoverGroup) []string {
	t.Helper()
	var pods corev1.PodList
	if err := c.List(context.Background(), &pods, client.InNamespace(fg.Namespace)); err != nil {
		t.Fatal(err)
	}
	var out []string
	for _, p := range pods.Items {
		if p.Labels[labelDragonflyRole] == "master" && p.Labels[labelDragonflyTraffic] == dragonflyTrafficEnabled {
			out = append(out, p.Labels[labelSite])
		}
	}
	slices.Sort(out)
	return out
}

func newEmergencyManagerWithPods(t *testing.T) (*DragonflyManager, client.Client, *v1alpha1.MysqlFailoverGroup) {
	t.Helper()
	fg := fgWithDragonflyEnabledAndActive()
	fg.Status.Dragonfly = &v1alpha1.DragonflyStatus{Enabled: true, ActiveSite: "dc1"}
	cb, key := newDragonflyFakeClient(fg)
	c := cb.WithObjects(
		makeDragonflyPod(fg.Name, "dc1", "master", true),
		makeDragonflyPod(fg.Name, "dc2", "replica", true),
	).Build()
	mgr := NewDragonflyManager(c, record.NewFakeRecorder(64), slog.Default(), key, time.Hour)
	shrinkEmergencyWaits(mgr)
	return mgr, c, fg
}

// A REPLTAKEOVER whose reply was lost may still have completed on the
// server. When the fallback cannot even be dialled, the outcome is settled
// by observing the target's role once the server can no longer be acting
// on the takeover: a master is recorded as a completed promotion.
func TestTryEmergencyPromote_UnconfirmedTakeoverObservedAsMasterIsRecorded(t *testing.T) {
	mgr, c, fg := newEmergencyManagerWithPods(t)
	const settle = 150 * time.Millisecond
	mgr.emergencySettle = func(time.Duration) time.Duration { return settle }
	conns := &scriptedConnector{}
	conns.script(emergencyTargetAddr,
		dialStep{conn: &fakeDragonflyConn{replTakeoverErr: errors.New("i/o timeout")}},
		dialStep{err: errors.New("connection refused")},
		dialStep{conn: &fakeDragonflyConn{info: dragonfly.ReplicationInfo{Role: "master", MasterReplOffset: 100}}},
	)
	conns.script(emergencySourceAddr, dialStep{conn: &fakeDragonflyConn{}}, dialStep{conn: &fakeDragonflyConn{}})
	mgr.SetConnector(conns.connect)
	rec := mgr.recorder.(*record.FakeRecorder)

	if !mgr.TryEmergencyPromote(context.Background(), "dc2", "dc1") {
		t.Fatal("TryEmergencyPromote() = false, want the observed master recorded as promoted")
	}
	dials := conns.dialTimes(emergencyTargetAddr)
	if len(dials) != 3 {
		t.Fatalf("target dials = %d, want takeover, failed fallback re-dial, observation", len(dials))
	}
	if gap := dials[2].Sub(dials[0]); gap < settle {
		t.Errorf("role observed %s after the takeover was sent, want at least the %s settle time", gap, settle)
	}
	var got v1alpha1.MysqlFailoverGroup
	if err := c.Get(context.Background(), mgr.fgKey, &got); err != nil {
		t.Fatal(err)
	}
	if got.Status.Dragonfly.ActiveSite != "dc2" {
		t.Errorf("status.dragonfly.activeSite = %q, want dc2", got.Status.Dragonfly.ActiveSite)
	}
	if eps := writableEndpoints(t, c, fg); !slices.Equal(eps, []string{"dc2"}) {
		t.Errorf("active-Service endpoints = %v, want [dc2]", eps)
	}
	sawLost := false
	for _, ev := range drainEventsCh(rec.Events, 16, 50*time.Millisecond) {
		sawLost = sawLost || strings.Contains(ev, ReasonDragonflySessionsLost)
	}
	if !sawLost {
		t.Error("no SessionsLost event for a promotion whose sessions may not have survived")
	}
}

// When REPLICAOF NO ONE fails and the target is then observed as a replica,
// the promotion failed: the old source gets its traffic label back so the
// active Service is not left empty, and status keeps the old site.
func TestTryEmergencyPromote_UnconfirmedFallbackObservedAsReplicaRestoresSource(t *testing.T) {
	mgr, c, fg := newEmergencyManagerWithPods(t)
	conns := &scriptedConnector{}
	conns.script(emergencyTargetAddr,
		dialStep{conn: &fakeDragonflyConn{replTakeoverErr: errors.New("takeover refused")}},
		dialStep{conn: &fakeDragonflyConn{replicaOfNoOneErr: errors.New("i/o timeout")}},
		dialStep{conn: &fakeDragonflyConn{info: dragonfly.ReplicationInfo{Role: "slave", MasterHost: "lion-dragonfly-dc1.shared-lion.svc.cluster.local", MasterPort: 6379, MasterLinkStatus: "up"}}},
	)
	conns.script(emergencySourceAddr, dialStep{conn: &fakeDragonflyConn{}})
	mgr.SetConnector(conns.connect)

	if mgr.TryEmergencyPromote(context.Background(), "dc2", "dc1") {
		t.Fatal("TryEmergencyPromote() = true for a target observed as a replica")
	}
	var got v1alpha1.MysqlFailoverGroup
	if err := c.Get(context.Background(), mgr.fgKey, &got); err != nil {
		t.Fatal(err)
	}
	if got.Status.Dragonfly.ActiveSite != "dc1" {
		t.Errorf("status.dragonfly.activeSite = %q, want dc1 kept", got.Status.Dragonfly.ActiveSite)
	}
	if got.Status.Dragonfly.LastPromotionTarget != "dc2" {
		t.Errorf("failed promotion not stamped: %+v", got.Status.Dragonfly)
	}
	if eps := writableEndpoints(t, c, fg); !slices.Equal(eps, []string{"dc1"}) {
		t.Errorf("active-Service endpoints = %v, want the old source dc1 restored", eps)
	}
}
