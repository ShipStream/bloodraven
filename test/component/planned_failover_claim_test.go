package component

// Planned-failover durable claim (#235) component tests.
//
// This file holds the per-interleaving tests of spec §14 (TestPF_I01…I28
// and the further pinned rows). They drive the real TopologyManagers, the
// production PlannedFailoverTopology facade over a test ManagerRegistry,
// the real PlannedFailoverEngine and an in-memory PlannedFailoverStore with
// scripted faults; rows 1–3 and 9 also run the production UpdateController
// with its old manager retired while blocked inside a statement. The rows
// whose mechanism is the Dragonfly split (6's Dragonfly part, 16, 27, 28)
// are in internal/controller/planned_failover_rows_test.go; the rows §14
// pins with internal/controller unit tests (14, 15, 20d/e, 22, 25, 26 and
// several of the further rows) are pinned there.
//
// The first tests below are the mock fidelity the rows rest on: a pinned
// session that cannot follow a replaced pod, and calls that block inside a
// statement.

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"

	v1alpha1 "github.com/shipstream/bloodraven/api/v1alpha1"
	"github.com/shipstream/bloodraven/internal/clock"
	"github.com/shipstream/bloodraven/internal/controller"
	"github.com/shipstream/bloodraven/internal/dst"
	"github.com/shipstream/bloodraven/internal/mysql"
	"github.com/shipstream/bloodraven/internal/platform"
	"github.com/shipstream/bloodraven/internal/state"
)

const (
	pfSourceUUID = "3e11fa47-71ca-11e1-9e33-c80aa9429562"
	pfTargetUUID = "4f22ab58-82db-22f2-af44-d91bb0530673"
	pfFenced     = pfSourceUUID + ":1-100"
)

// TestPinnedMockSession_FailsAfterReplacementBetweenProofAndWrite is the
// component form of the DST fidelity test of the same name: pin, prove,
// replace the pod, and the write fails without reaching either server.
func TestPinnedMockSession_FailsAfterReplacementBetweenProofAndWrite(t *testing.T) {
	ctx := context.Background()
	target := &mockMySQL{readOnly: true, superReadOnly: true, gtidExecuted: pfFenced, serverUUID: pfTargetUUID}
	sess, err := target.PinSession(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if snap, err := sess.ReadGTIDSnapshot(ctx); err != nil || snap.ServerUUID != pfTargetUUID || snap.GtidExecuted != pfFenced {
		t.Fatalf("proof: %+v %v", snap, err)
	}

	regressed := pfSourceUUID + ":1-60"
	target.replacePod(podReplacement{serverUUID: "5a33bc69-93ec-33a3-b055-ea2cc1641784", gtidExecuted: &regressed})

	if err := sess.SetSuperReadOnly(ctx, false); !errors.Is(err, errMockSessionLost) {
		t.Fatalf("SET super_read_only after replacement: %v", err)
	}
	if err := sess.SetReadOnly(ctx, false); !errors.Is(err, errMockSessionLost) {
		t.Fatalf("SET read_only after replacement: %v", err)
	}
	if target.writableGrants() != 0 || !target.isReadOnly() || target.isSuperReadOnly() {
		t.Fatalf("the new pod was changed or did not boot read_only=ON super_read_only=OFF (grants %d)", target.writableGrants())
	}
	// The pool reaches the new pod; a new pin is on it.
	if got, _ := target.GetGtidExecuted(ctx); got != regressed {
		t.Fatalf("pooled read %q, want the new pod's %q", got, regressed)
	}
	fresh, _ := target.PinSession(ctx)
	if snap, err := fresh.ReadGTIDSnapshot(ctx); err != nil || snap.ServerUUID == pfTargetUUID {
		t.Fatalf("fresh pin: %+v %v", snap, err)
	}
	if err := fresh.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := fresh.CheckReadOnly(ctx); !errors.Is(err, errMockSessionClosed) {
		t.Fatalf("after Close: %v", err)
	}
	// A restart is a new incarnation too.
	again, _ := target.PinSession(ctx)
	target.respawn(true)
	if err := again.StopReplica(ctx); !errors.Is(err, errMockSessionLost) {
		t.Fatalf("after respawn: %v", err)
	}
}

// TestMockBlockOn_HoldsInsideStatement: a blocked call is inside the
// statement while other actors act; a replacement before apply keeps it from
// applying, after apply it landed and its reply is lost.
func TestMockBlockOn_HoldsInsideStatement(t *testing.T) {
	ctx := context.Background()
	t.Run("beforeApply", func(t *testing.T) {
		m := &mockMySQL{readOnly: true, superReadOnly: true}
		sess, _ := m.PinSession(ctx)
		b := m.blockOn(t, "SetSuperReadOnly", false)
		done := make(chan error, 1)
		go func() { done <- sess.SetSuperReadOnly(ctx, false) }()
		<-b.Entered()
		// The mock is not locked while the call is held.
		if !m.isSuperReadOnly() {
			t.Fatal("applied before release")
		}
		m.replacePod(podReplacement{})
		b.Release()
		if err := <-done; !errors.Is(err, errMockSessionLost) {
			t.Fatalf("err %v, want errMockSessionLost", err)
		}
		if m.writableGrants() != 0 {
			t.Fatal("a statement lost before apply applied")
		}
	})
	t.Run("afterApply", func(t *testing.T) {
		m := &mockMySQL{readOnly: true, superReadOnly: true}
		sess, _ := m.PinSession(ctx)
		b := m.blockOn(t, "SetReadOnly", true)
		done := make(chan error, 1)
		go func() { done <- sess.SetReadOnly(ctx, false) }()
		<-b.Entered()
		if m.isReadOnly() {
			t.Fatal("not applied while held after apply")
		}
		m.replacePod(podReplacement{})
		b.Release()
		if err := <-done; !errors.Is(err, errMockSessionLost) {
			t.Fatalf("err %v, want errMockSessionLost (applied, reply lost)", err)
		}
	})
	t.Run("failNextAndCounts", func(t *testing.T) {
		m := &mockMySQL{clearChannelOnReset: true, replicaStatus: &mysql.ReplicaStatus{IORunning: true, SQLRunning: true}}
		m.failNextCalls("ResetReplicaAll", 1)
		if err := m.ResetReplicaAll(ctx); !errors.Is(err, errMockStatementFailed) {
			t.Fatalf("err %v", err)
		}
		if err := m.ResetReplicaAll(ctx); err != nil {
			t.Fatal(err)
		}
		if rs, _ := m.ShowReplicaStatus(ctx); rs != nil || m.callCount("ResetReplicaAll") != 2 {
			t.Fatalf("channel %+v after RESET REPLICA ALL, calls %d", rs, m.callCount("ResetReplicaAll"))
		}
		if got := gtidWithExtras(pfFenced, pfTargetUUID, 1, 3); got != pfFenced+","+pfTargetUUID+":1-3" {
			t.Fatalf("gtidWithExtras %q", got)
		}
		if snap, err := withServerUUID(m, pfTargetUUID).ReadGTIDSnapshot(ctx); err != nil || snap.ServerUUID != pfTargetUUID {
			t.Fatalf("pooled snapshot %+v %v", snap, err)
		}
	})
}

// TestHandoff_PodReplacedBetweenProofAndWriteNeverWrites runs the production
// maintenance handoff with the candidate's pod replaced while its first
// write after the GTID proofs (SET super_read_only=OFF on the pinned
// session) is in flight: the promotion stops, nothing reaches the new pod,
// and the source gets its writes back.
func TestHandoff_PodReplacedBetweenProofAndWriteNeverWrites(t *testing.T) {
	ctx := context.Background()
	source := &mockMySQL{gtidExecuted: pfFenced, serverUUID: pfSourceUUID}
	candidate := &mockMySQL{
		readOnly: true, superReadOnly: true, gtidExecuted: pfFenced, serverUUID: pfTargetUUID,
		replicaStatus: &mysql.ReplicaStatus{IORunning: true, SQLRunning: true, SourceHost: "source"},
	}
	held := candidate.blockOn(t, "SetSuperReadOnly", false)

	fc := controller.NewFailoverController(slog.New(slog.DiscardHandler))
	done := make(chan error, 1)
	go func() {
		_, err := fc.ExecuteHandoff(ctx, candidate, source, "candidate", controller.HandoffOptions{
			PreFenceCatchUp: 2 * time.Second, CatchUp: 2 * time.Second, PollInterval: time.Millisecond,
		})
		done <- err
	}()
	select {
	case <-held.Entered():
	case err := <-done:
		t.Fatalf("handoff ended before the candidate write: %v", err)
	case <-time.After(10 * time.Second):
		t.Fatal("the handoff never reached the candidate write")
	}
	regressed := pfSourceUUID + ":1-60"
	candidate.replacePod(podReplacement{serverUUID: "5a33bc69-93ec-33a3-b055-ea2cc1641784", gtidExecuted: &regressed})
	held.Release()

	if err := <-done; err == nil {
		t.Fatal("handoff succeeded onto a replaced pod")
	}
	if candidate.writableGrants() != 0 || !candidate.isReadOnly() {
		t.Fatalf("the replaced pod was made writable (grants %d)", candidate.writableGrants())
	}
	if source.isReadOnly() || source.isSuperReadOnly() {
		t.Fatal("source left fenced although nothing was written to the candidate")
	}
}

// ---------------------------------------------------------------------------
// pfHarness (§15.2): real TopologyManagers, the production facade over a test
// ManagerRegistry, the real engine, the DST's in-memory store with scripted
// faults, the production out-of-band recorder over that store, mockMySQL
// pinned sessions and a FakeClock.
// ---------------------------------------------------------------------------

const (
	pfUUIDA = "aaaaaaaa-0000-0000-0000-000000000001"
	pfUUIDB = "bbbbbbbb-0000-0000-0000-000000000002"
	pfUUIDC = "cccccccc-0000-0000-0000-000000000003"
	pfF     = pfUUIDA + ":1-100"
)

// pfRegistry is the test ManagerRegistry: one group, one process.
type pfRegistry struct {
	mu       sync.RWMutex
	nn       types.NamespacedName
	current  *controller.TopologyManager
	retiring []*controller.TopologyManager
	slot     *controller.PlannedClaimSlot
	ctx      context.Context
	cancel   context.CancelFunc
}

func newPFRegistry(nn types.NamespacedName) *pfRegistry {
	ctx, cancel := context.WithCancel(context.Background())
	return &pfRegistry{nn: nn, slot: &controller.PlannedClaimSlot{}, ctx: ctx, cancel: cancel}
}

func (g *pfRegistry) Snapshot(nn types.NamespacedName) controller.RegistrySnapshot {
	g.mu.RLock()
	defer g.mu.RUnlock()
	return controller.RegistrySnapshot{Current: g.current, Retiring: append([]*controller.TopologyManager(nil), g.retiring...), Slot: g.slot}
}

func (g *pfRegistry) Slot(types.NamespacedName) *controller.PlannedClaimSlot { return g.slot }
func (g *pfRegistry) LeaderContext() context.Context                         { return g.ctx }

// pfSink records what the engine reports.
type pfSink struct {
	mu        sync.Mutex
	events    []string
	terminals []v1alpha1.PlannedFailoverStatus
}

func (s *pfSink) Event(_ *v1alpha1.MysqlFailoverGroup, eventType, reason, message string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.events = append(s.events, eventType+"/"+reason+": "+message)
}

func (s *pfSink) RunTerminal(_ types.NamespacedName, pf *v1alpha1.PlannedFailoverStatus) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.terminals = append(s.terminals, *pf.DeepCopy())
}

func (s *pfSink) LagWaitObserved(types.NamespacedName, string, time.Duration)        {}
func (s *pfSink) Deferred(types.NamespacedName, v1alpha1.PlannedFailoverDeferReason) {}

func (s *pfSink) reasons() []string {
	s.mu.Lock()
	defer s.mu.Unlock()
	var out []string
	for _, e := range s.events {
		if i := strings.Index(e, ":"); i > 0 {
			out = append(out, e[:i])
		}
	}
	return out
}

// pfOpts shapes a harness.
type pfOpts struct {
	third    state.SiteRole // "": two sites
	cooldown time.Duration
	updater  bool // wire a production UpdateController
}

type pfHarness struct {
	t      *testing.T
	nn     types.NamespacedName
	clk    *clock.FakeClock
	logger *slog.Logger
	cfg    controller.TopologyConfig
	fc     *controller.FailoverController
	uc     *controller.UpdateController

	src, tgt, third *mockMySQL
	dns             *mockDNS
	stateDenied     bool // the out-of-band store outage

	store  *dst.PlannedStore
	reg    *pfRegistry
	facade controller.PlannedFailoverTopology
	eng    *controller.PlannedFailoverEngine
	sink   *pfSink
	tm     *controller.TopologyManager
	runSeq int
}

func newPFMock(uuid string, readOnly bool, sourceHost string) *mockMySQL {
	m := &mockMySQL{readOnly: readOnly, superReadOnly: readOnly, gtidExecuted: pfF, serverUUID: uuid,
		clearChannelOnReset: true, stopClearsThreads: true}
	if sourceHost != "" {
		m.replicaStatus = &mysql.ReplicaStatus{IORunning: true, SQLRunning: true, SourceHost: sourceHost}
	}
	return m
}

func newPFHarness(t *testing.T, o pfOpts) *pfHarness {
	t.Helper()
	h := &pfHarness{
		t:      t,
		nn:     types.NamespacedName{Namespace: "ns", Name: "lion"},
		clk:    clock.NewFakeClock(time.Date(2026, 11, 5, 12, 0, 0, 0, time.UTC)),
		logger: newQuietLogger(),
		src:    newPFMock(pfUUIDA, false, ""),
		tgt:    newPFMock(pfUUIDB, true, "mysql-dc1"),
		dns:    &mockDNS{},
		sink:   &pfSink{},
	}
	sites := []controller.SiteTopologyConfig{
		{Name: "dc1", Zone: "z1", LBIP: "10.0.0.1", Role: state.SiteRolePrimaryCandidate, Host: "mysql-dc1", TaintSelector: "site=dc1"},
		{Name: "dc2", Zone: "z2", LBIP: "10.0.0.2", Role: state.SiteRolePrimaryCandidate, Host: "mysql-dc2", TaintSelector: "site=dc2"},
	}
	if o.third != "" {
		h.third = newPFMock(pfUUIDC, true, "mysql-dc1")
		sites = append(sites, controller.SiteTopologyConfig{Name: "dc3", Zone: "z3", LBIP: "10.0.0.3", Role: o.third, Host: "mysql-dc3", TaintSelector: "site=dc3"})
	}
	h.cfg = controller.TopologyConfig{Namespace: h.nn.Namespace, Name: h.nn.Name, Sites: sites, PollInterval: int64(time.Second),
		FailureThreshold: 3, RecoveryThreshold: 2, FailoverCooldown: int64(o.cooldown)}
	h.fc = controller.NewFailoverController(h.logger)
	if o.updater {
		h.uc = controller.NewUpdateController(h.fc, h.logger)
		h.uc.SetTickIntervalForTest(5 * time.Millisecond)
	}

	obj := &v1alpha1.MysqlFailoverGroup{ObjectMeta: metav1.ObjectMeta{Namespace: h.nn.Namespace, Name: h.nn.Name}}
	for _, s := range sites {
		obj.Spec.Sites = append(obj.Spec.Sites, v1alpha1.SiteSpec{Name: s.Name, Role: v1alpha1.SiteRole(s.Role)})
	}
	obj.Spec.FailoverCooldown = &metav1.Duration{Duration: o.cooldown}
	obj.Spec.PlannedFailover = &v1alpha1.PlannedFailoverSpec{
		MaxLagWait: &metav1.Duration{Duration: 20 * time.Second}, DrainTimeout: &metav1.Duration{Duration: 3 * time.Second}}
	h.store = dst.NewPlannedStore(obj, h.clk)
	h.newProcess()
	for range 3 {
		h.tick()
	}
	if got := h.tm.Status().ActiveSite; got != "dc1" {
		t.Fatalf("setup: active site %q, want dc1", got)
	}
	return h
}

// checkers returns the sites' checkers; the pooled ReadGTIDSnapshot the
// grant probe asserts for is on the withServerUUID wrapper.
func (h *pfHarness) checkers() []mysql.Checker {
	out := []mysql.Checker{withServerUUID(h.src, h.src.serverUUID), withServerUUID(h.tgt, h.tgt.serverUUID)}
	if h.third != nil {
		out = append(out, withServerUUID(h.third, h.third.serverUUID))
	}
	return out
}

// newManager builds a TopologyManager restored from the object, as
// startManager does.
func (h *pfHarness) newManager() *controller.TopologyManager {
	tm := controller.NewTopologyManagerWithClock(h.cfg, h.checkers(), h.fc, h.uc, nil,
		controller.BootstrapConfig{ReplUser: "repl", ReplPassword: "secret"},
		newMockTainter(), platform.NewHub(h.logger), h.dns, h.logger, h.clk)
	tm.SetSleepForTest(func(time.Duration) {})
	obj := h.store.Object()
	rec := controller.NewAnnotationFailoverStateRecorderWithClock(dst.NewPlannedStoreClient(h.store, func() bool { return h.stateDenied }), nil, h.nn, h.clk)
	tm.RestoreFailoverStateForSim(obj, rec)
	tm.RestorePlannedFailoverStateForSim(obj)
	tm.StatusCallback = func(snap controller.TopologySnapshot) {
		if h.tm != tm {
			return // a replaced manager's writes are dropped, as the runner drops them
		}
		tm.MarkStatusWriteResult(h.store.MergeSnapshot(snap))
	}
	return tm
}

// wiring is the uncached ManagerWiring of the object.
func (h *pfHarness) wiring(slot *controller.PlannedClaimSlot) controller.ManagerWiring {
	obj := h.store.Object()
	w := controller.ManagerWiring{Slot: slot}
	// As registerManagerWith: a Promoting block whose own record is durable
	// released its claim and is not hydrated (O-m1).
	if pf := obj.Status.PlannedFailover; pf != nil && pfOwnsSites(obj) && controller.PlannedRunNeedsClaim(obj, h.clk.Now()) {
		w.HydrateFrom = pf.DeepCopy()
	}
	return w
}

// pfOwnsSites mirrors plannedFailoverOwnsSites (unexported).
func pfOwnsSites(obj *v1alpha1.MysqlFailoverGroup) bool {
	pf := obj.Status.PlannedFailover
	if pf == nil || pf.RunID == "" || pf.Result != nil {
		return false
	}
	if m, ok := controller.ParseTerminalMarker(obj.GetAnnotations()[controller.PlannedFailoverTerminalAnnotation]); ok && m.RunID == pf.RunID {
		return false
	}
	switch pf.Phase {
	case v1alpha1.PlannedFailoverPhaseFencing, v1alpha1.PlannedFailoverPhaseDraining, v1alpha1.PlannedFailoverPhaseWaitingForLag,
		v1alpha1.PlannedFailoverPhaseWaitingForDragonflySync, v1alpha1.PlannedFailoverPhasePromotingDragonfly,
		v1alpha1.PlannedFailoverPhasePromoting, v1alpha1.PlannedFailoverPhaseRollingBack:
		return true
	}
	return false
}

// newProcess is an operator (re)start: a new registry (no slot), a new
// facade and engine (no memos), a new manager restored and wired from the
// object.
func (h *pfHarness) newProcess() {
	if h.reg != nil {
		h.reg.cancel()
	}
	h.reg = newPFRegistry(h.nn)
	h.facade = controller.NewPlannedFailoverTopology(h.reg)
	h.eng = controller.NewPlannedFailoverEngine(h.store, h.facade, h.sink, h.clk, controller.SimHooks{})
	h.eng.SetRunIDSourceForSim(func() string { h.runSeq++; return fmt.Sprintf("run-%d", h.runSeq) })
	tm := h.newManager()
	controller.WireRegisteredManager(tm, h.wiring(h.reg.slot))
	h.reg.mu.Lock()
	h.reg.current = tm
	h.reg.retiring = nil
	h.reg.mu.Unlock()
	h.tm = tm
}

// replaceManager retires the current manager and registers a new one in
// the same process (the slot is shared).
func (h *pfHarness) replaceManager() *controller.TopologyManager {
	old := h.tm
	old.RetireForSim()
	tm := h.newManager()
	w := h.wiring(h.reg.slot)
	w.RetiringUpdateInFlight = old.OrderedUpdateClaimedForSim()
	w.RetiringBusy = func() bool { return old.ChangingSitesForSim() }
	controller.WireRegisteredManager(tm, w)
	h.reg.mu.Lock()
	h.reg.retiring = append(h.reg.retiring, old)
	h.reg.current = tm
	h.reg.mu.Unlock()
	h.tm = tm
	return old
}

// setAbsent unregisters the current manager (nil) or registers tm.
func (h *pfHarness) setCurrent(tm *controller.TopologyManager) {
	h.reg.mu.Lock()
	h.reg.current = tm
	h.reg.mu.Unlock()
}

// tick advances the clock one poll interval and polls the manager.
func (h *pfHarness) tick() {
	h.clk.Advance(time.Second)
	h.store.BeginPoll(int(h.clk.Now().Unix()))
	h.reg.mu.RLock()
	tm := h.reg.current
	h.reg.mu.RUnlock()
	if tm != nil {
		tm.Poll(context.Background())
	}
}

// pass runs one engine pass.
func (h *pfHarness) pass() time.Duration {
	d, _ := h.eng.Step(context.Background(), h.nn)
	return d
}

// request annotates the group with a planned failover to target.
func (h *pfHarness) request(target string) { h.store.SetRequest(target) }

func (h *pfHarness) pf() *v1alpha1.PlannedFailoverStatus {
	if obj := h.store.Object(); obj != nil && obj.Status.PlannedFailover != nil {
		return obj.Status.PlannedFailover.DeepCopy()
	}
	return nil
}

// runUntil alternates ticks and passes until done(pf) or max iterations.
func (h *pfHarness) runUntil(max int, done func(*v1alpha1.PlannedFailoverStatus) bool) *v1alpha1.PlannedFailoverStatus {
	h.t.Helper()
	for range max {
		h.pass()
		if pf := h.pf(); pf != nil && done(pf) {
			return pf
		}
		h.tick()
	}
	h.t.Fatalf("condition not reached after %d iterations; block %+v; store log:\n%s", max, h.pf(),
		strings.Join(h.store.LogStrings(), "\n"))
	return nil
}

func inPhase(phases ...v1alpha1.PlannedFailoverPhase) func(*v1alpha1.PlannedFailoverStatus) bool {
	return func(pf *v1alpha1.PlannedFailoverStatus) bool { return slices.Contains(phases, pf.Phase) }
}

func terminal(pf *v1alpha1.PlannedFailoverStatus) bool { return pf.Result != nil }

// held is the slot's claim.
func (h *pfHarness) held() (string, bool, bool) { return h.reg.slot.HeldForSim() }

func (h *pfHarness) wantOutcome(pf *v1alpha1.PlannedFailoverStatus, o v1alpha1.PlannedFailoverOutcome, r v1alpha1.PlannedFailoverReason) {
	h.t.Helper()
	if pf.Result == nil || pf.Result.Outcome != o || (r != "" && pf.Result.Reason != r) {
		h.t.Fatalf("result %+v, want %s/%s; message %q", pf.Result, o, r, pf.Message)
	}
}

// ---------------------------------------------------------------------------
// Happy paths
// ---------------------------------------------------------------------------

func TestPF_HappyPath_TwoSite(t *testing.T) {
	h := newPFHarness(t, pfOpts{})
	h.request("dc2")
	pf := h.runUntil(60, terminal)
	h.wantOutcome(pf, v1alpha1.PlannedFailoverOutcomeSucceeded, "")
	if h.tgt.isReadOnly() || !h.src.isSuperReadOnly() {
		t.Fatalf("after Succeeded: dc2 read-only=%v, dc1 fenced=%v", h.tgt.isReadOnly(), h.src.isSuperReadOnly())
	}
	if _, _, ok := h.held(); ok {
		t.Fatal("claim still held after Succeeded")
	}
	if got := h.tgt.statements(); !slices.Equal(got, []string{"StopReplica", "ResetReplicaAll", "SetSuperReadOnly:off", "SetReadOnly:off"}) {
		t.Fatalf("target statements %v", got)
	}
	if pf.Fence == nil || pf.Fence.SourceServerUUID != pfUUIDA || pf.Promotion == nil || pf.Promotion.TargetServerUUID != pfUUIDB {
		t.Fatalf("records %+v", pf)
	}
	obj := h.store.Object()
	if obj.Status.LastFailoverTarget != "dc2" || obj.Status.PromotionGtidExecuted != pfF {
		t.Fatalf("status record target=%q watermark=%q", obj.Status.LastFailoverTarget, obj.Status.PromotionGtidExecuted)
	}
}

// ---------------------------------------------------------------------------
// Harness steps used by the rows
// ---------------------------------------------------------------------------

// passBlocked runs one pass in a goroutine, waits until b holds a call
// inside its statement, runs during, releases b and returns the pass's
// requeue.
func (h *pfHarness) passBlocked(b *blockedCall, during func()) time.Duration {
	h.t.Helper()
	done := make(chan time.Duration, 1)
	go func() { done <- h.pass() }()
	select {
	case <-b.Entered():
	case d := <-done:
		h.t.Fatalf("the pass ended (requeue %s) before reaching the blocked statement; block %+v", d, h.pf())
	case <-time.After(10 * time.Second):
		h.t.Fatal("the pass never reached the blocked statement")
	}
	during()
	b.Release()
	return <-done
}

// runToPromotingIntent runs until the Promoting intent is durable; the
// pass that writes it sends no promotion statement (the next one does).
func (h *pfHarness) runToPromotingIntent() *v1alpha1.PlannedFailoverStatus {
	h.t.Helper()
	pf := h.runUntil(60, inPhase(v1alpha1.PlannedFailoverPhasePromoting))
	if got := h.tgt.statements(); len(got) != 0 {
		h.t.Fatalf("setup: target got %v before the Promoting intent settled", got)
	}
	return pf
}

func (h *pfHarness) holdCondition() bool {
	for _, c := range h.store.Object().Status.Conditions {
		if c.Type == v1alpha1.ConditionAutomaticFailoverHeld && c.Status == metav1.ConditionTrue {
			return true
		}
	}
	return false
}

func countOf(stmts []string, s string) int {
	n := 0
	for _, x := range stmts {
		if x == s {
			n++
		}
	}
	return n
}

// ---------------------------------------------------------------------------
// §14 rows (the ones the production updater is not needed for)
// ---------------------------------------------------------------------------

// Row 4: the predecessor wait never ends in "manual recovery"; manager
// absence is a wait, pre-claim and post-claim.
func TestPF_I04_ManagerAbsenceIsAWait(t *testing.T) {
	h := newPFHarness(t, pfOpts{})
	tm := h.tm
	h.setCurrent(nil)
	h.request("dc2")
	for range 5 {
		h.pass()
		h.tick()
	}
	pf := h.pf()
	if pf.Phase != v1alpha1.PlannedFailoverPhaseAdmitting || pf.Admission == nil ||
		pf.Admission.WaitReason != v1alpha1.PlannedFailoverWaitManagerAbsent {
		t.Fatalf("with no manager the run must wait in Admitting (ManagerAbsent): %+v", pf)
	}
	if h.src.isSuperReadOnly() {
		t.Fatal("fenced without a manager")
	}
	h.setCurrent(tm)
	h.wantOutcome(h.runUntil(60, terminal), v1alpha1.PlannedFailoverOutcomeSucceeded, "")
}

// Row 5: an update cannot start or claim between the grant and the release.
func TestPF_I05_EarlierUpdateExcludedFromGrantToRelease(t *testing.T) {
	h := newPFHarness(t, pfOpts{})
	h.request("dc2")
	h.runUntil(20, inPhase(v1alpha1.PlannedFailoverPhaseDraining, v1alpha1.PlannedFailoverPhaseWaitingForLag))
	if u, err := h.tm.BeginOrderedUpdateForSim(); err == nil {
		u.FinishOrderedUpdateForSim()
		t.Fatal("an ordered update started while the run held the claim")
	}
	h.wantOutcome(h.runUntil(60, terminal), v1alpha1.PlannedFailoverOutcomeSucceeded, "")
	h.tick()
	u, err := h.tm.BeginOrderedUpdateForSim()
	if err != nil {
		t.Fatalf("after the release an update must start: %v", err)
	}
	u.FinishOrderedUpdateForSim()
}

// Row 7: an update that claims between the request's acceptance and the
// grant makes the grant wait; nothing is fenced.
func TestPF_I07_UpdateClaimBetweenReadAndGrant(t *testing.T) {
	h := newPFHarness(t, pfOpts{})
	u, err := h.tm.BeginOrderedUpdateForSim()
	if err != nil {
		t.Fatal(err)
	}
	if ok, err := u.ClaimOrderedUpdateForSim(context.Background()); !ok || err != nil {
		t.Fatalf("update claim: %v %v", ok, err)
	}
	h.request("dc2")
	for range 4 {
		h.pass()
		h.tick()
	}
	if pf := h.pf(); pf.Phase != v1alpha1.PlannedFailoverPhaseAdmitting || h.src.isSuperReadOnly() {
		t.Fatalf("the grant must wait for the claimed update: %+v fenced=%v", pf, h.src.isSuperReadOnly())
	}
	u.FinishOrderedUpdateForSim()
	h.wantOutcome(h.runUntil(60, terminal), v1alpha1.PlannedFailoverOutcomeSucceeded, "")
}

// Row 8: a status write that timed out but committed keeps the claim; one
// that never committed releases the provisional grant with nothing sent.
func TestPF_I08_TimedOutCommittedWriteKeepsClaim(t *testing.T) {
	t.Run("ambiguousCommitted", func(t *testing.T) {
		h := newPFHarness(t, pfOpts{})
		h.request("dc2")
		h.runUntil(10, inPhase(v1alpha1.PlannedFailoverPhaseAdmitting))
		h.store.AmbiguousWrites(1) // the Fencing write
		h.pass()
		if id, _, ok := h.held(); !ok || id != h.pf().RunID {
			t.Fatalf("claim %q %v after an ambiguous-but-committed Fencing write", id, ok)
		}
		h.wantOutcome(h.runUntil(60, terminal), v1alpha1.PlannedFailoverOutcomeSucceeded, "")
	})
	t.Run("lostReleasesProvisional", func(t *testing.T) {
		h := newPFHarness(t, pfOpts{})
		h.request("dc2")
		h.runUntil(10, inPhase(v1alpha1.PlannedFailoverPhaseAdmitting))
		h.store.LoseWrites(1)
		h.pass()
		if _, _, ok := h.held(); ok && h.pf().Phase == v1alpha1.PlannedFailoverPhaseAdmitting {
			t.Fatal("a provisional grant survived a proven non-commit")
		}
		if h.src.isSuperReadOnly() {
			t.Fatal("fenced although the Fencing write never committed")
		}
		h.wantOutcome(h.runUntil(60, terminal), v1alpha1.PlannedFailoverOutcomeSucceeded, "")
	})
}

// Row 10: no manager at admission waits with ManagerAbsent and an Event,
// then ends AdmissionTimeout with nothing sent.
func TestPF_I10_NoManagerAtAdmission(t *testing.T) {
	h := newPFHarness(t, pfOpts{})
	h.setCurrent(nil)
	h.request("dc2")
	for range 3 {
		h.pass()
		h.tick()
	}
	h.clk.Advance(3 * time.Minute)
	pf := h.runUntil(10, terminal)
	h.wantOutcome(pf, v1alpha1.PlannedFailoverOutcomeNotStarted, v1alpha1.PlannedFailoverReasonAdmissionTimeout)
	if len(h.src.statements())+len(h.tgt.statements()) != 0 {
		t.Fatal("statements sent without a manager")
	}
}

// Row 12: strict peer evidence at the grant: a writable third site, or an
// unreadable one, keeps the run from fencing anything.
func TestPF_I12_AuthorityMovedBesideThirdWritable(t *testing.T) {
	for name, spoil := range map[string]func(h *pfHarness){
		"writable": func(h *pfHarness) { h.third.setRO(false, false) },
		"unreadableWritableC": func(h *pfHarness) {
			h.third.setRO(false, false)
			h.third.setError(errors.New("dial tcp: i/o timeout"))
		},
	} {
		t.Run(name, func(t *testing.T) {
			h := newPFHarness(t, pfOpts{third: state.SiteRolePrimaryCandidate})
			spoil(h)
			h.request("dc2")
			for range 6 {
				h.pass()
				h.tick()
			}
			if h.src.isSuperReadOnly() || len(h.tgt.statements()) != 0 {
				t.Fatalf("the run fenced or promoted beside a foreign writable/unreadable site: src fenced=%v tgt=%v",
					h.src.isSuperReadOnly(), h.tgt.statements())
			}
			if pf := h.pf(); pf.Phase != v1alpha1.PlannedFailoverPhaseAdmitting && (pf.Result == nil || pf.Result.Outcome != v1alpha1.PlannedFailoverOutcomeNotStarted) {
				t.Fatalf("block %+v, want Admitting or NotStarted", pf)
			}
		})
	}
}

// Row 12c: a foreign writable site seen after a possible write ends the run
// ManualRecoveryRequired with the automation hold, and automation fences
// neither site.
func TestPF_I12c_PostWriteForeignWritableHoldsAutomation(t *testing.T) {
	h := newPFHarness(t, pfOpts{third: state.SiteRolePrimaryCandidate})
	h.request("dc2")
	h.runToPromotingIntent()
	b := h.tgt.blockOn(t, "SetReadOnly", true) // read_only=OFF applied, reply pending
	h.passBlocked(b, func() { h.third.setRO(false, false) })
	pf := h.runUntil(20, terminal)
	h.wantOutcome(pf, v1alpha1.PlannedFailoverOutcomeManualRecoveryRequired, "")
	if !h.holdCondition() {
		t.Fatal("no AutomaticFailoverHeld condition")
	}
	if id, hold, ok := h.held(); !ok || !hold || id != pf.RunID {
		t.Fatalf("slot %q hold=%v ok=%v, want the run's hold", id, hold, ok)
	}
	for range 10 {
		h.tick()
	}
	if h.third.isReadOnly() || h.tgt.isReadOnly() {
		t.Fatal("automation fenced a site while the hold stands")
	}
}

// Row 13: a Promoting re-run after a lost reply confirms on a new session:
// no second STOP or RESET, no KILL, Succeeded.
func TestPF_I13_PromotingRerunConfirms(t *testing.T) {
	h := newPFHarness(t, pfOpts{})
	h.request("dc2")
	h.runToPromotingIntent()
	b := h.tgt.blockOn(t, "SetReadOnly", true)
	h.passBlocked(b, h.tgt.dropConnections) // applied; the reply is lost
	h.newProcess()                          // a cross-process re-run
	pf := h.runUntil(40, terminal)
	h.wantOutcome(pf, v1alpha1.PlannedFailoverOutcomeSucceeded, "")
	st := h.tgt.statements()
	if countOf(st, "StopReplica") != 1 || countOf(st, "ResetReplicaAll") != 1 || countOf(st, "KillAppConnections") != 0 {
		t.Fatalf("target statements %v: the re-run repeated the promotion", st)
	}
}

// Row 13 (round 3): a restart between RecordPlannedPromotion and the
// Publishing write recognises the run's own record and confirms; no hold.
func TestPF_I13_RestartBetweenRecordAndPublishingConfirms(t *testing.T) {
	h := newPFHarness(t, pfOpts{})
	h.request("dc2")
	h.runUntil(60, inPhase(v1alpha1.PlannedFailoverPhasePromoting))
	h.store.DenyStatus(1 << 20) // the Publishing write cannot land
	for range 4 {
		h.pass()
	}
	if h.tgt.isReadOnly() {
		t.Fatal("setup: the target was not promoted")
	}
	h.newProcess()
	h.store.ClearFaults()
	pf := h.runUntil(40, terminal)
	h.wantOutcome(pf, v1alpha1.PlannedFailoverOutcomeSucceeded, "")
	if h.holdCondition() {
		t.Fatal("the run's own record became a hold")
	}
}

// Row 13b: three conflicts on the Publishing write in one process still
// confirm.
func TestPF_I13b_PublishingConflictTimes3Confirms(t *testing.T) {
	h := newPFHarness(t, pfOpts{})
	h.request("dc2")
	h.runUntil(60, inPhase(v1alpha1.PlannedFailoverPhasePromoting))
	h.store.ConflictWrites(3)
	pf := h.runUntil(40, terminal)
	h.wantOutcome(pf, v1alpha1.PlannedFailoverOutcomeSucceeded, "")
	if h.holdCondition() {
		t.Fatal("hold after Publishing conflicts")
	}
}

// Row 15: a re-run resumes from the pinned stage: after a lost reply to
// RESET REPLICA ALL the next pass neither stops nor resets again.
func TestPF_I15_RerunResumesFromPinnedStage(t *testing.T) {
	h := newPFHarness(t, pfOpts{})
	h.request("dc2")
	h.runToPromotingIntent()
	b := h.tgt.blockOn(t, "ResetReplicaAll", true)
	h.passBlocked(b, h.tgt.dropConnections)
	pf := h.runUntil(40, terminal)
	h.wantOutcome(pf, v1alpha1.PlannedFailoverOutcomeSucceeded, "")
	st := h.tgt.statements()
	if countOf(st, "StopReplica") != 1 || countOf(st, "ResetReplicaAll") != 1 {
		t.Fatalf("target statements %v", st)
	}
}

// Row 17: a transient confirm failure is retried within the windows, not
// mapped to a failure.
func TestPF_I17_TransientConfirmFailureRetries(t *testing.T) {
	h := newPFHarness(t, pfOpts{})
	h.request("dc2")
	h.runToPromotingIntent()
	b := h.tgt.blockOn(t, "SetReadOnly", true)
	h.passBlocked(b, h.tgt.dropConnections) // the confirm cannot read on this session
	h.wantOutcome(h.runUntil(40, terminal), v1alpha1.PlannedFailoverOutcomeSucceeded, "")
}

// Row 18: a DNS failure ends Succeeded with the DNSUpdateFailed warning.
func TestPF_I18_DNSFailureSucceedsWithWarning(t *testing.T) {
	h := newPFHarness(t, pfOpts{})
	h.request("dc2")
	h.runUntil(60, inPhase(v1alpha1.PlannedFailoverPhaseWaitingForLag))
	h.dns.mu.Lock()
	h.dns.err = errors.New("dnsendpoints is forbidden")
	h.dns.mu.Unlock()
	pf := h.runUntil(40, terminal)
	h.wantOutcome(pf, v1alpha1.PlannedFailoverOutcomeSucceeded, "")
	if !slices.Contains(pf.Result.Warnings, v1alpha1.PlannedFailoverWarningDNSUpdateFailed) {
		t.Fatalf("warnings %v, want DNSUpdateFailed", pf.Result.Warnings)
	}
}

// Row 19: no grant before the manager's first poll, nor beside two
// writable sites.
func TestPF_I19_GrantRequiresReadyAndLiveEvidence(t *testing.T) {
	t.Run("notReady", func(t *testing.T) {
		h := newPFHarness(t, pfOpts{})
		h.newProcess() // no Poll yet
		h.request("dc2")
		h.pass()
		if pf := h.pf(); pf.Phase != v1alpha1.PlannedFailoverPhaseAdmitting || h.src.isSuperReadOnly() {
			t.Fatalf("granted before the first poll: %+v", pf)
		}
	})
	t.Run("twoWritable", func(t *testing.T) {
		h := newPFHarness(t, pfOpts{})
		h.tgt.setRO(false, false)
		h.request("dc2")
		for range 4 {
			h.pass()
		}
		if h.src.isSuperReadOnly() {
			t.Fatal("fenced beside a second writable site")
		}
	})
}

// Row 20a: a lost Fencing write sends nothing and releases the grant.
func TestPF_I20a_LostFenceWriteSendsNothing(t *testing.T) {
	h := newPFHarness(t, pfOpts{})
	h.request("dc2")
	h.runUntil(10, inPhase(v1alpha1.PlannedFailoverPhaseAdmitting))
	h.store.LoseWrites(1)
	h.pass()
	if h.src.isSuperReadOnly() || countOf(h.src.statements(), "SetSuperReadOnly:on") != 0 {
		t.Fatal("the fence was sent although its intent never committed")
	}
}

// Row 20b: a fence the run cannot attribute to itself is never cleared. The
// source's pod is replaced after the run's fence and boots fenced by its
// sidecar under a new server_uuid; a restarted process cannot attribute that
// fence (no memo, the intent's uuid does not match) and must leave it.
func TestPF_I20b_UnattributableFenceNeverCleared(t *testing.T) {
	h := newPFHarness(t, pfOpts{})
	h.request("dc2")
	h.runUntil(30, inPhase(v1alpha1.PlannedFailoverPhaseDraining, v1alpha1.PlannedFailoverPhaseWaitingForLag))
	h.src.replacePod(podReplacement{serverUUID: "eeeeeeee-0000-0000-0000-000000000005"})
	h.src.setRO(true, true)
	before := len(h.src.statements())
	h.newProcess()
	pf := h.runUntil(60, terminal)
	if pf.Result.Outcome == v1alpha1.PlannedFailoverOutcomeSucceeded || pf.Result.Outcome == v1alpha1.PlannedFailoverOutcomeRolledBack {
		t.Fatalf("result %+v on a fence that is not the run's", pf.Result)
	}
	after := h.src.statements()[before:]
	if countOf(after, "SetSuperReadOnly:off")+countOf(after, "SetReadOnly:off") != 0 {
		t.Fatalf("an unattributable fence was cleared: %v", after)
	}
}

// Row 20c: an unreadable promotable peer blocks the pre-write gate.
func TestPF_I20c_UnreadableThirdSiteBlocksGate(t *testing.T) {
	h := newPFHarness(t, pfOpts{third: state.SiteRolePrimaryCandidate})
	h.request("dc2")
	h.runToPromotingIntent()
	h.third.setError(errors.New("dial tcp: i/o timeout"))
	for range 4 {
		h.pass()
		h.tick()
	}
	if h.tgt.writableGrants() != 0 {
		t.Fatalf("the gate let the target be written beside an unreadable peer: %v", h.tgt.statements())
	}
}

// Row 21: a waiting request owns nothing: emergency failover still runs.
func TestPF_I21_WaitingRequestOwnsNothing(t *testing.T) {
	h := newPFHarness(t, pfOpts{third: state.SiteRolePrimaryCandidate})
	h.third.setError(errors.New("dial tcp: i/o timeout")) // the request waits (PeerUnreadable)
	h.request("dc2")
	for range 3 {
		h.pass()
		h.tick()
	}
	if _, _, ok := h.held(); ok {
		t.Fatal("setup: the waiting request holds the claim")
	}
	h.src.setError(errDown)
	for range 15 {
		h.pass()
		h.tick()
	}
	if h.tgt.isReadOnly() {
		t.Fatalf("emergency failover was blocked by a waiting request; block %+v", h.pf())
	}
}

// Row 22: the watermark is restored after a restart.
func TestPF_I22_WatermarkRestoredAfterRestart(t *testing.T) {
	h := newPFHarness(t, pfOpts{})
	h.request("dc2")
	h.wantOutcome(h.runUntil(60, terminal), v1alpha1.PlannedFailoverOutcomeSucceeded, "")
	h.newProcess()
	h.tick()
	if got := h.tm.Status().PromotionGtidExecuted; got != pfF {
		t.Fatalf("restored watermark %q, want F %q", got, pfF)
	}
	if rec, err := controller.FailoverRecordFromAnnotations(h.store.Object().GetAnnotations()); err != nil ||
		rec.PromotionGtidExecuted != pfF || rec.LastFailoverTarget != "dc2" {
		t.Fatalf("out-of-band record %+v %v", rec, err)
	}
}

// Row 23: Promoting before the replacement registers waits; the slot is on
// the registry, so the replacement is blocked from its first poll.
func TestPF_I23_PromotingBeforeReplacementRegistersWaits(t *testing.T) {
	h := newPFHarness(t, pfOpts{})
	h.request("dc2")
	h.runToPromotingIntent()
	h.setCurrent(nil)
	for range 3 {
		h.pass()
	}
	if len(h.tgt.statements()) != 0 {
		t.Fatal("promoted with no manager registered")
	}
	h.replaceManager()
	if id, _, ok := h.held(); !ok || id != h.pf().RunID {
		t.Fatal("the replacement does not share the run's claim")
	}
	h.wantOutcome(h.runUntil(40, terminal), v1alpha1.PlannedFailoverOutcomeSucceeded, "")
}

// Row 24: a replaced target is never confirmed as the run's promotion.
func TestPF_I24_TargetKeyedRecordNoFalseConfirm(t *testing.T) {
	h := newPFHarness(t, pfOpts{})
	h.request("dc2")
	h.runToPromotingIntent()
	h.tgt.replacePod(podReplacement{serverUUID: "dddddddd-0000-0000-0000-000000000004"})
	pf := h.runUntil(40, terminal)
	if pf.Result.Outcome == v1alpha1.PlannedFailoverOutcomeSucceeded {
		t.Fatal("confirmed a replaced target")
	}
	h.wantOutcome(pf, v1alpha1.PlannedFailoverOutcomeRolledBack, v1alpha1.PlannedFailoverReasonTargetChanged)
	if h.tgt.writableGrants() != 0 || h.src.isReadOnly() {
		t.Fatalf("target grants %d, source read-only %v", h.tgt.writableGrants(), h.src.isReadOnly())
	}
}

// Row 25: a stale read cannot clear the claim: a replacement wired from an
// object that predates the run still shares the held slot.
func TestPF_I25_StaleReadNeverClearsClaim(t *testing.T) {
	h := newPFHarness(t, pfOpts{})
	stale := h.store.Object()
	h.request("dc2")
	h.runUntil(20, inPhase(v1alpha1.PlannedFailoverPhaseDraining, v1alpha1.PlannedFailoverPhaseWaitingForLag))
	old := h.tm
	old.RetireForSim()
	tm := h.newManager()
	controller.WireRegisteredManager(tm, controller.ManagerWiring{Slot: h.reg.slot}) // nothing to hydrate: a stale view
	h.reg.mu.Lock()
	h.reg.current, h.reg.retiring = tm, []*controller.TopologyManager{old}
	h.reg.mu.Unlock()
	h.tm = tm
	_ = stale
	if id, _, ok := h.held(); !ok || id != h.pf().RunID {
		t.Fatal("a stale wiring cleared the claim")
	}
	h.wantOutcome(h.runUntil(40, terminal), v1alpha1.PlannedFailoverOutcomeSucceeded, "")
}

// Row 26: the recorded watermark is the run's fence.gtidSet, not whatever
// status says by then.
func TestPF_I26_WatermarkFromFenceNotStatus(t *testing.T) {
	h := newPFHarness(t, pfOpts{})
	h.request("dc2")
	h.runToPromotingIntent()
	h.store.MutateStatus(func(st *v1alpha1.MysqlFailoverGroupStatus) {
		st.PromotionGtidExecuted = pfUUIDA + ":1-7"
	})
	h.wantOutcome(h.runUntil(40, terminal), v1alpha1.PlannedFailoverOutcomeSucceeded, "")
	if got := h.store.Object().Status.PromotionGtidExecuted; got != pfF {
		t.Fatalf("watermark %q, want F", got)
	}
	if rec, _ := controller.FailoverRecordFromAnnotations(h.store.Object().GetAnnotations()); rec.PromotionGtidExecuted != pfF {
		t.Fatalf("out-of-band watermark %q", rec.PromotionGtidExecuted)
	}
}

// ---------------------------------------------------------------------------
// Rows with a replaced manager, the production updater, or a promotion in
// flight (1-3, 6, 9, 11)
// ---------------------------------------------------------------------------

// tickAsync polls in a goroutine (a Poll that will block inside a
// statement); the returned channel closes when it returns.
func (h *pfHarness) tickAsync() <-chan struct{} {
	done := make(chan struct{})
	h.clk.Advance(time.Second)
	tm := h.tm
	go func() {
		defer close(done)
		tm.Poll(context.Background())
	}()
	return done
}

func waitEntered(t *testing.T, b *blockedCall, what string) {
	t.Helper()
	select {
	case <-b.Entered():
	case <-time.After(10 * time.Second):
		t.Fatalf("%s never reached its blocked statement", what)
	}
}

func waitDone(t *testing.T, done <-chan struct{}, what string) {
	t.Helper()
	select {
	case <-done:
	case <-time.After(10 * time.Second):
		t.Fatalf("%s did not return", what)
	}
}

// waitIdle waits for the production updater to finish.
func (h *pfHarness) waitIdle(t *testing.T) {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for h.uc.IsUpdating() {
		if time.Now().After(deadline) {
			t.Fatal("the ordered update did not finish")
		}
		time.Sleep(5 * time.Millisecond)
	}
}

// startHandoffBlockedInFence starts a production ordered update of the
// active site whose handoff blocks inside the source fence.
func (h *pfHarness) startHandoffBlockedInFence(t *testing.T) *blockedCall {
	t.Helper()
	h.tm.ApplyUpdate = func(context.Context, string) error { return nil }
	b := h.src.blockOn(t, "SetSuperReadOnly", false)
	h.tm.SetSpecDriftSites([]string{"dc1"})
	h.tick() // starts the update goroutine
	waitEntered(t, b, "the ordered update's handoff")
	return b
}

// Row 1: a replaced manager's update is inside its fence statement when a
// planned failover asks for the claim: the grant waits (PredecessorBusy),
// and nothing of the run is sent.
func TestPF_I01_ReplacedManagerUpdateBlocksGrant(t *testing.T) {
	h := newPFHarness(t, pfOpts{updater: true})
	b := h.startHandoffBlockedInFence(t)
	h.replaceManager() // the old manager is retired while blocked inside the statement
	for range 3 {
		h.tick() // the replacement observes the sites before the request is validated
	}
	// The replacement's status no longer shows the retired manager's update
	// (its own updatePhase is idle), so only the claim can refuse: the
	// guard under test.
	h.store.MutateStatus(func(st *v1alpha1.MysqlFailoverGroupStatus) { st.UpdatePhase = "" })
	h.request("dc2")
	for range 5 {
		h.pass()
		h.tick()
	}
	pf := h.pf()
	if pf.Phase != v1alpha1.PlannedFailoverPhaseAdmitting || pf.Admission == nil ||
		(pf.Admission.WaitReason != v1alpha1.PlannedFailoverWaitPredecessorBusy && pf.Admission.WaitReason != v1alpha1.PlannedFailoverWaitUpdateInFlight) {
		t.Fatalf("the grant must wait for the predecessor's update: %+v admission %+v result %+v", pf, pf.Admission, pf.Result)
	}
	if _, _, ok := h.held(); ok {
		t.Fatal("granted while a replaced manager's update is mid-statement")
	}
	b.Release()
	h.waitIdle(t)
	pf = h.runUntil(60, terminal)
	if pf.Result.Outcome == v1alpha1.PlannedFailoverOutcomeSucceeded && countOf(h.tgt.statements(), "ResetReplicaAll") > 1 {
		t.Fatalf("the target was promoted twice: %v", h.tgt.statements())
	}
}

// Row 2: under the claim no update starts on any generation, so a rollback
// never unfences beside a replaced manager's promotion.
func TestPF_I02_RollbackBesideReplacedPromotion(t *testing.T) {
	h := newPFHarness(t, pfOpts{updater: true})
	h.tgt.setGtidExecuted(pfUUIDA + ":1-50") // the target lags: the run waits, then rolls back
	h.request("dc2")
	h.runUntil(30, inPhase(v1alpha1.PlannedFailoverPhaseWaitingForLag))
	var applied []string
	var mu sync.Mutex
	h.tm.ApplyUpdate = func(_ context.Context, site string) error {
		mu.Lock()
		applied = append(applied, site)
		mu.Unlock()
		return nil
	}
	h.tm.SetSpecDriftSites([]string{"dc1"})
	h.tick()
	old := h.replaceManager()
	old.Poll(context.Background()) // a retired manager starts nothing
	h.clk.Advance(30 * time.Second)
	pf := h.runUntil(30, terminal)
	h.wantOutcome(pf, v1alpha1.PlannedFailoverOutcomeRolledBack, v1alpha1.PlannedFailoverReasonLagTimeout)
	mu.Lock()
	defer mu.Unlock()
	if len(applied) != 0 || h.tgt.writableGrants() != 0 {
		t.Fatalf("an update ran under the claim (applied %v) or the target was written (%d)", applied, h.tgt.writableGrants())
	}
	if h.src.isReadOnly() {
		t.Fatal("the rollback did not unfence the source")
	}
}

// Row 3: an emergency promotion in flight (blocked inside its read_only
// clear) makes a planned grant wait; the run claims only once the
// promotion is observed, naming the new primary as its source.
func TestPF_I03_InFlightPromotionAtGrantWaits(t *testing.T) {
	h := newPFHarness(t, pfOpts{third: state.SiteRolePrimaryCandidate, updater: true})
	b := h.tgt.blockOn(t, "SetReadOnly", false)
	h.src.setError(errDown)
	var done <-chan struct{}
	for i := 0; i < 10 && done == nil; i++ {
		d := h.tickAsync()
		select {
		case <-b.Entered():
			done = d
		case <-d:
		}
	}
	if done == nil {
		t.Fatal("setup: the emergency promotion never started")
	}
	h.request("dc3")
	for range 3 {
		h.pass()
	}
	if _, _, ok := h.held(); ok || h.tgt.isSuperReadOnly() == false && countOf(h.tgt.statements(), "SetSuperReadOnly:on") > 0 {
		t.Fatalf("granted beside an in-flight promotion: block %+v", h.pf())
	}
	if pf := h.pf(); pf.Phase != v1alpha1.PlannedFailoverPhaseAdmitting {
		t.Fatalf("block %+v, want Admitting", pf)
	}
	b.Release()
	waitDone(t, done, "the promoting poll")
	h.src.respawn(true) // the old primary is back, read-only
	h.src.setRO(true, true)
	pf := h.runUntil(60, terminal)
	if pf.Result.Outcome == v1alpha1.PlannedFailoverOutcomeSucceeded && pf.SourceSite != "dc2" {
		t.Fatalf("the run named %q as its source, want the new primary dc2", pf.SourceSite)
	}
}

// Row 6: a rollback in WaitingForLag after a manager replacement: the
// replacement gets the slot before its first poll, the retired manager
// promotes nothing, and the rollback restores the source.
func TestPF_I06_RollbackInLagAfterReplacement(t *testing.T) {
	h := newPFHarness(t, pfOpts{})
	h.tgt.setGtidExecuted(pfUUIDA + ":1-50")
	h.request("dc2")
	h.runUntil(30, inPhase(v1alpha1.PlannedFailoverPhaseWaitingForLag))
	old := h.replaceManager()
	if id, _, ok := h.held(); !ok || id != h.pf().RunID {
		t.Fatal("the replacement does not hold the run's claim before its first poll")
	}
	h.clk.Advance(30 * time.Second)
	for range 3 {
		old.Poll(context.Background())
		h.pass()
		h.tick()
	}
	pf := h.runUntil(30, terminal)
	h.wantOutcome(pf, v1alpha1.PlannedFailoverOutcomeRolledBack, v1alpha1.PlannedFailoverReasonLagTimeout)
	if h.tgt.writableGrants() != 0 || h.src.isReadOnly() {
		t.Fatalf("target grants %d, source read-only %v", h.tgt.writableGrants(), h.src.isReadOnly())
	}
}

// Row 9: a retiring manager mid-promotion when a run claims: Wait
// (PredecessorBusy), decided outside the locks.
func TestPF_I09_RetiringMidPromotionWaits(t *testing.T) {
	h := newPFHarness(t, pfOpts{third: state.SiteRolePrimaryCandidate, updater: true})
	b := h.tgt.blockOn(t, "SetReadOnly", false)
	h.src.setError(errDown)
	var done <-chan struct{}
	for i := 0; i < 10 && done == nil; i++ {
		d := h.tickAsync()
		select {
		case <-b.Entered():
			done = d
		case <-d:
		}
	}
	if done == nil {
		t.Fatal("setup: the emergency promotion never started")
	}
	h.replaceManager() // the promoting manager retires mid-statement
	h.request("dc3")
	for range 3 {
		h.pass()
	}
	pf := h.pf()
	if _, _, ok := h.held(); ok || pf.Phase != v1alpha1.PlannedFailoverPhaseAdmitting || pf.Admission == nil ||
		pf.Admission.WaitReason != v1alpha1.PlannedFailoverWaitPredecessorBusy {
		t.Fatalf("block %+v held=%v, want Admitting with PredecessorBusy", pf, ok)
	}
	b.Release()
	waitDone(t, done, "the retired manager's poll")
}

// Row 11: an unobserved promotion when a run claims waits
// (PromotionInFlight), and the source is named by the poll that observes it.
func TestPF_I11_UnobservedPromotionWaits(t *testing.T) {
	h := newPFHarness(t, pfOpts{third: state.SiteRolePrimaryCandidate, cooldown: time.Second})
	b := h.tgt.blockOn(t, "SetReadOnly", false)
	h.src.setError(errDown)
	var done <-chan struct{}
	for i := 0; i < 10 && done == nil; i++ {
		d := h.tickAsync()
		select {
		case <-b.Entered():
			done = d
		case <-d:
		}
	}
	if done == nil {
		t.Fatal("setup: the emergency promotion never started")
	}
	h.request("dc3")
	h.pass()
	h.pass() // the first pass writes Admitting; the claim is asked on the next
	if pf := h.pf(); pf.Phase != v1alpha1.PlannedFailoverPhaseAdmitting || pf.Admission == nil ||
		(pf.Admission.WaitReason != v1alpha1.PlannedFailoverWaitPromotionInFlight && pf.Admission.WaitReason != v1alpha1.PlannedFailoverWaitPeerUnreadable) {
		t.Fatalf("block %+v admission %+v, want a wait while the promotion is in flight", pf, pf.Admission)
	}
	b.Release()
	waitDone(t, done, "the promoting poll")
	h.src.respawn(true)
	h.src.setRO(true, true)
	h.clk.Advance(2 * time.Second) // past the emergency promotion's cooldown
	pf := h.runUntil(60, terminal)
	h.wantOutcome(pf, v1alpha1.PlannedFailoverOutcomeSucceeded, "")
	if pf.SourceSite != "dc2" {
		t.Fatalf("source %q, want dc2 (the observed new primary)", pf.SourceSite)
	}
}

// ---------------------------------------------------------------------------
// Further pinned rows (§14) and round-3 additions
// ---------------------------------------------------------------------------

// A target restart after the write (read_only=OFF applied, then mysqld
// restarts read_only=ON) is not rolled back: the memo knows the write was
// sent, the stage is WasWritable, and the run finishes.
func TestPF_TargetRestartAfterWriteIsNotRolledBack(t *testing.T) {
	h := newPFHarness(t, pfOpts{})
	h.request("dc2")
	h.runToPromotingIntent()
	b := h.tgt.blockOn(t, "SetReadOnly", true)
	h.passBlocked(b, func() { h.tgt.respawn(true) })
	pf := h.runUntil(40, terminal)
	h.wantOutcome(pf, v1alpha1.PlannedFailoverOutcomeSucceeded, "")
	if !h.src.isSuperReadOnly() {
		t.Fatal("the source was unfenced after the target had taken the write")
	}
	if countOf(h.tgt.statements(), "ResetReplicaAll") != 1 {
		t.Fatalf("target statements %v", h.tgt.statements())
	}
}

// A run that finished confirmed before a restart, and whose re-run only
// writes status, never records the promotion again.
func TestPF_PublishingRerunNeverRecords(t *testing.T) {
	h := newPFHarness(t, pfOpts{})
	h.request("dc2")
	h.runUntil(60, inPhase(v1alpha1.PlannedFailoverPhasePublishing))
	before := 0
	for _, l := range h.store.LogStrings() {
		if strings.Contains(l, "recorderPatch → committed") {
			before++
		}
	}
	h.newProcess()
	h.wantOutcome(h.runUntil(20, terminal), v1alpha1.PlannedFailoverOutcomeSucceeded, "")
	after := 0
	for _, l := range h.store.LogStrings() {
		if strings.Contains(l, "recorderPatch → committed") {
			after++
		}
	}
	// The restarted manager's own restore may re-flush the same record (a
	// no-op patch); the run itself must not record a new promotion.
	if rec, _ := controller.FailoverRecordFromAnnotations(h.store.Object().GetAnnotations()); rec.LastFailoverTarget != "dc2" {
		t.Fatalf("record %+v", rec)
	}
	if after > before+1 {
		t.Fatalf("the Publishing re-run recorded the promotion again (%d → %d patches)", before, after)
	}
}

// Source loss in Draining hands off after the 10s source-loss window:
// SourceLeftFenced{SourceLost}, the claim released, and emergency
// failover then runs.
func TestPF_SourceLossInDrainingHandsOffIn10s(t *testing.T) {
	h := newPFHarness(t, pfOpts{})
	h.src.killConnectionResults = []int{5, 5, 5, 5, 5, 5, 5, 5}
	h.request("dc2")
	h.runUntil(30, inPhase(v1alpha1.PlannedFailoverPhaseDraining))
	h.src.setError(errDown)
	start := h.clk.Now()
	pf := h.runUntil(40, terminal)
	h.wantOutcome(pf, v1alpha1.PlannedFailoverOutcomeSourceLeftFenced, v1alpha1.PlannedFailoverReasonSourceLost)
	if took := h.clk.Now().Sub(start); took > 15*time.Second {
		t.Fatalf("the hand-off took %s, want about the 10s window", took)
	}
	if _, _, ok := h.held(); ok {
		t.Fatal("the claim was not released after SourceLost")
	}
	for range 15 {
		h.tick()
	}
	if h.tgt.isReadOnly() {
		t.Fatal("emergency failover did not promote the target after the hand-off")
	}
}

// A target that is writable with a replication channel, found by a
// cross-process re-run, is Inconsistent: ManualRecoveryRequired and a hold.
func TestPF_WritableWithChannelIsInconsistent(t *testing.T) {
	h := newPFHarness(t, pfOpts{})
	h.request("dc2")
	h.runToPromotingIntent()
	h.tgt.setRO(false, false) // someone else made it writable, channel still configured
	h.newProcess()
	pf := h.runUntil(20, terminal)
	h.wantOutcome(pf, v1alpha1.PlannedFailoverOutcomeManualRecoveryRequired, "")
	if !h.holdCondition() {
		t.Fatal("no hold")
	}
}

// A pre-write target loss rolls back after the 10s target-loss window.
func TestPF_PreWriteTargetLostRollsBackIn10s(t *testing.T) {
	h := newPFHarness(t, pfOpts{})
	h.tgt.setGtidExecuted(pfUUIDA + ":1-50")
	h.request("dc2")
	h.runUntil(30, inPhase(v1alpha1.PlannedFailoverPhaseWaitingForLag))
	h.tgt.setError(errDown)
	h.tgt.failNextCalls("GetGtidExecuted", 1000)
	start := h.clk.Now()
	pf := h.runUntil(40, terminal)
	h.wantOutcome(pf, v1alpha1.PlannedFailoverOutcomeRolledBack, v1alpha1.PlannedFailoverReasonTargetLost)
	if took := h.clk.Now().Sub(start); took > 15*time.Second {
		t.Fatalf("rolled back after %s, want about the 10s window", took)
	}
	if h.src.isReadOnly() {
		t.Fatal("the source was left fenced although nothing was written to the target")
	}
}

// PromotedUnconfirmed with an unreachable target does not block emergency
// failover back to the complete, fenced source.
func TestPF_UnconfirmedUnreachableTargetDoesNotBlockEmergency(t *testing.T) {
	h := newPFHarness(t, pfOpts{})
	h.request("dc2")
	h.runToPromotingIntent()
	b := h.tgt.blockOn(t, "SetReadOnly", true)
	h.passBlocked(b, func() {
		h.tgt.dropConnections()
		h.tgt.setError(errDown)
		h.tgt.failNextCalls("GetGtidExecuted", 1000)
		h.tgt.failNextCalls("ReadGTIDSnapshot", 1000)
		h.tgt.failNextCalls("ShowReplicaStatus", 1000)
		h.tgt.failNextCalls("CheckSuperReadOnly", 1000)
	})
	pf := h.runUntil(200, terminal)
	h.wantOutcome(pf, v1alpha1.PlannedFailoverOutcomePromotedUnconfirmed, "")
	if _, _, ok := h.held(); ok {
		t.Fatal("the claim outlived PromotedUnconfirmed")
	}
	for range 25 {
		h.tick()
	}
	if h.src.isReadOnly() {
		t.Fatalf("no writable primary 25s after PromotedUnconfirmed with an unreachable target (block %+v)", h.pf())
	}
}

// A restarted unconfirmed run never names its target over a newer failover.
func TestPF_RestartedUnconfirmedRunNeverNamesOverNewerFailover(t *testing.T) {
	h := newPFHarness(t, pfOpts{})
	h.request("dc2")
	h.runToPromotingIntent()
	b := h.tgt.blockOn(t, "SetReadOnly", true)
	h.passBlocked(b, func() {
		h.tgt.dropConnections()
		h.tgt.setError(errDown)
		h.tgt.failNextCalls("GetGtidExecuted", 1000)
		h.tgt.failNextCalls("ReadGTIDSnapshot", 1000)
		h.tgt.failNextCalls("ShowReplicaStatus", 1000)
		h.tgt.failNextCalls("CheckSuperReadOnly", 1000)
	})
	// A newer failover lands elsewhere before the restarted process decides.
	newer := metav1.NewTime(h.clk.Now().Add(time.Second))
	h.store.MutateStatus(func(st *v1alpha1.MysqlFailoverGroupStatus) {
		st.LastFailover, st.LastFailoverTarget = &newer, "dc1"
	})
	h.newProcess()
	pf := h.runUntil(200, terminal)
	// The re-proof sees the changed record after a possible write: a hold
	// (§4.4), or PromotedUnconfirmed if the record check passes. Either way
	// the target is never named over the newer failover.
	if o := pf.Result.Outcome; o != v1alpha1.PlannedFailoverOutcomePromotedUnconfirmed && o != v1alpha1.PlannedFailoverOutcomeManualRecoveryRequired {
		t.Fatalf("result %+v", pf.Result)
	}
	for range 2 {
		h.tick()
	}
	if got := h.store.Object().Status.LastFailoverTarget; got != "dc1" {
		t.Fatalf("lastFailoverTarget %q: the unconfirmed run named over the newer failover", got)
	}
}

// A run whose watermark exceeds the 64 KiB annotation limit keeps the claim
// through a status stall: the marker release is not available to it.
func TestPF_OversizedWatermarkKeepsClaimOnStall(t *testing.T) {
	h := newPFHarness(t, pfOpts{})
	var parts []string
	for i := range 1800 {
		parts = append(parts, fmt.Sprintf("%08x-0000-0000-0000-%012x:1-%d", i+1, i+1, i+2))
	}
	huge := strings.Join(parts, ",")
	h.src.setGtidExecuted(huge)
	h.tgt.setGtidExecuted(pfUUIDA + ":1-1") // lags: the run waits in WaitingForLag
	h.request("dc2")
	h.runUntil(30, inPhase(v1alpha1.PlannedFailoverPhaseWaitingForLag))
	if pf := h.pf(); pf.Fence == nil || pf.Fence.GtidSet == nil || len(*pf.Fence.GtidSet) <= 64*1024 {
		t.Fatalf("setup: watermark is not oversized (%d bytes)", len(*h.pf().Fence.GtidSet))
	}
	h.store.DenyStatus(1 << 20)
	for range 10 {
		h.clk.Advance(30 * time.Second)
		h.pass()
	}
	if id, _, ok := h.held(); !ok || id != h.pf().RunID {
		t.Fatal("an oversized-watermark run released its claim on a status stall")
	}
	if strings.Contains(h.store.Object().GetAnnotations()[controller.PlannedFailoverTerminalAnnotation], h.pf().RunID) {
		t.Fatal("an oversized-watermark run ended with a terminal marker")
	}
}

// ---------------------------------------------------------------------------
// #218: target replacement around the promotion (§15.2)
// ---------------------------------------------------------------------------

// Replaced between the lag proof and the promotion (regressed data, new
// uuid): TargetChanged, rolled back, nothing written to either pod.
func TestPF_218_TargetReplacedBetweenLagAndPromote(t *testing.T) {
	h := newPFHarness(t, pfOpts{})
	h.request("dc2")
	h.runToPromotingIntent()
	regressed := pfUUIDA + ":1-60"
	h.tgt.replacePod(podReplacement{serverUUID: "dddddddd-0000-0000-0000-000000000004", gtidExecuted: &regressed})
	pf := h.runUntil(40, terminal)
	h.wantOutcome(pf, v1alpha1.PlannedFailoverOutcomeRolledBack, v1alpha1.PlannedFailoverReasonTargetChanged)
	if h.tgt.writableGrants() != 0 {
		t.Fatal("the replaced pod was written")
	}
}

// Replaced mid-promotion after STOP REPLICA: the new pod comes up
// read-only; the promotion aborts, the channel is restored, the source gets
// its writes back.
func TestPF_218_ReplacedMidPromotionAfterStop(t *testing.T) {
	h := newPFHarness(t, pfOpts{})
	h.request("dc2")
	h.runToPromotingIntent()
	b := h.tgt.blockOn(t, "ResetReplicaAll", false)
	h.passBlocked(b, func() {
		h.tgt.replacePod(podReplacement{serverUUID: "dddddddd-0000-0000-0000-000000000004"})
	})
	pf := h.runUntil(40, terminal)
	if pf.Result.Outcome != v1alpha1.PlannedFailoverOutcomeRolledBack {
		t.Fatalf("result %+v, want RolledBack", pf.Result)
	}
	if h.tgt.writableGrants() != 0 || h.src.isReadOnly() {
		t.Fatalf("target grants %d, source read-only %v", h.tgt.writableGrants(), h.src.isReadOnly())
	}
}

// Replaced between the stage classification and the first statement: the
// pinned session fails and nothing reaches the new pod.
func TestPF_218_ReplacedBetweenClassificationAndFirstStatement(t *testing.T) {
	h := newPFHarness(t, pfOpts{})
	h.request("dc2")
	h.runToPromotingIntent()
	b := h.tgt.blockOn(t, "StopReplica", false)
	h.passBlocked(b, func() {
		h.tgt.replacePod(podReplacement{serverUUID: "dddddddd-0000-0000-0000-000000000004"})
	})
	pf := h.runUntil(40, terminal)
	if pf.Result.Outcome == v1alpha1.PlannedFailoverOutcomeSucceeded || h.tgt.writableGrants() != 0 {
		t.Fatalf("result %+v, target grants %d", pf.Result, h.tgt.writableGrants())
	}
}

// Replaced between the two GTID proofs (after STOP, before RESET's proof):
// the second proof fails on the pinned session.
func TestPF_218_ReplacedBetweenTheTwoGTIDProofs(t *testing.T) {
	h := newPFHarness(t, pfOpts{})
	h.request("dc2")
	h.runToPromotingIntent()
	b := h.tgt.blockOn(t, "StopReplica", true)
	h.passBlocked(b, func() {
		h.tgt.replacePod(podReplacement{serverUUID: "dddddddd-0000-0000-0000-000000000004"})
	})
	pf := h.runUntil(40, terminal)
	if pf.Result.Outcome == v1alpha1.PlannedFailoverOutcomeSucceeded || h.tgt.writableGrants() != 0 {
		t.Fatalf("result %+v, target grants %d", pf.Result, h.tgt.writableGrants())
	}
}

// ---------------------------------------------------------------------------
// Exactly-once side effects across crash points (§15.2)
// ---------------------------------------------------------------------------

// For every engine pass index of a run, kill the process right after that
// pass and rebuild it (a hydrated slot): at most one fence of the source,
// at most one RESET and no KILL on the target, and the run always ends
// with a defined outcome.
func TestPF_ExactlyOnceAcrossCrashPoints(t *testing.T) {
	for crash := 0; crash < 30; crash++ {
		t.Run(fmt.Sprintf("crashAfterPass%02d", crash), func(t *testing.T) {
			h := newPFHarness(t, pfOpts{})
			h.request("dc2")
			// The run's statements are those before the claim is released
			// (Publishing or terminal); after it the manager's own recovery
			// re-fences and repoints the old source.
			var src []string
			for i := 0; i < 80; i++ {
				h.pass()
				if i == crash {
					h.newProcess()
				}
				pf := h.pf()
				if src == nil && pf != nil && (pf.Phase == v1alpha1.PlannedFailoverPhasePublishing || pf.Result != nil) {
					src = h.src.statements()
				}
				if pf != nil && pf.Result != nil {
					break
				}
				h.tick()
			}
			pf := h.pf()
			if pf == nil || pf.Result == nil || pf.Result.Outcome == "" {
				t.Fatalf("no defined outcome: %+v", pf)
			}
			tgt := h.tgt.statements()
			if countOf(src, "SetSuperReadOnly:on") > 1 || countOf(tgt, "ResetReplicaAll") > 1 || countOf(tgt, "KillAppConnections") != 0 {
				t.Fatalf("outcome %s; source %v; target %v", pf.Result.Outcome, src, tgt)
			}
		})
	}
}

func TestPF_HappyPath_ThreeSite(t *testing.T) {
	for _, role := range []state.SiteRole{state.SiteRolePrimaryCandidate, state.SiteRoleDROnly, state.SiteRoleReadOnly} {
		t.Run(string(role), func(t *testing.T) {
			h := newPFHarness(t, pfOpts{third: role})
			h.request("dc2")
			h.wantOutcome(h.runUntil(60, terminal), v1alpha1.PlannedFailoverOutcomeSucceeded, "")
			if h.tgt.isReadOnly() || h.third.writableGrants() != 0 {
				t.Fatalf("dc2 read-only %v; dc3 grants %d", h.tgt.isReadOnly(), h.third.writableGrants())
			}
		})
	}
}

// #218 during the lag wait (the Dragonfly-wait variant's MySQL half): the
// target regresses under a new server_uuid while the run waits. The
// Promoting intent pins the new pod's uuid, the regressed set is caught by
// the lag proof, and nothing is promoted until it holds F.
func TestPF_218_TargetRegressedDuringLagWait(t *testing.T) {
	h := newPFHarness(t, pfOpts{})
	h.tgt.setGtidExecuted(pfUUIDA + ":1-50")
	h.request("dc2")
	h.runUntil(30, inPhase(v1alpha1.PlannedFailoverPhaseWaitingForLag))
	regressed := pfUUIDA + ":1-40"
	h.tgt.replacePod(podReplacement{serverUUID: "dddddddd-0000-0000-0000-000000000004", gtidExecuted: &regressed})
	for range 3 {
		h.pass()
		h.tick()
	}
	if h.tgt.writableGrants() != 0 || h.pf().Phase == v1alpha1.PlannedFailoverPhasePromoting {
		t.Fatalf("a regressed target was promoted or reached Promoting: %+v", h.pf())
	}
	h.tgt.setGtidExecuted(pfF) // it catches up
	pf := h.runUntil(60, terminal)
	h.wantOutcome(pf, v1alpha1.PlannedFailoverOutcomeSucceeded, "")
	if pf.Promotion.TargetServerUUID != "dddddddd-0000-0000-0000-000000000004" {
		t.Fatalf("the intent pinned %q, want the replacement's uuid", pf.Promotion.TargetServerUUID)
	}
}

// Row 8: a status stall ends the run durably outside /status: after the
// two-minute stall deadline the conditioned terminal marker names the run,
// the claim is released, and the run sends nothing more.
func TestPF_StatusStallWritesTerminalMarker(t *testing.T) {
	h := newPFHarness(t, pfOpts{})
	h.tgt.setGtidExecuted(pfUUIDA + ":1-50") // lags: the run holds the claim
	h.request("dc2")
	pf := h.runUntil(30, inPhase(v1alpha1.PlannedFailoverPhaseWaitingForLag))
	h.store.DenyStatus(1 << 30)
	for range 12 {
		h.clk.Advance(15 * time.Second)
		h.pass()
	}
	marker := h.store.Object().GetAnnotations()[controller.PlannedFailoverTerminalAnnotation]
	if m, ok := controller.ParseTerminalMarker(marker); !ok || m.RunID != pf.RunID {
		t.Fatalf("no terminal marker for %s after the stall deadline: %q", pf.RunID, marker)
	}
	if _, _, ok := h.held(); ok {
		t.Fatal("the claim outlived the terminal marker")
	}
	before := len(h.src.statements()) + len(h.tgt.statements())
	for range 5 {
		h.pass()
	}
	if after := len(h.src.statements()) + len(h.tgt.statements()); after != before {
		t.Fatal("a marked run kept sending statements")
	}
}

// Row 8: a status write that timed out unsettled and commits late is fenced
// by the terminal marker that ended the run meanwhile.
func TestPF_LateStatusCommitFencedByMarker(t *testing.T) {
	h := newPFHarness(t, pfOpts{})
	h.src.killConnectionResults = []int{3, 3, 3, 3, 3, 3}
	h.request("dc2")
	h.runUntil(30, inPhase(v1alpha1.PlannedFailoverPhaseDraining))
	h.store.LateWrites(1, 600) // the next write commits ten minutes late, if it still can
	h.store.SettleOutage(3)    // and its settlement cannot read: Unknown
	h.tgt.setGtidExecuted(pfUUIDA + ":1-50")
	for range 3 {
		h.clk.Advance(2 * time.Second) // past the 3s drain timeout: the WaitingForLag write is the late one
		h.pass()
	}
	h.store.DenyStatus(1 << 30)
	for range 12 {
		h.clk.Advance(15 * time.Second)
		h.pass()
	}
	if m := h.store.Object().GetAnnotations()[controller.PlannedFailoverTerminalAnnotation]; m == "" {
		t.Fatal("setup: no terminal marker")
	}
	h.clk.Advance(10 * time.Minute)
	h.store.BeginPoll(int(h.clk.Now().Unix()))
	refused := false
	for _, l := range h.store.LogStrings() {
		if strings.Contains(l, "lateCommit → refused") {
			refused = true
		}
		if strings.Contains(l, "lateCommit → committed") {
			t.Fatalf("a late status write committed over the terminal marker:\n%s", strings.Join(h.store.LogStrings(), "\n"))
		}
	}
	if !refused {
		t.Fatalf("setup: no late commit was attempted:\n%s", strings.Join(h.store.LogStrings(), "\n"))
	}
}
