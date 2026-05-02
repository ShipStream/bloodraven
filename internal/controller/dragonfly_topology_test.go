package controller

import (
	"context"
	"errors"
	"log/slog"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	io_prometheus_client "github.com/prometheus/client_model/go"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/tools/record"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"

	v1alpha1 "github.com/shipstream/bloodraven/api/v1alpha1"
	"github.com/shipstream/bloodraven/internal/dragonfly"
	"github.com/shipstream/bloodraven/internal/metrics"
)

// fakeDragonflyConn is a hand-rolled stub for DragonflyConnection. Each
// site addr maps to a scripted ReplicationInfo and an optional error.
type fakeDragonflyConn struct {
	infoErr error
	info    dragonfly.ReplicationInfo
	persist dragonfly.PersistenceInfo

	replicaOfErr  error
	replicaOfArgs []string

	replicaOfNoOneErr error

	replTakeoverErr error
	saveCalls       int
	saveErr         error

	clientKillTypes []string
	clientKillErr   error

	closed bool
}

func (c *fakeDragonflyConn) Ping(_ context.Context) error { return nil }

func (c *fakeDragonflyConn) InfoReplication(_ context.Context) (dragonfly.ReplicationInfo, error) {
	return c.info, c.infoErr
}

func (c *fakeDragonflyConn) InfoPersistence(_ context.Context) (dragonfly.PersistenceInfo, error) {
	return c.persist, nil
}

func (c *fakeDragonflyConn) ReplicaOf(_ context.Context, host string, port int32) error {
	c.replicaOfArgs = []string{host, formatInt(port)}
	return c.replicaOfErr
}

func (c *fakeDragonflyConn) ReplicaOfNoOne(_ context.Context) error { return c.replicaOfNoOneErr }

func (c *fakeDragonflyConn) ReplTakeover(_ context.Context, _ time.Duration) error {
	return c.replTakeoverErr
}

func (c *fakeDragonflyConn) Save(_ context.Context) error {
	c.saveCalls++
	return c.saveErr
}

func (c *fakeDragonflyConn) ClientKillType(_ context.Context, kind string) error {
	c.clientKillTypes = append(c.clientKillTypes, kind)
	return c.clientKillErr
}

func (c *fakeDragonflyConn) Close() error { c.closed = true; return nil }

// formatInt formats an int32 port for the test stubs. Mirrors the
// real strconv.Itoa(int(p)) used in production. Previously hard-coded
// to 6379 (the default port), which caused tests using a non-default
// port to silently produce empty REPLICAOF args — a fake-vs-real
// divergence noted in B22.
func formatInt(p int32) string { return strconv.Itoa(int(p)) }

// fakeConnector returns connections programmed by the test. Keyed on the
// addr the manager dials.
type fakeConnector struct {
	mu       sync.Mutex
	byAdd    map[string]*fakeDragonflyConn
	queues   map[string][]*fakeDragonflyConn
	dialErrs map[string]error
	dials    map[string]int
}

func newFakeConnector() *fakeConnector {
	return &fakeConnector{
		byAdd:    map[string]*fakeDragonflyConn{},
		queues:   map[string][]*fakeDragonflyConn{},
		dialErrs: map[string]error{},
		dials:    map[string]int{},
	}
}

func (f *fakeConnector) program(addr string, conn *fakeDragonflyConn) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.byAdd[addr] = conn
}

// programQueue programs a FIFO of conns for an addr. Each call to
// connect(addr, ...) returns and removes the front of the queue. After
// the queue empties the next dial errors. Use this to verify code paths
// that re-dial on the same addr (e.g. emergency promote dialing fresh
// after a REPLTAKEOVER error).
func (f *fakeConnector) programQueue(addr string, conns ...*fakeDragonflyConn) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.queues[addr] = append(f.queues[addr], conns...)
}

func (f *fakeConnector) dialFails(addr string, err error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.dialErrs[addr] = err
}

// dialCount returns how many times connect was called for the addr.
func (f *fakeConnector) dialCount(addr string) int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.dials[addr]
}

func (f *fakeConnector) connect(_ context.Context, addr, _ string) (DragonflyConnection, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.dials[addr]++
	if err, ok := f.dialErrs[addr]; ok {
		return nil, err
	}
	if q, ok := f.queues[addr]; ok && len(q) > 0 {
		next := q[0]
		f.queues[addr] = q[1:]
		return next, nil
	}
	conn, ok := f.byAdd[addr]
	if !ok {
		return nil, errors.New("no programmed conn for addr " + addr)
	}
	return conn, nil
}

// fgWithDragonflyEnabledAndActive returns a baseline FG with Dragonfly
// enabled and an explicit ActiveSite="dc1" so the manager has something
// to align replication against.
func fgWithDragonflyEnabledAndActive() *v1alpha1.MysqlFailoverGroup {
	fg := fgWithDragonfly()
	fg.Status.ActiveSite = "dc1"
	return fg
}

// helpers for installing/getting the FG into a fake client.
func newDragonflyFakeClient(fg *v1alpha1.MysqlFailoverGroup) (*fake.ClientBuilder, types.NamespacedName) {
	scheme := testScheme()
	cb := fake.NewClientBuilder().WithScheme(scheme).
		WithStatusSubresource(&v1alpha1.MysqlFailoverGroup{}).
		WithObjects(fg, newTestSecret())
	return cb, types.NamespacedName{Name: fg.Name, Namespace: fg.Namespace}
}

func TestDragonflyManager_Tick_HappyPath(t *testing.T) {
	fg := fgWithDragonflyEnabledAndActive()
	cb, key := newDragonflyFakeClient(fg)
	c := cb.Build()
	rec := record.NewFakeRecorder(8)
	mgr := NewDragonflyManager(c, rec, slog.Default(), key, 50*time.Millisecond)

	dc1Conn := &fakeDragonflyConn{
		info: dragonfly.ReplicationInfo{Role: "master", MasterReplOffset: 100},
	}
	dc2Conn := &fakeDragonflyConn{
		info: dragonfly.ReplicationInfo{
			Role:                   "slave",
			MasterHost:             "lion-dragonfly-dc1.shared-lion.svc.cluster.local",
			MasterPort:             6379,
			MasterLinkStatus:       "up",
			MasterLastIOSecondsAgo: 0,
			SlaveReplOffset:        100,
			MasterReplOffset:       100,
		},
	}
	conn := newFakeConnector()
	conn.program("lion-dragonfly-dc1.shared-lion.svc.cluster.local:6379", dc1Conn)
	conn.program("lion-dragonfly-dc2.shared-lion.svc.cluster.local:6379", dc2Conn)
	mgr.SetConnector(conn.connect)

	mgr.Tick(context.Background())

	var got v1alpha1.MysqlFailoverGroup
	if err := c.Get(context.Background(), key, &got); err != nil {
		t.Fatalf("get fg: %v", err)
	}
	if got.Status.Dragonfly == nil {
		t.Fatal("status.dragonfly not patched")
	}
	if got.Status.Dragonfly.ActiveSite != "dc1" {
		t.Errorf("ActiveSite = %q, want dc1", got.Status.Dragonfly.ActiveSite)
	}
	if got.Status.Dragonfly.Phase != v1alpha1.DragonflyPhaseReady {
		t.Errorf("Phase = %q, want Ready", got.Status.Dragonfly.Phase)
	}
	if len(got.Status.Dragonfly.Sites) != 2 {
		t.Fatalf("Sites len = %d, want 2", len(got.Status.Dragonfly.Sites))
	}
	gotRoles := map[string]v1alpha1.DragonflyRole{}
	for _, s := range got.Status.Dragonfly.Sites {
		gotRoles[s.Name] = s.Role
	}
	if gotRoles["dc1"] != v1alpha1.DragonflyRoleMaster {
		t.Errorf("dc1 role = %q, want master", gotRoles["dc1"])
	}
	if gotRoles["dc2"] != v1alpha1.DragonflyRoleReplica {
		t.Errorf("dc2 role = %q, want replica", gotRoles["dc2"])
	}
}

func TestDragonflyManager_Tick_ReplicaNeverSyncedNotReady(t *testing.T) {
	fg := fgWithDragonflyEnabledAndActive()
	cb, key := newDragonflyFakeClient(fg)
	c := cb.Build()
	mgr := NewDragonflyManager(c, record.NewFakeRecorder(8), slog.Default(), key, 50*time.Millisecond)

	conn := newFakeConnector()
	conn.program("lion-dragonfly-dc1.shared-lion.svc.cluster.local:6379", &fakeDragonflyConn{
		info: dragonfly.ReplicationInfo{Role: "master", MasterReplOffset: 100},
	})
	conn.program("lion-dragonfly-dc2.shared-lion.svc.cluster.local:6379", &fakeDragonflyConn{
		info: dragonfly.ReplicationInfo{
			Role:                   "slave",
			MasterHost:             "lion-dragonfly-dc1.shared-lion.svc.cluster.local",
			MasterPort:             6379,
			MasterLinkStatus:       "up",
			MasterLastIOSecondsAgo: -1,
			SlaveReplOffset:        100,
		},
	})
	mgr.SetConnector(conn.connect)

	mgr.Tick(context.Background())

	var got v1alpha1.MysqlFailoverGroup
	if err := c.Get(context.Background(), key, &got); err != nil {
		t.Fatalf("get fg: %v", err)
	}
	if got.Status.Dragonfly.Phase != v1alpha1.DragonflyPhaseConfiguringReplication {
		t.Errorf("Phase = %q, want ConfiguringReplication", got.Status.Dragonfly.Phase)
	}
	for _, s := range got.Status.Dragonfly.Sites {
		if s.Name == "dc2" {
			if s.Ready {
				t.Fatal("dc2 Ready = true, want false for never-synced replica")
			}
			if s.LastIOSecondsAgo != -1 {
				t.Fatalf("dc2 LastIOSecondsAgo = %d, want -1", s.LastIOSecondsAgo)
			}
			return
		}
	}
	t.Fatal("dc2 status not found")
}

func TestDragonflyManager_Tick_StaleMasterEmitsEvent(t *testing.T) {
	fg := fgWithDragonflyEnabledAndActive()
	cb, key := newDragonflyFakeClient(fg)
	c := cb.Build()
	rec := record.NewFakeRecorder(8)
	mgr := NewDragonflyManager(c, rec, slog.Default(), key, 50*time.Millisecond)

	// Both sites report master role; only dc1 should be valid master.
	// dc2 has connected_slaves > 0 so it does NOT pass the auto-rejoin
	// gate — only the StaleMasterDetected event should fire.
	dc1Conn := &fakeDragonflyConn{info: dragonfly.ReplicationInfo{Role: "master", MasterReplOffset: 100}}
	dc2Conn := &fakeDragonflyConn{info: dragonfly.ReplicationInfo{Role: "master", MasterReplOffset: 50, ConnectedSlaves: 1}}
	conn := newFakeConnector()
	conn.program("lion-dragonfly-dc1.shared-lion.svc.cluster.local:6379", dc1Conn)
	conn.program("lion-dragonfly-dc2.shared-lion.svc.cluster.local:6379", dc2Conn)
	mgr.SetConnector(conn.connect)

	mgr.Tick(context.Background())

	var got v1alpha1.MysqlFailoverGroup
	if err := c.Get(context.Background(), key, &got); err != nil {
		t.Fatalf("get fg: %v", err)
	}
	if got.Status.Dragonfly.Phase != v1alpha1.DragonflyPhaseDegraded {
		t.Errorf("Phase = %q, want Degraded", got.Status.Dragonfly.Phase)
	}

	events := drainEventsCh(rec.Events, 8, 100*time.Millisecond)
	foundStale := false
	foundReconfigured := false
	for _, ev := range events {
		if contains(ev, ReasonDragonflyStaleMasterDetected) {
			foundStale = true
		}
		if contains(ev, ReasonDragonflyOldSiteReconfigured) {
			foundReconfigured = true
		}
	}
	if !foundStale {
		t.Errorf("did not see DragonflyStaleMasterDetected event; events=%v", events)
	}
	if foundReconfigured {
		t.Errorf("saw DragonflyOldSiteReconfigured event despite connected_slaves>0; events=%v", events)
	}
	if dc2Conn.replicaOfArgs != nil {
		t.Errorf("expected no REPLICAOF on stale master with connected_slaves>0, got %v", dc2Conn.replicaOfArgs)
	}
}

func TestDragonflyManager_Tick_StaleMasterAutoReconfigures(t *testing.T) {
	// Stale master with connected_slaves=0 AND master_repl_offset=0:
	// provably never accepted writes. Manager should auto-attach it as
	// a replica of the active master and emit the reconfigured event.
	fg := fgWithDragonflyEnabledAndActive()
	cb, key := newDragonflyFakeClient(fg)
	c := cb.Build()
	rec := record.NewFakeRecorder(8)
	mgr := NewDragonflyManager(c, rec, slog.Default(), key, 50*time.Millisecond)

	dc1Conn := &fakeDragonflyConn{info: dragonfly.ReplicationInfo{Role: "master", MasterReplOffset: 100}}
	dc2Conn := &fakeDragonflyConn{info: dragonfly.ReplicationInfo{Role: "master", MasterReplOffset: 0, ConnectedSlaves: 0}}
	conn := newFakeConnector()
	conn.program("lion-dragonfly-dc1.shared-lion.svc.cluster.local:6379", dc1Conn)
	conn.program("lion-dragonfly-dc2.shared-lion.svc.cluster.local:6379", dc2Conn)
	mgr.SetConnector(conn.connect)

	mgr.Tick(context.Background())

	if dc2Conn.replicaOfArgs == nil {
		t.Fatal("expected REPLICAOF on stale master with connected_slaves=0,master_repl_offset=0")
	}
	if !contains(dc2Conn.replicaOfArgs[0], "lion-dragonfly-dc1.shared-lion.svc.cluster.local") {
		t.Errorf("REPLICAOF host = %q, want dc1 svc", dc2Conn.replicaOfArgs[0])
	}

	events := drainEventsCh(rec.Events, 8, 100*time.Millisecond)
	foundReconfigured := false
	for _, ev := range events {
		if contains(ev, ReasonDragonflyOldSiteReconfigured) {
			foundReconfigured = true
		}
	}
	if !foundReconfigured {
		t.Errorf("did not see DragonflyOldSiteReconfigured event; events=%v", events)
	}
}

func TestDragonflyManager_Tick_DialFailureMarksUnreachable(t *testing.T) {
	fg := fgWithDragonflyEnabledAndActive()
	cb, key := newDragonflyFakeClient(fg)
	c := cb.Build()
	rec := record.NewFakeRecorder(8)
	mgr := NewDragonflyManager(c, rec, slog.Default(), key, 50*time.Millisecond)

	dc1Conn := &fakeDragonflyConn{info: dragonfly.ReplicationInfo{Role: "master"}}
	conn := newFakeConnector()
	conn.program("lion-dragonfly-dc1.shared-lion.svc.cluster.local:6379", dc1Conn)
	conn.dialFails("lion-dragonfly-dc2.shared-lion.svc.cluster.local:6379", errors.New("conn refused"))
	mgr.SetConnector(conn.connect)

	mgr.Tick(context.Background())

	var got v1alpha1.MysqlFailoverGroup
	if err := c.Get(context.Background(), key, &got); err != nil {
		t.Fatalf("get fg: %v", err)
	}
	roles := map[string]v1alpha1.DragonflyRole{}
	for _, s := range got.Status.Dragonfly.Sites {
		roles[s.Name] = s.Role
	}
	if roles["dc2"] != v1alpha1.DragonflyRoleUnreachable {
		t.Errorf("dc2 role = %q, want unreachable", roles["dc2"])
	}
	if got.Status.Dragonfly.Phase != v1alpha1.DragonflyPhaseDegraded {
		t.Errorf("Phase = %q, want Degraded with one unreachable site", got.Status.Dragonfly.Phase)
	}
}

func TestDragonflyManager_Tick_PausedSkipsReplicationReconfigure(t *testing.T) {
	fg := fgWithDragonflyEnabledAndActive()
	cb, key := newDragonflyFakeClient(fg)
	c := cb.Build()
	rec := record.NewFakeRecorder(8)
	mgr := NewDragonflyManager(c, rec, slog.Default(), key, 50*time.Millisecond)
	mgr.SetPaused(true)

	dc1Conn := &fakeDragonflyConn{info: dragonfly.ReplicationInfo{Role: "master", MasterReplOffset: 100}}
	// dc2 is unconfigured (empty role) — manager would normally REPLICAOF it.
	dc2Conn := &fakeDragonflyConn{info: dragonfly.ReplicationInfo{}}
	conn := newFakeConnector()
	conn.program("lion-dragonfly-dc1.shared-lion.svc.cluster.local:6379", dc1Conn)
	conn.program("lion-dragonfly-dc2.shared-lion.svc.cluster.local:6379", dc2Conn)
	mgr.SetConnector(conn.connect)

	mgr.Tick(context.Background())

	if dc2Conn.replicaOfArgs != nil {
		t.Errorf("paused manager unexpectedly issued REPLICAOF: %v", dc2Conn.replicaOfArgs)
	}
}

func TestDragonflyManager_Tick_ReplicasOfMisalignedReplica(t *testing.T) {
	fg := fgWithDragonflyEnabledAndActive()
	cb, key := newDragonflyFakeClient(fg)
	c := cb.Build()
	rec := record.NewFakeRecorder(8)
	mgr := NewDragonflyManager(c, rec, slog.Default(), key, 50*time.Millisecond)

	dc1Conn := &fakeDragonflyConn{info: dragonfly.ReplicationInfo{Role: "master", MasterReplOffset: 100}}
	// dc2 thinks its master is somewhere else — manager should re-point it.
	dc2Conn := &fakeDragonflyConn{info: dragonfly.ReplicationInfo{
		Role:             "slave",
		MasterHost:       "10.0.0.99",
		MasterPort:       6379,
		MasterLinkStatus: "up",
	}}
	conn := newFakeConnector()
	conn.program("lion-dragonfly-dc1.shared-lion.svc.cluster.local:6379", dc1Conn)
	conn.program("lion-dragonfly-dc2.shared-lion.svc.cluster.local:6379", dc2Conn)
	mgr.SetConnector(conn.connect)

	mgr.Tick(context.Background())

	if dc2Conn.replicaOfArgs == nil {
		t.Fatal("manager did not issue REPLICAOF on misaligned replica")
	}
	if !contains(dc2Conn.replicaOfArgs[0], "lion-dragonfly-dc1.shared-lion.svc.cluster.local") {
		t.Errorf("REPLICAOF host = %q, want dc1 svc", dc2Conn.replicaOfArgs[0])
	}
	if dc2Conn.replicaOfArgs[1] != "6379" {
		t.Errorf("REPLICAOF port = %q, want 6379", dc2Conn.replicaOfArgs[1])
	}
}

// TestDragonflyManager_Tick_ReplicaWithLinkDownReissuesReplicaOf is a
// regression test for the post-master-pod-restart case: a replica that
// is configured with the right master host:port but whose underlying
// TCP connection has dropped (master_link_status="down"). Before the
// fix, reconcileReplication short-circuited on the host:port match and
// the replica silently stayed disconnected forever — same shape as
// upstream issue #2044. The fix re-issues REPLICAOF whenever the link
// is anything other than "up".
func TestDragonflyManager_Tick_ReplicaWithLinkDownReissuesReplicaOf(t *testing.T) {
	fg := fgWithDragonflyEnabledAndActive()
	cb, key := newDragonflyFakeClient(fg)
	c := cb.Build()
	rec := record.NewFakeRecorder(8)
	mgr := NewDragonflyManager(c, rec, slog.Default(), key, 50*time.Millisecond)

	dc1Conn := &fakeDragonflyConn{info: dragonfly.ReplicationInfo{Role: "master", MasterReplOffset: 100}}
	// dc2 points at the right master but the link is broken.
	dc2Conn := &fakeDragonflyConn{info: dragonfly.ReplicationInfo{
		Role:                   "slave",
		MasterHost:             "lion-dragonfly-dc1.shared-lion.svc.cluster.local",
		MasterPort:             6379,
		MasterLinkStatus:       "down",
		MasterLastIOSecondsAgo: 60,
	}}
	conn := newFakeConnector()
	conn.program("lion-dragonfly-dc1.shared-lion.svc.cluster.local:6379", dc1Conn)
	conn.program("lion-dragonfly-dc2.shared-lion.svc.cluster.local:6379", dc2Conn)
	mgr.SetConnector(conn.connect)

	mgr.Tick(context.Background())

	if dc2Conn.replicaOfArgs == nil {
		t.Fatal("manager did not re-issue REPLICAOF on replica with link=down")
	}
	if !contains(dc2Conn.replicaOfArgs[0], "lion-dragonfly-dc1.shared-lion.svc.cluster.local") {
		t.Errorf("REPLICAOF host = %q, want dc1 svc", dc2Conn.replicaOfArgs[0])
	}
}

func TestDragonflyManager_TryEmergencyPromote_Success(t *testing.T) {
	fg := fgWithDragonflyEnabledAndActive()
	cb, key := newDragonflyFakeClient(fg)
	c := cb.Build()
	rec := record.NewFakeRecorder(8)
	mgr := NewDragonflyManager(c, rec, slog.Default(), key, 50*time.Millisecond)

	target := &fakeDragonflyConn{info: dragonfly.ReplicationInfo{Role: "master"}}
	conn := newFakeConnector()
	conn.program("lion-dragonfly-dc2.shared-lion.svc.cluster.local:6379", target)
	mgr.SetConnector(conn.connect)

	mgr.TryEmergencyPromote(context.Background(), "dc2", "dc1")

	var got v1alpha1.MysqlFailoverGroup
	if err := c.Get(context.Background(), key, &got); err != nil {
		t.Fatalf("get fg: %v", err)
	}
	if got.Status.Dragonfly == nil || got.Status.Dragonfly.LastPromotionTarget != "dc2" {
		t.Errorf("lastPromotionTarget = %+v", got.Status.Dragonfly)
	}

	select {
	case ev := <-rec.Events:
		if !contains(ev, ReasonDragonflyPromotionCompleted) {
			t.Errorf("event = %q, want %q", ev, ReasonDragonflyPromotionCompleted)
		}
	case <-time.After(100 * time.Millisecond):
		t.Fatal("no DragonflyPromotionCompleted event emitted")
	}
}

func TestDragonflyManager_TryEmergencyPromote_TakeoverFailsFallsBack(t *testing.T) {
	fg := fgWithDragonflyEnabledAndActive()
	cb, key := newDragonflyFakeClient(fg)
	c := cb.Build()
	rec := record.NewFakeRecorder(8)
	mgr := NewDragonflyManager(c, rec, slog.Default(), key, 50*time.Millisecond)

	target := &fakeDragonflyConn{
		info:            dragonfly.ReplicationInfo{Role: "slave"},
		replTakeoverErr: errors.New("replication still active"),
	}
	conn := newFakeConnector()
	conn.program("lion-dragonfly-dc2.shared-lion.svc.cluster.local:6379", target)
	mgr.SetConnector(conn.connect)

	mgr.TryEmergencyPromote(context.Background(), "dc2", "dc1")

	// Drain at most 4 events; we expect a Warning DragonflyPromotionCompleted
	// (best-effort fallback path).
	got := drainEventsCh(rec.Events, 4, 100*time.Millisecond)
	found := false
	for _, ev := range got {
		if contains(ev, "REPLICAOF NO ONE") {
			found = true
			break
		}
	}
	if !found {
		t.Errorf("did not see REPLICAOF NO ONE fallback event; events=%v", got)
	}
}

// TestDragonflyManager_TryEmergencyPromote_BothPathsFailed_StampsFailureEvent
// regression-tests B23: when REPLTAKEOVER and the fallback REPLICAOF
// NO ONE both fail, the manager must emit a Warning event clearly
// stating both attempts failed and increment the failed-promotion
// counter, so operators can debug the cache-continuity loss.
func TestDragonflyManager_TryEmergencyPromote_BothPathsFailed_StampsFailureEvent(t *testing.T) {
	fg := fgWithDragonflyEnabledAndActive()
	cb, key := newDragonflyFakeClient(fg)
	c := cb.Build()
	rec := record.NewFakeRecorder(8)
	mgr := NewDragonflyManager(c, rec, slog.Default(), key, 50*time.Millisecond)

	// First conn fails REPLTAKEOVER. Re-dial succeeds; second conn
	// fails REPLICAOF NO ONE via replicaOfErr.
	first := &fakeDragonflyConn{
		info:            dragonfly.ReplicationInfo{Role: "slave"},
		replTakeoverErr: errors.New("replication still active"),
	}
	second := &fakeDragonflyConn{
		info:              dragonfly.ReplicationInfo{Role: "slave"},
		replicaOfNoOneErr: errors.New("REPLICAOF NO ONE rejected"),
	}
	conn := newFakeConnector()
	conn.programQueue("lion-dragonfly-dc2.shared-lion.svc.cluster.local:6379", first, second)
	mgr.SetConnector(conn.connect)

	mgr.TryEmergencyPromote(context.Background(), "dc2", "dc1")

	// Drain events; expect one Warning DragonflyPromotionFailed
	// containing both verbs.
	got := drainEventsCh(rec.Events, 4, 100*time.Millisecond)
	found := false
	for _, ev := range got {
		if strings.Contains(ev, "REPLTAKEOVER") && strings.Contains(ev, "REPLICAOF NO ONE") {
			found = true
			break
		}
	}
	if !found {
		t.Errorf("expected event mentioning both REPLTAKEOVER and REPLICAOF NO ONE failed; got %v", got)
	}
}

// TestDragonflyManager_TryEmergencyPromote_RedialsAfterTakeoverError
// regression-tests the bug where the emergency promotion path reused the
// post-REPLTAKEOVER connection for REPLICAOF NO ONE. RESP has no
// req/reply correlation: a late server reply for the failed REPLTAKEOVER
// could be consumed as the REPLICAOF NO ONE reply, producing a spurious
// success or failure event. The fix re-dials a fresh connection.
func TestDragonflyManager_TryEmergencyPromote_RedialsAfterTakeoverError(t *testing.T) {
	fg := fgWithDragonflyEnabledAndActive()
	cb, key := newDragonflyFakeClient(fg)
	c := cb.Build()
	rec := record.NewFakeRecorder(8)
	mgr := NewDragonflyManager(c, rec, slog.Default(), key, 50*time.Millisecond)

	first := &fakeDragonflyConn{
		info:            dragonfly.ReplicationInfo{Role: "slave"},
		replTakeoverErr: errors.New("replication still active"),
	}
	second := &fakeDragonflyConn{info: dragonfly.ReplicationInfo{Role: "master"}}
	conn := newFakeConnector()
	addr := "lion-dragonfly-dc2.shared-lion.svc.cluster.local:6379"
	conn.programQueue(addr, first, second)
	mgr.SetConnector(conn.connect)

	mgr.TryEmergencyPromote(context.Background(), "dc2", "dc1")

	if !first.closed {
		t.Errorf("first conn (post-REPLTAKEOVER error) was not closed")
	}
	if dials := conn.dialCount(addr); dials < 2 {
		t.Errorf("dialCount(%q) = %d, want >= 2 (a fresh dial after REPLTAKEOVER error)", addr, dials)
	}
}

// TestDragonflyManager_TryEmergencyPromote_DemoteFails_DoesNotRestoreSourceTraffic
// regression-tests B5: applyEmergencyPromotionLabels used to log-and-
// continue on demote failure, then restore the source's traffic
// label — leaving a pod with role=master+traffic=enabled selectable by
// the active Service alongside the new master. Split-brain at the
// routing layer.
//
// We inject an interceptor that fails Updates of the source pod when
// dragonfly-role=replica is being set, and assert the source's traffic
// label remains stripped.
func TestDragonflyManager_TryEmergencyPromote_DemoteFails_DoesNotRestoreSourceTraffic(t *testing.T) {
	fg := fgWithDragonflyEnabledAndActive()
	sourcePod := makeDragonflyPod(fg.Name, "dc1", "master", true)
	targetPod := makeDragonflyPod(fg.Name, "dc2", "replica", true)
	scheme := testScheme()
	c := fake.NewClientBuilder().WithScheme(scheme).
		WithStatusSubresource(&v1alpha1.MysqlFailoverGroup{}).
		WithObjects(fg, newTestSecret(), sourcePod, targetPod).
		WithInterceptorFuncs(interceptor.Funcs{
			Update: func(ctx context.Context, underlying client.WithWatch, obj client.Object, opts ...client.UpdateOption) error {
				if pod, ok := obj.(*corev1.Pod); ok && pod.Name == sourcePod.Name {
					if pod.Labels[labelDragonflyRole] == "replica" {
						return errors.New("simulated demote-patch failure")
					}
				}
				return underlying.Update(ctx, obj, opts...)
			},
		}).
		Build()
	key := types.NamespacedName{Name: fg.Name, Namespace: fg.Namespace}
	rec := record.NewFakeRecorder(8)
	mgr := NewDragonflyManager(c, rec, slog.Default(), key, 50*time.Millisecond)

	target := &fakeDragonflyConn{info: dragonfly.ReplicationInfo{Role: "master"}}
	source := &fakeDragonflyConn{info: dragonfly.ReplicationInfo{Role: "master"}}
	conn := newFakeConnector()
	conn.program("lion-dragonfly-dc2.shared-lion.svc.cluster.local:6379", target)
	conn.program("lion-dragonfly-dc1.shared-lion.svc.cluster.local:6379", source)
	mgr.SetConnector(conn.connect)

	mgr.TryEmergencyPromote(context.Background(), "dc2", "dc1")

	var gotSource corev1.Pod
	if err := c.Get(context.Background(), types.NamespacedName{Name: sourcePod.Name, Namespace: sourcePod.Namespace}, &gotSource); err != nil {
		t.Fatalf("get source: %v", err)
	}
	if _, has := gotSource.Labels[labelDragonflyTraffic]; has {
		t.Errorf("emergency: source traffic label = %q, want absent (demote failed → must not restore traffic)", gotSource.Labels[labelDragonflyTraffic])
	}
}

func TestDragonflyManager_TryEmergencyPromote_TargetUnreachableDoesNotPanic(t *testing.T) {
	fg := fgWithDragonflyEnabledAndActive()
	cb, key := newDragonflyFakeClient(fg)
	c := cb.Build()
	rec := record.NewFakeRecorder(8)
	mgr := NewDragonflyManager(c, rec, slog.Default(), key, 50*time.Millisecond)

	conn := newFakeConnector()
	conn.dialFails("lion-dragonfly-dc2.shared-lion.svc.cluster.local:6379", errors.New("conn refused"))
	mgr.SetConnector(conn.connect)

	mgr.TryEmergencyPromote(context.Background(), "dc2", "dc1") // must not panic
}

func TestDragonflyManager_TryEmergencyPromote_AppliesLabelsAndKills(t *testing.T) {
	// Steady-state pods with default labels; emergency promote should
	// strip old-source traffic, set target=master+traffic, demote
	// old-source role + restore traffic, and CLIENT KILL the old source.
	fg := fgWithDragonflyEnabledAndActive()
	sourcePod := makeDragonflyPod(fg.Name, "dc1", "master", true)
	targetPod := makeDragonflyPod(fg.Name, "dc2", "replica", true)
	cb := fake.NewClientBuilder().WithScheme(testScheme()).
		WithStatusSubresource(&v1alpha1.MysqlFailoverGroup{}).
		WithObjects(fg, newTestSecret(), sourcePod, targetPod)
	c := cb.Build()
	key := types.NamespacedName{Name: fg.Name, Namespace: fg.Namespace}
	rec := record.NewFakeRecorder(8)
	mgr := NewDragonflyManager(c, rec, slog.Default(), key, 50*time.Millisecond)

	target := &fakeDragonflyConn{info: dragonfly.ReplicationInfo{Role: "master"}}
	source := &fakeDragonflyConn{info: dragonfly.ReplicationInfo{Role: "master"}}
	conn := newFakeConnector()
	conn.program("lion-dragonfly-dc2.shared-lion.svc.cluster.local:6379", target)
	conn.program("lion-dragonfly-dc1.shared-lion.svc.cluster.local:6379", source)
	mgr.SetConnector(conn.connect)

	mgr.TryEmergencyPromote(context.Background(), "dc2", "dc1")

	var gotSource, gotTarget corev1.Pod
	if err := c.Get(context.Background(), types.NamespacedName{Name: sourcePod.Name, Namespace: sourcePod.Namespace}, &gotSource); err != nil {
		t.Fatalf("get source: %v", err)
	}
	if err := c.Get(context.Background(), types.NamespacedName{Name: targetPod.Name, Namespace: targetPod.Namespace}, &gotTarget); err != nil {
		t.Fatalf("get target: %v", err)
	}
	if gotTarget.Labels[labelDragonflyRole] != "master" {
		t.Errorf("target role = %q, want master", gotTarget.Labels[labelDragonflyRole])
	}
	if gotTarget.Labels[labelDragonflyTraffic] != dragonflyTrafficEnabled {
		t.Errorf("target traffic = %q, want enabled", gotTarget.Labels[labelDragonflyTraffic])
	}
	if gotSource.Labels[labelDragonflyRole] != "replica" {
		t.Errorf("source role = %q, want replica", gotSource.Labels[labelDragonflyRole])
	}
	if gotSource.Labels[labelDragonflyTraffic] != dragonflyTrafficEnabled {
		t.Errorf("source traffic = %q, want enabled (restored after takeover)", gotSource.Labels[labelDragonflyTraffic])
	}
	if len(source.clientKillTypes) == 0 {
		t.Errorf("expected CLIENT KILL on old source, got none")
	}
}

// TestDragonflyManager_Tick_InfoFailureSetsGaugeToZero regression-tests
// B10: when the dial succeeded but InfoReplication failed (AUTH wedge,
// process mid-load, etc.), observe used to fall through without
// touching the gauge. Prometheus gauges retain their last value, so
// the up-but-stuck condition this metric exists to alert on would
// silently report up=1 indefinitely. The gauge must flip to 0 on
// InfoReplication error.
func TestDragonflyManager_Tick_InfoFailureSetsGaugeToZero(t *testing.T) {
	fg := fgWithDragonflyEnabledAndActive()
	cb, key := newDragonflyFakeClient(fg)
	c := cb.Build()
	rec := record.NewFakeRecorder(8)
	mgr := NewDragonflyManager(c, rec, slog.Default(), key, 50*time.Millisecond)

	// Pre-set the gauge to 1 to simulate a previous successful tick.
	metrics.DragonflySiteUp.WithLabelValues(fg.Name, "dc1").Set(1)
	metrics.DragonflySiteUp.WithLabelValues(fg.Name, "dc2").Set(1)

	// Both sites: dial OK but InfoReplication errors.
	dc1 := &fakeDragonflyConn{infoErr: errors.New("AUTH wedge")}
	dc2 := &fakeDragonflyConn{infoErr: errors.New("AUTH wedge")}
	conn := newFakeConnector()
	conn.program("lion-dragonfly-dc1.shared-lion.svc.cluster.local:6379", dc1)
	conn.program("lion-dragonfly-dc2.shared-lion.svc.cluster.local:6379", dc2)
	mgr.SetConnector(conn.connect)

	mgr.Tick(context.Background())

	// Read gauge values via Prometheus testing helper.
	for _, site := range []string{"dc1", "dc2"} {
		got := readGauge(t, metrics.DragonflySiteUp.WithLabelValues(fg.Name, site))
		if got != 0 {
			t.Errorf("DragonflySiteUp[%s] = %v, want 0 (InfoReplication failed)", site, got)
		}
	}
}

// readGauge extracts the current value of a Prometheus Gauge via the
// metric collector's Write+ToFloat64 pattern. Prometheus does not
// expose a direct Get method on Gauge, so this helper centralises the
// dance.
func readGauge(t *testing.T, g prometheus.Gauge) float64 {
	t.Helper()
	var m io_prometheus_client.Metric
	if err := g.Write(&m); err != nil {
		t.Fatalf("read gauge: %v", err)
	}
	if m.Gauge == nil {
		t.Fatalf("metric has no gauge value")
	}
	return *m.Gauge.Value
}

// TestDragonflyManager_Run_PanicRecovery regression-tests B12: a
// panic in observe (e.g. NPE from a nil-info from a faulty connector)
// used to propagate up and kill the goroutine for the operator's
// lifetime. The runner only restarts the manager on TopologyConfig
// changes, so observation+replication would silently stop until the
// pod restarted. safeTick must catch the panic, log + meter it, and
// continue the loop.
func TestDragonflyManager_Run_PanicRecovery(t *testing.T) {
	fg := fgWithDragonflyEnabledAndActive()
	cb, key := newDragonflyFakeClient(fg)
	c := cb.Build()
	rec := record.NewFakeRecorder(8)
	mgr := NewDragonflyManager(c, rec, slog.Default(), key, 5*time.Millisecond)

	// Connector that returns a non-nil conn whose InfoReplication
	// panics. observe -> conn.InfoReplication panics -> safeTick
	// recovers; the next tick attempts again.
	var ticks atomic.Int32
	mgr.SetConnector(func(_ context.Context, _ string, _ string) (DragonflyConnection, error) {
		ticks.Add(1)
		return &panicOnInfoConn{}, nil
	})

	ctx, cancel := context.WithTimeout(context.Background(), 80*time.Millisecond)
	defer cancel()
	mgr.Run(ctx) // returns when ctx times out
	if got := ticks.Load(); got < 2 {
		t.Errorf("connector called %d times; want >=2 (panic recovery must allow loop to continue)", got)
	}
}

// panicOnInfoConn is a DragonflyConnection whose InfoReplication
// panics. Used by the panic-recovery test to simulate a wedged Tick
// without bringing down the test process.
type panicOnInfoConn struct{}

func (panicOnInfoConn) Ping(_ context.Context) error { return nil }
func (panicOnInfoConn) InfoReplication(_ context.Context) (dragonfly.ReplicationInfo, error) {
	panic("simulated observe panic")
}
func (panicOnInfoConn) InfoPersistence(_ context.Context) (dragonfly.PersistenceInfo, error) {
	return dragonfly.PersistenceInfo{}, nil
}
func (panicOnInfoConn) ReplicaOf(_ context.Context, _ string, _ int32) error { return nil }
func (panicOnInfoConn) ReplicaOfNoOne(_ context.Context) error               { return nil }
func (panicOnInfoConn) ReplTakeover(_ context.Context, _ time.Duration) error {
	return nil
}
func (panicOnInfoConn) Save(_ context.Context) error                     { return nil }
func (panicOnInfoConn) ClientKillType(_ context.Context, _ string) error { return nil }
func (panicOnInfoConn) Close() error                                     { return nil }

func drainEventsCh(ch <-chan string, max int, wait time.Duration) []string {
	out := make([]string, 0, max)
	deadline := time.NewTimer(wait)
	defer deadline.Stop()
	for i := 0; i < max; i++ {
		select {
		case ev := <-ch:
			out = append(out, ev)
		case <-deadline.C:
			return out
		}
	}
	return out
}

// TestSplitHostPort regression-tests B20: the helper used to discard
// the parsed port and always return the defaultPort. Real callers go
// through dragonflyAddr which always uses the FG's configured port,
// so the bug was silent for the common path; but a non-default-port
// REPLICAOF (e.g. when callers eventually pass a separately-configured
// port) would silently rewire to the wrong port.
func TestSplitHostPort(t *testing.T) {
	cases := []struct {
		addr string
		dflt int32
		host string
		port int32
	}{
		{"foo.bar.svc:6379", 1234, "foo.bar.svc", 6379},
		{"foo.bar.svc:9999", 1234, "foo.bar.svc", 9999},
		{"foo.bar.svc", 1234, "foo.bar.svc", 1234}, // no port → default
		{"foo.bar.svc:", 1234, "foo.bar.svc", 1234},
		{"foo.bar.svc:notanint", 1234, "foo.bar.svc", 1234},
		{"foo.bar.svc:0", 1234, "foo.bar.svc", 1234}, // out of range
	}
	for _, tc := range cases {
		gotHost, gotPort := splitHostPort(tc.addr, tc.dflt)
		if gotHost != tc.host || gotPort != tc.port {
			t.Errorf("splitHostPort(%q, %d) = %q,%d; want %q,%d", tc.addr, tc.dflt, gotHost, gotPort, tc.host, tc.port)
		}
	}
}

func TestDragonflyManager_Tick_DisabledIsNoOp(t *testing.T) {
	fg := newTestFG()
	cb, key := newDragonflyFakeClient(fg)
	c := cb.Build()
	rec := record.NewFakeRecorder(8)
	mgr := NewDragonflyManager(c, rec, slog.Default(), key, 50*time.Millisecond)
	conn := newFakeConnector()
	mgr.SetConnector(conn.connect)

	mgr.Tick(context.Background())

	var got v1alpha1.MysqlFailoverGroup
	if err := c.Get(context.Background(), key, &got); err != nil {
		t.Fatalf("get fg: %v", err)
	}
	if got.Status.Dragonfly != nil {
		t.Errorf("disabled config wrote status.dragonfly: %+v", got.Status.Dragonfly)
	}
}

// Helpers below intentionally minimal — `contains` is shared with
// dragonfly_resources_test.go in the same package.

var _ = strings.Contains
var _ = metav1.Now
var _ = corev1.EventTypeNormal
