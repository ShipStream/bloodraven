package controller

import (
	"context"
	"errors"
	"log/slog"
	"strings"
	"sync"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/tools/record"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	v1alpha1 "github.com/shipstream/bloodraven/api/v1alpha1"
	"github.com/shipstream/bloodraven/internal/dragonfly"
)

// fakeDragonflyConn is a hand-rolled stub for DragonflyConnection. Each
// site addr maps to a scripted ReplicationInfo and an optional error.
type fakeDragonflyConn struct {
	infoErr error
	info    dragonfly.ReplicationInfo
	persist dragonfly.PersistenceInfo

	replicaOfErr  error
	replicaOfArgs []string

	replTakeoverErr error

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

func (c *fakeDragonflyConn) ReplicaOfNoOne(_ context.Context) error { return nil }

func (c *fakeDragonflyConn) ReplTakeover(_ context.Context, _ time.Duration) error {
	return c.replTakeoverErr
}

func (c *fakeDragonflyConn) Close() error { c.closed = true; return nil }

func formatInt(p int32) string {
	switch p {
	case 6379:
		return "6379"
	}
	return ""
}

// fakeConnector returns connections programmed by the test. Keyed on the
// addr the manager dials.
type fakeConnector struct {
	mu    sync.Mutex
	byAdd map[string]*fakeDragonflyConn
	dialErrs map[string]error
}

func newFakeConnector() *fakeConnector {
	return &fakeConnector{
		byAdd:    map[string]*fakeDragonflyConn{},
		dialErrs: map[string]error{},
	}
}

func (f *fakeConnector) program(addr string, conn *fakeDragonflyConn) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.byAdd[addr] = conn
}

func (f *fakeConnector) dialFails(addr string, err error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.dialErrs[addr] = err
}

func (f *fakeConnector) connect(_ context.Context, addr, _ string) (DragonflyConnection, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if err, ok := f.dialErrs[addr]; ok {
		return nil, err
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
			Role:             "slave",
			MasterHost:       "lion-dragonfly-dc1.shared-lion.svc.cluster.local",
			MasterPort:       6379,
			MasterLinkStatus: "up",
			SlaveReplOffset:  100,
			MasterReplOffset: 100,
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

func TestDragonflyManager_Tick_StaleMasterEmitsEvent(t *testing.T) {
	fg := fgWithDragonflyEnabledAndActive()
	cb, key := newDragonflyFakeClient(fg)
	c := cb.Build()
	rec := record.NewFakeRecorder(8)
	mgr := NewDragonflyManager(c, rec, slog.Default(), key, 50*time.Millisecond)

	// Both sites report master role; only dc1 should be valid master.
	dc1Conn := &fakeDragonflyConn{info: dragonfly.ReplicationInfo{Role: "master", MasterReplOffset: 100}}
	dc2Conn := &fakeDragonflyConn{info: dragonfly.ReplicationInfo{Role: "master", MasterReplOffset: 50}}
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

	// The Event recorder is buffered; non-blocking drain.
	select {
	case ev := <-rec.Events:
		if !contains(ev, ReasonDragonflyStaleMasterDetected) {
			t.Errorf("event = %q, want substring %q", ev, ReasonDragonflyStaleMasterDetected)
		}
	case <-time.After(100 * time.Millisecond):
		t.Fatal("no DragonflyStaleMasterDetected event emitted")
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
