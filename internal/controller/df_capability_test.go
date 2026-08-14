package controller

import (
	"context"
	"log/slog"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	io_prometheus_client "github.com/prometheus/client_model/go"
	"k8s.io/client-go/tools/record"

	v1alpha1 "github.com/shipstream/bloodraven/api/v1alpha1"
	"github.com/shipstream/bloodraven/internal/dragonfly"
	"github.com/shipstream/bloodraven/internal/metrics"
)

func TestDragonflyManager_Tick_MissingReplTakeover(t *testing.T) {
	fg := fgWithDragonflyEnabledAndActive()
	cb, key := newDragonflyFakeClient(fg)
	c := cb.Build()
	rec := record.NewFakeRecorder(8)
	mgr := NewDragonflyManager(c, rec, slog.Default(), key, 50*time.Millisecond)

	dc1 := &fakeDragonflyConn{
		info:                    dragonfly.ReplicationInfo{Role: "master", MasterReplOffset: 100},
		replTakeoverUnsupported: true,
	}
	dc2 := &fakeDragonflyConn{
		info: dragonfly.ReplicationInfo{
			Role:                   "slave",
			MasterHost:             "lion-dragonfly-dc1.shared-lion.svc.cluster.local",
			MasterPort:             6379,
			MasterLinkStatus:       "up",
			MasterLastIOSecondsAgo: 0,
			SlaveReplOffset:        100,
			MasterReplOffset:       100,
		},
		replTakeoverUnsupported: true,
	}
	conn := newFakeConnector()
	conn.program("lion-dragonfly-dc1.shared-lion.svc.cluster.local:6379", dc1)
	conn.program("lion-dragonfly-dc2.shared-lion.svc.cluster.local:6379", dc2)
	mgr.SetConnector(conn.connect)

	mgr.Tick(context.Background())

	var got v1alpha1.MysqlFailoverGroup
	if err := c.Get(context.Background(), key, &got); err != nil {
		t.Fatalf("get fg: %v", err)
	}
	if got.Status.Dragonfly == nil || got.Status.Dragonfly.ReplTakeoverSupported == nil || *got.Status.Dragonfly.ReplTakeoverSupported {
		t.Fatalf("ReplTakeoverSupported = %+v, want false", got.Status.Dragonfly)
	}
	if got.Status.Dragonfly.Phase != v1alpha1.DragonflyPhaseReady {
		t.Errorf("Phase = %q, want Ready (capability must not degrade topology)", got.Status.Dragonfly.Phase)
	}
	if got.Status.Dragonfly.ReplTakeoverProbeMessage == "" {
		t.Error("expected ReplTakeoverProbeMessage to name the missing sites")
	}

	events := drainEventsCh(rec.Events, 4, 100*time.Millisecond)
	if !eventsContain(events, ReasonDragonflyReplTakeoverUnsupported) {
		t.Errorf("expected %s event, got %v", ReasonDragonflyReplTakeoverUnsupported, events)
	}
}

func TestDragonflyManager_Tick_PresentReplTakeoverNoWarning(t *testing.T) {
	fg := fgWithDragonflyEnabledAndActive()
	cb, key := newDragonflyFakeClient(fg)
	c := cb.Build()
	rec := record.NewFakeRecorder(8)
	mgr := NewDragonflyManager(c, rec, slog.Default(), key, 50*time.Millisecond)

	dc1 := &fakeDragonflyConn{info: dragonfly.ReplicationInfo{Role: "master", MasterReplOffset: 100}}
	dc2 := &fakeDragonflyConn{
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
	conn.program("lion-dragonfly-dc1.shared-lion.svc.cluster.local:6379", dc1)
	conn.program("lion-dragonfly-dc2.shared-lion.svc.cluster.local:6379", dc2)
	mgr.SetConnector(conn.connect)

	mgr.Tick(context.Background())

	var got v1alpha1.MysqlFailoverGroup
	if err := c.Get(context.Background(), key, &got); err != nil {
		t.Fatalf("get fg: %v", err)
	}
	if got.Status.Dragonfly == nil || got.Status.Dragonfly.ReplTakeoverSupported == nil || !*got.Status.Dragonfly.ReplTakeoverSupported {
		t.Fatalf("ReplTakeoverSupported = %+v, want true", got.Status.Dragonfly)
	}
	events := drainEventsCh(rec.Events, 4, 50*time.Millisecond)
	if eventsContain(events, ReasonDragonflyReplTakeoverUnsupported) {
		t.Errorf("unexpected unsupported event on a capable image: %v", events)
	}
}

func TestDragonflyManager_Tick_PreservesProbeAcrossUnreachable(t *testing.T) {
	fg := fgWithDragonflyEnabledAndActive()
	cb, key := newDragonflyFakeClient(fg)
	c := cb.Build()
	rec := record.NewFakeRecorder(8)
	mgr := NewDragonflyManager(c, rec, slog.Default(), key, 50*time.Millisecond)

	dc1 := &fakeDragonflyConn{
		info:                    dragonfly.ReplicationInfo{Role: "master", MasterReplOffset: 100},
		replTakeoverUnsupported: true,
	}
	dc2 := &fakeDragonflyConn{
		info:                    dragonfly.ReplicationInfo{Role: "slave", MasterLinkStatus: "up"},
		replTakeoverUnsupported: true,
	}
	conn := newFakeConnector()
	conn.program("lion-dragonfly-dc1.shared-lion.svc.cluster.local:6379", dc1)
	conn.program("lion-dragonfly-dc2.shared-lion.svc.cluster.local:6379", dc2)
	mgr.SetConnector(conn.connect)
	mgr.Tick(context.Background())

	conn.dialFails("lion-dragonfly-dc1.shared-lion.svc.cluster.local:6379", context.DeadlineExceeded)
	conn.dialFails("lion-dragonfly-dc2.shared-lion.svc.cluster.local:6379", context.DeadlineExceeded)
	mgr.Tick(context.Background())

	var got v1alpha1.MysqlFailoverGroup
	if err := c.Get(context.Background(), key, &got); err != nil {
		t.Fatalf("get fg: %v", err)
	}
	if got.Status.Dragonfly == nil || got.Status.Dragonfly.ReplTakeoverSupported == nil || *got.Status.Dragonfly.ReplTakeoverSupported {
		t.Fatalf("unreachable tick wiped probe: %+v", got.Status.Dragonfly)
	}
}

func TestDragonflyManager_Tick_ProbeTimeDoesNotPatchEveryPoll(t *testing.T) {
	fg := fgWithDragonflyEnabledAndActive()
	cb, key := newDragonflyFakeClient(fg)
	c := cb.Build()
	rec := record.NewFakeRecorder(8)
	mgr := NewDragonflyManager(c, rec, slog.Default(), key, 50*time.Millisecond)

	dc1 := &fakeDragonflyConn{info: dragonfly.ReplicationInfo{Role: "master", MasterReplOffset: 100}}
	dc2 := &fakeDragonflyConn{
		info: dragonfly.ReplicationInfo{
			Role: "slave", MasterHost: "lion-dragonfly-dc1.shared-lion.svc.cluster.local",
			MasterPort: 6379, MasterLinkStatus: "up", MasterLastIOSecondsAgo: 0,
			SlaveReplOffset: 100, MasterReplOffset: 100,
		},
	}
	conn := newFakeConnector()
	conn.program("lion-dragonfly-dc1.shared-lion.svc.cluster.local:6379", dc1)
	conn.program("lion-dragonfly-dc2.shared-lion.svc.cluster.local:6379", dc2)
	mgr.SetConnector(conn.connect)

	mgr.Tick(context.Background())
	var first v1alpha1.MysqlFailoverGroup
	if err := c.Get(context.Background(), key, &first); err != nil {
		t.Fatal(err)
	}
	if first.Status.Dragonfly == nil || first.Status.Dragonfly.ReplTakeoverProbeTime == nil {
		t.Fatal("expected probe time after first tick")
	}
	firstTime := *first.Status.Dragonfly.ReplTakeoverProbeTime
	firstRV := first.ResourceVersion

	time.Sleep(2 * time.Millisecond)
	mgr.Tick(context.Background())

	var second v1alpha1.MysqlFailoverGroup
	if err := c.Get(context.Background(), key, &second); err != nil {
		t.Fatal(err)
	}
	if second.Status.Dragonfly.ReplTakeoverProbeTime == nil || !second.Status.Dragonfly.ReplTakeoverProbeTime.Equal(&firstTime) {
		t.Errorf("probe time changed on a no-op tick: %v -> %v", firstTime, second.Status.Dragonfly.ReplTakeoverProbeTime)
	}
	if second.ResourceVersion != firstRV {
		t.Errorf("status resourceVersion changed on a no-op tick: %s -> %s", firstRV, second.ResourceVersion)
	}
}

func TestDragonflyManager_TryEmergencyPromote_FallbackIsLoud(t *testing.T) {
	fg := fgWithDragonflyEnabledAndActive()
	cb, key := newDragonflyFakeClient(fg)
	c := cb.Build()
	rec := record.NewFakeRecorder(8)
	mgr := NewDragonflyManager(c, rec, slog.Default(), key, 50*time.Millisecond)

	target := &fakeDragonflyConn{
		info:            dragonfly.ReplicationInfo{Role: "slave"},
		replTakeoverErr: context.DeadlineExceeded,
	}
	conn := newFakeConnector()
	conn.program("lion-dragonfly-dc2.shared-lion.svc.cluster.local:6379", target)
	mgr.SetConnector(conn.connect)

	beforeLost := readPromCounter(t, metrics.DragonflyPromotionsTotal.WithLabelValues(fg.Name, "dc2", "sessions_lost"))
	beforeOK := readPromCounter(t, metrics.DragonflyPromotionsTotal.WithLabelValues(fg.Name, "dc2", "success"))

	ok := mgr.TryEmergencyPromote(context.Background(), "dc2", "dc1")
	if !ok {
		t.Fatal("fallback promotion should return true (target is writable)")
	}

	events := drainEventsCh(rec.Events, 4, 100*time.Millisecond)
	if !eventsContain(events, ReasonDragonflySessionsLost) {
		t.Errorf("expected %s, got %v", ReasonDragonflySessionsLost, events)
	}
	if !eventsContain(events, ReasonDragonflyPromotionCompleted) {
		t.Errorf("expected %s to remain for contract, got %v", ReasonDragonflyPromotionCompleted, events)
	}
	if !eventsContain(events, "REPLICAOF NO ONE") {
		t.Errorf("expected fallback wording, got %v", events)
	}

	afterLost := readPromCounter(t, metrics.DragonflyPromotionsTotal.WithLabelValues(fg.Name, "dc2", "sessions_lost"))
	afterOK := readPromCounter(t, metrics.DragonflyPromotionsTotal.WithLabelValues(fg.Name, "dc2", "success"))
	if afterLost != beforeLost+1 {
		t.Errorf("sessions_lost = %v, want %v", afterLost, beforeLost+1)
	}
	if afterOK != beforeOK {
		t.Errorf("success incremented on fallback (%v -> %v); want unchanged", beforeOK, afterOK)
	}
}

func TestApplyReplTakeoverStatus_KeepsFalseOnPartialProbe(t *testing.T) {
	no := false
	yes := true
	fg := &v1alpha1.MysqlFailoverGroup{
		Status: v1alpha1.MysqlFailoverGroupStatus{
			Dragonfly: &v1alpha1.DragonflyStatus{
				ReplTakeoverSupported:    &no,
				ReplTakeoverProbeMessage: "REPLTAKEOVER not advertised on dc2",
			},
		},
	}
	st := v1alpha1.DragonflyStatus{}
	applyReplTakeoverStatus(&st, fg, []DragonflySiteSnapshot{
		{Name: "dc1", ReplTakeover: &yes},
		{Name: "dc2"}, // unprobed
	})
	if st.ReplTakeoverSupported == nil || *st.ReplTakeoverSupported {
		t.Fatalf("partial probe upgraded known-false to %v", st.ReplTakeoverSupported)
	}
	if st.ReplTakeoverProbeMessage != "REPLTAKEOVER not advertised on dc2" {
		t.Errorf("message rewritten on partial probe: %q", st.ReplTakeoverProbeMessage)
	}
}

func TestApplyReplTakeoverStatus_UpgradesWhenEverySiteProbed(t *testing.T) {
	no := false
	yes := true
	fg := &v1alpha1.MysqlFailoverGroup{
		Status: v1alpha1.MysqlFailoverGroupStatus{
			Dragonfly: &v1alpha1.DragonflyStatus{ReplTakeoverSupported: &no},
		},
	}
	st := v1alpha1.DragonflyStatus{}
	applyReplTakeoverStatus(&st, fg, []DragonflySiteSnapshot{
		{Name: "dc1", ReplTakeover: &yes},
		{Name: "dc2", ReplTakeover: &yes},
	})
	if st.ReplTakeoverSupported == nil || !*st.ReplTakeoverSupported {
		t.Fatalf("complete true probes should upgrade known-false, got %v", st.ReplTakeoverSupported)
	}
}

func TestFoldReplTakeover(t *testing.T) {
	yes, no := true, false
	tests := []struct {
		name string
		in   []DragonflySiteSnapshot
		want *bool
	}{
		{name: "no probes", in: []DragonflySiteSnapshot{{Name: "a"}}, want: nil},
		{name: "all true", in: []DragonflySiteSnapshot{{Name: "a", ReplTakeover: &yes}, {Name: "b", ReplTakeover: &yes}}, want: &yes},
		{name: "any false", in: []DragonflySiteSnapshot{{Name: "a", ReplTakeover: &yes}, {Name: "b", ReplTakeover: &no}}, want: &no},
		{name: "false only", in: []DragonflySiteSnapshot{{Name: "a", ReplTakeover: &no}}, want: &no},
		{name: "mix nil and true", in: []DragonflySiteSnapshot{{Name: "a"}, {Name: "b", ReplTakeover: &yes}}, want: &yes},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, _ := foldReplTakeover(tt.in)
			if !boolPtrEqual(got, tt.want) {
				t.Errorf("foldReplTakeover = %v, want %v", got, tt.want)
			}
		})
	}
}

func readPromCounter(t *testing.T, c prometheus.Counter) float64 {
	t.Helper()
	var m io_prometheus_client.Metric
	if err := c.Write(&m); err != nil {
		t.Fatalf("read counter: %v", err)
	}
	if m.Counter == nil {
		return 0
	}
	return *m.Counter.Value
}
