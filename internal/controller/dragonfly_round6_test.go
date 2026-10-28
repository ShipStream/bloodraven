package controller

import (
	"context"
	"log/slog"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/tools/record"

	v1alpha1 "github.com/shipstream/bloodraven/api/v1alpha1"
	"github.com/shipstream/bloodraven/internal/dragonfly"
)

// --- round 6 (Opus probes) ---------------------------------------------------

// P1: a snapshot SAVE holding the gate does not wedge the manager: the
// MySQL callback returns, other writers time out cleanly, Stop joins.

type blockingSaveConn struct {
	*fakeDragonflyConn
	entered chan struct{}
	release chan struct{}
}

func (c *blockingSaveConn) Save(ctx context.Context) error {
	close(c.entered)
	select {
	case <-c.release:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func TestDragonflySnapshotUpgrade_SaveUnderGateDoesNotWedgeManager(t *testing.T) {
	fg := fgWithDragonflySnapshotUpgrade()
	fg.Annotations = nil
	start := metav1.Now()
	fg.Status.Dragonfly.Upgrade = &v1alpha1.DragonflyUpgradeStatus{
		Phase: v1alpha1.DragonflyUpgradePhaseSavingSnapshot, ActiveSite: "dc1", TargetImage: "dragonfly:new", StartTime: &start,
	}
	cb, key := newDragonflyFakeClient(fg)
	c := cb.WithObjects(makeDragonflyPod(fg.Name, "dc1", "master", false), makeDragonflyPod(fg.Name, "dc2", "replica", false)).Build()
	runner := NewTopologyManagerRunner(c, nil, nil, record.NewFakeRecorder(64), slog.Default())
	runner.SetAPIReader(c)
	mgr := runner.newDragonflyManager(fg, key, 20*time.Millisecond)
	runner.SetManagerForTest(key, &TopologyManager{})
	runner.managers[key].dragonfly = mgr
	var mgrDials atomic.Int32
	mgr.SetConnector(func(context.Context, string, string) (DragonflyConnection, error) {
		mgrDials.Add(1)
		return &fakeDragonflyConn{info: dragonfly.ReplicationInfo{Role: "master"}}, nil
	})
	r := &MysqlFailoverGroupReconciler{Client: c, APIReader: c, Scheme: testScheme(), Runner: runner,
		Recorder: record.NewFakeRecorder(64), dragonflyGateWait: 50 * time.Millisecond}
	save := &blockingSaveConn{fakeDragonflyConn: &fakeDragonflyConn{}, entered: make(chan struct{}), release: make(chan struct{})}
	r.dragonflyConnector = func(context.Context, string, string) (DragonflyConnection, error) { return save, nil }

	upgradeDone := make(chan time.Duration, 1)
	go func() {
		d, err := r.reconcileDragonflySnapshotUpgrade(context.Background(), getFGByKey(t, c, key))
		if err != nil {
			t.Error(err)
		}
		upgradeDone <- d
	}()
	<-save.entered // the reconcile now holds the gate inside SAVE

	go mgr.Run(context.Background())
	returnsWithin(t, "MySQL emergency callback during SAVE", func() {
		emergencyDragonflyCallback(mgr)(context.Background(), "dc2", "dc1")
	})
	waitUntil(t, "Run to wait for the gate", mgr.gate.contended)

	// A planned/rollout writer on the same group times out cleanly.
	if _, ok := r.acquireDragonflyGate(context.Background(), key); ok {
		t.Fatal("acquired the gate while SAVE held it")
	}
	stopStart := time.Now()
	if !mgr.Stop(2 * time.Second) {
		t.Fatal("manager Stop wedged behind the upgrade's SAVE")
	}
	t.Logf("Stop returned in %s while SAVE held the gate", time.Since(stopStart))
	close(save.release)
	if d := <-upgradeDone; d <= 0 {
		t.Errorf("upgrade after SAVE requeue = %s", d)
	}
	if n := mgrDials.Load(); n != 0 {
		t.Errorf("manager dialled Dragonfly %d times during the upgrade", n)
	}
	if rel, ok := runner.DragonflyGate(key).tryAcquire(); !ok {
		t.Error("gate still held after SAVE returned")
	} else {
		rel()
	}
}

// P2: an upgrade annotation arriving during a planned failover, with the
// gate busy, neither deadlocks nor stalls the planned Dragonfly phase.

func TestDragonflySnapshotUpgrade_AnnotationDuringPlannedFailoverNoDeadlock(t *testing.T) {
	fg := plannedFailoverFGWithDragonfly("proceed")
	fg.Status.PlannedFailover.Phase = v1alpha1.PlannedFailoverPhasePromotingDragonfly
	fg.Status.PlannedFailover.Dragonfly = &v1alpha1.PlannedFailoverDragonflyStatus{Enabled: true}
	fg.Spec.Dragonfly.Snapshot = &v1alpha1.DragonflySnapshotSpec{Dir: "s3://x/y"}
	fg.Annotations = map[string]string{DragonflySnapshotUpgradeAnnotation: "dragonfly:new"}
	cb, nn := newDragonflyFakeClient(fg)
	c := cb.Build()
	runner := &TopologyManagerRunner{}
	rec := record.NewFakeRecorder(64)
	r := &MysqlFailoverGroupReconciler{Client: c, APIReader: c, Scheme: testScheme(), Recorder: rec, Runner: runner, dragonflyGateWait: 30 * time.Millisecond}
	r.dragonflyConnector = func(context.Context, string, string) (DragonflyConnection, error) {
		return &fakeDragonflyConn{info: dragonfly.ReplicationInfo{Role: "master"}}, nil
	}
	release, ok := runner.DragonflyGate(nn).tryAcquire() // an emergency on the wire
	if !ok {
		t.Fatal("gate busy")
	}
	d, err := r.reconcileDragonflySnapshotUpgrade(context.Background(), getFGByKey(t, c, nn))
	if err != nil || d != dragonflyGateRetry {
		t.Fatalf("upgrade with gate held = (%s, %v)", d, err)
	}
	release()
	d, err = r.reconcileDragonflySnapshotUpgrade(context.Background(), getFGByKey(t, c, nn))
	if err != nil || d != 0 {
		t.Fatalf("upgrade after release = (%s, %v), want reject and 0", d, err)
	}
	live := getFGByKey(t, c, nn)
	if _, still := live.Annotations[DragonflySnapshotUpgradeAnnotation]; still {
		t.Error("annotation not removed")
	}
	if up := dragonflyUpgradeStatus(live); dragonflyUpgradeInFlight(up) {
		t.Errorf("upgrade accepted during planned failover: %+v", up)
	}
	d, err = r.plannedFailoverPromotingDragonfly(context.Background(), live, nn)
	after := getFGByKey(t, c, nn).Status.PlannedFailover
	t.Logf("planned phase after upgrade reject: requeue=%s err=%v phase=%s df=%+v", d, err, after.Phase, after.Dragonfly)
	if after.Dragonfly != nil && after.Dragonfly.Message == plannedDragonflyGateWaitMessage {
		t.Errorf("planned phase still blocked on the gate after the upgrade released it")
	}
	if after.Phase == v1alpha1.PlannedFailoverPhasePromotingDragonfly && (after.Dragonfly == nil || after.Dragonfly.PromotionMethod == "") {
		t.Errorf("planned phase made no progress after the gate was released")
	}
}

// P3 fixtures: sites whose roles follow the commands sent to them.

type siteStateConnector struct {
	mu        sync.Mutex
	master    string
	replicaOf map[string]string
}

func (s *siteStateConnector) connect(_ context.Context, addr, _ string) (DragonflyConnection, error) {
	site := "dc2"
	if addr == emergencySourceAddr {
		site = "dc1"
	}
	return &siteStateConn{fakeDragonflyConn: &fakeDragonflyConn{}, s: s, site: site}, nil
}

type siteStateConn struct {
	*fakeDragonflyConn
	s    *siteStateConnector
	site string
}

func (c *siteStateConn) InfoReplication(context.Context) (dragonfly.ReplicationInfo, error) {
	c.s.mu.Lock()
	defer c.s.mu.Unlock()
	if c.s.master == c.site {
		return dragonfly.ReplicationInfo{Role: "master", ConnectedSlaves: 1}, nil
	}
	return dragonfly.ReplicationInfo{Role: "slave", MasterHost: "lion-dragonfly-" + c.s.master + ".shared-lion.svc.cluster.local",
		MasterPort: 6379, MasterLinkStatus: "up", MasterLastIOSecondsAgo: 0}, nil
}

func (c *siteStateConn) ReplTakeover(context.Context, time.Duration) error {
	c.s.mu.Lock()
	c.s.master = c.site
	c.s.mu.Unlock()
	return nil
}

func (c *siteStateConn) ReplicaOfNoOne(context.Context) error {
	return c.ReplTakeover(context.Background(), 0)
}

func (c *siteStateConn) ReplicaOf(_ context.Context, host string, _ int32) error {
	c.s.mu.Lock()
	if c.s.replicaOf == nil {
		c.s.replicaOf = map[string]string{}
	}
	c.s.replicaOf[c.site] = host
	c.s.mu.Unlock()
	return nil
}

type msgRecorder struct {
	slog.Handler
	mu   sync.Mutex
	msgs []string
}

func (h *msgRecorder) Handle(ctx context.Context, r slog.Record) error {
	h.mu.Lock()
	line := r.Message
	r.Attrs(func(a slog.Attr) bool { line += " " + a.String(); return true })
	h.msgs = append(h.msgs, line)
	h.mu.Unlock()
	return h.Handler.Handle(ctx, r)
}

// From TestRereview6InFlightTickPromotionDropsPublishedMySQLFollow: a Tick
// drift promotion of dc2 is already in flight on Run when MySQL fails back
// to dc1 and publishes it. Tick's dc2 stamp lands in a later second than
// the queued dc1 request. MySQL names dc1, so the request is its latest
// published decision and must run, not be dropped as superseded (which
// left Dragonfly on dc2 with drift repair suppressed for the TTL).
func TestDragonflyManager_PublishedMySQLFollowSurvivesInFlightTickPromotion(t *testing.T) {
	mgr, get := newEmergencyManager(t)
	mgr.pollInterval = time.Hour
	fg := get()
	fg.Status.ActiveSite = "dc2" // MySQL on dc2; Dragonfly still on dc1 (diverged)
	fg.Status.Dragonfly = &v1alpha1.DragonflyStatus{Enabled: true, ActiveSite: "dc1"}
	if err := mgr.client.Status().Update(context.Background(), fg); err != nil {
		t.Fatal(err)
	}
	entered, resume := make(chan struct{}), make(chan struct{})
	logs := &msgRecorder{Handler: slog.Default().Handler()}
	mgr.logger = slog.New(promotionLogBarrier{Handler: logs, entered: entered, resume: resume})
	state := &siteStateConnector{master: "dc1"}
	mgr.SetConnector(state.connect)

	go mgr.Run(context.Background())
	t.Cleanup(func() { mgr.Stop(10 * time.Second) })
	<-entered // Tick decided to promote dc2 to follow MySQL

	// MySQL's dc2 primary dies; MySQL fails back to dc1 and publishes it.
	live := get()
	now := metav1.Now()
	live.Status.ActiveSite, live.Status.LastFailoverTarget, live.Status.LastFailover = "dc1", "dc1", &now
	if err := mgr.client.Status().Update(context.Background(), live); err != nil {
		t.Fatal(err)
	}
	emergencyDragonflyCallback(mgr)(context.Background(), "dc1", "dc2")
	// Tick's promotion takes long enough to stamp in the next second.
	next := time.Now().Truncate(time.Second).Add(time.Second + 50*time.Millisecond)
	time.Sleep(time.Until(next))
	close(resume)
	waitUntil(t, "Tick's promotion to be recorded", dragonflyPromotedTo(mgr.client, mgr.fgKey, "dc2"))
	waitUntil(t, "the queued request to run", func() bool {
		mgr.pendingMu.Lock()
		defer mgr.pendingMu.Unlock()
		return mgr.pending == nil && len(mgr.wake) == 0
	})
	time.Sleep(50 * time.Millisecond)
	if !mgr.Stop(5 * time.Second) {
		t.Fatal("Run did not stop")
	}
	got := get()
	state.mu.Lock()
	master := state.master
	state.mu.Unlock()
	logs.mu.Lock()
	var superseded string
	for _, m := range logs.msgs {
		if strings.Contains(m, "superseded") {
			superseded = m
		}
	}
	logs.mu.Unlock()
	t.Logf("MySQL=%s status.dragonfly.activeSite=%s live master=%s; drop log: %q", got.Status.ActiveSite, got.Status.Dragonfly.ActiveSite, master, superseded)
	if master != "dc1" {
		t.Errorf("MySQL published dc1 but the queued follow request was dropped: Dragonfly master=%s (Tick drift suppressed for %s)", master, pendingPromotionActiveSiteTTL)
	}
}
