package controller

import (
	"context"
	"errors"
	"log/slog"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/tools/record"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"

	v1alpha1 "github.com/shipstream/bloodraven/api/v1alpha1"
	"github.com/shipstream/bloodraven/internal/dragonfly"
)

// Round-5 review reproductions for finding 4155662865055545: queued
// requests made obsolete by a newer decision, authority read through the
// informer cache after taking the gate, and the snapshot upgrade writing
// Dragonfly routing outside the gate.

// --- obsolete queued requests (GPT R1) ---------------------------------------

// From TestRereview5QueuedEmergencyAfterCompletedPlannedFailover: a dc1->dc2
// emergency request waits for the gate while a planned failback to dc1
// completes and unpauses the manager. When it finally runs it must be
// rejected as obsolete, not promote dc2 and delete the newer dc1 master.
func TestDragonflyManager_QueuedRequestObsoleteAfterCompletedPlannedFailover(t *testing.T) {
	mgr, c, fg := newEmergencyManagerWithPods(t)
	release, ok := mgr.gate.tryAcquire()
	if !ok {
		t.Fatal("gate busy")
	}
	mgr.Enqueue("dc2", "dc1")
	done := make(chan struct{})
	go func() { mgr.runPending(context.Background()); close(done) }()
	waitUntil(t, "queued request to wait for the gate", mgr.gate.contended)
	latest := getFGByKey(t, c, mgr.fgKey)
	latest.Status.ActiveSite = "dc1"
	latest.Status.PlannedFailover = &v1alpha1.PlannedFailoverStatus{
		Phase: v1alpha1.PlannedFailoverPhaseSucceeded, SourcePrimary: "dc2", Target: "dc1",
		Dragonfly: &v1alpha1.PlannedFailoverDragonflyStatus{PromotionMethod: "REPLTAKEOVER"},
	}
	now := metav1.Now()
	latest.Status.Dragonfly = &v1alpha1.DragonflyStatus{Enabled: true, ActiveSite: "dc1", LastPromotionTarget: "dc1", LastPromotionTime: &now}
	if err := c.Status().Update(context.Background(), latest); err != nil {
		t.Fatal(err)
	}
	mgr.SetPaused(false)
	var dials atomic.Int32
	mgr.SetConnector(func(context.Context, string, string) (DragonflyConnection, error) {
		dials.Add(1)
		return &fakeDragonflyConn{}, nil
	})
	release()
	<-done

	if n := dials.Load(); n != 0 {
		t.Errorf("obsolete request dialled Dragonfly %d times", n)
	}
	current := getFGByKey(t, c, mgr.fgKey)
	if current.Status.Dragonfly.ActiveSite != "dc1" {
		t.Errorf("queued request reversed the completed planned failover: MySQL=%s Dragonfly=%s", current.Status.ActiveSite, current.Status.Dragonfly.ActiveSite)
	}
	var pod corev1.Pod
	if err := c.Get(context.Background(), client.ObjectKeyFromObject(makeDragonflyPod(fg.Name, "dc1", "master", true)), &pod); apierrors.IsNotFound(err) {
		t.Error("obsolete request deleted the newer planned dc1 master")
	} else if err != nil {
		t.Fatal(err)
	}
}

// A newer MySQL decision made while Run waits for the gate replaces the
// older request: only the request current when the gate is held runs.
func TestDragonflyManager_NewerRequestWhileWaitingForGateReplacesOlder(t *testing.T) {
	mgr, c, _ := newEmergencyManagerWithPods(t)
	release, ok := mgr.gate.tryAcquire()
	if !ok {
		t.Fatal("gate busy")
	}
	mgr.Enqueue("dc2", "dc1")
	done := make(chan struct{})
	go func() { mgr.runPending(context.Background()); close(done) }()
	waitUntil(t, "queued request to wait for the gate", mgr.gate.contended)
	mgr.Enqueue("dc1", "dc2") // MySQL failed back before the first request ran
	var dc2Dials atomic.Int32
	mgr.SetConnector(func(_ context.Context, addr, _ string) (DragonflyConnection, error) {
		if addr == emergencyTargetAddr {
			dc2Dials.Add(1)
			return &fakeDragonflyConn{}, nil
		}
		// dc1 is still the Dragonfly master status names.
		return &fakeDragonflyConn{info: dragonfly.ReplicationInfo{Role: "master"}}, nil
	})
	release()
	<-done

	if n := dc2Dials.Load(); n != 0 {
		t.Errorf("the replaced dc2 request ran (%d dc2 dials)", n)
	}
	if p := mgr.peekPending(); p != nil {
		t.Errorf("request %+v left queued after Run took the gate", p)
	}
	if got := getFGByKey(t, c, mgr.fgKey).Status.Dragonfly; got.ActiveSite != "dc1" || got.LastPromotionTarget != "" {
		t.Errorf("status.dragonfly = %+v, want dc1 untouched", got)
	}
}

func TestSupersededPromotionRequest(t *testing.T) {
	issued := time.Now()
	sec := issued.Truncate(time.Second)
	at := func(d time.Duration) *metav1.Time { t := metav1.NewTime(sec.Add(d)); return &t }
	req := &dragonflyPromotionRequest{target: "dc2", oldSource: "dc1", issued: issued}
	for _, tc := range []struct {
		name     string
		mutate   func(*v1alpha1.MysqlFailoverGroup)
		obsolete bool
	}{
		{"nothing newer", func(*v1alpha1.MysqlFailoverGroup) {}, false},
		{"MySQL has not published yet", func(fg *v1alpha1.MysqlFailoverGroup) { fg.Status.ActiveSite = "dc1" }, false},
		{"MySQL published this decision", func(fg *v1alpha1.MysqlFailoverGroup) {
			fg.Status.ActiveSite, fg.Status.LastFailoverTarget, fg.Status.LastFailover = "dc2", "dc2", at(0)
		}, false},
		{"older failover to dc1", func(fg *v1alpha1.MysqlFailoverGroup) {
			fg.Status.LastFailoverTarget, fg.Status.LastFailover = "dc1", at(-time.Hour)
		}, false},
		{"newer MySQL failover to dc1", func(fg *v1alpha1.MysqlFailoverGroup) {
			fg.Status.LastFailoverTarget, fg.Status.LastFailover = "dc1", at(2*time.Second)
		}, true},
		{"planned failover started after", func(fg *v1alpha1.MysqlFailoverGroup) {
			fg.Status.PlannedFailover = &v1alpha1.PlannedFailoverStatus{Phase: v1alpha1.PlannedFailoverPhaseSucceeded, Target: "dc1", StartTime: at(time.Second)}
		}, true},
		{"planned failover from yesterday", func(fg *v1alpha1.MysqlFailoverGroup) {
			fg.Status.PlannedFailover = &v1alpha1.PlannedFailoverStatus{Phase: v1alpha1.PlannedFailoverPhaseSucceeded, Target: "dc1", StartTime: at(-24 * time.Hour)}
		}, false},
		{"newer Dragonfly promotion to dc1", func(fg *v1alpha1.MysqlFailoverGroup) {
			fg.Status.Dragonfly = &v1alpha1.DragonflyStatus{ActiveSite: "dc1", LastPromotionTarget: "dc1", LastPromotionTime: at(time.Second)}
		}, true},
		{"failed newer Dragonfly promotion to dc1", func(fg *v1alpha1.MysqlFailoverGroup) {
			fg.Status.Dragonfly = &v1alpha1.DragonflyStatus{ActiveSite: "dc2", LastPromotionTarget: "dc1", LastPromotionTime: at(time.Second)}
		}, false},
		{"older Dragonfly promotion to dc1", func(fg *v1alpha1.MysqlFailoverGroup) {
			fg.Status.Dragonfly = &v1alpha1.DragonflyStatus{ActiveSite: "dc1", LastPromotionTarget: "dc1", LastPromotionTime: at(-time.Second)}
		}, false},
		{"same-second promotion, MySQL elsewhere", func(fg *v1alpha1.MysqlFailoverGroup) {
			fg.Status.ActiveSite = "dc1"
			fg.Status.Dragonfly = &v1alpha1.DragonflyStatus{ActiveSite: "dc1", LastPromotionTarget: "dc1", LastPromotionTime: at(0)}
		}, true},
		{"later promotion elsewhere, MySQL published target", func(fg *v1alpha1.MysqlFailoverGroup) {
			fg.Status.ActiveSite, fg.Status.LastFailoverTarget, fg.Status.LastFailover = "dc2", "dc2", at(0)
			fg.Status.Dragonfly = &v1alpha1.DragonflyStatus{ActiveSite: "dc1", LastPromotionTarget: "dc1", LastPromotionTime: at(time.Second)}
		}, false},
		{"same-second promotion, MySQL on target", func(fg *v1alpha1.MysqlFailoverGroup) {
			fg.Status.ActiveSite = "dc2"
			fg.Status.Dragonfly = &v1alpha1.DragonflyStatus{ActiveSite: "dc1", LastPromotionTarget: "dc1", LastPromotionTime: at(0)}
		}, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			fg := fgWithDragonflyEnabledAndActive()
			tc.mutate(fg)
			if got := supersededPromotionRequest(fg, req) != ""; got != tc.obsolete {
				t.Errorf("superseded = %v (%q), want %v", got, supersededPromotionRequest(fg, req), tc.obsolete)
			}
		})
	}
}

// --- authority reads after taking the gate (GPT R2 = Opus R1) ----------------

// staleFGClient serves one fixed, older copy of the group, as an informer
// cache that has not caught up does; everything else passes through.
type staleFGClient struct {
	client.Client
	stale *v1alpha1.MysqlFailoverGroup
}

func (c *staleFGClient) Get(ctx context.Context, k client.ObjectKey, obj client.Object, opts ...client.GetOption) error {
	if fg, ok := obj.(*v1alpha1.MysqlFailoverGroup); ok && k == client.ObjectKeyFromObject(c.stale) {
		c.stale.DeepCopyInto(fg)
		return nil
	}
	return c.Client.Get(ctx, k, obj, opts...)
}

// From TestRereview5RolloutMustUseConfiguredAPIReader: the authoritative
// group records a planned promotion to dc1 still in Promoting; the informer
// cache returns the snapshot before it. The rollout promotion, holding the
// gate, must read the APIReader and send nothing.
func TestPromoteDragonflyForRollout_ReadsAuthorityUncached(t *testing.T) {
	mgr, liveClient, fg := newEmergencyManagerWithPods(t)
	stale := getFGByKey(t, liveClient, mgr.fgKey)
	latest := stale.DeepCopy()
	latest.Status.PlannedFailover = &v1alpha1.PlannedFailoverStatus{
		Phase: v1alpha1.PlannedFailoverPhasePromoting, SourcePrimary: "dc2", Target: "dc1",
		Dragonfly: &v1alpha1.PlannedFailoverDragonflyStatus{PromotionMethod: "REPLTAKEOVER"},
	}
	if err := liveClient.Status().Update(context.Background(), latest); err != nil {
		t.Fatal(err)
	}
	cached := &staleFGClient{Client: liveClient, stale: stale}
	r := &MysqlFailoverGroupReconciler{Client: cached, APIReader: liveClient, Runner: &TopologyManagerRunner{}, Recorder: record.NewFakeRecorder(64)}
	var dials atomic.Int32
	r.dragonflyConnector = func(context.Context, string, string) (DragonflyConnection, error) {
		dials.Add(1)
		return &fakeDragonflyConn{}, nil
	}
	promoted, deferred := r.promoteDragonflyForRollout(context.Background(), stale, "dc2", "dc1")
	if promoted || !deferred || dials.Load() != 0 {
		t.Errorf("rollout ignored the live planned failover: promoted=%v deferred=%v dials=%d", promoted, deferred, dials.Load())
	}
	var pod corev1.Pod
	if err := liveClient.Get(context.Background(), client.ObjectKeyFromObject(makeDragonflyPod(fg.Name, "dc1", "master", true)), &pod); apierrors.IsNotFound(err) {
		t.Error("rollout deleted the planned failover's dc1 master using stale informer state")
	} else if err != nil {
		t.Fatal(err)
	}
	if current := getFGByKey(t, liveClient, mgr.fgKey); current.Status.Dragonfly.ActiveSite != "dc1" {
		t.Errorf("rollout replaced live authority with %q", current.Status.Dragonfly.ActiveSite)
	}
}

// The rollout's target and source were chosen from an earlier read. If,
// by the time it holds the gate, another owner has moved the Dragonfly
// master, it must not take over from a site that is no longer the master.
func TestPromoteDragonflyForRollout_RevalidatesSourceUnderGate(t *testing.T) {
	mgr, c, _ := newEmergencyManagerWithPods(t)
	chosenFrom := getFGByKey(t, c, mgr.fgKey) // the rollout's read: dc1 active
	if err := stampDragonflyPromotionStatus(context.Background(), c, c, mgr.fgKey, "dc2", metav1.Now(), true); err != nil {
		t.Fatal(err)
	}
	r := &MysqlFailoverGroupReconciler{Client: c, APIReader: c, Runner: &TopologyManagerRunner{}, Recorder: record.NewFakeRecorder(64)}
	var dials atomic.Int32
	r.dragonflyConnector = func(context.Context, string, string) (DragonflyConnection, error) {
		dials.Add(1)
		return &fakeDragonflyConn{}, nil
	}
	if promoted, deferred := r.promoteDragonflyForRollout(context.Background(), chosenFrom, "dc2", "dc1"); promoted || !deferred {
		t.Errorf("rollout from a superseded source = (promoted %v, deferred %v), want deferred", promoted, deferred)
	}
	if n := dials.Load(); n != 0 {
		t.Errorf("rollout dialled Dragonfly %d times after its source stopped being the master", n)
	}
}

// laggingFGCache makes Gets of the group return the object as it was
// before the most recent status write, until lag has passed since that
// write: the informer-cache delay of the manager's production client.
type laggingFGCache struct {
	mu        sync.Mutex
	lag       time.Duration
	lastWrite time.Time
	before    *v1alpha1.MysqlFailoverGroup
	staleHits atomic.Int32
}

func (l *laggingFGCache) funcs() interceptor.Funcs {
	return interceptor.Funcs{
		Get: func(ctx context.Context, cl client.WithWatch, key client.ObjectKey, obj client.Object, opts ...client.GetOption) error {
			if fg, ok := obj.(*v1alpha1.MysqlFailoverGroup); ok {
				l.mu.Lock()
				var snap *v1alpha1.MysqlFailoverGroup
				if l.before != nil && time.Since(l.lastWrite) < l.lag {
					snap = l.before.DeepCopy()
				}
				l.mu.Unlock()
				if snap != nil {
					l.staleHits.Add(1)
					*fg = *snap
					return nil
				}
			}
			return cl.Get(ctx, key, obj, opts...)
		},
		SubResourcePatch: func(ctx context.Context, cl client.Client, sub string, obj client.Object, patch client.Patch, opts ...client.SubResourcePatchOption) error {
			if fg, ok := obj.(*v1alpha1.MysqlFailoverGroup); ok {
				var cur v1alpha1.MysqlFailoverGroup
				if err := cl.Get(ctx, client.ObjectKeyFromObject(fg), &cur); err == nil {
					l.mu.Lock()
					l.before = cur.DeepCopy()
					l.lastWrite = time.Now()
					l.mu.Unlock()
				}
			}
			return cl.SubResource(sub).Patch(ctx, obj, patch, opts...)
		},
	}
}

// lagTarget is the target Dragonfly: a replica of dc1 until REPLICAOF NO
// ONE, then a master that publishes no offset (as real Dragonfly masters
// do not).
type lagTarget struct {
	promoted     atomic.Bool
	replicaOfTo  atomic.Value
	takeoverSent atomic.Bool
}

type lagTargetConn struct {
	*fakeDragonflyConn
	s *lagTarget
}

func (c *lagTargetConn) InfoReplication(context.Context) (dragonfly.ReplicationInfo, error) {
	if c.s.promoted.Load() {
		return dragonfly.ReplicationInfo{Role: "master"}, nil
	}
	return dragonfly.ReplicationInfo{Role: "slave", MasterHost: "lion-dragonfly-dc1.shared-lion.svc.cluster.local", MasterPort: 6379, MasterLinkStatus: "up"}, nil
}

func (c *lagTargetConn) ReplTakeover(context.Context, time.Duration) error {
	c.s.takeoverSent.Store(true)
	time.Sleep(150 * time.Millisecond) // the replica could not catch up in time
	return errors.New("takeover timed out")
}

func (c *lagTargetConn) ReplicaOfNoOne(context.Context) error {
	c.s.promoted.Store(true)
	return nil
}

func (c *lagTargetConn) ReplicaOf(_ context.Context, host string, _ int32) error {
	c.s.replicaOfTo.Store(host)
	c.s.promoted.Store(false)
	return nil
}

// From TestRereview5ImmediateTickAfterPromotionActsOnCachedAuthority: a
// promotion that outlasts the poll interval leaves a tick buffered, so Run
// Ticks right after the promotion releases the gate. With the old source
// still a reachable master, a Tick that read the group through the lagging
// cache kept dc1 active: it stripped the just-promoted dc2's traffic and
// re-pointed it at dc1. With the uncached authority reader the runner
// wires in, the Tick sees the promotion.
func TestDragonflyManager_TickAfterPromotionReadsAuthorityUncached(t *testing.T) {
	for _, lag := range []time.Duration{0, 20 * time.Millisecond, 200 * time.Millisecond} {
		t.Run(lag.String(), func(t *testing.T) {
			fg := fgWithDragonflyEnabledAndActive()
			fg.Status.ActiveSite = "dc2" // MySQL already failed over to dc2
			fg.Status.Dragonfly = &v1alpha1.DragonflyStatus{Enabled: true, ActiveSite: "dc1"}
			cb, key := newDragonflyFakeClient(fg)
			base := cb.WithObjects(
				makeDragonflyPod(fg.Name, "dc1", "master", true),
				makeDragonflyPod(fg.Name, "dc2", "replica", true),
			).Build()
			cache := &laggingFGCache{lag: lag}
			c := interceptor.NewClient(base, cache.funcs())

			mgr := NewDragonflyManager(c, record.NewFakeRecorder(256), slog.Default(), key, 20*time.Millisecond)
			mgr.SetAuthorityReader(base) // what the runner does with the manager's APIReader
			s := &lagTarget{}
			mgr.SetConnector(func(_ context.Context, addr, _ string) (DragonflyConnection, error) {
				if addr == emergencyTargetAddr {
					return &lagTargetConn{fakeDragonflyConn: &fakeDragonflyConn{}, s: s}, nil
				}
				return &fakeDragonflyConn{info: dragonfly.ReplicationInfo{Role: "master"}}, nil // dc1 still answers
			})

			release, ok := mgr.gate.tryAcquire()
			if !ok {
				t.Fatal("gate busy")
			}
			go mgr.Run(context.Background())
			t.Cleanup(func() { mgr.Stop(10 * time.Second) })
			mgr.Enqueue("dc2", "dc1")
			waitUntil(t, "Run to wait for the gate", mgr.gate.contended)
			release()

			waitUntil(t, "the promotion to be stamped", dragonflyPromotedTo(base, key, "dc2"))
			// Let several Ticks run, the first of them straight after the
			// promotion, while the cache still lags. The ticker is the
			// only way to drive Run's buffered tick, so this waits.
			time.Sleep(400 * time.Millisecond)
			if !mgr.Stop(5 * time.Second) {
				t.Fatal("Run did not stop")
			}
			if !s.takeoverSent.Load() {
				t.Fatal("fixture: takeover not attempted")
			}
			if to := s.replicaOfTo.Load(); to != nil {
				t.Errorf("a Tick re-pointed the just-promoted dc2 at %v, undoing the promotion", to)
			}
			if eps := writableEndpoints(t, base, fg); len(eps) != 1 || eps[0] != "dc2" {
				t.Errorf("active-Service endpoints = %v, want [dc2]", eps)
			}
			if got := dragonflyActiveSite(getFGByKey(t, base, key)); got != "dc2" {
				t.Errorf("status.dragonfly.activeSite = %q, want dc2", got)
			}
		})
	}
}

// --- snapshot upgrade under the gate (GPT R3) ---------------------------------

type upgradeRoleConn struct {
	*fakeDragonflyConn
	master    *atomic.Bool
	demotions *atomic.Int32
}

func (c *upgradeRoleConn) InfoReplication(context.Context) (dragonfly.ReplicationInfo, error) {
	if c.master.Load() {
		return dragonfly.ReplicationInfo{Role: "master"}, nil
	}
	return dragonfly.ReplicationInfo{Role: "slave", MasterLinkStatus: "up"}, nil
}

func (c *upgradeRoleConn) ReplicaOf(context.Context, string, int32) error {
	c.master.Store(false)
	c.demotions.Add(1)
	return nil
}

// From TestRereview5SnapshotUpgradeDemotesMasterWhileEmergencyOwnsGate: an
// upgrade that retained dc1 reaches replica reattachment while an emergency
// dc2 takeover, already sent, awaits its reply. The upgrade must wait for
// the gate rather than demote dc2 inside the emergency's critical section;
// once the emergency has recorded dc2, the upgrade is superseded and is
// abandoned without re-attaching dc2 to dc1.
func TestDragonflySnapshotUpgrade_WaitsForAndYieldsToEmergencyPromotion(t *testing.T) {
	fg := fgWithDragonflySnapshotUpgrade()
	fg.Annotations = nil
	const image = "dragonfly:new"
	cb, key := newDragonflyFakeClient(fg)
	c := cb.WithObjects(dragonflyPodForTest(fg, "dc1", image, true), dragonflyPodForTest(fg, "dc2", image, true)).Build()
	fixture := &MysqlFailoverGroupReconciler{Client: c, Scheme: testScheme()}
	for _, site := range fg.Spec.Sites {
		if err := fixture.reconcileDragonflyStatefulSet(context.Background(), fg, site); err != nil {
			t.Fatal(err)
		}
	}
	runner := NewTopologyManagerRunner(c, nil, nil, record.NewFakeRecorder(64), slog.Default())
	mgr := NewDragonflyManager(c, record.NewFakeRecorder(64), slog.Default(), key, time.Hour)
	mgr.SetGate(runner.DragonflyGate(key))
	runner.SetManagerForTest(key, &TopologyManager{})
	runner.managers[key].dragonfly = mgr
	started, reply := make(chan struct{}), make(chan struct{})
	var replyOnce sync.Once
	sendReply := func() { replyOnce.Do(func() { close(reply) }) }
	t.Cleanup(sendReply)
	var targetMaster atomic.Bool
	var demotions atomic.Int32
	mgr.SetConnector(func(_ context.Context, addr, _ string) (DragonflyConnection, error) {
		if addr == emergencyTargetAddr {
			return &serverSideTakeoverConn{fakeDragonflyConn: &fakeDragonflyConn{}, onTakeover: func(ctx context.Context) error {
				targetMaster.Store(true)
				close(started)
				select {
				case <-reply:
					return nil
				case <-ctx.Done():
					return ctx.Err()
				}
			}}, nil
		}
		return &fakeDragonflyConn{}, nil
	})
	startActorWithRequest(t, mgr, "dc2", "dc1")
	<-started

	live := getFGByKey(t, c, key)
	upgradeStart := metav1.NewTime(time.Now().Add(-time.Second).Truncate(time.Second))
	live.Status.Dragonfly.Upgrade = &v1alpha1.DragonflyUpgradeStatus{
		Phase: v1alpha1.DragonflyUpgradePhaseReattachingReplicas, ActiveSite: "dc1", TargetImage: image, StartTime: &upgradeStart,
	}
	if err := c.Status().Update(context.Background(), live); err != nil {
		t.Fatal(err)
	}
	r := &MysqlFailoverGroupReconciler{Client: c, APIReader: c, Scheme: testScheme(), Runner: runner,
		Recorder: record.NewFakeRecorder(64), dragonflyGateWait: 50 * time.Millisecond}
	var upgradeDials atomic.Int32
	r.dragonflyConnector = func(_ context.Context, addr, _ string) (DragonflyConnection, error) {
		upgradeDials.Add(1)
		if addr == emergencyTargetAddr {
			return &upgradeRoleConn{fakeDragonflyConn: &fakeDragonflyConn{}, master: &targetMaster, demotions: &demotions}, nil
		}
		return nil, errors.New("old source not ready")
	}

	requeue, err := r.reconcileDragonflySnapshotUpgrade(context.Background(), live)
	if err != nil || requeue != dragonflyGateRetry {
		t.Fatalf("upgrade while the emergency held the gate = (%s, %v), want a %s requeue", requeue, err, dragonflyGateRetry)
	}
	if !mgr.paused.Load() {
		t.Error("fixture: snapshot upgrade did not pause the registered manager")
	}
	if n := upgradeDials.Load(); n != 0 {
		t.Fatalf("upgrade dialled Dragonfly %d times while the emergency held the gate", n)
	}

	sendReply()
	waitUntil(t, "the emergency to record dc2", dragonflyPromotedTo(c, key, "dc2"))
	if _, err := r.reconcileDragonflySnapshotUpgrade(context.Background(), getFGByKey(t, c, key)); err != nil {
		t.Fatal(err)
	}
	if n := demotions.Load(); n != 0 {
		t.Errorf("snapshot upgrade sent %d REPLICAOF commands to the promoted dc2", n)
	}
	current := getFGByKey(t, c, key)
	if up := current.Status.Dragonfly.Upgrade; up == nil || up.Phase != v1alpha1.DragonflyUpgradePhaseFailed || up.Reason != "ActiveSiteChanged" {
		t.Errorf("superseded upgrade = %+v, want Failed/ActiveSiteChanged", up)
	}
	if !targetMaster.Load() || current.Status.Dragonfly.ActiveSite != "dc2" {
		t.Errorf("dc2 master=%v, status.dragonfly.activeSite=%q; want dc2 kept", targetMaster.Load(), current.Status.Dragonfly.ActiveSite)
	}
}

// A runner resync derives the Dragonfly manager's pause from persisted
// status, so it cannot unpause a manager while a snapshot upgrade runs.
func TestDragonflyMaintenanceActive(t *testing.T) {
	fg := fgWithDragonflyEnabledAndActive()
	if dragonflyMaintenanceActive(fg) {
		t.Fatal("idle group reported maintenance")
	}
	fg.Status.Dragonfly = &v1alpha1.DragonflyStatus{Upgrade: &v1alpha1.DragonflyUpgradeStatus{Phase: v1alpha1.DragonflyUpgradePhaseUpdatingActive}}
	if !dragonflyMaintenanceActive(fg) {
		t.Error("in-flight snapshot upgrade not reported as maintenance")
	}
	fg.Status.Dragonfly.Upgrade.Phase = v1alpha1.DragonflyUpgradePhaseSucceeded
	fg.Status.PlannedFailover = &v1alpha1.PlannedFailoverStatus{Phase: v1alpha1.PlannedFailoverPhaseDraining}
	if !dragonflyMaintenanceActive(fg) {
		t.Error("in-flight planned failover not reported as maintenance")
	}
}

// --- planned failover waiting for the gate ------------------------------------

// A planned failover that finds the gate held says so: MySQL writes stay
// paused on the fenced source while it waits.
func TestPlannedFailoverPromotingDragonfly_ReportsGateWait(t *testing.T) {
	fg := plannedFailoverFGWithDragonfly("proceed")
	fg.Status.PlannedFailover.Phase = v1alpha1.PlannedFailoverPhasePromotingDragonfly
	fg.Status.PlannedFailover.Dragonfly = &v1alpha1.PlannedFailoverDragonflyStatus{Enabled: true}
	cb, nn := newDragonflyFakeClient(fg)
	c := cb.Build()
	runner := &TopologyManagerRunner{}
	rec := record.NewFakeRecorder(16)
	r := &MysqlFailoverGroupReconciler{Client: c, Scheme: testScheme(), Recorder: rec, Runner: runner, dragonflyGateWait: 20 * time.Millisecond}
	release, ok := runner.DragonflyGate(nn).tryAcquire()
	if !ok {
		t.Fatal("gate busy")
	}
	defer release()

	for range 2 {
		live := getFGByKey(t, c, nn)
		if requeue, err := r.plannedFailoverPromotingDragonfly(context.Background(), live, nn); err != nil || requeue != dragonflyGateRetry {
			t.Fatalf("planned phase with the gate held = (%s, %v), want a %s requeue", requeue, err, dragonflyGateRetry)
		}
	}
	got := getFGByKey(t, c, nn).Status.PlannedFailover
	if got.Phase != v1alpha1.PlannedFailoverPhasePromotingDragonfly || got.Dragonfly.Message != plannedDragonflyGateWaitMessage {
		t.Errorf("planned status = phase %q message %q, want the gate-wait message", got.Phase, got.Dragonfly.Message)
	}
	events := drainEventsCh(rec.Events, 4, 50*time.Millisecond)
	n := 0
	for _, ev := range events {
		if strings.Contains(ev, ReasonDragonflyPromotionWaiting) && strings.Contains(ev, "MySQL writes stay paused") {
			n++
		}
	}
	if n != 1 {
		t.Errorf("gate-wait Events = %d (%v), want exactly one per wait", n, events)
	}
}

// The runner builds each Dragonfly manager with the group's stable gate,
// its uncached reader, and the pause persisted status calls for.
func TestTopologyManagerRunner_NewDragonflyManagerWiring(t *testing.T) {
	fg := fgWithDragonflyEnabledAndActive()
	fg.Status.Dragonfly = &v1alpha1.DragonflyStatus{Upgrade: &v1alpha1.DragonflyUpgradeStatus{Phase: v1alpha1.DragonflyUpgradePhaseSavingSnapshot}}
	cb, key := newDragonflyFakeClient(fg)
	c := cb.Build()
	reader := &staleFGClient{Client: c, stale: fg}
	runner := NewTopologyManagerRunner(c, nil, nil, record.NewFakeRecorder(4), slog.Default())
	runner.SetAPIReader(reader)
	mgr := runner.newDragonflyManager(fg, key, time.Second)
	if mgr.gate != runner.DragonflyGate(key) {
		t.Error("manager does not share the group's stable gate")
	}
	if mgr.authority() != client.Reader(reader) {
		t.Error("manager does not read the group through the runner's uncached reader")
	}
	if !mgr.paused.Load() {
		t.Error("manager started unpaused during a snapshot upgrade")
	}
}

// A StatefulSet rollout picks the replica site to restart from the
// reconcile's read. If a promotion has since made that site the Dragonfly
// master, the restart is held back rather than taking down the new master.
func TestReconcileDragonflyResources_HoldsReplicaRestartOfNewMaster(t *testing.T) {
	fg := fgWithDragonfly()
	fg.Status.ActiveSite = "dc1"
	fg.Status.Dragonfly = &v1alpha1.DragonflyStatus{Enabled: true, ActiveSite: "dc1", Phase: v1alpha1.DragonflyPhaseReady}
	r, c := newReconciler(fg)
	r.Runner = &TopologyManagerRunner{}
	ctx := context.Background()
	if _, err := r.reconcileDragonflyResources(ctx, fg); err != nil {
		t.Fatalf("initial reconcile: %v", err)
	}
	before := dragonflyStatefulSetImageForTest(t, c, "dc2")

	stale := getFGByKey(t, c, fgNN(fg)) // the reconcile's read: dc1 active
	stale.Spec.Dragonfly.Image = "docker.dragonflydb.io/dragonflydb/dragonfly:v1.39.0"
	if err := stampDragonflyPromotionStatus(ctx, c, c, fgNN(fg), "dc2", metav1.Now(), true); err != nil {
		t.Fatal(err)
	}
	if _, err := r.reconcileDragonflyResources(ctx, stale); err != nil {
		t.Fatalf("rollout reconcile: %v", err)
	}
	if got := dragonflyStatefulSetImageForTest(t, c, "dc2"); got != before {
		t.Errorf("rollout restarted dc2 (image %q -> %q) after a promotion made it the Dragonfly master", before, got)
	}
}
