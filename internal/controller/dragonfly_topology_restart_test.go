package controller

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	v1alpha1 "github.com/shipstream/bloodraven/api/v1alpha1"
	"github.com/shipstream/bloodraven/internal/dragonfly"
)

const (
	restartTestOldReplID = "8c03c0ed9d972e36e6aaaa5a8222cabea1de09fd"
	restartTestNewReplID = "785aff8f33fc1d1f538aba5a75b76441669bdc2f"
	restartTestDC1Host   = "lion-dragonfly-dc1.shared-lion.svc.cluster.local"
)

// restartHarness is the threshold harness with replication ids and a clock:
// dc1 is the master (restartTestOldReplID) and dc2 its replica in stable
// sync, both holding 300 keys.
type restartHarness struct {
	*thresholdHarness
	clock time.Time
}

func newRestartHarness(t *testing.T, mutate func(*v1alpha1.MysqlFailoverGroup)) *restartHarness {
	t.Helper()
	h := &restartHarness{thresholdHarness: newThresholdHarness(t, mutate), clock: time.Unix(1_800_000_000, 0)}
	h.mgr.now = func() time.Time { return h.clock }
	h.dc1.info.MasterReplID, h.dc1.keys = restartTestOldReplID, 300
	h.dc2.info.MasterReplID, h.dc2.keys = restartTestOldReplID, 300
	return h
}

func (h *restartHarness) advance(d time.Duration) { h.clock = h.clock.Add(d) }

// masterRestarted makes the two instances report what a real pair reports
// after the master's process was replaced (Dragonfly v1.38.0 and v2.1 with
// --break_replication_on_master_restart): the master answers with a new
// replication id, holding keys keys and listing the replica in
// state=preparation; the replica keeps the old id and its keys and reports
// the link down.
func (h *restartHarness) masterRestarted(keys int64) {
	h.seeMaster()
	h.dc1.info = dragonfly.ReplicationInfo{Role: "master", ConnectedSlaves: 1, MasterReplID: restartTestNewReplID}
	h.dc1.keys = keys
	h.dc2.info.MasterLinkStatus = "down"
}

// outcome reads what the last cycle did to the replica.
func (h *restartHarness) outcome() (promoted bool, reattached bool) {
	h.t.Helper()
	return h.promoted() == "dc2", len(h.dc2.replicaOfArgs) > 0
}

func (h *restartHarness) wantPromotedNotReattached(when string) {
	h.t.Helper()
	promoted, reattached := h.outcome()
	if !promoted || reattached {
		h.t.Fatalf("%s: promoted=%v REPLICAOF sent to the replica=%v (%v); want the replica promoted and never re-attached\n%s",
			when, promoted, reattached, h.dc2.replicaOfArgs, h.logs.String())
	}
	if h.dc2.replTakeoverCalls != 0 {
		h.t.Fatalf("%s: REPLTAKEOVER sent %d times; the replica does not follow the restarted master", when, h.dc2.replTakeoverCalls)
	}
	if h.dc2.replicaOfNoOneCalls != 1 {
		h.t.Fatalf("%s: REPLICAOF NO ONE sent %d times, want once", when, h.dc2.replicaOfNoOneCalls)
	}
	if st := dragonflyStatusOf(h.t, h.c, h.key); st.ActiveSite != "dc2" {
		h.t.Fatalf("%s: activeSite = %q, want dc2", when, st.ActiveSite)
	}
}

func (h *restartHarness) wantReattached(when string) {
	h.t.Helper()
	promoted, reattached := h.outcome()
	if promoted || !reattached {
		h.t.Fatalf("%s: promoted=%v REPLICAOF sent to the replica=%v; want it re-attached to the master as before\n%s",
			when, promoted, reattached, h.logs.String())
	}
	if got := h.dc2.replicaOfArgs; got[0] != restartTestDC1Host || got[1] != "6379" {
		h.t.Fatalf("%s: REPLICAOF %v, want the active master", when, got)
	}
}

// A master that restarted empty is not what its replica is re-attached to:
// the replica holds the only copy of the cache and is promoted with
// REPLICAOF NO ONE, which keeps its keys. Before, the per-cycle REPLICAOF
// emptied it (350 keys to 0 on v1.38.0).
func TestDragonflyManager_Tick_RestartedEmptyMaster(t *testing.T) {
	t.Run("restarted faster than one poll, never missed", func(t *testing.T) {
		h := newRestartHarness(t, nil)
		h.tick(1)
		h.masterRestarted(0)
		h.tick(1)
		h.wantPromotedNotReattached("first cycle after the restart")
		if n := h.waitLogged(); n != 0 {
			t.Fatalf("the threshold wait was logged %d times; no observation was missed", n)
		}
	})
	t.Run("killed and back within the threshold", func(t *testing.T) {
		h := newRestartHarness(t, nil)
		h.tick(1)
		h.missMaster()
		h.dc2.info.MasterLinkStatus = "down"
		h.tick(defaultDragonflyMasterLossThreshold - 1)
		if promoted, _ := h.outcome(); promoted {
			t.Fatalf("promoted after %d missed observations, want none before %d", defaultDragonflyMasterLossThreshold-1, defaultDragonflyMasterLossThreshold)
		}
		// While the master is gone the replica is told to follow it again,
		// as before: Dragonfly refuses that and the replica keeps its keys.
		if len(h.dc2.replicaOfArgs) == 0 {
			t.Fatal("no REPLICAOF sent to the link-down replica while the master was unobserved")
		}
		h.dc2.replicaOfArgs = nil
		h.masterRestarted(0)
		h.tick(1)
		h.wantPromotedNotReattached("the cycle that saw the master back")
	})
	t.Run("an operator that restarted during the outage", func(t *testing.T) {
		h := newRestartHarness(t, nil)
		// No cycle saw the pair in sync: this manager starts here.
		h.masterRestarted(0)
		h.tick(1)
		h.wantPromotedNotReattached("first cycle of a new manager")
	})
	t.Run("sighting too old", func(t *testing.T) {
		h := newRestartHarness(t, nil)
		h.tick(1)
		h.advance(time.Hour)
		h.masterRestarted(0)
		h.tick(1)
		h.wantPromotedNotReattached("an empty master needs no sighting")
	})
}

// A recreated master of a busy cache holds keys by the time it is first
// observed (49 keys half a second after its return, with one client
// writing). The replica is still the one that has the cache, and the
// operator knows it when it saw the replica in sync with the master's
// earlier instance shortly before. Without that sighting, or with a key
// count it could not read, it re-attaches the replica as it always did.
func TestDragonflyManager_Tick_RestartedMasterWithKeys(t *testing.T) {
	maxAge := func(h *restartHarness) time.Duration {
		var fg v1alpha1.MysqlFailoverGroup
		if err := h.c.Get(context.Background(), h.key, &fg); err != nil {
			t.Fatal(err)
		}
		return h.mgr.linkedSightingMaxAge(&fg)
	}
	t.Run("seen in sync just before: promoted", func(t *testing.T) {
		h := newRestartHarness(t, nil)
		h.tick(1)
		h.advance(maxAge(h))
		h.masterRestarted(49)
		h.tick(1)
		h.wantPromotedNotReattached("a sighting at the age limit")
	})
	t.Run("sighting older than the limit: re-attached", func(t *testing.T) {
		h := newRestartHarness(t, nil)
		h.tick(1)
		h.advance(maxAge(h) + time.Second)
		h.masterRestarted(49)
		h.tick(1)
		h.wantReattached("a sighting past the age limit")
	})
	t.Run("no sighting (operator restarted): re-attached", func(t *testing.T) {
		h := newRestartHarness(t, nil)
		h.masterRestarted(49)
		h.tick(1)
		h.wantReattached("no sighting")
	})
	t.Run("sighting of another replication id: re-attached", func(t *testing.T) {
		h := newRestartHarness(t, nil)
		h.dc2.info.MasterReplID = "0000000000000000000000000000000000000000"
		h.tick(1)
		h.dc2.info.MasterReplID = restartTestOldReplID
		h.masterRestarted(49)
		h.tick(1)
		h.wantReattached("the sighting was of another master instance")
	})
	t.Run("sighting while in a full sync does not count", func(t *testing.T) {
		h := newRestartHarness(t, nil)
		h.dc2.info.MasterSyncInProgress = true
		h.tick(1)
		h.dc2.info.MasterSyncInProgress = false
		h.masterRestarted(49)
		h.tick(1)
		h.wantReattached("the replica was never seen in stable sync")
	})
	t.Run("the default age limit", func(t *testing.T) {
		h := newRestartHarness(t, nil)
		h.mgr.pollInterval = 2 * time.Second
		// (threshold 3 + 1) cycles of poll 2s + dial 3s + I/O 5s.
		if got, want := maxAge(h), 40*time.Second; got != want {
			t.Fatalf("linkedSightingMaxAge = %s, want %s", got, want)
		}
	})
}

// Every part of the evidence is needed, and an unread value is not
// evidence: in each of these the replica is re-attached as before.
func TestDragonflyManager_Tick_RestartedMasterEvidence(t *testing.T) {
	for _, tc := range []struct {
		name   string
		mutate func(h *restartHarness)
	}{
		{"same replication id: the link dropped, the master did not restart", func(h *restartHarness) {
			h.dc1.info.MasterReplID = restartTestOldReplID
		}},
		{"master reports no replication id", func(h *restartHarness) { h.dc1.info.MasterReplID = "" }},
		{"replica reports no replication id", func(h *restartHarness) { h.dc2.info.MasterReplID = "" }},
		{"master key count unreadable", func(h *restartHarness) { h.dc1.keysErr = errors.New("no keyspace section") }},
		{"replica key count unreadable", func(h *restartHarness) { h.dc2.keysErr = errors.New("no keyspace section") }},
		{"replica holds no keys: nothing to keep", func(h *restartHarness) { h.dc2.keys = 0 }},
		{"replica follows another address", func(h *restartHarness) {
			h.dc2.info.MasterHost = "lion-dragonfly-dc9.shared-lion.svc.cluster.local"
		}},
		{"replica in a full sync", func(h *restartHarness) { h.dc2.info.MasterSyncInProgress = true }},
		{"replica loading a snapshot", func(h *restartHarness) { h.dc2.persist.Loading = true }},
		{"link state not reported", func(h *restartHarness) { h.dc2.info.MasterLinkStatus = "" }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			h := newRestartHarness(t, nil)
			h.tick(1)
			h.masterRestarted(0)
			tc.mutate(h)
			h.tick(1)
			h.wantReattached(tc.name)
		})
	}
	t.Run("link up with another id: left alone", func(t *testing.T) {
		h := newRestartHarness(t, nil)
		h.tick(1)
		h.masterRestarted(0)
		h.dc2.info.MasterLinkStatus = "up"
		h.tick(1)
		if promoted, reattached := h.outcome(); promoted || reattached {
			t.Fatalf("promoted=%v reattached=%v, want neither for a replica that reports its link up", promoted, reattached)
		}
	})
}

// reconcileReplication itself refuses the REPLICAOF, whether or not a
// promotion was made first.
func TestDragonflyManager_ReconcileReplication_KeepsCacheOverRestartedMaster(t *testing.T) {
	h := newRestartHarness(t, nil)
	fg := fgWithDragonflyEnabledAndActive()
	fg.Status.Dragonfly = &v1alpha1.DragonflyStatus{ActiveSite: "dc1"}
	zero, many := int64(0), int64(300)
	replica := dragonflyReplicaOf(restartTestDC1Host)
	replica.MasterLinkStatus, replica.MasterReplID = "down", restartTestOldReplID
	snaps := []DragonflySiteSnapshot{
		{Name: "dc1", Reachable: true, Keys: &zero, Info: dragonfly.ReplicationInfo{Role: "master", MasterReplID: restartTestNewReplID}},
		{Name: "dc2", Reachable: true, Keys: &many, Info: replica},
	}
	h.mgr.reconcileReplication(context.Background(), fg, snaps, false)
	if len(h.dc2.replicaOfArgs) != 0 {
		t.Fatalf("REPLICAOF %v sent to the replica that holds the only copy of the cache", h.dc2.replicaOfArgs)
	}
	if !strings.Contains(h.logs.String(), "not re-attaching it") {
		t.Fatalf("the refusal was not logged:\n%s", h.logs.String())
	}
	// The same replica beside a master of the same instance is re-attached.
	snaps[0].Info.MasterReplID = restartTestOldReplID
	h.mgr.reconcileReplication(context.Background(), fg, snaps, false)
	if len(h.dc2.replicaOfArgs) == 0 {
		t.Fatal("no REPLICAOF sent to a link-down replica of the same master instance")
	}
}

// With three sites, the replica that executed the most of the earlier
// instance's journal is promoted; the other replica and the restarted
// master are then attached to it.
func TestDragonflyManager_Tick_RestartedMasterThreeSites(t *testing.T) {
	h := newRestartHarness(t, func(fg *v1alpha1.MysqlFailoverGroup) {
		dc3 := *fg.Spec.Sites[1].DeepCopy()
		dc3.Name, dc3.ServerID, dc3.Zone = "dc3", 103, "lion-dc3"
		fg.Spec.Sites = append(fg.Spec.Sites, dc3)
	})
	dc3 := &fakeDragonflyConn{info: dragonflyReplicaOf(restartTestDC1Host), keys: 300}
	dc3.info.MasterReplID = restartTestOldReplID
	dc3.info.SlaveReplOffset, h.dc2.info.SlaveReplOffset = 120, 100
	h.conn.program(holdTestDC3, dc3)
	h.tick(1)

	h.masterRestarted(0)
	dc3.info.MasterLinkStatus = "down"
	h.tick(1)
	if got := h.promoted(); got != "dc3" {
		t.Fatalf("promoted %q, want dc3, which is ahead of dc2", got)
	}
	if len(h.dc2.replicaOfArgs) != 0 || len(dc3.replicaOfArgs) != 0 {
		t.Fatalf("REPLICAOF sent in the promoting cycle: dc2=%v dc3=%v", h.dc2.replicaOfArgs, dc3.replicaOfArgs)
	}

	// The next cycle sees dc3 as the master; the restarted dc1, with no
	// replica attached, is a stale master.
	dc3.info = dragonfly.ReplicationInfo{Role: "master", MasterReplID: "81196fd297ab5946b6ceb24a817c963c091418c0"}
	h.dc1.info.ConnectedSlaves = 0
	h.tick(1)
	const dc3Host = "lion-dragonfly-dc3.shared-lion.svc.cluster.local"
	if got := h.dc2.replicaOfArgs; len(got) == 0 || got[0] != dc3Host {
		t.Fatalf("dc2 REPLICAOF %v, want it attached to the promoted dc3", got)
	}
	if got := h.dc1.replicaOfArgs; len(got) == 0 || got[0] != dc3Host {
		t.Fatalf("dc1 REPLICAOF %v, want the empty restarted master attached to dc3", got)
	}
	if st := dragonflyStatusOf(t, h.c, h.key); st.ActiveSite != "dc3" {
		t.Fatalf("activeSite = %q, want dc3", st.ActiveSite)
	}
}

// A replica whose link to the master is down does not shorten the
// threshold, however long it has been down: that is a state, and one
// missed observation of a healthy master beside it is not the master's
// loss. Before, such a replica was promoted on the first miss.
func TestDragonflyManager_Tick_LinkDownReplicaDoesNotShortenTheThreshold(t *testing.T) {
	for _, tc := range []struct {
		name   string
		lastIO int
	}{
		{"link down just now", 0},
		{"link down for an hour (the path between the pods is refused)", 3600},
	} {
		t.Run(tc.name, func(t *testing.T) {
			h := newRestartHarness(t, nil)
			h.dc2.info.MasterLinkStatus = "down"
			h.dc2.info.MasterLastIOSecondsAgo = tc.lastIO
			h.dc2.replicaOfErr = errors.New("ERR replication cancelled")
			h.tick(10)
			h.missMaster()
			h.tick(1)
			if got := h.promoted(); got != "" {
				t.Fatalf("promoted %q on one missed observation of the master, want none", got)
			}
			h.seeMaster()
			h.tick(1)
			h.missMaster()
			h.tick(defaultDragonflyMasterLossThreshold - 1)
			if got := h.promoted(); got != "" {
				t.Fatalf("promoted %q before %d consecutive misses", got, defaultDragonflyMasterLossThreshold)
			}
			h.tick(1)
			if got := h.promoted(); got != "dc2" {
				t.Fatalf("promoted %q after the full threshold, want dc2", got)
			}
		})
	}
}

// A cycle that reached the master ends the miss count even when it then
// yields to another owner of the gate before acting.
func TestDragonflyManager_Tick_YieldedCycleStillEndsTheMissCount(t *testing.T) {
	h := newRestartHarness(t, nil)
	h.missMaster()
	h.tick(defaultDragonflyMasterLossThreshold - 1)

	// The master answers again, in a cycle that yields: a planned failover
	// starts waiting for the gate while the cycle observes.
	h.seeMaster()
	waiter := make(chan func(), 1)
	h.mgr.SetConnector(func(ctx context.Context, addr, password string) (DragonflyConnection, error) {
		if !h.mgr.gate.contended() {
			go func() {
				release, err := h.mgr.gate.acquire(context.Background())
				if err != nil {
					panic(err)
				}
				waiter <- release
			}()
			waitUntil(t, "a waiter on the gate", h.mgr.gate.contended)
		}
		return h.conn.connect(ctx, addr, password)
	})
	h.tick(1)
	(<-waiter)()
	h.mgr.SetConnector(h.conn.connect)

	h.missMaster()
	h.tick(defaultDragonflyMasterLossThreshold - 1)
	if got := h.promoted(); got != "" {
		t.Fatalf("promoted %q: the master was observed between the misses, in a cycle that yielded", got)
	}
	h.tick(1)
	if got := h.promoted(); got != "dc2" {
		t.Fatalf("promoted %q after a full run of consecutive misses, want dc2", got)
	}
}

// Misses with a gap between them are not consecutive: a count older than
// two poll intervals starts again.
func TestDragonflyManager_Tick_MissCountExpires(t *testing.T) {
	h := newRestartHarness(t, nil)
	h.mgr.pollInterval = 2 * time.Second
	h.missMaster()
	h.tick(defaultDragonflyMasterLossThreshold - 1)

	// Cycles are skipped while another owner holds the gate.
	release, ok := h.mgr.gate.tryAcquire()
	if !ok {
		t.Fatal("gate busy")
	}
	h.tick(3)
	h.advance(2*h.mgr.pollInterval + time.Millisecond)
	release()

	h.tick(1)
	if got := h.promoted(); got != "" {
		t.Fatalf("promoted %q on a miss made %s after the one before it", got, 2*h.mgr.pollInterval+time.Millisecond)
	}
	if n := h.waitLogged(); n != 2 {
		t.Fatalf("wait logged %d times, want twice: the expired count is a new wait", n)
	}
	h.advance(2 * h.mgr.pollInterval)
	h.tick(1)
	h.tick(1)
	if got := h.promoted(); got != "dc2" {
		t.Fatalf("promoted %q after %d misses no more than two poll intervals apart, want dc2", got, defaultDragonflyMasterLossThreshold)
	}
}

// The count belongs to the active site: when the active site changes, the
// misses of the previous one do not count against the new one.
func TestDragonflyManager_Tick_MissCountStartsAgainWithTheActiveSite(t *testing.T) {
	h := newRestartHarness(t, nil)
	h.missMaster()
	h.tick(defaultDragonflyMasterLossThreshold - 1)

	// dc2 becomes the active site (a promotion elsewhere recorded it), and
	// dc1 is back as its replica. Then dc2 is missed once.
	var fg v1alpha1.MysqlFailoverGroup
	if err := h.c.Get(context.Background(), h.key, &fg); err != nil {
		t.Fatal(err)
	}
	fg.Status.ActiveSite = "dc2"
	fg.Status.Dragonfly.ActiveSite = "dc2"
	if err := h.c.Status().Update(context.Background(), &fg); err != nil {
		t.Fatal(err)
	}
	h.seeMaster()
	h.dc1.info = dragonflyReplicaOf("lion-dragonfly-dc2.shared-lion.svc.cluster.local")
	h.conn.dialFails(holdTestDC2, errors.New("i/o timeout"))
	h.tick(1)
	if got := h.promoted(); got != "" {
		t.Fatalf("promoted %q on the first miss of the new active site: the misses of the old one were counted", got)
	}
	h.mgr.mu.Lock()
	site, misses := h.mgr.masterMissSite, h.mgr.masterMisses
	h.mgr.mu.Unlock()
	if site != "dc2" || misses != 1 {
		t.Fatalf("miss count = %d for %q, want 1 for dc2", misses, site)
	}
}

// The emergency and rollout promotion does not read a REPLTAKEOVER error
// as "no role changed": after "Operation canceled", as after any other
// failure, it sends REPLICAOF NO ONE on a fresh connection, so the target
// is a master whether or not the old master completes the takeover later.
func TestDragonflyManager_Promote_CanceledTakeoverStillPromotes(t *testing.T) {
	h := newRestartHarness(t, nil)
	first := &fakeDragonflyConn{info: h.dc2.info, keys: 300,
		replTakeoverErr: &dragonfly.ServerError{Message: "ERR Couldn't execute takeover: Operation canceled"}}
	fallback := &fakeDragonflyConn{info: h.dc2.info, keys: 300}
	h.conn.programQueue(holdTestDC2, first)
	h.conn.program(holdTestDC2, fallback)
	if !h.mgr.TryEmergencyPromote(context.Background(), "dc2", "dc1") {
		t.Fatalf("TryEmergencyPromote = false\n%s", h.logs.String())
	}
	if first.replTakeoverCalls != 1 || first.replicaOfNoOneCalls != 0 {
		t.Fatalf("takeover connection: REPLTAKEOVER %d, REPLICAOF NO ONE %d; want 1 and 0 (the fallback takes a fresh connection)",
			first.replTakeoverCalls, first.replicaOfNoOneCalls)
	}
	if fallback.replicaOfNoOneCalls != 1 {
		t.Fatalf("REPLICAOF NO ONE sent %d times after a canceled takeover, want once", fallback.replicaOfNoOneCalls)
	}
	if got := h.promoted(); got != "dc2" {
		t.Fatalf("promoted %q, want dc2", got)
	}
}

// The late-takeover history on v1.38.0: a planned REPLTAKEOVER ends with
// "Operation canceled" and the run records it failed; the master then runs
// the takeover after all and exits, the target stays a replica with the
// link down, and the master's pod returns empty. The cycle that sees that
// promotes the target; it does not re-attach it.
func TestDragonflyManager_Tick_LateTakeoverSourceReturnsEmpty(t *testing.T) {
	h := newRestartHarness(t, nil)
	h.tick(1)
	// The old master exits: missed once, with the target's link down.
	h.missMaster()
	h.dc2.info.MasterLinkStatus = "down"
	h.tick(1)
	h.dc2.replicaOfArgs = nil
	// Its pod is back, empty; a client has already written to it.
	h.masterRestarted(3)
	h.tick(1)
	h.wantPromotedNotReattached("the cycle that saw the old master back")
}

func TestDragonflyReplicaOfEarlierMaster(t *testing.T) {
	fg := fgWithDragonflyEnabledAndActive()
	many := int64(300)
	master := func() *DragonflySiteSnapshot {
		return &DragonflySiteSnapshot{Name: "dc1", Reachable: true, Info: dragonfly.ReplicationInfo{Role: "master", MasterReplID: restartTestNewReplID}}
	}
	replica := func() *DragonflySiteSnapshot {
		info := dragonflyReplicaOf(restartTestDC1Host)
		info.MasterLinkStatus, info.MasterReplID = "down", restartTestOldReplID
		return &DragonflySiteSnapshot{Name: "dc2", Reachable: true, Info: info, Keys: &many}
	}
	if !dragonflyReplicaOfEarlierMaster(fg, "dc1", master(), replica()) {
		t.Fatal("a link-down replica holding keys and another replication id than its master is not recognised")
	}
	for _, tc := range []struct {
		name   string
		mutate func(m, r *DragonflySiteSnapshot)
	}{
		{"master unreachable", func(m, _ *DragonflySiteSnapshot) { m.Reachable = false }},
		{"active site answers as a replica", func(m, _ *DragonflySiteSnapshot) { m.Info.Role = "slave" }},
		{"replica unreachable", func(_, r *DragonflySiteSnapshot) { r.Reachable = false }},
		{"the other site is a master", func(_, r *DragonflySiteSnapshot) { r.Info.Role = "master" }},
		{"another port", func(_, r *DragonflySiteSnapshot) { r.Info.MasterPort = 7000 }},
		{"the active site itself", func(_, r *DragonflySiteSnapshot) { r.Name = "dc1" }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			m, r := master(), replica()
			tc.mutate(m, r)
			if dragonflyReplicaOfEarlierMaster(fg, "dc1", m, r) {
				t.Fatal("dragonflyReplicaOfEarlierMaster() = true")
			}
		})
	}
	if dragonflyReplicaOfEarlierMaster(fg, "dc1", nil, replica()) || dragonflyReplicaOfEarlierMaster(fg, "dc1", master(), nil) {
		t.Fatal("dragonflyReplicaOfEarlierMaster() = true without a snapshot")
	}
}
