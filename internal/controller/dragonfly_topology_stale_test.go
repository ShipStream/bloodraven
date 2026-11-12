package controller

import (
	"errors"
	"strings"
	"testing"
	"time"

	"k8s.io/client-go/tools/record"

	v1alpha1 "github.com/shipstream/bloodraven/api/v1alpha1"
	"github.com/shipstream/bloodraven/internal/dragonfly"
)

const (
	staleReplicaMsg = "dragonfly: replica of the restarted master holds a stale copy; not promoting it"
	restartWaitMsg  = "dragonfly: restarted master is loading a snapshot; leaving its replica as it is"
)

// eventsOf drains the harness's recorder and returns the events that
// carry reason.
func (h *restartHarness) eventsOf(reason string) []string {
	h.t.Helper()
	var out []string
	for _, ev := range drainEventsCh(h.mgr.recorder.(*record.FakeRecorder).Events, 64, 20*time.Millisecond) {
		if strings.Contains(ev, reason) {
			out = append(out, ev)
		}
	}
	return out
}

// cutOffFromLiveMaster runs polls cycles, two seconds apart, in which the
// replica reports its link down while the master answers under the
// replication id the replica follows and takes writes.
func (h *restartHarness) cutOffFromLiveMaster(polls int) {
	h.dc2.info.MasterLinkStatus = "down"
	for i := 0; i < polls; i++ {
		h.advance(2 * time.Second)
		h.dc1.keys += 10
		h.dc2.info.MasterLastIOSecondsAgo += 2
		h.tick(1)
	}
	h.dc2.replicaOfArgs = nil
}

// The path between the pods is refused while the operator reaches both:
// the replica stays link-down with its old keys for an hour while the
// master lives on. The master's pod is then recreated and returns empty.
// The replica matches every condition of the restarted-master path, and
// its copy is an hour old (and holds whatever the application flushed in
// that hour): it is not promoted. It is re-attached, which is what happened
// before that path existed, and that is reported once.
func TestDragonflyManager_Tick_StaleReplicaNotPromotedOverEmptyRestartedMaster(t *testing.T) {
	h := newRestartHarness(t, nil)
	h.mgr.pollInterval = 2 * time.Second
	h.tick(1) // in sync
	h.cutOffFromLiveMaster(1800)
	if got := h.promoted(); got != "" {
		t.Fatalf("promoted %q while the master lived", got)
	}
	h.eventsOf("")

	h.masterRestarted(0)
	// The re-attach does not get through either, so the state repeats.
	h.dc2.replicaOfErr = errors.New("ERR replication cancelled")
	h.advance(2 * time.Second)
	h.tick(3)
	promoted, reattached := h.outcome()
	if promoted || h.dc2.replicaOfNoOneCalls != 0 {
		t.Fatalf("promoted=%v, REPLICAOF NO ONE sent %d times: an hour-old copy that the manager watched fall behind a live master became the cache\n%s",
			promoted, h.dc2.replicaOfNoOneCalls, h.logs.String())
	}
	if !reattached {
		t.Fatalf("the stale replica was not re-attached to the restarted master\n%s", h.logs.String())
	}
	if n := strings.Count(h.logs.String(), staleReplicaMsg); n != 1 {
		t.Fatalf("the stale replica was logged %d times over three cycles, want once\n%s", n, h.logs.String())
	}
	events := h.eventsOf(v1alpha1.EventDragonflyStaleReplicaNotPromoted)
	if len(events) != 1 || !strings.Contains(events[0], "0 keys") {
		t.Fatalf("DragonflyStaleReplicaNotPromoted events = %v, want one that says the cache starts empty", events)
	}
}

// The replica is seen in sync at T0 and loses its link at T0+2 while the
// master lives on; the manager sees the master alive under the same
// replication id for 36 s. The master then restarts from a snapshot that
// holds those 36 s. The sighting is 38 s old, inside its 40 s, but every
// poll since said the replica was cut off: it is not promoted.
func TestDragonflyManager_Tick_SightingDoesNotOutliveAnObservedCutOff(t *testing.T) {
	h := newRestartHarness(t, nil)
	h.mgr.pollInterval = 2 * time.Second
	h.tick(1)
	h.cutOffFromLiveMaster(18)
	h.masterRestarted(h.dc1.keys)
	h.advance(2 * time.Second)
	h.tick(1)
	h.wantReattached("a 38 s old sighting of a replica seen cut off from the live master ever since")
	if h.dc2.replicaOfNoOneCalls != 0 {
		t.Fatalf("REPLICAOF NO ONE sent %d times", h.dc2.replicaOfNoOneCalls)
	}
}

// A replica that lost its link in the instant its master died is the case
// the restarted-master path exists for. The sites are observed one after
// the other, so the master can be seen alive in the cycle in which the
// replica already reports the link down: once. That is not a cut-off from a
// master that lives on, and the replica is promoted.
func TestDragonflyManager_Tick_OneCutOffSightingIsNotStale(t *testing.T) {
	h := newRestartHarness(t, nil)
	h.mgr.pollInterval = 2 * time.Second
	h.tick(1)
	h.cutOffFromLiveMaster(1) // the master died right after it answered
	h.missMaster()
	h.advance(2 * time.Second)
	h.tick(1)
	h.dc2.replicaOfArgs = nil
	h.masterRestarted(0)
	h.advance(2 * time.Second)
	h.tick(1)
	h.wantPromotedNotReattached("a replica seen cut off from the live master once")
}

// Cycles can be skipped while another writer holds the group's gate. One
// sighting of the replica cut off, followed by nothing for longer than a
// sighting is trusted, is then all there is, and it says "cut off".
func TestDragonflyManager_Tick_OneOldCutOffSightingIsStale(t *testing.T) {
	h := newRestartHarness(t, nil)
	h.mgr.pollInterval = 2 * time.Second
	h.tick(1)
	h.cutOffFromLiveMaster(1)
	h.advance(41 * time.Second) // no cycle ran
	h.masterRestarted(0)
	h.tick(1)
	h.wantReattached("one sighting of the replica cut off, 41 s ago")
}

// A replica that is linked again is no longer cut off: what it missed, it
// has received since.
func TestDragonflyManager_Tick_CutOffEndsWhenTheReplicaIsLinkedAgain(t *testing.T) {
	h := newRestartHarness(t, nil)
	h.mgr.pollInterval = 2 * time.Second
	h.tick(1)
	h.cutOffFromLiveMaster(30)
	h.dc2.info.MasterLinkStatus = "up"
	h.advance(2 * time.Second)
	h.tick(1)
	h.masterRestarted(0)
	h.advance(2 * time.Second)
	h.tick(1)
	h.wantPromotedNotReattached("a replica that was cut off and linked again before the restart")
}

// What does not count as a cut-off from a live master: a master that does
// not answer, a master under another replication id, a replica in a full
// sync.
func TestDragonflyManager_NoteCutOffReplicas(t *testing.T) {
	for _, tc := range []struct {
		name   string
		mutate func(h *restartHarness)
		want   int
	}{
		{"link down, master alive under the same id", func(*restartHarness) {}, 2},
		{"master unobserved", func(h *restartHarness) { h.missMaster() }, 0},
		{"master under another replication id", func(h *restartHarness) { h.dc1.info.MasterReplID = restartTestNewReplID }, 0},
		{"master reports no replication id", func(h *restartHarness) { h.dc1.info.MasterReplID, h.dc2.info.MasterReplID = "", "" }, 0},
		{"replica in a full sync", func(h *restartHarness) { h.dc2.info.MasterSyncInProgress = true }, 0},
		{"replica follows another address", func(h *restartHarness) {
			h.dc2.info.MasterHost = "lion-dragonfly-dc9.shared-lion.svc.cluster.local"
		}, 0},
		{"link state not reported", func(h *restartHarness) { h.dc2.info.MasterLinkStatus = "" }, 0},
	} {
		t.Run(tc.name, func(t *testing.T) {
			h := newRestartHarness(t, nil)
			h.dc2.info.MasterLinkStatus = "down"
			tc.mutate(h)
			h.tick(2)
			h.mgr.mu.Lock()
			got := h.mgr.replicaCutOff["dc2"].seen
			h.mgr.mu.Unlock()
			if got != tc.want {
				t.Fatalf("cut-off sightings = %d, want %d", got, tc.want)
			}
		})
	}
}

// A master that is loading a snapshot answers role:master with loading:1
// and a key count that starts at 0 and grows (592462 of 6000000 one second
// in, on v1.38.0). Its count says nothing, so nothing is decided: the
// replica is neither promoted nor re-attached until the load ends.
func TestDragonflyManager_Tick_LoadingMasterDecidesNothing(t *testing.T) {
	loading := func(t *testing.T, keys int64) *restartHarness {
		h := newRestartHarness(t, nil)
		h.tick(1)
		h.advance(time.Hour) // no fresh sighting
		h.masterRestarted(keys)
		h.dc1.persist = dragonfly.PersistenceInfo{Loading: true}
		h.tick(3)
		if promoted, reattached := h.outcome(); promoted || reattached || h.dc2.replicaOfNoOneCalls != 0 {
			t.Fatalf("master loading with %d keys: promoted=%v reattached=%v, want neither\n%s", keys, promoted, reattached, h.logs.String())
		}
		if n := strings.Count(h.logs.String(), restartWaitMsg); n != 1 {
			t.Fatalf("the wait was logged %d times over three cycles, want once\n%s", n, h.logs.String())
		}
		return h
	}
	t.Run("the load ends with the cache: the replica is re-attached", func(t *testing.T) {
		h := loading(t, 0)
		h.dc1.persist, h.dc1.keys = dragonfly.PersistenceInfo{}, 300
		h.tick(1)
		h.wantReattached("a master that loaded its snapshot, with no fresh sighting of the replica")
	})
	t.Run("a partial count during the load decides nothing either", func(t *testing.T) {
		loading(t, 120)
	})
	t.Run("the load ends empty: the replica is promoted", func(t *testing.T) {
		h := loading(t, 0)
		h.dc1.persist = dragonfly.PersistenceInfo{}
		h.tick(1)
		h.wantPromotedNotReattached("a master that finished loading and holds nothing")
	})
	t.Run("persistence state unreadable: re-attached as before", func(t *testing.T) {
		h := newRestartHarness(t, nil)
		h.tick(1)
		h.masterRestarted(0)
		h.dc1.persistErr = errors.New("i/o timeout")
		h.tick(1)
		h.wantReattached("a master whose INFO persistence could not be read")
	})
}

// The two checks of the active-site record that rest on "holds no keys" do
// not read a loading master's count either.
func TestDragonflyActiveSiteRecord_LoadingMasterIsNotKnownEmpty(t *testing.T) {
	zero, some := int64(0), int64(120)
	t.Run("an unobserved recorded site is held over a loading master, whatever its count", func(t *testing.T) {
		fg := fgWithDragonflyEnabledAndActive()
		fg.Status.Dragonfly = &v1alpha1.DragonflyStatus{ActiveSite: "dc1"}
		for _, keys := range []*int64{&zero, &some} {
			snaps := []DragonflySiteSnapshot{
				{Name: "dc1"},
				{Name: "dc2", Reachable: true, PersistRead: true, Keys: keys, Persist: dragonfly.PersistenceInfo{Loading: true},
					Info: dragonfly.ReplicationInfo{Role: "master"}},
			}
			if got := dragonflyHeldOverEmptyMaster(fg, snaps); got != "dc2" {
				t.Fatalf("keys=%d: held over %q, want dc2: a loading master's count is not evidence of what it holds", *keys, got)
			}
			snaps[1].Persist.Loading = false
			if got := dragonflyHeldOverEmptyMaster(fg, snaps); (got == "dc2") != (*keys == 0) {
				t.Fatalf("keys=%d, not loading: held over %q", *keys, got)
			}
		}
	})
	t.Run("a recorded site that is loading is not displaced as empty", func(t *testing.T) {
		fg := fgWithDragonflyEnabledAndActive()
		fg.Status.ActiveSite = "dc2"
		fg.Status.Dragonfly = &v1alpha1.DragonflyStatus{ActiveSite: "dc1"}
		snaps := []DragonflySiteSnapshot{
			{Name: "dc1", Reachable: true, PersistRead: true, Keys: &zero, Persist: dragonfly.PersistenceInfo{Loading: true},
				Info: dragonfly.ReplicationInfo{Role: "master"}},
			{Name: "dc2", Reachable: true, PersistRead: true, Keys: &some, Info: dragonfly.ReplicationInfo{Role: "master"}},
		}
		if got := dragonflyMasterOverEmptyRecord(fg, snaps); got != "" {
			t.Fatalf("record displaced by %q while the recorded site is loading its snapshot", got)
		}
		snaps[0].Persist.Loading = false
		if got := dragonflyMasterOverEmptyRecord(fg, snaps); got != "dc2" {
			t.Fatalf("record displaced by %q, want dc2 once the recorded site is done loading and holds nothing", got)
		}
	})
}

// threeSiteRestartHarness is a restart harness with a third site, dc3.
func threeSiteRestartHarness(t *testing.T, dc3 *fakeDragonflyConn) *restartHarness {
	t.Helper()
	h := newRestartHarness(t, func(fg *v1alpha1.MysqlFailoverGroup) {
		site := *fg.Spec.Sites[1].DeepCopy()
		site.Name, site.ServerID, site.Zone = "dc3", 103, "lion-dc3"
		fg.Spec.Sites = append(fg.Spec.Sites, site)
	})
	h.conn.program(holdTestDC3, dc3)
	return h
}

// Three sites with a lost promotion record: a takeover moved the master
// from dc1 to dc2 and its status stamp failed, so the record still names
// dc1. dc1 returned empty; dc2 is a master under its own replication id
// and holds the newest data; dc3 still follows dc1's earlier instance.
// Promoting dc3 over the empty dc1 would add a third master, and dc2 would
// then be attached to dc3 and lose what only it holds. With a master that
// may hold the cache present, the replica is not promoted, and dc2 keeps
// its keys.
func TestDragonflyManager_Tick_NoRestartedMasterPromotionBesideAnotherMasterWithCache(t *testing.T) {
	for _, tc := range []struct {
		name        string
		dc2Keys     int64
		dc2KeysErr  error
		wantPromote bool
	}{
		{"the other master holds keys", 900, nil, false},
		{"the other master's key count is unreadable", 0, errors.New("no keyspace section"), false},
		{"the other master is empty: it has nothing to lose", 0, nil, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			dc3 := &fakeDragonflyConn{info: dragonflyReplicaOf(restartTestDC1Host), keys: 300}
			dc3.info.MasterReplID, dc3.info.MasterLinkStatus = restartTestOldReplID, "down"
			h := threeSiteRestartHarness(t, dc3)
			h.dc1.info = dragonfly.ReplicationInfo{Role: "master", MasterReplID: restartTestNewReplID}
			h.dc1.keys = 0
			h.dc2.info = dragonfly.ReplicationInfo{Role: "master", MasterReplID: "81196fd297ab5946b6ceb24a817c963c091418c0"}
			h.dc2.keys, h.dc2.keysErr = tc.dc2Keys, tc.dc2KeysErr
			h.tick(1)
			if got := h.promoted() == "dc3"; got != tc.wantPromote {
				t.Fatalf("dc3 promoted = %v, want %v\n%s", got, tc.wantPromote, h.logs.String())
			}
			if tc.wantPromote {
				return
			}
			if dc3.replicaOfNoOneCalls != 0 {
				t.Fatalf("REPLICAOF NO ONE sent to dc3 %d times: a third master beside dc2", dc3.replicaOfNoOneCalls)
			}
			if tc.dc2KeysErr == nil && len(h.dc2.replicaOfArgs) != 0 {
				t.Fatalf("dc2, which holds the newest data, was re-attached: REPLICAOF %v", h.dc2.replicaOfArgs)
			}
			if st := dragonflyStatusOf(t, h.c, h.key); st.ActiveSite != "dc1" {
				t.Fatalf("activeSite = %q, want the recorded dc1 as before", st.ActiveSite)
			}
		})
	}
}

// With three sites and a master that is gone, the threshold promotion
// chooses the replica that executed the most of the master's journal; the
// other is then attached to it. Before, it required exactly one healthy
// replica and never fired.
func TestDragonflyManager_Tick_ThresholdPromotesTheMostAdvancedOfTwoReplicas(t *testing.T) {
	newH := func(t *testing.T) (*restartHarness, *fakeDragonflyConn) {
		dc3 := &fakeDragonflyConn{info: dragonflyReplicaOf(restartTestDC1Host), keys: 300}
		dc3.info.MasterReplID = restartTestOldReplID
		h := threeSiteRestartHarness(t, dc3)
		h.dc2.info.SlaveReplOffset, h.dc2.info.SlaveReplOffsetReported = 5022, true
		dc3.info.SlaveReplOffset, dc3.info.SlaveReplOffsetReported = 5052, true
		h.tick(1)
		h.missMaster()
		h.dc2.info.MasterLinkStatus, dc3.info.MasterLinkStatus = "down", "down"
		return h, dc3
	}
	t.Run("the replica ahead is promoted at the threshold", func(t *testing.T) {
		h, dc3 := newH(t)
		h.tick(defaultDragonflyMasterLossThreshold - 1)
		if got := h.promoted(); got != "" {
			t.Fatalf("promoted %q before the threshold", got)
		}
		h.tick(1)
		if got := h.promoted(); got != "dc3" {
			t.Fatalf("promoted %q, want dc3, which is 30 journal records ahead of dc2\n%s", got, h.logs.String())
		}
		if dc3.replicaOfNoOneCalls != 1 || h.dc2.replicaOfNoOneCalls != 0 {
			t.Fatalf("REPLICAOF NO ONE: dc3 %d, dc2 %d; want 1 and 0", dc3.replicaOfNoOneCalls, h.dc2.replicaOfNoOneCalls)
		}
	})
	t.Run("equal offsets: the first in spec order", func(t *testing.T) {
		h, dc3 := newH(t)
		dc3.info.SlaveReplOffset = h.dc2.info.SlaveReplOffset
		h.tick(defaultDragonflyMasterLossThreshold)
		if got := h.promoted(); got != "dc2" {
			t.Fatalf("promoted %q, want dc2", got)
		}
	})
	for _, tc := range []struct {
		name   string
		mutate func(h *restartHarness, dc3 *fakeDragonflyConn)
	}{
		{"replicas of different master instances", func(_ *restartHarness, dc3 *fakeDragonflyConn) {
			dc3.info.MasterReplID = restartTestNewReplID
		}},
		{"a replica that reports no offset", func(h *restartHarness, _ *fakeDragonflyConn) {
			h.dc2.info.SlaveReplOffsetReported = false
		}},
		{"a replica that reports no replication id", func(h *restartHarness, _ *fakeDragonflyConn) {
			h.dc2.info.MasterReplID = ""
		}},
	} {
		t.Run(tc.name+": nothing to compare, nothing promoted", func(t *testing.T) {
			h, dc3 := newH(t)
			tc.mutate(h, dc3)
			h.tick(defaultDragonflyMasterLossThreshold + 2)
			if got := h.promoted(); got != "" {
				t.Fatalf("promoted %q on offsets that cannot be compared", got)
			}
			const msg = "dragonfly: active master not observed and its replicas cannot be compared; not promoting any"
			if n := strings.Count(h.logs.String(), msg); n != 1 {
				t.Fatalf("logged %d times, want once per outage\n%s", n, h.logs.String())
			}
		})
	}
}

// The restarted-master promotion is a role change and waits while the
// manager is paused: a planned failover's run or its automation hold owns
// the group's Dragonfly, or a snapshot upgrade is in flight. The manager
// still observes and publishes status.
func TestDragonflyManager_Tick_RestartedMasterDoesNotPromoteWhilePaused(t *testing.T) {
	for _, tc := range []struct {
		name  string
		pause func(t *testing.T, h *restartHarness) (resume func())
	}{
		{"a planned failover run owns Dragonfly", func(t *testing.T, h *restartHarness) func() {
			slot := &PlannedClaimSlot{}
			h.mgr.SetPlannedOwner(slot.Held)
			if !slot.hydrate(plannedClaim{mode: claimModeRun, runID: "run-1", source: "dc1", target: "dc2"}) {
				t.Fatal("setup: claim not installed")
			}
			return func() { slot.release("run-1", ReleaseTerminal) }
		}},
		{"a planned-failover automation hold stands", func(t *testing.T, h *restartHarness) func() {
			slot := &PlannedClaimSlot{}
			h.mgr.SetPlannedOwner(slot.Held)
			if !slot.toHold("run-1") {
				t.Fatal("setup: hold not installed")
			}
			return func() { slot.release("run-1", ReleaseAck) }
		}},
		{"a snapshot upgrade is in flight", func(_ *testing.T, h *restartHarness) func() {
			h.mgr.SetUpgradePaused(true)
			return func() { h.mgr.SetUpgradePaused(false) }
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			h := newRestartHarness(t, nil)
			h.tick(1)
			resume := tc.pause(t, h)
			if !h.mgr.paused() {
				t.Fatal("setup: the manager is not paused")
			}
			h.masterRestarted(0)
			h.tick(3)
			if promoted, reattached := h.outcome(); promoted || reattached || h.dc2.replicaOfNoOneCalls != 0 {
				t.Fatalf("paused: promoted=%v reattached=%v REPLICAOF NO ONE=%d, want no command\n%s",
					promoted, reattached, h.dc2.replicaOfNoOneCalls, h.logs.String())
			}
			// Still observing: the replica's link state is published.
			if st := dragonflyStatusOf(t, h.c, h.key); len(st.Sites) != 2 || st.Sites[1].LinkStatus != "down" {
				t.Fatalf("status not published while paused: %+v", st.Sites)
			}
			resume()
			if h.mgr.paused() {
				t.Fatal("still paused after the owner released")
			}
			h.tick(1)
			h.wantPromotedNotReattached("the first cycle after the pause ended")
		})
	}
}
