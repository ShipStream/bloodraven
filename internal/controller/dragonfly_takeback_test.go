package controller

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"

	v1alpha1 "github.com/shipstream/bloodraven/api/v1alpha1"
	"github.com/shipstream/bloodraven/internal/dragonfly"
)

const (
	takeBackNotInSyncMsg = "dragonfly/mysql active-site drift: target not in sync with the Dragonfly master; not taking over"
	takeBackDelayedMsg   = "dragonfly: active master was replaced repeatedly; delaying the take-back to the MySQL active site"
	takeBackPromotingMsg = "dragonfly/mysql active-site drift: promoting Dragonfly replica to match MySQL"
	noFallbackMsg        = "dragonfly emergency: REPLTAKEOVER failed; not falling back to REPLICAOF NO ONE"
	restartTestDC2Host   = "lion-dragonfly-dc2.shared-lion.svc.cluster.local"
)

// ledElsewhere puts the harness in the state a restarted-master promotion
// leaves once its record is 40 s old: MySQL is active on dc1, the
// Dragonfly master is dc2 with 300 keys, and dc1 is its replica, in sync at
// the same position.
func (h *restartHarness) ledElsewhere() {
	h.t.Helper()
	var fg v1alpha1.MysqlFailoverGroup
	if err := h.c.Get(context.Background(), h.key, &fg); err != nil {
		h.t.Fatal(err)
	}
	then := metav1.NewTime(time.Now().Add(-40 * time.Second))
	fg.Status.Dragonfly.ActiveSite = "dc2"
	fg.Status.Dragonfly.LastPromotionTarget = "dc2"
	fg.Status.Dragonfly.LastPromotionTime = &then
	if err := h.c.Status().Update(context.Background(), &fg); err != nil {
		h.t.Fatal(err)
	}
	h.dc2.info = dragonfly.ReplicationInfo{Role: "master", ConnectedSlaves: 1, MasterReplID: restartTestNewReplID, MasterReplOffset: 500}
	h.dc2.keys = 300
	h.dc1.info = dragonflyReplicaOf(restartTestDC2Host)
	h.dc1.info.MasterReplID, h.dc1.info.SlaveReplOffset = restartTestNewReplID, 500
	h.dc1.keys = 300
	// The pods carry the roles the promotion stamped (a deleted pod is
	// skipped).
	for site, role := range map[string]string{"dc2": "master", "dc1": "replica"} {
		if err := h.mgr.setRoleLabel(context.Background(), &fg, site, role); err != nil {
			h.t.Fatal(err)
		}
	}
	h.mgr.driftSyncWait = 50 * time.Millisecond
	shrinkEmergencyWaits(h.mgr)
}

// takenBack reports whether the Dragonfly master was moved back to dc1.
func (h *restartHarness) takenBack() bool {
	h.t.Helper()
	return dragonflyStatusOf(h.t, h.c, h.key).ActiveSite == "dc1"
}

func (h *restartHarness) podLabels(site string) map[string]string {
	h.t.Helper()
	var pod corev1.Pod
	name := makeDragonflyPod(h.key.Name, site, "", false).Name
	if err := h.c.Get(context.Background(), types.NamespacedName{Namespace: h.key.Namespace, Name: name}, &pod); err != nil {
		h.t.Fatal(err)
	}
	return pod.Labels
}

// The take-back that makes Dragonfly follow MySQL takes over from a master
// that is alive, so it is made only for a replica proven in sync with it:
// stable link and, on every flow, the master's replication position.
// Before, a replica that had ever received data qualified. One with its
// link down and 10 of 300 keys was sent REPLTAKEOVER, refused (`Full sync
// not done`), and then made master with REPLICAOF NO ONE over the master
// that held the cache, whose pod was deleted.
func TestDragonflyManager_Tick_TakeBackNeedsTheSyncProof(t *testing.T) {
	notReady := func(t *testing.T, h *restartHarness, wantState string) {
		t.Helper()
		h.dc1.replTakeoverErr = &dragonfly.ServerError{Message: "ERR Full sync not done"}
		h.tick(3)
		if h.dc1.replTakeoverCalls != 0 || h.dc1.replicaOfNoOneCalls != 0 || h.takenBack() {
			t.Fatalf("REPLTAKEOVER %d, REPLICAOF NO ONE %d, taken back %v; want nothing sent to a target that is %s\n%s",
				h.dc1.replTakeoverCalls, h.dc1.replicaOfNoOneCalls, h.takenBack(), wantState, h.logs.String())
		}
		logs := h.logs.String()
		if n := strings.Count(logs, takeBackNotInSyncMsg); n != 1 || !strings.Contains(logs, "state="+wantState) {
			t.Fatalf("the wait was logged %d times over three cycles, want once with state %s\n%s", n, wantState, logs)
		}
		if got := h.podLabels("dc2")[v1alpha1.DragonflyTrafficLabel]; got != dragonflyTrafficEnabled {
			t.Fatalf("the master's traffic label = %q; it must keep serving", got)
		}
	}
	t.Run("link down with a part of the cache", func(t *testing.T) {
		h := newRestartHarness(t, nil)
		h.ledElsewhere()
		h.dc1.info.MasterLinkStatus, h.dc1.keys = "down", 10
		notReady(t, h, dragonflyRolloutNotLinked)
		// The replica repair still runs: it is what gets the target in sync.
		if len(h.dc1.replicaOfArgs) == 0 || h.dc1.replicaOfArgs[0] != restartTestDC2Host {
			t.Fatalf("the link-down target was not re-attached to the master: %v", h.dc1.replicaOfArgs)
		}
	})
	t.Run("full sync in progress", func(t *testing.T) {
		h := newRestartHarness(t, nil)
		h.ledElsewhere()
		h.dc1.info.MasterSyncInProgress = true
		notReady(t, h, dragonflyRolloutNotLinked)
	})
	t.Run("healthy link, behind the master's position", func(t *testing.T) {
		h := newRestartHarness(t, nil)
		h.ledElsewhere()
		h.dc1.info.SlaveReplOffset = 120
		notReady(t, h, dragonflyRolloutBehind)
	})
	t.Run("the master's position cannot be read", func(t *testing.T) {
		h := newRestartHarness(t, nil)
		h.ledElsewhere()
		h.dc2.positionErr = errors.New("i/o timeout")
		notReady(t, h, dragonflyRolloutUnreadable)
	})
	t.Run("in sync: taken over with REPLTAKEOVER", func(t *testing.T) {
		h := newRestartHarness(t, nil)
		h.ledElsewhere()
		h.tick(1)
		if h.dc1.replTakeoverCalls != 1 || h.dc1.replicaOfNoOneCalls != 0 || !h.takenBack() {
			t.Fatalf("REPLTAKEOVER %d, REPLICAOF NO ONE %d, taken back %v; want one takeover\n%s",
				h.dc1.replTakeoverCalls, h.dc1.replicaOfNoOneCalls, h.takenBack(), h.logs.String())
		}
	})
}

// A take-back that waits for two minutes is reported once.
func TestDragonflyManager_Tick_TakeBackStallIsReportedOnce(t *testing.T) {
	h := newRestartHarness(t, nil)
	h.ledElsewhere()
	h.dc1.info.MasterLinkStatus = "down"
	h.tick(1)
	if got := h.eventsOf(v1alpha1.EventDragonflyFollowStalled); len(got) != 0 {
		t.Fatalf("stall reported at once: %v", got)
	}
	h.advance(dragonflyFollowStallAfter)
	h.tick(3)
	events := h.eventsOf(v1alpha1.EventDragonflyFollowStalled)
	if len(events) != 1 || !strings.Contains(events[0], dragonflyRolloutNotLinked) {
		t.Fatalf("DragonflyFollowStalled events = %v, want one naming the state", events)
	}
	// In sync again: the take-back runs and the record ends.
	h.dc1.info.MasterLinkStatus = "up"
	h.tick(1)
	if !h.takenBack() {
		t.Fatalf("not taken back once the target was in sync\n%s", h.logs.String())
	}
}

// After a REPLTAKEOVER that failed, REPLICAOF NO ONE is for an old master
// that is gone. While the old master still answers as a master it keeps
// the role: the target is not made master with whatever it holds, and the
// old master's pod is not deleted.
func TestDragonflyManager_Promote_NoFallbackOverAMasterThatStillServes(t *testing.T) {
	type promote func(h *restartHarness) bool
	emergency := func(h *restartHarness) bool { return h.mgr.TryEmergencyPromote(context.Background(), "dc1", "dc2") }
	takeBack := func(h *restartHarness) bool { h.tick(1); return h.takenBack() }
	rollout := func(h *restartHarness) bool { return h.mgr.TryRolloutPromote(context.Background(), "dc1", "dc2") }
	for _, tc := range []struct {
		name string
		run  promote
		err  error
	}{
		{"the take-back, refused by the target", takeBack, &dragonfly.ServerError{Message: "ERR Full sync not done"}},
		{"the take-back, reply lost", takeBack, errors.New("i/o timeout")},
		{"a promotion MySQL asked for, refused by the target", emergency, &dragonfly.ServerError{Message: "ERR Full sync not done"}},
		{"a promotion MySQL asked for, target gave up waiting", emergency,
			&dragonfly.ServerError{Message: "ERR Couldn't execute takeover: Operation canceled"}},
		{"a rollout promotion, refused by the target", rollout, &dragonfly.ServerError{Message: "ERR Full sync not done"}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			h := newRestartHarness(t, nil)
			h.ledElsewhere()
			h.dc1.replTakeoverErr = tc.err
			if tc.run(h) {
				t.Fatalf("promoted although the takeover failed\n%s", h.logs.String())
			}
			if h.dc1.replTakeoverCalls != 1 {
				t.Fatalf("REPLTAKEOVER sent %d times, want once", h.dc1.replTakeoverCalls)
			}
			if h.dc1.replicaOfNoOneCalls != 0 {
				t.Fatalf("REPLICAOF NO ONE sent %d times over a master that still serves\n%s", h.dc1.replicaOfNoOneCalls, h.logs.String())
			}
			if !strings.Contains(h.logs.String(), noFallbackMsg) {
				t.Fatalf("the refusal to fall back was not logged\n%s", h.logs.String())
			}
			st := dragonflyStatusOf(t, h.c, h.key)
			if st.ActiveSite != "dc2" {
				t.Fatalf("activeSite = %q, want dc2 to keep the role", st.ActiveSite)
			}
			labels := h.podLabels("dc2")
			if labels[v1alpha1.DragonflyTrafficLabel] != dragonflyTrafficEnabled || labels[v1alpha1.DragonflyRoleLabel] != "master" {
				t.Fatalf("old master labels = %v, want master with its traffic label put back", labels)
			}
			if len(h.dc2.clientKillTypes) != 0 {
				t.Fatalf("the old master's clients were killed after a promotion that did not happen")
			}
			if got := h.eventsOf(v1alpha1.EventDragonflyPromotionFailed); len(got) != 1 {
				t.Fatalf("DragonflyPromotionFailed events = %v, want one", got)
			}
			if got := h.eventsOf(v1alpha1.EventDragonflySessionsLost); len(got) != 0 {
				t.Fatalf("DragonflySessionsLost emitted for a promotion that did not happen: %v", got)
			}
		})
	}
	// The fallback still exists for what it is for.
	t.Run("the old master is gone after the takeover failed: REPLICAOF NO ONE", func(t *testing.T) {
		h := newRestartHarness(t, nil)
		h.ledElsewhere()
		h.dc1.replTakeoverErr = &dragonfly.ServerError{Message: "ERR Couldn't execute takeover: Operation canceled"}
		// It answers the probe before the takeover and no dial after it.
		h.conn.programQueue(holdTestDC2, h.dc2)
		h.conn.mu.Lock()
		delete(h.conn.byAdd, holdTestDC2)
		h.conn.mu.Unlock()
		if !h.mgr.TryEmergencyPromote(context.Background(), "dc1", "dc2") || h.dc1.replicaOfNoOneCalls != 1 {
			t.Fatalf("REPLICAOF NO ONE sent %d times, want once: the old master no longer answers\n%s", h.dc1.replicaOfNoOneCalls, h.logs.String())
		}
	})
	t.Run("the old master answers as a replica: REPLICAOF NO ONE", func(t *testing.T) {
		h := newRestartHarness(t, nil)
		h.ledElsewhere()
		h.dc1.replTakeoverErr = &dragonfly.ServerError{Message: "ERR Full sync not done"}
		h.dc2.info = dragonflyReplicaOf(restartTestDC1Host)
		if !h.mgr.TryEmergencyPromote(context.Background(), "dc1", "dc2") || h.dc1.replicaOfNoOneCalls != 1 {
			t.Fatalf("REPLICAOF NO ONE sent %d times, want once: there is no master to keep\n%s", h.dc1.replicaOfNoOneCalls, h.logs.String())
		}
	})
	// A server without REPLTAKEOVER can only ever be promoted with
	// REPLICAOF NO ONE. Over a master that still serves, that is done for
	// a target in stable sync and for no other.
	unknown := &dragonfly.ServerError{Message: "ERR unknown command `REPLTAKEOVER`"}
	t.Run("no REPLTAKEOVER on the server, target in stable sync: REPLICAOF NO ONE", func(t *testing.T) {
		h := newRestartHarness(t, nil)
		h.ledElsewhere()
		h.dc1.replTakeoverErr = unknown
		if !h.mgr.TryEmergencyPromote(context.Background(), "dc1", "dc2") || h.dc1.replicaOfNoOneCalls != 1 {
			t.Fatalf("REPLICAOF NO ONE sent %d times, want once\n%s", h.dc1.replicaOfNoOneCalls, h.logs.String())
		}
	})
	t.Run("no REPLTAKEOVER on the server, target link down: nothing", func(t *testing.T) {
		h := newRestartHarness(t, nil)
		h.ledElsewhere()
		h.dc1.replTakeoverErr = unknown
		h.dc1.info.MasterLinkStatus = "down"
		if h.mgr.TryEmergencyPromote(context.Background(), "dc1", "dc2") || h.dc1.replicaOfNoOneCalls != 0 {
			t.Fatalf("REPLICAOF NO ONE sent %d times to a link-down target over a master that serves\n%s", h.dc1.replicaOfNoOneCalls, h.logs.String())
		}
	})
}

// When a site refuses the position command, the sync can never be proven
// by position. With the link checks holding, the take-back goes ahead with
// REPLTAKEOVER alone, says so once, and never falls back to REPLICAOF NO
// ONE, also not when the old master is gone by then.
func TestDragonflyManager_Tick_TakeBackWithoutAPositionIsTakeoverOnly(t *testing.T) {
	refusal := &dragonfly.ServerError{Message: "NOPERM ops has no ACL permissions"}
	t.Run("the takeover succeeds", func(t *testing.T) {
		h := newRestartHarness(t, nil)
		h.ledElsewhere()
		h.dc1.positionErr = refusal
		h.tick(1)
		if h.dc1.replTakeoverCalls != 1 || h.dc1.replicaOfNoOneCalls != 0 || !h.takenBack() {
			t.Fatalf("REPLTAKEOVER %d, REPLICAOF NO ONE %d, taken back %v\n%s",
				h.dc1.replTakeoverCalls, h.dc1.replicaOfNoOneCalls, h.takenBack(), h.logs.String())
		}
		if got := h.eventsOf(v1alpha1.EventDragonflySyncUnproven); len(got) != 1 {
			t.Fatalf("DragonflySyncUnproven events = %v, want one", got)
		}
	})
	t.Run("the master refuses the position command and the takeover fails: no fallback", func(t *testing.T) {
		h := newRestartHarness(t, nil)
		h.ledElsewhere()
		h.dc2.positionErr = refusal
		h.dc1.replTakeoverErr = &dragonfly.ServerError{Message: "ERR Full sync not done"}
		h.tick(1)
		if h.dc1.replTakeoverCalls != 1 {
			t.Fatalf("REPLTAKEOVER sent %d times, want once\n%s", h.dc1.replTakeoverCalls, h.logs.String())
		}
		if h.dc1.replicaOfNoOneCalls != 0 || h.takenBack() {
			t.Fatalf("REPLICAOF NO ONE sent %d times after a takeover-only promotion failed\n%s", h.dc1.replicaOfNoOneCalls, h.logs.String())
		}
		if !strings.Contains(h.logs.String(), "the target's sync was not proven by position") {
			t.Fatalf("the reason was not logged\n%s", h.logs.String())
		}
	})
	t.Run("the takeover fails and the old master is gone by then: still no fallback", func(t *testing.T) {
		h := newRestartHarness(t, nil)
		h.ledElsewhere()
		h.dc1.positionErr = refusal
		h.dc1.replTakeoverErr = &dragonfly.ServerError{Message: "ERR Full sync not done"}
		h.mgr.SetConnector(func(ctx context.Context, addr, password string) (DragonflyConnection, error) {
			if addr == holdTestDC2 && h.dc1.replTakeoverCalls > 0 {
				return nil, errors.New("connection refused")
			}
			return h.conn.connect(ctx, addr, password)
		})
		h.tick(1)
		if h.dc1.replTakeoverCalls != 1 || h.dc1.replicaOfNoOneCalls != 0 || h.takenBack() {
			t.Fatalf("REPLTAKEOVER %d, REPLICAOF NO ONE %d, taken back %v; want one takeover and no fallback\n%s",
				h.dc1.replTakeoverCalls, h.dc1.replicaOfNoOneCalls, h.takenBack(), h.logs.String())
		}
	})
	t.Run("the link checks do not hold: nothing is sent", func(t *testing.T) {
		h := newRestartHarness(t, nil)
		h.ledElsewhere()
		h.dc1.positionErr = refusal
		h.dc1.info.MasterReplID = restartTestOldReplID // follows another instance
		h.tick(1)
		if h.dc1.replTakeoverCalls != 0 || h.dc1.replicaOfNoOneCalls != 0 {
			t.Fatalf("REPLTAKEOVER %d, REPLICAOF NO ONE %d; want nothing", h.dc1.replTakeoverCalls, h.dc1.replicaOfNoOneCalls)
		}
		if got := h.eventsOf(v1alpha1.EventDragonflySyncUnproven); len(got) != 0 {
			t.Fatalf("DragonflySyncUnproven emitted for a target that is not linked: %v", got)
		}
	})
}

// promoteLocked with dragonflyTakeoverOnly never sends REPLICAOF NO ONE:
// not after a failed takeover whose old master then vanished, and not for
// an old master that does not answer the probe.
func TestDragonflyManager_Promote_TakeoverOnlyNeverFallsBack(t *testing.T) {
	promote := func(h *restartHarness) bool {
		var fg v1alpha1.MysqlFailoverGroup
		if err := h.c.Get(context.Background(), h.key, &fg); err != nil {
			h.t.Fatal(err)
		}
		release, ok := h.mgr.gate.tryAcquire()
		if !ok {
			h.t.Fatal("gate busy")
		}
		defer release()
		return h.mgr.promoteLocked(context.Background(), &fg, "dc1", "dc2", time.Second, dragonflyTakeoverOnly)
	}
	t.Run("takeover failed, old master gone", func(t *testing.T) {
		h := newRestartHarness(t, nil)
		h.ledElsewhere()
		h.dc1.replTakeoverErr = &dragonfly.ServerError{Message: "ERR Full sync not done"}
		h.conn.programQueue(holdTestDC2, h.dc2)
		h.conn.mu.Lock()
		delete(h.conn.byAdd, holdTestDC2)
		h.conn.mu.Unlock()
		if promote(h) || h.dc1.replTakeoverCalls != 1 || h.dc1.replicaOfNoOneCalls != 0 {
			t.Fatalf("REPLTAKEOVER %d, REPLICAOF NO ONE %d; want 1 and 0\n%s", h.dc1.replTakeoverCalls, h.dc1.replicaOfNoOneCalls, h.logs.String())
		}
	})
	t.Run("old master does not answer the probe", func(t *testing.T) {
		h := newRestartHarness(t, nil)
		h.ledElsewhere()
		h.conn.dialFails(holdTestDC2, errors.New("connection refused"))
		if promote(h) || h.dc1.replTakeoverCalls != 0 || h.dc1.replicaOfNoOneCalls != 0 {
			t.Fatalf("REPLTAKEOVER %d, REPLICAOF NO ONE %d; want nothing sent\n%s", h.dc1.replTakeoverCalls, h.dc1.replicaOfNoOneCalls, h.logs.String())
		}
	})
	t.Run("takeover succeeds", func(t *testing.T) {
		h := newRestartHarness(t, nil)
		h.ledElsewhere()
		if !promote(h) || h.dc1.replTakeoverCalls != 1 || h.dc1.replicaOfNoOneCalls != 0 {
			t.Fatalf("REPLTAKEOVER %d, REPLICAOF NO ONE %d; want the takeover", h.dc1.replTakeoverCalls, h.dc1.replicaOfNoOneCalls)
		}
	})
}

// A master that keeps restarting is replaced each time, and each time the
// master would be taken back to it 30 s later, to be lost again at its next
// restart. The take-back waits longer after each replacement of the same
// site: 30 s, 1 m, 2 m, up to 15 m.
func TestDragonflyManager_TakeBackHold(t *testing.T) {
	h := newRestartHarness(t, nil)
	if wait, n := h.mgr.takeBackHold("dc1"); wait != 0 || n != 0 {
		t.Fatalf("hold = %s after %d replacements, want none for a site that was never replaced", wait, n)
	}
	want := []time.Duration{30 * time.Second, time.Minute, 2 * time.Minute, 4 * time.Minute, 8 * time.Minute,
		15 * time.Minute, 15 * time.Minute}
	for i, delay := range want {
		h.mgr.noteMasterReplaced("dc1")
		if wait, n := h.mgr.takeBackHold("dc1"); wait != delay || n != i+1 {
			t.Fatalf("after replacement %d: hold = %s (%d), want %s", i+1, wait, n, delay)
		}
		if wait, _ := h.mgr.takeBackHold("dc2"); wait != 0 {
			t.Fatalf("hold = %s for another site", wait)
		}
		h.advance(delay - time.Second)
		if wait, _ := h.mgr.takeBackHold("dc1"); wait != time.Second {
			t.Fatalf("after replacement %d: %s left one second before the delay ends, want 1s", i+1, wait)
		}
		h.advance(time.Second)
		if wait, _ := h.mgr.takeBackHold("dc1"); wait != 0 {
			t.Fatalf("after replacement %d: %s left when the delay has passed", i+1, wait)
		}
	}
	// A site that stayed up for dragonflyTakeBackForget starts again.
	h.advance(dragonflyTakeBackForget + time.Second)
	h.mgr.noteMasterReplaced("dc1")
	if wait, n := h.mgr.takeBackHold("dc1"); wait != 30*time.Second || n != 1 {
		t.Fatalf("hold = %s (%d) after a quiet half hour, want the count to start again", wait, n)
	}
	// Another site's replacement starts its own count.
	h.mgr.noteMasterReplaced("dc2")
	if wait, n := h.mgr.takeBackHold("dc2"); wait != 30*time.Second || n != 1 {
		t.Fatalf("hold = %s (%d) for a newly replaced site", wait, n)
	}
	if wait, _ := h.mgr.takeBackHold("dc1"); wait != 0 {
		t.Fatalf("hold = %s for the site replaced before", wait)
	}
}

// A master that stays away past the threshold is replaced by the threshold
// promotion; that counts toward the delay like a restarted master does.
func TestDragonflyManager_Tick_ThresholdPromotionDelaysTheTakeBack(t *testing.T) {
	h := newRestartHarness(t, nil)
	h.tick(1)
	h.missMaster()
	h.dc2.info.MasterLinkStatus = "down"
	h.tick(defaultDragonflyMasterLossThreshold)
	if got := h.promoted(); got != "dc2" {
		t.Fatalf("promoted %q, want dc2", got)
	}
	if wait, n := h.mgr.takeBackHold("dc1"); wait != dragonflyTakeBackBaseDelay || n != 1 {
		t.Fatalf("hold = %s after %d replacements, want %s after 1", wait, n, dragonflyTakeBackBaseDelay)
	}
}

// The cycle of a crash-looping active master, on the manager: each restart
// is answered by a promotion of the replica, and the take-back that follows
// waits out its delay, which doubles.
func TestDragonflyManager_Tick_CrashLoopingMasterDoesNotCycleUnbounded(t *testing.T) {
	h := newRestartHarness(t, nil)
	h.mgr.pollInterval = 2 * time.Second
	h.mgr.driftSyncWait = 50 * time.Millisecond
	shrinkEmergencyWaits(h.mgr)
	h.tick(1)

	// restart makes dc1, the master on the MySQL active site, come back
	// empty, and runs the cycle that promotes dc2 in its place.
	restart := func(n int) {
		t.Helper()
		h.dc2.replicaOfNoOneCalls = 0
		h.masterRestarted(0)
		h.tick(1)
		if h.dc2.replicaOfNoOneCalls != 1 || dragonflyStatusOf(t, h.c, h.key).ActiveSite != "dc2" {
			t.Fatalf("restart %d: replica not promoted\n%s", n, h.logs.String())
		}
		h.ledElsewhere() // dc1 re-attached and in sync; the record's 30 s have passed
	}
	// takenBackAfter runs cycles two seconds apart for d and reports
	// whether any of them took the master back.
	takenBackAfter := func(d time.Duration) bool {
		t.Helper()
		for elapsed := time.Duration(0); elapsed < d; elapsed += 2 * time.Second {
			h.advance(2 * time.Second)
			h.tick(1)
			if h.takenBack() {
				return true
			}
		}
		return false
	}
	// back puts the master back on dc1 with dc2 as its replica in sync.
	back := func() {
		h.dc1.info = dragonfly.ReplicationInfo{Role: "master", ConnectedSlaves: 1, MasterReplID: restartTestOldReplID}
		h.dc2.info = dragonflyReplicaOf(restartTestDC1Host)
		h.dc2.info.MasterReplID = restartTestOldReplID
		h.dc1.replTakeoverCalls = 0
		h.tick(1)
	}

	restart(1)
	if takenBackAfter(28 * time.Second) {
		t.Fatalf("taken back within 28 s of the first replacement\n%s", h.logs.String())
	}
	if !takenBackAfter(4 * time.Second) {
		t.Fatalf("not taken back 30 s after the first replacement\n%s", h.logs.String())
	}
	back()

	restart(2)
	if takenBackAfter(58 * time.Second) {
		t.Fatalf("taken back within 58 s of the second replacement in a row: the delay did not double\n%s", h.logs.String())
	}
	if n := strings.Count(h.logs.String(), takeBackDelayedMsg); n != 2 {
		t.Fatalf("the delay was logged %d times, want once per replacement\n%s", n, h.logs.String())
	}
	if h.dc1.replTakeoverCalls != 0 {
		t.Fatalf("REPLTAKEOVER sent %d times during the delay", h.dc1.replTakeoverCalls)
	}
	if !takenBackAfter(4 * time.Second) {
		t.Fatalf("not taken back a minute after the second replacement\n%s", h.logs.String())
	}
	back()

	restart(3)
	if takenBackAfter(118 * time.Second) {
		t.Fatalf("taken back within 118 s of the third replacement in a row\n%s", h.logs.String())
	}
	// Three restarts in under four minutes: three DragonflySessionsLost
	// Events, and no more until the third delay ends.
	if got := h.eventsOf(v1alpha1.EventDragonflySessionsLost); len(got) != 3 {
		t.Fatalf("DragonflySessionsLost events = %d, want 3", len(got))
	}
}
