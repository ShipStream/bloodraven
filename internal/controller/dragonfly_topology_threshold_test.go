package controller

import (
	"bytes"
	"context"
	"errors"
	"log/slog"
	"strings"
	"testing"
	"time"

	"k8s.io/client-go/tools/record"
	"sigs.k8s.io/controller-runtime/pkg/client"

	v1alpha1 "github.com/shipstream/bloodraven/api/v1alpha1"
	"github.com/shipstream/bloodraven/internal/dragonfly"
)

const dragonflyMasterWaitMsg = "dragonfly: active master not observed; waiting before promoting a replica"

// thresholdHarness is a two-site group whose Dragonfly master is dc1 (also
// the MySQL active site) with dc2 as its replica.
type thresholdHarness struct {
	t    *testing.T
	c    client.Client
	key  client.ObjectKey
	mgr  *DragonflyManager
	conn *fakeConnector
	logs *bytes.Buffer
	dc1  *fakeDragonflyConn
	dc2  *fakeDragonflyConn
}

func newThresholdHarness(t *testing.T, mutate func(*v1alpha1.MysqlFailoverGroup)) *thresholdHarness {
	t.Helper()
	fg := fgWithDragonflyEnabledAndActive()
	fg.Status.Dragonfly = &v1alpha1.DragonflyStatus{ActiveSite: "dc1"}
	if mutate != nil {
		mutate(fg)
	}
	cb, key := newDragonflyFakeClient(fg)
	c := cb.WithObjects(
		makeDragonflyPod(fg.Name, "dc1", "master", true),
		makeDragonflyPod(fg.Name, "dc2", "replica", true),
	).Build()
	h := &thresholdHarness{t: t, c: c, key: key, conn: newFakeConnector(), logs: &bytes.Buffer{}}
	h.mgr = NewDragonflyManager(c, record.NewFakeRecorder(64), slog.New(slog.NewTextHandler(h.logs, nil)), key, 50*time.Millisecond)
	h.dc1 = &fakeDragonflyConn{info: dragonfly.ReplicationInfo{Role: "master", ConnectedSlaves: 1}, keys: 7}
	// A replica in stable sync that holds the cache: the master's silence
	// is all the operator has.
	h.dc2 = &fakeDragonflyConn{info: dragonflyReplicaOf("lion-dragonfly-dc1.shared-lion.svc.cluster.local"), keys: 7}
	h.conn.program(holdTestDC1, h.dc1)
	h.conn.program(holdTestDC2, h.dc2)
	h.mgr.SetConnector(h.conn.connect)
	return h
}

// missMaster makes dc1 unobservable; seeMaster makes it answer again.
func (h *thresholdHarness) missMaster() {
	h.conn.dialFails(holdTestDC1, errors.New("i/o timeout"))
}

func (h *thresholdHarness) seeMaster() {
	h.conn.mu.Lock()
	delete(h.conn.dialErrs, holdTestDC1)
	h.conn.mu.Unlock()
}

func (h *thresholdHarness) tick(n int) {
	for i := 0; i < n; i++ {
		h.mgr.Tick(context.Background())
	}
}

func (h *thresholdHarness) promoted() string {
	h.t.Helper()
	return dragonflyStatusOf(h.t, h.c, h.key).LastPromotionTarget
}

func (h *thresholdHarness) waitLogged() int {
	return strings.Count(h.logs.String(), dragonflyMasterWaitMsg)
}

// One missed observation of the master is not its loss: nothing is
// promoted until the threshold's worth of consecutive observations missed
// it. Before, the first miss promoted the replica.
func TestDragonflyManager_Tick_OneMissedObservationDoesNotPromote(t *testing.T) {
	h := newThresholdHarness(t, nil)
	h.tick(1)
	h.missMaster()

	h.tick(1)
	if got := h.promoted(); got != "" {
		t.Fatalf("promoted %q after one missed observation of the master, want none", got)
	}
	if st := dragonflyStatusOf(t, h.c, h.key); st.ActiveSite != "dc1" {
		t.Fatalf("activeSite = %q, want dc1", st.ActiveSite)
	}
	h.tick(defaultDragonflyMasterLossThreshold - 2)
	if got := h.promoted(); got != "" {
		t.Fatalf("promoted %q after %d missed observations, want none before %d", got,
			defaultDragonflyMasterLossThreshold-1, defaultDragonflyMasterLossThreshold)
	}

	h.tick(1)
	if got := h.promoted(); got != "dc2" {
		t.Fatalf("promoted %q after %d consecutive missed observations, want dc2", got, defaultDragonflyMasterLossThreshold)
	}
	if st := dragonflyStatusOf(t, h.c, h.key); st.ActiveSite != "dc2" {
		t.Fatalf("activeSite = %q, want dc2", st.ActiveSite)
	}
}

// An observation of the master in between ends the count: the misses have
// to be consecutive.
func TestDragonflyManager_Tick_ObservedMasterResetsTheMissCount(t *testing.T) {
	h := newThresholdHarness(t, nil)
	h.missMaster()
	h.tick(defaultDragonflyMasterLossThreshold - 1)
	h.seeMaster()
	h.tick(1)
	h.missMaster()
	h.tick(defaultDragonflyMasterLossThreshold - 1)
	if got := h.promoted(); got != "" {
		t.Fatalf("promoted %q: the misses were not consecutive, want none", got)
	}
	h.tick(1)
	if got := h.promoted(); got != "dc2" {
		t.Fatalf("promoted %q after a full run of consecutive misses, want dc2", got)
	}
}

// The threshold is spec.failureThreshold, the count the MySQL monitor uses.
func TestDragonflyManager_Tick_MasterLossThresholdIsSpecFailureThreshold(t *testing.T) {
	for _, tc := range []struct {
		name      string
		threshold int32
		want      int
	}{
		{"unset uses the default", 0, defaultDragonflyMasterLossThreshold},
		{"five", 5, 5},
		{"one", 1, 1},
	} {
		t.Run(tc.name, func(t *testing.T) {
			h := newThresholdHarness(t, func(fg *v1alpha1.MysqlFailoverGroup) { fg.Spec.FailureThreshold = tc.threshold })
			h.missMaster()
			h.tick(tc.want - 1)
			if got := h.promoted(); got != "" {
				t.Fatalf("promoted %q after %d misses, want none before %d", got, tc.want-1, tc.want)
			}
			h.tick(1)
			if got := h.promoted(); got != "dc2" {
				t.Fatalf("promoted %q after %d misses, want dc2", got, tc.want)
			}
		})
	}
}

// The active site answering as a replica is an observation, not a miss: it
// was reached and says it is not the master. Answering without a role says
// nothing and is counted as a miss.
func TestDragonflyManager_Tick_ActiveSiteAnsweringInAnotherRole(t *testing.T) {
	t.Run("as a replica", func(t *testing.T) {
		h := newThresholdHarness(t, nil)
		h.dc1.info = dragonflyReplicaOf("lion-dragonfly-dc9.shared-lion.svc.cluster.local")
		h.tick(1)
		if got := h.promoted(); got != "dc2" {
			t.Fatalf("promoted %q, want dc2 at once: the active site says it is a replica", got)
		}
	})
	t.Run("without a role", func(t *testing.T) {
		h := newThresholdHarness(t, nil)
		h.dc1.info = dragonfly.ReplicationInfo{}
		h.tick(defaultDragonflyMasterLossThreshold - 1)
		if got := h.promoted(); got != "" {
			t.Fatalf("promoted %q before the threshold, want none: an answer without a role is a miss", got)
		}
		h.tick(1)
		if got := h.promoted(); got != "dc2" {
			t.Fatalf("promoted %q after the threshold, want dc2", got)
		}
	})
}

// The wait is logged once per outage, and again for the next one.
func TestDragonflyManager_Tick_MasterWaitLoggedOncePerOutage(t *testing.T) {
	h := newThresholdHarness(t, nil)
	h.missMaster()
	h.tick(defaultDragonflyMasterLossThreshold - 1)
	if n := h.waitLogged(); n != 1 {
		t.Fatalf("wait logged %d times over %d missed observations, want once\n%s", n, defaultDragonflyMasterLossThreshold-1, h.logs.String())
	}
	h.seeMaster()
	h.tick(1)
	h.missMaster()
	h.tick(1)
	if n := h.waitLogged(); n != 2 {
		t.Fatalf("wait logged %d times over two outages, want twice", n)
	}
}

// The count is the process's own. A new manager (an operator restart)
// starts from zero even when the master was already gone.
func TestDragonflyManager_Tick_MissCountStartsAgainWithTheProcess(t *testing.T) {
	h := newThresholdHarness(t, nil)
	h.missMaster()
	h.tick(defaultDragonflyMasterLossThreshold - 1)

	restarted := NewDragonflyManager(h.c, record.NewFakeRecorder(64), slog.Default(), h.key, 50*time.Millisecond)
	restarted.SetConnector(h.conn.connect)
	restarted.Tick(context.Background())
	if got := h.promoted(); got != "" {
		t.Fatalf("promoted %q on the restarted manager's first miss, want none", got)
	}
	for i := 0; i < defaultDragonflyMasterLossThreshold-1; i++ {
		restarted.Tick(context.Background())
	}
	if got := h.promoted(); got != "dc2" {
		t.Fatalf("promoted %q after the restarted manager's own threshold, want dc2", got)
	}
}

// The hold over an empty master (the recorded site unobserved, the only
// master empty) outlasts the threshold: no replica is promoted inside the
// grace however many observations missed the recorded site.
func TestDragonflyManager_Tick_ThresholdDoesNotEndTheHoldOverEmptyMaster(t *testing.T) {
	fg := fgAfterDragonflyTakeover()
	cb, key := newDragonflyFakeClient(fg)
	c := cb.WithObjects(makeDragonflyPod(fg.Name, "dc1", "master", true), makeDragonflyPod(fg.Name, "dc3", "replica", true)).Build()
	mgr := NewDragonflyManager(c, record.NewFakeRecorder(64), slog.Default(), key, 50*time.Millisecond)
	conn := newFakeConnector()
	conn.dialFails(holdTestDC1, errors.New("i/o timeout"))
	conn.program(holdTestDC2, &fakeDragonflyConn{info: dragonfly.ReplicationInfo{Role: "master"}})
	// Even a corroborating replica is not promoted during the hold.
	dc3 := &fakeDragonflyConn{info: dragonflyReplicaOf("lion-dragonfly-dc1.shared-lion.svc.cluster.local"), keys: 9}
	dc3.info.MasterLinkStatus = "down"
	conn.program(holdTestDC3, dc3)
	mgr.SetConnector(conn.connect)

	for i := 0; i < defaultDragonflyMasterLossThreshold+2; i++ {
		mgr.Tick(context.Background())
	}
	st := dragonflyStatusOf(t, c, key)
	if st.LastPromotionTarget != "" || st.ActiveSite != "dc1" {
		t.Fatalf("activeSite = %q lastPromotionTarget = %q inside the grace, want dc1 and no promotion", st.ActiveSite, st.LastPromotionTarget)
	}
}

// Dragonfly follows a MySQL failover on MySQL's decision, not on its own
// count: neither the drift promotion nor the queued emergency promotion
// waits for the threshold.
func TestDragonflyManager_MySQLFollowDoesNotWaitForTheThreshold(t *testing.T) {
	t.Run("active-site drift", func(t *testing.T) {
		// MySQL moved to dc2; the Dragonfly master is still dc1 and answers.
		h := newThresholdHarness(t, func(fg *v1alpha1.MysqlFailoverGroup) { fg.Status.ActiveSite = "dc2" })
		h.tick(1)
		if got := h.promoted(); got != "dc2" {
			t.Fatalf("promoted %q on the first cycle, want dc2 to follow MySQL", got)
		}
	})
	t.Run("emergency promotion with the old master gone", func(t *testing.T) {
		h := newThresholdHarness(t, nil)
		h.missMaster()
		if !h.mgr.TryEmergencyPromote(context.Background(), "dc2", "dc1") {
			t.Fatal("TryEmergencyPromote = false, want the promotion made without any missed observation counted")
		}
		if got := h.promoted(); got != "dc2" {
			t.Fatalf("promoted %q, want dc2", got)
		}
	})
}

// The count belongs to the site that was missed: misses counted against
// another site do not promote a replica of this one.
func TestDragonflyOnlyPromotionCandidate_MissesOfAnotherSiteDoNotCount(t *testing.T) {
	h := newThresholdHarness(t, nil)
	fg := fgWithDragonflyEnabledAndActive()
	fg.Status.Dragonfly = &v1alpha1.DragonflyStatus{ActiveSite: "dc1"}
	seven := int64(7)
	snaps := []DragonflySiteSnapshot{
		{Name: "dc1"},
		{Name: "dc2", Reachable: true, Info: dragonflyReplicaOf("lion-dragonfly-dc1.shared-lion.svc.cluster.local"), Keys: &seven},
	}
	h.mgr.mu.Lock()
	h.mgr.masterMissSite, h.mgr.masterMisses = "dc9", defaultDragonflyMasterLossThreshold+1
	h.mgr.mu.Unlock()
	if target, _, ok := h.mgr.dragonflyOnlyPromotionCandidate(fg, snaps, false); ok {
		t.Fatalf("candidate = %q on misses counted against another site, want none", target)
	}
	h.mgr.mu.Lock()
	h.mgr.masterMissSite = "dc1"
	h.mgr.mu.Unlock()
	if target, source, ok := h.mgr.dragonflyOnlyPromotionCandidate(fg, snaps, false); !ok || target != "dc2" || source != "dc1" {
		t.Fatalf("candidate = %q from %q ok=%v, want dc2 from dc1", target, source, ok)
	}
}

// While a MySQL-follow promotion is queued, a Dragonfly-only promotion
// over a missed master promotes only the request's target, and none while
// that target was not observed. Before, the most advanced replica the cycle
// observed was promoted: in scenario 76 the MySQL site's Dragonfly missed
// one observation and the reader was promoted, a second master beside the
// one the queued request then made, whose writes were discarded when the
// reader was attached to it. Without a queued request, MySQL's active site
// does not restrict the promotion: while that site's Dragonfly is down
// (scenario 26 settles that way) the cache still fails over.
func TestDragonflyOnlyPromotionCandidate_FollowsMySQLOnceItMoved(t *testing.T) {
	seven, nine := int64(7), int64(9)
	replica := func(name string, keys *int64, offset int64) DragonflySiteSnapshot {
		info := dragonflyReplicaOf("lion-dragonfly-dc1.shared-lion.svc.cluster.local")
		info.MasterReplID, info.SlaveReplOffset, info.SlaveReplOffsetReported = "r1", offset, true
		return DragonflySiteSnapshot{Name: name, Reachable: true, Info: info, Keys: keys}
	}
	missed := func(h *thresholdHarness) {
		h.mgr.mu.Lock()
		h.mgr.masterMissSite, h.mgr.masterMisses = "dc1", defaultDragonflyMasterLossThreshold+1
		h.mgr.mu.Unlock()
	}
	for _, tc := range []struct {
		name     string
		mysql    string // status.activeSite
		queued   string // a queued MySQL-follow promotion
		dc2Seen  bool
		want     string
		wantNone bool
	}{
		{name: "follow to dc2 queued, dc2 not observed: wait", mysql: "dc2", queued: "dc2", wantNone: true},
		{name: "follow to dc2 queued: dc2 although dc3 is ahead", mysql: "dc2", queued: "dc2", dc2Seen: true, want: "dc2"},
		{name: "follow to dc2 queued before status says so", mysql: "dc1", queued: "dc2", wantNone: true},
		{name: "MySQL still on dc1: the most advanced replica", mysql: "dc1", dc2Seen: true, want: "dc3"},
		{name: "MySQL on dc2, no request, dc2's Dragonfly down: the most advanced replica", mysql: "dc2", want: "dc3"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			h := newThresholdHarness(t, nil)
			fg := fgWithDragonflyEnabledAndActive()
			fg.Status.ActiveSite = tc.mysql
			fg.Status.Dragonfly = &v1alpha1.DragonflyStatus{ActiveSite: "dc1"}
			snaps := []DragonflySiteSnapshot{{Name: "dc1"}, {Name: "dc2"}, replica("dc3", &nine, 20)}
			if tc.dc2Seen {
				snaps[1] = replica("dc2", &seven, 10)
			}
			if tc.queued != "" {
				h.mgr.pendingMu.Lock()
				h.mgr.pending = &dragonflyPromotionRequest{target: tc.queued, oldSource: "dc1"}
				h.mgr.pendingMu.Unlock()
			}
			missed(h)
			target, _, ok := h.mgr.dragonflyOnlyPromotionCandidate(fg, snaps, false)
			if tc.wantNone {
				if ok {
					t.Fatalf("candidate = %q, want none", target)
				}
				return
			}
			if !ok || target != tc.want {
				t.Fatalf("candidate = %q ok=%v, want %s", target, ok, tc.want)
			}
		})
	}
}
