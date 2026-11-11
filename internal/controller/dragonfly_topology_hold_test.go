package controller

import (
	"context"
	"errors"
	"log/slog"
	"testing"
	"time"

	"k8s.io/client-go/tools/record"
	"sigs.k8s.io/controller-runtime/pkg/client"

	v1alpha1 "github.com/shipstream/bloodraven/api/v1alpha1"
	"github.com/shipstream/bloodraven/internal/dragonfly"
)

const (
	holdTestDC1 = "lion-dragonfly-dc1.shared-lion.svc.cluster.local:6379"
	holdTestDC2 = "lion-dragonfly-dc2.shared-lion.svc.cluster.local:6379"
	holdTestDC3 = "lion-dragonfly-dc3.shared-lion.svc.cluster.local:6379"
)

// fgAfterDragonflyTakeover is a three-site group right after a takeover
// made dc1 the Dragonfly master: MySQL and Dragonfly both name dc1, and dc3
// is a third site (a reader) that replicates from it.
func fgAfterDragonflyTakeover() *v1alpha1.MysqlFailoverGroup {
	fg := fgWithDragonflyEnabledAndActive()
	dc3 := *fg.Spec.Sites[1].DeepCopy()
	dc3.Name = "dc3"
	dc3.ServerID = 103
	dc3.Zone = "lion-dc3"
	fg.Spec.Sites = append(fg.Spec.Sites, dc3)
	fg.Status.Dragonfly = &v1alpha1.DragonflyStatus{ActiveSite: "dc1"}
	return fg
}

func dragonflyReplicaOf(host string) dragonfly.ReplicationInfo {
	return dragonfly.ReplicationInfo{
		Role:                   "slave",
		MasterHost:             host,
		MasterPort:             6379,
		MasterLinkStatus:       "up",
		MasterLastIOSecondsAgo: 0,
	}
}

func dragonflyStatusOf(t *testing.T, c client.Client, key client.ObjectKey) *v1alpha1.DragonflyStatus {
	t.Helper()
	var got v1alpha1.MysqlFailoverGroup
	if err := c.Get(context.Background(), key, &got); err != nil {
		t.Fatalf("get fg: %v", err)
	}
	if got.Status.Dragonfly == nil {
		t.Fatal("status.dragonfly not set")
	}
	return got.Status.Dragonfly
}

// The sequence of the scenario-25 flap: a takeover made dc1 the master and
// its source (dc2) was recreated, so dc2 is up as an empty standalone
// master that is not reattached yet. One observation of dc1 fails. The
// recorded active site must keep the role: before, dc2 became the active
// site, the reader was pointed at it, and the next cycle pointed dc1 at it
// too.
func TestDragonflyManager_Tick_UnobservedActiveSiteKeepsItsRole(t *testing.T) {
	fg := fgAfterDragonflyTakeover()
	cb, key := newDragonflyFakeClient(fg)
	c := cb.Build()
	mgr := NewDragonflyManager(c, record.NewFakeRecorder(16), slog.Default(), key, 50*time.Millisecond)

	dc1 := &fakeDragonflyConn{info: dragonfly.ReplicationInfo{Role: "master", ConnectedSlaves: 1}}
	dc2 := &fakeDragonflyConn{info: dragonfly.ReplicationInfo{Role: "master"}}
	dc3 := &fakeDragonflyConn{info: dragonflyReplicaOf("lion-dragonfly-dc1.shared-lion.svc.cluster.local")}
	conn := newFakeConnector()
	conn.dialFails(holdTestDC1, errors.New("i/o timeout"))
	conn.program(holdTestDC2, dc2)
	conn.program(holdTestDC3, dc3)
	mgr.SetConnector(conn.connect)

	mgr.Tick(context.Background())

	st := dragonflyStatusOf(t, c, key)
	if st.ActiveSite != "dc1" {
		t.Fatalf("activeSite = %q after one missed observation of dc1, want dc1", st.ActiveSite)
	}
	if st.LastPromotionTarget != "" {
		t.Errorf("lastPromotionTarget = %q, want no promotion: dc3 must not be promoted while dc2 reports master", st.LastPromotionTarget)
	}
	if dc3.replicaOfArgs != nil {
		t.Errorf("reader got REPLICAOF %v, want it left on dc1", dc3.replicaOfArgs)
	}
	// Nothing is reattached to a site that is not observed: if dc1 never
	// came back, dc2 would have to become the master.
	if dc2.replicaOfArgs != nil {
		t.Errorf("dc2 got REPLICAOF %v while dc1 was not observed, want nothing sent", dc2.replicaOfArgs)
	}

	// dc1 answers again: the recreated source is reattached to it.
	conn.mu.Lock()
	delete(conn.dialErrs, holdTestDC1)
	conn.mu.Unlock()
	conn.program(holdTestDC1, dc1)

	mgr.Tick(context.Background())

	st = dragonflyStatusOf(t, c, key)
	if st.ActiveSite != "dc1" {
		t.Fatalf("activeSite = %q, want dc1", st.ActiveSite)
	}
	if dc2.replicaOfArgs == nil || !contains(dc2.replicaOfArgs[0], "lion-dragonfly-dc1.") {
		t.Errorf("dc2 REPLICAOF = %v, want it reattached to dc1", dc2.replicaOfArgs)
	}
	if dc1.replicaOfArgs != nil || dc3.replicaOfArgs != nil {
		t.Errorf("dc1 REPLICAOF = %v, dc3 REPLICAOF = %v, want neither", dc1.replicaOfArgs, dc3.replicaOfArgs)
	}
}

// An active site that stays unobservable for the whole grace no longer
// keeps the role: a single site that reports master becomes the active
// site even when it holds nothing, so the cache has a master again.
func TestDragonflyManager_Tick_UnobservedActiveSiteYieldsAfterGrace(t *testing.T) {
	fg := fgAfterDragonflyTakeover()
	cb, key := newDragonflyFakeClient(fg)
	c := cb.Build()
	mgr := NewDragonflyManager(c, record.NewFakeRecorder(16), slog.Default(), key, 50*time.Millisecond)

	dc2 := &fakeDragonflyConn{info: dragonfly.ReplicationInfo{Role: "master"}}
	dc3 := &fakeDragonflyConn{info: dragonflyReplicaOf("lion-dragonfly-dc2.shared-lion.svc.cluster.local")}
	conn := newFakeConnector()
	conn.dialFails(holdTestDC1, errors.New("connection refused"))
	conn.program(holdTestDC2, dc2)
	conn.program(holdTestDC3, dc3)
	mgr.SetConnector(conn.connect)

	mgr.Tick(context.Background())
	if st := dragonflyStatusOf(t, c, key); st.ActiveSite != "dc1" {
		t.Fatalf("activeSite = %q inside the grace, want dc1", st.ActiveSite)
	}

	mgr.mu.Lock()
	mgr.activeUnobservedSince = time.Now().Add(-2 * mgr.activeHoldGrace)
	mgr.mu.Unlock()
	mgr.Tick(context.Background())

	if st := dragonflyStatusOf(t, c, key); st.ActiveSite != "dc2" {
		t.Fatalf("activeSite = %q after the grace, want the observed master dc2", st.ActiveSite)
	}

	// The hold is per outage: once the recorded site (now dc2) is
	// observed, a later miss starts a new grace.
	mgr.Tick(context.Background())
	mgr.mu.Lock()
	site, logged := mgr.activeUnobservedSite, mgr.activeHoldLogged
	mgr.mu.Unlock()
	if site != "" || logged {
		t.Errorf("hold state = (%q, logged=%v) with the active site observed, want it cleared", site, logged)
	}
}

// A single master that holds keys replaces an unobserved recorded site at
// once, as before: that is a promotion whose record was lost, and the
// promoted site must not wait out the grace outside the active Service.
func TestDragonflyManager_Tick_MasterHoldingKeysReplacesUnobservedActiveAtOnce(t *testing.T) {
	fg := fgAfterDragonflyTakeover()
	cb, key := newDragonflyFakeClient(fg)
	c := cb.Build()
	mgr := NewDragonflyManager(c, record.NewFakeRecorder(16), slog.Default(), key, 50*time.Millisecond)

	dc2 := &fakeDragonflyConn{info: dragonfly.ReplicationInfo{Role: "master"}, keys: 5}
	dc3 := &fakeDragonflyConn{info: dragonflyReplicaOf("lion-dragonfly-dc1.shared-lion.svc.cluster.local")}
	conn := newFakeConnector()
	conn.dialFails(holdTestDC1, errors.New("connection refused"))
	conn.program(holdTestDC2, dc2)
	conn.program(holdTestDC3, dc3)
	mgr.SetConnector(conn.connect)

	mgr.Tick(context.Background())

	if st := dragonflyStatusOf(t, c, key); st.ActiveSite != "dc2" {
		t.Fatalf("activeSite = %q, want dc2: it is the only master and it holds the cache", st.ActiveSite)
	}
	if dc3.replicaOfArgs == nil || !contains(dc3.replicaOfArgs[0], "lion-dragonfly-dc2.") {
		t.Errorf("reader REPLICAOF = %v, want it pointed at dc2", dc3.replicaOfArgs)
	}
}

// A single master whose key count could not be read is not held off
// either: nothing says it is the empty one.
func TestDragonflyManager_Tick_MasterWithUnreadableKeyCountReplacesUnobservedActive(t *testing.T) {
	fg := fgAfterDragonflyTakeover()
	cb, key := newDragonflyFakeClient(fg)
	c := cb.Build()
	mgr := NewDragonflyManager(c, record.NewFakeRecorder(16), slog.Default(), key, 50*time.Millisecond)

	conn := newFakeConnector()
	conn.dialFails(holdTestDC1, errors.New("connection refused"))
	conn.program(holdTestDC2, &fakeDragonflyConn{info: dragonfly.ReplicationInfo{Role: "master"}, keysErr: errors.New("no keyspace section")})
	conn.program(holdTestDC3, &fakeDragonflyConn{info: dragonflyReplicaOf("lion-dragonfly-dc1.shared-lion.svc.cluster.local")})
	mgr.SetConnector(conn.connect)

	mgr.Tick(context.Background())

	if st := dragonflyStatusOf(t, c, key); st.ActiveSite != "dc2" {
		t.Fatalf("activeSite = %q, want dc2 as before the key count was read", st.ActiveSite)
	}
}

// With the active site held over an empty master, the single healthy
// replica is not promoted: that would be a third master after one missed
// observation. With the other master gone too, the replica is promoted as
// before.
func TestDragonflyManager_Tick_NoReplicaPromotedWhileActiveHeldOverEmptyMaster(t *testing.T) {
	for _, tc := range []struct {
		name         string
		emptyMaster  bool
		wantPromoted string
	}{
		{name: "empty master beside the missed active site", emptyMaster: true},
		{name: "no other master", wantPromoted: "dc3"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			fg := fgAfterDragonflyTakeover()
			cb, key := newDragonflyFakeClient(fg)
			c := cb.WithObjects(
				makeDragonflyPod(fg.Name, "dc1", "master", true),
				makeDragonflyPod(fg.Name, "dc3", "replica", true),
			).Build()
			mgr := NewDragonflyManager(c, record.NewFakeRecorder(16), slog.Default(), key, 50*time.Millisecond)

			conn := newFakeConnector()
			conn.dialFails(holdTestDC1, errors.New("i/o timeout"))
			if tc.emptyMaster {
				conn.program(holdTestDC2, &fakeDragonflyConn{info: dragonfly.ReplicationInfo{Role: "master"}})
			} else {
				conn.dialFails(holdTestDC2, errors.New("connection refused"))
			}
			conn.program(holdTestDC3, &fakeDragonflyConn{info: dragonflyReplicaOf("lion-dragonfly-dc1.shared-lion.svc.cluster.local")})
			mgr.SetConnector(conn.connect)

			mgr.Tick(context.Background())

			if st := dragonflyStatusOf(t, c, key); st.LastPromotionTarget != tc.wantPromoted {
				t.Errorf("lastPromotionTarget = %q, want %q", st.LastPromotionTarget, tc.wantPromoted)
			}
		})
	}
}

// The stale-master reattach (REPLICAOF discards what the instance holds).
// dc1 is the recorded active site and dc2 reports master beside it, with
// no replica attached to either. Only one combination is refused: the
// stale master holds keys and the active site holds none.
func TestDragonflyManager_Tick_StaleMasterReattachKeepsTheOnlyCopy(t *testing.T) {
	unknown := errors.New("no keyspace section")
	for _, tc := range []struct {
		name                        string
		activeKeys, staleKeys       int64
		activeKeysErr, staleKeysErr error
		wantReattached              bool
	}{
		{name: "stale holds keys, active holds none", staleKeys: 4},
		// The old master back with its data: its keys are the stale ones.
		{name: "both hold keys", activeKeys: 9, staleKeys: 4, wantReattached: true},
		{name: "stale holds nothing", activeKeys: 9, wantReattached: true},
		{name: "both hold nothing", wantReattached: true},
		// Without a count the check does not apply: as before it was read.
		{name: "stale holds keys, active unknown", activeKeysErr: unknown, staleKeys: 4, wantReattached: true},
		{name: "stale unknown, active holds none", staleKeysErr: unknown, wantReattached: true},
		{name: "both unknown", activeKeysErr: unknown, staleKeysErr: unknown, wantReattached: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			fg := fgWithDragonflyEnabledAndActive()
			fg.Status.Dragonfly = &v1alpha1.DragonflyStatus{ActiveSite: "dc1"}
			cb, key := newDragonflyFakeClient(fg)
			c := cb.Build()
			mgr := NewDragonflyManager(c, record.NewFakeRecorder(16), slog.Default(), key, 50*time.Millisecond)

			dc1 := &fakeDragonflyConn{info: dragonfly.ReplicationInfo{Role: "master"}, keys: tc.activeKeys, keysErr: tc.activeKeysErr}
			dc2 := &fakeDragonflyConn{info: dragonfly.ReplicationInfo{Role: "master"}, keys: tc.staleKeys, keysErr: tc.staleKeysErr}
			conn := newFakeConnector()
			conn.program(holdTestDC1, dc1)
			conn.program(holdTestDC2, dc2)
			mgr.SetConnector(conn.connect)

			mgr.Tick(context.Background())

			if st := dragonflyStatusOf(t, c, key); st.ActiveSite != "dc1" {
				t.Fatalf("activeSite = %q, want dc1", st.ActiveSite)
			}
			if dc1.replicaOfArgs != nil {
				t.Fatalf("active site got REPLICAOF %v", dc1.replicaOfArgs)
			}
			if got := dc2.replicaOfArgs != nil; got != tc.wantReattached {
				t.Errorf("stale master reattached = %v (REPLICAOF %v), want %v", got, dc2.replicaOfArgs, tc.wantReattached)
			}
		})
	}
}

// The second half of the scenario-25 flap, with a cache that holds keys:
// the empty recreated source was made the active site (here by a miss
// that outlasted the grace) and the master answers again. It is a stale
// master now, as before, but it is no longer emptied.
func TestDragonflyManager_Tick_MasterReturningAfterGraceIsNotEmptied(t *testing.T) {
	fg := fgAfterDragonflyTakeover()
	cb, key := newDragonflyFakeClient(fg)
	c := cb.Build()
	mgr := NewDragonflyManager(c, record.NewFakeRecorder(16), slog.Default(), key, 50*time.Millisecond)
	mgr.activeHoldGrace = 0

	dc1 := &fakeDragonflyConn{info: dragonfly.ReplicationInfo{Role: "master"}, keys: 5}
	dc2 := &fakeDragonflyConn{info: dragonfly.ReplicationInfo{Role: "master"}}
	conn := newFakeConnector()
	conn.dialFails(holdTestDC1, errors.New("i/o timeout"))
	conn.program(holdTestDC2, dc2)
	conn.program(holdTestDC3, &fakeDragonflyConn{info: dragonflyReplicaOf("lion-dragonfly-dc1.shared-lion.svc.cluster.local"), keys: 5})
	mgr.SetConnector(conn.connect)

	mgr.Tick(context.Background())
	if st := dragonflyStatusOf(t, c, key); st.ActiveSite != "dc2" {
		t.Fatalf("activeSite = %q with no grace left, want dc2", st.ActiveSite)
	}

	conn.mu.Lock()
	delete(conn.dialErrs, holdTestDC1)
	conn.mu.Unlock()
	conn.program(holdTestDC1, dc1)

	mgr.Tick(context.Background())
	mgr.Tick(context.Background())

	if dc1.replicaOfArgs != nil {
		t.Fatalf("dc1 got REPLICAOF %v: the master holding the only copy of the cache was emptied", dc1.replicaOfArgs)
	}
}

func TestActiveDragonflySiteFromObservation_UnobservedRecordedSite(t *testing.T) {
	keys := func(n int64) *int64 { return &n }
	master := func(name string, k *int64) DragonflySiteSnapshot {
		return DragonflySiteSnapshot{Name: name, Reachable: true, Info: dragonfly.ReplicationInfo{Role: "master"}, Keys: k}
	}
	replica := func(name string) DragonflySiteSnapshot {
		return DragonflySiteSnapshot{Name: name, Reachable: true, Info: dragonfly.ReplicationInfo{Role: "slave"}}
	}
	down := func(name string) DragonflySiteSnapshot { return DragonflySiteSnapshot{Name: name} }

	// status.dragonfly.activeSite is dc1 in every case.
	for _, tc := range []struct {
		name  string
		snaps []DragonflySiteSnapshot
		hold  bool
		want  string
	}{
		{name: "held over an empty master", snaps: []DragonflySiteSnapshot{down("dc1"), master("dc2", keys(0))}, hold: true, want: "dc1"},
		{name: "absent from the snapshot, held", snaps: []DragonflySiteSnapshot{master("dc2", keys(0))}, hold: true, want: "dc1"},
		{name: "grace over", snaps: []DragonflySiteSnapshot{down("dc1"), master("dc2", keys(0))}, want: "dc2"},
		{name: "other master holds keys", snaps: []DragonflySiteSnapshot{down("dc1"), master("dc2", keys(3))}, hold: true, want: "dc2"},
		{name: "other master's key count unknown", snaps: []DragonflySiteSnapshot{down("dc1"), master("dc2", nil)}, hold: true, want: "dc2"},
		// Two other masters: not the held case; the record stands as before.
		{name: "two empty masters", snaps: []DragonflySiteSnapshot{down("dc1"), master("dc2", keys(0)), master("dc3", keys(0))}, hold: true, want: "dc1"},
		// A recorded site that answers is never held.
		{name: "observed as replica", snaps: []DragonflySiteSnapshot{replica("dc1"), master("dc2", keys(0))}, hold: true, want: "dc2"},
		{name: "observed as master beside another", snaps: []DragonflySiteSnapshot{master("dc1", keys(0)), master("dc2", keys(3))}, hold: true, want: "dc1"},
		{name: "no master observed", snaps: []DragonflySiteSnapshot{down("dc1"), replica("dc2")}, hold: true, want: "dc1"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			fg := fgWithDragonflyEnabledAndActive()
			fg.Status.Dragonfly = &v1alpha1.DragonflyStatus{ActiveSite: "dc1"}
			if got := activeDragonflySiteFromObservation(fg, tc.snaps, tc.hold); got != tc.want {
				t.Errorf("active = %q, want %q", got, tc.want)
			}
		})
	}
}
