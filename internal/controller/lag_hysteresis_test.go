package controller

import (
	"testing"

	v1alpha1 "github.com/shipstream/bloodraven/api/v1alpha1"
	"github.com/shipstream/bloodraven/internal/mysql"
	"github.com/shipstream/bloodraven/internal/state"
)

// K018: lag hovering around maxLagSeconds publishes Lagging once and
// stays there until it falls a band below; it does not flip every poll.
// Broken and Unknown are never held back.
func TestLagHysteresis(t *testing.T) {
	const maxLag = 30
	poll := func(prev v1alpha1.SiteReplicationState, lag *int64, healthyThreads bool) v1alpha1.SiteReplicationState {
		repl := &mysql.ReplicaStatus{IORunning: healthyThreads, SQLRunning: healthyThreads, SecondsBehindSource: lag}
		st := siteReplicationState(state.StateReadOnly, repl, true, healthyThreads, maxLag)
		return lagHysteresis(prev, st, repl, maxLag)
	}
	cur := v1alpha1.SiteReplicationHealthy
	changes := 0
	for _, lag := range []int64{29, 31, 29, 31, 30, 28, 25, 31, 24, 23} {
		next := poll(cur, new(lag), true)
		if next != cur {
			changes++
		}
		cur = next
	}
	// Healthy -> Lagging at 31, back to Healthy at 24 (band 6), Lagging
	// never re-entered in between.
	if changes != 2 || cur != v1alpha1.SiteReplicationHealthy {
		t.Fatalf("%d publications, ending %s; want 2 ending Healthy", changes, cur)
	}
	if got := poll(v1alpha1.SiteReplicationLagging, new(int64(29)), false); got != v1alpha1.SiteReplicationBroken {
		t.Fatalf("broken threads while Lagging = %s, want Broken at once", got)
	}
	if got := poll(v1alpha1.SiteReplicationLagging, nil, true); got != v1alpha1.SiteReplicationHealthy {
		t.Fatalf("unknown lag while Lagging = %s, want Healthy", got)
	}
	if lagHysteresisBand(0) != 0 || lagHysteresisBand(1) != 1 || lagHysteresisBand(100) != 20 {
		t.Fatal("band is a fifth of the threshold, at least a second, at most the threshold")
	}
	// maxLagSeconds 0: a caught-up replica reads Healthy again at once.
	repl := &mysql.ReplicaStatus{IORunning: true, SQLRunning: true, SecondsBehindSource: new(int64(0))}
	if got := lagHysteresis(v1alpha1.SiteReplicationLagging, siteReplicationState(state.StateReadOnly, repl, true, true, 0), repl, 0); got != v1alpha1.SiteReplicationHealthy {
		t.Fatalf("zero lag with maxLagSeconds 0 = %s, want Healthy", got)
	}
}

// K018 (verification V005): the hysteresis the manager applies must reach
// status.sites[].replication. A status write in the band (lag back under
// the threshold but not yet a band below it) reports what the manager
// published, Lagging, rather than recomputing Healthy from raw lag, and
// the CR follows the manager back to Healthy once lag clears the band.
func TestLagHysteresis_ReachesCRStatus(t *testing.T) {
	site0 := &mockMySQL{readOnly: false}
	site1 := &mockMySQL{readOnly: true, replicaStatusVal: &mysql.ReplicaStatus{IORunning: true, SQLRunning: true, SourceHost: "mysql-dc1", SecondsBehindSource: new(int64(0))}}
	tm, _, _ := newTestTopologyManager(site0, site1)
	tm.cfg.MaxLagSeconds = 30
	var last TopologySnapshot
	tm.StatusCallback = func(s TopologySnapshot) { last = s }
	setLag := func(lag int64) {
		site1.mu.Lock()
		site1.replicaStatusVal = &mysql.ReplicaStatus{IORunning: true, SQLRunning: true, SourceHost: "mysql-dc1", SecondsBehindSource: new(lag)}
		site1.mu.Unlock()
	}
	cr := func() v1alpha1.SiteReplicationState {
		t.Helper()
		// The snapshot carries the manager's threshold, which is the
		// spec's the runner writes against.
		var st v1alpha1.MysqlFailoverGroupStatus
		if !MergeTopologySnapshotStatus(&st, last) {
			t.Fatal("snapshot not merged")
		}
		return st.Sites[1].Replication
	}
	ws := func() string {
		tm.mu.RLock()
		defer tm.mu.RUnlock()
		return tm.topologyMessageLocked(nil, "").Sites[1].Replication
	}
	pollN(tm, 8)
	if got := cr(); got != v1alpha1.SiteReplicationHealthy {
		t.Fatalf("setup: CR replication %s, want Healthy", got)
	}
	for _, step := range []struct {
		lag  int64
		want v1alpha1.SiteReplicationState
	}{
		{31, v1alpha1.SiteReplicationLagging},
		{29, v1alpha1.SiteReplicationLagging}, // inside the band (24..30]
		{25, v1alpha1.SiteReplicationLagging},
		{24, v1alpha1.SiteReplicationHealthy},
	} {
		setLag(step.lag)
		pollN(tm, 1)
		// An out-of-band status write (an update completion, a status
		// retry) carries the same value as the poll's.
		tm.emitStatusSnapshot()
		if got, w := cr(), ws(); got != step.want || w != string(step.want) {
			t.Fatalf("lag %d: CR %s, WebSocket %s, want %s on both", step.lag, got, w, step.want)
		}
	}
}
