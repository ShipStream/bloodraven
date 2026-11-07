package controller

import (
	"context"
	"encoding/json"
	"errors"
	"slices"
	"sync"
	"testing"
	"time"

	v1alpha1 "github.com/shipstream/bloodraven/api/v1alpha1"
	"github.com/shipstream/bloodraven/internal/mysql"
	"github.com/shipstream/bloodraven/internal/state"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
)

// TestSiteReplicationState pins A-43's producer mapping: Healthy and
// Lagging are exactly the observations the boolean replicating reported
// true for (a read with a debounced healthy streak), Lagging above
// maxLagSeconds, Broken for a read showing a stopped thread or no source,
// Unknown otherwise, NotApplicable on the writable primary.
func TestSiteReplicationState(t *testing.T) {
	healthy := &mysql.ReplicaStatus{IORunning: true, SQLRunning: true, SourceHost: "dc1"}
	lag := func(rs mysql.ReplicaStatus, s int64) *mysql.ReplicaStatus { rs.SecondsBehindSource = &s; return &rs }
	for _, tc := range []struct {
		name    string
		st      state.SiteState
		repl    *mysql.ReplicaStatus
		read    bool
		streak  bool
		want    v1alpha1.SiteReplicationState
		running bool
	}{
		{"primary", state.StateWritable, nil, false, false, v1alpha1.SiteReplicationNotApplicable, false},
		{"primary with a stale streak", state.StateWritable, healthy, true, true, v1alpha1.SiteReplicationNotApplicable, false},
		{"not probed yet", state.StateUnknown, nil, false, false, v1alpha1.SiteReplicationUnknown, false},
		{"unreachable", state.StateUnreachable, nil, false, true, v1alpha1.SiteReplicationUnknown, false},
		{"read failed", state.StateReadOnly, nil, false, true, v1alpha1.SiteReplicationUnknown, false},
		{"streak building", state.StateReadOnly, healthy, true, false, v1alpha1.SiteReplicationUnknown, false},
		{"healthy, lag unknown", state.StateReadOnly, healthy, true, true, v1alpha1.SiteReplicationHealthy, true},
		{"healthy at the threshold", state.StateReadOnly, lag(*healthy, 60), true, true, v1alpha1.SiteReplicationHealthy, true},
		{"lagging above the threshold", state.StateReadOnly, lag(*healthy, 61), true, true, v1alpha1.SiteReplicationLagging, true},
		{"no source configured", state.StateReadOnly, nil, true, false, v1alpha1.SiteReplicationBroken, false},
		{"IO thread stopped", state.StateReadOnly, &mysql.ReplicaStatus{SQLRunning: true, SourceHost: "dc1"}, true, false, v1alpha1.SiteReplicationBroken, false},
		{"SQL thread stopped", state.StateReadOnly, &mysql.ReplicaStatus{IORunning: true, SourceHost: "dc1"}, true, false, v1alpha1.SiteReplicationBroken, false},
		{"no source host", state.StateReadOnly, &mysql.ReplicaStatus{IORunning: true, SQLRunning: true}, true, false, v1alpha1.SiteReplicationBroken, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got := siteReplicationState(tc.st, tc.repl, tc.read, tc.streak, 60)
			if got != tc.want || got.Running() != tc.running {
				t.Fatalf("got %s (running=%t), want %s (running=%t)", got, got.Running(), tc.want, tc.running)
			}
		})
	}
}

// Every SiteReplicationState the producer can return is in the CRD enum,
// and every enum value is produced.
func TestSiteReplicationStateCoversTheEnum(t *testing.T) {
	enum := apiEnumValues(t, "SiteReplicationState")
	seen := map[string]bool{}
	healthy := &mysql.ReplicaStatus{IORunning: true, SQLRunning: true, SourceHost: "dc1"}
	lagging := &mysql.ReplicaStatus{IORunning: true, SQLRunning: true, SourceHost: "dc1", SecondsBehindSource: new(int64(99))}
	for _, st := range []state.SiteState{state.StateUnknown, state.StateWritable, state.StateReadOnly, state.StateUnreachable} {
		for _, repl := range []*mysql.ReplicaStatus{nil, healthy, lagging, {}} {
			for _, read := range []bool{false, true} {
				for _, streak := range []bool{false, true} {
					v := siteReplicationState(st, repl, read, streak, 10)
					if !slices.Contains(enum, string(v)) {
						t.Fatalf("%v/%+v/%t/%t -> %q, not in the CRD enum %v", st, repl, read, streak, v, enum)
					}
					seen[string(v)] = true
				}
			}
		}
	}
	for _, v := range enum {
		if !seen[v] {
			t.Errorf("enum value %s is never produced", v)
		}
	}
}

// TestPoll_PublishesReplicationState drives the topology manager through
// the producer mapping: Unknown while the healthy streak builds, Healthy,
// Lagging above maxLagSeconds, Broken with a stopped thread or no source,
// Unknown when replica status cannot be read. Each change is published
// (the status callback fires) even when the debounced health did not flip,
// and the status write carries the value.
func TestPoll_PublishesReplicationState(t *testing.T) {
	site0 := &mockMySQL{readOnly: false}
	site1 := &mockMySQL{readOnly: true, replicaStatusVal: &mysql.ReplicaStatus{IORunning: true, SQLRunning: true, SourceHost: "mysql-dc1"}}
	tm, _, _ := newTestTopologyManager(site0, site1)
	tm.cfg.MaxLagSeconds = 60
	var mu sync.Mutex
	var snaps []TopologySnapshot
	tm.StatusCallback = func(s TopologySnapshot) {
		mu.Lock()
		defer mu.Unlock()
		snaps = append(snaps, s)
	}
	published := func() (v1alpha1.SiteReplicationState, v1alpha1.SiteReplicationState, int) {
		t.Helper()
		mu.Lock()
		defer mu.Unlock()
		if len(snaps) == 0 {
			t.Fatal("no status published")
		}
		var st v1alpha1.MysqlFailoverGroupStatus
		snap := snaps[len(snaps)-1]
		if !MergeTopologySnapshotStatus(&st, snap) {
			t.Fatal("snapshot not merged")
		}
		return st.Sites[0].Replication, st.Sites[1].Replication, len(snaps)
	}
	// A thread the operator restarts within the poll is reported as the
	// re-read shows it (Unknown while the streak rebuilds); START REPLICA
	// is refused here so a stopped thread stays observed as Broken.
	set := func(rs *mysql.ReplicaStatus, err error) {
		site1.mu.Lock()
		site1.replicaStatusVal, site1.replicaStatusErr = rs, err
		site1.startReplicaErr = errors.New("START REPLICA refused")
		site1.mu.Unlock()
	}
	tracked := func() v1alpha1.SiteReplicationState {
		tm.mu.RLock()
		defer tm.mu.RUnlock()
		return tm.sites[1].replicationState
	}

	// Classification takes RecoveryThreshold polls; the first read-only
	// poll has a streak of one.
	for i := 0; i < 6 && tracked() != v1alpha1.SiteReplicationUnknown; i++ {
		pollN(tm, 1)
		if tm.sites[1].state == state.StateReadOnly && tracked() == v1alpha1.SiteReplicationHealthy {
			t.Fatal("Healthy before the healthy streak reached its threshold")
		}
	}
	pollN(tm, 4)
	if p, r, _ := published(); p != v1alpha1.SiteReplicationNotApplicable || r != v1alpha1.SiteReplicationHealthy {
		t.Fatalf("after a healthy streak: primary=%s replica=%s", p, r)
	}

	steps := []struct {
		name string
		rs   *mysql.ReplicaStatus
		err  error
		want v1alpha1.SiteReplicationState
	}{
		{"lag above the threshold", &mysql.ReplicaStatus{IORunning: true, SQLRunning: true, SourceHost: "mysql-dc1", SecondsBehindSource: new(int64(61))}, nil, v1alpha1.SiteReplicationLagging},
		{"caught up again", &mysql.ReplicaStatus{IORunning: true, SQLRunning: true, SourceHost: "mysql-dc1", SecondsBehindSource: new(int64(1))}, nil, v1alpha1.SiteReplicationHealthy},
		{"SQL thread stopped", &mysql.ReplicaStatus{IORunning: true, SourceHost: "mysql-dc1"}, nil, v1alpha1.SiteReplicationBroken},
		{"replica status unreadable", nil, context.DeadlineExceeded, v1alpha1.SiteReplicationUnknown},
		{"no source configured", nil, nil, v1alpha1.SiteReplicationBroken},
		{"healthy again: streak rebuilding", &mysql.ReplicaStatus{IORunning: true, SQLRunning: true, SourceHost: "mysql-dc1"}, nil, v1alpha1.SiteReplicationUnknown},
		{"streak complete", &mysql.ReplicaStatus{IORunning: true, SQLRunning: true, SourceHost: "mysql-dc1"}, nil, v1alpha1.SiteReplicationHealthy},
	}
	for _, s := range steps {
		_, _, before := published()
		set(s.rs, s.err)
		pollN(tm, 1)
		_, got, after := published()
		if got != s.want || tracked() != s.want {
			t.Fatalf("%s: published %s, tracked %s, want %s", s.name, got, tracked(), s.want)
		}
		if after == before {
			t.Fatalf("%s: the change was not published", s.name)
		}
	}
}

// TestWSStatusCarriesCRShapes pins A-114's /ws/status reshapes: the
// failover record as one lastFailover{time, target,
// promotionGtidExecuted} object, sites[].replication carrying the CR value
// (not the per-poll thread flag) and sites[].lastSeenTime.
func TestWSStatusCarriesCRShapes(t *testing.T) {
	site0 := &mockMySQL{readOnly: false}
	site1 := &mockMySQL{readOnly: true, replicaStatusVal: &mysql.ReplicaStatus{IORunning: true, SQLRunning: true, SourceHost: "dc1"}}
	tm, _, _ := newTestTopologyManager(site0, site1)
	pollN(tm, 1)

	decode := func() map[string]any {
		t.Helper()
		tm.mu.RLock()
		msg := tm.topologyMessageLocked([]*mysql.ReplicaStatus{nil, site1.replicaStatusVal}, "")
		tm.mu.RUnlock()
		raw, err := json.Marshal(msg)
		if err != nil {
			t.Fatal(err)
		}
		var out map[string]any
		if err := json.Unmarshal(raw, &out); err != nil {
			t.Fatal(err)
		}
		return out
	}
	site := func(m map[string]any, i int) map[string]any { return m["sites"].([]any)[i].(map[string]any) }

	// Threads run, but the streak has not built: the per-poll flag would
	// have said true, the CR value says Unknown.
	m := decode()
	if got := site(m, 1)["replication"]; got != string(v1alpha1.SiteReplicationUnknown) {
		t.Fatalf("replica replication before the streak = %v, want Unknown", got)
	}
	pollN(tm, 4)
	m = decode()
	for i, want := range []v1alpha1.SiteReplicationState{v1alpha1.SiteReplicationNotApplicable, v1alpha1.SiteReplicationHealthy} {
		s := site(m, i)
		if s["replication"] != string(want) {
			t.Errorf("site %d replication = %v, want %s", i, s["replication"], want)
		}
		if _, ok := s["replicating"]; ok {
			t.Errorf("site %d still carries replicating", i)
		}
		if _, ok := s["lastSeenTime"]; !ok {
			t.Errorf("site %d has no lastSeenTime: %v", i, s)
		}
		if _, ok := s["lastSeen"]; ok {
			t.Errorf("site %d still carries lastSeen", i)
		}
	}
	if _, ok := m["lastFailover"]; ok {
		t.Fatalf("lastFailover present without a failover record: %v", m["lastFailover"])
	}

	at := time.Date(2026, 11, 1, 12, 0, 0, 0, time.UTC)
	tm.mu.Lock()
	tm.lastFailover, tm.lastFailoverTarget, tm.promotionGtidExecuted = at, "dc2", "uuid:1-7"
	tm.mu.Unlock()
	m = decode()
	rec, ok := m["lastFailover"].(map[string]any)
	if !ok || rec["time"] != at.Format(time.RFC3339) || rec["target"] != "dc2" || rec["promotionGtidExecuted"] != "uuid:1-7" {
		t.Fatalf("lastFailover = %v", m["lastFailover"])
	}
	for _, flat := range []string{"lastFailoverTarget", "promotionGtidExecuted"} {
		if _, ok := m[flat]; ok {
			t.Errorf("/ws/status still carries the flat %s", flat)
		}
	}
}

// F-24: every reader that accepted replicating=true accepts a Lagging
// site (it keeps its own lag threshold), and refuses Unknown and Broken.

func TestBackupSourceSelection_AcceptsLaggingReplica(t *testing.T) {
	lag := int64(120) // over maxLagSeconds, within maxLagSecondsForSource
	for _, tc := range []struct {
		repl v1alpha1.SiteReplicationState
		want string
	}{
		{v1alpha1.SiteReplicationHealthy, "pdx"},
		{v1alpha1.SiteReplicationLagging, "pdx"},
		{v1alpha1.SiteReplicationUnknown, "iad"},
		{v1alpha1.SiteReplicationBroken, "iad"},
	} {
		status := &v1alpha1.MysqlFailoverGroupStatus{
			ActiveSite: "iad",
			Sites: []v1alpha1.SiteStatus{
				{Name: "iad", State: v1alpha1.SiteStateWritable, Replication: v1alpha1.SiteReplicationNotApplicable},
				{Name: "pdx", State: v1alpha1.SiteStateReadOnly, Replication: tc.repl, SecondsBehindSource: &lag},
			},
		}
		site, _, err := selectSourceSite(backupTestGroup(status), "", 300)
		if err != nil || site != tc.want {
			t.Errorf("replication %s: source %q (%v), want %q", tc.repl, site, err, tc.want)
		}
	}
}

func TestPlannedFailoverTarget_AcceptsLaggingTarget(t *testing.T) {
	for _, tc := range []struct {
		repl   v1alpha1.SiteReplicationState
		accept bool
	}{
		{v1alpha1.SiteReplicationHealthy, true},
		{v1alpha1.SiteReplicationLagging, true},
		{v1alpha1.SiteReplicationUnknown, false},
		{v1alpha1.SiteReplicationBroken, false},
	} {
		fg := plannedFG([]string{"iad", "pdx"}, nil, "iad", nil, nil)
		for i := range fg.Status.Sites {
			if fg.Status.Sites[i].Name == "pdx" {
				fg.Status.Sites[i].Replication = tc.repl
			}
		}
		decision, _, err := validatePlannedFailoverRequest(fg, PlannedFailoverRequest{Site: "pdx"}, time.Now(), false)
		if (decision == PlannedFailoverAccept) != tc.accept {
			t.Errorf("replication %s: decision %v (%v), accept=%t", tc.repl, decision, err, tc.accept)
		}
	}
}

func TestReaderHealthyLabel_AcceptsLaggingReader(t *testing.T) {
	lag0 := int64(0)
	for _, tc := range []struct {
		repl v1alpha1.SiteReplicationState
		yes  bool
	}{
		{v1alpha1.SiteReplicationHealthy, true},
		{v1alpha1.SiteReplicationLagging, true},
		{v1alpha1.SiteReplicationUnknown, false},
		{v1alpha1.SiteReplicationBroken, false},
	} {
		t.Run(string(tc.repl), func(t *testing.T) {
			fg := newTestFG()
			fg.Spec.Sites = append(fg.Spec.Sites, v1alpha1.SiteSpec{Name: "reader", ServerID: 103, Role: v1alpha1.SiteRoleReadOnly})
			fg.Status.ActiveSite = "dc1"
			fg.Status.Sites = []v1alpha1.SiteStatus{
				{Name: "dc1", State: v1alpha1.SiteStateWritable, Replication: v1alpha1.SiteReplicationNotApplicable},
				{Name: "dc2", State: v1alpha1.SiteStateReadOnly, Replication: v1alpha1.SiteReplicationHealthy},
				{Name: "reader", State: v1alpha1.SiteStateReadOnly, Replication: tc.repl, SourceHost: "mysql-lion-dc1-internal.shared-lion.svc.cluster.local",
					SourceConvergenceState: v1alpha1.SourceConvergenceConverged, SecondsBehindSource: &lag0},
			}
			readerPod := &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: "reader-pod", Namespace: fg.Namespace, Labels: map[string]string{
				labelAppName: "mysql", labelInstance: fg.Name, v1alpha1.SiteLabel: "reader", v1alpha1.RoleLabel: "replica",
			}}}
			r, c := newReconciler(fg, readerPod)
			if err := r.syncPodLabels(context.Background(), fg); err != nil {
				t.Fatal(err)
			}
			var got corev1.Pod
			if err := c.Get(context.Background(), types.NamespacedName{Name: readerPod.Name, Namespace: fg.Namespace}, &got); err != nil {
				t.Fatal(err)
			}
			if (got.Labels[v1alpha1.HealthyLabel] == "yes") != tc.yes {
				t.Fatalf("healthy label %q, want yes=%t", got.Labels[v1alpha1.HealthyLabel], tc.yes)
			}
		})
	}
}

// The deployment-lease gate refuses a non-active site unless its
// replication is Healthy or Lagging, as it refused replicating=false.
func TestDeploymentUnstableReason_ReplicationEnum(t *testing.T) {
	for _, tc := range []struct {
		repl v1alpha1.SiteReplicationState
		want string
	}{
		{v1alpha1.SiteReplicationHealthy, ""},
		{v1alpha1.SiteReplicationLagging, ""},
		{v1alpha1.SiteReplicationUnknown, v1alpha1.UnstableReasonReplicationNotRunning},
		{v1alpha1.SiteReplicationBroken, v1alpha1.UnstableReasonReplicationNotRunning},
		{v1alpha1.SiteReplicationNotApplicable, v1alpha1.UnstableReasonReplicationNotRunning},
		{"", v1alpha1.UnstableReasonReplicationNotRunning},
	} {
		_, fg, _ := deploymentLeaseFixture(t)
		if got := DeploymentUnstableReason(fg); got != "" {
			t.Fatalf("fixture unstable: %s", got)
		}
		for i := range fg.Status.Sites {
			if fg.Status.Sites[i].Name != fg.Status.ActiveSite {
				fg.Status.Sites[i].Replication = tc.repl
			}
		}
		if got := DeploymentUnstableReason(fg); got != tc.want {
			t.Errorf("replication %q: unstable reason %q, want %q", tc.repl, got, tc.want)
		}
	}
}
