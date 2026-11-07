package controller

import (
	"context"
	"errors"
	"strings"
	"testing"

	v1alpha1 "github.com/shipstream/bloodraven/api/v1alpha1"
	"github.com/shipstream/bloodraven/internal/mysql"
	"github.com/shipstream/bloodraven/internal/platform"
	"github.com/shipstream/bloodraven/internal/state"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/tools/record"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
)

// Issue #216: a new operator's site trackers start Unknown, and a writable
// answer needs RecoveryThreshold polls to be confirmed while a read-only one
// is immediate. The first poll after a start must not read as an outage.

func newThreeSiteTestTopologyManager(site0, site1, site2 *mockMySQL) (*TopologyManager, *mockDNS) {
	cfg := testTopologyConfig()
	cfg.Sites = append(cfg.Sites, SiteTopologyConfig{
		Name: "dc3", Zone: "lion-dc3", LoadBalancerIP: "3.3.3.3", Role: state.SiteRolePrimaryCandidate,
		TaintSelector: taintSelector("dc3"), Host: "mysql-dc3",
	})
	dns := &mockDNS{}
	tm := NewTopologyManager(cfg, []mysql.Checker{site0, site1, site2}, NewFailoverController(testLogger()), nil, nil,
		BootstrapConfig{}, newMockTainter(), platform.NewHub(testLogger()), dns, testLogger())
	tm.failoverCooldown = 0
	return tm, dns
}

func isReadOnly(m *mockMySQL) bool {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.readOnly
}

func TestPoll_FirstPollWritablePrimaryIsInitializing(t *testing.T) {
	primary := &mockMySQL{readOnly: false}
	standby := &mockMySQL{readOnly: true}
	tm, _, dns := newTestTopologyManager(primary, standby)
	var snap TopologySnapshot
	tm.StatusCallback = func(s TopologySnapshot) { snap = s }

	tm.Poll(context.Background())
	if snap.DegradedReason != "Initializing" || snap.Alert != "" {
		t.Fatalf("first poll reason=%q alert=%q, want Initializing with no alert", snap.DegradedReason, snap.Alert)
	}
	if s := snap.Sites[0]; s.State != state.StateUnknown || !s.PendingWritable {
		t.Fatalf("dc1 snapshot = %+v, want Unknown and PendingWritable", s)
	}
	// The debounce still gates every mutation: no authority, DNS or promotion.
	if snap.ActiveSite != "" || dns.getLastIP() != "" || !isReadOnly(standby) {
		t.Fatalf("unconfirmed writable answer was acted on: active=%q dns=%q standbyReadOnly=%v",
			snap.ActiveSite, dns.getLastIP(), isReadOnly(standby))
	}

	tm.Poll(context.Background())
	if snap.DegradedReason != "Healthy" || snap.ActiveSite != "dc1" || snap.Sites[0].PendingWritable {
		t.Fatalf("second poll reason=%q active=%q pending=%v, want Healthy with dc1 active",
			snap.DegradedReason, snap.ActiveSite, snap.Sites[0].PendingWritable)
	}
}

func TestPoll_FirstPollAllReadOnlyIsStillNoPrimary(t *testing.T) {
	tm, _, _ := newTestTopologyManager(&mockMySQL{readOnly: true}, &mockMySQL{readOnly: true})
	var snap TopologySnapshot
	tm.StatusCallback = func(s TopologySnapshot) { snap = s }

	tm.Poll(context.Background())
	if snap.DegradedReason != "NoPrimary" || !strings.HasPrefix(snap.Alert, "NO PRIMARY") {
		t.Fatalf("all read-only reason=%q alert=%q, want NoPrimary", snap.DegradedReason, snap.Alert)
	}
}

// A primary that answers writable once and then dies is promoted over on the
// same poll as before: FailureThreshold failures after its last answer.
func TestPoll_PrimaryFailureAfterStartFailsOverOnSchedule(t *testing.T) {
	primary := &mockMySQL{readOnly: false}
	standby := &mockMySQL{readOnly: true}
	tm, _, dns := newTestTopologyManager(primary, standby)
	var snap TopologySnapshot
	tm.StatusCallback = func(s TopologySnapshot) { snap = s }

	tm.Poll(context.Background())
	primary.setError(errors.New("connection refused"))
	for poll := 2; poll < 1+tm.cfg.FailureThreshold; poll++ {
		tm.Poll(context.Background())
		if !isReadOnly(standby) {
			t.Fatalf("standby promoted at poll %d, before the primary reached FailureThreshold", poll)
		}
		if snap.DegradedReason == "NoPrimary" {
			t.Fatalf("poll %d reported NoPrimary while dc1 was unclassified", poll)
		}
	}
	tm.Poll(context.Background())
	if isReadOnly(standby) || dns.getLastIP() != "2.2.2.2" {
		t.Fatalf("standby not promoted once the primary was confirmed unreachable: readOnly=%v dns=%q",
			isReadOnly(standby), dns.getLastIP())
	}
}

// While a site's writable answer awaits confirmation, a peer reaching
// unreachable must not get a third site promoted: that would make two
// writable primaries. The decision is re-made when the pending site is
// classified, which is always a state transition.
func TestPoll_PendingWritableBlocksPromotionOverIt(t *testing.T) {
	for _, primaryDies := range []bool{false, true} {
		name := "primary confirmed"
		if primaryDies {
			name = "primary dies before confirmation"
		}
		t.Run(name, func(t *testing.T) {
			primary := &mockMySQL{readOnly: false}
			down := &mockMySQL{readOnly: true, err: errors.New("connection refused")}
			standby := &mockMySQL{readOnly: true}
			tm, dns := newThreeSiteTestTopologyManager(primary, down, standby)
			tm.cfg.RecoveryThreshold = tm.cfg.FailureThreshold + 2
			var snap TopologySnapshot
			tm.StatusCallback = func(s TopologySnapshot) { snap = s }

			pollN(tm, tm.cfg.FailureThreshold)
			if tm.sites[1].state != state.StateUnreachable || tm.sites[0].state != state.StateUnknown {
				t.Fatalf("setup: states %s/%s, want dc1 unknown and dc2 unreachable", tm.sites[0].state, tm.sites[1].state)
			}
			if !isReadOnly(standby) || dns.getLastIP() != "" {
				t.Fatal("dc3 was promoted while dc1 had answered writable")
			}
			if snap.DegradedReason != "Initializing" {
				t.Fatalf("reason = %q, want Initializing", snap.DegradedReason)
			}

			if !primaryDies {
				pollN(tm, 2)
				if tm.sites[0].state != state.StateWritable || snap.DegradedReason != v1alpha1.ReasonSiteUnhealthy {
					t.Fatalf("dc1=%s reason=%q, want writable dc1 and SiteUnhealthy for unreachable dc2", tm.sites[0].state, snap.DegradedReason)
				}
				if !isReadOnly(standby) {
					t.Fatal("dc3 promoted alongside the confirmed primary")
				}
				return
			}
			primary.setError(errors.New("connection refused"))
			pollN(tm, tm.cfg.FailureThreshold)
			if tm.sites[0].state != state.StateUnreachable {
				t.Fatalf("dc1 = %s, want unreachable", tm.sites[0].state)
			}
			if isReadOnly(standby) || dns.getLastIP() != "3.3.3.3" {
				t.Fatalf("dc3 not promoted once dc1 was confirmed unreachable: readOnly=%v dns=%q", isReadOnly(standby), dns.getLastIP())
			}
		})
	}
}

// A second writable answer is reported as split brain before the debounce
// confirms it, but nothing is fenced until it does.
func TestPoll_UnconfirmedSecondWritableIsSplitBrainWithoutFencing(t *testing.T) {
	dc1 := &mockMySQL{readOnly: false}
	dc2 := &mockMySQL{readOnly: false, err: errors.New("connection refused")}
	tm, _, _ := newTestTopologyManager(dc1, dc2)
	tm.cfg.SitePriorities = []string{"dc1"}
	var snap TopologySnapshot
	tm.StatusCallback = func(s TopologySnapshot) { snap = s }

	tm.Poll(context.Background())
	dc2.setReadOnly(false)
	tm.Poll(context.Background())
	if tm.sites[0].state != state.StateWritable || !tm.sites[1].pendingWritable() {
		t.Fatalf("setup: dc1=%s dc2 pending=%v", tm.sites[0].state, tm.sites[1].pendingWritable())
	}
	if snap.DegradedReason != "SplitBrain" || !strings.Contains(snap.Alert, "dc2 not yet confirmed") {
		t.Fatalf("reason=%q alert=%q, want an unconfirmed SplitBrain naming dc2", snap.DegradedReason, snap.Alert)
	}
	if dc1.superReadOnlyCalls != 0 || dc2.superReadOnlyCalls != 0 {
		t.Fatalf("fenced before confirmation: dc1=%d dc2=%d", dc1.superReadOnlyCalls, dc2.superReadOnlyCalls)
	}

	tm.Poll(context.Background())
	if tm.sites[1].state != state.StateWritable || dc2.superReadOnlyCalls == 0 {
		t.Fatalf("confirmed split brain not resolved by sitePriorities: dc2=%s fences=%d", tm.sites[1].state, dc2.superReadOnlyCalls)
	}
}

func TestUpdateCRStatus_InitializingIsUnknownNotNoPrimary(t *testing.T) {
	fg := newTestFG()
	c := fake.NewClientBuilder().WithScheme(testScheme()).
		WithStatusSubresource(&v1alpha1.MysqlFailoverGroup{}).
		WithObjects(fg).Build()
	runner := &TopologyManagerRunner{client: c, logger: testLogger(), managers: make(map[types.NamespacedName]*managedTopology)}
	nn := types.NamespacedName{Name: fg.Name, Namespace: fg.Namespace}
	if err := runner.updateCRStatus(context.Background(), nn, TopologySnapshot{
		Sites: []SiteSnapshot{
			{Name: "dc1", Role: state.SiteRolePrimaryCandidate, State: state.StateUnknown, PendingWritable: true},
			{Name: "dc2", Role: state.SiteRolePrimaryCandidate, State: state.StateReadOnly},
		},
		DegradedReason: "Initializing",
	}); err != nil {
		t.Fatal(err)
	}
	var updated v1alpha1.MysqlFailoverGroup
	if err := c.Get(context.Background(), nn, &updated); err != nil {
		t.Fatal(err)
	}
	for _, typ := range []string{"Ready", "Degraded"} {
		cond := findCondition(updated.Status.Conditions, typ)
		if cond == nil || cond.Status != metav1.ConditionUnknown || cond.Reason != "Initializing" ||
			!strings.Contains(cond.Message, "dc1 answered writable") {
			t.Fatalf("%s condition = %+v, want Unknown/Initializing naming dc1", typ, cond)
		}
	}
	if got := DeploymentUnstableReason(&updated); got != "Initializing" {
		t.Fatalf("DeploymentUnstableReason = %q, want Initializing", got)
	}

	// A recorded divergence is not hidden behind Initializing.
	if err := runner.updateCRStatus(context.Background(), nn, TopologySnapshot{
		Sites: []SiteSnapshot{
			{Name: "dc1", Role: state.SiteRolePrimaryCandidate, State: state.StateUnknown, PendingWritable: true},
			{Name: "dc2", Role: state.SiteRolePrimaryCandidate, State: state.StateReadOnly,
				RecoveryState: v1alpha1.RecoveryStateBlocked, DivergentGtid: convergenceTestGTID, DivergentTxnCount: 10,
				DivergenceDetectedBy: divergenceDetectedByAudit, DivergenceComparedTo: "dc1"},
		},
		DegradedReason: "Initializing",
	}); err != nil {
		t.Fatal(err)
	}
	if err := c.Get(context.Background(), nn, &updated); err != nil {
		t.Fatal(err)
	}
	if cond := findCondition(updated.Status.Conditions, "Degraded"); cond == nil || cond.Status != metav1.ConditionTrue || cond.Reason != "DivergentTransactions" {
		t.Fatalf("Degraded = %+v, want True/DivergentTransactions", cond)
	}
}

func TestEmitDegradedTransitionEvents_InitializingIsSilent(t *testing.T) {
	rec := record.NewFakeRecorder(10)
	fg := newTestFG()
	runner, nn := newDegradedTestRunner(rec, fg, "")

	runner.emitDegradedTransitionEvents(fg, nn, TopologySnapshot{DegradedReason: "Initializing"})
	runner.emitDegradedTransitionEvents(fg, nn, TopologySnapshot{DegradedReason: "Healthy"})
	if events := drainRunnerEvents(rec); len(events) != 0 {
		t.Fatalf("Initializing then Healthy emitted %v, want no events", events)
	}

	runner, nn = newDegradedTestRunner(rec, fg, "")
	runner.emitDegradedTransitionEvents(fg, nn, TopologySnapshot{DegradedReason: "Initializing"})
	runner.emitDegradedTransitionEvents(fg, nn, TopologySnapshot{DegradedReason: "NoPrimary", Alert: "NO PRIMARY: both sites are read-only"})
	if events := drainRunnerEvents(rec); len(events) != 1 || !strings.Contains(events[0], "NoPrimaryDetected") {
		t.Fatalf("Initializing then NoPrimary emitted %v, want one NoPrimaryDetected", events)
	}
}

// End to end through the runner: a restart against a healthy group raises
// no Warning, no NoPrimary condition, and no SiteRecovered; a group that is
// genuinely all read-only still raises NoPrimaryDetected on its first poll.
func TestRunner_FirstPollAfterStartRaisesNoNoPrimary(t *testing.T) {
	for _, allReadOnly := range []bool{false, true} {
		name := "writable primary"
		if allReadOnly {
			name = "all read-only"
		}
		t.Run(name, func(t *testing.T) {
			fg := newTestFG()
			c := fake.NewClientBuilder().WithScheme(testScheme()).
				WithStatusSubresource(&v1alpha1.MysqlFailoverGroup{}).
				WithObjects(fg).Build()
			rec := record.NewFakeRecorder(20)
			runner := &TopologyManagerRunner{client: c, logger: testLogger(), recorder: rec, managers: make(map[types.NamespacedName]*managedTopology)}
			nn := types.NamespacedName{Name: fg.Name, Namespace: fg.Namespace}
			standby := &mockMySQL{readOnly: true, replicaStatusVal: &mysql.ReplicaStatus{IORunning: true, SQLRunning: true, SourceHost: "mysql-dc1"}}
			tm, _, _ := newTestTopologyManager(&mockMySQL{readOnly: allReadOnly}, standby)
			runner.managers[nn] = &managedTopology{tm: tm}
			tm.StatusCallback = func(s TopologySnapshot) {
				if err := runner.updateCRStatus(context.Background(), nn, s); err != nil {
					t.Errorf("updateCRStatus: %v", err)
				}
			}

			tm.Poll(context.Background())
			var updated v1alpha1.MysqlFailoverGroup
			if err := c.Get(context.Background(), nn, &updated); err != nil {
				t.Fatal(err)
			}
			degraded := findCondition(updated.Status.Conditions, "Degraded")
			events := drainRunnerEvents(rec)
			if allReadOnly {
				if degraded == nil || degraded.Reason != "NoPrimary" || len(events) != 1 || !strings.Contains(events[0], "NoPrimaryDetected") {
					t.Fatalf("all read-only: Degraded=%+v events=%v, want NoPrimary and NoPrimaryDetected", degraded, events)
				}
				return
			}
			if degraded == nil || degraded.Reason != "Initializing" || len(events) != 0 {
				t.Fatalf("first poll: Degraded=%+v events=%v, want Initializing and no events", degraded, events)
			}

			tm.Poll(context.Background())
			if err := c.Get(context.Background(), nn, &updated); err != nil {
				t.Fatal(err)
			}
			if updated.Status.ActiveSite != "dc1" {
				t.Fatalf("activeSite = %q, want dc1 once confirmed", updated.Status.ActiveSite)
			}
			if degraded := findCondition(updated.Status.Conditions, "Degraded"); degraded == nil || degraded.Reason == "Initializing" || degraded.Reason == "NoPrimary" {
				t.Fatalf("second poll Degraded = %+v, want a classified topology", degraded)
			}
			if events := drainRunnerEvents(rec); len(events) != 0 {
				t.Fatalf("second poll emitted %v, want no events", events)
			}
		})
	}
}

// A primary whose probes alternate between writable answers and errors is
// never confirmed and never unreachable. Initializing covers only the first
// failureThreshold+recoveryThreshold probes; after that the group reports
// NoPrimary again, naming the site, and still promotes nothing over it.
func TestPoll_FlappingPrimaryStopsReadingAsInitializing(t *testing.T) {
	primary := &mockMySQL{readOnly: false}
	standby := &mockMySQL{readOnly: true}
	tm, _, dns := newTestTopologyManager(primary, standby)
	var snap TopologySnapshot
	tm.StatusCallback = func(s TopologySnapshot) { snap = s }

	budget := tm.cfg.FailureThreshold + tm.cfg.RecoveryThreshold
	for poll := 1; poll <= budget+2; poll++ {
		if poll%2 == 0 {
			primary.setError(errors.New("connection reset"))
		} else {
			primary.setError(nil)
		}
		tm.Poll(context.Background())
		if s := snap.Sites[0]; s.State != state.StateUnknown {
			t.Fatalf("poll %d: dc1 state = %s, want it to stay unknown while flapping", poll, s.State)
		}
		want := "Initializing"
		if poll >= budget {
			want = "NoPrimary"
		}
		if snap.DegradedReason != want {
			t.Fatalf("poll %d: reason = %q (alert %q), want %q", poll, snap.DegradedReason, snap.Alert, want)
		}
		if want == "NoPrimary" && !strings.Contains(snap.Alert, "dc1 not classified") {
			t.Fatalf("poll %d: alert = %q, want it to name the flapping site", poll, snap.Alert)
		}
	}
	if !isReadOnly(standby) || dns.getLastIP() != "" {
		t.Fatalf("acted on a flapping primary: standbyReadOnly=%v dns=%q", isReadOnly(standby), dns.getLastIP())
	}
}
