package controller

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	v1alpha1 "github.com/shipstream/bloodraven/api/v1alpha1"
	"github.com/shipstream/bloodraven/internal/clock"
	"github.com/shipstream/bloodraven/internal/mysql"
	"github.com/shipstream/bloodraven/internal/state"
)

// handoffConditions are the states in which recovery, its connection drain
// and source convergence must leave the sites alone.
var handoffConditions = []struct {
	name  string
	set   func(tm *TopologyManager)
	clear func(tm *TopologyManager)
}{
	{"planned failover", func(tm *TopologyManager) { tm.plannedFailoverActive = true }, func(tm *TopologyManager) { tm.plannedFailoverActive = false }},
	{"ordered update running", func(tm *TopologyManager) { tm.deploymentUpdateRunning = true }, func(tm *TopologyManager) { tm.deploymentUpdateRunning = false }},
	{"topology change", func(tm *TopologyManager) { tm.deploymentChanging = 1 }, func(tm *TopologyManager) { tm.deploymentChanging = 0 }},
	{"awaiting observation", func(tm *TopologyManager) { tm.deploymentAwaitingObservation = true }, func(tm *TopologyManager) { tm.deploymentAwaitingObservation = false }},
	{"promotion not yet observed", func(tm *TopologyManager) { tm.promotedSite = "dc1" }, func(tm *TopologyManager) { tm.promotedSite = "" }},
}

// recoveringReplica sets dc1 as the writable primary and dc2 as a healthy,
// converged replica still carrying RecoveryInProgress, whose drain is next.
func recoveringReplica(tm *TopologyManager) []*mysql.ReplicaStatus {
	tm.mu.Lock()
	tm.sites[0].state = state.StateWritable
	tm.sites[1].state = state.StateReadOnly
	tm.sites[1].sourceConvergenceState = sourceConvergenceConverged
	tm.recovery["dc2"] = &siteRecovery{state: recoveryStateInProgress}
	tm.lastFailoverTarget = "dc1"
	tm.mu.Unlock()
	return []*mysql.ReplicaStatus{nil, {IORunning: true, SQLRunning: true, SourceHost: "mysql-dc1"}}
}

func TestCheckRecovery_DefersDrainWhileTopologyHandoffInFlight(t *testing.T) {
	for _, tc := range handoffConditions {
		t.Run(tc.name, func(t *testing.T) {
			site0 := &mockMySQL{readOnly: false}
			site1 := &mockMySQL{readOnly: true}
			tm, _, _ := newTestTopologyManager(site0, site1)
			repl := recoveringReplica(tm)
			tm.mu.Lock()
			tc.set(tm)
			tm.mu.Unlock()

			if tm.checkRecovery(context.Background(), repl) {
				t.Fatal("recovery state cleared while a handoff owns the sites")
			}
			if site1.killConnectionCalls != 0 {
				t.Fatalf("drain killed %d times during the handoff, want 0", site1.killConnectionCalls)
			}
			tm.mu.RLock()
			rec := tm.recovery["dc2"]
			tm.mu.RUnlock()
			if rec == nil || rec.state != recoveryStateInProgress {
				t.Fatalf("recovery record = %+v, want RecoveryInProgress kept", rec)
			}

			tm.mu.Lock()
			tc.clear(tm)
			tm.mu.Unlock()
			if !tm.checkRecovery(context.Background(), repl) {
				t.Fatal("recovery did not complete once the handoff ended")
			}
			if site1.killConnectionCalls != 1 {
				t.Fatalf("drain passes after the handoff = %d, want 1", site1.killConnectionCalls)
			}
		})
	}
}

func TestCheckRecovery_FinishedDrainStillClearsDuringHandoff(t *testing.T) {
	site0 := &mockMySQL{readOnly: false}
	site1 := &mockMySQL{readOnly: true}
	tm, _, _ := newTestTopologyManager(site0, site1)
	repl := recoveringReplica(tm)
	tm.mu.Lock()
	tm.recovery["dc2"].drainComplete = true
	tm.plannedFailoverActive = true
	tm.mu.Unlock()

	if !tm.checkRecovery(context.Background(), repl) {
		t.Fatal("a site whose drain already finished was not cleared")
	}
	if site1.killConnectionCalls != 0 {
		t.Fatalf("kill passes = %d, want 0", site1.killConnectionCalls)
	}
}

// A handoff longer than the drain budget must not use the budget up: the
// first pass afterwards would report a timeout without evicting anything.
func TestCheckRecovery_DeferralRestartsDrainBudget(t *testing.T) {
	clk := clock.NewFakeClock(time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC))
	site0 := &mockMySQL{readOnly: false}
	site1 := &mockMySQL{readOnly: true, killConnectionResults: []int{2, 0}}
	tm, _, _ := newTestTopologyManagerWithBootstrapClock(site0, site1, clk)
	repl := recoveringReplica(tm)
	ctx := context.Background()

	if tm.checkRecovery(ctx, repl) {
		t.Fatal("first pass evicted sessions; the drain is not complete yet")
	}
	tm.SetPlannedFailoverActive(true)
	tm.checkRecovery(ctx, repl)
	clk.Advance(2 * defaultConnectionDrainTimeout)
	tm.checkRecovery(ctx, repl)
	tm.SetPlannedFailoverActive(false)

	if !tm.checkRecovery(ctx, repl) {
		t.Fatal("drain did not complete after the handoff")
	}
	if site1.killConnectionCalls != 2 {
		t.Fatalf("kill passes = %d, want 2: the pass after the handoff must evict, not time out", site1.killConnectionCalls)
	}
}

func TestCheckRecovery_DefersOldPrimaryRecoveryDuringHandoff(t *testing.T) {
	for _, tc := range handoffConditions {
		t.Run(tc.name, func(t *testing.T) {
			gtid := "aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa:1-10"
			site0 := &mockMySQL{readOnly: false, gtidExecuted: gtid}
			site1 := &mockMySQL{readOnly: true, gtidExecuted: gtid}
			tm, _, _ := newTestTopologyManagerWithBootstrap(site0, site1)
			setRecoveredTopology(tm)
			tm.mu.Lock()
			tc.set(tm)
			tm.mu.Unlock()

			if tm.checkRecovery(context.Background(), []*mysql.ReplicaStatus{nil, nil}) {
				t.Fatal("old-primary recovery acted during a handoff")
			}
			if site1.superReadOnlyCalls != 0 || site1.killConnectionCalls != 0 || site1.changeSourceCalls != 0 {
				t.Fatalf("recovery mutated the old primary during a handoff: fence=%d kill=%d changeSource=%d",
					site1.superReadOnlyCalls, site1.killConnectionCalls, site1.changeSourceCalls)
			}

			tm.mu.Lock()
			tc.clear(tm)
			tm.mu.Unlock()
			tm.checkRecovery(context.Background(), []*mysql.ReplicaStatus{nil, nil})
			if got := site1.startReplicaCallCount(); got != 1 {
				t.Fatalf("recovery sequences after the handoff = %d, want 1", got)
			}
		})
	}
}

func TestSourceConvergenceSuppressedDuringTopologyHandoff(t *testing.T) {
	for _, tc := range handoffConditions {
		t.Run(tc.name, func(t *testing.T) {
			tm, _, _ := newTestTopologyManager(&mockMySQL{}, &mockMySQL{readOnly: true})
			tm.mu.Lock()
			tc.set(tm)
			tm.mu.Unlock()
			if !tm.sourceConvergenceSuppressed() {
				t.Fatal("source convergence may repoint followers during a handoff")
			}
			tm.mu.RLock()
			audit := tm.gtidAuditSuppressedLocked()
			tm.mu.RUnlock()
			if !audit {
				t.Fatal("GTID audit and source convergence disagree on the handoff gate")
			}
		})
	}
}

// The barrier: a handoff publishing its flag waits for an admitted section,
// and no section is admitted once the flag is up.
func TestAdmitTopologyHandoffWaitsForAdmittedFollowerMutation(t *testing.T) {
	tm, _, _ := newTestTopologyManager(&mockMySQL{}, &mockMySQL{readOnly: true})
	release, ok := tm.beginFollowerMutation()
	if !ok {
		t.Fatal("mutation refused with no handoff in flight")
	}
	published := make(chan struct{})
	go func() {
		_ = tm.admitTopologyHandoff(context.Background(), func() {
			tm.SetPlannedFailoverActive(true)
			close(published)
		})
	}()
	select {
	case <-published:
		t.Fatal("handoff published while a follower mutation was still running")
	case <-time.After(50 * time.Millisecond):
	}
	release()
	select {
	case <-published:
	case <-time.After(5 * time.Second):
		t.Fatal("handoff never published after the mutation finished")
	}
	if _, ok := tm.beginFollowerMutation(); ok {
		t.Fatal("follower mutation admitted while a planned failover is in flight")
	}
}

// A recovery drain pass already killing sessions on the target when the
// planned failover reaches PlannedPromote must finish before Execute
// touches MySQL, and no further pass may start.
func TestPlannedPromote_WaitsForInFlightRecoveryDrain(t *testing.T) {
	site0 := &mockMySQL{readOnly: false}
	site1 := &mockMySQL{readOnly: true, killConnectionResults: []int{1, 0}}
	tm, _, _ := newTestTopologyManagerWithBootstrap(site0, site1)
	repl := recoveringReplica(tm)

	entered := make(chan struct{})
	unblock := make(chan struct{})
	var once sync.Once
	site1.onKill = func() {
		once.Do(func() {
			close(entered)
			<-unblock
		})
	}
	recoveryDone := make(chan struct{})
	go func() {
		defer close(recoveryDone)
		tm.checkRecovery(context.Background(), repl)
	}()
	<-entered

	tm.SetPlannedFailoverActive(true)
	promoted := make(chan error, 1)
	go func() {
		_, err := tm.PlannedPromote(context.Background(), "dc2", "dc1")
		promoted <- err
	}()
	time.Sleep(50 * time.Millisecond)
	site0.mu.Lock()
	fenced := site0.superReadOnlyCalls
	site0.mu.Unlock()
	site1.mu.Lock()
	promotedEarly := site1.promoted
	site1.mu.Unlock()
	if fenced != 0 || promotedEarly {
		t.Fatal("PlannedPromote started while a recovery drain pass was still killing sessions on its target")
	}

	close(unblock)
	<-recoveryDone
	if err := <-promoted; err != nil {
		t.Fatalf("PlannedPromote: %v", err)
	}
	tm.checkRecovery(context.Background(), repl)
	site1.mu.Lock()
	kills := site1.killConnectionCalls
	site1.mu.Unlock()
	if kills != 1 {
		t.Fatalf("recovery kill passes = %d, want only the one admitted before the handoff", kills)
	}
}

// Even if the planned-failover flag flaps off (the Dragonfly upgrade path
// writes the same flag), a poll during Execute must not drain the target:
// the topology change itself suppresses recovery.
func TestPlannedPromote_MidPromotionPollDoesNotDrainTarget(t *testing.T) {
	site0 := &mockMySQL{readOnly: false}
	site1 := &mockMySQL{readOnly: true}
	tm, _, _ := newTestTopologyManagerWithBootstrap(site0, site1)
	repl := recoveringReplica(tm)
	site1.onStopReplica = func() {
		tm.SetPlannedFailoverActive(false)
		tm.checkRecovery(context.Background(), repl)
	}
	tm.SetPlannedFailoverActive(true)
	if _, err := tm.PlannedPromote(context.Background(), "dc2", "dc1"); err != nil {
		t.Fatalf("PlannedPromote: %v", err)
	}
	if site1.killConnectionCalls != 0 {
		t.Fatalf("recovery drained the promotion target mid-promotion: %d kills", site1.killConnectionCalls)
	}

	// After the promotion and before any poll has observed it, the stale
	// tracker still shows dc2 read-only; promotedSite holds recovery off.
	tm.checkRecovery(context.Background(), repl)
	if site1.killConnectionCalls != 0 {
		t.Fatalf("recovery drained the new primary from pre-promotion observations: %d kills", site1.killConnectionCalls)
	}
}

// The RecoveryInProgress status write runs outside any admitted section: a
// stalled API server must not hold a planned promotion at the barrier, and
// a handoff admitted during the write defers the rewrite to a later poll.
func TestRecoveryStatusWriteDoesNotHoldHandoffBarrier(t *testing.T) {
	gtid := "aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa:1-10"
	site0 := &mockMySQL{readOnly: false, gtidExecuted: gtid}
	site1 := &mockMySQL{readOnly: true, gtidExecuted: gtid}
	tm, _, _ := newTestTopologyManagerWithBootstrap(site0, site1)
	setRecoveredTopology(tm)

	entered := make(chan struct{})
	unblock := make(chan struct{})
	var once sync.Once
	tm.StatusCallback = func(TopologySnapshot) {
		once.Do(func() {
			close(entered)
			<-unblock
		})
	}
	recoveryDone := make(chan struct{})
	go func() {
		defer close(recoveryDone)
		tm.checkRecovery(context.Background(), []*mysql.ReplicaStatus{nil, nil})
	}()
	<-entered

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := tm.admitTopologyHandoff(ctx, func() { tm.SetPlannedFailoverActive(true) }); err != nil {
		t.Fatalf("handoff waited on a blocked status write: %v", err)
	}
	close(unblock)
	<-recoveryDone
	site1.mu.Lock()
	changes := site1.changeSourceCalls
	site1.mu.Unlock()
	if changes != 0 {
		t.Fatalf("recovery rewrote the old primary after a handoff was admitted: %d CHANGE SOURCE", changes)
	}

	tm.SetPlannedFailoverActive(false)
	tm.checkRecovery(context.Background(), []*mysql.ReplicaStatus{nil, nil})
	if got := site1.startReplicaCallCount(); got != 1 {
		t.Fatalf("deferred rewrite did not run on the next poll: %d sequences", got)
	}
}

// A promotion whose context ends while an admitted recovery step still
// runs returns errHandoffNotAdmitted without touching MySQL, and leaves the
// gate usable.
func TestPlannedPromote_CancelledWhileRecoveryHoldsGate(t *testing.T) {
	site0 := &mockMySQL{readOnly: false}
	site1 := &mockMySQL{readOnly: true}
	tm, _, _ := newTestTopologyManagerWithBootstrap(site0, site1)
	tm.mu.Lock()
	tm.sites[0].state, tm.sites[1].state = state.StateWritable, state.StateReadOnly
	tm.mu.Unlock()

	release, ok := tm.beginFollowerMutation()
	if !ok {
		t.Fatal("setup: mutation refused")
	}
	tm.SetPlannedFailoverActive(true)
	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	start := time.Now()
	_, err := tm.PlannedPromote(ctx, "dc2", "dc1")
	if !errors.Is(err, errHandoffNotAdmitted) {
		t.Fatalf("PlannedPromote = %v, want errHandoffNotAdmitted", err)
	}
	if waited := time.Since(start); waited > 2*time.Second {
		t.Fatalf("PlannedPromote ignored its context for %v", waited)
	}
	site0.mu.Lock()
	fenced := site0.superReadOnlyCalls
	site0.mu.Unlock()
	if fenced != 0 || site1.promoted {
		t.Fatal("a promotion that was never admitted touched MySQL")
	}
	release()

	tm.SetPlannedFailoverActive(false)
	again, ok := tm.beginFollowerMutation()
	if !ok {
		t.Fatal("the abandoned handoff left the gate closed")
	}
	again()
	if err := tm.admitTopologyHandoff(context.Background(), func() {}); err != nil {
		t.Fatalf("gate refused a later handoff: %v", err)
	}
}

// The planned-failover reconciler rolls an unadmitted promotion back: the
// source is unfenced and nothing was sent to the target.
func TestPlannedFailoverPromoting_RollsBackWhenHandoffNotAdmitted(t *testing.T) {
	fg := plannedFailoverFG("")
	start := metav1.NewTime(time.Now().Add(-2 * time.Second))
	fg.Status.PlannedFailover = &v1alpha1.PlannedFailoverStatus{
		Phase: v1alpha1.PlannedFailoverPhasePromoting, Target: "pdx", SourcePrimary: "iad", StartTime: &start,
	}
	nn := fgNN(fg)
	iad := &mockMySQL{readOnly: true, superReadOnly: true}
	pdx := &mockMySQL{readOnly: true, superReadOnly: true}
	r, _ := newReconciler(fg)
	r.Runner = plannedDrainRunner(nn, iad, pdx)
	tm, err := r.Runner.plannedFailoverManager(nn)
	if err != nil {
		t.Fatal(err)
	}
	if !tm.followerMutationGate.enter() {
		t.Fatal("setup: gate refused")
	}
	defer tm.followerMutationGate.exit()

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := r.reconcilePlannedFailover(ctx, fetchFG(t, r, nn)); err != nil {
		t.Fatalf("reconcile: %v", err)
	}
	pf := fetchFG(t, r, nn).Status.PlannedFailover
	if pf.Phase != v1alpha1.PlannedFailoverPhaseFailed || pf.Reason != "RecoveryInFlight" {
		t.Fatalf("phase=%q reason=%q (%s), want Failed/RecoveryInFlight", pf.Phase, pf.Reason, pf.Message)
	}
	if iad.superReadOnly || iad.readOnly || !pdx.readOnly {
		t.Errorf("iad super_read_only=%v read_only=%v, pdx read_only=%v; want iad unfenced and pdx untouched",
			iad.superReadOnly, iad.readOnly, pdx.readOnly)
	}
}

// A promotion that fails after admission releases everything: once the
// planned failover ends, the next poll drains and clears the recovering
// replica.
func TestPlannedPromote_FailureAfterAdmissionReleasesRecovery(t *testing.T) {
	site0 := &mockMySQL{readOnly: false}
	site1 := &mockMySQL{readOnly: true, stopReplicaErr: errors.New("stop replica failed"),
		replicaStatusVal: &mysql.ReplicaStatus{IORunning: true, SQLRunning: true, SourceHost: "mysql-dc1"}}
	tm, _, _ := newTestTopologyManagerWithBootstrap(site0, site1)
	pollN(tm, 4)
	tm.mu.Lock()
	tm.recovery["dc2"] = &siteRecovery{state: recoveryStateInProgress}
	tm.mu.Unlock()

	tm.SetPlannedFailoverActive(true)
	if _, err := tm.PlannedPromote(context.Background(), "dc2", ""); err == nil {
		t.Fatal("setup: promotion should fail at STOP REPLICA")
	}
	tm.SetPlannedFailoverActive(false)
	pollN(tm, 2)

	tm.mu.RLock()
	rec := tm.recovery["dc2"]
	tm.mu.RUnlock()
	if rec != nil {
		t.Fatalf("recovery did not resume after the failed promotion: %+v", rec)
	}
	if site1.killConnectionCalls != 1 {
		t.Fatalf("drain passes = %d, want 1", site1.killConnectionCalls)
	}
}

// Behavioural: a wrong-source follower is left alone while an ordered
// update owns the sites.
func TestSourceConvergence_LeavesFollowerToRunningUpdate(t *testing.T) {
	primary := &mockMySQL{gtidExecuted: convergenceTestGTID}
	follower := &mockMySQL{readOnly: true, gtidExecuted: convergenceTestGTID,
		replicaStatusVal: &mysql.ReplicaStatus{IORunning: true, SQLRunning: true, SourceHost: "wrong"}}
	tm := newConvergenceManager(t, []state.SiteRole{state.SiteRolePrimaryCandidate, state.SiteRoleReadOnly}, primary, follower)
	tm.mu.Lock()
	tm.deploymentUpdateRunning = true
	tm.mu.Unlock()
	tm.checkSourceConvergence(context.Background(), []*mysql.ReplicaStatus{nil, follower.replicaStatusVal})
	if follower.stopReplicaCalls != 0 || follower.changeSourceCalls != 0 || follower.startReplicaCalls != 0 {
		t.Fatalf("convergence mutated a follower during an ordered update: stop=%d change=%d start=%d",
			follower.stopReplicaCalls, follower.changeSourceCalls, follower.startReplicaCalls)
	}
	if tm.sites[1].sourceConvergenceState != sourceConvergencePending {
		t.Fatalf("source convergence state = %s, want Pending", tm.sites[1].sourceConvergenceState)
	}
}

// What startManager hydrates (planned-failover flag, RecoveryInProgress)
// holds on the first polls: nothing is drained, fenced or repointed.
func TestFirstPollsAfterRestartLeavePlannedFailoverSitesAlone(t *testing.T) {
	site0 := &mockMySQL{readOnly: false}
	site1 := &mockMySQL{readOnly: true, replicaStatusVal: &mysql.ReplicaStatus{IORunning: true, SQLRunning: true, SourceHost: "mysql-dc1"}}
	tm, _, _ := newTestTopologyManagerWithBootstrap(site0, site1)
	tm.SetPlannedFailoverActive(true)
	tm.SetRecoveryInProgress("dc2")
	pollN(tm, 4)
	if site1.killConnectionCalls != 0 || site1.superReadOnlyCalls != 0 || site1.changeSourceCalls != 0 {
		t.Fatalf("first polls mutated the planned failover's target: kill=%d fence=%d changeSource=%d",
			site1.killConnectionCalls, site1.superReadOnlyCalls, site1.changeSourceCalls)
	}
}
