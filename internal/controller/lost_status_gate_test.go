package controller

import (
	"context"
	"errors"
	"log/slog"
	"os"
	"testing"

	apimeta "k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"

	v1alpha1 "github.com/shipstream/bloodraven/api/v1alpha1"
	"github.com/shipstream/bloodraven/internal/mysql"
	"github.com/shipstream/bloodraven/internal/state"
)

// N-01 (visibility): while the lost-status wait stands the group is
// Degraded=True and Ready=False with reason AwaitingPrimaryObservation, the
// deployment API reports that reason and refuses leases. Once the wait ends
// the group is healthy and stable again.
func TestLostStatusWait_ReportedOnDegradedAndRefusesLeases(t *testing.T) {
	ctx := context.Background()
	m, fg, _ := deploymentLeaseFixture(t)
	runner := &TopologyManagerRunner{client: m.client, logger: slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelError})),
		managers: map[types.NamespacedName]*managedTopology{}}
	nn := client.ObjectKeyFromObject(fg)
	lag := int64(0)
	snapshot := func(awaiting bool) TopologySnapshot {
		snap := TopologySnapshot{
			Sites: []SiteSnapshot{
				{Name: "iad", Role: state.SiteRolePrimaryCandidate, State: state.StateWritable},
				{Name: "pdx", Role: state.SiteRolePrimaryCandidate, State: state.StateReadOnly, ReplicaStatusRead: true, ReplicationHealthy: true,
					Replication: &mysql.ReplicaStatus{IORunning: true, SQLRunning: true, SecondsBehindSource: &lag}, SourceConvergenceState: sourceConvergenceConverged},
			},
			DegradedReason:          v1alpha1.ReasonHealthy,
			TopologyGeneration:      fg.Status.TopologyGeneration,
			AppliedGeneration:       fg.Generation,
			AwaitingObservedPrimary: awaiting,
		}
		if !awaiting {
			snap.ActiveSite = "iad"
		}
		return snap
	}
	write := func(snap TopologySnapshot) (*v1alpha1.MysqlFailoverGroup, *metav1.Condition, *metav1.Condition) {
		t.Helper()
		if err := runner.updateCRStatus(ctx, nn, snap); err != nil {
			t.Fatal(err)
		}
		var got v1alpha1.MysqlFailoverGroup
		if err := m.client.Get(ctx, nn, &got); err != nil {
			t.Fatal(err)
		}
		return &got, apimeta.FindStatusCondition(got.Status.Conditions, v1alpha1.ConditionDegraded),
			apimeta.FindStatusCondition(got.Status.Conditions, v1alpha1.ConditionReady)
	}

	got, degraded, ready := write(snapshot(true))
	if degraded == nil || degraded.Status != metav1.ConditionTrue || degraded.Reason != v1alpha1.ReasonAwaitingPrimaryObservation {
		t.Fatalf("Degraded = %+v, want True/AwaitingPrimaryObservation", degraded)
	}
	if ready == nil || ready.Status != metav1.ConditionFalse || ready.Reason != v1alpha1.ReasonAwaitingPrimaryObservation {
		t.Fatalf("Ready = %+v, want False/AwaitingPrimaryObservation", ready)
	}
	if reason := DeploymentUnstableReason(got); reason != v1alpha1.ReasonAwaitingPrimaryObservation {
		t.Fatalf("unstableReason = %q, want AwaitingPrimaryObservation", reason)
	}
	_, err := m.Grant(ctx, got, "migration", "op1", "tenant", "tenant-ns", "deployer", 30)
	if e := requireDeploymentError(t, err, 423, "unstable"); e.Reason != v1alpha1.ReasonAwaitingPrimaryObservation {
		t.Fatalf("refusal reason %q, want AwaitingPrimaryObservation", e.Reason)
	}

	got, degraded, ready = write(snapshot(false))
	if degraded == nil || degraded.Status != metav1.ConditionFalse || degraded.Reason != v1alpha1.ReasonHealthy {
		t.Fatalf("after the wait Degraded = %+v, want False/Healthy", degraded)
	}
	if ready == nil || ready.Status != metav1.ConditionTrue {
		t.Fatalf("after the wait Ready = %+v, want True", ready)
	}
	if reason := DeploymentUnstableReason(got); reason != "" {
		t.Fatalf("after the wait unstableReason = %q, want stable", reason)
	}
	grantDeploymentLease(t, m, got, "migration", "op1", 30)
}

// TestDegradedReason_AwaitingPrimaryObservationRank pins where the wait
// ranks: above every verdict that would otherwise tell an administrator the
// operator is handling it, below SplitBrain and TotalLoss, and never over
// Initializing's Unknown.
func TestDegradedReason_AwaitingPrimaryObservationRank(t *testing.T) {
	for verdict, want := range map[string]string{
		v1alpha1.ReasonHealthy:        v1alpha1.ReasonAwaitingPrimaryObservation,
		v1alpha1.ReasonSiteUnhealthy:  v1alpha1.ReasonAwaitingPrimaryObservation,
		v1alpha1.ReasonNoPrimary:      v1alpha1.ReasonAwaitingPrimaryObservation,
		v1alpha1.ReasonCrossSiteAlert: v1alpha1.ReasonAwaitingPrimaryObservation,
		v1alpha1.ReasonSplitBrain:     v1alpha1.ReasonSplitBrain,
		v1alpha1.ReasonTotalLoss:      v1alpha1.ReasonTotalLoss,
		v1alpha1.ReasonInitializing:   v1alpha1.ReasonInitializing,
	} {
		snap := TopologySnapshot{DegradedReason: verdict, AwaitingObservedPrimary: true,
			BootstrapPhase: string(BootstrapPhaseFailed), BootstrapTarget: "pdx",
			Sites: []SiteSnapshot{{Name: "iad", State: state.StateWritable}, {Name: "pdx", State: state.StateReadOnly}}}
		if got := degradedReason(snap); got != want {
			t.Errorf("verdict %s during the wait: Degraded reason %s, want %s", verdict, got, want)
		}
	}
}

// A topology manager replaced in this process (a config edit, a runner
// restart of the manager) keeps a standing wait even when the object it is
// built from does not look lost (a cache that has not caught up).
func TestCarryLostStatusWait(t *testing.T) {
	old, _, _ := newTestTopologyManager(&mockMySQL{}, &mockMySQL{readOnly: true})
	repl, _, _ := newTestTopologyManager(&mockMySQL{}, &mockMySQL{readOnly: true})
	if repl.carryLostStatusWait(old) {
		t.Fatal("carried a wait the predecessor does not hold")
	}
	old.armLostStatusWait()
	if !repl.carryLostStatusWait(old) {
		t.Fatal("did not carry the predecessor's wait")
	}
	repl.mu.RLock()
	armed := repl.awaitingObservedPrimary
	repl.mu.RUnlock()
	if !armed {
		t.Fatal("the replacement does not wait")
	}
	if repl.carryLostStatusWait(repl) {
		t.Fatal("carried a wait onto itself")
	}
}

// N-02: once the failed bootstrap's target has been seen replicating, the
// failure no longer stands. A later, unrelated replication error on that
// site must not bring Degraded=BootstrapFailed and its old clone error back,
// while Bootstrapping keeps reporting the last clone's outcome. A new
// bootstrap resets the latch.
func TestBootstrapFailure_ResolutionLatches(t *testing.T) {
	dc1 := &mockMySQL{readOnly: false}
	dc2 := &mockMySQL{readOnly: true, replicaStatusVal: &mysql.ReplicaStatus{IORunning: true, SQLRunning: true, SourceHost: "mysql-dc1"}}
	tm, _, _ := newTestTopologyManager(dc1, dc2)
	tm.mu.Lock()
	tm.bootstrapPhase = BootstrapPhaseFailed
	tm.bootstrapErr = errors.New("clone plugin error")
	tm.bootstrapFailedTarget = "dc2"
	tm.mu.Unlock()
	stands := func() (TopologySnapshot, bool) {
		snap := tm.buildSnapshot(nil)
		return snap, bootstrapFailureStands(snap)
	}
	if _, ok := stands(); !ok {
		t.Fatal("setup: the failure does not stand before the target replicates")
	}
	pollN(tm, 4)
	snap, ok := stands()
	if ok || !snap.BootstrapFailureResolved {
		t.Fatalf("target replicates: BootstrapFailed stands=%t resolved=%t", ok, snap.BootstrapFailureResolved)
	}

	// The target breaks later for an unrelated reason.
	dc2.mu.Lock()
	dc2.replicaStatusVal = &mysql.ReplicaStatus{IORunning: false, SQLRunning: true, SourceHost: "mysql-dc1", LastIOError: "error reconnecting to source"}
	dc2.mu.Unlock()
	pollN(tm, 2)
	snap, ok = stands()
	if ok {
		t.Fatalf("an unrelated replication error brought the old clone failure back: %+v", snap)
	}
	if BootstrapPhase(snap.BootstrapPhase) != BootstrapPhaseFailed || snap.BootstrapError == "" {
		t.Fatalf("Bootstrapping no longer reports the last clone's outcome: phase %q error %q", snap.BootstrapPhase, snap.BootstrapError)
	}
	dc2.setError(errors.New("connection refused"))
	pollN(tm, 4)
	if snap, ok = stands(); ok {
		t.Fatalf("an outage of the target brought the old clone failure back: %+v", snap)
	}

	// A new bootstrap and its failure start over.
	if _, why := tm.claimBootstrap("dc2", "reclone"); why != "" {
		t.Fatalf("setup: %s", why)
	}
	tm.mu.Lock()
	tm.bootstrapPhase = BootstrapPhaseFailed
	tm.bootstrapFailedTarget = "dc2"
	tm.mu.Unlock()
	if _, ok := stands(); !ok {
		t.Fatal("a new failure does not stand: the latch survived a new bootstrap")
	}
}
