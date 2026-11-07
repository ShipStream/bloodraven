package controller

import (
	"context"
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

// TestBootstrapFailed_DegradedPersistsAndClearsOnRecovery pins A-72 and
// F-30: a failed clone is Bootstrapping=False/BootstrapFailed plus
// Degraded=True/BootstrapFailed, recomputed from the snapshot on every
// poll. It persists while the target does not replicate, refuses
// deployment leases, clears when the target replicates without another
// clone, and leases are granted again. Once the manager has latched that
// recovery, a later break of the site reports its own reason.
func TestBootstrapFailed_DegradedPersistsAndClearsOnRecovery(t *testing.T) {
	ctx := context.Background()
	m, fg, _ := deploymentLeaseFixture(t)
	runner := &TopologyManagerRunner{client: m.client, logger: slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelError})),
		managers: map[types.NamespacedName]*managedTopology{}}
	nn := client.ObjectKeyFromObject(fg)

	lag := int64(0)
	snapshot := func(pdx SiteSnapshot, phase BootstrapPhase) TopologySnapshot {
		snap := TopologySnapshot{
			ActiveSite: "iad",
			Sites: []SiteSnapshot{
				{Name: "iad", Role: state.SiteRolePrimaryCandidate, State: state.StateWritable},
				pdx,
			},
			DegradedReason:     v1alpha1.ReasonHealthy,
			BootstrapPhase:     string(phase),
			BootstrapSource:    "reclone",
			TopologyGeneration: fg.Status.TopologyGeneration,
			AppliedGeneration:  fg.Generation,
		}
		if phase == BootstrapPhaseFailed {
			snap.BootstrapTarget, snap.BootstrapError = "pdx", "clone plugin error"
		}
		return snap
	}
	broken := SiteSnapshot{Name: "pdx", Role: state.SiteRolePrimaryCandidate, State: state.StateReadOnly, ReplicaStatusRead: true,
		Replication: &mysql.ReplicaStatus{LastError: "Got fatal error 1236"}, SourceConvergenceState: sourceConvergenceConverged}
	healthy := SiteSnapshot{Name: "pdx", Role: state.SiteRolePrimaryCandidate, State: state.StateReadOnly, ReplicaStatusRead: true, ReplicationHealthy: true,
		Replication: &mysql.ReplicaStatus{IORunning: true, SQLRunning: true, SecondsBehindSource: &lag}, SourceConvergenceState: sourceConvergenceConverged}

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
			apimeta.FindStatusCondition(got.Status.Conditions, v1alpha1.ConditionBootstrapping)
	}

	// The failure holds across polls; it outranks the target's per-site
	// replication reasons.
	for poll := range 3 {
		got, degraded, boot := write(snapshot(broken, BootstrapPhaseFailed))
		if degraded == nil || degraded.Status != metav1.ConditionTrue || degraded.Reason != v1alpha1.ReasonBootstrapFailed {
			t.Fatalf("poll %d: Degraded = %+v, want True/BootstrapFailed", poll, degraded)
		}
		if boot == nil || boot.Status != metav1.ConditionFalse || boot.Reason != v1alpha1.ReasonBootstrapFailed {
			t.Fatalf("poll %d: Bootstrapping = %+v, want False/BootstrapFailed", poll, boot)
		}
		if reason := DeploymentUnstableReason(got); reason != v1alpha1.ReasonBootstrapFailed {
			t.Fatalf("poll %d: unstableReason = %q, want BootstrapFailed", poll, reason)
		}
		_, err := m.Grant(ctx, got, "migration", "op1", "tenant", "tenant-ns", "deployer", 30)
		if e := requireDeploymentError(t, err, 423, "unstable"); e.Reason != v1alpha1.ReasonBootstrapFailed {
			t.Fatalf("poll %d: refusal reason %q, want BootstrapFailed", poll, e.Reason)
		}
	}

	// The site replicates by another path: Degraded clears without a new
	// clone, Bootstrapping keeps reporting the last clone's failure, and
	// the group is stable again.
	got, degraded, boot := write(snapshot(healthy, BootstrapPhaseFailed))
	if degraded == nil || degraded.Status != metav1.ConditionFalse || degraded.Reason != v1alpha1.ReasonHealthy {
		t.Fatalf("after recovery Degraded = %+v, want False/Healthy", degraded)
	}
	if boot == nil || boot.Status != metav1.ConditionFalse || boot.Reason != v1alpha1.ReasonBootstrapFailed {
		t.Fatalf("after recovery Bootstrapping = %+v, want False/BootstrapFailed (the last clone's outcome)", boot)
	}
	if reason := DeploymentUnstableReason(got); reason != "" {
		t.Fatalf("after recovery unstableReason = %q, want stable", reason)
	}
	grantDeploymentLease(t, m, got, "migration", "op1", 30)

	// Recovered, then broken again for another reason (N-02): the manager
	// latched the resolution, so the old clone error and its reclone
	// remedy do not come back; the site's own reason is reported.
	resolved := snapshot(broken, BootstrapPhaseFailed)
	resolved.BootstrapFailureResolved = true
	_, degraded, boot = write(resolved)
	if degraded == nil || degraded.Reason == v1alpha1.ReasonBootstrapFailed {
		t.Fatalf("recovered, then broken: Degraded = %+v resurrects the old clone failure", degraded)
	}
	if boot == nil || boot.Status != metav1.ConditionFalse || boot.Reason != v1alpha1.ReasonBootstrapFailed {
		t.Fatalf("recovered, then broken: Bootstrapping = %+v, want False/BootstrapFailed (the last clone's outcome)", boot)
	}

	// A new clone ends the failure: Bootstrapping is True while it runs
	// and Degraded no longer reports the old failure.
	_, degraded, boot = write(snapshot(broken, BootstrapPhaseCloning))
	if boot == nil || boot.Status != metav1.ConditionTrue || boot.Reason != v1alpha1.ReasonCloning {
		t.Fatalf("new clone: Bootstrapping = %+v, want True/Cloning", boot)
	}
	if degraded != nil && degraded.Reason == v1alpha1.ReasonBootstrapFailed {
		t.Fatalf("new clone: Degraded = %+v still reports the previous failure", degraded)
	}

	// A finished clone is False/NotBootstrapping, never True.
	if _, _, boot = write(snapshot(healthy, BootstrapPhaseDone)); boot == nil || boot.Status != metav1.ConditionFalse || boot.Reason != v1alpha1.ReasonNotBootstrapping {
		t.Fatalf("finished clone: Bootstrapping = %+v, want False/NotBootstrapping", boot)
	}
}

// TestDegradedReason_BootstrapFailedRank pins where BootstrapFailed ranks:
// below the cross-site alerts, above SiteUnhealthy.
func TestDegradedReason_BootstrapFailedRank(t *testing.T) {
	failed := func(reason string) TopologySnapshot {
		return TopologySnapshot{DegradedReason: reason, BootstrapPhase: string(BootstrapPhaseFailed), BootstrapTarget: "pdx",
			Sites: []SiteSnapshot{{Name: "iad", State: state.StateWritable}, {Name: "pdx", State: state.StateReadOnly}}}
	}
	for verdict, want := range map[string]string{
		v1alpha1.ReasonHealthy:       v1alpha1.ReasonBootstrapFailed,
		v1alpha1.ReasonSiteUnhealthy: v1alpha1.ReasonBootstrapFailed,
		v1alpha1.ReasonSplitBrain:    v1alpha1.ReasonSplitBrain,
		v1alpha1.ReasonNoPrimary:     v1alpha1.ReasonNoPrimary,
		v1alpha1.ReasonTotalLoss:     v1alpha1.ReasonTotalLoss,
		v1alpha1.ReasonInitializing:  v1alpha1.ReasonInitializing,
	} {
		if got := degradedReason(failed(verdict)); got != want {
			t.Errorf("verdict %s with a standing clone failure: Degraded reason %s, want %s", verdict, got, want)
		}
	}
	// A failure whose target is now the writable primary has nothing left
	// to report.
	snap := failed(v1alpha1.ReasonHealthy)
	snap.BootstrapTarget = "iad"
	if got := degradedReason(snap); got != v1alpha1.ReasonHealthy {
		t.Errorf("target became the primary: Degraded reason %s, want Healthy", got)
	}
}
