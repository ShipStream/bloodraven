package controller

import (
	"context"
	"testing"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"

	v1alpha1 "github.com/shipstream/bloodraven/api/v1alpha1"
	"github.com/shipstream/bloodraven/internal/clock"
	"github.com/shipstream/bloodraven/internal/mysql"
	"github.com/shipstream/bloodraven/internal/platform"
	"github.com/shipstream/bloodraven/internal/state"
)

// A replica-audit divergence report is an integrity warning, not an
// outage: like replication lag it must not make the deployment API refuse
// leases. A returning old primary's divergence (a post-failover recovery
// state) still does, as before, and so does anything the report would
// otherwise hide.
func TestDeploymentUnstableReason_ReplicaAuditDivergence(t *testing.T) {
	count := int64(221)
	block := func(fg *v1alpha1.MysqlFailoverGroup, site int, detectedBy string) {
		s := &fg.Status.Sites[site]
		s.RecoveryState = recoveryStateBlocked
		s.DivergentGtid = issue202ErrantGTID
		s.DivergentTransactionCount = &count
		s.DivergenceDetectedBy = detectedBy
		s.DivergenceComparedTo = "iad"
	}
	conditions := func(fg *v1alpha1.MysqlFailoverGroup) {
		fg.Status.Conditions = append(fg.Status.Conditions,
			metav1.Condition{Type: "Degraded", Status: metav1.ConditionTrue, Reason: "DivergentTransactions", LastTransitionTime: metav1.Now()},
			metav1.Condition{Type: "RecoveryPending", Status: metav1.ConditionTrue, Reason: "DivergentTransactions", LastTransitionTime: metav1.Now()})
	}
	for _, tc := range []struct {
		name   string
		change func(*v1alpha1.MysqlFailoverGroup)
		stable bool
	}{
		{"replica audit block on a replicating follower", func(fg *v1alpha1.MysqlFailoverGroup) {
			block(fg, 1, divergenceDetectedByAudit)
			conditions(fg)
		}, true},
		{"old primary block", func(fg *v1alpha1.MysqlFailoverGroup) {
			block(fg, 1, divergenceDetectedByRecovery)
			conditions(fg)
		}, false},
		{"block restored without provenance (CRD not updated)", func(fg *v1alpha1.MysqlFailoverGroup) {
			block(fg, 1, "")
			conditions(fg)
		}, false},
		{"replica audit block on a replica that is not replicating", func(fg *v1alpha1.MysqlFailoverGroup) {
			block(fg, 1, divergenceDetectedByAudit)
			conditions(fg)
			fg.Status.Sites[1].Replicating = false
		}, false},
		{"replica audit block alongside recovery in progress", func(fg *v1alpha1.MysqlFailoverGroup) {
			block(fg, 1, divergenceDetectedByAudit)
			conditions(fg)
			fg.Status.Sites = append(fg.Status.Sites, v1alpha1.SiteStatus{Name: "ord", State: "read-only", Replicating: true, RecoveryState: recoveryStateInProgress})
			fg.Spec.Sites = append(fg.Spec.Sites, fg.Spec.Sites[1])
			fg.Spec.Sites[2].Name = "ord"
		}, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			m, fg, _ := deploymentLeaseFixture(t)
			tc.change(fg)
			if err := m.client.Status().Update(context.Background(), fg); err != nil {
				t.Fatal(err)
			}
			reason := DeploymentUnstableReason(fg)
			if tc.stable && reason != "" {
				t.Fatalf("DeploymentUnstableReason = %q, want stable: a replica-audit report must not block deployments", reason)
			}
			if !tc.stable && reason == "" {
				t.Fatal("DeploymentUnstableReason = stable, want the group refused")
			}
			_, err := m.Grant(context.Background(), fg, "migration", "op1", "tenant", "tenant-ns", "deployer", 30)
			if tc.stable && err != nil {
				t.Fatalf("Grant() error = %v, want granted", err)
			}
			if !tc.stable {
				requireDeploymentError(t, err, 423, "unstable")
			}
		})
	}
}

// When a replica-audit report is not the only recovery state, the lease is
// refused for the other site's reason: a replica's divergence never is the
// reason the deployment API gives.
func TestDeploymentUnstableReason_NamesTheGatingRecoveryState(t *testing.T) {
	m, fg, _ := deploymentLeaseFixture(t)
	count := int64(221)
	fg.Status.Sites[1].RecoveryState = recoveryStateBlocked
	fg.Status.Sites[1].DivergentGtid = issue202ErrantGTID
	fg.Status.Sites[1].DivergentTransactionCount = &count
	fg.Status.Sites[1].DivergenceDetectedBy = divergenceDetectedByAudit
	fg.Spec.Sites = append(fg.Spec.Sites, fg.Spec.Sites[1])
	fg.Spec.Sites[2].Name = "ord"
	fg.Status.Sites = append(fg.Status.Sites, v1alpha1.SiteStatus{Name: "ord", State: "read-only", Replicating: true, RecoveryState: recoveryStateInProgress})
	fg.Status.Conditions = append(fg.Status.Conditions,
		metav1.Condition{Type: "Degraded", Status: metav1.ConditionTrue, Reason: "DivergentTransactions", LastTransitionTime: metav1.Now()},
		metav1.Condition{Type: "RecoveryPending", Status: metav1.ConditionTrue, Reason: "DivergentTransactions", LastTransitionTime: metav1.Now()})
	if err := m.client.Status().Update(context.Background(), fg); err != nil {
		t.Fatal(err)
	}
	if got := DeploymentUnstableReason(fg); got != recoveryStateInProgress {
		t.Fatalf("DeploymentUnstableReason = %q, want %q (the site whose recovery gates)", got, recoveryStateInProgress)
	}
}

// An old primary's block keeps gating after someone reattaches its
// replication by hand: the audit then re-verifies the block, but must not
// relabel the unresolved old-primary loss as a replica-audit warning and
// so open the deployment API.
func TestDeploymentGate_OldPrimaryBlockSurvivesReattachAndAudit(t *testing.T) {
	const primaryHost = "mysql-iad-internal.ns.svc.cluster.local"
	iad := auditPrimary(issue202PrimaryUUID, issue202PrimaryGTID)
	pdx := &auditServer{mockMySQL: &mockMySQL{readOnly: true, superReadOnly: true, gtidExecuted: issue202ReplicaGTID}, uuid: issue202ReplicaUUID}
	clk := clock.NewFakeClock(time.Date(2026, 10, 29, 5, 0, 0, 0, time.UTC))
	sites := []SiteTopologyConfig{
		{Name: "iad", Role: state.SiteRolePrimaryCandidate, Host: primaryHost},
		{Name: "pdx", Role: state.SiteRolePrimaryCandidate, Host: "mysql-pdx-internal.ns.svc.cluster.local"},
	}
	tm := NewTopologyManagerWithClock(TopologyConfig{Name: "lion", Sites: sites}, []mysql.Checker{iad, pdx},
		NewFailoverController(testLogger()), nil, nil, BootstrapConfig{ReplUser: "repl", ReplPassword: "secret"},
		newMockTainter(), platform.NewHub(testLogger()), nil, testLogger(), clk)

	m, fg, _ := deploymentLeaseFixture(t)
	runner := &TopologyManagerRunner{client: m.client, logger: testLogger(), managers: make(map[types.NamespacedName]*managedTopology)}
	nn := types.NamespacedName{Name: fg.Name, Namespace: fg.Namespace}
	runner.SetManagerForTest(nn, tm)
	tm.StatusCallback = func(snap TopologySnapshot) {
		tm.MarkStatusWriteResult(runner.updateCRStatus(context.Background(), nn, snap))
	}

	// 1. The source-less returning old primary is compared by recovery.
	for i := 0; i < 4; i++ {
		clk.Advance(time.Second)
		tm.Poll(context.Background())
	}
	if rec := recoveryOf(tm, "pdx"); rec.state != recoveryStateBlocked || rec.detectedBy != divergenceDetectedByRecovery {
		t.Fatalf("setup: recovery = %+v, want an OldPrimaryRecovery block", rec)
	}

	// 2. An administrator reattaches replication without reconciling the
	// lost transactions; 3. the audit re-verifies the block.
	lag := int64(0)
	pdx.mockMySQL.mu.Lock()
	pdx.replicaStatusVal = &mysql.ReplicaStatus{IORunning: true, SQLRunning: true, SourceHost: primaryHost, SecondsBehindSource: &lag}
	pdx.mockMySQL.mu.Unlock()
	for i := 0; i < 3; i++ {
		clk.Advance(recoveryRetryDelay + time.Second)
		tm.Poll(context.Background())
	}
	if pdx.reads() == 0 {
		t.Fatal("setup: the audit never compared the reattached site")
	}
	rec := recoveryOf(tm, "pdx")
	if rec.state != recoveryStateBlocked || rec.divergentCount != 221 {
		t.Fatalf("recovery = %+v, want the block kept", rec)
	}
	if rec.detectedBy != divergenceDetectedByRecovery {
		t.Errorf("provenance downgraded to %q, want %q", rec.detectedBy, divergenceDetectedByRecovery)
	}

	// 4. The published status still refuses leases.
	fresh := &v1alpha1.MysqlFailoverGroup{}
	if err := m.client.Get(context.Background(), nn, fresh); err != nil {
		t.Fatal(err)
	}
	if reason := DeploymentUnstableReason(fresh); reason == "" {
		t.Fatal("DeploymentUnstableReason = stable: an unresolved old-primary block opened the deployment API")
	}
	_, err := m.Grant(context.Background(), fresh, "migration", "op1", "tenant", "tenant-ns", "deployer", 30)
	requireDeploymentError(t, err, 423, "unstable")
}
