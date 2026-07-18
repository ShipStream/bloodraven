package controller

import (
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/shipstream/bloodraven/internal/mysql"
	"github.com/shipstream/bloodraven/internal/platform"
	"github.com/shipstream/bloodraven/internal/state"
)

const convergenceTestGTID = "aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa:1-10"

func newConvergenceManager(t *testing.T, roles []state.SiteRole, checkers ...*mockMySQL) *TopologyManager {
	t.Helper()
	sites := make([]SiteTopologyConfig, len(checkers))
	interfaces := make([]mysql.Checker, len(checkers))
	for i := range checkers {
		name := []string{"primary", "follower-a", "follower-b"}[i]
		sites[i] = SiteTopologyConfig{Name: name, Role: roles[i], Host: "mysql-" + name + "-internal.ns.svc.cluster.local"}
		interfaces[i] = checkers[i]
	}
	tm := NewTopologyManager(TopologyConfig{Name: "fg", Sites: sites, ReadOnlyMaxLagSeconds: 30}, interfaces,
		NewFailoverController(testLogger()), nil, nil,
		BootstrapConfig{ReplUser: "repl", ReplPassword: "secret"}, newMockTainter(), platform.NewHub(testLogger()), nil, testLogger())
	tm.sites[0].state = state.StateWritable
	for i := 1; i < len(tm.sites); i++ {
		tm.sites[i].state = state.StateReadOnly
	}
	return tm
}

func TestCanonicalSourceHost(t *testing.T) {
	for _, input := range []string{
		"mysql-primary-internal.ns.svc.cluster.local",
		" MYSQL-PRIMARY-INTERNAL.NS.SVC.CLUSTER.LOCAL. ",
		"mysql-primary-internal.ns.svc.cluster.local:3306",
		"MYSQL-PRIMARY-INTERNAL.NS.SVC.CLUSTER.LOCAL.:3306",
	} {
		if got := canonicalSourceHost(input); got != "mysql-primary-internal.ns.svc.cluster.local" {
			t.Fatalf("canonicalSourceHost(%q) = %q", input, got)
		}
	}
}

func TestSourceConvergence_MultipleFollowers(t *testing.T) {
	primary := &mockMySQL{gtidExecuted: convergenceTestGTID}
	lag := int64(0)
	followerA := &mockMySQL{readOnly: true, gtidExecuted: convergenceTestGTID, replicaStatusVal: &mysql.ReplicaStatus{IORunning: true, SQLRunning: true, SourceHost: "intermediate", SecondsBehindSource: &lag}}
	followerB := &mockMySQL{readOnly: true, gtidExecuted: convergenceTestGTID, replicaStatusVal: &mysql.ReplicaStatus{IORunning: true, SQLRunning: true, SourceHost: "follower-a", SecondsBehindSource: &lag}}
	tm := newConvergenceManager(t, []state.SiteRole{state.SiteRolePrimaryCandidate, state.SiteRoleDROnly, state.SiteRoleReadOnly}, primary, followerA, followerB)
	repl := []*mysql.ReplicaStatus{nil, followerA.replicaStatusVal, followerB.replicaStatusVal}
	handled, changed := tm.checkSourceConvergence(context.Background(), repl)
	if !changed || len(handled) != 2 {
		t.Fatalf("changed=%v handled=%v", changed, handled)
	}
	for i, follower := range []*mockMySQL{followerA, followerB} {
		if follower.stopReplicaCalls != 1 || follower.changeSourceCalls != 1 || follower.startReplicaCalls != 1 || follower.resetReplicaCalls != 0 {
			t.Fatalf("follower %d calls stop=%d change=%d start=%d reset=%d", i, follower.stopReplicaCalls, follower.changeSourceCalls, follower.startReplicaCalls, follower.resetReplicaCalls)
		}
	}
	if tm.sites[2].sourceConvergenceState != sourceConvergenceConverged {
		t.Fatalf("reader source state = %s", tm.sites[2].sourceConvergenceState)
	}
}

func TestSourceConvergence_DivergenceBeforeAndAfterStop(t *testing.T) {
	t.Run("pre stop", func(t *testing.T) {
		primary := &mockMySQL{gtidExecuted: convergenceTestGTID}
		follower := &mockMySQL{readOnly: true, gtidExecuted: "bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb:1", replicaStatusVal: &mysql.ReplicaStatus{IORunning: true, SQLRunning: true, SourceHost: "wrong"}}
		tm := newConvergenceManager(t, []state.SiteRole{state.SiteRolePrimaryCandidate, state.SiteRoleReadOnly}, primary, follower)
		tm.checkSourceConvergence(context.Background(), []*mysql.ReplicaStatus{nil, follower.replicaStatusVal})
		if follower.stopReplicaCalls != 0 || tm.sites[1].sourceConvergenceReason != sourceReasonGTIDDiverged {
			t.Fatalf("stop=%d reason=%s", follower.stopReplicaCalls, tm.sites[1].sourceConvergenceReason)
		}
	})
	t.Run("post stop race", func(t *testing.T) {
		primary := &mockMySQL{gtidExecuted: convergenceTestGTID}
		follower := &mockMySQL{readOnly: true, gtidSequence: []string{"aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa:1-5", "bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb:1"}, replicaStatusVal: &mysql.ReplicaStatus{IORunning: true, SQLRunning: true, SourceHost: "wrong"}}
		tm := newConvergenceManager(t, []state.SiteRole{state.SiteRolePrimaryCandidate, state.SiteRoleReadOnly}, primary, follower)
		tm.checkSourceConvergence(context.Background(), []*mysql.ReplicaStatus{nil, follower.replicaStatusVal})
		if follower.stopReplicaCalls != 1 || follower.changeSourceCalls != 0 || follower.startReplicaCalls != 0 {
			t.Fatalf("calls stop=%d change=%d start=%d", follower.stopReplicaCalls, follower.changeSourceCalls, follower.startReplicaCalls)
		}
	})
}

func TestRepointReplica_RollbackUsesFreshBoundedContext(t *testing.T) {
	primary := &mockMySQL{gtidExecuted: convergenceTestGTID, respectContext: true}
	follower := &mockMySQL{
		readOnly: true, gtidExecuted: convergenceTestGTID,
		replicaStatusVal: &mysql.ReplicaStatus{IORunning: true, SQLRunning: true, SourceHost: "wrong"},
	}
	tm := newConvergenceManager(t, []state.SiteRole{state.SiteRolePrimaryCandidate, state.SiteRoleReadOnly}, primary, follower)
	opCtx, cancel := context.WithCancel(context.Background())
	follower.stopReplicaCancel = cancel

	err := tm.repointReplica(opCtx, &tm.sites[0], &tm.sites[1], "wrong")
	if err == nil || !strings.Contains(err.Error(), sourceReasonProbeFailed) {
		t.Fatalf("repointReplica() error = %v, want post-stop probe failure", err)
	}
	if follower.startReplicaCalls != 1 {
		t.Fatalf("rollback START REPLICA calls = %d, want 1", follower.startReplicaCalls)
	}
	if got := follower.startReplicaCtxErrs[0]; got != nil {
		t.Fatalf("rollback START REPLICA reused canceled operation context: %v", got)
	}
}

func TestSourceConvergence_MutationFailuresAndSuppression(t *testing.T) {
	for _, tc := range []struct {
		name      string
		configure func(*mockMySQL)
	}{
		{name: "stop", configure: func(f *mockMySQL) { f.stopReplicaErr = errors.New("stop") }},
		{name: "change", configure: func(f *mockMySQL) { f.changeSourceErr = errors.New("change") }},
		{name: "start", configure: func(f *mockMySQL) { f.startReplicaErr = errors.New("start") }},
		{name: "verify", configure: func(f *mockMySQL) { f.changeDoesNotUpdate = true }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			primary := &mockMySQL{gtidExecuted: convergenceTestGTID}
			follower := &mockMySQL{readOnly: true, gtidExecuted: convergenceTestGTID, replicaStatusVal: &mysql.ReplicaStatus{IORunning: true, SQLRunning: true, SourceHost: "wrong"}}
			tc.configure(follower)
			tm := newConvergenceManager(t, []state.SiteRole{state.SiteRolePrimaryCandidate, state.SiteRoleReadOnly}, primary, follower)
			tm.checkSourceConvergence(context.Background(), []*mysql.ReplicaStatus{nil, follower.replicaStatusVal})
			if tm.sites[1].sourceConvergenceState != sourceConvergencePending || follower.resetReplicaCalls != 0 {
				t.Fatalf("state=%s reset=%d", tm.sites[1].sourceConvergenceState, follower.resetReplicaCalls)
			}
		})
	}

	for _, tc := range []struct {
		name     string
		suppress func(*TopologyManager)
	}{
		{name: "pending promotion", suppress: func(tm *TopologyManager) { tm.promotedSite = "primary" }},
		{name: "bootstrap", suppress: func(tm *TopologyManager) { tm.bootstrapPhase = BootstrapPhaseCloning }},
		{name: "restore", suppress: func(tm *TopologyManager) { tm.autoBootstrapSuppressed = true }},
		{name: "topology freeze", suppress: func(tm *TopologyManager) { tm.topologyFrozen = true }},
		{name: "planned failover", suppress: func(tm *TopologyManager) { tm.plannedFailoverActive = true }},
		{name: "ordered update", suppress: func(tm *TopologyManager) {
			tm.updater = NewUpdateController(NewFailoverController(testLogger()), testLogger())
			tm.updater.updating = true
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			primary := &mockMySQL{gtidExecuted: convergenceTestGTID}
			follower := &mockMySQL{readOnly: true, gtidExecuted: convergenceTestGTID, replicaStatusVal: &mysql.ReplicaStatus{IORunning: true, SQLRunning: true, SourceHost: "wrong"}}
			tm := newConvergenceManager(t, []state.SiteRole{state.SiteRolePrimaryCandidate, state.SiteRoleReadOnly}, primary, follower)
			tc.suppress(tm)
			tm.checkSourceConvergence(context.Background(), []*mysql.ReplicaStatus{nil, follower.replicaStatusVal})
			if follower.stopReplicaCalls != 0 || tm.sites[1].sourceConvergenceState != sourceConvergencePending {
				t.Fatalf("suppressed convergence mutated follower")
			}
		})
	}
}
