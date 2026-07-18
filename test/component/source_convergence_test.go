package component

import (
	"context"
	"io"
	"log/slog"
	"testing"

	"github.com/shipstream/bloodraven/internal/controller"
	"github.com/shipstream/bloodraven/internal/mysql"
	"github.com/shipstream/bloodraven/internal/platform"
	"github.com/shipstream/bloodraven/internal/state"
)

func TestSourceConvergenceAfterRestartWithoutFailoverHistory(t *testing.T) {
	gtid := "aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa:1-10"
	primary := &mockMySQL{readOnly: false, gtidExecuted: gtid}
	candidate := &mockMySQL{readOnly: true, gtidExecuted: gtid, replicaStatus: &mysql.ReplicaStatus{IORunning: true, SQLRunning: true, SourceHost: "old-hop"}}
	reader := &mockMySQL{readOnly: true, gtidExecuted: gtid, replicaStatus: &mysql.ReplicaStatus{IORunning: true, SQLRunning: true, SourceHost: "candidate-internal"}}
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	tm := controller.NewTopologyManager(controller.TopologyConfig{
		Name: "lion", FailureThreshold: 1, RecoveryThreshold: 1, ReadOnlyMaxLagSeconds: 30,
		Sites: []controller.SiteTopologyConfig{
			{Name: "primary", Role: state.SiteRolePrimaryCandidate, Host: "primary-internal"},
			{Name: "candidate", Role: state.SiteRolePrimaryCandidate, Host: "candidate-internal"},
			{Name: "reader", Role: state.SiteRoleReadOnly, Host: "reader-internal"},
		},
	}, []mysql.Checker{primary, candidate, reader}, controller.NewFailoverController(logger), nil, nil,
		controller.BootstrapConfig{ReplUser: "repl", ReplPassword: "secret"}, newMockTainter(), platform.NewHub(logger), nil, logger)

	tm.Poll(context.Background())
	for name, follower := range map[string]*mockMySQL{"candidate": candidate, "reader": reader} {
		follower.mu.Lock()
		host := follower.replicaStatus.SourceHost
		changed := follower.replicationSourceSet
		reset := follower.resetReplicaAll
		follower.mu.Unlock()
		if !changed || host != "primary-internal" || reset {
			t.Fatalf("%s changed=%v source=%q reset=%v", name, changed, host, reset)
		}
	}
}
