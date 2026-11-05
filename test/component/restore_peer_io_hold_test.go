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

// stoppablePeer is a mockMySQL that can stop only its replication
// receiver, as the production checker can (mysql.ReplicaIOStopper).
type stoppablePeer struct {
	*mockMySQL
	ioStops int
}

func (p *stoppablePeer) StopReplicaIOThread(_ context.Context) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.ioStops++
	if p.replicaStatus != nil {
		p.replicaStatus.IORunning, p.replicaStatus.IOConnecting = false, false
	}
	return nil
}

func (p *stoppablePeer) MissingReplicaIOStopPrivileges(_ context.Context) ([]string, error) {
	return nil, nil
}

func (p *stoppablePeer) receiver() (io bool, started bool) {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.replicaStatus.IORunning, p.replicaStarted
}

func (p *stoppablePeer) startByHand() {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.replicaStatus.IORunning = true
}

func newHeldPeerTopology(t *testing.T) (*controller.TopologyManager, *stoppablePeer) {
	t.Helper()
	gtid := "aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa:1-10"
	primary := &mockMySQL{readOnly: false, gtidExecuted: gtid}
	peer := &stoppablePeer{mockMySQL: &mockMySQL{readOnly: true, gtidExecuted: gtid, replicaStatus: &mysql.ReplicaStatus{
		IORunning: true, SQLRunning: true, SourceHost: "primary-internal", RetrievedGtidSet: gtid, ExecutedGtidSet: gtid,
	}}}
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	tm := controller.NewTopologyManager(controller.TopologyConfig{
		Name: "lion", FailureThreshold: 1, RecoveryThreshold: 1, ReadOnlyMaxLagSeconds: 30,
		Sites: []controller.SiteTopologyConfig{
			{Name: "primary", Role: state.SiteRolePrimaryCandidate, Host: "primary-internal"},
			{Name: "peer", Role: state.SiteRolePrimaryCandidate, Host: "peer-internal"},
		},
	}, []mysql.Checker{primary, peer}, controller.NewFailoverController(logger), nil, nil,
		controller.BootstrapConfig{ReplUser: "repl", ReplPassword: "secret"}, newMockTainter(), platform.NewHub(logger), nil, logger)
	tm.Poll(context.Background())

	// Fencing: the restore freezes the topology and stops the receiver.
	tm.SetTopologyFrozen(true)
	out, err := tm.StopRestorePeerReplication(context.Background(), "peer", gtid, true)
	if err != nil || !out.Stopped {
		t.Fatalf("stop = %+v, %v", out, err)
	}
	if io, _ := peer.receiver(); io {
		t.Fatal("receiver running after the verified stop")
	}
	peer.mu.Lock()
	peer.replicaStarted = false
	peer.mu.Unlock()
	return tm, peer
}

// #212: through the restore and after a failed one (queue Held, topology
// unfrozen), polls never restart the held peer's receiver and re-stop one
// started by hand; removing the queue resumes it.
func TestRestorePeerHold_FailedRestoreKeepsPeerStoppedUntilReleased(t *testing.T) {
	tm, peer := newHeldPeerTopology(t)
	ctx := context.Background()

	// Restoring: Armed queue past Fencing.
	tm.SetRestorePeerHold([]string{"peer"}, []string{"peer"}, true)
	for i := 0; i < 5; i++ {
		tm.Poll(ctx)
	}
	if io, started := peer.receiver(); io || started {
		t.Fatalf("during the restore io=%v started=%v, want the receiver held stopped", io, started)
	}
	peer.startByHand()
	tm.Poll(ctx)
	if io, _ := peer.receiver(); io {
		t.Fatal("a receiver started by hand during the restore was not re-stopped")
	}

	// Failed and the primary changed: Held, topology unfrozen.
	tm.SetTopologyFrozen(false)
	tm.SetRestoreRecloneBlocked([]string{"peer"})
	tm.SetRestoreReclonePending([]string{"peer"})
	for i := 0; i < 10; i++ {
		tm.Poll(ctx)
	}
	if io, started := peer.receiver(); io || started {
		t.Fatalf("after the failed restore io=%v started=%v, want the receiver held stopped", io, started)
	}

	// The administrator removes the queue: replication resumes.
	tm.SetRestorePeerHold(nil, nil, true)
	tm.SetRestoreRecloneBlocked(nil)
	tm.SetRestoreReclonePending(nil)
	tm.Poll(ctx)
	if io, started := peer.receiver(); !io || !started {
		t.Fatalf("after the queue was removed io=%v started=%v, want replication resumed", io, started)
	}
}

// A Draining site is not re-stopped (its reclone sets replication up), but
// nothing else starts it either.
func TestRestorePeerHold_DrainingSiteIsNotRestartedOrReStopped(t *testing.T) {
	tm, peer := newHeldPeerTopology(t)
	ctx := context.Background()
	tm.SetTopologyFrozen(false)
	tm.SetRestorePeerHold([]string{"peer"}, nil, true)
	tm.SetRestoreReclonePending([]string{"peer"})
	for i := 0; i < 5; i++ {
		tm.Poll(ctx)
	}
	if io, started := peer.receiver(); io || started {
		t.Fatalf("Draining io=%v started=%v, want the stopped receiver left alone", io, started)
	}
	stops := peer.ioStops
	peer.startByHand()
	tm.Poll(ctx)
	if io, _ := peer.receiver(); !io || peer.ioStops != stops {
		t.Fatalf("Draining: a running receiver was re-stopped (io=%v stops=%d)", io, peer.ioStops)
	}
}
