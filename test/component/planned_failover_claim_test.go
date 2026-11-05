package component

// Planned-failover durable claim (#235) component tests.
//
// This file holds the per-interleaving tests of spec §14 (TestPF_I01…I28
// and the further pinned rows). They drive the real TopologyManagers, the
// production PlannedFailoverTopology facade over a test ManagerRegistry,
// the real PlannedFailoverEngine and an in-memory PlannedFailoverStore with
// scripted faults; rows 1–3 and 9 also run the production UpdateController
// with its old manager retired while blocked inside a statement. The
// checklist of every row, the package each test lives in and the symbols it
// needs is .tmp/megamind-pf-claim/wp3-test-checklist.md.
//
// The tests below are the mock fidelity those rows rest on: a pinned
// session that cannot follow a replaced pod, and calls that block inside a
// statement. They use only symbols that exist before the engine and the
// claim land.

import (
	"context"
	"errors"
	"log/slog"
	"testing"
	"time"

	"github.com/shipstream/bloodraven/internal/controller"
	"github.com/shipstream/bloodraven/internal/mysql"
)

const (
	pfSourceUUID = "3e11fa47-71ca-11e1-9e33-c80aa9429562"
	pfTargetUUID = "4f22ab58-82db-22f2-af44-d91bb0530673"
	pfFenced     = pfSourceUUID + ":1-100"
)

// TestPinnedMockSession_FailsAfterReplacementBetweenProofAndWrite is the
// component form of the DST fidelity test of the same name: pin, prove,
// replace the pod, and the write fails without reaching either server.
func TestPinnedMockSession_FailsAfterReplacementBetweenProofAndWrite(t *testing.T) {
	ctx := context.Background()
	target := &mockMySQL{readOnly: true, superReadOnly: true, gtidExecuted: pfFenced, serverUUID: pfTargetUUID}
	sess, err := target.PinSession(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if snap, err := sess.ReadGTIDSnapshot(ctx); err != nil || snap.ServerUUID != pfTargetUUID || snap.GtidExecuted != pfFenced {
		t.Fatalf("proof: %+v %v", snap, err)
	}

	regressed := pfSourceUUID + ":1-60"
	target.replacePod(podReplacement{serverUUID: "5a33bc69-93ec-33a3-b055-ea2cc1641784", gtidExecuted: &regressed})

	if err := sess.SetSuperReadOnly(ctx, false); !errors.Is(err, errMockSessionLost) {
		t.Fatalf("SET super_read_only after replacement: %v", err)
	}
	if err := sess.SetReadOnly(ctx, false); !errors.Is(err, errMockSessionLost) {
		t.Fatalf("SET read_only after replacement: %v", err)
	}
	if target.writableGrants() != 0 || !target.isReadOnly() || target.isSuperReadOnly() {
		t.Fatalf("the new pod was changed or did not boot read_only=ON super_read_only=OFF (grants %d)", target.writableGrants())
	}
	// The pool reaches the new pod; a new pin is on it.
	if got, _ := target.GetGtidExecuted(ctx); got != regressed {
		t.Fatalf("pooled read %q, want the new pod's %q", got, regressed)
	}
	fresh, _ := target.PinSession(ctx)
	if snap, err := fresh.ReadGTIDSnapshot(ctx); err != nil || snap.ServerUUID == pfTargetUUID {
		t.Fatalf("fresh pin: %+v %v", snap, err)
	}
	if err := fresh.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := fresh.CheckReadOnly(ctx); !errors.Is(err, errMockSessionClosed) {
		t.Fatalf("after Close: %v", err)
	}
	// A restart is a new incarnation too.
	again, _ := target.PinSession(ctx)
	target.respawn(true)
	if err := again.StopReplica(ctx); !errors.Is(err, errMockSessionLost) {
		t.Fatalf("after respawn: %v", err)
	}
}

// TestMockBlockOn_HoldsInsideStatement: a blocked call is inside the
// statement while other actors act; a replacement before apply keeps it from
// applying, after apply it landed and its reply is lost.
func TestMockBlockOn_HoldsInsideStatement(t *testing.T) {
	ctx := context.Background()
	t.Run("beforeApply", func(t *testing.T) {
		m := &mockMySQL{readOnly: true, superReadOnly: true}
		sess, _ := m.PinSession(ctx)
		b := m.blockOn(t, "SetSuperReadOnly", false)
		done := make(chan error, 1)
		go func() { done <- sess.SetSuperReadOnly(ctx, false) }()
		<-b.Entered()
		// The mock is not locked while the call is held.
		if !m.isSuperReadOnly() {
			t.Fatal("applied before release")
		}
		m.replacePod(podReplacement{})
		b.Release()
		if err := <-done; !errors.Is(err, errMockSessionLost) {
			t.Fatalf("err %v, want errMockSessionLost", err)
		}
		if m.writableGrants() != 0 {
			t.Fatal("a statement lost before apply applied")
		}
	})
	t.Run("afterApply", func(t *testing.T) {
		m := &mockMySQL{readOnly: true, superReadOnly: true}
		sess, _ := m.PinSession(ctx)
		b := m.blockOn(t, "SetReadOnly", true)
		done := make(chan error, 1)
		go func() { done <- sess.SetReadOnly(ctx, false) }()
		<-b.Entered()
		if m.isReadOnly() {
			t.Fatal("not applied while held after apply")
		}
		m.replacePod(podReplacement{})
		b.Release()
		if err := <-done; !errors.Is(err, errMockSessionLost) {
			t.Fatalf("err %v, want errMockSessionLost (applied, reply lost)", err)
		}
	})
	t.Run("failNextAndCounts", func(t *testing.T) {
		m := &mockMySQL{clearChannelOnReset: true, replicaStatus: &mysql.ReplicaStatus{IORunning: true, SQLRunning: true}}
		m.failNextCalls("ResetReplicaAll", 1)
		if err := m.ResetReplicaAll(ctx); !errors.Is(err, errMockStatementFailed) {
			t.Fatalf("err %v", err)
		}
		if err := m.ResetReplicaAll(ctx); err != nil {
			t.Fatal(err)
		}
		if rs, _ := m.ShowReplicaStatus(ctx); rs != nil || m.callCount("ResetReplicaAll") != 2 {
			t.Fatalf("channel %+v after RESET REPLICA ALL, calls %d", rs, m.callCount("ResetReplicaAll"))
		}
		if got := gtidWithExtras(pfFenced, pfTargetUUID, 1, 3); got != pfFenced+","+pfTargetUUID+":1-3" {
			t.Fatalf("gtidWithExtras %q", got)
		}
		if snap, err := withServerUUID(m, pfTargetUUID).ReadGTIDSnapshot(ctx); err != nil || snap.ServerUUID != pfTargetUUID {
			t.Fatalf("pooled snapshot %+v %v", snap, err)
		}
	})
}

// TestHandoff_PodReplacedBetweenProofAndWriteNeverWrites runs the production
// maintenance handoff with the candidate's pod replaced while its first
// write after the GTID proofs (SET super_read_only=OFF on the pinned
// session) is in flight: the promotion stops, nothing reaches the new pod,
// and the source gets its writes back.
func TestHandoff_PodReplacedBetweenProofAndWriteNeverWrites(t *testing.T) {
	ctx := context.Background()
	source := &mockMySQL{gtidExecuted: pfFenced, serverUUID: pfSourceUUID}
	candidate := &mockMySQL{
		readOnly: true, superReadOnly: true, gtidExecuted: pfFenced, serverUUID: pfTargetUUID,
		replicaStatus: &mysql.ReplicaStatus{IORunning: true, SQLRunning: true, SourceHost: "source"},
	}
	held := candidate.blockOn(t, "SetSuperReadOnly", false)

	fc := controller.NewFailoverController(slog.New(slog.DiscardHandler))
	done := make(chan error, 1)
	go func() {
		_, err := fc.ExecuteHandoff(ctx, candidate, source, "candidate", controller.HandoffOptions{
			PreFenceCatchUp: 2 * time.Second, CatchUp: 2 * time.Second, PollInterval: time.Millisecond,
		})
		done <- err
	}()
	select {
	case <-held.Entered():
	case err := <-done:
		t.Fatalf("handoff ended before the candidate write: %v", err)
	case <-time.After(10 * time.Second):
		t.Fatal("the handoff never reached the candidate write")
	}
	regressed := pfSourceUUID + ":1-60"
	candidate.replacePod(podReplacement{serverUUID: "5a33bc69-93ec-33a3-b055-ea2cc1641784", gtidExecuted: &regressed})
	held.Release()

	if err := <-done; err == nil {
		t.Fatal("handoff succeeded onto a replaced pod")
	}
	if candidate.writableGrants() != 0 || !candidate.isReadOnly() {
		t.Fatalf("the replaced pod was made writable (grants %d)", candidate.writableGrants())
	}
	if source.isReadOnly() || source.isSuperReadOnly() {
		t.Fatal("source left fenced although nothing was written to the candidate")
	}
}
