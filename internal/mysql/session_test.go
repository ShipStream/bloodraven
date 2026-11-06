package mysql_test

import (
	"context"
	"errors"
	"slices"
	"testing"

	"github.com/shipstream/bloodraven/internal/mysql"
	"github.com/shipstream/bloodraven/internal/testutil"
)

// A pinned Session stays on its server: after the address moves to another
// server, the pool reaches the new one but the Session fails instead of
// following it.
func TestPinSession_DoesNotFollowAnEndpointSwitch(t *testing.T) {
	first := testutil.NewFakeReplica(testutil.FakeReplicaState{ExecutedGtidSet: "uuid1:1-100", ReadOnly: true})
	second := testutil.NewFakeReplica(testutil.FakeReplicaState{ExecutedGtidSet: "uuid1:1-90", ReadOnly: true})
	endpoint := testutil.NewFakeEndpoint(first)
	c := endpoint.Checker()
	defer c.Close()
	ctx := context.Background()

	sess, err := c.PinSession(ctx)
	if err != nil {
		t.Fatalf("PinSession: %v", err)
	}
	defer sess.Close()
	if got, err := sess.GetGtidExecuted(ctx); err != nil || got != "uuid1:1-100" {
		t.Fatalf("session GTID = %q, %v; want the first server's", got, err)
	}

	endpoint.Switch(second)

	if got, err := c.GetGtidExecuted(ctx); err != nil || got != "uuid1:1-90" {
		t.Fatalf("pooled GTID after switch = %q, %v; want the pool to reach the second server", got, err)
	}
	if got, err := sess.GetGtidExecuted(ctx); err == nil {
		t.Fatalf("session read %q after the switch; want an error, not the other server", got)
	}
	if err := sess.SetReadOnly(ctx, false); err == nil {
		t.Fatal("session write succeeded after the switch")
	}
	if !second.State().ReadOnly {
		t.Fatal("the second server was made writable through the pinned session")
	}
}

// Every Session method runs its statement on the pinned connection and
// reads the pinned server's state.
func TestSession_MethodsRunOnThePinnedServer(t *testing.T) {
	replica := testutil.NewFakeReplica(testutil.FakeReplicaState{
		Configured: true, IORunning: true, SQLRunning: true,
		SourceHost: "iad", ExecutedGtidSet: "uuid1:1-100", RetrievedGtidSet: "uuid1:1-100",
		ReadOnly: true, SuperReadOnly: true, ServerUUID: "server-a",
	})
	c := replica.Checker()
	defer c.Close()
	ctx := context.Background()

	sess, err := c.PinSession(ctx)
	if err != nil {
		t.Fatalf("PinSession: %v", err)
	}
	defer sess.Close()

	if ro, err := sess.CheckReadOnly(ctx); err != nil || !ro {
		t.Fatalf("CheckReadOnly = %v, %v; want true", ro, err)
	}
	if sro, err := sess.CheckSuperReadOnly(ctx); err != nil || !sro {
		t.Fatalf("CheckSuperReadOnly = %v, %v; want true", sro, err)
	}
	snap, err := sess.ReadGTIDSnapshot(ctx)
	if err != nil || snap != (mysql.GTIDSnapshot{ServerUUID: "server-a", GtidExecuted: "uuid1:1-100"}) {
		t.Fatalf("ReadGTIDSnapshot = %+v, %v", snap, err)
	}
	rs, err := sess.ShowReplicaStatus(ctx)
	if err != nil || rs == nil || !rs.IORunning || !rs.SQLRunning || rs.SourceHost != "iad" {
		t.Fatalf("ShowReplicaStatus = %+v, %v; want the running channel from iad", rs, err)
	}
	if err := sess.StopReplica(ctx); err != nil {
		t.Fatalf("StopReplica: %v", err)
	}
	if err := sess.ChangeReplicationSource(ctx, mysql.ReplicationSourceOpts{Host: "pdx", User: "repl", Password: "pw"}); err != nil {
		t.Fatalf("ChangeReplicationSource: %v", err)
	}
	if err := sess.StartReplica(ctx); err != nil {
		t.Fatalf("StartReplica: %v", err)
	}
	if st := replica.State(); st.SourceHost != "pdx" || !st.IORunning || !st.SQLRunning {
		t.Fatalf("replica after re-point = %+v; want running from pdx", st)
	}
	if err := sess.StopReplica(ctx); err != nil {
		t.Fatalf("StopReplica: %v", err)
	}
	if err := sess.ResetReplicaAll(ctx); err != nil {
		t.Fatalf("ResetReplicaAll: %v", err)
	}
	if rs, err := sess.ShowReplicaStatus(ctx); err != nil || rs != nil {
		t.Fatalf("ShowReplicaStatus after RESET REPLICA ALL = %+v, %v; want nil, nil", rs, err)
	}
	if err := sess.SetSuperReadOnly(ctx, false); err != nil {
		t.Fatalf("SetSuperReadOnly: %v", err)
	}
	if sro, err := sess.CheckSuperReadOnly(ctx); err != nil || sro {
		t.Fatalf("CheckSuperReadOnly after clearing = %v, %v; want false", sro, err)
	}

	want := []string{
		"SELECT @@read_only",
		"SELECT @@super_read_only",
		"SELECT @@server_uuid, @@global.gtid_executed",
		"SHOW REPLICA STATUS",
		"STOP REPLICA",
		"CHANGE REPLICATION SOURCE TO SOURCE_HOST='pdx', SOURCE_USER='repl', SOURCE_PASSWORD='pw', SOURCE_AUTO_POSITION=1, GET_SOURCE_PUBLIC_KEY=1",
		"START REPLICA",
		"STOP REPLICA",
		"RESET REPLICA ALL",
		"SHOW REPLICA STATUS",
		"SET GLOBAL super_read_only = OFF",
		"SELECT @@super_read_only",
	}
	if got := replica.Statements(); !slices.Equal(got, want) {
		t.Fatalf("statements:\n got %q\nwant %q", got, want)
	}
}

// After the address moves to another server, every new Session method
// fails rather than reading or changing the other server.
func TestSession_NewMethodsDoNotFollowAnEndpointSwitch(t *testing.T) {
	first := testutil.NewFakeReplica(testutil.FakeReplicaState{ExecutedGtidSet: "uuid1:1-100", ServerUUID: "server-a", ReadOnly: true, SuperReadOnly: true})
	second := testutil.NewFakeReplica(testutil.FakeReplicaState{ExecutedGtidSet: "uuid1:1-90", ServerUUID: "server-b", ReadOnly: true})
	endpoint := testutil.NewFakeEndpoint(first)
	c := endpoint.Checker()
	defer c.Close()
	ctx := context.Background()

	sess, err := c.PinSession(ctx)
	if err != nil {
		t.Fatalf("PinSession: %v", err)
	}
	defer sess.Close()
	if snap, err := sess.ReadGTIDSnapshot(ctx); err != nil || snap.ServerUUID != "server-a" {
		t.Fatalf("ReadGTIDSnapshot = %+v, %v; want server-a", snap, err)
	}

	endpoint.Switch(second)

	calls := map[string]func() error{
		"CheckReadOnly":      func() error { _, err := sess.CheckReadOnly(ctx); return err },
		"CheckSuperReadOnly": func() error { _, err := sess.CheckSuperReadOnly(ctx); return err },
		"ReadGTIDSnapshot":   func() error { _, err := sess.ReadGTIDSnapshot(ctx); return err },
		"ShowReplicaStatus":  func() error { _, err := sess.ShowReplicaStatus(ctx); return err },
		"StartReplica":       func() error { return sess.StartReplica(ctx) },
		"ChangeReplicationSource": func() error {
			return sess.ChangeReplicationSource(ctx, mysql.ReplicationSourceOpts{Host: "iad"})
		},
	}
	for name, call := range calls {
		if err := call(); err == nil {
			t.Errorf("%s succeeded after the switch; want an error, not the other server", name)
		}
	}
	if got := second.Statements(); len(got) != 0 {
		t.Fatalf("the second server received %q through the pinned session", got)
	}
}

// bareChecker is a Checker without CheckSuperReadOnly or ReadGTIDSnapshot.
type bareChecker struct{ mysql.Checker }

// A CheckerSession over a Checker that lacks a method reports
// ErrSessionUnsupported instead of guessing; one whose Checker has it
// delegates.
func TestCheckerSession_Unsupported(t *testing.T) {
	ctx := context.Background()
	bare := mysql.CheckerSession(bareChecker{})
	if _, err := bare.CheckSuperReadOnly(ctx); !errors.Is(err, mysql.ErrSessionUnsupported) {
		t.Fatalf("CheckSuperReadOnly error = %v; want ErrSessionUnsupported", err)
	}
	if _, err := bare.ReadGTIDSnapshot(ctx); !errors.Is(err, mysql.ErrSessionUnsupported) {
		t.Fatalf("ReadGTIDSnapshot error = %v; want ErrSessionUnsupported", err)
	}

	replica := testutil.NewFakeReplica(testutil.FakeReplicaState{ExecutedGtidSet: "uuid1:1-5", ServerUUID: "server-a", SuperReadOnly: true, ReadOnly: true})
	c := replica.Checker()
	defer c.Close()
	full := mysql.CheckerSession(c)
	if sro, err := full.CheckSuperReadOnly(ctx); err != nil || !sro {
		t.Fatalf("CheckSuperReadOnly = %v, %v; want true", sro, err)
	}
	if snap, err := full.ReadGTIDSnapshot(ctx); err != nil || snap.ServerUUID != "server-a" || snap.GtidExecuted != "uuid1:1-5" {
		t.Fatalf("ReadGTIDSnapshot = %+v, %v", snap, err)
	}
}
