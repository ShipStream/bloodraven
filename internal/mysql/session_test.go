package mysql_test

import (
	"context"
	"testing"

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
