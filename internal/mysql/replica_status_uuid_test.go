package mysql_test

import (
	"context"
	"testing"

	"github.com/shipstream/bloodraven/internal/testutil"
)

// Source_UUID identifies the source server the receiver last connected
// to; the replica GTID audit uses a change in it to notice a primary
// replaced under the same host name.
func TestShowReplicaStatusReportsSourceUUID(t *testing.T) {
	const uuid = "d07c0c83-71ca-11f0-9e33-c80aa9429562"
	r := testutil.NewFakeReplica(testutil.FakeReplicaState{Configured: true, IORunning: true, SQLRunning: true,
		SourceHost: "primary", SourceUUID: uuid, ReadOnly: true})
	rs, err := r.Checker().ShowReplicaStatus(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if rs.SourceUUID != uuid {
		t.Fatalf("SourceUUID = %q, want %q", rs.SourceUUID, uuid)
	}
}
