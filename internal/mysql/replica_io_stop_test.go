package mysql_test

import (
	"context"
	"slices"
	"testing"

	"github.com/shipstream/bloodraven/internal/mysql"
	"github.com/shipstream/bloodraven/internal/testutil"
)

// A receiver retrying its source reports Connecting: not running, but it
// would stream events as soon as it connects.
func TestShowReplicaStatusReportsConnectingReceiver(t *testing.T) {
	r := testutil.NewFakeReplica(testutil.FakeReplicaState{Configured: true, IOConnecting: true, SQLRunning: true, SourceHost: "primary"})
	rs, err := r.Checker().ShowReplicaStatus(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if rs.IORunning || !rs.IOConnecting {
		t.Fatalf("IORunning=%v IOConnecting=%v, want a connecting receiver", rs.IORunning, rs.IOConnecting)
	}
}

// STOP REPLICA IO_THREAD stops only the receiver: the applier keeps
// applying the relay log already received.
func TestStopReplicaIOThreadLeavesApplierRunning(t *testing.T) {
	for _, connecting := range []bool{false, true} {
		r := testutil.NewFakeReplica(testutil.FakeReplicaState{Configured: true, IORunning: !connecting, IOConnecting: connecting,
			SQLRunning: true, SourceHost: "primary"})
		stopper, ok := r.Checker().(mysql.ReplicaIOStopper)
		if !ok {
			t.Fatal("the production checker must implement mysql.ReplicaIOStopper")
		}
		if err := stopper.StopReplicaIOThread(context.Background()); err != nil {
			t.Fatal(err)
		}
		st := r.State()
		if st.IORunning || st.IOConnecting || !st.SQLRunning {
			t.Fatalf("connecting=%v: state %+v, want the receiver stopped and the applier running", connecting, st)
		}
		if !slices.Contains(r.Statements(), "STOP REPLICA IO_THREAD") {
			t.Fatalf("statements %v", r.Statements())
		}
	}
}

func TestMissingReplicaIOStopPrivileges(t *testing.T) {
	for _, tc := range []struct {
		name   string
		grants []string
		want   []string
	}{
		{"all privileges", nil, nil},
		{"replication admin", []string{"GRANT REPLICATION_SLAVE_ADMIN ON *.* TO `op`@`%`"}, nil},
		{"super", []string{"GRANT SELECT, SUPER ON *.* TO `op`@`%`"}, nil},
		{"missing", []string{"GRANT SELECT, PROCESS ON *.* TO `op`@`%`"}, []string{"REPLICATION_SLAVE_ADMIN"}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			r := testutil.NewFakeReplica(testutil.FakeReplicaState{Grants: tc.grants})
			got, err := r.Checker().(mysql.ReplicaIOStopper).MissingReplicaIOStopPrivileges(context.Background())
			if err != nil {
				t.Fatal(err)
			}
			if !slices.Equal(got, tc.want) {
				t.Fatalf("missing = %v, want %v", got, tc.want)
			}
		})
	}
}
