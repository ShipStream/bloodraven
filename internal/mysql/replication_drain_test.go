package mysql_test

import (
	"context"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/shipstream/bloodraven/internal/testutil"
)

// connectErr is what a replica's receiver reports once its source is gone.
const connectErr = "Error reconnecting to source 'replicator@mysql-dc1:3306' - retry-time: 60 retries: 1 message: Can't connect to MySQL server on 'mysql-dc1:3306' (111)"

func int64p(v int64) *int64 { return &v }

// These drive the real checker through a fake database/sql driver, so the
// SHOW REPLICA STATUS column parsing and the drain loop are both under test.
func TestWaitForRelayLogDrain(t *testing.T) {
	tests := []struct {
		name    string
		state   testutil.FakeReplicaState
		onPoll  func(poll int, s *testutil.FakeReplicaState)
		timeout time.Duration
		// wantErr is a substring of the expected error; empty means nil.
		wantErr     string
		wantPolls   int // exact poll count; 0 skips the check
		minPolls    int
		wantRestart int // START REPLICA SQL_THREAD statements
		maxElapsed  time.Duration
	}{
		{
			// The live data-loss case: the primary is dead, the receiver
			// records a connect error, and the applier is still working
			// through a delayed backlog. The drain must wait for it.
			name: "io error with pending relay log keeps waiting until applied",
			state: testutil.FakeReplicaState{
				Configured: true, IOConnecting: true, SQLRunning: true,
				LastIOErrno: 2003, LastIOError: connectErr,
				RetrievedGtidSet: "uuid1:1-603", ExecutedGtidSet: "uuid1:1-542",
			},
			onPoll: func(poll int, s *testutil.FakeReplicaState) {
				if poll >= 3 {
					s.ExecutedGtidSet = s.RetrievedGtidSet
				}
			},
			timeout:   30 * time.Second,
			wantPolls: 3,
		},
		{
			// Seconds_Behind_Source is NULL with the receiver down; the
			// drain must not wait for it to read 0.
			name: "io error with nothing pending returns promptly",
			state: testutil.FakeReplicaState{
				Configured: true, IOConnecting: true, SQLRunning: true,
				LastIOErrno: 2003, LastIOError: connectErr,
				RetrievedGtidSet: "uuid1:1-603", ExecutedGtidSet: "uuid1:1-603",
			},
			timeout:    30 * time.Second,
			wantPolls:  1,
			maxElapsed: 400 * time.Millisecond,
		},
		{
			name: "io error with stopped sql thread restarts it and drains",
			state: testutil.FakeReplicaState{
				Configured: true, IOConnecting: true,
				LastIOErrno: 2003, LastIOError: connectErr,
				RetrievedGtidSet: "uuid1:1-20", ExecutedGtidSet: "uuid1:1-10",
			},
			onPoll: func(_ int, s *testutil.FakeReplicaState) {
				if s.SQLRunning {
					s.ExecutedGtidSet = s.RetrievedGtidSet
				}
			},
			timeout:     30 * time.Second,
			wantPolls:   2,
			wantRestart: 1,
		},
		{
			name: "sql thread error aborts",
			state: testutil.FakeReplicaState{
				Configured: true, IOConnecting: true,
				LastIOErrno: 2003, LastIOError: connectErr,
				LastSQLErrno: 1062, LastSQLError: "Error 'Duplicate entry' on query",
				RetrievedGtidSet: "uuid1:1-20", ExecutedGtidSet: "uuid1:1-10",
			},
			timeout:   30 * time.Second,
			wantErr:   "relay log drain aborted: SQL thread error: 1062: Error 'Duplicate entry'",
			wantPolls: 1,
		},
		{
			// Seconds_Behind_Source has one-second granularity and measures
			// event age, not queue depth: 0 with a backlog is not drained.
			name: "sbs zero with retrieved not executed is not drained",
			state: testutil.FakeReplicaState{
				Configured: true, IORunning: true, SQLRunning: true,
				SecondsBehindSource: int64p(0),
				RetrievedGtidSet:    "uuid1:1-20", ExecutedGtidSet: "uuid1:1-10",
			},
			onPoll: func(poll int, s *testutil.FakeReplicaState) {
				if poll >= 2 {
					s.ExecutedGtidSet = s.RetrievedGtidSet
				}
			},
			timeout:   30 * time.Second,
			wantPolls: 2,
		},
		{
			name: "sbs zero with a backlog that never applies times out",
			state: testutil.FakeReplicaState{
				Configured: true, IORunning: true, SQLRunning: true,
				SecondsBehindSource: int64p(0),
				RetrievedGtidSet:    "uuid1:1-20", ExecutedGtidSet: "uuid1:1-10",
			},
			timeout:  100 * time.Millisecond,
			wantErr:  "relay log drain timed out after 100ms with 10 relay log transactions unapplied (uuid1:11-20)",
			minPolls: 2,
		},
		{
			// Operator-only partition: the receiver is connected and keeps
			// fetching, the applier stays one poll behind. The drain ends
			// once what was retrieved when it began is applied.
			name: "receiver still fetching terminates once the initial backlog is applied",
			state: testutil.FakeReplicaState{
				Configured: true, IORunning: true, SQLRunning: true,
				SecondsBehindSource: int64p(1),
				RetrievedGtidSet:    "uuid1:1-20", ExecutedGtidSet: "uuid1:1-10",
			},
			onPoll: func(poll int, s *testutil.FakeReplicaState) {
				if poll == 1 {
					return
				}
				s.ExecutedGtidSet = s.RetrievedGtidSet
				s.RetrievedGtidSet = "uuid1:1-" + strconv.Itoa(20+10*poll)
			},
			timeout:   30 * time.Second,
			wantPolls: 2,
		},
		{
			// MySQL 8.4+ prints tagged transactions in the compact
			// uuid:intervals:tag:intervals form. The pending tagged
			// transaction (app:4-5) must be waited for, not rejected.
			name: "compact tagged gtid sets wait for the pending tagged backlog",
			state: testutil.FakeReplicaState{
				Configured: true, IOConnecting: true, SQLRunning: true,
				LastIOErrno: 2003, LastIOError: connectErr,
				RetrievedGtidSet: "3e11fa47-71ca-11e1-9e33-c80aa9429562:1-20:app:1-5",
				ExecutedGtidSet:  "3e11fa47-71ca-11e1-9e33-c80aa9429562:1-20:app:1-3",
			},
			onPoll: func(poll int, s *testutil.FakeReplicaState) {
				if poll >= 2 {
					s.ExecutedGtidSet = s.RetrievedGtidSet
				}
			},
			timeout:   30 * time.Second,
			wantPolls: 2,
		},
		{
			name:      "no replication configured returns nil",
			state:     testutil.FakeReplicaState{},
			timeout:   30 * time.Second,
			wantPolls: 1,
		},
		{
			name: "empty retrieved set has nothing to drain",
			state: testutil.FakeReplicaState{
				Configured: true, IOConnecting: true, SQLRunning: true,
				LastIOErrno: 2003, LastIOError: connectErr,
				ExecutedGtidSet: "uuid1:1-10",
			},
			timeout:   30 * time.Second,
			wantPolls: 1,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			fake := testutil.NewFakeReplica(tt.state)
			if tt.onPoll != nil {
				fake.OnPoll(tt.onPoll)
			}
			c := fake.Checker()
			defer c.Close()

			start := time.Now()
			err := c.WaitForRelayLogDrain(context.Background(), tt.timeout)
			elapsed := time.Since(start)

			switch {
			case tt.wantErr == "" && err != nil:
				t.Fatalf("WaitForRelayLogDrain() error = %v, want nil", err)
			case tt.wantErr != "" && err == nil:
				t.Fatalf("WaitForRelayLogDrain() = nil, want error containing %q", tt.wantErr)
			case tt.wantErr != "" && !strings.Contains(err.Error(), tt.wantErr):
				t.Fatalf("WaitForRelayLogDrain() error = %q, want it to contain %q", err, tt.wantErr)
			}
			if polls := fake.Polls(); tt.wantPolls != 0 && polls != tt.wantPolls {
				t.Errorf("SHOW REPLICA STATUS polls = %d, want %d", polls, tt.wantPolls)
			}
			if polls := fake.Polls(); polls < tt.minPolls {
				t.Errorf("SHOW REPLICA STATUS polls = %d, want at least %d", polls, tt.minPolls)
			}
			restarts := 0
			for _, s := range fake.Statements() {
				if s == "START REPLICA SQL_THREAD" {
					restarts++
				}
			}
			if restarts != tt.wantRestart {
				t.Errorf("START REPLICA SQL_THREAD issued %d times, want %d", restarts, tt.wantRestart)
			}
			if tt.maxElapsed != 0 && elapsed > tt.maxElapsed {
				t.Errorf("drain took %s, want at most %s", elapsed, tt.maxElapsed)
			}
		})
	}
}

// The timeout error names the receiver's error so a log reader can tell a
// dead source from a slow applier.
func TestWaitForRelayLogDrain_TimeoutReportsIOError(t *testing.T) {
	fake := testutil.NewFakeReplica(testutil.FakeReplicaState{
		Configured: true, IOConnecting: true, SQLRunning: true,
		LastIOErrno: 2003, LastIOError: connectErr,
		RetrievedGtidSet: "uuid1:1-20", ExecutedGtidSet: "uuid1:1-10",
	})
	c := fake.Checker()
	defer c.Close()
	err := c.WaitForRelayLogDrain(context.Background(), 50*time.Millisecond)
	if err == nil || !strings.Contains(err.Error(), "IO thread error: 2003: "+connectErr) {
		t.Fatalf("WaitForRelayLogDrain() error = %v, want the IO thread error appended", err)
	}
}

func TestShowReplicaStatus_SeparatesIOAndSQLErrors(t *testing.T) {
	tests := []struct {
		name          string
		state         testutil.FakeReplicaState
		wantLastError string
		wantIOErrno   int
		wantIOError   string
		wantSQLErrno  int
		wantSQLError  string
	}{
		{
			name: "receiver error only",
			state: testutil.FakeReplicaState{
				Configured: true, IOConnecting: true, SQLRunning: true,
				LastIOErrno: 2003, LastIOError: connectErr,
			},
			wantLastError: connectErr,
			wantIOErrno:   2003, wantIOError: connectErr,
		},
		{
			name: "applier error only",
			state: testutil.FakeReplicaState{
				Configured: true, IORunning: true,
				LastSQLErrno: 1062, LastSQLError: "dup",
			},
			wantLastError: "dup",
			wantSQLErrno:  1062, wantSQLError: "dup",
		},
		{
			// LastError keeps its historical precedence: the applier error
			// (Last_Error) wins over the receiver error.
			name: "both errors",
			state: testutil.FakeReplicaState{
				Configured:  true,
				LastIOErrno: 2003, LastIOError: connectErr,
				LastSQLErrno: 1062, LastSQLError: "dup",
			},
			wantLastError: "dup",
			wantIOErrno:   2003, wantIOError: connectErr,
			wantSQLErrno: 1062, wantSQLError: "dup",
		},
		{
			name:  "no errors",
			state: testutil.FakeReplicaState{Configured: true, IORunning: true, SQLRunning: true},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := testutil.NewFakeReplica(tt.state).Checker()
			defer c.Close()
			rs, err := c.ShowReplicaStatus(context.Background())
			if err != nil {
				t.Fatalf("ShowReplicaStatus() error = %v", err)
			}
			if rs.LastError != tt.wantLastError {
				t.Errorf("LastError = %q, want %q", rs.LastError, tt.wantLastError)
			}
			if rs.LastIOErrno != tt.wantIOErrno || rs.LastIOError != tt.wantIOError {
				t.Errorf("IO error = %d %q, want %d %q", rs.LastIOErrno, rs.LastIOError, tt.wantIOErrno, tt.wantIOError)
			}
			if rs.LastSQLErrno != tt.wantSQLErrno || rs.LastSQLError != tt.wantSQLError {
				t.Errorf("SQL error = %d %q, want %d %q", rs.LastSQLErrno, rs.LastSQLError, tt.wantSQLErrno, tt.wantSQLError)
			}
		})
	}
}
