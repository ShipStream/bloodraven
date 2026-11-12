package mysql_test

import (
	"context"
	"errors"
	"strings"
	"testing"

	mysqldriver "github.com/go-sql-driver/mysql"

	"github.com/shipstream/bloodraven/internal/mysql"
	"github.com/shipstream/bloodraven/internal/testutil"
)

type binlogPositionReader interface {
	ReadBinlogPosition(ctx context.Context) (mysql.BinlogPosition, error)
}

// The checker reads the binary log coordinates with the executed set they
// belong to, in one statement, and the applier's position from SHOW
// REPLICA STATUS: the two the replica GTID audit compares to tell a
// replica that is behind from one whose receiver dropped transactions.
func TestReadBinlogPosition(t *testing.T) {
	ctx := context.Background()
	r := testutil.NewFakeReplica(testutil.FakeReplicaState{ExecutedGtidSet: "uuid1:1-100", BinlogPosition: 5000})
	reader, ok := r.Checker().(binlogPositionReader)
	if !ok {
		t.Fatal("the production checker must read the binary log position")
	}
	pos, err := reader.ReadBinlogPosition(ctx)
	if err != nil || pos.File != "binlog.000001" || pos.Pos != 5000 || pos.GtidExecuted != "uuid1:1-100" {
		t.Fatalf("ReadBinlogPosition = %+v, %v", pos, err)
	}

	denied := errors.New("Error 1227 (42000): Access denied; you need (at least one of) the SUPER, REPLICATION CLIENT privilege(s) for this operation")
	r = testutil.NewFakeReplica(testutil.FakeReplicaState{ExecutedGtidSet: "uuid1:1-100", BinlogStatusErr: denied})
	if _, err := r.Checker().(binlogPositionReader).ReadBinlogPosition(ctx); !errors.Is(err, denied) {
		t.Fatalf("ReadBinlogPosition error = %v, want the statement's", err)
	}

	// Only an error that says the server will never report a position is
	// marked unavailable: the account may not run the statement (1227), or
	// the server has no binary log (no row). A statement that failed for
	// any other reason may succeed the next time, and a caller that took it
	// for "no position" would stop telling replication lag from a loss.
	if errors.Is(err, mysql.ErrBinlogPositionUnavailable) {
		t.Fatalf("a successful read reports %v", err)
	}
	for _, tc := range []struct {
		name        string
		state       testutil.FakeReplicaState
		unavailable bool
	}{
		{"error 1227", testutil.FakeReplicaState{BinlogStatusErr: &mysqldriver.MySQLError{Number: 1227,
			Message: "Access denied; you need (at least one of) the SUPER, REPLICATION CLIENT privilege(s) for this operation"}}, true},
		{"no row", testutil.FakeReplicaState{BinlogOff: true}, true},
		{"another server error", testutil.FakeReplicaState{BinlogStatusErr: &mysqldriver.MySQLError{Number: 1053, Message: "Server shutdown in progress"}}, false},
		{"a timeout", testutil.FakeReplicaState{BinlogStatusErr: context.DeadlineExceeded}, false},
		{"a lost connection", testutil.FakeReplicaState{BinlogStatusErr: errors.New("invalid connection")}, false},
		{"a text that only looks like 1227", testutil.FakeReplicaState{BinlogStatusErr: denied}, false},
	} {
		tc.state.ExecutedGtidSet = "uuid1:1-100"
		_, err := testutil.NewFakeReplica(tc.state).Checker().(binlogPositionReader).ReadBinlogPosition(ctx)
		if err == nil || errors.Is(err, mysql.ErrBinlogPositionUnavailable) != tc.unavailable {
			t.Fatalf("%s: ReadBinlogPosition error = %v, want unavailable %v", tc.name, err, tc.unavailable)
		}
	}

	// A position that does not parse is an error, never position 0: a
	// snapshot at 0 would count as passed by every applier.
	r = testutil.NewFakeReplica(testutil.FakeReplicaState{ExecutedGtidSet: "uuid1:1-100", BinlogPositionRaw: "not-a-number"})
	if pos, err := r.Checker().(binlogPositionReader).ReadBinlogPosition(ctx); err == nil || !strings.Contains(err.Error(), "position") {
		t.Fatalf("ReadBinlogPosition = %+v, %v; want an error for an unreadable position", pos, err)
	}
}

func TestShowReplicaStatus_ExecutedPosition(t *testing.T) {
	ctx := context.Background()
	r := testutil.NewFakeReplica(testutil.FakeReplicaState{Configured: true, IORunning: true, SQLRunning: true, SourceHost: "primary",
		ExecSourceLogFile: "binlog.000007", ExecSourceLogPos: 4711})
	rs, err := r.Checker().ShowReplicaStatus(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if rs.ExecSourceLogFile != "binlog.000007" || rs.ExecSourceLogPos != 4711 {
		t.Fatalf("executed position = %s:%d, want binlog.000007:4711", rs.ExecSourceLogFile, rs.ExecSourceLogPos)
	}
	if !rs.ExecutedPast(mysql.BinlogPosition{File: "binlog.000007", Pos: 4711}) || rs.ExecutedPast(mysql.BinlogPosition{File: "binlog.000007", Pos: 4712}) {
		t.Fatal("ExecutedPast disagrees with the reported position")
	}

	// A server that reports no file reports no position.
	r = testutil.NewFakeReplica(testutil.FakeReplicaState{Configured: true, IORunning: true, SQLRunning: true, SourceHost: "primary"})
	if rs, err = r.Checker().ShowReplicaStatus(ctx); err != nil || rs.ExecSourceLogFile != "" || rs.ExecutedPast(mysql.BinlogPosition{File: "binlog.000001", Pos: 4}) {
		t.Fatalf("status = %+v, %v; want no executed position", rs, err)
	}
}
