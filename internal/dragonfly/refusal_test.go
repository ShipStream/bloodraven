package dragonfly

import (
	"context"
	"errors"
	"fmt"
	"io"
	"strings"
	"testing"
)

// The replies are the ones Dragonfly v1.38.0 and v2.1 gave (ACL user
// without the command, a missing or renamed command, a subcommand the
// server does not have, the role changing under a replica command).
func TestIsCommandRefused(t *testing.T) {
	for _, tc := range []struct {
		name string
		err  error
		want bool
	}{
		{"ACL user without the command", &ServerError{Message: "NOPERM nodebug has no ACL permissions"}, true},
		{"command missing or renamed away", &ServerError{Message: "ERR unknown command `DEBUG`"}, true},
		{"DEBUG subcommand missing or wrong arity", &ServerError{Message: "ERR Unknown subcommand or wrong number of arguments for 'REPLICA'. Try DEBUG HELP."}, true},
		{"DFLY subcommand missing", &ServerError{Message: "ERR syntax error"}, true},
		{"wrapped", fmt.Errorf("target replication position: %w", &ServerError{Message: "NOPERM x has no ACL permissions"}), true},
		{"replica command on an instance that became a master", &ServerError{Message: "ERR I am master"}, false},
		{"loading", &ServerError{Message: "LOADING Dragonfly is loading the dataset in memory"}, false},
		{"takeover refused", &ServerError{Message: "ERR Full sync not done"}, false},
		{"transport", io.ErrUnexpectedEOF, false},
		{"a plain error with the same words", errors.New("ERR unknown command"), false},
		{"nil", nil, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if got := IsCommandRefused(tc.err); got != tc.want {
				t.Fatalf("IsCommandRefused(%v) = %v, want %v", tc.err, got, tc.want)
			}
		})
	}
}

func TestTakeoverOutcomeUnknown(t *testing.T) {
	for _, tc := range []struct {
		name string
		err  error
		want bool
	}{
		// v1.38.0: REPLTAKEOVER 2 on a replica cut off without a reset,
		// after 12.1s; the master then completed the takeover and exited.
		{"replica gave up waiting for its master", &ServerError{Message: "ERR Couldn't execute takeover: Operation canceled"}, true},
		{"reply lost", errors.New("read tcp 10.0.0.1:6379: i/o timeout"), true},
		{"connection closed", io.EOF, true},
		{"refused before the master was asked", &ServerError{Message: "ERR Full sync not done"}, false},
		{"no such command", &ServerError{Message: "ERR unknown command `REPLTAKEOVER`"}, false},
		{"success", nil, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if got := TakeoverOutcomeUnknown(tc.err); got != tc.want {
				t.Fatalf("TakeoverOutcomeUnknown(%v) = %v, want %v", tc.err, got, tc.want)
			}
		})
	}
}

// A refusal is told from an error that may pass: only a refusal falls
// back to the offset in INFO, and only a refusal is reported as one.
func TestClientReplicationPosition_RefusalAndOtherServerErrors(t *testing.T) {
	for _, tc := range []struct {
		name        string
		info        string
		cmd         string
		reply       string
		wantScalar  bool
		wantRefused bool
		wantErr     string
	}{
		{name: "replica: ACL refusal falls back to the INFO offset", info: infoReplicaCutOff, cmd: "DEBUG REPLICA OFFSET",
			reply: "-NOPERM ops has no ACL permissions\r\n", wantScalar: true},
		{name: "master: ACL refusal with no INFO offset is a refusal", info: infoMasterAhead, cmd: "DFLY REPLICAOFFSET",
			reply: "-NOPERM ops has no ACL permissions\r\n", wantRefused: true, wantErr: "master position unavailable"},
		{name: "replica: the role changed under the command", info: infoReplicaCutOff, cmd: "DEBUG REPLICA OFFSET",
			reply: "-ERR I am master\r\n", wantErr: "DEBUG REPLICA OFFSET: dragonfly: ERR I am master"},
		{name: "master: loading", info: infoMasterAhead, cmd: "DFLY REPLICAOFFSET",
			reply: "-LOADING Dragonfly is loading the dataset in memory\r\n", wantErr: "DFLY REPLICAOFFSET: dragonfly: LOADING"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			srv := newFakeServer(t)
			srv.reply("INFO REPLICATION", bulkString(tc.info))
			srv.reply(tc.cmd, tc.reply)
			c := mustNewClient(t, srv.addr(), "")
			defer func() { _ = c.Close() }()
			got, err := c.ReplicationPosition(context.Background())
			if tc.wantErr == "" {
				if err != nil || got.Scalar != tc.wantScalar {
					t.Fatalf("ReplicationPosition() = %+v, %v; want scalar=%v", got, err, tc.wantScalar)
				}
				return
			}
			if err == nil || !strings.Contains(err.Error(), tc.wantErr) {
				t.Fatalf("ReplicationPosition() = %+v, %v; want an error containing %q", got, err, tc.wantErr)
			}
			if IsCommandRefused(err) != tc.wantRefused {
				t.Fatalf("IsCommandRefused(%v) = %v, want %v", err, !tc.wantRefused, tc.wantRefused)
			}
		})
	}
}
