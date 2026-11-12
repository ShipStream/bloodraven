package dragonfly

import (
	"context"
	"strings"
	"testing"
)

// Replies captured from Dragonfly v1.38.0 (--proactor_threads=2); v2.1
// replied the same way. The master had written 24 and 28 journal records
// on its two shards; the replica, cut off from it after the full sync, had
// executed one record per flow and still reported a healthy link.
const (
	dflyReplicaOffsetMaster  = "*2\r\n:24\r\n:28\r\n"
	debugReplicaOffsetCaught = "*2\r\n$5\r\nSYNC1\r\n*2\r\n:24\r\n:28\r\n"
	debugReplicaOffsetBehind = "*2\r\n$5\r\nSYNC1\r\n*2\r\n:1\r\n:1\r\n"

	infoMasterAhead = "# Replication\r\nrole:master\r\nconnected_slaves:1\r\n" +
		"slave0:ip=192.168.176.3,port=6379,state=online,lag=27\r\n" +
		"master_replid:ebd8f508f20b79588f87e632d908641a529d02eb\r\n"
	infoReplicaCutOff = "# Replication\r\nrole:slave\r\nmaster_host:wp5-m4\r\nmaster_port:6379\r\n" +
		"master_link_status:up\r\nmaster_last_io_seconds_ago:0\r\nmaster_sync_in_progress:0\r\n" +
		"master_replid:ebd8f508f20b79588f87e632d908641a529d02eb\r\nslave_repl_offset:2\r\n" +
		"slave_priority:100\r\nslave_read_only:1\r\npsync_attempts:0\r\npsync_successes:0\r\n"
)

func TestPositionReachedBy(t *testing.T) {
	master := Position{Role: "master", Flows: []int64{24, 28}, ReplID: "a"}
	cases := []struct {
		name       string
		source     Position
		target     Position
		want       bool
		wantDetail string
	}{
		{"every flow reached", master, Position{Role: "slave", Flows: []int64{24, 28}, ReplID: "a"}, true, ""},
		{"target ahead of the captured position", master, Position{Role: "slave", Flows: []int64{30, 28}, ReplID: "a"}, true, ""},
		{"an idle source at zero", Position{Role: "master", Flows: []int64{0, 0}, ReplID: "a"},
			Position{Role: "slave", Flows: []int64{0, 0}, ReplID: "a"}, true, ""},
		{"one flow behind", master, Position{Role: "slave", Flows: []int64{24, 27}, ReplID: "a"}, false, "target flow 1 at 27 behind source 28"},
		// The sum (what slave_repl_offset prints) has reached the source's
		// sum, and one flow has not: the sum proves nothing.
		{"sum reached, one flow behind", master, Position{Role: "slave", Flows: []int64{51, 1}, ReplID: "a"}, false, "target flow 1 at 1 behind source 28"},
		{"cut-off replica", master, Position{Role: "slave", Flows: []int64{1, 1}, ReplID: "a"}, false, "target flow 0 at 1 behind source 24"},
		{"target without a position", master, Position{Role: "slave", ReplID: "a"}, false, "target reported no replication position"},
		{"source without a position", Position{Role: "master", ReplID: "a"}, Position{Role: "slave", Flows: []int64{1}, ReplID: "a"}, false,
			"source reported no replication position"},
		{"zero-value source", Position{}, Position{Role: "slave", Flows: []int64{1}}, false, `source reports role "", not master`},
		{"zero-value target", master, Position{}, false, `target reports role "", not a replica`},
		{"target is a master", master, Position{Role: "master", Flows: []int64{24, 28}, ReplID: "a"}, false, `target reports role "master", not a replica`},
		{"source is a replica", Position{Role: "slave", Flows: []int64{1}}, Position{Role: "slave", Flows: []int64{1}}, false,
			`source reports role "slave", not master`},
		{"flow counts differ", master, Position{Role: "slave", Flows: []int64{100}, ReplID: "a"}, false, "target has 1 replication flows, source has 2 shards"},
		{"scalar against per-flow", Position{Role: "master", Flows: []int64{5}, ReplID: "a"},
			Position{Role: "slave", Flows: []int64{9}, Scalar: true, ReplID: "a"}, false, "source and target report their positions in different forms"},
		{"scalar reached", Position{Role: "master", Flows: []int64{5}, Scalar: true, ReplID: "a"},
			Position{Role: "slave", Flows: []int64{5}, Scalar: true, ReplID: "a"}, true, ""},
		{"scalar behind", Position{Role: "master", Flows: []int64{5}, Scalar: true, ReplID: "a"},
			Position{Role: "slave", Flows: []int64{4}, Scalar: true, ReplID: "a"}, false, "target offset 4 behind source 5"},
		// A master gets a new replication id when it starts: a replica that
		// still reports the old one follows the instance that is gone, and
		// its counts say nothing about the new one's journal.
		{"target follows another incarnation", Position{Role: "master", Flows: []int64{0, 0}, ReplID: "new"},
			Position{Role: "slave", Flows: []int64{24, 28}, ReplID: "old"}, false, `target syncs from replication id "old", source is "new"`},
		{"target reports no replication id", master, Position{Role: "slave", Flows: []int64{24, 28}}, false,
			`target syncs from replication id "", source is "a"`},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got, detail := tc.source.ReachedBy(tc.target)
			if got != tc.want || detail != tc.wantDetail {
				t.Errorf("ReachedBy() = %v, %q; want %v, %q", got, detail, tc.want, tc.wantDetail)
			}
		})
	}
}

func TestPositionTotal(t *testing.T) {
	if got := (Position{Flows: []int64{999794, 1000208}}).Total(); got != 2000002 {
		t.Errorf("Total() = %d, want 2000002", got)
	}
	if got := (Position{}).Total(); got != 0 {
		t.Errorf("Total() of no flows = %d, want 0", got)
	}
}

func TestClientReplicationPosition(t *testing.T) {
	cases := []struct {
		name    string
		replies map[string]string
		want    Position
		wantErr string
	}{
		{
			name:    "master",
			replies: map[string]string{"INFO REPLICATION": bulkString(infoMasterAhead), "DFLY REPLICAOFFSET": dflyReplicaOffsetMaster},
			want:    Position{Role: "master", Flows: []int64{24, 28}, ReplID: "ebd8f508f20b79588f87e632d908641a529d02eb"},
		},
		{
			name:    "replica cut off from its master",
			replies: map[string]string{"INFO REPLICATION": bulkString(infoReplicaCutOff), "DEBUG REPLICA OFFSET": debugReplicaOffsetBehind},
			want:    Position{Role: "slave", Flows: []int64{1, 1}, ReplID: "ebd8f508f20b79588f87e632d908641a529d02eb"},
		},
		{
			name:    "replica caught up",
			replies: map[string]string{"INFO REPLICATION": bulkString(infoReplicaCutOff), "DEBUG REPLICA OFFSET": debugReplicaOffsetCaught},
			want:    Position{Role: "slave", Flows: []int64{24, 28}, ReplID: "ebd8f508f20b79588f87e632d908641a529d02eb"},
		},
		{
			// A Dragonfly master without the command and without an offset
			// in INFO has no position; it is not at zero.
			name: "master: command refused and no offset in INFO",
			replies: map[string]string{"INFO REPLICATION": bulkString(infoMasterAhead),
				"DFLY REPLICAOFFSET": "-ERR unknown subcommand or wrong number of arguments for 'REPLICAOFFSET'\r\n"},
			wantErr: "master position unavailable",
		},
		{
			// A Redis-style server: no DFLY command, an offset in INFO.
			name: "master: command refused, offset in INFO",
			replies: map[string]string{"INFO REPLICATION": bulkString("role:master\r\nmaster_replid:abc\r\nmaster_repl_offset:4242\r\n"),
				"DFLY REPLICAOFFSET": "-ERR unknown command 'DFLY'\r\n"},
			want: Position{Role: "master", Flows: []int64{4242}, Scalar: true, ReplID: "abc"},
		},
		{
			name: "replica: command refused, offset in INFO",
			replies: map[string]string{"INFO REPLICATION": bulkString(infoReplicaCutOff),
				"DEBUG REPLICA OFFSET": "-ERR unknown subcommand\r\n"},
			want: Position{Role: "slave", Flows: []int64{2}, Scalar: true, ReplID: "ebd8f508f20b79588f87e632d908641a529d02eb"},
		},
		{
			// During the full sync a replica prints no slave_repl_offset.
			name: "replica: command refused and no offset in INFO",
			replies: map[string]string{"INFO REPLICATION": bulkString(infoReplicationReplicaFullSyncV138),
				"DEBUG REPLICA OFFSET": "-ERR unknown subcommand\r\n"},
			wantErr: "replica position unavailable",
		},
		{
			name:    "no role",
			replies: map[string]string{"INFO REPLICATION": bulkString("# Replication\r\n")},
			wantErr: `no replication position for role ""`,
		},
		{
			name:    "master: reply is not an array",
			replies: map[string]string{"INFO REPLICATION": bulkString(infoMasterAhead), "DFLY REPLICAOFFSET": "+OK\r\n"},
			wantErr: "DFLY REPLICAOFFSET: unexpected reply type",
		},
		{
			name:    "master: empty array",
			replies: map[string]string{"INFO REPLICATION": bulkString(infoMasterAhead), "DFLY REPLICAOFFSET": "*0\r\n"},
			wantErr: "DFLY REPLICAOFFSET: empty reply",
		},
		{
			name:    "master: element is not an integer",
			replies: map[string]string{"INFO REPLICATION": bulkString(infoMasterAhead), "DFLY REPLICAOFFSET": "*2\r\n:24\r\n$2\r\nxx\r\n"},
			wantErr: "element 1 is not a non-negative integer",
		},
		{
			name:    "master: negative element",
			replies: map[string]string{"INFO REPLICATION": bulkString(infoMasterAhead), "DFLY REPLICAOFFSET": "*2\r\n:24\r\n:-1\r\n"},
			wantErr: "element 1 is not a non-negative integer",
		},
		{
			name:    "replica: no sync id",
			replies: map[string]string{"INFO REPLICATION": bulkString(infoReplicaCutOff), "DEBUG REPLICA OFFSET": "*2\r\n:1\r\n:1\r\n"},
			wantErr: "DEBUG REPLICA OFFSET: no sync id in the reply",
		},
		{
			name:    "replica: wrong shape",
			replies: map[string]string{"INFO REPLICATION": bulkString(infoReplicaCutOff), "DEBUG REPLICA OFFSET": "+OK\r\n"},
			wantErr: "DEBUG REPLICA OFFSET: unexpected reply shape",
		},
		{
			name: "replica: no flows",
			replies: map[string]string{"INFO REPLICATION": bulkString(infoReplicaCutOff),
				"DEBUG REPLICA OFFSET": "*2\r\n$5\r\nSYNC1\r\n*0\r\n"},
			wantErr: "DEBUG REPLICA OFFSET: empty reply",
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			srv := newFakeServer(t)
			for cmd, reply := range tc.replies {
				srv.reply(cmd, reply)
			}
			c := mustNewClient(t, srv.addr(), "")
			defer func() { _ = c.Close() }()
			got, err := c.ReplicationPosition(context.Background())
			if tc.wantErr != "" {
				if err == nil || !strings.Contains(err.Error(), tc.wantErr) {
					t.Fatalf("ReplicationPosition() = %+v, %v; want an error containing %q", got, err, tc.wantErr)
				}
				return
			}
			if err != nil {
				t.Fatalf("ReplicationPosition(): %v", err)
			}
			if got.Role != tc.want.Role || got.Scalar != tc.want.Scalar || got.ReplID != tc.want.ReplID || !equalInt64s(got.Flows, tc.want.Flows) {
				t.Errorf("ReplicationPosition() = %+v, want %+v", got, tc.want)
			}
		})
	}
}

// The captured pair: the replica's own INFO answers pass every link check
// while it is 23 and 27 records behind. The positions are what refuses it.
func TestCutOffReplicaPassesLinkChecksAndFailsThePosition(t *testing.T) {
	if !CandidateSyncReady(ParseInfoReplication(infoReplicaCutOff), PersistenceInfo{}) {
		t.Fatal("the captured cut-off replica should pass the link checks; the fixture no longer shows what it was captured for")
	}
	master := Position{Role: "master", Flows: []int64{24, 28}, ReplID: "ebd8f508f20b79588f87e632d908641a529d02eb"}
	replica := Position{Role: "slave", Flows: []int64{1, 1}, ReplID: "ebd8f508f20b79588f87e632d908641a529d02eb"}
	if reached, detail := master.ReachedBy(replica); reached {
		t.Fatalf("ReachedBy() = true (%q) for a replica behind on both flows", detail)
	}
}

func equalInt64s(a, b []int64) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}
