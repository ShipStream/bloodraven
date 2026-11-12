package dragonfly

import "testing"

func TestParseInfoReplication(t *testing.T) {
	cases := []struct {
		name string
		body string
		want ReplicationInfo
	}{
		{
			name: "master with no replicas",
			body: "# Replication\r\nrole:master\r\nconnected_slaves:0\r\nmaster_repl_offset:0\r\n",
			want: ReplicationInfo{
				Role:                     "master",
				MasterLastIOSecondsAgo:   -1,
				MasterReplOffsetReported: true,
			},
		},
		{
			name: "master with replication offset",
			body: "# Replication\r\nrole:master\r\nconnected_slaves:1\r\nmaster_repl_offset:123456789\r\n",
			want: ReplicationInfo{
				Role:                     "master",
				MasterLastIOSecondsAgo:   -1,
				MasterReplOffset:         123456789,
				MasterReplOffsetReported: true,
				ConnectedSlaves:          1,
			},
		},
		{
			// Dragonfly's `connected_replicas` alias resolves to the
			// same field as `connected_slaves` so the auto-rejoin
			// gate does not depend on which Dragonfly version is
			// emitting INFO.
			name: "connected_replicas alias",
			body: "# Replication\nrole:master\nconnected_replicas:3\nmaster_repl_offset:0\n",
			want: ReplicationInfo{
				Role:                     "master",
				MasterLastIOSecondsAgo:   -1,
				MasterReplOffsetReported: true,
				ConnectedSlaves:          3,
			},
		},
		{
			name: "healthy replica",
			body: "# Replication\nrole:slave\nmaster_host:10.42.1.5\nmaster_port:6379\nmaster_link_status:up\nmaster_sync_in_progress:0\nmaster_last_io_seconds_ago:1\nslave_repl_offset:123456000\nmaster_repl_offset:123456789\n",
			want: ReplicationInfo{
				Role:                         "slave",
				MasterHost:                   "10.42.1.5",
				MasterPort:                   6379,
				MasterLinkStatus:             "up",
				MasterSyncInProgress:         false,
				MasterSyncInProgressReported: true,
				MasterLastIOSecondsAgo:       1,
				MasterLastIOReported:         true,
				SlaveReplOffset:              123456000,
				SlaveReplOffsetReported:      true,
				MasterReplOffset:             123456789,
				MasterReplOffsetReported:     true,
			},
		},
		{
			name: "syncing replica",
			body: "# Replication\nrole:slave\nmaster_host:10.42.1.5\nmaster_port:6379\nmaster_link_status:up\nmaster_sync_in_progress:1\nmaster_last_io_seconds_ago:0\nslave_repl_offset:0\n",
			want: ReplicationInfo{
				Role:                         "slave",
				MasterHost:                   "10.42.1.5",
				MasterPort:                   6379,
				MasterLinkStatus:             "up",
				MasterSyncInProgress:         true,
				MasterSyncInProgressReported: true,
				MasterLastIOSecondsAgo:       0,
				MasterLastIOReported:         true,
				SlaveReplOffsetReported:      true,
			},
		},
		{
			name: "broken link",
			body: "# Replication\nrole:slave\nmaster_host:10.42.1.5\nmaster_port:6379\nmaster_link_status:down\nmaster_last_io_seconds_ago:42\n",
			want: ReplicationInfo{
				Role:                   "slave",
				MasterHost:             "10.42.1.5",
				MasterPort:             6379,
				MasterLinkStatus:       "down",
				MasterLastIOSecondsAgo: 42,
				MasterLastIOReported:   true,
			},
		},
		{
			name: "dragonfly replica alias normalized to slave",
			body: "role:replica\nmaster_link_status:up\nmaster_repl_offset:99\nreplica_repl_offset:99\n",
			want: ReplicationInfo{
				Role:                     "slave",
				MasterLinkStatus:         "up",
				MasterReplOffset:         99,
				MasterReplOffsetReported: true,
				SlaveReplOffset:          99,
				SlaveReplOffsetReported:  true,
				MasterLastIOSecondsAgo:   -1,
			},
		},
		{
			name: "malformed lines are skipped",
			body: "garbage\n# comment\nrole:master\nnoise without colon\n:lonely value\nmaster_repl_offset:7\n",
			want: ReplicationInfo{
				Role:                     "master",
				MasterLastIOSecondsAgo:   -1,
				MasterReplOffset:         7,
				MasterReplOffsetReported: true,
			},
		},
		{
			// A reported -1 ("never received data") is a reported value,
			// distinct from an absent field.
			name: "reported never-synced replica",
			body: "role:slave\nmaster_link_status:down\nmaster_sync_in_progress:0\nmaster_last_io_seconds_ago:-1\n",
			want: ReplicationInfo{
				Role:                         "slave",
				MasterLinkStatus:             "down",
				MasterSyncInProgressReported: true,
				MasterLastIOSecondsAgo:       -1,
				MasterLastIOReported:         true,
			},
		},
		{
			// Unparsable values are not reported values.
			name: "unparsable sync and io values",
			body: "role:slave\nmaster_sync_in_progress:maybe\nmaster_last_io_seconds_ago:soon\n",
			want: ReplicationInfo{Role: "slave", MasterLastIOSecondsAgo: -1},
		},
		{
			name: "empty body",
			body: "",
			want: ReplicationInfo{MasterLastIOSecondsAgo: -1},
		},
		{
			// Captured from Dragonfly v1.38.0 (v2.1 prints the same lines):
			// a master prints no offset of its own.
			name: "v1.38 master with a replica in stable sync",
			body: infoReplicationMasterV138,
			want: ReplicationInfo{
				Role:                   "master",
				MasterLastIOSecondsAgo: -1,
				ConnectedSlaves:        1,
				MasterReplID:           "c6abbdb2bdacc872709407ba9c10c81214d6d307",
			},
		},
		{
			name: "v1.38 replica in stable sync",
			body: infoReplicationReplicaV138,
			want: ReplicationInfo{
				Role:                         "slave",
				MasterHost:                   "wp5-m",
				MasterPort:                   6379,
				MasterLinkStatus:             "up",
				MasterSyncInProgressReported: true,
				MasterLastIOSecondsAgo:       0,
				MasterLastIOReported:         true,
				SlaveReplOffset:              2000002,
				SlaveReplOffsetReported:      true,
				MasterReplID:                 "c6abbdb2bdacc872709407ba9c10c81214d6d307",
			},
		},
		{
			// During the full sync a replica prints no slave_repl_offset.
			name: "v1.38 replica in full sync",
			body: infoReplicationReplicaFullSyncV138,
			want: ReplicationInfo{
				Role:                         "slave",
				MasterHost:                   "wp5-m",
				MasterPort:                   6379,
				MasterLinkStatus:             "up",
				MasterSyncInProgress:         true,
				MasterSyncInProgressReported: true,
				MasterLastIOSecondsAgo:       11,
				MasterLastIOReported:         true,
				MasterReplID:                 "c6abbdb2bdacc872709407ba9c10c81214d6d307",
			},
		},
		{
			// An offset that is not a number is not a reported offset.
			name: "unparsable offsets",
			body: "role:slave\nslave_repl_offset:lots\nmaster_repl_offset:-5\n",
			want: ReplicationInfo{Role: "slave", MasterLastIOSecondsAgo: -1},
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := ParseInfoReplication(tc.body)
			if got != tc.want {
				t.Errorf("ParseInfoReplication() mismatch\n got: %+v\nwant: %+v", got, tc.want)
			}
		})
	}
}

func TestParseInfoPersistence(t *testing.T) {
	cases := []struct {
		name string
		body string
		want PersistenceInfo
	}{
		{
			name: "not loading",
			body: "# Persistence\nloading:0\n",
			want: PersistenceInfo{Loading: false},
		},
		{
			name: "loading with state",
			body: "# Persistence\nloading:1\nload_state:rdb-restore\n",
			want: PersistenceInfo{Loading: true, LoadState: "rdb-restore"},
		},
		{
			name: "missing fields",
			body: "# Persistence\n",
			want: PersistenceInfo{},
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := ParseInfoPersistence(tc.body)
			if got != tc.want {
				t.Errorf("ParseInfoPersistence() mismatch\n got: %+v\nwant: %+v", got, tc.want)
			}
		})
	}
}

func TestParseInfoKeyspace(t *testing.T) {
	cases := []struct {
		name    string
		body    string
		want    int64
		wantErr bool
	}{
		{
			// Dragonfly v1.38.0, a started instance nothing was written to.
			name: "dragonfly empty instance",
			body: "# Keyspace\r\ndb0:keys=0,expires=0,hits=0,misses=0,hit_ratio=0.00,avg_ttl=-1\r\n",
			want: 0,
		},
		{
			// Dragonfly v1.38.0 after one key in db0 and two in db3.
			name: "keys in two databases",
			body: "# Keyspace\r\ndb0:keys=1,expires=0,hits=0,misses=0,hit_ratio=0.00,avg_ttl=-1\r\ndb3:keys=2,expires=0,hits=0,misses=0,hit_ratio=0.00,avg_ttl=-1\r\n",
			want: 3,
		},
		{name: "no database line", body: "# Keyspace\n", want: 0},
		{name: "other lines ignored", body: "# Keyspace\ndbsize:9\nnoise\ndb1:keys=4,expires=1\n", want: 4},
		{name: "database line without the header", body: "db0:keys=2,expires=0\n", want: 2},
		{name: "empty reply", body: "", wantErr: true},
		{name: "another section", body: "# Replication\nrole:master\n", wantErr: true},
		{name: "database line without a key count", body: "db0:expires=0\n", wantErr: true},
		{name: "unreadable key count", body: "db0:keys=many,expires=0\n", wantErr: true},
		{name: "negative key count", body: "db0:keys=-1\n", wantErr: true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got, err := ParseInfoKeyspace(tc.body)
			if (err != nil) != tc.wantErr {
				t.Fatalf("ParseInfoKeyspace() error = %v, wantErr %v", err, tc.wantErr)
			}
			if got != tc.want {
				t.Errorf("ParseInfoKeyspace() = %d, want %d", got, tc.want)
			}
		})
	}
}

// INFO replication bodies captured from Dragonfly v1.38.0 (docker,
// --proactor_threads=2). v2.1 printed the same lines.
const (
	infoReplicationMasterV138 = "# Replication\r\nrole:master\r\nconnected_slaves:1\r\n" +
		"slave0:ip=192.168.176.3,port=6379,state=online,lag=0\r\n" +
		"master_replid:c6abbdb2bdacc872709407ba9c10c81214d6d307\r\n"

	infoReplicationReplicaV138 = "# Replication\r\nrole:slave\r\nmaster_host:wp5-m\r\nmaster_port:6379\r\n" +
		"master_link_status:up\r\nmaster_last_io_seconds_ago:0\r\nmaster_sync_in_progress:0\r\n" +
		"master_replid:c6abbdb2bdacc872709407ba9c10c81214d6d307\r\nslave_repl_offset:2000002\r\n" +
		"slave_priority:100\r\nslave_read_only:1\r\npsync_attempts:0\r\npsync_successes:0\r\n"

	infoReplicationReplicaFullSyncV138 = "# Replication\r\nrole:slave\r\nmaster_host:wp5-m\r\nmaster_port:6379\r\n" +
		"master_link_status:up\r\nmaster_last_io_seconds_ago:11\r\nmaster_sync_in_progress:1\r\n" +
		"master_replid:c6abbdb2bdacc872709407ba9c10c81214d6d307\r\n" +
		"slave_priority:100\r\nslave_read_only:1\r\npsync_attempts:0\r\npsync_successes:0\r\n"
)
