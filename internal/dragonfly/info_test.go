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
				Role:                   "master",
				MasterLastIOSecondsAgo: -1,
			},
		},
		{
			name: "master with replication offset",
			body: "# Replication\r\nrole:master\r\nconnected_slaves:1\r\nmaster_repl_offset:123456789\r\n",
			want: ReplicationInfo{
				Role:                   "master",
				MasterLastIOSecondsAgo: -1,
				MasterReplOffset:       123456789,
			},
		},
		{
			name: "healthy replica",
			body: "# Replication\nrole:slave\nmaster_host:10.42.1.5\nmaster_port:6379\nmaster_link_status:up\nmaster_sync_in_progress:0\nmaster_last_io_seconds_ago:1\nslave_repl_offset:123456000\nmaster_repl_offset:123456789\n",
			want: ReplicationInfo{
				Role:                   "slave",
				MasterHost:             "10.42.1.5",
				MasterPort:             6379,
				MasterLinkStatus:       "up",
				MasterSyncInProgress:   false,
				MasterLastIOSecondsAgo: 1,
				SlaveReplOffset:        123456000,
				MasterReplOffset:       123456789,
			},
		},
		{
			name: "syncing replica",
			body: "# Replication\nrole:slave\nmaster_host:10.42.1.5\nmaster_port:6379\nmaster_link_status:up\nmaster_sync_in_progress:1\nmaster_last_io_seconds_ago:0\nslave_repl_offset:0\n",
			want: ReplicationInfo{
				Role:                   "slave",
				MasterHost:             "10.42.1.5",
				MasterPort:             6379,
				MasterLinkStatus:       "up",
				MasterSyncInProgress:   true,
				MasterLastIOSecondsAgo: 0,
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
			},
		},
		{
			name: "dragonfly replica alias normalized to slave",
			body: "role:replica\nmaster_link_status:up\nmaster_repl_offset:99\nreplica_repl_offset:99\n",
			want: ReplicationInfo{
				Role:                   "slave",
				MasterLinkStatus:       "up",
				MasterReplOffset:       99,
				SlaveReplOffset:        99,
				MasterLastIOSecondsAgo: -1,
			},
		},
		{
			name: "malformed lines are skipped",
			body: "garbage\n# comment\nrole:master\nnoise without colon\n:lonely value\nmaster_repl_offset:7\n",
			want: ReplicationInfo{
				Role:                   "master",
				MasterLastIOSecondsAgo: -1,
				MasterReplOffset:       7,
			},
		},
		{
			name: "empty body",
			body: "",
			want: ReplicationInfo{MasterLastIOSecondsAgo: -1},
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
