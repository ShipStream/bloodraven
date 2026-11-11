package dragonfly

import "testing"

func TestClassifySiteRole(t *testing.T) {
	cases := []struct {
		name      string
		info      ReplicationInfo
		reachable bool
		mySite    string
		active    string
		want      SiteRole
	}{
		{
			name:      "unreachable wins over any role data",
			info:      ReplicationInfo{Role: "master"},
			reachable: false,
			mySite:    "iad",
			active:    "iad",
			want:      RoleUnreachable,
		},
		{
			name:      "master on the expected active site",
			info:      ReplicationInfo{Role: "master"},
			reachable: true,
			mySite:    "iad",
			active:    "iad",
			want:      RoleMaster,
		},
		{
			name:      "stale master on non-active site",
			info:      ReplicationInfo{Role: "master"},
			reachable: true,
			mySite:    "pdx",
			active:    "iad",
			want:      RoleStaleMaster,
		},
		{
			name:      "replica regardless of active site",
			info:      ReplicationInfo{Role: "slave", MasterLinkStatus: "up"},
			reachable: true,
			mySite:    "pdx",
			active:    "iad",
			want:      RoleReplica,
		},
		{
			name:      "empty role means unconfigured",
			info:      ReplicationInfo{},
			reachable: true,
			mySite:    "pdx",
			active:    "iad",
			want:      RoleUnconfigured,
		},
		{
			name:      "unknown role string",
			info:      ReplicationInfo{Role: "sentinel"},
			reachable: true,
			mySite:    "pdx",
			active:    "iad",
			want:      RoleUnknown,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := ClassifySiteRole(tc.info, tc.reachable, tc.mySite, tc.active)
			if got != tc.want {
				t.Errorf("ClassifySiteRole() = %s, want %s", got, tc.want)
			}
		})
	}
}

func TestCandidateSyncReady(t *testing.T) {
	cases := []struct {
		name        string
		info        ReplicationInfo
		persistence PersistenceInfo
		want        bool
	}{
		{
			name: "ready: link up, not syncing, not loading",
			info: ReplicationInfo{MasterLinkStatus: "up"},
			want: true,
		},
		{
			name: "not ready: link down",
			info: ReplicationInfo{MasterLinkStatus: "down"},
			want: false,
		},
		{
			name: "not ready: link state not reported",
			info: ReplicationInfo{},
			want: false,
		},
		{
			name: "not ready: full sync in progress",
			info: ReplicationInfo{MasterLinkStatus: "up", MasterSyncInProgress: true},
			want: false,
		},
		{
			name:        "not ready: loading from disk",
			info:        ReplicationInfo{MasterLinkStatus: "up"},
			persistence: PersistenceInfo{Loading: true, LoadState: "rdb-restore"},
			want:        false,
		},
		{
			name: "not ready: link up but never received any IO",
			// MasterLastIOSecondsAgo=-1 is Dragonfly's "never synced"
			// sentinel — link_status flips to "up" the moment TCP
			// handshakes, well before any replication payload arrives.
			info: ReplicationInfo{MasterLinkStatus: "up", MasterLastIOSecondsAgo: -1},
			want: false,
		},
		{
			// The replica's offset is not this function's: a replica far
			// behind passes here and is refused by Position.ReachedBy.
			name: "ready whatever the offset says",
			info: ReplicationInfo{MasterLinkStatus: "up", SlaveReplOffset: 2, SlaveReplOffsetReported: true},
			want: true,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := CandidateSyncReady(tc.info, tc.persistence)
			if got != tc.want {
				t.Errorf("CandidateSyncReady() = %v, want %v", got, tc.want)
			}
		})
	}
}
