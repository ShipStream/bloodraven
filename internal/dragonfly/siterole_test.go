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
		name         string
		info         ReplicationInfo
		persistence  PersistenceInfo
		sourceOffset int64
		want         bool
	}{
		{
			name:         "ready: link up, not syncing, not loading, offset matched",
			info:         ReplicationInfo{MasterLinkStatus: "up", SlaveReplOffset: 100},
			sourceOffset: 100,
			want:         true,
		},
		{
			name:         "ready: replica overshot source",
			info:         ReplicationInfo{MasterLinkStatus: "up", SlaveReplOffset: 500},
			sourceOffset: 100,
			want:         true,
		},
		{
			name:         "not ready: link down",
			info:         ReplicationInfo{MasterLinkStatus: "down", SlaveReplOffset: 100},
			sourceOffset: 100,
			want:         false,
		},
		{
			name:         "not ready: full sync in progress",
			info:         ReplicationInfo{MasterLinkStatus: "up", MasterSyncInProgress: true, SlaveReplOffset: 100},
			sourceOffset: 100,
			want:         false,
		},
		{
			name:         "not ready: loading from disk",
			info:         ReplicationInfo{MasterLinkStatus: "up", SlaveReplOffset: 100},
			persistence:  PersistenceInfo{Loading: true, LoadState: "rdb-restore"},
			sourceOffset: 100,
			want:         false,
		},
		{
			name:         "not ready: replica behind",
			info:         ReplicationInfo{MasterLinkStatus: "up", SlaveReplOffset: 99},
			sourceOffset: 100,
			want:         false,
		},
		{
			name: "not ready: link up but never received any IO",
			// MasterLastIOSecondsAgo=-1 is Dragonfly's "never synced"
			// sentinel — link_status flips to "up" the moment TCP
			// handshakes, well before any replication payload arrives.
			info:         ReplicationInfo{MasterLinkStatus: "up", MasterLastIOSecondsAgo: -1, SlaveReplOffset: 100},
			sourceOffset: 100,
			want:         false,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := CandidateSyncReady(tc.info, tc.persistence, tc.sourceOffset)
			if got != tc.want {
				t.Errorf("CandidateSyncReady() = %v, want %v", got, tc.want)
			}
		})
	}
}
