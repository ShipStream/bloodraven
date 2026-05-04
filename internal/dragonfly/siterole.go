package dragonfly

// SiteRole is the operator-level classification of a Dragonfly site.
// Mirrors api/v1alpha1.DragonflyRole values; kept as string here to avoid
// pulling the API package into a leaf parser package.
type SiteRole string

const (
	RoleUnknown      SiteRole = "unknown"
	RoleMaster       SiteRole = "master"
	RoleReplica      SiteRole = "replica"
	RoleStaleMaster  SiteRole = "stale-master"
	RoleUnconfigured SiteRole = "unconfigured"
	RoleUnreachable  SiteRole = "unreachable"
)

// ClassifySiteRole categorizes a site's observed Dragonfly state.
//
// Inputs:
//   - info: the parsed `INFO replication` snapshot (zero-value when site
//     was unreachable; pass reachable=false for that case).
//   - reachable: true when the operator successfully completed an INFO
//     call against this site on this poll.
//   - mySite: this site's name.
//   - expectedActive: the site that should currently be the master
//     (typically status.activeSite).
//
// Classification rules:
//   - reachable=false              -> RoleUnreachable
//   - role="master"  AND  this site is the expected active site -> RoleMaster
//   - role="master"  AND  this site is NOT the expected active site -> RoleStaleMaster
//   - role="slave"                  -> RoleReplica (regardless of link health)
//   - role="" or unknown role       -> RoleUnconfigured (fresh pod, not yet wired)
//   - anything else                 -> RoleUnknown
//
// Stale-master detection is intentionally permissive (any non-active site
// reporting master is flagged) so the caller can decide whether to fence,
// reconfigure, or merely log.
func ClassifySiteRole(info ReplicationInfo, reachable bool, mySite, expectedActive string) SiteRole {
	if !reachable {
		return RoleUnreachable
	}
	switch info.Role {
	case "master":
		if mySite == expectedActive {
			return RoleMaster
		}
		return RoleStaleMaster
	case "slave":
		return RoleReplica
	case "":
		return RoleUnconfigured
	default:
		return RoleUnknown
	}
}

// CandidateSyncReady reports whether a target replica is safe to promote
// during planned switchover.
//
// Readiness requires:
//   - master link is up
//   - replica has received at least one byte from the master
//     (master_last_io_seconds_ago != -1)
//   - no full-sync transfer in progress
//   - replica is not loading from disk
//   - replica's applied offset has reached or exceeded the source's
//     captured offset
//
// The persistence argument may be the zero value if the caller did not
// fetch INFO persistence; in that case the loading check is skipped.
//
// sourceOffset is typically the master_repl_offset captured on the source
// after Bloodraven set super_read_only and stopped accepting writes.
//
// The MasterLastIOSecondsAgo gate is load-bearing: Dragonfly reports
// link_status=up the moment the TCP handshake completes, before any
// replication payload has been transferred. A "never-synced" replica
// reaching this function would otherwise pass the gate and be promoted
// as an empty master, silently losing every cached value.
func CandidateSyncReady(info ReplicationInfo, persistence PersistenceInfo, sourceOffset int64) bool {
	if info.MasterLinkStatus != "up" {
		return false
	}
	if info.MasterLastIOSecondsAgo < 0 {
		return false
	}
	if info.MasterSyncInProgress {
		return false
	}
	if persistence.Loading {
		return false
	}
	if info.SlaveReplOffset < sourceOffset {
		return false
	}
	return true
}
