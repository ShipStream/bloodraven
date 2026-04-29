// Package dragonfly provides a minimal Dragonfly client and INFO parsers
// used by Bloodraven to observe and steer per-site Dragonfly instances.
package dragonfly

import (
	"strconv"
	"strings"
)

// ReplicationInfo is the parsed subset of `INFO replication` Bloodraven
// uses. Fields that were not present on the wire are left as zero values;
// callers should branch on Role to decide which fields are meaningful.
type ReplicationInfo struct {
	// Role is "master" or "slave". On Dragonfly older than v1.x the
	// alias "replica" may also appear; ParseInfoReplication normalizes
	// that to "slave" for Redis compatibility.
	Role string

	// MasterHost is the configured master endpoint on a replica.
	MasterHost string

	// MasterPort is the configured master port on a replica.
	MasterPort int

	// MasterLinkStatus is "up" or "down" on a replica.
	MasterLinkStatus string

	// MasterSyncInProgress is true when an initial full-sync (RDB
	// snapshot transfer) is in flight on a replica.
	MasterSyncInProgress bool

	// MasterLastIOSecondsAgo is seconds since the replica last received
	// any byte from the master. -1 when not present on the wire.
	MasterLastIOSecondsAgo int

	// SlaveReplOffset is the replica's applied offset (bytes).
	SlaveReplOffset int64

	// MasterReplOffset is the master's published offset (bytes). Both
	// masters and replicas publish a value; the master's is the
	// authoritative high-water mark for catch-up.
	MasterReplOffset int64

	// ConnectedSlaves is the count of replicas currently linked to this
	// instance. Reported on masters; meaningless on replicas. Used to
	// gate stale-master auto-reconfigure: a stale master with
	// connected_slaves=0 has provably never been used by any replica
	// since restart, which combined with master_repl_offset=0 is the
	// upstream-blessed signal that no writes were accepted.
	ConnectedSlaves int
}

// PersistenceInfo is the parsed subset of `INFO persistence` used to detect
// load-after-restart states that should disqualify a replica from being
// promoted.
type PersistenceInfo struct {
	// Loading is true while Dragonfly is loading a snapshot from disk
	// at startup.
	Loading bool

	// LoadState is a free-form description (Dragonfly-specific) of the
	// loading progress when Loading is true. Empty otherwise.
	LoadState string
}

// ParseInfoReplication parses the body of an `INFO replication` response.
// Dragonfly returns a section header followed by `key:value\r\n` lines.
// Lines that do not match are ignored so that schema additions in newer
// Dragonfly versions never break us.
func ParseInfoReplication(body string) ReplicationInfo {
	info := ReplicationInfo{
		MasterLastIOSecondsAgo: -1,
	}
	for _, line := range splitInfoLines(body) {
		key, value, ok := splitKV(line)
		if !ok {
			continue
		}
		switch key {
		case "role":
			// Dragonfly may emit "replica"; normalize to Redis-style "slave".
			if value == "replica" {
				info.Role = "slave"
			} else {
				info.Role = value
			}
		case "master_host":
			info.MasterHost = value
		case "master_port":
			info.MasterPort = atoiOrZero(value)
		case "master_link_status":
			info.MasterLinkStatus = value
		case "master_sync_in_progress":
			info.MasterSyncInProgress = value == "1" || strings.EqualFold(value, "true")
		case "master_last_io_seconds_ago":
			info.MasterLastIOSecondsAgo = atoiOrNeg1(value)
		case "slave_repl_offset", "replica_repl_offset":
			info.SlaveReplOffset = atoi64OrZero(value)
		case "master_repl_offset":
			info.MasterReplOffset = atoi64OrZero(value)
		case "connected_slaves", "connected_replicas":
			info.ConnectedSlaves = atoiOrZero(value)
		}
	}
	return info
}

// ParseInfoPersistence parses the body of an `INFO persistence` response.
func ParseInfoPersistence(body string) PersistenceInfo {
	info := PersistenceInfo{}
	for _, line := range splitInfoLines(body) {
		key, value, ok := splitKV(line)
		if !ok {
			continue
		}
		switch key {
		case "loading":
			info.Loading = value == "1" || strings.EqualFold(value, "true")
		case "load_state":
			info.LoadState = value
		}
	}
	return info
}

// splitInfoLines splits an INFO response on either CRLF or LF and trims
// blank lines and section headers (lines starting with '#').
func splitInfoLines(body string) []string {
	body = strings.ReplaceAll(body, "\r\n", "\n")
	raw := strings.Split(body, "\n")
	out := make([]string, 0, len(raw))
	for _, line := range raw {
		line = strings.TrimSpace(line)
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		out = append(out, line)
	}
	return out
}

func splitKV(line string) (string, string, bool) {
	idx := strings.Index(line, ":")
	if idx <= 0 {
		return "", "", false
	}
	return strings.TrimSpace(line[:idx]), strings.TrimSpace(line[idx+1:]), true
}

func atoiOrZero(s string) int {
	n, err := strconv.Atoi(strings.TrimSpace(s))
	if err != nil {
		return 0
	}
	return n
}

func atoiOrNeg1(s string) int {
	n, err := strconv.Atoi(strings.TrimSpace(s))
	if err != nil {
		return -1
	}
	return n
}

func atoi64OrZero(s string) int64 {
	n, err := strconv.ParseInt(strings.TrimSpace(s), 10, 64)
	if err != nil {
		return 0
	}
	return n
}
