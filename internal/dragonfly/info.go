// Package dragonfly provides a minimal Dragonfly client and INFO parsers
// used by Bloodraven to observe and steer per-site Dragonfly instances.
package dragonfly

import (
	"errors"
	"fmt"
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

	// MasterSyncInProgressReported is true when master_sync_in_progress
	// was on the wire and parsed, so a reported false is distinguishable
	// from an absent field.
	MasterSyncInProgressReported bool

	// MasterLastIOSecondsAgo is seconds since the replica last received
	// any byte from the master. -1 when not present on the wire.
	MasterLastIOSecondsAgo int

	// MasterLastIOReported is true when master_last_io_seconds_ago was on
	// the wire and parsed, so a reported -1 ("never received data") is
	// distinguishable from an absent field.
	MasterLastIOReported bool

	// SlaveReplOffset is `slave_repl_offset`: on a Dragonfly replica the
	// number of journal records it has executed, summed over its flows
	// (one per master shard). It is not a byte offset. Dragonfly prints
	// the line only once the full sync is done. 0 when absent; see
	// SlaveReplOffsetReported.
	SlaveReplOffset int64

	// SlaveReplOffsetReported is true when slave_repl_offset was on the
	// wire and parsed.
	SlaveReplOffsetReported bool

	// MasterReplOffset is the `master_repl_offset` field, 0 when absent.
	// Dragonfly v1.38 and v2.1 report it in neither role (a master prints
	// role, connected_slaves, its slaveN lines and master_replid; a
	// replica prints slave_repl_offset), so 0 does not mean "nothing
	// written". A master's position is read with DFLY REPLICAOFFSET
	// (Client.ReplicationPosition).
	MasterReplOffset int64

	// MasterReplOffsetReported is true when master_repl_offset was on the
	// wire and parsed (a Redis-style server).
	MasterReplOffsetReported bool

	// MasterReplID is `master_replid`: on a master its own replication
	// id, on a replica the id of the master it syncs from. A master gets
	// a new id when its process starts, so a replica whose id differs
	// from its master's follows an earlier incarnation of it.
	MasterReplID string

	// ConnectedSlaves is the count of replicas currently linked to this
	// instance. Reported on masters; meaningless on replicas. One of the
	// gates of the stale-master auto-reconfigure: an instance replicas
	// are attached to is not reattached automatically. It is not a sign
	// of an empty instance: a target just promoted from a replica holds
	// the whole cache with connected_slaves=0. The key count
	// (ParseInfoKeyspace) is what tells the two apart.
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
			switch v := strings.TrimSpace(value); {
			case v == "1" || strings.EqualFold(v, "true"):
				info.MasterSyncInProgress, info.MasterSyncInProgressReported = true, true
			case v == "0" || strings.EqualFold(v, "false"):
				info.MasterSyncInProgress, info.MasterSyncInProgressReported = false, true
			}
		case "master_last_io_seconds_ago":
			if n, err := strconv.Atoi(strings.TrimSpace(value)); err == nil {
				info.MasterLastIOSecondsAgo, info.MasterLastIOReported = n, true
			}
		case "slave_repl_offset", "replica_repl_offset":
			if n, err := strconv.ParseInt(strings.TrimSpace(value), 10, 64); err == nil && n >= 0 {
				info.SlaveReplOffset, info.SlaveReplOffsetReported = n, true
			}
		case "master_repl_offset":
			if n, err := strconv.ParseInt(strings.TrimSpace(value), 10, 64); err == nil && n >= 0 {
				info.MasterReplOffset, info.MasterReplOffsetReported = n, true
			}
		case "master_replid":
			info.MasterReplID = strings.TrimSpace(value)
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

// ParseInfoKeyspace returns the number of keys an `INFO keyspace` body
// reports, summed over its `dbN:keys=...` lines. A keyspace section with
// no database line is an instance with no keys (Dragonfly v1.38 prints
// `db0:keys=0` for one, Redis prints the header alone). A body with
// neither the section header nor a database line, and a database line
// without a readable `keys=` field, are errors: the callers decide on
// "this instance holds nothing", and a reply this parser does not know
// (a server without the section answers with an empty body) must not
// read as that.
func ParseInfoKeyspace(body string) (int64, error) {
	var total int64
	section := false
	for _, line := range strings.Split(strings.ReplaceAll(body, "\r\n", "\n"), "\n") {
		if strings.EqualFold(strings.TrimSpace(line), "# Keyspace") {
			section = true
		}
	}
	for _, line := range splitInfoLines(body) {
		key, value, ok := splitKV(line)
		if !ok || !isKeyspaceDB(key) {
			continue
		}
		section = true
		keys, found := int64(0), false
		for _, field := range strings.Split(value, ",") {
			name, v, ok := strings.Cut(strings.TrimSpace(field), "=")
			if !ok || name != "keys" {
				continue
			}
			n, err := strconv.ParseInt(strings.TrimSpace(v), 10, 64)
			if err != nil || n < 0 {
				return 0, fmt.Errorf("dragonfly: INFO keyspace: unreadable key count in %q", line)
			}
			keys, found = n, true
		}
		if !found {
			return 0, fmt.Errorf("dragonfly: INFO keyspace: no key count in %q", line)
		}
		total += keys
	}
	if !section {
		return 0, errors.New("dragonfly: INFO keyspace: no keyspace section in the reply")
	}
	return total, nil
}

// isKeyspaceDB reports whether key is a keyspace database name: "db"
// followed by digits.
func isKeyspaceDB(key string) bool {
	digits, ok := strings.CutPrefix(key, "db")
	if !ok || digits == "" {
		return false
	}
	for _, r := range digits {
		if r < '0' || r > '9' {
			return false
		}
	}
	return true
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
