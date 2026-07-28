package sidecar

import (
	"database/sql"
	"testing"
)

func TestMySQLBool(t *testing.T) {
	tests := []struct {
		name  string
		value sql.NullString
		want  bool
	}{
		{name: "null", value: sql.NullString{}, want: false},
		{name: "zero", value: sql.NullString{String: "0", Valid: true}, want: false},
		{name: "one", value: sql.NullString{String: "1", Valid: true}, want: true},
		{name: "off", value: sql.NullString{String: "OFF", Valid: true}, want: false},
		{name: "on", value: sql.NullString{String: "On", Valid: true}, want: true},
		{name: "true", value: sql.NullString{String: " true ", Valid: true}, want: true},
		{name: "unknown", value: sql.NullString{String: "yes", Valid: true}, want: false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := mysqlBool(tt.value); got != tt.want {
				t.Fatalf("mysqlBool(%q) = %v, want %v", tt.value.String, got, tt.want)
			}
		})
	}
}

// A fence kills application sessions. It must never kill the site's own
// replication threads: on a replica those run as `system user`, and
// stopping them leaves the fenced site stalled — permanently, if it is
// diverged enough for source convergence to be Blocked (issue #119).
func TestKillableConnectionSparesReplicationThreads(t *testing.T) {
	spared := []struct{ user, command string }{
		{"system user", "Connect"}, // replica I/O thread
		{"system user", "Query"},   // applier / coordinator thread
		{"system user", "Daemon"},  // server-internal worker
		{"event_scheduler", "Daemon"},
		{"", "Daemon"},                     // NULL user, folded to "" by the query
		{"replicator", "Binlog Dump GTID"}, // outbound feed to a peer replica
		{"replicator", "Binlog Dump"},
	}
	for _, c := range spared {
		if killableConnection(c.user, c.command) {
			t.Errorf("killableConnection(%q, %q) = true, want false", c.user, c.command)
		}
	}

	killed := []struct{ user, command string }{
		{"counter", "Query"},
		{"counter", "Sleep"},
		{"root", "Query"},
		{"replicator", "Query"}, // an ordinary session, not a binlog feed
	}
	for _, c := range killed {
		if !killableConnection(c.user, c.command) {
			t.Errorf("killableConnection(%q, %q) = false, want true", c.user, c.command)
		}
	}
}
