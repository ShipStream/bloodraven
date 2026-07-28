package sidecar

import (
	"database/sql"
	"errors"
	"fmt"
	"strings"
	"testing"

	mysqldriver "github.com/go-sql-driver/mysql"
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

// A KILL against a session that already exited is the outcome the fence
// wanted. Counting it as a failure would fire the partial-fence warning
// on nearly every fence, since sessions churn between the SELECT and the
// KILL.
func TestIsUnknownThread(t *testing.T) {
	if !isUnknownThread(&mysqldriver.MySQLError{Number: 1094, Message: "Unknown thread id: 42"}) {
		t.Error("ER_NO_SUCH_THREAD not recognized")
	}
	if !isUnknownThread(fmt.Errorf("kill 42: %w", &mysqldriver.MySQLError{Number: 1094})) {
		t.Error("wrapped ER_NO_SUCH_THREAD not recognized")
	}
	// A privilege error is a real failure and must be reported.
	if isUnknownThread(&mysqldriver.MySQLError{Number: 1095, Message: "You are not owner of thread 42"}) {
		t.Error("ER_KILL_DENIED_ERROR must not be treated as a vanished session")
	}
	if isUnknownThread(errors.New("connection reset")) {
		t.Error("non-MySQL error must not be treated as a vanished session")
	}
	if isUnknownThread(nil) {
		t.Error("nil must not be treated as a vanished session")
	}
}

// An incomplete eviction has independent causes that can coincide. The
// warning must name all of them, not just the first.
func TestEvictionError(t *testing.T) {
	if err := evictionError(nil, 0, 0, 5); err != nil {
		t.Errorf("a complete pass must report no error, got %v", err)
	}

	iterFail := errors.New("driver went away")
	got := evictionError(iterFail, 2, 1, 4)
	if got == nil {
		t.Fatal("combined failures reported no error")
	}
	msg := got.Error()
	for _, want := range []string{
		"iterate connections: driver went away",
		"skipped 2 unreadable processlist rows",
		"failed to kill 1 of 4 sessions",
	} {
		if !strings.Contains(msg, want) {
			t.Errorf("combined error %q is missing %q", msg, want)
		}
	}
	// The underlying iteration error stays inspectable through the join.
	if !errors.Is(got, iterFail) {
		t.Error("joined error does not unwrap to the iteration error")
	}

	// Each cause alone is reported alone.
	if msg := evictionError(nil, 3, 0, 9).Error(); !strings.Contains(msg, "skipped 3") || strings.Contains(msg, "failed to kill") {
		t.Errorf("unreadable-only error reported extra causes: %q", msg)
	}
	if msg := evictionError(nil, 0, 2, 9).Error(); !strings.Contains(msg, "failed to kill 2 of 9") || strings.Contains(msg, "skipped") {
		t.Errorf("kill-failure-only error reported extra causes: %q", msg)
	}
}
