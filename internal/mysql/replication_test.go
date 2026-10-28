package mysql

import (
	"testing"
)

// TestReplicationSourceOpts verifies the struct can hold all required fields.
func TestReplicationSourceOpts(t *testing.T) {
	opts := ReplicationSourceOpts{
		Host:     "primary.example.com",
		User:     "repl",
		Password: "secret",
		UseSSL:   true,
	}
	if opts.Host != "primary.example.com" {
		t.Error("host mismatch")
	}
	if opts.User != "repl" {
		t.Error("user mismatch")
	}
	if opts.Password != "secret" {
		t.Error("password mismatch")
	}
	if !opts.UseSSL {
		t.Error("ssl should be true")
	}
}

// TestReplicaStatus verifies the struct fields.
func TestReplicaStatus(t *testing.T) {
	secs := int64(5)
	rs := ReplicaStatus{
		IORunning:           true,
		SQLRunning:          true,
		SecondsBehindSource: &secs,
		LastError:           "",
		SourceHost:          "primary.example.com",
		ExecutedGtidSet:     "uuid1:1-100",
	}
	if !rs.IORunning {
		t.Error("IO should be running")
	}
	if !rs.SQLRunning {
		t.Error("SQL should be running")
	}
	if *rs.SecondsBehindSource != 5 {
		t.Errorf("seconds behind: got %d, want 5", *rs.SecondsBehindSource)
	}
}

func TestUnappliedRelayLog(t *testing.T) {
	tests := []struct {
		name      string
		target    string
		retrieved string
		executed  string
		want      string
	}{
		{"empty retrieved", "", "", "uuid1:1-10", ""},
		{"both empty", "", "", "", ""},
		{"equal sets", "uuid1:1-10", "uuid1:1-10", "uuid1:1-10", ""},
		{"executed superset", "uuid1:1-5", "uuid1:1-5", "uuid1:1-10", ""},
		{"pending transactions", "uuid1:1-20", "uuid1:1-20", "uuid1:1-10", "uuid1:11-20"},
		{"completely different uuid", "uuid2:1-5", "uuid2:1-5", "uuid1:1-10", "uuid2:1-5"},
		// The receiver kept fetching after the drain began: only the
		// target, not the newer relay log, has to be applied.
		{"target applied while receiver still fetching", "uuid1:1-20", "uuid1:1-30", "uuid1:1-20", ""},
		{"target partly applied while receiver still fetching", "uuid1:1-20", "uuid1:1-30", "uuid1:1-15", "uuid1:16-20"},
		// The relay log was reset under the drain: nothing retrieved is
		// pending any more.
		{"retrieved shrank and is applied", "uuid1:1-20", "", "uuid1:1-10", ""},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			parse := func(s string) GTIDSet {
				g, err := ParseGTIDSet(s)
				if err != nil {
					t.Fatalf("parse %q: %v", s, err)
				}
				return g
			}
			got := unappliedRelayLog(parse(tt.target), parse(tt.retrieved), parse(tt.executed))
			if got.String() != tt.want {
				t.Errorf("unappliedRelayLog(target=%q, retrieved=%q, executed=%q) = %q, want %q",
					tt.target, tt.retrieved, tt.executed, got, tt.want)
			}
		})
	}
}

func TestChangeReplicationSourceSQL(t *testing.T) {
	base := "CHANGE REPLICATION SOURCE TO SOURCE_HOST='h', SOURCE_USER='u', SOURCE_PASSWORD='p''w', SOURCE_AUTO_POSITION=1"
	tests := []struct {
		name string
		ssl  bool
		want string
	}{
		{"tls verifies the source against the group CA", true, base + ", SOURCE_SSL=1, SOURCE_SSL_CA='/etc/mysql/tls/ca.crt'"},
		{"plaintext fetches the source public key", false, base + ", GET_SOURCE_PUBLIC_KEY=1"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := changeReplicationSourceSQL(ReplicationSourceOpts{Host: "h", User: "u", Password: "p'w", UseSSL: tt.ssl})
			if got != tt.want {
				t.Fatalf("got  %s\nwant %s", got, tt.want)
			}
		})
	}
}
