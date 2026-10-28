package mysql

import (
	"slices"
	"testing"
)

func TestMissingRestoreFencePrivileges(t *testing.T) {
	for _, tc := range []struct {
		name   string
		grants []string
		want   []string
	}{
		{"all privileges", []string{"GRANT ALL PRIVILEGES ON *.* TO `op`@`%` WITH GRANT OPTION"}, nil},
		{"mysql 8 root style", []string{
			"GRANT SELECT, INSERT, PROCESS, SUPER ON *.* TO `root`@`%`",
			"GRANT CONNECTION_ADMIN,REPLICATION_SLAVE_ADMIN,SYSTEM_USER,SYSTEM_VARIABLES_ADMIN ON *.* TO `root`@`%`",
		}, nil},
		{"super covers all but process", []string{"GRANT SELECT, SUPER ON *.* TO `op`@`%`"}, []string{"PROCESS"}},
		{"old documented minimum", []string{"GRANT SYSTEM_VARIABLES_ADMIN, CONNECTION_ADMIN ON *.* TO `op`@`%`"},
			[]string{"PROCESS", "REPLICATION_SLAVE_ADMIN"}},
		{"schema scope does not count", []string{"GRANT ALL PRIVILEGES ON `appdb`.* TO `op`@`%`", "GRANT USAGE ON *.* TO `op`@`%`"},
			RestoreFencePrivileges},
		{"role grant line ignored", []string{"GRANT `connadmin`@`%` TO `op`@`%`"}, RestoreFencePrivileges},
		{"backup role", []string{"GRANT SELECT, BACKUP_ADMIN ON *.* TO `backup`@`%`"}, RestoreFencePrivileges},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if got := missingRestoreFencePrivileges(tc.grants); !slices.Equal(got, tc.want) {
				t.Errorf("missing = %v, want %v", got, tc.want)
			}
		})
	}
}

func TestEvictableSession(t *testing.T) {
	keep := []string{"op", "repl"}
	for _, tc := range []struct {
		user, command string
		want          bool
	}{
		{"app", "Sleep", true},
		{"app", "Query", true},
		{"monitor", "Sleep", true},
		{"app", "Killed", false},
		{"op", "Query", false},
		{"repl", "Sleep", false},
		{"other", "Binlog Dump GTID", false},
		{"event_scheduler", "Daemon", false},
		{"system user", "Connect", false},
		{"unauthenticated user", "Connect", false},
		{"", "Daemon", false},
	} {
		if got := evictableSession(tc.user, tc.command, keep); got != tc.want {
			t.Errorf("evictableSession(%q, %q) = %v, want %v", tc.user, tc.command, got, tc.want)
		}
	}
}
