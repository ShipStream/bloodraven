package mysql

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"strings"

	"github.com/go-sql-driver/mysql"
)

// RestoreFencer applies the database-level fence of a full-instance
// in-place restore: @@GLOBAL.offline_mode, which disconnects and refuses
// every session without CONNECTION_ADMIN or SUPER while the operator, the
// sidecar and the restore Job (all on the operator account) keep working.
// The production checker implements it; the topology manager type-asserts
// and refuses to fence a checker that does not.
type RestoreFencer interface {
	// MissingRestoreFencePrivileges lists the RestoreFencePrivileges the
	// checker's own account lacks on *.*, directly and through its active
	// roles (nil when it holds them all).
	MissingRestoreFencePrivileges(ctx context.Context) ([]string, error)
	// GetOfflineMode reads @@GLOBAL.offline_mode.
	GetOfflineMode(ctx context.Context) (bool, error)
	// SetOfflineMode sets @@GLOBAL.offline_mode (runtime only, never
	// persisted) and reads it back.
	SetOfflineMode(ctx context.Context, on bool) error
	// EvictSessions kills every client session except this checker's
	// own account, the users in keepUsers, replication dump threads and
	// server threads, then reports how many of the sessions that existed
	// before the sweep are still there. With offline_mode ON only
	// CONNECTION_ADMIN/SUPER accounts can reconnect, and they come back
	// with new connection IDs, so a zero result means no session that
	// predates the fence survives. Sessions of SYSTEM_USER accounts that
	// the checker's account may not kill (it lacks SYSTEM_USER) are not
	// counted; their users are returned in protected.
	EvictSessions(ctx context.Context, keepUsers []string) (remaining int, protected []string, err error)
}

// RestoreFencePrivileges are the global privileges the operator account
// needs for the full-instance restore fence and the restore Job that runs
// under it, beyond DDL and DML. CONNECTION_ADMIN keeps the account
// working under offline_mode, SYSTEM_VARIABLES_ADMIN sets offline_mode,
// PROCESS lets the eviction see other accounts' sessions (without it the
// processlist shows only the caller's own), and REPLICATION_SLAVE_ADMIN
// lets the Job reset the primary's replication. SUPER covers all but
// PROCESS; ALL PRIVILEGES covers every one.
var RestoreFencePrivileges = []string{"CONNECTION_ADMIN", "SYSTEM_VARIABLES_ADMIN", "PROCESS", "REPLICATION_SLAVE_ADMIN"}

// errUnknownThread is MySQL's ER_NO_SUCH_THREAD: the session ended before
// KILL reached it.
const errUnknownThread = 1094

// errKillDenied is MySQL's ER_KILL_DENIED_ERROR: the target session
// belongs to a SYSTEM_USER account and the caller lacks SYSTEM_USER.
const errKillDenied = 1095

func (m *checker) MissingRestoreFencePrivileges(ctx context.Context) ([]string, error) {
	// SHOW GRANTS without FOR reports the session's effective privileges,
	// including those of its active roles (information_schema only lists
	// direct grants).
	rows, err := m.db.QueryContext(ctx, "SHOW GRANTS")
	if err != nil {
		return nil, fmt.Errorf("show grants: %w", err)
	}
	defer rows.Close()
	var grants []string
	for rows.Next() {
		var grant string
		if err := rows.Scan(&grant); err != nil {
			return nil, fmt.Errorf("scan grants: %w", err)
		}
		grants = append(grants, grant)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return missingRestoreFencePrivileges(grants), nil
}

// missingRestoreFencePrivileges returns the RestoreFencePrivileges that
// the SHOW GRANTS lines do not grant on *.*.
func missingRestoreFencePrivileges(grants []string) []string {
	held := map[string]bool{}
	for _, g := range grants {
		for _, p := range globalPrivileges(g) {
			held[p] = true
		}
	}
	var missing []string
	for _, want := range RestoreFencePrivileges {
		switch {
		case held[want], held["ALL"], held["ALL PRIVILEGES"]:
		case held["SUPER"] && want != "PROCESS":
		default:
			missing = append(missing, want)
		}
	}
	return missing
}

// globalPrivileges returns the privileges one SHOW GRANTS line grants on
// *.* (upper case), or nil for a line on a narrower scope or a role grant.
func globalPrivileges(grant string) []string {
	rest, ok := strings.CutPrefix(strings.TrimSpace(grant), "GRANT ")
	if !ok {
		return nil
	}
	privs, scope, ok := strings.Cut(rest, " ON ")
	if !ok || !strings.HasPrefix(strings.TrimSpace(scope), "*.*") {
		return nil
	}
	var out []string
	for _, p := range strings.Split(privs, ",") {
		out = append(out, strings.ToUpper(strings.TrimSpace(p)))
	}
	return out
}

func (m *checker) GetOfflineMode(ctx context.Context) (bool, error) {
	var on bool
	if err := m.db.QueryRowContext(ctx, "SELECT @@GLOBAL.offline_mode").Scan(&on); err != nil {
		return false, fmt.Errorf("read offline_mode: %w", err)
	}
	return on, nil
}

func (m *checker) SetOfflineMode(ctx context.Context, on bool) error {
	val := "OFF"
	if on {
		val = "ON"
	}
	if _, err := m.db.ExecContext(ctx, "SET GLOBAL offline_mode = "+val); err != nil {
		return fmt.Errorf("set offline_mode=%s: %w", val, err)
	}
	got, err := m.GetOfflineMode(ctx)
	if err != nil {
		return err
	}
	if got != on {
		return fmt.Errorf("set offline_mode=%s did not take effect", val)
	}
	return nil
}

type processlistEntry struct {
	id      int64
	user    string
	command string
}

// evictableSessions lists the client sessions an eviction targets.
func (m *checker) evictableSessions(ctx context.Context, keepUsers []string) ([]processlistEntry, error) {
	rows, err := m.db.QueryContext(ctx,
		`SELECT ID, USER, COMMAND FROM information_schema.PROCESSLIST WHERE ID != CONNECTION_ID()`)
	if err != nil {
		return nil, fmt.Errorf("list sessions: %w", err)
	}
	defer rows.Close()
	var out []processlistEntry
	for rows.Next() {
		var e processlistEntry
		if err := rows.Scan(&e.id, &e.user, &e.command); err != nil {
			return nil, fmt.Errorf("scan session: %w", err)
		}
		if evictableSession(e.user, e.command, keepUsers) {
			out = append(out, e)
		}
	}
	return out, rows.Err()
}

// evictableSession reports whether a PROCESSLIST row is a client session
// the restore fence must remove: not a server thread, not a session that
// is already being killed, not a replication dump thread (offline_mode
// itself cuts a non-admin replicator), and not one of keepUsers.
func evictableSession(user, command string, keepUsers []string) bool {
	switch {
	case user == "" || user == "system user" || user == "event_scheduler" || user == "unauthenticated user":
		return false
	case command == "Daemon" || command == "Killed" || command == "Binlog Dump" || command == "Binlog Dump GTID":
		return false
	}
	return !slices.Contains(keepUsers, user)
}

func (m *checker) EvictSessions(ctx context.Context, keepUsers []string) (int, []string, error) {
	var self string
	if err := m.db.QueryRowContext(ctx, "SELECT SUBSTRING_INDEX(CURRENT_USER(), '@', 1)").Scan(&self); err != nil {
		return 0, nil, fmt.Errorf("read current user: %w", err)
	}
	keep := append([]string{self}, keepUsers...)
	before, err := m.evictableSessions(ctx, keep)
	if err != nil {
		return 0, nil, err
	}
	var protectedIDs []int64
	var protected []string
	for _, s := range before {
		if _, err := m.db.ExecContext(ctx, fmt.Sprintf("KILL %d", s.id)); err != nil {
			var me *mysql.MySQLError
			if errors.As(err, &me) && me.Number == errUnknownThread {
				continue
			}
			if errors.As(err, &me) && me.Number == errKillDenied {
				protectedIDs = append(protectedIDs, s.id)
				if !slices.Contains(protected, s.user) {
					protected = append(protected, s.user)
				}
				continue
			}
			return 0, nil, fmt.Errorf("kill session %d (%s): %w", s.id, s.user, err)
		}
	}
	after, err := m.evictableSessions(ctx, keep)
	if err != nil {
		return 0, nil, err
	}
	remaining := 0
	for _, s := range after {
		if slices.Contains(protectedIDs, s.id) {
			continue
		}
		if slices.ContainsFunc(before, func(b processlistEntry) bool { return b.id == s.id }) {
			remaining++
		}
	}
	return remaining, protected, nil
}
