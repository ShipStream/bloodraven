package component

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"fmt"
	"io"
	"regexp"
	"slices"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
)

// ---------------------------------------------------------------------------
// fakeSQLServer is an in-memory MySQL model: users with passwords, databases
// with charset/collation, and schema-scoped grants.
//
// It exists so the MysqlDatabase component tests drive the real reconciler
// (real hashing, real rendering, real ordering) against fake SQL, rather than
// driving a fake reconciler. Registering it as a database/sql driver means
// the reconciler's own openMySQLFunc seam is the only thing substituted; the
// statements under test are the ones production emits, verbatim.
//
// Unrecognised statements are errors on purpose. If the reconciler starts
// emitting SQL this model does not model, the test should fail loudly rather
// than silently accept it.
// ---------------------------------------------------------------------------

type fakeSQLServer struct {
	mu sync.Mutex

	// users maps username to password. Host is always '%', matching
	// tenantUserHost.
	users map[string]string
	// databases maps schema name to "charset/collation".
	databases map[string]string
	// grants maps schema name to username to the granted privilege list.
	grants map[string]map[string][]string

	// statements records every statement executed, in order, across all
	// connections. This is how "zero MySQL statements when nothing
	// changed" is asserted literally.
	statements []string
	// dialedAddrs records every address a connection was opened against,
	// so a failover can be observed rather than inferred.
	dialedAddrs []string
	// authFailures counts connection attempts rejected for a bad password.
	authFailures int
}

func newFakeSQLServer() *fakeSQLServer {
	return &fakeSQLServer{
		users:     map[string]string{},
		databases: map[string]string{},
		grants:    map[string]map[string][]string{},
	}
}

func (s *fakeSQLServer) addUser(username, password string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.users[username] = password
}

// addDatabase pre-seeds a schema the model did not create through the
// reconciler — the setup for the adoption-refusal tests.
func (s *fakeSQLServer) addDatabase(name, charset, collation string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.databases[name] = charset + "/" + collation
}

func (s *fakeSQLServer) hasUser(username string) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	_, ok := s.users[username]
	return ok
}

func (s *fakeSQLServer) password(username string) (string, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	pw, ok := s.users[username]
	return pw, ok
}

func (s *fakeSQLServer) database(name string) (string, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	spec, ok := s.databases[name]
	return spec, ok
}

func (s *fakeSQLServer) grantsFor(database, username string) ([]string, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	byUser, ok := s.grants[database]
	if !ok {
		return nil, false
	}
	privs, ok := byUser[username]
	return privs, ok
}

func (s *fakeSQLServer) statementCount() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return len(s.statements)
}

func (s *fakeSQLServer) statementsSince(n int) []string {
	s.mu.Lock()
	defer s.mu.Unlock()
	if n > len(s.statements) {
		return nil
	}
	out := make([]string, len(s.statements)-n)
	copy(out, s.statements[n:])
	return out
}

func (s *fakeSQLServer) lastDialedAddr() string {
	s.mu.Lock()
	defer s.mu.Unlock()
	if len(s.dialedAddrs) == 0 {
		return ""
	}
	return s.dialedAddrs[len(s.dialedAddrs)-1]
}

// ---------------------------------------------------------------------------
// database/sql driver plumbing
// ---------------------------------------------------------------------------

const fakeSQLDriverName = "bloodraven-fake-mysql"

var (
	fakeSQLRegistry   sync.Map // model id -> *fakeSQLServer
	fakeSQLRegisterer sync.Once
	fakeSQLNextID     atomic.Int64
)

// register makes the model reachable through the driver and returns an
// openMySQLFunc-shaped dialer that authenticates against it.
func (s *fakeSQLServer) dialer() func(user, password, addr, tlsConfigName string) (*sql.DB, error) {
	fakeSQLRegisterer.Do(func() {
		sql.Register(fakeSQLDriverName, fakeSQLDriver{})
	})
	id := strconv.FormatInt(fakeSQLNextID.Add(1), 10)
	fakeSQLRegistry.Store(id, s)

	return func(user, password, addr, tlsConfigName string) (*sql.DB, error) {
		db, err := sql.Open(fakeSQLDriverName, fmt.Sprintf("%s|%s|%s|%s", id, user, password, addr))
		if err != nil {
			return nil, err
		}
		db.SetMaxOpenConns(2)
		if err := db.Ping(); err != nil {
			db.Close()
			return nil, err
		}
		return db, nil
	}
}

type fakeSQLDriver struct{}

func (fakeSQLDriver) Open(dsn string) (driver.Conn, error) {
	parts := strings.SplitN(dsn, "|", 4)
	if len(parts) != 4 {
		return nil, fmt.Errorf("fake mysql: malformed dsn %q", dsn)
	}
	id, user, password, addr := parts[0], parts[1], parts[2], parts[3]

	v, ok := fakeSQLRegistry.Load(id)
	if !ok {
		return nil, fmt.Errorf("fake mysql: unknown model %q", id)
	}
	server := v.(*fakeSQLServer)

	server.mu.Lock()
	server.dialedAddrs = append(server.dialedAddrs, addr)
	want, exists := server.users[user]
	server.mu.Unlock()

	if !exists || want != password {
		server.mu.Lock()
		server.authFailures++
		server.mu.Unlock()
		return nil, fmt.Errorf("fake mysql: access denied for user %q", user)
	}
	return &fakeSQLConn{server: server}, nil
}

type fakeSQLConn struct {
	server *fakeSQLServer
}

func (c *fakeSQLConn) Prepare(string) (driver.Stmt, error) {
	return nil, fmt.Errorf("fake mysql: Prepare is not supported; use ExecContext/QueryContext")
}
func (c *fakeSQLConn) Close() error { return nil }
func (c *fakeSQLConn) Begin() (driver.Tx, error) {
	return nil, fmt.Errorf("fake mysql: no transactions")
}

func (c *fakeSQLConn) ExecContext(_ context.Context, query string, args []driver.NamedValue) (driver.Result, error) {
	if len(args) != 0 {
		return nil, fmt.Errorf("fake mysql: unexpected args on exec %q", query)
	}
	if err := c.server.apply(query); err != nil {
		return nil, err
	}
	return driver.RowsAffected(0), nil
}

func (c *fakeSQLConn) QueryContext(_ context.Context, query string, args []driver.NamedValue) (driver.Rows, error) {
	switch query {
	case "SELECT 1 FROM mysql.user WHERE user = ? AND host = ?":
		if len(args) != 2 {
			return nil, fmt.Errorf("fake mysql: user-existence query wants 2 args, got %d", len(args))
		}
		username, _ := args[0].Value.(string)
		host, _ := args[1].Value.(string)
		if host != "%" {
			return nil, fmt.Errorf("fake mysql: unexpected host %q", host)
		}
		rows := &fakeSQLRows{}
		if c.server.hasUser(username) {
			rows.remaining = 1
		}
		return rows, nil

	case "SELECT 1 FROM information_schema.SCHEMATA WHERE SCHEMA_NAME = ?":
		if len(args) != 1 {
			return nil, fmt.Errorf("fake mysql: schema-existence query wants 1 arg, got %d", len(args))
		}
		name, _ := args[0].Value.(string)
		rows := &fakeSQLRows{}
		if _, exists := c.server.database(name); exists {
			rows.remaining = 1
		}
		return rows, nil
	}
	return nil, fmt.Errorf("fake mysql: unmodelled query %q", query)
}

type fakeSQLRows struct {
	remaining int
}

func (r *fakeSQLRows) Columns() []string { return []string{"1"} }
func (r *fakeSQLRows) Close() error      { return nil }
func (r *fakeSQLRows) Next(dest []driver.Value) error {
	if r.remaining == 0 {
		return io.EOF
	}
	r.remaining--
	dest[0] = int64(1)
	return nil
}

// ---------------------------------------------------------------------------
// Statement interpretation
// ---------------------------------------------------------------------------

var (
	reCreateDatabase = regexp.MustCompile("^CREATE DATABASE IF NOT EXISTS `([^`]+)` CHARACTER SET (\\S+) COLLATE (\\S+)$")
	reAlterDatabase  = regexp.MustCompile("^ALTER DATABASE `([^`]+)` CHARACTER SET (\\S+) COLLATE (\\S+)$")
	reCreateUser     = regexp.MustCompile(`^CREATE USER IF NOT EXISTS '(.*)'@'%' IDENTIFIED BY '(.*)'$`)
	reAlterUser      = regexp.MustCompile(`^ALTER USER '(.*)'@'%' IDENTIFIED BY '(.*)'$`)
	reGrant          = regexp.MustCompile("^GRANT (.+) ON `([^`]+)`\\.\\* TO '(.*)'@'%'$")
	reRevoke         = regexp.MustCompile("^REVOKE IF EXISTS ALL PRIVILEGES ON `([^`]+)`\\.\\* FROM '(.*)'@'%'( IGNORE UNKNOWN USER)?$")
	reRevokePartial  = regexp.MustCompile("^REVOKE IF EXISTS (.+) ON `([^`]+)`\\.\\* FROM '(.*)'@'%'( IGNORE UNKNOWN USER)?$")
	reDropDatabase   = regexp.MustCompile("^DROP DATABASE IF EXISTS `([^`]+)`$")
	reDropUser       = regexp.MustCompile(`^DROP USER IF EXISTS '(.*)'@'%'$`)
)

// fakeSQLAllPrivileges is what ALL PRIVILEGES expands to on a schema: the
// concrete privilege list the reconciler's allowlist permits. Revoking a
// named privilege from an account that holds ALL must leave the rest,
// exactly as the real server does.
var fakeSQLAllPrivileges = []string{
	"SELECT", "INSERT", "UPDATE", "DELETE", "CREATE", "DROP", "ALTER",
	"INDEX", "REFERENCES", "LOCK TABLES", "SHOW VIEW", "TRIGGER", "EVENT", "EXECUTE",
}

func (s *fakeSQLServer) apply(stmt string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.statements = append(s.statements, stmt)

	switch {
	case stmt == "FLUSH PRIVILEGES":
		return nil

	case reCreateDatabase.MatchString(stmt):
		m := reCreateDatabase.FindStringSubmatch(stmt)
		if _, exists := s.databases[m[1]]; !exists {
			s.databases[m[1]] = m[2] + "/" + m[3]
		}
		return nil

	case reAlterDatabase.MatchString(stmt):
		m := reAlterDatabase.FindStringSubmatch(stmt)
		if _, exists := s.databases[m[1]]; !exists {
			return fmt.Errorf("fake mysql: ALTER DATABASE on nonexistent database %q", m[1])
		}
		s.databases[m[1]] = m[2] + "/" + m[3]
		return nil

	case reCreateUser.MatchString(stmt):
		m := reCreateUser.FindStringSubmatch(stmt)
		if _, exists := s.users[m[1]]; !exists {
			s.users[m[1]] = fakeSQLUnescape(m[2])
		}
		return nil

	case reAlterUser.MatchString(stmt):
		m := reAlterUser.FindStringSubmatch(stmt)
		if _, exists := s.users[m[1]]; !exists {
			return fmt.Errorf("fake mysql: ALTER USER on nonexistent user %q", m[1])
		}
		s.users[m[1]] = fakeSQLUnescape(m[2])
		return nil

	case reGrant.MatchString(stmt):
		m := reGrant.FindStringSubmatch(stmt)
		privs, database, username := m[1], m[2], m[3]
		if _, exists := s.databases[database]; !exists {
			return fmt.Errorf("fake mysql: GRANT on nonexistent database %q", database)
		}
		if _, exists := s.users[username]; !exists {
			return fmt.Errorf("fake mysql: GRANT to nonexistent user %q", username)
		}
		if s.grants[database] == nil {
			s.grants[database] = map[string][]string{}
		}
		// Real MySQL GRANT is additive: it unions the new privileges with
		// whatever the account already holds, so narrowing requires an
		// explicit REVOKE. Modelling GRANT as replacement would let a
		// narrowing test pass without the reconciler ever revoking — the
		// model must not be more forgiving than the server.
		existing := s.grants[database][username]
		for _, p := range strings.Split(privs, ", ") {
			if !slices.Contains(existing, p) {
				existing = append(existing, p)
			}
		}
		if slices.Contains(existing, "ALL PRIVILEGES") {
			existing = []string{"ALL PRIVILEGES"}
		}
		s.grants[database][username] = existing
		return nil

	case reRevoke.MatchString(stmt):
		m := reRevoke.FindStringSubmatch(stmt)
		database, username, ignoreUnknown := m[1], m[2], m[3] != ""
		if _, exists := s.users[username]; !exists && !ignoreUnknown {
			// Mirrors real MySQL (ERROR 1141, verified on 9.7): IF EXISTS
			// only tolerates a missing grant; a missing *account* errors
			// unless IGNORE UNKNOWN USER is present. This is the exact
			// wedge the delete path had, so the model must not be more
			// forgiving than the server.
			return fmt.Errorf("fake mysql: REVOKE from nonexistent user %q without IGNORE UNKNOWN USER", username)
		}
		if byUser, ok := s.grants[database]; ok {
			delete(byUser, username)
		}
		return nil

	case reRevokePartial.MatchString(stmt):
		m := reRevokePartial.FindStringSubmatch(stmt)
		privs, database, username, ignoreUnknown := m[1], m[2], m[3], m[4] != ""
		if _, exists := s.users[username]; !exists && !ignoreUnknown {
			return fmt.Errorf("fake mysql: REVOKE from nonexistent user %q without IGNORE UNKNOWN USER", username)
		}
		byUser, ok := s.grants[database]
		if !ok {
			return nil
		}
		current, ok := byUser[username]
		if !ok {
			return nil
		}
		if len(current) == 1 && current[0] == "ALL PRIVILEGES" {
			current = append([]string(nil), fakeSQLAllPrivileges...)
		}
		for _, p := range strings.Split(privs, ", ") {
			current = slices.DeleteFunc(current, func(x string) bool { return x == p })
		}
		if len(current) == 0 {
			delete(byUser, username)
		} else {
			byUser[username] = current
		}
		return nil

	case reDropDatabase.MatchString(stmt):
		m := reDropDatabase.FindStringSubmatch(stmt)
		delete(s.databases, m[1])
		delete(s.grants, m[1])
		return nil

	case reDropUser.MatchString(stmt):
		m := reDropUser.FindStringSubmatch(stmt)
		delete(s.users, m[1])
		return nil
	}

	return fmt.Errorf("fake mysql: unmodelled statement %q", stmt)
}

// fakeSQLUnescape reverses escapeSingleQuotes so the model stores the real
// password and can therefore authenticate against it.
func fakeSQLUnescape(s string) string {
	s = strings.ReplaceAll(s, "''", "'")
	return strings.ReplaceAll(s, `\\`, `\`)
}

// assertNoGrantOption fails if any statement the reconciler has ever emitted
// carries WITH GRANT OPTION. Asserted over the whole recorded history rather
// than per-case, because the property is absolute.
func (s *fakeSQLServer) assertNoGrantOption(t *testing.T) {
	t.Helper()
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, stmt := range s.statements {
		if strings.Contains(strings.ToUpper(stmt), "GRANT OPTION") {
			t.Fatalf("reconciler emitted %q; a MysqlDatabase must never confer GRANT OPTION", stmt)
		}
	}
}
