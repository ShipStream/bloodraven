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

	// users maps "user@host" to password — one entry per MySQL account,
	// exactly as mysql.user does. Statements that name several accounts
	// touch each of them.
	users map[string]string
	// databases maps schema name to "charset/collation".
	databases map[string]string
	// grants maps schema name to username to the granted privilege list.
	grants map[string]map[string][]string
	// resourceLimits maps "user@host" to "maxUserConnections/maxQueriesPerHour"
	// as last applied via ALTER USER ... WITH.
	resourceLimits map[string]string

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

// accountKey is how the model keys one 'user'@'host' account.
func accountKey(username, host string) string { return username + "@" + host }

// addUser pre-seeds an account on '%', the host every pre-hosts test means.
func (s *fakeSQLServer) addUser(username, password string) {
	s.addAccount(username, "%", password)
}

func (s *fakeSQLServer) addAccount(username, host, password string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.users[accountKey(username, host)] = password
}

// addDatabase pre-seeds a schema the model did not create through the
// reconciler — the setup for the adoption-refusal tests.
func (s *fakeSQLServer) addDatabase(name, charset, collation string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.databases[name] = charset + "/" + collation
}

// removeUser deletes an account — used to simulate mid-rotation states
// where the old owner has already been dropped.
func (s *fakeSQLServer) removeUser(username string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	for key := range s.users {
		if u, _, _ := strings.Cut(key, "@"); u == username {
			delete(s.users, key)
		}
	}
}

// hasUser reports whether username exists on any host.
func (s *fakeSQLServer) hasUser(username string) bool {
	return len(s.hostsOf(username)) > 0
}

// hasAccount reports whether the exact 'user'@'host' account exists.
func (s *fakeSQLServer) hasAccount(username, host string) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	_, ok := s.users[accountKey(username, host)]
	return ok
}

// hostsOf lists the hosts username exists on, sorted.
func (s *fakeSQLServer) hostsOf(username string) []string {
	s.mu.Lock()
	defer s.mu.Unlock()
	var out []string
	for key := range s.users {
		if u, h, _ := strings.Cut(key, "@"); u == username {
			out = append(out, h)
		}
	}
	slices.Sort(out)
	return out
}

// password returns the password of username's '%' account, or — when it has
// no '%' account — of its single other account. Multi-host accounts share a
// password by construction (one statement sets all of them).
func (s *fakeSQLServer) password(username string) (string, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if pw, ok := s.users[accountKey(username, "%")]; ok {
		return pw, true
	}
	for key, pw := range s.users {
		if u, _, _ := strings.Cut(key, "@"); u == username {
			return pw, true
		}
	}
	return "", false
}

func (s *fakeSQLServer) database(name string) (string, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	spec, ok := s.databases[name]
	return spec, ok
}

// resourceLimitsFor returns the limits of username's '%' account or, failing
// that, of any of its accounts.
func (s *fakeSQLServer) resourceLimitsFor(username string) (string, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if limits, ok := s.resourceLimits[accountKey(username, "%")]; ok {
		return limits, true
	}
	for key, limits := range s.resourceLimits {
		if u, _, _ := strings.Cut(key, "@"); u == username {
			return limits, true
		}
	}
	return "", false
}

// grantsFor returns the grants of username's '%' account on database or,
// failing that, of any of its accounts.
func (s *fakeSQLServer) grantsFor(database, username string) ([]string, bool) {
	return s.grantsForAccount(database, username, "")
}

// grantsForAccount returns the grants of one account; host "" means "the '%'
// account, else any".
func (s *fakeSQLServer) grantsForAccount(database, username, host string) ([]string, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	byUser, ok := s.grants[database]
	if !ok {
		return nil, false
	}
	if host != "" {
		privs, ok := byUser[accountKey(username, host)]
		return privs, ok
	}
	if privs, ok := byUser[accountKey(username, "%")]; ok {
		return privs, true
	}
	for key, privs := range byUser {
		if u, _, _ := strings.Cut(key, "@"); u == username {
			return privs, true
		}
	}
	return nil, false
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
	want, exists := server.users[accountKey(user, "%")]
	if !exists {
		for key, pw := range server.users {
			if u, _, _ := strings.Cut(key, "@"); u == user {
				want, exists = pw, true
				break
			}
		}
	}
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
		rows := &fakeSQLRows{}
		if c.server.hasAccount(username, host) {
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
	reCreateUser     = regexp.MustCompile(`^CREATE USER IF NOT EXISTS (.+)$`)
	reAlterUserWith  = regexp.MustCompile(`^ALTER USER (.+?) WITH MAX_USER_CONNECTIONS (\d+) MAX_QUERIES_PER_HOUR (\d+)$`)
	reAlterUser      = regexp.MustCompile(`^ALTER USER (.+)$`)
	reGrant          = regexp.MustCompile("^GRANT (.+) ON `([^`]+)`\\.\\* TO (.+)$")
	reRevoke         = regexp.MustCompile("^REVOKE IF EXISTS ALL PRIVILEGES ON `([^`]+)`\\.\\* FROM (.+?)( IGNORE UNKNOWN USER)?$")
	reRevokePartial  = regexp.MustCompile("^REVOKE IF EXISTS (.+) ON `([^`]+)`\\.\\* FROM (.+?)( IGNORE UNKNOWN USER)?$")
	reDropDatabase   = regexp.MustCompile("^DROP DATABASE IF EXISTS `([^`]+)`$")
	reDropUser       = regexp.MustCompile(`^DROP USER IF EXISTS (.+)$`)

	// reAccountAuth matches one `'user'@'host' IDENTIFIED BY 'password'`
	// element of a CREATE USER / ALTER USER account list; reAccount one
	// bare `'user'@'host'` of a GRANT / REVOKE / DROP USER list. Passwords
	// may contain the escaped forms escapeSingleQuotes produces.
	reAccountAuth = regexp.MustCompile(`'([^']*)'@'([^']*)' IDENTIFIED BY '((?:[^'\\]|\\.|'')*)'`)
	reAccount     = regexp.MustCompile(`'([^']*)'@'([^']*)'`)
)

type fakeAccount struct{ user, host, password string }

// parseAccountAuthList splits a CREATE/ALTER USER account list. It errors
// on an element without an auth clause: the reconciler always renders one
// per account, and a statement that drifted from that must fail loudly.
func parseAccountAuthList(list string) ([]fakeAccount, error) {
	matches := reAccountAuth.FindAllStringSubmatch(list, -1)
	if len(matches) == 0 || len(matches) != len(reAccount.FindAllString(list, -1)) {
		return nil, fmt.Errorf("fake mysql: account list %q is not 'user'@'host' IDENTIFIED BY '...' per account", list)
	}
	out := make([]fakeAccount, 0, len(matches))
	for _, m := range matches {
		out = append(out, fakeAccount{user: m[1], host: m[2], password: fakeSQLUnescape(m[3])})
	}
	return out, nil
}

func parseAccountList(list string) ([]fakeAccount, error) {
	matches := reAccount.FindAllStringSubmatch(list, -1)
	if len(matches) == 0 {
		return nil, fmt.Errorf("fake mysql: empty account list %q", list)
	}
	out := make([]fakeAccount, 0, len(matches))
	for _, m := range matches {
		out = append(out, fakeAccount{user: m[1], host: m[2]})
	}
	return out, nil
}

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
		accounts, err := parseAccountAuthList(m[1])
		if err != nil {
			return err
		}
		for _, a := range accounts {
			key := accountKey(a.user, a.host)
			if _, exists := s.users[key]; !exists {
				s.users[key] = a.password
			}
		}
		return nil

	case reAlterUserWith.MatchString(stmt):
		m := reAlterUserWith.FindStringSubmatch(stmt)
		accounts, err := parseAccountAuthList(m[1])
		if err != nil {
			return err
		}
		for _, a := range accounts {
			key := accountKey(a.user, a.host)
			if _, exists := s.users[key]; !exists {
				return fmt.Errorf("fake mysql: ALTER USER on nonexistent account %q", key)
			}
			s.users[key] = a.password
			if s.resourceLimits == nil {
				s.resourceLimits = map[string]string{}
			}
			s.resourceLimits[key] = m[2] + "/" + m[3]
		}
		return nil

	case reAlterUser.MatchString(stmt):
		m := reAlterUser.FindStringSubmatch(stmt)
		accounts, err := parseAccountAuthList(m[1])
		if err != nil {
			return err
		}
		for _, a := range accounts {
			key := accountKey(a.user, a.host)
			if _, exists := s.users[key]; !exists {
				return fmt.Errorf("fake mysql: ALTER USER on nonexistent account %q", key)
			}
			s.users[key] = a.password
		}
		return nil

	case reGrant.MatchString(stmt):
		m := reGrant.FindStringSubmatch(stmt)
		privs, database := m[1], m[2]
		accounts, err := parseAccountList(m[3])
		if err != nil {
			return err
		}
		if _, exists := s.databases[database]; !exists {
			return fmt.Errorf("fake mysql: GRANT on nonexistent database %q", database)
		}
		for _, a := range accounts {
			key := accountKey(a.user, a.host)
			if _, exists := s.users[key]; !exists {
				return fmt.Errorf("fake mysql: GRANT to nonexistent account %q", key)
			}
			if s.grants[database] == nil {
				s.grants[database] = map[string][]string{}
			}
			// Real MySQL GRANT is additive: it unions the new privileges
			// with whatever the account already holds, so narrowing
			// requires an explicit REVOKE. Modelling GRANT as replacement
			// would let a narrowing test pass without the reconciler ever
			// revoking — the model must not be more forgiving than the
			// server.
			existing := s.grants[database][key]
			for _, p := range strings.Split(privs, ", ") {
				if !slices.Contains(existing, p) {
					existing = append(existing, p)
				}
			}
			if slices.Contains(existing, "ALL PRIVILEGES") {
				existing = []string{"ALL PRIVILEGES"}
			}
			s.grants[database][key] = existing
		}
		return nil

	case reRevoke.MatchString(stmt):
		m := reRevoke.FindStringSubmatch(stmt)
		database, ignoreUnknown := m[1], m[3] != ""
		accounts, err := parseAccountList(m[2])
		if err != nil {
			return err
		}
		for _, a := range accounts {
			key := accountKey(a.user, a.host)
			if _, exists := s.users[key]; !exists && !ignoreUnknown {
				// Mirrors real MySQL (ERROR 1141, verified on 9.7): IF
				// EXISTS only tolerates a missing grant; a missing
				// *account* errors unless IGNORE UNKNOWN USER is present.
				// This is the exact wedge the delete path had, so the
				// model must not be more forgiving than the server.
				return fmt.Errorf("fake mysql: REVOKE from nonexistent account %q without IGNORE UNKNOWN USER", key)
			}
			if byUser, ok := s.grants[database]; ok {
				delete(byUser, key)
			}
		}
		return nil

	case reRevokePartial.MatchString(stmt):
		m := reRevokePartial.FindStringSubmatch(stmt)
		privs, database, ignoreUnknown := m[1], m[2], m[4] != ""
		accounts, err := parseAccountList(m[3])
		if err != nil {
			return err
		}
		for _, a := range accounts {
			key := accountKey(a.user, a.host)
			if _, exists := s.users[key]; !exists && !ignoreUnknown {
				return fmt.Errorf("fake mysql: REVOKE from nonexistent account %q without IGNORE UNKNOWN USER", key)
			}
			byUser, ok := s.grants[database]
			if !ok {
				continue
			}
			current, ok := byUser[key]
			if !ok {
				continue
			}
			if len(current) == 1 && current[0] == "ALL PRIVILEGES" {
				current = append([]string(nil), fakeSQLAllPrivileges...)
			}
			for _, p := range strings.Split(privs, ", ") {
				current = slices.DeleteFunc(current, func(x string) bool { return x == p })
			}
			if len(current) == 0 {
				delete(byUser, key)
			} else {
				byUser[key] = current
			}
		}
		return nil

	case reDropDatabase.MatchString(stmt):
		m := reDropDatabase.FindStringSubmatch(stmt)
		delete(s.databases, m[1])
		delete(s.grants, m[1])
		return nil

	case reDropUser.MatchString(stmt):
		m := reDropUser.FindStringSubmatch(stmt)
		accounts, err := parseAccountList(m[1])
		if err != nil {
			return err
		}
		for _, a := range accounts {
			key := accountKey(a.user, a.host)
			delete(s.users, key)
			delete(s.resourceLimits, key)
			for _, byUser := range s.grants {
				delete(byUser, key)
			}
		}
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
