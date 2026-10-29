package component

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"encoding/json"
	"fmt"
	"io"
	"regexp"
	"slices"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"

	mysqldriver "github.com/go-sql-driver/mysql"
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
	// grants maps the database of a grant row, exactly as GRANT spelled
	// it (`acme\_wms` and `acme_wms` are different rows), to "user@host"
	// to the granted privilege list.
	grants           map[string]map[string][]string
	globalPrivileges map[string][]string
	restrictions     map[string]map[string][]string
	grantOptions     map[string]map[string]bool
	// partialRevokes is @@GLOBAL.partial_revokes. Off, as on a default
	// server, `_` in a grant row is a wildcard and a schema's own row is
	// spelled with `\_`.
	partialRevokes bool
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

	// statementFault, when set, is consulted for every statement: a
	// non-nil err is returned to the reconciler, after applying the
	// statement when applied is true (a connection lost after the server
	// executed it) and without any effect otherwise (a server refusal).
	statementFault func(stmt string) (applied bool, err error)
	// queryFault is statementFault for the existence queries; a faulted
	// query never answers.
	queryFault func(query string, args []driver.NamedValue) error
}

// failStatements injects err for every statement with the given prefix.
// A *mysql.MySQLError models the server refusing the statement (it did not
// run); any other error models an ambiguous connection failure, which
// applied controls.
func (s *fakeSQLServer) failStatements(prefix string, err error, applied bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.statementFault = func(stmt string) (bool, error) {
		if strings.HasPrefix(stmt, prefix) {
			return applied, err
		}
		return false, nil
	}
}

// failQueries injects err for every existence query whose first argument is
// arg.
func (s *fakeSQLServer) failQueries(arg string, err error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.queryFault = func(_ string, args []driver.NamedValue) error {
		if len(args) > 0 && args[0].Value == arg {
			return err
		}
		return nil
	}
}

// clearFaults removes every injected failure.
func (s *fakeSQLServer) clearFaults() {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.statementFault, s.queryFault = nil, nil
}

func newFakeSQLServer() *fakeSQLServer {
	return &fakeSQLServer{
		users:            map[string]string{},
		databases:        map[string]string{},
		grants:           map[string]map[string][]string{},
		globalPrivileges: map[string][]string{},
		restrictions:     map[string]map[string][]string{},
		grantOptions:     map[string]map[string]bool{},
	}
}

// accountKey is how the model keys one 'user'@'host' account.
func accountKey(username, host string) string { return username + "@" + strings.ToLower(host) }

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

// passwordOf returns the password of the exact 'username'@'host' account.
func (s *fakeSQLServer) passwordOf(username, host string) (string, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	pw, ok := s.users[accountKey(username, host)]
	return pw, ok
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

// resourceLimitsForAccount returns the limits of the exact account.
func (s *fakeSQLServer) resourceLimitsForAccount(username, host string) (string, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	limits, ok := s.resourceLimits[accountKey(username, host)]
	return limits, ok
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
	return s.grantRowLocked(s.grantRowKey(database), username, host)
}

// grantRowKey is the grant-row spelling that matches exactly the schema
// named database on this server.
func (s *fakeSQLServer) grantRowKey(database string) string {
	if s.partialRevokes {
		return database
	}
	return strings.ReplaceAll(database, "_", `\_`)
}

// grantRow returns the grants of one account on a grant row spelled
// exactly as rowKey; host "" means "the '%' account, else any".
func (s *fakeSQLServer) grantRow(rowKey, username, host string) ([]string, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.grantRowLocked(rowKey, username, host)
}

// seedGrantRow pre-seeds a grant row spelled exactly as rowKey, as an
// earlier release would have left it.
func (s *fakeSQLServer) seedGrantRow(rowKey, username, host string, privs ...string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.grants[rowKey] == nil {
		s.grants[rowKey] = map[string][]string{}
	}
	s.grants[rowKey][accountKey(username, host)] = privs
}

func (s *fakeSQLServer) grantRowLocked(rowKey, username, host string) ([]string, bool) {
	byUser, ok := s.grants[rowKey]
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
	return &fakeSQLConn{server: server, username: user}, nil
}

type fakeSQLConn struct {
	username string
	server   *fakeSQLServer
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
	if err := c.server.checkGrantor(c.username, query); err != nil {
		return nil, err
	}
	if err := c.server.apply(query); err != nil {
		return nil, err
	}
	return driver.RowsAffected(0), nil
}

func (c *fakeSQLConn) QueryContext(_ context.Context, query string, args []driver.NamedValue) (driver.Rows, error) {
	c.server.mu.Lock()
	fault := c.server.queryFault
	c.server.mu.Unlock()
	if fault != nil {
		if err := fault(query, args); err != nil {
			return nil, err
		}
	}
	if strings.HasPrefix(query, "SELECT User, Host, COALESCE(JSON_EXTRACT(User_attributes, '$.Restrictions')") {
		c.server.mu.Lock()
		defer c.server.mu.Unlock()
		raw, err := c.server.restrictionsJSONLocked(c.username, "%")
		if err != nil {
			return nil, err
		}
		return &fakeSQLValueRows{columns: []string{"User", "Host", "Restrictions"}, values: [][]driver.Value{{c.username, "%", raw}}}, nil
	}
	if strings.HasPrefix(query, "SELECT COALESCE(JSON_EXTRACT(User_attributes, '$.Restrictions')") {
		if len(args) != 2 {
			return nil, fmt.Errorf("restrictions query wants 2 args")
		}
		username, _ := args[0].Value.(string)
		host, _ := args[1].Value.(string)
		rows := &fakeSQLValueRows{columns: []string{"Restrictions", "self"}}
		if !c.server.hasAccount(username, host) {
			return rows, nil
		}
		c.server.mu.Lock()
		defer c.server.mu.Unlock()
		raw, err := c.server.restrictionsJSONLocked(username, host)
		if err != nil {
			return nil, err
		}
		rows.values = [][]driver.Value{{raw, username == c.username && host == "%"}}
		return rows, nil
	}
	if strings.HasPrefix(query, "SELECT PRIVILEGE_TYPE FROM information_schema.SCHEMA_PRIVILEGES ") {
		if len(args) != 3 {
			return nil, fmt.Errorf("schema grants query wants 3 args")
		}
		account, _ := args[0].Value.(string)
		database, _ := args[1].Value.(string)
		accounts, err := parseAccountList(account)
		if err != nil || len(accounts) != 1 {
			return nil, fmt.Errorf("invalid schema-grants account %q", account)
		}
		rows := &fakeSQLValueRows{columns: []string{"PRIVILEGE_TYPE"}}
		// GRANTEE is compared as binary text, unlike MySQL's account-name
		// matching. Hosts in the metadata are always stored lowercased.
		if accounts[0].host != strings.ToLower(accounts[0].host) {
			return rows, nil
		}
		privileges, _ := c.server.grantRow(database, accounts[0].user, accounts[0].host)
		if slices.Contains(privileges, "ALL PRIVILEGES") {
			privileges = fakeSQLAllPrivileges
		}
		c.server.mu.Lock()
		grantOption := c.server.grantOptions[database][accountKey(accounts[0].user, accounts[0].host)]
		c.server.mu.Unlock()
		if len(privileges) == 0 && grantOption {
			privileges = []string{"USAGE"}
		}
		for _, privilege := range privileges {
			rows.values = append(rows.values, []driver.Value{privilege})
		}
		return rows, nil
	}
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

	case "SELECT @@GLOBAL.lower_case_table_names":
		return &fakeSQLRows{remaining: 1, value: new(int64)}, nil

	case "SELECT @@GLOBAL.partial_revokes":
		c.server.mu.Lock()
		on := c.server.partialRevokes
		c.server.mu.Unlock()
		rows := &fakeSQLRows{remaining: 1}
		if !on {
			rows.value = new(int64)
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

func (s *fakeSQLServer) restrictionsJSONLocked(username, host string) ([]byte, error) {
	var databases []string
	for database := range s.restrictions {
		databases = append(databases, database)
	}
	slices.Sort(databases)
	var restrictions []map[string]any
	for _, database := range databases {
		if privileges := s.restrictions[database][accountKey(username, host)]; len(privileges) > 0 {
			restrictions = append(restrictions, map[string]any{"Database": database, "Privileges": privileges})
		}
	}
	return json.Marshal(restrictions)
}

type fakeSQLRows struct {
	remaining int
	// value, when set, replaces the 1 each row answers.
	value *int64
}

func (r *fakeSQLRows) Columns() []string { return []string{"1"} }
func (r *fakeSQLRows) Close() error      { return nil }
func (r *fakeSQLRows) Next(dest []driver.Value) error {
	if r.remaining == 0 {
		return io.EOF
	}
	r.remaining--
	dest[0] = int64(1)
	if r.value != nil {
		dest[0] = *r.value
	}
	return nil
}

type fakeSQLValueRows struct {
	columns []string
	values  [][]driver.Value
}

func (r *fakeSQLValueRows) Columns() []string { return r.columns }
func (r *fakeSQLValueRows) Close() error      { return nil }
func (r *fakeSQLValueRows) Next(dest []driver.Value) error {
	if len(r.values) == 0 {
		return io.EOF
	}
	copy(dest, r.values[0])
	r.values = r.values[1:]
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

// fakeSQLAllPrivileges is what ALL PRIVILEGES expands to on a schema: every
// database-level privilege except GRANT OPTION, including the routine
// privileges the reconciler's allowlist cannot name. Revoking a
// named privilege from an account that holds ALL must leave the rest,
// exactly as the real server does.
var fakeSQLAllPrivileges = []string{
	"SELECT", "INSERT", "UPDATE", "DELETE", "CREATE", "DROP", "ALTER",
	"INDEX", "REFERENCES", "LOCK TABLES", "SHOW VIEW", "TRIGGER", "EVENT", "EXECUTE",
	"CREATE TEMPORARY TABLES", "CREATE VIEW", "CREATE ROUTINE", "ALTER ROUTINE",
}

// A grantor's partial restriction also limits the GRANTs and REVOKEs it can
// execute. The fake otherwise assumes the admin has the required privileges.
func (s *fakeSQLServer) checkGrantor(username, stmt string) error {
	var match []string
	if match = reGrant.FindStringSubmatch(stmt); match == nil {
		match = reRevokePartial.FindStringSubmatch(stmt)
	}
	if match == nil {
		return nil
	}
	privileges := strings.Split(match[1], ", ")
	if match[1] == "ALL PRIVILEGES" {
		privileges = fakeSQLAllPrivileges
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.partialRevokes {
		for _, privilege := range privileges {
			if slices.Contains(s.restrictions[match[2]][accountKey(username, "%")], privilege) {
				return &mysqldriver.MySQLError{Number: 1044, Message: fmt.Sprintf("access denied for grantor %s on %s", username, match[2])}
			}
		}
	}
	return nil
}

func (s *fakeSQLServer) apply(stmt string) error {
	s.mu.Lock()
	fault := s.statementFault
	s.mu.Unlock()
	if fault != nil {
		if applied, err := fault(stmt); err != nil {
			if applied {
				if aerr := s.applyStatement(stmt); aerr != nil {
					return aerr
				}
			}
			return err
		}
	}
	return s.applyStatement(stmt)
}

func (s *fakeSQLServer) applyStatement(stmt string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.statements = append(s.statements, stmt)

	switch {
	case stmt == "FLUSH PRIVILEGES":
		return nil

	case stmt == "SET GLOBAL partial_revokes = ON":
		s.partialRevokes = true
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
		// A grant row must match exactly one existing schema: with
		// partial_revokes OFF an unescaped `_` is a wildcard over others.
		exact := false
		for name := range s.databases {
			exact = exact || s.grantRowKey(name) == database
		}
		if !exact && len(s.restrictions[database]) == 0 {
			return fmt.Errorf("fake mysql: GRANT on %q, which is not the exact grant row of an existing database", database)
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
			requested := strings.Split(privs, ", ")
			if privs == "ALL PRIVILEGES" && len(s.restrictions[database][key]) == 0 {
				s.grants[database][key] = []string{"ALL PRIVILEGES"}
				continue
			}
			if privs == "ALL PRIVILEGES" {
				requested = fakeSQLAllPrivileges
			}
			for _, privilege := range requested {
				if s.partialRevokes && slices.Contains(s.restrictions[database][key], privilege) {
					s.restrictions[database][key] = slices.DeleteFunc(s.restrictions[database][key], func(p string) bool { return p == privilege })
					continue
				}
				if !slices.Contains(existing, privilege) {
					existing = append(existing, privilege)
				}
			}
			if slices.Contains(existing, "ALL PRIVILEGES") {
				existing = []string{"ALL PRIVILEGES"}
			}
			if len(existing) > 0 {
				s.grants[database][key] = existing
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
		requested := strings.Split(privs, ", ")
		if privs == "ALL PRIVILEGES" {
			requested = fakeSQLAllPrivileges
		}
		for _, a := range accounts {
			key := accountKey(a.user, a.host)
			if _, exists := s.users[key]; !exists {
				if !ignoreUnknown {
					return fmt.Errorf("fake mysql: REVOKE from nonexistent account %q without IGNORE UNKNOWN USER", key)
				}
				continue
			}
			current := append([]string(nil), s.grants[database][key]...)
			if slices.Contains(current, "ALL PRIVILEGES") {
				current = append([]string(nil), fakeSQLAllPrivileges...)
			}
			for _, privilege := range requested {
				if s.partialRevokes && slices.Contains(s.globalPrivileges[key], privilege) && !slices.Contains(current, privilege) {
					if s.restrictions[database] == nil {
						s.restrictions[database] = map[string][]string{}
					}
					if !slices.Contains(s.restrictions[database][key], privilege) {
						s.restrictions[database][key] = append(s.restrictions[database][key], privilege)
					}
				}
				current = slices.DeleteFunc(current, func(p string) bool { return p == privilege })
			}
			if len(current) == 0 {
				delete(s.grants[database], key)
			} else {
				s.grants[database][key] = current
			}
		}
		return nil

	case reDropDatabase.MatchString(stmt):
		m := reDropDatabase.FindStringSubmatch(stmt)
		delete(s.databases, m[1])
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
			delete(s.globalPrivileges, key)
			for _, accounts := range s.grantOptions {
				delete(accounts, key)
			}
			for _, accounts := range s.restrictions {
				delete(accounts, key)
			}
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
