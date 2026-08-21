package controller

import (
	"fmt"
	"strings"

	v1alpha1 "github.com/shipstream/bloodraven/api/v1alpha1"
)

// tenantUserHost is the host part of every account a MysqlDatabase touches
// that carries no explicit hosts list: spec.grants[] principals (created
// elsewhere, by contract on '%'), and the owner and users[] entries when
// their hosts list is omitted. It matches every account in credentials.go.
const tenantUserHost = v1alpha1.DefaultMysqlHost

// mysqlAccount is one 'user'@'host' pair. A MysqlDatabase principal (owner
// or users[] entry) is one username over a hosts list, so every statement
// that touches a principal is rendered over all of its accounts at once —
// MySQL's account-management statements take account lists and are atomic,
// which keeps a multi-host rotation a single statement.
type mysqlAccount struct {
	user string
	host string
}

// String renders the account as 'user'@'host'; callers must have validated
// both parts (quoteAccount does).
func (a mysqlAccount) String() string {
	return fmt.Sprintf("'%s'@'%s'", escapeSingleQuotes(a.user), escapeSingleQuotes(a.host))
}

// defaultHosts is the hosts list for principals that have none: ["%"].
var defaultHosts = []string{tenantUserHost}

// The rendering helpers below all validate before they format. That order is
// the contract: a database name or username that fails validation must never
// reach a fmt.Sprintf that builds SQL, because escaping is a mitigation and
// rejection is a guarantee. escapeSingleQuotes is still applied on top,
// belt-and-braces, even though the validated character sets cannot produce a
// quote.

// quoteIdentifier renders a validated identifier as a backtick-quoted MySQL
// identifier. Callers must have validated it first; the check is repeated
// here so that no path can reach the format string unvalidated.
func quoteIdentifier(kind, name string) (string, error) {
	if err := v1alpha1.ValidateMysqlIdentifier(kind, name); err != nil {
		return "", err
	}
	return "`" + name + "`", nil
}

// quoteAccount renders a validated username and host as 'user'@'host'.
func quoteAccount(kind, username, host string) (string, error) {
	if err := v1alpha1.ValidateMysqlUsername(kind, username); err != nil {
		return "", err
	}
	if err := v1alpha1.ValidateMysqlHost(kind+" host", host); err != nil {
		return "", err
	}
	return mysqlAccount{user: username, host: host}.String(), nil
}

// quoteAccountList renders a username over its hosts as a comma-separated
// account list, validating every part first. An empty hosts list is a
// programming error, not a default: callers resolve defaults explicitly.
func quoteAccountList(kind, username string, hosts []string) (string, error) {
	if len(hosts) == 0 {
		return "", fmt.Errorf("%s: no hosts to render", kind)
	}
	parts := make([]string, 0, len(hosts))
	for _, h := range hosts {
		acct, err := quoteAccount(kind, username, h)
		if err != nil {
			return "", err
		}
		parts = append(parts, acct)
	}
	return strings.Join(parts, ", "), nil
}

// quoteAccountsIdentifiedBy renders `'u'@'h1' IDENTIFIED BY 'p', 'u'@'h2'
// IDENTIFIED BY 'p', …` — the per-account auth clause CREATE USER and ALTER
// USER require when more than one account is named.
func quoteAccountsIdentifiedBy(kind, username string, hosts []string, escapedPassword string) (string, error) {
	if len(hosts) == 0 {
		return "", fmt.Errorf("%s: no hosts to render", kind)
	}
	parts := make([]string, 0, len(hosts))
	for _, h := range hosts {
		acct, err := quoteAccount(kind, username, h)
		if err != nil {
			return "", err
		}
		parts = append(parts, fmt.Sprintf("%s IDENTIFIED BY '%s'", acct, escapedPassword))
	}
	return strings.Join(parts, ", "), nil
}

// renderCreateDatabase builds the idempotent CREATE DATABASE statement.
// The reconciler separately refuses to run it against a schema it did not
// create (schemaExistsQuery); IF NOT EXISTS stays so the create itself is
// idempotent across retries.
func renderCreateDatabase(database, characterSet, collation string) (string, error) {
	dbIdent, err := quoteIdentifier("spec.databaseName", database)
	if err != nil {
		return "", err
	}
	if err := v1alpha1.ValidateMysqlIdentifier("spec.characterSet", characterSet); err != nil {
		return "", err
	}
	if err := v1alpha1.ValidateMysqlIdentifier("spec.collation", collation); err != nil {
		return "", err
	}
	return fmt.Sprintf("CREATE DATABASE IF NOT EXISTS %s CHARACTER SET %s COLLATE %s",
		dbIdent, characterSet, collation), nil
}

// renderAlterDatabase builds ALTER DATABASE ... CHARACTER SET ... COLLATE ...
//
// It exists because characterSet/collation are mutable spec fields, and
// CREATE DATABASE IF NOT EXISTS is a no-op against an existing schema: an
// edit would report Ready while MySQL kept the old defaults. ALTER DATABASE
// only changes the schema defaults — never existing tables — so running it
// on every apply makes the fields true desired state without a migration.
func renderAlterDatabase(database, characterSet, collation string) (string, error) {
	dbIdent, err := quoteIdentifier("spec.databaseName", database)
	if err != nil {
		return "", err
	}
	if err := v1alpha1.ValidateMysqlIdentifier("spec.characterSet", characterSet); err != nil {
		return "", err
	}
	if err := v1alpha1.ValidateMysqlIdentifier("spec.collation", collation); err != nil {
		return "", err
	}
	return fmt.Sprintf("ALTER DATABASE %s CHARACTER SET %s COLLATE %s",
		dbIdent, characterSet, collation), nil
}

// renderOwnerUserStatements builds the CREATE USER IF NOT EXISTS + ALTER USER
// pair, which is the reconcileRole pattern verbatim. Applying both every time
// is what makes password rotation a Secret write and nothing else: ALTER USER
// is a no-op when the password already matches, and the fix when it does not.
func renderOwnerUserStatements(username, password string, hosts []string) ([]string, error) {
	if password == "" {
		return nil, fmt.Errorf("spec.owner secret password must not be empty")
	}
	escaped := escapeSingleQuotes(password)
	accounts, err := quoteAccountsIdentifiedBy("spec.owner secret username", username, hosts, escaped)
	if err != nil {
		return nil, err
	}
	return []string{
		"CREATE USER IF NOT EXISTS " + accounts,
		"ALTER USER " + accounts,
	}, nil
}

// renderTenantUserStatements is renderOwnerUserStatements for a spec.users[]
// entry: the same CREATE USER IF NOT EXISTS + ALTER USER pair, with the
// entry's resource limits appended to the ALTER. The WITH clause is rendered
// on every apply, with omitted limits as 0 (MySQL's "no account-level cap"):
// removing a limit from the spec must clear it in MySQL, or resourceLimits would be
// desired state on the way in but not on the way out. The limit values are
// int32s formatted with %d — no caller-controlled text reaches the format
// string.
//
// The WITH clause is statement-wide in ALTER USER, so one statement sets the
// same limits on every host's account; MySQL enforces them per account.
func renderTenantUserStatements(kind, username, password string, hosts []string, limits *v1alpha1.MysqlUserResourceLimits) ([]string, error) {
	if password == "" {
		return nil, fmt.Errorf("%s password must not be empty", kind)
	}
	var maxConns, maxQueries int32
	if limits != nil {
		maxConns = limits.MaxUserConnections
		maxQueries = limits.MaxQueriesPerHour
	}
	escaped := escapeSingleQuotes(password)
	accounts, err := quoteAccountsIdentifiedBy(kind, username, hosts, escaped)
	if err != nil {
		return nil, err
	}
	return []string{
		"CREATE USER IF NOT EXISTS " + accounts,
		fmt.Sprintf("ALTER USER %s WITH MAX_USER_CONNECTIONS %d MAX_QUERIES_PER_HOUR %d",
			accounts, maxConns, maxQueries),
	}, nil
}

// renderGrant builds GRANT <privileges> ON `db`.* TO 'user'@'%'.
//
// It never emits WITH GRANT OPTION, and there is no code path that can:
// the suffix does not exist anywhere in this file, and the privilege text
// comes from the fixed table in the API package rather than from the CR.
func renderGrant(kind string, privileges []v1alpha1.MysqlPrivilege, database, username string, hosts []string) (string, error) {
	privSQL, err := v1alpha1.CanonicalPrivileges(kind, privileges)
	if err != nil {
		return "", err
	}
	dbIdent, err := quoteIdentifier("spec.databaseName", database)
	if err != nil {
		return "", err
	}
	accounts, err := quoteAccountList(kind, username, hosts)
	if err != nil {
		return "", err
	}
	return fmt.Sprintf("GRANT %s ON %s.* TO %s", strings.Join(privSQL, ", "), dbIdent, accounts), nil
}

// renderRevokeSurplus builds the REVOKE that removes every allowlist
// privilege a principal holds on this database but the spec no longer
// declares — or "" when there is no surplus to revoke.
//
// It is the second half of grant-then-revoke: the desired set is GRANTed
// first, so a failure between the two statements leaves the principal with
// the union of old and new privileges (over-granted for one requeue
// interval) rather than with zero privileges on its own live database.
// IF EXISTS tolerates a surplus entry the account does not actually hold;
// IGNORE UNKNOWN USER tolerates a missing account, exactly as the
// revoke-all path does.
func renderRevokeSurplus(kind string, desired []v1alpha1.MysqlPrivilege, database, username string, hosts []string) (string, error) {
	desiredSet := make(map[v1alpha1.MysqlPrivilege]bool, len(desired))
	for _, p := range desired {
		desiredSet[p] = true
	}
	// ALL PRIVILEGES is the whole allowlist: nothing can be surplus once
	// it is granted. (Validation rejects combining it with other entries.)
	if desiredSet[v1alpha1.PrivilegeAllPrivileges] {
		return "", nil
	}

	var surplus []string
	for _, name := range v1alpha1.AllowedPrivilegeNames() {
		// ALL PRIVILEGES is a synonym for the whole set, not a listable
		// member: MySQL rejects it combined with other entries in a
		// REVOKE, and revoking every concrete privilege already empties an
		// ALL PRIVILEGES grant.
		if name == string(v1alpha1.PrivilegeAllPrivileges) {
			continue
		}
		if !desiredSet[v1alpha1.MysqlPrivilege(name)] {
			surplus = append(surplus, name)
		}
	}
	if len(surplus) == 0 {
		return "", nil
	}

	dbIdent, err := quoteIdentifier("spec.databaseName", database)
	if err != nil {
		return "", err
	}
	accounts, err := quoteAccountList(kind, username, hosts)
	if err != nil {
		return "", err
	}
	return fmt.Sprintf("REVOKE IF EXISTS %s ON %s.* FROM %s IGNORE UNKNOWN USER",
		strings.Join(surplus, ", "), dbIdent, accounts), nil
}

// renderRevokeAll builds REVOKE ALL PRIVILEGES ... ON `db`.* FROM 'user'@'%'.
// IF EXISTS keeps it idempotent when the grant was already removed by hand;
// IGNORE UNKNOWN USER keeps it from erroring when the account itself does
// not exist. The second clause is load-bearing on the delete path: a CR that
// failed with GrantUserMissing still lists that user in spec.grants[], and
// without IGNORE UNKNOWN USER the revoke would error (MySQL 1141), the
// finalizer would never release, and the CR would wedge in Deleting forever.
// Verified against MySQL 9.7: IF EXISTS alone does not cover a missing
// account, only a missing grant.
func renderRevokeAll(kind, database, username string, hosts []string) (string, error) {
	dbIdent, err := quoteIdentifier("spec.databaseName", database)
	if err != nil {
		return "", err
	}
	accounts, err := quoteAccountList(kind, username, hosts)
	if err != nil {
		return "", err
	}
	return fmt.Sprintf("REVOKE IF EXISTS ALL PRIVILEGES ON %s.* FROM %s IGNORE UNKNOWN USER", dbIdent, accounts), nil
}

// renderDropDatabase builds the DROP DATABASE statement. Only ever reached
// through deletionPolicy: Delete.
func renderDropDatabase(database string) (string, error) {
	dbIdent, err := quoteIdentifier("spec.databaseName", database)
	if err != nil {
		return "", err
	}
	return "DROP DATABASE IF EXISTS " + dbIdent, nil
}

// renderDropUser builds the DROP USER statement for the owner or a
// spec.users[] principal — the accounts this CRD created. It is never
// rendered for a spec.grants[] entry: those principals are shared, and this
// CRD did not create them.
func renderDropUser(kind, username string, hosts []string) (string, error) {
	accounts, err := quoteAccountList(kind, username, hosts)
	if err != nil {
		return "", err
	}
	return "DROP USER IF EXISTS " + accounts, nil
}

// grantUserExistsQuery is the parameterized existence check for one
// 'user'@'host' account — the spec.grants[] precondition and the owner and
// users[] adoption gates. Parameterized on purpose: this is the one place a
// username is compared rather than rendered, so there is no reason for it to
// go anywhere near a format string.
const grantUserExistsQuery = "SELECT 1 FROM mysql.user WHERE user = ? AND host = ?"

// schemaExistsQuery is the parameterized existence check for
// spec.databaseName. It gates adoption: a schema that already exists is
// only this CR's to manage when status.databaseCreated says this CR
// created it. Parameterized for the same reason grantUserExistsQuery is.
const schemaExistsQuery = "SELECT 1 FROM information_schema.SCHEMATA WHERE SCHEMA_NAME = ?"
