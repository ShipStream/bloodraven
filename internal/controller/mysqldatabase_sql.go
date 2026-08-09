package controller

import (
	"fmt"
	"strings"

	v1alpha1 "github.com/shipstream/bloodraven/api/v1alpha1"
)

// tenantUserHost is the host part of every account a MysqlDatabase touches.
//
// Everything in credentials.go hardcodes '%' (see buildRoles), and tenant
// owners are not the place to unilaterally diverge: a MysqlDatabase owner
// scoped to a pod CIDR while the app/readonly/monitor roles stay on '%'
// would be an inconsistency that looks like a bug. Host scoping is worth
// doing, but as one change across both paths.
const tenantUserHost = "%"

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

// quoteAccount renders a validated username as 'user'@'%'.
func quoteAccount(kind, username string) (string, error) {
	if err := v1alpha1.ValidateMysqlUsername(kind, username); err != nil {
		return "", err
	}
	return fmt.Sprintf("'%s'@'%s'", escapeSingleQuotes(username), tenantUserHost), nil
}

// renderCreateDatabase builds the idempotent CREATE DATABASE statement.
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

// renderOwnerUserStatements builds the CREATE USER IF NOT EXISTS + ALTER USER
// pair, which is the reconcileRole pattern verbatim. Applying both every time
// is what makes password rotation a Secret write and nothing else: ALTER USER
// is a no-op when the password already matches, and the fix when it does not.
func renderOwnerUserStatements(username, password string) ([]string, error) {
	account, err := quoteAccount("spec.owner secret username", username)
	if err != nil {
		return nil, err
	}
	if password == "" {
		return nil, fmt.Errorf("spec.owner secret password must not be empty")
	}
	escaped := escapeSingleQuotes(password)
	return []string{
		fmt.Sprintf("CREATE USER IF NOT EXISTS %s IDENTIFIED BY '%s'", account, escaped),
		fmt.Sprintf("ALTER USER %s IDENTIFIED BY '%s'", account, escaped),
	}, nil
}

// renderGrant builds GRANT <privileges> ON `db`.* TO 'user'@'%'.
//
// It never emits WITH GRANT OPTION, and there is no code path that can:
// the suffix does not exist anywhere in this file, and the privilege text
// comes from the fixed table in the API package rather than from the CR.
func renderGrant(kind string, privileges []v1alpha1.MysqlPrivilege, database, username string) (string, error) {
	privSQL, err := v1alpha1.CanonicalPrivileges(kind, privileges)
	if err != nil {
		return "", err
	}
	dbIdent, err := quoteIdentifier("spec.databaseName", database)
	if err != nil {
		return "", err
	}
	account, err := quoteAccount(kind, username)
	if err != nil {
		return "", err
	}
	return fmt.Sprintf("GRANT %s ON %s.* TO %s", strings.Join(privSQL, ", "), dbIdent, account), nil
}

// renderRevokeAll builds REVOKE ALL PRIVILEGES ... ON `db`.* FROM 'user'@'%'.
// IF EXISTS keeps it idempotent when the grant was already removed by hand.
func renderRevokeAll(kind, database, username string) (string, error) {
	dbIdent, err := quoteIdentifier("spec.databaseName", database)
	if err != nil {
		return "", err
	}
	account, err := quoteAccount(kind, username)
	if err != nil {
		return "", err
	}
	return fmt.Sprintf("REVOKE IF EXISTS ALL PRIVILEGES ON %s.* FROM %s", dbIdent, account), nil
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

// renderDropUser builds the DROP USER statement for the owner. It is never
// rendered for a spec.grants[] entry: those principals are shared, and this
// CRD did not create them.
func renderDropUser(username string) (string, error) {
	account, err := quoteAccount("spec.owner secret username", username)
	if err != nil {
		return "", err
	}
	return "DROP USER IF EXISTS " + account, nil
}

// grantUserExistsQuery is the parameterized existence check for a
// spec.grants[] principal. Parameterized on purpose: this is the one place a
// username is compared rather than rendered, so there is no reason for it to
// go anywhere near a format string.
const grantUserExistsQuery = "SELECT 1 FROM mysql.user WHERE user = ? AND host = ?"
