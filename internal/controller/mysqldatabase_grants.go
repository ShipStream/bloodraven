package controller

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"slices"
	"strings"

	corev1 "k8s.io/api/core/v1"

	v1alpha1 "github.com/shipstream/bloodraven/api/v1alpha1"
)

// SCHEMA_PRIVILEGES describes explicit mysql.db rows, not effective global
// or role access. Compare the stored spelling exactly, folding database
// names only on servers that do so themselves.
const schemaGrantPrivilegesQuery = "SELECT PRIVILEGE_TYPE FROM information_schema.SCHEMA_PRIVILEGES WHERE CAST(GRANTEE AS BINARY) = CAST(? AS BINARY) AND CAST(TABLE_SCHEMA AS BINARY) = CAST(IF(@@GLOBAL.lower_case_table_names = 0, ?, LOWER(?)) AS BINARY)"

var schemaPrivilegeNames = []string{
	"SELECT", "INSERT", "UPDATE", "DELETE", "CREATE", "CREATE TEMPORARY TABLES", "CREATE VIEW",
	"DROP", "ALTER", "INDEX", "REFERENCES", "LOCK TABLES", "SHOW VIEW", "TRIGGER", "EVENT",
	"EXECUTE", "CREATE ROUTINE", "ALTER ROUTINE",
}

func schemaGrantPrivileges(ctx context.Context, db *sql.DB, database string, account mysqlAccount) ([]string, error) {
	// MySQL stores host names in lowercase; usernames remain case-sensitive.
	account.host = strings.ToLower(account.host)
	rows, err := db.QueryContext(ctx, schemaGrantPrivilegesQuery, account.String(), database, database)
	if err != nil {
		return nil, fmt.Errorf("read schema grants for %s on %q: %w", account, database, err)
	}
	defer rows.Close()
	held := make(map[string]bool)
	for rows.Next() {
		var privilege string
		if err := rows.Scan(&privilege); err != nil {
			return nil, err
		}
		// A row holding only independently managed GRANT OPTION is USAGE.
		if privilege == "USAGE" {
			continue
		}
		if !slices.Contains(schemaPrivilegeNames, privilege) {
			return nil, fmt.Errorf("unsupported schema privilege %q for %s", privilege, account)
		}
		held[privilege] = true
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	var privileges []string
	for _, name := range schemaPrivilegeNames {
		if held[name] {
			privileges = append(privileges, name)
		}
	}
	return privileges, nil
}

type schemaRevokePlan struct {
	kind, database, username string
	hosts                    []string
	desired                  []string
}

// planSchemaRevokes validates every identifier before the apply executes
// SQL. Privileges to revoke are read at execution time, after GRANT and on
// every retry: a stale or absent grant must never become a partial revoke
// of a privilege the account holds globally.
func planSchemaRevokes(kind string, forms []string, username string, hosts []string, desired []v1alpha1.MysqlPrivilege) ([]schemaRevokePlan, error) {
	if _, err := quoteAccountList(kind, username, hosts); err != nil {
		return nil, err
	}
	var keep []string
	if len(desired) > 0 {
		var err error
		keep, err = v1alpha1.CanonicalPrivileges(kind, desired)
		if err != nil {
			return nil, err
		}
	}
	plans := make([]schemaRevokePlan, 0, len(forms))
	for i, form := range forms {
		if _, err := quoteGrantDatabase(form); err != nil {
			return nil, err
		}
		plan := schemaRevokePlan{kind: kind, database: form, username: username, hosts: hosts}
		if i == 0 {
			plan.desired = keep
		}
		plans = append(plans, plan)
	}
	return plans, nil
}

func (p schemaRevokePlan) run(ctx context.Context, db *sql.DB, exec func(string) error) error {
	if slices.Contains(p.desired, "ALL PRIVILEGES") {
		return nil
	}
	for _, host := range p.hosts {
		held, err := schemaGrantPrivileges(ctx, db, p.database, mysqlAccount{user: p.username, host: host})
		if err != nil {
			return err
		}
		surplus := slices.DeleteFunc(held, func(privilege string) bool { return slices.Contains(p.desired, privilege) })
		if len(surplus) == 0 {
			continue
		}
		stmt, err := renderSchemaRevoke(p.kind, surplus, p.database, p.username, []string{host})
		if err != nil {
			return err
		}
		if err := exec(stmt); err != nil {
			return err
		}
	}
	return nil
}

func renderSchemaRevoke(kind string, privileges []string, database, username string, hosts []string) (string, error) {
	dbIdent, err := quoteGrantDatabase(database)
	if err != nil {
		return "", err
	}
	accounts, err := quoteAccountList(kind, username, hosts)
	if err != nil {
		return "", err
	}
	if len(privileges) == 0 {
		return "", fmt.Errorf("cannot render an empty schema revoke")
	}
	if err := validateSchemaPrivileges(privileges); err != nil {
		return "", err
	}
	return fmt.Sprintf("REVOKE IF EXISTS %s ON %s.* FROM %s IGNORE UNKNOWN USER", strings.Join(privileges, ", "), dbIdent, accounts), nil
}

func validateSchemaPrivileges(privileges []string) error {
	for _, privilege := range privileges {
		if !slices.Contains(schemaPrivilegeNames, privilege) {
			return fmt.Errorf("unsupported schema privilege %q", privilege)
		}
	}
	return nil
}

const tenantGrantorRestrictionsQuery = "SELECT User, Host, COALESCE(JSON_EXTRACT(User_attributes, '$.Restrictions'), JSON_ARRAY()) FROM mysql.user WHERE CONCAT(User, '@', Host) = CURRENT_USER()"

const tenantRestrictionsQuery = "SELECT COALESCE(JSON_EXTRACT(User_attributes, '$.Restrictions'), JSON_ARRAY()), CONCAT(User, '@', Host) = CURRENT_USER() FROM mysql.user WHERE User = ? AND Host = ?"

type tenantRestriction struct {
	Database   string
	Privileges []string
}

func recordedTenantAccounts(st *v1alpha1.MysqlDatabaseStatus) []mysqlAccount {
	var accounts []mysqlAccount
	add := func(username string, hosts []string) {
		if username == "" {
			return
		}
		for _, host := range hosts {
			a := mysqlAccount{user: username, host: strings.ToLower(host)}
			if !slices.Contains(accounts, a) {
				accounts = append(accounts, a)
			}
		}
	}
	add(st.OwnerUser, recordedOwnerHosts(st))
	add(st.PendingOwnerUser, pendingOwnerHosts(st))
	for _, state := range st.AppliedUsers {
		for _, username := range ledgerUsernames(state) {
			add(username, ledgerHostsForName(state, username))
		}
	}
	for _, username := range st.AppliedGrants {
		if username != st.OwnerUser {
			add(username, defaultHosts)
		}
	}
	return accounts
}

// Only impossible escaped schema names identify legacy renderer artifacts.
// Their repair is best-effort: inert artifacts must never gate tenant access
// or deletion. Real-schema restrictions have no provenance and are left to
// their privilege owner beyond the normal desired GRANT.
func (r *MysqlDatabaseReconciler) repairTenantRestrictions(ctx context.Context, db *sql.DB, mdb *v1alpha1.MysqlDatabase) {
	if !mdb.Status.DatabaseCreated {
		return
	}
	warn := func(err error) {
		r.Recorder.Eventf(mdb, corev1.EventTypeWarning, "TenantPrivilegeRestriction",
			"legacy tenant restriction repair incomplete: %v; continuing tenant reconciliation; see bloodraven-doctor playbook 10", err)
	}
	var grantor mysqlAccount
	var raw []byte
	if err := db.QueryRowContext(ctx, tenantGrantorRestrictionsQuery).Scan(&grantor.user, &grantor.host, &raw); err != nil {
		warn(fmt.Errorf("read current grantor restrictions: %w", err))
		return
	}
	var grantorRestrictions []tenantRestriction
	if err := json.Unmarshal(raw, &grantorRestrictions); err != nil {
		warn(fmt.Errorf("decode current grantor restrictions: %w", err))
		return
	}
	var foldNames int
	if err := db.QueryRowContext(ctx, "SELECT @@GLOBAL.lower_case_table_names").Scan(&foldNames); err != nil {
		warn(err)
		return
	}
	key := func(database string) string {
		if foldNames != 0 {
			return strings.ToLower(database)
		}
		return database
	}
	forms := grantDatabaseSpellings(mdb.Spec.DatabaseName)
	formIndex := func(database string) int {
		return slices.IndexFunc(forms, func(form string) bool { return key(form) == key(database) })
	}
	blocked := make(map[string]bool)
	for _, restriction := range grantorRestrictions {
		name := key(restriction.Database)
		i := formIndex(restriction.Database)
		if i < 0 || len(restriction.Privileges) == 0 || blocked[name] {
			continue
		}
		if i == 0 {
			r.Recorder.Eventf(mdb, corev1.EventTypeWarning, "TenantPrivilegeRestriction",
				"grantor %s has a restriction on real schema %q; normal tenant SQL may be denied; use an unrestricted administrator (bloodraven-doctor playbook 10)", grantor, restriction.Database)
			continue
		}
		blocked[name] = true
		r.Recorder.Eventf(mdb, corev1.EventTypeWarning, "TenantPrivilegeRestriction",
			"grantor %s is restricted on %q; skipping legacy repairs there for every account; use an unrestricted administrator (bloodraven-doctor playbook 10)", grantor, restriction.Database)
	}
	for _, account := range recordedTenantAccounts(&mdb.Status) {
		if account == grantor {
			continue
		}
		if _, err := quoteAccount("recorded tenant account", account.user, account.host); err != nil {
			warn(err)
			continue
		}
		var self bool
		err := db.QueryRowContext(ctx, tenantRestrictionsQuery, account.user, account.host).Scan(&raw, &self)
		if errors.Is(err, sql.ErrNoRows) {
			continue
		}
		if err != nil {
			warn(fmt.Errorf("read tenant restrictions for %s: %w", account, err))
			continue
		}
		var restrictions []tenantRestriction
		if err := json.Unmarshal(raw, &restrictions); err != nil {
			warn(fmt.Errorf("decode tenant restrictions for %s: %w", account, err))
			continue
		}
		for _, restriction := range restrictions {
			i := formIndex(restriction.Database)
			if i < 0 || len(restriction.Privileges) == 0 || blocked[key(restriction.Database)] {
				continue
			}
			if i == 0 || self {
				r.Recorder.Eventf(mdb, corev1.EventTypeWarning, "TenantPrivilegeRestriction",
					"%s has a restriction on %q; desired tenant grants may lift their own privileges, but other restrictions require review by the privilege owner (bloodraven-doctor playbook 10)", account, restriction.Database)
				continue
			}
			if err := validateSchemaPrivileges(restriction.Privileges); err != nil {
				warn(err)
				continue
			}
			database, err := quoteGrantDatabase(forms[i])
			if err != nil {
				warn(err)
				continue
			}
			stmt := fmt.Sprintf("GRANT %s ON %s.* TO %s", strings.Join(restriction.Privileges, ", "), database, account)
			if _, err := db.ExecContext(ctx, stmt); err != nil {
				warn(fmt.Errorf("lift legacy escaped restriction for %s: %w", account, err))
				continue
			}
			r.Recorder.Eventf(mdb, corev1.EventTypeNormal, "PartialRevokeLifted", "removed legacy escaped-schema restriction on %q for %s", forms[i], account)
		}
	}
}
