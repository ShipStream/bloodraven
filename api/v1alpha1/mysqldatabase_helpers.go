package v1alpha1

import (
	"fmt"
	"regexp"
	"strings"
)

// Defaults applied when the corresponding spec field is empty. The CRD
// carries kubebuilder defaults for the same values; these exist so that
// objects constructed in Go (tests, older CRs stored before the default was
// added) resolve identically.
const (
	// DefaultDatabaseCharacterSet is the default value of
	// spec.characterSet.
	DefaultDatabaseCharacterSet = "utf8mb4"
	// DefaultDatabaseCollation is the default value of spec.collation.
	DefaultDatabaseCollation = "utf8mb4_unicode_ci"
)

// mysqlIdentifierPattern constrains schema names, character sets and
// collations. These values are interpolated into DDL; the pattern is the
// contract that keeps anything with quoting significance from getting near
// a format string. It is deliberately narrower than what MySQL itself
// accepts.
var mysqlIdentifierPattern = regexp.MustCompile(`^[A-Za-z0-9_]{1,64}$`)

// mysqlSystemSchemas are the schemas MySQL itself manages. They are
// rejected as tenant database names, case-insensitively: on a typical
// deployment `databaseName: mysql` would grant the tenant owner ALL
// PRIVILEGES on the grant tables, and `sys` is even droppable. The CEL
// rule on spec.databaseName enforces the same list at admission; this map
// is the second check, and the one that also runs for objects the API
// server never validated (tests, pre-rule storage).
var mysqlSystemSchemas = map[string]bool{
	"mysql":              true,
	"sys":                true,
	"information_schema": true,
	"performance_schema": true,
}

// IsSystemSchema reports whether name is a MySQL system schema,
// case-insensitively. Exported for the CEL-sync test: the admission rule
// and this map must not drift.
func IsSystemSchema(name string) bool {
	return mysqlSystemSchemas[strings.ToLower(name)]
}

// mysqlUsernamePattern constrains MySQL account names. MySQL caps usernames
// at 32 characters; the leading character is restricted further so a name
// cannot start with a separator.
var mysqlUsernamePattern = regexp.MustCompile(`^[A-Za-z0-9_][A-Za-z0-9_.$-]{0,31}$`)

// canonicalPrivileges is the privilege allowlist in canonical emission
// order. Rendering reads the SQL text out of this table rather than echoing
// the caller's string, so even a validation bypass could not smuggle text
// into a GRANT statement.
//
// GRANT OPTION is absent by design and must stay absent: an owner that can
// grant is an owner that can escape its own database.
var canonicalPrivileges = []struct {
	value MysqlPrivilege
	sql   string
}{
	{PrivilegeAllPrivileges, "ALL PRIVILEGES"},
	{PrivilegeSelect, "SELECT"},
	{PrivilegeInsert, "INSERT"},
	{PrivilegeUpdate, "UPDATE"},
	{PrivilegeDelete, "DELETE"},
	{PrivilegeCreate, "CREATE"},
	{PrivilegeDrop, "DROP"},
	{PrivilegeAlter, "ALTER"},
	{PrivilegeIndex, "INDEX"},
	{PrivilegeReferences, "REFERENCES"},
	{PrivilegeLockTables, "LOCK TABLES"},
	{PrivilegeShowView, "SHOW VIEW"},
	{PrivilegeTrigger, "TRIGGER"},
	{PrivilegeEvent, "EVENT"},
	{PrivilegeExecute, "EXECUTE"},
}

// ValidateMysqlIdentifier rejects any value that must not be interpolated
// into DDL. kind names the field for the error message.
func ValidateMysqlIdentifier(kind, value string) error {
	if value == "" {
		return fmt.Errorf("%s must not be empty", kind)
	}
	if !mysqlIdentifierPattern.MatchString(value) {
		return fmt.Errorf("%s %q is not a valid MySQL identifier (allowed: 1-64 characters matching [A-Za-z0-9_])", kind, value)
	}
	return nil
}

// ValidateMysqlUsername rejects any account name that must not be
// interpolated into SQL.
func ValidateMysqlUsername(kind, value string) error {
	if value == "" {
		return fmt.Errorf("%s must not be empty", kind)
	}
	if !mysqlUsernamePattern.MatchString(value) {
		return fmt.Errorf("%s %q is not a valid MySQL username (allowed: 1-32 characters matching [A-Za-z0-9_][A-Za-z0-9_.$-]*)", kind, value)
	}
	return nil
}

// CanonicalPrivileges validates a privilege list against the allowlist and
// returns the SQL text for each, deduplicated and in canonical order.
//
// It returns an error rather than filtering, because silently dropping an
// unrecognised privilege would turn a typo into a quietly under-privileged
// tenant.
func CanonicalPrivileges(kind string, privs []MysqlPrivilege) ([]string, error) {
	if len(privs) == 0 {
		return nil, fmt.Errorf("%s must list at least one privilege", kind)
	}

	seen := make(map[MysqlPrivilege]bool, len(privs))
	for _, p := range privs {
		if !isAllowedPrivilege(p) {
			return nil, fmt.Errorf("%s contains %q, which is not in the allowed privilege set %s",
				kind, string(p), strings.Join(AllowedPrivilegeNames(), ", "))
		}
		seen[p] = true
	}

	if seen[PrivilegeAllPrivileges] && len(seen) > 1 {
		return nil, fmt.Errorf("%s must not combine %q with other privileges", kind, PrivilegeAllPrivileges)
	}

	out := make([]string, 0, len(seen))
	for _, entry := range canonicalPrivileges {
		if seen[entry.value] {
			out = append(out, entry.sql)
		}
	}
	return out, nil
}

// AllowedPrivilegeNames returns the allowlist in canonical order. Used in
// error messages and to keep the CEL rule and the Go check in sync in tests.
func AllowedPrivilegeNames() []string {
	out := make([]string, 0, len(canonicalPrivileges))
	for _, entry := range canonicalPrivileges {
		out = append(out, string(entry.value))
	}
	return out
}

func isAllowedPrivilege(p MysqlPrivilege) bool {
	for _, entry := range canonicalPrivileges {
		if entry.value == p {
			return true
		}
	}
	return false
}

// EffectiveCharacterSet returns spec.characterSet or the default.
func (s *MysqlDatabaseSpec) EffectiveCharacterSet() string {
	if s.CharacterSet == "" {
		return DefaultDatabaseCharacterSet
	}
	return s.CharacterSet
}

// EffectiveCollation returns spec.collation or the default.
func (s *MysqlDatabaseSpec) EffectiveCollation() string {
	if s.Collation == "" {
		return DefaultDatabaseCollation
	}
	return s.Collation
}

// EffectiveOwnerPrivileges returns spec.owner.privileges or the default.
func (s *MysqlDatabaseSpec) EffectiveOwnerPrivileges() []MysqlPrivilege {
	if len(s.Owner.Privileges) == 0 {
		return []MysqlPrivilege{PrivilegeAllPrivileges}
	}
	return s.Owner.Privileges
}

// EffectiveDeletionPolicy returns spec.deletionPolicy or Retain.
//
// The zero value resolving to Retain is intentional and load-bearing: a CR
// stored before the field existed, or one deserialized by a client that
// dropped it, must never be interpreted as permission to DROP DATABASE.
func (s *MysqlDatabaseSpec) EffectiveDeletionPolicy() MysqlDatabaseDeletionPolicy {
	if s.DeletionPolicy == MysqlDatabaseDelete {
		return MysqlDatabaseDelete
	}
	return MysqlDatabaseRetain
}

// Validate checks every field that ends up in SQL, before any of it reaches
// a format string. The API server enforces the same constraints through the
// CRD schema; this is the second of the two independent checks, and the one
// that also covers the usernames, which arrive from Secrets and so cannot be
// validated by the API server at admission time. userUsernames maps each
// spec.users[] entry's secretName to the username its Secret currently
// carries; entries whose Secret has not resolved yet must not reach this
// function (the reconciler parks them Pending first).
//
// It returns the first problem found; callers surface it as a Failed phase
// rather than retrying, because none of these resolve on their own.
func (s *MysqlDatabaseSpec) Validate(ownerUsername string, userUsernames map[string]string) error {
	if err := ValidateMysqlIdentifier("spec.databaseName", s.DatabaseName); err != nil {
		return err
	}
	if IsSystemSchema(s.DatabaseName) {
		return fmt.Errorf("spec.databaseName %q is a MySQL system schema; tenant databases must use their own schema name", s.DatabaseName)
	}
	if err := ValidateMysqlIdentifier("spec.characterSet", s.EffectiveCharacterSet()); err != nil {
		return err
	}
	if err := ValidateMysqlIdentifier("spec.collation", s.EffectiveCollation()); err != nil {
		return err
	}
	if err := ValidateMysqlUsername("spec.owner secret username", ownerUsername); err != nil {
		return err
	}
	if _, err := CanonicalPrivileges("spec.owner.privileges", s.EffectiveOwnerPrivileges()); err != nil {
		return err
	}

	// users[] usernames are collected first so that grants[] can be checked
	// against them: a grants[] entry naming a users[] principal would make
	// two spec lists manage one account's privileges, with the last apply
	// winning silently.
	userSeen := make(map[string]string, len(s.Users))
	for i, u := range s.Users {
		field := fmt.Sprintf("spec.users[%d] secret username", i)
		username, ok := userUsernames[u.SecretName]
		if !ok {
			return fmt.Errorf("spec.users[%d] (secret %q) has no resolved username; this is a reconciler bug — unresolved Secrets must stay Pending", i, u.SecretName)
		}
		if err := ValidateMysqlUsername(field, username); err != nil {
			return err
		}
		if username == ownerUsername {
			return fmt.Errorf("%s %q is the owner username; the owner is declared via spec.owner", field, username)
		}
		if prior, dup := userSeen[username]; dup {
			return fmt.Errorf("%s %q is already the username of the entry for secret %q; each users[] entry must manage a distinct account", field, username, prior)
		}
		userSeen[username] = u.SecretName
		// The Go-side re-check of the CEL rule: ALL PRIVILEGES is the
		// owner's shape, not a users[] privilege — and CEL never sees
		// objects constructed in Go or stored before the rule existed.
		for _, p := range u.Privileges {
			if p == PrivilegeAllPrivileges {
				return fmt.Errorf("spec.users[%d].privileges must not include %q; an all-privileges principal is the owner's shape", i, PrivilegeAllPrivileges)
			}
		}
		if _, err := CanonicalPrivileges(fmt.Sprintf("spec.users[%d].privileges", i), u.Privileges); err != nil {
			return err
		}
	}

	seen := make(map[string]bool, len(s.Grants))
	for i, g := range s.Grants {
		field := fmt.Sprintf("spec.grants[%d].username", i)
		if err := ValidateMysqlUsername(field, g.Username); err != nil {
			return err
		}
		if g.Username == ownerUsername {
			return fmt.Errorf("%s %q is the owner username; declare owner privileges via spec.owner.privileges", field, g.Username)
		}
		if secretName, isUser := userSeen[g.Username]; isUser {
			return fmt.Errorf("%s %q is the username of spec.users[] entry %q; declare its privileges on that entry instead", field, g.Username, secretName)
		}
		if seen[g.Username] {
			return fmt.Errorf("%s %q is listed more than once", field, g.Username)
		}
		seen[g.Username] = true
		if _, err := CanonicalPrivileges(fmt.Sprintf("spec.grants[%d].privileges", i), g.Privileges); err != nil {
			return err
		}
	}
	return nil
}
