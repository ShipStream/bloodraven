package v1alpha1

import (
	"strings"
	"testing"
)

func TestValidateMysqlIdentifierRejectsInjection(t *testing.T) {
	// These are rejection cases, not escaping cases. The contract is that
	// nothing here reaches a fmt.Sprintf that builds SQL.
	cases := []struct {
		name  string
		value string
	}{
		{"empty", ""},
		{"backtick", "acme`"},
		{"backtick injection", "acme`; DROP DATABASE `other"},
		{"single quote", "acme'"},
		{"double quote", `acme"`},
		{"backslash", `acme\`},
		{"semicolon", "acme; DROP DATABASE other"},
		{"space", "acme wms"},
		{"comment", "acme-- "},
		{"block comment", "acme/*x*/"},
		{"newline", "acme\nDROP DATABASE other"},
		{"null byte", "acme\x00"},
		{"dot", "acme.wms"},
		{"hyphen", "acme-wms"},
		{"wildcard", "acme%"},
		{"too long", strings.Repeat("a", 65)},
		{"unicode", "acmé"},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if err := ValidateMysqlIdentifier("spec.databaseName", tc.value); err == nil {
				t.Fatalf("ValidateMysqlIdentifier(%q) = nil, want rejection", tc.value)
			}
		})
	}
}

func TestValidateMysqlIdentifierAcceptsValid(t *testing.T) {
	for _, value := range []string{"a", "acme_wms", "ACME123", "_leading", "utf8mb4", "utf8mb4_unicode_ci", strings.Repeat("a", 64)} {
		if err := ValidateMysqlIdentifier("spec.databaseName", value); err != nil {
			t.Fatalf("ValidateMysqlIdentifier(%q) = %v, want nil", value, err)
		}
	}
}

func TestValidateMysqlUsernameRejectsInjection(t *testing.T) {
	cases := []struct {
		name  string
		value string
	}{
		{"empty", ""},
		{"single quote", "maester'"},
		{"quote injection", "maester'@'%' WITH GRANT OPTION -- "},
		{"backslash", `maester\`},
		{"at sign", "maester@host"},
		{"space", "mae ster"},
		{"percent", "maester%"},
		{"leading hyphen", "-maester"},
		{"leading dot", ".maester"},
		{"too long", strings.Repeat("u", 33)},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if err := ValidateMysqlUsername("spec.grants[0].username", tc.value); err == nil {
				t.Fatalf("ValidateMysqlUsername(%q) = nil, want rejection", tc.value)
			}
		})
	}
}

func TestValidateMysqlUsernameAcceptsValid(t *testing.T) {
	for _, value := range []string{"maester", "acme_app", "app-1", "app.reader", "svc$cdc", strings.Repeat("u", 32)} {
		if err := ValidateMysqlUsername("spec.grants[0].username", value); err != nil {
			t.Fatalf("ValidateMysqlUsername(%q) = %v, want nil", value, err)
		}
	}
}

func TestCanonicalPrivilegesRejectsOutsideAllowlist(t *testing.T) {
	cases := []struct {
		name  string
		privs []MysqlPrivilege
	}{
		{"empty", nil},
		{"grant option", []MysqlPrivilege{"GRANT OPTION"}},
		{"all with grant option", []MysqlPrivilege{"ALL PRIVILEGES WITH GRANT OPTION"}},
		{"super", []MysqlPrivilege{"SUPER"}},
		{"file", []MysqlPrivilege{"FILE"}},
		{"create user", []MysqlPrivilege{"CREATE USER"}},
		{"replication slave", []MysqlPrivilege{"REPLICATION SLAVE"}},
		{"lowercase", []MysqlPrivilege{"select"}},
		{"trailing sql", []MysqlPrivilege{"SELECT, INSERT"}},
		{"injection", []MysqlPrivilege{"SELECT ON *.* TO 'evil'@'%'; GRANT ALL"}},
		{"valid plus invalid", []MysqlPrivilege{PrivilegeSelect, "SUPER"}},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if _, err := CanonicalPrivileges("spec.owner.privileges", tc.privs); err == nil {
				t.Fatalf("CanonicalPrivileges(%v) = nil error, want rejection", tc.privs)
			}
		})
	}
}

func TestCanonicalPrivilegesRejectsAllCombinedWithOthers(t *testing.T) {
	_, err := CanonicalPrivileges("spec.owner.privileges", []MysqlPrivilege{PrivilegeAllPrivileges, PrivilegeSelect})
	if err == nil {
		t.Fatal("CanonicalPrivileges(ALL PRIVILEGES + SELECT) = nil error, want rejection")
	}
	if !strings.Contains(err.Error(), "must not combine") {
		t.Fatalf("error = %q, want a combine-rejection message", err)
	}
}

func TestCanonicalPrivilegesNormalizesOrderAndDuplicates(t *testing.T) {
	got, err := CanonicalPrivileges("spec.grants[0].privileges", []MysqlPrivilege{
		PrivilegeDelete, PrivilegeSelect, PrivilegeDelete,
	})
	if err != nil {
		t.Fatalf("CanonicalPrivileges() error = %v", err)
	}
	want := []string{"SELECT", "DELETE"}
	if len(got) != len(want) {
		t.Fatalf("CanonicalPrivileges() = %v, want %v", got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("CanonicalPrivileges() = %v, want %v", got, want)
		}
	}
}

func TestAllowedPrivilegeNamesNeverIncludesGrantOption(t *testing.T) {
	for _, name := range AllowedPrivilegeNames() {
		if strings.Contains(strings.ToUpper(name), "GRANT OPTION") {
			t.Fatalf("privilege allowlist contains %q; an owner that can grant is an owner that can escape its database", name)
		}
	}
}

func TestEffectiveDeletionPolicyDefaultsToRetain(t *testing.T) {
	// The zero value must resolve to Retain. A CR stored before the field
	// existed, or one deserialized by a client that dropped it, must never
	// be read as permission to DROP DATABASE.
	// Only the exact "Delete" string opts into destruction; every near miss
	// falls back to Retain.
	cases := map[MysqlDatabaseDeletionPolicy]MysqlDatabaseDeletionPolicy{
		"":                  MysqlDatabaseRetain,
		MysqlDatabaseRetain: MysqlDatabaseRetain,
		MysqlDatabaseDelete: MysqlDatabaseDelete,
		"delete":            MysqlDatabaseRetain,
		"DELETE":            MysqlDatabaseRetain,
		"Delete ":           MysqlDatabaseRetain,
		"Destroy":           MysqlDatabaseRetain,
	}
	for in, want := range cases {
		spec := MysqlDatabaseSpec{DeletionPolicy: in}
		if got := spec.EffectiveDeletionPolicy(); got != want {
			t.Fatalf("EffectiveDeletionPolicy(%q) = %q, want %q", in, got, want)
		}
	}
}

func TestEffectiveDefaults(t *testing.T) {
	var spec MysqlDatabaseSpec
	if got := spec.EffectiveCharacterSet(); got != DefaultDatabaseCharacterSet {
		t.Fatalf("EffectiveCharacterSet() = %q, want %q", got, DefaultDatabaseCharacterSet)
	}
	if got := spec.EffectiveCollation(); got != DefaultDatabaseCollation {
		t.Fatalf("EffectiveCollation() = %q, want %q", got, DefaultDatabaseCollation)
	}
	privs := spec.EffectiveOwnerPrivileges()
	if len(privs) != 1 || privs[0] != PrivilegeAllPrivileges {
		t.Fatalf("EffectiveOwnerPrivileges() = %v, want [ALL PRIVILEGES]", privs)
	}
}

func TestSpecValidate(t *testing.T) {
	valid := func() MysqlDatabaseSpec {
		return MysqlDatabaseSpec{
			GroupRef:     LocalGroupRef{Name: "main"},
			DatabaseName: "acme_wms",
			Owner:        MysqlDatabaseOwner{SecretName: "acme-mysql-owner"},
			Grants: []MysqlDatabaseGrant{
				{Username: "maester", Privileges: []MysqlPrivilege{PrivilegeSelect, PrivilegeDelete}},
			},
		}
	}

	base := valid()
	if err := base.Validate("acme_app", nil); err != nil {
		t.Fatalf("Validate() on a valid spec = %v", err)
	}

	cases := []struct {
		name          string
		mutate        func(*MysqlDatabaseSpec)
		ownerUser     string
		userUsernames map[string]string
		wantField     string
	}{
		{
			// The users[] username arrives from a Secret, like the owner's.
			name: "bad users username from secret",
			mutate: func(s *MysqlDatabaseSpec) {
				s.Users = []MysqlDatabaseUser{{SecretName: "support-ro-mysql", Privileges: []MysqlPrivilege{PrivilegeSelect}}}
			},
			ownerUser:     "acme_app",
			userUsernames: map[string]string{"support-ro-mysql": "evil'@'%"},
			wantField:     "spec.users[0] secret username",
		},
		{
			name: "users username equals owner username",
			mutate: func(s *MysqlDatabaseSpec) {
				s.Users = []MysqlDatabaseUser{{SecretName: "support-ro-mysql", Privileges: []MysqlPrivilege{PrivilegeSelect}}}
			},
			ownerUser:     "acme_app",
			userUsernames: map[string]string{"support-ro-mysql": "acme_app"},
			wantField:     "spec.users[0] secret username",
		},
		{
			name: "two users entries resolve to one username",
			mutate: func(s *MysqlDatabaseSpec) {
				s.Users = []MysqlDatabaseUser{
					{SecretName: "support-ro-mysql", Privileges: []MysqlPrivilege{PrivilegeSelect}},
					{SecretName: "reporting-mysql", Privileges: []MysqlPrivilege{PrivilegeSelect}},
				}
			},
			ownerUser: "acme_app",
			userUsernames: map[string]string{
				"support-ro-mysql": "acme_support",
				"reporting-mysql":  "acme_support",
			},
			wantField: "spec.users[1] secret username",
		},
		{
			// ALL PRIVILEGES is rejected in Go even though CEL also rejects
			// it: objects constructed in Go never met the API server.
			name: "users privileges include ALL PRIVILEGES",
			mutate: func(s *MysqlDatabaseSpec) {
				s.Users = []MysqlDatabaseUser{{SecretName: "support-ro-mysql", Privileges: []MysqlPrivilege{PrivilegeAllPrivileges}}}
			},
			ownerUser:     "acme_app",
			userUsernames: map[string]string{"support-ro-mysql": "acme_support"},
			wantField:     "spec.users[0].privileges",
		},
		{
			name: "users privilege outside allowlist",
			mutate: func(s *MysqlDatabaseSpec) {
				s.Users = []MysqlDatabaseUser{{SecretName: "support-ro-mysql", Privileges: []MysqlPrivilege{"SUPER"}}}
			},
			ownerUser:     "acme_app",
			userUsernames: map[string]string{"support-ro-mysql": "acme_support"},
			wantField:     "spec.users[0].privileges",
		},
		{
			name: "users entry with no resolved username",
			mutate: func(s *MysqlDatabaseSpec) {
				s.Users = []MysqlDatabaseUser{{SecretName: "support-ro-mysql", Privileges: []MysqlPrivilege{PrivilegeSelect}}}
			},
			ownerUser:     "acme_app",
			userUsernames: nil,
			wantField:     "no resolved username",
		},
		{
			// A grants[] entry naming a users[] principal would make two
			// spec lists manage one account's privileges.
			name: "grant username is a users username",
			mutate: func(s *MysqlDatabaseSpec) {
				s.Users = []MysqlDatabaseUser{{SecretName: "support-ro-mysql", Privileges: []MysqlPrivilege{PrivilegeSelect}}}
				s.Grants[0].Username = "acme_support"
			},
			ownerUser:     "acme_app",
			userUsernames: map[string]string{"support-ro-mysql": "acme_support"},
			wantField:     "spec.grants[0].username",
		},
		{
			name:      "bad database name",
			mutate:    func(s *MysqlDatabaseSpec) { s.DatabaseName = "acme`; DROP DATABASE x" },
			ownerUser: "acme_app",
			wantField: "spec.databaseName",
		},
		{
			name:      "bad character set",
			mutate:    func(s *MysqlDatabaseSpec) { s.CharacterSet = "utf8mb4 COLLATE evil" },
			ownerUser: "acme_app",
			wantField: "spec.characterSet",
		},
		{
			name:      "bad collation",
			mutate:    func(s *MysqlDatabaseSpec) { s.Collation = "x'" },
			ownerUser: "acme_app",
			wantField: "spec.collation",
		},
		{
			// The owner username arrives from a Secret, so the API server
			// cannot vet it. This is the check that covers it.
			name:      "bad owner username from secret",
			mutate:    func(s *MysqlDatabaseSpec) {},
			ownerUser: "evil'@'%' WITH GRANT OPTION -- ",
			wantField: "spec.owner secret username",
		},
		{
			name:      "owner privilege outside allowlist",
			mutate:    func(s *MysqlDatabaseSpec) { s.Owner.Privileges = []MysqlPrivilege{"SUPER"} },
			ownerUser: "acme_app",
			wantField: "spec.owner.privileges",
		},
		{
			name:      "grant username outside charset",
			mutate:    func(s *MysqlDatabaseSpec) { s.Grants[0].Username = "maester'@'%" },
			ownerUser: "acme_app",
			wantField: "spec.grants[0].username",
		},
		{
			name:      "grant privilege outside allowlist",
			mutate:    func(s *MysqlDatabaseSpec) { s.Grants[0].Privileges = []MysqlPrivilege{"GRANT OPTION"} },
			ownerUser: "acme_app",
			wantField: "spec.grants[0].privileges",
		},
		{
			name: "duplicate grant username",
			mutate: func(s *MysqlDatabaseSpec) {
				s.Grants = append(s.Grants, MysqlDatabaseGrant{
					Username: "maester", Privileges: []MysqlPrivilege{PrivilegeSelect},
				})
			},
			ownerUser: "acme_app",
			wantField: "spec.grants[1].username",
		},
		{
			name:      "system schema mysql",
			mutate:    func(s *MysqlDatabaseSpec) { s.DatabaseName = "mysql" },
			ownerUser: "acme_app",
			wantField: "system schema",
		},
		{
			name:      "system schema in mixed case",
			mutate:    func(s *MysqlDatabaseSpec) { s.DatabaseName = "Performance_Schema" },
			ownerUser: "acme_app",
			wantField: "system schema",
		},
		{
			// A grants[] entry naming the owner would silently override
			// spec.owner.privileges: the grants pass runs after the owner
			// grant.
			name:      "grant username is the owner username",
			mutate:    func(s *MysqlDatabaseSpec) { s.Grants[0].Username = "acme_app" },
			ownerUser: "acme_app",
			wantField: "spec.grants[0].username",
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			spec := valid()
			tc.mutate(&spec)
			err := spec.Validate(tc.ownerUser, tc.userUsernames)
			if err == nil {
				t.Fatalf("Validate() = nil, want rejection")
			}
			if !strings.Contains(err.Error(), tc.wantField) {
				t.Fatalf("Validate() = %q, want a message naming %s", err, tc.wantField)
			}
		})
	}
}

func TestIsSystemSchema(t *testing.T) {
	for _, name := range []string{"mysql", "MySQL", "MYSQL", "sys", "Sys", "information_schema", "INFORMATION_SCHEMA", "performance_schema"} {
		if !IsSystemSchema(name) {
			t.Fatalf("IsSystemSchema(%q) = false, want true", name)
		}
	}
	for _, name := range []string{"acme_wms", "mysql_backup", "syslog", "performance", "", "my_sql"} {
		if IsSystemSchema(name) {
			t.Fatalf("IsSystemSchema(%q) = true, want false", name)
		}
	}
}
