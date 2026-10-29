package controller

import (
	"reflect"
	"strings"
	"testing"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	v1alpha1 "github.com/shipstream/bloodraven/api/v1alpha1"
)

func TestRenderCreateDatabase(t *testing.T) {
	got, err := renderCreateDatabase("acme_wms", "utf8mb4", "utf8mb4_unicode_ci")
	if err != nil {
		t.Fatalf("renderCreateDatabase() error = %v", err)
	}
	want := "CREATE DATABASE IF NOT EXISTS `acme_wms` CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci"
	if got != want {
		t.Fatalf("renderCreateDatabase() = %q, want %q", got, want)
	}
}

func TestRenderCreateDatabaseRejectsBeforeFormatting(t *testing.T) {
	// Each case would be a SQL injection if the value were merely escaped
	// and interpolated. The contract is rejection, so the statement is
	// never produced at all.
	cases := []struct {
		name       string
		database   string
		charset    string
		collation  string
		wantReject string
	}{
		{"database backtick", "acme`; DROP DATABASE other; --", "utf8mb4", "utf8mb4_unicode_ci", "spec.databaseName"},
		{"database empty", "", "utf8mb4", "utf8mb4_unicode_ci", "spec.databaseName"},
		{"charset injection", "acme_wms", "utf8mb4 COLLATE x, DEFAULT ENCRYPTION='N'", "utf8mb4_unicode_ci", "spec.characterSet"},
		{"collation injection", "acme_wms", "utf8mb4", "utf8mb4_unicode_ci; DROP DATABASE other", "spec.collation"},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got, err := renderCreateDatabase(tc.database, tc.charset, tc.collation)
			if err == nil {
				t.Fatalf("renderCreateDatabase() = %q, want rejection", got)
			}
			if got != "" {
				t.Fatalf("renderCreateDatabase() returned %q alongside an error; nothing must be rendered", got)
			}
			if !strings.Contains(err.Error(), tc.wantReject) {
				t.Fatalf("error = %q, want a message naming %s", err, tc.wantReject)
			}
		})
	}
}

func TestRenderOwnerUserStatementsMatchesReconcileRolePattern(t *testing.T) {
	got, err := renderOwnerUserStatements("acme_app", "s3cr3t", defaultHosts)
	if err != nil {
		t.Fatalf("renderOwnerUserStatements(, defaultHosts) error = %v", err)
	}
	want := []string{
		"CREATE USER IF NOT EXISTS 'acme_app'@'%' IDENTIFIED BY 's3cr3t'",
		"ALTER USER 'acme_app'@'%' IDENTIFIED BY 's3cr3t'",
	}
	if len(got) != len(want) {
		t.Fatalf("renderOwnerUserStatements(, defaultHosts) = %v, want %v", got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("renderOwnerUserStatements(, defaultHosts)[%d] = %q, want %q", i, got[i], want[i])
		}
	}
}

func TestRenderOwnerUserStatementsEscapesPassword(t *testing.T) {
	// The password is not validated (it is opaque caller-chosen material),
	// so it is the one value that relies on escaping — the same
	// escapeSingleQuotes discipline reconcileRole uses.
	got, err := renderOwnerUserStatements("acme_app", `p'; DROP DATABASE x; --`, defaultHosts)
	if err != nil {
		t.Fatalf("renderOwnerUserStatements(, defaultHosts) error = %v", err)
	}
	for _, stmt := range got {
		if !strings.Contains(stmt, `p''; DROP DATABASE x; --`) {
			t.Fatalf("statement %q did not escape the quote in the password", stmt)
		}
	}

	// A trailing backslash is the other literal-breakout vector: under
	// MySQL's default sql_mode, 'p\' would swallow the closing quote.
	// escapeSingleQuotes doubles backslashes before quotes, so the rendered
	// literal must end \\' — pinned here so the escaping order never quietly
	// regresses to quotes-only.
	got, err = renderOwnerUserStatements("acme_app", `p\`, defaultHosts)
	if err != nil {
		t.Fatalf("renderOwnerUserStatements(, defaultHosts) error = %v", err)
	}
	for _, stmt := range got {
		if !strings.HasSuffix(stmt, `IDENTIFIED BY 'p\\'`) {
			t.Fatalf("statement %q did not escape the trailing backslash; the literal can be broken out of", stmt)
		}
	}
}

func TestRenderOwnerUserStatementsRejectsBadUsername(t *testing.T) {
	if _, err := renderOwnerUserStatements(`evil'@'%' IDENTIFIED BY 'x'; GRANT ALL ON *.* TO 'evil'@'%`, "pw", defaultHosts); err == nil {
		t.Fatal("renderOwnerUserStatements(, defaultHosts) = nil error, want rejection of the username")
	}
	if _, err := renderOwnerUserStatements("acme_app", "", defaultHosts); err == nil {
		t.Fatal("renderOwnerUserStatements(, defaultHosts) with an empty password = nil error, want rejection")
	}
}

func TestRenderGrant(t *testing.T) {
	got, err := renderGrant("spec.grants[0].privileges",
		[]v1alpha1.MysqlPrivilege{v1alpha1.PrivilegeDelete, v1alpha1.PrivilegeSelect},
		"acme_wms", "maester", defaultHosts)
	if err != nil {
		t.Fatalf("renderGrant(, defaultHosts) error = %v", err)
	}
	want := "GRANT SELECT, DELETE ON `acme_wms`.* TO 'maester'@'%'"
	if got != want {
		t.Fatalf("renderGrant(, defaultHosts) = %q, want %q", got, want)
	}
}

func TestRenderGrantNeverEmitsGrantOption(t *testing.T) {
	// Belt and braces around the property that matters most: no input to
	// renderGrant can produce WITH GRANT OPTION, because the privilege text
	// comes from the fixed table and the suffix exists nowhere in the
	// rendering path.
	all := make([]v1alpha1.MysqlPrivilege, 0, len(v1alpha1.AllowedPrivilegeNames()))
	for _, name := range v1alpha1.AllowedPrivilegeNames() {
		if name == string(v1alpha1.PrivilegeAllPrivileges) {
			continue
		}
		all = append(all, v1alpha1.MysqlPrivilege(name))
	}

	for _, privs := range [][]v1alpha1.MysqlPrivilege{all, {v1alpha1.PrivilegeAllPrivileges}} {
		stmt, err := renderGrant("spec.owner.privileges", privs, "acme_wms", "acme_app", defaultHosts)
		if err != nil {
			t.Fatalf("renderGrant(, defaultHosts) error = %v", err)
		}
		if strings.Contains(strings.ToUpper(stmt), "GRANT OPTION") {
			t.Fatalf("renderGrant(, defaultHosts) = %q, must never carry GRANT OPTION", stmt)
		}
	}
}

func TestRenderGrantDeploymentPrivileges(t *testing.T) {
	for _, field := range []string{"spec.owner.privileges", "spec.users[0].privileges", "spec.grants[0].privileges"} {
		t.Run(field, func(t *testing.T) {
			got, err := renderGrant(field,
				[]v1alpha1.MysqlPrivilege{v1alpha1.PrivilegeCreateView, v1alpha1.PrivilegeCreateTemporaryTables, v1alpha1.PrivilegeSelect},
				"acme_wms", "acme_app", defaultHosts)
			if err != nil {
				t.Fatal(err)
			}
			want := "GRANT SELECT, CREATE TEMPORARY TABLES, CREATE VIEW ON `acme_wms`.* TO 'acme_app'@'%'"
			if got != want {
				t.Fatalf("renderGrant = %q, want %q", got, want)
			}
		})
	}
}

func TestRenderGrantRejectsBadInput(t *testing.T) {
	cases := []struct {
		name     string
		privs    []v1alpha1.MysqlPrivilege
		database string
		username string
	}{
		{"privilege outside allowlist", []v1alpha1.MysqlPrivilege{"SUPER"}, "acme_wms", "maester"},
		{"grant option", []v1alpha1.MysqlPrivilege{"GRANT OPTION"}, "acme_wms", "maester"},
		{"temporary tables injection", []v1alpha1.MysqlPrivilege{"CREATE TEMPORARY TABLES WITH GRANT OPTION"}, "acme_wms", "maester"},
		{"view injection", []v1alpha1.MysqlPrivilege{"CREATE VIEW; GRANT ALL"}, "acme_wms", "maester"},
		{"no privileges", nil, "acme_wms", "maester"},
		{"bad database", []v1alpha1.MysqlPrivilege{v1alpha1.PrivilegeSelect}, "acme`x", "maester"},
		{"bad username", []v1alpha1.MysqlPrivilege{v1alpha1.PrivilegeSelect}, "acme_wms", "maester'@'%"},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got, err := renderGrant("spec.grants[0].privileges", tc.privs, tc.database, tc.username, defaultHosts)
			if err == nil {
				t.Fatalf("renderGrant(, defaultHosts) = %q, want rejection", got)
			}
			if got != "" {
				t.Fatalf("renderGrant(, defaultHosts) rendered %q alongside an error", got)
			}
		})
	}
}

func TestRenderDestructiveStatements(t *testing.T) {
	drop, err := renderDropDatabase("acme_wms")
	if err != nil {
		t.Fatalf("renderDropDatabase() error = %v", err)
	}
	if want := "DROP DATABASE IF EXISTS `acme_wms`"; drop != want {
		t.Fatalf("renderDropDatabase() = %q, want %q", drop, want)
	}

	dropUser, err := renderDropUser("spec.owner secret username", "acme_app", defaultHosts)
	if err != nil {
		t.Fatalf("renderDropUser() error = %v", err)
	}
	if want := "DROP USER IF EXISTS 'acme_app'@'%'"; dropUser != want {
		t.Fatalf("renderDropUser() = %q, want %q", dropUser, want)
	}

	revoke, err := renderSchemaRevoke("spec.grants[0].username", []string{"SELECT"}, "acme_wms", "maester", defaultHosts)
	if err != nil {
		t.Fatalf("renderSchemaRevoke() error = %v", err)
	}
	if want := "REVOKE IF EXISTS SELECT ON `acme_wms`.* FROM 'maester'@'%' IGNORE UNKNOWN USER"; revoke != want {
		t.Fatalf("renderSchemaRevoke() = %q, want %q", revoke, want)
	}

	if _, err := renderDropDatabase("acme`; DROP DATABASE other"); err == nil {
		t.Fatal("renderDropDatabase() accepted an invalid identifier")
	}
	if _, err := renderDropUser("spec.owner secret username", "evil'@'%", defaultHosts); err == nil {
		t.Fatal("renderDropUser() accepted an invalid username")
	}
}

func TestComputeDatabaseHashStability(t *testing.T) {
	mdb := &v1alpha1.MysqlDatabase{
		ObjectMeta: metav1.ObjectMeta{Name: "tenant-acme", Namespace: "bloodraven"},
		Spec: v1alpha1.MysqlDatabaseSpec{
			GroupRef:     v1alpha1.LocalGroupRef{Name: "main"},
			DatabaseName: "acme_wms",
			Owner:        v1alpha1.MysqlDatabaseOwner{SecretName: "acme-mysql-owner"},
			Grants: []v1alpha1.MysqlDatabaseGrant{
				{Username: "maester", Privileges: []v1alpha1.MysqlPrivilege{v1alpha1.PrivilegeSelect, v1alpha1.PrivilegeDelete}},
			},
		},
	}
	secret := &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{Name: "acme-mysql-owner", Namespace: "bloodraven", UID: "uid-secret", ResourceVersion: "1"},
		Data:       map[string][]byte{"username": []byte("acme_app"), "password": []byte("pw1")},
	}
	fg := &v1alpha1.MysqlFailoverGroup{
		ObjectMeta: metav1.ObjectMeta{Name: "main", Namespace: "bloodraven", UID: "uid-group"},
	}
	fg.Status.ActiveSite = "dc1"

	base, err := computeDatabaseHash(mdb, secret, nil, fg)
	if err != nil {
		t.Fatalf("computeDatabaseHash() error = %v", err)
	}
	if len(base) != 16 {
		t.Fatalf("computeDatabaseHash() = %q, want a 16-character digest", base)
	}

	again, err := computeDatabaseHash(mdb, secret, nil, fg)
	if err != nil {
		t.Fatalf("computeDatabaseHash() error = %v", err)
	}
	if again != base {
		t.Fatalf("computeDatabaseHash() is not stable: %q then %q", base, again)
	}

	// A rotated Secret — new resourceVersion — must change the hash, or
	// rotation would be silently skipped by the "nothing changed"
	// short-circuit. The hash reads the Secret's revision, never its bytes.
	rotated := secret.DeepCopy()
	rotated.Data["password"] = []byte("pw2")
	rotated.ResourceVersion = "2"
	if h, err := computeDatabaseHash(mdb, rotated, nil, fg); err != nil || h == base {
		t.Fatalf("computeDatabaseHash() after rotation = %q (err %v), want a different digest", h, err)
	}

	// A failover must change the hash, or the group watch would re-enqueue
	// every CR only for the skip check to swallow the re-apply.
	failedOver := fg.DeepCopy()
	failedOver.Status.ActiveSite = "dc2"
	if h, err := computeDatabaseHash(mdb, secret, nil, failedOver); err != nil || h == base {
		t.Fatalf("computeDatabaseHash() after failover = %q (err %v), want a different digest", h, err)
	}

	// A recreated group (new UID) must invalidate every Ready hash, or a
	// restored-from-scratch group would inherit Ready CRs that never spoke
	// to its MySQL.
	recreated := fg.DeepCopy()
	recreated.UID = "uid-group-2"
	if h, err := computeDatabaseHash(mdb, secret, nil, recreated); err != nil || h == base {
		t.Fatalf("computeDatabaseHash() after group re-creation = %q (err %v), want a different digest", h, err)
	}

	// A completed in-place restore must invalidate the hash even when the
	// active site never moved: the fence transitions may have been missed
	// while the operator was down.
	restored := fg.DeepCopy()
	restored.Status.RestoreInPlace = &v1alpha1.RestoreInPlaceStatus{
		Phase:            v1alpha1.RestoreInPlaceSucceeded,
		ConfirmTokenUsed: "2026-08-10T00:00:00Z",
	}
	if h, err := computeDatabaseHash(mdb, secret, nil, restored); err != nil || h == base {
		t.Fatalf("computeDatabaseHash() after in-place restore = %q (err %v), want a different digest", h, err)
	}

	// A spec change must change the hash.
	edited := mdb.DeepCopy()
	edited.Spec.Grants = append(edited.Spec.Grants, v1alpha1.MysqlDatabaseGrant{
		Username: "reporting", Privileges: []v1alpha1.MysqlPrivilege{v1alpha1.PrivilegeSelect},
	})
	if h, err := computeDatabaseHash(edited, secret, nil, fg); err != nil || h == base {
		t.Fatalf("computeDatabaseHash() after a spec edit = %q (err %v), want a different digest", h, err)
	}
}

func TestComputeDatabaseHashNeverContainsSecretBytes(t *testing.T) {
	mdb := &v1alpha1.MysqlDatabase{
		ObjectMeta: metav1.ObjectMeta{Name: "tenant-acme", Namespace: "bloodraven"},
		Spec:       v1alpha1.MysqlDatabaseSpec{DatabaseName: "acme_wms"},
	}
	secret := &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{Name: "acme-mysql-owner"},
		Data:       map[string][]byte{"username": []byte("acme_app"), "password": []byte("hunter2")},
	}

	fg := &v1alpha1.MysqlFailoverGroup{ObjectMeta: metav1.ObjectMeta{Name: "main"}}
	fg.Status.ActiveSite = "dc1"
	hash, err := computeDatabaseHash(mdb, secret, nil, fg)
	if err != nil {
		t.Fatalf("computeDatabaseHash() error = %v", err)
	}
	if strings.Contains(hash, "hunter2") || strings.Contains(hash, "acme_app") {
		t.Fatalf("hash %q leaked secret material", hash)
	}
}

func TestRenderAlterDatabase(t *testing.T) {
	got, err := renderAlterDatabase("acme_wms", "utf8mb3", "utf8mb3_general_ci")
	if err != nil {
		t.Fatalf("renderAlterDatabase() error = %v", err)
	}
	want := "ALTER DATABASE `acme_wms` CHARACTER SET utf8mb3 COLLATE utf8mb3_general_ci"
	if got != want {
		t.Fatalf("renderAlterDatabase() = %q, want %q", got, want)
	}
	if _, err := renderAlterDatabase("acme`; DROP DATABASE other", "utf8mb4", "utf8mb4_unicode_ci"); err == nil {
		t.Fatal("renderAlterDatabase() accepted an invalid identifier")
	}
	if _, err := renderAlterDatabase("acme_wms", "utf8mb4 COLLATE evil", "utf8mb4_unicode_ci"); err == nil {
		t.Fatal("renderAlterDatabase() accepted an injected character set")
	}
}

func TestRenderSchemaRevoke(t *testing.T) {
	stmt, err := renderSchemaRevoke("test", []string{"SELECT", "CREATE ROUTINE", "ALTER ROUTINE"}, `acme\_wms`, "reader", []string{"10.0.0.1"})
	if err != nil {
		t.Fatal(err)
	}
	want := "REVOKE IF EXISTS SELECT, CREATE ROUTINE, ALTER ROUTINE ON `acme\\_wms`.* FROM 'reader'@'10.0.0.1' IGNORE UNKNOWN USER"
	if stmt != want {
		t.Fatalf("revoke = %q, want %q", stmt, want)
	}
	for _, privileges := range [][]string{nil, {"ALL PRIVILEGES"}, {"GRANT OPTION"}, {"SELECT; DROP USER root"}} {
		if _, err := renderSchemaRevoke("test", privileges, "acme_wms", "reader", defaultHosts); err == nil {
			t.Fatalf("accepted unsafe privileges %v", privileges)
		}
	}
	if _, err := planSchemaRevokes("test", []string{"acme`; DROP DATABASE other"}, "reader", defaultHosts, nil); err == nil {
		t.Fatal("accepted unsafe database name")
	}
	if _, err := planSchemaRevokes("test", []string{"acme_wms"}, "reader'", defaultHosts, nil); err == nil {
		t.Fatal("accepted unsafe username")
	}
}

func TestGroupFenced(t *testing.T) {
	group := func(mutate func(*v1alpha1.MysqlFailoverGroup)) *v1alpha1.MysqlFailoverGroup {
		fg := &v1alpha1.MysqlFailoverGroup{ObjectMeta: metav1.ObjectMeta{Name: "main"}}
		fg.Status.ActiveSite = "dc1"
		mutate(fg)
		return fg
	}

	cases := []struct {
		name       string
		fg         *v1alpha1.MysqlFailoverGroup
		wantFenced bool
		wantReason string
	}{
		{"clean", group(func(fg *v1alpha1.MysqlFailoverGroup) {}), false, ""},
		{
			"restore restoring",
			group(func(fg *v1alpha1.MysqlFailoverGroup) {
				fg.Spec.RestoreInPlace = &v1alpha1.RestoreInPlaceSpec{}
				fg.Status.RestoreInPlace = &v1alpha1.RestoreInPlaceStatus{Phase: v1alpha1.RestoreInPlaceRestoring}
			}),
			true, "RestoreInProgress",
		},
		{
			"restore fencing",
			group(func(fg *v1alpha1.MysqlFailoverGroup) {
				fg.Spec.RestoreInPlace = &v1alpha1.RestoreInPlaceSpec{}
				fg.Status.RestoreInPlace = &v1alpha1.RestoreInPlaceStatus{Phase: v1alpha1.RestoreInPlaceFencing}
			}),
			true, "RestoreInProgress",
		},
		{
			"restore requested but not yet observed",
			group(func(fg *v1alpha1.MysqlFailoverGroup) {
				fg.Spec.RestoreInPlace = &v1alpha1.RestoreInPlaceSpec{}
			}),
			true, "RestoreInProgress",
		},
		{
			"restore succeeded is not fenced",
			group(func(fg *v1alpha1.MysqlFailoverGroup) {
				fg.Spec.RestoreInPlace = &v1alpha1.RestoreInPlaceSpec{}
				fg.Status.RestoreInPlace = &v1alpha1.RestoreInPlaceStatus{Phase: v1alpha1.RestoreInPlaceSucceeded}
			}),
			false, "",
		},
		{
			"restore failed is not fenced",
			group(func(fg *v1alpha1.MysqlFailoverGroup) {
				fg.Spec.RestoreInPlace = &v1alpha1.RestoreInPlaceSpec{}
				fg.Status.RestoreInPlace = &v1alpha1.RestoreInPlaceStatus{Phase: v1alpha1.RestoreInPlaceFailed}
			}),
			false, "",
		},
		{
			"planned failover promoting",
			group(func(fg *v1alpha1.MysqlFailoverGroup) {
				fg.Status.PlannedFailover = &v1alpha1.PlannedFailoverStatus{Phase: v1alpha1.PlannedFailoverPhasePromoting}
			}),
			true, "PlannedFailoverInProgress",
		},
		{
			"planned failover draining",
			group(func(fg *v1alpha1.MysqlFailoverGroup) {
				fg.Status.PlannedFailover = &v1alpha1.PlannedFailoverStatus{Phase: v1alpha1.PlannedFailoverPhaseDraining}
			}),
			true, "PlannedFailoverInProgress",
		},
		{
			"planned failover succeeded is not fenced",
			group(func(fg *v1alpha1.MysqlFailoverGroup) {
				fg.Status.PlannedFailover = &v1alpha1.PlannedFailoverStatus{Phase: v1alpha1.PlannedFailoverPhaseSucceeded}
			}),
			false, "",
		},
		{
			"planned failover deferred is not fenced",
			group(func(fg *v1alpha1.MysqlFailoverGroup) {
				fg.Status.PlannedFailover = &v1alpha1.PlannedFailoverStatus{Phase: v1alpha1.PlannedFailoverPhaseDeferred}
			}),
			false, "",
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			reason, _, fenced := groupFenced(tc.fg)
			if fenced != tc.wantFenced {
				t.Fatalf("groupFenced() fenced = %v, want %v", fenced, tc.wantFenced)
			}
			if reason != tc.wantReason {
				t.Fatalf("groupFenced() reason = %q, want %q", reason, tc.wantReason)
			}
		})
	}
}

// TestRenderMultiHostPrincipal: a principal with several hosts is one
// statement per operation, naming every account — the per-account auth
// clause for CREATE/ALTER USER, a plain account list for GRANT/REVOKE/DROP.
func TestRenderMultiHostPrincipal(t *testing.T) {
	hosts := []string{"35.1.2.3", "35.4.5.6", "2001:db8::1"}
	got, err := renderTenantUserStatements("spec.users[0] secret username", "acme_support", "pw", hosts,
		&v1alpha1.MysqlUserResourceLimits{MaxUserConnections: 5})
	if err != nil {
		t.Fatalf("renderTenantUserStatements() error = %v", err)
	}
	want := []string{
		"CREATE USER IF NOT EXISTS 'acme_support'@'35.1.2.3' IDENTIFIED BY 'pw', 'acme_support'@'35.4.5.6' IDENTIFIED BY 'pw', 'acme_support'@'2001:db8::1' IDENTIFIED BY 'pw'",
		"ALTER USER 'acme_support'@'35.1.2.3' IDENTIFIED BY 'pw', 'acme_support'@'35.4.5.6' IDENTIFIED BY 'pw', 'acme_support'@'2001:db8::1' IDENTIFIED BY 'pw' WITH MAX_USER_CONNECTIONS 5 MAX_QUERIES_PER_HOUR 0",
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("renderTenantUserStatements() =\n%q\nwant\n%q", got, want)
	}

	grant, err := renderGrant("spec.users[0].privileges", []v1alpha1.MysqlPrivilege{v1alpha1.PrivilegeSelect}, "acme_wms", "acme_support", hosts)
	if err != nil {
		t.Fatalf("renderGrant() error = %v", err)
	}
	if want := "GRANT SELECT ON `acme_wms`.* TO 'acme_support'@'35.1.2.3', 'acme_support'@'35.4.5.6', 'acme_support'@'2001:db8::1'"; grant != want {
		t.Fatalf("renderGrant() = %q, want %q", grant, want)
	}
	drop, err := renderDropUser("spec.users[] ledger username", "acme_support", hosts[:2])
	if err != nil {
		t.Fatalf("renderDropUser() error = %v", err)
	}
	if want := "DROP USER IF EXISTS 'acme_support'@'35.1.2.3', 'acme_support'@'35.4.5.6'"; drop != want {
		t.Fatalf("renderDropUser() = %q, want %q", drop, want)
	}

	// A host that fails validation never reaches a format string.
	if _, err := renderDropUser("spec.users[] ledger username", "acme_support", []string{"35.1.2.3", "evil'@'%"}); err == nil {
		t.Fatal("renderDropUser() accepted an invalid host")
	}
	if _, err := renderGrant("spec.owner.privileges", []v1alpha1.MysqlPrivilege{v1alpha1.PrivilegeSelect}, "acme_wms", "acme_app", nil); err == nil {
		t.Fatal("renderGrant() accepted an empty hosts list; defaults are the caller's job")
	}
}

func TestHostSetHelpers(t *testing.T) {
	if got := unionHosts([]string{"a", "b"}, []string{"b", "c", ""}); !reflect.DeepEqual(got, []string{"a", "b", "c"}) {
		t.Fatalf("unionHosts = %v", got)
	}
	if got := diffHosts([]string{"a", "b", "c"}, []string{"b"}); !reflect.DeepEqual(got, []string{"a", "c"}) {
		t.Fatalf("diffHosts = %v", got)
	}
	// Pre-hosts records mean '%'.
	if got := recordedOwnerHosts(&v1alpha1.MysqlDatabaseStatus{OwnerUser: "acme_app"}); !reflect.DeepEqual(got, []string{"%"}) {
		t.Fatalf("recordedOwnerHosts(legacy) = %v", got)
	}
	if got := recordedOwnerHosts(&v1alpha1.MysqlDatabaseStatus{}); got != nil {
		t.Fatalf("recordedOwnerHosts(no owner) = %v, want nil", got)
	}
	if got := ledgerHosts(v1alpha1.MysqlDatabaseUserState{Username: "x"}); !reflect.DeepEqual(got, []string{"%"}) {
		t.Fatalf("ledgerHosts(legacy) = %v", got)
	}
}

func TestGrantDatabaseSpellings(t *testing.T) {
	cases := []struct {
		name string
		want []string
	}{
		{name: "acme_wms", want: []string{"acme_wms", `acme\_wms`}},
		{name: "acmewms", want: []string{"acmewms"}},
		// No escaped grant over MySQL's limit can exist, so none is revoked.
		{name: strings.Repeat("a_", 5) + strings.Repeat("b", 54), want: []string{strings.Repeat("a_", 5) + strings.Repeat("b", 54)}},
	}
	for _, tc := range cases {
		got := grantDatabaseSpellings(tc.name)
		if strings.Join(got, "|") != strings.Join(tc.want, "|") {
			t.Errorf("grantDatabaseSpellings(%q) = %q, want %q", tc.name, got, tc.want)
		}
	}

	grant, err := renderGrant("spec.owner.privileges", []v1alpha1.MysqlPrivilege{v1alpha1.PrivilegeSelect}, `my\_ql`, "acme_app", defaultHosts)
	if err != nil {
		t.Fatalf("renderGrant(escaped) error = %v", err)
	}
	if want := "GRANT SELECT ON `my\\_ql`.* TO 'acme_app'@'%'"; grant != want {
		t.Fatalf("renderGrant(escaped) = %q, want %q", grant, want)
	}
	for _, bad := range []string{`acme\wms`, `acme\\_wms`, "acme%wms", "acme`wms"} {
		if _, err := renderGrant("spec.owner.privileges", []v1alpha1.MysqlPrivilege{v1alpha1.PrivilegeSelect}, bad, "acme_app", defaultHosts); err == nil {
			t.Errorf("renderGrant(%q) accepted a malformed grant database", bad)
		}
	}
}

// MySQL rejects a GRANT whose escaped schema name exceeds 64 characters
// (ERROR 1059), even though the unescaped name is valid.
func TestQuoteGrantDatabaseRejectsOverlongEscapedName(t *testing.T) {
	name := strings.Repeat("a_", 5) + strings.Repeat("b", 54)
	escaped := strings.ReplaceAll(name, "_", `\_`)
	if _, err := quoteGrantDatabase(escaped); err == nil || !strings.Contains(err.Error(), "64-character") {
		t.Fatalf("quoteGrantDatabase(%q) error = %v, want the length limit", escaped, err)
	}
	forms := grantDatabaseSpellings(name)
	if len(forms) != 1 || forms[0] != name {
		t.Fatalf("grantDatabaseSpellings(%q) = %q, want only the literal name", name, forms)
	}
	if _, err := quoteGrantDatabase(forms[0]); err != nil {
		t.Fatalf("quoteGrantDatabase(literal) error = %v", err)
	}
}
