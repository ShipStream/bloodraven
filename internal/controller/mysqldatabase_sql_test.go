package controller

import (
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
	got, err := renderOwnerUserStatements("acme_app", "s3cr3t")
	if err != nil {
		t.Fatalf("renderOwnerUserStatements() error = %v", err)
	}
	want := []string{
		"CREATE USER IF NOT EXISTS 'acme_app'@'%' IDENTIFIED BY 's3cr3t'",
		"ALTER USER 'acme_app'@'%' IDENTIFIED BY 's3cr3t'",
	}
	if len(got) != len(want) {
		t.Fatalf("renderOwnerUserStatements() = %v, want %v", got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("renderOwnerUserStatements()[%d] = %q, want %q", i, got[i], want[i])
		}
	}
}

func TestRenderOwnerUserStatementsEscapesPassword(t *testing.T) {
	// The password is not validated (it is opaque caller-chosen material),
	// so it is the one value that relies on escaping — the same
	// escapeSingleQuotes discipline reconcileRole uses.
	got, err := renderOwnerUserStatements("acme_app", `p'; DROP DATABASE x; --`)
	if err != nil {
		t.Fatalf("renderOwnerUserStatements() error = %v", err)
	}
	for _, stmt := range got {
		if !strings.Contains(stmt, `p''; DROP DATABASE x; --`) {
			t.Fatalf("statement %q did not escape the quote in the password", stmt)
		}
	}
}

func TestRenderOwnerUserStatementsRejectsBadUsername(t *testing.T) {
	if _, err := renderOwnerUserStatements(`evil'@'%' IDENTIFIED BY 'x'; GRANT ALL ON *.* TO 'evil'@'%`, "pw"); err == nil {
		t.Fatal("renderOwnerUserStatements() = nil error, want rejection of the username")
	}
	if _, err := renderOwnerUserStatements("acme_app", ""); err == nil {
		t.Fatal("renderOwnerUserStatements() with an empty password = nil error, want rejection")
	}
}

func TestRenderGrant(t *testing.T) {
	got, err := renderGrant("spec.grants[0].privileges",
		[]v1alpha1.MysqlPrivilege{v1alpha1.PrivilegeDelete, v1alpha1.PrivilegeSelect},
		"acme_wms", "maester")
	if err != nil {
		t.Fatalf("renderGrant() error = %v", err)
	}
	want := "GRANT SELECT, DELETE ON `acme_wms`.* TO 'maester'@'%'"
	if got != want {
		t.Fatalf("renderGrant() = %q, want %q", got, want)
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
		stmt, err := renderGrant("spec.owner.privileges", privs, "acme_wms", "acme_app")
		if err != nil {
			t.Fatalf("renderGrant() error = %v", err)
		}
		if strings.Contains(strings.ToUpper(stmt), "GRANT OPTION") {
			t.Fatalf("renderGrant() = %q, must never carry GRANT OPTION", stmt)
		}
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
		{"no privileges", nil, "acme_wms", "maester"},
		{"bad database", []v1alpha1.MysqlPrivilege{v1alpha1.PrivilegeSelect}, "acme`x", "maester"},
		{"bad username", []v1alpha1.MysqlPrivilege{v1alpha1.PrivilegeSelect}, "acme_wms", "maester'@'%"},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got, err := renderGrant("spec.grants[0].privileges", tc.privs, tc.database, tc.username)
			if err == nil {
				t.Fatalf("renderGrant() = %q, want rejection", got)
			}
			if got != "" {
				t.Fatalf("renderGrant() rendered %q alongside an error", got)
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

	dropUser, err := renderDropUser("acme_app")
	if err != nil {
		t.Fatalf("renderDropUser() error = %v", err)
	}
	if want := "DROP USER IF EXISTS 'acme_app'@'%'"; dropUser != want {
		t.Fatalf("renderDropUser() = %q, want %q", dropUser, want)
	}

	revoke, err := renderRevokeAll("spec.grants[0].username", "acme_wms", "maester")
	if err != nil {
		t.Fatalf("renderRevokeAll() error = %v", err)
	}
	if want := "REVOKE IF EXISTS ALL PRIVILEGES ON `acme_wms`.* FROM 'maester'@'%'"; revoke != want {
		t.Fatalf("renderRevokeAll() = %q, want %q", revoke, want)
	}

	if _, err := renderDropDatabase("acme`; DROP DATABASE other"); err == nil {
		t.Fatal("renderDropDatabase() accepted an invalid identifier")
	}
	if _, err := renderDropUser("evil'@'%"); err == nil {
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
		ObjectMeta: metav1.ObjectMeta{Name: "acme-mysql-owner", Namespace: "bloodraven"},
		Data:       map[string][]byte{"username": []byte("acme_app"), "password": []byte("pw1")},
	}

	base, err := computeDatabaseHash(mdb, secret, "dc1")
	if err != nil {
		t.Fatalf("computeDatabaseHash() error = %v", err)
	}
	if len(base) != 16 {
		t.Fatalf("computeDatabaseHash() = %q, want a 16-character digest", base)
	}

	again, err := computeDatabaseHash(mdb, secret, "dc1")
	if err != nil {
		t.Fatalf("computeDatabaseHash() error = %v", err)
	}
	if again != base {
		t.Fatalf("computeDatabaseHash() is not stable: %q then %q", base, again)
	}

	// A rotated password must change the hash, or rotation would be
	// silently skipped by the "nothing changed" short-circuit.
	rotated := secret.DeepCopy()
	rotated.Data["password"] = []byte("pw2")
	if h, err := computeDatabaseHash(mdb, rotated, "dc1"); err != nil || h == base {
		t.Fatalf("computeDatabaseHash() after rotation = %q (err %v), want a different digest", h, err)
	}

	// A failover must change the hash, or the group watch would re-enqueue
	// every CR only for the skip check to swallow the re-apply.
	if h, err := computeDatabaseHash(mdb, secret, "dc2"); err != nil || h == base {
		t.Fatalf("computeDatabaseHash() after failover = %q (err %v), want a different digest", h, err)
	}

	// A spec change must change the hash.
	edited := mdb.DeepCopy()
	edited.Spec.Grants = append(edited.Spec.Grants, v1alpha1.MysqlDatabaseGrant{
		Username: "reporting", Privileges: []v1alpha1.MysqlPrivilege{v1alpha1.PrivilegeSelect},
	})
	if h, err := computeDatabaseHash(edited, secret, "dc1"); err != nil || h == base {
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

	hash, err := computeDatabaseHash(mdb, secret, "dc1")
	if err != nil {
		t.Fatalf("computeDatabaseHash() error = %v", err)
	}
	if strings.Contains(hash, "hunter2") || strings.Contains(hash, "acme_app") {
		t.Fatalf("hash %q leaked secret material", hash)
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
				fg.Status.RestoreInPlace = &v1alpha1.RestoreInPlaceStatus{Phase: v1alpha1.RestoreInPlaceRestoring}
			}),
			true, "RestoreInProgress",
		},
		{
			"restore fencing",
			group(func(fg *v1alpha1.MysqlFailoverGroup) {
				fg.Status.RestoreInPlace = &v1alpha1.RestoreInPlaceStatus{Phase: v1alpha1.RestoreInPlaceFencing}
			}),
			true, "RestoreInProgress",
		},
		{
			"restore succeeded is not fenced",
			group(func(fg *v1alpha1.MysqlFailoverGroup) {
				fg.Status.RestoreInPlace = &v1alpha1.RestoreInPlaceStatus{Phase: v1alpha1.RestoreInPlaceSucceeded}
			}),
			false, "",
		},
		{
			"restore failed is not fenced",
			group(func(fg *v1alpha1.MysqlFailoverGroup) {
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
