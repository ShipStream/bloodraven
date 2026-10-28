package component

import (
	"context"
	"database/sql"
	"os"
	"testing"

	mysqldriver "github.com/go-sql-driver/mysql"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/types"

	v1alpha1 "github.com/shipstream/bloodraven/api/v1alpha1"
)

// BLOODRAVEN_TENANT_TEST_ADDR must point at a disposable MySQL server with
// partial_revokes ON and an empty root password. These fixtures own their
// schemas and accounts; no live group or cluster is used.
// See README.md in this directory for the disposable-server run instructions.
func TestMysqlDatabaseGlobalGrantsMySQL(t *testing.T) {
	addr := os.Getenv("BLOODRAVEN_TENANT_TEST_ADDR")
	if addr == "" {
		t.Skip("set BLOODRAVEN_TENANT_TEST_ADDR to a disposable MySQL server")
	}
	connect := func(user, password string) (*sql.DB, error) {
		cfg := mysqldriver.NewConfig()
		cfg.User, cfg.Passwd, cfg.Net, cfg.Addr = user, password, "tcp", addr
		cfg.AllowNativePasswords = true
		return sql.Open("mysql", cfg.FormatDSN())
	}
	root, err := connect("root", "")
	if err != nil {
		t.Fatal(err)
	}
	defer root.Close()
	for _, stmt := range []string{"CREATE USER IF NOT EXISTS 'fixture_admin'@'%' IDENTIFIED BY 'fixture-pw'", "GRANT ALL PRIVILEGES ON *.* TO 'fixture_admin'@'%' WITH GRANT OPTION"} {
		if _, err := root.Exec(stmt); err != nil {
			t.Fatal(err)
		}
	}
	root, err = connect("fixture_admin", "fixture-pw")
	if err != nil {
		t.Fatal(err)
	}
	defer root.Close()
	var lowerCaseNames int
	if err := root.QueryRow("SELECT @@GLOBAL.lower_case_table_names").Scan(&lowerCaseNames); err != nil {
		t.Fatal(err)
	}
	for _, username := range []string{"cdc", "operator", "root"} {
		t.Run(username, func(t *testing.T) {
			exec := func(stmt string) {
				t.Helper()
				if _, err := root.ExecContext(context.Background(), stmt); err != nil {
					t.Fatalf("%s: %v", stmt, err)
				}
			}
			exec("DROP DATABASE IF EXISTS `acme_wms`")
			exec("DROP USER IF EXISTS 'acme_app'@'%', 'operator'@'%', 'cdc'@'%'")
			exec("CREATE USER 'operator'@'%' IDENTIFIED BY 'op-pw'")
			exec("GRANT ALL PRIVILEGES ON *.* TO 'operator'@'%' WITH GRANT OPTION")
			exec("GRANT ALL PRIVILEGES ON *.* TO 'root'@'%' WITH GRANT OPTION")
			exec("CREATE USER 'cdc'@'%' IDENTIFIED BY 'cdc-pw'")
			exec("GRANT SELECT, INSERT, CREATE ON *.* TO 'cdc'@'%'")
			t.Cleanup(func() {
				exec("DROP DATABASE IF EXISTS `acme_wms`")
				exec("DROP USER IF EXISTS 'acme_app'@'%', 'operator'@'%', 'cdc'@'%'")
			})
			cr := mdbCR(func(m *v1alpha1.MysqlDatabase) {
				if lowerCaseNames != 0 {
					m.Spec.DatabaseName = "Acme_Wms"
				}
				m.Spec.DeletionPolicy = v1alpha1.MysqlDatabaseDelete
				m.Spec.Grants = []v1alpha1.MysqlDatabaseGrant{{Username: username, Privileges: []v1alpha1.MysqlPrivilege{v1alpha1.PrivilegeSelect}}}
			})
			h := newMdbHarness(t, cr, mdbGroup("dc1"), mdbOperatorSecret(), mdbOwnerSecret())
			h.r.OpenDB = func(user, password, _, _ string) (*sql.DB, error) { return connect(user, password) }
			assertAccess := func(stage string) {
				t.Helper()
				var restrictions int
				if err := root.QueryRow("SELECT COUNT(*) FROM mysql.user WHERE User = ? AND Host = '%' AND JSON_LENGTH(User_attributes->'$.Restrictions') > 0", username).Scan(&restrictions); err != nil {
					t.Fatal(err)
				}
				if restrictions != 0 {
					t.Fatalf("%s: tenant reconciliation restricted %s's global privileges", stage, username)
				}
				password := map[string]string{"cdc": "cdc-pw", "operator": "op-pw", "root": ""}[username]
				account, err := connect(username, password)
				if err != nil {
					t.Fatal(err)
				}
				defer account.Close()
				for _, stmt := range []string{"CREATE TABLE IF NOT EXISTS acme_wms.access_probe (id INT)", "INSERT INTO acme_wms.access_probe VALUES (1)"} {
					if _, err := account.Exec(stmt); err != nil {
						t.Fatalf("%s: independent global access failed: %v", stage, err)
					}
				}
			}
			h.reconcile()
			h.requireReady()
			assertAccess("initial apply")
			if username == "cdc" {
				exec("REVOKE IF EXISTS SELECT, INSERT ON `acme\\_wms`.* FROM 'cdc'@'%'")
				h.update(func(m *v1alpha1.MysqlDatabase) {})
				h.reconcile()
				h.requireReady()
				assertAccess("legacy escaped restriction repair")
				if !h.sawEvent("PartialRevokeLifted") {
					t.Fatal("legacy repair event missing")
				}
				// A real-schema restriction has no provenance. Preserve INSERT
				// outside the desired SELECT until its privilege owner lifts it.
				exec("REVOKE INSERT ON `acme_wms`.* FROM 'cdc'@'%'")
				h.update(func(m *v1alpha1.MysqlDatabase) {})
				h.reconcile()
				h.requireReady()
				var restrictions int
				if err := root.QueryRow("SELECT JSON_LENGTH(User_attributes->'$.Restrictions') FROM mysql.user WHERE User='cdc' AND Host='%' ").Scan(&restrictions); err != nil || restrictions != 1 || !h.sawEvent("TenantPrivilegeRestriction") {
					t.Fatalf("unattributed restriction was not preserved and reported: %d, %v", restrictions, err)
				}
				exec("GRANT INSERT ON `acme_wms`.* TO 'cdc'@'%'")
			}
			if username == "operator" {
				exec("REVOKE IF EXISTS SELECT ON `acme\\_wms`.* FROM 'operator'@'%'")
				h.update(func(m *v1alpha1.MysqlDatabase) {})
				h.reconcile()
				h.requireReady()
				if !h.sawEvent("TenantPrivilegeRestriction") {
					t.Fatal("operator's own legacy restriction was not reported")
				}
				// A separate administrator can lift a self-restriction; the
				// operator must not turn this inert artifact into a Failed CR.
				exec("GRANT SELECT ON `acme\\_wms`.* TO 'operator'@'%'")
			}
			// Force a second apply, including redundant global/schema privileges
			// and routine DDL outside the CR's desired privilege allowlist.
			exec("GRANT DELETE, CREATE ROUTINE, ALTER ROUTINE ON `acme_wms`.* TO '" + username + "'@'%'")
			h.update(func(m *v1alpha1.MysqlDatabase) {})
			h.reconcile()
			h.requireReady()
			assertAccess("narrowing")
			h.update(func(m *v1alpha1.MysqlDatabase) { m.Spec.Grants = nil })
			h.reconcile()
			h.requireReady()
			assertAccess("removal")
			h.update(func(m *v1alpha1.MysqlDatabase) {})
			h.reconcile()
			assertAccess("removal retry")
			h.update(func(m *v1alpha1.MysqlDatabase) {
				m.Spec.Grants = []v1alpha1.MysqlDatabaseGrant{{Username: username, Privileges: []v1alpha1.MysqlPrivilege{v1alpha1.PrivilegeSelect}}}
			})
			h.reconcile()
			h.requireReady()
			exec("GRANT SELECT ON `acme\\_wms`.* TO '" + username + "'@'%'")
			if username == "cdc" {
				exec("REVOKE IF EXISTS INSERT ON `acme\\_wms`.* FROM 'cdc'@'%'")
			}
			h.delete()
			h.reconcile()
			var out v1alpha1.MysqlDatabase
			if err := h.client.Get(context.Background(), types.NamespacedName{Namespace: mdbNamespace, Name: mdbName}, &out); !apierrors.IsNotFound(err) {
				t.Fatalf("delete did not release finalizer: %v (%+v)", err, out.Status)
			}
			exec("CREATE DATABASE `acme_wms`")
			assertAccess("recreated database")
			var rows int
			if err := root.QueryRow("SELECT COUNT(*) FROM mysql.db WHERE User = ? AND (Db = ? OR Db = ?)", username, mdbDatabase, `acme\_wms`).Scan(&rows); err != nil || rows != 0 {
				t.Fatalf("direct grants survived removal/delete: %d rows, error %v", rows, err)
			}
		})
	}
}
