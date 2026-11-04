package component

import (
	"context"
	"database/sql"
	"errors"
	"os"
	"slices"
	"strings"
	"testing"

	mysqldriver "github.com/go-sql-driver/mysql"

	v1alpha1 "github.com/shipstream/bloodraven/api/v1alpha1"
)

// The deploy principal of issue #172: a users[] account that maintains the
// tenant's stored routines without being the schema owner.
var (
	deployPrivileges = []v1alpha1.MysqlPrivilege{
		v1alpha1.PrivilegeAlterRoutine, v1alpha1.PrivilegeSelect,
		v1alpha1.PrivilegeCreateRoutine, v1alpha1.PrivilegeExecute,
	}
	narrowedDeployPrivileges = []v1alpha1.MysqlPrivilege{v1alpha1.PrivilegeSelect, v1alpha1.PrivilegeExecute}
)

func withDeployUser(privileges []v1alpha1.MysqlPrivilege) func(*v1alpha1.MysqlDatabase) {
	return func(m *v1alpha1.MysqlDatabase) {
		m.Spec.Users = []v1alpha1.MysqlDatabaseUser{{SecretName: mdbSupportSecretName, Privileges: privileges}}
	}
}

func TestMysqlDatabaseUsersRoutinePrivileges(t *testing.T) {
	h := newMdbHarness(t, mdbCR(withDeployUser(deployPrivileges)), mdbGroup("dc1"), mdbOperatorSecret(), mdbOwnerSecret(), mdbSupportSecret())
	h.server.partialRevokes = true

	h.reconcile()
	h.requireReady()
	want := []string{"SELECT", "EXECUTE", "CREATE ROUTINE", "ALTER ROUTINE"}
	if got := h.server.statementsSince(0); !slices.Contains(got, "GRANT "+strings.Join(want, ", ")+" ON `acme_wms`.* TO 'acme_support'@'%'") {
		t.Fatalf("routine privileges not granted in canonical order; statements: %v", got)
	}
	if privs, ok := h.server.grantsFor(mdbDatabase, mdbSupportUser); !ok || !sameSet(privs, want) {
		t.Fatalf("deploy grants = %v (present=%v), want %v", privs, ok, want)
	}

	h.update(withDeployUser(narrowedDeployPrivileges))
	h.reconcile()
	h.requireReady()
	if privs, _ := h.server.grantsFor(mdbDatabase, mdbSupportUser); !sameSet(privs, []string{"SELECT", "EXECUTE"}) {
		t.Fatalf("grants after narrowing = %v, want routine privileges revoked", privs)
	}
	h.server.assertNoGrantOption(t)
}

// TestMysqlDatabaseRoutinePrivilegesMySQL checks the grants against a real
// server: the deploy principal can replace a stored function and procedure,
// and loses that after its entry is narrowed. See README.md for the
// disposable-server run instructions.
func TestMysqlDatabaseRoutinePrivilegesMySQL(t *testing.T) {
	addr := os.Getenv("BLOODRAVEN_TENANT_TEST_ADDR")
	if addr == "" {
		t.Skip("set BLOODRAVEN_TENANT_TEST_ADDR to a disposable MySQL server")
	}
	connect := func(user, password string) *sql.DB {
		t.Helper()
		cfg := mysqldriver.NewConfig()
		cfg.User, cfg.Passwd, cfg.Net, cfg.Addr = user, password, "tcp", addr
		cfg.AllowNativePasswords = true
		db, err := sql.Open("mysql", cfg.FormatDSN())
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _ = db.Close() })
		return db
	}
	root := connect("root", "")
	exec := func(db *sql.DB, stmt string) {
		t.Helper()
		if _, err := db.ExecContext(context.Background(), stmt); err != nil {
			t.Fatalf("%s: %v", stmt, err)
		}
	}
	// Binary logging is on, as on every Bloodraven site, so function DDL
	// from an account without SET_ANY_DEFINER needs the setting Bloodraven
	// puts in my.cnf by default.
	var trust int
	if err := root.QueryRow("SELECT @@GLOBAL.log_bin_trust_function_creators").Scan(&trust); err != nil {
		t.Fatal(err)
	}
	exec(root, "SET GLOBAL log_bin_trust_function_creators = 1")
	t.Cleanup(func() {
		if trust == 0 {
			exec(root, "SET GLOBAL log_bin_trust_function_creators = 0")
		}
	})
	exec(root, "DROP DATABASE IF EXISTS `acme_wms`")
	exec(root, "DROP USER IF EXISTS 'acme_app'@'%', 'acme_support'@'%', 'operator'@'%'")
	exec(root, "CREATE USER 'operator'@'%' IDENTIFIED BY 'op-pw'")
	exec(root, "GRANT ALL PRIVILEGES ON *.* TO 'operator'@'%' WITH GRANT OPTION")
	t.Cleanup(func() {
		exec(root, "DROP DATABASE IF EXISTS `acme_wms`")
		exec(root, "DROP USER IF EXISTS 'acme_app'@'%', 'acme_support'@'%', 'operator'@'%'")
	})

	h := newMdbHarness(t, mdbCR(withDeployUser(deployPrivileges)), mdbGroup("dc1"), mdbOperatorSecret(), mdbOwnerSecret(), mdbSupportSecret())
	h.r.OpenDB = func(user, password, _, _ string) (*sql.DB, error) { return connect(user, password), nil }
	h.reconcile()
	h.requireReady()

	deploy := connect(mdbSupportUser, mdbSupportPass)
	// A deploy script replaces its routines on every run; do it twice.
	for range 2 {
		for _, stmt := range []string{
			"DROP FUNCTION IF EXISTS acme_wms.routine_probe",
			"CREATE FUNCTION acme_wms.routine_probe(i INT) RETURNS INT DETERMINISTIC NO SQL RETURN i + 1",
			"DROP PROCEDURE IF EXISTS acme_wms.routine_probe_proc",
			"CREATE PROCEDURE acme_wms.routine_probe_proc() SELECT acme_wms.routine_probe(1)",
		} {
			exec(deploy, stmt)
		}
	}
	var got int
	if err := deploy.QueryRow("SELECT acme_wms.routine_probe(41)").Scan(&got); err != nil || got != 42 {
		t.Fatalf("calling the deployed function = %d, %v; want 42", got, err)
	}
	exec(deploy, "CALL acme_wms.routine_probe_proc()")

	h.update(withDeployUser(narrowedDeployPrivileges))
	h.reconcile()
	h.requireReady()
	var createRoutine, alterRoutine string
	if err := root.QueryRow("SELECT Create_routine_priv, Alter_routine_priv FROM mysql.db WHERE User = ? AND Host = '%' AND Db = ?",
		mdbSupportUser, mdbDatabase).Scan(&createRoutine, &alterRoutine); err != nil || createRoutine != "N" || alterRoutine != "N" {
		t.Fatalf("schema routine privileges after narrowing: create=%q alter=%q err=%v; want N/N", createRoutine, alterRoutine, err)
	}
	_, err := deploy.Exec("CREATE FUNCTION acme_wms.routine_probe_denied() RETURNS INT DETERMINISTIC NO SQL RETURN 1")
	var myErr *mysqldriver.MySQLError
	if !errors.As(err, &myErr) || myErr.Number != 1044 {
		t.Fatalf("CREATE FUNCTION after narrowing = %v, want access denied (1044)", err)
	}
}

func sameSet(got, want []string) bool {
	return len(got) == len(want) && !slices.ContainsFunc(want, func(p string) bool { return !slices.Contains(got, p) })
}
