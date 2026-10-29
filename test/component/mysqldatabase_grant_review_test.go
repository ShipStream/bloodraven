package component

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"os"
	"slices"
	"strings"
	"testing"
	"time"

	mysqldriver "github.com/go-sql-driver/mysql"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"

	v1alpha1 "github.com/shipstream/bloodraven/api/v1alpha1"
)

func newGrantReviewHarness(t *testing.T, backend string, funcs interceptor.Funcs, mutate ...func(*v1alpha1.MysqlDatabase)) (*mdbHarness, *sql.DB) {
	t.Helper()
	if backend == "mysql" && os.Getenv("BLOODRAVEN_TENANT_TEST_ADDR") == "" {
		t.Skip("set BLOODRAVEN_TENANT_TEST_ADDR to a disposable MySQL server")
	}
	h := newMdbHarnessWithInterceptor(t, funcs, mdbCR(mutate...), mdbGroup("dc1"), mdbOperatorSecret(), mdbOwnerSecret())
	if backend == "fake" {
		h.server.partialRevokes = true
		h.server.addUser("cdc", "cdc-pw")
		h.server.globalPrivileges["cdc@%"] = []string{"SELECT", "INSERT"}
		return h, nil
	}
	connect := func(user, password string) (*sql.DB, error) {
		cfg := mysqldriver.NewConfig()
		cfg.User, cfg.Passwd, cfg.Net, cfg.Addr = user, password, "tcp", os.Getenv("BLOODRAVEN_TENANT_TEST_ADDR")
		cfg.Timeout, cfg.ReadTimeout, cfg.WriteTimeout = 5*time.Second, 5*time.Second, 5*time.Second
		return sql.Open("mysql", cfg.FormatDSN())
	}
	admin, err := connect("root", "")
	if err != nil {
		t.Fatal(err)
	}
	clean := func() {
		grantReviewExec(t, admin, "DROP DATABASE IF EXISTS `acme_wms`")
		for _, user := range []string{"operator", "cdc", mdbOwnerUser, "CaseOwner", "next_owner"} {
			grantReviewExec(t, admin, "DROP USER IF EXISTS '"+user+"'@'%', '"+user+"'@'2001:db8::a'")
		}
	}
	t.Cleanup(func() { defer admin.Close(); clean() })
	clean()
	grantReviewExec(t, admin, "CREATE USER 'operator'@'%' IDENTIFIED BY 'op-pw'")
	grantReviewExec(t, admin, "GRANT ALL PRIVILEGES ON *.* TO 'operator'@'%' WITH GRANT OPTION")
	grantReviewExec(t, admin, "CREATE USER 'cdc'@'%' IDENTIFIED BY 'cdc-pw'")
	grantReviewExec(t, admin, "GRANT SELECT, INSERT ON *.* TO 'cdc'@'%'")
	h.r.OpenDB = func(user, password, _, _ string) (*sql.DB, error) { return connect(user, password) }
	return h, admin
}

func grantReviewExec(t *testing.T, admin *sql.DB, statement string) {
	t.Helper()
	if _, err := admin.Exec(statement); err != nil {
		t.Fatalf("%s: %v", statement, err)
	}
}

func requireGrantReviewDeleted(t *testing.T, h *mdbHarness) {
	t.Helper()
	var out v1alpha1.MysqlDatabase
	if err := h.client.Get(context.Background(), types.NamespacedName{Namespace: mdbNamespace, Name: mdbName}, &out); !apierrors.IsNotFound(err) {
		t.Fatalf("finalizer did not release: %v, status=%+v", err, out.Status)
	}
}

func TestMysqlDatabaseRestrictedGrantorDoesNotBlockTenant(t *testing.T) {
	for _, backend := range []string{"fake", "mysql"} {
		t.Run(backend, func(t *testing.T) {
			h, admin := newGrantReviewHarness(t, backend, interceptor.Funcs{}, func(m *v1alpha1.MysqlDatabase) {
				m.Spec.DeletionPolicy = v1alpha1.MysqlDatabaseDelete
				m.Spec.Grants = []v1alpha1.MysqlDatabaseGrant{{Username: "cdc", Privileges: []v1alpha1.MysqlPrivilege{v1alpha1.PrivilegeSelect}}}
			})
			h.reconcile()
			h.requireReady()
			if admin == nil {
				h.server.restrictions[`acme\_wms`] = map[string][]string{"operator@%": {"SELECT"}, "cdc@%": {"SELECT"}}
			} else {
				grantReviewExec(t, admin, "REVOKE SELECT ON `acme\\_wms`.* FROM 'operator'@'%', 'cdc'@'%'")
			}
			h.update(func(m *v1alpha1.MysqlDatabase) {})
			h.reconcile()
			h.requireReady()
			if !h.sawEvent("TenantPrivilegeRestriction") || h.sawEvent("PartialRevokeLifted") {
				t.Fatal("blocked legacy repair must warn and preserve the inert restrictions")
			}
			if len(h.seenEvents) != 1 || !strings.Contains(h.seenEvents[0], "grantor 'operator'@'%'") || !strings.Contains(h.seenEvents[0], "bloodraven-doctor playbook 10") {
				t.Fatalf("expected one warning naming the grantor and runbook, got %q", h.seenEvents)
			}
			if admin != nil {
				var restrictions int
				if err := admin.QueryRow("SELECT COUNT(*) FROM mysql.user WHERE User IN ('operator', 'cdc') AND Host = '%' AND JSON_LENGTH(User_attributes->'$.Restrictions') > 0").Scan(&restrictions); err != nil || restrictions != 2 {
					t.Fatalf("expected both inert restrictions preserved: count=%d, err=%v", restrictions, err)
				}
			}
			h.delete()
			h.reconcile()
			requireGrantReviewDeleted(t, h)
		})
	}
}

func TestMysqlDatabaseLiteralGrantorRestrictionWarnsForEachAccount(t *testing.T) {
	h, _ := newGrantReviewHarness(t, "fake", interceptor.Funcs{}, func(m *v1alpha1.MysqlDatabase) {
		m.Spec.Owner.Privileges = []v1alpha1.MysqlPrivilege{v1alpha1.PrivilegeSelect}
		m.Spec.Grants = []v1alpha1.MysqlDatabaseGrant{{Username: "cdc", Privileges: []v1alpha1.MysqlPrivilege{v1alpha1.PrivilegeSelect}}}
	})
	h.reconcile()
	h.requireReady()
	// INSERT restrictions do not prevent this SELECT-only apply. They must
	// still be reported for both the grantor and the recorded shared account.
	h.server.restrictions[mdbDatabase] = map[string][]string{"operator@%": {"INSERT"}, "cdc@%": {"INSERT"}}
	h.update(func(m *v1alpha1.MysqlDatabase) {})
	h.reconcile()
	h.requireReady()
	h.sawEvent("TenantPrivilegeRestriction")
	var grantorWarning, accountWarning, misleading bool
	for _, event := range h.seenEvents {
		if !strings.Contains(event, "TenantPrivilegeRestriction") {
			continue
		}
		grantorWarning = grantorWarning || (strings.Contains(event, "grantor 'operator'@'%'") && strings.Contains(event, `real schema "acme_wms"`))
		accountWarning = accountWarning || (strings.Contains(event, "'cdc'@'%'") && strings.Contains(event, `"acme_wms"`))
		misleading = misleading || strings.Contains(event, "skipping legacy repairs")
	}
	if !grantorWarning || !accountWarning || misleading {
		t.Fatalf("expected accurate real-schema warnings for both accounts, got %q", h.seenEvents)
	}
	for _, username := range []string{"operator@%", "cdc@%"} {
		if !slices.Equal(h.server.restrictions[mdbDatabase][username], []string{"INSERT"}) {
			t.Fatalf("reporting lifted %s's independent INSERT restriction", username)
		}
	}
}

func TestMysqlDatabaseLegacyRepairFailureIsBestEffort(t *testing.T) {
	for _, step := range []string{"metadata read", "repair grant"} {
		t.Run(step, func(t *testing.T) {
			h, _ := newGrantReviewHarness(t, "fake", interceptor.Funcs{}, func(m *v1alpha1.MysqlDatabase) {
				m.Spec.DeletionPolicy = v1alpha1.MysqlDatabaseDelete
				m.Spec.Grants = []v1alpha1.MysqlDatabaseGrant{{Username: "cdc", Privileges: []v1alpha1.MysqlPrivilege{v1alpha1.PrivilegeSelect}}}
			})
			h.reconcile()
			h.requireReady()
			h.server.restrictions[`acme\_wms`] = map[string][]string{"cdc@%": {"SELECT"}}
			if step == "metadata read" {
				h.server.queryFault = func(query string, _ []driver.NamedValue) error {
					if strings.Contains(query, "'$.Restrictions'") {
						return errors.New("injected restriction metadata read failure")
					}
					return nil
				}
			} else {
				h.server.failStatements("GRANT SELECT ON `acme\\_wms`", &mysqldriver.MySQLError{Number: 1044, Message: "repair denied"}, false)
			}
			h.update(func(m *v1alpha1.MysqlDatabase) {})
			h.reconcile()
			h.requireReady()
			if !h.sawEvent("TenantPrivilegeRestriction") {
				t.Fatal("repair failure did not emit a warning")
			}
			h.delete()
			h.reconcile()
			requireGrantReviewDeleted(t, h)
		})
	}
}

func TestMysqlDatabaseMixedCaseIPv6GrantCleanup(t *testing.T) {
	for _, backend := range []string{"fake", "mysql"} {
		for _, cleanup := range []string{"retire", "delete"} {
			t.Run(backend+"/"+cleanup, func(t *testing.T) {
				h, admin := newGrantReviewHarness(t, backend, interceptor.Funcs{}, func(m *v1alpha1.MysqlDatabase) {
					m.Spec.DeletionPolicy = v1alpha1.MysqlDatabaseDelete
					m.Spec.Owner.Hosts = []string{"%", "2001:DB8::A"}
					m.Spec.Owner.Privileges = []v1alpha1.MysqlPrivilege{v1alpha1.PrivilegeSelect, v1alpha1.PrivilegeInsert}
				})
				h.setOwnerUsername("CaseOwner", mdbOwnerPass)
				h.reconcile()
				h.requireReady()
				h.update(func(m *v1alpha1.MysqlDatabase) {
					m.Spec.Owner.Privileges = []v1alpha1.MysqlPrivilege{v1alpha1.PrivilegeSelect}
				})
				h.reconcile()
				h.requireReady()
				if admin == nil {
					privileges, _ := h.server.grantRow(mdbDatabase, "CaseOwner", "2001:db8::a")
					if !slices.Equal(privileges, []string{"SELECT"}) {
						t.Fatalf("Ready IPv6 owner retains surplus privileges: %v", privileges)
					}
				} else {
					var insertPrivilege string
					if err := admin.QueryRow("SELECT Insert_priv FROM mysql.db WHERE User = 'CaseOwner' AND Host = '2001:db8::a' AND Db = ?", mdbDatabase).Scan(&insertPrivilege); err != nil || insertPrivilege != "N" {
						t.Fatalf("Ready IPv6 owner retains INSERT: %s, %v", insertPrivilege, err)
					}
				}
				// A sibling keeps the account alive, so DROP USER cannot hide
				// a missing retirement or deletion revoke.
				sibling := mdbCR(func(m *v1alpha1.MysqlDatabase) {
					m.Name, m.Spec.DatabaseName, m.Spec.Owner.SecretName = "survivor", "survivor_db", "survivor-owner"
					m.Spec.Grants = []v1alpha1.MysqlDatabaseGrant{{Username: "CaseOwner", Privileges: []v1alpha1.MysqlPrivilege{v1alpha1.PrivilegeSelect}}}
				})
				if err := h.client.Create(context.Background(), userSecretFor("survivor-owner", "survivor_owner", "survivor-pw")); err != nil {
					t.Fatal(err)
				}
				if err := h.client.Create(context.Background(), sibling); err != nil {
					t.Fatal(err)
				}
				if cleanup == "retire" {
					h.setOwnerUsername("next_owner", mdbOwnerPass)
					h.reconcile()
					h.requireReady()
				} else {
					h.delete()
					h.reconcile()
					requireGrantReviewDeleted(t, h)
				}
				if admin == nil {
					if !h.server.hasAccount("CaseOwner", "2001:db8::a") {
						t.Fatal("test requires surviving IPv6 account")
					}
					if privileges, ok := h.server.grantRow(mdbDatabase, "CaseOwner", "2001:db8::a"); ok {
						t.Fatalf("IPv6 grant row survived cleanup: %v", privileges)
					}
				} else {
					var accounts, rows int
					if err := admin.QueryRow("SELECT COUNT(*) FROM mysql.user WHERE User = 'CaseOwner' AND Host = '2001:db8::a'").Scan(&accounts); err != nil || accounts != 1 {
						t.Fatalf("test requires surviving IPv6 account: %d, %v", accounts, err)
					}
					if err := admin.QueryRow("SELECT COUNT(*) FROM mysql.db WHERE User = 'CaseOwner' AND Host = '2001:db8::a' AND Db = ?", mdbDatabase).Scan(&rows); err != nil || rows != 0 {
						t.Fatalf("IPv6 grant row survived cleanup: %d, %v", rows, err)
					}
				}
			})
		}
	}
}

func TestMysqlDatabaseFakeRejectsRestrictedGrantorStatements(t *testing.T) {
	h, _ := newGrantReviewHarness(t, "fake", interceptor.Funcs{})
	h.server.restrictions[`acme\_wms`] = map[string][]string{"operator@%": {"SELECT"}, "cdc@%": {"SELECT"}}
	db, err := h.server.dialer()("operator", "op-pw", "primary", "")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	for _, stmt := range []string{
		"GRANT SELECT ON `acme\\_wms`.* TO 'cdc'@'%'",
		"REVOKE IF EXISTS SELECT ON `acme\\_wms`.* FROM 'cdc'@'%' IGNORE UNKNOWN USER",
	} {
		_, err := db.Exec(stmt)
		var mysqlErr *mysqldriver.MySQLError
		if !errors.As(err, &mysqlErr) || mysqlErr.Number != 1044 {
			t.Errorf("restricted grantor executed %q: %v", stmt, err)
		}
	}
}

func TestMysqlDatabaseUnknownSchemaPrivilegeStillFails(t *testing.T) {
	h, _ := newGrantReviewHarness(t, "fake", interceptor.Funcs{}, func(m *v1alpha1.MysqlDatabase) {
		m.Spec.Grants = []v1alpha1.MysqlDatabaseGrant{{Username: "cdc", Privileges: []v1alpha1.MysqlPrivilege{v1alpha1.PrivilegeSelect}}}
	})
	h.reconcile()
	h.requireReady()
	h.server.seedGrantRow(mdbDatabase, "cdc", "%", "UNRECOGNIZED")
	h.update(func(m *v1alpha1.MysqlDatabase) {})
	h.reconcile()
	if mdb := h.get(); mdb.Status.Phase != v1alpha1.MysqlDatabasePhaseFailed || !strings.Contains(mdb.Status.Message, "unsupported schema privilege") {
		t.Fatalf("unknown schema privilege was ignored: %+v", mdb.Status)
	}
}

func TestMysqlDatabaseGrantOnlyRowsSurviveCleanupRetry(t *testing.T) {
	for _, backend := range []string{"fake", "mysql"} {
		for _, cleanup := range []string{"remove", "delete"} {
			t.Run(backend+"/"+cleanup, func(t *testing.T) {
				var failReady, failFinalizer bool
				funcs := interceptor.Funcs{
					SubResourcePatch: func(ctx context.Context, c client.Client, sub string, obj client.Object, patch client.Patch, opts ...client.SubResourcePatchOption) error {
						if m, ok := obj.(*v1alpha1.MysqlDatabase); ok && failReady && m.Status.Phase == v1alpha1.MysqlDatabasePhaseReady {
							failReady = false
							return errors.New("injected Ready patch failure")
						}
						return c.SubResource(sub).Patch(ctx, obj, patch, opts...)
					},
					Update: func(ctx context.Context, c client.WithWatch, obj client.Object, opts ...client.UpdateOption) error {
						if m, ok := obj.(*v1alpha1.MysqlDatabase); ok && failFinalizer && !m.DeletionTimestamp.IsZero() && len(m.Finalizers) == 0 {
							failFinalizer = false
							return errors.New("injected finalizer update failure")
						}
						return c.Update(ctx, obj, opts...)
					},
				}
				h, admin := newGrantReviewHarness(t, backend, funcs, func(m *v1alpha1.MysqlDatabase) {
					m.Spec.DeletionPolicy = v1alpha1.MysqlDatabaseDelete
					m.Spec.Grants = []v1alpha1.MysqlDatabaseGrant{{Username: "cdc", Privileges: []v1alpha1.MysqlPrivilege{v1alpha1.PrivilegeSelect}}}
				})
				h.reconcile()
				h.requireReady()
				if admin == nil {
					h.server.grantOptions[mdbDatabase] = map[string]bool{"cdc@%": true}
				} else {
					grantReviewExec(t, admin, "GRANT SELECT ON `acme_wms`.* TO 'cdc'@'%' WITH GRANT OPTION")
				}
				if cleanup == "remove" {
					h.update(func(m *v1alpha1.MysqlDatabase) { m.Spec.Grants = nil })
					failReady = true
				} else {
					h.delete()
					failFinalizer = true
				}
				req := ctrl.Request{NamespacedName: types.NamespacedName{Namespace: mdbNamespace, Name: mdbName}}
				if _, err := h.r.Reconcile(context.Background(), req); err == nil {
					t.Fatal("persistence fault did not fire")
				}
				h.reconcile()
				if cleanup == "remove" {
					h.requireReady()
				} else {
					requireGrantReviewDeleted(t, h)
				}
				if admin == nil {
					if !h.server.grantOptions[mdbDatabase]["cdc@%"] {
						t.Fatal("tenant cleanup changed independently managed GRANT OPTION")
					}
				} else {
					var privilege, grantable string
					if err := admin.QueryRow("SELECT PRIVILEGE_TYPE, IS_GRANTABLE FROM information_schema.SCHEMA_PRIVILEGES WHERE GRANTEE = ? AND TABLE_SCHEMA = ?", "'cdc'@'%'", mdbDatabase).Scan(&privilege, &grantable); err != nil || privilege != "USAGE" || grantable != "YES" {
						t.Fatalf("grant-only row changed: %s/%s, %v", privilege, grantable, err)
					}
				}
			})
		}
	}
}
