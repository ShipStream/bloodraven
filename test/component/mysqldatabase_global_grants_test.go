package component

import (
	"context"
	"crypto/sha256"
	"database/sql/driver"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"slices"
	"strings"
	"testing"

	mysqldriver "github.com/go-sql-driver/mysql"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"

	v1alpha1 "github.com/shipstream/bloodraven/api/v1alpha1"
)

func requireNoTenantRestrictions(t *testing.T, server *fakeSQLServer, username string) {
	t.Helper()
	server.mu.Lock()
	defer server.mu.Unlock()
	for database, accounts := range server.restrictions {
		if privileges := accounts[accountKey(username, "%")]; len(privileges) != 0 {
			t.Fatalf("%s acquired restrictions on %s: %v", username, database, privileges)
		}
	}
}

func TestMysqlDatabaseGlobalGrantsSurviveCleanupRetries(t *testing.T) {
	for _, username := range []string{"cdc", "operator", "root"} {
		t.Run(username, func(t *testing.T) {
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
			grant := v1alpha1.MysqlDatabaseGrant{Username: username, Privileges: []v1alpha1.MysqlPrivilege{v1alpha1.PrivilegeSelect}}
			cr := mdbCR(func(m *v1alpha1.MysqlDatabase) {
				m.Spec.Grants = []v1alpha1.MysqlDatabaseGrant{grant}
				m.Spec.DeletionPolicy = v1alpha1.MysqlDatabaseDelete
			})
			h := newMdbHarnessWithInterceptor(t, funcs, cr, mdbGroup("dc1"), mdbOperatorSecret(), mdbOwnerSecret())
			if username != "operator" {
				h.server.addUser(username, "shared-pw")
			}
			h.server.partialRevokes = true
			h.server.globalPrivileges[accountKey(username, "%")] = []string{"SELECT", "INSERT", "CREATE"}
			h.reconcile()
			h.requireReady()
			requireNoTenantRestrictions(t, h.server, username)
			h.server.seedGrantRow(mdbDatabase, username, "%", "SELECT", "INSERT", "CREATE ROUTINE", "ALTER ROUTINE")
			h.server.seedGrantRow(`acme\_wms`, username, "%", "SELECT", "DELETE")
			h.update(func(m *v1alpha1.MysqlDatabase) {})
			h.reconcile()
			h.requireReady()
			requireNoTenantRestrictions(t, h.server, username)
			if privileges, _ := h.server.grantRow(mdbDatabase, username, "%"); !slices.Equal(privileges, []string{"SELECT"}) {
				t.Fatalf("surplus explicit grants survived: %v", privileges)
			}
			req := ctrl.Request{NamespacedName: types.NamespacedName{Namespace: mdbNamespace, Name: mdbName}}
			h.update(func(m *v1alpha1.MysqlDatabase) { m.Spec.Grants = nil })
			failReady = true
			if _, err := h.r.Reconcile(context.Background(), req); err == nil {
				t.Fatal("Ready persistence fault did not fire")
			}
			h.reconcile()
			h.requireReady()
			requireNoTenantRestrictions(t, h.server, username)
			if _, ok := h.server.grantRow(mdbDatabase, username, "%"); ok {
				t.Fatal("removed grants[] row remains")
			}
			h.update(func(m *v1alpha1.MysqlDatabase) { m.Spec.Grants = []v1alpha1.MysqlDatabaseGrant{grant} })
			h.reconcile()
			h.requireReady()
			h.server.seedGrantRow(`acme\_wms`, username, "%", "SELECT")
			h.delete()
			failFinalizer = true
			if _, err := h.r.Reconcile(context.Background(), req); err == nil {
				t.Fatal("finalizer persistence fault did not fire")
			}
			h.reconcile()
			requireNoTenantRestrictions(t, h.server, username)
			for _, form := range []string{mdbDatabase, `acme\_wms`} {
				if _, ok := h.server.grantRow(form, username, "%"); ok {
					t.Fatalf("grant row on %q survived deletion retry", form)
				}
			}
			if !h.server.hasUser(username) {
				t.Fatal("shared account dropped")
			}
		})
	}
}

func TestMysqlDatabaseSchemaGrantReadFailureNeverBlindlyRevokes(t *testing.T) {
	h := newMdbHarness(t, mdbCR(withSupportUser), mdbGroup("dc1"), mdbOperatorSecret(), mdbOwnerSecret(), mdbSupportSecret())
	h.reconcile()
	h.requireReady()
	h.server.seedGrantRow(mdbDatabase, mdbSupportUser, "%", "SELECT", "DELETE")
	h.update(func(m *v1alpha1.MysqlDatabase) {})
	h.server.queryFault = func(query string, _ []driver.NamedValue) error {
		if strings.HasPrefix(query, "SELECT PRIVILEGE_TYPE FROM information_schema.SCHEMA_PRIVILEGES ") {
			return &mysqldriver.MySQLError{Number: 2013, Message: "injected schema grant read failure"}
		}
		return nil
	}
	start := h.server.statementCount()
	h.reconcile()
	if h.get().Status.Phase == v1alpha1.MysqlDatabasePhaseReady {
		t.Fatal("ignored schema grant read failure")
	}
	for _, stmt := range h.server.statementsSince(start) {
		if strings.HasPrefix(stmt, "REVOKE ") {
			t.Fatalf("fell back to a blind revoke after read failure: %s", stmt)
		}
	}
	h.server.clearFaults()
	h.reconcile()
	h.requireReady()
	if privileges, _ := h.server.grantRow(mdbDatabase, mdbSupportUser, "%"); !slices.Equal(privileges, []string{"SELECT"}) {
		t.Fatalf("retry did not narrow explicit row: %v", privileges)
	}
}

func TestMysqlDatabaseRepairsOnlyIdentifiableLegacyRestrictions(t *testing.T) {
	h := newMdbHarness(t, mdbCR(func(m *v1alpha1.MysqlDatabase) {
		m.Spec.Grants = []v1alpha1.MysqlDatabaseGrant{{Username: "cdc", Privileges: []v1alpha1.MysqlPrivilege{v1alpha1.PrivilegeSelect}}}
	}), mdbGroup("dc1"), mdbOperatorSecret(), mdbOwnerSecret())
	h.server.addUser("cdc", "shared-pw")
	h.server.globalPrivileges["cdc@%"] = []string{"SELECT", "INSERT"}
	h.reconcile()
	h.requireReady()
	h.server.restrictions[`acme\_wms`] = map[string][]string{"cdc@%": {"SELECT", "INSERT"}, "operator@%": {"SELECT"}}
	h.server.restrictions[mdbDatabase] = map[string][]string{"cdc@%": {"SELECT", "INSERT"}}
	h.server.restrictions["other_tenant"] = map[string][]string{"cdc@%": {"INSERT"}}
	mdb := h.get()
	var owner corev1.Secret
	if err := h.client.Get(context.Background(), types.NamespacedName{Namespace: mdbNamespace, Name: mdbOwnerSecretName}, &owner); err != nil {
		t.Fatal(err)
	}
	specJSON, err := json.Marshal(mdb.Spec)
	if err != nil {
		t.Fatal(err)
	}
	// An unchanged Ready v4 object must reapply after this SQL change.
	legacyHash := sha256.Sum256([]byte(fmt.Sprintf("render=4\nspec=%s\nactiveSite=dc1\nsecret=%s/%s\ngroup=\n", specJSON, owner.UID, owner.ResourceVersion)))
	mdb.Status.LastAppliedHash = hex.EncodeToString(legacyHash[:])[:16]
	if err := h.client.Status().Update(context.Background(), mdb); err != nil {
		t.Fatal(err)
	}
	h.reconcile()
	h.requireReady()
	if len(h.server.restrictions[`acme\_wms`]["cdc@%"]) == 0 || h.sawEvent("PartialRevokeLifted") {
		t.Fatal("restricted grantor must leave the inert cdc restriction intact")
	}
	if !slices.Equal(h.server.restrictions[mdbDatabase]["cdc@%"], []string{"INSERT"}) || !h.sawEvent("TenantPrivilegeRestriction") {
		t.Fatal("restriction outside the desired grant was silently lifted or not reported")
	}
	if len(h.server.restrictions["other_tenant"]["cdc@%"]) != 1 || len(h.server.restrictions[`acme\_wms`]["operator@%"]) != 1 {
		t.Fatal("repair touched an unrelated schema or unrecorded account")
	}
	// Once a separate administrator restores the grantor, a later apply can
	// repair the identifiable artifact without lifting real-schema surplus.
	delete(h.server.restrictions[`acme\_wms`], "operator@%")
	h.update(func(m *v1alpha1.MysqlDatabase) {})
	h.reconcile()
	h.requireReady()
	if len(h.server.restrictions[`acme\_wms`]["cdc@%"]) != 0 || !h.sawEvent("PartialRevokeLifted") {
		t.Fatal("unrestricted grantor did not repair the identifiable artifact")
	}
}

func TestMysqlDatabaseHostGrantRowsAreReadSeparately(t *testing.T) {
	h := newMdbHarness(t, mdbCR(func(m *v1alpha1.MysqlDatabase) {
		m.Spec.Users = []v1alpha1.MysqlDatabaseUser{selectUserEntry(mdbSupportSecretName, hostA, hostB)}
	}), mdbGroup("dc1"), mdbOperatorSecret(), mdbOwnerSecret(), mdbSupportSecret())
	h.reconcile()
	h.requireReady()
	h.server.seedGrantRow(mdbDatabase, mdbSupportUser, hostA, "SELECT", "DELETE")
	h.server.seedGrantRow(mdbDatabase, mdbSupportUser, hostB, "SELECT", "CREATE ROUTINE")
	h.server.globalPrivileges[accountKey(mdbSupportUser, hostA)] = []string{"CREATE ROUTINE"}
	h.server.globalPrivileges[accountKey(mdbSupportUser, hostB)] = []string{"DELETE"}
	h.update(func(m *v1alpha1.MysqlDatabase) {})
	h.reconcile()
	h.requireReady()
	for _, host := range []string{hostA, hostB} {
		if privileges, _ := h.server.grantRow(mdbDatabase, mdbSupportUser, host); !slices.Equal(privileges, []string{"SELECT"}) {
			t.Fatalf("host %s retains surplus: %v", host, privileges)
		}
		if len(h.server.restrictions[mdbDatabase][accountKey(mdbSupportUser, host)]) != 0 {
			t.Fatalf("host %s acquired a restriction from another host's row", host)
		}
	}
}
