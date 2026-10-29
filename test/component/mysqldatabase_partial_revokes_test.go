package component

import (
	"context"
	"crypto/sha256"
	"database/sql/driver"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"reflect"
	"strings"
	"testing"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"

	v1alpha1 "github.com/shipstream/bloodraven/api/v1alpha1"
)

func TestMysqlDatabasePartialRevokesOffPreservesState(t *testing.T) {
	for _, tc := range []struct {
		name                   string
		existing, queryFailure bool
	}{
		{name: "first apply"},
		{name: "rotation cleanup", existing: true},
		{name: "cannot read setting", queryFailure: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			cr := mdbCR(withSupportUser)
			if tc.existing {
				cr.Status.DatabaseCreated = true
				cr.Status.OwnerUser = mdbOwnerUser
				cr.Status.OwnerHosts = []string{"%"}
				cr.Status.PendingOwnerUser = "abandoned"
				cr.Status.PendingOwnerHosts = []string{"%"}
			}
			h := newMdbHarness(t, cr, mdbGroup("dc1"), mdbOperatorSecret(), mdbOwnerSecret(), mdbSupportSecret())
			var attempted []string
			if tc.existing {
				h.server.addDatabase(mdbDatabase, "utf8mb4", "utf8mb4_unicode_ci")
				h.server.addUser(mdbOwnerUser, mdbOwnerPass)
				h.server.addUser("abandoned", "old-pw")
				h.server.seedGrantRow(`acme\_wms`, mdbOwnerUser, "%", "ALL PRIVILEGES")
			}
			if tc.queryFailure {
				h.server.queryFault = func(query string, _ []driver.NamedValue) error {
					if query == "SELECT @@GLOBAL.partial_revokes" {
						return errors.New("setting query unavailable")
					}
					return nil
				}
			} else {
				h.server.statementFault = func(stmt string) (bool, error) {
					attempted = append(attempted, stmt)
					if strings.HasPrefix(stmt, "SET GLOBAL partial_revokes") {
						return false, errors.New("SYSTEM_VARIABLES_ADMIN required")
					}
					return false, nil
				}
			}
			before := h.get().Status.DeepCopy()
			res := h.reconcile()
			mdb := h.get()
			if mdb.Status.Phase != v1alpha1.MysqlDatabasePhasePending || res.RequeueAfter == 0 {
				t.Fatalf("phase = %s, requeue = %s, want Pending with retry: %s", mdb.Status.Phase, res.RequeueAfter, mdb.Status.Message)
			}
			if len(mdb.Status.Conditions) != 1 || mdb.Status.Conditions[0].Reason != "PartialRevokesOff" || mdb.Status.Conditions[0].Status != metav1.ConditionFalse {
				t.Fatalf("conditions = %+v, want Ready=False/PartialRevokesOff", mdb.Status.Conditions)
			}
			if !h.sawEvent("PartialRevokesOff") || !strings.HasPrefix(h.seenEvents[0], "Warning PartialRevokesOff ") {
				t.Fatal("missing Warning PartialRevokesOff event")
			}
			want := []string{"SET GLOBAL partial_revokes = ON"}
			if tc.queryFailure {
				want = nil
			}
			if strings.Join(attempted, "\n") != strings.Join(want, "\n") || h.server.statementCount() != 0 {
				t.Fatalf("attempts = %q, executed = %q, want only the refused setting attempt %q", attempted, h.server.statementsSince(0), want)
			}
			if mdb.Status.DatabaseCreated != before.DatabaseCreated || mdb.Status.OwnerUser != before.OwnerUser ||
				mdb.Status.PendingOwnerUser != before.PendingOwnerUser || !reflect.DeepEqual(mdb.Status.OwnerHosts, before.OwnerHosts) ||
				!reflect.DeepEqual(mdb.Status.PendingOwnerHosts, before.PendingOwnerHosts) || !reflect.DeepEqual(mdb.Status.AppliedUsers, before.AppliedUsers) {
				t.Fatalf("write-ahead ledger changed: before %+v, after %+v", before, mdb.Status)
			}
			if tc.existing {
				if _, ok := h.server.grantRow(`acme\_wms`, mdbOwnerUser, "%"); !ok || !h.server.hasUser("abandoned") {
					t.Fatal("existing access or rotation target was removed while the setting was unavailable")
				}
			} else if h.server.hasUser(mdbOwnerUser) || h.server.hasUser(mdbSupportUser) {
				t.Fatal("created a tenant account before confirming the setting")
			}

			h.server.clearFaults()
			h.reconcile()
			h.requireReady()
			if !h.server.partialRevokes {
				t.Fatal("Ready without partial_revokes ON")
			}
			if _, ok := h.server.grantRow(mdbDatabase, mdbOwnerUser, "%"); !ok {
				t.Fatal("literal owner grant missing after recovery")
			}
			if _, ok := h.server.grantRow(`acme\_wms`, mdbOwnerUser, "%"); ok {
				t.Fatal("escaped owner grant remains after recovery")
			}
		})
	}
}

func TestMysqlDatabasePartialRevokesConnectionFailureIsPrimaryUnavailable(t *testing.T) {
	for _, fault := range []struct {
		name string
		err  error
	}{
		{name: "connection drop", err: io.EOF},
		{name: "bad connection", err: driver.ErrBadConn},
		{name: "deadline", err: context.DeadlineExceeded},
	} {
		for _, step := range []string{"read", "enable"} {
			t.Run(fault.name+"/"+step, func(t *testing.T) {
				h := newMdbHarness(t, mdbCR(), mdbGroup("dc1"), mdbOperatorSecret(), mdbOwnerSecret())
				if step == "read" {
					h.server.queryFault = func(query string, _ []driver.NamedValue) error {
						if query == "SELECT @@GLOBAL.partial_revokes" {
							return fault.err
						}
						return nil
					}
				} else {
					h.server.failStatements("SET GLOBAL partial_revokes", fault.err, false)
				}
				res := h.reconcile()
				mdb := h.get()
				if mdb.Status.Phase != v1alpha1.MysqlDatabasePhasePending || res.RequeueAfter == 0 {
					t.Fatalf("phase = %s, requeue = %s, want Pending with retry", mdb.Status.Phase, res.RequeueAfter)
				}
				if len(mdb.Status.Conditions) != 1 || mdb.Status.Conditions[0].Reason != "PrimaryUnavailable" || mdb.Status.Conditions[0].Status != metav1.ConditionFalse {
					t.Fatalf("conditions = %+v, want Ready=False/PrimaryUnavailable", mdb.Status.Conditions)
				}
				if h.sawEvent("PartialRevokesOff") {
					t.Fatal("connection failure emitted a misleading PartialRevokesOff warning")
				}
				if h.server.statementCount() != 0 || mdb.Status.DatabaseCreated || mdb.Status.OwnerUser != "" {
					t.Fatal("tenant SQL or write-ahead ran after a connection failure")
				}
				h.server.clearFaults()
				h.reconcile()
				h.requireReady()
			})
		}
	}
}

func TestMysqlDatabaseDeleteDoesNotRequirePartialRevokes(t *testing.T) {
	cr := mdbCR(func(m *v1alpha1.MysqlDatabase) {
		m.Spec.DeletionPolicy = v1alpha1.MysqlDatabaseDelete
		m.Spec.Grants = []v1alpha1.MysqlDatabaseGrant{{Username: "shared", Privileges: []v1alpha1.MysqlPrivilege{v1alpha1.PrivilegeSelect}}}
	})
	h := newMdbHarness(t, cr, mdbGroup("dc1"), mdbOperatorSecret(), mdbOwnerSecret())
	h.server.addUser("shared", "shared-pw")
	h.reconcile()
	h.requireReady()
	h.server.partialRevokes = false
	h.server.seedGrantRow(`acme\_wms`, "shared", "%", "SELECT")
	h.server.failStatements("SET GLOBAL partial_revokes", errors.New("setting cannot be changed"), false)
	h.server.queryFault = func(query string, _ []driver.NamedValue) error {
		if query == "SELECT @@GLOBAL.partial_revokes" {
			return errors.New("setting cannot be read")
		}
		return nil
	}
	start := h.server.statementCount()
	h.delete()
	h.reconcile()
	var out v1alpha1.MysqlDatabase
	if err := h.client.Get(context.Background(), types.NamespacedName{Namespace: mdbNamespace, Name: mdbName}, &out); !apierrors.IsNotFound(err) {
		t.Fatalf("delete did not release finalizer: object %+v, error %v", out.Status, err)
	}
	for _, spelling := range []string{mdbDatabase, `acme\_wms`} {
		if privs, ok := h.server.grantRow(spelling, "shared", "%"); ok {
			t.Fatalf("shared grant survives deletion on %q: %v", spelling, privs)
		}
	}
	for _, stmt := range h.server.statementsSince(start) {
		if strings.HasPrefix(stmt, "SET GLOBAL partial_revokes") {
			t.Fatalf("deletion attempted to change setting: %s", stmt)
		}
	}
}

func TestMysqlDatabaseReappliesFrozenRenderV3(t *testing.T) {
	for _, mode := range []bool{false, true} {
		t.Run(fmt.Sprintf("partial_revokes=%t", mode), func(t *testing.T) {
			cr := mdbCR(withSupportUser, func(m *v1alpha1.MysqlDatabase) {
				m.Spec.Grants = []v1alpha1.MysqlDatabaseGrant{{Username: "shared", Privileges: []v1alpha1.MysqlPrivilege{v1alpha1.PrivilegeSelect}}}
				m.Status = v1alpha1.MysqlDatabaseStatus{
					Phase: v1alpha1.MysqlDatabasePhaseReady, ObservedGeneration: m.Generation,
					Conditions: []metav1.Condition{{Type: "Ready", Status: metav1.ConditionTrue,
						ObservedGeneration: m.Generation, Reason: "DatabaseReconciled"}},
					DatabaseCreated: true, OwnerUser: mdbOwnerUser, OwnerHosts: []string{"%"},
					ActiveSite: "dc1", AppliedGrants: []string{mdbOwnerUser, "shared"},
					AppliedUsers: []v1alpha1.MysqlDatabaseUserState{{SecretName: mdbSupportSecretName, Username: mdbSupportUser, Hosts: []string{"%"}}},
				}
			})
			fg, owner, support := mdbGroup("dc1"), mdbOwnerSecret(), mdbSupportSecret()
			fg.UID, owner.UID, support.UID = "group-uid", "owner-uid", "support-uid"
			h := newMdbHarness(t, cr, fg, mdbOperatorSecret(), owner, support)
			mdb := h.get()
			for _, secret := range []*corev1.Secret{owner, support} {
				if err := h.client.Get(context.Background(), types.NamespacedName{Namespace: mdbNamespace, Name: secret.Name}, secret); err != nil {
					t.Fatal(err)
				}
			}
			// Freeze the v3 serialization independently of the current renderer.
			// Keeping the group, Secret revisions and generation unchanged is
			// essential: only the render version should force this apply.
			specJSON, err := json.Marshal(mdb.Spec)
			if err != nil {
				t.Fatal(err)
			}
			digest := sha256.New()
			fmt.Fprintf(digest, "render=3\nspec=%s\nactiveSite=dc1\nsecret=%s/%s\nuserSecret=%s=%s/%s\ngroup=%s\n",
				specJSON, owner.UID, owner.ResourceVersion, support.Name, support.UID, support.ResourceVersion, fg.UID)
			legacyHash := hex.EncodeToString(digest.Sum(nil))[:16]
			mdb.Status.LastAppliedHash = legacyHash
			if err := h.client.Status().Update(context.Background(), mdb); err != nil {
				t.Fatal(err)
			}
			h.server.partialRevokes = mode
			h.server.addDatabase(mdbDatabase, "utf8mb4", "utf8mb4_unicode_ci")
			for user, password := range map[string]string{mdbOwnerUser: mdbOwnerPass, mdbSupportUser: mdbSupportPass, "shared": "shared-pw"} {
				h.server.addUser(user, password)
				priv := "SELECT"
				if user == mdbOwnerUser {
					priv = "ALL PRIVILEGES"
				}
				h.server.seedGrantRow(`acme\_wms`, user, "%", priv)
			}
			h.reconcile()
			ready := h.requireReady()
			if ready.Status.LastAppliedHash == legacyHash {
				t.Fatal("Ready retained the frozen v3 hash instead of migrating escaped grants")
			}
			for _, user := range []string{mdbOwnerUser, mdbSupportUser, "shared"} {
				if _, ok := h.server.grantRow(mdbDatabase, user, "%"); !ok {
					t.Fatalf("%s has no literal schema grant", user)
				}
				if privs, ok := h.server.grantRow(`acme\_wms`, user, "%"); ok {
					t.Fatalf("%s retains escaped grant: %v", user, privs)
				}
			}
			start, dials := h.server.statementCount(), len(h.server.dialedAddrs)
			h.reconcile()
			if h.server.statementCount() != start || len(h.server.dialedAddrs) != dials {
				t.Fatal("unchanged Ready object touched MySQL after migration")
			}
		})
	}
}
