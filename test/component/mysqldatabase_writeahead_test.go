package component

import (
	"context"
	"errors"
	"reflect"
	"slices"
	"strings"
	"sync/atomic"
	"testing"

	mysqldriver "github.com/go-sql-driver/mysql"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"

	v1alpha1 "github.com/shipstream/bloodraven/api/v1alpha1"
)

// These tests pin the write-ahead invariant: a status record may name
// user@host only if that exact account was verified absent, or was already
// attributed to this CR, when the record was written — and records of
// principals none of whose statements executed are withdrawn. Every
// scenario is one in which the pre-fix reconciler adopted (reset the
// password of) or dropped an account it never created.

const (
	hostA = "10.0.0.1"
	hostB = "10.0.0.2"
	hostC = "10.0.0.3"
)

func userSecretFor(name, username, password string) *corev1.Secret {
	return &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: mdbNamespace},
		Data: map[string][]byte{
			"username": []byte(username),
			"password": []byte(password),
		},
	}
}

func selectUserEntry(secretName string, hosts ...string) v1alpha1.MysqlDatabaseUser {
	return v1alpha1.MysqlDatabaseUser{
		SecretName: secretName,
		Privileges: []v1alpha1.MysqlPrivilege{v1alpha1.PrivilegeSelect},
		Hosts:      hosts,
	}
}

func (h *mdbHarness) setOwnerUsername(username, password string) {
	h.t.Helper()
	var s corev1.Secret
	key := types.NamespacedName{Namespace: mdbNamespace, Name: mdbOwnerSecretName}
	if err := h.client.Get(context.Background(), key, &s); err != nil {
		h.t.Fatalf("get owner secret: %v", err)
	}
	s.Data["username"] = []byte(username)
	s.Data["password"] = []byte(password)
	if err := h.client.Update(context.Background(), &s); err != nil {
		h.t.Fatalf("update owner secret: %v", err)
	}
}

// requirePasswordOf fails unless the exact account exists with password.
func (h *mdbHarness) requirePasswordOf(username, host, password string) {
	h.t.Helper()
	pw, ok := h.server.passwordOf(username, host)
	if !ok {
		h.t.Fatalf("account %s@%s is gone", username, host)
	}
	if pw != password {
		h.t.Fatalf("account %s@%s password = %q, want %q (it was reset)", username, host, pw, password)
	}
}

// requireNoStatementNaming fails if any statement since n with prefix names
// the account list element 'username'.
func (h *mdbHarness) requireNoStatementNaming(n int, prefix, username string) {
	h.t.Helper()
	for _, stmt := range h.server.statementsSince(n) {
		if strings.HasPrefix(stmt, prefix) && strings.Contains(stmt, "'"+username+"'@") {
			h.t.Fatalf("reconciler ran %q", stmt)
		}
	}
}

// --- hole 1: speculative write-ahead ----------------------------------------

// TestMysqlDatabaseRefusedUsersThenRemovedNeverAdopts: two users[] entries
// name accounts someone else owns. The pre-fix reconciler stamped both,
// rolled back only the refused one, and the next reconcile — after the first
// entry was removed — adopted the second through its surviving record.
func TestMysqlDatabaseRefusedUsersThenRemovedNeverAdopts(t *testing.T) {
	cr := mdbCR(func(m *v1alpha1.MysqlDatabase) {
		m.Spec.DeletionPolicy = v1alpha1.MysqlDatabaseDelete
		m.Spec.Users = []v1alpha1.MysqlDatabaseUser{selectUserEntry("a-mysql"), selectUserEntry("b-mysql")}
	})
	h := newMdbHarness(t, cr, mdbGroup("dc1"), mdbOperatorSecret(), mdbOwnerSecret(),
		userSecretFor("a-mysql", "foreign_a", "attacker-a"), userSecretFor("b-mysql", "victim_b", "attacker-b"))
	h.server.addUser("foreign_a", "fa-pw")
	h.server.addUser("victim_b", "vb-pw")

	h.reconcile()
	mdb := h.get()
	requireFailed(t, mdb, "PreExistingUser")
	if len(mdb.Status.AppliedUsers) != 0 || mdb.Status.DatabaseCreated || mdb.Status.OwnerUser != "" {
		t.Fatalf("records written for a refused apply: created=%v owner=%q users=%+v",
			mdb.Status.DatabaseCreated, mdb.Status.OwnerUser, mdb.Status.AppliedUsers)
	}
	if n := h.server.statementCount(); n != 0 {
		t.Fatalf("a refused apply executed %d statements: %v", n, h.server.statementsSince(0))
	}

	h.update(func(m *v1alpha1.MysqlDatabase) { m.Spec.Users = m.Spec.Users[1:] })
	h.reconcile()
	mdb = h.get()
	requireFailed(t, mdb, "PreExistingUser")
	if !strings.Contains(mdb.Status.Message, "victim_b") {
		t.Fatalf("message %q does not name the refused account", mdb.Status.Message)
	}
	if len(mdb.Status.AppliedUsers) != 0 {
		t.Fatalf("status.appliedUsers = %+v, want empty", mdb.Status.AppliedUsers)
	}
	h.requirePasswordOf("foreign_a", "%", "fa-pw")
	h.requirePasswordOf("victim_b", "%", "vb-pw")

	h.delete()
	h.reconcile()
	h.requirePasswordOf("foreign_a", "%", "fa-pw")
	h.requirePasswordOf("victim_b", "%", "vb-pw")
}

// TestMysqlDatabasePreflightQueryFailureStampsNothing: an existence query
// failing is weather, and no record may be written ahead of a check that
// never answered.
func TestMysqlDatabasePreflightQueryFailureStampsNothing(t *testing.T) {
	h := newMdbHarness(t, mdbCR(withSupportUser), mdbGroup("dc1"), mdbOperatorSecret(), mdbOwnerSecret(), mdbSupportSecret())
	h.server.failQueries(mdbSupportUser, mysqldriver.ErrInvalidConn)

	h.reconcile()
	mdb := h.get()
	if mdb.Status.Phase != v1alpha1.MysqlDatabasePhasePending {
		t.Fatalf("phase = %q (message %q), want Pending", mdb.Status.Phase, mdb.Status.Message)
	}
	if mdb.Status.DatabaseCreated || mdb.Status.OwnerUser != "" || len(mdb.Status.OwnerHosts) != 0 || len(mdb.Status.AppliedUsers) != 0 {
		t.Fatalf("records written ahead of an unanswered preflight: created=%v owner=%q hosts=%v users=%+v",
			mdb.Status.DatabaseCreated, mdb.Status.OwnerUser, mdb.Status.OwnerHosts, mdb.Status.AppliedUsers)
	}
	if n := h.server.statementCount(); n != 0 {
		t.Fatalf("%d statements ran after a failed preflight", n)
	}

	h.server.clearFaults()
	h.reconcile()
	h.requireReady()
}

// TestMysqlDatabaseOwnerStatementRefusedWithdrawsUnexecutedRecords: the
// server refuses the owner's CREATE USER. The schema statements ran, so
// databaseCreated stays; the owner and users[] records describe accounts no
// statement created and are withdrawn — so a foreign account created with
// that name afterwards is refused, not adopted, and never dropped.
func TestMysqlDatabaseOwnerStatementRefusedWithdrawsUnexecutedRecords(t *testing.T) {
	cr := mdbCR(withSupportUser, func(m *v1alpha1.MysqlDatabase) { m.Spec.DeletionPolicy = v1alpha1.MysqlDatabaseDelete })
	h := newMdbHarness(t, cr, mdbGroup("dc1"), mdbOperatorSecret(), mdbOwnerSecret(), mdbSupportSecret())
	h.server.failStatements("CREATE USER IF NOT EXISTS '"+mdbOwnerUser+"'",
		&mysqldriver.MySQLError{Number: 1290, Message: "The MySQL server is running with the --super-read-only option"}, false)

	h.reconcile()
	mdb := h.get()
	if mdb.Status.Phase != v1alpha1.MysqlDatabasePhasePending {
		t.Fatalf("phase = %q (message %q), want Pending", mdb.Status.Phase, mdb.Status.Message)
	}
	if !mdb.Status.DatabaseCreated {
		t.Fatal("status.databaseCreated withdrawn although CREATE DATABASE executed")
	}
	if mdb.Status.OwnerUser != "" || len(mdb.Status.OwnerHosts) != 0 {
		t.Fatalf("owner record %q/%v survived a refused owner statement", mdb.Status.OwnerUser, mdb.Status.OwnerHosts)
	}
	if len(mdb.Status.AppliedUsers) != 0 {
		t.Fatalf("status.appliedUsers = %+v for an entry whose SQL never ran", mdb.Status.AppliedUsers)
	}

	h.server.clearFaults()
	h.server.addUser(mdbSupportUser, "foreign-password")
	h.reconcile()
	requireFailed(t, h.get(), "PreExistingUser")
	h.requirePasswordOf(mdbSupportUser, "%", "foreign-password")

	h.delete()
	h.reconcile()
	h.requirePasswordOf(mdbSupportUser, "%", "foreign-password")
}

// TestMysqlDatabaseAmbiguousOwnerFailureKeepsRecord: a connection lost
// mid-statement may have executed it, so its record must stay — withdrawing
// it would leak the account the server may have created — while the users[]
// entry that was never attempted is still withdrawn.
func TestMysqlDatabaseAmbiguousOwnerFailureKeepsRecord(t *testing.T) {
	h := newMdbHarness(t, mdbCR(withSupportUser), mdbGroup("dc1"), mdbOperatorSecret(), mdbOwnerSecret(), mdbSupportSecret())
	h.server.failStatements("CREATE USER IF NOT EXISTS '"+mdbOwnerUser+"'", mysqldriver.ErrInvalidConn, true)

	h.reconcile()
	mdb := h.get()
	if mdb.Status.Phase != v1alpha1.MysqlDatabasePhasePending {
		t.Fatalf("phase = %q (message %q), want Pending", mdb.Status.Phase, mdb.Status.Message)
	}
	if !h.server.hasAccount(mdbOwnerUser, "%") {
		t.Fatal("test premise broken: the faulted statement was not applied")
	}
	if mdb.Status.OwnerUser != mdbOwnerUser || !reflect.DeepEqual(mdb.Status.OwnerHosts, []string{"%"}) {
		t.Fatalf("owner record = %q/%v, want it kept after an ambiguous failure", mdb.Status.OwnerUser, mdb.Status.OwnerHosts)
	}
	if len(mdb.Status.AppliedUsers) != 0 {
		t.Fatalf("status.appliedUsers = %+v for an entry that was never attempted", mdb.Status.AppliedUsers)
	}

	h.server.clearFaults()
	h.reconcile()
	h.requireReady()
}

// TestMysqlDatabaseRefusedRotationStatementWithdrawsPendingOwner: a rotation
// whose CREATE USER the server refused must not leave the target in
// status.pendingOwnerUser, or a foreign account created under that name later
// would be adopted by the retry and dropped by Delete.
func TestMysqlDatabaseRefusedRotationStatementWithdrawsPendingOwner(t *testing.T) {
	cr := mdbCR(func(m *v1alpha1.MysqlDatabase) { m.Spec.DeletionPolicy = v1alpha1.MysqlDatabaseDelete })
	h := newMdbHarness(t, cr, mdbGroup("dc1"), mdbOperatorSecret(), mdbOwnerSecret())
	h.reconcile()
	h.requireReady()

	const next = "acme_app_v2"
	h.setOwnerUsername(next, "owner-pw-2")
	h.server.failStatements("CREATE USER IF NOT EXISTS '"+next+"'",
		&mysqldriver.MySQLError{Number: 1819, Message: "Your password does not satisfy the current policy requirements"}, false)

	h.reconcile()
	mdb := h.get()
	requireFailed(t, mdb, "MySQLError")
	if mdb.Status.PendingOwnerUser != "" || len(mdb.Status.PendingOwnerHosts) != 0 {
		t.Fatalf("pending owner record %q/%v survived a refused rotation", mdb.Status.PendingOwnerUser, mdb.Status.PendingOwnerHosts)
	}
	if mdb.Status.OwnerUser != mdbOwnerUser {
		t.Fatalf("status.ownerUser = %q, want the previous owner kept", mdb.Status.OwnerUser)
	}

	h.server.clearFaults()
	h.server.addUser(next, "foreign-password")
	h.reconcile()
	requireFailed(t, h.get(), "PreExistingOwnerUser")
	h.requirePasswordOf(next, "%", "foreign-password")

	h.delete()
	h.reconcile()
	h.requirePasswordOf(next, "%", "foreign-password")
}

// --- hole 2: adoption is per user@host --------------------------------------

// TestMysqlDatabaseHostAdditionOverForeignAccountRefused: a managed X@H1
// does not make a foreign X@H2 this CR's — for the owner or a users[] entry.
func TestMysqlDatabaseHostAdditionOverForeignAccountRefused(t *testing.T) {
	t.Run("owner", func(t *testing.T) {
		cr := mdbCR(withOwnerHosts(hostA), func(m *v1alpha1.MysqlDatabase) { m.Spec.DeletionPolicy = v1alpha1.MysqlDatabaseDelete })
		h := newMdbHarness(t, cr, mdbGroup("dc1"), mdbOperatorSecret(), mdbOwnerSecret())
		h.reconcile()
		h.requireReady()

		h.server.addAccount(mdbOwnerUser, hostB, "foreign-password")
		h.update(func(m *v1alpha1.MysqlDatabase) { m.Spec.Owner.Hosts = []string{hostA, hostB} })
		h.reconcile()
		mdb := h.get()
		requireFailed(t, mdb, "PreExistingOwnerUser")
		h.requirePasswordOf(mdbOwnerUser, hostB, "foreign-password")
		if !reflect.DeepEqual(mdb.Status.OwnerHosts, []string{hostA}) {
			t.Fatalf("status.ownerHosts = %v, want [%s]", mdb.Status.OwnerHosts, hostA)
		}

		h.delete()
		h.reconcile()
		h.requirePasswordOf(mdbOwnerUser, hostB, "foreign-password")
		if h.server.hasAccount(mdbOwnerUser, hostA) {
			t.Fatal("the CR's own owner account survived Delete")
		}
	})

	t.Run("users", func(t *testing.T) {
		cr := mdbCR(withSupportUser, withSupportHosts(hostA), func(m *v1alpha1.MysqlDatabase) { m.Spec.DeletionPolicy = v1alpha1.MysqlDatabaseDelete })
		h := newMdbHarness(t, cr, mdbGroup("dc1"), mdbOperatorSecret(), mdbOwnerSecret(), mdbSupportSecret())
		h.reconcile()
		h.requireReady()

		h.server.addAccount(mdbSupportUser, hostB, "foreign-password")
		h.update(func(m *v1alpha1.MysqlDatabase) { m.Spec.Users[0].Hosts = []string{hostA, hostB} })
		h.reconcile()
		mdb := h.get()
		requireFailed(t, mdb, "PreExistingUser")
		h.requirePasswordOf(mdbSupportUser, hostB, "foreign-password")
		if len(mdb.Status.AppliedUsers) != 1 || !reflect.DeepEqual(mdb.Status.AppliedUsers[0].Hosts, []string{hostA}) {
			t.Fatalf("status.appliedUsers = %+v, want hosts [%s]", mdb.Status.AppliedUsers, hostA)
		}

		h.delete()
		h.reconcile()
		h.requirePasswordOf(mdbSupportUser, hostB, "foreign-password")
		if h.server.hasAccount(mdbSupportUser, hostA) {
			t.Fatal("the CR's own users[] account survived Delete")
		}
	})
}

// --- hole 3: drops use exact per-name hosts ----------------------------------

// TestMysqlDatabaseRotationWithHostChangeDropsOnlyRecordedHosts: A@H1 rotates
// to B@H2 while a foreign A@H2 exists. The pre-fix drop of A ran over the
// union of A's and B's hosts and took the foreign A@H2 with it.
func TestMysqlDatabaseRotationWithHostChangeDropsOnlyRecordedHosts(t *testing.T) {
	t.Run("owner", func(t *testing.T) {
		h := newMdbHarness(t, mdbCR(withOwnerHosts(hostA)), mdbGroup("dc1"), mdbOperatorSecret(), mdbOwnerSecret())
		h.reconcile()
		h.requireReady()

		h.server.addAccount(mdbOwnerUser, hostB, "foreign-password")
		h.setOwnerUsername("acme_app_v2", "owner-pw-2")
		h.update(func(m *v1alpha1.MysqlDatabase) { m.Spec.Owner.Hosts = []string{hostB} })
		before := h.server.statementCount()
		h.reconcile()
		mdb := h.requireReady()

		if h.server.hasAccount(mdbOwnerUser, hostA) {
			t.Fatal("rotated-away owner survived on its recorded host")
		}
		h.requirePasswordOf(mdbOwnerUser, hostB, "foreign-password")
		h.requirePasswordOf("acme_app_v2", hostB, "owner-pw-2")
		if mdb.Status.OwnerUser != "acme_app_v2" || !reflect.DeepEqual(mdb.Status.OwnerHosts, []string{hostB}) {
			t.Fatalf("owner record = %q/%v", mdb.Status.OwnerUser, mdb.Status.OwnerHosts)
		}
		// The removed-grants revoke no longer touches the old name on '%':
		// it is revoked on its recorded hosts by its own retirement path.
		for _, stmt := range h.server.statementsSince(before) {
			if strings.HasPrefix(stmt, "REVOKE") && strings.Contains(stmt, "'"+mdbOwnerUser+"'@'"+hostB+"'") {
				t.Fatalf("%q touched the foreign account", stmt)
			}
			if strings.HasPrefix(stmt, "REVOKE") && strings.Contains(stmt, "'"+mdbOwnerUser+"'@'%'") {
				t.Fatalf("%q revoked the old owner on a host it was never recorded on", stmt)
			}
		}
	})

	t.Run("users", func(t *testing.T) {
		h := newMdbHarness(t, mdbCR(withSupportUser, withSupportHosts(hostA)), mdbGroup("dc1"), mdbOperatorSecret(), mdbOwnerSecret(), mdbSupportSecret())
		h.reconcile()
		h.requireReady()

		h.server.addAccount(mdbSupportUser, hostB, "foreign-password")
		h.updateSupportSecret(func(s *corev1.Secret) {
			s.Data["username"] = []byte("acme_support_v2")
			s.Data["password"] = []byte("support-pw-2")
		})
		h.update(func(m *v1alpha1.MysqlDatabase) { m.Spec.Users[0].Hosts = []string{hostB} })
		h.reconcile()
		h.requireReady()

		if h.server.hasAccount(mdbSupportUser, hostA) {
			t.Fatal("rotated-away users[] principal survived on its recorded host")
		}
		h.requirePasswordOf(mdbSupportUser, hostB, "foreign-password")
		h.requirePasswordOf("acme_support_v2", hostB, "support-pw-2")
	})
}

// TestMysqlDatabaseStalePendingTargetDropsOnlyItsHosts: status records
// A@H1 with pending B@H2 and the Secret moves on to C@H3 while foreign B@H1
// and B@H3 exist. Only B@H2 — the one account B's record names — may go.
func TestMysqlDatabaseStalePendingTargetDropsOnlyItsHosts(t *testing.T) {
	const (
		prev  = "acme_prev"
		stale = "acme_stale"
		next  = "acme_next"
	)

	t.Run("owner", func(t *testing.T) {
		cr := mdbCR(withOwnerHosts(hostC), func(m *v1alpha1.MysqlDatabase) {
			m.Status.DatabaseCreated = true
			m.Status.OwnerUser, m.Status.OwnerHosts = prev, []string{hostA}
			m.Status.PendingOwnerUser, m.Status.PendingOwnerHosts = stale, []string{hostB}
		})
		secret := mdbOwnerSecret()
		secret.Data["username"] = []byte(next)
		h := newMdbHarness(t, cr, mdbGroup("dc1"), mdbOperatorSecret(), secret)
		h.server.addAccount(prev, hostA, "prev-pw")
		h.server.addAccount(stale, hostB, "stale-pw")
		h.server.addAccount(stale, hostA, "foreign-1")
		h.server.addAccount(stale, hostC, "foreign-3")

		h.reconcile()
		mdb := h.requireReady()

		if h.server.hasAccount(stale, hostB) {
			t.Fatalf("stale target %s@%s survived", stale, hostB)
		}
		h.requirePasswordOf(stale, hostA, "foreign-1")
		h.requirePasswordOf(stale, hostC, "foreign-3")
		if h.server.hasAccount(prev, hostA) {
			t.Fatal("previous owner survived the rotation")
		}
		if mdb.Status.OwnerUser != next || mdb.Status.PendingOwnerUser != "" || len(mdb.Status.PendingOwnerHosts) != 0 {
			t.Fatalf("owner record = %q pending %q/%v", mdb.Status.OwnerUser, mdb.Status.PendingOwnerUser, mdb.Status.PendingOwnerHosts)
		}
	})

	t.Run("users", func(t *testing.T) {
		cr := mdbCR(withSupportUser, withSupportHosts(hostC), func(m *v1alpha1.MysqlDatabase) {
			m.Status.DatabaseCreated = true
			m.Status.OwnerUser = mdbOwnerUser
			m.Status.AppliedUsers = []v1alpha1.MysqlDatabaseUserState{{
				SecretName: mdbSupportSecretName,
				Username:   prev, Hosts: []string{hostA},
				PendingUsername: stale, PendingHosts: []string{hostB},
			}}
		})
		support := mdbSupportSecret()
		support.Data["username"] = []byte(next)
		h := newMdbHarness(t, cr, mdbGroup("dc1"), mdbOperatorSecret(), mdbOwnerSecret(), support)
		h.server.addUser(mdbOwnerUser, mdbOwnerPass)
		h.server.addAccount(prev, hostA, "prev-pw")
		h.server.addAccount(stale, hostB, "stale-pw")
		h.server.addAccount(stale, hostA, "foreign-1")
		h.server.addAccount(stale, hostC, "foreign-3")

		h.reconcile()
		mdb := h.requireReady()

		if h.server.hasAccount(stale, hostB) {
			t.Fatalf("stale target %s@%s survived", stale, hostB)
		}
		h.requirePasswordOf(stale, hostA, "foreign-1")
		h.requirePasswordOf(stale, hostC, "foreign-3")
		if h.server.hasAccount(prev, hostA) {
			t.Fatal("previous users[] principal survived the rotation")
		}
		if len(mdb.Status.AppliedUsers) != 1 || mdb.Status.AppliedUsers[0].Username != next ||
			mdb.Status.AppliedUsers[0].PendingUsername != "" || len(mdb.Status.AppliedUsers[0].PendingHosts) != 0 {
			t.Fatalf("status.appliedUsers = %+v", mdb.Status.AppliedUsers)
		}
	})
}

// TestMysqlDatabaseLegacyPendingWithoutPendingHosts: a rotation straddling
// the upgrade has a pending name but no pending hosts; it falls back to the
// shared hosts record the previous operator wrote and converges.
func TestMysqlDatabaseLegacyPendingWithoutPendingHosts(t *testing.T) {
	const next = "acme_app_v2"
	cr := mdbCR(withOwnerHosts(hostB), func(m *v1alpha1.MysqlDatabase) {
		m.Status.DatabaseCreated = true
		m.Status.Phase = v1alpha1.MysqlDatabasePhaseCreating
		m.Status.OwnerUser = mdbOwnerUser
		m.Status.OwnerHosts = []string{hostA, hostB}
		m.Status.PendingOwnerUser = next
	})
	secret := mdbOwnerSecret()
	secret.Data["username"] = []byte(next)
	secret.Data["password"] = []byte("owner-pw-2")
	h := newMdbHarness(t, cr, mdbGroup("dc1"), mdbOperatorSecret(), secret)
	h.server.addAccount(mdbOwnerUser, hostA, mdbOwnerPass)
	h.server.addAccount(next, hostB, "owner-pw-2")

	h.reconcile()
	mdb := h.requireReady() // not Failed/PreExistingOwnerUser
	h.requirePasswordOf(next, hostB, "owner-pw-2")
	if h.server.hasUser(mdbOwnerUser) {
		t.Fatalf("previous owner survived on %v", h.server.hostsOf(mdbOwnerUser))
	}
	if mdb.Status.OwnerUser != next || !reflect.DeepEqual(mdb.Status.OwnerHosts, []string{hostB}) ||
		mdb.Status.PendingOwnerUser != "" || len(mdb.Status.PendingOwnerHosts) != 0 {
		t.Fatalf("owner record = %q/%v pending %q/%v", mdb.Status.OwnerUser, mdb.Status.OwnerHosts,
			mdb.Status.PendingOwnerUser, mdb.Status.PendingOwnerHosts)
	}
}

// --- hole 4: cleanup respects every current claim ----------------------------

// staleOwnerTargetCR is a CR whose owner rotation acme_app → acme_app_v2
// failed after creating acme_app_v2, with the owner Secret since moved on to
// acme_app_v3.
func staleOwnerTargetCR(mutate ...func(*v1alpha1.MysqlDatabase)) *v1alpha1.MysqlDatabase {
	return mdbCR(append([]func(*v1alpha1.MysqlDatabase){func(m *v1alpha1.MysqlDatabase) {
		m.Status.DatabaseCreated = true
		m.Status.OwnerUser, m.Status.OwnerHosts = mdbOwnerUser, []string{"%"}
		m.Status.PendingOwnerUser, m.Status.PendingOwnerHosts = "acme_app_v2", []string{"%"}
	}}, mutate...)...)
}

func staleOwnerTargetHarness(t *testing.T, cr *v1alpha1.MysqlDatabase, extra ...*corev1.Secret) *mdbHarness {
	t.Helper()
	owner := mdbOwnerSecret()
	owner.Data["username"] = []byte("acme_app_v3")
	objs := []client.Object{cr, mdbGroup("dc1"), mdbOperatorSecret(), owner}
	for _, s := range extra {
		objs = append(objs, s)
	}
	h := newMdbHarness(t, objs...)
	h.server.addUser(mdbOwnerUser, mdbOwnerPass)
	h.server.addUser("acme_app_v2", "rotated-pw")
	return h
}

func TestMysqlDatabaseStaleOwnerTargetStillClaimed(t *testing.T) {
	t.Run("by this CR's users[]", func(t *testing.T) {
		cr := staleOwnerTargetCR(func(m *v1alpha1.MysqlDatabase) {
			m.Spec.Users = []v1alpha1.MysqlDatabaseUser{selectUserEntry("moved-mysql")}
		})
		h := staleOwnerTargetHarness(t, cr, userSecretFor("moved-mysql", "acme_app_v2", "moved-pw"))

		h.reconcile()
		mdb := h.requireReady() // not Failed/PreExistingUser: the account is this CR's
		h.requireNoStatementNaming(0, "DROP USER", "acme_app_v2")
		h.requirePasswordOf("acme_app_v2", "%", "moved-pw")
		if len(mdb.Status.AppliedUsers) != 1 || mdb.Status.AppliedUsers[0].Username != "acme_app_v2" {
			t.Fatalf("status.appliedUsers = %+v, want the moved account recorded", mdb.Status.AppliedUsers)
		}
		if h.server.hasUser(mdbOwnerUser) {
			t.Fatal("previous owner survived the rotation")
		}
	})

	t.Run("by this CR's users[] on different hosts", func(t *testing.T) {
		cr := staleOwnerTargetCR(func(m *v1alpha1.MysqlDatabase) {
			m.Status.PendingOwnerHosts = []string{hostB}
			m.Spec.Users = []v1alpha1.MysqlDatabaseUser{selectUserEntry("moved-mysql", hostC)}
		})
		h := staleOwnerTargetHarness(t, cr, userSecretFor("moved-mysql", "acme_app_v2", "moved-pw"))
		h.server.removeUser("acme_app_v2")
		h.server.addAccount("acme_app_v2", hostB, "rotated-pw")

		h.reconcile()
		mdb := h.requireReady()
		// The account on the host the new surface does not declare is this
		// CR's and obsolete: dropped, not leaked off the record.
		if h.server.hasAccount("acme_app_v2", hostB) {
			t.Fatalf("acme_app_v2@%s leaked", hostB)
		}
		h.requirePasswordOf("acme_app_v2", hostC, "moved-pw")
		if len(mdb.Status.AppliedUsers) != 1 || !reflect.DeepEqual(mdb.Status.AppliedUsers[0].Hosts, []string{hostC}) {
			t.Fatalf("status.appliedUsers = %+v", mdb.Status.AppliedUsers)
		}
		if !h.sawEvent("UserHostsRemoved") {
			t.Fatal("no UserHostsRemoved event")
		}
	})

	t.Run("by this CR's grants[]", func(t *testing.T) {
		cr := staleOwnerTargetCR(func(m *v1alpha1.MysqlDatabase) {
			m.Spec.Grants = []v1alpha1.MysqlDatabaseGrant{{Username: "acme_app_v2", Privileges: []v1alpha1.MysqlPrivilege{v1alpha1.PrivilegeSelect}}}
		})
		h := staleOwnerTargetHarness(t, cr)

		h.reconcile()
		mdb := h.requireReady()
		h.requireNoStatementNaming(0, "DROP USER", "acme_app_v2")
		h.requirePasswordOf("acme_app_v2", "%", "rotated-pw")
		if privs, _ := h.server.grantsFor(mdbDatabase, "acme_app_v2"); strings.Join(privs, ",") != "SELECT" {
			t.Fatalf("acme_app_v2 grants = %v, want [SELECT]", privs)
		}
		if mdb.Status.PendingOwnerUser != "" {
			t.Fatalf("status.pendingOwnerUser = %q, want forgotten", mdb.Status.PendingOwnerUser)
		}
	})

	for _, tc := range []struct {
		name    string
		sibling func(*v1alpha1.MysqlDatabase)
	}{
		{name: "by a sibling's owner record", sibling: func(m *v1alpha1.MysqlDatabase) { m.Status.OwnerUser = "acme_app_v2" }},
		{name: "by a sibling's users ledger", sibling: func(m *v1alpha1.MysqlDatabase) {
			m.Status.AppliedUsers = []v1alpha1.MysqlDatabaseUserState{{SecretName: "sibling-support", Username: "acme_app_v2"}}
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			sibling := mdbCR(func(m *v1alpha1.MysqlDatabase) {
				m.Name = "tenant-sibling"
				m.Spec.DatabaseName = "sibling_wms"
				m.Spec.Owner.SecretName = "sibling-owner"
			}, tc.sibling)
			owner := mdbOwnerSecret()
			owner.Data["username"] = []byte("acme_app_v3")
			h := newMdbHarness(t, staleOwnerTargetCR(), sibling, mdbGroup("dc1"), mdbOperatorSecret(), owner)
			h.server.addUser(mdbOwnerUser, mdbOwnerPass)
			h.server.addUser("acme_app_v2", "rotated-pw")

			h.reconcile()
			h.requireReady()
			h.requireNoStatementNaming(0, "DROP USER", "acme_app_v2")
			h.requirePasswordOf("acme_app_v2", "%", "rotated-pw")
			if !h.sawEvent("OwnerUserDropSkipped") {
				t.Fatal("no OwnerUserDropSkipped event")
			}
		})
	}
}

// staleUsersTargetCR is a CR whose support entry's rotation acme_support →
// acme_app_v2 failed after creating acme_app_v2, with the support Secret
// since moved on to acme_support_v3.
func staleUsersTargetCR(mutate ...func(*v1alpha1.MysqlDatabase)) *v1alpha1.MysqlDatabase {
	return mdbCR(append([]func(*v1alpha1.MysqlDatabase){withSupportUser, func(m *v1alpha1.MysqlDatabase) {
		m.Status.DatabaseCreated = true
		m.Status.OwnerUser, m.Status.OwnerHosts = mdbOwnerUser, []string{"%"}
		m.Status.AppliedUsers = []v1alpha1.MysqlDatabaseUserState{{
			SecretName: mdbSupportSecretName,
			Username:   mdbSupportUser, Hosts: []string{"%"},
			PendingUsername: "acme_app_v2", PendingHosts: []string{"%"},
		}}
	}}, mutate...)...)
}

func TestMysqlDatabaseStaleUsersTargetStillClaimed(t *testing.T) {
	t.Run("by this CR's owner", func(t *testing.T) {
		owner := mdbOwnerSecret()
		owner.Data["username"] = []byte("acme_app_v2")
		owner.Data["password"] = []byte("owner-pw-2")
		support := mdbSupportSecret()
		support.Data["username"] = []byte("acme_support_v3")
		h := newMdbHarness(t, staleUsersTargetCR(), mdbGroup("dc1"), mdbOperatorSecret(), owner, support)
		h.server.addUser(mdbOwnerUser, mdbOwnerPass)
		h.server.addUser(mdbSupportUser, mdbSupportPass)
		h.server.addUser("acme_app_v2", "rotated-pw")

		h.reconcile()
		mdb := h.requireReady() // not Failed/PreExistingOwnerUser
		h.requireNoStatementNaming(0, "DROP USER", "acme_app_v2")
		h.requirePasswordOf("acme_app_v2", "%", "owner-pw-2")
		if mdb.Status.OwnerUser != "acme_app_v2" {
			t.Fatalf("status.ownerUser = %q, want acme_app_v2", mdb.Status.OwnerUser)
		}
		if h.server.hasUser(mdbOwnerUser) || h.server.hasUser(mdbSupportUser) {
			t.Fatal("rotated-away accounts survived")
		}
	})

	t.Run("by this CR's grants[]", func(t *testing.T) {
		support := mdbSupportSecret()
		support.Data["username"] = []byte("acme_support_v3")
		cr := staleUsersTargetCR(func(m *v1alpha1.MysqlDatabase) {
			m.Spec.Grants = []v1alpha1.MysqlDatabaseGrant{{Username: "acme_app_v2", Privileges: []v1alpha1.MysqlPrivilege{v1alpha1.PrivilegeSelect}}}
		})
		h := newMdbHarness(t, cr, mdbGroup("dc1"), mdbOperatorSecret(), mdbOwnerSecret(), support)
		h.server.addUser(mdbOwnerUser, mdbOwnerPass)
		h.server.addUser(mdbSupportUser, mdbSupportPass)
		h.server.addUser("acme_app_v2", "rotated-pw")

		h.reconcile()
		h.requireReady()
		h.requireNoStatementNaming(0, "DROP USER", "acme_app_v2")
		h.requirePasswordOf("acme_app_v2", "%", "rotated-pw")
		if privs, _ := h.server.grantsFor(mdbDatabase, "acme_app_v2"); strings.Join(privs, ",") != "SELECT" {
			t.Fatalf("acme_app_v2 grants = %v, want [SELECT]", privs)
		}
	})
}

// TestMysqlDatabaseRemovedUserMovedToGrantsKept: an entry removed from
// users[] whose username the same edit declares in grants[] is current
// desired state: neither revoked nor dropped.
func TestMysqlDatabaseRemovedUserMovedToGrantsKept(t *testing.T) {
	h := newMdbHarness(t, mdbCR(withSupportUser), mdbGroup("dc1"), mdbOperatorSecret(), mdbOwnerSecret(), mdbSupportSecret())
	h.reconcile()
	h.requireReady()

	h.update(func(m *v1alpha1.MysqlDatabase) {
		m.Spec.Users = nil
		m.Spec.Grants = []v1alpha1.MysqlDatabaseGrant{{Username: mdbSupportUser, Privileges: []v1alpha1.MysqlPrivilege{v1alpha1.PrivilegeSelect}}}
	})
	before := h.server.statementCount()
	h.reconcile()
	mdb := h.requireReady()

	h.requireNoStatementNaming(before, "DROP USER", mdbSupportUser)
	h.requireNoStatementNaming(before, "REVOKE IF EXISTS ALL PRIVILEGES", mdbSupportUser)
	h.requirePasswordOf(mdbSupportUser, "%", mdbSupportPass)
	if privs, ok := h.server.grantsFor(mdbDatabase, mdbSupportUser); !ok || strings.Join(privs, ",") != "SELECT" {
		t.Fatalf("grants = %v (present=%v), want [SELECT]", privs, ok)
	}
	if len(mdb.Status.AppliedUsers) != 0 {
		t.Fatalf("status.appliedUsers = %+v, want the record retired", mdb.Status.AppliedUsers)
	}
	if !h.sawEvent("UserTransferred") {
		t.Fatal("no UserTransferred event")
	}
}

// --- review round 1 ----------------------------------------------------------

// TestMysqlDatabaseLegacyPendingHostsSurviveSharedHostEdit: a legacy owner
// rotation target (pendingOwnerUser set, pendingOwnerHosts empty) borrows
// ownerHosts. A write-ahead that grows ownerHosts must pin the target's hosts
// first, or the target silently inherits the new host: here a later cleanup
// dropped a foreign B@H2 the CR never created.
func TestMysqlDatabaseLegacyPendingHostsSurviveSharedHostEdit(t *testing.T) {
	const moved = "acme_moved"
	cr := mdbCR(withOwnerHosts(hostA, hostB), func(m *v1alpha1.MysqlDatabase) {
		m.Spec.Users = []v1alpha1.MysqlDatabaseUser{selectUserEntry("moved-mysql", hostA)}
		m.Spec.Grants = []v1alpha1.MysqlDatabaseGrant{{Username: "ghost", Privileges: []v1alpha1.MysqlPrivilege{v1alpha1.PrivilegeSelect}}}
		m.Status.DatabaseCreated = true
		m.Status.Phase = v1alpha1.MysqlDatabasePhaseCreating
		m.Status.OwnerUser, m.Status.OwnerHosts = mdbOwnerUser, []string{hostA}
		m.Status.PendingOwnerUser = moved // legacy: no pendingOwnerHosts
	})
	h := newMdbHarness(t, cr, mdbGroup("dc1"), mdbOperatorSecret(), mdbOwnerSecret(), userSecretFor("moved-mysql", moved, "moved-pw"))
	h.server.addAccount(mdbOwnerUser, hostA, mdbOwnerPass)
	h.server.addAccount(moved, hostA, "rotated-pw")
	h.server.addAccount(moved, hostB, "foreign-password")

	// The apply runs the owner and users[] SQL, then fails on the missing
	// grants[] user: the write-ahead stamp stands.
	h.reconcile()
	mdb := h.get()
	requireFailed(t, mdb, "GrantUserMissing")
	if mdb.Status.PendingOwnerUser == moved && slices.Contains(pendingHostsOf(&mdb.Status), hostB) {
		t.Fatalf("legacy pending target now attributed on %s: ownerHosts=%v pendingOwnerHosts=%v",
			hostB, mdb.Status.OwnerHosts, mdb.Status.PendingOwnerHosts)
	}

	h.server.addUser("ghost", "ghost-pw")
	h.reconcile()
	h.requireReady()
	h.requirePasswordOf(moved, hostB, "foreign-password")
	h.requirePasswordOf(moved, hostA, "moved-pw")
}

// pendingHostsOf mirrors the reconciler's legacy fallback for the owner
// rotation target's hosts.
func pendingHostsOf(st *v1alpha1.MysqlDatabaseStatus) []string {
	if len(st.PendingOwnerHosts) > 0 {
		return st.PendingOwnerHosts
	}
	return st.OwnerHosts
}

// TestMysqlDatabaseGrantMovedToUsersOnOtherHostRevokesPercentAccount:
// grants[] manages G@'%'; a users[] entry naming G on another host manages
// a different account. Moving the name between them must still revoke the
// removed grant on G@'%'.
func TestMysqlDatabaseGrantMovedToUsersOnOtherHostRevokesPercentAccount(t *testing.T) {
	const shared = "maester"
	cr := mdbCR(func(m *v1alpha1.MysqlDatabase) {
		m.Spec.Grants = []v1alpha1.MysqlDatabaseGrant{{Username: shared, Privileges: []v1alpha1.MysqlPrivilege{v1alpha1.PrivilegeSelect}}}
	})
	h := newMdbHarness(t, cr, mdbGroup("dc1"), mdbOperatorSecret(), mdbOwnerSecret(), userSecretFor("maester-mysql", shared, "maester-tenant-pw"))
	h.server.addUser(shared, "maester-pw")
	h.reconcile()
	h.requireReady()
	if _, ok := h.server.grantsForAccount(mdbDatabase, shared, "%"); !ok {
		t.Fatal("test premise broken: maester@% not granted")
	}

	h.update(func(m *v1alpha1.MysqlDatabase) {
		m.Spec.Grants = nil
		m.Spec.Users = []v1alpha1.MysqlDatabaseUser{selectUserEntry("maester-mysql", hostA)}
	})
	h.reconcile()
	h.requireReady()

	if privs, ok := h.server.grantsForAccount(mdbDatabase, shared, "%"); ok {
		t.Fatalf("maester@%% still holds %v after its grants[] entry was removed", privs)
	}
	h.requirePasswordOf(shared, "%", "maester-pw")
	h.requirePasswordOf(shared, hostA, "maester-tenant-pw")
}

// TestMysqlDatabaseWithdrawalPatchFailureDoesNotAdopt: the Pending patch that
// carries the withdrawal fails, so the speculative records stay persisted
// and the reconcile errors (requeue). The retry re-verifies before trusting
// them — the accounts are still absent, the server still refuses, and this
// time the withdrawal lands — so a foreign account created afterwards is
// refused, not adopted, and never dropped.
func TestMysqlDatabaseWithdrawalPatchFailureDoesNotAdopt(t *testing.T) {
	var failNextPending atomic.Bool
	failNextPending.Store(true)
	funcs := interceptor.Funcs{
		SubResourcePatch: func(ctx context.Context, c client.Client, sub string, obj client.Object, patch client.Patch, opts ...client.SubResourcePatchOption) error {
			if mdb, ok := obj.(*v1alpha1.MysqlDatabase); ok && mdb.Status.Phase == v1alpha1.MysqlDatabasePhasePending && failNextPending.CompareAndSwap(true, false) {
				return errors.New("injected status patch failure")
			}
			return c.SubResource(sub).Patch(ctx, obj, patch, opts...)
		},
	}
	cr := mdbCR(withSupportUser, func(m *v1alpha1.MysqlDatabase) { m.Spec.DeletionPolicy = v1alpha1.MysqlDatabaseDelete })
	h := newMdbHarnessWithInterceptor(t, funcs, cr, mdbGroup("dc1"), mdbOperatorSecret(), mdbOwnerSecret(), mdbSupportSecret())
	h.server.failStatements("CREATE USER IF NOT EXISTS '"+mdbOwnerUser+"'",
		&mysqldriver.MySQLError{Number: 1290, Message: "The MySQL server is running with the --super-read-only option"}, false)
	req := ctrl.Request{NamespacedName: types.NamespacedName{Namespace: mdbNamespace, Name: mdbName}}

	if _, err := h.r.Reconcile(context.Background(), req); err == nil {
		t.Fatal("Reconcile() = nil error, want the failed withdrawal patch surfaced for requeue")
	}
	if mdb := h.get(); len(mdb.Status.AppliedUsers) == 0 {
		t.Fatalf("test premise broken: the write-ahead records are not persisted (%+v)", mdb.Status)
	}

	h.reconcile()
	mdb := h.get()
	if mdb.Status.Phase != v1alpha1.MysqlDatabasePhasePending {
		t.Fatalf("phase = %q (message %q), want Pending", mdb.Status.Phase, mdb.Status.Message)
	}
	if mdb.Status.OwnerUser != "" || len(mdb.Status.AppliedUsers) != 0 {
		t.Fatalf("records survived the retried withdrawal: owner=%q users=%+v", mdb.Status.OwnerUser, mdb.Status.AppliedUsers)
	}

	h.server.clearFaults()
	h.server.addUser(mdbSupportUser, "foreign-password")
	h.reconcile()
	requireFailed(t, h.get(), "PreExistingUser")
	h.requirePasswordOf(mdbSupportUser, "%", "foreign-password")

	h.delete()
	h.reconcile()
	h.requirePasswordOf(mdbSupportUser, "%", "foreign-password")
}

// TestMysqlDatabaseOwnerUsersSwapConverges: the owner and a users[] entry
// swap usernames in one edit. Both accounts are this CR's and both stay
// desired, so nothing is dropped and each password follows its new Secret.
func TestMysqlDatabaseOwnerUsersSwapConverges(t *testing.T) {
	h := newMdbHarness(t, mdbCR(withSupportUser), mdbGroup("dc1"), mdbOperatorSecret(), mdbOwnerSecret(), mdbSupportSecret())
	h.reconcile()
	h.requireReady()

	h.setOwnerUsername(mdbSupportUser, "owner-pw-swapped")
	h.updateSupportSecret(func(s *corev1.Secret) {
		s.Data["username"] = []byte(mdbOwnerUser)
		s.Data["password"] = []byte("support-pw-swapped")
	})
	before := h.server.statementCount()
	h.reconcile()
	mdb := h.requireReady()

	h.requireNoStatementNaming(before, "DROP USER", mdbOwnerUser)
	h.requireNoStatementNaming(before, "DROP USER", mdbSupportUser)
	h.requirePasswordOf(mdbSupportUser, "%", "owner-pw-swapped")
	h.requirePasswordOf(mdbOwnerUser, "%", "support-pw-swapped")
	if privs, _ := h.server.grantsFor(mdbDatabase, mdbSupportUser); strings.Join(privs, ",") != "ALL PRIVILEGES" {
		t.Fatalf("new owner grants = %v, want [ALL PRIVILEGES]", privs)
	}
	if privs, _ := h.server.grantsFor(mdbDatabase, mdbOwnerUser); strings.Join(privs, ",") != "SELECT" {
		t.Fatalf("new users[] principal grants = %v, want [SELECT]", privs)
	}
	if mdb.Status.OwnerUser != mdbSupportUser || mdb.Status.PendingOwnerUser != "" ||
		len(mdb.Status.AppliedUsers) != 1 || mdb.Status.AppliedUsers[0].Username != mdbOwnerUser || mdb.Status.AppliedUsers[0].PendingUsername != "" {
		t.Fatalf("records = owner %q pending %q users %+v", mdb.Status.OwnerUser, mdb.Status.PendingOwnerUser, mdb.Status.AppliedUsers)
	}
}

// siblingClaiming creates a live sibling CR on the group whose status claims
// username through its users[] ledger.
func (h *mdbHarness) siblingClaiming(username string) {
	h.t.Helper()
	sibling := mdbCR(func(m *v1alpha1.MysqlDatabase) {
		m.Name = "tenant-sibling"
		m.Spec.DatabaseName = "sibling_wms"
		m.Spec.Owner.SecretName = "sibling-owner"
	})
	if err := h.client.Create(context.Background(), sibling); err != nil {
		h.t.Fatalf("create sibling: %v", err)
	}
	sibling.Status.AppliedUsers = []v1alpha1.MysqlDatabaseUserState{{SecretName: "sibling-support", Username: username}}
	if err := h.client.Status().Update(context.Background(), sibling); err != nil {
		h.t.Fatalf("stamp sibling claim: %v", err)
	}
}

// TestMysqlDatabaseVetoedRotationDropStillRevokes: a rotated-away users[]
// name whose drop a sibling vetoes survives as an account but loses its
// rights on this CR's database.
func TestMysqlDatabaseVetoedRotationDropStillRevokes(t *testing.T) {
	h := newMdbHarness(t, mdbCR(withSupportUser), mdbGroup("dc1"), mdbOperatorSecret(), mdbOwnerSecret(), mdbSupportSecret())
	h.reconcile()
	h.requireReady()

	h.siblingClaiming(mdbSupportUser)
	h.updateSupportSecret(func(s *corev1.Secret) { s.Data["username"] = []byte("acme_support_v2") })
	h.reconcile()
	h.requireReady()

	h.requirePasswordOf(mdbSupportUser, "%", mdbSupportPass)
	if privs, ok := h.server.grantsFor(mdbDatabase, mdbSupportUser); ok {
		t.Fatalf("vetoed rotated-away principal still holds %v on this database", privs)
	}
	if !h.sawEvent("UserDropSkipped") {
		t.Fatal("no UserDropSkipped event")
	}
}

// TestMysqlDatabaseUndeclaredHostDropIsVetted: removing a host of the owner
// does not drop the same-named account on that host while a sibling claims
// the name; it only loses its rights on this database.
func TestMysqlDatabaseUndeclaredHostDropIsVetted(t *testing.T) {
	h := newMdbHarness(t, mdbCR(withOwnerHosts(hostA, hostB)), mdbGroup("dc1"), mdbOperatorSecret(), mdbOwnerSecret())
	h.reconcile()
	h.requireReady()

	h.siblingClaiming(mdbOwnerUser)
	h.update(func(m *v1alpha1.MysqlDatabase) { m.Spec.Owner.Hosts = []string{hostA} })
	h.reconcile()
	mdb := h.requireReady()

	h.requirePasswordOf(mdbOwnerUser, hostB, mdbOwnerPass)
	if privs, ok := h.server.grantsForAccount(mdbDatabase, mdbOwnerUser, hostB); ok {
		t.Fatalf("%s@%s still holds %v on this database", mdbOwnerUser, hostB, privs)
	}
	if !h.sawEvent("OwnerUserDropSkipped") {
		t.Fatal("no OwnerUserDropSkipped event")
	}
	if !reflect.DeepEqual(mdb.Status.OwnerHosts, []string{hostA}) {
		t.Fatalf("status.ownerHosts = %v", mdb.Status.OwnerHosts)
	}
}

// TestMysqlDatabaseFailedApplyWithdrawsInThePhasePatch: the withdrawal rides
// the Pending/Failed status patch — a failed apply costs the write-ahead
// stamp plus one patch, not an extra write per requeue.
func TestMysqlDatabaseFailedApplyWithdrawsInThePhasePatch(t *testing.T) {
	var patches atomic.Int32
	funcs := interceptor.Funcs{
		SubResourcePatch: func(ctx context.Context, c client.Client, sub string, obj client.Object, patch client.Patch, opts ...client.SubResourcePatchOption) error {
			patches.Add(1)
			return c.SubResource(sub).Patch(ctx, obj, patch, opts...)
		},
	}
	h := newMdbHarnessWithInterceptor(t, funcs, mdbCR(withSupportUser), mdbGroup("dc1"), mdbOperatorSecret(), mdbOwnerSecret(), mdbSupportSecret())
	h.server.failStatements("CREATE USER IF NOT EXISTS '"+mdbOwnerUser+"'",
		&mysqldriver.MySQLError{Number: 1290, Message: "The MySQL server is running with the --super-read-only option"}, false)

	h.reconcile()
	mdb := h.get()
	if mdb.Status.Phase != v1alpha1.MysqlDatabasePhasePending || mdb.Status.OwnerUser != "" || len(mdb.Status.AppliedUsers) != 0 {
		t.Fatalf("phase %q owner %q users %+v, want Pending with records withdrawn", mdb.Status.Phase, mdb.Status.OwnerUser, mdb.Status.AppliedUsers)
	}
	if n := patches.Load(); n != 2 {
		t.Fatalf("status patches = %d, want 2 (write-ahead stamp, then Pending with the withdrawal)", n)
	}
}
