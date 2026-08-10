package component

import (
	"context"
	"sort"
	"strings"
	"testing"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	"k8s.io/client-go/tools/record"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	v1alpha1 "github.com/shipstream/bloodraven/api/v1alpha1"
	"github.com/shipstream/bloodraven/internal/controller"
)

// These tests drive the real MysqlDatabase reconciler against the in-memory
// MySQL model in mysqldatabase_fakesql_test.go. The only substitution is the
// dialer; hashing, validation, statement rendering and ordering are
// production code.

const (
	mdbNamespace       = "bloodraven"
	mdbName            = "tenant-acme"
	mdbGroupName       = "main"
	mdbDatabase        = "acme_wms"
	mdbOwnerUser       = "acme_app"
	mdbOwnerPass       = "owner-pw-1"
	mdbOwnerSecretName = "acme-mysql-owner"
)

type mdbHarness struct {
	t      *testing.T
	client client.Client
	server *fakeSQLServer
	rec    *record.FakeRecorder
	r      *controller.MysqlDatabaseReconciler
}

func newMdbHarness(t *testing.T, objs ...client.Object) *mdbHarness {
	t.Helper()

	scheme := runtime.NewScheme()
	if err := clientgoscheme.AddToScheme(scheme); err != nil {
		t.Fatalf("add client-go scheme: %v", err)
	}
	if err := v1alpha1.AddToScheme(scheme); err != nil {
		t.Fatalf("add bloodraven scheme: %v", err)
	}

	server := newFakeSQLServer()
	// The operator credential must already exist for the admin connection
	// to authenticate — Bloodraven holds it; the caller never does.
	server.addUser("operator", "op-pw")

	c := fake.NewClientBuilder().
		WithScheme(scheme).
		WithStatusSubresource(&v1alpha1.MysqlDatabase{}, &v1alpha1.MysqlFailoverGroup{}).
		WithObjects(objs...).
		Build()

	rec := record.NewFakeRecorder(50)
	return &mdbHarness{
		t:      t,
		client: c,
		server: server,
		rec:    rec,
		r: &controller.MysqlDatabaseReconciler{
			Client:   c,
			Scheme:   scheme,
			Recorder: rec,
			OpenDB:   server.dialer(),
		},
	}
}

func (h *mdbHarness) reconcile() ctrl.Result {
	h.t.Helper()
	res, err := h.r.Reconcile(context.Background(), ctrl.Request{
		NamespacedName: types.NamespacedName{Namespace: mdbNamespace, Name: mdbName},
	})
	if err != nil {
		h.t.Fatalf("Reconcile() error = %v", err)
	}
	return res
}

func (h *mdbHarness) get() *v1alpha1.MysqlDatabase {
	h.t.Helper()
	var out v1alpha1.MysqlDatabase
	if err := h.client.Get(context.Background(), types.NamespacedName{Namespace: mdbNamespace, Name: mdbName}, &out); err != nil {
		h.t.Fatalf("get mysqldatabase: %v", err)
	}
	return &out
}

func (h *mdbHarness) update(mutate func(*v1alpha1.MysqlDatabase)) {
	h.t.Helper()
	mdb := h.get()
	mutate(mdb)
	// The fake client does not bump generation on spec writes, so do it
	// here to model what the API server would do.
	mdb.Generation++
	if err := h.client.Update(context.Background(), mdb); err != nil {
		h.t.Fatalf("update mysqldatabase: %v", err)
	}
}

// delete issues a real Delete. The CR carries a finalizer, so the fake
// client stamps deletionTimestamp and keeps the object around — exactly what
// a real API server does, and what the finalizer path must handle.
func (h *mdbHarness) delete() {
	h.t.Helper()
	if err := h.client.Delete(context.Background(), h.get()); err != nil {
		h.t.Fatalf("delete mysqldatabase: %v", err)
	}
}

func (h *mdbHarness) setOwnerPassword(password string) {
	h.t.Helper()
	var s corev1.Secret
	key := types.NamespacedName{Namespace: mdbNamespace, Name: mdbOwnerSecretName}
	if err := h.client.Get(context.Background(), key, &s); err != nil {
		h.t.Fatalf("get owner secret: %v", err)
	}
	s.Data["password"] = []byte(password)
	if err := h.client.Update(context.Background(), &s); err != nil {
		h.t.Fatalf("update owner secret: %v", err)
	}
}

func (h *mdbHarness) requireReady() *v1alpha1.MysqlDatabase {
	h.t.Helper()
	mdb := h.get()
	if mdb.Status.Phase != v1alpha1.MysqlDatabasePhaseReady {
		h.t.Fatalf("phase = %q (message %q), want Ready", mdb.Status.Phase, mdb.Status.Message)
	}
	return mdb
}

func mdbGroup(activeSite string) *v1alpha1.MysqlFailoverGroup {
	fg := &v1alpha1.MysqlFailoverGroup{
		ObjectMeta: metav1.ObjectMeta{Name: mdbGroupName, Namespace: mdbNamespace},
		Spec: v1alpha1.MysqlFailoverGroupSpec{
			Credentials: &v1alpha1.CredentialsSpec{OperatorSecret: "mysql-operator"},
			Sites: []v1alpha1.SiteSpec{
				{Name: "dc1", Role: "primary-candidate"},
				{Name: "dc2", Role: "primary-candidate"},
			},
		},
	}
	fg.Status.ActiveSite = activeSite
	return fg
}

func mdbOperatorSecret() *corev1.Secret {
	return &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{Name: "mysql-operator", Namespace: mdbNamespace},
		Data: map[string][]byte{
			"username":            []byte("operator"),
			"password":            []byte("op-pw"),
			"MYSQL_ROOT_PASSWORD": []byte("root-pw"),
		},
	}
}

func mdbOwnerSecret() *corev1.Secret {
	return &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{Name: mdbOwnerSecretName, Namespace: mdbNamespace},
		Data: map[string][]byte{
			"username": []byte(mdbOwnerUser),
			"password": []byte(mdbOwnerPass),
		},
	}
}

func mdbCR(mutate ...func(*v1alpha1.MysqlDatabase)) *v1alpha1.MysqlDatabase {
	mdb := &v1alpha1.MysqlDatabase{
		ObjectMeta: metav1.ObjectMeta{
			Name:       mdbName,
			Namespace:  mdbNamespace,
			Generation: 1,
			Finalizers: []string{controller.MysqlDatabaseFinalizer},
		},
		Spec: v1alpha1.MysqlDatabaseSpec{
			GroupRef:     v1alpha1.LocalGroupRef{Name: mdbGroupName},
			DatabaseName: mdbDatabase,
			CharacterSet: "utf8mb4",
			Collation:    "utf8mb4_unicode_ci",
			Owner:        v1alpha1.MysqlDatabaseOwner{SecretName: mdbOwnerSecretName},
		},
	}
	for _, m := range mutate {
		m(mdb)
	}
	return mdb
}

// TestMysqlDatabaseCreateAndGrant is the happy path the ShipStream
// provisioner exercises: one database, one owner, one grant onto an existing
// shared principal.
func TestMysqlDatabaseCreateAndGrant(t *testing.T) {
	cr := mdbCR(func(m *v1alpha1.MysqlDatabase) {
		m.Spec.Grants = []v1alpha1.MysqlDatabaseGrant{
			{Username: "maester", Privileges: []v1alpha1.MysqlPrivilege{
				v1alpha1.PrivilegeSelect, v1alpha1.PrivilegeDelete,
			}},
		}
	})
	h := newMdbHarness(t, cr, mdbGroup("dc1"), mdbOperatorSecret(), mdbOwnerSecret())
	// maester is a group-level principal that already exists. spec.grants[]
	// grants onto it; it never creates it.
	h.server.addUser("maester", "maester-pw")

	h.reconcile()

	mdb := h.requireReady()
	if !mdb.Status.DatabaseCreated {
		t.Fatal("status.databaseCreated = false")
	}
	if mdb.Status.OwnerUser != mdbOwnerUser {
		t.Fatalf("status.ownerUser = %q, want %q", mdb.Status.OwnerUser, mdbOwnerUser)
	}
	if mdb.Status.ActiveSite != "dc1" {
		t.Fatalf("status.activeSite = %q, want dc1", mdb.Status.ActiveSite)
	}
	if got, want := strings.Join(mdb.Status.AppliedGrants, ","), mdbOwnerUser+",maester"; got != want {
		t.Fatalf("status.appliedGrants = %q, want %q", got, want)
	}
	if mdb.Status.LastAppliedHash == "" {
		t.Fatal("status.lastAppliedHash is empty")
	}
	if mdb.Status.ObservedGeneration != mdb.Generation {
		t.Fatalf("observedGeneration = %d, want %d", mdb.Status.ObservedGeneration, mdb.Generation)
	}

	// No credential material anywhere in status.
	if strings.Contains(mdb.Status.Message, mdbOwnerPass) || strings.Contains(mdb.Status.LastAppliedHash, mdbOwnerPass) {
		t.Fatal("status leaked the owner password")
	}

	// MySQL side.
	if spec, ok := h.server.database(mdbDatabase); !ok || spec != "utf8mb4/utf8mb4_unicode_ci" {
		t.Fatalf("database %q = %q (present=%v), want utf8mb4/utf8mb4_unicode_ci", mdbDatabase, spec, ok)
	}
	if pw, ok := h.server.password(mdbOwnerUser); !ok || pw != mdbOwnerPass {
		t.Fatalf("owner user password = %q (present=%v), want %q", pw, ok, mdbOwnerPass)
	}
	if privs, ok := h.server.grantsFor(mdbDatabase, mdbOwnerUser); !ok || strings.Join(privs, ",") != "ALL PRIVILEGES" {
		t.Fatalf("owner grants = %v (present=%v), want [ALL PRIVILEGES]", privs, ok)
	}
	if privs, ok := h.server.grantsFor(mdbDatabase, "maester"); !ok || strings.Join(privs, ",") != "SELECT,DELETE" {
		t.Fatalf("maester grants = %v (present=%v), want [SELECT DELETE]", privs, ok)
	}

	h.server.assertNoGrantOption(t)
}

// TestMysqlDatabaseUnchangedReapplyIssuesNoStatements is acceptance
// criterion 3, asserted against the statement log rather than inferred.
func TestMysqlDatabaseUnchangedReapplyIssuesNoStatements(t *testing.T) {
	h := newMdbHarness(t, mdbCR(), mdbGroup("dc1"), mdbOperatorSecret(), mdbOwnerSecret())

	h.reconcile()
	h.requireReady()
	after := h.server.statementCount()
	if after == 0 {
		t.Fatal("first reconcile issued no statements")
	}

	for i := 0; i < 3; i++ {
		h.reconcile()
	}

	if extra := h.server.statementsSince(after); len(extra) != 0 {
		t.Fatalf("re-applying an unchanged CR issued %d statements: %v", len(extra), extra)
	}
}

// TestMysqlDatabaseOwnerPasswordRotation is goal 4: rotation is a Secret
// write and nothing else. Proven by authenticating as the owner through the
// same driver — the new password works, the old one does not.
func TestMysqlDatabaseOwnerPasswordRotation(t *testing.T) {
	h := newMdbHarness(t, mdbCR(), mdbGroup("dc1"), mdbOperatorSecret(), mdbOwnerSecret())

	h.reconcile()
	h.requireReady()

	const rotated = "owner-pw-2"
	h.setOwnerPassword(rotated)

	// The CR itself is untouched: no spec edit, no annotation, nothing.
	h.reconcile()
	h.requireReady()

	if pw, ok := h.server.password(mdbOwnerUser); !ok || pw != rotated {
		t.Fatalf("owner password after rotation = %q (present=%v), want %q", pw, ok, rotated)
	}

	dial := h.server.dialer()
	if db, err := dial(mdbOwnerUser, rotated, "mysql-main-dc1-internal:3306", ""); err != nil {
		t.Fatalf("new owner password does not authenticate: %v", err)
	} else {
		db.Close()
	}
	if db, err := dial(mdbOwnerUser, mdbOwnerPass, "mysql-main-dc1-internal:3306", ""); err == nil {
		db.Close()
		t.Fatal("the old owner password still authenticates after rotation")
	}
}

// TestMysqlDatabaseAddGrantLater covers editing spec.grants[] on a live CR.
func TestMysqlDatabaseAddGrantLater(t *testing.T) {
	h := newMdbHarness(t, mdbCR(), mdbGroup("dc1"), mdbOperatorSecret(), mdbOwnerSecret())
	h.server.addUser("maester", "maester-pw")

	h.reconcile()
	h.requireReady()
	if _, ok := h.server.grantsFor(mdbDatabase, "maester"); ok {
		t.Fatal("maester was granted before it was requested")
	}

	h.update(func(m *v1alpha1.MysqlDatabase) {
		m.Spec.Grants = []v1alpha1.MysqlDatabaseGrant{
			{Username: "maester", Privileges: []v1alpha1.MysqlPrivilege{
				v1alpha1.PrivilegeSelect, v1alpha1.PrivilegeDelete,
			}},
		}
	})
	h.reconcile()
	h.requireReady()

	if privs, ok := h.server.grantsFor(mdbDatabase, "maester"); !ok || strings.Join(privs, ",") != "SELECT,DELETE" {
		t.Fatalf("maester grants = %v (present=%v), want [SELECT DELETE]", privs, ok)
	}
	h.server.assertNoGrantOption(t)
}

// TestMysqlDatabaseGrantUserMissingFailsAndCreatesNothing is the negative
// case that defines the CRD's blast radius: spec.grants[] must never be able
// to bring a MySQL principal into existence.
func TestMysqlDatabaseGrantUserMissingFailsAndCreatesNothing(t *testing.T) {
	cr := mdbCR(func(m *v1alpha1.MysqlDatabase) {
		m.Spec.Grants = []v1alpha1.MysqlDatabaseGrant{
			{Username: "ghost", Privileges: []v1alpha1.MysqlPrivilege{v1alpha1.PrivilegeSelect}},
		}
	})
	h := newMdbHarness(t, cr, mdbGroup("dc1"), mdbOperatorSecret(), mdbOwnerSecret())

	h.reconcile()

	mdb := h.get()
	if mdb.Status.Phase != v1alpha1.MysqlDatabasePhaseFailed {
		t.Fatalf("phase = %q, want Failed", mdb.Status.Phase)
	}
	var ready *metav1.Condition
	for i := range mdb.Status.Conditions {
		if mdb.Status.Conditions[i].Type == "Ready" {
			ready = &mdb.Status.Conditions[i]
		}
	}
	if ready == nil || ready.Status != metav1.ConditionFalse || ready.Reason != "GrantUserMissing" {
		t.Fatalf("Ready condition = %+v, want False/GrantUserMissing", ready)
	}

	// The whole point: no user was invented.
	if h.server.hasUser("ghost") {
		t.Fatal("spec.grants[] created the missing MySQL user; it must only ever grant")
	}
	if _, ok := h.server.grantsFor(mdbDatabase, "ghost"); ok {
		t.Fatal("a grant was applied for a nonexistent user")
	}

	// Nothing in the recorded statement log so much as mentions creating it.
	for _, stmt := range h.server.statementsSince(0) {
		if strings.Contains(stmt, "CREATE USER") && strings.Contains(stmt, "ghost") {
			t.Fatalf("reconciler emitted %q", stmt)
		}
	}

	// It self-heals once the principal really exists, without a CR edit.
	h.server.addUser("ghost", "ghost-pw")
	h.reconcile()
	h.requireReady()
	if privs, ok := h.server.grantsFor(mdbDatabase, "ghost"); !ok || strings.Join(privs, ",") != "SELECT" {
		t.Fatalf("ghost grants = %v (present=%v), want [SELECT]", privs, ok)
	}
}

// TestMysqlDatabaseDeleteRetainLeavesMySQLIntact is acceptance criterion 4.
func TestMysqlDatabaseDeleteRetainLeavesMySQLIntact(t *testing.T) {
	h := newMdbHarness(t, mdbCR(), mdbGroup("dc1"), mdbOperatorSecret(), mdbOwnerSecret())

	h.reconcile()
	h.requireReady()
	before := h.server.statementCount()

	h.delete()

	h.reconcile()

	if extra := h.server.statementsSince(before); len(extra) != 0 {
		t.Fatalf("Retain deletion issued %d MySQL statements: %v", len(extra), extra)
	}
	if _, ok := h.server.database(mdbDatabase); !ok {
		t.Fatal("Retain deletion dropped the database")
	}
	if !h.server.hasUser(mdbOwnerUser) {
		t.Fatal("Retain deletion dropped the owner user")
	}
}

// TestMysqlDatabaseDeletePolicyDropsDatabaseButNotSharedUsers covers the
// opt-in destructive path, including the rule that shared principals survive.
func TestMysqlDatabaseDeletePolicyDropsDatabaseButNotSharedUsers(t *testing.T) {
	cr := mdbCR(func(m *v1alpha1.MysqlDatabase) {
		m.Spec.DeletionPolicy = v1alpha1.MysqlDatabaseDelete
		m.Spec.Grants = []v1alpha1.MysqlDatabaseGrant{
			{Username: "maester", Privileges: []v1alpha1.MysqlPrivilege{v1alpha1.PrivilegeSelect}},
		}
	})
	h := newMdbHarness(t, cr, mdbGroup("dc1"), mdbOperatorSecret(), mdbOwnerSecret())
	h.server.addUser("maester", "maester-pw")

	h.reconcile()
	h.requireReady()

	h.delete()
	h.reconcile()

	if _, ok := h.server.database(mdbDatabase); ok {
		t.Fatal("deletionPolicy=Delete left the database in place")
	}
	if h.server.hasUser(mdbOwnerUser) {
		t.Fatal("deletionPolicy=Delete left the owner user in place")
	}
	if !h.server.hasUser("maester") {
		t.Fatal("deletionPolicy=Delete dropped a spec.grants[] user; those principals are shared")
	}
	if _, ok := h.server.grantsFor(mdbDatabase, "maester"); ok {
		t.Fatal("deletionPolicy=Delete left maester's grant on the dropped database")
	}
	// The grants map also empties when the database is dropped, so the
	// absence above cannot prove the REVOKE ran. The statement log can:
	// MySQL keeps schema-level grant rows across DROP DATABASE, so the
	// reconciler must have emitted an explicit revoke for maester.
	revoked := false
	for _, stmt := range h.server.statementsSince(0) {
		if strings.HasPrefix(stmt, "REVOKE") && strings.Contains(stmt, "'maester'") {
			revoked = true
		}
	}
	if !revoked {
		t.Fatal("no REVOKE for maester in the statement log; DROP DATABASE alone leaves the mysql.db grant row behind")
	}
}

// TestMysqlDatabaseReappliesAfterFailover is the reason the group watch
// exists: grants replicate, but a CR must not report Ready against a stale
// primary.
func TestMysqlDatabaseReappliesAfterFailover(t *testing.T) {
	h := newMdbHarness(t, mdbCR(), mdbGroup("dc1"), mdbOperatorSecret(), mdbOwnerSecret())

	h.reconcile()
	h.requireReady()
	if addr := h.server.lastDialedAddr(); !strings.Contains(addr, "dc1") {
		t.Fatalf("first apply dialed %q, want the dc1 primary", addr)
	}
	before := h.server.statementCount()

	// Promote dc2.
	var fg v1alpha1.MysqlFailoverGroup
	key := types.NamespacedName{Namespace: mdbNamespace, Name: mdbGroupName}
	if err := h.client.Get(context.Background(), key, &fg); err != nil {
		t.Fatalf("get failover group: %v", err)
	}
	fg.Status.ActiveSite = "dc2"
	if err := h.client.Status().Update(context.Background(), &fg); err != nil {
		t.Fatalf("update failover group status: %v", err)
	}

	h.reconcile()

	mdb := h.requireReady()
	if mdb.Status.ActiveSite != "dc2" {
		t.Fatalf("status.activeSite = %q, want dc2", mdb.Status.ActiveSite)
	}
	if addr := h.server.lastDialedAddr(); !strings.Contains(addr, "dc2") {
		t.Fatalf("re-apply dialed %q, want the dc2 primary", addr)
	}
	if extra := h.server.statementsSince(before); len(extra) == 0 {
		t.Fatal("failover did not trigger a re-apply against the new primary")
	}
}

// TestMysqlDatabasePendingDuringPlannedFailover proves the fencing back-off
// is Pending, not an error, and that it does not dial the fenced primary.
func TestMysqlDatabasePendingDuringPlannedFailover(t *testing.T) {
	fg := mdbGroup("dc1")
	fg.Status.PlannedFailover = &v1alpha1.PlannedFailoverStatus{
		Phase: v1alpha1.PlannedFailoverPhaseDraining,
	}
	h := newMdbHarness(t, mdbCR(), fg, mdbOperatorSecret(), mdbOwnerSecret())

	res := h.reconcile()
	if res.RequeueAfter == 0 {
		t.Fatal("expected a requeue while the primary is fenced")
	}

	mdb := h.get()
	if mdb.Status.Phase != v1alpha1.MysqlDatabasePhasePending {
		t.Fatalf("phase = %q, want Pending during a planned failover", mdb.Status.Phase)
	}
	if n := h.server.statementCount(); n != 0 {
		t.Fatalf("issued %d statements against a fenced primary", n)
	}
}

// TestMysqlDatabaseDeleteAfterPartialApplyCleansUp is the regression test for
// two related delete-path bugs found in review:
//
//  1. The REVOKE for a spec.grants[] user that never existed used to error
//     (MySQL 1141 — IF EXISTS does not cover a missing account), wedging the
//     finalizer forever. IGNORE UNKNOWN USER fixes it; the fake server
//     models the 1141 behavior, so this test fails without the clause.
//  2. status.ownerUser used to be stamped only on a successful apply, so a
//     partial apply (owner created, then GrantUserMissing) orphaned a live,
//     fully-privileged owner account on delete. It is now stamped before any
//     SQL runs.
func TestMysqlDatabaseDeleteAfterPartialApplyCleansUp(t *testing.T) {
	cr := mdbCR(func(m *v1alpha1.MysqlDatabase) {
		m.Spec.DeletionPolicy = v1alpha1.MysqlDatabaseDelete
		m.Spec.Grants = []v1alpha1.MysqlDatabaseGrant{
			{Username: "ghost", Privileges: []v1alpha1.MysqlPrivilege{v1alpha1.PrivilegeSelect}},
		}
	})
	h := newMdbHarness(t, cr, mdbGroup("dc1"), mdbOperatorSecret(), mdbOwnerSecret())

	h.reconcile()

	// Partial apply: database and owner exist, CR is Failed on the ghost.
	mdb := h.get()
	if mdb.Status.Phase != v1alpha1.MysqlDatabasePhaseFailed {
		t.Fatalf("phase = %q, want Failed", mdb.Status.Phase)
	}
	if !h.server.hasUser(mdbOwnerUser) {
		t.Fatal("test premise broken: owner user was not created before the grants[] check")
	}
	if mdb.Status.OwnerUser != mdbOwnerUser {
		t.Fatalf("status.ownerUser = %q after partial apply, want %q (the write-ahead record)",
			mdb.Status.OwnerUser, mdbOwnerUser)
	}

	h.delete()
	h.reconcile()

	// The finalizer released despite the ghost user never existing…
	var out v1alpha1.MysqlDatabase
	err := h.client.Get(context.Background(), types.NamespacedName{Namespace: mdbNamespace, Name: mdbName}, &out)
	if err == nil {
		t.Fatalf("MysqlDatabase still present (finalizers %v); deletion wedged", out.Finalizers)
	}
	// …and the partially-applied owner did not survive as an orphaned
	// privileged account.
	if h.server.hasUser(mdbOwnerUser) {
		t.Fatal("partial-apply owner user survived deletionPolicy=Delete; orphaned privileged account")
	}
	if _, ok := h.server.database(mdbDatabase); ok {
		t.Fatal("database survived deletionPolicy=Delete")
	}
}

// TestMysqlDatabaseOwnerUsernameRotationDropsOldUser: rotating the Secret's
// username must revoke the old credential, not leave a shadow account with
// the old password and full privileges — the stated contract is "rotation is
// a Secret write and nothing else", which has to include revocation.
func TestMysqlDatabaseOwnerUsernameRotationDropsOldUser(t *testing.T) {
	h := newMdbHarness(t, mdbCR(), mdbGroup("dc1"), mdbOperatorSecret(), mdbOwnerSecret())

	h.reconcile()
	h.requireReady()

	var s corev1.Secret
	key := types.NamespacedName{Namespace: mdbNamespace, Name: mdbOwnerSecretName}
	if err := h.client.Get(context.Background(), key, &s); err != nil {
		t.Fatalf("get owner secret: %v", err)
	}
	s.Data["username"] = []byte("acme_app_v2")
	s.Data["password"] = []byte("owner-pw-2")
	if err := h.client.Update(context.Background(), &s); err != nil {
		t.Fatalf("update owner secret: %v", err)
	}

	h.reconcile()
	mdb := h.requireReady()

	if mdb.Status.OwnerUser != "acme_app_v2" {
		t.Fatalf("status.ownerUser = %q, want acme_app_v2", mdb.Status.OwnerUser)
	}
	if h.server.hasUser(mdbOwnerUser) {
		t.Fatalf("old owner user %q still exists after username rotation; the leaked credential was not revoked", mdbOwnerUser)
	}
	if pw, ok := h.server.password("acme_app_v2"); !ok || pw != "owner-pw-2" {
		t.Fatalf("new owner password = %q (present=%v), want the rotated Secret value", pw, ok)
	}
	if privs, ok := h.server.grantsFor(mdbDatabase, "acme_app_v2"); !ok || len(privs) == 0 {
		t.Fatal("new owner user has no grants on the database")
	}
}

// TestMysqlDatabaseOwnerPrivilegeNarrowingRevokes: shrinking
// spec.owner.privileges must actually shrink MySQL's grant, not GRANT the
// narrower set on top of the wider one and report the narrow set as applied.
func TestMysqlDatabaseOwnerPrivilegeNarrowingRevokes(t *testing.T) {
	h := newMdbHarness(t, mdbCR(), mdbGroup("dc1"), mdbOperatorSecret(), mdbOwnerSecret())

	h.reconcile()
	h.requireReady()
	if privs, _ := h.server.grantsFor(mdbDatabase, mdbOwnerUser); strings.Join(privs, ",") != "ALL PRIVILEGES" {
		t.Fatalf("owner grants = %v, want default ALL PRIVILEGES", privs)
	}

	h.update(func(m *v1alpha1.MysqlDatabase) {
		m.Spec.Owner.Privileges = []v1alpha1.MysqlPrivilege{v1alpha1.PrivilegeSelect}
	})
	h.reconcile()
	h.requireReady()

	if privs, _ := h.server.grantsFor(mdbDatabase, mdbOwnerUser); strings.Join(privs, ",") != "SELECT" {
		t.Fatalf("owner grants = %v after narrowing to SELECT; the old wider grant survived", privs)
	}
}

// TestMysqlDatabaseGrantNarrowingRevokes: while a grants[] entry is listed,
// the CR is the authority on that user's privileges for this database, so
// narrowing the entry must actually narrow MySQL. The fake models GRANT as
// additive (like the real server), so this test cannot pass unless the
// reconciler revokes before it grants.
func TestMysqlDatabaseGrantNarrowingRevokes(t *testing.T) {
	cr := mdbCR(func(m *v1alpha1.MysqlDatabase) {
		m.Spec.Grants = []v1alpha1.MysqlDatabaseGrant{
			{Username: "maester", Privileges: []v1alpha1.MysqlPrivilege{
				v1alpha1.PrivilegeSelect, v1alpha1.PrivilegeInsert,
			}},
		}
	})
	h := newMdbHarness(t, cr, mdbGroup("dc1"), mdbOperatorSecret(), mdbOwnerSecret())
	h.server.addUser("maester", "maester-pw")

	h.reconcile()
	h.requireReady()
	privs, _ := h.server.grantsFor(mdbDatabase, "maester")
	sorted := append([]string(nil), privs...)
	sort.Strings(sorted)
	if strings.Join(sorted, ",") != "INSERT,SELECT" {
		t.Fatalf("maester grants = %v, want INSERT and SELECT", privs)
	}

	h.update(func(m *v1alpha1.MysqlDatabase) {
		m.Spec.Grants[0].Privileges = []v1alpha1.MysqlPrivilege{v1alpha1.PrivilegeSelect}
	})
	h.reconcile()
	h.requireReady()

	if privs, _ := h.server.grantsFor(mdbDatabase, "maester"); strings.Join(privs, ",") != "SELECT" {
		t.Fatalf("maester grants = %v after narrowing to SELECT; INSERT survived", privs)
	}
}

// requireFailed asserts a Failed phase with the given Ready reason.
func requireFailed(t *testing.T, mdb *v1alpha1.MysqlDatabase, reason string) {
	t.Helper()
	if mdb.Status.Phase != v1alpha1.MysqlDatabasePhaseFailed {
		t.Fatalf("phase = %q (message %q), want Failed/%s", mdb.Status.Phase, mdb.Status.Message, reason)
	}
	for i := range mdb.Status.Conditions {
		if mdb.Status.Conditions[i].Type == "Ready" {
			if mdb.Status.Conditions[i].Reason != reason {
				t.Fatalf("Ready reason = %q, want %s", mdb.Status.Conditions[i].Reason, reason)
			}
			return
		}
	}
	t.Fatalf("no Ready condition; want reason %s", reason)
}

// TestMysqlDatabaseRemovingGrantRevokesIt makes grants[] desired state on
// the way out, not just on the way in: removing an entry revokes it on the
// next apply, via the diff against status.appliedGrants.
func TestMysqlDatabaseRemovingGrantRevokesIt(t *testing.T) {
	cr := mdbCR(func(m *v1alpha1.MysqlDatabase) {
		m.Spec.Grants = []v1alpha1.MysqlDatabaseGrant{
			{Username: "maester", Privileges: []v1alpha1.MysqlPrivilege{
				v1alpha1.PrivilegeSelect, v1alpha1.PrivilegeDelete,
			}},
		}
	})
	h := newMdbHarness(t, cr, mdbGroup("dc1"), mdbOperatorSecret(), mdbOwnerSecret())
	h.server.addUser("maester", "maester-pw")

	h.reconcile()
	h.requireReady()
	if _, ok := h.server.grantsFor(mdbDatabase, "maester"); !ok {
		t.Fatal("test premise broken: maester not granted after first apply")
	}

	h.update(func(m *v1alpha1.MysqlDatabase) {
		m.Spec.Grants = nil
	})
	h.reconcile()
	h.requireReady()

	if privs, ok := h.server.grantsFor(mdbDatabase, "maester"); ok {
		t.Fatalf("maester still holds %v after its grants[] entry was removed; removal must revoke", privs)
	}
	// The shared principal itself is never dropped by a grants[] edit.
	if !h.server.hasUser("maester") {
		t.Fatal("removing a grants[] entry dropped the shared user")
	}
}

// TestMysqlDatabaseDeleteRevokesRemovedGrants: a grant removed from the
// spec immediately before deletion — no re-apply in between — must still be
// revoked by the delete path through the status.appliedGrants union; the
// mysql.db row otherwise survives DROP DATABASE and reactivates if the
// schema name is ever recreated.
func TestMysqlDatabaseDeleteRevokesRemovedGrants(t *testing.T) {
	cr := mdbCR(func(m *v1alpha1.MysqlDatabase) {
		m.Spec.DeletionPolicy = v1alpha1.MysqlDatabaseDelete
		m.Spec.Grants = []v1alpha1.MysqlDatabaseGrant{
			{Username: "maester", Privileges: []v1alpha1.MysqlPrivilege{v1alpha1.PrivilegeSelect}},
		}
	})
	h := newMdbHarness(t, cr, mdbGroup("dc1"), mdbOperatorSecret(), mdbOwnerSecret())
	h.server.addUser("maester", "maester-pw")

	h.reconcile()
	h.requireReady()

	// Remove the entry and delete WITHOUT re-applying: only the delete
	// path's union of spec.grants and status.appliedGrants can still reach
	// maester's grant.
	h.update(func(m *v1alpha1.MysqlDatabase) { m.Spec.Grants = nil })
	before := h.server.statementCount()
	h.delete()
	h.reconcile()

	if _, ok := h.server.database(mdbDatabase); ok {
		t.Fatal("database survived deletionPolicy=Delete")
	}
	for _, stmt := range h.server.statementsSince(before) {
		if strings.HasPrefix(stmt, "REVOKE") && strings.Contains(stmt, "'maester'") {
			return // the union revoke ran during deletion
		}
	}
	t.Fatal("delete path issued no REVOKE for maester (removed before deletion); the grant row would survive DROP DATABASE")
}

// TestMysqlDatabaseRefusesPreExistingSchema is the adoption gate: a schema
// this CR did not create is not granted onto, not owned, and never dropped.
func TestMysqlDatabaseRefusesPreExistingSchema(t *testing.T) {
	h := newMdbHarness(t, mdbCR(), mdbGroup("dc1"), mdbOperatorSecret(), mdbOwnerSecret())
	// Someone else's schema, same name.
	h.server.addDatabase(mdbDatabase, "latin1", "latin1_swedish_ci")

	h.reconcile()

	mdb := h.get()
	requireFailed(t, mdb, "DatabasePreExists")
	// The gate ran no SQL: no owner user, no grants, and the foreign
	// schema's defaults are untouched.
	if h.server.hasUser(mdbOwnerUser) {
		t.Fatal("owner user created despite refusing the schema")
	}
	if spec, _ := h.server.database(mdbDatabase); spec != "latin1/latin1_swedish_ci" {
		t.Fatalf("foreign schema modified: %q", spec)
	}
	if mdb.Status.DatabaseCreated {
		t.Fatal("status.databaseCreated set for a schema this CR never created; it would authorize a DROP")
	}

	// Deletion under Delete must release without touching the foreign
	// schema.
	h.update(func(m *v1alpha1.MysqlDatabase) { m.Spec.DeletionPolicy = v1alpha1.MysqlDatabaseDelete })
	h.delete()
	h.reconcile()
	if _, ok := h.server.database(mdbDatabase); !ok {
		t.Fatal("deletion dropped a schema the CR refused to adopt")
	}
}

// TestMysqlDatabaseRefusesPreExistingOwnerUser: CREATE USER IF NOT EXISTS +
// ALTER USER must never reset the password of an account this CR did not
// create, and deletion must never drop it.
func TestMysqlDatabaseRefusesPreExistingOwnerUser(t *testing.T) {
	h := newMdbHarness(t, mdbCR(), mdbGroup("dc1"), mdbOperatorSecret(), mdbOwnerSecret())
	// Someone else's account, same name as the owner the Secret declares.
	h.server.addUser(mdbOwnerUser, "foreign-password")

	h.reconcile()

	mdb := h.get()
	requireFailed(t, mdb, "PreExistingOwnerUser")
	if pw, _ := h.server.password(mdbOwnerUser); pw != "foreign-password" {
		t.Fatalf("foreign account password was reset to %q", pw)
	}
	if mdb.Status.OwnerUser != "" {
		t.Fatalf("status.ownerUser = %q, want empty — the account is not this CR's", mdb.Status.OwnerUser)
	}

	// Deletion under Delete drops the database this CR did create, but
	// never the foreign account.
	h.update(func(m *v1alpha1.MysqlDatabase) { m.Spec.DeletionPolicy = v1alpha1.MysqlDatabaseDelete })
	h.delete()
	h.reconcile()
	if !h.server.hasUser(mdbOwnerUser) {
		t.Fatal("deletion dropped a pre-existing account the CR refused to adopt")
	}
	if _, ok := h.server.database(mdbDatabase); ok {
		t.Fatal("database this CR created survived deletionPolicy=Delete")
	}
}

// TestMysqlDatabaseCharacterSetEditAppliesAlterDatabase: characterSet and
// collation are mutable desired state. CREATE DATABASE IF NOT EXISTS is a
// no-op on an existing schema, so the edit must land through ALTER DATABASE
// or the CR would report Ready while MySQL keeps the old defaults.
func TestMysqlDatabaseCharacterSetEditAppliesAlterDatabase(t *testing.T) {
	h := newMdbHarness(t, mdbCR(), mdbGroup("dc1"), mdbOperatorSecret(), mdbOwnerSecret())

	h.reconcile()
	h.requireReady()

	h.update(func(m *v1alpha1.MysqlDatabase) {
		m.Spec.CharacterSet = "utf8mb3"
		m.Spec.Collation = "utf8mb3_general_ci"
	})
	h.reconcile()
	h.requireReady()

	if spec, _ := h.server.database(mdbDatabase); spec != "utf8mb3/utf8mb3_general_ci" {
		t.Fatalf("schema defaults = %q after edit, want utf8mb3/utf8mb3_general_ci", spec)
	}
}

// TestMysqlDatabaseUsernameRotationCreatesBeforeDrop: rotation must be
// create-grant-then-drop, never drop-then-create — a failure mid-handover
// has to leave both accounts alive, not a tenant with no owner.
func TestMysqlDatabaseUsernameRotationCreatesBeforeDrop(t *testing.T) {
	h := newMdbHarness(t, mdbCR(), mdbGroup("dc1"), mdbOperatorSecret(), mdbOwnerSecret())

	h.reconcile()
	h.requireReady()

	var s corev1.Secret
	key := types.NamespacedName{Namespace: mdbNamespace, Name: mdbOwnerSecretName}
	if err := h.client.Get(context.Background(), key, &s); err != nil {
		t.Fatalf("get owner secret: %v", err)
	}
	s.Data["username"] = []byte("acme_app_v2")
	if err := h.client.Update(context.Background(), &s); err != nil {
		t.Fatalf("update owner secret: %v", err)
	}

	before := h.server.statementCount()
	h.reconcile()
	h.requireReady()

	createAt, dropAt := -1, -1
	for i, stmt := range h.server.statementsSince(before) {
		switch {
		case strings.Contains(stmt, "CREATE USER IF NOT EXISTS 'acme_app_v2'"):
			createAt = i
		case stmt == "DROP USER IF EXISTS 'acme_app'@'%'":
			dropAt = i
		}
	}
	if createAt == -1 || dropAt == -1 {
		t.Fatalf("rotation statements missing: create=%d drop=%d", createAt, dropAt)
	}
	if createAt > dropAt {
		t.Fatalf("rotation dropped the old owner (statement %d) before creating the new one (%d); a mid-rotation failure would leave the tenant ownerless", dropAt, createAt)
	}
}
