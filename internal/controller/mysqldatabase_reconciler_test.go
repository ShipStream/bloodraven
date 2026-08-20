package controller

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"fmt"
	"io"
	"net"
	"strings"
	"testing"
	"time"

	mysqldriver "github.com/go-sql-driver/mysql"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/tools/record"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
	"sigs.k8s.io/controller-runtime/pkg/event"

	v1alpha1 "github.com/shipstream/bloodraven/api/v1alpha1"
)

// mdbTestNamespace keeps the fixtures below in one namespace, matching the
// single-namespace scoping the CRD enforces.
const mdbTestNamespace = "bloodraven"

func mdbTestGroup() *v1alpha1.MysqlFailoverGroup {
	fg := &v1alpha1.MysqlFailoverGroup{
		ObjectMeta: metav1.ObjectMeta{Name: "main", Namespace: mdbTestNamespace},
		Spec: v1alpha1.MysqlFailoverGroupSpec{
			Credentials: &v1alpha1.CredentialsSpec{OperatorSecret: "mysql-operator"},
			Sites: []v1alpha1.SiteSpec{
				{Name: "dc1", Role: "primary-candidate"},
				{Name: "dc2", Role: "primary-candidate"},
			},
		},
	}
	fg.Status.ActiveSite = "dc1"
	return fg
}

func mdbTestCR(mutate ...func(*v1alpha1.MysqlDatabase)) *v1alpha1.MysqlDatabase {
	mdb := &v1alpha1.MysqlDatabase{
		ObjectMeta: metav1.ObjectMeta{
			Name:       "tenant-acme",
			Namespace:  mdbTestNamespace,
			Generation: 1,
			Finalizers: []string{MysqlDatabaseFinalizer},
		},
		Spec: v1alpha1.MysqlDatabaseSpec{
			GroupRef:     v1alpha1.LocalGroupRef{Name: "main"},
			DatabaseName: "acme_wms",
			Owner:        v1alpha1.MysqlDatabaseOwner{SecretName: "acme-mysql-owner"},
		},
	}
	for _, m := range mutate {
		m(mdb)
	}
	return mdb
}

// mdbTestOperatorSecret is the group's operator credential. The MysqlDatabase
// reconciler never surfaces it, but openAdminConnection reads it, so the
// fixtures need it present for any test that reaches the connection.
func mdbTestOperatorSecret() *corev1.Secret {
	return &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{Name: "mysql-operator", Namespace: mdbTestNamespace},
		Data: map[string][]byte{
			"username":            []byte("operator"),
			"password":            []byte("op-pw"),
			"MYSQL_ROOT_PASSWORD": []byte("root-pw"),
		},
	}
}

func mdbTestOwnerSecret() *corev1.Secret {
	return &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{Name: "acme-mysql-owner", Namespace: mdbTestNamespace},
		Data:       map[string][]byte{"username": []byte("acme_app"), "password": []byte("pw1")},
	}
}

// newMdbReconciler wires a reconciler whose dialer fails the test if it is
// called. Every case in this file is one that must reach a decision without
// touching MySQL; the dialer is the tripwire that proves it.
func newMdbReconciler(t *testing.T, objs ...client.Object) (*MysqlDatabaseReconciler, client.Client, *record.FakeRecorder) {
	t.Helper()
	scheme := testScheme()
	c := fake.NewClientBuilder().
		WithScheme(scheme).
		WithStatusSubresource(&v1alpha1.MysqlDatabase{}, &v1alpha1.MysqlFailoverGroup{}).
		WithIndex(&v1alpha1.MysqlDatabase{}, mdbSecretNamesIndex, mdbSecretNames).
		WithIndex(&v1alpha1.MysqlDatabase{}, mdbGroupRefIndex, func(o client.Object) []string {
			return []string{o.(*v1alpha1.MysqlDatabase).Spec.GroupRef.Name}
		}).
		WithObjects(objs...).
		Build()
	rec := record.NewFakeRecorder(20)
	return &MysqlDatabaseReconciler{
		Client:   c,
		Scheme:   scheme,
		Recorder: rec,
		OpenDB: func(user, password, addr, tlsConfigName string) (*sql.DB, error) {
			t.Fatalf("reconciler opened a MySQL connection (user=%q addr=%q) when it should not have", user, addr)
			return nil, nil
		},
	}, c, rec
}

func mdbRequest() ctrl.Request {
	return ctrl.Request{NamespacedName: types.NamespacedName{Namespace: mdbTestNamespace, Name: "tenant-acme"}}
}

func getMdb(t *testing.T, c client.Client) *v1alpha1.MysqlDatabase {
	t.Helper()
	var out v1alpha1.MysqlDatabase
	if err := c.Get(context.Background(), mdbRequest().NamespacedName, &out); err != nil {
		t.Fatalf("get mysqldatabase: %v", err)
	}
	return &out
}

func readyCondition(mdb *v1alpha1.MysqlDatabase) *metav1.Condition {
	for i := range mdb.Status.Conditions {
		if mdb.Status.Conditions[i].Type == ConditionDatabaseReady {
			return &mdb.Status.Conditions[i]
		}
	}
	return nil
}

// TestMysqlDatabasePendingPaths covers every dependency that is simply not
// ready yet. All of them are Pending, never Failed: a MysqlDatabase applied
// before its group, or during a maintenance window, is normal ordering.
func TestMysqlDatabasePendingPaths(t *testing.T) {
	cases := []struct {
		name       string
		objects    func() []client.Object
		wantReason string
	}{
		{
			name: "group absent",
			objects: func() []client.Object {
				return []client.Object{mdbTestCR(), mdbTestOwnerSecret()}
			},
			wantReason: "GroupNotFound",
		},
		{
			name: "group has no active site",
			objects: func() []client.Object {
				fg := mdbTestGroup()
				fg.Status.ActiveSite = ""
				return []client.Object{mdbTestCR(), fg, mdbTestOwnerSecret()}
			},
			wantReason: "NoActiveSite",
		},
		{
			name: "in-place restore fences the primary",
			objects: func() []client.Object {
				fg := mdbTestGroup()
				// Spec and status both set, as on a real cluster: the fence
				// delegates to inPlaceRestoreInFlight, which keys off both.
				fg.Spec.RestoreInPlace = &v1alpha1.RestoreInPlaceSpec{}
				fg.Status.RestoreInPlace = &v1alpha1.RestoreInPlaceStatus{Phase: v1alpha1.RestoreInPlaceRestoring}
				return []client.Object{mdbTestCR(), fg, mdbTestOwnerSecret()}
			},
			wantReason: "RestoreInProgress",
		},
		{
			name: "in-place restore requested but not yet observed",
			objects: func() []client.Object {
				fg := mdbTestGroup()
				fg.Spec.RestoreInPlace = &v1alpha1.RestoreInPlaceSpec{}
				return []client.Object{mdbTestCR(), fg, mdbTestOwnerSecret()}
			},
			wantReason: "RestoreInProgress",
		},
		{
			name: "group credential secret mid-rotation",
			objects: func() []client.Object {
				fg := mdbTestGroup()
				fg.Spec.Credentials.AppSecret = "mysql-app" // named but absent
				return []client.Object{mdbTestCR(), fg, mdbTestOwnerSecret(), mdbTestOperatorSecret()}
			},
			wantReason: "GroupSecretMissing",
		},
		{
			name: "planned failover fences the primary",
			objects: func() []client.Object {
				fg := mdbTestGroup()
				fg.Status.PlannedFailover = &v1alpha1.PlannedFailoverStatus{Phase: v1alpha1.PlannedFailoverPhaseDraining}
				return []client.Object{mdbTestCR(), fg, mdbTestOwnerSecret()}
			},
			wantReason: "PlannedFailoverInProgress",
		},
		{
			name: "owner secret not rendered yet",
			objects: func() []client.Object {
				return []client.Object{mdbTestCR(), mdbTestGroup()}
			},
			wantReason: "OwnerSecretMissing",
		},
		{
			name: "owner secret missing the password key",
			objects: func() []client.Object {
				s := mdbTestOwnerSecret()
				delete(s.Data, "password")
				return []client.Object{mdbTestCR(), mdbTestGroup(), s}
			},
			wantReason: "OwnerSecretIncomplete",
		},
		{
			name: "users secret not rendered yet",
			objects: func() []client.Object {
				cr := mdbTestCR(func(m *v1alpha1.MysqlDatabase) {
					m.Spec.Users = []v1alpha1.MysqlDatabaseUser{{
						SecretName: "support-ro-mysql",
						Privileges: []v1alpha1.MysqlPrivilege{v1alpha1.PrivilegeSelect},
					}}
				})
				return []client.Object{cr, mdbTestGroup(), mdbTestOwnerSecret()}
			},
			wantReason: "UserSecretMissing",
		},
		{
			name: "users secret missing the password key",
			objects: func() []client.Object {
				cr := mdbTestCR(func(m *v1alpha1.MysqlDatabase) {
					m.Spec.Users = []v1alpha1.MysqlDatabaseUser{{
						SecretName: "support-ro-mysql",
						Privileges: []v1alpha1.MysqlPrivilege{v1alpha1.PrivilegeSelect},
					}}
				})
				s := &corev1.Secret{
					ObjectMeta: metav1.ObjectMeta{Name: "support-ro-mysql", Namespace: mdbTestNamespace},
					Data:       map[string][]byte{"username": []byte("acme_support")},
				}
				return []client.Object{cr, mdbTestGroup(), mdbTestOwnerSecret(), s}
			},
			wantReason: "UserSecretIncomplete",
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			r, c, _ := newMdbReconciler(t, tc.objects()...)

			res, err := r.Reconcile(context.Background(), mdbRequest())
			if err != nil {
				t.Fatalf("Reconcile() error = %v, want nil (Pending is not an error)", err)
			}
			assertPendingRequeue(t, res.RequeueAfter)

			mdb := getMdb(t, c)
			if mdb.Status.Phase != v1alpha1.MysqlDatabasePhasePending {
				t.Fatalf("phase = %q, want Pending", mdb.Status.Phase)
			}
			if mdb.Status.ObservedGeneration != mdb.Generation {
				t.Fatalf("observedGeneration = %d, want %d", mdb.Status.ObservedGeneration, mdb.Generation)
			}
			cond := readyCondition(mdb)
			if cond == nil {
				t.Fatal("no Ready condition; observedGeneration and Ready are the polling contract")
			}
			if cond.Status != metav1.ConditionFalse {
				t.Fatalf("Ready = %q, want False", cond.Status)
			}
			if cond.Reason != tc.wantReason {
				t.Fatalf("Ready reason = %q, want %q", cond.Reason, tc.wantReason)
			}
		})
	}
}

// TestMysqlDatabaseRejectsBadOwnerUsernameFromSecret is the case the API
// server cannot catch: the owner username arrives from a Secret, so the CRD
// pattern never sees it. It must be rejected before any SQL is rendered.
func TestMysqlDatabaseRejectsBadOwnerUsernameFromSecret(t *testing.T) {
	secret := mdbTestOwnerSecret()
	secret.Data["username"] = []byte(`evil'@'%' IDENTIFIED BY 'x'; GRANT ALL PRIVILEGES ON *.* TO 'evil'@'%`)

	r, c, _ := newMdbReconciler(t, mdbTestCR(), mdbTestGroup(), secret)

	if _, err := r.Reconcile(context.Background(), mdbRequest()); err != nil {
		t.Fatalf("Reconcile() error = %v", err)
	}

	mdb := getMdb(t, c)
	if mdb.Status.Phase != v1alpha1.MysqlDatabasePhaseFailed {
		t.Fatalf("phase = %q, want Failed", mdb.Status.Phase)
	}
	cond := readyCondition(mdb)
	if cond == nil || cond.Reason != "InvalidSpec" {
		t.Fatalf("Ready condition = %+v, want reason InvalidSpec", cond)
	}
	if !strings.Contains(mdb.Status.Message, "spec.owner secret username") {
		t.Fatalf("message = %q, want it to name the offending field", mdb.Status.Message)
	}
	// The dialer in newMdbReconciler fails the test if it was called, so
	// reaching here also proves nothing was executed against MySQL.
}

// TestMysqlDatabaseSkipsWhenNothingChanged is acceptance criterion 3:
// re-applying an unchanged CR performs zero MySQL statements. The tripwire
// dialer enforces "zero" literally — it never even connects.
func TestMysqlDatabaseSkipsWhenNothingChanged(t *testing.T) {
	mdb := mdbTestCR()
	mdb.Status = v1alpha1.MysqlDatabaseStatus{
		Phase:              v1alpha1.MysqlDatabasePhaseReady,
		ObservedGeneration: mdb.Generation,
		DatabaseCreated:    true,
		OwnerUser:          "acme_app",
		ActiveSite:         "dc1",
	}
	secret := mdbTestOwnerSecret()

	r, c, _ := newMdbReconciler(t, mdb, mdbTestGroup(), secret, mdbTestOperatorSecret())

	// The hash must be computed against the objects as stored: the fake
	// client assigns resourceVersions on create, and the Secret's revision
	// is part of the hash.
	var storedSecret corev1.Secret
	if err := c.Get(context.Background(), types.NamespacedName{Namespace: mdbTestNamespace, Name: secret.Name}, &storedSecret); err != nil {
		t.Fatalf("get stored owner secret: %v", err)
	}
	var storedFG v1alpha1.MysqlFailoverGroup
	if err := c.Get(context.Background(), types.NamespacedName{Namespace: mdbTestNamespace, Name: "main"}, &storedFG); err != nil {
		t.Fatalf("get stored failover group: %v", err)
	}
	hash, err := computeDatabaseHash(getMdb(t, c), &storedSecret, nil, &storedFG)
	if err != nil {
		t.Fatalf("computeDatabaseHash() error = %v", err)
	}
	latest := getMdb(t, c)
	latest.Status.LastAppliedHash = hash
	if err := c.Status().Update(context.Background(), latest); err != nil {
		t.Fatalf("set lastAppliedHash: %v", err)
	}

	res, err := r.Reconcile(context.Background(), mdbRequest())
	if err != nil {
		t.Fatalf("Reconcile() error = %v", err)
	}
	if res.RequeueAfter != 0 || res.Requeue {
		t.Fatalf("Reconcile() = %+v, want no requeue for a settled CR", res)
	}
}

// TestMysqlDatabaseFailoverInvalidatesTheSkip proves the group watch is not
// decorative: when the active site moves, the skip check must not swallow
// the re-apply. Detected here by the tripwire dialer firing.
func TestMysqlDatabaseFailoverInvalidatesTheSkip(t *testing.T) {
	mdb := mdbTestCR()
	mdb.Status = v1alpha1.MysqlDatabaseStatus{
		Phase:              v1alpha1.MysqlDatabasePhaseReady,
		ObservedGeneration: mdb.Generation,
		ActiveSite:         "dc1",
	}

	fg := mdbTestGroup()
	fg.Status.ActiveSite = "dc2" // failover happened

	scheme := testScheme()
	c := fake.NewClientBuilder().
		WithScheme(scheme).
		WithStatusSubresource(&v1alpha1.MysqlDatabase{}, &v1alpha1.MysqlFailoverGroup{}).
		WithObjects(mdb, fg, mdbTestOwnerSecret(), mdbTestOperatorSecret()).
		Build()

	// The stale hash must be computed from the STORED objects — the fake
	// client assigns the Secret's resourceVersion on create, and the
	// Secret's revision is part of the hash. Hashing in-memory fixtures
	// with empty revisions would produce a hash that differs for the wrong
	// reason, letting the test pass even if the active site fell out of
	// the hash inputs — the exact regression this test guards.
	var storedSecret corev1.Secret
	if err := c.Get(context.Background(), types.NamespacedName{Namespace: mdbTestNamespace, Name: mdbTestOwnerSecret().Name}, &storedSecret); err != nil {
		t.Fatalf("get stored owner secret: %v", err)
	}
	var storedFG v1alpha1.MysqlFailoverGroup
	if err := c.Get(context.Background(), types.NamespacedName{Namespace: mdbTestNamespace, Name: "main"}, &storedFG); err != nil {
		t.Fatalf("get stored failover group: %v", err)
	}
	staleFG := storedFG.DeepCopy()
	staleFG.Status.ActiveSite = "dc1" // the site the CR last applied on
	staleHash, err := computeDatabaseHash(getMdb(t, c), &storedSecret, nil, staleFG)
	if err != nil {
		t.Fatalf("computeDatabaseHash() error = %v", err)
	}
	latest := getMdb(t, c)
	latest.Status.LastAppliedHash = staleHash
	if err := c.Status().Update(context.Background(), latest); err != nil {
		t.Fatalf("set lastAppliedHash: %v", err)
	}

	dialed := false
	r := &MysqlDatabaseReconciler{
		Client:   c,
		Scheme:   scheme,
		Recorder: record.NewFakeRecorder(20),
		OpenDB: func(user, password, addr, tlsConfigName string) (*sql.DB, error) {
			dialed = true
			if !strings.Contains(addr, "dc2") {
				t.Fatalf("dialed %q, want the new primary dc2", addr)
			}
			return nil, errTestDialRefused
		},
	}

	if _, err := r.Reconcile(context.Background(), mdbRequest()); err != nil {
		t.Fatalf("Reconcile() error = %v", err)
	}
	if !dialed {
		t.Fatal("reconciler skipped the re-apply after a failover; the active site must be part of the hash")
	}

	// A dial failure is transient, not a broken tenant.
	got := getMdb(t, c)
	if got.Status.Phase != v1alpha1.MysqlDatabasePhasePending {
		t.Fatalf("phase = %q, want Pending after an unreachable primary", got.Status.Phase)
	}
	if cond := readyCondition(got); cond == nil || cond.Reason != "PrimaryUnavailable" {
		t.Fatalf("Ready condition = %+v, want reason PrimaryUnavailable", cond)
	}
}

var errTestDialRefused = &dialRefusedError{}

type dialRefusedError struct{}

func (e *dialRefusedError) Error() string { return "connection refused" }

// TestMysqlDatabaseDeleteRetainNeverTouchesMySQL is the data-loss guard. The
// default policy must release the finalizer without so much as opening a
// connection.
func TestMysqlDatabaseDeleteRetainNeverTouchesMySQL(t *testing.T) {
	for _, policy := range []v1alpha1.MysqlDatabaseDeletionPolicy{"", v1alpha1.MysqlDatabaseRetain} {
		t.Run(string("policy="+policy), func(t *testing.T) {
			now := metav1.Now()
			mdb := mdbTestCR(func(m *v1alpha1.MysqlDatabase) {
				m.Spec.DeletionPolicy = policy
				m.DeletionTimestamp = &now
				m.Status.OwnerUser = "acme_app"
			})

			r, c, rec := newMdbReconciler(t, mdb, mdbTestGroup(), mdbTestOwnerSecret())

			if _, err := r.Reconcile(context.Background(), mdbRequest()); err != nil {
				t.Fatalf("Reconcile() error = %v", err)
			}

			// The fake client removes the object once the last finalizer
			// goes away, so a NotFound here is the success signal.
			var out v1alpha1.MysqlDatabase
			err := c.Get(context.Background(), mdbRequest().NamespacedName, &out)
			if err == nil && controllerutil.ContainsFinalizer(&out, MysqlDatabaseFinalizer) {
				t.Fatal("finalizer still present; Retain must release immediately")
			}

			assertEventContains(t, rec, "DatabaseRetained")
		})
	}
}

// TestMysqlDatabaseDeleteWithoutGroupReleases keeps a Delete-policy CR from
// wedging forever when the group it referenced is already gone.
func TestMysqlDatabaseDeleteWithoutGroupReleases(t *testing.T) {
	now := metav1.Now()
	mdb := mdbTestCR(func(m *v1alpha1.MysqlDatabase) {
		m.Spec.DeletionPolicy = v1alpha1.MysqlDatabaseDelete
		m.DeletionTimestamp = &now
		m.Status.DatabaseCreated = true // DDL was applied; there is something to drop
	})

	r, c, rec := newMdbReconciler(t, mdb)

	if _, err := r.Reconcile(context.Background(), mdbRequest()); err != nil {
		t.Fatalf("Reconcile() error = %v", err)
	}

	var out v1alpha1.MysqlDatabase
	err := c.Get(context.Background(), mdbRequest().NamespacedName, &out)
	if err == nil && controllerutil.ContainsFinalizer(&out, MysqlDatabaseFinalizer) {
		t.Fatal("finalizer still present; a vanished group must not wedge deletion")
	}
	assertEventContains(t, rec, "DatabaseCleanupSkipped")
}

// TestMysqlDatabaseDeleteWithoutAppliedDDLReleases proves the write-ahead
// gate: a Delete-policy CR that never executed any SQL (it failed validation,
// lost an ownership conflict, or never got that far) must release without
// connecting to MySQL — it has nothing of its own to drop, and the database
// it names may belong to someone else.
func TestMysqlDatabaseDeleteWithoutAppliedDDLReleases(t *testing.T) {
	now := metav1.Now()
	mdb := mdbTestCR(func(m *v1alpha1.MysqlDatabase) {
		m.Spec.DeletionPolicy = v1alpha1.MysqlDatabaseDelete
		m.DeletionTimestamp = &now
		// status.DatabaseCreated deliberately unset.
	})

	// The group exists with an active site, so the only thing standing
	// between this CR and a DROP DATABASE is the gate under test. The
	// tripwire dialer in newMdbReconciler fails the test if any connection
	// is attempted.
	r, c, rec := newMdbReconciler(t, mdb, mdbTestGroup(), mdbTestOperatorSecret())

	if _, err := r.Reconcile(context.Background(), mdbRequest()); err != nil {
		t.Fatalf("Reconcile() error = %v", err)
	}

	var out v1alpha1.MysqlDatabase
	err := c.Get(context.Background(), mdbRequest().NamespacedName, &out)
	if err == nil && controllerutil.ContainsFinalizer(&out, MysqlDatabaseFinalizer) {
		t.Fatal("finalizer still present; a never-applied CR must release without dropping anything")
	}
	assertEventContains(t, rec, "DatabaseDropSkipped")
}

// TestMysqlDatabaseDeleteDefersWithoutActiveSite proves the opposite side of
// the same coin: a requested DROP is deferred, never silently skipped.
func TestMysqlDatabaseDeleteDefersWithoutActiveSite(t *testing.T) {
	now := metav1.Now()
	mdb := mdbTestCR(func(m *v1alpha1.MysqlDatabase) {
		m.Spec.DeletionPolicy = v1alpha1.MysqlDatabaseDelete
		m.DeletionTimestamp = &now
		m.Status.DatabaseCreated = true
	})
	fg := mdbTestGroup()
	fg.Status.ActiveSite = ""

	r, c, rec := newMdbReconciler(t, mdb, fg)

	res, err := r.Reconcile(context.Background(), mdbRequest())
	if err != nil {
		t.Fatalf("Reconcile() error = %v", err)
	}
	assertPendingRequeue(t, res.RequeueAfter)

	out := getMdb(t, c)
	if !controllerutil.ContainsFinalizer(out, MysqlDatabaseFinalizer) {
		t.Fatal("finalizer released without performing the requested DROP")
	}
	assertEventContains(t, rec, "DatabaseDropDeferred")
}

func TestMysqlDatabaseAddsFinalizerOnFirstReconcile(t *testing.T) {
	mdb := mdbTestCR(func(m *v1alpha1.MysqlDatabase) { m.Finalizers = nil })
	r, c, _ := newMdbReconciler(t, mdb, mdbTestGroup(), mdbTestOwnerSecret())

	res, err := r.Reconcile(context.Background(), mdbRequest())
	if err != nil {
		t.Fatalf("Reconcile() error = %v", err)
	}
	if !res.Requeue {
		t.Fatal("expected a requeue after adding the finalizer")
	}
	if !controllerutil.ContainsFinalizer(getMdb(t, c), MysqlDatabaseFinalizer) {
		t.Fatal("finalizer was not added")
	}
}

// TestGroupActiveSiteChangedPredicate keeps a busy MysqlFailoverGroup's
// heartbeat status writes from fanning out to every tenant CR, while still
// firing on the transitions that matter.
func TestGroupActiveSiteChangedPredicate(t *testing.T) {
	p := groupActiveSiteChangedPredicate()

	base := mdbTestGroup()

	unchanged := base.DeepCopy()
	unchanged.Status.LastFailoverTarget = "dc2" // unrelated status churn
	if p.Update(event.UpdateEvent{ObjectOld: base, ObjectNew: unchanged}) {
		t.Fatal("predicate fired on unrelated group status churn")
	}

	movedPrimary := base.DeepCopy()
	movedPrimary.Status.ActiveSite = "dc2"
	if !p.Update(event.UpdateEvent{ObjectOld: base, ObjectNew: movedPrimary}) {
		t.Fatal("predicate did not fire when the active site moved")
	}

	fenced := base.DeepCopy()
	fenced.Spec.RestoreInPlace = &v1alpha1.RestoreInPlaceSpec{}
	fenced.Status.RestoreInPlace = &v1alpha1.RestoreInPlaceStatus{Phase: v1alpha1.RestoreInPlaceRestoring}
	if !p.Update(event.UpdateEvent{ObjectOld: base, ObjectNew: fenced}) {
		t.Fatal("predicate did not fire when the group became fenced")
	}
	if !p.Update(event.UpdateEvent{ObjectOld: fenced, ObjectNew: base}) {
		t.Fatal("predicate did not fire when the group left the fenced state")
	}
}

func TestMysqlDatabaseWatchMapping(t *testing.T) {
	other := mdbTestCR(func(m *v1alpha1.MysqlDatabase) {
		m.Name = "tenant-other"
		m.Spec.GroupRef.Name = "elsewhere"
		m.Spec.Owner.SecretName = "other-owner"
	})
	r, _, _ := newMdbReconciler(t, mdbTestCR(), other, mdbTestGroup(), mdbTestOwnerSecret())

	ctx := context.Background()

	got := r.mapGroupToDatabases(ctx, mdbTestGroup())
	if len(got) != 1 || got[0].Name != "tenant-acme" {
		t.Fatalf("mapGroupToDatabases() = %v, want just tenant-acme", got)
	}

	got = r.mapSecretToDatabases(ctx, mdbTestOwnerSecret())
	if len(got) != 1 || got[0].Name != "tenant-acme" {
		t.Fatalf("mapSecretToDatabases() = %v, want just tenant-acme", got)
	}
}

func assertEventContains(t *testing.T, rec *record.FakeRecorder, want string) {
	t.Helper()
	for {
		select {
		case ev := <-rec.Events:
			if strings.Contains(ev, want) {
				return
			}
		default:
			t.Fatalf("no event containing %q was recorded", want)
		}
	}
}

// TestAdminCredentialsBothModes covers the two ways a MysqlFailoverGroup
// carries its operator credential. The legacy DSN mode matters here because
// the playground and every pre-credentials group uses it: reconcileCredentials
// is guarded by UsesCredentials(), so before MysqlDatabase existed nothing
// ever reached this code with spec.credentials nil.
func TestAdminCredentialsBothModes(t *testing.T) {
	ctx := context.Background()

	t.Run("credentials mode", func(t *testing.T) {
		fg := mdbTestGroup()
		_, c, _ := newMdbReconciler(t, fg, mdbTestOperatorSecret())

		user, pass, root, err := adminCredentials(ctx, c, fg)
		if err != nil {
			t.Fatalf("adminCredentials() error = %v", err)
		}
		if user != "operator" || pass != "op-pw" || root != "root-pw" {
			t.Fatalf("adminCredentials() = (%q, %q, %q)", user, pass, root)
		}
	})

	t.Run("legacy dsn mode", func(t *testing.T) {
		fg := mdbTestGroup()
		fg.Spec.Credentials = nil
		fg.Spec.SecretName = "mysql-credentials"
		secret := &corev1.Secret{
			ObjectMeta: metav1.ObjectMeta{Name: "mysql-credentials", Namespace: mdbTestNamespace},
			Data: map[string][]byte{
				"dsn":                 []byte("root:playground-root-pw@tcp(127.0.0.1:3306)/mysql"),
				"MYSQL_ROOT_PASSWORD": []byte("playground-root-pw"),
			},
		}
		_, c, _ := newMdbReconciler(t, fg, secret)

		user, pass, root, err := adminCredentials(ctx, c, fg)
		if err != nil {
			t.Fatalf("adminCredentials() error = %v", err)
		}
		if user != "root" || pass != "playground-root-pw" || root != "playground-root-pw" {
			t.Fatalf("adminCredentials() = (%q, %q, %q)", user, pass, root)
		}
	})

	t.Run("dsn mode with no dsn key", func(t *testing.T) {
		fg := mdbTestGroup()
		fg.Spec.Credentials = nil
		fg.Spec.SecretName = "mysql-credentials"
		secret := &corev1.Secret{
			ObjectMeta: metav1.ObjectMeta{Name: "mysql-credentials", Namespace: mdbTestNamespace},
			Data:       map[string][]byte{"MYSQL_ROOT_PASSWORD": []byte("x")},
		}
		_, c, _ := newMdbReconciler(t, fg, secret)

		if _, _, _, err := adminCredentials(ctx, c, fg); err == nil {
			t.Fatal("adminCredentials() = nil error, want a missing-dsn failure")
		}
	})
}

// TestMysqlDatabaseReconcilesAgainstDSNModeGroup is the end-to-end version of
// the same point: a DSN-mode group must not panic the reconciler.
func TestMysqlDatabaseReconcilesAgainstDSNModeGroup(t *testing.T) {
	fg := mdbTestGroup()
	fg.Spec.Credentials = nil
	fg.Spec.SecretName = "mysql-credentials"
	secret := &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{Name: "mysql-credentials", Namespace: mdbTestNamespace},
		Data:       map[string][]byte{"dsn": []byte("root:pw@tcp(127.0.0.1:3306)/mysql")},
	}

	scheme := testScheme()
	c := fake.NewClientBuilder().
		WithScheme(scheme).
		WithStatusSubresource(&v1alpha1.MysqlDatabase{}, &v1alpha1.MysqlFailoverGroup{}).
		WithObjects(mdbTestCR(), fg, secret, mdbTestOwnerSecret()).
		Build()

	var dialedUser string
	r := &MysqlDatabaseReconciler{
		Client:   c,
		Scheme:   scheme,
		Recorder: record.NewFakeRecorder(20),
		OpenDB: func(user, password, addr, tlsConfigName string) (*sql.DB, error) {
			dialedUser = user
			return nil, errTestDialRefused
		},
	}

	if _, err := r.Reconcile(context.Background(), mdbRequest()); err != nil {
		t.Fatalf("Reconcile() error = %v", err)
	}
	if dialedUser != "root" {
		t.Fatalf("dialed as %q, want the DSN user root", dialedUser)
	}
}

// TestMysqlDatabaseRefusesReservedOwnerUsers is a privilege-escalation guard.
//
// Bloodraven applies ALTER USER ... IDENTIFIED BY from the owner Secret's
// bytes. Pointed at a Secret naming root or a group-level credential, that
// same statement resets a privileged account's password to a value the
// Secret's author chose. The CRD's own documentation says the two MySQL-admin
// call sites manage disjoint principals; this is the check that makes it so.
func TestMysqlDatabaseRefusesReservedOwnerUsers(t *testing.T) {
	appSecret := &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{Name: "mysql-app", Namespace: mdbTestNamespace},
		Data:       map[string][]byte{"username": []byte("app"), "password": []byte("app-pw")},
	}

	for _, username := range []string{"root", "operator", "app", "replicator"} {
		t.Run(username, func(t *testing.T) {
			fg := mdbTestGroup()
			fg.Spec.Credentials.AppSecret = "mysql-app"

			operator := mdbTestOperatorSecret()
			operator.Data["MYSQL_REPLICATION_USER"] = []byte("replicator")

			owner := mdbTestOwnerSecret()
			owner.Data["username"] = []byte(username)
			owner.Data["password"] = []byte("attacker-chosen")

			r, c, rec := newMdbReconciler(t, mdbTestCR(), fg, operator, appSecret, owner)

			if _, err := r.Reconcile(context.Background(), mdbRequest()); err != nil {
				t.Fatalf("Reconcile() error = %v", err)
			}

			mdb := getMdb(t, c)
			if mdb.Status.Phase != v1alpha1.MysqlDatabasePhaseFailed {
				t.Fatalf("phase = %q, want Failed for reserved owner %q", mdb.Status.Phase, username)
			}
			if cond := readyCondition(mdb); cond == nil || cond.Reason != "OwnerUserReserved" {
				t.Fatalf("Ready condition = %+v, want reason OwnerUserReserved", cond)
			}
			assertEventContains(t, rec, "OwnerUserReserved")
			// newMdbReconciler's dialer fails the test if called, so no
			// ALTER USER was ever built, let alone executed.
		})
	}
}

func TestReservedGroupUsernames(t *testing.T) {
	fg := mdbTestGroup()
	fg.Spec.Credentials.AppSecret = "mysql-app"

	operator := mdbTestOperatorSecret()
	operator.Data["MYSQL_REPLICATION_USER"] = []byte("replicator")
	appSecret := &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{Name: "mysql-app", Namespace: mdbTestNamespace},
		Data:       map[string][]byte{"username": []byte("app")},
	}

	_, c, _ := newMdbReconciler(t, fg, operator, appSecret)

	got, err := reservedGroupUsernames(context.Background(), c, fg)
	if err != nil {
		t.Fatalf("reservedGroupUsernames() error = %v", err)
	}
	for _, want := range []string{
		"root", "operator", "app", "replicator",
		"mysql.sys", "mysql.session", "mysql.infoschema",
	} {
		if !got[want] {
			t.Fatalf("reservedGroupUsernames() missing %q: %v", want, got)
		}
	}
	if got["acme_app"] {
		t.Fatalf("reservedGroupUsernames() wrongly reserved a tenant owner: %v", got)
	}
}

// A group Secret that is named but absent — a delete+recreate rotation, or
// bootstrap ordering — must fail the check closed with a NotFound the caller
// maps to Pending. Skipping it would un-reserve exactly the username being
// rotated for exactly the window in which it could be hijacked.
func TestReservedGroupUsernamesFailsClosedOnAbsentSecret(t *testing.T) {
	fg := mdbTestGroup()
	fg.Spec.Credentials.MonitorSecret = "mysql-monitor" // named, not created

	_, c, _ := newMdbReconciler(t, fg, mdbTestOperatorSecret())

	_, err := reservedGroupUsernames(context.Background(), c, fg)
	if err == nil {
		t.Fatal("reservedGroupUsernames() = nil error for a named-but-absent group Secret, want NotFound")
	}
	if !apierrors.IsNotFound(err) {
		t.Fatalf("reservedGroupUsernames() error = %v, want NotFound (the caller maps it to Pending)", err)
	}
}

// A transient Secret read error must fail the check closed, not shrink the
// reserved set. Otherwise one flaky API call is a window in which a tenant
// Secret naming the operator user passes the OwnerUserReserved guard.
func TestReservedGroupUsernamesFailsClosed(t *testing.T) {
	fg := mdbTestGroup()
	fg.Spec.Credentials.AppSecret = "mysql-app"
	operator := mdbTestOperatorSecret()
	appSecret := &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{Name: "mysql-app", Namespace: mdbTestNamespace},
		Data:       map[string][]byte{"username": []byte("app")},
	}

	c := fake.NewClientBuilder().
		WithScheme(testScheme()).
		WithObjects(fg, operator, appSecret).
		WithInterceptorFuncs(interceptor.Funcs{
			Get: func(ctx context.Context, cl client.WithWatch, key client.ObjectKey, obj client.Object, opts ...client.GetOption) error {
				if key.Name == "mysql-app" {
					return apierrors.NewInternalError(errors.New("etcd timeout"))
				}
				return cl.Get(ctx, key, obj, opts...)
			},
		}).
		Build()

	if _, err := reservedGroupUsernames(context.Background(), c, fg); err == nil {
		t.Fatal("reservedGroupUsernames() = nil error on a non-NotFound Secret read failure, want fail-closed error")
	}
}

// TestMysqlDatabaseOwnershipConflicts: two CRs must not fight over one
// database or one owner principal — the stakes are a DROP DATABASE of the
// survivor's data or an ALTER USER tug-of-war over its credential. The older
// CR wins; the newer fails before any SQL is rendered (the tripwire dialer
// proves no connection was attempted).
func TestMysqlDatabaseOwnershipConflicts(t *testing.T) {
	older := func(m *v1alpha1.MysqlDatabase) {
		m.Name = "tenant-elder"
		m.CreationTimestamp = metav1.NewTime(metav1.Now().Add(-time.Hour))
		m.Status.OwnerUser = "elder_app"
	}

	cases := []struct {
		name       string
		mutateOld  func(*v1alpha1.MysqlDatabase)
		wantReason string
	}{
		{
			name: "same database name",
			mutateOld: func(m *v1alpha1.MysqlDatabase) {
				older(m)
				m.Spec.Owner.SecretName = "elder-owner"
			},
			wantReason: "DatabaseNameConflict",
		},
		{
			name: "same owner secret",
			mutateOld: func(m *v1alpha1.MysqlDatabase) {
				older(m)
				m.Spec.DatabaseName = "elder_wms"
			},
			wantReason: "OwnerConflict",
		},
		{
			name: "same owner username via different secret",
			mutateOld: func(m *v1alpha1.MysqlDatabase) {
				older(m)
				m.Spec.DatabaseName = "elder_wms"
				m.Spec.Owner.SecretName = "elder-owner"
				m.Status.OwnerUser = "acme_app" // matches the newer CR's Secret
			},
			wantReason: "OwnerConflict",
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			elder := mdbTestCR(tc.mutateOld)
			newer := mdbTestCR(func(m *v1alpha1.MysqlDatabase) {
				m.CreationTimestamp = metav1.Now()
			})
			r, c, _ := newMdbReconciler(t,
				newer, elder, mdbTestGroup(), mdbTestOperatorSecret(), mdbTestOwnerSecret())

			if _, err := r.Reconcile(context.Background(), mdbRequest()); err != nil {
				t.Fatalf("Reconcile() error = %v", err)
			}

			mdb := getMdb(t, c)
			if mdb.Status.Phase != v1alpha1.MysqlDatabasePhaseFailed {
				t.Fatalf("phase = %q, want Failed", mdb.Status.Phase)
			}
			if cond := readyCondition(mdb); cond == nil || cond.Reason != tc.wantReason {
				t.Fatalf("Ready reason = %+v, want %s", cond, tc.wantReason)
			}
			if mdb.Status.DatabaseCreated {
				t.Fatal("a conflicted CR must fail before the write-ahead stamp; its delete must have nothing to drop")
			}
		})
	}
}

// TestMysqlDatabaseConflictLoserDeleteDropsNothing closes the loop on the
// conflict guard: deleting the losing CR with deletionPolicy: Delete must not
// touch MySQL, because DatabaseCreated was never stamped. Without the gate,
// this exact sequence dropped the winner's live database.
func TestMysqlDatabaseConflictLoserDeleteDropsNothing(t *testing.T) {
	now := metav1.Now()
	loser := mdbTestCR(func(m *v1alpha1.MysqlDatabase) {
		m.Spec.DeletionPolicy = v1alpha1.MysqlDatabaseDelete
		m.DeletionTimestamp = &now
		m.Status.Phase = v1alpha1.MysqlDatabasePhaseFailed // lost the conflict; no DatabaseCreated
	})
	winner := mdbTestCR(func(m *v1alpha1.MysqlDatabase) {
		m.Name = "tenant-elder"
		m.CreationTimestamp = metav1.NewTime(metav1.Now().Add(-time.Hour))
	})

	r, c, rec := newMdbReconciler(t, loser, winner, mdbTestGroup(), mdbTestOperatorSecret())

	if _, err := r.Reconcile(context.Background(), mdbRequest()); err != nil {
		t.Fatalf("Reconcile() error = %v", err)
	}
	var out v1alpha1.MysqlDatabase
	if err := c.Get(context.Background(), mdbRequest().NamespacedName, &out); err == nil &&
		controllerutil.ContainsFinalizer(&out, MysqlDatabaseFinalizer) {
		t.Fatal("conflict loser's finalizer did not release")
	}
	assertEventContains(t, rec, "DatabaseDropSkipped")
	// The tripwire dialer in newMdbReconciler proves no DROP was attempted.
}

// TestTransientSQLError pins the boundary between "the CR is broken" and
// "the primary is mid-failover": only the former may latch Phase=Failed.
func TestTransientSQLError(t *testing.T) {
	cases := []struct {
		name string
		err  error
		want bool
	}{
		{"read-only primary during promotion (1290)", &mysqldriver.MySQLError{Number: 1290, Message: "running with --super-read-only"}, true},
		{"read-only mode (1836)", &mysqldriver.MySQLError{Number: 1836, Message: "read only mode"}, true},
		{"wrapped 1290", fmt.Errorf("exec x: %w", &mysqldriver.MySQLError{Number: 1290}), true},
		{"bad conn", driver.ErrBadConn, true},
		{"invalid conn", mysqldriver.ErrInvalidConn, true},
		{"eof mid-handshake", io.EOF, true},
		{"net timeout", &net.OpError{Op: "dial", Err: &timeoutErr{}}, true},
		{"syntax error (1064)", &mysqldriver.MySQLError{Number: 1064, Message: "syntax"}, false},
		{"access denied (1044)", &mysqldriver.MySQLError{Number: 1044, Message: "denied"}, false},
		{"plain error", errors.New("rendered garbage"), false},
		{"reconcile budget expired", context.DeadlineExceeded, true},
		{"wrapped deadline", fmt.Errorf("exec x: %w", context.DeadlineExceeded), true},
		{"context canceled", context.Canceled, true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := transientSQLError(tc.err); got != tc.want {
				t.Fatalf("transientSQLError(%v) = %v, want %v", tc.err, got, tc.want)
			}
		})
	}
}

// assertPendingRequeue checks the requeue delay lands in the jittered
// Pending window [15s, 45s): the lockstep a fixed 30s would produce on a
// failover fan-out is exactly what the jitter removes.
func assertPendingRequeue(t *testing.T, got time.Duration) {
	t.Helper()
	if got < mysqlDatabasePendingRequeue/2 || got > mysqlDatabasePendingRequeue*3/2 {
		t.Fatalf("RequeueAfter = %v, want within [%v, %v]", got, mysqlDatabasePendingRequeue/2, mysqlDatabasePendingRequeue*3/2)
	}
}

// TestMysqlDatabaseRefusesSystemSchemaNames is the Go half of the
// system-schema gate (the CEL rule is the API-server half): databaseName
// must never name a schema MySQL itself manages, in any case.
func TestMysqlDatabaseRefusesSystemSchemaNames(t *testing.T) {
	for _, name := range []string{"mysql", "MySQL", "sys", "information_schema", "performance_schema"} {
		t.Run(name, func(t *testing.T) {
			r, c, _ := newMdbReconciler(t,
				mdbTestCR(func(m *v1alpha1.MysqlDatabase) { m.Spec.DatabaseName = name }),
				mdbTestGroup(), mdbTestOwnerSecret(), mdbTestOperatorSecret())

			if _, err := r.Reconcile(context.Background(), mdbRequest()); err != nil {
				t.Fatalf("Reconcile() error = %v", err)
			}
			mdb := getMdb(t, c)
			if mdb.Status.Phase != v1alpha1.MysqlDatabasePhaseFailed {
				t.Fatalf("phase = %q, want Failed", mdb.Status.Phase)
			}
			if cond := readyCondition(mdb); cond == nil || cond.Reason != "InvalidSpec" ||
				!strings.Contains(cond.Message, "system schema") {
				t.Fatalf("Ready condition = %+v, want InvalidSpec naming the system schema", cond)
			}
		})
	}
}

// TestMysqlDatabaseRefusesGrantsNamingOwner: a grants[] entry that names the
// owner username would silently override spec.owner.privileges, because the
// grants pass runs after the owner grant. Validate rejects it.
func TestMysqlDatabaseRefusesGrantsNamingOwner(t *testing.T) {
	cr := mdbTestCR(func(m *v1alpha1.MysqlDatabase) {
		m.Spec.Grants = []v1alpha1.MysqlDatabaseGrant{
			{Username: "acme_app", Privileges: []v1alpha1.MysqlPrivilege{v1alpha1.PrivilegeSelect}},
		}
	})
	r, c, _ := newMdbReconciler(t, cr, mdbTestGroup(), mdbTestOwnerSecret(), mdbTestOperatorSecret())

	if _, err := r.Reconcile(context.Background(), mdbRequest()); err != nil {
		t.Fatalf("Reconcile() error = %v", err)
	}
	mdb := getMdb(t, c)
	if mdb.Status.Phase != v1alpha1.MysqlDatabasePhaseFailed {
		t.Fatalf("phase = %q, want Failed", mdb.Status.Phase)
	}
	if cond := readyCondition(mdb); cond == nil || cond.Reason != "InvalidSpec" ||
		!strings.Contains(cond.Message, "owner username") {
		t.Fatalf("Ready condition = %+v, want InvalidSpec naming the owner collision", cond)
	}
}

// TestMysqlDatabaseSkipsApplyToTerminatingGroup: new tenant DDL must not
// start against a group that is being torn down.
func TestMysqlDatabaseSkipsApplyToTerminatingGroup(t *testing.T) {
	now := metav1.Now()
	fg := mdbTestGroup()
	fg.DeletionTimestamp = &now
	fg.Finalizers = []string{"keep"}
	r, c, _ := newMdbReconciler(t, mdbTestCR(), fg, mdbTestOwnerSecret(), mdbTestOperatorSecret())

	res, err := r.Reconcile(context.Background(), mdbRequest())
	if err != nil {
		t.Fatalf("Reconcile() error = %v", err)
	}
	assertPendingRequeue(t, res.RequeueAfter)
	mdb := getMdb(t, c)
	if cond := readyCondition(mdb); cond == nil || cond.Reason != "GroupTerminating" {
		t.Fatalf("Ready condition = %+v, want GroupTerminating", cond)
	}
}

// TestMysqlDatabasePeerSecretMissingDefersArbitration: when a higher-ranked
// peer's owner Secret cannot be read and its status does not answer either,
// arbitration must wait rather than guess — guessing is how two CRs end up
// sharing one MySQL account.
func TestMysqlDatabasePeerSecretMissingDefersArbitration(t *testing.T) {
	elder := mdbTestCR(func(m *v1alpha1.MysqlDatabase) {
		m.Name = "tenant-elder"
		m.CreationTimestamp = metav1.NewTime(metav1.Now().Add(-time.Hour))
		m.Spec.DatabaseName = "elder_wms"
		m.Spec.Owner.SecretName = "elder-owner" // absent on purpose
	})
	r, c, _ := newMdbReconciler(t,
		mdbTestCR(func(m *v1alpha1.MysqlDatabase) { m.CreationTimestamp = metav1.Now() }),
		elder, mdbTestGroup(),
		mdbTestOwnerSecret(), mdbTestOperatorSecret())

	res, err := r.Reconcile(context.Background(), mdbRequest())
	if err != nil {
		t.Fatalf("Reconcile() error = %v", err)
	}
	assertPendingRequeue(t, res.RequeueAfter)
	mdb := getMdb(t, c)
	if mdb.Status.Phase != v1alpha1.MysqlDatabasePhasePending {
		t.Fatalf("phase = %q, want Pending", mdb.Status.Phase)
	}
	if cond := readyCondition(mdb); cond == nil || cond.Reason != "PeerOwnerSecretMissing" {
		t.Fatalf("Ready condition = %+v, want PeerOwnerSecretMissing", cond)
	}
}

// TestMysqlDatabaseConflictDetectsPeerSecretUsername closes the arbitration
// blind spot: a higher-ranked peer that has not reconciled yet (empty
// status.ownerUser) is still detected through the username its Secret names.
func TestMysqlDatabaseConflictDetectsPeerSecretUsername(t *testing.T) {
	elder := mdbTestCR(func(m *v1alpha1.MysqlDatabase) {
		m.Name = "tenant-elder"
		m.CreationTimestamp = metav1.NewTime(metav1.Now().Add(-time.Hour))
		m.Spec.DatabaseName = "elder_wms"
		m.Spec.Owner.SecretName = "elder-owner"
	})
	elderSecret := &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{Name: "elder-owner", Namespace: mdbTestNamespace},
		Data:       map[string][]byte{"username": []byte("acme_app"), "password": []byte("pw")},
	}
	r, c, _ := newMdbReconciler(t,
		mdbTestCR(func(m *v1alpha1.MysqlDatabase) { m.CreationTimestamp = metav1.Now() }),
		elder, mdbTestGroup(),
		mdbTestOwnerSecret(), mdbTestOperatorSecret(), elderSecret)

	if _, err := r.Reconcile(context.Background(), mdbRequest()); err != nil {
		t.Fatalf("Reconcile() error = %v", err)
	}
	mdb := getMdb(t, c)
	if mdb.Status.Phase != v1alpha1.MysqlDatabasePhaseFailed {
		t.Fatalf("phase = %q, want Failed", mdb.Status.Phase)
	}
	if cond := readyCondition(mdb); cond == nil || cond.Reason != "OwnerConflict" {
		t.Fatalf("Ready condition = %+v, want OwnerConflict", cond)
	}
}

// TestMysqlDatabaseConnectFailureKeepsWriteAheadClean is the regression the
// moved write-ahead stamp pins down: a CR that never connected must not
// carry databaseCreated, and its deletionPolicy: Delete must therefore drop
// nothing — not even a same-named database created by someone else.
func TestMysqlDatabaseConnectFailureKeepsWriteAheadClean(t *testing.T) {
	r, c, _ := newMdbReconciler(t, mdbTestCR(), mdbTestGroup(), mdbTestOwnerSecret(), mdbTestOperatorSecret())
	r.OpenDB = func(user, password, addr, tlsConfigName string) (*sql.DB, error) {
		return nil, errTestDialRefused
	}

	if _, err := r.Reconcile(context.Background(), mdbRequest()); err != nil {
		t.Fatalf("Reconcile() error = %v", err)
	}
	mdb := getMdb(t, c)
	if mdb.Status.Phase != v1alpha1.MysqlDatabasePhasePending {
		t.Fatalf("phase = %q, want Pending after a failed connection", mdb.Status.Phase)
	}
	if mdb.Status.DatabaseCreated {
		t.Fatal("status.databaseCreated stamped without a connection; it would authorize dropping objects this CR never touched")
	}
	if mdb.Status.OwnerUser != "" {
		t.Fatalf("status.ownerUser = %q without a connection, want empty", mdb.Status.OwnerUser)
	}

	// Deleting now must release without dropping anything.
	mdb.Spec.DeletionPolicy = v1alpha1.MysqlDatabaseDelete
	if err := c.Update(context.Background(), mdb); err != nil {
		t.Fatalf("set deletionPolicy: %v", err)
	}
	if err := c.Delete(context.Background(), mdb); err != nil {
		t.Fatalf("delete: %v", err)
	}
	// The tripwire dialer is re-armed by restoring the fixture default: any
	// connection attempt on the delete path fails the test.
	r.OpenDB = func(user, password, addr, tlsConfigName string) (*sql.DB, error) {
		t.Fatalf("delete path dialed MySQL for a CR that never applied")
		return nil, nil
	}
	if _, err := r.Reconcile(context.Background(), mdbRequest()); err != nil {
		t.Fatalf("Reconcile() delete error = %v", err)
	}
	var out v1alpha1.MysqlDatabase
	if err := c.Get(context.Background(), mdbRequest().NamespacedName, &out); err == nil &&
		controllerutil.ContainsFinalizer(&out, MysqlDatabaseFinalizer) {
		t.Fatal("finalizer not released")
	}
}

// TestMysqlDatabaseDeleteScope guards the per-candidate vetting on the
// delete path: reserved principals and principals still granted by a
// sibling CR are never dropped, while a crashed mid-rotation handover is
// cleaned up for both usernames.
func TestMysqlDatabaseDeleteScope(t *testing.T) {
	deleting := func(mutate ...func(*v1alpha1.MysqlDatabase)) *v1alpha1.MysqlDatabase {
		now := metav1.Now()
		return mdbTestCR(func(m *v1alpha1.MysqlDatabase) {
			m.DeletionTimestamp = &now
			m.Spec.DeletionPolicy = v1alpha1.MysqlDatabaseDelete
			m.Status.DatabaseCreated = true
			m.Status.OwnerUser = "acme_app"
			for _, f := range mutate {
				f(m)
			}
		})
	}

	t.Run("reserved owner survives deletion", func(t *testing.T) {
		mdb := deleting()
		r, _, rec := newMdbReconciler(t, mdb, mdbTestGroup())

		dropDB, dropOwners, err := r.deleteScope(context.Background(), mdb, map[string]bool{"acme_app": true})
		if err != nil {
			t.Fatalf("deleteScope() error = %v", err)
		}
		if !dropDB {
			t.Fatal("dropDB = false, want true")
		}
		if len(dropOwners) != 0 {
			t.Fatalf("dropOwners = %v, want none — the owner became a group principal", dropOwners)
		}
		assertEventContains(t, rec, "OwnerUserReservedSkipped")
	})

	t.Run("sibling grants claim survives deletion", func(t *testing.T) {
		mdb := deleting()
		sibling := mdbTestCR(func(m *v1alpha1.MysqlDatabase) {
			m.Name = "tenant-sibling"
			m.Spec.DatabaseName = "sibling_wms"
			m.Spec.Owner.SecretName = "sibling-owner"
			m.Spec.Grants = []v1alpha1.MysqlDatabaseGrant{
				{Username: "acme_app", Privileges: []v1alpha1.MysqlPrivilege{v1alpha1.PrivilegeSelect}},
			}
		})
		r, _, rec := newMdbReconciler(t, mdb, sibling, mdbTestGroup())

		_, dropOwners, err := r.deleteScope(context.Background(), mdb, map[string]bool{})
		if err != nil {
			t.Fatalf("deleteScope() error = %v", err)
		}
		if len(dropOwners) != 0 {
			t.Fatalf("dropOwners = %v, want none — a sibling still grants acme_app", dropOwners)
		}
		assertEventContains(t, rec, "OwnerUserDropSkipped")
	})

	t.Run("sibling users ledger claim survives deletion", func(t *testing.T) {
		mdb := deleting(func(m *v1alpha1.MysqlDatabase) {
			m.Status.OwnerUser = "acme_app"
			m.Status.AppliedUsers = []v1alpha1.MysqlDatabaseUserState{
				{SecretName: "acme-support", Username: "acme_support"},
			}
		})
		// The sibling's own ledger claims the same account — a shared
		// support credential. Deleting this CR must leave it in MySQL.
		sibling := mdbTestCR(func(m *v1alpha1.MysqlDatabase) {
			m.Name = "tenant-sibling"
			m.Spec.DatabaseName = "sibling_wms"
			m.Spec.Owner.SecretName = "sibling-owner"
			m.Status.AppliedUsers = []v1alpha1.MysqlDatabaseUserState{
				{SecretName: "sibling-support", Username: "acme_support"},
			}
		})
		r, _, rec := newMdbReconciler(t, mdb, sibling, mdbTestGroup())

		_, dropOwners, err := r.deleteScope(context.Background(), mdb, map[string]bool{})
		if err != nil {
			t.Fatalf("deleteScope() error = %v", err)
		}
		for _, got := range dropOwners {
			if got == "acme_support" {
				t.Fatalf("dropOwners = %v, want no acme_support — a sibling's ledger still claims it", dropOwners)
			}
		}
		assertEventContains(t, rec, "UserDropSkipped")
	})

	t.Run("crashed rotation cleans up both usernames", func(t *testing.T) {
		mdb := deleting(func(m *v1alpha1.MysqlDatabase) {
			m.Status.OwnerUser = "old_app" // rotation crashed before status caught up
		})
		// The owner Secret names the new account — deleteScope must pick it
		// up or the privileged user leaks.
		r, _, _ := newMdbReconciler(t, mdb, mdbTestGroup(), mdbTestOwnerSecret())

		_, dropOwners, err := r.deleteScope(context.Background(), mdb, map[string]bool{})
		if err != nil {
			t.Fatalf("deleteScope() error = %v", err)
		}
		want := []string{"old_app", "acme_app"}
		if len(dropOwners) != len(want) {
			t.Fatalf("dropOwners = %v, want %v", dropOwners, want)
		}
		for i := range want {
			if dropOwners[i] != want[i] {
				t.Fatalf("dropOwners = %v, want %v", dropOwners, want)
			}
		}
	})

	t.Run("pending rotation record is cleaned up even without a secret", func(t *testing.T) {
		mdb := deleting(func(m *v1alpha1.MysqlDatabase) {
			m.Status.OwnerUser = "old_app"
			m.Status.PendingOwnerUser = "new_app" // rotation crashed after creating it
		})
		// No owner Secret in the fixtures: the pending record alone must
		// still put the new account on the drop list.
		r, _, _ := newMdbReconciler(t, mdb, mdbTestGroup())

		_, dropOwners, err := r.deleteScope(context.Background(), mdb, map[string]bool{})
		if err != nil {
			t.Fatalf("deleteScope() error = %v", err)
		}
		want := []string{"old_app", "new_app"}
		if len(dropOwners) != len(want) {
			t.Fatalf("dropOwners = %v, want %v", dropOwners, want)
		}
		for i := range want {
			if dropOwners[i] != want[i] {
				t.Fatalf("dropOwners = %v, want %v", dropOwners, want)
			}
		}
	})
}

type timeoutErr struct{}

func (timeoutErr) Error() string   { return "i/o timeout" }
func (timeoutErr) Timeout() bool   { return true }
func (timeoutErr) Temporary() bool { return true }
