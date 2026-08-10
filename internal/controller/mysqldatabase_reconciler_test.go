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
		WithIndex(&v1alpha1.MysqlDatabase{}, mdbOwnerSecretIndex, func(o client.Object) []string {
			return []string{o.(*v1alpha1.MysqlDatabase).Spec.Owner.SecretName}
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
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			r, c, _ := newMdbReconciler(t, tc.objects()...)

			res, err := r.Reconcile(context.Background(), mdbRequest())
			if err != nil {
				t.Fatalf("Reconcile() error = %v, want nil (Pending is not an error)", err)
			}
			if res.RequeueAfter != mysqlDatabasePendingRequeue {
				t.Fatalf("Reconcile() RequeueAfter = %v, want %v", res.RequeueAfter, mysqlDatabasePendingRequeue)
			}

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
	secret := mdbTestOwnerSecret()
	mdb := mdbTestCR()

	hash, err := computeDatabaseHash(mdb, secret, "dc1")
	if err != nil {
		t.Fatalf("computeDatabaseHash() error = %v", err)
	}
	mdb.Status = v1alpha1.MysqlDatabaseStatus{
		Phase:              v1alpha1.MysqlDatabasePhaseReady,
		ObservedGeneration: mdb.Generation,
		DatabaseCreated:    true,
		OwnerUser:          "acme_app",
		ActiveSite:         "dc1",
		LastAppliedHash:    hash,
	}

	r, _, _ := newMdbReconciler(t, mdb, mdbTestGroup(), secret)

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
	secret := mdbTestOwnerSecret()
	mdb := mdbTestCR()

	staleHash, err := computeDatabaseHash(mdb, secret, "dc1")
	if err != nil {
		t.Fatalf("computeDatabaseHash() error = %v", err)
	}
	mdb.Status = v1alpha1.MysqlDatabaseStatus{
		Phase:              v1alpha1.MysqlDatabasePhaseReady,
		ObservedGeneration: mdb.Generation,
		ActiveSite:         "dc1",
		LastAppliedHash:    staleHash,
	}

	fg := mdbTestGroup()
	fg.Status.ActiveSite = "dc2" // failover happened

	scheme := testScheme()
	c := fake.NewClientBuilder().
		WithScheme(scheme).
		WithStatusSubresource(&v1alpha1.MysqlDatabase{}, &v1alpha1.MysqlFailoverGroup{}).
		WithObjects(mdb, fg, secret, mdbTestOperatorSecret()).
		Build()

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
	if res.RequeueAfter != mysqlDatabasePendingRequeue {
		t.Fatalf("RequeueAfter = %v, want %v", res.RequeueAfter, mysqlDatabasePendingRequeue)
	}

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
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := transientSQLError(tc.err); got != tc.want {
				t.Fatalf("transientSQLError(%v) = %v, want %v", tc.err, got, tc.want)
			}
		})
	}
}

type timeoutErr struct{}

func (timeoutErr) Error() string   { return "i/o timeout" }
func (timeoutErr) Timeout() bool   { return true }
func (timeoutErr) Temporary() bool { return true }
