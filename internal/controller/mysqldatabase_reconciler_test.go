package controller

import (
	"context"
	"database/sql"
	"strings"
	"testing"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/tools/record"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
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
			Finalizers: []string{mysqlDatabaseFinalizer},
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
				fg.Status.RestoreInPlace = &v1alpha1.RestoreInPlaceStatus{Phase: v1alpha1.RestoreInPlaceRestoring}
				return []client.Object{mdbTestCR(), fg, mdbTestOwnerSecret()}
			},
			wantReason: "RestoreInProgress",
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
			if err == nil && controllerutil.ContainsFinalizer(&out, mysqlDatabaseFinalizer) {
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
	})

	r, c, rec := newMdbReconciler(t, mdb)

	if _, err := r.Reconcile(context.Background(), mdbRequest()); err != nil {
		t.Fatalf("Reconcile() error = %v", err)
	}

	var out v1alpha1.MysqlDatabase
	err := c.Get(context.Background(), mdbRequest().NamespacedName, &out)
	if err == nil && controllerutil.ContainsFinalizer(&out, mysqlDatabaseFinalizer) {
		t.Fatal("finalizer still present; a vanished group must not wedge deletion")
	}
	assertEventContains(t, rec, "DatabaseCleanupSkipped")
}

// TestMysqlDatabaseDeleteDefersWithoutActiveSite proves the opposite side of
// the same coin: a requested DROP is deferred, never silently skipped.
func TestMysqlDatabaseDeleteDefersWithoutActiveSite(t *testing.T) {
	now := metav1.Now()
	mdb := mdbTestCR(func(m *v1alpha1.MysqlDatabase) {
		m.Spec.DeletionPolicy = v1alpha1.MysqlDatabaseDelete
		m.DeletionTimestamp = &now
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
	if !controllerutil.ContainsFinalizer(out, mysqlDatabaseFinalizer) {
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
	if !controllerutil.ContainsFinalizer(getMdb(t, c), mysqlDatabaseFinalizer) {
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
