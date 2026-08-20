//go:build envtest

package envtest

import (
	"database/sql"
	"fmt"
	"strings"
	"testing"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/tools/record"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"

	v1alpha1 "github.com/shipstream/bloodraven/api/v1alpha1"
	"github.com/shipstream/bloodraven/internal/controller"
)

// These tests exercise the MysqlDatabase CRD against a real API server: the
// structural schema (patterns, enums), the CEL rules, the defaults, the
// status subresource, and the finalizer lifecycle. Nothing here reaches
// MySQL — the reconciler's dialer fails the test if it is called, because
// every case below must reach its verdict without a connection.

func newMysqlDatabaseCR(namespace, name string) *v1alpha1.MysqlDatabase {
	return &v1alpha1.MysqlDatabase{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: namespace},
		Spec: v1alpha1.MysqlDatabaseSpec{
			GroupRef:     v1alpha1.LocalGroupRef{Name: "main"},
			DatabaseName: "acme_wms",
			Owner:        v1alpha1.MysqlDatabaseOwner{SecretName: "acme-mysql-owner"},
		},
	}
}

func newMysqlDatabaseReconciler(t *testing.T) *controller.MysqlDatabaseReconciler {
	t.Helper()
	return &controller.MysqlDatabaseReconciler{
		Client:   k8sClient,
		Scheme:   scheme,
		Recorder: record.NewFakeRecorder(50),
		OpenDB: func(user, password, addr, tlsConfigName string) (*sql.DB, error) {
			t.Fatalf("reconciler dialed MySQL (user=%q addr=%q) when it should not have", user, addr)
			return nil, nil
		},
	}
}

// deleteAndDrainFinalizer deletes the CR and runs one reconcile so the
// deletion path releases the finalizer. Without the drain, any test whose
// reconciler added the finalizer would leave the object stuck with a
// deletionTimestamp and its namespace forever terminating.
func deleteAndDrainFinalizer(t *testing.T, r *controller.MysqlDatabaseReconciler, nn types.NamespacedName) {
	t.Helper()
	var cr v1alpha1.MysqlDatabase
	if err := k8sClient.Get(ctx, nn, &cr); err != nil {
		return
	}
	_ = k8sClient.Delete(ctx, &cr)
	_, _ = r.Reconcile(ctx, ctrl.Request{NamespacedName: nn})
}

func mysqlDatabaseReadyCondition(mdb *v1alpha1.MysqlDatabase) *metav1.Condition {
	for i := range mdb.Status.Conditions {
		if mdb.Status.Conditions[i].Type == "Ready" {
			return &mdb.Status.Conditions[i]
		}
	}
	return nil
}

func TestMysqlDatabase_EnvtestDefaults(t *testing.T) {
	ns := createNamespace(t, "mydb-defaults")
	cr := newMysqlDatabaseCR(ns, "tenant-acme")

	if err := k8sClient.Create(ctx, cr); err != nil {
		t.Fatalf("create MysqlDatabase: %v", err)
	}
	t.Cleanup(func() { _ = k8sClient.Delete(ctx, cr) })

	var got v1alpha1.MysqlDatabase
	if err := k8sClient.Get(ctx, types.NamespacedName{Namespace: ns, Name: "tenant-acme"}, &got); err != nil {
		t.Fatalf("get MysqlDatabase: %v", err)
	}

	if got.Spec.CharacterSet != "utf8mb4" {
		t.Errorf("spec.characterSet = %q, want utf8mb4", got.Spec.CharacterSet)
	}
	if got.Spec.Collation != "utf8mb4_unicode_ci" {
		t.Errorf("spec.collation = %q, want utf8mb4_unicode_ci", got.Spec.Collation)
	}
	// The default that matters. Dropping a tenant database because a CR was
	// garbage-collected is an unrecoverable data-loss incident, so the API
	// server must default this to Retain without the caller saying so.
	if got.Spec.DeletionPolicy != v1alpha1.MysqlDatabaseRetain {
		t.Errorf("spec.deletionPolicy = %q, want Retain", got.Spec.DeletionPolicy)
	}
	if len(got.Spec.Owner.Privileges) != 1 || got.Spec.Owner.Privileges[0] != v1alpha1.PrivilegeAllPrivileges {
		t.Errorf("spec.owner.privileges = %v, want [ALL PRIVILEGES]", got.Spec.Owner.Privileges)
	}
}

func TestMysqlDatabase_EnvtestSchemaRejections(t *testing.T) {
	ns := createNamespace(t, "mydb-reject")

	cases := []struct {
		name   string
		mutate func(*v1alpha1.MysqlDatabase)
	}{
		{
			name:   "database name with a backtick",
			mutate: func(m *v1alpha1.MysqlDatabase) { m.Spec.DatabaseName = "acme`wms" },
		},
		{
			name:   "database name with a quote and trailing SQL",
			mutate: func(m *v1alpha1.MysqlDatabase) { m.Spec.DatabaseName = "acme'; DROP DATABASE other; --" },
		},
		{
			name:   "database name with a hyphen",
			mutate: func(m *v1alpha1.MysqlDatabase) { m.Spec.DatabaseName = "acme-wms" },
		},
		{
			name:   "empty database name",
			mutate: func(m *v1alpha1.MysqlDatabase) { m.Spec.DatabaseName = "" },
		},
		{
			name:   "database name over 64 characters",
			mutate: func(m *v1alpha1.MysqlDatabase) { m.Spec.DatabaseName = strings.Repeat("a", 65) },
		},
		{
			name:   "character set with injected DDL",
			mutate: func(m *v1alpha1.MysqlDatabase) { m.Spec.CharacterSet = "utf8mb4 DEFAULT ENCRYPTION='N'" },
		},
		{
			name:   "collation with injected DDL",
			mutate: func(m *v1alpha1.MysqlDatabase) { m.Spec.Collation = "x; DROP DATABASE other" },
		},
		{
			name:   "owner privilege outside the allowlist",
			mutate: func(m *v1alpha1.MysqlDatabase) { m.Spec.Owner.Privileges = []v1alpha1.MysqlPrivilege{"SUPER"} },
		},
		{
			// The privilege that must never be expressible through this CRD.
			name:   "owner GRANT OPTION",
			mutate: func(m *v1alpha1.MysqlDatabase) { m.Spec.Owner.Privileges = []v1alpha1.MysqlPrivilege{"GRANT OPTION"} },
		},
		{
			name: "owner ALL PRIVILEGES combined with another privilege",
			mutate: func(m *v1alpha1.MysqlDatabase) {
				m.Spec.Owner.Privileges = []v1alpha1.MysqlPrivilege{
					v1alpha1.PrivilegeAllPrivileges, v1alpha1.PrivilegeSelect,
				}
			},
		},
		{
			name: "grant privilege outside the allowlist",
			mutate: func(m *v1alpha1.MysqlDatabase) {
				m.Spec.Grants = []v1alpha1.MysqlDatabaseGrant{
					{Username: "maester", Privileges: []v1alpha1.MysqlPrivilege{"FILE"}},
				}
			},
		},
		{
			name: "grant username with a quote",
			mutate: func(m *v1alpha1.MysqlDatabase) {
				m.Spec.Grants = []v1alpha1.MysqlDatabaseGrant{
					{Username: "maester'@'%", Privileges: []v1alpha1.MysqlPrivilege{v1alpha1.PrivilegeSelect}},
				}
			},
		},
		{
			name: "grant with no privileges",
			mutate: func(m *v1alpha1.MysqlDatabase) {
				m.Spec.Grants = []v1alpha1.MysqlDatabaseGrant{{Username: "maester"}}
			},
		},
		{
			name: "duplicate grant usernames",
			mutate: func(m *v1alpha1.MysqlDatabase) {
				m.Spec.Grants = []v1alpha1.MysqlDatabaseGrant{
					{Username: "maester", Privileges: []v1alpha1.MysqlPrivilege{v1alpha1.PrivilegeSelect}},
					{Username: "maester", Privileges: []v1alpha1.MysqlPrivilege{v1alpha1.PrivilegeDelete}},
				}
			},
		},
		{
			name:   "unknown deletion policy",
			mutate: func(m *v1alpha1.MysqlDatabase) { m.Spec.DeletionPolicy = "Destroy" },
		},
		{
			// users[] must never express an all-privileges principal; that
			// shape is the owner's.
			name: "users ALL PRIVILEGES",
			mutate: func(m *v1alpha1.MysqlDatabase) {
				m.Spec.Users = []v1alpha1.MysqlDatabaseUser{
					{SecretName: "support-ro-mysql", Privileges: []v1alpha1.MysqlPrivilege{v1alpha1.PrivilegeAllPrivileges}},
				}
			},
		},
		{
			name: "users GRANT OPTION",
			mutate: func(m *v1alpha1.MysqlDatabase) {
				m.Spec.Users = []v1alpha1.MysqlDatabaseUser{
					{SecretName: "support-ro-mysql", Privileges: []v1alpha1.MysqlPrivilege{"GRANT OPTION"}},
				}
			},
		},
		{
			name: "users entry with no privileges",
			mutate: func(m *v1alpha1.MysqlDatabase) {
				m.Spec.Users = []v1alpha1.MysqlDatabaseUser{{SecretName: "support-ro-mysql"}}
			},
		},
		{
			name: "users entry reusing the owner secret",
			mutate: func(m *v1alpha1.MysqlDatabase) {
				m.Spec.Users = []v1alpha1.MysqlDatabaseUser{
					{SecretName: m.Spec.Owner.SecretName, Privileges: []v1alpha1.MysqlPrivilege{v1alpha1.PrivilegeSelect}},
				}
			},
		},
		{
			name: "duplicate users secret names",
			mutate: func(m *v1alpha1.MysqlDatabase) {
				m.Spec.Users = []v1alpha1.MysqlDatabaseUser{
					{SecretName: "support-ro-mysql", Privileges: []v1alpha1.MysqlPrivilege{v1alpha1.PrivilegeSelect}},
					{SecretName: "support-ro-mysql", Privileges: []v1alpha1.MysqlPrivilege{v1alpha1.PrivilegeDelete}},
				}
			},
		},
		{
			name: "negative users resource limit",
			mutate: func(m *v1alpha1.MysqlDatabase) {
				m.Spec.Users = []v1alpha1.MysqlDatabaseUser{
					{
						SecretName:     "support-ro-mysql",
						Privileges:     []v1alpha1.MysqlPrivilege{v1alpha1.PrivilegeSelect},
						ResourceLimits: &v1alpha1.MysqlUserResourceLimits{MaxUserConnections: -1},
					},
				}
			},
		},
		{
			name:   "empty owner secret name",
			mutate: func(m *v1alpha1.MysqlDatabase) { m.Spec.Owner.SecretName = "" },
		},
		{
			name:   "empty group ref",
			mutate: func(m *v1alpha1.MysqlDatabase) { m.Spec.GroupRef.Name = "" },
		},
		{
			name:   "system schema mysql",
			mutate: func(m *v1alpha1.MysqlDatabase) { m.Spec.DatabaseName = "mysql" },
		},
		{
			// Case-insensitive: the denylist must not be bypassable by casing.
			name:   "system schema SYS in uppercase",
			mutate: func(m *v1alpha1.MysqlDatabase) { m.Spec.DatabaseName = "SYS" },
		},
		{
			name:   "system schema information_schema",
			mutate: func(m *v1alpha1.MysqlDatabase) { m.Spec.DatabaseName = "information_schema" },
		},
		{
			name:   "system schema performance_schema",
			mutate: func(m *v1alpha1.MysqlDatabase) { m.Spec.DatabaseName = "performance_schema" },
		},
	}

	for i, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			cr := newMysqlDatabaseCR(ns, fmt.Sprintf("reject-%d", i))
			tc.mutate(cr)
			if err := k8sClient.Create(ctx, cr); err == nil {
				_ = k8sClient.Delete(ctx, cr)
				t.Fatal("API server accepted the CR; it must be rejected by the schema")
			}
		})
	}
}

// TestMysqlDatabase_EnvtestDatabaseNameIsImmutable pins the XValidation
// transition rule: MySQL has no schema rename, so an edited databaseName
// would CREATE a second database and orphan the first — the API server must
// refuse the edit outright.
func TestMysqlDatabase_EnvtestDatabaseNameIsImmutable(t *testing.T) {
	ns := createNamespace(t, "mydb-immutable")

	cr := newMysqlDatabaseCR(ns, "tenant-immutable")
	if err := k8sClient.Create(ctx, cr); err != nil {
		t.Fatalf("create: %v", err)
	}
	t.Cleanup(func() { _ = k8sClient.Delete(ctx, cr) })

	cr.Spec.DatabaseName = "renamed_wms"
	if err := k8sClient.Update(ctx, cr); err == nil {
		t.Fatal("API server accepted a databaseName change; the field must be immutable")
	}

	// Any other spec edit still goes through.
	var fresh v1alpha1.MysqlDatabase
	if err := k8sClient.Get(ctx, client.ObjectKeyFromObject(cr), &fresh); err != nil {
		t.Fatalf("get: %v", err)
	}
	fresh.Spec.Grants = []v1alpha1.MysqlDatabaseGrant{
		{Username: "maester", Privileges: []v1alpha1.MysqlPrivilege{v1alpha1.PrivilegeSelect}},
	}
	if err := k8sClient.Update(ctx, &fresh); err != nil {
		t.Fatalf("an unrelated spec edit was rejected: %v", err)
	}
}

// TestMysqlDatabase_EnvtestExplicitEmptyOwnerPrivilegesRejected covers the
// case the typed client cannot express: an explicit empty privileges list.
// The Go struct serializes it away via omitempty, so the raw object is
// created unstructured — exactly what kubectl apply with `privileges: []`
// would send. Absent is the only way to ask for the ALL PRIVILEGES
// default; an explicit [] must be rejected like grants[].privileges.
func TestMysqlDatabase_EnvtestExplicitEmptyOwnerPrivilegesRejected(t *testing.T) {
	ns := createNamespace(t, "mydb-empty-privs")

	u := &unstructured.Unstructured{Object: map[string]any{
		"apiVersion": "shipstream.io/v1alpha1",
		"kind":       "MysqlDatabase",
		"metadata":   map[string]any{"name": "tenant-empty", "namespace": ns},
		"spec": map[string]any{
			"groupRef":     map[string]any{"name": "main"},
			"databaseName": "acme_wms",
			"owner": map[string]any{
				"secretName": "acme-mysql-owner",
				"privileges": []any{},
			},
		},
	}}

	if err := k8sClient.Create(ctx, u); err == nil {
		_ = k8sClient.Delete(ctx, u)
		t.Fatal("API server accepted an explicit empty owner.privileges; MinItems=1 must reject it")
	}
}

// TestMysqlDatabase_EnvtestGroupRefIsImmutable pins the groupRef transition
// rule: the reconciler cannot move a schema between groups, so retargeting
// would orphan the database on the old group and aim any later cleanup at
// the wrong MySQL. The API server must refuse the edit outright.
func TestMysqlDatabase_EnvtestGroupRefIsImmutable(t *testing.T) {
	ns := createNamespace(t, "mydb-group-immutable")

	cr := newMysqlDatabaseCR(ns, "tenant-group")
	if err := k8sClient.Create(ctx, cr); err != nil {
		t.Fatalf("create: %v", err)
	}
	t.Cleanup(func() { _ = k8sClient.Delete(ctx, cr) })

	cr.Spec.GroupRef.Name = "other-group"
	if err := k8sClient.Update(ctx, cr); err == nil {
		t.Fatal("API server accepted a groupRef change; the field must be immutable")
	}
}

func TestMysqlDatabase_EnvtestAcceptsFullAllowlist(t *testing.T) {
	ns := createNamespace(t, "mydb-allowlist")

	privs := make([]v1alpha1.MysqlPrivilege, 0)
	for _, name := range v1alpha1.AllowedPrivilegeNames() {
		if name == string(v1alpha1.PrivilegeAllPrivileges) {
			continue // cannot be combined; covered separately
		}
		privs = append(privs, v1alpha1.MysqlPrivilege(name))
	}

	cr := newMysqlDatabaseCR(ns, "tenant-allowlist")
	cr.Spec.Owner.Privileges = privs
	cr.Spec.Grants = []v1alpha1.MysqlDatabaseGrant{
		{Username: "maester", Privileges: []v1alpha1.MysqlPrivilege{v1alpha1.PrivilegeSelect, v1alpha1.PrivilegeDelete}},
		{Username: "reporting", Privileges: []v1alpha1.MysqlPrivilege{v1alpha1.PrivilegeAllPrivileges}},
	}

	if err := k8sClient.Create(ctx, cr); err != nil {
		t.Fatalf("API server rejected a CR using the full allowlist: %v", err)
	}
	t.Cleanup(func() { _ = k8sClient.Delete(ctx, cr) })
}

// TestMysqlDatabase_EnvtestPendingWithoutGroup is the ordering case: a
// MysqlDatabase applied before its MysqlFailoverGroup must go Pending with a
// Ready=False condition, not Failed and not an error.
func TestMysqlDatabase_EnvtestPendingWithoutGroup(t *testing.T) {
	ns := createNamespace(t, "mydb-pending")
	cr := newMysqlDatabaseCR(ns, "tenant-acme")
	if err := k8sClient.Create(ctx, cr); err != nil {
		t.Fatalf("create MysqlDatabase: %v", err)
	}

	r := newMysqlDatabaseReconciler(t)
	nn := types.NamespacedName{Namespace: ns, Name: "tenant-acme"}
	t.Cleanup(func() { deleteAndDrainFinalizer(t, r, nn) })

	// First reconcile adds the finalizer and requeues.
	if _, err := r.Reconcile(ctx, ctrl.Request{NamespacedName: nn}); err != nil {
		t.Fatalf("reconcile (finalizer): %v", err)
	}
	var afterFinalizer v1alpha1.MysqlDatabase
	if err := k8sClient.Get(ctx, nn, &afterFinalizer); err != nil {
		t.Fatalf("get after finalizer reconcile: %v", err)
	}
	if !controllerutil.ContainsFinalizer(&afterFinalizer, controller.MysqlDatabaseFinalizer) {
		t.Fatal("finalizer was not added")
	}

	res, err := r.Reconcile(ctx, ctrl.Request{NamespacedName: nn})
	if err != nil {
		t.Fatalf("reconcile: %v", err)
	}
	if res.RequeueAfter == 0 {
		t.Error("want a non-zero requeue while the group is absent")
	}

	var got v1alpha1.MysqlDatabase
	if err := k8sClient.Get(ctx, nn, &got); err != nil {
		t.Fatalf("get after reconcile: %v", err)
	}
	if got.Status.Phase != v1alpha1.MysqlDatabasePhasePending {
		t.Errorf("status.phase = %q, want Pending", got.Status.Phase)
	}
	if got.Status.ObservedGeneration != got.Generation {
		t.Errorf("status.observedGeneration = %d, want %d", got.Status.ObservedGeneration, got.Generation)
	}
	cond := mysqlDatabaseReadyCondition(&got)
	if cond == nil {
		t.Fatal("Ready condition missing; it is part of the polling contract")
	}
	if cond.Status != metav1.ConditionFalse || cond.Reason != "GroupNotFound" {
		t.Errorf("Ready = %s/%s, want False/GroupNotFound", cond.Status, cond.Reason)
	}
	if cond.ObservedGeneration != got.Generation {
		t.Errorf("Ready condition observedGeneration = %d, want %d", cond.ObservedGeneration, got.Generation)
	}
}

// TestMysqlDatabase_EnvtestStatusIsASubresource proves a caller cannot forge
// status through the main resource endpoint — the reason a caller Role that
// grants update on mysqldatabases still cannot claim "Ready".
func TestMysqlDatabase_EnvtestStatusIsASubresource(t *testing.T) {
	ns := createNamespace(t, "mydb-status")
	cr := newMysqlDatabaseCR(ns, "tenant-acme")
	if err := k8sClient.Create(ctx, cr); err != nil {
		t.Fatalf("create MysqlDatabase: %v", err)
	}
	t.Cleanup(func() { _ = k8sClient.Delete(ctx, cr) })

	nn := types.NamespacedName{Namespace: ns, Name: "tenant-acme"}
	var forged v1alpha1.MysqlDatabase
	if err := k8sClient.Get(ctx, nn, &forged); err != nil {
		t.Fatalf("get: %v", err)
	}
	forged.Status.Phase = v1alpha1.MysqlDatabasePhaseReady
	forged.Status.OwnerUser = "impostor"
	if err := k8sClient.Update(ctx, &forged); err != nil {
		t.Fatalf("update spec-side: %v", err)
	}

	var after v1alpha1.MysqlDatabase
	if err := k8sClient.Get(ctx, nn, &after); err != nil {
		t.Fatalf("get after update: %v", err)
	}
	if after.Status.Phase == v1alpha1.MysqlDatabasePhaseReady || after.Status.OwnerUser == "impostor" {
		t.Fatalf("status was writable through the main resource: %+v", after.Status)
	}

	// The status subresource does accept it.
	after.Status.Phase = v1alpha1.MysqlDatabasePhaseReady
	after.Status.OwnerUser = "acme_app"
	if err := k8sClient.Status().Update(ctx, &after); err != nil {
		t.Fatalf("status subresource update: %v", err)
	}
	var final v1alpha1.MysqlDatabase
	if err := k8sClient.Get(ctx, nn, &final); err != nil {
		t.Fatalf("get after status update: %v", err)
	}
	if final.Status.Phase != v1alpha1.MysqlDatabasePhaseReady {
		t.Fatalf("status.phase = %q after a subresource write, want Ready", final.Status.Phase)
	}
}

// TestMysqlDatabase_EnvtestRetainDeletionReleasesFinalizer covers the default
// deletion policy end to end against a real API server: the CR goes away and
// MySQL is never contacted (the dialer would fail the test).
func TestMysqlDatabase_EnvtestRetainDeletionReleasesFinalizer(t *testing.T) {
	ns := createNamespace(t, "mydb-retain")
	cr := newMysqlDatabaseCR(ns, "tenant-acme")
	if err := k8sClient.Create(ctx, cr); err != nil {
		t.Fatalf("create MysqlDatabase: %v", err)
	}

	r := newMysqlDatabaseReconciler(t)
	nn := types.NamespacedName{Namespace: ns, Name: "tenant-acme"}
	if _, err := r.Reconcile(ctx, ctrl.Request{NamespacedName: nn}); err != nil {
		t.Fatalf("reconcile (finalizer): %v", err)
	}

	var live v1alpha1.MysqlDatabase
	if err := k8sClient.Get(ctx, nn, &live); err != nil {
		t.Fatalf("get: %v", err)
	}
	if err := k8sClient.Delete(ctx, &live); err != nil {
		t.Fatalf("delete: %v", err)
	}

	// The finalizer holds the object; the API server has stamped
	// deletionTimestamp.
	var deleting v1alpha1.MysqlDatabase
	if err := k8sClient.Get(ctx, nn, &deleting); err != nil {
		t.Fatalf("get during deletion: %v", err)
	}
	if deleting.DeletionTimestamp.IsZero() {
		t.Fatal("deletionTimestamp not set; the finalizer is not holding the object")
	}

	if _, err := r.Reconcile(ctx, ctrl.Request{NamespacedName: nn}); err != nil {
		t.Fatalf("reconcile (deletion): %v", err)
	}

	var gone v1alpha1.MysqlDatabase
	if err := k8sClient.Get(ctx, nn, &gone); err == nil {
		t.Fatalf("CR still present after Retain deletion: finalizers=%v", gone.Finalizers)
	}
}

// TestMysqlDatabase_EnvtestDeletePolicyIsOptIn asserts that switching to
// Delete is an explicit, accepted spec change — and that it is the only way
// to express it.
func TestMysqlDatabase_EnvtestDeletePolicyIsOptIn(t *testing.T) {
	ns := createNamespace(t, "mydb-delete-optin")
	cr := newMysqlDatabaseCR(ns, "tenant-acme")
	cr.Spec.DeletionPolicy = v1alpha1.MysqlDatabaseDelete
	if err := k8sClient.Create(ctx, cr); err != nil {
		t.Fatalf("create with deletionPolicy=Delete: %v", err)
	}
	t.Cleanup(func() { _ = k8sClient.Delete(ctx, cr) })

	var got v1alpha1.MysqlDatabase
	if err := k8sClient.Get(ctx, types.NamespacedName{Namespace: ns, Name: "tenant-acme"}, &got); err != nil {
		t.Fatalf("get: %v", err)
	}
	if got.Spec.DeletionPolicy != v1alpha1.MysqlDatabaseDelete {
		t.Fatalf("spec.deletionPolicy = %q, want Delete", got.Spec.DeletionPolicy)
	}

	// A near-miss value is rejected outright rather than silently coerced.
	bad := newMysqlDatabaseCR(ns, "tenant-nearmiss")
	bad.Spec.DeletionPolicy = "delete"
	if err := k8sClient.Create(ctx, bad); err == nil {
		_ = k8sClient.Delete(ctx, bad)
		t.Fatal("API server accepted deletionPolicy=delete; the enum must reject it")
	}
}

// TestMysqlDatabase_EnvtestOwnerSecretIsNeverEchoed keeps the no-credential
// -in-status rule honest at the API boundary. The group exists and names an
// active site that does not resolve to a promotable site, so the reconciler
// reads the owner Secret, validates it, stamps Creating and then fails the
// admin connection *before* dialing — every status-writing path that could
// echo Secret bytes executes, while the no-dial invariant still holds.
// Without the group, reconciliation would stop at Pending(GroupNotFound)
// before ever reading the Secret, and the test would prove nothing.
func TestMysqlDatabase_EnvtestOwnerSecretIsNeverEchoed(t *testing.T) {
	ns := createNamespace(t, "mydb-nosecret")

	const password = "correct-horse-battery-staple"
	secret := &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{Name: "acme-mysql-owner", Namespace: ns},
		Data:       map[string][]byte{"username": []byte("acme_app"), "password": []byte(password)},
	}
	if err := k8sClient.Create(ctx, secret); err != nil {
		t.Fatalf("create owner secret: %v", err)
	}
	operator := &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{Name: "mysql-operator", Namespace: ns},
		Data: map[string][]byte{
			"username":            []byte("operator"),
			"password":            []byte("op-pw"),
			"MYSQL_ROOT_PASSWORD": []byte("root-pw"),
		},
	}
	if err := k8sClient.Create(ctx, operator); err != nil {
		t.Fatalf("create operator secret: %v", err)
	}

	fg := &v1alpha1.MysqlFailoverGroup{
		ObjectMeta: metav1.ObjectMeta{Name: "main", Namespace: ns},
		Spec: v1alpha1.MysqlFailoverGroupSpec{
			Image:       "mysql:9.7",
			Credentials: &v1alpha1.CredentialsSpec{OperatorSecret: "mysql-operator"},
			Sites: []v1alpha1.SiteSpec{
				{Name: "dc1", Zone: "main-dc1", LBIP: "203.0.113.1",
					TaintNodeSelector: map[string]string{"shipstream.io/site.main": "dc1"},
					Storage:           v1alpha1.StorageSpec{StorageClassName: "standard", Size: resource.MustParse("10Gi")}},
				{Name: "dc2", Zone: "main-dc2", LBIP: "203.0.113.2",
					TaintNodeSelector: map[string]string{"shipstream.io/site.main": "dc2"},
					Storage:           v1alpha1.StorageSpec{StorageClassName: "standard", Size: resource.MustParse("10Gi")}},
			},
			DNS: v1alpha1.DNSSpec{Hostname: "main.az.example.com", TTL: 60},
		},
	}
	if err := k8sClient.Create(ctx, fg); err != nil {
		t.Fatalf("create group: %v", err)
	}
	fg.Status.ActiveSite = "nowhere" // resolves to no promotable site: no dial
	if err := k8sClient.Status().Update(ctx, fg); err != nil {
		t.Fatalf("update group status: %v", err)
	}

	cr := newMysqlDatabaseCR(ns, "tenant-acme")
	if err := k8sClient.Create(ctx, cr); err != nil {
		t.Fatalf("create MysqlDatabase: %v", err)
	}

	r := newMysqlDatabaseReconciler(t)
	nn := types.NamespacedName{Namespace: ns, Name: "tenant-acme"}
	t.Cleanup(func() { deleteAndDrainFinalizer(t, r, nn) })
	for range 2 {
		if _, err := r.Reconcile(ctx, ctrl.Request{NamespacedName: nn}); err != nil {
			t.Fatalf("reconcile: %v", err)
		}
	}

	var got v1alpha1.MysqlDatabase
	if err := k8sClient.Get(ctx, nn, &got); err != nil {
		t.Fatalf("get: %v", err)
	}
	if got.Status.Phase != v1alpha1.MysqlDatabasePhasePending {
		t.Fatalf("phase = %q, want Pending for an unreachable primary", got.Status.Phase)
	}
	// The write-ahead record is stamped only once a connection is open: a
	// CR that never connected records no owner and creates no authority to
	// drop same-named objects later.
	if got.Status.OwnerUser != "" {
		t.Fatalf("status.ownerUser = %q without a connection, want empty", got.Status.OwnerUser)
	}
	if got.Status.DatabaseCreated {
		t.Fatal("status.databaseCreated stamped without a connection")
	}
	rendered := fmt.Sprintf("%+v", got.Status)
	if strings.Contains(rendered, password) {
		t.Fatalf("status leaked the owner password: %s", rendered)
	}
	if strings.Contains(rendered, "op-pw") || strings.Contains(rendered, "root-pw") {
		t.Fatalf("status leaked a group credential: %s", rendered)
	}

	// Proof the owner Secret really is read on this path: removing its
	// password key changes the verdict to OwnerSecretIncomplete.
	var s corev1.Secret
	if err := k8sClient.Get(ctx, types.NamespacedName{Namespace: ns, Name: "acme-mysql-owner"}, &s); err != nil {
		t.Fatalf("get owner secret: %v", err)
	}
	delete(s.Data, "password")
	if err := k8sClient.Update(ctx, &s); err != nil {
		t.Fatalf("update owner secret: %v", err)
	}
	if _, err := r.Reconcile(ctx, ctrl.Request{NamespacedName: nn}); err != nil {
		t.Fatalf("reconcile after secret change: %v", err)
	}
	if err := k8sClient.Get(ctx, nn, &got); err != nil {
		t.Fatalf("get: %v", err)
	}
	if cond := mysqlDatabaseReadyCondition(&got); cond == nil || cond.Reason != "OwnerSecretIncomplete" {
		t.Fatalf("Ready condition = %+v, want OwnerSecretIncomplete — the Secret read is what must produce it", cond)
	}
	rendered = fmt.Sprintf("%+v", got.Status)
	if strings.Contains(rendered, password) {
		t.Fatalf("status leaked the owner password: %s", rendered)
	}
}
