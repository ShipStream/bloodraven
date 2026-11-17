//go:build envtest

package envtest

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	rbacv1 "k8s.io/api/rbac/v1"
	apiextensionsv1 "k8s.io/apiextensions-apiserver/pkg/apis/apiextensions/v1"
	"k8s.io/apimachinery/pkg/api/equality"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/validation/field"
	"k8s.io/client-go/util/retry"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/envtest"
	"sigs.k8s.io/yaml"

	v1beta1 "github.com/shipstream/bloodraven/api/v1beta1"
	"github.com/shipstream/bloodraven/internal/crdcompat"
	"github.com/shipstream/bloodraven/internal/storagemigration"
)

// v160Objects creates the objects of testdata/objects-v1.6.0 that names
// lists (file names without .yaml) under the v1.6.0 CRDs.
func v160Objects(e *upgradeEnv, ns string, names ...string) {
	e.t.Helper()
	for _, name := range names {
		obj := loadObject(e.t, filepath.Join("testdata", "objects-v1.6.0", name+".yaml"))
		e.createAt(obj, ns, v1beta1.DeprecatedGroupVersion.Version)
	}
}

func startupCheck(e *upgradeEnv) crdcompat.StartupReport {
	e.t.Helper()
	get := func(_ context.Context, name string) (*apiextensionsv1.CustomResourceDefinition, error) {
		crd := &apiextensionsv1.CustomResourceDefinition{}
		return crd, e.admin.Get(ctx, client.ObjectKey{Name: name}, crd)
	}
	report, err := crdcompat.CheckStartup(ctx, get, crdcompat.DynamicLister(e.dyn))
	if err != nil {
		e.t.Fatalf("startup check: %v", err)
	}
	return report
}

func findings(list []crdcompat.Finding) string {
	var out []string
	for _, f := range list {
		out = append(out, f.String())
	}
	return strings.Join(out, "\n")
}

func resourceVersions(objs map[string]*unstructured.Unstructured) map[string]string {
	out := map[string]string{}
	for key, o := range objs {
		out[key] = o.GetResourceVersion()
	}
	return out
}

// C-7, with U-10 and U-13: what happens when this build's CRDs are applied
// to a cluster that holds v1.6.0 objects, which the upgrade docs forbid
// (there is no in-place upgrade from v1.6.0).
//
// The API server takes the CRDs, and from then on it reads the old objects
// with their old field names pruned. An update of such an object ratchets,
// so nothing stops a rewrite that makes the pruning permanent, except the
// check migrate-storage and the operator make first. Both must name every
// object that does not fit and write nothing.
func TestMigrateStorage_RefusesV160Objects(t *testing.T) {
	e := startUpgradeEnv(t, "crd-v1.6.0")
	ns := e.namespace("from-v160")
	v160Objects(e, ns, "mysqlfailovergroup", "mysqldatabase", "mysqlbackup", "mysqlbackupverification", "mysqlstandbycluster")
	e.applyBuildCRDs()

	// U-13: the storage version is listed the moment the CRDs are applied,
	// before any object is written, and there is no way back to CRDs that
	// lack it.
	for name, versions := range e.storedVersions() {
		if strings.Join(versions, ",") != "v1alpha1,v1beta1" {
			t.Fatalf("%s storedVersions = %v right after the CRDs were applied, want [v1alpha1 v1beta1]", name, versions)
		}
	}
	old := loadCRDFile(t, filepath.Join("testdata", "crd-v1.6.0", "shipstream.io_mysqlfailovergroups.yaml"))
	var live apiextensionsv1.CustomResourceDefinition
	if err := e.admin.Get(ctx, types.NamespacedName{Name: old.Name}, &live); err != nil {
		t.Fatal(err)
	}
	live.Spec = old.Spec
	if err := e.admin.Update(ctx, &live); !apierrors.IsInvalid(err) || !strings.Contains(err.Error(), "storedVersions") {
		t.Fatalf("re-applying the v1.6.0 CRD = %v, want it refused because storedVersions lists v1beta1", err)
	}

	before := e.allObjects()
	if len(before) != 5 {
		t.Fatalf("%d objects are readable after the CRDs were applied, want the 5 that were created", len(before))
	}

	// The checker, object by object.
	check, err := crdcompat.CheckObjects(ctx, crdcompat.DynamicLister(e.dyn), installedCRDs(e))
	if err != nil {
		t.Fatal(err)
	}
	problems := map[string]crdcompat.ObjectProblem{}
	for _, p := range check.Problems {
		problems[p.Resource.Kind] = p
	}
	wantStructural := func(kind string, typ field.ErrorType, path string) {
		t.Helper()
		p, ok := problems[kind]
		if !ok {
			t.Fatalf("the v1.6.0 %s is not reported; problems: %v", kind, check.Problems)
		}
		for _, e := range p.Structural {
			if e.Type == typ && e.Field == path {
				return
			}
		}
		t.Fatalf("the v1.6.0 %s is not reported with %s at %s: %s", kind, typ, path, p)
	}
	// U-10: only a required field is missing (the old name was pruned).
	wantStructural("MysqlDatabase", field.ErrorTypeRequired, "spec.failoverGroupRef")
	if p := problems["MysqlDatabase"]; len(p.Structural) != 1 || len(p.Rules) != 0 {
		t.Fatalf("the v1.6.0 MysqlDatabase should have that one problem only: %s", p)
	}
	// U-11: a standby cluster, which needs no failover group.
	wantStructural("MysqlStandbyCluster", field.ErrorTypeRequired, "spec.source.s3")
	// U-10: a type change that no CEL rule looks at.
	wantStructural("MysqlFailoverGroup", field.ErrorTypeTypeInvalid, "spec.updateStrategy")
	wantStructural("MysqlFailoverGroup", field.ErrorTypeTypeInvalid, "spec.cloneTimeout")
	for _, kind := range []string{"MysqlBackup", "MysqlBackupVerification"} {
		if p, bad := problems[kind]; bad {
			t.Fatalf("the v1.6.0 %s kept its field names and should conform: %s", kind, p)
		}
	}

	// migrate-storage refuses, names the three, and writes nothing.
	for _, dryRun := range []bool{true, false} {
		out, err := e.migrate(e.cfg, dryRun)
		var refusal *storagemigration.Refusal
		if !errors.As(err, &refusal) {
			t.Fatalf("migrate-storage (dry run %v) = %v, want a refusal", dryRun, err)
		}
		for _, want := range []string{
			"3 of 5 object(s) do not conform",
			"MysqlFailoverGroup " + ns + "/orders",
			"MysqlDatabase " + ns + "/tenant-acme",
			"MysqlStandbyCluster " + ns + "/orders-east-from-west",
			"Nothing was changed",
		} {
			if !strings.Contains(out, want) {
				t.Fatalf("the report (dry run %v) lacks %q:\n%s", dryRun, want, out)
			}
		}
		for _, mustNot := range []string{"groupRef:", "lbIP", "sidecarImage"} {
			if strings.Contains(out, mustNot) {
				t.Fatalf("the report names the legacy field %q, which the API server no longer shows:\n%s", mustNot, out)
			}
		}
	}
	after := e.allObjects()
	for key, rv := range resourceVersions(before) {
		if got := after[key].GetResourceVersion(); got != rv {
			t.Fatalf("%s was written (resourceVersion %s to %s) by a refused migration", key, rv, got)
		}
	}
	for name, versions := range e.storedVersions() {
		if strings.Join(versions, ",") != "v1alpha1,v1beta1" {
			t.Fatalf("%s storedVersions = %v after a refused migration", name, versions)
		}
	}

	// C-14: the operator refuses to start on the same objects.
	report := startupCheck(e)
	if len(report.Refusals) != 3 {
		t.Fatalf("the startup check refuses %d things, want the 3 objects:\n%s", len(report.Refusals), findings(report.Refusals))
	}
	for _, want := range []string{"MysqlFailoverGroup " + ns + "/orders", "MysqlDatabase " + ns + "/tenant-acme", "MysqlStandbyCluster " + ns + "/orders-east-from-west"} {
		if !strings.Contains(findings(report.Refusals), want) {
			t.Fatalf("the startup check does not refuse %s:\n%s", want, findings(report.Refusals))
		}
	}
}

// U-11: a standby cluster's DR site holds MysqlStandbyClusters and no
// MysqlFailoverGroup. The operator's check covers every kind, so it is
// refused too.
func TestStartupCheck_V160StandbyClusterWithoutAGroup(t *testing.T) {
	e := startUpgradeEnv(t, "crd-v1.6.0")
	ns := e.namespace("dr-site")
	v160Objects(e, ns, "mysqlstandbycluster")
	e.applyBuildCRDs()
	report := startupCheck(e)
	if len(report.Refusals) != 1 || !strings.Contains(report.Refusals[0].String(), "MysqlStandbyCluster "+ns+"/orders-east-from-west") ||
		!strings.Contains(report.Refusals[0].String(), "spec.source.s3") {
		t.Fatalf("refusals = %q, want the one standby cluster and its missing spec.source.s3", findings(report.Refusals))
	}
}

// C-14 (1): a chart-only `helm upgrade` leaves the previous release's CRDs
// in place. Against v1.7.0's, the version the operator talks is not served.
func TestStartupCheck_RefusesCRDsOfThePreviousRelease(t *testing.T) {
	e := startUpgradeEnv(t, "crd-v1.7.0")
	report := startupCheck(e)
	if len(report.Refusals) != len(crdcompat.Resources) {
		t.Fatalf("refusals = %q, want one per CRD", findings(report.Refusals))
	}
	for _, f := range report.Refusals {
		if !strings.Contains(f.Detail, "older than this operator needs") || !strings.Contains(f.Detail, "shipstream.io/v1beta1 is not served") {
			t.Fatalf("refusal %q does not say the CRD is older and why", f)
		}
	}
}

// C-14 (1) and (2) on this build's own CRDs: one that lacks a field the
// build writes is older and refuses; one with a field more is newer and
// only warns; the CRDs as built pass.
func TestStartupCheck_OlderRefusesNewerWarns(t *testing.T) {
	e := startUpgradeEnv(t, "")
	if report := startupCheck(e); len(report.Refusals)+len(report.NewerCRDs)+len(report.RuleViolations) != 0 {
		t.Fatalf("this build's CRDs do not pass its own check: %+v", report)
	}
	ns := e.namespace("fits")
	fg := newTestFG(ns)
	if err := e.admin.Create(ctx, fg); err != nil {
		t.Fatal(err)
	}
	if report := startupCheck(e); len(report.Refusals) != 0 || report.Checked["MysqlFailoverGroup"] != 1 {
		t.Fatalf("a group created under this build's CRDs does not pass: %+v", report)
	}

	mfg := crdcompat.Resources[0].CRDName()
	editInstalled := func(fn func(root *apiextensionsv1.JSONSchemaProps)) {
		t.Helper()
		var live apiextensionsv1.CustomResourceDefinition
		if err := e.admin.Get(ctx, types.NamespacedName{Name: mfg}, &live); err != nil {
			t.Fatal(err)
		}
		// The served versions share one schema: edit both.
		for i := range live.Spec.Versions {
			fn(live.Spec.Versions[i].Schema.OpenAPIV3Schema)
		}
		if err := e.admin.Update(ctx, &live); err != nil {
			t.Fatal(err)
		}
	}

	// Newer: an optional field this build does not know.
	editInstalled(func(root *apiextensionsv1.JSONSchemaProps) {
		root.Properties["spec"].Properties["futureField"] = apiextensionsv1.JSONSchemaProps{Type: "string"}
	})
	report := startupCheck(e)
	if len(report.Refusals) != 0 {
		t.Fatalf("a newer CRD refuses the start:\n%s", findings(report.Refusals))
	}
	if len(report.NewerCRDs) != 1 || report.NewerCRDs[0].Subject != mfg || !strings.Contains(report.NewerCRDs[0].Detail, "spec.futureField") {
		t.Fatalf("newer CRDs = %q, want the one extra field", findings(report.NewerCRDs))
	}

	// Older: a field this build writes is absent.
	editInstalled(func(root *apiextensionsv1.JSONSchemaProps) {
		delete(root.Properties["spec"].Properties, "futureField")
		delete(root.Properties["status"].Properties, "rollout")
	})
	report = startupCheck(e)
	if len(report.Refusals) != 1 || report.Refusals[0].Subject != mfg || !strings.Contains(report.Refusals[0].Detail, "status.rollout is missing") {
		t.Fatalf("refusals = %q, want the CRD that lacks status.rollout", findings(report.Refusals))
	}
}

// fixtureKinds are the files of test/fixtures/v1alpha1: one object per
// kind, as the published v1.7.0 stored it.
var fixtureKinds = []string{"mysqlfailovergroup", "mysqldatabase", "mysqlbackup", "mysqlbackupverification", "mysqlstandbycluster"}

// v170Fixtures creates the v1.7.0 fixtures under v1.7.0's CRDs, through
// v1alpha1, with the status the v1.7.0 operator had written. It returns
// the fixtures by "<Kind> <namespace>/<name>".
func v170Fixtures(e *upgradeEnv, ns string) map[string]*unstructured.Unstructured {
	e.t.Helper()
	out := map[string]*unstructured.Unstructured{}
	old := v1beta1.DeprecatedGroupVersion.Version
	for _, name := range fixtureKinds {
		fixture := loadObject(e.t, filepath.Join("..", "fixtures", "v1alpha1", name+".yaml"))
		created := e.createAt(fixture, ns, old)
		status, found, err := unstructured.NestedMap(fixture.Object, "status")
		if err != nil || !found {
			e.t.Fatalf("fixture %s has no status", name)
		}
		if err := unstructured.SetNestedMap(created.Object, status, "status"); err != nil {
			e.t.Fatal(err)
		}
		r := resourceOf(e.t, fixture)
		if _, err := e.dyn.Resource(r.GVR(old)).Namespace(ns).UpdateStatus(ctx, created, metav1.UpdateOptions{FieldValidation: "Strict"}); err != nil {
			e.t.Fatalf("write the v1.7.0 status of %s through %s: %v", r.Kind, old, err)
		}
		out[r.Kind+" "+ns+"/"+fixture.GetName()] = fixture
	}
	return out
}

// missingFrom returns the paths of want (a JSON value) that got lacks or
// holds another value at. Fields got has beyond want are fine: a newer
// schema adds defaults.
func missingFrom(path string, want, got interface{}) []string {
	switch w := want.(type) {
	case map[string]interface{}:
		g, ok := got.(map[string]interface{})
		if !ok {
			return []string{path}
		}
		var out []string
		for k, v := range w {
			out = append(out, missingFrom(path+"."+k, v, g[k])...)
		}
		return out
	case []interface{}:
		g, ok := got.([]interface{})
		if !ok || len(g) != len(w) {
			return []string{path}
		}
		var out []string
		for i := range w {
			out = append(out, missingFrom(fmt.Sprintf("%s[%d]", path, i), w[i], g[i])...)
		}
		return out
	default:
		if !equality.Semantic.DeepEqual(want, got) {
			return []string{path}
		}
		return nil
	}
}

// C-6, C-7 and C-5b on the supported upgrade: v1.7.0 to this build.
//
// The objects v1.7.0 stored are read back complete through v1beta1 once
// this build's CRDs are applied, the operator's check at start passes, and
// migrate-storage, run by an account that holds only the example
// ClusterRole, rewrites every one of them and trims storedVersions. The
// proof that each was rewritten: v1alpha1 can then be removed from the CRDs
// (the API server refuses that while storedVersions lists it), and every
// object is still readable, which an object left encoded as v1alpha1 is
// not.
func TestMigrateStorage_FromV170(t *testing.T) {
	e := startUpgradeEnv(t, "crd-v1.7.0")
	ns := e.namespace("from-v170")
	fixtures := v170Fixtures(e, ns)
	e.applyBuildCRDs()

	// C-6: nothing v1.7.0 stored is lost under this build's schema.
	before := e.allObjects()
	for key, fixture := range fixtures {
		got, ok := before[key]
		if !ok {
			t.Fatalf("%s is not readable through %s after the CRDs were applied", key, v1beta1.GroupVersion.Version)
		}
		for _, part := range []string{"spec", "status"} {
			if lost := missingFrom(part, fixture.Object[part], got.Object[part]); len(lost) > 0 {
				t.Errorf("%s: read through %s, the v1.7.0 object lost or changed %v", key, v1beta1.GroupVersion.Version, lost)
			}
		}
	}
	// The typed client decodes every one of them.
	for _, list := range []client.ObjectList{&v1beta1.MysqlFailoverGroupList{}, &v1beta1.MysqlDatabaseList{}, &v1beta1.MysqlBackupList{},
		&v1beta1.MysqlBackupVerificationList{}, &v1beta1.MysqlStandbyClusterList{}} {
		if err := e.admin.List(ctx, list, client.InNamespace(ns)); err != nil {
			t.Fatalf("the typed client cannot list %T: %v", list, err)
		}
	}
	// C-14: the operator starts.
	if report := startupCheck(e); len(report.Refusals)+len(report.RuleViolations)+len(report.NewerCRDs) != 0 {
		t.Fatalf("the operator's check at start does not pass on v1.7.0's objects: %+v", report)
	}
	for name, versions := range e.storedVersions() {
		if strings.Join(versions, ",") != "v1alpha1,v1beta1" {
			t.Fatalf("%s storedVersions = %v before the migration", name, versions)
		}
	}

	// C-5b: an account without the permissions is refused before anything
	// is read in bulk or written, with the list of what it lacks.
	user, err := e.env.AddUser(envtest.User{Name: "storage-migrator", Groups: []string{"migrators"}}, nil)
	if err != nil {
		t.Fatal(err)
	}
	out, err := e.migrate(user.Config(), false)
	var refusal *storagemigration.Refusal
	if !errors.As(err, &refusal) || !strings.Contains(out, "Missing permissions:") ||
		!strings.Contains(out, "update customresourcedefinitions/status in apiextensions.k8s.io (mysqlfailovergroups.shipstream.io)") ||
		!strings.Contains(out, "list mysqlbackups in shipstream.io") || !strings.Contains(out, "Nothing was changed") {
		t.Fatalf("migrate-storage without permissions = %v, want a refusal that lists them:\n%s", err, out)
	}
	if got := len(strings.Split(strings.TrimSpace(out[strings.Index(out, "Missing permissions:"):strings.Index(out, "Nothing was changed")]), "\n")) - 1; got != len(storagemigration.RequiredPermissions()) {
		t.Fatalf("%d missing permissions are listed, want all %d", got, len(storagemigration.RequiredPermissions()))
	}

	// The example ClusterRole, and nothing else, is enough.
	role := &rbacv1.ClusterRole{}
	raw, err := os.ReadFile(filepath.Join("..", "..", "examples", "storage-migrator-clusterrole.yaml"))
	if err != nil {
		t.Fatal(err)
	}
	if err := yaml.UnmarshalStrict(raw, role); err != nil {
		t.Fatal(err)
	}
	rbacScheme := runtime.NewScheme()
	if err := rbacv1.AddToScheme(rbacScheme); err != nil {
		t.Fatal(err)
	}
	rbacClient, err := client.New(e.cfg, client.Options{Scheme: rbacScheme})
	if err != nil {
		t.Fatal(err)
	}
	if err := rbacClient.Create(ctx, role); err != nil {
		t.Fatal(err)
	}
	if err := rbacClient.Create(ctx, &rbacv1.ClusterRoleBinding{
		ObjectMeta: metav1.ObjectMeta{Name: "storage-migrator"},
		RoleRef:    rbacv1.RoleRef{APIGroup: rbacv1.GroupName, Kind: "ClusterRole", Name: role.Name},
		Subjects:   []rbacv1.Subject{{APIGroup: rbacv1.GroupName, Kind: "Group", Name: "migrators"}},
	}); err != nil {
		t.Fatal(err)
	}

	// A dry run reports and writes nothing.
	out, err = e.migrate(user.Config(), true)
	if err != nil || !strings.Contains(out, "5 object(s) checked against the installed schema: all conform") || !strings.Contains(out, "Dry run:") {
		t.Fatalf("dry run = %v:\n%s", err, out)
	}
	for key, rv := range resourceVersions(before) {
		if got := e.allObjects()[key].GetResourceVersion(); got != rv {
			t.Fatalf("%s was written by a dry run", key)
		}
	}

	// The migration.
	out, err = e.migrate(user.Config(), false)
	if err != nil {
		t.Fatalf("migrate-storage: %v\n%s", err, out)
	}
	for _, want := range []string{"MysqlFailoverGroup", "1 written back", "Done. Every Bloodraven object is stored as shipstream.io/v1beta1."} {
		if !strings.Contains(out, want) {
			t.Fatalf("the report lacks %q:\n%s", want, out)
		}
	}
	for name, versions := range e.storedVersions() {
		if strings.Join(versions, ",") != "v1beta1" {
			t.Fatalf("%s storedVersions = %v after the migration, want [v1beta1]", name, versions)
		}
	}
	after := e.allObjects()
	for key, was := range before {
		now, ok := after[key]
		if !ok {
			t.Fatalf("%s is gone after the migration", key)
		}
		if now.GetUID() != was.GetUID() {
			t.Fatalf("%s was recreated by the migration (uid %s, was %s)", key, now.GetUID(), was.GetUID())
		}
		if now.GetResourceVersion() == was.GetResourceVersion() {
			t.Fatalf("%s was not rewritten: its resourceVersion is unchanged", key)
		}
		if now.GetGeneration() != was.GetGeneration() {
			t.Fatalf("%s: the migration changed the spec (generation %d, was %d)", key, now.GetGeneration(), was.GetGeneration())
		}
		for _, part := range []string{"spec", "status"} {
			if !equality.Semantic.DeepEqual(was.Object[part], now.Object[part]) {
				t.Fatalf("%s: the migration changed .%s", key, part)
			}
		}
	}

	// Running it again finds nothing to do and writes nothing.
	out, err = e.migrate(user.Config(), false)
	if err != nil || !strings.Contains(out, "Nothing to do") {
		t.Fatalf("second run = %v:\n%s", err, out)
	}
	for key, obj := range e.allObjects() {
		if obj.GetResourceVersion() != after[key].GetResourceVersion() {
			t.Fatalf("%s was written by a run that had nothing to do", key)
		}
	}

	// The proof: remove v1alpha1 from every CRD, then read everything.
	for _, r := range crdcompat.Resources {
		var crd apiextensionsv1.CustomResourceDefinition
		if err := e.admin.Get(ctx, types.NamespacedName{Name: r.CRDName()}, &crd); err != nil {
			t.Fatal(err)
		}
		var kept []apiextensionsv1.CustomResourceDefinitionVersion
		for _, v := range crd.Spec.Versions {
			if v.Name != v1beta1.DeprecatedGroupVersion.Version {
				kept = append(kept, v)
			}
		}
		crd.Spec.Versions = kept
		if err := e.admin.Update(ctx, &crd); err != nil {
			t.Fatalf("remove v1alpha1 from %s after the migration: %v", crd.Name, err)
		}
	}
	deadline := time.Now().Add(60 * time.Second)
	for {
		_, err := e.dyn.Resource(crdcompat.Resources[0].GVR(v1beta1.DeprecatedGroupVersion.Version)).List(ctx, metav1.ListOptions{Limit: 1})
		if apierrors.IsNotFound(err) {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("v1alpha1 is still served 60s after it was removed from the CRDs (%v)", err)
		}
		time.Sleep(200 * time.Millisecond)
	}
	final := e.allObjects()
	if len(final) != len(before) {
		t.Fatalf("%d objects are readable once v1alpha1 is gone, want %d: an object was left stored as v1alpha1", len(final), len(before))
	}
}

// U-13, the other half: without the migration the API server refuses a CRD
// that drops v1alpha1, because storedVersions still lists it.
func TestStoredVersionsGuardTheDeprecatedVersion(t *testing.T) {
	e := startUpgradeEnv(t, "crd-v1.7.0")
	v170Fixtures(e, e.namespace("unmigrated"))
	e.applyBuildCRDs()
	var crd apiextensionsv1.CustomResourceDefinition
	if err := e.admin.Get(ctx, types.NamespacedName{Name: crdcompat.Resources[0].CRDName()}, &crd); err != nil {
		t.Fatal(err)
	}
	crd.Spec.Versions = crd.Spec.Versions[:1]
	if crd.Spec.Versions[0].Name != v1beta1.GroupVersion.Version {
		t.Fatalf("the first version is %s", crd.Spec.Versions[0].Name)
	}
	if err := e.admin.Update(ctx, &crd); !apierrors.IsInvalid(err) || !strings.Contains(err.Error(), "storedVersions") {
		t.Fatalf("removing v1alpha1 before the migration = %v, want it refused because storedVersions lists v1alpha1", err)
	}
}

// U-12: the operator writes status on every poll while migrate-storage
// runs. A rewrite that loses to such a write is retried on a fresh read,
// and the migration still completes.
func TestMigrateStorage_ConcurrentStatusWriter(t *testing.T) {
	e := startUpgradeEnv(t, "crd-v1.7.0")
	ns := e.namespace("busy")
	v170Fixtures(e, ns)
	e.applyBuildCRDs()

	key := types.NamespacedName{Namespace: ns, Name: "playground"}
	stop := make(chan struct{})
	var writes, failures atomic.Int64
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := 0; ; i++ {
			select {
			case <-stop:
				return
			default:
			}
			var fg v1beta1.MysqlFailoverGroup
			if err := e.admin.Get(ctx, key, &fg); err != nil {
				failures.Add(1)
				continue
			}
			fg.Status.TopologyGeneration = int64(1000 + i)
			if err := e.admin.Status().Update(ctx, &fg); err != nil {
				if !apierrors.IsConflict(err) {
					failures.Add(1)
				}
				continue
			}
			writes.Add(1)
		}
	}()
	// Let the writer get going, so the migration really meets it.
	for writes.Load() < 20 {
		time.Sleep(10 * time.Millisecond)
	}
	out, err := e.migrate(e.cfg, false)
	during := writes.Load()
	close(stop)
	wg.Wait()
	if err != nil {
		t.Fatalf("migrate-storage beside a status writer: %v\n%s", err, out)
	}
	if failures.Load() != 0 {
		t.Fatalf("the status writer failed %d times beside the migration", failures.Load())
	}
	if during < 30 {
		t.Fatalf("the writer made only %d writes; the test did not exercise the race", during)
	}
	for name, versions := range e.storedVersions() {
		if strings.Join(versions, ",") != "v1beta1" {
			t.Fatalf("%s storedVersions = %v", name, versions)
		}
	}
	// The writer's last status survived: the migration writes back what it
	// read through the main resource, which does not touch status.
	var fg v1beta1.MysqlFailoverGroup
	if err := e.admin.Get(ctx, key, &fg); err != nil {
		t.Fatal(err)
	}
	if fg.Status.TopologyGeneration < 1000 {
		t.Fatalf("status.topologyGeneration = %d: the migration overwrote the writer's status", fg.Status.TopologyGeneration)
	}
}

// U-2 and U-13: recovery after this build's CRDs were applied to a cluster
// that holds v1.6.0 objects, while nothing has written a Bloodraven object
// since. The API server prunes the old field names from every read, but
// etcd still holds the objects as v1.6.0 wrote them. Re-applying the v1.6.0
// CRDs is refused, because status.storedVersions lists v1beta1 from the
// moment the new CRDs were applied. The way back, as the upgrade policy
// gives it:
//
//  1. apply the v1.6.0 CRD with one more version, v1beta1, not served and
//     not stored, so that the CRD still names every stored version;
//  2. remove v1beta1 from status.storedVersions, which is true only
//     because no object was written while v1beta1 was the storage version;
//  3. apply the v1.6.0 CRD as published.
//
// Afterwards every object reads exactly as it did before the mistake.
func TestRecoverV160CRDsAfterTheNewOnesWereApplied(t *testing.T) {
	e := startUpgradeEnv(t, "crd-v1.6.0")
	ns := e.namespace("mistake")
	old := v1beta1.DeprecatedGroupVersion.Version
	names := []string{"mysqlfailovergroup", "mysqldatabase", "mysqlbackup", "mysqlbackupverification", "mysqlstandbycluster"}
	v160Objects(e, ns, names...)
	read := func() map[string]*unstructured.Unstructured {
		t.Helper()
		out := map[string]*unstructured.Unstructured{}
		for _, r := range crdcompat.Resources {
			list, err := e.dyn.Resource(r.GVR(old)).Namespace(ns).List(ctx, metav1.ListOptions{})
			if err != nil {
				t.Fatalf("list %s through %s: %v", r.Plural, old, err)
			}
			for i := range list.Items {
				out[r.Kind+"/"+list.Items[i].GetName()] = &list.Items[i]
			}
		}
		return out
	}
	before := read()
	if len(before) != len(names) {
		t.Fatalf("%d objects before the mistake, want %d", len(before), len(names))
	}

	// The mistake.
	e.applyBuildCRDs()
	pruned := read()
	if _, found, _ := unstructured.NestedString(pruned["MysqlDatabase/tenant-acme"].Object, "spec", "groupRef", "name"); found {
		t.Fatal("the new CRD still shows the v1.6.0 field spec.groupRef: the premise of this test is gone")
	}

	for _, r := range crdcompat.Resources {
		published := loadCRDFile(t, filepath.Join("testdata", "crd-v1.6.0", "shipstream.io_"+r.Plural+".yaml"))
		key := types.NamespacedName{Name: r.CRDName()}

		// The API server's own controllers write the CRD's conditions, so
		// each step retries on a conflict.
		step := func(n int, status bool, mutate func(*apiextensionsv1.CustomResourceDefinition)) {
			t.Helper()
			err := retry.RetryOnConflict(retry.DefaultBackoff, func() error {
				var live apiextensionsv1.CustomResourceDefinition
				if err := e.admin.Get(ctx, key, &live); err != nil {
					return err
				}
				mutate(&live)
				if status {
					return e.admin.Status().Update(ctx, &live)
				}
				return e.admin.Update(ctx, &live)
			})
			if err != nil {
				t.Fatalf("step %d, %s: %v", n, r.CRDName(), err)
			}
		}
		preserve := true
		bridge := published.Spec.DeepCopy()
		bridge.Versions = append(bridge.Versions, apiextensionsv1.CustomResourceDefinitionVersion{
			Name: v1beta1.GroupVersion.Version, Served: false, Storage: false,
			Schema: &apiextensionsv1.CustomResourceValidation{OpenAPIV3Schema: &apiextensionsv1.JSONSchemaProps{Type: "object", XPreserveUnknownFields: &preserve}},
		})
		step(1, false, func(live *apiextensionsv1.CustomResourceDefinition) { live.Spec = *bridge })
		step(2, true, func(live *apiextensionsv1.CustomResourceDefinition) { live.Status.StoredVersions = []string{old} })
		step(3, false, func(live *apiextensionsv1.CustomResourceDefinition) { live.Spec = published.Spec })
	}

	// The API server serves the v1.6.0 schema again once it has rebuilt
	// its handlers: wait for the pruned field to come back.
	deadline := time.Now().Add(60 * time.Second)
	for {
		if _, found, _ := unstructured.NestedString(read()["MysqlDatabase/tenant-acme"].Object, "spec", "groupRef", "name"); found {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("the v1.6.0 schema is not served 60s after the v1.6.0 CRDs were restored")
		}
		time.Sleep(200 * time.Millisecond)
	}
	after := read()
	for key, was := range before {
		now := after[key]
		if now == nil {
			t.Fatalf("%s is gone", key)
		}
		if !equality.Semantic.DeepEqual(was.Object["spec"], now.Object["spec"]) {
			t.Errorf("%s: its spec is not what it was before the mistake:\nbefore %v\nafter  %v", key, was.Object["spec"], now.Object["spec"])
		}
		if was.GetResourceVersion() != now.GetResourceVersion() || was.GetUID() != now.GetUID() {
			t.Errorf("%s was written during the recovery", key)
		}
	}
	for name, versions := range e.storedVersions() {
		if strings.Join(versions, ",") != old {
			t.Errorf("%s storedVersions = %v after the recovery, want [%s]", name, versions, old)
		}
	}
}
