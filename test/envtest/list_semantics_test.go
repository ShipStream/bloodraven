//go:build envtest

package envtest

// Admission tests for WS-E E4: a required spec on every kind (A-05), the
// list-map, set and atomic list semantics (A-07 to A-14), the explicit
// site serverID (A-08), and the spec rules E4 tightens, with a status write
// on an object stored before them (A-115, C-15). CI runs the whole envtest
// suite on the supported floor (1.31) as well as the current version.

import (
	"fmt"
	"path/filepath"
	"strings"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	apiextensionsv1 "k8s.io/apiextensions-apiserver/pkg/apis/apiextensions/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	apimeta "k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/envtest"

	v1alpha1 "github.com/shipstream/bloodraven/api/v1alpha1"
)

// e4WantInvalid fails unless err is an Invalid refusal whose message
// contains every fragment.
func e4WantInvalid(t *testing.T, what string, err error, fragments ...string) {
	t.Helper()
	if err == nil {
		t.Fatalf("%s: accepted, want a refusal", what)
	}
	if !apierrors.IsInvalid(err) {
		t.Fatalf("%s: error %T %v, want Invalid", what, err, err)
	}
	for _, f := range fragments {
		if !strings.Contains(err.Error(), f) {
			t.Fatalf("%s: refused for another reason (want %q): %v", what, f, err)
		}
	}
}

// e4Unstructured converts a typed object for a write that sets fields the
// typed client would drop or default.
func e4Unstructured(t *testing.T, obj client.Object, kind string) *unstructured.Unstructured {
	t.Helper()
	m, err := runtime.DefaultUnstructuredConverter.ToUnstructured(obj)
	if err != nil {
		t.Fatal(err)
	}
	u := &unstructured.Unstructured{Object: m}
	u.SetAPIVersion(v1alpha1.GroupVersion.String())
	u.SetKind(kind)
	unstructured.RemoveNestedField(u.Object, "metadata", "creationTimestamp")
	unstructured.RemoveNestedField(u.Object, "status")
	return u
}

// A-05: an object without spec is refused, so no nested required field or
// rule is skipped.
func TestE4_SpecRequiredOnEveryKind(t *testing.T) {
	ns := createNamespace(t, "e4-spec")
	for _, kind := range []string{"MysqlFailoverGroup", "MysqlBackup", "MysqlBackupVerification", "MysqlDatabase", "MysqlStandbyCluster"} {
		u := &unstructured.Unstructured{Object: map[string]any{}}
		u.SetAPIVersion(v1alpha1.GroupVersion.String())
		u.SetKind(kind)
		u.SetNamespace(ns)
		u.SetName("no-spec")
		e4WantInvalid(t, kind+" without spec", k8sClient.Create(ctx, u), "spec", "Required value")
	}
}

// A-08: spec.sites is a list-map keyed by name, and every site carries a
// required serverID in 1..4294967295, unique in the group and immutable,
// independent of the site's position.
func TestE4_SiteServerID(t *testing.T) {
	ns := createNamespace(t, "e4-serverid")
	create := func(name string, mutate func(*v1alpha1.MysqlFailoverGroup)) error {
		fg := newTestFG(ns)
		fg.Name = name
		mutate(fg)
		return k8sClient.Create(ctx, fg)
	}

	if err := create("max", func(fg *v1alpha1.MysqlFailoverGroup) {
		fg.Spec.Sites[0].ServerID = v1alpha1.MaxServerID
		fg.Spec.Sites[1].ServerID = 7
	}); err != nil {
		t.Fatalf("serverIDs 4294967295 and 7 refused: %v", err)
	}
	var got v1alpha1.MysqlFailoverGroup
	if err := k8sClient.Get(ctx, client.ObjectKey{Namespace: ns, Name: "max"}, &got); err != nil {
		t.Fatal(err)
	}
	if got.Spec.Sites[0].ServerID != v1alpha1.MaxServerID || got.Spec.Sites[1].ServerID != 7 {
		t.Fatalf("stored serverIDs = %d, %d", got.Spec.Sites[0].ServerID, got.Spec.Sites[1].ServerID)
	}

	e4WantInvalid(t, "duplicate serverID", create("dup-id", func(fg *v1alpha1.MysqlFailoverGroup) {
		fg.Spec.Sites[1].ServerID = fg.Spec.Sites[0].ServerID
	}), "spec.sites[].serverID must be unique within the group")
	e4WantInvalid(t, "duplicate site name", create("dup-name", func(fg *v1alpha1.MysqlFailoverGroup) {
		fg.Spec.Sites[1].Name = fg.Spec.Sites[0].Name
	}), "spec.sites", "Duplicate value")
	for _, id := range []int64{0, -1, v1alpha1.MaxServerID + 1} {
		e4WantInvalid(t, fmt.Sprintf("serverID %d", id), create(fmt.Sprintf("bad-id-%d", id+1), func(fg *v1alpha1.MysqlFailoverGroup) {
			fg.Spec.Sites[0].ServerID = id
		}), "serverID")
	}
	missing := e4Unstructured(t, newTestFG(ns), "MysqlFailoverGroup")
	missing.SetName("no-id")
	sites, _, _ := unstructured.NestedSlice(missing.Object, "spec", "sites")
	delete(sites[0].(map[string]any), "serverID")
	if err := unstructured.SetNestedSlice(missing.Object, sites, "spec", "sites"); err != nil {
		t.Fatal(err)
	}
	e4WantInvalid(t, "site without serverID", k8sClient.Create(ctx, missing), "serverID", "Required value")

	// Immutable per site, by name: a reorder keeps each id, a change is refused.
	reordered := got.DeepCopy()
	reordered.Spec.Sites[0], reordered.Spec.Sites[1] = reordered.Spec.Sites[1], reordered.Spec.Sites[0]
	if err := k8sClient.Update(ctx, reordered); err != nil {
		t.Fatalf("reordering sites refused: %v", err)
	}
	changed := reordered.DeepCopy()
	changed.Spec.Sites[0].ServerID = 8
	e4WantInvalid(t, "serverID change", k8sClient.Update(ctx, changed), "serverID is immutable")
}

// A-13: splitBrainPolicy.sitePriorities entries are unique.
func TestE4_SitePrioritiesUnique(t *testing.T) {
	ns := createNamespace(t, "e4-priorities")
	fg := newTestFG(ns)
	fg.Spec.SplitBrainPolicy = &v1alpha1.SplitBrainPolicySpec{SitePriorities: []string{"dc1", "dc1"}}
	e4WantInvalid(t, "duplicate sitePriorities", k8sClient.Create(ctx, fg), "splitBrainPolicy.sitePriorities entries must be unique")
	fg.Spec.SplitBrainPolicy.SitePriorities = []string{"dc2", "dc1"}
	if err := k8sClient.Create(ctx, fg); err != nil {
		t.Fatalf("distinct sitePriorities refused: %v", err)
	}
}

// A-09, A-10: named spec lists are list-maps keyed by name, with bounds.
func TestE4_NamedSpecListsAreMaps(t *testing.T) {
	ns := createNamespace(t, "e4-named")
	container := func(name string) corev1.Container { return corev1.Container{Name: name, Image: "busybox"} }
	containers := func(n int) []corev1.Container {
		out := make([]corev1.Container, 0, n)
		for i := 0; i < n; i++ {
			out = append(out, container(fmt.Sprintf("c%d", i)))
		}
		return out
	}
	schedule := func(name string) v1alpha1.BackupSchedule {
		return v1alpha1.BackupSchedule{Name: name, ProfileName: "nightly-s3", Schedule: "0 3 * * *"}
	}
	schedules := func(n int) []v1alpha1.BackupSchedule {
		out := make([]v1alpha1.BackupSchedule, 0, n)
		for i := 0; i < n; i++ {
			out = append(out, schedule(fmt.Sprintf("s%d", i)))
		}
		return out
	}
	for i, tc := range []struct {
		name   string
		mutate func(*v1alpha1.MysqlFailoverGroup)
		want   []string // empty: accepted
	}{
		{"extraContainers duplicate", func(fg *v1alpha1.MysqlFailoverGroup) {
			fg.Spec.ExtraContainers = []corev1.Container{container("x"), container("x")}
		}, []string{"extraContainers", "Duplicate value"}},
		{"extraContainers 16", func(fg *v1alpha1.MysqlFailoverGroup) { fg.Spec.ExtraContainers = containers(16) }, nil},
		{"extraContainers 17", func(fg *v1alpha1.MysqlFailoverGroup) { fg.Spec.ExtraContainers = containers(17) }, []string{"extraContainers", "16"}},
		{"extraInitContainers duplicate", func(fg *v1alpha1.MysqlFailoverGroup) {
			fg.Spec.ExtraInitContainers = []corev1.Container{container("x"), container("x")}
		}, []string{"extraInitContainers", "Duplicate value"}},
		{"extraInitContainers 17", func(fg *v1alpha1.MysqlFailoverGroup) { fg.Spec.ExtraInitContainers = containers(17) }, []string{"extraInitContainers", "16"}},
		{"imagePullSecrets duplicate", func(fg *v1alpha1.MysqlFailoverGroup) {
			fg.Spec.Backup.ImagePullSecrets = []corev1.LocalObjectReference{{Name: "r"}, {Name: "r"}}
		}, []string{"imagePullSecrets", "Duplicate value"}},
		{"schedules duplicate", func(fg *v1alpha1.MysqlFailoverGroup) {
			fg.Spec.Backup.Schedules = []v1alpha1.BackupSchedule{schedule("daily"), schedule("daily")}
		}, []string{"schedules", "Duplicate value"}},
		{"schedules 32", func(fg *v1alpha1.MysqlFailoverGroup) { fg.Spec.Backup.Schedules = schedules(32) }, nil},
		{"schedules 33", func(fg *v1alpha1.MysqlFailoverGroup) { fg.Spec.Backup.Schedules = schedules(33) }, []string{"schedules", "32"}},
		{"schedule name 63", func(fg *v1alpha1.MysqlFailoverGroup) {
			fg.Spec.Backup.Schedules = []v1alpha1.BackupSchedule{schedule(strings.Repeat("a", 63))}
		}, nil},
		{"schedule name 64", func(fg *v1alpha1.MysqlFailoverGroup) {
			fg.Spec.Backup.Schedules = []v1alpha1.BackupSchedule{schedule(strings.Repeat("a", 64))}
		}, []string{"schedules", "63"}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			fg := newVerificationFG(ns)
			fg.Name = fmt.Sprintf("named-%d", i)
			tc.mutate(fg)
			err := k8sClient.Create(ctx, fg)
			if len(tc.want) == 0 {
				if err != nil {
					t.Fatalf("refused: %v", err)
				}
				return
			}
			e4WantInvalid(t, tc.name, err, tc.want...)
		})
	}
}

// A-12: schema-name lists are bounded sets; an in-place restore loads at
// most one schema.
func TestE4_SchemaListsAreSets(t *testing.T) {
	ns := createNamespace(t, "e4-schemas")
	backupRef := v1alpha1.RestoreSource{BackupRef: &v1alpha1.LocalMysqlBackupRef{Name: "b"}}
	for i, tc := range []struct {
		name   string
		mutate func(*v1alpha1.MysqlFailoverGroup)
		want   []string
	}{
		{"dump includeSchemas duplicate", func(fg *v1alpha1.MysqlFailoverGroup) {
			fg.Spec.Backup.Profiles[0].Dump = &v1alpha1.DumpOptions{IncludeSchemas: []string{"a", "a"}}
		}, []string{"includeSchemas", "Duplicate value"}},
		{"dump excludeSchemas name 65", func(fg *v1alpha1.MysqlFailoverGroup) {
			fg.Spec.Backup.Profiles[0].Dump = &v1alpha1.DumpOptions{ExcludeSchemas: []string{strings.Repeat("a", 65)}}
		}, []string{"excludeSchemas", "64"}},
		{"initFromBackup includeSchemas two", func(fg *v1alpha1.MysqlFailoverGroup) {
			fg.Spec.InitFromBackup = &v1alpha1.InitFromBackupSpec{Source: backupRef, LoadOptions: &v1alpha1.LoadOptions{IncludeSchemas: []string{"a", "b"}}}
		}, nil},
		{"initFromBackup excludeSchemas 257", func(fg *v1alpha1.MysqlFailoverGroup) {
			names := make([]string, 257)
			for j := range names {
				names[j] = fmt.Sprintf("s%d", j)
			}
			fg.Spec.InitFromBackup = &v1alpha1.InitFromBackupSpec{Source: backupRef, LoadOptions: &v1alpha1.LoadOptions{ExcludeSchemas: names}}
		}, []string{"excludeSchemas", "256"}},
		{"restoreInPlace includeSchemas one", func(fg *v1alpha1.MysqlFailoverGroup) {
			fg.Spec.RestoreInPlace = &v1alpha1.RestoreInPlaceSpec{Confirm: "2026-01-01T00:00:00Z", Source: backupRef,
				LoadOptions: &v1alpha1.LoadOptions{IncludeSchemas: []string{"a"}}}
		}, nil},
		{"restoreInPlace includeSchemas two", func(fg *v1alpha1.MysqlFailoverGroup) {
			fg.Spec.RestoreInPlace = &v1alpha1.RestoreInPlaceSpec{Confirm: "2026-01-01T00:00:00Z", Source: backupRef,
				LoadOptions: &v1alpha1.LoadOptions{IncludeSchemas: []string{"a", "b"}}}
		}, []string{"restoreInPlace.loadOptions.includeSchemas supports at most one schema"}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			fg := newVerificationFG(ns)
			fg.Name = fmt.Sprintf("schemas-%d", i)
			tc.mutate(fg)
			err := k8sClient.Create(ctx, fg)
			if len(tc.want) == 0 {
				if err != nil {
					t.Fatalf("refused: %v", err)
				}
				return
			}
			e4WantInvalid(t, tc.name, err, tc.want...)
		})
	}
}

// A-07, A-11: conditions and the status lists keyed by name are list-maps
// with a required key, so a status write with a duplicate or empty key is
// refused.
func TestE4_StatusListsAreMaps(t *testing.T) {
	ns := createNamespace(t, "e4-status")
	fg := newTestFG(ns)
	if err := k8sClient.Create(ctx, fg); err != nil {
		t.Fatal(err)
	}
	now := metav1.Now()
	cond := func(typ string) metav1.Condition {
		return metav1.Condition{Type: typ, Status: metav1.ConditionTrue, Reason: "Test", Message: "m", LastTransitionTime: now}
	}
	for _, tc := range []struct {
		name   string
		mutate func(*v1alpha1.MysqlFailoverGroupStatus)
		want   []string
	}{
		{"sites duplicate", func(s *v1alpha1.MysqlFailoverGroupStatus) {
			s.Sites = []v1alpha1.SiteStatus{{Name: "dc1"}, {Name: "dc1"}}
		}, []string{"status.sites", "Duplicate value"}},
		{"sites empty name", func(s *v1alpha1.MysqlFailoverGroupStatus) {
			s.Sites = []v1alpha1.SiteStatus{{Name: ""}}
		}, []string{"status.sites", "name"}},
		{"conditions duplicate", func(s *v1alpha1.MysqlFailoverGroupStatus) {
			s.Conditions = []metav1.Condition{cond("Ready"), cond("Ready")}
		}, []string{"status.conditions", "Duplicate value"}},
		{"backupSchedules duplicate", func(s *v1alpha1.MysqlFailoverGroupStatus) {
			s.BackupSchedules = []v1alpha1.BackupScheduleStatus{{Name: "daily"}, {Name: "daily"}}
		}, []string{"status.backupSchedules", "Duplicate value"}},
		{"dragonfly sites duplicate", func(s *v1alpha1.MysqlFailoverGroupStatus) {
			s.Dragonfly = &v1alpha1.DragonflyStatus{Sites: []v1alpha1.DragonflySiteStatus{{Name: "dc1"}, {Name: "dc1"}}}
		}, []string{"status.dragonfly.sites", "Duplicate value"}},
		{"restoreReclones failures duplicate", func(s *v1alpha1.MysqlFailoverGroupStatus) {
			s.RestoreReclones = &v1alpha1.RestoreReclonesStatus{ConfirmToken: "2026-01-01T00:00:00Z", Phase: v1alpha1.RestoreReclonesDraining,
				Pending: []string{"dc2"}, Failures: []v1alpha1.RestoreRecloneFailure{{Site: "dc2", Message: "a", Time: now}, {Site: "dc2", Message: "b", Time: now}}}
		}, []string{"status.restoreReclones.failures", "Duplicate value"}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			cur := &v1alpha1.MysqlFailoverGroup{}
			if err := k8sClient.Get(ctx, client.ObjectKeyFromObject(fg), cur); err != nil {
				t.Fatal(err)
			}
			tc.mutate(&cur.Status)
			e4WantInvalid(t, tc.name, k8sClient.Status().Update(ctx, cur), tc.want...)
		})
	}
}

// A-07, A-08, A-10: two field managers co-own spec.sites,
// spec.extraContainers and status.conditions entry by entry under
// server-side apply; neither manager's apply removes the other's entry.
func TestE4_SSACoOwnership(t *testing.T) {
	ns := createNamespace(t, "e4-ssa")
	name := "shared"
	key := client.ObjectKey{Namespace: ns, Name: name}
	applyAs := func(manager string, u *unstructured.Unstructured) {
		t.Helper()
		if err := k8sClient.Apply(ctx, client.ApplyConfigurationFromUnstructured(u), client.FieldOwner(manager), client.ForceOwnership); err != nil {
			t.Fatalf("apply as %s: %v", manager, err)
		}
	}

	platform := newTestFG(ns)
	platform.Name = name
	platform.Spec.ExtraContainers = []corev1.Container{{Name: "exporter", Image: "exporter:1"}}
	applyAs("platform", e4Unstructured(t, platform, "MysqlFailoverGroup"))

	// Team adds a reader site and its own sidecar container.
	reader := v1alpha1.SiteSpec{Name: "reader", ServerID: 103, Role: v1alpha1.SiteRoleReadOnly, Zone: "reader-zone",
		Storage: v1alpha1.StorageSpec{StorageClassName: "standard", Size: platform.Spec.Sites[0].Storage.Size}}
	teamTyped := &v1alpha1.MysqlFailoverGroup{Spec: v1alpha1.MysqlFailoverGroupSpec{
		Sites:           []v1alpha1.SiteSpec{reader},
		ExtraContainers: []corev1.Container{{Name: "shipper", Image: "shipper:1"}},
	}}
	full := e4Unstructured(t, teamTyped, "MysqlFailoverGroup")
	team := &unstructured.Unstructured{Object: map[string]any{}}
	team.SetGroupVersionKind(mfgGVK)
	team.SetNamespace(ns)
	team.SetName(name)
	for _, f := range []string{"sites", "extraContainers"} {
		v, _, _ := unstructured.NestedSlice(full.Object, "spec", f)
		if err := unstructured.SetNestedSlice(team.Object, v, "spec", f); err != nil {
			t.Fatal(err)
		}
	}
	applyAs("team", team)

	// The platform re-applies its own config unchanged; the team's entries stay.
	applyAs("platform", e4Unstructured(t, platform, "MysqlFailoverGroup"))
	got := &v1alpha1.MysqlFailoverGroup{}
	if err := k8sClient.Get(ctx, key, got); err != nil {
		t.Fatal(err)
	}
	var siteNames, containerNames []string
	for _, s := range got.Spec.Sites {
		siteNames = append(siteNames, s.Name)
	}
	for _, c := range got.Spec.ExtraContainers {
		containerNames = append(containerNames, c.Name)
	}
	if strings.Join(siteNames, ",") != "dc1,dc2,reader" || strings.Join(containerNames, ",") != "exporter,shipper" {
		t.Fatalf("after both managers applied: sites %v, extraContainers %v", siteNames, containerNames)
	}

	// Status conditions: each manager owns its own condition type.
	statusApply := func(manager, condType string) {
		t.Helper()
		u := &unstructured.Unstructured{Object: map[string]any{}}
		u.SetGroupVersionKind(mfgGVK)
		u.SetNamespace(ns)
		u.SetName(name)
		c := map[string]any{"type": condType, "status": "True", "reason": "Applied", "message": manager,
			"lastTransitionTime": time.Now().UTC().Truncate(time.Second).Format(time.RFC3339)}
		if err := unstructured.SetNestedSlice(u.Object, []any{c}, "status", "conditions"); err != nil {
			t.Fatal(err)
		}
		if err := k8sClient.Status().Apply(ctx, client.ApplyConfigurationFromUnstructured(u), client.FieldOwner(manager), client.ForceOwnership); err != nil {
			t.Fatalf("status apply as %s: %v", manager, err)
		}
	}
	statusApply("operator", "Ready")
	statusApply("watcher", "Observed")
	statusApply("operator", "Ready")
	if err := k8sClient.Get(ctx, key, got); err != nil {
		t.Fatal(err)
	}
	if apimeta.FindStatusCondition(got.Status.Conditions, "Ready") == nil || apimeta.FindStatusCondition(got.Status.Conditions, "Observed") == nil {
		t.Fatalf("conditions after both managers applied: %+v", got.Status.Conditions)
	}
}

// e4StripRule removes the x-kubernetes-validations entry whose message
// contains fragment from the schema node at path, and reports whether one
// was removed.
func e4StripRule(root *apiextensionsv1.JSONSchemaProps, path []string, fragment string) bool {
	if len(path) > 0 {
		child, ok := root.Properties[path[0]]
		if !ok {
			return false
		}
		removed := e4StripRule(&child, path[1:], fragment)
		root.Properties[path[0]] = child
		return removed
	}
	kept := root.XValidations[:0]
	removed := false
	for _, r := range root.XValidations {
		if strings.Contains(r.Message, fragment) {
			removed = true
			continue
		}
		kept = append(kept, r)
	}
	root.XValidations = kept
	return removed
}

// A-115, C-15: each spec CEL rule E4 tightens (serverID uniqueness,
// sitePriorities uniqueness, at most one in-place includeSchemas entry) is
// checked against an object stored before the rule existed. A main-resource
// write that leaves the violating value unchanged is accepted (ratcheting,
// on by default since 1.30), and one that changes it is refused. A status
// write is accepted from 1.33, which ratchets status writes; on 1.31 and
// 1.32 it is refused, a break the v1.7.0 release notes call out (v1.7.0 has
// no in-place upgrade from v1.6.0).
func TestE4_TightenedSpecRulesOnStoredObjects(t *testing.T) {
	opts := &envtest.CRDInstallOptions{Paths: []string{filepath.Join("..", "..", "config", "crd", "bases")}}
	if err := envtest.ReadCRDFiles(opts); err != nil {
		t.Fatal(err)
	}
	rules := []struct {
		path     []string
		fragment string
	}{
		{[]string{"spec"}, "serverID must be unique"},
		{[]string{"spec", "splitBrainPolicy", "sitePriorities"}, "sitePriorities entries must be unique"},
		{[]string{"spec", "restoreInPlace"}, "includeSchemas supports at most one schema"},
	}
	var final *apiextensionsv1.CustomResourceDefinition
	crds := make([]*apiextensionsv1.CustomResourceDefinition, 0, len(opts.CRDs))
	for _, crd := range opts.CRDs {
		if crd.Name != "mysqlfailovergroups.shipstream.io" {
			crds = append(crds, crd)
			continue
		}
		final = crd.DeepCopy()
		loose := crd.DeepCopy()
		for _, r := range rules {
			if !e4StripRule(loose.Spec.Versions[0].Schema.OpenAPIV3Schema, r.path, r.fragment) {
				t.Fatalf("rule %q not found at %v", r.fragment, r.path)
			}
		}
		crds = append(crds, loose)
	}
	upgradeScheme := runtime.NewScheme()
	for _, add := range []func(*runtime.Scheme) error{clientgoscheme.AddToScheme, v1alpha1.AddToScheme, apiextensionsv1.AddToScheme} {
		if err := add(upgradeScheme); err != nil {
			t.Fatal(err)
		}
	}
	env := &envtest.Environment{CRDs: crds, Scheme: upgradeScheme}
	envCfg, err := env.Start()
	if err != nil {
		t.Fatalf("start an envtest without E4's spec rules: %v", err)
	}
	t.Cleanup(func() { _ = env.Stop() })
	c, err := client.New(envCfg, client.Options{Scheme: upgradeScheme})
	if err != nil {
		t.Fatal(err)
	}
	ns := "e4-tightened"
	if err := c.Create(ctx, &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: ns}}); err != nil {
		t.Fatal(err)
	}

	cases := []struct {
		name    string
		message string
		violate func(*v1alpha1.MysqlFailoverGroup)
		edit    func(*v1alpha1.MysqlFailoverGroup) // changes the violating value, still violating
	}{
		{"dup-server-id", "serverID must be unique",
			func(fg *v1alpha1.MysqlFailoverGroup) { fg.Spec.Sites[1].ServerID = fg.Spec.Sites[0].ServerID },
			func(fg *v1alpha1.MysqlFailoverGroup) {
				fg.Spec.Sites = append(fg.Spec.Sites, v1alpha1.SiteSpec{Name: "reader", ServerID: fg.Spec.Sites[0].ServerID, Role: v1alpha1.SiteRoleReadOnly,
					Zone: "reader-zone", Storage: fg.Spec.Sites[0].Storage})
			}},
		{"dup-priorities", "sitePriorities entries must be unique",
			func(fg *v1alpha1.MysqlFailoverGroup) {
				fg.Spec.SplitBrainPolicy = &v1alpha1.SplitBrainPolicySpec{SitePriorities: []string{"dc1", "dc1"}}
			},
			func(fg *v1alpha1.MysqlFailoverGroup) {
				fg.Spec.SplitBrainPolicy.SitePriorities = []string{"dc2", "dc2"}
			}},
		{"two-schemas", "includeSchemas supports at most one schema",
			func(fg *v1alpha1.MysqlFailoverGroup) {
				fg.Spec.RestoreInPlace = &v1alpha1.RestoreInPlaceSpec{Confirm: "2026-01-01T00:00:00Z",
					Source:      v1alpha1.RestoreSource{BackupRef: &v1alpha1.LocalMysqlBackupRef{Name: "b"}},
					LoadOptions: &v1alpha1.LoadOptions{IncludeSchemas: []string{"a", "b"}}}
			},
			func(fg *v1alpha1.MysqlFailoverGroup) {
				fg.Spec.RestoreInPlace.LoadOptions.IncludeSchemas = []string{"a", "c"}
			}},
	}
	for _, tc := range cases {
		fg := newTestFG(ns)
		fg.Name = tc.name
		tc.violate(fg)
		if err := c.Create(ctx, fg); err != nil {
			t.Fatalf("%s: store under the CRD without the rule: %v", tc.name, err)
		}
	}

	// Install the final CRD and wait until its rules are served.
	var cur apiextensionsv1.CustomResourceDefinition
	if err := c.Get(ctx, client.ObjectKey{Name: final.Name}, &cur); err != nil {
		t.Fatal(err)
	}
	cur.Spec = final.Spec
	if err := c.Update(ctx, &cur); err != nil {
		t.Fatalf("install the final CRD: %v", err)
	}
	deadline := time.Now().Add(30 * time.Second)
	for {
		probe := newTestFG(ns)
		probe.Name = "probe"
		cases[0].violate(probe)
		err := c.Create(ctx, probe)
		if err != nil && strings.Contains(err.Error(), cases[0].message) {
			break
		}
		if err == nil {
			_ = c.Delete(ctx, probe)
		}
		if time.Now().After(deadline) {
			t.Fatalf("the final CRD's rules are not served: %v", err)
		}
		time.Sleep(200 * time.Millisecond)
	}

	ratchets := statusWritesRatchet(t, envCfg)
	for _, tc := range cases {
		get := func() *v1alpha1.MysqlFailoverGroup {
			t.Helper()
			fg := &v1alpha1.MysqlFailoverGroup{}
			if err := c.Get(ctx, client.ObjectKey{Namespace: ns, Name: tc.name}, fg); err != nil {
				t.Fatal(err)
			}
			return fg
		}
		meta := get()
		meta.Annotations = map[string]string{"e4.example/touched": "true"}
		if err := c.Update(ctx, meta); err != nil {
			t.Errorf("%s: a main-resource write that keeps the stored value was refused: %v", tc.name, err)
		}
		edited := get()
		tc.edit(edited)
		if err := c.Update(ctx, edited); err == nil || !strings.Contains(err.Error(), tc.message) {
			t.Errorf("%s: an edit that changes the violating value: %v, want a refusal naming %q", tc.name, err, tc.message)
		}
		status := get()
		status.Status.TopologyGeneration++
		err := c.Status().Update(ctx, status)
		switch {
		case ratchets && err != nil:
			t.Errorf("%s: status write refused on a version that ratchets status writes: %v", tc.name, err)
		case !ratchets && (err == nil || !strings.Contains(err.Error(), tc.message)):
			t.Errorf("%s: status write without status ratcheting: %v, want a refusal naming %q (the documented floor break)", tc.name, err, tc.message)
		}
	}
}
