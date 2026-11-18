package controller

import (
	"context"
	"slices"
	"testing"

	v1beta1 "github.com/shipstream/bloodraven/api/v1beta1"
	"github.com/shipstream/bloodraven/internal/mysql"
	"github.com/shipstream/bloodraven/internal/testutil"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/equality"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
)

func TestStorageRestartRequestValidation(t *testing.T) {
	fg := newTestFG()
	site := fg.Spec.Sites[0]
	pending := sgSiteClaim(site.Name, site.Storage.StorageClassName, "100Gi", "50Gi")
	pending.OwnerReferences = []metav1.OwnerReference{*metav1.NewControllerRef(fg, v1beta1.GroupVersion.WithKind("MysqlFailoverGroup"))}
	pending.Status.Conditions = []corev1.PersistentVolumeClaimCondition{{Type: corev1.PersistentVolumeClaimFileSystemResizePending, Status: corev1.ConditionTrue}}
	token := storageRestartToken(site.Name, string(pending.UID), "100Gi")
	fg.Annotations = map[string]string{v1beta1.StorageRestartAnnotation: token}
	for _, tc := range []struct {
		name string
		edit func(*v1beta1.MysqlFailoverGroup, *corev1.PersistentVolumeClaim)
		want string
	}{
		{name: "filesystem pending", want: token},
		{name: "node pending", want: token, edit: func(_ *v1beta1.MysqlFailoverGroup, p *corev1.PersistentVolumeClaim) {
			p.Status.Conditions = nil
			p.Status.AllocatedResourceStatuses = map[corev1.ResourceName]corev1.ClaimResourceStatus{corev1.ResourceStorage: corev1.PersistentVolumeClaimNodeResizePending}
		}},
		{name: "online resize finished", edit: func(_ *v1beta1.MysqlFailoverGroup, p *corev1.PersistentVolumeClaim) { p.Status.Conditions = nil }},
		{name: "replaced claim", edit: func(_ *v1beta1.MysqlFailoverGroup, p *corev1.PersistentVolumeClaim) { p.UID = "new-claim" }},
		{name: "foreign owner", edit: func(_ *v1beta1.MysqlFailoverGroup, p *corev1.PersistentVolumeClaim) { p.OwnerReferences = nil }},
		{name: "new desired size", edit: func(g *v1beta1.MysqlFailoverGroup, _ *corev1.PersistentVolumeClaim) {
			g.Spec.Sites[0].Storage.Size = "101Gi"
		}},
		{name: "recreate", edit: func(g *v1beta1.MysqlFailoverGroup, _ *corev1.PersistentVolumeClaim) {
			g.Spec.UpdateStrategy.Type = v1beta1.UpdateStrategyRecreate
		}},
		{name: "no request", edit: func(g *v1beta1.MysqlFailoverGroup, _ *corev1.PersistentVolumeClaim) { g.Annotations = nil }},
		{name: "wrong size", edit: func(g *v1beta1.MysqlFailoverGroup, _ *corev1.PersistentVolumeClaim) {
			g.Annotations[v1beta1.StorageRestartAnnotation] = storageRestartToken(site.Name, string(pending.UID), "50Gi")
		}},
		{name: "node expanding", edit: func(_ *v1beta1.MysqlFailoverGroup, p *corev1.PersistentVolumeClaim) {
			p.Status.AllocatedResourceStatuses = map[corev1.ResourceName]corev1.ClaimResourceStatus{corev1.ResourceStorage: corev1.PersistentVolumeClaimNodeResizeInProgress}
		}},
		{name: "controller expanding", edit: func(_ *v1beta1.MysqlFailoverGroup, p *corev1.PersistentVolumeClaim) {
			p.Status.Conditions = append(p.Status.Conditions, corev1.PersistentVolumeClaimCondition{Type: corev1.PersistentVolumeClaimResizing, Status: corev1.ConditionTrue})
		}},
		{name: "controller failure", edit: func(_ *v1beta1.MysqlFailoverGroup, p *corev1.PersistentVolumeClaim) {
			p.Status.Conditions = append(p.Status.Conditions, corev1.PersistentVolumeClaimCondition{Type: corev1.PersistentVolumeClaimControllerResizeError, Status: corev1.ConditionTrue})
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			group, pvc := fg.DeepCopy(), pending.DeepCopy()
			if tc.edit != nil {
				tc.edit(group, pvc)
			}
			reader := fake.NewClientBuilder().WithScheme(testScheme()).WithObjects(pvc).Build()
			got, err := requestedStorageRestart(t.Context(), reader, group, group.Spec.Sites[0])
			if err != nil || got != tc.want {
				t.Errorf("restart token = %q, %v; want %q", got, err, tc.want)
			}
		})
	}
}

func TestStorageRestartOnlyOrderedWorkerChangesTemplate(t *testing.T) {
	ctx := t.Context()
	fg := newTestFG()
	site := fg.Spec.Sites[0]
	pvc := sgSiteClaim(site.Name, site.Storage.StorageClassName, "100Gi", "50Gi")
	pvc.Status.Conditions = []corev1.PersistentVolumeClaimCondition{{Type: corev1.PersistentVolumeClaimFileSystemResizePending, Status: corev1.ConditionTrue}}
	token := storageRestartToken(site.Name, string(pvc.UID), "100Gi")
	fg.Annotations = map[string]string{v1beta1.StorageRestartAnnotation: token}
	f := newSGFixture(t, fg, pvc)
	f.reconcile(t)
	read := func() *appsv1.Deployment {
		t.Helper()
		var dep appsv1.Deployment
		if err := f.c.Get(ctx, client.ObjectKey{Namespace: fg.Namespace, Name: resourceName(fg.Name, site.Name)}, &dep); err != nil {
			t.Fatal(err)
		}
		return &dep
	}
	before := read()
	key := v1beta1.StorageRestartAnnotation
	if before.Spec.Template.Annotations[key] != "" {
		t.Fatal("ordinary create applied the restart")
	}
	runner := &TopologyManagerRunner{client: f.c, logger: testLogger()}
	if sites, ok := runner.specDriftSites(ctx, f.c, f.group(t)); !ok || !slices.Equal(sites, []string{site.Name}) {
		t.Fatalf("drift=%v ok=%t", sites, ok)
	}
	if err := f.r.applySiteDeployment(ctx, client.ObjectKeyFromObject(fg), site.Name); err != nil {
		t.Fatal(err)
	}
	after := read()
	if after.Spec.Template.Annotations[key] != token || before.Annotations[v1beta1.SpecHashAnnotation] != after.Annotations[v1beta1.SpecHashAnnotation] {
		t.Fatalf("ordered render did not apply only the requested remount: annotations=%v", after.Spec.Template.Annotations)
	}
	if sites, ok := runner.specDriftSites(ctx, f.c, f.group(t)); !ok || len(sites) != 0 {
		t.Fatalf("lingering condition retriggered drift=%v ok=%t", sites, ok)
	}
	group := f.group(t)
	delete(group.Annotations, key)
	group.Spec.UpdateStrategy.Type = v1beta1.UpdateStrategyRecreate
	if err := f.c.Update(ctx, group); err != nil {
		t.Fatal(err)
	}
	f.reconcile(t)
	if !equality.Semantic.DeepEqual(after.Spec.Template, read().Spec.Template) {
		t.Fatal("clearing request or switching to Recreate rolled the site again")
	}
}

func TestCompletedOnlineResizeSkipsPrimaryHandoff(t *testing.T) {
	logger := testLogger()
	updater := NewUpdateController(NewFailoverController(logger), logger)
	checked := false
	updater.confirmActiveDrift = func(context.Context, string) (bool, error) { checked = true; return false, nil }
	primary := &testutil.FakeMySQL{}
	standby := &testutil.FakeMySQL{ReadOnlyVal: true, ReplicaStatusVal: &mysql.ReplicaStatus{IORunning: true, SQLRunning: true, SourceHost: "primary"}}
	_, err := updater.ExecuteTargets(t.Context(), UpdateTarget{Name: "primary", Checker: primary, Drifted: true},
		[]UpdateTarget{{Name: "standby", Host: "standby", ExpectedSource: "primary", Checker: standby, Promotable: true}},
		func(context.Context, string) error { t.Fatal("applied obsolete update"); return nil }, nil)
	if !checked || err != nil || primary.ReadOnlyVal || !standby.ReadOnlyVal {
		t.Fatalf("recheck=%v err=%v primaryReadOnly=%v standbyReadOnly=%v", checked, err, primary.ReadOnlyVal, standby.ReadOnlyVal)
	}
}
