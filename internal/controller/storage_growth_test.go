package controller

import (
	"context"
	"strings"
	"testing"
	"time"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	storagev1 "k8s.io/api/storage/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/tools/record"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"

	v1beta1 "github.com/shipstream/bloodraven/api/v1beta1"
	"github.com/shipstream/bloodraven/internal/state"
)

func sgClaim(name, request, capacity string, bound bool) *corev1.PersistentVolumeClaim {
	class := "fast"
	pvc := &corev1.PersistentVolumeClaim{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: "shared-lion", UID: types.UID(name + "-uid"), CreationTimestamp: metav1.NewTime(time.Unix(1700000000, 0))},
		Spec: corev1.PersistentVolumeClaimSpec{
			StorageClassName: &class,
			Resources: corev1.VolumeResourceRequirements{Requests: corev1.ResourceList{
				corev1.ResourceStorage: resource.MustParse(request),
			}},
		},
	}
	if bound {
		pvc.Status.Phase = corev1.ClaimBound
		if capacity != "" {
			pvc.Status.Capacity = corev1.ResourceList{corev1.ResourceStorage: resource.MustParse(capacity)}
		}
	} else {
		pvc.Status.Phase = corev1.ClaimPending
	}
	return pvc
}

func TestObserveSiteClaim(t *testing.T) {
	desired := resource.MustParse("100Gi")
	withCond := func(pvc *corev1.PersistentVolumeClaim, ct corev1.PersistentVolumeClaimConditionType, msg string) *corev1.PersistentVolumeClaim {
		pvc.Status.Conditions = append(pvc.Status.Conditions, corev1.PersistentVolumeClaimCondition{Type: ct, Status: corev1.ConditionTrue, Message: msg})
		return pvc
	}
	withResize := func(pvc *corev1.PersistentVolumeClaim, s corev1.ClaimResourceStatus) *corev1.PersistentVolumeClaim {
		pvc.Status.AllocatedResourceStatuses = map[corev1.ResourceName]corev1.ClaimResourceStatus{corev1.ResourceStorage: s}
		return pvc
	}
	deleting := sgClaim("c", "100Gi", "100Gi", true)
	now := metav1.Now()
	deleting.DeletionTimestamp = &now

	for _, tc := range []struct {
		name    string
		pvc     *corev1.PersistentVolumeClaim
		phase   v1beta1.SiteStoragePhase
		grow    bool
		message string
	}{
		{"unbound", sgClaim("c", "100Gi", "", false), v1beta1.SiteStoragePending, false, "not bound"},
		{"unbound and smaller is not grown", sgClaim("c", "50Gi", "", false), v1beta1.SiteStoragePending, false, "once it is bound"},
		{"deleting", deleting, v1beta1.SiteStoragePending, false, "replacement is created at 100Gi"},
		{"satisfied", sgClaim("c", "100Gi", "100Gi", true), v1beta1.SiteStorageSatisfied, false, ""},
		{"equal quantity in another unit", sgClaim("c", "102400Mi", "102400Mi", true), v1beta1.SiteStorageSatisfied, false, ""},
		{"capacity rounded up", sgClaim("c", "100Gi", "101Gi", true), v1beta1.SiteStorageSatisfied, false, ""},
		{"smaller needs growth", sgClaim("c", "50Gi", "50Gi", true), v1beta1.SiteStorageExpanding, true, ""},
		{"larger manual claim is underspecified", sgClaim("c", "200Gi", "200Gi", true), v1beta1.SiteStorageUnderspecified, false, "raise the size to at least 200Gi"},
		{"larger and still expanding is underspecified", sgClaim("c", "200Gi", "100Gi", true), v1beta1.SiteStorageUnderspecified, false, "200Gi"},
		{"controller expanding", sgClaim("c", "100Gi", "50Gi", true), v1beta1.SiteStorageExpanding, false, "expanding"},
		{"controller resize error is quoted", withCond(sgClaim("c", "100Gi", "50Gi", true), corev1.PersistentVolumeClaimControllerResizeError, "quota"), v1beta1.SiteStorageFailed, false, "ControllerResizeError: quota"},
		{"filesystem pending with resize still running", withCond(withCond(sgClaim("c", "100Gi", "50Gi", true), corev1.PersistentVolumeClaimFileSystemResizePending, ""), corev1.PersistentVolumeClaimResizing, ""), v1beta1.SiteStorageExpanding, false, "expanding"},
		{"filesystem pending with node failure", withCond(withCond(sgClaim("c", "100Gi", "50Gi", true), corev1.PersistentVolumeClaimFileSystemResizePending, ""), corev1.PersistentVolumeClaimNodeResizeError, "device failure"), v1beta1.SiteStorageFailed, false, "device failure"},
		{"file system resize pending", withCond(sgClaim("c", "100Gi", "100Gi", true), corev1.PersistentVolumeClaimFileSystemResizePending, ""), v1beta1.SiteStorageFileSystemResizePending, false, "restart"},
		{"node resize pending", withResize(sgClaim("c", "100Gi", "100Gi", true), corev1.PersistentVolumeClaimNodeResizePending), v1beta1.SiteStorageFileSystemResizePending, false, ""},
		{"node resize in progress", withResize(sgClaim("c", "100Gi", "100Gi", true), corev1.PersistentVolumeClaimNodeResizeInProgress), v1beta1.SiteStorageExpanding, false, ""},
		{"infeasible is failed and not re-requested", withResize(sgClaim("c", "50Gi", "50Gi", true), corev1.PersistentVolumeClaimControllerResizeInfeasible), v1beta1.SiteStorageFailed, false, "infeasible"},
		{"node infeasible", withResize(sgClaim("c", "100Gi", "50Gi", true), corev1.PersistentVolumeClaimNodeResizeInfeasible), v1beta1.SiteStorageFailed, false, "NodeResizeInfeasible"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			st, grow := observeSiteClaim("dc1", "100Gi", desired, tc.pvc, nil)
			if st.Phase != tc.phase || grow != tc.grow {
				t.Fatalf("phase, grow = %s, %v, want %s, %v (%s)", st.Phase, grow, tc.phase, tc.grow, st.Message)
			}
			if tc.message != "" && !strings.Contains(st.Message, tc.message) {
				t.Fatalf("message %q does not contain %q", st.Message, tc.message)
			}
			if st.ClaimName != "c" || st.ClaimUID != "c-uid" || st.StorageClassName != "fast" || st.DesiredSize != "100Gi" {
				t.Fatalf("identity fields = %+v", st)
			}
		})
	}
}

func TestSiteFileSystemResizePending(t *testing.T) {
	pvc := sgClaim("c", "100Gi", "100Gi", true)
	if p, _ := siteFileSystemResizePending(pvc); p {
		t.Fatal("a settled claim reads pending")
	}
	since := metav1.NewTime(time.Unix(1700000000, 0))
	pvc.Status.Conditions = []corev1.PersistentVolumeClaimCondition{{Type: corev1.PersistentVolumeClaimFileSystemResizePending, Status: corev1.ConditionTrue, LastTransitionTime: since}}
	if p, s := siteFileSystemResizePending(pvc); !p || !s.Equal(since.Time) {
		t.Fatalf("pending, since = %v, %v", p, s)
	}
	pvc.Status.Phase = corev1.ClaimPending
	if p, _ := siteFileSystemResizePending(pvc); p {
		t.Fatal("an unbound claim reads pending")
	}
}

// sgFixture is a group of two sites whose claims exist bound, with the
// classes given, and a reconciler on a fake client that counts writes.
type sgFixture struct {
	r              *MysqlFailoverGroupReconciler
	c              client.Client
	rec            *record.FakeRecorder
	pvcPatches     map[string]int
	classGets      int
	statusUpdates  int
	failClassGet   map[string]error
	failClaimPatch map[string]error
}

func newSGFixture(t *testing.T, fg *v1beta1.MysqlFailoverGroup, objs ...client.Object) *sgFixture {
	t.Helper()
	f := &sgFixture{pvcPatches: map[string]int{}, failClassGet: map[string]error{}, failClaimPatch: map[string]error{}}
	scheme := testScheme()
	all := withTestPassphraseSecrets(append([]client.Object{newTestSecret(), fg}, objs...))
	f.c = fake.NewClientBuilder().WithScheme(scheme).
		WithStatusSubresource(&v1beta1.MysqlFailoverGroup{}, &corev1.PersistentVolumeClaim{}).
		WithObjects(all...).
		WithInterceptorFuncs(interceptor.Funcs{
			Get: func(ctx context.Context, c client.WithWatch, key client.ObjectKey, obj client.Object, opts ...client.GetOption) error {
				if _, ok := obj.(*storagev1.StorageClass); ok {
					f.classGets++
					if err := f.failClassGet[key.Name]; err != nil {
						return err
					}
				}
				return c.Get(ctx, key, obj, opts...)
			},
			Patch: func(ctx context.Context, c client.WithWatch, obj client.Object, patch client.Patch, opts ...client.PatchOption) error {
				if pvc, ok := obj.(*corev1.PersistentVolumeClaim); ok {
					if err := f.failClaimPatch[pvc.Name]; err != nil {
						return err
					}
					f.pvcPatches[pvc.Name]++
				}
				return c.Patch(ctx, obj, patch, opts...)
			},
			SubResourceUpdate: func(ctx context.Context, c client.Client, sub string, obj client.Object, opts ...client.SubResourceUpdateOption) error {
				if _, ok := obj.(*v1beta1.MysqlFailoverGroup); ok && sub == "status" {
					f.statusUpdates++
				}
				return c.SubResource(sub).Update(ctx, obj, opts...)
			},
		}).Build()
	f.rec = record.NewFakeRecorder(100)
	f.r = &MysqlFailoverGroupReconciler{Client: f.c, Scheme: scheme, Recorder: f.rec}
	return f
}

func (f *sgFixture) reconcile(t *testing.T) ctrl.Result {
	t.Helper()
	res, err := f.r.Reconcile(context.Background(), ctrl.Request{NamespacedName: types.NamespacedName{Name: "lion", Namespace: "shared-lion"}})
	if err != nil {
		t.Fatalf("reconcile: %v", err)
	}
	return res
}

func (f *sgFixture) group(t *testing.T) *v1beta1.MysqlFailoverGroup {
	t.Helper()
	var fg v1beta1.MysqlFailoverGroup
	if err := f.c.Get(context.Background(), types.NamespacedName{Name: "lion", Namespace: "shared-lion"}, &fg); err != nil {
		t.Fatal(err)
	}
	return &fg
}

func (f *sgFixture) claim(t *testing.T, site string) *corev1.PersistentVolumeClaim {
	t.Helper()
	var pvc corev1.PersistentVolumeClaim
	if err := f.c.Get(context.Background(), types.NamespacedName{Name: "mysql-lion-" + site + "-data", Namespace: "shared-lion"}, &pvc); err != nil {
		t.Fatal(err)
	}
	return &pvc
}

func (f *sgFixture) events() []string {
	var out []string
	for {
		select {
		case e := <-f.rec.Events:
			out = append(out, e)
		default:
			return out
		}
	}
}

func sgSiteClaim(site, class, request, capacity string) *corev1.PersistentVolumeClaim {
	pvc := sgClaim("mysql-lion-"+site+"-data", request, capacity, true)
	pvc.Spec.StorageClassName = &class
	pvc.Labels = commonLabels("lion", site)
	return pvc
}

func sgClass(name string, allow *bool) *storagev1.StorageClass {
	return &storagev1.StorageClass{ObjectMeta: metav1.ObjectMeta{Name: name}, Provisioner: "csi.example.com", AllowVolumeExpansion: allow}
}

func sgPhase(t *testing.T, fg *v1beta1.MysqlFailoverGroup, site string) v1beta1.SiteStorageStatus {
	t.Helper()
	s := fg.Status.SiteStorage(site)
	if s == nil {
		t.Fatalf("no status.storage entry for %s: %+v", site, fg.Status.Storage)
	}
	return *s
}

func TestReconcilePVC_MissingClaimCreatedAtDesiredSizeAndStatusWrittenOnce(t *testing.T) {
	fg := newTestFG()
	fg.Spec.Sites[0].Storage.Size = "150Gi"
	f := newSGFixture(t, fg)
	f.reconcile(t)
	pvc := f.claim(t, "dc1")
	if q := pvc.Spec.Resources.Requests[corev1.ResourceStorage]; q.Cmp(resource.MustParse("150Gi")) != 0 {
		t.Fatalf("created request = %s, want 150Gi", q.String())
	}
	got := f.group(t)
	st := sgPhase(t, got, "dc1")
	if st.Phase != v1beta1.SiteStoragePending || st.DesiredSize != "150Gi" || st.RequestedSize != "150Gi" || st.ClaimUID != pvc.UID || st.LastTransitionTime == nil {
		t.Fatalf("dc1 storage = %+v", st)
	}
	if f.classGets != 0 {
		t.Fatalf("a new claim read its StorageClass %d times", f.classGets)
	}
	writes := f.statusUpdates
	f.reconcile(t)
	if f.statusUpdates != writes {
		t.Fatalf("an unchanged observation wrote status again (%d -> %d)", writes, f.statusUpdates)
	}
}

func TestReconcilePVC_GrowsBoundClaimOnExpandableClassOnce(t *testing.T) {
	fg := newTestFG()
	fg.Spec.Sites[0].Storage.Size = "200Gi"
	allow := true
	f := newSGFixture(t, fg,
		sgSiteClaim("dc1", "local-dc1", "100Gi", "100Gi"),
		sgSiteClaim("dc2", "local-dc2", "100Gi", "100Gi"),
		sgClass("local-dc1", &allow))
	f.reconcile(t)
	if q := f.claim(t, "dc1").Spec.Resources.Requests[corev1.ResourceStorage]; q.Cmp(resource.MustParse("200Gi")) != 0 {
		t.Fatalf("dc1 request = %s, want 200Gi", q.String())
	}
	if f.pvcPatches["mysql-lion-dc1-data"] != 1 || f.pvcPatches["mysql-lion-dc2-data"] != 0 {
		t.Fatalf("claim patches = %v", f.pvcPatches)
	}
	got := f.group(t)
	if st := sgPhase(t, got, "dc1"); st.Phase != v1beta1.SiteStorageExpanding || st.RequestedSize != "200Gi" || st.Capacity != "100Gi" {
		t.Fatalf("dc1 storage = %+v", st)
	}
	if st := sgPhase(t, got, "dc2"); st.Phase != v1beta1.SiteStorageSatisfied {
		t.Fatalf("dc2 storage = %+v", st)
	}
	if ev := strings.Join(f.events(), "\n"); !strings.Contains(ev, v1beta1.EventStorageExpansionRequested) {
		t.Fatalf("no %s event: %s", v1beta1.EventStorageExpansionRequested, ev)
	}

	// Repeated passes do not raise it again.
	f.reconcile(t)
	f.reconcile(t)
	if f.pvcPatches["mysql-lion-dc1-data"] != 1 {
		t.Fatalf("claim patched %d times, want once", f.pvcPatches["mysql-lion-dc1-data"])
	}

	// The driver finishes, the file system waits for the node: reported
	// for the restart coordination, nothing else written to the claim.
	pvc := f.claim(t, "dc1")
	pvc.Status.Capacity = corev1.ResourceList{corev1.ResourceStorage: resource.MustParse("200Gi")}
	pvc.Status.Conditions = []corev1.PersistentVolumeClaimCondition{{Type: corev1.PersistentVolumeClaimFileSystemResizePending, Status: corev1.ConditionTrue}}
	if err := f.c.Status().Update(context.Background(), pvc); err != nil {
		t.Fatal(err)
	}
	f.reconcile(t)
	if st := sgPhase(t, f.group(t), "dc1"); st.Phase != v1beta1.SiteStorageFileSystemResizePending || st.Capacity != "200Gi" {
		t.Fatalf("dc1 storage = %+v", st)
	}
	pvc = f.claim(t, "dc1")
	pvc.Status.Conditions = nil
	if err := f.c.Status().Update(context.Background(), pvc); err != nil {
		t.Fatal(err)
	}
	f.reconcile(t)
	if st := sgPhase(t, f.group(t), "dc1"); st.Phase != v1beta1.SiteStorageSatisfied {
		t.Fatalf("dc1 storage = %+v", st)
	}
	if f.pvcPatches["mysql-lion-dc1-data"] != 1 {
		t.Fatalf("claim patched %d times, want once", f.pvcPatches["mysql-lion-dc1-data"])
	}
}

func TestReconcilePVC_UnsupportedClassIsReportedAndDoesNotBlock(t *testing.T) {
	no := false
	for _, tc := range []struct {
		name  string
		class *storagev1.StorageClass
		want  string
	}{
		{"expansion not allowed", sgClass("local-dc1", &no), "does not allow volume expansion"},
		{"expansion unset", sgClass("local-dc1", nil), "does not allow volume expansion"},
		{"class gone", nil, "does not exist"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			fg := newTestFG()
			fg.Spec.Sites[0].Storage.Size = "200Gi"
			fg.Spec.Sites[1].Storage.Size = "200Gi"
			allow := true
			objs := []client.Object{sgSiteClaim("dc1", "local-dc1", "100Gi", "100Gi"), sgSiteClaim("dc2", "local-dc2", "100Gi", "100Gi"), sgClass("local-dc2", &allow)}
			if tc.class != nil {
				objs = append(objs, tc.class)
			}
			f := newSGFixture(t, fg, objs...)
			res := f.reconcile(t)
			if f.pvcPatches["mysql-lion-dc1-data"] != 0 {
				t.Fatal("a claim on a class without expansion was patched")
			}
			// The other site grows, and the site's workload is still reconciled.
			if f.pvcPatches["mysql-lion-dc2-data"] != 1 {
				t.Fatalf("dc2 patches = %d, want 1", f.pvcPatches["mysql-lion-dc2-data"])
			}
			if err := f.c.Get(context.Background(), types.NamespacedName{Name: "mysql-lion-dc1", Namespace: "shared-lion"}, &appsv1.Deployment{}); err != nil {
				t.Fatalf("dc1 Deployment not reconciled: %v", err)
			}
			st := sgPhase(t, f.group(t), "dc1")
			if st.Phase != v1beta1.SiteStorageUnsupported || !strings.Contains(st.Message, tc.want) || st.RequestedSize != "100Gi" {
				t.Fatalf("dc1 storage = %+v", st)
			}
			if res.RequeueAfter <= 0 || res.RequeueAfter > storageRecheckInterval {
				t.Fatalf("requeue = %v, want a recheck", res.RequeueAfter)
			}
			countUnsupported := func() int {
				n := 0
				for _, e := range f.events() {
					if strings.Contains(e, v1beta1.EventStorageExpansionUnsupported) {
						n++
					}
				}
				return n
			}
			if n := countUnsupported(); n != 1 {
				t.Fatalf("%d %s events, want 1", n, v1beta1.EventStorageExpansionUnsupported)
			}
			gets := f.classGets
			f.reconcile(t)
			if n := countUnsupported(); n != 0 {
				t.Fatalf("the unchanged phase recorded %d more events", n)
			}
			if f.classGets != gets {
				t.Fatalf("the class was read again inside its TTL (%d -> %d)", gets, f.classGets)
			}
		})
	}
}

func TestReconcilePVC_ClaimWithoutClassIsUnsupported(t *testing.T) {
	fg := newTestFG()
	fg.Spec.Sites[0].Storage.Size = "200Gi"
	pvc := sgSiteClaim("dc1", "", "100Gi", "100Gi")
	pvc.Spec.StorageClassName = nil
	f := newSGFixture(t, fg, pvc, sgSiteClaim("dc2", "local-dc2", "100Gi", "100Gi"))
	f.reconcile(t)
	if st := sgPhase(t, f.group(t), "dc1"); st.Phase != v1beta1.SiteStorageUnsupported || !strings.Contains(st.Message, "no StorageClass") {
		t.Fatalf("dc1 storage = %+v", st)
	}
	if f.classGets != 0 || f.pvcPatches["mysql-lion-dc1-data"] != 0 {
		t.Fatalf("class gets %d, patches %v", f.classGets, f.pvcPatches)
	}
}

// The live claim's class decides, not the spec's (which is immutable but
// may differ from a claim the group adopted).
func TestReconcilePVC_UsesTheClaimsOwnClass(t *testing.T) {
	fg := newTestFG()
	fg.Spec.Sites[0].Storage.Size = "200Gi"
	allow, no := true, false
	f := newSGFixture(t, fg,
		sgSiteClaim("dc1", "actual", "100Gi", "100Gi"), sgSiteClaim("dc2", "local-dc2", "100Gi", "100Gi"),
		sgClass("local-dc1", &allow), sgClass("actual", &no))
	f.reconcile(t)
	if st := sgPhase(t, f.group(t), "dc1"); st.Phase != v1beta1.SiteStorageUnsupported || st.StorageClassName != "actual" {
		t.Fatalf("dc1 storage = %+v", st)
	}
}

func TestReconcilePVC_LargerManualClaimIsKeptAndThenMatched(t *testing.T) {
	fg := newTestFG()
	allow := true
	f := newSGFixture(t, fg,
		sgSiteClaim("dc1", "local-dc1", "300Gi", "300Gi"), sgSiteClaim("dc2", "local-dc2", "100Gi", "100Gi"),
		sgClass("local-dc1", &allow))
	f.reconcile(t)
	if q := f.claim(t, "dc1").Spec.Resources.Requests[corev1.ResourceStorage]; q.Cmp(resource.MustParse("300Gi")) != 0 {
		t.Fatalf("dc1 request = %s, want the manual 300Gi kept", q.String())
	}
	st := sgPhase(t, f.group(t), "dc1")
	if st.Phase != v1beta1.SiteStorageUnderspecified || !strings.Contains(st.Message, "at least 300Gi") {
		t.Fatalf("dc1 storage = %+v", st)
	}
	if ev := strings.Join(f.events(), "\n"); !strings.Contains(ev, v1beta1.EventStorageSizeUnderspecified) {
		t.Fatalf("no %s event: %s", v1beta1.EventStorageSizeUnderspecified, ev)
	}

	// The administrator raises the spec to the claim's size (as 307200Mi).
	live := f.group(t)
	live.Spec.Sites[0].Storage.Size = "307200Mi"
	live.Generation++
	if err := f.c.Update(context.Background(), live); err != nil {
		t.Fatal(err)
	}
	f.reconcile(t)
	if st := sgPhase(t, f.group(t), "dc1"); st.Phase != v1beta1.SiteStorageSatisfied || st.DesiredSize != "307200Mi" {
		t.Fatalf("dc1 storage = %+v", st)
	}
	if f.pvcPatches["mysql-lion-dc1-data"] != 0 || f.classGets != 0 {
		t.Fatalf("matching the claim patched %v and read the class %d times", f.pvcPatches, f.classGets)
	}
}

func TestReconcilePVC_TransientAndRefusedFailuresAreReportedPerSite(t *testing.T) {
	fg := newTestFG()
	fg.Spec.Sites[0].Storage.Size = "200Gi"
	fg.Spec.Sites[1].Storage.Size = "200Gi"
	allow := true
	f := newSGFixture(t, fg,
		sgSiteClaim("dc1", "local-dc1", "100Gi", "100Gi"), sgSiteClaim("dc2", "local-dc2", "100Gi", "100Gi"),
		sgClass("local-dc1", &allow), sgClass("local-dc2", &allow))
	f.failClassGet["local-dc1"] = apierrors.NewServiceUnavailable("apiserver restarting")
	f.failClaimPatch["mysql-lion-dc2-data"] = apierrors.NewForbidden(schema.GroupResource{Resource: "persistentvolumeclaims"}, "mysql-lion-dc2-data", stringError("exceeded quota"))
	res := f.reconcile(t)
	got := f.group(t)
	if st := sgPhase(t, got, "dc1"); st.Phase != v1beta1.SiteStorageRetrying || !strings.Contains(st.Message, "local-dc1") {
		t.Fatalf("dc1 storage = %+v", st)
	}
	if st := sgPhase(t, got, "dc2"); st.Phase != v1beta1.SiteStorageFailed || !strings.Contains(st.Message, "exceeded quota") {
		t.Fatalf("dc2 storage = %+v", st)
	}
	if res.RequeueAfter <= 0 || res.RequeueAfter > storageRetryInterval {
		t.Fatalf("requeue = %v, want the transient retry", res.RequeueAfter)
	}
	for _, site := range []string{"dc1", "dc2"} {
		if err := f.c.Get(context.Background(), types.NamespacedName{Name: "mysql-lion-" + site, Namespace: "shared-lion"}, &appsv1.Deployment{}); err != nil {
			t.Fatalf("%s Deployment not reconciled: %v", site, err)
		}
	}
	if q := f.claim(t, "dc2").Spec.Resources.Requests[corev1.ResourceStorage]; q.Cmp(resource.MustParse("100Gi")) != 0 {
		t.Fatalf("refused claim request = %s", q.String())
	}

	// Both recover: the transient error was not cached as a verdict.
	delete(f.failClassGet, "local-dc1")
	delete(f.failClaimPatch, "mysql-lion-dc2-data")
	f.r.Now = func() time.Time { return time.Now().Add(2 * storageClassVerdictTTL) }
	f.reconcile(t)
	got = f.group(t)
	for _, site := range []string{"dc1", "dc2"} {
		if st := sgPhase(t, got, site); st.Phase != v1beta1.SiteStorageExpanding || st.RequestedSize != "200Gi" {
			t.Fatalf("%s storage = %+v", site, st)
		}
	}
}

// A request computed from a stale claim never lands over a newer one:
// the claim patch carries the resourceVersion it was computed from.
func TestReconcilePVC_GrowthPatchIsOptimisticallyLocked(t *testing.T) {
	fg := newTestFG()
	fg.Spec.Sites[0].Storage.Size = "200Gi"
	allow := true
	f := newSGFixture(t, fg, sgSiteClaim("dc1", "local-dc1", "100Gi", "100Gi"), sgSiteClaim("dc2", "local-dc2", "100Gi", "100Gi"), sgClass("local-dc1", &allow))
	pvc := f.claim(t, "dc1")
	stale := pvc.DeepCopy()
	pvc.Spec.Resources.Requests[corev1.ResourceStorage] = resource.MustParse("500Gi")
	if err := f.c.Update(context.Background(), pvc); err != nil {
		t.Fatal(err)
	}
	st, _ := observeSiteClaim("dc1", "200Gi", resource.MustParse("200Gi"), stale, nil)
	res := f.r.growSiteClaim(context.Background(), fg, st, resource.MustParse("200Gi"), stale)
	// The conflict is answered from the stored claim, which already
	// requests more than the spec.
	if res.status.Phase != v1beta1.SiteStorageUnderspecified || res.status.RequestedSize != "500Gi" {
		t.Fatalf("stale growth = %+v, want the stored claim's Underspecified", res.status)
	}
	if q := f.claim(t, "dc1").Spec.Resources.Requests[corev1.ResourceStorage]; q.Cmp(resource.MustParse("500Gi")) != 0 {
		t.Fatalf("the newer 500Gi request was replaced by %s", q.String())
	}
	if q := stale.Spec.Resources.Requests[corev1.ResourceStorage]; q.Cmp(resource.MustParse("100Gi")) != 0 {
		t.Fatalf("the refused patch left %s on the in-memory claim", q.String())
	}
}

// A pass made against an older spec does not write: the reconcile the
// newer spec triggers writes against it.
func TestWriteStorageStatus_SkipsAStaleGeneration(t *testing.T) {
	fg := newTestFG()
	fg.Generation = 2
	f := newSGFixture(t, fg)
	stale := f.group(t)
	stale.Generation = 1
	if err := f.r.writeStorageStatus(context.Background(), stale, []v1beta1.SiteStorageStatus{{Name: "dc1", Phase: v1beta1.SiteStorageSatisfied}}); err != nil {
		t.Fatal(err)
	}
	if f.statusUpdates != 0 || f.group(t).Status.Storage != nil {
		t.Fatalf("a stale pass wrote status.storage (%d writes)", f.statusUpdates)
	}
}

// A CRD older than the operator prunes status.storage: the writer stops
// writing it for a while instead of writing on every pass.
func TestWriteStorageStatus_PrunedFieldIsNotRewrittenEveryPass(t *testing.T) {
	fg := newTestFG()
	f := newSGFixture(t, fg)
	// Stand in for the pruning API server.
	f.c = interceptor.NewClient(f.c.(client.WithWatch), interceptor.Funcs{
		SubResourceUpdate: func(ctx context.Context, c client.Client, sub string, obj client.Object, opts ...client.SubResourceUpdateOption) error {
			if g, ok := obj.(*v1beta1.MysqlFailoverGroup); ok {
				g.Status.Storage = nil
			}
			return c.SubResource(sub).Update(ctx, obj, opts...)
		},
	})
	f.r.Client = f.c
	sites := []v1beta1.SiteStorageStatus{{Name: "dc1", Phase: v1beta1.SiteStorageSatisfied}}
	if err := f.r.writeStorageStatus(context.Background(), f.group(t), sites); err != nil {
		t.Fatal(err)
	}
	writes := f.statusUpdates
	if err := f.r.writeStorageStatus(context.Background(), f.group(t), sites); err != nil {
		t.Fatal(err)
	}
	if f.statusUpdates != writes {
		t.Fatalf("a pruned status.storage was written again (%d -> %d)", writes, f.statusUpdates)
	}
}

// The transition time moves only with the phase.
func TestBuildStorageStatusKeepsTransitionTime(t *testing.T) {
	then := metav1.NewTime(time.Unix(1700000000, 0))
	live := &v1beta1.MysqlFailoverGroupStatus{Storage: &v1beta1.StorageStatus{Sites: []v1beta1.SiteStorageStatus{
		{Name: "dc1", Phase: v1beta1.SiteStorageExpanding, LastTransitionTime: &then},
		{Name: "dc2", Phase: v1beta1.SiteStorageExpanding, LastTransitionTime: &then},
	}}}
	now := metav1.NewTime(time.Unix(1700000600, 0))
	got := buildStorageStatus(live, []v1beta1.SiteStorageStatus{
		{Name: "dc1", Phase: v1beta1.SiteStorageExpanding, Message: "new message"},
		{Name: "dc2", Phase: v1beta1.SiteStorageSatisfied},
	}, now)
	if !got.Sites[0].LastTransitionTime.Equal(&then) || !got.Sites[1].LastTransitionTime.Equal(&now) {
		t.Fatalf("transition times = %v, %v", got.Sites[0].LastTransitionTime, got.Sites[1].LastTransitionTime)
	}
}

// The topology runner's wholesale status write keeps the live
// status.storage, never the older copy its snapshot was built on.
func TestRunnerStatusPreservesStorageStatus(t *testing.T) {
	ctx := context.Background()
	fg := pitrRolloutFG(false)
	fg.Status.ActiveSite = "dc1"
	fg.Status.Storage = &v1beta1.StorageStatus{Sites: []v1beta1.SiteStorageStatus{{Name: "dc1", Phase: v1beta1.SiteStorageSatisfied, RequestedSize: "200Gi"}}}
	gets := 0
	c := fake.NewClientBuilder().WithScheme(testScheme()).WithObjects(fg.DeepCopy()).WithStatusSubresource(&v1beta1.MysqlFailoverGroup{}).
		WithInterceptorFuncs(interceptor.Funcs{
			Get: func(ctx context.Context, c client.WithWatch, key client.ObjectKey, obj client.Object, opts ...client.GetOption) error {
				if err := c.Get(ctx, key, obj, opts...); err != nil {
					return err
				}
				if g, ok := obj.(*v1beta1.MysqlFailoverGroup); ok {
					gets++
					if gets == 1 {
						// The first read is the stale copy: still expanding.
						g.Status.Storage.Sites[0].Phase = v1beta1.SiteStorageExpanding
						g.Status.Storage.Sites[0].RequestedSize = "100Gi"
					}
				}
				return nil
			},
		}).Build()
	r := &TopologyManagerRunner{client: c, logger: testLogger(), managers: map[types.NamespacedName]*managedTopology{}}
	nn := types.NamespacedName{Namespace: fg.Namespace, Name: fg.Name}
	snap := TopologySnapshot{Sites: []SiteSnapshot{{Name: "dc1", State: state.StateWritable}, {Name: "dc2", State: state.StateReadOnly}}, ActiveSite: "dc1"}
	if err := r.updateCRStatus(ctx, nn, snap); err != nil {
		t.Fatal(err)
	}
	if gets < 2 {
		t.Fatalf("the stale read was not staged (%d reads)", gets)
	}
	var got v1beta1.MysqlFailoverGroup
	if err := c.Get(ctx, nn, &got); err != nil {
		t.Fatal(err)
	}
	if st := got.Status.SiteStorage("dc1"); st == nil || st.Phase != v1beta1.SiteStorageSatisfied || st.RequestedSize != "200Gi" {
		t.Fatalf("status.storage after the runner's write = %+v", got.Status.Storage)
	}
}

type stringError string

func (e stringError) Error() string { return string(e) }

func TestMissingPVCRefusesStaleGroupAtCreation(t *testing.T) {
	stale := newTestFG()
	stale.Generation = 1
	live := stale.DeepCopy()
	live.Generation++
	live.Spec.Sites[0].Storage.Size = "200Gi"
	r, c := newReconciler(stale)
	r.APIReader = fake.NewClientBuilder().WithScheme(testScheme()).WithObjects(live).Build()
	site := stale.Spec.Sites[0]
	if _, err := r.reconcilePVC(t.Context(), stale, site); err == nil {
		t.Fatal("stale reconcile created the lost claim")
	}
	var pvc corev1.PersistentVolumeClaim
	key := client.ObjectKey{Namespace: stale.Namespace, Name: resourceName(stale.Name, site.Name) + "-data"}
	if err := c.Get(t.Context(), key, &pvc); !apierrors.IsNotFound(err) {
		t.Fatalf("stale pass left a PVC: %v", err)
	}
	if _, err := r.reconcilePVC(t.Context(), live, live.Spec.Sites[0]); err != nil {
		t.Fatal(err)
	}
	if err := c.Get(t.Context(), key, &pvc); err != nil {
		t.Fatal(err)
	}
	if got := pvc.Spec.Resources.Requests.Storage().String(); got != "200Gi" {
		t.Errorf("replacement requests %s", got)
	}
}

// The operator's own patch can be followed by a pass on a cached copy of
// the claim from before it: the conflict that pass gets is answered from
// the stored claim, not reported as a retry.
func TestGrowSiteClaim_ConflictAfterOwnPatchReportsTheStoredClaim(t *testing.T) {
	fg := newTestFG()
	fg.Spec.Sites[0].Storage.Size = "200Gi"
	allow := true
	f := newSGFixture(t, fg, sgSiteClaim("dc1", "local-dc1", "100Gi", "100Gi"), sgSiteClaim("dc2", "local-dc2", "100Gi", "100Gi"), sgClass("local-dc1", &allow))
	pvc := f.claim(t, "dc1")
	stale := pvc.DeepCopy()
	pvc.Spec.Resources.Requests[corev1.ResourceStorage] = resource.MustParse("200Gi")
	if err := f.c.Update(context.Background(), pvc); err != nil {
		t.Fatal(err)
	}
	st, _ := observeSiteClaim("dc1", "200Gi", resource.MustParse("200Gi"), stale, nil)
	res := f.r.growSiteClaim(context.Background(), fg, st, resource.MustParse("200Gi"), stale)
	if res.status.Phase != v1beta1.SiteStorageExpanding || res.status.RequestedSize != "200Gi" || res.requeue != 0 {
		t.Fatalf("growth after the own patch = %+v (requeue %v), want Expanding at 200Gi", res.status, res.requeue)
	}
}

// An administrator who lowers the claim's request after a failed
// expansion (the Kubernetes recovery) is not undone: the operator stops
// growing the claim to that size until the spec asks for more.
func TestReconcilePVC_LoweredRequestIsNotRaisedAgain(t *testing.T) {
	fg := newTestFG()
	fg.Spec.Sites[0].Storage.Size = "200Gi"
	allow := true
	f := newSGFixture(t, fg,
		sgSiteClaim("dc1", "local-dc1", "100Gi", "100Gi"), sgSiteClaim("dc2", "local-dc2", "100Gi", "100Gi"),
		sgClass("local-dc1", &allow))
	f.reconcile(t)
	if f.pvcPatches["mysql-lion-dc1-data"] != 1 {
		t.Fatalf("claim patches = %v, want the growth", f.pvcPatches)
	}
	f.events()

	pvc := f.claim(t, "dc1")
	pvc.Spec.Resources.Requests[corev1.ResourceStorage] = resource.MustParse("150Gi")
	if err := f.c.Update(context.Background(), pvc); err != nil {
		t.Fatal(err)
	}
	for range 2 {
		res := f.reconcile(t)
		if f.pvcPatches["mysql-lion-dc1-data"] != 1 {
			t.Fatalf("the lowered request was raised again (%d patches)", f.pvcPatches["mysql-lion-dc1-data"])
		}
		st := sgPhase(t, f.group(t), "dc1")
		if st.Phase != v1beta1.SiteStorageFailed || st.AbandonedSize != "200Gi" || st.RequestedSize != "150Gi" || !strings.Contains(st.Message, "does not raise it to 200Gi again") {
			t.Fatalf("dc1 storage = %+v", st)
		}
		if res.RequeueAfter <= 0 || res.RequeueAfter > storageRecheckInterval {
			t.Fatalf("requeue = %v", res.RequeueAfter)
		}
	}
	n := 0
	for _, e := range f.events() {
		if strings.Contains(e, v1beta1.EventStorageExpansionFailed) {
			n++
		}
	}
	if n != 1 {
		t.Fatalf("%d %s events, want one on the transition", n, v1beta1.EventStorageExpansionFailed)
	}

	// A larger size is a new request: growth resumes and the record goes.
	live := f.group(t)
	live.Spec.Sites[0].Storage.Size = "300Gi"
	live.Generation++
	if err := f.c.Update(context.Background(), live); err != nil {
		t.Fatal(err)
	}
	f.reconcile(t)
	if q := f.claim(t, "dc1").Spec.Resources.Requests[corev1.ResourceStorage]; q.Cmp(resource.MustParse("300Gi")) != 0 {
		t.Fatalf("dc1 request = %s, want 300Gi", q.String())
	}
	if st := sgPhase(t, f.group(t), "dc1"); st.Phase != v1beta1.SiteStorageExpanding || st.AbandonedSize != "" {
		t.Fatalf("dc1 storage = %+v", st)
	}
}

func TestAbandonedGrowth(t *testing.T) {
	d := resource.MustParse("200Gi")
	for _, tc := range []struct {
		name      string
		prev      *v1beta1.SiteStorageStatus
		requested string
		want      bool
	}{
		{"no record", nil, "100Gi", false},
		{"growing from below", &v1beta1.SiteStorageStatus{ClaimUID: "u", RequestedSize: "100Gi"}, "100Gi", false},
		{"lowered after the raise", &v1beta1.SiteStorageStatus{ClaimUID: "u", RequestedSize: "200Gi"}, "150Gi", true},
		{"lowered from a larger manual request", &v1beta1.SiteStorageStatus{ClaimUID: "u", RequestedSize: "204800Mi"}, "150Gi", true},
		{"another claim", &v1beta1.SiteStorageStatus{ClaimUID: "old", RequestedSize: "200Gi"}, "150Gi", false},
		{"recorded at this size", &v1beta1.SiteStorageStatus{ClaimUID: "u", RequestedSize: "150Gi", AbandonedSize: "200Gi"}, "150Gi", true},
		{"recorded at a smaller size", &v1beta1.SiteStorageStatus{ClaimUID: "u", RequestedSize: "150Gi", AbandonedSize: "180Gi"}, "150Gi", false},
		{"request back at the size", &v1beta1.SiteStorageStatus{ClaimUID: "u", RequestedSize: "150Gi", AbandonedSize: "200Gi"}, "200Gi", false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if got := abandonedGrowth(tc.prev, "u", resource.MustParse(tc.requested), d); got != tc.want {
				t.Fatalf("abandonedGrowth = %v, want %v", got, tc.want)
			}
		})
	}
}

// A StorageClass read the API server refuses (the operator's role lacks
// get on storageclasses) is a failure with a cause, not a retry for ever,
// and is not repeated on every reconcile.
func TestReconcilePVC_RefusedClassReadIsFailedAndNotRepeated(t *testing.T) {
	fg := newTestFG()
	fg.Spec.Sites[0].Storage.Size = "200Gi"
	allow := true
	f := newSGFixture(t, fg,
		sgSiteClaim("dc1", "local-dc1", "100Gi", "100Gi"), sgSiteClaim("dc2", "local-dc2", "100Gi", "100Gi"),
		sgClass("local-dc1", &allow))
	f.failClassGet["local-dc1"] = apierrors.NewForbidden(schema.GroupResource{Group: "storage.k8s.io", Resource: "storageclasses"}, "local-dc1", stringError("no rule"))
	res := f.reconcile(t)
	st := sgPhase(t, f.group(t), "dc1")
	if st.Phase != v1beta1.SiteStorageFailed || !strings.Contains(st.Message, "needs get on storageclasses") {
		t.Fatalf("dc1 storage = %+v", st)
	}
	if res.RequeueAfter <= storageRetryInterval || res.RequeueAfter > storageRecheckInterval {
		t.Fatalf("requeue = %v, want the recheck", res.RequeueAfter)
	}
	if ev := strings.Join(f.events(), "\n"); !strings.Contains(ev, v1beta1.EventStorageExpansionFailed) {
		t.Fatalf("no %s event: %s", v1beta1.EventStorageExpansionFailed, ev)
	}
	gets := f.classGets
	f.reconcile(t)
	if f.classGets != gets {
		t.Fatalf("the refused read was repeated inside its TTL (%d -> %d)", gets, f.classGets)
	}
	if f.pvcPatches["mysql-lion-dc1-data"] != 0 {
		t.Fatal("the claim was raised without its class being read")
	}
}
