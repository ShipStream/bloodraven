package controller

import (
	"context"
	"errors"
	"io"
	"os"
	"strings"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/tools/record"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	v1alpha1 "github.com/shipstream/bloodraven/api/v1alpha1"
	"github.com/shipstream/bloodraven/internal/sidecar"
)

// fakeStore implements sidecar.ArchiveStore for testing. The caller
// preloads Objects with string keys → byte-slice values.
type fakeStore struct {
	Objects map[string][]byte
	ListErr error
	// GetCalls counts Get invocations per key (lazy-initialized). Used by the
	// dump-metadata cache tests to assert immutable @.json files are not
	// re-fetched on every scan.
	GetCalls map[string]int
}

func (f *fakeStore) Put(_ context.Context, key string, r io.Reader, _ int64) error {
	data, err := io.ReadAll(r)
	if err != nil {
		return err
	}
	f.Objects[key] = data
	return nil
}
func (f *fakeStore) PutFile(_ context.Context, _, _ string) error { return nil }
func (f *fakeStore) Delete(_ context.Context, key string) error   { delete(f.Objects, key); return nil }
func (f *fakeStore) GetFile(_ context.Context, _, _ string) error { return nil }

func (f *fakeStore) Get(_ context.Context, key string) ([]byte, bool, error) {
	if f.GetCalls == nil {
		f.GetCalls = map[string]int{}
	}
	f.GetCalls[key]++
	v, ok := f.Objects[key]
	return v, ok, nil
}

func (f *fakeStore) List(_ context.Context, prefix string) ([]string, error) {
	if f.ListErr != nil {
		return nil, f.ListErr
	}
	var out []string
	for k := range f.Objects {
		if len(k) >= len(prefix) && k[:len(prefix)] == prefix {
			out = append(out, k)
		}
	}
	return out, nil
}

// newStandbyScheme returns a runtime.Scheme with v1alpha1 registered.
func newStandbyScheme(t *testing.T) *runtime.Scheme {
	t.Helper()
	s := runtime.NewScheme()
	if err := v1alpha1.AddToScheme(s); err != nil {
		t.Fatalf("AddToScheme: %v", err)
	}
	if err := corev1.AddToScheme(s); err != nil {
		t.Fatalf("AddToScheme corev1: %v", err)
	}
	return s
}

// minimalStandbyCR returns a MysqlStandbyCluster suitable for testing.
func minimalStandbyCR(name, ns string) *v1alpha1.MysqlStandbyCluster {
	return &v1alpha1.MysqlStandbyCluster{
		ObjectMeta: metav1.ObjectMeta{
			Name:      name,
			Namespace: ns,
		},
		Spec: v1alpha1.MysqlStandbyClusterSpec{
			Transport: v1alpha1.StandbyTransportObjectStore,
			Source: v1alpha1.StandbySource{
				FailoverGroupName: "orders",
				ProfileName:       "nightly",
				Storage: v1alpha1.BackupStorage{
					Type: v1alpha1.BackupStorageS3,
					S3: &v1alpha1.S3Storage{
						Bucket: "my-bucket",
						Prefix: "orders",
						// No CredentialsSecret: tests bypass store construction
						// via newStoreFunc injection.
					},
				},
			},
			Template: v1alpha1.StandbyFailoverGroupTemplate{
				Name: "orders-dr",
				Spec: v1alpha1.MysqlFailoverGroupSpec{
					Image:      "mysql:9.6",
					SecretName: "mysql-creds",
				},
			},
		},
	}
}

// newTestReconciler builds a MysqlStandbyClusterReconciler backed by the
// controller-runtime fake client and an injected fakeStore.
func newTestReconciler(t *testing.T, objs []client.Object, store sidecar.ArchiveStore) (*MysqlStandbyClusterReconciler, *record.FakeRecorder) {
	t.Helper()
	scheme := newStandbyScheme(t)
	fakeClient := fake.NewClientBuilder().WithScheme(scheme).WithStatusSubresource(&v1alpha1.MysqlStandbyCluster{}).WithObjects(objs...).Build()
	recorder := record.NewFakeRecorder(32)
	r := &MysqlStandbyClusterReconciler{
		Client:   fakeClient,
		Scheme:   scheme,
		Recorder: recorder,
		newStoreFunc: func(_ context.Context, _ *sidecar.PITRConfig) (sidecar.ArchiveStore, error) {
			return store, nil
		},
	}
	return r, recorder
}

// reconcileStandby runs one reconcile cycle and returns the refreshed CR.
// It calls t.Fatal if the reconcile returns an error.
func reconcileStandby(t *testing.T, r *MysqlStandbyClusterReconciler, name, ns string) (*v1alpha1.MysqlStandbyCluster, ctrl.Result) {
	t.Helper()
	ctx := context.Background()
	res, err := r.Reconcile(ctx, ctrl.Request{NamespacedName: types.NamespacedName{Name: name, Namespace: ns}})
	if err != nil {
		t.Fatalf("reconcile returned unexpected error: %v", err)
	}
	var updated v1alpha1.MysqlStandbyCluster
	if err := r.Get(ctx, types.NamespacedName{Name: name, Namespace: ns}, &updated); err != nil {
		t.Fatalf("get CR after reconcile: %v", err)
	}
	return &updated, res
}

// reconcileStandbyExpectingError runs one reconcile cycle and returns the error
// along with the refreshed CR. Use this for tests that verify error-path behavior.
func reconcileStandbyExpectingError(t *testing.T, r *MysqlStandbyClusterReconciler, name, ns string) (*v1alpha1.MysqlStandbyCluster, ctrl.Result, error) {
	t.Helper()
	ctx := context.Background()
	res, reconcileErr := r.Reconcile(ctx, ctrl.Request{NamespacedName: types.NamespacedName{Name: name, Namespace: ns}})
	var updated v1alpha1.MysqlStandbyCluster
	if err := r.Get(ctx, types.NamespacedName{Name: name, Namespace: ns}, &updated); err != nil {
		// CR may not exist (NotFound path); return what we have.
		return nil, res, reconcileErr
	}
	return &updated, res, reconcileErr
}

// standbyFindCondition returns the named condition, or nil when absent.
func standbyFindCondition(cr *v1alpha1.MysqlStandbyCluster, condType string) *metav1.Condition {
	for i := range cr.Status.Conditions {
		if cr.Status.Conditions[i].Type == condType {
			return &cr.Status.Conditions[i]
		}
	}
	return nil
}

// --- TestMysqlStandbyCluster_UnsupportedTransport ---------------------------

// TestMysqlStandbyCluster_UnsupportedTransport verifies that a CR whose
// spec.transport is set to "Network" (reserved, unimplemented) gets
// BucketReadable=False with Reason=ConfigError and is requeued.
func TestMysqlStandbyCluster_UnsupportedTransport(t *testing.T) {
	cr := minimalStandbyCR("test-sc", "default")
	cr.Spec.Transport = v1alpha1.StandbyTransportNetwork

	// Spec-level XValidation prevents this in a real cluster, but we test
	// the reconciler's in-depth defense here.
	// The fake client bypasses admission webhooks.
	store := &fakeStore{Objects: map[string][]byte{}}
	r, _ := newTestReconciler(t, []client.Object{cr}, store)

	updated, res := reconcileStandby(t, r, "test-sc", "default")

	if res.RequeueAfter == 0 {
		t.Error("want non-zero requeue interval, got 0")
	}

	brc := standbyFindCondition(updated, v1alpha1.StandbyConditionBucketReadable)
	if brc == nil {
		t.Fatal("BucketReadable condition missing")
	}
	if brc.Status != metav1.ConditionFalse {
		t.Errorf("BucketReadable: want False, got %s", brc.Status)
	}
	if brc.Reason != "ConfigError" {
		t.Errorf("BucketReadable reason: want ConfigError, got %s", brc.Reason)
	}

	skc := standbyFindCondition(updated, v1alpha1.StandbyConditionSourceConfigKnown)
	if skc == nil {
		t.Fatal("SourceConfigKnown condition missing")
	}
	if skc.Status != metav1.ConditionFalse {
		t.Errorf("SourceConfigKnown: want False, got %s", skc.Status)
	}
}

// --- TestMysqlStandbyCluster_SuccessfulDiscovery ----------------------------

// TestMysqlStandbyCluster_SuccessfulDiscovery verifies that a successful
// bucket scan populates status.discovered and stamps both conditions True.
func TestMysqlStandbyCluster_SuccessfulDiscovery(t *testing.T) {
	cr := minimalStandbyCR("test-sc", "default")

	// Bucket layout:
	//   orders/orders-nightly-20260520/@.json
	//   orders/binlogs/manifest-dc1.json
	atJSON := `{"end":"2026-05-20T04:00:00Z","gtidExecuted":"abc:1-100","totalBytes":1073741824}`
	manifestJSON := `{"version":1,"site":"dc1","files":[{"name":"mysql-bin.000001","remotePath":"orders/binlogs/dc1/mysql-bin.000001","size":4096,"firstEventTime":"2026-05-20T00:00:00Z","lastEventTime":"2026-05-20T03:59:59Z","archivedAt":"2026-05-20T04:00:01Z"}]}`

	store := &fakeStore{Objects: map[string][]byte{
		"orders/orders-nightly-20260520/@.json": []byte(atJSON),
		"orders/binlogs/manifest-dc1.json":      []byte(manifestJSON),
	}}
	r, recorder := newTestReconciler(t, []client.Object{cr}, store)

	updated, res := reconcileStandby(t, r, "test-sc", "default")

	// Should requeue at default interval.
	if res.RequeueAfter != standbyDefaultDiscoveryInterval {
		t.Errorf("requeue: want %v, got %v", standbyDefaultDiscoveryInterval, res.RequeueAfter)
	}

	// BucketReadable=True.
	brc := standbyFindCondition(updated, v1alpha1.StandbyConditionBucketReadable)
	if brc == nil || brc.Status != metav1.ConditionTrue {
		t.Errorf("BucketReadable: want True, got %v", brc)
	}

	// SourceConfigKnown=True.
	skc := standbyFindCondition(updated, v1alpha1.StandbyConditionSourceConfigKnown)
	if skc == nil || skc.Status != metav1.ConditionTrue {
		t.Errorf("SourceConfigKnown: want True, got %v", skc)
	}

	// status.discovered populated.
	d := updated.Status.Discovered
	if d == nil {
		t.Fatal("status.discovered is nil")
	}
	if d.DumpName != "orders-nightly-20260520" {
		t.Errorf("DumpName: want orders-nightly-20260520, got %q", d.DumpName)
	}
	if d.DumpGtidExecuted != "abc:1-100" {
		t.Errorf("DumpGtidExecuted: want abc:1-100, got %q", d.DumpGtidExecuted)
	}
	if d.DumpSizeBytes != 1073741824 {
		t.Errorf("DumpSizeBytes: want 1073741824, got %d", d.DumpSizeBytes)
	}
	if d.ManifestCount != 1 {
		t.Errorf("ManifestCount: want 1, got %d", d.ManifestCount)
	}
	if d.ArchivedBinlogCount != 1 {
		t.Errorf("ArchivedBinlogCount: want 1, got %d", d.ArchivedBinlogCount)
	}
	if d.ArchivedBinlogBytes != 4096 {
		t.Errorf("ArchivedBinlogBytes: want 4096, got %d", d.ArchivedBinlogBytes)
	}
	if d.OldestArchivedBinlogTime == nil {
		t.Error("OldestArchivedBinlogTime is nil")
	}
	if d.NewestArchivedBinlogTime == nil {
		t.Error("NewestArchivedBinlogTime is nil")
	}
	if d.LastScanAt == nil {
		t.Error("LastScanAt is nil")
	}
	if d.DumpCompletionTime == nil {
		t.Error("DumpCompletionTime is nil")
	}

	// Event emitted.
	select {
	case ev := <-recorder.Events:
		if !strings.Contains(ev, "BucketScanned") {
			t.Errorf("want BucketScanned event, got %q", ev)
		}
	default:
		t.Error("expected BucketScanned event but none emitted")
	}
}

// --- TestMysqlStandbyCluster_ListError --------------------------------------

// TestMysqlStandbyCluster_ListError verifies that a List failure stamps
// BucketReadable=False with Reason=ListFailed.
func TestMysqlStandbyCluster_ListError(t *testing.T) {
	cr := minimalStandbyCR("test-sc", "default")
	store := &fakeStore{
		Objects: map[string][]byte{},
		ListErr: errors.New("access denied"),
	}
	r, recorder := newTestReconciler(t, []client.Object{cr}, store)

	updated, _ := reconcileStandby(t, r, "test-sc", "default")

	brc := standbyFindCondition(updated, v1alpha1.StandbyConditionBucketReadable)
	if brc == nil || brc.Status != metav1.ConditionFalse {
		t.Errorf("BucketReadable: want False, got %v", brc)
	}
	if brc.Reason != "ListFailed" {
		t.Errorf("Reason: want ListFailed, got %q", brc.Reason)
	}
	// SourceConfigKnown should also be False.
	skc := standbyFindCondition(updated, v1alpha1.StandbyConditionSourceConfigKnown)
	if skc == nil || skc.Status != metav1.ConditionFalse {
		t.Errorf("SourceConfigKnown: want False, got %v", skc)
	}
	// Warning event emitted.
	select {
	case ev := <-recorder.Events:
		if !strings.Contains(ev, "ListFailed") {
			t.Errorf("want ListFailed event, got %q", ev)
		}
	default:
		t.Error("expected ListFailed event but none emitted")
	}
}

// --- TestMysqlStandbyCluster_NoDumpFound ------------------------------------

// TestMysqlStandbyCluster_NoDumpFound verifies that an empty bucket (no @.json
// objects) results in BucketReadable=True, SourceConfigKnown=False with
// Reason=NoDumpFound.
func TestMysqlStandbyCluster_NoDumpFound(t *testing.T) {
	cr := minimalStandbyCR("test-sc", "default")
	store := &fakeStore{Objects: map[string][]byte{}} // empty bucket
	r, _ := newTestReconciler(t, []client.Object{cr}, store)

	updated, _ := reconcileStandby(t, r, "test-sc", "default")

	brc := standbyFindCondition(updated, v1alpha1.StandbyConditionBucketReadable)
	if brc == nil || brc.Status != metav1.ConditionTrue {
		t.Errorf("BucketReadable: want True (list succeeded), got %v", brc)
	}
	skc := standbyFindCondition(updated, v1alpha1.StandbyConditionSourceConfigKnown)
	if skc == nil || skc.Status != metav1.ConditionFalse {
		t.Errorf("SourceConfigKnown: want False, got %v", skc)
	}
	if skc.Reason != "NoDumpFound" {
		t.Errorf("Reason: want NoDumpFound, got %q", skc.Reason)
	}
}

// --- TestMysqlStandbyCluster_NoBinlogManifests ------------------------------

// TestMysqlStandbyCluster_NoBinlogManifests verifies that a bucket with a dump
// but no binlog manifests stamps SourceConfigKnown=False with
// Reason=NoBinlogManifests.
func TestMysqlStandbyCluster_NoBinlogManifests(t *testing.T) {
	cr := minimalStandbyCR("test-sc", "default")
	store := &fakeStore{Objects: map[string][]byte{
		"orders/orders-nightly-20260520/@.json": []byte(`{"end":"2026-05-20T04:00:00Z"}`),
		// No manifest files under orders/binlogs/.
	}}
	r, _ := newTestReconciler(t, []client.Object{cr}, store)

	updated, _ := reconcileStandby(t, r, "test-sc", "default")

	brc := standbyFindCondition(updated, v1alpha1.StandbyConditionBucketReadable)
	if brc == nil || brc.Status != metav1.ConditionTrue {
		t.Errorf("BucketReadable: want True, got %v", brc)
	}
	skc := standbyFindCondition(updated, v1alpha1.StandbyConditionSourceConfigKnown)
	if skc == nil || skc.Status != metav1.ConditionFalse {
		t.Errorf("SourceConfigKnown: want False, got %v", skc)
	}
	if skc.Reason != "NoBinlogManifests" {
		t.Errorf("Reason: want NoBinlogManifests, got %q", skc.Reason)
	}
	// Dump name should still be populated even when manifests are missing.
	if updated.Status.Discovered == nil || updated.Status.Discovered.DumpName == "" {
		t.Error("DumpName should be populated even when manifests are missing")
	}
}

// --- TestMysqlStandbyCluster_RequeueCadence ---------------------------------

// TestMysqlStandbyCluster_RequeueCadence verifies that the requeue interval
// matches spec.freshness.discoveryInterval when set.
func TestMysqlStandbyCluster_RequeueCadence(t *testing.T) {
	cr := minimalStandbyCR("test-sc", "default")
	customInterval := 10 * time.Minute
	cr.Spec.Freshness = &v1alpha1.StandbyFreshnessSpec{
		DiscoveryInterval: &metav1.Duration{Duration: customInterval},
	}

	store := &fakeStore{Objects: map[string][]byte{}}
	r, _ := newTestReconciler(t, []client.Object{cr}, store)

	_, res := reconcileStandby(t, r, "test-sc", "default")

	if res.RequeueAfter != customInterval {
		t.Errorf("requeue: want %v, got %v", customInterval, res.RequeueAfter)
	}
}

// --- TestMysqlStandbyCluster_DefaultRequeueCadence --------------------------

// TestMysqlStandbyCluster_DefaultRequeueCadence verifies that the default
// requeue interval (5m) is used when spec.freshness is nil.
func TestMysqlStandbyCluster_DefaultRequeueCadence(t *testing.T) {
	cr := minimalStandbyCR("test-sc", "default")
	// No Freshness set.

	store := &fakeStore{Objects: map[string][]byte{}}
	r, _ := newTestReconciler(t, []client.Object{cr}, store)

	_, res := reconcileStandby(t, r, "test-sc", "default")

	if res.RequeueAfter != standbyDefaultDiscoveryInterval {
		t.Errorf("requeue: want %v (default), got %v", standbyDefaultDiscoveryInterval, res.RequeueAfter)
	}
}

// --- TestMysqlStandbyCluster_PVCStorageConfigError --------------------------

// TestMysqlStandbyCluster_PVCStorageConfigError verifies that a PVC-backed
// MysqlStandbyCluster gets BucketReadable=False with Reason=ConfigError since
// the operator pod cannot mount a remote PVC.
func TestMysqlStandbyCluster_PVCStorageConfigError(t *testing.T) {
	cr := minimalStandbyCR("test-sc", "default")
	cr.Spec.Source.Storage = v1alpha1.BackupStorage{
		Type: v1alpha1.BackupStoragePVC,
		PVC: &v1alpha1.PVCStorage{
			ClaimName: "my-backup-pvc",
		},
	}
	// The newStoreFunc is not called when buildStoreCfg fails, but we
	// need to provide one so the reconciler struct is fully wired.
	store := &fakeStore{Objects: map[string][]byte{}}
	r, _ := newTestReconciler(t, []client.Object{cr}, store)
	// Override newStoreFunc so it never gets reached (test the config-error path).
	r.newStoreFunc = func(_ context.Context, _ *sidecar.PITRConfig) (sidecar.ArchiveStore, error) {
		t.Error("newStoreFunc should not be called for PVC storage")
		return nil, errors.New("unexpected call")
	}

	updated, _ := reconcileStandby(t, r, "test-sc", "default")

	brc := standbyFindCondition(updated, v1alpha1.StandbyConditionBucketReadable)
	if brc == nil || brc.Status != metav1.ConditionFalse {
		t.Errorf("BucketReadable: want False, got %v", brc)
	}
	if brc.Reason != "ConfigError" {
		t.Errorf("Reason: want ConfigError, got %q", brc.Reason)
	}
}

// --- TestMysqlStandbyCluster_MultiSiteManifests -----------------------------

// TestMysqlStandbyCluster_MultiSiteManifests verifies that binlog stats are
// correctly aggregated across multiple manifest files (multiple sites).
func TestMysqlStandbyCluster_MultiSiteManifests(t *testing.T) {
	cr := minimalStandbyCR("test-sc", "default")

	atJSON := `{"end":"2026-05-20T04:00:00Z","totalBytes":500}`
	manifest1 := `{"version":1,"site":"dc1","files":[{"name":"b1","remotePath":"orders/binlogs/dc1/b1","size":100,"firstEventTime":"2026-05-20T00:00:00Z","lastEventTime":"2026-05-20T01:00:00Z","archivedAt":"2026-05-20T01:01:00Z"}]}`
	manifest2 := `{"version":1,"site":"dc2","files":[{"name":"b1","remotePath":"orders/binlogs/dc2/b1","size":200,"firstEventTime":"2026-05-20T01:30:00Z","lastEventTime":"2026-05-20T03:59:59Z","archivedAt":"2026-05-20T04:00:01Z"}]}`

	store := &fakeStore{Objects: map[string][]byte{
		"orders/dump-2026-05-20/@.json":    []byte(atJSON),
		"orders/binlogs/manifest-dc1.json": []byte(manifest1),
		"orders/binlogs/manifest-dc2.json": []byte(manifest2),
	}}
	r, _ := newTestReconciler(t, []client.Object{cr}, store)

	updated, _ := reconcileStandby(t, r, "test-sc", "default")

	d := updated.Status.Discovered
	if d == nil {
		t.Fatal("status.discovered is nil")
	}
	if d.ManifestCount != 2 {
		t.Errorf("ManifestCount: want 2, got %d", d.ManifestCount)
	}
	if d.ArchivedBinlogCount != 2 {
		t.Errorf("ArchivedBinlogCount: want 2, got %d", d.ArchivedBinlogCount)
	}
	if d.ArchivedBinlogBytes != 300 {
		t.Errorf("ArchivedBinlogBytes: want 300, got %d", d.ArchivedBinlogBytes)
	}
	// Oldest from dc1 (00:00), newest from dc2 (03:59:59).
	if d.OldestArchivedBinlogTime == nil {
		t.Fatal("OldestArchivedBinlogTime nil")
	}
	wantOldest := time.Date(2026, 5, 20, 0, 0, 0, 0, time.UTC)
	if !d.OldestArchivedBinlogTime.Time.Equal(wantOldest) {
		t.Errorf("OldestArchivedBinlogTime: want %v, got %v", wantOldest, d.OldestArchivedBinlogTime.Time)
	}
	if d.NewestArchivedBinlogTime == nil {
		t.Fatal("NewestArchivedBinlogTime nil")
	}
	wantNewest := time.Date(2026, 5, 20, 3, 59, 59, 0, time.UTC)
	if !d.NewestArchivedBinlogTime.Time.Equal(wantNewest) {
		t.Errorf("NewestArchivedBinlogTime: want %v, got %v", wantNewest, d.NewestArchivedBinlogTime.Time)
	}
}

// --- TestMysqlStandbyCluster_NewestDumpSelected -----------------------------

// TestMysqlStandbyCluster_NewestDumpSelected verifies that when multiple dumps
// exist the lexicographically last one (most recent timestamp-named backup) is
// selected.
func TestMysqlStandbyCluster_NewestDumpSelected(t *testing.T) {
	cr := minimalStandbyCR("test-sc", "default")

	store := &fakeStore{Objects: map[string][]byte{
		"orders/orders-2026-05-18/@.json":  []byte(`{"end":"2026-05-18T04:00:00Z"}`),
		"orders/orders-2026-05-19/@.json":  []byte(`{"end":"2026-05-19T04:00:00Z"}`),
		"orders/orders-2026-05-20/@.json":  []byte(`{"end":"2026-05-20T04:00:00Z","gtidExecuted":"newest:1-999"}`),
		"orders/binlogs/manifest-dc1.json": []byte(`{"version":1,"site":"dc1","files":[]}`),
	}}
	r, _ := newTestReconciler(t, []client.Object{cr}, store)

	updated, _ := reconcileStandby(t, r, "test-sc", "default")

	d := updated.Status.Discovered
	if d == nil {
		t.Fatal("status.discovered is nil")
	}
	if d.DumpName != "orders-2026-05-20" {
		t.Errorf("DumpName: want orders-2026-05-20, got %q", d.DumpName)
	}
	if d.DumpGtidExecuted != "newest:1-999" {
		t.Errorf("GtidExecuted: want newest:1-999, got %q", d.DumpGtidExecuted)
	}
}

func TestMysqlStandbyCluster_NewestDumpSelectedBeyondLexicographicTail(t *testing.T) {
	cr := minimalStandbyCR("test-sc", "default")

	objects := map[string][]byte{
		"orders/binlogs/manifest-dc1.json": []byte(`{"version":1,"site":"dc1","files":[]}`),
		"orders/z-dump-00/@.json":          []byte(`{"end":"2026-05-10T04:00:00Z"}`),
		"orders/z-dump-01/@.json":          []byte(`{"end":"2026-05-11T04:00:00Z"}`),
		"orders/z-dump-02/@.json":          []byte(`{"end":"2026-05-12T04:00:00Z"}`),
		"orders/z-dump-03/@.json":          []byte(`{"end":"2026-05-13T04:00:00Z"}`),
		"orders/z-dump-04/@.json":          []byte(`{"end":"2026-05-14T04:00:00Z"}`),
		"orders/z-dump-05/@.json":          []byte(`{"end":"2026-05-15T04:00:00Z"}`),
		"orders/z-dump-06/@.json":          []byte(`{"end":"2026-05-16T04:00:00Z"}`),
		"orders/z-dump-07/@.json":          []byte(`{"end":"2026-05-17T04:00:00Z"}`),
		"orders/z-dump-08/@.json":          []byte(`{"end":"2026-05-18T04:00:00Z"}`),
		"orders/z-dump-09/@.json":          []byte(`{"end":"2026-05-19T04:00:00Z"}`),
		"orders/z-dump-10/@.json":          []byte(`{"end":"2026-05-20T04:00:00Z"}`),
		"orders/a-newest/@.json":           []byte(`{"end":"2026-05-21T04:00:00Z","gtidExecuted":"newest:1-999"}`),
	}
	store := &fakeStore{Objects: objects}
	r, _ := newTestReconciler(t, []client.Object{cr}, store)

	updated, _ := reconcileStandby(t, r, "test-sc", "default")

	d := updated.Status.Discovered
	if d == nil {
		t.Fatal("status.discovered is nil")
	}
	if d.DumpName != "a-newest" {
		t.Errorf("DumpName: want a-newest, got %q", d.DumpName)
	}
	if d.DumpGtidExecuted != "newest:1-999" {
		t.Errorf("GtidExecuted: want newest:1-999, got %q", d.DumpGtidExecuted)
	}
}

func TestMysqlStandbyCluster_SlashBoundedPrefix(t *testing.T) {
	cr := minimalStandbyCR("test-sc", "default")
	cr.Spec.Source.Storage.S3.Prefix = "orders/west"

	store := &fakeStore{Objects: map[string][]byte{
		"orders/west-old/other/@.json":        []byte(`{"end":"2026-05-21T04:00:00Z"}`),
		"orders/west/current/@.json":          []byte(`{"end":"2026-05-20T04:00:00Z","gtidExecuted":"current:1-100"}`),
		"orders/west/binlogs/manifest-a.json": []byte(`{"version":1,"site":"a","files":[]}`),
	}}
	r, _ := newTestReconciler(t, []client.Object{cr}, store)

	updated, _ := reconcileStandby(t, r, "test-sc", "default")

	d := updated.Status.Discovered
	if d == nil {
		t.Fatal("status.discovered is nil")
	}
	if d.DumpName != "current" {
		t.Errorf("DumpName: want current, got %q", d.DumpName)
	}
	if d.DumpGtidExecuted != "current:1-100" {
		t.Errorf("GtidExecuted: want current:1-100, got %q", d.DumpGtidExecuted)
	}
}

func TestMysqlStandbyCluster_DecryptionConfiguresPassphraseFile(t *testing.T) {
	cr := minimalStandbyCR("test-sc", "default")
	cr.Spec.Source.Decryption = &v1alpha1.BackupDecryptionSpec{
		PassphraseSecret: v1alpha1.PassphraseSecretRef{Name: "backup-passphrase", Key: "secret"},
	}
	secret := &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{Name: "backup-passphrase", Namespace: "default"},
		Data:       map[string][]byte{"secret": []byte("correct horse battery staple")},
	}
	scheme := newStandbyScheme(t)
	fakeClient := fake.NewClientBuilder().WithScheme(scheme).WithStatusSubresource(&v1alpha1.MysqlStandbyCluster{}).WithObjects(cr, secret).Build()
	store := &fakeStore{Objects: map[string][]byte{
		"orders/orders-nightly/@.json":     []byte(`{"end":"2026-05-20T04:00:00Z"}`),
		"orders/binlogs/manifest-dc1.json": []byte(`{"version":1,"site":"dc1","files":[]}`),
	}}
	var passphrase string
	r := &MysqlStandbyClusterReconciler{
		Client:   fakeClient,
		Scheme:   scheme,
		Recorder: record.NewFakeRecorder(32),
		newStoreFunc: func(_ context.Context, cfg *sidecar.PITRConfig) (sidecar.ArchiveStore, error) {
			if cfg.PassphraseFile == "" {
				t.Fatal("PassphraseFile is empty")
			}
			data, err := os.ReadFile(cfg.PassphraseFile)
			if err != nil {
				t.Fatalf("read passphrase file: %v", err)
			}
			passphrase = string(data)
			return store, nil
		},
	}

	reconcileStandby(t, r, "test-sc", "default")

	if passphrase != "correct horse battery staple" {
		t.Errorf("passphrase: want secret data, got %q", passphrase)
	}
}

// --- TestMysqlStandbyCluster_Idempotent -------------------------------------

// TestMysqlStandbyCluster_Idempotent verifies that reconciling twice with the
// same store state does not produce a second status patch (LastTransitionTime
// is preserved on unchanged conditions).
func TestMysqlStandbyCluster_Idempotent(t *testing.T) {
	cr := minimalStandbyCR("test-sc", "default")
	store := &fakeStore{Objects: map[string][]byte{
		"orders/orders-nightly-20260520/@.json": []byte(`{"end":"2026-05-20T04:00:00Z"}`),
		"orders/binlogs/manifest-dc1.json":      []byte(`{"version":1,"site":"dc1","files":[]}`),
	}}
	r, _ := newTestReconciler(t, []client.Object{cr}, store)

	// First reconcile.
	cr1, _ := reconcileStandby(t, r, "test-sc", "default")

	// Record the condition timestamps from the first reconcile.
	brc1 := standbyFindCondition(cr1, v1alpha1.StandbyConditionBucketReadable)
	skc1 := standbyFindCondition(cr1, v1alpha1.StandbyConditionSourceConfigKnown)
	if brc1 == nil || skc1 == nil {
		t.Fatal("conditions missing after first reconcile")
	}
	ltt1 := brc1.LastTransitionTime
	ltt1sk := skc1.LastTransitionTime

	// Second reconcile with the same store state.
	cr2, _ := reconcileStandby(t, r, "test-sc", "default")

	brc2 := standbyFindCondition(cr2, v1alpha1.StandbyConditionBucketReadable)
	skc2 := standbyFindCondition(cr2, v1alpha1.StandbyConditionSourceConfigKnown)
	if brc2 == nil || skc2 == nil {
		t.Fatal("conditions missing after second reconcile")
	}
	// LastTransitionTime must not change when status is the same.
	if !brc2.LastTransitionTime.Equal(&ltt1) {
		t.Errorf("BucketReadable LastTransitionTime changed on idempotent reconcile: %v → %v",
			ltt1, brc2.LastTransitionTime)
	}
	if !skc2.LastTransitionTime.Equal(&ltt1sk) {
		t.Errorf("SourceConfigKnown LastTransitionTime changed on idempotent reconcile: %v → %v",
			ltt1sk, skc2.LastTransitionTime)
	}
}

// --- TestMysqlStandbyCluster_StoreConstructionError -------------------------

// TestMysqlStandbyCluster_StoreConstructionError verifies that a store
// construction failure (e.g. bad credentials) stamps BucketReadable=False with
// Reason=AuthFailed.
func TestMysqlStandbyCluster_StoreConstructionError(t *testing.T) {
	cr := minimalStandbyCR("test-sc", "default")
	scheme := newStandbyScheme(t)
	fakeClient := fake.NewClientBuilder().WithScheme(scheme).WithStatusSubresource(&v1alpha1.MysqlStandbyCluster{}).WithObjects(cr).Build()
	recorder := record.NewFakeRecorder(32)
	r := &MysqlStandbyClusterReconciler{
		Client:   fakeClient,
		Scheme:   scheme,
		Recorder: recorder,
		newStoreFunc: func(_ context.Context, _ *sidecar.PITRConfig) (sidecar.ArchiveStore, error) {
			return nil, errors.New("invalid credentials")
		},
	}

	updated, _ := reconcileStandby(t, r, "test-sc", "default")

	brc := standbyFindCondition(updated, v1alpha1.StandbyConditionBucketReadable)
	if brc == nil || brc.Status != metav1.ConditionFalse {
		t.Errorf("BucketReadable: want False, got %v", brc)
	}
	if brc.Reason != "AuthFailed" {
		t.Errorf("Reason: want AuthFailed, got %q", brc.Reason)
	}
}

// --- TestMysqlStandbyCluster_NotFound ---------------------------------------

// TestMysqlStandbyCluster_NotFound verifies that a missing CR is handled
// gracefully (no error, no requeue).
func TestMysqlStandbyCluster_NotFound(t *testing.T) {
	scheme := newStandbyScheme(t)
	fakeClient := fake.NewClientBuilder().WithScheme(scheme).Build()
	recorder := record.NewFakeRecorder(32)
	r := &MysqlStandbyClusterReconciler{
		Client:   fakeClient,
		Scheme:   scheme,
		Recorder: recorder,
	}

	ctx := context.Background()
	res, err := r.Reconcile(ctx, ctrl.Request{NamespacedName: types.NamespacedName{Name: "nonexistent", Namespace: "default"}})
	if err != nil {
		t.Errorf("want nil error for missing CR, got %v", err)
	}
	if res.RequeueAfter != 0 || res.Requeue {
		t.Errorf("want empty result for missing CR, got %+v", res)
	}
}

// --- TestMysqlStandbyCluster_DumpMetaCachedAcrossScans ----------------------

// TestMysqlStandbyCluster_DumpMetaCachedAcrossScans verifies that an immutable
// dump @.json is fetched once and served from cache on subsequent scans, so
// discovery cost does not grow with historical backup count.
func TestMysqlStandbyCluster_DumpMetaCachedAcrossScans(t *testing.T) {
	cr := minimalStandbyCR("test-sc", "default")
	atKey := "orders/orders-nightly-20260520/@.json"
	store := &fakeStore{Objects: map[string][]byte{
		atKey:                              []byte(`{"end":"2026-05-20T04:00:00Z","gtidExecuted":"abc:1-100"}`),
		"orders/binlogs/manifest-dc1.json": []byte(`{"version":1,"site":"dc1","files":[]}`),
	}}
	r, _ := newTestReconciler(t, []client.Object{cr}, store)

	// Two scans against an unchanged bucket; the @.json must be read once.
	reconcileStandby(t, r, "test-sc", "default")
	reconcileStandby(t, r, "test-sc", "default")

	if got := store.GetCalls[atKey]; got != 1 {
		t.Errorf("@.json GET count across two scans: want 1 (cached after first scan), got %d", got)
	}
}

// --- TestMysqlStandbyCluster_DumpMetaCachePrunedWhenDumpRemoved -------------

// TestMysqlStandbyCluster_DumpMetaCachePrunedWhenDumpRemoved verifies that a
// dump removed from the bucket (e.g. by retention) is evicted from the cache
// on the next scan, keeping the memo bounded by the live dump set.
func TestMysqlStandbyCluster_DumpMetaCachePrunedWhenDumpRemoved(t *testing.T) {
	cr := minimalStandbyCR("test-sc", "default")
	oldKey := "orders/orders-old/@.json"
	newKey := "orders/orders-new/@.json"
	store := &fakeStore{Objects: map[string][]byte{
		oldKey:                             []byte(`{"end":"2026-05-19T04:00:00Z"}`),
		newKey:                             []byte(`{"end":"2026-05-20T04:00:00Z"}`),
		"orders/binlogs/manifest-dc1.json": []byte(`{"version":1,"site":"dc1","files":[]}`),
	}}
	r, _ := newTestReconciler(t, []client.Object{cr}, store)

	reconcileStandby(t, r, "test-sc", "default")
	if _, ok := r.dumpMetaCache[oldKey]; !ok {
		t.Fatalf("expected %q cached after first scan", oldKey)
	}

	// Retention removes the old dump from the bucket.
	delete(store.Objects, oldKey)
	reconcileStandby(t, r, "test-sc", "default")

	if _, ok := r.dumpMetaCache[oldKey]; ok {
		t.Errorf("pruned dump %q still in cache after it disappeared from the bucket", oldKey)
	}
	if _, ok := r.dumpMetaCache[newKey]; !ok {
		t.Errorf("present dump %q should remain cached", newKey)
	}
}
