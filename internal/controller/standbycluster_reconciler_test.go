package controller

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"path"
	"strings"
	"sync"
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
	mu      sync.Mutex
	Objects map[string][]byte
	ListErr error
	// GetCalls counts Get invocations per key (lazy-initialized). Used by the
	// dump-metadata cache tests to assert immutable @.json files are not
	// re-fetched on every scan.
	GetCalls map[string]int

	// BlockGet, when true, makes every Get block until its context is done and
	// then return the context error. Used to simulate a deadline firing during
	// the @.json read phase (WISHLIST #44 incompleteness tests). The Objects map
	// is consulted normally for non-blocking Gets when BlockGet is false.
	BlockGet bool
	// BlockList, when true, makes List block until its context is done and then
	// return the context error. Simulates a deadline firing during the List
	// phase.
	BlockList bool
}

func (f *fakeStore) Put(_ context.Context, key string, r io.Reader, _ int64) error {
	data, err := io.ReadAll(r)
	if err != nil {
		return err
	}
	f.mu.Lock()
	f.Objects[key] = data
	f.mu.Unlock()
	return nil
}
func (f *fakeStore) PutFile(_ context.Context, _, _ string) error { return nil }
func (f *fakeStore) Delete(_ context.Context, key string) error {
	f.mu.Lock()
	delete(f.Objects, key)
	f.mu.Unlock()
	return nil
}
func (f *fakeStore) GetFile(_ context.Context, _, _ string) error { return nil }

func (f *fakeStore) Get(ctx context.Context, key string) ([]byte, bool, error) {
	f.mu.Lock()
	if f.GetCalls == nil {
		f.GetCalls = map[string]int{}
	}
	f.GetCalls[key]++
	v, ok := f.Objects[key]
	f.mu.Unlock()

	if f.BlockGet {
		<-ctx.Done()
		return nil, false, ctx.Err()
	}
	return v, ok, nil
}

func (f *fakeStore) List(ctx context.Context, prefix string) ([]string, error) {
	if f.BlockList {
		<-ctx.Done()
		return nil, ctx.Err()
	}
	if f.ListErr != nil {
		return nil, f.ListErr
	}
	f.mu.Lock()
	var out []string
	for k := range f.Objects {
		if len(k) >= len(prefix) && k[:len(prefix)] == prefix {
			out = append(out, k)
		}
	}
	f.mu.Unlock()
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
					Image:      "mysql:9.7",
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

// reconcileStandbyWithCtx runs one reconcile cycle under the supplied context
// (so tests can impose a short deadline that the scan's derived timeouts
// inherit) and returns the refreshed CR. It calls t.Fatal if reconcile errors.
func reconcileStandbyWithCtx(t *testing.T, ctx context.Context, r *MysqlStandbyClusterReconciler, name, ns string) (*v1alpha1.MysqlStandbyCluster, ctrl.Result) {
	t.Helper()
	res, err := r.Reconcile(ctx, ctrl.Request{NamespacedName: types.NamespacedName{Name: name, Namespace: ns}})
	if err != nil {
		t.Fatalf("reconcile returned unexpected error: %v", err)
	}
	// Read the refreshed CR with a fresh context.
	//
	// What the test models vs. what production does:
	//   - In production, incompleteness is triggered by scanBucket's INTERNAL
	//     two-phase budget (List/read timeouts derived from the discovery
	//     interval), which expires while the parent reconcile ctx stays alive.
	//     The parent ctx being alive is what lets the final status patch persist
	//     the ScanIncomplete conditions and the preserved last-known-good
	//     discovered. Production never relies on an expired parent ctx.
	//   - These tests use a short PARENT deadline purely as a convenient proxy:
	//     because the derived timeouts inherit from the parent, an expired parent
	//     ctx exercises the exact same context-error → ScanIncomplete condition
	//     mapping without having to pump a real internal timeout. The status
	//     patch below is therefore read with a fresh ctx, decoupled from the
	//     deliberately-short scan ctx.
	var updated v1alpha1.MysqlStandbyCluster
	if err := r.Get(context.Background(), types.NamespacedName{Name: name, Namespace: ns}, &updated); err != nil {
		t.Fatalf("get CR after reconcile: %v", err)
	}
	return &updated, res
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
		return
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
		return
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
		return
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
	// D2: the NoBinlogManifests message must read as "dump found, no PITR
	// window yet" rather than implying a misconfiguration. Assert the
	// clarified wording so the dump-only semantics are locked in.
	if !strings.Contains(skc.Message, "no PITR binlog manifests") {
		t.Errorf("Message: want it to mention %q, got %q", "no PITR binlog manifests", skc.Message)
	}
	if !strings.Contains(skc.Message, "no point-in-time window") {
		t.Errorf("Message: want it to mention %q (dump-only semantics), got %q", "no point-in-time window", skc.Message)
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
		return
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
		return
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
		return
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
		return
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
		return
	}
	ltt1 := brc1.LastTransitionTime
	ltt1sk := skc1.LastTransitionTime

	// Second reconcile with the same store state.
	cr2, _ := reconcileStandby(t, r, "test-sc", "default")

	brc2 := standbyFindCondition(cr2, v1alpha1.StandbyConditionBucketReadable)
	skc2 := standbyFindCondition(cr2, v1alpha1.StandbyConditionSourceConfigKnown)
	if brc2 == nil || skc2 == nil {
		t.Fatal("conditions missing after second reconcile")
		return
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

// --- TestMysqlStandbyCluster_DiscoveryIntervalClampedToFloor ----------------

// TestMysqlStandbyCluster_DiscoveryIntervalClampedToFloor verifies that a
// sub-30s discoveryInterval that slipped past CRD admission (older client or a
// hand-edited object) is clamped to the 30s floor, so RequeueAfter never
// degenerates to 0 and silently stops the discovery loop.
func TestMysqlStandbyCluster_DiscoveryIntervalClampedToFloor(t *testing.T) {
	cr := minimalStandbyCR("test-sc", "default")
	cr.Spec.Freshness = &v1alpha1.StandbyFreshnessSpec{
		DiscoveryInterval: &metav1.Duration{Duration: time.Second}, // below the 30s floor
	}
	store := &fakeStore{Objects: map[string][]byte{}}
	r, _ := newTestReconciler(t, []client.Object{cr}, store)

	_, res := reconcileStandby(t, r, "test-sc", "default")

	if res.RequeueAfter != standbyMinDiscoveryInterval {
		t.Errorf("requeue: want clamp to %v, got %v", standbyMinDiscoveryInterval, res.RequeueAfter)
	}
}

// standbyDrainEvents non-blockingly drains all currently-buffered events from a
// FakeRecorder and returns them. Used to assert presence/absence of specific
// event reasons (e.g. that no BucketScanned is emitted on an incomplete scan).
func standbyDrainEvents(rec *record.FakeRecorder) []string {
	var out []string
	for {
		select {
		case ev := <-rec.Events:
			out = append(out, ev)
		default:
			return out
		}
	}
}

// standbyContainsEvent reports whether any drained event contains substr.
func standbyContainsEvent(events []string, substr string) bool {
	for _, ev := range events {
		if strings.Contains(ev, substr) {
			return true
		}
	}
	return false
}

// --- TestMysqlStandbyCluster_ColdStartManyDumpsCorrect ----------------------

// TestMysqlStandbyCluster_ColdStartManyDumpsCorrect seeds N=50 dump dirs with
// random-ish names and varied end times against an empty cache (cold start) and
// asserts that one reconcile selects the true timestamp-newest dump — i.e. the
// bounded-concurrency fan-out does not change the deterministic selection.
func TestMysqlStandbyCluster_ColdStartManyDumpsCorrect(t *testing.T) {
	cr := minimalStandbyCR("test-sc", "default")

	const n = 50
	objects := map[string][]byte{
		"orders/binlogs/manifest-dc1.json": []byte(`{"version":1,"site":"dc1","files":[]}`),
	}
	// Deterministic pseudo-random dir names and end times. The newest end time
	// is intentionally NOT on the lexicographically-last dir so concurrency +
	// selection are both exercised.
	base := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	var wantNewestDir string
	var wantNewestEnd time.Time
	var wantNewestGtid string
	for i := 0; i < n; i++ {
		// Scatter end times: dir index i gets end = base + (i*7 % n) hours, so
		// the maximum is not at i == n-1.
		offsetHours := (i * 7) % n
		end := base.Add(time.Duration(offsetHours) * time.Hour)
		// Name with a fixed-width index but a shuffled alpha prefix so lex order
		// differs from time order.
		name := fmt.Sprintf("dump-%c-%02d", 'a'+byte((i*13)%26), i)
		dir := "orders/" + name
		gtid := fmt.Sprintf("g%d:1-%d", i, i+1)
		objects[dir+"/@.json"] = []byte(fmt.Sprintf(
			`{"end":%q,"gtidExecuted":%q,"totalBytes":%d}`,
			end.Format(time.RFC3339), gtid, 1000+i))
		if wantNewestDir == "" || end.After(wantNewestEnd) {
			wantNewestDir = name
			wantNewestEnd = end
			wantNewestGtid = gtid
		}
	}

	store := &fakeStore{Objects: objects}
	r, recorder := newTestReconciler(t, []client.Object{cr}, store)

	updated, _ := reconcileStandby(t, r, "test-sc", "default")

	// A successful cold start IS a state transition, so the positive
	// BucketScanned event path must fire (the incompleteness paths suppress it).
	if events := standbyDrainEvents(recorder); !standbyContainsEvent(events, "BucketScanned") {
		t.Errorf("want BucketScanned event on successful cold start, got events: %v", events)
	}

	d := updated.Status.Discovered
	if d == nil {
		t.Fatal("status.discovered is nil")
		return
	}
	if d.DumpName != wantNewestDir {
		t.Errorf("DumpName: want timestamp-newest %q, got %q", wantNewestDir, d.DumpName)
	}
	if d.DumpGtidExecuted != wantNewestGtid {
		t.Errorf("DumpGtidExecuted: want %q, got %q", wantNewestGtid, d.DumpGtidExecuted)
	}
	skc := standbyFindCondition(updated, v1alpha1.StandbyConditionSourceConfigKnown)
	if skc == nil || skc.Status != metav1.ConditionTrue || skc.Reason != "DumpFound" {
		t.Errorf("SourceConfigKnown: want True/DumpFound, got %v", skc)
	}
	// Cold start reads each @.json exactly once.
	for key := range objects {
		if !strings.HasSuffix(key, "/@.json") {
			continue
		}
		if got := store.GetCalls[key]; got != 1 {
			t.Errorf("@.json %q GET count: want 1 on cold start, got %d", key, got)
		}
	}
}

// --- TestMysqlStandbyCluster_DeadlineDuringReads ----------------------------

// TestMysqlStandbyCluster_DeadlineDuringReads verifies that when the scan's read
// phase is cut short by a context deadline (Get blocks until ctx done), the
// reconciler stamps SourceConfigKnown=False/ScanIncomplete, BucketReadable stays
// True/ListSucceeded (List itself succeeded), the previous status.discovered is
// preserved byte-identical, and NO BucketScanned event is emitted.
func TestMysqlStandbyCluster_DeadlineDuringReads(t *testing.T) {
	cr := minimalStandbyCR("test-sc", "default")
	store := &fakeStore{Objects: map[string][]byte{
		"orders/orders-nightly-20260520/@.json": []byte(`{"end":"2026-05-20T04:00:00Z","gtidExecuted":"abc:1-100","totalBytes":111}`),
		"orders/binlogs/manifest-dc1.json":      []byte(`{"version":1,"site":"dc1","files":[{"name":"b1","remotePath":"orders/binlogs/dc1/b1","size":7,"firstEventTime":"2026-05-20T00:00:00Z","lastEventTime":"2026-05-20T03:00:00Z","archivedAt":"2026-05-20T03:00:01Z"}]}`),
	}}
	r, recorder := newTestReconciler(t, []client.Object{cr}, store)

	// First reconcile succeeds and stamps a known last-known-good discovered.
	good, _ := reconcileStandby(t, r, "test-sc", "default")
	if good.Status.Discovered == nil || good.Status.Discovered.DumpName != "orders-nightly-20260520" {
		t.Fatalf("first scan did not establish last-known-good: %+v", good.Status.Discovered)
	}
	wantDiscovered := good.Status.Discovered.DeepCopy()
	_ = standbyDrainEvents(recorder) // clear the first scan's BucketScanned event

	// Now force the read phase to be cut short. BlockGet makes every @.json Get
	// block until the (short-deadline) context fires, returning ctx.Err().
	// Clear the warm cache so a Get is actually attempted this scan.
	store.BlockGet = true
	r.dumpMetaCache = nil

	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	updated, _ := reconcileStandbyWithCtx(t, ctx, r, "test-sc", "default")

	brc := standbyFindCondition(updated, v1alpha1.StandbyConditionBucketReadable)
	if brc == nil || brc.Status != metav1.ConditionTrue || brc.Reason != "ListSucceeded" {
		t.Errorf("BucketReadable: want True/ListSucceeded (List succeeded), got %v", brc)
	}
	skc := standbyFindCondition(updated, v1alpha1.StandbyConditionSourceConfigKnown)
	if skc == nil || skc.Status != metav1.ConditionFalse || skc.Reason != "ScanIncomplete" {
		t.Errorf("SourceConfigKnown: want False/ScanIncomplete, got %v", skc)
	}

	// Last-known-good discovered preserved (compare ignoring LastScanAt, which
	// stampConditionsAndRequeue does not touch when discovered==nil anyway).
	if updated.Status.Discovered == nil {
		t.Fatal("status.discovered was cleared on an incomplete scan; must be preserved")
	}
	if updated.Status.Discovered.DumpName != wantDiscovered.DumpName ||
		updated.Status.Discovered.DumpGtidExecuted != wantDiscovered.DumpGtidExecuted ||
		updated.Status.Discovered.ArchivedBinlogCount != wantDiscovered.ArchivedBinlogCount {
		t.Errorf("status.discovered changed on incomplete scan: want %+v, got %+v",
			wantDiscovered, updated.Status.Discovered)
	}

	// No BucketScanned event on an incomplete scan.
	events := standbyDrainEvents(recorder)
	if standbyContainsEvent(events, "BucketScanned") {
		t.Errorf("BucketScanned event must NOT be emitted on an incomplete scan; got events: %v", events)
	}
}

// --- TestMysqlStandbyCluster_DeadlineDuringManifestLoop ---------------------

// TestMysqlStandbyCluster_DeadlineDuringManifestLoop verifies the MANIFEST-phase
// incompleteness path (VF1): the @.json fan-out completes (cache hits, no Get),
// but the manifest loop's LoadManifest is cut short by a deadline. The reconciler
// must stamp BucketReadable=True/ListSucceeded (List succeeded), SourceConfigKnown
// =False/ScanIncomplete, preserve status.discovered, emit no BucketScanned event,
// and produce a message that names the MANIFEST phase (not "read N/N dump @.json
// candidates", which would misreport a manifest-phase stall).
func TestMysqlStandbyCluster_DeadlineDuringManifestLoop(t *testing.T) {
	cr := minimalStandbyCR("test-sc", "default")
	store := &fakeStore{Objects: map[string][]byte{
		"orders/orders-nightly-20260520/@.json": []byte(`{"end":"2026-05-20T04:00:00Z","gtidExecuted":"abc:1-100","totalBytes":111}`),
		"orders/binlogs/manifest-dc1.json":      []byte(`{"version":1,"site":"dc1","files":[{"name":"b1","remotePath":"orders/binlogs/dc1/b1","size":7,"firstEventTime":"2026-05-20T00:00:00Z","lastEventTime":"2026-05-20T03:00:00Z","archivedAt":"2026-05-20T03:00:01Z"}]}`),
	}}
	r, recorder := newTestReconciler(t, []client.Object{cr}, store)

	// First reconcile succeeds: warms dumpMetaCache for the @.json AND stamps a
	// known last-known-good discovered.
	good, _ := reconcileStandby(t, r, "test-sc", "default")
	if good.Status.Discovered == nil || good.Status.Discovered.DumpName != "orders-nightly-20260520" {
		t.Fatalf("first scan did not establish last-known-good: %+v", good.Status.Discovered)
	}
	wantDiscovered := good.Status.Discovered.DeepCopy()
	_ = standbyDrainEvents(recorder) // clear the first scan's BucketScanned event

	// Second scan: BlockGet blocks every store.Get until ctx fires. The @.json is
	// served from the warm cache (no Get), so the ONLY blocking Get is the
	// manifest LoadManifest — the cut-short therefore lands in the MANIFEST phase.
	store.BlockGet = true

	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	updated, _ := reconcileStandbyWithCtx(t, ctx, r, "test-sc", "default")

	// Sanity: the @.json was NOT re-fetched this scan (cache hit), so the only
	// blocking Get really was the manifest.
	if got := store.GetCalls["orders/orders-nightly-20260520/@.json"]; got != 1 {
		t.Errorf("@.json should be a cache hit on the 2nd scan (Get count want 1, got %d); "+
			"the manifest must be the only blocking Get", got)
	}

	brc := standbyFindCondition(updated, v1alpha1.StandbyConditionBucketReadable)
	if brc == nil || brc.Status != metav1.ConditionTrue || brc.Reason != "ListSucceeded" {
		t.Errorf("BucketReadable: want True/ListSucceeded (List succeeded), got %v", brc)
	}
	skc := standbyFindCondition(updated, v1alpha1.StandbyConditionSourceConfigKnown)
	if skc == nil || skc.Status != metav1.ConditionFalse || skc.Reason != "ScanIncomplete" {
		t.Fatalf("SourceConfigKnown: want False/ScanIncomplete, got %v", skc)
	}
	// The message must name the manifest phase and must NOT claim a dump @.json
	// candidate ratio (VF1: that wording misleads triage for a manifest stall).
	if !strings.Contains(skc.Message, "manifest") {
		t.Errorf("ScanIncomplete message should name the manifest phase, got %q", skc.Message)
	}
	if strings.Contains(skc.Message, "dump @.json candidates") {
		t.Errorf("ScanIncomplete message must NOT report a dump @.json candidate ratio for a "+
			"manifest-phase stall, got %q", skc.Message)
	}

	// status.discovered preserved byte-identical (ignoring LastScanAt).
	if updated.Status.Discovered == nil {
		t.Fatal("status.discovered was cleared on an incomplete scan; must be preserved")
	}
	if updated.Status.Discovered.DumpName != wantDiscovered.DumpName ||
		updated.Status.Discovered.DumpGtidExecuted != wantDiscovered.DumpGtidExecuted ||
		updated.Status.Discovered.ArchivedBinlogCount != wantDiscovered.ArchivedBinlogCount {
		t.Errorf("status.discovered changed on incomplete manifest scan: want %+v, got %+v",
			wantDiscovered, updated.Status.Discovered)
	}

	// No BucketScanned event on an incomplete scan.
	if events := standbyDrainEvents(recorder); standbyContainsEvent(events, "BucketScanned") {
		t.Errorf("BucketScanned event must NOT be emitted on an incomplete scan; got events: %v", events)
	}
}

// --- TestMysqlStandbyCluster_DeadlineDuringList -----------------------------

// TestMysqlStandbyCluster_DeadlineDuringList verifies that when List itself is
// cut short by a context deadline, the reconciler stamps
// BucketReadable=False/ScanIncomplete (NOT ListFailed) and preserves the
// previous status.discovered.
func TestMysqlStandbyCluster_DeadlineDuringList(t *testing.T) {
	cr := minimalStandbyCR("test-sc", "default")
	store := &fakeStore{Objects: map[string][]byte{
		"orders/orders-nightly-20260520/@.json": []byte(`{"end":"2026-05-20T04:00:00Z","gtidExecuted":"abc:1-100"}`),
		"orders/binlogs/manifest-dc1.json":      []byte(`{"version":1,"site":"dc1","files":[]}`),
	}}
	r, recorder := newTestReconciler(t, []client.Object{cr}, store)

	// First reconcile succeeds (establish last-known-good).
	good, _ := reconcileStandby(t, r, "test-sc", "default")
	if good.Status.Discovered == nil || good.Status.Discovered.DumpName == "" {
		t.Fatalf("first scan did not establish last-known-good: %+v", good.Status.Discovered)
	}
	wantDumpName := good.Status.Discovered.DumpName
	_ = standbyDrainEvents(recorder)

	// Force List to block until the (short-deadline) ctx fires.
	store.BlockList = true

	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	updated, _ := reconcileStandbyWithCtx(t, ctx, r, "test-sc", "default")

	brc := standbyFindCondition(updated, v1alpha1.StandbyConditionBucketReadable)
	if brc == nil || brc.Status != metav1.ConditionFalse || brc.Reason != "ScanIncomplete" {
		t.Errorf("BucketReadable: want False/ScanIncomplete (deadline during List), got %v", brc)
	}
	skc := standbyFindCondition(updated, v1alpha1.StandbyConditionSourceConfigKnown)
	if skc == nil || skc.Status != metav1.ConditionFalse || skc.Reason != "ScanIncomplete" {
		t.Errorf("SourceConfigKnown: want False/ScanIncomplete, got %v", skc)
	}
	if updated.Status.Discovered == nil || updated.Status.Discovered.DumpName != wantDumpName {
		t.Errorf("status.discovered not preserved on incomplete List: want DumpName=%q, got %+v",
			wantDumpName, updated.Status.Discovered)
	}
	// No ListFailed warning event on a deadline (it is not a genuine failure).
	events := standbyDrainEvents(recorder)
	if standbyContainsEvent(events, "ListFailed") {
		t.Errorf("ListFailed event must NOT be emitted on a List deadline; got events: %v", events)
	}
	if standbyContainsEvent(events, "BucketScanned") {
		t.Errorf("BucketScanned event must NOT be emitted on an incomplete scan; got events: %v", events)
	}
}

// --- TestMysqlStandbyCluster_AllCandidatesMalformed -------------------------

// TestMysqlStandbyCluster_AllCandidatesMalformed verifies the all-candidates-
// failed NON-ctx fallback (VF2): when every dump @.json is genuinely malformed
// (not a deadline), the reconciler must stamp SourceConfigKnown=False/
// MetadataUnreadable — NOT ScanIncomplete — and the selected DumpName must be the
// lexicographically-newest dir (the documented all-failed fallback). This guards
// the PR's central distinction: malformed metadata is MetadataUnreadable, only a
// context cut-short is ScanIncomplete.
func TestMysqlStandbyCluster_AllCandidatesMalformed(t *testing.T) {
	cr := minimalStandbyCR("test-sc", "default")
	// Three dump dirs, all with invalid JSON. No deadline is imposed.
	store := &fakeStore{Objects: map[string][]byte{
		"orders/dump-a/@.json":             []byte(`{not valid json`),
		"orders/dump-b/@.json":             []byte(`}}}garbage`),
		"orders/dump-c/@.json":             []byte(`<<<>>>`),
		"orders/binlogs/manifest-dc1.json": []byte(`{"version":1,"site":"dc1","files":[]}`),
	}}
	r, recorder := newTestReconciler(t, []client.Object{cr}, store)

	updated, _ := reconcileStandby(t, r, "test-sc", "default")

	// List succeeded, so BucketReadable stays True.
	brc := standbyFindCondition(updated, v1alpha1.StandbyConditionBucketReadable)
	if brc == nil || brc.Status != metav1.ConditionTrue || brc.Reason != "ListSucceeded" {
		t.Errorf("BucketReadable: want True/ListSucceeded, got %v", brc)
	}
	// Malformed (non-ctx) metadata ⇒ MetadataUnreadable, NOT ScanIncomplete.
	skc := standbyFindCondition(updated, v1alpha1.StandbyConditionSourceConfigKnown)
	if skc == nil || skc.Status != metav1.ConditionFalse || skc.Reason != "MetadataUnreadable" {
		t.Fatalf("SourceConfigKnown: want False/MetadataUnreadable (malformed, not a deadline), got %v", skc)
	}
	if skc.Reason == "ScanIncomplete" {
		t.Errorf("malformed @.json must NOT be reported as ScanIncomplete")
	}
	// The selected dump (surfaced in the MetadataUnreadable message) must be the
	// lexicographically-newest dir (all-failed fallback). The sorted dump dirs are
	// dump-a < dump-b < dump-c, so lexNewest is dump-c. The MetadataUnreadable path
	// intentionally does NOT publish status.discovered (it preserves last-known-good,
	// which is nil on this first scan), so we assert via the condition message.
	wantDumpName := path.Base("orders/dump-c")
	if !strings.Contains(skc.Message, wantDumpName) {
		t.Errorf("SourceConfigKnown message should name the lex-newest fallback dump %q, got %q",
			wantDumpName, skc.Message)
	}
	if updated.Status.Discovered != nil {
		t.Errorf("status.discovered must stay nil on a first scan with all-unreadable @.json "+
			"(no last-known-good to preserve), got %+v", updated.Status.Discovered)
	}
	// MetadataParseFailed warning event emitted (not ScanIncomplete-related).
	events := standbyDrainEvents(recorder)
	if !standbyContainsEvent(events, "MetadataParseFailed") {
		t.Errorf("want MetadataParseFailed event for malformed @.json, got events: %v", events)
	}
	if standbyContainsEvent(events, "BucketScanned") {
		t.Errorf("BucketScanned must NOT be emitted when the dump @.json is unreadable; got events: %v", events)
	}
}

// --- TestMysqlStandbyCluster_TwoCRPruneIsolation ----------------------------

// TestMysqlStandbyCluster_TwoCRPruneIsolation verifies that two
// MysqlStandbyClusters on sibling prefixes (orders/west, orders/east) sharing
// one reconciler's dumpMetaCache do not evict each other's entries when one
// reconciles (the prune is scoped one level deep under each scan's prefix).
func TestMysqlStandbyCluster_TwoCRPruneIsolation(t *testing.T) {
	west := minimalStandbyCR("sc-west", "default")
	west.Spec.Source.Storage.S3.Prefix = "orders/west"
	east := minimalStandbyCR("sc-east", "default")
	east.Spec.Source.Storage.S3.Prefix = "orders/east"

	westKey := "orders/west/dump-w/@.json"
	eastKey := "orders/east/dump-e/@.json"
	store := &fakeStore{Objects: map[string][]byte{
		westKey:                               []byte(`{"end":"2026-05-20T04:00:00Z","gtidExecuted":"west:1-1"}`),
		"orders/west/binlogs/manifest-a.json": []byte(`{"version":1,"site":"a","files":[]}`),
		eastKey:                               []byte(`{"end":"2026-05-20T05:00:00Z","gtidExecuted":"east:1-1"}`),
		"orders/east/binlogs/manifest-b.json": []byte(`{"version":1,"site":"b","files":[]}`),
	}}

	scheme := newStandbyScheme(t)
	fakeClient := fake.NewClientBuilder().WithScheme(scheme).
		WithStatusSubresource(&v1alpha1.MysqlStandbyCluster{}).
		WithObjects(west, east).Build()
	r := &MysqlStandbyClusterReconciler{
		Client:   fakeClient,
		Scheme:   scheme,
		Recorder: record.NewFakeRecorder(32),
		newStoreFunc: func(_ context.Context, _ *sidecar.PITRConfig) (sidecar.ArchiveStore, error) {
			return store, nil
		},
	}

	// Warm both CRs' cache entries.
	reconcileStandby(t, r, "sc-west", "default")
	reconcileStandby(t, r, "sc-east", "default")
	if _, ok := r.dumpMetaCache[westKey]; !ok {
		t.Fatalf("west @.json not cached after warm-up")
	}
	if _, ok := r.dumpMetaCache[eastKey]; !ok {
		t.Fatalf("east @.json not cached after warm-up")
	}

	// Reconcile west again: its prune must NOT evict east's sibling-prefix entry.
	reconcileStandby(t, r, "sc-west", "default")
	if _, ok := r.dumpMetaCache[eastKey]; !ok {
		t.Errorf("reconciling sc-west evicted sc-east's cache entry %q (prune scope leaked)", eastKey)
	}
	if _, ok := r.dumpMetaCache[westKey]; !ok {
		t.Errorf("sc-west's own cache entry %q was dropped", westKey)
	}
}

// --- TestStandbyReadTimeout / TestStandbyListTimeout ------------------------

// TestStandbyListTimeout verifies the List-phase budget is min(interval, 30s).
func TestStandbyListTimeout(t *testing.T) {
	cases := []struct {
		interval time.Duration
		want     time.Duration
	}{
		{interval: 5 * time.Minute, want: standbyDefaultScanTimeout}, // 30s cap
		{interval: 10 * time.Second, want: 10 * time.Second},         // below 30s → interval
		{interval: 30 * time.Second, want: 30 * time.Second},
		{interval: 0, want: standbyDefaultScanTimeout},
	}
	for _, c := range cases {
		if got := standbyListTimeout(c.interval); got != c.want {
			t.Errorf("standbyListTimeout(%v): want %v, got %v", c.interval, c.want, got)
		}
	}
}

// TestStandbyReadTimeout verifies the work-aware read budget:
// clamp(perItem*work, floor=30s, cap=interval).
func TestStandbyReadTimeout(t *testing.T) {
	cases := []struct {
		name     string
		interval time.Duration
		work     int
		want     time.Duration
	}{
		{name: "tiny work clamps to floor", interval: 5 * time.Minute, work: 1, want: standbyDefaultScanTimeout},
		{name: "zero work clamps to floor", interval: 5 * time.Minute, work: 0, want: standbyDefaultScanTimeout},
		{name: "mid work scales", interval: 5 * time.Minute, work: 100, want: 100 * standbyPerItemScanBudget},
		{name: "huge work clamps to interval cap", interval: 2 * time.Minute, work: 10000, want: 2 * time.Minute},
		{name: "work above floor below cap", interval: 5 * time.Minute, work: 45, want: 45 * standbyPerItemScanBudget},
	}
	for _, c := range cases {
		if got := standbyReadTimeout(c.interval, c.work); got != c.want {
			t.Errorf("%s: standbyReadTimeout(%v, %d): want %v, got %v", c.name, c.interval, c.work, c.want, got)
		}
	}
}

// --- TestMysqlStandbyCluster_DeterministicAcrossListOrder -------------------

// TestMysqlStandbyCluster_DeterministicAcrossListOrder verifies the
// concurrency-based selection is order-independent: the same dump set yields the
// same selection regardless of the (map-iteration-random) List order. Two equal
// end times resolve to the lex-smallest dir (documented tie-break, V9).
func TestMysqlStandbyCluster_DeterministicAcrossListOrder(t *testing.T) {
	mkStore := func() *fakeStore {
		return &fakeStore{Objects: map[string][]byte{
			// Two dumps share the newest end time; lex-smallest dir wins.
			"orders/aaa-tie/@.json":            []byte(`{"end":"2026-05-20T04:00:00Z","gtidExecuted":"aaa"}`),
			"orders/zzz-tie/@.json":            []byte(`{"end":"2026-05-20T04:00:00Z","gtidExecuted":"zzz"}`),
			"orders/mmm-older/@.json":          []byte(`{"end":"2026-05-19T04:00:00Z","gtidExecuted":"mmm"}`),
			"orders/binlogs/manifest-dc1.json": []byte(`{"version":1,"site":"dc1","files":[]}`),
		}}
	}
	for i := 0; i < 10; i++ {
		cr := minimalStandbyCR("test-sc", "default")
		r, _ := newTestReconciler(t, []client.Object{cr}, mkStore())
		updated, _ := reconcileStandby(t, r, "test-sc", "default")
		d := updated.Status.Discovered
		if d == nil {
			t.Fatalf("iter %d: status.discovered nil", i)
			continue
		}
		if d.DumpName != "aaa-tie" {
			t.Errorf("iter %d: tie-break: want lex-smallest aaa-tie, got %q", i, d.DumpName)
		}
		if d.DumpGtidExecuted != "aaa" {
			t.Errorf("iter %d: want gtid aaa, got %q", i, d.DumpGtidExecuted)
		}
	}
}
