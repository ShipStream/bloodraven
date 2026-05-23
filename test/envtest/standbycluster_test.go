//go:build envtest

package envtest

import (
	"context"
	"errors"
	"io"
	"testing"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/tools/record"
	ctrl "sigs.k8s.io/controller-runtime"

	v1alpha1 "github.com/shipstream/bloodraven/api/v1alpha1"
	"github.com/shipstream/bloodraven/internal/controller"
	"github.com/shipstream/bloodraven/internal/sidecar"
)

// envtestFakeStore is a minimal ArchiveStore for envtest scenarios. Objects
// are keyed by their storage path. ListErr forces a List error when set.
type envtestFakeStore struct {
	Objects map[string][]byte
	ListErr error
}

func (s *envtestFakeStore) Put(_ context.Context, key string, r io.Reader, _ int64) error {
	data, err := io.ReadAll(r)
	if err != nil {
		return err
	}
	s.Objects[key] = data
	return nil
}
func (s *envtestFakeStore) PutFile(_ context.Context, _, _ string) error { return nil }
func (s *envtestFakeStore) Delete(_ context.Context, key string) error {
	delete(s.Objects, key)
	return nil
}
func (s *envtestFakeStore) GetFile(_ context.Context, _, _ string) error { return nil }
func (s *envtestFakeStore) Get(_ context.Context, key string) ([]byte, bool, error) {
	v, ok := s.Objects[key]
	return v, ok, nil
}
func (s *envtestFakeStore) List(_ context.Context, prefix string) ([]string, error) {
	if s.ListErr != nil {
		return nil, s.ListErr
	}
	var out []string
	for k := range s.Objects {
		if len(k) >= len(prefix) && k[:len(prefix)] == prefix {
			out = append(out, k)
		}
	}
	return out, nil
}

// newStandbyReconciler builds a MysqlStandbyClusterReconciler for envtest
// that uses the provided store instead of a real S3 connection.
func newStandbyReconciler(store sidecar.ArchiveStore) *controller.MysqlStandbyClusterReconciler {
	rec := record.NewFakeRecorder(32)
	r := &controller.MysqlStandbyClusterReconciler{
		Client:   k8sClient,
		Scheme:   scheme,
		Recorder: rec,
	}
	r.SetNewStoreFunc(func(_ context.Context, _ *sidecar.PITRConfig) (sidecar.ArchiveStore, error) {
		return store, nil
	})
	return r
}

// minimalEnvtestStandby returns a MysqlStandbyCluster suitable for envtest.
func minimalEnvtestStandby(name, ns string) *v1alpha1.MysqlStandbyCluster {
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
						// No CredentialsSecret: test uses injected store.
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

// envtestStandbyFindCondition finds a named condition on the CR, or nil.
func envtestStandbyFindCondition(cr *v1alpha1.MysqlStandbyCluster, condType string) *metav1.Condition {
	for i := range cr.Status.Conditions {
		if cr.Status.Conditions[i].Type == condType {
			return &cr.Status.Conditions[i]
		}
	}
	return nil
}

// TestMysqlStandbyCluster_EnvtestCreate_StampsConditions creates a
// MysqlStandbyCluster CR against a real API server and verifies that a single
// Reconcile call using a fake bucket populates status.discovered and stamps
// both BucketReadable and SourceConfigKnown conditions.
func TestMysqlStandbyCluster_EnvtestCreate_StampsConditions(t *testing.T) {
	ns := "default"
	scName := "envtest-standby"

	// Fake bucket with a dump and one manifest.
	store := &envtestFakeStore{Objects: map[string][]byte{
		"orders/orders-nightly-20260520/@.json": []byte(
			`{"end":"2026-05-20T04:00:00Z","gtidExecuted":"abc:1-100","totalBytes":512000}`),
		"orders/binlogs/manifest-site1.json": []byte(
			`{"version":1,"site":"site1","files":[{"name":"b1","remotePath":"orders/binlogs/site1/b1","size":1024,"firstEventTime":"2026-05-20T00:00:00Z","lastEventTime":"2026-05-20T03:59:59Z","archivedAt":"2026-05-20T04:00:01Z"}]}`),
	}}

	cr := minimalEnvtestStandby(scName, ns)
	if err := k8sClient.Create(ctx, cr); err != nil {
		t.Fatalf("create CR: %v", err)
	}
	t.Cleanup(func() { _ = k8sClient.Delete(ctx, cr) })

	r := newStandbyReconciler(store)
	res, err := r.Reconcile(ctx, ctrl.Request{
		NamespacedName: types.NamespacedName{Name: scName, Namespace: ns},
	})
	if err != nil {
		t.Fatalf("reconcile returned error: %v", err)
	}
	if res.RequeueAfter == 0 {
		t.Error("want non-zero requeue interval")
	}

	// Fetch the updated status.
	var updated v1alpha1.MysqlStandbyCluster
	if err := k8sClient.Get(ctx, types.NamespacedName{Name: scName, Namespace: ns}, &updated); err != nil {
		t.Fatalf("get after reconcile: %v", err)
	}

	// BucketReadable=True.
	brc := envtestStandbyFindCondition(&updated, v1alpha1.StandbyConditionBucketReadable)
	if brc == nil {
		t.Fatal("BucketReadable condition missing")
	}
	if brc.Status != metav1.ConditionTrue {
		t.Errorf("BucketReadable: want True, got %s (reason=%s, msg=%s)", brc.Status, brc.Reason, brc.Message)
	}

	// SourceConfigKnown=True.
	skc := envtestStandbyFindCondition(&updated, v1alpha1.StandbyConditionSourceConfigKnown)
	if skc == nil {
		t.Fatal("SourceConfigKnown condition missing")
	}
	if skc.Status != metav1.ConditionTrue {
		t.Errorf("SourceConfigKnown: want True, got %s (reason=%s, msg=%s)", skc.Status, skc.Reason, skc.Message)
	}

	// status.discovered populated.
	d := updated.Status.Discovered
	if d == nil {
		t.Fatal("status.discovered is nil")
	}
	if d.DumpName != "orders-nightly-20260520" {
		t.Errorf("DumpName: want orders-nightly-20260520, got %q", d.DumpName)
	}
	if d.ManifestCount != 1 {
		t.Errorf("ManifestCount: want 1, got %d", d.ManifestCount)
	}
	if d.LastScanAt == nil {
		t.Error("LastScanAt is nil")
	}
}

// TestMysqlStandbyCluster_EnvtestCreate_BucketUnreadable creates a CR against
// a real API server and verifies that a list error stamps BucketReadable=False.
func TestMysqlStandbyCluster_EnvtestCreate_BucketUnreadable(t *testing.T) {
	ns := "default"
	scName := "envtest-standby-unreadable"

	store := &envtestFakeStore{
		Objects: map[string][]byte{},
		ListErr: errors.New("bucket not found"),
	}

	cr := minimalEnvtestStandby(scName, ns)
	if err := k8sClient.Create(ctx, cr); err != nil {
		t.Fatalf("create CR: %v", err)
	}
	t.Cleanup(func() { _ = k8sClient.Delete(ctx, cr) })

	r := newStandbyReconciler(store)
	_, err := r.Reconcile(ctx, ctrl.Request{
		NamespacedName: types.NamespacedName{Name: scName, Namespace: ns},
	})
	if err != nil {
		// A requeue error is acceptable here; the condition is the signal.
		t.Logf("reconcile returned (expected) error: %v", err)
	}

	var updated v1alpha1.MysqlStandbyCluster
	if err := k8sClient.Get(ctx, types.NamespacedName{Name: scName, Namespace: ns}, &updated); err != nil {
		t.Fatalf("get after reconcile: %v", err)
	}

	brc := envtestStandbyFindCondition(&updated, v1alpha1.StandbyConditionBucketReadable)
	if brc == nil {
		t.Fatal("BucketReadable condition missing")
	}
	if brc.Status != metav1.ConditionFalse {
		t.Errorf("BucketReadable: want False, got %s", brc.Status)
	}
	if brc.Reason != "ListFailed" {
		t.Errorf("Reason: want ListFailed, got %s", brc.Reason)
	}
}

// TestMysqlStandbyCluster_EnvtestCreate_DiscoveryIntervalOverride verifies
// that setting spec.freshness.discoveryInterval overrides the default requeue.
func TestMysqlStandbyCluster_EnvtestCreate_DiscoveryIntervalOverride(t *testing.T) {
	ns := "default"
	scName := "envtest-standby-interval"

	store := &envtestFakeStore{Objects: map[string][]byte{}}

	cr := minimalEnvtestStandby(scName, ns)
	customInterval := 3 * time.Minute
	cr.Spec.Freshness = &v1alpha1.StandbyFreshnessSpec{
		DiscoveryInterval: &metav1.Duration{Duration: customInterval},
	}

	if err := k8sClient.Create(ctx, cr); err != nil {
		t.Fatalf("create CR: %v", err)
	}
	t.Cleanup(func() { _ = k8sClient.Delete(ctx, cr) })

	r := newStandbyReconciler(store)
	res, err := r.Reconcile(ctx, ctrl.Request{
		NamespacedName: types.NamespacedName{Name: scName, Namespace: ns},
	})
	if err != nil {
		t.Logf("reconcile error: %v", err)
	}
	if res.RequeueAfter != customInterval {
		t.Errorf("requeue: want %v, got %v", customInterval, res.RequeueAfter)
	}
}
