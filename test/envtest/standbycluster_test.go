//go:build envtest

package envtest

import (
	"context"
	"errors"
	"fmt"
	"io"
	"strings"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/tools/record"
	ctrl "sigs.k8s.io/controller-runtime"

	v1alpha1 "github.com/shipstream/bloodraven/api/v1alpha1"
	"github.com/shipstream/bloodraven/internal/controller"
	"github.com/shipstream/bloodraven/internal/pitrarchive"
	"github.com/shipstream/bloodraven/internal/sidecar"
)

// envtestFakeStore is a minimal ArchiveStore for envtest scenarios. Objects
// are keyed by their storage path. ListErr forces a List error when set.
type envtestFakeStore struct {
	Objects map[string][]byte
	ListErr error
}

func (s *envtestFakeStore) Open(ctx context.Context, key string) (io.ReadCloser, error) {
	data, ok, err := s.Get(ctx, key)
	if err != nil {
		return nil, err
	}
	if !ok {
		return nil, fmt.Errorf("missing %s", key)
	}
	return io.NopCloser(strings.NewReader(string(data))), nil
}

func (s *envtestFakeStore) BinlogContentID(digest string) string { return digest }

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
				S3: v1alpha1.StandbyS3Source{
					Bucket: "my-bucket",
					Prefix: "orders",
					// CredentialsSecret is required by the CRD schema. The
					// reconciler reads the Secret before the SetNewStoreFunc
					// injection returns the fake store, so the tests create
					// it with ensureEnvtestS3CredsSecret.
					CredentialsSecretName: "envtest-s3-creds",
				},
			},
		},
	}
}

// ensureEnvtestS3CredsSecret creates the dummy AWS-credentials Secret that
// the standby fixture references in spec.source.s3.credentialsSecretName.
// The reconciler's resolveS3CredsToDir path always reads the Secret before
// the SetNewStoreFunc-injected fake store is dispatched, so admission +
// reconcile both require the Secret to exist. Idempotent across tests in
// the same namespace.
func ensureEnvtestS3CredsSecret(t *testing.T, ns, name string) {
	t.Helper()
	sec := &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{
			Name:      name,
			Namespace: ns,
		},
		StringData: map[string]string{
			"AWS_ACCESS_KEY_ID":     "AKIAEXAMPLE",
			"AWS_SECRET_ACCESS_KEY": "secret",
		},
	}
	if err := k8sClient.Create(ctx, sec); err != nil {
		// Already exists from a sibling test in this namespace — fine.
		var existing corev1.Secret
		if getErr := k8sClient.Get(ctx, types.NamespacedName{Name: name, Namespace: ns}, &existing); getErr != nil {
			t.Fatalf("create S3 creds Secret %q: %v (subsequent Get also failed: %v)", name, err, getErr)
		}
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
		"orders/binlogs/archive.json": envtestArchiveDescriptor(t),
		"orders/binlogs/manifest-site1.json": []byte(
			`{"version":2,"archiveId":"ar1-aaaaaaaaaaaaaaaaaaaaaaaaaa","site":"site1","files":[{"name":"b1","remotePath":"orders/binlogs/site1/b1","size":1024,"firstEventTime":"2026-05-20T00:00:00Z","lastEventTime":"2026-05-20T03:59:59Z","archivedAt":"2026-05-20T04:00:01Z"}]}`),
	}}

	ensureEnvtestS3CredsSecret(t, ns, "envtest-s3-creds")
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
	if d.LastScanTime == nil {
		t.Error("lastScanTime is nil")
	}
	// latestDump and binlogArchive round-trip through the CRD schema.
	if dump := d.LatestDump; dump == nil || dump.Name != "orders-nightly-20260520" ||
		dump.Location != "orders/orders-nightly-20260520/" || dump.GtidExecuted != "abc:1-100" ||
		dump.SizeBytes == nil || *dump.SizeBytes != 512000 || dump.CompletionTime == nil {
		t.Errorf("discovered.latestDump: %+v", d.LatestDump)
	}
	a := d.BinlogArchive
	if a == nil {
		t.Fatal("discovered.binlogArchive is nil")
	}
	if a.Identity.ArchiveID != "ar1-aaaaaaaaaaaaaaaaaaaaaaaaaa" ||
		a.Identity.Source.UID != "orders-uid" || a.Identity.EncryptionMode != v1alpha1.EncryptionModeNone {
		t.Errorf("discovered.binlogArchive.identity: %+v", a.Identity)
	}
	if a.ManifestCount != 1 || a.FileCount != 1 || a.SizeBytes != 1024 ||
		a.OldestEventTime == nil || a.NewestEventTime == nil {
		t.Errorf("discovered.binlogArchive window: %+v", a)
	}
}

// envtestArchiveDescriptor is a plaintext archive.json whose ID the fixture
// manifests name.
func envtestArchiveDescriptor(t *testing.T) []byte {
	t.Helper()
	d := &pitrarchive.Descriptor{
		Kind: pitrarchive.DescriptorKind, Version: pitrarchive.DescriptorVersion,
		ArchiveID:  "ar1-aaaaaaaaaaaaaaaaaaaaaaaaaa",
		Source:     pitrarchive.Source{Namespace: "prod", Name: "orders", UID: "orders-uid"},
		Encryption: pitrarchive.DescriptorEncryption{Mode: pitrarchive.EncryptionNone},
		CreatedAt:  time.Date(2026, 5, 1, 0, 0, 0, 0, time.UTC), CreatedBy: "site1",
	}
	b, err := d.Canonical()
	if err != nil {
		t.Fatal(err)
	}
	return b
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

	ensureEnvtestS3CredsSecret(t, ns, "envtest-s3-creds")
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
		t.Fatalf("reconcile returned unexpected error: %v", err)
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

	ensureEnvtestS3CredsSecret(t, ns, "envtest-s3-creds")
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
		t.Fatalf("reconcile returned unexpected error: %v", err)
	}
	if res.RequeueAfter != customInterval {
		t.Errorf("requeue: want %v, got %v", customInterval, res.RequeueAfter)
	}
}

// TestMysqlStandbyCluster_EnvtestCreate_PVCSourceUnrepresentable verifies
// that the schema has no way to name a PVC source: a source without s3 is
// refused, and a leftover source.storage block is pruned rather than stored.
// Cross-cluster DR reads the source's object store; the operator pod cannot
// mount the source cluster's backup PVC.
func TestMysqlStandbyCluster_EnvtestCreate_PVCSourceUnrepresentable(t *testing.T) {
	ns := "default"
	ensureEnvtestS3CredsSecret(t, ns, "envtest-s3-creds")

	newUnstructured := func(name string, source map[string]any) *unstructured.Unstructured {
		u := &unstructured.Unstructured{Object: map[string]any{
			"apiVersion": v1alpha1.GroupVersion.String(),
			"kind":       "MysqlStandbyCluster",
			"metadata":   map[string]any{"name": name, "namespace": ns},
			"spec":       map[string]any{"source": source},
		}}
		return u
	}
	pvc := map[string]any{"type": "PVC", "pvc": map[string]any{"claimName": "source-backup-pvc"}}

	onlyPVC := newUnstructured("envtest-standby-pvc", map[string]any{"storage": pvc})
	err := k8sClient.Create(ctx, onlyPVC)
	if err == nil {
		_ = k8sClient.Delete(ctx, onlyPVC)
		t.Fatal("expected admission to reject a source without s3, but Create succeeded")
	}
	if !apierrors.IsInvalid(err) || !strings.Contains(err.Error(), "spec.source.s3") {
		t.Errorf("want a required spec.source.s3 refusal, got: %v", err)
	}

	withS3 := newUnstructured("envtest-standby-pvc-pruned", map[string]any{
		"storage": pvc,
		"s3":      map[string]any{"bucket": "my-bucket", "prefix": "orders", "credentialsSecretName": "envtest-s3-creds"},
	})
	if err := k8sClient.Create(ctx, withS3); err != nil {
		t.Fatalf("create with s3 and a leftover storage block: %v", err)
	}
	t.Cleanup(func() { _ = k8sClient.Delete(ctx, withS3) })
	got := &unstructured.Unstructured{}
	got.SetGroupVersionKind(v1alpha1.GroupVersion.WithKind("MysqlStandbyCluster"))
	if err := k8sClient.Get(ctx, types.NamespacedName{Namespace: ns, Name: withS3.GetName()}, got); err != nil {
		t.Fatal(err)
	}
	if _, found, _ := unstructured.NestedFieldNoCopy(got.Object, "spec", "source", "storage"); found {
		t.Error("spec.source.storage survived admission; it must be pruned")
	}
}

// TestMysqlStandbyCluster_EnvtestCreate_RejectsNetworkTransport verifies that
// the transport enum admits only ObjectStore: Network was never implemented.
func TestMysqlStandbyCluster_EnvtestCreate_RejectsNetworkTransport(t *testing.T) {
	ns := "default"
	ensureEnvtestS3CredsSecret(t, ns, "envtest-s3-creds")
	cr := minimalEnvtestStandby("envtest-standby-network", ns)
	cr.Spec.Transport = v1alpha1.StandbyTransport("Network")
	err := k8sClient.Create(ctx, cr)
	if err == nil {
		_ = k8sClient.Delete(ctx, cr)
		t.Fatal("expected admission to reject transport Network, but Create succeeded")
	}
	if !apierrors.IsInvalid(err) || !strings.Contains(err.Error(), "spec.transport") {
		t.Errorf("want a spec.transport enum refusal, got: %v", err)
	}

	defaulted := minimalEnvtestStandby("envtest-standby-transport-default", ns)
	defaulted.Spec.Transport = ""
	if err := k8sClient.Create(ctx, defaulted); err != nil {
		t.Fatalf("create without transport: %v", err)
	}
	t.Cleanup(func() { _ = k8sClient.Delete(ctx, defaulted) })
	if defaulted.Spec.Transport != v1alpha1.StandbyTransportObjectStore {
		t.Errorf("transport: want the ObjectStore default, got %q", defaulted.Spec.Transport)
	}
}

// TestMysqlStandbyCluster_EnvtestCreate_RejectsBucketRootPrefix verifies that
// source.s3.prefix must name at least one segment: an empty prefix, or one
// that normalizes to the bucket root, is refused at admission.
func TestMysqlStandbyCluster_EnvtestCreate_RejectsBucketRootPrefix(t *testing.T) {
	ns := "default"
	ensureEnvtestS3CredsSecret(t, ns, "envtest-s3-creds")

	for i, tc := range []struct {
		prefix string
		ok     bool
	}{
		{"", false},
		{"/", false},
		{"//", false},
		{"a//b", false},
		{"orders/..", false},
		{"orders", true},
		{"/orders/west/", true},
	} {
		cr := minimalEnvtestStandby(fmt.Sprintf("envtest-standby-prefix-%d", i), ns)
		cr.Spec.Source.S3.Prefix = tc.prefix
		err := k8sClient.Create(ctx, cr)
		if tc.ok {
			if err != nil {
				t.Errorf("prefix %q refused: %v", tc.prefix, err)
				continue
			}
			_ = k8sClient.Delete(ctx, cr)
			continue
		}
		if err == nil {
			_ = k8sClient.Delete(ctx, cr)
			t.Errorf("expected admission to reject prefix %q, but Create succeeded", tc.prefix)
			continue
		}
		if !apierrors.IsInvalid(err) || !strings.Contains(err.Error(), "spec.source.s3.prefix") {
			t.Errorf("prefix %q rejected for the wrong reason: %v", tc.prefix, err)
		}
	}
}

// TestMysqlStandbyCluster_EnvtestCreate_RejectsNonHTTPEndpoint verifies the
// endpointURL pattern: the operator pod dials this URL itself, so only
// absolute http(s) URLs are admitted.
func TestMysqlStandbyCluster_EnvtestCreate_RejectsNonHTTPEndpoint(t *testing.T) {
	ns := "default"
	ensureEnvtestS3CredsSecret(t, ns, "envtest-s3-creds")

	for i, bad := range []string{"minio:9000", "file:///etc/passwd", "http://host/?x=1", "https://user:pw@minio:9000"} {
		cr := minimalEnvtestStandby("envtest-standby-endpoint-"+string(rune('a'+i)), ns)
		cr.Spec.Source.S3.EndpointURL = bad
		err := k8sClient.Create(ctx, cr)
		if err == nil {
			_ = k8sClient.Delete(ctx, cr)
			t.Errorf("expected admission to reject endpointURL %q, but Create succeeded", bad)
			continue
		}
		if !strings.Contains(err.Error(), "endpointURL") {
			t.Errorf("endpointURL %q rejected for the wrong reason: %v", bad, err)
		}
	}
}
