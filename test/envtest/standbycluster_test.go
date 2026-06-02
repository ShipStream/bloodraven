//go:build envtest

package envtest

import (
	"context"
	"errors"
	"io"
	"strings"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
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
//
// The embedded template.spec MUST be a fully-valid MysqlFailoverGroupSpec
// because the CRD's deferred-validation embeds the parent spec — admission
// validates the template at standby-cluster create time, not only when the
// MFG is materialized at Phase 3 activation. The fixture below mirrors
// examples/minimal-failovergroup.yaml plus a credentialsSecret for the
// archive S3 backend (required by MinLength=1 on S3Storage).
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
						// CredentialsSecret is required by the CRD schema (MinLength=1
						// in BackupStorage's S3 stanza). The reconciler's S3-cred
						// resolution path is bypassed in envtest because the
						// SetNewStoreFunc injection returns the fake store before
						// resolveS3CredsToDir is reached, but admission still
						// requires the field to be set.
						CredentialsSecret: "envtest-s3-creds",
					},
				},
			},
			Template: v1alpha1.StandbyFailoverGroupTemplate{
				Name: name + "-dr",
				Spec: v1alpha1.MysqlFailoverGroupSpec{
					Image:      "mysql:9.6",
					SecretName: "mysql-creds",
					DNS: v1alpha1.DNSSpec{
						Hostname: name + "-dr.az.example.com",
						TTL:      60,
					},
					Sites: []v1alpha1.SiteSpec{
						{
							Name: "iad",
							Zone: "us-east-1a",
							TaintNodeSelector: map[string]string{
								"shipstream.io/failover-group.orders": "true",
								"shipstream.io/site.orders":           "iad",
							},
							LBIP: "10.0.1.1",
							Storage: v1alpha1.StorageSpec{
								StorageClassName: "fast-ssd",
								Size:             resource.MustParse("100Gi"),
							},
						},
						{
							Name: "pdx",
							Zone: "us-west-2a",
							TaintNodeSelector: map[string]string{
								"shipstream.io/failover-group.orders": "true",
								"shipstream.io/site.orders":           "pdx",
							},
							LBIP: "10.0.2.1",
							Storage: v1alpha1.StorageSpec{
								StorageClassName: "fast-ssd",
								Size:             resource.MustParse("100Gi"),
							},
						},
					},
				},
			},
		},
	}
}

// ensureEnvtestS3CredsSecret creates the dummy AWS-credentials Secret that
// the standby fixture references in spec.source.storage.s3.credentialsSecret.
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
		"orders/binlogs/manifest-site1.json": []byte(
			`{"version":1,"site":"site1","files":[{"name":"b1","remotePath":"orders/binlogs/site1/b1","size":1024,"firstEventTime":"2026-05-20T00:00:00Z","lastEventTime":"2026-05-20T03:59:59Z","archivedAt":"2026-05-20T04:00:01Z"}]}`),
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

// TestMysqlStandbyCluster_EnvtestCreate_RejectsPVCSource verifies that the
// spec-level XValidation rule (D1) rejects a PVC-backed source at admission.
// Cross-cluster DR requires S3-compatible storage because the operator pod
// does not mount the source cluster's backup PVC; the CEL rule fails fast at
// create time instead of letting a doomed CR error only at reconcile time
// (the runtime ConfigError in buildStoreCfg remains as defense-in-depth).
func TestMysqlStandbyCluster_EnvtestCreate_RejectsPVCSource(t *testing.T) {
	ns := "default"
	scName := "envtest-standby-pvc"

	ensureEnvtestS3CredsSecret(t, ns, "envtest-s3-creds")
	cr := minimalEnvtestStandby(scName, ns)
	// Swap the S3 source for a PVC source. The BackupStorage union rule
	// requires exactly one of s3/pvc matching the type discriminator, so we
	// must clear S3 and set PVC.
	cr.Spec.Source.Storage = v1alpha1.BackupStorage{
		Type: v1alpha1.BackupStoragePVC,
		PVC: &v1alpha1.PVCStorage{
			ClaimName: "source-backup-pvc",
		},
	}

	err := k8sClient.Create(ctx, cr)
	if err == nil {
		_ = k8sClient.Delete(ctx, cr) // clean up if it unexpectedly admitted
		t.Fatal("expected admission to reject PVC-backed source, but Create succeeded")
	}
	if !strings.Contains(err.Error(), "source.storage.type must be S3") {
		t.Errorf("want S3-only CEL rule violation, got: %v", err)
	}
}

// TestMysqlStandbyCluster_EnvtestCreate_RejectsEmptyPrefix verifies that the
// spec-level XValidation rule rejects an ObjectStore+S3 source with an empty
// prefix at admission. S3Storage.Prefix has no MinLength of its own, so this
// CEL rule is the sole guard. The rule uses size(prefix) > 0 rather than a
// CEL empty-string-literal comparison, because gofmt rewrites that literal
// into a curly quote (U+201D) that silently corrupts admission; this test
// locks the rejection behavior in regardless of how the marker is formatted.
func TestMysqlStandbyCluster_EnvtestCreate_RejectsEmptyPrefix(t *testing.T) {
	ns := "default"
	scName := "envtest-standby-emptyprefix"

	ensureEnvtestS3CredsSecret(t, ns, "envtest-s3-creds")
	cr := minimalEnvtestStandby(scName, ns)
	cr.Spec.Source.Storage.S3.Prefix = "" // violates the non-empty-prefix rule

	err := k8sClient.Create(ctx, cr)
	if err == nil {
		_ = k8sClient.Delete(ctx, cr) // clean up if it unexpectedly admitted
		t.Fatal("expected admission to reject empty source.storage.s3.prefix, but Create succeeded")
	}
	if !strings.Contains(err.Error(), "prefix must be non-empty") {
		t.Errorf("want non-empty-prefix CEL rule violation, got: %v", err)
	}
}
