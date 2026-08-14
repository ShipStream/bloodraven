package controller

import (
	"context"
	"log/slog"
	"os"
	"testing"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"

	v1alpha1 "github.com/shipstream/bloodraven/api/v1alpha1"
	"github.com/shipstream/bloodraven/internal/state"
)

// EXP-07 from the encryption chaos plan: the topology poller and the
// encryption state machine are independent writers of .status on the same
// MysqlFailoverGroup. The poller snapshots the whole status, mutates its
// own fields, and writes it back; the encryption reconciler merge-patches
// only .status.encryptionAtRest. If a keyring transition lands between
// the poller's read and its write, the poller's stale snapshot must not
// carry the pre-transition encryption status back over the top of it.
//
// The consequence of getting this wrong is not cosmetic. Losing a
// Sealed/Escrowed advance re-renders the site unsealed and rolls the pod;
// losing a Failed hides a site whose keyring custody is gone.

// concurrentEncryptionWriter returns interceptor funcs that simulate the
// encryption reconciler patching .status.encryptionAtRest exactly once,
// in the window between the topology poller's read and its write. The
// fake client rejects the poller's now-stale write with a conflict, which
// is what drives it through RetryOnConflict and re-reads the object — the
// same sequence a real API server produces.
func concurrentEncryptionWriter(nn types.NamespacedName, desired *v1alpha1.EncryptionAtRestStatus) interceptor.Funcs {
	fired := false
	return interceptor.Funcs{
		SubResourceUpdate: func(
			ctx context.Context, c client.Client, subResourceName string,
			obj client.Object, opts ...client.SubResourceUpdateOption,
		) error {
			if !fired {
				fired = true
				var latest v1alpha1.MysqlFailoverGroup
				if err := c.Get(ctx, nn, &latest); err != nil {
					return err
				}
				latest.Status.EncryptionAtRest = desired.DeepCopy()
				setCondition(&latest.Status.Conditions, metav1.Condition{
					Type:               conditionEncryptionReady,
					Status:             metav1.ConditionTrue,
					Reason:             "AllSitesSealed",
					LastTransitionTime: metav1.Now(),
					Message:            "every site is sealed against an escrowed keyring",
				})
				if err := c.Status().Update(ctx, &latest); err != nil {
					return err
				}
			}
			return c.Status().Update(ctx, obj, opts...)
		},
	}
}

func TestUpdateCRStatus_DoesNotClobberConcurrentEncryptionStatus(t *testing.T) {
	fg := encTestFG()
	nn := types.NamespacedName{Name: fg.Name, Namespace: fg.Namespace}
	digest := keyringDigest([]byte("keyring"))
	sealed := &v1alpha1.EncryptionAtRestStatus{
		Sealed: true,
		Sites: []v1alpha1.SiteEncryptionStatus{
			{Name: "dc1", Phase: v1alpha1.KeyringPhaseSealed,
				KeyringSecret: "mysql-lion-dc1-keyring-v1", KeyringVersion: 1, KeyringDigest: digest},
			{Name: "dc2", Phase: v1alpha1.KeyringPhaseSealed,
				KeyringSecret: "mysql-lion-dc2-keyring-v1", KeyringVersion: 1, KeyringDigest: digest},
		},
	}

	scheme := testScheme()
	c := fake.NewClientBuilder().WithScheme(scheme).
		WithStatusSubresource(&v1alpha1.MysqlFailoverGroup{}).
		WithObjects(fg).
		WithInterceptorFuncs(concurrentEncryptionWriter(nn, sealed)).
		Build()

	runner := &TopologyManagerRunner{
		client:   c,
		logger:   slog.New(slog.NewJSONHandler(os.Stderr, nil)),
		managers: make(map[types.NamespacedName]*managedTopology),
	}

	if err := runner.updateCRStatus(context.Background(), nn, TopologySnapshot{
		Sites: []SiteSnapshot{
			{Name: "dc1", State: state.StateWritable},
			{Name: "dc2", State: state.StateReadOnly},
		},
		ActiveSite: "dc1",
	}); err != nil {
		t.Fatalf("updateCRStatus: %v", err)
	}

	var after v1alpha1.MysqlFailoverGroup
	if err := c.Get(context.Background(), nn, &after); err != nil {
		t.Fatalf("get fg: %v", err)
	}

	// The poller's own fields must have landed.
	if after.Status.ActiveSite != "dc1" {
		t.Errorf("activeSite = %q, want dc1", after.Status.ActiveSite)
	}

	// And it must not have taken the encryption subsystem with it.
	if after.Status.EncryptionAtRest == nil {
		t.Fatal("the topology status write erased .status.encryptionAtRest; " +
			"a site that just sealed would be re-rendered unsealed and its pod rolled")
	}
	for _, name := range []string{"dc1", "dc2"} {
		s := after.Status.EncryptionAtRest.SiteEncryptionStatusByName(name)
		if s == nil || s.Phase != v1alpha1.KeyringPhaseSealed {
			t.Errorf("%s = %+v, want the concurrently-written Sealed phase", name, s)
		}
		if s != nil && s.KeyringSecret == "" {
			t.Errorf("%s lost its escrow reference: %+v", name, s)
		}
	}
	if !after.SiteKeyringSealed("dc1") {
		t.Error("dc1 would be re-rendered unsealed after a topology poll")
	}
	cond := findCondition(after.Status.Conditions, conditionEncryptionReady)
	if cond == nil || cond.Status != metav1.ConditionTrue {
		t.Errorf("%s = %+v, want the concurrently-written True", conditionEncryptionReady, cond)
	}
}

// TestUpdateCRStatus_DoesNotResurrectDeletedEncryptionStatus is the
// mirror case: encryption was turned off (the reconciler cleared
// .status.encryptionAtRest) while a topology poll was in flight holding a
// snapshot that still had it. The poller must not write it back.
func TestUpdateCRStatus_DoesNotResurrectDeletedEncryptionStatus(t *testing.T) {
	fg := encTestFG()
	nn := types.NamespacedName{Name: fg.Name, Namespace: fg.Namespace}
	fg.Status.EncryptionAtRest = &v1alpha1.EncryptionAtRestStatus{
		Sites: []v1alpha1.SiteEncryptionStatus{
			{Name: "dc1", Phase: v1alpha1.KeyringPhaseSealed, KeyringSecret: "mysql-lion-dc1-keyring-v1"},
		},
	}

	scheme := testScheme()
	c := fake.NewClientBuilder().WithScheme(scheme).
		WithStatusSubresource(&v1alpha1.MysqlFailoverGroup{}).
		WithObjects(fg).
		WithInterceptorFuncs(concurrentEncryptionWriter(nn, nil)).
		Build()

	runner := &TopologyManagerRunner{
		client:   c,
		logger:   slog.New(slog.NewJSONHandler(os.Stderr, nil)),
		managers: make(map[types.NamespacedName]*managedTopology),
	}

	if err := runner.updateCRStatus(context.Background(), nn, TopologySnapshot{
		Sites: []SiteSnapshot{
			{Name: "dc1", State: state.StateWritable},
			{Name: "dc2", State: state.StateReadOnly},
		},
		ActiveSite: "dc1",
	}); err != nil {
		t.Fatalf("updateCRStatus: %v", err)
	}

	var after v1alpha1.MysqlFailoverGroup
	if err := c.Get(context.Background(), nn, &after); err != nil {
		t.Fatalf("get fg: %v", err)
	}
	if after.Status.EncryptionAtRest != nil {
		t.Fatalf("topology poll resurrected a cleared encryption status: %+v", after.Status.EncryptionAtRest)
	}
}

// TestEncryptionToggleDoesNotRestartTheTopologyManager pins that
// TopologyConfig deliberately ignores encryption. Turning encryption on
// must not tear down and rebuild the poller under a live cluster, so
// startManager wires the clone gate unconditionally: RequestKeyringUnseal
// is a no-op while encryption is off, and a later adoption is honoured
// without a manager rebuild. UnsealReason=Clone is sticky until
// NotifyCloneComplete, so that wiring cannot livelock a reclone.
//
// If this test ever fails because encryption became part of
// TopologyConfig, the adoption path would rebuild the manager on its
// own. The gate can stay always-on either way.
func TestEncryptionToggleDoesNotRestartTheTopologyManager(t *testing.T) {
	plain := CRConfigToTopologyConfig(newTestFG())
	encrypted := CRConfigToTopologyConfig(encTestFG())

	if !plain.Equal(encrypted) {
		t.Fatal("TopologyConfig now distinguishes encrypted groups; the clone-gate wiring " +
			"comment in startManager assumes it does not")
	}
}
