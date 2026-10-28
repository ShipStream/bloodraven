package controller

import (
	"context"
	"log/slog"
	"os"
	"testing"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	v1alpha1 "github.com/shipstream/bloodraven/api/v1alpha1"
	"github.com/shipstream/bloodraven/internal/platform"
)

// lifecycleRunner returns a runner over a fake API server holding fg and its
// DSN-mode operator secret. The DSN points at a closed local port; the
// managers it starts poll MySQL in the background and fail to connect, which
// is all these tests need.
func lifecycleRunner(t *testing.T) (*TopologyManagerRunner, client.Client, *v1alpha1.MysqlFailoverGroup, *corev1.Secret) {
	t.Helper()
	fg := newTestFG()
	secret := newTestSecret()
	secret.Data = map[string][]byte{"dsn": []byte("op:pw@tcp(127.0.0.1:1)/mysql")}
	c := fake.NewClientBuilder().WithScheme(testScheme()).
		WithStatusSubresource(&v1alpha1.MysqlFailoverGroup{}).
		WithObjects(fg, secret).Build()
	logger := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelError}))
	runner := &TopologyManagerRunner{
		client:   c,
		logger:   logger,
		hub:      platform.NewHub(logger),
		managers: make(map[types.NamespacedName]*managedTopology),
	}
	t.Cleanup(runner.stopAll)
	return runner, c, fg, secret
}

func registeredManager(r *TopologyManagerRunner, nn types.NamespacedName) *managedTopology {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.managers[nn]
}

func managerStopped(mt *managedTopology) bool {
	if mt == nil || mt.done == nil {
		return false
	}
	select {
	case <-mt.done:
		return true
	default:
		return false
	}
}

func mustSync(t *testing.T, r *TopologyManagerRunner) {
	t.Helper()
	if err := r.sync(context.Background()); err != nil {
		t.Fatalf("sync: %v", err)
	}
}

// A restart whose replacement fails to start must not leave the stopped
// manager registered. Otherwise reverting the config hits the equal-config
// fast path, which updates the dead manager forever: no polling, status or
// failover until the operator process restarts.
func TestSync_FailedRestartThenRevertStartsLiveManager(t *testing.T) {
	r, c, fg, secret := lifecycleRunner(t)
	nn := FailoverGroupNamespacedName(fg)
	ctx := context.Background()

	mustSync(t, r)
	mt1 := registeredManager(r, nn)
	if mt1 == nil || managerStopped(mt1) {
		t.Fatalf("setup: want a running manager, got %+v", mt1)
	}

	// Readable but unusable material: the hash changes, the old manager is
	// stopped, and the replacement fails on the missing dsn key.
	original := secret.Data
	secret.Data = map[string][]byte{"not-dsn": []byte("x")}
	if err := c.Update(ctx, secret); err != nil {
		t.Fatalf("break secret: %v", err)
	}
	mustSync(t, r)
	if !managerStopped(mt1) {
		t.Fatal("old manager should have been stopped for the changed credentials")
	}
	if got := registeredManager(r, nn); got != nil {
		t.Fatalf("failed restart left a manager registered (stopped=%v)", managerStopped(got))
	}

	// Restore identical bytes: the config hash returns to mt1's.
	secret.Data = original
	if err := c.Update(ctx, secret); err != nil {
		t.Fatalf("restore secret: %v", err)
	}
	mustSync(t, r)
	mt2 := registeredManager(r, nn)
	if mt2 == nil {
		t.Fatal("no manager registered after the secret was restored")
	}
	if mt2 == mt1 || managerStopped(mt2) {
		t.Fatalf("registered manager is the stopped original (same=%v stopped=%v); want a fresh live one", mt2 == mt1, managerStopped(mt2))
	}
}

// A registered manager whose Run already returned is not a live manager,
// even when its config matches; sync must replace it.
func TestSync_ReplacesExitedManagerWithMatchingConfig(t *testing.T) {
	r, _, fg, _ := lifecycleRunner(t)
	nn := FailoverGroupNamespacedName(fg)

	mustSync(t, r)
	mt1 := registeredManager(r, nn)
	if mt1 == nil {
		t.Fatal("setup: no manager")
	}
	// Run exits on its own (context cancelled from elsewhere); the entry
	// keeps its config.
	mt1.cancel()
	<-mt1.done

	mustSync(t, r)
	mt2 := registeredManager(r, nn)
	if mt2 == nil || mt2 == mt1 || managerStopped(mt2) {
		t.Fatalf("exited manager was kept: same=%v stopped=%v", mt2 == mt1, managerStopped(mt2))
	}
}

// Briefly deleting the operator secret (a GitOps replace, an
// external-secrets re-sync) makes the connection material unreadable, not
// changed. The running manager's pools still hold working credentials, so
// it must keep running rather than be stopped for a restart that cannot
// succeed.
func TestSync_UnreadableSecretKeepsRunningManager(t *testing.T) {
	r, c, fg, secret := lifecycleRunner(t)
	nn := FailoverGroupNamespacedName(fg)
	ctx := context.Background()

	mustSync(t, r)
	mt1 := registeredManager(r, nn)
	if mt1 == nil {
		t.Fatal("setup: no manager")
	}

	if err := c.Delete(ctx, secret); err != nil {
		t.Fatalf("delete secret: %v", err)
	}
	mustSync(t, r)
	if got := registeredManager(r, nn); got != mt1 || managerStopped(mt1) {
		t.Fatalf("manager replaced or stopped while the secret was missing: same=%v stopped=%v", got == mt1, managerStopped(mt1))
	}

	recreated := secret.DeepCopy()
	recreated.ResourceVersion = ""
	if err := c.Create(ctx, recreated); err != nil {
		t.Fatalf("recreate secret: %v", err)
	}
	mustSync(t, r)
	if got := registeredManager(r, nn); got != mt1 || managerStopped(mt1) {
		t.Fatalf("manager restarted after identical secret was recreated: same=%v stopped=%v", got == mt1, managerStopped(mt1))
	}
}

// An unreadable secret is not a reason to keep a manager whose topology
// config changed for other reasons.
func TestSync_UnreadableSecretDoesNotPinChangedTopology(t *testing.T) {
	r, c, fg, secret := lifecycleRunner(t)
	nn := FailoverGroupNamespacedName(fg)
	ctx := context.Background()

	mustSync(t, r)
	mt1 := registeredManager(r, nn)
	if mt1 == nil {
		t.Fatal("setup: no manager")
	}

	if err := c.Delete(ctx, secret); err != nil {
		t.Fatalf("delete secret: %v", err)
	}
	var live v1alpha1.MysqlFailoverGroup
	if err := c.Get(ctx, nn, &live); err != nil {
		t.Fatalf("get fg: %v", err)
	}
	live.Spec.Sites[1].LBIP = "203.0.113.99"
	if err := c.Update(ctx, &live); err != nil {
		t.Fatalf("update fg: %v", err)
	}
	mustSync(t, r)
	if !managerStopped(mt1) {
		t.Fatal("manager with a stale site topology kept running")
	}
	if got := registeredManager(r, nn); got != nil {
		t.Fatalf("stopped manager still registered (same=%v)", got == mt1)
	}
}

func mustConnectionMaterialHash(t *testing.T, ctx context.Context, c client.Client, fg *v1alpha1.MysqlFailoverGroup) string {
	t.Helper()
	h, err := connectionMaterialHash(ctx, c, fg)
	if err != nil {
		t.Fatalf("connectionMaterialHash: %v", err)
	}
	return h
}

// A missing secret is reported as unreadable, never folded into a hash that
// a later identical secret would change back from.
func TestConnectionMaterialHash_UnreadableSecretIsAnError(t *testing.T) {
	ctx := context.Background()
	fg := newTestFG()
	c := fake.NewClientBuilder().WithScheme(testScheme()).Build()
	if h, err := connectionMaterialHash(ctx, c, fg); err == nil {
		t.Fatalf("missing operator secret: hash %q, want an error", h)
	}

	c = fake.NewClientBuilder().WithScheme(testScheme()).WithObjects(newTestSecret()).Build()
	fg.Spec.TLS = &v1alpha1.TLSSpec{SecretName: "mysql-tls"}
	if h, err := connectionMaterialHash(ctx, c, fg); err == nil {
		t.Fatalf("missing TLS secret: hash %q, want an error", h)
	}
}
