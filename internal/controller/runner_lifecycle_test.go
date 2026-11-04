package controller

import (
	"context"
	"errors"
	"log/slog"
	"os"
	"sync"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	v1alpha1 "github.com/shipstream/bloodraven/api/v1alpha1"
	"github.com/shipstream/bloodraven/internal/mysql"
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

// A manager started while a planned failover is in flight (an operator
// restart mid-switchover) must carry the planned-failover guard before its
// first poll: the RecoveryInProgress markers startManager restores would
// otherwise let that poll drain or recover the sites the switchover uses.
func TestStartManager_HydratesPlannedFailoverGuardBeforeRun(t *testing.T) {
	r, _, fg, _ := lifecycleRunner(t)
	nn := FailoverGroupNamespacedName(fg)
	fg.Status.PlannedFailover = &v1alpha1.PlannedFailoverStatus{Phase: v1alpha1.PlannedFailoverPhasePromoting, Target: "dc2"}
	fg.Status.Sites = []v1alpha1.SiteStatus{{Name: "dc2", RecoveryState: recoveryStateInProgress}}

	cfg := CRConfigToTopologyConfig(fg)
	if err := r.startManager(context.Background(), fg, cfg, nil); err != nil {
		t.Fatalf("startManager: %v", err)
	}
	mt := registeredManager(r, nn)
	if mt == nil {
		t.Fatal("no manager registered")
	}
	if !mt.tm.isPlannedFailoverActive() {
		t.Fatal("planned-failover guard not set when Run started")
	}
	if !mt.tm.followerMutationSuppressed() {
		t.Fatal("restored RecoveryInProgress sites are open to recovery during the planned failover")
	}
}

func managerRotationBlocked(mt *managedTopology, site string) bool {
	mt.tm.mu.RLock()
	defer mt.tm.mu.RUnlock()
	return mt.tm.keyringRotationBlockedLocked(site)
}

// No manager can be asked about an ordered update, so a rotation is refused
// (and retried) until one runs. A rotation admitted against a manager that
// is then replaced must block promotion on the replacement before its Run
// can claim an ordered update.
func TestRotationReservationSurvivesManagerReplacement(t *testing.T) {
	r, _, fg, _ := lifecycleRunner(t)
	nn := FailoverGroupNamespacedName(fg)
	ctx := context.Background()
	cfg := CRConfigToTopologyConfig(fg)

	if ok, why := r.ReserveKeyringRotation(nn, "dc2"); ok || why != rotationRefusedNoManager {
		t.Fatalf("reservation with no manager = %v, %q; want refused until a manager runs", ok, why)
	}
	if err := r.startManager(ctx, fg, cfg, nil); err != nil {
		t.Fatalf("startManager: %v", err)
	}
	first := registeredManager(r, nn)
	if ok, why := r.ReserveKeyringRotation(nn, "dc2"); !ok {
		t.Fatalf("reservation refused with a running manager: %s", why)
	}
	if !managerRotationBlocked(first, "dc2") {
		t.Fatal("the running manager does not block dc2")
	}

	// Replace the manager before the stamp is persisted.
	r.StopManager(nn)
	if err := r.startManager(ctx, fg, cfg, first); err != nil {
		t.Fatalf("restart: %v", err)
	}
	second := registeredManager(r, nn)
	if second == first || !managerRotationBlocked(second, "dc2") {
		t.Fatal("the replacement manager lost the rotation reservation")
	}

	// An authoritative view without the stamp ends the reservation for the
	// running manager and for any later one.
	r.PublishKeyringRotationBlocked(nn, nil)
	if managerRotationBlocked(second, "dc2") {
		t.Fatal("authoritative view did not end the reservation")
	}
	r.StopManager(nn)
	if err := r.startManager(ctx, fg, cfg, second); err != nil {
		t.Fatalf("restart: %v", err)
	}
	if managerRotationBlocked(registeredManager(r, nn), "dc2") {
		t.Fatal("an ended reservation was carried onto a new manager")
	}
}

// A reservation the running manager refuses is not recorded, so no later
// manager inherits it.
func TestRefusedRotationReservationIsNotCarried(t *testing.T) {
	r, _, fg, _ := lifecycleRunner(t)
	nn := FailoverGroupNamespacedName(fg)
	cfg := CRConfigToTopologyConfig(fg)
	if err := r.startManager(context.Background(), fg, cfg, nil); err != nil {
		t.Fatalf("startManager: %v", err)
	}
	mt := registeredManager(r, nn)
	mt.tm.mu.Lock()
	mt.tm.deploymentUpdatePending = true
	mt.tm.mu.Unlock()
	if ok, _ := r.ReserveKeyringRotation(nn, "dc2"); ok {
		t.Fatal("reservation admitted while an ordered update is pending")
	}
	r.reservationMu.Lock()
	pending := r.pendingKeyringRotationReservations(nn)
	r.reservationMu.Unlock()
	if len(pending) != 0 {
		t.Fatalf("refused reservation recorded: %v", pending)
	}
}

// The interleaving the retiring list exists for: a manager is replaced
// while its ordered-update goroutine is still alive and about to hand the
// active site off. Until that goroutine exits, a rotation must not be
// admitted, whatever replaced the manager; the old worker's handoff and a
// rotation of its target cannot both proceed.
func TestRotationRefusedWhileRetiringManagerUpdateInFlight(t *testing.T) {
	logger := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelError}))
	r := &TopologyManagerRunner{logger: logger, managers: make(map[types.NamespacedName]*managedTopology)}
	nn := types.NamespacedName{Namespace: "shared-lion", Name: "lion"}

	primary := &mockMySQL{readOnly: false}
	standby := &mockMySQL{readOnly: true, replicaStatusVal: &mysql.ReplicaStatus{IORunning: true, SQLRunning: true, SourceHost: "mysql-dc1"}}
	old, _, _ := newTestTopologyManagerWithBootstrap(primary, standby)
	old.updater = NewUpdateController(NewFailoverController(testLogger()), testLogger())
	pollN(old, 4)

	entered, unblock := make(chan struct{}), make(chan struct{})
	var once sync.Once
	old.PodReady = func(context.Context, string) (PodReadiness, error) {
		once.Do(func() {
			close(entered)
			<-unblock
		})
		return PodReadiness{Ready: true}, nil
	}
	// The old active's own update is where this test stops the worker.
	old.ApplyUpdate = func(context.Context, string) error { return errors.New("stop after the handoff") }
	r.SetManagerForTest(nn, old)
	old.SetSpecDriftSites([]string{"dc1"})
	if !old.checkUpdate(context.Background()) {
		t.Fatal("setup: ordered update did not start")
	}
	<-entered

	// sync replaces the manager: retire and unregister, as the runner does.
	r.mu.Lock()
	r.retireManagerLocked(nn, r.managers[nn])
	r.mu.Unlock()
	if ok, why := r.ReserveKeyringRotation(nn, "dc2"); ok || why != rotationRefusedManagerRestarting {
		t.Fatalf("rotation with the old update in flight and no replacement = %v, %q; want refused", ok, why)
	}
	replacement, _, _ := newTestTopologyManagerWithBootstrap(&mockMySQL{readOnly: false}, &mockMySQL{readOnly: true})
	r.SetManagerForTest(nn, replacement)
	if ok, why := r.ReserveKeyringRotation(nn, "dc2"); ok || why != rotationRefusedManagerRestarting {
		t.Fatalf("rotation with the old update in flight = %v, %q; want refused until it exits", ok, why)
	}
	if old.checkUpdate(context.Background()) {
		t.Fatal("a retired manager started another ordered update")
	}

	close(unblock)
	deadline := time.Now().Add(10 * time.Second)
	for {
		r.mu.RLock()
		retiring := len(r.retiring[nn])
		r.mu.RUnlock()
		if retiring == 0 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("the retired manager was never released after its update exited")
		}
		time.Sleep(5 * time.Millisecond)
	}
	if standbyReadOnly(standby) {
		t.Fatal("setup: the old worker did not hand off to dc2")
	}
	if ok, why := r.ReserveKeyringRotation(nn, "dc2"); !ok {
		t.Fatalf("rotation still refused after the old update exited: %s", why)
	}
}

func standbyReadOnly(m *mockMySQL) bool {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.readOnly
}
