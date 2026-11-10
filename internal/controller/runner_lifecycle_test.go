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

// lifecycleSecretData is the operator Secret of lifecycleRunner's group.
func lifecycleSecretData() map[string][]byte {
	return map[string][]byte{"username": []byte("op"), "password": []byte("pw")}
}

// lifecycleRunner returns a runner over a fake API server holding fg and its
// operator secret. The site Services do not resolve; the managers it starts
// poll MySQL in the background and fail to connect, which is all these tests
// need.
func lifecycleRunner(t *testing.T) (*TopologyManagerRunner, client.Client, *v1alpha1.MysqlFailoverGroup, *corev1.Secret) {
	t.Helper()
	fg := newTestFG()
	secret := newTestSecret()
	secret.Data = lifecycleSecretData()
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
	// stopped, and the replacement fails on the missing username key.
	original := secret.Data
	secret.Data = map[string][]byte{"password": []byte("pw")}
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
	live.Spec.Sites[1].LoadBalancerIP = "203.0.113.99"
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

// TestRegisterManager_HydratesUncachedBeforeRun: a manager started while a
// planned failover owns the sites (an operator restart mid-switchover)
// carries the run's claim before its first poll, hydrated from the
// uncached read, not from the (possibly stale) cached object the sync
// passed in. The RecoveryInProgress markers startManager restores would
// otherwise let that poll drain or recover the sites the switchover uses.
func TestRegisterManager_HydratesUncachedBeforeRun(t *testing.T) {
	r, c, fg, _ := lifecycleRunner(t)
	nn := FailoverGroupNamespacedName(fg)
	live := fg.DeepCopy()
	live.Status.PlannedFailover = &v1alpha1.PlannedFailoverStatus{
		RunID: "run-restart", Revision: 4, Phase: v1alpha1.PlannedFailoverPhasePromoting, TargetSite: "dc2", SourceSite: "dc1",
	}
	live.Status.Sites = []v1alpha1.SiteStatus{{Name: "dc2", RecoveryState: v1alpha1.RecoveryStateInProgress}}
	if err := c.Status().Update(context.Background(), live); err != nil {
		t.Fatalf("seed status: %v", err)
	}
	r.SetAPIReader(c)
	cached := fg.DeepCopy() // the sync's cached copy predates the run
	cached.Status.Sites = live.Status.Sites

	cfg := mustTopologyConfig(cached)
	if err := r.startManager(context.Background(), cached, cfg, nil); err != nil {
		t.Fatalf("startManager: %v", err)
	}
	mt := registeredManager(r, nn)
	if mt == nil {
		t.Fatal("no manager registered")
	}
	view, ok := r.Slot(nn).View()
	if !ok || view.RunID != "run-restart" || view.Mode != "Run" || !view.NeedsReprove {
		t.Fatalf("slot = %+v (held=%v), want run-restart hydrated in Run mode with needsReprove", view, ok)
	}
	if mt.tm.plannedClaimSlot() != r.Slot(nn) {
		t.Fatal("the manager is not wired to the group's slot")
	}
	if !mt.tm.followerMutationSuppressed() {
		t.Fatal("restored RecoveryInProgress sites are open to recovery during the planned failover")
	}
}

// An uncached read failure fails the start of a group's first manager
// (nothing could hydrate its claim), as initRestoreProtection's does.
func TestStartManager_PlannedViewReadErrorFailsFirstStart(t *testing.T) {
	r, _, fg, _ := lifecycleRunner(t)
	r.SetAPIReader(failingReader{})
	cfg := mustTopologyConfig(fg)
	if err := r.startManager(context.Background(), fg, cfg, nil); err == nil {
		t.Fatal("startManager succeeded without an uncached read of the planned-failover claim")
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
	cfg := mustTopologyConfig(fg)

	if ok, why := r.ReserveKeyringRotation(nn, "dc2"); ok || why != rotationRefusedNoManager {
		t.Fatalf("reservation with no manager = %v, %q; want refused until a manager runs", ok, why)
	}
	if err := r.startManager(ctx, fg, cfg, nil); err != nil {
		t.Fatalf("startManager: %v", err)
	}
	first := registeredManager(r, nn)
	settleAuthority(first.tm)
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
	cfg := mustTopologyConfig(fg)
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
// rotation of its target cannot both proceed. (Retired before its claim,
// the old worker now also never claims the sites.)
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
	settleAuthority(replacement)
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
	// It was still before its claim when retired, so it touched nothing.
	if !standbyReadOnly(standby) {
		t.Fatal("a retired manager's unclaimed update claimed the sites and handed off")
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

// A replaced manager's ordered update can outlive its Run. The manager that
// replaces it must not drain, recover or repoint the sites that update is
// still working on until it exits (issue #175 item 1 across a restart).
func TestReplacementManagerDefersToPredecessorUpdate(t *testing.T) {
	logger := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelError}))
	r := &TopologyManagerRunner{logger: logger, managers: make(map[types.NamespacedName]*managedTopology)}
	nn := types.NamespacedName{Namespace: "shared-lion", Name: "lion"}

	primary := &mockMySQL{readOnly: false}
	follower := &mockMySQL{readOnly: true, replicaStatusVal: &mysql.ReplicaStatus{IORunning: true, SQLRunning: true, SourceHost: "mysql-dc1"}}
	old, _, _ := newTestTopologyManagerWithBootstrap(primary, follower)
	old.updater = NewUpdateController(NewFailoverController(testLogger()), testLogger())
	pollN(old, 4)
	entered, unblock := make(chan struct{}), make(chan struct{})
	// The old update blocks rolling the follower, then gives up.
	old.ApplyUpdate = func(context.Context, string) error {
		close(entered)
		<-unblock
		return errors.New("rollout abandoned")
	}
	r.registerManager(nn, &managedTopology{tm: old})
	old.SetSpecDriftSites([]string{"dc2"})
	if !old.checkUpdate(context.Background()) {
		t.Fatal("setup: ordered update did not start")
	}
	<-entered

	r.mu.Lock()
	r.retireManagerLocked(nn, r.managers[nn])
	r.mu.Unlock()
	replacement, _, _ := newTestTopologyManagerWithBootstrap(primary, follower)
	replacement.SetRecoveryInProgress("dc2")
	r.registerManager(nn, &managedTopology{tm: replacement})

	pollN(replacement, 4)
	follower.mu.Lock()
	kills, fences, changes := follower.killConnectionCalls, follower.superReadOnlyCalls, follower.changeSourceCalls
	follower.mu.Unlock()
	if kills != 0 || fences != 0 || changes != 0 {
		t.Fatalf("replacement mutated a site the old update still owns: kill=%d fence=%d changeSource=%d", kills, fences, changes)
	}

	close(unblock)
	deadline := time.Now().Add(10 * time.Second)
	for {
		replacement.mu.RLock()
		waiting := replacement.predecessorUpdateInFlight
		replacement.mu.RUnlock()
		if !waiting {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("the replacement still defers after the old update exited")
		}
		time.Sleep(5 * time.Millisecond)
	}
	pollN(replacement, 2)
	follower.mu.Lock()
	kills = follower.killConnectionCalls
	follower.mu.Unlock()
	if kills == 0 {
		t.Fatal("recovery did not resume once the old update exited")
	}
}
