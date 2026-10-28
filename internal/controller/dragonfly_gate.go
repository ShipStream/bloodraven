package controller

import (
	"context"
	"sync"
	"sync/atomic"
	"time"

	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/log"

	v1alpha1 "github.com/shipstream/bloodraven/api/v1alpha1"
)

// dragonflyGate is the single ownership gate for one group's Dragonfly
// routing state: role changes (REPLTAKEOVER, REPLICAOF), pod role and
// traffic labels, Dragonfly pod deletion and restarts, and
// status.dragonfly. Every writer holds it for the whole of one unit of work
// and reads the authority it acts on (the group's status, live Dragonfly
// roles) after acquiring it, through an uncached reader: the informer
// cache can lag the previous holder's writes.
//
//   - the Dragonfly manager's Run goroutine, for each Tick (observation,
//     status publication, replica repair) and each promotion it executes;
//   - the planned-failover Dragonfly phase;
//   - the Dragonfly rollout promotion and the replica StatefulSet rollout;
//   - each Dragonfly snapshot-upgrade step;
//   - the reconciler's Dragonfly label sweep.
//
// Not covered: applying the active site's own StatefulSet template change
// when no replica can be promoted first (a deliberate restart of the
// master, decided from the reconcile's read).
//
// The runner owns one gate per group and keeps it across manager
// replacement, so an old manager's last unit of work and its replacement,
// or a planned failover running while no manager is registered, can never
// mutate concurrently.
type dragonflyGate struct {
	sem chan struct{}
	// waiters counts blocking acquirers. The manager checks it at safe
	// points so a planned failover or rollout is not held behind a long
	// Tick or promotion that has not yet sent a role-changing command.
	waiters atomic.Int32
}

func newDragonflyGate() *dragonflyGate {
	return &dragonflyGate{sem: make(chan struct{}, 1)}
}

// acquire blocks until the gate is free or ctx ends.
func (g *dragonflyGate) acquire(ctx context.Context) (release func(), err error) {
	g.waiters.Add(1)
	defer g.waiters.Add(-1)
	select {
	case g.sem <- struct{}{}:
		return g.releaser(), nil
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

// tryAcquire takes the gate only if it is free. It does not count as a
// waiter, so it never makes the current holder yield.
func (g *dragonflyGate) tryAcquire() (release func(), ok bool) {
	select {
	case g.sem <- struct{}{}:
		return g.releaser(), true
	default:
		return nil, false
	}
}

// contended reports whether a blocking acquirer is waiting.
func (g *dragonflyGate) contended() bool {
	return g.waiters.Load() > 0
}

func (g *dragonflyGate) releaser() func() {
	var once sync.Once
	return func() { once.Do(func() { <-g.sem }) }
}

const (
	// defaultDragonflyGateWait bounds how long a reconcile waits for the
	// group's Dragonfly gate before requeueing.
	defaultDragonflyGateWait = 5 * time.Second
	// dragonflyGateRetry is the requeue delay after the gate was busy.
	dragonflyGateRetry = time.Second
)

// acquireDragonflyGate waits up to defaultDragonflyGateWait for the group's
// Dragonfly gate. ok is false if the gate stayed busy (or ctx ended); the
// caller requeues. A reconciler without a runner (unit tests) has no
// manager to coordinate with and gets a no-op release.
func (r *MysqlFailoverGroupReconciler) acquireDragonflyGate(ctx context.Context, nn types.NamespacedName) (release func(), ok bool) {
	if r.Runner == nil {
		return func() {}, true
	}
	wait := r.dragonflyGateWait
	if wait <= 0 {
		wait = defaultDragonflyGateWait
	}
	waitCtx, cancel := context.WithTimeout(ctx, wait)
	defer cancel()
	release, err := r.Runner.DragonflyGate(nn).acquire(waitCtx)
	if err != nil {
		log.FromContext(ctx).Info("dragonfly gate busy; retrying", "fg", nn)
		return nil, false
	}
	return release, true
}

// tryDragonflyGate takes the group's Dragonfly gate only if it is free,
// without making its holder yield. Used by the label sweep, which runs on
// every reconcile and can simply skip a pass.
func (r *MysqlFailoverGroupReconciler) tryDragonflyGate(nn types.NamespacedName) (release func(), ok bool) {
	if r.Runner == nil {
		return func() {}, true
	}
	return r.Runner.DragonflyGate(nn).tryAcquire()
}

// freshFailoverGroup reads the group bypassing the informer cache when an
// uncached reader is configured, so a writer that has just taken the gate
// acts on the latest status (including what the previous holder wrote).
func (r *MysqlFailoverGroupReconciler) freshFailoverGroup(ctx context.Context, nn types.NamespacedName) (*v1alpha1.MysqlFailoverGroup, error) {
	var fg v1alpha1.MysqlFailoverGroup
	if err := r.authorityReader().Get(ctx, nn, &fg); err != nil {
		return nil, err
	}
	return &fg, nil
}

// authorityReader is the uncached reader when one is configured, else the
// reconciler's client.
func (r *MysqlFailoverGroupReconciler) authorityReader() client.Reader {
	if r.APIReader != nil {
		return r.APIReader
	}
	return r.Client
}
