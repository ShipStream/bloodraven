package dst

import (
	"context"
	"sync"

	"k8s.io/apimachinery/pkg/types"

	"github.com/shipstream/bloodraven/internal/controller"
)

// simRegistry is the DST's controller.ManagerRegistry: the one group's
// current topology manager, its retiring predecessors, and the process's
// PlannedClaimSlot (§15.1 "The registry").
//
// One registry is one operator process. OpRestartOperator builds a new
// registry, which drops the slot exactly as a process restart drops the
// runner's memory; OpReplaceManager stays in the process, so the slot is
// shared by every manager generation, as the runner shares it.
//
// Managers are wired with controller.WireRegisteredManager, exactly as the
// runner wires them before Run.
type simRegistry struct {
	mu sync.RWMutex
	nn types.NamespacedName

	current  *controller.TopologyManager
	retiring []*retiringManager
	slot     *controller.PlannedClaimSlot

	// absentPolls > 0 makes Snapshot report no current manager (the
	// replacement failed to start); the harness does not poll it either.
	absentPolls int

	leaderCtx    context.Context
	leaderCancel context.CancelFunc

	// retire is how a replaced manager is told it is retired, and
	// changingSites reports whether a retired manager is still changing
	// the sites (the runner's retiringChangingSites). Both are unexported in
	// the controller and reach the DST through the …ForSim hooks; while
	// either is nil a replaced manager stops polling at once (see replace).
	retire        func(*controller.TopologyManager)
	changingSites func(*controller.TopologyManager) bool
}

// retiringManager is a replaced manager that still polls for a while,
// modeling a Poll already in flight when its ctx was cancelled.
type retiringManager struct {
	tm *controller.TopologyManager
	// pollsLeft is how many more polls it runs before it observes its
	// cancelled ctx.
	pollsLeft int
	// updateInFlight reports whether its update actor is still stepping
	// (nil: none). When it ends, the registry clears the current manager's
	// predecessorUpdateInFlight, as retireManagerLocked's goroutine does.
	updateInFlight func() bool
}

var _ controller.ManagerRegistry = (*simRegistry)(nil)

func newSimRegistry(nn types.NamespacedName) *simRegistry {
	ctx, cancel := context.WithCancel(context.Background())
	return &simRegistry{
		nn:           nn,
		slot:         &controller.PlannedClaimSlot{},
		leaderCtx:    ctx,
		leaderCancel: cancel,
	}
}

// Snapshot is the registry state under one read lock; the caller inspects
// the managers with no registry lock held.
func (g *simRegistry) Snapshot(nn types.NamespacedName) controller.RegistrySnapshot {
	g.mu.RLock()
	defer g.mu.RUnlock()
	snap := controller.RegistrySnapshot{Slot: g.slotLocked(nn)}
	if nn != g.nn {
		return snap
	}
	if g.absentPolls == 0 {
		snap.Current = g.current
	}
	for _, rm := range g.retiring {
		snap.Retiring = append(snap.Retiring, rm.tm)
	}
	return snap
}

// Slot returns the group's slot; it is created with the registry and is
// never nil.
func (g *simRegistry) Slot(nn types.NamespacedName) *controller.PlannedClaimSlot {
	g.mu.RLock()
	defer g.mu.RUnlock()
	return g.slotLocked(nn)
}

func (g *simRegistry) slotLocked(nn types.NamespacedName) *controller.PlannedClaimSlot {
	if nn != g.nn {
		// One group per trial; any other name is a harness bug, and a fresh
		// slot (shared with nothing) makes it visible as ClaimRevoked rather
		// than as a silently shared claim.
		return &controller.PlannedClaimSlot{}
	}
	return g.slot
}

// LeaderContext is cancelled when the process loses leadership (at a
// restart).
func (g *simRegistry) LeaderContext() context.Context { return g.leaderCtx }

// register makes tm the current manager, wired before its first poll.
func (g *simRegistry) register(tm *controller.TopologyManager, w controller.ManagerWiring) {
	g.mu.Lock()
	w.Slot = g.slot
	// As the runner's registerManagerWith: the retiring managers' records
	// are carried into the new one (DST-1).
	for _, rm := range g.retiring {
		w.Predecessors = append(w.Predecessors, rm.tm)
	}
	if len(g.retiring) > 0 {
		for _, rm := range g.retiring {
			if rm.updateInFlight != nil && rm.updateInFlight() {
				w.RetiringUpdateInFlight = true
			}
		}
		if g.changingSites != nil {
			// As the runner's retiringChangingSites: a snapshot of the
			// retiring managers under the registry lock, inspected without it.
			w.RetiringBusy = func() bool {
				g.mu.RLock()
				retiring := append([]*retiringManager(nil), g.retiring...)
				changing := g.changingSites
				g.mu.RUnlock()
				for _, rm := range retiring {
					if changing(rm.tm) {
						return true
					}
				}
				return false
			}
		}
	}
	g.current = tm
	g.mu.Unlock()
	controller.WireRegisteredManager(tm, w)
}

// replace retires the current manager, which keeps polling for pollsLeft
// more polls, and registers next in its place.
//
// Without the retire and changingSites seams the old manager could not be
// told it is retired, and its successor could not see it changing the
// sites: two unretired managers polling side by side is a state production
// never reaches, and it shows up as double promotions. Until both seams are
// wired the old manager therefore stops at once (pollsLeft 0), and the
// replacement models only the new generation sharing the process's slot.
// inFlightWindow reports whether the window was honoured.
func (g *simRegistry) replace(next *controller.TopologyManager, w controller.ManagerWiring, pollsLeft int,
	updateInFlight func() bool) (inFlightWindow bool) {
	g.mu.Lock()
	if g.retire == nil || g.changingSites == nil {
		pollsLeft = 0
	}
	old := g.current
	g.current = nil
	if old != nil {
		g.retiring = append(g.retiring, &retiringManager{tm: old, pollsLeft: pollsLeft, updateInFlight: updateInFlight})
	}
	retire := g.retire
	g.mu.Unlock()
	if old != nil && retire != nil {
		retire(old)
	}
	g.register(next, w)
	return retire != nil
}

// Current is the manager the harness polls; nil while absent.
func (g *simRegistry) Current() *controller.TopologyManager {
	g.mu.RLock()
	defer g.mu.RUnlock()
	if g.absentPolls > 0 {
		return nil
	}
	return g.current
}

// setAbsent makes the current manager unregistered for n polls.
func (g *simRegistry) setAbsent(n int) {
	g.mu.Lock()
	defer g.mu.Unlock()
	g.absentPolls = max(g.absentPolls, n)
}

// retiringToPoll returns the retiring managers still inside their poll
// window and counts the window down; managers past it are dropped.
func (g *simRegistry) retiringToPoll() []*controller.TopologyManager {
	g.mu.Lock()
	var out, dropped []*controller.TopologyManager
	kept := g.retiring[:0]
	for _, rm := range g.retiring {
		if rm.pollsLeft > 0 {
			rm.pollsLeft--
			out = append(out, rm.tm)
		}
		if rm.pollsLeft > 0 || (rm.updateInFlight != nil && rm.updateInFlight()) {
			kept = append(kept, rm)
		} else {
			dropped = append(dropped, rm.tm)
		}
	}
	g.retiring = kept
	cur := g.current
	g.mu.Unlock()
	// As retireManagerLocked's goroutine once the retired manager's Run
	// and update workers have ended: its record reaches the current
	// manager (DST-1).
	for _, old := range dropped {
		controller.CarryRetiredFailoverRecord(cur, old)
	}
	return out
}

// endPoll counts the absence window down.
func (g *simRegistry) endPoll() {
	g.mu.Lock()
	defer g.mu.Unlock()
	if g.absentPolls > 0 {
		g.absentPolls--
	}
}

// retiringCount is how many replaced managers are still tracked.
func (g *simRegistry) retiringCount() int {
	g.mu.RLock()
	defer g.mu.RUnlock()
	return len(g.retiring)
}

// shutdown cancels the process's leader context (a restart).
func (g *simRegistry) shutdown() { g.leaderCancel() }
