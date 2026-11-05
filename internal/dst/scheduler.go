package dst

import (
	"context"
	"math/rand/v2"
	"strings"
)

// Yield points (§15.1). The planned-failover engine and its topology facade
// call controller.SimHooks.Yield at these points; simChecker.mutate yields at
// the two statement points. The string after the first ':' (site, statement)
// is detail; the prefix is the yield class the fidelity tests count.
const (
	YieldEngineLoad   = "engine.load"   // after the uncached Load
	YieldEngineWrite  = "engine.write"  // after each store Write
	YieldEngineSettle = "engine.settle" // after each Settle

	YieldPeerProbe     = "facade.peerProbe"     // after the grant's live peer probe
	YieldBeforePublish = "facade.beforePublish" // before publish under tm.mu
	YieldAfterPublish  = "facade.afterPublish"  // after publish
	YieldBeforeStmt    = "facade.beforeStmt"    // + ":<site>:<statement>"
	YieldAfterStmt     = "facade.afterStmt"     // + ":<site>:<statement>"
	YieldGateProbe     = "facade.gateProbe"     // after the pre-write gate's probe
	YieldConfirmProbe  = "facade.confirmProbe"  // after the confirm path's peer probe

	YieldStmtIssued  = "stmt.issued"  // slow statement, between issue and apply
	YieldStmtApplied = "stmt.applied" // slow statement, between apply and ack
)

// yieldClass is the point without its detail.
func yieldClass(point string) string {
	if i := strings.IndexByte(point, ':'); i >= 0 {
		return point[:i]
	}
	return point
}

// schedMaxDepth bounds nested yields: an actor stepping inside a yield may
// itself yield, up to this depth.
const schedMaxDepth = 3

// schedActor is one actor the scheduler can step at a yield.
type schedActor struct {
	name string
	// pct is the chance (0-100) the actor takes a step at a yield where it
	// is eligible.
	pct int
	// ready reports whether a step is possible now (nil: always).
	ready func() bool
	step  func(ctx context.Context)

	onStack bool
}

// lockProbe is a tracked mutex: try reports whether it is free (and leaves
// it unlocked).
type lockProbe struct {
	name string
	try  func() bool
}

// yieldScheduler interleaves the actors at yield points (§15.1 "The
// scheduler"). At each yield a PRNG-chosen subset of the other actors takes
// one step each, in registration order; an actor already on the current
// stack never re-enters, and nesting stops at schedMaxDepth.
//
// Yield discipline: a yield must never happen with a tracked mutex held,
// because the actors it runs take those mutexes. The harness is one
// goroutine (slow statements are limited to the planned and update actors,
// which issue them from it), so a failed TryLock means the current stack
// holds the lock; Yield panics naming it.
type yieldScheduler struct {
	rng    *rand.Rand
	actors []*schedActor
	locks  []lockProbe
	// observers see every yield before any actor steps (invariants, the
	// restart-promoted-target countdown).
	observers []func(point string)

	depth int

	// yields counts yields per class; steps counts, per class, the yields
	// at which at least one other actor stepped.
	yields map[string]int
	steps  map[string]int
	// stepsBy counts steps per actor.
	stepsBy map[string]int
}

// schedStreamTag keys the scheduler's PRNG stream, distinct from the fault
// schedule's and the actor schedule's, so adding a scheduler draw changes
// no generated op.
const schedStreamTag = 0x5c4ed017

func newYieldScheduler(seed uint64) *yieldScheduler {
	return &yieldScheduler{
		rng:     rand.New(rand.NewPCG(seed, schedStreamTag)),
		yields:  map[string]int{},
		steps:   map[string]int{},
		stepsBy: map[string]int{},
	}
}

// addActor registers an actor; registration order is step order.
func (s *yieldScheduler) addActor(a *schedActor) { s.actors = append(s.actors, a) }

// trackLock adds a mutex to the yield-discipline check.
func (s *yieldScheduler) trackLock(name string, try func() bool) {
	s.locks = append(s.locks, lockProbe{name: name, try: try})
}

// observe adds a yield observer.
func (s *yieldScheduler) observe(fn func(point string)) { s.observers = append(s.observers, fn) }

// actor returns the registered actor named name (nil if none).
func (s *yieldScheduler) actor(name string) *schedActor {
	for _, a := range s.actors {
		if a.name == name {
			return a
		}
	}
	return nil
}

// run runs fn as the named actor's top-level step: it is on the stack for
// the duration, so no yield inside fn re-enters it.
func (s *yieldScheduler) run(name string, fn func()) {
	a := s.actor(name)
	if a == nil {
		fn()
		return
	}
	if a.onStack {
		return
	}
	a.onStack = true
	defer func() { a.onStack = false }()
	fn()
}

// assertNoLocks panics when a tracked mutex is held at a yield.
func (s *yieldScheduler) assertNoLocks(point string) {
	for _, l := range s.locks {
		if !l.try() {
			panic("dst: yield at " + point + " while holding " + l.name)
		}
	}
}

// Yield is controller.SimHooks.Yield and Cluster's slow-statement hook.
func (s *yieldScheduler) Yield(point string) {
	s.assertNoLocks(point)
	class := yieldClass(point)
	s.yields[class]++
	for _, fn := range s.observers {
		fn(point)
	}
	if s.depth >= schedMaxDepth {
		return
	}
	s.depth++
	defer func() { s.depth-- }()
	stepped := false
	for _, a := range s.actors {
		// One draw per actor per yield, eligible or not, so eligibility
		// changes elsewhere do not shift which later actors step.
		roll := s.rng.IntN(100)
		if a.onStack || roll >= a.pct || (a.ready != nil && !a.ready()) {
			continue
		}
		a.onStack = true
		a.step(context.Background())
		a.onStack = false
		s.stepsBy[a.name]++
		stepped = true
	}
	if stepped {
		s.steps[class]++
	}
}
