package dst

import (
	"context"
	"math/rand/v2"
	"strings"

	"github.com/shipstream/bloodraven/internal/controller"
)

// Yield points (§15.1). The planned-failover engine and its topology facade
// call controller.SimHooks.Yield at these points; simChecker.mutate yields at
// the two statement points. The string after the first ':' (site, statement)
// is detail; the prefix is the yield class the fidelity tests count. The
// production points are aliases of the controller's constants, so the two
// lists cannot drift (K187); the statement points are the simulator's own.
const (
	YieldEngineLoad   = controller.YieldEngineLoadForSim   // after the uncached Load
	YieldEngineWrite  = controller.YieldEngineWriteForSim  // after each store Write
	YieldEngineSettle = controller.YieldEngineSettleForSim // after each Settle

	YieldPeerProbe = controller.YieldPeerProbeForSim // after a live peer probe (grant, fence, rollback)
	// YieldBeforePublish is before the grant's publish, outside tm.mu: the
	// publish takes the lock after the yield, and a yield with tm.mu held
	// would panic the scheduler's lock discipline.
	YieldBeforePublish = controller.YieldBeforePublishForSim
	YieldAfterPublish  = controller.YieldAfterPublishForSim // after the grant's publish
	YieldBeforeStmt    = controller.YieldBeforeStmtForSim   // + ":<site>:<statement>"
	YieldAfterStmt     = controller.YieldAfterStmtForSim    // + ":<site>:<statement>"
	YieldGateProbe     = controller.YieldGateProbeForSim    // after the pre-write gate's probe
	YieldConfirmProbe  = controller.YieldConfirmProbeForSim // after the confirm path's peer probe

	YieldStmtIssued  = "stmt.issued"  // slow statement, between issue and apply
	YieldStmtApplied = "stmt.applied" // slow statement, between apply and ack

	// YieldSnapshotCommit is between a topology-manager status write's read
	// and its conditioned commit (runner updateCRStatus, K042).
	YieldSnapshotCommit = "store.snapshotCommit"
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
	// onSwitch is called whenever control is about to leave the actor on
	// top of the stack (owner, "" at top level): at a yield, and when an
	// actor's step or run ends. The harness attributes what happened since
	// the last switch (claim releases) to owner.
	onSwitch func(owner string)
	// stack is the actors currently running, innermost last.
	stack []string

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
	s.push(name)
	defer func() {
		s.switchFrom()
		s.pop()
		a.onStack = false
	}()
	fn()
}

func (s *yieldScheduler) push(name string) { s.stack = append(s.stack, name) }
func (s *yieldScheduler) pop()             { s.stack = s.stack[:len(s.stack)-1] }

// top is the innermost running actor ("" at top level).
func (s *yieldScheduler) top() string {
	if len(s.stack) == 0 {
		return ""
	}
	return s.stack[len(s.stack)-1]
}

// switchFrom reports that control leaves the innermost running actor.
func (s *yieldScheduler) switchFrom() {
	if s.onSwitch != nil {
		s.onSwitch(s.top())
	}
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
	s.switchFrom()
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
		s.push(a.name)
		a.step(context.Background())
		s.switchFrom()
		s.pop()
		a.onStack = false
		s.stepsBy[a.name]++
		stepped = true
	}
	if stepped {
		s.steps[class]++
	}
}
