package dst

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"strconv"
	"strings"
	"sync"
	"time"

	apimeta "k8s.io/apimachinery/pkg/api/meta"
	"k8s.io/apimachinery/pkg/types"

	v1alpha1 "github.com/shipstream/bloodraven/api/v1alpha1"
	"github.com/shipstream/bloodraven/internal/clock"
	"github.com/shipstream/bloodraven/internal/controller"
)

// simPlannedStore is the simulated API server for one MysqlFailoverGroup:
// a single object (metadata annotations plus status) with an integer
// resourceVersion, and the controller.PlannedFailoverStore the planned
// failover engine runs against (§15.1).
//
// Every writer contends on the one resourceVersion, exactly as in
// production: the engine's conditioned status writes and metadata writes,
// the topology manager's snapshot writes (MergeSnapshot, which the harness
// wires to StatusCallback), the out-of-band failover record
// (PatchAnnotations) and the planned request annotation (SetRequest). A
// write that changes no bytes does not move the resourceVersion (the no-op
// trap, §2.4 step 7).
//
// Faults are scripted per write and per poll:
//   - LoseWrites: the request times out and never commits;
//   - AmbiguousWrites: the write commits and the response times out;
//   - LateWrites: the request times out and commits k polls later iff the
//     resourceVersion it was conditioned on is still current then; a fence
//     or marker write in between moves it and kills the late commit;
//   - ConflictWrites / DefiniteWrites: a 409 / 422 with nothing committed;
//   - DenyStatus: every status write (the engine's and the topology
//     manager's) is forbidden for n polls while metadata writes still work;
//   - ReadOutage: uncached reads (Load, settlement) fail for n polls.
//
// Ambiguous outcomes are settled inside Write by Settle, so the engine sees
// only the WriteOutcome classes of the contract.
//
// The harness is one goroutine; the mutex makes the store -race clean if a
// test drives it from several and is never held across a yield.
type simPlannedStore struct {
	mu  sync.Mutex
	nn  types.NamespacedName
	clk clock.Clock

	obj *v1alpha1.MysqlFailoverGroup // nil: the CR is gone
	rv  int64

	poll int

	// dead reports whether the operator process is dead (its calls fail);
	// clusterStatusDenied is the pre-existing OpStatusOutage knob, which
	// denies the same /status subresource. Both may be nil.
	dead                func() bool
	clusterStatusDenied func() bool

	script       []storeFault
	late         []lateCommit
	statusDenied int // polls left
	// statusDeniedLong is the part of statusDenied a long outage (K036)
	// keeps past heal-all, in polls left.
	statusDeniedLong int
	readOutage       int // polls left
	// settleOutage fails this many settlement reads (not the pass's Load):
	// a read outage that starts between a write and its settlement.
	settleOutage int
	// fenceSeq counts settlement fences (their distinct nanoseconds).
	fenceSeq int64
	// yield is the scheduler's yield between a snapshot write's read and
	// its commit (nil: none).
	yield func(point string)
	// metaScript scripts the outcome of the next engine metadata writes
	// (MetaFaults).
	metaScript []metaFault

	// onCommit observes every committed change (kind names the writer)
	// with the object before and after; invariants hang off it.
	onCommit func(kind string, before, after *v1alpha1.MysqlFailoverGroup)

	// log is every store operation and its outcome, in order.
	log []storeOp
}

// storeFaultKind is a scripted outcome for the next engine status write.
type storeFaultKind int

const (
	storeFaultLost storeFaultKind = iota
	storeFaultAmbiguousCommitted
	storeFaultLate
	storeFaultConflict
	storeFaultDefinite
)

func (k storeFaultKind) String() string {
	switch k {
	case storeFaultLost:
		return "lost"
	case storeFaultAmbiguousCommitted:
		return "ambiguousCommitted"
	case storeFaultLate:
		return "late"
	case storeFaultConflict:
		return "conflict"
	case storeFaultDefinite:
		return "definite"
	}
	return fmt.Sprintf("storeFault(%d)", int(k))
}

type storeFault struct {
	kind  storeFaultKind
	delay int // storeFaultLate: polls until the late commit is attempted
}

// lateCommit is a status write whose response was lost and which the server
// applies later, iff its precondition still holds.
type lateCommit struct {
	at     int
	baseRV int64
	status v1alpha1.MysqlFailoverGroupStatus
}

// storeOp is one entry of the store's operation log.
type storeOp struct {
	Poll    int
	Op      string
	Outcome string
}

func (o storeOp) String() string { return fmt.Sprintf("p%03d %s → %s", o.Poll, o.Op, o.Outcome) }

var _ controller.PlannedFailoverStore = (*simPlannedStore)(nil)

var (
	errStoreTimeout   = errors.New("context deadline exceeded (sim: request timed out)")
	errStoreForbidden = errors.New("mysqlfailovergroups/status is forbidden (sim: 403)")
	errStoreInvalid   = errors.New("MysqlFailoverGroup.shipstream.io is invalid (sim: 422)")
	errStoreConflict  = errors.New("the object has been modified; please apply your changes to the latest version (sim: 409)")
	errStoreReadDown  = errors.New("etcdserver: request timed out (sim: read outage)")
	errStoreGone      = errors.New("mysqlfailovergroups not found (sim: 404)")
)

// newSimPlannedStore builds the store around obj (copied).
func newSimPlannedStore(obj *v1alpha1.MysqlFailoverGroup, clk clock.Clock) *simPlannedStore {
	s := &simPlannedStore{
		nn:  types.NamespacedName{Namespace: obj.Namespace, Name: obj.Name},
		clk: clk,
		obj: obj.DeepCopy(),
		rv:  1,
	}
	s.obj.ResourceVersion = "1"
	return s
}

// ---------------------------------------------------------------------------
// Fault knobs (harness side)
// ---------------------------------------------------------------------------

func (s *simPlannedStore) push(k storeFaultKind, n, delay int) {
	s.mu.Lock()
	defer s.mu.Unlock()
	for range n {
		s.script = append(s.script, storeFault{kind: k, delay: delay})
	}
}

// LoseWrites makes the next n engine status writes time out uncommitted.
func (s *simPlannedStore) LoseWrites(n int) { s.push(storeFaultLost, n, 0) }

// AmbiguousWrites makes the next n engine status writes commit and time out.
func (s *simPlannedStore) AmbiguousWrites(n int) { s.push(storeFaultAmbiguousCommitted, n, 0) }

// LateWrites makes the next n engine status writes time out and commit k
// polls later iff their resourceVersion is still current.
func (s *simPlannedStore) LateWrites(n, k int) { s.push(storeFaultLate, n, max(k, 1)) }

// ConflictWrites makes the next n engine status writes return 409.
func (s *simPlannedStore) ConflictWrites(n int) { s.push(storeFaultConflict, n, 0) }

// DefiniteWrites makes the next n engine status writes return 422.
func (s *simPlannedStore) DefiniteWrites(n int) { s.push(storeFaultDefinite, n, 0) }

// DenyStatus forbids every status write for n polls; metadata writes work.
func (s *simPlannedStore) DenyStatus(n int) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.statusDenied = max(s.statusDenied, n)
}

// metaFault is a scripted outcome for the next engine metadata write.
type metaFault int

const (
	metaFaultLost      metaFault = iota // timed out, never committed
	metaFaultAmbiguous                  // committed, the response lost
	metaFaultForbidden                  // 403, nothing committed
)

// MetaFaults makes the next n engine metadata writes fail with class k
// (OpPFMetaFault's K).
func (s *simPlannedStore) MetaFaults(n, k int) {
	s.mu.Lock()
	defer s.mu.Unlock()
	for range n {
		s.metaScript = append(s.metaScript, metaFault(k))
	}
}

// DenyStatusLong is DenyStatus for an outage that outlives heal-all.
func (s *simPlannedStore) DenyStatusLong(n int) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.statusDenied = max(s.statusDenied, n)
	s.statusDeniedLong = max(s.statusDeniedLong, n)
}

// ReadOutage fails uncached reads for n polls.
func (s *simPlannedStore) ReadOutage(n int) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.readOutage = max(s.readOutage, n)
}

// ClearFaults drops every scripted and timed fault (heal-all) except a long
// status denial, which runs its course. Pending late commits stay: they
// are requests already in flight.
func (s *simPlannedStore) ClearFaults() {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.script = nil
	s.metaScript = nil
	s.statusDenied = s.statusDeniedLong
	s.readOutage = 0
}

// BeginPoll advances the store to poll p: timed faults count down and due
// late commits are attempted.
func (s *simPlannedStore) BeginPoll(p int) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if p > s.poll {
		elapsed := p - s.poll
		s.statusDenied = max(0, s.statusDenied-elapsed)
		s.statusDeniedLong = max(0, s.statusDeniedLong-elapsed)
		s.readOutage = max(0, s.readOutage-elapsed)
	}
	s.poll = p
	kept := s.late[:0]
	for _, lc := range s.late {
		if lc.at > p {
			kept = append(kept, lc)
			continue
		}
		switch {
		case s.obj == nil:
			s.logLocked("lateCommit", "gone")
		case s.rv != lc.baseRV:
			s.logLocked("lateCommit", fmt.Sprintf("refused (rv %d != base %d)", s.rv, lc.baseRV))
		default:
			s.commitStatusLocked("lateStatus", lc.status)
			s.logLocked("lateCommit", "committed")
		}
	}
	s.late = kept
}

// ---------------------------------------------------------------------------
// Other writers (harness side)
// ---------------------------------------------------------------------------

// Object returns a deep copy of the current object (nil when gone).
func (s *simPlannedStore) Object() *v1alpha1.MysqlFailoverGroup {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.obj == nil {
		return nil
	}
	return s.obj.DeepCopy()
}

// ResourceVersion is the current resourceVersion.
func (s *simPlannedStore) ResourceVersion() int64 {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.rv
}

// Log returns the operation log.
func (s *simPlannedStore) Log() []storeOp {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]storeOp(nil), s.log...)
}

// MergeSnapshot is the topology manager's status write, in the runner's
// two steps (updateCRStatus, K042): read the group and merge the snapshot
// into that status through the production MergeTopologySnapshotStatus;
// then, possibly after other writers have moved on (a scheduler yield),
// re-read it and write the computed status back over the fresh object,
// keeping from the fresh one only the subtrees the runner preserves (the
// planned-failover block and hold, the reconciler-owned conditions), and
// merging the failover record again over the fresh one (K058): a newer
// record committed between the two steps, the planned engine's terminal
// write say, is not replaced by the copy merged at the first read. Any
// other field is written back as it was at the first read. It fails while
// status writes are denied.
func (s *simPlannedStore) MergeSnapshot(snap controller.TopologySnapshot) error {
	s.mu.Lock()
	if s.obj == nil {
		s.mu.Unlock()
		return errStoreGone
	}
	if s.statusDeniedLocked() {
		s.logLocked("snapshot", "forbidden")
		s.mu.Unlock()
		return errStoreForbidden
	}
	computed := *s.obj.Status.DeepCopy()
	if !controller.MergeTopologySnapshotStatus(&computed, snap) {
		s.logLocked("snapshot", "stale generation")
		s.mu.Unlock()
		return nil
	}
	s.mu.Unlock()
	if s.yield != nil {
		s.yield(YieldSnapshotCommit)
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.obj == nil {
		return errStoreGone
	}
	if s.statusDeniedLocked() {
		s.logLocked("snapshot", "forbidden")
		return errStoreForbidden
	}
	fresh := s.obj.Status
	if fresh.TopologyGeneration > snap.TopologyGeneration {
		s.logLocked("snapshot", "superseded")
		return nil
	}
	next := *computed.DeepCopy()
	// As updateCRStatus's second step: status.lastFailover starts from the
	// live record and the snapshot's is merged over it under the
	// timestamp-monotonic rule; observedGeneration never moves back.
	live := *fresh.DeepCopy()
	controller.MergeTopologySnapshotStatus(&live, snap)
	next.LastFailover = live.LastFailover
	next.ObservedGeneration = max(fresh.ObservedGeneration, next.ObservedGeneration)
	next.PlannedFailover = fresh.PlannedFailover.DeepCopy()
	next.PlannedFailoverHold = fresh.PlannedFailoverHold.DeepCopy()
	for _, t := range runnerPreservedConditions {
		if c := apimeta.FindStatusCondition(fresh.Conditions, t); c != nil {
			apimeta.SetStatusCondition(&next.Conditions, *c)
		} else {
			apimeta.RemoveStatusCondition(&next.Conditions, t)
		}
	}
	s.commitStatusLocked("snapshot", next)
	return nil
}

// runnerPreservedConditions are the reconciler-owned conditions the
// runner's status write re-reads instead of replaying (runner.go
// reconcilerOwnedConditions) that the simulated group carries.
var runnerPreservedConditions = []string{v1alpha1.ConditionAutomaticFailoverHeld}

// PatchAnnotations is an unconditioned JSON merge patch of metadata (the
// out-of-band failover record today; "" deletes a key).
func (s *simPlannedStore) PatchAnnotations(kind string, kv map[string]string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.obj == nil {
		return errStoreGone
	}
	s.patchAnnotationsLocked(kind, kv)
	return nil
}

// SetRequest sets the planned-failover request annotation.
func (s *simPlannedStore) SetRequest(value string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.obj == nil {
		return
	}
	s.patchAnnotationsLocked("request", map[string]string{v1alpha1.PlannedFailoverAnnotation: value})
}

// ---------------------------------------------------------------------------
// controller.PlannedFailoverStore
// ---------------------------------------------------------------------------

// Load returns a deep copy of the object; (nil, nil) when it is gone.
func (s *simPlannedStore) Load(_ context.Context, _ types.NamespacedName) (*v1alpha1.MysqlFailoverGroup, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.loadLocked()
}

func (s *simPlannedStore) loadLocked() (*v1alpha1.MysqlFailoverGroup, error) {
	if s.deadLocked() {
		return nil, errOperatorDead
	}
	if s.readOutage > 0 {
		s.logLocked("load", "read outage")
		return nil, errStoreReadDown
	}
	if s.obj == nil {
		return nil, nil
	}
	return s.obj.DeepCopy(), nil
}

// Write is the engine's conditioned status write (§2.4 steps 3-5).
func (s *simPlannedStore) Write(ctx context.Context, base *v1alpha1.MysqlFailoverGroup, next v1alpha1.PlannedFailoverStatus,
	mutate func(*v1alpha1.MysqlFailoverGroupStatus)) (controller.WriteOutcome, error) {
	status := *base.Status.DeepCopy()
	pf := next.DeepCopy()
	status.PlannedFailover = pf
	if mutate != nil {
		mutate(&status)
	}
	op := fmt.Sprintf("write %s rev=%d phase=%s", pf.RunID, pf.Revision, pf.Phase)
	committed := func(cur *v1alpha1.MysqlFailoverGroup) bool {
		got := cur.Status.PlannedFailover
		return got != nil && got.RunID == pf.RunID && got.Revision >= pf.Revision
	}
	return s.statusWrite(ctx, op, base, status, committed)
}

// statusWrite issues one conditioned status write and settles an ambiguous
// response with committed, the predicate that recognises the write in a
// fresh read.
func (s *simPlannedStore) statusWrite(ctx context.Context, op string, base *v1alpha1.MysqlFailoverGroup,
	status v1alpha1.MysqlFailoverGroupStatus, committed func(*v1alpha1.MysqlFailoverGroup) bool) (controller.WriteOutcome, error) {
	s.mu.Lock()
	if s.deadLocked() {
		s.mu.Unlock()
		return controller.WriteUnknown, errOperatorDead
	}
	if s.obj == nil {
		s.logLocked(op, "gone")
		s.mu.Unlock()
		return controller.WriteGone, errStoreGone
	}
	if s.statusDeniedLocked() {
		s.logLocked(op, "forbidden")
		s.mu.Unlock()
		return controller.WriteNotCommitted, errStoreForbidden
	}
	baseRV := parseRV(base.ResourceVersion)
	var fault *storeFault
	if len(s.script) > 0 {
		f := s.script[0]
		s.script = s.script[1:]
		fault = &f
	}
	if fault != nil {
		switch fault.kind {
		case storeFaultConflict:
			s.logLocked(op, "conflict (injected)")
			s.mu.Unlock()
			return controller.WriteConflict, errStoreConflict
		case storeFaultDefinite:
			s.logLocked(op, "invalid (injected)")
			s.mu.Unlock()
			return controller.WriteNotCommitted, errStoreInvalid
		case storeFaultLost:
			s.logLocked(op, "timeout, not committed")
			s.mu.Unlock()
			return s.settle(ctx, base, committed)
		case storeFaultLate:
			s.late = append(s.late, lateCommit{at: s.poll + fault.delay, baseRV: baseRV, status: status})
			s.logLocked(op, fmt.Sprintf("timeout, late commit at p%03d", s.poll+fault.delay))
			s.mu.Unlock()
			return s.settle(ctx, base, committed)
		}
	}
	if baseRV != s.rv {
		s.logLocked(op, fmt.Sprintf("conflict (rv %d != base %d)", s.rv, baseRV))
		s.mu.Unlock()
		return controller.WriteConflict, errStoreConflict
	}
	s.commitStatusLocked("status", status)
	if fault != nil && fault.kind == storeFaultAmbiguousCommitted {
		s.logLocked(op, "committed, response lost")
		s.mu.Unlock()
		return s.settle(ctx, base, committed)
	}
	s.logLocked(op, "committed")
	s.mu.Unlock()
	return controller.WriteCommitted, nil
}

// Settle is §2.4 step 5 for a write of (runID, revision) against baseRV.
func (s *simPlannedStore) Settle(ctx context.Context, _ types.NamespacedName, baseRV, runID string,
	revision int64) (controller.WriteOutcome, error) {
	base := &v1alpha1.MysqlFailoverGroup{}
	base.ResourceVersion = baseRV
	return s.settle(ctx, base, func(cur *v1alpha1.MysqlFailoverGroup) bool {
		got := cur.Status.PlannedFailover
		return got != nil && got.RunID == runID && got.Revision >= revision
	})
}

// settleAttempts is §2.4 step 5's bound.
const settleAttempts = 3

func (s *simPlannedStore) settle(_ context.Context, base *v1alpha1.MysqlFailoverGroup,
	committed func(*v1alpha1.MysqlFailoverGroup) bool) (controller.WriteOutcome, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	baseRV := parseRV(base.ResourceVersion)
	var lastErr error
	for attempt := 1; attempt <= settleAttempts; attempt++ {
		cur, err := s.loadLocked()
		if err == nil && s.settleOutage > 0 {
			s.settleOutage--
			cur, err = nil, errStoreReadDown
		}
		if err != nil {
			lastErr = err
			continue
		}
		if cur == nil {
			s.logLocked("settle", "gone")
			return controller.WriteGone, errStoreGone
		}
		if committed(cur) {
			s.logLocked("settle", "committed")
			return controller.WriteCommitted, nil
		}
		if s.rv != baseRV {
			s.logLocked("settle", "not committed (rv moved)")
			return controller.WriteNotCommitted, errStoreTimeout
		}
		// rv == base: fence it. The fence is a conditioned metadata write
		// on base, so once it lands the pending write's precondition can
		// never hold again. Production stamps a nanosecond wall-clock
		// instant; the fake clock moves once per poll, so each fence adds
		// its own nanosecond to keep two fences of one tick distinct
		// (K029). A fence that moved nothing proves nothing: Unknown.
		s.fenceSeq++
		s.patchAnnotationsLocked("statusFence", map[string]string{
			v1alpha1.PlannedFailoverStatusFenceAnnotation: controller.FormatStatusFenceValue(
				s.clk.Now().Add(time.Duration(s.fenceSeq))),
		})
		if s.rv == baseRV {
			s.logLocked("settle", "unknown (fence changed nothing)")
			return controller.WriteUnknown, errStoreTimeout
		}
		s.logLocked("settle", "not committed (fenced)")
		return controller.WriteNotCommitted, errStoreTimeout
	}
	s.logLocked("settle", "unknown")
	return controller.WriteUnknown, fmt.Errorf("settle: %w", lastErr)
}

// WriteTerminalMarker writes the terminal marker, conditioned on base.
func (s *simPlannedStore) WriteTerminalMarker(_ context.Context, base *v1alpha1.MysqlFailoverGroup,
	m controller.TerminalMarker) (controller.WriteOutcome, error) {
	return s.metadataWrite("terminalMarker", base, func(a map[string]string) map[string]string {
		return map[string]string{v1alpha1.PlannedFailoverTerminalAnnotation: m.String()}
	})
}

// ClearTerminalMarker removes the marker when it names runID.
func (s *simPlannedStore) ClearTerminalMarker(_ context.Context, base *v1alpha1.MysqlFailoverGroup,
	runID string) (controller.WriteOutcome, error) {
	return s.metadataWrite("clearTerminalMarker", base, func(a map[string]string) map[string]string {
		if markerRunID(a[v1alpha1.PlannedFailoverTerminalAnnotation]) != runID {
			return nil
		}
		return map[string]string{v1alpha1.PlannedFailoverTerminalAnnotation: ""}
	})
}

// ConsumeRequest removes the request annotation.
func (s *simPlannedStore) ConsumeRequest(_ context.Context, base *v1alpha1.MysqlFailoverGroup) (controller.WriteOutcome, error) {
	return s.metadataWrite("consumeRequest", base, func(a map[string]string) map[string]string {
		if _, ok := a[v1alpha1.PlannedFailoverAnnotation]; !ok {
			return nil
		}
		return map[string]string{v1alpha1.PlannedFailoverAnnotation: ""}
	})
}

// MarkDeadlineStart records now for (kind, runID) unless the annotation
// already names runID, and returns the durable start.
func (s *simPlannedStore) MarkDeadlineStart(_ context.Context, base *v1alpha1.MysqlFailoverGroup,
	kind controller.DeadlineKind, runID string, now time.Time) (time.Time, controller.WriteOutcome, error) {
	key := kind.Annotation()
	if key == "" {
		return time.Time{}, controller.WriteNotCommitted, fmt.Errorf("unknown deadline kind %q", kind)
	}
	var start time.Time
	out, err := s.metadataWrite("markDeadline:"+string(kind), base, func(a map[string]string) map[string]string {
		if id, t, ok := parseDeadlineStart(a[key]); ok && id == runID {
			start = t
			return nil
		}
		start = now.UTC().Truncate(time.Second)
		return map[string]string{key: controller.FormatDeadlineStart(runID, start)}
	})
	if out != controller.WriteCommitted {
		return time.Time{}, out, err
	}
	return start, out, nil
}

// ClearDeadlineStart removes the annotation when it names runID.
func (s *simPlannedStore) ClearDeadlineStart(_ context.Context, base *v1alpha1.MysqlFailoverGroup,
	kind controller.DeadlineKind, runID string) (controller.WriteOutcome, error) {
	key := kind.Annotation()
	return s.metadataWrite("clearDeadline:"+string(kind), base, func(a map[string]string) map[string]string {
		if id, _, ok := parseDeadlineStart(a[key]); !ok || id != runID {
			return nil
		}
		return map[string]string{key: ""}
	})
}

// ClearHold commits final (when non-nil) and the removal of the
// status.plannedFailoverHold naming runID (its condition set False) in one
// status write.
func (s *simPlannedStore) ClearHold(ctx context.Context, base *v1alpha1.MysqlFailoverGroup, runID string,
	final *v1alpha1.PlannedFailoverStatus) (controller.WriteOutcome, error) {
	status := *base.Status.DeepCopy()
	changed := false
	if final != nil {
		status.PlannedFailover = final.DeepCopy()
		changed = true
	}
	if controller.ClearPlannedHold(&status, runID, base.Generation, s.clk.Now()) {
		changed = true
	}
	if !changed {
		// As production: nothing to change from base is a no-op success
		// with no API call, so it cannot meet a fault (K106).
		s.mu.Lock()
		s.logLocked("clearHold "+runID, "no-op (base)")
		s.mu.Unlock()
		return controller.WriteCommitted, nil
	}
	committed := func(cur *v1alpha1.MysqlFailoverGroup) bool {
		if holdNames(&cur.Status, runID) {
			return false
		}
		if final == nil {
			return true
		}
		got := cur.Status.PlannedFailover
		return got != nil && got.RunID == final.RunID && got.Revision >= final.Revision
	}
	return s.statusWrite(ctx, "clearHold "+runID, base, status, committed)
}

// DeleteHoldAck removes the acknowledgement annotation when it names runID.
func (s *simPlannedStore) DeleteHoldAck(_ context.Context, base *v1alpha1.MysqlFailoverGroup,
	runID string) (controller.WriteOutcome, error) {
	return s.metadataWrite("deleteHoldAck", base, func(a map[string]string) map[string]string {
		if v, ok := a[v1alpha1.AcknowledgePlannedFailoverHoldAnnotation]; !ok || strings.TrimSpace(v) != runID {
			return nil
		}
		return map[string]string{v1alpha1.AcknowledgePlannedFailoverHoldAnnotation: ""}
	})
}

// metadataWrite is a MergeFromWithOptimisticLock patch on base: patch
// computes the change from base's annotations, as production's
// patchAnnotations edits a copy of base. Nothing to change (nil) is a
// no-op success with no API call: no death, gone or resourceVersion check
// can fail it (K106).
func (s *simPlannedStore) metadataWrite(op string, base *v1alpha1.MysqlFailoverGroup,
	patch func(map[string]string) map[string]string) (controller.WriteOutcome, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	kv := patch(base.GetAnnotations())
	if kv == nil {
		s.logLocked(op, "no-op (base)")
		return controller.WriteCommitted, nil
	}
	if s.deadLocked() {
		return controller.WriteUnknown, errOperatorDead
	}
	if s.obj == nil {
		s.logLocked(op, "gone")
		return controller.WriteGone, errStoreGone
	}
	if len(s.metaScript) > 0 {
		f := s.metaScript[0]
		s.metaScript = s.metaScript[1:]
		switch f {
		case metaFaultLost:
			s.logLocked(op, "timeout, not committed (injected)")
			return controller.WriteUnknown, errStoreTimeout
		case metaFaultForbidden:
			s.logLocked(op, "forbidden (injected)")
			return controller.WriteNotCommitted, errStoreForbidden
		case metaFaultAmbiguous:
			if baseRV := parseRV(base.ResourceVersion); baseRV != s.rv {
				s.logLocked(op, "conflict, response lost (injected)")
				return controller.WriteUnknown, errStoreTimeout
			}
			s.patchAnnotationsLocked(op, kv)
			s.logLocked(op, "committed, response lost (injected)")
			return controller.WriteUnknown, errStoreTimeout
		}
	}
	if baseRV := parseRV(base.ResourceVersion); baseRV != s.rv {
		s.logLocked(op, fmt.Sprintf("conflict (rv %d != base %d)", s.rv, baseRV))
		return controller.WriteConflict, errStoreConflict
	}
	s.patchAnnotationsLocked(op, kv)
	s.logLocked(op, "committed")
	return controller.WriteCommitted, nil
}

// ---------------------------------------------------------------------------
// internals (s.mu held)
// ---------------------------------------------------------------------------

func (s *simPlannedStore) deadLocked() bool { return s.dead != nil && s.dead() }

func (s *simPlannedStore) statusDeniedLocked() bool {
	return s.statusDenied > 0 || (s.clusterStatusDenied != nil && s.clusterStatusDenied())
}

func (s *simPlannedStore) logLocked(op, outcome string) {
	s.log = append(s.log, storeOp{Poll: s.poll, Op: op, Outcome: outcome})
}

// commitStatusLocked replaces the status; a byte-identical status does not
// move the resourceVersion.
func (s *simPlannedStore) commitStatusLocked(kind string, status v1alpha1.MysqlFailoverGroupStatus) {
	if reflect.DeepEqual(s.obj.Status, status) {
		return
	}
	before := s.obj.DeepCopy()
	s.obj.Status = status
	s.bumpLocked(kind, before)
}

// patchAnnotationsLocked applies a merge patch; "" deletes the key.
func (s *simPlannedStore) patchAnnotationsLocked(kind string, kv map[string]string) {
	before := s.obj.DeepCopy()
	a := s.obj.GetAnnotations()
	if a == nil {
		a = map[string]string{}
	}
	changed := false
	for k, v := range kv {
		old, had := a[k]
		switch {
		case v == "" && had:
			delete(a, k)
			changed = true
		case v != "" && (!had || old != v):
			a[k] = v
			changed = true
		}
	}
	if !changed {
		return
	}
	s.obj.SetAnnotations(a)
	s.bumpLocked(kind, before)
}

func (s *simPlannedStore) bumpLocked(kind string, before *v1alpha1.MysqlFailoverGroup) {
	s.rv++
	s.obj.ResourceVersion = strconv.FormatInt(s.rv, 10)
	if s.onCommit != nil {
		s.onCommit(kind, before, s.obj.DeepCopy())
	}
}

func parseRV(rv string) int64 {
	n, err := strconv.ParseInt(rv, 10, 64)
	if err != nil {
		return -1
	}
	return n
}

// markerRunID is the runID a terminal-marker value names ("" if malformed).
func markerRunID(v string) string {
	m, ok := controller.ParseTerminalMarker(v)
	if !ok {
		return ""
	}
	return m.RunID
}

// parseDeadlineStart reads a controller.FormatDeadlineStart value.
func parseDeadlineStart(v string) (runID string, t time.Time, ok bool) {
	return controller.ParseDeadlineStart(v)
}

// holdNames reports whether status.plannedFailoverHold names runID.
func holdNames(st *v1alpha1.MysqlFailoverGroupStatus, runID string) bool {
	return st.PlannedFailoverHold != nil && st.PlannedFailoverHold.RunID == runID
}

// holdStands reports whether obj carries either durable hold record:
// status.plannedFailoverHold (or a True AutomaticFailoverHeld condition
// without it, which the controller treats as a hold), or a terminal marker
// with outcome ManualRecoveryRequired (§10.5).
func holdStands(obj *v1alpha1.MysqlFailoverGroup) bool {
	if obj == nil {
		return false
	}
	if obj.Status.PlannedFailoverHold != nil || holdConditionTrue(obj) {
		return true
	}
	m, ok := controller.ParseTerminalMarker(obj.GetAnnotations()[v1alpha1.PlannedFailoverTerminalAnnotation])
	return ok && m.Outcome == v1alpha1.PlannedFailoverOutcomeManualRecoveryRequired
}

// PlannedStore is the simulated planned-failover store, exported for the
// component tests (test/component) so both use one implementation of the
// write protocol and its fault script.
type PlannedStore = simPlannedStore

// NewPlannedStore returns a store over a copy of obj, its fence and
// deadline timestamps taken from clk.
func NewPlannedStore(obj *v1alpha1.MysqlFailoverGroup, clk clock.Clock) *PlannedStore {
	return newSimPlannedStore(obj, clk)
}

// SetDead installs the operator-death check (nil: never dead).
func (s *simPlannedStore) SetDead(dead func() bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.dead = dead
}

// SetAnnotations patches annotations as an unconditioned writer would ("" deletes).
func (s *simPlannedStore) SetAnnotations(kv map[string]string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.obj == nil {
		return
	}
	s.patchAnnotationsLocked("annotations", kv)
}

// MutateStatus applies fn to the stored status as an unconditioned status
// writer would (another controller, a human). A no-op change does not move
// the resourceVersion.
func (s *simPlannedStore) MutateStatus(fn func(*v1alpha1.MysqlFailoverGroupStatus)) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.obj == nil {
		return
	}
	next := *s.obj.Status.DeepCopy()
	fn(&next)
	s.commitStatusLocked("mutate", next)
}

// LogStrings renders the operation log.
func (s *simPlannedStore) LogStrings() []string {
	var out []string
	for _, op := range s.Log() {
		out = append(out, op.String())
	}
	return out
}

// SettleOutage fails the next n settlement reads (not the pass's Load): a
// read outage that starts between a write and its settlement.
func (s *simPlannedStore) SettleOutage(n int) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.settleOutage += n
}
