// Package controller — the planned-failover step engine.
//
// PlannedFailoverEngine drives one planned-failover run per group through
// its phases, one bounded pass per Step:
//
//	Admitting ──claim──► Fencing ─► Draining ─► WaitingForLag ─┬─► Promoting ─► Publishing ─► Succeeded
//	   │ (Deferred)                                            └─► WaitingForDragonflySync ─► PromotingDragonfly ─┘
//	   ▼                         any failure while nothing was written to the target ─► RollingBack ─► Failed
//	Failed{NotStarted}
//
// Every pass starts from an uncached read (PlannedFailoverStore.Load), and
// every decision is made from it. A record is made durable before the side
// effect it announces, and the next record after it; a pass that finds a
// record without its result confirms the side effect live through the
// topology facade (PlannedFailoverTopology), which owns every MySQL
// statement and the claim. The engine never reads the informer cache.
//
// The in-process memo only remembers what this process sent and saw
// (fence and promotion attempts, terminated runs) and caches the durable
// deadline annotations; it never decides on its own what status says.
package controller

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"strings"
	"sync"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/uuid"
	"sigs.k8s.io/controller-runtime/pkg/log"

	v1alpha1 "github.com/shipstream/bloodraven/api/v1alpha1"
	"github.com/shipstream/bloodraven/internal/clock"
)

const (
	// plannedFailoverAdmissionTimeout bounds a run's time in Admitting,
	// from entry, persisted in admission.deadline.
	plannedFailoverAdmissionTimeout = 2 * time.Minute
	// plannedFailoverStatusStallDeadline is how long status writes may fail
	// in a row, from the durable stall-since time, before the run ends with
	// a terminal marker.
	plannedFailoverStatusStallDeadline = 2 * time.Minute
	// plannedFailoverManagerAbsentDeadline is how long a run holding the
	// claim waits for a topology manager, from the durable absent-since
	// time, before it ends without MySQL access.
	plannedFailoverManagerAbsentDeadline = 5 * time.Minute
	// plannedFailoverManagerAbsentWarnAfter is when the PlannedFailoverStalled
	// Warning is emitted for an absent manager.
	plannedFailoverManagerAbsentWarnAfter = 30 * time.Second
	// plannedFailoverFenceDeadline bounds fence retries from fence.intendedAt.
	plannedFailoverFenceDeadline = 30 * time.Second
	// plannedFailoverPromotionDeadline bounds Promoting from promotion.intendedAt.
	plannedFailoverPromotionDeadline = 2 * time.Minute
	// plannedFailoverRollbackDeadline bounds RollingBack from rollback.intendedAt.
	plannedFailoverRollbackDeadline = 30 * time.Second
	// plannedFailoverLossWindow is how long consecutive failed probes of the
	// source or the target are tolerated before the run treats it as lost.
	plannedFailoverLossWindow = defaultActivePrimaryLossWindow
	// plannedFailoverHoldPendingInterval paces the PlannedFailoverHoldPending
	// Warning while an automation hold stands.
	plannedFailoverHoldPendingInterval = 10 * time.Minute
	// plannedFailoverMarkerRetryInterval paces the retries of a marked
	// run's terminal status record.
	plannedFailoverMarkerRetryInterval = 10 * time.Second
	// plannedFailoverWatermarkAnnotationLimit is the out-of-band watermark's
	// size guard: a run whose fenced set is larger has no durable watermark
	// outside status, so the stall release is not available to it.
	plannedFailoverWatermarkAnnotationLimit = 64 * 1024

	// Pass wall-time bounds (§2.4 step 8), measured from before the pass's
	// uncached load: the load and every phase step share one budget.
	// PromotingDragonfly has its own bound (plannedDragonflyPassBound:
	// gate wait + maxSyncWait + 12s + one ordinary budget, 62s by default).
	// Settlement of an ambiguous write runs detached and is bounded
	// separately (plannedStoreSettleAttempts × plannedStoreSettleAttemptTimeout,
	// 30s per ambiguous write), so the real maximum of one pass is its
	// bound plus 30s for each ambiguous write it settles: in practice
	// 45s (ordinary), 60s (Promoting) and 75s (confirmed tail).
	plannedPassOrdinary      = 15 * time.Second
	plannedPassPromoting     = 30 * time.Second
	plannedPassConfirmedTail = 45 * time.Second

	// plannedWriteConflictRetries is how often a conflicting status write is
	// reloaded and recomputed in one pass.
	plannedWriteConflictRetries = 3

	// Requeue intervals.
	plannedRequeueNext    = 200 * time.Millisecond // a durable transition; the next step is due now
	plannedRequeuePoll    = 1 * time.Second        // waiting, polling, conflict
	plannedRequeueBackoff = 2 * time.Second        // manager absent, unknown write, transient facade error
)

// Engine yield points (§15.1). Production leaves SimHooks.Yield nil.
const (
	plannedYieldAfterLoad   = YieldEngineLoadForSim
	plannedYieldAfterWrite  = YieldEngineWriteForSim
	plannedYieldAfterSettle = YieldEngineSettleForSim
)

// plannedAdmissionGuard wraps an admission pass with the deployment
// lease's admission reservation (BeginPlanned/EndPlanned). hold non-nil:
// a deployment failover-hold is standing.
type plannedAdmissionGuard interface {
	BeginPlanned(ctx context.Context, fg *v1alpha1.MysqlFailoverGroup) (hold *DeploymentLeaseView, release func(), err error)
	// Now is the clock the lease holds are evaluated on, so a deferral
	// wakes exactly when the hold that caused it expires.
	Now() time.Time
}

// PlannedFailoverEngine drives planned-failover runs through the store and
// the topology facade, one bounded pass per Step call.
type PlannedFailoverEngine struct {
	store PlannedFailoverStore
	topo  PlannedFailoverTopology
	sink  PlannedFailoverSink
	clk   clock.Clock
	hooks SimHooks

	// dragonfly runs the Dragonfly phases; nil skips them (the run records
	// the takeover as Skipped).
	dragonfly plannedDragonflyOps
	// admission wraps admission passes; nil means no deployment leases.
	admission plannedAdmissionGuard

	// runIDSource mints run IDs; nil is uuid.NewUUID. The simulator sets
	// a deterministic one (SetRunIDSourceForSim).
	runIDSource func() string

	mu     sync.Mutex
	runs   map[string]*plannedRunMemo
	groups map[types.NamespacedName]*plannedGroupMemo
	// terminatedRuns are the runs this process ended (§3.4): a later pass
	// never treats them as live again.
	terminatedRuns map[string]struct{}
}

// pfFenceMemo is what this process knows about its own fence of the run's
// source: the uuid it fenced and whether a SET was proven applied.
type pfFenceMemo struct {
	uuid string
	sent bool
	// attempted is true once this process called Fence for the run.
	attempted bool
	// setSent mirrors the facade's memo (FenceProof.Sent): a SET
	// super_read_only=ON was sent for the run, so the source's fence is the
	// run's own even when no readback proved it applied.
	setSent bool
}

// pfPromoteMemo is what this process knows about its own promotion
// attempts: whether super_read_only=OFF may have been sent, and a
// confirmation the confirmed tail has not yet made durable.
type pfPromoteMemo struct {
	mayHaveWritten bool
	confirmed      bool
	confirmedAt    time.Time
	recorded       bool
	dns            *v1alpha1.PlannedFailoverDNSRecord
	// released: the confirmed tail recorded the promotion, published DNS
	// and released the claim (the DST-4 ruling); the Publishing write that
	// follows is bookkeeping and is retried without the claim.
	released bool
}

// plannedRunMemo is the engine memo for one run.
type plannedRunMemo struct {
	granted bool // this process was granted the claim for the run
	fence   pfFenceMemo
	promote pfPromoteMemo

	warnings []v1alpha1.PlannedFailoverWarning

	sourceLossSince time.Time
	targetLossSince time.Time

	// absentSince and stallSince cache the durable deadline annotations.
	absentSince  time.Time
	absentWarned bool
	stallSince   time.Time

	// pendingTerminal is the terminal record this process computed for the
	// run but has not seen commit (a stalled status, or a marker-ended run).
	pendingTerminal *v1alpha1.PlannedFailoverStatus
	holdSites       []string
	reported        bool // RunTerminal and the terminal event were emitted

	recordWaitLogged bool // "failover record is not durable yet" logged

	confirmedPhases map[v1alpha1.PlannedFailoverPhase]bool // "confirming recorded side effect" logged
	writtenPhase    v1alpha1.PlannedFailoverPhase          // the last phase this process committed
	cannotWriteAt   time.Time
	holdAcked       bool
}

// plannedGroupMemo is the engine memo for one group.
type plannedGroupMemo struct {
	lastRunID     string // the last run whose block this process saw live
	holdPendingAt time.Time
	ackWarned     string
}

// NewPlannedFailoverEngine returns an engine over store, topo and sink.
func NewPlannedFailoverEngine(store PlannedFailoverStore, topo PlannedFailoverTopology, sink PlannedFailoverSink,
	clk clock.Clock, hooks SimHooks) *PlannedFailoverEngine {
	if clk == nil {
		clk = clock.RealClock{}
	}
	return &PlannedFailoverEngine{
		store:          store,
		topo:           topo,
		sink:           sink,
		clk:            clk,
		hooks:          hooks,
		runs:           make(map[string]*plannedRunMemo),
		groups:         make(map[types.NamespacedName]*plannedGroupMemo),
		terminatedRuns: make(map[string]struct{}),
	}
}

// SetSimHooksForSim installs the DST yield hooks.
func (e *PlannedFailoverEngine) SetSimHooksForSim(h SimHooks) { e.hooks = h }

// newRunID mints a run's ID (§2.1).
func (e *PlannedFailoverEngine) newRunID() string {
	if e.runIDSource != nil {
		return e.runIDSource()
	}
	return string(uuid.NewUUID())
}

func (e *PlannedFailoverEngine) yield(point string) {
	if e.hooks.Yield != nil {
		e.hooks.Yield(point)
	}
}

func (e *PlannedFailoverEngine) runMemo(runID string) *plannedRunMemo {
	e.mu.Lock()
	defer e.mu.Unlock()
	m := e.runs[runID]
	if m == nil {
		m = &plannedRunMemo{confirmedPhases: make(map[v1alpha1.PlannedFailoverPhase]bool)}
		e.runs[runID] = m
	}
	return m
}

func (e *PlannedFailoverEngine) groupMemo(nn types.NamespacedName) *plannedGroupMemo {
	e.mu.Lock()
	defer e.mu.Unlock()
	m := e.groups[nn]
	if m == nil {
		m = &plannedGroupMemo{}
		e.groups[nn] = m
	}
	return m
}

func (e *PlannedFailoverEngine) markTerminated(runID string) {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.terminatedRuns[runID] = struct{}{}
}

func (e *PlannedFailoverEngine) terminated(runID string) bool {
	e.mu.Lock()
	defer e.mu.Unlock()
	_, ok := e.terminatedRuns[runID]
	return ok
}

// Step runs one pass for nn and returns when to run the next (0: nothing is
// due). A nil error with a positive requeue is the normal waiting case.
func (e *PlannedFailoverEngine) Step(ctx context.Context, nn types.NamespacedName) (time.Duration, error) {
	// The pass clock starts before the load: a slow successful load leaves
	// the phase only the remaining budget (§2.4 step 8). Context deadlines
	// are wall-clock, so the start is the wall clock too.
	start := time.Now()
	loadCtx, cancel := context.WithTimeout(ctx, plannedPassOrdinary)
	fg, err := e.store.Load(loadCtx, nn)
	cancel()
	if err != nil {
		return plannedRequeueBackoff, fmt.Errorf("planned failover: uncached read: %w", err)
	}
	e.yield(plannedYieldAfterLoad)
	if fg == nil {
		e.releaseZombie(ctx, nn, nil)
		return 0, nil
	}
	return e.pass(ctx, fg, start)
}

// passBound returns the wall-time bound of a pass over pf (§2.4 step 8).
func (e *PlannedFailoverEngine) passBound(pf *v1alpha1.PlannedFailoverStatus) time.Duration {
	if pf != nil && pf.Phase == v1alpha1.PlannedFailoverPhasePromotingDragonfly {
		return plannedDragonflyPassBound(pf)
	}
	if pf == nil || pf.Phase != v1alpha1.PlannedFailoverPhasePromoting {
		return plannedPassOrdinary
	}
	if e.runMemo(pf.RunID).promote.confirmed {
		return plannedPassConfirmedTail
	}
	return plannedPassPromoting
}

func (e *PlannedFailoverEngine) pass(ctx context.Context, fg *v1alpha1.MysqlFailoverGroup, start time.Time) (time.Duration, error) {
	nn := types.NamespacedName{Namespace: fg.Namespace, Name: fg.Name}
	pf, marked := plannedFailoverRun(fg)
	hold := plannedHoldFrom(fg)

	ctx, cancel := context.WithDeadline(ctx, start.Add(e.passBound(pf)))
	defer cancel()

	if ack := strings.TrimSpace(fg.GetAnnotations()[AckPlannedFailoverHoldAnnotation]); ack != "" {
		if d, done := e.ackPass(ctx, fg, hold, ack); done {
			return d, nil
		}
	}
	if pf != nil && marked {
		return e.markedRunPass(ctx, fg, pf)
	}
	if pf != nil && plannedLegacyBlock(pf) {
		return e.refuseLegacy(ctx, fg, pf)
	}
	if hold != nil {
		return e.holdPass(ctx, fg, pf, hold)
	}
	e.releaseZombie(ctx, nn, pf)

	if pf != nil && pf.RunID != "" && !plannedFailoverTerminal(pf) {
		e.groupMemo(nn).lastRunID = pf.RunID
	}

	switch {
	case pf == nil || plannedFailoverTerminal(pf):
		if _, ok := fg.GetAnnotations()[PlannedFailoverAnnotation]; ok {
			return e.admitNew(ctx, fg)
		}
		return 0, nil
	case pf.Phase == v1alpha1.PlannedFailoverPhaseDeferred:
		return e.deferredPass(ctx, fg, pf)
	case pf.Phase == v1alpha1.PlannedFailoverPhaseAdmitting:
		return e.admittingPass(ctx, fg, pf)
	case pf.Phase == v1alpha1.PlannedFailoverPhasePublishing:
		e.consumeStaleRequest(ctx, fg, pf)
		return e.publishingPass(ctx, fg, pf)
	default:
		e.consumeStaleRequest(ctx, fg, pf)
		return e.claimPass(ctx, fg, pf)
	}
}

// ---------- predicates on the block ----------

// plannedEnginePhases are the non-terminal phases this engine writes.
var plannedEnginePhases = []v1alpha1.PlannedFailoverPhase{
	v1alpha1.PlannedFailoverPhaseAdmitting,
	v1alpha1.PlannedFailoverPhaseDeferred,
	v1alpha1.PlannedFailoverPhaseFencing,
	v1alpha1.PlannedFailoverPhaseDraining,
	v1alpha1.PlannedFailoverPhaseWaitingForLag,
	v1alpha1.PlannedFailoverPhaseWaitingForDragonflySync,
	v1alpha1.PlannedFailoverPhasePromotingDragonfly,
	v1alpha1.PlannedFailoverPhasePromoting,
	v1alpha1.PlannedFailoverPhasePublishing,
	v1alpha1.PlannedFailoverPhaseRollingBack,
}

// Phases only an older operator wrote; they are outside the API enum, but a
// block stored before the upgrade can still carry them (§13).
const (
	plannedLegacyPhaseNone       v1alpha1.PlannedFailoverPhase = ""
	plannedLegacyPhasePending    v1alpha1.PlannedFailoverPhase = "Pending"
	plannedLegacyPhaseValidating v1alpha1.PlannedFailoverPhase = "Validating"
	plannedLegacyPhaseResuming   v1alpha1.PlannedFailoverPhase = "Resuming"
)

// plannedLegacyBlock reports a non-terminal block an older operator wrote:
// no runId, or a phase this engine never writes (§13).
func plannedLegacyBlock(pf *v1alpha1.PlannedFailoverStatus) bool {
	if pf == nil || plannedFailoverTerminal(pf) {
		return false
	}
	return pf.RunID == "" || !slices.Contains(plannedEnginePhases, pf.Phase)
}

func plannedRunRef(nn types.NamespacedName, pf *v1alpha1.PlannedFailoverStatus) PlannedRunRef {
	return PlannedRunRef{Group: nn, RunID: pf.RunID, Source: pf.Source, Target: pf.Target}
}

func plannedFenceSet(pf *v1alpha1.PlannedFailoverStatus) string {
	if pf == nil || pf.Fence == nil || pf.Fence.GtidSet == nil {
		return ""
	}
	return *pf.Fence.GtidSet
}

func addPlannedWarnings(dst []v1alpha1.PlannedFailoverWarning, ws ...v1alpha1.PlannedFailoverWarning) []v1alpha1.PlannedFailoverWarning {
	for _, w := range ws {
		if w != "" && !slices.Contains(dst, w) {
			dst = append(dst, w)
		}
	}
	return dst
}

func reasonPtr(r v1alpha1.PlannedFailoverReason) *v1alpha1.PlannedFailoverReason { return &r }

// ---------- writes ----------

// plannedBuild computes the next block (and the status mutation) from a
// freshly loaded base. ok=false means the block moved on and the write is
// abandoned.
type plannedBuild func(base *v1alpha1.MysqlFailoverGroup) (next *v1alpha1.PlannedFailoverStatus,
	mutate func(*v1alpha1.MysqlFailoverGroupStatus), ok bool)

var errPlannedBlockMoved = errors.New("planned failover: the block moved on since this pass loaded it")

// freshRunBuild writes the first record of a new run, refusing when the
// (re)loaded base already carries a live run.
func freshRunBuild(next *v1alpha1.PlannedFailoverStatus) plannedBuild {
	return func(base *v1alpha1.MysqlFailoverGroup) (*v1alpha1.PlannedFailoverStatus, func(*v1alpha1.MysqlFailoverGroupStatus), bool) {
		if cur, marked := plannedFailoverRun(base); cur != nil && !marked && !plannedFailoverTerminal(cur) && cur.RunID != next.RunID {
			return nil, nil, false
		}
		return next, nil, true
	}
}

// commit writes build(base) conditioned on base, reloading and recomputing
// on a conflict up to plannedWriteConflictRetries times. claimHeld says
// whether the run holds a claim, so a non-commit counts toward the status
// stall (§6.4). It returns the outcome and the next record it wrote.
func (e *PlannedFailoverEngine) commit(ctx context.Context, fg *v1alpha1.MysqlFailoverGroup, claimHeld bool,
	build plannedBuild) (WriteOutcome, *v1alpha1.PlannedFailoverStatus, error) {
	base := fg
	nn := types.NamespacedName{Namespace: fg.Namespace, Name: fg.Name}
	var lastErr error
	for attempt := 0; attempt < plannedWriteConflictRetries; attempt++ {
		next, mutate, ok := build(base)
		if !ok {
			return WriteConflict, nil, errPlannedBlockMoved
		}
		out, err := e.store.Write(ctx, base, *next, mutate)
		e.yield(plannedYieldAfterWrite)
		switch out {
		case WriteCommitted:
			e.afterCommit(ctx, nn, base, next)
			return WriteCommitted, next, nil
		case WriteConflict:
			lastErr = err
			reloaded, lerr := e.store.Load(ctx, nn)
			if lerr != nil {
				return WriteConflict, next, lerr
			}
			if reloaded == nil {
				return WriteGone, next, errors.New("planned failover: the MysqlFailoverGroup is gone")
			}
			base = reloaded
			continue
		case WriteUnknown:
			e.yield(plannedYieldAfterSettle)
			if claimHeld {
				log.FromContext(ctx).Error(err, "planned failover: cannot settle status write; holding",
					"fg", nn.String(), "runId", next.RunID)
			}
		}
		if claimHeld && (out == WriteUnknown || out == WriteNotCommitted) {
			e.noteStall(ctx, base, next.RunID)
		}
		return out, next, err
	}
	return WriteConflict, nil, lastErr
}

// afterCommit clears the stall deadline once a status write of the run
// commits (§6.4).
func (e *PlannedFailoverEngine) afterCommit(ctx context.Context, nn types.NamespacedName, base *v1alpha1.MysqlFailoverGroup,
	next *v1alpha1.PlannedFailoverStatus) {
	m := e.runMemo(next.RunID)
	m.writtenPhase = next.Phase
	m.stallSince = time.Time{}
	if _, ok := parseDeadlineStart(base.GetAnnotations()[PlannedFailoverStallSinceAnnotation], next.RunID); !ok {
		return
	}
	if fresh, err := e.store.Load(ctx, nn); err == nil && fresh != nil {
		if out, err := e.store.ClearDeadlineStart(ctx, fresh, DeadlineStatusStall, next.RunID); out != WriteCommitted {
			log.FromContext(ctx).Info("planned failover: could not clear the stall-since annotation; the next commit retries",
				"fg", nn.String(), "runId", next.RunID, "error", errString(err))
		}
	}
}

// noteStall records the durable stall start for the run (never moving it
// forward).
func (e *PlannedFailoverEngine) noteStall(ctx context.Context, base *v1alpha1.MysqlFailoverGroup, runID string) {
	m := e.runMemo(runID)
	if start, ok := parseDeadlineStart(base.GetAnnotations()[PlannedFailoverStallSinceAnnotation], runID); ok {
		m.stallSince = start
		return
	}
	nn := types.NamespacedName{Namespace: base.Namespace, Name: base.Name}
	fresh, err := e.store.Load(ctx, nn)
	if err != nil || fresh == nil {
		return
	}
	start, out, err := e.store.MarkDeadlineStart(ctx, fresh, DeadlineStatusStall, runID, e.clk.Now())
	if out == WriteCommitted {
		m.stallSince = start
		return
	}
	log.FromContext(ctx).Info("planned failover: could not record the stall-since annotation",
		"fg", nn.String(), "runId", runID, "outcome", out.String(), "error", errString(err))
}

func errString(err error) string {
	if err == nil {
		return ""
	}
	return err.Error()
}

// reload re-reads the object after a commit, for the metadata writes that
// follow it in the same pass.
func (e *PlannedFailoverEngine) reload(ctx context.Context, fg *v1alpha1.MysqlFailoverGroup) *v1alpha1.MysqlFailoverGroup {
	fresh, err := e.store.Load(ctx, types.NamespacedName{Namespace: fg.Namespace, Name: fg.Name})
	if err != nil || fresh == nil {
		return nil
	}
	return fresh
}

// sameRun returns base's block when it is still the run of pf in one of
// phases (any phase when none is given).
func sameRun(base *v1alpha1.MysqlFailoverGroup, pf *v1alpha1.PlannedFailoverStatus,
	phases ...v1alpha1.PlannedFailoverPhase) (*v1alpha1.PlannedFailoverStatus, bool) {
	cur, marked := plannedFailoverRun(base)
	if cur == nil || marked || cur.RunID != pf.RunID {
		return nil, false
	}
	if len(phases) > 0 && !slices.Contains(phases, cur.Phase) {
		return nil, false
	}
	return cur, true
}

// advance writes the run's next phase, built by edit on a copy of the
// current block (revision bumped). It returns the outcome.
func (e *PlannedFailoverEngine) advance(ctx context.Context, fg *v1alpha1.MysqlFailoverGroup, pf *v1alpha1.PlannedFailoverStatus,
	edit func(next *v1alpha1.PlannedFailoverStatus)) (WriteOutcome, error) {
	from := pf.Phase
	claimHeld := plannedFailoverOwnsSitesPhase(from) || (from == v1alpha1.PlannedFailoverPhaseAdmitting && e.runMemo(pf.RunID).granted)
	out, _, err := e.commit(ctx, fg, claimHeld, func(base *v1alpha1.MysqlFailoverGroup) (*v1alpha1.PlannedFailoverStatus,
		func(*v1alpha1.MysqlFailoverGroupStatus), bool) {
		cur, ok := sameRun(base, pf, from)
		if !ok {
			return nil, nil, false
		}
		next := cur.DeepCopy()
		next.Revision = cur.Revision + 1
		edit(next)
		return next, nil, true
	})
	return out, err
}

// plannedFailoverOwnsSitesPhase reports whether a run in phase holds the
// claim durably.
func plannedFailoverOwnsSitesPhase(p v1alpha1.PlannedFailoverPhase) bool {
	switch p {
	case v1alpha1.PlannedFailoverPhaseFencing,
		v1alpha1.PlannedFailoverPhaseDraining,
		v1alpha1.PlannedFailoverPhaseWaitingForLag,
		v1alpha1.PlannedFailoverPhaseWaitingForDragonflySync,
		v1alpha1.PlannedFailoverPhasePromotingDragonfly,
		v1alpha1.PlannedFailoverPhasePromoting,
		v1alpha1.PlannedFailoverPhaseRollingBack:
		return true
	}
	return false
}

// requeueFor maps a non-committed write outcome to a requeue.
func requeueFor(out WriteOutcome) time.Duration {
	switch out {
	case WriteCommitted:
		return plannedRequeueNext
	case WriteGone:
		return 0
	case WriteUnknown:
		return plannedRequeueBackoff
	}
	return plannedRequeuePoll
}

// ---------- admission (owns nothing) ----------

// newRunBlock is the first record of a run.
func (e *PlannedFailoverEngine) newRunBlock(fg *v1alpha1.MysqlFailoverGroup, req PlannedFailoverRequest,
	phase v1alpha1.PlannedFailoverPhase, msg string) *v1alpha1.PlannedFailoverStatus {
	now := metav1.NewTime(e.clk.Now())
	settings := plannedFailoverSettingsFor(fg, req)
	return &v1alpha1.PlannedFailoverStatus{
		RunID:     e.newRunID(),
		Revision:  1,
		Phase:     phase,
		Target:    req.Site,
		Settings:  settings,
		StartTime: now,
		Message:   msg,
	}
}

// admitNew handles a request annotation with no live run.
func (e *PlannedFailoverEngine) admitNew(ctx context.Context, fg *v1alpha1.MysqlFailoverGroup) (time.Duration, error) {
	nn := types.NamespacedName{Namespace: fg.Namespace, Name: fg.Name}
	raw := fg.GetAnnotations()[PlannedFailoverAnnotation]
	req, perr := parsePlannedFailoverAnnotation(raw)
	if perr != nil {
		e.sink.Event(fg, corev1.EventTypeWarning, "PlannedFailoverRejected", perr.Error())
		e.sink.RunTerminal(nn, &v1alpha1.PlannedFailoverStatus{Result: &v1alpha1.PlannedFailoverResult{
			Outcome: v1alpha1.PlannedFailoverOutcomeNotStarted, Reason: v1alpha1.PlannedFailoverReasonInvalidAnnotation}})
		e.consumeRequest(ctx, fg, "invalid")
		return 0, nil
	}

	hold, release, err := e.beginAdmission(ctx, fg)
	if err != nil {
		// Owns nothing: the request stays; a later pass retries.
		e.noteAdmissionBlocked(ctx, fg, "", err)
		return plannedRequeuePoll, nil
	}
	defer release()
	if hold != nil {
		next := e.newRunBlock(fg, req, v1alpha1.PlannedFailoverPhaseDeferred, "")
		return e.writeDeferredHold(ctx, fg, nil, next, hold)
	}

	now := e.clk.Now()
	result, reason, verr := validatePlannedFailoverRequest(fg, req, now, false)
	switch result {
	case PlannedFailoverSkip:
		e.sink.Event(fg, corev1.EventTypeNormal, "PlannedFailoverSkipped",
			fmt.Sprintf("planned-failover: site %q is already the active primary; nothing to do", req.Site))
		e.consumeRequest(ctx, fg, "idempotent")
		return 0, nil
	case PlannedFailoverReject:
		if reason == v1alpha1.PlannedFailoverReasonCooldownActive && effectiveOnCooldown(fg) == PlannedFailoverOnCooldownDefer {
			next := e.newRunBlock(fg, req, v1alpha1.PlannedFailoverPhaseDeferred, "")
			return e.writeDeferredCooldown(ctx, fg, nil, next, verr.Error(), true)
		}
		next := e.newRunBlock(fg, req, v1alpha1.PlannedFailoverPhaseFailed, verr.Error())
		next.Result = &v1alpha1.PlannedFailoverResult{Outcome: v1alpha1.PlannedFailoverOutcomeNotStarted, Reason: reason,
			CompletionTime: metav1.NewTime(now)}
		out, _, _ := e.commit(ctx, fg, false, freshRunBuild(next))
		if out != WriteCommitted {
			return requeueFor(out), nil
		}
		e.reportTerminal(nn, fg, next, "PlannedFailoverRejected")
		e.consumeRequest(ctx, e.reload(ctx, fg), "rejected")
		return 0, nil
	}

	next := e.newRunBlock(fg, req, v1alpha1.PlannedFailoverPhaseAdmitting,
		fmt.Sprintf("admin requested graceful switchover to %q", req.Site))
	deadline := metav1.NewTime(now.Add(plannedFailoverAdmissionTimeout))
	next.Admission = &v1alpha1.PlannedFailoverAdmission{Deadline: &deadline}
	out, _, _ := e.commit(ctx, fg, false, freshRunBuild(next))
	if out != WriteCommitted {
		return requeueFor(out), nil
	}
	e.groupMemo(nn).lastRunID = next.RunID
	e.consumeRequest(ctx, e.reload(ctx, fg), "accepted")
	e.sink.Event(fg, corev1.EventTypeNormal, "PlannedFailoverStarted",
		fmt.Sprintf("planned failover from %q to %q accepted (maxLagWait=%s, drainTimeout=%s)",
			fg.Status.ActiveSite, req.Site, next.Settings.MaxLagWait.Duration, next.Settings.DrainTimeout.Duration))
	return plannedRequeueNext, nil
}

// plannedAdmissionWaitReason maps a deployment-lease admission refusal to
// the wait reason it is recorded under.
func plannedAdmissionWaitReason(err error) (v1alpha1.PlannedFailoverWaitReason, string) {
	var le *DeploymentLeaseError
	if errors.As(err, &le) {
		switch le.Reason {
		case "TopologyPersistencePending":
			return v1alpha1.PlannedFailoverWaitAuthorityUnsettled, "the last topology change is not yet persisted"
		case "ConcurrentOperation":
			return v1alpha1.PlannedFailoverWaitUpdateInFlight, "another deployment operation holds the sites"
		}
		return v1alpha1.PlannedFailoverWaitClaimHeld, fmt.Sprintf("deployment admission refused (%s)", le.Reason)
	}
	return v1alpha1.PlannedFailoverWaitAuthorityUnsettled, fmt.Sprintf("deployment admission could not be checked: %v", err)
}

// noteAdmissionBlocked logs a deployment-lease refusal for a request with
// no Admitting record to carry it.
func (e *PlannedFailoverEngine) noteAdmissionBlocked(ctx context.Context, fg *v1alpha1.MysqlFailoverGroup, runID string, err error) {
	reason, msg := plannedAdmissionWaitReason(err)
	log.FromContext(ctx).Info("planned failover: admission waiting", "fg", fg.Namespace+"/"+fg.Name, "runId", runID,
		"waitReason", string(reason), "detail", msg)
}

// beginAdmission takes the deployment admission reservation.
func (e *PlannedFailoverEngine) beginAdmission(ctx context.Context, fg *v1alpha1.MysqlFailoverGroup) (*DeploymentLeaseView, func(), error) {
	if e.admission == nil {
		return nil, func() {}, nil
	}
	hold, release, err := e.admission.BeginPlanned(ctx, fg)
	if release == nil {
		release = func() {}
	}
	return hold, release, err
}

// consumeRequest removes the request annotation, logging a failure (a
// later pass retries: an unconsumed request of a live run is recognised).
func (e *PlannedFailoverEngine) consumeRequest(ctx context.Context, fg *v1alpha1.MysqlFailoverGroup, why string) {
	if fg == nil {
		return
	}
	if out, err := e.store.ConsumeRequest(ctx, fg); out != WriteCommitted {
		log.FromContext(ctx).Info("planned failover: could not remove the request annotation; a later pass retries",
			"fg", fg.Namespace+"/"+fg.Name, "why", why, "outcome", out.String(), "error", errString(err))
	}
}

// consumeStaleRequest removes a request annotation found while a run is
// past admission. The run's own request (same target) is consumed
// silently; any other is refused with a Warning.
func (e *PlannedFailoverEngine) consumeStaleRequest(ctx context.Context, fg *v1alpha1.MysqlFailoverGroup, pf *v1alpha1.PlannedFailoverStatus) {
	raw, ok := fg.GetAnnotations()[PlannedFailoverAnnotation]
	if !ok {
		return
	}
	if req, err := parsePlannedFailoverAnnotation(raw); err != nil || req.Site != pf.Target || pf.Phase != v1alpha1.PlannedFailoverPhaseAdmitting {
		e.sink.Event(fg, corev1.EventTypeWarning, "PlannedFailoverRejected",
			fmt.Sprintf("planned-failover annotation ignored: a previous planned failover is still running (phase=%q)", pf.Phase))
	}
	e.consumeRequest(ctx, fg, "stale")
}

// writeDeferredCooldown writes (or keeps) Deferred{CooldownActive}.
func (e *PlannedFailoverEngine) writeDeferredCooldown(ctx context.Context, fg *v1alpha1.MysqlFailoverGroup,
	cur, fresh *v1alpha1.PlannedFailoverStatus, rejectMsg string, initial bool) (time.Duration, error) {
	nn := types.NamespacedName{Namespace: fg.Namespace, Name: fg.Name}
	now := e.clk.Now()
	retry := cooldownRetryAfter(fg, now)
	msg := rejectMsg + " — annotation retained for automatic retry"
	out, _ := e.writeDeferred(ctx, fg, cur, fresh, v1alpha1.PlannedFailoverDeferCooldownActive, retry, msg)
	if out != WriteCommitted {
		return requeueFor(out), nil
	}
	if initial {
		e.sink.Deferred(nn, v1alpha1.PlannedFailoverDeferCooldownActive)
		target := ""
		if fresh != nil {
			target = fresh.Target
		} else if cur != nil {
			target = cur.Target
		}
		e.sink.Event(fg, corev1.EventTypeNormal, "PlannedFailoverDeferred",
			fmt.Sprintf("planned failover to %q deferred: cooldown active, retrying at %s", target, retry.UTC().Format(time.RFC3339)))
	}
	if retry.IsZero() {
		return 30 * time.Second, nil
	}
	return min(max(retry.Sub(now)+time.Second, time.Second), 10*time.Minute), nil
}

// writeDeferredHold writes (or keeps) Deferred{DeploymentHold}.
func (e *PlannedFailoverEngine) writeDeferredHold(ctx context.Context, fg *v1alpha1.MysqlFailoverGroup,
	cur, fresh *v1alpha1.PlannedFailoverStatus, hold *DeploymentLeaseView) (time.Duration, error) {
	nn := types.NamespacedName{Namespace: fg.Namespace, Name: fg.Name}
	now := e.clk.Now()
	if e.admission != nil {
		now = e.admission.Now()
	}
	msg := fmt.Sprintf("planned-failover held by operation %s instance %s until %s", hold.OperationID, hold.Instance,
		hold.ExpiresAt.UTC().Format(time.RFC3339))
	initial := cur == nil || cur.Admission == nil || cur.Admission.DeferReason != v1alpha1.PlannedFailoverDeferDeploymentHold
	out, _ := e.writeDeferred(ctx, fg, cur, fresh, v1alpha1.PlannedFailoverDeferDeploymentHold, hold.ExpiresAt, msg)
	if out != WriteCommitted {
		return requeueFor(out), nil
	}
	if initial {
		e.sink.Deferred(nn, v1alpha1.PlannedFailoverDeferDeploymentHold)
		e.sink.Event(fg, corev1.EventTypeNormal, "PlannedFailoverDeferred", msg)
	}
	return max(time.Second, hold.ExpiresAt.Sub(now)+time.Nanosecond), nil
}

// writeDeferred writes a Deferred record: fresh (a new run) or an update of
// cur. An unchanged record is not rewritten.
func (e *PlannedFailoverEngine) writeDeferred(ctx context.Context, fg *v1alpha1.MysqlFailoverGroup,
	cur, fresh *v1alpha1.PlannedFailoverStatus, reason v1alpha1.PlannedFailoverDeferReason, retry time.Time, msg string) (WriteOutcome, error) {
	var retryMeta *metav1.Time
	if !retry.IsZero() {
		t := metav1.NewTime(retry.UTC().Truncate(time.Second))
		retryMeta = &t
	}
	if fresh != nil {
		fresh.Phase = v1alpha1.PlannedFailoverPhaseDeferred
		fresh.Admission = &v1alpha1.PlannedFailoverAdmission{DeferReason: reason, RetryAfter: retryMeta}
		fresh.Message = msg
		out, _, err := e.commit(ctx, fg, false, freshRunBuild(fresh))
		if out == WriteCommitted {
			e.groupMemo(types.NamespacedName{Namespace: fg.Namespace, Name: fg.Name}).lastRunID = fresh.RunID
		}
		return out, err
	}
	if a := cur.Admission; a != nil && a.DeferReason == reason && cur.Message == msg &&
		((a.RetryAfter == nil && retryMeta == nil) || (a.RetryAfter != nil && retryMeta != nil && a.RetryAfter.Equal(retryMeta))) {
		return WriteCommitted, nil
	}
	return e.advance(ctx, fg, cur, func(next *v1alpha1.PlannedFailoverStatus) {
		next.Admission = &v1alpha1.PlannedFailoverAdmission{DeferReason: reason, RetryAfter: retryMeta}
		next.Message = msg
	})
}

// endNotStarted ends a run that owns nothing: Failed{NotStarted, reason}.
func (e *PlannedFailoverEngine) endNotStarted(ctx context.Context, fg *v1alpha1.MysqlFailoverGroup, pf *v1alpha1.PlannedFailoverStatus,
	reason v1alpha1.PlannedFailoverReason, msg string, why ClaimReleaseReason, consume bool) (time.Duration, error) {
	d, err := e.finish(ctx, fg, pf, plannedEnd{outcome: v1alpha1.PlannedFailoverOutcomeNotStarted, reason: reason, msg: msg,
		why: why, eventReason: "PlannedFailoverRejected"})
	if consume && d == 0 && err == nil {
		e.consumeRequest(ctx, e.reload(ctx, fg), "rejected")
	}
	return d, err
}

// deferredPass re-validates a Deferred run.
func (e *PlannedFailoverEngine) deferredPass(ctx context.Context, fg *v1alpha1.MysqlFailoverGroup, pf *v1alpha1.PlannedFailoverStatus) (time.Duration, error) {
	raw, ok := fg.GetAnnotations()[PlannedFailoverAnnotation]
	if !ok {
		return e.endNotStarted(ctx, fg, pf, v1alpha1.PlannedFailoverReasonCancelled,
			"planned-failover annotation removed while deferred; run cancelled", ReleaseTerminal, false)
	}
	req, perr := parsePlannedFailoverAnnotation(raw)
	if perr != nil {
		return e.endNotStarted(ctx, fg, pf, v1alpha1.PlannedFailoverReasonInvalidAnnotation, perr.Error(), ReleaseTerminal, true)
	}
	hold, release, err := e.beginAdmission(ctx, fg)
	if err != nil {
		e.noteAdmissionBlocked(ctx, fg, pf.RunID, err)
		return plannedRequeuePoll, nil
	}
	defer release()
	if hold != nil {
		return e.writeDeferredHold(ctx, fg, pf, nil, hold)
	}
	now := e.clk.Now()
	result, reason, verr := validatePlannedFailoverRequest(fg, req, now, true)
	switch result {
	case PlannedFailoverAccept:
		settings := plannedFailoverSettingsFor(fg, req)
		out, _ := e.advance(ctx, fg, pf, func(next *v1alpha1.PlannedFailoverStatus) {
			next.Phase = v1alpha1.PlannedFailoverPhaseAdmitting
			next.Target = req.Site
			next.Settings = settings
			deadline := metav1.NewTime(now.Add(plannedFailoverAdmissionTimeout))
			next.Admission = &v1alpha1.PlannedFailoverAdmission{Deadline: &deadline}
			next.Message = fmt.Sprintf("proceeding with deferred planned failover to %q", req.Site)
		})
		if out != WriteCommitted {
			return requeueFor(out), nil
		}
		e.consumeRequest(ctx, e.reload(ctx, fg), "resumed")
		e.sink.Event(fg, corev1.EventTypeNormal, "PlannedFailoverStarted",
			fmt.Sprintf("deferred planned failover to %q resumed (maxLagWait=%s, drainTimeout=%s)",
				req.Site, settings.MaxLagWait.Duration, settings.DrainTimeout.Duration))
		return plannedRequeueNext, nil
	case PlannedFailoverSkip:
		e.sink.Event(fg, corev1.EventTypeNormal, "PlannedFailoverSkipped",
			fmt.Sprintf("deferred planned failover: site %q is already the active primary; clearing annotation", req.Site))
		return e.endNotStarted(ctx, fg, pf, v1alpha1.PlannedFailoverReasonAlreadyActive,
			fmt.Sprintf("deferred planned failover to %q resolved as no-op: target is already active", req.Site), ReleaseTerminal, true)
	}
	if reason == v1alpha1.PlannedFailoverReasonCooldownActive && effectiveOnCooldown(fg) == PlannedFailoverOnCooldownDefer {
		return e.writeDeferredCooldown(ctx, fg, pf, nil, verr.Error(), false)
	}
	return e.endNotStarted(ctx, fg, pf, reason, verr.Error(), ReleaseTerminal, true)
}

// admittingPass validates and asks for the claim (§3.2, §6.1).
func (e *PlannedFailoverEngine) admittingPass(ctx context.Context, fg *v1alpha1.MysqlFailoverGroup, pf *v1alpha1.PlannedFailoverStatus) (time.Duration, error) {
	nn := types.NamespacedName{Namespace: fg.Namespace, Name: fg.Name}
	m := e.runMemo(pf.RunID)
	now := e.clk.Now()
	e.consumeStaleRequest(ctx, fg, pf)

	if pf.Admission != nil && pf.Admission.Deadline != nil && !now.Before(pf.Admission.Deadline.Time) {
		last := v1alpha1.PlannedFailoverWaitReason("none")
		if pf.Admission.WaitReason != "" {
			last = pf.Admission.WaitReason
		}
		return e.endNotStarted(ctx, fg, pf, v1alpha1.PlannedFailoverReasonAdmissionTimeout,
			fmt.Sprintf("planned failover to %q was not admitted within %s (last wait reason: %s)", pf.Target,
				plannedFailoverAdmissionTimeout, last), ReleaseAdmissionTimeout, false)
	}

	hold, release, err := e.beginAdmission(ctx, fg)
	if err != nil {
		// A deployment-lease refusal is a wait like any other: recorded as
		// the admission's wait reason, bounded by the admission deadline,
		// never a reconcile error with controller backoff.
		reason, msg := plannedAdmissionWaitReason(err)
		return e.noteWait(ctx, fg, pf, reason, msg)
	}
	defer release()
	if hold != nil {
		return e.endNotStarted(ctx, fg, pf, v1alpha1.PlannedFailoverReasonConcurrentOperation,
			fmt.Sprintf("planned-failover held by operation %s instance %s until %s; re-annotate once it is released",
				hold.OperationID, hold.Instance, hold.ExpiresAt.UTC().Format(time.RFC3339)), ReleaseTerminal, false)
	}
	req := PlannedFailoverRequest{Site: pf.Target}
	result, reason, verr := validatePlannedFailoverRequest(fg, req, now, true)
	switch result {
	case PlannedFailoverSkip:
		return e.endNotStarted(ctx, fg, pf, v1alpha1.PlannedFailoverReasonAlreadyActive,
			fmt.Sprintf("planned-failover: target %q is already the active primary; nothing to do", pf.Target), ReleaseTerminal, false)
	case PlannedFailoverReject:
		return e.endNotStarted(ctx, fg, pf, reason, verr.Error(), ReleaseTerminal, false)
	}
	if d := fg.Status.Dragonfly; d != nil && dragonflyUpgradeInFlight(d.Upgrade) {
		return e.noteWait(ctx, fg, pf, v1alpha1.PlannedFailoverWaitDragonflyUpgradeInFlight,
			"a Dragonfly snapshot upgrade is in flight")
	}

	baseline, berr := durableFailoverRecord(fg, now)
	if berr != nil && baseline.LastFailover.IsZero() && baseline.LastFailoverTarget == "" {
		return e.endNotStarted(ctx, fg, pf, v1alpha1.PlannedFailoverReasonInvalidFailoverState,
			fmt.Sprintf("planned-failover: durable failover record is invalid: %v", berr), ReleaseTerminal, false)
	}
	dec, err := e.topo.ClaimPlannedFailover(ctx, nn, PlannedClaimRequest{RunID: pf.RunID, Target: pf.Target, DurableBaseline: baseline})
	switch {
	case errors.Is(err, errManagerAbsent):
		return e.noteWait(ctx, fg, pf, v1alpha1.PlannedFailoverWaitManagerAbsent, "no topology manager is registered for the group")
	case errors.Is(err, errLeaderLost):
		return plannedRequeueBackoff, nil
	case errors.Is(err, errGateBusy):
		return e.noteWait(ctx, fg, pf, v1alpha1.PlannedFailoverWaitRecoveryInFlight, "a follower mutation is in flight")
	case err != nil:
		log.FromContext(ctx).Info("planned failover: claim attempt failed; retrying", "fg", nn.String(), "runId", pf.RunID, "error", err.Error())
		return plannedRequeuePoll, nil
	}
	switch dec.Kind {
	case ClaimWait:
		return e.noteWait(ctx, fg, pf, dec.WaitReason, dec.Message)
	case ClaimReject:
		msg := dec.Message
		if msg == "" {
			msg = fmt.Sprintf("planned failover to %q refused: %s", pf.Target, dec.RejectReason)
		}
		return e.endNotStarted(ctx, fg, pf, dec.RejectReason, msg, ReleaseTerminal, false)
	}

	// Granted: the slot holds a provisional grant until Fencing commits.
	m.granted = true
	m.fence = pfFenceMemo{}
	claimedAt := metav1.NewTime(e.clk.Now())
	out, _ := e.advance(ctx, fg, pf, func(next *v1alpha1.PlannedFailoverStatus) {
		next.Phase = v1alpha1.PlannedFailoverPhaseFencing
		next.Source = dec.Source
		next.ClaimedAt = &claimedAt
		next.ClaimBaseline = dec.Baseline.toAPI()
		next.Fence = &v1alpha1.PlannedFailoverFenceRecord{
			State: v1alpha1.PlannedFailoverFenceIntended, IntendedAt: claimedAt, SourceServerUUID: dec.SourceServerUUID,
		}
		next.Message = fmt.Sprintf("claim granted; fencing source %q", dec.Source)
	})
	if out != WriteCommitted {
		// The provisional write is not durable. A proven non-commit releases
		// the grant at once (§2.4 step 6); an Unknown one keeps it, bounded
		// by the status stall.
		if out == WriteUnknown {
			if d, done := e.maybeStallFinish(ctx, fg, pf); done {
				return d, nil
			}
			return plannedRequeueBackoff, nil
		}
		e.release(ctx, nn, pf.RunID, ReleaseNotCommitted, "")
		m.granted = false
		return requeueFor(out), nil
	}
	log.FromContext(ctx).Info("planned failover: claim granted", "fg", nn.String(), "runId", pf.RunID,
		"source", dec.Source, "target", pf.Target)
	e.sink.Event(fg, corev1.EventTypeNormal, "PlannedFailoverClaimed",
		fmt.Sprintf("planned failover %s claimed %q → %q", pf.RunID, dec.Source, pf.Target))
	return plannedRequeueNext, nil
}

// noteWait records an admission wait reason when it changes, with a
// PlannedFailoverWaiting Event.
func (e *PlannedFailoverEngine) noteWait(ctx context.Context, fg *v1alpha1.MysqlFailoverGroup, pf *v1alpha1.PlannedFailoverStatus,
	reason v1alpha1.PlannedFailoverWaitReason, msg string) (time.Duration, error) {
	if pf.Admission != nil && pf.Admission.WaitReason == reason {
		return plannedRequeuePoll, nil
	}
	out, err := e.advance(ctx, fg, pf, func(next *v1alpha1.PlannedFailoverStatus) {
		if next.Admission == nil {
			next.Admission = &v1alpha1.PlannedFailoverAdmission{}
		} else {
			next.Admission = next.Admission.DeepCopy()
		}
		next.Admission.WaitReason = reason
		next.Message = fmt.Sprintf("waiting for admission (%s)", reason)
		if msg != "" {
			next.Message += ": " + msg
		}
	})
	if out == WriteCommitted {
		e.sink.Event(fg, corev1.EventTypeNormal, "PlannedFailoverWaiting",
			fmt.Sprintf("planned failover to %q waiting: %s %s", pf.Target, reason, msg))
	}
	if out == WriteGone {
		return 0, nil
	}
	_ = err
	return plannedRequeuePoll, nil
}

// ---------- claim phases ----------

// claimPass runs one pass of a run that owns sites.
func (e *PlannedFailoverEngine) claimPass(ctx context.Context, fg *v1alpha1.MysqlFailoverGroup, pf *v1alpha1.PlannedFailoverStatus) (time.Duration, error) {
	nn := types.NamespacedName{Namespace: fg.Namespace, Name: fg.Name}
	ref := plannedRunRef(nn, pf)
	if e.terminated(pf.RunID) {
		// This process ended the run (a terminal marker that has since been
		// removed by hand, for example); it never acts for it again.
		return 0, nil
	}
	if m := e.runMemo(pf.RunID); pf.Phase == v1alpha1.PlannedFailoverPhasePromoting && m.promote.released {
		if held, ok := e.topo.HeldRunClaim(nn); !ok || held != pf.RunID {
			// The claim was released after the promotion was recorded and
			// published: only the Publishing write is left, and it must not
			// hydrate the claim back from the Promoting block.
			return e.writePublishing(ctx, fg, pf)
		}
		// A replacement manager hydrated the claim from the Promoting block
		// before Publishing committed: re-prove and record the promotion
		// into it, then release again.
		m.promote.released, m.promote.recorded = false, false
	}
	dec, err := e.topo.EnsureClaim(ctx, ref, fg)
	switch {
	case errors.Is(err, errManagerAbsent) || (err == nil && dec.Kind == ClaimWait && dec.WaitReason == v1alpha1.PlannedFailoverWaitManagerAbsent):
		return e.managerAbsent(ctx, fg, pf)
	case errors.Is(err, errLeaderLost):
		return plannedRequeueBackoff, nil
	case err != nil:
		log.FromContext(ctx).Info("planned failover: cannot confirm the claim; retrying", "fg", nn.String(), "runId", pf.RunID, "error", err.Error())
		return plannedRequeueBackoff, nil
	case dec.Kind == ClaimWait:
		return plannedRequeuePoll, nil
	case dec.Kind == ClaimReject:
		reason := dec.RejectReason
		if reason == "" {
			reason = v1alpha1.PlannedFailoverReasonAuthorityChanged
		}
		return e.endWithoutMySQL(ctx, fg, pf, reason, "the claim could not be re-established: "+dec.Message, ReleaseTerminal)
	}
	e.clearAbsence(ctx, fg, pf)
	e.noteConfirming(ctx, nn, pf)

	switch pf.Phase {
	case v1alpha1.PlannedFailoverPhaseFencing:
		return e.fencingStep(ctx, fg, pf, ref)
	case v1alpha1.PlannedFailoverPhaseDraining:
		return e.drainingStep(ctx, fg, pf, ref)
	case v1alpha1.PlannedFailoverPhaseWaitingForLag:
		return e.lagStep(ctx, fg, pf, ref)
	case v1alpha1.PlannedFailoverPhaseWaitingForDragonflySync:
		return e.dragonflySyncStep(ctx, fg, pf, ref)
	case v1alpha1.PlannedFailoverPhasePromotingDragonfly:
		return e.dragonflyTakeoverStep(ctx, fg, pf, ref)
	case v1alpha1.PlannedFailoverPhasePromoting:
		return e.promotingStep(ctx, fg, pf, ref)
	case v1alpha1.PlannedFailoverPhaseRollingBack:
		return e.rollbackStep(ctx, fg, pf, ref)
	}
	return 0, nil
}

// noteConfirming logs that a pass found a record this process did not
// write, so its side effect is confirmed live (§2.3).
func (e *PlannedFailoverEngine) noteConfirming(ctx context.Context, nn types.NamespacedName, pf *v1alpha1.PlannedFailoverStatus) {
	m := e.runMemo(pf.RunID)
	if m.writtenPhase == pf.Phase || m.confirmedPhases[pf.Phase] {
		return
	}
	m.confirmedPhases[pf.Phase] = true
	log.FromContext(ctx).Info("planned failover: confirming recorded side effect", "fg", nn.String(), "runId", pf.RunID,
		"phase", string(pf.Phase), "effect", plannedPhaseEffect(pf.Phase))
}

func plannedPhaseEffect(p v1alpha1.PlannedFailoverPhase) string {
	switch p {
	case v1alpha1.PlannedFailoverPhaseFencing:
		return "fence"
	case v1alpha1.PlannedFailoverPhaseDraining:
		return "drain"
	case v1alpha1.PlannedFailoverPhaseWaitingForLag:
		return "lagWait"
	case v1alpha1.PlannedFailoverPhaseWaitingForDragonflySync:
		return "dragonflySync"
	case v1alpha1.PlannedFailoverPhasePromotingDragonfly:
		return "dragonflyTakeover"
	case v1alpha1.PlannedFailoverPhasePromoting:
		return "promotion"
	case v1alpha1.PlannedFailoverPhaseRollingBack:
		return "rollback"
	}
	return string(p)
}

// facadeErr maps a facade sentinel. handled=false: a MySQL or transport
// error the caller counts toward its loss window or deadline.
func (e *PlannedFailoverEngine) facadeErr(ctx context.Context, fg *v1alpha1.MysqlFailoverGroup, pf *v1alpha1.PlannedFailoverStatus,
	err error) (time.Duration, error, bool) {
	nn := types.NamespacedName{Namespace: fg.Namespace, Name: fg.Name}
	switch {
	case err == nil:
		return 0, nil, false
	case errors.Is(err, errManagerAbsent):
		d, err := e.managerAbsent(ctx, fg, pf)
		return d, err, true
	case errors.Is(err, errReproveRequired):
		d, err := e.reprove(ctx, fg, pf)
		return d, err, true
	case errors.Is(err, errLeaderLost):
		return plannedRequeueBackoff, nil, true
	case errors.Is(err, errClaimLost):
		log.FromContext(ctx).Info("planned failover: the claim is not held by this run; waiting", "fg", nn.String(), "runId", pf.RunID)
		return plannedRequeueBackoff, nil, true
	case errors.Is(err, errSiteNotFound):
		d, err := e.endSiteRemoved(ctx, fg, pf)
		return d, err, true
	}
	return 0, nil, false
}

// endSiteRemoved ends a run whose source or target is no longer in the
// topology (removed from spec.sites while the run held the claim). No
// retry can bring the site back into this manager, and the hydrated claim
// would block emergency failover for as long as the run waited, so the run
// ends at once without MySQL access, like a manager absence (§6.3):
// SourceLeftFenced, or PromotedUnconfirmed in Promoting, with nothing
// named.
func (e *PlannedFailoverEngine) endSiteRemoved(ctx context.Context, fg *v1alpha1.MysqlFailoverGroup, pf *v1alpha1.PlannedFailoverStatus) (time.Duration, error) {
	return e.endWithoutMySQL(ctx, fg, pf, v1alpha1.PlannedFailoverReasonUnknownSite,
		fmt.Sprintf("the run's source %q or target %q is no longer in spec.sites; the run ended without MySQL access", pf.Source, pf.Target),
		ReleaseTerminal)
}

// lossWindow tracks consecutive failed probes; lost is true once they
// have lasted plannedFailoverLossWindow.
func (e *PlannedFailoverEngine) lossWindow(since *time.Time, reachable bool) (lost bool) {
	now := e.clk.Now()
	if reachable {
		*since = time.Time{}
		return false
	}
	if since.IsZero() {
		*since = now
	}
	return now.Sub(*since) >= plannedFailoverLossWindow
}

// reprove runs the hydrated claim's re-proof (§4.4) and maps its result.
func (e *PlannedFailoverEngine) reprove(ctx context.Context, fg *v1alpha1.MysqlFailoverGroup, pf *v1alpha1.PlannedFailoverStatus) (time.Duration, error) {
	nn := types.NamespacedName{Namespace: fg.Namespace, Name: fg.Name}
	res, err := e.topo.Reprove(ctx, plannedRunRef(nn, pf), fg)
	if err != nil {
		switch {
		case errors.Is(err, errManagerAbsent):
			return e.managerAbsent(ctx, fg, pf)
		case errors.Is(err, errSiteNotFound):
			return e.endSiteRemoved(ctx, fg, pf)
		case errors.Is(err, errReproveRequired), errors.Is(err, errLeaderLost), errors.Is(err, errClaimLost):
			return plannedRequeueBackoff, nil
		}
		log.FromContext(ctx).Info("planned failover: re-proof failed; retrying", "fg", nn.String(), "runId", pf.RunID, "error", err.Error())
		return plannedRequeueBackoff, nil
	}
	m := e.runMemo(pf.RunID)
	if !res.SourceUnreachable {
		m.sourceLossSince = time.Time{}
	}
	switch {
	case res.SourceUnreachable:
		if e.lossWindow(&m.sourceLossSince, false) {
			return e.toRollingBack(ctx, fg, pf, v1alpha1.PlannedFailoverReasonSourceLost)
		}
		return plannedRequeuePoll, nil
	case res.OK:
		return plannedRequeueNext, nil
	case res.Hold != nil:
		return e.endManualRecovery(ctx, fg, pf, *res.Hold, "the re-proof after hydration found that data may be at risk")
	case res.RollBack != nil:
		return e.toRollingBack(ctx, fg, pf, *res.RollBack)
	case res.HandOff != nil:
		return e.applyPromoteOutcome(ctx, fg, pf, *res.HandOff)
	}
	return plannedRequeueBackoff, nil
}

// fencingStep sends (or confirms) the fence and writes Draining.
func (e *PlannedFailoverEngine) fencingStep(ctx context.Context, fg *v1alpha1.MysqlFailoverGroup, pf *v1alpha1.PlannedFailoverStatus,
	ref PlannedRunRef) (time.Duration, error) {
	m := e.runMemo(pf.RunID)
	if pf.Fence == nil {
		return e.endWithoutMySQL(ctx, fg, pf, v1alpha1.PlannedFailoverReasonInternalError, "Fencing record has no fence intent", ReleaseTerminal)
	}
	var proof FenceProof
	var err error
	if m.fence.sent {
		// Same process, after a lost Draining write: prove the fence again.
		proof, err = e.topo.ProveFence(ctx, ref, *pf.Fence)
	} else {
		m.fence.attempted = true
		proof, err = e.topo.Fence(ctx, ref, pf.Fence.SourceServerUUID)
	}
	if d, ferr, handled := e.facadeErr(ctx, fg, pf, err); handled {
		return d, ferr
	}
	if err != nil {
		proof = FenceProof{Kind: FenceRetry}
	}
	if proof.Sent {
		m.fence.setSent = true
	}
	if proof.Kind == FenceRetry && proof.SourceWritable {
		// ProveFence found the source writable: the SET did not land, so the
		// next attempt fences it again through Fence, which re-proves
		// authority first.
		m.fence.sent = false
	}
	pastDeadline := !e.clk.Now().Before(pf.Fence.IntendedAt.Add(plannedFailoverFenceDeadline))
	if proof.Kind != FenceSourceUnreachable {
		m.sourceLossSince = time.Time{}
	}
	if len(proof.UnreadablePeers) > 0 {
		m.warnings = addPlannedWarnings(m.warnings, v1alpha1.PlannedFailoverWarningUnreachablePeers)
	}
	reason := proof.Reason
	switch proof.Kind {
	case FenceApplied:
		m.fence = pfFenceMemo{uuid: proof.SourceServerUUID, sent: true, attempted: true, setSent: m.fence.setSent}
		now := metav1.NewTime(e.clk.Now())
		gtid := proof.GtidSet
		out, werr := e.advance(ctx, fg, pf, func(next *v1alpha1.PlannedFailoverStatus) {
			next.Phase = v1alpha1.PlannedFailoverPhaseDraining
			next.Fence = next.Fence.DeepCopy()
			next.Fence.State = v1alpha1.PlannedFailoverFenceApplied
			next.Fence.AppliedAt = &now
			next.Fence.GtidSet = &gtid
			next.Drain = &v1alpha1.PlannedFailoverDrainRecord{StartTime: now}
			next.Message = fmt.Sprintf("source %q fenced at %s; draining connections", pf.Source, plannedGtidHint(gtid))
			if len(proof.UnreadablePeers) > 0 {
				next.Message += fmt.Sprintf(" (unreadable peers: %s)", strings.Join(proof.UnreadablePeers, ", "))
			}
		})
		if out == WriteCommitted {
			e.sink.Event(fg, corev1.EventTypeNormal, "PlannedFailoverDraining",
				fmt.Sprintf("fenced source primary %q; draining application connections (timeout %s)",
					pf.Source, plannedSettings(pf).DrainTimeout.Duration))
		}
		return e.afterAdvance(ctx, fg, pf, out, werr)
	case FenceNotSent:
		if reason == "" {
			reason = v1alpha1.PlannedFailoverReasonAuthorityChanged
		}
		m.fence.attempted = false
		outcome := v1alpha1.PlannedFailoverOutcomeSourceLeftFenced
		if proof.SourceWritable {
			outcome = v1alpha1.PlannedFailoverOutcomeRolledBack
		}
		msg := fmt.Sprintf("authority moved before the fence was sent to %q; nothing was sent", pf.Source)
		if m.fence.setSent {
			msg = fmt.Sprintf("authority moved after a fence was sent to %q; the source no longer holds the run's fence", pf.Source)
		}
		return e.finish(ctx, fg, pf, plannedEnd{outcome: outcome, reason: reason, msg: msg})
	case FenceForeignWritable:
		return e.toRollingBack(ctx, fg, pf, v1alpha1.PlannedFailoverReasonForeignWritable)
	case FenceNotAttributable:
		if reason == "" {
			reason = v1alpha1.PlannedFailoverReasonAuthorityChanged
		}
		return e.finish(ctx, fg, pf, plannedEnd{outcome: v1alpha1.PlannedFailoverOutcomeSourceLeftFenced, reason: reason,
			warnings: []v1alpha1.PlannedFailoverWarning{v1alpha1.PlannedFailoverWarningFenceNotAttributable},
			msg:      fmt.Sprintf("source %q is read-only, but the fence is not this run's; check which site is writable, then unfence the source or re-annotate", pf.Source)})
	case FenceSourceUnreachable:
		if e.lossWindow(&m.sourceLossSince, false) {
			return e.toRollingBack(ctx, fg, pf, v1alpha1.PlannedFailoverReasonSourceLost)
		}
		return plannedRequeuePoll, nil
	}
	// FenceRetry, or a transport error.
	if pastDeadline {
		return e.toRollingBack(ctx, fg, pf, v1alpha1.PlannedFailoverReasonFenceFailed)
	}
	return plannedRequeuePoll, nil
}

func plannedGtidHint(gtid string) string {
	if gtid == "" {
		return "an empty gtid set"
	}
	return "gtid " + truncateGtidHint(gtid)
}

// drainingStep kills application connections on the fenced source until it
// is idle or drainTimeout passes.
func (e *PlannedFailoverEngine) drainingStep(ctx context.Context, fg *v1alpha1.MysqlFailoverGroup, pf *v1alpha1.PlannedFailoverStatus,
	ref PlannedRunRef) (time.Duration, error) {
	m := e.runMemo(pf.RunID)
	remaining, err := e.topo.KillAppConnections(ctx, ref)
	if d, ferr, handled := e.facadeErr(ctx, fg, pf, err); handled {
		return d, ferr
	}
	if e.lossWindow(&m.sourceLossSince, err == nil) {
		return e.toRollingBack(ctx, fg, pf, v1alpha1.PlannedFailoverReasonSourceLost)
	}
	start := e.clk.Now()
	if pf.Drain != nil {
		start = pf.Drain.StartTime.Time
	}
	elapsed := e.clk.Now().Sub(start)
	budget := elapsed >= plannedSettings(pf).DrainTimeout.Duration
	if !(err == nil && remaining == 0) && !budget {
		return plannedRequeuePoll, nil
	}
	now := metav1.NewTime(e.clk.Now())
	msg := fmt.Sprintf("source %q drained cleanly in %s", pf.Source, truncateDur(elapsed))
	if err != nil || remaining > 0 {
		msg = fmt.Sprintf("drain budget exhausted after %s with %d connection(s) remaining on %q; proceeding",
			truncateDur(elapsed), remaining, pf.Source)
	}
	out, werr := e.advance(ctx, fg, pf, func(next *v1alpha1.PlannedFailoverStatus) {
		next.Phase = v1alpha1.PlannedFailoverPhaseWaitingForLag
		if next.Drain == nil {
			next.Drain = &v1alpha1.PlannedFailoverDrainRecord{StartTime: now}
		} else {
			next.Drain = next.Drain.DeepCopy()
		}
		next.Drain.CompletedAt = &now
		left := remaining
		next.Drain.RemainingConnections = &left
		next.LagWait = &v1alpha1.PlannedFailoverLagWaitRecord{StartTime: now}
		next.Message = fmt.Sprintf("%s; waiting for target %q to catch up", msg, pf.Target)
	})
	return e.afterAdvance(ctx, fg, pf, out, werr)
}

// afterAdvance maps the outcome of a claim-phase transition, ending the run
// with a terminal marker once status writes have stalled (§6.4).
func (e *PlannedFailoverEngine) afterAdvance(ctx context.Context, fg *v1alpha1.MysqlFailoverGroup, pf *v1alpha1.PlannedFailoverStatus,
	out WriteOutcome, err error) (time.Duration, error) {
	if out == WriteUnknown || out == WriteNotCommitted {
		if d, done := e.maybeStallFinish(ctx, fg, pf); done {
			return d, nil
		}
	}
	if err != nil {
		// The outcome already decided the requeue; the cause is only logged.
		log.FromContext(ctx).Info("planned failover: status write did not commit; retrying", "fg", fg.Namespace+"/"+fg.Name,
			"runId", pf.RunID, "outcome", out.String(), "error", err.Error())
	}
	return requeueFor(out), nil
}

// lagStep waits for the target to apply the fenced set.
func (e *PlannedFailoverEngine) lagStep(ctx context.Context, fg *v1alpha1.MysqlFailoverGroup, pf *v1alpha1.PlannedFailoverStatus,
	ref PlannedRunRef) (time.Duration, error) {
	nn := ref.Group
	m := e.runMemo(pf.RunID)
	probe, err := e.topo.ProbeLag(ctx, ref, plannedFenceSet(pf))
	if d, ferr, handled := e.facadeErr(ctx, fg, pf, err); handled {
		return d, ferr
	}
	if err != nil {
		probe = LagProbe{SourceReachable: true}
	}
	if e.lossWindow(&m.sourceLossSince, probe.SourceReachable) {
		return e.toRollingBack(ctx, fg, pf, v1alpha1.PlannedFailoverReasonSourceLost)
	}
	if e.lossWindow(&m.targetLossSince, probe.TargetReachable) {
		return e.toRollingBack(ctx, fg, pf, v1alpha1.PlannedFailoverReasonTargetLost)
	}
	if probe.Divergent {
		return e.toRollingBack(ctx, fg, pf, v1alpha1.PlannedFailoverReasonDivergentTarget)
	}
	start := e.clk.Now()
	if pf.LagWait != nil {
		start = pf.LagWait.StartTime.Time
	}
	elapsed := e.clk.Now().Sub(start)
	settings := plannedSettings(pf)
	if probe.CaughtUp {
		e.sink.LagWaitObserved(nn, pf.Target, elapsed)
		now := metav1.NewTime(e.clk.Now())
		if settings.Dragonfly != nil {
			out, werr := e.advance(ctx, fg, pf, func(next *v1alpha1.PlannedFailoverStatus) {
				next.Phase = v1alpha1.PlannedFailoverPhaseWaitingForDragonflySync
				next.LagWait = plannedLagCaughtUp(next.LagWait, now)
				next.Dragonfly = &v1alpha1.PlannedFailoverDragonflyStatus{SyncWaitStartTime: &now,
					Message: fmt.Sprintf("waiting for target %q replica to catch up", pf.Target)}
				next.Message = fmt.Sprintf("target %q caught up in %s; waiting for Dragonfly replica", pf.Target, truncateDur(elapsed))
			})
			if out == WriteCommitted {
				e.sink.Event(fg, corev1.EventTypeNormal, "PlannedFailoverLagOK",
					fmt.Sprintf("target %q caught up after %s; proceeding", pf.Target, truncateDur(elapsed)))
			}
			return e.afterAdvance(ctx, fg, pf, out, werr)
		}
		d, perr := e.enterPromoting(ctx, fg, pf, ref, func(next *v1alpha1.PlannedFailoverStatus) {
			next.LagWait = plannedLagCaughtUp(next.LagWait, now)
			next.Message = fmt.Sprintf("target %q caught up in %s; promoting", pf.Target, truncateDur(elapsed))
		})
		if d == plannedRequeueNext {
			e.sink.Event(fg, corev1.EventTypeNormal, "PlannedFailoverLagOK",
				fmt.Sprintf("target %q caught up after %s; proceeding", pf.Target, truncateDur(elapsed)))
		}
		return d, perr
	}
	if elapsed >= settings.MaxLagWait.Duration {
		e.sink.LagWaitObserved(nn, pf.Target, elapsed)
		return e.toRollingBack(ctx, fg, pf, v1alpha1.PlannedFailoverReasonLagTimeout)
	}
	return plannedRequeuePoll, nil
}

func plannedLagCaughtUp(lw *v1alpha1.PlannedFailoverLagWaitRecord, now metav1.Time) *v1alpha1.PlannedFailoverLagWaitRecord {
	if lw == nil {
		lw = &v1alpha1.PlannedFailoverLagWaitRecord{StartTime: now}
	} else {
		lw = lw.DeepCopy()
	}
	lw.CaughtUpAt = &now
	return lw
}

// enterPromoting reads the target's server_uuid on a pinned session and
// writes the Promoting intent with it.
func (e *PlannedFailoverEngine) enterPromoting(ctx context.Context, fg *v1alpha1.MysqlFailoverGroup, pf *v1alpha1.PlannedFailoverStatus,
	ref PlannedRunRef, edit func(next *v1alpha1.PlannedFailoverStatus)) (time.Duration, error) {
	m := e.runMemo(pf.RunID)
	targetUUID, err := e.topo.ReadTargetServerUUID(ctx, ref)
	if d, ferr, handled := e.facadeErr(ctx, fg, pf, err); handled {
		return d, ferr
	}
	if err != nil || targetUUID == "" {
		if e.lossWindow(&m.targetLossSince, false) {
			return e.toRollingBack(ctx, fg, pf, v1alpha1.PlannedFailoverReasonTargetLost)
		}
		return plannedRequeuePoll, nil
	}
	m.targetLossSince = time.Time{}
	now := metav1.NewTime(e.clk.Now())
	m.promote = pfPromoteMemo{}
	out, werr := e.advance(ctx, fg, pf, func(next *v1alpha1.PlannedFailoverStatus) {
		edit(next)
		next.Phase = v1alpha1.PlannedFailoverPhasePromoting
		next.Promotion = &v1alpha1.PlannedFailoverPromotionRecord{
			State: v1alpha1.PlannedFailoverPromotionIntended, IntendedAt: now, TargetServerUUID: targetUUID,
		}
	})
	return e.afterAdvance(ctx, fg, pf, out, werr)
}

// promotingStep runs one staged promotion attempt, or the confirmed tail.
func (e *PlannedFailoverEngine) promotingStep(ctx context.Context, fg *v1alpha1.MysqlFailoverGroup, pf *v1alpha1.PlannedFailoverStatus,
	ref PlannedRunRef) (time.Duration, error) {
	m := e.runMemo(pf.RunID)
	if m.promote.confirmed {
		return e.confirmedTail(ctx, fg, pf, ref)
	}
	out, err := e.topo.Promote(ctx, ref, pf)
	if d, ferr, handled := e.facadeErr(ctx, fg, pf, err); handled {
		return d, ferr
	}
	if err != nil {
		log.FromContext(ctx).Info("planned failover: promotion attempt failed; retrying", "fg", ref.Group.String(),
			"runId", pf.RunID, "error", err.Error())
		out = PromoteOutcome{Kind: PromoteUnconfirmed, TargetReachable: true, MayHaveWritten: m.promote.mayHaveWritten}
	}
	return e.applyPromoteOutcome(ctx, fg, pf, out)
}

// applyPromoteOutcome is the §7.4 mapping.
func (e *PlannedFailoverEngine) applyPromoteOutcome(ctx context.Context, fg *v1alpha1.MysqlFailoverGroup, pf *v1alpha1.PlannedFailoverStatus,
	out PromoteOutcome) (time.Duration, error) {
	m := e.runMemo(pf.RunID)
	m.promote.mayHaveWritten = m.promote.mayHaveWritten || out.MayHaveWritten
	m.warnings = addPlannedWarnings(m.warnings, out.Warnings...)
	switch out.Kind {
	case PromoteConfirmed:
		m.promote.confirmed = true
		m.promote.mayHaveWritten = true
		m.promote.confirmedAt = e.clk.Now()
		m.targetLossSince = time.Time{}
		// The confirmed tail runs in its own pass with its own bound.
		return plannedRequeueNext, nil
	case PromoteAborted:
		reason := out.Reason
		if reason == "" {
			reason = v1alpha1.PlannedFailoverReasonPromotionAborted
		}
		if out.Terminal != "" {
			if out.Terminal == v1alpha1.PlannedFailoverOutcomeManualRecoveryRequired {
				return e.endManualRecovery(ctx, fg, pf, reason, "the promotion found the target in a state the run cannot explain")
			}
			msg := fmt.Sprintf("promotion of %q aborted (%s); nothing was written to the target", pf.Target, reason)
			if out.MayHaveWritten || m.promote.mayHaveWritten {
				// A WasWritable gate refusal: an earlier attempt may have made
				// the target writable.
				msg = fmt.Sprintf("promotion of %q aborted (%s); the target may have accepted writes from an earlier attempt", pf.Target, reason)
			}
			return e.finish(ctx, fg, pf, plannedEnd{outcome: out.Terminal, reason: reason, msg: msg})
		}
		return e.toRollingBack(ctx, fg, pf, reason)
	case PromoteInconsistent:
		reason := out.Reason
		if reason == "" {
			reason = v1alpha1.PlannedFailoverReasonExecuteFailed
		}
		if out.RollbackIfUnrecorded && !plannedOwnRecordDurable(fg, pf, e.clk.Now()) {
			// The DST-3 ruling: the target was replaced before this process
			// saw any promotion statement, the run's own failover record is
			// not durable (so it never reached RecordPlannedPromotion and DNS
			// never pointed at the target), the source still holds all of F
			// under the run's fence, and no site is writable. Roll back; §9
			// still decides whether the source is unfenced.
			log.FromContext(ctx).Info("planned failover: target replaced before the promotion was recorded; rolling back",
				"fg", fg.Namespace+"/"+fg.Name, "runId", pf.RunID, "target", pf.Target, "targetServerUUID", out.TargetServerUUID)
			return e.toRollingBack(ctx, fg, pf, v1alpha1.PlannedFailoverReasonTargetChanged)
		}
		return e.endManualRecovery(ctx, fg, pf, reason, fmt.Sprintf("target %q is in a state the run cannot explain (stage %s)", pf.Target, out.Stage))
	}
	// Unconfirmed: stay Promoting and re-classify on a new pinned session.
	intended := e.clk.Now()
	if pf.Promotion != nil {
		intended = pf.Promotion.IntendedAt.Time
	}
	targetLost := e.lossWindow(&m.targetLossSince, out.TargetReachable)
	pastDeadline := !e.clk.Now().Before(intended.Add(plannedFailoverPromotionDeadline))
	if !targetLost && !pastDeadline {
		return plannedRequeueBackoff, nil
	}
	if m.promote.mayHaveWritten || out.MayHaveWritten {
		reason := v1alpha1.PlannedFailoverReasonPromotionUnconfirmed
		if targetLost {
			reason = v1alpha1.PlannedFailoverReasonTargetLost
		}
		return e.finish(ctx, fg, pf, plannedEnd{outcome: v1alpha1.PlannedFailoverOutcomePromotedUnconfirmed, reason: reason,
			msg: fmt.Sprintf("super_read_only=OFF may have been sent to %q but the promotion cannot be confirmed; %q is named as the last failover target only",
				pf.Target, pf.Target)})
	}
	if targetLost {
		return e.toRollingBack(ctx, fg, pf, v1alpha1.PlannedFailoverReasonTargetLost)
	}
	return e.toRollingBack(ctx, fg, pf, v1alpha1.PlannedFailoverReasonPromotionAborted)
}

// confirmedTail records the confirmed promotion in the topology manager,
// publishes DNS, releases the claim, then writes Publishing (§7.4, the
// DST-4 ruling): once the promotion is confirmed, recorded and published the
// claim protects nothing the run still has to do, so a stalled Publishing
// write must not keep automation (a split-brain fence of a site an external
// writer made writable, say) blocked. A restart before Publishing commits
// re-derives the confirmation from the live Promoting block (§7.1 Writable)
// and recognises the run's own failover record by content (§4.4).
func (e *PlannedFailoverEngine) confirmedTail(ctx context.Context, fg *v1alpha1.MysqlFailoverGroup, pf *v1alpha1.PlannedFailoverStatus,
	ref PlannedRunRef) (time.Duration, error) {
	nn := ref.Group
	m := e.runMemo(pf.RunID)
	if !m.promote.recorded {
		// §4.4 check 1 in the same pass, also for a claim this process was
		// granted: the durable record is still the baseline, or the run's
		// own. Within one leader nothing records a failover under the
		// claim; a changed record means a second writer, so the run never
		// records over it.
		if !plannedRecordUnchanged(fg, pf, e.clk.Now()) {
			return e.endManualRecovery(ctx, fg, pf, v1alpha1.PlannedFailoverReasonAuthorityChanged,
				"a failover was recorded during the run, so its confirmed promotion is not recorded over it")
		}
		err := e.topo.RecordPlannedPromotion(ctx, ref, m.promote.confirmedAt, plannedFenceSet(pf))
		if d, ferr, handled := e.facadeErr(ctx, fg, pf, err); handled {
			return d, ferr
		}
		if err != nil {
			log.FromContext(ctx).Info("planned failover: recording the promotion failed; retrying", "fg", nn.String(),
				"runId", pf.RunID, "error", err.Error())
			return plannedRequeueBackoff, nil
		}
		m.promote.recorded = true
	}
	if m.promote.dns == nil {
		state, err := e.topo.ApplyDNS(ctx, ref)
		if d, ferr, handled := e.facadeErr(ctx, fg, pf, err); handled {
			return d, ferr
		}
		rec := &v1alpha1.PlannedFailoverDNSRecord{State: state, AttemptedAt: metav1.NewTime(e.clk.Now())}
		if err != nil {
			rec.State = v1alpha1.PlannedFailoverDNSFailed
			rec.LastError = err.Error()
		}
		if rec.State == "" {
			rec.State = v1alpha1.PlannedFailoverDNSSkipped
		}
		if rec.State == v1alpha1.PlannedFailoverDNSFailed {
			m.warnings = addPlannedWarnings(m.warnings, v1alpha1.PlannedFailoverWarningDNSUpdateFailed)
			e.sink.Event(fg, corev1.EventTypeWarning, "PlannedFailoverDNSUpdateFailed",
				fmt.Sprintf("planned failover promoted %q but the DNS update failed (%s); the topology manager retries it", pf.Target, rec.LastError))
		}
		m.promote.dns = rec
	}
	if !m.promote.released {
		e.release(ctx, nn, pf.RunID, ReleasePublishing, "")
		m.promote.released = true
	}
	return e.writePublishing(ctx, fg, pf)
}

// writePublishing writes the Publishing record of a confirmed, recorded
// and released run. It holds no claim; a non-commit is retried, and the
// status stall (§6.4) may end the run once its failover record is durable.
func (e *PlannedFailoverEngine) writePublishing(ctx context.Context, fg *v1alpha1.MysqlFailoverGroup, pf *v1alpha1.PlannedFailoverStatus) (time.Duration, error) {
	m := e.runMemo(pf.RunID)
	confirmedAt := metav1.NewTime(m.promote.confirmedAt)
	dns := m.promote.dns.DeepCopy()
	warnings := slices.Clone(m.warnings)
	slices.Sort(warnings)
	out, err := e.advance(ctx, fg, pf, func(next *v1alpha1.PlannedFailoverStatus) {
		next.Phase = v1alpha1.PlannedFailoverPhasePublishing
		next.Promotion = next.Promotion.DeepCopy()
		next.Promotion.State = v1alpha1.PlannedFailoverPromotionConfirmed
		next.Promotion.ConfirmedAt = &confirmedAt
		next.Promotion.Warnings = warnings
		next.DNS = dns
		next.Message = fmt.Sprintf("promoted %q; publishing", pf.Target)
	})
	if out != WriteCommitted {
		return e.afterAdvance(ctx, fg, pf, out, err)
	}
	return plannedRequeueNext, nil
}

// plannedRecordUnchanged is §4.4 check 1 on fg: the durable failover
// record is the run's claim baseline, or the run's own record recognised
// by content.
func plannedRecordUnchanged(fg *v1alpha1.MysqlFailoverGroup, pf *v1alpha1.PlannedFailoverStatus, now time.Time) bool {
	rec, _, err := EffectiveFailoverRecord(fg, now)
	if err != nil {
		return false
	}
	return sameFailoverRecord(newFailoverRecordRef(rec.LastFailover, rec.LastFailoverTarget), failoverRecordRefFromAPI(pf.ClaimBaseline)) ||
		ownFailoverRecord(fg, rec, pf)
}

// publishingPass writes the Succeeded record with the failover record.
func (e *PlannedFailoverEngine) publishingPass(ctx context.Context, fg *v1alpha1.MysqlFailoverGroup, pf *v1alpha1.PlannedFailoverStatus) (time.Duration, error) {
	nn := types.NamespacedName{Namespace: fg.Namespace, Name: fg.Name}
	// The claim was released when Publishing committed; a release that did
	// not run (a restart in between) is harmless to repeat.
	e.release(ctx, nn, pf.RunID, ReleasePublishing, "")
	return e.finish(ctx, fg, pf, plannedEnd{outcome: v1alpha1.PlannedFailoverOutcomeSucceeded,
		msg: fmt.Sprintf("promoted %q", pf.Target), why: ReleasePublishing, eventReason: "PlannedFailoverCompleted"})
}

// toRollingBack writes the rollback intent.
func (e *PlannedFailoverEngine) toRollingBack(ctx context.Context, fg *v1alpha1.MysqlFailoverGroup, pf *v1alpha1.PlannedFailoverStatus,
	reason v1alpha1.PlannedFailoverReason) (time.Duration, error) {
	now := metav1.NewTime(e.clk.Now())
	out, err := e.advance(ctx, fg, pf, func(next *v1alpha1.PlannedFailoverStatus) {
		next.Phase = v1alpha1.PlannedFailoverPhaseRollingBack
		next.Rollback = &v1alpha1.PlannedFailoverRollbackRecord{Reason: reason, IntendedAt: now}
		next.Message = fmt.Sprintf("rolling back (%s)", reason)
	})
	if out == WriteCommitted {
		e.sink.Event(fg, corev1.EventTypeWarning, "PlannedFailoverRollingBack",
			fmt.Sprintf("planned failover to %q rolling back (%s)", pf.Target, reason))
	}
	return e.afterAdvance(ctx, fg, pf, out, err)
}

// rollbackStep runs one preconditioned rollback attempt (§9).
func (e *PlannedFailoverEngine) rollbackStep(ctx context.Context, fg *v1alpha1.MysqlFailoverGroup, pf *v1alpha1.PlannedFailoverStatus,
	ref PlannedRunRef) (time.Duration, error) {
	m := e.runMemo(pf.RunID)
	reason := v1alpha1.PlannedFailoverReasonInternalError
	intended := e.clk.Now()
	if pf.Rollback != nil {
		reason = pf.Rollback.Reason
		intended = pf.Rollback.IntendedAt.Time
	}
	res, err := e.topo.Rollback(ctx, ref, pf)
	if d, ferr, handled := e.facadeErr(ctx, fg, pf, err); handled {
		return d, ferr
	}
	if err == nil && res.Outcome != "" {
		r := res.Reason
		if r == "" {
			r = reason
		}
		m.warnings = addPlannedWarnings(m.warnings, res.Warnings...)
		if res.Outcome == v1alpha1.PlannedFailoverOutcomeManualRecoveryRequired {
			return e.endManualRecovery(ctx, fg, pf, r, "the rollback found writes on the target outside the fenced set")
		}
		return e.finish(ctx, fg, pf, plannedEnd{outcome: res.Outcome, reason: r,
			msg: plannedRollbackMessage(pf, res.Outcome, r)})
	}
	if err != nil {
		log.FromContext(ctx).Info("planned failover: rollback attempt failed; retrying", "fg", ref.Group.String(),
			"runId", pf.RunID, "error", err.Error())
	}
	m.warnings = addPlannedWarnings(m.warnings, res.Warnings...)
	if !e.clk.Now().Before(intended.Add(plannedFailoverRollbackDeadline)) {
		return e.finish(ctx, fg, pf, plannedEnd{outcome: v1alpha1.PlannedFailoverOutcomeSourceLeftFenced, reason: reason,
			msg: fmt.Sprintf("rollback (%s) could not confirm the source %q writable within %s; it stays fenced",
				reason, pf.Source, plannedFailoverRollbackDeadline)})
	}
	return plannedRequeuePoll, nil
}

func plannedRollbackMessage(pf *v1alpha1.PlannedFailoverStatus, outcome v1alpha1.PlannedFailoverOutcome, reason v1alpha1.PlannedFailoverReason) string {
	switch outcome {
	case v1alpha1.PlannedFailoverOutcomeRolledBack:
		return fmt.Sprintf("planned failover to %q rolled back (%s); source %q is writable", pf.Target, reason, pf.Source)
	case v1alpha1.PlannedFailoverOutcomeSourceLeftFenced:
		return fmt.Sprintf("planned failover to %q rolled back (%s); source %q left fenced", pf.Target, reason, pf.Source)
	case v1alpha1.PlannedFailoverOutcomePromotedUnconfirmed:
		return fmt.Sprintf("planned failover to %q ended unconfirmed (%s); %q is named as the last failover target only", pf.Target, reason, pf.Target)
	}
	return fmt.Sprintf("planned failover to %q ended %s (%s)", pf.Target, outcome, reason)
}

// ---------- manager absence (§6.3) ----------

// managerAbsent waits for a manager while the run holds the claim, ending
// it without MySQL access 5 minutes after the durable absent-since time.
func (e *PlannedFailoverEngine) managerAbsent(ctx context.Context, fg *v1alpha1.MysqlFailoverGroup, pf *v1alpha1.PlannedFailoverStatus) (time.Duration, error) {
	nn := types.NamespacedName{Namespace: fg.Namespace, Name: fg.Name}
	m := e.runMemo(pf.RunID)
	start, ok := parseDeadlineStart(fg.GetAnnotations()[PlannedFailoverManagerAbsentSinceAnnotation], pf.RunID)
	if !ok {
		var out WriteOutcome
		var err error
		start, out, err = e.store.MarkDeadlineStart(ctx, fg, DeadlineManagerAbsent, pf.RunID, e.clk.Now())
		if out != WriteCommitted {
			log.FromContext(ctx).Info("planned failover: could not record the manager-absent-since annotation; retrying",
				"fg", nn.String(), "runId", pf.RunID, "outcome", out.String(), "error", errString(err))
			return plannedRequeueBackoff, nil
		}
	}
	m.absentSince = start
	elapsed := e.clk.Now().Sub(start)
	if elapsed >= plannedFailoverManagerAbsentWarnAfter && !m.absentWarned {
		m.absentWarned = true
		e.sink.Event(fg, corev1.EventTypeWarning, "PlannedFailoverStalled",
			fmt.Sprintf("planned failover %s (phase %s) has had no topology manager for %s; it ends after %s",
				pf.RunID, pf.Phase, truncateDur(elapsed), plannedFailoverManagerAbsentDeadline))
	}
	if elapsed < plannedFailoverManagerAbsentDeadline {
		return plannedRequeueBackoff, nil
	}
	reason := v1alpha1.PlannedFailoverReasonManagerUnavailable
	if pf.Phase == v1alpha1.PlannedFailoverPhaseRollingBack && pf.Rollback != nil {
		reason = pf.Rollback.Reason
	}
	return e.endWithoutMySQL(ctx, fg, pf, reason,
		fmt.Sprintf("no topology manager for %s; the run ended without MySQL access", plannedFailoverManagerAbsentDeadline), ReleaseManagerAbsent)
}

// clearAbsence removes the run's manager-absent-since annotation once a
// manager is back.
func (e *PlannedFailoverEngine) clearAbsence(ctx context.Context, fg *v1alpha1.MysqlFailoverGroup, pf *v1alpha1.PlannedFailoverStatus) {
	m := e.runMemo(pf.RunID)
	m.absentWarned = false
	m.absentSince = time.Time{}
	if _, ok := parseDeadlineStart(fg.GetAnnotations()[PlannedFailoverManagerAbsentSinceAnnotation], pf.RunID); !ok {
		return
	}
	if out, err := e.store.ClearDeadlineStart(ctx, fg, DeadlineManagerAbsent, pf.RunID); out != WriteCommitted {
		log.FromContext(ctx).Info("planned failover: could not clear the manager-absent-since annotation; retrying",
			"fg", fg.Namespace+"/"+fg.Name, "runId", pf.RunID, "error", errString(err))
	}
}

// endWithoutMySQL ends a claim-holding run with no MySQL access: the source
// stays as it is, a possible promotion stays unconfirmed (and unnamed).
func (e *PlannedFailoverEngine) endWithoutMySQL(ctx context.Context, fg *v1alpha1.MysqlFailoverGroup, pf *v1alpha1.PlannedFailoverStatus,
	reason v1alpha1.PlannedFailoverReason, msg string, why ClaimReleaseReason) (time.Duration, error) {
	outcome := v1alpha1.PlannedFailoverOutcomeSourceLeftFenced
	if pf.Phase == v1alpha1.PlannedFailoverPhasePromoting {
		outcome = v1alpha1.PlannedFailoverOutcomePromotedUnconfirmed
	}
	return e.finish(ctx, fg, pf, plannedEnd{outcome: outcome, reason: reason, msg: msg, why: why, noNaming: true})
}

// ---------- terminal ----------

// plannedEnd describes a terminal record.
type plannedEnd struct {
	outcome  v1alpha1.PlannedFailoverOutcome
	reason   v1alpha1.PlannedFailoverReason
	warnings []v1alpha1.PlannedFailoverWarning
	msg      string
	// why is the release reason (default terminal).
	why ClaimReleaseReason
	// eventReason overrides the terminal Event's reason.
	eventReason string
	// noNaming skips the topology-manager naming (no manager to name in).
	noNaming  bool
	holdSites []string
	// confirmedAt is a Succeeded run's confirmation time when the block
	// does not carry it yet (it ended before Publishing committed).
	confirmedAt time.Time
}

// endManualRecovery ends the run ManualRecoveryRequired and sets the hold.
func (e *PlannedFailoverEngine) endManualRecovery(ctx context.Context, fg *v1alpha1.MysqlFailoverGroup, pf *v1alpha1.PlannedFailoverStatus,
	reason v1alpha1.PlannedFailoverReason, why string) (time.Duration, error) {
	sites := []string{}
	if pf.Target != "" {
		sites = append(sites, pf.Target)
	}
	if pf.Source != "" && pf.Source != pf.Target {
		sites = append(sites, pf.Source)
	}
	return e.finish(ctx, fg, pf, plannedEnd{outcome: v1alpha1.PlannedFailoverOutcomeManualRecoveryRequired, reason: reason,
		msg: fmt.Sprintf("%s (%s); automatic failover is held until %s=%s", why, reason, AckPlannedFailoverHoldAnnotation, pf.RunID),
		why: ReleaseToHold, holdSites: sites})
}

// fenceAttributable reports whether the source's fence is this run's own:
// proven applied (durably, or by this process's memo).
func (e *PlannedFailoverEngine) fenceAttributable(pf *v1alpha1.PlannedFailoverStatus, warnings []v1alpha1.PlannedFailoverWarning) bool {
	if slices.Contains(warnings, v1alpha1.PlannedFailoverWarningFenceNotAttributable) {
		return false
	}
	if pf.Fence != nil && pf.Fence.State == v1alpha1.PlannedFailoverFenceApplied {
		return true
	}
	m := e.runMemo(pf.RunID)
	return m.fence.sent || m.fence.setSent
}

// terminalRecord builds the terminal block of pf for end.
func (e *PlannedFailoverEngine) terminalRecord(pf *v1alpha1.PlannedFailoverStatus, end plannedEnd) *v1alpha1.PlannedFailoverStatus {
	m := e.runMemo(pf.RunID)
	next := pf.DeepCopy()
	next.Revision = pf.Revision + 1
	next.Phase = v1alpha1.PlannedFailoverPhaseFailed
	if end.outcome == v1alpha1.PlannedFailoverOutcomeSucceeded {
		next.Phase = v1alpha1.PlannedFailoverPhaseSucceeded
	}
	warnings := addPlannedWarnings(slices.Clone(m.warnings), end.warnings...)
	// What the durable records carry, so a terminal record written by
	// another process (a restart in Publishing) keeps the run's warnings.
	if pf.Promotion != nil {
		warnings = addPlannedWarnings(warnings, pf.Promotion.Warnings...)
	}
	if pf.DNS != nil && pf.DNS.State == v1alpha1.PlannedFailoverDNSFailed {
		warnings = addPlannedWarnings(warnings, v1alpha1.PlannedFailoverWarningDNSUpdateFailed)
	}
	if end.outcome == v1alpha1.PlannedFailoverOutcomeSucceeded && !end.confirmedAt.IsZero() && next.Promotion != nil &&
		next.Promotion.ConfirmedAt == nil {
		// A Succeeded record ended before Publishing committed (the status
		// stall, or a marker): it carries the confirmation, so its terminal
		// write applies the run's failover record (§5.3).
		next.Promotion = next.Promotion.DeepCopy()
		next.Promotion.State = v1alpha1.PlannedFailoverPromotionConfirmed
		at := metav1.NewTime(end.confirmedAt.UTC().Truncate(time.Second))
		next.Promotion.ConfirmedAt = &at
	}
	if pf.Dragonfly != nil && pf.Dragonfly.Takeover != nil && pf.Dragonfly.Takeover.State == v1alpha1.DragonflyTakeoverSucceeded &&
		end.outcome != v1alpha1.PlannedFailoverOutcomeSucceeded {
		warnings = addPlannedWarnings(warnings, v1alpha1.PlannedFailoverWarningDragonflySessionsLost)
	}
	slices.Sort(warnings)
	if next.Admission != nil && next.Admission.RetryAfter != nil {
		// A finished run is not retried.
		next.Admission = next.Admission.DeepCopy()
		next.Admission.RetryAfter = nil
	}
	next.Result = &v1alpha1.PlannedFailoverResult{
		Outcome: end.outcome, Reason: end.reason, CompletionTime: metav1.NewTime(e.clk.Now()), Warnings: warnings,
	}
	if end.msg != "" {
		next.Message = end.msg
	}
	return next
}

// terminalMutate is the status mutation that goes with a terminal record:
// the failover record for Succeeded, the hold condition for
// ManualRecoveryRequired.
//
// The Succeeded write also takes status.activeSite and
// status.topologyGeneration from the current manager through
// MergePlannedAuthority, the rule the runner's snapshot writes use, so the
// terminal record never lands beside an activeSite that still names the
// source (§5.3, §12).
func (e *PlannedFailoverEngine) terminalMutate(nn types.NamespacedName, next *v1alpha1.PlannedFailoverStatus, sites []string) func(*v1alpha1.MysqlFailoverGroupStatus) {
	switch next.Result.Outcome {
	case v1alpha1.PlannedFailoverOutcomeManualRecoveryRequired:
		runID, reason, now := next.RunID, next.Result.Reason, e.clk.Now()
		return func(st *v1alpha1.MysqlFailoverGroupStatus) {
			setPlannedHoldCondition(st, runID, reason, sites, now)
		}
	case v1alpha1.PlannedFailoverOutcomeSucceeded:
		if next.Promotion == nil || next.Promotion.ConfirmedAt == nil {
			return nil
		}
		confirmedAt, target, watermark := next.Promotion.ConfirmedAt.Time, next.Target, plannedFenceSet(next)
		authority, haveAuthority := e.topo.ManagerAuthority(nn)
		return func(st *v1alpha1.MysqlFailoverGroupStatus) {
			if haveAuthority {
				MergePlannedAuthority(st, authority)
			}
			applyPlannedFailoverRecord(st, confirmedAt, target, watermark)
		}
	}
	return nil
}

// applyPlannedFailoverRecord applies the run's failover record
// {confirmedAt, target, watermark} to status under the timestamp-monotonic
// rule (§5.3): never to a strictly older lastFailover; on an equal one
// (second precision) only the target and watermark may change.
func applyPlannedFailoverRecord(st *v1alpha1.MysqlFailoverGroupStatus, confirmedAt time.Time, target, watermark string) {
	rec := newFailoverRecordRef(confirmedAt, target)
	if st.LastFailover != nil {
		stored := st.LastFailover.UTC().Truncate(time.Second)
		if stored.After(rec.LastFailover) {
			return
		}
	}
	t := metav1.NewTime(rec.LastFailover)
	st.LastFailover = &t
	st.LastFailoverTarget = target
	st.PromotionGtidExecuted = watermark
}

// finish writes the terminal record and, once it has committed, releases
// the claim (or converts it into a hold) and reports the run.
func (e *PlannedFailoverEngine) finish(ctx context.Context, fg *v1alpha1.MysqlFailoverGroup, pf *v1alpha1.PlannedFailoverStatus,
	end plannedEnd) (time.Duration, error) {
	nn := types.NamespacedName{Namespace: fg.Namespace, Name: fg.Name}
	ref := plannedRunRef(nn, pf)
	m := e.runMemo(pf.RunID)
	if end.why == "" {
		end.why = ReleaseTerminal
	}
	if !end.noNaming {
		e.nameForOutcome(ctx, pf, ref, end)
	}
	claimHeld := plannedFailoverOwnsSitesPhase(pf.Phase) || (pf.Phase == v1alpha1.PlannedFailoverPhaseAdmitting && m.granted)
	var written *v1alpha1.PlannedFailoverStatus
	out, next, err := e.commit(ctx, fg, claimHeld, func(base *v1alpha1.MysqlFailoverGroup) (*v1alpha1.PlannedFailoverStatus,
		func(*v1alpha1.MysqlFailoverGroupStatus), bool) {
		cur, ok := sameRun(base, pf, pf.Phase)
		if !ok {
			return nil, nil, false
		}
		written = e.terminalRecord(cur, end)
		return written, e.terminalMutate(nn, written, end.holdSites), true
	})
	if next != nil {
		m.pendingTerminal = next.DeepCopy()
		m.holdSites = end.holdSites
	}
	if out != WriteCommitted {
		if claimHeld && (out == WriteUnknown || out == WriteNotCommitted) {
			if d, done := e.maybeStallFinish(ctx, fg, pf); done {
				return d, nil
			}
		}
		if err != nil {
			log.FromContext(ctx).Info("planned failover: terminal status write did not commit; retrying", "fg", nn.String(),
				"runId", pf.RunID, "outcome", out.String(), "error", err.Error())
		}
		return requeueFor(out), nil
	}
	m.pendingTerminal = nil
	e.afterTerminal(ctx, nn, fg, written, end.why, end.eventReason)
	return 0, nil
}

// nameForOutcome records the names a terminal outcome hands to the
// topology manager before the release (§6.2, §6.5).
func (e *PlannedFailoverEngine) nameForOutcome(ctx context.Context, pf *v1alpha1.PlannedFailoverStatus, ref PlannedRunRef, end plannedEnd) {
	nn := ref.Group
	m := e.runMemo(pf.RunID)
	switch end.outcome {
	case v1alpha1.PlannedFailoverOutcomeSourceLeftFenced:
		if ref.Source == "" || !e.fenceAttributable(pf, addPlannedWarnings(slices.Clone(m.warnings), end.warnings...)) {
			return
		}
		if err := e.topo.NameFencedSource(ref); err == nil {
			log.FromContext(ctx).Info("planned failover: named fenced source for re-assert", "fg", nn.String(),
				"runId", pf.RunID, "site", ref.Source)
		}
	case v1alpha1.PlannedFailoverOutcomePromotedUnconfirmed:
		if end.reason == v1alpha1.PlannedFailoverReasonManagerUnavailable {
			return
		}
		if err := e.topo.NameUnconfirmedTarget(ref); err != nil && !errors.Is(err, errManagerAbsent) {
			log.FromContext(ctx).Info("planned failover: could not name the unconfirmed target", "fg", nn.String(),
				"runId", pf.RunID, "error", err.Error())
		}
	}
}

// afterTerminal runs once a terminal record (or the terminal marker) of the
// run has committed: release or hold, report, and clean the durable
// deadline annotations.
func (e *PlannedFailoverEngine) afterTerminal(ctx context.Context, nn types.NamespacedName, fg *v1alpha1.MysqlFailoverGroup,
	final *v1alpha1.PlannedFailoverStatus, why ClaimReleaseReason, eventReason string) {
	m := e.runMemo(final.RunID)
	e.markTerminated(final.RunID)
	outcome := final.Result.Outcome
	if outcome == v1alpha1.PlannedFailoverOutcomeManualRecoveryRequired {
		e.topo.ConvertToHold(nn, final.RunID)
		log.FromContext(ctx).Info("planned failover: automation held pending acknowledgement", "fg", nn.String(),
			"runId", final.RunID, "sites", strings.Join(m.holdSites, ","))
	} else {
		e.release(ctx, nn, final.RunID, why, outcome)
	}
	if !m.reported {
		m.reported = true
		e.reportTerminal(nn, fg, final, eventReason)
	}
	fresh := e.reload(ctx, fg)
	if fresh == nil {
		return
	}
	for _, kind := range []DeadlineKind{DeadlineManagerAbsent, DeadlineStatusStall} {
		if _, ok := parseDeadlineStart(fresh.GetAnnotations()[kind.Annotation()], final.RunID); !ok {
			continue
		}
		if out, _ := e.store.ClearDeadlineStart(ctx, fresh, kind, final.RunID); out == WriteCommitted {
			if f := e.reload(ctx, fresh); f != nil {
				fresh = f
			}
		}
	}
}

// reportTerminal emits the run's metrics and its terminal Event.
func (e *PlannedFailoverEngine) reportTerminal(nn types.NamespacedName, fg *v1alpha1.MysqlFailoverGroup,
	final *v1alpha1.PlannedFailoverStatus, eventReason string) {
	e.sink.RunTerminal(nn, final)
	r := final.Result
	switch {
	case r.Outcome == v1alpha1.PlannedFailoverOutcomeSucceeded:
		if eventReason == "" {
			eventReason = "PlannedFailoverCompleted"
		}
		e.sink.Event(fg, corev1.EventTypeNormal, eventReason,
			fmt.Sprintf("planned failover to %q complete (warnings: %s)", final.Target, plannedWarningList(r.Warnings)))
	case r.Outcome == v1alpha1.PlannedFailoverOutcomeManualRecoveryRequired:
		e.sink.Event(fg, corev1.EventTypeWarning, "PlannedFailoverManualRecoveryRequired",
			fmt.Sprintf("planned failover %s ended ManualRecoveryRequired (%s): resolve the split brain by hand, then annotate %s=%s",
				final.RunID, r.Reason, AckPlannedFailoverHoldAnnotation, final.RunID))
	default:
		if eventReason == "" {
			eventReason = "PlannedFailoverFailed"
		}
		if r.Outcome == v1alpha1.PlannedFailoverOutcomePromotedUnconfirmed {
			e.sink.Event(fg, corev1.EventTypeWarning, "PlannedFailoverPromotionUnconfirmed", final.Message)
		}
		e.sink.Event(fg, corev1.EventTypeWarning, eventReason,
			fmt.Sprintf("planned failover %s (%s): %s", r.Outcome, r.Reason, final.Message))
	}
}

func plannedWarningList(ws []v1alpha1.PlannedFailoverWarning) string {
	if len(ws) == 0 {
		return "none"
	}
	out := make([]string, len(ws))
	for i, w := range ws {
		out[i] = string(w)
	}
	return strings.Join(out, ", ")
}

// release releases the run's claim and logs it when it held one. outcome
// is the run's outcome when it is terminal, "" otherwise.
func (e *PlannedFailoverEngine) release(ctx context.Context, nn types.NamespacedName, runID string, why ClaimReleaseReason,
	outcome v1alpha1.PlannedFailoverOutcome) {
	if runID == "" {
		return
	}
	if e.topo.ReleasePlannedFailoverClaim(nn, runID, why) {
		log.FromContext(ctx).Info("planned failover: claim released", "fg", nn.String(), "runId", runID,
			"outcome", string(outcome), "why", string(why))
	}
}

// releaseZombie releases a Run-mode claim whose run is no longer live in
// the uncached block (terminal, marked, absent, or another run's), §3.4.
// The held run comes from the slot, so a claim hydrated by startManager for
// a run this process never saw live is cleared too.
func (e *PlannedFailoverEngine) releaseZombie(ctx context.Context, nn types.NamespacedName, pf *v1alpha1.PlannedFailoverStatus) {
	runID, ok := e.topo.HeldRunClaim(nn)
	if !ok || runID == "" {
		return
	}
	if pf != nil && pf.RunID == runID && !plannedFailoverTerminal(pf) {
		return
	}
	e.release(ctx, nn, runID, ReleaseZombie, "")
}

// ---------- status stall (§6.4) ----------

// maybeStallFinish ends the run with a terminal marker once its status
// writes have failed for plannedFailoverStatusStallDeadline from the
// durable stall-since time. done=false: keep retrying the status write.
func (e *PlannedFailoverEngine) maybeStallFinish(ctx context.Context, fg *v1alpha1.MysqlFailoverGroup, pf *v1alpha1.PlannedFailoverStatus) (time.Duration, bool) {
	nn := types.NamespacedName{Namespace: fg.Namespace, Name: fg.Name}
	m := e.runMemo(pf.RunID)
	fresh := e.reload(ctx, fg)
	if fresh != nil {
		if start, ok := parseDeadlineStart(fresh.GetAnnotations()[PlannedFailoverStallSinceAnnotation], pf.RunID); ok {
			m.stallSince = start
		}
	}
	if m.stallSince.IsZero() || e.clk.Now().Sub(m.stallSince) < plannedFailoverStatusStallDeadline || fresh == nil {
		return 0, false
	}
	if len(plannedFenceSet(pf)) > plannedFailoverWatermarkAnnotationLimit {
		log.FromContext(ctx).Info("planned failover: status stalled, but the fenced set is too large for a durable out-of-band watermark; keeping the claim",
			"fg", nn.String(), "runId", pf.RunID)
		return 0, false
	}
	cur, ok := sameRun(fresh, pf)
	if !ok || plannedFailoverTerminal(cur) {
		return 0, false
	}
	end := e.stallOutcome(ctx, nn, cur)
	if p := m.pendingTerminal; p != nil && p.RunID == cur.RunID && p.Result != nil {
		// The stalled write was this run's terminal record: keep its outcome.
		end = plannedEnd{outcome: p.Result.Outcome, reason: p.Result.Reason,
			warnings: addPlannedWarnings(slices.Clone(p.Result.Warnings), v1alpha1.PlannedFailoverWarningStatusStalled), msg: p.Message,
			confirmedAt: end.confirmedAt}
		if p.Promotion != nil && p.Promotion.ConfirmedAt != nil {
			end.confirmedAt = p.Promotion.ConfirmedAt.Time
		}
	}
	if end.outcome == v1alpha1.PlannedFailoverOutcomeSucceeded && cur.Phase != v1alpha1.PlannedFailoverPhasePublishing {
		// A confirmed run whose Publishing record never committed is ended
		// by the marker only once its failover record, watermark included,
		// is durable outside this process (the out-of-band annotation or
		// the status copy, read back uncached): the marker ends the run, and
		// a restart restores the watermark only from a durable record.
		// Until then the live Promoting block is the record: a restart
		// re-derives the confirmation from it (§7.1 Writable) and records
		// it again, so status writes simply keep retrying.
		check := e.reload(ctx, fresh)
		if check == nil || !runRecordDurable(check, cur, end.confirmedAt, e.clk.Now()) {
			if !m.recordWaitLogged {
				m.recordWaitLogged = true
				log.FromContext(ctx).Info("planned failover: status stalled, but the promotion's failover record is not durable yet; retrying",
					"fg", nn.String(), "runId", cur.RunID)
			}
			return 0, false
		}
		fresh = check
	}
	e.nameForOutcome(ctx, cur, plannedRunRef(nn, cur), end)
	final := e.terminalRecord(cur, end)
	marker := TerminalMarker{RunID: cur.RunID, Outcome: final.Result.Outcome, Reason: final.Result.Reason}
	base := fresh
	var out WriteOutcome
	var err error
	for attempt := 0; attempt < plannedWriteConflictRetries; attempt++ {
		out, err = e.store.WriteTerminalMarker(ctx, base, marker)
		if out != WriteConflict {
			break
		}
		if base = e.reload(ctx, base); base == nil {
			return plannedRequeueBackoff, true
		}
	}
	if out != WriteCommitted {
		if e.clk.Now().Sub(m.cannotWriteAt) >= time.Minute {
			m.cannotWriteAt = e.clk.Now()
			e.sink.Event(fg, corev1.EventTypeWarning, "PlannedFailoverStatusStalled",
				fmt.Sprintf("the operator cannot write MysqlFailoverGroup %s (status and metadata writes fail: %s); the planned-failover claim stays held",
					nn, errString(err)))
		}
		return plannedRequeueBackoff, true
	}
	if final.Result.Outcome == v1alpha1.PlannedFailoverOutcomeManualRecoveryRequired && len(m.holdSites) == 0 {
		m.holdSites = []string{cur.Target}
	}
	m.pendingTerminal = final.DeepCopy()
	log.FromContext(ctx).Error(err, "planned failover: status stalled; ended the run with a terminal marker",
		"fg", nn.String(), "runId", cur.RunID, "outcome", string(final.Result.Outcome), "reason", string(final.Result.Reason))
	e.sink.Event(fg, corev1.EventTypeWarning, "PlannedFailoverStatusStalled",
		fmt.Sprintf("status writes for planned failover %s failed for %s; the run ended %s (%s) with the %s annotation",
			cur.RunID, plannedFailoverStatusStallDeadline, final.Result.Outcome, final.Result.Reason, PlannedFailoverTerminalAnnotation))
	why := ReleaseStatusStalled
	e.afterTerminal(ctx, nn, fg, final, why, "")
	return plannedFailoverMarkerRetryInterval, true
}

// stallOutcome is §6.4 step 1: finish in memory only (no MySQL rollback).
// A confirmed promotion is still recorded in the topology manager and
// published before the release.
func (e *PlannedFailoverEngine) stallOutcome(ctx context.Context, nn types.NamespacedName, pf *v1alpha1.PlannedFailoverStatus) plannedEnd {
	m := e.runMemo(pf.RunID)
	stalled := []v1alpha1.PlannedFailoverWarning{v1alpha1.PlannedFailoverWarningStatusStalled}
	reason := v1alpha1.PlannedFailoverReasonStatusStalled
	switch {
	case pf.Phase == v1alpha1.PlannedFailoverPhasePublishing:
		return plannedEnd{outcome: v1alpha1.PlannedFailoverOutcomeSucceeded, warnings: stalled,
			msg: fmt.Sprintf("promoted %q; status writes stalled", pf.Target)}
	case m.promote.confirmed:
		ref := plannedRunRef(nn, pf)
		if !m.promote.recorded {
			if err := e.topo.RecordPlannedPromotion(ctx, ref, m.promote.confirmedAt, plannedFenceSet(pf)); err == nil {
				m.promote.recorded = true
			}
		}
		if m.promote.dns == nil {
			state, err := e.topo.ApplyDNS(ctx, ref)
			if err != nil || state == v1alpha1.PlannedFailoverDNSFailed {
				stalled = addPlannedWarnings(stalled, v1alpha1.PlannedFailoverWarningDNSUpdateFailed)
			}
		}
		return plannedEnd{outcome: v1alpha1.PlannedFailoverOutcomeSucceeded, warnings: stalled, confirmedAt: m.promote.confirmedAt,
			msg: fmt.Sprintf("promoted %q; status writes stalled", pf.Target)}
	case m.promote.mayHaveWritten:
		return plannedEnd{outcome: v1alpha1.PlannedFailoverOutcomePromotedUnconfirmed, reason: reason, warnings: stalled,
			msg: fmt.Sprintf("status writes stalled after super_read_only=OFF may have been sent to %q", pf.Target)}
	case pf.Phase == v1alpha1.PlannedFailoverPhaseAdmitting:
		return plannedEnd{outcome: v1alpha1.PlannedFailoverOutcomeNotStarted, reason: reason, warnings: stalled,
			msg: "status writes stalled before the claim was durable; nothing was sent"}
	case pf.Phase == v1alpha1.PlannedFailoverPhaseFencing && pf.Fence != nil && pf.Fence.State == v1alpha1.PlannedFailoverFenceIntended &&
		m.granted && !m.fence.setSent && !m.fence.sent:
		// The facade's memo is the authority on "a SET was sent": a fence
		// attempt that returned before its SET (an unreadable peer, say)
		// left the source untouched.
		return plannedEnd{outcome: v1alpha1.PlannedFailoverOutcomeRolledBack, reason: reason, warnings: stalled,
			msg: fmt.Sprintf("status writes stalled before the fence was sent; source %q is untouched", pf.Source)}
	}
	return plannedEnd{outcome: v1alpha1.PlannedFailoverOutcomeSourceLeftFenced, reason: reason, warnings: stalled,
		msg: fmt.Sprintf("status writes stalled; source %q left fenced", pf.Source)}
}

// markedRunPass handles a run the terminal marker ended: retry its
// terminal status record every 10s, then delete a non-hold marker (§6.4
// step 4). The claim is released (or held) whatever status says.
func (e *PlannedFailoverEngine) markedRunPass(ctx context.Context, fg *v1alpha1.MysqlFailoverGroup, pf *v1alpha1.PlannedFailoverStatus) (time.Duration, error) {
	nn := types.NamespacedName{Namespace: fg.Namespace, Name: fg.Name}
	marker, _ := parseTerminalMarker(fg.GetAnnotations()[PlannedFailoverTerminalAnnotation])
	m := e.runMemo(pf.RunID)
	e.markTerminated(pf.RunID)
	isHold := marker.Outcome == v1alpha1.PlannedFailoverOutcomeManualRecoveryRequired
	if isHold {
		e.topo.ConvertToHold(nn, pf.RunID)
	} else {
		e.release(ctx, nn, pf.RunID, ReleaseStatusStalled, marker.Outcome)
	}
	if plannedFailoverTerminal(pf) {
		if isHold {
			return 0, nil
		}
		if out, err := e.store.ClearTerminalMarker(ctx, fg, pf.RunID); out != WriteCommitted {
			// A non-hold marker names a finished run; leaving it is harmless.
			log.FromContext(ctx).Info("planned failover: could not delete the terminal marker of a finished run",
				"fg", nn.String(), "runId", pf.RunID, "error", errString(err))
			return plannedFailoverMarkerRetryInterval, nil
		}
		return 0, nil
	}
	final := m.pendingTerminal
	if final == nil || final.RunID != pf.RunID {
		end := plannedEnd{outcome: marker.Outcome, reason: marker.Reason,
			warnings: []v1alpha1.PlannedFailoverWarning{v1alpha1.PlannedFailoverWarningStatusStalled},
			msg:      fmt.Sprintf("ended %s (%s) by the terminal marker after status writes stalled", marker.Outcome, marker.Reason)}
		if marker.Outcome == v1alpha1.PlannedFailoverOutcomeSucceeded {
			// The marker was written only once the run's own failover record
			// was durable: recover its confirmation from it, so the terminal
			// write applies the record (§5.3).
			end.confirmedAt = plannedOwnRecordTime(fg, pf, e.clk.Now())
		}
		final = e.terminalRecord(pf, end)
	}
	sites := m.holdSites
	if isHold && len(sites) == 0 {
		sites = []string{pf.Target}
	}
	out, _, _ := e.commit(ctx, fg, false, func(base *v1alpha1.MysqlFailoverGroup) (*v1alpha1.PlannedFailoverStatus,
		func(*v1alpha1.MysqlFailoverGroupStatus), bool) {
		cur := base.Status.PlannedFailover
		if cur == nil || cur.RunID != pf.RunID || plannedFailoverTerminal(cur) {
			return nil, nil, false
		}
		next := final.DeepCopy()
		next.Revision = cur.Revision + 1
		return next, e.terminalMutate(types.NamespacedName{Namespace: fg.Namespace, Name: fg.Name}, next, sites), true
	})
	if out != WriteCommitted {
		return plannedFailoverMarkerRetryInterval, nil
	}
	// The run was reported when the marker ended it.
	m.pendingTerminal = nil
	return plannedRequeueNext, nil
}

// plannedOwnRecordDurable reports whether the durable failover record of fg
// is the run's own, recognised by content (§4.4 check 1).
func plannedOwnRecordDurable(fg *v1alpha1.MysqlFailoverGroup, pf *v1alpha1.PlannedFailoverStatus, now time.Time) bool {
	return !plannedOwnRecordTime(fg, pf, now).IsZero()
}

// plannedOwnRecordTime is the time of the run's own failover record when
// the durable record of fg is it (§4.4 check 1's by-content rule), or zero.
func plannedOwnRecordTime(fg *v1alpha1.MysqlFailoverGroup, pf *v1alpha1.PlannedFailoverStatus, now time.Time) time.Time {
	if pf.Promotion != nil && pf.Promotion.ConfirmedAt != nil {
		return pf.Promotion.ConfirmedAt.Time
	}
	rec, _, err := EffectiveFailoverRecord(fg, now)
	if (err != nil && rec.IsZero()) || !ownFailoverRecord(fg, rec, pf) {
		return time.Time{}
	}
	return rec.LastFailover
}

// ---------- legacy (§13) ----------

// refuseLegacy ends a block an older operator wrote: nothing is hydrated.
func (e *PlannedFailoverEngine) refuseLegacy(ctx context.Context, fg *v1alpha1.MysqlFailoverGroup, pf *v1alpha1.PlannedFailoverStatus) (time.Duration, error) {
	nn := types.NamespacedName{Namespace: fg.Namespace, Name: fg.Name}
	outcome := v1alpha1.PlannedFailoverOutcomeNotStarted
	msg := fmt.Sprintf("planned failover in phase %q was written by an older operator and is refused; re-annotate to run it again", pf.Phase)
	switch pf.Phase {
	case plannedLegacyPhaseNone, plannedLegacyPhasePending, plannedLegacyPhaseValidating,
		v1alpha1.PlannedFailoverPhaseDeferred, v1alpha1.PlannedFailoverPhaseAdmitting:
	default:
		outcome = v1alpha1.PlannedFailoverOutcomeSourceLeftFenced
		msg = fmt.Sprintf("planned failover in phase %q was written by an older operator and is refused: "+
			"check which site is writable, then unfence the source or re-annotate", pf.Phase)
	}
	now := metav1.NewTime(e.clk.Now())
	start := now
	if !pf.StartTime.IsZero() {
		start = pf.StartTime
	}
	target := pf.Target
	if target == "" {
		target = "unknown"
	}
	// The older operator's sourcePrimary is pruned on read: the source is
	// not known, which is why the message asks a human to check.
	next := &v1alpha1.PlannedFailoverStatus{
		RunID: e.newRunID(), Revision: 1, Phase: v1alpha1.PlannedFailoverPhaseFailed,
		Target: target, Source: pf.Source, StartTime: start, Message: msg,
		Settings: plannedFailoverSettingsFor(fg, PlannedFailoverRequest{Site: target}),
		Result:   &v1alpha1.PlannedFailoverResult{Outcome: outcome, Reason: v1alpha1.PlannedFailoverReasonLegacyRunRefused, CompletionTime: now},
	}
	phase := pf.Phase
	out, _, _ := e.commit(ctx, fg, false, func(base *v1alpha1.MysqlFailoverGroup) (*v1alpha1.PlannedFailoverStatus,
		func(*v1alpha1.MysqlFailoverGroupStatus), bool) {
		cur := base.Status.PlannedFailover
		if cur == nil || !plannedLegacyBlock(cur) || cur.Phase != phase {
			return nil, nil, false
		}
		return next, nil, true
	})
	if out != WriteCommitted {
		return requeueFor(out), nil
	}
	log.FromContext(ctx).Info("planned failover: legacy status refused", "fg", nn.String(), "phase", string(phase))
	e.sink.RunTerminal(nn, next)
	e.sink.Event(fg, corev1.EventTypeWarning, "PlannedFailoverLegacyRefused", msg)
	return plannedRequeueNext, nil
}
