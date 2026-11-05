// Package controller — the planned-failover durable-claim contract.
//
// This file fixes the names, types and error contracts the planned-failover
// engine, its status store and its topology facade are built against: the run
// identity and records, the terminal marker and the automation hold, the
// status predicates, and the interfaces between the engine (store, topology,
// registry, sink). The engine and the claim slot below are placeholders until
// their implementations land.
package controller

import (
	"context"
	"errors"
	"fmt"
	"regexp"
	"slices"
	"strings"
	"time"

	apimeta "k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"

	v1alpha1 "github.com/shipstream/bloodraven/api/v1alpha1"
	"github.com/shipstream/bloodraven/internal/clock"
)

// ---------- annotations ----------

const (
	// PlannedFailoverStatusFenceAnnotation is bumped (RFC3339Nano) with a
	// write conditioned on the resourceVersion an ambiguous status write was
	// made against. Its success proves that write can never commit.
	PlannedFailoverStatusFenceAnnotation = "bloodraven.shipstream.io/planned-failover-status-fence"

	// PlannedFailoverTerminalAnnotation ends a run outside /status when its
	// status writes stall: "<runId>/<outcome>/<reason>" (TerminalMarker).
	// Every planned-failover predicate treats the run it names as terminal.
	PlannedFailoverTerminalAnnotation = "bloodraven.shipstream.io/planned-failover-terminal"

	// AckPlannedFailoverHoldAnnotation acknowledges an automation hold. Its
	// value is the held run's runId.
	AckPlannedFailoverHoldAnnotation = "bloodraven.shipstream.io/ack-planned-failover-hold"

	// PromotionGtidExecutedAnnotation is the out-of-band copy of the
	// promotion watermark, beside LastFailoverAnnotation and
	// LastFailoverTargetAnnotation.
	PromotionGtidExecutedAnnotation = "bloodraven.shipstream.io/promotion-gtid-executed"

	// PlannedFailoverStallSinceAnnotation is the durable start of the
	// status-stall deadline: "<runId>/<RFC3339>".
	PlannedFailoverStallSinceAnnotation = "bloodraven.shipstream.io/planned-failover-stall-since"

	// PlannedFailoverManagerAbsentSinceAnnotation is the durable start of
	// the manager-absence deadline: "<runId>/<RFC3339>".
	PlannedFailoverManagerAbsentSinceAnnotation = "bloodraven.shipstream.io/planned-failover-manager-absent-since"
)

// ---------- identity, records, markers ----------

// PlannedRunRef names the run a facade call acts for. Every mutating call
// checks it against the slot before its first statement.
type PlannedRunRef struct {
	Group  types.NamespacedName
	RunID  string
	Source string
	Target string
}

// FailoverRecordRef is {lastFailover, lastFailoverTarget} at second
// precision, UTC. Build it only with newFailoverRecordRef.
type FailoverRecordRef struct {
	LastFailover       time.Time
	LastFailoverTarget string
}

// newFailoverRecordRef truncates t to the second in UTC, the precision of
// both durable copies of the failover record.
func newFailoverRecordRef(t time.Time, target string) FailoverRecordRef {
	return FailoverRecordRef{LastFailover: t.UTC().Truncate(time.Second), LastFailoverTarget: target}
}

// sameFailoverRecord is the only comparison of failover records: the times
// at second precision (recordFailover stamps nanoseconds in memory, while
// both durable copies hold seconds) and the targets exactly.
func sameFailoverRecord(a, b FailoverRecordRef) bool {
	return a.LastFailover.UTC().Truncate(time.Second).Equal(b.LastFailover.UTC().Truncate(time.Second)) &&
		a.LastFailoverTarget == b.LastFailoverTarget
}

// failoverRecordRefFromAPI converts the persisted copy; nil is no history.
func failoverRecordRefFromAPI(r *v1alpha1.PlannedFailoverRecordRef) FailoverRecordRef {
	if r == nil {
		return FailoverRecordRef{}
	}
	var t time.Time
	if r.LastFailover != nil {
		t = r.LastFailover.Time
	}
	return newFailoverRecordRef(t, r.LastFailoverTarget)
}

// toAPI converts to the persisted copy, at second precision. A zero time is
// omitted.
func (r FailoverRecordRef) toAPI() *v1alpha1.PlannedFailoverRecordRef {
	out := &v1alpha1.PlannedFailoverRecordRef{LastFailoverTarget: r.LastFailoverTarget}
	if !r.LastFailover.IsZero() {
		t := metav1.NewTime(r.LastFailover.UTC().Truncate(time.Second))
		out.LastFailover = &t
	}
	return out
}

// durableFailoverRecord is the newer of status and the out-of-band record
// (EffectiveFailoverRecord), as a FailoverRecordRef; err as EffectiveFailoverRecord.
func durableFailoverRecord(fg *v1alpha1.MysqlFailoverGroup, now time.Time) (FailoverRecordRef, error) {
	rec, _, err := EffectiveFailoverRecord(fg, now)
	return newFailoverRecordRef(rec.LastFailover, rec.LastFailoverTarget), err
}

// TerminalMarker is the planned-failover-terminal annotation value.
type TerminalMarker struct {
	RunID   string
	Outcome v1alpha1.PlannedFailoverOutcome
	Reason  v1alpha1.PlannedFailoverReason
}

// String renders "<runId>/<outcome>/<reason>"; the reason may be empty.
func (m TerminalMarker) String() string {
	return m.RunID + "/" + string(m.Outcome) + "/" + string(m.Reason)
}

// parseTerminalMarker parses a TerminalMarker. false means malformed: an
// empty runId, an unknown outcome or reason, or the wrong number of parts.
// Callers treat a malformed marker as absent and log it.
func parseTerminalMarker(v string) (TerminalMarker, bool) {
	parts := strings.Split(v, "/")
	if len(parts) != 3 || parts[0] == "" {
		return TerminalMarker{}, false
	}
	m := TerminalMarker{
		RunID:   parts[0],
		Outcome: v1alpha1.PlannedFailoverOutcome(parts[1]),
		Reason:  v1alpha1.PlannedFailoverReason(parts[2]),
	}
	if !slices.Contains(plannedFailoverOutcomes, m.Outcome) {
		return TerminalMarker{}, false
	}
	if m.Reason != "" && !slices.Contains(plannedFailoverReasons, m.Reason) {
		return TerminalMarker{}, false
	}
	return m, true
}

// plannedFailoverOutcomes and plannedFailoverReasons are every value of the
// API enums, in their kubebuilder order.
var (
	plannedFailoverOutcomes = []v1alpha1.PlannedFailoverOutcome{
		v1alpha1.PlannedFailoverOutcomeSucceeded,
		v1alpha1.PlannedFailoverOutcomeNotStarted,
		v1alpha1.PlannedFailoverOutcomeRolledBack,
		v1alpha1.PlannedFailoverOutcomeSourceLeftFenced,
		v1alpha1.PlannedFailoverOutcomePromotedUnconfirmed,
		v1alpha1.PlannedFailoverOutcomeManualRecoveryRequired,
	}
	plannedFailoverReasons = []v1alpha1.PlannedFailoverReason{
		v1alpha1.PlannedFailoverReasonInvalidAnnotation,
		v1alpha1.PlannedFailoverReasonUnknownSite,
		v1alpha1.PlannedFailoverReasonTargetNotPromotable,
		v1alpha1.PlannedFailoverReasonAlreadyActive,
		v1alpha1.PlannedFailoverReasonTargetUnhealthy,
		v1alpha1.PlannedFailoverReasonDivergentTarget,
		v1alpha1.PlannedFailoverReasonKeyringRotation,
		v1alpha1.PlannedFailoverReasonCooldownActive,
		v1alpha1.PlannedFailoverReasonConcurrentOperation,
		v1alpha1.PlannedFailoverReasonInvalidFailoverState,
		v1alpha1.PlannedFailoverReasonCancelled,
		v1alpha1.PlannedFailoverReasonAdmissionTimeout,
		v1alpha1.PlannedFailoverReasonAuthorityChanged,
		v1alpha1.PlannedFailoverReasonLegacyRunRefused,
		v1alpha1.PlannedFailoverReasonFenceFailed,
		v1alpha1.PlannedFailoverReasonSourceLost,
		v1alpha1.PlannedFailoverReasonSourceUnfenced,
		v1alpha1.PlannedFailoverReasonTargetLost,
		v1alpha1.PlannedFailoverReasonForeignWritable,
		v1alpha1.PlannedFailoverReasonPeerUnreadable,
		v1alpha1.PlannedFailoverReasonLagTimeout,
		v1alpha1.PlannedFailoverReasonInvalidGTID,
		v1alpha1.PlannedFailoverReasonDragonflySyncTimeout,
		v1alpha1.PlannedFailoverReasonDragonflyPromotionFailed,
		v1alpha1.PlannedFailoverReasonTargetChanged,
		v1alpha1.PlannedFailoverReasonPromotionAborted,
		v1alpha1.PlannedFailoverReasonPromotionUnconfirmed,
		v1alpha1.PlannedFailoverReasonManagerUnavailable,
		v1alpha1.PlannedFailoverReasonStatusStalled,
		v1alpha1.PlannedFailoverReasonManualRecoveryPending,
		v1alpha1.PlannedFailoverReasonExecuteFailed,
		v1alpha1.PlannedFailoverReasonInternalError,
	}
)

// PlannedHold is the uncached evidence of an automation hold.
// ManagerWiring.HoldFrom carries it; nil means no hold.
type PlannedHold struct {
	RunID         string
	Reason        v1alpha1.PlannedFailoverReason
	FromCondition bool // the AutomaticFailoverHeld condition names RunID
	FromMarker    bool // a terminal marker with outcome ManualRecoveryRequired names RunID
}

// plannedHoldConditionMessage is the message of a True AutomaticFailoverHeld
// condition. It names the run (parsePlannedHoldConditionMessage reads it
// back), the sites that were writable or ambiguous, and the acknowledgement.
func plannedHoldConditionMessage(runID string, reason v1alpha1.PlannedFailoverReason, sites []string) string {
	named := "none recorded"
	if len(sites) > 0 {
		named = strings.Join(sites, ", ")
	}
	return fmt.Sprintf("planned failover run %s ended ManualRecoveryRequired (%s); writable or ambiguous sites: %s. "+
		"Automatic failover is held: resolve the split brain by hand, then annotate %s=%s",
		runID, reason, named, AckPlannedFailoverHoldAnnotation, runID)
}

var plannedHoldConditionMessageRE = regexp.MustCompile(`^planned failover run (\S+) ended ManualRecoveryRequired \(([A-Za-z]*)\)`)

// parsePlannedHoldConditionMessage reads the run and reason back from a
// plannedHoldConditionMessage.
func parsePlannedHoldConditionMessage(msg string) (runID string, reason v1alpha1.PlannedFailoverReason, ok bool) {
	m := plannedHoldConditionMessageRE.FindStringSubmatch(msg)
	if m == nil {
		return "", "", false
	}
	return m[1], v1alpha1.PlannedFailoverReason(m[2]), true
}

// ---------- predicates; defined here, wired by the engine and the claim ----------

// plannedFailoverRun returns the planned-failover block and whether the
// terminal marker names its run. A marked run is terminal whatever its
// phase says.
func plannedFailoverRun(fg *v1alpha1.MysqlFailoverGroup) (pf *v1alpha1.PlannedFailoverStatus, marked bool) {
	if fg == nil || fg.Status.PlannedFailover == nil {
		return nil, false
	}
	pf = fg.Status.PlannedFailover
	if pf.RunID == "" {
		return pf, false
	}
	m, ok := parseTerminalMarker(fg.GetAnnotations()[PlannedFailoverTerminalAnnotation])
	return pf, ok && m.RunID == pf.RunID
}

// plannedFailoverPending: the run is Admitting or Deferred and not marked.
// It owns nothing in status. The slot may hold a provisional grant between
// the grant and a committed Fencing write; consumers that must exclude it
// also ask Runner.PlannedClaimHeld.
func plannedFailoverPending(fg *v1alpha1.MysqlFailoverGroup) bool {
	pf, marked := plannedFailoverRun(fg)
	if pf == nil || marked {
		return false
	}
	switch pf.Phase {
	case v1alpha1.PlannedFailoverPhaseAdmitting, v1alpha1.PlannedFailoverPhaseDeferred:
		return true
	}
	return false
}

// plannedFailoverOwnsSites: the run holds the claim durably (Fencing
// through Promoting, or RollingBack) and is not marked.
func plannedFailoverOwnsSites(fg *v1alpha1.MysqlFailoverGroup) bool {
	pf, marked := plannedFailoverRun(fg)
	if pf == nil || marked {
		return false
	}
	switch pf.Phase {
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

// plannedFailoverActive: the run is not terminal (pending, owning sites, or
// Publishing) and not marked. Use it only for informational and
// conservative refusals.
func plannedFailoverActive(fg *v1alpha1.MysqlFailoverGroup) bool {
	if plannedFailoverPending(fg) || plannedFailoverOwnsSites(fg) {
		return true
	}
	pf, marked := plannedFailoverRun(fg)
	return pf != nil && !marked && pf.Phase == v1alpha1.PlannedFailoverPhasePublishing
}

// plannedFailoverFencedSite returns the run's source while the run owns
// sites; otherwise "".
func plannedFailoverFencedSite(fg *v1alpha1.MysqlFailoverGroup) string {
	if !plannedFailoverOwnsSites(fg) {
		return ""
	}
	return fg.Status.PlannedFailover.Source
}

// plannedHoldFrom returns the durable evidence of an automation hold, or nil.
// Either record holds on its own: a True AutomaticFailoverHeld condition, or
// a terminal marker with outcome ManualRecoveryRequired. A True condition
// whose message does not name a run still holds (RunID ""): no
// acknowledgement can match it, so an administrator removes it by hand.
// When both records name different runs the condition's run is reported;
// the marker rebuilds its own hold once the condition is gone.
func plannedHoldFrom(fg *v1alpha1.MysqlFailoverGroup) *PlannedHold {
	if fg == nil {
		return nil
	}
	var hold *PlannedHold
	if c := apimeta.FindStatusCondition(fg.Status.Conditions, v1alpha1.ConditionAutomaticFailoverHeld); c != nil && c.Status == metav1.ConditionTrue {
		runID, reason, _ := parsePlannedHoldConditionMessage(c.Message)
		hold = &PlannedHold{RunID: runID, Reason: reason, FromCondition: true}
	}
	m, ok := parseTerminalMarker(fg.GetAnnotations()[PlannedFailoverTerminalAnnotation])
	if !ok || m.Outcome != v1alpha1.PlannedFailoverOutcomeManualRecoveryRequired {
		return hold
	}
	switch {
	case hold == nil:
		return &PlannedHold{RunID: m.RunID, Reason: m.Reason, FromMarker: true}
	case hold.RunID == m.RunID:
		hold.FromMarker = true
		if hold.Reason == "" {
			hold.Reason = m.Reason
		}
	}
	return hold
}

// ---------- sentinels ----------

var (
	errManagerAbsent   = errors.New("planned failover: no topology manager registered")
	errClaimLost       = errors.New("planned failover: the claim is not held by this run")
	errLeaderLost      = errors.New("planned failover: leadership lost")
	errReproveRequired = errors.New("planned failover: hydrated claim must re-prove first")
	errGateBusy        = errors.New("planned failover: follower mutation in flight") // internal: maps to Wait{RecoveryInFlight}
)

// ---------- store ----------

// WriteOutcome classifies a conditioned write. err is nil iff the outcome is
// WriteCommitted; otherwise it carries the cause for logs.
type WriteOutcome int

const (
	WriteCommitted    WriteOutcome = iota // 2xx, or settled Committed: the announced side effect may run
	WriteConflict                         // 409: nothing committed; reload and recompute (≤3 per pass), then requeue 1s
	WriteNotCommitted                     // 403/422, or settled NotCommitted: proven not committed
	WriteGone                             // 404: the CR is gone; the run ends
	WriteUnknown                          // Ambiguous and unsettled: no side effect, keep the claim, requeue 2s
)

func (o WriteOutcome) String() string {
	switch o {
	case WriteCommitted:
		return "Committed"
	case WriteConflict:
		return "Conflict"
	case WriteNotCommitted:
		return "NotCommitted"
	case WriteGone:
		return "Gone"
	case WriteUnknown:
		return "Unknown"
	}
	return fmt.Sprintf("WriteOutcome(%d)", int(o))
}

// DeadlineKind names a durable deadline start.
type DeadlineKind string

const (
	DeadlineStatusStall   DeadlineKind = "stall"          // planned-failover-stall-since
	DeadlineManagerAbsent DeadlineKind = "manager-absent" // planned-failover-manager-absent-since
)

// Annotation is the annotation that holds the deadline start; "" for an
// unknown kind.
func (k DeadlineKind) Annotation() string {
	switch k {
	case DeadlineStatusStall:
		return PlannedFailoverStallSinceAnnotation
	case DeadlineManagerAbsent:
		return PlannedFailoverManagerAbsentSinceAnnotation
	}
	return ""
}

// PlannedFailoverStore is the run's only path to the API server. Reads are
// uncached (APIReader); writes are conditioned on base.ResourceVersion.
// Implementations: apiPlannedFailoverStore, simPlannedStore and the
// component in-memory store.
type PlannedFailoverStore interface {
	// Load returns a deep copy of the uncached object; (nil, nil) on
	// NotFound. Any other error: requeue, nothing decided.
	Load(ctx context.Context, nn types.NamespacedName) (*v1alpha1.MysqlFailoverGroup, error)
	// Write issues Status().Update of base with status.plannedFailover = next
	// and mutate applied (terminal writes: the failover record and the hold
	// condition; nil otherwise). An Ambiguous response is settled inside
	// Write, so the caller never sees "Ambiguous".
	Write(ctx context.Context, base *v1alpha1.MysqlFailoverGroup, next v1alpha1.PlannedFailoverStatus,
		mutate func(*v1alpha1.MysqlFailoverGroupStatus)) (WriteOutcome, error)
	// Settle decides an ambiguous write of (runID, revision) made against
	// baseRV: detached from ctx's deadline, 10s per attempt, at most 3
	// attempts. Exported on the interface for tests.
	Settle(ctx context.Context, nn types.NamespacedName, baseRV, runID string, revision int64) (WriteOutcome, error)

	// Metadata writes: MergeFromWithOptimisticLock on base, never /status.
	// WriteConflict means reload and retry.
	WriteTerminalMarker(ctx context.Context, base *v1alpha1.MysqlFailoverGroup, m TerminalMarker) (WriteOutcome, error)
	ClearTerminalMarker(ctx context.Context, base *v1alpha1.MysqlFailoverGroup, runID string) (WriteOutcome, error) // only if it names runID
	// ConsumeRequest removes the request annotation; only after Admitting committed.
	ConsumeRequest(ctx context.Context, base *v1alpha1.MysqlFailoverGroup) (WriteOutcome, error)
	// MarkDeadlineStart records now for (kind, runID) unless a value for the
	// same runID exists, and returns the durable start (never moved forward).
	MarkDeadlineStart(ctx context.Context, base *v1alpha1.MysqlFailoverGroup, kind DeadlineKind, runID string,
		now time.Time) (time.Time, WriteOutcome, error)
	ClearDeadlineStart(ctx context.Context, base *v1alpha1.MysqlFailoverGroup, kind DeadlineKind, runID string) (WriteOutcome, error)
	// ClearHold commits, in one conditioned status write, the terminal record
	// final (when base does not carry it yet; nil otherwise) and the removal
	// of the AutomaticFailoverHeld condition naming runID.
	ClearHold(ctx context.Context, base *v1alpha1.MysqlFailoverGroup, runID string,
		final *v1alpha1.PlannedFailoverStatus) (WriteOutcome, error)
	DeleteHoldAck(ctx context.Context, base *v1alpha1.MysqlFailoverGroup, runID string) (WriteOutcome, error)
}

// ---------- topology facade ----------

// PlannedClaimRequest asks the topology manager for the planned-failover
// claim.
type PlannedClaimRequest struct {
	RunID           string
	Target          string
	DurableBaseline FailoverRecordRef // durableFailoverRecord of this pass's uncached read
}

// ClaimDecisionKind is the kind of a PlannedClaimDecision.
type ClaimDecisionKind int

const (
	ClaimGranted ClaimDecisionKind = iota
	ClaimWait
	ClaimReject
)

// PlannedClaimDecision is the topology manager's answer to a claim request.
type PlannedClaimDecision struct {
	Kind             ClaimDecisionKind
	Source           string                             // Granted
	SourceServerUUID string                             // Granted: the grant probe's pooled ReadGTIDSnapshot
	Baseline         FailoverRecordRef                  // Granted: persisted as claimBaseline
	WaitReason       v1alpha1.PlannedFailoverWaitReason // Wait
	RejectReason     v1alpha1.PlannedFailoverReason     // Reject
	Message          string
}

// ClaimReleaseReason records why a claim was released.
type ClaimReleaseReason string

const (
	ReleaseNotCommitted     ClaimReleaseReason = "notCommitted"
	ReleaseAdmissionTimeout ClaimReleaseReason = "admissionTimeout"
	ReleasePublishing       ClaimReleaseReason = "publishing"
	ReleaseTerminal         ClaimReleaseReason = "terminal"
	ReleaseToHold           ClaimReleaseReason = "toHold"
	ReleaseAck              ClaimReleaseReason = "ack"
	ReleaseZombie           ClaimReleaseReason = "zombie"
	ReleaseManagerAbsent    ClaimReleaseReason = "managerAbsent"
	ReleaseStatusStalled    ClaimReleaseReason = "statusStalled"
)

// FenceAttribution says how a super_read_only=ON on the source was
// attributed to the run.
type FenceAttribution int

const (
	FenceUnattributed    FenceAttribution = iota
	FenceByMemo                           // same process: fenceMemo[runId].sent
	FenceByIntentAndUUID                  // cross process: durable intent + matching server_uuid
)

// FenceResultKind is the kind of a FenceProof.
type FenceResultKind int

const (
	FenceApplied           FenceResultKind = iota // the proof below holds
	FenceNotSent                                  // authority moved before the SET: see Reason
	FenceForeignWritable                          // the post-fence proof saw a writable peer
	FenceNotAttributable                          // re-run: super_read_only=ON is not the run's
	FenceRetry                                    // SET or proof failed with the source reachable; retry until fenceDeadline
	FenceSourceUnreachable                        // counts toward the source-loss window
)

// FenceProof is the result of a fence attempt or of re-proving a fence.
type FenceProof struct {
	Kind             FenceResultKind
	Reason           v1alpha1.PlannedFailoverReason
	SourceWritable   bool // FenceNotSent: RolledBack vs SourceLeftFenced
	SourceServerUUID string
	GtidSet          string // F, when Applied
	Attribution      FenceAttribution
	UnreadablePeers  []string // recorded as UnreachablePeers; enforced at the gate
}

// LagProbe is one observation of the target catching up with the fenced set.
type LagProbe struct {
	SourceReachable bool
	TargetReachable bool
	CaughtUp        bool // target GTID ⊇ F and the applier is idle
	Divergent       bool // target holds GTIDs outside F (DivergentTarget)
	TargetGtid      string
}

// PromotionStage is the target's promotion stage classified from its live
// state on a pinned session.
type PromotionStage string

const (
	PromotionStageNotStarted     PromotionStage = "NotStarted"
	PromotionStageReplicaStopped PromotionStage = "ReplicaStopped"
	PromotionStageReplicaReset   PromotionStage = "ReplicaReset"
	PromotionStageWasWritable    PromotionStage = "WasWritable"
	PromotionStageWritable       PromotionStage = "Writable"
	PromotionStageTargetReplaced PromotionStage = "TargetReplaced"
	PromotionStageInconsistent   PromotionStage = "Inconsistent"
)

// PromoteOutcomeKind is the kind of a PromoteOutcome.
type PromoteOutcomeKind int

const (
	PromoteConfirmed    PromoteOutcomeKind = iota // the confirmed tail runs next
	PromoteAborted                                // nothing written by this attempt: RollingBack{Reason} unless Terminal is set
	PromoteUnconfirmed                            // stay Promoting; PromotedUnconfirmed past the window or deadline
	PromoteInconsistent                           // ManualRecoveryRequired{Reason} plus a hold
)

// PromoteOutcome is the result of one promotion attempt.
type PromoteOutcome struct {
	Kind             PromoteOutcomeKind
	Stage            PromotionStage
	Reason           v1alpha1.PlannedFailoverReason
	Terminal         v1alpha1.PlannedFailoverOutcome // non-empty: a direct terminal outcome
	Warnings         []v1alpha1.PlannedFailoverWarning
	TargetServerUUID string
	TargetReachable  bool // feeds the target-loss window
	MayHaveWritten   bool // the memo, or the stage, says SRO=OFF may have been sent
}

// ReproveResult: OK, or exactly one of RollBack, Hold, HandOff.
type ReproveResult struct {
	OK       bool
	RollBack *v1alpha1.PlannedFailoverReason // go to RollingBack with this reason
	Hold     *v1alpha1.PlannedFailoverReason // ManualRecoveryRequired{reason} plus a hold
	HandOff  *PromoteOutcome                 // apply the pre-write gate / confirm mapping
}

// RollbackResult is the result of one rollback attempt.
type RollbackResult struct {
	Outcome  v1alpha1.PlannedFailoverOutcome // "": not finished; retry until rollbackDeadline
	Reason   v1alpha1.PlannedFailoverReason
	Warnings []v1alpha1.PlannedFailoverWarning
}

// PlannedFailoverTopology is the engine's only path to topology managers and
// MySQL. Implemented by runnerPlannedTopology over a ManagerRegistry.
//
// Error contract for every method that takes a PlannedRunRef: errLeaderLost,
// errClaimLost, errReproveRequired and errManagerAbsent mean nothing was
// sent; any other error is a MySQL or transport failure that the engine
// counts toward the phase's loss window or deadline. Probe-only methods need
// no claim. ctx carries the pass bound.
type PlannedFailoverTopology interface {
	ClaimPlannedFailover(ctx context.Context, nn types.NamespacedName, req PlannedClaimRequest) (PlannedClaimDecision, error)
	EnsureClaim(ctx context.Context, ref PlannedRunRef, fg *v1alpha1.MysqlFailoverGroup) (PlannedClaimDecision, error)
	ReleasePlannedFailoverClaim(nn types.NamespacedName, runID string, why ClaimReleaseReason) bool
	ConvertToHold(nn types.NamespacedName, runID string) bool // why=toHold
	PlannedClaimHeld(nn types.NamespacedName) bool

	Reprove(ctx context.Context, ref PlannedRunRef, fg *v1alpha1.MysqlFailoverGroup) (ReproveResult, error)
	Fence(ctx context.Context, ref PlannedRunRef, sourceServerUUID string) (FenceProof, error)
	ProveFence(ctx context.Context, ref PlannedRunRef, fence v1alpha1.PlannedFailoverFenceRecord) (FenceProof, error)
	KillAppConnections(ctx context.Context, ref PlannedRunRef) (remaining int32, err error)
	ProbeLag(ctx context.Context, ref PlannedRunRef, fenced string) (LagProbe, error)
	ReadTargetServerUUID(ctx context.Context, ref PlannedRunRef) (string, error) // pinned; for the Promoting intent
	Promote(ctx context.Context, ref PlannedRunRef, pf *v1alpha1.PlannedFailoverStatus) (PromoteOutcome, error)
	RecordPlannedPromotion(ctx context.Context, ref PlannedRunRef, confirmedAt time.Time, watermark string) error
	ApplyDNS(ctx context.Context, ref PlannedRunRef) (v1alpha1.PlannedFailoverDNSState, error)
	NameUnconfirmedTarget(ref PlannedRunRef) error // errManagerAbsent: no name recorded
	NameFencedSource(ref PlannedRunRef) error      // errManagerAbsent: restoreFailoverState names it later
	Rollback(ctx context.Context, ref PlannedRunRef, pf *v1alpha1.PlannedFailoverStatus) (RollbackResult, error)
	ActiveSite(nn types.NamespacedName) (site string, ok bool)
}

// ManagerRegistry is what the facade needs from the runner. Implemented by
// TopologyManagerRunner and simRegistry.
type ManagerRegistry interface {
	// Snapshot is one r.mu.RLock; the caller inspects the managers with no
	// registry lock held.
	Snapshot(nn types.NamespacedName) RegistrySnapshot
	// Slot is created on first use and never nil.
	Slot(nn types.NamespacedName) *PlannedClaimSlot
	// LeaderContext is cancelled when leadership is lost.
	LeaderContext() context.Context
}

// RegistrySnapshot is the registry state for one group.
type RegistrySnapshot struct {
	Current  *TopologyManager   // nil: no registered manager
	Retiring []*TopologyManager // copy
	Slot     *PlannedClaimSlot
}

// PlannedFailoverSink receives events and metrics. Production: the
// reconciler's Recorder and internal/metrics; tests: a recorder fake.
type PlannedFailoverSink interface {
	Event(fg *v1alpha1.MysqlFailoverGroup, eventType, reason, message string)
	// RunTerminal records bloodraven_planned_failovers_total{target_site,
	// result=mapOutcomeToMetricResult(pf.Result)} and the duration histogram.
	RunTerminal(nn types.NamespacedName, pf *v1alpha1.PlannedFailoverStatus)
	LagWaitObserved(nn types.NamespacedName, target string, d time.Duration)
	Deferred(nn types.NamespacedName, reason v1alpha1.PlannedFailoverDeferReason)
}

// SimHooks is nil-Yield in production.
type SimHooks struct {
	Yield func(point string)
}

// ---------- engine (placeholder; the signatures are final) ----------

// PlannedFailoverEngine drives planned-failover runs through the store and
// the topology facade, one bounded step per call. This placeholder does
// nothing; nothing constructs it yet.
type PlannedFailoverEngine struct{}

var errPlannedFailoverEngineUnimplemented = errors.New("planned failover: engine not implemented")

// NewPlannedFailoverEngine returns an engine over store, topo and sink.
func NewPlannedFailoverEngine(store PlannedFailoverStore, topo PlannedFailoverTopology, sink PlannedFailoverSink,
	clk clock.Clock, hooks SimHooks) *PlannedFailoverEngine {
	return &PlannedFailoverEngine{}
}

// Step runs one pass for nn and returns when to run the next.
func (e *PlannedFailoverEngine) Step(ctx context.Context, nn types.NamespacedName) (requeueAfter time.Duration, err error) {
	return 0, errPlannedFailoverEngineUnimplemented
}

// ---------- manager wiring (no-op until the claim lands) ----------

// PlannedClaimSlot is a group's planned-failover claim, shared by every
// topology-manager generation of the group. Placeholder until the claim
// lands; the field gives it a non-zero size, so distinct slots never share
// an address.
type PlannedClaimSlot struct {
	_ byte
}

// ManagerWiring is everything a manager must see before Run.
type ManagerWiring struct {
	Slot                   *PlannedClaimSlot
	HydrateFrom            *v1alpha1.PlannedFailoverStatus // uncached, non-marked owns-sites run; nil = nothing to hydrate
	HoldFrom               *PlannedHold                    // nil = no hold
	PendingRotations       []string
	RetiringUpdateInFlight bool
	RetiringBusy           func() bool // nil when no retiring manager
}

// WireRegisteredManager applies everything a manager must see before Run:
// pending rotation reservations, the predecessor guards, the planned-failover
// claim slot (hydrated uncached) and any promotion recorded on the claim.
// No-op until the claim lands; nothing calls it yet.
func WireRegisteredManager(tm *TopologyManager, w ManagerWiring) {}
