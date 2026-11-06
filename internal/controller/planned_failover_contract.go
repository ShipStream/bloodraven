// Package controller — the planned-failover durable-claim contract.
//
// This file fixes the names, types and error contracts the planned-failover
// engine, its status store and its topology facade are built against: the run
// identity and records, the terminal marker and the automation hold, the
// status predicates, and the interfaces between the engine (store, topology,
// registry, sink). The engine is in planned_failover_engine.go and the claim
// slot in planned_failover_claim.go.
package controller

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"slices"
	"strings"
	"time"

	apimeta "k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"

	v1alpha1 "github.com/shipstream/bloodraven/api/v1alpha1"
)

// ---------- annotations ----------

const (
	// PlannedFailoverStatusFenceAnnotation is bumped (an ISO 8601 basic UTC
	// instant with nanoseconds, FormatStatusFenceValue) with a write
	// conditioned on the resourceVersion an ambiguous status write was made
	// against. Its success proves that write can never commit. Internal.
	PlannedFailoverStatusFenceAnnotation = "bloodraven.shipstream.io/planned-failover-status-fence"

	// PlannedFailoverTerminalAnnotation ends a run outside /status when its
	// status writes stall. Its value is a versioned JSON TerminalMarker
	// ({"schemaVersion":1,"runID":…,"outcome":…,"reason":…}). Every
	// planned-failover predicate treats the run it names as terminal.
	// Internal.
	PlannedFailoverTerminalAnnotation = "bloodraven.shipstream.io/planned-failover-terminal"

	// AcknowledgePlannedFailoverHoldAnnotation acknowledges an automation
	// hold. Its value is the held run's runID
	// (status.plannedFailoverHold.runID). Public; wrapped by kubectl
	// bloodraven ack-planned-failover-hold.
	AcknowledgePlannedFailoverHoldAnnotation = "bloodraven.shipstream.io/acknowledge-planned-failover-hold"

	// PromotionGtidExecutedAnnotation is the out-of-band copy of the
	// promotion watermark, beside LastFailoverAnnotation and
	// LastFailoverTargetAnnotation. Operator-owned read-only.
	PromotionGtidExecutedAnnotation = "bloodraven.shipstream.io/promotion-gtid-executed"

	// PlannedFailoverStallSinceAnnotation is the durable start of the
	// status-stall deadline: "<runID>:since=<YYYYMMDDThhmmssZ>"
	// (FormatDeadlineStart). Internal.
	PlannedFailoverStallSinceAnnotation = "bloodraven.shipstream.io/planned-failover-stall-since"

	// PlannedFailoverManagerAbsentSinceAnnotation is the durable start of
	// the manager-absence deadline, in the same form as
	// PlannedFailoverStallSinceAnnotation. Internal.
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
func failoverRecordRefFromAPI(r *v1alpha1.FailoverRecord) FailoverRecordRef {
	if r == nil {
		return FailoverRecordRef{}
	}
	var t time.Time
	if r.Time != nil {
		t = r.Time.Time
	}
	return newFailoverRecordRef(t, r.Target)
}

// toAPI converts to the persisted copy, at second precision. A zero time is
// omitted.
func (r FailoverRecordRef) toAPI() *v1alpha1.FailoverRecord {
	out := &v1alpha1.FailoverRecord{Target: r.LastFailoverTarget}
	if !r.LastFailover.IsZero() {
		t := metav1.NewTime(r.LastFailover.UTC().Truncate(time.Second))
		out.Time = &t
	}
	return out
}

// durableFailoverRecord is the newer of status and the out-of-band record
// (EffectiveFailoverRecord), as a FailoverRecordRef; err as EffectiveFailoverRecord.
func durableFailoverRecord(fg *v1alpha1.MysqlFailoverGroup, now time.Time) (FailoverRecordRef, error) {
	rec, _, err := EffectiveFailoverRecord(fg, now)
	return newFailoverRecordRef(rec.LastFailover, rec.LastFailoverTarget), err
}

// TerminalMarkerSchemaVersion is the only planned-failover-terminal payload
// version this operator writes and reads.
const TerminalMarkerSchemaVersion = 1

// TerminalMarker is the planned-failover-terminal annotation value.
type TerminalMarker struct {
	RunID   string
	Outcome v1alpha1.PlannedFailoverOutcome
	Reason  v1alpha1.PlannedFailoverReason
}

// terminalMarkerPayload is the annotation's JSON form.
type terminalMarkerPayload struct {
	SchemaVersion int                             `json:"schemaVersion"`
	RunID         string                          `json:"runID"`
	Outcome       v1alpha1.PlannedFailoverOutcome `json:"outcome"`
	Reason        v1alpha1.PlannedFailoverReason  `json:"reason,omitempty"`
}

// String renders the versioned JSON payload; the reason may be empty.
func (m TerminalMarker) String() string {
	b, err := json.Marshal(terminalMarkerPayload{SchemaVersion: TerminalMarkerSchemaVersion, RunID: m.RunID,
		Outcome: m.Outcome, Reason: m.Reason})
	if err != nil {
		// Unreachable: every member is a string or an int.
		panic(err)
	}
	return string(b)
}

// ParseTerminalMarker parses a TerminalMarker. false means malformed: not
// the JSON payload, an unknown schemaVersion, an empty runID, or an unknown
// outcome or reason. A malformed marker never proves anything: callers
// treat it as absent and log it.
func ParseTerminalMarker(v string) (TerminalMarker, bool) {
	var p terminalMarkerPayload
	dec := json.NewDecoder(strings.NewReader(v))
	dec.DisallowUnknownFields()
	if err := dec.Decode(&p); err != nil || p.SchemaVersion != TerminalMarkerSchemaVersion || p.RunID == "" {
		return TerminalMarker{}, false
	}
	// Anything after the object but whitespace is malformed. Decoder.More
	// is not enough: it reports false before a stray '}' or ']'.
	var rest json.RawMessage
	if err := dec.Decode(&rest); !errors.Is(err, io.EOF) {
		return TerminalMarker{}, false
	}
	m := TerminalMarker{RunID: p.RunID, Outcome: p.Outcome, Reason: p.Reason}
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
		v1alpha1.PlannedFailoverReasonCanceled,
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
	RunID      string
	Reason     v1alpha1.PlannedFailoverReason
	FromStatus bool // status.plannedFailoverHold names RunID (or a True AutomaticFailoverHeld condition stands without it)
	FromMarker bool // a terminal marker with outcome ManualRecoveryRequired names RunID
}

// plannedHoldConditionMessage is the human-readable message of a True
// AutomaticFailoverHeld condition, a summary of status.plannedFailoverHold.
// Nothing reads it back: the hold's fields are the record.
func plannedHoldConditionMessage(h *v1alpha1.PlannedFailoverHold) string {
	named := "none recorded"
	if len(h.AmbiguousSites) > 0 {
		named = strings.Join(h.AmbiguousSites, ", ")
	}
	return fmt.Sprintf("planned failover run %s ended ManualRecoveryRequired (%s); writable or ambiguous sites: %s. "+
		"Automatic failover is held: resolve the split brain by hand, then annotate %s=%s",
		h.RunID, h.Reason, named, AcknowledgePlannedFailoverHoldAnnotation, h.RunID)
}

// setPlannedHold records the automation hold h in status and sets the True
// AutomaticFailoverHeld condition that summarises it, stamped with
// generation (the generation of the object the terminal write is built on).
// Runner status writes preserve both: they are reconciler-owned.
func setPlannedHold(st *v1alpha1.MysqlFailoverGroupStatus, h v1alpha1.PlannedFailoverHold, generation int64) {
	st.PlannedFailoverHold = h.DeepCopy()
	apimeta.SetStatusCondition(&st.Conditions, metav1.Condition{
		Type:               v1alpha1.ConditionAutomaticFailoverHeld,
		Status:             metav1.ConditionTrue,
		Reason:             v1alpha1.ReasonPlannedFailoverManualRecoveryRequired,
		Message:            plannedHoldConditionMessage(&h),
		ObservedGeneration: generation,
		LastTransitionTime: h.HoldTime,
	})
}

// ClearPlannedHold removes the hold of runID from status and sets the
// AutomaticFailoverHeld condition False/PlannedFailoverHoldAcknowledged.
// false: status carries no hold of runID, and nothing changed.
func ClearPlannedHold(st *v1alpha1.MysqlFailoverGroupStatus, runID string, generation int64, now time.Time) bool {
	if st.PlannedFailoverHold == nil || st.PlannedFailoverHold.RunID != runID {
		return false
	}
	st.PlannedFailoverHold = nil
	apimeta.SetStatusCondition(&st.Conditions, metav1.Condition{
		Type:               v1alpha1.ConditionAutomaticFailoverHeld,
		Status:             metav1.ConditionFalse,
		Reason:             v1alpha1.ReasonPlannedFailoverHoldAcknowledged,
		Message:            fmt.Sprintf("the automation hold of planned failover run %s was acknowledged; automatic failover resumes", runID),
		ObservedGeneration: generation,
		LastTransitionTime: metav1.NewTime(now),
	})
	return true
}

// statusHoldNames reports whether status records the hold of runID.
func statusHoldNames(st *v1alpha1.MysqlFailoverGroupStatus, runID string) bool {
	return st.PlannedFailoverHold != nil && st.PlannedFailoverHold.RunID == runID
}

// deadlineStartLayout is the ISO 8601 basic UTC form of a deadline start
// (A-78: an instant inside an annotation value has no colons).
const deadlineStartLayout = "20060102T150405Z"

// FormatDeadlineStart renders a durable deadline start:
// "<runID>:since=<YYYYMMDDThhmmssZ>", at second precision.
func FormatDeadlineStart(runID string, start time.Time) string {
	return runID + ":since=" + start.UTC().Truncate(time.Second).Format(deadlineStartLayout)
}

// ParseDeadlineStart reads a FormatDeadlineStart value; false when it is
// absent or malformed.
func ParseDeadlineStart(v string) (runID string, start time.Time, ok bool) {
	id, pair, found := strings.Cut(v, ":")
	if !found || id == "" {
		return "", time.Time{}, false
	}
	key, ts, found := strings.Cut(pair, "=")
	if !found || key != "since" {
		return "", time.Time{}, false
	}
	t, err := time.Parse(deadlineStartLayout, ts)
	if err != nil {
		return "", time.Time{}, false
	}
	return id, t.UTC(), true
}

// statusFenceLayout is the ISO 8601 basic UTC form with nanoseconds, so
// that every bump changes the value.
const statusFenceLayout = "20060102T150405.000000000Z"

// FormatStatusFenceValue renders a planned-failover-status-fence bump.
func FormatStatusFenceValue(now time.Time) string {
	return now.UTC().Format(statusFenceLayout)
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
	m, ok := ParseTerminalMarker(fg.GetAnnotations()[PlannedFailoverTerminalAnnotation])
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

// PlannedRunNeedsClaim reports whether fg's run must hold the claim, which
// hydration (manager registration, EnsureClaim) re-establishes from the
// durable block: it owns the sites (plannedFailoverOwnsSites) and is not a
// Promoting block whose own failover record is already durable. Such a run
// released its claim once the promotion was recorded and published (the
// DST-4 ruling) and has only its Publishing write left: hydrating it back
// would defer automation again, and a re-proof after a later emergency
// failover would hold the group (O-m1). The engine drives it to Publishing
// without a claim.
func PlannedRunNeedsClaim(fg *v1alpha1.MysqlFailoverGroup, now time.Time) bool {
	if !plannedFailoverOwnsSites(fg) {
		return false
	}
	return !plannedPromotionRecorded(fg, fg.Status.PlannedFailover, now)
}

// plannedPromotionRecorded reports whether pf is a Promoting block whose
// own failover record (recognised by content) is the durable record of fg.
func plannedPromotionRecorded(fg *v1alpha1.MysqlFailoverGroup, pf *v1alpha1.PlannedFailoverStatus, now time.Time) bool {
	return pf != nil && pf.Phase == v1alpha1.PlannedFailoverPhasePromoting && plannedOwnRecordDurable(fg, pf, now)
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
	return fg.Status.PlannedFailover.SourceSite
}

// PlannedHoldFrom returns the durable evidence of an automation hold, or nil.
// Either record holds on its own: status.plannedFailoverHold, or a terminal
// marker with outcome ManualRecoveryRequired. A True AutomaticFailoverHeld
// condition without status.plannedFailoverHold (an inconsistent or
// hand-edited status) still holds, fail closed, with RunID "": no
// acknowledgement can match it, so an administrator removes it by hand.
// When both records name different runs the status hold's run is reported;
// the marker rebuilds its own hold once the status hold is gone.
func PlannedHoldFrom(fg *v1alpha1.MysqlFailoverGroup) *PlannedHold {
	if fg == nil {
		return nil
	}
	var hold *PlannedHold
	if h := fg.Status.PlannedFailoverHold; h != nil {
		hold = &PlannedHold{RunID: h.RunID, Reason: h.Reason, FromStatus: true}
	} else if c := apimeta.FindStatusCondition(fg.Status.Conditions, v1alpha1.ConditionAutomaticFailoverHeld); c != nil && c.Status == metav1.ConditionTrue {
		hold = &PlannedHold{FromStatus: true}
	}
	m, ok := ParseTerminalMarker(fg.GetAnnotations()[PlannedFailoverTerminalAnnotation])
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
	// Sent reports that the facade's memo records a SET super_read_only=ON
	// sent for the run (it may have applied, whatever Kind says). The
	// engine treats it as its own fence.sent: the fence is attributable to
	// the run, and the next attempt re-proves it rather than re-sending.
	Sent bool
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
	// RollbackIfUnrecorded marks an Inconsistent verdict for a target
	// found replaced (@@server_uuid ≠ the intent's, no memo) where the
	// source still holds all of F under the run's attributable fence and no
	// site is writable (the DST-3 ruling). The engine rolls back instead of
	// holding when the run's own failover record is not durable either: the
	// run never reached RecordPlannedPromotion, so DNS never pointed at the
	// target, and anything only the replaced pod could hold was written
	// after the fence (the async-DR RPO).
	RollbackIfUnrecorded bool
}

// ReproveResult: OK, or exactly one of RollBack, Hold, HandOff.
type ReproveResult struct {
	OK       bool
	RollBack *v1alpha1.PlannedFailoverReason // go to RollingBack with this reason
	Hold     *v1alpha1.PlannedFailoverReason // ManualRecoveryRequired{reason} plus a hold
	HandOff  *PromoteOutcome                 // apply the pre-write gate / confirm mapping
	// SourceUnreachable: the source did not answer the re-proof's identity
	// read before Promoting. Nothing is decided; the engine counts it
	// toward the source-loss window and re-proves on its next pass.
	SourceUnreachable bool
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
	// HeldRunClaim returns the run whose Run-mode claim the slot holds
	// (ok=false: empty, or a hold). The engine's zombie clear (§3.4) uses
	// it to release a claim hydrated for a run this process never saw live.
	HeldRunClaim(nn types.NamespacedName) (runID string, ok bool)
	// ManagerAuthority is the current manager's view of authority, which
	// the terminal Succeeded write merges into status beside the failover
	// record (§5.3); ok=false when no manager is registered.
	ManagerAuthority(nn types.NamespacedName) (PlannedAuthority, bool)

	// CheckRunClaim is §3.3's check for a side effect the facade does not
	// send itself (the Dragonfly takeover): the leader context is alive, a
	// manager is registered, and the slot holds ref's Run claim, re-proven
	// (errReproveRequired for a hydrated claim). It sends nothing.
	CheckRunClaim(ref PlannedRunRef) error

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

// PlannedAuthority is the slice of a topology manager's snapshot that the
// planned failover's terminal Succeeded write takes: the fields
// MergeTopologySnapshotStatus derives status.activeSite and
// status.topologyGeneration from.
type PlannedAuthority struct {
	ActiveSite           string
	DeploymentActiveSite string
	TopologyGeneration   int64
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

// ---------- engine ----------
//
// PlannedFailoverEngine, NewPlannedFailoverEngine and Step live in
// planned_failover_engine.go; their signatures are part of this contract.

// ---------- manager wiring (no-op until the claim lands) ----------

// ManagerWiring is everything a manager must see before Run.
type ManagerWiring struct {
	Slot                   *PlannedClaimSlot
	HydrateFrom            *v1alpha1.PlannedFailoverStatus // uncached, non-marked owns-sites run; nil = nothing to hydrate
	HoldFrom               *PlannedHold                    // nil = no hold
	PendingRotations       []string
	RetiringUpdateInFlight bool
	RetiringBusy           func() bool // nil when no retiring manager
	// Predecessors are the group's retiring managers. Their in-memory
	// failover records (an ordered-update handoff recorded on a manager
	// that is being replaced, say) are carried into the new manager under
	// the timestamp-monotonic rule before its Run (DST-1).
	Predecessors []*TopologyManager
	// RetiredFailover is the newest record a manager of the group held
	// when it retired, for a predecessor that has already left retiring.
	RetiredFailover *FailoverRecord
}

// WireRegisteredManager applies everything a manager must see before Run:
// pending rotation reservations, the predecessor guards, the planned-failover
// claim slot (hydrated uncached) and any promotion recorded on the claim.
// The runner's registerManager and the simulator's registry both call it.
func WireRegisteredManager(tm *TopologyManager, w ManagerWiring) { fillWireRegisteredManager(tm, w) }
