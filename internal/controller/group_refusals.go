package controller

import (
	"encoding/json"
	"fmt"
	"slices"
	"sort"
	"strings"

	corev1 "k8s.io/api/core/v1"
	apimeta "k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"

	v1alpha1 "github.com/shipstream/bloodraven/api/v1alpha1"
)

// A group refusal is something the operator refuses to act on for a
// group until a human fixes it, and does not retry in a loop: a site
// workload whose immutable selector differs from the one the operator
// renders (WorkloadSelectorMismatch), or an internal annotation payload
// with a schemaVersion the operator does not know
// (MetadataVersionUnsupported, A-80). The source that finds one records it
// with SetGroupRefusal, which emits a Warning Event of the same reason
// when the refusal is new or its message changed, and clears it with
// ClearGroupRefusal once the cause is gone. The group's status write
// reports the refusals as Degraded=True (refusalDegraded).
type groupRefusal struct {
	reason, message string
}

// refusalReasonRank orders the refusal reasons for the Degraded
// condition: a workload the operator cannot update outranks a payload it
// cannot read.
var refusalReasonRank = []string{
	v1alpha1.ReasonWorkloadSelectorMismatch,
	v1alpha1.ReasonMetadataVersionUnsupported,
}

// SetGroupRefusal records the refusal key of fg. A new refusal, or one
// whose reason or message changed, emits a Warning Event and asks the
// group's topology manager for a status write.
func (r *TopologyManagerRunner) SetGroupRefusal(fg *v1alpha1.MysqlFailoverGroup, key, reason, message string) {
	if r == nil || fg == nil {
		return
	}
	nn := FailoverGroupNamespacedName(fg)
	r.refusalMu.Lock()
	if r.refusals == nil {
		r.refusals = map[types.NamespacedName]map[string]groupRefusal{}
	}
	if r.refusals[nn] == nil {
		r.refusals[nn] = map[string]groupRefusal{}
	}
	want := groupRefusal{reason: reason, message: message}
	changed := r.refusals[nn][key] != want
	r.refusals[nn][key] = want
	r.refusalMu.Unlock()
	if !changed {
		return
	}
	if r.recorder != nil {
		r.recorder.Event(fg, corev1.EventTypeWarning, refusalEventReason(reason), message)
	}
	r.requestStatusRefresh(nn)
}

// ClearGroupRefusal removes the refusal key of nn, if recorded, and asks
// for a status write.
func (r *TopologyManagerRunner) ClearGroupRefusal(nn types.NamespacedName, key string) {
	if r == nil {
		return
	}
	r.refusalMu.Lock()
	_, had := r.refusals[nn][key]
	delete(r.refusals[nn], key)
	if len(r.refusals[nn]) == 0 {
		delete(r.refusals, nn)
	}
	r.refusalMu.Unlock()
	if had {
		r.requestStatusRefresh(nn)
	}
}

// GroupRefusalKeys returns the recorded refusal keys of nn with prefix,
// sorted.
func (r *TopologyManagerRunner) GroupRefusalKeys(nn types.NamespacedName, prefix string) []string {
	if r == nil {
		return nil
	}
	r.refusalMu.Lock()
	defer r.refusalMu.Unlock()
	var keys []string
	for k := range r.refusals[nn] {
		if strings.HasPrefix(k, prefix) {
			keys = append(keys, k)
		}
	}
	sort.Strings(keys)
	return keys
}

// pruneGroupRefusals drops the refusals of groups not in seen.
func (r *TopologyManagerRunner) pruneGroupRefusals(seen map[types.NamespacedName]struct{}) {
	r.refusalMu.Lock()
	defer r.refusalMu.Unlock()
	for nn := range r.refusals {
		if _, ok := seen[nn]; !ok {
			delete(r.refusals, nn)
		}
	}
}

// requestStatusRefresh asks nn's topology manager, if one runs, for a
// status write.
func (r *TopologyManagerRunner) requestStatusRefresh(nn types.NamespacedName) {
	r.mu.RLock()
	mt := r.managers[nn]
	r.mu.RUnlock()
	if mt != nil && mt.tm != nil {
		mt.tm.RequestStatusRefresh()
	}
}

// refusalEventReason is the Event reason of a refusal: the condition
// reason it mirrors.
func refusalEventReason(reason string) string {
	switch reason {
	case v1alpha1.ReasonWorkloadSelectorMismatch:
		return v1alpha1.EventWorkloadSelectorMismatch
	default:
		return v1alpha1.EventMetadataVersionUnsupported
	}
}

// refusalDegraded returns the Degraded=True reason and message for nn's
// refusals: the highest-ranked reason, with every refusal of that reason
// in the message (sorted by key). "" when there is none.
func (r *TopologyManagerRunner) refusalDegraded(nn types.NamespacedName) (reason, message string) {
	r.refusalMu.Lock()
	defer r.refusalMu.Unlock()
	set := r.refusals[nn]
	if len(set) == 0 {
		return "", ""
	}
	keys := make([]string, 0, len(set))
	for k := range set {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	for _, want := range refusalReasonRank {
		var msgs []string
		for _, k := range keys {
			if set[k].reason == want {
				msgs = append(msgs, set[k].message)
			}
		}
		if len(msgs) > 0 {
			return want, strings.Join(msgs, "; ")
		}
	}
	return "", ""
}

// setRefusalDegraded overwrites the Degraded condition with nn's
// refusals, if any, unless it already reports a topology alert: those
// stay. It runs after the per-site replication reasons, so a refusal
// outranks them; the divergence block, set after it, still wins.
func (r *TopologyManagerRunner) setRefusalDegraded(conds *[]metav1.Condition, nn types.NamespacedName, topologyReason string, gen int64, now metav1.Time) {
	if topologyReason != v1alpha1.ReasonHealthy && topologyReason != v1alpha1.ReasonInitializing {
		return
	}
	reason, message := r.refusalDegraded(nn)
	if reason == "" {
		return
	}
	if c := apimeta.FindStatusCondition(*conds, v1alpha1.ConditionDegraded); c != nil && c.Status == metav1.ConditionTrue &&
		!slices.Contains(refusalOverrides, c.Reason) {
		return
	}
	setCondition(conds, metav1.Condition{
		Type: v1alpha1.ConditionDegraded, Status: metav1.ConditionTrue,
		ObservedGeneration: gen, LastTransitionTime: now,
		Reason: reason, Message: message,
	})
}

// appendRefusalDetail keeps nn's refusals visible when a higher-ranked
// reason holds the Degraded condition (a topology alert, or a divergence
// block set after setRefusalDegraded): their messages, which carry the
// remedy, are appended to the condition's message. Call it after every
// other Degraded writer.
func (r *TopologyManagerRunner) appendRefusalDetail(conds *[]metav1.Condition, nn types.NamespacedName) {
	reason, message := r.refusalDegraded(nn)
	if reason == "" {
		return
	}
	c := apimeta.FindStatusCondition(*conds, v1alpha1.ConditionDegraded)
	if c == nil || c.Status != metav1.ConditionTrue || c.Reason == reason || strings.Contains(c.Message, message) {
		return
	}
	c.Message = strings.TrimSuffix(c.Message, ".") + ". Also refused (" + reason + "): " + message
}

// refusalOverrides are the Degraded=True reasons a refusal replaces: the
// per-site reasons, which a workload the operator cannot update or a
// payload it cannot read usually explains. Healthy and Initializing set
// Degraded False or Unknown, which a refusal replaces too.
var refusalOverrides = []string{
	v1alpha1.ReasonReplicationBroken,
	v1alpha1.ReasonReplicationError,
	v1alpha1.ReasonReplicationLagging,
	v1alpha1.ReasonReplicationSourceMismatch,
	v1alpha1.ReasonRestoreReclonePending,
}

// terminalMarkerRefusalKey is the refusal key of an unreadable
// planned-failover terminal marker.
const terminalMarkerRefusalKey = "annotation/" + v1alpha1.PlannedFailoverTerminalAnnotation

// terminalMarkerVersionProblem returns the refusal message for fg's
// planned-failover terminal marker when it is a JSON object whose
// schemaVersion the operator does not know, or "" otherwise. Such a
// marker proves no outcome (ParseTerminalMarker rejects it); one naming a
// run with outcome ManualRecoveryRequired still holds automation
// (parseHoldMarker fails closed). An object without schemaVersion has
// version 0. A marker that is not a JSON object, or whose schemaVersion
// is not an integer, is malformed rather than of an unknown version and
// is not reported here.
func terminalMarkerVersionProblem(fg *v1alpha1.MysqlFailoverGroup) string {
	raw, ok := fg.GetAnnotations()[v1alpha1.PlannedFailoverTerminalAnnotation]
	if !ok {
		return ""
	}
	version, known := payloadSchemaVersion(raw)
	if !known || version == TerminalMarkerSchemaVersion {
		return ""
	}
	effect := "it proves no outcome and is ignored"
	if m, holds := parseHoldMarker(raw); holds {
		effect = fmt.Sprintf("it names run %s with outcome ManualRecoveryRequired, so automatic failover stays held; "+
			"resolve the run, then annotate %s=%s", m.RunID, v1alpha1.AcknowledgePlannedFailoverHoldAnnotation, m.RunID)
	}
	return fmt.Sprintf("annotation %s has schemaVersion %d, which this operator does not know (it reads %d): %s",
		v1alpha1.PlannedFailoverTerminalAnnotation, version, TerminalMarkerSchemaVersion, effect)
}

// payloadSchemaVersion reads the schemaVersion member of a JSON object
// payload (A-80). known is false when raw is not a JSON object or its
// schemaVersion is not an integer; an object without the member has
// version 0, which no payload is written with.
func payloadSchemaVersion(raw string) (version int, known bool) {
	var probe map[string]json.RawMessage
	if err := json.Unmarshal([]byte(raw), &probe); err != nil || probe == nil {
		return 0, false
	}
	v, ok := probe["schemaVersion"]
	if !ok {
		return 0, true
	}
	if err := json.Unmarshal(v, &version); err != nil {
		return 0, false
	}
	return version, true
}
