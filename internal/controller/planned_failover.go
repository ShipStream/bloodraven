// Package controller — planned (admin-triggered) failover.
//
// Planned failover is a graceful switchover requested by an operator
// via an annotation on the MysqlFailoverGroup. Unlike emergency
// failover, the source is still writable — we fence it, wait for the
// target to catch up on the source's fenced GTID, and only then
// promote. The machinery reuses FailoverController.Execute for the
// destructive steps so planned and emergency paths converge.
//
// This file owns the annotation grammar and the up-front validation.
// The state-machine driver lives in planned_failover_reconciler.go.
package controller

import (
	"fmt"
	"regexp"
	"strings"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	v1alpha1 "github.com/shipstream/bloodraven/api/v1alpha1"
)

const (
	// PlannedFailoverAnnotation triggers a graceful, cooldown-respecting
	// switchover to the named primary-candidate site. Consumed and
	// cleared on the next reconcile, like RecloneAnnotation. The
	// annotation value is either a bare site name or a site followed by
	// key=value overrides separated by ':'.
	//
	// Examples:
	//   bloodraven.shipstream.io/planned-failover=pdx
	//   bloodraven.shipstream.io/planned-failover=pdx:maxLagWait=30s
	PlannedFailoverAnnotation = "bloodraven.shipstream.io/planned-failover"

	// defaultPlannedFailoverMaxLagWait is the fallback maxLagWait when
	// neither spec.plannedFailover.maxLagWait nor the annotation
	// override is supplied.
	defaultPlannedFailoverMaxLagWait = 5 * time.Minute

	// defaultPlannedFailoverDrainTimeout is the fallback drainTimeout
	// when spec.plannedFailover.drainTimeout is not supplied.
	defaultPlannedFailoverDrainTimeout = 30 * time.Second
)

// PlannedFailoverRequest is the parsed form of the
// bloodraven.shipstream.io/planned-failover annotation value.
type PlannedFailoverRequest struct {
	// Site is the promotion target.
	Site string

	// MaxLagWait is the per-request override for the zero-lag wait
	// timeout, or zero when the annotation did not include one. The
	// effective timeout applied by the state machine is the first
	// non-zero value among: this field, spec.plannedFailover.maxLagWait,
	// defaultPlannedFailoverMaxLagWait.
	MaxLagWait time.Duration
}

// annotationValueRE is A-78's value charset for one-shot action
// annotations: a value never contains ':' or '=', so the pair split is
// unambiguous.
var annotationValueRE = regexp.MustCompile(`^[A-Za-z0-9._/+-]+$`)

// parsePlannedFailoverAnnotation parses a planned-failover annotation
// value. The grammar is A-78's <subject>(:<key>=<value>)*:
//
//	<siteName>
//	<siteName>:maxLagWait=<duration>
//
// The subject is the target site. Each pair splits at its first '=', its
// value matches ^[A-Za-z0-9._/+-]+$, and a key appears at most once.
// Whitespace around the whole value is trimmed (a shell heredoc's trailing
// newline). Unknown keys are rejected so typos do not silently accept a
// request with default knobs. Returns a PlannedFailoverRequest on success;
// an error on an empty pair, an unknown or repeated key, or a malformed
// value.
func parsePlannedFailoverAnnotation(raw string) (PlannedFailoverRequest, error) {
	v := strings.TrimSpace(raw)
	if v == "" {
		return PlannedFailoverRequest{}, fmt.Errorf(
			"planned-failover annotation is empty; expected <site>[:maxLagWait=<duration>]")
	}

	parts := strings.Split(v, ":")
	req := PlannedFailoverRequest{Site: parts[0]}
	if req.Site != "" && !annotationValueRE.MatchString(req.Site) {
		return PlannedFailoverRequest{}, fmt.Errorf(
			"planned-failover annotation: site %q contains characters outside [A-Za-z0-9._/+-]", req.Site)
	}

	seen := map[string]bool{}
	for _, kv := range parts[1:] {
		key, val, ok := strings.Cut(kv, "=")
		if !ok || key == "" {
			return PlannedFailoverRequest{}, fmt.Errorf(
				"planned-failover annotation override %q must be key=value", kv)
		}
		if seen[key] {
			return PlannedFailoverRequest{}, fmt.Errorf(
				"planned-failover annotation: key %q appears more than once", key)
		}
		seen[key] = true
		if !annotationValueRE.MatchString(val) {
			return PlannedFailoverRequest{}, fmt.Errorf(
				"planned-failover annotation: %s=%q: the value must match [A-Za-z0-9._/+-]+", key, val)
		}
		switch key {
		case "maxLagWait":
			d, err := time.ParseDuration(val)
			if err != nil {
				return PlannedFailoverRequest{}, fmt.Errorf(
					"planned-failover annotation: maxLagWait=%q is not a valid duration: %w", val, err)
			}
			if d <= 0 {
				return PlannedFailoverRequest{}, fmt.Errorf(
					"planned-failover annotation: maxLagWait must be positive (got %s)", d)
			}
			req.MaxLagWait = d
		default:
			return PlannedFailoverRequest{}, fmt.Errorf(
				"planned-failover annotation: unknown key %q (supported: maxLagWait)", key)
		}
	}

	return req, nil
}

// PlannedFailoverValidationResult distinguishes the three possible
// validator outcomes so callers can emit the right Kubernetes Event.
type PlannedFailoverValidationResult int

const (
	// PlannedFailoverAccept — the request is well-formed and the
	// cluster is in a state where the state machine can proceed.
	PlannedFailoverAccept PlannedFailoverValidationResult = iota

	// PlannedFailoverSkip — the request is valid but trivially a no-op
	// (target is already the active site). No state machine is run;
	// callers should emit PlannedFailoverSkipped and clear the
	// annotation.
	PlannedFailoverSkip

	// PlannedFailoverReject — the request is invalid or the cluster is
	// in an inappropriate state. Callers should emit
	// PlannedFailoverRejected with the error's message and clear the
	// annotation.
	PlannedFailoverReject
)

// validatePlannedFailoverRequest checks the static pre-conditions that
// can be evaluated from the CR alone (spec + status). The engine calls it
// on every admission pass with the pass's uncached snapshot; it never
// grants anything (only the claim does), it only refuses. Everything
// time-sensitive (cooldown, clock) is evaluated via the supplied now
// argument so tests can drive the boundary deterministically.
//
// Returns (PlannedFailoverAccept, "", nil) on success, (PlannedFailoverSkip,
// AlreadyActive, nil) when the target already matches status.activeSite, or
// (PlannedFailoverReject, reason, err) with an error message suitable for an
// event when something is wrong. allowCurrentRun lets the request's own
// non-terminal run pass the concurrent-run check.
func validatePlannedFailoverRequest(fg *v1alpha1.MysqlFailoverGroup, req PlannedFailoverRequest, now time.Time, allowCurrentRun bool) (PlannedFailoverValidationResult, v1alpha1.PlannedFailoverReason, error) {
	if req.Site == "" {
		return PlannedFailoverReject, v1alpha1.PlannedFailoverReasonInvalidAnnotation, fmt.Errorf(
			"planned-failover annotation is empty; expected <site>[:maxLagWait=<duration>]")
	}

	// Target must name a site in spec.sites and must be a
	// primary-candidate. dr-only sites are never auto-promoted, not
	// even by explicit admin request (the DR promotion story is
	// wishlist #7 and has separate semantics).
	var targetSpec *v1alpha1.SiteSpec
	for i := range fg.Spec.Sites {
		if fg.Spec.Sites[i].Name == req.Site {
			targetSpec = &fg.Spec.Sites[i]
			break
		}
	}
	if targetSpec == nil {
		return PlannedFailoverReject, v1alpha1.PlannedFailoverReasonUnknownSite, fmt.Errorf(
			"planned-failover: unknown site %q; must match one of spec.sites[].name", req.Site)
	}
	if !targetSpec.IsPromotable() {
		return PlannedFailoverReject, v1alpha1.PlannedFailoverReasonTargetNotPromotable, fmt.Errorf(
			"planned-failover: site %q has role %q; only primary-candidate sites may be promoted",
			req.Site, targetSpec.EffectiveRole())
	}

	// Idempotent no-op: target is already the active site.
	if fg.Status.ActiveSite == req.Site {
		return PlannedFailoverSkip, v1alpha1.PlannedFailoverReasonAlreadyActive, nil
	}

	// Mid-rotation check sits before transient health checks so a
	// replica that is rolling for rotation is not mis-labelled
	// TargetUnhealthy. Reject, do not defer: the documented procedure
	// waits for Sealed, then re-annotates.
	if fg.SiteKeyringRotationBlocksPromotion(req.Site) {
		return PlannedFailoverReject, v1alpha1.PlannedFailoverReasonKeyringRotation, fmt.Errorf(
			"planned-failover: site %q is mid-keyring-rotation (UnsealReason=Rotation); finish the rotation before this site can be promoted",
			req.Site)
	}

	// Target's observed state must be read-only and replicating — a
	// site that is unreachable or unknown cannot catch up, and one
	// that is already writable means the cluster has bigger problems
	// than a planned switchover.
	var targetStatus *v1alpha1.SiteStatus
	for i := range fg.Status.Sites {
		if fg.Status.Sites[i].Name == req.Site {
			targetStatus = &fg.Status.Sites[i]
			break
		}
	}
	if targetStatus == nil {
		return PlannedFailoverReject, v1alpha1.PlannedFailoverReasonTargetUnhealthy, fmt.Errorf(
			"planned-failover: no observed status for site %q yet; retry after the operator completes its first poll cycle",
			req.Site)
	}
	// A target holding transactions the primary lacks is refused before
	// anything is fenced: promoting it would make that history
	// authoritative. Reject, do not defer: a human resolves it (reclone, or
	// make the primary contain the set) and re-annotates.
	if targetStatus.RecoveryState == "RecoveryBlocked" {
		var count int64
		if targetStatus.DivergentTransactionCount != nil {
			count = *targetStatus.DivergentTransactionCount
		}
		return PlannedFailoverReject, v1alpha1.PlannedFailoverReasonDivergentTarget, fmt.Errorf(
			"planned-failover: site %q is divergent (%d transactions the primary lacks: %s); reclone it or make the primary contain them before it can be promoted",
			req.Site, count, truncateGtidSet(targetStatus.DivergentGtid))
	}
	if targetStatus.State != "read-only" {
		return PlannedFailoverReject, v1alpha1.PlannedFailoverReasonTargetUnhealthy, fmt.Errorf(
			"planned-failover: site %q is in state %q; target must be a healthy read-only replica",
			req.Site, targetStatus.State)
	}
	if restoreReclonePending(fg, req.Site) {
		// Its replication receiver is held stopped until its post-restore
		// reclone, so it cannot catch up; refuse before fencing the source
		// even when the observed status still says it replicates.
		return PlannedFailoverReject, v1alpha1.PlannedFailoverReasonTargetUnhealthy, fmt.Errorf(
			"planned-failover: site %q awaits its post-restore reclone (status.restoreReclones.pending) and its replication is held stopped; refusing to promote it",
			req.Site)
	}
	if !targetStatus.Replicating {
		return PlannedFailoverReject, v1alpha1.PlannedFailoverReasonTargetUnhealthy, fmt.Errorf(
			"planned-failover: site %q is read-only but not replicating; refusing to promote a non-replicating target",
			req.Site)
	}

	// No concurrent destructive restore.
	if rip := fg.Status.RestoreInPlace; rip != nil {
		switch rip.Phase {
		case v1alpha1.RestoreInPlacePreflight,
			v1alpha1.RestoreInPlaceFencing,
			v1alpha1.RestoreInPlaceRestoring,
			v1alpha1.RestoreInPlaceResuming:
			return PlannedFailoverReject, v1alpha1.PlannedFailoverReasonConcurrentOperation, fmt.Errorf(
				"planned-failover: in-place restore is in phase %q; retry once status.restoreInPlace is terminal",
				rip.Phase)
		}
	}

	// No in-flight ordered update.
	if fg.Status.UpdatePhase != "" {
		return PlannedFailoverReject, v1alpha1.PlannedFailoverReasonConcurrentOperation, fmt.Errorf(
			"planned-failover: ordered update is in progress (status.updatePhase=%q); retry once the rollout is idle",
			fg.Status.UpdatePhase)
	}

	// No other non-terminal planned-failover run. Terminal runs do not
	// block; they are replaced.
	if plannedFailoverActive(fg) && !allowCurrentRun {
		return PlannedFailoverReject, v1alpha1.PlannedFailoverReasonConcurrentOperation, fmt.Errorf(
			"planned-failover: previous planned failover is still running (status.plannedFailover.phase=%q)",
			fg.Status.PlannedFailover.Phase)
	}

	// Anti-flap cooldown. This is the same effective durable record used by
	// the automatic path after restart, so a status-write outage cannot make
	// planned and emergency failover disagree about the cooldown.
	cooldown := failoverCooldownFromSpec(fg)
	failoverRecord, _, err := EffectiveFailoverRecord(fg, now)
	if err != nil && failoverRecord.IsZero() {
		return PlannedFailoverReject, v1alpha1.PlannedFailoverReasonInvalidFailoverState, fmt.Errorf(
			"planned-failover: durable anti-flap state is invalid: %w", err)
	}
	if !failoverRecord.LastFailover.IsZero() {
		elapsed := now.Sub(failoverRecord.LastFailover)
		if elapsed < cooldown {
			retryAfter := failoverRecord.LastFailover.Add(cooldown)
			return PlannedFailoverReject, v1alpha1.PlannedFailoverReasonCooldownActive, fmt.Errorf(
				"planned-failover: anti-flap cooldown active (last failover at %s, cooldown %s, retry after %s)",
				failoverRecord.LastFailover.UTC().Format(time.RFC3339),
				cooldown, retryAfter.UTC().Format(time.RFC3339))
		}
	}

	return PlannedFailoverAccept, "", nil
}

// failoverCooldownFromSpec returns the effective anti-flap cooldown for
// the CR: spec.failoverCooldown when set, otherwise the default 5m. The
// topology manager applies the same default at startup.
func failoverCooldownFromSpec(fg *v1alpha1.MysqlFailoverGroup) time.Duration {
	if fg.Spec.FailoverCooldown != nil && fg.Spec.FailoverCooldown.Duration > 0 {
		return fg.Spec.FailoverCooldown.Duration
	}
	return 5 * time.Minute
}

// effectiveMaxLagWait returns the max-lag-wait for a planned failover,
// honoring the annotation override first, then
// spec.plannedFailover.maxLagWait, then the package default.
func effectiveMaxLagWait(fg *v1alpha1.MysqlFailoverGroup, req PlannedFailoverRequest) time.Duration {
	if req.MaxLagWait > 0 {
		return req.MaxLagWait
	}
	if fg.Spec.PlannedFailover != nil && fg.Spec.PlannedFailover.MaxLagWait != nil && fg.Spec.PlannedFailover.MaxLagWait.Duration > 0 {
		return fg.Spec.PlannedFailover.MaxLagWait.Duration
	}
	return defaultPlannedFailoverMaxLagWait
}

// effectiveDrainTimeout returns the drain timeout for a planned
// failover, honoring spec.plannedFailover.drainTimeout first, then the
// package default.
func effectiveDrainTimeout(fg *v1alpha1.MysqlFailoverGroup) time.Duration {
	if fg.Spec.PlannedFailover != nil && fg.Spec.PlannedFailover.DrainTimeout != nil && fg.Spec.PlannedFailover.DrainTimeout.Duration > 0 {
		return fg.Spec.PlannedFailover.DrainTimeout.Duration
	}
	return defaultPlannedFailoverDrainTimeout
}

// effectiveOnCooldown returns the spec.plannedFailover.onCooldown
// policy, defaulting to Reject. An empty value or any unknown value
// falls back to Reject so a malformed spec does not accidentally
// enable the defer path. The comparison ignores case: releases before
// v1beta1 wrote the lower-case `defer`, which CRD ratcheting keeps on an
// object updated without touching the field.
func effectiveOnCooldown(fg *v1alpha1.MysqlFailoverGroup) v1alpha1.PlannedFailoverCooldownPolicy {
	if fg.Spec.PlannedFailover != nil && strings.EqualFold(string(fg.Spec.PlannedFailover.OnCooldown), string(v1alpha1.PlannedFailoverCooldownDefer)) {
		return v1alpha1.PlannedFailoverCooldownDefer
	}
	return v1alpha1.PlannedFailoverCooldownReject
}

// cooldownRetryAfter returns the earliest time after which the cooldown
// check will accept a new planned failover. Returns the zero time when
// no prior failover has been recorded.
func cooldownRetryAfter(fg *v1alpha1.MysqlFailoverGroup, now time.Time) time.Time {
	rec, _, _ := EffectiveFailoverRecord(fg, now)
	if rec.LastFailover.IsZero() {
		return time.Time{}
	}
	return rec.LastFailover.Add(failoverCooldownFromSpec(fg))
}

// plannedFailoverSettingsFor snapshots the run's settings at acceptance:
// the annotation override, then spec, then the defaults. The Dragonfly
// block is present iff Dragonfly is enabled, so the engine inserts the
// Dragonfly phases exactly for the runs that were accepted with it.
func plannedFailoverSettingsFor(fg *v1alpha1.MysqlFailoverGroup, req PlannedFailoverRequest) v1alpha1.PlannedFailoverSettings {
	s := v1alpha1.PlannedFailoverSettings{
		MaxLagWait:   metav1.Duration{Duration: effectiveMaxLagWait(fg, req)},
		DrainTimeout: metav1.Duration{Duration: effectiveDrainTimeout(fg)},
	}
	if dragonflyEnabled(fg) {
		s.Dragonfly = &v1alpha1.PlannedFailoverDragonflySettings{
			MaxSyncWait:   metav1.Duration{Duration: effectiveDragonflyMaxSyncWait(fg)},
			OnSyncTimeout: effectiveDragonflyOnSyncTimeout(fg),
		}
	}
	return s
}

// plannedSettings returns the run's snapshotted settings, falling back to
// the defaults for a block that carries none.
func plannedSettings(pf *v1alpha1.PlannedFailoverStatus) v1alpha1.PlannedFailoverSettings {
	s := v1alpha1.PlannedFailoverSettings{
		MaxLagWait:   metav1.Duration{Duration: defaultPlannedFailoverMaxLagWait},
		DrainTimeout: metav1.Duration{Duration: defaultPlannedFailoverDrainTimeout},
	}
	if pf == nil {
		return s
	}
	if pf.Settings.MaxLagWait.Duration > 0 {
		s.MaxLagWait = pf.Settings.MaxLagWait
	}
	if pf.Settings.DrainTimeout.Duration > 0 {
		s.DrainTimeout = pf.Settings.DrainTimeout
	}
	if d := pf.Settings.Dragonfly; d != nil {
		dd := *d
		if dd.MaxSyncWait.Duration <= 0 {
			dd.MaxSyncWait.Duration = defaultDragonflySyncWait
		}
		dd.OnSyncTimeout = normalizeDragonflySyncTimeout(dd.OnSyncTimeout)
		s.Dragonfly = &dd
	}
	return s
}
