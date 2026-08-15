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
	"strings"
	"time"

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

	// PlannedFailoverOnCooldownReject is the default behaviour:
	// cooldown at Validating results in a terminal Failed status with
	// reason "CooldownActive".
	PlannedFailoverOnCooldownReject = "reject"

	// PlannedFailoverOnCooldownDefer keeps the annotation in place and
	// re-tries validation at cooldown expiry; the state machine enters
	// the Deferred phase in the interim.
	PlannedFailoverOnCooldownDefer = "defer"
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

// parsePlannedFailoverAnnotation parses a planned-failover annotation
// value. The grammar is:
//
//	<siteName>
//	<siteName>:<key>=<value>[:<key>=<value>...]
//
// Whitespace around tokens is trimmed. Unknown keys are rejected so
// typos do not silently accept a request with default knobs. Returns a
// PlannedFailoverRequest on success; an error on unknown keys or
// malformed values.
func parsePlannedFailoverAnnotation(raw string) (PlannedFailoverRequest, error) {
	v := strings.TrimSpace(raw)
	if v == "" {
		return PlannedFailoverRequest{}, fmt.Errorf(
			"planned-failover annotation is empty; expected <site>[:maxLagWait=<duration>]")
	}

	parts := strings.Split(v, ":")
	req := PlannedFailoverRequest{Site: strings.TrimSpace(parts[0])}

	for _, kv := range parts[1:] {
		kv = strings.TrimSpace(kv)
		if kv == "" {
			continue
		}
		eq := strings.Index(kv, "=")
		if eq < 0 {
			return PlannedFailoverRequest{}, fmt.Errorf(
				"planned-failover annotation override %q must be key=value", kv)
		}
		key := strings.TrimSpace(kv[:eq])
		val := strings.TrimSpace(kv[eq+1:])
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
// can be evaluated from the CR alone (spec + status). Everything
// time-sensitive — cooldown, clock — is evaluated via the supplied now
// argument so tests can drive the boundary deterministically.
//
// Returns (PlannedFailoverAccept, nil) on success, (PlannedFailoverSkip,
// nil) when the target already matches status.activeSite, or
// (PlannedFailoverReject, err) with an error message suitable for an
// event when something is wrong. The reason string is a short machine-
// readable tag ("UnknownSite", "CooldownActive", ...) that the state
// machine stamps into status.plannedFailover.reason.
func validatePlannedFailoverRequest(fg *v1alpha1.MysqlFailoverGroup, req PlannedFailoverRequest, now time.Time, allowCurrentRun bool) (PlannedFailoverValidationResult, string, error) {
	if req.Site == "" {
		return PlannedFailoverReject, "InvalidAnnotation", fmt.Errorf(
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
		return PlannedFailoverReject, "UnknownSite", fmt.Errorf(
			"planned-failover: unknown site %q; must match one of spec.sites[].name", req.Site)
	}
	if !targetSpec.IsPromotable() {
		return PlannedFailoverReject, "UnknownSite", fmt.Errorf(
			"planned-failover: site %q has role %q; only primary-candidate sites may be promoted",
			req.Site, targetSpec.EffectiveRole())
	}

	// Idempotent no-op: target is already the active site. The admin
	// sees one PlannedFailoverSkipped event and a clean annotation
	// clear.
	if fg.Status.ActiveSite == req.Site {
		return PlannedFailoverSkip, "", nil
	}

	// Mid-rotation check sits before transient health checks so a
	// replica that is rolling for rotation is not mis-labelled
	// TargetUnhealthy. Reject, do not defer: the documented procedure
	// waits for Sealed, then re-annotates.
	if fg.SiteKeyringRotationBlocksPromotion(req.Site) {
		return PlannedFailoverReject, "KeyringRotation", fmt.Errorf(
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
		return PlannedFailoverReject, "TargetUnhealthy", fmt.Errorf(
			"planned-failover: no observed status for site %q yet; retry after the operator completes its first poll cycle",
			req.Site)
	}
	if targetStatus.State != "read-only" {
		return PlannedFailoverReject, "TargetUnhealthy", fmt.Errorf(
			"planned-failover: site %q is in state %q; target must be a healthy read-only replica",
			req.Site, targetStatus.State)
	}
	if !targetStatus.Replicating {
		return PlannedFailoverReject, "TargetUnhealthy", fmt.Errorf(
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
			return PlannedFailoverReject, "ConcurrentOperation", fmt.Errorf(
				"planned-failover: in-place restore is in phase %q; retry once status.restoreInPlace is terminal",
				rip.Phase)
		}
	}

	// No in-flight ordered update.
	if fg.Status.UpdatePhase != "" {
		return PlannedFailoverReject, "ConcurrentOperation", fmt.Errorf(
			"planned-failover: ordered update is in progress (status.updatePhase=%q); retry once the rollout is idle",
			fg.Status.UpdatePhase)
	}

	// No in-flight planned failover (either from the same attempt
	// being re-delivered or from a previous attempt that is still
	// running). Terminal phases do not block; they are replaced.
	if pf := fg.Status.PlannedFailover; plannedFailoverInFlight(pf) {
		if !(allowCurrentRun && (pf.Phase == v1alpha1.PlannedFailoverPhasePending || pf.Phase == v1alpha1.PlannedFailoverPhaseValidating)) {
			return PlannedFailoverReject, "ConcurrentOperation", fmt.Errorf(
				"planned-failover: previous planned failover is still running (status.plannedFailover.phase=%q)",
				pf.Phase)
		}
	}

	// Anti-flap cooldown. This is the same effective durable record used by
	// the automatic path after restart, so a status-write outage cannot make
	// planned and emergency failover disagree about the cooldown.
	cooldown := failoverCooldownFromSpec(fg)
	failoverRecord, _, err := EffectiveFailoverRecord(fg, now)
	if err != nil && failoverRecord.IsZero() {
		return PlannedFailoverReject, "InvalidFailoverState", fmt.Errorf(
			"planned-failover: durable anti-flap state is invalid: %w", err)
	}
	if !failoverRecord.LastFailover.IsZero() {
		elapsed := now.Sub(failoverRecord.LastFailover)
		if elapsed < cooldown {
			retryAfter := failoverRecord.LastFailover.Add(cooldown)
			return PlannedFailoverReject, "CooldownActive", fmt.Errorf(
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
// policy, defaulting to "reject". An empty value or any unknown value
// falls back to "reject" so a malformed spec does not accidentally
// enable the defer path.
func effectiveOnCooldown(fg *v1alpha1.MysqlFailoverGroup) string {
	if fg.Spec.PlannedFailover != nil {
		switch fg.Spec.PlannedFailover.OnCooldown {
		case PlannedFailoverOnCooldownDefer:
			return PlannedFailoverOnCooldownDefer
		}
	}
	return PlannedFailoverOnCooldownReject
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

// plannedFailoverFencesSourcePrimary reports whether syncPodLabels
// should strip the primary role label on the currently-active site.
// True during Draining, WaitingForLag, and Promoting — i.e. from the
// moment we fence the source until the reconciler has persisted a new
// status.activeSite. Resuming itself flips status.activeSite to the
// target, so by the time the next reconcile runs, the "primary" label
// naturally belongs to the new primary and no explicit unfencing step
// is needed.
func plannedFailoverFencesSourcePrimary(fg *v1alpha1.MysqlFailoverGroup) bool {
	pf := fg.Status.PlannedFailover
	if pf == nil {
		return false
	}
	switch pf.Phase {
	case v1alpha1.PlannedFailoverPhaseDraining,
		v1alpha1.PlannedFailoverPhaseWaitingForLag,
		v1alpha1.PlannedFailoverPhaseWaitingForDragonflySync,
		v1alpha1.PlannedFailoverPhasePromotingDragonfly,
		v1alpha1.PlannedFailoverPhasePromoting:
		return true
	}
	return false
}
