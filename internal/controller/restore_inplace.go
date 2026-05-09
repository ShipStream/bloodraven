// Package controller — in-place restore state machine.
//
// This file implements spec.restoreInPlace: a re-triggerable destructive
// restore that loads a previous dump into the currently-active primary
// WITHOUT a teardown/rename cycle. Unlike initFromBackup (one-shot,
// greenfield), an in-place restore runs against a live cluster.
//
// Two modes, selected by spec.restoreInPlace.loadOptions.includeSchemas:
//
//   - Full-instance (includeSchemas empty): drops every user schema on
//     the primary, re-loads from the dump, then hands off to the
//     existing reclone-annotation machinery to resync the replica via
//     CLONE INSTANCE. Writes are fenced at the Service layer by
//     stripping the primary role label (so the -primary Service sheds
//     endpoints).
//
//   - Per-schema (exactly one includeSchemas entry): drops only the
//     named schema, re-loads just that schema, and relies on MySQL
//     replication to propagate the DROP+load to the replica. The
//     primary Service stays up — other tenants keep writing — and the
//     caller is responsible for putting the affected tenant into
//     application-level maintenance mode.
//
// The state machine advances one phase per reconcile so operator
// restarts land on a well-defined observable state.
package controller

import (
	"context"
	"fmt"
	"strings"
	"time"

	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	k8sretry "k8s.io/client-go/util/retry"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
	"sigs.k8s.io/controller-runtime/pkg/log"

	v1alpha1 "github.com/shipstream/bloodraven/api/v1alpha1"
)

// inPlaceRestoreInFlight reports whether spec.restoreInPlace is set and
// the currently-observed status is in a non-terminal phase. The runner
// calls this to set the topology-frozen gate on every sync.
func inPlaceRestoreInFlight(fg *v1alpha1.MysqlFailoverGroup) bool {
	if fg.Spec.RestoreInPlace == nil {
		return false
	}
	if fg.Status.RestoreInPlace == nil {
		// Spec is set but we have not yet observed it; treat as in-flight
		// so the next reconcile does preflight before any cross-site
		// action can fire.
		return true
	}
	switch fg.Status.RestoreInPlace.Phase {
	case v1alpha1.RestoreInPlaceSucceeded, v1alpha1.RestoreInPlaceFailed, v1alpha1.RestoreInPlaceNone:
		return false
	}
	return true
}

// inPlaceRestoreFencesPrimaryService reports whether the current
// in-place restore phase should cause syncPodLabels to strip the
// primary role label on the active site (and thus drain the -primary
// Service). True for full-instance restores in Fencing, Restoring, or
// Resuming phases; false for per-schema restores (other tenants keep
// writing) and when no in-place restore is in flight.
func inPlaceRestoreFencesPrimaryService(fg *v1alpha1.MysqlFailoverGroup) bool {
	if fg.Spec.RestoreInPlace == nil {
		return false
	}
	_, schema := inPlaceRestoreScope(fg.Spec.RestoreInPlace)
	if schema != "" {
		// Per-schema: Service stays up.
		return false
	}
	if fg.Status.RestoreInPlace == nil {
		// Spec set but not yet observed — be conservative and fence
		// starting with the next reconcile. Safer than letting a
		// restore-in-progress Service flip flap.
		return true
	}
	switch fg.Status.RestoreInPlace.Phase {
	case v1alpha1.RestoreInPlaceFencing,
		v1alpha1.RestoreInPlaceRestoring,
		v1alpha1.RestoreInPlaceResuming:
		return true
	}
	return false
}

// inPlaceRestoreTerminal reports whether the observed phase is a
// terminal state (Succeeded or Failed) — used to skip the reconciler
// work while still allowing the user to re-arm via a newer confirm
// timestamp.
func inPlaceRestoreTerminal(s *v1alpha1.RestoreInPlaceStatus) bool {
	if s == nil {
		return false
	}
	return s.Phase == v1alpha1.RestoreInPlaceSucceeded || s.Phase == v1alpha1.RestoreInPlaceFailed
}

// inPlaceRestoreJobName is distinct from the bootstrap restoreJobName
// so both kinds of restore Jobs can coexist if the user re-applies
// initFromBackup after a previous bootstrap.
func inPlaceRestoreJobName(fgName, siteName string) string {
	return truncateDNS1123(fmt.Sprintf("mysql-%s-%s-inplace-restore", fgName, siteName))
}

// parseConfirmTimestamp parses spec.restoreInPlace.confirm. The confirm
// field must be RFC 3339 so the monotonicity check is well-defined and
// programmatic callers can just send time.Now().UTC().Format(time.RFC3339).
func parseConfirmTimestamp(s string) (time.Time, error) {
	s = strings.TrimSpace(s)
	if s == "" {
		return time.Time{}, fmt.Errorf("confirm is required")
	}
	t, err := time.Parse(time.RFC3339, s)
	if err != nil {
		return time.Time{}, fmt.Errorf("confirm must be an RFC 3339 timestamp (e.g. %q): %w",
			time.Now().UTC().Format(time.RFC3339), err)
	}
	return t, nil
}

// confirmAdvances reports whether the spec confirm is strictly greater
// than the last-used confirm (monotonic check). The empty string on
// either side is treated as "no prior value" (advancing permitted).
func confirmAdvances(specConfirm, lastUsed string) (bool, error) {
	specT, err := parseConfirmTimestamp(specConfirm)
	if err != nil {
		return false, err
	}
	if lastUsed == "" {
		return true, nil
	}
	lastT, err := time.Parse(time.RFC3339, lastUsed)
	if err != nil {
		// Prior value unparseable — treat as "replace it" so we do not
		// wedge the controller on stale garbage.
		return true, nil
	}
	return specT.After(lastT), nil
}

// inPlaceRestoreScope returns ("full", "") for a full-instance restore
// or ("schema:<name>", "<name>") for a per-schema restore. The second
// return value is the bare schema name used for DROP DATABASE and for
// the mysqlbinlog --database filter.
func inPlaceRestoreScope(spec *v1alpha1.RestoreInPlaceSpec) (string, string) {
	if spec == nil || spec.LoadOptions == nil || len(spec.LoadOptions.IncludeSchemas) == 0 {
		return "full", ""
	}
	name := spec.LoadOptions.IncludeSchemas[0]
	return "schema:" + name, name
}

// validateInPlaceRestoreSpec enforces invariants that cannot be (or are
// awkward to) encode as CEL rules. Returns a human-readable error
// suitable for stamping into status.message.
func validateInPlaceRestoreSpec(spec *v1alpha1.RestoreInPlaceSpec) error {
	if spec == nil {
		return fmt.Errorf("restoreInPlace is nil")
	}
	if _, err := parseConfirmTimestamp(spec.Confirm); err != nil {
		return err
	}
	if spec.LoadOptions != nil && len(spec.LoadOptions.IncludeSchemas) > 1 {
		return fmt.Errorf("restoreInPlace.loadOptions.includeSchemas supports at most one schema (got %d); use a full-instance restore instead",
			len(spec.LoadOptions.IncludeSchemas))
	}
	// Source must be set to exactly one of the three alternatives. The
	// CEL rule on InitFromBackupSource already enforces this but we
	// re-check so we emit a clean preflight error instead of crashing
	// inside buildRestoreJob.
	src := spec.Source
	setCount := 0
	if src.MysqlBackupRef != nil {
		setCount++
	}
	if src.S3 != nil {
		setCount++
	}
	if src.PVC != nil {
		setCount++
	}
	if setCount != 1 {
		return fmt.Errorf("restoreInPlace.source must set exactly one of mysqlBackupRef, s3, pvc (got %d)", setCount)
	}
	return nil
}

// reconcileInPlaceRestore drives the state machine for spec.restoreInPlace.
// Like reconcileRestoreJob it returns a non-zero requeue duration when
// the caller should wake back up before the default resync, and no
// error when it has already stamped a terminal status (i.e. the
// reconciler should not crash-loop on a user-recoverable error).
func (r *MysqlFailoverGroupReconciler) reconcileInPlaceRestore(ctx context.Context, fg *v1alpha1.MysqlFailoverGroup) (time.Duration, error) {
	// No spec: clear stale status and return.
	if fg.Spec.RestoreInPlace == nil {
		if fg.Status.RestoreInPlace != nil {
			r.setInPlaceRestoreStatus(ctx, fg, nil)
		}
		return 0, nil
	}

	spec := fg.Spec.RestoreInPlace
	cur := fg.Status.RestoreInPlace

	// Terminal states hold until the user advances spec.confirm past
	// status.confirmTokenUsed. Note: terminal status stays in place
	// across the re-arm so users can still see the previous outcome.
	if inPlaceRestoreTerminal(cur) {
		advances, err := confirmAdvances(spec.Confirm, cur.ConfirmTokenUsed)
		if err != nil {
			// Invalid new confirm: leave the terminal status alone but
			// surface the error on the CR events so the user knows
			// why the re-arm did not take.
			r.Recorder.Eventf(fg, corev1.EventTypeWarning, "RestoreInPlaceRejected",
				"invalid spec.restoreInPlace.confirm: %v", err)
			return 0, nil
		}
		if !advances {
			// Current terminal status already reflects this confirm
			// value (or an older one). Nothing to do.
			return 0, nil
		}
		// User bumped confirm past the last-used value — start a fresh
		// run. Fall through.
		cur = nil
	}

	// If we have no status yet, validate the spec up-front and move to
	// Preflight. Keeping the initial transition in one place makes the
	// phase-dispatch below trivially table-driven.
	if cur == nil {
		if err := validateInPlaceRestoreSpec(spec); err != nil {
			r.stampTerminalFailure(ctx, fg, "", "", err.Error(), "RestoreInPlaceRejected")
			return 0, nil
		}
		scope, _ := inPlaceRestoreScope(spec)
		now := metav1.Now()
		next := &v1alpha1.RestoreInPlaceStatus{
			Phase:     v1alpha1.RestoreInPlacePreflight,
			Scope:     scope,
			StartTime: &now,
			Message:   "validating preconditions",
		}
		applyRestoreMetadataToInPlaceStatus(next, r.restoreSourceMetadata(ctx, fg, spec.Source))
		r.setInPlaceRestoreStatus(ctx, fg, next)
		r.Recorder.Eventf(fg, corev1.EventTypeNormal, "RestoreInPlaceStarted",
			"starting in-place restore (scope=%s, confirm=%s)", scope, spec.Confirm)
		return 5 * time.Second, nil
	}

	switch cur.Phase {
	case v1alpha1.RestoreInPlacePreflight:
		return r.inPlacePreflight(ctx, fg)
	case v1alpha1.RestoreInPlaceFencing:
		return r.inPlaceFencing(ctx, fg)
	case v1alpha1.RestoreInPlaceRestoring:
		return r.inPlaceRestoring(ctx, fg)
	case v1alpha1.RestoreInPlaceResuming:
		return r.inPlaceResuming(ctx, fg)
	}

	// Unknown phase (forward compatibility): treat as fresh start.
	r.setInPlaceRestoreStatus(ctx, fg, nil)
	return 5 * time.Second, nil
}

// inPlacePreflight validates that the cluster is in a state where a
// restore can safely proceed (active primary writable, deployment
// rolled out) and resolves the target site. On success it transitions
// to Fencing.
func (r *MysqlFailoverGroupReconciler) inPlacePreflight(ctx context.Context, fg *v1alpha1.MysqlFailoverGroup) (time.Duration, error) {
	spec := fg.Spec.RestoreInPlace
	cur := fg.Status.RestoreInPlace

	if fg.Status.ActiveSite == "" {
		next := &v1alpha1.RestoreInPlaceStatus{
			Phase:     v1alpha1.RestoreInPlacePreflight,
			Scope:     cur.Scope,
			StartTime: cur.StartTime,
			Message:   "waiting for status.activeSite to be populated",
		}
		applyRestoreMetadataToInPlaceStatus(next, restoreMetadataFromInPlaceStatus(cur))
		r.setInPlaceRestoreStatus(ctx, fg, next)
		return 15 * time.Second, nil
	}
	target := fg.Status.ActiveSite

	// The active site must look writable. We refuse to run in-place
	// restore while a failover is in progress or the primary is fenced
	// for some other reason — the restore would either stall (loadDump
	// can't write to super_read_only=ON) or race the topology manager.
	var activeStatus *v1alpha1.SiteStatus
	for i := range fg.Status.Sites {
		if fg.Status.Sites[i].Name == target {
			activeStatus = &fg.Status.Sites[i]
			break
		}
	}
	if activeStatus == nil || activeStatus.State != "writable" {
		observedState := "<unknown>"
		if activeStatus != nil {
			observedState = activeStatus.State
		}
		next := &v1alpha1.RestoreInPlaceStatus{
			Phase:      v1alpha1.RestoreInPlacePreflight,
			TargetSite: target,
			Scope:      cur.Scope,
			StartTime:  cur.StartTime,
			Message: fmt.Sprintf("waiting for active site %q to be writable (observed: %s)",
				target, observedState),
		}
		applyRestoreMetadataToInPlaceStatus(next, restoreMetadataFromInPlaceStatus(cur))
		r.setInPlaceRestoreStatus(ctx, fg, next)
		return 15 * time.Second, nil
	}

	if err := validateInPlaceRestoreSpec(spec); err != nil {
		r.stampTerminalFailure(ctx, fg, target, cur.Scope, err.Error(), "RestoreInPlaceRejected")
		return 0, nil
	}

	next := &v1alpha1.RestoreInPlaceStatus{
		Phase:      v1alpha1.RestoreInPlaceFencing,
		TargetSite: target,
		Scope:      cur.Scope,
		StartTime:  cur.StartTime,
		Message:    "applying pre-restore fence",
	}
	applyRestoreMetadataToInPlaceStatus(next, restoreMetadataFromInPlaceStatus(cur))
	r.setInPlaceRestoreStatus(ctx, fg, next)
	return 2 * time.Second, nil
}

// inPlaceFencing advances to Restoring once the topology manager has
// observed the frozen flag. We do not block here waiting for the
// fencing to "take" because the topology runner re-reads
// status.restoreInPlace.phase on every tick — by the time the restore
// Job starts the frozen flag is already propagated.
//
// For full-instance restores the primary role label strip also happens
// via syncPodLabels, which keys on the current phase. By the time we
// move on to Restoring the label sweep has run at least once.
func (r *MysqlFailoverGroupReconciler) inPlaceFencing(ctx context.Context, fg *v1alpha1.MysqlFailoverGroup) (time.Duration, error) {
	cur := fg.Status.RestoreInPlace

	// Ensure pod labels reflect the fenced state before we advance. If
	// syncPodLabels has not yet run for this phase (reconciler just
	// entered Fencing this tick), requeue briefly.
	_, schema := inPlaceRestoreScope(fg.Spec.RestoreInPlace)
	isFullInstance := schema == ""
	if isFullInstance && !r.primaryPodLabelFenced(ctx, fg) {
		return 2 * time.Second, nil
	}

	next := &v1alpha1.RestoreInPlaceStatus{
		Phase:      v1alpha1.RestoreInPlaceRestoring,
		TargetSite: cur.TargetSite,
		Scope:      cur.Scope,
		StartTime:  cur.StartTime,
		Message:    "creating restore Job",
	}
	applyRestoreMetadataToInPlaceStatus(next, restoreMetadataFromInPlaceStatus(cur))
	r.setInPlaceRestoreStatus(ctx, fg, next)
	return 2 * time.Second, nil
}

// inPlaceRestoring creates the restore Job (if missing) and waits for
// it to reach a terminal state.
func (r *MysqlFailoverGroupReconciler) inPlaceRestoring(ctx context.Context, fg *v1alpha1.MysqlFailoverGroup) (time.Duration, error) {
	spec := fg.Spec.RestoreInPlace
	cur := fg.Status.RestoreInPlace
	target := cur.TargetSite
	_, schema := inPlaceRestoreScope(spec)
	isFullInstance := schema == ""

	credsName := restoreCredsSecretName(fg.Name)
	if err := r.ensureRestoreCredsSecret(ctx, fg, credsName); err != nil {
		r.stampTerminalFailure(ctx, fg, target, cur.Scope,
			fmt.Sprintf("ensure restore creds: %v", err), "RestoreInPlaceBuildFailed")
		return 0, nil
	}

	var job batchv1.Job
	jobName := inPlaceRestoreJobName(fg.Name, target)
	jobKey := types.NamespacedName{Namespace: fg.Namespace, Name: jobName}
	if err := r.Get(ctx, jobKey, &job); err != nil {
		if !apierrors.IsNotFound(err) {
			return 0, fmt.Errorf("get in-place restore job: %w", err)
		}

		built, err := r.buildInPlaceRestoreJob(ctx, fg, target, credsName, isFullInstance, schema)
		if err != nil {
			r.stampTerminalFailure(ctx, fg, target, cur.Scope, err.Error(), "RestoreInPlaceBuildFailed")
			return 0, nil
		}
		if err := controllerutil.SetControllerReference(fg, built, r.Scheme); err != nil {
			return 0, fmt.Errorf("set in-place restore job owner ref: %w", err)
		}
		if err := r.Create(ctx, built); err != nil {
			return 0, fmt.Errorf("create in-place restore job: %w", err)
		}
		next := &v1alpha1.RestoreInPlaceStatus{
			Phase:      v1alpha1.RestoreInPlaceRestoring,
			JobName:    built.Name,
			TargetSite: target,
			Scope:      cur.Scope,
			StartTime:  cur.StartTime,
			Message:    "restore Job created",
		}
		applyRestoreMetadataToInPlaceStatus(next, restoreMetadataFromInPlaceStatus(cur))
		r.setInPlaceRestoreStatus(ctx, fg, next)
		r.Recorder.Eventf(fg, corev1.EventTypeNormal, "RestoreInPlaceJobCreated",
			"created in-place restore Job %s targeting site %s (scope=%s)",
			built.Name, target, cur.Scope)
		return 15 * time.Second, nil
	}

	phase, message := jobPhase(&job, "in-place restore")
	if phase == "" {
		return 15 * time.Second, nil
	}

	if phase == v1alpha1.BackupPhaseSucceeded {
		now := metav1.Now()
		meta := r.restoreSourceMetadata(ctx, fg, spec.Source)
		curMeta := restoreMetadataFromInPlaceStatus(cur)
		if curMeta.SourceSizeBytes > 0 || curMeta.SourceGtidExecuted != "" || curMeta.SourceBinlogFile != "" || curMeta.SourceBinlogPos > 0 {
			meta = curMeta
		}
		if parsed, ok := r.tailRestoreCompletion(ctx, &job); ok {
			meta = parsed
		}
		jobStart := job.Status.StartTime
		if jobStart == nil {
			jobStart = cur.StartTime
		}
		if jobStart == nil {
			jobStart = &now
		}
		r.Recorder.Eventf(fg, corev1.EventTypeNormal, "RestoreInPlaceLoaded",
			"in-place restore Job %s completed: %s", job.Name, message)
		next := &v1alpha1.RestoreInPlaceStatus{
			Phase:      v1alpha1.RestoreInPlaceResuming,
			JobName:    job.Name,
			TargetSite: target,
			Scope:      cur.Scope,
			StartTime:  cur.StartTime,
			Message:    "load complete; lifting fence",
		}
		applyRestoreMetadataToInPlaceStatus(next, meta)
		if r.setInPlaceRestoreStatus(ctx, fg, next) {
			emitRestoreSuccessMetrics(fg, "in_place", target, next.SourceSizeBytes, jobStart, now)
		}
		return 2 * time.Second, nil
	}

	// Failure.
	r.Recorder.Eventf(fg, corev1.EventTypeWarning, "RestoreInPlaceFailed",
		"in-place restore Job %s failed: %s", job.Name, message)
	r.stampTerminalFailure(ctx, fg, target, cur.Scope, message, "RestoreInPlaceFailed")
	// Even on failure we unfence so the cluster is not indefinitely
	// stuck read-only. stampTerminalFailure triggers the runner to
	// clear the topologyFrozen flag on next sync.
	return 0, nil
}

// inPlaceResuming lifts the fence: stamps the confirm token, schedules
// a replica reclone for full-instance restores, and transitions to
// Succeeded. The topology manager unfreezes automatically once the
// runner next observes the terminal phase.
func (r *MysqlFailoverGroupReconciler) inPlaceResuming(ctx context.Context, fg *v1alpha1.MysqlFailoverGroup) (time.Duration, error) {
	spec := fg.Spec.RestoreInPlace
	cur := fg.Status.RestoreInPlace
	target := cur.TargetSite
	_, schema := inPlaceRestoreScope(spec)
	isFullInstance := schema == ""

	// For full-instance restores the replica is now out-of-sync: its
	// old GTID state diverges from the freshly-loaded primary. Set the
	// reclone annotation so the topology manager picks it up via the
	// existing checkReclone path once the topologyFrozen flag clears.
	if isFullInstance {
		replicaSite := ""
		for i := range fg.Spec.Sites {
			if fg.Spec.Sites[i].Name != target {
				replicaSite = fg.Spec.Sites[i].Name
				break
			}
		}
		if replicaSite != "" {
			if err := r.setRecloneAnnotation(ctx, fg, replicaSite); err != nil {
				log.FromContext(ctx).Error(err, "failed to set reclone annotation for replica",
					"fg", fg.Name, "replicaSite", replicaSite)
				// Non-fatal: the restore itself succeeded; surface the
				// issue as an event so the operator can hand-trigger
				// the reclone.
				r.Recorder.Eventf(fg, corev1.EventTypeWarning, "RestoreInPlaceRecloneAnnotationFailed",
					"in-place restore succeeded but failed to schedule replica reclone: %v", err)
			} else {
				r.Recorder.Eventf(fg, corev1.EventTypeNormal, "RestoreInPlaceRecloneScheduled",
					"scheduled reclone of replica site %s after in-place restore", replicaSite)
			}
		}
	}

	now := metav1.Now()
	next := &v1alpha1.RestoreInPlaceStatus{
		Phase:            v1alpha1.RestoreInPlaceSucceeded,
		JobName:          cur.JobName,
		TargetSite:       target,
		Scope:            cur.Scope,
		ConfirmTokenUsed: spec.Confirm,
		StartTime:        cur.StartTime,
		CompletionTime:   &now,
		Message:          "in-place restore complete",
	}
	applyRestoreMetadataToInPlaceStatus(next, restoreMetadataFromInPlaceStatus(cur))
	r.setInPlaceRestoreStatus(ctx, fg, next)
	r.Recorder.Eventf(fg, corev1.EventTypeNormal, "RestoreInPlaceSucceeded",
		"in-place restore complete (scope=%s, site=%s)", cur.Scope, target)
	return 0, nil
}

// setInPlaceRestoreStatus patches fg.status.restoreInPlace.
func (r *MysqlFailoverGroupReconciler) setInPlaceRestoreStatus(ctx context.Context, fg *v1alpha1.MysqlFailoverGroup, s *v1alpha1.RestoreInPlaceStatus) bool {
	patch := client.MergeFrom(fg.DeepCopy())
	fg.Status.RestoreInPlace = s
	if err := r.Status().Patch(ctx, fg, patch); err != nil && !apierrors.IsNotFound(err) {
		log.FromContext(ctx).Error(err, "update in-place restore status", "fg", fg.Name)
		return false
	} else if apierrors.IsNotFound(err) {
		return false
	}
	// Push the freeze state to the topology manager directly so it
	// takes effect on the next poll cycle rather than waiting for the
	// runner's 30-second re-sync tick. Without this step there is a
	// window between status patch and runner re-sync in which the
	// topology manager could still fire a cross-site action (promotion,
	// auto-clone, recovery) against a cluster that is actively being
	// restored. A return of false from SetTopologyFrozen is expected
	// when no manager is running yet (fresh deploy) — the runner's
	// startManager path reads status.restoreInPlace and applies the
	// correct flag when it starts the manager.
	if r.Runner != nil {
		r.Runner.SetTopologyFrozen(
			types.NamespacedName{Namespace: fg.Namespace, Name: fg.Name},
			inPlaceRestoreInFlight(fg),
		)
	}
	return true
}

// stampTerminalFailure writes a Failed status with the given message
// and preserves prior metadata (scope, startTime, target).
func (r *MysqlFailoverGroupReconciler) stampTerminalFailure(ctx context.Context, fg *v1alpha1.MysqlFailoverGroup, target, scope, msg, eventReason string) {
	now := metav1.Now()
	cur := fg.Status.RestoreInPlace
	var start *metav1.Time
	var jobName string
	if cur != nil {
		start = cur.StartTime
		jobName = cur.JobName
	}
	if start == nil {
		start = &now
	}
	next := &v1alpha1.RestoreInPlaceStatus{
		Phase:          v1alpha1.RestoreInPlaceFailed,
		JobName:        jobName,
		TargetSite:     target,
		Scope:          scope,
		StartTime:      start,
		CompletionTime: &now,
		Message:        msg,
	}
	applyRestoreMetadataToInPlaceStatus(next, restoreMetadataFromInPlaceStatus(cur))
	r.setInPlaceRestoreStatus(ctx, fg, next)
	if eventReason != "" {
		r.Recorder.Eventf(fg, corev1.EventTypeWarning, eventReason, "%s", msg)
	}
}

// setRecloneAnnotation adds the one-shot reclone annotation to the CR.
// Mirrors the shape the topology runner expects in removeRecloneAnnotation.
func (r *MysqlFailoverGroupReconciler) setRecloneAnnotation(ctx context.Context, fg *v1alpha1.MysqlFailoverGroup, site string) error {
	nn := types.NamespacedName{Namespace: fg.Namespace, Name: fg.Name}
	return k8sretry.RetryOnConflict(k8sretry.DefaultRetry, func() error {
		var fresh v1alpha1.MysqlFailoverGroup
		if err := r.Get(ctx, nn, &fresh); err != nil {
			return err
		}
		annotations := fresh.GetAnnotations()
		if annotations == nil {
			annotations = map[string]string{}
		}
		annotations[RecloneAnnotation] = site
		fresh.SetAnnotations(annotations)
		return r.Update(ctx, &fresh)
	})
}

// buildInPlaceRestoreJob constructs the Job used by the in-place
// restore path. It delegates to buildRestoreJobSpec and adds the
// preflight env vars the restore script consumes:
//
//   - Full-instance: BLOODRAVEN_DROP_ALL_USER_SCHEMAS=1,
//     BLOODRAVEN_RESET_REPLICATION=1.
//   - Per-schema: BLOODRAVEN_DROP_SCHEMAS=<schema>,
//     BLOODRAVEN_PITR_FILTER_DATABASE=<schema> (when PITR is enabled).
//
// For per-schema restores it also forces LoadOptions.SkipBinlog=false
// so the DROP+load flows through the primary's binlog to the replica
// and the replica resyncs via normal replication.
func (r *MysqlFailoverGroupReconciler) buildInPlaceRestoreJob(ctx context.Context, fg *v1alpha1.MysqlFailoverGroup, targetSite, credsName string, isFullInstance bool, schema string) (*batchv1.Job, error) {
	spec := fg.Spec.RestoreInPlace

	// Copy LoadOptions so we can force skipBinlog=false for per-schema
	// without mutating the user-supplied spec.
	var loadOpts *v1alpha1.LoadOptions
	if spec.LoadOptions != nil {
		copied := *spec.LoadOptions
		loadOpts = &copied
	} else {
		loadOpts = &v1alpha1.LoadOptions{}
	}
	if !isFullInstance {
		// Per-schema: the load must be binlogged so replication
		// propagates the DROP+load to the replica.
		f := false
		loadOpts.SkipBinlog = &f
	}

	var extraEnv []corev1.EnvVar
	if isFullInstance {
		extraEnv = append(extraEnv,
			corev1.EnvVar{Name: "BLOODRAVEN_DROP_ALL_USER_SCHEMAS", Value: "1"},
			corev1.EnvVar{Name: "BLOODRAVEN_RESET_REPLICATION", Value: "1"},
		)
	} else {
		extraEnv = append(extraEnv,
			corev1.EnvVar{Name: "BLOODRAVEN_DROP_SCHEMAS", Value: schema},
		)
		if spec.PointInTime != nil {
			extraEnv = append(extraEnv,
				corev1.EnvVar{Name: "BLOODRAVEN_PITR_FILTER_DATABASE", Value: schema},
			)
		}
	}

	return r.buildRestoreJobSpec(ctx, fg, restoreJobInputs{
		JobName:     inPlaceRestoreJobName(fg.Name, targetSite),
		TargetSite:  targetSite,
		CredsName:   credsName,
		Source:      spec.Source,
		LoadOptions: loadOpts,
		PointInTime: spec.PointInTime,
		Decryption:  spec.Decryption,
		FieldPath:   "restoreInPlace",
		ExtraEnv:    extraEnv,
	})
}

// primaryPodLabelFenced returns true when the pod(s) on the active site
// have had their role label stripped ("fenced" or empty) — which is the
// signal that the -primary Service has shed endpoints and the fence is
// observable to clients.
func (r *MysqlFailoverGroupReconciler) primaryPodLabelFenced(ctx context.Context, fg *v1alpha1.MysqlFailoverGroup) bool {
	if fg.Status.ActiveSite == "" {
		return false
	}
	pods := &corev1.PodList{}
	if err := r.List(ctx, pods,
		client.InNamespace(fg.Namespace),
		client.MatchingLabels{
			labelAppName:  "mysql",
			labelInstance: fg.Name,
			labelSite:     fg.Status.ActiveSite,
		},
	); err != nil {
		return false
	}
	if len(pods.Items) == 0 {
		return false
	}
	for i := range pods.Items {
		role := pods.Items[i].Labels[labelRole]
		if role == "primary" {
			return false
		}
	}
	return true
}
