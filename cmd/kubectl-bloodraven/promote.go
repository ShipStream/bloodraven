package main

import (
	"context"
	"flag"
	"fmt"
	"io"
	"os"
	"strconv"
	"strings"
	"time"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"

	v1alpha1 "github.com/shipstream/bloodraven/api/v1alpha1"
)

const promoteUsage = `Trigger a planned (graceful) failover via annotation.

Usage:
  kubectl bloodraven promote <group> <site> [flags]

The operator drains writes on the current primary, waits for <site> to
catch up on the fenced GTID set, and promotes it. Status lands on
status.plannedFailover.phase=Succeeded with result.outcome=Succeeded. With
--wait, a run the operator ended with the planned-failover-terminal
annotation (status writes stalled) is reported from that annotation.

Per-request override (defaults to spec.plannedFailover.maxLagWait):
  --max-lag-wait DURATION   override spec.plannedFailover.maxLagWait

drainTimeout is not an annotation override — the operator only
consults spec.plannedFailover.drainTimeout. Set it on the CR if you
need a non-default value.

Flags:
  --wait               block until this request's run is terminal
                       (Succeeded / Failed); Deferred (cooldown still
                       pending) returns non-zero because the operator has
                       not performed the promotion. A run that ends
                       NotStarted/AlreadyActive (the site became active
                       first) exits 0, like a skipped request. The run
                       already in status is ignored unless the request
                       joins it (an Admitting run to the same site, or a
                       Deferred run, which resumes for this site); a run
                       for another site is never this request's result. A
                       refusal without a run is reported from its
                       PlannedFailoverRejected Event
  --timeout DURATION   timeout for --wait (default: 10m)
  --kubeconfig string  path to kubeconfig
  --context string     kubeconfig context
  --namespace, -n      namespace (default: context namespace, or "default")
`

func runPromote(args []string, stdout, stderr *os.File) error {
	fs := flag.NewFlagSet("promote", flag.ContinueOnError)
	fs.SetOutput(stderr)
	var (
		cf         commonFlags
		maxLagWait time.Duration
		wait       bool
		timeout    time.Duration
	)
	registerCommonFlags(fs, &cf)
	fs.DurationVar(&maxLagWait, "max-lag-wait", 0, "override spec.plannedFailover.maxLagWait")
	fs.BoolVar(&wait, "wait", false, "block until the planned failover reaches a terminal phase")
	fs.DurationVar(&timeout, "timeout", 10*time.Minute, "timeout for --wait")
	fs.Usage = func() { fmt.Fprint(stderr, promoteUsage) }
	positional, err := parseFlagsAndArgs(fs, args)
	if err != nil {
		return err
	}
	if len(positional) != 2 {
		fmt.Fprint(stderr, promoteUsage)
		return fmt.Errorf("promote requires exactly two positional arguments (group and site)")
	}
	group, site := positional[0], positional[1]
	if group == "" || site == "" {
		return fmt.Errorf("group and site must be non-empty")
	}

	value, err := buildPlannedFailoverValue(site, maxLagWait)
	if err != nil {
		return err
	}

	cl, ns, err := cf.newClient()
	if err != nil {
		return err
	}

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	baseline, err := annotatePlannedFailover(ctx, cl, ns, group, site, value, wait)
	if err != nil {
		return err
	}
	fmt.Fprintf(stdout, "Annotated %s/%s with %s=%s\n", ns, group, v1alpha1.PlannedFailoverAnnotation, value)

	if !wait {
		fmt.Fprintln(stdout, "Watch progress with: kubectl bloodraven status", group, "-n", ns)
		return nil
	}
	return waitForPlannedFailover(stdout, stderr, cl, ns, group, timeout, baseline)
}

// promoteConflictRetries bounds how often annotatePlannedFailover re-reads
// the group after a resourceVersion conflict.
const promoteConflictRetries = 5

// annotatePlannedFailover writes the request annotation, conditioned on the
// resourceVersion of the read it validated (and, with wait, took the
// baseline from). A conflict means the group changed in between, perhaps a
// run was admitted for another request, so the read, the validation and
// the baseline are redone: the baseline then describes exactly the state the
// request was written over.
func annotatePlannedFailover(ctx context.Context, cl client.Client, ns, group, site, value string, wait bool) (plannedWaitBaseline, error) {
	for attempt := 1; ; attempt++ {
		var fg v1alpha1.MysqlFailoverGroup
		if err := cl.Get(ctx, client.ObjectKey{Namespace: ns, Name: group}, &fg); err != nil {
			if apierrors.IsNotFound(err) {
				return plannedWaitBaseline{}, fmt.Errorf("MysqlFailoverGroup %s/%s not found", ns, group)
			}
			return plannedWaitBaseline{}, fmt.Errorf("get failover group: %w", err)
		}
		if !groupHasSite(&fg, site) {
			return plannedWaitBaseline{}, fmt.Errorf("site %q is not defined in spec.sites of %s/%s", site, ns, group)
		}
		// Recorded before the patch, so --wait never reports the previous
		// run's outcome as this request's result.
		baseline := plannedWaitBaseline{site: site}
		if wait {
			baseline = newPlannedWaitBaseline(ctx, cl, &fg, site)
		}
		patched := fg.DeepCopy()
		if patched.Annotations == nil {
			patched.Annotations = map[string]string{}
		}
		patched.Annotations[v1alpha1.PlannedFailoverAnnotation] = value
		err := cl.Patch(ctx, patched, client.MergeFromWithOptions(&fg, client.MergeFromWithOptimisticLock{}))
		switch {
		case err == nil:
			return baseline, nil
		case apierrors.IsConflict(err) && attempt < promoteConflictRetries:
			continue
		}
		return plannedWaitBaseline{}, fmt.Errorf("annotate failover group: %w", err)
	}
}

// plannedWaitBaseline is what --wait knew before it wrote the request: the
// run already in status.plannedFailover, and the planned-failover refusal
// Events already recorded for the group.
type plannedWaitBaseline struct {
	// site is the requested site. A new run is this request's only when it
	// targets site: a run admitted for a concurrent request to another site
	// is not this request's result.
	site string
	// runID is the run in status before the patch ("" for none). Its
	// block and its terminal marker are not this request's result.
	runID string
	// follow is set when the request joins that run instead: an Admitting
	// run to the same site absorbs the request, and a Deferred run re-reads
	// the annotation when it resumes, so it carries the request whatever
	// site it was deferred for.
	follow bool
	// deferredFor is the site a followed Deferred run was deferred for,
	// when that is not site (status keeps it until the run resumes).
	deferredFor string
	// hadBlock and startTime describe the block before the patch. An
	// operator older than this plugin writes no runID; its run is
	// recognised by a startTime that changed (legacyRun).
	hadBlock  bool
	startTime metav1.Time
	// events maps each refusal Event's UID to its count; nil when the
	// Events could not be listed, and eventsErr says why.
	events    map[types.UID]int32
	eventsErr error
}

func newPlannedWaitBaseline(ctx context.Context, cl client.Client, fg *v1alpha1.MysqlFailoverGroup, site string) plannedWaitBaseline {
	b := plannedWaitBaseline{site: site}
	if pf := fg.Status.PlannedFailover; pf != nil {
		b.runID, b.hadBlock, b.startTime = pf.RunID, true, pf.StartTime
		_, _, marked := plannedTerminalMarker(fg)
		switch {
		case pf.RunID == "" || marked:
		case pf.Phase == v1alpha1.PlannedFailoverPhaseAdmitting:
			// Another site's request is refused beside an Admitting run.
			b.follow = pf.TargetSite == site
		case pf.Phase == v1alpha1.PlannedFailoverPhaseDeferred:
			b.follow = true
			if pf.TargetSite != site {
				b.deferredFor = pf.TargetSite
			}
		}
	}
	evs, err := plannedRefusalEvents(ctx, cl, fg.Namespace, fg.Name)
	if err != nil {
		b.eventsErr = err
		return b
	}
	b.events = make(map[types.UID]int32, len(evs))
	for _, ev := range evs {
		b.events[ev.UID] = ev.Count
	}
	return b
}

// ours reports whether pf is the block of the run this request started
// (or joined).
func (b plannedWaitBaseline) ours(pf *v1alpha1.PlannedFailoverStatus) bool {
	switch {
	case pf == nil || pf.RunID == "":
		return false
	case pf.RunID != b.runID:
		return pf.TargetSite == b.site
	}
	return b.follow
}

// legacyRun reports a run written by an operator older than this plugin:
// a block without a runID whose startTime is not the baseline's.
func (b plannedWaitBaseline) legacyRun(pf *v1alpha1.PlannedFailoverStatus) bool {
	return pf != nil && pf.RunID == "" && !pf.StartTime.IsZero() && (!b.hadBlock || !pf.StartTime.Equal(&b.startTime))
}

// plannedRefusalEvents lists the group's PlannedFailoverRejected and
// PlannedFailoverSkipped Events: the only trace of a request the operator
// refused without writing a run (an invalid value, a target that is
// already active, a standing hold, or a request beside a running run).
func plannedRefusalEvents(ctx context.Context, cl client.Client, ns, group string) ([]corev1.Event, error) {
	var list corev1.EventList
	if err := cl.List(ctx, &list, client.InNamespace(ns), client.MatchingFields{"involvedObject.name": group}); err != nil {
		return nil, err
	}
	var out []corev1.Event
	for _, ev := range list.Items {
		if ev.InvolvedObject.Kind != "MysqlFailoverGroup" || ev.InvolvedObject.Name != group {
			continue
		}
		if ev.Reason == v1alpha1.EventPlannedFailoverRejected || ev.Reason == v1alpha1.EventPlannedFailoverSkipped {
			out = append(out, ev)
		}
	}
	return out, nil
}

// newRefusalEvent returns the newest refusal Event recorded after the
// baseline: a new Event, or an aggregated one whose count grew. err is why
// the Events could not be read (at the baseline or now).
func (b plannedWaitBaseline) newRefusalEvent(ctx context.Context, cl client.Client, ns, group string) (*corev1.Event, error) {
	if b.events == nil {
		return nil, b.eventsErr
	}
	evs, err := plannedRefusalEvents(ctx, cl, ns, group)
	if err != nil {
		return nil, err
	}
	var newest *corev1.Event
	for i := range evs {
		ev := &evs[i]
		if seen, ok := b.events[ev.UID]; ok && ev.Count <= seen {
			continue
		}
		if newest == nil || eventTime(ev).After(eventTime(newest)) {
			newest = ev
		}
	}
	return newest, nil
}

func eventTime(ev *corev1.Event) time.Time {
	switch {
	case !ev.LastTimestamp.IsZero():
		return ev.LastTimestamp.Time
	case !ev.EventTime.IsZero():
		return ev.EventTime.Time
	}
	return ev.FirstTimestamp.Time
}

// refusalEventWait bounds how long --wait looks for the Event of a
// request the operator consumed without a run: Events are recorded
// asynchronously, after the annotation is removed. plannedWaitPoll is the
// wait's poll interval. Both are variables for the tests.
var (
	refusalEventWait = 10 * time.Second
	plannedWaitPoll  = 2 * time.Second
)

// buildPlannedFailoverValue assembles the annotation value the operator
// parses. Mirrors the controller's grammar exactly: "<site>" or
// "<site>:key=value[:key=value...]". Caller is expected to validate
// the site. The operator currently recognises only the "maxLagWait"
// key (see internal/controller/planned_failover.go); other overrides
// would be rejected.
func buildPlannedFailoverValue(site string, maxLagWait time.Duration) (string, error) {
	if site == "" {
		return "", fmt.Errorf("site is required")
	}
	pairs := []kv{}
	if maxLagWait > 0 {
		// The annotation grammar admits only [A-Za-z0-9._/+-] in a value;
		// time.Duration renders microseconds with "µs", so a sub-millisecond
		// value is written in nanoseconds instead.
		d := maxLagWait.String()
		if strings.ContainsRune(d, 'µ') {
			d = strconv.FormatInt(int64(maxLagWait), 10) + "ns"
		}
		pairs = append(pairs, kv{"maxLagWait", d})
	}
	if len(pairs) == 0 {
		return site, nil
	}
	return site + ":" + joinKVs(pairs), nil
}

func groupHasSite(fg *v1alpha1.MysqlFailoverGroup, site string) bool {
	for _, s := range fg.Spec.Sites {
		if s.Name == site {
			return true
		}
	}
	return false
}

// waitForPlannedFailover polls every 2s until the planned-failover
// state machine reaches a terminal phase or the caller-supplied
// timeout expires. Succeeded / Failed are operator-terminal; Deferred
// is terminal *for this wait* but the operator will retry the request
// once the anti-flap cooldown ticks — it is NOT operator-terminal, so
// we surface the retry time and return a non-zero error to make sure
// scripts don't treat it as a successful promotion. A run that ended
// NotStarted because its target already became the active primary is
// reported like a skipped request (exit 0): the requested end state holds.
//
// Individual Get calls run with a short request-scoped context so a
// transient apiserver blip doesn't end the wait, and so the curated
// "timed out after X waiting" message wins over a less-helpful
// "context deadline exceeded" when the parent timeout trips inside a
// Get.
//
// The run in status before the request (baseline.runID) is ignored until a
// new run for the requested site appears, unless the request joined it
// (baseline.follow). Once a run is adopted the wait reports only that run.
// A request the operator consumes without writing a run was refused with an
// Event only; the wait reports that Event, or says it could not find it.
func waitForPlannedFailover(stdout, stderr io.Writer, cl client.Client, ns, group string, timeout time.Duration, baseline plannedWaitBaseline) error {
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()

	lastPhase := v1alpha1.PlannedFailoverPhase("")
	tick := time.NewTicker(plannedWaitPoll)
	defer tick.Stop()
	startedAt := time.Now()
	consecutiveErrors := 0
	var consumedAt time.Time
	adopted, legacy := "", false
	switch {
	case baseline.follow && baseline.deferredFor != "":
		fmt.Fprintf(stdout, "The request replaces the target of deferred planned failover %s (was %q); it runs for %q when it resumes.\n",
			baseline.runID, baseline.deferredFor, baseline.site)
	case baseline.follow:
		fmt.Fprintf(stdout, "The request joins pending planned failover %s.\n", baseline.runID)
	}

	for {
		var fg v1alpha1.MysqlFailoverGroup
		getCtx, getCancel := context.WithTimeout(ctx, 10*time.Second)
		err := cl.Get(getCtx, client.ObjectKey{Namespace: ns, Name: group}, &fg)
		getCancel()
		if err != nil {
			if apierrors.IsNotFound(err) {
				return fmt.Errorf("MysqlFailoverGroup %s/%s disappeared during wait", ns, group)
			}
			if ctx.Err() != nil {
				return fmt.Errorf("timed out after %s waiting for planned failover to complete (last phase: %s)", timeout, lastPhase)
			}
			consecutiveErrors++
			if consecutiveErrors >= maxWaitGetFailures {
				return fmt.Errorf("get failover group during wait: %w (gave up after %d consecutive failures)", err, consecutiveErrors)
			}
			fmt.Fprintf(stderr, "warning: get %s/%s failed: %v; retrying in 2s\n", ns, group, err)
			select {
			case <-ctx.Done():
				return fmt.Errorf("timed out after %s waiting for planned failover to complete (last phase: %s)", timeout, lastPhase)
			case <-tick.C:
				continue
			}
		}
		consecutiveErrors = 0
		pf := fg.Status.PlannedFailover
		_, pending := fg.Annotations[v1alpha1.PlannedFailoverAnnotation]
		_, _, marked := plannedTerminalMarker(&fg)
		switch {
		case legacy:
			if !baseline.legacyRun(pf) {
				return fmt.Errorf("the planned failover this request started was replaced in status before its result was read; see kubectl describe mysqlfailovergroup %s -n %s", group, ns)
			}
		case adopted != "" && adopted == baseline.runID && pf != nil && pf.RunID != adopted && baseline.ours(pf):
			// The followed run ended without absorbing the request, and the
			// operator admitted the request as a new run.
			adopted = pf.RunID
			fmt.Fprintf(stdout, "Request admitted as planned failover %s.\n", pf.RunID)
		case adopted != "":
			if pf == nil || pf.RunID != adopted {
				return fmt.Errorf("planned failover %s was replaced in status before its result was read; see kubectl describe mysqlfailovergroup %s -n %s", adopted, group, ns)
			}
		case baseline.ours(pf):
			adopted = pf.RunID
			if pf.RunID != baseline.runID {
				fmt.Fprintf(stdout, "Request admitted as planned failover %s.\n", pf.RunID)
			}
		case baseline.legacyRun(pf):
			// An operator older than this plugin writes no runID.
			legacy = true
			fmt.Fprintln(stderr, "warning: the operator wrote status.plannedFailover without a runID: it is older than this plugin; following the run it started by its start time")
		default:
			// Not admitted yet, or refused without a run.
			if !pending {
				if consumedAt.IsZero() {
					consumedAt = time.Now()
				}
				if done, err := reportPlannedRefusal(ctx, stdout, stderr, cl, ns, group, baseline, pf, time.Since(consumedAt) >= refusalEventWait); done {
					return err
				}
			}
			select {
			case <-ctx.Done():
				if consumedAt.IsZero() {
					return fmt.Errorf("timed out after %s: the operator has not admitted the request (no new planned-failover run; the annotation is still set: is the operator running?)", timeout)
				}
				return fmt.Errorf("timed out after %s: the operator removed the request without starting a run; see kubectl describe mysqlfailovergroup %s -n %s", timeout, group, ns)
			case <-tick.C:
				continue
			}
		}
		if !legacy && adopted == baseline.runID && pending && (marked || plannedPhaseTerminal(pf.Phase)) {
			// The followed run ended while the request is still set: the
			// run's last pass may still consume it (its result is then this
			// request's), or the run ended without absorbing it and the
			// operator admits it as a new run. Wait for the request to go.
			select {
			case <-ctx.Done():
				return fmt.Errorf("timed out after %s: planned failover %s ended but the operator has not taken the request (the annotation is still set)", timeout, adopted)
			case <-tick.C:
				continue
			}
		}
		phase := pf.Phase
		if phase != lastPhase {
			elapsed := time.Since(startedAt).Round(time.Second)
			if pf.Message != "" {
				fmt.Fprintf(stdout, "[%s] phase: %s — %s\n", elapsed, emptyDash(string(phase)), pf.Message)
			} else {
				fmt.Fprintf(stdout, "[%s] phase: %s\n", elapsed, emptyDash(string(phase)))
			}
			lastPhase = phase
		}
		if outcome, reason, ok := plannedTerminalMarker(&fg); ok {
			// The terminal marker ended the run while status writes were
			// failing: status may still show a non-terminal phase.
			switch {
			case outcome == v1alpha1.PlannedFailoverOutcomeSucceeded:
				fmt.Fprintln(stdout, "Planned failover succeeded (ended by the terminal marker after status writes stalled; warnings: StatusStalled).")
				return nil
			case plannedAlreadyActive(outcome, reason):
				fmt.Fprintf(stdout, "Planned failover skipped: site %q became the active primary before the run started (ended by the terminal marker).\n", baseline.site)
				return nil
			}
			fmt.Fprintf(stderr, "Planned failover ended by the terminal marker after status writes stalled: outcome=%s reason=%s\n", outcome, reason)
			return fmt.Errorf("planned failover failed: %s", plannedOutcomeReason(outcome, reason))
		}
		switch phase {
		case v1alpha1.PlannedFailoverPhaseSucceeded:
			if pf.Result != nil && len(pf.Result.Warnings) > 0 {
				ws := make([]string, len(pf.Result.Warnings))
				for i, w := range pf.Result.Warnings {
					ws[i] = string(w)
				}
				fmt.Fprintf(stdout, "Planned failover succeeded (warnings: %s).\n", strings.Join(ws, ", "))
				return nil
			}
			fmt.Fprintln(stdout, "Planned failover succeeded.")
			return nil
		case v1alpha1.PlannedFailoverPhaseFailed:
			outcome, reason := "", ""
			if pf.Result != nil {
				outcome, reason = string(pf.Result.Outcome), string(pf.Result.Reason)
				if plannedAlreadyActive(pf.Result.Outcome, pf.Result.Reason) {
					fmt.Fprintf(stdout, "Planned failover skipped: site %q became the active primary before the run started.\n", baseline.site)
					return nil
				}
			}
			fmt.Fprintf(stderr, "Planned failover failed: outcome=%s reason=%s message=%s\n", outcome, reason, pf.Message)
			if legacy {
				return fmt.Errorf("planned failover failed: %s", pf.Message)
			}
			return fmt.Errorf("planned failover failed: %s", reason)
		case v1alpha1.PlannedFailoverPhaseDeferred:
			retry := "(unset)"
			if pf.Admission != nil && pf.Admission.RetryTime != nil {
				retry = pf.Admission.RetryTime.Time.UTC().Format(time.RFC3339)
			}
			fmt.Fprintf(stderr, "Planned failover deferred; the operator will retry after %s — rerun status to check.\n", retry)
			return fmt.Errorf("planned failover deferred until %s; the operator has not performed the promotion yet", retry)
		}
		select {
		case <-ctx.Done():
			return fmt.Errorf("timed out after %s waiting for planned failover to complete (last phase: %s)", timeout, lastPhase)
		case <-tick.C:
		}
	}
}

func plannedPhaseTerminal(p v1alpha1.PlannedFailoverPhase) bool {
	return p == v1alpha1.PlannedFailoverPhaseSucceeded || p == v1alpha1.PlannedFailoverPhaseFailed
}

// plannedAlreadyActive: the run never started because its target already
// was the active primary. A request refused for that reason before a run
// was written is Skipped (exit 0); a run that learns it after being stamped
// ends this way, and --wait reports both alike.
func plannedAlreadyActive(outcome v1alpha1.PlannedFailoverOutcome, reason v1alpha1.PlannedFailoverReason) bool {
	return outcome == v1alpha1.PlannedFailoverOutcomeNotStarted && reason == v1alpha1.PlannedFailoverReasonAlreadyActive
}

// reportPlannedRefusal reports a request the operator consumed without
// writing a run, from its refusal Event. done is false while the Event has not
// arrived and giveUp is false; the caller then polls again. cur is the block
// in status, named when a run for another site took the request's place.
func reportPlannedRefusal(ctx context.Context, stdout, stderr io.Writer, cl client.Client, ns, group string,
	baseline plannedWaitBaseline, cur *v1alpha1.PlannedFailoverStatus, giveUp bool) (bool, error) {
	getCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
	ev, listErr := baseline.newRefusalEvent(getCtx, cl, ns, group)
	cancel()
	switch {
	case ev != nil && ev.Reason == v1alpha1.EventPlannedFailoverSkipped:
		fmt.Fprintf(stdout, "Planned failover skipped: %s\n", ev.Message)
		return true, nil
	case ev != nil:
		fmt.Fprintf(stderr, "Planned failover refused: %s\n", ev.Message)
		return true, fmt.Errorf("planned failover refused: %s", ev.Message)
	case !giveUp:
		return false, nil
	}
	other := ""
	if cur != nil && cur.RunID != "" && cur.RunID != baseline.runID && cur.TargetSite != baseline.site {
		other = fmt.Sprintf(" (planned failover %s runs for site %q, from another request)", cur.RunID, cur.TargetSite)
	}
	if listErr != nil {
		return true, fmt.Errorf("the operator removed the request without starting a run%s, and its refusal Event could not be read: %w; see kubectl describe mysqlfailovergroup %s -n %s",
			other, listErr, group, ns)
	}
	return true, fmt.Errorf("the operator removed the request without starting a run%s, and no %s Event was found; see kubectl describe mysqlfailovergroup %s -n %s",
		other, v1alpha1.EventPlannedFailoverRejected, group, ns)
}

// maxWaitGetFailures bounds how many consecutive cl.Get failures a
// wait loop will tolerate before bailing. Tuned so a brief apiserver
// blip during a 2h backup wait is survived, but a persistent outage
// still produces a clean error.
const maxWaitGetFailures = 6
