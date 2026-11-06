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
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"

	v1alpha1 "github.com/shipstream/bloodraven/api/v1alpha1"
)

// PlannedFailoverAnnotation must match
// internal/controller/planned_failover.go's PlannedFailoverAnnotation
// constant. Duplicated here rather than imported so the plugin doesn't
// pull in the entire controller package.
const PlannedFailoverAnnotation = "bloodraven.shipstream.io/planned-failover"

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
                       not performed the promotion. The run already in
                       status is ignored; a refusal without a run is
                       reported from its PlannedFailoverRejected Event
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

	var fg v1alpha1.MysqlFailoverGroup
	if err := cl.Get(ctx, client.ObjectKey{Namespace: ns, Name: group}, &fg); err != nil {
		if apierrors.IsNotFound(err) {
			return fmt.Errorf("MysqlFailoverGroup %s/%s not found", ns, group)
		}
		return fmt.Errorf("get failover group: %w", err)
	}
	if !groupHasSite(&fg, site) {
		return fmt.Errorf("site %q is not defined in spec.sites of %s/%s", site, ns, group)
	}

	// Recorded before the patch, so --wait never reports the previous
	// run's outcome as this request's result.
	var baseline plannedWaitBaseline
	if wait {
		baseline = newPlannedWaitBaseline(ctx, cl, &fg, site)
	}

	patched := fg.DeepCopy()
	if patched.Annotations == nil {
		patched.Annotations = map[string]string{}
	}
	patched.Annotations[PlannedFailoverAnnotation] = value
	if err := cl.Patch(ctx, patched, client.MergeFrom(&fg)); err != nil {
		return fmt.Errorf("annotate failover group: %w", err)
	}
	fmt.Fprintf(stdout, "Annotated %s/%s with %s=%s\n", ns, group, PlannedFailoverAnnotation, value)

	if !wait {
		fmt.Fprintln(stdout, "Watch progress with: kubectl bloodraven status", group, "-n", ns)
		return nil
	}
	return waitForPlannedFailover(stdout, stderr, cl, ns, group, timeout, baseline)
}

// plannedWaitBaseline is what --wait knew before it wrote the request: the
// run already in status.plannedFailover, and the planned-failover refusal
// Events already recorded for the group.
type plannedWaitBaseline struct {
	// runID is the run in status before the patch ("" for none). Its
	// block and its terminal marker are not this request's result.
	runID string
	// follow is set when the request joins that run instead: a pending
	// (Admitting or Deferred) run to the same site absorbs the request.
	follow bool
	// events maps each refusal Event's UID to its count; nil when the
	// Events could not be listed.
	events map[types.UID]int32
}

func newPlannedWaitBaseline(ctx context.Context, cl client.Client, fg *v1alpha1.MysqlFailoverGroup, site string) plannedWaitBaseline {
	var b plannedWaitBaseline
	if pf := fg.Status.PlannedFailover; pf != nil {
		b.runID = pf.RunID
		_, _, marked := plannedTerminalMarker(fg)
		pending := pf.Phase == v1alpha1.PlannedFailoverPhaseAdmitting || pf.Phase == v1alpha1.PlannedFailoverPhaseDeferred
		b.follow = pf.RunID != "" && pf.TargetSite == site && pending && !marked
	}
	if evs, err := plannedRefusalEvents(ctx, cl, fg.Namespace, fg.Name); err == nil {
		b.events = make(map[types.UID]int32, len(evs))
		for _, ev := range evs {
			b.events[ev.UID] = ev.Count
		}
	}
	return b
}

// ours reports whether pf is the block of the run this request started
// (or joined).
func (b plannedWaitBaseline) ours(pf *v1alpha1.PlannedFailoverStatus) bool {
	return pf != nil && pf.RunID != "" && (b.follow || pf.RunID != b.runID)
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
// baseline: a new Event, or an aggregated one whose count grew.
func (b plannedWaitBaseline) newRefusalEvent(ctx context.Context, cl client.Client, ns, group string) (*corev1.Event, bool) {
	if b.events == nil {
		return nil, false
	}
	evs, err := plannedRefusalEvents(ctx, cl, ns, group)
	if err != nil {
		return nil, false
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
	return newest, newest != nil
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
// scripts don't treat it as a successful promotion.
//
// Individual Get calls run with a short request-scoped context so a
// transient apiserver blip doesn't end the wait, and so the curated
// "timed out after X waiting" message wins over a less-helpful
// "context deadline exceeded" when the parent timeout trips inside a
// Get.
//
// The run in status before the request (baseline.runID) is ignored until a
// new run appears, unless the request joined it (baseline.follow). A
// request the operator consumes without writing a run was refused with an
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
	admitted := false
	if baseline.follow {
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
		if !baseline.ours(pf) {
			// Not admitted yet, or refused without a run.
			if _, pending := fg.Annotations[PlannedFailoverAnnotation]; !pending {
				if consumedAt.IsZero() {
					consumedAt = time.Now()
				}
				if done, err := reportPlannedRefusal(ctx, stdout, stderr, cl, ns, group, baseline, time.Since(consumedAt) >= refusalEventWait); done {
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
		if !admitted && !baseline.follow {
			admitted = true
			fmt.Fprintf(stdout, "Request admitted as planned failover %s.\n", pf.RunID)
		}
		phase := v1alpha1.PlannedFailoverPhase("")
		if pf != nil {
			phase = pf.Phase
		}
		if phase != lastPhase {
			elapsed := time.Since(startedAt).Round(time.Second)
			if pf != nil && pf.Message != "" {
				fmt.Fprintf(stdout, "[%s] phase: %s — %s\n", elapsed, emptyDash(string(phase)), pf.Message)
			} else {
				fmt.Fprintf(stdout, "[%s] phase: %s\n", elapsed, emptyDash(string(phase)))
			}
			lastPhase = phase
		}
		if outcome, reason, ok := plannedTerminalMarker(&fg); ok {
			// The terminal marker ended the run while status writes were
			// failing: status may still show a non-terminal phase.
			if outcome == v1alpha1.PlannedFailoverOutcomeSucceeded {
				fmt.Fprintln(stdout, "Planned failover succeeded (ended by the terminal marker after status writes stalled; warnings: StatusStalled).")
				return nil
			}
			fmt.Fprintf(stderr, "Planned failover ended by the terminal marker after status writes stalled: outcome=%s reason=%s\n", outcome, reason)
			return fmt.Errorf("planned failover failed: %s", plannedOutcomeReason(outcome, reason))
		}
		switch phase {
		case v1alpha1.PlannedFailoverPhaseSucceeded:
			if pf != nil && pf.Result != nil && len(pf.Result.Warnings) > 0 {
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
			outcome, reason, msg := "", "", ""
			if pf != nil {
				msg = pf.Message
				if pf.Result != nil {
					outcome, reason = string(pf.Result.Outcome), string(pf.Result.Reason)
				}
			}
			fmt.Fprintf(stderr, "Planned failover failed: outcome=%s reason=%s message=%s\n", outcome, reason, msg)
			return fmt.Errorf("planned failover failed: %s", reason)
		case v1alpha1.PlannedFailoverPhaseDeferred:
			retry := "(unset)"
			if pf != nil && pf.Admission != nil && pf.Admission.RetryTime != nil {
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

// reportPlannedRefusal reports a request the operator consumed without
// writing a run, from its refusal Event. done is false while the Event has not
// arrived and giveUp is false; the caller then polls again.
func reportPlannedRefusal(ctx context.Context, stdout, stderr io.Writer, cl client.Client, ns, group string,
	baseline plannedWaitBaseline, giveUp bool) (bool, error) {
	getCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
	ev, ok := baseline.newRefusalEvent(getCtx, cl, ns, group)
	cancel()
	switch {
	case ok && ev.Reason == v1alpha1.EventPlannedFailoverSkipped:
		fmt.Fprintf(stdout, "Planned failover skipped: %s\n", ev.Message)
		return true, nil
	case ok:
		fmt.Fprintf(stderr, "Planned failover refused: %s\n", ev.Message)
		return true, fmt.Errorf("planned failover refused: %s", ev.Message)
	case giveUp:
		return true, fmt.Errorf("the operator removed the request without starting a run, and no %s Event was found; see kubectl describe mysqlfailovergroup %s -n %s",
			v1alpha1.EventPlannedFailoverRejected, group, ns)
	}
	return false, nil
}

// maxWaitGetFailures bounds how many consecutive cl.Get failures a
// wait loop will tolerate before bailing. Tuned so a brief apiserver
// blip during a 2h backup wait is survived, but a persistent outage
// still produces a clean error.
const maxWaitGetFailures = 6
