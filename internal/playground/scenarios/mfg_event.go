package scenarios

import (
	"context"
	"fmt"
	"strings"
	"time"

	corev1 "k8s.io/api/core/v1"

	v1alpha1 "github.com/shipstream/bloodraven/api/v1alpha1"
	pglogs "github.com/shipstream/bloodraven/internal/playground/logs"
	"github.com/shipstream/bloodraven/internal/playground/runner"
)

// matchMFGEvent reports whether ev is a MysqlFailoverGroup event for fg
// whose reason matches, whose timestamp is at or after notBefore (with
// 2s slack for second-granularity LastTimestamp), and whose message
// contains expectedSnippet when that string is non-empty.
//
// Events with neither LastTimestamp nor EventTime set cannot prove
// freshness and never match.
func matchMFGEvent(ev corev1.Event, fg string, notBefore time.Time, expectedReason, expectedSnippet string) bool {
	if ev.InvolvedObject.Kind != "MysqlFailoverGroup" || ev.InvolvedObject.Name != fg {
		return false
	}
	if ev.Reason != expectedReason {
		return false
	}
	ts := ev.LastTimestamp.Time
	if ts.IsZero() {
		ts = ev.EventTime.Time
	}
	if ts.IsZero() || ts.Before(notBefore.Add(-2*time.Second)) {
		return false
	}
	if expectedSnippet != "" && !strings.Contains(ev.Message, expectedSnippet) {
		return false
	}
	return true
}

// findMFGEvent is one RecentEvents scan using matchMFGEvent.
func findMFGEvent(ctx context.Context, env *runner.Env, notBefore time.Time, expectedReason, expectedSnippet string) (corev1.Event, bool, error) {
	events, err := env.Kube.RecentEvents(ctx, env.Namespace, 200)
	if err != nil {
		return corev1.Event{}, false, err
	}
	for _, ev := range events {
		if matchMFGEvent(ev, env.FG, notBefore, expectedReason, expectedSnippet) {
			return ev, true, nil
		}
	}
	return corev1.Event{}, false, nil
}

// waitForMFGEvent polls the namespace's events for one whose
// involvedObject is the failover group, reason matches expectedReason,
// timestamp is at or after notBefore (minus 2s slack), and (when
// expectedSnippet is non-empty) message contains expectedSnippet.
//
// We poll rather than using a watch because the executor doesn't yet
// thread an EventClient, and the ~30s sync cadence means a 2s poll is
// well within budget.
func waitForMFGEvent(ctx context.Context, env *runner.Env, notBefore time.Time, expectedReason, expectedSnippet string) (corev1.Event, error) {
	tick := time.NewTicker(2 * time.Second)
	defer tick.Stop()
	if ev, ok, err := findMFGEvent(ctx, env, notBefore, expectedReason, expectedSnippet); err != nil {
		return corev1.Event{}, err
	} else if ok {
		return ev, nil
	}
	for {
		select {
		case <-ctx.Done():
			return corev1.Event{}, fmt.Errorf("no matching event (reason=%s snippet=%q) before deadline: %w", expectedReason, expectedSnippet, ctx.Err())
		case <-tick.C:
			if ev, ok, err := findMFGEvent(ctx, env, notBefore, expectedReason, expectedSnippet); err != nil {
				return corev1.Event{}, err
			} else if ok {
				return ev, nil
			}
		}
	}
}

// verifyNoPrimaryNotReported is the negative assertion of #291 for a
// scenario whose only write pause is one the operator opens itself (the
// handoff of an ordered update, a planned failover): the pause is reported
// as PlannedWritePause, so the run leaves no NoPrimaryDetected Event on the
// group and no "NO PRIMARY" ALERT line in the operator log.
func verifyNoPrimaryNotReported() runner.Step {
	return runner.Step{
		Phase: runner.PhaseVerify,
		Name:  "the planned write pause was not reported as NoPrimary (no Event, no ALERT)",
		Do: func(ctx context.Context, env *runner.Env) error {
			ev, found, err := findMFGEvent(ctx, env, env.StartTime, v1alpha1.EventNoPrimaryDetected, "")
			if err != nil {
				return err
			}
			if found {
				return fmt.Errorf("%s Event during a planned write pause: %s", v1alpha1.EventNoPrimaryDetected, ev.Message)
			}
			tail, err := env.Logs("operator")
			if err != nil {
				return err
			}
			if matched, line := firstMatchSince(tail, env.StartTime, pglogs.Substring("NO PRIMARY")); matched {
				return fmt.Errorf("the operator logged a NO PRIMARY alert during a planned write pause: %s", line)
			}
			env.Capture.Note("no NoPrimaryDetected Event and no NO PRIMARY alert since the scenario started")
			return nil
		},
	}
}
