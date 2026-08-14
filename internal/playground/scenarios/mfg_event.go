package scenarios

import (
	"context"
	"fmt"
	"strings"
	"time"

	corev1 "k8s.io/api/core/v1"

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
