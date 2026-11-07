package controller

import (
	"sync"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

// retentionGraceClock decides when terminalRetentionGrace starts for a
// terminal backup or verification. The grace protects a verdict until
// readers have seen it, so it counts from the latest of:
//
//   - the run's status.completionTime, which for a Job-driven run is the
//     Job's own completion and can be long past when the operator observes
//     the Job late (operator restart, leader change, reconcile backlog);
//   - when this process wrote the verdict (recordVerdict);
//   - when this process first evaluated a grace (once countFromFirstUse is
//     set): reconciles run only on the leader, so this is no earlier than
//     when this process became leader, and it covers a verdict a previous
//     leader wrote shortly before it stopped.
//
// It is in-memory on purpose: it can only lengthen a grace, a lost record
// falls back to the first use, and no API field or annotation is needed
// to carry a two-minute timer. The zero value is ready to use and counts
// from completionTime and recorded verdicts only; SetupWithManager calls
// countFromFirstUse. Unit tests that build a reconciler literal get the
// zero value.
type retentionGraceClock struct {
	mu       sync.Mutex
	fromUse  bool
	firstUse time.Time
	verdicts map[string]time.Time
}

// retentionGraceKey identifies one incarnation of an object: a re-created
// object of the same name has a new UID and no recorded verdict.
func retentionGraceKey(obj client.Object) string {
	return obj.GetNamespace() + "/" + obj.GetName() + "/" + string(obj.GetUID())
}

// countFromFirstUse makes every grace also count from the first call to
// left in this process.
func (c *retentionGraceClock) countFromFirstUse() {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.fromUse = true
}

// recordVerdict records that this process wrote obj's terminal status at
// now. Records whose grace has ended are dropped, so the map only holds
// verdicts written in the last terminalRetentionGrace.
func (c *retentionGraceClock) recordVerdict(obj client.Object, now time.Time) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.verdicts == nil {
		c.verdicts = map[string]time.Time{}
	}
	for k, at := range c.verdicts {
		if !now.Before(at.Add(terminalRetentionGrace)) {
			delete(c.verdicts, k)
		}
	}
	c.verdicts[retentionGraceKey(obj)] = now
}

// left is how much of terminalRetentionGrace is left at now for obj, which
// completed at completion, or 0. An object without a completion time, and
// without a recorded verdict or a first use within the grace, has none.
func (c *retentionGraceClock) left(obj client.Object, completion *metav1.Time, now time.Time) time.Duration {
	var start time.Time
	if completion != nil {
		start = completion.Time
	}
	c.mu.Lock()
	if at, ok := c.verdicts[retentionGraceKey(obj)]; ok && at.After(start) {
		start = at
	}
	if c.fromUse && c.firstUse.IsZero() {
		c.firstUse = now
	}
	if c.firstUse.After(start) {
		start = c.firstUse
	}
	c.mu.Unlock()
	if start.IsZero() {
		return 0
	}
	if left := start.Add(terminalRetentionGrace).Sub(now); left > 0 {
		return left
	}
	return 0
}
