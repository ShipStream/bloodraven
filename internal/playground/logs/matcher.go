package logs

import (
	"context"
	"regexp"
	"strings"
	"time"
)

// Predicate is a per-line decision function. Return true to stop
// scanning; false to keep waiting for a future line.
type Predicate func(line string) bool

// Substring builds a predicate that matches a literal substring.
func Substring(needle string) Predicate {
	return func(line string) bool { return strings.Contains(line, needle) }
}

// Regex builds a predicate from a compiled regex.
func Regex(re *regexp.Regexp) Predicate {
	return func(line string) bool { return re.MatchString(line) }
}

// And combines predicates with logical AND.
func And(preds ...Predicate) Predicate {
	return func(line string) bool {
		for _, p := range preds {
			if !p(line) {
				return false
			}
		}
		return true
	}
}

// Wait blocks until a line matching the predicate is observed in the
// tailer's ring buffer, or the context expires. The search begins at
// the supplied since time — callers pass time.Now() before injecting
// chaos so they only match lines that were emitted after the
// injection.
func (t *Tailer) Wait(ctx context.Context, since time.Time, pred Predicate) (Match, error) {
	deadlineCh := ctx.Done()

	t.mu.Lock()
	defer t.mu.Unlock()

	scanIdx := 0
	// Watcher goroutine that wakes us when the context fires.
	stopWatcher := make(chan struct{})
	defer close(stopWatcher)
	go func() {
		select {
		case <-deadlineCh:
			t.cond.Broadcast()
		case <-stopWatcher:
		}
	}()

	for {
		// Advance scanIdx if the buffer has rotated under us.
		if scanIdx > len(t.ring) {
			scanIdx = len(t.ring)
		}
		for ; scanIdx < len(t.ring); scanIdx++ {
			m := t.ring[scanIdx]
			if !since.IsZero() && m.Time.Before(since) {
				continue
			}
			if pred(m.Line) {
				return m, nil
			}
		}
		if ctx.Err() != nil {
			return Match{}, ctx.Err()
		}
		if t.done {
			return Match{}, t.err
		}
		t.cond.Wait()
	}
}
