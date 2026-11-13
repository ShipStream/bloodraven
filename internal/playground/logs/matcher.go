package logs

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"regexp"
	"strconv"
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

// Or combines predicates with logical OR.
func Or(preds ...Predicate) Predicate {
	return func(line string) bool {
		for _, p := range preds {
			if p(line) {
				return true
			}
		}
		return false
	}
}

// Structured matches a structured log message and exact field values in
// either JSON or slog text output. Operator logging format differs between
// local and CI deployments, while the public msg/field contract is stable.
func Structured(msg string, fields map[string]string) Predicate {
	return func(line string) bool {
		var record map[string]any
		if json.Unmarshal([]byte(line), &record) == nil {
			if fmt.Sprint(record["msg"]) != msg {
				return false
			}
			for key, value := range fields {
				if fmt.Sprint(record[key]) != value {
					return false
				}
			}
			return true
		}

		if !textFieldMatches(line, "msg", msg) {
			return false
		}
		for key, value := range fields {
			if !textFieldMatches(line, key, value) {
				return false
			}
		}
		return true
	}
}

// textFieldMatches reports whether line contains the complete
// whitespace-delimited slog token key=value (or key="value"). Substring
// matching is not sufficient: key boundaries (oldsource=...) and value
// prefixes (source=auto-clone-old for source=auto-clone) would otherwise
// falsely satisfy chaos log assertions.
func textFieldMatches(line, key, value string) bool {
	pattern := `(^|[[:space:]])` +
		regexp.QuoteMeta(key) + `=(` +
		regexp.QuoteMeta(value) + `|` +
		regexp.QuoteMeta(strconv.Quote(value)) +
		`)($|[[:space:]])`
	return regexp.MustCompile(pattern).MatchString(line)
}

// Fields parses one structured log line into its attributes, each value as
// a string: a JSON record (what the operator and the sidecar write) or slog
// text output (key=value tokens, values quoted when they hold a space). ok
// is false when the line is neither, or carries no msg. Scenarios use it to
// read a value out of a line (a duration, say), where Structured only
// matches one.
func Fields(line string) (map[string]string, bool) {
	line = strings.TrimSpace(line)
	if strings.HasPrefix(line, "{") {
		dec := json.NewDecoder(strings.NewReader(line))
		// Numbers keep their spelling: 12 stays "12", not "1.2e+01".
		dec.UseNumber()
		var record map[string]any
		if dec.Decode(&record) == nil {
			out := make(map[string]string, len(record))
			for key, value := range record {
				out[key] = jsonFieldString(value)
			}
			_, ok := out["msg"]
			return out, ok
		}
	}
	out := textFields(line)
	_, ok := out["msg"]
	return out, ok
}

func jsonFieldString(v any) string {
	switch t := v.(type) {
	case nil:
		return ""
	case string:
		return t
	case json.Number:
		return t.String()
	case bool:
		return strconv.FormatBool(t)
	}
	// A nested group or list: its JSON form.
	b, err := json.Marshal(v)
	if err != nil {
		return fmt.Sprint(v)
	}
	return string(b)
}

// textFields reads the key=value tokens of a slog text line. A value that
// starts with a double quote runs to its closing quote and is unquoted; any
// other runs to the next space. Tokens without '=' are skipped.
func textFields(line string) map[string]string {
	out := map[string]string{}
	for i := 0; i < len(line); {
		if line[i] == ' ' || line[i] == '\t' {
			i++
			continue
		}
		end := i
		for end < len(line) && line[end] != '=' && line[end] != ' ' && line[end] != '\t' {
			end++
		}
		if end == len(line) || line[end] != '=' || end == i {
			// Free text: skip to the next space.
			for end < len(line) && line[end] != ' ' && line[end] != '\t' {
				end++
			}
			i = end
			continue
		}
		key := line[i:end]
		rest := line[end+1:]
		if strings.HasPrefix(rest, `"`) {
			if quoted, err := strconv.QuotedPrefix(rest); err == nil {
				if value, err := strconv.Unquote(quoted); err == nil {
					out[key] = value
					i = end + 1 + len(quoted)
					continue
				}
			}
		}
		stop := strings.IndexAny(rest, " \t")
		if stop < 0 {
			stop = len(rest)
		}
		out[key] = rest[:stop]
		i = end + 1 + stop
	}
	return out
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
	// Watcher goroutine that wakes us when the context fires. The
	// watcher must take t.mu before broadcasting: otherwise the
	// broadcast could race the moment between this goroutine's
	// ctx.Err() check and t.cond.Wait(), get delivered while no
	// goroutine is parked on the cond, and leave us blocked
	// indefinitely. Acquiring t.mu serializes the broadcast against
	// cond.Wait()'s atomic unlock+park, guaranteeing the wakeup
	// either reaches us or precedes the next ctx.Err() check.
	stopWatcher := make(chan struct{})
	defer close(stopWatcher)
	go func() {
		select {
		case <-deadlineCh:
			t.mu.Lock()
			t.cond.Broadcast()
			t.mu.Unlock()
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

// Watch pairs one tailer with the predicate to run against its lines
// and a label naming the component it observes. Used by WaitAny.
type Watch struct {
	// Label names the actor this watch observes ("operator",
	// "sidecar:reader"). Returned by WaitAny so the caller can report
	// which component satisfied the invariant.
	Label  string
	Tailer *Tailer
	Pred   Predicate
}

// WaitAny blocks until any watch observes a line satisfying its own
// predicate, and returns that match plus the winning watch's label.
// Use it when a single invariant may legitimately be satisfied by more
// than one component and the caller must not encode which one wins the
// race — for example a writable non-promotable site, which is fenced by
// whichever of the operator's poll loop and the site's own sidecar
// fencing monitor ticks first (issue #119).
//
// Watches whose tailer stream fails are not fatal on their own: the
// remaining watches keep waiting, and a stream error is only returned
// once no watch can still match. A stream error outranks a context
// expiry in the returned error so triage can tell a broken tailer from
// a genuine "nobody fenced" timeout.
func WaitAny(ctx context.Context, since time.Time, watches ...Watch) (Match, string, error) {
	if len(watches) == 0 {
		return Match{}, "", fmt.Errorf("WaitAny: no watches supplied")
	}
	type result struct {
		match Match
		label string
		err   error
	}
	// Cancel the losers as soon as one watch matches so their Wait
	// goroutines do not outlive this call.
	waitCtx, cancel := context.WithCancel(ctx)
	defer cancel()

	results := make(chan result, len(watches))
	for _, w := range watches {
		go func(w Watch) {
			m, err := w.Tailer.Wait(waitCtx, since, w.Pred)
			results <- result{match: m, label: w.Label, err: err}
		}(w)
	}

	var streamErr, ctxErr error
	for range watches {
		r := <-results
		switch {
		case r.err == nil:
			return r.match, r.label, nil
		case errors.Is(r.err, context.DeadlineExceeded), errors.Is(r.err, context.Canceled):
			if ctxErr == nil {
				ctxErr = r.err
			}
		default:
			if streamErr == nil {
				streamErr = fmt.Errorf("%s: %w", r.label, r.err)
			}
		}
	}
	if streamErr != nil {
		return Match{}, "", streamErr
	}
	return Match{}, "", ctxErr
}
