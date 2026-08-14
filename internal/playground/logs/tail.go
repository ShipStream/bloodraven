// Package logs streams a pod container's logs into a ring-buffered
// matcher so scenarios can wait for documented msg= lines from the
// public log contract (site/content/docs/8.observability/7.log-schema.md).
package logs

import (
	"bufio"
	"context"
	"fmt"
	"io"
	"strings"
	"sync"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	pgkube "github.com/shipstream/bloodraven/internal/playground/kube"
)

// Source identifies the pod and container a Tailer is attached to.
type Source struct {
	Namespace string
	Pod       string
	Container string
	// SinceTime, when non-zero, is forwarded to the kube API as
	// PodLogOptions.SinceTime so the stream skips lines emitted
	// before that moment. Used by the runner to scope each
	// scenario's tailer to its own start time, preventing log lines
	// from prior scenarios from contaminating the ring buffer.
	SinceTime time.Time
}

// Match is one line that satisfied a pattern, with the time the line
// was observed (not parsed from the line itself).
type Match struct {
	Time time.Time
	Line string
}

// Tailer streams a single container's logs. Lines are buffered into
// a ring; matchers block until a line satisfying their predicate
// arrives or the context expires.
type Tailer struct {
	Source Source

	mu   sync.Mutex
	cond *sync.Cond
	ring []Match
	cap  int
	done bool
	err  error
}

// New creates a Tailer with a ring buffer of capacity cap.
func New(src Source, cap int) *Tailer {
	if cap <= 0 {
		cap = 4096
	}
	t := &Tailer{Source: src, cap: cap}
	t.cond = sync.NewCond(&t.mu)
	return t
}

// Snapshot returns a copy of the ring buffer. Used by forensic
// capture to emit the last N matched lines.
func (t *Tailer) Snapshot() []Match {
	t.mu.Lock()
	defer t.mu.Unlock()
	out := make([]Match, len(t.ring))
	copy(out, t.ring)
	return out
}

// Start opens a streaming log read on the source pod and feeds lines
// into the ring buffer. Returns immediately; cancel ctx to stop.
//
// On EOF (pod restart, e.g. after operator-kill), Start retries the
// stream every second until ctx is done. Lines are timestamped with
// the kubelet's server-side timestamp (PodLogOptions.Timestamps=true)
// so callers' Wait(since=...) filtering reflects the *emission* time
// of the log line, not when our reader happened to drain it. Without
// server-side timestamps, backlog lines appended to the ring during
// initial buffering would be tagged with time.Now() and could falsely
// satisfy a Wait whose `since` predates the inject step.
func (t *Tailer) Start(ctx context.Context, k *pgkube.Client) {
	go t.run(ctx, k)
}

func (t *Tailer) run(ctx context.Context, k *pgkube.Client) {
	for {
		if ctx.Err() != nil {
			t.markDone(ctx.Err())
			return
		}
		err := t.readOne(ctx, k)
		if err != nil && ctx.Err() != nil {
			t.markDone(ctx.Err())
			return
		}
		// Brief backoff on restart so we do not hammer the API
		// server when the pod is bouncing.
		select {
		case <-ctx.Done():
			t.markDone(ctx.Err())
			return
		case <-time.After(time.Second):
		}
	}
}

func (t *Tailer) markDone(err error) {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.done = true
	t.err = err
	t.cond.Broadcast()
}

func (t *Tailer) readOne(ctx context.Context, k *pgkube.Client) error {
	follow := true
	opts := &corev1.PodLogOptions{
		Container:  t.Source.Container,
		Follow:     follow,
		Timestamps: true,
	}
	if !t.Source.SinceTime.IsZero() {
		opts.SinceTime = &metav1.Time{Time: t.Source.SinceTime}
	}
	req := k.Kubernetes.CoreV1().Pods(t.Source.Namespace).GetLogs(t.Source.Pod, opts)
	stream, err := req.Stream(ctx)
	if err != nil {
		return err
	}
	defer stream.Close()
	r := bufio.NewReader(stream)
	for {
		line, err := r.ReadString('\n')
		if line != "" {
			ts, body := splitTimestamp(trimNewline(line))
			t.append(Match{Time: ts, Line: body})
		}
		if err != nil {
			if err == io.EOF || ctx.Err() != nil {
				return nil
			}
			return fmt.Errorf("read log line: %w", err)
		}
	}
}

func trimNewline(s string) string {
	for len(s) > 0 && (s[len(s)-1] == '\n' || s[len(s)-1] == '\r') {
		s = s[:len(s)-1]
	}
	return s
}

// splitTimestamp parses the kubelet-prepended RFC3339Nano timestamp
// from `<ts> <body>`. Falls back to time.Now() and the unmodified
// line if the prefix is malformed (defensive: the kubelet's format
// is documented but a future server quirk shouldn't drop log lines
// on the floor).
func splitTimestamp(s string) (time.Time, string) {
	sp := strings.IndexByte(s, ' ')
	if sp <= 0 {
		return time.Now(), s
	}
	ts, err := time.Parse(time.RFC3339Nano, s[:sp])
	if err != nil {
		return time.Now(), s
	}
	return ts, s[sp+1:]
}

func (t *Tailer) append(m Match) {
	t.mu.Lock()
	defer t.mu.Unlock()
	if len(t.ring) == t.cap {
		copy(t.ring, t.ring[1:])
		t.ring[len(t.ring)-1] = m
	} else {
		t.ring = append(t.ring, m)
	}
	t.cond.Broadcast()
}
