package logs

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"
)

func TestTailerWaitMatchesAfterInjection(t *testing.T) {
	tail := New(Source{Pod: "p"}, 16)

	// Pre-existing line BEFORE the since cutoff.
	tail.append(Match{Time: time.Now().Add(-10 * time.Second), Line: `msg="failover complete"`})
	since := time.Now()

	// Line that arrives after since but does not match.
	go func() {
		time.Sleep(10 * time.Millisecond)
		tail.append(Match{Time: time.Now(), Line: `msg="state transition"`})
		time.Sleep(10 * time.Millisecond)
		tail.append(Match{Time: time.Now(), Line: `msg="failover complete" target="pdx"`})
	}()

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	got, err := tail.Wait(ctx, since, Substring(`msg="failover complete"`))
	if err != nil {
		t.Fatalf("Wait err: %v", err)
	}
	if got.Time.Before(since) {
		t.Fatalf("returned match predates since cutoff: %v < %v", got.Time, since)
	}
	if got.Line == "" {
		t.Fatalf("returned match has empty line")
	}
}

func TestTailerWaitTimeoutReturnsCtxErr(t *testing.T) {
	tail := New(Source{Pod: "p"}, 16)
	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	_, err := tail.Wait(ctx, time.Now(), Substring("never appears"))
	if err == nil {
		t.Fatalf("expected timeout error, got nil")
	}
}

func TestStructuredMatchesJSONAndText(t *testing.T) {
	pred := Structured("starting bootstrap", map[string]string{
		"source":    "auto-clone",
		"recipient": "reader",
	})
	for _, line := range []string{
		`{"msg":"starting bootstrap","source":"auto-clone","recipient":"reader"}`,
		`time=now level=INFO msg="starting bootstrap" source=auto-clone recipient=reader`,
	} {
		if !pred(line) {
			t.Errorf("Structured predicate did not match %q", line)
		}
	}
	if pred(`{"msg":"starting bootstrap","source":"reclone","recipient":"reader"}`) {
		t.Error("Structured predicate matched the wrong source")
	}
}

func TestStructuredRejectsPartialTextTokens(t *testing.T) {
	pred := Structured("starting bootstrap", map[string]string{
		"source":    "auto-clone",
		"recipient": "reader",
	})
	for _, line := range []string{
		// Value prefix must not satisfy the exact value.
		`msg="starting bootstrap" source=auto-clone-old recipient=reader`,
		// Key prefix must not satisfy the exact key.
		`msg="starting bootstrap" oldsource=auto-clone recipient=reader`,
		// A matching value on a different key must not leak across fields.
		`msg="starting bootstrap" source=auto-clone recipient=reader2`,
	} {
		if pred(line) {
			t.Errorf("Structured predicate matched partial token in %q", line)
		}
	}
	// Quoted values with spaces still match as a single token.
	quoted := Structured("replication source convergence complete", map[string]string{"site": "reader"})
	if !quoted(`time=now level=INFO msg="replication source convergence complete" site=reader`) {
		t.Error("Structured predicate rejected quoted msg containing spaces")
	}
}

func TestWaitAnyReturnsFirstMatchingWatch(t *testing.T) {
	operator := New(Source{Pod: "operator"}, 16)
	sidecar := New(Source{Pod: "sidecar"}, 16)
	since := time.Now()

	go func() {
		time.Sleep(10 * time.Millisecond)
		// The loser emits an unrelated line; only the sidecar fences.
		operator.append(Match{Time: time.Now(), Line: `msg="state transition"`})
		sidecar.append(Match{Time: time.Now(), Line: `msg="SELF-FENCING: topology mismatch" site=reader`})
	}()

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	m, label, err := WaitAny(ctx, since,
		Watch{Label: "operator", Tailer: operator, Pred: Substring("fenced writable non-promotable site")},
		Watch{Label: "sidecar:reader", Tailer: sidecar, Pred: Substring("SELF-FENCING: topology mismatch")},
	)
	if err != nil {
		t.Fatalf("WaitAny err: %v", err)
	}
	if label != "sidecar:reader" {
		t.Errorf("WaitAny label = %q, want sidecar:reader", label)
	}
	if !strings.Contains(m.Line, "SELF-FENCING") {
		t.Errorf("WaitAny returned the wrong line: %q", m.Line)
	}
}

func TestWaitAnyTimesOutWhenNoWatchMatches(t *testing.T) {
	a := New(Source{Pod: "a"}, 16)
	b := New(Source{Pod: "b"}, 16)
	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	if _, _, err := WaitAny(ctx, time.Now(),
		Watch{Label: "a", Tailer: a, Pred: Substring("never")},
		Watch{Label: "b", Tailer: b, Pred: Substring("never")},
	); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("WaitAny err = %v, want DeadlineExceeded", err)
	}
}

// A dead tailer must not sink the whole wait: the surviving watch still
// gets to match, and its result wins.
func TestWaitAnySurvivesOneDeadTailer(t *testing.T) {
	dead := New(Source{Pod: "dead"}, 16)
	dead.markDone(errors.New("stream broke"))
	live := New(Source{Pod: "live"}, 16)
	since := time.Now()

	go func() {
		time.Sleep(10 * time.Millisecond)
		live.append(Match{Time: time.Now(), Line: `msg="fenced writable non-promotable site" site=reader`})
	}()

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	_, label, err := WaitAny(ctx, since,
		Watch{Label: "dead", Tailer: dead, Pred: Substring("anything")},
		Watch{Label: "live", Tailer: live, Pred: Substring("fenced writable non-promotable site")},
	)
	if err != nil {
		t.Fatalf("WaitAny err: %v", err)
	}
	if label != "live" {
		t.Errorf("WaitAny label = %q, want live", label)
	}
}

// When nothing matches, a broken stream is reported ahead of the plain
// context expiry so triage can tell a dead tailer from a real timeout.
func TestWaitAnyPrefersStreamErrorOverTimeout(t *testing.T) {
	dead := New(Source{Pod: "dead"}, 16)
	dead.markDone(errors.New("stream broke"))
	live := New(Source{Pod: "live"}, 16)

	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	_, _, err := WaitAny(ctx, time.Now(),
		Watch{Label: "dead", Tailer: dead, Pred: Substring("never")},
		Watch{Label: "live", Tailer: live, Pred: Substring("never")},
	)
	if err == nil || !strings.Contains(err.Error(), "stream broke") {
		t.Fatalf("WaitAny err = %v, want the stream error", err)
	}
}

// TestFieldsParsesJSONAndText pins the two formats scenarios read values
// from: the operator's and sidecar's JSON records and slog text output.
func TestFieldsParsesJSONAndText(t *testing.T) {
	cases := []struct {
		name string
		line string
		ok   bool
		want map[string]string
	}{
		{
			name: "operator JSON record",
			line: `{"time":"2026-11-10T10:00:00.123456789Z","level":"INFO","msg":"promotion binlog rotated","site":"pdx","durationMs":12}`,
			ok:   true,
			want: map[string]string{"msg": "promotion binlog rotated", "site": "pdx", "durationMs": "12", "level": "INFO"},
		},
		{
			name: "JSON bool, large number and null",
			line: `{"msg":"promotion binlog rotation failed","timedOut":true,"durationMs":1000,"waitedMs":12345678901,"error":null}`,
			ok:   true,
			want: map[string]string{"timedOut": "true", "durationMs": "1000", "waitedMs": "12345678901", "error": ""},
		},
		{
			name: "JSON nested group keeps its JSON form",
			line: `{"msg":"x","group":{"a":1}}`,
			ok:   true,
			want: map[string]string{"group": `{"a":1}`},
		},
		{
			name: "slog text",
			line: `time=2026-11-10T10:00:00.123Z level=INFO msg="promotion binlog rotated" site=pdx durationMs=12`,
			ok:   true,
			want: map[string]string{"msg": "promotion binlog rotated", "site": "pdx", "durationMs": "12", "level": "INFO"},
		},
		{
			name: "slog text with a quoted value holding spaces, '=' and an escaped quote",
			line: `level=WARN msg="promotion binlog rotation failed" site=pdx durationMs=1000 timedOut=true error="no answer within 1s; a=\"b\" continuing"`,
			ok:   true,
			want: map[string]string{"timedOut": "true", "error": `no answer within 1s; a="b" continuing`, "durationMs": "1000"},
		},
		{
			name: "text with a leading free token",
			line: `I1110 level=INFO msg=ready site=iad`,
			ok:   true,
			want: map[string]string{"msg": "ready", "site": "iad"},
		},
		{name: "JSON without msg", line: `{"site":"pdx","durationMs":12}`, ok: false},
		{name: "text without msg", line: `site=pdx durationMs=12`, ok: false},
		{name: "plain text", line: `mysqld: ready for connections.`, ok: false},
		{name: "empty", line: ``, ok: false},
		{name: "truncated JSON", line: `{"msg":"promotion binlog rotated","site":"pd`, ok: false},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			got, ok := Fields(c.line)
			if ok != c.ok {
				t.Fatalf("ok=%v, want %v (fields %v)", ok, c.ok, got)
			}
			for key, want := range c.want {
				if got[key] != want {
					t.Errorf("field %s=%q, want %q", key, got[key], want)
				}
			}
		})
	}
}

// TestFieldsAgreesWithStructured: whatever Structured matches, Fields reads
// the same values from.
func TestFieldsAgreesWithStructured(t *testing.T) {
	for _, line := range []string{
		`{"msg":"pre-reclone archive tail archived","site":"iad","waitedMs":4012}`,
		`time=now level=INFO msg="pre-reclone archive tail archived" site=iad waitedMs=4012`,
	} {
		if !Structured("pre-reclone archive tail archived", map[string]string{"site": "iad", "waitedMs": "4012"})(line) {
			t.Fatalf("Structured does not match %q", line)
		}
		got, ok := Fields(line)
		if !ok || got["msg"] != "pre-reclone archive tail archived" || got["site"] != "iad" || got["waitedMs"] != "4012" {
			t.Errorf("Fields(%q)=%v ok=%v", line, got, ok)
		}
	}
}
