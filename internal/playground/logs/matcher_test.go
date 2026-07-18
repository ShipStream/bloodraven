package logs

import (
	"context"
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
