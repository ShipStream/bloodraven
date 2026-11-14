package logs

import (
	"context"
	"errors"
	"strings"
	"testing"
)

func lines(t *testing.T, tail *Tailer) []string {
	t.Helper()
	var out []string
	for _, m := range tail.Snapshot() {
		out = append(out, m.Line)
	}
	return out
}

// A reconnect to the same pod replays from the last second read; the lines
// already appended must not be appended again (the forensics of #236 had
// the whole buffer repeated once per reconnect).
func TestFeed_ReconnectSkipsLinesAlreadyRead(t *testing.T) {
	tail := New(Source{Pod: "p"}, 16)
	tail.pod = "p"
	ctx := context.Background()
	first := "2026-11-12T21:41:01.100000000Z a\n" +
		"2026-11-12T21:41:01.200000000Z b\n" +
		"2026-11-12T21:41:01.200000000Z c\n"
	if err := tail.feed(ctx, strings.NewReader(first)); err != nil {
		t.Fatal(err)
	}
	// The replay starts at the second of the last line and goes on.
	second := "2026-11-12T21:41:01.100000000Z a\n" +
		"2026-11-12T21:41:01.200000000Z b\n" +
		"2026-11-12T21:41:01.200000000Z c\n" +
		"2026-11-12T21:41:01.200000000Z d\n" +
		"2026-11-12T21:41:02.000000000Z e\n"
	if err := tail.feed(ctx, strings.NewReader(second)); err != nil {
		t.Fatal(err)
	}
	if got, want := strings.Join(lines(t, tail), ","), "a,b,c,d,e"; got != want {
		t.Fatalf("lines = %s, want %s", got, want)
	}
}

// A tailer with a resolver follows the component to its replacement pod,
// and the new pod's lines are all read, also ones older than the last
// line of the previous pod.
func TestResolvePod_FollowsReplacementPod(t *testing.T) {
	current := "old"
	var resolveErr error
	tail := New(Source{Pod: "old", Resolve: func(context.Context) (string, error) { return current, resolveErr }}, 16)
	ctx := context.Background()
	if pod := tail.resolvePod(ctx); pod != "old" {
		t.Fatalf("pod = %q, want old", pod)
	}
	if err := tail.feed(ctx, strings.NewReader("2026-11-12T21:41:05Z from-old\n")); err != nil {
		t.Fatal(err)
	}
	current = "new"
	if pod := tail.resolvePod(ctx); pod != "new" {
		t.Fatalf("pod = %q, want new", pod)
	}
	if err := tail.feed(ctx, strings.NewReader("2026-11-12T21:41:04Z from-new\n")); err != nil {
		t.Fatal(err)
	}
	// A failing resolver keeps the pod being followed.
	resolveErr = errors.New("api down")
	current = ""
	if pod := tail.resolvePod(ctx); pod != "new" {
		t.Fatalf("pod after a failed resolve = %q, want new", pod)
	}
	if got, want := strings.Join(lines(t, tail), ","), "from-old,from-new"; got != want {
		t.Fatalf("lines = %s, want %s", got, want)
	}
}
