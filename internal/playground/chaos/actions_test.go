package chaos

import (
	"context"
	"errors"
	"strings"
	"testing"
)

func TestRevertStackRunsLIFOAndJoinsErrors(t *testing.T) {
	a := &Actions{}
	var order []string
	a.push("first", func(ctx context.Context) error {
		order = append(order, "first")
		return nil
	})
	a.push("second", func(ctx context.Context) error {
		order = append(order, "second")
		return errors.New("boom")
	})
	a.push("third", func(ctx context.Context) error {
		order = append(order, "third")
		return nil
	})

	err := a.Revert(context.Background())
	if err == nil || !strings.Contains(err.Error(), "boom") {
		t.Fatalf("expected joined error containing 'boom', got %v", err)
	}
	if got := strings.Join(order, ","); got != "third,second,first" {
		t.Fatalf("revert order = %q, want third,second,first", got)
	}
	if got := a.PendingReverts(); len(got) != 0 {
		t.Fatalf("expected stack drained, got %v", got)
	}
}

func TestRevertEmptyStackIsNoOp(t *testing.T) {
	a := &Actions{}
	if err := a.Revert(context.Background()); err != nil {
		t.Fatalf("Revert empty: %v", err)
	}
}
