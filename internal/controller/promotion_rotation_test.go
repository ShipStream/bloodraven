package controller

import (
	"bytes"
	"context"
	"errors"
	"io"
	"log/slog"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/shipstream/bloodraven/internal/mysql"
)

type rotationCandidate struct {
	*trackingMock
	flushErr error
	block    bool
	deadline time.Duration
}

func (c *rotationCandidate) FlushBinaryLogs(ctx context.Context) error {
	c.record("FlushBinaryLogs")
	if deadline, ok := ctx.Deadline(); ok {
		c.deadline = time.Until(deadline)
	}
	if c.block {
		<-ctx.Done()
		return ctx.Err()
	}
	return c.flushErr
}
func (c *rotationCandidate) PinSession(context.Context) (mysql.Session, error) {
	return mysql.CheckerSession(c), nil
}

func TestPromotionRotatesBeforeWrites(t *testing.T) {
	for _, path := range []string{"emergency", "planned", "ordered handoff"} {
		for _, failure := range []bool{false, true} {
			t.Run(path+map[bool]string{false: " success", true: " flush failure"}[failure], func(t *testing.T) {
				candidate := &rotationCandidate{trackingMock: &trackingMock{readOnly: true, gtidExecuted: handoffUUID + ":1-10"}}
				source := &rotationCandidate{trackingMock: &trackingMock{gtidExecuted: handoffUUID + ":1-10"}}
				if failure {
					candidate.flushErr = errors.New("rotation refused")
				}
				f := NewFailoverController(slog.New(slog.NewTextHandler(io.Discard, nil)))
				var err error
				if path == "ordered handoff" {
					_, err = f.ExecuteHandoff(context.Background(), candidate, source, "pdx", HandoffOptions{})
				} else {
					_, err = f.Execute(context.Background(), candidate, source, "pdx")
				}
				if err != nil {
					t.Fatal(err)
				}
				calls := candidate.getCalls()
				flush := slices.Index(calls, "FlushBinaryLogs")
				reset := slices.Index(calls, "ResetReplicaAll")
				write := slices.Index(calls, "SetSuperReadOnly(OFF)")
				if flush < 0 || flush <= reset || flush >= write {
					t.Fatalf("rotation must follow reset and precede enabling writes: %v", calls)
				}
				if candidate.deadline <= 0 || candidate.deadline > 2*time.Second {
					t.Fatalf("rotation timeout unbounded: %v", candidate.deadline)
				}
				if slices.Contains(source.getCalls(), "FlushBinaryLogs") {
					t.Fatal("rotated old primary")
				}
				if count := len(slices.DeleteFunc(slices.Clone(calls), func(s string) bool { return s != "FlushBinaryLogs" })); count != 1 {
					t.Fatalf("rotation retried: %d", count)
				}
			})
		}
	}
}

func TestPromotionRotationTimeoutKeepsHandoffWritable(t *testing.T) {
	candidate := &rotationCandidate{trackingMock: &trackingMock{readOnly: true, gtidExecuted: handoffUUID + ":1-10"}, block: true}
	source := &rotationCandidate{trackingMock: &trackingMock{gtidExecuted: handoffUUID + ":1-10"}}
	var logs bytes.Buffer
	ctx := context.Background()
	f := NewFailoverController(slog.New(slog.NewJSONHandler(&logs, nil)))
	if _, err := f.ExecuteHandoff(ctx, candidate, source, "pdx", HandoffOptions{}); err != nil {
		t.Fatal(err)
	}
	if candidate.readOnly || ctx.Err() != nil {
		t.Fatal("rotation timeout poisoned promotion context")
	}
	if strings.Count(logs.String(), `"msg":"promotion binlog rotation failed"`) != 1 || !strings.Contains(logs.String(), `"site":"pdx"`) {
		t.Fatalf("missing bounded rotation warning: %s", logs.String())
	}
}
