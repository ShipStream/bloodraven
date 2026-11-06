package controller

import (
	"context"
	"errors"
	"slices"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/shipstream/bloodraven/internal/mysql"
	"github.com/shipstream/bloodraven/internal/testutil"
)

// lostROClearChecker pins sessions whose read_only=OFF applies and then
// returns an error (a response lost to a connection reset).
type lostROClearChecker struct {
	mysql.Checker
	// readOnlyErr fails every pooled read_only probe once read_only=OFF
	// was sent.
	readOnlyErr error
	cleared     *atomic.Bool
}

func newLostROClearChecker(c mysql.Checker, readOnlyErr error) lostROClearChecker {
	return lostROClearChecker{Checker: c, readOnlyErr: readOnlyErr, cleared: &atomic.Bool{}}
}

func (c lostROClearChecker) PinSession(ctx context.Context) (mysql.Session, error) {
	s, err := c.Checker.PinSession(ctx)
	if err != nil {
		return nil, err
	}
	return lostROClearSession{Session: s, cleared: c.cleared}, nil
}

func (c lostROClearChecker) CheckReadOnly(ctx context.Context) (bool, error) {
	if c.readOnlyErr != nil && c.cleared.Load() {
		return false, c.readOnlyErr
	}
	return c.Checker.CheckReadOnly(ctx)
}

type lostROClearSession struct {
	mysql.Session
	cleared *atomic.Bool
}

func (s lostROClearSession) SetReadOnly(ctx context.Context, on bool) error {
	if err := s.Session.SetReadOnly(ctx, on); err != nil || on {
		return err
	}
	s.cleared.Store(true)
	return errors.New("invalid connection")
}

// N2: an ordered update whose final read_only=OFF applies but returns an
// error made the standby writable: the handoff is recorded (failover
// record, cooldown, watermark, DNS) before the error is returned, so the
// anti-flap cooldown and lastFailoverTarget see the promotion. The source
// stays fenced and the active site is not rolled.
func TestExecuteTargets_HandoffRecordsAppliedPromotionDespiteError(t *testing.T) {
	src := &testutil.FakeMySQL{ReadOnlyVal: false, GtidExecutedVal: handoffUUID + ":1-100"}
	candidate := handoffCandidate(handoffUUID + ":1-100")
	run := runHandoff(t, src, newLostROClearChecker(candidate.Checker(), nil))
	if run.err == nil || !strings.Contains(run.err.Error(), "may already accept writes") {
		t.Fatalf("ExecuteTargets() error = %v, want the write-attempted handoff error", run.err)
	}
	var attempted *HandoffWriteAttemptedError
	if !errors.As(run.err, &attempted) {
		t.Fatalf("error %v does not carry *HandoffWriteAttemptedError", run.err)
	}
	if run.promoted != "standby" {
		t.Fatalf("authority callback site = %q, want standby: a writable standby must be recorded", run.promoted)
	}
	got, err := mysql.ParseGTIDSet(run.gtid)
	want, _ := mysql.ParseGTIDSet(handoffUUID + ":1-100")
	if err != nil || !got.Contains(want) || !want.Contains(got) {
		t.Fatalf("recorded promotion GTID %q, want the fenced set %s", run.gtid, want)
	}
	if calls := roleCalls(src.GetCalls()); slices.Contains(calls, "SetSuperReadOnly(OFF)") {
		t.Errorf("source was unfenced after read_only=OFF was issued: %v", calls)
	}
	if slices.Contains(run.applied, "primary") {
		t.Errorf("applied = %v; the old active must not be rolled after a failed handoff", run.applied)
	}
}

// N2: without positive evidence the standby is writable (its read_only
// cannot be read), nothing is recorded: the next poll observes it.
func TestExecuteTargets_HandoffUnconfirmedWritableIsNotRecorded(t *testing.T) {
	src := &testutil.FakeMySQL{ReadOnlyVal: false, GtidExecutedVal: handoffUUID + ":1-100"}
	candidate := handoffCandidate(handoffUUID + ":1-100")
	run := runHandoff(t, src, newLostROClearChecker(candidate.Checker(), errors.New("connection refused")))
	var attempted *HandoffWriteAttemptedError
	if !errors.As(run.err, &attempted) {
		t.Fatalf("error = %v, want *HandoffWriteAttemptedError", run.err)
	}
	if run.promoted != "" {
		t.Fatalf("authority callback ran for %q without evidence the standby is writable", run.promoted)
	}
}
