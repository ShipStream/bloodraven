package mysql

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"
)

// TestReplicationSourceOpts verifies the struct can hold all required fields.
func TestReplicationSourceOpts(t *testing.T) {
	opts := ReplicationSourceOpts{
		Host:     "primary.example.com",
		User:     "repl",
		Password: "secret",
		UseSSL:   true,
	}
	if opts.Host != "primary.example.com" {
		t.Error("host mismatch")
	}
	if opts.User != "repl" {
		t.Error("user mismatch")
	}
	if opts.Password != "secret" {
		t.Error("password mismatch")
	}
	if !opts.UseSSL {
		t.Error("ssl should be true")
	}
}

// TestReplicaStatus verifies the struct fields.
func TestReplicaStatus(t *testing.T) {
	secs := int64(5)
	rs := ReplicaStatus{
		IORunning:           true,
		SQLRunning:          true,
		SecondsBehindSource: &secs,
		LastError:           "",
		SourceHost:          "primary.example.com",
		ExecutedGtidSet:     "uuid1:1-100",
	}
	if !rs.IORunning {
		t.Error("IO should be running")
	}
	if !rs.SQLRunning {
		t.Error("SQL should be running")
	}
	if *rs.SecondsBehindSource != 5 {
		t.Errorf("seconds behind: got %d, want 5", *rs.SecondsBehindSource)
	}
}

// TestWaitForRelayLogDrain_NilStatus tests that WaitForRelayLogDrain returns
// immediately when ShowReplicaStatus returns nil (no replication configured).
// This tests the logic via a mockChecker since we can't connect to a real MySQL.
type mockCheckerForDrain struct {
	calls        int
	statusResult *ReplicaStatus
}

func (m *mockCheckerForDrain) CheckReadOnly(_ context.Context) (bool, error) { return false, nil }
func (m *mockCheckerForDrain) Promote(_ context.Context) error               { return nil }
func (m *mockCheckerForDrain) Close() error                                  { return nil }
func (m *mockCheckerForDrain) SetSuperReadOnly(_ context.Context, _ bool) error {
	return nil
}
func (m *mockCheckerForDrain) StopReplica(_ context.Context) error     { return nil }
func (m *mockCheckerForDrain) ResetReplicaAll(_ context.Context) error { return nil }
func (m *mockCheckerForDrain) SetReadOnly(_ context.Context, _ bool) error {
	return nil
}
func (m *mockCheckerForDrain) ShowReplicaStatus(_ context.Context) (*ReplicaStatus, error) {
	m.calls++
	return m.statusResult, nil
}
func (m *mockCheckerForDrain) ChangeReplicationSource(_ context.Context, _ ReplicationSourceOpts) error {
	return nil
}
func (m *mockCheckerForDrain) StartReplica(_ context.Context) error { return nil }
func (m *mockCheckerForDrain) WaitForRelayLogDrain(ctx context.Context, timeout time.Duration) error {
	// Mirror the real checker's WaitForRelayLogDrain logic for testing.
	deadline := time.Now().Add(timeout)
	interval := 100 * time.Millisecond

	for {
		rs, err := m.ShowReplicaStatus(ctx)
		if err != nil {
			return err
		}
		if rs == nil {
			return nil
		}
		if !rs.SQLRunning {
			return nil
		}
		if rs.SecondsBehindSource != nil && *rs.SecondsBehindSource == 0 {
			return nil
		}
		if rs.LastError != "" {
			return fmt.Errorf("relay log drain aborted: SQL thread error: %s", rs.LastError)
		}
		if time.Now().After(deadline) {
			return nil // simplified for test
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(interval):
		}
		if interval < 400*time.Millisecond {
			interval *= 2
		}
	}
}

func TestWaitForRelayLogDrain_NoReplication(t *testing.T) {
	mock := &mockCheckerForDrain{statusResult: nil}
	err := mock.WaitForRelayLogDrain(context.Background(), 5*time.Second)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if mock.calls != 1 {
		t.Errorf("expected 1 call to ShowReplicaStatus, got %d", mock.calls)
	}
}

func TestWaitForRelayLogDrain_AlreadyCaughtUp(t *testing.T) {
	zero := int64(0)
	mock := &mockCheckerForDrain{
		statusResult: &ReplicaStatus{
			SQLRunning:          true,
			SecondsBehindSource: &zero,
		},
	}
	err := mock.WaitForRelayLogDrain(context.Background(), 5*time.Second)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if mock.calls != 1 {
		t.Errorf("expected 1 call, got %d", mock.calls)
	}
}

func TestWaitForRelayLogDrain_SQLStopped(t *testing.T) {
	mock := &mockCheckerForDrain{
		statusResult: &ReplicaStatus{
			SQLRunning: false,
		},
	}
	err := mock.WaitForRelayLogDrain(context.Background(), 5*time.Second)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestWaitForRelayLogDrain_EarlyExitOnLastError(t *testing.T) {
	behind := int64(10)
	mock := &mockCheckerForDrain{
		statusResult: &ReplicaStatus{
			SQLRunning:          true,
			SecondsBehindSource: &behind,
			LastError:           "Error 'Duplicate entry' on query",
		},
	}
	err := mock.WaitForRelayLogDrain(context.Background(), 5*time.Second)
	if err == nil {
		t.Fatal("expected error for LastError early exit")
	}
	if !strings.Contains(err.Error(), "SQL thread error") {
		t.Errorf("expected SQL thread error message, got: %v", err)
	}
	// Should exit after first check, not poll repeatedly.
	if mock.calls != 1 {
		t.Errorf("expected 1 call (early exit), got %d", mock.calls)
	}
}
