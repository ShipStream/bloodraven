package controller

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/shipstream/bloodraven/internal/mysql"
)

// trackingMock records which methods were called and in what order.
type trackingMock struct {
	mu        sync.Mutex
	calls     []string
	readOnly  bool
	drainErr  error
	fenceErr  error
}

func (t *trackingMock) record(name string) {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.calls = append(t.calls, name)
}

func (t *trackingMock) getCalls() []string {
	t.mu.Lock()
	defer t.mu.Unlock()
	out := make([]string, len(t.calls))
	copy(out, t.calls)
	return out
}

func (t *trackingMock) CheckReadOnly(_ context.Context) (bool, error) {
	t.record("CheckReadOnly")
	t.mu.Lock()
	defer t.mu.Unlock()
	return t.readOnly, nil
}

func (t *trackingMock) Promote(_ context.Context) error {
	t.record("Promote")
	return nil
}

func (t *trackingMock) Close() error { return nil }

func (t *trackingMock) SetSuperReadOnly(_ context.Context, on bool) error {
	if on {
		t.record("SetSuperReadOnly(ON)")
	} else {
		t.record("SetSuperReadOnly(OFF)")
	}
	t.mu.Lock()
	err := t.fenceErr
	t.mu.Unlock()
	return err
}

func (t *trackingMock) StopReplica(_ context.Context) error {
	t.record("StopReplica")
	return nil
}

func (t *trackingMock) ResetReplicaAll(_ context.Context) error {
	t.record("ResetReplicaAll")
	return nil
}

func (t *trackingMock) SetReadOnly(_ context.Context, on bool) error {
	if on {
		t.record("SetReadOnly(ON)")
	} else {
		t.record("SetReadOnly(OFF)")
	}
	t.mu.Lock()
	defer t.mu.Unlock()
	t.readOnly = on
	return nil
}

func (t *trackingMock) ShowReplicaStatus(_ context.Context) (*mysql.ReplicaStatus, error) {
	t.record("ShowReplicaStatus")
	return nil, nil
}

func (t *trackingMock) ChangeReplicationSource(_ context.Context, _ mysql.ReplicationSourceOpts) error {
	t.record("ChangeReplicationSource")
	return nil
}

func (t *trackingMock) StartReplica(_ context.Context) error {
	t.record("StartReplica")
	return nil
}

func (t *trackingMock) WaitForRelayLogDrain(_ context.Context, _ time.Duration) error {
	t.record("WaitForRelayLogDrain")
	t.mu.Lock()
	err := t.drainErr
	t.mu.Unlock()
	return err
}

func (t *trackingMock) SetCloneDonorList(_ context.Context, donor string) error {
	t.record("SetCloneDonorList")
	return nil
}

func (t *trackingMock) CloneInstance(_ context.Context, _, _, _ string, _ bool, _ int) error {
	t.record("CloneInstance")
	return nil
}

func TestFailoverExecute_FullSequence(t *testing.T) {
	candidate := &trackingMock{readOnly: true}
	oldPrimary := &trackingMock{}
	fc := NewFailoverController(testLogger())

	err := fc.Execute(context.Background(), candidate, oldPrimary, "site1")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	// Verify old primary was fenced.
	opCalls := oldPrimary.getCalls()
	if len(opCalls) != 1 || opCalls[0] != "SetSuperReadOnly(ON)" {
		t.Errorf("old primary calls: got %v, want [SetSuperReadOnly(ON)]", opCalls)
	}

	// Verify candidate sequence: drain -> stop -> reset -> clear super_read_only -> set read_only=0.
	candCalls := candidate.getCalls()
	expected := []string{"WaitForRelayLogDrain", "StopReplica", "ResetReplicaAll", "SetSuperReadOnly(OFF)", "SetReadOnly(OFF)"}
	if len(candCalls) != len(expected) {
		t.Fatalf("candidate calls: got %v, want %v", candCalls, expected)
	}
	for i, want := range expected {
		if candCalls[i] != want {
			t.Errorf("candidate call[%d]: got %q, want %q", i, candCalls[i], want)
		}
	}

	// Verify candidate is now writable.
	candidate.mu.Lock()
	ro := candidate.readOnly
	candidate.mu.Unlock()
	if ro {
		t.Error("candidate should be writable after promotion")
	}
}

func TestFailoverExecute_OldPrimaryNil(t *testing.T) {
	candidate := &trackingMock{readOnly: true}
	fc := NewFailoverController(testLogger())

	err := fc.Execute(context.Background(), candidate, nil, "site1")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	// Should still complete promotion without fencing.
	candCalls := candidate.getCalls()
	expected := []string{"WaitForRelayLogDrain", "StopReplica", "ResetReplicaAll", "SetSuperReadOnly(OFF)", "SetReadOnly(OFF)"}
	if len(candCalls) != len(expected) {
		t.Fatalf("candidate calls: got %v, want %v", candCalls, expected)
	}
	for i, want := range expected {
		if candCalls[i] != want {
			t.Errorf("candidate call[%d]: got %q, want %q", i, candCalls[i], want)
		}
	}
}

func TestFailoverExecute_DrainTimeout(t *testing.T) {
	candidate := &trackingMock{
		readOnly: true,
		drainErr: errors.New("relay log drain timed out"),
	}
	oldPrimary := &trackingMock{}
	fc := NewFailoverController(testLogger())

	// Should still succeed despite drain timeout.
	err := fc.Execute(context.Background(), candidate, oldPrimary, "site1")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	// Verify promotion still happened.
	candCalls := candidate.getCalls()
	if len(candCalls) != 5 {
		t.Fatalf("expected 5 calls, got %v", candCalls)
	}
	if candCalls[4] != "SetReadOnly(OFF)" {
		t.Errorf("last call should be SetReadOnly(OFF), got %q", candCalls[4])
	}
}

func TestFailoverExecute_FenceError(t *testing.T) {
	candidate := &trackingMock{readOnly: true}
	oldPrimary := &trackingMock{fenceErr: errors.New("connection refused")}
	fc := NewFailoverController(testLogger())

	// Fence error is ignored, promotion should proceed.
	err := fc.Execute(context.Background(), candidate, oldPrimary, "site1")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	candCalls := candidate.getCalls()
	if len(candCalls) != 5 {
		t.Fatalf("expected 5 calls, got %v", candCalls)
	}
}
