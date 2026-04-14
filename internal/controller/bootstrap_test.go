package controller

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/shipstream/bloodraven/internal/mysql"
)

// bootstrapMock is a mock MySQL for bootstrap tests, tracking calls and allowing
// individual methods to be configured to fail.
type bootstrapMock struct {
	mu              sync.Mutex
	calls           []string
	readOnly        bool
	checkReadOnlyErr error
	setDonorErr     error
	cloneErr            error
	cloneTimeoutSec     int
	superReadOnlyErr    error
	changeSourceErr     error
	startReplicaErr     error
}

func (b *bootstrapMock) record(name string) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.calls = append(b.calls, name)
}

func (b *bootstrapMock) getCalls() []string {
	b.mu.Lock()
	defer b.mu.Unlock()
	out := make([]string, len(b.calls))
	copy(out, b.calls)
	return out
}

func (b *bootstrapMock) CheckReadOnly(_ context.Context) (bool, error) {
	b.record("CheckReadOnly")
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.readOnly, b.checkReadOnlyErr
}

func (b *bootstrapMock) Promote(_ context.Context) error {
	b.record("Promote")
	return nil
}

func (b *bootstrapMock) Close() error { return nil }

func (b *bootstrapMock) KillAppConnections(_ context.Context) (int, error) {
	b.record("KillAppConnections")
	return 0, nil
}

func (b *bootstrapMock) SetSuperReadOnly(_ context.Context, on bool) error {
	if on {
		b.record("SetSuperReadOnly(ON)")
	} else {
		b.record("SetSuperReadOnly(OFF)")
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.superReadOnlyErr
}

func (b *bootstrapMock) StopReplica(_ context.Context) error {
	b.record("StopReplica")
	return nil
}

func (b *bootstrapMock) ResetReplicaAll(_ context.Context) error {
	b.record("ResetReplicaAll")
	return nil
}

func (b *bootstrapMock) SetReadOnly(_ context.Context, on bool) error {
	b.record("SetReadOnly")
	b.mu.Lock()
	defer b.mu.Unlock()
	b.readOnly = on
	return nil
}

func (b *bootstrapMock) ShowReplicaStatus(_ context.Context) (*mysql.ReplicaStatus, error) {
	b.record("ShowReplicaStatus")
	return nil, nil
}

func (b *bootstrapMock) ChangeReplicationSource(_ context.Context, _ mysql.ReplicationSourceOpts) error {
	b.record("ChangeReplicationSource")
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.changeSourceErr
}

func (b *bootstrapMock) StartReplica(_ context.Context) error {
	b.record("StartReplica")
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.startReplicaErr
}

func (b *bootstrapMock) WaitForRelayLogDrain(_ context.Context, _ time.Duration) error {
	b.record("WaitForRelayLogDrain")
	return nil
}

func (b *bootstrapMock) SetCloneDonorList(_ context.Context, donor string) error {
	b.record("SetCloneDonorList")
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.setDonorErr
}

func (b *bootstrapMock) GetGtidExecuted(_ context.Context) (string, error) {
	b.record("GetGtidExecuted")
	return "", nil
}

func (b *bootstrapMock) CloneInstance(_ context.Context, _, _, _ string, _ bool, cloneTimeoutSec int) error {
	b.record("CloneInstance")
	b.mu.Lock()
	defer b.mu.Unlock()
	b.cloneTimeoutSec = cloneTimeoutSec
	return b.cloneErr
}

// --- BootstrapReplica tests ---

func TestBootstrapReplica_HappyPath(t *testing.T) {
	primary := &bootstrapMock{readOnly: false}
	replica := &bootstrapMock{readOnly: true}
	bc := NewBootstrapController(testLogger())

	err := bc.BootstrapReplica(context.Background(), BootstrapOpts{
		Primary:      primary,
		Replica:      replica,
		PrimaryHost:  "primary.example.com",
		ReplicaSite:    "dc2",
		ReplUser:     "repl",
		ReplPassword: "secret",
		UseSSL:       true,
		CloneTimeout: 30 * time.Minute,
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	// Primary should have CheckReadOnly called
	pCalls := primary.getCalls()
	if len(pCalls) != 1 || pCalls[0] != "CheckReadOnly" {
		t.Errorf("primary calls: got %v, want [CheckReadOnly]", pCalls)
	}

	// Replica should have SetCloneDonorList and CloneInstance called
	rCalls := replica.getCalls()
	expected := []string{"SetCloneDonorList", "CloneInstance"}
	if len(rCalls) != len(expected) {
		t.Fatalf("replica calls: got %v, want %v", rCalls, expected)
	}
	for i, want := range expected {
		if rCalls[i] != want {
			t.Errorf("replica call[%d]: got %q, want %q", i, rCalls[i], want)
		}
	}
}

func TestBootstrapReplica_PrimaryReadOnly(t *testing.T) {
	primary := &bootstrapMock{readOnly: true}
	replica := &bootstrapMock{}
	bc := NewBootstrapController(testLogger())

	err := bc.BootstrapReplica(context.Background(), BootstrapOpts{
		Primary:      primary,
		Replica:      replica,
		PrimaryHost:  "primary.example.com",
		ReplicaSite:    "dc2",
		ReplUser:     "repl",
		ReplPassword: "secret",
		CloneTimeout: 30 * time.Minute,
	})
	if err == nil {
		t.Fatal("expected error when primary is read-only")
	}
	if got := err.Error(); got != "primary is read-only, cannot bootstrap from it" {
		t.Errorf("unexpected error message: %s", got)
	}

	// Replica should not have been touched
	rCalls := replica.getCalls()
	if len(rCalls) != 0 {
		t.Errorf("replica should not have been called, got %v", rCalls)
	}
}

func TestBootstrapReplica_CheckReadOnlyFails(t *testing.T) {
	primary := &bootstrapMock{checkReadOnlyErr: errors.New("connection refused")}
	replica := &bootstrapMock{}
	bc := NewBootstrapController(testLogger())

	err := bc.BootstrapReplica(context.Background(), BootstrapOpts{
		Primary:      primary,
		Replica:      replica,
		PrimaryHost:  "primary.example.com",
		ReplicaSite:    "dc2",
		ReplUser:     "repl",
		ReplPassword: "secret",
		CloneTimeout: 30 * time.Minute,
	})
	if err == nil {
		t.Fatal("expected error when CheckReadOnly fails")
	}

	rCalls := replica.getCalls()
	if len(rCalls) != 0 {
		t.Errorf("replica should not have been called, got %v", rCalls)
	}
}

func TestBootstrapReplica_SetCloneDonorListFails(t *testing.T) {
	primary := &bootstrapMock{readOnly: false}
	replica := &bootstrapMock{setDonorErr: errors.New("access denied")}
	bc := NewBootstrapController(testLogger())

	err := bc.BootstrapReplica(context.Background(), BootstrapOpts{
		Primary:      primary,
		Replica:      replica,
		PrimaryHost:  "primary.example.com",
		ReplicaSite:    "dc2",
		ReplUser:     "repl",
		ReplPassword: "secret",
		CloneTimeout: 30 * time.Minute,
	})
	if err == nil {
		t.Fatal("expected error when SetCloneDonorList fails")
	}

	// CloneInstance should not have been called
	rCalls := replica.getCalls()
	for _, c := range rCalls {
		if c == "CloneInstance" {
			t.Error("CloneInstance should not be called when SetCloneDonorList fails")
		}
	}
}

func TestBootstrapReplica_CloneFails(t *testing.T) {
	primary := &bootstrapMock{readOnly: false}
	replica := &bootstrapMock{cloneErr: errors.New("clone failed: disk full")}
	bc := NewBootstrapController(testLogger())

	err := bc.BootstrapReplica(context.Background(), BootstrapOpts{
		Primary:      primary,
		Replica:      replica,
		PrimaryHost:  "primary.example.com",
		ReplicaSite:    "dc2",
		ReplUser:     "repl",
		ReplPassword: "secret",
		CloneTimeout: 30 * time.Minute,
	})
	if err == nil {
		t.Fatal("expected error when CloneInstance fails")
	}
}

func TestBootstrapReplica_PassesCloneTimeout(t *testing.T) {
	primary := &bootstrapMock{readOnly: false}
	replica := &bootstrapMock{readOnly: true}
	bc := NewBootstrapController(testLogger())

	err := bc.BootstrapReplica(context.Background(), BootstrapOpts{
		Primary:      primary,
		Replica:      replica,
		PrimaryHost:  "primary.example.com",
		ReplicaSite:    "dc2",
		ReplUser:     "repl",
		ReplPassword: "secret",
		UseSSL:       true,
		CloneTimeout: 2 * time.Hour,
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	replica.mu.Lock()
	got := replica.cloneTimeoutSec
	replica.mu.Unlock()

	if got != 7200 {
		t.Errorf("clone timeout seconds: got %d, want 7200", got)
	}
}

func TestBootstrapReplica_DefaultCloneTimeout(t *testing.T) {
	primary := &bootstrapMock{readOnly: false}
	replica := &bootstrapMock{readOnly: true}
	bc := NewBootstrapController(testLogger())

	// CloneTimeout of 0 should default to 3600
	err := bc.BootstrapReplica(context.Background(), BootstrapOpts{
		Primary:      primary,
		Replica:      replica,
		PrimaryHost:  "primary.example.com",
		ReplicaSite:    "dc2",
		ReplUser:     "repl",
		ReplPassword: "secret",
		CloneTimeout: 0,
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	replica.mu.Lock()
	got := replica.cloneTimeoutSec
	replica.mu.Unlock()

	if got != 3600 {
		t.Errorf("default clone timeout seconds: got %d, want 3600", got)
	}
}

// --- SetupReplication tests ---

func TestSetupReplication_HappyPath(t *testing.T) {
	replica := &bootstrapMock{}
	bc := NewBootstrapController(testLogger())

	err := bc.SetupReplication(context.Background(), replica, ReplicationSetupOpts{
		SourceHost:   "primary.example.com",
		ReplUser:     "repl",
		ReplPassword: "secret",
		UseSSL:       true,
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	calls := replica.getCalls()
	expected := []string{"SetSuperReadOnly(ON)", "ChangeReplicationSource", "StartReplica"}
	if len(calls) != len(expected) {
		t.Fatalf("calls: got %v, want %v", calls, expected)
	}
	for i, want := range expected {
		if calls[i] != want {
			t.Errorf("call[%d]: got %q, want %q", i, calls[i], want)
		}
	}
}

func TestSetupReplication_SetSuperReadOnlyFails(t *testing.T) {
	replica := &bootstrapMock{superReadOnlyErr: errors.New("access denied")}
	bc := NewBootstrapController(testLogger())

	err := bc.SetupReplication(context.Background(), replica, ReplicationSetupOpts{
		SourceHost:   "primary.example.com",
		ReplUser:     "repl",
		ReplPassword: "secret",
	})
	if err == nil {
		t.Fatal("expected error when SetSuperReadOnly fails")
	}

	// ChangeReplicationSource and StartReplica should not have been called
	calls := replica.getCalls()
	for _, c := range calls {
		if c == "ChangeReplicationSource" || c == "StartReplica" {
			t.Errorf("should not have called %s when SetSuperReadOnly fails", c)
		}
	}
}

func TestSetupReplication_ChangeReplicationSourceFails(t *testing.T) {
	replica := &bootstrapMock{changeSourceErr: errors.New("syntax error")}
	bc := NewBootstrapController(testLogger())

	err := bc.SetupReplication(context.Background(), replica, ReplicationSetupOpts{
		SourceHost:   "primary.example.com",
		ReplUser:     "repl",
		ReplPassword: "secret",
	})
	if err == nil {
		t.Fatal("expected error when ChangeReplicationSource fails")
	}

	// StartReplica should not have been called
	calls := replica.getCalls()
	for _, c := range calls {
		if c == "StartReplica" {
			t.Error("StartReplica should not be called when ChangeReplicationSource fails")
		}
	}
}

func TestSetupReplication_StartReplicaFails(t *testing.T) {
	replica := &bootstrapMock{startReplicaErr: errors.New("replica already running")}
	bc := NewBootstrapController(testLogger())

	err := bc.SetupReplication(context.Background(), replica, ReplicationSetupOpts{
		SourceHost:   "primary.example.com",
		ReplUser:     "repl",
		ReplPassword: "secret",
	})
	if err == nil {
		t.Fatal("expected error when StartReplica fails")
	}

	// Verify we got as far as StartReplica
	calls := replica.getCalls()
	if len(calls) != 3 {
		t.Fatalf("expected 3 calls, got %v", calls)
	}
	if calls[2] != "StartReplica" {
		t.Errorf("call[2]: got %q, want StartReplica", calls[2])
	}
}
