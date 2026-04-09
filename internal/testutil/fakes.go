// Package testutil provides consolidated test fakes and helpers.
//
// All test packages should import fakes from here rather than maintaining
// their own duplicate implementations. This satisfies the Testing 2.0
// fixture consolidation requirement.
package testutil

import (
	"context"
	"log/slog"
	"os"
	"sync"
	"time"

	"github.com/shipstream/bloodraven/internal/mysql"
)

// ---------------------------------------------------------------------------
// FakeMySQL — consolidated mock for mysql.Checker
// ---------------------------------------------------------------------------

// FakeMySQL implements mysql.Checker with full tracking and error injection.
// It is thread-safe and suitable for concurrent poll tests.
type FakeMySQL struct {
	mu sync.Mutex

	ReadOnlyVal bool
	Err         error

	// Tracking fields
	Promoted              bool
	SuperReadOnlySet      bool
	StoppedReplica        bool
	ResetReplicaAllCalled bool
	ReplicationSourceSet  bool
	ReplicaStarted        bool
	CloneDonorList        string
	CloneInstanceCalled   bool
	ChangeReplicationOpts mysql.ReplicationSourceOpts

	// Replication status
	ReplicaStatusVal *mysql.ReplicaStatus
	ReplicaStatusErr error

	// Error injection per method
	FenceErr        error
	DrainErr        error
	StopReplicaErr  error
	ResetReplicaErr error
	SetReadOnlyErr  error
	ChangeSourceErr error
	StartReplicaErr error
	SetDonorErr     error
	CloneErr        error

	// Call tracking
	Calls []string
}

func (m *FakeMySQL) record(name string) {
	// Caller must hold m.mu or call under lock.
	m.Calls = append(m.Calls, name)
}

func (m *FakeMySQL) GetCalls() []string {
	m.mu.Lock()
	defer m.mu.Unlock()
	out := make([]string, len(m.Calls))
	copy(out, m.Calls)
	return out
}

func (m *FakeMySQL) CheckReadOnly(_ context.Context) (bool, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.record("CheckReadOnly")
	return m.ReadOnlyVal, m.Err
}

func (m *FakeMySQL) Promote(_ context.Context) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.record("Promote")
	m.Promoted = true
	m.ReadOnlyVal = false
	return nil
}

func (m *FakeMySQL) Close() error { return nil }

func (m *FakeMySQL) SetSuperReadOnly(_ context.Context, on bool) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if on {
		m.record("SetSuperReadOnly(ON)")
	} else {
		m.record("SetSuperReadOnly(OFF)")
	}
	if m.FenceErr != nil {
		return m.FenceErr
	}
	m.SuperReadOnlySet = on
	if on {
		m.ReadOnlyVal = true
	}
	return nil
}

func (m *FakeMySQL) StopReplica(_ context.Context) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.record("StopReplica")
	if m.StopReplicaErr != nil {
		return m.StopReplicaErr
	}
	m.StoppedReplica = true
	return nil
}

func (m *FakeMySQL) ResetReplicaAll(_ context.Context) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.record("ResetReplicaAll")
	if m.ResetReplicaErr != nil {
		return m.ResetReplicaErr
	}
	m.ResetReplicaAllCalled = true
	return nil
}

func (m *FakeMySQL) SetReadOnly(_ context.Context, on bool) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if on {
		m.record("SetReadOnly(ON)")
	} else {
		m.record("SetReadOnly(OFF)")
	}
	if m.SetReadOnlyErr != nil {
		return m.SetReadOnlyErr
	}
	m.ReadOnlyVal = on
	return nil
}

func (m *FakeMySQL) ShowReplicaStatus(_ context.Context) (*mysql.ReplicaStatus, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.record("ShowReplicaStatus")
	if m.ReplicaStatusErr != nil {
		return nil, m.ReplicaStatusErr
	}
	return m.ReplicaStatusVal, nil
}

func (m *FakeMySQL) ChangeReplicationSource(_ context.Context, opts mysql.ReplicationSourceOpts) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.record("ChangeReplicationSource")
	if m.ChangeSourceErr != nil {
		return m.ChangeSourceErr
	}
	m.ReplicationSourceSet = true
	m.ChangeReplicationOpts = opts
	return nil
}

func (m *FakeMySQL) StartReplica(_ context.Context) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.record("StartReplica")
	if m.StartReplicaErr != nil {
		return m.StartReplicaErr
	}
	m.ReplicaStarted = true
	return nil
}

func (m *FakeMySQL) WaitForRelayLogDrain(_ context.Context, _ time.Duration) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.record("WaitForRelayLogDrain")
	return m.DrainErr
}

func (m *FakeMySQL) SetCloneDonorList(_ context.Context, donor string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.record("SetCloneDonorList")
	if m.SetDonorErr != nil {
		return m.SetDonorErr
	}
	m.CloneDonorList = donor
	return nil
}

func (m *FakeMySQL) CloneInstance(_ context.Context, _, _, _ string, _ bool, _ int) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.record("CloneInstance")
	if m.CloneErr != nil {
		return m.CloneErr
	}
	m.CloneInstanceCalled = true
	return nil
}

// SetState is a test helper to set read-only state and clear errors.
func (m *FakeMySQL) SetState(readOnly bool) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.ReadOnlyVal = readOnly
	m.Err = nil
}

// SetError makes subsequent CheckReadOnly calls return this error.
func (m *FakeMySQL) SetError(err error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.Err = err
}

// IsReadOnlyVal returns the current readOnly state (thread-safe).
func (m *FakeMySQL) IsReadOnlyVal() bool {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.ReadOnlyVal
}

// ---------------------------------------------------------------------------
// FakeTainter — consolidated mock for platform.NodeTainter
// ---------------------------------------------------------------------------

// FakeTainter implements platform.NodeTainter with state tracking.
type FakeTainter struct {
	mu     sync.Mutex
	Taints map[string]bool // selector -> tainted
}

func NewFakeTainter() *FakeTainter {
	return &FakeTainter{Taints: make(map[string]bool)}
}

func (m *FakeTainter) SetTaint(_ context.Context, selector string, taint bool) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.Taints[selector] = taint
	return nil
}

func (m *FakeTainter) IsTainted(selector string) bool {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.Taints[selector]
}

// ---------------------------------------------------------------------------
// FakeDNS — consolidated mock for platform.DNSUpdater
// ---------------------------------------------------------------------------

// FakeDNS implements platform.DNSUpdater with call tracking.
type FakeDNS struct {
	mu       sync.Mutex
	LastIP   string
	NumCalls int
	Err      error
}

func (m *FakeDNS) UpdateDNSRecord(_ context.Context, ip string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.Err != nil {
		return m.Err
	}
	m.LastIP = ip
	m.NumCalls++
	return nil
}

func (m *FakeDNS) GetLastIP() string {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.LastIP
}

func (m *FakeDNS) GetCalls() int {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.NumCalls
}

// ---------------------------------------------------------------------------
// FakeFencer — consolidated mock for sidecar.Fencer
// ---------------------------------------------------------------------------

// FakeFencer implements sidecar.Fencer with state tracking.
type FakeFencer struct {
	mu            sync.Mutex
	ReadOnlyVal   bool
	ReadOnlyErr   error
	SuperReadOnly bool
	SetCh         chan struct{} // signaled on SetSuperReadOnly call
}

func NewFakeFencer(readOnly bool) *FakeFencer {
	return &FakeFencer{
		ReadOnlyVal: readOnly,
		SetCh:       make(chan struct{}, 1),
	}
}

func (m *FakeFencer) IsReadOnly(_ context.Context) (bool, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.ReadOnlyVal, m.ReadOnlyErr
}

func (m *FakeFencer) SetSuperReadOnly(_ context.Context) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.SuperReadOnly = true
	select {
	case m.SetCh <- struct{}{}:
	default:
	}
	return nil
}

func (m *FakeFencer) IsSuperReadOnly() bool {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.SuperReadOnly
}

// ---------------------------------------------------------------------------
// TestLogger — shared quiet logger for tests
// ---------------------------------------------------------------------------

// TestLogger returns a logger that only emits errors, suitable for tests.
func TestLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelError}))
}
