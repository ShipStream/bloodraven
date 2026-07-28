package controller

import (
	"testing"

	"github.com/shipstream/bloodraven/internal/mysql"
	"github.com/shipstream/bloodraven/internal/platform"
	"github.com/shipstream/bloodraven/internal/state"
)

// newReaderCloneTopology builds a three-site group — two primary-candidates
// and one non-promotable reader — for exercising which in-flight clones are
// allowed to suppress cross-site actions.
func newReaderCloneTopology(t *testing.T) *TopologyManager {
	t.Helper()
	cfg := testTopologyConfig()
	cfg.Sites = append(cfg.Sites, SiteTopologyConfig{
		Name:          "reader",
		Zone:          "lion-reader",
		LBIP:          "3.3.3.3",
		Role:          state.SiteRoleReadOnly,
		TaintSelector: taintSelector("reader"),
		Host:          "mysql-reader",
	})
	checkers := []mysql.Checker{
		&mockMySQL{readOnly: false},
		&mockMySQL{readOnly: true},
		&mockMySQL{readOnly: true},
	}
	return NewTopologyManager(cfg, checkers, NewFailoverController(testLogger()), nil,
		NewBootstrapController(testLogger()), BootstrapConfig{ReplUser: "repl"},
		newMockTainter(), platform.NewHub(testLogger()), &mockDNS{}, testLogger())
}

// TestBootstrapBlocksCrossSite covers the rule that a clone into a
// non-promotable reader must not suppress cross-site failover actions.
//
// Regression guard: a reader losing its PVC triggers an auto-clone, and
// when that clone suppressed all cross-site actions, killing the primary
// during it left the group wedged with no writable site — activeSite went
// empty and no promotion target was ever selected.
func TestBootstrapBlocksCrossSite(t *testing.T) {
	tests := []struct {
		name      string
		phase     BootstrapPhase
		recipient string
		want      bool
	}{
		{
			name:  "idle bootstrap never blocks",
			phase: BootstrapPhaseDone,
			want:  false,
		},
		{
			name:      "clone into a primary-candidate blocks",
			phase:     BootstrapPhaseCloning,
			recipient: "dc2",
			want:      true,
		},
		{
			name:      "clone into a reader does not block",
			phase:     BootstrapPhaseCloning,
			recipient: "reader",
			want:      false,
		},
		{
			name:      "reader clone does not block while restarting",
			phase:     BootstrapPhaseRestarting,
			recipient: "reader",
			want:      false,
		},
		{
			name:      "reader clone does not block during replication setup",
			phase:     BootstrapPhaseSetupRepl,
			recipient: "reader",
			want:      false,
		},
		{
			name:      "unknown recipient falls back to blocking",
			phase:     BootstrapPhaseCloning,
			recipient: "",
			want:      true,
		},
		{
			name:      "recipient absent from topology falls back to blocking",
			phase:     BootstrapPhaseCloning,
			recipient: "not-a-site",
			want:      true,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			tm := newReaderCloneTopology(t)
			tm.mu.Lock()
			tm.bootstrapPhase = tc.phase
			tm.bootstrapRecipient = tc.recipient
			tm.mu.Unlock()

			if got := tm.bootstrapBlocksCrossSite(); got != tc.want {
				t.Errorf("bootstrapBlocksCrossSite() = %v, want %v", got, tc.want)
			}
		})
	}
}

// TestBootstrapBlocksCrossSite_DROnlyRecipientBlocks pins the dr-only case
// separately: a dr-only site follows the active primary and goes unreachable
// across CLONE INSTANCE, so it must keep suppressing like a candidate does.
func TestBootstrapBlocksCrossSite_DROnlyRecipientBlocks(t *testing.T) {
	tm := newReaderCloneTopology(t)
	tm.mu.Lock()
	tm.sites[2].role = state.SiteRoleDROnly
	tm.bootstrapPhase = BootstrapPhaseCloning
	tm.bootstrapRecipient = "reader"
	tm.mu.Unlock()

	if !tm.bootstrapBlocksCrossSite() {
		t.Error("a clone into a dr-only site must suppress cross-site actions")
	}
}

// TestStartBootstrapByName_RecordsRecipient verifies the recipient is
// recorded before the phase flips to Cloning. Without it,
// bootstrapBlocksCrossSite would fall back to blocking on every clone and
// the reader carve-out would never take effect.
func TestStartBootstrapByName_RecordsRecipient(t *testing.T) {
	tm := newReaderCloneTopology(t)
	tm.mu.Lock()
	tm.bootstrapPhase = BootstrapPhaseCloning
	tm.bootstrapRecipient = "reader"
	tm.mu.Unlock()

	// A reader clone in flight must leave the promotion path open.
	if tm.bootstrapBlocksCrossSite() {
		t.Fatal("reader clone should not suppress cross-site actions")
	}

	// Switching the recipient to a candidate re-arms suppression, which is
	// what protects a mid-clone replica from a spurious failover.
	tm.mu.Lock()
	tm.bootstrapRecipient = "dc2"
	tm.mu.Unlock()
	if !tm.bootstrapBlocksCrossSite() {
		t.Error("candidate clone should suppress cross-site actions")
	}
}
