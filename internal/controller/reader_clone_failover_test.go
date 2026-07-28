package controller

import (
	"context"
	"errors"
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

// TestStartBootstrapByName_RecordsRecipient drives the real
// startBootstrapByName and asserts it records the recipient, so removing
// that assignment fails this test. Without the recorded recipient,
// bootstrapBlocksCrossSite falls back to blocking on every clone and the
// reader carve-out silently stops working.
//
// The recipient is written synchronously before the clone goroutine is
// spawned, so the assertion needs no synchronisation beyond the lock. The
// context is cancelled on cleanup so the goroutine cannot outlive the test.
func TestStartBootstrapByName_RecordsRecipient(t *testing.T) {
	tm := newReaderCloneTopology(t)
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)

	// "fresh-deploy" skips the confirmed-active-primary precondition, which
	// is not what this test is about.
	if !tm.startBootstrapByName(ctx, "dc1", "reader", "fresh-deploy") {
		t.Fatal("startBootstrapByName returned false; expected the bootstrap to start")
	}

	tm.mu.RLock()
	got := tm.bootstrapRecipient
	tm.mu.RUnlock()
	if got != "reader" {
		t.Errorf("bootstrapRecipient = %q, want %q", got, "reader")
	}
}

// TestBootstrapBlocksCrossSite_RecipientRoleDrivesSuppression pins that the
// suppression decision tracks the recipient's role rather than the mere
// presence of a bootstrap: a reader clone leaves the promotion path open,
// while a candidate clone re-arms suppression to protect a mid-clone replica
// from a spurious failover.
func TestBootstrapBlocksCrossSite_RecipientRoleDrivesSuppression(t *testing.T) {
	tm := newReaderCloneTopology(t)
	tm.mu.Lock()
	tm.bootstrapPhase = BootstrapPhaseCloning
	tm.bootstrapRecipient = "reader"
	tm.mu.Unlock()

	if tm.bootstrapBlocksCrossSite() {
		t.Fatal("reader clone should not suppress cross-site actions")
	}

	tm.mu.Lock()
	tm.bootstrapRecipient = "dc2"
	tm.mu.Unlock()
	if !tm.bootstrapBlocksCrossSite() {
		t.Error("candidate clone should suppress cross-site actions")
	}
}

// TestBootstrapBlocksCrossSite_UnrecognisedRoleBlocks guards the conservative
// fallback: only an explicit read-only reader is exempt, so a role this build
// does not know about must not silently open the cross-site path.
func TestBootstrapBlocksCrossSite_UnrecognisedRoleBlocks(t *testing.T) {
	tm := newReaderCloneTopology(t)
	tm.mu.Lock()
	tm.sites[2].role = state.SiteRole("some-future-role")
	tm.bootstrapPhase = BootstrapPhaseCloning
	tm.bootstrapRecipient = "reader"
	tm.mu.Unlock()

	if !tm.bootstrapBlocksCrossSite() {
		t.Error("an unrecognised recipient role must fall back to suppressing")
	}
}

// TestCloningReaderSite covers which site, if any, the reassert peer checks
// are allowed to skip. Only the reader currently being cloned qualifies.
func TestCloningReaderSite(t *testing.T) {
	tests := []struct {
		name      string
		phase     BootstrapPhase
		recipient string
		role      state.SiteRole
		want      string
	}{
		{
			name:      "reader clone in flight is skippable",
			phase:     BootstrapPhaseCloning,
			recipient: "reader",
			role:      state.SiteRoleReadOnly,
			want:      "reader",
		},
		{
			name:      "no bootstrap means nothing is skippable",
			phase:     BootstrapPhaseDone,
			recipient: "reader",
			role:      state.SiteRoleReadOnly,
			want:      "",
		},
		{
			name:      "candidate clone is never skippable",
			phase:     BootstrapPhaseCloning,
			recipient: "dc2",
			role:      state.SiteRoleReadOnly,
			want:      "",
		},
		{
			name:      "unrecognised role is never skippable",
			phase:     BootstrapPhaseCloning,
			recipient: "reader",
			role:      state.SiteRole("some-future-role"),
			want:      "",
		},
		{
			name:      "empty recipient is never skippable",
			phase:     BootstrapPhaseCloning,
			recipient: "",
			role:      state.SiteRoleReadOnly,
			want:      "",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			tm := newReaderCloneTopology(t)
			tm.mu.Lock()
			tm.sites[2].role = tc.role
			tm.bootstrapPhase = tc.phase
			tm.bootstrapRecipient = tc.recipient
			tm.mu.Unlock()

			if got := tm.cloningReaderSite(); got != tc.want {
				t.Errorf("cloningReaderSite() = %q, want %q", got, tc.want)
			}
		})
	}
}

// TestPrimaryReassert_ProceedsDuringReaderClone is the end-to-end guard for
// the wedge this PR exists to fix. A group is stuck with no writable site
// while a reader is mid-clone: the reader is unreachable across its CLONE
// INSTANCE restart and its GTID set is unreadable. Re-asserting the promoted
// primary must still fire, because that is what returns the group to service.
func TestPrimaryReassert_ProceedsDuringReaderClone(t *testing.T) {
	tm := newReaderCloneTopology(t)
	dc1 := tm.sites[0].mysql.(*mockMySQL)
	dc2 := tm.sites[1].mysql.(*mockMySQL)
	readerMySQL := tm.sites[2].mysql.(*mockMySQL)

	// The wedge: every reachable site fenced read-only, nothing writable.
	dc1.setReadOnly(true)
	dc2.setReadOnly(true)
	dc1.setGtidExecuted("aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa:1-10")
	dc2.setGtidExecuted("aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa:1-8")
	// Mid-clone the reader answers nothing.
	readerMySQL.setError(errors.New("reader is mid-clone"))

	tm.mu.Lock()
	tm.sites[0].state = state.StateReadOnly
	tm.sites[1].state = state.StateReadOnly
	tm.sites[2].state = state.StateUnreachable
	tm.lastFailoverTarget = "dc1"
	tm.promotionGtidExecuted = "aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa:1-9"
	tm.bootstrapPhase = BootstrapPhaseCloning
	tm.bootstrapRecipient = "reader"
	tm.mu.Unlock()

	if !tm.checkPrimaryReassert(context.Background()) {
		t.Fatal("re-assert must heal the wedge while a reader clone is in flight")
	}
	if ro, _ := dc1.CheckReadOnly(context.Background()); ro {
		t.Error("target should be writable after re-assert")
	}
}

// TestPrimaryReassert_BlockedDuringCandidateClone is the other half of the
// contract: a clone into a promotable candidate still holds the reassert
// path shut, because that site is part of the topology the decision reasons
// about and promoting around it mid-clone risks divergence.
func TestPrimaryReassert_BlockedDuringCandidateClone(t *testing.T) {
	tm := newReaderCloneTopology(t)
	dc1 := tm.sites[0].mysql.(*mockMySQL)
	dc2 := tm.sites[1].mysql.(*mockMySQL)
	readerMySQL := tm.sites[2].mysql.(*mockMySQL)

	dc1.setReadOnly(true)
	dc2.setReadOnly(true)
	dc1.setGtidExecuted("aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa:1-10")
	dc2.setGtidExecuted("aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa:1-8")
	readerMySQL.setGtidExecuted("aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa:1-8")

	tm.mu.Lock()
	tm.sites[0].state = state.StateReadOnly
	tm.sites[1].state = state.StateReadOnly
	tm.sites[2].state = state.StateReadOnly
	tm.lastFailoverTarget = "dc1"
	tm.promotionGtidExecuted = "aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa:1-9"
	tm.bootstrapPhase = BootstrapPhaseCloning
	tm.bootstrapRecipient = "dc2"
	tm.mu.Unlock()

	if tm.checkPrimaryReassert(context.Background()) {
		t.Fatal("re-assert must stay suppressed while a candidate clone is in flight")
	}
	if ro, _ := dc1.CheckReadOnly(context.Background()); !ro {
		t.Error("target must remain read-only while a candidate clone runs")
	}
}

// TestPrimaryReassert_UnreachableReaderOutsideCloneStillBlocks pins that the
// carve-out is scoped to the reader actually being cloned. An unreachable
// reader with no clone in flight is an unexplained failure, and the reassert
// path must keep deferring to the normal promotion path for it.
func TestPrimaryReassert_UnreachableReaderOutsideCloneStillBlocks(t *testing.T) {
	tm := newReaderCloneTopology(t)
	dc1 := tm.sites[0].mysql.(*mockMySQL)
	dc2 := tm.sites[1].mysql.(*mockMySQL)

	dc1.setReadOnly(true)
	dc2.setReadOnly(true)
	dc1.setGtidExecuted("aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa:1-10")
	dc2.setGtidExecuted("aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa:1-8")

	tm.mu.Lock()
	tm.sites[0].state = state.StateReadOnly
	tm.sites[1].state = state.StateReadOnly
	tm.sites[2].state = state.StateUnreachable
	tm.lastFailoverTarget = "dc1"
	tm.promotionGtidExecuted = "aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa:1-9"
	tm.bootstrapPhase = BootstrapPhaseDone
	tm.mu.Unlock()

	if tm.checkPrimaryReassert(context.Background()) {
		t.Fatal("an unreachable reader with no clone in flight must still block re-assert")
	}
}
