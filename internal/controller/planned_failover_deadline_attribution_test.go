package controller

import (
	"errors"
	"testing"

	v1alpha1 "github.com/shipstream/bloodraven/api/v1alpha1"
)

// A rollback for AuthorityChanged over a fence that was only intended is
// not attributable: the run never sent its fence and authority had moved,
// so a read-only state found on the source is not the run's to lift. The
// rollback's deadline does not change that. Whether the last pass could
// read the source decides the result's reason (SourceLost when it could
// not), not whose the fence is: with the source unreachable at the
// deadline the run ended SourceLost and named the source for re-assert.
func TestPlannedFailover_RollbackDeadlineKeepsTheAttributionOfItsReason(t *testing.T) {
	for _, tc := range []struct {
		name        string
		rollbackFor v1alpha1.PlannedFailoverReason
		unreachable bool
		wantReason  v1alpha1.PlannedFailoverReason
		wantNamed   bool
	}{
		{"AuthorityChanged, the unfence failed", v1alpha1.PlannedFailoverReasonAuthorityChanged, false,
			v1alpha1.PlannedFailoverReasonAuthorityChanged, false},
		{"AuthorityChanged, the source is unreachable", v1alpha1.PlannedFailoverReasonAuthorityChanged, true,
			v1alpha1.PlannedFailoverReasonSourceLost, false},
		{"LegacyRunRefused, the source is unreachable", v1alpha1.PlannedFailoverReasonLegacyRunRefused, true,
			v1alpha1.PlannedFailoverReasonSourceLost, false},
		// The control: a rollback for a reason that leaves the intent
		// attributable still names the source.
		{"LagTimeout, the source is unreachable", v1alpha1.PlannedFailoverReasonLagTimeout, true,
			v1alpha1.PlannedFailoverReasonSourceLost, true},
		{"SourceLost, the source is unreachable", v1alpha1.PlannedFailoverReasonSourceLost, true,
			v1alpha1.PlannedFailoverReasonSourceLost, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			h := pfHarnessAt(t, v1alpha1.PlannedFailoverPhaseRollingBack)
			pf := h.pf().DeepCopy()
			pf.Fence.State = v1alpha1.PlannedFailoverFenceIntended
			pf.Fence.ApplyTime, pf.Fence.GtidSet = nil, nil
			pf.Rollback.Reason = tc.rollbackFor
			h.setBlock(pf)
			h.topo.rollback = func(PlannedRunRef) (RollbackResult, error) {
				if tc.unreachable {
					return RollbackResult{SourceUnreachable: true}, nil
				}
				return RollbackResult{}, errors.New("unfence failed")
			}
			h.clk.Advance(plannedFailoverRollbackDeadline)
			h.step()
			got := h.pf()
			if got.Result == nil || got.Result.Outcome != v1alpha1.PlannedFailoverOutcomeSourceLeftFenced || got.Result.Reason != tc.wantReason {
				t.Fatalf("result = %+v, want SourceLeftFenced{%s}", got.Result, tc.wantReason)
			}
			if named := len(h.topo.named) > 0; named != tc.wantNamed {
				t.Fatalf("named = %v, want named %v", h.topo.named, tc.wantNamed)
			}
			// A manager that starts on the record decides the same way.
			if _, ok := plannedFenceAttributableEnd(h.fg()); ok != tc.wantNamed {
				t.Fatalf("plannedFenceAttributableEnd = %v, want %v: a restarted manager must agree with the engine", ok, tc.wantNamed)
			}
		})
	}
}
