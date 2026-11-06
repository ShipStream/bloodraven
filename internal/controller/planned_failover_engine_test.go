package controller

import (
	"errors"
	"os"
	"regexp"
	"slices"
	"strings"
	"testing"
	"time"

	apimeta "k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	v1alpha1 "github.com/shipstream/bloodraven/api/v1alpha1"
)

// pfExpect is one engine-table expectation after the scripted steps.
type pfExpect struct {
	phase    v1alpha1.PlannedFailoverPhase
	outcome  v1alpha1.PlannedFailoverOutcome
	reason   v1alpha1.PlannedFailoverReason
	releases []string // exact release log
	events   []string // event reasons that must have been emitted
	held     string   // the run holding the slot afterwards ("" = empty)
	holdMode bool
	named    []string
	check    func(t *testing.T, h *pfEngineHarness)
}

func (x pfExpect) assert(t *testing.T, h *pfEngineHarness) {
	t.Helper()
	pf := h.pf()
	if pf == nil {
		t.Fatal("no block")
	}
	if x.phase != "" && pf.Phase != x.phase {
		t.Errorf("phase = %q, want %q (message %q)", pf.Phase, x.phase, pf.Message)
	}
	if x.outcome != "" {
		if pf.Result == nil || pf.Result.Outcome != x.outcome || pf.Result.Reason != x.reason {
			t.Errorf("result = %+v, want %s/%s (message %q)", pf.Result, x.outcome, x.reason, pf.Message)
		}
	}
	if got := h.topo.releasesCopy(); !slices.Equal(got, x.releases) {
		t.Errorf("releases = %v, want %v", got, x.releases)
	}
	for _, ev := range x.events {
		if h.sink.eventCount(ev) == 0 {
			t.Errorf("event %s not emitted; events %v", ev, h.sink.events)
		}
	}
	if held, mode := h.topo.held(); held != x.held || mode != x.holdMode {
		t.Errorf("slot = (%q, hold=%v), want (%q, hold=%v)", held, mode, x.held, x.holdMode)
	}
	if x.named != nil && !slices.Equal(h.topo.named, x.named) {
		t.Errorf("named = %v, want %v", h.topo.named, x.named)
	}
	if x.check != nil {
		x.check(t, h)
	}
}

// TestPlannedFailoverEngine_Table is the engine table: phase × store
// outcome × topology result → the next record, releases, events.
func TestPlannedFailoverEngine_Table(t *testing.T) {
	adm := v1alpha1.PlannedFailoverPhaseAdmitting
	cases := []struct {
		name  string
		phase v1alpha1.PlannedFailoverPhase
		setup func(h *pfEngineHarness)
		steps int
		until v1alpha1.PlannedFailoverPhase // stop stepping once the block reaches it
		want  pfExpect
	}{
		{name: "admitting/granted/committed", phase: adm, steps: 1,
			want: pfExpect{phase: v1alpha1.PlannedFailoverPhaseFencing, held: "run-1", events: []string{"PlannedFailoverClaimGranted"},
				check: func(t *testing.T, h *pfEngineHarness) {
					pf := h.pf()
					if pf.SourceSite != "iad" || pf.Fence == nil || pf.Fence.State != v1alpha1.PlannedFailoverFenceIntended ||
						pf.Fence.SourceServerUUID != "uuid-iad" || pf.ClaimBaseline == nil || pf.Revision != 2 {
						t.Errorf("fencing record = %+v", pf)
					}
				}}},
		{name: "admitting/granted/conflict×3 releases", phase: adm, steps: 1,
			setup: func(h *pfEngineHarness) {
				h.faults.statusFaults = []pfWriteFault{pfWriteConflict, pfWriteConflict, pfWriteConflict}
			},
			want: pfExpect{phase: adm, releases: []string{"run-1:notCommitted"}}},
		{name: "admitting/granted/ambiguous→committed", phase: adm, steps: 1,
			setup: func(h *pfEngineHarness) { h.faults.statusFaults = []pfWriteFault{pfWriteCommitTimeout} },
			want:  pfExpect{phase: v1alpha1.PlannedFailoverPhaseFencing, held: "run-1"}},
		{name: "admitting/granted/ambiguous→notCommitted releases", phase: adm, steps: 1,
			setup: func(h *pfEngineHarness) { h.faults.statusFaults = []pfWriteFault{pfWriteDropTimeout} },
			want: pfExpect{phase: adm, releases: []string{"run-1:notCommitted"},
				check: func(t *testing.T, h *pfEngineHarness) {
					if h.fg().Annotations[v1alpha1.PlannedFailoverStatusFenceAnnotation] == "" {
						t.Error("settlement did not fence the unmoved base version")
					}
				}}},
		{name: "admitting/granted/unknown keeps the provisional grant", phase: adm, steps: 1,
			setup: func(h *pfEngineHarness) {
				h.faults.statusFaults = []pfWriteFault{pfWriteDropTimeout}
				h.faults.metaFaults = []pfWriteFault{pfWriteDropTimeout, pfWriteOK}
			},
			want: pfExpect{phase: adm, held: "run-1",
				check: func(t *testing.T, h *pfEngineHarness) {
					if _, ok := parseDeadlineStart(h.fg().Annotations[v1alpha1.PlannedFailoverStallSinceAnnotation], "run-1"); !ok {
						t.Error("an Unknown write under a held claim did not start the durable stall deadline")
					}
				}}},
		{name: "admitting/wait records the reason once", phase: adm, steps: 2,
			setup: func(h *pfEngineHarness) {
				h.topo.claim = func(PlannedClaimRequest) (PlannedClaimDecision, error) {
					return PlannedClaimDecision{Kind: ClaimWait, WaitReason: v1alpha1.PlannedFailoverWaitPeerUnreadable}, nil
				}
			},
			want: pfExpect{phase: adm, events: []string{"PlannedFailoverAdmissionDelayed"},
				check: func(t *testing.T, h *pfEngineHarness) {
					if a := h.pf().Admission; a == nil || a.WaitReason != v1alpha1.PlannedFailoverWaitPeerUnreadable {
						t.Errorf("admission = %+v", a)
					}
					if n := h.sink.eventCount("PlannedFailoverAdmissionDelayed"); n != 1 {
						t.Errorf("Waiting events = %d, want 1 (only on change)", n)
					}
				}}},
		{name: "admitting/reject", phase: adm, steps: 1,
			setup: func(h *pfEngineHarness) {
				h.topo.claim = func(PlannedClaimRequest) (PlannedClaimDecision, error) {
					return PlannedClaimDecision{Kind: ClaimReject, RejectReason: v1alpha1.PlannedFailoverReasonManualRecoveryPending}, nil
				}
			},
			want: pfExpect{outcome: v1alpha1.PlannedFailoverOutcomeNotStarted, reason: v1alpha1.PlannedFailoverReasonManualRecoveryPending,
				events: []string{"PlannedFailoverRejected"}}},
		{name: "admitting/deadline", phase: adm, steps: 1,
			setup: func(h *pfEngineHarness) { h.clk.Advance(plannedFailoverAdmissionTimeout) },
			want:  pfExpect{outcome: v1alpha1.PlannedFailoverOutcomeNotStarted, reason: v1alpha1.PlannedFailoverReasonAdmissionTimeout}},
		{name: "admitting/manager absent waits", phase: adm, steps: 1,
			setup: func(h *pfEngineHarness) {
				// As the production claim answers with no manager (ultra
				// K123: the engine no longer maps errManagerAbsent here).
				h.topo.claim = func(PlannedClaimRequest) (PlannedClaimDecision, error) {
					return claimDecisionWait(v1alpha1.PlannedFailoverWaitManagerAbsent, "no topology manager is registered"), nil
				}
			},
			want: pfExpect{phase: adm, check: func(t *testing.T, h *pfEngineHarness) {
				if a := h.pf().Admission; a == nil || a.WaitReason != v1alpha1.PlannedFailoverWaitManagerAbsent {
					t.Errorf("admission = %+v", a)
				}
			}}},
		{name: "fencing/applied", phase: v1alpha1.PlannedFailoverPhaseFencing, steps: 1,
			want: pfExpect{phase: v1alpha1.PlannedFailoverPhaseDraining, held: "run-1", events: []string{"PlannedFailoverDrainStarted"},
				check: func(t *testing.T, h *pfEngineHarness) {
					pf := h.pf()
					if pf.Fence.State != v1alpha1.PlannedFailoverFenceApplied || plannedFenceSet(pf) != "uuid-iad:1-10" || pf.Drain == nil {
						t.Errorf("draining record = %+v %+v", pf.Fence, pf.Drain)
					}
				}}},
		{name: "fencing/applied/lost draining write re-proves, never re-fences", phase: v1alpha1.PlannedFailoverPhaseFencing, steps: 2,
			setup: func(h *pfEngineHarness) { h.faults.statusFaults = []pfWriteFault{pfWriteForbidden} },
			want: pfExpect{phase: v1alpha1.PlannedFailoverPhaseDraining, held: "run-1",
				check: func(t *testing.T, h *pfEngineHarness) {
					if h.topo.count("fence") != 1 || h.topo.count("proveFence") != 1 {
						t.Errorf("calls = %v, want one fence then one proof", h.topo.calls)
					}
				}}},
		{name: "fencing/not sent, source writable", phase: v1alpha1.PlannedFailoverPhaseFencing, steps: 1,
			setup: func(h *pfEngineHarness) {
				h.topo.fence = func(PlannedRunRef) (FenceProof, error) {
					return FenceProof{Kind: FenceNotSent, SourceWritable: true}, nil
				}
			},
			want: pfExpect{outcome: v1alpha1.PlannedFailoverOutcomeRolledBack, reason: v1alpha1.PlannedFailoverReasonAuthorityChanged,
				releases: []string{"run-1:terminal"}, named: []string{}}},
		{name: "fencing/not attributable is never named", phase: v1alpha1.PlannedFailoverPhaseFencing, steps: 1,
			setup: func(h *pfEngineHarness) {
				h.topo.fence = func(PlannedRunRef) (FenceProof, error) { return FenceProof{Kind: FenceNotAttributable}, nil }
			},
			want: pfExpect{outcome: v1alpha1.PlannedFailoverOutcomeSourceLeftFenced, reason: v1alpha1.PlannedFailoverReasonAuthorityChanged,
				releases: []string{"run-1:terminal"}, named: []string{},
				check: func(t *testing.T, h *pfEngineHarness) {
					if !slices.Contains(h.pf().Result.Warnings, v1alpha1.PlannedFailoverWarningFenceNotAttributable) {
						t.Errorf("warnings = %v", h.pf().Result.Warnings)
					}
				}}},
		{name: "fencing/foreign writable rolls back", phase: v1alpha1.PlannedFailoverPhaseFencing, steps: 1,
			setup: func(h *pfEngineHarness) {
				h.topo.fence = func(PlannedRunRef) (FenceProof, error) { return FenceProof{Kind: FenceForeignWritable}, nil }
			},
			want: pfExpect{phase: v1alpha1.PlannedFailoverPhaseRollingBack, held: "run-1", events: []string{"PlannedFailoverRollbackStarted"}}},
		{name: "fencing/retry past the deadline", phase: v1alpha1.PlannedFailoverPhaseFencing, steps: 1,
			setup: func(h *pfEngineHarness) {
				h.topo.fence = func(PlannedRunRef) (FenceProof, error) { return FenceProof{Kind: FenceRetry}, nil }
				h.clk.Advance(plannedFailoverFenceDeadline)
			},
			want: pfExpect{phase: v1alpha1.PlannedFailoverPhaseRollingBack, held: "run-1",
				check: func(t *testing.T, h *pfEngineHarness) {
					if h.pf().Rollback.Reason != v1alpha1.PlannedFailoverReasonFenceFailed {
						t.Errorf("rollback = %+v", h.pf().Rollback)
					}
				}}},
		{name: "fencing/source unreachable for the loss window", phase: v1alpha1.PlannedFailoverPhaseFencing, until: v1alpha1.PlannedFailoverPhaseRollingBack, steps: 12,
			setup: func(h *pfEngineHarness) {
				h.topo.fence = func(PlannedRunRef) (FenceProof, error) { return FenceProof{Kind: FenceSourceUnreachable}, nil }
			},
			want: pfExpect{phase: v1alpha1.PlannedFailoverPhaseRollingBack, held: "run-1",
				check: func(t *testing.T, h *pfEngineHarness) {
					if h.pf().Rollback.Reason != v1alpha1.PlannedFailoverReasonSourceLost {
						t.Errorf("rollback = %+v", h.pf().Rollback)
					}
				}}},
		{name: "draining/idle", phase: v1alpha1.PlannedFailoverPhaseDraining, steps: 1,
			want: pfExpect{phase: v1alpha1.PlannedFailoverPhaseWaitingForLag, held: "run-1"}},
		{name: "draining/busy until the drain timeout", phase: v1alpha1.PlannedFailoverPhaseDraining, until: v1alpha1.PlannedFailoverPhaseWaitingForLag, steps: 8,
			setup: func(h *pfEngineHarness) { h.topo.kill = func(PlannedRunRef) (int32, error) { return 3, nil } },
			want: pfExpect{phase: v1alpha1.PlannedFailoverPhaseWaitingForLag, held: "run-1",
				check: func(t *testing.T, h *pfEngineHarness) {
					if r := h.pf().Drain.RemainingConnections; r == nil || *r != 3 {
						t.Errorf("remaining = %v", r)
					}
				}}},
		{name: "lag/caught up writes the promoting intent", phase: v1alpha1.PlannedFailoverPhaseWaitingForLag, steps: 1,
			want: pfExpect{phase: v1alpha1.PlannedFailoverPhasePromoting, held: "run-1", events: []string{"PlannedFailoverTargetCaughtUp"},
				check: func(t *testing.T, h *pfEngineHarness) {
					if p := h.pf().Promotion; p == nil || p.TargetServerUUID != "uuid-pdx" || p.State != v1alpha1.PlannedFailoverPromotionIntended {
						t.Errorf("promotion = %+v", p)
					}
				}}},
		{name: "lag/divergent", phase: v1alpha1.PlannedFailoverPhaseWaitingForLag, steps: 1,
			setup: func(h *pfEngineHarness) {
				h.topo.probe = func(PlannedRunRef) (LagProbe, error) {
					return LagProbe{SourceReachable: true, TargetReachable: true, Divergent: true}, nil
				}
			},
			want: pfExpect{phase: v1alpha1.PlannedFailoverPhaseRollingBack, held: "run-1",
				check: func(t *testing.T, h *pfEngineHarness) {
					if h.pf().Rollback.Reason != v1alpha1.PlannedFailoverReasonDivergentTarget {
						t.Errorf("rollback = %+v", h.pf().Rollback)
					}
				}}},
		{name: "lag/timeout", phase: v1alpha1.PlannedFailoverPhaseWaitingForLag, steps: 1,
			setup: func(h *pfEngineHarness) {
				h.topo.probe = func(PlannedRunRef) (LagProbe, error) {
					return LagProbe{SourceReachable: true, TargetReachable: true}, nil
				}
				h.clk.Advance(time.Minute)
			},
			want: pfExpect{phase: v1alpha1.PlannedFailoverPhaseRollingBack, held: "run-1",
				check: func(t *testing.T, h *pfEngineHarness) {
					if h.pf().Rollback.Reason != v1alpha1.PlannedFailoverReasonLagTimeout || h.sink.lagWaits != 1 {
						t.Errorf("rollback = %+v lagWaits=%d", h.pf().Rollback, h.sink.lagWaits)
					}
				}}},
		{name: "lag/target lost", phase: v1alpha1.PlannedFailoverPhaseWaitingForLag, until: v1alpha1.PlannedFailoverPhaseRollingBack, steps: 12,
			setup: func(h *pfEngineHarness) {
				h.topo.probe = func(PlannedRunRef) (LagProbe, error) { return LagProbe{SourceReachable: true}, nil }
			},
			want: pfExpect{phase: v1alpha1.PlannedFailoverPhaseRollingBack, held: "run-1",
				check: func(t *testing.T, h *pfEngineHarness) {
					if h.pf().Rollback.Reason != v1alpha1.PlannedFailoverReasonTargetLost {
						t.Errorf("rollback = %+v", h.pf().Rollback)
					}
				}}},
		{name: "promoting/confirmed then the confirmed tail", phase: v1alpha1.PlannedFailoverPhasePromoting, steps: 2,
			want: pfExpect{phase: v1alpha1.PlannedFailoverPhasePublishing, releases: []string{"run-1:publishing"},
				check: func(t *testing.T, h *pfEngineHarness) {
					pf := h.pf()
					if pf.Promotion.State != v1alpha1.PlannedFailoverPromotionConfirmed || pf.Promotion.ConfirmationTime == nil ||
						pf.DNS == nil || pf.DNS.State != v1alpha1.PlannedFailoverDNSApplied {
						t.Errorf("publishing record = %+v %+v", pf.Promotion, pf.DNS)
					}
					if h.topo.count("record") != 1 || h.topo.count("dns") != 1 {
						t.Errorf("calls = %v", h.topo.calls)
					}
				}}},
		{name: "promoting/dns failure still publishes with a warning", phase: v1alpha1.PlannedFailoverPhasePromoting, until: v1alpha1.PlannedFailoverPhaseSucceeded, steps: 3,
			setup: func(h *pfEngineHarness) {
				h.topo.dns = func(PlannedRunRef) (v1alpha1.PlannedFailoverDNSState, error) {
					return v1alpha1.PlannedFailoverDNSFailed, errors.New("dns down")
				}
			},
			want: pfExpect{outcome: v1alpha1.PlannedFailoverOutcomeSucceeded, releases: []string{"run-1:publishing"},
				events: []string{"PlannedFailoverDNSUpdateFailed", "PlannedFailoverSucceeded"},
				check: func(t *testing.T, h *pfEngineHarness) {
					if !slices.Contains(h.pf().Result.Warnings, v1alpha1.PlannedFailoverWarningDNSUpdateFailed) {
						t.Errorf("warnings = %v", h.pf().Result.Warnings)
					}
				}}},
		{name: "promoting/aborted rolls back", phase: v1alpha1.PlannedFailoverPhasePromoting, steps: 1,
			setup: func(h *pfEngineHarness) {
				h.topo.promote = func(PlannedRunRef) (PromoteOutcome, error) {
					return PromoteOutcome{Kind: PromoteAborted, Reason: v1alpha1.PlannedFailoverReasonTargetChanged, TargetReachable: true}, nil
				}
			},
			want: pfExpect{phase: v1alpha1.PlannedFailoverPhaseRollingBack, held: "run-1"}},
		{name: "promoting/aborted with a direct terminal", phase: v1alpha1.PlannedFailoverPhasePromoting, steps: 1,
			setup: func(h *pfEngineHarness) {
				h.topo.promote = func(PlannedRunRef) (PromoteOutcome, error) {
					return PromoteOutcome{Kind: PromoteAborted, Reason: v1alpha1.PlannedFailoverReasonSourceLost,
						Terminal: v1alpha1.PlannedFailoverOutcomeSourceLeftFenced}, nil
				}
			},
			want: pfExpect{outcome: v1alpha1.PlannedFailoverOutcomeSourceLeftFenced, reason: v1alpha1.PlannedFailoverReasonSourceLost,
				releases: []string{"run-1:terminal"}, named: []string{"source:iad"}}},
		{name: "promoting/inconsistent holds", phase: v1alpha1.PlannedFailoverPhasePromoting, steps: 1,
			setup: func(h *pfEngineHarness) {
				h.topo.promote = func(PlannedRunRef) (PromoteOutcome, error) {
					return PromoteOutcome{Kind: PromoteInconsistent, Reason: v1alpha1.PlannedFailoverReasonForeignWritable, MayHaveWritten: true}, nil
				}
			},
			want: pfExpect{outcome: v1alpha1.PlannedFailoverOutcomeManualRecoveryRequired, reason: v1alpha1.PlannedFailoverReasonForeignWritable,
				releases: []string{"run-1:toHold"}, held: "run-1", holdMode: true, named: []string{},
				events: []string{"PlannedFailoverManualRecoveryRequired"},
				check: func(t *testing.T, h *pfEngineHarness) {
					hold := PlannedHoldFrom(h.fg())
					if hold == nil || hold.RunID != "run-1" || !hold.FromStatus || hold.Reason != v1alpha1.PlannedFailoverReasonForeignWritable {
						t.Errorf("hold = %+v", hold)
					}
				}}},
		{name: "promoting/unconfirmed after a possible write, past the deadline", phase: v1alpha1.PlannedFailoverPhasePromoting, steps: 1,
			setup: func(h *pfEngineHarness) {
				h.topo.promote = func(PlannedRunRef) (PromoteOutcome, error) {
					return PromoteOutcome{Kind: PromoteUnconfirmed, TargetReachable: true, MayHaveWritten: true}, nil
				}
				h.clk.Advance(plannedFailoverPromotionDeadline)
			},
			want: pfExpect{outcome: v1alpha1.PlannedFailoverOutcomePromotedUnconfirmed, reason: v1alpha1.PlannedFailoverReasonPromotionUnconfirmed,
				releases: []string{"run-1:terminal"}, named: []string{"target:pdx"},
				events: []string{"PlannedFailoverPromotionConfirmationFailed"}}},
		{name: "promoting/unconfirmed with nothing sent, past the deadline", phase: v1alpha1.PlannedFailoverPhasePromoting, steps: 1,
			setup: func(h *pfEngineHarness) {
				h.topo.promote = func(PlannedRunRef) (PromoteOutcome, error) {
					return PromoteOutcome{Kind: PromoteUnconfirmed, TargetReachable: true}, nil
				}
				h.clk.Advance(plannedFailoverPromotionDeadline)
			},
			want: pfExpect{phase: v1alpha1.PlannedFailoverPhaseRollingBack, held: "run-1",
				check: func(t *testing.T, h *pfEngineHarness) {
					if h.pf().Rollback.Reason != v1alpha1.PlannedFailoverReasonPromotionAborted {
						t.Errorf("rollback = %+v", h.pf().Rollback)
					}
				}}},
		{name: "promoting/hydrated claim re-proves first", phase: v1alpha1.PlannedFailoverPhasePromoting, steps: 1,
			setup: func(h *pfEngineHarness) {
				h.topo.needsProof = true
				h.topo.reprove = func(PlannedRunRef) (ReproveResult, error) {
					return ReproveResult{RollBack: reasonPtr(v1alpha1.PlannedFailoverReasonAuthorityChanged)}, nil
				}
			},
			want: pfExpect{phase: v1alpha1.PlannedFailoverPhaseRollingBack, held: "run-1",
				check: func(t *testing.T, h *pfEngineHarness) {
					if h.topo.count("promote") != 0 || h.topo.count("reprove") != 1 {
						t.Errorf("calls = %v, want the re-proof and no promotion", h.topo.calls)
					}
				}}},
		{name: "rollingBack/rolled back", phase: v1alpha1.PlannedFailoverPhaseRollingBack, steps: 1,
			want: pfExpect{outcome: v1alpha1.PlannedFailoverOutcomeRolledBack, reason: v1alpha1.PlannedFailoverReasonLagTimeout,
				releases: []string{"run-1:terminal"}, events: []string{"PlannedFailoverFailed"}}},
		{name: "rollingBack/left fenced is named for re-assert", phase: v1alpha1.PlannedFailoverPhaseRollingBack, steps: 1,
			setup: func(h *pfEngineHarness) {
				h.topo.rollback = func(PlannedRunRef) (RollbackResult, error) {
					return RollbackResult{Outcome: v1alpha1.PlannedFailoverOutcomeSourceLeftFenced}, nil
				}
			},
			want: pfExpect{outcome: v1alpha1.PlannedFailoverOutcomeSourceLeftFenced, reason: v1alpha1.PlannedFailoverReasonLagTimeout,
				releases: []string{"run-1:terminal"}, named: []string{"source:iad"}}},
		{name: "rollingBack/deadline leaves the source fenced", phase: v1alpha1.PlannedFailoverPhaseRollingBack, steps: 1,
			setup: func(h *pfEngineHarness) {
				h.topo.rollback = func(PlannedRunRef) (RollbackResult, error) { return RollbackResult{}, errors.New("unfence failed") }
				h.clk.Advance(plannedFailoverRollbackDeadline)
			},
			want: pfExpect{outcome: v1alpha1.PlannedFailoverOutcomeSourceLeftFenced, reason: v1alpha1.PlannedFailoverReasonLagTimeout,
				releases: []string{"run-1:terminal"}, named: []string{"source:iad"}}},
		{name: "publishing/succeeded writes the failover record", phase: v1alpha1.PlannedFailoverPhasePublishing, steps: 1,
			want: pfExpect{outcome: v1alpha1.PlannedFailoverOutcomeSucceeded, events: []string{"PlannedFailoverSucceeded"},
				check: func(t *testing.T, h *pfEngineHarness) {
					st := h.fg().Status
					if st.LastFailover == nil || st.LastFailoverTarget != "pdx" || st.PromotionGtidExecuted != "uuid-iad:1-10" {
						t.Errorf("failover record = %v %q %q", st.LastFailover, st.LastFailoverTarget, st.PromotionGtidExecuted)
					}
					if len(h.sink.terminals) != 1 || mapOutcomeToMetricResult(&h.sink.terminals[0]) != "success" {
						t.Errorf("terminals = %+v", h.sink.terminals)
					}
				}}},
		{name: "publishing/never moves the failover record back", phase: v1alpha1.PlannedFailoverPhasePublishing, steps: 1,
			setup: func(h *pfEngineHarness) {
				fg := h.fg()
				// Newer, but within FailoverClockSkewGrace of the clock: a
				// plausible later record (K023 lets only an implausibly
				// future-dated one be written over).
				later := metav1.NewTime(h.clk.Now().Add(2 * time.Minute))
				fg.Status.LastFailover, fg.Status.LastFailoverTarget, fg.Status.PromotionGtidExecuted = &later, "iad", "newer"
				if err := h.c.Status().Update(t.Context(), fg); err != nil {
					t.Fatal(err)
				}
			},
			want: pfExpect{outcome: v1alpha1.PlannedFailoverOutcomeSucceeded,
				check: func(t *testing.T, h *pfEngineHarness) {
					if st := h.fg().Status; st.LastFailoverTarget != "iad" || st.PromotionGtidExecuted != "newer" {
						t.Errorf("an older run record replaced a newer one: %q %q", st.LastFailoverTarget, st.PromotionGtidExecuted)
					}
				}}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			h := pfHarnessAt(t, tc.phase)
			if tc.setup != nil {
				tc.setup(h)
			}
			for i := 0; i < tc.steps; i++ {
				if tc.until != "" && h.pf().Phase == tc.until {
					break
				}
				d := h.step()
				if d <= 0 {
					d = time.Second
				}
				if i < tc.steps-1 {
					h.clk.Advance(d)
				}
			}
			tc.want.assert(t, h)
		})
	}
}

// A fresh request runs Admitting → Succeeded with the annotation removed
// only after the acceptance write.
func TestPlannedFailoverEngine_HappyPath(t *testing.T) {
	h := newPFEngineHarness(t, plannedFailoverFG("pdx"))
	h.step()
	pf := h.pf()
	if pf == nil || pf.Phase != v1alpha1.PlannedFailoverPhaseAdmitting || pf.RunID == "" || pf.Revision != 1 || pf.Settings.MaxLagWait.Duration == 0 {
		t.Fatalf("admitting record = %+v", pf)
	}
	if _, ok := h.fg().Annotations[v1alpha1.PlannedFailoverAnnotation]; ok {
		t.Error("annotation kept after the Admitting write committed")
	}
	runID := pf.RunID
	h.stepUntil(20, func(fg *v1alpha1.MysqlFailoverGroup) bool { return plannedFailoverTerminal(fg.Status.PlannedFailover) })
	pf = h.pf()
	if pf.RunID != runID || pf.Result.Outcome != v1alpha1.PlannedFailoverOutcomeSucceeded {
		t.Fatalf("final = %+v", pf)
	}
	if got := h.topo.releasesCopy(); !slices.Equal(got, []string{runID + ":publishing"}) {
		t.Errorf("releases = %v", got)
	}
	if h.topo.count("fence") != 1 || h.topo.count("promote") != 1 || h.topo.count("record") != 1 {
		t.Errorf("side effects = %v, want exactly one fence, promotion and record", h.topo.calls)
	}
}

// An annotation-free Deferred run is cancelled; a cooldown with
// onCooldown=defer defers and keeps the annotation.
func TestPlannedFailoverEngine_DeferredCooldown(t *testing.T) {
	fg := plannedFailoverFG("pdx")
	fg.Spec.PlannedFailover = &v1alpha1.PlannedFailoverSpec{OnCooldown: v1alpha1.PlannedFailoverCooldownDefer}
	recent := metav1.NewTime(time.Date(2026, 11, 4, 11, 58, 0, 0, time.UTC))
	fg.Status.LastFailover, fg.Status.LastFailoverTarget = &recent, "iad"
	h := newPFEngineHarness(t, fg)
	h.step()
	pf := h.pf()
	if pf.Phase != v1alpha1.PlannedFailoverPhaseDeferred || pf.Admission == nil || pf.Admission.DeferReason != v1alpha1.PlannedFailoverDeferCooldownActive ||
		pf.Admission.RetryTime == nil {
		t.Fatalf("deferred record = %+v", pf)
	}
	if _, ok := h.fg().Annotations[v1alpha1.PlannedFailoverAnnotation]; !ok {
		t.Fatal("Deferred must keep the annotation")
	}
	if len(h.sink.deferred) != 1 {
		t.Errorf("deferred metric = %v", h.sink.deferred)
	}
	runID := pf.RunID
	h.clk.Advance(5 * time.Minute)
	h.step()
	if pf := h.pf(); pf.Phase != v1alpha1.PlannedFailoverPhaseAdmitting || pf.RunID != runID {
		t.Fatalf("after the cooldown = %+v, want the same run in Admitting", pf)
	}
	if _, ok := h.fg().Annotations[v1alpha1.PlannedFailoverAnnotation]; ok {
		t.Error("annotation kept after Admitting committed")
	}
}

func TestPlannedFailoverEngine_DeferredAnnotationRemovedCancels(t *testing.T) {
	h := pfHarnessAt(t, v1alpha1.PlannedFailoverPhaseDeferred)
	h.step()
	if pf := h.pf(); pf.Result == nil || pf.Result.Outcome != v1alpha1.PlannedFailoverOutcomeNotStarted ||
		pf.Result.Reason != v1alpha1.PlannedFailoverReasonCanceled {
		t.Fatalf("block = %+v", pf)
	}
}

// A pending request owns nothing: no claim call happens while it waits on a
// deployment hold, and a fresh request validates statically first.
func TestPlannedFailoverEngine_RejectsStatically(t *testing.T) {
	h := newPFEngineHarness(t, plannedFailoverFG("nowhere"))
	h.step()
	pf := h.pf()
	if pf == nil || pf.Result == nil || pf.Result.Reason != v1alpha1.PlannedFailoverReasonUnknownSite {
		t.Fatalf("block = %+v", pf)
	}
	if h.topo.count("claim") != 0 {
		t.Error("a statically invalid request asked for the claim")
	}
	if _, ok := h.fg().Annotations[v1alpha1.PlannedFailoverAnnotation]; ok {
		t.Error("rejected annotation kept")
	}
}

// A request for the active site is skipped without a block.
func TestPlannedFailoverEngine_AlreadyActiveSkipped(t *testing.T) {
	h := newPFEngineHarness(t, plannedFailoverFG("iad"))
	h.step()
	if h.pf() != nil {
		t.Fatalf("block = %+v, want none", h.pf())
	}
	if h.sink.eventCount("PlannedFailoverSkipped") != 1 {
		t.Errorf("events = %v", h.sink.events)
	}
}

// §13: blocks an older operator wrote are refused, and nothing is hydrated.
func TestPlannedFailoverEngine_LegacyRefused(t *testing.T) {
	for _, tc := range []struct {
		phase   v1alpha1.PlannedFailoverPhase
		outcome v1alpha1.PlannedFailoverOutcome
	}{
		{plannedLegacyPhasePending, v1alpha1.PlannedFailoverOutcomeNotStarted},
		{plannedLegacyPhaseValidating, v1alpha1.PlannedFailoverOutcomeNotStarted},
		{v1alpha1.PlannedFailoverPhaseDeferred, v1alpha1.PlannedFailoverOutcomeNotStarted},
		{v1alpha1.PlannedFailoverPhaseDraining, v1alpha1.PlannedFailoverOutcomeSourceLeftFenced},
		// The ultra review's K096: an older operator's promotion phases may
		// have made the target writable.
		{plannedLegacyPhaseResuming, v1alpha1.PlannedFailoverOutcomePromotedUnconfirmed},
		{v1alpha1.PlannedFailoverPhasePromoting, v1alpha1.PlannedFailoverOutcomePromotedUnconfirmed},
	} {
		t.Run(string(tc.phase), func(t *testing.T) {
			h := newPFEngineHarness(t, plannedFailoverFG(""))
			start := metav1.NewTime(h.clk.Now())
			h.setBlock(&v1alpha1.PlannedFailoverStatus{Phase: tc.phase, TargetSite: "pdx", StartTime: start})
			h.step()
			pf := h.pf()
			if pf.Result == nil || pf.Result.Outcome != tc.outcome || pf.Result.Reason != v1alpha1.PlannedFailoverReasonLegacyRunRefused || pf.RunID == "" {
				t.Fatalf("block = %+v", pf)
			}
			if h.sink.eventCount("PlannedFailoverLegacyRunRefused") != 1 {
				t.Errorf("events = %v", h.sink.events)
			}
			// K044: a run that may leave every site read-only never
			// advises re-annotating as the way out.
			if tc.outcome != v1alpha1.PlannedFailoverOutcomeNotStarted && !strings.Contains(pf.Message, plannedManualUnfenceAdvice) {
				t.Errorf("message = %q; want the manual-unfence advice", pf.Message)
			}
			if len(h.topo.calls) != 0 || h.topo.PlannedClaimHeld(h.nn) {
				t.Errorf("a legacy block touched the topology: %v", h.topo.calls)
			}
		})
	}
}

// §6.4: the stall deadline runs from the durable stall-since annotation,
// so repeated restarts during a status outage do not extend it.
func TestPF_StallDeadlineSurvivesRepeatedRestarts(t *testing.T) {
	h := pfHarnessAt(t, v1alpha1.PlannedFailoverPhaseWaitingForLag)
	h.faults.statusDenied = true
	h.step() // caught up: the Promoting intent write is denied
	start, ok := parseDeadlineStart(h.fg().Annotations[v1alpha1.PlannedFailoverStallSinceAnnotation], "run-1")
	if !ok {
		t.Fatal("no durable stall start after a denied write under the claim")
	}
	for i := 0; i < 4; i++ {
		h.clk.Advance(25 * time.Second)
		h.restart()
		h.step()
		if got, _ := parseDeadlineStart(h.fg().Annotations[v1alpha1.PlannedFailoverStallSinceAnnotation], "run-1"); !got.Equal(start) {
			t.Fatalf("restart %d moved the stall start from %s to %s", i, start, got)
		}
		if _, marked := plannedFailoverRun(h.fg()); marked {
			t.Fatalf("marked after only %s", h.clk.Now().Sub(start))
		}
	}
	h.clk.Advance(plannedFailoverStatusStallDeadline - 100*time.Second)
	h.restart()
	h.step()
	fg := h.fg()
	m, ok := ParseTerminalMarker(fg.Annotations[v1alpha1.PlannedFailoverTerminalAnnotation])
	if !ok || m.RunID != "run-1" || m.Outcome != v1alpha1.PlannedFailoverOutcomeSourceLeftFenced || m.Reason != v1alpha1.PlannedFailoverReasonStatusStalled {
		t.Fatalf("marker = %q", fg.Annotations[v1alpha1.PlannedFailoverTerminalAnnotation])
	}
	if !plannedFailoverOwnsSites(fg) == false {
		t.Error("a marked run still owns the sites")
	}
	if got := h.topo.releasesCopy(); !slices.Equal(got, []string{"run-1:statusStalled"}) {
		t.Errorf("releases = %v", got)
	}
	if !slices.Contains(h.topo.named, "source:iad") {
		t.Errorf("the attributable fenced source was not named before the release: %v", h.topo.named)
	}
	// Status recovers: the terminal record lands, then the marker goes.
	h.faults.statusDenied = false
	h.clk.Advance(plannedFailoverMarkerRetryInterval)
	h.step()
	h.step()
	fg = h.fg()
	if pf := fg.Status.PlannedFailover; pf.Result == nil || pf.Result.Outcome != v1alpha1.PlannedFailoverOutcomeSourceLeftFenced {
		t.Fatalf("terminal record = %+v", pf)
	}
	if _, ok := fg.Annotations[v1alpha1.PlannedFailoverTerminalAnnotation]; ok {
		t.Error("non-hold marker kept after the terminal record committed")
	}
}

// A stall before any fence was sent ends NotStarted: the provisional
// grant's Fencing write keeps timing out and its settlement cannot fence
// (a proven non-commit would release the grant at once instead).
func TestPF_StatusStallBeforeFenceIsNotStarted(t *testing.T) {
	h := newPFEngineHarness(t, plannedFailoverFG(""))
	pf := pfRunAt(v1alpha1.PlannedFailoverPhaseAdmitting, h.clk.Now())
	later := metav1.NewTime(h.clk.Now().Add(10 * time.Minute))
	pf.Admission.DeadlineTime = &later // the stall, not the admission deadline, ends this run
	h.setBlock(pf)
	h.faults.statusDefault = pfWriteDropTimeout
	h.faults.denyMetaKey = v1alpha1.PlannedFailoverStatusFenceAnnotation
	h.step()
	if held, _ := h.topo.held(); held != "run-1" {
		t.Fatalf("an unsettled Fencing write released the grant (slot %q)", held)
	}
	h.clk.Advance(plannedFailoverStatusStallDeadline)
	h.step()
	m, ok := ParseTerminalMarker(h.fg().Annotations[v1alpha1.PlannedFailoverTerminalAnnotation])
	if !ok || m.Outcome != v1alpha1.PlannedFailoverOutcomeNotStarted || m.Reason != v1alpha1.PlannedFailoverReasonStatusStalled {
		t.Fatalf("marker = %+v ok=%v", m, ok)
	}
	if held, _ := h.topo.held(); held != "" {
		t.Errorf("slot still held by %q", held)
	}
}

// §6.3: the manager-absence deadline runs from the durable annotation.
func TestPF_ManagerAbsenceDeadlineSurvivesRestart(t *testing.T) {
	h := pfHarnessAt(t, v1alpha1.PlannedFailoverPhaseWaitingForLag)
	h.topo.setHeld("", false)
	h.topo.ensure = func(PlannedRunRef) (PlannedClaimDecision, error) { return PlannedClaimDecision{}, errManagerAbsent }
	h.step()
	start, ok := parseDeadlineStart(h.fg().Annotations[v1alpha1.PlannedFailoverManagerAbsentSinceAnnotation], "run-1")
	if !ok {
		t.Fatal("no durable absent-since start")
	}
	h.clk.Advance(plannedFailoverManagerAbsentWarnAfter)
	h.step()
	if h.sink.eventCount("PlannedFailoverStalledManagerAbsent") != 1 {
		t.Errorf("events = %v, want PlannedFailoverStalledManagerAbsent after 30s", h.sink.events)
	}
	for i := 0; i < 3; i++ {
		h.clk.Advance(time.Minute)
		h.restart()
		h.step()
		if got, _ := parseDeadlineStart(h.fg().Annotations[v1alpha1.PlannedFailoverManagerAbsentSinceAnnotation], "run-1"); !got.Equal(start) {
			t.Fatalf("restart moved the absent-since start: %s → %s", start, got)
		}
		if plannedFailoverTerminal(h.pf()) {
			t.Fatalf("ended after %s", h.clk.Now().Sub(start))
		}
	}
	h.clk.Advance(plannedFailoverManagerAbsentDeadline - h.clk.Now().Sub(start))
	h.restart()
	h.step()
	pf := h.pf()
	if pf.Result == nil || pf.Result.Outcome != v1alpha1.PlannedFailoverOutcomeSourceLeftFenced ||
		pf.Result.Reason != v1alpha1.PlannedFailoverReasonManagerUnavailable {
		t.Fatalf("block = %+v", pf)
	}
	if slices.ContainsFunc(h.topo.calls, func(c string) bool { return c != "ensure" }) {
		t.Errorf("MySQL calls without a manager: %v", h.topo.calls)
	}
	if _, ok := h.fg().Annotations[v1alpha1.PlannedFailoverManagerAbsentSinceAnnotation]; ok {
		t.Error("absent-since annotation kept after the terminal record")
	}
}

// A run this process marked is never driven as live again.
func TestPF_MarkedRunNeverRehydrated(t *testing.T) {
	h := pfHarnessAt(t, v1alpha1.PlannedFailoverPhaseWaitingForLag)
	h.setAnnotation(v1alpha1.PlannedFailoverTerminalAnnotation, `{"schemaVersion":1,"runID":"run-1","outcome":"SourceLeftFenced","reason":"StatusStalled"}`)
	h.faults.statusDenied = true
	h.step()
	if held, _ := h.topo.held(); held != "" {
		t.Errorf("a marked run's claim stayed held: %q", held)
	}
	if h.topo.count("ensure") != 0 || h.topo.count("probe") != 0 {
		t.Errorf("a marked run was driven: %v", h.topo.calls)
	}
}

// Zombie clear: a Run-mode claim whose block is gone or terminal is
// released by the engine.
func TestPF_ZombieClaimReleased(t *testing.T) {
	h := pfHarnessAt(t, v1alpha1.PlannedFailoverPhaseWaitingForLag)
	h.topo.probe = func(PlannedRunRef) (LagProbe, error) {
		return LagProbe{SourceReachable: true, TargetReachable: true}, nil
	}
	h.step() // sees run-1 live
	h.setBlock(nil)
	h.step()
	if got := h.topo.releasesCopy(); !slices.Equal(got, []string{"run-1:zombie"}) {
		t.Errorf("releases = %v", got)
	}
}

// TestOutcomeReasonMapping pins the metric mapping and that every reason
// in §10.2 is a known enum value.
func TestOutcomeReasonMapping(t *testing.T) {
	cases := []struct {
		outcome v1alpha1.PlannedFailoverOutcome
		reason  v1alpha1.PlannedFailoverReason
		want    string
	}{
		{v1alpha1.PlannedFailoverOutcomeSucceeded, "", "success"},
		{v1alpha1.PlannedFailoverOutcomeNotStarted, v1alpha1.PlannedFailoverReasonCooldownActive, "rejected"},
		{v1alpha1.PlannedFailoverOutcomeNotStarted, v1alpha1.PlannedFailoverReasonAdmissionTimeout, "failed_timeout"},
		{v1alpha1.PlannedFailoverOutcomeRolledBack, v1alpha1.PlannedFailoverReasonLagTimeout, "failed_timeout"},
		{v1alpha1.PlannedFailoverOutcomeSourceLeftFenced, v1alpha1.PlannedFailoverReasonDragonflySyncTimeout, "failed_timeout"},
		{v1alpha1.PlannedFailoverOutcomeRolledBack, v1alpha1.PlannedFailoverReasonDivergentTarget, "failed_other"},
		{v1alpha1.PlannedFailoverOutcomePromotedUnconfirmed, v1alpha1.PlannedFailoverReasonTargetLost, "failed_other"},
		{v1alpha1.PlannedFailoverOutcomeManualRecoveryRequired, v1alpha1.PlannedFailoverReasonExecuteFailed, "failed_other"},
	}
	for _, tc := range cases {
		if got := mapOutcomeToMetricResult(&v1alpha1.PlannedFailoverResult{Outcome: tc.outcome, Reason: tc.reason}); got != tc.want {
			t.Errorf("%s/%s → %q, want %q", tc.outcome, tc.reason, got, tc.want)
		}
	}
	if got := mapOutcomeToMetricResult(nil); got != "failed_other" {
		t.Errorf("nil → %q", got)
	}
	// ExecuteFailed is only ever a ManualRecoveryRequired reason (§10.2):
	// the engine maps an Inconsistent promotion without a reason to it.
	h := pfHarnessAt(t, v1alpha1.PlannedFailoverPhasePromoting)
	h.topo.promote = func(PlannedRunRef) (PromoteOutcome, error) { return PromoteOutcome{Kind: PromoteInconsistent}, nil }
	h.step()
	if r := h.pf().Result; r == nil || r.Outcome != v1alpha1.PlannedFailoverOutcomeManualRecoveryRequired || r.Reason != v1alpha1.PlannedFailoverReasonExecuteFailed {
		t.Errorf("inconsistent → %+v", r)
	}
}

// TestNoCachedDecisionReads: the engine, its store and the marker handling
// read only through PlannedFailoverStore.Load (the uncached reader); none
// of them reaches the informer cache through the reconciler's client.
func TestNoCachedDecisionReads(t *testing.T) {
	forbidden := regexp.MustCompile(`\br\.(Client\.)?Get\(|\.Client\.Get\(|\bc\.Get\(|MysqlFailoverGroupReconciler`)
	for _, file := range []string{"planned_failover_engine.go", "planned_failover_marker.go", "planned_failover_store.go"} {
		b, err := os.ReadFile(file)
		if err != nil {
			t.Fatal(err)
		}
		for i, line := range strings.Split(string(b), "\n") {
			code, _, _ := strings.Cut(line, "//")
			if forbidden.MatchString(code) {
				t.Errorf("%s:%d reads outside the store: %s", file, i+1, strings.TrimSpace(line))
			}
		}
	}
	b, err := os.ReadFile("planned_failover_store.go")
	if err != nil {
		t.Fatal(err)
	}
	if strings.Count(string(b), "s.reader.Get(") != 1 {
		t.Error("the store must read only through its uncached reader, in Load")
	}
}

// The terminal write records the hold in status.plannedFailoverHold, and
// the True condition summarises it: PlannedHoldFrom reads the typed field,
// never the message.
func TestPlannedFailoverEngine_HoldRecord(t *testing.T) {
	h := pfHarnessAt(t, v1alpha1.PlannedFailoverPhasePromoting)
	h.topo.promote = func(PlannedRunRef) (PromoteOutcome, error) {
		return PromoteOutcome{Kind: PromoteInconsistent, Reason: v1alpha1.PlannedFailoverReasonForeignWritable}, nil
	}
	h.step()
	fg := h.fg()
	c := apimeta.FindStatusCondition(fg.Status.Conditions, v1alpha1.ConditionAutomaticFailoverHeld)
	if c == nil || c.Status != metav1.ConditionTrue || c.Reason != v1alpha1.ReasonPlannedFailoverManualRecoveryRequired ||
		c.ObservedGeneration != fg.Generation || !strings.Contains(c.Message, v1alpha1.AcknowledgePlannedFailoverHoldAnnotation+"=run-1") {
		t.Fatalf("condition = %+v", c)
	}
	hold := fg.Status.PlannedFailoverHold
	if hold == nil || hold.RunID != "run-1" || hold.Reason != v1alpha1.PlannedFailoverReasonForeignWritable ||
		hold.TargetSite != fg.Status.PlannedFailover.TargetSite || hold.SourceSite != fg.Status.PlannedFailover.SourceSite ||
		hold.HoldTime.IsZero() {
		t.Errorf("status.plannedFailoverHold = %+v", hold)
	}
}
