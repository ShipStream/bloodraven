package controller

import (
	"context"
	"errors"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	v1alpha1 "github.com/shipstream/bloodraven/api/v1alpha1"
	"github.com/shipstream/bloodraven/internal/state"
)

// Tests of the fixes that followed the second review of the planned-failover
// DST findings (internal/dst/README.md, "Fixed in the operator"):
// the fenced source named from the durable intent, the replaced or
// regressed target in the re-proof and the promotion attempt, the
// confirmation memo, and what the terminal records say.

// opsOwnRecordGroup is fg with the run's own failover record durable: the
// confirmed tail recorded and published the promotion, and the Publishing
// write did not commit.
func opsOwnRecordGroup(f *opsFixture, pf *v1alpha1.PlannedFailoverStatus) *v1alpha1.MysqlFailoverGroup {
	at := pf.Promotion.IntentTime.Add(2 * time.Second)
	fg := opsReproveGroup(f, pf, at, "dc2")
	fg.Annotations = map[string]string{
		v1alpha1.LastFailoverAnnotation:             failoverStampFormat(at),
		v1alpha1.LastFailoverTargetAnnotation:       "dc2",
		v1alpha1.PromotionGtidExecutedAnnotation:    opsF,
		v1alpha1.PromotionGtidExecutedForAnnotation: failoverWatermarkBinding(at, "dc2"),
	}
	return fg
}

// TestReprove_ReplacedTargetNeedsTheRollingBackEvidence: a re-proof that
// finds the target read-only inside the fenced set rolls back only when
// that proves nothing was written. It does for the server the intent names
// with the whole fenced set. A target that is another server, or the
// intent's on an older copy of its data, proves it only with the evidence
// the promotion attempt needs for the same finding (the run's record is not
// durable, the source holds the fenced set under the run's fence, every
// promotable peer answers read-only); without it the run holds.
func TestReprove_ReplacedTargetNeedsTheRollingBackEvidence(t *testing.T) {
	type want struct {
		rollBack, hold v1alpha1.PlannedFailoverReason
	}
	replaced := func(f *opsFixture) { f.tgt.uuid = opsUUIDD }
	regressed := func(f *opsFixture) { f.tgt.gtid = opsUUIDA + ":1-7" }
	cases := []struct {
		name string
		prep func(f *opsFixture)
		// durable: the run's own failover record is durable.
		durable bool
		// record: another failover was recorded during the run.
		record bool
		want   want
	}{
		{name: "the intent's server with the fenced set, a peer down",
			prep: func(f *opsFixture) { f.third.down = true },
			want: want{rollBack: v1alpha1.PlannedFailoverReasonPeerUnreadable}},
		{name: "another server, a promotable peer down",
			prep: func(f *opsFixture) { replaced(f); f.third.down = true },
			want: want{hold: v1alpha1.PlannedFailoverReasonExecuteFailed}},
		{name: "an older copy of the intent's server, a promotable peer down",
			prep: func(f *opsFixture) { regressed(f); f.third.down = true },
			want: want{hold: v1alpha1.PlannedFailoverReasonExecuteFailed}},
		{name: "another server, the run's record durable, a promotable peer down", durable: true,
			prep: func(f *opsFixture) { replaced(f); f.third.down = true },
			want: want{hold: v1alpha1.PlannedFailoverReasonExecuteFailed}},
		{name: "another server, the source's fence gone",
			prep: func(f *opsFixture) { replaced(f); f.src.sro = false },
			want: want{hold: v1alpha1.PlannedFailoverReasonExecuteFailed}},
		{name: "another server, a peer writable",
			prep: func(f *opsFixture) { replaced(f); f.third.ro, f.third.sro = false, false },
			want: want{hold: v1alpha1.PlannedFailoverReasonExecuteFailed}},
		{name: "another server, a failover recorded during the run, the evidence complete", record: true,
			prep: replaced,
			want: want{rollBack: v1alpha1.PlannedFailoverReasonAuthorityChanged}},
		{name: "another server, a failover recorded during the run, a peer down", record: true,
			prep: func(f *opsFixture) { replaced(f); f.third.down = true },
			want: want{hold: v1alpha1.PlannedFailoverReasonAuthorityChanged}},
		{name: "an older copy, a failover recorded during the run, the evidence complete", record: true,
			prep: regressed,
			want: want{rollBack: v1alpha1.PlannedFailoverReasonAuthorityChanged}},
		// The process that knows it sent no clear needs no such evidence.
		{name: "another server, this process sent no clear, a peer down",
			prep: func(f *opsFixture) { replaced(f); f.third.down = true; f.topo.startPromoteMemo(f.ref.RunID) },
			want: want{rollBack: v1alpha1.PlannedFailoverReasonPeerUnreadable}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			f := newOpsFixture(t, state.SiteRoleDROnly)
			f.hold(true)
			f.fenced()
			f.tgt.repl, f.tgt.sro = nil, false
			tc.prep(f)
			pf := opsRun(v1alpha1.PlannedFailoverPhasePromoting)
			fg := opsGroup(pf)
			switch {
			case tc.durable:
				fg = opsOwnRecordGroup(f, pf)
				if !plannedOwnRecordDurable(fg, pf, f.clk.Now()) {
					t.Fatal("setup: the run's own record is not durable")
				}
			case tc.record:
				fg = opsReproveGroup(f, pf, f.clk.Now().Add(-10*time.Second), "dc1")
			}
			res, err := f.topo.Reprove(context.Background(), f.ref, fg)
			if err != nil {
				t.Fatal(err)
			}
			var got want
			if res.RollBack != nil {
				got.rollBack = *res.RollBack
			}
			if res.Hold != nil {
				got.hold = *res.Hold
			}
			if got != tc.want || res.OK || res.HandOff != nil {
				t.Fatalf("Reprove = OK %v, RollBack %q, Hold %q, HandOff %v; want RollBack %q, Hold %q",
					res.OK, got.rollBack, got.hold, res.HandOff != nil, tc.want.rollBack, tc.want.hold)
			}
			if tc.want.hold != "" {
				if got := f.tgt.statements(); len(got) != 0 {
					t.Fatalf("target statements %v, want none before a hold", got)
				}
			}
		})
	}
}

// TestPromote_RegressedTargetTakesTheReplacedTargetRuling: a promotion
// attempt by a process without the memo finds the intent's server
// read-only, without a channel, lacking part of the fenced set and holding
// nothing outside it: the target came back on an older copy of its data.
// What a clear let onto the promoted copy is as gone as on a replaced pod,
// so the same ruling decides: roll back with its evidence, hold without.
func TestPromote_RegressedTargetTakesTheReplacedTargetRuling(t *testing.T) {
	cases := []struct {
		name     string
		prep     func(f *opsFixture)
		rollback bool
	}{
		{"the evidence complete", func(*opsFixture) {}, true},
		{"the source down", func(f *opsFixture) { f.src.down = true }, false},
		{"a promotable peer down", func(f *opsFixture) { f.third.down = true }, false},
		{"a peer writable", func(f *opsFixture) { f.third.ro, f.third.sro = false, false }, false},
		{"the source short of the fenced set too", func(f *opsFixture) { f.src.gtid = opsUUIDA + ":1-9" }, false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			f := newOpsFixture(t, state.SiteRoleDROnly)
			f.hold(false)
			f.fenced()
			f.tgt.repl, f.tgt.gtid = nil, opsUUIDA+":1-7"
			tc.prep(f)
			out, err := f.topo.Promote(context.Background(), f.ref, opsRun(v1alpha1.PlannedFailoverPhasePromoting))
			if err != nil || out.Kind != PromoteInconsistent || out.Reason != v1alpha1.PlannedFailoverReasonExecuteFailed {
				t.Fatalf("Promote = %+v, %v; want Inconsistent{ExecuteFailed}", out, err)
			}
			if out.RollbackIfUnrecorded != tc.rollback {
				t.Fatalf("RollbackIfUnrecorded = %v, want %v", out.RollbackIfUnrecorded, tc.rollback)
			}
			if got := f.tgt.statements(); len(got) != 0 {
				t.Fatalf("target statements %v, want none", got)
			}
		})
	}
	// The process whose memo proves no clear rolls back without the ruling,
	// as before; a writable target short of the fenced set still holds.
	t.Run("this process sent no clear", func(t *testing.T) {
		f := newOpsFixture(t, state.SiteRoleDROnly)
		f.hold(false)
		f.fenced()
		f.tgt.repl, f.tgt.gtid = nil, opsUUIDA+":1-7"
		f.topo.startPromoteMemo(f.ref.RunID)
		out, err := f.topo.Promote(context.Background(), f.ref, opsRun(v1alpha1.PlannedFailoverPhasePromoting))
		if err != nil || out.Kind != PromoteAborted || out.Reason != v1alpha1.PlannedFailoverReasonTargetChanged {
			t.Fatalf("Promote = %+v, %v; want Aborted{TargetChanged}", out, err)
		}
	})
	t.Run("writable and short of the fenced set", func(t *testing.T) {
		f := newOpsFixture(t, state.SiteRoleDROnly)
		f.hold(false)
		f.fenced()
		f.tgt.repl, f.tgt.gtid, f.tgt.ro, f.tgt.sro = nil, opsUUIDA+":1-7", false, false
		out, err := f.topo.Promote(context.Background(), f.ref, opsRun(v1alpha1.PlannedFailoverPhasePromoting))
		if err != nil || out.Kind != PromoteInconsistent || out.RollbackIfUnrecorded {
			t.Fatalf("Promote = %+v, %v; want Inconsistent without the rollback ruling", out, err)
		}
	})
}

// opsFailAfter makes an opsMySQL's reads fail from the nth CheckReadOnly
// on (1-based), so a site answers one probe of a pass and not the next.
type opsFailAfter struct {
	*opsMySQL
	n     int32
	reads atomic.Int32
}

func (h *opsFailAfter) CheckReadOnly(ctx context.Context) (bool, error) {
	if h.reads.Add(1) >= h.n {
		return false, errors.New("connection refused")
	}
	return h.opsMySQL.CheckReadOnly(ctx)
}

// TestReprove_WritableSourceIsRememberedForTheConfirmation: the re-proof
// reads the source writable and hands the target to the confirmation; the
// source then stops answering. The confirmation counts it as the writer the
// process saw, and holds. Without the re-proof's note the source would be a
// site the process never read, and the run would only stay unconfirmed.
func TestReprove_WritableSourceIsRememberedForTheConfirmation(t *testing.T) {
	f := newOpsFixture(t, "")
	f.hold(true)
	// The source's fence is gone and it is writable; the target was made
	// writable by an earlier process and holds its own transactions.
	f.tgt.repl, f.tgt.ro, f.tgt.sro = nil, false, false
	f.tgt.gtid = opsF + "," + opsUUIDB + ":1"
	// The re-proof's identity read is the source's first read_only read;
	// the confirmation's is the second.
	f.tm.sites[0].mysql = &opsFailAfter{opsMySQL: f.src, n: 2}
	res, err := f.topo.Reprove(context.Background(), f.ref, opsGroup(opsRun(v1alpha1.PlannedFailoverPhasePromoting)))
	if err != nil || res.HandOff == nil {
		t.Fatalf("Reprove = %+v, %v; want a hand-off to the confirmation", res, err)
	}
	if res.HandOff.Kind != PromoteInconsistent || res.HandOff.Reason != v1alpha1.PlannedFailoverReasonForeignWritable ||
		len(res.HandOff.Sites) != 1 || res.HandOff.Sites[0] != "dc1" {
		t.Fatalf("hand-off = %+v; want Inconsistent{ForeignWritable} naming dc1, the writer the re-proof saw", *res.HandOff)
	}
}

// TestConfirm_ReadOnlySourceIsRememberedForALaterConfirmation: a process
// that did not pass the gate itself reads the source read-only at a first
// confirmation that stays unconfirmed (a promotable peer it never read does
// not answer). At the next one the peer answers and the source does not:
// the source's earlier answer makes it the documented residual, and the
// promotion is confirmed with the warning. Without the confirmation's note
// the source would be a site the process never read.
func TestConfirm_ReadOnlySourceIsRememberedForALaterConfirmation(t *testing.T) {
	f := newOpsFixture(t, state.SiteRoleDROnly)
	f.hold(false)
	f.fenced()
	f.tgt.repl, f.tgt.ro, f.tgt.sro = nil, false, false // made writable by an earlier process
	f.third.down = true
	pf := opsRun(v1alpha1.PlannedFailoverPhasePromoting)
	out, err := f.topo.Promote(context.Background(), f.ref, pf)
	if err != nil || out.Kind != PromoteUnconfirmed || len(out.Sites) != 1 || out.Sites[0] != "dc3" {
		t.Fatalf("first Promote = %+v, %v; want Unconfirmed over dc3", out, err)
	}
	f.third.set(func(m *opsMySQL) { m.down = false })
	f.src.set(func(m *opsMySQL) { m.down = true })
	out, err = f.topo.Promote(context.Background(), f.ref, pf)
	if err != nil || out.Kind != PromoteConfirmed {
		t.Fatalf("second Promote = %+v, %v; want Confirmed on the source's earlier read-only answer", out, err)
	}
	found := false
	for _, w := range out.Warnings {
		found = found || w == v1alpha1.PlannedFailoverWarningUnreachablePeers
	}
	if !found {
		t.Fatalf("warnings = %v, want UnreachablePeers", out.Warnings)
	}
}

// TestPF_WritableTargetWithChannelRecordSaysWhatIsKnown: a process without
// the promotion memo that finds the target writable with its channel ends
// SourceLeftFenced in both readings, but only the process whose memo proves
// no clear may say that nothing was written to the target.
func TestPF_WritableTargetWithChannelRecordSaysWhatIsKnown(t *testing.T) {
	for _, tc := range []struct {
		name    string
		memo    bool
		unknown bool
	}{
		{"another process", false, true},
		{"this process, no clear sent", true, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			f := newOpsFixture(t, "")
			f.hold(false)
			f.fenced()
			f.tgt.ro, f.tgt.sro = false, false
			if tc.memo {
				f.topo.startPromoteMemo(f.ref.RunID)
			}
			out, err := f.topo.Promote(context.Background(), f.ref, opsRun(v1alpha1.PlannedFailoverPhasePromoting))
			if err != nil || out.Kind != PromoteAborted || out.Terminal != v1alpha1.PlannedFailoverOutcomeSourceLeftFenced {
				t.Fatalf("Promote = %+v, %v", out, err)
			}
			if out.ClearsUnknown != tc.unknown {
				t.Fatalf("ClearsUnknown = %v, want %v", out.ClearsUnknown, tc.unknown)
			}
		})
	}
	for _, tc := range []struct {
		name    string
		unknown bool
		want    string
		not     string
	}{
		{"unknown", true, "an earlier operator process of the run did", "nothing was written to the target"},
		{"known", false, "nothing was written to the target", "an earlier operator process"},
	} {
		t.Run("record/"+tc.name, func(t *testing.T) {
			h := pfHarnessAt(t, v1alpha1.PlannedFailoverPhasePromoting)
			h.topo.promote = func(PlannedRunRef) (PromoteOutcome, error) {
				return PromoteOutcome{Kind: PromoteAborted, Reason: v1alpha1.PlannedFailoverReasonForeignWritable,
					Terminal: v1alpha1.PlannedFailoverOutcomeSourceLeftFenced, ClearsUnknown: tc.unknown}, nil
			}
			h.step()
			pf := h.pf()
			if pf.Result == nil || pf.Result.Outcome != v1alpha1.PlannedFailoverOutcomeSourceLeftFenced {
				t.Fatalf("result = %+v", pf.Result)
			}
			if !strings.Contains(pf.Message, tc.want) || strings.Contains(pf.Message, tc.not) {
				t.Fatalf("message %q; want %q and not %q", pf.Message, tc.want, tc.not)
			}
		})
	}
}

// TestPF_FenceIntentAloneNamesTheSource: a run that ends SourceLeftFenced
// with only its fence intent durable (an earlier operator process may have
// sent the fence; its Draining record never committed) names its source for
// the re-assert, bound to the intent's server, as a restarted manager does
// from the same record. Authority that moved before the fence was applied
// names nothing.
func TestPF_FenceIntentAloneNamesTheSource(t *testing.T) {
	t.Run("the source lost before the fence was proven", func(t *testing.T) {
		h := pfHarnessAt(t, v1alpha1.PlannedFailoverPhaseFencing)
		h.topo.fence = func(PlannedRunRef) (FenceProof, error) {
			return FenceProof{Kind: FenceSourceUnreachable, Reason: v1alpha1.PlannedFailoverReasonSourceLost}, nil
		}
		h.topo.rollback = func(PlannedRunRef) (RollbackResult, error) {
			return RollbackResult{Outcome: v1alpha1.PlannedFailoverOutcomeSourceLeftFenced}, nil
		}
		h.stepUntil(40, func(fg *v1alpha1.MysqlFailoverGroup) bool { return fg.Status.PlannedFailover.Result != nil })
		pf := h.pf()
		if pf.Result == nil || pf.Result.Outcome != v1alpha1.PlannedFailoverOutcomeSourceLeftFenced ||
			pf.Result.Reason != v1alpha1.PlannedFailoverReasonSourceLost {
			t.Fatalf("result = %+v (phase %s, message %q)", pf.Result, pf.Phase, pf.Message)
		}
		if pf.Fence.State != v1alpha1.PlannedFailoverFenceIntended {
			t.Fatalf("setup: fence state %s, want only the intent", pf.Fence.State)
		}
		if got := h.topo.named; len(got) != 1 || got[0] != "source:iad" {
			t.Fatalf("named = %v, want the source", got)
		}
		if got := h.topo.namedServers; len(got) != 1 || got[0] != "uuid-iad" {
			t.Fatalf("named for servers %v, want the intent's uuid-iad", got)
		}
		if !strings.Contains(pf.Message, "may or may not have sent") {
			t.Fatalf("message %q does not say the fence is unproven", pf.Message)
		}
	})
	t.Run("authority moved before the fence was applied", func(t *testing.T) {
		h := pfHarnessAt(t, v1alpha1.PlannedFailoverPhaseFencing)
		h.topo.needsProof = true
		h.topo.reprove = func(PlannedRunRef) (ReproveResult, error) {
			return ReproveResult{RollBack: reasonPtr(v1alpha1.PlannedFailoverReasonAuthorityChanged)}, nil
		}
		h.topo.rollback = func(PlannedRunRef) (RollbackResult, error) {
			return RollbackResult{Outcome: v1alpha1.PlannedFailoverOutcomeSourceLeftFenced}, nil
		}
		h.stepUntil(40, func(fg *v1alpha1.MysqlFailoverGroup) bool { return fg.Status.PlannedFailover.Result != nil })
		pf := h.pf()
		if pf.Result == nil || pf.Result.Outcome != v1alpha1.PlannedFailoverOutcomeSourceLeftFenced ||
			pf.Result.Reason != v1alpha1.PlannedFailoverReasonAuthorityChanged {
			t.Fatalf("result = %+v (phase %s, message %q)", pf.Result, pf.Phase, pf.Message)
		}
		if got := h.topo.named; len(got) != 0 {
			t.Fatalf("named = %v, want nothing", got)
		}
	})
}

// TestPF_SourceLeftRecordSaysWhatTheRunKnowsOfItsFence: SourceLeftFenced
// assumes a fence. The record says whether the run proved one, sent one
// without reading it back, sent none, or cannot know.
func TestPF_SourceLeftRecordSaysWhatTheRunKnowsOfItsFence(t *testing.T) {
	intended := &v1alpha1.PlannedFailoverFenceRecord{State: v1alpha1.PlannedFailoverFenceIntended, SourceServerUUID: "uuid-iad"}
	applied := &v1alpha1.PlannedFailoverFenceRecord{State: v1alpha1.PlannedFailoverFenceApplied, SourceServerUUID: "uuid-iad"}
	cases := []struct {
		name  string
		fence *v1alpha1.PlannedFailoverFenceRecord
		memo  func(m *plannedRunMemo)
		want  string
	}{
		{"proven applied", applied, func(*plannedRunMemo) {}, `source "iad" left fenced`},
		{"proven by this process", intended, func(m *plannedRunMemo) { m.fence.sent = true }, `source "iad" left fenced`},
		{"sent, not read back", intended, func(m *plannedRunMemo) { m.granted, m.fence.setSent = true, true }, "never read back"},
		{"this process sent none", intended, func(m *plannedRunMemo) { m.granted = true }, "this run sent it no fence"},
		{"another process may have", intended, func(*plannedRunMemo) {}, "may or may not have sent"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			h := pfHarnessAt(t, v1alpha1.PlannedFailoverPhaseFencing)
			pf := &v1alpha1.PlannedFailoverStatus{RunID: "run-x", SourceSite: "iad", Fence: tc.fence}
			tc.memo(h.e.runMemo(pf.RunID))
			if got := h.e.sourceLeftClause(pf); !strings.Contains(got, tc.want) {
				t.Fatalf("clause %q, want %q", got, tc.want)
			}
		})
	}
}

// TestPF_RollbackOverASourceThatLostTransactionsHolds: the facade's verdict
// becomes ManualRecoveryRequired with the hold, and the record names the
// transactions the source lost.
func TestPF_RollbackOverASourceThatLostTransactionsHolds(t *testing.T) {
	h := pfHarnessAt(t, v1alpha1.PlannedFailoverPhaseRollingBack)
	h.topo.rollback = func(PlannedRunRef) (RollbackResult, error) {
		return RollbackResult{Outcome: v1alpha1.PlannedFailoverOutcomeManualRecoveryRequired, Reason: v1alpha1.PlannedFailoverReasonSourceUnfenced,
			LostGtid: "uuid-iad:9-10", Warnings: []v1alpha1.PlannedFailoverWarning{v1alpha1.PlannedFailoverWarningTargetReplicationNotRestored}}, nil
	}
	h.step()
	pf := h.pf()
	if pf.Result == nil || pf.Result.Outcome != v1alpha1.PlannedFailoverOutcomeManualRecoveryRequired ||
		pf.Result.Reason != v1alpha1.PlannedFailoverReasonSourceUnfenced {
		t.Fatalf("result = %+v (message %q)", pf.Result, pf.Message)
	}
	for _, want := range []string{"uuid-iad:9-10", "second history", v1alpha1.AcknowledgePlannedFailoverHoldAnnotation} {
		if !strings.Contains(pf.Message, want) {
			t.Fatalf("message %q lacks %q", pf.Message, want)
		}
	}
	hold := PlannedHoldFrom(h.fg())
	if hold == nil || hold.RunID != "run-1" || hold.Reason != v1alpha1.PlannedFailoverReasonSourceUnfenced {
		t.Fatalf("hold = %+v", hold)
	}
	if held, ok := h.topo.HeldHoldClaim(h.nn); !ok || held != "run-1" {
		t.Fatalf("the claim is not a hold: %q, %v", held, ok)
	}
	if got := h.topo.named; len(got) != 0 {
		t.Fatalf("named = %v, want nothing under a hold", got)
	}
}
