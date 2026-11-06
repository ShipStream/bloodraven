package controller

// Regression tests for the ultra review of PR #246 (group B, the
// planned-failover topology facade). Each test names its finding.

import (
	"bytes"
	"context"
	"errors"
	"log/slog"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	v1alpha1 "github.com/shipstream/bloodraven/api/v1alpha1"
	"github.com/shipstream/bloodraven/internal/mysql"
	"github.com/shipstream/bloodraven/internal/state"
)

// opsUltraRollbackRun is the run's RollingBack block (with a Promoting
// intent when promoting), its rollback intent at the fixture's now.
func opsUltraRollbackRun(f *opsFixture, reason v1alpha1.PlannedFailoverReason, promoting bool) *v1alpha1.PlannedFailoverStatus {
	pf := opsRun(v1alpha1.PlannedFailoverPhaseRollingBack)
	if promoting {
		pf.Promotion = opsRun(v1alpha1.PlannedFailoverPhasePromoting).Promotion
	}
	pf.Rollback = &v1alpha1.PlannedFailoverRollbackRecord{Reason: reason, IntentTime: metav1.NewTime(f.clk.Now())}
	return pf
}

// opsUltraSetSite swaps the checker of the fixture's site name.
func opsUltraSetSite(f *opsFixture, name string, c mysql.Checker) {
	f.tm.mu.Lock()
	defer f.tm.mu.Unlock()
	for i := range f.tm.sites {
		if f.tm.sites[i].name == name {
			f.tm.sites[i].mysql = c
		}
	}
}

// opsUltraLogBuffer is a goroutine-safe slog sink.
type opsUltraLogBuffer struct {
	mu  sync.Mutex
	buf bytes.Buffer
}

func (b *opsUltraLogBuffer) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.Write(p)
}

func (b *opsUltraLogBuffer) String() string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.String()
}

func opsUltraCaptureFailoverLog(f *opsFixture) *opsUltraLogBuffer {
	b := &opsUltraLogBuffer{}
	f.tm.failover.logger = slog.New(slog.NewTextHandler(b, &slog.HandlerOptions{Level: slog.LevelInfo}))
	return b
}

// K006 (a): a WasWritable target holds the run's own writes; a source that
// does not answer at the gate must not end the run SourceLeftFenced (which
// names the source and tells the operator nothing was written).
func TestPFUltra_K006_WasWritableSourceLostIsPromotedUnconfirmed(t *testing.T) {
	f := newOpsFixture(t, "")
	f.hold(false)
	f.fenced()
	f.tgt.repl, f.tgt.sro = nil, false
	f.tgt.gtid = opsF + "," + opsUUIDB + ":1-3"
	f.src.down = true
	out, err := f.topo.Promote(context.Background(), f.ref, opsRun(v1alpha1.PlannedFailoverPhasePromoting))
	if err != nil || out.Kind != PromoteAborted || out.Reason != v1alpha1.PlannedFailoverReasonSourceLost ||
		out.Terminal != v1alpha1.PlannedFailoverOutcomePromotedUnconfirmed || !out.MayHaveWritten {
		t.Fatalf("Promote = %+v, %v; want Aborted{SourceLost} -> PromotedUnconfirmed, MayHaveWritten", out, err)
	}
	if got := f.tgt.statements(); len(got) != 0 {
		t.Fatalf("the gate refused but the target got %v", got)
	}
}

// K006 (b): at NotStarted the promotion has already stopped and reset the
// target's channel when the gate runs. One failed source probe retries
// (inside the loss window); only a lost source rolls back, and the rollback
// reports the channel the promotion removed.
func TestPFUltra_K006_GateSourceProbeUsesLossWindow(t *testing.T) {
	f := newOpsFixture(t, "")
	f.hold(false)
	f.fenced()
	pf := opsRun(v1alpha1.PlannedFailoverPhasePromoting)
	f.src.down = true
	out, err := f.topo.Promote(context.Background(), f.ref, pf)
	if err != nil || out.Kind != PromoteUnconfirmed || out.MayHaveWritten {
		t.Fatalf("first failed source probe: Promote = %+v, %v; want Unconfirmed (retry) with MayHaveWritten=false", out, err)
	}
	if got, want := f.tgt.statements(), []string{"stop", "reset"}; !slices.Equal(got, want) {
		t.Fatalf("target statements = %v, want %v", got, want)
	}
	// The source answers again inside the window: the run promotes.
	f.src.down = false
	out, err = f.topo.Promote(context.Background(), f.ref, pf)
	if err != nil || out.Kind != PromoteConfirmed || out.Stage != PromotionStageReplicaReset {
		t.Fatalf("source back: Promote = %+v, %v; want Confirmed from ReplicaReset", out, err)
	}

	// A source that stays silent for the window rolls back with SourceLost.
	g := newOpsFixture(t, "")
	g.hold(false)
	g.fenced()
	g.src.down = true
	if out, err := g.topo.Promote(context.Background(), g.ref, pf); err != nil || out.Kind != PromoteUnconfirmed {
		t.Fatalf("Promote = %+v, %v; want Unconfirmed", out, err)
	}
	g.clk.Advance(plannedFailoverLossWindow)
	out, err = g.topo.Promote(context.Background(), g.ref, pf)
	if err != nil || out.Kind != PromoteAborted || out.Reason != v1alpha1.PlannedFailoverReasonSourceLost || out.Terminal != "" {
		t.Fatalf("lost source: Promote = %+v, %v; want Aborted{SourceLost} into RollingBack", out, err)
	}
	res, err := g.topo.Rollback(context.Background(), g.ref, opsUltraRollbackRun(g, v1alpha1.PlannedFailoverReasonSourceLost, true))
	if err != nil || res.Outcome != v1alpha1.PlannedFailoverOutcomeSourceLeftFenced ||
		!slices.Contains(res.Warnings, v1alpha1.PlannedFailoverWarningTargetReplicationNotRestored) {
		t.Fatalf("Rollback = %+v, %v; want SourceLeftFenced with TargetReplicationNotRestored", res, err)
	}
	if got := g.tgt.statements(); slices.ContainsFunc(got, func(s string) bool { return strings.HasPrefix(s, "change:") }) {
		t.Fatalf("the target was pointed at an unverified source: %v", got)
	}
}

// K010 / K072: a read-only target left at super_read_only=OFF is fenced
// again before its channel is restored and the source unfenced; when that
// fails the rollback retries and leaves the source fenced.
func TestPFUltra_K010_K072_RollbackRefencesTargetBeforeUnfence(t *testing.T) {
	t.Run("re-fence fails", func(t *testing.T) {
		f := newOpsFixture(t, "")
		f.hold(false)
		f.fenced()
		f.tgt.repl, f.tgt.sro = nil, false
		f.tgt.failStmt = map[string]error{"sro=ON": errors.New("lock wait timeout")}
		res, err := f.topo.Rollback(context.Background(), f.ref, opsUltraRollbackRun(f, v1alpha1.PlannedFailoverReasonPeerUnreadable, false))
		if err != nil || res.Outcome != "" {
			t.Fatalf("Rollback = %+v, %v; want a retry", res, err)
		}
		if got := f.src.statements(); len(got) != 0 {
			t.Fatalf("the source was unfenced beside a target at super_read_only=OFF: %v", got)
		}
		if got := f.tgt.statements(); !slices.Equal(got, []string{"sro=ON"}) {
			t.Fatalf("target statements = %v; want only the re-fence attempt", got)
		}
	})
	t.Run("re-fence succeeds", func(t *testing.T) {
		f := newOpsFixture(t, "")
		f.hold(false)
		f.fenced()
		f.tgt.repl, f.tgt.sro = nil, false
		res, err := f.topo.Rollback(context.Background(), f.ref, opsUltraRollbackRun(f, v1alpha1.PlannedFailoverReasonPeerUnreadable, false))
		if err != nil || res.Outcome != v1alpha1.PlannedFailoverOutcomeRolledBack {
			t.Fatalf("Rollback = %+v, %v; want RolledBack", res, err)
		}
		if got, want := f.tgt.statements(), []string{"sro=ON", "change:mysql-dc1", "start"}; !slices.Equal(got, want) {
			t.Fatalf("target statements = %v, want %v", got, want)
		}
		if !slices.Contains(f.src.statements(), "sro=OFF") {
			t.Fatalf("source statements = %v; want it unfenced", f.src.statements())
		}
	})
}

// K052: a re-proof that fails only for lack of evidence after the
// promotion deadline confirms a writable target live instead of handing
// back a synthetic Unconfirmed (which ends PromotedUnconfirmed unseen).
func TestPFUltra_K052_ReproveLackOfEvidenceConfirmsLive(t *testing.T) {
	f := newOpsFixture(t, state.SiteRolePrimaryCandidate)
	f.hold(true)
	f.fenced()
	f.third.down = true
	f.tgt.repl, f.tgt.ro, f.tgt.sro = nil, false, false
	pf := opsRun(v1alpha1.PlannedFailoverPhasePromoting)
	pf.Promotion.IntentTime = metav1.NewTime(f.clk.Now().Add(-3 * plannedFailoverPromotionDeadline))
	res, err := f.topo.Reprove(context.Background(), f.ref, opsGroup(pf))
	if err != nil || res.HandOff == nil || res.HandOff.Kind != PromoteConfirmed || res.HandOff.Stage != PromotionStageWritable ||
		!slices.Contains(res.HandOff.Warnings, v1alpha1.PlannedFailoverWarningUnreachablePeers) {
		t.Fatalf("Reprove = %+v (handoff %+v), %v; want HandOff Confirmed from Writable with UnreachablePeers", res, res.HandOff, err)
	}
	if v, _ := f.reg.slot.View(); v.NeedsReprove {
		t.Fatal("HandOff left needsReprove set")
	}
}

// K053: one failed target read at a rollback that may follow a write is
// retried inside the target-loss window (the source stays fenced), and is
// PromotedUnconfirmed only once the window, or the rollback deadline, is
// due.
func TestPFUltra_K053_RollbackTargetLossWindow(t *testing.T) {
	f := newOpsFixture(t, "")
	f.hold(false)
	f.fenced()
	f.tgt.repl = nil
	f.tgt.down = true
	pf := opsUltraRollbackRun(f, v1alpha1.PlannedFailoverReasonPeerUnreadable, true)
	res, err := f.topo.Rollback(context.Background(), f.ref, pf)
	if err != nil || res.Outcome != "" {
		t.Fatalf("first failed read: Rollback = %+v, %v; want a retry", res, err)
	}
	f.clk.Advance(plannedFailoverLossWindow / 2)
	if res, err = f.topo.Rollback(context.Background(), f.ref, pf); err != nil || res.Outcome != "" {
		t.Fatalf("inside the window: Rollback = %+v, %v; want a retry", res, err)
	}
	f.clk.Advance(plannedFailoverLossWindow / 2)
	if res, err = f.topo.Rollback(context.Background(), f.ref, pf); err != nil || res.Outcome != v1alpha1.PlannedFailoverOutcomePromotedUnconfirmed {
		t.Fatalf("window expired: Rollback = %+v, %v; want PromotedUnconfirmed", res, err)
	}
	if got := f.src.statements(); len(got) != 0 {
		t.Fatalf("the source was touched while the target was unreadable: %v", got)
	}

	// A read that recovers inside the window rolls back cleanly.
	g := newOpsFixture(t, "")
	g.hold(false)
	g.fenced()
	g.tgt.repl = nil
	g.tgt.down = true
	pf = opsUltraRollbackRun(g, v1alpha1.PlannedFailoverReasonPeerUnreadable, true)
	if res, err = g.topo.Rollback(context.Background(), g.ref, pf); err != nil || res.Outcome != "" {
		t.Fatalf("Rollback = %+v, %v; want a retry", res, err)
	}
	g.tgt.down = false
	if res, err = g.topo.Rollback(context.Background(), g.ref, pf); err != nil || res.Outcome != v1alpha1.PlannedFailoverOutcomeRolledBack {
		t.Fatalf("target back: Rollback = %+v, %v; want RolledBack", res, err)
	}

	// The window never outlasts the rollback deadline: the run must not
	// end SourceLeftFenced (unnamed target) at the deadline.
	h := newOpsFixture(t, "")
	h.hold(false)
	h.fenced()
	h.tgt.down = true
	pf = opsUltraRollbackRun(h, v1alpha1.PlannedFailoverReasonPeerUnreadable, true)
	h.clk.Advance(plannedFailoverRollbackDeadline - plannedRequeuePoll)
	if res, err = h.topo.Rollback(context.Background(), h.ref, pf); err != nil || res.Outcome != v1alpha1.PlannedFailoverOutcomePromotedUnconfirmed {
		t.Fatalf("at the rollback deadline: Rollback = %+v, %v; want PromotedUnconfirmed", res, err)
	}
}

// K054: a cross-process re-run whose target still has its channel proves
// no SRO clear ever ran: a failed STOP REPLICA is not a possible write.
func TestPFUltra_K054_ConfiguredChannelProvesNoWrite(t *testing.T) {
	f := newOpsFixture(t, "")
	f.hold(false)
	f.fenced()
	f.tgt.failStmt = map[string]error{"stop": errors.New("lock wait timeout")}
	out, err := f.topo.Promote(context.Background(), f.ref, opsRun(v1alpha1.PlannedFailoverPhasePromoting))
	if err != nil || out.Kind != PromoteUnconfirmed || out.MayHaveWritten {
		t.Fatalf("Promote = %+v, %v; want Unconfirmed with MayHaveWritten=false", out, err)
	}
	// Once this process sends the clear, the memo no longer proves it.
	f.tgt.failStmt = map[string]error{"ro=OFF": errors.New("lost reply")}
	out, err = f.topo.Promote(context.Background(), f.ref, opsRun(v1alpha1.PlannedFailoverPhasePromoting))
	if err != nil || out.Kind != PromoteUnconfirmed || !out.MayHaveWritten {
		t.Fatalf("after the SRO clear: Promote = %+v, %v; want Unconfirmed with MayHaveWritten", out, err)
	}
}

// K055: a re-run that finds the target already writable logs the
// documented completion line once confirmed; a WasWritable re-run logs
// the set it made writable (F plus its own extras), not F.
func TestPFUltra_K055_CompletionLogOnResumedPromotions(t *testing.T) {
	t.Run("Writable", func(t *testing.T) {
		f := newOpsFixture(t, "")
		logs := opsUltraCaptureFailoverLog(f)
		f.hold(false)
		f.fenced()
		f.tgt.repl, f.tgt.ro, f.tgt.sro = nil, false, false
		out, err := f.topo.Promote(context.Background(), f.ref, opsRun(v1alpha1.PlannedFailoverPhasePromoting))
		if err != nil || out.Kind != PromoteConfirmed || out.Stage != PromotionStageWritable {
			t.Fatalf("Promote = %+v, %v; want Confirmed from Writable", out, err)
		}
		if !strings.Contains(logs.String(), `msg="failover complete" promotedSite=dc2`) {
			t.Fatalf("no failover complete line for the Writable re-run; log:\n%s", logs.String())
		}
	})
	t.Run("WasWritable", func(t *testing.T) {
		f := newOpsFixture(t, "")
		logs := opsUltraCaptureFailoverLog(f)
		f.hold(false)
		f.fenced()
		f.tgt.repl, f.tgt.sro = nil, false
		f.tgt.gtid = opsF + "," + opsUUIDB + ":1-3"
		out, err := f.topo.Promote(context.Background(), f.ref, opsRun(v1alpha1.PlannedFailoverPhasePromoting))
		if err != nil || out.Kind != PromoteConfirmed {
			t.Fatalf("Promote = %+v, %v; want Confirmed", out, err)
		}
		if !strings.Contains(logs.String(), opsUUIDB+":1-3") {
			t.Fatalf("the completion line does not carry the target's own writes; log:\n%s", logs.String())
		}
	})
}

// K056: a writable source beside a target the run may have made writable
// is two writers, not a clean rollback. A writer the run never touched
// (no promotion statement could have been sent) is reported, not held.
func TestPFUltra_K056_WritableSourceBesideWritableTargetHolds(t *testing.T) {
	f := newOpsFixture(t, "")
	f.hold(false)
	f.tgt.repl, f.tgt.ro, f.tgt.sro = nil, false, false
	res, err := f.topo.Rollback(context.Background(), f.ref, opsUltraRollbackRun(f, v1alpha1.PlannedFailoverReasonPeerUnreadable, true))
	if err != nil || res.Outcome != v1alpha1.PlannedFailoverOutcomeManualRecoveryRequired || res.Reason != v1alpha1.PlannedFailoverReasonForeignWritable {
		t.Fatalf("Rollback = %+v, %v; want ManualRecoveryRequired{ForeignWritable}", res, err)
	}
	if got := f.tgt.statements(); len(got) != 0 {
		t.Fatalf("target statements = %v; want none", got)
	}

	g := newOpsFixture(t, "")
	g.hold(false)
	g.tgt.repl, g.tgt.ro, g.tgt.sro = nil, false, false
	res, err = g.topo.Rollback(context.Background(), g.ref, opsUltraRollbackRun(g, v1alpha1.PlannedFailoverReasonPeerUnreadable, false))
	if err != nil || res.Outcome != v1alpha1.PlannedFailoverOutcomeRolledBack {
		t.Fatalf("no promotion intent: Rollback = %+v, %v; want RolledBack (the writer is not the run's)", res, err)
	}
}

// opsUltraKillCounter is a source whose kill pass ends `killed` sessions
// while `remaining` stay connected (reconnecting clients).
type opsUltraKillCounter struct {
	*opsMySQL
	killed, remaining int
}

func (c *opsUltraKillCounter) KillAppConnections(ctx context.Context) (int, error) {
	if _, err := c.opsMySQL.KillAppConnections(ctx); err != nil {
		return 0, err
	}
	return c.killed, nil
}

func (c *opsUltraKillCounter) CountAppConnections(context.Context) (int, error) {
	return c.remaining, nil
}

// K070: the drain reports the sessions still connected after the kill
// pass, not the number it killed.
func TestPFUltra_K070_KillReportsRemaining(t *testing.T) {
	for _, tc := range []struct {
		killed, remaining int
	}{{killed: 4, remaining: 0}, {killed: 4, remaining: 2}, {killed: 0, remaining: 0}} {
		f := newOpsFixture(t, "")
		f.hold(false)
		f.fenced()
		opsUltraSetSite(f, "dc1", &opsUltraKillCounter{opsMySQL: f.src, killed: tc.killed, remaining: tc.remaining})
		got, err := f.topo.KillAppConnections(context.Background(), f.ref)
		if err != nil || int(got) != tc.remaining {
			t.Fatalf("killed %d, %d still connected: KillAppConnections = %d, %v; want %d", tc.killed, tc.remaining, got, err, tc.remaining)
		}
	}
}

// K073: a source whose fence was lifted, written to and set again (same
// server, super_read_only=ON) holds transactions outside F; promoting a
// target that holds only F would lose them.
func TestPFUltra_K073_SourceAdvancedPastF(t *testing.T) {
	advanced := opsF + "," + opsUUIDA + ":11-12"
	t.Run("gate", func(t *testing.T) {
		f := newOpsFixture(t, "")
		f.hold(false)
		f.fenced()
		f.tgt.repl = nil
		f.src.gtid = advanced
		out, err := f.topo.Promote(context.Background(), f.ref, opsRun(v1alpha1.PlannedFailoverPhasePromoting))
		if err != nil || out.Kind != PromoteAborted || out.Reason != v1alpha1.PlannedFailoverReasonAuthorityChanged {
			t.Fatalf("Promote = %+v, %v; want Aborted{AuthorityChanged}", out, err)
		}
		if got := f.tgt.statements(); len(got) != 0 {
			t.Fatalf("the target got %v", got)
		}
	})
	t.Run("gate after WasWritable", func(t *testing.T) {
		f := newOpsFixture(t, "")
		f.hold(false)
		f.fenced()
		f.tgt.repl, f.tgt.sro = nil, false
		f.tgt.gtid = opsF + "," + opsUUIDB + ":1"
		f.src.gtid = advanced
		out, err := f.topo.Promote(context.Background(), f.ref, opsRun(v1alpha1.PlannedFailoverPhasePromoting))
		if err != nil || out.Kind != PromoteInconsistent || out.Reason != v1alpha1.PlannedFailoverReasonForeignWritable {
			t.Fatalf("Promote = %+v, %v; want Inconsistent{ForeignWritable}", out, err)
		}
	})
	t.Run("the target's own writes replicated back are not foreign", func(t *testing.T) {
		f := newOpsFixture(t, "")
		f.hold(false)
		f.fenced()
		f.tgt.repl, f.tgt.ro, f.tgt.sro = nil, false, false
		f.tgt.gtid = opsF + "," + opsUUIDB + ":1"
		f.src.gtid = opsF + "," + opsUUIDB + ":1"
		out, err := f.topo.Promote(context.Background(), f.ref, opsRun(v1alpha1.PlannedFailoverPhasePromoting))
		if err != nil || out.Kind != PromoteConfirmed {
			t.Fatalf("Promote = %+v, %v; want Confirmed", out, err)
		}
	})
	t.Run("re-proof", func(t *testing.T) {
		f := newOpsFixture(t, "")
		f.hold(true)
		f.fenced()
		f.src.gtid = advanced
		res, err := f.topo.Reprove(context.Background(), f.ref, opsGroup(opsRun(v1alpha1.PlannedFailoverPhaseWaitingForLag)))
		if err != nil || res.RollBack == nil || *res.RollBack != v1alpha1.PlannedFailoverReasonAuthorityChanged {
			t.Fatalf("Reprove = %+v, %v; want RollBack{AuthorityChanged}", res, err)
		}
	})
}

// opsUltraPinSwap answers pooled reads itself and pins sessions to
// another server (the source address now reaches a replacement).
type opsUltraPinSwap struct {
	*opsMySQL
	pinTo *opsMySQL
}

func (s *opsUltraPinSwap) PinSession(ctx context.Context) (mysql.Session, error) {
	return s.pinTo.PinSession(ctx)
}

// K163: the rollback verifies the source's identity on its pinned session
// before pointing the target's channel at it; a writable replacement
// source is never handed the target either.
func TestPFUltra_K163_RollbackVerifiesSourceBeforeChannel(t *testing.T) {
	t.Run("replaced behind the pooled read", func(t *testing.T) {
		f := newOpsFixture(t, "")
		f.hold(false)
		f.fenced()
		f.tgt.repl = nil
		replacement := &opsMySQL{name: "dc1", uuid: "dddddddd-dddd-dddd-dddd-dddddddddddd", gtid: opsF, ro: true, sro: true}
		opsUltraSetSite(f, "dc1", &opsUltraPinSwap{opsMySQL: f.src, pinTo: replacement})
		res, err := f.topo.Rollback(context.Background(), f.ref, opsUltraRollbackRun(f, v1alpha1.PlannedFailoverReasonLagTimeout, false))
		if err != nil || res.Outcome != v1alpha1.PlannedFailoverOutcomeSourceLeftFenced ||
			!slices.Contains(res.Warnings, v1alpha1.PlannedFailoverWarningFenceNotAttributable) ||
			!slices.Contains(res.Warnings, v1alpha1.PlannedFailoverWarningTargetReplicationNotRestored) {
			t.Fatalf("Rollback = %+v, %v; want SourceLeftFenced with FenceNotAttributable and TargetReplicationNotRestored", res, err)
		}
		if got := f.tgt.statements(); len(got) != 0 {
			t.Fatalf("the target was pointed at an unverified source: %v", got)
		}
	})
	t.Run("writable replacement source", func(t *testing.T) {
		f := newOpsFixture(t, "")
		f.hold(false)
		f.tgt.repl = nil
		f.src.uuid = "dddddddd-dddd-dddd-dddd-dddddddddddd"
		res, err := f.topo.Rollback(context.Background(), f.ref, opsUltraRollbackRun(f, v1alpha1.PlannedFailoverReasonLagTimeout, false))
		if err != nil || res.Outcome != v1alpha1.PlannedFailoverOutcomeRolledBack ||
			!slices.Contains(res.Warnings, v1alpha1.PlannedFailoverWarningSourceReplaced) ||
			!slices.Contains(res.Warnings, v1alpha1.PlannedFailoverWarningTargetReplicationNotRestored) {
			t.Fatalf("Rollback = %+v, %v; want RolledBack with SourceReplaced and TargetReplicationNotRestored", res, err)
		}
		if got := f.tgt.statements(); len(got) != 0 {
			t.Fatalf("the target was pointed at a replacement source: %v", got)
		}
	})
}

// K088: a released or held run's in-process memos are dropped.
func TestPFUltra_K088_ReleaseForgetsRunMemos(t *testing.T) {
	for _, hold := range []bool{false, true} {
		f := newOpsFixture(t, "")
		f.hold(false)
		f.topo.noteFenceSent(f.ref.RunID, opsUUIDA)
		f.topo.startPromoteMemo(f.ref.RunID)
		why := ReleaseTerminal
		if hold {
			why = ReleaseToHold
		}
		if !f.topo.ReleasePlannedFailoverClaim(f.nn, f.ref.RunID, why) {
			t.Fatal("release refused")
		}
		if _, ok := f.topo.fenceMemoFor(f.ref.RunID); ok {
			t.Fatalf("hold=%v: fence memo kept after release", hold)
		}
		if _, ok := f.topo.promoteMemoFor(f.ref.RunID); ok {
			t.Fatalf("hold=%v: promotion memo kept after release", hold)
		}
	}
}

// opsUltraDNS fails every update and records when each attempt ran.
type opsUltraDNS struct {
	mu    sync.Mutex
	times []time.Time
}

func (d *opsUltraDNS) UpdateDNSRecord(context.Context, string) error {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.times = append(d.times, time.Now())
	return errors.New("provider unavailable")
}

// K103: DNS retries back off between attempts, and a budget that ran out
// before any attempt is an error, not a silent DNSFailed.
func TestPFUltra_K103_ApplyDNSBacksOff(t *testing.T) {
	f := newOpsFixture(t, "")
	f.hold(false)
	dns := &opsUltraDNS{}
	f.tm.dns = dns
	dnsState, err := f.topo.ApplyDNS(context.Background(), f.ref)
	if dnsState != v1alpha1.PlannedFailoverDNSFailed || err == nil {
		t.Fatalf("ApplyDNS = %s, %v; want Failed with the provider error", dnsState, err)
	}
	if len(dns.times) != plannedDNSAttempts {
		t.Fatalf("attempts = %d, want %d", len(dns.times), plannedDNSAttempts)
	}
	if gap := dns.times[1].Sub(dns.times[0]); gap < plannedDNSRetryBackoff {
		t.Fatalf("second attempt after %s; want a backoff of at least %s", gap, plannedDNSRetryBackoff)
	}

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	dnsState, err = f.topo.ApplyDNS(ctx, f.ref)
	if dnsState != v1alpha1.PlannedFailoverDNSFailed || err == nil {
		t.Fatalf("expired context: ApplyDNS = %s, %v; want Failed with an error", dnsState, err)
	}
}

// K159: a resumed Writable promotion moves deployment authority before
// its confirm, as the path that sends the clears does.
func TestPFUltra_K159_WritableStageAdvancesAuthority(t *testing.T) {
	f := newOpsFixture(t, state.SiteRolePrimaryCandidate)
	f.hold(false)
	f.fenced()
	f.tgt.repl, f.tgt.ro, f.tgt.sro = nil, false, false
	f.third.ro, f.third.sro = false, false // the confirm finds a foreign writer
	out, err := f.topo.Promote(context.Background(), f.ref, opsRun(v1alpha1.PlannedFailoverPhasePromoting))
	if err != nil || out.Kind != PromoteInconsistent || out.Stage != PromotionStageWritable {
		t.Fatalf("Promote = %+v, %v; want Inconsistent from Writable", out, err)
	}
	f.tm.mu.RLock()
	got := f.tm.deploymentActiveSite
	f.tm.mu.RUnlock()
	if got != "dc2" {
		t.Fatalf("deploymentActiveSite = %q, want dc2", got)
	}
}

// K162: §9 step 2 for an existing channel: a running one is left alone, a
// stopped one is started, and a failed start is reported.
func TestPFUltra_K162_RestoreExistingTargetChannel(t *testing.T) {
	cases := []struct {
		name     string
		running  bool
		startErr error
		stmts    []string
		warning  bool
	}{
		{name: "running", running: true},
		{name: "stopped", stmts: []string{"start"}},
		{name: "start fails", startErr: errors.New("replica start failed"), stmts: []string{"start"}, warning: true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			f := newOpsFixture(t, "")
			f.hold(false)
			f.fenced()
			f.tgt.repl = &mysql.ReplicaStatus{IORunning: tc.running, SQLRunning: tc.running, SourceHost: "mysql-dc1"}
			if tc.startErr != nil {
				f.tgt.failStmt = map[string]error{"start": tc.startErr}
			}
			res, err := f.topo.Rollback(context.Background(), f.ref, opsUltraRollbackRun(f, v1alpha1.PlannedFailoverReasonLagTimeout, false))
			if err != nil || res.Outcome != v1alpha1.PlannedFailoverOutcomeRolledBack {
				t.Fatalf("Rollback = %+v, %v; want RolledBack", res, err)
			}
			if got := f.tgt.statements(); !slices.Equal(got, tc.stmts) {
				t.Fatalf("target statements = %v, want %v", got, tc.stmts)
			}
			if got := slices.Contains(res.Warnings, v1alpha1.PlannedFailoverWarningTargetReplicationNotRestored); got != tc.warning {
				t.Fatalf("warnings = %v; TargetReplicationNotRestored = %v, want %v", res.Warnings, got, tc.warning)
			}
		})
	}
}
