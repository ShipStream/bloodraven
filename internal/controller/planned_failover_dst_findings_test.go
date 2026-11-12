package controller

import (
	"context"
	"errors"
	"slices"
	"strings"
	"testing"

	v1alpha1 "github.com/shipstream/bloodraven/api/v1alpha1"
	"github.com/shipstream/bloodraven/internal/mysql"
	"github.com/shipstream/bloodraven/internal/state"
)

// Tests of the three outcome rows a 112,000-seed planned-failover DST sweep
// found wrong (internal/dst/README.md, "Fixed in the operator"):
// a writable target with its replication channel found by a process without
// a promotion memo, a target replaced after the process that decides sent a
// clear, and a confirmation beside a promotable site that does not answer.
// The shrunk trials are pinned end to end in
// test/component/dst_regression_test.go.

// opsUUIDD is a replacement pod's server_uuid.
const opsUUIDD = "dddddddd-dddd-dddd-dddd-dddddddddddd"

// memoOfAClear gives the facade the promotion memo of the process that
// wrote the Promoting intent and then sent super_read_only=OFF.
func (f *opsFixture) memoOfAClear() {
	f.topo.startPromoteMemo(f.ref.RunID)
	f.topo.notePromoteSent(f.ref.RunID, promoteMemoSROClearSent)
}

func (m *opsMySQL) set(edit func(m *opsMySQL)) {
	m.mu.Lock()
	defer m.mu.Unlock()
	edit(m)
}

// A writable target that still has its replication channel was not made
// writable by the run: the run's clears follow RESET REPLICA ALL and the
// gate's no-channel read. A process without a memo ends like the process
// whose memo proves no clear, SourceLeftFenced and no hold; only the
// process that did send a clear holds.
func TestPFFinding_WritableTargetWithChannel(t *testing.T) {
	stopped := &mysql.ReplicaStatus{SourceHost: "mysql-dc1"}
	cases := []struct {
		name string
		prep func(f *opsFixture)
		kind PromoteOutcomeKind
		term v1alpha1.PlannedFailoverOutcome
	}{
		{"another process, channel running", func(*opsFixture) {}, PromoteAborted, v1alpha1.PlannedFailoverOutcomeSourceLeftFenced},
		{"another process, channel stopped", func(f *opsFixture) { f.tgt.repl = stopped }, PromoteAborted, v1alpha1.PlannedFailoverOutcomeSourceLeftFenced},
		{"another process, the external writer's own transactions", func(f *opsFixture) { f.tgt.gtid = opsF + "," + opsUUIDB + ":1-3" },
			PromoteAborted, v1alpha1.PlannedFailoverOutcomeSourceLeftFenced},
		{"this process, no clear sent", func(f *opsFixture) { f.topo.startPromoteMemo(f.ref.RunID) }, PromoteAborted, v1alpha1.PlannedFailoverOutcomeSourceLeftFenced},
		{"this process, a clear sent", func(f *opsFixture) { f.memoOfAClear() }, PromoteInconsistent, ""},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			f := newOpsFixture(t, "")
			f.hold(false)
			f.fenced()
			f.tgt.ro, f.tgt.sro = false, false // an external read_only=0; the channel is still configured
			tc.prep(f)
			out, err := f.topo.Promote(context.Background(), f.ref, opsRun(v1alpha1.PlannedFailoverPhasePromoting))
			if err != nil || out.Kind != tc.kind || out.Reason != v1alpha1.PlannedFailoverReasonForeignWritable || out.Terminal != tc.term {
				t.Fatalf("Promote = %+v, %v; want kind %d ForeignWritable terminal %q", out, err, tc.kind, tc.term)
			}
			if tc.kind == PromoteAborted && out.MayHaveWritten {
				t.Fatalf("Promote = %+v; the abort says a write may have been sent", out)
			}
			if got := append(f.tgt.statements(), f.src.statements()...); len(got) != 0 {
				t.Fatalf("statements sent: %v; want none (the source stays fenced, the target is left alone)", got)
			}
		})
	}
}

// What the row above rests on: no path of the run leaves a channel on a
// target whose read-only flags it cleared. The clears are sent only after
// the gate read the target without a channel on the same session; a RESET
// REPLICA ALL that answers with an error, or a channel that appears after
// the stage was classified, sends no clear.
func TestPFFinding_ClearsNeverLeaveAChannel(t *testing.T) {
	clearsSent := func(m *opsMySQL) bool {
		return slices.ContainsFunc(m.statements(), func(s string) bool { return strings.HasSuffix(s, "=OFF") })
	}
	t.Run("a channel configured after the stage was classified", func(t *testing.T) {
		f := newOpsFixture(t, "")
		f.hold(false)
		f.fenced()
		f.tgt.repl = nil // ReplicaReset: read-only, no channel, exactly F
		f.tgt.flushFn = func(context.Context) error {
			f.tgt.set(func(m *opsMySQL) { m.repl = &mysql.ReplicaStatus{SourceHost: "mysql-dc1"} })
			return nil
		}
		out, err := f.topo.Promote(context.Background(), f.ref, opsRun(v1alpha1.PlannedFailoverPhasePromoting))
		if err != nil || out.Kind != PromoteAborted || out.Reason != v1alpha1.PlannedFailoverReasonTargetChanged {
			t.Fatalf("Promote = %+v, %v; want the gate to refuse with Aborted{TargetChanged}", out, err)
		}
		if clearsSent(f.tgt) {
			t.Fatalf("target statements %v: a clear was sent to a target with a channel", f.tgt.statements())
		}
	})
	t.Run("RESET REPLICA ALL answers with an error", func(t *testing.T) {
		f := newOpsFixture(t, "")
		f.hold(false)
		f.fenced()
		f.topo.startPromoteMemo(f.ref.RunID)
		f.tgt.failStmt = map[string]error{"reset": errors.New("lost connection")}
		clearedWithChannel := false
		f.tgt.onStmt = func(stmt string) {
			if strings.HasSuffix(stmt, "=OFF") && f.tgt.repl != nil {
				clearedWithChannel = true
			}
		}
		pf := opsRun(v1alpha1.PlannedFailoverPhasePromoting)
		out, err := f.topo.Promote(context.Background(), f.ref, pf)
		if err != nil || out.Kind != PromoteUnconfirmed || out.MayHaveWritten {
			t.Fatalf("Promote = %+v, %v; want Unconfirmed with no possible write", out, err)
		}
		if got := f.tgt.statements(); !slices.Equal(got, []string{"stop", "reset"}) {
			t.Fatalf("target statements %v; want the promotion to stop at the failed RESET", got)
		}
		// The retried pass finds the channel still there and resets it
		// before it clears anything.
		f.tgt.set(func(m *opsMySQL) { m.failStmt = nil })
		out, err = f.topo.Promote(context.Background(), f.ref, pf)
		if err != nil || out.Kind != PromoteConfirmed {
			t.Fatalf("retried Promote = %+v, %v; want Confirmed", out, err)
		}
		if got := f.tgt.statements(); !slices.Equal(got, []string{"stop", "reset", "reset", "sro=OFF", "ro=OFF"}) {
			t.Fatalf("target statements %v", got)
		}
		if clearedWithChannel {
			t.Fatal("a clear was sent while the channel was configured")
		}
	})
}

// A target found replaced (another server_uuid, read-only, nothing outside
// the fenced set) after a clear may have been sent is the DST-3 ruling's
// case in whichever process decides: with the source still fenced under the
// run's fence and holding all of F, and every other promotable site
// answering read-only, the verdict carries RollbackIfUnrecorded. Without
// that evidence the hold stands.
func TestPFFinding_ReplacedTargetAfterAClear(t *testing.T) {
	cases := []struct {
		name     string
		prep     func(f *opsFixture)
		rollback bool
	}{
		{"this process sent a clear", func(f *opsFixture) { f.memoOfAClear() }, true},
		{"another process", func(*opsFixture) {}, true},
		{"a promotable peer does not answer", func(f *opsFixture) { f.memoOfAClear(); f.third.down = true }, false},
		{"a peer is writable", func(f *opsFixture) { f.memoOfAClear(); f.third.ro, f.third.sro = false, false }, false},
		{"the source's fence is gone", func(f *opsFixture) { f.memoOfAClear(); f.src.sro = false }, false},
		{"the source does not answer", func(f *opsFixture) { f.memoOfAClear(); f.src.down = true }, false},
		{"the source lacks part of F", func(f *opsFixture) { f.memoOfAClear(); f.src.gtid = opsUUIDA + ":1-9" }, false},
		// The intent's server restarted under a new server_uuid with its data
		// (auto.cnf removed): the transaction it accepted after the clear is
		// still there, outside F.
		{"the same data under a new server_uuid", func(f *opsFixture) { f.memoOfAClear(); f.tgt.gtid = opsF + "," + opsUUIDB + ":1" }, false},
		{"the same data under a new server_uuid, another process", func(f *opsFixture) { f.tgt.gtid = opsF + "," + opsUUIDB + ":1" }, false},
		{"the replacement is writable", func(f *opsFixture) { f.memoOfAClear(); f.tgt.ro = false }, false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			f := newOpsFixture(t, state.SiteRolePrimaryCandidate)
			f.hold(false)
			f.fenced()
			// The replacement pod: a new server_uuid, mysqld's boot state, no
			// channel, a data directory that is behind F.
			f.tgt.uuid, f.tgt.repl, f.tgt.ro, f.tgt.sro, f.tgt.gtid = opsUUIDD, nil, true, false, opsUUIDA+":1-7"
			tc.prep(f)
			out, err := f.topo.Promote(context.Background(), f.ref, opsRun(v1alpha1.PlannedFailoverPhasePromoting))
			if err != nil || out.Kind != PromoteInconsistent || out.Reason != v1alpha1.PlannedFailoverReasonExecuteFailed {
				t.Fatalf("Promote = %+v, %v; want Inconsistent{ExecuteFailed}", out, err)
			}
			if out.RollbackIfUnrecorded != tc.rollback {
				t.Fatalf("RollbackIfUnrecorded = %v, want %v (%+v)", out.RollbackIfUnrecorded, tc.rollback, out)
			}
			if got := append(f.tgt.statements(), f.src.statements()...); len(got) != 0 {
				t.Fatalf("statements sent: %v; want none", got)
			}
		})
	}
	t.Run("this process sent no clear", func(t *testing.T) {
		// Unchanged: the memo proves nothing was written, so the run rolls
		// back as TargetChanged without the ruling.
		f := newOpsFixture(t, "")
		f.hold(false)
		f.fenced()
		f.topo.startPromoteMemo(f.ref.RunID)
		f.tgt.uuid, f.tgt.repl, f.tgt.gtid = opsUUIDD, nil, opsUUIDA+":1-7"
		out, err := f.topo.Promote(context.Background(), f.ref, opsRun(v1alpha1.PlannedFailoverPhasePromoting))
		if err != nil || out.Kind != PromoteAborted || out.Reason != v1alpha1.PlannedFailoverReasonTargetChanged || out.Terminal != "" {
			t.Fatalf("Promote = %+v, %v; want Aborted{TargetChanged}", out, err)
		}
	})
}

// The confirmation takes a promotable site that does not answer as a
// warning only when this process last read it read-only for the run, at the
// pre-write gate or an earlier confirm probe. A site it last read writable
// still counts as writable (Inconsistent); a site it never read leaves the
// promotion unconfirmed. A read-only reader that does not answer stays a
// warning.
func TestPFFinding_ConfirmNeedsAnAnswer(t *testing.T) {
	ctx := context.Background()
	warned := func(out PromoteOutcome) bool {
		return slices.Contains(out.Warnings, v1alpha1.PlannedFailoverWarningUnreachablePeers)
	}
	// writableTarget is the state another process finds after the clears: a
	// writable target with exactly F and no channel, and no memo.
	writableTarget := func(t *testing.T, third state.SiteRole) *opsFixture {
		f := newOpsFixture(t, third)
		f.hold(false)
		f.fenced()
		f.tgt.repl, f.tgt.ro, f.tgt.sro = nil, false, false
		return f
	}
	promote := func(f *opsFixture) PromoteOutcome {
		t.Helper()
		out, err := f.topo.Promote(ctx, f.ref, opsRun(v1alpha1.PlannedFailoverPhasePromoting))
		if err != nil {
			t.Fatalf("Promote: %v", err)
		}
		return out
	}

	t.Run("read-only at the gate, unreadable at the confirm", func(t *testing.T) {
		for _, site := range []string{"dc3", "dc1"} {
			f := newOpsFixture(t, state.SiteRolePrimaryCandidate)
			f.hold(false)
			f.fenced()
			lost := f.third
			if site == "dc1" {
				lost = f.src
			}
			// After the gate's probes, before the clears.
			f.tgt.beforeSRO = func() { lost.set(func(m *opsMySQL) { m.down = true }) }
			out := promote(f)
			if out.Kind != PromoteConfirmed || !warned(out) {
				t.Fatalf("%s lost after the gate: Promote = %+v; want Confirmed with UnreachablePeers", site, out)
			}
		}
	})
	t.Run("a process that passed no gate", func(t *testing.T) {
		cases := []struct {
			name  string
			third state.SiteRole
			down  string
			kind  PromoteOutcomeKind
			warn  bool
		}{
			{"every site answers", state.SiteRolePrimaryCandidate, "", PromoteConfirmed, false},
			{"a primary candidate does not answer", state.SiteRolePrimaryCandidate, "dc3", PromoteUnconfirmed, false},
			{"a dr-only site does not answer", state.SiteRoleDROnly, "dc3", PromoteUnconfirmed, false},
			{"the source does not answer", state.SiteRolePrimaryCandidate, "dc1", PromoteUnconfirmed, false},
			{"a reader does not answer", state.SiteRoleReadOnly, "dc3", PromoteConfirmed, true},
		}
		for _, tc := range cases {
			t.Run(tc.name, func(t *testing.T) {
				f := writableTarget(t, tc.third)
				switch tc.down {
				case "dc3":
					f.third.down = true
				case "dc1":
					f.src.down = true
				}
				out := promote(f)
				if out.Kind != tc.kind || warned(out) != tc.warn || out.Stage != PromotionStageWritable {
					t.Fatalf("Promote = %+v; want kind %d, UnreachablePeers %v", out, tc.kind, tc.warn)
				}
				if tc.kind == PromoteUnconfirmed &&
					(!slices.Equal(out.Sites, []string{tc.down}) || !out.MayHaveWritten || !out.TargetReachable ||
						out.Reason != v1alpha1.PlannedFailoverReasonPromotionUnconfirmed) {
					t.Fatalf("Promote = %+v; want Unconfirmed{PromotionUnconfirmed} naming %s, the target reachable, a possible write", out, tc.down)
				}
			})
		}
	})
	t.Run("an answer at an earlier confirmation counts", func(t *testing.T) {
		f := writableTarget(t, state.SiteRolePrimaryCandidate)
		f.src.down = true
		if out := promote(f); out.Kind != PromoteUnconfirmed || !slices.Equal(out.Sites, []string{"dc1"}) {
			t.Fatalf("source down: Promote = %+v; want Unconfirmed naming dc1", out)
		}
		// dc3 answered read-only at that confirmation; now the source
		// answers and dc3 does not.
		f.src.set(func(m *opsMySQL) { m.down = false })
		f.third.set(func(m *opsMySQL) { m.down = true })
		if out := promote(f); out.Kind != PromoteConfirmed || !warned(out) {
			t.Fatalf("dc3 down after it answered: Promote = %+v; want Confirmed with UnreachablePeers", out)
		}
	})
	t.Run("a peer last seen writable", func(t *testing.T) {
		f := writableTarget(t, state.SiteRolePrimaryCandidate)
		f.third.ro, f.third.sro = false, false
		if out := promote(f); out.Kind != PromoteInconsistent || out.Reason != v1alpha1.PlannedFailoverReasonForeignWritable {
			t.Fatalf("writable peer: Promote = %+v; want Inconsistent{ForeignWritable}", out)
		}
		// The hold did not commit; the peer stops answering.
		f.third.set(func(m *opsMySQL) { m.down = true })
		out := promote(f)
		if out.Kind != PromoteInconsistent || out.Reason != v1alpha1.PlannedFailoverReasonForeignWritable || !slices.Equal(out.Sites, []string{"dc3"}) {
			t.Fatalf("the writable peer stopped answering: Promote = %+v; want Inconsistent{ForeignWritable} naming dc3", out)
		}
		// It answers again, read-only: nothing is held against it.
		f.third.set(func(m *opsMySQL) { m.down, m.ro, m.sro = false, true, true })
		if out := promote(f); out.Kind != PromoteConfirmed || warned(out) {
			t.Fatalf("the peer answers read-only: Promote = %+v; want Confirmed", out)
		}
	})
	t.Run("a reader last seen writable", func(t *testing.T) {
		f := writableTarget(t, state.SiteRoleReadOnly)
		f.third.ro, f.third.sro = false, false
		if out := promote(f); out.Kind != PromoteInconsistent {
			t.Fatalf("writable reader: Promote = %+v; want Inconsistent", out)
		}
		f.third.set(func(m *opsMySQL) { m.down = true })
		if out := promote(f); out.Kind != PromoteInconsistent || !slices.Equal(out.Sites, []string{"dc3"}) {
			t.Fatalf("the writable reader stopped answering: Promote = %+v; want Inconsistent naming dc3", out)
		}
	})
	t.Run("the source last seen writable", func(t *testing.T) {
		f := writableTarget(t, "")
		f.src.ro, f.src.sro = false, false
		if out := promote(f); out.Kind != PromoteInconsistent {
			t.Fatalf("writable source: Promote = %+v; want Inconsistent", out)
		}
		f.src.set(func(m *opsMySQL) { m.down = true })
		if out := promote(f); out.Kind != PromoteInconsistent || !slices.Equal(out.Sites, []string{"dc1"}) {
			t.Fatalf("the writable source stopped answering: Promote = %+v; want Inconsistent naming dc1", out)
		}
	})
	t.Run("the source last seen past the fenced set", func(t *testing.T) {
		f := writableTarget(t, "")
		f.src.gtid = opsF + "," + opsUUIDA + ":11"
		if out := promote(f); out.Kind != PromoteInconsistent {
			t.Fatalf("advanced source: Promote = %+v; want Inconsistent", out)
		}
		f.src.set(func(m *opsMySQL) { m.down = true })
		if out := promote(f); out.Kind != PromoteInconsistent || !slices.Equal(out.Sites, []string{"dc1"}) {
			t.Fatalf("the advanced source stopped answering: Promote = %+v; want Inconsistent naming dc1", out)
		}
	})
	t.Run("a writable peer the re-proof saw", func(t *testing.T) {
		f := writableTarget(t, state.SiteRolePrimaryCandidate)
		f.hold(true)
		f.third.ro, f.third.sro = false, false
		fg := opsGroup(opsRun(v1alpha1.PlannedFailoverPhasePromoting))
		res, err := f.topo.Reprove(ctx, f.ref, fg)
		if err != nil || res.Hold == nil || *res.Hold != v1alpha1.PlannedFailoverReasonForeignWritable {
			t.Fatalf("Reprove beside a writable peer = %+v, %v; want Hold{ForeignWritable}", res, err)
		}
		// The hold did not commit; the peer stops answering, so the next
		// re-proof lacks evidence only and hands off to the confirmation.
		f.third.set(func(m *opsMySQL) { m.down = true })
		res, err = f.topo.Reprove(ctx, f.ref, fg)
		if err != nil || res.HandOff == nil || res.HandOff.Kind != PromoteInconsistent ||
			res.HandOff.Reason != v1alpha1.PlannedFailoverReasonForeignWritable || !slices.Equal(res.HandOff.Sites, []string{"dc3"}) {
			t.Fatalf("Reprove after the peer stopped answering = %+v (handoff %+v), %v; want HandOff Inconsistent{ForeignWritable} naming dc3",
				res, res.HandOff, err)
		}
	})
	t.Run("a read-only answer to the re-proof is not the gate's", func(t *testing.T) {
		f := writableTarget(t, state.SiteRolePrimaryCandidate)
		f.hold(true)
		fg := opsGroup(opsRun(v1alpha1.PlannedFailoverPhasePromoting))
		if res, err := f.topo.Reprove(ctx, f.ref, fg); err != nil || !res.OK {
			t.Fatalf("Reprove = %+v, %v; want OK", res, err)
		}
		f.third.set(func(m *opsMySQL) { m.down = true })
		if out := promote(f); out.Kind != PromoteUnconfirmed || !slices.Equal(out.Sites, []string{"dc3"}) {
			t.Fatalf("Promote = %+v; want Unconfirmed naming dc3", out)
		}
	})
	t.Run("the answers end with the run's memos", func(t *testing.T) {
		f := writableTarget(t, state.SiteRolePrimaryCandidate)
		if out := promote(f); out.Kind != PromoteConfirmed {
			t.Fatalf("Promote = %+v; want Confirmed", out)
		}
		if ro, known := f.topo.peerAnswerFor(f.ref.RunID, "dc3"); !known || !ro {
			t.Fatalf("dc3's answer = %v, %v; want read-only, known", ro, known)
		}
		f.topo.forgetRun(f.ref.RunID)
		if _, known := f.topo.peerAnswerFor(f.ref.RunID, "dc3"); known {
			t.Fatal("forgetRun kept the run's peer answers")
		}
	})
}
