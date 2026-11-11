package controller

import (
	"context"
	"slices"
	"strings"
	"testing"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	v1alpha1 "github.com/shipstream/bloodraven/api/v1alpha1"
	"github.com/shipstream/bloodraven/internal/mysql"
	"github.com/shipstream/bloodraven/internal/state"
)

// rollbackSourceCase is one rollback over a source and a target in a given
// state: what the run ends as and what it may send to the target.
type rollbackSourceCase struct {
	name string
	// promoting: the rollback started from Promoting (a promotion intent
	// exists), else from WaitingForLag.
	promoting bool
	reason    v1alpha1.PlannedFailoverReason
	third     state.SiteRole
	mutate    func(f *opsFixture)

	outcome   v1alpha1.PlannedFailoverOutcome
	outReason v1alpha1.PlannedFailoverReason
	lost      string
	warnings  []v1alpha1.PlannedFailoverWarning
	// target is every statement the target may receive, in order.
	target []string
	// unfenced: the source's read-only flags were cleared.
	unfenced bool
}

func runRollbackSourceCase(t *testing.T, tc rollbackSourceCase) {
	t.Helper()
	f := newOpsFixture(t, tc.third)
	f.hold(false)
	tc.mutate(f)
	pf := opsRun(v1alpha1.PlannedFailoverPhaseRollingBack)
	if tc.promoting {
		pf.Promotion = opsRun(v1alpha1.PlannedFailoverPhasePromoting).Promotion
	}
	reason := tc.reason
	if reason == "" {
		reason = v1alpha1.PlannedFailoverReasonSourceUnfenced
	}
	pf.Rollback = &v1alpha1.PlannedFailoverRollbackRecord{Reason: reason, IntentTime: metav1.NewTime(f.clk.Now())}
	res, err := f.topo.Rollback(context.Background(), f.ref, pf)
	if err != nil {
		t.Fatal(err)
	}
	if res.Outcome != tc.outcome {
		t.Fatalf("outcome = %q (reason %s, warnings %v), want %q", res.Outcome, res.Reason, res.Warnings, tc.outcome)
	}
	if tc.outReason != "" && res.Reason != tc.outReason {
		t.Fatalf("reason = %s, want %s", res.Reason, tc.outReason)
	}
	if res.LostGtid != tc.lost {
		t.Fatalf("LostGtid = %q, want %q", res.LostGtid, tc.lost)
	}
	for _, w := range tc.warnings {
		if !slices.Contains(res.Warnings, w) {
			t.Fatalf("warnings = %v, want %s among them", res.Warnings, w)
		}
	}
	if got := f.tgt.statements(); !slices.Equal(got, tc.target) {
		t.Fatalf("target statements = %v, want %v", got, tc.target)
	}
	if got := slices.Contains(f.src.statements(), "sro=OFF"); got != tc.unfenced {
		t.Fatalf("source statements = %v; unfenced = %v, want %v", f.src.statements(), got, tc.unfenced)
	}
	if !tc.unfenced && len(f.src.statements()) != 0 {
		t.Fatalf("source statements = %v, want none", f.src.statements())
	}
}

// TestRollback_NeverPointsTargetAtSourceThatLostHistory: no rollback path
// creates or starts the target's channel toward a source that answered
// without part of the fenced set, or without transactions it issued that
// the target holds. A writable source in that state ends the run with the
// hold (a second history has begun); a read-only one stays fenced.
func TestRollback_NeverPointsTargetAtSourceThatLostHistory(t *testing.T) {
	noChannel := func(f *opsFixture) { f.tgt.repl = nil }
	stopped := func(f *opsFixture) {
		f.tgt.repl = &mysql.ReplicaStatus{SourceHost: "mysql-dc1"}
	}
	hold, leftFenced, rolledBack := v1alpha1.PlannedFailoverOutcomeManualRecoveryRequired,
		v1alpha1.PlannedFailoverOutcomeSourceLeftFenced, v1alpha1.PlannedFailoverOutcomeRolledBack
	notRestored := []v1alpha1.PlannedFailoverWarning{v1alpha1.PlannedFailoverWarningTargetReplicationNotRestored}
	cases := []rollbackSourceCase{
		// The source is writable (the fixture's default) under the run's
		// server_uuid, on a data directory that lacks 9-10 of the fenced set.
		{name: "writable source lacks part of the fenced set, target without a channel",
			mutate:  func(f *opsFixture) { noChannel(f); f.src.gtid = opsUUIDA + ":1-8" },
			outcome: hold, outReason: v1alpha1.PlannedFailoverReasonSourceUnfenced, lost: opsUUIDA + ":9-10", warnings: notRestored},
		{name: "the same after a promotion intent", promoting: true,
			mutate:  func(f *opsFixture) { noChannel(f); f.src.gtid = opsUUIDA + ":1-8" },
			outcome: hold, outReason: v1alpha1.PlannedFailoverReasonSourceUnfenced, lost: opsUUIDA + ":9-10", warnings: notRestored},
		{name: "writable source lacks part of the fenced set, target channel stopped", promoting: true,
			mutate:  func(f *opsFixture) { stopped(f); f.src.gtid = opsUUIDA + ":1-8" },
			outcome: hold, lost: opsUUIDA + ":9-10", warnings: notRestored},
		{name: "writable source lacks part of the fenced set, target channel running",
			mutate:  func(f *opsFixture) { f.src.gtid = opsUUIDA + ":1-8" },
			outcome: hold, lost: opsUUIDA + ":9-10"},
		// The target lags and does not hold the lost part either: the fenced
		// set alone proves the loss.
		{name: "writable source lacks part of the fenced set that the target lacks too", reason: v1alpha1.PlannedFailoverReasonLagTimeout,
			mutate:  func(f *opsFixture) { f.src.gtid, f.tgt.gtid = opsUUIDA+":1-8", opsUUIDA+":1-6" },
			outcome: hold, lost: opsUUIDA + ":9-10"},
		// The source restarted on the older directory before the fence was
		// read: the fenced set is the short one, and the target holds what
		// the source issued and lost.
		{name: "writable source lacks its own transactions the target holds",
			mutate:  func(f *opsFixture) { noChannel(f); f.tgt.gtid = opsUUIDA + ":1-13" },
			outcome: hold, lost: opsUUIDA + ":11-13", warnings: notRestored},
		{name: "writable source lacks both",
			mutate:  func(f *opsFixture) { noChannel(f); f.src.gtid, f.tgt.gtid = opsUUIDA+":1-8", opsUUIDA+":1-13" },
			outcome: hold, lost: opsUUIDA + ":9-13", warnings: notRestored},
		{name: "a replaced writable source lacks part of the fenced set",
			mutate:  func(f *opsFixture) { noChannel(f); f.src.uuid, f.src.gtid = "replaced", opsUUIDA+":1-8" },
			outcome: hold, lost: opsUUIDA + ":9-10", warnings: []v1alpha1.PlannedFailoverWarning{v1alpha1.PlannedFailoverWarningSourceReplaced}},
		// The target at read_only=ON, super_read_only=OFF goes back behind
		// super_read_only before the hold; nothing else is sent to it.
		{name: "the hold re-fences a target left at super_read_only=OFF", promoting: true,
			mutate:  func(f *opsFixture) { noChannel(f); f.tgt.sro = false; f.src.gtid = opsUUIDA + ":1-8" },
			outcome: hold, lost: opsUUIDA + ":9-10", target: []string{"sro=ON"}},

		// A read-only source in that state is left fenced, as before; a
		// channel the promotion stopped is no longer started toward it.
		{name: "fenced source lacks part of the fenced set, target channel stopped", reason: v1alpha1.PlannedFailoverReasonLagTimeout,
			mutate:  func(f *opsFixture) { f.fenced(); stopped(f); f.src.gtid = opsUUIDA + ":1-8" },
			outcome: leftFenced, warnings: notRestored},
		{name: "fenced source lacks its own transactions, target channel stopped", reason: v1alpha1.PlannedFailoverReasonLagTimeout,
			mutate:  func(f *opsFixture) { f.fenced(); stopped(f); f.tgt.gtid = opsUUIDA + ":1-13" },
			outcome: leftFenced, warnings: notRestored},
		{name: "fenced source lacks part of the fenced set beside a writable reader", third: state.SiteRoleReadOnly,
			reason: v1alpha1.PlannedFailoverReasonLagTimeout,
			mutate: func(f *opsFixture) {
				f.fenced()
				stopped(f)
				f.src.gtid = opsUUIDA + ":1-8"
				f.third.ro, f.third.sro = false, false
			},
			outcome: leftFenced, warnings: notRestored},
		{name: "fenced source lacks part of the fenced set, source lost", reason: v1alpha1.PlannedFailoverReasonSourceLost,
			mutate:  func(f *opsFixture) { f.fenced(); stopped(f); f.src.gtid = opsUUIDA + ":1-8" },
			outcome: leftFenced, warnings: notRestored},

		// Controls: a source that holds everything is still used.
		{name: "writable source holds everything: channel created",
			mutate:  noChannel,
			outcome: rolledBack, target: []string{"change:mysql-dc1", "start"}},
		{name: "writable source holds everything: stopped channel started",
			mutate:  stopped,
			outcome: rolledBack, target: []string{"start"}},
		{name: "fenced source holds everything: channel created, source unfenced", reason: v1alpha1.PlannedFailoverReasonLagTimeout,
			mutate:  func(f *opsFixture) { f.fenced(); noChannel(f) },
			outcome: rolledBack, target: []string{"change:mysql-dc1", "start"}, unfenced: true},
		// A source that does not answer proves nothing either way: the
		// channel the promotion stopped is started again, which puts back
		// what the run found, and none is created.
		{name: "unreachable source: stopped channel started", reason: v1alpha1.PlannedFailoverReasonSourceLost,
			mutate:  func(f *opsFixture) { f.fenced(); stopped(f); f.src.down = true },
			outcome: leftFenced, target: []string{"start"}},
		{name: "unreachable source: no channel created", reason: v1alpha1.PlannedFailoverReasonSourceLost,
			mutate:  func(f *opsFixture) { f.fenced(); noChannel(f); f.src.down = true },
			outcome: leftFenced, warnings: notRestored},
		// A writable source ahead of the fenced set (its fence was lifted
		// and it took writes) holds everything the target holds.
		{name: "writable source ahead of the fenced set: channel created",
			mutate:  func(f *opsFixture) { noChannel(f); f.src.gtid = opsUUIDA + ":1-14" },
			outcome: rolledBack, target: []string{"change:mysql-dc1", "start"}},
		// A target that holds transactions of a third server is a divergent
		// target: no channel is created, and that is not the source's loss.
		{name: "divergent target: no channel created, no hold", reason: v1alpha1.PlannedFailoverReasonDivergentTarget,
			mutate:  func(f *opsFixture) { noChannel(f); f.tgt.gtid = opsF + "," + opsUUIDC + ":1" },
			outcome: rolledBack, warnings: notRestored},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) { runRollbackSourceCase(t, tc) })
	}
}

// TestRollback_SourceMadeWritableBetweenTheReads: the pooled read finds the
// source read-only and the pinned one finds it writable and short of the
// fenced set. The run holds instead of reporting the source left fenced.
func TestRollback_SourceMadeWritableBetweenTheReads(t *testing.T) {
	f := newOpsFixture(t, "")
	f.hold(false)
	f.fenced()
	f.tgt.repl = nil
	f.src.gtid = opsUUIDA + ":1-8"
	pf := opsRun(v1alpha1.PlannedFailoverPhaseRollingBack)
	pf.Rollback = &v1alpha1.PlannedFailoverRollbackRecord{Reason: v1alpha1.PlannedFailoverReasonLagTimeout, IntentTime: metav1.NewTime(f.clk.Now())}
	// The target is read twice before the source is pinned: for the
	// rollback's precondition, before the pooled read of the source, and
	// by the peer probe after it. The source turns writable at the second.
	reads := 0
	f.tm.sites[1].mysql = &opsReadHook{opsMySQL: f.tgt, onCheckReadOnly: func() {
		if reads++; reads != 2 {
			return
		}
		f.src.mu.Lock()
		f.src.ro, f.src.sro = false, false
		f.src.mu.Unlock()
	}}
	res, err := f.topo.Rollback(context.Background(), f.ref, pf)
	if err != nil {
		t.Fatal(err)
	}
	if reads < 2 {
		t.Fatalf("setup: the target was read %d times, want the peer probe after the source's pooled read", reads)
	}
	if res.Outcome != v1alpha1.PlannedFailoverOutcomeManualRecoveryRequired || res.Reason != v1alpha1.PlannedFailoverReasonSourceUnfenced ||
		res.LostGtid != opsUUIDA+":9-10" {
		t.Fatalf("Rollback = %+v, want ManualRecoveryRequired{SourceUnfenced} naming %s:9-10", res, opsUUIDA)
	}
	if got := f.tgt.statements(); len(got) != 0 {
		t.Fatalf("target statements = %v, want none", got)
	}
	if got := f.src.statements(); len(got) != 0 {
		t.Fatalf("source statements = %v, want none", got)
	}
}

// opsReadHook runs onCheckReadOnly before every CheckReadOnly of an
// opsMySQL, so a test can change another site between two reads of a pass.
type opsReadHook struct {
	*opsMySQL
	onCheckReadOnly func()
}

func (h *opsReadHook) CheckReadOnly(ctx context.Context) (bool, error) {
	h.onCheckReadOnly()
	return h.opsMySQL.CheckReadOnly(ctx)
}

func (h *opsReadHook) PinSession(context.Context) (mysql.Session, error) {
	h.opsMySQL.mu.Lock()
	defer h.opsMySQL.mu.Unlock()
	if err := h.opsMySQL.read(); err != nil {
		return nil, err
	}
	return mysql.CheckerSession(h), nil
}

// TestPF_SourceLostTransactionsEndsWithTheHold: the engine turns the
// facade's verdict into ManualRecoveryRequired with the hold, and the
// record names the lost transactions.
func TestPF_SourceLostTransactionsEndsWithTheHold(t *testing.T) {
	msg := plannedSourceLostMessage(&v1alpha1.PlannedFailoverStatus{SourceSite: "dc1", TargetSite: "dc2"}, opsUUIDA+":9-10")
	for _, want := range []string{`"dc1"`, `"dc2"`, opsUUIDA + ":9-10", "second history", "Do not reclone"} {
		if !strings.Contains(msg, want) {
			t.Fatalf("message %q lacks %q", msg, want)
		}
	}
	long := plannedSourceLostMessage(&v1alpha1.PlannedFailoverStatus{}, strings.Repeat("a", 2000))
	if len(long) > 800 {
		t.Fatalf("a long lost set is not cut: %d bytes", len(long))
	}
}
