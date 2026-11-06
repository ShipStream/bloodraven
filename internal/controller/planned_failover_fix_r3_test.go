package controller

import (
	"context"
	"errors"
	"slices"
	"strings"
	"testing"
	"time"

	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"

	v1alpha1 "github.com/shipstream/bloodraven/api/v1alpha1"
	"github.com/shipstream/bloodraven/internal/state"
)

// Fix round 3, K072: when the source is already writable, the rollback
// still requires the target back behind a verified super_read_only before
// it completes. A failed re-fence is retried (the target's channel is left
// alone); at the rollback deadline the run ends RolledBack with
// TargetSuperReadOnlyNotRestored and TargetReplicationNotRestored instead
// of a clean RolledBack.
func TestFixR3_K072_WritableSourceRollbackNeedsVerifiedTargetRefence(t *testing.T) {
	setup := func(t *testing.T, fail bool) (*opsFixture, *v1alpha1.PlannedFailoverStatus) {
		f := newOpsFixture(t, "")
		f.hold(false)
		f.src.mu.Lock()
		f.src.ro, f.src.sro = false, false // an earlier unfence whose readback was lost
		f.src.mu.Unlock()
		f.tgt.repl, f.tgt.ro, f.tgt.sro = nil, true, false
		if fail {
			f.tgt.failStmt = map[string]error{"sro=ON": errors.New("lock wait timeout")}
		}
		return f, opsUltraRollbackRun(f, v1alpha1.PlannedFailoverReasonPeerUnreadable, false)
	}
	t.Run("re-fence fails", func(t *testing.T) {
		f, pf := setup(t, true)
		res, err := f.topo.Rollback(context.Background(), f.ref, pf)
		if err != nil || res.Outcome != "" {
			t.Fatalf("Rollback = %+v, %v; want a retry while the target reads super_read_only=OFF", res, err)
		}
		if got := f.tgt.statements(); slices.ContainsFunc(got, func(s string) bool { return s != "sro=ON" }) {
			t.Fatalf("target statements = %v; want only re-fence attempts (no channel restore)", got)
		}
		f.clk.Advance(plannedFailoverRollbackDeadline - plannedRequeuePoll)
		res, err = f.topo.Rollback(context.Background(), f.ref, pf)
		if err != nil || res.Outcome != v1alpha1.PlannedFailoverOutcomeRolledBack ||
			!slices.Contains(res.Warnings, v1alpha1.PlannedFailoverWarningTargetSuperReadOnlyNotRestored) ||
			!slices.Contains(res.Warnings, v1alpha1.PlannedFailoverWarningTargetReplicationNotRestored) {
			t.Fatalf("at the deadline: Rollback = %+v, %v; want RolledBack with TargetSuperReadOnlyNotRestored", res, err)
		}
		if got := f.tgt.statements(); slices.ContainsFunc(got, func(s string) bool { return strings.HasPrefix(s, "change:") || s == "start" }) {
			t.Fatalf("the target's channel was restored with super_read_only=OFF: %v", got)
		}
	})
	t.Run("re-fence succeeds", func(t *testing.T) {
		f, pf := setup(t, false)
		res, err := f.topo.Rollback(context.Background(), f.ref, pf)
		if err != nil || res.Outcome != v1alpha1.PlannedFailoverOutcomeRolledBack ||
			slices.Contains(res.Warnings, v1alpha1.PlannedFailoverWarningTargetSuperReadOnlyNotRestored) {
			t.Fatalf("Rollback = %+v, %v; want a clean RolledBack", res, err)
		}
		if got := f.tgt.statements(); len(got) == 0 || got[0] != "sro=ON" {
			t.Fatalf("target statements = %v; want the re-fence first", got)
		}
	})
}

// Fix round 3, I001: the target's role is re-checked against the manager's
// current configuration (a role change replaces the manager, and the claim
// it shares is not re-proven) before any promotion statement and again at
// the pre-write gate, immediately before super_read_only=OFF. A target that
// is no longer a primary candidate is not made writable: the run rolls
// back with TargetNotPromotable.
func TestFixR3_I001_TargetRoleRecheckedBeforeTheWrite(t *testing.T) {
	setRole := func(f *opsFixture, role state.SiteRole) {
		f.tm.mu.Lock()
		defer f.tm.mu.Unlock()
		f.tm.getSite("dc2").role = role
	}
	t.Run("before any statement", func(t *testing.T) {
		f := newOpsFixture(t, "")
		f.hold(false)
		f.fenced()
		setRole(f, state.SiteRoleReadOnly)
		out, err := f.topo.Promote(context.Background(), f.ref, opsRun(v1alpha1.PlannedFailoverPhasePromoting))
		if err != nil || out.Kind != PromoteAborted || out.Reason != v1alpha1.PlannedFailoverReasonTargetNotPromotable || out.Terminal != "" {
			t.Fatalf("Promote = %+v, %v; want Aborted{TargetNotPromotable} into RollingBack", out, err)
		}
		if got := f.tgt.statements(); len(got) != 0 {
			t.Fatalf("target statements = %v; want none", got)
		}
	})
	t.Run("at the pre-write gate", func(t *testing.T) {
		f := newOpsFixture(t, "")
		f.hold(false)
		f.fenced()
		SetSimHooksForSim(f.topo, SimHooks{Yield: func(point string) {
			if point == YieldAfterStmtForSim+":dc2:RESET REPLICA ALL" {
				setRole(f, state.SiteRoleDROnly)
			}
		}})
		out, err := f.topo.Promote(context.Background(), f.ref, opsRun(v1alpha1.PlannedFailoverPhasePromoting))
		if err != nil || out.Kind != PromoteAborted || out.Reason != v1alpha1.PlannedFailoverReasonTargetNotPromotable {
			t.Fatalf("Promote = %+v, %v; want Aborted{TargetNotPromotable}", out, err)
		}
		if got := f.tgt.statements(); slices.Contains(got, "sro=OFF") || slices.Contains(got, "ro=OFF") {
			t.Fatalf("target statements = %v; the target was made writable", got)
		}
	})
}

// fixR3FailingReader fails every uncached read of a MysqlFailoverGroup
// (the engine's load) and passes everything else to the client.
type fixR3FailingReader struct{ client.Reader }

func (f fixR3FailingReader) Get(ctx context.Context, key client.ObjectKey, obj client.Object, opts ...client.GetOption) error {
	if _, ok := obj.(*v1alpha1.MysqlFailoverGroup); ok {
		return errors.New("apiserver: connection reset")
	}
	return f.Reader.Get(ctx, key, obj, opts...)
}

// Fix round 3, I002: a pass whose uncached load fails asks for a bounded
// retry (plannedRequeueBackoff); Reconcile keeps that interval instead of
// returning the error into the controller's exponential backoff.
func TestFixR3_I002_LoadFailureKeepsTheEngineRequeue(t *testing.T) {
	fg := plannedFailoverFG("")
	fg.Status.PlannedFailover = pfRunAt(v1alpha1.PlannedFailoverPhaseWaitingForLag, time.Now())
	r, _, topo, _ := ultraAReconciler(t, interceptor.Funcs{}, newTestSecret(), fg)
	topo.setHeld("run-1", false)
	r.APIReader = fixR3FailingReader{Reader: r.Client}
	res, err := ultraAReconcile(r, fg)
	if err != nil {
		t.Fatalf("Reconcile = %v; want the engine's bounded retry, not the controller backoff", err)
	}
	if res.RequeueAfter <= 0 || res.RequeueAfter > plannedRequeueBackoff {
		t.Fatalf("result = %+v; want a requeue within %s", res, plannedRequeueBackoff)
	}
	if d, err := plannedStepResult(context.Background(), fgNN(fg))(0, errors.New("x")); err == nil || d != 0 {
		t.Fatal("an error with no requested requeue was swallowed")
	}
}
