package controller

// Regression tests for the ultra review of PR #246, group A (engine, store,
// marker, reconciler, restore, deployment leases). Each test names the
// finding it pins.

import (
	"context"
	"errors"
	"slices"
	"strings"
	"testing"
	"time"

	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/tools/record"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"

	v1alpha1 "github.com/shipstream/bloodraven/api/v1alpha1"
	"github.com/shipstream/bloodraven/internal/clock"
	"github.com/shipstream/bloodraven/internal/state"
)

// ultraASetRecord stores a status failover record {at, target} and, when
// annotations is non-nil, replaces the group's annotations.
func ultraASetRecord(t *testing.T, h *pfEngineHarness, at time.Time, target string, annotations map[string]string) {
	t.Helper()
	if annotations != nil {
		fg := h.fg()
		a := fg.GetAnnotations()
		if a == nil {
			a = map[string]string{}
		}
		for k, v := range annotations {
			a[k] = v
		}
		fg.SetAnnotations(a)
		if err := h.c.Update(context.Background(), fg); err != nil {
			t.Fatal(err)
		}
	}
	fg := h.fg()
	ts := metav1.NewTime(at)
	fg.Status.SetLastFailover(&ts, target, "")
	if err := h.c.Status().Update(context.Background(), fg); err != nil {
		t.Fatal(err)
	}
}

// ---------- K009 ----------

// K009: an invalid durable copy beside an unchanged valid winner is no
// authority change, in the confirmed tail and in the re-proof alike.
func TestUltraA_K009_InvalidCopyBesideUnchangedWinner(t *testing.T) {
	old := time.Date(2026, 11, 1, 0, 0, 0, 0, time.UTC)
	bad := map[string]string{v1alpha1.LastFailoverAnnotation: "not-a-time", v1alpha1.LastFailoverTargetAnnotation: "iad"}

	t.Run("predicate", func(t *testing.T) {
		fg := plannedFailoverFG("")
		ts := metav1.NewTime(old)
		fg.Status.SetLastFailover(&ts, "iad", "")
		fg.SetAnnotations(bad)
		pf := pfRunAt(v1alpha1.PlannedFailoverPhasePromoting, old.Add(time.Hour))
		pf.ClaimBaseline = newFailoverRecordRef(old, "iad").toAPI()
		if _, _, err := EffectiveFailoverRecord(fg, old.Add(time.Hour)); err == nil {
			t.Fatal("the malformed annotation must be a read error")
		}
		if !plannedRecordUnchanged(fg, pf, old.Add(time.Hour)) {
			t.Fatal("unchanged valid winner beside a malformed copy reported as changed")
		}
		// No valid copy left: fail closed.
		fg.Status.SetLastFailover(nil, "", "")
		if plannedRecordUnchanged(fg, pf, old.Add(time.Hour)) {
			t.Fatal("no valid copy left must fail closed")
		}
		// A removed site in one copy, the valid baseline in the other.
		fg = plannedFailoverFG("")
		fg.Status.SetLastFailover(&ts, "iad", "")
		fg.SetAnnotations(map[string]string{v1alpha1.LastFailoverAnnotation: old.Add(-time.Hour).Format(time.RFC3339), v1alpha1.LastFailoverTargetAnnotation: "gone"})
		if !plannedRecordUnchanged(fg, pf, old.Add(time.Hour)) {
			t.Fatal("a copy naming a removed site beside the unchanged winner reported as changed")
		}
	})

	t.Run("confirmed tail", func(t *testing.T) {
		h := pfHarnessAt(t, v1alpha1.PlannedFailoverPhasePromoting)
		ultraASetRecord(t, h, old, "iad", bad)
		pf := h.pf()
		pf.ClaimBaseline = newFailoverRecordRef(old, "iad").toAPI()
		h.setBlock(pf)
		h.stepUntil(10, func(fg *v1alpha1.MysqlFailoverGroup) bool {
			return fg.Status.PlannedFailover.Phase == v1alpha1.PlannedFailoverPhasePublishing ||
				plannedFailoverTerminal(fg.Status.PlannedFailover)
		})
		if r := h.pf().Result; r != nil && r.Outcome == v1alpha1.PlannedFailoverOutcomeManualRecoveryRequired {
			t.Fatalf("result = %+v; an invalid copy beside the unchanged winner must not hold", r)
		}
		if n := h.topo.count("record"); n != 1 {
			t.Fatalf("RecordPlannedPromotion ran %d times, want 1", n)
		}
	})

	t.Run("reprove", func(t *testing.T) {
		f := newOpsFixture(t, "")
		f.hold(true)
		f.fenced()
		f.tgt.repl, f.tgt.ro, f.tgt.sro = nil, false, false // promoted
		pf := opsRun(v1alpha1.PlannedFailoverPhasePromoting)
		pf.ClaimBaseline = newFailoverRecordRef(old, "dc1").toAPI()
		fg := opsGroup(pf)
		ts := metav1.NewTime(old)
		fg.Status.SetLastFailover(&ts, "dc1", "")
		fg.SetAnnotations(map[string]string{v1alpha1.LastFailoverAnnotation: "not-a-time", v1alpha1.LastFailoverTargetAnnotation: "dc1"})
		res, err := f.topo.Reprove(context.Background(), f.ref, fg)
		if err != nil || res.Hold != nil {
			t.Fatalf("Reprove = %+v, %v; an invalid copy beside the unchanged winner must not hold", res, err)
		}
	})
}

// ---------- K015 ----------

// ultraASourceLeftFenced is run-1 ended SourceLeftFenced with its own
// (applied, attributable) fence of iad.
func ultraASourceLeftFenced(now time.Time) *v1alpha1.PlannedFailoverStatus {
	pf := pfRunAt(v1alpha1.PlannedFailoverPhaseWaitingForLag, now)
	pf.Phase = v1alpha1.PlannedFailoverPhaseFailed
	pf.Revision = 5
	pf.Result = &v1alpha1.PlannedFailoverResult{Outcome: v1alpha1.PlannedFailoverOutcomeSourceLeftFenced,
		Reason: v1alpha1.PlannedFailoverReasonLagTimeout, CompletionTime: metav1.NewTime(now)}
	return pf
}

// K015: a new request never replaces the only durable evidence that lets a
// restarted manager name a fenced source for re-assert, until it healed.
func TestUltraA_K015_FencedSourceEvidenceIsNotReplaced(t *testing.T) {
	setSite := func(h *pfEngineHarness, state v1alpha1.SiteState) {
		fg := h.fg()
		for i := range fg.Status.Sites {
			if fg.Status.Sites[i].Name == "iad" {
				fg.Status.Sites[i].State = state
			}
		}
		if err := h.c.Status().Update(context.Background(), fg); err != nil {
			t.Fatal(err)
		}
	}

	h := newPFEngineHarness(t, plannedFailoverFG(""))
	h.setBlock(ultraASourceLeftFenced(h.clk.Now().Add(-time.Minute)))
	setSite(h, v1alpha1.SiteStateReadOnly)
	h.setAnnotation(v1alpha1.PlannedFailoverAnnotation, "pdx")
	h.step()
	if pf := h.pf(); pf.RunID != "run-1" || pf.Result.Outcome != v1alpha1.PlannedFailoverOutcomeSourceLeftFenced {
		t.Fatalf("block = %+v; the fenced-source evidence was replaced", pf)
	}
	if _, ok := h.fg().GetAnnotations()[v1alpha1.PlannedFailoverAnnotation]; ok {
		t.Fatal("the refused request was not consumed")
	}
	if h.sink.eventCount(v1alpha1.EventPlannedFailoverRejected) != 1 {
		t.Fatalf("events = %v, want one PlannedFailoverRejected", h.sink.events)
	}
	// The restore still finds the run to name.
	if src, ok := plannedFenceAttributableEnd(h.fg()); !ok || src.SourceSite != "iad" {
		t.Fatal("the restart can no longer name the fenced source")
	}

	// Healed (the source is writable again): the request is admitted.
	setSite(h, v1alpha1.SiteStateWritable)
	h.setAnnotation(v1alpha1.PlannedFailoverAnnotation, "pdx")
	h.step()
	if pf := h.pf(); pf.RunID == "run-1" {
		t.Fatalf("block = %+v; a healed fence must not block new requests", pf)
	}

	// Superseded by a later failover record: not blocked either.
	g := newPFEngineHarness(t, plannedFailoverFG(""))
	g.setBlock(ultraASourceLeftFenced(g.clk.Now().Add(-time.Hour)))
	setSite(g, "read-only")
	ultraASetRecord(t, g, g.clk.Now().Add(-10*time.Minute), "pdx", nil)
	g.setAnnotation(v1alpha1.PlannedFailoverAnnotation, "pdx")
	g.step()
	if pf := g.pf(); pf.RunID == "run-1" {
		t.Fatalf("block = %+v; a later failover record supersedes the fenced-source evidence", pf)
	}
}

// ---------- K051 ----------

// K051: a PromotedUnconfirmed end names its target only once the terminal
// record has committed, never while the run may still continue.
func TestUltraA_K051_UnconfirmedNamingAfterTerminalCommit(t *testing.T) {
	h := pfHarnessAt(t, v1alpha1.PlannedFailoverPhasePromoting)
	h.topo.promote = func(PlannedRunRef) (PromoteOutcome, error) {
		return PromoteOutcome{Kind: PromoteUnconfirmed, TargetReachable: true, MayHaveWritten: true}, nil
	}
	h.clk.Advance(plannedFailoverPromotionDeadline)
	h.faults.statusFaults = []pfWriteFault{pfWriteForbidden}
	h.step()
	if plannedFailoverTerminal(h.pf()) {
		t.Fatal("the terminal write was meant to fail")
	}
	if len(h.topo.named) != 0 {
		t.Fatalf("named = %v before the terminal record committed", h.topo.named)
	}
	h.step()
	if r := h.pf().Result; r == nil || r.Outcome != v1alpha1.PlannedFailoverOutcomePromotedUnconfirmed {
		t.Fatalf("result = %+v, want PromotedUnconfirmed", r)
	}
	if len(h.topo.named) != 1 || h.topo.named[0] != "target:pdx" {
		t.Fatalf("named = %v, want [target:pdx] after the commit", h.topo.named)
	}
}

// ---------- K011 / K057 / K075 / K080 (Reconcile drives the engine) ----------

// ultraAReconciler builds a reconciler over objs whose client applies
// funcs, with a scripted planned-failover topology and a fake clock.
func ultraAReconciler(t *testing.T, funcs interceptor.Funcs, objs ...client.Object) (*MysqlFailoverGroupReconciler, client.Client, *pfFakeTopology, *clock.FakeClock) {
	t.Helper()
	c := fake.NewClientBuilder().WithScheme(testScheme()).WithStatusSubresource(&v1alpha1.MysqlFailoverGroup{}).
		WithObjects(objs...).WithInterceptorFuncs(funcs).Build()
	clk := clock.NewFakeClock(time.Now())
	topo := newPFFakeTopology()
	r := &MysqlFailoverGroupReconciler{Client: c, Scheme: testScheme(), Recorder: record.NewFakeRecorder(256),
		PlannedTopology: topo, plannedClock: clk}
	return r, c, topo, clk
}

func ultraAReconcile(r *MysqlFailoverGroupReconciler, fg *v1alpha1.MysqlFailoverGroup) (ctrl.Result, error) {
	return r.Reconcile(context.Background(), ctrl.Request{NamespacedName: types.NamespacedName{Namespace: fg.Namespace, Name: fg.Name}})
}

// K011: an earlier Reconcile step that keeps failing does not starve a
// fenced run's deadlines: the engine still runs, and the reconcile
// requeues on the run's cadence instead of the controller's backoff.
func TestUltraA_K011_EarlyErrorStillDrivesFencedRun(t *testing.T) {
	fg := plannedFailoverFG("")
	pf := pfRunAt(v1alpha1.PlannedFailoverPhaseWaitingForLag, time.Now().Add(-2*time.Minute))
	fg.Status.PlannedFailover = pf
	denied := errors.New("admission webhook denied the ConfigMap")
	r, c, topo, _ := ultraAReconciler(t, interceptor.Funcs{
		Create: func(ctx context.Context, cl client.WithWatch, obj client.Object, opts ...client.CreateOption) error {
			if _, ok := obj.(*corev1.ConfigMap); ok {
				return denied
			}
			return cl.Create(ctx, obj, opts...)
		},
	}, newTestSecret(), fg)
	topo.setHeld("run-1", false)
	topo.probe = func(PlannedRunRef) (LagProbe, error) {
		return LagProbe{SourceReachable: true, TargetReachable: true}, nil
	}
	res, err := ultraAReconcile(r, fg)
	if err != nil {
		t.Fatalf("Reconcile = %v; while a run owns the sites the earlier error must not hand the requeue to the controller backoff", err)
	}
	if res.RequeueAfter <= 0 {
		t.Fatalf("result = %+v, want the run's requeue", res)
	}
	got := fetchFG(t, r, fgNN(fg)).Status.PlannedFailover
	if got.Phase != v1alpha1.PlannedFailoverPhaseRollingBack || got.Rollback == nil || got.Rollback.Reason != v1alpha1.PlannedFailoverReasonLagTimeout {
		t.Fatalf("block = %+v; the lag deadline did not fire behind the failing step", got)
	}
	_ = c
}

// K057: a non-error early return (here: the credential Secret is missing)
// still drives an Admitting run, whose admission deadline then fires,
// and merges the run's requeue into the result. No new request is
// admitted on such a return.
func TestUltraA_K057_EarlyRequeueStillDrivesPendingRun(t *testing.T) {
	fg := plannedFailoverFG("")
	pf := pfRunAt(v1alpha1.PlannedFailoverPhaseAdmitting, time.Now().Add(-3*time.Minute))
	fg.Status.PlannedFailover = pf
	r, _, _, _ := ultraAReconciler(t, interceptor.Funcs{}, fg) // no Secret: validateCredentialSecrets requeues
	if _, err := ultraAReconcile(r, fg); err != nil {
		t.Fatalf("Reconcile: %v", err)
	}
	got := fetchFG(t, r, fgNN(fg)).Status.PlannedFailover
	if got.Result == nil || got.Result.Reason != v1alpha1.PlannedFailoverReasonAdmissionTimeout {
		t.Fatalf("block = %+v; the admission deadline did not fire behind the early return", got)
	}

	// A bare new request is not admitted on an early return.
	g := plannedFailoverFG("pdx")
	r2, _, topo2, _ := ultraAReconciler(t, interceptor.Funcs{}, g)
	res, err := ultraAReconcile(r2, g)
	if err != nil || res.RequeueAfter <= 0 {
		t.Fatalf("Reconcile = %+v, %v; want the Secret's requeue", res, err)
	}
	if pf := fetchFG(t, r2, fgNN(g)).Status.PlannedFailover; pf != nil {
		t.Fatalf("block = %+v; a request must not be admitted while an earlier step returns", pf)
	}
	if topo2.count("claim") != 0 {
		t.Fatal("the claim was asked for on an early return")
	}

	// Nor when something else makes the pass run (a stale stall-since
	// annotation of a finished run).
	h := plannedFailoverFG("pdx")
	h.Annotations[v1alpha1.PlannedFailoverStallSinceAnnotation] = FormatDeadlineStart("run-0", time.Now().Add(-time.Hour))
	r3, _, _, _ := ultraAReconciler(t, interceptor.Funcs{}, h)
	if _, err := ultraAReconcile(r3, h); err != nil {
		t.Fatalf("Reconcile: %v", err)
	}
	if pf := fetchFG(t, r3, fgNN(h)).Status.PlannedFailover; pf != nil {
		t.Fatalf("block = %+v; a request must not be admitted while an earlier step returns", pf)
	}
}

// K075: a Deferred run without a claim does not hold up the rest of the
// reconcile (backups and label work keep running while it waits).
func TestUltraA_K075_DeferredRunDoesNotShortCircuitReconcile(t *testing.T) {
	SetOperatorImageDefaults("bloodraven:test", "bloodraven")
	defer SetOperatorImageDefaults("", "")
	fg := plannedFailoverFG("pdx")
	fg.Spec.Backup = fgWithBackup().Spec.Backup
	now := metav1.Now()
	at := metav1.NewTime(now.Add(-time.Minute))
	fg.Status.SetLastFailover(&at, "iad", "")
	fg.Spec.PlannedFailover = &v1alpha1.PlannedFailoverSpec{OnCooldown: v1alpha1.PlannedFailoverCooldownDefer}
	retry := metav1.NewTime(now.Add(4 * time.Minute))
	fg.Status.PlannedFailover = &v1alpha1.PlannedFailoverStatus{RunID: "run-1", Revision: 1, Phase: v1alpha1.PlannedFailoverPhaseDeferred,
		TargetSite: "pdx", StartTime: now, Settings: plannedFailoverSettingsFor(fg, PlannedFailoverRequest{Site: "pdx"}),
		Admission: &v1alpha1.PlannedFailoverAdmission{DeferReason: v1alpha1.PlannedFailoverDeferCooldownActive, RetryTime: &retry}}
	r, c, _, _ := ultraAReconciler(t, interceptor.Funcs{}, newTestSecret(), fg)
	if _, err := ultraAReconcile(r, fg); err != nil {
		t.Fatalf("Reconcile: %v", err)
	}
	if pf := fetchFG(t, r, fgNN(fg)).Status.PlannedFailover; pf == nil || pf.Phase != v1alpha1.PlannedFailoverPhaseDeferred {
		t.Fatalf("block = %+v, want Deferred", pf)
	}
	var cj batchv1.CronJob
	if err := c.Get(context.Background(), types.NamespacedName{Namespace: fg.Namespace, Name: scheduleCronJobName("lion", "nightly")}, &cj); err != nil {
		t.Fatalf("backup schedule not reconciled while a Deferred run waits: %v", err)
	}
}

// K080: the label sync after the pass reads the group as the pass left it:
// the source's role label follows the fence recorded on the same reconcile.
func TestUltraA_K080_LabelsFollowFenceOnSameReconcile(t *testing.T) {
	fg := plannedFailoverFG("")
	fg.Status.PlannedFailover = pfRunAt(v1alpha1.PlannedFailoverPhaseAdmitting, time.Now())
	pod := &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: "mysql-lion-iad-0", Namespace: fg.Namespace,
		Labels: map[string]string{labelAppName: "mysql", labelInstance: fg.Name, v1alpha1.SiteLabel: "iad", v1alpha1.RoleLabel: "primary"}}}
	r, c, _, _ := ultraAReconciler(t, interceptor.Funcs{}, newTestSecret(), fg, pod)
	if _, err := ultraAReconcile(r, fg); err != nil {
		t.Fatalf("Reconcile: %v", err)
	}
	if pf := fetchFG(t, r, fgNN(fg)).Status.PlannedFailover; pf.Phase != v1alpha1.PlannedFailoverPhaseFencing {
		t.Fatalf("block = %+v, want Fencing after the pass", pf)
	}
	var got corev1.Pod
	if err := c.Get(context.Background(), client.ObjectKeyFromObject(pod), &got); err != nil {
		t.Fatal(err)
	}
	if got.Labels[v1alpha1.RoleLabel] != "fenced" {
		t.Fatalf("source role label = %q, want fenced on the reconcile that recorded the fence", got.Labels[v1alpha1.RoleLabel])
	}
}

// ---------- K003 / K046 / K018 (in-place restore and planned failover) ----------

// ultraARestoreFixture is a lease-managed group with a full-instance
// in-place restore in Preflight, and a reconciler wired to it.
func ultraARestoreFixture(t *testing.T, mutate func(fg *v1alpha1.MysqlFailoverGroup)) (*MysqlFailoverGroupReconciler, *DeploymentLeaseManager, *v1alpha1.MysqlFailoverGroup) {
	t.Helper()
	m, fg, _ := deploymentLeaseFixture(t)
	fg.Spec.RestoreInPlace = fgInPlaceRestore("").Spec.RestoreInPlace
	if err := m.client.Update(context.Background(), fg); err != nil {
		t.Fatal(err)
	}
	fg.Status.RestoreInPlace = &v1alpha1.RestoreInPlaceStatus{Phase: v1alpha1.RestoreInPlacePreflight, Scope: &v1alpha1.RestoreScope{Type: v1alpha1.RestoreScopeFull}}
	if mutate != nil {
		mutate(fg)
	}
	if err := m.client.Status().Update(context.Background(), fg); err != nil {
		t.Fatal(err)
	}
	runner := NewTopologyManagerRunner(m.client, nil, nil, nil, testLogger())
	runner.SetDeploymentLeases(m)
	r := &MysqlFailoverGroupReconciler{Client: m.client, Recorder: record.NewFakeRecorder(100), Runner: runner}
	withFenceRunner(r, m.client, fg)
	return r, m, fg
}

func ultraAHeldStatus(fg *v1alpha1.MysqlFailoverGroup) {
	now := metav1.Now()
	fg.Status.PlannedFailover = &v1alpha1.PlannedFailoverStatus{
		RunID: "run-1", Revision: 9, Phase: v1alpha1.PlannedFailoverPhaseFailed, TargetSite: "pdx", SourceSite: "iad", StartTime: now,
		Result: &v1alpha1.PlannedFailoverResult{Outcome: v1alpha1.PlannedFailoverOutcomeManualRecoveryRequired,
			Reason: v1alpha1.PlannedFailoverReasonForeignWritable, CompletionTime: now},
	}
	setPlannedHold(&fg.Status, v1alpha1.PlannedFailoverHold{RunID: "run-1", Reason: v1alpha1.PlannedFailoverReasonForeignWritable,
		SourceSite: "iad", TargetSite: "pdx", AmbiguousSites: []string{"pdx", "iad"}, HoldTime: now}, fg.Generation)
}

// K003 + K046: a restore in Preflight beside an automation hold waits
// (no reconcile error), BeginPlanned refuses it on the durable hold alone
// (no slot hydrated), and the hold's acknowledgement is still processed.
func TestUltraA_K003_RestorePreflightWaitsOnHoldAndAckProceeds(t *testing.T) {
	r, m, fg := ultraARestoreFixture(t, ultraAHeldStatus)
	if _, err := m.BeginPlanned(context.Background(), fg, "RestoreInPlace"); err == nil {
		t.Fatal("BeginPlanned admitted a restore beside a durable hold with no slot hydrated")
	} else {
		requireDeploymentError(t, err, 423, "unstable")
	}
	d, err := r.inPlacePreflight(context.Background(), fg)
	if err != nil || d <= 0 {
		t.Fatalf("inPlacePreflight = %v, %v; a held sites refusal is a wait, not a reconcile error", d, err)
	}
	if rip := fetchFG(t, r, fgNN(fg)).Status.RestoreInPlace; rip.Phase != v1alpha1.RestoreInPlacePreflight || !strings.Contains(rip.Message, "run-1") {
		t.Fatalf("restore = %+v, want Preflight waiting on the hold of run-1", rip)
	}

	// The acknowledgement is processed by the in-flight pass the
	// restore branch runs.
	topo := newPFFakeTopology()
	topo.setHeld("run-1", true)
	r.PlannedTopology = topo
	r.plannedClock = clock.NewFakeClock(time.Now())
	cur := fetchFG(t, r, fgNN(fg))
	if cur.Annotations == nil {
		cur.Annotations = map[string]string{}
	}
	cur.Annotations[v1alpha1.AcknowledgePlannedFailoverHoldAnnotation] = "run-1"
	if err := r.Update(context.Background(), cur); err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 4; i++ {
		if _, _, err := r.reconcilePlannedFailoverInFlight(context.Background(), fetchFG(t, r, fgNN(fg))); err != nil {
			t.Fatal(err)
		}
	}
	if h := PlannedHoldFrom(fetchFG(t, r, fgNN(fg))); h != nil {
		t.Fatalf("hold = %+v; the acknowledgement was not processed", h)
	}
	if run, _ := topo.held(); run != "" {
		t.Fatalf("slot still held by %q", run)
	}
}

// K003 (b): a run that owns the sites makes the restore wait, not error.
func TestUltraA_K003_RestorePreflightWaitsOnOwningRun(t *testing.T) {
	r, _, fg := ultraARestoreFixture(t, func(fg *v1alpha1.MysqlFailoverGroup) {
		fg.Status.PlannedFailover = pfRunAt(v1alpha1.PlannedFailoverPhaseDraining, time.Now())
	})
	d, err := r.inPlacePreflight(context.Background(), fg)
	if err != nil || d <= 0 {
		t.Fatalf("inPlacePreflight = %v, %v; want a wait", d, err)
	}
	if rip := fetchFG(t, r, fgNN(fg)).Status.RestoreInPlace; rip.Phase != v1alpha1.RestoreInPlacePreflight || !strings.Contains(rip.Message, "ConcurrentOperation") {
		t.Fatalf("restore = %+v, want Preflight waiting (ConcurrentOperation)", rip)
	}
}

// K018: a Deferred planned failover (owns nothing, no deadline, refused
// while the restore is in flight) does not hold a full-instance restore
// in Preflight.
func TestUltraA_K018_RestoreProceedsPastDeferredRun(t *testing.T) {
	r, _, fg := ultraARestoreFixture(t, func(fg *v1alpha1.MysqlFailoverGroup) {
		now := metav1.Now()
		fg.Status.PlannedFailover = &v1alpha1.PlannedFailoverStatus{RunID: "run-1", Revision: 1, Phase: v1alpha1.PlannedFailoverPhaseDeferred,
			TargetSite: "pdx", StartTime: now,
			Admission: &v1alpha1.PlannedFailoverAdmission{DeferReason: v1alpha1.PlannedFailoverDeferCooldownActive}}
	})
	if _, err := r.inPlacePreflight(context.Background(), fg); err != nil {
		t.Fatal(err)
	}
	if rip := fetchFG(t, r, fgNN(fg)).Status.RestoreInPlace; rip.Phase != v1alpha1.RestoreInPlaceFencing {
		t.Fatalf("restore = %+v; it must not wait for a Deferred run", rip)
	}
	// Nor for an Admitting run without a claim (its admission is refused
	// while the restore is in flight).
	r2, _, fg2 := ultraARestoreFixture(t, func(fg *v1alpha1.MysqlFailoverGroup) {
		fg.Status.PlannedFailover = pfRunAt(v1alpha1.PlannedFailoverPhaseAdmitting, time.Now())
	})
	if _, err := r2.inPlacePreflight(context.Background(), fg2); err != nil {
		t.Fatal(err)
	}
	if rip := fetchFG(t, r2, fgNN(fg2)).Status.RestoreInPlace; rip.Phase != v1alpha1.RestoreInPlaceFencing {
		t.Fatalf("restore = %+v; it must not wait for an Admitting run without a claim", rip)
	}
	// An Admitting run holding a provisional claim is waited for.
	r3, _, fg3 := ultraARestoreFixture(t, func(fg *v1alpha1.MysqlFailoverGroup) {
		fg.Status.PlannedFailover = pfRunAt(v1alpha1.PlannedFailoverPhaseAdmitting, time.Now())
	})
	topo := newPFFakeTopology()
	topo.setHeld("run-1", false)
	r3.PlannedTopology = topo
	r3.Runner.deploymentLeases = nil // the claim alone, as without the lease manager
	if _, err := r3.inPlacePreflight(context.Background(), fg3); err != nil {
		t.Fatal(err)
	}
	if rip := fetchFG(t, r3, fgNN(fg3)).Status.RestoreInPlace; rip.Phase != v1alpha1.RestoreInPlacePreflight {
		t.Fatalf("restore = %+v; it must wait for a run holding a provisional claim", rip)
	}
}

// ---------- K066 ----------

// K066: an annotation with overrides but no site is a parse error, so the
// request is consumed with a rejection instead of building a run with an
// empty targetSite.
func TestUltraA_K066_EmptySiteWithOverridesIsRejected(t *testing.T) {
	if _, err := parsePlannedFailoverAnnotation(":maxLagWait=5s"); err == nil {
		t.Fatal("parse accepted an annotation with no site")
	}
	h := newPFEngineHarness(t, plannedFailoverFG(":maxLagWait=5s"))
	h.step()
	if pf := h.pf(); pf != nil {
		t.Fatalf("block = %+v; no run may be built for an annotation with no site", pf)
	}
	if _, ok := h.fg().GetAnnotations()[v1alpha1.PlannedFailoverAnnotation]; ok {
		t.Fatal("the malformed request was not consumed")
	}
	if h.sink.eventCount(v1alpha1.EventPlannedFailoverRejected) != 1 {
		t.Fatalf("events = %v, want one rejection", h.sink.events)
	}
}

// ---------- K012 / K013 / K151 (stall bookkeeping and the write budget) ----------

// ultraACtxStore honours ctx like a real API client: an expired ctx fails
// every call. hangWrites makes status writes block until ctx expires
// (a hung status webhook), then report Unknown.
type ultraACtxStore struct {
	PlannedFailoverStore
	hangWrites bool
}

func (s *ultraACtxStore) Load(ctx context.Context, nn types.NamespacedName) (*v1alpha1.MysqlFailoverGroup, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	return s.PlannedFailoverStore.Load(ctx, nn)
}

func (s *ultraACtxStore) Write(ctx context.Context, base *v1alpha1.MysqlFailoverGroup, next v1alpha1.PlannedFailoverStatus,
	mutate func(*v1alpha1.MysqlFailoverGroupStatus)) (WriteOutcome, error) {
	if s.hangWrites {
		select {
		case <-ctx.Done():
		case <-time.After(300 * time.Millisecond):
		}
		return WriteUnknown, errors.New("hung status webhook")
	}
	if err := ctx.Err(); err != nil {
		return WriteNotCommitted, err
	}
	return s.PlannedFailoverStore.Write(ctx, base, next, mutate)
}

func (s *ultraACtxStore) MarkDeadlineStart(ctx context.Context, base *v1alpha1.MysqlFailoverGroup, kind DeadlineKind, runID string,
	now time.Time) (time.Time, WriteOutcome, error) {
	if err := ctx.Err(); err != nil {
		return time.Time{}, WriteNotCommitted, err
	}
	return s.PlannedFailoverStore.MarkDeadlineStart(ctx, base, kind, runID, now)
}

func (s *ultraACtxStore) WriteTerminalMarker(ctx context.Context, base *v1alpha1.MysqlFailoverGroup, m TerminalMarker) (WriteOutcome, error) {
	if err := ctx.Err(); err != nil {
		return WriteNotCommitted, err
	}
	return s.PlannedFailoverStore.WriteTerminalMarker(ctx, base, m)
}

// K012: status writes that conflict on every attempt, pass after pass,
// count toward the status stall and end the run by the terminal marker.
func TestUltraA_K012_ExhaustedConflictsCountTowardStall(t *testing.T) {
	h := pfHarnessAt(t, v1alpha1.PlannedFailoverPhaseDraining)
	h.faults.statusDefault = pfWriteConflict
	for i := 0; i < 400 && h.fg().GetAnnotations()[v1alpha1.PlannedFailoverTerminalAnnotation] == ""; i++ {
		h.step()
		h.clk.Advance(time.Second)
	}
	m, ok := ParseTerminalMarker(h.fg().GetAnnotations()[v1alpha1.PlannedFailoverTerminalAnnotation])
	if !ok || m.RunID != "run-1" || m.Reason != v1alpha1.PlannedFailoverReasonStatusStalled {
		t.Fatalf("marker = %+v (%v); conflicting status writes must end the run by the stall marker", m, ok)
	}
	if run, _ := h.topo.held(); run != "" {
		t.Fatalf("claim still held by %q after the stall", run)
	}
}

// K013: a status write that used up the pass's deadline still records the
// stall start, and the stall deadline still ends the run.
func TestUltraA_K013_StallBookkeepingSurvivesExpiredPass(t *testing.T) {
	h := pfHarnessAt(t, v1alpha1.PlannedFailoverPhaseDraining)
	store := &ultraACtxStore{PlannedFailoverStore: h.e.store, hangWrites: true}
	h.e.store = store
	pass := func() {
		ctx, cancel := context.WithTimeout(context.Background(), 150*time.Millisecond)
		defer cancel()
		if _, err := h.e.Step(ctx, h.nn); err != nil {
			t.Fatalf("Step: %v", err)
		}
	}
	pass()
	if _, ok := parseDeadlineStart(h.fg().GetAnnotations()[v1alpha1.PlannedFailoverStallSinceAnnotation], "run-1"); !ok {
		t.Fatal("the stall start was not recorded after a write that used up the pass")
	}
	h.clk.Advance(plannedFailoverStatusStallDeadline + time.Second)
	pass()
	if m, ok := ParseTerminalMarker(h.fg().GetAnnotations()[v1alpha1.PlannedFailoverTerminalAnnotation]); !ok || m.RunID != "run-1" {
		t.Fatalf("marker = %+v (%v); the stall deadline did not end the run", m, ok)
	}
}

// K151: facade work that used up the pass's budget does not starve the
// terminal write.
func TestUltraA_K151_TerminalWriteKeepsItsBudget(t *testing.T) {
	h := pfHarnessAt(t, v1alpha1.PlannedFailoverPhaseRollingBack)
	h.e.store = &ultraACtxStore{PlannedFailoverStore: h.e.store}
	h.topo.rollback = func(PlannedRunRef) (RollbackResult, error) {
		time.Sleep(250 * time.Millisecond) // blackholed probes: the pass's bound is gone
		return RollbackResult{Outcome: v1alpha1.PlannedFailoverOutcomeRolledBack}, nil
	}
	ctx, cancel := context.WithTimeout(context.Background(), 150*time.Millisecond)
	defer cancel()
	if _, err := h.e.Step(ctx, h.nn); err != nil {
		t.Fatalf("Step: %v", err)
	}
	if r := h.pf().Result; r == nil || r.Outcome != v1alpha1.PlannedFailoverOutcomeRolledBack {
		t.Fatalf("result = %+v; the terminal write was starved by the facade work", r)
	}
}

// ---------- K034 ----------

// K034: a terminal record a pass computed but never saw commit does not
// override a later outcome: a promotion confirmed after a lost
// PromotedUnconfirmed write, whose Publishing write then stalls, ends
// Succeeded, not PromotedUnconfirmed.
func TestUltraA_K034_StalePendingTerminalDoesNotOverride(t *testing.T) {
	h := pfHarnessAt(t, v1alpha1.PlannedFailoverPhasePromoting)
	confirmed := false
	h.topo.promote = func(PlannedRunRef) (PromoteOutcome, error) {
		if confirmed {
			return PromoteOutcome{Kind: PromoteConfirmed, Stage: PromotionStageWritable, TargetReachable: true, MayHaveWritten: true}, nil
		}
		return PromoteOutcome{Kind: PromoteUnconfirmed, TargetReachable: true, MayHaveWritten: true}, nil
	}
	h.topo.record = func(PlannedRunRef) error {
		// The topology manager persists the run's record out of band.
		fg := h.fg()
		pf := fg.Status.PlannedFailover
		a := fg.GetAnnotations()
		if a == nil {
			a = map[string]string{}
		}
		// Encoded as production encodes it: the watermark counts only
		// beside its binding to this event (K095).
		enc, err := FailoverRecordAnnotations(FailoverRecord{LastFailover: h.e.runMemo("run-1").promote.confirmedAt,
			LastFailoverTarget: pf.TargetSite, PromotionGtidExecuted: *pf.Fence.GtidSet})
		if err != nil {
			return err
		}
		for k, v := range enc {
			a[k] = v
		}
		fg.SetAnnotations(a)
		return h.c.Update(context.Background(), fg)
	}
	h.clk.Advance(plannedFailoverPromotionDeadline)
	h.faults.statusFaults = []pfWriteFault{pfWriteForbidden}
	h.step() // finish(PromotedUnconfirmed) does not commit
	if plannedFailoverTerminal(h.pf()) {
		t.Fatal("setup: the terminal write was meant to fail")
	}
	confirmed = true
	h.faults.statusDenied = true
	for i := 0; i < 60 && h.fg().GetAnnotations()[v1alpha1.PlannedFailoverTerminalAnnotation] == ""; i++ {
		h.step()
		h.clk.Advance(time.Second)
	}
	m, ok := ParseTerminalMarker(h.fg().GetAnnotations()[v1alpha1.PlannedFailoverTerminalAnnotation])
	if !ok || m.Outcome != v1alpha1.PlannedFailoverOutcomeSucceeded {
		t.Fatalf("marker = %+v (%v); a confirmed, recorded promotion must end Succeeded", m, ok)
	}
	for _, n := range h.topo.named {
		if n == "target:pdx" {
			t.Fatal("the confirmed target was named PromotedUnconfirmed")
		}
	}
}

// ---------- K014 / J002 ----------

// K014: the confirmed tail records the run's confirmation durably before
// it releases the claim, so a restart before Publishing commits does not
// hydrate the released claim back, even after a later failover replaced
// the run's failover record (which used to end ManualRecoveryRequired).
func TestUltraA_K014_ReleasedRunSurvivesReplacedRecordAcrossRestart(t *testing.T) {
	h := pfHarnessAt(t, v1alpha1.PlannedFailoverPhasePromoting)
	h.faults.statusDenied = true
	h.step() // Promote: confirmed
	h.step() // confirmed tail: record, DNS, confirmation, release; Publishing denied
	if run, _ := h.topo.held(); run != "" {
		t.Fatalf("claim still held by %q after the confirmed tail", run)
	}
	if _, ok := parseDeadlineStart(h.fg().GetAnnotations()[v1alpha1.PlannedFailoverConfirmedAnnotation], "run-1"); !ok {
		t.Fatal("the confirmation was not recorded before the release")
	}
	// A later emergency failover replaces the run's record; then the
	// operator restarts before the Publishing write commits.
	h.faults.statusDenied = false
	ultraASetRecord(t, h, h.clk.Now().Add(5*time.Second), "iad", nil)
	h.restart()
	h.topo.mu.Lock()
	h.topo.calls = nil
	h.topo.mu.Unlock()
	if PlannedRunNeedsClaim(h.fg(), h.clk.Now().Add(6*time.Second)) {
		t.Fatal("hydration would take the claim back for a run that already released it")
	}
	h.clk.Advance(6 * time.Second)
	h.stepUntil(10, func(fg *v1alpha1.MysqlFailoverGroup) bool { return plannedFailoverTerminal(fg.Status.PlannedFailover) })
	if r := h.pf().Result; r.Outcome != v1alpha1.PlannedFailoverOutcomeSucceeded {
		t.Fatalf("result = %+v, want Succeeded (no hold)", r)
	}
	if h.topo.count("reprove") != 0 || h.topo.count("ensure") != 0 {
		t.Fatalf("calls = %v; the released run must not re-establish its claim", h.topo.calls)
	}
	if _, ok := h.fg().GetAnnotations()[v1alpha1.PlannedFailoverConfirmedAnnotation]; ok {
		t.Fatal("the confirmation annotation outlived the finished run")
	}
	if h.fg().Status.PlannedFailoverHold != nil {
		t.Fatal("an automation hold was set")
	}
}

// J002: per-run annotations a finished run left behind are removed by the
// next pass, so they do not keep a pass due on every reconcile.
func TestUltraA_J002_StaleRunAnnotationsAreCleared(t *testing.T) {
	h := newPFEngineHarness(t, plannedFailoverFG(""))
	pf := ultraASourceLeftFenced(h.clk.Now())
	pf.Result.Outcome, pf.Phase = v1alpha1.PlannedFailoverOutcomeRolledBack, v1alpha1.PlannedFailoverPhaseFailed
	h.setBlock(pf)
	h.setAnnotation(v1alpha1.PlannedFailoverManagerAbsentSinceAnnotation, FormatDeadlineStart("run-1", h.clk.Now()))
	h.setAnnotation(v1alpha1.PlannedFailoverStallSinceAnnotation, FormatDeadlineStart("run-0", h.clk.Now()))
	if !plannedFailoverPassDue(h.fg()) {
		t.Fatal("setup: the stale annotations keep a pass due")
	}
	h.step()
	a := h.fg().GetAnnotations()
	for _, k := range []string{v1alpha1.PlannedFailoverManagerAbsentSinceAnnotation, v1alpha1.PlannedFailoverStallSinceAnnotation} {
		if _, ok := a[k]; ok {
			t.Fatalf("%s survived the pass", k)
		}
	}
	if plannedFailoverPassDue(h.fg()) {
		t.Fatal("a pass is still due for a quiet group")
	}
}

// ---------- K025 ----------

// K025: a ManualRecoveryRequired hold lists the sites the failing
// operation classified, not only the run's endpoints; a hold rebuilt
// from the terminal marker in another process lists every site.
func TestUltraA_K025_HoldListsObservedSites(t *testing.T) {
	// The gate names the writable peer it refused on.
	f := newOpsFixture(t, state.SiteRoleDROnly)
	f.hold(false)
	f.fenced()
	f.tgt.repl, f.tgt.gtid = nil, opsF
	f.third.ro, f.third.sro = false, false
	out, err := f.topo.Promote(context.Background(), f.ref, opsRun(v1alpha1.PlannedFailoverPhasePromoting))
	if err != nil || !slices.Contains(out.Sites, "dc3") {
		t.Fatalf("Promote = %+v, %v; want the writable peer dc3 in Sites", out, err)
	}

	// The engine carries them into the hold.
	h := pfHarnessAt(t, v1alpha1.PlannedFailoverPhasePromoting)
	h.topo.promote = func(PlannedRunRef) (PromoteOutcome, error) {
		return PromoteOutcome{Kind: PromoteInconsistent, Reason: v1alpha1.PlannedFailoverReasonForeignWritable,
			TargetReachable: true, MayHaveWritten: true, Sites: []string{"dr"}}, nil
	}
	h.stepUntil(5, func(fg *v1alpha1.MysqlFailoverGroup) bool { return plannedFailoverTerminal(fg.Status.PlannedFailover) })
	hold := h.fg().Status.PlannedFailoverHold
	if hold == nil || !slices.Equal(hold.AmbiguousSites, []string{"pdx", "iad", "dr"}) {
		t.Fatalf("hold = %+v, want ambiguous sites [pdx iad dr]", hold)
	}

	// Rebuilt from the marker by a process that never saw the run.
	g := pfHarnessAt(t, v1alpha1.PlannedFailoverPhasePromoting)
	g.setAnnotation(v1alpha1.PlannedFailoverTerminalAnnotation, TerminalMarker{RunID: "run-1",
		Outcome: v1alpha1.PlannedFailoverOutcomeManualRecoveryRequired, Reason: v1alpha1.PlannedFailoverReasonForeignWritable}.String())
	g.stepUntil(5, func(fg *v1alpha1.MysqlFailoverGroup) bool { return plannedFailoverTerminal(fg.Status.PlannedFailover) })
	hold = g.fg().Status.PlannedFailoverHold
	if hold == nil || !slices.Equal(hold.AmbiguousSites, []string{"pdx", "iad"}) {
		t.Fatalf("hold = %+v, want every site [pdx iad] (target and source first)", hold)
	}
}

// ---------- K048 / K145 / K146 ----------

// K048: a request that arrives while the deployment admission reservation
// is refused becomes a visible, bounded Admitting wait (or a static
// rejection), never an invisible, unbounded queue. K145: the wait reason
// tells an in-place restore from an ordered update.
func TestUltraA_K048_ReservationRefusalIsABoundedWait(t *testing.T) {
	refusal := fixR1Admission{err: &DeploymentLeaseError{Status: 423, Code: "unstable", Reason: v1alpha1.UnstableReasonConcurrentOperation}}

	fg := plannedFailoverFG("pdx")
	fg.Status.UpdatePhase = "UpdatingReplica"
	h := newPFEngineHarness(t, fg)
	h.e.admission = refusal
	h.step()
	pf := h.pf()
	if pf == nil || pf.Phase != v1alpha1.PlannedFailoverPhaseAdmitting || pf.Admission == nil ||
		pf.Admission.WaitReason != v1alpha1.PlannedFailoverWaitUpdateInFlight || pf.Admission.DeadlineTime == nil {
		t.Fatalf("block = %+v, want Admitting{UpdateInFlight} with a deadline", pf)
	}
	h.clk.Advance(plannedFailoverAdmissionTimeout)
	h.step()
	if r := h.pf().Result; r == nil || r.Reason != v1alpha1.PlannedFailoverReasonAdmissionTimeout {
		t.Fatalf("result = %+v, want AdmissionTimeout once the wait outlives its deadline", r)
	}
	if _, ok := h.fg().GetAnnotations()[v1alpha1.PlannedFailoverAnnotation]; ok {
		t.Fatal("the request outlived its run")
	}

	// An in-place restore is reported as one.
	g := plannedFailoverFG("pdx")
	g.Spec.RestoreInPlace = fgInPlaceRestore("").Spec.RestoreInPlace
	g.Status.RestoreInPlace = &v1alpha1.RestoreInPlaceStatus{Phase: v1alpha1.RestoreInPlaceRestoring, Scope: &v1alpha1.RestoreScope{Type: v1alpha1.RestoreScopeFull}}
	hg := newPFEngineHarness(t, g)
	hg.e.admission = refusal
	hg.step()
	if a := hg.pf().Admission; a == nil || a.WaitReason != v1alpha1.PlannedFailoverWaitRestoreInFlight {
		t.Fatalf("admission = %+v, want RestoreInFlight", a)
	}

	// A refusal that depends on the request alone ends at once.
	u := plannedFailoverFG("nowhere")
	hu := newPFEngineHarness(t, u)
	hu.e.admission = refusal
	hu.step()
	if r := hu.pf().Result; r == nil || r.Reason != v1alpha1.PlannedFailoverReasonUnknownSite {
		t.Fatalf("result = %+v, want NotStarted{UnknownSite}", r)
	}

	// A Deferred run records the wait without losing its deferral.
	d := newPFEngineHarness(t, plannedFailoverFG("pdx"))
	d.setBlock(&v1alpha1.PlannedFailoverStatus{RunID: "run-1", Revision: 1, Phase: v1alpha1.PlannedFailoverPhaseDeferred,
		TargetSite: "pdx", StartTime: metav1.NewTime(d.clk.Now()),
		Admission: &v1alpha1.PlannedFailoverAdmission{DeferReason: v1alpha1.PlannedFailoverDeferCooldownActive}})
	d.e.admission = refusal
	d.step()
	if pf := d.pf(); pf.Phase != v1alpha1.PlannedFailoverPhaseDeferred || pf.Admission.WaitReason != v1alpha1.PlannedFailoverWaitUpdateInFlight ||
		pf.Admission.DeferReason != v1alpha1.PlannedFailoverDeferCooldownActive || !strings.Contains(pf.Message, "admission waiting") {
		t.Fatalf("block = %+v, want Deferred{CooldownActive} with the wait recorded", pf)
	}
}

// K146: a deferral that switches from a deployment hold to the cooldown is
// reported like a new one.
func TestUltraA_K146_DeferReasonSwitchIsReported(t *testing.T) {
	fg := plannedFailoverFG("pdx")
	fg.Spec.PlannedFailover = &v1alpha1.PlannedFailoverSpec{OnCooldown: v1alpha1.PlannedFailoverCooldownDefer}
	h := newPFEngineHarness(t, fg)
	ultraASetRecord(t, h, h.clk.Now().Add(-time.Minute), "iad", nil)
	h.setBlock(&v1alpha1.PlannedFailoverStatus{RunID: "run-1", Revision: 1, Phase: v1alpha1.PlannedFailoverPhaseDeferred,
		TargetSite: "pdx", StartTime: metav1.NewTime(h.clk.Now()),
		Admission: &v1alpha1.PlannedFailoverAdmission{DeferReason: v1alpha1.PlannedFailoverDeferDeploymentHold}})
	h.step()
	if a := h.pf().Admission; a == nil || a.DeferReason != v1alpha1.PlannedFailoverDeferCooldownActive {
		t.Fatalf("admission = %+v, want CooldownActive", a)
	}
	if !slices.Contains(h.sink.deferred, v1alpha1.PlannedFailoverDeferCooldownActive) ||
		h.sink.eventCount(v1alpha1.EventPlannedFailoverDeferred) != 1 {
		t.Fatalf("deferred = %v, events = %v; the switch to the cooldown deferral was not reported", h.sink.deferred, h.sink.events)
	}
}

// ---------- K050 / K074 / K099 / K142 ----------

// K050: a request that waited in Admitting before its validation (here an
// unsettled authority) is deferred on cooldown as onCooldown=Defer asks,
// its request kept for the retry, instead of ending NotStarted.
func TestUltraA_K050_UnsettledRequestDefersOnCooldown(t *testing.T) {
	fg := plannedFailoverFG("dr")
	fg.Spec.Sites = append(fg.Spec.Sites, fg.Spec.Sites[1])
	fg.Spec.Sites[2].Name = "dr"
	fg.Spec.PlannedFailover = &v1alpha1.PlannedFailoverSpec{OnCooldown: v1alpha1.PlannedFailoverCooldownDefer}
	fg.Status.ActiveSite = "" // an emergency failover to pdx is in flight
	// Mid-failover, dr's replication is stopped: validation would refuse it.
	fg.Status.Sites = append(fg.Status.Sites, v1alpha1.SiteStatus{Name: "dr", State: v1alpha1.SiteStateReadOnly, Replication: v1alpha1.SiteReplicationBroken})
	h := newPFEngineHarness(t, fg)
	h.step()
	if pf := h.pf(); pf == nil || pf.Phase != v1alpha1.PlannedFailoverPhaseAdmitting || pf.Admission.WaitReason != v1alpha1.PlannedFailoverWaitAuthorityUnsettled {
		t.Fatalf("block = %+v, want Admitting{AuthorityUnsettled}", pf)
	}
	// The emergency failover is published; its cooldown now applies.
	cur := h.fg()
	at := metav1.NewTime(h.clk.Now())
	cur.Status.ActiveSite = "pdx"
	cur.Status.SetLastFailover(&at, "pdx", "")
	cur.Status.Sites[0].State, cur.Status.Sites[1].State, cur.Status.Sites[1].Replication = v1alpha1.SiteStateReadOnly, v1alpha1.SiteStateWritable, v1alpha1.SiteReplicationNotApplicable
	cur.Status.Sites[2].Replication = v1alpha1.SiteReplicationHealthy
	if err := h.c.Status().Update(context.Background(), cur); err != nil {
		t.Fatal(err)
	}
	h.clk.Advance(time.Second)
	h.step()
	pf := h.pf()
	if pf.Phase != v1alpha1.PlannedFailoverPhaseDeferred || pf.Admission == nil || pf.Admission.DeferReason != v1alpha1.PlannedFailoverDeferCooldownActive {
		t.Fatalf("block = %+v, want Deferred{CooldownActive}", pf)
	}
	if h.fg().GetAnnotations()[v1alpha1.PlannedFailoverAnnotation] != "dr" {
		t.Fatal("the deferred request was not retained for its retry")
	}
}

// K074: consuming a request after a commit never removes a different
// request written in the meantime.
func TestUltraA_K074_ConsumeOnlyTheEvaluatedRequest(t *testing.T) {
	h := newPFEngineHarness(t, plannedFailoverFG("pdx"))
	evaluated := h.fg()
	h.setAnnotation(v1alpha1.PlannedFailoverAnnotation, "iad") // replaced after the pass read "pdx"
	h.e.consumeRequest(context.Background(), evaluated, "accepted")
	if got := h.fg().GetAnnotations()[v1alpha1.PlannedFailoverAnnotation]; got != "iad" {
		t.Fatalf("annotation = %q; a request the pass never evaluated was consumed", got)
	}
	h.e.consumeRequest(context.Background(), h.fg(), "accepted")
	if _, ok := h.fg().GetAnnotations()[v1alpha1.PlannedFailoverAnnotation]; ok {
		t.Fatal("the evaluated request was not consumed")
	}
}

// K099/K142: the run's own request left behind by a failed consumption is
// removed silently in a later phase, without a rejection Warning.
func TestUltraA_K099_OwnRequestConsumedSilentlyPastAdmission(t *testing.T) {
	h := pfHarnessAt(t, v1alpha1.PlannedFailoverPhaseDraining)
	h.setAnnotation(v1alpha1.PlannedFailoverAnnotation, "pdx")
	h.step()
	if _, ok := h.fg().GetAnnotations()[v1alpha1.PlannedFailoverAnnotation]; ok {
		t.Fatal("the run's own request was not consumed")
	}
	if n := h.sink.eventCount(v1alpha1.EventPlannedFailoverRejected); n != 0 {
		t.Fatalf("events = %v; the run's own request must not be refused", h.sink.events)
	}
	// Another request is still refused.
	h.setAnnotation(v1alpha1.PlannedFailoverAnnotation, "iad")
	h.step()
	if n := h.sink.eventCount(v1alpha1.EventPlannedFailoverRejected); n != 1 {
		t.Fatalf("events = %v; a different request must be refused", h.sink.events)
	}
}

// ---------- K017 / K067 ----------

// K017: settlement only accepts the write's own transition at its
// revision, not another transition computed from the same base.
func TestUltraA_K017_SettlementIdentifiesTheWrite(t *testing.T) {
	now := time.Now()
	fencing := pfRunAt(v1alpha1.PlannedFailoverPhaseFencing, now)
	fencing.Revision = 2
	failed := fencing.DeepCopy()
	failed.Phase = v1alpha1.PlannedFailoverPhaseFailed
	failed.Result = &v1alpha1.PlannedFailoverResult{Outcome: v1alpha1.PlannedFailoverOutcomeNotStarted,
		Reason: v1alpha1.PlannedFailoverReasonAdmissionTimeout}
	if plannedWriteLanded(fencing, failed) {
		t.Fatal("a Fencing block at the same revision proved that a Failed write committed")
	}
	if !plannedWriteLanded(failed, failed) || !plannedWriteLanded(fencing, fencing) {
		t.Fatal("the write's own transition must prove it landed")
	}
	later := fencing.DeepCopy()
	later.Revision, later.Phase = 3, v1alpha1.PlannedFailoverPhaseDraining
	if !plannedWriteLanded(later, fencing) {
		t.Fatal("a later revision of the run proves the write landed")
	}
}

// K067: a provisional grant whose Fencing write returned Unknown is
// released once a settlement proves the write cannot commit, instead of
// deferring emergency failover through the admission waits.
func TestUltraA_K067_UnknownFencingWriteReleasesGrant(t *testing.T) {
	h := pfHarnessAt(t, v1alpha1.PlannedFailoverPhaseAdmitting)
	h.faults.statusFaults = []pfWriteFault{pfWriteDropTimeout}
	h.faults.metaFaults = []pfWriteFault{pfWriteForbidden, pfWriteForbidden, pfWriteForbidden}
	h.step()
	if run, _ := h.topo.held(); run != "run-1" || h.pf().Phase != v1alpha1.PlannedFailoverPhaseAdmitting {
		t.Fatalf("setup: held=%q block=%s; want a provisional grant beside an unsettled Fencing write", run, h.pf().Phase)
	}
	// Authority becomes unsettled: the run now waits in Admitting.
	cur := h.fg()
	cur.Status.ActiveSite = ""
	if err := h.c.Status().Update(context.Background(), cur); err != nil {
		t.Fatal(err)
	}
	h.step()
	if run, _ := h.topo.held(); run != "" {
		t.Fatalf("the provisional grant is still held by %q while the run waits", run)
	}
	if a := h.pf().Admission; a == nil || a.WaitReason != v1alpha1.PlannedFailoverWaitAuthorityUnsettled {
		t.Fatalf("admission = %+v, want an AuthorityUnsettled wait", a)
	}
}

// ---------- K068 ----------

// K068: a pinned identity read that keeps failing while the pooled lag
// probe succeeds ends the wait in a bounded time (TargetLost).
func TestUltraA_K068_FailingIdentityReadIsBounded(t *testing.T) {
	h := pfHarnessAt(t, v1alpha1.PlannedFailoverPhaseWaitingForLag)
	h.topo.uuid = func(PlannedRunRef) (string, error) { return "", errors.New("too many connections") }
	for i := 0; i < 30 && h.pf().Phase == v1alpha1.PlannedFailoverPhaseWaitingForLag; i++ {
		h.step()
		h.clk.Advance(time.Second)
	}
	pf := h.pf()
	if pf.Phase != v1alpha1.PlannedFailoverPhaseRollingBack || pf.Rollback.Reason != v1alpha1.PlannedFailoverReasonTargetLost {
		t.Fatalf("block = %s/%+v; a failing identity read must roll back TargetLost within the loss window", pf.Phase, pf.Rollback)
	}
}

// ---------- K138 ----------

// K138: the lag-wait histogram gets one sample per run, after the
// transition it measures committed, however often that write is retried.
func TestUltraA_K138_LagWaitObservedOnceAfterCommit(t *testing.T) {
	h := pfHarnessAt(t, v1alpha1.PlannedFailoverPhaseWaitingForLag)
	h.faults.statusFaults = []pfWriteFault{pfWriteForbidden, pfWriteForbidden, pfWriteForbidden}
	for i := 0; i < 3; i++ {
		h.step()
	}
	if h.sink.lagWaits != 0 {
		t.Fatalf("lag waits observed = %d before the transition committed", h.sink.lagWaits)
	}
	h.stepUntil(5, func(fg *v1alpha1.MysqlFailoverGroup) bool {
		return fg.Status.PlannedFailover.Phase == v1alpha1.PlannedFailoverPhasePromoting
	})
	if h.sink.lagWaits != 1 {
		t.Fatalf("lag waits observed = %d, want 1", h.sink.lagWaits)
	}
}

// ---------- K069 / J003 / K155 ----------

func ultraAHeldTerminal(now time.Time) *v1alpha1.PlannedFailoverStatus {
	pf := pfRunAt(v1alpha1.PlannedFailoverPhasePromoting, now)
	pf.Phase, pf.Revision = v1alpha1.PlannedFailoverPhaseFailed, 7
	pf.Result = &v1alpha1.PlannedFailoverResult{Outcome: v1alpha1.PlannedFailoverOutcomeManualRecoveryRequired,
		Reason: v1alpha1.PlannedFailoverReasonForeignWritable, CompletionTime: metav1.NewTime(now)}
	return pf
}

// K069 + J003: a hold carried by the terminal marker (its terminal record
// committed) refuses a new request like any standing hold, and the refusal
// is counted under the requested target; it is never left to run once
// the hold is acknowledged.
func TestUltraA_K069_MarkerHoldRefusesRequests(t *testing.T) {
	h := newPFEngineHarness(t, plannedFailoverFG(""))
	h.setBlock(ultraAHeldTerminal(h.clk.Now()))
	h.setAnnotation(v1alpha1.PlannedFailoverTerminalAnnotation, TerminalMarker{RunID: "run-1",
		Outcome: v1alpha1.PlannedFailoverOutcomeManualRecoveryRequired, Reason: v1alpha1.PlannedFailoverReasonForeignWritable}.String())
	h.setAnnotation(v1alpha1.PlannedFailoverAnnotation, "pdx")
	h.step()
	if _, ok := h.fg().GetAnnotations()[v1alpha1.PlannedFailoverAnnotation]; ok {
		t.Fatal("a request beside a marker hold was neither refused nor consumed")
	}
	if h.sink.eventCount(v1alpha1.EventPlannedFailoverRejected) != 1 {
		t.Fatalf("events = %v, want one refusal", h.sink.events)
	}
	if n := len(h.sink.terminalTargets); n != 1 || h.sink.terminalTargets[0] != "pdx" {
		t.Fatalf("refusal metric targets = %v, want [pdx]", h.sink.terminalTargets)
	}
	if pf := h.pf(); pf.RunID != "run-1" {
		t.Fatalf("block = %+v; the hold run's record must stay", pf)
	}
}

// K155: the PlannedFailoverHoldPersisted Warning is silent at first, then
// repeats every plannedFailoverHoldPendingInterval, and its cadence starts
// over after an acknowledgement.
func TestUltraA_K155_HoldPersistedCadence(t *testing.T) {
	h := newPFEngineHarness(t, plannedFailoverFG(""))
	pf := ultraAHeldTerminal(h.clk.Now())
	h.setBlock(pf)
	fg := h.fg()
	setPlannedHold(&fg.Status, v1alpha1.PlannedFailoverHold{RunID: "run-1", Reason: v1alpha1.PlannedFailoverReasonForeignWritable,
		SourceSite: "iad", TargetSite: "pdx", HoldTime: metav1.NewTime(h.clk.Now())}, fg.Generation)
	if err := h.c.Status().Update(context.Background(), fg); err != nil {
		t.Fatal(err)
	}
	d := h.step()
	if n := h.sink.eventCount(v1alpha1.EventPlannedFailoverHoldPersisted); n != 0 {
		t.Fatalf("HoldPersisted emitted %d times on the first pass", n)
	}
	if d != plannedFailoverHoldPendingInterval {
		t.Fatalf("requeue = %s, want %s", d, plannedFailoverHoldPendingInterval)
	}
	h.clk.Advance(plannedFailoverHoldPendingInterval - time.Second)
	h.step()
	if n := h.sink.eventCount(v1alpha1.EventPlannedFailoverHoldPersisted); n != 0 {
		t.Fatal("HoldPersisted emitted before its interval")
	}
	h.clk.Advance(time.Second)
	h.step()
	if n := h.sink.eventCount(v1alpha1.EventPlannedFailoverHoldPersisted); n != 1 {
		t.Fatalf("HoldPersisted emitted %d times after one interval, want 1", n)
	}
	// Acknowledge: the cadence starts over for a later hold.
	h.topo.setHeld("run-1", true)
	h.setAnnotation(v1alpha1.AcknowledgePlannedFailoverHoldAnnotation, "run-1")
	h.stepUntil(6, func(fg *v1alpha1.MysqlFailoverGroup) bool {
		_, acked := fg.GetAnnotations()[v1alpha1.AcknowledgePlannedFailoverHoldAnnotation]
		return PlannedHoldFrom(fg) == nil && !acked
	})
	if !h.e.groupMemo(h.nn).holdPendingAt.IsZero() {
		t.Fatal("the HoldPersisted cadence was not reset by the acknowledgement")
	}
}

// ---------- K071 ----------

// K071: a PromotedUnconfirmed end never names its target over a failover
// another actor recorded during the run (§4.4 check 1 before naming).
func TestUltraA_K071_NoUnconfirmedNameOverChangedRecord(t *testing.T) {
	h := pfHarnessAt(t, v1alpha1.PlannedFailoverPhaseRollingBack)
	h.topo.rollback = func(PlannedRunRef) (RollbackResult, error) {
		return RollbackResult{Outcome: v1alpha1.PlannedFailoverOutcomePromotedUnconfirmed, Reason: v1alpha1.PlannedFailoverReasonTargetLost}, nil
	}
	ultraASetRecord(t, h, h.clk.Now().Add(time.Second), "iad", nil) // someone else's failover
	h.clk.Advance(2 * time.Second)
	h.step()
	if r := h.pf().Result; r == nil || r.Outcome != v1alpha1.PlannedFailoverOutcomePromotedUnconfirmed {
		t.Fatalf("result = %+v", r)
	}
	if len(h.topo.named) != 0 {
		t.Fatalf("named = %v over a changed failover record", h.topo.named)
	}

	// With the record unchanged it is named.
	g := pfHarnessAt(t, v1alpha1.PlannedFailoverPhaseRollingBack)
	g.topo.rollback = h.topo.rollback
	g.step()
	if len(g.topo.named) != 1 || g.topo.named[0] != "target:pdx" {
		t.Fatalf("named = %v, want [target:pdx]", g.topo.named)
	}
}

// ---------- K097 ----------

// K097: a malformed durable fenced set rolls back InvalidGTID at once,
// not TargetLost after reading the probe's parse error as a lost target.
func TestUltraA_K097_InvalidFencedSetRollsBackInvalidGTID(t *testing.T) {
	h := pfHarnessAt(t, v1alpha1.PlannedFailoverPhaseWaitingForLag)
	pf := h.pf()
	bad := "not-a-gtid-set"
	pf.Fence.GtidSet = &bad
	h.setBlock(pf)
	h.step()
	got := h.pf()
	if got.Phase != v1alpha1.PlannedFailoverPhaseRollingBack || got.Rollback.Reason != v1alpha1.PlannedFailoverReasonInvalidGTID {
		t.Fatalf("block = %s/%+v, want RollingBack{InvalidGTID}", got.Phase, got.Rollback)
	}
}

// ---------- K100 ----------

// K100: a reachable source whose drain query keeps failing is not lost:
// the drain proceeds at its budget, recording the remaining count as
// unknown (unset), never 0.
func TestUltraA_K100_DrainErrorIsNotSourceLoss(t *testing.T) {
	h := pfHarnessAt(t, v1alpha1.PlannedFailoverPhaseDraining)
	slow := h.pf()
	slow.Settings.DrainTimeout.Duration = 30 * time.Second // longer than the source-loss window
	h.setBlock(slow)
	h.topo.kill = func(PlannedRunRef) (int32, error) { return 0, errors.New("processlist query failed") }
	h.stepUntil(60, func(fg *v1alpha1.MysqlFailoverGroup) bool {
		return fg.Status.PlannedFailover.Phase != v1alpha1.PlannedFailoverPhaseDraining
	})
	pf := h.pf()
	if pf.Phase != v1alpha1.PlannedFailoverPhaseWaitingForLag {
		t.Fatalf("block = %s/%+v; a reachable source with a failing drain query must proceed at the drain budget", pf.Phase, pf.Rollback)
	}
	if pf.Drain.RemainingConnections != nil {
		t.Fatalf("remaining connections = %d; unknown must stay unset", *pf.Drain.RemainingConnections)
	}

	// An unreachable source is still lost within the loss window.
	g := pfHarnessAt(t, v1alpha1.PlannedFailoverPhaseDraining)
	long := g.pf()
	long.Settings.DrainTimeout.Duration = time.Minute
	g.setBlock(long)
	g.topo.kill = h.topo.kill
	g.topo.probe = func(PlannedRunRef) (LagProbe, error) { return LagProbe{}, nil }
	g.stepUntil(20, func(fg *v1alpha1.MysqlFailoverGroup) bool {
		return fg.Status.PlannedFailover.Phase != v1alpha1.PlannedFailoverPhaseDraining
	})
	if pf := g.pf(); pf.Phase != v1alpha1.PlannedFailoverPhaseRollingBack || pf.Rollback.Reason != v1alpha1.PlannedFailoverReasonSourceLost {
		t.Fatalf("block = %s/%+v, want RollingBack{SourceLost}", pf.Phase, pf.Rollback)
	}
}

// ---------- K143 ----------

// K143: a FenceNotSent end after an earlier SET was sent does not name the
// (replaced) source for re-assert, and its record says why.
func TestUltraA_K143_FenceNotSentAfterSetIsNotAttributable(t *testing.T) {
	h := pfHarnessAt(t, v1alpha1.PlannedFailoverPhaseFencing)
	calls := 0
	h.topo.fence = func(PlannedRunRef) (FenceProof, error) {
		calls++
		if calls == 1 {
			return FenceProof{Kind: FenceRetry, Sent: true}, nil // SET sent, not confirmed
		}
		return FenceProof{Kind: FenceNotSent, Reason: v1alpha1.PlannedFailoverReasonAuthorityChanged}, nil
	}
	h.stepUntil(5, func(fg *v1alpha1.MysqlFailoverGroup) bool { return plannedFailoverTerminal(fg.Status.PlannedFailover) })
	r := h.pf().Result
	if r.Outcome != v1alpha1.PlannedFailoverOutcomeSourceLeftFenced || !slices.Contains(r.Warnings, v1alpha1.PlannedFailoverWarningFenceNotAttributable) {
		t.Fatalf("result = %+v, want SourceLeftFenced with FenceNotAttributable", r)
	}
	if len(h.topo.named) != 0 {
		t.Fatalf("named = %v; a fence that is not the run's is never named", h.topo.named)
	}
	if _, ok := plannedFenceAttributableEnd(h.fg()); ok {
		t.Fatal("a restart would name the source")
	}
}

// ---------- K149 ----------

// ultraAPublishingGuard publishes a topology change while it grants the
// admission reservation, as a topology write that reopens the deployment
// gate during BeginPlanned does.
type ultraAPublishingGuard struct{ publish func() }

func (g ultraAPublishingGuard) BeginPlanned(context.Context, *v1alpha1.MysqlFailoverGroup) (*DeploymentLeaseView, func(), error) {
	g.publish()
	return nil, func() {}, nil
}
func (g ultraAPublishingGuard) Now() time.Time { return time.Now() }

// K149: a new request is validated against status as of the admission
// reservation (a failover published while it was taken is seen), and a
// fresh failover record naming a site other than status.activeSite counts
// as unsettled authority for a new request too.
func TestUltraA_K149_NewRequestSeesPublishedFailover(t *testing.T) {
	// The emergency failover to pdx is recorded but not yet published.
	fg := plannedFailoverFG("pdx")
	h := newPFEngineHarness(t, fg)
	ultraASetRecord(t, h, h.clk.Now().Add(-5*time.Second), "pdx", nil)
	cur := h.fg()
	cur.Status.Sites[1].Replication = v1alpha1.SiteReplicationBroken // pdx is being promoted
	if err := h.c.Status().Update(context.Background(), cur); err != nil {
		t.Fatal(err)
	}
	if unsettled, _ := plannedAuthorityUnsettled(h.fg(), nil, h.clk.Now()); !unsettled {
		t.Fatal("a fresh record naming a site other than activeSite must be unsettled for a new request")
	}
	// The topology is published during the reservation.
	h.e.admission = ultraAPublishingGuard{publish: func() {
		fresh := h.fg()
		fresh.Status.ActiveSite = "pdx"
		fresh.Status.Sites[0].State, fresh.Status.Sites[1].State = v1alpha1.SiteStateReadOnly, v1alpha1.SiteStateWritable
		if err := h.c.Status().Update(context.Background(), fresh); err != nil {
			t.Fatal(err)
		}
	}}
	h.step()
	if pf := h.pf(); pf != nil && pf.Result != nil && pf.Result.Reason != v1alpha1.PlannedFailoverReasonAlreadyActive {
		t.Fatalf("result = %+v; the request was judged on the snapshot from before the reservation", pf.Result)
	}
	if h.sink.eventCount(v1alpha1.EventPlannedFailoverSkipped) != 1 {
		t.Fatalf("events = %v, want the request resolved as already active", h.sink.events)
	}
}

// ---------- K150 ----------

// K150: a run this process ended by the marker, whose marker was removed
// by hand before its terminal record landed, still gets its terminal
// record, and a claim hydrated back for it is released.
func TestUltraA_K150_RemovedMarkerStillLandsTerminalRecord(t *testing.T) {
	h := pfHarnessAt(t, v1alpha1.PlannedFailoverPhaseDraining)
	h.faults.statusDenied = true
	for i := 0; i < 400 && h.fg().GetAnnotations()[v1alpha1.PlannedFailoverTerminalAnnotation] == ""; i++ {
		h.step()
		h.clk.Advance(time.Second)
	}
	if h.fg().GetAnnotations()[v1alpha1.PlannedFailoverTerminalAnnotation] == "" {
		t.Fatal("setup: no marker")
	}
	h.setAnnotation(v1alpha1.PlannedFailoverTerminalAnnotation, "") // removed by hand
	h.topo.setHeld("run-1", false)                                  // hydrated back
	h.faults.statusDenied = false
	h.step()
	pf := h.pf()
	if !plannedFailoverTerminal(pf) || pf.Result.Outcome != v1alpha1.PlannedFailoverOutcomeSourceLeftFenced {
		t.Fatalf("block = %s/%+v; the remembered terminal record must land", pf.Phase, pf.Result)
	}
	if run, _ := h.topo.held(); run != "" {
		t.Fatalf("claim still held by %q", run)
	}
	if n := h.topo.count("kill"); n > 0 {
		// Draining kills happened before the stall; none after the end.
		before := n
		h.step()
		if h.topo.count("kill") != before {
			t.Fatal("MySQL was touched for a run this process ended")
		}
	}
}

// ---------- K152 ----------

// K152: a promotion this process confirmed before the topology manager
// went away ends Succeeded with its failover record applied, not
// PromotedUnconfirmed with no record.
func TestUltraA_K152_ConfirmedPromotionSurvivesManagerAbsence(t *testing.T) {
	h := pfHarnessAt(t, v1alpha1.PlannedFailoverPhasePromoting)
	h.step() // Promote: confirmed in memory
	if !h.e.runMemo("run-1").promote.confirmed {
		t.Fatal("setup: not confirmed")
	}
	h.topo.ensure = func(PlannedRunRef) (PlannedClaimDecision, error) { return PlannedClaimDecision{}, errManagerAbsent }
	for i := 0; i < 400 && !plannedFailoverTerminal(h.pf()); i++ {
		h.step()
		h.clk.Advance(5 * time.Second)
	}
	pf := h.pf()
	if pf.Result == nil || pf.Result.Outcome != v1alpha1.PlannedFailoverOutcomeSucceeded {
		t.Fatalf("result = %+v, want Succeeded", pf.Result)
	}
	if st := h.fg().Status; st.LastFailoverTarget() != "pdx" || st.LastFailoverTime() == nil || st.PromotionGtidExecuted() != "uuid-iad:1-10" {
		t.Fatalf("failover record = %v/%v/%q; the confirmed promotion's record was not applied", st.LastFailover, st.LastFailoverTarget(), st.PromotionGtidExecuted())
	}
}

// ---------- K035 ----------

// K035: an acknowledgement with no durable hold behind it never reports
// that automation resumed: not beside a slot held for another run, and not
// for a stray run ID.
func TestUltraA_K035_AckWithoutHoldIsNotASuccess(t *testing.T) {
	h := newPFEngineHarness(t, plannedFailoverFG(""))
	h.topo.setHeld("run-other", true) // the durable records were removed by hand
	h.setAnnotation(v1alpha1.AcknowledgePlannedFailoverHoldAnnotation, "run-1")
	h.step()
	if h.sink.eventCount(v1alpha1.EventPlannedFailoverHoldAcknowledged) != 0 ||
		h.sink.eventCount(v1alpha1.EventPlannedFailoverHoldAcknowledgmentIgnored) != 1 {
		t.Fatalf("events = %v, want only an ignored acknowledgement", h.sink.events)
	}
	if run, hold := h.topo.held(); run != "run-other" || !hold {
		t.Fatalf("slot = %q/%v; another run's hold was touched", run, hold)
	}
	if _, ok := h.fg().GetAnnotations()[v1alpha1.AcknowledgePlannedFailoverHoldAnnotation]; ok {
		t.Fatal("the ignored acknowledgement was not removed")
	}

	// A stray acknowledgement with nothing held.
	g := newPFEngineHarness(t, plannedFailoverFG(""))
	g.setAnnotation(v1alpha1.AcknowledgePlannedFailoverHoldAnnotation, "run-9")
	g.step()
	if g.sink.eventCount(v1alpha1.EventPlannedFailoverHoldAcknowledged) != 0 {
		t.Fatalf("events = %v; a stray acknowledgement reported success", g.sink.events)
	}

	// The real flow still reports success.
	k := newPFEngineHarness(t, plannedFailoverFG(""))
	k.setBlock(ultraAHeldTerminal(k.clk.Now()))
	fg := k.fg()
	setPlannedHold(&fg.Status, v1alpha1.PlannedFailoverHold{RunID: "run-1", Reason: v1alpha1.PlannedFailoverReasonForeignWritable,
		HoldTime: metav1.NewTime(k.clk.Now())}, fg.Generation)
	if err := k.c.Status().Update(context.Background(), fg); err != nil {
		t.Fatal(err)
	}
	k.topo.setHeld("run-1", true)
	k.setAnnotation(v1alpha1.AcknowledgePlannedFailoverHoldAnnotation, "run-1")
	k.stepUntil(6, func(fg *v1alpha1.MysqlFailoverGroup) bool {
		_, acked := fg.GetAnnotations()[v1alpha1.AcknowledgePlannedFailoverHoldAnnotation]
		return !acked
	})
	if k.sink.eventCount(v1alpha1.EventPlannedFailoverHoldAcknowledged) != 1 {
		t.Fatalf("events = %v, want the acknowledgement reported", k.sink.events)
	}
}

// ---------- K166 ----------

// K166: a run's terminal marker never replaces another run's
// ManualRecoveryRequired marker.
func TestUltraA_K166_MarkerNeverReplacesAnotherRunsHold(t *testing.T) {
	h := newPFEngineHarness(t, plannedFailoverFG(""))
	hold := TerminalMarker{RunID: "run-b", Outcome: v1alpha1.PlannedFailoverOutcomeManualRecoveryRequired,
		Reason: v1alpha1.PlannedFailoverReasonForeignWritable}
	h.setAnnotation(v1alpha1.PlannedFailoverTerminalAnnotation, hold.String())
	out, err := h.e.store.WriteTerminalMarker(context.Background(), h.fg(), TerminalMarker{RunID: "run-a",
		Outcome: v1alpha1.PlannedFailoverOutcomeSourceLeftFenced, Reason: v1alpha1.PlannedFailoverReasonStatusStalled})
	if out == WriteCommitted || err == nil {
		t.Fatalf("WriteTerminalMarker = %s, %v; it replaced another run's hold", out, err)
	}
	if got := h.fg().GetAnnotations()[v1alpha1.PlannedFailoverTerminalAnnotation]; got != hold.String() {
		t.Fatalf("marker = %s", got)
	}
	// The hold's own run, and a non-hold marker, are still written.
	if out, err := h.e.store.WriteTerminalMarker(context.Background(), h.fg(), TerminalMarker{RunID: "run-b",
		Outcome: v1alpha1.PlannedFailoverOutcomeManualRecoveryRequired, Reason: v1alpha1.PlannedFailoverReasonStatusStalled}); out != WriteCommitted {
		t.Fatalf("own marker = %s, %v", out, err)
	}
}

// ---------- K144 ----------

// K144: an admission wait whose detail changes (another claimant) is
// recorded again, not only when its reason changes.
func TestUltraA_K144_WaitDetailChangeIsRecorded(t *testing.T) {
	h := pfHarnessAt(t, v1alpha1.PlannedFailoverPhaseAdmitting)
	holder := "run-a"
	h.topo.claim = func(PlannedClaimRequest) (PlannedClaimDecision, error) {
		return claimDecisionWait(v1alpha1.PlannedFailoverWaitClaimHeld, "the claim is held by %s", holder), nil
	}
	h.step()
	if !strings.Contains(h.pf().Message, "run-a") {
		t.Fatalf("message = %q", h.pf().Message)
	}
	holder = "run-b"
	h.step()
	if !strings.Contains(h.pf().Message, "run-b") {
		t.Fatalf("message = %q; the new claimant was not recorded", h.pf().Message)
	}
}

// ---------- K026 ----------

// K026: the memo of a finished run is dropped once another run's block
// replaced it, and a deleted group's memo with it.
func TestUltraA_K026_MemosArePruned(t *testing.T) {
	h := newPFEngineHarness(t, plannedFailoverFG("nowhere"))
	h.step() // NotStarted{UnknownSite}: a finished run
	first := h.pf().RunID
	h.setAnnotation(v1alpha1.PlannedFailoverAnnotation, "nowhere")
	h.step() // a second finished run replaces it
	h.step()
	h.e.mu.Lock()
	_, kept := h.e.runs[first]
	_, term := h.e.terminatedRuns[first]
	h.e.mu.Unlock()
	if kept || term {
		t.Fatalf("memo of finished run %s kept (runs=%v terminated=%v)", first, kept, term)
	}
	h.e.groupMemo(h.nn)
	if err := h.c.Delete(context.Background(), h.fg()); err != nil {
		t.Fatal(err)
	}
	h.step()
	h.e.mu.Lock()
	_, kept = h.e.groups[h.nn]
	h.e.mu.Unlock()
	if kept {
		t.Fatal("a deleted group's memo was kept")
	}
}

// ---------- K148 ----------

// K148: a claim hydrated back for a run whose confirmed tail already
// released it (before Publishing committed) makes the same process run
// the tail again under that claim: record, release, then publish.
func TestUltraA_K148_RehydratedClaimRerunsConfirmedTail(t *testing.T) {
	h := pfHarnessAt(t, v1alpha1.PlannedFailoverPhasePromoting)
	h.faults.statusDenied = true
	h.step() // confirmed
	h.step() // tail: record, DNS, release; Publishing denied
	if run, _ := h.topo.held(); run != "" || h.topo.count("record") != 1 {
		t.Fatalf("setup: held=%q records=%d", run, h.topo.count("record"))
	}
	h.topo.setHeld("run-1", false) // a replacement manager hydrated it
	h.faults.statusDenied = false
	h.stepUntil(10, func(fg *v1alpha1.MysqlFailoverGroup) bool { return plannedFailoverTerminal(fg.Status.PlannedFailover) })
	if r := h.pf().Result; r.Outcome != v1alpha1.PlannedFailoverOutcomeSucceeded {
		t.Fatalf("result = %+v, want Succeeded", r)
	}
	if n := h.topo.count("record"); n != 2 {
		t.Fatalf("records = %d, want the promotion recorded again into the hydrated claim", n)
	}
	if run, _ := h.topo.held(); run != "" {
		t.Fatalf("claim still held by %q", run)
	}
}

// ---------- K164 ----------

// K164: every reconcile-side claim question goes through the one facade, so
// an injected facade answers them all, with or without a runner.
func TestUltraA_K164_ClaimQuestionsUseOneFacade(t *testing.T) {
	fg := plannedFailoverFG("")
	r, _, topo, _ := ultraAReconciler(t, interceptor.Funcs{}, fg)
	nn := fgNN(fg)
	topo.setHeld("run-1", false)
	if !r.plannedClaimHeld(nn) || !r.plannedRunClaimHeld(nn) || !r.plannedFailoverPassRuns(fg) {
		t.Fatal("the injected facade's held claim is not seen by every check")
	}
	runner := &TopologyManagerRunner{logger: testLogger()}
	r.Runner = runner // its own slot is empty
	if !r.plannedClaimHeld(nn) {
		t.Fatal("plannedClaimHeld bypassed the injected facade for the runner's slot")
	}
}

// ---------- K118 ----------

// K118: the injected claim-slot check makes an otherwise stable group
// unstable for deployments: Grant, Snapshot and BeginPlanned refuse while
// the slot is held (a provisional grant status does not show yet).
func TestUltraA_K118_HeldSlotGuardsDeploymentAdmission(t *testing.T) {
	m, fg, _ := deploymentLeaseFixture(t)
	if reason := m.UnstableReason(fg); reason != "" {
		t.Fatalf("setup: unstable %q", reason)
	}
	held := false
	m.SetPlannedClaimHeld(func(types.NamespacedName) bool { return held })
	epoch := m.TopologyPending(client.ObjectKeyFromObject(fg))
	if err := m.TopologyPersisted(context.Background(), fg, epoch); err != nil {
		t.Fatal(err)
	}
	grantDeploymentLease(t, m, fg, "migration", "op0", 30) // stable: granted
	held = true
	if reason := m.UnstableReason(fg); reason != "PlannedFailover" {
		t.Fatalf("UnstableReason = %q with the slot held, want PlannedFailover", reason)
	}
	_, err := m.Grant(context.Background(), fg, "migration", "op1", "tenant", "tenant-ns", "deployer", 30)
	requireDeploymentError(t, err, 423, "unstable")
	if _, err := m.BeginPlanned(context.Background(), fg, deploymentOperationOrderedUpdate); err == nil {
		t.Fatal("BeginPlanned admitted an ordered update beside a held slot")
	}
	if _, err := m.BeginPlanned(context.Background(), fg, deploymentOperationPlannedFailover); err != nil {
		t.Fatalf("the planned failover's own admission was refused: %v", err)
	}
	m.EndPlanned(fg)
	held = false
	if reason := m.UnstableReason(fg); reason != "" {
		t.Fatalf("UnstableReason = %q after release", reason)
	}
}

// WE-Q8: a standing automation hold is unstableReason AutomaticFailoverHeld,
// not PlannedFailover (a run in flight or its claim), whether the operator
// holds it in the claim slot or it is only durable (status, condition or
// terminal marker) before the slot is hydrated. Leases stay refused.
func TestUnstableReason_AutomaticFailoverHeld(t *testing.T) {
	for _, tc := range []struct {
		name              string
		slotRun, slotHold bool
		durable           func(*v1alpha1.MysqlFailoverGroup)
		want              string
	}{
		{name: "run claim in the slot", slotRun: true, want: v1alpha1.UnstableReasonPlannedFailover},
		{name: "hold in the slot", slotRun: true, slotHold: true, want: v1alpha1.UnstableReasonAutomaticFailoverHeld},
		{name: "status.plannedFailoverHold", durable: func(fg *v1alpha1.MysqlFailoverGroup) {
			fg.Status.PlannedFailoverHold = &v1alpha1.PlannedFailoverHold{RunID: "run-1", Reason: v1alpha1.PlannedFailoverReasonPromotionUnconfirmed}
		}, want: v1alpha1.UnstableReasonAutomaticFailoverHeld},
		{name: "True AutomaticFailoverHeld condition", durable: func(fg *v1alpha1.MysqlFailoverGroup) {
			fg.Status.Conditions = append(fg.Status.Conditions, metav1.Condition{Type: v1alpha1.ConditionAutomaticFailoverHeld, Status: metav1.ConditionTrue,
				Reason: v1alpha1.ReasonPlannedFailoverManualRecoveryRequired, LastTransitionTime: metav1.Now()})
		}, want: v1alpha1.UnstableReasonAutomaticFailoverHeld},
		{name: "an acknowledged hold", durable: func(fg *v1alpha1.MysqlFailoverGroup) {
			fg.Status.Conditions = append(fg.Status.Conditions, metav1.Condition{Type: v1alpha1.ConditionAutomaticFailoverHeld, Status: metav1.ConditionFalse,
				Reason: v1alpha1.ReasonPlannedFailoverHoldAcknowledged, LastTransitionTime: metav1.Now()})
		}, want: ""},
	} {
		t.Run(tc.name, func(t *testing.T) {
			m, fg, _ := deploymentLeaseFixture(t)
			m.SetPlannedClaimHeld(func(types.NamespacedName) bool { return tc.slotRun })
			m.SetPlannedHoldHeld(func(types.NamespacedName) bool { return tc.slotHold })
			if tc.durable != nil {
				tc.durable(fg)
				if err := m.client.Status().Update(context.Background(), fg); err != nil {
					t.Fatal(err)
				}
			}
			if reason := m.UnstableReason(fg); reason != tc.want {
				t.Fatalf("UnstableReason = %q, want %q", reason, tc.want)
			}
			if tc.want == "" {
				return
			}
			_, err := m.Grant(context.Background(), fg, "migration", "op1", "tenant", "tenant-ns", "deployer", 30)
			if e := requireDeploymentError(t, err, 423, "unstable"); e.Reason != tc.want {
				t.Fatalf("refusal reason %q, want %q", e.Reason, tc.want)
			}
		})
	}
}
