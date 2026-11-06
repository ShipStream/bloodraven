package controller

import (
	"context"
	"strings"
	"testing"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/tools/record"

	v1alpha1 "github.com/shipstream/bloodraven/api/v1alpha1"
	"github.com/shipstream/bloodraven/internal/clock"
)

// plannedFailoverFG returns a two-site failover group with activeSite=iad,
// pdx as a healthy replicating read-only replica, and an annotation
// requesting failover to pdx. Tests may further mutate status before
// calling into the reconciler.
func plannedFailoverFG(annotationValue string) *v1alpha1.MysqlFailoverGroup {
	fg := newTestFG()
	fg.Spec.Sites[0].Name = "iad"
	fg.Spec.Sites[1].Name = "pdx"
	fg.Spec.FailoverCooldown = &metav1.Duration{Duration: 5 * time.Minute}
	fg.Status.ActiveSite = "iad"
	fg.Status.Sites = []v1alpha1.SiteStatus{
		{Name: "iad", State: "writable", Replicating: false},
		{Name: "pdx", State: "read-only", Replicating: true},
	}
	if annotationValue != "" {
		fg.SetAnnotations(map[string]string{
			v1alpha1.PlannedFailoverAnnotation: annotationValue,
		})
	}
	return fg
}

func fetchFG(t *testing.T, r *MysqlFailoverGroupReconciler, nn types.NamespacedName) *v1alpha1.MysqlFailoverGroup {
	t.Helper()
	var fg v1alpha1.MysqlFailoverGroup
	if err := r.Get(context.Background(), nn, &fg); err != nil {
		t.Fatalf("fetch fg: %v", err)
	}
	return &fg
}

func fgNN(fg *v1alpha1.MysqlFailoverGroup) types.NamespacedName {
	return types.NamespacedName{Namespace: fg.Namespace, Name: fg.Name}
}

// pfReconciler returns a reconciler over fg with a scripted topology facade
// and a fake clock.
func pfReconciler(t *testing.T, fg *v1alpha1.MysqlFailoverGroup) (*MysqlFailoverGroupReconciler, *pfFakeTopology) {
	t.Helper()
	r, _ := newReconciler(fg)
	r.Recorder = record.NewFakeRecorder(64)
	topo := newPFFakeTopology()
	r.PlannedTopology = topo
	r.plannedClock = clock.NewFakeClock(time.Now())
	return r, topo
}

// --- the adapter ---------------------------------------------------------

func TestReconcilePlannedFailover_NoopWhenIdle(t *testing.T) {
	fg := plannedFailoverFG("")
	r, topo := pfReconciler(t, fg)
	d, err := r.reconcilePlannedFailover(context.Background(), fg)
	if err != nil || d != 0 {
		t.Fatalf("expected no-op, got d=%s err=%v", d, err)
	}
	if fetched := fetchFG(t, r, fgNN(fg)); fetched.Status.PlannedFailover != nil {
		t.Errorf("status stamped on an idle group: %+v", fetched.Status.PlannedFailover)
	}
	if len(topo.calls) != 0 {
		t.Errorf("idle pass reached the topology: %v", topo.calls)
	}
}

func TestReconcilePlannedFailover_NoopTerminalStatusWithoutAnnotation(t *testing.T) {
	fg := plannedFailoverFG("")
	now := metav1.Now()
	fg.Status.PlannedFailover = &v1alpha1.PlannedFailoverStatus{RunID: "run-0", Revision: 3, Phase: v1alpha1.PlannedFailoverPhaseSucceeded,
		TargetSite: "pdx", StartTime: now, Result: &v1alpha1.PlannedFailoverResult{Outcome: v1alpha1.PlannedFailoverOutcomeSucceeded, CompletionTime: now}}
	r, _ := pfReconciler(t, fg)
	if plannedFailoverPassDue(fg) {
		t.Fatal("a terminal block with no annotation needs no pass")
	}
	if d, err := r.reconcilePlannedFailover(context.Background(), fg); err != nil || d != 0 {
		t.Fatalf("d=%s err=%v", d, err)
	}
	if pf := fetchFG(t, r, fgNN(fg)).Status.PlannedFailover; pf.Revision != 3 {
		t.Errorf("terminal block rewritten: %+v", pf)
	}
}

// Accept → Admitting with the run's settings snapshotted, the annotation
// removed and a PlannedFailoverStarted Event.
func TestReconcilePlannedFailover_AcceptStampsAdmitting(t *testing.T) {
	fg := plannedFailoverFG("pdx:maxLagWait=30s")
	r, _ := pfReconciler(t, fg)
	if _, err := r.reconcilePlannedFailover(context.Background(), fg); err != nil {
		t.Fatal(err)
	}
	got := fetchFG(t, r, fgNN(fg))
	pf := got.Status.PlannedFailover
	if pf == nil || pf.Phase != v1alpha1.PlannedFailoverPhaseAdmitting || pf.RunID == "" || pf.TargetSite != "pdx" ||
		pf.Settings.MaxLagWait.Duration != 30*time.Second ||
		pf.Settings.DrainTimeout.Duration != defaultPlannedFailoverDrainTimeout || pf.Admission == nil || pf.Admission.DeadlineTime == nil {
		t.Fatalf("admitting record = %+v", pf)
	}
	if _, ok := got.Annotations[v1alpha1.PlannedFailoverAnnotation]; ok {
		t.Error("annotation not removed after the Admitting write")
	}
	select {
	case ev := <-r.Recorder.(*record.FakeRecorder).Events:
		if !strings.Contains(ev, "PlannedFailoverStarted") || !strings.Contains(ev, "pdx") {
			t.Errorf("unexpected event: %s", ev)
		}
	case <-time.After(time.Second):
		t.Error("no event emitted")
	}
}

// The adapter drives a run to Succeeded through the injected facade; the
// engine is kept across passes.
func TestReconcilePlannedFailover_EndToEndWithInjectedFacade(t *testing.T) {
	fg := plannedFailoverFG("pdx")
	r, topo := pfReconciler(t, fg)
	nn := fgNN(fg)
	for pass := 0; pass < 20; pass++ {
		if _, err := r.reconcilePlannedFailover(context.Background(), fetchFG(t, r, nn)); err != nil {
			t.Fatalf("pass %d: %v", pass, err)
		}
		if plannedFailoverTerminal(fetchFG(t, r, nn).Status.PlannedFailover) {
			break
		}
	}
	pf := fetchFG(t, r, nn).Status.PlannedFailover
	if pf.Result == nil || pf.Result.Outcome != v1alpha1.PlannedFailoverOutcomeSucceeded {
		t.Fatalf("final = %+v", pf)
	}
	if topo.count("fence") != 1 || topo.count("promote") != 1 {
		t.Errorf("side effects = %v", topo.calls)
	}
}

func TestReconcilePlannedFailover_RejectUnknownSite(t *testing.T) {
	fg := plannedFailoverFG("nowhere")
	r, _ := pfReconciler(t, fg)
	if _, err := r.reconcilePlannedFailover(context.Background(), fg); err != nil {
		t.Fatal(err)
	}
	got := fetchFG(t, r, fgNN(fg))
	if pf := got.Status.PlannedFailover; pf == nil || pf.Result == nil || pf.Result.Outcome != v1alpha1.PlannedFailoverOutcomeNotStarted ||
		pf.Result.Reason != v1alpha1.PlannedFailoverReasonUnknownSite {
		t.Fatalf("block = %+v", pf)
	}
	if _, ok := got.Annotations[v1alpha1.PlannedFailoverAnnotation]; ok {
		t.Error("rejected annotation not removed")
	}
}

func TestReconcilePlannedFailover_CooldownRejectDefault(t *testing.T) {
	fg := plannedFailoverFG("pdx")
	last := metav1.NewTime(time.Now().Add(-time.Minute))
	fg.Status.LastFailover, fg.Status.LastFailoverTarget = &last, "iad"
	r, _ := pfReconciler(t, fg)
	if _, err := r.reconcilePlannedFailover(context.Background(), fg); err != nil {
		t.Fatal(err)
	}
	if pf := fetchFG(t, r, fgNN(fg)).Status.PlannedFailover; pf.Result == nil || pf.Result.Reason != v1alpha1.PlannedFailoverReasonCooldownActive {
		t.Fatalf("block = %+v", pf)
	}
}

// A request that arrives while a run owns the sites is refused with a
// Warning and removed.
func TestReconcilePlannedFailover_StaleAnnotationDuringRunCleared(t *testing.T) {
	fg := plannedFailoverFG("iad")
	fg.Status.PlannedFailover = pfRunAt(v1alpha1.PlannedFailoverPhaseWaitingForLag, time.Now())
	r, topo := pfReconciler(t, fg)
	topo.setHeld("run-1", false)
	topo.probe = func(PlannedRunRef) (LagProbe, error) {
		return LagProbe{SourceReachable: true, TargetReachable: true}, nil
	}
	if _, err := r.reconcilePlannedFailover(context.Background(), fg); err != nil {
		t.Fatal(err)
	}
	if _, ok := fetchFG(t, r, fgNN(fg)).Annotations[v1alpha1.PlannedFailoverAnnotation]; ok {
		t.Error("stale annotation kept during a run")
	}
	ev := <-r.Recorder.(*record.FakeRecorder).Events
	if !strings.Contains(ev, "PlannedFailoverRejected") {
		t.Errorf("event = %s", ev)
	}
}

// A Deferred request edited to the active site resolves as a no-op.
func TestReconcilePlannedFailover_DeferredAnnotationEditedToActiveIsSkipped(t *testing.T) {
	fg := plannedFailoverFG("iad")
	fg.Status.PlannedFailover = pfRunAt(v1alpha1.PlannedFailoverPhaseDeferred, time.Now())
	r, _ := pfReconciler(t, fg)
	if _, err := r.reconcilePlannedFailover(context.Background(), fg); err != nil {
		t.Fatal(err)
	}
	got := fetchFG(t, r, fgNN(fg))
	if pf := got.Status.PlannedFailover; pf.Result == nil || pf.Result.Reason != v1alpha1.PlannedFailoverReasonAlreadyActive {
		t.Fatalf("block = %+v", pf)
	}
	if _, ok := got.Annotations[v1alpha1.PlannedFailoverAnnotation]; ok {
		t.Error("annotation kept")
	}
}

// Until the runner's facade is wired in, production sends nothing: the
// runner's claim stub waits on ManagerAbsent and the request stays in
// admission.
func TestReconcilePlannedFailover_PendingRunnerFacadeSendsNothing(t *testing.T) {
	fg := plannedFailoverFG("pdx")
	r, _ := newReconciler(fg)
	r.Runner = NewTopologyManagerRunner(r.Client, nil, nil, nil, testLogger())
	nn := fgNN(fg)
	for pass := 0; pass < 3; pass++ {
		if _, err := r.reconcilePlannedFailover(context.Background(), fetchFG(t, r, nn)); err != nil {
			t.Fatal(err)
		}
	}
	pf := fetchFG(t, r, nn).Status.PlannedFailover
	if pf.Phase != v1alpha1.PlannedFailoverPhaseAdmitting || pf.Admission.WaitReason != v1alpha1.PlannedFailoverWaitManagerAbsent {
		t.Fatalf("block = %+v", pf)
	}
}

// --- helpers --------------------------------------------------------------

func TestEffectiveDrainTimeout(t *testing.T) {
	fg := &v1alpha1.MysqlFailoverGroup{}
	if got := effectiveDrainTimeout(fg); got != defaultPlannedFailoverDrainTimeout {
		t.Errorf("default: got %s, want %s", got, defaultPlannedFailoverDrainTimeout)
	}
	fg.Spec.PlannedFailover = &v1alpha1.PlannedFailoverSpec{
		DrainTimeout: &metav1.Duration{Duration: 10 * time.Second},
	}
	if got := effectiveDrainTimeout(fg); got != 10*time.Second {
		t.Errorf("override: got %s, want 10s", got)
	}
}

func TestEffectiveOnCooldown(t *testing.T) {
	fg := &v1alpha1.MysqlFailoverGroup{}
	if got := effectiveOnCooldown(fg); got != v1alpha1.PlannedFailoverCooldownReject {
		t.Errorf("default: got %q, want Reject", got)
	}
	fg.Spec.PlannedFailover = &v1alpha1.PlannedFailoverSpec{OnCooldown: v1alpha1.PlannedFailoverCooldownDefer}
	if got := effectiveOnCooldown(fg); got != v1alpha1.PlannedFailoverCooldownDefer {
		t.Errorf("Defer: got %q, want Defer", got)
	}
	// The released lower-case spelling, which CRD ratcheting keeps on a
	// stored object, keeps its meaning (G-M1).
	for _, v := range []v1alpha1.PlannedFailoverCooldownPolicy{"defer", "DEFER"} {
		fg.Spec.PlannedFailover.OnCooldown = v
		if got := effectiveOnCooldown(fg); got != v1alpha1.PlannedFailoverCooldownDefer {
			t.Errorf("%q: got %q, want Defer", v, got)
		}
	}
	// Malformed values fall back to Reject (defence against typos).
	for _, v := range []v1alpha1.PlannedFailoverCooldownPolicy{"skip", "reject", "Reject", ""} {
		fg.Spec.PlannedFailover.OnCooldown = v
		if got := effectiveOnCooldown(fg); got != v1alpha1.PlannedFailoverCooldownReject {
			t.Errorf("%q: got %q, want Reject fallback", v, got)
		}
	}
}

// The released lower-case onSyncTimeout values keep their meaning, both in
// the spec and in a run's frozen settings (G-M1).
func TestDragonflyOnSyncTimeoutIgnoresCase(t *testing.T) {
	for _, tc := range []struct {
		in   v1alpha1.DragonflySyncTimeoutPolicy
		want v1alpha1.DragonflySyncTimeoutPolicy
	}{
		{"Fail", v1alpha1.DragonflySyncTimeoutFail},
		{"fail", v1alpha1.DragonflySyncTimeoutFail},
		{"FAIL", v1alpha1.DragonflySyncTimeoutFail},
		{"Proceed", v1alpha1.DragonflySyncTimeoutProceed},
		{"proceed", v1alpha1.DragonflySyncTimeoutProceed},
		{"", v1alpha1.DragonflySyncTimeoutProceed},
		{"bogus", v1alpha1.DragonflySyncTimeoutProceed},
	} {
		if got := effectiveDragonflyOnSyncTimeout(plannedFailoverFGWithDragonfly(string(tc.in))); got != tc.want {
			t.Errorf("spec %q: got %q, want %q", tc.in, got, tc.want)
		}
		pf := &v1alpha1.PlannedFailoverStatus{Settings: v1alpha1.PlannedFailoverSettings{
			Dragonfly: &v1alpha1.PlannedFailoverDragonflySettings{OnSyncTimeout: tc.in}}}
		if got := plannedSettings(pf).Dragonfly.OnSyncTimeout; got != tc.want {
			t.Errorf("settings %q: got %q, want %q", tc.in, got, tc.want)
		}
	}
}

func TestCooldownRetryAfter(t *testing.T) {
	fg := &v1alpha1.MysqlFailoverGroup{}
	if !cooldownRetryAfter(fg, time.Now()).IsZero() {
		t.Error("no lastFailover: want zero time")
	}
	last := metav1.NewTime(time.Date(2026, 4, 20, 12, 0, 0, 0, time.UTC))
	fg.Status.LastFailover = &last
	fg.Spec.FailoverCooldown = &metav1.Duration{Duration: 10 * time.Minute}
	want := last.Time.Add(10 * time.Minute)
	if got := cooldownRetryAfter(fg, last.Time); !got.Equal(want) {
		t.Errorf("got %v, want %v", got, want)
	}
}

// The run's settings are snapshotted at acceptance and read back with the
// defaults filling a block that carries none.
func TestPlannedSettings(t *testing.T) {
	if s := plannedSettings(nil); s.DrainTimeout.Duration != defaultPlannedFailoverDrainTimeout || s.MaxLagWait.Duration != defaultPlannedFailoverMaxLagWait || s.Dragonfly != nil {
		t.Errorf("nil block: %+v", s)
	}
	fg := plannedFailoverFGWithDragonfly("Fail")
	s := plannedFailoverSettingsFor(fg, PlannedFailoverRequest{Site: "pdx", MaxLagWait: 7 * time.Second})
	if s.MaxLagWait.Duration != 7*time.Second || s.Dragonfly == nil || s.Dragonfly.OnSyncTimeout != v1alpha1.DragonflySyncTimeoutFail ||
		s.Dragonfly.MaxSyncWait.Duration != 100*time.Millisecond {
		t.Errorf("snapshot: %+v %+v", s, s.Dragonfly)
	}
	if got := plannedSettings(&v1alpha1.PlannedFailoverStatus{Settings: s}); got.MaxLagWait.Duration != 7*time.Second {
		t.Errorf("read back: %+v", got)
	}
	if s := plannedFailoverSettingsFor(plannedFailoverFG(""), PlannedFailoverRequest{Site: "pdx"}); s.Dragonfly != nil {
		t.Error("Dragonfly settings snapshotted for a group without Dragonfly")
	}
}
