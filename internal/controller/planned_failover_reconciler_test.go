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
			PlannedFailoverAnnotation: annotationValue,
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

// --- top-level no-op and cancel ----------------------------------------

func TestReconcilePlannedFailover_NoopWhenIdle(t *testing.T) {
	fg := plannedFailoverFG("")
	r, _ := newReconciler(fg)
	d, err := r.reconcilePlannedFailover(context.Background(), fg)
	if err != nil || d != 0 {
		t.Fatalf("expected no-op, got d=%s err=%v", d, err)
	}
	if fetched := fetchFG(t, r, fgNN(fg)); fetched.Status.PlannedFailover != nil {
		t.Errorf("expected no status stamped, got %+v", fetched.Status.PlannedFailover)
	}
}

func TestReconcilePlannedFailover_NoopTerminalStatusWithoutAnnotation(t *testing.T) {
	fg := plannedFailoverFG("")
	done := metav1.Now()
	fg.Status.PlannedFailover = &v1alpha1.PlannedFailoverStatus{
		Phase:          v1alpha1.PlannedFailoverPhaseSucceeded,
		Target:         "pdx",
		CompletionTime: &done,
	}
	r, _ := newReconciler(fg)
	d, err := r.reconcilePlannedFailover(context.Background(), fg)
	if err != nil || d != 0 {
		t.Fatalf("expected no-op with terminal status, got d=%s err=%v", d, err)
	}
}

func TestPlannedFailoverRollback_ContinuesOnUnfenceError(t *testing.T) {
	fg := plannedFailoverFG("")
	fg.Status.PlannedFailover = &v1alpha1.PlannedFailoverStatus{
		Phase:         v1alpha1.PlannedFailoverPhaseWaitingForLag,
		Target:        "pdx",
		SourcePrimary: "iad",
	}

	r, _ := newReconciler(fg)
	// Empty runner has no managers, so unfence fails. Rollback must still
	// stamp terminal Failed status and release the planned-failover guard.
	r.Runner = &TopologyManagerRunner{}

	if d, err := r.plannedFailoverRollback(context.Background(), fg, fgNN(fg), "LagTimeout", "timeout message", "failed_timeout"); err != nil || d != 0 {
		t.Fatalf("rollback returned d=%s err=%v, want terminal failure without requeue", d, err)
	}

	fetched := fetchFG(t, r, fgNN(fg))
	pf := fetched.Status.PlannedFailover
	if pf == nil || pf.Phase != v1alpha1.PlannedFailoverPhaseFailed {
		t.Fatalf("expected terminal Failed, got %+v", pf)
	}
	if pf.Reason != "LagTimeout" {
		t.Errorf("reason = %q, want LagTimeout", pf.Reason)
	}
	if !strings.Contains(pf.Message, "timeout message") {
		t.Errorf("message = %q, want it to contain original message", pf.Message)
	}
	if !strings.Contains(pf.Message, "warning: source primary unfence failed") {
		t.Errorf("message = %q, want unfence failure warning", pf.Message)
	}
}

// --- accept/reject paths -----------------------------------------------

func TestReconcilePlannedFailover_AcceptStampsPending(t *testing.T) {
	fg := plannedFailoverFG("pdx")
	r, _ := newReconciler(fg)
	d, err := r.reconcilePlannedFailover(context.Background(), fg)
	if err != nil {
		t.Fatalf("unexpected err: %v", err)
	}
	if d <= 0 {
		t.Errorf("expected positive requeue to advance, got %s", d)
	}
	fetched := fetchFG(t, r, fgNN(fg))
	if fetched.Status.PlannedFailover == nil {
		t.Fatal("expected planned-failover status stamped")
	}
	if fetched.Status.PlannedFailover.Phase != v1alpha1.PlannedFailoverPhasePending {
		t.Errorf("phase = %q, want Pending", fetched.Status.PlannedFailover.Phase)
	}
	if fetched.Status.PlannedFailover.Target != "pdx" {
		t.Errorf("target = %q, want pdx", fetched.Status.PlannedFailover.Target)
	}
	if fetched.Status.PlannedFailover.SourcePrimary != "iad" {
		t.Errorf("source = %q, want iad", fetched.Status.PlannedFailover.SourcePrimary)
	}
	if _, ok := fetched.Annotations[PlannedFailoverAnnotation]; ok {
		t.Error("annotation should be cleared after accept")
	}
	if mlw := fetched.Status.PlannedFailover.MaxLagWait; mlw == nil || mlw.Duration != defaultPlannedFailoverMaxLagWait {
		t.Errorf("maxLagWait = %v, want default %s", mlw, defaultPlannedFailoverMaxLagWait)
	}
	if dt := fetched.Status.PlannedFailover.DrainTimeout; dt == nil || dt.Duration != defaultPlannedFailoverDrainTimeout {
		t.Errorf("drainTimeout = %v, want default %s", dt, defaultPlannedFailoverDrainTimeout)
	}
}

func TestReconcilePlannedFailover_RejectUnknownSite(t *testing.T) {
	fg := plannedFailoverFG("sfo")
	r, _ := newReconciler(fg)
	if _, err := r.reconcilePlannedFailover(context.Background(), fg); err != nil {
		t.Fatalf("unexpected err: %v", err)
	}
	fetched := fetchFG(t, r, fgNN(fg))
	if fetched.Status.PlannedFailover == nil ||
		fetched.Status.PlannedFailover.Phase != v1alpha1.PlannedFailoverPhaseFailed {
		t.Fatalf("expected terminal Failed, got %+v", fetched.Status.PlannedFailover)
	}
	if fetched.Status.PlannedFailover.Reason != "UnknownSite" {
		t.Errorf("reason = %q, want UnknownSite", fetched.Status.PlannedFailover.Reason)
	}
	if _, ok := fetched.Annotations[PlannedFailoverAnnotation]; ok {
		t.Error("annotation should be cleared after reject")
	}
}

// --- cooldown reject vs defer -----------------------------------------

func TestReconcilePlannedFailover_CooldownRejectDefault(t *testing.T) {
	fg := plannedFailoverFG("pdx")
	// Anti-flap cooldown still active.
	recent := metav1.NewTime(time.Now().Add(-2 * time.Minute))
	fg.Status.LastFailover = &recent

	r, _ := newReconciler(fg)
	if _, err := r.reconcilePlannedFailover(context.Background(), fg); err != nil {
		t.Fatalf("unexpected err: %v", err)
	}
	fetched := fetchFG(t, r, fgNN(fg))
	pf := fetched.Status.PlannedFailover
	if pf == nil || pf.Phase != v1alpha1.PlannedFailoverPhaseFailed {
		t.Fatalf("expected Failed under default reject policy, got %+v", pf)
	}
	if pf.Reason != "CooldownActive" {
		t.Errorf("reason = %q, want CooldownActive", pf.Reason)
	}
	if _, ok := fetched.Annotations[PlannedFailoverAnnotation]; ok {
		t.Error("annotation should be cleared under reject policy")
	}
}

func TestReconcilePlannedFailover_CooldownDeferKeepsAnnotation(t *testing.T) {
	fg := plannedFailoverFG("pdx")
	fg.Spec.PlannedFailover = &v1alpha1.PlannedFailoverSpec{
		OnCooldown: PlannedFailoverOnCooldownDefer,
	}
	recent := metav1.NewTime(time.Now().Add(-2 * time.Minute))
	fg.Status.LastFailover = &recent

	r, _ := newReconciler(fg)
	d, err := r.reconcilePlannedFailover(context.Background(), fg)
	if err != nil {
		t.Fatalf("unexpected err: %v", err)
	}
	if d <= 0 {
		t.Errorf("expected positive requeue toward cooldown expiry, got %s", d)
	}
	fetched := fetchFG(t, r, fgNN(fg))
	pf := fetched.Status.PlannedFailover
	if pf == nil || pf.Phase != v1alpha1.PlannedFailoverPhaseDeferred {
		t.Fatalf("expected Deferred, got %+v", pf)
	}
	if pf.Target != "pdx" {
		t.Errorf("target = %q, want pdx", pf.Target)
	}
	if pf.RetryAfter == nil {
		t.Error("expected RetryAfter populated")
	}
	if _, ok := fetched.Annotations[PlannedFailoverAnnotation]; !ok {
		t.Error("annotation should be retained under defer policy")
	}
}

func TestReconcilePlannedFailover_DeferredAdvancesWhenCooldownClears(t *testing.T) {
	fg := plannedFailoverFG("pdx")
	fg.Spec.PlannedFailover = &v1alpha1.PlannedFailoverSpec{
		OnCooldown: PlannedFailoverOnCooldownDefer,
	}
	// Cooldown elapsed 1 minute ago (failover was 6m ago, cooldown 5m).
	old := metav1.NewTime(time.Now().Add(-6 * time.Minute))
	fg.Status.LastFailover = &old
	// Already in Deferred phase (previous reconcile stamped it).
	now := metav1.Now()
	start := metav1.NewTime(now.Add(-3 * time.Minute))
	retry := metav1.NewTime(now.Add(-1 * time.Minute))
	fg.Status.PlannedFailover = &v1alpha1.PlannedFailoverStatus{
		Phase:         v1alpha1.PlannedFailoverPhaseDeferred,
		Target:        "pdx",
		SourcePrimary: "iad",
		StartTime:     &start,
		RetryAfter:    &retry,
		Reason:        "CooldownActive",
	}

	r, _ := newReconciler(fg)
	if _, err := r.reconcilePlannedFailover(context.Background(), fg); err != nil {
		t.Fatalf("unexpected err: %v", err)
	}
	fetched := fetchFG(t, r, fgNN(fg))
	pf := fetched.Status.PlannedFailover
	if pf == nil || pf.Phase != v1alpha1.PlannedFailoverPhasePending {
		t.Fatalf("expected Pending after cooldown cleared, got %+v", pf)
	}
	// StartTime should survive the deferred→pending promotion. Compare
	// at second precision because metav1.Time round-trips through RFC
	// 3339 on the patch path and drops sub-second bits.
	if pf.StartTime == nil || pf.StartTime.Unix() != start.Unix() {
		t.Errorf("StartTime not preserved across deferred→pending transition: got %v want %v", pf.StartTime, start)
	}
	if _, ok := fetched.Annotations[PlannedFailoverAnnotation]; ok {
		t.Error("annotation should be cleared once deferred request advances")
	}
}

func TestReconcilePlannedFailover_DeferredAnnotationRemovedIsCancel(t *testing.T) {
	fg := plannedFailoverFG("")
	start := metav1.Now()
	retry := metav1.NewTime(start.Add(3 * time.Minute))
	fg.Status.PlannedFailover = &v1alpha1.PlannedFailoverStatus{
		Phase:         v1alpha1.PlannedFailoverPhaseDeferred,
		Target:        "pdx",
		SourcePrimary: "iad",
		StartTime:     &start,
		RetryAfter:    &retry,
	}

	r, _ := newReconciler(fg)
	if _, err := r.reconcilePlannedFailover(context.Background(), fg); err != nil {
		t.Fatalf("unexpected err: %v", err)
	}
	fetched := fetchFG(t, r, fgNN(fg))
	pf := fetched.Status.PlannedFailover
	if pf == nil || pf.Phase != v1alpha1.PlannedFailoverPhaseFailed {
		t.Fatalf("expected Failed{Cancelled} when annotation removed, got %+v", pf)
	}
	if pf.Reason != "Cancelled" {
		t.Errorf("reason = %q, want Cancelled", pf.Reason)
	}
}

// --- state machine phases ---------------------------------------------

func TestReconcilePlannedFailover_DrainingWithoutSourceStampsSourceCrashed(t *testing.T) {
	fg := plannedFailoverFG("")
	now := metav1.Now()
	fg.Status.ActiveSite = "" // simulate source crashed before Draining entered
	fg.Status.PlannedFailover = &v1alpha1.PlannedFailoverStatus{
		Phase:         v1alpha1.PlannedFailoverPhaseDraining,
		Target:        "pdx",
		SourcePrimary: "", // lost
		StartTime:     &now,
	}

	r, _ := newReconciler(fg)
	if _, err := r.reconcilePlannedFailover(context.Background(), fg); err != nil {
		t.Fatalf("unexpected err: %v", err)
	}
	fetched := fetchFG(t, r, fgNN(fg))
	pf := fetched.Status.PlannedFailover
	if pf == nil || pf.Phase != v1alpha1.PlannedFailoverPhaseFailed {
		t.Fatalf("expected Failed, got %+v", pf)
	}
	if pf.Reason != "SourceCrashed" {
		t.Errorf("reason = %q, want SourceCrashed", pf.Reason)
	}
}

func TestReconcilePlannedFailover_PendingAdvancesToDraining(t *testing.T) {
	fg := plannedFailoverFG("")
	start := metav1.NewTime(time.Now().Add(-2 * time.Second))
	fg.Status.PlannedFailover = &v1alpha1.PlannedFailoverStatus{
		Phase:         v1alpha1.PlannedFailoverPhasePending,
		Target:        "pdx",
		SourcePrimary: "iad",
		StartTime:     &start,
		MaxLagWait:    &metav1.Duration{Duration: 30 * time.Second},
	}

	r, _ := newReconciler(fg)
	d, err := r.reconcilePlannedFailover(context.Background(), fg)
	if err != nil {
		t.Fatalf("unexpected err: %v", err)
	}
	if d <= 0 {
		t.Fatalf("expected positive requeue, got %s", d)
	}
	fetched := fetchFG(t, r, fgNN(fg))
	pf := fetched.Status.PlannedFailover
	if pf == nil || pf.Phase != v1alpha1.PlannedFailoverPhaseDraining {
		t.Fatalf("expected Draining, got %+v", pf)
	}
	if pf.Message == "" || !strings.Contains(pf.Message, "fencing source primary") {
		t.Fatalf("expected draining message, got %q", pf.Message)
	}
}

func TestReconcilePlannedFailover_ValidatingAlreadyActiveDoesNotPanic(t *testing.T) {
	fg := plannedFailoverFG("")
	fg.Status.ActiveSite = "pdx"
	start := metav1.NewTime(time.Now().Add(-2 * time.Second))
	fg.Status.PlannedFailover = &v1alpha1.PlannedFailoverStatus{
		Phase:         v1alpha1.PlannedFailoverPhaseValidating,
		Target:        "pdx",
		SourcePrimary: "iad",
		StartTime:     &start,
	}

	r, _ := newReconciler(fg)
	if _, err := r.reconcilePlannedFailover(context.Background(), fg); err != nil {
		t.Fatalf("unexpected err: %v", err)
	}
	fetched := fetchFG(t, r, fgNN(fg))
	pf := fetched.Status.PlannedFailover
	if pf == nil || pf.Phase != v1alpha1.PlannedFailoverPhaseFailed {
		t.Fatalf("expected Failed terminal status, got %+v", pf)
	}
	if pf.Reason != "AlreadyActive" {
		t.Fatalf("expected AlreadyActive, got %q", pf.Reason)
	}
}

func TestReconcilePlannedFailover_ResumingStampsSucceeded(t *testing.T) {
	fg := plannedFailoverFG("")
	start := metav1.NewTime(time.Now().Add(-10 * time.Second))
	fg.Status.PlannedFailover = &v1alpha1.PlannedFailoverStatus{
		Phase:                 v1alpha1.PlannedFailoverPhaseResuming,
		Target:                "pdx",
		SourcePrimary:         "iad",
		SourceGtidAtFence:     "abc:1-10",
		TargetGtidAtPromotion: "abc:1-10",
		StartTime:             &start,
	}

	r, _ := newReconciler(fg)
	if _, err := r.reconcilePlannedFailover(context.Background(), fg); err != nil {
		t.Fatalf("unexpected err: %v", err)
	}
	fetched := fetchFG(t, r, fgNN(fg))
	pf := fetched.Status.PlannedFailover
	if pf == nil || pf.Phase != v1alpha1.PlannedFailoverPhaseSucceeded {
		t.Fatalf("expected Succeeded, got %+v", pf)
	}
	if pf.TransactionsLost == nil || *pf.TransactionsLost != 0 {
		t.Errorf("expected transactionsLost=0 for aligned GTIDs, got %v", pf.TransactionsLost)
	}
	if pf.CompletionTime == nil {
		t.Error("expected CompletionTime populated")
	}
	if fetched.Status.LastFailoverTarget != "pdx" {
		t.Errorf("lastFailoverTarget = %q, want pdx", fetched.Status.LastFailoverTarget)
	}
	if fetched.Status.ActiveSite != "pdx" {
		t.Errorf("activeSite = %q, want pdx", fetched.Status.ActiveSite)
	}
	if fetched.Status.LastFailover == nil {
		t.Error("expected lastFailover stamped")
	}
	if fetched.Status.PromotionGtidExecuted != "abc:1-10" {
		t.Errorf("promotionGtidExecuted = %q, want abc:1-10", fetched.Status.PromotionGtidExecuted)
	}
}

// --- stale annotation during active run -------------------------------

func TestReconcilePlannedFailover_StaleAnnotationDuringInFlightCleared(t *testing.T) {
	fg := plannedFailoverFG("pdx") // second annotation arrives
	start := metav1.Now()
	fg.Status.PlannedFailover = &v1alpha1.PlannedFailoverStatus{
		Phase:         v1alpha1.PlannedFailoverPhaseWaitingForLag,
		Target:        "pdx",
		SourcePrimary: "iad",
		StartTime:     &start,
	}

	r, _ := newReconciler(fg)
	// Runner is nil; WaitingForLag will then fail with InternalError.
	// We care about the annotation-clearing step which runs before dispatch.
	_, _ = r.reconcilePlannedFailover(context.Background(), fg)
	fetched := fetchFG(t, r, fgNN(fg))
	if _, ok := fetched.Annotations[PlannedFailoverAnnotation]; ok {
		t.Error("stale duplicate annotation should be cleared while an active run is in flight")
	}
}

// --- deferred phase: annotation edited to already-active target --------

func TestReconcilePlannedFailover_DeferredAnnotationEditedToActiveIsSkipped(t *testing.T) {
	fg := plannedFailoverFG("iad") // user changed annotation while deferred
	fg.Spec.PlannedFailover = &v1alpha1.PlannedFailoverSpec{
		OnCooldown: PlannedFailoverOnCooldownDefer,
	}
	now := metav1.Now()
	start := metav1.NewTime(now.Add(-1 * time.Minute))
	fg.Status.PlannedFailover = &v1alpha1.PlannedFailoverStatus{
		Phase:         v1alpha1.PlannedFailoverPhaseDeferred,
		Target:        "pdx",
		SourcePrimary: "iad",
		StartTime:     &start,
	}

	r, _ := newReconciler(fg)
	if _, err := r.reconcilePlannedFailover(context.Background(), fg); err != nil {
		t.Fatalf("unexpected err: %v", err)
	}
	fetched := fetchFG(t, r, fgNN(fg))
	pf := fetched.Status.PlannedFailover
	if pf == nil || pf.Phase != v1alpha1.PlannedFailoverPhaseFailed {
		t.Fatalf("expected terminal Failed (AlreadyActive), got %+v", pf)
	}
	if pf.Reason != "AlreadyActive" {
		t.Errorf("reason = %q, want AlreadyActive", pf.Reason)
	}
	if _, ok := fetched.Annotations[PlannedFailoverAnnotation]; ok {
		t.Error("annotation should be cleared once deferred target is already active")
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
	if got := effectiveOnCooldown(fg); got != PlannedFailoverOnCooldownReject {
		t.Errorf("default: got %q, want reject", got)
	}
	fg.Spec.PlannedFailover = &v1alpha1.PlannedFailoverSpec{OnCooldown: "defer"}
	if got := effectiveOnCooldown(fg); got != PlannedFailoverOnCooldownDefer {
		t.Errorf("defer: got %q, want defer", got)
	}
	// Malformed values fall back to reject (defence against typos).
	fg.Spec.PlannedFailover.OnCooldown = "skip"
	if got := effectiveOnCooldown(fg); got != PlannedFailoverOnCooldownReject {
		t.Errorf("typo: got %q, want reject fallback", got)
	}
}

func TestCooldownRetryAfter(t *testing.T) {
	fg := &v1alpha1.MysqlFailoverGroup{}
	if !cooldownRetryAfter(fg).IsZero() {
		t.Error("no lastFailover: want zero time")
	}
	last := metav1.NewTime(time.Date(2026, 4, 20, 12, 0, 0, 0, time.UTC))
	fg.Status.LastFailover = &last
	fg.Spec.FailoverCooldown = &metav1.Duration{Duration: 10 * time.Minute}
	want := last.Time.Add(10 * time.Minute)
	if got := cooldownRetryAfter(fg); !got.Equal(want) {
		t.Errorf("got %v, want %v", got, want)
	}
}

// Verify that effectiveDrainTimeoutFromStatus prefers status over the
// package default — important across operator upgrades that change the
// default without a re-arm.
func TestEffectiveDrainTimeoutFromStatus(t *testing.T) {
	if got := effectiveDrainTimeoutFromStatus(nil); got != defaultPlannedFailoverDrainTimeout {
		t.Errorf("nil status: got %s want default", got)
	}
	s := &v1alpha1.PlannedFailoverStatus{}
	if got := effectiveDrainTimeoutFromStatus(s); got != defaultPlannedFailoverDrainTimeout {
		t.Errorf("empty status: got %s want default", got)
	}
	s.DrainTimeout = &metav1.Duration{Duration: 15 * time.Second}
	if got := effectiveDrainTimeoutFromStatus(s); got != 15*time.Second {
		t.Errorf("override: got %s want 15s", got)
	}
}

// Check that the Pending stamp message and event include the expected
// target, so operators watching events can correlate runs.
func TestAccept_EmitsPlannedFailoverStartedEvent(t *testing.T) {
	fg := plannedFailoverFG("pdx:maxLagWait=30s")
	r, _ := newReconciler(fg)
	if _, err := r.reconcilePlannedFailover(context.Background(), fg); err != nil {
		t.Fatalf("unexpected err: %v", err)
	}
	rec, ok := r.Recorder.(*record.FakeRecorder)
	if !ok {
		t.Fatalf("expected FakeRecorder, got %T", r.Recorder)
	}
	select {
	case ev := <-rec.Events:
		if !strings.Contains(ev, "PlannedFailoverStarted") || !strings.Contains(ev, "pdx") {
			t.Errorf("unexpected event: %s", ev)
		}
	case <-time.After(time.Second):
		t.Error("no event emitted")
	}
}
