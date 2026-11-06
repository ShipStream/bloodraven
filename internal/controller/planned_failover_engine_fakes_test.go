package controller

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"

	v1alpha1 "github.com/shipstream/bloodraven/api/v1alpha1"
	"github.com/shipstream/bloodraven/internal/clock"
)

// ---------- topology fake ----------

// pfFakeTopology is a scripted PlannedFailoverTopology. It models the claim
// slot (one run, Run or Hold mode) and records every call. Each behaviour
// defaults to the happy path.
type pfFakeTopology struct {
	mu sync.Mutex

	heldRun  string
	holdMode bool
	// hydrateHold, when set, is what EnsureClaim hydrates into an empty
	// slot (the startManager hydration of a hold).
	hydrateHold string

	claim      func(req PlannedClaimRequest) (PlannedClaimDecision, error)
	ensure     func(ref PlannedRunRef) (PlannedClaimDecision, error)
	reprove    func(ref PlannedRunRef) (ReproveResult, error)
	fence      func(ref PlannedRunRef) (FenceProof, error)
	prove      func(ref PlannedRunRef) (FenceProof, error)
	kill       func(ref PlannedRunRef) (int32, error)
	probe      func(ref PlannedRunRef) (LagProbe, error)
	uuid       func(ref PlannedRunRef) (string, error)
	promote    func(ref PlannedRunRef) (PromoteOutcome, error)
	record     func(ref PlannedRunRef) error
	dns        func(ref PlannedRunRef) (v1alpha1.PlannedFailoverDNSState, error)
	rollback   func(ref PlannedRunRef) (RollbackResult, error)
	needsProof bool // the next mutating call returns errReproveRequired
	// probeUnguarded makes ProbeLag a plain read, as in production (the
	// facade's ProbeLag needs no claim).
	probeUnguarded bool
	authority      *PlannedAuthority // ManagerAuthority's answer; nil: no manager
	// onCtx, when set, sees the ctx of every ProbeLag and Promote call (the
	// pass bound the engine applied).
	onCtx func(op string, ctx context.Context)

	calls    []string
	releases []string // "<runId>:<why>"
	named    []string // "source:<site>" / "target:<site>"
}

func newPFFakeTopology() *pfFakeTopology { return &pfFakeTopology{} }

func (f *pfFakeTopology) note(call string) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.calls = append(f.calls, call)
}

func (f *pfFakeTopology) count(call string) int {
	f.mu.Lock()
	defer f.mu.Unlock()
	n := 0
	for _, c := range f.calls {
		if c == call {
			n++
		}
	}
	return n
}

func (f *pfFakeTopology) releasesCopy() []string {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]string(nil), f.releases...)
}

func (f *pfFakeTopology) held() (string, bool) {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.heldRun, f.holdMode
}

func (f *pfFakeTopology) setHeld(runID string, hold bool) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.heldRun, f.holdMode = runID, hold
}

// guard is the facade's §3.3 check for mutating calls.
func (f *pfFakeTopology) guard(ref PlannedRunRef) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.heldRun != ref.RunID || f.holdMode {
		return errClaimLost
	}
	if f.needsProof {
		return errReproveRequired
	}
	return nil
}

func (f *pfFakeTopology) ClaimPlannedFailover(_ context.Context, _ types.NamespacedName, req PlannedClaimRequest) (PlannedClaimDecision, error) {
	f.note("claim")
	if f.claim != nil {
		d, err := f.claim(req)
		if err == nil && d.Kind == ClaimGranted {
			f.setHeld(req.RunID, false)
		}
		return d, err
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.heldRun != "" && f.heldRun != req.RunID {
		return PlannedClaimDecision{Kind: ClaimWait, WaitReason: v1alpha1.PlannedFailoverWaitClaimHeld}, nil
	}
	f.heldRun = req.RunID
	return PlannedClaimDecision{Kind: ClaimGranted, Source: "iad", SourceServerUUID: "uuid-iad", Baseline: req.DurableBaseline}, nil
}

func (f *pfFakeTopology) EnsureClaim(_ context.Context, ref PlannedRunRef, _ *v1alpha1.MysqlFailoverGroup) (PlannedClaimDecision, error) {
	f.note("ensure")
	if f.ensure != nil {
		return f.ensure(ref)
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.heldRun == "" {
		if f.hydrateHold != "" {
			f.heldRun, f.holdMode = f.hydrateHold, true
			return PlannedClaimDecision{Kind: ClaimReject, RejectReason: v1alpha1.PlannedFailoverReasonManualRecoveryPending}, nil
		}
		f.heldRun = ref.RunID
	}
	if f.heldRun != ref.RunID {
		return PlannedClaimDecision{Kind: ClaimWait, WaitReason: v1alpha1.PlannedFailoverWaitClaimHeld}, nil
	}
	return PlannedClaimDecision{Kind: ClaimGranted, Source: ref.Source}, nil
}

func (f *pfFakeTopology) ReleasePlannedFailoverClaim(_ types.NamespacedName, runID string, why ClaimReleaseReason) bool {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.heldRun != runID || runID == "" {
		return false
	}
	if f.holdMode != (why == ReleaseAck || why == ReleaseOrphanHold) {
		return false
	}
	f.heldRun, f.holdMode = "", false
	f.releases = append(f.releases, runID+":"+string(why))
	return true
}

func (f *pfFakeTopology) ConvertToHold(_ types.NamespacedName, runID string, _ ...string) bool {
	f.mu.Lock()
	defer f.mu.Unlock()
	if runID == "" {
		return false // the fake cannot hold a run-less hold
	}
	if f.heldRun == "" {
		f.heldRun = runID
	}
	if f.heldRun != runID {
		return false
	}
	if !f.holdMode {
		f.holdMode = true
		f.releases = append(f.releases, runID+":"+string(ReleaseToHold))
	}
	return true
}

func (f *pfFakeTopology) ManagerAuthority(types.NamespacedName) (PlannedAuthority, bool) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.authority == nil {
		return PlannedAuthority{}, false
	}
	return *f.authority, true
}

func (f *pfFakeTopology) HeldRunClaim(types.NamespacedName) (string, bool) {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.heldRun, f.heldRun != "" && !f.holdMode
}

func (f *pfFakeTopology) HeldHoldClaim(types.NamespacedName) (string, bool) {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.heldRun, f.heldRun != "" && f.holdMode
}

func (f *pfFakeTopology) PlannedClaimHeld(types.NamespacedName) bool {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.heldRun != ""
}

func (f *pfFakeTopology) CheckRunClaim(ref PlannedRunRef) error {
	f.note("checkClaim")
	return f.guard(ref)
}

func (f *pfFakeTopology) Reprove(_ context.Context, ref PlannedRunRef, _ *v1alpha1.MysqlFailoverGroup) (ReproveResult, error) {
	f.note("reprove")
	f.mu.Lock()
	f.needsProof = false
	f.mu.Unlock()
	if f.reprove != nil {
		return f.reprove(ref)
	}
	return ReproveResult{OK: true}, nil
}

func (f *pfFakeTopology) Fence(_ context.Context, ref PlannedRunRef, _ string) (FenceProof, error) {
	if err := f.guard(ref); err != nil {
		return FenceProof{}, err
	}
	f.note("fence")
	if f.fence != nil {
		return f.fence(ref)
	}
	return FenceProof{Kind: FenceApplied, SourceServerUUID: "uuid-iad", GtidSet: "uuid-iad:1-10", Attribution: FenceByMemo}, nil
}

func (f *pfFakeTopology) ProveFence(_ context.Context, ref PlannedRunRef, _ v1alpha1.PlannedFailoverFenceRecord) (FenceProof, error) {
	if err := f.guard(ref); err != nil {
		return FenceProof{}, err
	}
	f.note("proveFence")
	if f.prove != nil {
		return f.prove(ref)
	}
	return FenceProof{Kind: FenceApplied, SourceServerUUID: "uuid-iad", GtidSet: "uuid-iad:1-10", Attribution: FenceByMemo}, nil
}

func (f *pfFakeTopology) KillAppConnections(_ context.Context, ref PlannedRunRef) (int32, error) {
	if err := f.guard(ref); err != nil {
		return 0, err
	}
	f.note("kill")
	if f.kill != nil {
		return f.kill(ref)
	}
	return 0, nil
}

func (f *pfFakeTopology) ProbeLag(ctx context.Context, ref PlannedRunRef, _ string) (LagProbe, error) {
	if f.onCtx != nil {
		f.onCtx("probe", ctx)
	}
	if !f.probeUnguarded {
		if err := f.guard(ref); err != nil {
			return LagProbe{}, err
		}
	}
	f.note("probe")
	if f.probe != nil {
		return f.probe(ref)
	}
	return LagProbe{SourceReachable: true, TargetReachable: true, CaughtUp: true}, nil
}

func (f *pfFakeTopology) ReadTargetServerUUID(_ context.Context, ref PlannedRunRef) (string, error) {
	if err := f.guard(ref); err != nil {
		return "", err
	}
	f.note("targetUUID")
	if f.uuid != nil {
		return f.uuid(ref)
	}
	return "uuid-pdx", nil
}

func (f *pfFakeTopology) Promote(ctx context.Context, ref PlannedRunRef, _ *v1alpha1.PlannedFailoverStatus) (PromoteOutcome, error) {
	if f.onCtx != nil {
		f.onCtx("promote", ctx)
	}
	if err := f.guard(ref); err != nil {
		return PromoteOutcome{}, err
	}
	f.note("promote")
	if f.promote != nil {
		return f.promote(ref)
	}
	return PromoteOutcome{Kind: PromoteConfirmed, Stage: PromotionStageWritable, TargetReachable: true, MayHaveWritten: true}, nil
}

func (f *pfFakeTopology) RecordPlannedPromotion(_ context.Context, ref PlannedRunRef, _ time.Time, _ string) error {
	if err := f.guard(ref); err != nil {
		return err
	}
	f.note("record")
	if f.record != nil {
		return f.record(ref)
	}
	return nil
}

func (f *pfFakeTopology) ApplyDNS(_ context.Context, ref PlannedRunRef) (v1alpha1.PlannedFailoverDNSState, error) {
	if err := f.guard(ref); err != nil {
		return "", err
	}
	f.note("dns")
	if f.dns != nil {
		return f.dns(ref)
	}
	return v1alpha1.PlannedFailoverDNSApplied, nil
}

func (f *pfFakeTopology) NameUnconfirmedTarget(ref PlannedRunRef) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.named = append(f.named, "target:"+ref.Target)
	return nil
}

func (f *pfFakeTopology) NameFencedSource(ref PlannedRunRef) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.named = append(f.named, "source:"+ref.Source)
	return nil
}

func (f *pfFakeTopology) Rollback(_ context.Context, ref PlannedRunRef, _ *v1alpha1.PlannedFailoverStatus) (RollbackResult, error) {
	if err := f.guard(ref); err != nil {
		return RollbackResult{}, err
	}
	f.note("rollback")
	if f.rollback != nil {
		return f.rollback(ref)
	}
	return RollbackResult{Outcome: v1alpha1.PlannedFailoverOutcomeRolledBack}, nil
}

func (f *pfFakeTopology) ActiveSite(types.NamespacedName) (string, bool) { return "", false }

var _ PlannedFailoverTopology = (*pfFakeTopology)(nil)

// ---------- sink fake ----------

type pfFakeSink struct {
	mu        sync.Mutex
	events    []string // "<type> <reason>: <message>"
	terminals []v1alpha1.PlannedFailoverResult
	deferred  []v1alpha1.PlannedFailoverDeferReason
	lagWaits  int
}

func (s *pfFakeSink) Event(_ *v1alpha1.MysqlFailoverGroup, eventType, reason, message string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.events = append(s.events, eventType+" "+reason+": "+message)
}

func (s *pfFakeSink) RunTerminal(_ types.NamespacedName, pf *v1alpha1.PlannedFailoverStatus) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if pf != nil && pf.Result != nil {
		s.terminals = append(s.terminals, *pf.Result)
	}
}

func (s *pfFakeSink) LagWaitObserved(types.NamespacedName, string, time.Duration) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.lagWaits++
}

func (s *pfFakeSink) Deferred(_ types.NamespacedName, reason v1alpha1.PlannedFailoverDeferReason) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.deferred = append(s.deferred, reason)
}

func (s *pfFakeSink) eventCount(reason string) int {
	s.mu.Lock()
	defer s.mu.Unlock()
	n := 0
	for _, e := range s.events {
		var typ, r string
		if _, err := fmt.Sscanf(e, "%s %s", &typ, &r); err == nil && (r == reason+":") {
			n++
		}
	}
	return n
}

// ---------- store faults (an interceptor over the fake client) ----------

// pfWriteFault scripts one status write.
type pfWriteFault int

const (
	pfWriteOK            pfWriteFault = iota
	pfWriteConflict                   // 409, nothing committed
	pfWriteCommitTimeout              // committed, then the response is lost
	pfWriteDropTimeout                // not committed, the response is a timeout
	pfWriteForbidden                  // 403, nothing committed
)

// pfFaults is the interceptor's script: status writes pop statusFaults
// (then statusDenied applies); metadata patches pop metaFaults (then
// metaDenied applies).
type pfFaults struct {
	mu           sync.Mutex
	statusFaults []pfWriteFault
	statusDenied bool
	// statusDefault, when set, applies to every status write once
	// statusFaults is empty (statusDenied wins).
	statusDefault pfWriteFault
	metaFaults    []pfWriteFault
	metaDenied    bool
	// denyMetaKey, when set, denies only patches that touch this annotation.
	denyMetaKey string
	statusCalls int
}

var errPFTimeout = errors.New("pf test: request timed out")

func (f *pfFaults) nextStatus() pfWriteFault {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.statusCalls++
	if len(f.statusFaults) > 0 {
		x := f.statusFaults[0]
		f.statusFaults = f.statusFaults[1:]
		return x
	}
	if f.statusDenied {
		return pfWriteForbidden
	}
	return f.statusDefault
}

func (f *pfFaults) nextMeta(obj client.Object, before map[string]string) pfWriteFault {
	f.mu.Lock()
	defer f.mu.Unlock()
	if len(f.metaFaults) > 0 {
		x := f.metaFaults[0]
		f.metaFaults = f.metaFaults[1:]
		return x
	}
	if f.metaDenied {
		return pfWriteForbidden
	}
	if f.denyMetaKey != "" {
		after := obj.GetAnnotations()
		if before[f.denyMetaKey] != after[f.denyMetaKey] {
			return pfWriteForbidden
		}
	}
	return pfWriteOK
}

var pfGroupResource = schema.GroupResource{Group: "shipstream.io", Resource: "mysqlfailovergroups"}

func pfApplyFault(fault pfWriteFault, name string, do func() error) error {
	switch fault {
	case pfWriteConflict:
		return apierrors.NewConflict(pfGroupResource, name, errors.New("pf test conflict"))
	case pfWriteForbidden:
		return apierrors.NewForbidden(pfGroupResource, name, errors.New("pf test forbidden"))
	case pfWriteDropTimeout:
		return errPFTimeout
	case pfWriteCommitTimeout:
		if err := do(); err != nil {
			return err
		}
		return errPFTimeout
	}
	return do()
}

// newPFFakeClient builds a fake client over objs with the fault script.
func newPFFakeClient(faults *pfFaults, objs ...client.Object) client.WithWatch {
	return fake.NewClientBuilder().WithScheme(testScheme()).
		WithStatusSubresource(&v1alpha1.MysqlFailoverGroup{}).
		WithObjects(objs...).
		WithInterceptorFuncs(interceptor.Funcs{
			SubResourceUpdate: func(ctx context.Context, c client.Client, sub string, obj client.Object, opts ...client.SubResourceUpdateOption) error {
				return pfApplyFault(faults.nextStatus(), obj.GetName(), func() error {
					return c.SubResource(sub).Update(ctx, obj, opts...)
				})
			},
			Patch: func(ctx context.Context, c client.WithWatch, obj client.Object, patch client.Patch, opts ...client.PatchOption) error {
				var before map[string]string
				var cur v1alpha1.MysqlFailoverGroup
				if err := c.Get(ctx, client.ObjectKeyFromObject(obj), &cur); err == nil {
					before = cur.GetAnnotations()
				}
				return pfApplyFault(faults.nextMeta(obj, before), obj.GetName(), func() error {
					return c.Patch(ctx, obj, patch, opts...)
				})
			},
		}).
		Build()
}

// ---------- harness ----------

// pfEngineHarness drives the production engine and store over a fake
// client, with a scripted topology and a fake clock.
type pfEngineHarness struct {
	t      *testing.T
	nn     types.NamespacedName
	c      client.WithWatch
	faults *pfFaults
	clk    *clock.FakeClock
	topo   *pfFakeTopology
	sink   *pfFakeSink
	e      *PlannedFailoverEngine
}

func newPFEngineHarness(t *testing.T, fg *v1alpha1.MysqlFailoverGroup) *pfEngineHarness {
	t.Helper()
	h := &pfEngineHarness{
		t:      t,
		nn:     types.NamespacedName{Namespace: fg.Namespace, Name: fg.Name},
		faults: &pfFaults{},
		clk:    clock.NewFakeClock(time.Date(2026, 11, 4, 12, 0, 0, 0, time.UTC)),
		topo:   newPFFakeTopology(),
		sink:   &pfFakeSink{},
	}
	h.c = newPFFakeClient(h.faults, fg)
	h.restart()
	return h
}

// restart replaces the engine (a new operator process): the memo is gone,
// the durable records and the topology slot stay.
func (h *pfEngineHarness) restart() {
	h.e = NewPlannedFailoverEngine(newAPIPlannedFailoverStore(h.c, h.c, h.clk), h.topo, h.sink, h.clk, SimHooks{})
}

func (h *pfEngineHarness) step() time.Duration {
	h.t.Helper()
	d, err := h.e.Step(context.Background(), h.nn)
	if err != nil {
		h.t.Fatalf("Step: %v", err)
	}
	return d
}

// stepUntil steps (advancing the clock by each requeue) until done holds,
// at most n times.
func (h *pfEngineHarness) stepUntil(n int, done func(*v1alpha1.MysqlFailoverGroup) bool) {
	h.t.Helper()
	for i := 0; i < n; i++ {
		if done(h.fg()) {
			return
		}
		d := h.step()
		if d <= 0 {
			d = time.Second
		}
		h.clk.Advance(d)
	}
	if !done(h.fg()) {
		h.t.Fatalf("condition not reached after %d steps; block = %+v", n, h.fg().Status.PlannedFailover)
	}
}

func (h *pfEngineHarness) fg() *v1alpha1.MysqlFailoverGroup {
	h.t.Helper()
	var fg v1alpha1.MysqlFailoverGroup
	if err := h.c.Get(context.Background(), h.nn, &fg); err != nil {
		h.t.Fatalf("get: %v", err)
	}
	return &fg
}

func (h *pfEngineHarness) pf() *v1alpha1.PlannedFailoverStatus {
	h.t.Helper()
	return h.fg().Status.PlannedFailover
}

// setBlock stores pf (and annotations, when non-nil) on the group.
func (h *pfEngineHarness) setBlock(pf *v1alpha1.PlannedFailoverStatus) {
	h.t.Helper()
	fg := h.fg()
	fg.Status.PlannedFailover = pf
	if err := h.c.Status().Update(context.Background(), fg); err != nil {
		h.t.Fatalf("seed block: %v", err)
	}
}

func (h *pfEngineHarness) setAnnotation(key, value string) {
	h.t.Helper()
	fg := h.fg()
	a := fg.GetAnnotations()
	if a == nil {
		a = map[string]string{}
	}
	if value == "" {
		delete(a, key)
	} else {
		a[key] = value
	}
	fg.SetAnnotations(a)
	if err := h.c.Update(context.Background(), fg); err != nil {
		h.t.Fatalf("set annotation: %v", err)
	}
}

// pfRunAt returns a block of run "run-1" in phase with the records a run in
// that phase carries.
func pfRunAt(phase v1alpha1.PlannedFailoverPhase, now time.Time) *v1alpha1.PlannedFailoverStatus {
	t := metav1.NewTime(now)
	deadline := metav1.NewTime(now.Add(plannedFailoverAdmissionTimeout))
	f := "uuid-iad:1-10"
	pf := &v1alpha1.PlannedFailoverStatus{
		RunID: "run-1", Revision: 1, Phase: phase, TargetSite: "pdx", StartTime: t,
		Settings: v1alpha1.PlannedFailoverSettings{MaxLagWait: metav1.Duration{Duration: time.Minute},
			DrainTimeout: metav1.Duration{Duration: 5 * time.Second}},
		Admission: &v1alpha1.PlannedFailoverAdmission{DeadlineTime: &deadline},
	}
	order := []v1alpha1.PlannedFailoverPhase{
		v1alpha1.PlannedFailoverPhaseFencing, v1alpha1.PlannedFailoverPhaseDraining, v1alpha1.PlannedFailoverPhaseWaitingForLag,
		v1alpha1.PlannedFailoverPhasePromoting, v1alpha1.PlannedFailoverPhasePublishing,
	}
	idx := -1
	for i, p := range order {
		if p == phase {
			idx = i
		}
	}
	if phase == v1alpha1.PlannedFailoverPhaseRollingBack {
		idx = 2
	}
	if idx >= 0 {
		pf.SourceSite = "iad"
		pf.ClaimTime = &t
		pf.ClaimBaseline = &v1alpha1.FailoverRecordRef{}
		pf.Fence = &v1alpha1.PlannedFailoverFenceRecord{State: v1alpha1.PlannedFailoverFenceIntended, IntentTime: t, SourceServerUUID: "uuid-iad"}
	}
	if idx >= 1 {
		pf.Fence.State = v1alpha1.PlannedFailoverFenceApplied
		pf.Fence.ApplyTime = &t
		pf.Fence.GtidSet = &f
		pf.Drain = &v1alpha1.PlannedFailoverDrainRecord{StartTime: t}
	}
	if idx >= 2 {
		pf.Drain.CompletionTime = &t
		pf.LagWait = &v1alpha1.PlannedFailoverLagWaitRecord{StartTime: t}
	}
	if idx >= 3 {
		pf.LagWait.CatchUpTime = &t
		pf.Promotion = &v1alpha1.PlannedFailoverPromotionRecord{State: v1alpha1.PlannedFailoverPromotionIntended, IntentTime: t, TargetServerUUID: "uuid-pdx"}
	}
	if idx >= 4 {
		pf.Promotion.State = v1alpha1.PlannedFailoverPromotionConfirmed
		pf.Promotion.ConfirmationTime = &t
		pf.DNS = &v1alpha1.PlannedFailoverDNSRecord{State: v1alpha1.PlannedFailoverDNSApplied, AttemptTime: t}
	}
	if phase == v1alpha1.PlannedFailoverPhaseRollingBack {
		pf.Rollback = &v1alpha1.PlannedFailoverRollbackRecord{Reason: v1alpha1.PlannedFailoverReasonLagTimeout, IntentTime: t}
	}
	return pf
}

// pfHarnessAt seeds a harness with run-1 in phase, its claim held by the
// topology fake.
func pfHarnessAt(t *testing.T, phase v1alpha1.PlannedFailoverPhase) *pfEngineHarness {
	t.Helper()
	h := newPFEngineHarness(t, plannedFailoverFG(""))
	h.setBlock(pfRunAt(phase, h.clk.Now()))
	if phase != v1alpha1.PlannedFailoverPhaseAdmitting && phase != v1alpha1.PlannedFailoverPhasePublishing {
		h.topo.setHeld("run-1", false)
	}
	return h
}
