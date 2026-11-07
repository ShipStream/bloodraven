//go:build envtest

package envtest

import (
	"context"
	"log/slog"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	apiextensionsv1 "k8s.io/apiextensions-apiserver/pkg/apis/apiextensions/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	apimeta "k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	"k8s.io/client-go/tools/record"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/envtest"

	v1alpha1 "github.com/shipstream/bloodraven/api/v1alpha1"
	"github.com/shipstream/bloodraven/internal/controller"
)

// pfEnvTopology is a happy-path PlannedFailoverTopology with a one-run slot.
type pfEnvTopology struct {
	mu       sync.Mutex
	held     string
	hold     bool
	promote  func() controller.PromoteOutcome
	calls    []string
	releases []string
}

func (f *pfEnvTopology) note(c string) { f.mu.Lock(); f.calls = append(f.calls, c); f.mu.Unlock() }

func (f *pfEnvTopology) ClaimPlannedFailover(_ context.Context, _ types.NamespacedName, req controller.PlannedClaimRequest) (controller.PlannedClaimDecision, error) {
	f.note("claim")
	f.mu.Lock()
	defer f.mu.Unlock()
	f.held = req.RunID
	return controller.PlannedClaimDecision{Kind: controller.ClaimGranted, Source: "dc1", SourceServerUUID: "uuid-dc1", Baseline: req.DurableBaseline}, nil
}
func (f *pfEnvTopology) EnsureClaim(_ context.Context, ref controller.PlannedRunRef, _ *v1alpha1.MysqlFailoverGroup) (controller.PlannedClaimDecision, error) {
	f.note("ensure")
	return controller.PlannedClaimDecision{Kind: controller.ClaimGranted, Source: ref.Source}, nil
}
func (f *pfEnvTopology) ReleasePlannedFailoverClaim(_ types.NamespacedName, runID string, why controller.ClaimReleaseReason) bool {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.held != runID || f.hold != (why == controller.ReleaseAck || why == controller.ReleaseOrphanHold) {
		return false
	}
	f.held, f.hold = "", false
	f.releases = append(f.releases, string(why))
	return true
}
func (f *pfEnvTopology) ConvertToHold(_ types.NamespacedName, runID string, _ ...string) bool {
	if runID == "" {
		return false
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	f.held, f.hold = runID, true
	return true
}
func (f *pfEnvTopology) ManagerAuthority(types.NamespacedName) (controller.PlannedAuthority, bool) {
	return controller.PlannedAuthority{}, false
}
func (f *pfEnvTopology) HeldRunClaim(types.NamespacedName) (string, bool) {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.held, f.held != "" && !f.hold
}
func (f *pfEnvTopology) HeldHoldClaim(types.NamespacedName) (string, bool) {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.held, f.held != "" && f.hold
}
func (f *pfEnvTopology) PlannedClaimHeld(types.NamespacedName) bool {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.held != ""
}
func (f *pfEnvTopology) CheckRunClaim(controller.PlannedRunRef) error { return nil }
func (f *pfEnvTopology) Reprove(context.Context, controller.PlannedRunRef, *v1alpha1.MysqlFailoverGroup) (controller.ReproveResult, error) {
	return controller.ReproveResult{OK: true}, nil
}
func (f *pfEnvTopology) Fence(context.Context, controller.PlannedRunRef, string) (controller.FenceProof, error) {
	f.note("fence")
	return controller.FenceProof{Kind: controller.FenceApplied, SourceServerUUID: "uuid-dc1", GtidSet: "uuid-dc1:1-5"}, nil
}
func (f *pfEnvTopology) ProveFence(context.Context, controller.PlannedRunRef, v1alpha1.PlannedFailoverFenceRecord) (controller.FenceProof, error) {
	return controller.FenceProof{Kind: controller.FenceApplied, SourceServerUUID: "uuid-dc1", GtidSet: "uuid-dc1:1-5"}, nil
}
func (f *pfEnvTopology) KillAppConnections(context.Context, controller.PlannedRunRef) (int32, error) {
	return 0, nil
}
func (f *pfEnvTopology) ProbeLag(context.Context, controller.PlannedRunRef, string) (controller.LagProbe, error) {
	return controller.LagProbe{SourceReachable: true, TargetReachable: true, CaughtUp: true}, nil
}
func (f *pfEnvTopology) ReadTargetServerUUID(context.Context, controller.PlannedRunRef) (string, error) {
	return "uuid-dc2", nil
}
func (f *pfEnvTopology) Promote(context.Context, controller.PlannedRunRef, *v1alpha1.PlannedFailoverStatus) (controller.PromoteOutcome, error) {
	f.note("promote")
	if f.promote != nil {
		return f.promote(), nil
	}
	return controller.PromoteOutcome{Kind: controller.PromoteConfirmed, TargetReachable: true, MayHaveWritten: true}, nil
}
func (f *pfEnvTopology) RecordPlannedPromotion(context.Context, controller.PlannedRunRef, time.Time, string) error {
	return nil
}
func (f *pfEnvTopology) ApplyDNS(context.Context, controller.PlannedRunRef) (v1alpha1.PlannedFailoverDNSState, error) {
	return v1alpha1.PlannedFailoverDNSApplied, nil
}
func (f *pfEnvTopology) NameUnconfirmedTarget(controller.PlannedRunRef) error { return nil }
func (f *pfEnvTopology) NameFencedSource(controller.PlannedRunRef) error      { return nil }
func (f *pfEnvTopology) Rollback(context.Context, controller.PlannedRunRef, *v1alpha1.PlannedFailoverStatus) (controller.RollbackResult, error) {
	return controller.RollbackResult{Outcome: v1alpha1.PlannedFailoverOutcomeRolledBack}, nil
}
func (f *pfEnvTopology) ActiveSite(types.NamespacedName) (string, bool) { return "", false }

var _ controller.PlannedFailoverTopology = (*pfEnvTopology)(nil)

// pfEnvGroup creates a two-site group with dc1 active and dc2 a healthy
// replica, and returns its key.
func pfEnvGroup(t *testing.T, prefix string, annotations map[string]string) types.NamespacedName {
	t.Helper()
	ns := createNamespace(t, prefix)
	fg := newTestFG(ns)
	fg.Annotations = annotations
	if err := k8sClient.Create(ctx, fg); err != nil {
		t.Fatal(err)
	}
	fg.Status.ActiveSite = "dc1"
	fg.Status.Sites = []v1alpha1.SiteStatus{{Name: "dc1", State: "writable"}, {Name: "dc2", State: "read-only", Replicating: true}}
	if err := k8sClient.Status().Update(ctx, fg); err != nil {
		t.Fatal(err)
	}
	return client.ObjectKeyFromObject(fg)
}

func pfEnvGet(t *testing.T, nn types.NamespacedName) *v1alpha1.MysqlFailoverGroup {
	t.Helper()
	var fg v1alpha1.MysqlFailoverGroup
	if err := k8sClient.Get(ctx, nn, &fg); err != nil {
		t.Fatal(err)
	}
	return &fg
}

func pfEnvReconciler(topo controller.PlannedFailoverTopology) *controller.MysqlFailoverGroupReconciler {
	return &controller.MysqlFailoverGroupReconciler{Client: k8sClient, APIReader: k8sClient, Scheme: scheme,
		Recorder: record.NewFakeRecorder(256), PlannedTopology: topo}
}

func pfEnvStepUntil(t *testing.T, r *controller.MysqlFailoverGroupReconciler, nn types.NamespacedName, n int,
	done func(*v1alpha1.MysqlFailoverGroup) bool) {
	t.Helper()
	for i := 0; i < n; i++ {
		if done(pfEnvGet(t, nn)) {
			return
		}
		if _, err := r.StepPlannedFailoverForTest(ctx, nn); err != nil {
			t.Fatalf("step %d: %v", i, err)
		}
	}
	if !done(pfEnvGet(t, nn)) {
		t.Fatalf("not reached after %d steps: %+v", n, pfEnvGet(t, nn).Status.PlannedFailover)
	}
}

// The reconciler end to end against the API server with a fake facade:
// Admitting → Succeeded, the annotation removed only once the acceptance
// record committed, revisions strictly increasing, and the failover record
// written with the terminal record.
func TestEnvtest_PlannedFailoverEngine_EndToEnd(t *testing.T) {
	nn := pfEnvGroup(t, "pf-e2e", map[string]string{v1alpha1.PlannedFailoverAnnotation: "dc2"})
	topo := &pfEnvTopology{}
	r := pfEnvReconciler(topo)
	if _, err := r.StepPlannedFailoverForTest(ctx, nn); err != nil {
		t.Fatal(err)
	}
	fg := pfEnvGet(t, nn)
	pf := fg.Status.PlannedFailover
	if pf == nil || pf.Phase != v1alpha1.PlannedFailoverPhaseAdmitting || pf.RunID == "" || pf.Revision != 1 {
		t.Fatalf("admitting record = %+v", pf)
	}
	if _, ok := fg.Annotations[v1alpha1.PlannedFailoverAnnotation]; ok {
		t.Fatal("annotation kept after the Admitting record committed")
	}
	runID, rev := pf.RunID, pf.Revision
	for i := 0; i < 20; i++ {
		if _, err := r.StepPlannedFailoverForTest(ctx, nn); err != nil {
			t.Fatal(err)
		}
		cur := pfEnvGet(t, nn).Status.PlannedFailover
		if cur.RunID != runID || cur.Revision < rev {
			t.Fatalf("block regressed: run %s rev %d after rev %d", cur.RunID, cur.Revision, rev)
		}
		rev = cur.Revision
		if cur.Phase == v1alpha1.PlannedFailoverPhaseSucceeded {
			break
		}
	}
	fg = pfEnvGet(t, nn)
	pf = fg.Status.PlannedFailover
	if pf.Result == nil || pf.Result.Outcome != v1alpha1.PlannedFailoverOutcomeSucceeded {
		t.Fatalf("final = %+v", pf)
	}
	if fg.Status.LastFailoverTarget != "dc2" || fg.Status.PromotionGtidExecuted != "uuid-dc1:1-5" || fg.Status.LastFailover == nil {
		t.Errorf("failover record = %v %q %q", fg.Status.LastFailover, fg.Status.LastFailoverTarget, fg.Status.PromotionGtidExecuted)
	}
}

// §13, against the real upgrade path: blocks written by an older operator
// under the pre-#235 CRD, then the CRD is upgraded to the final schema and
// the objects get the spec fields the final schema requires (serverID).
//   - The old fields are pruned on read. A legacy block whose values the
//     final schema still accepts stays writable beside the runner's status
//     writes. One in a removed phase (Validating) is outside the phase enum:
//     Kubernetes 1.33 and newer ratchet it on status writes, so those keep
//     working; 1.31 and 1.32 validate status writes without ratcheting, so
//     they are refused until the engine replaces the block. The engine's
//     refusal is that replacement on every version, so the group never
//     wedges: its status writes work again right after.
//   - A mid-run legacy block is refused (Failed{SourceLeftFenced,
//     LegacyRunRefused}, the Event), and nothing is hydrated from it.
//   - A legacy terminal block is replaced by the next request's run.
func TestEnvtest_PlannedFailover_LegacyRefusedAfterUpgrade(t *testing.T) {
	upgradeScheme := runtime.NewScheme()
	for _, add := range []func(*runtime.Scheme) error{clientgoscheme.AddToScheme, v1alpha1.AddToScheme, apiextensionsv1.AddToScheme} {
		if err := add(upgradeScheme); err != nil {
			t.Fatal(err)
		}
	}
	oldEnv := &envtest.Environment{
		CRDDirectoryPaths: []string{filepath.Join("testdata", "crd-pre-235")},
		Scheme:            upgradeScheme,
	}
	oldCfg, err := oldEnv.Start()
	if err != nil {
		t.Fatalf("start a second envtest with the pre-#235 CRD: %v", err)
	}
	t.Cleanup(func() { _ = oldEnv.Stop() })
	c, err := client.New(oldCfg, client.Options{Scheme: upgradeScheme})
	if err != nil {
		t.Fatal(err)
	}

	ns := &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: "pf-legacy-upgrade"}}
	if err := c.Create(ctx, ns); err != nil {
		t.Fatal(err)
	}
	start := time.Now().UTC().Truncate(time.Second).Format(time.RFC3339)
	seed := func(name string, block map[string]any) types.NamespacedName {
		t.Helper()
		fg := newTestFG(ns.Name)
		fg.Name = name
		if err := c.Create(ctx, fg); err != nil {
			t.Fatal(err)
		}
		u := &unstructured.Unstructured{}
		u.SetGroupVersionKind(v1alpha1.GroupVersion.WithKind("MysqlFailoverGroup"))
		if err := c.Get(ctx, client.ObjectKeyFromObject(fg), u); err != nil {
			t.Fatal(err)
		}
		status := map[string]any{
			"activeSite": "dc1",
			"sites": []any{
				map[string]any{"name": "dc1", "state": "writable"},
				map[string]any{"name": "dc2", "state": "read-only", "replicating": true},
			},
			"plannedFailover": block,
		}
		if err := unstructured.SetNestedField(u.Object, status, "status"); err != nil {
			t.Fatal(err)
		}
		if err := c.Status().Update(ctx, u); err != nil {
			t.Fatalf("seed %s under the old CRD: %v", name, err)
		}
		return client.ObjectKeyFromObject(fg)
	}
	midRun := seed("midrun", map[string]any{
		"phase": "Draining", "target": "dc2", "sourcePrimary": "dc1", "startTime": start,
		"sourceGtidAtFence": "3e11fa47-71ca-11e1-9e33-c80aa9429562:1-5", "drainStartTime": start,
		"maxLagWait": "5m0s", "drainTimeout": "30s",
	})
	finished := seed("finished", map[string]any{
		"phase": "Succeeded", "target": "dc2", "sourcePrimary": "dc1", "startTime": start, "completionTime": start,
		"durationSeconds": int64(12), "transactionsLost": int64(0), "message": "promoted \"dc2\", 0 transactions lost",
	})
	validating := seed("validating", map[string]any{
		"phase": "Validating", "target": "dc2", "sourcePrimary": "dc1", "startTime": start, "reason": "CooldownActive",
	})

	// Upgrade the CRD in place to the final schema.
	crdOpts := &envtest.CRDInstallOptions{Paths: []string{filepath.Join("..", "..", "config", "crd", "bases")}}
	if err := envtest.ReadCRDFiles(crdOpts); err != nil {
		t.Fatal(err)
	}
	for _, crd := range crdOpts.CRDs {
		if crd.Name != "mysqlfailovergroups.shipstream.io" {
			continue
		}
		var cur apiextensionsv1.CustomResourceDefinition
		if err := c.Get(ctx, client.ObjectKeyFromObject(crd), &cur); err != nil {
			t.Fatal(err)
		}
		cur.Spec = crd.Spec
		if err := c.Update(ctx, &cur); err != nil {
			t.Fatalf("upgrade the CRD: %v", err)
		}
	}
	// The served schema follows the CRD asynchronously: wait until an old
	// field is pruned on read.
	deadline := time.Now().Add(30 * time.Second)
	for {
		u := &unstructured.Unstructured{}
		u.SetGroupVersionKind(v1alpha1.GroupVersion.WithKind("MysqlFailoverGroup"))
		if err := c.Get(ctx, finished, u); err != nil {
			t.Fatal(err)
		}
		if _, found, _ := unstructured.NestedFieldNoCopy(u.Object, "status", "plannedFailover", "sourcePrimary"); !found {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("the upgraded schema never pruned status.plannedFailover.sourcePrimary")
		}
		time.Sleep(200 * time.Millisecond)
	}

	// The pre-#235 CRD pruned spec.sites[].serverID on create; the final
	// schema requires it (A-08), and every write evaluates the rule that
	// keeps it unique, so the upgrade adds it first.
	for _, nn := range []types.NamespacedName{midRun, finished, validating} {
		u := &unstructured.Unstructured{}
		u.SetGroupVersionKind(v1alpha1.GroupVersion.WithKind("MysqlFailoverGroup"))
		if err := c.Get(ctx, nn, u); err != nil {
			t.Fatal(err)
		}
		sites, _, _ := unstructured.NestedSlice(u.Object, "spec", "sites")
		for i := range sites {
			sites[i].(map[string]any)["serverID"] = int64(101 + i)
		}
		if err := unstructured.SetNestedSlice(u.Object, sites, "spec", "sites"); err != nil {
			t.Fatal(err)
		}
		if err := c.Update(ctx, u); err != nil {
			t.Fatalf("%s: add spec.sites[].serverID after the upgrade: %v", nn.Name, err)
		}
	}

	get := func(nn types.NamespacedName) *v1alpha1.MysqlFailoverGroup {
		t.Helper()
		var fg v1alpha1.MysqlFailoverGroup
		if err := c.Get(ctx, nn, &fg); err != nil {
			t.Fatal(err)
		}
		return &fg
	}
	snapshotWrite := func(nn types.NamespacedName) error {
		t.Helper()
		fg := get(nn)
		fg.Status.UpdatePhase = ""
		fg.Status.TopologyGeneration++
		return c.Status().Update(ctx, fg)
	}
	// A status write that leaves the legacy block unchanged (the runner's
	// snapshot writes) is accepted beside a block the schema still accepts,
	// and beside a removed phase only where status writes ratchet.
	ratchets := statusWritesRatchet(t, oldCfg)
	for _, nn := range []types.NamespacedName{midRun, finished, validating} {
		err := snapshotWrite(nn)
		switch {
		case nn != validating || ratchets:
			if err != nil {
				t.Fatalf("%s: a status write beside the unchanged legacy block was refused: %v", nn.Name, err)
			}
		case err == nil:
			t.Fatalf("%s: a status write beside a phase outside the enum was accepted without status ratcheting", nn.Name)
		case !apierrors.IsInvalid(err) || !strings.Contains(err.Error(), "status.plannedFailover.phase"):
			t.Fatalf("%s: status write refused for another reason: %v", nn.Name, err)
		}
	}

	topo := &pfEnvTopology{}
	r := &controller.MysqlFailoverGroupReconciler{Client: c, APIReader: c, Scheme: upgradeScheme,
		Recorder: record.NewFakeRecorder(256), PlannedTopology: topo}
	for _, tc := range []struct {
		nn      types.NamespacedName
		outcome v1alpha1.PlannedFailoverOutcome
	}{
		{midRun, v1alpha1.PlannedFailoverOutcomeSourceLeftFenced},
		{validating, v1alpha1.PlannedFailoverOutcomeNotStarted},
	} {
		if _, err := r.StepPlannedFailoverForTest(ctx, tc.nn); err != nil {
			t.Fatal(err)
		}
		pf := get(tc.nn).Status.PlannedFailover
		if pf.Result == nil || pf.Result.Outcome != tc.outcome || pf.Result.Reason != v1alpha1.PlannedFailoverReasonLegacyRunRefused ||
			pf.RunID == "" || pf.Revision != 1 || pf.TargetSite != "unknown" {
			t.Fatalf("%s: block = %+v", tc.nn.Name, pf)
		}
	}
	if len(topo.calls) != 0 || topo.PlannedClaimHeld(midRun) {
		t.Errorf("a legacy block reached the topology: %v", topo.calls)
	}
	// The refusal replaced the block, so status writes work on every
	// version, including those that do not ratchet status writes.
	for _, nn := range []types.NamespacedName{midRun, validating} {
		if err := snapshotWrite(nn); err != nil {
			t.Fatalf("%s: a status write after the legacy block was refused failed: %v", nn.Name, err)
		}
	}
	found := false
	for len(r.Recorder.(*record.FakeRecorder).Events) > 0 {
		if ev := <-r.Recorder.(*record.FakeRecorder).Events; strings.Contains(ev, "PlannedFailoverLegacyRunRefused") {
			found = true
		}
	}
	if !found {
		t.Error("no PlannedFailoverLegacyRunRefused event")
	}

	// A legacy terminal block is history: a new request replaces it.
	fg := get(finished)
	fg.Annotations = map[string]string{v1alpha1.PlannedFailoverAnnotation: "dc2"}
	if err := c.Update(ctx, fg); err != nil {
		t.Fatal(err)
	}
	if _, err := r.StepPlannedFailoverForTest(ctx, finished); err != nil {
		t.Fatal(err)
	}
	if pf := get(finished).Status.PlannedFailover; pf.Phase != v1alpha1.PlannedFailoverPhaseAdmitting || pf.RunID == "" {
		t.Fatalf("new request over a legacy terminal block = %+v", pf)
	}
}

// The no-op trap and the fence: a status update that changes no bytes does
// not move resourceVersion, while the conditioned metadata fence does, and
// a late write conditioned on the fenced version is refused.
func TestEnvtest_PlannedFailoverStatusFence_NoOpTrapAndLateCommit(t *testing.T) {
	nn := pfEnvGroup(t, "pf-fence", nil)
	base := pfEnvGet(t, nn)
	noop := base.DeepCopy()
	if err := k8sClient.Status().Update(ctx, noop); err != nil {
		t.Fatal(err)
	}
	if noop.ResourceVersion != base.ResourceVersion {
		t.Fatalf("a no-op status update moved resourceVersion %s → %s; the fence must change bytes", base.ResourceVersion, noop.ResourceVersion)
	}
	for _, key := range []string{v1alpha1.PlannedFailoverStatusFenceAnnotation, v1alpha1.PlannedFailoverTerminalAnnotation} {
		cur := pfEnvGet(t, nn)
		fenced := cur.DeepCopy()
		if fenced.Annotations == nil {
			fenced.Annotations = map[string]string{}
		}
		fenced.Annotations[key] = `{"schemaVersion":1,"runID":"run-1","outcome":"SourceLeftFenced","reason":"StatusStalled"}`
		if err := k8sClient.Patch(ctx, fenced, client.MergeFromWithOptions(cur.DeepCopy(), client.MergeFromWithOptimisticLock{})); err != nil {
			t.Fatalf("%s: %v", key, err)
		}
		late := cur.DeepCopy()
		late.Status.PlannedFailover = &v1alpha1.PlannedFailoverStatus{RunID: "run-1", Revision: 9, Phase: v1alpha1.PlannedFailoverPhaseDraining, TargetSite: "dc2"}
		if err := k8sClient.Status().Update(ctx, late); !apierrors.IsConflict(err) {
			t.Fatalf("%s: late commit after the fence = %v, want a conflict", key, err)
		}
	}
}

// §10.5: the hold condition survives a runner status write, and the
// acknowledgement clears it, releases the slot and deletes itself.
func TestEnvtest_PlannedFailover_HoldSurvivesRunnerWriteAndAck(t *testing.T) {
	nn := pfEnvGroup(t, "pf-hold", map[string]string{v1alpha1.PlannedFailoverAnnotation: "dc2"})
	topo := &pfEnvTopology{promote: func() controller.PromoteOutcome {
		return controller.PromoteOutcome{Kind: controller.PromoteInconsistent, Reason: v1alpha1.PlannedFailoverReasonForeignWritable, MayHaveWritten: true}
	}}
	r := pfEnvReconciler(topo)
	pfEnvStepUntil(t, r, nn, 20, func(fg *v1alpha1.MysqlFailoverGroup) bool {
		return fg.Status.PlannedFailover != nil && fg.Status.PlannedFailover.Result != nil
	})
	fg := pfEnvGet(t, nn)
	pf := fg.Status.PlannedFailover
	if pf.Result.Outcome != v1alpha1.PlannedFailoverOutcomeManualRecoveryRequired {
		t.Fatalf("result = %+v", pf.Result)
	}
	if c := apimeta.FindStatusCondition(fg.Status.Conditions, v1alpha1.ConditionAutomaticFailoverHeld); c == nil || c.Status != metav1.ConditionTrue {
		t.Fatalf("hold condition = %+v", c)
	}

	runner := controller.NewTopologyManagerRunner(k8sClient, nil, nil, nil, slog.New(slog.DiscardHandler))
	snap := controller.TopologySnapshot{ActiveSite: "dc1", TopologyGeneration: fg.Status.TopologyGeneration}
	if err := runner.WriteStatusForTest(ctx, nn, snap); err != nil {
		t.Fatal(err)
	}
	if got := pfEnvGet(t, nn); apimeta.FindStatusCondition(got.Status.Conditions, v1alpha1.ConditionAutomaticFailoverHeld) == nil ||
		got.Status.PlannedFailoverHold == nil || got.Status.PlannedFailoverHold.RunID != pf.RunID {
		t.Fatalf("a runner status write dropped the hold: hold %+v", got.Status.PlannedFailoverHold)
	}

	fg = pfEnvGet(t, nn)
	fg.Annotations = map[string]string{v1alpha1.AcknowledgePlannedFailoverHoldAnnotation: pf.RunID}
	if err := k8sClient.Update(ctx, fg); err != nil {
		t.Fatal(err)
	}
	pfEnvStepUntil(t, r, nn, 5, func(fg *v1alpha1.MysqlFailoverGroup) bool {
		_, ack := fg.Annotations[v1alpha1.AcknowledgePlannedFailoverHoldAnnotation]
		return !ack
	})
	fg = pfEnvGet(t, nn)
	if c := apimeta.FindStatusCondition(fg.Status.Conditions, v1alpha1.ConditionAutomaticFailoverHeld); fg.Status.PlannedFailoverHold != nil ||
		c == nil || c.Status != metav1.ConditionFalse || c.Reason != v1alpha1.ReasonPlannedFailoverHoldAcknowledged ||
		c.ObservedGeneration != fg.Generation {
		t.Errorf("after the acknowledgement: hold %+v, condition %+v; want no hold and False/%s at the current generation",
			fg.Status.PlannedFailoverHold, c, v1alpha1.ReasonPlannedFailoverHoldAcknowledged)
	}
	if topo.PlannedClaimHeld(nn) {
		t.Error("slot still held after the acknowledgement")
	}
}

// §6.4 / §6.3: a durable deadline annotation is written once per run and
// never moved forward by a restarted reconciler; it is removed after the
// next committed status write.
func TestEnvtest_PlannedFailover_DurableDeadlineAnnotations(t *testing.T) {
	nn := pfEnvGroup(t, "pf-deadline", nil)
	fg := pfEnvGet(t, nn)
	now := metav1.Now()
	f := "uuid-dc1:1-5"
	fg.Status.PlannedFailover = &v1alpha1.PlannedFailoverStatus{RunID: "run-1", Revision: 3, Phase: v1alpha1.PlannedFailoverPhaseWaitingForLag,
		TargetSite: "dc2", SourceSite: "dc1", StartTime: now,
		Settings: v1alpha1.PlannedFailoverSettings{MaxLagWait: metav1.Duration{Duration: time.Hour}, DrainTimeout: metav1.Duration{Duration: time.Second}},
		Fence: &v1alpha1.PlannedFailoverFenceRecord{State: v1alpha1.PlannedFailoverFenceApplied, IntentTime: now, SourceServerUUID: "uuid-dc1",
			ApplyTime: &now, GtidSet: &f},
		Drain: &v1alpha1.PlannedFailoverDrainRecord{StartTime: now, CompletionTime: &now}, LagWait: &v1alpha1.PlannedFailoverLagWaitRecord{StartTime: now}}
	if err := k8sClient.Status().Update(ctx, fg); err != nil {
		t.Fatal(err)
	}
	absent := &pfEnvAbsentTopology{pfEnvTopology: &pfEnvTopology{held: "run-1"}}
	var first string
	for i := 0; i < 3; i++ {
		r := pfEnvReconciler(absent) // a restarted operator each time
		if _, err := r.StepPlannedFailoverForTest(ctx, nn); err != nil {
			t.Fatal(err)
		}
		got := pfEnvGet(t, nn).Annotations[v1alpha1.PlannedFailoverManagerAbsentSinceAnnotation]
		if got == "" {
			t.Fatal("no manager-absent-since annotation")
		}
		if first == "" {
			first = got
		} else if got != first {
			t.Fatalf("restart %d moved the absent-since start: %q → %q", i, first, got)
		}
		time.Sleep(1100 * time.Millisecond)
	}
	// A manager is back: the annotation goes.
	r := pfEnvReconciler(absent.pfEnvTopology)
	if _, err := r.StepPlannedFailoverForTest(ctx, nn); err != nil {
		t.Fatal(err)
	}
	if v, ok := pfEnvGet(t, nn).Annotations[v1alpha1.PlannedFailoverManagerAbsentSinceAnnotation]; ok {
		t.Errorf("absent-since kept once a manager answered: %q", v)
	}
}

// pfEnvAbsentTopology reports no registered manager.
type pfEnvAbsentTopology struct{ *pfEnvTopology }

func (f *pfEnvAbsentTopology) EnsureClaim(context.Context, controller.PlannedRunRef, *v1alpha1.MysqlFailoverGroup) (controller.PlannedClaimDecision, error) {
	return controller.PlannedClaimDecision{Kind: controller.ClaimWait, WaitReason: v1alpha1.PlannedFailoverWaitManagerAbsent}, nil
}
