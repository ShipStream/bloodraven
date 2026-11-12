//go:build envtest

package envtest

import (
	"bytes"
	"log/slog"
	"path/filepath"
	"strings"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	apiextensionsv1 "k8s.io/apiextensions-apiserver/pkg/apis/apiextensions/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	apimeta "k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	"k8s.io/client-go/tools/record"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/envtest"

	v1alpha1 "github.com/shipstream/bloodraven/api/v1alpha1"
	"github.com/shipstream/bloodraven/internal/controller"
	"github.com/shipstream/bloodraven/internal/state"
)

// An operator running against the CRD of the release before
// recoveryState RecloneRequired and sourceConvergenceReason
// OwnTransactionsLost existed (helm does not upgrade a chart's crds/).
// That CRD refuses the whole status update for either value. The status
// writer must still land the active site, the failover record and the
// conditions, with the nearest value the old CRD knows, and must not
// announce the same report on every write because the old CRD prunes
// status.sites[].lostGtid.
func TestEnvtest_StatusWrite_OlderCRD(t *testing.T) {
	oldScheme := runtime.NewScheme()
	for _, add := range []func(*runtime.Scheme) error{clientgoscheme.AddToScheme, v1alpha1.AddToScheme, apiextensionsv1.AddToScheme} {
		if err := add(oldScheme); err != nil {
			t.Fatal(err)
		}
	}
	oldEnv := &envtest.Environment{
		CRDDirectoryPaths: []string{filepath.Join("testdata", "crd-pre-own-loss")},
		Scheme:            oldScheme,
	}
	oldCfg, err := oldEnv.Start()
	if err != nil {
		t.Fatalf("start a second envtest with the previous CRD: %v", err)
	}
	t.Cleanup(func() { _ = oldEnv.Stop() })
	c, err := client.New(oldCfg, client.Options{Scheme: oldScheme})
	if err != nil {
		t.Fatal(err)
	}
	ns := &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: "status-older-crd"}}
	if err := c.Create(ctx, ns); err != nil {
		t.Fatal(err)
	}
	fg := newTestFG(ns.Name)
	if err := c.Create(ctx, fg); err != nil {
		t.Fatal(err)
	}
	nn := types.NamespacedName{Name: fg.Name, Namespace: ns.Name}

	// The premise: this CRD refuses both values on a direct write.
	for _, site := range []v1alpha1.SiteStatus{
		{Name: "dc2", State: v1alpha1.SiteStateReadOnly, RecoveryState: v1alpha1.RecoveryStateRecloneRequired},
		{Name: "dc2", State: v1alpha1.SiteStateReadOnly, SourceConvergenceReason: v1alpha1.SourceReasonOwnTransactionsLost},
	} {
		var cur v1alpha1.MysqlFailoverGroup
		if err := c.Get(ctx, nn, &cur); err != nil {
			t.Fatal(err)
		}
		cur.Status.Sites = []v1alpha1.SiteStatus{{Name: "dc1", State: v1alpha1.SiteStateWritable}, site}
		if err := c.Status().Update(ctx, &cur); !apierrors.IsInvalid(err) {
			t.Fatalf("direct status write of %+v = %v, want it refused as Invalid by the previous CRD", site, err)
		}
	}

	var logs bytes.Buffer
	rec := record.NewFakeRecorder(64)
	runner := controller.NewTopologyManagerRunner(c, nil, nil, rec, slog.New(slog.NewTextHandler(&logs, nil)))
	runner.SetManagerForTest(nn, &controller.TopologyManager{})
	snapshot := func(lostGtid string) controller.TopologySnapshot {
		dc2 := controller.SiteSnapshot{Name: "dc2", Role: state.SiteRolePrimaryCandidate, State: state.StateReadOnly}
		snap := controller.TopologySnapshot{ActiveSite: "dc1", DegradedReason: v1alpha1.ReasonHealthy}
		if lostGtid != "" {
			dc2.RecoveryState = v1alpha1.RecoveryStateRecloneRequired
			dc2.LostGtid, dc2.LostTxnCount, dc2.LostComparedTo = lostGtid, 10, "dc1"
			dc2.SourceConvergenceState = v1alpha1.SourceConvergenceBlocked
			dc2.SourceConvergenceReason = v1alpha1.SourceReasonOwnTransactionsLost
			snap.RecoverySite, snap.RecoveryState = "dc2", v1alpha1.RecoveryStateRecloneRequired
		} else {
			dc2.ReplicationHealthy = true
			dc2.SourceConvergenceState = v1alpha1.SourceConvergenceConverged
		}
		snap.Sites = []controller.SiteSnapshot{{Name: "dc1", Role: state.SiteRolePrimaryCandidate, State: state.StateWritable}, dc2}
		return snap
	}
	events := func(reason string) int {
		n := 0
		for len(rec.Events) > 0 {
			if e := <-rec.Events; strings.Contains(e, " "+reason+" ") {
				n++
			}
		}
		return n
	}
	read := func() v1alpha1.MysqlFailoverGroup {
		t.Helper()
		var got v1alpha1.MysqlFailoverGroup
		if err := c.Get(ctx, nn, &got); err != nil {
			t.Fatal(err)
		}
		return got
	}

	const lost = "22222222-0000-11f1-8000-0000000000b2:91-100"
	if err := runner.WriteStatusForTest(ctx, nn, snapshot(lost)); err != nil {
		t.Fatalf("status write with a RecloneRequired site against the previous CRD: %v", err)
	}
	got := read()
	if got.Status.ActiveSite != "dc1" || len(got.Status.Sites) != 2 {
		t.Fatalf("status = %+v, want the active site and both sites written", got.Status)
	}
	dc2 := got.Status.Sites[1]
	if dc2.RecoveryState != v1alpha1.RecoveryStateBlocked || dc2.SourceConvergenceReason != v1alpha1.SourceReasonGTIDDiverged {
		t.Fatalf("dc2 = recoveryState %q, sourceConvergenceReason %q; want the values the previous CRD knows (RecoveryBlocked, GTIDDiverged)",
			dc2.RecoveryState, dc2.SourceConvergenceReason)
	}
	if dc2.LostGtid != "" || dc2.LostComparedTo != "" {
		t.Fatalf("lostGtid = %q, lostComparedTo = %q: the previous CRD was expected to prune them; the fixture is not the previous CRD",
			dc2.LostGtid, dc2.LostComparedTo)
	}
	// What is stored is RecoveryBlocked without any set: the shape an
	// operator that restarts on this status takes for an own-loss report
	// whose set the CRD pruned, and restores as one (controller test
	// TestStartManager_RestoresAnOwnLossReportDegradedForAnOlderCRD).
	if dc2.DivergentGtid != "" || dc2.DivergentTransactionCount != nil {
		t.Fatalf("dc2 = %+v: the degraded report carries a divergent set, so a restart would take it for a divergence", dc2)
	}
	// The conditions carry the real reason: the group does not read
	// healthy, and says what is wrong.
	for _, ty := range []string{v1alpha1.ConditionDegraded, v1alpha1.ConditionRecoveryPending} {
		cond := apimeta.FindStatusCondition(got.Status.Conditions, ty)
		if cond == nil || cond.Status != metav1.ConditionTrue || cond.Reason != v1alpha1.ReasonOwnTransactionsLost || !strings.Contains(cond.Message, "dc2 lacks 10 transactions") {
			t.Fatalf("%s = %+v, want True/OwnTransactionsLost naming dc2", ty, cond)
		}
	}
	if n := events(v1alpha1.EventOwnTransactionsLost); n != 1 {
		t.Fatalf("%d OwnTransactionsLost Events after the first write, want 1", n)
	}
	if n := strings.Count(logs.String(), "status written with the values an older CRD accepts"); n != 1 {
		t.Fatalf("the older-CRD condition was logged %d times, want once:\n%s", n, logs.String())
	}

	// Later writes: nothing is refused again, the report is not announced
	// again although the stored status has no lostGtid, and an unchanged
	// status is not rewritten (the wait puts the writes in different
	// seconds: a status written again would carry new timestamps).
	rv := got.ResourceVersion
	for i := 0; i < 3; i++ {
		time.Sleep(600 * time.Millisecond)
		if err := runner.WriteStatusForTest(ctx, nn, snapshot(lost)); err != nil {
			t.Fatalf("status write %d: %v", i, err)
		}
	}
	if got = read(); got.ResourceVersion != rv {
		t.Fatalf("resourceVersion moved %s -> %s on unchanged status writes", rv, got.ResourceVersion)
	}
	if n := events(v1alpha1.EventOwnTransactionsLost); n != 0 {
		t.Fatalf("%d more OwnTransactionsLost Events for the same report, want none", n)
	}
	if n := strings.Count(logs.String(), "status written with the values an older CRD accepts"); n != 1 {
		t.Fatalf("the older-CRD condition was logged %d times, want once", n)
	}

	// The report ends: resolved once, and the site reads healthy again.
	if err := runner.WriteStatusForTest(ctx, nn, snapshot("")); err != nil {
		t.Fatal(err)
	}
	got = read()
	if s := got.Status.Sites[1]; s.RecoveryState != "" {
		t.Fatalf("dc2 recoveryState = %q after the report ended, want none", s.RecoveryState)
	}
	if n := events(v1alpha1.EventOwnTransactionsLostResolved); n != 1 {
		t.Fatalf("%d OwnTransactionsLostResolved Events, want 1", n)
	}
}

// The other way an older CRD shows: no value it refuses, only a field it
// prunes. A site that is RecoveryBlocked and carries lostGtid passes the
// old enum, the write succeeds, and lostGtid does not come back. The
// writer notices, logs the older CRD once, and does not write the same
// status again on every poll.
func TestEnvtest_StatusWrite_OlderCRDPrunesLostGtid(t *testing.T) {
	oldScheme := runtime.NewScheme()
	for _, add := range []func(*runtime.Scheme) error{clientgoscheme.AddToScheme, v1alpha1.AddToScheme, apiextensionsv1.AddToScheme} {
		if err := add(oldScheme); err != nil {
			t.Fatal(err)
		}
	}
	oldEnv := &envtest.Environment{
		CRDDirectoryPaths: []string{filepath.Join("testdata", "crd-pre-own-loss")},
		Scheme:            oldScheme,
	}
	oldCfg, err := oldEnv.Start()
	if err != nil {
		t.Fatalf("start a second envtest with the previous CRD: %v", err)
	}
	t.Cleanup(func() { _ = oldEnv.Stop() })
	c, err := client.New(oldCfg, client.Options{Scheme: oldScheme})
	if err != nil {
		t.Fatal(err)
	}
	ns := &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: "status-older-crd-pruned"}}
	if err := c.Create(ctx, ns); err != nil {
		t.Fatal(err)
	}
	fg := newTestFG(ns.Name)
	if err := c.Create(ctx, fg); err != nil {
		t.Fatal(err)
	}
	nn := types.NamespacedName{Name: fg.Name, Namespace: ns.Name}
	var logs bytes.Buffer
	rec := record.NewFakeRecorder(64)
	runner := controller.NewTopologyManagerRunner(c, nil, nil, rec, slog.New(slog.NewTextHandler(&logs, nil)))
	runner.SetManagerForTest(nn, &controller.TopologyManager{})
	const lost, divergent = "22222222-0000-11f1-8000-0000000000b2:91-100", "22222222-0000-11f1-8000-0000000000b2:101-105"
	snap := controller.TopologySnapshot{ActiveSite: "dc1", DegradedReason: v1alpha1.ReasonHealthy,
		RecoverySite: "dc2", RecoveryState: v1alpha1.RecoveryStateBlocked, DivergentGtid: divergent, DivergentTxnCount: 5,
		Sites: []controller.SiteSnapshot{
			{Name: "dc1", Role: state.SiteRolePrimaryCandidate, State: state.StateWritable},
			{Name: "dc2", Role: state.SiteRolePrimaryCandidate, State: state.StateReadOnly,
				RecoveryState: v1alpha1.RecoveryStateBlocked, DivergentGtid: divergent, DivergentTxnCount: 5, DivergenceComparedTo: "dc1",
				LostGtid: lost, LostTxnCount: 10, LostComparedTo: "dc1"},
		}}
	var rv string
	ownLossEvents := 0
	for i := 0; i < 4; i++ {
		if i > 0 {
			time.Sleep(600 * time.Millisecond)
		}
		if err := runner.WriteStatusForTest(ctx, nn, snap); err != nil {
			t.Fatalf("status write %d: %v", i, err)
		}
		var got v1alpha1.MysqlFailoverGroup
		if err := c.Get(ctx, nn, &got); err != nil {
			t.Fatal(err)
		}
		if s := got.Status.Sites[1]; s.RecoveryState != v1alpha1.RecoveryStateBlocked || s.DivergentGtid != divergent || s.LostGtid != "" || s.LostComparedTo != "" {
			t.Fatalf("write %d: dc2 = %+v, want RecoveryBlocked with its divergent set, and lostGtid and lostComparedTo pruned", i, s)
		}
		// The first write finds out; from the second on nothing changes.
		if i == 1 {
			rv = got.ResourceVersion
		} else if i > 1 && got.ResourceVersion != rv {
			t.Fatalf("write %d: resourceVersion moved %s -> %s: the same status was written again", i, rv, got.ResourceVersion)
		}
		for len(rec.Events) > 0 {
			if e := <-rec.Events; strings.Contains(e, " "+v1alpha1.EventOwnTransactionsLost+" ") {
				ownLossEvents++
			}
		}
	}
	if ownLossEvents != 1 {
		t.Fatalf("%d OwnTransactionsLost Events over four writes of one report, want 1", ownLossEvents)
	}
	if n := strings.Count(logs.String(), "status written with the values an older CRD accepts"); n != 1 {
		t.Fatalf("the older-CRD condition was logged %d times, want once:\n%s", n, logs.String())
	}
}

// The same writes against the current CRD keep the current values.
func TestEnvtest_StatusWrite_CurrentCRDKeepsRecloneRequired(t *testing.T) {
	ns := "envtest-status-current-crd"
	ensureNamespace(t, ns)
	fg := newTestFG(ns)
	if err := k8sClient.Create(ctx, fg); err != nil {
		t.Fatal(err)
	}
	nn := types.NamespacedName{Name: fg.Name, Namespace: ns}
	var logs bytes.Buffer
	runner := controller.NewTopologyManagerRunner(k8sClient, nil, nil, record.NewFakeRecorder(16), slog.New(slog.NewTextHandler(&logs, nil)))
	runner.SetManagerForTest(nn, &controller.TopologyManager{})
	snap := controller.TopologySnapshot{ActiveSite: "dc1", DegradedReason: v1alpha1.ReasonHealthy,
		RecoverySite: "dc2", RecoveryState: v1alpha1.RecoveryStateRecloneRequired,
		Sites: []controller.SiteSnapshot{
			{Name: "dc1", Role: state.SiteRolePrimaryCandidate, State: state.StateWritable},
			{Name: "dc2", Role: state.SiteRolePrimaryCandidate, State: state.StateReadOnly,
				RecoveryState: v1alpha1.RecoveryStateRecloneRequired, LostGtid: "22222222-0000-11f1-8000-0000000000b2:91-100", LostTxnCount: 10, LostComparedTo: "dc1",
				SourceConvergenceState: v1alpha1.SourceConvergenceBlocked, SourceConvergenceReason: v1alpha1.SourceReasonOwnTransactionsLost},
		}}
	if err := runner.WriteStatusForTest(ctx, nn, snap); err != nil {
		t.Fatal(err)
	}
	var got v1alpha1.MysqlFailoverGroup
	if err := k8sClient.Get(ctx, nn, &got); err != nil {
		t.Fatal(err)
	}
	if s := got.Status.Sites[1]; s.RecoveryState != v1alpha1.RecoveryStateRecloneRequired || s.SourceConvergenceReason != v1alpha1.SourceReasonOwnTransactionsLost || s.LostGtid == "" {
		t.Fatalf("dc2 = %+v, want RecloneRequired, OwnTransactionsLost and the lost set stored", s)
	}
	// The site the report was compared with is stored with it: a restarted
	// operator restores the report with its holder.
	if s := got.Status.Sites[1]; s.LostComparedTo != "dc1" {
		t.Fatalf("dc2 lostComparedTo = %q, want dc1 stored by the current CRD", s.LostComparedTo)
	}
	if strings.Contains(logs.String(), "older CRD") {
		t.Fatalf("the current CRD was reported as older:\n%s", logs.String())
	}
	// An unchanged status is not written again, and Degraded keeps its
	// transition time (it is built in two steps: the wait puts the two
	// writes in different seconds).
	time.Sleep(1100 * time.Millisecond)
	if err := runner.WriteStatusForTest(ctx, nn, snap); err != nil {
		t.Fatal(err)
	}
	var again v1alpha1.MysqlFailoverGroup
	if err := k8sClient.Get(ctx, nn, &again); err != nil {
		t.Fatal(err)
	}
	if again.ResourceVersion != got.ResourceVersion {
		t.Fatalf("resourceVersion moved %s -> %s on an unchanged status write", got.ResourceVersion, again.ResourceVersion)
	}
	before := apimeta.FindStatusCondition(got.Status.Conditions, v1alpha1.ConditionDegraded)
	after := apimeta.FindStatusCondition(again.Status.Conditions, v1alpha1.ConditionDegraded)
	if before == nil || after == nil || !before.LastTransitionTime.Equal(&after.LastTransitionTime) {
		t.Fatalf("Degraded = %+v, then %+v: lastTransitionTime moved without a transition", before, after)
	}
}
